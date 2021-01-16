//
//  main.mm
//  co_dispatch
//
//  Created by Antony Searle on 14/1/21.
//

#include <atomic>
#include <thread>
#include <chrono>
#include <experimental/coroutine>
#include <dispatch/dispatch.h>
#include <cinttypes>

// rationale: establish some coroutines practices without worrying about reactors etc.

using std::experimental::coroutine_handle;

void dispatch_async(coroutine_handle<> continuation) {
    assert(continuation);
    dispatch_async_f(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0),
                     continuation.address(),
                     [ ](void* address) {
        coroutine_handle<>::from_address(address).resume();
    });
    
}

struct async_mutex { // after cppcoro
    
    enum : std::uintptr_t {
        LOCKED_NO_WAITERS = 0, // <-- zero because it terminates the list of waiters
        NOT_LOCKED = 1
    };

    struct awaitable {
        async_mutex& _mutex;
        std::experimental::coroutine_handle<> _continuation;
        awaitable* _next;
        
        explicit awaitable(async_mutex& mutex)
        : _mutex(mutex)
        , _next((awaitable*) NOT_LOCKED) {
            assert(this != (awaitable*) NOT_LOCKED);
            assert(this != (awaitable*) LOCKED_NO_WAITERS);
        }
        
        bool await_ready() {
            return _mutex._state.compare_exchange_weak(_next,
                                                        (awaitable*) LOCKED_NO_WAITERS,
                                                        std::memory_order_acquire,
                                                        std::memory_order_relaxed);
        }
        
        coroutine_handle<> await_suspend(coroutine_handle<> continuation) {
            _continuation = continuation;
            for (;;) {
                if (_next == (awaitable*) NOT_LOCKED) {
                    if (await_ready()) {
                        return _continuation;
                    }
                } else {
                    // locked, with or without waiters
                    if (_mutex._state.compare_exchange_weak(_next,
                                                             this,
                                                             std::memory_order_release,
                                                             std::memory_order_relaxed)) {
                        // todo: ask global queue for next job instead
                        return std::experimental::noop_coroutine();
                    }
                }
            }
        }
        
        void await_resume() {}
        
    }; // awaitable
    
    std::atomic<awaitable*> _state = (awaitable*) NOT_LOCKED;
    awaitable* _waiters = nullptr; // slist of waiters, protected by the mutex itself
    
    awaitable lock() {
        return awaitable(*this);
    }
    
    void unlock() {
        if (!_waiters) {
            auto expected = (awaitable*) LOCKED_NO_WAITERS;
            if (_state.compare_exchange_strong(expected,
                                              (awaitable*) NOT_LOCKED,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                return;
            }
            assert(expected != (awaitable*) NOT_LOCKED);
            expected = _state.exchange((awaitable*) LOCKED_NO_WAITERS,
                                      std::memory_order_acquire);
            assert(expected != (awaitable*) LOCKED_NO_WAITERS);
            assert(expected != (awaitable*) NOT_LOCKED);
            // reverse list into _waiters
            do {
                _waiters = std::exchange(expected,
                                         std::exchange(expected->_next,
                                                       _waiters));
            } while (expected);
        }
        assert(_waiters);
        dispatch_async_f(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0),
                         std::exchange(_waiters, _waiters->_next)->_continuation.address(),
                         [](void* address) {
            coroutine_handle<>::from_address(address).resume();
        });
    }
    
    bool try_lock() {
        auto expected = (awaitable*) NOT_LOCKED;
        return _state.compare_exchange_strong(expected,
                                              (awaitable*) LOCKED_NO_WAITERS,
                                              std::memory_order_acquire,
                                              std::memory_order_relaxed);
    }
    
}; // async_mutex


template<typename T>
union tagged_ptr {
    
    struct _ptr_t {
        enum { MASK = -8 };
        std::uintptr_t _raw;
        operator bool() const { return _raw & MASK; }
        operator T*() const { return (T*) (_raw & MASK); }
        T* operator->() const { return (T*) (_raw & MASK); }
        bool operator!() const { return !(_raw & MASK); }
    } ptr;
    
    struct _tag_t {
        enum { MASK = 7 };
        std::uintptr_t _raw;
        explicit operator bool() const { return _raw & MASK; }
        operator std::uint64_t() const { return _raw & MASK; }
    } tag;
    std::uintptr_t _raw;
    
    tagged_ptr() : _raw(0) {}
    
    tagged_ptr(std::nullptr_t) : _raw(0) {}
    
    explicit tagged_ptr(T* ptr)
    : _raw((std::uintptr_t) ptr) {
        assert(!tag);
    }
    
    tagged_ptr(T* ptr, std::uintptr_t tag)
    : _raw(((std::uintptr_t) ptr) | tag) {
        assert(!(((std::uintptr_t) ptr) & _tag_t::MASK));
        assert(!(tag & _ptr_t::MASK));
    }
    
    tagged_ptr(std::uintptr_t x) : _raw(x) {}
    
    tagged_ptr& operator=(std::nullptr_t) { _raw = 0; return *this; }
    
    T* operator->() { return ptr; }

    explicit operator bool() const { return ptr; }
    bool operator!() const { return !ptr; }
    
    tagged_ptr operator^(std::uintptr_t x) {
        assert(!(x & _ptr_t::MASK));
        return tagged_ptr(_raw ^ x);
    }
    
    tagged_ptr operator|(std::uintptr_t x) {
        assert(!(x & _ptr_t::MASK));
        return tagged_ptr(_raw | x);
    }
    
};

template<typename T>
struct channel {
    // unbuffered channel rendezvous push and pop
    // there are no waiters, or push waiters only, or pop waiters only
    // aka dual queue
    // can't extend life of nodes if they are in coroutine frames (without rc
    // all promise types!)
    // therefore we have to use some kind of lock
    // therefore we have to use a mutex?
    // mutex is a very similar structure though...
    
    //              ____________________________________
    //             /                                    \
    //             \       @                     @      /
    //              UNLOCKED | PUSH        LOCKED | PUSH
    //             /               \      /             \         @
    //     UNLOCKED                 LOCKED               LOCKED | PUSH | POP
    //    /        \               /    \ \             /         @         \
    //    \         UNLOCKED | POP      /  LOCKED | POP                     /
    //     \       /       @           /         @     \                   /
    //      \      \__________________L________________/                  /
    //       \______L________________L___________________________________/
    //
    // note that UNLOCKED | PUSH | POP is forbidden
    // enter
    // enqueue self to acquire lock
    //
    
    enum : std::uintptr_t {
        LOCKED = 1,
        PUSH   = 2,
        POP    = 4,
        MASK = PUSH | POP,
        GO     = 8,
    };
    
    template<typename Node>
    struct intrusive_singly_linked_list {
        
        using Pointer = decltype(std::declval<Node>()._next);
        
        Pointer head = nullptr;
        Pointer tail = nullptr;
        
        Pointer pop_head() {
            Pointer old = head;
            if (head) {
                head = head->_next;
                if (!head)
                    tail = nullptr;
                old->_next = nullptr;
            }
            return old;
        }
        void push_head(Pointer desired) {
            if (desired) {
                if (!head)
                    tail = desired;
                desired->_next = head;
                head = desired;
            }
        }
        void push_tail(Pointer desired) {
            if (desired) {
                assert(!desired->_next);
                if (tail) {
                    tail->_next = desired;
                }
                tail = desired;
                if (!head)
                    head = desired;
            }
        }
        void splice_head(intrusive_singly_linked_list& other) {
            if (other.tail) {
                assert(other.head);
                other.tail->_next = head;
                head = other.head;
                if (!tail)
                    tail = other.tail;
                other.head = 0;
                other.tail = 0;
            }
            assert(!other.head);
            assert(!other.tail);
        }
        void swap(intrusive_singly_linked_list& other) {
            using std::swap;
            swap(head, other.head);
            swap(tail, other.tail);
        }
        std::size_t size() const {
            std::size_t n = 0;
            auto a = head;
            while (a) {
                a = a->_next;
                ++n;
            }
            return n;
        }
    };
    
    struct awaitable_base {
        channel& _channel;
        union { T _payload; };
        tagged_ptr<awaitable_base> _next;
        coroutine_handle<> _continuation;
        std::uintptr_t _flags;
        
        bool await_ready() const { return false; }
        
        void _distribute() {
            auto& _queue = this->_channel._queue;
            if (_next.ptr) {
                // we have acquired waiters
                // distribute and reverse them
                intrusive_singly_linked_list<awaitable_base> qpop;
                intrusive_singly_linked_list<awaitable_base> qpush;
                do {
                    if (_next->_flags & POP) {
                        qpop.push_head(std::exchange(_next, _next->_next));
                    } else {
                        assert(_next->_flags & PUSH);
                        qpush.push_head(std::exchange(_next, _next->_next));
                    }
                } while (_next.ptr);
                // prepend existing waiters to appropriate queue
                if (_queue.head) {
                    if (_queue.head->_flags & POP) {
                        qpop.splice_head(_queue);
                    } else {
                        assert(_queue.head->_flags & PUSH);
                        qpush.splice_head(_queue);
                    }
                }
                // pair until one is empty
                while (qpop.head && qpush.head) {
                    assert(qpop.head->_flags & POP);
                    assert(!(qpop.head->_flags & PUSH));
                    assert(qpush.head->_flags & PUSH);
                    assert(!(qpush.head->_flags & POP));
                    new (&qpop.head->_payload) T(std::move(qpush.head->_payload));
                    qpush.head->_payload.~T();
                    dispatch_async(qpop.pop_head()->_continuation);
                    dispatch_async(qpush.pop_head()->_continuation);
                }
                // install the survivors, if any
                if (qpop.head) {
                    assert(!qpush.head);
                    _queue.swap(qpop);
                } else if (qpush.head) {
                    assert(!qpop.head);
                    _queue.swap(qpush);
                }
            }
        }
        
        void _unlock() {
            auto& _queue = _channel._queue;
            auto& _state = _channel._state;
            auto& expected = this->_next;
            expected = _state.load(std::memory_order_relaxed);
            for (;;) {
                assert(expected.tag & LOCKED);
                std::uintptr_t qtag = 0;
                if (_queue.head) {
                    if (_queue.head->_flags & POP) {
                        assert(!(_queue.head->_flags & PUSH));
                        qtag = POP;
                    } else {
                        assert(_queue.head->_flags & PUSH);
                        assert(!(_queue.head->_flags & POP));
                        qtag = PUSH;
                    }
                }
                if (((expected.tag | qtag) & MASK) == MASK) {
                    // both pushes and pops are present, we need get and match them
                    if (_state.compare_exchange_weak(expected,
                                                     LOCKED,
                                                     std::memory_order_acquire,
                                                     std::memory_order_relaxed)) {
                        this->_distribute();
                    }
                } else {
                    // at most one kind of operation is present, we are done
                    tagged_ptr<awaitable_base> desired = (expected ^ LOCKED) | qtag;
                    assert(desired.tag != (PUSH | POP));
                    if (_state.compare_exchange_weak(expected,
                                                     desired,
                                                     std::memory_order_release,
                                                     std::memory_order_relaxed)) {
                        return;
                    }
                }
            }
        }
        
    };
    
    static_assert(alignof(awaitable_base) >= 8, "not enough alignment bits");
    
    std::atomic<tagged_ptr<awaitable_base>> _state;
    intrusive_singly_linked_list<awaitable_base> _queue;
    
    struct awaitable_push_operation : awaitable_base {
        
        coroutine_handle<> await_suspend(coroutine_handle<> continuation) {
            this->_continuation = continuation;
            this->_flags = PUSH;
            this->_next = this->_channel._state.load(std::memory_order_relaxed);
            for (;;) {
                assert(this->_next.tag != (PUSH | POP));
                if (this->_next.tag == POP) {
                    // unlocked and there is a partner for us, we can proceed
                    if (this->_channel._state.compare_exchange_weak(this->_next,
                                                                    LOCKED,
                                                                    std::memory_order_acquire,
                                                                    std::memory_order_relaxed)) {
                        this->_flags |= GO;
                        this->_continuation = nullptr;
                        return continuation;
                    }
                } else {
                    // locked or no partner, we must suspend
                    tagged_ptr<awaitable_base> desired{this, this->_next.tag | PUSH};
                    assert(desired.tag != (PUSH | POP));
                    if (this->_channel._state.compare_exchange_weak(this->_next,
                                                                    desired,
                                                                    std::memory_order_release,
                                                                    std::memory_order_relaxed)) {
                        return std::experimental::noop_coroutine();
                    }
                }
            }
        }
        
        void await_resume() {
            
            auto& _next = this->_next;
            auto& _queue = this->_channel._queue;
            
            if (this->_flags & GO) {

                // we arrive here after state transition POP -> LOCKED
                this->_distribute();
                
                // there should now be one or more pop operation queued
                assert(_queue.head);
                assert(_queue.head->_flags & POP);
                // we have a partner
                new (&_queue.head->_payload) T(std::move(this->_payload));
                dispatch_async(_queue.pop_head()->_continuation);
                this->_payload.~T();
                
                // this push is now complete, but it still has to extricate
                // itself from the locked state

                this->_unlock();
            }
        }
    };
    
    struct awaitable_pop_operation : awaitable_base {
        
        coroutine_handle<> await_suspend(coroutine_handle<> continuation) {
            this->_continuation = continuation;
            this->_flags = POP;
            this->_next = this->_channel._state.load(std::memory_order_relaxed);
            for (;;) {
                assert(this->_next.tag != (PUSH | POP));
                if (this->_next.tag == PUSH) {
                    // unlocked and there is a partner for us, we can proceed
                    if (this->_channel._state.compare_exchange_weak(this->_next,
                                                                    LOCKED,
                                                                    std::memory_order_acquire,
                                                                    std::memory_order_relaxed)) {
                        this->_flags |= GO;
                        this->_continuation = nullptr;
                        return continuation;
                    }
                } else {
                    // locked or no partner, we must suspend
                    tagged_ptr<awaitable_base> desired{this, this->_next.tag | POP};
                    assert(desired.tag != (PUSH | POP));
                    if (this->_channel._state.compare_exchange_weak(this->_next,
                                                                    desired,
                                                                    std::memory_order_release,
                                                                    std::memory_order_relaxed)) {
                        return std::experimental::noop_coroutine();
                    }
                }
            }
        }
        
        T await_resume() {
            
            auto& _next = this->_next;
            auto& _queue = this->_channel._queue;
                        
            if (this->_flags & GO) {

                // we arrive here after state transition PUSH -> LOCKED
                this->_distribute();
                // there should now be one or more push operation queued
                assert(_queue.head);
                assert(_queue.head->_flags & PUSH);
                // we have a partner
                new (&this->_payload) T(std::move(_queue.head->_payload));
                _queue.head->_payload.~T();
                dispatch_async(_queue.pop_head()->_continuation);
                
                // this pop is now complete, but it still has to extricate
                // itself from the locked state
                
                this->_unlock();
            }
            T result{std::move(this->_payload)};
            this->_payload.~T();
            return result;
        }
    };
        
    [[nodiscard]] awaitable_push_operation push(T x) {
        return awaitable_push_operation{ *this, x };
    }
    
    [[nodiscard]] awaitable_pop_operation pop() {
        return awaitable_pop_operation{ *this };
    }
    
};

struct until {
    dispatch_time_t _when;
    dispatch_queue_t _queue;
    bool await_ready() { return false; }
    template<typename Promise>
    void await_suspend(std::experimental::coroutine_handle<Promise> handle) {
        dispatch_after_f(_when,
                         _queue,
                         handle.address(),
                         [](void* address) {
            std::experimental::coroutine_handle<Promise>::from_address(address).resume();
        });
    }
    void await_resume() {}
};

struct awaitable_queue {
    dispatch_queue_t _queue;
    bool await_ready() { return false; }
    template<typename Promise>
    void await_suspend(coroutine_handle<Promise> handle) {
        dispatch_async_f(_queue,
                         handle.address(),
                         [](void* address) {
            coroutine_handle<Promise>::from_address(address).resume();
        });
    }
    dispatch_queue_t await_resume() { return _queue; }
}; // awaitable_queue

struct awaitable_source {
    dispatch_source_t _source;
    void* _address;
    uintptr_t _data;
    bool await_ready() { return false; }
    template<typename Promise>
    void await_suspend(std::experimental::coroutine_handle<Promise> handle) {
        _address = handle.address();
        dispatch_set_context(_source, this);
        dispatch_source_set_event_handler_f(_source, [](void* context) {
            assert(context);
            auto ptr = (awaitable_source*) context;
            ptr->_data = dispatch_source_get_data(ptr->_source);
            dispatch_suspend(ptr->_source);
            dispatch_set_context(ptr->_source, nullptr);
            dispatch_source_set_event_handler_f(ptr->_source, nullptr);
            std::experimental::coroutine_handle<Promise>::from_address(ptr->_address).resume();
        });
        dispatch_resume(_source);
    }
    uintptr_t await_resume() { return _data; }
};

namespace std::experimental {

    template<typename... Args>
    struct coroutine_traits<void, Args...> {
        struct promise_type {
            
            void get_return_object() {}
            suspend_never initial_suspend() { return {}; }
            suspend_never final_suspend() { return {}; }
            void return_void() {}
            void unhandled_exception() { std::terminate(); }
            
            
        }; // promise_type
    }; // coroutine_traits<void, ...>
    
    template<typename... Args>
    struct coroutine_traits<coroutine_handle<>, Args...> {
        struct promise_type {
            coroutine_handle<> get_return_object() {
                return coroutine_handle<promise_type>::from_promise(*this);
            };
            suspend_always initial_suspend() { return {}; }
            suspend_never final_suspend() { return {}; }
            void return_void() {}
            void unhandled_exception() { std::terminate(); }
        };
    };

} // std::experimental

void foo(channel<int>& z) {
    for (int i = 0; i != 10000; ++i) {
        co_await z.push(i);
    }
    puts("bye");
}

void bar(channel<int>& z) {
    for (int i = 0; i != 10000; ++i) {
        auto j = co_await z.pop();
    }
    puts("bye");
}



int main(int argc, const char * argv[]) {

    channel<int> z;
    
    for (int i = 0; i != 400; ++i) {
        foo(z);
        bar(z);
    }

    dispatch_main();
}
