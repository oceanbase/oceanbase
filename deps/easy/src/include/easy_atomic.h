#ifndef EASY_LOCK_ATOMIC_H_
#define EASY_LOCK_ATOMIC_H_

#include "easy_define.h"
#include <stdint.h>
#include <sched.h>

/**
 * 原子操作
 */

EASY_CPP_START

#define easy_atomic_set(v,i)        ((v) = (i))
typedef volatile int32_t            easy_atomic32_t;

// 32bit
static __inline__ void easy_atomic32_add(easy_atomic32_t *v, int i)
{
    __sync_fetch_and_add(v, i);  //for x86 and arm
}
static __inline__ int32_t easy_atomic32_add_return(easy_atomic32_t *value, int32_t diff)
{
	return __sync_add_and_fetch( value, diff ) ;  // for x86 and arm
}
static __inline__ void easy_atomic32_inc(easy_atomic32_t *v)
{
	__sync_add_and_fetch( v, 1 );  // for x86 and arm
}
static __inline__ void easy_atomic32_dec(easy_atomic32_t *v)
{
	__sync_sub_and_fetch( v, 1 );  // for x86 and arm
}

// 64bit
#if __WORDSIZE == 64
typedef volatile int64_t easy_atomic_t;
static __inline__ void easy_atomic_add(easy_atomic_t *v, int64_t i)
{
	__sync_fetch_and_add(v, i);  // for x86 and arm
}
static __inline__ int64_t easy_atomic_add_return(easy_atomic_t *value, int64_t i)
{
	return __sync_add_and_fetch( value, i ) ;  // for x86 and arm
}
static __inline__ int64_t easy_atomic_cmp_set(easy_atomic_t *lock, int64_t old, int64_t set)
{
	return __sync_bool_compare_and_swap(lock, old, set);  // for x86 and arm
}
static __inline__ void easy_atomic_inc(easy_atomic_t *v)
{
	__sync_add_and_fetch( v, 1 );  //for x86 and arm
}
static __inline__ void easy_atomic_dec(easy_atomic_t *v)
{
	__sync_sub_and_fetch( v, 1 );  //for x86 and arm
}
#else
typedef volatile int32_t easy_atomic_t;
#define easy_atomic_add(v,i) easy_atomic32_add(v,i)
#define easy_atomic_add_return(v,diff) easy_atomic32_add_return(v,diff)
#define easy_atomic_inc(v) easy_atomic32_inc(v)
#define easy_atomic_dec(v) easy_atomic32_dec(v)
static __inline__ int32_t easy_atomic_cmp_set(easy_atomic_t *lock, int32_t old, int32_t set)
{
	    return __sync_bool_compare_and_swap(lock, old, set);  // for x86 and arm
}
#endif

#define easy_trylock(lock)  (*(lock) == 0 && easy_atomic_cmp_set(lock, 0, 1))
#define easy_unlock(lock)   __atomic_store_n(lock, 0, __ATOMIC_SEQ_CST)
#define easy_spin_unlock easy_unlock
#define easy_mfence() __atomic_thread_fence(__ATOMIC_SEQ_CST)
static __inline__ void easy_spin_lock(easy_atomic_t *lock)
{
    int                     i, n;

    for ( ; ; ) {
        if (*lock == 0 && easy_atomic_cmp_set(lock, 0, 1)) {
            return;
        }

        for (n = 1; n < 1024; n <<= 1) {

            for (i = 0; i < n; i++) {
#if defined(__x86_64__)
                __asm__ (".byte 0xf3, 0x90");
#elif defined(__aarch64__)
                __asm__ ("yield");  // for ARM
#else
    #error arch unsupported
#endif
            }

            if (*lock == 0 && easy_atomic_cmp_set(lock, 0, 1)) {
                return;
            }
        }

        sched_yield();
    }
}

static __inline__ void easy_clear_bit(unsigned long nr, volatile void *addr)
{
    int8_t                  *m = ((int8_t *) addr) + (nr >> 3);
    *m &= (int8_t)(~(1 << (nr & 7)));
}
static __inline__ void easy_set_bit(unsigned long nr, volatile void *addr)
{
    int8_t                  *m = ((int8_t *) addr) + (nr >> 3);
    *m |= (int8_t)(1 << (nr & 7));
}

typedef struct easy_spinrwlock_t {
    easy_atomic_t           ref_cnt;
    easy_atomic_t           wait_write;
} easy_spinrwlock_t;
#define EASY_SPINRWLOCK_INITIALIZER {0, 0}
static __inline__ int easy_spinrwlock_rdlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        int                     cond = 1;

        while (cond) {
            int                     loop = 1;

            do {
                easy_atomic_t           oldv = lock->ref_cnt;

                if (0 <= oldv
                        && 0 == lock->wait_write) {
                    easy_atomic_t           newv = oldv + 1;

                    if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                        cond = 0;
                        break;
                    }
                }

#if defined(__x86_64__)
                asm("pause");
#elif defined(__aarch64__)
                asm("yield");  // for ARM
#else
    #error arch unsupported
#endif
                loop <<= 1;
            } while (loop < 1024);

            sched_yield();
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_wrlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        int                     cond = 1;
        easy_atomic_inc(&lock->wait_write);

        while (cond) {
            int                     loop = 1;

            do {
                easy_atomic_t           oldv = lock->ref_cnt;

                if (0 == oldv) {
                    easy_atomic_t           newv = -1;

                    if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                        cond = 0;
                        break;
                    }
                }

#if defined(__x86_64__)
                asm("pause");
#elif defined(__aarch64__)
                asm("yield");  // for ARM
#else
    #error arch unsupported
#endif

                loop <<= 1;
            } while (loop < 1024);

            sched_yield();
        }

        easy_atomic_dec(&lock->wait_write);
    }

    return ret;
}
static __inline__ int easy_spinrwlock_try_rdlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        ret = EASY_AGAIN;
        easy_atomic_t           oldv = lock->ref_cnt;

        if (0 <= oldv
                && 0 == lock->wait_write) {
            easy_atomic_t           newv = oldv + 1;

            if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                ret = EASY_OK;
            }
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_try_wrlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        ret = EASY_AGAIN;
        easy_atomic_t           oldv = lock->ref_cnt;

        if (0 == oldv) {
            easy_atomic_t           newv = -1;

            if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                ret = EASY_OK;
            }
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_unlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        while (1) {
            easy_atomic_t           oldv = lock->ref_cnt;

            if (-1 == oldv) {
                easy_atomic_t           newv = 0;

                if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                    break;
                }
            } else if (0 < oldv) {
                easy_atomic_t           newv = oldv - 1;

                if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                    break;
                }
            } else {
                ret = EASY_ERROR;
                break;
            }
        }
    }

    return ret;
}

EASY_CPP_END

#endif
