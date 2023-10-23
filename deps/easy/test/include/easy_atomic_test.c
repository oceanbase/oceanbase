#include "easy_atomic.h"
#include <pthread.h>
#include <easy_test.h>

/**
 * 测试 easy_atomic.h
 */
#define TEST_ATOMIC_ADD32(v1, init_value, add_value) \
    {v1 = (init_value); \
        easy_atomic32_add(&v1, (add_value)); \
        EXPECT_EQ(v1, (int32_t)((int64_t)(init_value) + (add_value))); \
        v1 = (init_value); \
        typeof(v1) vv = easy_atomic32_add_return(&v1, add_value);\
        EXPECT_EQ(vv, (int32_t)((int64_t)(init_value) + (add_value))); \
        EXPECT_EQ(v1, (int32_t)((int64_t)(init_value) + (add_value)));}

#define TEST_ATOMIC_ADD(v2, init_value, add_value) \
    {v2 = (easy_atomic_t)(init_value); \
        int64_t                 cv = v2 + (easy_atomic_t)add_value; \
        easy_atomic_add(&v2, (easy_atomic_t)(add_value)); \
        EXPECT_EQ(v2, cv); \
        v2 = (easy_atomic_t)(init_value); \
        EXPECT_EQ(easy_atomic_add_return(&v2, (easy_atomic_t)add_value), cv); \
        EXPECT_EQ(v2, cv);}

#define TEST_ATOMIC_CMP_SET(v3, init_value, set_value) \
    v3 = (easy_atomic_t)(init_value); \
    if (init_value != set_value) EXPECT_EQ(easy_atomic_cmp_set(&v3, set_value, init_value), 0); \
    EXPECT_EQ(easy_atomic_cmp_set(&v3, init_value, set_value), 1); \
    EXPECT_EQ(v3, (easy_atomic_t)(set_value));

#define TEST_ATOMIC_INC(i, v3, start, end) \
    for (v3 = i = (start); i < (end);) { \
        easy_atomic_inc(&v3); \
        i ++; \
        EXPECT_EQ(v3, i); }

#define TEST_ATOMIC_DEC(i, v3, start, end) \
    for (v3 = i = (start); i > (end);) { \
        easy_atomic_dec(&v3); \
        i --; \
        EXPECT_EQ(v3, i); }

#define TEST_ATOMIC_INC32(i, v4, start, end) \
    for (v4 = i = (start); i < (end);) { \
        easy_atomic32_inc(&v4); \
        i ++; \
        EXPECT_EQ(v4, i); }

#define TEST_ATOMIC_DEC32(i, v3, start, end) \
    for (v3 = i = (start); i > (end);) { \
        easy_atomic32_dec(&v3); \
        i --; \
        EXPECT_EQ(v3, i); }

#define INT32_MAX_VALUE 0x7fffffff
#define INT32_MIN_VALUE 0x80000000
#if __WORDSIZE == 64
#define INT64_MAX_VALUE 0x7fffffffffffffff
#define INT64_MIN_VALUE 0x8000000000000000
#else
#define INT64_MAX_VALUE INT32_MAX_VALUE
#define INT64_MIN_VALUE INT32_MIN_VALUE
#endif

TEST(easy_atomic, func)
{
    easy_atomic32_t         v1 = 0;
    easy_atomic_t           v2 = 0;

    easy_atomic32_add(&v1, 1);
    EXPECT_EQ(v1, 1);
    easy_atomic32_add(&v1, 10);
    EXPECT_EQ(v1, 11);
    EXPECT_EQ(easy_atomic32_add_return(&v1, 100), 111);
    EXPECT_EQ(v1, 111);

    TEST_ATOMIC_ADD32(v1, INT32_MAX_VALUE, INT32_MIN_VALUE); // 2147483647 + -2147483648 = -1
    TEST_ATOMIC_ADD32(v1, INT32_MAX_VALUE, INT32_MAX_VALUE); // 2147483647 + 2147483647 = -2
    TEST_ATOMIC_ADD32(v1, 0x0, INT32_MIN_VALUE);  // 0 + -2147483648 = -2147483648
    TEST_ATOMIC_ADD32(v1, 0x0, INT32_MAX_VALUE);  // 0 + 2147483647 = 2147483647
    TEST_ATOMIC_ADD32(v1, INT32_MIN_VALUE, INT32_MIN_VALUE);  // -2147483648 + -2147483648 = 0
    TEST_ATOMIC_ADD32(v1, INT32_MIN_VALUE, INT32_MAX_VALUE);  // -2147483648 + 2147483647 = -1

    easy_atomic_add(&v2, 1);
    EXPECT_EQ(v2, 1);
    easy_atomic_add(&v2, 10);
    EXPECT_EQ(v2, 11);
    EXPECT_EQ(easy_atomic_add_return(&v2, 100), 111);
    EXPECT_EQ(v2, 111);

    EXPECT_EQ(easy_atomic_cmp_set(&v2, 110, 1), 0);
    EXPECT_EQ(v2, 111);
    EXPECT_EQ(easy_atomic_cmp_set(&v2, 111, 1), 1);
    EXPECT_EQ(v2, 1);

    TEST_ATOMIC_ADD(v2, INT64_MAX_VALUE, INT64_MIN_VALUE);
    //TEST_ATOMIC_ADD(v2, INT64_MAX_VALUE, INT64_MAX_VALUE);
    TEST_ATOMIC_ADD(v2, 0x0, INT64_MIN_VALUE);
    TEST_ATOMIC_ADD(v2, 0x0, INT64_MAX_VALUE);
    TEST_ATOMIC_ADD(v2, INT64_MIN_VALUE, INT64_MIN_VALUE);
    TEST_ATOMIC_ADD(v2, INT64_MIN_VALUE, INT64_MAX_VALUE);

    TEST_ATOMIC_CMP_SET(v2, INT64_MAX_VALUE, INT64_MIN_VALUE);
    TEST_ATOMIC_CMP_SET(v2, INT64_MAX_VALUE, INT64_MAX_VALUE);
    TEST_ATOMIC_CMP_SET(v2, 0x0, INT64_MIN_VALUE);
    TEST_ATOMIC_CMP_SET(v2, 0x0, INT64_MAX_VALUE);
    TEST_ATOMIC_CMP_SET(v2, INT64_MIN_VALUE, INT64_MIN_VALUE);
    TEST_ATOMIC_CMP_SET(v2, INT64_MIN_VALUE, INT64_MAX_VALUE);

    EXPECT_EQ(easy_trylock(&v2), 0);
    easy_unlock(&v2);
    EXPECT_EQ(v2, 0);

    EXPECT_EQ(easy_trylock(&v2), 1);
    EXPECT_EQ(v2, 1);
    easy_unlock(&v2);
    EXPECT_EQ(v2, 0);

    easy_spin_lock(&v2);
    EXPECT_EQ(v2, 1);
    EXPECT_EQ(easy_trylock(&v2), 0);
    easy_unlock(&v2);
    EXPECT_EQ(v2, 0);

    // 64 bit, atomic inc and dec
    int64_t                 i = 0;
    easy_atomic_t           v3 = 0;
    TEST_ATOMIC_INC(i, v3, -1000, 1000);
    TEST_ATOMIC_DEC(i, v3, 1000, -1000);
    easy_atomic_t           v3_t = INT64_MAX_VALUE;
    easy_atomic_add_return(&v3_t, 1000);
    TEST_ATOMIC_INC(i, v3, INT64_MAX_VALUE - 1000, v3_t);
    TEST_ATOMIC_DEC(i, v3, v3_t, INT64_MAX_VALUE - 1000);

    // 32 bit, atomic inc and dec
    easy_atomic32_t         v4 = 0;
    TEST_ATOMIC_INC32(i, v4, -1000, 1000);
    TEST_ATOMIC_DEC32(i, v4, 1000, -1000);
    easy_atomic32_t         v4_t = INT32_MAX_VALUE;
    easy_atomic32_add_return(&v4_t, 1000);
    TEST_ATOMIC_INC32(i, v4, INT32_MAX_VALUE - 1000, v4_t);
    TEST_ATOMIC_DEC32(i, v4, v4_t, INT32_MAX_VALUE - 1000);
}

typedef struct easy_atomic_mt_args_t {
    easy_atomic_t           lock;
    int64_t                 loop_count;
    easy_atomic_t           v1;
    int64_t                 v2;
    int64_t                 sleep_value;
} easy_atomic_mt_args_t;

void *easy_atomic_mthread_start(void *args)
{
    int                     i;
    easy_atomic_mt_args_t   *p = (easy_atomic_mt_args_t *)args;

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic_add(&p->v1, 1);
    }

    volatile int            t;

    for(i = 0; i < p->loop_count; i++) {
        easy_spin_lock(&p->lock);
        t = p->v2;
        p->v2 = t + 1;
        easy_spin_unlock(&p->lock);
    }

    struct timespec         req;

    struct timespec         rem;

    req.tv_sec = 0;

    req.tv_nsec = 1000000;

    {
        easy_spin_lock(&p->lock);
        t = p->sleep_value;
        EXPECT_EQ(nanosleep(&req, &rem), 0);
        p->sleep_value = t + 1;
        easy_spin_unlock(&p->lock);
    }

    return (void *)NULL;
}

TEST(easy_atomic, mthread)
{
    const int               thread_count = 10;
    pthread_t               tids[thread_count];
    int                     i;
    easy_atomic_mt_args_t   param;
    param.lock = 0;
    param.loop_count = 50000;
    param.v1 = 0;
    param.v2 = 0;
    param.sleep_value = 0;

    for(i = 0; i < thread_count; i++) {
        pthread_create(&tids[i], NULL, easy_atomic_mthread_start, &param);
    }

    for(i = 0; i < thread_count; i++) {
        pthread_join(tids[i], NULL);
    }

    EXPECT_EQ(param.v1, param.loop_count * thread_count);
    EXPECT_EQ(param.v2, param.loop_count * thread_count);
    EXPECT_EQ(param.sleep_value, thread_count);
}

void *easy_atomic_mthread_trylock_start(void *args)
{
    int                     i;
    easy_atomic_mt_args_t   *p = (easy_atomic_mt_args_t *)args;

    volatile int            t;

    for(i = 0; i < p->loop_count;) {
        if (easy_trylock(&p->lock)) {
            t = p->v2;
            p->v2 = t + 1;
            easy_unlock(&p->lock);
            i ++;
        }
    }

    /*
    struct timespec         req;

    struct timespec         rem;

    req.tv_sec = 0;

    req.tv_nsec = 1000000;
    */

    while(1) {
        if (easy_trylock(&p->lock) == 0)
            continue;

        int                     t = p->v1;
        //EXPECT_EQ(nanosleep(&req, &rem), 0);
        p->v1 = t + 1;
        break;
    }

    easy_unlock(&p->lock);

    return (void *)NULL;
}

TEST(easy_atomic, mthread_trylock)
{
    const int               thread_count = 10;
    pthread_t               tids[thread_count];
    int                     i;
    easy_atomic_mt_args_t   param;
    param.lock = 0;
    param.loop_count = 10000;
    param.v1 = 0;
    param.v2 = 0;

    for(i = 0; i < thread_count; i++) {
        pthread_create(&tids[i], NULL, easy_atomic_mthread_trylock_start, &param);
    }

    for(i = 0; i < thread_count; i++) {
        pthread_join(tids[i], NULL);
    }

    EXPECT_EQ(param.v1, thread_count);
    EXPECT_EQ(param.v2, param.loop_count * thread_count);
}

typedef struct easy_atomic_mt_op_args_t {
    easy_atomic_t           int64_add;
    easy_atomic_t           int64_add_return;
    easy_atomic_t           int64_inc;
    easy_atomic_t           int64_dec;
    easy_atomic_t           int64_set;
    easy_atomic_t           int64_set_sum;
    easy_atomic_t           int64_sum;
    easy_atomic32_t         int32_add;
    easy_atomic32_t         int32_add_return;
    easy_atomic32_t         int32_inc;
    easy_atomic32_t         int32_dec;
    easy_atomic32_t         int32_sum;
    int64_t                 loop_count;
} easy_atomic_mt_op_args_t;

void *easy_atomic_mthread_op_start(void *args)
{
    easy_atomic_mt_op_args_t *p = (easy_atomic_mt_op_args_t *)args;
    int                     i;

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic_add(&p->int64_add, 1);
        easy_atomic_add(&p->int64_sum, 1);
    }

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic_add_return(&p->int64_add_return, 1);
        easy_atomic_add_return(&p->int64_sum, 1);
    }

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic_inc(&p->int64_inc);
        easy_atomic_inc(&p->int64_sum);
    }

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic_dec(&p->int64_dec);
        easy_atomic_dec(&p->int64_sum);
    }

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic32_add(&p->int32_add, 1);
        easy_atomic32_add(&p->int32_sum, 1);
    }

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic32_add_return(&p->int32_add_return, 1);
        easy_atomic32_add_return(&p->int32_sum, 1);
    }

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic32_inc(&p->int32_inc);
        easy_atomic32_inc(&p->int32_sum);
    }

    for(i = 0; i < p->loop_count; i++) {
        easy_atomic32_dec(&p->int32_dec);
        easy_atomic32_dec(&p->int32_sum);
    }

    int                     j = 0;
    int                     t = 0;
    easy_atomic_t           v;

    for(j = 0; j < p->loop_count; j++) {
        do {
            v = p->int64_set;
            t = easy_atomic_cmp_set(&p->int64_set, v, v + 1);
        } while(t == 0);

        easy_atomic_add(&p->int64_set_sum, t);
    }

    return (void *)NULL;
}

TEST(easy_atomic, mthread_atomic_op)
{
    const int               thread_count = 10;
    pthread_t               tids[thread_count];
    int                     i;
    easy_atomic_mt_op_args_t param;
    memset(&param, 0, sizeof(param));
    param.loop_count = 10000;

    for(i = 0; i < thread_count; i++) {
        pthread_create(&tids[i], NULL, easy_atomic_mthread_op_start, &param);
    }

    for(i = 0; i < thread_count; i++) {
        pthread_join(tids[i], NULL);
    }

    EXPECT_EQ(param.int64_add, param.loop_count * thread_count);
    EXPECT_EQ(param.int64_add_return, param.loop_count * thread_count);
    EXPECT_EQ(param.int64_inc, param.loop_count * thread_count);
    EXPECT_EQ(param.int64_dec,  - param.loop_count * thread_count);
    EXPECT_EQ(param.int64_sum, param.loop_count * 2 * thread_count);

    EXPECT_EQ(param.int32_add, param.loop_count * thread_count);
    EXPECT_EQ(param.int32_add_return, param.loop_count * thread_count);
    EXPECT_EQ(param.int32_inc, param.loop_count * thread_count);
    EXPECT_EQ(param.int32_dec, - param.loop_count * thread_count);
    EXPECT_EQ(param.int32_sum, param.loop_count * 2 * thread_count);

    EXPECT_EQ(param.int64_set_sum, param.loop_count * thread_count);
}

TEST(easy_atomic, easy_bit)
{
    uint64_t                x = 0;
    int                     i;

    for(i = 0; i < 8; i++) easy_set_bit(i * 8, &x);

    EXPECT_EQ(x, __INT64_C(0x0101010101010101));

    for(i = 0; i < 8; i++) easy_set_bit(i * 8 + 7, &x);

    EXPECT_EQ(x, __INT64_C(0x8181818181818181));

    for(i = 0; i < 8; i++) easy_clear_bit(i * 8 + 7, &x);

    EXPECT_EQ(x, __INT64_C(0x0101010101010101));

    for(i = 0; i < 8; i++) easy_clear_bit(i * 8, &x);

    EXPECT_EQ(x, 0);
}

TEST(easy_atomic, easy_spinrwlock)
{
    easy_spinrwlock_t       lock = EASY_SPINRWLOCK_INITIALIZER;

    int                     ret = easy_spinrwlock_rdlock(&lock);
    EXPECT_EQ(EASY_OK, ret);
    ret = easy_spinrwlock_unlock(&lock);
    EXPECT_EQ(EASY_OK, ret);

    ret = easy_spinrwlock_wrlock(&lock);
    EXPECT_EQ(EASY_OK, ret);
    ret = easy_spinrwlock_unlock(&lock);
    EXPECT_EQ(EASY_OK, ret);

    ret = easy_spinrwlock_rdlock(&lock);
    EXPECT_EQ(EASY_OK, ret);
    ret = easy_spinrwlock_try_rdlock(&lock);
    EXPECT_EQ(EASY_OK, ret);
    ret = easy_spinrwlock_unlock(&lock);
    EXPECT_EQ(EASY_OK, ret);
    ret = easy_spinrwlock_try_wrlock(&lock);
    EXPECT_EQ(EASY_AGAIN, ret);
    ret = easy_spinrwlock_unlock(&lock);
    EXPECT_EQ(EASY_OK, ret);

    ret = easy_spinrwlock_wrlock(&lock);
    EXPECT_EQ(EASY_OK, ret);
    ret = easy_spinrwlock_try_rdlock(&lock);
    EXPECT_EQ(EASY_AGAIN, ret);
    ret = easy_spinrwlock_try_wrlock(&lock);
    EXPECT_EQ(EASY_AGAIN, ret);
    ret = easy_spinrwlock_unlock(&lock);
    EXPECT_EQ(EASY_OK, ret);
}

