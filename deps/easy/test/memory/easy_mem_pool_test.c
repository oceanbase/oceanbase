#include <pthread.h>
#include "memory/easy_mem_pool.h"
#include <easy_test.h>

/**
 * 测试 easy_mem_pool
 */

#if __WORDSIZE == 64 && __GNUC__ >= 4
#define ATOMIC_ADD(val, addv) (void)__sync_fetch_and_add((val), addv)

const int64_t           thread_num = 4;
const int64_t           limit = 10 * 1024 * 1024;
volatile int64_t        ref_cnt_free = 0;
volatile int64_t        ref_cnt_assert = 0;
volatile int64_t        ref_cnt_total = 0;

typedef struct buf_t {
    struct buf_t            *next;
} buf_t;

typedef void (*set_memlimit_pt)(int64_t);
typedef void *(*realloc_pt)(void *, size_t);
typedef int64_t (*get_memtotal_pt)(void);

void test_realloc(realloc_pt mp_realloc, get_memtotal_pt mp_get_memtotal, int64_t size, int64_t add_size, char checker)
{
    char                    *old_buf = (char *)easy_mempool_global_realloc(NULL, size);
    EXPECT_TRUE(NULL != old_buf);
    memset(old_buf, checker, size);

    char                    *new_buf = (char *)easy_mempool_global_realloc(old_buf, size + add_size);
    EXPECT_TRUE(NULL != new_buf);
    EXPECT_NE(old_buf, new_buf);
    int64_t                 i = 0;

    for (i = 0; i < size; i++) {
        EXPECT_EQ(checker, new_buf[i]);
    }

    easy_mempool_global_realloc(new_buf, 0);
}

void test_dup_realloc(realloc_pt mp_realloc, get_memtotal_pt mp_get_memtotal)
{
    const int64_t           max_size = EASY_MEMPOOL_PAGE_SIZE;
    int64_t                 i = 1;
    int8_t                  c = 0;

    for (i = max_size - 32; i < max_size + 32; i++) {
        test_realloc(mp_realloc, mp_get_memtotal, i, 1, c++);
    }
}

void test_limit(set_memlimit_pt mp_set_memlimit, realloc_pt mp_realloc, get_memtotal_pt mp_get_memtotal, int64_t volatile *total)
{
    int64_t                 size = 2 * sizeof(buf_t);
    buf_t                   *head = NULL;
    mp_set_memlimit(limit);

    while (1) {
        buf_t                   *buf = (buf_t *)mp_realloc(NULL, size);

        if (NULL == buf) {
            break;
        }

        buf->next = head;
        head = buf;
        *(buf_t *)((char *)buf + size - sizeof(buf_t)) = *buf;
        ATOMIC_ADD(total, size);
        size += 1;
    }

    ATOMIC_ADD(&ref_cnt_assert, 1);
    ATOMIC_ADD(&ref_cnt_total, -1);

    while (0 != ref_cnt_total);

    EXPECT_TRUE(*total + size > limit);
    EXPECT_EQ(*total, mp_get_memtotal());
    ATOMIC_ADD(&ref_cnt_free, 1);
    ATOMIC_ADD(&ref_cnt_assert, -1);

    while (0 != ref_cnt_assert);

    buf_t                   *iter = head;

    while (NULL != iter) {
        size -= 1;
        buf_t                   *next = iter->next;
        buf_t                   *check = (buf_t *)((char *)iter + size - sizeof(buf_t));
        EXPECT_TRUE(0 == memcmp(check, iter, sizeof(buf_t)));
        mp_realloc(iter, 0);
        ATOMIC_ADD(total, 0 - size);
        iter = next;
    }

    EXPECT_EQ(2 * sizeof(buf_t), size);
    ATOMIC_ADD(&ref_cnt_free, -1);

    while (0 != ref_cnt_free);

    EXPECT_EQ(0, *total);
    EXPECT_EQ(2 * sizeof(buf_t), size);
}

void *test_set_thread_memlimit_thread_func(void *data)
{
    int64_t volatile        *total_ptr = (int64_t volatile *)data;
    test_limit(easy_mempool_set_thread_memlimit, easy_mempool_thread_realloc,
               easy_mempool_get_thread_memtotal, total_ptr);
    return NULL;
}

void *test_thread_realloc_thread_func(void *data)
{
    test_dup_realloc(easy_mempool_thread_realloc, easy_mempool_get_thread_memtotal);
    return NULL;
}

void *test_set_global_memlimit_thread_func(void *data)
{
    int64_t volatile        *total_ptr = (int64_t volatile *)data;
    test_limit(easy_mempool_set_global_memlimit, easy_mempool_global_realloc,
               easy_mempool_get_global_memtotal, total_ptr);
    return NULL;
}

void *test_global_realloc_thread_func(void *data)
{
    test_dup_realloc(easy_mempool_global_realloc, easy_mempool_get_global_memtotal);
    return NULL;
}

typedef struct buf_list_t {
    volatile buf_t          *head;
    buf_t                   *tail;
    int64_t                 total;
    pthread_spinlock_t      lock;
    easy_mempool_t          *pool;
} buf_list_t;

void *producer(void *data)
{
    void                    *tmp = easy_mempool_thread_realloc(NULL, 1);
    buf_list_t              *list = (buf_list_t *)data;
    int                     loop = 1;
    uint32_t                seed = 0;

    while (loop) {
        int32_t                 size = rand_r(&seed) % (32 * 1024) + sizeof(buf_t);
        buf_t                   *buf = (buf_t *)easy_mempool_alloc(list->pool, size);

        //buf_t *buf = easy_mempool_global_realloc(NULL, size);
        //buf_t *buf = easy_mempool_thread_realloc(NULL, size);
        if (NULL != buf) {
            //memset(buf, 0, sizeof(buf_t));
            pthread_spin_lock(&(list->lock));
            buf->next = NULL;

            if (NULL != list->tail) {
                list->tail->next = buf;
            }

            list->tail = buf;

            if (NULL == list->head) {
                list->head = list->tail;
            }

            list->total--;

            if (0 == list->total) {
                loop = 0;
            }

            pthread_spin_unlock(&(list->lock));
        } else {
            //usleep(1);
        }
    }

    easy_mempool_thread_realloc(tmp, 0);
    return NULL;
}

void *consummer(void *data)
{
    buf_list_t              *list = (buf_list_t *)data;
    int                     loop = 1;

    while (loop) {
        buf_t                   *buf = NULL;

        if (NULL == list->head
                && 0 != list->total) {
            continue;
        }

        pthread_spin_lock(&(list->lock));
        buf = (buf_t *)(list->head);

        if (NULL != buf) {
            list->head = buf->next;

            if (buf == list->tail) {
                list->tail = NULL;
            }
        } else {
            if (0 == list->total) {
                loop = 0;
            }
        }

        pthread_spin_unlock(&(list->lock));

        if (NULL != buf) {
            easy_mempool_free(list->pool, buf);
            //easy_mempool_global_realloc(buf, 0);
            //easy_mempool_thread_realloc(buf, 0);
        }
    }

    return NULL;
}

void test_thread_press(int64_t total, int64_t thread)
{
    buf_list_t              *list = (buf_list_t *)easy_malloc(sizeof(buf_list_t) * thread);
    pthread_t               *p_pd = (pthread_t *)easy_malloc(sizeof(pthread_t) * thread);
    pthread_t               *c_pd = (pthread_t *)easy_malloc(sizeof(pthread_t) * thread);
    easy_mempool_t          *pool = easy_mempool_create(0);
    int64_t                 i = 0;

    for (i = 0; i < thread; i++) {
        list[i].head = NULL;
        list[i].tail = NULL;
        list[i].total = total;
        pthread_spin_init(&(list[i].lock), PTHREAD_PROCESS_PRIVATE);
        list[i].pool = pool;
        pthread_create(&(p_pd[i]), NULL, producer, &list[i]);
    }

    for (i = 0; i < thread; i++) {
        pthread_create(&(c_pd[i]), NULL, consummer, &list[i]);
    }

    for (i = 0; i < thread; i++) {
        pthread_join(p_pd[i], NULL);
    }

    for (i = 0; i < thread; i++) {
        pthread_join(c_pd[i], NULL);
    }

    EXPECT_EQ(0, easy_mempool_get_memtotal(pool));
    easy_mempool_destroy(pool);
    easy_free(c_pd);
    easy_free(p_pd);
}

TEST(easy_mem_pool, set_global_memlimit)
{
    volatile int64_t        total = 0;
    int64_t                 i = 0;
    ref_cnt_total = thread_num;
    pthread_t               pd[thread_num];

    for (i = 0; i < thread_num; i++) {
        pthread_create(&(pd[i]), NULL, test_set_global_memlimit_thread_func, (void *)&total);
    }

    for (i = 0; i < thread_num; i++) {
        pthread_join(pd[i], NULL);
    }

    EXPECT_EQ(0, easy_mempool_get_global_memtotal());
}

TEST(easy_mem_pool, set_thread_memlimit)
{
    volatile int64_t        total = 0;
    int64_t                 i = 0;
    ref_cnt_total = thread_num;
    pthread_t               pd[thread_num];

    for (i = 0; i < thread_num; i++) {
        pthread_create(&(pd[i]), NULL, test_set_thread_memlimit_thread_func, (void *)&total);
    }

    for (i = 0; i < thread_num; i++) {
        pthread_join(pd[i], NULL);
    }

    EXPECT_EQ(0, easy_mempool_get_thread_memtotal());
}

TEST(easy_mem_pool, global_realloc)
{
    int64_t                 i = 0;
    const int64_t           thread_num = 4;
    pthread_t               pd[thread_num];

    for (i = 0; i < thread_num; i++) {
        pthread_create(&(pd[i]), NULL, test_global_realloc_thread_func, NULL);
    }

    for (i = 0; i < thread_num; i++) {
        pthread_join(pd[i], NULL);
    }

    EXPECT_EQ(0, easy_mempool_get_global_memtotal());
}

TEST(easy_mem_pool, thread_realloc)
{
    int64_t                 i = 0;
    const int64_t           thread_num = 4;
    pthread_t               pd[thread_num];

    for (i = 0; i < thread_num; i++) {
        pthread_create(&(pd[i]), NULL, test_thread_realloc_thread_func, NULL);
    }

    for (i = 0; i < thread_num; i++) {
        pthread_join(pd[i], NULL);
    }

    EXPECT_EQ(0, easy_mempool_get_thread_memtotal());
}

TEST(easy_mem_pool, alloc_limit)
{
    easy_mempool_t          *pool = easy_mempool_create(0);

    int64_t                 i = 0;
    int64_t                 total = 0;
    int64_t                 limit = 1024 * 1024;
    buf_t                   *head = NULL;
    easy_mempool_set_memlimit(pool, limit);

    for (i = 0; total < limit; i++) {
        int32_t                 alloc_size = sizeof(buf_t) + i;
        buf_t                   *buf = (buf_t *)easy_mempool_alloc(pool, alloc_size);

        if (total + alloc_size > limit) {
            assert(NULL == buf);
        } else {
            assert(NULL != buf);
        }

        if (NULL == buf) {
            break;
        }

        total += alloc_size;
        buf->next = head;
        head = buf;
    }

    buf_t                   *iter = head;

    while (NULL != iter) {
        buf_t                   *next = iter->next;
        easy_mempool_free(pool, iter);
        iter = next;
    }

    easy_mempool_destroy(pool);
}

TEST(easy_mem_pool, thread_press)
{
    test_thread_press(1000000, 2);
}
#endif

