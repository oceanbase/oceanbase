#include <assert.h>
#include <stdio.h>
#include <malloc.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdint.h>
#include "memory/easy_mem_pool.h"

#if __GNUC__ >= 4
/**
 * mem buffer
 */

// 私有数据结构

enum {
    EASY_MEMPOOL_ALLOC = 0,     // 内存由pool申请
    EASY_MEMPOOL_DIRECT_ALLOC = 1,   // 内存直接从allocator申请
};

typedef union easy_mempool_atomic_t {
    volatile uint64_t       atomic;
    struct {
        volatile int32_t        ref_cnt;
        volatile int32_t        seq_num;
    };
} easy_mempool_atomic_t;

// page结构 后面直接跟page内存
typedef struct easy_mempool_page_t {
    volatile int32_t        base;
    int32_t                 reserve;
    struct easy_mempool_page_t *next;
} easy_mempool_page_t;

// 管理page的元数据结构 16Byte
typedef struct easy_mempool_page_meta_t {
    union {
        volatile uint64_t       atomic;
        struct {
            volatile int32_t        ref_cnt;
            volatile int32_t        seq_num;
        };
    };
    easy_mempool_page_t *volatile page;
} easy_mempool_page_meta_t;

// 分配内存的头部结构 16Byte
typedef struct easy_mempool_buf_t {
    uint16_t                magic_num;
    uint8_t                 alloc_type;
    uint8_t                 reserve1;
    uint16_t                page_pos;
    uint32_t                size;
    uint32_t                reserve2;
} easy_mempool_buf_t;

// 一个pool结构
struct easy_mempool_t {
    volatile int32_t        cur_page_pos;
    volatile int32_t        direct_alloc_cnt;
    volatile int64_t        mem_total;
    int64_t                 mem_limit;

    easy_mempool_allocator_t *allocator;

    int32_t                 page_size;
    int32_t                 page_num;
    easy_mempool_page_meta_t *page_metas;

    int32_t                 free_num;
    int32_t                 reserve2;
    easy_mempool_page_t     *free_list;
    pthread_spinlock_t      free_list_lock;
};

typedef struct easy_mempool_thread_info_t {
    volatile int64_t        ref_cnt;
    easy_mempool_t          *pool;
} easy_mempool_thread_info_t;

easy_mempool_allocator_t easy_mempool_g_allocator = {memalign, free};

static int64_t          EASY_MEMPOOL_BUF_MAGIC_NUM = 0xabcd;
static int64_t          EASY_MEMPOOL_BUF_FREE_FLAG = 0Xef12;
static easy_mempool_t   *easy_mempool_g_pool = NULL;
static pthread_key_t    easy_mempool_g_thread_key = INT32_MAX;
static int64_t          easy_mempool_g_thread_memlimit = INT64_MAX;
static volatile int64_t easy_mempool_g_thread_memtotal = 0;

////////////////////////////////////////////////////////////////////////////////////////////////////

// 私有方法

#define ATOMIC_INC(val) (void)__sync_add_and_fetch((val), 1)
#define ATOMIC_INC_FETCH(val) __sync_add_and_fetch((val), 1)
#define ATOMIC_DEC(val) (void)__sync_sub_and_fetch((val), 1)
#define ATOMIC_DEC_FETCH(val) __sync_sub_and_fetch((val), 1)
#define ATOMIC_ADD(val, addv) (void)__sync_fetch_and_add((val), addv)
#define ATOMIC_SUB(val, addv) (void)__sync_fetch_and_sub((val), addv)
#define ATOMIC_CAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))

static void *easy_mempool_alloc_(easy_mempool_t *pool, uint32_t size, uint32_t align_size);

static void easy_mempool_destroy_free_list_(easy_mempool_t *pool);
static easy_mempool_page_t *easy_mempool_alloc_page_(easy_mempool_t *pool);
static void easy_mempool_free_page_(easy_mempool_t *pool, easy_mempool_page_t *page);

static easy_mempool_page_t *easy_mempool_get_cur_page_(easy_mempool_t *pool, int32_t ensure_size, int32_t *page_pos);
static void *easy_mempool_alloc_from_page_(easy_mempool_page_t *page, int32_t page_size, int32_t alloc_size);

static void easy_mempool_deref_page_(easy_mempool_t *pool, int32_t page_pos);
static int32_t easy_mempool_dec_ref_cnt_and_inc_seq_num_(easy_mempool_page_meta_t *page_meta);

static void easy_mempool_thread_destroy_callback_(void *data);

////////////////////////////////////////////////////////////////////////////////////////////////////

void easy_mempool_set_global_memlimit(int64_t limit)
{
    easy_mempool_set_memlimit(easy_mempool_g_pool, limit);
}

int64_t easy_mempool_get_global_memtotal()
{
    return easy_mempool_get_memtotal(easy_mempool_g_pool);
}

void *easy_mempool_global_realloc(void *ptr, size_t size)
{
    void                    *ret = NULL;

    if (0 != size) {
        ret = easy_mempool_alloc(easy_mempool_g_pool, size);
    }

    if (NULL != ptr && NULL != ret) {
        easy_mempool_buf_t      *buf = (easy_mempool_buf_t *)((char *)ptr - sizeof(easy_mempool_buf_t));

        if (NULL == buf) {
            easy_mempool_alloc((easy_mempool_t *)ret, 0);
        } else {
            memcpy(ret, ptr, buf->size > size ? size : buf->size);
        }
    }

    easy_mempool_free(easy_mempool_g_pool, ptr);
    return ret;
}

void easy_mempool_set_thread_memlimit(int64_t limit)
{
    if (0 < limit) {
        easy_mempool_g_thread_memlimit = limit;
    }
}

int64_t easy_mempool_get_thread_memtotal()
{
    return easy_mempool_g_thread_memtotal;
}

void *easy_mempool_thread_realloc(void *ptr, size_t size)
{
    void                    *ret = NULL;
    void                    *alloc_ptr = NULL;

    easy_mempool_thread_info_t *thread_info = (easy_mempool_thread_info_t *)pthread_getspecific(easy_mempool_g_thread_key);

    if (NULL == thread_info) {
        thread_info = (easy_mempool_thread_info_t *)easy_mempool_g_allocator.memalign(EASY_MEMPOOL_ALIGNMENT,
                      sizeof(easy_mempool_thread_info_t));

        if (NULL != thread_info) {
            thread_info->ref_cnt = 1;
            thread_info->pool = easy_mempool_create(0);
        }

        pthread_setspecific(easy_mempool_g_thread_key, thread_info);
    }

    if (0 != size
            && NULL != thread_info
            && easy_mempool_g_thread_memlimit >= (int64_t)(easy_mempool_g_thread_memtotal + size)) {
        alloc_ptr = easy_mempool_alloc(thread_info->pool, size + sizeof(easy_mempool_thread_info_t *));

        if (NULL != alloc_ptr) {
            *(easy_mempool_thread_info_t **)(alloc_ptr) = thread_info;
            ret = (char *)alloc_ptr + sizeof(easy_mempool_thread_info_t *);
            ATOMIC_INC(&(thread_info->ref_cnt));
            ATOMIC_ADD(&easy_mempool_g_thread_memtotal, size);
        }
    }

    if (NULL != ptr && NULL != ret) {
        easy_mempool_buf_t      *buf = (easy_mempool_buf_t *)((char *)ptr -
                                       sizeof(easy_mempool_thread_info_t *) - sizeof(easy_mempool_buf_t));

        if (NULL == buf) {
            easy_mempool_free(thread_info->pool, alloc_ptr);
        } else {
            memcpy(ret, ptr, buf->size > size ? size : buf->size);
        }
    }

    if (NULL != ptr) {
        ptr = (char *)ptr - sizeof(easy_mempool_thread_info_t *);
        easy_mempool_buf_t      *buf = (easy_mempool_buf_t *)((char *)ptr - sizeof(easy_mempool_buf_t));
        easy_mempool_thread_info_t *host = *(easy_mempool_thread_info_t **)ptr;

        if (NULL != buf) {
            ATOMIC_SUB(&easy_mempool_g_thread_memtotal, buf->size - sizeof(easy_mempool_thread_info_t *));
        }

        if (NULL != host) {
            easy_mempool_free(host->pool, ptr);

            if (0 == ATOMIC_DEC_FETCH(&(host->ref_cnt))) {
                easy_mempool_destroy(host->pool);
                easy_mempool_g_allocator.free(host);
            }
        }
    }

    return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

easy_mempool_t *easy_mempool_create(uint32_t size)
{
    easy_mempool_t          *ret = NULL;
    int32_t                 page_num = EASY_MEMPOOL_PAGE_MAX_NUM;
    int32_t                 size2alloc = sizeof(easy_mempool_t) + sizeof(easy_mempool_page_t) * page_num;

    if (NULL != (ret = (easy_mempool_t *)easy_mempool_g_allocator.memalign(EASY_MEMPOOL_ALIGNMENT, size2alloc))) {
        ret->cur_page_pos = 0;
        ret->direct_alloc_cnt = 0;
        ret->mem_total = 0;
        ret->mem_limit = INT64_MAX;
        ret->allocator = &easy_mempool_g_allocator;
        ret->page_size = size ? size : EASY_MEMPOOL_PAGE_SIZE;
        ret->page_num = page_num;
        ret->page_metas = (easy_mempool_page_meta_t *)((char *)ret + sizeof(easy_mempool_t));
        memset(ret->page_metas, 0, sizeof(easy_mempool_page_t) * page_num);
        ret->page_metas[ret->cur_page_pos].ref_cnt = 1;
        ret->free_num = 0;
        ret->free_list = NULL;
        pthread_spin_init(&(ret->free_list_lock), PTHREAD_PROCESS_PRIVATE);
    }

    return ret;
}

void easy_mempool_destroy(easy_mempool_t *pool)
{
    if (NULL != pool) {
        easy_mempool_clear(pool);
        easy_mempool_g_allocator.free(pool);
    }
}

void easy_mempool_clear(easy_mempool_t *pool)
{
    if (NULL != pool) {
        int32_t                 unfreed_num = 0;
        int32_t                 i = 0;

        for (i = 0; i < EASY_MEMPOOL_PAGE_MAX_NUM; i++) {
            if (0 == pool->page_metas[i].ref_cnt
                    || (i == pool->cur_page_pos && 1 == pool->page_metas[i].ref_cnt)) {
                easy_mempool_free_page_(pool, pool->page_metas[i].page);
                pool->page_metas[i].page = NULL;
            } else {
                unfreed_num++;
            }
        }

        easy_mempool_destroy_free_list_(pool);

        if (0 != unfreed_num
                || 0 != pool->direct_alloc_cnt) {
            fprintf(stderr, "[WARN] there are still %d pool_buf or %d direct_buf used cannot free",
                    unfreed_num, pool->direct_alloc_cnt);
        }
    }
}

void *easy_mempool_alloc(easy_mempool_t *pool, uint32_t size)
{
    return easy_mempool_alloc_(pool, size, EASY_MEMPOOL_ALIGNMENT);
}

void easy_mempool_free(easy_mempool_t *pool, void *ptr)
{
    if (NULL != pool
            && NULL != ptr) {
        easy_mempool_buf_t      *buf = (easy_mempool_buf_t *)((char *)ptr - sizeof(easy_mempool_buf_t));

        if (EASY_MEMPOOL_BUF_MAGIC_NUM == buf->magic_num) {
            int64_t                 size = buf->size;
            buf->magic_num = EASY_MEMPOOL_BUF_FREE_FLAG;

            if (EASY_MEMPOOL_DIRECT_ALLOC == buf->alloc_type) {
                pool->allocator->free(buf);
                ATOMIC_DEC(&(pool->direct_alloc_cnt));
            } else {
                easy_mempool_deref_page_(pool, buf->page_pos);
            }

            ATOMIC_SUB(&(pool->mem_total), size);
        }
    }
}

void easy_mempool_set_memlimit(easy_mempool_t *pool, int64_t limit)
{
    if (NULL != pool && 0 < limit) {
        pool->mem_limit = limit;
    }
}

void easy_mempool_set_allocator(easy_mempool_t *pool, easy_mempool_allocator_t *allocator)
{
    if (NULL != pool
            && NULL != allocator->memalign
            && NULL != allocator->free) {
        pool->allocator = allocator;
    }
}

int64_t easy_mempool_get_memtotal(easy_mempool_t *pool)
{
    int64_t                 ret = 0;

    if (NULL != pool) {
        ret = pool->mem_total;
    }

    return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

static void *easy_mempool_alloc_(easy_mempool_t *pool, uint32_t size, uint32_t align_size)
{
    void                    *ret = NULL;
    int32_t                 alloc_size = size + sizeof(easy_mempool_buf_t);
    alloc_size = easy_mempool_align(alloc_size, align_size);

    if (NULL != pool) {
        if ((pool->mem_total + size) > pool->mem_limit) {
            // memory over limit
        } else if (pool->page_size < alloc_size) {
            easy_mempool_buf_t      *buf = (easy_mempool_buf_t *)pool->allocator->memalign(align_size, alloc_size);

            if (NULL != buf) {
                buf->magic_num = EASY_MEMPOOL_BUF_MAGIC_NUM;
                buf->alloc_type = EASY_MEMPOOL_DIRECT_ALLOC;
                buf->size = size;
                ret = (char *)buf + sizeof(easy_mempool_buf_t);
                ATOMIC_INC(&(pool->direct_alloc_cnt));
            }
        } else {
            easy_mempool_page_t     *page = NULL;
            easy_mempool_buf_t      *buf = NULL;
            int32_t                 page_pos = -1;

            while (1) {
                if (NULL == (page = easy_mempool_get_cur_page_(pool, alloc_size, &page_pos))) {
                    break;
                }

                buf = (easy_mempool_buf_t *)easy_mempool_align_ptr(easy_mempool_alloc_from_page_(page, pool->page_size, alloc_size), align_size);

                if (NULL != buf) {
                    ATOMIC_INC(&(pool->page_metas[page_pos].ref_cnt));
                }

                easy_mempool_deref_page_(pool, page_pos);

                if (NULL != buf) {
                    buf->magic_num = EASY_MEMPOOL_BUF_MAGIC_NUM;
                    buf->alloc_type = EASY_MEMPOOL_ALLOC;
                    buf->page_pos = page_pos;
                    buf->size = size;
                    ret = (char *)buf + sizeof(easy_mempool_buf_t);
                    break;
                }
            }
        }

        if (NULL != ret) {
            ATOMIC_ADD(&(pool->mem_total), size);
        }
    }

    return ret;
}

static void easy_mempool_destroy_free_list_(easy_mempool_t *pool)
{
    if (NULL != pool) {
        easy_mempool_page_t     *iter = pool->free_list;

        while (NULL != iter) {
            easy_mempool_page_t     *next = iter->next;
            pool->allocator->free(iter);
            iter = next;
        }
    }
}

static easy_mempool_page_t *easy_mempool_alloc_page_(easy_mempool_t *pool)
{
    easy_mempool_page_t     *ret = NULL;

    if (NULL != pool) {
        pthread_spin_lock(&(pool->free_list_lock));
        easy_mempool_page_t     *page = pool->free_list;

        if (NULL != page) {
            pool->free_list = page->next;
            pool->free_num--;
            ret = page;
        }

        pthread_spin_unlock(&(pool->free_list_lock));

        if (NULL == ret) {
            ret = (easy_mempool_page_t *)pool->allocator->memalign(EASY_MEMPOOL_ALIGNMENT, pool->page_size + sizeof(easy_mempool_page_t));
        }

        if (NULL != ret) {
            ret->base = 0;
        }
    }

    return ret;
}

static void easy_mempool_free_page_(easy_mempool_t *pool, easy_mempool_page_t *page)
{
    if (NULL != pool
            && NULL != page) {
        pthread_spin_lock(&(pool->free_list_lock));

        if (EASY_MEMPOOL_PAGE_FREE_NUM > pool->free_num) {
            page->next = pool->free_list;
            pool->free_list = page;
            pool->free_num++;
            page = NULL;
        }

        pthread_spin_unlock(&(pool->free_list_lock));

        if (NULL != page) {
            pool->allocator->free(page);
        }
    }
}

static void easy_mempool_deref_page_(easy_mempool_t *pool, int32_t page_pos)
{
    if (NULL != pool
            && pool->page_num > page_pos) {
        easy_mempool_page_t     *tmp_page = pool->page_metas[page_pos].page;

        if (0 == easy_mempool_dec_ref_cnt_and_inc_seq_num_(&(pool->page_metas[page_pos]))) {
            if (tmp_page == ATOMIC_CAS(&(pool->page_metas[page_pos].page), tmp_page, NULL)) {
                easy_mempool_free_page_(pool, tmp_page);
            }
        }
    }
}

static easy_mempool_page_t *easy_mempool_get_cur_page_(easy_mempool_t *pool, int32_t ensure_size, int32_t *page_pos)
{
    easy_mempool_page_t     *ret = NULL;

    if (NULL != pool) {
        volatile int32_t        oldv = -1;
        volatile int32_t        newv = -1;
        volatile int32_t        cmpv = -1;
        easy_mempool_page_t     *cur_page = NULL;

        while (oldv != pool->cur_page_pos) {
            oldv = pool->cur_page_pos;
            newv = oldv;
            cmpv = oldv;
            ATOMIC_INC(&(pool->page_metas[oldv].ref_cnt));

            if (NULL == pool->page_metas[oldv].page) {
                easy_mempool_page_t     *tmp_page = easy_mempool_alloc_page_(pool);

                if (NULL != tmp_page) {
                    if (NULL != ATOMIC_CAS(&(pool->page_metas[oldv].page), NULL, tmp_page)) {
                        easy_mempool_free_page_(pool, tmp_page);
                    }
                }
            }

            if (NULL == (cur_page = pool->page_metas[oldv].page)) {
                easy_mempool_deref_page_(pool, oldv);
                break;
            }

            if ((pool->page_size - cur_page->base) < ensure_size) {
                int32_t                 base = cur_page->base;
                easy_mempool_deref_page_(pool, oldv);

                if (0 == base) {
                    break;
                }

                int32_t                 counter = 0;

                while (++counter < pool->page_num) {
                    newv = (newv + 1) % pool->page_num;

                    if (0 == ATOMIC_CAS(&(pool->page_metas[newv].ref_cnt), 0, 1)) {
                        if (oldv == ATOMIC_CAS(&(pool->cur_page_pos), cmpv, newv)) {
                            easy_mempool_deref_page_(pool, oldv);
                        } else {
                            easy_mempool_deref_page_(pool, newv);
                        }

                        break;
                    }
                }
            } else {
                *page_pos = oldv;
                ret = cur_page;
                break;
            }
        }
    }

    return ret;
}

static void *easy_mempool_alloc_from_page_(easy_mempool_page_t *page, int32_t page_size, int32_t alloc_size)
{
    void                    *ret = NULL;

    if (NULL != page) {
        volatile int32_t        oldv = 0;
        volatile int32_t        newv = 0;
        volatile int32_t        cmpv = 0;

        while (1) {
            oldv = page->base;
            newv = oldv + alloc_size;
            cmpv = oldv;

            if (newv > page_size) {
                break;
            }

            if (oldv == ATOMIC_CAS(&(page->base), cmpv, newv)) {
                ret = (char *)page + sizeof(easy_mempool_page_t) + oldv;
                break;
            }
        }
    }

    return ret;
}

static int32_t easy_mempool_dec_ref_cnt_and_inc_seq_num_(easy_mempool_page_meta_t *page_meta)
{
    int32_t                 ret = -1;

    if (NULL != page_meta) {
        easy_mempool_atomic_t   oldv = {0};
        easy_mempool_atomic_t   newv = {0};
        easy_mempool_atomic_t   cmpv = {0};

        while (1) {
            oldv.atomic = page_meta->atomic;
            newv.atomic = oldv.atomic;
            cmpv.atomic = oldv.atomic;
            newv.ref_cnt -= 1;

            if (0 == newv.ref_cnt) {
                newv.seq_num += 1;
            }

            //assert(0 != oldv.ref_cnt);
            if (oldv.atomic == ATOMIC_CAS(&(page_meta->atomic), cmpv.atomic, newv.atomic)) {
                ret = newv.ref_cnt;
                break;
            }
        }
    }

    return ret;
}

static void easy_mempool_thread_destroy_callback_(void *data)
{
    if (NULL != data) {
        easy_mempool_thread_info_t *thread_info = (easy_mempool_thread_info_t *)data;

        if (0 == ATOMIC_DEC_FETCH(&(thread_info->ref_cnt))) {
            easy_mempool_destroy(thread_info->pool);
            easy_mempool_g_allocator.free(thread_info);
        }
    }
}

void __attribute__((constructor)) init_global_easy_mempool_()
{
    easy_mempool_g_pool = easy_mempool_create(0);
    pthread_key_create(&easy_mempool_g_thread_key, easy_mempool_thread_destroy_callback_);
}

void __attribute__((destructor)) destroy_global_easy_mempool_()
{
    easy_mempool_destroy(easy_mempool_g_pool);
    void                    *data = pthread_getspecific(easy_mempool_g_thread_key);
    easy_mempool_thread_destroy_callback_(data);
    pthread_key_delete(easy_mempool_g_thread_key);
}

#else

#endif

