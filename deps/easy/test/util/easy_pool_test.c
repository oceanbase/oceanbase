#include "util/easy_pool.h"
#include <easy_test.h>

/**
 * 测试 easy_pool
 */
static int              test_alloc_byte = 0;
static void *test_realloc (void *ptr, size_t size)
{
    return NULL;
}
static void *test_realloc_2 (void *ptr, size_t size)
{
    char                    *p1, *p = (char *)ptr;

    if (p) p -= 8;

    if (size) {
        test_alloc_byte += size;
        p1 = (char *)easy_realloc(p, size + 8);

        if (p1) {
            *((int *)p1) = size;
            p1 += 8;
        }

        return p1;
    } else if (p) {
        test_alloc_byte -= *((int *)p);
        easy_free(p);
    }

    return NULL;
}
static void test_cleanup(const void *data)
{
    int                     *p;

    p = (int *) data;
    *p += 1;
}

TEST(easy_pool, create)
{
    easy_pool_t             *pool;

    pool = easy_pool_create(0);
    EXPECT_TRUE(pool != NULL);
    EXPECT_TRUE(pool->end == ((uint8_t *)pool) + EASY_POOL_ALIGNMENT);
    easy_pool_destroy(pool);

    pool = easy_pool_create(1);
    EXPECT_TRUE(pool != NULL);
    EXPECT_TRUE(pool->end == ((uint8_t *)pool) + EASY_POOL_ALIGNMENT);
    easy_pool_destroy(pool);

    pool = easy_pool_create(EASY_POOL_ALIGNMENT - sizeof(easy_pool_t));
    EXPECT_TRUE(pool != NULL);
    EXPECT_TRUE(pool->end == ((uint8_t *)pool) + EASY_POOL_ALIGNMENT);
    easy_pool_destroy(pool);

    pool = easy_pool_create(EASY_POOL_ALIGNMENT - sizeof(easy_pool_t) + 1);
    EXPECT_TRUE(pool != NULL);
    EXPECT_TRUE(pool->end == ((uint8_t *)pool) + EASY_POOL_ALIGNMENT * 2);
    easy_pool_destroy(pool);

    // set realloc
    easy_pool_set_allocator(test_realloc);
    pool = easy_pool_create(0);
    EXPECT_TRUE(pool == NULL);

    easy_pool_set_allocator(easy_test_realloc);
}

// easy_pool_clear
TEST(easy_pool, clear)
{
    int                     i;
    easy_pool_t             *pool;

    test_alloc_byte = 0;
    easy_pool_set_allocator(test_realloc_2);
    pool = easy_pool_create(0);
    EXPECT_EQ(pool->end - pool->last, EASY_POOL_ALIGNMENT - sizeof(easy_pool_t));

    // large alloc
    for(i = 0; i < 10; i++) {
        easy_pool_alloc(pool, EASY_POOL_ALIGNMENT);
    }

    EXPECT_EQ(test_alloc_byte, EASY_POOL_ALIGNMENT * 11);

    easy_pool_clear(pool);
    EXPECT_EQ(test_alloc_byte, EASY_POOL_ALIGNMENT);
    EXPECT_EQ(pool->last - (uint8_t *)pool, sizeof(easy_pool_t));

    easy_pool_destroy(pool);
    EXPECT_EQ(test_alloc_byte, 0);

    easy_pool_set_allocator(easy_test_realloc);
}

TEST(easy_pool, alloc)
{
    easy_pool_t             *pool;
    int                     i, size, msize;
    void                    *p;

    pool = easy_pool_create(0);
    EXPECT_TRUE(pool != NULL);
    EXPECT_TRUE(pool->end - pool->last == EASY_POOL_ALIGNMENT - sizeof(easy_pool_t));
    size = pool->end - pool->last;
    msize = size - 64 + 1;

    for(i = 0; i < 3; i++) {
        size -= 63;
        p = easy_pool_alloc(pool, 63);
        EXPECT_TRUE(p != NULL);
        EXPECT_EQ(pool->end - pool->last, size);
        EXPECT_TRUE(pool->next == NULL);
        size --;
    }

    size ++;
    p = easy_pool_alloc(pool, size);
    EXPECT_TRUE(p != NULL);
    EXPECT_TRUE(pool->next != NULL);
    EXPECT_EQ(pool->end - pool->last, size);

    EXPECT_TRUE(pool->large == NULL);
    p = easy_pool_alloc(pool, EASY_POOL_ALIGNMENT);
    EXPECT_TRUE(p != NULL);
    EXPECT_TRUE(pool->large != NULL);

    EXPECT_TRUE(pool->current == pool);

    for(i = 0; i < 6; i++) p = easy_pool_alloc(pool, msize);

    EXPECT_TRUE(pool->current != pool);

    size = 0;

    for(i = 0; i < 1024; i++) {
        p = easy_pool_alloc(pool, i + 1);

        if (p) size ++;
    }

    EXPECT_EQ(size, 1024);
    easy_pool_destroy(pool);
}

TEST(easy_pool, nalloc)
{
    easy_pool_t             *pool;
    int                     i, size, msize;
    void                    *p;

    pool = easy_pool_create(0);
    EXPECT_TRUE(pool != NULL);
    EXPECT_TRUE(pool->end - pool->last == EASY_POOL_ALIGNMENT - sizeof(easy_pool_t));
    size = pool->end - pool->last;
    msize = size - 64 + 1;

    for(i = 0; i < 3; i++) {
        size -= 63;
        p = easy_pool_nalloc(pool, 63);
        EXPECT_TRUE(p != NULL);
        EXPECT_EQ(pool->end - pool->last, size);
        EXPECT_TRUE(pool->next == NULL);
    }

    p = easy_pool_nalloc(pool, size + 1);
    EXPECT_TRUE(p != NULL);
    EXPECT_TRUE(pool->next != NULL);
    EXPECT_EQ(pool->end - pool->last, size);

    EXPECT_TRUE(pool->large == NULL);
    p = easy_pool_nalloc(pool, EASY_POOL_ALIGNMENT);
    EXPECT_TRUE(p != NULL);
    EXPECT_TRUE(pool->large != NULL);

    EXPECT_TRUE(pool->current == pool);

    for(i = 0; i < 6; i++) {
        if (i % 2) pool->ref ++;

        p = easy_pool_nalloc(pool, msize);

        if (i % 2) pool->ref --;
    }

    EXPECT_TRUE(pool->current != pool);

    easy_pool_destroy(pool);
}

TEST(easy_pool, calloc)
{
    easy_pool_t             *pool;
    char                    *p;
    int                     i, cnt, size;

    size = EASY_POOL_ALIGNMENT / 2;
    pool = easy_pool_create(0);
    p = (char *)easy_pool_calloc(pool, size);
    cnt = 0;

    for(i = 0; i < size; i++)
        if (p[i]) cnt ++;

    EXPECT_EQ(cnt, 0);

    easy_pool_set_allocator(test_realloc);
    p = (char *)easy_pool_calloc(pool, size);
    EXPECT_TRUE(p == NULL);
    p = (char *)easy_pool_calloc(pool, 1);
    EXPECT_TRUE(p != NULL);
    p = (char *)easy_pool_calloc(pool, EASY_POOL_ALIGNMENT);
    EXPECT_TRUE(p == NULL);

    // easy_pool_default_realloc
    easy_pool_set_allocator(NULL);
    p = (char *)easy_pool_realloc(NULL, 1);
    EXPECT_TRUE(p != NULL);
    p = (char *)easy_pool_realloc(p, 2);
    EXPECT_TRUE(p != NULL);
    p = (char *)easy_pool_realloc(p, 0);
    EXPECT_TRUE(p == NULL);
    p = (char *)easy_pool_realloc(p, 0);
    EXPECT_TRUE(p == NULL);

    easy_pool_set_allocator(easy_test_realloc);
    easy_pool_destroy(pool);
}

TEST(easy_pool, realloc)
{
    char                    *ptr;
    ptr = (char *)easy_pool_realloc(NULL, 1);
    EXPECT_TRUE(ptr != NULL);
    ptr = (char *)easy_pool_realloc(ptr, 1);
    EXPECT_TRUE(ptr != NULL);
    ptr = (char *)easy_pool_realloc(ptr, 0);
    EXPECT_TRUE(ptr == NULL);
    ptr = (char *)easy_pool_realloc(ptr, 0);
    EXPECT_TRUE(ptr == NULL);
}

//char *easy_pool_strdup(easy_pool_t *pool, const char *str)
TEST(easy_pool, strdup)
{
    easy_pool_t             *pool;
    char                    *p;
    pool = easy_pool_create(0);
    char                    data[EASY_POOL_ALIGNMENT];
    memset(data, 'A', EASY_POOL_ALIGNMENT);
    data[EASY_POOL_ALIGNMENT - 1] = '\0';

    // null
    p = easy_pool_strdup(pool, NULL);
    EXPECT_TRUE(p == NULL);
    p = easy_pool_strdup(pool, "ABC");
    EXPECT_TRUE(memcmp(p, "ABC", 4) == 0);
    p = easy_pool_strdup(pool, data);
    EXPECT_TRUE(memcmp(p, data, EASY_POOL_ALIGNMENT) == 0);

    // alloc failure
    easy_pool_set_allocator(test_realloc);
    p = easy_pool_strdup(pool, "ABC");
    EXPECT_TRUE(p != NULL);
    p = easy_pool_strdup(pool, data);
    EXPECT_TRUE(p == NULL);
    easy_pool_set_allocator(easy_test_realloc);

    easy_pool_destroy(pool);
}

TEST(easy_pool, cleanup)
{
    easy_pool_t             *pool;
    easy_pool_cleanup_t     *cl;
    int                     i, cnt, size;

    i = 0;
    cnt = 0;
    size = 111;
    cl = NULL;
    pool = easy_pool_create(0);

    for (i = 0; i < size; ++ i)
    {
        cl = easy_pool_cleanup_new(pool, &cnt, test_cleanup);
        EXPECT_TRUE(cl != NULL);

        easy_pool_cleanup_reg(pool, cl);
    }

    cl = NULL;
    easy_pool_destroy(pool);

    EXPECT_TRUE(cnt == size);
}
