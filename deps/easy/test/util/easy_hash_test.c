#include "util/easy_hash.h"
#include "util/easy_buf.h"
#include <easy_test.h>

/**
 * 测试 easy_hash
 */
typedef struct test_1_t {
    int                     id;
    easy_hash_list_t        node;
} test_1_t;
typedef struct test_2_t {
    int                     id;
    easy_hash_list_t        hash;
    easy_list_t             list;
} test_2_t;

static void *test_realloc_1 (void *ptr, size_t size)
{
    if (size > 1024)
        return NULL;
    else
        return easy_test_realloc(ptr, size);
}
static void *test_realloc_2 (void *ptr, size_t size)
{
    if (size > 0 && size < 1024)
        return NULL;
    else
        return easy_test_realloc(ptr, size);
}

TEST(easy_hash, create)
{
    easy_pool_t             *pool;
    easy_hash_t             *table;

    pool = easy_pool_create(0);
    table = easy_hash_create(pool, 100, 0);
    EXPECT_TRUE(table != NULL);
    EXPECT_EQ(table->size, 128);

    // 1
    easy_pool_set_allocator(test_realloc_1);
    table = easy_hash_create(pool, 1000, 0);
    EXPECT_TRUE(table == NULL);

    // 2
    easy_pool_set_allocator(test_realloc_2);
    easy_buf_create(pool, 0);
    table = easy_hash_create(pool, 1000, 0);
    EXPECT_TRUE(table == NULL);
    easy_pool_destroy(pool);

    easy_pool_set_allocator(easy_test_realloc);
}

// int easy_hash_add(easy_hash_t *table, uint64_t key, easy_hash_list_t *list)
TEST(easy_hash, add_find_del)
{
    easy_pool_t             *pool;
    easy_hash_t             *table;
    test_1_t                *objects, *obj;
    int                     i, size;
    uint32_t                n;
    easy_hash_list_t        *node;

    pool = easy_pool_create(0);
    table = easy_hash_create(pool, 100, offsetof(test_1_t, node));
    EXPECT_TRUE(table != NULL);
    EXPECT_EQ(table->size, 128);

    // objects
    objects = (test_1_t *)easy_pool_alloc(pool, sizeof(test_1_t) * 512);

    for(i = 0; i < 512; i++) {
        easy_hash_add(table, i, &objects[i].node);
    }

    EXPECT_EQ(table->count, 512);

    // 1
    size = 0;

    for(i = 0; i < 512; i++) {
        obj = (test_1_t *)easy_hash_find(table, i);

        if (obj == &objects[i]) size ++;
    }

    EXPECT_EQ(size, 512);

    // 2
    size = 0;

    for(i = 0; i < 512; i++) {
        obj = (test_1_t *)easy_hash_find(table, i + 0x10000);

        if (!obj) size ++;
    }

    EXPECT_EQ(size, 512);

    // 3 check
    size = 0;
    easy_hash_for_each(n, node, table) {
        size ++;
    }
    EXPECT_EQ(size, 512);

    // 4, del
    size = 0;

    for(i = 0; i < 500; i++) {
        obj = (test_1_t *)easy_hash_del(table, i);

        if (obj == &objects[i]) size ++;

        if (obj) memset(obj, 0, sizeof(test_1_t));
    }

    easy_hash_del(table, 0);
    EXPECT_EQ(size, 500);
    EXPECT_EQ(table->count, 12);

    // 5. del_node
    for(i = 0; i < 500; i++) {
        easy_hash_add(table, i, &objects[i].node);
    }

    size = 0;

    for(i = 0; i < 512; i++) {
        if (easy_hash_del_node(&(objects[i].node))) size ++;
    }

    EXPECT_EQ(size, 512);

    // 6. del_node
    easy_hash_del_node(&objects[0].node);

    // check
    size = 0;
    easy_hash_for_each(n, node, table) {
        size ++;
    }
    EXPECT_EQ(size, 0);

    easy_pool_destroy(pool);
}

// uint64_t easy_hash_code(const void *key, int len, unsigned int seed)
TEST(easy_hash, code)
{
    int                     i;
    char                    buffer[257];

    for(i = 0; i < 256; i++) {
        buffer[i] = i;
        easy_hash_code(buffer, i + 1, 3);
    }
}

// int easy_hash_dlist_add(easy_hash_t *table, uint64_t key, easy_hash_list_t *list)
TEST(easy_hash, dadd_find_del)
{
    easy_pool_t             *pool;
    easy_hash_t             *table;
    test_2_t                *objects, *obj;
    int                     i, size;

    pool = easy_pool_create(0);
    table = easy_hash_create(pool, 100, offsetof(test_2_t, hash));
    EXPECT_TRUE(table != NULL);
    EXPECT_EQ(table->size, 128);

    // objects
    objects = (test_2_t *)easy_pool_alloc(pool, sizeof(test_2_t) * 512);

    for(i = 0; i < 512; i++) {
        easy_hash_dlist_add(table, i, &objects[i].hash, &objects[i].list);
    }

    EXPECT_EQ(table->count, 512);

    // foreach
    size = 0;
    i = 0;
    easy_list_for_each_entry(obj, &table->list, list) {
        if (obj == &objects[i++]) size ++;
    }
    EXPECT_EQ(size, 512);

    // del
    size = 0;

    for(i = 0; i < 512; i++) {
        obj = (test_2_t *)easy_hash_dlist_del(table, i);

        if (obj == &objects[i]) size ++;
    }

    EXPECT_EQ(size, 512);
    EXPECT_EQ(easy_list_empty(&table->list), 1);

    easy_pool_destroy(pool);
}

TEST(easy_hash_pair_del, args)
{
    easy_pool_t             *pool;
    easy_hash_string_t      *table;
    easy_string_pair_t      *pair, *npair;

    pool = easy_pool_create(0);
    table = easy_hash_string_create(pool, 256, 1);

    pair = (easy_string_pair_t *)easy_pool_calloc(pool, sizeof(easy_string_pair_t));
    easy_buf_string_append(&pair->name, "X-Cache", strlen("X-Cache"));
    easy_buf_string_append(&pair->value, "HIT dirn:0:0", strlen("HIT dirn:0:0"));
    easy_hash_string_add(table, pair);

    pair = (easy_string_pair_t *)easy_pool_calloc(pool, sizeof(easy_string_pair_t));
    easy_buf_string_append(&pair->name, "X-Cache", strlen("X-Cache"));
    easy_buf_string_append(&pair->value, "HIT dirn:0:1", strlen("HIT dirn:0:1"));
    easy_hash_string_add(table, pair);

    easy_list_for_each_entry_safe(pair, npair, &table->list, list) {
        EXPECT_TRUE(pair == easy_hash_pair_del(table, pair));
        // Dead loop, if use easy_hash_string_del
        //EXPECT_EQ(pair, easy_hash_string_del(table, pair->name.data,  pair->name.len));
    }

    easy_pool_destroy(pool);
}

