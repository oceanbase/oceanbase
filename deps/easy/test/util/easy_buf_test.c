#include "util/easy_buf.h"
#include <easy_test.h>

// easy_buf_t *easy_buf_create(easy_pool_t *pool, uint32_t size)
TEST(easy_buf, create)
{
    easy_pool_t             *pool;
    easy_buf_t              *b;

    pool = easy_pool_create(0);
    b = easy_buf_create(pool, 10);
    EXPECT_TRUE(b != NULL);
    EXPECT_EQ(b->end - b->pos, 10);
    EXPECT_EQ((long)b->last, (long)b->pos);

    b = easy_buf_create(pool, 0);
    EXPECT_TRUE(b != NULL);
    EXPECT_EQ((long)pool->last, (long)pool->end);
    easy_pool_destroy(pool);
}

// easy_buf_t *easy_buf_pack(easy_pool_t *pool, const char *data, uint32_t size)
TEST(easy_buf, pack)
{
    easy_pool_t             *pool;
    easy_buf_t              *b;
    const char              *p = "xxxxxabbbbbb123444354352345";

    pool = easy_pool_create(0);
    b = easy_buf_pack(pool, p, strlen(p));
    EXPECT_TRUE(b != NULL);

    EXPECT_TRUE(b->pos == p);
    EXPECT_TRUE(b->last == p + strlen(p));
    EXPECT_TRUE(b->end == p + strlen(p));

    easy_pool_destroy(pool);
}

// easy_file_buf_t *easy_file_buf_create(easy_pool_t *pool)
TEST(easy_buf, file_create)
{
    easy_pool_t             *pool;
    easy_file_buf_t         *ft;

    pool = easy_pool_create(0);
    ft = easy_file_buf_create(pool);
    EXPECT_TRUE(ft != NULL);
    EXPECT_EQ(ft->flags, EASY_BUF_FILE);
    ft->count = 1025;
    EXPECT_EQ(easy_buf_len((easy_buf_t *)ft), 1025);
    easy_pool_destroy(pool);
}

// easy_buf_destroy
TEST(easy_buf, destroy)
{
    int64_t                 ref;
    easy_pool_t             *pool;
    easy_file_buf_t         *ft;
    easy_buf_t              *b;

    ref = 0;
    pool = easy_pool_create(0);
    ft = easy_file_buf_create(pool);
    EXPECT_TRUE(ft != NULL);
    EXPECT_EQ(ft->flags, EASY_BUF_FILE);
    easy_file_buf_set_close(ft);
    EXPECT_EQ(ref, 0);
    easy_buf_destroy((easy_buf_t *)ft);

    // not close
    ft = easy_file_buf_create(pool);
    easy_buf_destroy((easy_buf_t *)ft);

    // buf
    b = easy_buf_create(pool, 10);
    EXPECT_TRUE(b != NULL);
    easy_buf_destroy(b);
    EXPECT_EQ((b->flags & EASY_BUF_FILE), 0);
    easy_pool_destroy(pool);
}

// int easy_buf_check_read_space(easy_pool_t *pool, easy_buf_t *b, uint32_t size)
TEST(easy_buf, check_read_space)
{
    easy_pool_t             *pool;
    easy_buf_t              *b;
    char                    *ptr;
    int                     ret;

    pool = easy_pool_create(0);
    b = easy_buf_create(pool, 10);
    EXPECT_TRUE(b != NULL);
    EXPECT_EQ(b->end - b->pos, 10);
    ptr = b->pos;
    b->pos = ptr + 3;
    b->last = ptr + 8;
    ret = easy_buf_len(b);
    EXPECT_EQ(ret, 5);
    ptr = b->pos;

    ret = easy_buf_check_read_space(pool, b, 2);
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_TRUE(b->pos == ptr);
    ret = easy_buf_check_read_space(pool, b, 10);
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_FALSE(b->pos == ptr);
    EXPECT_EQ((long)(b->pos + 5), (long)b->last);

    // no data
    b = easy_buf_create(pool, 10);
    ptr = b->pos;
    EXPECT_EQ(b->end - b->pos, 10);
    ret = easy_buf_check_read_space(pool, b, 11);
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_FALSE(b->pos == ptr);
    EXPECT_EQ((long)b->pos, (long)b->last);
    easy_pool_destroy(pool);
}

// int easy_buf_check_write_space(pool, easy_list_t      *bc, easy_buf_t *b, uint32_t size)
TEST(easy_buf, check_write_space)
{
    easy_pool_t             *pool;
    easy_buf_t              *b, *b1, *bx;
    char                    *ptr;
    easy_list_t             *bc;

    pool = easy_pool_create(0);
    bc = (easy_list_t *)easy_pool_calloc(pool, sizeof(easy_list_t     ));
    easy_list_init(bc);
    b = easy_buf_create(pool, 10);
    ptr = b->end;
    easy_list_add_tail(&b->node, bc);

    // 1, 直接返回OK
    b1 = easy_buf_check_write_space(pool, bc, 2);
    EXPECT_TRUE(b1 == b);
    EXPECT_TRUE(b1->end == ptr);
    b1->last += 3;

    // 2.
    b1 = easy_buf_check_write_space(pool, bc, 7);
    EXPECT_TRUE(b1 == b);
    EXPECT_TRUE(b1->end == ptr);

    // 3. 新增一个PAGE
    b1 = easy_buf_check_write_space(pool, bc, 8);
    EXPECT_TRUE(b1 != b);
    bx = easy_list_get_last(bc, easy_buf_t, node);
    EXPECT_TRUE(b1 == bx);

    b = easy_buf_check_write_space(pool, bc, 11);
    EXPECT_TRUE(b1 == b);

    easy_pool_destroy(pool);
}

// easy_buf_chain_clear
TEST(easy_buf, chain_clear)
{
    easy_pool_t             *pool;
    easy_list_t             *bc;
    easy_buf_t              *b;
    int                     i;

    pool = easy_pool_create(0);
    bc = (easy_list_t *)easy_pool_calloc(pool, sizeof(easy_list_t     ));
    easy_list_init(bc);
    easy_buf_chain_clear(bc);
    EXPECT_TRUE(easy_list_empty(bc));

    // push
    for(i = 0; i < 10; i++) {
        b = easy_buf_create(pool, 1);
        easy_list_add_tail(&b->node, bc);
    }

    easy_buf_chain_clear(bc);
    EXPECT_TRUE(easy_list_empty(bc));

    // no init list
    bc = (easy_list_t *)easy_pool_calloc(pool, sizeof(easy_list_t));
    b = easy_buf_create(pool, 1);
    easy_buf_chain_offer(bc, b);

    // init list
    bc = (easy_list_t *)easy_pool_calloc(pool, sizeof(easy_list_t));
    easy_list_init(bc);
    b = easy_buf_create(pool, 1);
    easy_buf_chain_offer(bc, b);

    easy_pool_destroy(pool);
}

//void easy_file_buf_on_close(int fd, void *args)
TEST(easy_buf, file_set_close)
{
    easy_file_buf_t         b;
    b.flags = 0;
    easy_file_buf_set_close(&b);
}

// int easy_buf_string_copy(easy_pool_t *pool, easy_buf_string_t *d, easy_buf_string_t *s)
TEST(easy_buf, string_copy)
{
    easy_buf_string_t       s1, s2, s3;
    easy_pool_t             *pool;
    int                     ret;

    easy_buf_string_set(&s1, "hello");
    easy_buf_string_set(&s2, "");

    pool = easy_pool_create(0);
    ret = easy_buf_string_copy(pool, &s3, &s1);
    EXPECT_EQ(ret, 5);
    ret = memcmp(s1.data, s3.data, 5);
    EXPECT_EQ(ret, 0);
    ret = easy_buf_string_copy(pool, &s3, &s2);
    EXPECT_EQ(ret, 0);

    ret = easy_buf_string_printf(pool, &s3, "%08X", 0xf1234567);
    EXPECT_EQ(ret, 8);
    ret = memcmp("F1234567", easy_buf_string_ptr(&s3), 8);
    EXPECT_EQ(ret, 0);
    easy_pool_destroy(pool);
}

// static inline void easy_buf_string_append(easy_buf_string_t *s, const char *value, int len)
TEST(easy_buf, string_append)
{
    easy_buf_string_t       s1;
    const char              *p1, *p2;

    p1 = "abcdABCd";
    p2 = "abcdABCd12345678";
    easy_buf_string_set(&s1, p1);

    EXPECT_EQ(s1.len, 8);
    easy_buf_string_append(&s1, p2 + 8, 5);
    EXPECT_EQ(s1.len, 13);
    EXPECT_TRUE(s1.data == p2);
}

static void *test_realloc (void *ptr, size_t size)
{
    return NULL;
}
TEST(easy_buf, other)
{
    easy_pool_t             *pool;
    easy_buf_t              *b, *b1;
    int                     ret;
    easy_list_t             bc;

    // easy_buf_create
    pool = easy_pool_create(0);
    easy_pool_set_allocator(test_realloc);
    b = easy_buf_create(pool, EASY_POOL_ALIGNMENT);
    EXPECT_TRUE(b == NULL);

    // 1
    b1 = easy_buf_create(pool, 0);
    EXPECT_TRUE(b1 != NULL);
    b = easy_buf_create(pool, EASY_POOL_ALIGNMENT);
    EXPECT_TRUE(b == NULL);

    // 2
    ret = easy_buf_check_read_space(pool, b1, EASY_POOL_ALIGNMENT);
    EXPECT_EQ(ret, EASY_ERROR);

    // 3
    memset(&bc, 0, sizeof(bc));
    b = easy_buf_check_write_space(pool, &bc, EASY_POOL_ALIGNMENT);
    EXPECT_TRUE(b == NULL);

    // 4
    b = easy_buf_pack(pool, "aaaa", 4);
    EXPECT_TRUE(b == NULL);

    easy_pool_set_allocator(easy_test_realloc);
    easy_pool_destroy(pool);
}
