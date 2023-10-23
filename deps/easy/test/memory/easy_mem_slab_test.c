#include "memory/easy_mem_slab.h"
#include <easy_test.h>
#include <pthread.h>

/**
 * 测试 easy_mem_slab
 */
#define Mbyte(s) (s*1024*1024)
#define Kbyte(s,d) (s*1024+d)
extern easy_mem_mgr_t   easy_mem_mgr_var;

TEST(easy_mem_slab, init)
{
    int                     ret;
    EXPECT_EQ(easy_mem_mgr_var.started, 0);

    ret = easy_mem_slab_init(Mbyte(1), Mbyte(16));
    EXPECT_EQ(ret, EASY_OK);

    EXPECT_EQ(easy_mem_mgr_var.started, 1);
    easy_mem_slab_destroy();
}

typedef struct test_1_t {
    int                     size;
    int                     idx;
    int                     in;
} test_1_t;
static int test_1_check(test_1_t *t)
{
    unsigned char           *ptr;
    easy_mem_zone_t         *z;
    unsigned long           a;
    int                     ret, order;
    easy_mem_slab_t         *slab;

    z = easy_mem_mgr_var.zone;
    ptr = (unsigned char *)easy_mem_slab_realloc(NULL, t->size);

    if (ptr == NULL) {
        return 0;
    }

    ret = 1;

    if (t->in == 0) {
        if (ptr >= z->mem_start && ptr < z->mem_end) {
            ret = 0;
        }
    } else {
        order = (ptr - z->mem_start) >> EASY_MEM_PAGE_SHIFT;
        order = (z->page_flags[order] & 0x0f);
        a = (1 << (EASY_MEM_PAGE_SHIFT + order));
        slab = (easy_mem_slab_t *)(((unsigned long)ptr) & ~(a - 1));

        if (slab->cache_idx != t->idx) {
            ret = 0;
        }
    }

    easy_mem_slab_realloc(ptr, 0);
    return ret;
}

// void *easy_mem_slab_realloc(void *ptr, size_t size)
TEST(easy_mem_slab, realloc)
{
    int                     i, ret, end, max_size, cnt;
    test_1_t                param[] = {
        {511, 0, 1}, {512, 0, 1}, {513, 1, 1},
        {Kbyte(1, -1), 1, 1}, {Kbyte(1, 0), 1, 1}, {Kbyte(1, 1), 2, 1},
        {Kbyte(2, -1), 2, 1}, {Kbyte(2, 0), 2, 1}, {Kbyte(2, 1), 3, 1},
        {Kbyte(4, -1), 3, 1}, {Kbyte(4, 0), 3, 1}, {Kbyte(4, 1), 4, 1},
        {Kbyte(8, -1), 4, 1}, {Kbyte(8, 0), 4, 1}, {Kbyte(8, 1), 5, 1},
        {Kbyte(16, -1), 5, 1}, {Kbyte(16, 0), 5, 1}, {Kbyte(16, 1), 6, 1},
        {Kbyte(32, -1), 6, 1}, {Kbyte(32, 0), 6, 1}, {Kbyte(32, 1), 7, 1},
        {Kbyte(64, -1), 7, 1}, {Kbyte(64, 0), 7, 1}, {Kbyte(64, 1), 8, 1},
        {Kbyte(128, -1), 8, 1}, {Kbyte(128, 0), 8, 1}, {Kbyte(128, 1), 9, 1},
        {Kbyte(256, -1), 9, 1}, {Kbyte(256, 0), 9, 1}, {Kbyte(256, 1), 10, 1},
        {Kbyte(512, -1), 10, 1}, {Kbyte(512, 0), 10, 1}, {Kbyte(512, 1), 11, 1},
        {Kbyte(1024, -1), 11, 1}, {Kbyte(1024, 0), 11, 1}, {Kbyte(1024, 1), 0, 0},
        {Kbyte(2048, -1), 0, 0}, {Kbyte(2048, 0), 0, 0}, {Kbyte(2048, 1), 0, 0},
    };

    // setup
    EXPECT_EQ(easy_mem_mgr_var.started, 0);
    ret = easy_mem_slab_init(Mbyte(1), Mbyte(16));
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_EQ(easy_mem_mgr_var.started, 1);

    // 1.
    end = easy_mem_mgr_var.cache_num - 1;
    max_size = easy_mem_mgr_var.caches[end].buffer_size;
    EXPECT_EQ(max_size, Mbyte(1));

    // 2.
    cnt = sizeof(param) / sizeof(param[0]);
    ret = 0;

    for(i = 0; i < cnt; i++) {
        ret += test_1_check(&param[i]);
    }

    EXPECT_EQ(ret, cnt);

    // 3.
    ret = 0;

    for(i = 0; i < cnt; i++) {
        ret += test_1_check(&param[cnt - 1 - i]);
    }

    EXPECT_EQ(ret, cnt);

    // 4.
    ret = 0;
    end = 0;
    srand(time(NULL));

    for(i = 0; i < 3000; i++) {
        ret += test_1_check(&param[rand() % cnt]);
        end ++;
    }

    EXPECT_EQ(ret, end);

    // 5. other
    char                    *ptr = NULL;

    for(i = Mbyte(1) - 5; i < Mbyte(1) + 5; i++) {
        ptr = (char *)easy_mem_slab_realloc(ptr, i);
        EXPECT_TRUE(ptr != NULL);
    }

    cnt = 0;

    for(i = 0; i < 10; i++) {
        ptr = (char *)easy_mem_slab_realloc(ptr, i);

        if (ptr) cnt ++;
    }

    EXPECT_EQ(cnt, 9);

    // down
    easy_mem_slab_destroy();
}

void *test_mem_alloc(void *args)
{
    unsigned char           *ptr[100], *str;
    int                     i, cnt = 0, size, idx;
    srand(pthread_self());
    easy_mem_zone_t         *z = easy_mem_mgr_var.zone;

    // 1.
    for(i = 0; i < 10000; i++) {
        size = ((rand() * 10243) & 0x7fffffff) % Mbyte(1) + 10;
        ptr[0] = (unsigned char *)easy_mem_slab_realloc(NULL, size);

        if (ptr[0]) {
            easy_mem_slab_realloc(ptr[0], 0);
            cnt ++;
        }
    }

    EXPECT_EQ(cnt, 10000);

    // 2.
    idx = 0;
    cnt = 0;

    for(i = 0; i < 10000; i++) {
        size = ((rand() * 10243) & 0x7fffffff) % Kbyte(4, 0) + 10;
        str = (unsigned char *)easy_mem_slab_realloc(NULL, size);

        if (str < z->mem_start || str > z->mem_end) abort();

        ptr[idx++] = str;

        if (idx >= 100) {
            while(idx > 50) {
                if (ptr[idx - 1]) {
                    easy_mem_slab_realloc(ptr[idx - 1], 0);
                    cnt ++;
                }

                idx --;
            }
        }
    }

    while(idx > 0) {
        if (ptr[idx - 1]) {
            easy_mem_slab_realloc(ptr[idx - 1], 0);
            cnt ++;
        }

        idx --;
    }

    EXPECT_EQ(cnt, 10000);
    return (void *)NULL;
}

TEST(easy_mem_slab, mthread)
{
    int                     i, thread_cnt = 10;
    pthread_t               tid[thread_cnt];

    // setup
    easy_mem_slab_init(Mbyte(1), Mbyte(32));

    for(i = 0; i < thread_cnt; i++) {
        pthread_create(&tid[i], NULL, test_mem_alloc, NULL);
    }

    for(i = 0; i < thread_cnt; i++) {
        pthread_join(tid[i], NULL);
    }

    // down
    easy_mem_slab_destroy();
    easy_mem_slab_destroy();
}

TEST(easy_mem_slab, realloc_cmp)
{
    char                    *ptr, *ptr1;
    char                    str[100];

    memset(str, 'A', 100);
    easy_mem_slab_init(Mbyte(1), Mbyte(32));

    // 1.
    ptr = (char *)easy_mem_slab_realloc(NULL, 1);
    ptr1 = (char *)easy_mem_slab_realloc(ptr, 100);
    EXPECT_TRUE(ptr == ptr1);
    memcpy(ptr1, str, 100);

    // 2.
    ptr = (char *)easy_mem_slab_realloc(ptr1, 10000);
    EXPECT_FALSE(ptr == ptr1);

    if (memcmp(ptr, str, 100)) {
        EXPECT_TRUE(0);
    }

    ptr[9999] = 'B';

    // 3.
    ptr1 = (char *)easy_mem_slab_realloc(ptr, Mbyte(1) + 1);
    EXPECT_FALSE(ptr == ptr1);

    if (memcmp(ptr1, str, 100)) {
        EXPECT_TRUE(0);
    }

    EXPECT_TRUE(ptr1[9999] == 'B');
    easy_mem_slab_realloc(ptr1, 0);

    easy_mem_slab_destroy();
}

