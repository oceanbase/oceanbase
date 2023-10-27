#include "io/easy_baseth_pool.h"
#include "io/easy_io.h"
#include "packet/easy_simple_handler.h"
#include <easy_test.h>

TEST(easy_thread_pool, index)
{
    easy_io_t               *eio;
    void                    *ptr, *ptr1, *ptr2;

    // 1.
    eio = easy_io_create(2);
    ptr = easy_thread_pool_index(eio->io_thread_pool, -1);
    EXPECT_TRUE(ptr == NULL);

    ptr = easy_thread_pool_index(eio->io_thread_pool, 2);
    EXPECT_TRUE(ptr == NULL);

    ptr = easy_thread_pool_index(eio->io_thread_pool, 0);
    EXPECT_TRUE(ptr != NULL);

    // 2.
    ptr = easy_thread_pool_hash(eio->io_thread_pool, 1024);
    EXPECT_TRUE(ptr != NULL);
    ptr1 = easy_thread_pool_hash(eio->io_thread_pool, 1025);
    EXPECT_TRUE(ptr1 != NULL);
    EXPECT_TRUE(ptr1 != ptr);
    ptr2 = easy_thread_pool_hash(eio->io_thread_pool, 1026);
    EXPECT_TRUE(ptr2 != NULL);
    EXPECT_TRUE(ptr == ptr2);

    // 3.
    ptr = easy_thread_pool_rr(eio->io_thread_pool, 0);
    ptr1 = easy_thread_pool_rr(eio->io_thread_pool, 0);
    ptr2 = easy_thread_pool_rr(eio->io_thread_pool, 0);
    EXPECT_TRUE(ptr == ptr2);
    EXPECT_TRUE(ptr1 != ptr2);
    ptr2 = easy_thread_pool_rr(eio->io_thread_pool, 200);
    EXPECT_TRUE(ptr2 != NULL);
    EXPECT_TRUE(ptr2 == ptr1);

    easy_io_destroy();
}
