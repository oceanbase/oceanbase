#include "easy_list.h"
#include <easy_test.h>

/**
 * 测试 easy_list
 */
typedef struct test_object_t {
    easy_list_t             node;
    int                     id;
    int                     del;
} test_object_t;

TEST(easy_list, add_tail)
{
    int                     i, size;
    easy_list_t             *ptr;
    easy_list_t             list = EASY_LIST_HEAD_INIT(list);
    test_object_t           n[10];

    for(i = 0; i < 10; i++) {
        easy_list_init(&(n[i].node));
        n[i].id = i + 1;
    }

    // add
    for(i = 0; i < 10; i++) {
        easy_list_add_tail(&n[i].node, &list);
    }

    // check
    size = 0;

    for(i = 0; i < 10; i++) {
        ptr = ((i < 9) ? &n[i + 1].node : &list);

        if (n[i].node.next == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    size = 0;

    for(i = 9; i >= 0; i--) {
        ptr = ((i > 0) ? &n[i - 1].node : &list);

        if (n[i].node.prev == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    EXPECT_TRUE(list.next == &n[0].node);
    EXPECT_TRUE(list.prev == &n[9].node);
}

TEST(easy_list, add_head)
{
    int                     i, size;
    easy_list_t             *ptr;
    easy_list_t             list = EASY_LIST_HEAD_INIT(list);
    test_object_t           n[10];

    for(i = 0; i < 10; i++) {
        easy_list_init(&(n[i].node));
        n[i].id = i + 1;
    }

    // add
    for(i = 0; i < 10; i++) {
        easy_list_add_head(&n[i].node, &list);
    }

    // check
    size = 0;

    for(i = 0; i < 10; i++) {
        ptr = ((i < 9) ? &n[i + 1].node : &list);

        if (n[i].node.prev == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    size = 0;

    for(i = 9; i >= 0; i--) {
        ptr = ((i > 0) ? &n[i - 1].node : &list);

        if (n[i].node.next == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    EXPECT_TRUE(list.prev == &n[0].node);
    EXPECT_TRUE(list.next == &n[9].node);
}

TEST(easy_list, del)
{
    int                     j, i, size, cnt, k;
    easy_list_t             *ptr;
    easy_list_t             list = EASY_LIST_HEAD_INIT(list);
    test_object_t           n[10];

    // init
    for(i = 0; i < 10; i++) {
        easy_list_add_tail(&n[i].node, &list);
        n[i].id = i + 1;
        n[i].del = 0;
    }

    // check
    cnt = 10;
    int                     idx[] = {0, 9, 5, 4, 1, 2, 8, 6, 7, 3};

    for(j = 0; j < 10; j++) {
        size = 0;

        for(i = 0; i < 10; i++) {
            if (n[i].del) continue;

            ptr = &list;

            for(k = i + 1; k < 10; k++) {
                if (n[k].del) continue;

                ptr = &n[k].node;
                break;
            }

            if (n[i].node.next == ptr) size ++;
        }

        EXPECT_EQ(size, cnt);

        size = 0;

        for(i = 9; i >= 0; i--) {
            if (n[i].del) continue;

            ptr = &list;

            for(k = i - 1; k >= 0; k--) {
                if (n[k].del) continue;

                ptr = &n[k].node;
                break;
            }

            if (n[i].node.prev == ptr) size ++;
        }

        EXPECT_EQ(size, cnt);

        ptr = &list;

        for(k = 0; k < 10; k++) {
            if (n[k].del) continue;

            ptr = &n[k].node;
            break;
        }

        EXPECT_TRUE(list.next == ptr);

        ptr = &list;

        for(k = 9; k >= 0; k--) {
            if (n[k].del) continue;

            ptr = &n[k].node;
            break;
        }

        EXPECT_TRUE(list.prev == ptr);
        cnt --;
        easy_list_del(&n[idx[j]].node);
        n[idx[j]].del = 1;
    }

    EXPECT_TRUE(easy_list_empty(&list));
}

TEST(easy_list, movelist)
{
    int                     i, size, cnt;
    easy_list_t             *ptr;
    easy_list_t             list = EASY_LIST_HEAD_INIT(list);
    easy_list_t             newlist;
    test_object_t           n[10], *n1;

    // emtpy
    easy_list_movelist(&list, &newlist);
    EXPECT_TRUE(easy_list_empty(&list));
    EXPECT_TRUE(easy_list_empty(&newlist));

    // init
    for(i = 0; i < 10; i++) {
        easy_list_add_tail(&n[i].node, &list);
        n[i].id = i + 1;
        n[i].del = 0;
    }

    easy_list_movelist(&list, &newlist);
    EXPECT_TRUE(easy_list_empty(&list));
    EXPECT_FALSE(easy_list_empty(&newlist));

    // check
    size = 0;

    for(i = 0; i < 10; i++) {
        ptr = ((i < 9) ? &n[i + 1].node : &newlist);

        if (n[i].node.next == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    size = 0;

    for(i = 9; i >= 0; i--) {
        ptr = ((i > 0) ? &n[i - 1].node : &newlist);

        if (n[i].node.prev == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    EXPECT_TRUE(newlist.next == &n[0].node);
    EXPECT_TRUE(newlist.prev == &n[9].node);

    // foreach
    size = 0;
    cnt = 0;
    easy_list_for_each_entry(n1, &newlist, node) {
        cnt ++;

        if (n1->id == cnt) size ++;
    }
    EXPECT_EQ(size, 10);

    // list
    size = 0;
    cnt = 0;
    easy_list_for_each_entry(n1, &list, node) {
        cnt ++;

        if (n1->id == cnt) size ++;
    }
    EXPECT_EQ(size, 0);
}

TEST(easy_list, join)
{
    int                     i, size, cnt;
    easy_list_t             *ptr;
    easy_list_t             list = EASY_LIST_HEAD_INIT(list);
    easy_list_t             newlist = EASY_LIST_HEAD_INIT(newlist);
    test_object_t           n[20], *n1;

    // emtpy
    easy_list_join(&list, &newlist);
    EXPECT_TRUE(easy_list_empty(&newlist));

    // init
    for(i = 0; i < 10; i++) {
        easy_list_add_tail(&n[i].node, &list);
        n[i].id = i + 1;
        n[i].del = 0;
    }

    easy_list_join(&list, &newlist);
    EXPECT_FALSE(easy_list_empty(&newlist));

    // check
    size = 0;

    for(i = 0; i < 10; i++) {
        ptr = ((i < 9) ? &n[i + 1].node : &newlist);

        if (n[i].node.next == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    size = 0;

    for(i = 9; i >= 0; i--) {
        ptr = ((i > 0) ? &n[i - 1].node : &newlist);

        if (n[i].node.prev == ptr) size ++;
    }

    EXPECT_EQ(size, 10);

    EXPECT_TRUE(newlist.next == &n[0].node);
    EXPECT_TRUE(newlist.prev == &n[9].node);

    // foreach
    size = 0;
    cnt = 0;
    easy_list_for_each_entry(n1, &newlist, node) {
        cnt ++;

        if (n1->id == cnt) size ++;
    }
    EXPECT_EQ(size, 10);

    // again
    easy_list_init(&list);

    for(i = 10; i < 20; i++) {
        easy_list_add_tail(&n[i].node, &list);
        n[i].id = i + 1;
        n[i].del = 0;
    }

    easy_list_join(&list, &newlist);
    EXPECT_FALSE(easy_list_empty(&newlist));

    // check
    size = 0;

    for(i = 0; i < 20; i++) {
        ptr = ((i < 19) ? &n[i + 1].node : &newlist);

        if (n[i].node.next == ptr) size ++;
    }

    EXPECT_EQ(size, 20);

    size = 0;

    for(i = 19; i >= 0; i--) {
        ptr = ((i > 0) ? &n[i - 1].node : &newlist);

        if (n[i].node.prev == ptr) size ++;
    }

    EXPECT_EQ(size, 20);

    EXPECT_TRUE(newlist.next == &n[0].node);
    EXPECT_TRUE(newlist.prev == &n[19].node);

    // foreach
    size = 0;
    cnt = 0;
    easy_list_for_each_entry(n1, &newlist, node) {
        cnt ++;

        if (n1->id == cnt) size ++;
    }
    EXPECT_EQ(size, 20);
}

TEST(easy_list, foreach)
{
    int                     i, size;
    easy_list_t             list = EASY_LIST_HEAD_INIT(list);
    test_object_t           n[10], *n1, *n2;

    for(i = 0; i < 10; i++) {
        easy_list_add_tail(&n[i].node, &list);
        n[i].id = i + 1;
    }

    n1 = easy_list_entry(list.next, test_object_t, node);
    EXPECT_TRUE(n1 == &n[0]);
    n1 = easy_list_entry(list.prev, test_object_t, node);
    EXPECT_TRUE(n1 == &n[9]);

    // foreach safe
    i = size = 0;
    easy_list_for_each_entry_safe(n1, n2, &list, node) {
        i ++;

        if (n1->id == i) size ++;
    }
    EXPECT_EQ(size, 10);
}


