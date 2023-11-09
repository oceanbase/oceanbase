#include "thread/easy_uthread.h"
#include <easy_test.h>

typedef struct {
    int                     start;
    int                     end;
    int                     ret;
    easy_uthread_t          *current;
    void                    *args;
} test_args_t;

#define UTHREAD_CREATE_AND_SCHEDULER(start_routine, expect_sche_ret) \
    easy_uthread_control_t  control; \
    easy_uthread_t          *uts[uthread_count]; \
    test_args_t             ut_args[uthread_count]; \
    int                     i = 0; \
    memset(ut_args, 0, sizeof(ut_args)); \
    easy_uthread_init(&control); \
    for (i = 0; i < uthread_count; i++) { \
        uts[i] = easy_uthread_create(start_routine, &ut_args[i], 4096); \
        EXPECT_TRUE(uts[i] != NULL); } \
    EXPECT_EQ(easy_uthread_scheduler(), expect_sche_ret);

extern __thread easy_uthread_control_t *easy_uthread_var;
static int              yield_value = 0;

static void easy_uthread_create_start_routine(void *args)
{
    long                    *v = (long *)args;
    (*v) ++;
}

TEST(easy_uthread, easy_uthread_create)
{
    easy_uthread_control_t  control;
    easy_uthread_t          *ut;
    long                    args = 0;

    ut = easy_uthread_create(easy_uthread_create_start_routine, &args, 4096);
    EXPECT_TRUE(ut == NULL);
    easy_uthread_init(&control);

    ut = easy_uthread_create(easy_uthread_create_start_routine, &args, 4096);
    EXPECT_TRUE(ut != NULL);
    easy_uthread_scheduler();
    EXPECT_EQ(args, 1);

    easy_uthread_destroy();
}


TEST(easy_uthread, easy_uthread_init)
{
    easy_uthread_control_t  control, control1;
    easy_uthread_t          *ut;
    long                    args = 0;

    ut = easy_uthread_create(easy_uthread_create_start_routine, &args, 4096);
    EXPECT_TRUE(ut == NULL);
    easy_uthread_init(&control);
    easy_uthread_init(&control1);
    EXPECT_EQ((long)&control, (long)easy_uthread_var);

    ut = easy_uthread_create(easy_uthread_create_start_routine, &args, 4096);
    EXPECT_TRUE(ut != NULL);
    easy_uthread_scheduler();
    EXPECT_EQ(args, 1);

    easy_uthread_destroy();
    easy_uthread_destroy();
}

static void easy_uthread_yield_start(void *args)
{
    test_args_t             *t = (test_args_t *)args;
    t->start = yield_value;
    yield_value ++;
    t->ret = easy_uthread_yield();
    t->end = yield_value;
    yield_value ++;
}

void test_easy_uthread_yield(int uthread_count)
{
    yield_value = 0;
    UTHREAD_CREATE_AND_SCHEDULER(easy_uthread_yield_start, 0);

    for (i = 0; i < uthread_count; i++) {
        EXPECT_EQ(ut_args[i].start, i);
        EXPECT_EQ(ut_args[i].end, uthread_count + i);
        EXPECT_EQ(ut_args[i].ret, uthread_count - 1);
    }

    easy_uthread_t          *t;
    int                     thread_count = uthread_count;
    easy_list_for_each_entry(t, &control.thread_list, thread_list_node) {
        if (t->exiting != 1)
            thread_count --;
    }
    EXPECT_EQ(control.thread_count, 0);
    EXPECT_EQ(thread_count, uthread_count);

    easy_uthread_destroy();
}

TEST(easy_uthread, easy_uthread_yield)
{
    test_easy_uthread_yield(1);
    test_easy_uthread_yield(10);
}

static void easy_uthread_switch_start(void *args)
{
    test_args_t             *t = (test_args_t *)args;
    t->end = t->start = t->start + 1;
    easy_uthread_switch();
    t->end = t->start + 1;
}

void test_easy_uthread_switch(int uthread_count)
{
    UTHREAD_CREATE_AND_SCHEDULER(easy_uthread_switch_start, 1);

    for (i = 0; i < uthread_count; i++) {
        EXPECT_EQ(ut_args[i].start, 1);
        EXPECT_EQ(ut_args[i].end, 1);
    }

    easy_uthread_t          *t;
    int                     thread_count = 0;
    easy_list_for_each_entry(t, &control.thread_list, thread_list_node) {
        if (t->exiting != 1)
            thread_count ++;
    }
    EXPECT_EQ(thread_count, uthread_count);
    EXPECT_EQ(control.thread_count, uthread_count);

    easy_uthread_destroy();
}

TEST(easy_uthread, easy_uthread_switch)
{
    test_easy_uthread_switch(1);
    test_easy_uthread_switch(10);
}

void test_easy_uthread_ready(int uthread_count)
{
    UTHREAD_CREATE_AND_SCHEDULER(easy_uthread_switch_start, 1);

    for (i = 0; i < uthread_count; i++) {
        EXPECT_EQ(ut_args[i].start, 1);
        EXPECT_EQ(ut_args[i].end, 1);
    }

    easy_uthread_t          *t;
    int                     thread_count = 0;
    easy_list_for_each_entry(t, &control.thread_list, thread_list_node) {
        if (t->exiting != 1)
            thread_count ++;
    }
    EXPECT_EQ(thread_count, uthread_count);
    EXPECT_EQ(control.thread_count, uthread_count);

    easy_list_for_each_entry(t, &control.thread_list, thread_list_node) {
        easy_uthread_ready(t);
    }

    easy_uthread_scheduler();

    thread_count = uthread_count;

    for (i = 0; i < uthread_count; i++) {
        EXPECT_EQ(ut_args[i].start, 1);
        EXPECT_EQ(ut_args[i].end, 2);
    }

    easy_list_for_each_entry(t, &control.thread_list, thread_list_node) {
        if (t->exiting != 1)
            thread_count --;
    }
    EXPECT_EQ(thread_count, uthread_count);
    EXPECT_EQ(control.thread_count, 0);

    easy_uthread_destroy();
}

TEST(easy_uthread, easy_uthread_ready)
{
    test_easy_uthread_ready(1);
    test_easy_uthread_ready(10);
}

void test_easy_uthread_destroy(int uthread_count)
{
    UTHREAD_CREATE_AND_SCHEDULER(easy_uthread_switch_start, 1);

    for (i = 0; i < uthread_count; i++) {
        EXPECT_EQ(ut_args[i].start, 1);
        EXPECT_EQ(ut_args[i].end, 1);
    }

    easy_uthread_t          *t;
    int                     thread_count = 0;
    easy_list_for_each_entry(t, &control.thread_list, thread_list_node) {
        if (t->exiting != 1)
            thread_count ++;
    }

    EXPECT_EQ(thread_count, uthread_count);
    EXPECT_TRUE(easy_uthread_var);
    EXPECT_FALSE(easy_list_empty(&control.thread_list));
    easy_uthread_destroy();
    EXPECT_FALSE(easy_uthread_var);
}

TEST(easy_uthread, easy_uthread_destroy)
{
    test_easy_uthread_destroy(1);
    test_easy_uthread_destroy(10);
}

static void easy_uthread_current_start(void *args)
{
    test_args_t             *t = (test_args_t *)args;
    t->current = easy_uthread_current();
}

void test_easy_uthread_current(int uthread_count)
{
    UTHREAD_CREATE_AND_SCHEDULER(easy_uthread_current_start, 0);

    for (i = 0; i < uthread_count; i++) {
        EXPECT_TRUE(ut_args[i].current == uts[i]);
    }

    EXPECT_TRUE(easy_uthread_var);
    EXPECT_TRUE(easy_list_empty(&control.thread_list));
    easy_uthread_destroy();
    EXPECT_FALSE(easy_uthread_var);
}

TEST(easy_uthread, easy_uthread_current)
{
    test_easy_uthread_current(1);
    test_easy_uthread_current(10);
}

static void easy_uthread_stop_start(void *args)
{
    test_args_t             *t = (test_args_t *)args;
    t->end = t->start = t->start + 1;
    easy_uthread_stop();
    t->end = t->start + 1;
}

void test_easy_uthread_stop(int uthread_count)
{
    UTHREAD_CREATE_AND_SCHEDULER(easy_uthread_stop_start, 0);

    EXPECT_EQ(ut_args[0].start, 1);
    EXPECT_EQ(ut_args[0].end, 2);

    for(i = 1; i < uthread_count; i++) {
        EXPECT_EQ(ut_args[i].start, 0);
        EXPECT_EQ(ut_args[i].end, 0);
    }

    EXPECT_TRUE(easy_uthread_var);
    EXPECT_EQ(easy_uthread_var->thread_count, uthread_count - 1);
    easy_uthread_destroy();
    EXPECT_FALSE(easy_uthread_var);
}

TEST(easy_uthread, easy_uthread_stop)
{
    test_easy_uthread_stop(1);
    test_easy_uthread_stop(10);
}
typedef struct {
    easy_uthread_t          *uthread;
} test_1_t;
static void test_print_1(void *args)
{
    test_1_t                *a = (test_1_t *) args;

    while (1) {
        if (a->uthread) {
            easy_uthread_ready(a->uthread);
            easy_uthread_set_errcode(a->uthread, 1);
            a->uthread = NULL;
            break;
        }

        easy_uthread_print(0);
        easy_uthread_yield();
    }
}
static void test_print_2(void *args)
{
    test_1_t                *a = (test_1_t *) args;

    if ((a->uthread = easy_uthread_current()) == NULL) {
        return;
    } else {
        easy_uthread_switch();
        EXPECT_EQ(easy_uthread_get_errcode(), 1);
    }
}
TEST(easy_uthread, print)
{
    easy_uthread_control_t  control;
    test_1_t                a;
    a.uthread = NULL;
    easy_uthread_init(&control);
    easy_uthread_get_errcode();
    easy_uthread_ready(NULL);

    easy_uthread_create(test_print_1, &a, 65536);
    easy_uthread_create(test_print_2, &a, 65536);

    easy_uthread_scheduler();
    easy_uthread_destroy();
}

static void *test_realloc (void *ptr, size_t size)
{
    return NULL;
}
TEST(easy_uthread, other)
{
    easy_uthread_control_t  control;
    easy_uthread_init(&control);

    easy_pool_set_allocator(test_realloc);
    easy_uthread_create(NULL, NULL, 10);
    easy_pool_set_allocator(easy_test_realloc);

    easy_uthread_destroy();

    EXPECT_TRUE(easy_uthread_current() == NULL);
    EXPECT_TRUE(easy_uthread_get_errcode() == 0);

}
