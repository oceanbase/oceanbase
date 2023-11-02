#include "easy_atomic.h"
#include "thread/easy_uthread.h"
#include <signal.h>

/**
 * 用户态线程
 */

__thread easy_uthread_control_t *easy_uthread_var = NULL;

static easy_uthread_t *easy_uthread_alloc(easy_uthread_start_pt *fn, void *args, int stack_size);
static void easy_uthread_start(uint32_t y, uint32_t x);
static void easy_uthread_context_switch(ucontext_t *from, ucontext_t *to);

/**
 * 需要初始化一下
 */
void easy_uthread_init(easy_uthread_control_t *control)
{
    if (easy_uthread_var == NULL) {
        easy_uthread_var = control;
        memset(easy_uthread_var, 0, sizeof(easy_uthread_control_t));
        easy_list_init(&easy_uthread_var->runqueue);
        easy_list_init(&easy_uthread_var->thread_list);
    }
}

/**
 * 释放掉所有资源
 */
void easy_uthread_destroy()
{
    easy_uthread_t          *t, *t2;

    if (!easy_uthread_var)
        return;

    easy_list_for_each_entry_safe(t, t2, &easy_uthread_var->thread_list, thread_list_node) {
        easy_pool_destroy(t->pool);
    }

    easy_uthread_var = NULL;
}

/**
 * 停止运行
 */
void easy_uthread_stop()
{
    if (easy_uthread_var) {
        easy_uthread_var->stoped = 1;
    }
}

/**
 * 创建一用户态线程
 */
easy_uthread_t *easy_uthread_create(easy_uthread_start_pt *start_routine, void *args, int stack_size)
{
    easy_uthread_t          *t;

    if (!easy_uthread_var)
        return NULL;

    if ((t = easy_uthread_alloc(start_routine, args, stack_size)) == NULL)
        return NULL;

    easy_uthread_var->thread_count ++;
    easy_list_add_tail(&t->thread_list_node, &easy_uthread_var->thread_list);

    easy_uthread_ready(t);
    return t;
}

/**
 * 退出线程数
 */
void easy_uthread_exit(int val)
{
    easy_uthread_var->exit_value = val;
    easy_uthread_var->running->exiting = 1;
    easy_uthread_switch();
}

/**
 * 切换给其他CPU
 */
void easy_uthread_switch()
{
    easy_uthread_var->running->errcode = 0;
    easy_uthread_needstack(0);
    easy_uthread_context_switch(&easy_uthread_var->running->context, &easy_uthread_var->context);
}

/**
 * 得到当前正常运行的easy_uthread_t
 */
easy_uthread_t *easy_uthread_current()
{
    return (easy_uthread_var ? easy_uthread_var->running : NULL);
}

/**
 * 得到errcode
 */
int easy_uthread_get_errcode()
{
    if (easy_uthread_var && easy_uthread_var->running)
        return easy_uthread_var->running->errcode;
    else
        return 0;
}
/**
 * 设置errcode
 */
void easy_uthread_set_errcode(easy_uthread_t *t, int errcode)
{
    t->errcode = (errcode & 0xff);
}

/**
 * stack检查.
 */
void easy_uthread_needstack(int n)
{
    easy_uthread_t          *t;

    t = easy_uthread_var->running;

    if((char *)&t <= (char *)t->stk || (char *)&t - (char *)t->stk < 256 + n) {
        fprintf(stderr, "uthread stack overflow: &t=%p tstk=%p n=%d\n", &t, t->stk, 256 + n);
        abort();
    }
}

/**
 * 准备就绪, 加入到运行队列中
 */
void easy_uthread_ready(easy_uthread_t *t)
{
    if (t) {
        t->ready = 1;
        easy_list_add_tail(&t->runqueue_node, &easy_uthread_var->runqueue);
    }
}

/**
 * 空闲,释放CPU
 */
int easy_uthread_yield()
{
    int                     n;

    n = easy_uthread_var->nswitch;
    easy_uthread_ready(easy_uthread_var->running);
    easy_uthread_switch();
    return easy_uthread_var->nswitch - n - 1;
}

/**
 * 调度控制
 */
int easy_uthread_scheduler()
{
    easy_uthread_t          *t;

    while(easy_uthread_var->stoped == 0) {
        if(easy_uthread_var->thread_count == 0) {
            break;
        }

        if (easy_list_empty(&easy_uthread_var->runqueue)) {
            fprintf(stderr, "no runnable user thread! (%d)\n", easy_uthread_var->thread_count);
            easy_uthread_var->exit_value = 1;
            break;
        }

        // first entry
        t = easy_list_entry(easy_uthread_var->runqueue.next, easy_uthread_t, runqueue_node);
        easy_list_del(&t->runqueue_node);

        t->ready = 0;
        easy_uthread_var->running = t;
        easy_uthread_var->nswitch ++;

        // 切换回去运行
        easy_uthread_context_switch(&easy_uthread_var->context, &t->context);
        easy_uthread_var->running = NULL;

        // 用户线程退出了
        if(t->exiting) {
            easy_list_del(&t->thread_list_node);
            easy_uthread_var->thread_count --;
            easy_pool_destroy(t->pool);
        }
    }

    return easy_uthread_var->exit_value;
}

/**
 * 打印出uthread信息
 */
void easy_uthread_print(int sig)
{
    easy_uthread_t          *t;
    char                    *extra;

    fprintf(stderr, "uthread list:\n");
    easy_list_for_each_entry(t, &easy_uthread_var->thread_list, thread_list_node) {
        if(t == easy_uthread_var->running)
            extra = " (running)";
        else if(t->ready)
            extra = " (ready)";
        else
            extra = "";

        fprintf(stderr, "%6d %s\n", t->id, extra);
    }
}
///////////////////////////////////////////////////////////////////////////////////////////////////
static easy_uthread_t *easy_uthread_alloc(easy_uthread_start_pt *fn, void *args, int stack_size)
{
    easy_uthread_t          *t;
    easy_pool_t             *pool;
    int                     size;
    sigset_t                zero;
    uint32_t                x, y;
    uint64_t                z;

    // 创建一个pool
    size = sizeof(easy_uthread_t) + stack_size;

    if ((pool = easy_pool_create(size)) == NULL)
        return NULL;

    if ((t = (easy_uthread_t *) easy_pool_alloc(pool, size)) == NULL)
        goto error_exit;

    // 初始化
    memset(t, 0, sizeof(easy_uthread_t));
    t->pool = pool;
    t->stk = (unsigned char *) (t + 1);
    t->stksize = stack_size;
    t->id = ++ easy_uthread_var->gid;
    t->startfn = fn;
    t->startargs = args;

    /* do a reasonable initialization */
    memset(&t->context, 0, sizeof(t->context));
    sigemptyset(&zero);
    sigprocmask(SIG_BLOCK, &zero, &t->context.uc_sigmask);

    /* must initialize with current context */
    if(getcontext(&t->context) < 0)
        goto error_exit;

    /* call makecontext to do the real work. */
    t->context.uc_stack.ss_sp = t->stk;
    t->context.uc_stack.ss_size = t->stksize;
    z = (unsigned long)t;
    y = (uint32_t)z;
    x = (uint32_t)(z >> 32);

    makecontext(&t->context, (void( *)())easy_uthread_start, 2, y, x);

    return t;
error_exit:

    if (pool)
        easy_pool_destroy(pool);

    return NULL;
}

static void easy_uthread_start(uint32_t y, uint32_t x)
{
    uint64_t                z;

    z = x;
    z <<= 32;
    z |= y;
    easy_uthread_t          *t = (easy_uthread_t *)(long)z;
    t->startfn(t->startargs);
    easy_uthread_exit(0);
}

static void easy_uthread_context_switch(ucontext_t *from, ucontext_t *to)
{
    if(swapcontext(from, to) < 0) {
        fprintf(stderr, "swapcontext failed.\n");
        abort();
    }
}

