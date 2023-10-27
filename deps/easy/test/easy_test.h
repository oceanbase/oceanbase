#ifndef EASY_TEST_H_
#define EASY_TEST_H_

#include "easy_define.h"
#include <easy_list.h>
#include "util/easy_hash.h"
#include "util/easy_pool.h"
#include <easy_atomic.h>
#include <stdint.h>
#include <getopt.h>
#include <sys/time.h>
#include "util/easy_string.h"

EASY_CPP_START

typedef struct easy_test_func_t easy_test_func_t;
typedef struct easy_test_case_t easy_test_case_t;
typedef struct cmdline_param_t cmdline_param_t;
typedef void easy_test_func_pt();

struct easy_test_case_t {
    const char                *case_name;
    easy_test_func_pt         *fcsetup;
    easy_test_func_pt         *fcdown;
    easy_test_func_pt         *fsetup;
    easy_test_func_pt         *fdown;
    easy_list_t               listnode;
    easy_hash_list_t          hash_node;
    easy_list_t               list;
    int                       list_cnt;
};

// struct test
struct easy_test_func_t {
    easy_test_case_t          *tc;
    const char                *func_name;
    easy_test_func_pt         *func;
    easy_list_t               listnode;
    int                       ret;
};
// cmdline parameter
struct cmdline_param_t {
    const char                *filter_str;
    int                       filter_str_len;
    int                       filter_flags;
};

#define EASY_TEST_COLOR_RED   1
#define EASY_TEST_COLOR_GREEN 2

// extern
extern easy_pool_t      *easy_test_pool;
extern easy_hash_t      *easy_test_case_table;
extern easy_list_t      easy_test_case_list;
extern int              easy_test_retval;
extern easy_atomic_t    easy_test_alloc_byte;

// color printf
static inline void easy_test_color_printf(int color, const char *fmt, ...)
{
    va_list                 args;
    va_start(args, fmt);
    printf("\033[0;3%dm", color);
    vprintf(fmt, args);
    printf("\033[m");
    va_end(args);
}

static int64_t easy_test_now()
{
    struct timeval          tv;
    gettimeofday (&tv, 0);
    return 1000L * tv.tv_sec + tv.tv_usec / 1000;
}

static int easy_test_case_cmp(const void *a, const void *b)
{
    easy_test_case_t        *tc = (easy_test_case_t *) b;
    return strcmp(tc->case_name, (const char *)a);
}

static easy_test_case_t *easy_test_get_tc(const char *case_name)
{
    easy_test_case_t        *tc;
    uint64_t                key;

    if (easy_test_pool == NULL) {
        easy_test_pool = easy_pool_create(1024);
        easy_test_case_table = easy_hash_create(easy_test_pool, 128,
                                                offsetof(easy_test_case_t, hash_node));
    }

    // 处理case
    key = easy_hash_code(case_name, strlen(case_name), 3);
    tc = (easy_test_case_t *)easy_hash_find_ex(easy_test_case_table, key, easy_test_case_cmp, case_name);

    if (tc == NULL) {
        tc = (easy_test_case_t *)easy_pool_calloc(easy_test_pool, sizeof(easy_test_case_t));
        tc->case_name = case_name;
        easy_list_init(&tc->list);
        easy_hash_add(easy_test_case_table, key, &tc->hash_node);
        easy_list_add_head(&tc->listnode, &easy_test_case_list);
    }

    return tc;
}

static inline void easy_test_reg_func(const char *case_name, const char *func_name,
                                      easy_test_func_pt *func, int before)
{
    easy_test_func_t        *t;
    easy_test_case_t        *tc;

    tc = easy_test_get_tc(case_name);
    t = (easy_test_func_t *)easy_pool_calloc(easy_test_pool, sizeof(easy_test_func_t));
    t->func_name = func_name;
    t->func = func;
    t->tc = tc;

    easy_list_add_head(&t->listnode, &tc->list);
    tc->list_cnt ++;
}

static inline void easy_test_print_usage(char *prog_name)
{
    fprintf(stderr, "%s [-f [-]filter_string]\n"
            "    -f, --filter            filter string\n"
            "    -l, --list              list tests\n"
            "    -h, --help              display this help and exit\n"
            "    -V, --version           version and build time\n\n", prog_name);
}

/**
 * 打印tests case
 */
static inline void easy_test_print_list()
{
    easy_test_case_t        *tc;
    easy_test_func_t        *t;

    // foreach
    easy_list_for_each_entry(tc, &easy_test_case_list, listnode) {
        printf("%s.\n", tc->case_name);
        easy_list_for_each_entry(t, &tc->list, listnode) {
            printf("  %s.%s\n", tc->case_name, t->func_name);
        }
    }
}

static inline int easy_test_is_skip(const char *str, cmdline_param_t *cp)
{
    int                     ret;

    if (cp->filter_str_len > 0) {
        ret = strncmp(cp->filter_str, str, cp->filter_str_len);

        if ((ret != 0 && cp->filter_flags == 0) || (ret == 0 && cp->filter_flags != 0))
            return 1;
    } else if (cp->filter_str_len < 0) {
        ret = strcmp(cp->filter_str, str);

        if ((ret != 0 && cp->filter_flags == 0) || (ret == 0 && cp->filter_flags != 0))
            return 1;
    }

    return 0;
}

/**
 * 解析命令行
 */
static inline int easy_test_parse_cmd_line(int argc, char *const argv[], cmdline_param_t *cp)
{
    int                     opt, len;
    const char              *opt_string = "hVf:l";
    struct option           long_opts[] = {
        {"filter", 1, NULL, 'f'},
        {"list", 0, NULL, 'l'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'V'},
        {0, 0, 0, 0}
    };

    opterr = 0;

    while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
        switch (opt) {
        case 'f':

            if (*optarg == '-') {
                cp->filter_flags = 1;
                cp->filter_str = optarg + 1;
            } else {
                cp->filter_str = optarg;
            }

            len = strlen(cp->filter_str);

            if (len > 0 && (cp->filter_str[len - 1] == '*' || cp->filter_str[len - 1] == '?'))
                cp->filter_str_len = len - 1;
            else
                cp->filter_str_len = -1;

            break;

        case 'l':
            easy_test_print_list();
            return EASY_ERROR;

        case 'V':
            fprintf(stderr, "BUILD_TIME: %s %s\n", __DATE__, __TIME__);
            return EASY_ERROR;

        case 'h':
        default:
            easy_test_print_usage(argv[0]);
            return EASY_ERROR;
        }
    }

    return EASY_OK;
}

static inline void *easy_test_realloc (void *ptr, size_t size)
{
    char                    *p1, *p = (char *)ptr;

    if (p) {
        p -= 8;
        easy_atomic_add(&easy_test_alloc_byte, -(*((int *)p)));
    }

    if (size) {
        easy_atomic_add(&easy_test_alloc_byte, size);
        p1 = (char *)easy_realloc(p, size + 8);

        if (p1) {
            *((int *)p1) = size;
            p1 += 8;
        }

        return p1;
    } else if (p) {
        easy_free(p);
    }

    return NULL;
}

static int easy_test_exec_case(easy_test_case_t *tc)
{
    easy_test_func_t        *t;
    int64_t                 t1, t2;
    int                     failcnt = 0;

    easy_test_color_printf(EASY_TEST_COLOR_GREEN, "[----------]");
    printf(" %d tests from %s\n", tc->list_cnt, tc->case_name);

    if (tc->fcsetup) (*tc->fcsetup)();

    easy_list_for_each_entry(t, &tc->list, listnode) {
        // start run
        easy_test_color_printf(EASY_TEST_COLOR_GREEN, "[ RUN      ]");
        printf(" %s.%s\n", tc->case_name, t->func_name);

        easy_test_retval = 0;
        t1 = easy_test_now();

        if (tc->fsetup) (*tc->fsetup)();

        (t->func)();

        if (tc->fdown) (*tc->fdown)();

        t2 = easy_test_now();
        t->ret = easy_test_retval;

        // failure
        if (easy_test_retval) {
            easy_test_color_printf(EASY_TEST_COLOR_RED, "[  FAILED  ]");
            failcnt ++;
        } else {
            easy_test_color_printf(EASY_TEST_COLOR_GREEN, "[       OK ]");
        }

        printf(" %s.%s (%d ms)\n", tc->case_name, t->func_name, (int)(t2 - t1));
    }

    if (tc->fcdown) (*tc->fcdown)();

    easy_test_color_printf(EASY_TEST_COLOR_GREEN, "[----------]");
    printf(" %d tests from %s\n\n", tc->list_cnt, tc->case_name);
    return failcnt;
}

static inline int easy_test_main(int argc, char *argv[])
{
    easy_test_case_t        *tc, *tc1;
    easy_test_func_t        *t, *nt;
    int64_t                 t1, t2;
    int                     total_failcnt, total_func_cnt, total_case_cnt;
    char                    test_func_name[256];
    cmdline_param_t         cp;

    // parse cmd
    memset(&cp, 0, sizeof(cmdline_param_t));

    if (easy_test_parse_cmd_line(argc, argv, &cp) == EASY_ERROR) {
        return -1;
    }

    // init
    easy_test_color_printf(EASY_TEST_COLOR_GREEN, "[==========]");

    // 计算个数
    total_func_cnt = 0;
    total_case_cnt = 0;
    total_failcnt = 0;
    easy_list_for_each_entry_safe(tc, tc1, &easy_test_case_list, listnode) {
        if (cp.filter_str_len) {
            easy_list_for_each_entry_safe(t, nt, &tc->list, listnode) {
                snprintf(test_func_name, 256, "%s.%s", tc->case_name, t->func_name);

                if (easy_test_is_skip(test_func_name, &cp)) {
                    easy_list_del(&t->listnode);
                    tc->list_cnt --;
                }
            }
        }

        if (tc->list_cnt == 0) {
            easy_list_del(&tc->listnode);
        } else {
            total_func_cnt += tc->list_cnt;
            total_case_cnt ++;
        }
    }

    printf(" Running %d tests from %d cases.\n", total_func_cnt, total_case_cnt);

    t1 = easy_test_now();
    easy_pool_set_allocator(easy_test_realloc);
    easy_list_for_each_entry(tc, &easy_test_case_list, listnode) {
        total_failcnt += easy_test_exec_case(tc);
    }
    t2 = easy_test_now();

    easy_test_color_printf(EASY_TEST_COLOR_GREEN, "[==========]");
    printf(" %d tests ran. (%d ms total)\n", total_func_cnt, (int)(t2 - t1));
    easy_test_color_printf(EASY_TEST_COLOR_GREEN, "[  PASSED  ]");
    printf(" %d tests.\n", total_func_cnt - total_failcnt);

    if (total_failcnt > 0) {
        easy_test_color_printf(EASY_TEST_COLOR_RED, "[  FAILED  ]");
        printf(" %d tests, listed below:\n", total_failcnt);
        easy_list_for_each_entry(tc, &easy_test_case_list, listnode) {
            easy_list_for_each_entry(t, &tc->list, listnode) {
                if (!t->ret) continue;

                easy_test_color_printf(EASY_TEST_COLOR_RED, "[  FAILED  ]");
                printf(" %s.%s\n", tc->case_name, t->func_name);
            }
        }

        printf(" %d FAILED TEST\n", total_failcnt);
    }

    if (easy_test_alloc_byte) {
        printf("no free memory: %ld\n", (long)easy_test_alloc_byte);
    }

    return (total_failcnt > 0 ? 1 : 0);
}

#define EASY_TEST_MAIN_DEFINE                                                           \
    int                     easy_test_retval = 0;                                                           \
    easy_atomic_t           easy_test_alloc_byte = 0;                                             \
    easy_pool_t             *easy_test_pool = NULL;                                                 \
    easy_hash_t             *easy_test_case_table = NULL;                                           \
    easy_list_t             easy_test_case_list = EASY_LIST_HEAD_INIT(easy_test_case_list);         \
    __attribute__((destructor)) void unregtest_main() {                                 \
        easy_pool_set_allocator(NULL);                                                  \
        if (easy_test_pool) easy_pool_destroy(easy_test_pool);                          \
    }

#define RUN_TEST_MAIN                                                                   \
    EASY_TEST_MAIN_DEFINE                                                               \
    int main(int argc, char *argv[]) {                                                  \
        return easy_test_main(argc, argv);                                              \
    }

#define TEST_NAME(case_name, func_name) easy_testf_##case_name##_##func_name
#define TEST_CASE(case_name, func_name) easy_testc_##case_name##_##func_name
// TEST
#define TEST(case_name, func_name)                                                      \
    void TEST_NAME(case_name, func_name)();                                             \
    __attribute__((constructor)) void easy_testg_##case_name##_##func_name() {          \
        easy_test_reg_func(#case_name, #func_name,                                      \
                           TEST_NAME(case_name, func_name), 1);                         \
    }                                                                                   \
    void TEST_NAME(case_name, func_name)()

#define TEST_SETUP_DOWN(case_name, func_name)                                           \
    void TEST_CASE(case_name, func_name)();                                             \
    __attribute__((constructor)) void easy_testd_##case_name##_##func_name() {          \
        easy_test_case_t        *tc = easy_test_get_tc(#case_name);                            \
        tc->f##func_name = TEST_CASE(case_name, func_name);                             \
    }                                                                                   \
    void TEST_CASE(case_name, func_name)()

#define TEST_CASE_SETUP(case_name) TEST_SETUP_DOWN(case_name, csetup)
#define TEST_CASE_DOWN(case_name) TEST_SETUP_DOWN(case_name, cdown)

#define TEST_SETUP(case_name) TEST_SETUP_DOWN(case_name, setup)
#define TEST_DOWN(case_name) TEST_SETUP_DOWN(case_name, down)

// TEST_FAIL
#define TEST_FAIL(fmt, args...)                                                         \
    printf("ERROR at %s:%d, TEST_FAIL: " fmt "\n",                                  \
           __FILE__, __LINE__, ## args); easy_test_retval = 1;

// EXPECT_TRUE
#define EXPECT_TRUE(c) if(!(c)) {                                                       \
        printf("ERROR at %s:%d, EXPECT_TRUE(" #c ")\n",                                 \
               __FILE__, __LINE__); easy_test_retval = 1;}

// EXPECT_FALSE
#define EXPECT_FALSE(c) if((c)) {                                                       \
        printf("ERROR at %s:%d, EXPECT_FALSE(" #c ")\n",                                \
               __FILE__, __LINE__); easy_test_retval = 1;}

EASY_CPP_END

#ifndef __cplusplus
// EXPECT_EQ
#define EXPECT_EQ(a, b) if(!((int64_t)(a)==(int64_t)(b))) {                                 \
        printf("ERROR at %s: %d, EXPECT_EQ(" #a ", " #b ") (a=%" PRId64 ",b=%" PRId64 ")\n",\
               __FILE__, __LINE__, (int64_t)(a), (int64_t)(b)); easy_test_retval = 1;}

// EXPECT_NE
#define EXPECT_NE(a, b) if(((int64_t)(a)==(int64_t)(b))) {                                  \
        printf("ERROR at %s: %d, EXPECT_NE(" #a ", " #b ") (a=%" PRId64 ",b=%" PRId64 ")\n",\
               __FILE__, __LINE__, (int64_t)(a), (int64_t)(b)); easy_test_retval = 1;}

#else

#include <string>
#define EASY_STRINGIFY(x) #x
#define EASY_TOSTRING(x) EASY_STRINGIFY(x)

// EXPECT_EQ
#define EXPECT_EQ(a, b)                                                                     \
    EXPECT_EASY_EQ(__FILE__ ":" EASY_TOSTRING(__LINE__) ", EXPECT_EQ(" #a ", " #b ")",  \
                   (a), (b), false)

// EXPECT_NE
#define EXPECT_NE(a, b)                                                                     \
    EXPECT_EASY_EQ(__FILE__ ":" EASY_TOSTRING(__LINE__) ", EXPECT_NE(" #a ", " #b ")",  \
                   (a), (b), true)

template <typename A, typename B>
static inline void EXPECT_EASY_EQ(const char *location, const A &a, const B &b, bool neq)
{
    if (!((int64_t)(a) == (int64_t)(b)) ^ neq) {
        printf("ERROR at %s (a=%" PRId64 ",b=%" PRId64 ")\n",
               location, (int64_t)(a), (int64_t)(b));
        easy_test_retval = 1;
    }
}

static inline void EXPECT_EASY_EQ(
    const char *location, const std::string &a, const std::string &b, bool neq)
{
    if (!(a == b) ^ neq) {
        printf("ERROR at %s\na=%s\nb=%s\n",
               location, a.c_str(), b.c_str());
        easy_test_retval = 1;
    }
}

static inline void EXPECT_EASY_EQ(
    const char *location, const std::string &a, const char *const bcs, bool neq)
{
    EXPECT_EASY_EQ(location, a, std::string(bcs), neq);
}

static inline void EXPECT_EASY_EQ(
    const char *location, const char *const acs, const std::string &b, bool neq)
{
    EXPECT_EASY_EQ(location, std::string(acs), b, neq);
}

static inline void EXPECT_EASY_EQ(
    const char *location, const char *const acs, const char *const bcs, bool neq)
{
    EXPECT_EASY_EQ(location, std::string(acs), std::string(bcs), neq);
}
#endif

#endif
