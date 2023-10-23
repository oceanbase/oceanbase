#include "util/easy_string.h"
#include <easy_test.h>

/**
 * 测试 easy_string
 */

// char *easy_strncpy(char *dst, const char *src, size_t n)
TEST(easy_string, strncpy)
{
    int                     i, j, k, n, cnt, size;
    char                    dest[65], src[65], *p;

    memset(src, 'B', 32);

    cnt = size = 0;

    for(i = 1; i < 64; i++) {
        src[i] = '\0';
        memset(dest, 'A', 32);

        for(j = 1; j < 64; j++) {
            cnt ++;
            easy_strncpy(dest, src, j);
            // check
            n = ((j > i) ? i : (j - 1));

            for(k = 0; k < n; k++) {
                if (dest[k] != 'B') break;
            }

            if (k == n && dest[n] == '\0')
                size ++;
        }

        src[i] = 'B';
    }

    EXPECT_EQ(size, cnt);

    // null
    p = easy_strncpy(NULL, src, 0);
    EXPECT_TRUE(p == NULL);
    p = easy_strncpy(NULL, src, 1);
    EXPECT_TRUE(p == NULL);
    p = easy_strncpy(dest, src, 0);
    EXPECT_TRUE(p == NULL);
    p = easy_strncpy(dest, src, 1);
    EXPECT_TRUE(p != NULL);
}

//char *easy_string_tohex(const unsigned char *str, int n, char *result, int size)
TEST(easy_string, tohex)
{
    char                    str[] = "\x12\x34\x56\x78\xab\xfe\xcd\x09\x00";
    char                    *p, result[64];

    p = easy_string_tohex(str, sizeof(str), result, 4);
    EXPECT_TRUE(memcmp(p, "12", 3) == 0);

    p = easy_string_tohex(str, sizeof(str), result, 5);
    EXPECT_TRUE(memcmp(p, "1234", 5) == 0);

    p = easy_string_tohex(str, sizeof(str), result, 64);
    EXPECT_TRUE(memcmp(p, "12345678ABFECD090000", 2 * sizeof(str)) == 0);
}

TEST(easy_string, lnprintf)
{
    int                     ret;
    char                    buffer[256];

    ret = lnprintf(buffer, 3, "XXXX");
    EXPECT_TRUE(ret == 2);
}

TEST(easy_string, capitalize)
{
    char                    buffer[32];
    lnprintf(buffer, 32, "content-type");
    easy_string_capitalize(buffer, strlen(buffer));
    EXPECT_TRUE(memcmp(buffer, "Content-Type", strlen("Content-Type")) == 0);
}

#define test_string_lnprintf(fmt,args...)                                  \
    {                                                                      \
        char                    buffer[32];                                                   \
        char                    buffer1[32];                                                  \
        int                     size = snprintf(buffer, 32, fmt, ##args);                      \
        size = (size < 32 ? size : 31);                                    \
        int                     size1 = lnprintf(buffer1, 32, fmt, ##args);                    \
        EXPECT_EQ(size, size1);                                            \
        if (size != size1 || memcmp(buffer, buffer1, size)) {              \
            EXPECT_TRUE(0);                                                \
            printf("(%s),(%s)\n", buffer, buffer1);                        \
        }                                                                  \
    }

TEST(easy_string, lnprintf2)
{
    int                     i, v = 1234560897;
# if __WORDSIZE == 64
    int64_t                 l = 123456789012345l;
#else
    int64_t                 l = 123456789012345ll;
#endif
    double                  f = 9876123.2334;

    for(i = 0; i < 2; i++) {
        test_string_lnprintf("%d", v);
        test_string_lnprintf("%015d", v);
        test_string_lnprintf("%15d", v);
        test_string_lnprintf("%28d", v);
        test_string_lnprintf("%-28d", v);
        test_string_lnprintf("%-40d", v);
        test_string_lnprintf("%40d", v);
        test_string_lnprintf("%x", v);
        test_string_lnprintf("%X", v);
        test_string_lnprintf("%u", v);
        test_string_lnprintf("%f", f);
        test_string_lnprintf("%20f", f);
        test_string_lnprintf("%-20f", f);
        test_string_lnprintf("%40f", f);
        test_string_lnprintf("%-40f", f);
        test_string_lnprintf("%-20.3f", f);
        test_string_lnprintf("%20.3f", f);
        test_string_lnprintf("%-40.16f", f);
        test_string_lnprintf("%40.16f", f);
        test_string_lnprintf("%p", &v);
        test_string_lnprintf("%s", "abced");
        test_string_lnprintf("%-20s", "abced");
        test_string_lnprintf("%20s", "abced");
        test_string_lnprintf("%40s", "abced");
        test_string_lnprintf("%-40s", "abced");
        test_string_lnprintf("%s%%abc%%deef%%%d", "abced", v);

        test_string_lnprintf("%ld", (long)l);
        test_string_lnprintf("%lx", (long)l);
        test_string_lnprintf("%.*s", 3, "abcdef");
        test_string_lnprintf("%2.4f", f);
        test_string_lnprintf("%8.2f", f);
        test_string_lnprintf("%18.9f", f);
        test_string_lnprintf("%8.9f", f);
        v = -v;
        l = -l;
        f = -f;
    }
}

static int test_easy_printf(char *buffer, int size, const char *fmt, ...)
{
    int                     len;

    va_list                 args;
    va_start(args, fmt);
    len = easy_vsnprintf(buffer, size, fmt, args);
    va_end(args);

    return len;
}


#define test_string_vsprintf(fmt,args...)                                  \
    {                                                                      \
        char                    buffer[32];                                                   \
        char                    buffer1[32];                                                  \
        int                     size = snprintf(buffer, 32, fmt, ##args);                      \
        size = (size < 32 ? size : 31);                                    \
        int                     size1 = test_easy_printf(buffer1, 32, fmt, ##args);            \
        EXPECT_EQ(size, size1);                                            \
        if (size != size1 || memcmp(buffer, buffer1, size)) {              \
            EXPECT_TRUE(0);                                                \
            printf("(%s),(%s)\n", buffer, buffer1);                        \
        }                                                                  \
    }
TEST(easy_string, vsprintf2)
{
    int                     i, v = 1234560897;
# if __WORDSIZE == 64
    int64_t                 l = 123456789012345l;
#else
    int64_t                 l = 123456789012345ll;
#endif
    double                  f = 9876123.2334;
    char                    *p = NULL;
    char                    buffer[32];
    int                     len;

    for(i = 0; i < 2; i++) {
        test_string_vsprintf("%d", v);
        test_string_vsprintf("%015d", v);
        test_string_vsprintf("%15d", v);
        test_string_vsprintf("%28d", v);
        test_string_vsprintf("%-28d", v);
        test_string_vsprintf("%-40d", v);
        test_string_vsprintf("%40d", v);
        test_string_vsprintf("%x", v);
        test_string_vsprintf("%X", v);
        test_string_vsprintf("%u", v);
        test_string_vsprintf("%f", f);
        test_string_vsprintf("%20f", f);
        test_string_vsprintf("%-20f", f);
        test_string_vsprintf("%40f", f);
        test_string_vsprintf("%-40f", f);
        test_string_vsprintf("%-20.3f", f);
        test_string_vsprintf("%20.3f", f);
        test_string_vsprintf("%-40.16f", f);
        test_string_vsprintf("%40.16f", f);
        test_string_vsprintf("%p", &v);
        test_string_vsprintf("%s", "abced");
        test_string_vsprintf("%-20s", "abced");
        test_string_vsprintf("%20s", "abced");
        test_string_vsprintf("%40s", "abced");
        test_string_vsprintf("%-40s", "abced");
        test_string_vsprintf("%s%%abc%%deef%%%d", "abced", v);

        test_string_vsprintf("%ld", (long)l);
        test_string_vsprintf("%lx", (long)l);
        test_string_vsprintf("%.*s", 3, "abcdef");
        test_string_vsprintf("%2.4f", f);
        test_string_vsprintf("%8.2f", f);
        test_string_vsprintf("%18.9f", f);
        test_string_vsprintf("%8.9f", f);

        v = -v;
        l = -l;
        f = -f;
    }

    p = NULL;
    len = test_easy_printf(buffer, 32, "%s", p);
    EXPECT_EQ(len, 0);
}
