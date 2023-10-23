#include "util/easy_inet.h"
#include <easy_test.h>

/**
 * 测试 easy_inet
 */
typedef struct test_check_addr_t {
    char                    *host;
    int                     port;
    char                    *check;
} test_check_addr_t;
// char *easy_inet_addr_to_str(easy_addr_t *addr, char *buffer, int len);
TEST(easy_inet, addr_to_str)
{
    easy_addr_t             addr;
    char                    buffer[64], *str;
    int                     i, cnt, size;
    const char              *ip[] = {
        "192.168.1.1:2001",
        "192.168.1.1",
        "2001:fefe::1",
        "[2001::1]:2001",
        "1.1.1.1:1",
        "255.255.255.254:255"
    };

    cnt = sizeof(ip) / sizeof(char *);
    size = 0;

    for(i = 0; i < cnt; i++) {
        addr = easy_inet_str_to_addr(ip[i], 0);
        EXPECT_TRUE(addr.u.addr != 0);

        str = easy_inet_addr_to_str(&addr, buffer, 64);

        if (memcmp(ip[i], str, strlen(ip[i])) == 0)
            size ++;
        else
            fprintf(stderr, "Error: %d: %s <> %s\n", i, ip[i], str);
    }

    EXPECT_EQ(cnt, size);
}

// easy_addr_t easy_inet_str_to_addr(const char *host, int port);
TEST(easy_inet, str_to_addr)
{
    char                    toolarge_host[128];
    easy_addr_t             addr;
    char                    buffer[64], *str;
    int                     cnt, i, success;
    test_check_addr_t       *c, list[] = {
        {"2001:2:3:4:5:6:7:8", 0, "2001:2:3:4:5:6:7:8"},
        {"2:2:2003:2004:2005:2006:2007:2008", 12345, "[2:2:2003:2004:2005:2006:2007:2008]:12345"},
        {"192.168.1.1", 1001, "192.168.1.1:1001"},
        {"192.168.001.001", 1001, "192.168.1.1:1001"},
        {"192.168.1.1", 0, "192.168.1.1"},
        {"2001::1", 1, "[2001::1]:1"},
        {"2001::1", 0, "2001::1"},
        {"[2001::1]", 0, "2001::1"},
        {"[2001::1]:2000", 0, "[2001::1]:2000"},
        {"localhost", 1001, "127.0.0.1:1001"},
        {"localhost:8080", 9000, "127.0.0.1:9000"},
        {"localhost:8080", 0, "127.0.0.1:8080"},
        {"testabc.taobao.com", 0, "*"},
        {toolarge_host, 0, NULL},
        {"", 0, "0.0.0.0"},
        {NULL, 1001, "0.0.0.0:1001"},
        {NULL, 0, "0.0.0.0"},
    };

    memset(toolarge_host, 'a', 128);
    toolarge_host[124] = ':';
    toolarge_host[127] = '\0';
    success = 0;
    cnt = sizeof(list) / sizeof(test_check_addr_t);

    for(i = 0; i < cnt; i++) {
        c = &list[i];
        addr = easy_inet_str_to_addr(c->host, c->port);

        if (addr.u.addr == 0 && c->check == NULL) {
            success ++;
        } else if (*(c->check) == '*' && addr.u.addr != 0) {
            success ++;
        } else {
            str = easy_inet_addr_to_str(&addr, buffer, 64);
            buffer[63] = '\0';

            if (str && c->check && memcmp(str, c->check, strlen(c->check)) == 0) {
                success ++;
            } else {
                fprintf(stderr, "Error: %d. %s:%d => %s\n", i, c->host, c->port, str);
            }
        }
    }

    EXPECT_EQ(cnt, success);
}

// int easy_inet_parse_host(easy_addr_t *address, const char *host, int port);
TEST(easy_inet, parse_host)
{
    easy_addr_t             addr;
    char                    buffer[64], *str;
    int                     cnt, i, success;
    test_check_addr_t       *c, list[] = {
        {"[]", 1001, "[::]:1001"},
        {"192.168.1.1", 1001, "192.168.1.1:1001"},
        {"192.168.001.001", 1001, "192.168.1.1:1001"},
        {"192.168.1.1", 0, "192.168.1.1"},
        {"192.256.257.300", 1001, NULL},
        {"localhost", 1001, "127.0.0.1:1001"},
        {"localhost_xxxxxx", 1001, NULL},
        {NULL, 1001, "0.0.0.0:1001"},
    };

    success = 0;
    cnt = sizeof(list) / sizeof(test_check_addr_t);

    for(i = 0; i < cnt; i++) {
        c = &list[i];
        addr = easy_inet_str_to_addr(c->host, c->port);

        if (addr.family == 0 && c->check == NULL) {
            success ++;
        } else {
            str = easy_inet_addr_to_str(&addr, buffer, 64);
            buffer[63] = '\0';

            if (str && c->check && memcmp(str, c->check, strlen(c->check)) == 0) {
                success ++;
            } else {
                fprintf(stderr, "Error: %d. %s:%d => %s\n", i, c->host, c->port, str);
            }
        }
    }

    EXPECT_EQ(cnt, success);
}

// int easy_inet_is_ipaddr(const char *host);
TEST(easy_inet, is_ipaddr)
{
    int                     ret;

    ret = easy_inet_is_ipaddr("192.168.1.2");
    EXPECT_EQ(ret, 1);
    ret = easy_inet_is_ipaddr("xxx.xxx.xx.xxx");
    EXPECT_EQ(ret, 0);
    ret = easy_inet_is_ipaddr("192.1.0.x");
    EXPECT_EQ(ret, 0);
    ret = easy_inet_is_ipaddr("x.1.2.1");
    EXPECT_EQ(ret, 0);
    ret = easy_inet_is_ipaddr("1 2 1 0");
    EXPECT_EQ(ret, 0);
}

TEST(easy_inet, hostaddr)
{
    int                     size;
    uint64_t                address[16];

    size = easy_inet_hostaddr(address, 16, 1);
    EXPECT_TRUE(size > 0);

#ifdef TODO
    char                    buffer[64];
    easy_addr_t             addr;
    memset(&addr, 0, sizeof(addr));
    int                     i;

    for(i = 0; i < size; i++) {
        addr.addr = address[i];
        fprintf(stderr, "%s\n", easy_inet_addr_to_str(&addr, buffer, 64));
    }

#endif
}

TEST(easy_inet, add_port)
{
    easy_addr_t             addr;
    char                    buffer[32];

    addr = easy_inet_str_to_addr("192.168.1.2", 255);
    addr = easy_inet_add_port(&addr, 3);
    easy_inet_addr_to_str(&addr, buffer, 32);
    EXPECT_EQ(strcmp(buffer, "192.168.1.2:258"), 0);
    addr = easy_inet_add_port(&addr, -4);
    easy_inet_addr_to_str(&addr, buffer, 32);
    EXPECT_EQ(strcmp(buffer, "192.168.1.2:254"), 0);
}

