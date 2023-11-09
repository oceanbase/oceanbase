#ifndef EASY_DEFINE_H_
#define EASY_DEFINE_H_

/**
 * 定义一些编译参数
 */

#ifdef __cplusplus
# define EASY_CPP_START extern "C" {
# define EASY_CPP_END }
#else
# define EASY_CPP_START
# define EASY_CPP_END
#endif

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stddef.h>
#include <inttypes.h>
#include <unistd.h>
#include <execinfo.h>
#include <sys/uio.h>

///////////////////////////////////////////////////////////////////////////////////////////////////
// define
#define easy_free(ptr)              if(ptr) free(ptr)
#define easy_malloc(size)           malloc(size)
#define easy_realloc(ptr, size)     realloc(ptr, size)
#ifndef likely
#define likely(x)                   __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x)                 __builtin_expect(!!(x), 0)
#endif
#define easy_align_ptr(p, a)        (uint8_t*)(((uintptr_t)(p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))
#define easy_align(d, a)            (((d) + (a - 1)) & ~(a - 1))
#define easy_max(a,b)               (a > b ? a : b)
#define easy_min(a,b)               (a < b ? a : b)
#define easy_div(a,b)               ((b) ? ((a)/(b)) : 0)
#define easy_memcpy(dst, src, n)    (((char *) memcpy(dst, src, (n))) + (n))
#define easy_const_strcpy(b, s)     easy_memcpy(b, s, sizeof(s)-1)
#define easy_safe_close(fd)         {if((fd)>=0){close((fd));(fd)=-1;}}
#define easy_ignore(exp)            {int ignore __attribute__ ((unused)) = (exp);}

#define EASY_OK                     0
#define EASY_ERROR                  (-1)
#define EASY_ABORT                  (-2)
#define EASY_ASYNC                  (-3)
#define EASY_BREAK                  (-4)
#define EASY_AGAIN                  (-EAGAIN)
#define EASY_STOP                   (-45)
#define EASY_DISCONNECT             (-46)
#define EASY_TIMEOUT                (-47)
#define EASY_ALLOC_FAIL             (-48)
#define EASY_CONNECT_FAIL           (-49)
#define EASY_KEEPALIVE_ERROR        (-50)
#define EASY_DISPATCH_ERROR         (-51)
#define EASY_CLUSTER_ID_MISMATCH    (-52)
#define EASY_BUSY                   (-53)
#define EASY_TIMEOUT_NOT_SENT_OUT       (-54)
#define EASY_DISCONNECT_NOT_SENT_OUT    (-55)

#define EASY_CONTINUE               (1)

// interval
#define EASY_REACH_TIME_INTERVAL(i) \
  ({ \
    char bret = 0; \
    static volatile int64_t last_time = 0; \
    int64_t cur_time = current_time(); \
    int64_t old_time = last_time; \
    if ((i + last_time) < cur_time \
        && easy_atomic_cmp_set(&last_time, old_time, cur_time)) \
    { \
      bret = 1; \
    } \
    bret; \
  })

// DEBUG
//#define EASY_DEBUG_DOING            1
//#define EASY_DEBUG_MAGIC            1
///////////////////////////////////////////////////////////////////////////////////////////////////
// typedef
#include <stdint.h>
typedef struct easy_addr_t {
    uint16_t                  family;
    uint16_t                  port;
    union {
        uint32_t                addr;
        uint8_t                 addr6[16];
        char                    unix_path[16];
    } u;
    uint32_t                cidx;
} easy_addr_t;

typedef unsigned long long cycles_t;
#endif
