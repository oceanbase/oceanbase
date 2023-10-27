#include "util/easy_time.h"
#include <sys/time.h>
#if defined(__x86_64__)
#include <cpuid.h>
#endif

/**
 * localtime删除tzset,一起调用一次tzset
 */
int easy_localtime(const time_t *t, struct tm *tp)
{
    static const unsigned short int mon_yday[2][13] = {
        /* Normal years.  */
        { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 },
        /* Leap years.  */
        { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 }
    };

#define SECS_PER_HOUR   (60 * 60)
#define SECS_PER_DAY    (SECS_PER_HOUR * 24)
#define ISLEAP(year)    ((year) % 4 == 0 && ((year) % 100 != 0 || (year) % 400 == 0))
#define DIV(a, b)       ((a) / (b) - ((a) % (b) < 0))
#define LEAPS_THRU_END_OF(y) (DIV (y, 4) - DIV (y, 100) + DIV (y, 400))

    long int                days, rem, y;
    const unsigned short int *ip;

    days = *t / SECS_PER_DAY;
    rem = *t % SECS_PER_DAY;
    rem -= timezone;

    while (rem < 0) {
        rem += SECS_PER_DAY;
        --days;
    }

    while (rem >= SECS_PER_DAY) {
        rem -= SECS_PER_DAY;
        ++days;
    }

    tp->tm_hour = rem / SECS_PER_HOUR;
    rem %= SECS_PER_HOUR;
    tp->tm_min = rem / 60;
    tp->tm_sec = rem % 60;
    /* January 1, 1970 was a Thursday.  */
    tp->tm_wday = (4 + days) % 7;

    if (tp->tm_wday < 0)
        tp->tm_wday += 7;

    y = 1970;

    while (days < 0 || days >= (ISLEAP(y) ? 366 : 365)) {
        /* Guess a corrected year, assuming 365 days per year.  */
        long int                yg = y + days / 365 - (days % 365 < 0);

        /* Adjust DAYS and Y to match the guessed year.  */
        days -= ((yg - y) * 365
                 + LEAPS_THRU_END_OF(yg - 1)
                 - LEAPS_THRU_END_OF(y - 1));
        y = yg;
    }

    tp->tm_year = y - 1900;

    if (tp->tm_year != y - 1900) {
        return 0;
    }

    tp->tm_yday = days;
    ip = mon_yday[ISLEAP(y)];

    for (y = 11; days < (long int) ip[y]; --y)
        continue;

    days -= ip[y];
    tp->tm_mon = y;
    tp->tm_mday = days + 1;
    return 1;
}

void __attribute__((constructor)) easy_time_start_()
{
    tzset();
}

int64_t easy_time_now()
{
    struct timeval          tv;
    gettimeofday(&tv, 0);
    return __INT64_C(1000000) * tv.tv_sec + tv.tv_usec;
}

// 判断是否支持invariant tsc, 入口在CPUID.80000007H:EDX[8].
int is_support_invariant_tsc()
{
    int ret = 0;

#if defined(__x86_64__)
    unsigned int cpu_info[4];
    cpu_info[3] = 0;
    __get_cpuid(0x80000007, cpu_info, cpu_info + 1, cpu_info + 2, cpu_info + 3);
    if (cpu_info[3] & 0x100) {
        ret = 1;
    }
#else
    ret = 1;
#endif

    return ret;
}

typedef struct ObTscTimestampStruct {
    int64_t start_us;
    uint64_t tsc_count;
    uint64_t scale;
    int use_tsc;
    int is_inited;
} ObTscTimestamp;

__thread ObTscTimestamp tsc_obj;

#if defined(__x86_64__)
static __inline__ uint64_t rdtsc()
{
    uint64_t rax, rdx;
    __asm__ __volatile__("rdtsc" : "=a"(rax), "=d"(rdx) :: "%rcx");
    return (rdx << 32) + rax;
}

static __inline__ uint64_t rdtscp()
{
    uint64_t rax, rdx;
    __asm__ __volatile__("rdtscp" : "=a"(rax), "=d"(rdx) :: "%rcx");
    return (rdx << 32) + rax;
}
#else
static __inline__ uint64_t rdtscp()
{
    int64_t virtual_timer_value;
    asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
    return virtual_timer_value;
}
static __inline__ uint64_t rdtsc()
{
    return rdtscp();
}

#endif

#if defined(__x86_64__)
// 读取cpu频率
uint64_t get_cpufreq_khz()
{
    char line[256];
    FILE *stream = NULL;
    double freq_mhz = 0.0;
    uint64_t freq_khz = 0;

    stream = fopen("/proc/cpuinfo", "r");
    if (NULL == stream) {
    } else {
        while (fgets(line, sizeof(line), stream)) {
            if (sscanf(line, "cpu MHz\t: %lf", &freq_mhz) == 1) {
                freq_khz = (uint64_t)(freq_mhz * 1000UL);
                break;
            }
        }
        fclose(stream);
    }

    return freq_khz;
}
#else
uint64_t get_cpufreq_khz(void)
{
    uint64_t timer_frequency;
    asm volatile("mrs %0, cntfrq_el0":"=r"(timer_frequency));
    return timer_frequency / 1000;
}
#endif

// 初始化tsc时钟
void build_tsc_timestamp()
{
    if (!tsc_obj.is_inited) {
        if (is_support_invariant_tsc()) {
            int64_t cpu_freq_khz = get_cpufreq_khz();
            struct timeval tv;
            gettimeofday(&tv, NULL);
            tsc_obj.start_us = tv.tv_sec * 1000000 + tv.tv_usec;
            tsc_obj.tsc_count = rdtscp();
            if (tsc_obj.tsc_count > 0 && cpu_freq_khz > 0) {
                tsc_obj.scale = (1000 << 20) / cpu_freq_khz;
                tsc_obj.use_tsc = 1;
            }
        }
        // printf("build_tsc_timestamp: use_tsc=%d scale=%ld\n", tsc_obj.use_tsc, tsc_obj.scale);
        tsc_obj.is_inited = 1;
    }
}

#define LIKELY(x) __builtin_expect(!!(x),!!1)

int64_t fast_current_time()
{
    if (LIKELY(tsc_obj.use_tsc)) {
        uint64_t current_tsc = rdtsc();
        return ((current_tsc - tsc_obj.tsc_count) * tsc_obj.scale >> 20) + tsc_obj.start_us;
    } else {
        if (!tsc_obj.is_inited) {
            build_tsc_timestamp();
        }
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return tv.tv_sec * 1000000 + tv.tv_usec;
    }
}
