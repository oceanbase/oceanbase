#include <sys/time.h>
#include <math.h>
#include "util/easy_util.h"
#include "io/easy_log.h"

#define STACK_BT_BUFFER_LENGTH 2048
#define MEASUREMENTS 200
#define USECSTEP 10
#define USECSTART 100
static const double EASY_DOUBLE_EPSINON = 1e-14;
extern char *parray_c(char *buf, int64_t len, int64_t *array, int size);
const char *easy_lbt()
{
    static __thread void *addrs[100];
    static __thread char buf[1024];
    int size = ob_backtrace_c(addrs, 100);
    return parray_c(buf, sizeof(buf), (int64_t *)addrs, size);
}

const char *easy_lbt_str()
{
    static __thread void *addrs[100];
    static __thread char buf[STACK_BT_BUFFER_LENGTH];
    int size, len, pos = 0;
    char **symbols = NULL;
    char *sym;
    int idx;

    sprintf(buf, "\n");
    pos++;

    size    = ob_backtrace_c(addrs, 100);
    symbols = backtrace_symbols(addrs, 100);
    if (NULL == symbols) {
        return buf;
    }

    for (idx = 0; idx < size; idx++) {
        sym = symbols[idx];
        if (NULL != sym) {
            len = strlen(sym);
            if ((pos + len + 1) > STACK_BT_BUFFER_LENGTH) {
                break;
            } else {
                sprintf(buf + pos, "%s\n", sym);
                pos += len + 1;
            }
        }
    }

    free(symbols);
    return buf;
}

const char* easy_get_func_name(void *addr)
{
    char *func_name = "";
    char **res;

    res = backtrace_symbols(&addr, 1);
    if (res != NULL) {
        func_name = res[0];
        free(res);
    }

    return func_name;
}

/*
 * Use linear regression to calculate cycles per microsecond.
 */
static double sample_get_cpu_mhz(void)
{
    struct timeval tv1, tv2;
    cycles_t start;
    double sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
    double tx, ty;
    int i;

    /* Regression: y = a + b x */
    long x[MEASUREMENTS];
    cycles_t y[MEASUREMENTS];
    // double a; /* system call overhead in cycles */
    double b; /* cycles per microsecond */
    double r_2;

    for (i = 0; i < MEASUREMENTS; ++i) {
        start = easy_get_cycles();

        if (gettimeofday(&tv1, NULL)) {
            easy_error_log("gettimeofday failed.\n");
            return 0;
        }

        do {
            if (gettimeofday(&tv2, NULL)) {
                easy_error_log("gettimeofday failed.\n");
                return 0;
            }
        } while ((tv2.tv_sec - tv1.tv_sec) * 1000000 +
                 (tv2.tv_usec - tv1.tv_usec) < USECSTART + i * USECSTEP);

        x[i] = (tv2.tv_sec - tv1.tv_sec) * 1000000 +
               tv2.tv_usec - tv1.tv_usec;
        y[i] = easy_get_cycles() - start;
        // if (DEBUG_DATA)
        // DEBUG("x=%ld y=%Ld.\n", x[i], (long long)y[i]);
    }

    for (i = 0; i < MEASUREMENTS; ++i) {
        tx = x[i];
        ty = y[i];
        sx += tx;
        sy += ty;
        sxx += tx * tx;
        syy += ty * ty;
        sxy += tx * ty;
    }

    b = (MEASUREMENTS * sxy - sx * sy) / (MEASUREMENTS * sxx - sx * sx);
    // a = (sy - b * sx) / MEASUREMENTS;
    // DEBUG("a = %g\n", a);
    // DEBUG("b = %g\n", b);
    // DEBUG("a / b = %g\n", a / b);
    r_2 = (MEASUREMENTS * sxy - sx * sy) *
          (MEASUREMENTS * sxy - sx * sy) /
          (MEASUREMENTS * sxx - sx * sx) /
          (MEASUREMENTS * syy - sy * sy);

    // DEBUG("r^2 = %g\n", r_2);
    if (r_2 < 0.9) {
        easy_error_log("Correlation coefficient r^2: %g < 0.9.\n", r_2);
        return 0;
    }

    return b;
}

static double proc_get_cpu_mhz(int no_cpu_freq_fail)
{
    FILE *f;
    char buf[256];
    double mhz = 0.0;

    f = fopen("/proc/cpuinfo", "r");
    if (!f) {
        return 0.0;
    }

    while (fgets(buf, sizeof(buf), f)) {
        double m;
        int rc;
        rc = sscanf(buf, "cpu MHz : %lf", &m);
        if (rc != 1) {	/* PPC has a different format */
            rc = sscanf(buf, "clock : %lf", &m);
            if (rc != 1) {
                continue;
            }
        }
        if (fabs(mhz) < EASY_DOUBLE_EPSINON) {
            mhz = m;
            continue;
        }
        if (fabs(mhz - m) > EASY_DOUBLE_EPSINON) {
            // DEBUG("Conflicting CPU frequency values detected: %lf != %lf.\n", mhz, m);
            if (no_cpu_freq_fail) {
                // DEBUG("Test integrity may be harmed!\n");
            } else {
                fclose(f);
                return 0.0;
            }
            continue;
        }
    }
    fclose(f);
    return mhz;
}

double easy_get_cpu_mhz(int no_cpu_freq_fail)
{
    double sample, proc, delta;
    sample = sample_get_cpu_mhz();
    proc = proc_get_cpu_mhz(no_cpu_freq_fail);

    easy_debug_log("Got CPU mhz, sample(%lf), proc(%lf).\n", sample, proc);
	if ((fabs(proc) < EASY_DOUBLE_EPSINON) ||
		(fabs(sample) < EASY_DOUBLE_EPSINON)) {
		return 0;
	}

    delta = proc > sample ? proc - sample : sample - proc;
    if (delta / proc > 0.01) {
        easy_warn_log("Measured timestamp frequency %g differs from nominal %g MHz.\n", sample, proc);
        return sample;
    }
    return proc;
}
