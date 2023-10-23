#include "util/easy_string.h"

static char *easy_sprintf_num(char *buf, char *last, uint64_t ui64, char zero, int hexadecimal, int width, int sign);
static char *easy_fill_space(int width, char *buf, char *fstart, char *last);

char *easy_strncpy(char *dst, const char *src, size_t n)
{
    if (!n || !dst)
        return NULL;

    const uint64_t          himagic = __UINT64_C(0x8080808080808080);
    const uint64_t          lomagic = __UINT64_C(0x0101010101010101);
    const uint64_t          *nsrc = (const uint64_t *)src;
    const uint64_t          *nend = nsrc + (--n / 8);
    uint64_t                *ndst = (uint64_t *)dst;

    while (nsrc != nend) {
        uint64_t                k = *nsrc;

        if (((k - lomagic) & ~k & himagic) != 0) {
            const char              *cp = (const char *) nsrc;

            if (cp[0] == 0) {
                n = 0;
                break;
            }

            if (cp[1] == 0) {
                n = 1;
                break;
            }

            if (cp[2] == 0) {
                n = 2;
                break;
            }

            if (cp[3] == 0) {
                n = 3;
                break;
            }

            if (cp[4] == 0) {
                n = 4;
                break;
            }

            if (cp[5] == 0) {
                n = 5;
                break;
            }

            if (cp[6] == 0) {
                n = 6;
                break;
            }

            n = 7;
            break;
        }

        *ndst++ = k;
        nsrc ++;
    }

    const char              *nsrc2 = (const char *) nsrc;

    char                    *ndst2 = (char *) ndst;

    switch (n & 7) {
    case 7:
        *ndst2++ = *nsrc2++;

    case 6:
        *ndst2++ = *nsrc2++;

    case 5:
        *ndst2++ = *nsrc2++;

    case 4:
        *ndst2++ = *nsrc2++;

    case 3:
        *ndst2++ = *nsrc2++;

    case 2:
        *ndst2++ = *nsrc2++;

    case 1:
        *ndst2++ = *nsrc2++;
    };

    *ndst2 = '\0';

    return dst;
}

/**
 * 把char转成hex
 */
char *easy_string_tohex(const char *str, int n, char *result, int size)
{
    int                     i, j = 0;
    static char             hexconvtab[] = "0123456789ABCDEF";
    const unsigned char     *p = (const unsigned char *)str;

    n = easy_min((size - 1) / 2, n);

    for (i = 0; i < n; i++) {
        result[j++] = hexconvtab[p[i] >> 4];
        result[j++] = hexconvtab[p[i] & 0xf];
    }

    result[j] = '\0';

    return result;
}

/**
 * 转成大写
 */
char *easy_string_toupper(char *str)
{
    char                    *p = str;

    while (*p) {
        if ((*p) >= 'a' && (*p) <= 'z')
            (*p) -= 32;

        p ++;
    }

    return str;
}

/**
 * 转成小写
 */
char *easy_string_tolower(char *str)
{
    char                    *p = str;

    while (*p) {
        if ((*p) >= 'A' && (*p) <= 'Z')
            (*p) += 32;

        p ++;
    }

    return str;
}

/**
 * 把首字母转成大写,其余转成小写
 */
char *easy_string_capitalize(char *str, int len)
{
    char                    *p = str;
    int                     first = 1;
    char                    *end = str + len;

    while (p < end) {
        if ((*p) >= 'A' && (*p) <= 'Z') {
            if (!first)(*p) += 32;

            first = 0;
        } else if ((*p) >= 'a' && (*p) <= 'z') {
            if (first)(*p) -= 32;

            first = 0;
        } else if ((*p) == '-' || (*p) == '_') {
            first = 1;
        }

        p ++;
    }

    return str;
}

/**
 * 转成byte可读的KMGTPE
 */
char *easy_string_format_size(double byte, char *buffer, int size)
{
    static const char       units[] = " KMGTPEZY";

    int                     idx = 0;

    while (byte >= 1024) {
        byte /= 1024;
        idx ++;
    }

    buffer[0] = '\0';

    if (idx == 0)
        lnprintf(buffer, size, "%.2f", byte);
    else if (idx < 9)
        lnprintf(buffer, size, "%.2f %cB", byte, units[idx]);

    return buffer;
}

/**
 * 把copy string
 */
char *easy_strcpy(char *dest, const char *src)
{
    int                     len = strlen(src);
    return easy_memcpy(dest, src, len);
}

/**
 * 把number转成string
 */
#define EASY_NUM_LEN 32
char *easy_num_to_str(char *dest, int len, uint64_t number)
{
    char                    t[EASY_NUM_LEN], *p, *end;

    p = end = t + EASY_NUM_LEN;

    if (number <= 0xffffffffU) {
        uint32_t                v = number;

        do {
            *--p = (char)(v % 10 + '0');
        } while (v /= 10);
    } else {
        do {
            *--p = (char)(number % 10 + '0');
        } while (number /= 10);
    }

    while (p < end) *dest ++ = *p ++;

    *dest = '\0';
    return dest;
}

/**
 * lnprintf
 */
int lnprintf(char *str, size_t size, const char *fmt, ...)
{
    int                     ret;
    va_list                 args;

    va_start(args, fmt);
    ret = easy_vsnprintf(str, size, fmt, args);
    va_end(args);

    return ret;
}

/**
 * easy_vsnprintf
 */
int easy_vsnprintf(char *buf, size_t size, const char *fmt, va_list args)
{
    char                    *p, zero;
    double                  f, scale;
    int64_t                 i64;
    uint64_t                ui64;
    int                     width, sign, hex, frac_width, slen, width_sign;
    char                    *last, *start, *fstart;
    char                    length_modifier;

    start = buf;
    last = buf + size - 1;

    while (*fmt && buf < last) {

        if (*fmt == '%') {

            zero = (char)((*++fmt == '0') ? '0' : ' ');
            width_sign = ((*fmt == '-') ? (fmt++, -1) : 1);
            width = 0;
            sign = 1;
            hex = 0;
            frac_width = 6;
            slen = -1;
            length_modifier = '0';
            fstart = buf;

            while (*fmt >= '0' && *fmt <= '9') {
                width = width * 10 + *fmt++ - '0';
            }

            width                   *= width_sign;

            // width
            switch (*fmt) {
            case '.':
                fmt++;

                if (*fmt != '*') {
                    frac_width = 0;

                    while (*fmt >= '0' && *fmt <= '9') {
                        frac_width = frac_width * 10 + *fmt++ - '0';
                    }

                    break;
                }

            case '*':
                slen = va_arg(args, size_t);
                fmt++;
                break;

            case 'l':
                fmt++;
#ifdef _LP64
                length_modifier ++;
#endif

                if (*fmt == 'l') {
                    length_modifier ++;
                    fmt ++;
                }

                break;

            default:
                break;
            }

            // type
            switch (*fmt) {
            case 's':
                p = va_arg(args, char *);

                if (slen < 0) {
                    slen = last - buf;
                } else {
                    slen = easy_min(((size_t)(last - buf)), slen);
                }

                while (slen-- > 0 && p && *p) {
                    *buf++ = *p++;
                }

                if (width > 0 && (buf - fstart) < width) {
                    p = easy_min(fstart + width, last);
                    buf -= (fstart + width - p);

                    while (buf > fstart) {
                        *--p = *--buf;
                    }

                    while (p > fstart) *--p = ' ';

                    buf = easy_min(fstart + width, last);
                }

                buf = easy_fill_space(width, buf, fstart, last);
                fmt++;

                continue;

            case 'x':
                hex = 1;

            case 'X':
                if (!hex) hex = 2;

            case 'u':
                sign = 0;

                if (length_modifier == '0') {
                    ui64 = (uint64_t) va_arg(args, uint32_t);
                } else {
                    ui64 = (uint64_t) va_arg(args, uint64_t);
                }

                break;

            case 'd':
                if (length_modifier == '0') {
                    i64 = (int64_t) va_arg(args, int);
                } else {
                    i64 = (int64_t) va_arg(args, int64_t);
                }

                break;

            case 'f':
                f = va_arg(args, double);

                if (f < 0) {
                    sign = -1;
                    f = -f;
                } else {
                    sign = 0;
                }

                ui64 = (int64_t) f;

                slen = width - frac_width - (frac_width ? 1 : 0);
                buf = easy_sprintf_num(buf, last, ui64, zero, 0, slen, sign);

                if (frac_width) {

                    if (buf < last) {
                        *buf++ = '.';
                    }

                    if (frac_width > 16) frac_width = 16;

                    scale = 1.0;

                    for (sign = frac_width; sign; sign--) {
                        scale                   *= 10.0;
                    }

                    ui64 = (uint64_t)((f - (int64_t) ui64) * scale + 0.5);

                    buf = easy_sprintf_num(buf, last, ui64, '0', 0, frac_width, 0);
                }

                buf = easy_fill_space(width, buf, fstart, last);

                fmt++;

                continue;

            case 'p':
                ui64 = (uintptr_t) va_arg(args, void *);
                hex = 1;
                sign = 0;
                zero = '0';
                width = 0;

                if (buf + 2 < last) {
                    *buf++ = '0';
                    *buf++ = 'x';
                }

                break;

            case 'c':
                i64 = va_arg(args, int);
                *buf++ = (char)(i64 & 0xff);
                fmt++;

                continue;

            case '%':
                *buf++ = '%';
                fmt++;

                continue;

            default:
                *buf++ = *fmt++;

                continue;
            }

            if (sign) {
                if (i64 < 0) {
                    sign = -1;
                    ui64 = (uint64_t) - i64;
                } else {
                    sign = 0;
                    ui64 = (uint64_t) i64;
                }
            }

            buf = easy_sprintf_num(buf, last, ui64, zero, hex, width, sign);
            buf = easy_fill_space(width, buf, fstart, last);

            fmt++;

        } else {
            *buf++ = *fmt++;
        }
    }

    *buf = '\0';

    return (buf - start);
}

static char *easy_fill_space(int width, char *buf, char *fstart, char *last)
{
    if (width >= 0 || (buf - fstart) >= -width)
        return buf;

    last = easy_min(fstart - width, last);

    while (buf < last) {
        *buf++ = ' ';
    }

    return buf;
}

/**
 * 把number转成字符串
 */
static char *easy_sprintf_num(char *buf, char *last, uint64_t ui64, char zero, int hexadecimal, int width, int sign)
{
    char          *p, temp[EASY_NUM_LEN + 1];
    int                     len;
    uint32_t                ui32;
    static char             hex[] = "0123456789abcdef";
    static char             HEX[] = "0123456789ABCDEF";

    p = temp + EASY_NUM_LEN;

    if (hexadecimal == 0) {
        if (ui64 <= (uint32_t) 0xffffffff) {
            ui32 = (uint32_t) ui64;

            do {
                *--p = (char)(ui32 % 10 + '0');
            } while (ui32 /= 10);
        } else {
            do {
                *--p = (char)(ui64 % 10 + '0');
            } while (ui64 /= 10);
        }
    } else if (hexadecimal == 1) {

        do {
            *--p = hex[(uint32_t)(ui64 & 0xf)];
        } while (ui64 >>= 4);

    } else { /* hexadecimal == 2 */

        do {
            *--p = HEX[(uint32_t)(ui64 & 0xf)];
        } while (ui64 >>= 4);
    }

    if (sign) {
        if (zero == ' ') {
            *--p = '-';
        } else if (buf < last) {
            *buf++ = '-';
            width --;
        }
    }

    /* zero or space padding */
    len = (temp + EASY_NUM_LEN) - p;

    while (len++ < width && buf < last) {
        *buf++ = zero;
    }

    len = (temp + EASY_NUM_LEN) - p;

    if (buf + len > last) {
        len = last - buf;
    }

    return easy_memcpy(buf, p, len);
}

