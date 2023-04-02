#include <stdarg.h>
typedef struct format_t {
  int64_t limit;
  int64_t pos;
  char buf[1024];
} format_t;

extern void format_init(format_t* f, int64_t limit);
extern void format_reset(format_t* f);
extern char* format_gets(format_t* f);
extern char* format_append(format_t* f, const char* format, ...) __attribute__((format(printf, 2, 3)));
extern char* format_sf(format_t* f, const char* format, ...) __attribute__((format(printf, 2, 3)));
extern char* strf(char* buf, int64_t size, const char *f, ...) __attribute__((format(printf, 3, 4))) ;
