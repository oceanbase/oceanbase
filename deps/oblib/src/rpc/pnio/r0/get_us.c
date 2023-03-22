#include <sys/time.h>
int64_t rk_get_us() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000 + tv.tv_usec;
}

int64_t rk_get_corse_us()
{
  struct timespec tp;
  clock_gettime(CLOCK_REALTIME_COARSE, &tp);
  return tp.tv_sec * 1000000 + tp.tv_nsec/1000;
}
