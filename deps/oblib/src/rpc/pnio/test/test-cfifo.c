#include "interface/pkt-nio.h"
#include <pthread.h>

#define N 16
cfifo_alloc_t alloc;
void* thread_func(void* arg)
{
  for(int i = 0; i < 10000000; i++) {
    void* ret = cfifo_alloc(&alloc, 40);
    cfifo_free(ret);
  }
  return NULL;
}

int main()
{
  pthread_t th[N];
  cfifo_alloc_init(&alloc, 0);
  for(int i = 0; i < N; i++) {
    pthread_create(th + i, NULL, thread_func, NULL);
  }
  for(int i = 0; i < N; i++) {
    pthread_join(th[i], NULL);
  }
  return 0;
}

#include "interface/pkt-nio.c"
