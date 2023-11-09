#ifndef EASY_ARRAY_H_
#define EASY_ARRAY_H_

/**
 * 固定长度数组分配
 */
#include "util/easy_pool.h"
#include "easy_list.h"

EASY_CPP_START

typedef struct easy_array_t {
    easy_pool_t             *pool;
    easy_list_t             list;
    int                     object_size;
    int                     count;
} easy_array_t;

easy_array_t *easy_array_create(int object_size);
void easy_array_destroy(easy_array_t *array);
void *easy_array_alloc(easy_array_t *array);
void easy_array_free(easy_array_t *array, void *ptr);

EASY_CPP_END

#endif
