#include "util/easy_array.h"

easy_array_t *easy_array_create(int object_size)
{
    easy_pool_t             *pool;
    easy_array_t            *array;

    if ((pool = easy_pool_create(0)) == NULL)
        return NULL;

    if ((array = (easy_array_t *)easy_pool_alloc(pool, sizeof(easy_array_t))) == NULL)
        return NULL;

    easy_list_init(&array->list);
    array->count = 0;
    array->pool = pool;
    array->object_size = easy_max(object_size, (int)sizeof(easy_list_t));

    return array;
}

void easy_array_destroy(easy_array_t *array)
{
    easy_pool_destroy(array->pool);
}

void *easy_array_alloc(easy_array_t *array)
{
    if (easy_list_empty(&array->list) == 0) {
        array->count --;
        char                    *ptr = (char *)array->list.prev;
        easy_list_del((easy_list_t *)ptr);
        return ptr;
    }

    return easy_pool_alloc(array->pool, array->object_size);
}

void easy_array_free(easy_array_t *array, void *ptr)
{
    array->count ++;
    easy_list_add_tail((easy_list_t *)ptr, &array->list);
}
