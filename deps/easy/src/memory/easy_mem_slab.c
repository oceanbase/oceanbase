#include "memory/easy_mem_slab.h"

#define EASY_MEM_POS_END      (((uint16_t)(~0U))-0)

easy_mem_mgr_t          easy_mem_mgr_var = {0};
static void *easy_mem_slab_get_obj(easy_mem_cache_t *cache, easy_mem_slab_t *slab);
static void *easy_mem_cache_grow(easy_mem_cache_t *cache);
static void easy_mem_slab_put_obj(easy_mem_cache_t *cache, easy_mem_slab_t *slab, void *obj);
static inline easy_mem_cache_t *easy_mem_get_cache(unsigned char *obj);
static inline easy_mem_slab_t *easy_mem_virt_to_slab(int order, const void *obj);
static inline easy_mem_cache_t *easy_mem_cache_size(uint32_t size);

// 内存初始化
int easy_mem_slab_init(int start_alloc_size, int64_t max_size)
{
    int                     size, cache_num;

    if (easy_mem_mgr_var.started)
        return EASY_ERROR;

    // 初始化
    memset(&easy_mem_mgr_var, 0, sizeof(easy_mem_mgr_var));
    easy_mem_mgr_var.max_size = max_size;
    easy_mem_mgr_var.zone = easy_mem_zone_create(max_size);

    if (easy_mem_mgr_var.zone == NULL)
        return EASY_ERROR;

    // init
    cache_num = (easy_mem_mgr_var.zone->curr_end - easy_mem_mgr_var.zone->curr);
    cache_num /= sizeof(easy_mem_cache_t);
    cache_num --;

    size = EASY_MEM_SLAB_MIN;
    easy_list_init(&easy_mem_mgr_var.list);
    easy_mem_mgr_var.cache_max_num = easy_max(cache_num, 1024);
    easy_mem_mgr_var.caches = (easy_mem_cache_t *)easy_mem_mgr_var.zone->curr;

    // 分配mem_cache
    while(size <= start_alloc_size) {
        if (easy_mem_cache_create(size) == NULL)
            break;

        size <<= 1;
    }

    easy_mem_mgr_var.cache_fix_num = easy_mem_mgr_var.cache_num;
    easy_mem_mgr_var.started = 1;
    return EASY_OK;
}

// destroy
void easy_mem_slab_destroy()
{
    if (easy_mem_mgr_var.started) {
        easy_mem_zone_destroy(easy_mem_mgr_var.zone);
        memset(&easy_mem_mgr_var, 0, sizeof(easy_mem_mgr_var));
    }
}

// 内存分配
void *easy_mem_slab_realloc(void *ptr, size_t size)
{
    easy_mem_cache_t        *cache, *ncache;
    unsigned char           *obj;

    // free
    obj = (unsigned char *)ptr;

    if (size == 0 && obj != NULL) {
        if ((cache = easy_mem_get_cache(obj)) != NULL)
            easy_mem_cache_free(cache, obj);
        else
            easy_free(ptr);

        return NULL;
        // realloc
    } else if (obj != NULL) {
        if ((cache = easy_mem_get_cache(obj)) != NULL && size <= cache->buffer_size)
            return ptr;

        // 分配新的
        ncache = easy_mem_cache_size(size);

        if (ncache) {
            ptr = easy_mem_cache_alloc(ncache);
        } else if (cache) {
            ptr = easy_malloc(size);
        } else {
            ptr = easy_realloc(ptr, size);
        }

        // 把旧的memcpy到新的上
        if (cache) {
            memcpy(ptr, obj, cache->buffer_size);
            easy_mem_cache_free(cache, obj);
        }

        return ptr;
        // malloc
    } else {
        cache = easy_mem_cache_size(size);
        return (cache ? easy_mem_cache_alloc(cache) : malloc(size));
    }
}

// 分配
void *easy_mem_cache_alloc(easy_mem_cache_t *cache)
{
    easy_list_t             *entry;
    easy_mem_slab_t         *slab;
    void                    *obj;

    obj = NULL;
    easy_spin_lock(&cache->lock);
    entry = cache->slabs_partial.next;

    if (entry == &cache->slabs_partial) {
        entry = cache->slabs_free.next;

        if (entry == &cache->slabs_free) {
            goto grow_done;
        }
    }

    // slab
    slab = easy_list_entry(entry, easy_mem_slab_t, list);
    obj = easy_mem_slab_get_obj(cache, slab);

    /* move slab to correct slab list */
    easy_list_del(&slab->list);

    if (slab->free == EASY_MEM_POS_END) {
        easy_list_add_head(&slab->list, &cache->slabs_full);
    } else {
        easy_list_add_head(&slab->list, &cache->slabs_partial);
    }

grow_done:
    easy_spin_unlock(&cache->lock);

    if (obj == NULL) obj = easy_mem_cache_grow(cache);

    return obj;
}

// 释放
void easy_mem_cache_free(easy_mem_cache_t *cache, void *obj)
{
    easy_mem_slab_t         *slab;

    easy_spin_lock(&cache->lock);
    slab = easy_mem_virt_to_slab(cache->order, obj);
    easy_mem_slab_put_obj(cache, slab, obj);

    // no use
    easy_list_del(&slab->list);

    if (slab->inuse == 0) {
        if (cache->free_objects > cache->free_limit) {
            cache->free_objects -= cache->num;
            slab->mem = NULL; // free mem
        } else {
            easy_list_add_head(&slab->list, &cache->slabs_free);
        }
    } else {
        easy_list_add_tail(&slab->list, &cache->slabs_partial);
    }

    easy_spin_unlock(&cache->lock);

    if (slab->mem == NULL) {
        easy_spin_lock(&easy_mem_mgr_var.lock);
        easy_mem_free_pages(easy_mem_mgr_var.zone, (easy_mem_page_t *)slab);
        easy_spin_unlock(&easy_mem_mgr_var.lock);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
easy_mem_cache_t *easy_mem_cache_create(int buffer_size)
{
    easy_mem_cache_t        *cache = NULL;
    int                     order, size, left_over, num;

    easy_spin_lock(&easy_mem_mgr_var.lock);

    if (easy_mem_mgr_var.cache_num < easy_mem_mgr_var.cache_max_num) {
        cache = easy_mem_mgr_var.caches + easy_mem_mgr_var.cache_num;
        memset(cache, 0, sizeof(easy_mem_cache_t));

        // 计算slab_size大小
        left_over = 0;

        for (order = 0; order <= EASY_MEM_MAX_ORDER; order++) {
            size = (EASY_MEM_PAGE_SIZE << order);
            num = (size - sizeof(easy_mem_slab_t)) / (buffer_size + sizeof(uint16_t));
            left_over = size - num * buffer_size - sizeof(easy_mem_slab_t);
            cache->num = num;
            cache->order = order;

            if (left_over * 4 <= size) break;
        }

        cache->buffer_size = buffer_size;
        cache->offset = (size - cache->num * buffer_size);
        cache->free_limit = cache->num * 2;
        cache->free_objects = 0;
        cache->idx = easy_mem_mgr_var.cache_num;
        easy_list_init(&cache->slabs_full);
        easy_list_init(&cache->slabs_partial);
        easy_list_init(&cache->slabs_free);

        easy_mem_mgr_var.cache_num ++;
    }

    easy_spin_unlock(&easy_mem_mgr_var.lock);

    return cache;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
static void *easy_mem_slab_get_obj(easy_mem_cache_t *cache, easy_mem_slab_t *slab)
{
    void                    *obj;
    uint16_t                next;

    obj = slab->mem + cache->buffer_size * slab->free;
    slab->inuse ++;
    next = slab->next_pos[slab->free];
    slab->free = next;

    return obj;
}

static void *easy_mem_cache_grow(easy_mem_cache_t *cache)
{
    easy_mem_zone_t         *z;
    easy_mem_slab_t         *slab;
    void                    *obj;
    unsigned char           *ptr;
    unsigned long           page_idx;
    uint32_t                i;

    z = easy_mem_mgr_var.zone;
    easy_spin_lock(&easy_mem_mgr_var.lock);
    slab = (easy_mem_slab_t *)easy_mem_alloc_pages(z, cache->order);
    easy_spin_unlock(&easy_mem_mgr_var.lock);

    // 分配不出来
    if (slab == NULL)
        return NULL;

    // init
    slab->mem = (unsigned char *)slab + cache->offset;
    slab->inuse = 0;
    slab->free = 0;
    slab->cache_idx = cache->idx;

    // set page flags
    if (cache->buffer_size > EASY_MEM_PAGE_SIZE) {
        ptr = (unsigned char *)slab->mem;

        for (i = 0; i < cache->num; i++) {
            page_idx = (ptr - z->mem_start) >> EASY_MEM_PAGE_SHIFT;
            z->page_flags[page_idx] = (0x80 | cache->order);
            ptr += cache->buffer_size;
        }
    } else {
        page_idx = ((unsigned char *)slab - z->mem_start) >> EASY_MEM_PAGE_SHIFT;
        memset(z->page_flags + page_idx, (0x80 | cache->order), (1 << cache->order));
    }

    // num
    for (i = 0; i < cache->num; i++) {
        slab->next_pos[i] = i + 1;
    }

    slab->next_pos[i - 1] = EASY_MEM_POS_END;
    cache->free_objects += cache->num;

    // 分配
    obj = easy_mem_slab_get_obj(cache, slab);

    easy_spin_lock(&cache->lock);

    if (slab->free == EASY_MEM_POS_END) {
        easy_list_add_head(&slab->list, &cache->slabs_full);
    } else {
        easy_list_add_head(&slab->list, &cache->slabs_partial);
    }

    easy_spin_unlock(&cache->lock);

    return obj;
}

// 根据obj定位出slab的位置
static inline easy_mem_slab_t *easy_mem_virt_to_slab(int order, const void *obj)
{
    unsigned long           a = (1 << (EASY_MEM_PAGE_SHIFT + order));
    return (easy_mem_slab_t *)(((unsigned long)obj) & ~(a - 1));
}

static void easy_mem_slab_put_obj(easy_mem_cache_t *cache, easy_mem_slab_t *slab, void *obj)
{
    uint16_t                idx;

    idx = (unsigned)((unsigned char *)obj - slab->mem) / cache->buffer_size;
    slab->next_pos[idx] = slab->free;
    slab->free = idx;
    slab->inuse--;
}

static inline easy_mem_cache_t *easy_mem_get_cache(unsigned char *obj)
{
    int                     order;
    easy_mem_slab_t         *slab;

    if (obj < easy_mem_mgr_var.zone->mem_start || obj >= easy_mem_mgr_var.zone->mem_end)
        return NULL;

    // order
    order = (obj - easy_mem_mgr_var.zone->mem_start) >> EASY_MEM_PAGE_SHIFT;
    order = (easy_mem_mgr_var.zone->page_flags[order] & 0x0f);
    slab = easy_mem_virt_to_slab(order, obj);
    return &easy_mem_mgr_var.caches[slab->cache_idx];
}

static inline easy_mem_cache_t *easy_mem_cache_size(uint32_t size)
{
    int                     flag, start, mid, end;

    start = 0;
    mid = 1;
    end = easy_mem_mgr_var.cache_num - 1;

    // 大于最大
    if (size > easy_mem_mgr_var.caches[end].buffer_size)
        return NULL;

    while (start != end) {
        mid = ((start + end) >> 1);
        flag = easy_mem_mgr_var.caches[mid].buffer_size - size;

        if (flag > 0)
            end = mid;
        else if (flag < 0)
            start = mid + 1;
        else {
            start = mid;
            break;
        }
    }

    return &easy_mem_mgr_var.caches[start];
}
