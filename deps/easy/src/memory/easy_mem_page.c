#include "memory/easy_mem_page.h"
#include <malloc.h>

static easy_mem_page_t *easy_mem_rmqueue(easy_mem_zone_t *zone, uint32_t order);
static void easy_mem_expand(easy_mem_zone_t *zone, easy_mem_page_t *page,
                            int low, int high, easy_mem_area_t *area);
static void easy_mem_merge_buddy_page(easy_mem_zone_t *zone, easy_mem_page_t *page, uint32_t order);

static inline easy_mem_page_t *easy_mem_page_ptr(easy_mem_page_t *page, int index);
static inline easy_mem_page_t *easy_mem_index_to_page(easy_mem_zone_t *zone, int index);
static inline int easy_mem_page_to_index(easy_mem_zone_t *zone, easy_mem_page_t *page);
static inline int easy_mem_find_buddy_index(int page_idx, uint32_t order);
static inline int easy_mem_find_combined_index(int page_idx, uint32_t order);
static inline void easy_mem_set_page_free(easy_mem_zone_t *zone, easy_mem_page_t *page, int order);
static inline void easy_mem_set_page_used(easy_mem_zone_t *zone, easy_mem_page_t *page, int order);

// 内存初始化, 最大每zone 2G
easy_mem_zone_t *easy_mem_zone_create(int64_t max_size)
{
    easy_mem_zone_t         *zone;
    unsigned char           *memptr;
    int64_t                 size;
    uint32_t                n;
    int                     page_size, pos, order, asize;

    // min = 128k
    order = 0;
    size = EASY_MEM_PAGE_SIZE;

    while (size < max_size) {
        size <<= 1;
        order ++;
    }

    pos = (size / EASY_MEM_PAGE_SIZE) + sizeof(easy_mem_zone_t);
    page_size = easy_align(pos, EASY_MEM_PAGE_SIZE);
    asize = (1 << (EASY_MEM_MAX_ORDER + EASY_MEM_PAGE_SHIFT - 1));
    asize = easy_max(page_size, easy_min(asize, size));

    // alloc memory
    if ((memptr = (unsigned char *)memalign(EASY_MEM_PAGE_SIZE, asize + size)) == NULL) {
        return NULL;
    }

    // init
    zone = (easy_mem_zone_t *)memptr;
    memset(zone, 0, sizeof(easy_mem_zone_t));
    zone->curr = memptr + easy_align(pos, 32);
    zone->curr_end = memptr + page_size;

    zone->max_order = ((order >= EASY_MEM_MAX_ORDER) ? (EASY_MEM_MAX_ORDER - 1) : order);
    zone->mem_start = (unsigned char *)easy_align((unsigned long)zone->curr_end, asize);
    zone->mem_last = zone->mem_start;
    zone->mem_end = zone->mem_start + size;

    for (n = 0; n <= zone->max_order; n++) {
        easy_list_init(&zone->area[n].free_list);
    }

    return zone;
}

// 内存释放
void easy_mem_zone_destroy(easy_mem_zone_t *zone)
{
    easy_free(zone);
}

// 内存分配
easy_mem_page_t *easy_mem_alloc_pages(easy_mem_zone_t *zone, uint32_t order)
{
    easy_mem_page_t         *page;
    easy_mem_area_t         *area;

    if (order > zone->max_order)
        return NULL;

    // 有空的page直接分配
    if (zone->free_pages >= (1 << order))
        if ((page = easy_mem_rmqueue(zone, order)) != NULL)
            return page;

    // 加入freelist
    if (zone->mem_last < zone->mem_end) {
        page = (easy_mem_page_t *) zone->mem_last;
        zone->mem_last += ((1 << zone->max_order) << EASY_MEM_PAGE_SHIFT);

        zone->free_pages += (1 << zone->max_order);
        area = zone->area + zone->max_order;
        easy_list_add_head(&page->lru, &area->free_list);
        area->nr_free ++;

        return easy_mem_rmqueue(zone, order);
    }

    return NULL;
}

// 内存释放
void easy_mem_free_pages(easy_mem_zone_t *zone, easy_mem_page_t *page)
{
    unsigned long           page_idx;
    unsigned char           *ptr = (unsigned char *) page;

    if (ptr < zone->mem_start || ptr > (zone->mem_last - EASY_MEM_PAGE_SIZE))
        return;

    // page index
    page_idx = easy_mem_page_to_index(zone, page);

    if ((zone->page_flags[page_idx] & 0x80)) {
        easy_mem_merge_buddy_page(zone, page, (zone->page_flags[page_idx] & 0x0f));
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
static easy_mem_page_t *easy_mem_rmqueue(easy_mem_zone_t *zone, uint32_t order)
{
    easy_mem_area_t         *area;
    uint32_t                n;
    easy_mem_page_t         *page;

    for (n = order; n <= zone->max_order; n++) {
        area = zone->area + n;

        if (easy_list_empty(&area->free_list))
            continue;

        page = easy_list_entry(area->free_list.next, easy_mem_page_t, lru);
        easy_list_del(&page->lru);
        area->nr_free--;
        zone->free_pages -= (1 << order);
        easy_mem_expand(zone, page, order, n, area);
        easy_mem_set_page_used(zone, page, order);
        return page;
    }

    return NULL;
}

static void easy_mem_expand(easy_mem_zone_t *zone, easy_mem_page_t *page,
                            int low, int high, easy_mem_area_t *area)
{
    easy_mem_page_t         *newpage;
    unsigned long           size = 1 << high;

    while (high > low) {
        area--;
        high--;
        size >>= 1;
        newpage = easy_mem_page_ptr(page, size);
        easy_list_add_head(&newpage->lru, &area->free_list);
        area->nr_free ++;
        easy_mem_set_page_free(zone, newpage, high);
    }
}

static void easy_mem_merge_buddy_page(easy_mem_zone_t *zone, easy_mem_page_t *page, uint32_t order)
{
    int                     page_idx, buddy_idx, combined_idx, order_size;
    easy_mem_page_t         *buddy;

    order_size = 1 << order;
    page_idx = easy_mem_page_to_index(zone, page);
    zone->free_pages += order_size;

    while (order < zone->max_order) {
        buddy_idx = easy_mem_find_buddy_index(page_idx, order);

        if (zone->page_flags[buddy_idx] != order)
            break;

        buddy = easy_mem_index_to_page(zone, buddy_idx);
        easy_list_del(&buddy->lru);
        zone->area[order].nr_free --;
        zone->page_flags[buddy_idx] = 0;

        combined_idx = easy_mem_find_combined_index(page_idx, order);
        page = easy_mem_index_to_page(zone, combined_idx);
        page_idx = combined_idx;
        order ++;
    }

    easy_mem_set_page_free(zone, page, order);
    easy_list_add_head(&page->lru, &zone->area[order].free_list);
    zone->area[order].nr_free ++;
}

// inline function
static inline easy_mem_page_t *easy_mem_page_ptr(easy_mem_page_t *page, int index)
{
    return (easy_mem_page_t *)(((unsigned char *)page) + index * EASY_MEM_PAGE_SIZE);
}

static inline int easy_mem_page_to_index(easy_mem_zone_t *zone, easy_mem_page_t *page)
{
    return (((unsigned char *)page) - zone->mem_start) >> EASY_MEM_PAGE_SHIFT;
}

static inline easy_mem_page_t *easy_mem_index_to_page(easy_mem_zone_t *zone, int index)
{
    return (easy_mem_page_t *)(zone->mem_start + (index << EASY_MEM_PAGE_SHIFT));
}

static inline int easy_mem_find_buddy_index(int page_idx, uint32_t order)
{
    return page_idx ^ (1 << order);
}

static inline int easy_mem_find_combined_index(int page_idx, uint32_t order)
{
    return (page_idx & ~(1 << order));
}

static inline void easy_mem_set_page_free(easy_mem_zone_t *zone, easy_mem_page_t *page, int order)
{
    unsigned long           page_idx;

    page_idx = (((unsigned char *)page) - zone->mem_start) >> EASY_MEM_PAGE_SHIFT;
    zone->page_flags[page_idx] = (0x0f & order);
}

static inline void easy_mem_set_page_used(easy_mem_zone_t *zone, easy_mem_page_t *page, int order)
{
    unsigned long           page_idx;

    page_idx = (((unsigned char *)page) - zone->mem_start) >> EASY_MEM_PAGE_SHIFT;
    zone->page_flags[page_idx] = (0x80 | order);
}
