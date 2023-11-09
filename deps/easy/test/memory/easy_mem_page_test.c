#include "memory/easy_mem_page.h"
#include <easy_test.h>

/**
 * 测试 easy_mem_page
 */

const int64_t           zone_size = (1LL << (EASY_MEM_MAX_ORDER - 1)) << EASY_MEM_PAGE_SHIFT;
// easy_mem_zone_t *easy_mem_zone_create(int max_size)
TEST(easy_mem_page, zone_create)
{
    easy_mem_zone_t         *zone;
    int                     i, size, cnt;

    // 1.
    zone = easy_mem_zone_create(zone_size / 2);
    EXPECT_TRUE(zone != NULL);
    size = (1 << zone->max_order) * EASY_MEM_PAGE_SIZE;
    EXPECT_EQ(size, zone_size / 2);
    easy_mem_zone_destroy(zone);

    // 2.
    zone = easy_mem_zone_create(zone_size * 4);
    EXPECT_TRUE(zone != NULL);
    size = (1 << zone->max_order) * EASY_MEM_PAGE_SIZE;
    EXPECT_EQ(size, zone_size);

    // 3.
    cnt = 0;

    for(i = 0; i <= (int)zone->max_order; i++) {
        if (easy_list_empty(&zone->area[i].free_list)) cnt ++;
    }

    EXPECT_EQ(cnt, zone->max_order + 1);
    easy_mem_zone_destroy(zone);
}

//easy_mem_page_t *easy_mem_alloc_pages(zone, uint32_t order)
TEST(easy_mem_page, alloc_pages)
{
    easy_mem_page_t         *page;
    easy_mem_zone_t         *zone;
    int64_t                 i, n, cnt;

    zone = easy_mem_zone_create(zone_size);

    // 1.
    cnt = 0;

    for(i = 0; i < zone->max_order; i++) {
        page = easy_mem_alloc_pages(zone, i);
        cnt += (1 << i);
        EXPECT_TRUE(page != NULL);
    }

    cnt += (1 << 0);
    page = easy_mem_alloc_pages(zone, 0);
    EXPECT_TRUE(page != NULL);
    EXPECT_EQ(cnt * EASY_MEM_PAGE_SIZE, zone_size);
    EXPECT_EQ(zone->free_pages, 0);
    page = easy_mem_alloc_pages(zone, 0);
    EXPECT_TRUE(page == NULL);
    easy_mem_zone_destroy(zone);

    // 2.
    zone = easy_mem_zone_create(zone_size * 8);
    cnt = 0;

    for(n = 0; n < 4; n++) {
        for(i = 0; i <= zone->max_order; i++) {
            page = easy_mem_alloc_pages(zone, i);
            cnt += (1 << i);
            EXPECT_TRUE(page != NULL);
        }

        page = easy_mem_alloc_pages(zone, 0);
        cnt += (1 << 0);
        EXPECT_TRUE(page != NULL);
    }

    EXPECT_EQ(cnt * EASY_MEM_PAGE_SIZE, zone_size * 8);

    // 3.
    page = easy_mem_alloc_pages(zone, 0);
    EXPECT_TRUE(page == NULL);
    easy_mem_zone_destroy(zone);

    // 4.
    zone = easy_mem_zone_create(zone_size);
    page = easy_mem_alloc_pages(zone, EASY_MEM_MAX_ORDER);
    EXPECT_TRUE(page == NULL);
    easy_mem_zone_destroy(zone);
}

//void easy_mem_free_pages(zone, easy_mem_page_t *page, uint32_t order)
TEST(easy_mem_page, free_pages)
{
    easy_mem_page_t         *page[EASY_MEM_MAX_ORDER];
    easy_mem_zone_t         *zone;
    easy_mem_area_t         *area;
    int                     i, cnt;

    // 1.
    zone = easy_mem_zone_create(zone_size);
    cnt = 0;

    for(i = 0; i < (int)zone->max_order; i++) {
        page[i] = easy_mem_alloc_pages(zone, i);
        EXPECT_TRUE(page[i] != NULL);
        cnt ++;
    }

    // 2.
    for(i = 0; i < cnt; i++) {
        easy_mem_free_pages(zone, page[i]);
    }

    page[0] = easy_mem_alloc_pages(zone, zone->max_order);
    EXPECT_TRUE(page[0] != NULL);
    page[1] = easy_mem_alloc_pages(zone, 0);
    EXPECT_TRUE(page[1] == NULL);
    easy_mem_free_pages(zone, page[0]);
    // branch.
    area = &zone->area[zone->max_order];

    if (!easy_list_empty(&area->free_list)) {
        page[0] = easy_list_entry(area->free_list.next, easy_mem_page_t, lru);
        easy_list_del(&(page[0]->lru));
        page[1] = easy_mem_alloc_pages(zone, 0);
        EXPECT_TRUE(page[1] == NULL);
        easy_list_add_head(&(page[0]->lru), &area->free_list);
    }

    // 3.
    for(i = 0; i < (int)zone->max_order; i++) {
        page[i] = easy_mem_alloc_pages(zone, i);
        EXPECT_TRUE(page[i] != NULL);
        easy_mem_free_pages(zone, page[i]);
    }

    EXPECT_EQ(zone->free_pages, zone_size >> EASY_MEM_PAGE_SHIFT);

    // 4. other
    page[0] = easy_mem_alloc_pages(zone, 0);
    EXPECT_EQ(zone->area[0].nr_free, 1);
    zone->page_flags[0] = '\0';
    easy_mem_free_pages(zone, page[0]);

    easy_mem_free_pages(zone, NULL);
    easy_mem_free_pages(zone, (easy_mem_page_t *)1);
    easy_mem_free_pages(zone, (easy_mem_page_t *) - 1);
    easy_mem_zone_destroy(zone);
}

TEST(easy_mem_page, all)
{
    easy_mem_zone_t         *zone;
    easy_mem_page_t         *page[128];
    int                     i, cnt, type;

    // 1.
    zone = easy_mem_zone_create(zone_size);
    memset(page, 0, sizeof(page));
    srand(time(NULL));
    cnt = 0;

    for(i = 0; i < 10000; i++) {
        type = rand();

        if (type % 3 != 0 && cnt < 127) {
            page[cnt++] = easy_mem_alloc_pages(zone, type % 4);
            memset(page[cnt - 1], 0, 128);
        } else if (cnt > 0) {
            easy_mem_free_pages(zone, page[--cnt]);

            while(cnt > 100) easy_mem_free_pages(zone, page[--cnt]);
        }
    }

    // 2. check
    while(cnt > 0) easy_mem_free_pages(zone, page[--cnt]);

    cnt = 0;

    for(i = 0; i < (int)zone->max_order; i++) {
        if (easy_list_empty(&zone->area[i].free_list) && zone->area[i].nr_free == 0) {
            cnt ++;
        }
    }

    EXPECT_EQ(cnt, zone->max_order);

    if (easy_list_empty(&zone->area[zone->max_order].free_list) || zone->area[zone->max_order].nr_free == 0) {
        EXPECT_TRUE(0);
    }

    easy_mem_zone_destroy(zone);
}
