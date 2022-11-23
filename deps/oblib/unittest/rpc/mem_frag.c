/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <linux/kernel.h>
#include <linux/skbuff.h>
#include <linux/rtnetlink.h>
#include <linux/netdevice.h>
#include <linux/ip.h>
#include <linux/tcp.h>

struct page_list {
  struct page_list *next;
};

static struct page_list *free_list;

uint64_t count=0;

#define PG_ORDER  2

int __init mem_frag_init(void)
{
  struct page *pg;
  struct page_list *tmp;
  struct page_list *prev, *next;

  free_list = NULL;
  while (1) {
    pg = alloc_pages(GFP_KERNEL, PG_ORDER);
    if (pg == NULL)
      break;
    tmp = (struct page_list *)page_address(pg);
    tmp->next = free_list;
    free_list = tmp;
    count++;
    //printk(KERN_DEBUG "PFN:%lu\n", page_to_pfn(pg));
  }

  printk(KERN_DEBUG "%s: alloc count:%llu\n", __func__, count);

  if (free_list == NULL)
    return -ENOMEM;
  //free half of pages back to buddy.
  prev = free_list;

  while (prev && prev->next) {
    next = prev->next;
    prev->next = next->next;
    free_pages((uint64_t)next, PG_ORDER);
    prev = prev->next;
    count--;
  }
  printk(KERN_DEBUG "%s: Fin count:%llu\n", __func__, count);

  return 0;
}

void __exit mem_frag_exit(void)
{
  struct page_list *tmp;

  while (free_list) {
    tmp = free_list;
    free_list = free_list->next;
    free_pages((uint64_t)tmp, PG_ORDER);
    count--;
  }
  printk(KERN_DEBUG "%s: exit. count:%llu\n", __func__, count);
}

module_init(mem_frag_init);
module_exit(mem_frag_exit);
MODULE_DESCRIPTION("make page fragment");
MODULE_LICENSE("GPL");
