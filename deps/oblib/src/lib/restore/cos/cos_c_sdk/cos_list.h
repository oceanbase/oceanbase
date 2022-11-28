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

#ifndef LIBCOS_LIST_H
#define LIBCOS_LIST_H

#include <apr_general.h>

// from kernel list
typedef struct cos_list_s cos_list_t;

struct cos_list_s {
    cos_list_t *next, *prev;
};

#define cos_list_head_init(name) {&(name), &(name)}

#define cos_list_init(ptr) do {                  \
        (ptr)->next = (ptr);                    \
        (ptr)->prev = (ptr);                    \
    } while (0)

static APR_INLINE void __cos_list_add(cos_list_t *list, cos_list_t *prev, cos_list_t *next)
{
    next->prev = list;
    list->next = next;
    list->prev = prev;
    prev->next = list;
}

// list head to add it before
static APR_INLINE void cos_list_add_tail(cos_list_t *list, cos_list_t *head)
{
    __cos_list_add(list, head->prev, head);
}

static APR_INLINE void __cos_list_del(cos_list_t *prev, cos_list_t *next)
{
    next->prev = prev;
    prev->next = next;
}

// deletes entry from list
static APR_INLINE void cos_list_del(cos_list_t *entry)
{
    __cos_list_del(entry->prev, entry->next);
    cos_list_init(entry);
}

// tests whether a list is empty
static APR_INLINE int cos_list_empty(const cos_list_t *head)
{
    return (head->next == head);
}

// move list to new_list
static APR_INLINE void cos_list_movelist(cos_list_t *list, cos_list_t *new_list)
{
    if (!cos_list_empty(list)) {
        new_list->prev = list->prev;
        new_list->next = list->next;
        new_list->prev->next = new_list;
        new_list->next->prev = new_list;
        cos_list_init(list);
    } else {
        cos_list_init(new_list);
    }
}

// get last
#define cos_list_get_last(list, type, member)                           \
    cos_list_empty(list) ? NULL : cos_list_entry((list)->prev, type, member)

// get first
#define cos_list_get_first(list, type, member)                          \
    cos_list_empty(list) ? NULL : cos_list_entry((list)->next, type, member)

#define cos_list_entry(ptr, type, member) \
    (type *)( (char *)ptr - APR_OFFSETOF(type, member) )

// traversing
#define cos_list_for_each_entry(postp, pos, head, member)                      \
    for (pos = cos_list_entry((head)->next, postp, member);      \
         &pos->member != (head);                                        \
         pos = cos_list_entry(pos->member.next, postp, member))

#define cos_list_for_each_entry_reverse(postp, pos, head, member)              \
    for (pos = cos_list_entry((head)->prev, postp, member);      \
         &pos->member != (head);                                        \
         pos = cos_list_entry(pos->member.prev, postp, member))

#define cos_list_for_each_entry_safe(postp, pos, n, head, member)              \
    for (pos = cos_list_entry((head)->next, postp, member),      \
                 n = cos_list_entry(pos->member.next, postp, member); \
         &pos->member != (head);                                        \
         pos = n, n = cos_list_entry(n->member.next, postp, member))

#define cos_list_for_each_entry_safe_reverse(postp, pos, n, head, member)      \
    for (pos = cos_list_entry((head)->prev, postp, member),      \
                 n = cos_list_entry(pos->member.prev, postp, member); \
         &pos->member != (head);                                        \
         pos = n, n = cos_list_entry(n->member.prev, postp, member))

#endif
