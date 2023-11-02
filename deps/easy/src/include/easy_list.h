#ifndef EASY_LIST_H_
#define EASY_LIST_H_

/**
 * 列表，参考kernel上的list.h
 */
#include "easy_define.h"

EASY_CPP_START

// from kernel list
typedef struct easy_list_t easy_list_t;

struct easy_list_t {
    easy_list_t             *next, *prev;
};

#define EASY_LIST_HEAD_INIT(name) {&(name), &(name)}
#define easy_list_init(ptr) do {                \
        (ptr)->next = (ptr);                    \
        (ptr)->prev = (ptr);                    \
    } while (0)

static inline void __easy_list_add(easy_list_t *list,
                                   easy_list_t *prev, easy_list_t *next)
{
    next->prev = list;
    list->next = next;
    list->prev = prev;
    prev->next = list;
}
// list head to add it after
static inline void easy_list_add_head(easy_list_t *list, easy_list_t *head)
{
    __easy_list_add(list, head, head->next);
}
// list head to add it before
static inline void easy_list_add_tail(easy_list_t *list, easy_list_t *head)
{
    __easy_list_add(list, head->prev, head);
}
static inline void __easy_list_del(easy_list_t *prev, easy_list_t *next)
{
    next->prev = prev;
    prev->next = next;
}
// deletes entry from list
static inline void easy_list_del(easy_list_t *entry)
{
    __easy_list_del(entry->prev, entry->next);
    easy_list_init(entry);
}

static inline void easy_list_replace(easy_list_t* entry, easy_list_t* new_entry)
{
  __easy_list_add(new_entry, entry->prev, entry->next);
  easy_list_init(entry);
}

// tests whether a list is empty
static inline int easy_list_empty(const easy_list_t *head)
{
    return (head->next == head);
}
// move list to new_list
static inline void easy_list_movelist(easy_list_t *list, easy_list_t *new_list)
{
    if (!easy_list_empty(list)) {
        new_list->prev = list->prev;
        new_list->next = list->next;
        new_list->prev->next = new_list;
        new_list->next->prev = new_list;
        easy_list_init(list);
    } else {
        easy_list_init(new_list);
    }
}
// join list to head
static inline void easy_list_join(easy_list_t *list, easy_list_t *head)
{
    if (!easy_list_empty(list)) {
        easy_list_t             *first = list->next;
        easy_list_t             *last = list->prev;
        easy_list_t             *at = head->prev;

        first->prev = at;
        at->next = first;
        last->next = head;
        head->prev = last;
    }
}

// get last
#define easy_list_get_last(list, type, member)                              \
    easy_list_empty(list) ? NULL : easy_list_entry((list)->prev, type, member)

// get first
#define easy_list_get_first(list, type, member)                             \
    easy_list_empty(list) ? NULL : easy_list_entry((list)->next, type, member)

#define easy_list_entry(ptr, type, member) ({                               \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);                \
        (type *)( (char *)__mptr - offsetof(type,member) );})

#define easy_list_for_each_entry(pos, head, member)                         \
    for (pos = easy_list_entry((head)->next, typeof(*pos), member);         \
            &pos->member != (head);                                         \
            pos = easy_list_entry(pos->member.next, typeof(*pos), member))

#define easy_list_for_each_entry_reverse(pos, head, member)                 \
    for (pos = easy_list_entry((head)->prev, typeof(*pos), member);     \
            &pos->member != (head);                                        \
            pos = easy_list_entry(pos->member.prev, typeof(*pos), member))

#define easy_list_for_each_entry_safe(pos, n, head, member)                 \
    for (pos = easy_list_entry((head)->next, typeof(*pos), member),         \
            n = easy_list_entry(pos->member.next, typeof(*pos), member);    \
            &pos->member != (head);                                         \
            pos = n, n = easy_list_entry(n->member.next, typeof(*n), member))

#define easy_list_for_each_entry_safe_reverse(pos, n, head, member)         \
    for (pos = easy_list_entry((head)->prev, typeof(*pos), member),         \
            n = easy_list_entry(pos->member.prev, typeof(*pos), member);    \
            &pos->member != (head);                                         \
            pos = n, n = easy_list_entry(n->member.prev, typeof(*n), member))

EASY_CPP_END

#endif
