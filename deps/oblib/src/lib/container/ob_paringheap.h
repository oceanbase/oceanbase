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

#ifndef OCEANBASE_PARING_HEAP_H_
#define OCEANBASE_PARING_HEAP_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/*
 * A Pairing Heap implementation.
 *
 *******************************************************************************
 */

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace container
{

template<class T>
struct ObParingHeapNodeBase
{
  ObParingHeapNodeBase()
  : prev_(NULL),
    next_(NULL),
    lchild_(NULL) {}
  ~ObParingHeapNodeBase() {}
  OB_INLINE T *get_prev() const
  {
    return prev_;
  }

  OB_INLINE void set_prev(T *node)
  {
    prev_ = node;
  }

  OB_INLINE T *get_next() const
  {
    return next_;
  }

  OB_INLINE void set_next(T *node)
  {
    next_ = node;
  }

  OB_INLINE T *get_lchild() const
  {
    return lchild_;
  }

  OB_INLINE void set_lchild(T *node)
  {
    lchild_ = node;
  }

  T *prev_;
  T *next_;
  T *lchild_;
};

#define PHNODE(type, phlink) class ObParingHeapNode##_##phlink : public ObParingHeapNodeBase<type> { public :\
   OB_INLINE static type *get_next(const type * n) { return n->phlink##_.get_next(); } \
   OB_INLINE static void set_next(type *s, type *t) { s->phlink##_.set_next(t); } \
   OB_INLINE static type *get_prev(const type *n) { return n->phlink##_.get_prev(); } \
   OB_INLINE static void set_prev(type *s, type *t) { s->phlink##_.set_prev(t); } \
   OB_INLINE static type *get_lchild(const type *n) { return n->phlink##_.get_lchild(); } \
   OB_INLINE static void set_lchild(type *s, type *t) { s->phlink##_.set_lchild(t); } \
   OB_INLINE static void mem_set(type *n) { memset(&n->phlink##_, 0, sizeof(ObParingHeapNodeBase)); } \
}; ObParingHeapNodeBase<type> phlink##_;

template<typename Key>
struct ObDummyCompHelper
{
  int compare(const Key *search_key, const Key *index_key) const
  {
    return search_key->compare(index_key);
  }
};

template<class T, class Compare, class L = typename T::ObParingHeapNode_phlink>
class ObParingHeap
{
public :
  typedef Compare ObCompHepler;
  typedef ObParingHeap<T, Compare, L> Paringheap;

  ObParingHeap()
      : root_(NULL) {}

  ~ObParingHeap() {}

  OB_INLINE int init()
  {
    int ret = OB_SUCCESS;
    root_ = NULL;
    return ret;
  }

  OB_INLINE bool is_empty()
  {
    return OB_ISNULL(root_);
  }

  OB_INLINE int get_first(T *&r_node)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(root_)) {
      r_node = NULL;
    } else if (OB_FAIL(merge_aux())) {
      OB_LOG(ERROR, "paring heap merge aux fail", K(ret));
    } else {
      r_node = root_;
    }
    return ret;
  }

  OB_INLINE int get_any(T *&r_node)
  {
    int ret = OB_SUCCESS;
    T *aux = NULL;
    if (OB_ISNULL(root_)) {
      r_node = NULL;
    } else {
      aux = get_next(root_);
      if (OB_NOT_NULL(aux)) {
        r_node = aux;
      } else {
        r_node = root_;
      }
    }
    return ret;
  }

  OB_INLINE int insert(T *phn)
  {
    /*
     * Treat the root as an aux list during insertion, and lazily
     * merge during remove_first().  For elements that
     * are inserted, then removed via remove() before the
     * aux list is ever processed, this makes insert/remove
     * constant-time, whereas eager merging would make insert
     * O(log n).
     */
    int ret = OB_SUCCESS;
    T *t_node = NULL;
    if (OB_ISNULL(phn)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "paring heap insert fail", K(ret));
    } else {
      mem_set(phn);
      if (OB_ISNULL(root_)) {
        root_ = phn;
      } else {
        t_node = get_next(root_);
        set_next(phn, t_node);
        if (OB_NOT_NULL(t_node)) {
          set_prev(t_node, phn);
        }
        set_prev(phn, root_);
        set_next(root_, phn);
      }
    }
    return ret;
  }

  OB_INLINE int remove_first(T *&r_node)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(merge_aux())) {
      OB_LOG(ERROR, "paring heap merge aux fail", K(ret));
    } else {
      r_node = root_;
      ret = merge_children(root_, root_);
    }
    return ret;
  }

  OB_INLINE int remove_any(T *&r_node)
  {
    int ret = OB_SUCCESS;
    T *aux = NULL;
    if (OB_ISNULL(root_)) {
      r_node = NULL;
    } else {
      r_node = get_next(root_);
      if (OB_NOT_NULL(r_node)) {
        aux = get_next(r_node);
        set_next(root_, aux);
        if (OB_NOT_NULL(aux)) {
          set_prev(aux, root_);
        }
      } else {
        r_node = root_;
        ret = merge_children(root_, root_);
      }
    }
    return ret;
  }

  OB_INLINE int remove(T *phn)
  {
    int ret = OB_SUCCESS;
    T *replace = NULL;
    T *parent = NULL;
    T *t_node = NULL;
    T *next = NULL;
    if (OB_ISNULL(phn)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "paring remove fail", K(ret));
    } else if (root_ == phn) {
      /*
       * We can delete from aux list without merging it, but
       * we need to merge if we are dealing with the root
       * node and it has children.
       */
      t_node = get_lchild(phn);
      if (OB_ISNULL(t_node)) {
        root_ = get_next(phn);
        if (OB_NOT_NULL(root_)) {
          set_prev(root_, NULL);
        }
      } else {
        if (OB_FAIL(merge_aux())) {
          OB_LOG(ERROR, "paring heap merge aux fail", K(ret));
        } else {
          if (root_ == phn) {
            ret = merge_children(root_, root_);
          }
        }
      }
    } else {
      if (OB_NOT_NULL(parent = get_prev(phn))) {
        /* Get parent (if phn is leftmost child) before mutating. */
        if (phn != get_lchild(parent)) {
          parent = NULL;
        }
      }
      if (OB_FAIL(merge_children(phn, replace))) {
        OB_LOG(ERROR, "paring heap merge children fail", K(ret));
      } else {
        if (OB_NOT_NULL(replace)) {
          if (OB_NOT_NULL(parent)) {
            set_prev(replace, parent);
            set_lchild(parent, replace);
          } else {
            t_node = get_prev(phn);
            set_prev(replace, t_node);
            if (OB_NOT_NULL(t_node)) {
              set_next(t_node, replace);
            }
          }
          t_node = get_next(phn);
          set_next(replace, t_node);
          if (OB_NOT_NULL(t_node)) {
            set_prev(t_node, replace);
          }
        } else {
          if (OB_NOT_NULL(parent)) {
            next = get_next(phn);
            set_lchild(parent, next);
            if (OB_NOT_NULL(next)) {
              set_prev(next, parent);
            }
          } else {
            t_node = get_prev(phn);
            if (OB_ISNULL(t_node)) {
              ret = OB_ERROR;
              OB_LOG(ERROR, "node prev should not be NULL", K(ret));
            } else {
              set_next(t_node, get_next(phn));
            }
          }
          if (OB_NOT_NULL(get_next(phn))) {
            set_prev(get_next(phn), get_prev(phn));
          }
        }
      }
    }
    return ret;
  }

  OB_INLINE int merge_ordered(T *phn_first, T *phn_second)
  {
    int ret = OB_SUCCESS;
    T *phn_first_child = NULL;
    if (OB_ISNULL(phn_first) || OB_ISNULL(phn_second)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(ERROR , "paring heap merge ordered fail", K(ret));
    } else {
      if (compare_.compare(phn_first, phn_second) > 0) {
        ret = OB_ERROR;
        OB_LOG(ERROR, "phn_first should smaller phn_second", K(ret));
      } else {
        set_prev(phn_second, phn_first);
        phn_first_child = get_lchild(phn_first);
        set_next(phn_second, phn_first_child);
        if (OB_NOT_NULL(phn_first_child)) {
          set_prev(phn_first_child, phn_second);
        }
        set_lchild(phn_first, phn_second);
      }
    }
    return ret;
  }

  OB_INLINE int merge(T *phn_first, T *phn_second, T *&r_phn)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(phn_first)) {
      r_phn = phn_second;
    } else if (OB_ISNULL(phn_second)) {
      r_phn = phn_first;
    } else if (compare_.compare(phn_first, phn_second) < 0) {
      if (OB_FAIL(merge_ordered(phn_first, phn_second))) {
        OB_LOG(ERROR, "paring heap merge ordered fail", K(ret));
      } else {
        r_phn = phn_first;
      }
    } else if (OB_FAIL(merge_ordered(phn_second, phn_first))) {
      OB_LOG(ERROR, "paring heap merge ordered fail", K(ret));
    } else {
      r_phn = phn_second;
    }
    return ret;
  }

  OB_INLINE int merge_siblings(T *phn, T *&r_phn)
  {
    int ret = OB_SUCCESS;
    T *head = NULL;
    T *tail = NULL;
    T *phn_first = NULL;
    T *phn_second = NULL;
    T *phnrest = NULL;

    /*
     * Multipass merge, wherein the first two elements of a FIFO
     * are repeatedly merged, and each result is appended to the
     * singly linked FIFO, until the FIFO contains only a single
     * element.  We start with a sibling list but no reference to
     * its tail, so we do a single pass over the sibling list to
     * populate the FIFO.
     */

    phn_first = phn;
    phn_second = get_next(phn_first);
    if (OB_NOT_NULL(phn_second)) {
      phnrest = get_next(phn_second);
      if (OB_NOT_NULL(phnrest)) {
        set_prev(phnrest, NULL);
      }
      set_prev(phn_first, NULL);
      set_next(phn_first, NULL);
      set_prev(phn_second, NULL);
      set_next(phn_second, NULL);
      if (OB_FAIL(merge(phn_first, phn_second, phn_first))) {
        OB_LOG(ERROR, "paring heap merge fail", K(ret));
      } else {
        head = tail = phn_first;
        phn_first = phnrest;
        while (OB_NOT_NULL(phn_first)) {
          phn_second = get_next(phn_first);
          if (OB_NOT_NULL(phn_second)) {
            phnrest = get_next(phn_second);
            if (OB_NOT_NULL(phnrest)) {
              set_prev(phnrest, NULL);
            }
            set_prev(phn_first, NULL);
            set_next(phn_first, NULL);
            set_prev(phn_second, NULL);
            set_next(phn_second, NULL);
            if (OB_FAIL(merge(phn_first, phn_second, phn_first))) {
              OB_LOG(ERROR, "paring heap merge fail", K(ret));
            } else {
              set_next(tail, phn_first);
              tail = phn_first;
              phn_first = phnrest;
            }
          } else {
            set_next(tail, phn_first);
            tail = phn_first;
            phn_first = NULL;
          }

        }
        if (OB_SUCC(ret)) {
          phn_first = head;
          phn_second = get_next(phn_first);
          if (OB_NOT_NULL(phn_second)) {
            do {
              head = get_next(phn_second);
              if (OB_NOT_NULL(get_prev(phn_first))) {
                ret = OB_ERROR;
                OB_LOG(ERROR, "paring heap prev should be NULL", K(ret));
              } else {
                set_next(phn_first, NULL);
                if (OB_NOT_NULL(get_prev(phn_second))) {
                  ret = OB_ERROR;
                  OB_LOG(ERROR, "paring heap prev should be NULL", K(ret));
                } else {
                  set_next(phn_second, NULL);
                  if (OB_FAIL(merge(phn_first, phn_second, phn_first))) {
                    OB_LOG(ERROR, "paring heap merge fail", K(ret));
                  } else if (OB_NOT_NULL(head)) {
                    set_next(tail, phn_first);
                    tail = phn_first;
                    phn_first = head;
                    phn_second = get_next(phn_first);
                  }
                }
              }
            } while (OB_SUCC(ret) && OB_NOT_NULL(head));
          }
        }
      }
    }
    r_phn = phn_first;
    return ret;
  }

  OB_INLINE int merge_aux()
  {
    int ret = OB_SUCCESS;
    T *phn = NULL;
    phn = get_next(root_);
    if (OB_NOT_NULL(phn)) {
      set_prev(root_, NULL);
      set_next(root_, NULL);
      set_prev(phn, NULL);
      if (OB_FAIL(merge_siblings(phn, phn))) {
        OB_LOG(ERROR, "paring heap merge siblings fail", K(ret));
      } else if (OB_NOT_NULL(get_next(phn))) {
        ret = OB_ERROR;
        OB_LOG(ERROR, "paring heap next should  be NULL", K(ret));
      } else {
        ret = merge(root_, phn, root_);
      }
    }
    return ret;
  }

  OB_INLINE int merge_children(T *phn, T *&r_phn)
  {
    int ret = OB_SUCCESS;
    T *lchild = NULL;
    if (OB_ISNULL(phn)) {
      ret = OB_ERROR;
      OB_LOG(ERROR, "paring heap merge children fail", K(ret));
    } else {
      lchild = get_lchild(phn);
      if (OB_ISNULL(lchild)) {
        r_phn = NULL;
      } else {
        ret = merge_siblings(lchild, r_phn);
      }
    }
    return ret;
  }

  OB_INLINE T *get_root() const
  {
    return root_;
  }

  OB_INLINE void mem_set(T * node)
  {
    L::mem_set(node);
  }

  OB_INLINE T *get_next(const T *node) const
  {
    return L::get_next(node);
  }

  OB_INLINE void set_next(T *node_source, T *node_target)
  {
    L::set_next(node_source, node_target);
  }

  OB_INLINE T *get_prev(const T *node) const
  {
    return L::get_prev(node);
  }

  OB_INLINE void set_prev(T *node_source, T *node_target)
  {
    L::set_prev(node_source, node_target);
  }

  OB_INLINE T *get_lchild(const T *node) const
  {
    return L::get_lchild(node);
  }

  OB_INLINE void set_lchild(T *node_source, T *node_target)
  {
    L::set_lchild(node_source, node_target);
  }

private:
  T *root_;
  ObCompHepler compare_;
};

}//container
}//oceanbase

#endif /* PARING_HEAP_H_ */
