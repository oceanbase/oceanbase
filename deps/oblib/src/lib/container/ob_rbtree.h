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

#ifndef OCEANBASE_RBTREE_REDBLACKTREE_
#define OCEANBASE_RBTREE_REDBLACKTREE_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#include "lib/ob_abort.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace container
{

template<typename T>
struct ObRbNode
{
  ObRbNode() : left_(NULL), right_red_(NULL) {}
  ~ObRbNode() {}

  // Left accessors
  OB_INLINE T *get_left() const
  {
    return left_;
  }

  OB_INLINE void set_left(T *node)
  {
    left_ = node;
  }

  //Right accessors
  OB_INLINE T *get_right() const
  {
    return (reinterpret_cast<T *>((reinterpret_cast<int64_t> (right_red_)) & (static_cast<int64_t> (- 2))));
  }

  OB_INLINE void set_right(T *right_node)
  {
    right_red_ = reinterpret_cast<T *>((reinterpret_cast<uint64_t> (right_node))
            | ((reinterpret_cast<uint64_t> (right_red_)) & (static_cast<uint64_t> (1))));
  }

  T *left_;
  T *right_red_;
};


#define RBNODE(type, rblink) class ObRbNode##_##rblink : public container::ObRbNode<type> { public : \
  OB_INLINE static type *get_left(const type *n) { return n->rblink##_.get_left(); } \
  OB_INLINE static void set_left(type *s, type *t) { s->rblink##_.set_left(t); } \
  OB_INLINE static type *get_right(const type *n) { return n->rblink##_.get_right(); } \
  OB_INLINE static void set_right(type *s, type *t) { s->rblink##_.set_right(t); } \
  OB_INLINE static void set_color(type *n, const bool red) { \
    type *tmp = reinterpret_cast<type *>(((reinterpret_cast<int64_t>((n)->rblink##_.right_red_)) & (static_cast<int64_t>(- 2))) | (static_cast<int64_t>(red))); \
    n->rblink##_.right_red_ = tmp; } \
  OB_INLINE static bool get_red(type *n) { return (static_cast<bool>(((reinterpret_cast<uint64_t>((n)->rblink##_.right_red_)) & (static_cast<uint64_t>(1))))); } \
  OB_INLINE static void set_red(type *n) { n->rblink##_.right_red_ = reinterpret_cast<type *>((reinterpret_cast<uint64_t>(n->rblink##_.right_red_)) | (static_cast<int64_t>(1))); } \
  OB_INLINE static void set_black(type *n) { n->rblink##_.right_red_ = reinterpret_cast<type *>((reinterpret_cast<int64_t>(n->rblink##_.right_red_)) & (static_cast<int64_t>(- 2))); } \
}; container::ObRbNode<type> rblink##_;


template<typename Key>
struct ObDummyCompHelper
{
  int compare(const Key *source_node, const Key *target_node) const
  {
    return source_node->compare(target_node);
  }
};

// Red-black tree structure.
template<class T, class Compare, class L = typename T ::ObRbNode_rblink>
class ObRbTree
{
public:

  struct ObRbPath
  {
    T *node_;
    int cmp_;
  };

  typedef Compare CompHepler;
  typedef ObRbTree<T, Compare, L> Rbtree;

  ObRbTree()
      : root_(NULL) {}
  ~ObRbTree() {};

  OB_INLINE int init_tree()
  {
    int ret = common::OB_SUCCESS;
    root_ = NULL;
    return ret;
  }

  OB_INLINE bool is_empty()
  {
    return (NULL == root_);
  }

  OB_INLINE T *get_first()
  {
    T *tmp_node = NULL;
    if (OB_NOT_NULL(root_)) {
      tmp_node = get_first(root_);
    }
    return tmp_node;
  }

  OB_INLINE T *get_last()
  {
    T *tmp_node = NULL;
    if (OB_NOT_NULL(root_)) {
      tmp_node = get_last(root_);
    }
    return tmp_node;
  }

  OB_INLINE int get_next(const T *node, T *&return_node)
  {
    int ret = common::OB_SUCCESS;
    T *tmp_node = NULL;
    int cmp = 0;
    if (OB_ISNULL(node)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "get rb tree next fail", K(ret));
    } else {
      tmp_node = get_right(node);
      if (OB_NOT_NULL(tmp_node)) {
        return_node = get_first(tmp_node);
      } else {
        tmp_node = root_;
        abort_unless(NULL != tmp_node);
        return_node = NULL;
        do {
          cmp  = compare_.compare(node, tmp_node);
          if (cmp < 0) {
            return_node = tmp_node;
            tmp_node = get_left(tmp_node);
          } else if (cmp > 0) {
            tmp_node = get_right(tmp_node);
          }
        } while (0 != cmp && OB_NOT_NULL(tmp_node));
      }
    }
    return ret;
  }

  OB_INLINE int get_prev(const T *node, T *&return_node)
  {
    int ret = common::OB_SUCCESS;
    T *tmp_node = NULL;
    int cmp = 0;
    if (OB_ISNULL(node)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "get rbtree prev node fail", K(ret));
    } else {
      tmp_node = get_left(node);
      if (OB_NOT_NULL(tmp_node)) {
        return_node = get_last(tmp_node);
      } else {
        tmp_node = root_;
        abort_unless(NULL != tmp_node);
        return_node = NULL;
        do {
          cmp = compare_.compare(node, tmp_node);
          if (cmp < 0) {
            tmp_node = get_left(tmp_node);
          } else if (cmp > 0) {
            return_node = tmp_node;
            tmp_node = get_right(tmp_node);
          }
        } while (0 != cmp && OB_NOT_NULL(tmp_node));
      }
    }
    return ret;
  }

  //Node in tree that matches key, or NULL if no match
  OB_INLINE int search(const T *key, T *&return_node)
  {
    int ret = common::OB_SUCCESS;
    int cmp = 0;
    return_node = NULL;
    if (OB_ISNULL(key)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "key should not be NULL", K(ret));
    } else {
      return_node = root_;
      while (OB_NOT_NULL(return_node) && (0 != (cmp = compare_.compare(key, return_node)))) {
        if (cmp < 0) {
          return_node = get_left(return_node);
        } else {
          return_node = get_right(return_node);
        }
      }
    }
    return ret;
  }

  //Node in tree that matches key, or if no match, hypothetical node's successor
  OB_INLINE int nsearch(const T *key, T *&return_node)
  {
    int ret = common::OB_SUCCESS;
    int cmp = 0;
    T *tmp_node = NULL;
    tmp_node = root_;
    return_node = NULL;
    if (OB_ISNULL(key)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "key should not be NULL", K(ret));
    } else {
      while (OB_NOT_NULL(tmp_node)) {
        cmp = compare_.compare(key, tmp_node);
        if (cmp < 0) {
          return_node = tmp_node;
          tmp_node = get_left(tmp_node);
        } else if (cmp > 0) {
          tmp_node = get_right(tmp_node);
        } else {
          return_node = tmp_node;
          break;
        }
      }
    }
    return ret;
  }

  //Node in tree that matches key, or if no match, hypothetical node's predecessor
  OB_INLINE int psearch(const T *key, T *&return_node)
  {
    int ret = common::OB_SUCCESS;
    int cmp = 0;
    T *tmp_node = NULL;
    return_node = NULL;
    if (OB_ISNULL(key)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "key should not be NULL", K(ret));
    } else {
      tmp_node = root_;
      while (OB_NOT_NULL(tmp_node)) {
        cmp = compare_.compare(key, tmp_node);
        if (cmp < 0) {
          tmp_node = get_left(tmp_node);
        } else if (cmp > 0) {
          return_node = tmp_node;
          tmp_node = get_right(tmp_node);
        } else {
          return_node = tmp_node;
          break;
        }
      }
    }
    return ret;
  }

  OB_INLINE int insert(T *node)
  {
    int ret = common::OB_SUCCESS;
    int cmp = 0;
    T *cur_node = NULL;
    T *tmp_node = NULL;
    T *left_node = NULL;
    T *right_node = NULL;
    T *left_node_left = NULL;
    ObRbPath path[sizeof(void *) << 4], *pathp;

    if (OB_ISNULL(node)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "rbtree insert node fail", K(ret));
    } else {
      init_node(node);
      path[0].node_ = root_;
      for (pathp = path; OB_NOT_NULL(pathp->node_); pathp++) {
        cmp = pathp->cmp_ = compare_.compare(node, pathp->node_);
        abort_unless(0 != cmp);
        if (cmp < 0) {
          pathp[1].node_ = get_left(pathp->node_);
        } else if (cmp > 0) {
          pathp[1].node_ = get_right(pathp->node_);
        }
      }

      pathp->node_ = node;
      for (pathp--; (reinterpret_cast<uint64_t>(pathp) >= reinterpret_cast<uint64_t>(path)); pathp--) {
        cur_node = pathp->node_;
        if (pathp->cmp_ < 0) {
          left_node = pathp[1].node_;
          set_left(cur_node, left_node);
          if (get_red(left_node)) {
            left_node_left = get_left(left_node);
            if (OB_NOT_NULL(left_node_left) && get_red(left_node_left)) {
              //Fix up 4-node.
              set_black(left_node_left);
              rotate_right(cur_node, tmp_node);
              cur_node = tmp_node;
            }
          } else {
            break;
          }
        } else {
          right_node = pathp[1].node_;
          set_right(cur_node, right_node);
          if (!get_red(right_node)) {
            break;
          } else {
            left_node = get_left(cur_node);
            if (OB_NOT_NULL(left_node) && get_red(left_node)) {
              //Split 4-node.
              set_black(left_node);
              set_black(right_node);
              set_red(cur_node);
            } else {
              //Lean left.
              bool tred = get_red(cur_node);
              rotate_left(cur_node, tmp_node);
              set_color(tmp_node, tred);
              set_red(cur_node);
              cur_node = tmp_node;
            }
          }
        }
        pathp->node_ = cur_node;
      }

      //Set root, and make it black.
      root_ = path->node_;
      set_black(root_);

    }
    return ret;
  }

  OB_INLINE int remove(T *node)
  {
    int ret = common::OB_SUCCESS;
    int cmp = 0;
    T *left_node = NULL;
    T *left_node_right = NULL;
    T *left_node_right_left = NULL;
    T *left_node_left = NULL;
    T *right_node = NULL;
    T *right_node_left = NULL;
    T *tmp_node = NULL;
    T *cur_node = NULL;
    bool finish_flag = false;

    ObRbPath *pathp, *nodep, path[sizeof(void *) << 4];
    nodep = NULL;

    if (OB_ISNULL(node)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "rbtree remove node fail", K(ret));
    } else {
      path->node_ = root_;
      for (pathp = path; OB_NOT_NULL(pathp->node_); pathp++) {
        cmp = pathp->cmp_ = compare_.compare(node, pathp->node_);
        if (cmp < 0) {
          pathp[1].node_ = get_left(pathp->node_);
        } else {
          pathp[1].node_ = get_right(pathp->node_);
          if (0 == cmp) {
            // Find node's successor, in preparation for swap.
            pathp->cmp_ = 1;
            nodep = pathp;
            for (pathp++; NULL != pathp->node_; pathp++) {
              pathp->cmp_ = -1;
              pathp[1].node_ = get_left(pathp->node_);
            }
            break;
          }
        }
      }

      abort_unless(nodep->node_ == node);
      pathp--;
      if (pathp->node_ != node) {
        //Swap node with its successor.
        bool tred = get_red(pathp->node_);
        set_color(pathp->node_, get_red(node));
        set_left(pathp->node_, get_left(node));
        //If node's successor is its right child, the following code
        //will do the wrong thing for the right child pointer.
        //However, it doesn't matter, because the pointer will be
        //properly set when the successor is pruned.
        set_right(pathp->node_, get_right(node));
        set_color(node, tred);
        //The pruned leaf node's child pointers are never accessed
        //again, so don't bother setting them to nil.
        nodep->node_ = pathp->node_;
        pathp->node_ = node;
        if (nodep == path) {
          root_ = nodep->node_;
        } else if (nodep[-1].cmp_ < 0) {
          set_left(nodep[-1].node_, nodep->node_);
        } else {
          set_right(nodep[-1].node_, nodep->node_);
        }
      } else {
        left_node = get_left(node);
        if (OB_NOT_NULL(left_node)) {
          //node has no successor, but it has a left child.
          //Splice node out, without losing the left child.
          abort_unless(!get_red(node));
          abort_unless(get_red(left_node));
          set_black(left_node);
          if (pathp == path) {
            root_ = left_node;
          } else if (pathp[-1].cmp_ < 0) {
            set_left(pathp[-1].node_, left_node);
          } else {
            set_right(pathp[-1].node_, left_node);
          }
          finish_flag = true;
        } else if (pathp == path) {
          //The tree only contained one node.
          root_ = NULL;
          finish_flag = true;
        }
      }

      if (!finish_flag) {
        if (get_red(pathp->node_)) {
          //Prune red node, which requires no fixup.
          abort_unless(pathp[-1].cmp_ < 0);
          set_left(pathp[-1].node_, NULL);
          finish_flag = true;
        }
      }

      if (!finish_flag) {
        //The node to be pruned is black, so unwind until balance is
        //restored.
        pathp->node_ = NULL;
        for (pathp--; (reinterpret_cast<uint64_t>(pathp) >= reinterpret_cast<uint64_t>(path)) && (!finish_flag); pathp--) {
          abort_unless(0 != pathp->cmp_);
          if (pathp->cmp_ < 0) {
            set_left(pathp->node_, pathp[1].node_);
            if (get_red(pathp->node_)) {
              right_node = get_right(pathp->node_);
              right_node_left = get_left(right_node);
              if (OB_NOT_NULL(right_node_left) && get_red(right_node_left)) {
                /* In the following diagrams, ||, //, and \\      */
                /* indicate the path to the removed node.         */
                /*                                                */
                /*      ||                                        */
                /*    pathp(r)                                    */
                /*  //        \                                   */
                /* (b)        (b)                                 */
                /*           /                                    */
                /*          (r)                                   */
                /*                                                */
                set_black(pathp->node_);
                rotate_right(right_node, tmp_node);
                set_right(pathp->node_, tmp_node);
                rotate_left(pathp->node_, tmp_node);
              } else {
                /*      ||                                        */
                /*    pathp(r)                                    */
                /*  //        \                                   */
                /* (b)        (b)                                 */
                /*           /                                    */
                /*          (b)                                   */
                /*                                                */
                rotate_left(pathp->node_, tmp_node);
              }
              //Balance restored, but rotation modified subtree
              //root.
              abort_unless(reinterpret_cast<uint64_t>(pathp) > reinterpret_cast<uint64_t>(path));
              if (pathp[-1].cmp_ < 0) {
                set_left(pathp[-1].node_, tmp_node);
              } else {
                set_right(pathp[-1].node_, tmp_node);
              }
              finish_flag = true;
            } else {
              right_node = get_right(pathp->node_);
              right_node_left = get_left(right_node);
              if (OB_NOT_NULL(right_node_left) && get_red(right_node_left)) {
                /*      ||                                        */
                /*    pathp(b)                                    */
                /*  //        \                                   */
                /* (b)        (b)                                 */
                /*           /                                    */
                /*          (r)                                   */
                set_black(right_node_left);
                rotate_right(right_node, tmp_node);
                set_right(pathp->node_, tmp_node);
                rotate_left(pathp->node_, tmp_node);
                //Balance restored, but rotation modified
                //subtree root, which may actually be the tree
                //root.
                if (pathp == path) {
                  //Set root.
                  root_ = tmp_node;
                } else if (pathp[-1].cmp_ < 0) {
                  set_left(pathp[-1].node_, tmp_node);
                } else {
                  set_right(pathp[-1].node_, tmp_node);
                }
                finish_flag = true;
              } else {
                /*      ||                                        */
                /*    pathp(b)                                    */
                /*  //        \                                   */
                /* (b)        (b)                                 */
                /*           /                                    */
                /*          (b)                                   */
                set_red(pathp->node_);
                rotate_left(pathp->node_, tmp_node);
                pathp->node_ = tmp_node;
              }
            }

          } else {
            set_right(pathp->node_, pathp[1].node_);
            left_node = get_left(pathp->node_);
            if (get_red(left_node)) {
              left_node_right = get_right(left_node);
              left_node_right_left = get_left(left_node_right);
              if (OB_NOT_NULL(left_node_right_left) && get_red(left_node_right_left)) {
                /*      ||                                        */
                /*    pathp(b)                                    */
                /*   /        \\                                  */
                /* (r)        (b)                                 */
                /*   \                                            */
                /*   (b)                                          */
                /*   /                                            */
                /* (r)                                            */
                set_black(left_node_right_left);
                rotate_right(pathp->node_, cur_node);
                rotate_right(pathp->node_, tmp_node);
                set_right(cur_node, tmp_node);
                rotate_left(cur_node, tmp_node);

              } else {
                /*      ||                                        */
                /*    pathp(b)                                    */
                /*   /        \\                                  */
                /* (r)        (b)                                 */
                /*   \                                            */
                /*   (b)                                          */
                /*   /                                            */
                /* (b)                                            */
                abort_unless(NULL != left_node_right);
                set_red(left_node_right);
                rotate_right(pathp->node_, tmp_node);
                set_black(tmp_node);
              }
              //Balance restored, but rotation modified subtree
              //root, which may actually be the tree root.
              if (pathp == path) {
                root_ = tmp_node;
              } else if (pathp[-1].cmp_ < 0) {
                set_left(pathp[-1].node_, tmp_node);
              } else {
                set_right(pathp[-1].node_, tmp_node);
              }
              finish_flag = true;
            } else if (get_red(pathp->node_)) {
              left_node_left = get_left(left_node);
              if (OB_NOT_NULL(left_node_left) && get_red(left_node_left)) {
                /*        ||                                      */
                /*      pathp(r)                                  */
                /*     /        \\                                */
                /*   (b)        (b)                               */
                /*   /                                            */
                /* (r)                                            */

                set_black(pathp->node_);
                set_red(left_node);
                set_black(left_node_left);
                rotate_right(pathp->node_, tmp_node);
                //Balance restored, but rotation modified
                //subtree root.
                abort_unless(reinterpret_cast<uint64_t>(pathp) > reinterpret_cast<uint64_t>(path));
                if (pathp[-1].cmp_ < 0) {
                  set_left(pathp[-1].node_, tmp_node);
                } else {
                  set_right(pathp[-1].node_, tmp_node);
                }
                finish_flag = true;

              } else {
                /*        ||                                      */
                /*      pathp(r)                                  */
                /*     /        \\                                */
                /*   (b)        (b)                               */
                /*   /                                            */
                /* (b)                                            */

                set_red(left_node);
                set_black(pathp->node_);
                //Balance restored.
                finish_flag = true;
              }
            } else {
              left_node_left = get_left(left_node);
              if (OB_NOT_NULL(left_node_left) && get_red(left_node_left)) {
                /*               ||                               */
                /*             pathp(b)                           */
                /*            /        \\                         */
                /*          (b)        (b)                        */
                /*          /                                     */
                /*        (r)                                     */

                set_black(left_node_left);
                rotate_right(pathp->node_, tmp_node);
                //Balance restored, but rotation modified
                //subtree root, which may actually be the tree
                //root.
                if (pathp == path) {
                  //Set root.
                  root_ = tmp_node;
                } else if (pathp[-1].cmp_ < 0) {
                  set_left(pathp[-1].node_, tmp_node);
                } else {
                  set_right(pathp[-1].node_, tmp_node);
                }
                finish_flag = true;
              } else {
                /*               ||                               */
                /*             pathp(b)                           */
                /*            /        \\                         */
                /*          (b)        (b)                        */
                /*          /                                     */
                /*        (b)                                     */
                set_red(left_node);
              }
            }

          }
        }
        if (!finish_flag) {
          //Set root.
          root_ = path->node_;
          abort_unless(!get_red(root_));
        }
      }

    }
    return ret;
  }

  T *iter_recurse(Rbtree *rbtree, T *node,
                          T * (*cb)(Rbtree *, T *, void *), void *arg)
  {
    T *r_node = NULL;
    if (OB_ISNULL(node) || OB_ISNULL(cb)) {
      r_node = NULL;
    } else {
      if (OB_ISNULL(r_node = iter_recurse(rbtree, get_left(node), cb, arg)) && OB_ISNULL(r_node = cb(rbtree, node, arg))) {
          r_node = iter_recurse(rbtree, get_right(node), cb, arg);
      }
    }
    return r_node;
  }

  T *iter_start(Rbtree *rbtree, T *start, T *node,
                        T * (*cb)(Rbtree *, T *, void *), void *arg)
  {
    int cmp = 0;
    T *r_node = NULL;
    if (OB_ISNULL(start) || OB_ISNULL(node) || OB_ISNULL(cb)) {
      r_node = NULL;
    } else {
      cmp = compare_.compare(start, node);
      if (0 > cmp) {
        if (OB_ISNULL(r_node = iter_start(rbtree, start, get_left(node), cb, arg)) &&
            OB_ISNULL(r_node = cb(rbtree, node, arg))) {
          r_node = iter_recurse(rbtree, get_right(node), cb, arg);
        }
      } else if (0 < cmp) {
        r_node = iter_start(rbtree, start, get_right(node), cb, arg);
      } else {
        if (OB_ISNULL(r_node = cb(rbtree, node, arg))) {
          r_node = iter_recurse(rbtree, get_right(node), cb, arg);
        }
      }
    }
    return r_node;
  }

  OB_INLINE T *iter_rbtree(Rbtree *rbtree, T *start,
                  T * (*cb)(Rbtree *, T *, void *), void *arg)
  {
    T *r_node = NULL;
    if (OB_ISNULL(cb)) {
      r_node = NULL;
    } else if (OB_NOT_NULL(start)) {
      r_node = iter_start(rbtree, start, rbtree->root_,
                                  cb, arg);
    } else {
      r_node = iter_recurse(rbtree, rbtree->root_,
                                    cb, arg);
    }
    return r_node;
  }

  T *reverse_iter_recurse(Rbtree *rbtree, T *node,
                                  T * (*cb)(Rbtree *, T *, void *), void *arg)
  {
    T *r_node = NULL;
    if (OB_ISNULL(node) || OB_ISNULL(cb)) {
      r_node = NULL;
    } else {
      if (OB_ISNULL(r_node = reverse_iter_recurse(rbtree, get_right(node), cb, arg)) &&
          OB_ISNULL(r_node = cb(rbtree, node ,arg))) {
        r_node = reverse_iter_recurse(rbtree, get_left(node), cb, arg);
      }
    }
    return r_node;
  }

  T *reverse_iter_start(Rbtree *rbtree, T *start, T *node,
                                T * (*cb)(Rbtree *, T *, void *), void *arg)
  {
    T *r_node = NULL;
    int cmp = 0;
    if (OB_ISNULL(start) || OB_ISNULL(node) || OB_ISNULL(cb)) {
      r_node = NULL;
    } else {
      cmp = compare_.compare(start, node);
      if (cmp > 0) {
        if (OB_ISNULL(r_node = reverse_iter_start(rbtree, start, get_right(node), cb, arg)) &&
            OB_ISNULL(r_node = cb(rbtree, node, arg))) {
          r_node = reverse_iter_recurse(rbtree, get_left(node), cb, arg);
        }
      } else if (cmp < 0) {
        r_node = reverse_iter_start(rbtree, start,
                                          get_left(node), cb, arg);
      } else {
        if (OB_ISNULL(r_node = cb(rbtree, node ,arg))) {
          r_node = reverse_iter_recurse(rbtree, get_left(node), cb, arg);
        }
      }
    }
    return r_node;
  }

  OB_INLINE T *reverse_iter_rbtree(Rbtree *rbtree, T *start,
                          T * (*cb)(Rbtree *, T *, void *), void *arg)
  {
    T *r_node = NULL;
    if (OB_ISNULL(cb)) {
      r_node = NULL;
    } else if (NULL != start) {
      r_node = reverse_iter_start(rbtree, start,
                                          rbtree->root_, cb, arg);
    } else {
      r_node = reverse_iter_recurse(rbtree,
                                            rbtree->root_, cb, arg);
    }
    return r_node;
  }


  int destroy_recurse(Rbtree *rbtree, T *node,
                              void (*cb)(T *, void *), void *arg)
  {
    int ret = common::OB_SUCCESS;
    if (OB_NOT_NULL(node) && OB_NOT_NULL(cb)) {
      if (OB_FAIL(destroy_recurse(rbtree, get_left(node), cb, arg))) {
        OB_LOG(ERROR, "red black tree destroy recurse fail", K(ret));
      } else {
        set_left(node, NULL);
        if (OB_FAIL(destroy_recurse(rbtree, get_right(node), cb, arg))) {
          OB_LOG(ERROR, "red black tree destroy recurse fail", K(ret));
        } else {
          set_right(node, NULL);
        }
        if (OB_SUCC(ret)) {
          if (cb) {
            cb(node, arg);
          }
        }
      }
    }
    return ret;
  }

  OB_INLINE void destroy(Rbtree *rbtree, void (*cb)(T *, void *), void *arg)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(rbtree) || OB_ISNULL(cb)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "rbtree destroy fail", K(ret));
    } else {
      if (OB_FAIL(destroy_recurse(rbtree, rbtree->root_, cb, arg))) {
        OB_LOG(ERROR, "red black tree destroy recurse fail", K(ret));
      } else {
        rbtree->root_ = NULL;
      }
    }
  }

  OB_INLINE uint64_t get_black_height(Rbtree *rbtree)
  {
    T *t_node = NULL;
    uint64_t r_height = 0;
    for (t_node = rbtree->root_, r_height = 0; t_node != NULL;
         t_node = get_left(t_node)) {
      if (!get_red(t_node)) {
        r_height++;
      }
    }
    return r_height;
  }

  //Color accessors.
  OB_INLINE bool get_red(T *node) const
  {
    return L::get_red(node);
  }

  OB_INLINE void set_color(T *node, const bool red)
  {
    L::set_color(node, red);
  }

  OB_INLINE void set_red(T *node)
  {
    L::set_red(node);
  }

  OB_INLINE void set_black(T *node)
  {
    L::set_black(node);
  }

  OB_INLINE T *get_root() const
  {
    return root_;
  }

  OB_INLINE T *get_left(const T *node) const
  {
    return L::get_left(node);
  }

  OB_INLINE void set_left(T *node_source, T *node_target)
  {
    L::set_left(node_source, node_target);
  }

  OB_INLINE T *get_right(const T *node) const
  {
    return L::get_right(node);
  }

  OB_INLINE void set_right(T *node_source, T *node_target)
  {
    L::set_right(node_source, node_target);
  }

private:
  //Node initializer
  OB_INLINE void init_node(T *node)
  {
    abort_unless(0 == (reinterpret_cast<uint64_t>(node) & 0x1));
    set_left(node, NULL);
    set_right(node, NULL);
    set_red(node);
  }

  //Internal utility macros.
  OB_INLINE T *get_first(T *root)
  {
    T *tmp_node = NULL;
    T * return_node = NULL;
    return_node = root;
    tmp_node = return_node;
    while (OB_NOT_NULL((return_node = get_left(return_node)))) {
      tmp_node = return_node;
    }
    return_node = tmp_node;
    return return_node;
  }

  OB_INLINE T *get_last(T *root)
  {
    T *tmp_node = NULL;
    T *return_node = NULL;
    return_node = root;
    tmp_node = return_node;
    while (OB_NOT_NULL((return_node = get_right(return_node)))) {
      tmp_node = return_node;
    }
    return_node = tmp_node;
    return return_node;
  }

  OB_INLINE void rotate_left(T *node, T *&return_node)
  {
    return_node = get_right(node);
    set_right(node, get_left(return_node));
    set_left(return_node, node);
  }

  OB_INLINE void rotate_right(T *node, T *&return_node)
  {
    return_node = get_left(node);
    set_left(node, get_right(return_node));
    set_right(return_node, node);
  }

private:
  T *root_;
  CompHepler compare_;
  DISALLOW_COPY_AND_ASSIGN(ObRbTree);
};
}
}

#endif /* OCEANBASE_RBTREE_REDBLACKTREE_ */
