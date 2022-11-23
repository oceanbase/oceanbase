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

#include <unistd.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include "lib/container/ob_rbtree.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace container
{

#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

typedef struct node_s node_t;

struct node_s
{
#define NODE_MAGIC 0x9823af7e
  int magic;
  RBNODE(node_t, rblink);
  int key;

  inline int compare(const node_s *node) const
  {
    int ret = 0;
    EXPECT_EQ(this->magic, NODE_MAGIC);
    EXPECT_EQ(node->magic, NODE_MAGIC);
    ret = (key > node->key) - (key < node->key);
    if (ret == 0) {
      /*
       * Duplicates are not allowed in the tree, so force an
       * arbitrary ordering for non-identical items with equal keys.
       */
      ret = (((uintptr_t)this) > ((uintptr_t)node))
        - (((uintptr_t)this) < ((uintptr_t)node));
    }
    return ret;
  }
};

typedef ObRbTree<node_t, ObDummyCompHelper<node_t>> tree_t;

TEST (TestRbTree, empty)
{
  int ret = OB_SUCCESS;
  node_t *r_node = NULL;
  tree_t tree;
  node_t key;
  tree.init_tree();
  bool tmp = tree.is_empty();
  EXPECT_EQ(tmp, true);
  r_node = tree.get_first();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree get first fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    ASSERT_TRUE(r_node == NULL);
  }

  r_node = tree.get_last();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree get last fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    ASSERT_TRUE(r_node == NULL);
  }

  key.key = 0;
  key.magic = NODE_MAGIC;
  ret = tree.search(&key, r_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree search fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    ASSERT_TRUE(r_node == NULL);
  }

  key.key = 0;
  key.magic = NODE_MAGIC;
  ret = tree.nsearch(&key, r_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree nsearch fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    ASSERT_TRUE(r_node == NULL);
  }

  ret = tree.psearch(&key, r_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree psarch fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    ASSERT_TRUE(r_node == NULL);
  }
}

static unsigned tree_recurse(node_t *node, uint64_t black_height,
                             unsigned black_depth, tree_t &rbtree)
{
  unsigned ret = 0;
  node_t *left_node = NULL;
  node_t *right_node = NULL;

  if (node == NULL) {
    return ret;
  }

  left_node = rbtree.get_left(node);
  right_node = rbtree.get_right(node);

  if (!rbtree.get_red(node)) {
    black_depth++;
  }

  /* Red nodes must be interleaved with black nodes. */
  if (rbtree.get_red(node)) {
    if (left_node != NULL) {
      EXPECT_EQ(rbtree.get_red(left_node), false);
    }
    if (right_node != NULL) {
      EXPECT_EQ(rbtree.get_red(right_node), false);
    }
  }

  /* Self. */
  EXPECT_EQ(node->magic, NODE_MAGIC);

  /* Left subtree. */
  if (left_node != NULL) {
          ret += tree_recurse(left_node, black_height, black_depth, rbtree);
  } else {
          ret += (black_depth != black_height);
  }

  /* Right subtree. */
  if (right_node != NULL) {
          ret += tree_recurse(right_node, black_height, black_depth, rbtree);
  } else {
          ret += (black_depth != black_height);
  }
  return ret;
}

static node_t *tree_iterate_cb(tree_t *tree, node_t *node, void *data)
{
  unsigned *i = (unsigned *)data;
  node_t *search_node = NULL;
  int ret = OB_SUCCESS;
  EXPECT_EQ(node->magic, NODE_MAGIC);

  /* Test rb_search(). */
  ret = tree->search(node, search_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree search fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    EXPECT_EQ(node, search_node);
  }

  /* Test rb_nsearch(). */
  ret = tree->nsearch(node, search_node);
  if (OB_FAIL(ret)) {
     fprintf(stderr, "red black tree nsearch fail %d", ret);
     EXPECT_EQ(ret, OB_SUCCESS);
   } else {
     EXPECT_EQ(node, search_node);
   }

  /* Test rb_psearch(). */
  ret = tree->psearch( node, search_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree psearch fail %d", ret);
     EXPECT_EQ(ret, OB_SUCCESS);
   } else {
     EXPECT_EQ(node, search_node);
   }
  (*i)++;
  return NULL;
}

static unsigned tree_iterate(tree_t *tree)
{
  unsigned i = 0;
  tree->iter_rbtree(tree, NULL, tree_iterate_cb, (void *)&i);
  return i;
}

static unsigned tree_iterate_reverse(tree_t *tree)
{
  unsigned i = 0;
  tree->reverse_iter_rbtree(tree, NULL, tree_iterate_cb, (void *)&i);
  return i;
}

static void node_remove(tree_t *tree, node_t *node, unsigned nnodes)
{
  int ret = OB_SUCCESS;
  node_t *search_node = NULL;
  uint64_t black_height = 0;
  unsigned imbalances = 0;

  int count = 0;

  ret = tree->remove(node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree remove fail %d", ret);
  }
  assert(ret == OB_SUCCESS);
  /* Test rb_nsearch(). */
  ret = tree->nsearch(node, search_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red tree nsearch fail %d", ret);
    assert(ret == OB_SUCCESS);
  } else {
    if (NULL != search_node) {
      ASSERT_TRUE(search_node->key >= node->key);
    }
  }

  /* Test rb_psearch(). */
  ret = tree->psearch(node,search_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red tree psearch fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    if (NULL != search_node) {
      ASSERT_TRUE(search_node->key <= node->key);
    }
  }
  node->magic = 0;
  black_height = tree->get_black_height(tree);
  imbalances = tree_recurse(tree->get_root(), black_height, 0, *tree);
  EXPECT_EQ(imbalances, 0);

  count = tree_iterate(tree);
  EXPECT_EQ(count, nnodes - 1);

  count = tree_iterate_reverse(tree);
  EXPECT_EQ(count, nnodes - 1);
}

static node_t *remove_iterate_cb(tree_t *tree, node_t *node, void *data)
{
  int ret = OB_SUCCESS;
  node_t *tmp_node = NULL;
  unsigned *nnodes = (unsigned *)data;
  ret = tree->get_next(node, tmp_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree get next fail %d", ret);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    node_remove(tree, node, *nnodes);
    return tmp_node;
  }
  return NULL;
}

static node_t *remove_reverse_iterate_cb(tree_t *tree, node_t *node, void *data) {
  int ret = OB_SUCCESS;
  node_t *tmp_node = NULL;
  unsigned *nnodes = (unsigned *)data;
  ret = tree->get_prev(node, tmp_node);
  if (OB_FAIL(ret)) {
    fprintf(stderr, "red black tree get prev fail %d", ret);
    assert(ret == OB_SUCCESS);
    EXPECT_EQ(ret, OB_SUCCESS);
  } else {
    node_remove(tree, node, *nnodes);
    return tmp_node;
  }

  return NULL;
}

static void destroy_cb(node_t *node , void *data) {
  UNUSED(node);
  unsigned *nnodes = (unsigned *)data;
  assert(*nnodes > 0);
  (*nnodes)--;
}

TEST (TestRbTree, random)
{
#define NNODES 25
#define NBAGS 250
#define SEED 42
  int bag[NNODES];
  tree_t tree;
  node_t nodes[NNODES];
  uint64_t black_height;
  unsigned i, j, k, imbalances;
  srand((unsigned)time(NULL));

  i = 2;
  int tmp = 0;
  bool test_bool = false;
  node_t *tmp_node = NULL;
  node_t *pre_node = NULL;
  UNUSED(pre_node);
  for (i = 0; i < NBAGS; i++) {
    switch (i) {
    case 0:
            /* Insert in order. */
            for (j = 0; j < NNODES; j++) {
               bag[j] = j;
            }
            break;
    case 1:
            /* Insert in reverse order. */
            for (j = 0; j < NNODES; j++) {
               bag[j] = NNODES - j - 1;
            }
            break;
    default:{
            //cout <<"Enter default"<<endl;
            for (j = 0; j < NNODES; j++) {
               bag[j] = rand()%100;
            }
          }
    }

    for (j = 1; j <= NNODES; j++) {
            /* Initialize tree and nodes. */
      tree.init_tree();
      for (k = 0; k < j; k++) {
         nodes[k].magic = NODE_MAGIC;
         nodes[k].key = bag[k];
      }

      /* Insert nodes. */
      for (k = 0; k < j; k++) {
        int ret = tree.insert(&nodes[k]);
        EXPECT_EQ(ret, OB_SUCCESS);
        black_height = tree.get_black_height(&tree);

        imbalances = tree_recurse(tree.get_root(),
            black_height, 0, tree);
        EXPECT_EQ(imbalances, 0);

        tmp = tree_iterate(&tree);
        EXPECT_EQ(tmp, k+1);
        tmp = tree_iterate_reverse(&tree);
        EXPECT_EQ(tmp, k+1);

        test_bool = tree.is_empty();
        EXPECT_EQ(test_bool, false);
        tmp_node = tree.get_first();
        if (OB_FAIL(ret)) {
          fprintf(stderr, "red black tree get first fail %d", ret);
          EXPECT_EQ(ret, OB_SUCCESS);
        } else {
          ASSERT_TRUE(tmp_node != NULL);
        }

        tmp_node = tree.get_last();
        if (OB_FAIL(ret)) {
          fprintf(stderr, "red black tree get last fail %d", ret);
          EXPECT_EQ(ret, OB_SUCCESS);
         } else {
           ASSERT_TRUE(tmp_node != NULL);
         }
          ret = tree.get_next(&nodes[k],tmp_node);
          EXPECT_EQ(ret, OB_SUCCESS);

          ret = tree.get_prev(&nodes[k],tmp_node);
          EXPECT_EQ(ret, OB_SUCCESS);
      }

      switch (i % 5) {
      case 0:
              for (k = 0; k < j; k++) {
                  node_remove(&tree,&nodes[k],j-k);
              }
              break;
      case 1:
              for (k = j;k > 0; k--) {
                   node_remove(&tree, &nodes[k-1],k);
               }
              break;
      case 2: {
              node_t *start = NULL;
              unsigned nnodes = j;

              start = NULL;
              do {
                      start = tree.iter_rbtree(&tree, start,
                          remove_iterate_cb, (void *)&nnodes);
                      nnodes--;
              } while (start != NULL);
              assert(nnodes == 0);
              break;
      } case 3: {
              node_t *start = NULL;
              unsigned nnodes = j;
              do {
                      start = tree.reverse_iter_rbtree(&tree, start,
                          remove_reverse_iterate_cb,
                          (void *)&nnodes);
                      nnodes--;
              } while (start != NULL);
              assert(nnodes == 0);
              break;
      } case 4: {
               unsigned nnodes = j;
               tree.destroy(&tree, destroy_cb, &nnodes);
               assert(nnodes == 0);
              break;
      } default:
              break;
      }
    }
  }
#undef NNODES
#undef NBAGS
#undef SEED
}

}
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
