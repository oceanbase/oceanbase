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
#include <assert.h>
#include <time.h>
#include "lib/container/ob_paringheap.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace container
{

typedef struct node_s node_t;
struct node_s
{
#define NODE_MAGIC 0x9823af7e
  uint32_t magic;
  PHNODE(node_t, phlink);
  uint64_t key;

  inline int compare(const node_t *node) const
  {
    int ret = 0;
    EXPECT_EQ(this->magic, NODE_MAGIC);
    EXPECT_EQ(node->magic, NODE_MAGIC);
    ret = (this->key > node->key) - (this->key < node->key);
    if (ret == 0) {
      /*
       * Duplicates are not allowed in the heap, so force an
       * arbitrary ordering for non-identical items with equal keys.
       */
      ret = (((uintptr_t)this) > ((uintptr_t)node))
          - (((uintptr_t)this) < ((uintptr_t)node));
    }
    return ret;
  }

};

typedef ObParingHeap<node_t, ObDummyCompHelper<node_t>> heap_t;

static void node_print(heap_t *heap , node_t *node, unsigned depth) {
  unsigned i;
  node_t *leftmost_child, *sibling;

  for (i = 0; i < depth; i++) {
          printf("\t");
  }
  printf("%ld\n", node->key);
  leftmost_child = heap->get_lchild(node);
  if (leftmost_child == NULL) {
          return;
  }
  node_print(heap, leftmost_child, depth + 1);

  for (sibling = heap->get_next(leftmost_child); sibling !=
      NULL; sibling = heap->get_next(sibling)) {
          node_print(heap, sibling, depth + 1);
  }
}

static void heap_print(heap_t *heap) {
  node_t *auxelm;

  printf("vvv heap %p vvv\n", heap);
  if (heap->get_root() == NULL) {
          goto label_return;
  }
  node_print(heap, heap->get_root(), 0);

  for (auxelm = heap->get_next(heap->get_root()); auxelm != NULL;
      auxelm = heap->get_next(auxelm)) {
      assert(heap->get_next(heap->get_prev(auxelm)) == auxelm);
      node_print(heap, auxelm, 0);
  }

label_return:
        printf("^^^ heap %p ^^^\n", heap);
}

static unsigned node_validate(heap_t *heap, node_t *node,
                              const node_t *parent)
{
  unsigned nnodes = 1;
  node_t *leftmost_child, *sibling;

  if (parent != NULL) {
    EXPECT_GE(node->compare(parent), 0);
  }

  leftmost_child = heap->get_lchild(node);
  if (leftmost_child == NULL) {
          return nnodes;
  }
  assert(heap->get_prev(leftmost_child) == node);
  nnodes += node_validate(heap, leftmost_child, node);

  for (sibling = heap->get_next(leftmost_child); sibling !=
      NULL; sibling = heap->get_next(sibling)) {
      EXPECT_TRUE(heap->get_next(heap->get_prev(sibling)) == sibling);
      nnodes += node_validate(heap, sibling, node);
  }
  return nnodes;
}

static unsigned heap_validate(heap_t *heap)
{
  unsigned nnodes = 0;
  node_t *auxelm;

  if (heap->get_root() == NULL) {
          goto label_return;
  }

  nnodes += node_validate(heap, heap->get_root(), NULL);

  for (auxelm = heap->get_next(heap->get_root()); auxelm != NULL;
      auxelm = heap->get_next(auxelm)) {
    //EXPECT_TRUE(auxelm->ph_link.get_prev()->ph_link.get_next() == auxelm);
    EXPECT_TRUE(heap->get_next(heap->get_prev(auxelm)) == auxelm);
    nnodes += node_validate(heap, auxelm, NULL);
  }

label_return:
  if (false) {
    heap_print(heap);
  }
  return nnodes;
}

TEST (TEST_ParingHeap, empty)
{
  heap_t heap;
  int ret = 0;
  node_t *r_node = NULL;
  heap.init();
  ASSERT_TRUE(heap.is_empty() == true);

  ret = heap.get_first(r_node);
  if (0 != ret) {
    fprintf(stderr, "paring heap get first fail %d", ret);
  } else {
    ASSERT_TRUE(r_node == NULL);
  }
  ret = heap.get_any(r_node);
  if (0 != ret) {
    fprintf(stderr, "paring heap get any fail %d", ret);
  } else {
    ASSERT_TRUE(r_node == NULL);
  }
}

TEST (Test_ParingHeap, random)
{
#define NNODES 25
#define NBAGS 250
#define SEED 42


  int bag[NNODES];
  heap_t heap;
  node_t nodes[NNODES];
  unsigned i, j, k;

  srand((unsigned)time(NULL));

  int ret = 0;
  node_t *r_node = NULL;
  i = 0;
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
    default:
            for (j = 0; j < NNODES; j++) {
                    bag[j] = rand();
            }
            break;
    }

    for (j = 1; j <= NNODES; j++) {
            /* Initialize heap and nodes. */
            ret = heap.init();
            if (0 != ret) {
              fprintf(stderr, "paring heap new fail %d", ret);
              ASSERT_TRUE(ret == 0);
            } else {
              ASSERT_TRUE(heap_validate(&heap) == 0);
            }

            for (k = 0; k < j; k++) {
                    nodes[k].magic = NODE_MAGIC;
                    nodes[k].key = bag[k];
            }

            /* Insert nodes. */
            for (k = 0; k < j; k++) {
              ret = heap.insert(&nodes[k]);
              if (ret != 0) {
                fprintf(stderr, "heap paring insert fail %d", ret);
                ASSERT_TRUE(ret == 0);
              }
              if (i % 13 == 12) {
                ret = heap.get_any(r_node);
                if (ret != 0) {
                  fprintf(stderr, "heap paring any fail %d",ret);
                  ASSERT_TRUE(ret == 0);
                } else {
                  ASSERT_TRUE(r_node != NULL);
                }


                ret = heap.get_first(r_node);
                if (ret != 0) {
                  fprintf(stderr, "paring heap get first fail %d", ret);
                  ASSERT_TRUE(ret == 0);
                } else {
                  ASSERT_TRUE(r_node != NULL);
                }
                      /* Trigger merging. */
              }
              ASSERT_TRUE(heap_validate(&heap) == (k+1));
            }

            ASSERT_TRUE(heap.is_empty() != true);

            switch(i % 6) {
              case 0 :
                {
                  for (k = 0; k < j; k++) {
                    ASSERT_TRUE(heap_validate(&heap) == (j-k));
                    heap.remove(&nodes[k]);
                    ASSERT_TRUE(heap_validate(&heap) == (j-k-1));
                  }
                  break;
                }
              case 1:
                {
                  for (k = j; k > 0; k--) {
                    heap.remove(&nodes[k-1]);
                    ASSERT_TRUE(heap_validate(&heap) == (k - 1));
                  }
                  break;
                }
              case 2:
                {
                  node_t *prev = NULL;
                  for (k = 0; k < j; k++) {
                    ret = heap.remove_first(r_node);
                    if (0 != ret) {
                       fprintf(stderr, "ERROR:remove_first ret error %d", ret);
                       ASSERT_TRUE(ret == 0);
                    }
                    ASSERT_TRUE(heap_validate(&heap) == (j-k-1));
                    if (prev != NULL) {
                      ASSERT_TRUE(r_node->compare(prev) >= 0);
                    }
                    prev = r_node;
                  }
                  break;
                }
              case 3:
                {
                  node_t *prev = NULL;
                   for (k = 0; k < j; k++) {
                     ret = heap.get_first(r_node);
                     if (0 != ret) {
                         fprintf(stderr,"ERROR :ph_first ret error %d", ret);
                         ASSERT_TRUE(ret == 0);
                     }
                     ASSERT_TRUE(heap_validate(&heap) == (j-k));
                     if (prev != NULL) {
                       ASSERT_TRUE(r_node->compare(prev) >= 0);
                     }
                     heap.remove(r_node);
                     ASSERT_TRUE(heap_validate(&heap) == (j-k-1));
                     prev = r_node;
                   }
                   break;
                }
              case 4:
                {
                  for (k = 0; k < j; k++) {
                     ret = heap.remove_any(r_node);
                     if (0 != ret) {
                       fprintf(stderr, "ERROR: ph_remove_any ret error %d", ret);
                       ASSERT_TRUE(ret == 0);
                     }
                     ASSERT_TRUE(heap_validate(&heap) == (j-k-1));
                   }
                  break;
                }
              case 5:
                {
                  for (k = 0; k < j; k++) {
                     ret = heap.get_any(r_node);
                     if (0 != ret) {
                       fprintf(stderr, "ERROR:ph_any ret error %d", ret);
                       ASSERT_TRUE(ret == 0);
                     }
                     ASSERT_TRUE(heap_validate(&heap) == (j-k));
                     ret = heap.remove(r_node);
                     if (0 != ret) {
                       fprintf(stderr, "ERROR:paring heap remove error %d", ret);
                       ASSERT_TRUE(ret == 0);
                     }
                     ASSERT_TRUE(heap_validate(&heap) == (j-k-1));
                  }
                  break;
                }

            }


          ret = heap.get_first(r_node);
          ASSERT_TRUE(ret == 0);
          ASSERT_TRUE(heap.is_empty() == true);

          ret = heap.get_any(r_node);
          ASSERT_TRUE(ret == 0);
          ASSERT_TRUE(r_node == NULL);
    }
  }
#undef NNODES
#undef SEED
}

}
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

