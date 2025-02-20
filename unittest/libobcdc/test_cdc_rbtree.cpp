/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "gtest/gtest.h"
#include "ob_cdc_lightly_sorted_list.h"
#include "ob_log_trans_log.h"

#define USING_LOG_PREFIX OBLOG

#define INIT_NODES \
  int ret = OB_SUCCESS; \
  Node* nodes = static_cast<Node*>(ob_malloc(node_cnt * sizeof(Node), "rbtree_test")); \
  for(int i = 0; i < node_cnt; i++) { \
    LSN lsn(i); \
    new(nodes+i) Node(lsn); \
    EXPECT_EQ(nodes[i].lsn_.val_, lsn.val_); \
  } \

#define FREE_NODES \
  for (int i = 0; i < node_cnt; i++) { \
    nodes[i].~Node(); \
  } \
  ob_free(nodes); \
  nodes = nullptr; \

namespace oceanbase
{
using namespace oceanbase::container;
using namespace oceanbase::palf;
namespace libobcdc
{
struct Node
{
  Node() : lsn_(), next_(nullptr) {}
  Node(LSN &lsn) : lsn_(lsn), next_(nullptr) {}
  ~Node() { lsn_.reset(); next_ = nullptr; }
  OB_INLINE int compare(const Node *other) const
  {
    return lsn_.val_ - other->lsn_.val_;
  }
  OB_INLINE bool operator==(const Node &other) {return compare(&other) == 0;}
  OB_INLINE bool operator<(const Node &other) {return compare(&other) < 0;}
  OB_INLINE void set_next(Node *node) {next_ = node;}
  OB_INLINE Node *get_next() const {return next_;}
  RBNODE(Node, rblink);
  LSN lsn_;
  Node* next_;
  TO_STRING_KV(K_(lsn));
};

typedef ObRbTree<Node, ObDummyCompHelper<Node>> tree_t;

static void init_nodes(int64_t node_cnt, Node *&nodes)
{
  nodes = static_cast<Node*>(ob_malloc(node_cnt * sizeof(Node), "rbtree_test"));
  for(int i = 0; i < node_cnt; i++) {
    LSN lsn(i);
    new(nodes+i) Node(lsn);
    EXPECT_EQ(nodes[i].lsn_.val_, lsn.val_);
  }
}

static void free_nodes(int64_t node_cnt, Node*& nodes)
{
  for (int i = 0; i < node_cnt; i++) {
    nodes[i].~Node();
  }
  ob_free(nodes);
  nodes = nullptr;
}

static void build_tree(tree_t &tree, Node* nodes, int64_t node_cnt, int64_t reverse_cnt = 0, bool total_reverse = false)
{
  tree.init_tree();
  if (total_reverse) {
    for (int i = node_cnt - 1; i >=0; i--) {
      EXPECT_EQ(OB_SUCCESS, tree.insert(&nodes[i]));
    }
  } else {
    for (int i = node_cnt - reverse_cnt; i < node_cnt; i++) {
      EXPECT_EQ(OB_SUCCESS, tree.insert(&nodes[i]));
    }
    for (int i = 0; i < node_cnt-reverse_cnt; i++) {
      EXPECT_EQ(OB_SUCCESS, tree.insert(&nodes[i]));
    }
  }
}

static void build_list(SortedLightyList<Node> &list, Node* nodes, int64_t node_cnt, int64_t reverse_cnt = 0, bool total_reverse = false)
{
  list.reset();
  if (total_reverse) {
    for (int i = node_cnt - 1; i >=0; i--) {
      EXPECT_EQ(OB_SUCCESS, list.push(&nodes[i]));
    }
  } else {
    for (int i = node_cnt - reverse_cnt; i < node_cnt; i++) {
      EXPECT_EQ(OB_SUCCESS, list.push(&nodes[i]));
    }
    for (int i = 0; i < node_cnt-reverse_cnt; i++) {
      EXPECT_EQ(OB_SUCCESS, list.push(&nodes[i]));
    }
  }
}

static void iter_tree(tree_t &tree, int64_t node_cnt)
{
  int node_cntt = 0;
  Node* node = tree.get_first();
  while (NULL != node) {
    EXPECT_EQ(node->lsn_.val_, node_cntt);
    node_cntt++;
    Node *next = NULL;
    tree.get_next(node, next);
    node = next;
  }
  EXPECT_EQ(node_cntt, node_cnt);
}

static void iter_list(SortedLightyList<Node> &list, int64_t node_cnt)
{
  int node_cntt = 0;
  Node *node = list.get_first_node();
  while (NULL != node) {
    EXPECT_EQ(node->lsn_.val_, node_cntt);
    node_cntt++;
    Node *next = node->get_next();
    node = next;
  }
  EXPECT_EQ(node_cntt, node_cnt);
}

TEST(TESTCDCRbTree, init_and_free)
{
  LOG_INFO("========== test begin ==========");
  int node_cnt = 500000;
  INIT_NODES;
  FREE_NODES;
}

TEST(TESTCDCRbTree, sequential_verify)
{
  int node_cnt = 500000;
  INIT_NODES;
  int64_t start_ts = get_timestamp();
  tree_t tree;
  build_tree(tree, nodes, node_cnt);
  int64_t built_ts = get_timestamp();
  iter_tree(tree, node_cnt);
  int64_t verify_ts = get_timestamp();

  LOG_INFO("sequential_verify", "build_cost", built_ts - start_ts, "verify_cost", verify_ts - built_ts);

  FREE_NODES;
};

TEST(TESTCDCRbTree, part_reverse_verify)
{
  int node_cnt = 500000;
  int reverse_cnt = 1000;
  INIT_NODES;

  int64_t start_ts = get_timestamp();
  tree_t tree;
  build_tree(tree, nodes, node_cnt, reverse_cnt);
  int64_t built_ts = get_timestamp();
  iter_tree(tree, node_cnt);
  int64_t verify_ts = get_timestamp();

  LOG_INFO("part_reverse_verify", "build_cost", built_ts - start_ts, "verify_cost", verify_ts - built_ts);

  FREE_NODES;
};

TEST(TESTCDCRbTree, part_reverse_verify_5w)
{
  int node_cnt = 50000;
  int reverse_cnt = 1000;
  INIT_NODES;

  int64_t start_ts = get_timestamp();
  tree_t tree;
  build_tree(tree, nodes, node_cnt, reverse_cnt);
  int64_t built_ts = get_timestamp();
  iter_tree(tree, node_cnt);
  int64_t verify_ts = get_timestamp();

  LOG_INFO("part_reverse_verify_5w", "build_cost", built_ts - start_ts, "verify_cost", verify_ts - built_ts);

  FREE_NODES;
};

TEST(TESTCDCRbTree, total_reverse_verify)
{
  int node_cnt = 500000;
  INIT_NODES;

  int64_t start_ts = get_timestamp();
  tree_t tree;
  build_tree(tree, nodes, node_cnt, 0, true);
  int64_t built_ts = get_timestamp();
  iter_tree(tree, node_cnt);
  int64_t verify_ts = get_timestamp();

  LOG_INFO("total_reverse_verify", "build_cost", built_ts - start_ts, "verify_cost", verify_ts - built_ts);

  FREE_NODES;

};

TEST(TESTCDCLightyList, sequential_verify)
{
  int node_cnt = 500000;
  INIT_NODES;
  int64_t start_ts = get_timestamp();
  SortedLightyList<Node> list(true);
  build_list(list, nodes, node_cnt);
  int64_t built_ts = get_timestamp();
  iter_list(list, node_cnt);
  int64_t verify_ts = get_timestamp();
  LOG_INFO("sequential_verify", "build_cost", built_ts - start_ts, "verify_cost", verify_ts - built_ts);

  list.reset();
  FREE_NODES;
}

TEST(TESTCDCLightyList, part_reverse_verify)
{
  int node_cnt = 5000;
  int reverse_cnt = 1000;
  INIT_NODES;
  int64_t start_ts = get_timestamp();
  SortedLightyList<Node> list(true);
  build_list(list, nodes, node_cnt, reverse_cnt);
  int64_t built_ts = get_timestamp();
  iter_list(list, node_cnt);
  int64_t verify_ts = get_timestamp();
  LOG_INFO("part_reverse_verify", "build_cost", built_ts - start_ts, "verify_cost", verify_ts - built_ts);

  list.reset();
  FREE_NODES;
}

TEST(TESTCDCRbTree, detect_balance_node_count)
{
  int64_t node_cnt = 4;

  while(node_cnt++ < 50) {
    Node *nodes = nullptr;
    tree_t tree;
    SortedLightyList<Node> list(true);

    init_nodes(node_cnt, nodes);
    int64_t start_ts1 = common::ObTimeUtility::current_time_ns();
    build_tree(tree, nodes, node_cnt, 1);
    int64_t end_ts1 = common::ObTimeUtility::current_time_ns();
    free_nodes(node_cnt, nodes);
    init_nodes(node_cnt, nodes);
    int64_t start_ts2 = common::ObTimeUtility::current_time_ns();
    build_list(list, nodes, node_cnt, 1);
    int64_t end_ts2 = common::ObTimeUtility::current_time_ns();
    list.reset();
    free_nodes(node_cnt, nodes);
    int64_t tree_build_time = (end_ts1 - start_ts1);
    int64_t list_build_time = (end_ts2 - start_ts2);
    bool is_tree_better = (tree_build_time < list_build_time);

    if (is_tree_better) {
      LOG_INFO("tree_build_time less than list_build_time", K(node_cnt), K(tree_build_time), K(list_build_time));
    } else {
      LOG_INFO("tree_build_time greater than list_build_time", K(node_cnt), K(tree_build_time), K(list_build_time));
    }
  }
};

TEST(TESTCDCRbTree, list_to_tree)
{
  int64_t node_cnt = 1000000;
  Node* nodes = nullptr;
  init_nodes(node_cnt, nodes);
  tree_t tree;
  SortedLightyList<Node> list(true);
  build_list(list, nodes, node_cnt);

  int64_t start_ts = get_timestamp();
  Node *node = list.get_first_node();
  while (NULL != node) {
    tree.insert(node);
    Node *next = node->get_next();
    node = next;
  }
  int64_t end_ts = get_timestamp();
  list.reset();
  LOG_INFO("list_to_tree", K(node_cnt), "cost_us", end_ts-start_ts);
  int64_t start_ts1 = get_timestamp();
  node = tree.get_first();
  while (NULL != node) {
    EXPECT_EQ(OB_SUCCESS, list.push(node));
    Node *next = NULL;
    tree.get_next(node, next);
    node = next;
  }
  int64_t end_ts1 = get_timestamp();
  LOG_INFO("tree_to_list", K(node_cnt), "cost_us", end_ts1-start_ts1);
};

}
}

int main(int argc, char **argv) {
  // system("rm -rf cdc_sorted_list_test.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("cdc_sorted_list_test.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
