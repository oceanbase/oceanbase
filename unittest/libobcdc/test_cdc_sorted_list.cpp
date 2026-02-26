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
#include "ob_log_utils.h"
#include "ob_cdc_sorted_treeify_list.h"
#include "ob_cdc_sorted_list_iterator.h"

#define USING_LOG_PREFIX OBLOG

#define ASSERT_SUCC(statement) \
  ASSERT_EQ(OB_SUCCESS, statement);

namespace oceanbase
{
using namespace oceanbase::container;
using namespace oceanbase::palf;
namespace libobcdc
{

int build_lsn_list(ObIArray<LSN> &lsn_arr, const int64_t lsn_cnt)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < lsn_cnt; ++i) {
    LSN lsn(i);
    if (OB_FAIL(lsn_arr.push_back(lsn))) {
      LOG_ERROR("push_back lsn into array failed", KR(ret));
    }
  }
  return ret;
}

int build_list(
    const ObIArray<LSN> &lsn_array,
    const int64_t node_cnt,
    SortedList<LSN, LSNComparator> &list,
    const int64_t reverse_cnt = 0,
    const bool total_reverse = false)
{
  int ret = OB_SUCCESS;
  ob_assert(node_cnt <= lsn_array.count());
  ob_assert(reverse_cnt <= node_cnt && reverse_cnt >= 0);

  if (total_reverse) {
    for (int i = node_cnt-1; OB_SUCC(ret) && i >=0 ; --i) {
      const LSN &lsn = lsn_array.at(i);
      if (OB_FAIL(list.push(const_cast<LSN&>(lsn)))) {
        LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
      }
    }
  } else {
    for (int i = node_cnt - reverse_cnt; OB_SUCC(ret) && i < node_cnt; i++) {
      const LSN &lsn = lsn_array.at(i);
      if (OB_FAIL(list.push(const_cast<LSN&>(lsn)))) {
        LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < node_cnt - reverse_cnt; i++) {
      const LSN &lsn = lsn_array.at(i);
      if (OB_FAIL(list.push(const_cast<LSN&>(lsn)))) {
        LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
      }
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(node_cnt != list.count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("list node count not match", KR(ret), K(node_cnt), K(list));
  }

  LOG_INFO("build_list finish", KR(ret), K(list));

  return ret;
}

int iter_list_and_verify(const ObIArray<LSN> &lsn_array, const int64_t node_cnt, const SortedList<LSN, LSNComparator> &list)
{
  int ret = OB_SUCCESS;
  SortedList<LSN, LSNComparator>::Iterator iter = list.begin();
  int64_t idx = 0;

  while(OB_SUCC(ret) && iter != list.end()) {
    LSN &lsn = *iter;
    const LSN &arr = lsn_array.at(idx);
    if (lsn != lsn_array.at(idx)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("value check failed", KR(ret), K(idx), K(*iter), K(lsn), K(arr), K(list));
    } else {
      iter++;
      idx++;
    }
  }

  if (OB_SUCC(ret)) {
    if (idx != node_cnt || idx != list.count()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("iterate count not match", KR(ret), K(idx), K(node_cnt), K(list));
    }
  }

  return ret;
}

int pop_and_check_val(const ObIArray<LSN> &lsn_array, const int64_t node_cnt, SortedList<LSN, LSNComparator> &list, const int64_t pop = -1)
{
  int ret = OB_SUCCESS;
  int64_t pop_cnt = pop <= 0 ? node_cnt : pop;
  const int64_t remain_cnt = node_cnt - pop_cnt;
  LSN *val_ptr = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < pop_cnt; i++) {
    if (OB_FAIL(list.pop(val_ptr))) {
      LOG_ERROR("SortedTreeifyList pop failed", KR(ret), K(i), K(list));
    } else if (OB_ISNULL(val_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("val poped from list is null", KR(ret), K(i), K(list));
    } else if (OB_UNLIKELY(lsn_array.at(i) != *val_ptr)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("value check failed", KR(ret), K(ret), K(i), KPC(val_ptr), K(list));
    } else {
      val_ptr = nullptr;
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!list.empty())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("list not empry after pop", K(list));
  }

  return ret;
}

TEST(SortedLinkedList, init_list_and_free)
{
  const int64_t node_cnt = 50000;
  const int64_t local_cnt = node_cnt;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedLinkedList<LSN, LSNComparator> linked_list(allocator);
  LOG_INFO("========== test SortedLinkedList begin ==========");
  ObTimeGuard time_guard("init sorted linked_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, linked_list));
  time_guard.click("build_linked_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, linked_list));
  time_guard.click("iter_linked_list");
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, linked_list));
  time_guard.click("pop_linked_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, linked_list));
  time_guard.click("rebuild_linked_list");
  ASSERT_EQ(OB_ENTRY_EXIST, linked_list.push(lsn_array.at(0)));
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, linked_list));
  time_guard.click("pop_linked_list after rebuild");
  linked_list.reset();
  linked_list.reset();
  ASSERT_TRUE(linked_list.empty());
  ASSERT_TRUE(linked_list.is_list_empty());
  LOG_INFO("init_list_and_free finish", K(node_cnt), K(time_guard));
}

TEST(SortedTreeifyList, init_list_and_free)
{
  const int64_t node_cnt = 5;
  const int64_t local_cnt = node_cnt;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedTreeifyList<LSN, LSNComparator> treeify_list(allocator);
  LOG_INFO("========== test SortedLinkedList begin ==========");
  ObTimeGuard time_guard("init sorted linked_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  time_guard.click("build_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  time_guard.click("iter_list");
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, treeify_list));
  time_guard.click("pop_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  time_guard.click("rebuild_list");
  ASSERT_EQ(OB_ENTRY_EXIST, treeify_list.push(lsn_array.at(0)));
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, treeify_list));
  time_guard.click("pop_list after rebuild");
  treeify_list.reset();
  treeify_list.reset();
  ASSERT_TRUE(treeify_list.empty());
  LOG_INFO("init_list_and_free finish", K(node_cnt), K(time_guard));
}

TEST(SortedTreeifyList, init_treeify_list_and_free_manual_treeify_mode)
{
  const int64_t node_cnt = 3;
  const int64_t local_cnt = node_cnt;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedTreeifyList<LSN, LSNComparator> treeify_list(allocator, false/* manual_treeify_mode */);
  LOG_INFO("========== test SortedTreeifyList manual_treeify_mode begin ==========");
  ObTimeGuard time_guard("init sorted treeify_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  time_guard.click("build_treeify_list manual_treeify_mode");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  ASSERT_EQ(OB_ENTRY_EXIST, treeify_list.push(lsn_array.at(0)));
  time_guard.click("iter_treeify_list manual_treeify_mode");
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, treeify_list));
  time_guard.click("pop_treeify_list manual_treeify_mode");
  treeify_list.reset();
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  time_guard.click("rebuild_treeify_list manual_treeify_mode");
  ASSERT_SUCC(treeify_list.treeify());
  time_guard.click("treeify list manual_treeify_mode");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  time_guard.click("iter_list after treeify manual_treeify_mode");
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, treeify_list));
  time_guard.click("pop_list after treeify");
  treeify_list.reset();
  treeify_list.reset();
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  ASSERT_SUCC(treeify_list.treeify());
  ASSERT_EQ(OB_ENTRY_EXIST, treeify_list.push(lsn_array.at(0)));
  ASSERT_SUCC(treeify_list.untreeify());
  ASSERT_EQ(OB_ENTRY_EXIST, treeify_list.push(lsn_array.at(0)));
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, treeify_list));
  treeify_list.reset();
  treeify_list.reset();
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  ASSERT_SUCC(treeify_list.treeify());
  treeify_list.reset();
  treeify_list.reset();
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  ASSERT_SUCC(treeify_list.treeify());
  ASSERT_SUCC(treeify_list.untreeify());
  ASSERT_SUCC(treeify_list.treeify());
  ASSERT_SUCC(treeify_list.untreeify());
  ASSERT_SUCC(treeify_list.treeify());
  ASSERT_EQ(OB_ENTRY_EXIST, treeify_list.push(lsn_array.at(0)));
  treeify_list.reset();
  treeify_list.reset();
  ASSERT_TRUE(treeify_list.empty());
  LOG_INFO("init_treeify_list_and_free_manual_treeify_mode finish", K(node_cnt), K(time_guard));
}


TEST(SortedTreeifyList, manual_treeify)
{
  int ret = OB_SUCCESS;
  const int64_t node_cnt = 500000;
  const int64_t local_cnt = node_cnt;
  const int64_t reverse_cnt = 1000;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedTreeifyList<LSN, LSNComparator> treeify_list(allocator, false/* manual_treeify_mode */);
  LOG_INFO("========== test SortedTreeifyList manual_treeify_mode begin ==========", "sizeof", sizeof(SortedTreeifyList<LSN, LSNComparator>::NodeType));
  ObTimeGuard time_guard("init sorted treeify_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  time_guard.click("build_treeify_list manual_treeify_mode");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  time_guard.click("iter_treeify_list manual_treeify_mode");
  treeify_list.reset();
  time_guard.click("reset_treeify_list manual_treeify_mode");

  // test manually

  for (int i = node_cnt - reverse_cnt; OB_SUCC(ret) && i < node_cnt; i++) {
    const LSN &lsn = lsn_array.at(i);
    if (OB_FAIL(treeify_list.push(const_cast<LSN&>(lsn)))) {
      LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
    }
  }
  time_guard.click("push_reverse_nodes");
  treeify_list.treeify();
  time_guard.click("treeify");

  for (int i = 0; OB_SUCC(ret) && i < node_cnt - reverse_cnt; i++) {
    const LSN &lsn = lsn_array.at(i);
    if (OB_FAIL(treeify_list.push(const_cast<LSN&>(lsn)))) {
      LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
    }
  }
  time_guard.click("push_remain_nodes");
  treeify_list.untreeify();
  time_guard.click("untreeify");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  time_guard.click("iter_list finish");

  LOG_INFO("manual_treeify finish", K(node_cnt), K(reverse_cnt), K(allocator), K(time_guard));
}

TEST(SortedTreeifyList, manual_treeify_from_middle)
{
  int ret = OB_SUCCESS;
  const int64_t node_cnt = 500000;
  const int64_t local_cnt = node_cnt;
  const int64_t start_idx = node_cnt / 2;
  const int64_t reverse_cnt = 1000;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedTreeifyList<LSN, LSNComparator> treeify_list(allocator, false/* manual_treeify_mode */);
  LOG_INFO("========== test SortedTreeifyList manual_treeify_mode from middle begin ==========", "sizeof", sizeof(SortedTreeifyList<LSN, LSNComparator>::NodeType));
  ObTimeGuard time_guard("init sorted treeify_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  time_guard.click("build_treeify_list manual_treeify_mode");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  time_guard.click("iter_treeify_list manual_treeify_mode");
  treeify_list.reset();
  time_guard.click("reset_treeify_list manual_treeify_mode");

  // test manually

  for (int i = start_idx; OB_SUCC(ret) && i < start_idx + reverse_cnt; i++) {
    const LSN &lsn = lsn_array.at(i);
    if (OB_FAIL(treeify_list.push(const_cast<LSN&>(lsn)))) {
      LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
    }
  }
  ASSERT_TRUE((lsn_array.at(start_idx) == *(treeify_list.top())));
  time_guard.click("push_reverse_nodes");
  treeify_list.treeify();
  ASSERT_TRUE((lsn_array.at(start_idx) == *(treeify_list.top())));
  time_guard.click("treeify");

  for (int i = 0; OB_SUCC(ret) && i < start_idx; i++) {
    const LSN &lsn = lsn_array.at(i);
    if (OB_FAIL(treeify_list.push(const_cast<LSN&>(lsn)))) {
      LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
    }
  }
  time_guard.click("push_front_nodes");
  treeify_list.untreeify();
  time_guard.click("untreeify");
  for (int i = start_idx + reverse_cnt; OB_SUCC(ret) && i < node_cnt; i++) {
    const LSN &lsn = lsn_array.at(i);
    if (OB_FAIL(treeify_list.push(const_cast<LSN&>(lsn)))) {
      LOG_ERROR("SortedTreeifyList push failed", KR(ret), K(lsn), K(node_cnt));
    }
  }
  time_guard.click("push_remain_nodes");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  time_guard.click("iter_list finish");

  LOG_INFO("manual_treeify finish", K(node_cnt), K(reverse_cnt), K(allocator), K(time_guard));
}

TEST(SortedTreeifyList, init_treeify_list_and_free_auto_treeify_mode)
{
  const int64_t node_cnt = 5000000;
  const int64_t local_cnt = 1000000;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedTreeifyList<LSN, LSNComparator> treeify_list(allocator, true/*auto_treeify_mode*/);
  LOG_INFO("========== test SortedTreeifyList auto_treeify_mode begin ==========");
  ObTimeGuard time_guard("init sorted treeify_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list));
  time_guard.click("build_treeify_list auto_treeify_mode");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list));
  time_guard.click("iter_treeify_list auto_treeify_mode");
  ASSERT_SUCC(pop_and_check_val(lsn_array, node_cnt, treeify_list));
  time_guard.click("pop_treeify_list auto_treeify_mode");
  treeify_list.reset();
  treeify_list.reset();
  ASSERT_TRUE(treeify_list.empty());
  LOG_INFO("init_treeify_list_and_free_auto_treeify_mode finish", K(node_cnt), K(time_guard));
}

TEST(SortedLinkedList, sequential_verify)
{
  const int64_t node_cnt = 50000;
  const int64_t local_cnt = node_cnt;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedLinkedList<LSN, LSNComparator> linked_list(allocator);
  SortedTreeifyList<LSN, LSNComparator> treeify_list_1(allocator, false/*manual_treeify_mode*/);
  SortedTreeifyList<LSN, LSNComparator> treeify_list_2(allocator, true/*auto_treeify_mode*/);
  LOG_INFO("========== test SortedLinkedList begin sequential_verify ==========");
  ObTimeGuard time_guard("sequential_verify linked_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, linked_list));
  time_guard.click("build_linked_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list_1));
  time_guard.click("build_manual_treeify_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list_2));
  time_guard.click("build_auto_treeify_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, linked_list));
  time_guard.click("iter_linked_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list_1));
  time_guard.click("iter_manual_treeify_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list_2));
  time_guard.click("iter_auto_treeify_list");
  linked_list.reset();
  time_guard.click("reset_linked_list");
  treeify_list_1.reset();
  time_guard.click("reset_manual_treeify_list");
  treeify_list_2.reset();
  time_guard.click("reset_auto_treeify_list");
  LOG_INFO("init_treeify_list_and_free_auto_treeify_mode sequential_verify finish", K(node_cnt), K(time_guard));
}

TEST(SortedLinkedList, part_reverse_verify)
{
  const int64_t node_cnt = 5000;
  const int64_t local_cnt = node_cnt;
  const int64_t reverse_cnt = 100;
  ObArenaAllocator allocator;
  ObSEArray<LSN, local_cnt> lsn_array;
  SortedLinkedList<LSN, LSNComparator> linked_list(allocator);
  SortedTreeifyList<LSN, LSNComparator> treeify_list_1(allocator, false/*manual_treeify_mode*/);
  SortedTreeifyList<LSN, LSNComparator> treeify_list_2(allocator, true/*auto_treeify_mode*/);
  LOG_INFO("========== test SortedLinkedList part_reverse_verify begin ==========");
  ObTimeGuard time_guard("sequential_verify linked_list");
  ASSERT_SUCC(build_lsn_list(lsn_array, node_cnt));
  time_guard.click("build_lsn_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, linked_list, reverse_cnt));
  time_guard.click("build_linked_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list_1, reverse_cnt));
  time_guard.click("build_manual_treeify_list");
  ASSERT_SUCC(build_list(lsn_array, node_cnt, treeify_list_2, reverse_cnt));
  time_guard.click("build_auto_treeify_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, linked_list));
  time_guard.click("iter_linked_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list_1));
  time_guard.click("iter_manual_treeify_list");
  ASSERT_SUCC(iter_list_and_verify(lsn_array, node_cnt, treeify_list_2));
  time_guard.click("iter_auto_treeify_list");
  linked_list.reset();
  time_guard.click("reset_linked_list");
  treeify_list_1.reset();
  time_guard.click("reset_manual_treeify_list");
  treeify_list_2.reset();
  time_guard.click("reset_auto_treeify_list");
  LOG_INFO("init_treeify_list_and_free_auto_treeify_mode part_reverse_verify finish", K(node_cnt), K(reverse_cnt), K(time_guard));
}

}
}

int main(int argc, char **argv) {
  system("rm -rf cdc_sorted_treeify_list.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("cdc_sorted_treeify_list.log", true);
  logger.set_log_level("DEBUG");
  oceanbase::lib::reload_trace_log_config(true);
  oceanbase::lib::reload_diagnose_info_config(true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}