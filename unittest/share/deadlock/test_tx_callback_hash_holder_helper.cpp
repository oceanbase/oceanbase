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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>
#define private public
#define protected public
#include "src/storage/memtable/mvcc/ob_tx_callback_hash_holder_helper.h"
#include "share/deadlock/ob_deadlock_key_wrapper.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/deadlock/test/test_key.h"
#include "src/storage/memtable/hash_holder/ob_row_holder_info.h"
#include "storage/memtable/hash_holder/ob_row_hash_holder_map.h"
#include "src/storage/memtable/mvcc/ob_mvcc.h"

namespace oceanbase {
using namespace common;
using namespace share::detector;
using namespace std;
using namespace memtable;
using namespace transaction;
namespace unittest {

class TestTxCallbackHashHolderHelper : public ::testing::Test {
public:
  TestTxCallbackHashHolderHelper() {}
  ~TestTxCallbackHashHolderHelper() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

struct TestCallback : public memtable::ObITransCallback {
  TestCallback(const TestCallback &rhs) : info_(rhs.info_) {}
  TestCallback(const RowHolderInfo &info) : info_(info) {}
  TestCallback &operator=(const TestCallback &) = default;
  virtual int get_holder_info(RowHolderInfo &holder_info) const override {
    int ret = OB_SUCCESS;
    holder_info = info_;
    return ret;
  }
  RowHolderInfo info_;
};

inline share::SCN mock_scn(int64_t val) { share::SCN scn; scn.convert_for_gts(val); return scn; }

TEST_F(TestTxCallbackHashHolderHelper, insert) {
  TestCallback head(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(5)));
  ObTxCallbackHashHolderList list;
  ASSERT_EQ(list.insert_callback(&head.get_hash_holder_linker(), false), OB_SUCCESS);
  ASSERT_EQ(list.size(), 1);
  TestCallback callback1(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(4)));
  ASSERT_EQ(list.insert_callback(&callback1.get_hash_holder_linker(), false), OB_SUCCESS);
  TestCallback callback2(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(7)));
  ASSERT_EQ(list.insert_callback(&callback2.get_hash_holder_linker(), false), OB_SUCCESS);
  TestCallback callback3(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(5)));
  ASSERT_EQ(list.insert_callback(&callback3.get_hash_holder_linker(), false), OB_SUCCESS);
  ASSERT_EQ(list.head(), &callback2.get_hash_holder_linker());
  ASSERT_EQ(list.head()->older_node_, &callback3.get_hash_holder_linker());
  ASSERT_EQ(list.head()->older_node_->older_node_, &head.get_hash_holder_linker());
  ASSERT_EQ(list.head()->older_node_->older_node_->older_node_, &callback1.get_hash_holder_linker());
}

TEST_F(TestTxCallbackHashHolderHelper, reverse_insert) {
  TestCallback head(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(5)));
  ObTxCallbackHashHolderList list;
  ASSERT_EQ(list.insert_callback(&head.get_hash_holder_linker(), false), OB_SUCCESS);
  ASSERT_EQ(list.size(), 1);
  TestCallback callback1(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(4)));
  ASSERT_EQ(list.insert_callback(&callback1.get_hash_holder_linker(), true), OB_SUCCESS);
  TestCallback callback2(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(7)));
  ASSERT_EQ(list.insert_callback(&callback2.get_hash_holder_linker(), true), OB_SUCCESS);
  TestCallback callback3(RowHolderInfo(ObTransID(1), ObTxSEQ::mk_v0(1), mock_scn(5)));
  ASSERT_EQ(list.insert_callback(&callback3.get_hash_holder_linker(), true), OB_SUCCESS);
  ASSERT_EQ(list.head(), &callback2.get_hash_holder_linker());
  ASSERT_EQ(list.head()->older_node_, &callback3.get_hash_holder_linker());
  ASSERT_EQ(list.head()->older_node_->older_node_, &head.get_hash_holder_linker());
  ASSERT_EQ(list.head()->older_node_->older_node_->older_node_, &callback1.get_hash_holder_linker());
}

TEST_F(TestTxCallbackHashHolderHelper, random_insert_and_erase) {
  memtable::RowHolderMapper mapper;
  ASSERT_EQ(OB_SUCCESS, mapper.init());
  vector<TestCallback> callback_vec;
  for (int64_t idx = 0; idx < 100; ++idx) {
    callback_vec.emplace_back(RowHolderInfo{ObTransID(idx), ObTxSEQ::mk_v0(idx + 1), mock_scn(idx)});
  }
  vector<int> idx_vec;
  for (int64_t idx = 0; idx < 100; ++idx) {
    idx_vec.push_back(idx);
  }
  std::random_device rd;  // 获取随机数种子
  std::mt19937 gen(rd());  // 使用梅森旋转算法生成随机数
  std::shuffle(idx_vec.begin(), idx_vec.end(), gen);// 随机打乱 vector
  for (int64_t idx = 0; idx < 100; ++idx) {
    mapper.insert_hash_holder(0, (&callback_vec[idx_vec[idx]])->get_hash_holder_linker(), (rand() % 2) == 0);
  }
  OCCAM_LOG(INFO, "1");
  struct JudgeOp {
    JudgeOp() : idx_(0), has_error_(false) {}
    int operator()(ObTxCallbackHashHolderLinker &node) {
      int ret = OB_SUCCESS;
      RowHolderInfo holder_info;
      if (OB_FAIL(node.get_holder_info(holder_info))) {
        OCCAM_LOG(ERROR, "get holder info failed");
      } else if (holder_info.scn_.get_val_for_gts() != idx_) {
        ret = OB_ERR_UNEXPECTED;
        has_error_ = true;
        OCCAM_LOG(ERROR, "get scn not equal to idx", K(holder_info), K_(idx));
      }
      idx_++;
      return ret;
    }
    int64_t idx_;
    bool has_error_;
  } judge_op;
  ObTxCallbackHashHolderList list;
  auto func = [&judge_op](const RowHolderMapper::KeyWrapper &key, ObTxCallbackHashHolderList &list) {
    list.for_each_node_(judge_op, ObTxCallbackHashHolderList::IterDirection::FROM_OLD_TO_NEW);
    return true;
  };
  ASSERT_EQ(OB_SUCCESS, mapper.holder_map_.operate(RowHolderMapper::KeyWrapper(0), func));
  ASSERT_EQ(judge_op.has_error_, false);
  ASSERT_EQ(mapper.list_count(), 1);
  ASSERT_EQ(mapper.node_count(), 100);

  std::shuffle(idx_vec.begin(), idx_vec.end(), gen);// 再随机打乱 vector
  for (int64_t idx = 0; idx < 100; ++idx) {
    mapper.erase_hash_holder_record(0, callback_vec[idx_vec[idx]].get_hash_holder_linker(), (rand() % 2) == 0);
  }
  ASSERT_EQ(mapper.list_count(), 0);
  ASSERT_EQ(mapper.node_count(), 0);
}

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tx_callback_hash_holder_helper.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_tx_callback_hash_holder_helper.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}