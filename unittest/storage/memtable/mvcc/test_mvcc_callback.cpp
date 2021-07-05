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

#define private public

#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/memtable/ob_memtable_context.h"

namespace oceanbase {
namespace unittest {
using namespace oceanbase::common;
using namespace oceanbase::memtable;

class ObMockTransCallback : public ObITransCallback {
public:
  ObMockTransCallback(ObMemtable* mt) : fake_mt_(mt)
  {}

  int callback(const int type, const bool for_replay, const bool need_lock_for_write, ObMemtable* memtable) override
  {
    int ret = OB_SUCCESS;
    UNUSED(for_replay);
    UNUSED(need_lock_for_write);

    if (TCB_REMOVE_CALLBACK == type) {
      if (memtable == fake_mt_) {
        del();
      } else {
        ret = OB_ITEM_NOT_MATCH;
      }
    } else {
      abort();
    }

    return ret;
  }

  int del() override
  {
    int ret = OB_SUCCESS;

    ObITransCallback* cur = this;

    if (NULL == cur) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      ObITransCallback* prev = cur->prev_;
      ObITransCallback* next = cur->next_;
      if (NULL == prev || NULL == next) {
        ret = common::OB_INVALID_ARGUMENT;
      } else {
        prev->next_ = next;
        next->prev_ = prev;
      }
    }

    return ret;
  }

  ObMemtable* fake_mt_;
};

TEST(TestObMvccCallback, remove_callback_test)
{
  uint64_t addr1 = 3;
  ObMockTransCallback cb1((ObMemtable*)(addr1));
  uint64_t addr2 = 1;
  ObMockTransCallback cb2((ObMemtable*)(addr2));
  uint64_t addr3 = 2;
  int64_t cnt = 0;

  ObMemtableCtx mt_ctx;
  ObTransCallbackMgr mgr(mt_ctx);
  mgr.append(&cb1);
  mgr.append(&cb2);

  ASSERT_EQ(2, mgr.count());
  mgr.remove_callback_for_uncommited_txn((ObMemtable*)(addr1), cnt);
  ASSERT_EQ(1, mgr.count());
  mgr.remove_callback_for_uncommited_txn((ObMemtable*)(addr3), cnt);
  ASSERT_EQ(1, mgr.count());
  mgr.remove_callback_for_uncommited_txn((ObMemtable*)(addr2), cnt);
  ASSERT_EQ(0, mgr.count());
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_mvcc_trans_ctx.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
