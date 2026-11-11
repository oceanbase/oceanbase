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
#include <memory>
#include <string>
#include <vector>
#define private public
#define protected public
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_trans_part_ctx.h"
#undef private
#undef protected
#include "storage/tx/ob_trans_functor.h"

namespace oceanbase
{
using namespace common;
using namespace memtable;
using namespace transaction;
namespace unittest
{

class TestObTxMisc : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObTxMisc, multiple_checksum_collapse_for_commit_log)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // only one checksum
  {
    uint64_t checksum0 = 12323221;
    ObArrayHelper<uint64_t> arr(1, &checksum0, 1);
    uint8_t signature0 = 0;
    ObArrayHelper<uint8_t> sig(1, &signature0, 1);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 12323221);
    EXPECT_EQ(0, sig.count());
  }
  // multiple, but only one is valid
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 12323221);
    EXPECT_EQ(0, sig.count());
    // valid is in middle
    checksum[0] = 1;
    checksum[13] = 34443;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 34443);
    EXPECT_EQ(0, sig.count());
  }
  // multiple, multiple valid: 1, 13
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1;
    checksum[13] = 34443;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_GT(result, 1);
    EXPECT_NE(result, 12323221);
    EXPECT_NE(result, 34443);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(12323221 & 0xFF, sig.at(0));
    EXPECT_EQ(34443 & 0xFF, sig.at(13));
  }
  // multiple, multiple valid, 18,21
  {
    uint64_t checksum[64];
    for (int i = 0; i < 64; i++) checksum[i] = 1;
    checksum[18] = 34443;
    checksum[21] = 34444;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_GT(result, 1);
    EXPECT_NE(result, 34443);
    EXPECT_NE(result, 34444);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(34443 & 0xFF, sig.at(18));
    EXPECT_EQ(34444 & 0xFF, sig.at(21));
  }
  // multiple, all is valid
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1 + i;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_NE(result, 12323221);
    EXPECT_GT(result, 1);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(12323221 & 0xFF, sig.at(0));
  }
}

TEST_F(TestObTxMisc, TxDesc_add_mofied_tables)
{
  ObTxDesc txdesc;
  {
    uint64_t tids[] = {1,2,3,4,5,6,7};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 9; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9, 10};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9, 10, 1, 5, 7};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {11,12,13,14,15,16,17,18,19,20};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 20; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {20, 19, 17, 14, 18, 20, 1, 7, 5, 4, 9};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 20; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
}

class LockStatCallback : public ObITransCallback
{
public:
  LockStatCallback(const uint64_t tablet_id,
                   const uint64_t hash_val,
                   const ObTxSEQ seq,
                   const char *text,
                   const int64_t delay_us = 0)
    : seq_(seq), delay_us_(delay_us), merge_count_(0), merge_ret_(OB_SUCCESS),
      key_array_tenant_id_(0), key_array_capacity_(0)
  {
    key_.init(hash_val);
    key_.set_tablet_id(ObTabletID(tablet_id));
    snprintf(key_.get_buf(), ObMemtableKeyInfo::MEMTABLE_KEY_INFO_BUF_SIZE, "%s", text);
  }

  ObTxSEQ get_seq_no() const override { return seq_; }
  bool is_need_free() const override { return false; }
  int merge_memtable_key(ObMemtableKeyArray &keys) override
  {
    int ret = merge_ret_;

    ++merge_count_;
    if (OB_SUCC(ret)) {
      ret = keys.push_back(key_);
      key_array_tenant_id_ = keys.block_allocator_.get_attr().tenant_id_;
      key_array_capacity_ = keys.get_capacity();
    }

    if (OB_SUCC(ret) && delay_us_ > 0) {
      ob_usleep(delay_us_);
    }

    return ret;
  }

  ObMemtableKeyInfo key_;
  ObTxSEQ seq_;
  int64_t delay_us_;
  int64_t merge_count_;
  int merge_ret_;
  uint64_t key_array_tenant_id_;
  int64_t key_array_capacity_;
};

class TestTxLockStat : public ::testing::Test
{
public:
  TestTxLockStat()
    : tx_ctx_(), mem_ctx_(tx_ctx_.mt_ctx_), callback_mgr_(mem_ctx_.trans_mgr_), callbacks_() {}

  void SetUp() override
  {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ASSERT_EQ(OB_SUCCESS, mem_ctx_.ctx_cb_allocator_.init(1001));
    tx_ctx_.tenant_id_ = 1001;
    tx_ctx_.role_state_ = TxCtxRoleState::LEADER;
    tx_ctx_.is_inited_ = true;
  }

  void TearDown() override
  {
    tx_ctx_.is_inited_ = false;
    EXPECT_EQ(OB_SUCCESS, callback_mgr_.callback_list_.tx_abort());
    if (nullptr != callback_mgr_.callback_lists_) {
      for (int64_t i = 0; i < MAX_CALLBACK_LIST_COUNT - 1; ++i) {
        EXPECT_EQ(OB_SUCCESS, callback_mgr_.callback_lists_[i].tx_abort());
      }
    }

    callbacks_.clear();
    callback_mgr_.reset();
  }

  LockStatCallback *append(const uint64_t tablet_id,
                           const uint64_t hash_val,
                           const int64_t branch,
                           const char *text,
                           const int64_t delay_us = 0)
  {
    const ObTxSEQ seq(static_cast<int64_t>(callbacks_.size()) + 1, branch);
    std::unique_ptr<LockStatCallback> cb(new LockStatCallback(tablet_id, hash_val, seq, text, delay_us));
    LockStatCallback *result = cb.get();

    EXPECT_EQ(OB_SUCCESS, callback_mgr_.append(result));
    callbacks_.push_back(std::move(cb));
    return result;
  }

  ObPartTransCtx tx_ctx_;
  ObMemtableCtx &mem_ctx_;
  ObTransCallbackMgr &callback_mgr_;
  std::vector<std::unique_ptr<LockStatCallback>> callbacks_;
};

TEST_F(TestTxLockStat, visits_all_callback_lists)
{
  ObMemtableKeyArray keys;

  append(1001, 7, 0, "first");
  append(1001, 8, 1, "second");
  append(1002, 9, MAX_CALLBACK_LIST_COUNT - 1, "last list");
  ASSERT_EQ(OB_SUCCESS, tx_ctx_.get_memtable_key_arr(keys));
  ASSERT_EQ(3, keys.count());
  EXPECT_STREQ("first", keys.at(0).read_buf());
  EXPECT_STREQ("second", keys.at(1).read_buf());
  EXPECT_STREQ("last list", keys.at(2).read_buf());
}

TEST_F(TestTxLockStat, expired_deadline_skips_all_lists)
{
  ObMemtableKeyArray keys;

  append(1001, 7, 0, "first");
  append(1001, 8, 1, "second");
  ASSERT_EQ(OB_SUCCESS, callback_mgr_.get_memtable_key_arr(keys, ObTimeUtility::current_time() - 1));
  EXPECT_EQ(0, keys.count());
  EXPECT_EQ(0, callbacks_.at(0)->merge_count_);
  EXPECT_EQ(0, callbacks_.at(1)->merge_count_);
}

TEST_F(TestTxLockStat, context_uses_shared_300ms_deadline)
{
  ObMemtableKeyArray keys;

  append(1001, 7, 0, "first", 200 * 1000);
  append(1001, 8, 1, "second", 200 * 1000);
  append(1001, 9, 2, "third", 200 * 1000);
  ASSERT_EQ(OB_SUCCESS, tx_ctx_.get_memtable_key_arr(keys));
  // The next list must use the remaining budget, even if one callback crosses the deadline.
  EXPECT_GE(keys.count(), 1);
  EXPECT_LT(keys.count(), 3);
  EXPECT_EQ(0, callbacks_.at(2)->merge_count_);
}

TEST_F(TestTxLockStat, deadline_is_checked_inside_a_list)
{
  ObMemtableKeyArray keys;

  append(1001, 7, 0, "first", 200 * 1000);
  append(1001, 8, 0, "second", 200 * 1000);
  append(1001, 9, 0, "third", 200 * 1000);
  ASSERT_EQ(OB_SUCCESS, tx_ctx_.get_memtable_key_arr(keys));
  EXPECT_GE(keys.count(), 1);
  EXPECT_LT(keys.count(), 3);
  EXPECT_EQ(0, callbacks_.at(2)->merge_count_);
  EXPECT_EQ(3, callback_mgr_.callback_list_.get_length());
}

TEST_F(TestTxLockStat, sql_deadline_limits_collection)
{
  ObMemtableKeyArray keys;
  const int64_t previous_timeout = THIS_WORKER.get_timeout_ts();

  append(1001, 7, 0, "first");
  append(1001, 8, 1, "second");
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() - 1);
  const int ret = tx_ctx_.get_memtable_key_arr(keys);

  THIS_WORKER.set_timeout_ts(previous_timeout);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, keys.count());
  EXPECT_EQ(0, callbacks_.at(0)->merge_count_);
  EXPECT_EQ(0, callbacks_.at(1)->merge_count_);
}

TEST_F(TestTxLockStat, sql_deadline_expires_during_collection)
{
  ObMemtableKeyArray keys;
  const int64_t previous_timeout = THIS_WORKER.get_timeout_ts();

  // A 150ms callback crosses the 100ms SQL deadline.
  append(1001, 7, 0, "first", 150 * 1000);
  append(1001, 8, 0, "same list");
  append(1001, 9, 1, "next list");
  const int64_t sql_deadline = ObTimeUtility::current_time() + 100 * 1000;

  THIS_WORKER.set_timeout_ts(sql_deadline);
  const int64_t begin_ts = ObTimeUtility::current_time();
  const int ret = tx_ctx_.get_memtable_key_arr(keys);
  const int64_t finish_ts = ObTimeUtility::current_time();

  THIS_WORKER.set_timeout_ts(previous_timeout);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_LT(begin_ts, sql_deadline);
  EXPECT_GE(finish_ts, sql_deadline);
  ASSERT_EQ(1, keys.count());
  EXPECT_STREQ("first", keys.at(0).read_buf());
  EXPECT_EQ(1, callbacks_.at(0)->merge_count_);
  EXPECT_EQ(0, callbacks_.at(1)->merge_count_);
  EXPECT_EQ(0, callbacks_.at(2)->merge_count_);
}

TEST_F(TestTxLockStat, merge_failure_stops_collection)
{
  ObMemtableKeyArray keys;
  LockStatCallback *failing_cb = nullptr;

  append(1001, 7, 0, "first");
  failing_cb = append(1001, 8, 1, "second");
  ASSERT_NE(nullptr, failing_cb);
  failing_cb->merge_ret_ = OB_ALLOCATE_MEMORY_FAILED;
  append(1001, 9, 2, "third");
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, tx_ctx_.get_memtable_key_arr(keys));
  EXPECT_EQ(1, keys.count());
  EXPECT_EQ(0, callbacks_.at(2)->merge_count_);
}

TEST_F(TestTxLockStat, lock_key_memory_uses_transaction_tenant)
{
  ObTxLockStatIterator iter;
  ObTxLockStat stat;
  IterateTxLockStatFunctor functor(iter);
  int64_t result_count = 0;
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < 32; ++i) {
    const std::string key_text = "row-" + std::to_string(i);

    append(1001, i + 1, 0, key_text.c_str());
  }

  tx_ctx_.trans_id_ = ObTransID(1);
  ASSERT_TRUE(functor(&tx_ctx_));
  EXPECT_GT(callbacks_.back()->key_array_capacity_, 16);
  EXPECT_EQ(tx_ctx_.get_tenant_id(), callbacks_.back()->key_array_tenant_id_);
  ASSERT_EQ(OB_SUCCESS, iter.set_ready());
  while (OB_SUCC(iter.get_next(stat))) {
    ++result_count;
  }

  EXPECT_EQ(OB_ITER_END, ret);
  EXPECT_EQ(32, result_count);
}

TEST_F(TestTxLockStat, row_callback_merges_by_tablet_and_hash)
{
  ObMemtable first_memtable;
  ObMemtable second_memtable;
  ObMvccRow first_row;
  ObMvccRow second_row;
  ObMvccRowCallback first_cb(mem_ctx_, first_row, &first_memtable);
  ObMvccRowCallback second_cb(mem_ctx_, second_row, &second_memtable);
  ObObj obj;
  ObMemtableKeyArray keys;

  obj.set_int(42);
  ObStoreRowkey rowkey(&obj, 1);

  first_memtable.key_.tablet_id_ = ObTabletID(1001);
  second_memtable.key_.tablet_id_ = ObTabletID(1002);
  ASSERT_EQ(OB_SUCCESS, first_cb.key_.encode(&rowkey));
  ASSERT_EQ(OB_SUCCESS, second_cb.key_.encode(&rowkey));
  ASSERT_EQ(OB_SUCCESS, first_cb.merge_memtable_key(keys));
  ASSERT_EQ(OB_SUCCESS, first_cb.merge_memtable_key(keys));
  ASSERT_EQ(1, keys.count());
  ASSERT_EQ(OB_SUCCESS, second_cb.merge_memtable_key(keys));
  ASSERT_EQ(2, keys.count());
  EXPECT_EQ(1001, keys.at(0).get_tablet_id().id());
  EXPECT_EQ(1002, keys.at(1).get_tablet_id().id());
  EXPECT_EQ(keys.at(0).get_hash_val(), keys.at(1).get_hash_val());
  EXPECT_NE(std::string::npos, std::string(keys.at(0).read_buf()).find("42"));
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_tx_misc.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
