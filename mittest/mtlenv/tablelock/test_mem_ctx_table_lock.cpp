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

#define USING_LOG_PREFIX TABLELOCK

#include <gtest/gtest.h>
#define protected public
#define private public

#include "lib/allocator/page_arena.h"
#include "storage/ls/ob_freezer.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_mem_ctx_table_lock.h"
#include "table_lock_common_env.h"
#include "storage/memtable/ob_memtable_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;

namespace transaction
{
namespace tablelock
{
int ObLockMemtable::update_lock_status(const ObTableLockOp &op_info,
                                       const share::SCN &commit_version,
                                       const share::SCN &commit_scn,
                                       const ObTableLockOpStatus status)
{
  UNUSEDx(op_info, commit_version, commit_scn, status);
  return OB_SUCCESS;
}

class TestMemCtxTableLock : public ::testing::Test
{
public:
  TestMemCtxTableLock()
    : ls_id_(1),
      fake_t3m_(common::OB_SERVER_TENANT_ID),
      mem_ctx_(fake_memtable_ctx_)
  {
    LOG_INFO("construct TestMemCtxTableLock");
  }
  ~TestMemCtxTableLock() {}

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override
  {
    create_memtable();
    init_mem_ctx();
    LOG_INFO("set up success");
  }
  virtual void TearDown() override
  {
    LOG_INFO("tear down success");
  }
public:
  void create_memtable()
  {
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::LOCK_MEMTABLE;
    table_key.tablet_id_ = LS_LOCK_TABLET;
    table_key.scn_range_.start_scn_.convert_for_gts(1); // fake
    table_key.scn_range_.end_scn_.convert_for_gts(2); // fake

    ASSERT_EQ(OB_SUCCESS, memtable_.init(table_key, ls_id_, &freezer_));
    ASSERT_EQ(OB_SUCCESS, handle_.set_table(&memtable_, &fake_t3m_, ObITable::LOCK_MEMTABLE));
  }

  void init_mem_ctx()
  {
    ASSERT_EQ(OB_SUCCESS, fake_memtable_ctx_.init(common::OB_SERVER_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, mem_ctx_.init(handle_));
  }

private:
  ObLSID ls_id_;
  ObLockMemtable memtable_;
  ObTableHandleV2 handle_;
  ObTenantMetaMemMgr fake_t3m_;
  memtable::ObMemtableCtx fake_memtable_ctx_;
  ObFreezer freezer_;
  ObLockMemCtx mem_ctx_;
  ObArenaAllocator allocator_;
};

void TestMemCtxTableLock::SetUpTestCase()
{
  LOG_INFO("SetUpTestCase");
  init_default_lock_test_value();
}

void TestMemCtxTableLock::TearDownTestCase()
{
  LOG_INFO("TearDownTestCase");
}

TEST_F(TestMemCtxTableLock, add_lock_record)
{
  LOG_INFO("TestMemCtxTableLock::add_lock_record");
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  // 1. IN TRANS LOCK
  // 1.1 add lock record
  LOG_INFO("TestMemCtxTableLock::add_lock_record 1.1");
  ret = mem_ctx_.add_lock_record(DEFAULT_IN_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check lock exist
  LOG_INFO("TestMemCtxTableLock::add_lock_record 1.2");
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, true);
  // 2. OUT TRANS LOCK
  // 2.1 add lock redord
  LOG_INFO("TestMemCtxTableLock::add_lock_record 2.1");
  ret = mem_ctx_.add_lock_record(DEFAULT_OUT_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 check lock exist
  LOG_INFO("TestMemCtxTableLock::add_lock_record 2.2");
  ret = mem_ctx_.check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, true);
  // 3. REMOVE CHECK
  // 3.1 in trans lock remove test
  LOG_INFO("TestMemCtxTableLock::add_lock_record 3.1");
  mem_ctx_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  // 3.2 out trans lock remove test
  LOG_INFO("TestMemCtxTableLock::add_lock_record 3.2");
  mem_ctx_.remove_lock_record(DEFAULT_OUT_TRANS_LOCK_OP);
  ret = mem_ctx_.check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);

  // 4 REMOVE WITH PTR
  // 4.1 add lock record
  LOG_INFO("TestMemCtxTableLock::add_lock_record 4.1");
  ret = mem_ctx_.add_lock_record(DEFAULT_IN_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, true);
  // 4.2 remove lock record
  LOG_INFO("TestMemCtxTableLock::add_lock_record 4.2");
  mem_ctx_.remove_lock_record(lock_op_node);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
}

TEST_F(TestMemCtxTableLock, clear_table_lock)
{
  LOG_INFO("TestMemCtxTableLock::clear_table_lock");
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  bool is_committed = false;
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_min();
  commit_scn.set_min();
  // 1 EMPTY CLEAR
  LOG_INFO("TestMemCtxTableLock::clear_table_lock 1");
  ret = mem_ctx_.clear_table_lock(is_committed, commit_version, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2 COMMIT CLEAR
  is_committed = true;
  commit_version = share::SCN::plus(share::SCN::min_scn(), 1);
  // 2.1 in trans lock record
  LOG_INFO("TestMemCtxTableLock::clear_table_lock 2.1");
  ret = mem_ctx_.add_lock_record(DEFAULT_IN_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.clear_table_lock(is_committed, commit_version, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  // 2.2 out trans lock record
  LOG_INFO("TestMemCtxTableLock::clear_table_lock 2.2");
  ret = mem_ctx_.add_lock_record(DEFAULT_OUT_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.clear_table_lock(is_committed, commit_version, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  // 2.3 out trans unlock record
  LOG_INFO("TestMemCtxTableLock::clear_table_lock 2.3");
  ret = mem_ctx_.add_lock_record(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.clear_table_lock(is_committed, commit_version, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.check_lock_exist(DEFAULT_OUT_TRANS_UNLOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_UNLOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_UNLOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_UNLOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);

  // 3 ABORT CLEAR
  is_committed = false;
  LOG_INFO("TestMemCtxTableLock::clear_table_lock 3.1");
  ret = mem_ctx_.add_lock_record(DEFAULT_IN_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.clear_table_lock(is_committed, commit_version, commit_scn);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
}

TEST_F(TestMemCtxTableLock, rollback_table_lock)
{
  LOG_INFO("TestMemCtxTableLock::rollback_table_lock");
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  // 1.1 add lock record
  LOG_INFO("TestMemCtxTableLock::rollback_table_lock 1.1");
  ObTableLockOp lock_op1 = DEFAULT_IN_TRANS_LOCK_OP;
  lock_op1.lock_seq_no_ = ObTxSEQ(2, 0);
  ret = mem_ctx_.add_lock_record(lock_op1,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTableLockOp lock_op2 = DEFAULT_OUT_TRANS_LOCK_OP;
  lock_op2.lock_seq_no_ = ObTxSEQ(3, 0);
  ret = mem_ctx_.add_lock_record(lock_op2,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check exist
  LOG_INFO("TestMemCtxTableLock::rollback_table_lock 1.2");
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, true);
  ret = mem_ctx_.check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, true);
  // 1.3 rollback
  LOG_INFO("TestMemCtxTableLock::rollback_table_lock 1.3");
  auto rollback_to_seq_no = ObTxSEQ(2, 0);
  auto rollback_from_seq_no = ObTxSEQ(100, 0);
  ret = mem_ctx_.rollback_table_lock(rollback_to_seq_no, rollback_from_seq_no);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, true);
  ret = mem_ctx_.check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  // 1.4 rollback again
  LOG_INFO("TestMemCtxTableLock::rollback_table_lock 1.4");
  rollback_to_seq_no = ObTxSEQ(1, 0);
  ret = mem_ctx_.rollback_table_lock(rollback_to_seq_no, rollback_from_seq_no);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
}

TEST_F(TestMemCtxTableLock, check_lock_need_replay)
{
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay");
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  share::SCN scn;
  scn.set_min();
  bool need_replay = false;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  // 1.1 empty check.
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 1.1");
  ret = mem_ctx_.check_lock_need_replay(scn,
                                        DEFAULT_IN_TRANS_LOCK_OP,
                                        need_replay);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(need_replay, true);
  // 1.2 add a lock record
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 1.2");
  ret = mem_ctx_.add_lock_record(DEFAULT_IN_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.3 not need replay because the lock op exist.
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 1.3");
  ret = mem_ctx_.check_lock_need_replay(scn,
                                        DEFAULT_IN_TRANS_LOCK_OP,
                                        need_replay);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(need_replay, false);
  // 1.4 need replay because of lock op no exist.
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 1.4");
  ret = mem_ctx_.check_lock_need_replay(scn,
                                        DEFAULT_OUT_TRANS_LOCK_OP,
                                        need_replay);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(need_replay, true);
  // 1.5 can not replay because of log ts too small
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 1.5");
  scn = share::SCN::plus(share::SCN::min_scn(), 2);
  mem_ctx_.max_durable_scn_ = scn;
  scn = share::SCN::plus(share::SCN::min_scn(), 1);
  ret = mem_ctx_.check_lock_need_replay(scn,
                                        DEFAULT_OUT_TRANS_LOCK_OP,
                                        need_replay);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(need_replay, false);
  // 1.6 clear the lock op
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 1.6");
  mem_ctx_.remove_lock_record(lock_op_node);
  ret = mem_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
}

TEST_F(TestMemCtxTableLock, get_table_lock_store_info)
{
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay");
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  ObTableLockInfo table_lock_info;
  int64_t op_count = 0;
  share::SCN scn;
  scn.set_min();
  // 1 CHECK NOT LOGGED LOCK
  // 1.1 add in trans lock
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 1.1");
  ret = mem_ctx_.add_lock_record(DEFAULT_IN_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.get_table_lock_store_info(table_lock_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  op_count = table_lock_info.table_lock_ops_.count();
  ASSERT_EQ(1, op_count);
  // 2 CHECK LOGGED LOCK
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 2.1");
  scn.set_base();
  table_lock_info.reset();
  mem_ctx_.sync_log_succ(scn);
  ret = mem_ctx_.get_table_lock_store_info(table_lock_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  op_count = table_lock_info.table_lock_ops_.count();
  ASSERT_EQ(1, op_count);
  ASSERT_EQ(scn, table_lock_info.max_durable_scn_);
  // 3 CHECK LOGGED WITH NOT LOGGED
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 3.1");
  table_lock_info.reset();
  ret = mem_ctx_.add_lock_record(DEFAULT_OUT_TRANS_LOCK_OP,
                                 lock_op_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx_.get_table_lock_store_info(table_lock_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  op_count = table_lock_info.table_lock_ops_.count();
  ASSERT_EQ(2, op_count);
  ASSERT_EQ(scn, table_lock_info.max_durable_scn_);
  // 4 CHECK TWO LOGGED
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 4.1");
  table_lock_info.reset();
  scn = share::SCN::plus(share::SCN::min_scn(), 2);
  mem_ctx_.sync_log_succ(scn);
  ret = mem_ctx_.get_table_lock_store_info(table_lock_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  op_count = table_lock_info.table_lock_ops_.count();
  ASSERT_EQ(2, op_count);
  ASSERT_EQ(scn, table_lock_info.max_durable_scn_);
  // 5 CLEAN TEST
  LOG_INFO("TestMemCtxTableLock::check_lock_need_replay 5.1");
  mem_ctx_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  mem_ctx_.remove_lock_record(DEFAULT_OUT_TRANS_LOCK_OP);
  table_lock_info.reset();
  ret = mem_ctx_.get_table_lock_store_info(table_lock_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  op_count = table_lock_info.table_lock_ops_.count();
  ASSERT_EQ(0, op_count);
}



} // tablelock
} // transaction
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mem_ctx_table_lock.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_mem_ctx_table_lock.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  oceanbase::common::ObLogger::get_logger().set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
