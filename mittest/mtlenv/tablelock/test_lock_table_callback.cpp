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

#include "common/rowkey/ob_store_rowkey.h"
#include "storage/ls/ob_freezer.h"
#include "storage/memtable/ob_memtable_util.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_mem_ctx_table_lock.h"
#include "table_lock_common_env.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace memtable;

namespace transaction
{
namespace tablelock
{

int ObLockMemtable::update_lock_status(
    const ObTableLockOp &op_info,
    const share::SCN &commit_version,
    const share::SCN &commit_scn,
    const ObTableLockOpStatus status)
{
  UNUSEDx(op_info, commit_version, commit_scn, status);
  return OB_SUCCESS;
}


class TestLockTableCallback : public ::testing::Test
{
public:
  TestLockTableCallback()
    : ls_id_(1),
      fake_t3m_(common::OB_SERVER_TENANT_ID)
  {
    LOG_INFO("construct TestLockTableCallback");
  }
  ~TestLockTableCallback() {}

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override
  {
    create_memtable();
    init_mem_ctx(handle_);
    LOG_INFO("set up success");
  }
  virtual void TearDown() override
  {
    LOG_INFO("tear down success");
  }
private:
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

  void init_mem_ctx(ObTableHandleV2 &lock_memtable_handle)
  {
    mt_ctx_.is_inited_ = true;
    mt_ctx_.ctx_cb_allocator_.init(common::OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, mt_ctx_.enable_lock_table(lock_memtable_handle));
  }

  void create_callback(const ObTableLockOp &lock_op, ObOBJLockCallback *&cb)
  {
    int ret = OB_SUCCESS;
    ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
    static ObFakeStoreRowKey tablelock_fake_rowkey("tbl", 3);
    const ObStoreRowkey &rowkey = tablelock_fake_rowkey.get_rowkey();
    ObMemtableKey mt_key;
    ret = mt_ctx_.lock_mem_ctx_.add_lock_record(lock_op, lock_op_node);
    ASSERT_EQ(OB_SUCCESS, ret);
    cb = mt_ctx_.create_table_lock_callback(mt_ctx_,
                                           &memtable_);
    ASSERT_NE(nullptr, cb);
    ret = mt_key.encode(&rowkey);
    ASSERT_EQ(OB_SUCCESS, ret);
    cb->set(mt_key, lock_op_node);
  }
  void free_callback(ObOBJLockCallback *&cb)
  {
    mt_ctx_.free_table_lock_callback(cb);
    cb = nullptr;
  }

private:
  ObLSID ls_id_;
  ObLockMemtable memtable_;
  ObTableHandleV2 handle_;
  ObTenantMetaMemMgr fake_t3m_;
  ObFreezer freezer_;

  ObMemtableCtx mt_ctx_;
};

void TestLockTableCallback::SetUpTestCase()
{
  LOG_INFO("SetUpTestCase");
  init_default_lock_test_value();
}

void TestLockTableCallback::TearDownTestCase()
{
  LOG_INFO("TearDownTestCase");
}

TEST_F(TestLockTableCallback, callback)
{
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  const bool for_replay = false;
  ObIMemtable *memtable = nullptr;
  ObOBJLockCallback *cb = nullptr;
  // 1. UNNSED CALLBACK TYPE
  LOG_INFO("TestLockTableCallback::callback 1.");
  static const int UNUSED_TYPE_NUM = 4;
  create_callback(DEFAULT_IN_TRANS_LOCK_OP, cb);
  ret = cb->elr_trans_preparing();
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2. TCB_PRINT_CALLBACK
  LOG_INFO("TestLockTableCallback::callback 2.");
  ret = cb->print_callback();
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3. IN_TRANS_LOCK
  // 3.1 intrans commit
  LOG_INFO("TestLockTableCallback::callback 3.1");
  ret = mt_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                 DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                 lock_exist,
                                 lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  ret = cb->trans_commit();
  ASSERT_EQ(OB_SUCCESS, ret);
  free_callback(cb);
  ret = mt_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                 DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                 lock_exist,
                                 lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, false);
  // 3.2 intrans abort
  LOG_INFO("TestLockTableCallback::callback 3.2");
  create_callback(DEFAULT_IN_TRANS_LOCK_OP, cb);
  ret = mt_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                 DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                 lock_exist,
                                 lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  ret = cb->trans_abort();
  ASSERT_EQ(OB_SUCCESS, ret);
  free_callback(cb);
  ret = mt_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                 DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                 lock_exist,
                                 lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, false);
  // 3.3 intrans stmt abort
  LOG_INFO("TestLockTableCallback::callback 3.3");
  create_callback(DEFAULT_IN_TRANS_LOCK_OP, cb);
  ret = mt_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                 DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                 lock_exist,
                                 lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  ret = cb->rollback_callback();
  ASSERT_EQ(OB_SUCCESS, ret);
  free_callback(cb);
  ret = mt_ctx_.check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                 DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                 DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                 lock_exist,
                                 lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, false);
}

TEST_F(TestLockTableCallback, basic)
{
  LOG_INFO("TestLockTableCallback::basic");
  int ret = OB_SUCCESS;
  ObOBJLockCallback *cb = nullptr;

  ObTableLockOp lock_op = DEFAULT_IN_TRANS_LOCK_OP;
  create_callback(lock_op, cb);
  ASSERT_EQ(lock_op.lock_seq_no_, cb->get_seq_no());
  ASSERT_EQ(false, cb->must_log());
  ASSERT_EQ(false, cb->is_log_submitted());
  share::SCN scn_10;
  scn_10.convert_for_logservice(10);
  ObIMemtable *mt = NULL;
  ret = cb->log_submitted_cb(scn_10, mt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, cb->is_log_submitted());
  ASSERT_EQ(LS_LOCK_TABLET, cb->get_tablet_id());

  TableLockRedoDataNode redo_node;
  ret = cb->get_redo(redo_node);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(redo_node.lock_id_, DEFAULT_IN_TRANS_LOCK_OP.lock_id_);
  ret = cb->rollback_callback();
  ASSERT_EQ(OB_SUCCESS, ret);
  free_callback(cb);
}

} // tablelock
} // transaction
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lock_table_callback.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_lock_table_callback.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  oceanbase::common::ObLogger::get_logger().set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
