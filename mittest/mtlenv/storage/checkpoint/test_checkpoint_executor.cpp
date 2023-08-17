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
#define protected public
#define private public
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "storage/checkpoint/ob_common_checkpoint.h"
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/init_basic_struct.h"
#include "storage/mock_ob_log_handler.h"
#include "share/scn.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;


namespace oceanbase
{
using namespace palf;

namespace storage
{
using namespace checkpoint;

MockObLogHandler mock_log_handler_;

int ObTableHandleV2::set_table(
  ObITable *const table,
  ObTenantMetaMemMgr *const t3m,
  const ObITable::TableType table_type)
{
  int ret = OB_SUCCESS;
  reset();
  table_ = table;
  table_->inc_ref();
  t3m_ = t3m;
  table_type_ = table_type;
  return ret;
}

void ObTableHandleV2::reset()
{
  if (nullptr != table_) {
    if (nullptr == t3m_) {
    } else {
      if (0 == table_->dec_ref()) {
        // just for debug
        if (ObITable::TX_DATA_MEMTABLE == table_type_ || ObITable::TX_CTX_MEMTABLE == table_type_) {
          STORAGE_LOG(INFO, "push memtable into gc queue", K(table_type_), K(lbt()), KPC(this));
        }
        t3m_->push_table_into_gc_queue(table_, table_type_);
      }
      table_ = nullptr;
      t3m_ = nullptr;
    }
  }
}

int ObFreezer::get_ls_weak_read_scn(share::SCN &weak_read_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(weak_read_scn.convert_for_logservice(100))) {
    STORAGE_LOG(WARN, "fail to convert_for_logservice", K(ret));
  }
  return ret;
}

logservice::ObILogHandler* ObFreezer::get_ls_log_handler()
{
  return &mock_log_handler_;
}

typedef common::ObFunction<int(ObLSMeta &)> WriteSlog;
WriteSlog ObLSMeta::write_slog_ = [](ObLSMeta &ls_meta) { return OB_SUCCESS; };

int64_t ObCheckPointService::CHECKPOINT_INTERVAL = 1000 * 1000L;

class TestMemtable : public memtable::ObMemtable
{
public:
  TestMemtable()
    : rec_scn_(share::SCN::max_scn()),
      ready_for_flush_(true)
  {}
  ~TestMemtable()
  {
    ObFreezeCheckpoint::remove_from_data_checkpoint();
  }

  void set_rec_scn(share::SCN rec_scn)
  {
    if (rec_scn < rec_scn_) {
      rec_scn_ = rec_scn;
      data_checkpoint_->transfer_from_new_create_to_active_without_src_lock_(this);
    }
  }

  share::SCN get_rec_scn() { return rec_scn_; }

  bool rec_scn_is_stable() { return true; }

  bool ready_for_flush() { return ready_for_flush_; }

  bool is_frozen_checkpoint() const { return true; }

  bool is_active_checkpoint() const { return false; }

  void set_ready_for_flush(bool ready) { ready_for_flush_ = ready; }

  ObTabletID get_tablet_id() const override {
    return ObTabletID(111111111111);
  }

  int flush(share::ObLSID ls_id)
  {
    UNUSED(ls_id);
    share::SCN tmp;
    tmp.val_ = rec_scn_.val_ + 100;
    rec_scn_ = tmp;
    return OB_SUCCESS;
  }

private:
  share::SCN rec_scn_;
  bool ready_for_flush_;
};

class TestCommonCheckpoint : public ObCommonCheckpoint
{
public:
  share::SCN get_rec_scn() override {
    return rec_scn_;
  }

  int flush(share::SCN recycle_scn, bool need_freeze = true) override {
    share::SCN tmp;
    tmp.val_ = rec_scn_.val_ + 20;
    rec_scn_ = tmp;
    return OB_SUCCESS;
  }

  ObTabletID get_tablet_id() const override {
    return ObTabletID(ObTabletID::LS_TX_CTX_TABLET_ID);
  }

  bool is_flushing() const override {
    return true;
  }

  void set_rec_scn(SCN rec_scn) {
    rec_scn_ = rec_scn;
  }
private:
  share::SCN rec_scn_;
};

class TestService : public logservice::ObICheckpointSubHandler
{
public:
  TestService()
    : rec_scn_(share::SCN::max_scn())
  {}
  share::SCN get_rec_scn() override { return rec_scn_; }

  int flush(share::SCN &rec_scn) override
  {
    if (rec_scn_ <= rec_scn) {
      share::SCN tmp;
      tmp.val_ = rec_scn_.val_ + 2;
      rec_scn_ = tmp;
    }
    return OB_SUCCESS;
  }
private:
  share::SCN rec_scn_;
};

class TestCheckpointExecutor : public ::testing::Test
{
public:
  TestCheckpointExecutor();
  virtual ~TestCheckpointExecutor() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();

private:
  int64_t tenant_id_;
};

TestCheckpointExecutor::TestCheckpointExecutor()
{
}

void TestCheckpointExecutor::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  tenant_id_ = MTL_ID();
}

void TestCheckpointExecutor::TearDown()
{
}

void TestCheckpointExecutor::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
}

void TestCheckpointExecutor::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestCheckpointExecutor, calculate_checkpoint)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObCreateLSArg arg;
  ObLSHandle handle;
  //ls102
  ObLSID ls_id(102);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id_, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS * ls2 = handle.get_ls();
  ASSERT_NE(nullptr, ls2);
  ObDataCheckpoint *data_checkpoint = ls2->get_data_checkpoint();
  TestMemtable memtable1;
  memtable1.add_to_data_checkpoint(data_checkpoint);
  TestMemtable memtable2;
  memtable2.add_to_data_checkpoint(data_checkpoint);
  TestMemtable memtable3;
  memtable3.add_to_data_checkpoint(data_checkpoint);
  ASSERT_EQ(3, data_checkpoint->new_create_list_.checkpoint_list_.get_size());
  share::SCN tmp;
  tmp.val_ = 10;
  memtable1.set_rec_scn(tmp);
  tmp.val_ = 20;
  memtable2.set_rec_scn(tmp);
  tmp.val_ = 30;
  memtable3.set_rec_scn(tmp);

  TestCommonCheckpoint common_checkpoint;
  tmp.val_ = 5;
  common_checkpoint.set_rec_scn(tmp);
  ObLSTxService *ls_tx_svr = ls2->get_tx_svr();
  ASSERT_NE(nullptr, ls_tx_svr);
  ASSERT_EQ(OB_SUCCESS, ls_tx_svr->register_common_checkpoint(checkpoint::TEST_COMMON_CHECKPOINT, &common_checkpoint));

  TestService service1;
  tmp.val_ = 9;
  service1.rec_scn_ = tmp;
  ObCheckpointExecutor *checkpoint_executor2 = ls2->get_checkpoint_executor();
  checkpoint_executor2->register_handler(TIMESTAMP_LOG_BASE_TYPE, &service1);
  ASSERT_EQ(OB_SUCCESS, checkpoint_executor2->init(ls2, &mock_log_handler_));
  checkpoint_executor2->start();


  ASSERT_EQ(OB_SUCCESS, checkpoint_executor2->update_clog_checkpoint());
  tmp.val_ = 5;
  ASSERT_EQ(tmp, ls2->get_ls_meta().get_clog_checkpoint_scn());

  tmp.val_ = 12;
  ASSERT_EQ(OB_SUCCESS, checkpoint_executor2->advance_checkpoint_by_flush(tmp));
  ASSERT_EQ(OB_SUCCESS, ls2->get_data_checkpoint()->flush(share::SCN::max_scn(), false));
  usleep(60L * 1000L);  // 60ms
  checkpoint_executor2->offline();
  ASSERT_EQ(OB_SUCCESS, checkpoint_executor2->update_clog_checkpoint());
  tmp.val_ = 5;
  ASSERT_EQ(tmp, ls2->get_ls_meta().get_clog_checkpoint_scn());
  checkpoint_executor2->online();
  ASSERT_EQ(OB_SUCCESS, checkpoint_executor2->update_clog_checkpoint());
  tmp.val_ = 11;
  ASSERT_EQ(tmp, ls2->get_ls_meta().get_clog_checkpoint_scn());
  tmp.val_ = 25;
  ASSERT_EQ(tmp, common_checkpoint.get_rec_scn());
  tmp.val_ = 110;
  ASSERT_EQ(true, tmp == memtable1.get_rec_scn());

  handle.reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id, false));
}

TEST_F(TestCheckpointExecutor, timer_verify_rec_scn_stable)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObCreateLSArg arg;
  ObLSHandle handle;
  //ls103
  ObLSID ls_id(103);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id_, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS * ls2 = handle.get_ls();
  ASSERT_NE(nullptr, ls2);
  ObDataCheckpoint *data_checkpoint = ls2->get_data_checkpoint();
  TestMemtable memtable1;
  memtable1.add_to_data_checkpoint(data_checkpoint);
  TestMemtable memtable2;
  memtable2.add_to_data_checkpoint(data_checkpoint);
  TestMemtable memtable3;
  memtable3.add_to_data_checkpoint(data_checkpoint);
  ASSERT_EQ(3, data_checkpoint->new_create_list_.checkpoint_list_.get_size());
  ObCheckpointExecutor *checkpoint_executor2 = ls2->get_checkpoint_executor();
  ASSERT_EQ(OB_SUCCESS, ls2->get_data_checkpoint()->check_can_move_to_active_in_newcreate());
  ASSERT_EQ(0, data_checkpoint->new_create_list_.checkpoint_list_.get_size());
  ASSERT_EQ(3, data_checkpoint->active_list_.checkpoint_list_.get_size());

  handle.reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id, false));
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_checkpoint_executor.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
