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
#include <thread>

#define USING_LOG_PREFIX STORAGE

#define private public
#define protected public

#include "share/ob_ls_id.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "share/scn.h"
#include "logservice/palf/log_define.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
class TestTabletStatus : public ::testing::Test
{
public:
  TestTabletStatus(const uint64_t tenant_id = TEST_TENANT_ID);
  virtual ~TestTabletStatus() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  void wait_for_tablet(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle);
public:
  static const uint64_t TEST_TENANT_ID = 1;
  static const uint64_t TEST_LS_ID = 1001;

  const uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTenantBase *tenant_base_;
  common::ObArenaAllocator allocator_;
};

TestTabletStatus::TestTabletStatus(const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    ls_id_(TEST_LS_ID),
    tenant_base_(nullptr)
{
}

void TestTabletStatus::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  ret = t3m->init();
  ASSERT_EQ(OB_SUCCESS, ret);

  tenant_base_ = MTL_CTX();
  ASSERT_TRUE(tenant_base_ != nullptr);
}

void TestTabletStatus::TearDown()
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
}

void TestTabletStatus::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TestSchemaUtils::TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTabletStatus::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  ret = t3m->init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestTabletStatus::wait_for_tablet(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = 3 * 1000 * 1000;
  share::ObTenantEnv::set_tenant(tenant_base_);

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;
  while (true) {
    ret = ls_tablet_service.get_tablet(tablet_id, tablet_handle, timeout_us);
    if (OB_TIMEOUT == ret) {
      LOG_INFO("get tablet timeout", K(ret), K(tablet_id));
      continue;
    } else if (OB_SUCCESS == ret) {
      break;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      break;
    }
  }
}

TEST_F(TestTabletStatus, misc)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTabletID tablet_id(123);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObTabletHandle tablet_handle;

  // get ls
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();

  ret = t3m->create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet_handle.is_valid());

  // mock inited
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  const transaction::ObTransID tx_id = 1;
  const int64_t snapshot_version = 1;
  const lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  ObTabletID empty_tablet_id;
  ObFreezer *freezer = ls->get_freezer();

  ObTablet *tablet = tablet_handle.get_obj();
  tablet->tablet_meta_.tx_data_.tablet_status_ = ObTabletStatus::CREATING; // mock
  ObTabletTableStoreFlag store_flag;
  store_flag.set_with_major_sstable();
  ret = tablet->init(allocator_, ls_id_, tablet_id, tablet_id, empty_tablet_id, empty_tablet_id,
      share::SCN::base_scn(), snapshot_version, table_schema, compat_mode, store_flag, nullptr, freezer);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  // set tx data
  ObTabletTxMultiSourceDataUnit tx_data;
  const bool for_replay = false;
  tx_data.tx_id_ = 1;
  tx_data.tx_scn_.convert_for_gts(share::OB_MAX_SCN_TS_NS);
  tx_data.tablet_status_ = ObTabletStatus::CREATING;
  ret = tablet->set_tx_data(tx_data, for_replay);
  ASSERT_EQ(OB_SUCCESS, ret);

  // get tablet
  ObLSTabletService &ls_tablet_service = ls->ls_tablet_svr_;
  const int64_t timeout_us = 100 * 1000;
  ret = ls_tablet_service.get_tablet(tablet_id, tablet_handle, timeout_us);
  ASSERT_EQ(OB_TIMEOUT, ret);

  // mock
  ObTabletHandle handle;
  std::thread t1(&TestTabletStatus::wait_for_tablet, this, tablet_id, std::ref(handle));
  ASSERT_TRUE(!handle.is_valid());

  tx_data.reset();
  ret = tablet->get_tx_data(tx_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::CREATING, tx_data.tablet_status_);

  // tablet status NORMAL
  ObMulSourceDataNotifyArg trans_flags;
  trans_flags.tx_id_ = 1;
  trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 1);
  trans_flags.for_replay_ = for_replay;

  ret = ObTabletCreateDeleteHelper::set_tablet_final_status(tablet_handle, ObTabletStatus::NORMAL,
      trans_flags.scn_, share::SCN::max_scn(), trans_flags.for_replay_);
  ASSERT_EQ(OB_SUCCESS, ret);

  t1.join();

  ASSERT_TRUE(handle.is_valid());
  ASSERT_EQ(tablet_id, handle.get_obj()->tablet_meta_.tablet_id_);

  tx_data.reset();
  ret = tablet->get_tx_data(tx_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
  ASSERT_EQ(ObTabletCommon::FINAL_TX_ID, tx_data.tx_id_);

  // tablet status DELETING
  tx_data.tx_id_ = 2;
  tx_data.tx_scn_ = share::SCN::max_scn();
  tx_data.tablet_status_ = ObTabletStatus::DELETING;
  ret = tablet_handle.get_obj()->set_tx_data(tx_data, for_replay);
  ASSERT_EQ(OB_SUCCESS, ret);

  // get tablet
  ret = ls_tablet_service.get_tablet(tablet_id, tablet_handle, timeout_us);
  ASSERT_EQ(OB_TIMEOUT, ret);

  handle.reset();
  std::thread t2(&TestTabletStatus::wait_for_tablet, this, tablet_id, std::ref(handle));
  ASSERT_TRUE(!handle.is_valid());

  tx_data.reset();
  ret = tablet->get_tx_data(tx_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::DELETING, tx_data.tablet_status_);

  // tablet status DELETED
  trans_flags.tx_id_ = 2;
  ret = ObTabletCreateDeleteHelper::set_tablet_final_status(tablet_handle, ObTabletStatus::DELETED,
                                                          trans_flags.scn_, share::SCN::max_scn(), trans_flags.for_replay_);
  ASSERT_EQ(OB_SUCCESS, ret);

  t2.join();

  ASSERT_TRUE(!handle.is_valid()); // tablet is deleted, so handle is invalid

  tx_data.reset();
  ret = tablet->get_tx_data(tx_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::DELETED, tx_data.tablet_status_);

  // get tablet
  ret = ls_tablet_service.get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_status.log*");
  OB_LOGGER.set_file_name("test_tablet_status.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
