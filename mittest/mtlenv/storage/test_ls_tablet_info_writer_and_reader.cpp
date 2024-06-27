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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define protected public
#define private public

#include "storage/schema_utils.h"
#include "storage/ob_storage_schema.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_dml_common.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
int ObClusterVersion::get_tenant_data_version(const uint64_t tenant_id, uint64_t &data_version)
{
  data_version = DATA_VERSION_4_3_2_0;
  return OB_SUCCESS;
}
namespace storage
{
class TestLSTabletInfoWR : public ::testing::Test
{
public:
  TestLSTabletInfoWR();
  virtual ~TestLSTabletInfoWR() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp() override;
  virtual void TearDown() override;
  void fill_tablet_meta();
  void inner_init();
  void clean_env();
public:
  static const uint64_t TEST_TENANT_ID = 1;
  static const int64_t TEST_LS_ID = 101;
  ObArray<ObMigrationTabletParam> tablet_metas;
  share::ObBackupDest backup_set_dest_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  ObArenaAllocator arena_allocator_;
};

void TestLSTabletInfoWR::inner_init()
{
  int ret = OB_SUCCESS;
  common::ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_extern_info_mgr", get_current_dir_name());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  clean_env();
  ret = backup_set_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = util.mkdir(test_dir_uri_, backup_set_dest_.get_storage_info());
  EXPECT_EQ(OB_SUCCESS, ret);
}

void TestLSTabletInfoWR::clean_env()
{
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

TestLSTabletInfoWR::TestLSTabletInfoWR()
{
}

void TestLSTabletInfoWR::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestLSTabletInfoWR::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestLSTabletInfoWR::SetUp()
{
  inner_init();
  fill_tablet_meta();
}

void TestLSTabletInfoWR::TearDown()
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  ObTabletMapKey key;
  key.ls_id_ = TEST_LS_ID;

  key.tablet_id_ = 1002;
  ret = t3m->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  key.tablet_id_ = 1003;
  ret = t3m->del_tablet(key);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestLSTabletInfoWR::fill_tablet_meta()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ObLSID(TEST_LS_ID), ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletMapKey src_key;
  src_key.ls_id_ = TEST_LS_ID;
  src_key.tablet_id_ = 1002;

  ObTabletHandle src_handle;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ret = t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, src_key, arena_allocator_, ls_handle, src_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;

  ret = create_tablet_schema.init(schema_allocator, table_schema, lib::Worker::CompatMode::MYSQL,
        false/*skip_column_info*/, ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletID empty_tablet_id;
  SCN scn;
  scn.convert_from_ts(ObTimeUtility::current_time());
  ret = src_handle.get_obj()->init_for_first_time_creation(arena_allocator_, src_key.ls_id_, src_key.tablet_id_, src_key.tablet_id_,
      scn, 2022, create_tablet_schema, true/*need_create_empty_major_sstable*/, ls_handle.get_ls()->get_freezer());
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 50);
  // write data to mds table no.1 row
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = src_handle.get_obj()->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(create_commit_scn, create_commit_scn);
  }

  ObMigrationTabletParam tablet_param;
  ret = src_handle.get_obj()->build_migration_tablet_param(tablet_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet_param.is_valid());

  for (int i = 0; i < 3; i++) {
    tablet_param.tablet_id_ = ObTabletID(tablet_param.tablet_id_.id() + 1);
    tablet_metas.push_back(tablet_param);
  }
}

TEST_F(TestLSTabletInfoWR, testTabletInfoWriterAndReader)
{
  int ret = OB_SUCCESS;
  ObInOutBandwidthThrottle bandwidth_throttle;
  ASSERT_EQ(OB_SUCCESS, bandwidth_throttle.init(1024 * 1024 * 60));
  LOG_INFO("test tablet info", K(tablet_metas.count()), K(backup_set_dest_));
  backup::ObExternTabletMetaWriter writer;
  backup::ObExternTabletMetaReader reader;
  ASSERT_EQ(OB_SUCCESS, writer.init(backup_set_dest_, ObLSID(TEST_LS_ID), 1, 0, bandwidth_throttle));
  for (int i = 0; i < tablet_metas.count(); i++) {
    blocksstable::ObSelfBufferWriter buffer_writer("TestBuff");
    blocksstable::ObBufferReader buffer_reader;
    if (OB_FAIL(buffer_writer.ensure_space(backup::OB_BACKUP_READ_BLOCK_SIZE))) {
      LOG_WARN("failed to ensure space");
    } else if (OB_FAIL(buffer_writer.write_serialize(tablet_metas.at(i)))) {
      LOG_WARN("failed to writer", K(tablet_metas.at(i)));
    } else {
      buffer_reader.assign(buffer_writer.data(), buffer_writer.length(), buffer_writer.length());
      ASSERT_EQ(OB_SUCCESS, writer.write_meta_data(buffer_reader, tablet_metas.at(i).tablet_id_));
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, reader.init(backup_set_dest_, ObLSID(TEST_LS_ID)));
  while (OB_SUCC(ret)) {
    storage::ObMigrationTabletParam tablet_meta;
    ret = reader.get_next(tablet_meta);
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ls_tablet_info_writer_and_reader.log*");
  OB_LOGGER.set_file_name("test_ls_tablet_info_writer_and_reader.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
