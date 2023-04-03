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

#define USING_LOG_PREFIX STORAGE

#include "lib/allocator/page_arena.h"

#define protected public
#define private public

#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/ob_pg_ddl_kv_mgr.h"
#include "logservice/palf/palf_options.h"
#include "share/ob_alive_server_tracer.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "lib/file/file_directory_utils.h"
#include "storage/mock_ob_meta_report.h"
#include "storage/ob_super_block_struct.h"
#include "storage/mock_gctx.h"

#undef private
#undef protected

namespace oceanbase
{
using namespace share;
namespace storage
{

const char *clog_base_dir = "./test_clog";

class TestTabletCreateMemtable : public ::testing::Test
{
public:
  TestTabletCreateMemtable();
  virtual ~TestTabletCreateMemtable() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;

private:
  int build_table_handle(
      const share::schema::ObTableSchema &table_schema,
      ObTableHandleV2 &table_handle);
public:
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;

public:
  common::ObArenaAllocator allocator_;
  ObTablet test_tablet_;
  ObPGDDLKvMgr ddl_kv_mgr_;
  ObFreezer handler;
  blocksstable::ObLogFileSpec log_file_spec_;
  char dir_[128];
};

TestTabletCreateMemtable::TestTabletCreateMemtable()
  : allocator_(),
    test_tablet_(),
    log_file_spec_()
{
}

void TestTabletCreateMemtable::SetUp()
{
  int ret = OB_SUCCESS;
  system("rm -rf ./test_create_tablet_memtable_dir");
  MEMCPY(dir_, "./test_create_tablet_memtable_dir", sizeof("./test_create_tablet_memtable_dir"));
  init_global_context();
  FileDirectoryUtils::create_full_path("./test_create_tablet_memtable_dir");
  log_file_spec_.retry_write_policy_ = "normal";
  log_file_spec_.log_create_policy_ = "normal";
  log_file_spec_.log_write_policy_ = "truncate";
  share::ObLocationService location_service;
  obrpc::ObBatchRpc batch_rpc;
  share::schema::ObMultiVersionSchemaService schema_service;
  share::ObAliveServerTracer server_tracer;
  palf::PalfDiskOptions disk_options;
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  MockObMetaReport reporter;
  ObAddr self_addr(ObAddr::IPV4, "127.0.0.1", 52965);
  palf::PalfHandle palf_handle;
  ObTenantSuperBlock super_block(1);


  ret = ObTenantMutilAllocatorMgr::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogger *slogger = nullptr;
  SLOGGERMGR.init(dir_, MAX_FILE_SIZE, log_file_spec_);
  SLOGGERMGR.register_tenant(1);
  ret = SLOGGERMGR.get_tenant_storage_logger(1, slogger);
  ASSERT_EQ(OB_SUCCESS, ret);

  share::ObLSID ls_id(1);
  common::ObTabletID tablet_id(1);
  common::ObTabletID data_tablet_id(1);
  share::schema::ObTableSchema table_schema;
  ObTabletMemtableMgr *memtable_mgr = new ObTabletMemtableMgr(ddl_kv_mgr_);
  ret = memtable_mgr->init(tablet_id, &handler);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  test_tablet_.memtable_mgr_ = memtable_mgr;
  test_tablet_.palf_handle_ = &palf_handle;

  ObTableHandleV2 table_handle;
  ret = build_table_handle(table_schema, table_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = test_tablet_.init(ls_id, tablet_id, data_tablet_id, table_schema, table_handle);
  //ASSERT_EQ(common::OB_SUCCESS, ret);
  test_tablet_.memtable_mgr_ = memtable_mgr;
}

void TestTabletCreateMemtable::TearDown()
{
  system("rm -rf ./test_create_tablet_memtable_dir");
  // delete test_tablet_.memtable_mgr_;
}

int TestTabletCreateMemtable::build_table_handle(
    const share::schema::ObTableSchema &table_schema,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObTabletCreateSSTableParam param;
  param.ddl_scn_.set_min();
  param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  param.table_key_.table_id_ = 1;
  param.table_key_.tablet_id_ = common::ObTabletID(1);
  param.table_key_.trans_version_range_.multi_version_start_ = 0;
  param.table_key_.trans_version_range_.base_version_ = ObVersionRange::MIN_VERSION;
  param.table_key_.trans_version_range_.snapshot_version_ = 0;

  param.schema_version_ = 0;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 1;
  param.progressive_merge_step_ = 0;
  param.logical_data_version_ = 0;

  param.table_mode_ = table_schema.get_table_mode_struct();
  param.index_type_ = table_schema.get_index_type();
  param.rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
          + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ObTenantMetaMemMgr &meta_mem_mgr = *(MTL(ObTenantMetaMemMgr*));
  common::ObIAllocator &allocator = meta_mem_mgr.get_tenant_allocator();
  ObTableHandleV2 handle;
  ObSSTable *sstable = nullptr;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(meta_mem_mgr.acquire_sstable(handle))) {
    LOG_WARN("failed to acquire sstable", K(ret));
  } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(handle.get_table()))) {
    LOG_WARN("failed to get sstable from table handle", K(ret), K(handle));
  } else if (OB_FAIL(sstable->init(param.table_key_, &allocator))) {
    LOG_WARN("failed to init sstable", K(ret), "table key", param.table_key_);
  } else if (OB_FAIL(sstable->open(param))) {
    // TODO: simplify sstable init/open procedure
    LOG_WARN("failed to open sstable", K(ret), K(param));
  } else {
    table_handle = handle;
  }

  return ret;
}

TEST_F(TestTabletCreateMemtable, test_create_tablet_memtable)
{
  // TODO: table store refactor
  /*int ret = OB_SUCCESS;
  ObMemtableHandle table_handle;
  memtable::ObMemtable *memtable = nullptr;

  ret = test_tablet_.create_memtable();
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ret = test_tablet_.memtable_mgr_->get_active_memtable(table_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ASSERT_TRUE(table_handle.is_valid());
  ASSERT_NE(nullptr, table_handle.get_obj());*/
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_create_tablet_memtable.log*");
  rmdir(oceanbase::storage::clog_base_dir);
  mkdir(oceanbase::storage::clog_base_dir, 0777);
  OB_LOGGER.set_file_name("test_create_tablet_memtable.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
