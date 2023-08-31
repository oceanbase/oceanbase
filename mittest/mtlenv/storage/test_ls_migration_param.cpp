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

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{
class TestLSMigrationParam : public ::testing::Test
{
public:
  TestLSMigrationParam();
  virtual ~TestLSMigrationParam() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp() override;
  virtual void TearDown() override;
public:
  static const uint64_t TEST_TENANT_ID = 1;
  static const int64_t TEST_LS_ID = 101;

  blocksstable::ObSSTableMeta sstable_meta_;
  common::ObArenaAllocator allocator_;
};

TestLSMigrationParam::TestLSMigrationParam()
  : sstable_meta_()
{
}

void TestLSMigrationParam::SetUpTestCase()
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

void TestLSMigrationParam::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestLSMigrationParam::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  ObMetaDiskAddr addr;
  addr.set_none_addr();

  sstable_meta_.basic_meta_.row_count_ = 0;
  sstable_meta_.basic_meta_.occupy_size_ = 0;
  sstable_meta_.basic_meta_.data_checksum_ = 0;
  sstable_meta_.basic_meta_.index_type_ = 0;
  sstable_meta_.basic_meta_.rowkey_column_count_ = 0;
  sstable_meta_.basic_meta_.column_cnt_ = 0;
  sstable_meta_.basic_meta_.index_macro_block_count_ = 0;
  sstable_meta_.basic_meta_.use_old_macro_block_count_ = 0;
  sstable_meta_.basic_meta_.index_macro_block_count_ = 0;
  sstable_meta_.basic_meta_.sstable_format_version_ = 0;
  sstable_meta_.basic_meta_.schema_version_ = 0;
  sstable_meta_.basic_meta_.create_snapshot_version_ = 0;
  sstable_meta_.basic_meta_.progressive_merge_round_ = 0;
  sstable_meta_.basic_meta_.progressive_merge_step_ = 0;
  sstable_meta_.basic_meta_.upper_trans_version_ = 0;
  sstable_meta_.basic_meta_.max_merged_trans_version_ = 0;
  sstable_meta_.basic_meta_.ddl_scn_.set_min();
  sstable_meta_.basic_meta_.filled_tx_scn_.set_min();
  sstable_meta_.basic_meta_.contain_uncommitted_row_ = 0;
  sstable_meta_.data_root_info_.addr_ = addr;
  sstable_meta_.basic_meta_.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  sstable_meta_.basic_meta_.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  sstable_meta_.basic_meta_.data_index_tree_height_ = 0;
  sstable_meta_.macro_info_.macro_meta_info_.addr_ = addr;
  ASSERT_TRUE(sstable_meta_.check_meta());
  sstable_meta_.is_inited_ = true;
  ASSERT_TRUE(sstable_meta_.is_valid());
}

void TestLSMigrationParam::TearDown()
{
  int ret = OB_SUCCESS;
  sstable_meta_.reset();

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

TEST_F(TestLSMigrationParam, test_migrate_sstable_param)
{
  int ret = OB_SUCCESS;

  ObMigrationSSTableParam sstable_param;
  ObITable::TableKey key;
  key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  key.tablet_id_ = 20221106;
  key.version_range_.base_version_ = 1;
  key.version_range_.snapshot_version_ = 11;

  sstable_param.basic_meta_.row_count_                     = sstable_meta_.basic_meta_.row_count_;
  sstable_param.basic_meta_.occupy_size_                   = sstable_meta_.basic_meta_.occupy_size_;
  sstable_param.basic_meta_.original_size_                 = sstable_meta_.basic_meta_.original_size_;
  sstable_param.basic_meta_.data_checksum_                 = sstable_meta_.basic_meta_.data_checksum_;
  sstable_param.basic_meta_.index_type_                    = sstable_meta_.basic_meta_.index_type_;
  sstable_param.basic_meta_.rowkey_column_count_           = sstable_meta_.basic_meta_.rowkey_column_count_;
  sstable_param.basic_meta_.column_cnt_                    = sstable_meta_.basic_meta_.column_cnt_;
  sstable_param.basic_meta_.data_macro_block_count_        = sstable_meta_.basic_meta_.data_macro_block_count_;
  sstable_param.basic_meta_.data_micro_block_count_        = sstable_meta_.basic_meta_.data_micro_block_count_;
  sstable_param.basic_meta_.use_old_macro_block_count_     = sstable_meta_.basic_meta_.use_old_macro_block_count_;
  sstable_param.basic_meta_.index_macro_block_count_       = sstable_meta_.basic_meta_.index_macro_block_count_;
  sstable_param.basic_meta_.sstable_format_version_        = sstable_meta_.basic_meta_.sstable_format_version_;
  sstable_param.basic_meta_.schema_version_                = sstable_meta_.basic_meta_.schema_version_;
  sstable_param.basic_meta_.create_snapshot_version_       = sstable_meta_.basic_meta_.create_snapshot_version_;
  sstable_param.basic_meta_.progressive_merge_round_       = sstable_meta_.basic_meta_.progressive_merge_round_;
  sstable_param.basic_meta_.progressive_merge_step_        = sstable_meta_.basic_meta_.progressive_merge_step_;
  sstable_param.basic_meta_.upper_trans_version_           = sstable_meta_.basic_meta_.upper_trans_version_;
  sstable_param.basic_meta_.ddl_scn_                       = sstable_meta_.basic_meta_.ddl_scn_;
  sstable_param.basic_meta_.filled_tx_scn_                 = sstable_meta_.basic_meta_.filled_tx_scn_;
  sstable_param.basic_meta_.max_merged_trans_version_      = sstable_meta_.basic_meta_.max_merged_trans_version_;
  sstable_param.basic_meta_.table_mode_                    = sstable_meta_.basic_meta_.table_mode_;
  sstable_param.basic_meta_.contain_uncommitted_row_       = sstable_meta_.basic_meta_.contain_uncommitted_row_;
  sstable_param.basic_meta_.root_row_store_type_           = sstable_meta_.basic_meta_.root_row_store_type_;
  sstable_param.basic_meta_.latest_row_store_type_         = sstable_meta_.basic_meta_.latest_row_store_type_;
  sstable_param.table_key_                     = key;
  sstable_param.basic_meta_.compressor_type_               = sstable_meta_.basic_meta_.compressor_type_;
  sstable_param.basic_meta_.encrypt_id_                    = sstable_meta_.basic_meta_.encrypt_id_;
  sstable_param.basic_meta_.master_key_id_                 = sstable_meta_.basic_meta_.master_key_id_;
  MEMCPY(sstable_param.basic_meta_.encrypt_key_, sstable_meta_.basic_meta_.encrypt_key_,
      share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  for (int64_t i = 0; i < sstable_meta_.get_col_checksum_cnt(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sstable_param.column_checksums_.push_back(sstable_meta_.get_col_checksum()[i]));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(sstable_meta_.is_valid());
  ASSERT_TRUE(sstable_param.is_valid());
}

TEST_F(TestLSMigrationParam, test_placeholder_storage_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObStorageSchema storage_schema;
  compaction::ObMediumCompactionInfoList medium_info_list;
  ObTabletFullMemoryMdsData full_memory_mds_data;

  ret = ObMigrationTabletParam::construct_placeholder_storage_schema_and_medium(allocator, storage_schema, medium_info_list, full_memory_mds_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  void *ptr = nullptr;
  ObTablet placeholder_tablet;
  ASSERT_NE(nullptr, ptr = allocator.alloc(sizeof(ObStorageSchema)));
  placeholder_tablet.storage_schema_addr_.ptr_ = new (ptr) ObStorageSchema();
  ret = placeholder_tablet.storage_schema_addr_.get_ptr()->init(allocator, storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);

  placeholder_tablet.tablet_meta_.compat_mode_ = lib::Worker::get_compatibility_mode();
  ASSERT_NE(nullptr, ptr = allocator.alloc(sizeof(ObRowkeyReadInfo)));
  placeholder_tablet.rowkey_read_info_ = new (ptr) ObRowkeyReadInfo();
  placeholder_tablet.build_read_info(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLSMigrationParam, test_migrate_tablet_param)
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
  ret = t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, src_key, allocator_, ls_handle, src_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);

  ObTabletID empty_tablet_id;
  ObTabletTableStoreFlag store_flag;
  store_flag.set_with_major_sstable();
  SCN scn;
  scn.convert_from_ts(ObTimeUtility::current_time());
  ret = src_handle.get_obj()->init_for_first_time_creation(allocator_, src_key.ls_id_, src_key.tablet_id_, src_key.tablet_id_,
      scn, 2022, table_schema,
      lib::Worker::CompatMode::MYSQL, store_flag, nullptr, ls_handle.get_ls()->get_freezer());
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMigrationTabletParam tablet_param;
  ret = src_handle.get_obj()->build_migration_tablet_param(tablet_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet_param.is_valid());

  ObTabletMapKey dst_key;
  dst_key.ls_id_ = TEST_LS_ID;
  dst_key.tablet_id_ = 1003;

  ObTabletHandle dst_handle;
  ret = t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, dst_key, allocator_, ls_handle, dst_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ret = dst_handle.get_obj()->init_with_migrate_param(allocator_, tablet_param, false, ls_handle.get_ls()->get_freezer());
  ASSERT_EQ(common::OB_SUCCESS, ret);

  const ObTabletMeta &src_meta = src_handle.get_obj()->get_tablet_meta();
  const ObTabletMeta &dst_meta = dst_handle.get_obj()->get_tablet_meta();
  LOG_INFO("dump meta", K(src_meta));
  LOG_INFO("dump meta", K(dst_meta));
  ASSERT_TRUE(src_meta.is_valid());
  ASSERT_TRUE(dst_meta.is_valid());

  // check create_schema_version_ in tablet meta/migrate param
  ASSERT_TRUE(table_schema.get_schema_version() == src_meta.create_schema_version_);
  ASSERT_TRUE(table_schema.get_schema_version() == tablet_param.create_schema_version_);
}

TEST_F(TestLSMigrationParam, test_migration_param_compat)
{
  const int64_t buf_len = 4096;
  char buf[buf_len] = {};

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
  ret = t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, src_key, allocator_, ls_handle, src_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);

  ObTabletID empty_tablet_id;
  ObTabletTableStoreFlag store_flag;
  store_flag.set_with_major_sstable();
  SCN scn;
  scn.convert_from_ts(ObTimeUtility::current_time());
  ret = src_handle.get_obj()->init_for_first_time_creation(allocator_, src_key.ls_id_, src_key.tablet_id_, src_key.tablet_id_,
      scn, 2022, table_schema,
      lib::Worker::CompatMode::MYSQL, store_flag, nullptr, ls_handle.get_ls()->get_freezer());
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMigrationTabletParam tablet_param;
  ret = src_handle.get_obj()->build_migration_tablet_param(tablet_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet_param.is_valid());

  // check create_schema_version_ in tablet meta/migrate param
  ASSERT_TRUE(table_schema.get_schema_version() == src_handle.get_obj()->get_tablet_meta().create_schema_version_);
  ASSERT_TRUE(table_schema.get_schema_version() == tablet_param.create_schema_version_);

  tablet_param.ddl_commit_scn_.convert_for_tx(20230403);
  int64_t pos = 0;
  ret = tablet_param.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "cooper", K(tablet_param));

  // normal deserialize
  ObMigrationTabletParam des_param;
  pos = 0;
  ret = des_param.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(des_param.is_valid());
  OB_LOG(INFO, "cooper", K(des_param));
}

TEST_F(TestLSMigrationParam, test_deleted_tablet_info)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  char buf[buf_len] = {};
  share::ObLSID ls_id(1);
  ObTabletID tablet_id(200001);
  ObMigrationTabletParam param;
  ret = param.build_deleted_tablet_info(ls_id, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(param.is_valid());

  int64_t pos = 0;
  ret = param.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  pos = 0;
  ObMigrationTabletParam des_param;
  ret = des_param.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ls_migration_param.log*");
  OB_LOGGER.set_file_name("test_ls_migration_param.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
