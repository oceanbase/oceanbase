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

#ifndef OB_MULTI_VERSION_SSTABLE_TEST_H_
#define OB_MULTI_VERSION_SSTABLE_TEST_H_

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "ob_data_file_prepare.h"
#include "share/ob_device_manager.h"
#include "share/ob_local_device.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/mock_ob_meta_report.h"
#include "storage/mock_disk_usage_report.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "lib/file/file_directory_utils.h"
#include "share/ob_device_manager.h"
#include "share/ob_local_device.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_dml_common.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "../mockcontainer/mock_ob_iterator.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "unittest/storage/mock_ob_table_read_info.h"

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace share::schema;
static ObSimpleMemLimitGetter getter;

namespace storage
{

int init_io_device(const char *test_name,
                   const int64_t macro_block_size = OB_DEFAULT_MACRO_BLOCK_SIZE,
                   const int64_t macro_block_count = 1000)
{
  int ret = OB_SUCCESS;
  char cur_dir[OB_MAX_FILE_NAME_LENGTH];
  char data_dir[OB_MAX_FILE_NAME_LENGTH];
  char file_dir[OB_MAX_FILE_NAME_LENGTH];
  char slog_dir[OB_MAX_FILE_NAME_LENGTH];
  char clog_dir[OB_MAX_FILE_NAME_LENGTH];
  char cmd[OB_MAX_FILE_NAME_LENGTH];
  char dirname[MAX_PATH_SIZE];
  char link_name[MAX_PATH_SIZE];
  ObStorageEnv storage_env;
  const int64_t bucket_num = 1024L;
  const int64_t max_cache_size = 1024L * 1024L * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  const int64_t mem_limit = 10 * 1024L * 1024L * 1024L;
  lib::set_memory_limit(mem_limit);

  if (NULL == getcwd(cur_dir,  OB_MAX_FILE_NAME_LENGTH)) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "cannot get cur dir", K(ret));
  } else if (OB_FAIL(databuff_printf(data_dir, OB_MAX_FILE_NAME_LENGTH, "%s/data_%s", cur_dir, test_name))) {
    STORAGE_LOG(WARN, "failed to gen data dir", K(ret));
  } else if (OB_FAIL(databuff_printf(file_dir, OB_MAX_FILE_NAME_LENGTH, "%s/sstable/", data_dir))) {
    STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(databuff_printf(slog_dir, OB_MAX_FILE_NAME_LENGTH, "%s/slog/", data_dir))) {
    STORAGE_LOG(WARN, "failed to gen slog dir", K(ret));
  } else if (OB_FAIL(databuff_printf(clog_dir, OB_MAX_FILE_NAME_LENGTH, "%s/clog/", data_dir))) {
    STORAGE_LOG(WARN, "failed to gen clog dir", K(ret));
  } else {
    storage_env.data_dir_ = data_dir;
    storage_env.sstable_dir_ = file_dir;
    storage_env.default_block_size_ = macro_block_size;

    storage_env.log_spec_.log_dir_ = slog_dir;
    storage_env.log_spec_.max_log_file_size_ = 64 * 1024 * 1024;

    storage_env.clog_dir_ = clog_dir;

    storage_env.bf_cache_miss_count_threshold_ = 10000;
    storage_env.bf_cache_priority_ = 1;
    storage_env.index_block_cache_priority_ = 10;
    storage_env.user_block_cache_priority_ = 1;
    storage_env.user_row_cache_priority_ = 1;
    storage_env.fuse_row_cache_priority_ = 1;
    storage_env.storage_meta_cache_priority_ = 10;
    storage_env.ethernet_speed_ = 1000000;
    storage_env.redundancy_level_ = ObStorageEnv::NORMAL_REDUNDANCY;

    storage_env.clog_file_spec_.retry_write_policy_ = "normal";
    storage_env.clog_file_spec_.log_create_policy_ = "normal";
    storage_env.clog_file_spec_.log_write_policy_ = "truncate";

    storage_env.slog_file_spec_.retry_write_policy_ = "normal";
    storage_env.slog_file_spec_.log_create_policy_ = "normal";
    storage_env.slog_file_spec_.log_write_policy_ = "truncate";

    storage_env.data_disk_size_ = macro_block_count * macro_block_size;
    GCONF.micro_block_merge_verify_level = 0;
    ObAddr tmp_addr = GCTX.self_addr();
    tmp_addr.set_ip_addr("100.1.2.3", 456);
    GCTX.self_addr_seq_.set_addr(tmp_addr);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObIODeviceWrapper::get_instance().init(
        storage_env.data_dir_,
        storage_env.sstable_dir_,
        storage_env.default_block_size_,
        storage_env.data_disk_percentage_,
        storage_env.data_disk_size_))) {
      STORAGE_LOG(WARN, "init io device fail", K(ret), K(storage_env));
    } else if (OB_FAIL(OB_STORE_CACHE.init(
        storage_env.index_block_cache_priority_,
        storage_env.user_block_cache_priority_,
        storage_env.user_row_cache_priority_,
        storage_env.fuse_row_cache_priority_,
        storage_env.bf_cache_priority_,
        storage_env.bf_cache_miss_count_threshold_,
        storage_env.storage_meta_cache_priority_))) {
      STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(storage_env.data_dir_));
    }
  }
  return ret;

}

class ObMultiVersionSSTableTest : public ::testing::Test
{
public:
  ObMultiVersionSSTableTest(
      const char *test_name,
      const ObMergeType merge_type = MINOR_MERGE,
      const ObRowStoreType row_store_type = FLAT_ROW_STORE);
  virtual ~ObMultiVersionSSTableTest();
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_data(
      ObTableHandleV2 &handle,
      const char **micro_data,
      const int64_t micro_cnt,
      const int64_t rowkey_cnt,
      const ObScnRange &scn_range,
      const int64_t snapshot_version);
public:
  ObITable::TableType get_merged_table_type() const;
  void prepare_table_schema(const char **micro_data, const int64_t schema_rowkey_cnt, const ObScnRange &scn_range, const int64_t snapshot_version);
  void reset_writer(const int64_t snapshot_version);
  void prepare_one_macro(
      const char **micro_data,
      const int64_t micro_cnt,
      const int64_t max_merged_trans_version = INT64_MAX - 2,
      const bool contain_uncommitted = false);
  void prepare_data_end(ObTableHandleV2 &handle, const ObITable::TableType &table_type = ObITable::MINI_SSTABLE);
  void append_micro_block(ObMockIterator &data_iter);

protected:
  static const int64_t MICRO_BLOCK_SIZE = 4 * 1024;
  static const int64_t MACRO_BLOCK_SIZE = 64 * 1024;
  static const int64_t MAX_MICRO_BLOCK_CNT = 100;
  static const int64_t MACRO_BLOCK_COUNT = 1000;
  static const int64_t SCHEMA_VERSION = 10;
  static const int64_t TEST_COLUMN_CNT = 6;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;
  enum LoadDataType
  {
    ALL_DELETE = 0,
    ALL_INSERT = 1,
    MIX_DELETE_WITH_UPDATE = 2,
  };

  static const uint64_t tenant_id_ = 1;
  static const uint64_t tablet_id_ = 50001;
  static const uint64_t table_id_ = 50001;
  static const uint64_t ls_id_ = 1001;

  ObMergeType merge_type_;
  ObTenantFreezeInfoMgr *mgr_;
  ObTableSchema table_schema_;

  ObITable::TableKey table_key_;
  ObDataStoreDesc data_desc_;
  ObDataStoreDesc index_desc_;
  ObMacroBlockWriter macro_writer_;
  ObMicroBlockWriter micro_writer_;
  ObRowStoreType row_store_type_;
  ObSSTableIndexBuilder *root_index_builder_;

  int64_t data_iter_cursor_;
  ObMockIterator data_iter_[MAX_MICRO_BLOCK_CNT];

  // query
  ObFixedArray<ObColDesc, common::ObIAllocator> full_cols_;
  ObTableIterParam iter_param_;
  ObTableAccessContext context_;
  storage::MockObTableReadInfo full_read_info_;
  ObDatumRow datum_row_;
  static ObArenaAllocator allocator_;
  char test_name_[100];

  MockObMetaReport rs_reporter_;
  MockDiskUsageReport disk_reporter_;
};

ObArenaAllocator ObMultiVersionSSTableTest::allocator_;

void ObMultiVersionSSTableTest::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  //OK(init_io_device("multi_version_test"));

  ObIOManager::get_instance().add_tenant_io_manager(
      tenant_id_, ObTenantIOConfig::default_instance());

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(tenant_id_, ObLSID(ls_id_), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void ObMultiVersionSSTableTest::TearDownTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ObLSID(ls_id_), false));
  ObKVGlobalCache::get_instance().destroy();
  //ObIODeviceWrapper::get_instance().destroy();
  OB_STORE_CACHE.destroy();

  MockTenantModuleEnv::get_instance().destroy();
}

ObMultiVersionSSTableTest::ObMultiVersionSSTableTest(
    const char *test_name,
    const ObMergeType merge_type,
    const ObRowStoreType row_store_type)
    : merge_type_(merge_type),
    mgr_(nullptr),
    table_schema_(),
    row_store_type_(row_store_type),
    root_index_builder_(nullptr),
    data_iter_cursor_(0),
    full_read_info_()
{
  memcpy(test_name_, test_name, strlen(test_name));
}

ObMultiVersionSSTableTest::~ObMultiVersionSSTableTest()
{}

void ObMultiVersionSSTableTest::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
}

void ObMultiVersionSSTableTest::TearDown()
{
  if (nullptr != root_index_builder_) {
    root_index_builder_->~ObSSTableIndexBuilder();
    allocator_.free((void *)root_index_builder_);
    root_index_builder_ = nullptr;
  }
}

ObITable::TableType ObMultiVersionSSTableTest::get_merged_table_type() const
{
  ObITable::TableType table_type = ObITable::MAX_TABLE_TYPE;
  if (MAJOR_MERGE == merge_type_) {
    table_type = ObITable::TableType::MAJOR_SSTABLE;
  } else if (MINI_MERGE == merge_type_) {
    table_type = ObITable::TableType::MINI_SSTABLE;
  } else if (META_MAJOR_MERGE == merge_type_) {
    table_type = ObITable::TableType::META_MAJOR_SSTABLE;
  } else if (DDL_KV_MERGE == merge_type_) {
    table_type = ObITable::TableType::DDL_DUMP_SSTABLE;
  } else { // MINOR_MERGE
    table_type = ObITable::TableType::MINOR_SSTABLE;
  }
  return table_type;
}

void ObMultiVersionSSTableTest::prepare_table_schema(
    const char **micro_data,
    const int64_t schema_rowkey_cnt,
    const ObScnRange &scn_range,
    const int64_t snapshot_version)
{
  full_read_info_.reset();
  data_iter_cursor_ = 0;
  for (int64_t i = 0; i < MAX_MICRO_BLOCK_CNT; i++) {
    data_iter_[i].reset();
  }
  OK(data_iter_[0].from(micro_data[0]));

  ObSEArray<ObColDesc, 8> tmp_col_descs;
  int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  int64_t column_cnt = data_iter_[0].get_column_cnt();
  for (int64_t i = 0; i < column_cnt; i++) {
    share::schema::ObColDesc col_desc;
    col_desc.col_type_ = data_iter_[0].get_column_type()[i];
    if (i == schema_rowkey_cnt) {
      col_desc.col_id_ = common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
    } else if (i == schema_rowkey_cnt + 1) {
      col_desc.col_id_ = common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
    } else {
      col_desc.col_id_ = common::OB_APP_MIN_COLUMN_ID + i;
    }
    OK(tmp_col_descs.push_back(col_desc));
  }
  OK(full_read_info_.init(allocator_,
                          column_cnt - extra_rowkey_cnt,
                          schema_rowkey_cnt,
                          lib::is_oracle_mode(),
                          tmp_col_descs));

  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_block"));
  table_schema_.set_tenant_id(tenant_id_);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id_);
  table_schema_.set_rowkey_column_num(full_read_info_.get_schema_rowkey_count());
  table_schema_.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + full_read_info_.get_request_count());
  table_schema_.set_block_size(2 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(FLAT_ROW_STORE);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  ObColumnSchemaV2 column;
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  column_cnt = full_read_info_.get_request_count();
  for(int64_t i = 0; i < tmp_col_descs.count(); ++i) {
    column.reset();
    bool is_rowkey_col = false;
    if (i < schema_rowkey_cnt) {
      is_rowkey_col = true;
      column.set_rowkey_position(i + 1);
    } else if (i == schema_rowkey_cnt || i == schema_rowkey_cnt + 1) {
      continue;
    }
    column.set_table_id(table_id_);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(tmp_col_descs.at(i).col_type_.get_type());
    column.set_column_id(tmp_col_descs.at(i).col_id_);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  column.reset();

  table_key_.table_type_ = get_merged_table_type();
  table_key_.tablet_id_ = tablet_id_;
  table_key_.scn_range_ = scn_range;
  if (MAJOR_MERGE == merge_type_) {
    table_key_.version_range_.snapshot_version_ = snapshot_version;
  }

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObTabletHandle tablet_handle;
  void *ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, ptr = allocator_.alloc(sizeof(ObStorageSchema)));
  tablet->storage_schema_addr_.ptr_ = new (ptr) ObStorageSchema();
  tablet->storage_schema_addr_.get_ptr()->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL);
  ASSERT_NE(nullptr, ptr = allocator_.alloc(sizeof(ObRowkeyReadInfo)));
  tablet->rowkey_read_info_ = new (ptr) ObRowkeyReadInfo();
  tablet->build_read_info(allocator_);
}

void ObMultiVersionSSTableTest::reset_writer(const int64_t snapshot_version)
{
  ObMacroDataSeq start_seq(0);
  start_seq.set_data_block();
  macro_writer_.reset();
  datum_row_.reset();
  datum_row_.init(allocator_, full_read_info_.get_request_count());
  if (nullptr != root_index_builder_) {
    root_index_builder_->~ObSSTableIndexBuilder();
    allocator_.free((void *)root_index_builder_);
    root_index_builder_ = nullptr;
  }

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ASSERT_EQ(OB_SUCCESS, data_desc_.init(table_schema_, ls_id, tablet_id, merge_type_, snapshot_version, 1000000));
  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  root_index_builder_ = new (builder_buf) ObSSTableIndexBuilder();
  ASSERT_NE(nullptr, root_index_builder_);
  data_desc_.sstable_index_builder_ = root_index_builder_;
  data_desc_.need_prebuild_bloomfilter_ = false;
  data_desc_.bloomfilter_rowkey_prefix_ = full_read_info_.get_schema_rowkey_count();
  data_desc_.row_store_type_ = row_store_type_;
  ASSERT_TRUE(data_desc_.is_valid());

  ASSERT_EQ(OB_SUCCESS, index_desc_.init_as_index(table_schema_, ls_id, tablet_id, merge_type_, snapshot_version, DATA_VERSION_4_1_0_0));
  ASSERT_TRUE(index_desc_.is_valid());
  ASSERT_EQ(OB_SUCCESS, root_index_builder_->init(index_desc_));

  ASSERT_EQ(OB_SUCCESS, macro_writer_.open(data_desc_, start_seq));
}

void ObMultiVersionSSTableTest::prepare_one_macro(
    const char **micro_data,
    const int64_t micro_cnt,
    const int64_t max_merged_trans_version,
    const bool contain_uncommitted)
{
  int64_t parsed_micro_cnt = 0;
  ASSERT_TRUE(data_iter_cursor_ < MAX_MICRO_BLOCK_CNT);
  macro_writer_.macro_blocks_[macro_writer_.current_index_].update_max_merged_trans_version(max_merged_trans_version);
  if (contain_uncommitted) {
    macro_writer_.macro_blocks_[macro_writer_.current_index_].set_contain_uncommitted_row();
  }
  if (0 == data_iter_cursor_) {
    macro_writer_.last_key_.set_min_rowkey();
    append_micro_block(data_iter_[0]);
    if (1 == micro_cnt) {
      OK(macro_writer_.build_micro_block());
      OK(macro_writer_.try_switch_macro_block());
    } else {
      OK(macro_writer_.build_micro_block());
    }
    parsed_micro_cnt++;
    data_iter_cursor_++;
  }

  for (; parsed_micro_cnt < micro_cnt && data_iter_cursor_ < MAX_MICRO_BLOCK_CNT;) {
    OK(data_iter_[data_iter_cursor_].from(micro_data[parsed_micro_cnt]));
    append_micro_block(data_iter_[data_iter_cursor_]);
    parsed_micro_cnt++;
    data_iter_cursor_++;
    if (micro_cnt == parsed_micro_cnt) {
      OK(macro_writer_.build_micro_block());
      OK(macro_writer_.try_switch_macro_block());
    } else {
      OK(macro_writer_.build_micro_block());
    }
  }
}

void ObMultiVersionSSTableTest::append_micro_block(ObMockIterator &data_iter)
{
  const ObStoreRow *row = nullptr;
  for (int64_t i = 0; i < data_iter.count(); i++) {
    OK(data_iter.get_row(i, row));
    ASSERT_TRUE(nullptr != row);
    datum_row_.from_store_row(*row);
    ASSERT_EQ(OB_SUCCESS, macro_writer_.append_row(datum_row_));
  }
}

void ObMultiVersionSSTableTest::prepare_data_end(
    ObTableHandleV2 &handle,
    const ObITable::TableType &table_type)
{
  ASSERT_EQ(OB_SUCCESS, macro_writer_.close());
  ObSSTableMergeRes res;
  const int64_t column_cnt =
      table_schema_.get_column_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_EQ(OB_SUCCESS, root_index_builder_->close(res));

  ObTabletCreateSSTableParam param;
  table_key_.table_type_ = table_type;
  param.data_block_ids_ = res.data_block_ids_;
  param.other_block_ids_ = res.other_block_ids_;
  param.table_key_ = table_key_;
  param.schema_version_ = SCHEMA_VERSION;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = table_schema_.get_progressive_merge_round();
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema_.get_table_mode_struct();
  param.index_type_ = table_schema_.get_index_type();
  param.latest_row_store_type_ = table_schema_.get_row_store_type();
  param.rowkey_column_cnt_ = table_schema_.get_rowkey_column_num()
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
                                        param.root_block_addr_, param.root_block_data_);
  ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
                                        param.data_block_macro_meta_addr_, param.data_block_macro_meta_);
  param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
  param.root_row_store_type_ = res.root_row_store_type_;
  param.data_index_tree_height_ = res.root_desc_.height_;
  param.index_blocks_cnt_ = res.index_blocks_cnt_;
  param.data_blocks_cnt_ = res.data_blocks_cnt_;
  param.micro_block_cnt_ = res.micro_block_cnt_;
  param.use_old_macro_block_count_ = 0;
  param.column_cnt_= column_cnt;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.original_size_ = 0;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.nested_size_ = res.nested_size_;
  param.nested_offset_ = res.nested_offset_;
  param.ddl_scn_.set_min();
  if (table_type == ObITable::MAJOR_SSTABLE) {
    ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
  }

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSID lsid(ls_id_);
  OK(ls_svr->get_ls(lsid, ls_handle, ObLSGetMod::STORAGE_MOD));
  void *buf = allocator_.alloc(sizeof(ObSSTable));
  ASSERT_TRUE(nullptr != buf);
  ObSSTable *sstable = new (buf) ObSSTable();
  OK(ObTabletCreateDeleteHelper::create_sstable(param, allocator_, *sstable));
  ObTableReadInfo read_info;
  ObSEArray<share::schema::ObColDesc, 16> cols_desc;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_multi_version_column_descs(cols_desc));
  ASSERT_EQ(OB_SUCCESS, read_info.init(allocator_, table_schema_.get_rowkey_column_num() + 1,
   table_schema_.get_rowkey_column_num(), false, cols_desc, nullptr/*storage_cols_index*/));
  ASSERT_EQ(OB_SUCCESS, handle.set_sstable(sstable, &allocator_));
}

void ObMultiVersionSSTableTest::prepare_data(
    ObTableHandleV2 &handle,
    const char **micro_data,
    const int64_t micro_cnt,
    const int64_t schema_rowkey_cnt,
    const ObScnRange &scn_range,
    const int64_t snapshot_version)
{
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, micro_cnt);
  prepare_data_end(handle);
}

} // end namespace uinttest
} // end namspace oceanbase
#endif // OB_MULTI_VERSION_SSTABLE_TEST_H_
