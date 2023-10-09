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

#ifndef OB_INDEX_BLOCK_DATA_PREPARE_H_
#define OB_INDEX_BLOCK_DATA_PREPARE_H_

#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/scn.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/mock_ls_tablet_service.h"
#include "storage/mock_access_service.h"
#include "storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"

namespace oceanbase
{
namespace palf
{
class SCN;
}

using namespace common;
static ObSimpleMemLimitGetter getter;
namespace blocksstable
{
class TestIndexBlockDataPrepare : public ::testing::Test
{
public:
  TestIndexBlockDataPrepare(
      const char *test_name,
      const ObMergeType merge_type = MAJOR_MERGE,
      const int64_t macro_block_size = OB_DEFAULT_MACRO_BLOCK_SIZE,
      const int64_t macro_block_count = 10000,
      const int64_t max_row_count = 65536,
      const ObRowStoreType row_store_type = ENCODING_ROW_STORE);
  virtual ~TestIndexBlockDataPrepare();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  virtual void prepare_schema();
  virtual void prepare_data();
  virtual void insert_data(ObMacroBlockWriter &data_writer); // override to define data in sstable
  static void convert_to_multi_version_row(const ObDatumRow &org_row, const ObTableSchema &schema,
      const int64_t snapshot_version, const ObDmlFlag dml_flag, ObDatumRow &multi_row);
  static void fake_freeze_info();
  virtual ObITable::TableType get_merged_table_type() const;
  void prepare_query_param(const bool is_reverse_scan, ObArenaAllocator *allocator = nullptr);
  void destroy_query_param();
  void prepare_ddl_kv();
protected:
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t MAX_TEST_COLUMN_CNT = TEST_COLUMN_CNT + 3;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 4;
  static const int64_t TEST_INDEX_BLOCK_COLUMN_CNT = TEST_ROWKEY_COLUMN_CNT + 1;
  static const int64_t DATA_VERSION = 1;
  static const int64_t SNAPSHOT_VERSION = 2;
  static const uint64_t tenant_id_ = 1;
  static const uint64_t tablet_id_ = 50001;
  static const uint64_t TEST_TABLE_ID = 50001;
  static const uint64_t ls_id_ = 1001;
  ObMergeType merge_type_;
  const int64_t max_row_cnt_;
  ObTableSchema table_schema_;
  ObTableSchema index_schema_;
  ObRowGenerate row_generate_;
  int64_t row_cnt_;
  ObSSTable sstable_;
  storage::ObDDLKV ddl_kv_;
  ObSSTableIndexBuilder *root_index_builder_;
  ObMicroBlockData root_block_data_buf_;
  ObRowStoreType row_store_type_;
  int64_t max_row_seed_;
  int64_t min_row_seed_;
  int64_t data_macro_block_cnt_;
  // query
  ObFixedArray<ObColDesc, common::ObIAllocator> schema_cols_;
  ObTableIterParam iter_param_;
  ObTableAccessContext context_;
  ObStoreCtx store_ctx_;
  ObTableReadInfo read_info_;
  static ObArenaAllocator allocator_;
  ObTabletHandle tablet_handle_;
};

ObArenaAllocator TestIndexBlockDataPrepare::allocator_;

void TestIndexBlockDataPrepare::prepare_query_param(const bool is_reverse_scan, ObArenaAllocator *allocator)
{
  ObArenaAllocator *test_allocator = nullptr == allocator ? &allocator_ : allocator;
  schema_cols_.set_allocator(test_allocator);
  schema_cols_.init(table_schema_.get_column_count());
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(schema_cols_));
  iter_param_.table_id_ = table_schema_.get_table_id();
  iter_param_.tablet_id_ = table_schema_.get_table_id();
  ASSERT_EQ(OB_SUCCESS, read_info_.init(
      *test_allocator, 10, table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), schema_cols_, nullptr/*storage_cols_index*/));
  iter_param_.read_info_ = &read_info_;
  //jsut for test
  context_.query_flag_.set_not_use_row_cache();
  context_.query_flag_.set_use_block_cache();
  if (is_reverse_scan) {
    context_.query_flag_.scan_order_ = ObQueryFlag::ScanOrder::Reverse;
  } else {
    context_.query_flag_.scan_order_ = ObQueryFlag::ScanOrder::Forward;
  }
  context_.store_ctx_ = &store_ctx_;
  context_.ls_id_ = ls_id_;
  context_.allocator_ = test_allocator;
  context_.stmt_allocator_ = test_allocator;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestIndexBlockDataPrepare::destroy_query_param()
{
  schema_cols_.reset();
  context_.reset();
  read_info_.reset();
}

TestIndexBlockDataPrepare::TestIndexBlockDataPrepare(
    const char *test_name,
    const ObMergeType merge_type,
    const int64_t macro_block_size,
    const int64_t macro_block_count,
    const int64_t max_row_cnt,
    const ObRowStoreType row_store_type)
  : merge_type_(merge_type),
    max_row_cnt_(max_row_cnt),
    row_cnt_(0),
    root_index_builder_(nullptr),
    row_store_type_(row_store_type),
    max_row_seed_(0),
    min_row_seed_(0),
    data_macro_block_cnt_(0),
    schema_cols_(),
    read_info_()
{
}

TestIndexBlockDataPrepare::~TestIndexBlockDataPrepare()
{
}

void TestIndexBlockDataPrepare::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  ObClockGenerator::init();

  ObIOManager::get_instance().add_tenant_io_manager(
      tenant_id_, ObTenantIOConfig::default_instance());

  fake_freeze_info();

  // create ls
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::create_ls(tenant_id_, ObLSID(ls_id_), ls_handle));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 30007;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));
}

void TestIndexBlockDataPrepare::TearDownTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ObLSID(ls_id_), false));
  ObKVGlobalCache::get_instance().destroy();
  OB_STORE_CACHE.destroy();
  MockTenantModuleEnv::get_instance().destroy();
}

void TestIndexBlockDataPrepare::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  prepare_schema();
  prepare_data();
}

void TestIndexBlockDataPrepare::TearDown()
{
  sstable_.reset();
  ddl_kv_.reset();
  if (nullptr != root_block_data_buf_.buf_) {
    allocator_.free((void *)root_block_data_buf_.buf_);
  }
  if (nullptr != root_index_builder_) {
    root_index_builder_->~ObSSTableIndexBuilder();
    allocator_.free((void *)root_index_builder_);
  }
  allocator_.reuse();
}

void TestIndexBlockDataPrepare::fake_freeze_info()
{
  common::ObArray<ObTenantFreezeInfoMgr::FreezeInfo> freeze_info;
  common::ObArray<share::ObSnapshotInfo> snapshots;

  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr*);

  const int64_t snapshot_gc_ts = 500;
  bool changed = false;

  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(1, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(100, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(200, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(400, 1, 0)));

  ASSERT_EQ(OB_SUCCESS, mgr->update_info(
        snapshot_gc_ts,
        freeze_info,
        snapshots,
        INT64_MAX,
        changed));

}

ObITable::TableType TestIndexBlockDataPrepare::get_merged_table_type() const
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
  } else { // MINOR_MERGE || HISTORY_MINOR_MERGE
    table_type = ObITable::TableType::MINOR_SSTABLE;
  }
  return table_type;
}

void TestIndexBlockDataPrepare::prepare_schema()
{
    ObColumnSchemaV2 column;
  //init table schema
  uint64_t table_id =  TEST_TABLE_ID;
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_block"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(2 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(row_store_type_);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  index_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_index_block"));
  index_schema_.set_tenant_id(1);
  index_schema_.set_tablegroup_id(1);
  index_schema_.set_database_id(1);
  index_schema_.set_table_id(table_id);
  index_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  index_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  index_schema_.set_block_size(2 * 1024);
  index_schema_.set_compress_func_name("none");
  index_schema_.set_row_store_type(row_store_type_);
  index_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(TEST_TABLE_ID);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    bool is_rowkey_col = true;
    if(obj_type == common::ObVarcharType){
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObCharType){
      column.set_rowkey_position(2);
    } else if (obj_type == common::ObDoubleType){
      column.set_rowkey_position(3);
    } else if (obj_type == common::ObNumberType){
      column.set_rowkey_position(4);
    } else {
      column.set_rowkey_position(0);
      is_rowkey_col = false;
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    if (is_rowkey_col) {
      ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));
    }
  }
  column.reset();
  column.set_table_id(TEST_TABLE_ID);
  column.set_column_id(TEST_COLUMN_CNT + OB_APP_MIN_COLUMN_ID);
  ASSERT_EQ(OB_SUCCESS, column.set_column_name("Index block data"));
  column.set_data_type(ObVarcharType);
  column.set_collation_type(CS_TYPE_BINARY);
  column.set_data_length(1);
  column.set_rowkey_position(0);
  ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));

  const uint64_t tenant_id = table_schema_.get_tenant_id();
  for (int64_t i = 0; i < schema_cols_.count(); i++) {
    if (ObHexStringType == schema_cols_.at(i).col_type_.type_) {
      schema_cols_.at(i).col_type_.set_collation_type(CS_TYPE_BINARY);
      schema_cols_.at(i).col_type_.set_collation_level(CS_LEVEL_NUMERIC);
    }
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

void TestIndexBlockDataPrepare::prepare_data()
{
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  start_seq.set_data_block();
  row_generate_.reset();

  ObDataStoreDesc desc;
  ASSERT_EQ(OB_SUCCESS, desc.init(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION));
  desc.schema_version_ = 10;
  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  root_index_builder_ = new (builder_buf) ObSSTableIndexBuilder();
  ASSERT_NE(nullptr, root_index_builder_);
  desc.sstable_index_builder_ = root_index_builder_;
  desc.need_prebuild_bloomfilter_ = true;
  desc.bloomfilter_rowkey_prefix_ = table_schema_.rowkey_column_num_;

  ASSERT_TRUE(desc.is_valid());

  ObDataStoreDesc index_desc;
  ASSERT_EQ(OB_SUCCESS, index_desc.init_as_index(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION, CLUSTER_VERSION_4_1_0_0));
  index_desc.schema_version_ = 10;
  index_desc.need_prebuild_bloomfilter_ = false;
  ASSERT_TRUE(index_desc.is_valid());

  ASSERT_EQ(OB_SUCCESS, root_index_builder_->init(index_desc));
  root_index_builder_->index_store_desc_.need_pre_warm_ = false;  // close index block pre warm

  ASSERT_EQ(OB_SUCCESS, writer.open(desc, start_seq));
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));

  ObITable::TableKey table_key;
  int64_t table_id = 3001;
  int64_t tenant_id = 1;
  table_key.table_type_ = get_merged_table_type();
  table_key.tablet_id_ = table_id;
  table_key.version_range_.snapshot_version_ = SNAPSHOT_VERSION;

  ObTabletCreateSSTableParam param;
  param.table_key_ = table_key;
  param.schema_version_ = 10;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = table_schema_.get_progressive_merge_round();
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema_.get_table_mode_struct();
  param.index_type_ = table_schema_.get_index_type();
  param.rowkey_column_cnt_ = table_schema_.get_rowkey_column_num()
          + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.recycle_version_ = 1;
  param.sstable_logic_seq_ = 1;
  param.is_ready_for_read_ = true;
  int64_t column_cnt = 0;

  table_schema_.get_store_column_count(column_cnt, true);
  column_cnt += ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  sstable_.reset();

  insert_data(writer);
  ASSERT_EQ(OB_SUCCESS, writer.close());

  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ObSSTableMergeRes res;
  ASSERT_EQ(OB_SUCCESS, root_index_builder_->close(res));
  ObIndexTreeRootBlockDesc root_desc;
  root_desc = res.root_desc_;
  ASSERT_TRUE(root_desc.is_valid());
  ObRowStoreType root_row_store_type = res.root_row_store_type_;

  char *root_buf = nullptr;
  int64_t root_size = 0;
  if (root_desc.addr_.is_block()) {
    // read macro block
    ObMacroBlockReadInfo read_info;
    ObMacroBlockHandle macro_handle;
    const int64_t macro_block_size = 2 * 1024 * 1024;
    ASSERT_EQ(OB_SUCCESS, root_desc.addr_.get_block_addr(read_info.macro_block_id_, read_info.offset_, read_info.size_));
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.offset_ = 0;
    read_info.size_ = macro_block_size;
    ASSERT_EQ(OB_SUCCESS, ObBlockManager::read_block(read_info, macro_handle));
    ASSERT_NE(macro_handle.get_buffer(), nullptr);
    ASSERT_EQ(macro_handle.get_data_size(), macro_block_size);
    // get root block
    int64_t block_offset = root_desc.addr_.offset_;
    int64_t block_size = root_desc.addr_.size_;
    const char *block_buf = macro_handle.get_buffer() + block_offset;
    // decompress and decrypt root block
    ObMicroBlockDesMeta meta(ObCompressorType::NONE_COMPRESSOR, 0, 0, nullptr);
    ObMacroBlockReader reader;
    const char *decomp_buf = nullptr;
    int64_t decomp_size = 0;
    bool is_compressed = false;
    ASSERT_EQ(OB_SUCCESS, reader.decrypt_and_decompress_data(meta, block_buf, root_desc.addr_.size_,
        decomp_buf, decomp_size, is_compressed, true, &allocator_));
    root_buf = const_cast<char *>(decomp_buf);
    root_size = decomp_size;
  } else if (root_desc.is_mem_type()) {
    root_buf = root_desc.buf_;
    root_size = root_desc.addr_.size_;
  } else {
    STORAGE_LOG(INFO, "not supported root block", K(root_desc));
    ASSERT_TRUE(false);
  }

  // data write ctx has been moved to root_index_builder
  ASSERT_EQ(writer.get_macro_block_write_ctx().get_macro_block_count(), 0);
  data_macro_block_cnt_ = root_index_builder_->roots_[0]->data_write_ctx_->get_macro_block_count();
  ASSERT_GE(data_macro_block_cnt_, 0);

  ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
    param.root_block_addr_, param.root_block_data_);
  ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
    param.data_block_macro_meta_addr_, param.data_block_macro_meta_);
  param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.row_count_ = res.row_count_;
  param.root_row_store_type_ = root_row_store_type;
  param.latest_row_store_type_ = table_schema_.get_row_store_type();
  param.data_index_tree_height_ = root_desc.height_;
  param.index_blocks_cnt_ = res.index_blocks_cnt_;
  param.data_blocks_cnt_ = res.data_blocks_cnt_;
  param.micro_block_cnt_ = res.micro_block_cnt_;
  param.use_old_macro_block_count_ = res.use_old_macro_block_count_;
  param.column_cnt_= column_cnt;
  param.data_checksum_ = res.data_checksum_;
  param.occupy_size_ = res.occupy_size_;
  param.original_size_ = res.original_size_;
  param.nested_offset_ = res.nested_offset_;
  param.nested_size_ = res.nested_size_;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.data_block_ids_ = res.data_block_ids_;
  param.other_block_ids_ = res.other_block_ids_;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_.set_min();
  param.contain_uncommitted_row_ = false;
  param.encrypt_id_ = res.encrypt_id_;
  param.master_key_id_ = res.master_key_id_;
  MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  if (merge_type_ == MAJOR_MERGE) {
    ASSERT_EQ(OB_SUCCESS, param.column_checksums_.assign(res.data_column_checksums_));
  }
  ASSERT_EQ(OB_SUCCESS, sstable_.init(param, &allocator_));
  STORAGE_LOG(INFO, "create sstable param", K(param));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  prepare_ddl_kv();

  root_block_data_buf_.buf_ = root_buf;
  root_block_data_buf_.size_ = root_size;
  row_store_type_ = root_row_store_type;
}

void TestIndexBlockDataPrepare::prepare_ddl_kv()
{
  ddl_kv_.reset();
  ObTabletHandle tablet_handle;
  int ret = OB_SUCCESS;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  share::SCN ddl_start_scn;
  ddl_start_scn.convert_from_ts(ObTimeUtility::current_time());
  ASSERT_EQ(OB_SUCCESS, ddl_kv_.init(*tablet_handle.get_obj(), ddl_start_scn, sstable_.get_data_version(), ddl_start_scn, 4000));

  SMART_VAR(ObSSTableSecMetaIterator, meta_iter) {
    ObDatumRange query_range;
    query_range.set_whole_range();
    ObDataMacroBlockMeta data_macro_meta;
    ASSERT_EQ(OB_SUCCESS, meta_iter.open(query_range,
                                         ObMacroBlockMetaType::DATA_BLOCK_META,
                                         sstable_,
                                         tablet_handle.get_obj()->get_rowkey_read_info(),
                                         allocator_));

    while (OB_SUCC(ret)) {
      if (OB_FAIL(meta_iter.get_next(data_macro_meta))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "get data macro meta failed", K(ret));
        }
      } else {
        ObDDLMacroHandle macro_handle;
        macro_handle.set_block_id(data_macro_meta.get_macro_id());
        ObDataMacroBlockMeta *copied_meta = nullptr;
        ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(copied_meta, allocator_));
        ASSERT_EQ(OB_SUCCESS, ddl_kv_.insert_block_meta_tree(macro_handle, copied_meta));
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(OB_SUCCESS, ddl_kv_.block_meta_tree_.build_sorted_rowkeys());
  }
}

void TestIndexBlockDataPrepare::insert_data(ObMacroBlockWriter &data_writer)
{
  row_cnt_ = 0;

  int64_t seed = min_row_seed_;
  ObDatumRow row;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag flags[] = {DF_INSERT, DF_UPDATE, DF_DELETE};

  while (true) {
    if (row_cnt_ >= max_row_cnt_) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
    ++seed;

    ObDmlFlag dml = flags[row_cnt_ % ARRAYSIZEOF(flags)]; // INSERT / UPDATE / DELETE
    convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
    ++row_cnt_;
  }

  max_row_seed_ = seed - 1;
}

void TestIndexBlockDataPrepare::convert_to_multi_version_row(const ObDatumRow &org_row,
    const ObTableSchema &schema, const int64_t snapshot_version, const ObDmlFlag dml_flag,
    ObDatumRow &multi_row)
{
  int64_t i  = 0;
  for (; i < schema.get_rowkey_column_num(); ++i) {
    multi_row.storage_datums_[i] = org_row.storage_datums_[i];
  }
  multi_row.storage_datums_[i++].set_int(-snapshot_version);
  multi_row.storage_datums_[i++].set_int(0);
  for(int64_t j = schema.get_rowkey_column_num(); j < schema.get_column_count(); ++i,++j) {
    multi_row.storage_datums_[i] = org_row.storage_datums_[j];
  }
  multi_row.count_ = i;

  if (ObDmlFlag::DF_DELETE == dml_flag) {
    multi_row.row_flag_ = ObDmlFlag::DF_DELETE;
  } else {
    multi_row.row_flag_ = ObDmlFlag::DF_INSERT;
  }
  multi_row.mvcc_row_flag_.reset();
  multi_row.mvcc_row_flag_.set_last_multi_version_row(true);
}

} // namespace blocksstable
} // namespace oceanbase

#endif // OB_INDEX_BLOCK_DATA_PREPARE_H_
