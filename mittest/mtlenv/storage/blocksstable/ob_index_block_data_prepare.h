/**
 * Copyright (c) 2023 OceanBase
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
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
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
#include "storage/ddl/ob_ddl_merge_task.h"

namespace oceanbase
{

using namespace common;
using namespace compaction;
static ObSimpleMemLimitGetter getter;
namespace blocksstable
{
class TestIndexBlockDataPrepare : public ::testing::Test
{
public:
  TestIndexBlockDataPrepare(
      const char *test_name,
      const ObMergeType merge_type = MAJOR_MERGE,
      const bool need_aggregate_data = false,
      const int64_t macro_block_size = OB_DEFAULT_MACRO_BLOCK_SIZE,
      const int64_t macro_block_count = 10000,
      const int64_t max_row_count = 65536,
      const ObRowStoreType row_store_type = ENCODING_ROW_STORE,
      const int64_t rows_per_mirco_block = INT64_MAX,
      const int64_t mirco_blocks_per_macro_block = INT64_MAX);
  virtual ~TestIndexBlockDataPrepare();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  virtual void prepare_schema();
  virtual void prepare_data(const int64_t micro_block_size = 0);
  virtual void prepare_partial_ddl_data();
  virtual void prepare_partial_cg_data();
  virtual void prepare_cg_data();
  virtual void insert_data(ObMacroBlockWriter &data_writer); // override to define data in sstable
  virtual void insert_cg_data(ObMacroBlockWriter &data_writer); // override to define data in sstable
  virtual void insert_partial_data(ObMacroBlockWriter &data_writer, const int64_t row_cnt); // override to define data in partial_sstable
  static void convert_to_multi_version_row(const ObDatumRow &org_row, const ObTableSchema &schema,
      const int64_t snapshot_version, const ObDmlFlag dml_flag, ObDatumRow &multi_row);
  static void fake_freeze_info();
  virtual ObITable::TableType get_merged_table_type() const;
  void prepare_query_param(const bool is_reverse_scan, ObArenaAllocator *allocator = nullptr);
  void destroy_query_param();
  void prepare_contrastive_sstable();
  void prepare_ddl_kv();
  void prepare_merge_ddl_kvs();
  void close_builder_and_prepare_sstable(const int64_t column_cnt);
  void prepare_partial_sstable(const int64_t column_cnt);
  int prepare_cg_read_info(const ObColDesc &col_desc);
  int gen_create_tablet_arg(const int64_t tenant_id,
                            const share::ObLSID &ls_id,
                            const ObTabletID &tablet_id,
                            obrpc::ObBatchCreateTabletArg &arg,
                            share::schema::ObTableSchema &table_schema,
                            const int64_t count = 1);
protected:
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t MAX_TEST_COLUMN_CNT = TEST_COLUMN_CNT + 3;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 4;
  static const int64_t TEST_INDEX_BLOCK_COLUMN_CNT = TEST_ROWKEY_COLUMN_CNT + 1;
  static const int64_t DATA_VERSION = 1;
  static const int64_t SNAPSHOT_VERSION = 2;
  static const uint64_t tenant_id_ = 1;
  static const uint64_t tablet_id_ = 200001;
  static const uint64_t TEST_TABLE_ID = 200001;
  static const uint64_t ls_id_ = 1001;
  static const int64_t DDL_KVS_CNT = 3;
  ObMergeType merge_type_;
  int64_t max_row_cnt_;
  int64_t max_partial_row_cnt_;
  int64_t co_sstable_row_offset_;
  int64_t partial_kv_start_idx_;
  ObTableSchema table_schema_;
  ObTableSchema index_schema_;
  ObRowGenerate row_generate_;
  int64_t row_cnt_;
  int64_t partial_sstable_row_cnt_;
  ObSSTable partial_sstable_;
  ObSSTable sstable_;
  storage::ObDDLMemtable ddl_kv_;
  storage::ObDDLKVHandle ddl_kvs_;
  ObDDLKV *ddl_kv_ptr_;
  ObSSTableIndexBuilder *root_index_builder_;
  ObSSTableIndexBuilder *merge_root_index_builder_;
  ObMicroBlockData root_block_data_buf_;
  ObMicroBlockData merge_root_block_data_buf_;
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
  ObFixedArray<ObSkipIndexColMeta, common::ObIAllocator> agg_col_metas_;
  bool need_agg_data_;
  ObTableReadInfo cg_read_info_;
  ObDatumRowkey start_key_;
  ObDatumRowkey end_key_;
  int64_t rows_per_mirco_block_;
  int64_t mirco_blocks_per_macro_block_;
  bool is_cg_data_;
  bool is_ddl_merge_data_;

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
  read_info_.reset();
  ASSERT_EQ(OB_SUCCESS, read_info_.init(
      *test_allocator, 10, table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), schema_cols_, nullptr/*storage_cols_index*/));
  iter_param_.read_info_ = &read_info_;
  iter_param_.has_lob_column_out_ = false;
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
  context_.tablet_id_ = tablet_id_;
  context_.allocator_ = test_allocator;
  context_.stmt_allocator_ = test_allocator;
  context_.limit_param_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, context_.micro_block_handle_mgr_.init(false /* disable limit */, context_.table_store_stat_, context_.query_flag_));
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
    const bool need_aggregate_data,
    const int64_t macro_block_size,
    const int64_t macro_block_count,
    const int64_t max_row_cnt,
    const ObRowStoreType row_store_type,
    const int64_t rows_per_mirco_block,
    const int64_t mirco_blocks_per_macro_block)
  : merge_type_(merge_type),
    max_row_cnt_(max_row_cnt),
    co_sstable_row_offset_(0),
    row_cnt_(0),
    partial_sstable_row_cnt_(0),
    root_index_builder_(nullptr),
    merge_root_index_builder_(nullptr),
    row_store_type_(row_store_type),
    max_row_seed_(0),
    min_row_seed_(0),
    data_macro_block_cnt_(0),
    schema_cols_(),
    read_info_(),
    agg_col_metas_(&allocator_),
    need_agg_data_(need_aggregate_data),
    rows_per_mirco_block_(rows_per_mirco_block),
    mirco_blocks_per_macro_block_(mirco_blocks_per_macro_block),
    is_cg_data_(false),
    is_ddl_merge_data_(false)
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

  fake_freeze_info();

  // create ls
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::create_ls(tenant_id_, ObLSID(ls_id_), ls_handle));
}

void TestIndexBlockDataPrepare::TearDownTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ObLSID(ls_id_)));
  ObKVGlobalCache::get_instance().destroy();
  OB_STORE_CACHE.destroy();
  MockTenantModuleEnv::get_instance().destroy();
}

int TestIndexBlockDataPrepare::gen_create_tablet_arg(const int64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    obrpc::ObBatchCreateTabletArg &arg,
    share::schema::ObTableSchema &table_schema,
    const int64_t count)
{
  int ret = OB_SUCCESS;
  obrpc::ObCreateTabletInfo tablet_info;
  ObArray<common::ObTabletID> index_tablet_ids;
  ObArray<int64_t> index_tablet_schema_idxs;
  arg.reset();

  for(int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObTabletID tablet_id_insert(tablet_id.id() + i);
    if (OB_FAIL(index_tablet_ids.push_back(tablet_id_insert))) {
      STORAGE_LOG(WARN, "failed to push back tablet id", KR(ret), K(tablet_id_insert));
    } else if (OB_FAIL(index_tablet_schema_idxs.push_back(0))) {
      STORAGE_LOG(WARN, "failed to push back index id", KR(ret));
    }
  }


  if (FAILEDx(tablet_info.init(index_tablet_ids,
          tablet_id,
          index_tablet_schema_idxs,
          lib::Worker::CompatMode::MYSQL,
          false))) {
    STORAGE_LOG(WARN, "failed to init tablet info", KR(ret), K(index_tablet_ids),
        K(tablet_id), K(index_tablet_schema_idxs));
  } else if (OB_FAIL(arg.init_create_tablet(ls_id, share::SCN::min_scn(), false))) {
    STORAGE_LOG(WARN, "failed to init create tablet", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(arg.table_schemas_.push_back(table_schema))) {
    STORAGE_LOG(WARN, "failed to push back table schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(arg.tablets_.push_back(tablet_info))) {
    STORAGE_LOG(WARN, "failed to push back tablet info", KR(ret), K(tablet_info));
  }
  return ret;
}

void TestIndexBlockDataPrepare::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  prepare_schema();
  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(tenant_id_, ls_id, tablet_id, create_tablet_arg, table_schema_, 1));

  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();

  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema_, allocator_));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
  iter_param_.set_tablet_handle(&tablet_handle_);
  sstable_.key_.table_type_ = ObITable::TableType::COLUMN_ORIENTED_SSTABLE;

  if (is_cg_data_ && is_ddl_merge_data_) {
    prepare_partial_cg_data();
    partial_sstable_.key_.table_type_ = ObITable::TableType::DDL_MERGE_CG_SSTABLE;
  } else if (is_cg_data_) {
    prepare_cg_data();
  } else if (is_ddl_merge_data_) {
    prepare_partial_ddl_data();
    partial_sstable_.key_.table_type_ = ObITable::TableType::DDL_MERGE_CO_SSTABLE;
  } else {
    prepare_data();
  }
}
void TestIndexBlockDataPrepare::TearDown()
{
  sstable_.reset();
  partial_sstable_.reset();
  ddl_kv_.reset();
  ddl_kvs_.reset();
  ddl_kv_ptr_ = nullptr;
  cg_read_info_.reset();
  if (nullptr != root_block_data_buf_.buf_) {
    allocator_.free((void *)root_block_data_buf_.buf_);
    root_block_data_buf_.buf_ = nullptr;
  }
  if (nullptr != root_block_data_buf_.buf_) {
    allocator_.free((void *)root_block_data_buf_.buf_);
    root_block_data_buf_.buf_ = nullptr;
  }
  if (nullptr != merge_root_block_data_buf_.buf_) {
    allocator_.free((void *)merge_root_block_data_buf_.buf_);
    merge_root_block_data_buf_.buf_ = nullptr;
  }
  if (nullptr != root_index_builder_) {
    root_index_builder_->~ObSSTableIndexBuilder();
    allocator_.free((void *)root_index_builder_);
    root_index_builder_ = nullptr;
  }
  if (nullptr != merge_root_index_builder_) {
    merge_root_index_builder_->~ObSSTableIndexBuilder();
    allocator_.free((void *)merge_root_index_builder_);
    merge_root_index_builder_ = nullptr;
  }
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();
  ls_tablet_svr->delete_all_tablets();
  allocator_.reuse();
}

void TestIndexBlockDataPrepare::fake_freeze_info()
{
  common::ObArray<share::ObSnapshotInfo> snapshots;

  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr*);
  share::ObFreezeInfoList &info_list = mgr->freeze_info_mgr_.freeze_info_;
  info_list.reset();

  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 100;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 200;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 400;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  info_list.latest_snapshot_gc_scn_.val_ = 500;
}

ObITable::TableType TestIndexBlockDataPrepare::get_merged_table_type() const
{
  ObITable::TableType table_type = ObITable::MAX_TABLE_TYPE;
  if (is_major_merge_type(merge_type_)) {
    table_type = ObITable::TableType::MAJOR_SSTABLE;
  } else if (MINI_MERGE == merge_type_) {
    table_type = ObITable::TableType::MINI_SSTABLE;
  } else if (META_MAJOR_MERGE == merge_type_) {
    table_type = ObITable::TableType::META_MAJOR_SSTABLE;
  } else if (DDL_KV_MERGE == merge_type_) {
    table_type = ObITable::TableType::DDL_MERGE_CO_SSTABLE;
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

    share::schema::ObSkipIndexColumnAttr skip_idx_attr;
    if (need_agg_data_ && !is_lob_storage(obj_type)) {
      skip_idx_attr.set_min_max();
      column.set_skip_index_attr(skip_idx_attr.get_packed_value());
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
}

void TestIndexBlockDataPrepare::close_builder_and_prepare_sstable(const int64_t column_cnt)
{
  ObSSTableMergeRes res;
  OK(root_index_builder_->close(res));
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
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    ASSERT_NE(nullptr, read_info.buf_ = reinterpret_cast<char*>(allocator_.alloc(read_info.size_)));
    ASSERT_EQ(OB_SUCCESS, ObBlockManager::read_block(read_info, macro_handle));
    ASSERT_NE(macro_handle.get_buffer(), nullptr);
    ASSERT_EQ(macro_handle.get_data_size(), macro_block_size);
    // get root block
    int64_t block_offset = root_desc.addr_.offset_;
    int64_t block_size = root_desc.addr_.size_;
    const char *block_buf = macro_handle.get_buffer() + block_offset;
    // decompress and decrypt root block
    ObMicroBlockDesMeta meta(ObCompressorType::NONE_COMPRESSOR, root_row_store_type, 0, 0, nullptr);
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

  // deserialize micro block header in root block buf
  ObMicroBlockHeader root_micro_header;
  int64_t des_pos = 0;
  ASSERT_EQ(OB_SUCCESS, root_micro_header.deserialize(root_buf, root_size, des_pos));
  root_block_data_buf_.buf_ = static_cast<char *>(allocator_.alloc(root_size));
  root_block_data_buf_.size_ = root_size;
  int64_t copy_pos = 0;
  ObMicroBlockHeader *copied_micro_header = nullptr;
  ASSERT_EQ(OB_SUCCESS, root_micro_header.deep_copy(
      (char *)root_block_data_buf_.buf_, root_block_data_buf_.size_, copy_pos, copied_micro_header));
  ASSERT_TRUE(copied_micro_header->is_valid());
  MEMCPY((char *)(root_block_data_buf_.buf_ + copy_pos), root_buf + des_pos, root_size - des_pos);
  row_store_type_ = root_row_store_type;

  ObITable::TableKey table_key;
  int64_t tenant_id = 1;
  table_key.table_type_ = get_merged_table_type();
  table_key.tablet_id_ = tablet_id_;
  table_key.version_range_.snapshot_version_ = SNAPSHOT_VERSION;

  ObTabletCreateSSTableParam param;
  param.table_key_ = table_key;
  param.schema_version_ = 10;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = table_schema_.get_progressive_merge_round();
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema_.get_table_mode_struct();
  param.index_type_ = table_schema_.get_index_type();
  if (is_cg_data_) {
    param.rowkey_column_cnt_ = 0;
  } else {
    param.rowkey_column_cnt_ = table_schema_.get_rowkey_column_num()
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }

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
  ASSERT_EQ(OB_SUCCESS, param.data_block_ids_.assign(res.data_block_ids_));
  ASSERT_EQ(OB_SUCCESS, param.other_block_ids_.assign(res.other_block_ids_));
  param.ddl_scn_.set_min();
  param.filled_tx_scn_.set_min();
  param.contain_uncommitted_row_ = false;
  param.encrypt_id_ = res.encrypt_id_;
  param.master_key_id_ = res.master_key_id_;
  MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  if (merge_type_ == MAJOR_MERGE) {
    OK(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
  }
  sstable_.reset();
  ASSERT_EQ(OB_SUCCESS, sstable_.init(param, &allocator_));
  STORAGE_LOG(INFO, "create sstable param", K(param));

}

void TestIndexBlockDataPrepare::prepare_data(const int64_t micro_block_size)
{
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  start_seq.set_data_block();
  row_generate_.reset();
  share::SCN scn;
  scn.convert_for_tx(SNAPSHOT_VERSION);
  ObWholeDataStoreDesc desc;
  ASSERT_EQ(OB_SUCCESS, desc.init(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION,
                                  DATA_CURRENT_VERSION, scn));
  desc.get_desc().static_desc_->schema_version_ = 10;
  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  root_index_builder_ = new (builder_buf) ObSSTableIndexBuilder();
  ASSERT_NE(nullptr, root_index_builder_);
  desc.get_desc().sstable_index_builder_ = root_index_builder_;

  ASSERT_TRUE(desc.is_valid());

  // ObDataStoreDesc index_desc;
  // ASSERT_EQ(OB_SUCCESS, index_desc.init(index_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION));
  // index_desc.schema_version_ = 10;
  // ASSERT_TRUE(index_desc.is_valid());

  if (need_agg_data_) {
    ASSERT_EQ(OB_SUCCESS, desc.get_desc().col_desc_->agg_meta_array_.assign(agg_col_metas_));
    // ASSERT_EQ(OB_SUCCESS, index_desc.agg_meta_array_.assign(agg_col_metas_));
  }

  ASSERT_EQ(OB_SUCCESS, root_index_builder_->init(desc.get_desc()));
  if (micro_block_size > 0) {
    root_index_builder_->index_store_desc_.get_desc().micro_block_size_ = 500;
  }

  ASSERT_EQ(OB_SUCCESS, writer.open(desc.get_desc(), start_seq));
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));

  insert_data(writer);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  // data write ctx has been moved to root_index_builder
  ASSERT_EQ(writer.get_macro_block_write_ctx().get_macro_block_count(), 0);
  data_macro_block_cnt_ = root_index_builder_->roots_[0]->macro_metas_->count();
  ASSERT_GE(data_macro_block_cnt_, 0);

  const int64_t column_cnt =
    table_schema_.get_column_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  close_builder_and_prepare_sstable(column_cnt);
  prepare_ddl_kv();
}

int TestIndexBlockDataPrepare::prepare_cg_read_info(const ObColDesc &col_desc)
{
  int ret = OB_SUCCESS;
  cg_read_info_.reset();
  if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->construct_cg_read_info(
              allocator_, lib::is_oracle_mode(), col_desc, nullptr, cg_read_info_))) {
    STORAGE_LOG(WARN, "Fail to construct cg read info", K(ret));
  }
  return ret;
}

void TestIndexBlockDataPrepare::prepare_cg_data()
{
  // need_aggregate_data is ignored temporarily
  ObMacroBlockWriter data_writer;
  ObMacroDataSeq start_seq(0);
  start_seq.set_data_block();
  row_generate_.reset();

  // get IntType column
  uint16_t cg_cols[1];
  ObWholeDataStoreDesc desc;
  share::SCN scn;
  scn.convert_for_tx(SNAPSHOT_VERSION);
  ASSERT_EQ(OB_SUCCESS, desc.init(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION, DATA_CURRENT_VERSION, scn));
  ObIArray<ObColDesc> &col_descs = desc.get_desc().col_desc_->col_desc_array_;
  for (int64_t i = 0; i < col_descs.count(); ++i) {
    if (col_descs.at(i).col_type_.type_ == ObIntType) {
      cg_cols[0] = i;
    }
  }

  storage::ObStorageColumnGroupSchema cg_schema;
  cg_schema.type_ = share::schema::SINGLE_COLUMN_GROUP;
  cg_schema.compressor_type_ = table_schema_.get_compressor_type();
  cg_schema.row_store_type_ = table_schema_.get_row_store_type();
  cg_schema.block_size_ = table_schema_.get_block_size();
  cg_schema.column_cnt_ = 1;
  cg_schema.schema_column_cnt_ = table_schema_.get_column_count();
  cg_schema.column_idxs_ = cg_cols;

  ASSERT_EQ(merge_type_, ObMergeType::MAJOR_MERGE);
  ObWholeDataStoreDesc data_desc;
  OK(data_desc.init(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_),
                    merge_type_, SNAPSHOT_VERSION, DATA_CURRENT_VERSION,
                    scn, &cg_schema, 0));
  data_desc.get_desc().static_desc_->schema_version_ = 10;
  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  root_index_builder_ = new (builder_buf) ObSSTableIndexBuilder();
  ASSERT_NE(nullptr, root_index_builder_);
  data_desc.get_desc().sstable_index_builder_ = root_index_builder_;

  OK(prepare_cg_read_info(data_desc.get_desc().get_col_desc_array().at(0)));

  ASSERT_TRUE(data_desc.is_valid());

  OK(root_index_builder_->init(data_desc.get_desc()));
  OK(data_writer.open(data_desc.get_desc(), start_seq));
  OK(row_generate_.init(table_schema_, &allocator_));
  insert_cg_data(data_writer);
  OK(data_writer.close());
  // data write ctx has been moved to root_index_builder
  ASSERT_EQ(data_writer.get_macro_block_write_ctx().get_macro_block_count(), 0);
  data_macro_block_cnt_ = root_index_builder_->roots_[0]->macro_metas_->count();
  ASSERT_GE(data_macro_block_cnt_, 0);

  const int64_t column_cnt = data_desc.get_desc().get_row_column_count();
  close_builder_and_prepare_sstable(column_cnt);
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
  ASSERT_EQ(OB_SUCCESS, ddl_kv_.init(allocator_, *tablet_handle.get_obj(), sstable_.get_key(), ddl_start_scn, DATA_CURRENT_VERSION));

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
        ASSERT_EQ(OB_SUCCESS, ddl_kv_.insert_block_meta_tree(macro_handle, copied_meta, 0));
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
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

  int64_t rows_per_mirco_block = rows_per_mirco_block_;
  int64_t rows_per_macro_block = rows_per_mirco_block_ * mirco_blocks_per_macro_block_;
  int64_t rows_cnt = max_row_cnt_;
  if (INT64_MAX == rows_per_mirco_block_ || INT64_MAX == mirco_blocks_per_macro_block_) {
    rows_per_mirco_block = INT64_MAX;
    rows_per_macro_block = INT64_MAX;
  }

  while (true) {
    if (row_cnt_ >= max_row_cnt_) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
    ++seed;

    ObDmlFlag dml = flags[row_cnt_ % ARRAYSIZEOF(flags)]; // INSERT / UPDATE / DELETE
    convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
    if (row_cnt_ == 0) {
      ObDatumRowkey &start_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, start_key.deep_copy(start_key_, allocator_));
    }
    if (row_cnt_ == max_row_cnt_ - 1) {
      ObDatumRowkey &end_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, end_key.deep_copy(end_key_, allocator_));
    }
    if ((row_cnt_ + 1) % rows_per_mirco_block == 0) {
      OK(data_writer.build_micro_block());
    }
    if ((row_cnt_ + 1) % rows_per_macro_block == 0) {
      OK(data_writer.try_switch_macro_block());
    }
    ++row_cnt_;
  }

  max_row_seed_ = seed - 1;
}

void TestIndexBlockDataPrepare::insert_cg_data(ObMacroBlockWriter &data_writer)
{
  row_cnt_ = 0;
  const int64_t test_row_cnt = 10000;
  // 10 rows per micro block; 10 blocks per macro block; 10 macro blocks.
  ObDatumRow cg_row;
  OK(cg_row.init(1));
  ObDmlFlag flags[] = {DF_INSERT, DF_UPDATE, DF_DELETE};

  while (true) {
    if (row_cnt_ >= test_row_cnt) {
      break;
    }
    cg_row.storage_datums_[0].set_int(row_cnt_);
    OK(data_writer.append_row(cg_row));
    if ((row_cnt_ + 1) % 10 == 0) {
      OK(data_writer.build_micro_block());
    }
    if ((row_cnt_ + 1) % 100 == 0) {
      OK(data_writer.try_switch_macro_block());
    }
    ++row_cnt_;
  }
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

void TestIndexBlockDataPrepare::prepare_partial_ddl_data()
{
  prepare_contrastive_sstable();
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  start_seq.set_data_block();
  row_generate_.reset();
  ObWholeDataStoreDesc desc(true/*is ddl*/);
  share::SCN end_scn;
  end_scn.convert_from_ts(ObTimeUtility::current_time());
  ASSERT_EQ(OB_SUCCESS, desc.init(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION, CLUSTER_CURRENT_VERSION, end_scn));
  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  merge_root_index_builder_ = new (builder_buf) ObSSTableIndexBuilder();
  ASSERT_NE(nullptr, merge_root_index_builder_);
  desc.get_desc().sstable_index_builder_ = merge_root_index_builder_;
  ASSERT_TRUE(desc.is_valid());
  if (need_agg_data_) {
    ASSERT_EQ(OB_SUCCESS, desc.get_desc().col_desc_->agg_meta_array_.assign(agg_col_metas_));
  }
  ASSERT_EQ(OB_SUCCESS, merge_root_index_builder_->init(desc.get_desc()));
  ASSERT_EQ(OB_SUCCESS, writer.open(desc.get_desc(), start_seq));
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));
  const int64_t partial_row_cnt = max_partial_row_cnt_;
  insert_partial_data(writer, partial_row_cnt);
  ASSERT_EQ(OB_SUCCESS, writer.close());
  // data write ctx has been moved to merge_root_index_builder_
  ASSERT_EQ(writer.get_macro_block_write_ctx().get_macro_block_count(), 0);
  data_macro_block_cnt_ = merge_root_index_builder_->roots_[0]->macro_metas_->count();
  ASSERT_GE(data_macro_block_cnt_, 0);
  int64_t column_cnt = 0;
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_EQ(OB_SUCCESS, storage_schema->get_stored_column_count_in_sstable(column_cnt));
  prepare_partial_sstable(column_cnt);
  prepare_merge_ddl_kvs();

  ObArray<blocksstable::ObSSTable *> ddl_tables;
  ASSERT_EQ(OB_SUCCESS, ddl_tables.push_back(&partial_sstable_));
  for (int64_t i = 0; i < DDL_KVS_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, ddl_tables.push_back(ddl_kvs_.get_obj()->get_ddl_memtables().at(i)));
  }
  share::SCN ddl_start_scn;
  ddl_start_scn.convert_from_ts(ObTimeUtility::current_time());
  ObTabletDDLParam ddl_param;
  ddl_param.table_key_ = partial_sstable_.key_;
  ddl_param.data_format_version_ = DATA_VERSION_4_3_0_0;
  ddl_param.start_scn_ = ddl_start_scn;
  ddl_param.commit_scn_ = ddl_start_scn;
  ddl_param.direct_load_type_ = DIRECT_LOAD_DDL;
  ddl_param.ls_id_ = ls_id_;
  ddl_param.snapshot_version_ = SNAPSHOT_VERSION;
  ObArray<ObDDLBlockMeta> sorted_metas;
  ObArenaAllocator arena("compact_test", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ObTabletDDLUtil::get_compact_meta_array(
                                         *tablet_handle.get_obj(),
                                         ddl_tables,
                                         ddl_param,
                                         tablet_handle.get_obj()->get_rowkey_read_info(),
                                         storage_schema,
                                         arena,
                                         sorted_metas));
  if (co_sstable_row_offset_ != 0) {
    ASSERT_EQ(sorted_metas.at(2).end_row_offset_, co_sstable_row_offset_);
  }
  arena.reset();
  ObTabletObjLoadHelper::free(allocator_, storage_schema);
}

void TestIndexBlockDataPrepare::prepare_partial_cg_data()
{
  prepare_contrastive_sstable();
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  start_seq.set_data_block();
  row_generate_.reset();
  ObWholeDataStoreDesc desc(true/*is ddl*/);
  share::SCN end_scn;
  end_scn.convert_from_ts(ObTimeUtility::current_time());
  ASSERT_EQ(OB_SUCCESS, desc.init(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION, CLUSTER_CURRENT_VERSION, end_scn));
  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  merge_root_index_builder_ = new (builder_buf) ObSSTableIndexBuilder();
  ASSERT_NE(nullptr, merge_root_index_builder_);
  desc.get_desc().sstable_index_builder_ = merge_root_index_builder_;
  ASSERT_TRUE(desc.is_valid());
  if (need_agg_data_) {
    ASSERT_EQ(OB_SUCCESS, desc.get_desc().col_desc_->agg_meta_array_.assign(agg_col_metas_));
  }
  ASSERT_EQ(OB_SUCCESS, merge_root_index_builder_->init(desc.get_desc()));
  ASSERT_EQ(OB_SUCCESS, writer.open(desc.get_desc(), start_seq));
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));
  const int64_t partial_row_cnt = max_partial_row_cnt_;
  insert_partial_data(writer, partial_row_cnt);
  ASSERT_EQ(OB_SUCCESS, writer.close());
  // data write ctx has been moved to merge_root_index_builder_
  ASSERT_EQ(writer.get_macro_block_write_ctx().get_macro_block_count(), 0);
  data_macro_block_cnt_ = merge_root_index_builder_->roots_[0]->macro_metas_->count();
  ASSERT_GE(data_macro_block_cnt_, 0);
  int64_t column_cnt = 0;
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_EQ(OB_SUCCESS, storage_schema->get_stored_column_count_in_sstable(column_cnt));
  prepare_partial_sstable(column_cnt);
  prepare_merge_ddl_kvs();
  ObTabletObjLoadHelper::free(allocator_, storage_schema);
}

void TestIndexBlockDataPrepare::prepare_partial_sstable(const int64_t column_cnt)
{
  ObSSTableMergeRes res;
  OK(merge_root_index_builder_->close(res));
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
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    ASSERT_NE(nullptr, read_info.buf_ = reinterpret_cast<char*>(allocator_.alloc(read_info.size_)));
    ASSERT_EQ(OB_SUCCESS, ObBlockManager::read_block(read_info, macro_handle));
    ASSERT_NE(macro_handle.get_buffer(), nullptr);
    ASSERT_EQ(macro_handle.get_data_size(), macro_block_size);
    // get root block
    int64_t block_offset = root_desc.addr_.offset_;
    int64_t block_size = root_desc.addr_.size_;
    const char *block_buf = macro_handle.get_buffer() + block_offset;
    // decompress and decrypt root block
    ObMicroBlockDesMeta meta(ObCompressorType::NONE_COMPRESSOR, root_row_store_type, 0, 0, nullptr);
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
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(merge_root_index_builder_->index_store_desc_.get_desc().row_store_type_, micro_reader));
  ObMicroBlockData root_block(root_buf, root_size);
  ObDatumRow row;
  OK(row.init(allocator_, merge_root_index_builder_->index_store_desc_.get_desc().col_desc_->row_column_count_));
  OK(micro_reader->init(root_block, nullptr));
  ObIndexBlockRowParser idx_row_parser;
  int64_t last_idx = 0;
  for (int64_t it = 0; it != micro_reader->row_count(); ++it) {
    idx_row_parser.reset();
    OK(micro_reader->get_row(it, row));
    OK(idx_row_parser.init(TEST_ROWKEY_COLUMN_CNT + 2, row));
    int64_t before_last_idx = last_idx;
    STORAGE_LOG(INFO, "check offset", K(idx_row_parser.get_row_offset()), K(it));
  }

  // deserialize micro block header in root block buf
  ObMicroBlockHeader root_micro_header;
  int64_t des_pos = 0;
  ASSERT_EQ(OB_SUCCESS, root_micro_header.deserialize(root_buf, root_size, des_pos));
  merge_root_block_data_buf_.buf_ = static_cast<char *>(allocator_.alloc(root_size));
  merge_root_block_data_buf_.size_ = root_size;
  int64_t copy_pos = 0;
  ObMicroBlockHeader *copied_micro_header = nullptr;
  ASSERT_EQ(OB_SUCCESS, root_micro_header.deep_copy(
      (char *)merge_root_block_data_buf_.buf_, merge_root_block_data_buf_.size_, copy_pos, copied_micro_header));
  ASSERT_TRUE(copied_micro_header->is_valid());
  MEMCPY((char *)(merge_root_block_data_buf_.buf_ + copy_pos), root_buf + des_pos, root_size - des_pos);
  row_store_type_ = root_row_store_type;
  ObITable::TableKey table_key;
  int64_t table_id = 3001;
  int64_t tenant_id = 1;
  table_key.table_type_ = ObITable::TableType::DDL_MERGE_CO_SSTABLE;
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
  if (is_cg_data_) {
    param.rowkey_column_cnt_ = 0;
  } else {
    param.rowkey_column_cnt_ = table_schema_.get_rowkey_column_num()
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }
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
  param.ddl_scn_.convert_from_ts(ObTimeUtility::current_time());
  param.filled_tx_scn_.set_min();
  param.contain_uncommitted_row_ = false;
  param.encrypt_id_ = res.encrypt_id_;
  param.master_key_id_ = res.master_key_id_;
  ASSERT_EQ(OB_SUCCESS, param.data_block_ids_.assign(res.data_block_ids_));
  ASSERT_EQ(OB_SUCCESS, param.other_block_ids_.assign(res.other_block_ids_));
  if (param.table_key_.is_co_sstable() && param.column_group_cnt_ <= 1) {
    param.column_group_cnt_ = column_cnt + 2; /* set column group_cnt to avoid return err, cnt is calculated as each + all + default*/
  }
  MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  if (merge_type_ == MAJOR_MERGE) {
    OK(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
  }
  partial_sstable_.reset();
  ASSERT_EQ(OB_SUCCESS, partial_sstable_.init(param, &allocator_));
  STORAGE_LOG(INFO, "create partial_sstable param", K(param));
}

void TestIndexBlockDataPrepare::insert_partial_data(ObMacroBlockWriter &data_writer, const int64_t row_cnt)
{
  partial_sstable_row_cnt_ = 0;
  int64_t seed = min_row_seed_;
  ObDatumRow row;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag flags[] = {DF_INSERT, DF_UPDATE, DF_DELETE};
  int64_t rows_per_mirco_block = rows_per_mirco_block_;
  int64_t rows_per_macro_block = rows_per_mirco_block_ * mirco_blocks_per_macro_block_;
  int64_t rows_cnt = row_cnt;
  if (INT64_MAX == rows_per_mirco_block_ || INT64_MAX == mirco_blocks_per_macro_block_) {
    rows_per_mirco_block = INT64_MAX;
    rows_per_macro_block = INT64_MAX;
  }
  while (true) {
    if (partial_sstable_row_cnt_ >= rows_cnt) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
    ++seed;
    ObDmlFlag dml = flags[partial_sstable_row_cnt_ % ARRAYSIZEOF(flags)]; // INSERT / UPDATE / DELETE
    convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
    if (partial_sstable_row_cnt_ == 0) {
      ObDatumRowkey &start_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, start_key.deep_copy(start_key_, allocator_));
    }
    if (partial_sstable_row_cnt_ == rows_cnt - 1) {
      ObDatumRowkey &end_key = data_writer.last_key_;
      ASSERT_EQ(OB_SUCCESS, end_key.deep_copy(end_key_, allocator_));
    }
    if ((partial_sstable_row_cnt_ + 1) % rows_per_mirco_block == 0) {
      OK(data_writer.build_micro_block());
    }
    if ((partial_sstable_row_cnt_ + 1) % rows_per_macro_block == 0) {
      OK(data_writer.try_switch_macro_block());
    }
    ++partial_sstable_row_cnt_;
  }
  //max_row_seed_ = seed - 1;
}

void TestIndexBlockDataPrepare::prepare_contrastive_sstable()
{
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  start_seq.set_data_block();
  row_generate_.reset();

  ObWholeDataStoreDesc desc;
  share::SCN end_scn;
  end_scn.convert_from_ts(ObTimeUtility::current_time());
  ASSERT_EQ(OB_SUCCESS, desc.init(table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type_, SNAPSHOT_VERSION, CLUSTER_CURRENT_VERSION, end_scn));
  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  root_index_builder_ = new (builder_buf) ObSSTableIndexBuilder();
  ASSERT_NE(nullptr, root_index_builder_);
  desc.get_desc().sstable_index_builder_ = root_index_builder_;

  ASSERT_TRUE(desc.is_valid());
  if (need_agg_data_) {
    ASSERT_EQ(OB_SUCCESS, desc.get_desc().col_desc_->agg_meta_array_.assign(agg_col_metas_));
  }
  ASSERT_EQ(OB_SUCCESS, root_index_builder_->init(desc.get_desc()));
  ASSERT_EQ(OB_SUCCESS, writer.open(desc.get_desc(), start_seq));
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));

  insert_data(writer);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  // data write ctx has been moved to root_index_builder
  ASSERT_EQ(writer.get_macro_block_write_ctx().get_macro_block_count(), 0);
  data_macro_block_cnt_ = root_index_builder_->roots_[0]->macro_metas_->count();
  ASSERT_GE(data_macro_block_cnt_, 0);

  int64_t column_cnt = 0;
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_EQ(OB_SUCCESS, storage_schema->get_stored_column_count_in_sstable(column_cnt));
  close_builder_and_prepare_sstable(column_cnt);
  prepare_ddl_kv();
  ObTabletObjLoadHelper::free(allocator_, storage_schema);
}

void TestIndexBlockDataPrepare::prepare_merge_ddl_kvs()
{
  ddl_kvs_.reset();
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
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(t3m, nullptr);
  ASSERT_EQ(OB_SUCCESS, t3m->acquire_ddl_kv(ddl_kvs_));
  ASSERT_EQ(OB_SUCCESS, ddl_kvs_.get_obj()->init(ls_id, tablet_id, ddl_start_scn, sstable_.get_data_version(), ddl_start_scn, 4000));

  ObITable::TableKey ddl_key = sstable_.get_key();
  ddl_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  for (int64_t i = 0; i < DDL_KVS_CNT; ++i) {
    void *buf = allocator_.alloc(sizeof(ObDDLMemtable));
    ASSERT_NE(nullptr, buf);
    ObDDLMemtable *new_ddl_table = new (buf) ObDDLMemtable;
    ASSERT_EQ(OB_SUCCESS, new_ddl_table->init(allocator_, *tablet_handle.get_obj(), ddl_key, ddl_start_scn, 4000));
    ASSERT_EQ(OB_SUCCESS, ddl_kvs_.get_obj()->get_ddl_memtables().push_back(new_ddl_table));
  }
  ObDDLKVHandle kv_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*CREATE*/));
  ddl_kv_mgr_handle.get_obj()->set_ddl_kv(0, ddl_kvs_);
  ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(ddl_start_scn, sstable_.get_data_version(), 4000, ddl_start_scn);
  ddl_kv_ptr_ = ddl_kvs_.get_obj();
  tablet_handle.get_obj()->ddl_kvs_ = &ddl_kv_ptr_;
  tablet_handle.get_obj()->ddl_kv_count_ = 1;
  SMART_VAR(ObSSTableSecMetaIterator, meta_iter) {
    ObDatumRange query_range;
    query_range.set_whole_range();
    ObDataMacroBlockMeta data_macro_meta;
    ASSERT_EQ(OB_SUCCESS, meta_iter.open(query_range,
                                         ObMacroBlockMetaType::DATA_BLOCK_META,
                                         sstable_,
                                         tablet_handle.get_obj()->get_rowkey_read_info(),
                                         allocator_));
    int64_t macro_idx = 0;
    int64_t kv_idx = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(meta_iter.get_next(data_macro_meta))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "get data macro meta failed", K(ret));
        }
      } else {
        STORAGE_LOG(INFO, "data_macro_meta_key", K(data_macro_meta));
        ++macro_idx;
        ObDDLMacroHandle macro_handle;
        macro_handle.set_block_id(data_macro_meta.get_macro_id());
        if (macro_idx > partial_kv_start_idx_) {
          ObDataMacroBlockMeta *copied_meta = nullptr;
          ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(copied_meta, allocator_));
          ASSERT_EQ(OB_SUCCESS, ddl_kvs_.get_obj()->get_ddl_memtables().at(kv_idx)->insert_block_meta_tree(macro_handle, copied_meta, 0));
          ++kv_idx;
        }
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

} // namespace blocksstable
} // namespace oceanbase

#endif // OB_INDEX_BLOCK_DATA_PREPARE_H_
