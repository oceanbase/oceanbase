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

#include <errno.h>
#include <gtest/gtest.h>
#define protected public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define NOT_NULL(ass) ASSERT_NE(nullptr, (ass))
#define private public
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "share/config/ob_server_config.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_i_store.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/compaction/ob_compaction_memory_pool.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace std;
static ObSimpleMemLimitGetter getter;


// just for test, skip to use
int ObCompactionBufferWriter::ensure_space(const int64_t size)
{
  int ret = OB_SUCCESS;
  use_mem_pool_ = false;

  if (size <= 0) {
    // do nothing
  } else if (nullptr == data_) { // first alloc
    if (OB_UNLIKELY(!block_.empty())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      data_ = (char *) share::mtl_malloc(size, label_);
      pos_ = 0;
      capacity_ = size;
      block_.buffer_ = data_;
      block_.buffer_size_ = size;
      block_.type_ = ObCompactionBufferBlock::MTL_PIECE_TYPE;
    }
  } else if (capacity_ < size) {
    char *new_data = (char *) share::mtl_malloc(size, label_);
    MEMCPY(new_data, data_, pos_);
    data_ = new_data;
    capacity_ = size;
    block_.buffer_ = data_;
    block_.buffer_size_ = size;
    block_.type_ = ObCompactionBufferBlock::MTL_PIECE_TYPE;
  }
  return ret;
}

void ObCompactionBufferWriter::reset()
{
}

namespace unittest
{

class TestIndexTree : public ::testing::Test
{
public:
  TestIndexTree();
  virtual ~TestIndexTree();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
protected:
  void fake_freeze_info();
  void prepare_schema();
  void prepare_4K_cols_schema();
  void prepare_data();
  void prepare_index_builder(ObWholeDataStoreDesc &data_desc, ObSSTableIndexBuilder &sstable_builder);
  void prepare_data_desc(ObWholeDataStoreDesc &data_desc, ObSSTableIndexBuilder *sstable_builder);
  void prepare_index_desc(const ObWholeDataStoreDesc &data_desc, ObWholeDataStoreDesc &index_desc);
  void mock_compaction(const int64_t test_row_num,
                       ObIArray<ObMacroBlocksWriteCtx *> &data_write_ctxs,
                       ObIArray<ObMacroBlocksWriteCtx *> &index_write_ctxs,
                       ObMacroMetasArray *&meta_info_list,
                       ObSSTableMergeRes &ctx,
                       IndexTreeRootCtxList *&roots);
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  static const int64_t TEST_ROW_CNT = 1000;
  static const int64_t MAX_TEST_COLUMN_CNT = TEST_COLUMN_CNT + 3;
  static const int64_t SNAPSHOT_VERSION = 2;
  const uint64_t tenant_id_;
  ObTenantFreezeInfoMgr *mgr_;
  ObDecodeResourcePool *decode_res_pool_;
  ObTenantCompactionMemPool *mem_pool_;
  share::ObTenantBase tenant_base_;
  ObTableSchema table_schema_;
  ObTableSchema index_schema_;
  ObRowGenerate row_generate_;
  ObRowGenerate index_row_generate_;
  ObITable::TableKey table_key_;
  ObDatumRow row_;
  ObDatumRow multi_row_;
  ObArenaAllocator allocator_;
  ObSharedMacroBlockMgr *shared_blk_mgr_;
};

TestIndexTree::TestIndexTree()
    : tenant_id_(500),
    mgr_(nullptr),
    decode_res_pool_(nullptr),
    mem_pool_(nullptr),
    tenant_base_(500),
    shared_blk_mgr_(nullptr)
{
  ObAddr self;
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  obrpc::ObCommonRpcProxy rs_rpc_proxy;
  share::ObRsMgr rs_mgr;
  int64_t tenant_id = 1;
  self.set_ip_addr("127.0.0.1", 8086);
  getter.add_tenant(tenant_id,
                    2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
}

TestIndexTree::~TestIndexTree()
{
}

void TestIndexTree::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "SetUpTestCase");
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestIndexTree::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestIndexTree::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;
  mgr_ = OB_NEW(ObTenantFreezeInfoMgr, ObModIds::TEST);
  decode_res_pool_ = OB_NEW(ObDecodeResourcePool, ObModIds::TEST);
  shared_blk_mgr_ = OB_NEW(ObSharedMacroBlockMgr, ObModIds::TEST);
  mem_pool_ = OB_NEW(ObTenantCompactionMemPool, ObModIds::TEST);
  tenant_base_.set(shared_blk_mgr_);
  tenant_base_.set(mgr_);
  tenant_base_.set(decode_res_pool_);
  tenant_base_.set(mem_pool_);
  share::ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(OB_SUCCESS, mgr_->init(500, *GCTX.sql_proxy_));
  ASSERT_EQ(OB_SUCCESS, decode_res_pool_->init());
  ASSERT_EQ(OB_SUCCESS, mem_pool_->init());
  ASSERT_EQ(OB_SUCCESS, shared_blk_mgr_->init());
  fake_freeze_info();
  ASSERT_EQ(shared_blk_mgr_, MTL(ObSharedMacroBlockMgr *));
  ASSERT_EQ(mgr_, MTL(ObTenantFreezeInfoMgr *));
  ASSERT_EQ(decode_res_pool_, MTL(ObDecodeResourcePool *));
  ASSERT_EQ(mem_pool_, MTL(ObTenantCompactionMemPool *));
  int tmp_ret = OB_SUCCESS;

  prepare_schema();
  row_generate_.reset();
  index_row_generate_.reset();
  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = index_row_generate_.init(index_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  table_key_.tablet_id_ = table_schema_.get_table_id();
  table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
  table_key_.version_range_.snapshot_version_ = share::OB_MAX_SCN_TS_NS - 1;
  ASSERT_TRUE(table_key_.is_valid());
}

void TestIndexTree::TearDown()
{
  row_generate_.reset();
  index_row_generate_.reset();
  table_schema_.reset();
  tenant_base_.destroy_mtl_module(); // stop threads
  tenant_base_.destroy();
  share::ObTenantEnv::set_tenant(nullptr);
}

void TestIndexTree::fake_freeze_info()
{
  common::ObArray<share::ObSnapshotInfo> snapshots;

  ASSERT_TRUE(nullptr != mgr_);
  share::ObFreezeInfoList &info_list = mgr_->freeze_info_mgr_.freeze_info_;
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

static void convert_to_multi_version_row(const ObDatumRow &org_row,
    const int64_t schema_rowkey_cnt, const int64_t column_cnt, const int64_t snapshot_version, const ObDmlFlag dml_flag,
    ObDatumRow &multi_row)
{
  for (int64_t i = 0; i < schema_rowkey_cnt; ++i) {
    multi_row.storage_datums_[i] = org_row.storage_datums_[i];
  }
  multi_row.storage_datums_[schema_rowkey_cnt].set_int(-snapshot_version);
  multi_row.storage_datums_[schema_rowkey_cnt + 1].set_int(0);
  for(int64_t i = schema_rowkey_cnt; i < column_cnt; ++i) {
    multi_row.storage_datums_[i + 2] = org_row.storage_datums_[i];
  }

  multi_row.count_ = column_cnt + 2;
  if (ObDmlFlag::DF_DELETE == dml_flag) {
    multi_row.row_flag_= ObDmlFlag::DF_DELETE;
  } else {
    multi_row.row_flag_ = ObDmlFlag::DF_INSERT;
  }
  multi_row.mvcc_row_flag_.set_last_multi_version_row(true);
}

void TestIndexTree::prepare_schema()
{
  ObColumnSchemaV2 column;
  uint64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
  table_schema_.reset();
  index_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_tree"));
  ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_index_tree"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(ENCODING_ROW_STORE);
  index_schema_.set_tenant_id(1);
  index_schema_.set_tablegroup_id(1);
  index_schema_.set_database_id(1);
  index_schema_.set_table_id(table_id);
  index_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  index_schema_.set_max_used_column_id(TEST_ROWKEY_COLUMN_CNT + 1);
  index_schema_.set_block_size(micro_block_size);
  index_schema_.set_compress_func_name("none");
  index_schema_.set_row_store_type(ENCODING_ROW_STORE);
  //init column
  ObArray<share::schema::ObColDesc> out_cols;
  const int64_t schema_version = 1;
  const int64_t rowkey_count = TEST_ROWKEY_COLUMN_CNT;
  const int64_t column_count = TEST_COLUMN_CNT;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_data_length(1);
    if(obj_type == common::ObInt32Type){
      column.set_rowkey_position(1);
      column.set_order_in_rowkey(ObOrderType::ASC);
    } else if(obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_order_in_rowkey(ObOrderType::DESC);
    } else {
      column.set_rowkey_position(0);
    }
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(obj_type);
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col));
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    if (column.is_rowkey_column()) {
      ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));
    }
  }
  column.reset();
  column.set_table_id(table_id);
  column.set_column_id(TEST_COLUMN_CNT +OB_APP_MIN_COLUMN_ID);
  column.set_data_type(ObVarcharType);
  column.set_collation_type(CS_TYPE_BINARY);
  column.set_data_length(1);
  column.set_rowkey_position(0);
  ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));
}

void TestIndexTree::prepare_4K_cols_schema()
{
  ObColumnSchemaV2 column;
  uint64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  const int64_t test_rowkey_cnt = 1;
  const int64_t test_col_cnt = 4096;
  //init table schema
  table_schema_.reset();
  index_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_tree"));
  ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_index_tree"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(test_rowkey_cnt);
  table_schema_.set_max_used_column_id(test_col_cnt);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(FLAT_ROW_STORE);
  index_schema_.set_tenant_id(1);
  index_schema_.set_tablegroup_id(1);
  index_schema_.set_database_id(1);
  index_schema_.set_table_id(table_id);
  index_schema_.set_rowkey_column_num(test_rowkey_cnt);
  index_schema_.set_max_used_column_id(test_col_cnt + 1);
  index_schema_.set_block_size(micro_block_size);
  index_schema_.set_compress_func_name("none");
  index_schema_.set_row_store_type(FLAT_ROW_STORE);
  //init column
  ObArray<share::schema::ObColDesc> out_cols;
  const int64_t schema_version = 1;
  const int64_t rowkey_count = test_rowkey_cnt;
  const int64_t column_count = test_col_cnt;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < test_col_cnt; ++i){
    ObObjType obj_type = ObObjType::ObIntType;
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_data_length(1);
    if(0 == i){
      column.set_rowkey_position(1);
      column.set_order_in_rowkey(ObOrderType::ASC);
    } else {
      column.set_rowkey_position(0);
    }
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(obj_type);
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col));
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    if (column.is_rowkey_column()) {
      ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));
    }
  }
  column.reset();
  column.set_table_id(table_id);
  column.set_column_id(test_col_cnt +OB_APP_MIN_COLUMN_ID);
  column.set_data_type(ObVarcharType);
  column.set_collation_type(CS_TYPE_BINARY);
  column.set_data_length(1);
  column.set_rowkey_position(0);
  ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));
}

void TestIndexTree::prepare_data()
{
  int ret = OB_SUCCESS;
  //ObSSTable *data_sstable = NULL;
  ObWholeDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObRowGenerate data_row_generate;

  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ret = data_desc.init(table_schema_, ObLSID(1), ObTabletID(1), MAJOR_MERGE, 1, DATA_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.open(data_desc.get_desc(), start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  int64_t free_macro_block_cnt = OB_SERVER_BLOCK_MGR.get_free_macro_block_count();
  int64_t row_id = 0;
  while (free_macro_block_cnt - OB_SERVER_BLOCK_MGR.get_free_macro_block_count() > 0) {
    ret = row_generate_.get_next_row(row_id, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ret = writer.append_row(multi_row);
    ASSERT_EQ(OB_SUCCESS, ret) << "rowkey cnt: " << table_schema_.get_rowkey_column_num() << "column cnt: " << table_schema_.get_column_count();
    ++row_id;
  }

  int64_t border_row_id = row_id;
  row_id += 10000;
  free_macro_block_cnt = OB_SERVER_BLOCK_MGR.get_free_macro_block_count();
  while (free_macro_block_cnt - OB_SERVER_BLOCK_MGR.get_free_macro_block_count() > 0) {
    ret = row_generate_.get_next_row(row_id, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ret = writer.append_row(multi_row);
    ASSERT_EQ(OB_SUCCESS, ret) << "rowkey cnt: " << table_schema_.get_rowkey_column_num() << "column cnt: " << table_schema_.get_column_count();
    ++row_id;
  }
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  writer.reset();

  ret = writer.open(data_desc.get_desc(), start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = border_row_id + 1; i < border_row_id + 10000; ++i) {
    ret = row_generate_.get_next_row(i, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ret = writer.append_row(multi_row);
    ASSERT_EQ(OB_SUCCESS, ret) << "rowkey cnt: " << table_schema_.get_rowkey_column_num() << "column cnt: " << table_schema_.get_column_count();
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}


class TestIndexTreeStress : public share::ObThreadPool
{
public:
  TestIndexTreeStress();
  virtual ~TestIndexTreeStress() {}
  int init(uint64_t tenant_id, const int64_t thread_cnt, ObRowGenerate &row_generate, ObWholeDataStoreDesc &data_store_desc,
      ObMacroDataSeq &start_seq, ObSSTableSecMetaIterator *lob_iter,
      ObMacroBlockWriter *index_writer = NULL);
  virtual void run1();

private:
  uint64_t tenant_id_;
  int64_t thread_cnt_;
  int64_t row_count_;
  ObArenaAllocator allocator_;
  ObRowGenerate *row_generate_;
  ObWholeDataStoreDesc *data_store_desc_;
  ObMacroDataSeq *start_seq_;
  ObSSTableSecMetaIterator *lob_iter_;
  ObMacroBlockWriter *index_writer_;
  ObSSTableIndexBuilder *sstable_builder_;
  common::SpinRWLock lock_;
  share::ObTenantBase tenant_base_;
  ObDecodeResourcePool *decode_res_pool_;
};

TestIndexTreeStress::TestIndexTreeStress()
  : tenant_id_(0), thread_cnt_(0), row_count_(0), allocator_(), row_generate_(NULL), data_store_desc_(NULL), start_seq_(NULL),
    lob_iter_(NULL), index_writer_(NULL), sstable_builder_(NULL), lock_(), tenant_base_(500), decode_res_pool_(nullptr)
{
}

int TestIndexTreeStress::init(uint64_t tenant_id, const int64_t thread_cnt, ObRowGenerate &row_generate,
    ObWholeDataStoreDesc &data_store_desc, ObMacroDataSeq &start_seq, ObSSTableSecMetaIterator *lob_iter,
    ObMacroBlockWriter *index_writer)
{
  int ret = OB_SUCCESS;
  if (thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(thread_cnt));
  } else {
    tenant_id_ = tenant_id;
    thread_cnt_ = thread_cnt;
    row_count_ = 0;
    row_generate_ = &row_generate;
    data_store_desc_ = &data_store_desc;
    start_seq_ = &start_seq;
    lob_iter_ = lob_iter;
    index_writer_ = index_writer;
    set_thread_count(static_cast<int32_t>(thread_cnt));
    decode_res_pool_ = new(allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool;
    tenant_base_.set(decode_res_pool_);
    share::ObTenantEnv::set_tenant(&tenant_base_);
    tenant_base_.init();
    decode_res_pool_->init();
  }
  return ret;
}

void TestIndexTreeStress::run1()
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_base_);
  ObMacroBlockWriter *macro_writer = new ObMacroBlockWriter();
  //{
    SpinWLockGuard guard(lock_);
    start_seq_->set_parallel_degree(get_thread_idx() + 2);
    ret = macro_writer->open(data_store_desc_->get_desc(), *start_seq_);
  //}
  ASSERT_EQ(OB_SUCCESS, ret);
  static const int64_t MAX_TEST_COLUMN_CNT = TestIndexTree::TEST_COLUMN_CNT + 3;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  int64_t test_row_num = row_count_ > 0 ? row_count_ : TestIndexTree::TEST_ROW_CNT;
  ObDatumRow row;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, TestIndexTree::TEST_COLUMN_CNT));
  for(int64_t i = 0; i < test_row_num; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_->get_next_row(row));
    convert_to_multi_version_row(row, TestIndexTree::TEST_ROWKEY_COLUMN_CNT, TestIndexTree::TEST_COLUMN_CNT, 2, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, macro_writer->append_row(multi_row));
  }
  ASSERT_EQ(OB_SUCCESS, macro_writer->close());
  delete macro_writer;
}

void TestIndexTree::prepare_index_desc(
  const ObWholeDataStoreDesc &data_desc,
  ObWholeDataStoreDesc &index_desc)
{
  int ret = OB_SUCCESS;
  ret = index_desc.gen_index_store_desc(data_desc.get_desc());
  ASSERT_EQ(OB_SUCCESS, ret);
}
void TestIndexTree::prepare_index_builder(ObWholeDataStoreDesc &data_desc,
                                          ObSSTableIndexBuilder &sstable_builder)
{
  int ret = OB_SUCCESS;
  prepare_data_desc(data_desc, &sstable_builder);
  ret = sstable_builder.init(data_desc.get_desc());
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestIndexTree::prepare_data_desc(ObWholeDataStoreDesc &data_desc,
                                      ObSSTableIndexBuilder *sstable_builder)
{
  int ret = OB_SUCCESS;
  ret = data_desc.init(table_schema_, ObLSID(1), ObTabletID(1), MAJOR_MERGE, 1, DATA_CURRENT_VERSION);
  data_desc.get_desc().sstable_index_builder_ = sstable_builder;
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestIndexTree::mock_compaction(const int64_t test_row_num,
                                    ObIArray<ObMacroBlocksWriteCtx *> &data_write_ctxs,
                                    ObIArray<ObMacroBlocksWriteCtx *> &index_write_ctxs,
                                    ObMacroMetasArray *&meta_info_list,
                                    ObSSTableMergeRes &res,
                                    IndexTreeRootCtxList *&roots)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  char *buf = static_cast<char *> (allocator_.alloc(sizeof(ObSSTableIndexBuilder)));
  ObSSTableIndexBuilder *sstable_builder = new (buf) ObSSTableIndexBuilder();
  prepare_index_builder(data_desc, *sstable_builder);
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter data_writer;
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), data_seq));

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  for(int64_t i = 0; i < test_row_num; ++i){
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
    ASSERT_EQ(OB_SUCCESS, data_writer.build_micro_block());
    ASSERT_EQ(OB_SUCCESS, data_writer.try_switch_macro_block());
  }
  ObMacroBlock &fir_blk = data_writer.macro_blocks_[0];
  ObMacroBlock &sec_blk = data_writer.macro_blocks_[1];
  ASSERT_EQ(fir_blk.original_size_, fir_blk.data_zsize_);
  ASSERT_EQ(sec_blk.original_size_, sec_blk.data_zsize_);
  ASSERT_EQ(OB_SUCCESS, data_writer.close());
  ASSERT_EQ(OB_SUCCESS, sstable_builder->close(res));

  ObSSTableMergeRes tmp_res;
  ASSERT_EQ(OB_ERR_UNEXPECTED, data_writer.close()); // not re-entrant
  OK(sstable_builder->close(tmp_res)); // re-entrant
  ASSERT_EQ(tmp_res.root_desc_.buf_, res.root_desc_.buf_);
  ASSERT_EQ(tmp_res.data_root_desc_.buf_, res.data_root_desc_.buf_);
  ASSERT_EQ(tmp_res.data_blocks_cnt_, res.data_blocks_cnt_);

  meta_info_list = sstable_builder->roots_[0]->macro_metas_;
  roots = &(sstable_builder->roots_);
}
TEST_F(TestIndexTree, test_macro_id_index_block)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObWholeDataStoreDesc data_desc_small;
  data_desc_small.get_static_desc().micro_block_size_limit_ = 1 * 1024L;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc_small, sstable_builder);

  ObWholeDataStoreDesc data_desc;
  prepare_data_desc(data_desc, &sstable_builder);
  int64_t num = 100;
  ObMacroDataSeq data_seq(num);
  ObMacroBlockWriter data_writer;
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), data_seq));
  ObDataIndexBlockBuilder *builder = data_writer.builder_;
  ASSERT_NE(data_writer.builder_, nullptr);

  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, row));
  convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
  ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
  ASSERT_EQ(OB_SUCCESS, data_writer.build_micro_block());
  const MacroBlockId first_macro_id = data_writer.macro_handles_[0].get_macro_id();
  ASSERT_TRUE(first_macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, data_writer.close());
  // get macro row
  ObIndexBlockRowDesc &macro_row_desc = builder->macro_row_desc_;
  ASSERT_EQ(macro_row_desc.macro_id_, ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID);
  ObDataMacroBlockMeta *macro_meta = sstable_builder.roots_[0]->macro_metas_->at(0);
  ASSERT_EQ(macro_meta->val_.macro_id_, first_macro_id);

  // read macro block
  ObMacroBlockReadInfo read_info;
  ObMacroBlockHandle macro_handle;
  const int64_t macro_block_size = 2 * 1024 * 1024;
  read_info.macro_block_id_ = first_macro_id;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.offset_ = 0;
  read_info.size_ = macro_block_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  ASSERT_NE(nullptr, read_info.buf_ = reinterpret_cast<char*>(allocator_.alloc(read_info.size_)));

  OK(ObBlockManager::read_block(read_info, macro_handle));
  ASSERT_NE(macro_handle.get_buffer(), nullptr);
  ASSERT_EQ(macro_handle.get_data_size(), macro_block_size);

  const char *macro_block_buf = macro_handle.get_buffer();
  int64_t pos = sizeof(ObMacroBlockCommonHeader);
  ObSSTableMacroBlockHeader macro_header;
  OK(macro_header.deserialize(macro_block_buf, macro_block_size, pos));
  ASSERT_EQ(macro_header.fixed_header_.idx_block_offset_, macro_row_desc.block_offset_); // n-1 level block offset
  ASSERT_EQ(macro_header.fixed_header_.idx_block_size_, macro_row_desc.block_size_); // n-1 level block size

  int64_t size = macro_row_desc.block_size_;
  const char *leaf_block_buf = macro_block_buf + macro_row_desc.block_offset_;
  int64_t payload_size = 0;
  const char *payload_buf = nullptr;
  ObMicroBlockHeader header;
  pos = 0;
  OK(header.deserialize(leaf_block_buf, size, pos));
  OK(header.check_and_get_record(
      leaf_block_buf, size, MICRO_BLOCK_HEADER_MAGIC, payload_buf, payload_size));
  ObMicroBlockData root_block(payload_buf, payload_size);
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder.index_store_desc_.get_desc().row_store_type_, micro_reader));
  OK(micro_reader->init(root_block, nullptr));

  ObDatumRow index_row;
  OK(index_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  STORAGE_LOG(INFO, "macro block id", K(index_row));
  void *buf = allocator_.alloc(common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj));
  int64_t row_count = micro_reader->row_count();
  ASSERT_EQ(row_count, 1);
  for (int64_t it = 0; it != row_count; ++it) {
    OK(micro_reader->get_row(it, index_row));
    ObString val = index_row.storage_datums_[TEST_ROWKEY_COLUMN_CNT + 2].get_string();
    ObIndexBlockRowHeader *header = reinterpret_cast<ObIndexBlockRowHeader *>(val.ptr());
    ASSERT_EQ(header->get_macro_id(), ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID);
  }
}

TEST_F(TestIndexTree, test_macro_writer_bug1)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  OK(row.init(allocator_, TEST_COLUMN_CNT));
  ObDatumRow multi_row;
  OK(multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObWholeDataStoreDesc data_desc;
  prepare_data_desc(data_desc, nullptr);
  ObMacroBlockWriter data_writer;
  OK(data_writer.open(data_desc.get_desc(), 0));

  OK(row_generate_.get_next_row(0, row));
  convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
  OK(data_writer.append_row(multi_row));
  OK(data_writer.build_micro_block());
  OK(data_writer.try_switch_macro_block()); // force split

  // failed to append fake_block, but succeeded to switch block again
  ObMacroBlockDesc fake_block;
  ASSERT_EQ(OB_INVALID_ARGUMENT, data_writer.append_macro_block(fake_block));

  OK(row_generate_.get_next_row(1, row));
  convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
  OK(data_writer.append_row(multi_row));
  OK(data_writer.close());
}

TEST_F(TestIndexTree, test_index_macro_writer)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc, sstable_builder);
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObMacroDataSeq data_seq(0);
  ObDataStoreDesc &desc = sstable_builder.container_store_desc_;
  ASSERT_EQ(nullptr, desc.sstable_index_builder_);
  ObMacroBlockWriter container_macro_writer;
  ret = container_macro_writer.open(desc, data_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t test_row_num = 100;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 1));
  for(int64_t i = 0; i < test_row_num; ++i){
    ASSERT_EQ(OB_SUCCESS, index_row_generate_.get_next_row(row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), TEST_ROWKEY_COLUMN_CNT + 1, SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, container_macro_writer.append_row(multi_row));
  }
  ASSERT_EQ(OB_SUCCESS, container_macro_writer.close());
}

TEST_F(TestIndexTree, test_empty_index_tree)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc, sstable_builder);

  ObMacroDataSeq data_seq(0);
  prepare_data_desc(data_desc, &sstable_builder);
  ObMacroBlockWriter data_writer;
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), data_seq));
  // do not insert any data
  ASSERT_EQ(OB_SUCCESS, data_writer.close());
  ASSERT_EQ(1, sstable_builder.roots_.count());
  ObSSTableMergeRes res;
  ret = sstable_builder.close(res);
  ASSERT_EQ(0, sstable_builder.roots_.count());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(res.root_desc_.is_empty());

  // test rebuild macro blocks
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), data_seq));
  // do not insert any data
  ASSERT_EQ(OB_SUCCESS, data_writer.close());
  ASSERT_EQ(1, sstable_builder.roots_.count());
  ObSSTableIndexBuilder::ObMacroMetaIter macro_iter;
  sstable_builder.init_meta_iter(macro_iter);
  ASSERT_EQ(0, sstable_builder.roots_.count());

  ret = sstable_builder.close(res);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(res.root_desc_.is_empty());

  // test easy index tree
  ObDatumRow row;
  OK(row.init(allocator_, TEST_COLUMN_CNT));
  ObDatumRow multi_row;
  OK(multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;
  { // mock sstable builder release before index_builder and rebuilder
    ObMacroBlockWriter data_writer;
    ObIndexBlockRebuilder rebuilder;
    {
      ObWholeDataStoreDesc other_data_desc;
      ObSSTableIndexBuilder other_sstable_builder;
      prepare_index_builder(other_data_desc, other_sstable_builder);
      prepare_data_desc(other_data_desc, &other_sstable_builder);
      OK(rebuilder.init(other_sstable_builder));
      OK(data_writer.open(other_data_desc.get_desc(), data_seq));
      for(int64_t i = 0; i < 10; ++i) {
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
        convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
        ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
      }
      OK(data_writer.close());
    } // release other_sstable_builder
    data_writer.reset();
    rebuilder.reset();
  }
}

TEST_F(TestIndexTree, test_accumulative_info)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq data_seq(0);
  ObWholeDataStoreDesc data_desc;
  prepare_data_desc(data_desc, nullptr);
  ObWholeDataStoreDesc index_desc;
  prepare_index_desc(data_desc, index_desc);
  ObMacroBlockWriter index_macro_writer;
  ASSERT_EQ(OB_SUCCESS, index_macro_writer.open(index_desc.get_desc(), data_seq));
  ObBaseIndexBlockBuilder builder;
  ret = builder.init(data_desc.get_desc(), index_desc.get_desc(), allocator_, &index_macro_writer, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObIMicroBlockWriter *micro_writer = builder.micro_writer_;

  const int64_t test_row_num = 100;
  const int64_t round_row_num = 10; // 10 rows one micro block
  const int64_t len = 128;
  ObStorageDatum obj[4];
  obj[0].set_int32(0);
  obj[1].set_string(ObString(1, "1"));
  obj[2].set_int(-2);
  obj[3].set_int(0);

  ObDatumRowkey row_key;
  row_key.assign(obj, 4);
  ObIndexBlockRowDesc index_row(index_desc.get_desc());
  index_row.row_count_ = 1;
  index_row.micro_block_count_ = 1;
  index_row.macro_block_count_ = 1;
  index_row.row_key_ = row_key;
  for(int64_t i = 0; i < test_row_num; ++i) {
    ASSERT_EQ(OB_SUCCESS, builder.append_row(index_row));
    if (micro_writer->get_row_count() == round_row_num) {
      ASSERT_EQ(round_row_num, builder.index_block_aggregator_.get_row_count());
      ASSERT_EQ(OB_SUCCESS, builder.append_index_micro_block());
      ASSERT_EQ(0, builder.index_block_aggregator_.get_row_count());
    }
  }
  ObBaseIndexBlockBuilder *next_builder = builder.next_level_builder_;
  ASSERT_NE(next_builder, nullptr);
  ASSERT_EQ(test_row_num, next_builder->index_block_aggregator_.get_row_count());

  const int64_t row_cnt = test_row_num / round_row_num;
  ASSERT_EQ(OB_SUCCESS, index_macro_writer.try_switch_macro_block());
  const int64_t cur_idx = index_macro_writer.current_index_;
  ObMacroBlock &cur_macro_block = index_macro_writer.macro_blocks_[cur_idx];
  ObSSTableMacroBlockHeader &header = cur_macro_block.macro_header_;
  ASSERT_EQ(OB_SUCCESS, next_builder->append_index_micro_block());
  ASSERT_EQ(1, header.fixed_header_.micro_block_count_);
  ASSERT_EQ(row_cnt, header.fixed_header_.row_count_);

  ObBaseIndexBlockBuilder *next_next_builder = next_builder->next_level_builder_;
  ASSERT_NE(next_next_builder, nullptr);
  ASSERT_EQ(test_row_num, next_next_builder->index_block_aggregator_.get_row_count());
  ASSERT_EQ(test_row_num, next_next_builder->index_block_aggregator_.micro_block_count_);
  ASSERT_EQ(test_row_num, next_next_builder->index_block_aggregator_.macro_block_count_);
}

TEST_F(TestIndexTree, test_multi_writers_with_close)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc, sstable_builder);

  ObMacroDataSeq data_seq(0);
  TestIndexTreeStress stress;
  int thread_ct = 3;
  stress.init(tenant_id_, thread_ct, row_generate_, data_desc, data_seq, nullptr);
  ASSERT_EQ(OB_SUCCESS, ret);

  stress.start();
  stress.wait();
  ASSERT_EQ(OB_SUCCESS, ret);

  // test sstable_builder re-entrant 4005 bug
  ObSSTableMergeRes res;
  OK(sstable_builder.index_builder_.init(
      sstable_builder.data_store_desc_.get_desc(),
      sstable_builder.index_store_desc_.get_desc(), sstable_builder.self_allocator_,
      &sstable_builder.macro_writer_, 1));
  ASSERT_GT(sstable_builder.self_allocator_.used(), 0);
  res.reset();
  sstable_builder.clean_status();
  OK(sstable_builder.sort_roots());
  OK(sstable_builder.merge_index_tree(res));
  res.reset();
  OK(sstable_builder.close(res));

  ObIndexTreeRootBlockDesc &root_desc = res.root_desc_;
  ASSERT_TRUE(root_desc.is_valid());
  ASSERT_TRUE(root_desc.is_mem_type());
  char *root_buf = root_desc.buf_;
  int64_t root_size = root_desc.addr_.size_;

  ObMicroBlockData root_block(root_buf, root_size);
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder.index_store_desc_.get_desc().row_store_type_, micro_reader));
  ret = micro_reader->init(root_block, nullptr);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  void *buf = allocator_.alloc(common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj));
  int64_t row_count = micro_reader->row_count();
  int64_t total_row_cnt = 0;
  for (int64_t it = 0; it != row_count; ++it) {
    ret = micro_reader->get_row(it, row);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObString val = row.storage_datums_[TEST_ROWKEY_COLUMN_CNT + 2].get_string();
    ObIndexBlockRowHeader *header = reinterpret_cast<ObIndexBlockRowHeader *>(val.ptr());
    total_row_cnt += header->row_count_;
  }
  int32_t last_rowkey_int = row.storage_datums_[0].get_int32();
  int64_t test_row_num = TestIndexTree::TEST_ROW_CNT;
  ASSERT_EQ(thread_ct * test_row_num - 1, last_rowkey_int);
  ASSERT_EQ(total_row_cnt, test_row_num * thread_ct);
}


//===================== test secondary meta ================
TEST_F(TestIndexTree, test_merge_info_build_row)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;
  mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, merge_info_list, res, roots);

  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT;
  blocksstable::ObDatumRow datum_row;
  ASSERT_EQ(OB_SUCCESS, datum_row.init(allocator_, rowkey_cnt + 3));
  for (int64_t i = 0; nullptr != merge_info_list && i < merge_info_list->count(); ++i) {
    ObDataMacroBlockMeta *info = merge_info_list->at(i);
    ASSERT_NE(nullptr, info);
    ObDataBlockMetaVal &info_val = info->val_;
    ASSERT_TRUE(info->is_valid());
    ASSERT_EQ(OB_SUCCESS, info->build_row(datum_row, allocator_));
    ASSERT_EQ(i, datum_row.storage_datums_[0].get_int());
    ObDataMacroBlockMeta parsed_meta;
    ASSERT_EQ(OB_SUCCESS, parsed_meta.parse_row(datum_row));
    ASSERT_TRUE(parsed_meta.is_valid());
    ASSERT_EQ(info_val.column_count_, parsed_meta.val_.column_count_);
    for (int64_t i = 0; i < info_val.column_count_; ++i) {
      ASSERT_EQ(info_val.column_checksums_[i], parsed_meta.val_.column_checksums_[i]);
    }
  }

  // test deep_copy of macro_meta with given allocator
  ObArenaAllocator arena_allocator;
  ObFIFOAllocator safe_allocator;
  OK(safe_allocator.init(&arena_allocator, OB_MALLOC_BIG_BLOCK_SIZE,
                         ObMemAttr(OB_SERVER_TENANT_ID, ObNewModIds::TEST)));
  ObDataMacroBlockMeta *copy_meta = nullptr;
  ObDataMacroBlockMeta &large_meta = *merge_info_list->at(0);
  int64_t test_col_cnt = 100;
  for (int64_t i = 0; i < test_col_cnt; ++i) {
    OK(large_meta.val_.column_checksums_.push_back(i));
  }
  ASSERT_EQ(safe_allocator.current_using_, nullptr);
  ASSERT_EQ(safe_allocator.normal_used_, 0);
  OK(large_meta.deep_copy(copy_meta, safe_allocator));
  ASSERT_NE(safe_allocator.current_using_, nullptr);
  safe_allocator.free(copy_meta);
  copy_meta->~ObDataMacroBlockMeta();
  const int64_t end_key_deep_copy_size = 64; // due to end_key::reset by memset
  ASSERT_EQ(safe_allocator.normal_used_, end_key_deep_copy_size);
}

TEST_F(TestIndexTree, test_meta_builder)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;
  mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, merge_info_list, res, roots);

  ObDatumRow leaf_row;
  ASSERT_EQ(OB_SUCCESS, leaf_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc, sstable_builder);
  ObWholeDataStoreDesc index_desc;
  prepare_index_desc(data_desc, index_desc);
  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter container_macro_writer;
  ASSERT_EQ(OB_SUCCESS, container_macro_writer.open(index_desc.get_desc(), data_seq));
  ObMetaIndexBlockBuilder meta_builder;
  ASSERT_EQ(OB_SUCCESS, meta_builder.init(index_desc.get_desc(), allocator_, container_macro_writer));
  for (int64_t i = 0; nullptr != merge_info_list && i < merge_info_list->count(); ++i) {
    ObDataMacroBlockMeta *info = merge_info_list->at(i);
    ASSERT_EQ(OB_SUCCESS, info->build_row(leaf_row, allocator_));
    ASSERT_EQ(OB_SUCCESS, meta_builder.append_leaf_row(leaf_row));
  }

  ObIndexTreeRootBlockDesc root_desc;
  ASSERT_EQ(OB_SUCCESS, meta_builder.close(allocator_, *roots, OB_SERVER_BLOCK_MGR.get_macro_block_size(), 0, root_desc));
  ASSERT_TRUE(root_desc.is_valid());
  ASSERT_TRUE(root_desc.is_mem_type());
  int64_t root_size = root_desc.addr_.size_;
  char *root_buf = root_desc.buf_;

  ObMicroBlockData root_block(root_buf, root_size);
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder.index_store_desc_.get_desc().row_store_type_, micro_reader));
  ret = micro_reader->init(root_block, nullptr);
  ASSERT_EQ(OB_SUCCESS, ret);

  blocksstable::ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  int64_t total_row_cnt = 0;
  for (int64_t it = 0; it != micro_reader->row_count(); ++it) {
    ASSERT_EQ(OB_SUCCESS, micro_reader->get_row(it, row));

    ObString val = row.storage_datums_[TEST_ROWKEY_COLUMN_CNT + 2].get_string();
    ObIndexBlockRowHeader *header = reinterpret_cast<ObIndexBlockRowHeader *>(val.ptr());
    total_row_cnt += header->row_count_;
  }
  ASSERT_EQ(total_row_cnt, test_row_num);
}

TEST_F(TestIndexTree, test_meta_builder_data_root)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 2;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;
  mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, merge_info_list, res, roots);

  ObDatumRow leaf_row;
  ASSERT_EQ(OB_SUCCESS, leaf_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));

  ObWholeDataStoreDesc data_desc;
  prepare_data_desc(data_desc, nullptr);
  ObWholeDataStoreDesc index_desc;
  prepare_index_desc(data_desc, index_desc);
  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter container_macro_writer;
  ASSERT_EQ(OB_SUCCESS, container_macro_writer.open(index_desc.get_desc(), data_seq));
  ObMetaIndexBlockBuilder meta_builder;
  ASSERT_EQ(OB_SUCCESS, meta_builder.init(index_desc.get_desc(), allocator_, container_macro_writer));
  for (int64_t i = 0; i < merge_info_list->count(); ++i) {
    ObDataMacroBlockMeta *info = merge_info_list->at(i);
    ASSERT_EQ(OB_SUCCESS, info->build_row(leaf_row, allocator_));
    ASSERT_EQ(OB_SUCCESS, meta_builder.append_leaf_row(leaf_row));
  }

  ObIndexTreeRootBlockDesc root_desc;
  ASSERT_EQ(OB_SUCCESS, meta_builder.close(allocator_, *roots, OB_SERVER_BLOCK_MGR.get_macro_block_size(), 0, root_desc));
  ASSERT_TRUE(root_desc.is_valid());
  ASSERT_TRUE(root_desc.is_mem_type());
  ASSERT_TRUE(root_desc.is_meta_root_);
  int64_t root_size = root_desc.addr_.size_;
  char *root_buf = root_desc.buf_;

  ObMicroBlockData root_block(root_buf, root_size);
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(index_desc, sstable_builder);
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder.index_store_desc_.get_desc().row_store_type_, micro_reader));
  ret = micro_reader->init(root_block, nullptr);
  ASSERT_EQ(OB_SUCCESS, ret);

  blocksstable::ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  int64_t total_row_cnt = 0;
  ASSERT_EQ(micro_reader->row_count(), test_row_num);
}

TEST_F(TestIndexTree, test_single_row_desc)
{
  // we need use 4K columns to test ObMetaIndexBlockBuilder::build_single_macro_row_desc
  prepare_4K_cols_schema();
  ObSSTableIndexBuilder sstable_builder;
  ObWholeDataStoreDesc data_desc;
  prepare_data_desc(data_desc, &sstable_builder);
  ObWholeDataStoreDesc index_desc;
  prepare_index_desc(data_desc, index_desc);
  prepare_index_builder(index_desc, sstable_builder);

  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter data_writer;
  OK(data_writer.open(data_desc.get_desc(), data_seq));

  ObDatumRow row;
  const int64_t test_col_cnt = 4096;
  OK(row.init(allocator_, test_col_cnt));
  row_generate_.reset();
  OK(row_generate_.init(table_schema_, &allocator_));
  OK(row_generate_.get_next_row(0, row));
  ObDatumRow multi_row;
  OK(multi_row.init(allocator_, test_col_cnt + 10));
  ObDmlFlag dml = DF_INSERT;
  convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
  OK(data_writer.append_row(multi_row));
  OK(data_writer.close());

  ObSSTableMergeRes res;
  sstable_builder.optimization_mode_ = ObSSTableIndexBuilder::ObSpaceOptimizationMode::DISABLE;
  OK(sstable_builder.close(res));

  // test rebuild sstable
  ObSSTableIndexBuilder sstable_builder2;
  prepare_index_builder(index_desc, sstable_builder2);
  ObIndexBlockRebuilder rebuilder;
  OK(rebuilder.init(sstable_builder2));
  ObMacroBlockHandle macro_handle;
  macro_handle.reset();
  ObMacroBlockReadInfo info;
  const int64_t macro_block_size = 2 * 1024 * 1024;
  info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  info.offset_ = 0;
  info.size_ = macro_block_size;
  info.macro_block_id_ = res.data_block_ids_[0];
  info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  ASSERT_NE(nullptr, info.buf_ = reinterpret_cast<char*>(allocator_.alloc(info.size_)));
  OK(ObBlockManager::read_block(info, macro_handle));
  OK(rebuilder.append_macro_row(macro_handle.get_buffer(), macro_handle.get_data_size(), info.macro_block_id_));
  OK(rebuilder.close());
  ObSSTableMergeRes res2;
  OK(sstable_builder2.close(res2));
  // test rebuild sstable by another append_macro_row
  ObSSTableIndexBuilder sstable_builder3;
  prepare_index_builder(index_desc, sstable_builder3);
  sstable_builder3.index_store_desc_.get_desc().static_desc_->major_working_cluster_version_ = DATA_VERSION_4_1_0_0;
  sstable_builder3.container_store_desc_.static_desc_->major_working_cluster_version_ = DATA_VERSION_4_1_0_0;
  ObIndexBlockRebuilder other_rebuilder;
  OK(other_rebuilder.init(sstable_builder3));
  ObDataMacroBlockMeta *macro_meta = nullptr;
  OK(sstable_builder.roots_[0]->macro_metas_->at(0)->deep_copy(macro_meta, allocator_));
  OK(other_rebuilder.append_macro_row(*macro_meta));
  OK(other_rebuilder.close());
  ObSSTableMergeRes res3;
  sstable_builder3.optimization_mode_ = ObSSTableIndexBuilder::ObSpaceOptimizationMode::ENABLE;
  OK(sstable_builder3.close(res3));
}

TEST_F(TestIndexTree, test_extend_micro_block_size)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 20;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;
  ObWholeDataStoreDesc data_desc;
  char *buf = static_cast<char *> (allocator_.alloc(sizeof(ObSSTableIndexBuilder)));
  ObSSTableIndexBuilder *sstable_builder = new (buf) ObSSTableIndexBuilder();

  prepare_data_desc(data_desc, sstable_builder);
  data_desc.get_desc().micro_block_size_ = 248; // only push one index block row will exceed 248.
  ret = sstable_builder->init(data_desc.get_desc());
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter data_writer;
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), data_seq));

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  for(int64_t i = 0; i < test_row_num; ++i){
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
    ASSERT_EQ(OB_SUCCESS, data_writer.build_micro_block());
    ASSERT_EQ(OB_SUCCESS, data_writer.try_switch_macro_block());
  }
  ObMacroBlock &fir_blk = data_writer.macro_blocks_[0];
  ObMacroBlock &sec_blk = data_writer.macro_blocks_[1];
  ASSERT_EQ(fir_blk.original_size_, fir_blk.data_zsize_);
  ASSERT_EQ(sec_blk.original_size_, sec_blk.data_zsize_);
  ASSERT_EQ(OB_SUCCESS, data_writer.close());
  ASSERT_EQ(OB_SUCCESS, sstable_builder->close(res));

  ObSSTableMergeRes tmp_res;
  ASSERT_EQ(OB_ERR_UNEXPECTED, data_writer.close()); // not re-entrant
  OK(sstable_builder->close(tmp_res)); // re-entrant
  ASSERT_EQ(tmp_res.root_desc_.buf_, res.root_desc_.buf_);
  ASSERT_EQ(tmp_res.data_root_desc_.buf_, res.data_root_desc_.buf_);
  ASSERT_EQ(tmp_res.data_blocks_cnt_, res.data_blocks_cnt_);


}

TEST_F(TestIndexTree, test_data_block_checksum)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;
  mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, merge_info_list, res, roots);
  ASSERT_EQ(test_row_num, merge_info_list->count());

  int64_t max_reported_column_id = TEST_COLUMN_CNT + 2;
  ObArray<ObSSTableColumnMeta> column_metas;
  ObArray<int64_t> column_default_checksum;
  for (int64_t i = 0; i < TEST_COLUMN_CNT + 2; ++i) {
    ObSSTableColumnMeta column_meta;
    column_meta.column_id_ = i;
    ASSERT_EQ(OB_SUCCESS, column_metas.push_back(column_meta));
    ASSERT_EQ(OB_SUCCESS, column_default_checksum.push_back(column_meta.column_default_checksum_));
  }
  for (int64_t i = TEST_COLUMN_CNT + 2; i < max_reported_column_id; ++i) {
    ObSSTableColumnMeta column_meta;
    column_meta.column_id_ = i;
    column_meta.column_default_checksum_ = i - TEST_COLUMN_CNT;
    ASSERT_EQ(OB_SUCCESS, column_metas.push_back(column_meta));
    ASSERT_EQ(OB_SUCCESS, column_default_checksum.push_back(column_meta.column_default_checksum_));
  }
}

TEST_F(TestIndexTree, test_reuse_macro_block)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *macro_metas = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;
  mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, macro_metas, res, roots);
  ASSERT_EQ(test_row_num, macro_metas->count());

  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc, sstable_builder);
  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter data_writer;
  OK(data_writer.open(data_desc.get_desc(), data_seq));
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObMacroBlockDesc macro_desc;
    macro_desc.macro_meta_ = macro_metas->at(i);
    OK(data_writer.append_macro_block(macro_desc));
  }
  OK(data_writer.close());
  ObSSTableMergeRes reused_res;
  OK(sstable_builder.close(reused_res));

  ASSERT_EQ(res.data_blocks_cnt_, reused_res.data_blocks_cnt_);
  ASSERT_EQ(res.row_count_, reused_res.row_count_);
  for (int64_t i = 0; i < res.data_blocks_cnt_; ++i) {
    ASSERT_EQ(res.data_block_ids_.at(i), reused_res.data_block_ids_.at(i));
  }
}

TEST_F(TestIndexTree, DISABLED_test_writer_try_to_append_row)
{
  // fix try_to_append_row, enable this case
  int ret = OB_SUCCESS;
  ObDatumRow row;
  OK(row.init(allocator_, TEST_COLUMN_CNT));
  ObDatumRow multi_row;
  OK(multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObWholeDataStoreDesc data_desc;
  ObMacroBlockWriter data_writer;
  ObMicroBlockDesc micro_block_desc;
  prepare_data_desc(data_desc, nullptr);
  data_desc.get_desc().row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;

  OK(data_writer.open(data_desc.get_desc(), 0));
  ObIMicroBlockWriter *micro_writer = data_writer.micro_writer_;
  const int64_t test_num = 1; // only add one row to avoid encoding optimization
  for (int64_t i = 0; i < test_num; ++i) {
    OK(row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    OK(micro_writer->append_row(multi_row));
  }
  OK(micro_writer->build_micro_block_desc(micro_block_desc));
  const int64_t need_store_size = micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;

  { // set size upper bound after reuse
    micro_writer->reuse();
    micro_writer->set_block_size_upper_bound(need_store_size - 1);
    // if append succeeded, set_block_size_upper_bound has not limited the block size
    ASSERT_EQ(-4024, micro_writer->append_row(multi_row));
  }


  { // set size upper bound after reset
    OK(data_writer.open(data_desc.get_desc(), 0));
    micro_writer = data_writer.micro_writer_;
    micro_writer->set_block_size_upper_bound(need_store_size - 1);
    // if append succeeded, set_block_size_upper_bound has not limited the block size
    ASSERT_EQ(-4024, micro_writer->append_row(multi_row));
  }
}

TEST_F(TestIndexTree, test_writer_try_to_append_row)
{
  // If fail to pass this test, please check ObIMicroBlockWriter::try_to_append_row
  int ret = OB_SUCCESS;
  ObDatumRow row;
  OK(row.init(allocator_, TEST_COLUMN_CNT));
  ObDatumRow multi_row;
  OK(multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObWholeDataStoreDesc data_desc;
  ObMacroBlockWriter data_writer;
  ObMicroBlockDesc micro_block_desc;
  prepare_data_desc(data_desc, nullptr);
  for (int64_t j = 0; j < ObRowStoreType::MAX_ROW_STORE; ++j) {
    data_desc.get_desc().row_store_type_ = static_cast<ObRowStoreType>(j);
    OK(data_writer.open(data_desc.get_desc(), 0));
    ObIMicroBlockWriter *micro_writer = data_writer.micro_writer_;
    const int64_t test_num = 10;
    for (int64_t i = 0; i < test_num; ++i) {
      OK(row_generate_.get_next_row(i, row));
      convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
      OK(micro_writer->append_row(multi_row));
    }
    OK(micro_writer->build_micro_block_desc(micro_block_desc));

    int64_t estimate_size = 0;
    if (ObRowStoreType::FLAT_ROW_STORE == j) {
      estimate_size = micro_block_desc.original_size_ + micro_block_desc.header_->header_size_;
    } else if (ObRowStoreType::CS_ENCODING_ROW_STORE == j) {
      estimate_size = micro_block_desc.original_size_
          + static_cast<ObMicroBlockCSEncoder *>(micro_writer)->all_headers_size_;
    } else {
      estimate_size = micro_block_desc.original_size_
          + static_cast<ObMicroBlockEncoder *>(micro_writer)->header_size_;
    }

    { // set size upper bound after reuse
      micro_writer->reuse();
      micro_writer->set_block_size_upper_bound(estimate_size);
      for (int64_t i = 0; i < test_num; ++i) {
        OK(row_generate_.get_next_row(i, row));
        convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
        OK(micro_writer->append_row(multi_row));
      }
    }

    { // set size upper bound after reset
      OK(data_writer.open(data_desc.get_desc(), 0));
      micro_writer = data_writer.micro_writer_;
      micro_writer->set_block_size_upper_bound(estimate_size);
      for (int64_t i = 0; i < test_num; ++i) {
        OK(row_generate_.get_next_row(i, row));
        convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
        OK(micro_writer->append_row(multi_row));
      }
    }
  }
}



TEST_F(TestIndexTree, test_rebuilder)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  OK(row.init(allocator_, TEST_COLUMN_CNT));
  ObDatumRow multi_row;
  OK(multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObWholeDataStoreDesc data_desc1;
  ObWholeDataStoreDesc data_desc2;
  ObSSTableIndexBuilder sstable_builder1;
  ObSSTableIndexBuilder sstable_builder2;
  prepare_index_builder(data_desc1, sstable_builder1);
  prepare_index_builder(data_desc2, sstable_builder2);
  // write data
  int64_t test_num = 10;
  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter data_writer;
  OK(data_writer.open(data_desc1.get_desc(), data_seq));
  for (int64_t i = 0; i < test_num; ++i) {
    OK(row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    OK(data_writer.append_row(multi_row));
    OK(data_writer.build_micro_block());
    OK(data_writer.try_switch_macro_block());
  }
  OK(data_writer.close());
  ObSSTableMergeRes res1;
  OK(sstable_builder1.close(res1));


  ObIndexBlockRebuilder rebuilder;
  OK(rebuilder.init(sstable_builder2));
  // read data blocks
  ObMacroBlockReadInfo info;
  ObMacroBlockHandle macro_handle;
  const int64_t macro_block_size = 2 * 1024 * 1024;
  info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  info.offset_ = 0;
  info.size_ = macro_block_size;
  ObIArray<MacroBlockId> &data_block_ids = res1.data_block_ids_;
  ObDataMacroBlockMeta *macro_meta = nullptr;
  ObArenaAllocator meta_allocator;
  ASSERT_NE(nullptr, info.buf_ = reinterpret_cast<char*>(meta_allocator.alloc(info.size_)));
  ASSERT_EQ(data_block_ids.count(), test_num);
  // rebuild index tree
  for (int64_t i = data_block_ids.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    // i starts from count()-1, to mock disordered data
    MacroBlockId &cur_id = data_block_ids.at(i);
    info.macro_block_id_ = cur_id;
    info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    macro_handle.reset();
    OK(ObBlockManager::read_block(info, macro_handle));
    ASSERT_NE(macro_handle.get_buffer(), nullptr);
    ASSERT_EQ(macro_handle.get_data_size(), macro_block_size);
    OK(rebuilder.get_macro_meta(macro_handle.get_buffer(), macro_block_size, cur_id, meta_allocator, macro_meta));
    OK(rebuilder.append_macro_row(*macro_meta));
  }
  OK(rebuilder.close());
  ObSSTableMergeRes res2;
  OK(sstable_builder2.close(res2));
  // compare merge res
  ASSERT_EQ(res1.root_desc_.height_, res2.root_desc_.height_);
  ASSERT_EQ(res1.root_desc_.height_, 2);
  ObMicroBlockData root_block1(res1.root_desc_.buf_, res1.root_desc_.addr_.size_);
  ObMicroBlockData root_block2(res2.root_desc_.buf_, res2.root_desc_.addr_.size_);
  ObIMicroBlockReader *micro_reader1;
  ObIMicroBlockReader *micro_reader2;

  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder1.index_store_desc_.get_desc().row_store_type_, micro_reader1));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder2.index_store_desc_.get_desc().row_store_type_, micro_reader2));

  OK(micro_reader1->init(root_block1, nullptr));
  OK(micro_reader2->init(root_block2, nullptr));

  blocksstable::ObDatumRow row1;
  blocksstable::ObDatumRow row2;
  OK(row1.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  OK(row2.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  ASSERT_EQ(micro_reader1->row_count(), micro_reader2->row_count());
  ASSERT_EQ(micro_reader1->row_count(), test_num);

  for (int64_t it = 0; it != micro_reader1->row_count(); ++it) {
    OK(micro_reader1->get_row(it, row1));
    OK(micro_reader2->get_row(it, row2));

    ObString val1 = row1.storage_datums_[TEST_ROWKEY_COLUMN_CNT + 2].get_string();
    ObString val2 = row2.storage_datums_[TEST_ROWKEY_COLUMN_CNT + 2].get_string();
    ASSERT_EQ(val1, val2);
  }
}

TEST_F(TestIndexTree, test_diagnose_dump)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc, sstable_builder);
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter data_writer;
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), data_seq));

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  for(int64_t i = 0; i < 10; ++i){
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
  }
  OK(data_writer.build_micro_block());
  OK(row_generate_.get_next_row(10, row));
  convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
  OK(data_writer.append_row(multi_row));
  data_writer.dump_block_and_writer_buffer();
  data_writer.micro_writer_->dump_diagnose_info();
}

TEST_F(TestIndexTree, test_estimate_meta_block_size)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc index_desc;
  ObSSTableIndexBuilder sstable_builder;
  ObWholeDataStoreDesc data_desc;
  prepare_data_desc(data_desc, &sstable_builder);
  prepare_index_desc(data_desc, index_desc);
  OK(sstable_builder.init(index_desc.get_desc()));
  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter data_writer;
  OK(data_writer.open(data_desc.get_desc(), data_seq));

  ObDatumRow multi_row;
  OK(multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDatumRow row;
  OK(row.init(allocator_, TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;

  for(int64_t i = 0; i < 10; ++i) {
    OK(row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    STORAGE_LOG(INFO, "append row", K(i), K(multi_row));
    OK(data_writer.append_row(multi_row));
  }
  const ObDatumRowkey& last_data_key = data_writer.last_key_;
  int64_t estimate_meta_block_size = 0;
  OK(data_writer.builder_->cal_macro_meta_block_size(last_data_key, estimate_meta_block_size));

  ObMacroBlock &macro_block = data_writer.macro_blocks_[0];
  ObMacroBlockHandle &macro_handle = data_writer.macro_handles_[0];
  OK(data_writer.build_micro_block());
  OK(data_writer.builder_->generate_macro_row(macro_block, macro_handle.get_macro_id(), -1/*ddl_start_row_offset*/));
  const ObSSTableMacroBlockHeader &macro_header_ = macro_block.macro_header_;
  ASSERT_EQ(macro_header_.fixed_header_.idx_block_offset_ + macro_header_.fixed_header_.idx_block_size_,
            macro_header_.fixed_header_.meta_block_offset_);
  ASSERT_GT(macro_header_.fixed_header_.meta_block_size_, 0);
  ASSERT_GE(estimate_meta_block_size, macro_header_.fixed_header_.meta_block_size_);
  ObDataMacroBlockMeta *macro_meta = data_writer.builder_->macro_meta_list_->at(0);
  const int64_t val_max_size = macro_meta->val_.get_max_serialize_size();
  macro_meta->val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
  ASSERT_GE(macro_meta->val_.get_serialize_size() + estimate_meta_block_size - val_max_size,
            macro_header_.fixed_header_.meta_block_size_);
}

TEST_F(TestIndexTree, test_cg_row_offset)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 500;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;

  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;
  prepare_index_builder(data_desc, sstable_builder);

  ObWholeDataStoreDesc index_desc;
  prepare_index_desc(data_desc, index_desc);

  vector<int64_t> size_arr = { 1L<<10, 1L<<12, 1L<<14 };

  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder.index_store_desc_.get_desc().row_store_type_, micro_reader));

  for (int i = 0; i < size_arr.size(); ++i) {
    const int64_t micro_block_size = size_arr.at(i);
    index_schema_.set_block_size(micro_block_size);
    mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, merge_info_list, res, roots);
    ObMicroBlockData root_block(res.root_desc_.buf_, res.root_desc_.addr_.size_);

    ObDatumRow row;
    OK(row.init(allocator_, index_desc.get_desc().col_desc_->row_column_count_));
    OK(micro_reader->init(root_block, nullptr));
    ObIndexBlockRowParser idx_row_parser;
    int64_t rows_cnt = -1;
    for (int64_t it = 0; it != micro_reader->row_count(); ++it) {
      idx_row_parser.reset();
      OK(micro_reader->get_row(it, row));
      OK(idx_row_parser.init(TEST_ROWKEY_COLUMN_CNT + 2, row));
      rows_cnt += idx_row_parser.header_->row_count_;
      ASSERT_EQ(rows_cnt, idx_row_parser.get_row_offset());
    }
  }
}

TEST_F(TestIndexTree, test_absolute_offset)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 500;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;

  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder;

  prepare_data_desc(data_desc, &sstable_builder);
  data_desc.get_desc().micro_block_size_ = 512; // make test index tree height > 2
  ret = sstable_builder.init(data_desc.get_desc());
  ASSERT_EQ(OB_SUCCESS, ret);

  ObWholeDataStoreDesc index_desc;
  prepare_index_desc(data_desc, index_desc);

  ObIndexBlockRebuilder rebuilder;
  OK(rebuilder.init(sstable_builder, false, nullptr, true/*use_absolute_offset*/));

  mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, merge_info_list, res, roots);
  ASSERT_EQ(test_row_num, merge_info_list->count());
  vector<int64_t> absolute_offsets;
  for (int meta_idx = 0; meta_idx < test_row_num; meta_idx += 10) {
    merge_info_list->at(meta_idx)->val_.ddl_end_row_offset_ = meta_idx;
    rebuilder.append_macro_row(*merge_info_list->at(meta_idx));
    absolute_offsets.push_back(meta_idx);
  }
  OK(rebuilder.close());
  ObSSTableMergeRes res2;
  OK(sstable_builder.close(res2));
  ASSERT_GT(res2.root_desc_.height_, 2);

  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(sstable_builder.index_store_desc_.get_desc().row_store_type_, micro_reader));


  ObMicroBlockData root_block(res2.root_desc_.buf_, res2.root_desc_.addr_.size_);
  ObDatumRow row;
  OK(row.init(allocator_, index_desc.get_desc().col_desc_->row_column_count_));
  OK(micro_reader->init(root_block, nullptr));
  ObIndexBlockRowParser idx_row_parser;
  int64_t last_idx = 0;
  for (int64_t it = 0; it != micro_reader->row_count(); ++it) {
    idx_row_parser.reset();
    OK(micro_reader->get_row(it, row));
    OK(idx_row_parser.init(TEST_ROWKEY_COLUMN_CNT + 2, row));
    int64_t before_last_idx = last_idx;
    for (int absolute_idx = last_idx; absolute_idx < absolute_offsets.size(); absolute_idx ++) {
      if (absolute_offsets[absolute_idx] == idx_row_parser.get_row_offset()) {
        last_idx = absolute_idx;
      }
    }
    ASSERT_GT(last_idx, before_last_idx);
    if (it == micro_reader->row_count() - 1) {
      ASSERT_EQ(absolute_offsets[absolute_offsets.size() - 1], idx_row_parser.get_row_offset());
    }
  }
}

TEST_F(TestIndexTree, test_close_with_old_schema)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10;
  ObArray<ObMacroBlocksWriteCtx *> data_write_ctxs;
  ObArray<ObMacroBlocksWriteCtx *> index_write_ctxs;
  ObMacroMetasArray *merge_info_list = nullptr;
  ObSSTableMergeRes res;
  IndexTreeRootCtxList *roots = nullptr;
  mock_compaction(test_row_num, data_write_ctxs, index_write_ctxs, merge_info_list, res, roots);
  ASSERT_EQ(test_row_num, merge_info_list->count());

  // mock old schema with fewer columns
  ObWholeDataStoreDesc index_desc;
  OK(index_desc.init(table_schema_, ObLSID(1), ObTabletID(1), MAJOR_MERGE, 1/*snapshot*/, 0/*cluster_version*/));
  index_desc.static_desc_.major_working_cluster_version_ = DATA_VERSION_4_0_0_0;
  --index_desc.get_desc().col_desc_->full_stored_col_cnt_;
  index_desc.get_desc().col_desc_->col_default_checksum_array_.pop_back();

  // read old macro block metas
  ObSSTableIndexBuilder sstable_builder;
  OK(sstable_builder.init(index_desc.get_desc()));
  ObIndexBlockRebuilder rebuilder;
  OK(rebuilder.init(sstable_builder));
  for (int64_t i = 0; i < test_row_num; ++i) {
    OK(rebuilder.append_macro_row(*merge_info_list->at(i)));
  }
  OK(rebuilder.close());
  ObSSTableMergeRes res2;
  ASSERT_EQ(OB_INVALID_ARGUMENT, sstable_builder.close(res2));
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_tree.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_index_tree.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
