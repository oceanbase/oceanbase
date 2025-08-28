// owner: zs475329
// owner group: storage

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
#include <stdexcept>
#include <chrono>
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define NOT_NULL(ass) ASSERT_NE(nullptr, (ass))
#define private public
#define protected public

#include "storage/blocksstable/ob_row_generate.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "mtlenv/mock_tenant_module_env.h"


namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace std;


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

void ObBlockWriterConcurrentGuard::on_error()
{
  throw std::runtime_error("concurrently visit is not allowed");
}

namespace unittest
{

void prepare_data_desc(ObWholeDataStoreDesc &data_desc,ObTableSchema *table_schema,
  ObSSTableIndexBuilder *sstable_builder)
{
  int ret = OB_SUCCESS;
  ret = data_desc.init(false/*is_ddl*/, *table_schema, ObLSID(1), ObTabletID(1), MAJOR_MERGE, 
  ObTimeUtility::fast_current_time()/*snapshot_version*/, DATA_CURRENT_VERSION, 
  table_schema->get_micro_index_clustered(), 0/*transfer_seq*/);
  data_desc.get_desc().sstable_index_builder_ = sstable_builder;
  ASSERT_EQ(OB_SUCCESS, ret);
}
void prepare_index_builder(ObWholeDataStoreDesc &data_desc, ObTableSchema *table_schema,
                                          ObSSTableIndexBuilder &sstable_builder)
{
  int ret = OB_SUCCESS;
  prepare_data_desc(data_desc, table_schema, &sstable_builder);
  ret = sstable_builder.init(data_desc.get_desc());
  ASSERT_EQ(OB_SUCCESS, ret);
}
  
    
class TestConcurrencyDefenses : public ::testing::Test
{
public:
  TestConcurrencyDefenses();
  virtual ~TestConcurrencyDefenses();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
protected:
  void prepare_schema();
  void prepare_index_desc(const ObWholeDataStoreDesc &data_desc, ObWholeDataStoreDesc &index_desc);
  void fake_freeze_info();

  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  static const int64_t TEST_ROW_CNT = 1000;
  static const int64_t MAX_TEST_COLUMN_CNT = TEST_COLUMN_CNT + 3;
  static const int64_t SNAPSHOT_VERSION = 2;
  const uint64_t tenant_id_;
  ObTenantFreezeInfoMgr *mgr_;
  ObTableSchema table_schema_;
  ObTableSchema index_schema_;
  ObTenantCompactionMemPool *mem_pool_;
  share::ObTenantBase tenant_base_;
  ObRowGenerate row_generate_;
  ObRowGenerate index_row_generate_;
  ObITable::TableKey table_key_;
  ObDatumRow row_;
  ObDatumRow multi_row_;
  ObArenaAllocator allocator_;
  ObSharedMacroBlockMgr *shared_blk_mgr_;
  ObTimerService *timer_service_;
};

TestConcurrencyDefenses::TestConcurrencyDefenses()
  : tenant_id_(500),
  mgr_(nullptr),
  mem_pool_(nullptr),
  tenant_base_(500),
  shared_blk_mgr_(nullptr),
  timer_service_(nullptr)
{

}

TestConcurrencyDefenses::~TestConcurrencyDefenses()
{
}

void TestConcurrencyDefenses::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "SetUpTestCase");
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}
void TestConcurrencyDefenses::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}
void TestConcurrencyDefenses::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;
  timer_service_ = OB_NEW(ObTimerService, ObModIds::TEST, 500);
  mgr_ = OB_NEW(ObTenantFreezeInfoMgr, ObModIds::TEST);
  shared_blk_mgr_ = OB_NEW(ObSharedMacroBlockMgr, ObModIds::TEST);
  mem_pool_ = OB_NEW(ObTenantCompactionMemPool, ObModIds::TEST);
  tenant_base_.set(timer_service_);
  tenant_base_.set(shared_blk_mgr_);
  tenant_base_.set(mgr_);
  tenant_base_.set(mem_pool_);
  share::ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, timer_service_->start());
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(OB_SUCCESS, mgr_->init(500, *GCTX.sql_proxy_));
  ASSERT_EQ(OB_SUCCESS, mem_pool_->init());
  ASSERT_EQ(OB_SUCCESS, shared_blk_mgr_->init());
  fake_freeze_info();
  ASSERT_EQ(timer_service_, MTL(ObTimerService *));
  ASSERT_EQ(shared_blk_mgr_, MTL(ObSharedMacroBlockMgr *));
  ASSERT_EQ(mgr_, MTL(ObTenantFreezeInfoMgr *));
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
void TestConcurrencyDefenses::TearDown()
{
  row_generate_.reset();
  index_row_generate_.reset();
  table_schema_.reset();
  if (nullptr != timer_service_) {
    timer_service_->stop();
    timer_service_->wait();
    timer_service_->destroy();
    timer_service_ = nullptr;
  }
  tenant_base_.destroy_mtl_module(); // stop threads
  tenant_base_.destroy();
  share::ObTenantEnv::set_tenant(nullptr);
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

void TestConcurrencyDefenses::fake_freeze_info()
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

void TestConcurrencyDefenses::prepare_schema()
{
  ObColumnSchemaV2 column;
  uint64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
  table_schema_.reset();
  index_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_concurrency_defenses"));
  ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_concurrency_defenses"));
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

class TestConcurrencyDefensesStressMacroWriter : public share::ObThreadPool
{
public:
TestConcurrencyDefensesStressMacroWriter();
  virtual ~TestConcurrencyDefensesStressMacroWriter() {}
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
};

TestConcurrencyDefensesStressMacroWriter::TestConcurrencyDefensesStressMacroWriter()
  : tenant_id_(0), thread_cnt_(0), row_count_(0), allocator_(), row_generate_(NULL), data_store_desc_(NULL), start_seq_(NULL),
    lob_iter_(NULL), index_writer_(NULL), sstable_builder_(NULL), lock_(), tenant_base_(500)
{
}

int TestConcurrencyDefensesStressMacroWriter::init(uint64_t tenant_id, const int64_t thread_cnt, ObRowGenerate &row_generate,
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
    share::ObTenantEnv::set_tenant(&tenant_base_);
    tenant_base_.init();
  }
  return ret;
}

void TestConcurrencyDefensesStressMacroWriter::run1()
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_base_);
  start_seq_->set_parallel_degree(get_thread_idx() + 2);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = start_seq_->macro_data_seq_;
  const ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
  ObSSTablePrivateObjectCleaner cleaner;
  try {
    ret = index_writer_->open(data_store_desc_->get_desc(), start_seq_->get_parallel_idx(),
                         seq_param, pre_warm_param, cleaner);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  catch (const std::runtime_error& e) {
    std::cerr << "运行时错误: " << e.what() << std::endl;
  }
  catch (...) {
    std::cerr << "未知异常类型" << std::endl;
  }
}

class TestConcurrencyDefensesStressIndexBuilder : public share::ObThreadPool
{
public:
TestConcurrencyDefensesStressIndexBuilder();
  virtual ~TestConcurrencyDefensesStressIndexBuilder() {}
  int init(uint64_t tenant_id, const int64_t thread_cnt, ObRowGenerate &row_generate, ObWholeDataStoreDesc *data_store_desc,
    ObSSTableIndexBuilder &sstable_builder, ObSSTableSecMetaIterator *lob_iter, ObTableSchema *table_schema,
      ObMacroBlockWriter *index_writer = NULL);
  virtual void run1();

private:
  uint64_t tenant_id_;
  int64_t thread_cnt_;
  int64_t row_count_;
  ObArenaAllocator allocator_;
  ObRowGenerate *row_generate_;
  ObTableSchema *table_schema_;
  ObWholeDataStoreDesc *data_store_desc_;
  ObSSTableSecMetaIterator *lob_iter_;
  ObMacroBlockWriter *index_writer_;
  ObSSTableIndexBuilder *sstable_builder_;
  common::SpinRWLock lock_;
  share::ObTenantBase tenant_base_;

};

TestConcurrencyDefensesStressIndexBuilder::TestConcurrencyDefensesStressIndexBuilder()
  : tenant_id_(0), thread_cnt_(0), row_count_(0), allocator_(), row_generate_(NULL), table_schema_(nullptr), data_store_desc_(NULL), 
    lob_iter_(NULL), index_writer_(NULL), sstable_builder_(NULL), lock_(), tenant_base_(500)
{
}

int TestConcurrencyDefensesStressIndexBuilder::init(uint64_t tenant_id, const int64_t thread_cnt, ObRowGenerate &row_generate,
    ObWholeDataStoreDesc *data_store_desc, ObSSTableIndexBuilder &sstable_builder, ObSSTableSecMetaIterator *lob_iter,
    ObTableSchema *table_schema, ObMacroBlockWriter *index_writer)
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
    table_schema_ = table_schema;
    data_store_desc_ = data_store_desc;
    sstable_builder_ = &sstable_builder;
    lob_iter_ = lob_iter;
    index_writer_ = index_writer;
    set_thread_count(static_cast<int32_t>(thread_cnt));
    share::ObTenantEnv::set_tenant(&tenant_base_);
    tenant_base_.init();
  }
  return ret;
}

void TestConcurrencyDefensesStressIndexBuilder::run1()
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(&tenant_base_);
  try {
    ret = sstable_builder_->init(data_store_desc_->get_desc());
    bool result = (ret == OB_SUCCESS || ret == OB_INIT_TWICE);
    ASSERT_EQ(true, result);
  }
  catch (const std::runtime_error& e) {
    std::cerr << "运行时错误: " << e.what() << std::endl;
  }
  catch (...) {
    std::cerr << "未知异常类型" << std::endl;
  }
}

TEST_F(TestConcurrencyDefenses, test_macro_writer_concurrency_defense)
{
  LOG_INFO("BEGIN TestConcurrencyDefenses.test_macro_writer_concurrency_defense");
  int ret = OB_SUCCESS;

  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, &table_schema_, sstable_builder);

  ObMacroDataSeq data_seq(0);
  TestConcurrencyDefensesStressMacroWriter stress;
  int thread_ct = 3;
  ObMacroBlockWriter *macro_writer = new ObMacroBlockWriter();
  stress.init(tenant_id_, thread_ct, row_generate_, data_desc, data_seq, nullptr, macro_writer);

  ASSERT_EQ(OB_SUCCESS, ret);
  stress.start();
  stress.wait();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, macro_writer->close());
  delete macro_writer;
  LOG_INFO("FINISH TestConcurrencyDefenses.test_macro_writer_concurrency_defense");
}

TEST_F(TestConcurrencyDefenses, test_index_builder_concurrency_defense)
{
  LOG_INFO("BEGIN TestConcurrencyDefenses.test_index_builder_concurrency_defense");
  int ret = OB_SUCCESS;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);

  ObWholeDataStoreDesc data_desc;
  ret = data_desc.init(false/*is_ddl*/, table_schema_, ObLSID(1), ObTabletID(1), MAJOR_MERGE, 
  ObTimeUtility::fast_current_time()/*snapshot_version*/, DATA_CURRENT_VERSION, 
  table_schema_.get_micro_index_clustered(), 0/*transfer_seq*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  data_desc.get_desc().sstable_index_builder_ = &sstable_builder;

  int thread_ct = 3;
  TestConcurrencyDefensesStressIndexBuilder stress;
  stress.init(tenant_id_, thread_ct, row_generate_, &data_desc, sstable_builder, nullptr, &table_schema_, nullptr);

  ASSERT_EQ(OB_SUCCESS, ret);
  stress.start();
  stress.wait();

  ObSSTableMergeRes res;
  ret = sstable_builder.close(res);
  ASSERT_EQ(OB_SUCCESS, ret);
  
  LOG_INFO("FINISH TestConcurrencyDefenses.test_index_builder_concurrency_defense");
}

TEST_F(TestConcurrencyDefenses, test_concurrency_defense_effect)
{
  LOG_INFO("BEGIN TestConcurrencyDefenses.test_concurrency_defense_effect");
  int ret = OB_SUCCESS;
  
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, &table_schema_, sstable_builder);
  
  ObDmlFlag dml = DF_INSERT;

  ObMacroBlockWriter data_writer;
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 0;
  const ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, data_writer.open(data_desc.get_desc(), 0/*parallel_idx*/, seq_param/*start_seq*/, pre_warm_param, cleaner));

  static const int64_t MAX_TEST_COLUMN_CNT = TestConcurrencyDefenses::TEST_COLUMN_CNT + 3;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));

  int64_t test_row_num = TestConcurrencyDefenses::TEST_ROW_CNT;
  ObDatumRow row;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, TestConcurrencyDefenses::TEST_COLUMN_CNT));
  auto start = std::chrono::high_resolution_clock::now();
  for(int64_t i = 0; i < test_row_num; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    convert_to_multi_version_row(row, TestConcurrencyDefenses::TEST_ROWKEY_COLUMN_CNT, TestConcurrencyDefenses::TEST_COLUMN_CNT, 2, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
  }
  auto end = std::chrono::high_resolution_clock::now();
  LOG_INFO("macro_block_writer append costs ns",
            K(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()));
  ASSERT_EQ(OB_SUCCESS, data_writer.close());
  LOG_INFO("FINISH TestConcurrencyDefenses.test_concurrency_defense_effect");
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_concurrency_defenses.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_concurrency_defenses.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}