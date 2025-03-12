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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "../unittest/storage/blocksstable/ob_data_file_prepare.h"
#include "../unittest/storage/blocksstable/ob_row_generate.h"
#include "storage/direct_load/ob_direct_load_sstable_scanner.h"
#include "storage/direct_load/ob_direct_load_sstable_compactor.h"
#include "mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace share;
using namespace table;

static ObSimpleMemLimitGetter getter;

namespace unittest
{
class TestDataBlockWriter : public TestDataFilePrepare
{
public:
  static const int64_t rowkey_column_count = 2;
  // Every ObObjType from ObTinyIntType to ObHexStringType inclusive.
  // Skip ObNullType and ObExtendType because for external usage, a column type
  // can't be NULL or NOP.
  static const int64_t column_num = ObHexStringType + 1;
  static const int64_t macro_block_size = 2L * 8 * 1024L;
  static const int64_t SNAPSHOT_VERSION = 2;

  class RowsGuard;
  class SSTableGuard;
  class SSTableScannerGuard;

public:
  TestDataBlockWriter() : TestDataFilePrepare(&getter, "TestDataBlockWriter", 2 * 1024 * 1024, 2048){};
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
  void build_sstable(const RowsGuard &rows_guard,
                     SSTableGuard &sstable_guard,
                     const int64_t start = 0,
                     const int64_t count = -1);
  void compact_sstable(const SSTableGuard &sstable_guard1,
                       const SSTableGuard &sstable_guard2,
                       SSTableGuard &result);
  void scan_sstable(const SSTableGuard &sstable_guard,
                    const ObDatumRange &datum_range,
                    SSTableScannerGuard &scanner_guard);
  void scan_sstable_index_block_meta(const SSTableGuard &sstable_guard,
                                     SSTableScannerGuard &scanner_guard);
  void check_row(const ObDirectLoadDatumRow *next_row, const ObDirectLoadDatumRow *curr_row);
  void make_string(const int64_t size, ObString &str, ObIAllocator &allocator);
  int init_tenant_mgr()
  {
    int ret = OB_SUCCESS;
    ObAddr self;
    obrpc::ObSrvRpcProxy rpc_proxy;
    obrpc::ObCommonRpcProxy rs_rpc_proxy;
    share::ObRsMgr rs_mgr;
    self.set_ip_addr("127.0.0.1", 8086);
    rpc::frame::ObReqTransport req_transport(NULL, NULL);
    const int64_t ulmt = 128LL << 30;
    const int64_t llmt = 128LL << 30;
    ret = getter.add_tenant(OB_SYS_TENANT_ID, ulmt, llmt);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = getter.add_tenant(OB_SERVER_TENANT_ID, ulmt, llmt);
    EXPECT_EQ(OB_SUCCESS, ret);
    lib::set_memory_limit(128LL << 32);
    return ret;
  }

private:
  void prepare_schema();

protected:
  class RowsGuard
  {
    friend class RowGenerate;
  public:
    RowsGuard() {}
    ~RowsGuard() { reset(); }
    void reset()
    {
      for (int64_t i = 0; i < rows_.count(); ++i) {
        ObDirectLoadDatumRow *row = rows_.at(i);
        row->~ObDirectLoadDatumRow();
      }
      rows_.reset();
      allocator_.reset();
    }
    int get_new_row(ObDirectLoadDatumRow *&row)
    {
      int ret = OB_SUCCESS;
      row = nullptr;
      if (OB_ISNULL(row = OB_NEWx(ObDirectLoadDatumRow, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to new ObDirectLoadDatumRow", KR(ret));
      } else if (OB_FAIL(row->init(column_num, &allocator_))) {
        STORAGE_LOG(WARN, "fail to init datum row", KR(ret));
      } else if (FALSE_IT(row->seq_no_ = 0)) {
      } else if (OB_FAIL(rows_.push_back(row))) {
        STORAGE_LOG(WARN, "fail to push back", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != row) {
          row->~ObDirectLoadDatumRow();
          row = nullptr;
        }
      }
      return ret;
    }
    ObDirectLoadDatumRow *at(int64_t idx) { return rows_.at(idx); }
    const ObDirectLoadDatumRow *at(int64_t idx) const { return rows_.at(idx); }
    int64_t count() const { return rows_.count(); }
    ObArray<ObDirectLoadDatumRow *> &get_rows() { return rows_; }
    const ObArray<ObDirectLoadDatumRow *> &get_rows() const { return rows_; }
  private:
    ObArenaAllocator allocator_;
    ObArray<ObDirectLoadDatumRow *> rows_;
  };

  class SSTableGuard
  {
  public:
    SSTableGuard() : sstable_(nullptr) {}
    void reset()
    {
      sstable_ = nullptr;
      table_array_.reset();
    }
    ObDirectLoadSSTable *get_sstable() const { return sstable_; }
  private:
    ObDirectLoadTableHandleArray table_array_;
    ObDirectLoadSSTable *sstable_;
  };

  class SSTableScannerGuard
  {
  public:
    SSTableScannerGuard()
      : scanner_(nullptr),
        meta_iter_(nullptr)
    {
    }
    ~SSTableScannerGuard()
    {
      reset();
    }
    void reset()
    {
      if (nullptr != scanner_) {
        scanner_->~ObDirectLoadSSTableScanner();
        scanner_ = nullptr;
      }
      if (nullptr != meta_iter_) {
        meta_iter_->~ObDirectLoadIndexBlockMetaIterator();
        meta_iter_ = nullptr;
      }
      allocator_.reset();
    }
    ObDirectLoadSSTableScanner *get_scanner() const { return scanner_; }
    ObDirectLoadIndexBlockMetaIterator *get_meta_iter() const { return meta_iter_; }
  private:
    ObArenaAllocator allocator_;
    ObDirectLoadSSTableScanner *scanner_;
    ObDirectLoadIndexBlockMetaIterator *meta_iter_;
  };

  class RowGenerate
  {
  public:
    RowGenerate() = default;
    ~RowGenerate()
    {
      reuse();
    }
    void reuse()
    {
      for (int64_t i = 0; i < rows_.count(); ++i) {
        ObDirectLoadDatumRow *row = rows_.at(i);
        row->~ObDirectLoadDatumRow();
      }
      rows_.reset();
      allocator_.reset();
    }
    int init(const share::schema::ObTableSchema &src_schema,
             bool is_multi_version_row = false)
    {
      return row_generate_.init(src_schema, is_multi_version_row);
    }
    int generate_rows(RowsGuard &rows_guard, const int64_t count)
    {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ObDirectLoadDatumRow *row = nullptr;
        if (OB_FAIL(rows_guard.get_new_row(row))) {
          STORAGE_LOG(WARN, "fail to get new row", KR(ret));
        } else {
          datum_row_.storage_datums_ = row->storage_datums_;
          datum_row_.count_ = row->count_;
          if (OB_FAIL(row_generate_.get_next_row(datum_row_))) {
            STORAGE_LOG(WARN, "fail to generate row", KR(ret));
          }
        }
      }
      return ret;
    }
  private:
    ObArenaAllocator allocator_;
    ObRowGenerate row_generate_;
    ObDatumRow datum_row_;
    ObArray<ObDirectLoadDatumRow *> rows_;
  };

protected:
  ObTableSchema table_schema_;
  ObStorageDatumUtils datum_utils_;
  ObDirectLoadTableDataDesc table_data_desc_;
  RowGenerate row_generate_;
  ObDirectLoadTmpFileManager *file_mgr_;
  ObDirectLoadTableManager *table_mgr_;
};

void TestDataBlockWriter::make_string(const int64_t size, ObString &str, ObIAllocator &allocator)
{
  ASSERT_TRUE(size > 0);
  char *ptr = reinterpret_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, ptr);
  str.assign(ptr, size);
}

void TestDataBlockWriter::build_sstable(
  const RowsGuard &rows_guard,
  SSTableGuard &sstable_guard,
  const int64_t start,
  const int64_t count)
{
  sstable_guard.reset();

  ObDirectLoadSSTableBuildParam param;
  ObDirectLoadSSTableBuilder sstable_builder;
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils_;
  param.file_mgr_ = file_mgr_;
  ASSERT_EQ(OB_SUCCESS, sstable_builder.init(param));
  const int64_t end = count == -1 ? rows_guard.count() : MIN(start + count, rows_guard.count());
  for (int64_t i = start; i < end; ++i) {
    const ObDirectLoadDatumRow *row = rows_guard.at(i);
    ASSERT_EQ(OB_SUCCESS, sstable_builder.append_row(table_schema_.get_tablet_id(), *row));
  }
  ASSERT_EQ(OB_SUCCESS, sstable_builder.close());

  ObDirectLoadTableHandleArray table_array;
  ObDirectLoadSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable_builder.get_tables(table_array, table_mgr_));
  ASSERT_EQ(1, table_array.count());
  ASSERT_TRUE(table_array.at(0).get_table()->is_sstable());
  ASSERT_NE(nullptr, sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0).get_table()));

  ASSERT_EQ(OB_SUCCESS, sstable_guard.table_array_.assign(table_array));
  sstable_guard.sstable_ = sstable;
}

void TestDataBlockWriter::compact_sstable(
  const SSTableGuard &sstable_guard1,
  const SSTableGuard &sstable_guard2,
  SSTableGuard &result)
{
  ASSERT_NE(nullptr, sstable_guard1.get_sstable());
  ASSERT_NE(nullptr, sstable_guard2.get_sstable());
  result.reset();

  ObDirectLoadSSTableCompactParam compact_param;
  ObDirectLoadSSTableCompactor compactor;
  compact_param.tablet_id_ = table_schema_.get_tablet_id();
  compact_param.table_data_desc_ = table_data_desc_;
  compact_param.datum_utils_ = &datum_utils_;
  ASSERT_EQ(OB_SUCCESS, compactor.init(compact_param));
  ASSERT_EQ(OB_SUCCESS, compactor.add_table(sstable_guard1.table_array_.at(0)));
  ASSERT_EQ(OB_SUCCESS, compactor.add_table(sstable_guard2.table_array_.at(0)));
  ASSERT_EQ(OB_SUCCESS, compactor.compact());

  ObDirectLoadTableHandle table_handle;
  ObDirectLoadSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, compactor.get_table(table_handle, table_mgr_));
  ASSERT_TRUE(table_handle.is_valid());
  ASSERT_TRUE(table_handle.get_table()->is_sstable());
  ASSERT_NE(nullptr, sstable = dynamic_cast<ObDirectLoadSSTable *>(table_handle.get_table()));

  ASSERT_EQ(OB_SUCCESS, result.table_array_.add(table_handle));
  result.sstable_ = sstable;
}

void TestDataBlockWriter::scan_sstable(
  const SSTableGuard &sstable_guard,
  const ObDatumRange &datum_range,
  SSTableScannerGuard &sstable_scanner_guard)
{
  sstable_scanner_guard.reset();
  ObDirectLoadSSTable *sstable = nullptr;
  ASSERT_NE(nullptr, sstable = sstable_guard.get_sstable());
  ASSERT_EQ(OB_SUCCESS, sstable->scan(table_data_desc_, datum_range, &datum_utils_, sstable_scanner_guard.allocator_, sstable_scanner_guard.scanner_));
}

void TestDataBlockWriter::scan_sstable_index_block_meta(
  const SSTableGuard &sstable_guard,
  SSTableScannerGuard &sstable_scanner_guard)
{
  sstable_scanner_guard.reset();
  ObDirectLoadSSTable *sstable = nullptr;
  ASSERT_NE(nullptr, sstable = sstable_guard.get_sstable());
  ASSERT_EQ(OB_SUCCESS, sstable->scan_index_block_meta(sstable_scanner_guard.allocator_, sstable_scanner_guard.meta_iter_));
}

void TestDataBlockWriter::check_row(const ObDirectLoadDatumRow *next_row, const ObDirectLoadDatumRow *curr_row)
{
  int cmp_ret = 0;
  ObDatumRowkey next_key(next_row->storage_datums_, rowkey_column_count);
  ObDatumRowkey curr_key(curr_row->storage_datums_, rowkey_column_count);
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  ASSERT_EQ(OB_SUCCESS, next_key.compare(curr_key, datum_utils, cmp_ret));
  ASSERT_TRUE(cmp_ret == 0);
}

void TestDataBlockWriter::prepare_schema()
{
  ObColumnSchemaV2 column;
  int64_t table_id = 3001;
  // init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_macro_file"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_tablet_id(1);
  table_schema_.set_rowkey_column_num(rowkey_column_count);
  table_schema_.set_max_used_column_id(column_num);

  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    if (i == column_num - 1) {
      obj_type = ObTextType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    if (obj_type == common::ObIntType) {
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObNumberType) {
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    column.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void TestDataBlockWriter::SetUp()
{
  int ret = OB_SUCCESS;
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  // init file
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();
  // init table_schema_
  prepare_schema();
  // init datum_utils_
  ObArray<ObColDesc> col_descs;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ASSERT_EQ(OB_SUCCESS, datum_utils_.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_));
  // init table_data_desc_
  table_data_desc_.rowkey_column_num_ = table_schema_.get_rowkey_column_num();
  table_data_desc_.column_count_ = column_num;
  table_data_desc_.external_data_block_size_ = (2LL << 20);
  table_data_desc_.sstable_index_block_size_ = DIRECT_LOAD_DEFAULT_SSTABLE_INDEX_BLOCK_SIZE;
  table_data_desc_.sstable_data_block_size_ = DIRECT_LOAD_DEFAULT_SSTABLE_DATA_BLOCK_SIZE;
  table_data_desc_.extra_buf_size_ = (2LL << 20);
  table_data_desc_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  // init row_generate_
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_));
  // init file_mgr_
  ASSERT_NE(nullptr, file_mgr_ = OB_NEWx(ObDirectLoadTmpFileManager, (&allocator_)));
  ASSERT_EQ(OB_SUCCESS, file_mgr_->init(table_schema_.get_tenant_id()));
  // init table_mgr_
  ASSERT_NE(nullptr, table_mgr_ = OB_NEWx(ObDirectLoadTableManager, (&allocator_)));
  ASSERT_EQ(OB_SUCCESS, table_mgr_->init());

  ASSERT_EQ(OB_SUCCESS, getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L));
  if (OB_FAIL(ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size))) {
    ASSERT_EQ(OB_INIT_TWICE, ret);
    ret = OB_SUCCESS;
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, init_tenant_mgr());
  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpBlockCache::get_instance().init("tmp_block_cache", 1));
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpPageCache::get_instance().init("sn_tmp_page_cache", 1));

  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);

  ObTimerService *timer_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_new(timer_service));
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_start(timer_service));
  tenant_ctx.set(timer_service);

  tmp_file::ObTenantTmpFileManager *tf_mgr = nullptr;
  EXPECT_EQ(OB_SUCCESS, mtl_new_default(tf_mgr));
  EXPECT_EQ(OB_SUCCESS, tmp_file::ObTenantTmpFileManager::mtl_init(tf_mgr));
  tf_mgr->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.default_wbp_memory_limit_ = 40*1024*1024;
  EXPECT_EQ(OB_SUCCESS, tf_mgr->start());
  tenant_ctx.set(tf_mgr);

  ObTenantEnv::set_tenant(&tenant_ctx);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestDataBlockWriter::TearDown()
{
  file_mgr_->~ObDirectLoadTmpFileManager();
  table_mgr_->~ObDirectLoadTableManager();
  tmp_file::ObTmpBlockCache::get_instance().destroy();
  tmp_file::ObTmpPageCache::get_instance().destroy();
  common::ObClockGenerator::destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
  TestDataFilePrepare::TearDown();
}

TEST_F(TestDataBlockWriter, test_empty_write_and_scan)
{
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  build_sstable(rows_guard, sstable_guard);

  SSTableScannerGuard sstable_scanner_guard;
  ObDatumRange datum_range;
  datum_range.set_whole_range();
  scan_sstable(sstable_guard, datum_range, sstable_scanner_guard);
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;
  ASSERT_NE(nullptr, iter = sstable_scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  scan_sstable_index_block_meta(sstable_guard, sstable_scanner_guard);
  ObDirectLoadIndexBlockMetaIterator* meta_iter = nullptr;
  ObDirectLoadIndexBlockMeta meta;
  ASSERT_NE(nullptr, meta_iter = sstable_scanner_guard.get_meta_iter());
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));
}

TEST_F(TestDataBlockWriter, test_write_and_scan)
{
  const int64_t test_row_num = 100000;
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard, test_row_num));
  build_sstable(rows_guard, sstable_guard);

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;
  ObDirectLoadIndexBlockMetaIterator *meta_iter = nullptr;
  ObDirectLoadIndexBlockMeta meta;

  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  datum_range.start_key_ = ObDatumRowkey(rows_guard.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard.at(max)->storage_datums_, rowkey_column_count);

  // left closed ,right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  int64_t new_min = min + 1;
  for (int64_t i = new_min; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = new_min; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  scan_sstable_index_block_meta(sstable_guard, scanner_guard);
  ASSERT_NE(nullptr, meta_iter = scanner_guard.get_meta_iter());
  int64_t number = meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));
}

TEST_F(TestDataBlockWriter, test_scan_greater_range)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100000;
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard, test_row_num));
  build_sstable(rows_guard, sstable_guard, 0, 5000); // 只写前5000行

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;

  ObDatumRange datum_range;
  int64_t min = 6000;
  int64_t max = 6085;
  datum_range.start_key_ = ObDatumRowkey(rows_guard.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard.at(max)->storage_datums_, rowkey_column_count);

  // left closed, right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open, right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed, right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open, right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
}

TEST_F(TestDataBlockWriter, test_scan_less_range)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10000;
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard, test_row_num));
  build_sstable(rows_guard, sstable_guard, 5000); // 只写后5000行

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;

  ObDatumRange datum_range;
  int64_t min = 3000;
  int64_t max = 4999;
  datum_range.start_key_ = ObDatumRowkey(rows_guard.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard.at(max)->storage_datums_, rowkey_column_count);

  // left closed, right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open, rigth closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open, rigth open
  datum_range.set_left_open();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed, rigth open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
}

TEST_F(TestDataBlockWriter, test_scan_range)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10000;
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard, test_row_num));
  build_sstable(rows_guard, sstable_guard, 0, 5000); // 只写前5000行

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;

  ObDatumRange datum_range;
  int64_t min = 3000;
  int64_t max = 6085;
  datum_range.start_key_ = ObDatumRowkey(rows_guard.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard.at(max)->storage_datums_, rowkey_column_count);

  // left closed, right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i < 5000; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open, rigth closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i < 5000; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open, rigth open
  datum_range.set_left_open();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i < 5000; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed, rigth open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i < 5000; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
}

TEST_F(TestDataBlockWriter, test_write_and_scan_large_low)
{
  int ret = OB_SUCCESS;
  const int32_t value1_size = 20 * 1024;
  ObArenaAllocator allocator;
  ObString large_string;
  make_string(value1_size, large_string, allocator);

  const int64_t test_row_num = 100000;
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard, test_row_num));
  for (int64_t i = 0; i < test_row_num; i += 100) {
    ObDirectLoadDatumRow *row = rows_guard.at(i);
    row->storage_datums_[24].set_string(large_string);
  }
  build_sstable(rows_guard, sstable_guard);

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;
  ObDirectLoadIndexBlockMetaIterator *meta_iter = nullptr;
  ObDirectLoadIndexBlockMeta meta;

  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  datum_range.start_key_ = ObDatumRowkey(rows_guard.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard.at(max)->storage_datums_, rowkey_column_count);

  // left closed, right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right open
  datum_range.set_left_open();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right close
  datum_range.set_left_open();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  scan_sstable_index_block_meta(sstable_guard, scanner_guard);
  ASSERT_NE(nullptr, meta_iter = scanner_guard.get_meta_iter());
  int64_t number= meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));
}

TEST_F(TestDataBlockWriter, test_write_and_scan_range_large_low)
{
  int ret = OB_SUCCESS;
  const int32_t value1_size = 20 * 1024;
  ObArenaAllocator allocator;
  ObString large_string;
  make_string(value1_size, large_string, allocator);

  const int64_t test_row_num = 10000;
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard, test_row_num));
  for (int64_t i = 0; i < test_row_num; i += 100) {
    ObDirectLoadDatumRow *row = rows_guard.at(i);
    row->storage_datums_[24].set_string(large_string);
  }
  build_sstable(rows_guard, sstable_guard, 0, 5000); // 只写前5000行

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;

  ObDatumRange datum_range;
  int64_t min = 6000;
  int64_t max = 6085;
  datum_range.start_key_ = ObDatumRowkey(rows_guard.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard.at(max)->storage_datums_, rowkey_column_count);

  // left closed, right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
}

TEST_F(TestDataBlockWriter, test_scan_range_large_low)
{
  int ret = OB_SUCCESS;
  const int32_t value1_size = 20 * 1024;
  ObArenaAllocator allocator;
  ObString large_string;
  make_string(value1_size, large_string, allocator);

  const int64_t test_row_num = 10000;
  RowsGuard rows_guard;
  SSTableGuard sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard, test_row_num));
  for (int64_t i = 0; i < test_row_num; i += 100) {
    ObDirectLoadDatumRow *row = rows_guard.at(i);
    row->storage_datums_[24].set_string(large_string);
  }
  build_sstable(rows_guard, sstable_guard, 0, 5000); // 只写前5000行

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;

  ObDatumRange datum_range;
  int64_t min = 3000;
  int64_t max = 6085;
  datum_range.start_key_ = ObDatumRowkey(rows_guard.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard.at(max)->storage_datums_, rowkey_column_count);

  // left closed, right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i <= 4999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
}

TEST_F(TestDataBlockWriter, test_write_and_compact)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100000;
  RowsGuard rows_guard1, rows_guard2;
  SSTableGuard sstable_guard1, sstable_guard2, compact_sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard1, test_row_num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard2, test_row_num));
  build_sstable(rows_guard1, sstable_guard1);
  build_sstable(rows_guard2, sstable_guard2);
  compact_sstable(sstable_guard1, sstable_guard2, compact_sstable_guard);

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;
  ObDirectLoadIndexBlockMetaIterator *meta_iter = nullptr;
  ObDirectLoadIndexBlockMeta meta;

  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  datum_range.start_key_ = ObDatumRowkey(rows_guard1.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard2.at(max)->storage_datums_, rowkey_column_count);

  // left closed ,right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  scan_sstable_index_block_meta(compact_sstable_guard, scanner_guard);
  ASSERT_NE(nullptr, meta_iter = scanner_guard.get_meta_iter());
  int64_t number = meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));
}

TEST_F(TestDataBlockWriter, test_write_and_compact_large_row)
{
  int ret = OB_SUCCESS;
  const int32_t value1_size = 20 * 1024;
  ObArenaAllocator allocator;
  ObString large_string;
  make_string(value1_size, large_string, allocator);

  const int64_t test_row_num = 100000;
  RowsGuard rows_guard1, rows_guard2;
  SSTableGuard sstable_guard1, sstable_guard2, compact_sstable_guard;
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard1, test_row_num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.generate_rows(rows_guard2, test_row_num));
  for (int64_t i = 0; i < test_row_num; i += 100) {
    ObDirectLoadDatumRow *row1 = rows_guard1.at(i);
    ObDirectLoadDatumRow *row2 = rows_guard2.at(i);
    row1->storage_datums_[24].set_string(large_string);
    row2->storage_datums_[24].set_string(large_string);
  }
  build_sstable(rows_guard1, sstable_guard1);
  build_sstable(rows_guard2, sstable_guard2);
  compact_sstable(sstable_guard1, sstable_guard2, compact_sstable_guard);

  SSTableScannerGuard scanner_guard;
  ObDirectLoadSSTableScanner *iter = nullptr;
  const ObDirectLoadDatumRow *datum_row = nullptr;
  ObDirectLoadIndexBlockMetaIterator *meta_iter = nullptr;
  ObDirectLoadIndexBlockMeta meta;

  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  datum_range.start_key_ = ObDatumRowkey(rows_guard1.at(min)->storage_datums_, rowkey_column_count);
  datum_range.end_key_ = ObDatumRowkey(rows_guard2.at(max)->storage_datums_, rowkey_column_count);

  // left closed ,right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  scan_sstable(compact_sstable_guard, datum_range, scanner_guard);
  ASSERT_NE(nullptr, iter = scanner_guard.get_scanner());
  for (int64_t i = (min + 1); i <=99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, rows_guard2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  scan_sstable_index_block_meta(compact_sstable_guard, scanner_guard);
  ASSERT_NE(nullptr, meta_iter = scanner_guard.get_meta_iter());
  int64_t number = meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_direct_load_data_block_writer.log");
  OB_LOGGER.set_file_name("test_direct_load_data_block_writer.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
