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
#include <cstdlib>
#include <ctime>
#include "../unittest/storage/blocksstable/ob_data_file_prepare.h"
#include "../unittest/storage/blocksstable/ob_row_generate.h"
#include "observer/table_load/ob_table_load_partition_location.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/table/ob_table_load_define.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "storage/direct_load/ob_direct_load_sstable_scanner.h"
#include "storage/direct_load/ob_direct_load_sstable_compactor.h"
#include "storage/ob_i_store.h"

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

public:
  TestDataBlockWriter() : TestDataFilePrepare(&getter, "TestDataBlockWriter", 8 * 1024 * 1024, 2048){};
  virtual void SetUp();
  virtual void TearDown();
  void check_row(const ObDatumRow *next_row, const ObDatumRow *curr_row);
  void test_alloc(char *&ptr, const int64_t size);

private:
  void prepare_schema();

protected:
  ObTableSchema table_schema_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObRowGenerate row_generate_;
  ObDirectLoadTmpFileManager *file_mgr_;
};

void TestDataBlockWriter::test_alloc(char *&ptr, const int64_t size)
{
  ptr = reinterpret_cast<char *>(allocator_.alloc(size));
  ASSERT_TRUE(nullptr != ptr);
}

void TestDataBlockWriter::check_row(const ObDatumRow *next_row, const ObDatumRow *curr_row)
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
  ObTmpFileManager::get_instance().destroy();
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
  prepare_schema();
  table_data_desc_.rowkey_column_num_ = table_schema_.get_rowkey_column_num();
  table_data_desc_.column_count_ = column_num;
  table_data_desc_.external_data_block_size_ = (2LL << 20);
  table_data_desc_.sstable_index_block_size_ = DIRECT_LOAD_DEFAULT_SSTABLE_INDEX_BLOCK_SIZE;
  table_data_desc_.sstable_data_block_size_ = DIRECT_LOAD_DEFAULT_SSTABLE_DATA_BLOCK_SIZE;
  table_data_desc_.extra_buf_size_ = (2LL << 20);
  table_data_desc_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  table_data_desc_.is_heap_table_ = false;
  table_data_desc_.mem_chunk_size_ = (64LL << 20);
  table_data_desc_.max_mem_chunk_count_ = 128;
  table_data_desc_.merge_count_per_round_ = 64;
  table_data_desc_.heap_table_mem_chunk_size_ = (64LL << 20);
  file_mgr_ = OB_NEWx(ObDirectLoadTmpFileManager, (&allocator_));
  ASSERT_TRUE(nullptr != file_mgr_);
  ret = file_mgr_->init(table_schema_.get_tenant_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_));

  ret = getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  static ObTenantBase tenant_ctx(1);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
}

void TestDataBlockWriter::TearDown()
{
  file_mgr_->~ObDirectLoadTmpFileManager();
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}

TEST_F(TestDataBlockWriter, test_empty_write_and_scan)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObDirectLoadTmpFileManager *file_mgr = new ObDirectLoadTmpFileManager();
  ret = file_mgr->init(table_schema_.get_tenant_id());
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;

  datum_range.set_whole_range();

  ObTableReadInfo read_info;
  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;

  // left closed ,right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  int64_t index_item_count = sstable->get_meta().index_item_count_;
  ObDirectLoadIndexBlockMetaIterator* meta_iter;
  ObDirectLoadIndexBlockMeta meta;
  ret = sstable->scan_index_block_meta(allocator_, meta_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));
  //delete file_mgr; //释放会导致core，因为sstable等都没释放，干脆就都不释放了
}

TEST_F(TestDataBlockWriter, test_write_and_scan)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObDirectLoadTmpFileManager *file_mgr = new ObDirectLoadTmpFileManager();
  ret = file_mgr->init(table_schema_.get_tenant_id());
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    array.push_back(row);
    ret = sstable_builder.append_row(table_schema_.get_tablet_id(), seq_no, *row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  // max = 6085;
  // min = 6083;
  ObDatumRowkey start_key(array.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);

  ObTableReadInfo read_info;
  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;

  // left closed ,right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  for (int64_t i = min; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t new_min = min + 1;
  for (int64_t i = new_min; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = min; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = new_min; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  int64_t index_item_count = sstable->get_meta().index_item_count_;
  ObDirectLoadIndexBlockMetaIterator* meta_iter;
  ObDirectLoadIndexBlockMeta meta;
  ret = sstable->scan_index_block_meta(allocator_, meta_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t number= meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));

  for (int64_t i = 0; i < array.count(); ++i) {
    ObDatumRow *row = array.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
  //delete file_mgr; //同上
}

TEST_F(TestDataBlockWriter, test_write_and_scan_range)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    array.push_back(row);
    if (i < 5000) {
      ret = sstable_builder.append_row(table_schema_.get_tablet_id(), seq_no, *row);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  max = 6085;
  min = 6000;
  ObDatumRowkey start_key(array.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);
  datum_range.set_left_closed();
  datum_range.set_right_closed();

  ObTableReadInfo read_info;
  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  for (int64_t i = 0; i < array.count(); ++i) {
    ObDatumRow *row = array.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
}

TEST_F(TestDataBlockWriter, test_scan_less_range)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    array.push_back(row);
    if (i >= 5000) {
      ret = sstable_builder.append_row(table_schema_.get_tablet_id(), seq_no, *row);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  max = 4999;
  min = 3000;
  ObDatumRowkey start_key(array.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);
  datum_range.set_left_closed();
  datum_range.set_right_closed();

  ObTableReadInfo read_info;
  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;
  // left closed ,right closed
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  // left open ,rigth closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  // left open ,rigth open
  datum_range.set_left_open();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  // left closed ,rigth open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  for (int64_t i = 0; i < array.count(); ++i) {
    ObDatumRow *row = array.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
}
TEST_F(TestDataBlockWriter, test_scan_range)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    array.push_back(row);
    if (i < 5000) {
      ret = sstable_builder.append_row(table_schema_.get_tablet_id(), seq_no, *row);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  max = 6085;
  min = 3000;
  ObDatumRowkey start_key(array.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);
  datum_range.set_left_closed();
  datum_range.set_right_closed();

  ObTableReadInfo read_info;

  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;
  // left closed ,right closed
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  for (int64_t i = min; i <= 4999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  // left open ,rigth closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = (min + 1); i <= 4999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    ASSERT_NO_FATAL_FAILURE(check_row(datum_row, array.at(i)));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  // left open ,rigth open
  datum_range.set_left_open();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = (min + 1); i <= 4999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  // left closed ,rigth open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = min; i <= 4999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  ASSERT_TRUE(datum_row == nullptr);

  for (int64_t i = 0; i < array.count(); ++i) {
    ObDatumRow *row = array.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
}

TEST_F(TestDataBlockWriter, test_write_and_scan_large_low)
{
  int ret = OB_SUCCESS;
  const int64_t index_block_size = DIO_ALIGN_SIZE;
  const int64_t data_block_size = 4 * DIO_ALIGN_SIZE;
  const int64_t test_row_num = 100000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    if (i % 100 == 0) {
      int32_t value1_size = 20 * 1024;
      char *ptr1 = nullptr;
      test_alloc(ptr1, value1_size);
      ASSERT_TRUE(nullptr != ptr1);
      row->storage_datums_[24].set_string(ObString(value1_size, ptr1));
    }
    array.push_back(row);
    ret = sstable_builder.append_row(table_schema_.get_tablet_id(), seq_no, *row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  // max = 6085;
  // min = 6083;
  ObDatumRowkey start_key(array.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);
  datum_range.set_left_closed();
  datum_range.set_right_closed();

  ObTableReadInfo read_info;
  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;
  // left closed ,rigth closed
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  for (int64_t i = min; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right open
  datum_range.set_left_open();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = (min + 1); i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right open
  datum_range.set_left_open();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = (min + 1); i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = min; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

int64_t index_item_count = sstable->get_meta().index_item_count_;
  ObDirectLoadIndexBlockMetaIterator* meta_iter;
  ObDirectLoadIndexBlockMeta meta;
  ret = sstable->scan_index_block_meta(allocator_, meta_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t number= meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
   ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));
  for (int64_t i = 0; i < array.count(); ++i) {
    ObDatumRow *row = array.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
}

TEST_F(TestDataBlockWriter, test_write_and_scan_range_large_low)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    if (i % 100 == 0) {
      int32_t value1_size = 20 * 1024;
      char *ptr1 = nullptr;
      test_alloc(ptr1, value1_size);
      ASSERT_TRUE(nullptr != ptr1);
      row->storage_datums_[24].set_string(ObString(value1_size, ptr1));
    }
    array.push_back(row);
    if (i < 5000) {
      ret = sstable_builder.append_row(table_schema_.get_tablet_id(), seq_no, *row);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  max = 6085;
  min = 6000;
  ObDatumRowkey start_key(array.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);
  datum_range.set_left_closed();
  datum_range.set_right_closed();

  ObTableReadInfo read_info;
  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;

  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  for (int64_t i = 0; i < array.count(); ++i) {
    ObDatumRow *row = array.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
}

TEST_F(TestDataBlockWriter, test_scan_range_large_low)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 10000;
  ObArray<ObDatumRow *> array;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder;
  ret = sstable_builder.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    if (i % 100 == 0) {
      int32_t value1_size = 20 * 1024;
      char *ptr1 = nullptr;
      test_alloc(ptr1, value1_size);
      ASSERT_TRUE(nullptr != ptr1);
      row->storage_datums_[24].set_string(ObString(value1_size, ptr1));
    }
    array.push_back(row);
    if (i < 5000) {
      ret = sstable_builder.append_row(table_schema_.get_tablet_id(), seq_no, *row);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  ret = sstable_builder.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array;
  ret = sstable_builder.get_tables(table_array, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable = nullptr;
  sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(0));
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  max = 6085;
  min = 3000;
  ObDatumRowkey start_key(array.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);
  datum_range.set_left_closed();
  datum_range.set_right_closed();

  ObTableReadInfo read_info;
  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  for (int64_t i = min; i <= 4999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  for (int64_t i = 0; i < array.count(); ++i) {
    ObDatumRow *row = array.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
}

TEST_F(TestDataBlockWriter, test_write_and_compact)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100000;
  ObArray<ObDatumRow *> array1;
  ObArray<ObDatumRow *> array2;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder1;
  ObDirectLoadSSTableBuilder sstable_builder2;
  ret = sstable_builder1.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_builder2.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    array1.push_back(row);
    ret = sstable_builder1.append_row(table_schema_.get_tablet_id(), seq_no, *row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
    array2.push_back(row);
    ret = sstable_builder2.append_row(table_schema_.get_tablet_id(), seq_no, *row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = sstable_builder1.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_builder2.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array1;
  ObArray<ObIDirectLoadPartitionTable *> table_array2;
  ret = sstable_builder1.get_tables(table_array1, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_builder2.get_tables(table_array2, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable1 = nullptr;
  sstable1 = dynamic_cast<ObDirectLoadSSTable *>(table_array1.at(0));
  ASSERT_TRUE(nullptr != sstable1);

  ObDirectLoadSSTable *sstable2 = nullptr;
  sstable2 = dynamic_cast<ObDirectLoadSSTable *>(table_array2.at(0));
  ASSERT_TRUE(nullptr != sstable2);

  ObIDirectLoadPartitionTable *new_sstable = nullptr;
  ObDirectLoadSSTableCompactor compactor;
  ObDirectLoadSSTableCompactParam compact_param;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  compact_param.tablet_id_ = table_schema_.get_tablet_id();
  compact_param.table_data_desc_ = table_data_desc_;
  ObTableReadInfo read_info;

  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  compact_param.datum_utils_ = &(read_info.datum_utils_);

  ret = compactor.init(compact_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compactor.add_table(sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compactor.add_table(sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, compactor.compact());
  ASSERT_EQ(OB_SUCCESS, compactor.get_table(new_sstable, allocator_));
  ObDirectLoadSSTable *sstable = dynamic_cast<ObDirectLoadSSTable *>(new_sstable);
  ASSERT_TRUE(nullptr != sstable);
  ObDirectLoadSSTableScanner *iter = nullptr;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  ObDatumRowkey start_key(array1.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array2.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;

  // left closed ,right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t new_min = min + 1;
  for (int64_t i = new_min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = new_min; i <=99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  int64_t index_item_count = sstable->get_meta().index_item_count_;
  ObDirectLoadIndexBlockMetaIterator* meta_iter;
  ObDirectLoadIndexBlockMeta meta;
  ret = sstable->scan_index_block_meta(allocator_, meta_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t number = meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));

  for (int64_t i = 0; i < array1.count(); ++i) {
    ObDatumRow *row = array1.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
  for (int64_t i = 0; i < array2.count(); ++i) {
    ObDatumRow *row = array2.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
}

TEST_F(TestDataBlockWriter, test_write_and_compact_large_row)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100000;
  ObArray<ObDatumRow *> array1;
  ObArray<ObDatumRow *> array2;
  ObDirectLoadSSTableBuildParam param;
  ObArray<ObColDesc> col_descs;
  ObStorageDatumUtils datum_utils;
  ObTableLoadSequenceNo seq_no(0);
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));
  ret = datum_utils.init(col_descs, rowkey_column_count, lib::is_oracle_mode(), allocator_);
  param.tablet_id_ = table_schema_.get_tablet_id();
  param.table_data_desc_ = table_data_desc_;
  param.datum_utils_ = &datum_utils;
  param.file_mgr_ = file_mgr_;
  ObDirectLoadSSTableBuilder sstable_builder1;
  ObDirectLoadSSTableBuilder sstable_builder2;
  ret = sstable_builder1.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_builder2.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
     if (i % 100 == 0) {
      int32_t value1_size = 20 * 1024;
      char *ptr1 = nullptr;
      test_alloc(ptr1, value1_size);
      ASSERT_TRUE(nullptr != ptr1);
      row->storage_datums_[24].set_string(ObString(value1_size, ptr1));
    }
    array1.push_back(row);
    ret = sstable_builder1.append_row(table_schema_.get_tablet_id(), seq_no, *row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; i < test_row_num; ++i) {
    ObDatumRow *row = OB_NEWx(ObDatumRow, (&allocator_));
    ASSERT_EQ(OB_SUCCESS, row->init(allocator_, column_num));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(*row));
     if (i % 100 == 0) {
      int32_t value1_size = 20 * 1024;
      char *ptr1 = nullptr;
      test_alloc(ptr1, value1_size);
      ASSERT_TRUE(nullptr != ptr1);
      row->storage_datums_[24].set_string(ObString(value1_size, ptr1));
    }
    array2.push_back(row);
    ret = sstable_builder2.append_row(table_schema_.get_tablet_id(), seq_no, *row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = sstable_builder1.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_builder2.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObIDirectLoadPartitionTable *> table_array1;
  ObArray<ObIDirectLoadPartitionTable *> table_array2;
  ret = sstable_builder1.get_tables(table_array1, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_builder2.get_tables(table_array2, allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDirectLoadSSTable *sstable1 = nullptr;
  sstable1 = dynamic_cast<ObDirectLoadSSTable *>(table_array1.at(0));
  ASSERT_TRUE(nullptr != sstable1);

  ObDirectLoadSSTable *sstable2 = nullptr;
  sstable2 = dynamic_cast<ObDirectLoadSSTable *>(table_array2.at(0));
  ASSERT_TRUE(nullptr != sstable2);

  ObIDirectLoadPartitionTable *new_sstable = nullptr;
  ObDirectLoadSSTableCompactor compactor;
  ObDirectLoadSSTableCompactParam compact_param;
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  compact_param.tablet_id_ = table_schema_.get_tablet_id();
  compact_param.table_data_desc_ = table_data_desc_;
  ObTableReadInfo read_info;

  // init access_param
  ret = read_info.init(allocator_, table_schema_.get_column_count(),
                       table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs, nullptr/*storage_cols_index*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = access_param.init_merge_param(table_schema_.get_table_id(), param.tablet_id_, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  compact_param.datum_utils_ = &(read_info.datum_utils_);
  ret = compactor.init(compact_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compactor.add_table(sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compactor.add_table(sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, compactor.compact());
  ASSERT_EQ(OB_SUCCESS, compactor.get_table(new_sstable, allocator_));
  ObDirectLoadSSTable *sstable = dynamic_cast<ObDirectLoadSSTable *>(new_sstable);
  ASSERT_TRUE(nullptr != sstable);

  ObDirectLoadSSTableScanner *iter = nullptr;
  ObDatumRange datum_range;
  int64_t start_count = rand() % test_row_num;
  int64_t end_count = rand() % test_row_num;
  int64_t max = std::max(start_count, end_count);
  int64_t min = std::min(start_count, end_count);
  ObDatumRowkey start_key(array1.at(min)->storage_datums_, rowkey_column_count);
  ObDatumRowkey end_key(array2.at(max)->storage_datums_, rowkey_column_count);

  datum_range.set_start_key(start_key);
  datum_range.set_end_key(end_key);

  // init access_ctx
  const int64_t snapshot_version = ObTimeUtil::current_time_ns();
  ObQueryFlag query_flag(ObQueryFlag::Forward, false /*daily_merge*/, true /*optimize*/,
                         true /*whole_macro_scan*/, false /*full_row*/, false /*index_back*/,
                         false /*query_stat*/
  );
  ObVersionRange trans_version_range;
  query_flag.multi_version_minor_merge_ = false;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = snapshot_version;
  ObStoreCtx store_ctx;
  access_ctx.store_ctx_ = &store_ctx;
  access_ctx.stmt_allocator_ = &allocator_;
  access_ctx.allocator_ = &allocator_;
  access_ctx.is_inited_ = true;

  // left closed ,right closed
  datum_range.set_left_closed();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObDatumRow *datum_row = nullptr;
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_closed();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t new_min = min + 1;
  for (int64_t i = new_min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i <= max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left closed ,right open
  datum_range.set_left_closed();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = min; i <= 99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));

  // left open ,right closed
  datum_range.set_left_open();
  datum_range.set_right_open();
  ret = sstable->scan(param.table_data_desc_, datum_range, param.datum_utils_, allocator_, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = new_min; i <=99999; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array1.at(i));
  }
  for (int64_t i = 0; i < max; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(datum_row));
    check_row(datum_row, array2.at(i));
  }
  ASSERT_EQ(OB_ITER_END, iter->get_next_row(datum_row));
  int64_t index_item_count = sstable->get_meta().index_item_count_;
  ObDirectLoadIndexBlockMetaIterator* meta_iter;
  ObDirectLoadIndexBlockMeta meta;
  ret = sstable->scan_index_block_meta(allocator_, meta_iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t number = meta_iter->get_total_block_count();
  for (int64_t i = 0; i < number; ++i) {
    ASSERT_EQ(OB_SUCCESS, meta_iter->get_next(meta));
  }
  ASSERT_EQ(OB_ITER_END, meta_iter->get_next(meta));

  for (int64_t i = 0; i < array1.count(); ++i) {
    ObDatumRow *row = array1.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
  for (int64_t i = 0; i < array2.count(); ++i) {
    ObDatumRow *row = array2.at(i);
    row->~ObDatumRow();
    allocator_.free(row);
  }
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
