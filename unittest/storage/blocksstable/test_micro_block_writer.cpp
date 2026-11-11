/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/ob_micro_block_writer.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_row_generate.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest
{
class TestMicroBlockWriter : public ::testing::Test
{
public:
  static const int64_t rowkey_column_count = 2;
  // Every ObObjType from ObTinyIntType to ObHexStringType inclusive.
  // Skip ObNullType and ObExtendType because for external usage, a column type
  // can't be NULL or NOP.
  static const int64_t column_num = ObHexStringType;
  static const int64_t macro_block_size = 2L * 1024 * 1024L;
  static const int64_t SNAPSHOT_VERSION = 2;

public:
  TestMicroBlockWriter() : allocator_(ObModIds::TEST), read_info_() {};
  void SetUp();
  virtual void TearDown() {}
  void test_alloc(char *&ptr, const int64_t size);
  void build_micro_block(
      const ObIArray<const ObDatumRow *> &rows,
      const int64_t physical_column_count,
      ObMicroBlockWriter<> &writer,
      ObMicroBlockData &block_data,
      const int64_t min_merged_trans_version = 0,
      const int64_t max_merged_trans_version = 0);

protected:
  ObRowGenerate row_generate_;
  ObArenaAllocator allocator_;
  ObTableReadInfo read_info_;
};

static void convert_to_multi_version_row(const ObDatumRow &org_row,
    const ObTableSchema &schema, const int64_t snapshot_version,
    ObDatumRow &multi_row)
{
  int64_t rowkey_column_count = schema.get_rowkey_column_num();
  for (int64_t i = 0; i < schema.get_column_count(); ++i) {
    if (i < rowkey_column_count) {
      multi_row.storage_datums_[i] = org_row.storage_datums_[i];
    } else {
      multi_row.storage_datums_[i + 2] = org_row.storage_datums_[i];
    }
  }
  multi_row.storage_datums_[schema.get_rowkey_column_num()].set_int(-snapshot_version);
  multi_row.storage_datums_[schema.get_rowkey_column_num() + 1].set_int(0);

  multi_row.row_flag_= org_row.row_flag_;
}

void TestMicroBlockWriter::test_alloc(char *&ptr, const int64_t size)
{
  ptr = reinterpret_cast<char*>(allocator_.alloc(size));
  ASSERT_TRUE(nullptr != ptr);
}

void TestMicroBlockWriter::build_micro_block(
    const ObIArray<const ObDatumRow *> &rows,
    const int64_t physical_column_count,
    ObMicroBlockWriter<> &writer,
    ObMicroBlockData &block_data,
    const int64_t min_merged_trans_version,
    const int64_t max_merged_trans_version)
{
  ASSERT_GT(rows.count(), 0);
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);
  ASSERT_EQ(OB_SUCCESS, writer.init(macro_block_size, rowkey_column_count + 2, physical_column_count));
  if (max_merged_trans_version > 0) {
    ASSERT_GT(min_merged_trans_version, 0);
    ASSERT_LE(min_merged_trans_version, max_merged_trans_version);
    writer.update_merged_trans_version(max_merged_trans_version);
    writer.update_merged_trans_version(min_merged_trans_version);
  }
  for (int64_t i = 0; i < rows.count(); ++i) {
    ASSERT_NE(nullptr, rows.at(i));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(*rows.at(i)));
  }
  ObMicroBlockDesc micro_block_desc;
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block_desc_in_unittest(micro_block_desc));
  ASSERT_EQ(OB_SUCCESS,
            block_data.init_with_prepare_micro_header(
                micro_block_desc.get_block_buf(), micro_block_desc.get_block_size()));
}

void TestMicroBlockWriter::SetUp()
{
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  const int64_t table_id = 3001;
  ObTableSchema table_schema;
  ObColumnSchemaV2 column;
  //init table schema
  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_row_writer"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_count);
  table_schema.set_max_used_column_id(column_num);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < column_num; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    if(obj_type == common::ObIntType){
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObNumberType){
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    column.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  //init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema));
}

TEST_F(TestMicroBlockWriter, test_init)
{
  int ret = OB_SUCCESS;
  ObMicroBlockWriter<> writer;
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);

  //invalid macro_block_size
  ret = writer.init(0, 2, 5);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //invalid column_count
  ret = writer.init(1024, 2, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //invalid rowkey_column_count
  ret = writer.init(1024, 6, 5);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //success
  ret = writer.init(1024, 2, 5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(writer.micro_header_.get_serialize_size() + 4, writer.get_block_size());
  ASSERT_EQ(0, writer.get_row_count());
  ASSERT_EQ(writer.micro_header_.get_serialize_size(), writer.get_data_size());
}

TEST_F(TestMicroBlockWriter, append_success)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, column_num));
  ObDatumRow multi_version_row;
  ASSERT_EQ(OB_SUCCESS, multi_version_row.init(allocator_, column_num + 2));

  ObMicroBlockWriter<> writer;
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);
  ret = writer.init(macro_block_size, rowkey_column_count, column_num + 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  for(int64_t i = 0; i < test_row_num; ++i){
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    convert_to_multi_version_row(row, row_generate_.get_schema(), SNAPSHOT_VERSION, multi_version_row);
    ASSERT_EQ(OB_SUCCESS, writer.append_row(multi_version_row));
  }
  ObMicroBlockDesc micro_block_desc;
  ret = writer.build_micro_block_desc_in_unittest(micro_block_desc);
  ASSERT_EQ(OB_SUCCESS, ret);
  const char *buf = writer.data_buffer_.data();
  const int64_t size = writer.data_buffer_.size();

  //check ObMicroBlockWriter right by ObMicroBlockReader and ObRowGenerate
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, read_info_.init(
      allocator_, 16000, row_generate_.get_schema().get_rowkey_column_num(), lib::is_oracle_mode(), columns, nullptr/*storage_cols_index*/));
  ObMicroBlockReader<> reader;
  ObMicroBlockData block;
  ASSERT_EQ(OB_SUCCESS, block.init_with_prepare_micro_header(buf, size));
  ret = reader.init(block, read_info_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObDatumRow read_row;
  ASSERT_EQ(OB_SUCCESS, read_row.init(allocator_, column_num));
  for(int64_t i = 0; i < test_row_num; ++ i){
    ASSERT_EQ(OB_SUCCESS, reader.get_row(i, read_row));
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, row_generate_.check_one_row(read_row, exist));
    ASSERT_TRUE(exist) << "i: " << i;
    //every obj should equal
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
    for (int64_t j = 0; j < column_num ; ++ j){
      ObCStringHelper helper;
      ASSERT_TRUE(read_row.storage_datums_[j] == row.storage_datums_[j])
        << "\n i: " << i << " j: " << j
        << "\n writer:  "<< helper.convert(row.storage_datums_[j])
        << "\n reader:  " << helper.convert(read_row.storage_datums_[j]);
    }
  }
}

TEST_F(TestMicroBlockWriter, append_row_error)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, column_num));
  ObDatumRow multi_version_row;
  ASSERT_EQ(OB_SUCCESS, multi_version_row.init(allocator_, column_num + 2));
  //not init
  ObMicroBlockWriter<> writer;
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  convert_to_multi_version_row(row, row_generate_.get_schema(), SNAPSHOT_VERSION, multi_version_row);
  ASSERT_EQ(OB_NOT_INIT, writer.append_row(multi_version_row));
  //column count not equal
  ret = writer.init(macro_block_size, rowkey_column_count, column_num + 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  convert_to_multi_version_row(row, row_generate_.get_schema(), SNAPSHOT_VERSION, multi_version_row);
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_row(multi_version_row));
  //fail to write not support type
  ret = writer.init(macro_block_size, rowkey_column_count, column_num + 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
}


TEST_F(TestMicroBlockWriter, build_block_error)
{
  char *buf = NULL;
  int64_t size = 0;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, column_num));
  //not init
  ObMicroBlockWriter<> writer;
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  ASSERT_EQ(OB_NOT_INIT, writer.build_block(buf, size));
}

TEST_F(TestMicroBlockWriter, init_max_column_count)
{
  // data_buffer_ and index_buffer_ in ObMicroBlockWriter should init succeed
  ObMicroBlockWriter<> writer;
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);
  int64_t ret = writer.init(
      common::OB_DEFAULT_MACRO_BLOCK_SIZE,
      1,
      common::OB_ROW_MAX_COLUMNS_COUNT
      );

  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMicroBlockWriter, mvcc_advance_skip_keeps_target_after_early_compaction)
{
  const int64_t physical_column_count = column_num + 2;
  ObRowGenerate multi_version_row_generate;
  ObArray<ObColDesc> multi_version_columns;
  ObArray<ObColDesc> columns;
  ObDatumRow first_row;
  ObDatumRow last_row;
  ObDatumRow target_row;
  ASSERT_EQ(OB_SUCCESS, multi_version_row_generate.init(row_generate_.get_schema(), &allocator_, true));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_multi_version_column_descs(multi_version_columns));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(0)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(1)));
  ASSERT_EQ(OB_SUCCESS, first_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, last_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, target_row.init(allocator_, physical_column_count));

  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                1, 10, ObDmlFlag::DF_INSERT, true, false, true, first_row));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                1, 5, ObDmlFlag::DF_INSERT, false, true, false, last_row));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                2, 10, ObDmlFlag::DF_INSERT, true, true, true, target_row));

  ObMicroBlockWriter<> writer;
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);
  ASSERT_EQ(OB_SUCCESS, writer.init(macro_block_size, rowkey_column_count + 2, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(first_row));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(last_row));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(target_row));
  ObMicroBlockDesc micro_block_desc;
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block_desc_in_unittest(micro_block_desc));
  ObMicroBlockData block_data;
  ASSERT_EQ(OB_SUCCESS,
            block_data.init_with_prepare_micro_header(
                micro_block_desc.get_block_buf(), micro_block_desc.get_block_size()));

  ObTableReadInfo scan_read_info;
  ASSERT_EQ(OB_SUCCESS,
            scan_read_info.init(
                allocator_, column_num, rowkey_column_count, lib::is_oracle_mode(), columns,
                nullptr/*storage_cols_index*/));
  ObTableIterParam iter_param;
  iter_param.table_id_ = 3001;
  iter_param.tablet_id_ = ObTabletID(1);
  iter_param.read_info_ = &scan_read_info;

  ObStoreCtx store_ctx;
  ObTableAccessContext access_context;
  access_context.is_inited_ = true;
  access_context.store_ctx_ = &store_ctx;
  access_context.allocator_ = &allocator_;
  access_context.stmt_allocator_ = &allocator_;
  access_context.query_flag_.scan_order_ = ObQueryFlag::Forward;
  access_context.trans_version_range_.base_version_ = 0;
  access_context.trans_version_range_.snapshot_version_ = 100;
  access_context.trans_version_range_.multi_version_start_ = 0;

  ObSSTable sstable;
  sstable.key_.table_type_ = ObITable::TableType::MINI_SSTABLE;
  ObDatumRange range;
  range.set_whole_range();
  ObMultiVersionMicroBlockRowScanner scanner(allocator_);
  ASSERT_EQ(OB_SUCCESS, scanner.init(iter_param, access_context, &sstable));
  ASSERT_EQ(OB_SUCCESS, scanner.set_range(range));
  const MacroBlockId macro_id(0, 1, 0);
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, scanner.open(macro_id, block_data, true, true));

  ObIMicroBlockRowScanner &base_scanner = scanner;
  ObDatumRowkey first_key(first_row.storage_datums_, rowkey_column_count);
  int64_t skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  bool has_data = false;
  ASSERT_EQ(OB_SUCCESS,
            base_scanner.skip_to_rowkey(
                base_scanner.get_current_pos(), base_scanner.get_last_pos(), first_key,
                true, true, true, skip_row_idx, has_data));
  ASSERT_TRUE(has_data);
  ASSERT_EQ(0, skip_row_idx);

  const ObDatumRow *result_row = nullptr;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(result_row));
  ASSERT_NE(nullptr, result_row);
  ObDatumRowkey actual_first_key(result_row->storage_datums_, rowkey_column_count);
  bool is_equal = false;
  ASSERT_EQ(OB_SUCCESS, first_key.equal(actual_first_key, scan_read_info.get_datum_utils(), is_equal));
  ASSERT_TRUE(is_equal);
  ASSERT_TRUE(scanner.finish_scanning_cur_rowkey_);
  ASSERT_FALSE(scanner.is_last_multi_version_row_);

  ObDatumRowkey target_key(target_row.storage_datums_, rowkey_column_count);
  // Match ObAdvanceSkipScanner: locate with the expanded MVCC lower bound.
  ObDatumRange target_range;
  ObDatumRange multi_version_target_range;
  target_range.set_whole_range();
  target_range.start_key_ = target_key;
  target_range.border_flag_.set_inclusive_start();
  ASSERT_EQ(OB_SUCCESS, target_range.to_multi_version_range(allocator_, multi_version_target_range));
  const int64_t previous_pos = base_scanner.get_current_pos();
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            base_scanner.skip_to_rowkey(
                -1, base_scanner.get_last_pos(), multi_version_target_range.get_start_key(),
                true, true, true, skip_row_idx, has_data));
  ASSERT_EQ(previous_pos, base_scanner.get_current_pos());
  ASSERT_TRUE(scanner.finish_scanning_cur_rowkey_);
  ASSERT_FALSE(scanner.is_last_multi_version_row_);
  skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  has_data = false;
  ASSERT_EQ(OB_SUCCESS,
            base_scanner.skip_to_rowkey(
                base_scanner.get_current_pos(), base_scanner.get_last_pos(), multi_version_target_range.get_start_key(),
                true, true, true, skip_row_idx, has_data));
  ASSERT_TRUE(has_data);
  ASSERT_EQ(2, skip_row_idx);
  ASSERT_TRUE(scanner.finish_scanning_cur_rowkey_);

  result_row = nullptr;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(result_row));
  ASSERT_NE(nullptr, result_row);
  ObDatumRowkey actual_key(result_row->storage_datums_, rowkey_column_count);
  is_equal = false;
  ASSERT_EQ(OB_SUCCESS, target_key.equal(actual_key, scan_read_info.get_datum_utils(), is_equal));
  ASSERT_TRUE(is_equal);
}

TEST_F(TestMicroBlockWriter, mvcc_advance_skip_resets_cross_block_state)
{
  // The target can either start above the snapshot, or need columns from the next block.
  for (const bool target_first_version_visible : {false, true}) {
    SCOPED_TRACE(target_first_version_visible);
    const int64_t physical_column_count = column_num + 2;
    const int64_t data_store_idx = rowkey_column_count + 2;
    const int64_t data_output_idx = rowkey_column_count;
    ObRowGenerate multi_version_row_generate;
    ObArray<ObColDesc> multi_version_columns;
    ObArray<ObColDesc> columns;
    ObDatumRow predecessor_partial;
    ObDatumRow predecessor_compacted;
    ObDatumRow predecessor_last;
    ObDatumRow middle_row;
    ObDatumRow target_new_row;
    ObDatumRow target_old_row;
    ObDatumRow successor_row;
    ASSERT_EQ(OB_SUCCESS, multi_version_row_generate.init(row_generate_.get_schema(), &allocator_, true));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_multi_version_column_descs(multi_version_columns));
    ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(0)));
    ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(1)));
    ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(data_store_idx)));
    ASSERT_EQ(OB_SUCCESS, predecessor_partial.init(allocator_, physical_column_count));
    ASSERT_EQ(OB_SUCCESS, predecessor_compacted.init(allocator_, physical_column_count));
    ASSERT_EQ(OB_SUCCESS, predecessor_last.init(allocator_, physical_column_count));
    ASSERT_EQ(OB_SUCCESS, middle_row.init(allocator_, physical_column_count));
    ASSERT_EQ(OB_SUCCESS, target_new_row.init(allocator_, physical_column_count));
    ASSERT_EQ(OB_SUCCESS, target_old_row.init(allocator_, physical_column_count));
    ASSERT_EQ(OB_SUCCESS, successor_row.init(allocator_, physical_column_count));

    ASSERT_EQ(OB_SUCCESS,
              multi_version_row_generate.get_next_row(
                  1, 100, ObDmlFlag::DF_INSERT, false, false, true, predecessor_partial));
    predecessor_partial.storage_datums_[data_store_idx].set_nop();
    ASSERT_EQ(OB_SUCCESS,
              multi_version_row_generate.get_next_row(
                  1, 90, ObDmlFlag::DF_INSERT, true, false, false, predecessor_compacted));
    predecessor_compacted.storage_datums_[data_store_idx].set_int(11);
    ASSERT_EQ(OB_SUCCESS,
              multi_version_row_generate.get_next_row(
                  1, 80, ObDmlFlag::DF_INSERT, false, true, false, predecessor_last));
    predecessor_last.storage_datums_[data_store_idx].set_int(10);
    ASSERT_EQ(OB_SUCCESS,
              multi_version_row_generate.get_next_row(
                  2, 90, ObDmlFlag::DF_INSERT, true, true, true, middle_row));
    middle_row.storage_datums_[data_store_idx].set_int(22);
    ASSERT_EQ(OB_SUCCESS,
              multi_version_row_generate.get_next_row(
                  3, target_first_version_visible ? 100 : 120, ObDmlFlag::DF_INSERT,
                  !target_first_version_visible, false, true, target_new_row));
    if (target_first_version_visible) {
      target_new_row.storage_datums_[data_store_idx].set_nop();
    } else {
      target_new_row.storage_datums_[data_store_idx].set_int(99);
    }
    ASSERT_EQ(OB_SUCCESS,
              multi_version_row_generate.get_next_row(
                  3, 90, ObDmlFlag::DF_INSERT, false, true, false, target_old_row));
    target_old_row.storage_datums_[data_store_idx].set_int(42);
    ASSERT_EQ(OB_SUCCESS,
              multi_version_row_generate.get_next_row(
                  4, 90, ObDmlFlag::DF_INSERT, true, true, true, successor_row));
    successor_row.storage_datums_[data_store_idx].set_int(77);

    ObArray<const ObDatumRow *> block_rows;
    ObMicroBlockWriter<> predecessor_first_writer;
    ObMicroBlockWriter<> predecessor_second_writer;
    ObMicroBlockWriter<> middle_writer;
    ObMicroBlockWriter<> target_first_writer;
    ObMicroBlockWriter<> target_second_writer;
    ObMicroBlockData predecessor_first_block;
    ObMicroBlockData predecessor_second_block;
    ObMicroBlockData middle_block;
    ObMicroBlockData target_first_block;
    ObMicroBlockData target_second_block;
    ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&predecessor_partial));
    build_micro_block(block_rows, physical_column_count, predecessor_first_writer, predecessor_first_block);
    block_rows.reset();
    ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&predecessor_compacted));
    ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&predecessor_last));
    build_micro_block(block_rows, physical_column_count, predecessor_second_writer, predecessor_second_block);
    block_rows.reset();
    ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&middle_row));
    build_micro_block(block_rows, physical_column_count, middle_writer, middle_block);
    block_rows.reset();
    ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&target_new_row));
    build_micro_block(
        block_rows, physical_column_count, target_first_writer, target_first_block,
        target_first_version_visible ? 100 : 120, target_first_version_visible ? 100 : 120);
    block_rows.reset();
    ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&target_old_row));
    ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&successor_row));
    build_micro_block(block_rows, physical_column_count, target_second_writer, target_second_block);

    ObTableReadInfo scan_read_info;
    ASSERT_EQ(OB_SUCCESS,
              scan_read_info.init(
                  allocator_, column_num, rowkey_column_count, lib::is_oracle_mode(), columns,
                  nullptr/*storage_cols_index*/));
    ObTableIterParam iter_param;
    iter_param.table_id_ = 3001;
    iter_param.tablet_id_ = ObTabletID(1);
    iter_param.read_info_ = &scan_read_info;

    ObStoreCtx store_ctx;
    ObTableAccessContext access_context;
    access_context.is_inited_ = true;
    access_context.store_ctx_ = &store_ctx;
    access_context.allocator_ = &allocator_;
    access_context.stmt_allocator_ = &allocator_;
    access_context.query_flag_.scan_order_ = ObQueryFlag::Forward;
    access_context.trans_version_range_.base_version_ = 0;
    access_context.trans_version_range_.snapshot_version_ = 100;
    access_context.trans_version_range_.multi_version_start_ = 0;

    ObSSTable sstable;
    sstable.key_.table_type_ = ObITable::TableType::MINI_SSTABLE;
    ObDatumRange range;
    range.set_whole_range();
    ObMultiVersionMicroBlockRowScanner scanner(allocator_);
    ASSERT_EQ(OB_SUCCESS, scanner.init(iter_param, access_context, &sstable));
    ASSERT_EQ(OB_SUCCESS, scanner.set_range(range));
    ObIMicroBlockRowScanner &base_scanner = scanner;
    const ObDatumRow *result_row = nullptr;

    ASSERT_EQ(OB_SUCCESS, scanner.open(MacroBlockId(0, 10, 0), predecessor_first_block, true, false));
    ASSERT_EQ(OB_ITER_END, scanner.get_next_row(result_row));
    ASSERT_EQ(nullptr, result_row);
    ASSERT_FALSE(scanner.prev_micro_row_.row_flag_.is_not_exist());
    ASSERT_FALSE(scanner.finish_scanning_cur_rowkey_);

    ASSERT_EQ(OB_SUCCESS, scanner.open(MacroBlockId(0, 11, 0), predecessor_second_block, false, false));
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(result_row));
    ASSERT_NE(nullptr, result_row);
    ASSERT_EQ(11, result_row->storage_datums_[data_output_idx].get_int());
    ASSERT_FALSE(scanner.prev_micro_row_.row_flag_.is_not_exist());
    ASSERT_TRUE(scanner.finish_scanning_cur_rowkey_);
    ASSERT_FALSE(scanner.is_last_multi_version_row_);

    ObDatumRowkey target_key(target_new_row.storage_datums_, rowkey_column_count);
    int64_t skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    bool has_data = true;
    ASSERT_EQ(OB_SUCCESS,
              base_scanner.skip_to_rowkey(
                  base_scanner.get_current_pos(), base_scanner.get_last_pos(), target_key,
                  true, false, false, skip_row_idx, has_data));
    ASSERT_FALSE(has_data);
    ASSERT_TRUE(scanner.prev_micro_row_.row_flag_.is_not_exist());
    ASSERT_TRUE(scanner.finish_scanning_cur_rowkey_);
    ASSERT_TRUE(scanner.is_last_multi_version_row_);

    ASSERT_EQ(OB_SUCCESS, scanner.open(MacroBlockId(0, 12, 0), middle_block, false, false));
    skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    has_data = true;
    ASSERT_EQ(OB_SUCCESS,
              base_scanner.skip_to_rowkey(
                  base_scanner.get_current_pos(), base_scanner.get_last_pos(), target_key,
                  true, false, false, skip_row_idx, has_data));
    ASSERT_FALSE(has_data);

    ASSERT_EQ(OB_SUCCESS, scanner.open(MacroBlockId(0, 13, 0), target_first_block, false, false));
    ASSERT_EQ(target_first_version_visible, scanner.read_row_direct_flag_);
    skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    has_data = false;
    ASSERT_EQ(OB_SUCCESS,
              base_scanner.skip_to_rowkey(
                  base_scanner.get_current_pos(), base_scanner.get_last_pos(), target_key,
                  true, false, false, skip_row_idx, has_data));
    ASSERT_TRUE(has_data);
    result_row = nullptr;
    ASSERT_EQ(OB_ITER_END, scanner.get_next_row(result_row));
    ASSERT_EQ(nullptr, result_row);
    ASSERT_FALSE(scanner.finish_scanning_cur_rowkey_);
    ASSERT_FALSE(scanner.is_last_multi_version_row_);
    ASSERT_EQ(target_first_version_visible, !scanner.prev_micro_row_.row_flag_.is_not_exist());

    ASSERT_EQ(OB_SUCCESS, scanner.open(MacroBlockId(0, 14, 0), target_second_block, false, true));
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(result_row));
    ASSERT_NE(nullptr, result_row);
    ObDatumRowkey actual_key(result_row->storage_datums_, rowkey_column_count);
    bool is_equal = false;
    ASSERT_EQ(OB_SUCCESS, target_key.equal(actual_key, scan_read_info.get_datum_utils(), is_equal));
    ASSERT_TRUE(is_equal);
    ASSERT_EQ(42, result_row->storage_datums_[data_output_idx].get_int());

    result_row = nullptr;
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(result_row));
    ASSERT_NE(nullptr, result_row);
    ObDatumRowkey successor_key(successor_row.storage_datums_, rowkey_column_count);
    ObDatumRowkey actual_successor_key(result_row->storage_datums_, rowkey_column_count);
    is_equal = false;
    ASSERT_EQ(OB_SUCCESS,
              successor_key.equal(actual_successor_key, scan_read_info.get_datum_utils(), is_equal));
    ASSERT_TRUE(is_equal);
    ASSERT_EQ(77, result_row->storage_datums_[data_output_idx].get_int());
    result_row = nullptr;
    ASSERT_EQ(OB_ITER_END, scanner.get_next_row(result_row));
    ASSERT_EQ(nullptr, result_row);
  }
}

TEST_F(TestMicroBlockWriter, mvcc_advance_skip_missing_target)
{
  const int64_t physical_column_count = column_num + 2;
  const int64_t data_store_idx = rowkey_column_count + 2;
  const int64_t data_output_idx = rowkey_column_count;
  ObRowGenerate multi_version_row_generate;
  ObArray<ObColDesc> multi_version_columns;
  ObArray<ObColDesc> columns;
  ObDatumRow first_row;
  ObDatumRow successor_new_row;
  ObDatumRow successor_old_row;
  ObDatumRow missing_row;
  ObDatumRow beyond_last_row;
  ASSERT_EQ(OB_SUCCESS, multi_version_row_generate.init(row_generate_.get_schema(), &allocator_, true));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_multi_version_column_descs(multi_version_columns));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(0)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(1)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(data_store_idx)));
  ASSERT_EQ(OB_SUCCESS, first_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, successor_new_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, successor_old_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, missing_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, beyond_last_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                1, 90, ObDmlFlag::DF_INSERT, true, true, true, first_row));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                3, 120, ObDmlFlag::DF_INSERT, true, false, true, successor_new_row));
  successor_new_row.storage_datums_[data_store_idx].set_int(99);
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                3, 90, ObDmlFlag::DF_INSERT, false, true, false, successor_old_row));
  successor_old_row.storage_datums_[data_store_idx].set_int(42);
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                2, 90, ObDmlFlag::DF_INSERT, true, true, true, missing_row));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                4, 90, ObDmlFlag::DF_INSERT, true, true, true, beyond_last_row));

  ObArray<const ObDatumRow *> block_rows;
  ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&first_row));
  ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&successor_new_row));
  ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&successor_old_row));
  ObMicroBlockWriter<> writer;
  ObMicroBlockData block_data;
  build_micro_block(block_rows, physical_column_count, writer, block_data, 90, 120);

  ObTableReadInfo scan_read_info;
  ASSERT_EQ(OB_SUCCESS,
            scan_read_info.init(
                allocator_, column_num, rowkey_column_count, lib::is_oracle_mode(), columns,
                nullptr/*storage_cols_index*/));
  ObTableIterParam iter_param;
  iter_param.table_id_ = 3001;
  iter_param.tablet_id_ = ObTabletID(1);
  iter_param.read_info_ = &scan_read_info;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_context;
  access_context.is_inited_ = true;
  access_context.store_ctx_ = &store_ctx;
  access_context.allocator_ = &allocator_;
  access_context.stmt_allocator_ = &allocator_;
  access_context.query_flag_.scan_order_ = ObQueryFlag::Forward;
  access_context.trans_version_range_.base_version_ = 0;
  access_context.trans_version_range_.snapshot_version_ = 100;
  access_context.trans_version_range_.multi_version_start_ = 0;

  ObSSTable sstable;
  sstable.key_.table_type_ = ObITable::TableType::MINI_SSTABLE;
  ObDatumRange range;
  range.set_whole_range();
  ObMultiVersionMicroBlockRowScanner scanner(allocator_);
  ASSERT_EQ(OB_SUCCESS, scanner.init(iter_param, access_context, &sstable));
  ASSERT_EQ(OB_SUCCESS, scanner.set_range(range));
  ASSERT_EQ(OB_SUCCESS, scanner.open(MacroBlockId(0, 20, 0), block_data, true, true));
  ASSERT_FALSE(scanner.can_ignore_multi_version_);
  ASSERT_FALSE(scanner.read_row_direct_flag_);
  ObIMicroBlockRowScanner &base_scanner = scanner;
  int64_t skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  bool has_data = false;

  ObDatumRowkey missing_key(missing_row.storage_datums_, rowkey_column_count);
  ASSERT_EQ(OB_SUCCESS,
            base_scanner.skip_to_rowkey(
                base_scanner.get_current_pos(), base_scanner.get_last_pos(), missing_key,
                true, true, true, skip_row_idx, has_data));
  ASSERT_TRUE(has_data);
  ASSERT_EQ(1, skip_row_idx);
  const ObDatumRow *result_row = nullptr;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(result_row));
  ASSERT_NE(nullptr, result_row);
  ObDatumRowkey successor_key(successor_old_row.storage_datums_, rowkey_column_count);
  ObDatumRowkey actual_key(result_row->storage_datums_, rowkey_column_count);
  bool is_equal = false;
  ASSERT_EQ(OB_SUCCESS, successor_key.equal(actual_key, scan_read_info.get_datum_utils(), is_equal));
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(42, result_row->storage_datums_[data_output_idx].get_int());

  ObDatumRowkey beyond_last_key(beyond_last_row.storage_datums_, rowkey_column_count);
  skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  has_data = true;
  ASSERT_EQ(OB_SUCCESS,
            base_scanner.skip_to_rowkey(
                0, base_scanner.get_last_pos(), beyond_last_key,
                true, true, true, skip_row_idx, has_data));
  ASSERT_FALSE(has_data);
  result_row = nullptr;
  ASSERT_EQ(OB_ITER_END, scanner.get_next_row(result_row));
  ASSERT_EQ(nullptr, result_row);
}

TEST_F(TestMicroBlockWriter, mvcc_advance_skip_open_start_skips_all_versions)
{
  const int64_t physical_column_count = column_num + 2;
  ObRowGenerate multi_version_row_generate;
  ObArray<ObColDesc> multi_version_columns;
  ObArray<ObColDesc> columns;
  ObDatumRow first_version;
  ObDatumRow last_version;
  ObDatumRow successor_row;
  ASSERT_EQ(OB_SUCCESS, multi_version_row_generate.init(row_generate_.get_schema(), &allocator_, true));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_multi_version_column_descs(multi_version_columns));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(0)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(multi_version_columns.at(1)));
  ASSERT_EQ(OB_SUCCESS, first_version.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, last_version.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS, successor_row.init(allocator_, physical_column_count));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                2, 20, ObDmlFlag::DF_INSERT, true, false, true, first_version));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                2, 10, ObDmlFlag::DF_INSERT, false, true, false, last_version));
  ASSERT_EQ(OB_SUCCESS,
            multi_version_row_generate.get_next_row(
                3, 10, ObDmlFlag::DF_INSERT, true, true, true, successor_row));

  ObArray<const ObDatumRow *> block_rows;
  ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&first_version));
  ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&last_version));
  ASSERT_EQ(OB_SUCCESS, block_rows.push_back(&successor_row));
  ObMicroBlockWriter<> writer;
  ObMicroBlockData block_data;
  build_micro_block(block_rows, physical_column_count, writer, block_data);

  ObTableReadInfo scan_read_info;
  ASSERT_EQ(OB_SUCCESS,
            scan_read_info.init(
                allocator_, column_num, rowkey_column_count, lib::is_oracle_mode(), columns,
                nullptr/*storage_cols_index*/));
  ObTableIterParam iter_param;
  iter_param.table_id_ = 3001;
  iter_param.tablet_id_ = ObTabletID(1);
  iter_param.read_info_ = &scan_read_info;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_context;
  access_context.is_inited_ = true;
  access_context.store_ctx_ = &store_ctx;
  access_context.allocator_ = &allocator_;
  access_context.stmt_allocator_ = &allocator_;
  access_context.query_flag_.scan_order_ = ObQueryFlag::Forward;
  access_context.trans_version_range_.base_version_ = 0;
  access_context.trans_version_range_.snapshot_version_ = 100;
  access_context.trans_version_range_.multi_version_start_ = 0;

  ObSSTable sstable;
  sstable.key_.table_type_ = ObITable::TableType::MINI_SSTABLE;
  ObDatumRange range;
  range.set_whole_range();
  ObMultiVersionMicroBlockRowScanner scanner(allocator_);
  ASSERT_EQ(OB_SUCCESS, scanner.init(iter_param, access_context, &sstable));
  ASSERT_EQ(OB_SUCCESS, scanner.set_range(range));
  ASSERT_EQ(OB_SUCCESS, scanner.open(MacroBlockId(0, 21, 0), block_data, true, true));

  ObIMicroBlockRowScanner &base_scanner = scanner;
  ObDatumRowkey excluded_key(first_version.storage_datums_, rowkey_column_count);
  int64_t skip_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  bool has_data = false;
  ASSERT_EQ(OB_SUCCESS,
            base_scanner.skip_to_rowkey(
                base_scanner.get_current_pos(), base_scanner.get_last_pos(), excluded_key,
                false, true, true, skip_row_idx, has_data));
  ASSERT_TRUE(has_data);
  ASSERT_EQ(2, skip_row_idx);

  const ObDatumRow *result_row = nullptr;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_row(result_row));
  ASSERT_NE(nullptr, result_row);
  ObDatumRowkey successor_key(successor_row.storage_datums_, rowkey_column_count);
  ObDatumRowkey actual_key(result_row->storage_datums_, rowkey_column_count);
  bool is_equal = false;
  ASSERT_EQ(OB_SUCCESS, successor_key.equal(actual_key, scan_read_info.get_datum_utils(), is_equal));
  ASSERT_TRUE(is_equal);
  result_row = nullptr;
  ASSERT_EQ(OB_ITER_END, scanner.get_next_row(result_row));
  ASSERT_EQ(nullptr, result_row);
}

TEST_F(TestMicroBlockWriter, append_large_row)
{
  // create a large row whose size is 1.5MB
  int64_t large_row_col_cnt = 2;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, large_row_col_cnt));
  row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);

  int32_t value1_size = 1024 * 1024;
  char *ptr1 = nullptr;
  test_alloc(ptr1, value1_size);
  ASSERT_TRUE(nullptr != ptr1);
  row.storage_datums_[0].set_string(ObString(value1_size, ptr1));

  int32_t value2_size = 512 * 1024;
  char *ptr2 = nullptr;
  test_alloc(ptr2, value2_size);
  row.storage_datums_[1].set_string(ObString(value2_size, ptr2));

  ObMicroBlockWriter<> writer;
  writer.data_buffer_.allocator_.set_tenant_id(500);
  writer.index_buffer_.allocator_.set_tenant_id(500);
  int64_t ret = writer.init(common::OB_DEFAULT_MACRO_BLOCK_SIZE, 1, large_row_col_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  // append large row;
  ret = writer.append_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_micro_block_writer.log");
  OB_LOGGER.set_file_name("test_micro_block_writer.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
