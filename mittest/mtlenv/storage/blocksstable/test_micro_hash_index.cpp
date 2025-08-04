/**
 * Copyright (c) 2025 OceanBase
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

#include "ob_index_block_data_prepare.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_tree_cursor.h"

namespace oceanbase
{
class TestMicroHashIndex : public TestIndexBlockDataPrepare
{
public:
  TestMicroHashIndex()
      : TestIndexBlockDataPrepare("Test Hash Index",
                                  /* merge_type */ MINOR_MERGE,
                                  /* need_aggregate_data */ true)
  {
  }

  virtual ~TestMicroHashIndex() {}

  static void SetUpTestCase() { TestIndexBlockDataPrepare::SetUpTestCase(); }

  static void TearDownTestCase() { TestIndexBlockDataPrepare::TearDownTestCase(); }

  virtual void SetUp()
  {
    TestIndexBlockDataPrepare::SetUp();

    ObLSID ls_id(ls_id_);
    ObTabletID tablet_id(tablet_id_);
    ObLSHandle ls_handle;
    ObLSService *ls_svr = MTL(ObLSService *);

    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
  }

  virtual void TearDown()
  {
    tablet_handle_.reset();
    TestIndexBlockDataPrepare::TearDown();
  }

  void prepare_schema() override
  {
    ObColumnSchemaV2 column;
    // init table schema
    uint64_t table_id = TEST_TABLE_ID;
    table_schema_.reset();
    ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_block"));
    table_schema_.set_tenant_id(1);
    table_schema_.set_tablegroup_id(1);
    table_schema_.set_database_id(1);
    table_schema_.set_table_id(table_id);
    table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
    table_schema_.set_max_used_column_id(ObDateTimeType);
    table_schema_.set_block_size(2 * 1024);
    table_schema_.set_compress_func_name("none");
    table_schema_.set_row_store_type(row_store_type_);
    table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
    table_schema_.set_micro_index_clustered(false);
    table_schema_.set_micro_block_format_version(ObMicroBlockFormatVersionHelper::LATEST_VERSION);

    index_schema_.reset();
    ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_index_block"));
    index_schema_.set_tenant_id(1);
    index_schema_.set_tablegroup_id(1);
    index_schema_.set_database_id(1);
    index_schema_.set_table_id(table_id);
    index_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
    index_schema_.set_max_used_column_id(ObDateTimeType);
    index_schema_.set_block_size(2 * 1024);
    index_schema_.set_compress_func_name("none");
    index_schema_.set_row_store_type(row_store_type_);
    index_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
    index_schema_.set_micro_index_clustered(table_schema_.get_micro_index_clustered());

    // init column
    char name[OB_MAX_FILE_NAME_LENGTH];
    memset(name, 0, sizeof(name));
    for (int64_t i = 0; i < ObDateTimeType; ++i) {
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
      if (obj_type == common::ObIntType) {
        column.set_rowkey_position(1);
      } else if (obj_type == common::ObNumberType) {
        column.set_rowkey_position(2);
      } else if (obj_type == common::ObDoubleType) {
        column.set_rowkey_position(3);
      } else if (obj_type == common::ObUInt64Type) {
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

  void insert_data(ObMacroBlockWriter &data_writer) override
  {
    row_cnt_ = 0;

    int64_t seed = min_row_seed_;
    ObDatumRow row;
    ObDatumRow multi_row;
    ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
    ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
    ASSERT_EQ(OB_SUCCESS, row_key_array_.prepare_allocate(max_row_cnt_));
    ObDmlFlag flags[] = {DF_INSERT};

    while (row_cnt_ < max_row_cnt_) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));

      ObDmlFlag dml = flags[row_cnt_ % ARRAYSIZEOF(flags)]; // INSERT / UPDATE / DELETE
      convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, dml, multi_row);
      multi_row.mvcc_row_flag_.set_uncommitted_row(false); // ensure build micro hash index
      ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
      if (row_cnt_ == 0) {
        ObDatumRowkey &start_key = data_writer.last_key_;
        ASSERT_EQ(OB_SUCCESS, start_key.deep_copy(start_key_, allocator_));
      }
      if (row_cnt_ == max_row_cnt_ - 1) {
        ObDatumRowkey &end_key = data_writer.last_key_;
        ASSERT_EQ(OB_SUCCESS, end_key.deep_copy(end_key_, allocator_));
      }
      ASSERT_EQ(OB_SUCCESS, data_writer.last_key_.deep_copy(row_key_array_[row_cnt_], allocator_));
      ++row_cnt_;
      seed = seed + random() % 100 + 1;
      max_row_seed_ = seed;
    }
  }

  ObArray<ObDatumRowkey> row_key_array_;
};

TEST_F(TestMicroHashIndex, hash_index_get_all_row)
{
  ObIndexBlockMacroIterator macro_iter;
  ObDatumRange iter_range;
  iter_range.set_whole_range();

  ASSERT_EQ(OB_SUCCESS,
            macro_iter.open(sstable_,
                            iter_range,
                            tablet_handle_.get_obj()->get_rowkey_read_info(),
                            allocator_,
                            false,
                            true));

  schema_cols_.set_allocator(&allocator_);
  ASSERT_EQ(OB_SUCCESS, schema_cols_.init(table_schema_.get_column_count()));
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(schema_cols_));
  ObTableReadInfo rowkey_read_info;
  ASSERT_EQ(OB_SUCCESS,
            rowkey_read_info.init(allocator_,
                                  table_schema_.get_column_count(),
                                  table_schema_.get_rowkey_column_num(),
                                  lib::is_oracle_mode(),
                                  schema_cols_,
                                  nullptr /*storage_cols_index*/));

  MacroBlockId macro_block_id;
  int64_t start_row_offset;

  int ret = OB_SUCCESS;
  int number_of_micro_block = 0;
  int number_of_hash_index = 0;
  int number_of_find_use_hash_index = 0;
  int64_t row_idx = 0;
  ObDatumRowkey find_rowkey;

  while (OB_SUCC(macro_iter.get_next_macro_block(macro_block_id, start_row_offset))) {
    ObMacroBlockReadInfo read_info;
    ObMacroBlockHandle macro_handle;
    read_info.macro_block_id_ = macro_block_id;
    read_info.offset_ = 0;
    read_info.size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    ASSERT_NE(nullptr,
              read_info.buf_ = reinterpret_cast<char *>(allocator_.alloc(read_info.size_)));
    ASSERT_EQ(OB_SUCCESS, ObBlockManager::async_read_block(read_info, macro_handle));
    ASSERT_EQ(OB_SUCCESS, macro_handle.wait());

    ObMicroBlockBareIterator micro_bare_iter;
    ObMicroBlockData micro_data;
    ASSERT_EQ(
        OB_SUCCESS,
        micro_bare_iter.open(macro_handle.get_buffer(), macro_handle.get_data_size(), true, true));

    int tmp_ret = OB_SUCCESS;
    while (tmp_ret = micro_bare_iter.get_next_micro_block_data(micro_data), OB_SUCCESS == tmp_ret) {
      number_of_micro_block++;
      number_of_hash_index += micro_data.get_micro_header()->is_contain_hash_index();

      ObMicroBlockGetReader<> reader;
      ObMicroBlockAddr block_addr;
      for (int i = 0; i < micro_data.get_micro_header()->row_count_; i++) {
        row_key_array_[row_idx++].deep_copy(find_rowkey, allocator_);
        find_rowkey.datum_cnt_ = table_schema_.get_rowkey_column_num();
        bool exist;
        bool found;
        bool is_equal;
        ObDatumRow row;
        ObDatumRowkey row_key;
        ASSERT_EQ(OB_SUCCESS,
                  reader.exist_row(micro_data, find_rowkey, rowkey_read_info, exist, found));
        ASSERT_EQ(OB_SUCCESS, reader.get_row(block_addr, micro_data, find_rowkey, rowkey_read_info, row));
        ASSERT_TRUE(found);
        ASSERT_TRUE(exist);
        ASSERT_EQ(OB_SUCCESS, row_key.assign(row.storage_datums_, rowkey_read_info.datum_utils_.get_rowkey_count() - 2));
        STORAGE_LOG(INFO, "DebugTest", K(row_key), K(find_rowkey));
        ASSERT_EQ(OB_SUCCESS, row_key.equal(find_rowkey, rowkey_read_info.datum_utils_, is_equal));
        ASSERT_TRUE(is_equal);

        if (micro_data.get_micro_header()->is_contain_hash_index()) {
          bool need_binary_search;
          int64_t find_row_idx;
          ASSERT_EQ(
              OB_SUCCESS,
              reader.locate_rowkey_fast_path(find_rowkey, find_row_idx, need_binary_search, found));
          ASSERT_TRUE(found);
          if (!need_binary_search) {
            number_of_find_use_hash_index++;
          }
        }
      }
    }
    ASSERT_EQ(OB_ITER_END, tmp_ret);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GE(number_of_hash_index, number_of_micro_block * 0.8);
  ASSERT_GE(number_of_find_use_hash_index, max_row_cnt_ * 0.4);
  STORAGE_LOG(INFO, "use_hash_index", K(number_of_find_use_hash_index));
}

TEST_F(TestMicroHashIndex, hash_index_get_not_exist_row)
{
  ObIndexBlockMacroIterator macro_iter;
  ObDatumRange iter_range;
  iter_range.set_whole_range();

  ASSERT_EQ(OB_SUCCESS,
            macro_iter.open(sstable_,
                            iter_range,
                            tablet_handle_.get_obj()->get_rowkey_read_info(),
                            allocator_,
                            false,
                            true));

  schema_cols_.set_allocator(&allocator_);
  ASSERT_EQ(OB_SUCCESS, schema_cols_.init(table_schema_.get_column_count()));
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(schema_cols_));
  ObTableReadInfo rowkey_read_info;
  ASSERT_EQ(OB_SUCCESS,
            rowkey_read_info.init(allocator_,
                                  table_schema_.get_column_count(),
                                  table_schema_.get_rowkey_column_num(),
                                  lib::is_oracle_mode(),
                                  schema_cols_,
                                  nullptr /*storage_cols_index*/));

  MacroBlockId macro_block_id;
  int64_t start_row_offset;

  int ret = OB_SUCCESS;
  int number_of_micro_block = 0;
  int number_of_hash_index = 0;
  int number_of_find_use_hash_index = 0;
  int64_t row_idx = 0;
  ObDatumRowkey find_rowkey;

  while (OB_SUCC(macro_iter.get_next_macro_block(macro_block_id, start_row_offset))) {
    ObMacroBlockReadInfo read_info;
    ObMacroBlockHandle macro_handle;
    read_info.macro_block_id_ = macro_block_id;
    read_info.offset_ = 0;
    read_info.size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    ASSERT_NE(nullptr,
              read_info.buf_ = reinterpret_cast<char *>(allocator_.alloc(read_info.size_)));
    ASSERT_EQ(OB_SUCCESS, ObBlockManager::async_read_block(read_info, macro_handle));
    ASSERT_EQ(OB_SUCCESS, macro_handle.wait());

    ObMicroBlockBareIterator micro_bare_iter;
    ObMicroBlockData micro_data;
    ASSERT_EQ(
        OB_SUCCESS,
        micro_bare_iter.open(macro_handle.get_buffer(), macro_handle.get_data_size(), true, true));

    int tmp_ret = OB_SUCCESS;
    while (tmp_ret = micro_bare_iter.get_next_micro_block_data(micro_data), OB_SUCCESS == tmp_ret) {
      number_of_micro_block++;
      number_of_hash_index += micro_data.get_micro_header()->is_contain_hash_index();

      ObMicroBlockGetReader<> reader;
      for (int i = 0; i < micro_data.get_micro_header()->row_count_; i++) {
        int64_t next_block_row_idx
            = ((row_idx++) + micro_data.get_micro_header()->row_count_) % max_row_cnt_;
        row_key_array_[next_block_row_idx].deep_copy(find_rowkey, allocator_);
        find_rowkey.datum_cnt_ = table_schema_.get_rowkey_column_num();
        bool exist;
        bool found;
        ASSERT_EQ(OB_SUCCESS,
                  reader.exist_row(micro_data, find_rowkey, rowkey_read_info, exist, found));
        ASSERT_FALSE(found);
        ASSERT_FALSE(exist);

        if (micro_data.get_micro_header()->is_contain_hash_index()) {
          bool need_binary_search;
          int64_t find_row_idx;
          ASSERT_EQ(
              OB_SUCCESS,
              reader.locate_rowkey_fast_path(find_rowkey, find_row_idx, need_binary_search, found));
          ASSERT_FALSE(found);
          if (!need_binary_search) {
            number_of_find_use_hash_index++;
          }
        }
      }
    }
    ASSERT_EQ(OB_ITER_END, tmp_ret);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GE(number_of_hash_index, number_of_micro_block * 0.8);
  ASSERT_GE(number_of_find_use_hash_index, max_row_cnt_ * 0.4);
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_micro_hash_index.log*");
  OB_LOGGER.set_file_name("test_micro_hash_index.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
