/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public

#include "ob_index_block_data_prepare.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

class TestSkipIndexMetaRowFull : public TestIndexBlockDataPrepare
{
public:
  TestSkipIndexMetaRowFull()
      : TestIndexBlockDataPrepare("Test Skip Index Meta Row Full",
                                  /* merge_type */ MAJOR_MERGE,
                                  /* need_aggregate_data */ true),
        min_int_(0),
        max_int_(0),
        min_double_(0),
        max_double_(0),
        min_float_(0),
        max_float_(0),
        min_smallint_(0),
        max_smallint_(0),
        min_char_(),
        max_char_(),
        null_cnt_int_(0),
        null_cnt_double_(0),
        null_cnt_float_(0),
        null_cnt_smallint_(0),
        null_cnt_char_(0),
        null_cnt_varchar_(0),
        has_min_int_(false),
        has_max_int_(false),
        has_min_double_(false),
        has_max_double_(false),
        has_min_float_(false),
        has_max_float_(false),
        has_min_smallint_(false),
        has_max_smallint_(false),
        has_min_char_(false),
        has_max_char_(false),
        has_min_varchar_(false),
        has_max_varchar_(false)
  {
  }

protected:
  void prepare_schema() override
  {
    const uint64_t table_id = TEST_TABLE_ID;
    table_schema_.reset();
    index_schema_.reset();

    ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_skip_index_meta_row_full"));
    table_schema_.set_tenant_id(1);
    table_schema_.set_tablegroup_id(1);
    table_schema_.set_database_id(1);
    table_schema_.set_table_id(table_id);
    table_schema_.set_rowkey_column_num(2);
    table_schema_.set_max_used_column_id(6);
    table_schema_.set_block_size(2 * 1024);
    table_schema_.set_compress_func_name("none");
    table_schema_.set_row_store_type(ENCODING_ROW_STORE);
    table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
    table_schema_.set_micro_index_clustered(false);
    table_schema_.set_micro_block_format_version(ObMicroBlockFormatVersionHelper::LATEST_VERSION);

    ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_skip_index_meta_row_full"));
    index_schema_.set_tenant_id(1);
    index_schema_.set_tablegroup_id(1);
    index_schema_.set_database_id(1);
    index_schema_.set_table_id(table_id);
    index_schema_.set_rowkey_column_num(2);
    index_schema_.set_max_used_column_id(6);
    index_schema_.set_block_size(2 * 1024);
    index_schema_.set_compress_func_name("none");
    index_schema_.set_row_store_type(ENCODING_ROW_STORE);
    index_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
    index_schema_.set_micro_index_clustered(false);

    ObColumnSchemaV2 column;
    char name[OB_MAX_FILE_NAME_LENGTH];
    memset(name, 0, sizeof(name));

    // col0: int64, rowkey
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    snprintf(name, sizeof(name), "c_int64");
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObIntType);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_rowkey_position(1);
    share::schema::ObSkipIndexColumnAttr skip_idx_attr;
    skip_idx_attr.set_min_max();
    column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));

    // col1: double, rowkey
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID + 1);
    snprintf(name, sizeof(name), "c_double");
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObDoubleType);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_rowkey_position(2);
    column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));

    // col2: float, non-rowkey
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID + 2);
    snprintf(name, sizeof(name), "c_float");
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObFloatType);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_rowkey_position(0);
    column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));

    // col3: smallint, non-rowkey
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID + 3);
    snprintf(name, sizeof(name), "c_smallint");
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObSmallIntType);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_rowkey_position(0);
    column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));

    // col4: char, non-rowkey
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID + 4);
    snprintf(name, sizeof(name), "c_char");
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObCharType);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(8);
    column.set_rowkey_position(0);
    column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));

    // col5: varchar, non-rowkey
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID + 5);
    snprintf(name, sizeof(name), "c_varchar");
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObVarcharType);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(32);
    column.set_rowkey_position(0);
    column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));

    ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, true /*multi_version*/));
  }

  void insert_data(ObMacroBlockWriter &data_writer) override
  {
    row_cnt_ = 0;
    const int64_t store_col_cnt =
        table_schema_.get_column_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    ObDatumRow row;
    ObDatumRow mv_row;
    OK(row.init(allocator_, table_schema_.get_column_count()));
    OK(mv_row.init(allocator_, store_col_cnt));

    const int64_t total_rows = 60000; // 10 rows per micro, 2~5 micro per macro => ~1200+ macro blocks
    const int64_t rows_per_micro_block = 10;
    ObRandom macro_pack_rng;
    macro_pack_rng.seed(20251210);
    int64_t macro_fill_target = rows_per_micro_block * macro_pack_rng.get(2, 5);
    while (row_cnt_ < total_rows) {
      // col0 int pattern: even numbers around negative/positive
      const int64_t val_int = row_cnt_ * 2 - 100;
      row.storage_datums_[0].set_int(val_int);

      // col1 double pattern with some nulls
      if (row_cnt_ % 7 == 0) {
        row.storage_datums_[1].set_null();
        ++null_cnt_double_;
      } else {
        const double val_d = 1000.0 - 0.5 * row_cnt_;
        row.storage_datums_[1].set_double(val_d);
        update_min_max_double_(val_d);
      }

      // col2 float pattern with some nulls
      if (row_cnt_ % 9 == 0) {
        row.storage_datums_[2].set_null();
        ++null_cnt_float_;
      } else {
        const float val_f = static_cast<float>(row_cnt_ % 100) / 3.0f;
        row.storage_datums_[2].set_float(val_f);
        update_min_max_float_(val_f);
      }

      // col3 smallint pattern with some nulls
      if (row_cnt_ % 11 == 0) {
        row.storage_datums_[3].set_null();
        ++null_cnt_smallint_;
      } else {
        const int16_t val_small = static_cast<int16_t>((row_cnt_ % 200) - 100);
        row.storage_datums_[3].set_int(val_small);
        update_min_max_smallint_(val_small);
      }

      // col4 char pattern with some nulls
      if (row_cnt_ % 6 == 0) {
        row.storage_datums_[4].set_null();
        ++null_cnt_char_;
      } else {
        char buf_char[8];
        memset(buf_char, 'a' + (row_cnt_ % 26), sizeof(buf_char));
        row.storage_datums_[4].set_string(buf_char, static_cast<int32_t>(sizeof(buf_char)));
        update_min_max_char_(buf_char, static_cast<int32_t>(sizeof(buf_char)));
      }

      // col5 varchar pattern with some nulls
      if (row_cnt_ % 5 == 0) {
        row.storage_datums_[5].set_null();
        ++null_cnt_varchar_;
      } else {
        char buf[32];
        snprintf(buf, sizeof(buf), "val_%03ld", row_cnt_ % 300);
        row.storage_datums_[5].set_string(buf, static_cast<int32_t>(strlen(buf)));
        update_min_max_varchar_(buf);
      }

      update_min_max_int_(val_int);

      convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, DF_INSERT, mv_row);
      OK(data_writer.append_row(mv_row));

      if ((row_cnt_ + 1) % rows_per_micro_block == 0) {
        OK(data_writer.build_micro_block());
      }
      if ((row_cnt_ + 1) >= macro_fill_target) {
        OK(data_writer.try_switch_macro_block());
        macro_fill_target = (row_cnt_ + 1) + rows_per_micro_block * macro_pack_rng.get(2, 5);
      }
      ++row_cnt_;
    }

    STORAGE_LOG(INFO, "finish data prepare for skip-index meta row",
        K(total_rows),
        K(row_cnt_),
        K(data_macro_block_cnt_),
        K(min_int_),
        K(max_int_),
        K(null_cnt_int_),
        K(min_double_),
        K(max_double_),
        K(null_cnt_double_),
        K(min_float_),
        K(max_float_),
        K(null_cnt_float_),
        K(min_smallint_),
        K(max_smallint_),
        K(null_cnt_smallint_),
        K(min_char_),
        K(max_char_),
        K(null_cnt_char_),
        K(min_varchar_),
        K(max_varchar_),
        K(null_cnt_varchar_));
  }

  void update_min_max_int_(const int64_t v)
  {
    if (!has_min_int_) {
      min_int_ = max_int_ = v;
      has_min_int_ = has_max_int_ = true;
    } else {
      min_int_ = std::min(min_int_, v);
      max_int_ = std::max(max_int_, v);
    }
  }

  void update_min_max_double_(const double v)
  {
    if (!has_min_double_) {
      min_double_ = max_double_ = v;
      has_min_double_ = has_max_double_ = true;
    } else {
      min_double_ = std::min(min_double_, v);
      max_double_ = std::max(max_double_, v);
    }
  }

  void update_min_max_float_(const float v)
  {
    if (!has_min_float_) {
      min_float_ = max_float_ = v;
      has_min_float_ = has_max_float_ = true;
    } else {
      min_float_ = std::min(min_float_, v);
      max_float_ = std::max(max_float_, v);
    }
  }

  void update_min_max_smallint_(const int16_t v)
  {
    if (!has_min_smallint_) {
      min_smallint_ = max_smallint_ = v;
      has_min_smallint_ = has_max_smallint_ = true;
    } else {
      min_smallint_ = std::min(min_smallint_, static_cast<int64_t>(v));
      max_smallint_ = std::max(max_smallint_, static_cast<int64_t>(v));
    }
  }

  void update_min_max_char_(const char *buf, const int32_t len)
  {
    if (!has_min_char_) {
      deep_copy_string_(buf, len, min_char_);
      deep_copy_string_(buf, len, max_char_);
      has_min_char_ = has_max_char_ = true;
    } else {
      ObString tmp;
      tmp.assign_ptr(buf, len);
      if (tmp < min_char_) {
        deep_copy_string_(buf, len, min_char_);
      }
      if (tmp > max_char_) {
        deep_copy_string_(buf, len, max_char_);
      }
    }
  }

  void update_min_max_varchar_(const char *str)
  {
    if (!has_min_varchar_) {
      const int32_t len = static_cast<int32_t>(strlen(str));
      deep_copy_string_(str, len, min_varchar_);
      deep_copy_string_(str, len, max_varchar_);
      has_min_varchar_ = has_max_varchar_ = true;
    } else {
      ObString tmp;
      const int32_t len = static_cast<int32_t>(strlen(str));
      tmp.assign_ptr(str, len);
      if (tmp < min_varchar_) {
        deep_copy_string_(str, len, min_varchar_);
      }
      if (tmp > max_varchar_) {
        deep_copy_string_(str, len, max_varchar_);
      }
    }
  }

  void deep_copy_string_(const char *src, const int32_t len, ObString &dest)
  {
    char *buf = static_cast<char *>(allocator_.alloc(len));
    ASSERT_NE(nullptr, buf);
    MEMCPY(buf, src, len);
    dest.assign_ptr(buf, len);
  }

protected:
  int64_t min_int_;
  int64_t max_int_;
  double min_double_;
  double max_double_;
  float min_float_;
  float max_float_;
  int64_t min_smallint_;
  int64_t max_smallint_;
  ObString min_char_;
  ObString max_char_;
  int64_t null_cnt_int_;
  int64_t null_cnt_double_;
  int64_t null_cnt_float_;
  int64_t null_cnt_smallint_;
  int64_t null_cnt_char_;
  int64_t null_cnt_varchar_;
  bool has_min_int_;
  bool has_max_int_;
  bool has_min_double_;
  bool has_max_double_;
  bool has_min_float_;
  bool has_max_float_;
  bool has_min_smallint_;
  bool has_max_smallint_;
  bool has_min_char_;
  bool has_max_char_;
  bool has_min_varchar_;
  bool has_max_varchar_;
  ObString min_varchar_;
  ObString max_varchar_;
};

TEST_F(TestSkipIndexMetaRowFull, sstable_skip_index_row_contains_correct_stats)
{
  ObSSTableMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, sstable_.get_meta(meta_handle));
  const ObSSTableMeta &meta = meta_handle.get_sstable_meta();

  ASSERT_TRUE(meta.has_sstable_skip_index());
  ASSERT_NE(nullptr, meta.get_sstable_skip_index_buf());
  ASSERT_GT(meta.get_sstable_skip_index_size(), 0);
  // 60000 rows, ~10 rows/micro, 2~5 micros/macro => expect 1000+ macro blocks
  ASSERT_GE(data_macro_block_cnt_, 500);

  ObAggRowReader agg_reader;
  ASSERT_EQ(OB_SUCCESS,
            agg_reader.init(meta.get_sstable_skip_index_buf(),
                            meta.get_sstable_skip_index_size()));
  const ObAggRowHeader *header = agg_reader.get_header();
  ASSERT_NE(nullptr, header);
  ASSERT_TRUE(header->is_valid());

  const ObIArray<ObSkipIndexColMeta> &agg_metas =
      root_index_builder_->data_store_desc_.get_desc().get_agg_meta_array();
  const ObIArray<ObColDesc> &col_descs =
      root_index_builder_->data_store_desc_.get_desc().col_desc_->col_desc_array_;

  int64_t verified = 0;
  ObDatum datum;
  for (int64_t i = 0; i < agg_metas.count(); ++i) {
    const ObSkipIndexColMeta &col_meta = agg_metas.at(i);
    ASSERT_LT(col_meta.col_idx_, col_descs.count());
    const ObColDesc &col_desc = col_descs.at(col_meta.col_idx_);
    ASSERT_EQ(OB_SUCCESS, agg_reader.read(col_meta, datum));

    switch (col_desc.col_id_) {
      case OB_APP_MIN_COLUMN_ID: { // c_int64
        if (col_meta.col_type_ == SK_IDX_MIN) {
          ASSERT_EQ(min_int_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "int64_min", K(min_int_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_MAX) {
          ASSERT_EQ(max_int_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "int64_max", K(max_int_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_NULL_COUNT) {
          ASSERT_EQ(null_cnt_int_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "int64_null", K(null_cnt_int_), K(datum));
          ++verified;
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: { // c_double
        if (col_meta.col_type_ == SK_IDX_MIN) {
          ASSERT_DOUBLE_EQ(min_double_, datum.get_double());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "double_min", K(min_double_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_MAX) {
          ASSERT_DOUBLE_EQ(max_double_, datum.get_double());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "double_max", K(max_double_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_NULL_COUNT) {
          ASSERT_EQ(null_cnt_double_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "double_null", K(null_cnt_double_), K(datum));
          ++verified;
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: { // c_float
        if (col_meta.col_type_ == SK_IDX_MIN) {
          ASSERT_FLOAT_EQ(min_float_, datum.get_float());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "float_min", K(min_float_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_MAX) {
          ASSERT_FLOAT_EQ(max_float_, datum.get_float());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "float_max", K(max_float_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_NULL_COUNT) {
          ASSERT_EQ(null_cnt_float_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "float_null", K(null_cnt_float_), K(datum));
          ++verified;
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: { // c_smallint
        if (col_meta.col_type_ == SK_IDX_MIN) {
          ASSERT_EQ(min_smallint_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "smallint_min", K(min_smallint_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_MAX) {
          ASSERT_EQ(max_smallint_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "smallint_max", K(max_smallint_), K(datum));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_NULL_COUNT) {
          ASSERT_EQ(null_cnt_smallint_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "smallint_null", K(null_cnt_smallint_), K(datum));
          ++verified;
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: { // c_char
        if (col_meta.col_type_ == SK_IDX_MIN) {
          ASSERT_EQ(0, min_char_.compare(datum.get_string()));
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "char_min", K(min_char_), K(datum.get_string()));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_MAX) {
          ASSERT_EQ(0, max_char_.compare(datum.get_string()));
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "char_max", K(max_char_), K(datum.get_string()));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_NULL_COUNT) {
          ASSERT_EQ(null_cnt_char_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "char_null", K(null_cnt_char_), K(datum));
          ++verified;
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: { // c_varchar
        if (col_meta.col_type_ == SK_IDX_MIN) {
          ASSERT_EQ(0, min_varchar_.compare(datum.get_string()));
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "varchar_min", K(min_varchar_), K(datum.get_string()));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_MAX) {
          ASSERT_EQ(0, max_varchar_.compare(datum.get_string()));
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "varchar_max", K(max_varchar_), K(datum.get_string()));
          ++verified;
        } else if (col_meta.col_type_ == SK_IDX_NULL_COUNT) {
          ASSERT_EQ(null_cnt_varchar_, datum.get_int());
          STORAGE_LOG(INFO, "skip index value", K(col_desc.col_id_), "type", "varchar_null", K(null_cnt_varchar_), K(datum));
          ++verified;
        }
        break;
      }
      default:
        break;
    }
  }

  // Expect at least min/max/null_count for each of the 3 columns
  ASSERT_GE(verified, 9);
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_skip_index_meta_row.log*");
  OB_LOGGER.set_file_name("test_skip_index_meta_row.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
