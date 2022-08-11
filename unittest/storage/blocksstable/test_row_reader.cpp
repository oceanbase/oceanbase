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

#include <gtest/gtest.h>
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "lib/number/ob_number_v2.h"
#include "ob_row_generate.h"
#include "share/object/ob_obj_cast.h"
#include "storage/blocksstable/ob_column_map.h"

#define protected public
#define private public

#ifndef INT24_MIN
#define INT24_MIN (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX (16777215U)
#endif

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace common::number;
using namespace share::schema;

namespace unittest {
class TestRowReader : public ::testing::Test {
public:
  static const int64_t rowkey_column_count = 1;
  // Every ObObjType from ObTinyIntType to ObHexStringType inclusive.
  // Skip ObNullType and ObExtendType because for external usage, a column type
  // can't be NULL or NOP.
  ObObjType not_test_type[8] = {ObNullType,
      ObExtendType,
      ObUnknownType,
      ObEnumInnerType,
      ObSetInnerType,
      ObNumberFloatType,
      ObURowIDType,
      ObLobType};
  static const int64_t column_num = ObMaxType - sizeof(not_test_type) / sizeof(ObObjType);

public:
  virtual void SetUp();
  virtual void TearDown();
  char* get_serialize_buf()
  {
    return serialize_buf_;
  }

private:
  int build_column_map(const int64_t* columns, const int64_t count);
  int init_column_map(const ObStoreRow& writer_row, ObArray<ObColDesc>& columns);
  char* serialize_buf_;

protected:
  ObRowGenerate row_generate_;
  ObColumnMap column_map_;
  ObTableSchema table_schema_;
  ObArenaAllocator allocator_;
};

int TestRowReader::init_column_map(const ObStoreRow& writer_row, ObArray<ObColDesc>& columns)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObNCharType == obj_type || ObNVarchar2Type == obj_type ||
        ObTinyTextType == obj_type || ObTextType == obj_type || ObMediumTextType == obj_type ||
        ObLongTextType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    } else if (ObRawType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_BINARY);
    }
    columns[i].col_type_ = obj_meta;
  }
  if (OB_FAIL(column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          columns))) {
    STORAGE_LOG(WARN, "failed to init column map");
  }
  return ret;
}

int TestRowReader::build_column_map(const int64_t* columns, const int64_t count)
{
  int ret = OB_SUCCESS;
  column_map_.reset();
  ObArray<ObColDesc> out_cols;
  ObArray<int32_t> projector;
  for (int64_t i = 0; i < count; ++i) {
    int64_t request_id = columns[i];
    const ObColumnSchemaV2* column = table_schema_.get_column_schema(request_id);
    int64_t store_index = table_schema_.get_column_idx(request_id, true);
    if (NULL == column)
      return OB_ERROR;
    ObColDesc col_desc;
    col_desc.col_id_ = request_id;
    col_desc.col_type_ = column->get_meta_type();
    out_cols.push_back(col_desc);
    projector.push_back(static_cast<int32_t>(store_index));
  }
  ret = column_map_.init(allocator_,
      table_schema_.get_schema_version(),
      table_schema_.get_rowkey_column_num(),
      column_num,
      out_cols,
      nullptr,
      &projector);
  if (!column_map_.is_valid())
    return OB_NOT_INIT;
  return ret;
}

void TestRowReader::SetUp()
{
  const int64_t serialize_buf_size_ = 10 * 1024 * 1024;  // 2M
  serialize_buf_ = reinterpret_cast<char*>(ob_malloc(serialize_buf_size_, ObModIds::TEST));
  ASSERT_TRUE(NULL != serialize_buf_);

  const int64_t table_id = 3001;

  oceanbase::common::ObClusterVersion::get_instance().refresh_cluster_version("1.4.70");
  ObColumnSchemaV2 column;
  // init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_row_writer"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_column_count);
  table_schema_.set_max_used_column_id(column_num);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));

  int not_test_index = 0;
  for (int64_t i = 0; i < ObMaxType; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i);
    if (not_test_type[not_test_index] == obj_type) {
      not_test_index++;
      continue;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if (obj_type == common::ObIntType) {
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  // init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_));
}

void TestRowReader::TearDown()
{
  ob_free(serialize_buf_);
}

void append_col(ObStoreRow& row)
{
  int index = row.row_val_.count_;
  ObScale scale;
  scale = ObLobScale::STORE_IN_ROW;
  row.row_val_.cells_[index].meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  row.row_val_.cells_[index].meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  row.row_val_.cells_[index].set_scale(scale);
  row.row_val_.cells_[index++].set_string(ObTinyTextType, "xxxxxx");

  row.row_val_.cells_[index].meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  row.row_val_.cells_[index].meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  row.row_val_.cells_[index].set_scale(scale);
  row.row_val_.cells_[index++].set_string(ObTextType, "xxxxxx");

  row.row_val_.cells_[index].meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  row.row_val_.cells_[index].meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  row.row_val_.cells_[index].set_scale(scale);
  row.row_val_.cells_[index++].set_string(ObMediumTextType, "xxxxxx");

  row.row_val_.cells_[index].meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  row.row_val_.cells_[index].meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  row.row_val_.cells_[index].set_scale(scale);
  row.row_val_.cells_[index++].set_string(ObLongTextType, "xxxxxx");

  row.row_val_.cells_[index++].set_bit(999);
  row.row_val_.cells_[index++].set_enum(88);
  row.row_val_.cells_[index++].set_set(1849);
  // row.row_val_.cells_[index++].set_enum_inner(ObString("jakgja"));
  // row.row_val_.cells_[index++].set_set_inner(ObString("jfao"));

  uint16_t sec = 12;
  row.row_val_.cells_[index++].set_otimestamp_value(ObTimestampTZType, 888, sec);
  row.row_val_.cells_[index++].set_otimestamp_value(ObTimestampLTZType, 888, sec);
  row.row_val_.cells_[index++].set_otimestamp_value(ObTimestampNanoType, 888, sec);
  row.row_val_.cells_[index++].set_raw(ObString("agjaljg"));
  row.row_val_.cells_[index++].set_interval_ym(ObIntervalYMValue(11));
  row.row_val_.cells_[index++].set_interval_ds(ObIntervalDSValue(11, 14));
  uint32_t digits = 8;
  row.row_val_.cells_[index++].set_number_float(ObNumber(3, &digits));

  row.row_val_.cells_[index].meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  row.row_val_.cells_[index].meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  row.row_val_.cells_[index++].set_nvarchar2(ObString("ahgiqh"));

  row.row_val_.cells_[index].meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  row.row_val_.cells_[index].meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  row.row_val_.cells_[index++].set_nchar(ObString("ahkqh"));

  for (int i = 0; i < index; ++i) {
    row.column_ids_[i] = 100 + i;
  }
  row.row_val_.count_ = index;
}

TEST_F(TestRowReader, test_number_float)
{
  ObObj write_obj;
  uint32_t digits = 8;
  write_obj.set_number_float(ObNumber(3, &digits));

  char* buf = get_serialize_buf();
  int64_t buf_size = 2 * 1024 * 1024;
  int64_t pos = 0;

  int ret = OB_SUCCESS;
  ret = write_obj.get_unumber().encode(buf, buf_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  COMMON_LOG(INFO, "write number float", K(ret), K(write_obj), K(write_obj.get_unumber()));

  ObObj read_obj;
  number::ObNumber value;
  int64_t read_pos = 0;
  if (OB_FAIL(value.decode(buf, pos, read_pos))) {
    COMMON_LOG(WARN, "decode number failed", K(ret));
  } else {
    read_obj.set_number_float(value);
    COMMON_LOG(INFO, "read number float", K(ret), K(read_obj), K(value));
  }
  ASSERT_TRUE(read_obj == write_obj);
  /*
    // try in rowWriter
    int num = 50;
    ObRowWriter row_writer;
    oceanbase::common::ObObj objs[num];
    uint16_t column_ids[num];
    ObStoreRow writer_row;
    writer_row.row_val_.cells_ = objs;
    writer_row.row_val_.count_ = column_num;
    writer_row.column_ids_ = column_ids;
    writer_row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    writer_row.dml_ = T_DML_INSERT;

    digits = 0;
    common::ObObjMeta column_type_array[column_num];
    ObObjMeta obj_meta;
    obj_meta.set_type(ObNumberFloatType);
    for(int i = 0; i < column_num; ++i) {
      digits = i;
      writer_row.row_val_.cells_[i].set_number_float(ObNumber(i, &digits));
      column_type_array[i] = obj_meta;
    }

    int64_t rowkey_start_pos = 0;
    int64_t rowkey_length = 0;
    pos = 0;
    ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024,
        pos, rowkey_start_pos, rowkey_length);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "write_row", K(writer_row));

    ObFlatRowReader row_reader;
    ObStoreRow reader_row;
    oceanbase::common::ObObj read_objs[num];
    uint16_t read_column_ids[num];
    reader_row.row_val_.cells_ = read_objs;
    reader_row.row_val_.count_ = column_num;
    reader_row.capacity_ = num;
    reader_row.column_ids_ = read_column_ids;

    ret = row_reader.read_full_row(buf, pos, read_pos, column_type_array, allocator_, reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "reader_row", K(reader_row));

    for (int i = 0; i < column_num; ++i) {
      ASSERT_EQ(writer_row.row_val_.cells_[i], reader_row.row_val_.cells_[i]);
    }
    */
}

TEST_F(TestRowReader, test_cell_reader)
{
  int ret = OB_SUCCESS;
  int num = 50;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[num];
  uint16_t column_ids[num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;
  writer_row.column_ids_ = column_ids;

  ObSparseRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj read_objs[num];
  uint16_t read_column_ids[num];
  reader_row.row_val_.cells_ = read_objs;
  reader_row.row_val_.count_ = num;
  reader_row.capacity_ = num;
  reader_row.column_ids_ = read_column_ids;

  // init ObRowGenerate
  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;
  for (int i = 0; i < 10; ++i) {
    writer_row.row_val_.count_ = column_num;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
    writer_row.is_sparse_row_ = true;
    STORAGE_LOG(INFO, "write_row", K(writer_row));
    pos = 0;
    rowkey_start_pos = 0;
    rowkey_length = 0;
    char* buf = get_serialize_buf();
    ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
    ASSERT_EQ(OB_SUCCESS, ret);

    read_pos = 0;
    ret = row_reader.read_full_row(buf, pos, read_pos, nullptr, allocator_, reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "read_full_row", K(reader_row));
    ASSERT_EQ(reader_row.row_val_.count_, writer_row.row_val_.count_);
    ASSERT_EQ(reader_row.is_sparse_row_, writer_row.is_sparse_row_);
    for (int j = 0; j < reader_row.row_val_.count_; ++j) {
      STORAGE_LOG(INFO, "obj", K(reader_row.row_val_.cells_[j]), K(writer_row.row_val_.cells_[j]));
      if (ObNumberFloatType != reader_row.row_val_.cells_[j].get_type()) {
        ASSERT_TRUE(reader_row.row_val_.cells_[j] == writer_row.row_val_.cells_[j]);
        ASSERT_TRUE(reader_row.column_ids_[j] == writer_row.column_ids_[j]);
      }
    }
  }
}

TEST_F(TestRowReader, init_fail)
{
  int ret = OB_SUCCESS;
  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj objs[column_num];
  reader_row.row_val_.cells_ = objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;
  char* buf = get_serialize_buf();

  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  read_pos = 0;
  // invalid column map
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // invalid buf
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjMeta obj_meta;
    ObObjType obj_type = static_cast<ObObjType>(i);
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    columns[i].col_type_ = obj_meta;
  }
  share::schema::ColumnMap col_id_map(allocator_);
  ret = col_id_map.init(columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          columns));

  ret = row_reader.read_row(NULL, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // invalid  pos
  pos = 10;
  read_pos = 100;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  pos = 100;
  read_pos = -1;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  (void)rowkey_start_pos;
  (void)rowkey_length;
}

TEST_F(TestRowReader, read_flat_row_from_flat_storage)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "read row", K(reader_row));

  bool exist = false;
  ASSERT_EQ(OB_SUCCESS, row_generate_.check_one_row(reader_row, exist));
  ASSERT_TRUE(column_num == reader_row.row_val_.count_);
  // every obj should equal
  for (int64_t i = 0; i < column_num; ++i) {
    STORAGE_LOG(INFO, "compare", K(i), K(writer_row.row_val_.cells_[i]), K(reader_row.row_val_.cells_[i]));
    ASSERT_EQ(writer_row.row_val_.cells_[i], reader_row.row_val_.cells_[i]);
  }
  // new row write success
  ObNewRow new_reader_row;
  new_reader_row.count_ = column_num;
  new_reader_row.cells_ = reader_objs;

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret = row_writer.write(writer_row.row_val_, buf, 2 * 1024 * 1024, FLAT_ROW_STORE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init column type array
  ObObjMeta column_types[common::OB_ROW_MAX_COLUMNS_COUNT];
  for (int64_t i = 0; i < column_num; ++i) {
    column_types[i] = writer_row.row_val_.cells_[i].get_meta();
  }
  // new row read success
  read_pos = 0;
  ret =
      row_reader.read_compact_rowkey(column_types, column_map_.get_request_count(), buf, pos, read_pos, new_reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  // every obj should equal
  for (int64_t i = 0; i < column_num; ++i) {
    ASSERT_TRUE(writer_row.row_val_.cells_[i] == new_reader_row.cells_[i])
        << "\n i: " << i << "\n writer:  " << to_cstring(writer_row.row_val_.cells_[i])
        << "\n reader:  " << to_cstring(reader_row.row_val_.cells_[i]);
  }
}

TEST_F(TestRowReader, read_flat_row_from_flat_storage2)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  // write 100 rows
  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row, SPARSE_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(column_num == reader_row.row_val_.count_);
  // every obj should equal
  int index = 0;
  for (int64_t i = 0; i < column_num; ++i) {
    if (!writer_row.row_val_.cells_[i].is_nop_value()) {
      ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[index]);
      ASSERT_TRUE(columns[i].col_id_ == reader_row.column_ids_[index]);
      index++;
    }
  }
}

TEST_F(TestRowReader, read_overflow)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read_header_overflow
  read_pos = 0;
  ret = row_reader.read_row(buf, 3, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  // read_column_index_overflow
  read_pos = 0;
  ret = row_reader.read_row(buf, sizeof(ObRowHeader), read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  // step == 1, read_columns overflow
  read_pos = 0;
  ret = row_reader.read_row(buf, 200, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);

  // read_sequence_columns overflow
  ObNewRow new_writer_row;
  new_writer_row.count_ = column_num;
  new_writer_row.cells_ = objs;
  ObNewRow new_reader_row;
  new_reader_row.count_ = 0;
  new_reader_row.cells_ = reader_objs;

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret = row_writer.write(new_writer_row, buf, 2 * 1024 * 1024, FLAT_ROW_STORE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init column type array
  ObObjMeta column_types[common::OB_ROW_MAX_COLUMNS_COUNT];
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = new_writer_row.cells_[i].get_type();
    ObObjMeta& obj_meta = column_types[i];
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
  }
  // new row read success
  read_pos = 0;
  ret =
      row_reader.read_compact_rowkey(column_types, column_map_.get_request_count(), buf, 200, read_pos, new_reader_row);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
}

TEST_F(TestRowReader, read_sparse_row_from_flat_storage)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  // set nop value
  int nop_cell_cnt = 5;
  int nop_indexs[] = {5, 9, 12, 18, 21};
  for (int i = 0; i < nop_cell_cnt; ++i) {
    writer_row.row_val_.cells_[nop_indexs[i]].set_nop_value();
  }

  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row, SPARSE_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(column_num - nop_cell_cnt == reader_row.row_val_.count_);
  // every obj should equal
  int index = 0;
  for (int64_t i = 0; i < column_num; ++i) {
    if (!writer_row.row_val_.cells_[i].is_nop_value()) {
      ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[index]);
      ASSERT_TRUE(columns[i].col_id_ == reader_row.column_ids_[index]);
      index++;
    }
  }
}

TEST_F(TestRowReader, flat_row_test)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  // set nop value
  int nop_cell_cnt = 5;
  int nop_indexs[] = {5, 9, 12, 18, 21};
  for (int i = 0; i < nop_cell_cnt; ++i) {
    writer_row.row_val_.cells_[nop_indexs[i]].set_nop_value();
  }
  // write 100 rows
  int row_cnt = 100;
  for (int i = 0; i < row_cnt; ++i) {
    ret = row_writer.write_flat_row(
        rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read success
  read_pos = 0;
  for (int j = 0; j < row_cnt; ++j) {
    ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row, SPARSE_ROW_STORE);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_TRUE(column_num - nop_cell_cnt == reader_row.row_val_.count_);
    // every obj should equal
    int index = 0;
    for (int64_t i = 0; i < column_num; ++i) {
      if (!writer_row.row_val_.cells_[i].is_nop_value()) {
        ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[index]);
        ASSERT_TRUE(columns[i].col_id_ == reader_row.column_ids_[index]);
        index++;
      }
    }
  }
}

TEST_F(TestRowReader, flat_row_test_about_trans_id_flag_change)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  int idx = 0;
  writer_row.row_val_.cells_[idx++].set_int32(1);
  writer_row.row_val_.cells_[idx++].set_int(-INT64_MAX);
  writer_row.row_val_.cells_[idx++].set_int(-36202);
  writer_row.row_val_.cells_[idx++].set_int32(1);
  transaction::ObTransID trans_id(ObAddr(ObAddr::IPV4, "11.158.41.173", 16046));
  writer_row.row_type_flag_.flag_ = 10;
  writer_row.trans_id_ptr_ = &trans_id;
  writer_row.row_val_.count_ = 4;

  // write 100 rows
  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap

  // read success
  read_pos = 0;
  transaction::ObTransID read_trans_id;
  ret = row_reader.setup_row(buf, pos, read_pos, 4, &read_trans_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "read row", K(read_trans_id));
}

TEST_F(TestRowReader, read_flat_row_from_sparse_storage)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader sparse_row_reader;
  ObFlatRowReader flat_row_reader;
  ObStoreRow sparse_row;
  oceanbase::common::ObObj sparse_row_objs[column_num];
  uint16_t sparse_col_id_array[column_num];
  sparse_row.row_val_.cells_ = sparse_row_objs;
  sparse_row.row_val_.count_ = column_num;
  sparse_row.column_ids_ = sparse_col_id_array;
  sparse_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  // set nop value
  int nop_cell_cnt = 5;
  int nop_indexs[] = {5, 9, 12, 18, 21};
  for (int i = 0; i < nop_cell_cnt; ++i) {
    writer_row.row_val_.cells_[nop_indexs[i]].set_nop_value();
  }

  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read success
  read_pos = 0;
  ret = flat_row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, sparse_row, SPARSE_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(column_num - nop_cell_cnt == sparse_row.row_val_.count_);
  // every obj should equal
  int index = 0;
  for (int64_t i = 0; i < column_num; ++i) {
    if (!writer_row.row_val_.cells_[i].is_nop_value()) {
      ASSERT_TRUE(writer_row.row_val_.cells_[i] == sparse_row.row_val_.cells_[index]);
      ASSERT_TRUE(columns[i].col_id_ == sparse_row.column_ids_[index]);
      index++;
    }
  }

  int sparse_buf_size = 10 * 1024;
  char sparse_buf[sparse_buf_size];
  MEMSET(sparse_buf, 0, sparse_buf_size);

  // write sparse row
  pos = 0;
  ret = row_writer.write_sparse_row(
      rowkey_column_count, sparse_row, sparse_buf, sparse_buf_size, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;
  uint16_t col_id_array[column_num];
  reader_row.column_ids_ = col_id_array;

  // read success
  read_pos = 0;
  ret = sparse_row_reader.read_row(sparse_buf, pos, read_pos, column_map_, allocator_, reader_row, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, reader_row.is_sparse_row_);
  ASSERT_TRUE(column_num == reader_row.row_val_.count_);
  // every obj should equal
  for (int64_t i = 0; i < column_num; ++i) {
    if (!writer_row.row_val_.cells_[i].is_nop_value()) {
      ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i]);
    } else {
      ASSERT_EQ(true, reader_row.row_val_.cells_[i].is_nop_value());
    }
  }
}

TEST_F(TestRowReader, read_sparse_row_from_sparse_storage)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader row_reader;
  ObStoreRow sparse_row;
  oceanbase::common::ObObj sparse_row_objs[column_num];
  sparse_row.row_val_.cells_ = sparse_row_objs;
  sparse_row.row_val_.count_ = column_num;
  uint16_t sparse_column_id[column_num];
  sparse_row.column_ids_ = sparse_column_id;
  sparse_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  ObObjMeta meta_type_array[column_num];
  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // set nop value
  int sparse_cell_cnt = 5;
  int sparse_indexs[] = {5, 9, 12, 18, 21, INT_MAX};
  for (int i = 0; i < sparse_cell_cnt; ++i) {
    sparse_row.row_val_.cells_[i] = writer_row.row_val_.cells_[sparse_indexs[i]];
    sparse_row.column_ids_[i] = columns[sparse_indexs[i]].col_id_;
    meta_type_array[i] = columns[sparse_indexs[i]].col_type_;
  }
  sparse_row.row_val_.count_ = sparse_cell_cnt;
  sparse_row.is_sparse_row_ = true;
  sparse_row.flag_ = writer_row.flag_;
  sparse_row.dml_ = writer_row.dml_;

  ret = row_writer.write_sparse_row(
      rowkey_column_count, sparse_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  ObStoreRow reader_flat_row;
  oceanbase::common::ObObj reader_flat_objs[column_num];
  reader_flat_row.row_val_.cells_ = reader_flat_objs;
  reader_flat_row.row_val_.count_ = column_num;
  reader_flat_row.capacity_ = column_num;

  // read success
  read_pos = 0;
  ret = row_reader.read_full_row(buf, pos, read_pos, meta_type_array, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, reader_row.is_sparse_row_);
  ASSERT_TRUE(sparse_cell_cnt == reader_row.row_val_.count_);
  // every obj should equal
  int index = 0;
  for (int64_t i = 0; i < reader_row.row_val_.count_; ++i) {
    ASSERT_TRUE(sparse_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i]);
    ASSERT_TRUE(sparse_row.column_ids_[i] == reader_row.column_ids_[i]);
  }

  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_flat_row, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, reader_flat_row.is_sparse_row_);
  ASSERT_TRUE(column_num == reader_flat_row.row_val_.count_);
  // every obj should equal
  index = 0;
  for (int64_t i = 0; i < column_num; ++i) {
    if (i == sparse_indexs[index]) {
      ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_flat_row.row_val_.cells_[i]);
      index++;
    } else {
      ASSERT_EQ(true, reader_flat_row.row_val_.cells_[i].is_nop_value());
    }
  }
}

TEST_F(TestRowReader, sparse_row_test)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader row_reader;
  ObStoreRow sparse_row;
  oceanbase::common::ObObj sparse_row_objs[column_num];
  sparse_row.row_val_.cells_ = sparse_row_objs;
  sparse_row.row_val_.count_ = column_num;
  uint16_t column_ids[column_num];
  sparse_row.column_ids_ = column_ids;
  sparse_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // set nop value
  int sparse_cell_cnt = 5;
  int sparse_indexs[] = {5, 9, 12, 18, 21, INT_MAX};
  for (int i = 0; i < sparse_cell_cnt; ++i) {
    sparse_row.row_val_.cells_[i] = writer_row.row_val_.cells_[sparse_indexs[i]];
    sparse_row.column_ids_[i] = columns[sparse_indexs[i]].col_id_;
  }
  sparse_row.row_val_.count_ = sparse_cell_cnt;
  sparse_row.is_sparse_row_ = true;
  sparse_row.flag_ = writer_row.flag_;
  sparse_row.dml_ = writer_row.dml_;

  int row_cnt = 100;
  for (int i = 0; i < row_cnt; ++i) {
    ret = row_writer.write_sparse_row(
        rowkey_column_count, sparse_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  read_pos = 0;
  for (int j = 0; j < row_cnt; ++j) {
    // read success
    ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row, SPARSE_ROW_STORE);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(true, reader_row.is_sparse_row_);
    ASSERT_TRUE(sparse_cell_cnt == reader_row.row_val_.count_);
    // every obj should equal
    int index = 0;
    for (int64_t i = 0; i < reader_row.row_val_.count_; ++i) {
      ASSERT_TRUE(sparse_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i]);
      ASSERT_TRUE(sparse_row.column_ids_[i] == reader_row.column_ids_[i]);
    }
  }

  ObStoreRow reader_flat_row;
  oceanbase::common::ObObj reader_flat_objs[column_num];
  reader_flat_row.row_val_.cells_ = reader_flat_objs;
  reader_flat_row.row_val_.count_ = column_num;
  reader_flat_row.capacity_ = column_num;

  // read success
  read_pos = 0;
  reader_row.row_val_.count_ = column_num;
  for (int j = 0; j < row_cnt; ++j) {
    ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_flat_row, FLAT_ROW_STORE);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(false, reader_flat_row.is_sparse_row_);
    ASSERT_TRUE(column_num == reader_flat_row.row_val_.count_);
    // every obj should equal
    int index = 0;
    for (int64_t i = 0; i < column_num; ++i) {
      if (i == sparse_indexs[index]) {
        ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_flat_row.row_val_.cells_[i]);
        index++;
      } else {
        ASSERT_EQ(true, reader_flat_row.row_val_.cells_[i].is_nop_value());
        // STORAGE_LOG(WARN, "~~~", K(i), K(index));
      }
    }
  }
}

TEST_F(TestRowReader, sparse_row_with_trans_id)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader row_reader;
  ObStoreRow sparse_row;
  oceanbase::common::ObObj sparse_row_objs[column_num];
  sparse_row.row_val_.cells_ = sparse_row_objs;
  sparse_row.row_val_.count_ = column_num;
  uint16_t sparse_column_id[column_num];
  sparse_row.column_ids_ = sparse_column_id;
  sparse_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);
  // set nop value
  int sparse_cell_cnt = 5;
  int sparse_indexs[] = {5, 9, 12, 18, 21, INT_MAX};
  for (int i = 0; i < sparse_cell_cnt; ++i) {
    sparse_row.row_val_.cells_[i] = writer_row.row_val_.cells_[sparse_indexs[i]];
    sparse_row.column_ids_[i] = columns[sparse_indexs[i]].col_id_;
  }
  transaction::ObTransID trans_id(common::ObAddr(2, 3333));
  transaction::ObTransID* trans_id_ptr = &trans_id;
  sparse_row.row_val_.count_ = sparse_cell_cnt;
  sparse_row.is_sparse_row_ = true;
  sparse_row.flag_ = writer_row.flag_;
  sparse_row.dml_ = writer_row.dml_;
  sparse_row.trans_id_ptr_ = trans_id_ptr;
  sparse_row.row_type_flag_.set_uncommitted_row(true);

  ret = row_writer.write_sparse_row(
      rowkey_column_count, sparse_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length, false);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  ObStoreRow reader_flat_row;
  oceanbase::common::ObObj reader_flat_objs[column_num];
  reader_flat_row.row_val_.cells_ = reader_flat_objs;
  reader_flat_row.row_val_.count_ = column_num;
  reader_flat_row.capacity_ = column_num;
  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row, SPARSE_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, reader_row.is_sparse_row_);
  ASSERT_TRUE(sparse_cell_cnt == reader_row.row_val_.count_);
  // every obj should equal
  int index = 0;
  for (int64_t i = 0; i < reader_row.row_val_.count_; ++i) {
    ASSERT_TRUE(sparse_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i]);
    ASSERT_TRUE(sparse_row.column_ids_[i] == reader_row.column_ids_[i]);
  }
  ASSERT_TRUE(*sparse_row.trans_id_ptr_ == trans_id);
}

TEST_F(TestRowReader, read_index_column_in_flat_storage)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    columns[i].col_type_ = obj_meta;
  }

  ret = row_reader.setup_row(buf, pos, 0, column_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  int column_index_cnt = 5;
  int column_indexs[] = {12, 5, 21, 18, 9};

  for (int64_t i = 0; i < column_index_cnt; ++i) {
    row_reader.read_column(
        columns[column_indexs[i]].col_type_, allocator_, column_indexs[i], reader_row.row_val_.cells_[i]);
    ASSERT_TRUE(writer_row.row_val_.cells_[column_indexs[i]] == reader_row.row_val_.cells_[i]);
  }
}

TEST_F(TestRowReader, flat_row_with_trans_id)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  transaction::ObTransID trans_id(common::ObAddr(2, 3333));
  transaction::ObTransID* trans_id_ptr = &trans_id;
  writer_row.row_type_flag_.set_uncommitted_row(true);
  writer_row.trans_id_ptr_ = trans_id_ptr;
  ret = row_writer.write(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length, false);

  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  transaction::ObTransID read_trans_id;
  transaction::ObTransID* read_trans_id_ptr = &read_trans_id;
  // read success
  read_pos = 0;
  reader_row.trans_id_ptr_ = read_trans_id_ptr;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, reader_row.is_sparse_row_);
  ASSERT_TRUE(column_num == reader_row.row_val_.count_);
  // every obj should equal
  int index = 0;
  for (int64_t i = 0; i < reader_row.row_val_.count_; ++i) {
    ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i]);
  }
  ASSERT_TRUE(read_trans_id == trans_id);
}

TEST_F(TestRowReader, read_full_sparse_row)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  ObStoreRow sparse_row;
  oceanbase::common::ObObj sparse_row_objs[column_num];
  sparse_row.row_val_.cells_ = sparse_row_objs;
  sparse_row.row_val_.count_ = column_num;
  uint16_t column_ids[column_num];
  sparse_row.column_ids_ = column_ids;
  sparse_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  // set sparse cell value
  int sparse_cell_cnt = 5;
  int sparse_indexs[] = {5, 9, 12, 18, 21, INT_MAX};
  for (int i = 0; i < sparse_cell_cnt; ++i) {
    sparse_row.row_val_.cells_[i] = writer_row.row_val_.cells_[sparse_indexs[i]];
    sparse_row.column_ids_[i] = columns[sparse_indexs[i]].col_id_;
  }
  sparse_row.row_val_.count_ = sparse_cell_cnt;
  sparse_row.is_sparse_row_ = true;
  sparse_row.flag_ = writer_row.flag_;
  sparse_row.dml_ = writer_row.dml_;

  int row_cnt = 100;
  ret = row_writer.write_sparse_row(
      rowkey_column_count, sparse_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();

  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    columns[i].col_type_ = obj_meta;
  }
  ret = row_reader.read_full_row(buf, pos, 0, nullptr, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < reader_row.row_val_.count_; ++i) {
    ASSERT_TRUE(writer_row.row_val_.cells_[sparse_indexs[i]] == reader_row.row_val_.cells_[i]);
  }
}

TEST_F(TestRowReader, read_index_column_in_sparse_storage)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  ObStoreRow sparse_row;
  oceanbase::common::ObObj sparse_row_objs[column_num];
  sparse_row.row_val_.cells_ = sparse_row_objs;
  sparse_row.row_val_.count_ = column_num;
  uint16_t column_ids[column_num];
  sparse_row.column_ids_ = column_ids;
  sparse_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  // set sparse cell value
  int sparse_cell_cnt = 5;
  int sparse_indexs[] = {5, 9, 12, 18, 21, INT_MAX};
  for (int i = 0; i < sparse_cell_cnt; ++i) {
    sparse_row.row_val_.cells_[i] = writer_row.row_val_.cells_[sparse_indexs[i]];
    sparse_row.column_ids_[i] = columns[sparse_indexs[i]].col_id_;
  }
  sparse_row.row_val_.count_ = sparse_cell_cnt;
  sparse_row.is_sparse_row_ = true;
  sparse_row.flag_ = writer_row.flag_;
  sparse_row.dml_ = writer_row.dml_;

  int row_cnt = 100;
  ret = row_writer.write_sparse_row(
      rowkey_column_count, sparse_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();

  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    columns[i].col_type_ = obj_meta;
  }
  ret = row_reader.setup_row(buf, pos, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < sparse_cell_cnt; ++i) {
    row_reader.read_column(columns[sparse_indexs[i]].col_type_, allocator_, i, reader_row.row_val_.cells_[i]);
    ASSERT_TRUE(writer_row.row_val_.cells_[sparse_indexs[i]] == reader_row.row_val_.cells_[i]);
  }
}

TEST_F(TestRowReader, test_skip_column)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  ObArray<ObColDesc> column_desc_array;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(column_desc_array));
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    if (obj_type == ObMediumIntType) {
      objs[i].set_mediumint(1000000);
    } else if (obj_type == ObIntType) {
      objs[i].set_int(1000000000000000000);
    }
  }
  ret = row_writer.write_flat_row(
      rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();

  ObArray<ObColDesc> out_cols;
  ObArray<int32_t> projector;
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObNCharType == obj_type || ObNVarchar2Type == obj_type ||
        ObTinyTextType == obj_type || ObTextType == obj_type || ObMediumTextType == obj_type ||
        ObLongTextType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    } else if (ObHexStringType == obj_type) {
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    } else if (ObRawType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_BINARY);
    }
    ObColDesc col_desc = column_desc_array[i];
    col_desc.col_type_ = obj_meta;
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(static_cast<int32_t>(i)));
  }
  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          out_cols,
          nullptr,
          &projector));

  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  // skip column error
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ObObjMeta obj_meta;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret = row_writer.write(rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // skip rowkey column error
  column_map_.reset();
  out_cols.reset();
  projector.reset();
  ObColDesc col_desc;

  ObObjType obj_type = writer_row.row_val_.cells_[0].get_type();
  obj_meta.set_type(obj_type);
  col_desc = column_desc_array[0];
  col_desc.col_type_ = obj_meta;
  ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(-1));

  obj_type = writer_row.row_val_.cells_[1].get_type();
  obj_meta.set_type(obj_type);
  col_desc = column_desc_array[1];
  col_desc.col_type_ = obj_meta;
  ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(1));

  obj_type = writer_row.row_val_.cells_[2].get_type();
  obj_meta.set_type(obj_type);
  col_desc = column_desc_array[2];
  col_desc.col_type_ = obj_meta;
  ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
  ASSERT_EQ(OB_SUCCESS, projector.push_back(2));

  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          out_cols,
          nullptr,
          &projector));

  read_pos = 0;
  ret = row_reader.read_row(buf, 8 + 4 + 2, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
}

TEST_F(TestRowReader, read_multi)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t sum_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  // pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret =
      row_writer.write(rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, sum_pos, rowkey_start_pos, rowkey_length);
  pos = sum_pos;
  ASSERT_EQ(OB_SUCCESS, ret);

  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  ret = init_column_map(writer_row, columns);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read success
  ret = row_reader.read_row(buf, pos, 0, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  // every reader obj should equal
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, writer_row));
  for (int64_t i = 0; i < column_num; ++i) {
    ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i])
        << "\n i: " << i << "\n writer:  " << to_cstring(writer_row.row_val_.cells_[i])
        << "\n reader:  " << to_cstring(reader_row.row_val_.cells_[i]);
  }
  // read success
  ret = row_reader.read_row(buf, pos, 0, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  // every reader obj should equal
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, writer_row));
  for (int64_t i = 0; i < column_num; ++i) {
    ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i])
        << "\n i: " << i << "\n writer:  " << to_cstring(writer_row.row_val_.cells_[i])
        << "\n reader:  " << to_cstring(reader_row.row_val_.cells_[i]);
  }
}

TEST_F(TestRowReader, read_flat_rowkey)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = 0;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  ObArray<ObColDesc> column_desc_array;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(column_desc_array));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    column_desc_array[i].col_type_ = obj_meta;
  }
  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          column_desc_array));
  {
    ObNewRow new_writer_row;
    new_writer_row.count_ = column_num;
    new_writer_row.cells_ = objs;
    ObNewRow new_reader_row;
    new_reader_row.count_ = 0;
    new_reader_row.cells_ = reader_objs;

    pos = 0;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
    ret = row_writer.write(new_writer_row, buf, 2 * 1024 * 1024, FLAT_ROW_STORE, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    // init column type array
    ObObjMeta column_types[common::OB_ROW_MAX_COLUMNS_COUNT];
    for (int64_t i = 0; i < column_num; ++i) {
      ObObjType obj_type = new_writer_row.cells_[i].get_type();
      ObObjMeta& obj_meta = column_types[i];
      obj_meta.set_type(obj_type);
      if (ObVarcharType == obj_type || ObCharType == obj_type) {
        obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
      }
    }
    // buf is NULL
    read_pos = 0;
    ret = row_reader.read_compact_rowkey(NULL, 0, NULL, pos, read_pos, new_reader_row);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    // read_sequence_columns overflow
    ret = row_reader.read_compact_rowkey(
        column_types, column_map_.get_rowkey_store_count(), buf, 1, read_pos, new_reader_row);
    // FIXEME: we don't check this overflow type
    ASSERT_EQ(OB_SUCCESS, ret);
    // new row read success
    read_pos = 0;
    ret = row_reader.read_compact_rowkey(
        column_types, column_map_.get_rowkey_store_count(), buf, pos, read_pos, new_reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    // every reader obj should equal
    for (int64_t i = 0; i < rowkey_column_count; ++i) {
      ASSERT_TRUE(new_writer_row.cells_[i] == new_reader_row.cells_[i])
          << "\n i: " << i << "\n writer:  " << to_cstring(new_writer_row.cells_[i])
          << "\n reader:  " << to_cstring(new_reader_row.cells_[i]);
    }
  }
}

TEST_F(TestRowReader, read_sparse_rowkey)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = 0;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  ObArray<ObColDesc> column_desc_array;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(column_desc_array));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    column_desc_array[i].col_type_ = obj_meta;
  }
  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          column_desc_array));
  {
    ObNewRow new_writer_row;
    new_writer_row.count_ = column_num;
    new_writer_row.cells_ = objs;
    ObNewRow new_reader_row;
    new_reader_row.count_ = 0;
    new_reader_row.cells_ = reader_objs;

    pos = 0;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
    ret = row_writer.write(new_writer_row, buf, 2 * 1024 * 1024, SPARSE_ROW_STORE, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    // init column type array
    ObObjMeta column_types[common::OB_ROW_MAX_COLUMNS_COUNT];
    for (int64_t i = 0; i < column_num; ++i) {
      ObObjType obj_type = new_writer_row.cells_[i].get_type();
      ObObjMeta& obj_meta = column_types[i];
      obj_meta.set_type(obj_type);
      if (ObVarcharType == obj_type || ObCharType == obj_type) {
        obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
      }
    }
    // buf is NULL
    read_pos = 0;
    ret = row_reader.read_compact_rowkey(NULL, 0, NULL, pos, read_pos, new_reader_row);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    // read_sequence_columns overflow
    ret = row_reader.read_compact_rowkey(
        column_types, column_map_.get_rowkey_store_count(), buf, 1, read_pos, new_reader_row);
    ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
    // new row read success
    read_pos = 0;
    ret = row_reader.read_compact_rowkey(
        column_types, column_map_.get_rowkey_store_count(), buf, pos, read_pos, new_reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    // every reader obj should equal
    for (int64_t i = 0; i < rowkey_column_count; ++i) {
      ASSERT_TRUE(new_writer_row.cells_[i] == new_reader_row.cells_[i]);
    }
  }
}

TEST_F(TestRowReader, read_not_exist_column)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  ObArray<ObColDesc> column_desc_array;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(column_desc_array));
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret = row_writer.write(rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<int32_t> projector;
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObNCharType == obj_type || ObNVarchar2Type == obj_type ||
        ObTinyTextType == obj_type || ObTextType == obj_type || ObMediumTextType == obj_type ||
        ObLongTextType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    column_desc_array[i].col_type_ = obj_meta;
    if (i % 2 == 0 && i > rowkey_column_count) {
      ASSERT_EQ(OB_SUCCESS, projector.push_back(static_cast<int32_t>(-1)));
    } else {
      ASSERT_EQ(OB_SUCCESS, projector.push_back(static_cast<int32_t>(i)));
    }
  }
  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          column_desc_array,
          nullptr,
          &projector));
  // read success
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  // every reader obj should equal
  for (int64_t i = 0; i < column_num; ++i) {
    if (i % 2 == 0 && i > rowkey_column_count) {
      ASSERT_TRUE(reader_row.row_val_.cells_[i].is_nop_value());
    } else {
      ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i]);
    }
  }
}

TEST_F(TestRowReader, unknown_type)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret = row_writer.write(rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObColumnMap
  column_map_.reset();
  ObArray<ObColDesc> column_desc_array;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(column_desc_array));
  ObArray<ObColDesc> out_cols;
  ObArray<int32_t> projector;
  for (int64_t i = 0; i < column_num;) {
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObNCharType == obj_type || ObNVarchar2Type == obj_type ||
        ObTinyTextType == obj_type || ObTextType == obj_type || ObMediumTextType == obj_type ||
        ObLongTextType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    if (2 == i) {
      obj_meta.set_type(static_cast<ObObjType>(ObUnknownType));
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    ObColDesc col_desc = column_desc_array[i];
    col_desc.col_type_ = obj_meta;
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col_desc));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(static_cast<int32_t>(i)));
    i += 2;
  }
  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          out_cols,
          nullptr,
          &projector));
  read_pos = 0;
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
}

TEST_F(TestRowReader, obj_cast_write_read_different)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  // write success
  char* buf = get_serialize_buf();
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  ret = row_writer.write(rowkey_column_count, writer_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);
  // every type cast to others
  for (int64_t i = 0; i < column_num; ++i) {
    // not support cast
    for (int64_t j = 0; j < column_num; ++j) {
      if (!cast_supported(
              objs[j].get_type(), objs[j].get_collation_type(), objs[i].get_type(), objs[i].get_collation_type())) {
        ret = OB_NOT_SUPPORTED;
      }
    }
    // init ObColumnMap
    column_map_.reset();
    ObArray<ObColDesc> columns;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

    ret = init_column_map(writer_row, columns);
    ASSERT_EQ(OB_SUCCESS, ret);
    // read success
    read_pos = 0;
    ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
    if (OB_SUCCESS != ret && OB_NOT_SUPPORTED != ret && OB_DATA_OUT_OF_RANGE != ret && OB_INVALID_DATE_VALUE != ret) {
      ASSERT_TRUE(false);
    }
  }
}

TEST_F(TestRowReader, sparse_row_about_lob)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObSparseRowReader row_reader;
  ObStoreRow sparse_row;
  oceanbase::common::ObObj sparse_row_objs[column_num];
  sparse_row.row_val_.cells_ = sparse_row_objs;
  sparse_row.row_val_.count_ = column_num;
  uint16_t column_ids[column_num];
  sparse_row.column_ids_ = column_ids;
  sparse_row.capacity_ = column_num;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;

  char* buf = get_serialize_buf();
  // write success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  // set nop value
  int sparse_cell_cnt = 5;
  int sparse_indexs[] = {5, 9, 12, 18, 21};
  for (int i = 0; i < sparse_cell_cnt; ++i) {
    sparse_row.row_val_.cells_[i] = writer_row.row_val_.cells_[sparse_indexs[i]];
    sparse_row.column_ids_[i] = columns[sparse_indexs[i]].col_id_;
  }

  int64_t lob_buf_size = 1 * 1024 * 1024;
  char lob_buf[lob_buf_size];
  MEMSET(lob_buf, 'a', lob_buf_size);
  ObString str(lob_buf);
  ObScale scale;
  scale = ObLobScale::STORE_IN_ROW;

  uint64_t lob_column_id = 39;
  uint64_t lob_column_idx = 20;
  sparse_row.row_val_.cells_[sparse_cell_cnt].set_string(ObLongTextType, str);
  sparse_row.row_val_.cells_[sparse_cell_cnt].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  sparse_row.row_val_.cells_[sparse_cell_cnt].set_scale(scale);
  sparse_row.column_ids_[sparse_cell_cnt] = lob_column_id;

  for (int64_t i = 0; i < column_num; ++i) {
    ObObjMeta obj_meta;
    ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObLongTextType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    }
    columns[i].col_type_ = obj_meta;
  }

  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          columns));

  sparse_cell_cnt++;
  sparse_row.row_val_.count_ = sparse_cell_cnt;
  sparse_row.is_sparse_row_ = true;
  sparse_row.flag_ = writer_row.flag_;
  sparse_row.dml_ = writer_row.dml_;

  ret = row_writer.write_sparse_row(
      rowkey_column_count, sparse_row, buf, 2 * 1024 * 1024, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;
  reader_row.capacity_ = column_num;

  read_pos = 0;
  // read success
  ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row, SPARSE_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, reader_row.is_sparse_row_);
  ASSERT_TRUE(sparse_cell_cnt == reader_row.row_val_.count_);
  // every obj should equal
  int index = 0;
  for (int64_t i = 0; i < reader_row.row_val_.count_; ++i) {
    STORAGE_LOG(INFO, "compare column", K(i), K(sparse_row.row_val_.cells_[i]), K(reader_row.row_val_.cells_[i]));
    ASSERT_TRUE(sparse_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i]);
    ASSERT_TRUE(sparse_row.column_ids_[i] == reader_row.column_ids_[i]);
  }
  STORAGE_LOG(INFO, "success to read sparse row", K(reader_row));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_row_reader.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_row_reader.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
