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

#ifndef OCEANBASE_ENCODING_TEST_COLUMN_DECODER_H_
#define OCEANBASE_ENCODING_TEST_COLUMN_DECODER_H_


#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/encoding/ob_micro_block_encoder.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/access/ob_block_row_store.h"
#include "storage/ob_i_store.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "lib/string/ob_sql_string.h"
#include "../ob_row_generate.h"
#include "common/rowkey/ob_rowkey.h"
#include "unittest/storage/mock_ob_table_read_info.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;
using namespace share::schema;

class TestColumnDecoder : public ::testing::Test
{
public:
  static const int64_t ROWKEY_CNT = 1;
  int64_t COLUMN_CNT = ObExtendType - 1 + 7;

  static const int64_t ROW_CNT = 64;

  virtual void SetUp();
  virtual void TearDown();

  TestColumnDecoder()
      : is_retro_(false), tenant_ctx_(OB_SERVER_TENANT_ID)
  {
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
  }
  TestColumnDecoder(ObColumnHeader::Type column_encoding_type)
      : is_retro_(false), column_encoding_type_(column_encoding_type), tenant_ctx_(OB_SERVER_TENANT_ID)
  {
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
  }
  TestColumnDecoder(bool is_retro)
      : is_retro_(is_retro), tenant_ctx_(OB_SERVER_TENANT_ID)
  {
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
  }
  virtual ~TestColumnDecoder() {}

  inline void setup_obj(ObObj& obj, int64_t column_id, int64_t seed);

  int test_filter_pushdown(
        const uint64_t col_idx,
        bool is_retro,
        ObMicroBlockDecoder& decoder,
        sql::ObPushdownWhiteFilterNode &filter_node,
        common::ObBitmap &result_bitmap,
        common::ObFixedArray<ObObj, ObIAllocator> &objs);

  void basic_filter_pushdown_in_op_test();

  void basic_filter_pushdown_eq_ne_nu_nn_test();

  void basic_filter_pushdown_comparison_test();

  void basic_filter_pushdown_bt_test();

  void filter_pushdown_comaprison_neg_test();

  void batch_decode_to_datum_test(bool is_condensed = false);

  void batch_get_row_perf_test();

  void set_encoding_type(ObColumnHeader::Type type);

  void set_column_type_default();

  void set_column_type_integer();

  void set_column_type_string();

protected:
  ObRowGenerate row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  ObMicroBlockEncoder encoder_;
  MockObTableReadInfo read_info_;
  ObArenaAllocator allocator_;
  bool is_retro_;
  ObColumnHeader::Type column_encoding_type_;
  ObObjType *col_obj_types_;
  int64_t extra_rowkey_cnt_;
  int64_t column_cnt_;
  int64_t full_column_cnt_;
  int64_t rowkey_cnt_;
  share::ObTenantBase tenant_ctx_;
};

void TestColumnDecoder::set_column_type_default()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  column_cnt_ = ObExtendType - 1 + 7;
  rowkey_cnt_ = 2;
  col_obj_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
  for (int64_t i = 0; i < column_cnt_; ++i) {
    ObObjType type = static_cast<ObObjType>(i + 1);
    if (column_cnt_ - 1 == i) {
      type = ObURowIDType;
    } else if (column_cnt_ - 2 == i) {
      type = ObIntervalYMType;
    } else if (column_cnt_ - 3 == i) {
      type = ObIntervalDSType;
    } else if (column_cnt_ - 4 == i) {
      type = ObTimestampTZType;
    } else if (column_cnt_ - 5 == i) {
      type = ObTimestampLTZType;
    } else if (column_cnt_ - 6 == i) {
      type = ObTimestampNanoType;
    } else if (column_cnt_ - 7 == i) {
      type = ObRawType;
    } else if (type == ObExtendType || type == ObUnknownType) {
      type = ObVarcharType;
    }
    col_obj_types_[i] = type;
  }
}

void TestColumnDecoder::set_column_type_integer()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  column_cnt_ = ObUDoubleType - ObTinyIntType + 1 + 5;
  rowkey_cnt_ = 1;
  col_obj_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
  for (int64_t i = 0; i <= ObUDoubleType - ObTinyIntType + 1; ++i) {
    ObObjType type = static_cast<ObObjType>(i + ObTinyIntType);
    col_obj_types_[i] = type;
  }
  col_obj_types_[column_cnt_ - 5] = static_cast<ObObjType>(ObDateTimeType);
  col_obj_types_[column_cnt_ - 4] = static_cast<ObObjType>(ObTimestampType);
  col_obj_types_[column_cnt_ - 3] = static_cast<ObObjType>(ObDateType);
  col_obj_types_[column_cnt_ - 2] = static_cast<ObObjType>(ObTimeType);
  col_obj_types_[column_cnt_ - 1] = static_cast<ObObjType>(ObYearType);
}

void TestColumnDecoder::set_column_type_string()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  column_cnt_ = 4;
  rowkey_cnt_ = 1;
  col_obj_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
  col_obj_types_[0] = ObIntType;
  col_obj_types_[1] = ObVarcharType;
  col_obj_types_[2] = ObCharType;
  col_obj_types_[3] = ObHexStringType;
}

void TestColumnDecoder::SetUp()
{
  if (column_encoding_type_ == ObColumnHeader::Type::INTEGER_BASE_DIFF) {
    set_column_type_integer();
  } else if (column_encoding_type_ == ObColumnHeader::Type::HEX_PACKING
      || column_encoding_type_ == ObColumnHeader::Type::STRING_DIFF
      || column_encoding_type_ == ObColumnHeader::Type::STRING_PREFIX) {
    set_column_type_string();
  } else {
    set_column_type_default();
  }
  extra_rowkey_cnt_ = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  full_column_cnt_ = column_cnt_ + extra_rowkey_cnt_;
  const int64_t tid = 200001;
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(1);
  table.set_database_id(1);
  table.set_table_id(tid);
  table.set_table_name("test_column_decoder_schema");
  table.set_rowkey_column_num(rowkey_cnt_);
  table.set_max_column_id(column_cnt_ * 2);
  table.set_block_size(2 * 1024);
  table.set_compress_func_name("none");
  table.set_row_store_type(ENCODING_ROW_STORE);
  table.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  ObSqlString str;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    col.reset();
    col.set_table_id(tid);
    col.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    str.assign_fmt("test%ld", i);
    col.set_column_name(str.ptr());
    ObObjType type = col_obj_types_[i]; // 0 is ObNullType

    col.set_data_type(type);
    if (ObVarcharType == type || ObCharType == type || ObHexStringType == type
        || ObNVarchar2Type == type || ObNCharType == type || ObTextType == type){
      col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      if (ObCharType == type) {
        const int64_t max_char_length = lib::is_oracle_mode()
                                        ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                        : OB_MAX_CHAR_LENGTH;
        col.set_data_length(max_char_length);
      }
    } else {
      col.set_collation_type(CS_TYPE_BINARY);
    }
    if (type == ObIntType) {
      col.set_rowkey_position(1);
    } else if (type == ObUInt64Type) {
      col.set_rowkey_position(2);
    } else{
      col.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table.add_column(col));
  }

  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table, true/*multi_version*/));
  ASSERT_EQ(OB_SUCCESS, table.get_multi_version_column_descs(col_descs_));
  ASSERT_EQ(OB_SUCCESS, read_info_.init(
      allocator_,
      table.get_column_count(),
      table.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      col_descs_));

  ctx_.micro_block_size_ = 64L << 11;
  ctx_.macro_block_size_ = 2L << 20;
  ctx_.rowkey_column_cnt_ = rowkey_cnt_ + extra_rowkey_cnt_;
  ctx_.column_cnt_ = column_cnt_ + extra_rowkey_cnt_;
  ctx_.col_descs_ = &col_descs_;
  ctx_.row_store_type_ = common::ENCODING_ROW_STORE;

  if (!is_retro_) {
    int64_t *column_encodings = reinterpret_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * ctx_.column_cnt_));
    ctx_.column_encodings_ = column_encodings;
    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
        ctx_.column_encodings_[i] = ObColumnHeader::Type::RAW;
        continue;
      }
      if (ObColumnHeader::Type::INTEGER_BASE_DIFF == column_encoding_type_) {
        ctx_.column_encodings_[i] = column_encoding_type_;
      } else if (col_obj_types_[i] == ObIntType) {
        ctx_.column_encodings_[i] = ObColumnHeader::Type::DICT;
      } else {
        ctx_.column_encodings_[i] = column_encoding_type_;
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, encoder_.init(ctx_));
}

void TestColumnDecoder::TearDown()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  if (OB_NOT_NULL(ctx_.column_encodings_)) {
    allocator_.free(ctx_.column_encodings_);
  }
  col_descs_.reset();
  encoder_.reuse();
}

void TestColumnDecoder::set_encoding_type(ObColumnHeader::Type type)
{
  column_encoding_type_ = type;
}

inline void TestColumnDecoder::setup_obj(ObObj& obj, int64_t column_id, int64_t seed)
{
  obj.copy_meta_type(row_generate_.column_list_.at(column_id).col_type_);
  ObObjType column_type = row_generate_.column_list_.at(column_id).col_type_.get_type();
  row_generate_.set_obj(column_type, row_generate_.column_list_.at(column_id).col_id_, seed, obj, 0);
  if ( ObVarcharType == column_type || ObCharType == column_type || ObHexStringType == column_type
      || ObNVarchar2Type == column_type || ObNCharType == column_type || ObTextType == column_type){
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    obj.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    obj.set_collation_type(CS_TYPE_BINARY);
    obj.set_collation_level(CS_LEVEL_NUMERIC);
  }
}

int TestColumnDecoder::test_filter_pushdown(
    const uint64_t col_idx,
    bool is_retro,
    ObMicroBlockDecoder& decoder,
    sql::ObPushdownWhiteFilterNode &filter_node,
    common::ObBitmap &result_bitmap,
    common::ObFixedArray<ObObj, ObIAllocator> &objs)
{
  int ret = OB_SUCCESS;
  storage::PushdownFilterInfo pd_filter_info;
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  sql::ObWhiteFilterExecutor filter(allocator_, filter_node, op);
  filter.col_offsets_.init(COLUMN_CNT);
  filter.col_params_.init(COLUMN_CNT);
  const ObColumnParam *col_param = nullptr;
  filter.col_params_.push_back(col_param);
  filter.col_offsets_.push_back(col_idx);
  filter.n_cols_ = 1;
  void *obj_buf = allocator_.alloc(sizeof(ObObj) * COLUMN_CNT);
  EXPECT_TRUE(obj_buf != nullptr);
  pd_filter_info.col_buf_ = new (obj_buf) ObObj [COLUMN_CNT]();
  pd_filter_info.col_capacity_ = full_column_cnt_;
  pd_filter_info.start_ = 0;
  pd_filter_info.end_ = decoder.row_count_;
  filter.params_ = objs;
  if (sql::WHITE_OP_IN == filter.get_op_type()) {
    filter.init_obj_set();
  }

  if (is_retro) {
    ret = decoder.filter_pushdown_retro(nullptr, filter, pd_filter_info, col_idx, filter.col_params_.at(0), pd_filter_info.col_buf_[0], result_bitmap);
  } else {
    ret = decoder.filter_pushdown_filter(nullptr, filter, pd_filter_info, result_bitmap);
  }
  if (nullptr != obj_buf) {
    allocator_.free(obj_buf);
  }
  return ret;
}

void TestColumnDecoder::basic_filter_pushdown_in_op_test()
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  int64_t seed2 = 10002;
  int64_t seed3 = 10003;
  int64_t seed5 = 10005;

  for (int64_t i = 0; i < ROW_CNT - 40; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 40; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 20; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed3, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  int64_t seed0_count = ROW_CNT - 40;
  int64_t seed1_count = 10;
  int64_t seed2_count = 10;
  int64_t seed3_count = 10;
  int64_t null_count = 10;
  int64_t seed5_count = 0;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    sql::ObPushdownWhiteFilterNode white_filter_2(allocator_);

    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 3);
    objs.init(3);

    ObObj ref_obj0;
    setup_obj(ref_obj0, i, seed0);
    ObObj ref_obj1;
    setup_obj(ref_obj1, i, seed1);
    ObObj ref_obj2;
    setup_obj(ref_obj2, i, seed2);
    ObObj ref_obj5;
    setup_obj(ref_obj5, i, seed5);

    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj5);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_IN;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());

    objs.reuse();
    objs.init(3);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    white_filter_2.op_type_ = sql::WHITE_OP_IN;

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter_2, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
  }
}

void TestColumnDecoder::basic_filter_pushdown_eq_ne_nu_nn_test()
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed_1 = 10001;
  int64_t seed_2 = 10002;
  for (int64_t i = 0; i < ROW_CNT - 40; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed_1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 40; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed_2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  int64_t seed1_count = ROW_CNT - 40;
  int64_t seed2_count = 20;
  int64_t null_count = 20;

  char* buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1, ref_obj_2;
    setup_obj(ref_obj_1, i, seed_1);
    setup_obj(ref_obj_2, i, seed_2);
    objs.push_back(ref_obj_1);

    int32_t col_idx = i;
    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);

    white_filter.op_type_ = sql::WHITE_OP_NU;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(null_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NN;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_EQ;
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    objs.pop_back();
    objs.push_back(ref_obj_2);
    white_filter.op_type_ = sql::WHITE_OP_EQ;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());
  }
}

void TestColumnDecoder::basic_filter_pushdown_comparison_test()
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  int64_t seed2 = 10002;
  int64_t neg_seed_0 = -10000;
  for (int64_t i = 0; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 20; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  int64_t seed0_count = ROW_CNT - 30;
  int64_t seed1_count = 10;
  int64_t seed2_count = 10;
  int64_t null_count = 10;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);

  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);

    objs.push_back(ref_obj);

    int32_t col_idx = i;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    // Greater Than
    white_filter.op_type_ = sql::WHITE_OP_GT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());

    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());

    if (ob_is_int_tc(row_generate_.column_list_.at(i).col_type_.get_type())) {
      // Test cmp with negative values
      setup_obj(ref_obj, i, neg_seed_0);
      objs.clear();
      objs.push_back(ref_obj);
      white_filter.op_type_ = sql::WHITE_OP_GT;
      result_bitmap.reuse();
      ASSERT_EQ(0, result_bitmap.popcnt());
      ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
      ASSERT_EQ(ROW_CNT - null_count, result_bitmap.popcnt());

      white_filter.op_type_ = sql::WHITE_OP_LT;
      result_bitmap.reuse();
      ASSERT_EQ(0, result_bitmap.popcnt());
      ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
      ASSERT_EQ(0, result_bitmap.popcnt());
    }
  }
}

void TestColumnDecoder::filter_pushdown_comaprison_neg_test()
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  const int64_t seed0 = -128;
  const int64_t seed1 = seed0 + 1;
  const int64_t seed2 = seed0 + 2;
  const int64_t ref_seed0 = seed1;
  const int64_t ref_seed1 = seed0 - 1;
  const int64_t seed0_count = ROW_CNT - 30;
  const int64_t seed1_count = 10;
  const int64_t seed2_count = 20;
  for (int64_t i = 0; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 20; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);

  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObObjTypeStoreClass column_sc
        = get_store_class_map()[row_generate_.column_list_.at(i).col_type_.get_type_class()];
    if (column_sc != ObIntSC) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj;
    setup_obj(ref_obj, i, ref_seed0);
    objs.push_back(ref_obj);
    int32_t col_idx = i;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_GT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());

    setup_obj(ref_obj, i, ref_seed1);
    objs.clear();
    objs.push_back(ref_obj);

    white_filter.op_type_ = sql::WHITE_OP_GT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count + seed2_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count + seed2_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
  }
}

void TestColumnDecoder::basic_filter_pushdown_bt_test()
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  int64_t seed2 = 10002;
  for (int64_t i = 0; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  int64_t seed0_count = ROW_CNT - 10;
  int64_t seed1_count = 10;
  int64_t seed3_count = 0;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 2);
    objs.init(2);

    ObObj ref_obj1;
    setup_obj(ref_obj1, i, seed1);

    ObObj ref_obj2;
    setup_obj(ref_obj2, i, seed2);

    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);

    int32_t col_idx = i;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    // Normal
    white_filter.op_type_ = sql::WHITE_OP_BT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());

    // Empty range
    objs.reuse();
    objs.init(2);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj1);
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
  }
}

void TestColumnDecoder::batch_decode_to_datum_test(bool is_condensed)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;

  if (is_condensed) {
    for (int64_t i = 0; i < ROW_CNT - 60; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
      for (int64_t j = 0; j < full_column_cnt_; ++j) {
        // Generate data for var_length
        if (col_descs_[i].col_type_.get_type() == ObVarcharType) {
          ObStorageDatum &datum = row.storage_datums_[j];
          datum.len_ = i < datum.len_ ? i : datum.len_;
        }
      }
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row));
    }
    for (int64_t i = ROW_CNT - 60; i < ROW_CNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row));
    }
    const_cast<bool &>(encoder_.ctx_.encoder_opt_.enable_bit_packing_) = false;
  } else {
    for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    }
    for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    }

    for (int64_t j = 0; j < full_column_cnt_; ++j) {
      row.storage_datums_[j].set_null();
    }
    for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    }
    for (int64_t i = ROW_CNT - 30; i < ROW_CNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    }
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  const ObRowHeader *row_header = nullptr;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  const char *cell_datas[ROW_CNT];
  void *datum_buf_1 = allocator_.alloc(sizeof(int8_t) * 128 * ROW_CNT);
  void *datum_buf_2 = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    STORAGE_LOG(INFO, "Current col: ", K(i), K(col_descs_.at(i)),
        K(row.storage_datums_[i]), K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int64_t row_ids[ROW_CNT];
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf_1) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
              .batch_decode(decoder.row_index_,
                            row_ids,
                            cell_datas,
                            ROW_CNT,
                            datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObObj obj;
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(row_ids[j], row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(obj, row_ids[j], bs, row_data, row_len));
      ObObj obj_cast_from_datum;
      ASSERT_EQ(OB_SUCCESS, datums[j].to_obj(obj_cast_from_datum, col_descs_.at(i).col_type_));
      STORAGE_LOG(INFO, "row: ", K(j), K(obj), K(obj_cast_from_datum));
      ObDatum datum_cast_from_obj;
      datum_cast_from_obj.ptr_ = reinterpret_cast<char *>(datum_buf_2);
      ASSERT_EQ(OB_SUCCESS, datum_cast_from_obj.from_obj(obj));
      STORAGE_LOG(INFO, "row: ", K(j), K(datum_cast_from_obj), K(datums[j]));
      ASSERT_EQ(obj, obj_cast_from_datum);
      ASSERT_TRUE(ObDatum::binary_equal(datum_cast_from_obj, datums[j]));
    }
  }
}

// void TestColumnDecoder::batch_get_row_perf_test()
// {
//   ObDatumRow row;
//   ASSERT_EQ(OB_SUCCESS, row.init(allocator_, column_cnt_));
//   int64_t seed0 = 10000;
//   int64_t seed1 = 10001;

//   for (int64_t i = 0; i < ROW_CNT - 5; ++i) {
//     ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
//     ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
//   }
//   for (int64_t i = ROW_CNT - 5; i < ROW_CNT - 2; ++i) {
//     ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
//     ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
//   }

//   for (int64_t j = 0; j < column_cnt_; ++j) {
//     row.storage_datums_[j].set_null();
//   }
//   for (int64_t i = ROW_CNT - 2; i < ROW_CNT; ++i) {
//     ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
//   }
//   char *buf = NULL;
//   int64_t size = 0;
//   ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
//   ObMicroBlockDecoder decoder;
//   // Batch get rows by row
//   ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
//   ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
//   ObStoreRow *rows = new ObStoreRow[ROW_CNT];
//   char *obj_buf = new char[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj) * ROW_CNT];
//   for (int64_t i = 0; i < ROW_CNT; ++i) {
//     rows[i].row_val_.cells_ = reinterpret_cast<ObObj *>(obj_buf) + i * common::OB_ROW_MAX_COLUMNS_COUNT;
//     rows[i].row_val_.count_ = common::OB_ROW_MAX_COLUMNS_COUNT;
//   }
//   int64_t single_start_time = common::ObTimeUtility::current_time();
//   for (int64_t j = 0; j < 10000; ++j) {
//     int64_t batch_count = 0;
//     int64_t batch_num = ROW_CNT;
//     decoder.get_rows(0, ROW_CNT, ROW_CNT, rows, batch_count);
//   }
//   int64_t single_end_time = common::ObTimeUtility::current_time();
//   std::cout << "Single decode by row cost time: " << single_end_time - single_start_time << std::endl;
//   // Batch get rows by column
//   ObSEArray<int32_t, OB_DEFAULT_SE_ARRAY_COUNT> cols;
//   for (int64_t i = 0; i < (column_cnt_); ++i) {
//     cols.push_back(i);
//   }
//   common::ObFixedArray<const share::schema::ObColumnParam *, common::ObIAllocator> col_params(allocator_);
//   col_params.init(COLUMN_CNT);
//   const share::schema::ObColumnParam *param = nullptr;
//   for (int64_t i = 0; i < COLUMN_CNT; i++) {
//     col_params.push_back(param);
//   }
//   int64_t row_ids[ROW_CNT];
//   for (int64_t i = 0; i < ROW_CNT; ++i) {
//     row_ids[i] = i;
//   }
//   ObDatum *datum_buf = new ObDatum[ROW_CNT * column_cnt_];
//   int64_t single_ptr_buf_len = 8;
//   char *datum_ptr_buf = reinterpret_cast<char *>(
//       allocator_.alloc(ROW_CNT * column_cnt_ * sizeof(char) * single_ptr_buf_len));
//   const char *cell_datas[ROW_CNT];
//   ObArray<ObDatum *> datum_arr;
//   for (int64_t i = 0; i < (column_cnt_); ++i) {
//     ASSERT_EQ(OB_SUCCESS, datum_arr.push_back(datum_buf + ROW_CNT * i));
//     for (int64_t j = 0; j < ROW_CNT; ++j) {
//       datum_arr.at(i)[j].ptr_ = reinterpret_cast<char *>(
//                                           &datum_ptr_buf[single_ptr_buf_len * (ROW_CNT * i + j)]);
//     }
//   }
//   ASSERT_EQ(OB_SUCCESS, decoder.get_rows(cols, col_params, row_ids, cell_datas, ROW_CNT, datum_arr));
//   int64_t batch_start_time = common::ObTimeUtility::current_time();
//   for (int64_t i = 0; i < 10000; ++i) {
//     decoder.get_rows(cols, col_params, row_ids, cell_datas, ROW_CNT, datum_arr);
//   }
//   int64_t batch_end_time = common::ObTimeUtility::current_time();
//   std::cout << "Batch decode by column cost time: " << batch_end_time - batch_start_time << std::endl;
// }



} // end of namespace blocksstable
} // end of namespace oceanbase

#endif // OCEANBASE_ENCODING_TEST_COLUMN_DECODER_H_
