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

class VectorDecodeTestUtil
{
public:
  static int generate_column_output_expr(
      const int64_t batch_cnt,
      const ObObjMeta &obj_meta,
      const VectorFormat &format,
      sql::ObEvalCtx &eval_ctx,
      sql::ObExpr &col_expr,
      ObIAllocator &frame_allocator);
  static bool verify_vector_and_datum_match(
      const ObIVector &vector,
      const int64_t vec_idx,
      const ObDatum &datum);
  static bool need_test_vec_with_type(
      const VectorFormat &format,
      const VecValueTypeClass &vec_tc);

  static int test_batch_decode_perf(
      ObMicroBlockDecoder &decoder,
      const int64_t col_idx,
      const ObObjMeta obj_meta,
      const int64_t decode_cnt,
      const VectorFormat vector_format);
};

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
    decode_res_pool_ = new(allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool;
    tenant_ctx_.set(decode_res_pool_);
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
    encoder_.data_buffer_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    encoder_.row_buf_holder_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    decode_res_pool_->init();
  }
  TestColumnDecoder(ObColumnHeader::Type column_encoding_type)
      : is_retro_(false), column_encoding_type_(column_encoding_type), tenant_ctx_(OB_SERVER_TENANT_ID)
  {
    decode_res_pool_ = new(allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool;
    tenant_ctx_.set(decode_res_pool_);
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
    encoder_.data_buffer_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    encoder_.row_buf_holder_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    decode_res_pool_->init();
  }
  TestColumnDecoder(bool is_retro)
      : is_retro_(is_retro), tenant_ctx_(OB_SERVER_TENANT_ID)
 {
    decode_res_pool_ = new(allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool;
    tenant_ctx_.set(decode_res_pool_);
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
    encoder_.data_buffer_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    encoder_.row_buf_holder_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    decode_res_pool_->init();
  }
  virtual ~TestColumnDecoder() {}

  inline void setup_obj(ObObj& obj, int64_t column_id, int64_t seed);

  void init_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf);

  void init_in_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf);

  int test_filter_pushdown(
        const uint64_t col_idx,
        bool is_retro,
        ObMicroBlockDecoder& decoder,
        sql::ObPushdownWhiteFilterNode &filter_node,
        common::ObBitmap &result_bitmap,
        common::ObFixedArray<ObObj, ObIAllocator> &objs);

  int test_filter_pushdown_with_pd_info(
        int64_t start,
        int64_t end,
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
  void batch_decode_to_vector_test(
      const bool is_condensed,
      const bool has_null,
      const bool align_row_id,
      const VectorFormat vector_format);
  void col_equal_batch_decode_to_vector_test(const VectorFormat vector_format);
  void col_substr_batch_decode_to_vector_test(const VectorFormat vector_format);
  void cell_decode_to_datum_test();

  void cell_decode_to_datum_test_without_hex();

  void cell_column_equal_decode_to_datum_test();

  void cell_inter_column_substring_to_datum_test();

  void batch_get_row_perf_test();

  void set_encoding_type(ObColumnHeader::Type type);

  void set_column_type_default();

  void set_column_type_integer();

  void set_column_type_string();

  void set_column_type_column_equal();

  void set_column_type_column_substring();

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
  share::ObTenantBase tenant_ctx_;
  ObDecodeResourcePool *decode_res_pool_;
  int64_t extra_rowkey_cnt_;
  int64_t column_cnt_;
  int64_t full_column_cnt_;
  int64_t rowkey_cnt_;
};

void TestColumnDecoder::set_column_type_default()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  column_cnt_ = ObExtendType - 1 + 8;
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
    } else if (column_cnt_ - 8 == i) {
      type = ObDecimalIntType;
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

void TestColumnDecoder::set_column_type_column_equal()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  column_cnt_ = 11;
  rowkey_cnt_ = 1;
  col_obj_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
  col_obj_types_[0] = ObIntType;
  col_obj_types_[1] = ObUInt32Type;
  col_obj_types_[2] = ObUInt32Type;
  col_obj_types_[3] = ObNumberType;
  col_obj_types_[4] = ObNumberType;
  col_obj_types_[5] = ObVarcharType;
  col_obj_types_[6] = ObVarcharType;
  col_obj_types_[7] = ObTimestampTZType;
  col_obj_types_[8] = ObTimestampTZType;
  col_obj_types_[9] = ObIntervalYMType;
  col_obj_types_[10] = ObIntervalYMType;
}

void TestColumnDecoder::set_column_type_column_substring()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  column_cnt_ = 7;
  rowkey_cnt_ = 1;
  col_obj_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
  col_obj_types_[0] = ObIntType;
  col_obj_types_[1] = ObVarcharType;
  col_obj_types_[2] = ObVarcharType;
  col_obj_types_[3] = ObCharType;
  col_obj_types_[4] = ObCharType;
  col_obj_types_[5] = ObHexStringType;
  col_obj_types_[6] = ObHexStringType;
}

void TestColumnDecoder::SetUp()
{
  if (column_encoding_type_ == ObColumnHeader::Type::INTEGER_BASE_DIFF) {
    set_column_type_integer();
  } else if (column_encoding_type_ == ObColumnHeader::Type::HEX_PACKING
      || column_encoding_type_ == ObColumnHeader::Type::STRING_DIFF
      || column_encoding_type_ == ObColumnHeader::Type::STRING_PREFIX) {
    set_column_type_string();
  } else if (column_encoding_type_ == ObColumnHeader::Type::COLUMN_EQUAL){
    set_column_type_column_equal();
  } else if (column_encoding_type_ == ObColumnHeader::Type::COLUMN_SUBSTR){
    set_column_type_column_substring();
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

    if (type == ObDecimalIntType) {
      col.set_data_precision(10);
      col.set_data_scale(0);
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
  ctx_.compressor_type_ = common::ObCompressorType::NONE_COMPRESSOR;

  if (!is_retro_) {
    int64_t *column_encodings = reinterpret_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * ctx_.column_cnt_));
    ctx_.column_encodings_ = column_encodings;
    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
        ctx_.column_encodings_[i] = ObColumnHeader::Type::RAW;
        continue;
      }
      if (ObColumnHeader::Type::COLUMN_EQUAL == column_encoding_type_ || ObColumnHeader::Type::COLUMN_SUBSTR == column_encoding_type_){
        if(i >= read_info_.get_rowkey_count() && i % 2){
          ctx_.column_encodings_[i] = column_encoding_type_;
        } else {
          ctx_.column_encodings_[i] = ObColumnHeader::Type::RAW;
        }
      } else if (ObColumnHeader::Type::INTEGER_BASE_DIFF == column_encoding_type_) {
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

void TestColumnDecoder::init_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf)
{
  int count = filter_objs.count();
  ObWhiteFilterOperatorType op_type = filter.filter_.get_op_type();
  if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
    count = 1;
  }

  filter.filter_.expr_ = new (expr_buf) ObExpr();
  filter.filter_.expr_->arg_cnt_ = count + 1;
  filter.filter_.expr_->args_ = expr_p_buf;
  ASSERT_EQ(OB_SUCCESS, filter.datum_params_.init(count));

  for (int64_t i = 0; i <= count; ++i) {
    filter.filter_.expr_->args_[i] = new (expr_buf + 1 + i) ObExpr();
    if (i < count) {
      if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
        filter.filter_.expr_->args_[i]->obj_meta_.set_null();
        filter.filter_.expr_->args_[i]->datum_meta_.type_ = ObNullType;
      } else {
        filter.filter_.expr_->args_[i]->obj_meta_ = filter_objs.at(i).get_meta();
        filter.filter_.expr_->args_[i]->datum_meta_.type_ = filter_objs.at(i).get_meta().get_type();
        datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
        datums[i].from_obj(filter_objs.at(i));
        ASSERT_EQ(OB_SUCCESS, filter.datum_params_.push_back(datums[i]));
        if (filter.is_null_param(datums[i], filter_objs.at(i).get_meta())) {
          filter.null_param_contained_ = true;
        }
      }
    } else {
      filter.filter_.expr_->args_[i]->type_ = T_REF_COLUMN;
      filter.filter_.expr_->args_[i]->obj_meta_.set_null(); // unused
    }
  }
  filter.cmp_func_ = get_datum_cmp_func(filter.filter_.expr_->args_[0]->obj_meta_, filter.filter_.expr_->args_[0]->obj_meta_);
}

void TestColumnDecoder::init_in_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf)
{
  int count = filter_objs.count();
  ASSERT_TRUE(count > 0);
  filter.filter_.expr_ = new (expr_buf) ObExpr();
  filter.filter_.expr_->arg_cnt_ = 2;
  filter.filter_.expr_->args_ = expr_p_buf;
  filter.filter_.expr_->args_[0] = new (expr_buf + 1) ObExpr();
  filter.filter_.expr_->args_[1] = new (expr_buf + 2) ObExpr();
  filter.filter_.expr_->inner_func_cnt_ = count;
  filter.filter_.expr_->args_[1]->args_ = expr_p_buf + 2;

  ObObjMeta obj_meta = filter_objs.at(0).get_meta();
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
    obj_meta.get_type(), obj_meta.get_collation_type(), obj_meta.get_scale(), false, obj_meta.has_lob_header());
  ObDatumCmpFuncType cmp_func = get_datum_cmp_func(obj_meta, obj_meta);

  filter.filter_.expr_->args_[0]->type_ = T_REF_COLUMN;
  filter.filter_.expr_->args_[0]->obj_meta_ = obj_meta;
  filter.filter_.expr_->args_[0]->datum_meta_.type_ = obj_meta.get_type();
  filter.filter_.expr_->args_[0]->basic_funcs_ = basic_funcs;

  ASSERT_EQ(OB_SUCCESS, filter.datum_params_.init(count));
  ASSERT_EQ(OB_SUCCESS, filter.param_set_.create(count * 2));
  filter.param_set_.set_hash_and_cmp_func(basic_funcs->murmur_hash_v2_, basic_funcs->null_first_cmp_);
  for (int64_t i = 0; i < count; ++i) {
    filter.filter_.expr_->args_[1]->args_[i] = new (expr_buf + 3 + i) ObExpr();
    filter.filter_.expr_->args_[1]->args_[i]->obj_meta_ = obj_meta;
    filter.filter_.expr_->args_[1]->args_[i]->datum_meta_.type_ = obj_meta.get_type();
    filter.filter_.expr_->args_[1]->args_[i]->basic_funcs_ = basic_funcs;
    datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
    datums[i].from_obj(filter_objs.at(i));
    if (!filter.is_null_param(datums[i], filter_objs.at(i).get_meta())) {
      ASSERT_EQ(OB_SUCCESS, filter.add_to_param_set_and_array(datums[i], filter.filter_.expr_->args_[1]->args_[i]));
    }
  }
  std::sort(filter.datum_params_.begin(), filter.datum_params_.end(),
            [cmp_func] (const ObDatum datum1, const ObDatum datum2) {
                int cmp_ret = 0;
                cmp_func(datum1, datum2, cmp_ret);
                return cmp_ret < 0;
            });
  filter.cmp_func_ = cmp_func;
  filter.cmp_func_rev_ = cmp_func;
  filter.param_set_.set_hash_and_cmp_func(basic_funcs->murmur_hash_v2_, filter.cmp_func_rev_);
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
  sql::PushdownFilterInfo pd_filter_info;
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
  void *storage_datum_buf = allocator_.alloc(sizeof(ObStorageDatum) * COLUMN_CNT);
  EXPECT_TRUE(storage_datum_buf != nullptr);
  pd_filter_info.datum_buf_ = new (storage_datum_buf) ObStorageDatum [COLUMN_CNT]();
  pd_filter_info.col_capacity_ = full_column_cnt_;
  pd_filter_info.start_ = 0;
  pd_filter_info.count_ = decoder.row_count_;

  int count = objs.count();
  ObWhiteFilterOperatorType op_type = filter_node.get_op_type();
  if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
    count = 1;
  }
  int count_expr = WHITE_OP_IN == op_type ? count + 3 : count + 2;
  int count_expr_p = WHITE_OP_IN == op_type ? count + 2 : count + 1;
  sql::ObExpr *expr_buf = reinterpret_cast<sql::ObExpr *>(allocator_.alloc(sizeof(sql::ObExpr) * count_expr));
  sql::ObExpr **expr_p_buf = reinterpret_cast<sql::ObExpr **>(allocator_.alloc(sizeof(sql::ObExpr*) * count_expr_p));
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128 * count);
  ObDatum datums[count];
  EXPECT_TRUE(OB_NOT_NULL(expr_buf));
  EXPECT_TRUE(OB_NOT_NULL(expr_p_buf));

  if (WHITE_OP_IN == op_type) {
    init_in_filter(filter, objs, expr_buf, expr_p_buf, datums, datum_buf);
  } else {
    init_filter(filter, objs, expr_buf, expr_p_buf, datums, datum_buf);
  }

  if (OB_UNLIKELY(2 > filter.filter_.expr_->arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter expr", K(ret), K(filter.filter_.expr_->arg_cnt_));
  } else {
    if (is_retro) {
      ret = decoder.filter_pushdown_retro(nullptr, filter, pd_filter_info, col_idx, filter.col_params_.at(0), pd_filter_info.datum_buf_[0], result_bitmap);
    } else {
      ret = decoder.filter_pushdown_filter(nullptr, filter, pd_filter_info, result_bitmap);
    }
  }
  if (nullptr != storage_datum_buf) {
    allocator_.free(storage_datum_buf);
  }
  if (nullptr != expr_buf) {
    allocator_.free(expr_buf);
  }
  if (nullptr != expr_p_buf) {
    allocator_.free(expr_p_buf);
  }
  if (nullptr != datum_buf) {
    allocator_.free(datum_buf);
  }
  return ret;
}
int TestColumnDecoder::test_filter_pushdown_with_pd_info(
    int64_t start,
    int64_t end,
    const uint64_t col_idx,
    bool is_retro,
    ObMicroBlockDecoder& decoder,
    sql::ObPushdownWhiteFilterNode &filter_node,
    common::ObBitmap &result_bitmap,
    common::ObFixedArray<ObObj, ObIAllocator> &objs)
{
  int ret = OB_SUCCESS;
  sql::PushdownFilterInfo pd_filter_info;
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
  void *storage_datum_buf = allocator_.alloc(sizeof(ObStorageDatum) * COLUMN_CNT);
  EXPECT_TRUE(storage_datum_buf != nullptr);
  pd_filter_info.datum_buf_ = new (storage_datum_buf) ObStorageDatum [COLUMN_CNT]();
  pd_filter_info.col_capacity_ = full_column_cnt_;
  pd_filter_info.start_ = start;
  pd_filter_info.count_ = end - start;

  int count = objs.count();
  ObWhiteFilterOperatorType op_type = filter_node.get_op_type();
  if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
    count = 1;
  }
  int count_expr = WHITE_OP_IN == op_type ? count + 3 : count + 2;
  int count_expr_p = WHITE_OP_IN == op_type ? count + 2 : count + 1;
  sql::ObExpr *expr_buf = reinterpret_cast<sql::ObExpr *>(allocator_.alloc(sizeof(sql::ObExpr) * count_expr));
  sql::ObExpr **expr_p_buf = reinterpret_cast<sql::ObExpr **>(allocator_.alloc(sizeof(sql::ObExpr*) * count_expr_p));
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128 * count);
  ObDatum datums[count];
  EXPECT_TRUE(OB_NOT_NULL(expr_buf));
  EXPECT_TRUE(OB_NOT_NULL(expr_p_buf));

  if (WHITE_OP_IN == op_type) {
    init_in_filter(filter, objs, expr_buf, expr_p_buf, datums, datum_buf);
  } else {
    init_filter(filter, objs, expr_buf, expr_p_buf, datums, datum_buf);
  }

  if (OB_UNLIKELY(2 > filter.filter_.expr_->arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter expr", K(ret), K(filter.filter_.expr_->arg_cnt_));
  } else {
    if (is_retro) {
      ret = decoder.filter_pushdown_retro(nullptr, filter, pd_filter_info, col_idx, filter.col_params_.at(0), pd_filter_info.datum_buf_[0], result_bitmap);
    } else {
      ret = decoder.filter_pushdown_filter(nullptr, filter, pd_filter_info, result_bitmap);
    }
  }
  if (nullptr != storage_datum_buf) {
    allocator_.free(storage_datum_buf);
  }
  if (nullptr != expr_buf) {
    allocator_.free(expr_buf);
  }
  if (nullptr != expr_p_buf) {
    allocator_.free(expr_p_buf);
  }
  if (nullptr != datum_buf) {
    allocator_.free(datum_buf);
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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

    // 0--- ROW_CNT-40 --- ROW_CNT-30 --- ROW_CNT-20 ---ROW_CNT-10 --- ROW_CNT
    // |    seed0   |   seed1   |     seed2     |   seed3    |     null      |
    //                    |                                          |
    //                ROW_CNT-35                                 ROW_CNT-5

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_IN;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5 + seed2_count, pd_result_bitmap.popcnt());

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
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);

    //  0 --- ROW_CNT-40 --- ROW_CNT-30 --- ROW_CNT-10 --- ROW_CNT
    //  |    seed1   |   null   |     seed2     |   null    |
    //       	  |                        |
    //       ROW_CNT-50              ROW_CNT-20
    white_filter.op_type_ = sql::WHITE_OP_NU;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(null_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 50, ROW_CNT - 20, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(10, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NN;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 50, ROW_CNT - 20, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(20, pd_result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_EQ;
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 50, ROW_CNT - 20, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(10, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 50, ROW_CNT - 20, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(10, pd_result_bitmap.popcnt());

    objs.pop_back();
    objs.push_back(ref_obj_2);
    white_filter.op_type_ = sql::WHITE_OP_EQ;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 50, ROW_CNT - 20, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(10, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 50, ROW_CNT - 20, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(10, pd_result_bitmap.popcnt());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);

  // 0--- ROW_CNT-30 --- ROW_CNT-20 --- ROW_CNT-10 --- ROW_CNT
  // |    seed0   |   seed1   |     seed2     |   null    |
  //           |                                   |
  //       ROW_CNT-35                           ROW_CNT-5
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
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    // Greater Than
    white_filter.op_type_ = sql::WHITE_OP_GT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5, pd_result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(15, pd_result_bitmap.popcnt());

    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(seed1_count + 5, pd_result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(seed1_count + 15, pd_result_bitmap.popcnt());

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
      pd_result_bitmap.reuse();
      ASSERT_EQ(0, pd_result_bitmap.popcnt());
      ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
      ASSERT_EQ(15 + seed1_count + 5, pd_result_bitmap.popcnt());

      white_filter.op_type_ = sql::WHITE_OP_LT;
      result_bitmap.reuse();
      ASSERT_EQ(0, result_bitmap.popcnt());
      ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
      ASSERT_EQ(0, result_bitmap.popcnt());
      pd_result_bitmap.reuse();
      ASSERT_EQ(0, pd_result_bitmap.popcnt());
      ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
      ASSERT_EQ(0, pd_result_bitmap.popcnt());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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
    // 0--- ROW_CNT-30 --- ROW_CNT-20 --- ROW_CNT
    // |    seed0   |   seed1   |     seed2     |
    //         |                    |
    //       ROW_CNT-45             ROW_CNT-15

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_GT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(seed1_count + 5, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(15, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(15 + seed1_count, pd_result_bitmap.popcnt());

    setup_obj(ref_obj, i, ref_seed1);
    objs.clear();
    objs.push_back(ref_obj);

    white_filter.op_type_ = sql::WHITE_OP_GT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count + seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(15 + seed1_count + 5, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count + seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(15 + seed1_count + 5, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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

    // 0 --------- ROW_CNT-10 --- ROW_CNT
    // |   seed0      |    seed1    |
    //         |               |
    //     ROW_CNT-35      ROW_CNT-5

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    // Normal
    white_filter.op_type_ = sql::WHITE_OP_BT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5, pd_result_bitmap.popcnt());

    // Empty range
    objs.reuse();
    objs.init(2);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj1);
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, is_retro_, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, is_retro_, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
  }
}

void TestColumnDecoder::batch_decode_to_datum_test(bool is_condensed)
{
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
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
      rows[i].deep_copy(row, allocator_);
    }
    for (int64_t i = ROW_CNT - 60; i < ROW_CNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row));
      rows[i].deep_copy(row, allocator_);
    }
    const_cast<bool &>(encoder_.ctx_.encoder_opt_.enable_bit_packing_) = false;
  } else {
    for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, allocator_);
    }
    for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, allocator_);
    }

    for (int64_t j = 0; j < full_column_cnt_; ++j) {
      row.storage_datums_[j].set_null();
    }
    for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, allocator_);
    }
    for (int64_t i = ROW_CNT - 30; i < ROW_CNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, allocator_);
    }
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  const ObRowHeader *row_header = nullptr;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  const char *cell_datas[ROW_CNT];
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128 * ROW_CNT);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    STORAGE_LOG(INFO, "Current col: ", K(i), K(col_descs_.at(i)),
        K(row.storage_datums_[i]), K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int32_t row_ids[ROW_CNT];
    for (int32_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
              .batch_decode(decoder.row_index_,
                            row_ids,
                            cell_datas,
                            ROW_CNT,
                            datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datums[j]));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datums[j]));
    }
  }
}

void TestColumnDecoder::cell_decode_to_datum_test()
{
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  int64_t row_len = 0;
  const char *row_data = nullptr;
  void *datum_buf_1 = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_descs_.at(i)),  K(*decoder.decoders_[col_offset].ctx_));

    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObDatum datum;
      datum.ptr_ = reinterpret_cast<char *>(datum_buf_1);
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(j, row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(datum,j, bs, row_data, row_len));
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datum));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datum));
    }
  }
}

void TestColumnDecoder::cell_decode_to_datum_test_without_hex()
{
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  const char *str_test = "cell_decode_to_datum_test_without_hex_string";

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = read_info_.get_rowkey_count(); j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_string(str_test,strlen(str_test));
  }
  for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  int64_t row_len = 0;
  const char *row_data = nullptr;
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_descs_.at(i)),  K(*decoder.decoders_[col_offset].ctx_));

    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObDatum datum;
      datum.ptr_ = reinterpret_cast<char *>(datum_buf);
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(j, row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(datum,j, bs, row_data, row_len));
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datum));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datum));
    }
  }
}

void TestColumnDecoder::cell_column_equal_decode_to_datum_test()
{
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_; j += 2) {
      row.storage_datums_[j] = row.storage_datums_[j - 1];
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_; j += 2) {
      row.storage_datums_[j] = row.storage_datums_[j - 1];
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_; j += 2) {
      row.storage_datums_[j] = row.storage_datums_[j - 1];
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  // generate exception data
  for (int64_t i = ROW_CNT - 2; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  int64_t row_len = 0;
  const char *row_data = nullptr;
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_descs_.at(i)),  K(*decoder.decoders_[col_offset].ctx_));

    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObDatum datum;
      datum.ptr_ = reinterpret_cast<char *>(datum_buf);
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(j, row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(datum,j, bs, row_data, row_len));
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datum));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datum));
    }
  }
}

void TestColumnDecoder::cell_inter_column_substring_to_datum_test()
{
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    // generate different start point data
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_ - 2; j += 2) {
      ObString str = row.storage_datums_[j].get_string();
      ObString sub_str;
      ob_sub_str(allocator_, str, i / 5, sub_str);
      row.storage_datums_[j - 1].set_string(sub_str);
    }
    // generate same start point data
    ObString str = row.storage_datums_[full_column_cnt_ - 1].get_string();
    ObString sub_str;
    ob_sub_str(allocator_,str,1,sub_str);
    row.storage_datums_[full_column_cnt_ - 2].set_string(sub_str);
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_ - 2; j += 2) {
      ObString str = row.storage_datums_[j].get_string();
      ObString sub_str;
      ob_sub_str(allocator_, str, i / 5, sub_str);
      row.storage_datums_[j - 1].set_string(sub_str);
    }
    ObString str = row.storage_datums_[full_column_cnt_ - 1].get_string();
    ObString sub_str;
    ob_sub_str(allocator_,str,1,sub_str);
    row.storage_datums_[full_column_cnt_ - 2].set_string(sub_str);
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_ - 2; j += 2) {
      ObString str = row.storage_datums_[j].get_string();
      ObString sub_str;
      ob_sub_str(allocator_, str, i / 5, sub_str);
      row.storage_datums_[j - 1].set_string(sub_str);
    }
    ObString str = row.storage_datums_[full_column_cnt_ - 1].get_string();
    ObString sub_str;
    ob_sub_str(allocator_,str,1,sub_str);
    row.storage_datums_[full_column_cnt_ - 2].set_string(sub_str);
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  // generate exception data
  for (int64_t i = ROW_CNT - 2; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  int64_t row_len = 0;
  const char *row_data = nullptr;
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_descs_.at(i)),  K(*decoder.decoders_[col_offset].ctx_));

    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObDatum datum;
      datum.ptr_ = reinterpret_cast<char *>(datum_buf);
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(j, row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(datum,j, bs, row_data, row_len));
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datum));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datum));
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
//   ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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

void TestColumnDecoder::batch_decode_to_vector_test(
    const bool is_condensed,
    const bool has_null,
    const bool align_row_id,
    const VectorFormat vector_format)
{
  FLOG_INFO("start one batch decode to vector test", K(is_condensed), K(has_null), K(vector_format));
  ObArenaAllocator test_allocator;
  encoder_.reuse();
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(test_allocator, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(test_allocator, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }
  for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }

  if (has_null) {
    for (int64_t j = 0; j < full_column_cnt_; ++j) {
      row.storage_datums_[j].set_null();
    }
    for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, test_allocator);
    }
  } else {
    for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, test_allocator);
    }
  }

  for (int64_t i = ROW_CNT - 30; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }
  if (is_condensed) {
    const_cast<bool &>(encoder_.ctx_.encoder_opt_.enable_bit_packing_) = false;
  } else {
    const_cast<bool &>(encoder_.ctx_.encoder_opt_.enable_bit_packing_) = true;
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));

  ObArenaAllocator frame_allocator;
  sql::ObExecContext exec_context(test_allocator);
  sql::ObEvalCtx eval_ctx(exec_context);
  const char *ptr_arr[ROW_CNT];
  uint32_t len_arr[ROW_CNT];
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    bool need_test_column = true;
    ObObjMeta col_meta = col_descs_.at(i).col_type_;
    const int16_t precision = col_meta.is_decimal_int() ? col_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
    VecValueTypeClass vec_tc = common::get_vec_value_tc(
        col_meta.get_type(),
        col_meta.get_scale(),
        precision);
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      need_test_column = false;
    } else {
      need_test_column = VectorDecodeTestUtil::need_test_vec_with_type(vector_format, vec_tc);
    }

    if (!need_test_column) {
      continue;
    }

    sql::ObExpr col_expr;
    int64_t test_row_cnt = align_row_id ? ROW_CNT : ROW_CNT / 2;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        ROW_CNT, col_meta, vector_format, eval_ctx, col_expr, frame_allocator));
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_meta),  K(*decoder.decoders_[col_offset].ctx_), K(precision), K(vec_tc));

    int32_t row_ids[test_row_cnt];
    int32_t row_id_idx = 0;
    for (int64_t datum_idx = 0; datum_idx < ROW_CNT; ++datum_idx) {
      if (!align_row_id && 0 == datum_idx % 2) {
        // skip
      } else if (row_id_idx == test_row_cnt) {
        // skip
      } else {
        row_ids[row_id_idx] = datum_idx;
        ++row_id_idx;
      }
    }

    ObVectorDecodeCtx vector_ctx(ptr_arr, len_arr, row_ids, test_row_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset].decode_vector(decoder.row_index_, vector_ctx));
    for (int64_t vec_idx = 0; vec_idx < test_row_cnt; ++vec_idx) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match(*(col_expr.get_vector_header(eval_ctx).get_vector()),
          vec_idx, rows[row_ids[vec_idx]].storage_datums_[col_offset]));
    }

    // ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::test_batch_decode_perf(decoder, col_offset, col_meta, 100000, vector_format));
    decoder.decoder_allocator_.reuse();
  }
}

void TestColumnDecoder::col_equal_batch_decode_to_vector_test(const VectorFormat vector_format)
{
  FLOG_INFO("start one column equal batch decode to vector test", K(vector_format));
  ObArenaAllocator test_allocator;
  encoder_.reuse();
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_; j += 2) {
      row.storage_datums_[j] = row.storage_datums_[j - 1];
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_; j += 2) {
      row.storage_datums_[j] = row.storage_datums_[j - 1];
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_; j += 2) {
      row.storage_datums_[j] = row.storage_datums_[j - 1];
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  // generate exception data
  for (int64_t i = ROW_CNT - 2; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));

  ObArenaAllocator frame_allocator;
  sql::ObExecContext exec_context(test_allocator);
  sql::ObEvalCtx eval_ctx(exec_context);
  const char *ptr_arr[ROW_CNT];
  uint32_t len_arr[ROW_CNT];
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    bool need_test_column = true;
    ObObjMeta col_meta = col_descs_.at(i).col_type_;
    const int16_t precision = col_meta.is_decimal_int() ? col_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
    VecValueTypeClass vec_tc = common::get_vec_value_tc(
        col_meta.get_type(),
        col_meta.get_scale(),
        precision);
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      need_test_column = false;
    } else {
      need_test_column = VectorDecodeTestUtil::need_test_vec_with_type(vector_format, vec_tc);
    }
    if (!need_test_column) {
      continue;
    }
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        ROW_CNT, col_meta, vector_format, eval_ctx, col_expr, frame_allocator));
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_meta),  K(*decoder.decoders_[col_offset].ctx_), K(precision), K(vec_tc));

    int32_t row_ids[ROW_CNT];
    for (int32_t datum_idx = 0; datum_idx < ROW_CNT; ++datum_idx) {
      row_ids[datum_idx] = datum_idx;
    }

    ObVectorDecodeCtx vector_ctx(ptr_arr, len_arr, row_ids, ROW_CNT, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset].decode_vector(decoder.row_index_, vector_ctx));
    for (int64_t vec_idx = 0; vec_idx < ROW_CNT; ++vec_idx) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match(*(col_expr.get_vector_header(eval_ctx).get_vector()),
          vec_idx, rows[vec_idx].storage_datums_[col_offset]));
    }
  }
}

void TestColumnDecoder::col_substr_batch_decode_to_vector_test(const VectorFormat vector_format)
{
  FLOG_INFO("start one column substring batch decode to vector test", K(vector_format));
  ObArenaAllocator test_allocator;
  encoder_.reuse();
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t seed1 = 10001;

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    // generate different start point data
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_ - 2; j += 2) {
      ObString str = row.storage_datums_[j].get_string();
      ObString sub_str;
      ob_sub_str(allocator_, str, i / 5, sub_str);
      row.storage_datums_[j - 1].set_string(sub_str);
    }
    // generate same start point data
    ObString str = row.storage_datums_[full_column_cnt_ - 1].get_string();
    ObString sub_str;
    ob_sub_str(allocator_,str,1,sub_str);
    row.storage_datums_[full_column_cnt_ - 2].set_string(sub_str);
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 35; i < ROW_CNT - 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_ - 2; j += 2) {
      ObString str = row.storage_datums_[j].get_string();
      ObString sub_str;
      ob_sub_str(allocator_, str, i / 5, sub_str);
      row.storage_datums_[j - 1].set_string(sub_str);
    }
    ObString str = row.storage_datums_[full_column_cnt_ - 1].get_string();
    ObString sub_str;
    ob_sub_str(allocator_,str,1,sub_str);
    row.storage_datums_[full_column_cnt_ - 2].set_string(sub_str);
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 32; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_ - 2; j += 2) {
      ObString str = row.storage_datums_[j].get_string();
      ObString sub_str;
      ob_sub_str(allocator_, str, i / 5, sub_str);
      row.storage_datums_[j - 1].set_string(sub_str);
    }
    ObString str = row.storage_datums_[full_column_cnt_ - 1].get_string();
    ObString sub_str;
    ob_sub_str(allocator_,str,1,sub_str);
    row.storage_datums_[full_column_cnt_ - 2].set_string(sub_str);
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  // generate exception data
  for (int64_t i = ROW_CNT - 2; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  ObArenaAllocator frame_allocator;
  sql::ObExecContext exec_context(test_allocator);
  sql::ObEvalCtx eval_ctx(exec_context);
  const char *ptr_arr[ROW_CNT];
  uint32_t len_arr[ROW_CNT];
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    bool need_test_column = true;
    ObObjMeta col_meta = col_descs_.at(i).col_type_;
    const int16_t precision = col_meta.is_decimal_int() ? col_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
    VecValueTypeClass vec_tc = common::get_vec_value_tc(
        col_meta.get_type(),
        col_meta.get_scale(),
        precision);
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      need_test_column = false;
    } else {
      need_test_column = VectorDecodeTestUtil::need_test_vec_with_type(vector_format, vec_tc);
    }
    if (!need_test_column) {
      continue;
    }
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        ROW_CNT, col_meta, vector_format, eval_ctx, col_expr, frame_allocator));
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_meta),  K(*decoder.decoders_[col_offset].ctx_), K(precision), K(vec_tc));

    int32_t row_ids[ROW_CNT];
    for (int32_t datum_idx = 0; datum_idx < ROW_CNT; ++datum_idx) {
      row_ids[datum_idx] = datum_idx;
    }

    ObVectorDecodeCtx vector_ctx(ptr_arr, len_arr, row_ids, ROW_CNT, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset].decode_vector(decoder.row_index_, vector_ctx));
    for (int64_t vec_idx = 0; vec_idx < ROW_CNT; ++vec_idx) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match(*(col_expr.get_vector_header(eval_ctx).get_vector()),
          vec_idx, rows[vec_idx].storage_datums_[col_offset]));
    }
  }
}

int VectorDecodeTestUtil::generate_column_output_expr(
    const int64_t batch_cnt,
    const ObObjMeta &obj_meta,
    const VectorFormat &format,
    sql::ObEvalCtx &eval_ctx,
    sql::ObExpr &col_expr,
    ObIAllocator &frame_allocator)
{
  // int ObStaticEngineExprCG::arrange_datums_data
  int ret = OB_SUCCESS;
  int64_t cur_total_size = 0;
  col_expr.reset();
  col_expr.batch_result_ = 1;
  // hard coded
  col_expr.frame_idx_ = 0;
  col_expr.res_buf_len_ = 128;

  const int16_t precision = obj_meta.is_decimal_int() ? obj_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;

  col_expr.vec_value_tc_ = get_vec_value_tc(obj_meta.get_type(), obj_meta.get_scale(), precision);
  col_expr.is_fixed_length_data_ = common::is_fixed_length_vec(col_expr.vec_value_tc_);

  col_expr.datum_off_ = cur_total_size;
  cur_total_size += sizeof(ObDatum) * batch_cnt;

  col_expr.pvt_skip_off_ = cur_total_size;
  cur_total_size += sql::ObBitVector::memory_size(batch_cnt);

  if (!col_expr.is_fixed_length_data_) {
    col_expr.len_arr_off_ = cur_total_size;
    cur_total_size += sizeof(uint32_t) * (batch_cnt + 1); // offsets size

    col_expr.offset_off_ = cur_total_size;
    cur_total_size += sizeof(char *) * (batch_cnt); // ptrs size
  }

  col_expr.vector_header_off_ = cur_total_size;
  cur_total_size += sizeof(sql::VectorHeader);

  col_expr.null_bitmap_off_ = cur_total_size;
  cur_total_size += sql::ObBitVector::memory_size(batch_cnt);

  if (!col_expr.is_fixed_length_data_) {
    col_expr.cont_buf_off_ = cur_total_size;
    cur_total_size += sizeof(sql::ObDynReserveBuf);
  }

  col_expr.eval_info_off_ = cur_total_size;
  cur_total_size += sizeof(sql::ObEvalInfo);

  col_expr.eval_flags_off_ = cur_total_size;
  cur_total_size += sql::ObBitVector::memory_size(batch_cnt);

  col_expr.dyn_buf_header_offset_ = cur_total_size;
  cur_total_size += sql::ObDynReserveBuf::supported(obj_meta.get_type()) ? batch_cnt * sizeof(sql::ObDynReserveBuf) : 0;

  col_expr.res_buf_off_ = cur_total_size;
  cur_total_size += col_expr.res_buf_len_ * batch_cnt;

  char **frame_arr = (char **)frame_allocator.alloc(sizeof(char *));
  char *frame = (char *)frame_allocator.alloc(cur_total_size);
  if (nullptr == frame || nullptr == frame_arr) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "null frame", K(ret));
  } else {
    memset(frame, 0, cur_total_size);
    eval_ctx.frames_ = frame_arr;
    eval_ctx.frames_[0] = frame;
    if (OB_FAIL(col_expr.init_vector(eval_ctx, format, batch_cnt))) {
      STORAGE_LOG(WARN, "failed to init vector", K(ret));
    } else {
      if (is_uniform_format(format)) {
        col_expr.reset_datums_ptr(frame, batch_cnt);
      } else if (is_discrete_format(format)) {
        col_expr.reset_discretes_ptr(frame, batch_cnt, col_expr.get_discrete_vector_ptrs(eval_ctx));
      }
    }
  }

  return ret;
}

bool VectorDecodeTestUtil::verify_vector_and_datum_match(
    const ObIVector &vector,
    const int64_t vec_idx,
    const ObDatum &datum)
{
  int bret = false;
  ObDatum vec_datum;
  if (datum.is_null()) {
    bret = vector.is_null(vec_idx);
  } else {
    ObLength length = vector.get_length(vec_idx);
    vec_datum.len_ = length;
    vec_datum.ptr_ = vector.get_payload(vec_idx);
    bret = ObDatum::binary_equal(vec_datum, datum);
  }
  if (!bret) {
    LOG_INFO("datum not match with datum from vector", K(vec_idx), K(datum), K(vec_datum));
  }
  return bret;
}

bool VectorDecodeTestUtil::need_test_vec_with_type(
    const VectorFormat &vector_format,
    const VecValueTypeClass &vec_tc)
{
  bool need_test_column = true;
  if (vector_format == VEC_FIXED) {
    VecValueTypeClass fixed_tc_arr[] = {VEC_TC_INTEGER, VEC_TC_UINTEGER, VEC_TC_FLOAT, VEC_TC_DOUBLE,
        VEC_TC_FIXED_DOUBLE, VEC_TC_DATETIME, VEC_TC_DATE, VEC_TC_TIME, VEC_TC_YEAR, VEC_TC_UNKNOWN,
        VEC_TC_BIT, VEC_TC_ENUM_SET, VEC_TC_TIMESTAMP_TZ, VEC_TC_TIMESTAMP_TINY, VEC_TC_INTERVAL_YM,
        VEC_TC_INTERVAL_DS, VEC_TC_DEC_INT32, VEC_TC_DEC_INT64, VEC_TC_DEC_INT128, VEC_TC_DEC_INT256,
        VEC_TC_DEC_INT512};
    VecValueTypeClass *vec = std::find(std::begin(fixed_tc_arr), std::end(fixed_tc_arr), vec_tc);
    if (vec == std::end(fixed_tc_arr)) {
      need_test_column = false;
    }
  } else if (vector_format == VEC_DISCRETE) {
    VecValueTypeClass var_tc_arr[] = {VEC_TC_NUMBER, VEC_TC_EXTEND, VEC_TC_STRING, VEC_TC_ENUM_SET_INNER,
        VEC_TC_RAW, VEC_TC_ROWID, VEC_TC_LOB, VEC_TC_JSON, VEC_TC_GEO, VEC_TC_UDT, VEC_TC_ROARINGBITMAP};
    VecValueTypeClass *vec = std::find(std::begin(var_tc_arr), std::end(var_tc_arr), vec_tc);
    if (vec == std::end(var_tc_arr)) {
      need_test_column = false;
    }
  } else if (vector_format == VEC_CONTINUOUS) {
    // not support shallow copy to continuous vector for now
    need_test_column = VEC_TC_NUMBER == vec_tc;
  } else {
    need_test_column = true;
  }
  return need_test_column;
}

int VectorDecodeTestUtil::test_batch_decode_perf(
    ObMicroBlockDecoder &decoder,
    const int64_t col_idx,
    const ObObjMeta col_meta,
    const int64_t decode_cnt,
    const VectorFormat vector_format)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator;
  sql::ObExecContext exec_context(test_allocator);
  sql::ObEvalCtx eval_ctx(exec_context);
  sql::ObExpr col_expr;
  const int16_t precision = col_meta.is_decimal_int() ? col_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
  VecValueTypeClass vec_tc = common::get_vec_value_tc(col_meta.get_type(), col_meta.get_scale(), precision);
  int64_t row_cnt = 0;
  void *datum_buf = nullptr;
  if (OB_UNLIKELY(!VectorDecodeTestUtil::need_test_vec_with_type(vector_format, vec_tc))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported test vector type with column meta", K(ret),
        K(vector_format), K(col_meta), K(vec_tc), K(precision));
  } else if (OB_FAIL(decoder.get_row_count(row_cnt))) {
    LOG_WARN("failed to get row cnt", K(ret));
  } else if (OB_FAIL(VectorDecodeTestUtil::generate_column_output_expr(
      row_cnt, col_meta, vector_format, eval_ctx, col_expr, test_allocator))) {
    LOG_WARN("failed to generate column output expr", K(ret));
  } else if (OB_ISNULL(datum_buf = test_allocator.alloc(sizeof(int8_t) * 128 * row_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for datum buffer", K(ret));
  } else {
    // decode to vector
    const char *ptr_arr[row_cnt];
    uint32_t len_arr[row_cnt];
    int32_t row_ids[row_cnt];
    for (int32_t datum_idx = 0; datum_idx < row_cnt; ++datum_idx) {
      row_ids[datum_idx] = datum_idx;
    }
    ObVectorDecodeCtx decode_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));

    const int64_t vector_start_ts = ObTimeUtility::current_time();
    for (int64_t decode_round = 0; decode_round < decode_cnt; ++decode_round) {
      if (OB_FAIL(decoder.decoders_[col_idx].decode_vector(decoder.row_index_, decode_ctx))) {
        LOG_WARN("failed to decode vector", K(ret), K(decode_ctx));
      }
    }
    const int64_t vector_end_ts = ObTimeUtility::current_time();

    // decode to datums
    ObDatum datums[row_cnt];
    for (int64_t j = 0; j < row_cnt; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf) + j * 128;
      row_ids[j] = j;
    }

    const int64_t datum_start_ts = ObTimeUtility::current_time();
    for (int64_t decode_round = 0; decode_round < decode_cnt; ++decode_round) {
      if (OB_FAIL(decoder.decoders_[col_idx].batch_decode(decoder.row_index_, row_ids, ptr_arr, row_cnt, datums))) {
        LOG_WARN("failed to decode vector", K(ret), K(decode_ctx));
      }
    }
    const int64_t datum_end_ts = ObTimeUtility::current_time();
    FLOG_INFO("finish one batch decode perf test: ", K(col_idx), K(col_meta), K(vector_format),
      K(decode_cnt), K(vector_start_ts), K(vector_end_ts), K(datum_start_ts), K(datum_end_ts),
      "vector decode time", vector_end_ts - vector_start_ts,
      "batch decode time", datum_end_ts - datum_start_ts,
      KPC(decoder.decoders_[col_idx].ctx_));
  }

  return ret;
}



} // end of namespace blocksstable
} // end of namespace oceanbase

#endif // OCEANBASE_ENCODING_TEST_COLUMN_DECODER_H_
