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
#include <iostream>
#include <fstream>
#include <iostream>
#include <fstream>
#define protected public

#define private public
#include "ob_cs_encoding_test_base.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_util.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "lib/lob/ob_lob_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_diff.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/string/ob_hex_utils_base.h"
#include "share/datum/ob_datum.h"
#include "storage/blocksstable/cs_encoding/semistruct_encoding/ob_semistruct_encoding_util.h"
#include "sql/engine/expr/ob_json_param_type.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

#define ADD_NULL_JSON_NODE(obj, key_name, key, value_name) \
    ObString key_name(key); \
    ObJsonNull value_name; \
    ASSERT_EQ(OB_SUCCESS, obj.add(key_name, &value_name));

#define ADD_INT_JSON_NODE(obj, key_name, key, value_name, value) \
    ObString key_name(key); \
    ObJsonInt value_name(value); \
    ASSERT_EQ(OB_SUCCESS, obj.add(key_name, &value_name));

#define ADD_UINT_JSON_NODE(obj, key_name, key, value_name, value) \
    ObString key_name(key); \
    ObJsonUint value_name(value); \
    ASSERT_EQ(OB_SUCCESS, obj.add(key_name, &value_name));

#define ADD_DOUBLE_JSON_NODE(obj, key_name, key, value_name, value) \
    ObString key_name(key); \
    ObJsonDouble value_name(value); \
    ASSERT_EQ(OB_SUCCESS, obj.add(key_name, &value_name));

#define ADD_FLOAT_JSON_NODE(obj, key_name, key, value_name, value) \
    ObString key_name(key); \
    ObJsonOFloat value_name(value); \
    ASSERT_EQ(OB_SUCCESS, obj.add(key_name, &value_name));

#define ADD_STRING_JSON_NODE(obj, key_name, key, value_name, value_ptr, value_len) \
    ObString key_name(key); \
    ObJsonString value_name(value_ptr, value_len); \
    ASSERT_EQ(OB_SUCCESS, obj.add(key_name, &value_name));


static std::default_random_engine e_;
std::string str_rand(int length)
{
  std::uniform_int_distribution<int> u_l(0, length);
  std::uniform_int_distribution<int> u_data(0, 10000);

  int final_len = u_l(e_);
  char tmp;
  std::string buffer;
  for (int i = 0; i < final_len; i++) {
    tmp = u_data(e_) % 36;
    if (tmp < 10) {
      tmp += '0';
    } else {
      tmp -= 10;
      tmp += 'A';
    }
    buffer += tmp;
  }
  return buffer;
}

class TestSemiStructEncoding : public ObCSEncodingTestBase, public ::testing::Test {
public:
  TestSemiStructEncoding()
  {}
  ~TestSemiStructEncoding()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

  int do_encode(
      ObIAllocator& allocator,
      ObMicroBlockCSEncoder &encoder,
      const int64_t row_cnt,
      ObDatumRow &row,
      ObMicroBlockHeader *&header,
      ObMicroBlockDesc &micro_block_desc,
      ObColDatums& datums);

  int check_full_transform(
      ObIAllocator &allocator,
      const int64_t row_cnt,
      ObDatumRow &row,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      ObColDatums &datums);

  int check_freq_column(
      ObIAllocator &allocator,
      const ObString &path_str,
      ObColumnCSDecoderCtx &ctx,
      bool &is_freq_column);

  int decode_semistruct_col(
      ObIAllocator &allocator,
      const ObString &path_str,
      const int64_t row_idx,
      const ObColumnCSDecoderCtx &ctx,
      ObStorageDatum &datum);


  int check_part_transform(
      ObIAllocator &allocator,
      const int64_t row_cnt,
      const int64_t rowkey_cnt,
      const int64_t col_cnt,
      ObDatumRow &row,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      ObColDatums &datums);

  int check_int_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const int64_t filter_value,
      const int64_t res_count,
      const bool use_tinyint = false);
  int check_int_between_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const int64_t begin_value,
      const int64_t end_value,
      const int64_t res_count);
  int check_int_in_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const int64_t value1,
      const int64_t value2,
      const int64_t res_count);
  int check_uint_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const uint64_t filter_value,
      const int64_t res_count);
  int check_uint_between_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const uint64_t begin_value,
      const uint64_t end_value,
      const int64_t res_count);
  int check_uint_in_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const uint64_t value1,
      const uint64_t value2,
      const int64_t res_count);
  int check_str_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const ObString &filter_value,
      const int64_t res_count);
  int check_str_between_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const ObString &begin_value,
      const ObString &end_value,
      const int64_t res_count);
  int check_str_in_white_filter(
      ObIAllocator &allocator,
      ObMicroBlockHeader *header,
      ObMicroBlockDesc &micro_block_desc,
      const ObWhiteFilterOperatorType &op_type,
      const int64_t row_cnt,
      const int64_t col_cnt,
      const int64_t col_offset,
      const ObString &path_str,
      const ObString &value1,
      const ObString &value2,
      const int64_t res_count);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSemiStructEncoding);

};

int TestSemiStructEncoding::check_full_transform(
    ObIAllocator &allocator,
    const int64_t row_cnt,
    ObDatumRow &row,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    ObColDatums &datums)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    if (OB_FAIL(decoder.get_row(i, row))) {
      LOG_WARN("decode fail", K(ret), K(i));
    } else {
      LOG_TRACE("result row", K(i), K(row));
    }
    if (OB_FAIL(ret)) {
    } else if (row.storage_datums_[1].is_null()) {
      LOG_TRACE("compare info", K(datums.at(i)), K(row.storage_datums_[1]));
      if (! datums.at(i).is_null()) ob_abort();
    } else {
      const ObLobCommon& lob_common = row.storage_datums_[1].get_lob_data();
      ObString json_data(lob_common.get_byte_size(row.storage_datums_[1].len_), lob_common.get_inrow_data_ptr());
      ObJsonBuffer j_buf(&allocator);
      ObJsonBin j_bin(json_data.ptr(), json_data.length(), &allocator);
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("reset iter fail", K(ret));
      } else if (OB_FAIL(j_bin.print(j_buf, true))) {
        LOG_WARN("print json fail", K(ret));
      } else {
        LOG_TRACE("result json", K(i), K(j_buf.string()));
      }
      ObString src = datums.at(i).get_string();
      ObString output = row.storage_datums_[1].get_string();
      LOG_TRACE("compare info", K(datums.at(i)), K(row.storage_datums_[1]));
      if (src.compare(output) != 0) ob_abort();
    }
  }
  return ret;
}

int TestSemiStructEncoding::check_freq_column(
  ObIAllocator &allocator,
  const ObString &path_str,
  ObColumnCSDecoderCtx &ctx,
  bool &is_freq_column)
{
  int ret = OB_SUCCESS;
  is_freq_column = false;
  const ObSemiStructColumnDecoderCtx &semistruct_ctx = ctx.semistruct_ctx_;
  ObSemiStructDecodeHandler *handler = semistruct_ctx.handler_;
  ObSemiSchemaAbstract &sub_schema = *handler->sub_schema_;
  uint16_t col_id = -1;
  ObSubColumnPath *sub_column_path = nullptr;
  if (OB_ISNULL(sub_column_path = OB_NEWx(ObSubColumnPath, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory fail", K(ret));
  } else if (OB_FAIL(share::ObSubColumnPath::parse_sub_column_path(path_str, *sub_column_path))) {
    LOG_WARN("parse sub column path fail", K(ret));
  } else if (OB_FAIL(sub_schema.get_column_id(*sub_column_path, col_id))) {
    LOG_WARN("get sub col fail", K(ret), K(col_id));
  } else {
    is_freq_column = sub_schema.is_freq_column(col_id);
  }
  return ret;
}

int TestSemiStructEncoding::decode_semistruct_col(
  ObIAllocator &allocator,
  const ObString &path_str,
  const int64_t row_idx,
  const ObColumnCSDecoderCtx &ctx,
  ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  const ObSemiStructColumnDecoderCtx &semistruct_ctx = ctx.semistruct_ctx_;
  ObSemiStructDecodeHandler *handler = semistruct_ctx.handler_;
  ObSemiStructSubSchema &sub_schema = reinterpret_cast<ObSemiStructSubSchema&>(*handler->sub_schema_);
  bool need_check_null = false;
  datum.reuse();
  if (OB_UNLIKELY(semistruct_ctx.nop_flag_ != ObBaseColumnDecoderCtx::ObNopFlag::HAS_NO_NOP)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support nop encode", K(ret), K(semistruct_ctx.nop_flag_));
  } else if (OB_UNLIKELY(ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED == semistruct_ctx.null_flag_
      || ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF == semistruct_ctx.null_flag_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support null encode", K(ret), K(semistruct_ctx.null_flag_));
  } else if (ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_OR_NOP_BITMAP == semistruct_ctx.null_flag_) {
    need_check_null = true;
  }
  ObSubColumnPath *sub_column_path = nullptr;
  uint16_t col_id = -1;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sub_column_path = OB_NEWx(ObSubColumnPath, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory fail", K(ret));
  } else if (OB_FAIL(share::ObSubColumnPath::parse_sub_column_path(path_str, *sub_column_path))) {
    LOG_WARN("parse sub column path fail", K(ret));
  } else if (OB_FAIL(sub_schema.get_column_id(*sub_column_path, col_id))) {
    LOG_WARN("get col fail", K(ret));
  } else if (col_id < 0 || col_id >= sub_schema.get_store_column_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col id is invalid", K(ret), K(col_id), K(sub_schema.get_store_column_cnt()));
  } else if (need_check_null && ObCSDecodingUtil::test_bit(semistruct_ctx.null_or_nop_bitmap_, row_idx)) {
    datum.set_null();
  } else {
    const ObCSColumnHeader &sub_col_header = semistruct_ctx.sub_col_headers_[col_id];
    ObColumnCSDecoderCtx &sub_col_ctx =  semistruct_ctx.sub_col_ctxs_[col_id];
    const ObIColumnCSDecoder *decoder = semistruct_ctx.sub_col_decoders_[col_id];
    if (OB_ISNULL(decoder)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub column decoder is null", K(ret), K(row_idx), K(col_id), K(sub_col_header));
    } else if (OB_FAIL(decoder->decode(sub_col_ctx, row_idx, datum))) {
      LOG_WARN("decode sub column fail", K(ret), K(row_idx), K(col_id), K(sub_col_header));
    }
  }
  return ret;
}

int TestSemiStructEncoding::check_part_transform(
    ObIAllocator &allocator,
    const int64_t row_cnt,
    const int64_t rowkey_cnt,
    const int64_t col_cnt,
    ObDatumRow &row,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    ObColDatums &datums)
{
  int ret = OB_SUCCESS;
  ObMicroBlockCSDecoder decoder;
  const char *block_buf = micro_block_desc.buf_  - header->header_size_;
  const int64_t block_buf_len = micro_block_desc.buf_size_ + header->header_size_;
  ObMicroBlockData part_transformed_data(block_buf, block_buf_len);
  MockObTableReadInfo read_info;
  common::ObArray<int32_t> storage_cols_index;
  common::ObArray<share::schema::ObColDesc> col_descs;
  for(int store_id = 0; OB_SUCC(ret) && store_id < col_cnt; ++store_id) {
    if (OB_FAIL(storage_cols_index.push_back(store_id))) {
      LOG_WARN("push back fail", K(ret), K(store_id));
    } else if (OB_FAIL(col_descs.push_back(col_descs_.at(store_id)))) {
      LOG_WARN("push back", K(ret), K(store_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_info.init(allocator, col_cnt, rowkey_cnt, false, col_descs, &storage_cols_index))) {
    LOG_WARN("init read info fail", K(ret));
  } else if (OB_FAIL(decoder.init(part_transformed_data, read_info))) {
    LOG_WARN("init decoder fail", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    if (OB_FAIL(decoder.get_row(i, row))) {
      LOG_WARN("decode fail", K(ret), K(i));
    } else {
      LOG_TRACE("result row", K(i), K(row));
    }
    if (OB_FAIL(ret)) {
    } else if (row.storage_datums_[1].is_null()) {
      LOG_TRACE("compare info", K(datums.at(i)), K(row.storage_datums_[1]));
      if (! datums.at(i).is_null()) ob_abort();
    } else {
      const ObLobCommon& lob_common = row.storage_datums_[1].get_lob_data();
      ObString json_data(lob_common.get_byte_size(row.storage_datums_[1].len_), lob_common.get_inrow_data_ptr());
      ObJsonBuffer j_buf(&allocator);
      ObJsonBin j_bin(json_data.ptr(), json_data.length(), &allocator);
      if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("reset iter fail", K(ret));
      } else if (OB_FAIL(j_bin.print(j_buf, true))) {
        LOG_WARN("print json fail", K(ret));
      } else {
        LOG_TRACE("result json", K(i), K(j_buf.string()));
      }
      ObString src = datums.at(i).get_string();
      ObString output = row.storage_datums_[1].get_string();
      LOG_TRACE("compare info", K(datums.at(i)), K(row.storage_datums_[1]));
      if (src.compare(output) != 0) ob_abort();
    }
  }
  return ret;
}

int TestSemiStructEncoding::do_encode(
    ObIAllocator& allocator,
    ObMicroBlockCSEncoder &encoder,
    const int64_t row_cnt,
    ObDatumRow &row,
    ObMicroBlockHeader *&header,
    ObMicroBlockDesc &micro_block_desc,
    ObColDatums& datums)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  if (OB_FAIL(prepare(col_types, rowkey_cnt, col_cnt))) {
    LOG_WARN("prepare fail", K(ret));
  } else {
    ctx_.semistruct_properties_.mode_ = 1;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(encoder.init(ctx_))) {
    LOG_WARN("init encoder fail", K(ret));
  } else if (OB_FAIL(row.init(allocator, col_cnt))) {
    LOG_WARN("init row fail", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    if (datums.at(i).is_null()) {
      row.storage_datums_[1].set_null();
    } else {
      row.storage_datums_[1].set_string(datums.at(i).get_string());
    }
    if (OB_FAIL(encoder.append_row(row))) {
      LOG_WARN("append row fail", K(ret), K(i), K(row));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_micro_block_desc(encoder, micro_block_desc, header))) {
    LOG_WARN("build_micro_block_desc fail", K(ret));
  } else if (encoder.encoders_[1]->get_type() != ObCSColumnHeader::Type::SEMISTRUCT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("encoder is not semistruct encoding", K(ret), K(encoder.encoders_[1]->get_type()));
  }
  return ret;
}

int add_json_datum(ObIAllocator& allocator, ObColDatums& datums, ObIJsonBase *j_bin)
{
  int ret = OB_SUCCESS;
  ObDatum json_datum;
  ObString raw_binary;
  if (OB_FAIL(j_bin->get_raw_binary(raw_binary, &allocator))) {
    LOG_WARN("get_raw_binary fail", K(ret));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary))) {
    LOG_WARN("fill_lob_header fail", K(ret));
  } else if (FALSE_IT(json_datum.set_string(raw_binary))) {
  } else if (OB_FAIL(datums.push_back(json_datum))) {
    LOG_WARN("push back datum fail", K(ret));
  }
  return ret;
}

int build_white_filter(
    ObIAllocator &allocator,
    sql::ObPushdownOperator &pd_operator,
    sql::PushdownFilterInfo &pd_filter_info,
    sql::ObSemiStructWhiteFilterNode &pd_filter_node,
    ObWhiteFilterExecutor *&white_filter,
    ObBitmap *&res_bitmap,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt)
{
  int ret = OB_SUCCESS;
  if (row_cnt < 1 || col_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(col_cnt), K(row_cnt));
  } else {
    // build pd_filter_info
    void *storage_datum_buf = allocator.alloc(sizeof(ObStorageDatum) * col_cnt);
    pd_filter_info.datum_buf_ = new (storage_datum_buf) ObStorageDatum [col_cnt]();
    pd_filter_info.col_capacity_ = col_cnt;
    pd_filter_info.start_ = 0;
    pd_filter_info.count_ = row_cnt;

    // build white_filter_executor
    pd_filter_node.op_type_ = op_type;

    if (OB_ISNULL(white_filter = OB_NEWx(ObSemiStructWhiteFilterExecutor, &allocator, allocator, pd_filter_node, pd_operator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate white filter", KR(ret));
    } else if (OB_FAIL(white_filter->init_bitmap(row_cnt, res_bitmap))) {
      LOG_WARN("fail to init bitmap", KR(ret), K(row_cnt));
    } else if (OB_ISNULL(res_bitmap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result bitmap should not be null", KR(ret), K(row_cnt));
    }
  }
  return ret;
}

int init_filter(
    sql::ObWhiteFilterExecutor &filter,
    const ObIArray<ObObj> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    sql::ObExpr *json_expr,
    ObDatum *datums,
    void *datum_buf,
    const ObObjMeta &col_meta)
{
  int ret = OB_SUCCESS;
  int count = filter_objs.count();
  ObWhiteFilterOperatorType op_type = filter.filter_.get_op_type();
  if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
    count = 1;
  }
  filter.filter_.expr_ = new (expr_buf) ObExpr();
  filter.filter_.expr_->arg_cnt_ = count + 1;
  filter.filter_.expr_->args_ = expr_p_buf;
  if (OB_FAIL(filter.datum_params_.init(count))) {
    LOG_WARN("init filter datum param fail", K(ret), K(count));
  } else {
    filter.filter_.expr_->args_[0] = json_expr;
  }
  for (int64_t i = 1; OB_SUCC(ret) && i <= count; ++i) {
    filter.filter_.expr_->args_[i] = new (expr_buf + 1 + i) ObExpr();
    if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
      filter.filter_.expr_->args_[i]->obj_meta_.set_null();
      filter.filter_.expr_->args_[i]->datum_meta_.type_ = ObNullType;
    } else {
      filter.filter_.expr_->args_[i]->obj_meta_ = filter_objs.at(i - 1).get_meta();
      filter.filter_.expr_->args_[i]->datum_meta_.type_ = filter_objs.at(i - 1).get_meta().get_type();
      datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + (i - 1) * 128;
      datums[i].from_obj(filter_objs.at(i - 1));
      if (OB_FAIL(filter.datum_params_.push_back(datums[i]))) {
        LOG_WARN("push back filter datum", K(ret), K(i));
      } else if (filter.is_null_param(datums[i], filter_objs.at(i - 1).get_meta())) {
        filter.null_param_contained_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    filter.cmp_func_ = get_datum_cmp_func(col_meta, filter.filter_.expr_->args_[0]->obj_meta_);
  }
  return ret;
}

int init_in_filter(
    sql::ObWhiteFilterExecutor &filter,
    const ObIArray<ObObj> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    sql::ObExpr *json_expr,
    ObDatum *datums,
    void *datum_buf,
    const ObObjMeta &col_meta)
{
  int ret = OB_SUCCESS;
  int count = filter_objs.count();
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
  sql::ObExprBasicFuncs *col_basic_funcs = ObDatumFuncs::get_basic_func(
    col_meta.get_type(), col_meta.get_collation_type(), col_meta.get_scale(), false, col_meta.has_lob_header());
  ObDatumCmpFuncType cmp_func = get_datum_cmp_func(col_meta, obj_meta);
  ObDatumCmpFuncType cmp_func_rev = get_datum_cmp_func(obj_meta, col_meta);

  filter.filter_.expr_->args_[0] = json_expr;
  filter.filter_.expr_->args_[0]->basic_funcs_ = col_basic_funcs;

  if (OB_FAIL(filter.datum_params_.init(count))) {
    LOG_WARN("init filter datum fail", K(ret), K(count));
  } else if (OB_FAIL(filter.param_set_.create(count * 2))) {
    LOG_WARN("create param set fail", K(ret), K(count));
  }
  filter.param_set_.set_hash_and_cmp_func(basic_funcs->murmur_hash_v2_, basic_funcs->null_first_cmp_);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    filter.filter_.expr_->args_[1]->args_[i] = new (expr_buf + 3 + i) ObExpr();
    filter.filter_.expr_->args_[1]->args_[i]->obj_meta_ = obj_meta;
    filter.filter_.expr_->args_[1]->args_[i]->datum_meta_.type_ = obj_meta.get_type();
    filter.filter_.expr_->args_[1]->args_[i]->basic_funcs_ = basic_funcs;
    datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
    datums[i].from_obj(filter_objs.at(i));
    if (!filter.is_null_param(datums[i], filter_objs.at(i).get_meta())) {
      if (OB_FAIL(filter.add_to_param_set_and_array(datums[i], filter.filter_.expr_->args_[1]->args_[i]))) {
        LOG_WARN("add_to_param_set_and_array fail", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(filter.datum_params_.begin(), filter.datum_params_.end(),
              [cmp_func] (const ObDatum datum1, const ObDatum datum2) {
                  int cmp_ret = 0;
                  cmp_func(datum1, datum2, cmp_ret);
                  return cmp_ret < 0;
              });
    filter.cmp_func_ = cmp_func;
    filter.param_set_.set_hash_and_cmp_func(col_basic_funcs->murmur_hash_v2_, filter.cmp_func_);
  }
  return ret;
}

int init_json_expr(
    ObIAllocator &allocator,
    sql::ObExpr *&json_expr,
    const ObObjMeta &col_meta)
{
  int ret = OB_SUCCESS;
  const int count = sql::OB_JSON_VALUE_EXPR_PARAM_COUNT + 1;
  sql::ObExpr *expr_buf = reinterpret_cast<sql::ObExpr *>(allocator.alloc(sizeof(sql::ObExpr) * count));
  sql::ObExpr **expr_p_buf = reinterpret_cast<sql::ObExpr **>(allocator.alloc(sizeof(sql::ObExpr*) * sql::OB_JSON_VALUE_EXPR_PARAM_COUNT));
  void *datum_buf = allocator.alloc(sizeof(int8_t) * 128 * count);
  json_expr = expr_buf;
  json_expr->arg_cnt_ = sql::OB_JSON_VALUE_EXPR_PARAM_COUNT;
  json_expr->args_ = expr_p_buf;
  for (int i = 0; i < sql::OB_JSON_VALUE_EXPR_PARAM_COUNT; ++i) {
    json_expr->args_[i] = new (expr_buf + 1 + i) ObExpr();
  }
  json_expr->type_ = T_FUN_SYS_JSON_VALUE;
  json_expr->obj_meta_ = col_meta;
  json_expr->datum_meta_.type_ = col_meta.get_type();
  json_expr->datum_meta_.cs_type_ = col_meta.get_collation_type();

  // ref
  json_expr->args_[0]->type_ = T_REF_COLUMN;
  json_expr->args_[0]->obj_meta_.set_json();
  json_expr->args_[0]->datum_meta_.type_ = ObJsonType;

  // path
  json_expr->args_[1]->obj_meta_.set_varchar();
  json_expr->args_[1]->datum_meta_.type_ = ObVarcharType;

  return ret;
}

int check_white_filter(
    ObIAllocator &allocator,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObObjMeta &col_meta,
    const ObIArray<ObObj> &ref_objs,
    const ObString &path_str,
    ObMicroBlockCSDecoder &decoder,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  sql::ObSemiStructWhiteFilterNode pd_filter_node(allocator);
  sql::PushdownFilterInfo pd_filter_info;
  sql::ObExecContext exec_ctx(allocator);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator);
  sql::ObPushdownOperator pd_operator(eval_ctx, expr_spec);
  ObWhiteFilterExecutor *white_filter = nullptr;
  common::ObBitmap *res_bitmap = nullptr;

  if (row_cnt < 1 || col_cnt < 1 || col_offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cnt), K(col_cnt), K(col_offset), K(ref_objs.count()));
  } else if (OB_FAIL(build_white_filter(allocator, pd_operator, pd_filter_info, pd_filter_node, white_filter, res_bitmap,
    op_type, row_cnt, col_cnt))) {
    LOG_WARN("fail to build white filter", KR(ret));
  } else if (OB_ISNULL(white_filter) || OB_ISNULL(res_bitmap)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ref_objs.count() < 1 && op_type != sql::WHITE_OP_NU && op_type != sql::WHITE_OP_NN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(op_type));
  } else {
    white_filter->col_offsets_.init(1);
    white_filter->col_params_.init(1);
    ObColumnParam col_param(allocator);
    col_param.set_meta_type(col_meta);
    white_filter->col_params_.push_back(&col_param);
    white_filter->col_offsets_.push_back(col_offset);
    white_filter->n_cols_ = 1;

    int count = ref_objs.count();
    ObWhiteFilterOperatorType op_type = pd_filter_node.get_op_type();
    if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
      count = 1;
    }
    int count_expr = (WHITE_OP_IN == op_type ? count + 3 : count + 2);
    int count_expr_p = (WHITE_OP_IN == op_type ? count + 2 : count + 1);
    sql::ObExpr *expr_buf = reinterpret_cast<sql::ObExpr *>(allocator.alloc(sizeof(sql::ObExpr) * count_expr));
    sql::ObExpr **expr_p_buf = reinterpret_cast<sql::ObExpr **>(allocator.alloc(sizeof(sql::ObExpr*) * count_expr_p));
    void *datum_buf = allocator.alloc(sizeof(int8_t) * 128 * count);
    ObStorageDatum datums[count];

    sql::ObExpr *json_expr = nullptr;
    init_json_expr(allocator, json_expr, col_meta);
    share::ObSubColumnPath::parse_sub_column_path(path_str, pd_filter_node.get_sub_col_path());
    if (WHITE_OP_IN == op_type) {
      init_in_filter(*white_filter, ref_objs, expr_buf, expr_p_buf, json_expr, datums, datum_buf, col_meta);
    } else {
      init_filter(*white_filter, ref_objs, expr_buf, expr_p_buf, json_expr, datums, datum_buf, col_meta);
    }

    if (OB_UNLIKELY(2 > white_filter->filter_.expr_->arg_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected filter expr", K(ret), K(white_filter->filter_.expr_->arg_cnt_));
    } else if (OB_FAIL(decoder.filter_pushdown_filter(nullptr, *white_filter, pd_filter_info, *res_bitmap))) {
      LOG_WARN("fail to filter pushdown filter", KR(ret));
    } else if (res_count != res_bitmap->popcnt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("res count is incorrect", K(ret), K(res_count), K(res_bitmap->popcnt()), K(op_type));
    }
  }
  return ret;
}

int TestSemiStructEncoding::check_int_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const int64_t filter_value,
    const int64_t res_count,
    const bool use_tinyint)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  if (use_tinyint) { col_meta.set_tinyint(); }
  else { col_meta.set_int(); }
  ObObj obj;
  if (use_tinyint) { obj.set_tinyint(filter_value); }
  else { obj.set_int(filter_value); }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(obj))) {
    LOG_WARN("push back fail", K(ret), K(obj));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret), K(use_tinyint));
  }
  return ret;
}

int TestSemiStructEncoding::check_int_between_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const int64_t begin_value,
    const int64_t end_value,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_int();
  ObObj begin_obj;
  begin_obj.set_int(begin_value);
  ObObj end_obj;
  end_obj.set_int(end_value);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(begin_obj))) {
    LOG_WARN("push back fail", K(ret), K(begin_obj));
  } else if (OB_FAIL(ref_objs.push_back(end_obj))) {
    LOG_WARN("push back fail", K(ret), K(end_obj));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}

int TestSemiStructEncoding::check_int_in_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const int64_t value1,
    const int64_t value2,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_int();
  ObObj obj1;
  obj1.set_int(value1);
  ObObj obj2;
  obj2.set_int(value2);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(obj1))) {
    LOG_WARN("push back fail", K(ret), K(obj1));
  } else if (OB_FAIL(ref_objs.push_back(obj2))) {
    LOG_WARN("push back fail", K(ret), K(obj2));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}

int TestSemiStructEncoding::check_uint_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const uint64_t filter_value,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_uint64();
  ObObj obj;
  obj.set_uint64(filter_value);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(obj))) {
    LOG_WARN("push back fail", K(ret), K(obj));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}

int TestSemiStructEncoding::check_uint_between_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const uint64_t begin_value,
    const uint64_t end_value,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_uint64();
  ObObj begin_obj;
  begin_obj.set_uint64(begin_value);
  ObObj end_obj;
  end_obj.set_uint64(end_value);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(begin_obj))) {
    LOG_WARN("push back fail", K(ret), K(begin_obj));
  } else if (OB_FAIL(ref_objs.push_back(end_obj))) {
    LOG_WARN("push back fail", K(ret), K(end_obj));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}

int TestSemiStructEncoding::check_uint_in_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const uint64_t value1,
    const uint64_t value2,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_uint64();
  ObObj obj1;
  obj1.set_uint64(value1);
  ObObj obj2;
  obj2.set_uint64(value2);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(obj1))) {
    LOG_WARN("push back fail", K(ret), K(obj1));
  } else if (OB_FAIL(ref_objs.push_back(obj2))) {
    LOG_WARN("push back fail", K(ret), K(obj2));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}
int TestSemiStructEncoding::check_str_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const ObString &filter_value,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_varchar();
  col_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObObj obj;
  obj.set_varchar(filter_value);
  obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(obj))) {
    LOG_WARN("push back fail", K(ret), K(obj));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}

int TestSemiStructEncoding::check_str_between_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const ObString &begin_value,
    const ObString &end_value,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_varchar();
  col_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObObj begin_obj;
  begin_obj.set_varchar(begin_value);
  begin_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObObj end_obj;
  end_obj.set_varchar(end_value);
  begin_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(begin_obj))) {
    LOG_WARN("push back fail", K(ret), K(begin_obj));
  } else if (OB_FAIL(ref_objs.push_back(end_obj))) {
    LOG_WARN("push back fail", K(ret), K(end_obj));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}

int TestSemiStructEncoding::check_str_in_white_filter(
    ObIAllocator &allocator,
    ObMicroBlockHeader *header,
    ObMicroBlockDesc &micro_block_desc,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObString &path_str,
    const ObString &value1,
    const ObString &value2,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder))) {
    LOG_WARN("init_cs_decoder fail", K(ret));
  }
  ObArray<ObObj> ref_objs;
  ObObjMeta col_meta;
  col_meta.set_varchar();
  col_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObObj obj1;
  obj1.set_varchar(value1);
  obj1.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObObj obj2;
  obj2.set_varchar(value2);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ref_objs.push_back(obj1))) {
    LOG_WARN("push back fail", K(ret), K(obj1));
  } else if (OB_FAIL(ref_objs.push_back(obj2))) {
    LOG_WARN("push back fail", K(ret), K(obj2));
  } else if (OB_FAIL(check_white_filter(allocator, op_type, row_cnt, col_cnt, col_offset, col_meta, ref_objs, path_str, decoder, res_count))) {
    LOG_WARN("check_white_filter fail", K(ret));
  }
  return ret;
}

static int build_json_datum(ObIAllocator& allocator, const ObString& j_text, ObDatum& json_datum)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_bin = nullptr;
  ObString raw_binary;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin))) {
    LOG_WARN("get_json_base fail", K(ret), K(j_text));
  } else if (OB_FAIL(j_bin->get_raw_binary(raw_binary, &allocator))) {
    LOG_WARN("get_raw_binary fail", K(ret), K(j_text), KPC(j_bin));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary))) {
    LOG_WARN("fill_lob_header fail", K(ret),K(j_text), KPC(j_bin), K(raw_binary));
  } else {
    json_datum.set_string(raw_binary);
  }
  return ret;
}

TEST_F(TestSemiStructEncoding, test_bug1)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  const int64_t row_cnt = 9;
  const char* json_texts[row_cnt] = {
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/49699333?\",\"display_login\":\"dependabot\",\"gravatar_id\":\"\",\"id\":49699333,\"login\":\"dependabot[bot]\",\"url\":\"xaau;||api$github$com/users/dependabot[bot]\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/27874591?\",\"display_login\":\"u1i\",\"gravatar_id\":\"\",\"id\":27874591,\"login\":\"u1i\",\"url\":\"xaau;||api$github$com/users/u1i\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/41898282?\",\"display_login\":\"github-actions\",\"gravatar_id\":\"\",\"id\":41898282,\"login\":\"github-actions[bot]\",\"url\":\"xaau;||api$github$com/users/github-actions[bot]\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/58356571?\",\"display_login\":\"Puwya\",\"gravatar_id\":\"\",\"id\":58356571,\"login\":\"Puwya\",\"url\":\"xaau;||api$github$com/users/Puwya\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/49699333?\",\"display_login\":\"dependabot\",\"gravatar_id\":\"\",\"id\":49699333,\"login\":\"dependabot[bot]\",\"url\":\"xaau;||api$github$com/users/dependabot[bot]\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/49699333?\",\"display_login\":\"dependabot\",\"gravatar_id\":\"\",\"id\":49699333,\"login\":\"dependabot[bot]\",\"url\":\"xaau;||api$github$com/users/dependabot[bot]\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/105941316?\",\"display_login\":\"codestar-github-bot-us-west-1\",\"gravatar_id\":\"\",\"id\":105941316,\"login\":\"codestar-github-bot-us-west-1\",\"url\":\"xaau;||api$github$com/users/codestar-github-bot-us-west-1\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/25180681?\",\"display_login\":\"renovate-bot\",\"gravatar_id\":\"\",\"id\":25180681,\"login\":\"renovate-bot\",\"url\":\"xaau;||api$github$com/users/renovate-bot\"}",
    "{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/19896123?\",\"display_login\":\"justindbaur\",\"gravatar_id\":\"\",\"id\":19896123,\"login\":\"justindbaur\",\"url\":\"xaau;||api$github$com/users/justindbaur\"}",
  };

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_bug2)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  const int64_t row_cnt = 5;
  const char* json_texts[row_cnt] = {
    "{\"id\":215968269,\"name\":\"WJShengD/Micro-Expression-Classification-using-Local-Binary-Pattern-from-Five-Intersecting-Planes\",\"url\":\"xaau;||api$github$com/repos/WJShengD/Micro-Expression-Classification-using-Local-Binary-Pattern-from-Five-Intersecting-Planes\"}",
    "{\"id\":36473389,\"name\":\"EVEIPH/EVE-IPH\",\"url\":\"xaau;||api$github$com/repos/EVEIPH/EVE-IPH\"}",
    "{\"id\":252649085,\"name\":\"mdmintz/pytest\",\"url\":\"xaau;||api$github$com/repos/mdmintz/pytest\"}",
    "{\"id\":553937707,\"name\":\"sumonece15/New-ChatBot\",\"url\":\"xaau;||api$github$com/repos/sumonece15/New-ChatBot\"}",
    "{\"id\":460137687,\"name\":\"jvkassi/filament-modal\",\"url\":\"xaau;||api$github$com/repos/jvkassi/filament-modal\"}"
  };

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_bug3)
{ // serizlize does not count header size when add child
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 5;
  ObColDatums datums(allocator);
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    ObString key0("");
    ObJsonDouble key0_value(1560067611.0 + i);
    ASSERT_EQ(OB_SUCCESS, obj.add(key0, &key0_value));

    ObString key1("W");
    ObJsonString key1_value("IPC1GARYY6MSQCVMTBINLGIIB4MJUR2SGEUT5RRTUEOPP63SVY508P6UN9I7", STRLEN("IPC1GARYY6MSQCVMTBINLGIIB4MJUR2SGEUT5RRTUEOPP63SVY508P6UN9I7"));
    ASSERT_EQ(OB_SUCCESS, obj.add(key1, &key1_value));

    ObString key2("G4I");
    ObJsonOFloat key2_value(423736000.0);
    ASSERT_EQ(OB_SUCCESS, obj.add(key2, &key2_value));

    ObString key3("KSW");
    ObJsonString key3_value("1SZBN2J3EHKQ61P213OKEXJRYABJCTZVJ41EB93GX88DJM2CKGXFTNN91C117XG66XZOBYZYARW6BNZN59AFC", STRLEN("1SZBN2J3EHKQ61P213OKEXJRYABJCTZVJ41EB93GX88DJM2CKGXFTNN91C117XG66XZOBYZYARW6BNZN59AFC"));
    ASSERT_EQ(OB_SUCCESS, obj.add(key3, &key3_value));

    ObString key4("0T2L");
    ObJsonUint key4_value(1740737970);
    ASSERT_EQ(OB_SUCCESS, obj.add(key4, &key4_value));

    ObString key5("MH2DJ");
    ObJsonBoolean key5_value(true);
    ASSERT_EQ(OB_SUCCESS, obj.add(key5, &key5_value));

    ObString key6("NJAULU");
    ObJsonString key6_value("69S989ZLIHCZMT5IB7YWS4BFS8", STRLEN("69S989ZLIHCZMT5IB7YWS4BFS8"));
    ASSERT_EQ(OB_SUCCESS, obj.add(key6, &key6_value));
    // obj.update_serialize_size();
    add_json_datum(allocator, datums, &obj);
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_complex_situation)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 5;
  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 8;
  ObObjType col_types[col_cnt] = {ObIntType, ObIntType, ObVarcharType, ObJsonType, ObJsonType, ObJsonType, ObTinyIntType, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  { // 0
    row.storage_datums_[0].set_int(0);
    row.storage_datums_[1].set_int(25061821923);
    row.storage_datums_[2].set_string(ObString("CreateEvent"));
    {
      ObString j_text("{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/49699333?\",\"display_login\":\"dependabot\",\"gravatar_id\":\"\",\"id\":49699333,\"login\":\"dependabot[bot]\",\"url\":\"xaau;||api$github$com/users/dependabot[bot]\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[3].set_string(raw_binary);
    }
    {
      ObString j_text("{\"id\":240446072,\"name\":\"AdamariMosqueda/P05.Mosqueda-Espinoza-Adamari-Antonia\",\"url\":\"xaau;||api$github$com/repos/AdamariMosqueda/P05.Mosqueda-Espinoza-Adamari-Antonia\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[4].set_string(raw_binary);
    }
    {
      ObString j_text("{\"master_branch\":\"master\",\"pusher_type\":\"user\",\"ref\":\"dependabot/npm_and_yarn/minimatch-and-ionic/v1-toolkit-and-gulp-3.0.4\",\"ref_type\":\"branch\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[5].set_string(raw_binary);
    }
    row.storage_datums_[6].set_int(1);
    row.storage_datums_[7].set_string(ObString("2022-11-07 11:00:00"));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  { // 1
    row.storage_datums_[0].set_int(1);
    row.storage_datums_[1].set_int(25061821927);
    row.storage_datums_[2].set_string(ObString("PushEvent"));
    {
      ObString j_text("{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/40018936?\",\"display_login\":\"ramachandrasai7\",\"gravatar_id\":\"\",\"id\":40018936,\"login\":\"ramachandrasai7\",\"url\":\"xaau;||api$github$com/users/ramachandrasai7\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[3].set_string(raw_binary);
    }
    {
      ObString j_text("{\"id\":561944721,\"name\":\"disha4u/CSE564-Assignment3\",\"url\":\"xaau;||api$github$com/repos/disha4u/CSE564-Assignment3\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[4].set_string(raw_binary);
    }
    {
      ObString j_text("{\"before\":\"e1d861513d3c35b801fc4d97db86fc3246683e01\",\"commits\":[{\"sha\":\"2d9fbe9df4f6312004e77859b4aa0efbb8e5a454\",\"author\":{\"email\":\"40018936+ramachandrasai7#users.noreply.github.com\",\"name\":\"ramachandrasai7\"},\"message\":\"Dec Obs Single\",\"distinct\":true,\"url\":\"xaau;||api$github$com/repos/disha4u/CSE564-Assignment3/commits/2d9fbe9df4f6312004e77859b4aa0efbb8e5a454\"}],\"distinct_size\":1,\"head\":\"2d9fbe9df4f6312004e77859b4aa0efbb8e5a454\",\"push_id\":11572649905,\"ref\":\"refs/heads/main\",\"size\":1}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[5].set_string(raw_binary);
    }
    row.storage_datums_[6].set_int(1);
    row.storage_datums_[7].set_string(ObString("2022-11-07 11:00:00"));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  {
    row.storage_datums_[0].set_int(2);
    row.storage_datums_[1].set_int(25061821932);
    row.storage_datums_[2].set_string(ObString("IssuesEvent"));
    {
      ObString j_text("{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/1753262?\",\"display_login\":\"mo9a7i\",\"gravatar_id\":\"\",\"id\":1753262,\"login\":\"mo9a7i\",\"url\":\"xaau;||api$github$com/users/mo9a7i\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[3].set_string(raw_binary);
    }
    {
      ObString j_text("{\"id\":481452780,\"name\":\"mo9a7i/time_now\",\"url\":\"xaau;||api$github$com/repos/mo9a7i/time_now\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[4].set_string(raw_binary);
    }
    {
      ObString j_text("{\"action\":\"opened\",\"issue\":{\"author_association\":\"OWNER\",\"body\":\"Please check if the time in `time_now.txt` file is synchronized with world clocks 1667790000384 and if there are any other issues in the repo.\",\"comments\":0,\"comments_url\":\"xaau;||api$github$com/repos/mo9a7i/time_now/issues/12475/comments\",\"created_at\":\"2022-11-07T03:00:00Z\",\"events_url\":\"xaau;||api$github$com/repos/mo9a7i/time_now/issues/12475/events\",\"html_url\":\"xaau;||github.com/mo9a7i/time_now/issues/12475\",\"id\":1437688974,\"labels_url\":\"xaau;||api$github$com/repos/mo9a7i/time_now/issues/12475/labels{/name}\",\"locked\":0,\"node_id\":\"I_kwDOHLJi7M5VsWSO\",\"number\":12475,\"reactions\":{\"+1\":0,\"-1\":0,\"confused\":0,\"eyes\":0,\"heart\":0,\"hooray\":0,\"laugh\":0,\"rocket\":0,\"total_count\":0,\"url\":\"xaau;||api$github$com/repos/mo9a7i/time_now/issues/12475/reactions\"},\"repository_url\":\"xaau;||api$github$com/repos/mo9a7i/time_now\",\"state\":\"open\",\"timeline_url\":\"xaau;||api$github$com/repos/mo9a7i/time_now/issues/12475/timeline\",\"title\":\"Check if time is accruate - 1667790000384\",\"updated_at\":\"2022-11-07T03:00:00Z\",\"url\":\"xaau;||api$github$com/repos/mo9a7i/time_now/issues/12475\",\"user\":{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/1753262?v=4\",\"events_url\":\"xaau;||api$github$com/users/mo9a7i/events{/privacy}\",\"followers_url\":\"xaau;||api$github$com/users/mo9a7i/followers\",\"following_url\":\"xaau;||api$github$com/users/mo9a7i/following{/other_user}\",\"gists_url\":\"xaau;||api$github$com/users/mo9a7i/gists{/gist_id}\",\"gravatar_id\":\"\",\"html_url\":\"xaau;||github.com/mo9a7i\",\"id\":1753262,\"login\":\"mo9a7i\",\"node_id\":\"MDQ6VXNlcjE3NTMyNjI=\",\"organizations_url\":\"xaau;||api$github$com/users/mo9a7i/orgs\",\"received_events_url\":\"xaau;||api$github$com/users/mo9a7i/received_events\",\"repos_url\":\"xaau;||api$github$com/users/mo9a7i/repos\",\"site_admin\":0,\"starred_url\":\"xaau;||api$github$com/users/mo9a7i/starred{/owner}{/repo}\",\"subscriptions_url\":\"xaau;||api$github$com/users/mo9a7i/subscriptions\",\"type\":\"User\",\"url\":\"xaau;||api$github$com/users/mo9a7i\"}}}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[5].set_string(raw_binary);
    }
    row.storage_datums_[6].set_int(1);
    row.storage_datums_[7].set_string(ObString("2022-11-07 11:00:01"));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  {
    row.storage_datums_[0].set_int(3);
    row.storage_datums_[1].set_int(25061821961);
    row.storage_datums_[2].set_string(ObString("CreateEvent"));
    {
      ObString j_text("{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/31103872?\",\"display_login\":\"amulyadutta\",\"gravatar_id\":\"\",\"id\":31103872,\"login\":\"amulyadutta\",\"url\":\"xaau;||api$github$com/users/amulyadutta\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[3].set_string(raw_binary);
    }
    {
      ObString j_text("{\"id\":562683951,\"name\":\"amulyadutta/Presenter\",\"url\":\"xaau;||api$github$com/repos/amulyadutta/Presenter\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[4].set_string(raw_binary);
    }
    {
      ObString j_text("{\"master_branch\":\"master\",\"pusher_type\":\"user\",\"ref\":\"master\",\"ref_type\":\"branch\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[5].set_string(raw_binary);
    }
    row.storage_datums_[6].set_int(1);
    row.storage_datums_[7].set_string(ObString("2022-11-07 11:00:01"));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  {
    row.storage_datums_[0].set_int(4);
    row.storage_datums_[1].set_int(25061821969);
    row.storage_datums_[2].set_string(ObString("PushEvent"));
    {
      ObString j_text("{\"avatar_url\":\"xaau;||avatars$githubusercontent$com/u/20792079?\",\"display_login\":\"WwZzz\",\"gravatar_id\":\"\",\"id\":20792079,\"login\":\"WwZzz\",\"url\":\"xaau;||api$github$com/users/WwZzz\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[3].set_string(raw_binary);
    }
    {
      ObString j_text("{\"id\":365980796,\"name\":\"WwZzz/easyFL\",\"url\":\"xaau;||api$github$com/repos/WwZzz/easyFL\"}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[4].set_string(raw_binary);
    }
    {
      ObString j_text("{\"before\":\"393a38ccdf6a897b0d21a5004ae66d01f35a8c96\",\"commits\":[{\"sha\":\"a324955f6b50612043240f8cbdd73ed96894c55b\",\"author\":{\"email\":\"zwang#stu.xmu.edu.cn\",\"name\":\"WwZzz\"},\"message\":\"Update README.md\",\"distinct\":true,\"url\":\"xaau;||api$github$com/repos/WwZzz/easyFL/commits/a324955f6b50612043240f8cbdd73ed96894c55b\"}],\"distinct_size\":1,\"head\":\"a324955f6b50612043240f8cbdd73ed96894c55b\",\"push_id\":11572649921,\"ref\":\"refs/heads/main\",\"size\":1}");
      ObIJsonBase *j_bin = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[5].set_string(raw_binary);
    }
    row.storage_datums_[6].set_int(1);
    row.storage_datums_[7].set_string(ObString("2022-11-07 11:00:01"));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
  ASSERT_EQ(encoder.encoders_[3]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_EQ(encoder.encoders_[4]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  // ASSERT_EQ(encoder.encoders_[5]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

  { // full transform
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
    for (int32_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, row));
      LOG_TRACE("result row", K(i), K(row));
      for (int j = 3; j < 6; ++j) {
        int src_idx = i * 3 + j - 3;
        if (row.storage_datums_[j].is_null()) {
          LOG_TRACE("compare info", K(i), K(j), K(src_idx), K(datums.at(src_idx)), K(row.storage_datums_[j]));
          ASSERT_TRUE(datums.at(src_idx).is_null());
        } else {
          const ObLobCommon& lob_common = row.storage_datums_[j].get_lob_data();
          ObString json_data(lob_common.get_byte_size(row.storage_datums_[j].len_), lob_common.get_inrow_data_ptr());
          ObJsonBuffer j_buf(&allocator);
          ObJsonBin j_bin(json_data.ptr(), json_data.length(), &allocator);
          ASSERT_EQ(OB_SUCCESS, j_bin.reset_iter());
          ASSERT_EQ(OB_SUCCESS, j_bin.print(j_buf, true));
          LOG_TRACE("result json", K(i), K(j), K(src_idx), K(j_buf.string()));
          ObString src = datums.at(src_idx).get_string();
          ObString output = row.storage_datums_[j].get_string();
          LOG_TRACE("compare info", K(i), K(j), K(src_idx), K(datums.at(src_idx)), K(row.storage_datums_[j]));
          ASSERT_EQ(0, src.compare(output)) << "i: " << i << " j: " << j << " src_idx: " << src_idx << " src : " << src << " output :" << output;
        }
      }
    }
  }

  { // part transform
    ObMicroBlockCSDecoder decoder;
    const char *block_buf = micro_block_desc.buf_  - header->header_size_;
    const int64_t block_buf_len = micro_block_desc.buf_size_ + header->header_size_;
    ObMicroBlockData part_transformed_data(block_buf, block_buf_len);
    MockObTableReadInfo read_info;
    common::ObArray<int32_t> storage_cols_index;
    common::ObArray<share::schema::ObColDesc> col_descs;
    for(int store_id = 0; store_id < col_cnt; ++store_id) {
      ASSERT_EQ(OB_SUCCESS, storage_cols_index.push_back(store_id));
      ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_descs_.at(store_id)));
    }
    ASSERT_EQ(OB_SUCCESS, read_info.init(allocator, col_cnt, rowkey_cnt, false, col_descs, &storage_cols_index));
    ASSERT_EQ(OB_SUCCESS, decoder.init(part_transformed_data, read_info));
    for (int32_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, row));
      LOG_TRACE("result row", K(i), K(row));
      for (int j = 3; j < 6; ++j) {
        int src_idx = i * 3 + j - 3;
        if (row.storage_datums_[j].is_null()) {
          LOG_TRACE("compare info", K(i), K(j), K(src_idx), K(datums.at(src_idx)), K(row.storage_datums_[j]));
          ASSERT_TRUE(datums.at(src_idx).is_null());
        } else {
          const ObLobCommon& lob_common = row.storage_datums_[j].get_lob_data();
          ObString json_data(lob_common.get_byte_size(row.storage_datums_[j].len_), lob_common.get_inrow_data_ptr());
          ObJsonBuffer j_buf(&allocator);
          ObJsonBin j_bin(json_data.ptr(), json_data.length(), &allocator);
          ASSERT_EQ(OB_SUCCESS, j_bin.reset_iter());
          ASSERT_EQ(OB_SUCCESS, j_bin.print(j_buf, true));
          LOG_TRACE("result json", K(i), K(j), K(src_idx), K(j_buf.string()));
          ObString src = datums.at(src_idx).get_string();
          ObString output = row.storage_datums_[j].get_string();
          LOG_TRACE("compare info", K(i), K(j), K(src_idx), K(datums.at(src_idx)), K(row.storage_datums_[j]));
          ASSERT_EQ(0, src.compare(output)) << "i: " << i << " j: " << j << " src_idx: " << src_idx << " src : " << src << " output :" << output;
        }
      }
    }
  }
}

TEST_F(TestSemiStructEncoding, test_flat_json)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  const char *fix_strings[20] = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh",
                    "iiii", "jjjj", "kkkk", "llll", "mmmm", "nnnn", "oooo", "pppp", "qqqq", "rrrr", "ssss", "s"};
  {
    ObString j_text("{\"name\": \"Mike\", \"null\": null, \"age\" : 30, \"like\": [1, 2, 3], \"score\": 89.5, \"int_dict\": 51, \"str_dict\": \"json\", \"bool_type\": true, \"int_dicu\": \"\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": \"Jackson\", \"null\": null, \"age\" : 26, \"like\": [1, 10, 8], \"score\": 79.5, \"int_dict\": 53, \"str_dict\": \"gis\", \"bool_type\": true, \"int_dicu\": \"\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    // ObDatum json_datum;
    // json_datum.set_null();
    // ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    ObString j_text("{\"name\": \"Jackson\", \"null\": null, \"age\" : 26, \"like\": [1, 10, 8], \"score\": 79.5, \"int_dict\": 53, \"str_dict\": \"gis\", \"bool_type\": true, \"int_dicu\": \"\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    for (int i = 3; i < row_cnt - 3; ++i) {
      ObJsonObject obj(&allocator);

      ObString name_key("name");
      ObJsonString name_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(name_key, &name_value));

      ObString age_key("age");
      ObJsonInt age_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(age_key, &age_value));

      ObString score_key("score");
      ObJsonDouble score_value(i + 66.6);
      ASSERT_EQ(OB_SUCCESS, obj.add(score_key, &score_value));

      ObString int_dict_key("int_dict");
      ObJsonInt int_dict_value(i%4 + 50);
      ASSERT_EQ(OB_SUCCESS, obj.add(int_dict_key, &int_dict_value));

      ObString str_dict_key("str_dict");
      ObJsonString str_dict_value("Fake", STRLEN("Fake"));
      ASSERT_EQ(OB_SUCCESS, obj.add(str_dict_key, &str_dict_value));

      ObString bool_type_key("bool_type");
      ObJsonBoolean bool_type_value(i%2 == 0);
      ASSERT_EQ(OB_SUCCESS, obj.add(bool_type_key, &bool_type_value));

      ObString null_key("null");
      ObJsonNull null_value;
      ASSERT_EQ(OB_SUCCESS, obj.add(null_key, &null_value));

      ObString fix_str_key("int_dicu");
      ObJsonString fix_str_value("", STRLEN(""));
      ASSERT_EQ(OB_SUCCESS, obj.add(fix_str_key, &fix_str_value));

      ObString likes_key("like");
      ObJsonArray likes(&allocator);
      ObJsonInt j_int1(1+i);
      ObJsonInt j_int2(2+i);
      ObJsonInt j_int3(3+i);
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int1));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int2));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int3));
      ASSERT_EQ(OB_SUCCESS, obj.add(likes_key, &likes));

      ObIJsonBase *j_bin = &obj;
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    }
  }
  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : 126, \"null\": null, \"like\": [122, null, 8], \"score\": 11.5, \"int_dict\": 52, \"str_dict\": \"xml\", \"bool_type\": true, \"int_dicu\": \"\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": null, \"age\" : 96, \"like\": [132, 2, 88], \"null\": null, \"score\": 12.5, \"int_dict\": 50, \"str_dict\": \"struct\", \"bool_type\": true, \"int_dicu\": \"\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": \"null\", \"age\" : 6, \"like\": [null, 66, 108], \"score\": null, \"null\": \"null\", \"int_dict\": 1152, \"str_dict\": \"array\", \"bool_type\": true, \"int_dicu\": \"s\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_spare)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  {
    ObString j_text("{\"name\": \"Mike\", \"like\": [1, 2, 3], \"score\": 89.5, \"int_dict\": 52, \"str_dict\": \"xml\", \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : 26, \"like\": [1, \"10\", 8], \"int_dict\": 53, \"str_dict\": \"gis\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObDatum json_datum;
    json_datum.set_null();
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    for (int i = 3; i < row_cnt - 3; ++i) {
      ObJsonObject obj(&allocator);

      ObString name_key("name");
      ObJsonString name_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(name_key, &name_value));

      ObString age_key("age");
      ObJsonInt age_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(age_key, &age_value));

      ObString score_key("score");
      ObJsonDouble score_value(i + 66.6);
      ASSERT_EQ(OB_SUCCESS, obj.add(score_key, &score_value));

      ObString int_dict_key("int_dict");
      ObJsonInt int_dict_value(i%4 + 50);
      ASSERT_EQ(OB_SUCCESS, obj.add(int_dict_key, &int_dict_value));

      ObString str_dict_key("str_dict");
      ObJsonString str_dict_value("Fake", STRLEN("Fake"));
      ASSERT_EQ(OB_SUCCESS, obj.add(str_dict_key, &str_dict_value));

      ObString bool_type_key("bool_type");
      ObJsonBoolean bool_type_value(i%2 == 0);
      ASSERT_EQ(OB_SUCCESS, obj.add(bool_type_key, &bool_type_value));

      ObString likes_key("like");
      ObJsonArray likes(&allocator);
      ObJsonInt j_int1(1+i);
      ObJsonInt j_int2(2+i);
      ObJsonInt j_int3(3+i);
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int1));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int2));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int3));
      ASSERT_EQ(OB_SUCCESS, obj.add(likes_key, &likes));

      ObIJsonBase *j_bin = &obj;
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    }
  }

  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : null, \"like\": [122, null, 8], \"score\": 11.5, \"int_dict\": 52, \"str_dict\": \"xml\", \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": null, \"age\" : 96, \"like\": [132, 2, 88], \"score\": 12.5, \"int_dict\": 50, \"str_dict\": \"struct\", \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": \"null\", \"age\" : 6, \"like\": [null, 66, 108], \"score\": null, \"int_dict\": 1152, \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  ASSERT_EQ(row_cnt, datums.count());

  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_without_null_row)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  {
    ObString j_text("{\"name\": \"Mike\", \"like\": [1, 2, 3], \"score\": 89.5, \"int_dict\": 52, \"str_dict\": \"xml\", \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : 26, \"like\": [1, \"10\", 8], \"int_dict\": 53, \"str_dict\": \"gis\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : 26, \"like\": [1, \"10\", 8], \"int_dict\": 53, \"str_dict\": \"gis\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    for (int i = 3; i < row_cnt - 3; ++i) {
      ObJsonObject obj(&allocator);

      ObString name_key("name");
      ObJsonString name_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(name_key, &name_value));

      ObString age_key("age");
      ObJsonInt age_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(age_key, &age_value));

      ObString score_key("score");
      ObJsonDouble score_value(i + 66.6);
      ASSERT_EQ(OB_SUCCESS, obj.add(score_key, &score_value));

      ObString int_dict_key("int_dict");
      ObJsonInt int_dict_value(i%4 + 50);
      ASSERT_EQ(OB_SUCCESS, obj.add(int_dict_key, &int_dict_value));

      ObString str_dict_key("str_dict");
      ObJsonString str_dict_value("Fake", STRLEN("Fake"));
      ASSERT_EQ(OB_SUCCESS, obj.add(str_dict_key, &str_dict_value));

      ObString bool_type_key("bool_type");
      ObJsonBoolean bool_type_value(i%2 == 0);
      ASSERT_EQ(OB_SUCCESS, obj.add(bool_type_key, &bool_type_value));

      ObString likes_key("like");
      ObJsonArray likes(&allocator);
      ObJsonInt j_int1(1+i);
      ObJsonInt j_int2(2+i);
      ObJsonInt j_int3(3+i);
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int1));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int2));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int3));
      ASSERT_EQ(OB_SUCCESS, obj.add(likes_key, &likes));

      ObIJsonBase *j_bin = &obj;
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    }
  }

  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : 126, \"like\": [122, null, 8], \"score\": 11.5, \"int_dict\": 52, \"str_dict\": \"xml\", \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": null, \"age\" : 96, \"like\": [132, 2, 88], \"score\": 12.5, \"int_dict\": 50, \"str_dict\": \"struct\", \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": null, \"age\" : 6, \"like\": [null, 66, 108], \"score\": null, \"int_dict\": 1152, \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_spare_null_row)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  {
    ObString j_text("{\"name\": \"Mike\", \"age\" : 26, \"score\": 89.5, \"int_dict\": 52, \"str_dict\": \"xml\", \"bool_type\": true}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : 126, \"score\": 69.5, \"like\": [1, \"10\", 8], \"int_dict\": 53, \"str_dict\": \"gis\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    for (int i = 2; i < row_cnt - 1; ++i) {
      ObJsonObject obj(&allocator);

      ObString name_key("name");
      ObJsonString name_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(name_key, &name_value));

      ObString age_key("age");
      ObJsonInt age_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(age_key, &age_value));

      ObString score_key("score");
      ObJsonDouble score_value(i + 66.6);
      ASSERT_EQ(OB_SUCCESS, obj.add(score_key, &score_value));

      ObIJsonBase *j_bin = &obj;
      ObDatum json_datum;
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      json_datum.set_string(raw_binary);
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    }
  }
  {
    ObString j_text("{\"name\": \"Jackson\", \"age\" : 26, \"score\": 99.1, \"like\": [1, \"10\", 8], \"int_dict\": 53, \"str_dict\": \"gis\"}");
    ObIJsonBase *j_bin = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
    ObDatum json_datum;
    ObString raw_binary;
    ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_binary, &allocator));
    ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
    json_datum.set_string(raw_binary);
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_full_json_type)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  {
    for (int i = 0; i < row_cnt; ++i) {
      ObJsonObject obj(&allocator);

      // J_NULL
      ObString null_key("null");
      ObJsonNull null_value;
      ASSERT_EQ(OB_SUCCESS, obj.add(null_key, &null_value));

      // J_DECIMAL
      ObScale scale = 2;
      ObPrecision prec = 10;
      char buf[128] = {0};
      number::ObNumber num;
      sprintf(buf, "%f", i + 1.333333);
      ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
      ObJsonDecimal j_decimal(num);
      ObString decimal_key("decimal");
      ASSERT_EQ(OB_SUCCESS, obj.add(decimal_key, &j_decimal));

      // J_INT
      ObString int_key("int");
      ObJsonInt int_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(int_key, &int_value));

      // J_UINT
      ObString uint_key("uint");
      ObJsonUint uint_value(i + 123);
      ASSERT_EQ(OB_SUCCESS, obj.add(uint_key, &uint_value));

      // J_DOUBLE
      ObString double_key("double");
      ObJsonDouble double_value(i + 66.6);
      ASSERT_EQ(OB_SUCCESS, obj.add(double_key, &double_value));

      // J_STRING
      ObString string_key("string");
      ObJsonString string_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(string_key, &string_value));


      // J_BOOLEAN
      ObString bool_type_key("bool");
      ObJsonBoolean bool_type_value(i%2 == 0);
      ASSERT_EQ(OB_SUCCESS, obj.add(bool_type_key, &bool_type_value));

      // J_DATE
      ObTime t_date;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540 + i, t_date));
      ObString date_key("date");
      ObJsonDatetime date_value(ObJsonNodeType::J_DATE, t_date);
      ASSERT_EQ(OB_SUCCESS, obj.add(date_key, &date_value));

      // J_TIME
      ObTime t_time;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509 * static_cast<int64_t>(USECS_PER_SEC) + i * 60, t_time));
      ObString time_key("time");
      ObJsonDatetime time_value(ObJsonNodeType::J_TIME, t_time);
      ASSERT_EQ(OB_SUCCESS, obj.add(time_key, &time_value));

      // J_DATETIME
      ObTime t_datetime;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC + i * 100, NULL, t_datetime));
      ObString datetime_key("datetime");
      ObJsonDatetime datetime_value(ObJsonNodeType::J_DATETIME, t_datetime);
      ASSERT_EQ(OB_SUCCESS, obj.add(datetime_key, &datetime_value));

      // J_MYSQL_DATE
      ObTime t_mysql_date;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::mdate_to_ob_time(43509 * static_cast<int64_t>(USECS_PER_SEC) + i * 60, t_mysql_date));
      ObString mysql_date_key("mysql_date");
      ObJsonDatetime mysql_date_value(ObJsonNodeType::J_TIME, t_mysql_date);
      ASSERT_EQ(OB_SUCCESS, obj.add(time_key, &time_value));

      // J_MYSQL_DATETIME
      ObTime t_mysql_datetime;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::mdatetime_to_ob_time(1429089727 * USECS_PER_SEC + i * 100, t_mysql_datetime));
      ObString mysql_datetime_key("mysql_datetime");
      ObJsonDatetime mysql_datetime_value(ObJsonNodeType::J_DATETIME, t_mysql_datetime);
      ASSERT_EQ(OB_SUCCESS, obj.add(datetime_key, &datetime_value));

      // J_TIMESTAMP
      ObTime t_timestamp;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::usec_to_ob_time(ObTimeUtility::current_time() + i, t_timestamp));
      ObJsonDatetime timestamp_value(ObJsonNodeType::J_TIMESTAMP, t_timestamp);
      ObString timestamp_key("timestamp");
      ASSERT_EQ(OB_SUCCESS, obj.add(timestamp_key, &timestamp_value));

      // J_OPAQUE
      ObString opaque_str("10101011010101010110101010101");
      ObJsonOpaque opaque_value(opaque_str, ObBitType);
      ObString opaque_key("opaque");
      ASSERT_EQ(OB_SUCCESS, obj.add(opaque_key, &opaque_value));

      add_json_datum(allocator, datums, &obj);
    }
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

  {
    ObArray<ObObj> ref_objs;
    ObObjMeta col_meta;
    ObString string_value("Noel");
    col_meta.set_varchar();
    col_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObObj obj;
    obj.set_varchar(string_value);
    obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObString path_str("$.string");
    ASSERT_EQ(OB_SUCCESS, ref_objs.push_back(obj));
    ASSERT_EQ(OB_SUCCESS, check_white_filter(allocator, ObWhiteFilterOperatorType::WHITE_OP_EQ, row_cnt, col_cnt, 1, col_meta, ref_objs, path_str, decoder, 1));
  }

}

// 测试小于4行的场景 ObCSEncodingUtil::ENCODING_ROW_COUNT_THRESHOLD
TEST_F(TestSemiStructEncoding, test_ENCODING_ROW_COUNT_THRESHOLD)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 3;
  ObColDatums datums(allocator);
  const char *names[row_cnt] = {"Mike", "Jackson", "Liam"};
  {
    for (int i = 0; i < row_cnt; ++i) {
      ObJsonObject obj(&allocator);

      // J_NULL
      ObString null_key("null");
      ObJsonNull null_value;
      ASSERT_EQ(OB_SUCCESS, obj.add(null_key, &null_value));

      // J_DECIMAL
      ObScale scale = 2;
      ObPrecision prec = 10;
      char buf[128] = {0};
      number::ObNumber num;
      sprintf(buf, "%f", i + 1.333333);
      ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
      ObJsonDecimal j_decimal(num);
      ObString decimal_key("decimal");
      ASSERT_EQ(OB_SUCCESS, obj.add(decimal_key, &j_decimal));

      // J_INT
      ObString int_key("intxxxxxxxxxxxxxxx");
      ObJsonInt int_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(int_key, &int_value));

      // J_UINT
      ObString uint_key("uintxxxxxxxxxxxx");
      ObJsonUint uint_value(i + 123);
      ASSERT_EQ(OB_SUCCESS, obj.add(uint_key, &uint_value));

      // J_DOUBLE
      ObString double_key("doublexxxxxxxxx");
      ObJsonDouble double_value(i + 66.6);
      ASSERT_EQ(OB_SUCCESS, obj.add(double_key, &double_value));

      // J_STRING
      ObString string_key("string");
      ObJsonString string_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(string_key, &string_value));

      // J_BOOLEAN
      ObString bool_type_key("bool");
      ObJsonBoolean bool_type_value(i%2 == 0);
      ASSERT_EQ(OB_SUCCESS, obj.add(bool_type_key, &bool_type_value));

      // J_DATE
      ObTime t_date;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540 + i, t_date));
      ObString date_key("date");
      ObJsonDatetime date_value(ObJsonNodeType::J_DATE, t_date);
      ASSERT_EQ(OB_SUCCESS, obj.add(date_key, &date_value));

      // J_TIME
      ObTime t_time;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509 * static_cast<int64_t>(USECS_PER_SEC) + i * 60, t_time));
      ObString time_key("time");
      ObJsonDatetime time_value(ObJsonNodeType::J_TIME, t_time);
      ASSERT_EQ(OB_SUCCESS, obj.add(time_key, &time_value));

      // J_DATETIME
      ObTime t_datetime;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC + i * 100, NULL, t_datetime));
      ObString datetime_key("datetime");
      ObJsonDatetime datetime_value(ObJsonNodeType::J_DATETIME, t_datetime);
      ASSERT_EQ(OB_SUCCESS, obj.add(datetime_key, &datetime_value));

      // J_TIMESTAMP
      ObTime t_timestamp;
      ASSERT_EQ(OB_SUCCESS, ObTimeConverter::usec_to_ob_time(ObTimeUtility::current_time() + i, t_timestamp));
      ObJsonDatetime timestamp_value(ObJsonNodeType::J_TIMESTAMP, t_timestamp);
      ObString timestamp_key("timestamp");
      ASSERT_EQ(OB_SUCCESS, obj.add(timestamp_key, &timestamp_value));

      add_json_datum(allocator, datums, &obj);
    }
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_string_to_uint)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 2000;
  ObColDatums datums(allocator);
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    obj.set_use_lexicographical_order();

    // int8_t
    ObString i8_key("int8");
    int8_t i8 = -i;
    ObJsonString i8_value(reinterpret_cast<char*>(&i8), sizeof(int8_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i8_key, &i8_value));

    // int16_t
    ObString i16_key("int16");
    int16_t i16 = -(rand() + 10000);
    ObJsonString i16_value(reinterpret_cast<char*>(&i16), sizeof(int16_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i16_key, &i16_value));

    // int32_t
    ObString i32_key("int32");
    int32_t i32 = -(rand() + 1000000);
    ObJsonString i32_value(reinterpret_cast<char*>(&i32), sizeof(int32_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i32_key, &i32_value));

    // int64_t
    ObString i64_key("int64");
    int64_t i64 = INT64_MIN + rand();
    ObJsonString i64_value(reinterpret_cast<char*>(&i64), sizeof(int64_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i64_key, &i64_value));

    // uint8_t
    ObString u8_key("uint8");
    uint8_t u8 = rand();
    ObJsonString u8_value(reinterpret_cast<char*>(&u8), sizeof(uint8_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u8_key, &u8_value));

    // uint16_t
    ObString u16_key("uint16");
    uint16_t u16 = rand() + 10000;
    ObJsonString u16_value(reinterpret_cast<char*>(&u16), sizeof(uint16_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u16_key, &u16_value));

    // uint32_t
    ObString u32_key("uint32");
    uint32_t u32 = rand() + 1000000;
    ObJsonString u32_value(reinterpret_cast<char*>(&u32), sizeof(uint32_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u32_key, &u32_value));

    // uint64_t
    ObString u64_key("uint64");
    uint64_t u64 = UINT64_MAX - i;
    ObJsonString u64_value(reinterpret_cast<char*>(&u64), sizeof(uint64_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u64_key, &u64_value));

    // bool
    ObString bool_key("bool");
    bool b = rand()%2 == 0;
    ObJsonString bool_value(reinterpret_cast<char*>(&b), sizeof(bool));
    ASSERT_EQ(OB_SUCCESS, obj.add(bool_key, &bool_value));

    // double
    ObString double_key("double");
    double d = rand() + 666666.6;
    ObJsonString double_value(reinterpret_cast<char*>(&d), sizeof(double));
    ASSERT_EQ(OB_SUCCESS, obj.add(double_key, &double_value));

    // float
    ObString float_key("float");
    float f = rand() + 66.6;
    ObJsonString float_value(reinterpret_cast<char*>(&f), sizeof(float));
    ASSERT_EQ(OB_SUCCESS, obj.add(float_key, &float_value));

    add_json_datum(allocator, datums, &obj);
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_lexicographical_order)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 2000;
  ObColDatums datums(allocator);
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    obj.set_use_lexicographical_order();

    // int8_t
    ObString i8_key("int8");
    int8_t i8 = -i;
    ObJsonString i8_value(reinterpret_cast<char*>(&i8), sizeof(int8_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i8_key, &i8_value));

    // int16_t
    ObString i16_key("int16");
    int16_t i16 = -(rand() + 10000);
    ObJsonString i16_value(reinterpret_cast<char*>(&i16), sizeof(int16_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i16_key, &i16_value));

    // int32_t
    ObString i32_key("int32");
    int32_t i32 = -(rand() + 1000000);
    ObJsonString i32_value(reinterpret_cast<char*>(&i32), sizeof(int32_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i32_key, &i32_value));

    // int64_t
    ObString i64_key("int64");
    int64_t i64 = INT64_MIN + rand();
    ObJsonString i64_value(reinterpret_cast<char*>(&i64), sizeof(int64_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(i64_key, &i64_value));

    // uint8_t
    ObString u8_key("uint8");
    uint8_t u8 = rand();
    ObJsonString u8_value(reinterpret_cast<char*>(&u8), sizeof(uint8_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u8_key, &u8_value));

    // uint16_t
    ObString u16_key("uint16");
    uint16_t u16 = rand() + 10000;
    ObJsonString u16_value(reinterpret_cast<char*>(&u16), sizeof(uint16_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u16_key, &u16_value));

    // uint32_t
    ObString u32_key("uint32");
    uint32_t u32 = rand() + 1000000;
    ObJsonString u32_value(reinterpret_cast<char*>(&u32), sizeof(uint32_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u32_key, &u32_value));

    // uint64_t
    ObString u64_key("uint64");
    uint64_t u64 = UINT64_MAX - i;
    ObJsonString u64_value(reinterpret_cast<char*>(&u64), sizeof(uint64_t));
    ASSERT_EQ(OB_SUCCESS, obj.add(u64_key, &u64_value));

    // bool
    ObString bool_key("bool");
    bool b = rand()%2 == 0;
    ObJsonString bool_value(reinterpret_cast<char*>(&b), sizeof(bool));
    ASSERT_EQ(OB_SUCCESS, obj.add(bool_key, &bool_value));

    // double
    ObString double_key("double");
    double d = rand() + 666666.6;
    ObJsonString double_value(reinterpret_cast<char*>(&d), sizeof(double));
    ASSERT_EQ(OB_SUCCESS, obj.add(double_key, &double_value));

    // float
    ObString float_key("float");
    float f = rand() + 66.6;
    ObJsonString float_value(reinterpret_cast<char*>(&f), sizeof(float));
    ASSERT_EQ(OB_SUCCESS, obj.add(float_key, &float_value));

    add_json_datum(allocator, datums, &obj);
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_int_sub_column)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 200;
  ObColDatums datums(allocator);
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    ADD_INT_JSON_NODE(obj, int8_key, "int8", int8_value, (rand() % UINT8_MAX - INT8_MAX))
    ADD_INT_JSON_NODE(obj, int16_key, "int16", int16_value, (rand() % UINT16_MAX - INT16_MAX))
    ADD_INT_JSON_NODE(obj, int32_key, "int32", int32_value, (rand() % UINT32_MAX - INT32_MAX))
    ADD_INT_JSON_NODE(obj, int64_key, "int64", int64_value, (rand() % UINT64_MAX - INT64_MAX))
    add_json_datum(allocator, datums, &obj);
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_double_sub_column)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 200;
  ObColDatums datums(allocator);
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    ADD_DOUBLE_JSON_NODE(obj, double_key, "double", double_value, (rand()%100000/100000000.0 * (i%2 == 0 ? 1 : -1)))
    ADD_FLOAT_JSON_NODE(obj, float_key, "float", float_value, (rand()%100000/100000000.0 * (i%2 == 0 ? 1 : -1)))
    add_json_datum(allocator, datums, &obj);
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_int_filter)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 2000;
  ObColDatums datums(allocator);
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i == row_cnt/2) {
      ++null_cnt;
      ADD_NULL_JSON_NODE(obj, int_key, "int", int_value)
      ADD_NULL_JSON_NODE(obj, uint_key, "uint", uint_value)
      add_json_datum(allocator, datums, &obj);
    } else {
      ADD_INT_JSON_NODE(obj, int_key, "int", int_value, (i * (i%2 == 0 ? 1 : -1)))
      ADD_UINT_JSON_NODE(obj, uint_key, "uint", uint_value, i)
      add_json_datum(allocator, datums, &obj);
    }
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  {
    ObString path_str("$.int");
    int64_t int_filter_value = 0;
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_EQ, row_cnt, col_cnt, 1, path_str, int_filter_value, 1));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 + 1));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LT, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GT, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 - 1 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - 1 - null_cnt));

    ASSERT_EQ(OB_SUCCESS, check_int_between_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_BT, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2, row_cnt/2/2));
    ASSERT_EQ(OB_SUCCESS, check_int_in_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_IN, row_cnt, col_cnt, 1, path_str, int_filter_value, 100, 2));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, int_filter_value, null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - null_cnt));
  }
  {
    ObString path_str("$.uint");
    uint64_t int_filter_value = row_cnt/2 + 1;
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_EQ, row_cnt, col_cnt, 1, path_str, int_filter_value, 1));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 + 1));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LT, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 - 1));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GT, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 - 1 - 1));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - 1 - null_cnt));

    ASSERT_EQ(OB_SUCCESS, check_uint_between_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_BT, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 + 10, 10));
    ASSERT_EQ(OB_SUCCESS, check_uint_in_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_IN, row_cnt, col_cnt, 1, path_str, int_filter_value, 100, 2));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, int_filter_value, null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - null_cnt));
  }
}

TEST_F(TestSemiStructEncoding, test_int_dict_filter)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 2000;
  ObColDatums datums(allocator);
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i == row_cnt/2) {
      ++null_cnt;
      ADD_NULL_JSON_NODE(obj, id_key, "id", id_value)
      ADD_NULL_JSON_NODE(obj, int_key, "int", int_value)
      ADD_NULL_JSON_NODE(obj, uint_key, "uint", uint_value)
      add_json_datum(allocator, datums, &obj);
    } else {
      ADD_INT_JSON_NODE(obj, id_key, "id", id_value, i)
      ADD_INT_JSON_NODE(obj, int_key, "int", int_value, ((i % 10 + 100) * (i%2 == 0 ? 1 : -1)))
      ADD_UINT_JSON_NODE(obj, uint_key, "uint", uint_value, (i % 10 + 100))
      add_json_datum(allocator, datums, &obj);
    }
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  {
    ObString path_str("$.int");
    int64_t int_filter_value = -105;
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_EQ, row_cnt, col_cnt, 1, path_str, int_filter_value, 200));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LE, row_cnt, col_cnt, 1, path_str, int_filter_value, 3 * 200));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LT, row_cnt, col_cnt, 1, path_str, int_filter_value, 2 * 200));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GE, row_cnt, col_cnt, 1, path_str, int_filter_value, 8 * 200 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GT, row_cnt, col_cnt, 1, path_str, int_filter_value, 7 * 200 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - 200 - null_cnt));

    ASSERT_EQ(OB_SUCCESS, check_int_between_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_BT, row_cnt, col_cnt, 1, path_str, int_filter_value, 107, (4 + 3) * 200 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_in_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_IN, row_cnt, col_cnt, 1, path_str, int_filter_value, 100, 2 * 200 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, int_filter_value, null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - null_cnt));
  }
  {
    ObString path_str("$.uint");
    uint64_t int_filter_value = 105;
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_EQ, row_cnt, col_cnt, 1, path_str, int_filter_value, 200));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LE, row_cnt, col_cnt, 1, path_str, int_filter_value, 6 * 200 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LT, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 - null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt/2 ));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GT, row_cnt, col_cnt, 1, path_str, int_filter_value, 4 * 200));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NE, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - 200 - null_cnt));

    ASSERT_EQ(OB_SUCCESS, check_uint_between_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_BT, row_cnt, col_cnt, 1, path_str, int_filter_value, 107, 3 * 200));
    ASSERT_EQ(OB_SUCCESS, check_uint_in_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_IN, row_cnt, col_cnt, 1, path_str, int_filter_value, 101, 2 * 200));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, int_filter_value, null_cnt));
    ASSERT_EQ(OB_SUCCESS, check_uint_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, int_filter_value, row_cnt - null_cnt));
  }
}

TEST_F(TestSemiStructEncoding, test_str_filter)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i == row_cnt/2) {
      ++null_cnt;
      ADD_NULL_JSON_NODE(obj, string_key, "string", string_value)
      add_json_datum(allocator, datums, &obj);
    } else {
      ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i], STRLEN(names[i]))
      add_json_datum(allocator, datums, &obj);
    }
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  ObString path_str("$.string");
  ObString str_filter_value(STRLEN("Gordon"), "Gordon");
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_EQ, row_cnt, col_cnt, 1, path_str, str_filter_value, 1));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LE, row_cnt, col_cnt, 1, path_str, str_filter_value, 6));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LT, row_cnt, col_cnt, 1, path_str, str_filter_value, 5));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GE, row_cnt, col_cnt, 1, path_str, str_filter_value, 14));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GT, row_cnt, col_cnt, 1, path_str, str_filter_value, 13));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NE, row_cnt, col_cnt, 1, path_str, str_filter_value, row_cnt - 1 - null_cnt));

  ObString end_filter_value(STRLEN("Les"), "Les");
  ASSERT_EQ(OB_SUCCESS, check_str_between_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_BT, row_cnt, col_cnt, 1, path_str, str_filter_value, end_filter_value, row_cnt/2/2));
  ASSERT_EQ(OB_SUCCESS, check_str_in_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_IN, row_cnt, col_cnt, 1, path_str, str_filter_value, end_filter_value, 2));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, str_filter_value, null_cnt));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, str_filter_value, row_cnt - null_cnt));
}

TEST_F(TestSemiStructEncoding, test_str_dict_filter)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 2000;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i == row_cnt/2) {
      ++null_cnt;
      ADD_NULL_JSON_NODE(obj, string_key, "string", string_value)
      add_json_datum(allocator, datums, &obj);
    } else {
      ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
      add_json_datum(allocator, datums, &obj);
    }
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  ObString path_str("$.string");
  ObString str_filter_value(STRLEN("Gordon"), "Gordon");
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_EQ, row_cnt, col_cnt, 1, path_str, str_filter_value, 100));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LE, row_cnt, col_cnt, 1, path_str, str_filter_value, 700));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_LT, row_cnt, col_cnt, 1, path_str, str_filter_value, 600));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GE, row_cnt, col_cnt, 1, path_str, str_filter_value, 1399));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_GT, row_cnt, col_cnt, 1, path_str, str_filter_value, 1299));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NE, row_cnt, col_cnt, 1, path_str, str_filter_value, row_cnt - 100 - null_cnt));

  ObString end_filter_value(STRLEN("Les"), "Les");
  ASSERT_EQ(OB_SUCCESS, check_str_between_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_BT, row_cnt, col_cnt, 1, path_str, str_filter_value, end_filter_value, 500));
  ASSERT_EQ(OB_SUCCESS, check_str_in_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_IN, row_cnt, col_cnt, 1, path_str, str_filter_value, end_filter_value, 200));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, str_filter_value, null_cnt));
  ASSERT_EQ(OB_SUCCESS, check_str_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, str_filter_value, row_cnt - null_cnt));
}

TEST_F(TestSemiStructEncoding, test_nop_bitmap)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  int64_t row_cnt = 21;
  ObColDatums datums(allocator);
  uint64_t semistruct_threshold = 90;
  const char *names[21] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo", "dsad"};
  {
    ObDatum json_datum;
    json_datum.set_null();
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  {
    for (int i = 1; i < row_cnt - 1; ++i) {
      ObJsonObject obj(&allocator);

      ObString name_key("name");
      ObJsonString name_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(name_key, &name_value));

      ObString age_key("age");
      ObJsonInt age_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(age_key, &age_value));
      ObString score_key("score");
      ObJsonDouble score_value(i + 66.6);
      if (i <= row_cnt - 3) {
        ASSERT_EQ(OB_SUCCESS, obj.add(score_key, &score_value));
      }
      ObString spare_key("spare");
      char buf[128] = {0};
      number::ObNumber num;
      sprintf(buf, "%f", 1.333333);
      ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
      ObJsonDecimal decimal_value(num, 10, 2);
      if (i <= row_cnt - 4) {
        ASSERT_EQ(OB_SUCCESS, obj.add(spare_key, &decimal_value));
      }
      add_json_datum(allocator, datums, &obj);
    }
    ObJsonObject obj(&allocator);

    ObString name_key("name");
    ObJsonNull j_null;
    ASSERT_EQ(OB_SUCCESS, obj.add(name_key, &j_null));

    ObString score_key("score");
    ASSERT_EQ(OB_SUCCESS, obj.add(score_key, &j_null));

    add_json_datum(allocator, datums, &obj);
  }

  ASSERT_EQ(row_cnt, datums.count());
  {
    ObMicroBlockCSEncoder encoder;
    ctx_.semistruct_properties_.freq_threshold_ = semistruct_threshold;
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
    bool is_freq_column = false;
    ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.name"), *decoder.decoders_[1].ctx_, is_freq_column));
    ASSERT_TRUE(is_freq_column);
    ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.age"), *decoder.decoders_[1].ctx_, is_freq_column));
    ASSERT_TRUE(is_freq_column);
    ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.score"), *decoder.decoders_[1].ctx_, is_freq_column));
    ASSERT_TRUE(is_freq_column);
    ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.spare"), *decoder.decoders_[1].ctx_, is_freq_column));
    ASSERT_FALSE(is_freq_column);
    ObStorageDatum datum;
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.name"), 0, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_null());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.age"), 0, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_null());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.score"), 0, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_null());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.spare"), 0, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_null());
    for (int i = 1; i <= row_cnt - 3; i++) {
      ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.name"), i, *decoder.decoders_[1].ctx_, datum));
      ASSERT_FALSE(datum.is_nop());
      ASSERT_FALSE(datum.is_null());
      ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.age"), i, *decoder.decoders_[1].ctx_, datum));
      ASSERT_FALSE(datum.is_nop());
      ASSERT_FALSE(datum.is_null());
      ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.score"), i, *decoder.decoders_[1].ctx_, datum));
      ASSERT_FALSE(datum.is_nop());
      ASSERT_FALSE(datum.is_null());
      ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.spare"), i, *decoder.decoders_[1].ctx_, datum));
      if (i == row_cnt - 3) {
        ASSERT_TRUE(datum.is_null());
      } else {
        ASSERT_FALSE(datum.is_nop());
        ASSERT_FALSE(datum.is_null());
      }
    }
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.name"), row_cnt - 2, *decoder.decoders_[1].ctx_, datum));
    ASSERT_FALSE(datum.is_nop());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.age"), row_cnt - 2, *decoder.decoders_[1].ctx_, datum));
    ASSERT_FALSE(datum.is_nop());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.score"), row_cnt - 2, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_nop());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.spare"), row_cnt - 2, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_null());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.name"), row_cnt - 1, *decoder.decoders_[1].ctx_, datum));
    ASSERT_FALSE(datum.is_nop());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.age"), row_cnt - 1, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_nop());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.score"), row_cnt - 1, *decoder.decoders_[1].ctx_, datum));
    ASSERT_FALSE(datum.is_nop());
    ASSERT_EQ(OB_SUCCESS, decode_semistruct_col(allocator, ObString("$.spare"), row_cnt - 1, *decoder.decoders_[1].ctx_, datum));
    ASSERT_TRUE(datum.is_null());
  }
  reuse();
  datums.reuse();

  row_cnt = 20;
  {
    for (int i = 0; i < row_cnt; ++i) {
      ObJsonObject obj(&allocator);

      ObString name_key("name");
      ObJsonString name_value(names[i], STRLEN(names[i]));
      ASSERT_EQ(OB_SUCCESS, obj.add(name_key, &name_value));

      ObString age_key("age");
      ObJsonInt age_value(i + 20);
      if (i < row_cnt - 2) {
        ASSERT_EQ(OB_SUCCESS, obj.add(age_key, &age_value));
      }
      ASSERT_EQ(OB_SUCCESS, obj.add(age_key, &age_value));
      ObString score_key("score");
      ObJsonDouble score_value(i + 66.6);
      if (i < row_cnt - 3) {
        ASSERT_EQ(OB_SUCCESS, obj.add(score_key, &score_value));
      }
      add_json_datum(allocator, datums, &obj);
    }
  }

  ASSERT_EQ(row_cnt, datums.count());
  {
    ObMicroBlockCSEncoder encoder;
    ctx_.semistruct_properties_.freq_threshold_ = semistruct_threshold;
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
    bool is_freq_column = false;
    ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.name"), *decoder.decoders_[1].ctx_, is_freq_column));
    ASSERT_TRUE(is_freq_column);
    ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.age"), *decoder.decoders_[1].ctx_, is_freq_column));
    ASSERT_TRUE(is_freq_column);
    ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.score"), *decoder.decoders_[1].ctx_, is_freq_column));
    ASSERT_FALSE(is_freq_column);
  }

  {
    ObDatumRow row;
    ObMicroBlockCSEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
    int col_cnt = 2;
    ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
    for (int i = 0; i < row_cnt; ++i) {
      row.storage_datums_[0].set_int(i + 1);
      row.storage_datums_[1].set_string(datums.at(i).get_string());
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
    }
    ctx_.semistruct_properties_.freq_threshold_ = semistruct_threshold;
    ObMicroBlockHeader *header = nullptr;
    ObMicroBlockDesc micro_block_desc;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
    ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

    ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
    ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, 1, col_cnt, row, header, micro_block_desc, datums));
  }
}

TEST_F(TestSemiStructEncoding, test_all_null_sub_col)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 2000;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    ADD_NULL_JSON_NODE(obj, int_null_key, "null", int_null_value)
    ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
    add_json_datum(allocator, datums, &obj);
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
  ObString path_str("$.null");
  ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, 0, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, 0, 0, true));
}

TEST_F(TestSemiStructEncoding, test_int_null_replace)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i % 5 == 0) {
      ADD_NULL_JSON_NODE(obj, int_null_key, "int_null", int_null_value)
      ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
      add_json_datum(allocator, datums, &obj);
    } else {
      ADD_INT_JSON_NODE(obj, int_null_key, "int_null", int_null_value, rand())
      ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
      add_json_datum(allocator, datums, &obj);
    }
  }
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));

  ObSemiStructColumnEncodeCtx *semistruct_ctx = encoder.encoders_[1]->ctx_->semistruct_ctx_;
  int sub_col_id = 1;
  ASSERT_EQ(2, semistruct_ctx->get_store_column_count());
  ASSERT_EQ(true, reinterpret_cast<ObIntegerColumnEncoder *>(semistruct_ctx->sub_encoders_.at(sub_col_id))->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
  ObString path_str("$.int_null");
  ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NU, row_cnt, col_cnt, 1, path_str, 0, 4));
  ASSERT_EQ(OB_SUCCESS, check_int_white_filter(allocator, header, micro_block_desc, ObWhiteFilterOperatorType::WHITE_OP_NN, row_cnt, col_cnt, 1, path_str, 0, 16));
}

int build_json_object_datum(ObIAllocator &allocator, const std::vector<std::string> &key_names, ObJsonNodeType* sub_col_types, const int sub_col_cnt, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObJsonObject obj(&allocator);
  ObIJsonBase *j_base = &obj;
  ObString raw_binary;
  for (int i = 0; OB_SUCC(ret) && i < sub_col_cnt; ++i) {
    ObJsonNodeType type = sub_col_types[i];
    ObString key(key_names[i].length(), key_names[i].c_str());
    ObJsonNode *j_node = nullptr;
    if (rand()%sub_col_cnt == 0) type = ObJsonNodeType::J_NULL;
    if (ObJsonNodeType::J_NULL == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonNull, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonNull));
      } else  if (OB_FAIL(obj.add(key, j_node))) {
        LOG_WARN("add child fail", K(ret), K(i), K(key));
      }
    } else if (ObJsonNodeType::J_INT == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonInt, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonInt));
      } else  if (OB_FAIL(obj.add(key, j_node))) {
        LOG_WARN("add child fail", K(ret), K(i), K(key));
      }
    } else if (ObJsonNodeType::J_UINT == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonUint, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonUint));
      } else  if (OB_FAIL(obj.add(key, j_node))) {
        LOG_WARN("add child fail", K(ret), K(i), K(key));
      }
    } else if (ObJsonNodeType::J_STRING == type) {
      std::string str = str_rand(rand() % 100);
      ObString value(str.length(), str.c_str());
      if (OB_FAIL(ob_write_string(allocator, value, value))) {
        LOG_WARN("copy string fail", K(ret), K(value));
      } else if (OB_ISNULL(j_node = OB_NEWx(ObJsonString, &allocator, value))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonString));
      } else  if (OB_FAIL(obj.add(key, j_node))) {
        LOG_WARN("add child fail", K(ret), K(i), K(key));
      }
    } else if (ObJsonNodeType::J_DOUBLE == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonDouble, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonDouble));
      } else  if (OB_FAIL(obj.add(key, j_node))) {
        LOG_WARN("add child fail", K(ret), K(i), K(key));
      }
    } else if (ObJsonNodeType::J_OFLOAT == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonOFloat, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonOFloat));
      } else  if (OB_FAIL(obj.add(key, j_node))) {
        LOG_WARN("add child fail", K(ret), K(i), K(key));
      }
    } else if (ObJsonNodeType::J_BOOLEAN == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonBoolean, &allocator, (rand()%2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonBoolean));
      } else  if (OB_FAIL(obj.add(key, j_node))) {
        LOG_WARN("add child fail", K(ret), K(i), K(key));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support type", K(ret), K(i), K(type));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(j_base->get_raw_binary(raw_binary, &allocator))) {
    LOG_WARN("get_raw_binary fail", K(ret), K(obj));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary))) {
    LOG_WARN("fill_lob_header fail", K(ret));
  } else {
    datum.set_string(raw_binary);
  }
  return ret;
}

int generate_datums(ObIAllocator &allocator, const std::vector<std::string> &key_names, ObJsonNodeType* sub_col_types, const int sub_col_cnt, const int row_cnt, ObColDatums &datums)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    ObDatum datum;
    if (OB_FAIL(build_json_object_datum(allocator, key_names, sub_col_types, sub_col_cnt, datum))) {
      LOG_WARN("build_json_datum fail", K(ret), K(i));
    } else if (OB_FAIL(datums.push_back(datum))) {
      LOG_WARN("push back datum fail", K(ret), K(i));
    }
  }
  return ret;
}

int append_row_to_encoder(ObMicroBlockCSEncoder &encoder, ObDatumRow &row, ObColDatums &datums)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < datums.count(); ++i) {
    row.storage_datums_[0].set_int(i + 1);
    if (datums.at(i).is_null()) {
      row.storage_datums_[1].set_null();
    } else {
      row.storage_datums_[1].set_string(datums.at(i).get_string());
    }
    if (OB_FAIL(encoder.append_row(row))) {
      LOG_WARN("append_row fail", K(ret), K(i), K(row));
    }
  }
  return ret;
}

TEST_F(TestSemiStructEncoding, test_hete_column_adpter)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  ctx_.semistruct_properties_.freq_threshold_ = 90;

  const int64_t row_cnt = 10;
  const char* json_texts[row_cnt] = {
    "{\"id\":215968269, \"str_k\":{}}",
    "{\"id\":36473389, \"str_k\":[]}",
    "{\"id\":252649085, \"str_k\":{\"hete_obj_k\": \"obj_v1\"}}",
    "{\"id\":553937707, \"str_k\":[123, 456]}",
    "{\"id\":460137687, \"str_k\":null}",
    "{\"id\":461137687, \"str_k\":23}",
    "{\"id\":462137687, \"str_k\":6.2}",
    "{\"id\":463237687, \"str_k\":{\"hete_obj_k\": \"obj_v2\"}}",
    "{\"str_k\":{\"hete_obj_k2\": \"obj_v3\"}}",
    "{\"id\":460437687}"
  };
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
  bool is_freq_column = false;
  ASSERT_EQ(OB_SEARCH_NOT_FOUND, check_freq_column(allocator, ObString("$.str_k"), *decoder.decoders_[1].ctx_, is_freq_column));
  is_freq_column = false;
  ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.id"), *decoder.decoders_[1].ctx_, is_freq_column));
  ASSERT_TRUE(is_freq_column);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
  ctx_.semistruct_properties_.freq_threshold_ = 40;
  const char* json_texts1[row_cnt] = {
    "{\"id\":215968269, \"str_k\":{}}",
    "{\"id\":36473389, \"str_k\":[]}",
    "{\"id\":252649085, \"str_k\":{\"hete_obj_k\": \"obj_v1\"}}",
    "{\"id\":553937707, \"str_k\":[123, 456]}",
    "{\"id\":460137687, \"str_k\":null}",
    "{\"id\":461137687, \"str_k\":23}",
    "{\"id\":462137687, \"str_k\":{\"hete_obj_k\": \"obj_v2\"}}",
    "{\"id\":463237687, \"str_k\":{\"hete_obj_k\": \"obj_v3\"}}",
    "{\"str_k\":{\"hete_obj_k\": \"obj_v4\"}}",
    "{\"id\":460437687}"
  };
  encoder.reset();
  row.reset();
  micro_block_desc.reset();
  decoder.reset();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts1[i]), json_texts1[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
  is_freq_column = false;
  ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.str_k.hete_obj_k"), *decoder.decoders_[1].ctx_, is_freq_column));
  ASSERT_TRUE(is_freq_column);
  is_freq_column = false;
  ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.str_k[0]"), *decoder.decoders_[1].ctx_, is_freq_column));
  ASSERT_FALSE(is_freq_column);
  ASSERT_EQ(OB_SUCCESS, check_freq_column(allocator, ObString("$.id"), *decoder.decoders_[1].ctx_, is_freq_column));
  ASSERT_TRUE(is_freq_column);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}


TEST_F(TestSemiStructEncoding, test_all_empty_obj)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  const int64_t row_cnt = 10;
  const char* json_texts[row_cnt] = {
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}",
    "{}"
  };

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::STR_DICT);

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
  reuse();
}

TEST_F(TestSemiStructEncoding, test_sub_schema_null_reuse)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  const int64_t row_cnt = 10;
  const char* json_texts[row_cnt] = {
    "{\"id\":215968269, \"int_k\":null, \"str_k\":null}",
    "{\"id\":36473389, \"int_k\":null, \"str_k\":null}",
    "{\"id\":252649085, \"int_k\":null, \"str_k\":null}",
    "{\"id\":553937707, \"int_k\":null}",
    "{\"id\":460137687, \"int_k\":null, \"str_k\":null}",
    "{\"id\":461137687, \"int_k\":null, \"str_k\":null}",
    "{\"id\":462137687, \"str_k\":null}",
    "{\"id\":463237687, \"int_k\":null, \"str_k\":null}",
    "{\"id\":464337687, \"int_k\":null, \"str_k\":null}",
    "{\"id\":460437687, \"int_k\":null, \"str_k\":null}"
  };

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <2> reuse
  encoder.reuse();
  datums.reuse();
  ObSemiStructColumnEncodeCtx *semistruct_col_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  {
    const char* json_texts2[row_cnt] = {
      "{\"id\":215968269, \"int_k\":1, \"str_k\":null}",
      "{\"id\":36473389, \"int_k\":null, \"str_k\":\"4\"}",
      "{\"id\":252649085, \"int_k\":null, \"str_k\":null}",
      "{\"id\":553937707, \"int_k\":null, \"str_k\":\"3\"}",
      "{\"id\":460137687, \"int_k\":2, \"str_k\":null}",
      "{\"id\":460137687, \"int_k\":null, \"str_k\":null}",
      "{\"id\":460137687, \"int_k\":20, \"str_k\":null}",
      "{\"id\":460137687, \"int_k\":21, \"str_k\":null}",
      "{\"id\":460137687, \"int_k\":22, \"str_k\":null}",
      "{\"id\":460137687, \"int_k\":23}"
    };

    for (int i = 0; i < row_cnt; ++i) {
      row.storage_datums_[0].set_int(i + 1);
      ObString j_text(STRLEN(json_texts2[i]), json_texts2[i]);
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_string(json_datum.get_string());
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
    }
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
    ASSERT_NE(nullptr, encoder.encoders_[1]->ctx_->semistruct_ctx_);
    ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
    ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
  }
  reuse();
}

TEST_F(TestSemiStructEncoding, test_sub_schema_reuse)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObColDatums datums(allocator);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  ObIColumnCSEncoder *e = nullptr;
  ObSemiStructColumnEncodeCtx *semistruct_col_ctx = nullptr;
  ObMicroBlockHeader *header = nullptr;
  ObMicroBlockDesc micro_block_desc;

  int sub_col_count = 5;
  ObJsonNodeType sub_col_types[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  std::vector<std::string> sub_col_names;
  for (int i = 0; i < 10; ++i) {
    int len = rand()%10;
    sub_col_names.push_back(str_rand(len > 0 ? len : i));
  }
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <2> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <3> reuse
  encoder.reuse();
  datums.reuse();
  sub_col_count = 6;
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <4> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <5> reuse
  encoder.reuse();
  datums.reuse();
  sub_col_count = 10;
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <6> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  //<7> reuse, and write all null
  encoder.reuse();
  datums.reuse();
  micro_block_desc.reset();
  // ctx_.column_encodings_[1] = ObCSColumnHeader::Type::SEMISTRUCT;
  for (int i=0; i < row_cnt; ++i) {
    ObDatum json_datum; json_datum.set_null();
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  // ASSERT_EQ(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <8> reuse
  encoder.reuse();
  datums.reuse();
  sub_col_count = 8;
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <9> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  reuse();
}

TEST_F(TestSemiStructEncoding, test_sub_schema_reuse1)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  int64_t row_cnt = 10000;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObColDatums datums(allocator);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  ObIColumnCSEncoder *e = nullptr;
  ObSemiStructColumnEncodeCtx *semistruct_col_ctx = nullptr;
  ObMicroBlockHeader *header = nullptr;
  ObMicroBlockDesc micro_block_desc;

  int sub_col_count = 5;
  ObJsonNodeType sub_col_types[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  std::vector<std::string> sub_col_names;
  std::vector<std::string> copy_sub_col_names;
  copy_sub_col_names.reserve(10);
  for (int i = 0; i < 10; ++i) {
    int len = rand()%10;
    sub_col_names.push_back(str_rand(len > 0 ? len : i));
  }
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <2> reuse
  for (int i = 0; i < 100; i++) {
    // Encode in a loop, thereby triggering the release of micro-block data.
    datums.reuse();
    ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
    encoder.reuse();
    row_cnt = 200;
    ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
    ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
    ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    e = encoder.encoders_[1];
    ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
    ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
    ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
    ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
  }
  reuse();
}

int check_json_datum(ObIAllocator& allocator, const ObDatum& src_datum, const ObDatum& decode)
{
  int ret = OB_SUCCESS;
  if (decode.is_null()) {
    LOG_TRACE("compare info", K(src_datum), K(decode));
    if (! src_datum.is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src is not null", K(src_datum), K(decode));
      ob_abort();
    }
  } else {
    const ObLobCommon& lob_common = decode.get_lob_data();
    ObString src = src_datum.get_string();
    ObString output = decode.get_string();
    LOG_TRACE("compare info", K(src_datum), K(decode));
    if (src.compare(output) != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src and decode is not binary equal",  K(src_datum), K(decode));
      ob_abort();
    }
  }
  return ret;
}

TEST_F(TestSemiStructEncoding, test_zero_stream)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 4;
  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 3;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  { // 0
    row.storage_datums_[0].set_int(0);
    {
      ObString j_text("{\"avatar_url\":\"111111111111111111\",\"display_login\":\"dependabot\",\"gravatar_id\":\"2222222222222222\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_string(json_datum.get_string());
    }
    {
      ObString j_text("{\"id\":240446072,\"name\":\"AdamariMosqueda/P05.Mosqueda-Espinoza-Adamari-Antonia\",\"url\":\"xaau;||api$github$com/repos/AdamariMosqueda/P05.Mosqueda-Espinoza-Adamari-Antonia\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  { // 1
    row.storage_datums_[0].set_int(1);
    {
      ObString j_text("{\"avatar_url\":\"2222222222222222\",\"display_login\":\"dependabot\",\"gravatar_id\":\"33333333333333333\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_string(json_datum.get_string());
    }
    {
      ObString j_text("{\"id\":561944721,\"name\":\"disha4u/CSE564-Assignment3\",\"url\":\"xaau;||api$github$com/repos/disha4u/CSE564-Assignment3\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  {
    row.storage_datums_[0].set_int(2);
    {
      ObString j_text("{\"avatar_url\":\"2222222222222222\",\"display_login\":\"dependabot\",\"gravatar_id\":\"2222222222222222\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_string(json_datum.get_string());
    }
    {
      ObString j_text("{\"id\":481452780,\"name\":\"mo9a7i/time_now\",\"url\":\"xaau;||api$github$com/repos/mo9a7i/time_now\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  {
    row.storage_datums_[0].set_int(3);
    {
      ObString j_text("{\"avatar_url\":\"111111111111111111\",\"display_login\":\"dependabot\",\"gravatar_id\":\"2222222222222222\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_string(json_datum.get_string());
    }
    {
      ObString j_text("{\"id\":562683951,\"name\":\"amulyadutta/Presenter\",\"url\":\"xaau;||api$github$com/repos/amulyadutta/Presenter\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  {
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
    ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
    ASSERT_EQ(encoder.encoders_[2]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
    for (int32_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, row));
      ASSERT_EQ(OB_SUCCESS, check_json_datum(allocator, datums.at(i * 2), row.storage_datums_[1])) << "i: " << i;
      ASSERT_EQ(OB_SUCCESS, check_json_datum(allocator, datums.at(i * 2 + 1), row.storage_datums_[2])) << "i: " << i;
    }
  }
  encoder.reuse();
  datums.reuse();
  { // 0
    row.storage_datums_[0].set_int(0);
    {
      ObDatum json_datum;json_datum.set_null();
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_null();
    }
    {
      ObString j_text("{\"id\":240446072,\"name\":\"AdamariMosqueda/P05.Mosqueda-Espinoza-Adamari-Antonia\",\"url\":\"xaau;||api$github$com/repos/AdamariMosqueda/P05.Mosqueda-Espinoza-Adamari-Antonia\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  { // 1
    row.storage_datums_[0].set_int(1);
    {
      ObDatum json_datum;json_datum.set_null();
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_null();
    }
    {
      ObString j_text("{\"id\":561944721,\"name\":\"disha4u/CSE564-Assignment3\",\"url\":\"xaau;||api$github$com/repos/disha4u/CSE564-Assignment3\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  {
    row.storage_datums_[0].set_int(2);
    {
      ObDatum json_datum;json_datum.set_null();
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_null();
    }
    {
      ObString j_text("{\"id\":481452780,\"name\":\"mo9a7i/time_now\",\"url\":\"xaau;||api$github$com/repos/mo9a7i/time_now\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  {
    row.storage_datums_[0].set_int(3);
    {
      ObDatum json_datum;json_datum.set_null();
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[1].set_null();
    }
    {
      ObString j_text("{\"id\":562683951,\"name\":\"amulyadutta/Presenter\",\"url\":\"xaau;||api$github$com/repos/amulyadutta/Presenter\"}");
      ObDatum json_datum;
      ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
      ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
      row.storage_datums_[2].set_string(json_datum.get_string());
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  {
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
    ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
    ASSERT_EQ(encoder.encoders_[2]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
    { // full transform
      ObMicroBlockData full_transformed_data;
      ObMicroBlockCSDecoder decoder;
      ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
      for (int32_t i = 0; i < row_cnt; ++i) {
        ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, row));
        ASSERT_EQ(OB_SUCCESS, check_json_datum(allocator, datums.at(i * 2), row.storage_datums_[1])) << "i: " << i;
        ASSERT_EQ(OB_SUCCESS, check_json_datum(allocator, datums.at(i * 2 + 1), row.storage_datums_[2])) << "i: " << i;
      }
    }
    { // part transform
      ObMicroBlockCSDecoder decoder;
      const char *block_buf = micro_block_desc.buf_  - header->header_size_;
      const int64_t block_buf_len = micro_block_desc.buf_size_ + header->header_size_;
      ObMicroBlockData part_transformed_data(block_buf, block_buf_len);
      MockObTableReadInfo read_info;
      common::ObArray<int32_t> storage_cols_index;
      common::ObArray<share::schema::ObColDesc> col_descs;
      for(int store_id = 0; store_id < col_cnt; ++store_id) {
        ASSERT_EQ(OB_SUCCESS, storage_cols_index.push_back(store_id));
        ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_descs_.at(store_id)));
      }
      ASSERT_EQ(OB_SUCCESS, read_info.init(allocator, col_cnt, rowkey_cnt, false, col_descs, &storage_cols_index));
      ASSERT_EQ(OB_SUCCESS, decoder.init(part_transformed_data, read_info));
      for (int32_t i = 0; i < row_cnt; ++i) {
        ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, row));
        ASSERT_EQ(OB_SUCCESS, check_json_datum(allocator, datums.at(i * 2), row.storage_datums_[1])) << "i: " << i;
        ASSERT_EQ(OB_SUCCESS, check_json_datum(allocator, datums.at(i * 2 + 1), row.storage_datums_[2])) << "i: " << i;
      }
    }
  }
}

TEST_F(TestSemiStructEncoding, test_heteroid_sub_column)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObColDatums datums(allocator);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  ObIColumnCSEncoder *e = nullptr;
  ObSemiStructColumnEncodeCtx *semistruct_col_ctx = nullptr;
  ObMicroBlockHeader *header = nullptr;
  ObMicroBlockDesc micro_block_desc;

  int sub_col_count = 10;
  ObJsonNodeType sub_col_types1[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  ObJsonNodeType sub_col_types2[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_UINT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  ObJsonNodeType sub_col_types3[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_DOUBLE };
  std::vector<std::string> sub_col_names;
  for (int i = 0; i < 10; ++i) {
    sub_col_names.push_back(str_rand(i));
  }
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types1, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types2, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <2> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types2, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types3, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

int build_json_array_datum(ObIAllocator &allocator, ObJsonNodeType* sub_col_types, const int sub_col_cnt, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObJsonArray obj(&allocator);
  ObIJsonBase *j_base = &obj;
  ObString raw_binary;
  for (int i = 0; OB_SUCC(ret) && i < sub_col_cnt; ++i) {
    ObJsonNodeType type = sub_col_types[i];
    ObJsonNode *j_node = nullptr;
    if (rand()%sub_col_cnt == 0) type = ObJsonNodeType::J_NULL;
    if (ObJsonNodeType::J_NULL == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonNull, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonNull));
      } else  if (OB_FAIL(obj.append(j_node))) {
        LOG_WARN("add child fail", K(ret), K(i));
      }
    } else if (ObJsonNodeType::J_INT == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonInt, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonInt));
      } else  if (OB_FAIL(obj.append(j_node))) {
        LOG_WARN("add child fail", K(ret), K(i));
      }
    } else if (ObJsonNodeType::J_UINT == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonUint, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonUint));
      } else  if (OB_FAIL(obj.append(j_node))) {
        LOG_WARN("add child fail", K(ret), K(i));
      }
    } else if (ObJsonNodeType::J_STRING == type) {
      std::string str = str_rand(rand() % 100);
      ObString value(str.length(), str.c_str());
      if (OB_FAIL(ob_write_string(allocator, value, value))) {
        LOG_WARN("copy string fail", K(ret), K(value));
      } else if (OB_ISNULL(j_node = OB_NEWx(ObJsonString, &allocator, value))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonString));
      } else  if (OB_FAIL(obj.append(j_node))) {
        LOG_WARN("add child fail", K(ret), K(i));
      }
    } else if (ObJsonNodeType::J_DOUBLE == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonDouble, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonDouble));
      } else  if (OB_FAIL(obj.append(j_node))) {
        LOG_WARN("add child fail", K(ret), K(i));
      }
    } else if (ObJsonNodeType::J_OFLOAT == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonOFloat, &allocator, (rand())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonOFloat));
      } else  if (OB_FAIL(obj.append(j_node))) {
        LOG_WARN("add child fail", K(ret), K(i));
      }
    } else if (ObJsonNodeType::J_BOOLEAN == type) {
      if (OB_ISNULL(j_node = OB_NEWx(ObJsonBoolean, &allocator, (rand()%2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonBoolean));
      } else  if (OB_FAIL(obj.append(j_node))) {
        LOG_WARN("add child fail", K(ret), K(i));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support type", K(ret), K(i), K(type));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(j_base->get_raw_binary(raw_binary, &allocator))) {
    LOG_WARN("get_raw_binary fail", K(ret), K(obj));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary))) {
    LOG_WARN("fill_lob_header fail", K(ret));
  } else {
    datum.set_string(raw_binary);
  }
  return ret;
}

int generate_array_datums(ObIAllocator &allocator, ObJsonNodeType* sub_col_types, const int sub_col_cnt, const int row_cnt, ObColDatums &datums)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    ObDatum datum;
    if (OB_FAIL(build_json_array_datum(allocator, sub_col_types, sub_col_cnt, datum))) {
      LOG_WARN("build_json_datum fail", K(ret), K(i));
    } else if (OB_FAIL(datums.push_back(datum))) {
      LOG_WARN("push back datum fail", K(ret), K(i));
    }
  }
  return ret;
}

TEST_F(TestSemiStructEncoding, test_array_sub_schema_reuse)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObColDatums datums(allocator);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  ObIColumnCSEncoder *e = nullptr;
  ObSemiStructColumnEncodeCtx *semistruct_col_ctx = nullptr;
  ObMicroBlockHeader *header = nullptr;
  ObMicroBlockDesc micro_block_desc;

  int sub_col_count = 5;
  ObJsonNodeType sub_col_types[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <2> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <3> reuse
  encoder.reuse();
  datums.reuse();
  sub_col_count = 6;
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <4> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <5> reuse
  encoder.reuse();
  datums.reuse();
  sub_col_count = 10;
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <6> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  //<7> reuse, and write all null
  encoder.reuse();
  datums.reuse();
  micro_block_desc.reset();
  // ctx_.column_encodings_[1] = ObCSColumnHeader::Type::SEMISTRUCT;
  for (int i=0; i < row_cnt; ++i) {
    ObDatum json_datum; json_datum.set_null();
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
  }
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  // ASSERT_EQ(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <8> reuse
  encoder.reuse();
  datums.reuse();
  sub_col_count = 8;
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <9> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  reuse();
}

TEST_F(TestSemiStructEncoding, test_array_heteroid_sub_column)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObColDatums datums(allocator);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  ObIColumnCSEncoder *e = nullptr;
  ObSemiStructColumnEncodeCtx *semistruct_col_ctx = nullptr;
  ObMicroBlockHeader *header = nullptr;
  ObMicroBlockDesc micro_block_desc;

  int sub_col_count = 10;
  ObJsonNodeType sub_col_types1[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  ObJsonNodeType sub_col_types2[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_UINT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  ObJsonNodeType sub_col_types3[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_DOUBLE };
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types1, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types2, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <2> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types2, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, generate_array_datums(allocator, sub_col_types3, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_scalar)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  int64_t row_cnt = 100;
  ObColDatums datums(allocator);
  for (int i = 0; i < row_cnt; ++i) {
    std::string str = str_rand(rand()%100);
    ObJsonString str_node(str.c_str(), str.length());
    add_json_datum(allocator, datums, &str_node);
  }
  ObMicroBlockCSEncoder encoder;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    row.storage_datums_[1].set_string(datums.at(i).get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_NE(nullptr, encoder.encoders_[1]->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

// object and array mix at root
TEST_F(TestSemiStructEncoding, test_mix)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    if (i == 19) {
      ObJsonArray likes(&allocator);
      ObJsonInt j_int1(1);
      ObJsonInt j_int2(2);
      ObJsonInt j_int3(3);
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int1));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int2));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int3));
      add_json_datum(allocator, datums, &likes);
    } else {
      ObJsonObject obj(&allocator);
      ADD_INT_JSON_NODE(obj, int_null_key, "int_null", int_null_value, rand())
      ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
      add_json_datum(allocator, datums, &obj);
    }
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    row.storage_datums_[1].set_string(datums.at(i).get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_NE(nullptr, encoder.encoders_[1]->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

// array and scalar mix
TEST_F(TestSemiStructEncoding, test_mix2)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i == 10) {
      ADD_NULL_JSON_NODE(obj, int_null_key, "int_null", int_null_value)
      ObJsonArray likes(&allocator);
      ObJsonInt j_int1(1);
      ObJsonInt j_int2(2);
      ObJsonInt j_int3(3);
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int1));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int2));
      ASSERT_EQ(OB_SUCCESS, likes.append(&j_int3));
      ObString string_key("string");
      ASSERT_EQ(OB_SUCCESS, obj.add(string_key, &likes));
      add_json_datum(allocator, datums, &obj);
    } else {
      ADD_INT_JSON_NODE(obj, int_null_key, "int_null", int_null_value, rand())
      ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
      add_json_datum(allocator, datums, &obj);
    }
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    row.storage_datums_[1].set_string(datums.at(i).get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, encoder.encoders_[1]->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

// object and scalar mix
TEST_F(TestSemiStructEncoding, test_mix3)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i == 10) {
      ADD_NULL_JSON_NODE(obj, int_null_key, "int_null", int_null_value)
      ObJsonObject likes(&allocator);
      ADD_INT_JSON_NODE(likes, int1_key, "int1", int1_value, rand())
      ADD_INT_JSON_NODE(likes, int2_key, "int2", int2_value, rand())
      ADD_INT_JSON_NODE(likes, int3_key, "int3", int3_value, rand())
      ObString string_key("string");
      ASSERT_EQ(OB_SUCCESS, obj.add(string_key, &likes));
      add_json_datum(allocator, datums, &obj);
    } else {
      ADD_INT_JSON_NODE(obj, int_null_key, "int_null", int_null_value, rand())
      ADD_STRING_JSON_NODE(obj, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
      add_json_datum(allocator, datums, &obj);
    }
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    row.storage_datums_[1].set_string(datums.at(i).get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, encoder.encoders_[1]->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_mix4)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  const char *names[20] = {"Mike", "Jackson", "Liam", "William", "Noah", "Bill", "Christopher", "Bruce",
                    "Les", "Hunter", "Frank", "Noel", "Ted", "Andy", "Devin", "Gordon", "JJJ", "XXXX", "ZZZZ", "oo"};
  int64_t null_cnt = 0;
  for (int i = 0; i < row_cnt; ++i) {
    ObJsonObject obj(&allocator);
    if (i > 10) {
      ObJsonArray child1(&allocator);
      ObJsonInt j_int1(1);
      ObJsonInt j_int2(2);
      ObJsonInt j_int3(3);
      ASSERT_EQ(OB_SUCCESS, child1.append(&j_int1));
      ASSERT_EQ(OB_SUCCESS, child1.append(&j_int2));
      ASSERT_EQ(OB_SUCCESS, child1.append(&j_int3));
      ObString child1_key("child1");
      ASSERT_EQ(OB_SUCCESS, obj.add(child1_key, &child1));
      add_json_datum(allocator, datums, &obj);
    } else {
      ObJsonObject child1(&allocator);
      ADD_INT_JSON_NODE(child1, int1_key, "int1", int1_value, rand())
      ADD_STRING_JSON_NODE(child1, string_key, "string", string_value, names[i%20], STRLEN(names[i%20]))
      ADD_INT_JSON_NODE(child1, int2_key, "int2____", int2_value, rand())
      ObString child1_key("child1");
      ASSERT_EQ(OB_SUCCESS, obj.add(child1_key, &child1));
      add_json_datum(allocator, datums, &obj);
    }
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    row.storage_datums_[1].set_string(datums.at(i).get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_NE(nullptr, encoder.encoders_[1]->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}


struct ObRandomJsonGenerator
{
  int generates(const int row_cnt, ObColDatums &datums);
  int generate(ObIJsonBase *&node, ObDatum &datum);
  int generate_array(ObIJsonBase *&node, const int depth);
  int generate_object(ObIJsonBase *&node, const int depth);
  int generate_scalar(ObIJsonBase *&node, const bool change_type = false);
  int generate_value(ObIJsonBase *&node, const int depth, const bool change_type = false);

  ObIAllocator *allocator_;
  int max_depth_;
};

int ObRandomJsonGenerator::generates(const int row_cnt, ObColDatums &datums)
{
  int ret = OB_SUCCESS;
  max_depth_ = rand() % 10 + 3;
  ObIJsonBase *node = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    ObDatum datum;
    if (OB_FAIL(generate(node, datum))) {
      LOG_WARN("generate fail", K(ret), K(i));
    } else if (OB_FAIL(datums.push_back(datum))) {
      LOG_WARN("push back fail", K(ret), K(i));
    }
  }
  return ret;
}

int ObRandomJsonGenerator::generate(ObIJsonBase *&node, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObString raw_binary;
  if (nullptr != node && node->json_type() == ObJsonNodeType::J_ARRAY) {
    if (OB_FAIL(generate_array(node, 1))) {
      LOG_WARN("generate_array fail", K(ret));
    }
  } else if (nullptr != node && node->json_type() == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(generate_object(node, 1))) {
      LOG_WARN("generate_object fail", K(ret));
    }
  } else if (rand()%4 == 3) {
    if (OB_FAIL(generate_array(node, 1))) {
      LOG_WARN("generate_object fail", K(ret));
    }
  } else if (OB_FAIL(generate_object(node, 1))) {
    LOG_WARN("generate_object fail", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(node->get_raw_binary(raw_binary, allocator_))) {
    LOG_WARN("get_raw_binary fail", K(ret));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(*allocator_, raw_binary, raw_binary))) {
    LOG_WARN("fill_lob_header fail", K(ret));
  } else {
    datum.set_string(raw_binary);
  }
  return ret;
}

int ObRandomJsonGenerator::generate_object(ObIJsonBase* &node, const int depth)
{
  int ret = OB_SUCCESS;
  int child_cnt = rand() % 100 + 10;
  bool need_build = true;
  if (node != nullptr){
    need_build = false;
    child_cnt = node->element_count();
  }
  if (node == nullptr && OB_ISNULL(node = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonObject));
  }
  for (int i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
    ObIJsonBase* child = nullptr;
    ObIJsonBase* old_child = nullptr;
    ObString key;
    bool need_add = true;
    if (! need_build) {
      if (OB_FAIL(node->get_object_value(i, key, child))) {
        LOG_WARN("get child fail", K(ret), K(i), K(child_cnt), K(node->element_count()));
      } else {
        old_child = child;
      }
    } else {
      std::string gen_key = str_rand(rand()%20 + i);
      ObString value(gen_key.length(), gen_key.c_str());
      if (OB_FAIL(ob_write_string(*allocator_, key, value))) {
        LOG_WARN("copy string fail", K(ret), K(value));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_value(child, depth + 1, (i == child_cnt/2 & rand()%100 == 0) ? true : false))) {
      LOG_WARN("generate_value fail", K(ret), K(i));
    } else if (need_build) {
      if (OB_FAIL(node->object_add(key, child))) {
        LOG_WARN("object add fail", K(ret), K(i), K(key));
      }
    } else if (OB_FAIL(node->replace(old_child, child))) {
      LOG_WARN("object replace fail", K(ret), K(i), K(key));
    }
  }
  return ret;
}

int ObRandomJsonGenerator::generate_array(ObIJsonBase* &node, const int depth)
{
  int ret = OB_SUCCESS;
  int child_cnt = rand() % 100 + 10;
  bool need_build = true;
  if (node != nullptr){
    need_build = false;
    child_cnt = node->element_count();
  }
  if (node == nullptr && OB_ISNULL(node = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonObject));
  }
  for (int i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
    ObIJsonBase* child = nullptr;
    ObIJsonBase* old_child = nullptr;
    if (! need_build) {
      if (OB_FAIL(node->get_array_element(i, child))) {
        LOG_WARN("get child fail", K(ret), K(i), K(child_cnt), K(node->element_count()));
      } else {
        old_child = child;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_value(child, depth + 1, (i == child_cnt/2 & rand()%100 == 0) ? true : false))) {
      LOG_WARN("generate_value fail", K(ret), K(i));
    } else if (need_build) {
      if (OB_FAIL(node->array_append(child))) {
        LOG_WARN("object add fail", K(ret), K(i));
      }
    } else if (OB_FAIL(node->replace(old_child, child))) {
      LOG_WARN("object replace fail", K(ret), K(i));
    }
  }
  return ret;
}

int ObRandomJsonGenerator::generate_value(ObIJsonBase *&node, const int depth, const bool change_type)
{
  int ret = OB_SUCCESS;
  if (nullptr != node && node->json_type() == ObJsonNodeType::J_ARRAY) {
    if (OB_FAIL(generate_array(node, depth + 1))) {
      LOG_WARN("generate_array fail", K(ret));
    }
  } else if (nullptr != node && node->json_type() == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(generate_object(node, depth + 1))) {
      LOG_WARN("generate_object fail", K(ret));
    }
  } else if (nullptr != node) {
    if (OB_FAIL(generate_scalar(node, change_type))) {
      LOG_WARN("generate_scalar fail", K(ret));
    }
  } else if (depth >= max_depth_) {
    if (OB_FAIL(generate_scalar(node))) {
      LOG_WARN("generate_scalar fail", K(ret));
    }
  } else if (depth < 3 && rand()%10 == 9) {
    if (OB_FAIL(generate_array(node, depth + 1))) {
      LOG_WARN("generate_array fail", K(ret));
    }
  } else if (depth < 3 && rand()%5 >= 6) {
    if (OB_FAIL(generate_object(node, depth + 1))) {
      LOG_WARN("generate_object fail", K(ret));
    }
  } else if (OB_FAIL(generate_scalar(node))) {
    LOG_WARN("generate_scalar fail", K(ret));
  }
  return ret;
}

int ObRandomJsonGenerator::generate_scalar(ObIJsonBase *&j_node, const bool change_type)
{
  int ret = OB_SUCCESS;
  ObJsonNodeType sub_col_types[5] = {ObJsonNodeType::J_INT, ObJsonNodeType::J_UINT, ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT};
  ObJsonNodeType type = sub_col_types[rand() % 5];
  if (nullptr != j_node) type = j_node->json_type();
  if (ObJsonNodeType::J_NULL == type) {
    if (OB_ISNULL(j_node = OB_NEWx(ObJsonNull, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonNull));
    }
  } else if (ObJsonNodeType::J_INT == type) {
    if (OB_ISNULL(j_node = OB_NEWx(ObJsonInt, allocator_, (rand())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonInt));
    }
  } else if (ObJsonNodeType::J_UINT == type) {
    if (OB_ISNULL(j_node = OB_NEWx(ObJsonUint, allocator_, (rand())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonUint));
    }
  } else if (ObJsonNodeType::J_STRING == type) {
    std::string str = str_rand(rand() % 100);
    ObString value(str.length(), str.c_str());
    if (OB_FAIL(ob_write_string(*allocator_, value, value))) {
      LOG_WARN("copy string fail", K(ret), K(value));
    } else if (OB_ISNULL(j_node = OB_NEWx(ObJsonString, allocator_, value))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonString));
    }
  } else if (ObJsonNodeType::J_DOUBLE == type) {
    if (OB_ISNULL(j_node = OB_NEWx(ObJsonDouble, allocator_, (rand())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonDouble));
    }
  } else if (ObJsonNodeType::J_OFLOAT == type) {
    if (OB_ISNULL(j_node = OB_NEWx(ObJsonOFloat, allocator_, (rand())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonOFloat));
    }
  } else if (ObJsonNodeType::J_BOOLEAN == type) {
    if (OB_ISNULL(j_node = OB_NEWx(ObJsonBoolean, allocator_, (rand()%2)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObJsonBoolean));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support type", K(ret), K(type));
  }
  return ret;
}

TEST_F(TestSemiStructEncoding, test_deep_json)
{
  srand(time(0));
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  ctx_.micro_block_size_ = 1L << 20;
  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObColDatums datums(allocator);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  ObIColumnCSEncoder *e = nullptr;
  ObMicroBlockHeader *header = nullptr;
  ObMicroBlockDesc micro_block_desc;
  ObRandomJsonGenerator generator;
  generator.allocator_ = &allocator;
  ASSERT_EQ(OB_SUCCESS, generator.generates(row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_NE(nullptr, e->ctx_->semistruct_ctx_);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_read_file)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  // const char* data_file = "/data/aozeliu.azl/obdev/task-2025022800107363404/build_debug/unittest/storage/blocksstable/cs_encoding/src_data.bin";
  // // run tests
  // std::ifstream if_tests(data_file);
  // ASSERT_TRUE(if_tests.is_open());
  // std::string content((std::istreambuf_iterator<char>(if_tests)),
  //                       (std::istreambuf_iterator<char>()));
  ObString hex_str("1E00801A000000000640056218021A021C021E022002018222012E013E014E0163316332633363346335FFFF0000C0000001292FED01FFFF0002C0000002292FED0169F92613FFFF0002C0000002292FED0169F92613FFFF001EC0000003292FED0169F9261340122213");
  char *unhex_buf = nullptr;
  int64_t unhex_buf_len = 0;
  ASSERT_EQ(OB_SUCCESS, ObHexUtilsBase::unhex(hex_str, allocator, unhex_buf, unhex_buf_len));
  ObString bin_data(unhex_buf_len, unhex_buf);
  ObJsonBin j_bin(bin_data.ptr(), bin_data.length(), &allocator);
  ASSERT_EQ(OB_SUCCESS, j_bin.reset_iter());
  ObJsonBuffer j_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_bin.print(j_buf, true));
  LOG_INFO("result json", K(j_buf.string()));
}

TEST_F(TestSemiStructEncoding, test_bug_var_size)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  const int64_t row_cnt = 5;
  const char* json_texts[row_cnt] = {
    "[1, 1385, {\"empty_array\":[], \"empty_object\":{}}, {\"id\":215968269,\"name\":\"WJShengD/Micro-Expression-Classification-using-Local-Binary-Pattern-from-Five-Intersecting-Planes\",\"url\":\"xaau;||api$github$com/repos/WJShengD/Micro-Expression-Classification-using\"}, \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\", 1385, 1385]",
    "[2, 1385, {\"empty_array\":[], \"empty_object\":{}}, {\"id\":36473389,\"name\":\"EVEIPH/EVE-IPH\",\"url\":\"xaau;||api$github$com/repos/EVEIPH/EVE-IPH\"}, \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\", 1385, 1385]",
    "[3, 1385, {\"empty_array\":[], \"empty_object\":{}}, {\"id\":252649085,\"name\":\"mdmintz/pytest\",\"url\":\"xaau;||api$github$com/repos/mdmintz/pytest\"}, \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\", 1385, 1385]",
    "[4, 1385, {\"empty_array\":[], \"empty_object\":{}}, {\"id\":553937707,\"name\":\"sumonece15/New-ChatBot\",\"url\":\"xaau;||api$github$com/repos/sumonece15/New-ChatBot\"}, \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\", 1385, 1385]",
    "[5, 1385, {\"empty_array\":[], \"empty_object\":{}}, {\"id\":460137687,\"name\":\"jvkassi/filament-modal\",\"url\":\"xaau;||api$github$com/repos/jvkassi/filament-modal\"}, \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\", 1385, 1385]"
  };

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    // LOG_INFO("json_datum", K(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_empty_array)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  const int64_t row_cnt = 5;
  const char* json_texts[row_cnt] = {
    "{\"id\":215968269, \"empty_array\":[], \"empty_object\":{}}",
    "{\"id\":36473389, \"empty_array\":[], \"empty_object\":{}}",
    "{\"id\":252649085, \"empty_array\":[], \"empty_object\":{}}",
    "{\"id\":553937707, \"empty_array\":[], \"empty_object\":{}}",
    "{\"id\":460137687, \"empty_array\":[], \"empty_object\":{}}"
  };

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    // LOG_INFO("json_datum", K(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_empty_array_v2)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator allocator(ObModIds::TEST);

  ObColDatums datums(allocator);
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;

  const int64_t row_cnt = 5;
  const char* json_texts[row_cnt] = {
    "{\"id\":215968269, \"empty_array\":[], \"empty_object\":{}}",
    "{\"id\":36473389, \"empty_array\":[1], \"empty_object\":{}}",
    "{\"id\":252649085, \"empty_array\":[], \"empty_object\":{\"k1\": 1}}",
    "{\"id\":553937707, \"empty_array\":[3, \"4\"], \"empty_object\":{}}",
    "{\"id\":460137687, \"empty_array\":[], \"empty_object\":{}}"
  };

  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));

  for (int i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(i + 1);
    ObString j_text(STRLEN(json_texts[i]), json_texts[i]);
    ObDatum json_datum;
    ASSERT_EQ(OB_SUCCESS, build_json_datum(allocator, j_text, json_datum));
    ASSERT_EQ(OB_SUCCESS, datums.push_back(json_datum));
    // LOG_INFO("json_datum", K(json_datum));
    row.storage_datums_[1].set_string(json_datum.get_string());
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_TRACE("micro block", K(micro_block_desc), KPC(header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);

  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

TEST_F(TestSemiStructEncoding, test_sub_schema_re_scan_fail)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObJsonType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.semistruct_properties_.mode_ = 1;
  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObColDatums datums(allocator);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, col_cnt));
  ObSemiStructColumnEncodeCtx *semistruct_col_ctx = nullptr;
  ObMicroBlockHeader *header = nullptr;
  ObMicroBlockDesc micro_block_desc;

  int sub_col_count = 5;
  ObJsonNodeType sub_col_types[10] = {
    ObJsonNodeType::J_INT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_UINT, ObJsonNodeType::J_STRING, ObJsonNodeType::J_OFLOAT,
    ObJsonNodeType::J_DOUBLE, ObJsonNodeType::J_INT, ObJsonNodeType::J_BOOLEAN, ObJsonNodeType::J_STRING, ObJsonNodeType::J_DOUBLE };
  std::vector<std::string> sub_col_names;
  for (int i = 0; i < 10; ++i) {
    int len = rand()%10;
    sub_col_names.push_back(str_rand(len > 0 ? len : i));
  }
  ASSERT_EQ(OB_SUCCESS, generate_datums(allocator, sub_col_names, sub_col_types, sub_col_count, row_cnt, datums));
  ASSERT_EQ(OB_SUCCESS, append_row_to_encoder(encoder, row, datums));
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));

  // <2> reuse
  encoder.reuse();
  datums.reuse();
  ASSERT_EQ(OB_SUCCESS, encoder.semistruct_encode_ctx_->get_col_ctx(1, ctx_, semistruct_col_ctx));
  ASSERT_EQ(true, semistruct_col_ctx->sub_schema_->is_inited());
  {
    for (int i = 0; i < row_cnt; ++i) {
      ObJsonObject obj(&allocator);
      if (i == 10) {
        ADD_NULL_JSON_NODE(obj, int_null_key, "int_null", int_null_value)
        ObJsonObject likes(&allocator);
        ADD_INT_JSON_NODE(likes, int1_key, "int1", int1_value, rand())
        ADD_INT_JSON_NODE(likes, int2_key, "int2", int2_value, rand())
        ADD_INT_JSON_NODE(likes, int3_key, "int3", int3_value, rand())
        ObString int_key("int");
        ASSERT_EQ(OB_SUCCESS, obj.add(int_key, &likes));
        add_json_datum(allocator, datums, &obj);
      } else {
        ADD_INT_JSON_NODE(obj, int_null_key, "int_null", int_null_value, rand())
        ADD_INT_JSON_NODE(obj, int_key, "int", int_value, rand()%100)
        add_json_datum(allocator, datums, &obj);
      }
    }

    for (int i = 0; i < row_cnt; ++i) {
      row.storage_datums_[0].set_int(i + 1);
      row.storage_datums_[1].set_string(datums.at(i).get_string());
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
    }
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    ASSERT_EQ(encoder.encoders_[1]->get_type(), ObCSColumnHeader::Type::SEMISTRUCT);
    ASSERT_NE(nullptr, encoder.encoders_[1]->ctx_->semistruct_ctx_);
    ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
    ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
  }
  reuse();
}


TEST_F(TestSemiStructEncoding, test_decimal)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t row_cnt = 20;
  ObColDatums datums(allocator);
  {
      ObJsonObject obj(&allocator);

      // J_DECIMAL
      ObScale scale = 2;
      ObPrecision prec = 10;
      char buf[128] = {0};
      number::ObNumber num;
      sprintf(buf, "%f", 1.333333);
      ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
      ObJsonDecimal j_decimal(num, -1, 0);
      ObString decimal_key("decimal");
      ASSERT_EQ(OB_SUCCESS, obj.add(decimal_key, &j_decimal));

      // J_INT
      ObString int_key("int");
      ObJsonInt int_value(20);
      ASSERT_EQ(OB_SUCCESS, obj.add(int_key, &int_value));

      add_json_datum(allocator, datums, &obj);
  }
  {
    for (int i = 1; i < row_cnt; ++i) {
      ObJsonObject obj(&allocator);

      ObScale scale = 10;
      ObPrecision prec = 8;
      char buf[128] = {0};
      number::ObNumber num;
      sprintf(buf, "%f", i + 1.333333);
      ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
      ObJsonDecimal j_decimal(num, prec, scale);
      ObString decimal_key("decimal");
      ASSERT_EQ(OB_SUCCESS, obj.add(decimal_key, &j_decimal));

      ObScale scale2 = 2;
      ObPrecision prec2 = 10;
      char buf2[128] = {0};
      number::ObNumber num2;
      sprintf(buf2, "%f", i + 1.333333);
      ASSERT_EQ(OB_SUCCESS, num2.from(buf2, allocator));
      ObJsonDecimal j_decimal2(num2, prec2, scale2);
      ObString decimal2_key("decimal2");
      ASSERT_EQ(OB_SUCCESS, obj.add(decimal2_key, &j_decimal2));

      ObScale scale3 = 10;
      ObPrecision prec3 = -1;
      char buf3[128] = {0};
      number::ObNumber num3;
      sprintf(buf3, "%f", i + 222222.444444);
      ASSERT_EQ(OB_SUCCESS, num3.from(buf3, allocator));
      ObJsonDecimal j_decimal3(num3, prec3, scale3);
      ObString decimal3_key("decimal3");
      ASSERT_EQ(OB_SUCCESS, obj.add(decimal3_key, &j_decimal3));

      ObScale scale4 = i%3 + 10;
      ObPrecision prec4 = -1;
      char buf4[128] = {0};
      number::ObNumber num4;
      sprintf(buf4, "%f", i + 5.444444);
      ASSERT_EQ(OB_SUCCESS, num4.from(buf4, allocator));
      ObJsonDecimal j_decimal4(num4, prec4, scale4);
      ObString decimal4_key("decimal4");
      ASSERT_EQ(OB_SUCCESS, obj.add(decimal4_key, &j_decimal4));

      // J_INT
      ObString int_key("int");
      ObJsonInt int_value(i + 20);
      ASSERT_EQ(OB_SUCCESS, obj.add(int_key, &int_value));

      {
          ObJsonBuffer j_buf(&allocator);
          ASSERT_EQ(OB_SUCCESS, obj.print(j_buf, true));
          LOG_INFO("result json", K(i), K(j_buf.string()));
      }
      add_json_datum(allocator, datums, &obj);
    }
  }
  ASSERT_EQ(row_cnt, datums.count());
  ObMicroBlockCSEncoder encoder;
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, do_encode(allocator, encoder, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_full_transform(allocator, row_cnt, row, header, micro_block_desc, datums));
  ASSERT_EQ(OB_SUCCESS, check_part_transform(allocator, row_cnt, rowkey_cnt, col_cnt, row, header, micro_block_desc, datums));
}

}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test.log");
  // OB_LOGGER.set_file_name("test.log");
  // OB_LOGGER.set_log_level("INFO");
  oceanbase::lib::reload_trace_log_config(true);
  return RUN_ALL_TESTS();
}