
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

#ifndef OCEANBASE_CS_ENCODING_OB_CS_ENCODING_TEST_BASE_H_
#define OCEANBASE_CS_ENCODING_OB_CS_ENCODING_TEST_BASE_H_
#define USING_LOG_PREFIX STORAGE

#include "unittest/storage/blocksstable/encoding/test_column_decoder.h"
#include "../ob_row_generate.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_cluster_version.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/cs_encoding/ob_column_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_decoder.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "storage/blocksstable/ob_decode_resource_pool.h"
#include "storage/ob_i_store.h"
#include <gtest/gtest.h>
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "unittest/storage/mock_ob_table_read_info.h"

namespace oceanbase
{
using namespace sql;
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;
class ObCSEncodingTestBase
{
public:
  ObCSEncodingTestBase(): tenant_ctx_(500)
  {
    decode_res_pool_ = new(allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool;
    tenant_ctx_.set(decode_res_pool_);
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
    decode_res_pool_->init();
  }
  virtual ~ObCSEncodingTestBase() {}
  int prepare(const ObObjType *col_types, const int64_t rowkey_cnt, const int64_t column_cnt,
      const ObCompressorType compressor_type = ObCompressorType::ZSTD_1_3_8_COMPRESSOR,
      const int64_t *precision_arr = nullptr);
  void reuse();

protected:
  int init_cs_decoder(const ObMicroBlockHeader *header,
                      const ObMicroBlockDesc &desc,
                      ObMicroBlockData &full_transformed_data,
                      ObMicroBlockCSDecoder &decoder);
  int build_micro_block_desc(ObMicroBlockCSEncoder &encoder, ObMicroBlockDesc &desc, ObMicroBlockHeader* &header);
  int check_decode_vector(ObMicroBlockCSDecoder &decoder,
                          const ObDatumRow *row_arr,
                          const int64_t row_cnt,
                          const VectorFormat vector_format);
  int full_transform_check_row(const ObMicroBlockHeader *header,
                               const ObMicroBlockDesc &desc,
                               const ObDatumRow *row_arr,
                               const int64_t row_cnt,
                               const bool check_by_get = false);
 int part_transform_check_row(const ObMicroBlockHeader *header,
                              const ObMicroBlockDesc &desc,
                              const ObDatumRow *row_arr,
                              const int64_t row_cnt,
                              const bool check_by_get = false);
 int check_get_row_count(const ObMicroBlockHeader *header,
                         const ObMicroBlockDesc &desc,
                         const int64_t *expected_row_cnt_arr,
                         const int64_t col_cnt,
                         const bool contains_null);

protected:
  ObRowGenerate row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  MockObTableReadInfo read_info_;
  ObArenaAllocator allocator_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  int64_t column_cnt_;
  share::ObTenantBase tenant_ctx_;
  ObDecodeResourcePool *decode_res_pool_;
};

int ObCSEncodingTestBase::prepare(const ObObjType *col_types, const int64_t rowkey_cnt,
    const int64_t column_cnt, const ObCompressorType compressor_type, const int64_t *precision_arr)
{
  int ret = OB_SUCCESS;
  const int64_t tid = 200001;
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(1);
  table.set_database_id(1);
  table.set_table_id(tid);
  table.set_table_name("test_cs_encoder_schema");
  table.set_rowkey_column_num(rowkey_cnt);
  table.set_max_column_id(column_cnt * 2);
  ctx_.column_encodings_ = static_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * column_cnt));
  ObSqlString str;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
    col.reset();
    col.set_table_id(tid);
    col.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    str.assign_fmt("test%ld", i);
    col.set_column_name(str.ptr());
    ObObjType type = col_types[i];
    col.set_data_type(type);


    if (ObDecimalIntType == type) {
      if (precision_arr == nullptr) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid precision_arr", K(ret));
      } else {
        col.set_data_precision(precision_arr[i]);
        col.set_data_scale(0);
      }
    }
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
    if (i < rowkey_cnt) {
      col.set_rowkey_position(i + 1);
    } else {
      col.set_rowkey_position(0);
    }
    if (OB_FAIL(table.add_column(col))) {
      LOG_WARN("fail to add column", K(ret));
    }
    ctx_.column_encodings_[i] = ObCSColumnHeader::Type::MAX_TYPE;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_generate_.init(table, &allocator_))) {
    LOG_WARN("fail to init row_generate_", K(ret));
  } else if (OB_FAIL(row_generate_.get_schema().get_column_ids(col_descs_))) {
    LOG_WARN("fail to get_column_ids", K(ret));
  }
  const ObColumnSchemaV2 *col_schema = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
    if (OB_ISNULL(col_schema = row_generate_.get_schema().get_column_schema(i + OB_APP_MIN_COLUMN_ID))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column schema is NULL", K(ret), K(i));
    } else if (col_descs_.at(i).col_type_.is_decimal_int()) {
      col_descs_.at(i).col_type_.set_stored_precision(col_schema->get_accuracy().get_precision());
      col_descs_.at(i).col_type_.set_scale(col_schema->get_accuracy().get_scale());
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_info_.init(allocator_, row_generate_.get_schema().get_column_count(),
               row_generate_.get_schema().get_rowkey_column_num(), lib::is_oracle_mode(),
               col_descs_, nullptr))) {
    LOG_WARN("fail to init read_info", K(ret));
  } else {
    ctx_.micro_block_size_ = 1L << 20;  // 1MB, maximum micro block size;
    ctx_.macro_block_size_ = 2L << 20;
    ctx_.rowkey_column_cnt_ = rowkey_cnt;
    ctx_.column_cnt_ = column_cnt;
    ctx_.col_descs_ = &col_descs_;
    ctx_.major_working_cluster_version_ = cal_version(4, 1, 0, 0);
    ctx_.row_store_type_ = common::CS_ENCODING_ROW_STORE;
    ctx_.compressor_type_ = compressor_type;
    ctx_.need_calc_column_chksum_ = true;
  }

  return ret;
}

void ObCSEncodingTestBase::reuse()
{
  read_info_.reset();
  row_generate_.reset();
  allocator_.reuse();
  col_descs_.reuse();
  ctx_.column_encodings_ = nullptr;
}

int ObCSEncodingTestBase::build_micro_block_desc(ObMicroBlockCSEncoder &encoder, ObMicroBlockDesc &desc, ObMicroBlockHeader* &header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encoder.build_micro_block_desc(desc))) {
    LOG_WARN("fail to build_micro_block_desc", K(ret));
  } else {
    header = const_cast<ObMicroBlockHeader *>(desc.header_);
    header->data_length_ = desc.buf_size_;
    header->data_zlength_ = desc.buf_size_;
    header->data_checksum_ = ob_crc64_sse42(0, desc.buf_, desc.buf_size_);
    header->original_length_ = desc.original_size_;
    header->set_header_checksum();
  }

  return ret;
}

int ObCSEncodingTestBase::init_cs_decoder(const ObMicroBlockHeader *header,
                                          const ObMicroBlockDesc &desc,
                                          ObMicroBlockData &full_transformed_data,
                                          ObMicroBlockCSDecoder &decoder)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  int64_t buf_len = 0;
  ObCSMicroBlockTransformer tansformer;
  if (OB_FAIL(tansformer.init(header, desc.buf_, desc.buf_size_))) {
    LOG_WARN("fail to init tansformer", K(ret));
  } else if (OB_FAIL(tansformer.calc_full_transform_size(buf_len))) {
    LOG_WARN("fail to calc_full_transform_size", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc", K(ret), K(buf_len));
  } else if (OB_FAIL(tansformer.full_transform(buf, buf_len, pos))) {
    LOG_WARN("fail to full transfrom", K(ret));
  } else if (buf_len != pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is unexpected", K(ret), K(buf_len), K(pos));
  } else {
    full_transformed_data.buf_ = buf;
    full_transformed_data.size_ = buf_len;
    if (OB_FAIL(decoder.init(full_transformed_data, read_info_))) {
      LOG_WARN("fail to init decoder", K(ret));
    }
  }
  return ret;
}

int ObCSEncodingTestBase::full_transform_check_row(const ObMicroBlockHeader *header,
                                                   const ObMicroBlockDesc &desc,
                                                   const ObDatumRow *row_arr,
                                                   const int64_t row_cnt,
                                                   const bool check_by_get)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  if (OB_FAIL(init_cs_decoder(header, desc, full_transformed_data, decoder))) {
    LOG_WARN("fail to init cs_decoder", KR(ret));
  } else {
    ObDatumRow row;
    if (OB_FAIL(row.init(allocator_, ctx_.column_cnt_))) {
      LOG_WARN("fail to init row", K(ret), K(ctx_.column_cnt_));
    }

    for (int32_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      if (OB_FAIL(decoder.get_row(i, row))) {
        LOG_WARN("fail to get row", K(i));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < ctx_.column_cnt_; ++j) {
        if (!ObDatum::binary_equal(row_arr[i].storage_datums_[j], row.storage_datums_[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_INFO("not equal row: ", K(ret), K(i), K(j), K(row_arr[i].storage_datums_[j]), K(row.storage_datums_[j]));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_decode_vector(decoder, row_arr, row_cnt, VEC_UNIFORM))) {
      LOG_WARN("fail to check_decode_vector for VEC_UNIFORM", K(ret));
    } else if (OB_FAIL(check_decode_vector(decoder, row_arr, row_cnt, VEC_FIXED))) {
      LOG_WARN("fail to check_decode_vector for VEC_FIXED", K(ret));
    } else if (OB_FAIL(check_decode_vector(decoder, row_arr, row_cnt, VEC_DISCRETE))) {
      LOG_WARN("fail to check_decode_vector for VEC_DISCRETE", K(ret));
    } else if (OB_FAIL(check_decode_vector(decoder, row_arr, row_cnt, VEC_CONTINUOUS))) {
      LOG_WARN("fail to check_decode_vector for VEC_CONTINUOUS", K(ret));
    }

    if (OB_SUCC(ret) && check_by_get) {
      ObCSEncodeBlockGetReader get_reader;
      ObDatumRowkey rowkey;
      for(int32_t i = 0; i < row_cnt; ++i) {
        rowkey.assign(row_arr[i].storage_datums_, ctx_.rowkey_column_cnt_);
        if (OB_FAIL(get_reader.get_row(full_transformed_data, rowkey, read_info_, row))) {
          LOG_WARN("fail to get row", K(ret), K(rowkey));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < ctx_.column_cnt_; ++j) {
          if (!ObDatum::binary_equal(row_arr[i].storage_datums_[j], row.storage_datums_[j])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_INFO("not equal row: ", K(ret), K(i), K(j), K(row_arr[i].storage_datums_[j]), K(row.storage_datums_[j]));
          }
        }
      }
    }
  }

  return ret;
}

int ObCSEncodingTestBase::check_decode_vector(ObMicroBlockCSDecoder &decoder,
                                              const ObDatumRow *row_arr,
                                              const int64_t row_cnt,
                                              const VectorFormat vector_format)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator frame_allocator;
  sql::ObExecContext exec_context(allocator_);
  sql::ObEvalCtx eval_ctx(exec_context);
  char *buf = nullptr;
  if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_.alloc(
      row_cnt * (sizeof(char*)/*ptr_arr*/ + sizeof(uint32_t)/*len_arr*/ + sizeof(int32_t)/*row_ids*/))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate", K(ret));
  } else {
    const char** ptr_arr = reinterpret_cast<const char**>(buf);
    buf += row_cnt * sizeof(char*);
    uint32_t *len_arr = reinterpret_cast<uint32_t*>(buf) ;
    buf += row_cnt * sizeof(uint32_t);
    int32_t *row_ids = reinterpret_cast<int32_t*>(buf);
    bool need_test_column = true;

    for (int col_idx = 0; OB_SUCC(ret) && col_idx < ctx_.column_cnt_; col_idx++) {
      sql::ObExpr col_expr;
      ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
      const int16_t precision = col_meta.is_decimal_int() ? col_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
      VecValueTypeClass vec_tc = common::get_vec_value_tc(col_meta.get_type(), col_meta.get_scale(), precision);
      need_test_column = true;

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
      } else if (vector_format == VEC_DISCRETE || vector_format == VEC_UNIFORM) {
        VecValueTypeClass var_tc_arr[] = {VEC_TC_NUMBER, VEC_TC_EXTEND, VEC_TC_STRING, VEC_TC_ENUM_SET_INNER,
            VEC_TC_RAW, VEC_TC_ROWID, VEC_TC_LOB, VEC_TC_JSON, VEC_TC_GEO, VEC_TC_UDT, VEC_TC_ROARINGBITMAP};
        VecValueTypeClass *vec = std::find(std::begin(var_tc_arr), std::end(var_tc_arr), vec_tc);
        if (vec == std::end(var_tc_arr)) {
          need_test_column = false;
        }
      } else if (vector_format == VEC_CONTINUOUS) {
        /* can't test now
        VecValueTypeClass var_tc_arr[] = {VEC_TC_NUMBER, VEC_TC_EXTEND, VEC_TC_STRING, VEC_TC_ENUM_SET_INNER,
            VEC_TC_RAW, VEC_TC_ROWID, VEC_TC_LOB, VEC_TC_JSON, VEC_TC_GEO, VEC_TC_UDT, VEC_TC_ROARINGBITMAP};
        VecValueTypeClass *vec = std::find(std::begin(var_tc_arr), std::end(var_tc_arr), vec_tc);
        if (vec == std::end(var_tc_arr)) {
          need_test_column = false;
        }
        */
        need_test_column = false;

      }

      if (!need_test_column) {
        continue;
      }
      LOG_INFO("Current col: ", K(col_idx), K(col_meta), K(vector_format), K(precision), K(vec_tc));
      if (OB_FAIL(VectorDecodeTestUtil::generate_column_output_expr(
          row_cnt, col_meta, vector_format, eval_ctx, col_expr, frame_allocator))) {
        LOG_WARN("fail to generate_column_output_expr", K(ret), K(vec_tc), K(col_meta), K(vector_format));
      } else {
        for (int32_t row_idx = 0; row_idx < row_cnt; ++row_idx) {
          row_ids[row_idx] = row_idx;
        }
        ObVectorDecodeCtx vector_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));
        if (OB_FAIL(decoder.get_col_data(col_idx, vector_ctx))) {
          LOG_WARN("fail to get_col_dagta", K(col_idx), K(vector_ctx));
        }

        for (int64_t vec_idx = 0;  OB_SUCC(ret) && vec_idx < row_cnt; ++vec_idx) {
          if (false == VectorDecodeTestUtil::verify_vector_and_datum_match(
              *vector_ctx.get_vector(), vec_idx, row_arr[vec_idx].storage_datums_[col_idx])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("decode vector result mismatch",
              K(ret), K(vec_idx), K(col_idx), K(col_meta), K(vec_tc), K(vector_format), K(precision));
          }
        }
      }
    }
  }

  return ret;
}

int ObCSEncodingTestBase::part_transform_check_row(const ObMicroBlockHeader *header,
                                                   const ObMicroBlockDesc &desc,
                                                   const ObDatumRow *row_arr,
                                                   const int64_t row_cnt,
                                                   const bool check_by_get)
{
  int ret = OB_SUCCESS;
  // test part transform
  ObMicroBlockCSDecoder decoder;
  const char *block_buf = desc.buf_  - header->header_size_;
  const int64_t block_buf_len = desc.buf_size_ + header->header_size_;
  ObMicroBlockData part_transformed_data(block_buf, block_buf_len);
  int32_t project_step = 1;
  for (int project_step = 1; OB_SUCC(ret) && project_step < ctx_.column_cnt_; project_step++) {
    ObDatumRow row;
    MockObTableReadInfo read_info;
    common::ObArray<int32_t> storage_cols_index;
    common::ObArray<share::schema::ObColDesc> col_descs;
    for (int store_id = 0; OB_SUCC(ret) && store_id < ctx_.column_cnt_; store_id += project_step) {
      if (OB_FAIL(storage_cols_index.push_back(store_id))) {
        LOG_WARN("fail to push_back", K(ret), K(store_id));
      } else if (OB_FAIL(col_descs.push_back(col_descs_.at(store_id)))) {
        LOG_WARN("fail to push_back", K(ret), K(store_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(read_info.init(allocator_, row_generate_.get_schema().get_column_count(),
               row_generate_.get_schema().get_rowkey_column_num(), lib::is_oracle_mode(),
               col_descs, &storage_cols_index))) {
      LOG_WARN("fail to init read_info", K(ret), K(col_descs), K(storage_cols_index));
    } else if (OB_FAIL(decoder.init(part_transformed_data, read_info))) {
      LOG_WARN("fail to init decoder", K(ret));
    } else if (OB_FAIL(row.init(allocator_, col_descs.count()))) {
      LOG_WARN("fail to init row", K(ret), K(col_descs.count()));
    }

    LOG_INFO("part read", K(project_step), K(storage_cols_index), K(col_descs), K(read_info));

    for (int32_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      if (OB_FAIL(decoder.get_row(i, row))) {
        LOG_WARN("fail to get row", K(i));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < storage_cols_index.count(); ++j) {
        if (!ObDatum::binary_equal(row_arr[i].storage_datums_[storage_cols_index.at(j)], row.storage_datums_[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_INFO("not equal row: ", K(ret), K(i), K(j), K(row_arr[i].storage_datums_[j]), K(row.storage_datums_[j]));
        }
      }
    }

    if (OB_SUCC(ret) && check_by_get) {
      ObCSEncodeBlockGetReader get_reader;
      ObDatumRowkey rowkey;
      for(int32_t i = 0; i < row_cnt; ++i) {
        rowkey.assign(row_arr[i].storage_datums_, ctx_.rowkey_column_cnt_);
        if (OB_FAIL(get_reader.get_row(part_transformed_data, rowkey, read_info, row))) {
          LOG_WARN("fail to get row", K(ret), K(rowkey));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < storage_cols_index.count(); ++j) {
          if (!ObDatum::binary_equal(row_arr[i].storage_datums_[storage_cols_index.at(j)], row.storage_datums_[j])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_INFO("not equal row: ", K(ret), K(i), K(j), K(storage_cols_index.at(j)),
                K(row_arr[i].storage_datums_[storage_cols_index.at(j)]), K(row.storage_datums_[j]));
          }
        }
      }
    }
  }

  return ret;
}

int ObCSEncodingTestBase::check_get_row_count(const ObMicroBlockHeader *header,
                                              const ObMicroBlockDesc &desc,
                                              const int64_t *expected_row_cnt_arr,
                                              const int64_t col_cnt,
                                              const bool contains_null)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  int32_t *row_ids = nullptr;
  if (OB_FAIL(init_cs_decoder(header, desc, full_transformed_data, decoder))) {
    LOG_WARN("fail to init cs_decoder", KR(ret));
  } else if (OB_ISNULL(row_ids = (int32_t*)allocator_.alloc(header->row_count_ * sizeof(int32_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc", K(ret), KPC(header));
  } else {
    int64_t real_row_count = 0;
    for (int64_t i = 0; i < header->row_count_; i++) {
      row_ids[i] = i;
    }

    uint64_t seed = ObTimeUtil::current_time();
    std::shuffle(row_ids, row_ids + header->row_count_, std::default_random_engine(seed)); // 随机打乱数组

    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_cnt; col_idx++) {
      if (OB_FAIL(decoder.get_row_count(col_idx, row_ids, header->row_count_, contains_null, nullptr, real_row_count))) {
        if (expected_row_cnt_arr[col_idx] != real_row_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row count mismatch", K(ret), K(contains_null),
              K(col_idx), K(expected_row_cnt_arr[col_idx]), K(real_row_count));
        }
      }
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase

#endif
