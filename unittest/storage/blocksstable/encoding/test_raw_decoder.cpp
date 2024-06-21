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
#define protected public
#define private public
#include "storage/blocksstable/encoding/ob_micro_block_encoder.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/encoding/ob_raw_decoder.h"
#include "storage/blocksstable/encoding/ob_raw_encoder.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/access/ob_block_row_store.h"
#include "storage/ob_i_store.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "lib/string/ob_sql_string.h"
#include "../ob_row_generate.h"
#include "common/rowkey/ob_rowkey.h"
#include "test_column_decoder.h"


namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class ObMicroBlockRawEncoder : public ObMicroBlockEncoder
{
public:
  int build_block(char *&buf, int64_t &size);

};

/** This function is to override the function in ObMicroBlockEncoder to perform unittest on
 *  raw_encoding and filter pushdown operators.
 *  Any modify on the original function should be synchronized to here.
 */
int ObMicroBlockRawEncoder::build_block(char *&buf, int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t encoders_need_size = 0;
  const int64_t col_header_size = ctx_.column_cnt_ * (sizeof(ObColumnHeader));
  char *encoding_meta_buf = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(datum_rows_.empty())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("empty micro block", K(ret));
  } else if (OB_FAIL(set_datum_rows_ptr())) {
    STORAGE_LOG(WARN, "fail to set datum rows ptr", K(ret));
  } else if (OB_FAIL(pivot())) {
    LOG_WARN("pivot rows to columns failed", K(ret));
  } else if (OB_FAIL(row_indexs_.reserve(datum_rows_.count()))) {
    LOG_WARN("array reserve failed", K(ret), "count", datum_rows_.count());
  } else if (OB_FAIL(encoder_detection(encoders_need_size))) {
    LOG_WARN("detect column encoding failed", K(ret));
  } else {
    encoders_need_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      const bool force_var_store = false;
      if (NULL != encoders_[i]) {
        free_encoder(encoders_[i]);
        encoders_[i] = NULL;
      }

      ObIColumnEncoder *e = NULL;
      if (OB_FAIL(force_raw_encoding(i, force_var_store, e))) {
        LOG_WARN("force_raw_encoding failed", K(ret), K(i), K(force_var_store));
      } else if (OB_ISNULL(e)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("encoder is NULL", K(ret), K(i));
      } else {
        encoders_[i] = e;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < encoders_.count(); i++) {
      int64_t need_size = 0;
      if (OB_FAIL(encoders_.at(i)->get_encoding_store_meta_need_space(need_size))) {
        STORAGE_LOG(WARN, "fail to get_encoding_store_meta_need_space", K(ret), K(i), K(encoders_));
      } else {
        need_size += encoders_.at(i)->calc_encoding_fix_data_need_space();
        encoders_need_size += need_size;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_buffer_.ensure_space(col_header_size + encoders_need_size))) {
    STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(data_buffer_));
  } else if (OB_ISNULL(encoding_meta_buf = static_cast<char *>(encoding_meta_allocator_.alloc(encoders_need_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc fix header buf", K(ret), K(encoders_need_size));
  } else {
    STORAGE_LOG(DEBUG, "[debug] build micro block", K_(estimate_size), K_(header_size), K_(expand_pct),
        K(datum_rows_.count()), K(ctx_));

    // <1> store encoding metas and fix cols data in encoding_meta_buffer
    int64_t encoding_meta_offset = 0;
    int64_t encoding_meta_size = 0;
    ObBufferWriter meta_buf_writer(encoding_meta_buf, encoders_need_size, 0);
    if (OB_FAIL(store_encoding_meta_and_fix_cols(meta_buf_writer, encoding_meta_offset))) {
      LOG_WARN("failed to store encoding meta and fixed col data", K(ret));
    } else if (FALSE_IT(encoding_meta_size = meta_buf_writer.length())) {
    } else if (OB_FAIL(data_buffer_.write_nop(encoding_meta_size))) {
      STORAGE_LOG(WARN, "failed to write nop", K(ret), K(meta_buf_writer), K(data_buffer_));
    }

    // <2> set row data store offset
    int64_t fix_data_size = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_row_data_pos(fix_data_size))) {
        LOG_WARN("set row data position failed", K(ret));
      } else {
        get_header(data_buffer_)->var_column_count_ = static_cast<uint16_t>(var_data_encoders_.count());
      }
    }

    // <3> fill row data (i.e. var data)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_row_data(fix_data_size))) {
        LOG_WARN("fill row data failed", K(ret));
      }
    }

    // <4> fill row index
    if (OB_SUCC(ret)) {
      if (var_data_encoders_.empty()) {
        get_header(data_buffer_)->row_index_byte_ = 0;
      } else {
        get_header(data_buffer_)->row_index_byte_ = 2;
        if (row_indexs_.at(row_indexs_.count() - 1) > UINT16_MAX) {
          get_header(data_buffer_)->row_index_byte_ = 4;
        }
        ObIntegerArrayGenerator gen;
        const int64_t row_index_size = row_indexs_.count() * get_header(data_buffer_)->row_index_byte_;
        if (OB_FAIL(data_buffer_.ensure_space(row_index_size))) {
          STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(row_index_size), K(data_buffer_));
        } else if (OB_FAIL(gen.init(data_buffer_.data() + data_buffer_.length(), get_header(data_buffer_)->row_index_byte_))) {
          LOG_WARN("init integer array generator failed",
              K(ret), "byte", get_header(data_buffer_)->row_index_byte_);
        } else if (OB_FAIL(data_buffer_.write_nop(row_index_size))) {
          LOG_WARN("advance data buffer failed", K(ret), K(row_index_size));
        } else {
          for (int64_t idx = 0; idx < row_indexs_.count(); ++idx) {
            gen.get_array().set(idx, row_indexs_.at(idx));
          }
        }
      }
    }

    // <5> fill header, encoding_meta and fix cols data
    if (OB_SUCC(ret)) {
      get_header(data_buffer_)->row_count_ = static_cast<uint32_t>(datum_rows_.count());
      get_header(data_buffer_)->has_string_out_row_ = has_string_out_row_;
      get_header(data_buffer_)->all_lob_in_row_ = !has_lob_out_row_;
      get_header(data_buffer_)->max_merged_trans_version_ = max_merged_trans_version_;
      const int64_t header_size = get_header(data_buffer_)->header_size_;
      char *data = data_buffer_.data() + header_size;
      FOREACH(e, encoders_) {
        MEMCPY(data, &(*e)->get_column_header(), sizeof(ObColumnHeader));
        data += sizeof(ObColumnHeader);
      }
      // fill encoding meta and fix cols data
      MEMCPY(data_buffer_.data() + encoding_meta_offset, encoding_meta_buf, encoding_meta_size);
    }

    if (OB_SUCC(ret)) {
      // update encoding context
      ctx_.estimate_block_size_ += estimate_size_;
      ctx_.real_block_size_ += data_buffer_.length() - encoding_meta_offset;
      ctx_.micro_block_cnt_++;
      ObPreviousEncoding pe;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < encoders_.count(); ++idx) {
        ObIColumnEncoder *e = encoders_.at(idx);
        pe.type_ = static_cast<ObColumnHeader::Type>(e->get_column_header().type_);
        if (ObColumnHeader::is_inter_column_encoder(pe.type_)) {
          pe.ref_col_idx_ = static_cast<ObSpanColumnEncoder *>(e)->get_ref_col_idx();
        } else {
          pe.ref_col_idx_ = 0;
        }
        if (ObColumnHeader::STRING_PREFIX == pe.type_) {
          pe.last_prefix_length_ = col_ctxs_.at(idx).last_prefix_length_;
        }
        if (idx < ctx_.previous_encodings_.count()) {
          if (OB_FAIL(ctx_.previous_encodings_.at(idx).put(pe))) {
            LOG_WARN("failed to store previous encoding", K(ret), K(idx), K(pe));
          }

          //if (ctx_->previous_encodings_.at(idx).last_1 != pe.type_) {
            //LOG_DEBUG("encoder changing", K(idx),
                //"previous type", ctx_->previous_encodings_.at(idx).last_,
                //"current type", pe);
          //}
        } else {
          ObPreviousEncodingArray<ObMicroBlockEncodingCtx::MAX_PREV_ENCODING_COUNT> pe_array;
          if (OB_FAIL(pe_array.put(pe))) {
            LOG_WARN("failed to store previous encoding", K(ret), K(idx), K(pe));
          } else if (OB_FAIL(ctx_.previous_encodings_.push_back(pe_array))) {
            LOG_WARN("push back previous encoding failed");
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      buf = data_buffer_.data();
      size = data_buffer_.length();
    }
  }

  return ret;
}

class TestRawDecoder : public ::testing::Test
{
public:
  static const int64_t ROWKEY_CNT = 2;
  static const int64_t COLUMN_CNT = ObExtendType - 1 + 8;

  static const int64_t ROW_CNT = 64;

  virtual void SetUp();
  virtual void TearDown() {}

  TestRawDecoder() : tenant_ctx_(OB_SERVER_TENANT_ID)
  {
    decode_res_pool_ = new(allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool;
    tenant_ctx_.set(decode_res_pool_);
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
    encoder_.encoding_meta_allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    encoder_.data_buffer_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    encoder_.row_buf_holder_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    decode_res_pool_->init();
  }
  virtual ~TestRawDecoder() {}

  void setup_obj(ObObj& obj, int64_t column_id, int64_t seed);
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
      const uint64_t col_id,
      ObMicroBlockDecoder& decoder,
      sql::ObPushdownWhiteFilterNode &filter_node,
      common::ObBitmap &result_bitmap,
      common::ObFixedArray<ObObj, ObIAllocator> &objs);
  int test_filter_pushdown_with_pd_info(
        int64_t start,
        int64_t end,
        const uint64_t col_id,
        ObMicroBlockDecoder& decoder,
        sql::ObPushdownWhiteFilterNode &filter_node,
        common::ObBitmap &result_bitmap,
        common::ObFixedArray<ObObj, ObIAllocator> &objs);
  void test_batch_decode_to_vector(
      const bool is_condensed,
      const bool has_null,
      const bool align_row_id,
      const VectorFormat vector_format);

protected:
  ObRowGenerate row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  ObMicroBlockRawEncoder encoder_;
  MockObTableReadInfo read_info_;
  share::ObTenantBase tenant_ctx_;
  ObDecodeResourcePool *decode_res_pool_;
  int64_t full_column_cnt_;
  ObArenaAllocator allocator_;
};

void TestRawDecoder::SetUp()
{
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  const int64_t tid = 200001;
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(1);
  table.set_database_id(1);
  table.set_table_id(tid);
  table.set_table_name("test_raw_decoder_schema");
  table.set_rowkey_column_num(ROWKEY_CNT);
  table.set_max_column_id(COLUMN_CNT * 2);
  table.set_block_size(2 * 1024);
  table.set_compress_func_name("none");
  table.set_row_store_type(ENCODING_ROW_STORE);
  table.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  ObSqlString str;
  for (int64_t i = 0; i < COLUMN_CNT; ++i) {
    col.reset();
    col.set_table_id(tid);
    col.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    str.assign_fmt("test%ld", i);
    col.set_column_name(str.ptr());
    ObObjType type = static_cast<ObObjType>(i + 1); // 0 is ObNullType
    if (COLUMN_CNT - 1 == i) { // test urowid for last column
      type = ObURowIDType;
    } else if (COLUMN_CNT - 2 == i) {
      type = ObIntervalYMType;
    } else if (COLUMN_CNT - 3 == i) {
      type = ObIntervalDSType;
    } else if (COLUMN_CNT - 4 == i) {
      type = ObTimestampTZType;
    } else if (COLUMN_CNT - 5 == i) {
      type = ObTimestampLTZType;
    } else if (COLUMN_CNT - 6 == i) {
      type = ObTimestampNanoType;
    } else if (COLUMN_CNT - 7 == i) {
      type = ObRawType;
    } else if (COLUMN_CNT - 8 == i) {
      type = ObDecimalIntType;
      col.set_data_precision(18);
      col.set_data_scale(0);
    } else if (type == ObExtendType || type == ObUnknownType) {
      type = ObVarcharType;
    }
    col.set_data_type(type);

    if ( ObVarcharType == type || ObCharType == type || ObHexStringType == type
        || ObNVarchar2Type == type || ObNCharType == type || ObTextType == type){
      col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
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

  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  full_column_cnt_ = COLUMN_CNT + extra_rowkey_cnt;
  ctx_.micro_block_size_ = 64L << 11;
  ctx_.macro_block_size_ = 2L << 20;
  ctx_.rowkey_column_cnt_ = ROWKEY_CNT + extra_rowkey_cnt;
  ctx_.column_cnt_ = COLUMN_CNT + extra_rowkey_cnt;
  ctx_.col_descs_ = &col_descs_;
  ctx_.compressor_type_ = common::ObCompressorType::NONE_COMPRESSOR;

  int64_t *column_encodings = reinterpret_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * ctx_.column_cnt_));
  ctx_.column_encodings_ = column_encodings;
  for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
    ctx_.column_encodings_[i] = ObColumnHeader::Type::RAW;
  }
  ctx_.row_store_type_ = common::ENCODING_ROW_STORE;

  ASSERT_EQ(OB_SUCCESS, encoder_.init(ctx_));
}

void TestRawDecoder::setup_obj(ObObj& obj, int64_t column_id, int64_t seed)
{
  obj.copy_meta_type(row_generate_.column_list_.at(column_id).col_type_);
  ObObjType column_type = row_generate_.column_list_.at(column_id).col_type_.get_type();
  LOG_INFO("Type of current column is: ", K(column_type));
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

void TestRawDecoder::init_filter(
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

void TestRawDecoder::init_in_filter(
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

int TestRawDecoder::test_filter_pushdown(
    const uint64_t col_idx,
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
    pd_filter_info.col_capacity_ = full_column_cnt_;
    pd_filter_info.start_ = 0;
    pd_filter_info.count_ = decoder.row_count_;

    ret = decoder.filter_pushdown_filter(nullptr, filter, pd_filter_info, result_bitmap);
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

int TestRawDecoder::test_filter_pushdown_with_pd_info(
    int64_t start,
    int64_t end,
    const uint64_t col_idx,
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

  pd_filter_info.col_capacity_ = full_column_cnt_;
  pd_filter_info.start_ = start;
  pd_filter_info.count_ = end - start;

  ret = decoder.filter_pushdown_filter(nullptr, filter, pd_filter_info, result_bitmap);
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

void TestRawDecoder::test_batch_decode_to_vector(
    const bool is_condensed,
    const bool has_null,
    const bool align_row_id,
    const VectorFormat vector_format)
{
  FLOG_INFO("start one batch decode to vector test", K(is_condensed), K(has_null), K(align_row_id), K(vector_format));
  ObArenaAllocator test_allocator;
  encoder_.reuse();
    // Generate data and encode
  void *row_buf = test_allocator.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ASSERT_TRUE(nullptr != row_buf);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(test_allocator, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(test_allocator, full_column_cnt_));
  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }

  if (has_null) {
    for (int64_t j = 0; j < full_column_cnt_; ++j) {
      row.storage_datums_[j].set_null();
    }
    for (int64_t i = ROW_CNT - 35; i < 40; ++i) {
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, test_allocator);
    }
  } else {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    for (int64_t i = ROW_CNT - 35; i < 40; ++i) {
      ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
      rows[i].deep_copy(row, test_allocator);
    }
  }

  for (int64_t i = 40; i < 60; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }

  for (int64_t i = 60; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0 - i, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }

  if (is_condensed) {
    encoder_.ctx_.encoder_opt_.enable_bit_packing_ = false;
  } else {
    encoder_.ctx_.encoder_opt_.enable_bit_packing_ = true;
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
    } else if (vector_format == VEC_FIXED) {
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

    if (!need_test_column) {
      continue;
    }

    sql::ObExpr col_expr;
    int64_t test_row_cnt = align_row_id ? ROW_CNT : ROW_CNT / 2;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        ROW_CNT, col_meta, vector_format, eval_ctx, col_expr, frame_allocator));
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_meta),  K(*decoder.decoders_[col_offset].ctx_),
        K(precision), K(vec_tc), K(need_test_column));

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

TEST_F(TestRawDecoder, filter_pushdown_all_eq_ne)
{
  // Generate data and encode
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed1 = 0xF;
  int64_t seed2 = 0x0;
  for (int64_t i = 0; i < ROW_CNT - 20; ++i) {
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


  int64_t seed1_count = ROW_CNT - 20;
  int64_t seed2_count = 10;
  int64_t null_count = 10;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));


  // Dedcode and filter_push_down
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);


  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    // Test Equal operator
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    objs.push_back(ref_obj);

    // 0--- ROW_CNT-20 --- ROW_CNT-10 --- ROW_CNT
    // |    seed1   |   seed2   |     null     |
    //   |               |
    //   ROW_CNT-45    ROW_CNT-15
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_EQ;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(25, pd_result_bitmap.popcnt());

    // Test Not Equal operator
    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5, pd_result_bitmap.popcnt());
  }
}

TEST_F(TestRawDecoder, filter_push_down_gt_lt_ge_le)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
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


  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);

    objs.push_back(ref_obj);

    // 0 --- ROW_CNT-30 --- ROW_CNT-20 --- ROW_CNT-10 --- ROW_CNT
    // |  seed0  |    seed1    |   seed2      |      null       |
    //       |                          |
    //   ROW_CNT-45                ROW_CNT-15
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_GT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    // Greater Than
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5, pd_result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(15, pd_result_bitmap.popcnt());


    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(seed1_count + 5, pd_result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(15 + seed1_count, pd_result_bitmap.popcnt());

    // Invalid input parameter
    objs.pop_back();
    ASSERT_EQ(objs.count(), 0);
    ASSERT_EQ(OB_ERR_UNEXPECTED, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(OB_ERR_UNEXPECTED, test_filter_pushdown_with_pd_info(ROW_CNT - 45, ROW_CNT - 15, col_idx, decoder, white_filter, pd_result_bitmap, objs));
  }
}

TEST_F(TestRawDecoder, filter_push_down_bt)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
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

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 2);
    objs.init(2);

    ObObj ref_obj1;
    setup_obj(ref_obj1, i, seed0);

    ObObj ref_obj2;
    setup_obj(ref_obj2, i, seed2);

    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);

    // 0 --------- ROW_CNT-10 --- ROW_CNT
    // |   seed0      |    seed1    |
    //         |               |
    //     ROW_CNT-35      ROW_CNT-5

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_BT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(30, pd_result_bitmap.popcnt());

    objs.reuse();
    objs.init(2);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj1);

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
  }
}

TEST_F(TestRawDecoder, filter_push_down_in_nu)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  int64_t seed3 = 0x3;
  int64_t seed4 = 0x4;
  int64_t seed5 = 0x5;

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
    LOG_INFO("Null row appended: ", K(row));
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
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    sql::ObPushdownWhiteFilterNode white_filter_2(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer);
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

    // 0 --- ROW_CNT-40 --- ROW_CNT-30 --- ROW_CNT-20 --- ROW_CNT-10 --- ROW_CNT
    // |  seed0   |    seed1    |   seed2      |      seed3       |   null     |
    //                   |                        			              |
    //              ROW_CNT-35                                    ROW_CNT-5
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_IN;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5 + seed2_count, pd_result_bitmap.popcnt());

    objs.reuse();
    objs.init(3);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    white_filter_2.op_type_ = sql::WHITE_OP_IN;

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter_2, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_NU;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(null_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(5, pd_result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_NN;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(ROW_CNT - null_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 35, ROW_CNT - 5, col_idx, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(25, pd_result_bitmap.popcnt());
  }
}

TEST_F(TestRawDecoder, batch_decode_to_datum)
{
  // Generate data and encode
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 35; i < 40; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  for (int64_t i = 40; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
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
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_descs_.at(i)),  K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int32_t row_ids[ROW_CNT];
    for (int32_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
        .batch_decode(decoder.row_index_, row_ids, cell_datas, ROW_CNT, datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datums[j]));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datums[j]));
    }
  }
}

TEST_F(TestRawDecoder, batch_decode_to_vector)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  test_batch_decode_to_vector(false, true, row_aligned, vec_format); \
  test_batch_decode_to_vector(false, false, row_aligned, vec_format); \
  test_batch_decode_to_vector(true, true, row_aligned, vec_format); \
  test_batch_decode_to_vector(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_FIXED);
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestRawDecoder, opt_batch_decode_to_datum)
{
  // Generate data and encode
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  for (int64_t i = 0; i < ROW_CNT ; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    for (int64_t j = 0; j < full_column_cnt_; ++j) {
      // Generate data for var_length
      if (col_descs_[j].col_type_.get_type() == ObVarcharType) {
        ObStorageDatum &datum = row.storage_datums_[j];
        datum.len_ = i < datum.len_ ? i : datum.len_;
      }
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  const_cast<bool &>(encoder_.ctx_.encoder_opt_.enable_bit_packing_) = false;
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
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    STORAGE_LOG(INFO, "Current col: ", K(i),K(col_descs_.at(i)), K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int32_t row_ids[ROW_CNT];
    for (int32_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
        .batch_decode(decoder.row_index_, row_ids, cell_datas, ROW_CNT, datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datums[j]));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datums[j]));
    }
  }
}

TEST_F(TestRawDecoder, cell_decode_to_datum)
{
  // Generate data and encode
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 35; i < 40; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }
  for (int64_t i = 40; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
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

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_raw_decoder.log*");
  OB_LOGGER.set_file_name("test_raw_decoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
