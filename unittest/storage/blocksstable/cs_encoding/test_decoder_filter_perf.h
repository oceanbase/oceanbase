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
#ifndef OCEANBASE_CS_ENCODING_TEST_DECODER_FILTER_PERF_H_
#define OCEANBASE_CS_ENCODING_TEST_DECODER_FILTER_PERF_H_

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/encoding/ob_micro_block_encoder.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/cs_encoding/ob_column_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_decoder.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "lib/string/ob_sql_string.h"
#include "common/rowkey/ob_rowkey.h"
#include "../ob_row_generate.h"
#include "unittest/storage/mock_ob_table_read_info.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share::schema;

struct TestFilterPerfTimeCtx
{
const static int64_t FORMAT_TYPE = 2; // 0: PAX; 1: COLUMN_STORE
const static int64_t DATA_TYPE = 2; // 0: INTEGER; 1: STRING
const static int64_t OP_TYPE = 4; // 0: NU_NN; 1: EQ_NE; 2: CMP; 3: IN_BT

TestFilterPerfTimeCtx()
{
  reuse();
}

OB_INLINE int64_t get_total_cost(const int64_t format_type)
{
  int64_t total_cost_ns = 0;
  for (int64_t i = 0; i < DATA_TYPE; ++i) {
    for (int64_t j = 0; j < OP_TYPE; ++j) {
      total_cost_ns += cost_arr_[format_type][i][j];
    }
  }
  return total_cost_ns;
}

OB_INLINE void add_filter_cost(const bool is_column_store, const bool is_string,
  const sql::ObWhiteFilterOperatorType &op_type, const int64_t cost_ns)
{
  uint8_t format_idx = (uint8_t)(is_column_store);
  uint8_t data_idx = (uint8_t)(is_string);
  uint8_t op_idx = 0;
  switch (op_type) {
    case sql::WHITE_OP_NU:
    case sql::WHITE_OP_NN:
      op_idx = 0;
      break;
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE:
      op_idx = 1;
      break;
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_LE:
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_GE:
      op_idx = 2;
      break;
    case sql::WHITE_OP_IN:
    case sql::WHITE_OP_BT:
      op_idx = 3;
      break;
    default:
      break;
  }
  cost_arr_[format_idx][data_idx][op_idx] += cost_ns;
}

OB_INLINE void reuse()
{
  MEMSET(cost_arr_, 0, FORMAT_TYPE * DATA_TYPE * OP_TYPE * sizeof(int64_t));
}

int64_t cost_arr_[FORMAT_TYPE][DATA_TYPE][OP_TYPE];
ObSEArray<int64_t, 31> tmp_pax_cost_;
ObSEArray<int64_t, 31> tmp_cs_cost_;
};

struct TestDecoderPerfTimeCtx
{
const static int64_t FORMAT_TYPE = 2; // 0: PAX; 1: COLUMN_STORE
const static int64_t OP_TYPE = 5; // 0: NU_NN; 1: EQ_NE; 2: CMP; 3: IN_BT; 4: DECOMPRESS

const static int64_t NU_NN_IDX = 0;
const static int64_t EQ_NE_IDX = 1;
const static int64_t CMP_IDX = 2;
const static int64_t IN_BT_IDX = 3;
const static int64_t DECOMP_IDX = 4;

TestDecoderPerfTimeCtx()
{
  reuse();
}

OB_INLINE int64_t get_total_cost(const int64_t format_type)
{
  int64_t total_cost_ns = 0;
  // not include decompress
  for (int64_t i = 0; i < OP_TYPE - 1; ++i) {
    total_cost_ns += cost_arr_[format_type][i];
  }
  return total_cost_ns;
}

OB_INLINE int64_t get_decompress_cost(const int64_t format_type)
{
  return cost_arr_[format_type][DECOMP_IDX];
}

OB_INLINE void reuse()
{
  MEMSET(cost_arr_, 0, FORMAT_TYPE * OP_TYPE * sizeof(int64_t));
}

OB_INLINE void add_decode_cost(const bool is_column_store, const int64_t op_idx, const int64_t cost_ns)
{
  uint8_t format_idx = (uint8_t)(is_column_store);
  cost_arr_[format_idx][op_idx] += cost_ns;
}

int64_t cost_arr_[FORMAT_TYPE][OP_TYPE];
};

struct TestPerfTimeCtx
{
  TestPerfTimeCtx() :
    row_cnt_(0), is_raw_(false), need_compress_(false),
    need_cs_full_transform_(false), is_bit_packing_(true),
    execute_round_(1), with_null_data_(false),
    filter_time_ctx_(), decoder_time_ctx_()
  {}

  void reset();
  void reuse() { filter_time_ctx_.reuse(); decoder_time_ctx_.reuse(); }

  void print_report(const bool need_total = true, const bool need_detail = true);
  void print_total_report();
  void print_detail_report();

  void calc_diff(const int64_t l_val, const int64_t r_val, bool &is_positive, double &diff);

  int64_t row_cnt_;
  bool is_raw_;
  bool need_compress_;
  bool need_cs_full_transform_;
  bool is_bit_packing_;
  int64_t execute_round_;
  bool with_null_data_;
  TestFilterPerfTimeCtx filter_time_ctx_;
  TestDecoderPerfTimeCtx decoder_time_ctx_;
};

void TestPerfTimeCtx::reset()
{
  row_cnt_ = 0;
  is_raw_ = false;
  need_compress_ = false;
  need_cs_full_transform_ = false;
  is_bit_packing_ = true;
  execute_round_ = 1;
  with_null_data_ = false;
  filter_time_ctx_.reuse();
  decoder_time_ctx_.reuse();
}

#define TPPRINT(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)

void TestPerfTimeCtx::print_report(
    const bool need_total,
    const bool need_detail)
{
  if (need_total) {
    print_total_report();
  }
  if (need_detail) {
    print_detail_report();
  }
}

void TestPerfTimeCtx::print_total_report()
{
  const char* format_type_str = is_raw_ ? "raw" : "dict";
  TPPRINT("TOTAL COST[encode=%s compress=%s full_trans=%s bit_packing=%s round=%ld with_null=%s]", format_type_str,
    need_compress_ ? "true" : "false",
    need_cs_full_transform_ ? "true" : "false",
    is_bit_packing_ ? "true" : "false",
    execute_round_,
    with_null_data_ ? "true" : "false");
  TPPRINT("%-10s %-15s %-15s %-10s", "", "pax", "cs", "diff");

  bool is_positive = true;
  double diff = 0;
  int64_t pax_avg_filter = filter_time_ctx_.get_total_cost(0) / execute_round_;
  int64_t cs_avg_filter = filter_time_ctx_.get_total_cost(1) / execute_round_;
  calc_diff(pax_avg_filter, cs_avg_filter, is_positive, diff);
  TPPRINT("%-10s %-15ld %-15ld %s%.2lf%s", "filter", pax_avg_filter, cs_avg_filter, is_positive ? "+" : "-", diff, "%");
  int64_t pax_avg_decomp = decoder_time_ctx_.get_decompress_cost(0) / execute_round_;
  int64_t cs_avg_decomp = decoder_time_ctx_.get_decompress_cost(1) / execute_round_;
  calc_diff(pax_avg_decomp, cs_avg_decomp, is_positive, diff);
  TPPRINT("%-10s %-15ld %-15ld %s%.2lf%s", "decomp", pax_avg_decomp, cs_avg_decomp, is_positive ? "+" : "-", diff, "%");
  int64_t pax_avg_decoder = decoder_time_ctx_.get_total_cost(0) / execute_round_;
  int64_t cs_avg_decoder = decoder_time_ctx_.get_total_cost(1) / execute_round_;
  calc_diff(pax_avg_decoder, cs_avg_decoder, is_positive, diff);
  TPPRINT("%-10s %-15ld %-15ld %s%.2lf%s", "decoder", pax_avg_decoder, cs_avg_decoder, is_positive ? "+" : "-", diff, "%");
}

void TestPerfTimeCtx::print_detail_report()
{
  const char* format_type_str = is_raw_ ? "raw" : "dict";
  TPPRINT("DETAIL COST[encode=%s compress=%s]", format_type_str, need_compress_ ? "true" : "false");
  TPPRINT("%-10s %-15s %-15s %-15s %-15s %-15s %-15s", "", "pax-integer", "cs-integer", "pax-string", "cs-string",
    "pax-decode", "cs-decode");

  TPPRINT("%-10s %-15ld %-15ld %-15ld %-15ld %-15ld %ld", "nu_nn", filter_time_ctx_.cost_arr_[0][0][0],
    filter_time_ctx_.cost_arr_[1][0][0], filter_time_ctx_.cost_arr_[0][1][0], filter_time_ctx_.cost_arr_[1][1][0],
    decoder_time_ctx_.cost_arr_[0][0], decoder_time_ctx_.cost_arr_[1][0]);
  TPPRINT("%-10s %-15ld %-15ld %-15ld %-15ld %-15ld %ld", "eq_ne", filter_time_ctx_.cost_arr_[0][0][1],
    filter_time_ctx_.cost_arr_[1][0][1], filter_time_ctx_.cost_arr_[0][1][1], filter_time_ctx_.cost_arr_[1][1][1],
    decoder_time_ctx_.cost_arr_[0][1], decoder_time_ctx_.cost_arr_[1][1]);
  TPPRINT("%-10s %-15ld %-15ld %-15ld %-15ld %-15ld %ld", "cmp", filter_time_ctx_.cost_arr_[0][0][2],
    filter_time_ctx_.cost_arr_[1][0][2], filter_time_ctx_.cost_arr_[0][1][2], filter_time_ctx_.cost_arr_[1][1][2],
    decoder_time_ctx_.cost_arr_[0][2], decoder_time_ctx_.cost_arr_[1][2]);
  TPPRINT("%-10s %-15ld %-15ld %-15ld %-15ld %-15ld %ld", "in_bt", filter_time_ctx_.cost_arr_[0][0][3],
    filter_time_ctx_.cost_arr_[1][0][3], filter_time_ctx_.cost_arr_[0][1][3], filter_time_ctx_.cost_arr_[1][1][3],
    decoder_time_ctx_.cost_arr_[0][3], decoder_time_ctx_.cost_arr_[1][3]);
}

void TestPerfTimeCtx::calc_diff(
    const int64_t l_val,
    const int64_t r_val,
    bool &is_positive,
    double &diff)
{
  is_positive = true;
  if (l_val == r_val || l_val == 0) {
    diff = 0;
  } else {
    if (l_val < r_val) {
      is_positive = false;
    }
    double val_diff = (double)(std::max(l_val, r_val) - std::min(l_val, r_val));
    diff = val_diff / (double)(l_val);
    diff *= 100;
  }
}

struct TestEncodeDecodeParam
{
  int64_t start_row_idx_;
  int64_t end_row_idx_;
  bool is_null_;
  int64_t seed_val_;
  TestEncodeDecodeParam()
    : start_row_idx_(0), end_row_idx_(0), is_null_(false), seed_val_(0)
  {}

  TestEncodeDecodeParam(const int64_t start_row_idx, const int64_t end_row_idx,
    const bool is_null, const int64_t seed_val)
    : start_row_idx_(start_row_idx), end_row_idx_(end_row_idx), is_null_(is_null),
      seed_val_(seed_val)
  {}

  int64_t get_row_count() const
  {
    return end_row_idx_ - start_row_idx_;
  }

  TO_STRING_KV(K_(start_row_idx), K_(end_row_idx), K_(is_null), K_(seed_val));
};

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

// We declared 'encoder' & 'cs_encoder', to check that in these two situations,
// the perf of 'pushdown filter' & decode. We can build a result comparing these two.
class TestDecoderFilterPerf : public ::testing::Test
{
public:
  static const int64_t ROWKEY_CNT = 1;
  static const int64_t COLUMN_CNT = ObExtendType - 1 + 7; // ObExtendType = 25
  static const int64_t ROW_CNT = 64;
  static const int64_t SIMPLE_COLUMN_CNT = 3; // if set as 1, means we only check one column
  static const int64_t SIMPLE_ROW_CNT = 2500;

  TestDecoderFilterPerf()
    : check_all_type_(true), is_raw_encoder_(true), need_compress_(false), need_decode_(false),
      need_cs_full_transform_(false), is_bit_packing_(true), tenant_ctx_(500), execute_round_(1),
      highest_data_pct_(0), with_null_data_(false)
  {}

  virtual ~TestDecoderFilterPerf() {}

  virtual void SetUp();
  virtual void TearDown();

  void reset();
  void basic_reuse();

  inline void gen_obj(ObObj& obj, int64_t column_idx, int64_t seed);

protected:
  int prepare(const bool is_raw_encoder, const bool need_compress, const bool need_decode,
    const bool need_cs_full_transform, const bool is_bit_packing = true, const bool check_all_type = true,
    const int64_t execute_round = 1, const int64_t highest_data_pct = 0, const bool with_null_data = false);

  void basic_filter_pushdown_eqne_nunn_op_test(const bool is_column_store);
  void eqne_nunn_op_test_for_all(const bool is_column_store);
  void eqne_nunn_op_test_for_simple(const bool is_column_store);
  void eqne_nunn_op_test_for_general(const bool is_column_store);
  void basic_filter_pushdown_comp_op_test(const bool is_column_store);
  void cmp_op_test_for_all(const bool is_column_store);
  void cmp_op_test_for_simple(const bool is_column_store);
  void cmp_op_test_for_general(const bool is_column_store);
  void basic_filter_pushdown_in_op_test(const bool is_column_store);
  void in_op_test_for_all(const bool is_column_store);
  void in_op_test_for_simple(const bool is_column_store);
  void in_op_test_for_general(const bool is_column_store);
  void basic_filter_pushdown_bt_op_test(const bool is_column_store);
  void bt_op_test_for_all(const bool is_column_store);
  void bt_op_test_for_simple(const bool is_column_store);
  void bt_op_test_for_general(const bool is_column_store);
  void init_filter(sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf, sql::ObExpr **expr_p_buf, ObDatum *datums, void *datum_buf);
  void init_in_filter(sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf, sql::ObExpr **expr_p_buf, ObDatum *datums, void *datum_buf);
  int test_filter_pushdown(const bool is_column_store, const uint64_t col_idx,
    ObIMicroBlockDecoder *decoder, sql::ObPushdownWhiteFilterNode &filter_node,
    const int64_t row_start, const int64_t row_count, common::ObBitmap &result_bitmap,
    common::ObFixedArray<ObObj, ObIAllocator> &objs, uint64_t &filter_cost_ns);

private:
  void gen_default_column_type();
  void gen_simple_column_type();
  void build_table_schema(ObTableSchema &table_schema, const bool is_column_store);
  int add_column_into_table(ObTableSchema &table_schema);

  void init_encoding_ctx(ObMicroBlockEncodingCtx &ctx, const bool is_column_store);
  void gen_null_row(const bool is_column_store, ObDatumRow &null_row);
  bool is_string_type_column(const int64_t column_idx);

  int64_t calc_result_count(const ObArray<TestEncodeDecodeParam> &param_arr, const sql::ObWhiteFilterOperatorType &op_type);

protected:
  bool check_all_type_; // TRUE: check all 31 column types; FALSE: only check several column types
  bool is_raw_encoder_;  // TRUE: raw; FALSE: dict
  bool need_compress_;

  bool need_decode_;
  bool need_cs_full_transform_;
  bool is_bit_packing_;

  ObMicroBlockRawEncoder raw_encoder_; // for pax, raw encoding
  ObMicroBlockEncoder encoder_;
  ObMicroBlockCSEncoder cs_encoder_;
  ObRowGenerate row_generate_;
  ObRowGenerate cs_row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  ObMicroBlockEncodingCtx cs_ctx_;
  MockObTableReadInfo read_info_;
  MockObTableReadInfo cs_read_info_;
  ObArenaAllocator allocator_;
  ObArenaAllocator cs_allocator_;
  ObArenaAllocator common_allocator_;
  share::ObTenantBase tenant_ctx_;
  ObDecodeResourcePool *decode_res_pool_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  common::ObArray<share::schema::ObColDesc> cs_col_descs_;
  int64_t rowkey_cnt_;
  int64_t extra_rowkey_cnt_;
  int64_t column_cnt_;
  int64_t full_column_cnt_;
  ObObjType *col_obj_types_;
  TestPerfTimeCtx perf_ctx_;
  int64_t execute_round_;
  int64_t highest_data_pct_;
  bool with_null_data_;
};

void TestDecoderFilterPerf::SetUp()
{
  decode_res_pool_ = new(common_allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool;
  tenant_ctx_.set(decode_res_pool_);
  share::ObTenantEnv::set_tenant(&tenant_ctx_);
  encoder_.data_buffer_.allocator_.set_tenant_id(500);
  encoder_.row_buf_holder_.allocator_.set_tenant_id(500);
  cs_encoder_.data_buffer_.allocator_.set_tenant_id(500);
  cs_encoder_.row_buf_holder_.allocator_.set_tenant_id(500);
  cs_encoder_.all_string_data_buffer_.allocator_.set_tenant_id(500);
  raw_encoder_.data_buffer_.allocator_.set_tenant_id(500);
  raw_encoder_.row_buf_holder_.allocator_.set_tenant_id(500);
  decode_res_pool_->init();
}

void TestDecoderFilterPerf::TearDown()
{
  reset();
}

int TestDecoderFilterPerf::prepare(
    const bool is_raw_encoder,
    const bool need_compress,
    const bool need_decode,
    const bool need_cs_full_transform,
    const bool is_bit_packing,
    const bool check_all_type,
    const int64_t execute_round,
    const int64_t highest_data_pct,
    const bool with_null_data)
{
  int ret = OB_SUCCESS;
  check_all_type_ = check_all_type;
  is_raw_encoder_ = is_raw_encoder;
  need_compress_ = need_compress;
  need_decode_ = need_decode;
  need_cs_full_transform_ = need_cs_full_transform;
  is_bit_packing_ = is_bit_packing;
  execute_round_ = execute_round;
  highest_data_pct_ = highest_data_pct;
  with_null_data_ = with_null_data;
  perf_ctx_.is_raw_ = is_raw_encoder;
  perf_ctx_.need_compress_ = need_compress;
  perf_ctx_.need_cs_full_transform_ = need_cs_full_transform;
  perf_ctx_.is_bit_packing_ = is_bit_packing;
  perf_ctx_.execute_round_ = execute_round;
  perf_ctx_.with_null_data_ = with_null_data;

  if (check_all_type_) {
    gen_default_column_type();
  } else {
    gen_simple_column_type();
  }
  extra_rowkey_cnt_ = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  full_column_cnt_ = column_cnt_;
  const bool is_multi_version = check_all_type;
  if (is_multi_version) {
    full_column_cnt_ += extra_rowkey_cnt_;
  }

  ObTableSchema pax_table;
  build_table_schema(pax_table, false/*is_column_store*/);
  ObTableSchema cs_table;
  build_table_schema(cs_table, true/*is_column_store*/);

  if (OB_FAIL(add_column_into_table(pax_table))) {
    STORAGE_LOG(WARN, "fail to add_column_into_table", KR(ret));
  } else if (OB_FAIL(add_column_into_table(cs_table))) {
    STORAGE_LOG(WARN, "fail to add_column_into_table", KR(ret));
  } else if (OB_FAIL(row_generate_.init(pax_table, is_multi_version))) {
    STORAGE_LOG(WARN, "fail to init row_generate", KR(ret));
  } else if (is_multi_version && OB_FAIL(pax_table.get_multi_version_column_descs(col_descs_))) {
    STORAGE_LOG(WARN, "fail to get column ids", KR(ret));
  } else if (!is_multi_version && OB_FAIL(row_generate_.get_schema().get_column_ids(col_descs_))) {
    STORAGE_LOG(WARN, "fail to get column ids", KR(ret));
  } else if (OB_FAIL(read_info_.init(allocator_, pax_table.get_column_count(),
            pax_table.get_rowkey_column_num(), lib::is_oracle_mode(), col_descs_))) {
    STORAGE_LOG(WARN, "fail to init read info", KR(ret));
  } else if (OB_FAIL(cs_row_generate_.init(cs_table, is_multi_version))) {
    STORAGE_LOG(WARN, "fail to init row_generate", KR(ret));
  } else if (is_multi_version && OB_FAIL(cs_table.get_multi_version_column_descs(cs_col_descs_))) {
    STORAGE_LOG(WARN, "fail to get column ids", KR(ret));
  } else if (!is_multi_version && OB_FAIL(cs_row_generate_.get_schema().get_column_ids(cs_col_descs_))) {
    STORAGE_LOG(WARN, "fail to get column ids", KR(ret));
  } else if (OB_FAIL(cs_read_info_.init(cs_allocator_, cs_table.get_column_count(),
            cs_table.get_rowkey_column_num(), lib::is_oracle_mode(), cs_col_descs_))) {
    STORAGE_LOG(WARN, "fail to init read info", KR(ret));
  } else {
    STORAGE_LOG(INFO, "read info", K(read_info_), K(cs_read_info_));
    init_encoding_ctx(ctx_, false/*is_cs*/);
    init_encoding_ctx(cs_ctx_, true/*is_cs*/);
    if (OB_FAIL(cs_encoder_.init(cs_ctx_))) {
      STORAGE_LOG(WARN, "fail to init cs_encoder", KR(ret));
    } else if (is_raw_encoder && OB_FAIL(raw_encoder_.init(ctx_))) {
      STORAGE_LOG(WARN, "fail to init raw_encoder", KR(ret));
    } else if (!is_raw_encoder && OB_FAIL(encoder_.init(ctx_))) {
      STORAGE_LOG(WARN, "fail to init encoder", KR(ret));
    }
  }

  return ret;
}

void TestDecoderFilterPerf::reset()
{
  tenant_ctx_.destroy();
  if (OB_NOT_NULL(col_obj_types_)) {
    common_allocator_.free(col_obj_types_);
  }
  if (OB_NOT_NULL(ctx_.column_encodings_)) {
    allocator_.free(ctx_.column_encodings_);
  }
  if (OB_NOT_NULL(cs_ctx_.column_encodings_)) {
    cs_allocator_.free(cs_ctx_.column_encodings_);
  }
  if (OB_NOT_NULL(decode_res_pool_)) {
    common_allocator_.free(decode_res_pool_);
    decode_res_pool_ = nullptr;
  }
  is_raw_encoder_ = true;
  need_compress_ = false;
  need_decode_ = false;
  need_cs_full_transform_ = false;
  basic_reuse();
  read_info_.reset();
  cs_read_info_.reset();
  row_generate_.reset();
  cs_row_generate_.reset();
  allocator_.reset();
  cs_allocator_.reset();
  common_allocator_.reset();
  col_descs_.reset();
  cs_col_descs_.reset();
  perf_ctx_.reset();
}

void TestDecoderFilterPerf::basic_reuse()
{
  raw_encoder_.reuse();
  encoder_.reuse();
  cs_encoder_.reuse();
}

inline void TestDecoderFilterPerf::gen_obj(ObObj& obj, int64_t column_idx, int64_t seed)
{
  obj.copy_meta_type(row_generate_.column_list_.at(column_idx).col_type_);
  ObObjType column_type = row_generate_.column_list_.at(column_idx).col_type_.get_type();
  row_generate_.set_obj(column_type, row_generate_.column_list_.at(column_idx).col_id_, seed, obj, 0);
  if ( ObVarcharType == column_type || ObCharType == column_type || ObHexStringType == column_type
      || ObNVarchar2Type == column_type || ObNCharType == column_type || ObTextType == column_type){
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    obj.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    obj.set_collation_type(CS_TYPE_BINARY);
    obj.set_collation_level(CS_LEVEL_NUMERIC);
  }
}

void TestDecoderFilterPerf::gen_default_column_type()
{
  if (OB_NOT_NULL(col_obj_types_)) {
    common_allocator_.free(col_obj_types_);
  }
  column_cnt_ = COLUMN_CNT;
  rowkey_cnt_ = 2;
  col_obj_types_ = reinterpret_cast<ObObjType *>(common_allocator_.alloc(sizeof(ObObjType) * column_cnt_));
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
    } /*else if (type == ObExtendType || type == ObUnknownType) {
      type = ObVarcharType;
    }*/
    col_obj_types_[i] = type;
  }
}

void TestDecoderFilterPerf::gen_simple_column_type()
{
  // Only 3 columns: ObInt32Type(row_key), ObIntType, ObVarcharType
  if (OB_NOT_NULL(col_obj_types_)) {
    common_allocator_.free(col_obj_types_);
  }
  column_cnt_ = SIMPLE_COLUMN_CNT;
  rowkey_cnt_ = 1;
  col_obj_types_ = reinterpret_cast<ObObjType *>(common_allocator_.alloc(sizeof(ObObjType) * SIMPLE_COLUMN_CNT));
  for (int64_t i = 0; i < column_cnt_; ++i) {
    if (i == 0) {
      col_obj_types_[0] = ObObjType::ObInt32Type;
    } else if (i == 1) {
      col_obj_types_[1] = ObObjType::ObIntType;
    } else {
      col_obj_types_[2] = ObObjType::ObVarcharType;
    }
  }
}

void TestDecoderFilterPerf::gen_null_row(const bool is_column_store, ObDatumRow &null_row)
{
  ObArenaAllocator &tmp_allocator = is_column_store ? cs_allocator_ : allocator_;
  ASSERT_EQ(OB_SUCCESS, null_row.init(tmp_allocator, full_column_cnt_));
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    null_row.storage_datums_[i].set_null();
  }
}

bool TestDecoderFilterPerf::is_string_type_column(const int64_t column_idx)
{
  bool bool_ret = false;
  if (check_all_type_) {
    //ObNumberType & ObUNumberType & ObVarcharType & ObCharType & ObHexStringType & ObRawType & ObURowIDType
    //ObTimestampTZType & ObTimestampLTZType & ObTimestampNanoType & ObIntervalDSType
    //
    // TODO @donglou.zl check ObIntervalYMType
    if ((column_idx >= 23 && column_idx <= 32) || (column_idx == 16 || column_idx == 17)) {
      bool_ret = true;
    }
  } else {
    if ((column_idx > 0) && (column_idx == full_column_cnt_ - 1)) {
      bool_ret = true;
    }
  }

  return bool_ret;
}

void TestDecoderFilterPerf::build_table_schema(
    ObTableSchema &table_schema, const bool is_column_store)
{
  table_schema.reset();
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  uint64_t tid = is_column_store ? 200001 : 200002;
  table_schema.set_table_id(tid);
  const char *tb_name = is_column_store ? "test_cs_decoder_schema" : "test_decoder_schema";
  table_schema.set_table_name(tb_name);
  table_schema.set_rowkey_column_num(rowkey_cnt_);
  table_schema.set_max_column_id(column_cnt_ * 2);
  table_schema.set_block_size(2 * 1024);
  const char *comp_func = need_compress_ ? "zstd_1.3.8" : "none";
  table_schema.set_compress_func_name(comp_func);
  ObRowStoreType row_store_type = is_column_store ? CS_ENCODING_ROW_STORE :
    (is_bit_packing_ ? ENCODING_ROW_STORE/*dynamic*/ : SELECTIVE_ENCODING_ROW_STORE/*condensed*/);
  table_schema.set_row_store_type(row_store_type);
  table_schema.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
}

int TestDecoderFilterPerf::add_column_into_table(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 col;
  ObSqlString str;
  const uint64_t tid = table_schema.get_table_id();
  for (int64_t i = 0; OB_SUCC(ret) && (i < column_cnt_); ++i) {
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
    if (i < rowkey_cnt_) {
      col.set_rowkey_position(i + 1);
    } else{
      col.set_rowkey_position(0);
    }
    if (OB_FAIL(table_schema.add_column(col))) {
      STORAGE_LOG(WARN, "fail to add column", KR(ret), K(i), K(col));
    }
  }
  return ret;
}

void TestDecoderFilterPerf::init_encoding_ctx(
    ObMicroBlockEncodingCtx &ctx,
    const bool is_column_store)
{
  ObArenaAllocator &cur_allocator = is_column_store ? cs_allocator_ : allocator_;
  ctx.micro_block_size_ = 64L << 11; // 1L << 20, 1MB, maximum micro block size;
  ctx.macro_block_size_ = 2L << 20;
  ctx.rowkey_column_cnt_ = rowkey_cnt_;
  ctx.column_cnt_ = column_cnt_;
  if (check_all_type_) {
    ctx.rowkey_column_cnt_ += extra_rowkey_cnt_;
    ctx.column_cnt_ += extra_rowkey_cnt_;
  }
  ctx.col_descs_ = is_column_store ? &cs_col_descs_ : &col_descs_;
  ctx.major_working_cluster_version_ = cal_version(4, 1, 0, 0);
  ctx.row_store_type_ = is_column_store ? CS_ENCODING_ROW_STORE : (is_bit_packing_ ? ENCODING_ROW_STORE : SELECTIVE_ENCODING_ROW_STORE);
  if (!is_column_store) {
    ctx.encoder_opt_.enable_bit_packing_ = is_bit_packing_;
  }
  ctx.compressor_type_ = need_compress_ ? common::ObCompressorType::ZSTD_1_3_8_COMPRESSOR
                         : common::ObCompressorType::NONE_COMPRESSOR;
  // ctx.need_calc_column_chksum_ = true;
  ctx.column_encodings_ = reinterpret_cast<int64_t *>(cur_allocator.alloc(sizeof(int64_t) * column_cnt_));
  for (int64_t i = 0; i < ctx.column_cnt_; ++i) {
     ObObjType obj_type = is_column_store ? cs_col_descs_[i].col_type_.get_type() : col_descs_[i].col_type_.get_type();
     const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(obj_type)];
     uint8_t cs_encoding_type = 0;
     if (ObCSEncodingUtil::is_integer_store_class(store_class)) {
       cs_encoding_type = ObCSColumnHeader::Type::INT_DICT; // sepcfiy dict column encoding
     } else {
       cs_encoding_type = ObCSColumnHeader::Type::STR_DICT; // sepcfiy dict column encoding
     }

    if (check_all_type_ && i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      if (is_raw_encoder_) {
        ctx.column_encodings_[i] = is_column_store ? ObCSColumnHeader::Type::INTEGER : ObColumnHeader::Type::RAW;
      } else {
        ctx.column_encodings_[i] = is_column_store ? cs_encoding_type : ObColumnHeader::Type::RAW;
      }
      continue;
    }
    if (is_column_store) {
      if (!is_raw_encoder_) {
        ctx.column_encodings_[i] = cs_encoding_type;
      } else if (is_string_type_column(i)) {
        ctx.column_encodings_[i] = ObCSColumnHeader::Type::STRING;
      } else {
        ctx.column_encodings_[i] = ObCSColumnHeader::Type::INTEGER;
      }
    } else {
      if (is_raw_encoder_) {
        ctx.column_encodings_[i] = ObColumnHeader::Type::RAW;
      } else {
        if (highest_data_pct_ >= 100) {
          ctx.column_encodings_[i] = ObColumnHeader::Type::CONST;
        } else {
          ctx.column_encodings_[i] = ObColumnHeader::Type::DICT;
        }
      }
    }
  }
}

#define TEST_FILTER_PUSHDOWN_OP(op_type, filter, res_cnt, col_idx) \
  filter.op_type_ = op_type; \
  for (int64_t r = 0; r < execute_round_; ++r) { \
    result_bitmap.reuse(); \
    filter_cost_ns = 0; \
    ASSERT_EQ(0, result_bitmap.popcnt()); \
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(is_column_store, col_idx, cur_decoder, filter, row_start, row_count, result_bitmap, objs, filter_cost_ns)); \
    ASSERT_EQ(res_cnt, result_bitmap.popcnt())<< "i: " << i << "; is_column_store: " << is_column_store << std::endl; \
    if (!is_first_check) { \
      perf_ctx_.filter_time_ctx_.add_filter_cost(is_column_store, is_string_type_column(i), op_type, filter_cost_ns); \
    } \
  } \

#define TEST_FILTER_PUSHDOWN_OP_1(op_type, filter, res_cnt, col_idx, ref_obj, seed_val) \
  gen_obj(ref_obj, col_idx, seed_val); \
  objs.push_back(ref_obj); \
  TEST_FILTER_PUSHDOWN_OP(op_type, filter, res_cnt, col_idx); \
  objs.clear(); \

#define TEST_FILTER_PUSHDOWN_OP_2(op_type, filter, res_cnt, col_idx, ref_obj1, seed_val1, ref_obj2, seed_val2) \
  gen_obj(ref_obj1, col_idx, seed_val1); \
  gen_obj(ref_obj2, col_idx, seed_val2); \
  objs.push_back(ref_obj1); \
  objs.push_back(ref_obj2); \
  TEST_FILTER_PUSHDOWN_OP(op_type, filter, res_cnt, col_idx); \
  objs.clear(); \

#define TEST_INIT_ROW() \
  ObIMicroBlockWriter *cur_encoder = nullptr; \
  ObRowGenerate *cur_row_gen = nullptr; \
  ObArenaAllocator &cur_allocator = is_column_store ? cs_allocator_ : allocator_; \
  if (is_column_store) { \
    cur_encoder = &cs_encoder_; \
    cur_row_gen = &cs_row_generate_; \
  } else { \
    cur_row_gen = &row_generate_; \
    if (is_raw_encoder_) { \
      cur_encoder = &raw_encoder_; \
    } else { \
      cur_encoder = &encoder_; \
    } \
  } \
  ASSERT_NE(nullptr, cur_encoder); \
  ASSERT_NE(nullptr, cur_row_gen); \
  ObDatumRow null_row; \
  gen_null_row(is_column_store, null_row); \
  ObDatumRow row; \
  ASSERT_EQ(OB_SUCCESS, row.init(cur_allocator, full_column_cnt_)); \

#define TEST_BUILD_FILTER_DECODER() \
  ObMicroBlockData data; \
  ObMicroBlockDecoder decoder; \
  ObMicroBlockCSDecoder cs_decoder; \
  ObIMicroBlockDecoder *cur_decoder = nullptr; \
  ObMicroBlockDesc micro_block_desc; \
  char* buf = NULL; \
  int64_t size = 0; \
  if (is_column_store) { \
    ASSERT_EQ(OB_SUCCESS, cs_encoder_.build_micro_block_desc(micro_block_desc)); \
    if (need_cs_full_transform_) { \
      int64_t pos = 0; \
      ObMicroBlockHeader *header = const_cast<ObMicroBlockHeader *>(micro_block_desc.header_); \
      header->data_length_ = micro_block_desc.buf_size_; \
      header->data_zlength_ = micro_block_desc.buf_size_; \
      header->data_checksum_ = ob_crc64_sse42(0, micro_block_desc.buf_, micro_block_desc.buf_size_); \
      header->original_length_ = micro_block_desc.original_size_; \
      header->set_header_checksum(); \
      ObCSMicroBlockTransformer tansformer; \
      ASSERT_EQ(OB_SUCCESS, tansformer.init(header, micro_block_desc.buf_, micro_block_desc.buf_size_)); \
      ASSERT_EQ(OB_SUCCESS, tansformer.calc_full_transform_size(size)); \
      buf = static_cast<char *>(cur_allocator.alloc(size)); \
      ASSERT_TRUE(buf != nullptr); \
      int64_t round = execute_round_; \
      while (round > 0) { \
        pos = 0; \
        MEMSET(buf, '\0', size); \
        int64_t trans_start_ns = ObTimeUtility::current_time_ns(); \
        int ret = tansformer.full_transform(buf, size, pos); \
        int64_t trans_cost_ns = ObTimeUtility::current_time_ns() - trans_start_ns; \
        perf_ctx_.decoder_time_ctx_.add_decode_cost(is_column_store, TestDecoderPerfTimeCtx::DECOMP_IDX, trans_cost_ns); \
        ASSERT_EQ(OB_SUCCESS, ret); \
        ASSERT_EQ(size, pos); \
        --round; \
      } \
      data.buf_ = buf; \
      data.size_ = size; \
    } else { \
      data.buf_ = micro_block_desc.buf_ - micro_block_desc.header_->header_size_; \
      data.size_ = micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_; \
    } \
    ASSERT_EQ(OB_SUCCESS, cs_decoder.init(data, cs_read_info_)) << "buffer size: " << data.get_buf_size() << std::endl; \
    cur_decoder = &cs_decoder; \
  } else { \
    if (is_raw_encoder_) { \
      ASSERT_EQ(OB_SUCCESS, raw_encoder_.build_block(buf, size)); \
    } else { \
      ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size)); \
    } \
    if (need_compress_) { \
      ObMicroBlockCompressor compressor; \
      int ret = compressor.init(ctx_.micro_block_size_, ctx_.compressor_type_); \
      ASSERT_EQ(OB_SUCCESS, ret); \
      const char *compress_buf; \
      int64_t compress_buf_size = 0; \
      ret = compressor.compress(buf, size, compress_buf, compress_buf_size); \
      ASSERT_EQ(OB_SUCCESS, ret); \
      ASSERT_NE(nullptr, compress_buf); \
      int64_t round = execute_round_; \
      while (round > 0) { \
        const char *decompress_buf; \
        int64_t decompress_buf_size = 0; \
        int64_t decomp_start_ns = ObTimeUtility::current_time_ns(); \
        ret = compressor.decompress(compress_buf, compress_buf_size, size, decompress_buf, decompress_buf_size); \
        int64_t decomp_cost_ns = ObTimeUtility::current_time_ns() - decomp_start_ns; \
        ASSERT_EQ(OB_SUCCESS, ret); \
        ASSERT_EQ(size, decompress_buf_size); \
        perf_ctx_.decoder_time_ctx_.add_decode_cost(is_column_store, TestDecoderPerfTimeCtx::DECOMP_IDX, decomp_cost_ns); \
        --round; \
      } \
    } \
    if (is_raw_encoder_) { \
      data.buf_ = raw_encoder_.data_buffer_.data(); \
      data.size_ = raw_encoder_.data_buffer_.length(); \
    } else { \
      data.buf_ = encoder_.data_buffer_.data(); \
      data.size_ = encoder_.data_buffer_.length(); \
    } \
    ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl; \
    cur_decoder = &decoder; \
  } \
  ASSERT_NE(nullptr, cur_decoder); \

#define TEST_CHECK_DECODE(row_idx, is_null, seed_val) \
  decode_start_ns = ObTimeUtility::current_time_ns(); \
  ret = cur_decoder->get_row(row_idx, cur_row); \
  decode_cost_ns = ObTimeUtility::current_time_ns() - decode_start_ns; \
  ASSERT_EQ(OB_SUCCESS, ret); \
  perf_ctx_.decoder_time_ctx_.add_decode_cost(is_column_store, op_idx, decode_cost_ns); \
  if (is_null) { \
    ASSERT_EQ(null_row, cur_row) << "row_idx: " << row_idx << std::endl; \
  } else { \
    ASSERT_EQ(OB_SUCCESS, cur_row_gen->get_next_row(seed_val, row)); \
    ASSERT_EQ(row, cur_row) << "row_idx: " << row_idx << std::endl; \
  }

#define TEST_ENCODE_AND_DECODE_PROCEDURE(params) \
  for (int64_t i = 0; i < params.size(); ++i) { \
    for (int64_t j = params.at(i).start_row_idx_; j < params.at(i).end_row_idx_; ++j) { \
      if (params.at(i).is_null_) { \
        ASSERT_EQ(OB_SUCCESS, cur_encoder->append_row(null_row)) << "j: " << j << std::endl; \
      } else { \
        ASSERT_EQ(OB_SUCCESS, cur_row_gen->get_next_row(params.at(i).seed_val_, row)); \
        ASSERT_EQ(OB_SUCCESS, cur_encoder->append_row(row)) << "j: " << j << std::endl; \
      } \
    } \
  } \
  TEST_BUILD_FILTER_DECODER(); \
  if (need_decode_) { \
    int ret = OB_SUCCESS; \
    int64_t decode_start_ns = 0; \
    int64_t decode_cost_ns = 0; \
    ObDatumRow cur_row; \
    ASSERT_EQ(OB_SUCCESS, cur_row.init(cur_allocator, full_column_cnt_)); \
    if (check_all_type_) { \
      int64_t round = execute_round_; \
      while (round > 0) { \
        for (int64_t i = 0; i < params.size(); ++i) { \
          for (int64_t j = params.at(i).start_row_idx_; j < params.at(i).end_row_idx_; ++j) { \
            if (params.at(i).is_null_) { \
              TEST_CHECK_DECODE(j, true, 0); \
            } else { \
              TEST_CHECK_DECODE(j, false, params.at(i).seed_val_); \
            } \
          } \
        } \
        --round; \
      } \
    } else { \
      const int64_t cur_column_cnt = full_column_cnt_; \
      int32_t row_ids[SIMPLE_ROW_CNT]; \
      for (int32_t i = 0; i < SIMPLE_ROW_CNT; ++i) { \
        row_ids[i] = i; \
      } \
      ObSEArray<int32_t, OB_DEFAULT_SE_ARRAY_COUNT> cols; \
      ObSEArray<const share::schema::ObColumnParam *, 16> col_params; \
      const share::schema::ObColumnParam *param = nullptr; \
      const char *cell_datas[SIMPLE_ROW_CNT]; \
      int64_t single_ptr_buf_len = 8; \
      char *datum_ptr_buf = reinterpret_cast<char *>(cur_allocator.alloc(SIMPLE_ROW_CNT * cur_column_cnt * sizeof(char) * single_ptr_buf_len)); \
      ObDatum *datum_buf = new ObDatum[SIMPLE_ROW_CNT * cur_column_cnt]; \
      ObSEArray<ObSqlDatumInfo, 16> datum_arr; \
      void *expr_arr = cur_allocator.alloc(sizeof(sql::ObExpr) * cur_column_cnt); \
      sql::ObExpr *exprs = reinterpret_cast<sql::ObExpr *>(expr_arr); \
      for (int64_t i = 0; i < cur_column_cnt; ++i) { \
        ASSERT_EQ(OB_SUCCESS, datum_arr.push_back(ObSqlDatumInfo())); \
        datum_arr.at(i).datum_ptr_ = datum_buf + SIMPLE_ROW_CNT * i; \
        if (i == cur_column_cnt - 1) { \
          exprs[i].obj_datum_map_ = ObObjDatumMapType::OBJ_DATUM_STRING; \
          datum_arr.at(i).expr_ = exprs + i; \
        } else { \
          exprs[i].obj_datum_map_ = ObObjDatumMapType::OBJ_DATUM_NUMBER; \
          datum_arr.at(i).expr_ = exprs + i; \
        } \
        cols.push_back(i); \
        col_params.push_back(param); \
        for (int64_t j = 0; j < SIMPLE_ROW_CNT; ++j) { \
          datum_arr.at(i).datum_ptr_[j].ptr_ = reinterpret_cast<char *>(&datum_ptr_buf[single_ptr_buf_len * (SIMPLE_ROW_CNT * i + j)]); \
        } \
      } \
      ASSERT_EQ(true, cur_decoder != nullptr); \
      int64_t round = execute_round_; \
      while (round > 0) { \
        decode_start_ns = ObTimeUtility::current_time_ns(); \
        ret = cur_decoder->get_rows(cols, col_params, row_ids, cell_datas, SIMPLE_ROW_CNT, datum_arr); \
        decode_cost_ns = ObTimeUtility::current_time_ns() - decode_start_ns; \
        ASSERT_EQ(OB_SUCCESS, ret); \
        perf_ctx_.decoder_time_ctx_.add_decode_cost(is_column_store, op_idx, decode_cost_ns); \
        for (int64_t i = 0; i < param_arr.size(); ++i) { \
          ASSERT_EQ(OB_SUCCESS, cur_row_gen->get_next_row(param_arr.at(i).seed_val_, row)); \
          for (int64_t j = param_arr.at(i).start_row_idx_; j < param_arr.at(i).end_row_idx_; ++j) { \
            if (param_arr.at(i).is_null_) { \
              for (int64_t x = 0; x < cur_column_cnt; ++x) { \
                ASSERT_EQ(true, datum_arr.at(x).datum_ptr_[j].is_null()); \
              } \
            } else { \
              for (int64_t x = 0; x < cur_column_cnt; ++x) { \
                if (x == 0) { \
                  ASSERT_EQ(row.storage_datums_[x].get_int32(), datum_arr.at(x).datum_ptr_[j].get_int32()) << "i: " << i << ", j: " << j << std::endl; \
                } else if (x == 1) { \
                  ASSERT_EQ(row.storage_datums_[x].get_int(), datum_arr.at(x).datum_ptr_[j].get_int()) << "i: " << i << ", j: " << j << std::endl; \
                } else if (x == 2) { \
                  ASSERT_EQ(row.storage_datums_[x].get_string(), datum_arr.at(x).datum_ptr_[j].get_string()) << "i: " << i << ", j: " << j << std::endl; \
                } \
              } \
            } \
          } \
        } \
        --round; \
      } \
    } \
  } \

#define TEST_GENERAL_ENCODE_DECODE() \
  ASSERT_GT(highest_data_pct_, 0); \
  ASSERT_LE(highest_data_pct_, 100); \
  TEST_INIT_ROW(); \
  int64_t seed_cnt = 100 / highest_data_pct_; \
  seed_cnt += (100 % highest_data_pct_ == 0 ? 0 : 1); \
  bool need_check_filter = !((seed_cnt == 1) && with_null_data_ && (op_idx != TestDecoderPerfTimeCtx::EQ_NE_IDX)); \
  int64_t start_seed = 10001; \
  ObArray<TestEncodeDecodeParam> param_arr; \
  int64_t cur_sum_pct = 0; \
  for (int64_t i = 0; (i < seed_cnt) && (cur_sum_pct <= 100); ++i) { \
    int64_t cur_seed_val = start_seed + i; \
    int64_t cur_row_start = SIMPLE_ROW_CNT * cur_sum_pct / 100; \
    int64_t cur_row_cnt = 0; \
    int64_t cur_pct = 0; \
    if (highest_data_pct_ <= (100 - cur_sum_pct)) { \
      cur_pct = highest_data_pct_; \
    } else { \
      cur_pct = 100 - cur_sum_pct; \
    } \
    cur_row_cnt = SIMPLE_ROW_CNT * cur_pct / 100; \
    if (with_null_data_ && (i == seed_cnt - 1)) { \
      TestEncodeDecodeParam tmp_param(cur_row_start, cur_row_start + cur_row_cnt, true, 0); \
      ASSERT_EQ(OB_SUCCESS, param_arr.push_back(tmp_param)); \
    } else { \
      TestEncodeDecodeParam tmp_param(cur_row_start, cur_row_start + cur_row_cnt, false, cur_seed_val); \
      ASSERT_EQ(OB_SUCCESS, param_arr.push_back(tmp_param)); \
    } \
    cur_sum_pct += cur_pct; \
  } \
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr); \

void TestDecoderFilterPerf::basic_filter_pushdown_eqne_nunn_op_test(const bool is_column_store)
{
  if (check_all_type_) {
    eqne_nunn_op_test_for_all(is_column_store);
  } else if (highest_data_pct_ == 0) {
    eqne_nunn_op_test_for_simple(is_column_store);
  } else {
    eqne_nunn_op_test_for_general(is_column_store);
  }
}

void TestDecoderFilterPerf::eqne_nunn_op_test_for_all(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed_1 = 10001;
  int64_t seed_2 = 10002;
  int64_t seed1_count = ROW_CNT - 40;
  int64_t seed2_count = 20;
  int64_t null_count = 20;

  const int64_t op_idx = TestDecoderPerfTimeCtx::EQ_NE_IDX;
  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, seed1_count, false, seed_1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(seed1_count, seed1_count + 10, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TestEncodeDecodeParam param3(ROW_CNT - 30, ROW_CNT - 10, false, seed_2);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param3));
  TestEncodeDecodeParam param4(ROW_CNT - 10, ROW_CNT, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param4));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  const int64_t row_start = 0;
  const int64_t row_count = ROW_CNT; // cur_decoder->row_count_
  uint64_t filter_cost_ns = 0;
  ObBitmap result_bitmap(cur_allocator);
  result_bitmap.init(ROW_CNT);

  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1, ref_obj_2;
    //  0 --- ROW_CNT-40 --- ROW_CNT-30 --- ROW_CNT-10 --- ROW_CNT
    //  |    seed1   |   null   |     seed2     |   null    |
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NU, white_filter, null_count, i, ref_obj_1, seed_1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NN, white_filter, seed1_count + seed2_count, i, ref_obj_1, seed_1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_EQ, white_filter, seed1_count, i, ref_obj_1, seed_1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NE, white_filter, seed2_count, i, ref_obj_1, seed_1);

    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_EQ, white_filter, seed2_count, i, ref_obj_2, seed_2);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NE, white_filter, seed1_count, i, ref_obj_2, seed_2);
  }
}

void TestDecoderFilterPerf::eqne_nunn_op_test_for_simple(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed_1 = 10001;
  int64_t seed_2 = 10002;
  int64_t seed1_count = SIMPLE_ROW_CNT * 3 / 10;
  int64_t seed2_count = SIMPLE_ROW_CNT * 3 / 10;
  int64_t null_count = SIMPLE_ROW_CNT * 4 / 10;

  const int64_t op_idx = TestDecoderPerfTimeCtx::EQ_NE_IDX;
  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, seed1_count, false, seed_1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(seed1_count, seed1_count + null_count / 2, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TestEncodeDecodeParam param3(seed1_count + null_count / 2, SIMPLE_ROW_CNT - null_count / 2, false, seed_2);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param3));
  TestEncodeDecodeParam param4(SIMPLE_ROW_CNT - null_count / 2, SIMPLE_ROW_CNT, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param4));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  const int64_t row_start = 0;
  const int64_t row_count = SIMPLE_ROW_CNT;
  uint64_t filter_cost_ns = 0;
  ObBitmap result_bitmap(cur_allocator);
  result_bitmap.init(SIMPLE_ROW_CNT);

  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1, ref_obj_2;
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NU, white_filter, null_count, i, ref_obj_1, seed_1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NN, white_filter, seed1_count + seed2_count, i, ref_obj_1, seed_1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_EQ, white_filter, seed1_count, i, ref_obj_1, seed_1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NE, white_filter, seed2_count, i, ref_obj_1, seed_1);

    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_EQ, white_filter, seed2_count, i, ref_obj_2, seed_2);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NE, white_filter, seed1_count, i, ref_obj_2, seed_2);
  }
}

void TestDecoderFilterPerf::eqne_nunn_op_test_for_general(const bool is_column_store)
{
  const int64_t op_idx = TestDecoderPerfTimeCtx::EQ_NE_IDX;
  TEST_GENERAL_ENCODE_DECODE();

  const int64_t row_start = 0;
  const int64_t row_count = SIMPLE_ROW_CNT;
  uint64_t filter_cost_ns = 0;
  ObBitmap result_bitmap(cur_allocator);
  result_bitmap.init(SIMPLE_ROW_CNT);

  bool is_first_check = true;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  for (int64_t i = 0; need_check_filter && (i < full_column_cnt_); ++i) {
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1;
    if (with_null_data_) {
      int64_t nu_cnt = param_arr.at(param_arr.size() - 1).get_row_count();
      if (is_first_check) {
        TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NU, white_filter, nu_cnt, i, ref_obj_1, start_seed);
        is_first_check = false;
      }
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NU, white_filter, nu_cnt, i, ref_obj_1, start_seed);
      int64_t nn_cnt = SIMPLE_ROW_CNT - nu_cnt;
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NN, white_filter, nn_cnt, i, ref_obj_1, start_seed);
    }

    if ((seed_cnt == 1) && with_null_data_) {
    } else {
      int64_t eq_cnt = param_arr.at(0).get_row_count();
      if (is_first_check) {
        TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_EQ, white_filter, eq_cnt, i, ref_obj_1, start_seed);
        is_first_check = false;
      }
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_EQ, white_filter, eq_cnt, i, ref_obj_1, start_seed);
      int64_t ne_cnt = calc_result_count(param_arr, sql::WHITE_OP_NE);
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_NE, white_filter, ne_cnt, i, ref_obj_1, start_seed);
    }
  }
  decoder.reset();
  cs_decoder.reset();
}

void TestDecoderFilterPerf::basic_filter_pushdown_comp_op_test(const bool is_column_store)
{
  if (check_all_type_) {
    cmp_op_test_for_all(is_column_store);
  } else if (highest_data_pct_ == 0) {
    cmp_op_test_for_simple(is_column_store);
  } else {
    cmp_op_test_for_general(is_column_store);
  }
}

void TestDecoderFilterPerf::cmp_op_test_for_all(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  int64_t seed2 = 10002;
  int64_t neg_seed_0 = -10000;
  int64_t seed0_count = ROW_CNT - 30;
  int64_t seed1_count = 10;
  int64_t seed2_count = 10;
  int64_t null_count = 10;
  const int64_t op_idx = TestDecoderPerfTimeCtx::CMP_IDX;

  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, ROW_CNT - 30, false, seed0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(ROW_CNT - 30, ROW_CNT - 20, false, seed1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TestEncodeDecodeParam param3(ROW_CNT - 20, ROW_CNT - 10, false, seed2);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param3));
  TestEncodeDecodeParam param4(ROW_CNT - 10, ROW_CNT, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param4));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  // 0--- ROW_CNT-30 --- ROW_CNT-20 --- ROW_CNT-10 --- ROW_CNT
  // |    seed0   |   seed1   |     seed2     |   null    |
  //           |                                   |
  //       ROW_CNT-35                           ROW_CNT-5
  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1;

    const int64_t row_start = 0;
    const int64_t row_count = ROW_CNT; // cur_decoder->row_count_
    uint64_t filter_cost_ns = 0;
    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GT, white_filter, seed2_count, i, ref_obj_1, seed1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LT, white_filter, seed0_count, i, ref_obj_1, seed1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GE, white_filter, seed1_count + seed2_count, i, ref_obj_1, seed1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LE, white_filter, seed0_count + seed1_count, i, ref_obj_1, seed1);

    if (ob_is_int_tc(row_generate_.column_list_.at(i).col_type_.get_type())) {
      // Test cmp with negative values
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GT, white_filter, ROW_CNT - null_count, i, ref_obj_1, neg_seed_0);
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LT, white_filter, 0, i, ref_obj_1, neg_seed_0);
    }
  }
}

void TestDecoderFilterPerf::cmp_op_test_for_simple(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  int64_t seed2 = 10002;
  int64_t neg_seed_0 = -10000;
  int64_t seed0_count = SIMPLE_ROW_CNT / 5;
  int64_t seed1_count = SIMPLE_ROW_CNT / 5;
  int64_t seed2_count = SIMPLE_ROW_CNT / 5;
  int64_t null_count = SIMPLE_ROW_CNT * 2 / 5;
  const int64_t op_idx = TestDecoderPerfTimeCtx::CMP_IDX;

  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, seed0_count, false, seed0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(seed0_count, seed0_count + seed1_count, false, seed1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TestEncodeDecodeParam param3(seed0_count + seed1_count, SIMPLE_ROW_CNT - null_count, false, seed2);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param3));
  TestEncodeDecodeParam param4(SIMPLE_ROW_CNT - null_count, SIMPLE_ROW_CNT, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param4));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1;

    const int64_t row_start = 0;
    const int64_t row_count = SIMPLE_ROW_CNT;
    uint64_t filter_cost_ns = 0;
    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(SIMPLE_ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GT, white_filter, seed2_count, i, ref_obj_1, seed1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LT, white_filter, seed0_count, i, ref_obj_1, seed1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GE, white_filter, seed1_count + seed2_count, i, ref_obj_1, seed1);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LE, white_filter, seed0_count + seed1_count, i, ref_obj_1, seed1);

    if (ob_is_int_tc(row_generate_.column_list_.at(i).col_type_.get_type())) {
      // Test cmp with negative values
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GT, white_filter, SIMPLE_ROW_CNT - null_count, i, ref_obj_1, neg_seed_0);
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LT, white_filter, 0, i, ref_obj_1, neg_seed_0);
    }
  }
}

void TestDecoderFilterPerf::cmp_op_test_for_general(const bool is_column_store)
{
  const int64_t op_idx = TestDecoderPerfTimeCtx::CMP_IDX;
  TEST_GENERAL_ENCODE_DECODE();

  const int64_t row_start = 0;
  const int64_t row_count = SIMPLE_ROW_CNT;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  int64_t nu_cnt = with_null_data_ ? param_arr.at(param_arr.size() - 1).get_row_count() : 0;
  bool is_first_check = true;
  for (int64_t i = 0; need_check_filter && (i < full_column_cnt_); ++i) {
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1;

    uint64_t filter_cost_ns = 0;
    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(SIMPLE_ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    int64_t gt_cnt = calc_result_count(param_arr, sql::WHITE_OP_GT);
    if (is_first_check) {
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GT, white_filter, gt_cnt, i, ref_obj_1, start_seed);
      is_first_check = false;
    }
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GT, white_filter, gt_cnt, i, ref_obj_1, start_seed);
    int64_t lt_cnt = calc_result_count(param_arr, sql::WHITE_OP_LT);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LT, white_filter, lt_cnt, i, ref_obj_1, start_seed);
    int64_t ge_cnt = calc_result_count(param_arr, sql::WHITE_OP_GE);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GE, white_filter, ge_cnt, i, ref_obj_1, start_seed);
    int64_t le_cnt = calc_result_count(param_arr, sql::WHITE_OP_LE);
    TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LE, white_filter, le_cnt, i, ref_obj_1, start_seed);

    if (ob_is_int_tc(row_generate_.column_list_.at(i).col_type_.get_type())) {
      int64_t neg_seed_val = -10000;
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_GT, white_filter, SIMPLE_ROW_CNT - nu_cnt, i, ref_obj_1, neg_seed_val);
      TEST_FILTER_PUSHDOWN_OP_1(sql::WHITE_OP_LT, white_filter, 0, i, ref_obj_1, neg_seed_val);
    }
  }
}

void TestDecoderFilterPerf::basic_filter_pushdown_in_op_test(const bool is_column_store)
{
  if (check_all_type_) {
    in_op_test_for_all(is_column_store);
  } else if (highest_data_pct_ == 0) {
    in_op_test_for_simple(is_column_store);
  } else {
    in_op_test_for_general(is_column_store);
  }
}

void TestDecoderFilterPerf::in_op_test_for_all(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed0 = 1000;
  int64_t seed1 = 1001;
  int64_t seed2 = 1002;
  int64_t seed3 = 1003;
  int64_t seed5 = 1005;
  int64_t seed0_count = ROW_CNT - 40;
  int64_t seed1_count = 10;
  int64_t seed2_count = 10;
  int64_t seed3_count = 10;
  int64_t null_count = 10;
  int64_t seed5_count = 0;
  const int64_t op_idx = TestDecoderPerfTimeCtx::IN_BT_IDX;

  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, ROW_CNT - 40, false, seed0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(ROW_CNT - 40, ROW_CNT - 30, false, seed1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TestEncodeDecodeParam param3(ROW_CNT - 30, ROW_CNT - 20, false, seed2);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param3));
  TestEncodeDecodeParam param4(ROW_CNT - 20, ROW_CNT - 10, false, seed3);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param4));
  TestEncodeDecodeParam param5(ROW_CNT - 10, ROW_CNT, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param5));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  const int64_t row_start = 0;
  const int64_t row_count = ROW_CNT; // cur_decoder->row_count_
  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  sql::ObPushdownWhiteFilterNode white_filter_2(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }

    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 3);
    objs.init(3);

    ObObj ref_obj0, ref_obj1, ref_obj2, ref_obj5;
    gen_obj(ref_obj0, i, seed0);
    gen_obj(ref_obj1, i, seed1);
    gen_obj(ref_obj2, i, seed2);
    gen_obj(ref_obj5, i, seed5);
    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj5);

    // 0--- ROW_CNT-40 --- ROW_CNT-30 --- ROW_CNT-20 ---ROW_CNT-10 --- ROW_CNT
    // |    seed0   |   seed1   |     seed2     |   seed3    |     null      |
    //                    |                                          |
    //                ROW_CNT-35                                 ROW_CNT-5

    uint64_t filter_cost_ns = 0;
    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    TEST_FILTER_PUSHDOWN_OP(sql::WHITE_OP_IN, white_filter, seed1_count + seed2_count, i);

    objs.reuse();
    objs.init(3);
    gen_obj(ref_obj5, i, seed5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    TEST_FILTER_PUSHDOWN_OP(sql::WHITE_OP_IN, white_filter_2, 0, i);
  }
}

void TestDecoderFilterPerf::in_op_test_for_simple(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed0 = 1000;
  int64_t seed1 = 1001;
  int64_t seed2 = 1002;
  int64_t seed3 = 1003;
  int64_t seed5 = 1005;
  int64_t seed0_count = SIMPLE_ROW_CNT / 5 + 10;
  int64_t seed1_count = SIMPLE_ROW_CNT / 5 - 10;
  int64_t seed2_count = SIMPLE_ROW_CNT / 5 + 5;
  int64_t seed3_count = SIMPLE_ROW_CNT / 5 - 5;
  int64_t null_count = SIMPLE_ROW_CNT / 5;
  int64_t seed5_count = 0;
  const int64_t op_idx = TestDecoderPerfTimeCtx::IN_BT_IDX;

  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, seed0_count, false, seed0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(seed0_count, seed0_count + seed1_count, false, seed1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TestEncodeDecodeParam param3(seed0_count + seed1_count, seed0_count + seed1_count + seed2_count, false, seed2);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param3));
  TestEncodeDecodeParam param4(seed0_count + seed1_count + seed2_count, SIMPLE_ROW_CNT - null_count, false, seed3);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param4));
  TestEncodeDecodeParam param5(SIMPLE_ROW_CNT - null_count, SIMPLE_ROW_CNT, true, 0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param5));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  const int64_t row_start = 0;
  const int64_t row_count = SIMPLE_ROW_CNT;
  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  sql::ObPushdownWhiteFilterNode white_filter_2(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }

    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 3);
    objs.init(3);

    ObObj ref_obj0, ref_obj1, ref_obj2, ref_obj5;
    gen_obj(ref_obj0, i, seed0);
    gen_obj(ref_obj1, i, seed1);
    gen_obj(ref_obj2, i, seed2);
    gen_obj(ref_obj5, i, seed5);
    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj5);

    uint64_t filter_cost_ns = 0;
    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(SIMPLE_ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    TEST_FILTER_PUSHDOWN_OP(sql::WHITE_OP_IN, white_filter, seed1_count + seed2_count, i);

    objs.reuse();
    objs.init(3);
    gen_obj(ref_obj5, i, seed5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    TEST_FILTER_PUSHDOWN_OP(sql::WHITE_OP_IN, white_filter_2, 0, i);
  }
}

void TestDecoderFilterPerf::in_op_test_for_general(const bool is_column_store)
{
  const int64_t op_idx = TestDecoderPerfTimeCtx::IN_BT_IDX;
  TEST_GENERAL_ENCODE_DECODE();

  const int64_t row_start = 0;
  const int64_t row_count = SIMPLE_ROW_CNT;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  sql::ObPushdownWhiteFilterNode white_filter_2(cur_allocator);
  int64_t real_seed_cnt = with_null_data_ ? seed_cnt - 1 : seed_cnt;
  bool is_first_check = true;
  need_check_filter = need_check_filter & (real_seed_cnt > 0);
  int64_t invalid_seed_val = param_arr.at(param_arr.size() - 1).seed_val_ + 100000;
  for (int64_t i = 0; need_check_filter && (i < full_column_cnt_); ++i) {
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    ObObj ref_obj0;
    gen_obj(ref_obj0, i, param_arr.at(0).seed_val_);
    objs.push_back(ref_obj0);

    uint64_t filter_cost_ns = 0;
    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(SIMPLE_ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    int64_t in_cnt = param_arr.at(0).get_row_count();
    if (is_first_check) {
      TEST_FILTER_PUSHDOWN_OP(sql::WHITE_OP_IN, white_filter, in_cnt, i);
      is_first_check = false;
    }
    TEST_FILTER_PUSHDOWN_OP(sql::WHITE_OP_IN, white_filter, in_cnt, i);

    objs.reuse();
    objs.init(1);
    ObObj invalid_ref_obj;
    gen_obj(invalid_ref_obj, i, invalid_seed_val);
    objs.push_back(invalid_ref_obj);
    TEST_FILTER_PUSHDOWN_OP(sql::WHITE_OP_IN, white_filter_2, 0, i);
  }
}

void TestDecoderFilterPerf::basic_filter_pushdown_bt_op_test(const bool is_column_store)
{
  if (check_all_type_) {
    bt_op_test_for_all(is_column_store);
  } else if (highest_data_pct_ == 0) {
    bt_op_test_for_simple(is_column_store);
  } else {
    bt_op_test_for_general(is_column_store);
  }
}

void TestDecoderFilterPerf::bt_op_test_for_all(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  int64_t seed2 = 10002;
  int64_t seed0_count = ROW_CNT - 10;
  int64_t seed1_count = 10;
  int64_t seed3_count = 0;
  const int64_t op_idx = TestDecoderPerfTimeCtx::IN_BT_IDX;

  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, ROW_CNT - 10, false, seed0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(ROW_CNT - 10, ROW_CNT, false, seed1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  const int64_t row_start = 0;
  const int64_t row_count = ROW_CNT; // cur_decoder->row_count_
  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 2);
    objs.init(2);
    ObObj ref_obj1, ref_obj2;

    // 0 --------- ROW_CNT-10 --- ROW_CNT
    // |   seed0      |    seed1    |
    //         |               |
    //     ROW_CNT-35      ROW_CNT-5

    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    uint64_t filter_cost_ns = 0;

    TEST_FILTER_PUSHDOWN_OP_2(sql::WHITE_OP_BT, white_filter, seed1_count, i, ref_obj1, seed1, ref_obj2, seed2);
    TEST_FILTER_PUSHDOWN_OP_2(sql::WHITE_OP_BT, white_filter, 0, i, ref_obj2, seed2, ref_obj1, seed1);
  }
}

void TestDecoderFilterPerf::bt_op_test_for_simple(const bool is_column_store)
{
  TEST_INIT_ROW();

  int64_t seed0 = 10000;
  int64_t seed1 = 10001;
  int64_t seed2 = 10002;
  int64_t seed0_count = SIMPLE_ROW_CNT / 2 - 10;
  int64_t seed1_count = SIMPLE_ROW_CNT / 2 + 10;
  int64_t seed3_count = 0;
  const int64_t op_idx = TestDecoderPerfTimeCtx::IN_BT_IDX;

  ObArray<TestEncodeDecodeParam> param_arr;
  TestEncodeDecodeParam param1(0, seed0_count, false, seed0);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param1));
  TestEncodeDecodeParam param2(seed0_count, SIMPLE_ROW_CNT, false, seed1);
  ASSERT_EQ(OB_SUCCESS, param_arr.push_back(param2));
  TEST_ENCODE_AND_DECODE_PROCEDURE(param_arr);

  const int64_t row_start = 0;
  const int64_t row_count = SIMPLE_ROW_CNT;
  bool is_first_check = false;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 2);
    objs.init(2);
    ObObj ref_obj1, ref_obj2;

    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(SIMPLE_ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    uint64_t filter_cost_ns = 0;

    TEST_FILTER_PUSHDOWN_OP_2(sql::WHITE_OP_BT, white_filter, seed1_count, i, ref_obj1, seed1, ref_obj2, seed2);
    TEST_FILTER_PUSHDOWN_OP_2(sql::WHITE_OP_BT, white_filter, 0, i, ref_obj2, seed2, ref_obj1, seed1);
  }
}

void TestDecoderFilterPerf::bt_op_test_for_general(const bool is_column_store)
{
  const int64_t op_idx = TestDecoderPerfTimeCtx::IN_BT_IDX;
  TEST_GENERAL_ENCODE_DECODE();

  const int64_t row_start = 0;
  const int64_t row_count = SIMPLE_ROW_CNT;
  sql::ObPushdownWhiteFilterNode white_filter(cur_allocator);
  int64_t real_seed_cnt = with_null_data_ ? seed_cnt - 1 : seed_cnt;
  bool is_first_check = true;
  need_check_filter = need_check_filter & (real_seed_cnt > 1);
  for (int64_t i = 0; need_check_filter && (i < full_column_cnt_); ++i) {
    ObMalloc mallocer;
    mallocer.set_label("ColumnDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 2);
    objs.init(2);
    ObObj ref_obj0, ref_obj1;

    ObBitmap result_bitmap(cur_allocator);
    result_bitmap.init(SIMPLE_ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    uint64_t filter_cost_ns = 0;

    int64_t seed0_val = param_arr.at(0).seed_val_;
    int64_t seed1_val = param_arr.at(1).seed_val_;
    int64_t left_boundary = std::min(seed0_val, seed1_val);
    int64_t right_boundary = std::max(seed0_val, seed1_val);
    int64_t bt_cnt = param_arr.at(0).get_row_count() + param_arr.at(1).get_row_count();
    if (is_first_check) {
      TEST_FILTER_PUSHDOWN_OP_2(sql::WHITE_OP_BT, white_filter, bt_cnt, i, ref_obj0, left_boundary, ref_obj1, right_boundary);
      is_first_check = false;
    }
    TEST_FILTER_PUSHDOWN_OP_2(sql::WHITE_OP_BT, white_filter, bt_cnt, i, ref_obj0, left_boundary, ref_obj1, right_boundary);
    TEST_FILTER_PUSHDOWN_OP_2(sql::WHITE_OP_BT, white_filter, 0, i, ref_obj1, right_boundary, ref_obj0, left_boundary);
  }
}

void TestDecoderFilterPerf::init_filter(
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

void TestDecoderFilterPerf::init_in_filter(
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

int TestDecoderFilterPerf::test_filter_pushdown(
    const bool is_column_store,
    const uint64_t col_idx,
    ObIMicroBlockDecoder *decoder,
    sql::ObPushdownWhiteFilterNode &filter_node,
    const int64_t row_start,
    const int64_t row_count,
    common::ObBitmap &result_bitmap,
    common::ObFixedArray<ObObj, ObIAllocator> &objs,
    uint64_t &filter_cost_ns)
{
  int ret = OB_SUCCESS;
  filter_cost_ns = 0;
  EXPECT_NE(nullptr, decoder);
  ObArenaAllocator &cur_allocator = is_column_store ? cs_allocator_ : allocator_;
  sql::PushdownFilterInfo pd_filter_info;
  sql::ObExecContext exec_ctx(cur_allocator);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(cur_allocator);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  sql::ObWhiteFilterExecutor filter(cur_allocator, filter_node, op);
  filter.col_offsets_.init(COLUMN_CNT);
  filter.col_params_.init(COLUMN_CNT);
  const ObColumnParam *col_param = nullptr;
  filter.col_params_.push_back(col_param);
  filter.col_offsets_.push_back(col_idx);
  filter.n_cols_ = 1;
  void *storage_datum_buf = cur_allocator.alloc(sizeof(ObStorageDatum) * COLUMN_CNT);
  EXPECT_TRUE(storage_datum_buf != nullptr);
  pd_filter_info.datum_buf_ = new (storage_datum_buf) ObStorageDatum [COLUMN_CNT]();
  pd_filter_info.col_capacity_ = full_column_cnt_;
  pd_filter_info.start_ = row_start;
  pd_filter_info.count_ = row_count;

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
    uint64_t start_ns = ObTimeUtility::current_time_ns();
    ret = decoder->filter_pushdown_filter(nullptr, filter, pd_filter_info, result_bitmap);
    filter_cost_ns = ObTimeUtility::current_time_ns() - start_ns;
  }
  if (nullptr != storage_datum_buf) { cur_allocator.free(storage_datum_buf); }
  if (nullptr != expr_buf) { cur_allocator.free(expr_buf); }
  if (nullptr != expr_p_buf) { cur_allocator.free(expr_p_buf); }
  if (nullptr != datum_buf) { cur_allocator.free(datum_buf); }
  return ret;
}

int64_t TestDecoderFilterPerf::calc_result_count(
  const ObArray<TestEncodeDecodeParam> &param_arr,
  const sql::ObWhiteFilterOperatorType &op_type)
{
  int64_t cnt = 0;
  switch (op_type) {
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_GT:
      for (int64_t i = 1; i < param_arr.size(); ++i) {
        if (param_arr.at(i).seed_val_ > param_arr.at(0).seed_val_ && !param_arr.at(i).is_null_) {
          cnt += param_arr.at(i).get_row_count();
        }
      }
      if (sql::WHITE_OP_GE == op_type) {
        cnt += param_arr.at(0).get_row_count();
      }
      break;
    case sql::WHITE_OP_LE:
    case sql::WHITE_OP_LT:
      for (int64_t i = 1; i < param_arr.size(); ++i) {
        if (param_arr.at(i).seed_val_ < param_arr.at(0).seed_val_ && !param_arr.at(i).is_null_) {
          cnt += param_arr.at(i).get_row_count();
        }
      }
      if (sql::WHITE_OP_LE == op_type) {
        cnt += param_arr.at(0).get_row_count();
      }
      break;
    case sql::WHITE_OP_NE:
      for (int64_t i = 1; i < param_arr.size(); ++i) {
        if (param_arr.at(i).seed_val_ != param_arr.at(0).seed_val_ && !param_arr.at(i).is_null_) {
          cnt += param_arr.at(i).get_row_count();
        }
      }
      break;
    default:
      cnt = -1;
  }
  return cnt;
}

} // end of namespace blocksstable
} // end of namespace oceanbase

#endif // OCEANBASE_CS_ENCODING_TEST_DECODER_FILTER_PERF_H_
