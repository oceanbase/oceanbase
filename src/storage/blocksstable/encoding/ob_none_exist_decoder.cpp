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

#include "ob_none_exist_decoder.h"

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"
#include "ob_row_index.h"
#include "ob_vector_decode_util.h"
#include "common/ob_target_specific.h"
#include "lib/hash/ob_hashset.h"
#include "sql/engine/expr/ob_expr_cmp_func.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIRowIndex;
using namespace common;

int ObNoneExistColumnDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNoneExistColumnDecoder::get_distinct_count(int64_t &distinct_count) const
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNoneExistColumnDecoder::read_distinct(
    const ObColumnDecoderCtx &ctx,
    const char **cell_datas,
    storage::ObGroupByCell &group_by_cell)  const
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNoneExistColumnDecoder::read_reference(
    const ObColumnDecoderCtx &ctx,
    const int64_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell) const
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNoneExistColumnDecoder::decode_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex *row_index,
      ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < vector_ctx.row_cap_; ++idx) {
    int64_t row_idx = vector_ctx.row_ids_[idx];
    ObDatum *col_datum = nullptr;
    sql::ObExpr *expr = vector_ctx.get_expr();
    sql::ObEvalCtx *eval_ctx = vector_ctx.get_eval_ctx();
    const VectorFormat format = expr->get_format(*eval_ctx);
    col_datum = vector_ctx.get_default_datum();

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(col_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null datum", K(ret));
      } else {
        const int64_t vec_idx = idx + vector_ctx.vec_offset_;
        if (VEC_DISCRETE == format) {
          static_cast<ObDiscreteFormat *>(expr->get_vector(*eval_ctx))->set_datum(vec_idx, *col_datum);
        } else {
          static_cast<ObFixedLengthBase *>(expr->get_vector(*eval_ctx))->set_datum(vec_idx, *col_datum);
        }
      }
    }
  }
  return ret;
}

int ObNoneExistColumnDecoder::decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len)const
{
  datum.set_ext();
  datum.no_cv(datum.extend_obj_)->set_ext(common::ObActionFlag::OP_NOP);
  return common::OB_SUCCESS;
}

int ObNoneExistColumnDecoder::pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  ObDatum default_datums;
  default_datums = filter.get_default_datums()[0];
  const common::ObIArray<common::ObDatum> &ref_datums = filter.get_datums();
  ObDatumCmpFuncType cmp_func = filter.cmp_func_;

  switch (op_type) {
    case sql::WHITE_OP_NN: {
      if (!default_datums.is_null()) {
        result_bitmap.bit_not();
      }
      break;
    }
    case sql::WHITE_OP_NU: {
      if (default_datums.is_null()) {
        result_bitmap.bit_not();
      }
      break;
    }
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE:
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_LE: {
      bool cmp_ret = false;
      if (OB_UNLIKELY(ref_datums.count() != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument for comparison operator", K(ret), K(ref_datums));
      } else if (default_datums.is_null() || ref_datums.at(0).is_null()) {
        // Result of compare with null is null
      } else if (OB_FAIL(compare_datum(
                  default_datums, ref_datums.at(0),
                  cmp_func,
                  sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()],
                  cmp_ret))) {
        LOG_WARN("Failed to compare datum", K(ret), K(default_datums), K(ref_datums.at(0)),
            K(sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()]));
      } else if (cmp_ret) {
        result_bitmap.bit_not();
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      int cmp_ret_0 = 0;
      int cmp_ret_1 = 0;
      if (OB_UNLIKELY(ref_datums.count() != 2)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument for between operators", K(ret), K(ref_datums));
      } else if (default_datums.is_null()) {
        // Result of compare with null is null
      } else if (OB_FAIL(cmp_func(default_datums, ref_datums.at(0), cmp_ret_0))) {
        LOG_WARN("Failed to compare datum", K(ret), K(default_datums), K(ref_datums.at(0)));
      } else if (cmp_ret_0 < 0) {
      } else if (OB_FAIL(cmp_func(default_datums, ref_datums.at(1), cmp_ret_1))) {
        LOG_WARN("Failed to compare datum", K(ret), K(default_datums), K(ref_datums.at(0)));
      } else if (cmp_ret_1 <= 0) {
        result_bitmap.bit_not();
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      bool is_existed = false;
      if (OB_FAIL(filter.exist_in_datum_set(default_datums, is_existed))) {
        LOG_WARN("Failed to check object in hashset", K(ret), K(default_datums));
      } else if (is_existed) {
        result_bitmap.bit_not();
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unexpected filter pushdown operation type", K(ret), K(op_type));
    }
  } // end of switch
  return ret;
}

int ObNoneExistColumnDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    sql::ObBlackFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    bool &filter_applied) const
{
  int ret = OB_SUCCESS;
  sql::ObPhysicalFilterExecutor *black_filter = static_cast<sql::ObPhysicalFilterExecutor *>(&filter);
  sql::ObPushdownOperator &pushdown_op = black_filter->get_op();
  ObStorageDatum *default_datums = const_cast<ObStorageDatum *>(&filter.get_default_datums()[0]);
  // bool filtered = false;
  LOG_DEBUG("ob_none_exist_decoder.184", K(filter_applied), K(*result_bitmap.get_data()), K(result_bitmap.is_all_false()));
  if (pushdown_op.enable_rich_format_ &&
      OB_FAIL(storage::init_exprs_uniform_header(black_filter->get_cg_col_exprs(), pushdown_op.get_eval_ctx(), 1))) {
    LOG_WARN("Failed to init exprs vector header", K(ret));
  } else if (OB_FAIL(black_filter->filter(default_datums, black_filter->get_col_count(), *pd_filter_info.skip_bit_, filter_applied))) {
    LOG_WARN("Failed to filter row with black filter", K(ret), K(black_filter));
  }
  LOG_DEBUG("ob_none_exist_decoder.184", K(filter_applied), K(*result_bitmap.get_data()), K(result_bitmap.is_all_false()));
  if (OB_SUCC(ret)) {
    if(!filter_applied)
    {
      LOG_DEBUG("ob_none_exist_decoder.187");
      result_bitmap.bit_not();
    }
  }
  return ret;
}

}
}