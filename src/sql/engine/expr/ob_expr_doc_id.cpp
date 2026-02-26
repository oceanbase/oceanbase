/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "sql/engine/expr/ob_expr_doc_id.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprDocID::ObExprDocID(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_DOC_ID, N_DOC_ID, ZERO_OR_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
  STORAGE_FTS_LOG(DEBUG, "construct doc id expr", K(common::lbt()));
}

int ObExprDocID::calc_result_typeN(ObExprResType &type,
                                   ObExprResType *types,
                                   int64_t param_num,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(param_num, types);
  type.set_varbinary();
  type.set_length(sizeof(ObDocId));
  return ret;
}

int ObExprDocID::calc_resultN(ObObj &result,
                              const ObObj *objs_array,
                              int64_t param_num,
                              ObExprCtx &expr_ctx) const
{
  return OB_NOT_SUPPORTED;
}

int ObExprDocID::cg_expr(
    ObExprCGCtx &cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObVarcharType != rt_expr.datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, expr type isn't varchar", K(ret), K(rt_expr.datum_meta_.type_));
  } else if (OB_UNLIKELY(rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_UNLIKELY(rt_expr.arg_cnt_ == 1) && OB_ISNULL(rt_expr.args_) ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_doc_id;
  }
  return ret;
}

/*static*/ int ObExprDocID::generate_doc_id(
    const ObExpr &raw_ctx,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObObjectID partition_id = OB_INVALID_ID;
  ObTabletID tablet_id;
  if (raw_ctx.arg_cnt_ == 0) {
    //expr_datum.set_null();
    LOG_TRACE("succeed to genearte empty document id", KP(&raw_ctx), K(raw_ctx), K(expr_datum), K(eval_ctx), K(lbt()));
  } else if (OB_UNLIKELY(1 != raw_ctx.arg_cnt_) || OB_ISNULL(raw_ctx.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(raw_ctx), KP(raw_ctx.args_));
  } else if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(raw_ctx.args_[0], eval_ctx, partition_id, tablet_id))) {
    LOG_WARN("fail to calc part and tablet id by expr", K(ret));
  } else {
    uint64_t seq_id = 0;
    uint64_t buf_len = sizeof(ObDocId);
    uint64_t *buf = reinterpret_cast<uint64_t *>(raw_ctx.get_str_res_mem(eval_ctx, buf_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), KP(buf));
    } else {
      if (eval_ctx.exec_ctx_.is_ddl_idempotent_autoinc()) {
        seq_id = ObDDLUtil::generate_idempotent_value(eval_ctx.exec_ctx_.get_slice_count(), // tablet slice count
                                                      eval_ctx.exec_ctx_.get_slice_idx(), // tablet slice idx
                                                      eval_ctx.exec_ctx_.get_autoinc_range_interval(),
                                                      eval_ctx.exec_ctx_.get_slice_row_idx());
      } else {
        share::ObTabletAutoincrementService &auto_inc = share::ObTabletAutoincrementService::get_instance();
        if (OB_FAIL(auto_inc.get_autoinc_seq(MTL_ID(), tablet_id, seq_id))) {
          LOG_WARN("fail to get tablet autoinc seq", K(ret), K(tablet_id));
        }
      }
      if (OB_SUCC(ret)) {
        ObDocId *doc_id = new (buf) ObDocId(tablet_id.id(), seq_id);
        expr_datum.set_string(doc_id->get_string());
        LOG_TRACE("succeed to genearte document id", K(tablet_id), K(seq_id), K(eval_ctx.exec_ctx_.is_ddl_idempotent_autoinc()), K(lbt()));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
