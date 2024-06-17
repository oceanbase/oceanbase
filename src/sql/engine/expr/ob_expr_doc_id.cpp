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
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 1) || OB_ISNULL(rt_expr.args_) || ObVarcharType != rt_expr.datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
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
  common::ObDatum *datum = nullptr;
  if (OB_UNLIKELY(1 != raw_ctx.arg_cnt_) || OB_ISNULL(raw_ctx.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(raw_ctx), KP(raw_ctx.args_));
  } else if (OB_FAIL(raw_ctx.args_[0]->eval(eval_ctx, datum))) {
    LOG_WARN("fail to eval tablet id", K(ret), K(raw_ctx), K(eval_ctx));
  } else if (OB_ISNULL(datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum ptr", K(ret), KP(datum));
  } else {
    share::ObTabletAutoincrementService &auto_inc = share::ObTabletAutoincrementService::get_instance();
    const ObTabletID tablet_id(datum->get_int());
    uint64_t seq_id = 0;
    uint64_t buf_len = sizeof(ObDocId);
    uint64_t *buf = reinterpret_cast<uint64_t *>(raw_ctx.get_str_res_mem(eval_ctx, buf_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), KP(buf));
    } else if (OB_FAIL(auto_inc.get_autoinc_seq(MTL_ID(), tablet_id, seq_id))) {
      LOG_WARN("fail to get tablet autoinc seq", K(ret), K(tablet_id));
    } else {
      ObDocId *doc_id = new (buf) ObDocId(tablet_id.id(), seq_id);
      expr_datum.set_string(doc_id->get_string());
      FLOG_INFO("succeed to genearte document id", K(tablet_id), K(seq_id));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
