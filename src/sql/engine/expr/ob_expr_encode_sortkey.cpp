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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_encode_sortkey.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{
ObExprEncodeSortkey::ObExprEncodeSortkey(ObIAllocator &alloc)
  : ObExprOperator(
      alloc, T_FUN_SYS_ENCODE_SORTKEY, N_ENCODE_SORTKEY, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
      INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{}

ObExprEncodeSortkey::~ObExprEncodeSortkey()
{}

int ObExprEncodeSortkey::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types,
                                           int64_t param_num,
                                           ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types);
  int ret = OB_SUCCESS;
  if (param_num % 3 != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should be according to (colunm, asc/desc, "
             "nulls_first/nulls_last)",
             K(param_num), K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_num; i += 3) {
      types[i + 1].set_calc_type(ObIntType);
      types[i + 2].set_calc_type(ObIntType);
    }
    type.set_varchar();
    type.set_length(OB_MAX_ROW_LENGTH);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprEncodeSortkey::eval_encode_sortkey(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;

  ObSQLSessionInfo *sess = NULL;
  // get ptr from extra, prt ==null alloc
  uint64_t enc_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  ObExprEncodeCtx *encode_ctx = NULL;
  ObExecContext *exec_ctx = &ctx.exec_ctx_;

  if (expr.arg_cnt_ % 3 != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should be according to (colunm, asc/desc, "
             "nulls_first/nulls_last)",
             K(ret));
  } else {
    if (NULL == (encode_ctx = static_cast<ObExprEncodeCtx *>(exec_ctx->get_expr_op_ctx(enc_id)))) {
      share::ObEncParam *buf = NULL;
      int64_t param_cnt = expr.arg_cnt_ / 3;
      if (OB_FAIL(exec_ctx->create_expr_op_ctx(enc_id, encode_ctx))) {
        LOG_WARN("failed to create operator context", K(ret));
      } else if (OB_ISNULL(buf = static_cast<share::ObEncParam *>(exec_ctx->get_allocator().alloc(
                             param_cnt * sizeof(share::ObEncParam))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory.", K(ret));
      } else if (OB_ISNULL(sess = ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid session info", K(ret));
      } else {
        encode_ctx->max_len_ = expr.res_buf_len_;
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i += 3) {
          ObDatum *order = NULL;
          ObDatum *nulls_pos = NULL;
          if (OB_FAIL(expr.args_[i + 1]->eval(ctx, order))) {
            LOG_WARN("incorrect order", K(ret), K(i + 1));
          } else if (OB_FAIL(expr.args_[i + 2]->eval(ctx, nulls_pos))) {
            LOG_WARN("incorrect null position", K(ret), K(i + 2));
          } else if (OB_ISNULL(order)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret));
          } else if (OB_ISNULL(nulls_pos)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret));
          } else {
            share::ObEncParam *param = new (buf + i / 3) share::ObEncParam();
            param->type_ = expr.args_[i]->datum_meta_.type_;
            param->cs_type_ = expr.args_[i]->datum_meta_.cs_type_;
            param->is_var_len_ = !lib::is_oracle_mode() && is_pad_char_to_full_length(sess->get_sql_mode());
            param->is_memcmp_ = lib::is_oracle_mode();
            param->is_nullable_ = true;
            int64_t odr = order->get_int();
            int64_t np = nulls_pos->get_int();
            // null pos: null first -> 0, nulls last -> 1
            // order: asc -> 0, desc -> 1
            if (odr != 0 && odr != 1) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret));
            } else if (np != 0 && np != 1) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret));
            } else {
              param->is_null_first_ = (np == 0);
              param->is_asc_ = (odr == 0);
            }
          }
        }
        encode_ctx->params_ = buf;
      }
    } else {
      // has encode ctx, not need to init
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(encode_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(encode_ctx->params_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    share::ObEncParam *params = encode_ctx->params_;
    while (true) {
      ret = OB_SUCCESS;
      int64_t data_len = 0;
      unsigned char *buf
        = reinterpret_cast<unsigned char *>(expr.get_str_res_mem(ctx, encode_ctx->max_len_));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("invalid argument", K(ret), K(encode_ctx->max_len_));
      }
      // encode
      bool has_invalid_uni = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i += 3) {
        ObDatum *data = NULL;
        if (OB_FAIL(expr.args_[i]->eval(ctx, data))) {
          LOG_WARN("incorrect data", K(ret), K(i));
        } else {
          int64_t tmp_data_len = 0;
          if (OB_FAIL(share::ObSortkeyConditioner::process_key_conditioning(
                       *data, buf + data_len, encode_ctx->max_len_ - data_len,
                       tmp_data_len, params[i / 3]))) {
            if (ret != OB_BUF_NOT_ENOUGH) {
              LOG_WARN("failed  to encode sortkey", K(ret));
            }
          } else {
            if (!params[i/3].is_valid_uni_) has_invalid_uni=true;
            data_len += tmp_data_len;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (has_invalid_uni) {
          res_datum.set_null();
        } else {
          res_datum.set_string(ObString(data_len, (char *)buf));
        }
      } else if (ret == OB_BUF_NOT_ENOUGH) {
        encode_ctx->max_len_ = encode_ctx->max_len_ * 2;
        continue;
      }
      break;
    }
  }

  return ret;
}

int ObExprEncodeSortkey::eval_encode_sortkey_batch(const ObExpr &expr,
                                                   ObEvalCtx &ctx,
                                                   const ObBitVector &skip,
                                                   const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = NULL;
  bool is_pad_char = false;

  // get ptr from extra, prt ==null alloc
  uint64_t enc_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  ObExprEncodeCtx *encode_ctx = NULL;
  ObExecContext *exec_ctx = &ctx.exec_ctx_;

  if (expr.arg_cnt_ % 3 != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should be according to (colunm, asc/desc, "
             "nulls_first/nulls_last)",
             K(ret));
  } else {
    if (NULL == (encode_ctx = static_cast<ObExprEncodeCtx *>(exec_ctx->get_expr_op_ctx(enc_id)))) {
      share::ObEncParam *buf = NULL;
      int64_t param_cnt = expr.arg_cnt_ / 3;
      if (OB_FAIL(exec_ctx->create_expr_op_ctx(enc_id, encode_ctx))) {
        LOG_WARN("failed to create operator context", K(ret));
      } else if (OB_ISNULL(buf = static_cast<share::ObEncParam *>(exec_ctx->get_allocator().alloc(
                             param_cnt * sizeof(share::ObEncParam))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory.", K(ret));
      } else if (OB_ISNULL(sess = ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid session info", K(ret));
      } else {
        // init max_len
        encode_ctx->max_len_ = expr.res_buf_len_;
        // init param
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i += 3) {
          if (OB_FAIL(expr.args_[i + 1]->eval_batch(ctx, skip, batch_size))) {
            LOG_WARN("incorrect order", K(ret), K(i + 1));
          } else if (OB_FAIL(expr.args_[i + 2]->eval_batch(ctx, skip, batch_size))) {
            LOG_WARN("incorrect null position", K(ret), K(i + 2));
          } else {
            ObDatum order = expr.args_[i + 1]->locate_expr_datum(ctx);
            ObDatum nulls_pos = expr.args_[i + 2]->locate_expr_datum(ctx);
            share::ObEncParam *param = new (buf + i / 3) share::ObEncParam();
            param->type_ = expr.args_[i]->datum_meta_.type_;
            param->cs_type_ = expr.args_[i]->datum_meta_.cs_type_;
            param->is_var_len_ = is_pad_char_to_full_length(sess->get_sql_mode());
            param->is_memcmp_ = lib::is_oracle_mode();
            param->is_nullable_ = true;
            int64_t odr = order.get_int();
            int64_t np = nulls_pos.get_int();
            // null pos: null first -> 0, nulls last -> 1
            // order: asc -> 0, desc -> 1
            if (odr != 0 && odr != 1) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret));
            } else if (np != 0 && np != 1) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret));
            } else {
              param->is_null_first_ = (np == 0);
              param->is_asc_ = (odr == 0);
            }
          }
        }
        encode_ctx->params_ = buf;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(encode_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(encode_ctx->params_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  }

  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  share::ObEncParam *params = encode_ctx->params_;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i += 3) {
    if (OB_FAIL(expr.args_[i]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("incorrect data", K(ret), K(i));
    } else {
      // do nothing
    }
  }

  // do encoding
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; i++) {
    if (skip.at(i) || eval_flags.at(i)) {
      continue;
    }
    while (true) {
      ret = OB_SUCCESS;
      int64_t encode_len = 0;
      unsigned char *buf
        = reinterpret_cast<unsigned char *>(expr.get_str_res_mem(ctx, encode_ctx->max_len_, i));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("invalid argument", K(ret), K(buf));
      }
      bool has_invalid_uni = false;
      for (int64_t j = 0; !has_invalid_uni && OB_SUCC(ret) && j < expr.arg_cnt_; j += 3) {
        ObDatum &data = expr.args_[j]->locate_expr_datum(ctx, i);
        int64_t tmp_data_len = 0;
        if (OB_FAIL(share::ObSortkeyConditioner::process_key_conditioning(
                     data, buf + encode_len, encode_ctx->max_len_ - encode_len, tmp_data_len,
                     params[j / 3]))) {
          if (ret != OB_BUF_NOT_ENOUGH) {
            LOG_WARN("failed  to encode sortkey", K(ret));
          }
        } else {
          if (!params[j/3].is_valid_uni_) has_invalid_uni=true;
          encode_len += tmp_data_len;
        }
      }
      if (OB_SUCC(ret)) {
        if (has_invalid_uni) {
          expr.locate_expr_datum(ctx, i).set_null();
        } else {
          expr.locate_expr_datum(ctx, i).set_string(ObString(encode_len, (char *)buf));
        }
      } else if (ret == OB_BUF_NOT_ENOUGH) {
        encode_ctx->max_len_ = encode_ctx->max_len_ * 2;
        continue;
      }
      break;
    }
  }
  return ret;
}

int ObExprEncodeSortkey::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  rt_expr.eval_func_ = &eval_encode_sortkey;
  rt_expr.eval_batch_func_ = &eval_encode_sortkey_batch;
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
