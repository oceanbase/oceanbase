/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_length.h"
#include "ob_expr_json_func_helper.h"
#include "lib/json_type/ob_json_bin.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonLength::ObExprJsonLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_LENGTH, N_JSON_LENGTH,
                         ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonLength::~ObExprJsonLength()
{
}

int ObExprJsonLength::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  INIT_SUCC(ret);
  UNUSED(type_ctx);

  // set result type to int32
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);

  // 0 position is json doc
  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_LENGTH))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
  } else if (param_num > 1 && OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, 1))) {
    LOG_WARN("wrong type for json path.", K(ret), K(types_stack[1].get_type()));
  }

  return ret;
}

static int get_length_from_raw_bin(const ObString &buf, int32_t &length)
{
  INIT_SUCC(ret);
  int64_t pos = 0;
  if (OB_UNLIKELY(buf.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json_doc buffer is empty", K(ret), K(buf.length()));
  } else {
    if (ObJsonBin::is_doc_header(static_cast<uint8_t>(buf[0]))) {
      pos = sizeof(ObJsonBinDocHeader);
    }
    if (OB_UNLIKELY(pos >= buf.length())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("json_doc buffer too short", K(ret), K(pos), K(buf.length()));
    } else {
      uint8_t type_byte = static_cast<uint8_t>(buf[pos]);
      ObJBVerType vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type_byte));
      ObJsonNodeType node_type = ObJsonVerType::get_json_type(vertype);
      if (node_type == ObJsonNodeType::J_OBJECT || node_type == ObJsonNodeType::J_ARRAY) {
        // skip ObJsonBinHeader type_ byte
        pos++;
        if (pos >= buf.length()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("buffer too short for header flags", K(ret), K(pos), K(buf.length()));
        } else {
          // [Type][Flags][Element Count][...]
          // header flags:
          //   bits 0-1: entry_size_
          //   bits 2-3: count_size_
          //   bits 4-5: obj_size_size_
          uint8_t bin_header_flags = static_cast<uint8_t>(buf[pos]);
          uint8_t count_size = (bin_header_flags >> 2) & 0x3;
          uint64_t count_var_size = ObJsonVar::get_var_size(count_size);
          if (buf.length() < pos + 1 + count_var_size) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("buffer too short for element_count", K(ret), K(buf.length()), K(count_var_size), K(pos));
          } else {
            uint64_t element_count = 0;
            if (OB_FAIL(ObJsonVar::read_var(buf.ptr() + pos + 1, count_size, &element_count))) {
              LOG_WARN("read element_count fail", K(ret), K(count_size));
            } else if (element_count > static_cast<uint64_t>(INT32_MAX)) {
              ret = OB_NUMERIC_OVERFLOW;
              LOG_WARN("element_count overflow int32", K(ret), K(element_count));
            } else {
              length = static_cast<int32_t>(element_count);
            }
          }
        }
      } else {
        // scalar length is always 1
        length = 1;
      }
    }
  }
  return ret;
}

// for new sql engine
int ObExprJsonLength::eval_json_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  uint64_t expr_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  ObJsonMemCtx *json_json_ctx = NULL;
  if (OB_ISNULL(json_json_ctx =
                    static_cast<ObJsonMemCtx *>(ctx.exec_ctx_.get_expr_op_ctx(expr_ctx_id)))) {
    if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr_ctx_id, json_json_ctx))) {
      LOG_WARN("failed to create expr op ctx", K(ret), K(expr_ctx_id));
    } else {
      json_json_ctx->set_json_max_depth_config(ObJsonExprHelper::get_json_max_depth_config(ctx));
    }
  } else {
    json_json_ctx->reuse();
  }
  if (OB_SUCC(ret) && OB_ISNULL(json_json_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json json ctx is null", K(ret), K(expr_ctx_id));
  }
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    if (OB_FAIL(json_json_ctx->init(expr.type_, tenant_id, true))) {
      LOG_WARN("failed to init json length ctx", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    MultimodeAlloctor *allocator = json_json_ctx->get_allocator();
    int32_t json_max_depth_config = json_json_ctx->get_json_max_depth_config();
    ObJsonPathCache *path_cache = json_json_ctx->get_path_cache();
    bool is_null = false;
    int32_t res_len = 0;
    ObIJsonBase *j_base = NULL;
    ObDatum *json_doc = NULL;
    ObDatum *path = NULL;
    ObDatumMeta path_meta;
    bool path_has_lob_header = false;
    ObExpr *arg0 = expr.args_[0];
    ObString json_bin;
    ObString path_bin;
    bool is_const = false;

    if (OB_FAIL(allocator->eval_arg(arg0, ctx, json_doc))) {
      LOG_WARN("fail to eval json arg", K(ret), K(arg0->datum_meta_));
    } else if (expr.arg_cnt_ > 1) {
      ObExpr *arg1 = expr.args_[1];
      is_const = arg1->is_static_const_expr();
      path_meta = arg1->datum_meta_;
      path_has_lob_header = arg1->obj_meta_.has_lob_header();
      if (OB_FAIL(allocator->eval_arg(arg1, ctx, path))) {
        LOG_WARN("fail to eval path arg", K(ret), K(path_meta));
      }
    }

    if (OB_SUCC(ret)) {
      ObObjType json_doc_type = arg0->datum_meta_.type_;
      ObCollationType json_doc_cs_type = arg0->datum_meta_.cs_type_;
      ObJsonInType j_in_type = ObJsonInType::JSON_BIN;

      if (json_doc_type == ObNullType || json_doc->is_null()) {
        is_null = true;
      } else if (json_doc_type != ObJsonType && !ob_is_string_type(json_doc_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("input type error", K(ret), K(json_doc_type));
      } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(json_doc_type, json_doc_cs_type))) {
        LOG_WARN("fail to ensure collation", K(ret), K(json_doc_type), K(json_doc_cs_type));
      } else if (OB_FALSE_IT(json_bin = json_doc->get_string())) {
      } else if (json_bin.length() == 0) {
        ret = OB_ERR_INVALID_JSON_TEXT;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                   *allocator, *json_doc, arg0->datum_meta_, arg0->obj_meta_.has_lob_header(), json_bin))) {
        LOG_WARN("fail to get real data.", K(ret), K(json_bin));
      }

      if (OB_SUCC(ret) && !is_null) {
        allocator->add_baseline_size(json_bin.length());
        j_in_type = ObJsonExprHelper::get_json_internal_type(json_doc_type);
      }

      bool valid_path = false;
      if (OB_SUCC(ret) && !is_null && !OB_ISNULL(path)) {
        if (path->is_null() || path_meta.type_ == ObNullType) {
          is_null = true;
        } else {
          path_bin = path->get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  *allocator, *path, path_meta, path_has_lob_header, path_bin))) {
            LOG_WARN("fail to get real data.", K(ret), K(path_bin));
          } else {
            valid_path = true;
          }
        }
      }

      if (OB_SUCC(ret) && !is_null && !valid_path) {
        if (j_in_type == ObJsonInType::JSON_BIN) {
          if (OB_FAIL(get_length_from_raw_bin(json_bin, res_len))) {
            LOG_WARN("fail to get json length from binary", K(ret), K(json_bin.length()));
          }
        } else {
          if (OB_FAIL(ObJsonBaseFactory::get_json_base(
                  allocator, json_bin, j_in_type, j_in_type, j_base, 0, json_max_depth_config))) {
            LOG_WARN("fail to get json base", K(ret), K(json_doc_type), K(json_bin), K(j_in_type));
          } else {
            res_len = static_cast<int32_t>(j_base->member_count());
          }
        }
      } else if (OB_SUCC(ret) && !is_null) {
        ObJsonSeekResult hit;
        ObJsonPath *j_path = NULL;
        if (OB_FAIL(ObJsonBaseFactory::get_json_base(
                allocator, json_bin, j_in_type, j_in_type, j_base, 0, json_max_depth_config))) {
          LOG_WARN("fail to get json base", K(ret), K(json_doc_type), K(json_bin), K(j_in_type));
        } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(
                       json_json_ctx->get_ctx_allocator(), path_cache, j_path, path_bin, 1, true, is_const))) {
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
          LOG_WARN("fail to parse json path", K(ret), K(path_meta.type_), K(path_bin));
        } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, false, hit))) {
          LOG_WARN("fail to seek json node", K(ret), K(path_bin));
        } else if (hit.size() == 0) {
          is_null = true;
        } else if (hit.size() > 1) {
          res_len = static_cast<int32_t>(hit.size());
        } else {
          res_len = static_cast<int32_t>(hit[0]->member_count());
        }
      }

      if (OB_SUCC(ret)) {
        if (is_null) {
          res.set_null();
        } else {
          res.set_int32(res_len);
        }
      }
    }
  }
  return ret;
}

int ObExprJsonLength::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_length;
  return OB_SUCCESS;
}


}
}
