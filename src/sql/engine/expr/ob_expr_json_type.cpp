/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_type.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "lib/json_type/ob_json_bin.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonType::ObExprJsonType(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_TYPE, N_JSON_TYPE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonType::~ObExprJsonType()
{
}

/**
  Maps the enumeration value of type ObJsonNodeType into a string.
  For example:
  json_type_string_map[J_OBJECT] == "OBJECT"
*/
static constexpr const char *json_type_string_map[] = {
  "NULL",
  "DECIMAL",
  "INTEGER",
  "UNSIGNED INTEGER",
  "DOUBLE",
  "STRING",
  "OBJECT",
  "ARRAY",
  "BOOLEAN",
  "DATE",
  "TIME",
  "DATETIME",
  "TIMESTAMP",
  "OPAQUE",

  // OPAQUE types with special names
  "BLOB",
  "BIT",
  "GEOMETRY",
};

/**
   Compute an index into json_type_string_map
   to be applied to certain sub-types of J_OPAQUE.

   @param field_type The refined field type of the opaque value.

   @return an index into json_type_string_map
*/
uint32_t ObExprJsonType::opaque_index(ObObjType field_type)
{
  uint32_t idx = 0;

  switch (field_type) {
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObHexStringType: // BIT in mysql5.7, BLOB in mysql8.0
    case ObLongTextType: {
      idx = static_cast<uint32_t>(JsonOpaqueType::J_OPAQUE_BLOB);
      break;
    }

    case ObBitType: {
      idx = static_cast<uint32_t>(JsonOpaqueType::J_OPAQUE_BIT);
      break;
    }

    // TODO: support GEOMETRY
    // case obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY: {
    //   idx = static_cast<uint32_t>(JsonOpaqueType::J_OPAQUE_GEOMETRY);
    //   break;
    // }

    default: {
      idx = static_cast<uint32_t>(ObJsonNodeType::J_OPAQUE);
      break;
    }
  }

  return idx;
}

int ObExprJsonType::get_type_from_raw_bin(const ObString &buf, ObJsonNodeType &node_type)
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
      node_type = ObJsonVerType::get_json_type(vertype);
    }
  }
  return ret;
}

int ObExprJsonType::calc_result_type1(ObExprResType &type,
                                      ObExprResType &type1,
                                      common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);

  type.set_varchar();
  type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  type.set_collation_level(CS_LEVEL_IMPLICIT);

  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_TYPE))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  }

  return ret;
}

// for new sql engine
int ObExprJsonType::eval_json_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  uint64_t expr_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  ObJsonMemCtx *json_type_ctx = NULL;
  if (OB_ISNULL(json_type_ctx =
                    static_cast<ObJsonMemCtx *>(ctx.exec_ctx_.get_expr_op_ctx(expr_ctx_id)))) {
    if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr_ctx_id, json_type_ctx))) {
      LOG_WARN("failed to create expr op ctx", K(ret), K(expr_ctx_id));
    } else {
      json_type_ctx->set_json_max_depth_config(ObJsonExprHelper::get_json_max_depth_config(ctx));
    }
  } else {
    json_type_ctx->reuse();
  }
  if (OB_SUCC(ret) && OB_ISNULL(json_type_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json type ctx is null", K(ret), K(expr_ctx_id));
  }
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    if (OB_FAIL(json_type_ctx->init(expr.type_, tenant_id))) {
      LOG_WARN("failed to init json type ctx", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    MultimodeAlloctor *allocator = json_type_ctx->get_allocator();
    int32_t json_max_depth_config = json_type_ctx->get_json_max_depth_config();
    bool is_null = false;
    uint32_t type_idx = 0;
    ObIJsonBase *j_base = NULL;
    ObDatum *json_doc = NULL;
    ObExpr *arg0 = expr.args_[0];
    ObString json_bin;

    if (OB_FAIL(allocator->eval_arg(arg0, ctx, json_doc))) {
      LOG_WARN("fail to eval json arg", K(ret), K(arg0->datum_meta_));
    }

    if (OB_SUCC(ret)) {
      ObObjType json_doc_type = arg0->datum_meta_.type_;
      ObCollationType json_doc_cs_type = arg0->datum_meta_.cs_type_;
      ObJsonInType j_in_type = ObJsonInType::JSON_BIN;

      if (json_doc_type == ObNullType || json_doc->is_null()) {
        is_null = true;
      } else if (json_doc_type != ObJsonType && !ob_is_string_type(json_doc_type)) {
        ret = OB_ERR_INVALID_JSON_TEXT;
        LOG_WARN("Incorrect type for function json_type", K(ret), K(json_doc_type));
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

      if (OB_SUCC(ret) && !is_null) {
        ObJsonNodeType j_type = ObJsonNodeType::J_NULL;
        bool need_full_parse = (j_in_type != ObJsonInType::JSON_BIN);
        if (!need_full_parse) {
          if (OB_FAIL(get_type_from_raw_bin(json_bin, j_type))) {
            LOG_WARN("fail to get json type from binary", K(ret), K(json_bin.length()));
          } else if (j_type == ObJsonNodeType::J_OPAQUE) {
            // OPAQUE needs field_type to determine sub-type, fallback to full parse
            need_full_parse = true;
          }
        }
        if (OB_SUCC(ret) && need_full_parse) {
          if (OB_FAIL(ObJsonBaseFactory::get_json_base(
                  allocator, json_bin, j_in_type, j_in_type, j_base, 0, json_max_depth_config))) {
            LOG_WARN("fail to get json base", K(ret), K(json_doc_type), K(json_bin), K(j_in_type));
            if (ret == OB_ERR_INVALID_JSON_TEXT_IN_PARAM) {
              ret = OB_ERR_INVALID_JSON_TEXT;
              LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
            }
          } else {
            j_type = j_base->json_type();
            if (j_type == ObJsonNodeType::J_OPAQUE) {
              type_idx = opaque_index(j_base->field_type());
            }
          }
        }
        if (OB_SUCC(ret) && j_type != ObJsonNodeType::J_OPAQUE) {
          if (j_type == ObJsonNodeType::J_MYSQL_DATE) {
            j_type = ObJsonNodeType::J_DATE;
          } else if (j_type == ObJsonNodeType::J_MYSQL_DATETIME) {
            j_type = ObJsonNodeType::J_DATETIME;
          }
          type_idx = static_cast<uint32_t>(j_type);
        }
      }

      // set result
      if (OB_SUCC(ret)) {
        if (is_null) {
          res.set_null();
        } else {
          const char *j_type_str = json_type_string_map[type_idx];
          uint32_t j_type_str_len = strlen(j_type_str);
          char *buf = expr.get_str_res_mem(ctx, j_type_str_len + 1);
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory for result buf", K(ret), K(j_type_str), K(j_type_str_len));
          } else {
            MEMMOVE(buf, j_type_str, j_type_str_len);
            res.set_string(buf, j_type_str_len);
          }
        }
      }
    }
  }

  return ret;
}

int ObExprJsonType::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_type;
  return OB_SUCCESS;
}


}
}