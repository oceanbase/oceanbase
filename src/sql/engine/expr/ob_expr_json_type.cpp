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
 * This file contains implementation for json_type.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_type.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"

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

int ObExprJsonType::calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta, bool has_lob_header,
                         ObIAllocator *allocator, uint32_t &type_idx, bool &is_null)
{
  INIT_SUCC(ret);
  ObObjType type = meta.type_;
  ObCollationType cs_type = meta.cs_type_;

  if (data.is_null()) {
    is_null = true;
  } else {
    switch (type) {
      case ObVarcharType:
      case ObCharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObJsonType:
      case ObLongTextType: {
        common::ObString j_str = data.get_string(); // json text or json binary
        ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
        ObIJsonBase *j_base = NULL;
        if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
          LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
        } else if (j_str.length() == 0) {
          ret = OB_ERR_INVALID_JSON_TEXT;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(*allocator, data, meta, has_lob_header, j_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(j_str));
        } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, j_in_type,
            j_in_type, j_base))) {
          LOG_WARN("fail to get json base", K(ret), K(type), K(j_str), K(j_in_type));
          if (ret == OB_ERR_INVALID_JSON_TEXT_IN_PARAM) {
            ret = OB_ERR_INVALID_JSON_TEXT;
            LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
          }
        } else {
          ObJsonNodeType j_type = j_base->json_type();
          type_idx = static_cast<uint32_t>(j_type);
          if (j_type == ObJsonNodeType::J_OPAQUE) {
            type_idx = opaque_index(j_base->field_type());
          }
        }
        break;
      }

      case ObNullType: {
        is_null = true;
        break;
      }
      
      default: {
        ret = OB_ERR_INVALID_JSON_TEXT;
        LOG_WARN("Incorrect type for function json_type", K(ret), K(type));
        break;
      }
    }
  }

  return ret;
}                      

// for new sql engine
int ObExprJsonType::eval_json_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *datum = NULL;
  ObExpr *arg = expr.args_[0];

  if (OB_FAIL(arg->eval(ctx, datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else {
    uint32_t type_idx = 0;
    bool is_null = false;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObIAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(calc(ctx, *datum, arg->datum_meta_, arg->obj_meta_.has_lob_header(), &tmp_allocator, type_idx, is_null))) {
      LOG_WARN("fail to calc json type result", K(ret), K(arg->datum_meta_));
    } else if (is_null) {
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