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

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_user_can_access_obj.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/parser/ob_item_type.h"
#include "common/data_buffer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "share/schema/ob_synonym_mgr.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprUserCanAccessObj::ObExprUserCanAccessObj(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_USER_CAN_ACCESS_OBJ, N_USER_CAN_ACCESS_OBJ, 3, NOT_ROW_DIMENSION)
{}

ObExprUserCanAccessObj::~ObExprUserCanAccessObj()
{}

int ObExprUserCanAccessObj::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_int();
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type1.set_calc_type(ObUInt64Type);
  type2.set_calc_type(ObUInt64Type);
  type3.set_calc_type(ObUInt64Type);
  ObExprOperator::calc_result_flag3(type, type1, type2, type3);
  return ret;
}

int ObExprUserCanAccessObj::build_raw_obj_priv(uint64_t obj_type, share::ObRawObjPrivArray& raw_obj_priv_array)
{
  int ret = OB_SUCCESS;
  switch (obj_type) {
    case static_cast<uint64_t>(share::schema::ObObjectType::TABLE):
    case static_cast<uint64_t>(share::schema::ObObjectType::TRIGGER):
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_SELECT));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_INSERT));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_DELETE));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_UPDATE));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_INDEX));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_REFERENCES));
      break;

    case static_cast<uint64_t>(share::schema::ObObjectType::VIEW):
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_SELECT));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_INSERT));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_DELETE));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_UPDATE));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_REFERENCES));
      break;

    case static_cast<uint64_t>(share::schema::ObObjectType::SEQUENCE):
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_SELECT));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_ALTER));
      break;

    case static_cast<uint64_t>(share::schema::ObObjectType::FUNCTION):
    case static_cast<uint64_t>(share::schema::ObObjectType::PROCEDURE):
    case static_cast<uint64_t>(share::schema::ObObjectType::PACKAGE):
    case static_cast<uint64_t>(share::schema::ObObjectType::TYPE):
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_EXECUTE));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_DEBUG));
      break;
    case static_cast<uint64_t>(share::schema::ObObjectType::DIRECTORY):
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_READ));
      OZ(raw_obj_priv_array.push_back(OBJ_PRIV_ID_WRITE));
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(obj_type));
      break;
  }
  return ret;
}

int ObExprUserCanAccessObj::build_real_obj_type_for_sym(
    uint64_t tenant_id, share::schema::ObSchemaGetterGuard* schema_guard, uint64_t& obj_type, uint64_t& obj_id)
{
  int ret = OB_SUCCESS;
  const share::schema::ObSimpleSynonymSchema* synonym_info = NULL;
  CK(schema_guard != NULL);
  if (OB_SUCC(ret) && obj_type == static_cast<uint64_t>(share::schema::ObObjectType::SYNONYM)) {
    OZ(schema_guard->get_simple_synonym_info(tenant_id, obj_id, synonym_info));
    if (OB_SUCC(ret)) {
      if (synonym_info == NULL) {
        ret = OB_SYNONYM_NOT_EXIST;
      } else {
        const share::schema::ObSimpleTableSchemaV2* simple_table_schema = NULL;
        uint64_t db_id = synonym_info->get_object_database_id();
        const ObString& obj_name = synonym_info->get_object_name_str();
        OZ(schema_guard->get_simple_table_schema(tenant_id,
            db_id,
            obj_name,
            false, /* is_index */
            simple_table_schema));
        if (OB_SUCC(ret)) {
          if (simple_table_schema != NULL) {
            obj_type = static_cast<uint64_t>(share::schema::ObObjectType::TABLE);
            obj_id = simple_table_schema->get_table_id();
          } else {
            ret = OB_TABLE_NOT_EXIST;
          }
        }
        // check sequence
        if (OB_TABLE_NOT_EXIST == ret) {
          bool exists = false;
          uint64_t sequence_id = OB_INVALID_ID;
          OZ(schema_guard->check_sequence_exist_with_name(tenant_id, db_id, obj_name, exists, sequence_id));
          if (OB_SUCC(ret)) {
            if (exists) {
              obj_type = static_cast<uint64_t>(share::schema::ObObjectType::SEQUENCE);
              obj_id = sequence_id;
            } else {
              ret = OB_TABLE_NOT_EXIST;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObExprUserCanAccessObj::check_user_access_obj(share::schema::ObSchemaGetterGuard* schema_guard,
    ObSQLSessionInfo* session, uint64_t obj_type, uint64_t obj_id, uint64_t owner_id, bool& can_access)
{
  int ret = OB_SUCCESS;
  bool syn_base_obj_exists = true;
  can_access = false;
  share::ObRawObjPrivArray raw_obj_priv_array;
  CK(schema_guard != NULL);
  CK(session != NULL);
  if (OB_SUCC(ret)) {
    if (obj_type == static_cast<uint64_t>(share::schema::ObObjectType::SYNONYM)) {
      OZ(build_real_obj_type_for_sym(session->get_effective_tenant_id(), schema_guard, obj_type, obj_id),
          obj_type,
          obj_id);
      if (ret == OB_TABLE_NOT_EXIST) {
        ret = OB_SUCCESS;
        syn_base_obj_exists = false;
        OX(can_access = true);
      }
    }
  }
  if (OB_SUCC(ret) && syn_base_obj_exists) {
    OZ(build_raw_obj_priv(obj_type, raw_obj_priv_array));
    OZX2(ObOraSysChecker::check_ora_obj_privs_or(*schema_guard,
             session->get_effective_tenant_id(),
             session->get_user_id(), /* userid */
             ObString(""),
             obj_id, /* object id */
             COL_ID_FOR_TAB_PRIV,
             obj_type, /* object type */
             raw_obj_priv_array,
             CHECK_FLAG_NORMAL, /* check_flag */
             owner_id,          /* owner id */
             session->get_enable_role_array()),
        OB_ERR_NO_PRIVILEGE,
        OB_TABLE_NOT_EXIST);
  }
  if (OB_SUCC(ret)) {
    OX(can_access = true);
  } else {
    if (ret == OB_ERR_NO_SYS_PRIVILEGE || ret == OB_ERR_NO_PRIVILEGE) {
      ret = OB_SUCCESS;
      OX(can_access = false);
    }
  }
  return ret;
}

int ObExprUserCanAccessObj::calc_result3(ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2,
    const common::ObObj& arg3, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    share::schema::ObSchemaGetterGuard* schema_guard = expr_ctx.exec_ctx_->get_virtual_table_ctx().schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("wrong schema", K(ret));
    } else {
      ObObj res;
      bool can_access;
      OZ(check_user_access_obj(
          schema_guard, expr_ctx.my_session_, arg1.get_uint64(), arg2.get_uint64(), arg3.get_uint64(), can_access));
      OX(result.set_int(static_cast<int>(can_access)));
    }
  }
  return ret;
}

int ObExprUserCanAccessObj::eval_user_can_access_obj(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* ns = NULL;
  ObDatum* para = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatum& obj_type = expr.locate_param_datum(ctx, 0);
    ObDatum& obj_id = expr.locate_param_datum(ctx, 1);
    ObDatum& owner_id = expr.locate_param_datum(ctx, 2);

    ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
    CK(OB_NOT_NULL(session));
    share::schema::ObSchemaGetterGuard* schema_guard = ctx.exec_ctx_.get_virtual_table_ctx().schema_guard_;
    CK(OB_NOT_NULL(schema_guard));
    if (OB_SUCC(ret)) {
      OX(res_datum.set_int(0));
      bool can_access;
      OZ(check_user_access_obj(
          schema_guard, session, obj_type.get_uint64(), obj_id.get_uint64(), owner_id.get_uint64(), can_access));
      OX(res_datum.set_int(static_cast<int64_t>(can_access)));
    }
  }
  return ret;
}

int ObExprUserCanAccessObj::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(3 == rt_expr.arg_cnt_);
  OX(rt_expr.eval_func_ = eval_user_can_access_obj);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
