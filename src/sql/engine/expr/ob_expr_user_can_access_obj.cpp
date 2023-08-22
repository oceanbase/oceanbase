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
#include "objit/common/ob_item_type.h"
#include "common/data_buffer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "share/schema/ob_synonym_mgr.h"
#include "share/ob_get_compat_mode.h"
#include "lib/worker.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprUserCanAccessObj::ObExprUserCanAccessObj(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_USER_CAN_ACCESS_OBJ,
                         N_USER_CAN_ACCESS_OBJ, 3, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUserCanAccessObj::~ObExprUserCanAccessObj()
{
}

int ObExprUserCanAccessObj::calc_result_type3(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprResType &type3,
                                             ObExprTypeCtx &type_ctx) const
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

/* 在oracle上验证过
   存取权限，只有select，ins/del/upd。ddl的相关权限(alter/create/drop)不行
   和报错table not exists 不太一样。
   报错和acess权限的区别：
       报错：ddl, dml 相关的权限都包括了。
       存取权限只能用dml权限。 */
int ObExprUserCanAccessObj::build_raw_obj_priv(
    uint64_t obj_type,
    share::ObRawObjPrivArray &raw_obj_priv_array)
{
  int ret = OB_SUCCESS;
  switch (obj_type) {
    /* 对于trigger，传入的object id是基表或者视图的object id*/
    case static_cast<uint64_t>(share::schema::ObObjectType::TABLE):
    case static_cast<uint64_t>(share::schema::ObObjectType::TRIGGER):
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_SELECT));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_INSERT));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_DELETE));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_UPDATE));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_INDEX));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_REFERENCES));
      break;

    case static_cast<uint64_t>(share::schema::ObObjectType::VIEW):
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_SELECT));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_INSERT));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_DELETE));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_UPDATE));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_REFERENCES));
      break;

    case static_cast<uint64_t>(share::schema::ObObjectType::SEQUENCE):
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_SELECT));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_ALTER));
      break;

    case static_cast<uint64_t>(share::schema::ObObjectType::FUNCTION):
    case static_cast<uint64_t>(share::schema::ObObjectType::PROCEDURE):
    case static_cast<uint64_t>(share::schema::ObObjectType::PACKAGE):
    case static_cast<uint64_t>(share::schema::ObObjectType::TYPE):
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_EXECUTE));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_DEBUG));
      break;
    case static_cast<uint64_t>(share::schema::ObObjectType::DIRECTORY):
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_READ));
      OZ (raw_obj_priv_array.push_back(OBJ_PRIV_ID_WRITE));
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(obj_type));
      break;
  }
  return ret;
}

int ObExprUserCanAccessObj::build_real_obj_type_for_sym(
    uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard *schema_guard,
    uint64_t &obj_type,
    uint64_t &obj_id)
{
  int ret = OB_SUCCESS;
  const share::schema::ObSimpleSynonymSchema *synonym_info = NULL;
  CK (schema_guard != NULL);
  if (OB_SUCC(ret)
      && obj_type == static_cast<uint64_t>(share::schema::ObObjectType::SYNONYM)) {
    OZ (schema_guard->get_simple_synonym_info(tenant_id, obj_id, synonym_info));
    if (OB_SUCC(ret)) {
      if (synonym_info == NULL) {
        ret = OB_SYNONYM_NOT_EXIST;
      } else {
        const share::schema::ObSimpleTableSchemaV2 *simple_table_schema = NULL;
        uint64_t db_id = synonym_info->get_object_database_id();
        const ObString &obj_name = synonym_info->get_object_name_str();
        OZ (schema_guard->get_simple_table_schema(tenant_id,
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
        //check sequence
        if (OB_TABLE_NOT_EXIST == ret) {
          bool exists = false;
          uint64_t sequence_id = OB_INVALID_ID;
          bool is_system_generated = false;
          ret = OB_SUCCESS;
          OZ (schema_guard->check_sequence_exist_with_name(tenant_id, db_id, obj_name,
                                                           exists, sequence_id, is_system_generated));
          if (OB_SUCC(ret)) {
            if (exists) {
              obj_type = static_cast<uint64_t>(share::schema::ObObjectType::SEQUENCE);
              obj_id = sequence_id;
            } else {
              ret = OB_TABLE_NOT_EXIST;
            }
          }
        }
        //check procedure/function
        if (OB_TABLE_NOT_EXIST == ret) {
          uint64_t routine_id = OB_INVALID_ID;
          ret = OB_SUCCESS;
          OZ (schema_guard->get_standalone_procedure_id(tenant_id, db_id, obj_name, routine_id));
          if (OB_SUCC(ret)) {
            if (routine_id != OB_INVALID_ID) {
              obj_type = static_cast<uint64_t>(share::schema::ObObjectType::PROCEDURE);
              obj_id = routine_id;
            } else {
              ret = OB_TABLE_NOT_EXIST;
            }
          }
          if (ret == OB_TABLE_NOT_EXIST) {
            ret = OB_SUCCESS;
            OZ (schema_guard->get_standalone_function_id(tenant_id, db_id, obj_name, routine_id));
            if (OB_SUCC(ret)) {
              if (routine_id != OB_INVALID_ID) {
                obj_type = static_cast<uint64_t>(share::schema::ObObjectType::FUNCTION);
                obj_id = routine_id;
              } else {
                ret = OB_TABLE_NOT_EXIST;
              }
            }
          }
        }

        //check package
        if (OB_TABLE_NOT_EXIST == ret) {
          int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                          : COMPATIBLE_MYSQL_MODE;
          uint64_t package_id = OB_INVALID_ID;
          ret = OB_SUCCESS;
          OZ (schema_guard->get_package_id(tenant_id, db_id, obj_name,
                                          share::schema::PACKAGE_TYPE,
                                          compatible_mode, package_id));
          if (OB_SUCC(ret)) {
            if (package_id != OB_INVALID_ID) {
              obj_type = static_cast<uint64_t>(share::schema::ObObjectType::PACKAGE);
              obj_id = package_id;
            } else {
              ret = OB_TABLE_NOT_EXIST;
            }
          }
        }

        //check type
        if (OB_TABLE_NOT_EXIST == ret) {
          uint64_t udt_id = 0;
          ret = OB_SUCCESS;
          OZ (schema_guard->get_udt_id(tenant_id, db_id, OB_INVALID_ID, obj_name, udt_id));
          if (OB_SUCC(ret)) {
            if (udt_id != OB_INVALID_ID) {
              obj_type = static_cast<uint64_t>(share::schema::ObObjectType::TYPE);
              obj_id = udt_id;
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

int ObExprUserCanAccessObj::check_user_access_obj(
    share::schema::ObSchemaGetterGuard *schema_guard,
    ObSQLSessionInfo *session,
    uint64_t obj_type,
    uint64_t obj_id,
    uint64_t owner_id,
    bool &can_access)
{
  int ret = OB_SUCCESS;
  bool syn_base_obj_exists = true;
  can_access = false;
  share::ObRawObjPrivArray raw_obj_priv_array;
  CK (schema_guard != NULL);
  CK (session != NULL);
  if (OB_SUCC(ret)) {
    if (obj_type == static_cast<uint64_t>(share::schema::ObObjectType::SYNONYM)) {
      OZ (build_real_obj_type_for_sym(session->get_effective_tenant_id(),
                                      schema_guard, obj_type, obj_id),
                                      obj_type, obj_id);
      /* 忽略synonym对于的object不存在的错误 */
      if (ret == OB_TABLE_NOT_EXIST) {
        ret = OB_SUCCESS;
        syn_base_obj_exists = false;
        OX (can_access = true);
      }
    }
  }
  if (OB_SUCC(ret) && syn_base_obj_exists) {
    uint64_t dbid = OB_INVALID_ID;
    const ObUserInfo *user_info = schema_guard->get_user_info(
                                    session->get_effective_tenant_id(),
                                    session->get_priv_user_id());
    OZ (build_raw_obj_priv(obj_type, raw_obj_priv_array));

    /* get dbid of same name as user */
    OZ (schema_guard->get_database_id(session->get_effective_tenant_id(),
                                      user_info ? user_info->get_user_name_str() : session->get_user_name(),
                                      dbid));
    OZX2 (ObOraSysChecker::check_ora_obj_privs_or(
                *schema_guard,
                session->get_effective_tenant_id(),
                dbid,   /* userid */
                user_info ? user_info->get_user_id() : session->get_user_id(),
                ObString(""),
                obj_id,    /* object id */
                OBJ_LEVEL_FOR_TAB_PRIV,
                obj_type,    /* object type */
                raw_obj_priv_array,
                CHECK_FLAG_NORMAL, /* check_flag */
                owner_id,    /* owner id */
                session->get_enable_role_array()),
                OB_ERR_NO_PRIVILEGE, OB_TABLE_NOT_EXIST);
  }
  if (OB_SUCC(ret)) {
    OX (can_access = true);
  } else {
    /* 如果无任何权限，则return 0 */
    if (ret == OB_ERR_NO_SYS_PRIVILEGE
       || ret == OB_ERR_NO_PRIVILEGE) {
      ret = OB_SUCCESS;
      OX (can_access = false);
    }
  }
  return ret;
}

int ObExprUserCanAccessObj::eval_user_can_access_obj(const ObExpr &expr,
                                                     ObEvalCtx &ctx,
                                                     ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatum &obj_type = expr.locate_param_datum(ctx, 0);
    ObDatum &obj_id = expr.locate_param_datum(ctx, 1);
    uint64_t database_id = expr.locate_param_datum(ctx, 2).get_uint64();

    const ObSimpleDatabaseSchema *db_schema = NULL;
    uint64_t owner_id = OB_INVALID_ID;
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    share::schema::ObSchemaGetterGuard *schema_guard =
        ctx.exec_ctx_.get_virtual_table_ctx().schema_guard_;

    OX (res_datum.set_int(0));
    CK(OB_NOT_NULL(session));
    CK(OB_NOT_NULL(schema_guard));
    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = session->get_effective_tenant_id();
      // In 4.0, database_id and owner_id are different, so we first transfer database_id to user_id.
      lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
      if (FAILEDx(schema_guard->get_database_schema(
          tenant_id, database_id, db_schema))) {
        LOG_WARN("fail to get database", KR(ret), K(tenant_id), K(database_id));
      } else if (OB_ISNULL(db_schema)) {
        // target owner not exist
        LOG_TRACE("database not exist", KR(ret), K(tenant_id), K(database_id));
      } else if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
        LOG_WARN("fail to get tenant mode", KR(ret), K(tenant_id));
      } else {
        // treat some mysql db as `sys` user in oracle to check priv
        ObString user_name = (lib::Worker::CompatMode::ORACLE == compat_mode
                              && (is_oceanbase_sys_database_id(database_id)
                                  || is_recyclebin_database_id(database_id)
                                  || is_public_database_id(database_id))) ?
                             ObString(OB_ORA_SYS_USER_NAME) : db_schema->get_database_name();
        ObString host_name(OB_DEFAULT_HOST_NAME);
        if (OB_FAIL(schema_guard->get_user_id(tenant_id, user_name, host_name, owner_id))) {
          LOG_WARN("fail to get user_id", KR(ret), K(tenant_id), K(user_name));
        } else if (OB_INVALID_ID == owner_id) {
          LOG_TRACE("user not exist", KR(ret), K(tenant_id), K(user_name));
        } else {
          bool can_access;
          OZ (check_user_access_obj(schema_guard,
                                    session,
                                    obj_type.get_uint64(),
                                    obj_id.get_uint64(),
                                    owner_id,
                                    can_access));
          OX (res_datum.set_int(static_cast<int64_t>(can_access)));
        }
      }
    }
  }
  return ret;
}

int ObExprUserCanAccessObj::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(3 == rt_expr.arg_cnt_);
  OX(rt_expr.eval_func_ = eval_user_can_access_obj);
  return ret;
}

} /* sql */
} /* oceanbase */
