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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/charset/ob_charset.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/schema/ob_column_schema.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_get_compat_mode.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/object/ob_obj_type.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_column_ref.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_part_func_checker.h"
#include "sql/resolver/expr/ob_raw_expr_part_expr_checker.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/ob_sql_utils.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_stmt.h"
#include "observer/ob_server_struct.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/parser/ob_parser_utils.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/engine/expr/ob_expr_unistr.h"
#include "sql/resolver/dml/ob_inlist_resolver.h"
#include "lib/charset/ob_ctype.h"
#include "sql/engine/expr/ob_expr_cast.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace obrpc;
using namespace pl;
namespace sql
{
ObItemType ObResolverUtils::item_type_ = T_INVALID;

const ObString ObResolverUtils::stmt_type_string[] = {
#define OB_STMT_TYPE_DEF(stmt_type, priv_check_func, id, action_type) ObString::make_string(#stmt_type),
#include "sql/resolver/ob_stmt_type.h"
#undef OB_STMT_TYPE_DEF
};

int ObResolverUtils::get_user_type(ObIAllocator *allocator,
                                   ObSQLSessionInfo *session_info,
                                   ObMySQLProxy *sql_proxy,
                                   share::schema::ObSchemaGetterGuard *schema_guard,
                                   pl::ObPLPackageGuard &package_guard,
                                   uint64_t type_id,
                                   const pl::ObUserDefinedType *&user_type)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(allocator));
  CK (OB_NOT_NULL(session_info));
  CK (OB_NOT_NULL(sql_proxy));
  CK (OB_NOT_NULL(schema_guard));
  OX (user_type = NULL);
  if (OB_SUCC(ret)) {
    pl::ObPLResolveCtx resolve_ctx(
      *allocator, *session_info, *schema_guard, package_guard, *sql_proxy, false);
    if (!package_guard.is_inited()) {
      OZ (package_guard.init());
    }
    OZ (resolve_ctx.get_user_type(type_id, user_type, allocator));
  }
  return ret;
}

int ObResolverUtils::get_all_function_table_column_names(const TableItem &table_item,
                                                         ObResolverParams &params,
                                                         ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  ObRawExpr *table_expr = NULL;
  ObPLPackageGuard *package_guard = nullptr;
  const ObUserDefinedType *user_type = NULL;
  ObExecContext *exec_ctx = params.session_info_->get_cur_exec_ctx();
  CK (OB_NOT_NULL(exec_ctx));
  OZ (exec_ctx->get_package_guard(package_guard));
  CK (OB_NOT_NULL(package_guard));
  CK (OB_LIKELY(table_item.is_function_table()));
  CK (OB_NOT_NULL(table_expr = table_item.function_table_expr_));
  CK (table_expr->get_udt_id() != OB_INVALID_ID);

  CK (OB_NOT_NULL(params.schema_checker_));
  OZ (ObResolverUtils::get_user_type(
    params.allocator_, params.session_info_, params.sql_proxy_,
    params.schema_checker_->get_schema_guard(),
    *package_guard,
    table_expr->get_udt_id(), user_type));
  CK (OB_NOT_NULL(user_type));
  if (OB_SUCC(ret) && !user_type->is_collection_type()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("function table get udf with return type not table type",
             K(ret), K(user_type->is_collection_type()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "user define type is not collation type in function table");
  }
  const ObCollectionType *coll_type = NULL;
  CK (OB_NOT_NULL(coll_type = static_cast<const ObCollectionType*>(user_type)));
  if (OB_SUCC(ret)
      && !coll_type->get_element_type().is_obj_type()
      && !coll_type->get_element_type().is_record_type()
      && !(coll_type->get_element_type().is_opaque_type()
            && coll_type->get_element_type().get_user_type_id() == T_OBJ_XML)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not suppoert type in table function", K(ret), KPC(coll_type));
    ObString err;
    err.write(coll_type->get_name().ptr(), coll_type->get_name().length());
    err.write(" collation type in table function\0", sizeof(" collation type in table function\0"));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err.ptr());
  }
  if (OB_SUCC(ret) && (coll_type->get_element_type().is_obj_type()
                      || coll_type->get_element_type().is_opaque_type())) {
    OZ (column_names.push_back(ObString("COLUMN_VALUE")));
  }
  if (OB_SUCC(ret) && coll_type->get_element_type().is_record_type()) {
    const ObRecordType *record_type = NULL;
    const ObUserDefinedType *user_type = NULL;
    CK (OB_NOT_NULL(params.schema_checker_));
    OZ (ObResolverUtils::get_user_type(
      params.allocator_, params.session_info_, params.sql_proxy_,
      params.schema_checker_->get_schema_guard(),
      *package_guard,
      coll_type->get_element_type().get_user_type_id(), user_type));
    CK (OB_NOT_NULL(user_type));
    CK (user_type->is_record_type());
    CK (OB_NOT_NULL(record_type = static_cast<const ObRecordType *>(user_type)));
    for (int64_t i = 0; OB_SUCC(ret) && i < record_type->get_member_count(); ++i) {
      ObString name;
      const ObString *member_name = record_type->get_record_member_name(i);
      CK (OB_NOT_NULL(member_name));

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (PL_TYPE_PACKAGE == user_type->get_type_from()) {
        OZ (ob_write_string(*params.allocator_, *member_name, name));
      } else {
        name = *member_name;
      }

      OZ (column_names.push_back(name));
    }
  }
  return ret;
}

int ObResolverUtils::check_function_table_column_exist(const TableItem &table_item,
                                                       ObResolverParams &params,
                                                       const ObString &column_name)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 16> column_names;
  bool exist = false;
  OZ (get_all_function_table_column_names(table_item, params, column_names));
  for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
    if (ObCharset::case_compat_mode_equal(column_names.at(i), column_name)) {
      exist = true;
      break;
    }
  }
  if (!exist) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("not found column in table function", K(ret), K(column_name));
  }
  return ret;
}



int ObResolverUtils::check_json_table_column_exists(const TableItem &table_item,
                                                    ObResolverParams &params,
                                                    const ObString &column_name,
                                                    bool& exists)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 16> column_names;
  bool exist = false;
  CK(table_item.is_json_table());
  CK (OB_NOT_NULL(table_item.json_table_def_));

  ObJsonTableDef* jt_def = table_item.json_table_def_;

  for (size_t i = 0; OB_SUCC(ret) && i < jt_def->all_cols_.count(); ++i) {
    ObJtColBaseInfo* col_info = jt_def->all_cols_.at(i);
    if (col_info->col_type_ != NESTED_COL_TYPE) {
      ObString& cur_column_name = col_info->col_name_;
      if (ObCharset::case_compat_mode_equal(cur_column_name, column_name)) {
        exists = true;
        break;
      }
    }
  }

  if (OB_SUCC(ret) && !exists) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("not found column in table function", K(ret), K(column_name));
  }
  return ret;
}

int ObResolverUtils::collect_schema_version(share::schema::ObSchemaGetterGuard &schema_guard,
                                            const ObSQLSessionInfo *session_info,
                                            ObRawExpr *expr,
                                            ObIArray<ObSchemaObjVersion> &dependency_objects,
                                            bool is_called_in_sql,
                                            ObIArray<uint64_t> *dep_db_array)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session_info));
  CK (OB_NOT_NULL(expr));

  if (OB_FAIL(ret)) {
  } else if (T_OP_GET_PACKAGE_VAR == expr->get_expr_type()) {
    uint64_t package_id = OB_INVALID_ID;
    const ObPackageInfo *spec_info = NULL;
    const ObPackageInfo *body_info = NULL;
    ObSchemaObjVersion ver;
    CK (expr->get_param_count() >= 3);
    OX (package_id = static_cast<const ObConstRawExpr *>(expr->get_param_expr(0))->get_value().get_uint64());
    if (package_id != OB_INVALID_ID) {
      OZ (pl::ObPLPackageManager::get_package_schema_info(schema_guard, package_id, spec_info, body_info));
    }
    if (OB_NOT_NULL(spec_info)) {
      OX (ver.object_id_ = spec_info->get_package_id());
      OX (ver.version_ = spec_info->get_schema_version());
      OX (ver.object_type_ = DEPENDENCY_PACKAGE);
      OZ (dependency_objects.push_back(ver));
      if (OB_NOT_NULL(dep_db_array)) {
        OZ (dep_db_array->push_back(spec_info->get_database_id()));
      }
    }
    if (OB_NOT_NULL(body_info)) {
      OX (ver.object_id_ = body_info->get_package_id());
      OX (ver.version_ = body_info->get_schema_version());
      OX (ver.object_type_ = DEPENDENCY_PACKAGE_BODY);
      OZ (dependency_objects.push_back(ver));
      if (OB_NOT_NULL(dep_db_array)) {
        OZ (dep_db_array->push_back(body_info->get_database_id()));
      }
    }
  } else if (T_FUN_UDF == expr->get_expr_type()) {
    ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr*>(expr);
    ObSchemaObjVersion ver;
    uint64_t database_id = OB_INVALID_ID;
    CK (OB_NOT_NULL(udf_expr));
    if (OB_SUCC(ret) && udf_expr->need_add_dependency()) {
      OZ (schema_guard.get_database_id(session_info->get_effective_tenant_id(),
                                        udf_expr->get_database_name().empty() ? session_info->get_database_name() : udf_expr->get_database_name(),
                                        database_id));
      if (OB_SUCC(ret)) {
        bool exist = false;
        uint64_t object_db_id = OB_INVALID_ID;
        ObSchemaChecker schema_checker;
        ObSynonymChecker synonym_checker;
        ObString object_name;
        OZ (schema_checker.init(schema_guard, session_info->get_sessid()));
        OZ (ObResolverUtils::resolve_synonym_object_recursively(schema_checker,
                                                                synonym_checker,
                                                                session_info->get_effective_tenant_id(),
                                                                database_id,
                                                                udf_expr->get_func_name(),
                                                                object_db_id, object_name, exist));
        if (OB_SUCC(ret) && exist) {
          for (int64_t i = 0; OB_SUCC(ret) && i < synonym_checker.get_synonym_ids().count(); ++i) {
            int64_t schema_version = OB_INVALID_VERSION;
            uint64_t obj_id = synonym_checker.get_synonym_ids().at(i);
            uint64_t dep_db_id = synonym_checker.get_database_ids().at(i);
            ObSchemaObjVersion syn_version;
            OZ (schema_guard.get_schema_version(SYNONYM_SCHEMA,
                                                  session_info->get_effective_tenant_id(),
                                                  obj_id,
                                                  schema_version));
            OX (syn_version.object_id_ = obj_id);
            OX (syn_version.version_ = schema_version);
            OX (syn_version.object_type_ = DEPENDENCY_SYNONYM);
            OZ (dependency_objects.push_back(syn_version));
            if (OB_NOT_NULL(dep_db_array)) {
              OZ (dep_db_array->push_back(dep_db_id));
            }
          }
        }
      }
      ObArray<ObSchemaObjVersion> vers;
      OZ (udf_expr->get_schema_object_version(schema_guard, vers));
      for (int64_t i = 0; OB_SUCC(ret) && i < vers.count(); ++i) {
        OZ (dependency_objects.push_back(vers.at(i)));
        if (OB_NOT_NULL(dep_db_array)) {
          OZ (dep_db_array->push_back(database_id));
        }
      }
    }
    OX (expr->set_is_called_in_sql(is_called_in_sql));
  } else if (T_FUN_PL_OBJECT_CONSTRUCT == expr->get_expr_type()) {
    ObObjectConstructRawExpr *object_expr = static_cast<ObObjectConstructRawExpr*>(expr);
    ObSchemaObjVersion ver;
    CK (OB_NOT_NULL(object_expr));
    if (OB_SUCC(ret) && object_expr->need_add_dependency()) {
      OZ (object_expr->get_schema_object_version(ver));
      OZ (dependency_objects.push_back(ver));
      if (OB_NOT_NULL(dep_db_array)) {
        OZ (dep_db_array->push_back(object_expr->get_database_id()));
      }
    }
  } else if (T_FUN_PL_COLLECTION_CONSTRUCT == expr->get_expr_type()) {
    ObCollectionConstructRawExpr *coll_expr = static_cast<ObCollectionConstructRawExpr*>(expr);
    ObSchemaObjVersion ver;
    CK (OB_NOT_NULL(coll_expr));
    if (OB_SUCC(ret) && coll_expr->need_add_dependency()) {
      OZ (coll_expr->get_schema_object_version(ver));
      OZ (dependency_objects.push_back(ver));
      if (OB_NOT_NULL(dep_db_array)) {
        OZ (dep_db_array->push_back(coll_expr->get_database_id()));
      }
    }
  } else if (T_OBJ_ACCESS_REF == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      OZ (collect_schema_version(schema_guard,
                                 session_info,
                                 expr->get_param_expr(i),
                                 dependency_objects,
                                 is_called_in_sql,
                                 dep_db_array));
    }
  }

  return ret;
}

int ObResolverUtils::add_dependency_synonym_object(share::schema::ObSchemaGetterGuard *schema_guard,
                                                   const ObSQLSessionInfo *session_info,
                                                   const ObSynonymChecker &synonym_checker,
                                                   DependenyTableStore &dep_table)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(schema_guard));
  CK (OB_NOT_NULL(session_info));
  for (int64_t i = 0; OB_SUCC(ret) && i < synonym_checker.get_synonym_ids().count(); ++i) {
    int64_t schema_version = OB_INVALID_VERSION;
    uint64_t obj_id = synonym_checker.get_synonym_ids().at(i);
    if (OB_FAIL(schema_guard->get_schema_version(SYNONYM_SCHEMA,
                                                  session_info->get_effective_tenant_id(),
                                                  obj_id,
                                                  schema_version))) {
      LOG_WARN("get schema version failed", K(session_info->get_effective_tenant_id()),
                                            K(obj_id), K(ret));
    } else {
      if (OB_FAIL(dep_table.push_back(ObSchemaObjVersion(obj_id, schema_version, DEPENDENCY_SYNONYM)))) {
        LOG_WARN("add dependency object failed", K(obj_id), K(ret));
      }
    }
  }

  return ret;
}

int ObResolverUtils::add_dependency_synonym_object(share::schema::ObSchemaGetterGuard *schema_guard,
                                                   const ObSQLSessionInfo *session_info,
                                                   const ObSynonymChecker &synonym_checker,
                                                   const pl::ObPLDependencyTable &dep_table)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(schema_guard));
  CK (OB_NOT_NULL(session_info));
  for (int64_t i = 0; OB_SUCC(ret) && i < synonym_checker.get_synonym_ids().count(); ++i) {
    int64_t schema_version = OB_INVALID_VERSION;
    uint64_t obj_id = synonym_checker.get_synonym_ids().at(i);
    if (OB_FAIL(schema_guard->get_schema_version(SYNONYM_SCHEMA,
                                                  session_info->get_effective_tenant_id(),
                                                  obj_id,
                                                  schema_version))) {
      LOG_WARN("get schema version failed", K(session_info->get_effective_tenant_id()),
                                            K(obj_id), K(ret));
    } else {
      ObSchemaObjVersion ver(obj_id, schema_version, DEPENDENCY_SYNONYM);
      if (OB_FAIL(ObPLCompileUnitAST::add_dependency_object_impl(dep_table, ver))) {
        LOG_WARN("add dependency object failed", K(obj_id), K(ret));
      }
    }
  }

  return ret;
}

int ObResolverUtils::resolve_extended_type_info(const ParseNode &str_list_node, ObIArray<ObString>& type_info_array)
{
  int ret = OB_SUCCESS;
  ObString cur_type_info;
  CK(str_list_node.num_child_ > 0);
  for (int64_t i = 0; OB_SUCC(ret) && i < str_list_node.num_child_; ++i) {
    cur_type_info.reset();
    const ParseNode *str_node = str_list_node.children_[i];
    if (OB_ISNULL(str_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid str_node", K(ret), K(str_node), K(i));
    } else if (FALSE_IT(cur_type_info.assign_ptr(str_node->str_value_, static_cast<ObString::obstr_size_t>(str_node->str_len_)))) {
    } else if (OB_FAIL(type_info_array.push_back(cur_type_info))) {
      LOG_WARN("fail to push back type info", K(ret), K(i), K(cur_type_info));
    }
  }
  return ret;
}

int ObResolverUtils::check_extended_type_info(common::ObIAllocator &alloc,
                                              ObIArray<ObString> &type_infos,
                                              ObCollationType ori_cs_type,
                                              const ObString &col_name,
                                              ObObjType col_type,
                                              ObCollationType cs_type,
                                              ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  ObString cur_type_info;
  const ObString &sep = ObCharsetUtils::get_const_str(cs_type, ',');
  int32_t dup_cnt;
  // convert type infos from %ori_cs_type to %cs_type first
  FOREACH_CNT_X(str, type_infos, OB_SUCC(ret)) {
    OZ(ObCharset::charset_convert(alloc, *str, ori_cs_type, cs_type, *str));
  }
  if (OB_SUCC(ret)) {
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < type_infos.count(); ++i) {
    ObString &cur_val = type_infos.at(i);
    int32_t no_sp_len = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, cur_val.ptr(), cur_val.length()));
    cur_val.assign_ptr(cur_val.ptr(), static_cast<ObString::obstr_size_t>(no_sp_len)); //remove tail space
    int32_t char_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, cur_val.ptr(), cur_val.length()));
    if (OB_UNLIKELY(char_len > OB_MAX_INTERVAL_VALUE_LENGTH)) { //max value length
      ret = OB_ER_TOO_LONG_SET_ENUM_VALUE;
      LOG_USER_ERROR(OB_ER_TOO_LONG_SET_ENUM_VALUE, col_name.length(), col_name.ptr());
    } else if (ObSetType == col_type //set can't contain commas
               && 0 != ObCharset::instr(cs_type, cur_val.ptr(), cur_val.length(),
                                        sep.ptr(), sep.length())) {
      ret = OB_ERR_ILLEGAL_VALUE_FOR_TYPE;
      LOG_USER_ERROR(OB_ERR_ILLEGAL_VALUE_FOR_TYPE, "set", cur_val.length(), cur_val.ptr());
    } else {/*do nothing*/}
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_duplicates_in_type_infos(//check duplicate value
                         type_infos, col_name, col_type, cs_type, sql_mode, dup_cnt))) {
    LOG_WARN("fail to check duplicate", K(ret), K(dup_cnt), K(type_infos), K(col_name), K(col_type));
  } else if (OB_FAIL(check_max_val_count(//check value count
                         col_type, col_name, type_infos.count(), dup_cnt))) {
    LOG_WARN("fail to check max val count", K(ret), K(col_type), K(col_name), K(type_infos), K(dup_cnt));
  } else {/*do nothing*/}
  return ret;
}

int ObResolverUtils::check_duplicates_in_type_infos(const ObIArray<common::ObString> &type_infos,
                                                    const ObString &col_name,
                                                    ObObjType col_type,
                                                    ObCollationType cs_type,
                                                    ObSQLMode sql_mode,
                                                    int32_t &dup_cnt)
{
  int ret = OB_SUCCESS;
  dup_cnt = 0;
  for (uint32_t i = 0; OB_SUCC(ret) && i < type_infos.count() - 1; ++i) {
    int32_t pos = i + 1;
    const ObString &cur_val = type_infos.at(i);
    if (OB_FAIL(find_type(type_infos, cs_type, cur_val, pos))) {
      LOG_WARN("fail to find type", K(type_infos), K(cur_val), K(pos));
    } else if (OB_UNLIKELY(pos >= 0)) {
      ++dup_cnt;
      if (is_strict_mode(sql_mode)) {
        ret = OB_ER_DUPLICATED_VALUE_IN_TYPE;
        LOG_USER_ERROR(OB_ER_DUPLICATED_VALUE_IN_TYPE, col_name.length(), col_name.ptr(),
                       cur_val.length(), cur_val.ptr(), ob_sql_type_str(col_type));
      } else {
        LOG_USER_WARN(OB_ER_DUPLICATED_VALUE_IN_TYPE, col_name.length(), col_name.ptr(),
                       cur_val.length(), cur_val.ptr(), ob_sql_type_str(col_type));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_max_val_count(ObObjType type, const ObString &col_name, int64_t val_cnt, int32_t dup_cnt)
{
  int ret = OB_SUCCESS;
  if (ObEnumType == type) {
    if (val_cnt > OB_MAX_ENUM_ELEMENT_NUM) {
      ret = OB_ER_TOO_BIG_ENUM;
      LOG_USER_ERROR(OB_ER_TOO_BIG_ENUM, col_name.length(), col_name.ptr());
    }
  } else if (ObSetType == type) {
    if (val_cnt - dup_cnt > OB_MAX_SET_ELEMENT_NUM) {
      ret = OB_ERR_TOO_BIG_SET;
      LOG_USER_ERROR(OB_ERR_TOO_BIG_SET, col_name.length(), col_name.ptr());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(val_cnt), K(dup_cnt));
  }
  return ret;
}


int ObResolverUtils::check_routine_exists(const ObSQLSessionInfo *session_info,
                                          ObSchemaChecker *schema_checker,
                                          pl::ObPLBlockNS *secondary_namespace,
                                          const ObString &db_name,
                                          const ObString &package_name,
                                          const ObString &routine_name,
                                          const share::schema::ObRoutineType routine_type,
                                          bool &exists,
                                          pl::ObProcType &proc_type,
                                          uint64_t udt_id)
{
  int ret = OB_SUCCESS;
  exists = false;
  proc_type = pl::INVALID_PROC_TYPE;
  if (NULL != secondary_namespace) {
    if (OB_FAIL(secondary_namespace->check_routine_exists(db_name, package_name, routine_name,
                                       routine_type, exists, proc_type, udt_id))) {
      LOG_WARN("failed to check routine exists",
               K(ret), K(db_name), K(package_name),
               K(routine_name), K(udt_id), K(routine_type), K(exists), K(proc_type));
    }
  }
  if (OB_SUCC(ret) && !exists) {
    if (NULL == session_info || NULL == schema_checker) {
      //PL resovler don't have session info and schema checker
    } else {
      if (OB_FAIL(check_routine_exists(*schema_checker,
                                       *session_info,
                                       db_name,
                                       package_name,
                                       routine_name,
                                       routine_type,
                                       exists,
                                       udt_id))) {
       LOG_WARN("failed to check_routine_exists",
                K(ret), K(db_name), K(package_name),
                K(routine_name), K(udt_id), K(routine_type), K(exists), K(proc_type));
     } else if (exists) {
       proc_type = ROUTINE_PROCEDURE_TYPE == routine_type ? pl::STANDALONE_PROCEDURE : pl::STANDALONE_FUNCTION;
     } else { /*do nothing*/ }
   }
  }
  return ret;
}

int ObResolverUtils::check_routine_exists(ObSchemaChecker &schema_checker,
                                            const ObSQLSessionInfo &session_info,
                                            const ObString &db_name,
                                            const ObString &package_name,
                                            const ObString &routine_name,
                                            const share::schema::ObRoutineType routine_type,
                                            bool &exists,
                                            uint64_t udt_id)
{
  int ret = OB_SUCCESS;
  exists = false;
  common::ObArray<const share::schema::ObIRoutineInfo *> routines;
  if (OB_FAIL(get_candidate_routines(schema_checker,
                                     session_info.get_effective_tenant_id(),
                                     session_info.get_database_name(),
                                     db_name,
                                     package_name,
                                     routine_name,
                                     routine_type,
                                     routines,
                                     udt_id))) {
    LOG_WARN("failed to get candidate routines",
             K(ret),
             K(db_name), K(package_name), K(routine_name), K(udt_id), K(routine_type), K(exists));
  } else {
    exists = !routines.empty();
  }
  return ret;
}

int ObResolverUtils::get_candidate_routines(ObSchemaChecker &schema_checker,
  uint64_t tenant_id, const ObString &current_database, const ObString &db_name,
  const ObString &package_name, const ObString &routine_name,
  const share::schema::ObRoutineType routine_type,
  common::ObIArray<const share::schema::ObIRoutineInfo *> &routines,
  uint64_t udt_id, const pl::ObPLResolveCtx *resolve_ctx,
  ObSynonymChecker *outer_synonym_checker)
{
  int ret = OB_SUCCESS;

  uint64_t package_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  uint64_t object_db_id = OB_INVALID_ID;
  ObString object_name;
  ObString real_db_name;
  int64_t compatible_mode = COMPATIBLE_MYSQL_MODE;
  if (OB_SYS_TENANT_ID != tenant_id) {
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
    if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(tenant_id), K(compat_mode), K(ret));
    } else {
      compatible_mode = (lib::Worker::CompatMode::ORACLE == compat_mode) ?
                        COMPATIBLE_ORACLE_MODE : COMPATIBLE_MYSQL_MODE;
    }
  } else {
    compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                            : COMPATIBLE_MYSQL_MODE;
  }

  UNUSED(udt_id);

  OV (!routine_name.empty(), OB_INVALID_ARGUMENT, K(routine_name));
  OV (OB_LIKELY(ROUTINE_PROCEDURE_TYPE == routine_type || ROUTINE_FUNCTION_TYPE == routine_type),
    OB_INVALID_ARGUMENT, K(routine_type));

  if (OB_SUCC(ret)) { // adjust database name for empty.
    real_db_name = db_name;
    if (db_name.empty()) {
      real_db_name = current_database;
      if (real_db_name.empty()) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no db select",
          K(routine_type), K(db_name), K(package_name), K(routine_name), K(ret));
      }
    }
  }

#define TRY_SYNONYM(synonym_name)             \
if ((OB_FAIL(ret) || 0 == routines.count())   \
      && OB_ALLOCATE_MEMORY_FAILED != ret) {  \
  ret = OB_SUCCESS;                           \
  bool exist = false;                         \
  ObSynonymChecker synonym_checker;           \
  OZ (resolve_synonym_object_recursively(     \
    schema_checker, (outer_synonym_checker != NULL) ? *outer_synonym_checker : synonym_checker,          \
    tenant_id, database_id, synonym_name,     \
    object_db_id, object_name, exist));       \
  if (OB_SUCC(ret) && exist) {                \
    need_try_synonym = true;                  \
  }                                           \
}

#define GET_STANDALONE_ROUTINE()                                                              \
  if (OB_FAIL(ret)) {                                                                         \
  } else if (ROUTINE_PROCEDURE_TYPE == routine_type) {                                        \
    if (OB_FAIL(schema_checker.get_standalone_procedure_info(                                 \
          tenant_id, object_db_id, object_name, routine_info))) {                             \
      LOG_WARN("failed to get procedure info",                                                \
            K(ret), K(tenant_id), K(real_db_name), K(routine_name), K(object_name), K(ret));  \
    } else {                                                                                  \
      LOG_DEBUG("success get procedure info",                                                 \
            K(ret), K(tenant_id), K(real_db_name), K(routine_name), K(object_name), K(ret));  \
    }                                                                                         \
  } else {                                                                                    \
    if (OB_FAIL(schema_checker.get_standalone_function_info(                                  \
          tenant_id, object_db_id, object_name, routine_info))) {                             \
      LOG_WARN("failed to get function info",                                                 \
               K(ret), K(tenant_id), K(real_db_name), K(package_name), K(routine_name),       \
               K(object_db_id), K(db_name), K(object_name), K(ret));                          \
    }                                                                                         \
  }                                                                                           \
  if (OB_SUCC(ret) && NULL != routine_info) {                                                 \
    OZ (routines.push_back(routine_info));                                                    \
  }

  if (OB_FAIL(ret)) {
    // do nothing ...
  } else if (package_name.empty()) { // must be standalone procedure/function.
    const share::schema::ObRoutineInfo *routine_info = NULL;
    bool need_try_synonym = false;
    OZ (schema_checker.get_database_id(tenant_id, real_db_name, database_id));
    OX (object_db_id = database_id);
    OX (object_name = routine_name);
    if (OB_SUCC(ret)) {
      GET_STANDALONE_ROUTINE();
      TRY_SYNONYM(routine_name);
    }
    if (OB_SUCC(ret) && need_try_synonym) {
      GET_STANDALONE_ROUTINE();
      if (OB_SUCC(ret) && OB_ISNULL(routine_info) && OB_NOT_NULL(resolve_ctx)) {
        // try dblink synonym
        ObString tmp_name = object_name;
        ObString full_object_name = tmp_name.split_on('@');
        if (full_object_name.empty()) {
          // not a dblink
        } else {
          ObString remote_db_name = full_object_name.split_on('.');
          OZ (resolve_ctx->package_guard_.dblink_guard_.get_routine_infos_with_synonym(resolve_ctx->session_info_,
              resolve_ctx->schema_guard_, tmp_name, remote_db_name, ObString(""), full_object_name, routines));
        }
      }
    }
  } else { // try package routines
    OZ (schema_checker.get_database_id(tenant_id, real_db_name, database_id));
    OX (object_db_id = database_id);
    OX (object_name = package_name);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_checker.get_package_id( // try user package now!
          tenant_id, object_db_id, object_name, compatible_mode, package_id))
        || OB_INVALID_ID == package_id) {
      if (OB_FAIL(schema_checker.get_udt_id( // try user type now!
          tenant_id, object_db_id, OB_INVALID_ID, object_name, package_id))
        || OB_INVALID_ID == package_id) {
        bool need_try_synonym = false;
        TRY_SYNONYM(package_name);
        if (OB_SUCC(ret) && need_try_synonym) {
          if (OB_FAIL(schema_checker.get_package_id( // try synonym user package now!
              tenant_id, object_db_id, object_name, compatible_mode, package_id))
              || OB_INVALID_ID == package_id) {
            if ((is_sys_database_id(object_db_id)
                  && OB_FAIL(schema_checker.get_package_id(OB_SYS_TENANT_ID, object_db_id, object_name, compatible_mode, package_id)))
                || OB_INVALID_ID == package_id) {
              if (OB_FAIL(schema_checker.get_udt_id( // try synonym user type now!
                    tenant_id, object_db_id, OB_INVALID_ID, object_name, package_id))
                  || OB_INVALID_ID == package_id) {
                LOG_WARN("failed to get package id", K(ret));
              } else { // it`s user udt, get udt routines
                OZ (schema_checker.get_udt_routine_infos(
                  tenant_id, object_db_id, package_id, routine_name, routine_type, routines));
              }
            } else { // it`s user pacakge, get package routines
              OZ (schema_checker.get_package_routine_infos(
                tenant_id, package_id, object_db_id, routine_name, routine_type, routines));
            }
          } else { // it`s user pacakge, get package routines
            OZ (schema_checker.get_package_routine_infos(
              tenant_id, package_id, object_db_id, routine_name, routine_type, routines));
          }
        }
      } else { // it`s user udt, get udt routines
        OZ (schema_checker.get_udt_routine_infos(
          tenant_id, object_db_id, package_id, routine_name, routine_type, routines));
      }
    } else { // it`s user pacakge, get package routines
      OZ (schema_checker.get_package_routine_infos(
        tenant_id, package_id, object_db_id, routine_name, routine_type, routines));
    }
    // try system package or udt
    if (OB_FAIL(ret) || OB_INVALID_ID == package_id) {
      if (lib::is_oracle_mode() && (db_name.empty()
          || 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)
          || 0 ==  db_name.case_compare(OB_ORA_SYS_SCHEMA_NAME))) {
        if (OB_FAIL(schema_checker.get_package_id( // try system pacakge
            OB_SYS_TENANT_ID, OB_SYS_DATABASE_NAME, object_name, compatible_mode, package_id))
            || OB_INVALID_ID == package_id) {
          if (OB_FAIL(schema_checker.get_udt_id( // try system udt
              OB_SYS_TENANT_ID, OB_SYS_DATABASE_NAME, object_name, package_id))
              || OB_INVALID_ID == package_id) {
            LOG_WARN("failed to get package id", K(ret));
          } else { // it`s system udt, get udt routines
            OZ (schema_checker.get_udt_routine_infos(OB_SYS_TENANT_ID,
              package_id, OB_SYS_DATABASE_NAME, routine_name, routine_type, routines));
          }
        } else { // it`s system pacakge, get package rotuiens.
          OZ (schema_checker.get_package_routine_infos(OB_SYS_TENANT_ID,
            package_id, OB_SYS_DATABASE_NAME, routine_name, routine_type, routines));
        }
      } else if (lib::is_mysql_mode()) { // mysql mode only has system package
        if (OB_FAIL(schema_checker.get_package_id( // try system pacakge
            OB_SYS_TENANT_ID, OB_SYS_DATABASE_NAME, object_name, compatible_mode, package_id))
            || OB_INVALID_ID == package_id) {
          LOG_WARN("failed to get package id", K(ret));
        } else {
          OZ (schema_checker.get_package_routine_infos(OB_SYS_TENANT_ID,
            package_id, OB_SYS_DATABASE_NAME, routine_name, routine_type, routines));
        }
      }
      // get routine failed, print parameters for debug.
      if (OB_FAIL(ret) || OB_INVALID_ID == package_id) {
        LOG_WARN("failed to get routines",
                 K(ret), K(current_database), K(db_name), K(package_name), K(real_db_name),
                 K(routine_name), K(routine_type), K(object_name), K(object_db_id));
      }
    }
  }
#undef TRY_SYNONYM
  return ret;
}

#define IS_NUMRIC_TYPE(type)  \
  (ob_is_int_tc(type)         \
   || ob_is_uint_tc(type)     \
   || ob_is_float_tc(type)    \
   || ob_is_double_tc(type)   \
   || ob_is_number_tc(type)   \
   || ob_is_decimal_int_tc(type))

int ObResolverUtils::check_type_match(ObResolverParams &params,
                                      ObRoutineMatchInfo::MatchInfo &match_info,
                                      ObRawExpr *expr,
                                      ObObjType src_type,
                                      uint64_t src_type_id,
                                      ObPLDataType &dst_pl_type)
{
  int ret = OB_SUCCESS;
  ObPLPackageGuard package_guard(params.session_info_->get_effective_tenant_id());
  ObPLResolveCtx resolve_ctx(*(params.allocator_),
                             *(params.session_info_),
                             *(params.schema_checker_->get_schema_guard()),
                             package_guard,
                             *(params.sql_proxy_),
                             false);
  OZ (package_guard.init());
  OZ (check_type_match(
    resolve_ctx, match_info, expr, src_type, src_type_id, dst_pl_type));
  return ret;
}

int ObResolverUtils::check_type_match(const pl::ObPLResolveCtx &resolve_ctx,
                                      ObRoutineMatchInfo::MatchInfo &match_info,
                                      ObRawExpr *expr,
                                      ObObjType src_type,
                                      uint64_t src_type_id,
                                      ObPLDataType &dst_pl_type,
                                      bool is_sys_package)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
    // do nothing ...
  } else if (T_QUESTIONMARK == expr->get_expr_type()
             && (resolve_ctx.is_prepare_protocol_
                 || !resolve_ctx.is_sql_scope_
                 || resolve_ctx.session_info_.get_pl_context() != NULL)
             && (ObUnknownType == expr->get_result_type().get_type()
                 || ObNullType == expr->get_result_type().get_type()
                 || (is_oracle_mode() ? expr->get_result_type().is_oracle_question_mark_type()
                                      : expr->get_result_type().is_mysql_question_mark_type()))) {
    OX (match_info =
      (ObRoutineMatchInfo::MatchInfo(false,
                                     ObUnknownType,
                                     dst_pl_type.get_obj_type())));
  } else if (T_NULL == expr->get_expr_type()
            || ObNullTC == ob_obj_type_class(src_type)) {
    // NULL可以匹配任何类型
    OX (match_info =
      (ObRoutineMatchInfo::MatchInfo(false,
                                     dst_pl_type.get_obj_type(),
                                     dst_pl_type.get_obj_type())));
  } else {
    if (T_REF_QUERY == expr->get_expr_type() && !dst_pl_type.is_cursor_type()) {
      const ObQueryRefRawExpr *query_expr = static_cast<const ObQueryRefRawExpr*>(expr);
      CK (OB_NOT_NULL(query_expr));
      if (OB_SUCC(ret) && 1 != query_expr->get_output_column()) {
        ret = OB_ERR_TOO_MANY_VALUES;
        LOG_WARN("subquery return too many columns", K(ret), KPC(query_expr));
      }
    }
    ObCollationType src_coll_type = expr->get_result_type().get_collation_type();
    if (OB_SUCC(ret)
        && dst_pl_type.is_cursor_type()
        && expr->get_result_type().get_extend_type() != ObPLType::PL_REF_CURSOR_TYPE
        && expr->get_expr_type() != T_REF_QUERY) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("PLS-00382: expression is of wrong type",
               K(ret), K(src_type_id), K(dst_pl_type), K(src_type),
               K(expr->get_result_type().get_extend_type()),
               KPC(expr));
    }
    if (OB_FAIL(ret)) {
    } else if ((ObLobType == src_type || ObLongTextType == src_type)
               && (ObLobType == dst_pl_type.get_obj_type()
                  || ObLongTextType == dst_pl_type.get_obj_type())) {
      if (src_coll_type
            == dst_pl_type.get_meta_type()->get_collation_type()
          || (src_coll_type != CS_TYPE_BINARY
              && dst_pl_type.get_meta_type()->get_collation_type() != CS_TYPE_BINARY)) {
        OX (match_info =
          (ObRoutineMatchInfo::MatchInfo(
            false, dst_pl_type.get_obj_type(), dst_pl_type.get_obj_type())));
      } else {
        // BLOB和CLOB不能隐式转换, 如果LOB类型不同, 直接淘汰掉
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("PLS-00382: expression is of wrong type",
                 K(ret), K(src_type_id), K(dst_pl_type), K(src_type));
      }
    } else if (((ObLobType == src_type || ObLongTextType == src_type)\
                 && CS_TYPE_BINARY == src_coll_type)
              || ((ObLobType == dst_pl_type.get_obj_type()
                   || ObLongTextType == dst_pl_type.get_obj_type())
                  && CS_TYPE_BINARY == dst_pl_type.get_meta_type()->get_collation_type())) {
      if (ObRawType == src_type || ObRawType == dst_pl_type.get_obj_type()) {
        // Raw and Blob can matched!
        OX (match_info =
          (ObRoutineMatchInfo::MatchInfo(true, src_type, dst_pl_type.get_obj_type())));
      } else {
        // BLOB不能向其他类型转, 其他类型也不能向Blob转, 因此是Blob直接淘汰
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("PLS-00382: expression is of wrong type",
                 K(ret), K(src_type_id), K(dst_pl_type), K(src_type));
      }
    } else {
      OZ (check_type_match(
        resolve_ctx, match_info, expr, src_type, src_coll_type, src_type_id, dst_pl_type, is_sys_package));
    }
    LOG_DEBUG("debug for check type match:",
               K(src_type), K(src_type_id), K(dst_pl_type), K(match_info), KPC(expr));
  }
  return ret;
}

int ObResolverUtils::check_type_match(const pl::ObPLResolveCtx &resolve_ctx,
                                      ObRoutineMatchInfo::MatchInfo &match_info,
                                      ObRawExpr *expr,
                                      ObObjType src_type,
                                      ObCollationType src_coll_type,
                                      uint64_t src_type_id,
                                      ObPLDataType &dst_pl_type,
                                      bool is_sys_package)
{
  int ret = OB_SUCCESS;

#define OBJ_TO_TYPE_CLASS(type) \
  (ObRawTC == ob_obj_type_class(type) ? ObStringTC : ob_obj_type_class(type))

  ObObjType dst_type = dst_pl_type.get_obj_type();
  // Case1: 处理BOOLEAN类型, BOOLEAN类型仅能匹配BOOLEAN
  if (lib::is_oracle_mode() && (ObTinyIntType == dst_type || ObTinyIntType == src_type)) {
    if (dst_type == src_type) {
      OX (match_info = (ObRoutineMatchInfo::MatchInfo(false, src_type, dst_type)));
    } else {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("PLS-00382: expression is of wrong type",
               K(ret), K(src_type_id), K(dst_pl_type), K(dst_type), K(src_type));
    }
  } else if (lib::is_oracle_mode() &&
            ob_is_oracle_datetime_tc(src_type) &&
            ob_is_oracle_datetime_tc(dst_type)) {
    if (dst_type == src_type) {
      OX (match_info = (ObRoutineMatchInfo::MatchInfo(false, src_type, dst_type)));
    } else if (is_sys_package && ob_is_otimestamp_type(src_type) && ob_is_otimestamp_type(dst_type)) {
      OX (match_info = (ObRoutineMatchInfo::MatchInfo(false, src_type, dst_type)));
    } else {
      OX (match_info = (ObRoutineMatchInfo::MatchInfo(true, src_type, dst_type)));
    }
  // Case2: 处理TypeClass相同的情形
  } else if (OBJ_TO_TYPE_CLASS(src_type) == OBJ_TO_TYPE_CLASS(dst_type)) {
    if (ob_obj_type_class(src_type) != ObExtendTC // 普通类型TypeClass相同
        || src_type_id == dst_pl_type.get_user_type_id()) { // 复杂类型TypeID相同
      OX (match_info =
        (ObRoutineMatchInfo::MatchInfo(
          ObLongTextType == dst_type ? true : false,
          (ObRawType == src_type || ObRawType == dst_type) ? src_type : dst_type,
          dst_type)));
    } else if (dst_pl_type.is_cursor_type()) {
      // TODO: check cursor type compatible
      OX (match_info = (ObRoutineMatchInfo::MatchInfo(false, src_type, dst_type)));
    } else if (resolve_ctx.is_prepare_protocol_ &&
               ObExtendType == src_type &&
               OB_INVALID_ID == src_type_id &&
               T_QUESTIONMARK == expr->get_expr_type()) { // 匿名数组
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(expr);
      int64_t idx = const_expr->get_value().get_unknown();
      CK (OB_NOT_NULL(resolve_ctx.params_.param_list_));
      CK (idx < resolve_ctx.params_.param_list_->count());
      if (OB_SUCC(ret)) {
        const ObObjParam &param = resolve_ctx.params_.param_list_->at(idx);
        const pl::ObPLComposite *src_composite = NULL;
        CK (OB_NOT_NULL(src_composite = reinterpret_cast<const ObPLComposite *>(param.get_ext())));
        if (OB_FAIL(ret)) {
        } else if (!dst_pl_type.is_collection_type()) {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("incorrect argument type", K(ret));
        } else {
          const pl::ObPLCollection *src_coll = NULL;
          ObPLResolveCtx pl_resolve_ctx(resolve_ctx.allocator_,
                                        resolve_ctx.session_info_,
                                        resolve_ctx.schema_guard_,
                                        resolve_ctx.package_guard_,
                                        resolve_ctx.sql_proxy_,
                                        false);
          const pl::ObUserDefinedType *pl_user_type = NULL;
          const pl::ObCollectionType *coll_type = NULL;
          OZ (pl_resolve_ctx.get_user_type(dst_pl_type.get_user_type_id(), pl_user_type));
          CK (OB_NOT_NULL(coll_type = static_cast<const ObCollectionType *>(pl_user_type)));
          CK (OB_NOT_NULL(src_coll = static_cast<const ObPLCollection *>(src_composite)));
          if (OB_FAIL(ret)) {
          } else if (coll_type->get_element_type().is_obj_type() ^
                     src_coll->get_element_desc().is_obj_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("incorrect argument type, diff type",
                          K(ret), K(coll_type->get_element_type()), K(src_coll->get_element_desc()));
          } else if (coll_type->get_element_type().is_obj_type()) { // basic data type
            const ObDataType *src_data_type = &src_coll->get_element_desc();
            const ObDataType *dst_data_type = coll_type->get_element_type().get_data_type();
            if (dst_data_type->get_obj_type() == src_data_type->get_obj_type()) {
              // do nothing
            } else if (cast_supported(src_data_type->get_obj_type(),
                                      src_data_type->get_collation_type(),
                                      dst_data_type->get_obj_type(),
                                      dst_data_type->get_collation_type())) {
              // do nothing
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("incorrect argument type, diff type", K(ret));
            }
          } else {
            // element is composite type
            uint64_t element_type_id = src_coll->get_element_desc().get_udt_id();
            bool is_compatible = element_type_id == coll_type->get_element_type().get_user_type_id();
            if (!is_compatible) {
              OZ (ObPLResolver::check_composite_compatible(
                NULL == resolve_ctx.params_.secondary_namespace_
                    ? static_cast<const ObPLINS&>(resolve_ctx)
                    : static_cast<const ObPLINS&>(*resolve_ctx.params_.secondary_namespace_),
                element_type_id, coll_type->get_element_type().get_user_type_id(), is_compatible));
            }
            if (OB_SUCC(ret) && !is_compatible) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("incorrect argument type",
                        K(ret), K(element_type_id), K(dst_pl_type), KPC(src_coll));
            }
          }
          OX (match_info = (ObRoutineMatchInfo::MatchInfo(false, src_type, dst_type)));
        }
      }
    } else {
      // 复杂类型的TypeClass相同, 需要检查兼容性
      bool is_compatible = false;
#ifdef OB_BUILD_ORACLE_PL
      if (ObPlJsonUtil::is_pl_jsontype(src_type_id)) {
        OZ (ObPLResolver::check_composite_compatible(
          NULL == resolve_ctx.params_.secondary_namespace_
              ? static_cast<const ObPLINS&>(resolve_ctx)
                  : static_cast<const ObPLINS&>(*resolve_ctx.params_.secondary_namespace_),
          dst_pl_type.get_user_type_id(),
          src_type_id,
          is_compatible), K(src_type_id), K(dst_pl_type), K(resolve_ctx.params_.is_execute_call_stmt_));
      } else {
#endif
        OZ (ObPLResolver::check_composite_compatible(
            NULL == resolve_ctx.params_.secondary_namespace_
                ? static_cast<const ObPLINS&>(resolve_ctx)
                    : static_cast<const ObPLINS&>(*resolve_ctx.params_.secondary_namespace_),
            src_type_id,
            dst_pl_type.get_user_type_id(),
            is_compatible), K(src_type_id), K(dst_pl_type), K(resolve_ctx.params_.is_execute_call_stmt_));
#ifdef OB_BUILD_ORACLE_PL
      }
#endif
      if (OB_FAIL(ret)) {
      } else if (is_compatible) {
        OX (match_info = ObRoutineMatchInfo::MatchInfo(true, src_type, dst_type));
      } else {
        ret = OB_ERR_EXPRESSION_WRONG_TYPE;
        LOG_WARN("PLS-00382: expression is of wrong type",K(ret), K(src_type_id), K(dst_pl_type));
      }
    }
  // Case3: 处理TypeClass不同的情形
  } else {
    // xmltype can not cast with varchar,
    if ((ObExtendTC == ob_obj_type_class(src_type)
          && !(ObUserDefinedSQLTC == ob_obj_type_class(dst_type)
                || ObExtendTC == ob_obj_type_class(dst_type)))
        || (ObUserDefinedSQLTC == ob_obj_type_class(src_type)
          && !(ObUserDefinedSQLTC == ob_obj_type_class(dst_type)
                || ObExtendTC == ob_obj_type_class(dst_type)))
        || ((ObUserDefinedSQLTC == ob_obj_type_class(dst_type)
            || ObExtendTC == ob_obj_type_class(dst_type))
          && !(ObUserDefinedSQLTC == ob_obj_type_class(src_type)
                || ObExtendTC == ob_obj_type_class(src_type)
                || ObGeometryTC == ob_obj_type_class(src_type)))
        || (ObGeometryTC == ob_obj_type_class(src_type) && lib::is_oracle_mode()
            && ObExtendTC != ob_obj_type_class(dst_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("argument count not match", K(ret), K(src_type), K(dst_type));
    } else if (ObExtendTC == ob_obj_type_class(src_type) // 普通类型与复杂类型不能互转
        || ObExtendTC == ob_obj_type_class(dst_type)) {
      if (ObUserDefinedSQLTC == ob_obj_type_class(src_type)
          || ObUserDefinedSQLTC == ob_obj_type_class(dst_type)
          || ObGeometryTC == ob_obj_type_class(dst_type)
          || ObGeometryTC == ob_obj_type_class(src_type)) {
      // check can cast
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("argument count not match", K(ret), K(src_type), K(dst_type));
      }
    } else { // 检查普通类型之间是否可以互转
      if (lib::is_oracle_mode()) {
        OZ (ObObjCaster::can_cast_in_oracle_mode(dst_type,
          dst_pl_type.get_meta_type()->get_collation_type(), src_type, src_coll_type));
      } else if (!cast_supported(
        src_type, src_coll_type, dst_type, dst_pl_type.get_meta_type()->get_collation_type())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent datatypes",
          K(ret), K(src_type), K(src_coll_type),
          K(dst_type), K(dst_pl_type.get_meta_type()->get_collation_type()));
      }
    }
    bool is_numric_type = (IS_NUMRIC_TYPE(src_type) && IS_NUMRIC_TYPE(dst_type));
    OX (match_info = ObRoutineMatchInfo::MatchInfo(
      is_numric_type ? false : true, is_numric_type ? src_type : dst_type, dst_type));
    LOG_DEBUG("need cast",
             K(ret),
             K(src_type),
             K(dst_type),
             K(ob_obj_type_class(src_type)),
             K(ob_obj_type_class(dst_type)));
  }

#undef OBJ_TO_TYPE_CLASS
  return ret;
}

int ObResolverUtils::get_type_and_type_id(
  ObRawExpr *expr, ObObjType &type, uint64_t &type_id)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  OX (type_id = expr->get_result_type().get_expr_udt_id());
  if (OB_FAIL(ret)) {
  } else if (IS_BOOL_OP(expr->get_expr_type())) {
    type = ObTinyIntType;
  } else if (T_FUN_PL_INTEGER_CHECKER == expr->get_expr_type()) {
    type = ObInt32Type;
  } else if (expr->get_result_type().is_geometry()) {
    type = ObGeometryType;
    type_id = T_OBJ_SDO_GEOMETRY;
  } else {
    type = expr->get_result_type().get_type();
  }
  CK (is_valid_obj_type(type));
  return ret;
}

// 检查当前参数是否与当前routine匹配
int ObResolverUtils::check_match(const pl::ObPLResolveCtx &resolve_ctx,
                                 const common::ObIArray<ObRawExpr *> &expr_params,
                                 const ObIRoutineInfo *routine_info,
                                 ObRoutineMatchInfo &match_info)
{
  int ret = OB_SUCCESS;
  bool is_sys_package = false;
  CK (OB_NOT_NULL(routine_info));
  if (OB_FAIL(ret)) {
  } else if (expr_params.count() > routine_info->get_param_count()) {
    ret = OB_ERR_SP_WRONG_ARG_NUM;
    LOG_WARN("argument count not match",
             K(ret), K(expr_params.count()), K(routine_info->get_param_count()));
  }
  OX (match_info.routine_info_ = routine_info);
  // MatchInfo初始化
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_info->get_param_count(); ++i) {
    OZ (match_info.match_info_.push_back(ObRoutineMatchInfo::MatchInfo()));
  }

  OX (is_sys_package = (get_tenant_id_by_object_id(routine_info->get_package_id()) == OB_SYS_TENANT_ID));

  int64_t offset = 0;
  if (OB_FAIL(ret)) {
  } else if (0 == expr_params.count() && routine_info->is_udt_routine() && !routine_info->is_udt_static_routine()) {
    // set first param matched
    OX (match_info.match_info_.at(0) = (ObRoutineMatchInfo::MatchInfo(false, ObExtendType, ObExtendType)));
    OX (offset = 1);
  } else if(expr_params.count() > 0 && OB_NOT_NULL(expr_params.at(0))) {
    ObRawExpr *first_arg = expr_params.at(0);
    if (first_arg->has_flag(IS_UDT_UDF_SELF_PARAM)) {
      // do nothing, may be we can check if routine is static or not
    } else if (routine_info->is_udt_cons()) {
      if (expr_params.count() > 0 && 1 == routine_info->get_param_count()) {
        ret = OB_ERR_SP_WRONG_ARG_NUM;
        LOG_WARN("argument count not match",
                  K(ret), K(expr_params.count()), K(routine_info->get_param_count()));
      } else {
        // construct function & no self real paremeter
        OX (match_info.match_info_.at(0) = (ObRoutineMatchInfo::MatchInfo(false, ObExtendType, ObExtendType)));
        OX (offset = 1);
      }
    } else if (routine_info->is_udt_routine()
               && !routine_info->is_udt_static_routine()) {
      uint64_t src_type_id = OB_INVALID_ID;
      ObObjType src_type;
      if (T_SP_CPARAM == first_arg->get_expr_type()) {
        ObCallParamRawExpr *call_expr = static_cast<ObCallParamRawExpr*>(first_arg);
        OZ (call_expr->get_expr()->extract_info());
        OZ (call_expr->get_expr()->deduce_type(&resolve_ctx.session_info_));
        OZ (get_type_and_type_id(call_expr->get_expr(), src_type, src_type_id));
      } else {
        OZ (first_arg->extract_info());
        OZ (first_arg->deduce_type(&resolve_ctx.session_info_));
        OZ (get_type_and_type_id(first_arg, src_type, src_type_id));
      }
      if (OB_SUCC(ret)
          && (src_type_id != routine_info->get_package_id())) {
        // set first param matched
        OX (match_info.match_info_.at(0) = (ObRoutineMatchInfo::MatchInfo(false, src_type, src_type)));
        if ((expr_params.count() + 1) > routine_info->get_param_count()) {
          ret = OB_ERR_SP_WRONG_ARG_NUM;
          LOG_WARN("argument count not match",
             K(ret),
             K(expr_params.count()),
             K(src_type_id),
             KPC(first_arg),
             K(routine_info->get_param_count()),
             K(routine_info->get_routine_name()));
        } else {
          OX (offset = 1);
        }
      }
    }
  }

  // 解析参数表达式数组
  bool has_assign_param = false;
  int arg_cnt = routine_info->get_param_count();
  for (int64_t i = 0; OB_SUCC(ret) && offset < arg_cnt && i < expr_params.count(); ++i) {
    int64_t position = OB_INVALID_ID;
    ObObjType src_type;
    uint64_t src_type_id;
    ObPLDataType dst_pl_type;
    ObRawExpr* expr = NULL;
    CK (OB_NOT_NULL(expr_params.at(i)));
    if (OB_FAIL(ret)) {
    } else if (T_SP_CPARAM == expr_params.at(i)->get_expr_type()) {
      ObCallParamRawExpr* call_expr = static_cast<ObCallParamRawExpr*>(expr_params.at(i));
      OX (has_assign_param = true);
      CK (OB_NOT_NULL(call_expr) && OB_NOT_NULL(call_expr->get_expr()));
      OZ (call_expr->get_expr()->extract_info());
      OZ (call_expr->get_expr()->deduce_type(&resolve_ctx.session_info_));
      CK (!call_expr->get_name().empty());
      OZ (routine_info->find_param_by_name(call_expr->get_name(), position));
      OX (expr = call_expr->get_expr());
      OZ (get_type_and_type_id(call_expr->get_expr(), src_type, src_type_id));
    } else if (has_assign_param) {
      ret = OB_ERR_POSITIONAL_FOLLOW_NAME;
      LOG_WARN("can not set param without assign after param with assign",
               K(ret), K(i), K(expr_params), K(match_info));
    } else {
      OZ (expr_params.at(i)->extract_info());
      OZ (expr_params.at(i)->deduce_type(&resolve_ctx.session_info_));
      OX (position = i + offset);
      OX (expr = expr_params.at(i));
      OZ (get_type_and_type_id(expr_params.at(i), src_type, src_type_id));
    }
    // 如果在相同的位置已经进行过匹配, 说明给定的参数在参数列表中出现了两次
    if (OB_SUCC(ret)
        && (OB_INVALID_ID == position
            || position >= match_info.match_info_.count()
            || ObMaxType != match_info.match_info_.at(position).dest_type_)) {
      ret = OB_ERR_SP_WRONG_ARG_NUM;
      LOG_WARN("argument count not match",
             K(ret),
             K(expr_params.count()),
             K(routine_info->get_param_count()),
             K(position));
    }
    // 取出匹配位置的type信息, 进行对比
    ObIRoutineParam *routine_param = NULL;
    OZ (routine_info->get_routine_param(position, routine_param));
    CK (OB_NOT_NULL(routine_param));
    if (OB_FAIL(ret)) {
    } else if (routine_param->is_schema_routine_param()) {
      ObRoutineParam *param = static_cast<ObRoutineParam*>(routine_param);
      CK (OB_NOT_NULL(param));
      OZ (pl::ObPLDataType::transform_from_iparam(param,
                                                  resolve_ctx.schema_guard_,
                                                  resolve_ctx.session_info_,
                                                  resolve_ctx.allocator_,
                                                  resolve_ctx.sql_proxy_,
                                                  dst_pl_type,
                                                  NULL,
                                                  &resolve_ctx.package_guard_.dblink_guard_));
#ifdef OB_BUILD_ORACLE_PL
      if (OB_SUCC(ret) && dst_pl_type.is_subtype()) {
        const ObUserDefinedType *user_type = NULL;
        const ObUserDefinedSubType *sub_type = NULL;
        OZ (resolve_ctx.get_user_type(
          dst_pl_type.get_user_type_id(), user_type, &(resolve_ctx.allocator_)));
        CK (OB_NOT_NULL(sub_type = static_cast<const ObUserDefinedSubType *>(sub_type)));
        OX (dst_pl_type = *(sub_type->get_base_type()));
      }
#endif
    } else {
      dst_pl_type = routine_param->get_pl_data_type();
    }
    if (OB_SUCC(ret) && OB_FAIL(check_type_match(resolve_ctx,
                                                 match_info.match_info_.at(position),
                                                 expr,
                                                 src_type,
                                                 src_type_id,
                                                 dst_pl_type,
                                                 is_sys_package))) {
      LOG_WARN("argument type not match", K(ret), K(i), KPC(expr_params.at(i)), K(src_type), K(dst_pl_type));
    }
  }

  // 处理空缺参数
  OZ (match_vacancy_parameters(*routine_info, match_info));
  return ret;
}

int ObResolverUtils::match_vacancy_parameters(
  const ObIRoutineInfo &routine_info, ObRoutineMatchInfo &match_info)
{
  int ret = OB_SUCCESS;
  SET_LOG_CHECK_MODE();
  // 处理空缺参数, 如果有默认值填充默认值, 否则报错
  for (int64_t i = 0; OB_SUCC(ret) && i < match_info.match_info_.count(); ++i) {
    ObIRoutineParam *routine_param = NULL;
    if(ObMaxType == match_info.match_info_.at(i).dest_type_) {
      OZ (routine_info.get_routine_param(i, routine_param));
      CK (OB_NOT_NULL(routine_param));
      if (OB_FAIL(ret)) {
      } else if (routine_param->get_default_value().empty()) {
        ret = OB_ERR_SP_WRONG_ARG_NUM;
        LOG_WARN("argument count not match",
               K(ret),
               K(routine_info.get_param_count()),
               K(i),
               K(match_info));
      } else {
        OX (match_info.match_info_.at(i) =
          ObRoutineMatchInfo::MatchInfo(routine_param->is_default_cast(),
                                        routine_param->get_pl_data_type().get_obj_type(),
                                        routine_param->get_pl_data_type().get_obj_type()));
      }
    }
  }
  CANCLE_LOG_CHECK_MODE();
  return ret;
}

// 处理存在多个匹配的情况
// 主要处理需要cast的情况, 如果某一个routine完全不需要cast即可调用, 则是最优匹配
// 但是如果存在多个不需要cast即可匹配的情况则报错
// 如果所有的匹配都需要cast同样报错
int ObResolverUtils::pick_routine(ObIArray<ObRoutineMatchInfo> &match_infos,
                                  const ObIRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  routine_info = NULL;
  CK (match_infos.count() > 1);

  // TODO: 处理Prepare协议下的选择, 因为Prepare时没有参数类型, 如果存在多个匹配, 随机选择一个
  for (int64_t i = 0; OB_SUCC(ret) && i < match_infos.count(); ++i) {
    if (match_infos.at(i).has_unknow_type()) {
      routine_info = match_infos.at(i).routine_info_;
      break;
    }
  }

  // 选择一个不需要Cast即可匹配的Routine
  ObSEArray<ObRoutineMatchInfo, 16> tmp_match_infos;
  if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < match_infos.count(); ++i) {
      if (!match_infos.at(i).need_cast()) {
        if (match_infos.at(i).match_same_type()) {
          if (OB_NOT_NULL(routine_info)) {
            ret = OB_ERR_FUNC_DUP;
            LOG_WARN("PLS-00307: too many declarations of 'string' match this call",
                     K(ret), K(match_infos));
          } else {
            routine_info = match_infos.at(i).routine_info_;
          }
        } else {
          OZ (tmp_match_infos.push_back(match_infos.at(i)));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
    if (1 == tmp_match_infos.count()) {
      routine_info = tmp_match_infos.at(0).routine_info_;
    } else if (0 == tmp_match_infos.count()) {
      tmp_match_infos.assign(match_infos);
    }
  }

// Formal Parameters that Differ Only in Numeric Data Type:
// PL/SQL looks for matching numeric parameters in this order:
// 1. PLS_INTEGER (or BINARY_INTEGER, an identical data type)
// 2. NUMBER
// 3. BINARY_FLOAT
// 4. BINARY_DOUBLE
// 实际测试下来, PLS_INTEGER的优先级最低

#define NUMRIC_TYPE_LEVEL(type)                                \
  (ob_is_number_tc(type) || ob_is_decimal_int(type)) ? 1       \
    : ob_is_float_tc(type) ? 2                                 \
      : ob_is_double_tc(type) ? 3                              \
        : ob_is_int_tc(type) || ob_is_uint_tc(type) ? 4 : 5;

  // 处理所有的匹配都需要经过Cast的情况
  if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
    ObSEArray<int, 16> numric_args;
    // 判断是否仅是Numric不同, 如果不是则直接报错, 否则记录Numric不同的参数位置
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_match_infos.at(0).match_info_.count(); ++i) {
      for (int64_t j = 1; OB_SUCC(ret) && j < tmp_match_infos.count(); ++j) {
        if (ob_obj_type_class(tmp_match_infos.at(0).get_type(i))
              == ob_obj_type_class(tmp_match_infos.at(j).get_type(i))) {
          // do nothing ...
        } else if (!(IS_NUMRIC_TYPE(tmp_match_infos.at(0).get_type(i)))
                  || !(IS_NUMRIC_TYPE(tmp_match_infos.at(j).get_type(i)))) {
          ret = OB_ERR_FUNC_DUP;
          LOG_WARN("PLS-00307: too many declarations of 'string' match this call",
                   K(ret), K(tmp_match_infos));
        } else {
          OZ (numric_args.push_back(i));
        }
      }
    }
    // 按照优先级选择一个Routine
    int64_t pos = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < numric_args.count(); ++i) {
      int64_t tmp_pos = 0;
      for (int64_t j = 1; OB_SUCC(ret) && j < tmp_match_infos.count(); ++j) {
        int level1 = NUMRIC_TYPE_LEVEL(tmp_match_infos.at(j).get_type(numric_args.at(i)));
        int level2 = NUMRIC_TYPE_LEVEL(tmp_match_infos.at(tmp_pos).get_type(numric_args.at(i)));
        if (level1 < level2) {
          OX (tmp_pos = j);
        }
      }
      if (-1 == pos) {
        pos = tmp_pos;
      } else if (pos != tmp_pos) {
        ret = OB_ERR_FUNC_DUP;
        LOG_WARN("PLS-00307: too many declarations of 'string' match this call",
                  K(ret), K(tmp_match_infos), K(numric_args));
      }
    }
    if (OB_SUCC(ret) && numric_args.count() > 0) {
      CK (pos != -1);
      OX (routine_info = tmp_match_infos.at(pos).routine_info_);
    }
  }

#undef NUMRIC_TYPE_LEVEL

  if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
    ret = OB_ERR_FUNC_DUP;
    LOG_WARN("PLS-00307: too many declarations of 'string' match this call",
              K(ret), K(match_infos));
  }
  return ret;
}

#undef IS_NUMRIC_TYPE

// 从多个同名的routine中寻找一个最佳匹配
int ObResolverUtils::pick_routine(const pl::ObPLResolveCtx &resolve_ctx,
                                  const common::ObIArray<ObRawExpr *> &expr_params,
                                  const common::ObIArray<const ObIRoutineInfo *> &routine_infos,
                                  const ObIRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  routine_info = NULL;
  common::ObSEArray<ObRoutineMatchInfo, 16> match_infos;
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_infos.count(); ++i) {
    ObRoutineMatchInfo match_info;
    if (OB_FAIL(check_match(resolve_ctx, expr_params, routine_infos.at(i), match_info))) {
      ret = (OB_ERR_SP_WRONG_ARG_NUM == ret
             || OB_ERR_SP_UNDECLARED_VAR == ret
             || OB_ERR_EXPRESSION_WRONG_TYPE == ret
             || OB_ERR_INVALID_TYPE_FOR_OP == ret) ? OB_SUCCESS : ret;
    } else {
      OZ (match_infos.push_back(match_info));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == match_infos.count()) { // 没有匹配的routine直接报错
    ret = OB_ERR_SP_WRONG_ARG_NUM;
    // ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call to 'string'",
             K(ret), KPC(routine_info), K(expr_params), K(routine_infos));
  } else if (1 == match_infos.count()) { // 恰好有一个, 直接返回
    CK (OB_NOT_NULL(match_infos.at(0).routine_info_));
    OX (routine_info = match_infos.at(0).routine_info_);
  } else { // 存在多个匹配, 继续pick
    OZ (pick_routine(match_infos, routine_info));
  }
  return ret;
}

int ObResolverUtils::pick_routine(const pl::ObPLResolveCtx &resolve_ctx,
                              const common::ObIArray<ObRawExpr *> &expr_params,
                              const common::ObIArray<const ObIRoutineInfo *> &routine_infos,
                              const ObPLRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  const share::schema::ObIRoutineInfo *i_routine_info = NULL;
  OZ (pick_routine(resolve_ctx, expr_params, routine_infos, i_routine_info));
  OX (routine_info = static_cast<const ObPLRoutineInfo*>(i_routine_info));
  return ret;
}

int ObResolverUtils::pick_routine(const pl::ObPLResolveCtx &resolve_ctx,
                              const common::ObIArray<ObRawExpr *> &expr_params,
                              const common::ObIArray<const ObIRoutineInfo *> &routine_infos,
                              const share::schema::ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  const share::schema::ObIRoutineInfo *i_routine_info = NULL;
  OZ (pick_routine(resolve_ctx, expr_params, routine_infos, i_routine_info));
  OX (routine_info = static_cast<const share::schema::ObRoutineInfo*>(i_routine_info));
  return ret;
}

int ObResolverUtils::get_routine(pl::ObPLPackageGuard &package_guard,
                                 ObResolverParams &params,
                                 uint64_t tenant_id,
                                 const ObString &current_database,
                                 const ObString &db_name,
                                 const ObString &package_name,
                                 const ObString &routine_name,
                                 const share::schema::ObRoutineType routine_type,
                                 const common::ObIArray<ObRawExpr *> &expr_params,
                                 const ObRoutineInfo *&routine,
                                 const ObString &dblink_name,
                                 ObIAllocator *allocator,
                                 ObSynonymChecker *outer_synonym_checker)
{
  int ret = OB_SUCCESS;
#define COPY_DBLINK_ROUTINE(dblink_routine) \
  ObRoutineInfo *copy_routine = NULL; \
  CK (OB_NOT_NULL(allocator)); \
  OZ (ObSchemaUtils::alloc_schema(*allocator, \
                                  *(static_cast<const ObRoutineInfo *>(dblink_routine)), \
                                  copy_routine)); \
  OX (routine = copy_routine);

  const ObRoutineInfo *tmp_routine_info = NULL;
  CK (OB_NOT_NULL(params.allocator_));
  CK (OB_NOT_NULL(params.session_info_));
  CK (OB_NOT_NULL(params.schema_checker_));
  CK (OB_NOT_NULL(params.schema_checker_->get_schema_guard()));
  CK (OB_NOT_NULL(GCTX.sql_proxy_));
  if (OB_SUCC(ret)) {
    ObPLResolveCtx resolve_ctx(*(params.allocator_),
                               *(params.session_info_),
                               *(params.schema_checker_->get_schema_guard()),
                               package_guard,
                               *(GCTX.sql_proxy_),
                               params.is_prepare_protocol_,
                               false, /*check mode*/
                               true, /*sql scope*/
                               params.param_list_);
    resolve_ctx.params_.secondary_namespace_ = params.secondary_namespace_;
    resolve_ctx.params_.param_list_ = params.param_list_;
    resolve_ctx.params_.is_execute_call_stmt_ = params.is_execute_call_stmt_;
    if (dblink_name.empty()) {
      OZ (get_routine(resolve_ctx,
                      tenant_id,
                      current_database,
                      db_name,
                      package_name,
                      routine_name,
                      routine_type,
                      expr_params,
                      tmp_routine_info,
                      outer_synonym_checker));
      if (OB_SUCC(ret)) {
        if (OB_INVALID_ID == tmp_routine_info->get_dblink_id()) {
          routine = tmp_routine_info;
        } else {
          COPY_DBLINK_ROUTINE(tmp_routine_info);
        }
      }
      if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
        /* Example 1: create or replace synonym test.pkg100_syn for webber.pkg100@oci_link;
        *    `pkg100` is a package name in remote database `webber`.
        * Example 2: create or replace synonym test.p101_syn for webber.p101@oci_link;
        *    `p101` is a procedure name in remote database `webber`.
        *
        * If the code executes here, there are the following situations
        * 1. Only `db_name.empty()` is true , this is not possible;
        * 2. Only `package_name.empty()` is true, user call statement is `call test.p101_syn(1)`;
        * 3. `(db_name.empty() && package_name.empty()` is true,  user call statement is `call p101_syn(1)`;
        * 4. `db_name.empty()` is false and `package_name.empty()` is false,
        *     user call statement is `call test.pkg100_syn.p1(1)`
        */

        ret = OB_SUCCESS;
        uint64_t database_id = OB_INVALID_ID;
        uint64_t object_db_id = OB_INVALID_ID;
        bool exist = false;
        ObString object_name;
        ObSchemaChecker schema_checker;
        ObSynonymChecker synonym_checker;
        bool routine_name_is_synonym = package_name.empty();
        const ObString &synonym_name = routine_name_is_synonym ? routine_name : package_name;
        OZ (schema_checker.init(resolve_ctx.schema_guard_));
        OZ (schema_checker.get_database_id(tenant_id, (db_name.empty() ? current_database : db_name), database_id));
        OZ (resolve_synonym_object_recursively(schema_checker, synonym_checker, tenant_id, database_id,
                                               synonym_name, object_db_id, object_name, exist));
        LOG_INFO("after find synonym", K(ret), K(synonym_name), K(object_name), K(object_db_id), K(exist));
        if (OB_SUCC(ret) && exist) {
          ObString tmp_name;
          if (OB_FAIL(ob_write_string(*params.allocator_, object_name, tmp_name))) {
            LOG_WARN("write string failed", K(ret));
          } else {
            ObString full_object_name = tmp_name.split_on('@');
            if (full_object_name.empty()) {
              exist = false;
            } else {
              ObString remote_db_name = full_object_name.split_on('.');
              const ObIRoutineInfo *dblink_routine_info = NULL;
              // ObRoutineInfo *tmp_routine_info = NULL;
              if (routine_name_is_synonym) {
                OZ (ObPLResolver::resolve_dblink_routine(resolve_ctx,
                                                        tmp_name,
                                                        remote_db_name,
                                                        ObString(""),
                                                        full_object_name,
                                                        expr_params,
                                                        dblink_routine_info));
              } else {
                OZ (ObPLResolver::resolve_dblink_routine(resolve_ctx,
                                                         tmp_name,
                                                         remote_db_name,
                                                         full_object_name,
                                                         routine_name,
                                                         expr_params,
                                                         dblink_routine_info));
              }
              if (OB_SUCC(ret) && OB_NOT_NULL(dblink_routine_info)) {
                COPY_DBLINK_ROUTINE(dblink_routine_info);
              }
            }
          }
        }
        if (OB_SUCC(ret) && !exist) {
          ret = OB_ERR_SP_DOES_NOT_EXIST;
          LOG_WARN("routine not exist", K(ret));
        }
      }
    } else {
      const ObIRoutineInfo *dblink_routine_info = NULL;
      OZ (ObPLResolver::resolve_dblink_routine(resolve_ctx,
                                              dblink_name,
                                              db_name,
                                              package_name,
                                              routine_name,
                                              expr_params,
                                              dblink_routine_info));
      if (OB_SUCC(ret) && OB_NOT_NULL(dblink_routine_info)) {
        COPY_DBLINK_ROUTINE(dblink_routine_info);
      }
    }
  }
  return ret;
}

int ObResolverUtils::get_routine(const pl::ObPLResolveCtx &resolve_ctx,
                                 uint64_t tenant_id,
                                 const ObString &current_database,
                                 const ObString &db_name,
                                 const ObString &package_name,
                                 const ObString &routine_name,
                                 const share::schema::ObRoutineType routine_type,
                                 const common::ObIArray<ObRawExpr *> &expr_params,
                                 const ObRoutineInfo *&routine,
                                 ObSynonymChecker *synonym_checker)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<const share::schema::ObIRoutineInfo *, 4> candidate_routine_infos;
  ObSchemaChecker schema_checker;
  routine = NULL;
  uint64_t udt_id = OB_INVALID_ID;
  OZ (schema_checker.init(resolve_ctx.schema_guard_, resolve_ctx.session_info_.get_sessid()));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_candidate_routines(schema_checker,
                                     tenant_id,
                                     current_database,
                                     db_name,
                                     package_name,
                                     routine_name,
                                     routine_type,
                                     candidate_routine_infos,
                                     udt_id,
                                     &resolve_ctx,
                                     synonym_checker))) {
    LOG_WARN("failed to get candidate routine infos",
             K(db_name), K(package_name), K(routine_name), K(ret));
  } else {
    if (!candidate_routine_infos.empty()) {
      if (lib::is_mysql_mode()) {
        CK (1 == candidate_routine_infos.count());
        OX (routine = static_cast<const ObRoutineInfo *>(candidate_routine_infos.at(0)));
      } else {
        OZ (pick_routine(resolve_ctx, expr_params, candidate_routine_infos, routine));
        LOG_INFO("call ObResolverUtils::get_routine fit routine",
                  K(db_name),
                  K(package_name),
                  K(routine_name),
                  KPC(routine),
                  K(candidate_routine_infos));
      }
    }
    if (OB_SUCC(ret) && NULL == routine) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_WARN("procedure/function not found", K(db_name), K(package_name), K(routine_name), K(ret));
      if (ROUTINE_FUNCTION_TYPE == routine_type) {
        ret = OB_ERR_FUNCTION_UNKNOWN;
        LOG_WARN("stored function not exists", K(ret), K(routine_name), K(db_name), K(package_name));
        LOG_USER_ERROR(OB_ERR_FUNCTION_UNKNOWN, "FUNCTION", routine_name.length(), routine_name.ptr());
      } else {
        ret = OB_ERR_SP_DOES_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST,
                       "procedure",
                       package_name.empty() ? (db_name.empty() ? current_database.length() : db_name.length()) : package_name.length(),
                       package_name.empty() ? (db_name.empty() ? current_database.ptr() : db_name.ptr()) : package_name.ptr(),
                       routine_name.length(), routine_name.ptr());
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_sp_access_name(ObSchemaChecker &schema_checker,
                                            ObIAllocator &allocator,
                                            uint64_t tenant_id,
                                            const ObString& current_database,
                                            const ObString& procedure_name,
                                            ObString &database_name,
                                            ObString &package_name,
                                            ObString &routine_name)
{
  int ret = OB_SUCCESS;
  ParseResult parser_result;
  ObString dblink_name;
  ObStmtNodeTree *parser_tree = NULL;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("failed to get tenant mode", K(ret), K(tenant_id), K(compat_mode));
  } else {
    lib::CompatModeGuard g(compat_mode);
    ObSqlString call_str;
    ObParser call_parser(
      allocator, ob_compatibility_mode_to_sql_mode(static_cast<ObCompatibilityMode>(compat_mode)));
    if (OB_FAIL(call_str.append_fmt("CALL %.*s();", procedure_name.length(), procedure_name.ptr()))) {
      LOG_WARN("failed to append call string", K(ret), K(procedure_name));
    } else if (OB_FAIL(call_parser.parse(call_str.string(), parser_result))) {
      LOG_WARN("failed to resolve procedure name", K(ret), K(procedure_name), K(compat_mode));
    } else if (FALSE_IT(parser_tree = parser_result.result_tree_)) {
    } else if (OB_ISNULL(parser_tree)
               || OB_UNLIKELY(T_STMT_LIST != parser_tree->type_)
               || OB_UNLIKELY(parser_tree->num_child_ != 1)
               || OB_ISNULL(parser_tree->children_)
               || OB_ISNULL(parser_tree->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the children of parse tree is invalid",
               K(ret), K(parser_tree->type_), K(parser_tree->children_[0]));
    } else if (OB_UNLIKELY(T_SP_CALL_STMT != parser_tree->children_[0]->type_)
               || OB_UNLIKELY(parser_tree->children_[0]->num_child_ < 1)
               || OB_ISNULL(parser_tree->children_[0]->children_[0])
               || OB_UNLIKELY(T_SP_ACCESS_NAME != parser_tree->children_[0]->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the children of parse tree is invalid",
               K(ret), K(parser_tree->children_[0]->type_),
               K(parser_tree->children_[0]->num_child_),
               K(parser_tree->children_[0]->children_[0]),
               K(parser_tree->children_[0]->children_[0]->type_));
    } else if (OB_FAIL(resolve_sp_access_name(schema_checker,
                                              tenant_id,
                                              current_database,
                                              *(parser_tree->children_[0]->children_[0]),
                                              database_name, package_name, routine_name, dblink_name))) {
      LOG_WARN("failed to resolve_sp_access_name",
               K(ret), K(procedure_name), K(tenant_id), K(current_database));
    }
  }
  return ret;
}

int ObResolverUtils::resolve_synonym_object_recursively(ObSchemaChecker &schema_checker,
                                                        ObSynonymChecker &synonym_checker,
                                                        uint64_t tenant_id,
                                                        uint64_t database_id,
                                                        const ObString &synonym_name,
                                                        uint64_t &object_database_id,
                                                        ObString &object_name,
                                                        bool &exist,
                                                        bool search_public_schema)
{
  int ret = OB_SUCCESS;
  uint64_t synonym_id = OB_INVALID_ID;
  bool exist_with_synonym = false;
  bool exist_non_syn_object = false;
  bool is_private_syn = false;
  if (OB_FAIL(schema_checker.get_synonym_schema(
      tenant_id, database_id, synonym_name, object_database_id,
      synonym_id, object_name, exist_with_synonym, search_public_schema))) {
    LOG_WARN("failed to get synonym", K(ret), K(tenant_id), K(database_id), K(synonym_name));
  } else if (exist_with_synonym) {
    exist = true;
    synonym_checker.set_synonym(true);
    if (OB_FAIL(synonym_checker.add_synonym_id(synonym_id, database_id))) {
      if (OB_ERR_LOOP_OF_SYNONYM == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to add synonym id to synonym checker",
                K(ret), K(tenant_id), K(database_id), K(synonym_name));
      }
    } else if (OB_FAIL(schema_checker.check_exist_same_name_object_with_synonym(tenant_id,
                                                                                object_database_id,
                                                                                object_name,
                                                                                exist_non_syn_object,
                                                                                is_private_syn)) ||
                                                                                !exist_non_syn_object ||
                                                                                is_private_syn) {
      ret = OB_SUCCESS;
      OZ (SMART_CALL(resolve_synonym_object_recursively(
        schema_checker, synonym_checker, tenant_id,
        object_database_id, object_name, object_database_id, object_name, exist_with_synonym,
        search_public_schema)));
    }
  }
  return ret;
}

//TODO(guangang.gg):consider invoker and definer semantics
int ObResolverUtils::resolve_sp_access_name(ObSchemaChecker &schema_checker,
                                            uint64_t tenant_id,
                                            const ObString &current_database,
                                            const ParseNode &sp_access_name_node,
                                            ObString &db_name,
                                            ObString &package_name,
                                            ObString &routine_name,
                                            ObString &dblink_name)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(sp_access_name_node.type_ != T_SP_ACCESS_NAME)
      || OB_UNLIKELY(sp_access_name_node.num_child_ != 3)
      || OB_ISNULL(sp_access_name_node.children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sp_name_node is invalid", K_(sp_access_name_node.type),
             K_(sp_access_name_node.num_child), K_(sp_access_name_node.children));
  } else {
    const ParseNode *sp_node = sp_access_name_node.children_[2];
    const ParseNode *package_or_db_node = sp_access_name_node.children_[1];
    const ParseNode *db_node = sp_access_name_node.children_[0];
    if (OB_ISNULL(sp_node) || OB_UNLIKELY(sp_node->type_ != T_IDENT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sp_node is invalid", K(sp_node));
    } else {
      routine_name.assign_ptr(sp_node->str_value_, static_cast<int32_t>(sp_node->str_len_));
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(db_node) && OB_NOT_NULL(package_or_db_node)) {
        if (OB_UNLIKELY(package_or_db_node->type_ != T_IDENT)
            || OB_UNLIKELY(db_node->type_ != T_IDENT)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node is invalid", K(package_or_db_node), K(db_node), K(ret));
        } else {
          package_name.assign_ptr(package_or_db_node->str_value_, static_cast<int32_t>(package_or_db_node->str_len_));
          db_name.assign_ptr(db_node->str_value_, static_cast<int32_t>(db_node->str_len_));
        }
      } else if (OB_ISNULL(db_node) && OB_NOT_NULL(package_or_db_node)) {
        // search package name first, then database name
        ObString package_or_db_name;
        uint64_t package_id = OB_INVALID_ID;
        uint64_t database_id = OB_INVALID_ID;
        bool is_dblink_routine = false;
        if (OB_UNLIKELY(package_or_db_node->type_ != T_IDENT)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package_or_db_node is invalid", K(package_or_db_node));
        } else {
          package_or_db_name.assign_ptr(package_or_db_node->str_value_, static_cast<int32_t>(package_or_db_node->str_len_));
          if (is_oracle_mode()) { //Oracle mode
            uint64_t object_db_id = OB_INVALID_ID;
            ObString object_name;
            bool exist = false;
            //search package name in current database, then search in sys tenant space in case of sys package
            OZ (schema_checker.get_database_id(tenant_id, current_database, database_id));
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(schema_checker.get_package_id(
                  tenant_id, database_id, package_or_db_name, COMPATIBLE_ORACLE_MODE, package_id))) {
              ObSynonymChecker synonym_checker;
              ret = OB_SUCCESS;
              OZ (resolve_synonym_object_recursively(
                schema_checker, synonym_checker, tenant_id, database_id,
                package_or_db_name, object_db_id, object_name, exist));
              if (OB_FAIL(ret)) {
              } else if (exist) {
                if (object_db_id != database_id) {
                  const ObDatabaseSchema *database_schema = NULL;
                  if (OB_FAIL(schema_checker.get_database_schema(tenant_id, object_db_id, database_schema))) {
                    LOG_WARN("failed to get database schema",
                             K(ret), K(object_db_id), K(object_name), K(database_id));
                  } else if (OB_ISNULL(database_schema)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("database schema is null",
                              K(ret), K(object_db_id), K(object_name), K(database_id));
                  } else {
                    db_name = database_schema->get_database_name_str();
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(schema_checker.get_package_id(
                                    tenant_id, object_db_id, object_name, COMPATIBLE_ORACLE_MODE, package_id))) {
                  LOG_WARN("failed to get package id", K(ret), K(object_db_id), K(object_name));
                  // If failed, object_name may be a dblink object
                  if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
                    ret = OB_SUCCESS;
                    ObString full_pkg_name = object_name.split_on('@');
                    if (full_pkg_name.empty()) {
                      // not a dblink
                    } else {
                      dblink_name = object_name;
                      db_name = full_pkg_name.split_on('.');
                      package_name = full_pkg_name;
                      is_dblink_routine = true;
                    }
                    if (OB_SUCC(ret) && !is_dblink_routine) {
                      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
                    }
                  }
                } else {
                  package_name = object_name;
                }
              } else {
                object_db_id = database_id;
                object_name = package_or_db_name;
              }
            }
            if (is_dblink_routine) {
            } else if (OB_FAIL(ret) || OB_INVALID_ID == package_id) {
              if (OB_FAIL(schema_checker.get_package_id(
                    OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID,
                    object_name, COMPATIBLE_ORACLE_MODE, package_id))) {
                if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
                  if (OB_FAIL(schema_checker.get_udt_id(tenant_id, object_db_id, OB_INVALID_ID, object_name, package_id))) {
                    if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
                      if (OB_FAIL(schema_checker.get_database_id(tenant_id, object_name, database_id))) {
                        if (OB_ERR_BAD_DATABASE == ret) {
                          package_name = package_or_db_name;
                          LOG_USER_ERROR(OB_ERR_BAD_DATABASE, package_or_db_name.length(), package_or_db_name.ptr());
                        } else {
                          LOG_WARN("get database id failed", K(package_or_db_name), K(ret));
                        }
                      } else {
                        db_name = package_or_db_name;
                      }
                    } else {
                      db_name = package_or_db_name;
                    }
                  } else if (package_id != OB_INVALID_ID) {
                    package_name = package_or_db_name;
                  } else {
                    if (OB_FAIL(schema_checker.get_database_id(tenant_id, object_name, database_id))) {
                      if (OB_ERR_BAD_DATABASE == ret) {
                        package_name = package_or_db_name;
                        LOG_USER_ERROR(OB_ERR_BAD_DATABASE, package_or_db_name.length(), package_or_db_name.ptr());
                      } else {
                        LOG_WARN("get database id failed", K(package_or_db_name), K(ret));
                      }
                    } else {
                      db_name = package_or_db_name;
                    }
                  }
                } else {
                  LOG_WARN("get package id failed", K(package_or_db_name), K(ret));
                }
              } else {
                package_name = package_or_db_name;
                db_name.assign_ptr(OB_SYS_DATABASE_NAME, static_cast<int32_t>(strlen(OB_SYS_DATABASE_NAME)));
              }
            } else if (!exist) {
              package_name = package_or_db_name;
              db_name = current_database;
            }
          } else { //Mysql mode
            OZ (schema_checker.get_database_id(tenant_id, current_database, database_id));
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_SUCC(schema_checker.get_package_id(tenant_id, database_id,
                                                             package_or_db_name,
                                                             COMPATIBLE_MYSQL_MODE,
                                                             package_id))) {
              package_name = package_or_db_name;
              db_name = current_database;
            }
            if (OB_FAIL(ret)) {
              // 如果package_or_db_name作为package获取失败,则把package_or_db_name当做database去获取
              ret = OB_SUCCESS;
              if (OB_FAIL(schema_checker.get_database_id(tenant_id, package_or_db_name, database_id))) {
                int64_t old_ret = ret;
                if (OB_FAIL(schema_checker.get_package_id(
                    OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID,
                    package_or_db_name, COMPATIBLE_MYSQL_MODE, package_id))) {
                  ret = old_ret;
                  LOG_WARN("get database id failed", K(package_or_db_name), K(ret));
                  LOG_USER_ERROR(OB_ERR_BAD_DATABASE, package_or_db_name.length(), package_or_db_name.ptr());
                } else {
                package_name = package_or_db_name;
                db_name.assign_ptr(OB_SYS_DATABASE_NAME, static_cast<int32_t>(strlen(OB_SYS_DATABASE_NAME)));
                }
              } else {
                db_name = package_or_db_name;
              }
            }
          }
        }
      } else if (OB_ISNULL(db_node) && OB_ISNULL(package_or_db_node)) {
        //do nothing
        //in pl context, this call may be a package private procedure, not a schema object
      } else { /* not possible */ }
    }
  }

  return ret;
}

int ObResolverUtils::resolve_sp_name(ObSQLSessionInfo &session_info,
                                     const ParseNode &sp_name_node,
                                     ObString &db_name,
                                     ObString &sp_name,
                                     bool need_db_name)
{
  int ret = OB_SUCCESS;
  const ParseNode *sp_node = NULL;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_UNLIKELY(sp_name_node.type_ != T_SP_NAME)
      || OB_UNLIKELY(sp_name_node.num_child_ != 2)
      || OB_ISNULL(sp_name_node.children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sp_name_node is invalid",
             K_(sp_name_node.type), K_(sp_name_node.num_child), K_(sp_name_node.children));
  } else if (OB_ISNULL(sp_node = sp_name_node.children_[1])
             || OB_UNLIKELY(sp_node->type_ != T_IDENT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sp_node is invalid", K(sp_node), K(ret));
  } else if (OB_FAIL(session_info.get_name_case_mode(mode))) {
    LOG_WARN("fail to get name case mode", K(mode), K(ret));
  } else if (OB_FAIL(session_info.get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation connection", K(ret));
  } else {
    const ParseNode *db_node = NULL;
    bool perserve_lettercase = lib::is_oracle_mode() ?
        true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    sp_name.assign_ptr(sp_node->str_value_, static_cast<int32_t>(sp_node->str_len_));
    if (sp_name.empty()
        || !sp_name[0] || ' ' == sp_name[sp_name.length() -1]) {
      ret = OB_ER_SP_WRONG_NAME;
      LOG_USER_ERROR(OB_ER_SP_WRONG_NAME, static_cast<int32_t>(sp_name.length()), sp_name.ptr());
      LOG_WARN("Incorrect routine name", K(sp_name), K(ret));
    } else if (OB_UNLIKELY(sp_name.length() >
      (lib::is_oracle_mode() ? OB_MAX_PL_IDENT_LENGTH : OB_MAX_MYSQL_PL_IDENT_LENGTH))) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_WARN("identifier is too long", K(sp_name), K(ret));
    } else if (NULL == (db_node = sp_name_node.children_[0])) {
      if (!need_db_name && lib::is_oracle_mode()) {
        // do nothing ...
      } else if (session_info.get_database_name().empty()) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_USER_ERROR(OB_ERR_NO_DB_SELECTED);
        LOG_WARN("No Database Selected", K(ret));
      } else {
        db_name = session_info.get_database_name();
      }
    } else if (OB_UNLIKELY(db_node->type_ != T_IDENT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("db_node is invalid", K(db_node), K(ret));
    } else {
      db_name.assign_ptr(db_node->str_value_, static_cast<int32_t>(db_node->str_len_));
      if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, db_name))) {
        LOG_WARN("fail to check and convert database name", K(db_name), K(ret));
      }
    }
  }
  LOG_DEBUG("check sp name result", K(sp_name), K(db_name));
  return ret;
}

int ObResolverUtils::resolve_column_ref(const ParseNode *node, const ObNameCaseMode case_mode,
                                        ObQualifiedName& column_ref)
{
  int ret = OB_SUCCESS;
  ParseNode* db_node = NULL;
  ParseNode* relation_node = NULL;
  ParseNode* column_node = NULL;
  ObString column_name;
  ObString table_name;
  ObString database_name;
  if (OB_ISNULL(node) || node->num_child_ < 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse node is invalid", K(node));
  } else {
    db_node = node->children_[0];
    relation_node = node->children_[1];
    column_node = node->children_[2];
    if (db_node != NULL) {
      column_ref.database_name_.assign_ptr(const_cast<char*>(db_node->str_value_),
                                           static_cast<int32_t>(db_node->str_len_));
    }
  }
  if (OB_SUCC(ret)) {
    if (relation_node != NULL) {
      column_ref.tbl_name_.assign_ptr(const_cast<char *>(relation_node->str_value_),
                                      static_cast<int32_t>(relation_node->str_len_));
    }
    if (OB_ISNULL(column_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column node is null");
    } else if (column_node->type_ == T_STAR) {
      column_ref.is_star_ = true;
    } else {
      column_ref.is_star_ = false;
      column_ref.col_name_.assign_ptr(const_cast<char *>(column_node->str_value_),
                                      static_cast<int32_t>(column_node->str_len_));
    }
  }

  if (OB_SUCC(ret) && lib::is_mysql_mode() && OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, column_ref.database_name_);
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, column_ref.tbl_name_);
  }
  return ret;
}

int ObResolverUtils::resolve_obj_access_ref_node(ObRawExprFactory &expr_factory,
                                                 const ParseNode *node,
                                                 ObQualifiedName &q_name,
                                                 const ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  // generate raw expr
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (T_IDENT == node->type_/*Mysql mode*/
      || T_COLUMN_REF == node->type_/*Mysql mode*/) {
    ObTimeZoneInfo tz_info;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
    ctx.session_info_ = &session_info;
    //not set query_ctx, 这个函数只会由PL Resolver调用, PL Resolver中没有query ctx
    // ??
    //ctx.is_oracle_compatible_ = (T_OBJ_ACCESS_REF == node->type_);

    ObRawExprResolverImpl expr_resolver(ctx);
    ObRawExpr *expr = NULL;
    ObSEArray<ObQualifiedName, 1> columns;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObArray<ObUDFInfo> udf_info;
    ObArray<ObOpRawExpr*> op_exprs;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    ObSEArray<ObMatchFunRawExpr*, 1> match_exprs;
    ObArray<ObInListInfo> inlist_infos;
    if (OB_FAIL(expr_resolver.resolve(node, expr, columns, sys_vars, sub_query_info, aggr_exprs,
                                      win_exprs, udf_info, op_exprs, user_var_exprs,
                                      inlist_infos, match_exprs))) {
      LOG_WARN("failed to resolve expr tree", K(ret));
    } else if (OB_UNLIKELY(1 != columns.count())
        || OB_UNLIKELY(!sys_vars.empty())
        || OB_UNLIKELY(!sub_query_info.empty())
        || OB_UNLIKELY(!aggr_exprs.empty())
        || OB_UNLIKELY(!win_exprs.empty())
        || OB_UNLIKELY(!inlist_infos.empty())
        || OB_UNLIKELY(!match_exprs.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is invalid", K(op_exprs.empty()), K(columns.count()), K(sys_vars.count()),
                K(sub_query_info.count()), K(aggr_exprs.count()), K(win_exprs.count()),
                K(udf_info.count()), K(match_exprs.count()), K(ret));
    } else if (OB_FAIL(q_name.assign(columns.at(0)))) {
      LOG_WARN("assign qualified name failed", K(ret), K(columns));
    } else { /*do nothing*/ }
  } else if (T_SYSTEM_VARIABLE == node->type_ || T_USER_VARIABLE_IDENTIFIER == node->type_) {
    ObString access_name(node->str_len_, node->str_value_);
    ObObjAccessIdent ident(access_name);
    ret = q_name.access_idents_.push_back(ident);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is invalid", K_(node->type));
  }
  return ret;
}

stmt::StmtType ObResolverUtils::get_stmt_type_by_item_type(const ObItemType item_type)
{
  stmt::StmtType type;
  switch (item_type) {
#define SET_STMT_TYPE(item_type) \
  case item_type: {              \
    type = stmt::item_type;      \
  } break;
      // dml
      SET_STMT_TYPE(T_SELECT);
      SET_STMT_TYPE(T_INSERT);
      SET_STMT_TYPE(T_DELETE);
      SET_STMT_TYPE(T_UPDATE);
      SET_STMT_TYPE(T_MERGE);
      SET_STMT_TYPE(T_REPLACE);
      // ps
      SET_STMT_TYPE(T_PREPARE);
      SET_STMT_TYPE(T_EXECUTE);
      SET_STMT_TYPE(T_DEALLOCATE);
      // ddl
      // tenant resource
      SET_STMT_TYPE(T_CREATE_RESOURCE_POOL);
      SET_STMT_TYPE(T_DROP_RESOURCE_POOL);
      SET_STMT_TYPE(T_ALTER_RESOURCE_POOL);
      SET_STMT_TYPE(T_SPLIT_RESOURCE_POOL);
      SET_STMT_TYPE(T_MERGE_RESOURCE_POOL);
      SET_STMT_TYPE(T_CREATE_RESOURCE_UNIT);
      SET_STMT_TYPE(T_ALTER_RESOURCE_UNIT);
      SET_STMT_TYPE(T_DROP_RESOURCE_UNIT);
      SET_STMT_TYPE(T_CREATE_TENANT);
      SET_STMT_TYPE(T_CREATE_STANDBY_TENANT);
      SET_STMT_TYPE(T_DROP_TENANT);
      SET_STMT_TYPE(T_MODIFY_TENANT);
      SET_STMT_TYPE(T_LOCK_TENANT);
      // database
      SET_STMT_TYPE(T_CREATE_DATABASE);
      SET_STMT_TYPE(T_ALTER_DATABASE);
      SET_STMT_TYPE(T_DROP_DATABASE);
      // tablegroup
      SET_STMT_TYPE(T_CREATE_TABLEGROUP);
      SET_STMT_TYPE(T_ALTER_TABLEGROUP);
      SET_STMT_TYPE(T_DROP_TABLEGROUP);
      // table
      SET_STMT_TYPE(T_CREATE_TABLE);
      SET_STMT_TYPE(T_DROP_TABLE);
      SET_STMT_TYPE(T_RENAME_TABLE);
      SET_STMT_TYPE(T_TRUNCATE_TABLE);
      SET_STMT_TYPE(T_CREATE_TABLE_LIKE);
      SET_STMT_TYPE(T_ALTER_TABLE);
      SET_STMT_TYPE(T_OPTIMIZE_TABLE);
      SET_STMT_TYPE(T_OPTIMIZE_TENANT);
      SET_STMT_TYPE(T_OPTIMIZE_ALL);
      // view
      SET_STMT_TYPE(T_CREATE_VIEW);
      SET_STMT_TYPE(T_ALTER_VIEW);
      SET_STMT_TYPE(T_DROP_VIEW);
      // index
      SET_STMT_TYPE(T_CREATE_INDEX);
      SET_STMT_TYPE(T_DROP_INDEX);
      // flashback
      SET_STMT_TYPE(T_FLASHBACK_TENANT);
      SET_STMT_TYPE(T_FLASHBACK_DATABASE);
      SET_STMT_TYPE(T_FLASHBACK_TABLE_FROM_RECYCLEBIN);
      SET_STMT_TYPE(T_FLASHBACK_TABLE_TO_SCN);
      SET_STMT_TYPE(T_FLASHBACK_INDEX);
      // purge
      SET_STMT_TYPE(T_PURGE_RECYCLEBIN);
      SET_STMT_TYPE(T_PURGE_TENANT);
      SET_STMT_TYPE(T_PURGE_DATABASE);
      SET_STMT_TYPE(T_PURGE_TABLE);
      SET_STMT_TYPE(T_PURGE_INDEX);
      // outline
      SET_STMT_TYPE(T_CREATE_OUTLINE);
      SET_STMT_TYPE(T_ALTER_OUTLINE);
      SET_STMT_TYPE(T_DROP_OUTLINE);
      // synonym
      SET_STMT_TYPE(T_CREATE_SYNONYM);
      SET_STMT_TYPE(T_DROP_SYNONYM);
      // directory
      SET_STMT_TYPE(T_CREATE_DIRECTORY);
      SET_STMT_TYPE(T_DROP_DIRECTORY);
      // variable set
      SET_STMT_TYPE(T_VARIABLE_SET);
      // get diagnostics
      SET_STMT_TYPE(T_DIAGNOSTICS);
      // read only
      SET_STMT_TYPE(T_EXPLAIN);
      SET_STMT_TYPE(T_SHOW_COLUMNS);
      SET_STMT_TYPE(T_SHOW_TABLES);
      SET_STMT_TYPE(T_SHOW_DATABASES);
      SET_STMT_TYPE(T_SHOW_TABLE_STATUS);
      SET_STMT_TYPE(T_SHOW_SERVER_STATUS);
      SET_STMT_TYPE(T_SHOW_VARIABLES);
      SET_STMT_TYPE(T_SHOW_SCHEMA);
      SET_STMT_TYPE(T_SHOW_CREATE_DATABASE);
      SET_STMT_TYPE(T_SHOW_CREATE_TABLE);
      SET_STMT_TYPE(T_SHOW_CREATE_VIEW);
      SET_STMT_TYPE(T_SHOW_WARNINGS);
      SET_STMT_TYPE(T_SHOW_ERRORS);
      SET_STMT_TYPE(T_SHOW_GRANTS);
      SET_STMT_TYPE(T_SHOW_CHARSET);
      SET_STMT_TYPE(T_SHOW_COLLATION);
      SET_STMT_TYPE(T_SHOW_PARAMETERS);
      SET_STMT_TYPE(T_SHOW_INDEXES);
      SET_STMT_TYPE(T_SHOW_PROCESSLIST);
      SET_STMT_TYPE(T_SHOW_TABLEGROUPS);
      SET_STMT_TYPE(T_SHOW_RESTORE_PREVIEW);
      SET_STMT_TYPE(T_SHOW_TRIGGERS);
      SET_STMT_TYPE(T_HELP);
      SET_STMT_TYPE(T_SHOW_RECYCLEBIN);
      SET_STMT_TYPE(T_SHOW_PROFILE);
      SET_STMT_TYPE(T_SHOW_TENANT);
      SET_STMT_TYPE(T_SHOW_SEQUENCES);
      SET_STMT_TYPE(T_SHOW_STATUS);
      SET_STMT_TYPE(T_SHOW_CREATE_TENANT);
      SET_STMT_TYPE(T_SHOW_TRACE);
      SET_STMT_TYPE(T_SHOW_ENGINES);
      SET_STMT_TYPE(T_SHOW_PRIVILEGES);
      SET_STMT_TYPE(T_SHOW_CREATE_PROCEDURE);
      SET_STMT_TYPE(T_SHOW_CREATE_FUNCTION);
      SET_STMT_TYPE(T_SHOW_PROCEDURE_STATUS);
      SET_STMT_TYPE(T_SHOW_FUNCTION_STATUS);
      SET_STMT_TYPE(T_SHOW_CREATE_TABLEGROUP);
      SET_STMT_TYPE(T_SHOW_PROCEDURE_CODE);
      SET_STMT_TYPE(T_SHOW_FUNCTION_CODE);
      SET_STMT_TYPE(T_CREATE_SAVEPOINT);
      SET_STMT_TYPE(T_RELEASE_SAVEPOINT);
      SET_STMT_TYPE(T_ROLLBACK_SAVEPOINT);
      SET_STMT_TYPE(T_SHOW_QUERY_RESPONSE_TIME);
      SET_STMT_TYPE(T_CREATE_USER);
      SET_STMT_TYPE(T_DROP_USER);
      SET_STMT_TYPE(T_RENAME_USER);
      SET_STMT_TYPE(T_SET_PASSWORD);
      SET_STMT_TYPE(T_GRANT);
      SET_STMT_TYPE(T_GRANT_ROLE);
      SET_STMT_TYPE(T_SYSTEM_GRANT);
      SET_STMT_TYPE(T_SHOW_CREATE_TRIGGER);
      SET_STMT_TYPE(T_REVOKE);
      SET_STMT_TYPE(T_SYSTEM_REVOKE);
      SET_STMT_TYPE(T_REVOKE_ROLE);
      SET_STMT_TYPE(T_CREATE_CONTEXT);
      SET_STMT_TYPE(T_DROP_CONTEXT);
      SET_STMT_TYPE(T_SHOW_ENGINE);
      SET_STMT_TYPE(T_SHOW_OPEN_TABLES);
      SET_STMT_TYPE(T_REPAIR_TABLE);
      SET_STMT_TYPE(T_CHECKSUM_TABLE);
      SET_STMT_TYPE(T_CACHE_INDEX);
      SET_STMT_TYPE(T_LOAD_INDEX_INTO_CACHE);
#undef SET_STMT_TYPE
      case T_ROLLBACK:
      case T_COMMIT: {
        type = stmt::T_END_TRANS;
      }
      break;
      case T_BEGIN: {
        type = stmt::T_START_TRANS;
      }
      break;
      case T_ALTER_SYSTEM_KILL: {
        type = stmt::T_KILL;
      }
      break;
      case T_MULTI_INSERT: {
        type = stmt::T_INSERT;
      }
      break;
      case T_SP_CREATE_TYPE: {
        type = stmt::T_CREATE_TYPE;
      }
      break;
      case T_SP_DROP_TYPE: {
        type = stmt::T_DROP_TYPE;
      }
      break;
      // stored procedure
      case T_SP_CREATE:
      case T_SF_CREATE: {
        type = stmt::T_CREATE_ROUTINE;
      }
      break;
      case T_SP_ALTER:
      case T_SF_ALTER: {
        type = stmt::T_ALTER_ROUTINE;
      }
      break;
      case T_SP_DROP:
      case T_SF_DROP: {
        type = stmt::T_DROP_ROUTINE;
      }
      break;
      // package
      case T_PACKAGE_CREATE: {
        type = stmt::T_CREATE_PACKAGE;
      }
      break;
      case T_PACKAGE_CREATE_BODY: {
        type = stmt::T_CREATE_PACKAGE_BODY;
      }
      break;
      case T_PACKAGE_ALTER: {
        type = stmt::T_ALTER_PACKAGE;
      }
      break;
      case T_PACKAGE_DROP: {
        type = stmt::T_DROP_PACKAGE;
      }
      break;
      case T_SP_ANONYMOUS_BLOCK: {
        type = stmt::T_ANONYMOUS_BLOCK;
      }
      break;
      case T_TG_CREATE: {
        type = stmt::T_CREATE_TRIGGER;
      }
      break;
      case T_TG_DROP: {
        type = stmt::T_DROP_TRIGGER;
      }
      break;
      case T_TG_ALTER: {
        type = stmt::T_ALTER_TRIGGER;
      } break;
      case T_LOAD_DATA: {
        type = stmt::T_LOAD_DATA;
      }
      break;
      case T_LOCK_TABLE: {
        type = stmt::T_LOCK_TABLE;
      }
      break;
      case T_ALTER_USER_DEFAULT_ROLE: {
        type = stmt::T_ALTER_USER_PROFILE;
      }
      break;
      case T_SP_CALL_STMT: {
        type = stmt::T_CALL_PROCEDURE;
      }
      break;
      default: {
        type = stmt::T_NONE;
      }
      break;
    }

    return type;
}

int ObResolverUtils::resolve_stmt_type(const ParseResult &result, stmt::StmtType &type)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(result.result_tree_));
  CK((T_STMT_LIST == result.result_tree_->type_));
  CK((1 == result.result_tree_->num_child_));
  CK(OB_NOT_NULL(result.result_tree_->children_[0]));
  if (OB_SUCC(ret)) {
    type = get_stmt_type_by_item_type(result.result_tree_->children_[0]->type_);
  }
  return ret;
}

int ObResolverUtils::set_string_val_charset(ObIAllocator &allocator,
                                            ObObjParam &val, ObString &charset, ObObj &result_val,
                                            bool is_strict_mode,
                                            bool return_ret)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(charset.trim()))) {
    ret = OB_ERR_UNKNOWN_CHARSET;
    LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
  } else {
    // use the default collation of the specified charset
    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
    val.set_collation_type(collation_type);
    LOG_DEBUG("use default collation", K(charset_type), K(collation_type));
    ObLength length = static_cast<ObLength>(ObCharset::strlen_char(val.get_collation_type(),
          val.get_string_ptr(),
          val.get_string_len()));
    val.set_length(length);

    //pad 0 front.
    const ObCharsetInfo *cs = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(cs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    } else if (cs->mbminlen > 0 && val.get_string_len() % cs->mbminlen != 0) {
      int64_t align_offset = cs->mbminlen - val.get_string_len() % cs->mbminlen;
      char *buf = NULL;
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator.alloc(val.get_string_len() + align_offset))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMSET(buf, 0, align_offset);
        MEMCPY(buf + align_offset, val.get_string_ptr(), val.get_string_len());
        val.set_string(val.get_type(), buf, static_cast<int32_t>(val.get_string_len() + align_offset));
      }
    }

    // 为了跟mysql报错一样，这里检查一下字符串是否合法，仅仅是检查，不合法则报错，不做其他操作
    // check_well_formed_str的ret_error参数为true的时候，is_strict_mode参数失效，因此这里is_strict_mode直接传入true
    if (OB_SUCC(ret) && OB_FAIL(ObSQLUtils::check_well_formed_str(val, result_val, is_strict_mode, return_ret))) {
      LOG_WARN("invalid str", K(ret), K(val), K(is_strict_mode), K(return_ret));
    }
  }
  return ret;
}

int ObResolverUtils::resolve_const(const ParseNode *node,
                                   const stmt::StmtType stmt_type,
                                   ObIAllocator &allocator,
                                   const ObCollationType connection_collation,
                                   const ObCollationType nchar_collation,
                                   const ObTimeZoneInfo *tz_info,
                                   ObObjParam &val,
                                   const bool is_paramlize,
                                   common::ObString &literal_prefix,
                                   const ObLengthSemantics default_length_semantics,
                                   const ObCollationType server_collation,
                                   ObExprInfo *parents_expr_info,
                                   const ObSQLMode sql_mode,
                                   bool enable_decimal_int_type,
                                   const ObCompatType compat_type,
                                   bool is_from_pl /* false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_UNLIKELY(node->type_ < T_INVALID) || OB_UNLIKELY(node->type_ >= T_MAX_CONST)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse node is invalid", K(node));
  } else {
    LOG_DEBUG("resolve item", "item_type", get_type_name(static_cast<int>(node->type_)), K(stmt_type));
    int16_t precision = PRECISION_UNKNOWN_YET;
    int16_t scale = SCALE_UNKNOWN_YET;
    // const value contains NOT_NULL flag
    val.set_result_flag(NOT_NULL_FLAG);
    switch (node->type_) {
    case T_HEX_STRING: {
      if (node->str_len_ > OB_MAX_LONGTEXT_LENGTH) {
        ret = OB_ERR_INVALID_INPUT_ARGUMENT;
        LOG_WARN("input str len is over size", K(ret), K(node->str_len_));
      } else {
        ObString str_val;
        str_val.assign_ptr(const_cast<char *>(node->str_value_), static_cast<int32_t>(node->str_len_));
        val.set_hex_string(str_val);
        val.set_collation_level(CS_LEVEL_COERCIBLE);
        val.set_scale(0);
        val.set_length(static_cast<int32_t>(node->str_len_));
        val.set_param_meta(val.get_meta());
      }
      break;
    }
    case T_VARCHAR:
    case T_CHAR:
    case T_NVARCHAR2:
    case T_NCHAR: {
      bool is_nchar = T_NVARCHAR2 == node->type_ || T_NCHAR == node->type_;
      if (lib::is_oracle_mode() && node->str_len_ == 0) {
        val.set_null();
        val.unset_result_flag(NOT_NULL_FLAG);
        val.set_length(0);
        val.set_length_semantics(default_length_semantics);
        //区分Oracle 模式下的空串和null
        //空串类型为char 值为null
        ObObjMeta null_meta = ObObjMeta();
        null_meta.set_collation_type(is_nchar ? nchar_collation : server_collation);
        null_meta.set_type(is_nchar ? ObNCharType : ObCharType);
        null_meta.set_collation_level(CS_LEVEL_COERCIBLE);
        val.set_param_meta(null_meta);
      } else {
        ObString str_val;
        ObObj result_val;
        str_val.assign_ptr(const_cast<char *>(node->str_value_), static_cast<int32_t>(node->str_len_));
        val.set_string(lib::is_mysql_mode() && is_nchar ?
                              ObVarcharType : static_cast<ObObjType>(node->type_), str_val);
        // decide collation
        /*
         MySQL determines a literal's character set and collation in the following manner:

         If both _X and COLLATE Y are specified, character set X and collation Y are used.

         If _X is specified but COLLATE is not specified, character set X and its default collation
         are used. To see the default collation for each character set, use the SHOW COLLATION statement.

         Otherwise, the character set and collation given by the character_set_connection and
         collation_connection system variables are used.
         */
        val.set_collation_level(CS_LEVEL_COERCIBLE);
        //TODO::@yanhua  raw
//        if (lib::is_oracle_mode()) {
//          val.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
//          LOG_DEBUG("oracle use default cs_type", K(val), K(connection_collation));
//        } else if (0 == node->num_child_) {
        if (node->str_len_ > OB_MAX_LONGTEXT_LENGTH) {
          ret = OB_ERR_INVALID_INPUT_ARGUMENT;
          LOG_WARN("input str len is over size", K(ret), K(node->str_len_));
        } else if (0 == node->num_child_) {
          // for STRING without collation, e.g. show tables like STRING;
          if (lib::is_mysql_mode() && is_nchar) {
            ObString charset(strlen("utf8mb4"), "utf8mb4");
            if (OB_FAIL(set_string_val_charset(allocator, val, charset, result_val, false, false))) {
              LOG_WARN("set string val charset failed", K(ret));
            }
          } else {
            val.set_collation_type(connection_collation);
          }
        } else {
          // STRING in SQL expression
          ParseNode *charset_node = NULL;
          if (NULL != node->children_[0] && T_CHARSET == node->children_[0]->type_) {
            charset_node = node->children_[0];
          }

          if (NULL == charset_node) {
            val.set_collation_type(connection_collation);
          } else {
            ObCharsetType charset_type = CHARSET_INVALID;
            ObCollationType collation_type = CS_TYPE_INVALID;
            bool is_hex_string = node->num_child_ == 1 ? true : false;
            if (charset_node != NULL) {
              ObString charset(charset_node->str_len_, charset_node->str_value_);
              if (OB_FAIL(set_string_val_charset(allocator, val, charset, result_val, false, is_hex_string))) {
                LOG_WARN("set string val charset failed", K(ret));
              }
            }
          }
        }
        ObLengthSemantics length_semantics = LS_DEFAULT;
        if (OB_SUCC(ret)) {
          if (lib::is_oracle_mode() && (T_NVARCHAR2 == node->type_ || T_NCHAR == node->type_)) {
            length_semantics = LS_CHAR;
          } else {
            length_semantics = default_length_semantics;
          }
          if (is_oracle_byte_length(lib::is_oracle_mode(), length_semantics)
              && T_NVARCHAR2 != node->type_
              && T_NCHAR != node->type_) {
            val.set_length(val.get_string_len());
            val.set_length_semantics(LS_BYTE);
          } else {
            ObLength length = static_cast<ObLength>(ObCharset::strlen_char(val.get_collation_type(),
                                                                           val.get_string_ptr(),
                                                                           val.get_string_len()));
            val.set_length(length);
            val.set_length_semantics(LS_CHAR);
          }
        }

        // 对于字符，此处使用的是连接里设置的字符集，在Oracle模式下，需要
        // 转换成server使用的字符集，MySQL不需要
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (lib::is_oracle_mode()) {
          ObCollationType target_collation = CS_TYPE_INVALID;
          if (T_NVARCHAR2 == node->type_ || T_NCHAR == node->type_) {
            target_collation = nchar_collation;
          } else {
            target_collation = server_collation;
          }
          ObString str;
          val.get_string(str);
          char *buf = nullptr;
          /* OB目前支持utf8、utf16、和gbk
          * utf8mb4是1~4个字节，gbk是1到2，utf16是2或者4
          * 进行转换，极限情况是1个字节转成4个，所以这里放大了4倍
          */
          const int CharConvertFactorNum = 4;
          int32_t buf_len = str.length() * CharConvertFactorNum;
          uint32_t result_len = 0;
          uint32_t incomplete_len = 0;
          if (0 == buf_len) {
            //do nothing
          } else if (CS_TYPE_INVALID == target_collation) {
            ret = OB_ERR_UNEXPECTED;
          }
          if (OB_FAIL(ret)) {
          } else if (node->value_ == -1) {
            // process u'string': connection_char->unicode->target_char
            if (OB_UNLIKELY(node->type_ != T_NCHAR)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("u string is not nchar", K(ret));
            } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_ERROR("alloc memory failed", K(ret), K(buf_len));
            } else if (OB_FAIL(ObExprUnistr::calc_unistr(str, connection_collation, target_collation, buf, buf_len,
                                                                                (int32_t &)(result_len)))) {
              LOG_WARN("calc unistr failed", K(ret));
            } else {
              str.assign_ptr(buf, result_len);
              val.set_string(val.get_type(), buf, result_len);
              val.set_collation_type(nchar_collation);
              val.set_collation_level(CS_LEVEL_COERCIBLE);
            }
          } else {
            // 为了解决部分gbk字符因为转换函数中不包含对应unicode字符导致全gbk链路中字符解析错误的问题，
            // 当两个ObCollationType的CharsetType相同时，跳过 mb_wc, wc_mb 的转换过程，
            // 直接设置结果的collation_type
            if (ObCharset::charset_type_by_coll(connection_collation) ==
                ObCharset::charset_type_by_coll(target_collation)) {
              val.set_collation_type(target_collation);
            } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_ERROR("alloc memory failed", K(ret), K(buf_len));
            } else {
              bool trim_incomplete_tail = !(lib::is_oracle_mode());
              ret = ObCharset::charset_convert(connection_collation, str.ptr(),
                    str.length(), target_collation, buf, buf_len, result_len, trim_incomplete_tail);
              if (OB_SUCCESS != ret) {
                int32_t str_offset = 0;
                int64_t buf_offset = 0;
                ObString question_mark = ObCharsetUtils::get_const_str(target_collation, '?');
                while (str_offset < str.length() && buf_offset + question_mark.length() <= buf_len) {
                  int64_t offset = ObCharset::charpos(connection_collation, str.ptr() + str_offset,
                      str.length() - str_offset, 1);
                  ret = ObCharset::charset_convert(connection_collation, str.ptr() + str_offset,
                    offset, target_collation, buf + buf_offset, buf_len - buf_offset, result_len);
                  str_offset += offset;
                  if (OB_SUCCESS == ret && result_len > 0) {
                    buf_offset += result_len;
                  } else {
                    //在Oracle转换失败的字符都是用'?'代替，这里做兼容
                    MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
                    buf_offset += question_mark.length();
                  }
                }
                if (str_offset < str.length()) {
                  ret = OB_SIZE_OVERFLOW;
                  LOG_WARN("size overflow", K(ret), K(str), KPHEX(str.ptr(), str.length()));
                } else {
                  // 这里打印日志是为了提醒存在转换失败的字符用'?'替代了
                  LOG_DEBUG("charset convert failed", K(ret), K(connection_collation), K(target_collation));
                  result_len = buf_offset;
                  ret = OB_SUCCESS;
                }
              }

              if (OB_SUCC(ret)) {
                val.set_string(val.get_type(), buf, static_cast<int32_t>(result_len));
                val.set_collation_type(target_collation);
                val.set_collation_level(CS_LEVEL_COERCIBLE);
              }
            }
          }
        }

        // 为了跟mysql报错一样，这里检查一下字符串是否合法，仅仅是检查，不合法则报错，不做其他操作
        // check_well_formed_str的ret_error参数为true的时候，is_strict_mode参数失效，因此这里is_strict_mode直接传入true
        //if (OB_SUCC(ret) && lib::is_mysql_mode() &&
        //    OB_FAIL(ObSQLUtils::check_well_formed_str(val, result_val, true, true))) {
        //  LOG_WARN("invalid str", K(ret), K(val));
        //}
        val.set_param_meta(val.get_meta());
      }
      LOG_DEBUG("resolve const char", K(val));
      break;
    }
    case T_IEEE754_NAN: {
      double value = NAN;
      val.set_double(value);
      val.set_precision(PRECISION_UNKNOWN_YET);
      val.set_scale(NUMBER_SCALE_UNKNOWN_YET);
      val.set_param_meta(val.get_meta());
      _LOG_DEBUG("finish parse double NAN");
      break;
    }
    case T_IEEE754_INFINITE: {
      double value = INFINITY;
      val.set_double(value);
      val.set_precision(PRECISION_UNKNOWN_YET);
      val.set_scale(NUMBER_SCALE_UNKNOWN_YET);
      val.set_param_meta(val.get_meta());
      _LOG_DEBUG("finish parse double INFINITE");
      break;
    }
    case T_FLOAT:
    case T_DOUBLE: {
      int err = 0;
      double value = 0;
      char *endptr = NULL;
      value = strntod(node->str_value_, static_cast<int32_t>(node->str_len_),
                      node->type_, &endptr, &err);
      if (EOVERFLOW == err) {
        literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
        if (is_oracle_mode()) {
          ret = OB_NUMERIC_OVERFLOW;
          LOG_WARN("float/double out of range", K(literal_prefix), K(val), K(ret));
        } else {
          ret = OB_ERR_ILLEGAL_VALUE_FOR_TYPE;
          LOG_USER_ERROR(OB_ERR_ILLEGAL_VALUE_FOR_TYPE, "double", literal_prefix.length(),
                        literal_prefix.ptr());
        }
      } else {
        if (T_DOUBLE == node->type_) {
          val.set_double(value);
        } else {
          val.set_float(value);
        }
        val.set_length(static_cast<int16_t>(node->str_len_));
        ObString tmp_input(static_cast<int32_t>(node->str_len_), node->str_value_);
        if (lib::is_oracle_mode()) {
          literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
          val.set_precision(PRECISION_UNKNOWN_YET);
          val.set_scale(NUMBER_SCALE_UNKNOWN_YET);
        }
        _LOG_DEBUG("finish parse double from str, tmp_input=%.*s, value=%.30lf, ret=%d, type=%d, literal_prefix=%.*s",
                   tmp_input.length(), tmp_input.ptr(), value, ret, node->type_, literal_prefix.length(), literal_prefix.ptr());
      }
      val.set_param_meta(val.get_meta());
      break;
    }
    case T_UINT64:
    case T_INT: {
      if (NULL != parents_expr_info) {
        LOG_DEBUG("T_INT as pl acc idx", K(parents_expr_info->has_member(IS_PL_ACCESS_IDX)));
      }
      if (lib::is_oracle_mode() && NULL != node->str_value_
          && (NULL == parents_expr_info || !parents_expr_info->has_member(IS_PL_ACCESS_IDX))) {
        //go through as T_NUMBER
      } else {
        if (T_INT == node->type_) {
          val.set_int(node->value_);
        } else {
          val.set_uint64(static_cast<uint64_t>(node->value_));
        }
        val.set_scale(0);
        val.set_precision(static_cast<int16_t>(node->str_len_));
        val.set_length(static_cast<int16_t>(node->str_len_));
        val.set_param_meta(val.get_meta());
        if (true == node->is_date_unit_) {
          literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
        }

        break;
      }
    }
    case T_NUMBER: {
      number::ObNumber nmb;
      ObDecimalInt *decint = nullptr;
      int16_t len = 0;
      ObString tmp_string(static_cast<int32_t>(node->str_len_), node->str_value_);
      bool use_decimalint_as_result = false;
      int tmp_ret = OB_E(EventTable::EN_ENABLE_ORA_DECINT_CONST) OB_SUCCESS;

      if (enable_decimal_int_type && !is_from_pl && OB_SUCC(tmp_ret)) {
        // 如果开启decimal int类型，T_NUMBER解析成decimal int
        int32_t val_len = 0;
        ret = wide::from_string(node->str_value_, node->str_len_, allocator, scale, precision,
                                val_len, decint);
        len = static_cast<int16_t>(val_len);
        // in oracle mode
        // +.12e-3 will parse as decimal, with scale = 5, precision = 2
        // in this case, ObNumber is used.
        use_decimalint_as_result = (precision <= OB_MAX_DECIMAL_POSSIBLE_PRECISION
                                    && scale <= OB_MAX_DECIMAL_POSSIBLE_PRECISION
                                    && scale >= 0
                                    && precision >= scale);
        if (lib::is_oracle_mode()
            && (ObStmt::is_ddl_stmt(stmt_type, true) || ObStmt::is_show_stmt(stmt_type)
                || precision > OB_MAX_NUMBER_PRECISION)) {
          use_decimalint_as_result = false;
        }
      }
      if (use_decimalint_as_result) {
        // do nothing, result type is decimal int
      } else if (NULL != tmp_string.find('e') || NULL != tmp_string.find('E')) {
        ret = nmb.from_sci(node->str_value_, static_cast<int32_t>(node->str_len_), allocator, &precision, &scale);
        len = static_cast<int16_t>(node->str_len_);
        if (lib::is_oracle_mode()) {
          literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
        }
      } else {
        ret = nmb.from(node->str_value_, static_cast<int32_t>(node->str_len_), allocator, &precision, &scale);
        len = static_cast<int16_t>(node->str_len_);
      }
      if (OB_FAIL(ret)) {
        if (OB_INTEGER_PRECISION_OVERFLOW == ret) {
          LOG_WARN("integer presision overflow", K(ret));
        } else if (OB_NUMERIC_OVERFLOW == ret) {
          LOG_WARN("numeric overflow");
        } else {
          LOG_WARN("unexpected error", K(ret));
        }
      } else {
        if (use_decimalint_as_result) {
          val.set_decimal_int(len, scale, decint);
          val.set_precision(precision);
          val.set_scale(scale);
          val.set_length(len);
          LOG_DEBUG("finish parse decimal int from str", K(literal_prefix), K(precision), K(scale));
        } else {
          if (lib::is_oracle_mode()) {
            precision = PRECISION_UNKNOWN_YET;
            scale = NUMBER_SCALE_UNKNOWN_YET;
          }
          val.set_number(nmb);
          val.set_precision(precision);
          val.set_scale(scale);
          val.set_length(len);
          LOG_DEBUG("finish parse number from str", K(literal_prefix), K(nmb), K(precision),
                    K(scale));
        }
      }
      val.set_param_meta(val.get_meta());
      break;
    }
    case T_NUMBER_FLOAT: {
      number::ObNumber nmb;
      if (OB_FAIL(nmb.from(node->str_value_, static_cast<int32_t>(node->str_len_), allocator, &precision, &scale))) {
        if (OB_INTEGER_PRECISION_OVERFLOW == ret) {
          LOG_WARN("integer presision overflow", K(ret));
        } else if (OB_NUMERIC_OVERFLOW == ret) {
          LOG_WARN("numeric overflow");
        } else {
          LOG_WARN("unexpected error", K(ret));
        }
      } else {
        val.set_number(nmb);
        val.set_precision(PRECISION_UNKNOWN_YET);
        val.set_length(static_cast<int16_t>(node->str_len_));
      }
      val.set_param_meta(val.get_meta());
      break;
    }
    case T_QUESTIONMARK: {
      if (!is_paramlize) {
        val.set_unknown(node->value_);
      } else {
        //used for sql 限流
        val.set_int(0);
        val.set_scale(0);
        val.set_precision(1);
        val.set_length(1);
      }
      val.set_param_meta(val.get_meta());
      break;
    }
    case T_BOOL: {
      val.set_is_boolean(true);
      val.set_bool(node->value_ == 1 ? true : false);
      val.set_scale(0);
      val.set_precision(1);
      val.set_length(1);
      val.set_param_meta(val.get_meta());
      break;
    }
    case T_YEAR: {
      ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
      uint8_t time_val = 0;
      if (OB_FAIL(ObTimeConverter::str_to_year(time_str, time_val))) {
        LOG_WARN("fail to convert str to year", K(time_str), K(ret));
      } else {
        val.set_year(time_val);
        val.set_scale(0);
        val.set_param_meta(val.get_meta());
      }
      break;
    }
    case T_DATE: {
      ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
      int32_t time_val = 0;
      ObDateSqlMode date_sql_mode;
      if (FALSE_IT(date_sql_mode.init(sql_mode))) {
      } else if (OB_FAIL(ObTimeConverter::str_to_date(time_str, time_val, date_sql_mode))) {
        ret = OB_ERR_WRONG_VALUE;
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "DATE", to_cstring(time_str));
      } else {
        val.set_date(time_val);
        val.set_scale(0);
        val.set_param_meta(val.get_meta());
        literal_prefix = ObString::make_string(LITERAL_PREFIX_DATE);
      }
      break;
    }
    case T_TIME: {
      ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
      int64_t time_val = 0;
      if (OB_FAIL(ObTimeConverter::str_to_time(time_str, time_val, &scale))) {
        ret = OB_ERR_WRONG_VALUE;
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "TIME", to_cstring(time_str));
      } else {
        val.set_time(time_val);
        val.set_scale(scale);
        val.set_param_meta(val.get_meta());
        literal_prefix = ObString::make_string(MYSQL_LITERAL_PREFIX_TIME);
      }
      break;
    }
    case T_DATETIME: {
      ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
      int64_t time_val = 0;
      ObTimeConvertCtx cvrt_ctx(tz_info, false);
      if (OB_FAIL(ObTimeConverter::literal_date_validate_oracle(time_str, cvrt_ctx, time_val))) {
        LOG_WARN("fail to convert str to oracle date", K(time_str), K(ret));
      } else {
        val.set_datetime(time_val);
        val.set_scale(OB_MAX_DATE_PRECISION);
        val.set_param_meta(val.get_meta());
        literal_prefix = ObString::make_string(LITERAL_PREFIX_DATE);
      }
      break;
    }
    case T_TIMESTAMP: {
      ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
      int64_t time_val = 0;
      ObDateSqlMode date_sql_mode;
      ObTimeConvertCtx cvrt_ctx(tz_info, false);
      if (FALSE_IT(date_sql_mode.init(sql_mode))) {
      } else if (FALSE_IT(date_sql_mode.allow_invalid_dates_ = false)) {
      } else if (OB_FAIL(ObTimeConverter::str_to_datetime(time_str, cvrt_ctx, time_val, &scale, date_sql_mode))) {
        ret = OB_ERR_WRONG_VALUE;
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "DATETIME", to_cstring(time_str));
      } else {
        val.set_datetime(time_val);
        val.set_scale(scale);
        val.set_param_meta(val.get_meta());
        literal_prefix = ObString::make_string(LITERAL_PREFIX_TIMESTAMP);
      }
      break;
    }
    case T_TIMESTAMP_TZ: {
      ObObjType value_type = ObNullType;
      ObOTimestampData tz_value;
      ObTimeConvertCtx cvrt_ctx(tz_info, true);
      ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
      //if (OB_FAIL(ObTimeConverter::str_to_otimestamp(time_str, cvrt_ctx, tmp_type, ot_data))) {
      if (OB_FAIL(ObTimeConverter::literal_timestamp_validate_oracle(time_str, cvrt_ctx, value_type, tz_value))) {
        LOG_WARN("fail to str_to_otimestamp", K(time_str), K(ret));
      } else {
        /* use max scale bug:#18093350 */
        val.set_otimestamp_value(value_type, tz_value);
        val.set_scale(OB_MAX_TIMESTAMP_TZ_PRECISION);
        literal_prefix = ObString::make_string(LITERAL_PREFIX_TIMESTAMP);
        val.set_param_meta(val.get_meta());
      }
      break;
    };
    case T_INTERVAL_YM: {
      char *tmp_ptr = NULL;
      ObString tmp_str;
      ObIntervalYMValue interval_value;
      ObScale defined_scale;
      ObString interval_str;
      int16_t leading_precision = -1;
      ObDateUnitType part_begin = DATE_UNIT_MAX;
      ObDateUnitType part_end = DATE_UNIT_MAX;
      ObValueChecker<int16_t> precision_checker(0, MAX_SCALE_FOR_ORACLE_TEMPORAL,
                                                OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE);

      literal_prefix.assign_ptr(node->raw_text_, static_cast<int32_t>(node->text_len_));

      OZ (ob_dup_cstring(allocator, literal_prefix, tmp_ptr));
      OX (tmp_str = ObString(tmp_ptr));

      if (OB_SUCC(ret)) {
        char *to_pos = strcasestr(tmp_ptr, "to");
        if (OB_NOT_NULL(to_pos)) {
          *to_pos = '\0';
          OZ (parse_interval_ym_type(tmp_ptr, part_begin));
          OZ (parse_interval_ym_type(to_pos + 1, part_end));
        } else {
          OZ (parse_interval_ym_type(tmp_ptr, part_begin));
          part_end = part_begin;
        }
      }

      OZ (parse_interval_precision(tmp_ptr, leading_precision, is_from_pl ? 9 : 2));
      OZ (precision_checker.validate(leading_precision), leading_precision);

      if (OB_SUCC(ret)) {
        defined_scale = ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(static_cast<int8_t>(leading_precision));
        interval_str.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
        ObScale scale = defined_scale;
        if (OB_FAIL(ObTimeConverter::literal_interval_ym_validate_oracle(interval_str,
                                                                         interval_value,
                                                                         scale,
                                                                         part_begin,
                                                                         part_end))) {
          LOG_WARN("fail to validate interval literal", K(ret), K(interval_str), K(part_begin), K(part_end), K(scale));
        } else {
          val.set_interval_ym(interval_value);
          val.set_scale(defined_scale);
          val.set_param_meta(val.get_meta());
        }
      }

      LOG_DEBUG("interval year to month literal resolve", K(interval_str), K(literal_prefix));
      break;
    }
    case T_INTERVAL_DS: {
      char *tmp_ptr = NULL;
      ObString tmp_str;
      ObIntervalDSValue interval_value;
      ObScale defined_scale;
      ObString interval_str;
      int16_t leading_precision = -1;
      int16_t second_precision = -1;
      ObDateUnitType part_begin = DATE_UNIT_MAX;
      ObDateUnitType part_end = DATE_UNIT_MAX;
      ObValueChecker<int16_t> precision_checker(0, MAX_SCALE_FOR_ORACLE_TEMPORAL,
                                                OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE);

      literal_prefix.assign_ptr(node->raw_text_, static_cast<int32_t>(node->text_len_));
      OZ (ob_dup_cstring(allocator, literal_prefix, tmp_ptr));
      OX (tmp_str = ObString(tmp_ptr));

      if (OB_SUCC(ret)) {
        char *to_pos = strcasestr(tmp_ptr, "to");
        if (OB_NOT_NULL(to_pos)) {  // interval 'xxx' day (x) to second (x)
          *to_pos = '\0';
          OZ (parse_interval_ds_type(tmp_ptr, part_begin));
          OZ (parse_interval_precision(tmp_ptr, leading_precision, is_from_pl ? 9 : 2));
          OZ (parse_interval_ds_type(to_pos + 1, part_end));
          OZ (parse_interval_precision(to_pos + 1, second_precision,
                                                  is_from_pl ? 9 : (DATE_UNIT_SECOND == part_end ? 6 : 0)));
        } else {  // interval 'xxx' day (x)
          char *comma_pos = strchr(tmp_ptr, ',');
          OZ (parse_interval_ds_type(tmp_ptr, part_begin));
          part_end = part_begin;
          if (OB_NOT_NULL(comma_pos)) { // interval 'xxx' second(x, x)
            *comma_pos = ')';
            OZ (parse_interval_precision(tmp_ptr, leading_precision, is_from_pl ? 9 : 2));
            *comma_pos = '(';
            OZ (parse_interval_precision(comma_pos, second_precision, is_from_pl ? 9 : 6));
          } else {
            OZ (parse_interval_precision(tmp_ptr, leading_precision, is_from_pl ? 9 : 2));
            if (OB_SUCC(ret) && is_from_pl) {
              second_precision = 9;
            } else {
              second_precision = DATE_UNIT_SECOND == part_end ? 6 : 0;
            }
          }
        }
      }


      OZ (precision_checker.validate(leading_precision), leading_precision);
      OZ (precision_checker.validate(second_precision), second_precision);

      if (OB_SUCC(ret)) {
        defined_scale = ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(static_cast<int8_t>(leading_precision),
                                                                           static_cast<int8_t>(second_precision));
        interval_str.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
        ObScale scale = defined_scale;
        if (OB_FAIL(ObTimeConverter::literal_interval_ds_validate_oracle(interval_str,
                                                                         interval_value,
                                                                         scale,
                                                                         part_begin,
                                                                         part_end))) {
          LOG_WARN("fail to validate interval literal", K(ret), K(interval_str), K(part_begin), K(part_end), K(scale));
        } else {
          val.set_interval_ds(interval_value);
          val.set_scale(defined_scale);
          val.set_param_meta(val.get_meta());
        }
      }

      LOG_DEBUG("interval day to second literal resolve", K(interval_str), K(literal_prefix));
      break;
    }
    case T_NULL: {
      if (OB_UNLIKELY(compat_type == COMPAT_MYSQL8 && node->value_ == 1)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "\\N in MySQL8");
        LOG_WARN("\\N is not supprted in MySQL8", K(ret));
      } else {
        val.set_null();
        val.unset_result_flag(NOT_NULL_FLAG);
        val.set_length(0);
        val.set_param_meta(val.get_meta());
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unknown item type here", K(ret), K(node->type_));
    }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_timestamp_node(const bool is_set_null,
                                            const bool is_set_default,
                                            const bool is_first_timestamp_column,
                                            ObSQLSessionInfo *session_info,
                                            ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  //mysql的非标准行为：
  bool explicit_value = false;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_FAIL(session_info->get_explicit_defaults_for_timestamp(explicit_value))) {
    LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
  } else if (!explicit_value && !column.is_generated_column()) {
    //（1）每个表的第一个timestamp列，如果没有显式的指定NULL属性，也没有显示的指定default
    //系统会自动为该列添加Default current_timestamp、on update current_timestamp属性；
    if (is_first_timestamp_column && !is_set_null && !is_set_default && !column.is_on_update_current_timestamp()) {
      column.set_nullable(false);
      const_cast<ObObj*>(&column.get_cur_default_value())->set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
      column.set_on_update_current_timestamp(true);
    } else if (!is_set_null) {
      //（2）timestamp列默认为not null
      column.set_nullable(false);
      //(3)如果没有显式指定默认值，此时timestamp列的默认值为0
      if (!is_set_default) {
        if (is_no_zero_date(session_info->get_sql_mode())) {
          ret = OB_INVALID_DEFAULT;
          LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        } else {
          const_cast<ObObj*>(&column.get_cur_default_value())->set_timestamp(ObTimeConverter::ZERO_DATETIME);
        }
      } else if (column.get_cur_default_value().is_null()) {
        //(4) timestamp类型的列默认是NOT NULL，所以不容许设定列default NULL
        ret = OB_INVALID_DEFAULT;
        LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
      }
    } else {
      //如果主动设置可以为NULL
      column.set_nullable(true);
      if (!is_set_default) {
        const_cast<ObObj*>(&column.get_cur_default_value())->set_null();
      }
    }
  }
  return ret;
}

int ObResolverUtils::add_column_ids_without_duplicate(const uint64_t column_id, common::ObIArray<uint64_t> *array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array) || OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(array), K(column_id));
  } else {
    bool exist = false;
    for(int64_t i = 0; !exist && i < array->count(); i++) {
      if (array->at(i) == column_id) {
        exist = true;
      }
    }
    if (!exist) {
      if(OB_FAIL(array->push_back(column_id))) {
        LOG_WARN("fail to push back column id", K(ret));
      }
    }
  }
  return ret;
}

//for recursively process columns item in resolve_const_expr
//just wrap columns process logic in resolve_const_expr
int ObResolverUtils::resolve_columns_for_const_expr(ObRawExpr *&expr, ObArray<ObQualifiedName> &columns, ObResolverParams &resolve_params)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(resolve_params.allocator_));
  CK(OB_NOT_NULL(resolve_params.expr_factory_));
  CK(OB_NOT_NULL(resolve_params.session_info_));
  CK(OB_NOT_NULL(resolve_params.schema_checker_));
  CK(OB_NOT_NULL(resolve_params.schema_checker_->get_schema_guard()));
  // CK(OB_NOT_NULL(resolve_params.sql_proxy_));
  ObArray<ObRawExpr*> real_exprs;
  ObRawExpr* real_ref_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      if (OB_FAIL(q_name.access_idents_.at(0).check_param_num())) {
        LOG_WARN("sys func check param failed", K(ret));
      } else {
        real_ref_expr = q_name.access_idents_.at(0).sys_func_expr_;
      }
    } else if (q_name.is_pl_udf()) {
      if (OB_FAIL(ObResolverUtils::resolve_external_symbol(*resolve_params.allocator_,
                                                           *resolve_params.expr_factory_,
                                                           *resolve_params.session_info_,
                                                           *resolve_params.schema_checker_->get_schema_guard(),
                                                           resolve_params.sql_proxy_,
                                                           &(resolve_params.external_param_info_),
                                                           resolve_params.secondary_namespace_,
                                                           q_name,
                                                           columns,
                                                           real_exprs,
                                                           real_ref_expr,
                                                           resolve_params.package_guard_,
                                                           resolve_params.is_prepare_protocol_,
                                                           false, /*is_check_mode*/
                                                           true /*is_sql_scope*/))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "failed to resolve var", K(q_name), K(ret));
      }
    } else {
      //const expr can't use column
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString column_name = concat_qualified_name(q_name.database_name_,
                                                    q_name.tbl_name_, q_name.col_name_);
      ObString scope_name = ObString::make_string("field_list");
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(),
                      scope_name.length(), scope_name.ptr());
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(real_exprs.push_back(real_ref_expr))) {
        LOG_WARN("push back error", K(ret));
      }
    }
    //因为obj access的参数拉平处理，a(b,c)在columns会被存储为b,c,a，所以解释完一个ObQualifiedName
    //都要把他前面的ObQualifiedName拿过来尝试替换一遍参数
    for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
      OZ (ObRawExprUtils::replace_ref_column(
        real_ref_expr, columns.at(i).ref_expr_, real_exprs.at(i)));
    }
    OZ (ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr));
  }
  return ret;
}

int ObResolverUtils::resolve_const_expr(ObResolverParams &params,
                                        const ParseNode &node,
                                        ObRawExpr *&const_expr,
                                        ObIArray<ObVarInfo> *var_infos)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObOpRawExpr*> op_exprs;
  ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
  ObArray<ObInListInfo> inlist_infos;
  ObSEArray<ObMatchFunRawExpr*, 1> match_exprs;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObExprResolveContext ctx(*params.expr_factory_, params.session_info_->get_timezone_info(),
                             OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params.param_list_;
    ctx.is_extract_param_type_ = !params.is_prepare_protocol_; //when prepare do not extract
    ctx.schema_checker_ = params.schema_checker_;
    ctx.session_info_ = params.session_info_;
    ctx.secondary_namespace_ = params.secondary_namespace_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node, const_expr, columns, sys_vars,
                                             sub_query_info, aggr_exprs, win_exprs,
                                             udf_info, op_exprs, user_var_exprs, inlist_infos, match_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (OB_FAIL(resolve_columns_for_const_expr(const_expr, columns, params))) {
      LOG_WARN("resolve columnts for const expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "subqueries or stored function calls here");
    } else if (udf_info.count() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("UDFInfo should not found be here!!!", K(ret));
    } else if (OB_UNLIKELY(!inlist_infos.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in_expr should not been found here", K(ret));
    } else if (match_exprs.count() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fulltext search expr should not found be here", K(ret));
    }

    //process oracle compatible implicit conversion
    if (OB_SUCC(ret) && op_exprs.count() > 0) {
      LOG_WARN("impicit cast for oracle", K(ret));
      if (OB_FAIL(ObRawExprUtils::resolve_op_exprs_for_oracle_implicit_cast(ctx.expr_factory_,
                                                                  ctx.session_info_, op_exprs))) {
        LOG_WARN("impicit cast faild", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (T_SP_CPARAM == const_expr->get_expr_type()) {
        ObCallParamRawExpr *call_expr = static_cast<ObCallParamRawExpr*>(const_expr);
        CK (OB_NOT_NULL(call_expr->get_expr()));
        OZ (call_expr->get_expr()->formalize(params.session_info_));
      } else {
        OZ (const_expr->formalize(params.session_info_));
      }
      if (OB_SUCC(ret)) {
        if (var_infos != NULL) {
          if (OB_FAIL(ObRawExprUtils::merge_variables(sys_vars, *var_infos))) {
            LOG_WARN("merge variables failed", K(ret));
          }
        } else {
          params.prepare_param_count_ += ctx.prepare_param_count_; //prepare param count
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::get_collation_type_of_names(const ObSQLSessionInfo *session_info,
                                                 const ObNameTypeClass type_class,
                                                 ObCollationType &cs_type)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  cs_type = CS_TYPE_INVALID;
  if (lib::is_mysql_mode()) {
    if (OB_TABLE_NAME_CLASS == type_class) {
      if (OB_ISNULL(session_info)) {
        ret = OB_NOT_INIT;
        LOG_WARN("session info is null");
      } else if (OB_FAIL(session_info->get_name_case_mode(case_mode))) {
        LOG_WARN("fail to get name case mode", K(ret));
      } else if (OB_ORIGIN_AND_SENSITIVE == case_mode) {
        cs_type = CS_TYPE_UTF8MB4_BIN;
      } else if (OB_ORIGIN_AND_INSENSITIVE == case_mode || OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
        cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      }
    } else if (OB_COLUMN_NAME_CLASS == type_class) {
      cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    } else if (OB_USER_NAME_CLASS == type_class) {
      cs_type = CS_TYPE_UTF8MB4_BIN;
    }
  } else {
    cs_type = CS_TYPE_UTF8MB4_BIN;
  }
  return ret;
}

int ObResolverUtils::name_case_cmp(const ObSQLSessionInfo *session_info,
                                   const ObString &name,
                                   const ObString &name_other,
                                   const ObNameTypeClass type_class,
                                   bool &is_equal)
{

  ObCollationType cs_type = CS_TYPE_INVALID;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_collation_type_of_names(session_info, type_class, cs_type))) {
    LOG_WARN("fail to get collation type of name", K(name), K(name_other), K(type_class), K(ret));
  } else if (0 == ObCharset::strcmp(cs_type, name, name_other)) {
    is_equal = true;
  } else {
    is_equal = false;
  }
  return ret;
}

bool ObResolverUtils::is_valid_oracle_interval_data_type(
    const ObObjType data_type,
    ObItemType &item_type)
{
  bool bret = false;
  switch (data_type) {
    case ObIntType:
      bret = true;
      item_type = T_INT;
      break;
    // case ObFloatType:
    case ObNumberFloatType:
      bret = true;
      item_type = T_FLOAT;
      break;
    // case ObDoubleType:
    //   bret = true;
    //   item_type = T_DOUBLE;
    //   break;
    case ObNumberType:
      bret = true;
      item_type = T_NUMBER;
      break;
    case ObDateTimeType:
      bret = true;
      item_type = T_DATETIME;
      break;
    case ObTimestampNanoType:
      bret = true;
      item_type = T_TIMESTAMP_NANO;
      break;
    default:
      bret = false;
  }
  return bret;
}

bool ObResolverUtils::is_valid_oracle_partition_data_type(const ObObjType data_type,
    const bool check_value)
{
  bool bret = false;
  switch (data_type) {
    case ObIntType:
    case ObFloatType:
    case ObDoubleType:
    case ObNumberType:
    case ObDateTimeType:
    case ObVarcharType:
    case ObCharType:
    case ObTimestampNanoType:
    case ObDecimalIntType: {
      bret = true;
      break;
    }
    case ObTimestampType:
    case ObTimestampLTZType: {
      bret = !check_value;
      break;
    }
    case ObTimestampTZType: {
      bret = check_value;
      break;
    }
    case ObRawType:
    case ObIntervalYMType:
    case ObIntervalDSType:
    case ObNumberFloatType:
    case ObNVarchar2Type:
    case ObNCharType: {
      bret = true;
      break;
    }
    default: {
      bret = false;
    }
  }
  return bret;
}

bool ObResolverUtils::is_valid_partition_column_type(const ObObjType type,
    const ObPartitionFuncType part_type, const bool is_check_value)
{
  int bret = false;
  if (is_key_part(part_type)) {
    if (!ob_is_text_tc(type) && !ob_is_json_tc(type)) {
      bret = true;
    }
  } else if (PARTITION_FUNC_TYPE_HASH == part_type ||
      PARTITION_FUNC_TYPE_RANGE == part_type ||
      PARTITION_FUNC_TYPE_INTERVAL == part_type ||
      PARTITION_FUNC_TYPE_LIST == part_type) {
    //对partition by hash(c1) 中列的类型进行检查
    //对partition by range(c1) 中列的类型进行检查
    LOG_DEBUG("check partition column type", K(lib::is_oracle_mode()), K(part_type), K(type), K(lbt()));
    if (ob_is_integer_type(type) || ObYearType == type || ObBitType == type) {
      bret = true;
    } else if (lib::is_oracle_mode() && is_valid_oracle_partition_data_type(type, is_check_value)) {
      //oracle模式下的hash分区允许字符类型和各种数字型、时间日期(ts with tz除外)
      bret = true;
    }
  } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    if (lib::is_oracle_mode()) {
      if (is_valid_oracle_partition_data_type(type, is_check_value)) {
        //oracle模式下的hash分区允许字符类型和各种数字型、时间日期(ts with tz除外)
        bret = true;
      }
    } else {
      //对partition by range columns(c1) 中列的类型进行检查
      /**
       * All integer types: TINYINT, SMALLINT, MEDIUMINT, INT (INTEGER), and BIGINT. (This is the same as with partitioning by RANGE and LIST.)
       * Other numeric data types (such as DECIMAL or FLOAT) are not supported as partitioning columns.
       * DATE and DATETIME.
       * Columns using other data types relating to dates or times are not supported as partitioning columns.
       * The following string types: CHAR, VARCHAR, BINARY, and VARBINARY.
       * TEXT and BLOB columns are not supported as partitioning columns.
       */
      //https://dev.mysql.com/doc/refman/5.7/en/partitioning-columns.html
      ObObjTypeClass type_class = ob_obj_type_class(type);
      if (ObIntTC == type_class || ObUIntTC == type_class ||
        (ObDateTimeTC == type_class && ObTimestampType != type) || ObDateTC == type_class ||
        ObStringTC == type_class || ObYearTC == type_class || ObTimeTC == type_class) {
        bret = true;
      }else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type &&
                 GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_1 &&
                 (ObFloatTC == type_class || ObDoubleTC == type_class ||
                  ObDecimalIntTC == type_class || ObTimestampType == type)) {
        /*
          not compatible with MySql, relaied by size partition
        */
        bret = true;
      }
    }
  }
  return bret;
}

/* src_expr.type only can be int or number
   restype only can be (unsigned tiny/smallint/int/bigint)
*/
int ObResolverUtils::try_convert_to_unsiged(const ObExprResType restype,
                                            ObRawExpr&          src_expr,
                                            bool&               is_out_of_range)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx;
  const ObObj& src    = static_cast<sql::ObConstRawExpr*>(&src_expr)->get_value();
  ObObj& result = static_cast<sql::ObConstRawExpr*>(&src_expr)->get_value();

  is_out_of_range = true;

  if (OB_FAIL(ObObjCaster::to_type(restype.get_type(), cast_ctx, src, result))) {
    if (ret == OB_DATA_OUT_OF_RANGE) {
        ret = OB_SUCCESS;
    } else {
      LOG_WARN("to type fail", K(restype), K(src_expr), K(ret));
    }
  } else {
    is_out_of_range = false;
  }

  return ret;
}

/*
 * 参考mysql的实现
 * 对于 partition by range(expr) partitions p0 values less than(value_expr)
 * 如果part_expr 是int/uint类型, 那么期望 value_expr是 int/uint类型
 * 如果part_expr 是DateTime/Date/Time 那么期望value_expr是 string类型
 *  例如 create table ta (c1 datetime) partition by range columns (c1) (partition p0 values less than ('2011-1-1 10:10:00'));
 *  enum set bit is not allowed  type for range columns partition
 */
int ObResolverUtils::check_partition_range_value_result_type(const ObPartitionFuncType part_func_type,
                                                             const ObColumnRefRawExpr &part_column_expr,
                                                             ObRawExpr &part_value_expr)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("check_partition_range_value_result_type ", K(part_func_type), K(part_column_expr), K(part_value_expr));

  ObObjTypeClass expect_value_tc = ObMaxTC;
  ObObjType part_column_expr_type = part_column_expr.get_data_type();
  const ObObjMeta& part_col_meta_type = static_cast<const ObObjMeta&>(part_value_expr.get_result_type());
  ObObj& result = static_cast<sql::ObConstRawExpr*>(&part_value_expr)->get_value();
  if (OB_FAIL(deduce_expect_value_tc(part_column_expr_type,
                                     part_func_type,
                                     part_column_expr.get_collation_type(),
                                     part_column_expr.get_column_name(),
                                     expect_value_tc))) {
    LOG_WARN("failed to deduce expect value tc", K(ret), K(part_column_expr_type));
  } else if (OB_FAIL(check_part_value_result_type(part_func_type,
                                                  part_column_expr_type,
                                                  part_col_meta_type,
                                                  expect_value_tc,
                                                  part_value_expr.is_const_raw_expr(),
                                                  result))) {
    LOG_WARN("failed to check part value result type",
    K(ret), K(part_func_type), K(part_column_expr_type), K(part_col_meta_type), K(expect_value_tc));
  }
  return ret;
}

bool ObResolverUtils::is_expr_can_be_used_in_table_function(const ObRawExpr &expr)
{
  bool bret = false;
  if (expr.get_result_type().is_ext()) {
    // for UDF
    bret = true;
  } else if (T_FUN_SYS_GENERATOR == expr.get_expr_type()) {
    // for generator(N) stream function
    bret = true;
  }
  return bret;
}

int ObResolverUtils::check_partition_range_value_result_type(const ObPartitionFuncType part_func_type,
                                                             const ObExprResType &column_type,
                                                             const ObString &column_name,
                                                             ObObj &part_value)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("check_partition_range_value_result_type ", K(part_func_type), K(column_type), K(column_name));

  ObObjTypeClass expect_value_tc = ObMaxTC;
  const bool is_oracle_mode = lib::is_oracle_mode();
  ObObjType part_column_expr_type = column_type.get_type();
  const ObObjMeta& part_col_meta_type = part_value.get_meta();
  if (OB_FAIL(deduce_expect_value_tc(part_column_expr_type,
                                     part_func_type,
                                     column_type.get_collation_type(),
                                     column_name,
                                     expect_value_tc))) {
    LOG_WARN("failed to deduce expect value tc", K(ret), K(part_column_expr_type));
  } else if (OB_FAIL(check_part_value_result_type(part_func_type,
                                                  part_column_expr_type,
                                                  part_col_meta_type,
                                                  expect_value_tc,
                                                  true,
                                                  part_value))) {
    LOG_WARN("failed to check part value result type",
    K(ret), K(part_func_type), K(part_column_expr_type), K(part_col_meta_type), K(expect_value_tc));
  }
  return ret;
}

int ObResolverUtils::check_part_value_result_type(const ObPartitionFuncType part_func_type,
                                                  const ObObjType part_column_expr_type,
                                                  const ObObjMeta part_col_meta_type,
                                                  const ObObjTypeClass expect_value_tc,
                                                  bool is_const_expr,
                                                  ObObj &part_value)
{
  int ret = OB_SUCCESS;
  const bool is_oracle_mode = lib::is_oracle_mode();
  const ObObjType part_value_expr_type = part_col_meta_type.get_type();
  const ObObjTypeClass part_value_expr_tc = part_col_meta_type.get_type_class();
  if (part_value_expr_tc != expect_value_tc || ObTimestampLTZType == part_column_expr_type) {
    bool is_allow = false;
    if (ObNullTC == part_value_expr_tc && PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type) {
      is_allow = true;
    } else if (is_oracle_mode) {
      switch(part_column_expr_type) {
        case ObFloatType:
        case ObDoubleType:
        case ObNumberFloatType:
        case ObNumberType:
        case ObDecimalIntType: {
          //oracle模式的分区列number, 值int也ok
          is_allow = (ObStringTC == part_value_expr_tc || ObDecimalIntTC == part_value_expr_tc
                      || (ObIntTC <= part_value_expr_tc && part_value_expr_tc <= ObNumberTC));
          break;
        }
        case ObDateTimeType:
        case ObTimestampNanoType: {
          is_allow = (ObDateTimeType == part_value_expr_type
                      || ObTimestampNanoType == part_value_expr_type
                      || ObTimestampTZType == part_value_expr_type);
          break;
        }
        case ObTimestampLTZType: {
          is_allow = (ObTimestampTZType == part_value_expr_type);
          break;
        }
        case ObIntervalYMType: {
          is_allow = (ObStringTC == part_value_expr_tc);
          break;
        }
        case ObVarcharType:
        case ObCharType:
        case ObNVarchar2Type:
        case ObNCharType: {
          is_allow = true;
          break;
        }
        default: {
          break;
        }
      }
    } else {
      /* 处理mysql的date，datetime数据类型。

          create table t1_date(c1 date,c2 int) partition by range columns(c1) (partition p0 values less than (date '2020-10-10'));
          create table t1_date(c1 datetime,c2 int) partition by range columns(c1) (partition p0 values less than (time '10:10:00'));
          create table t1_date(c1 datetime,c2 int) partition by range columns(c1) (partition p0 values less than (date '2020-10-10'));
          create table t1_date(c1 datetime,c2 int) partition by range columns(c1) (partition p0 values less than (timestamp '2020-10-10 10:00:00'));
                */
      if (OB_SUCC(ret) && !is_allow) {
        if (part_value_expr_type == part_column_expr_type) {
          is_allow = true;
        } else if (ObDateTimeType == part_column_expr_type
                  && ( ObDateType == part_value_expr_type
                    || ObTimeType == part_value_expr_type)) {
          is_allow = true;
        } else if (ObTimestampType == part_column_expr_type) {
          is_allow = (ObDateTimeType == part_value_expr_type) ||
                     (ObDateType == part_value_expr_type) ||
                     (ObTimeType == part_value_expr_type);
        }
      }
      bool is_out_of_range = true;
      /* for mysql mode only (siged -> unsigned ) */
      if (is_const_expr
          && (part_value_expr_tc == ObIntTC || part_value_expr_tc == ObNumberTC)
          && expect_value_tc == ObUIntTC) {

        ObCastCtx cast_ctx;
        if (OB_FAIL(ObObjCaster::to_type(part_column_expr_type, cast_ctx, part_value, part_value))) {
          if (ret == OB_DATA_OUT_OF_RANGE) {
              ret = OB_SUCCESS;
          } else {
            LOG_WARN("to type fail", K(part_column_expr_type), K(ret));
          }
        } else {
          is_allow = true;
        }
      }
      if (ObFloatType == part_column_expr_type ||
          ObDoubleType == part_column_expr_type ||
          ObDecimalIntType == part_column_expr_type) {
          is_allow = (ObStringTC == part_value_expr_tc || ObDecimalIntTC == part_value_expr_tc
                      || (ObIntTC <= part_value_expr_tc && part_value_expr_tc <= ObNumberTC));
      }
    }
    if (OB_SUCC(ret) && !is_allow) {
      ret = (ObTimestampLTZType == part_column_expr_type ? OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR: OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);

      LOG_WARN("object type is invalid ", K(ret), K(expect_value_tc),
                K(part_value_expr_tc), K(part_column_expr_type), K(part_value_expr_type));
    }
  }
  return ret;
}

int ObResolverUtils::deduce_expect_value_tc(const ObObjType part_column_expr_type,
                                            const ObPartitionFuncType part_func_type,
                                            const ObCollationType coll_type,
                                            const ObString &column_name,
                                            ObObjTypeClass &expect_value_tc)
{
  int ret = OB_SUCCESS;
  bool need_cs_check = false; //表示是否需要字符集检测，not used now
  const bool is_oracle_mode = lib::is_oracle_mode();
  switch(part_column_expr_type) {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      expect_value_tc = ObIntTC;
      need_cs_check = false;
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      expect_value_tc = ObUIntTC;
      need_cs_check = false;
      break;
    }
    case ObDateTimeType:
    case ObDateType:
    case ObTimeType:
    case ObRawType:
    case ObTimestampLTZType:
    case ObTimestampNanoType: {
      expect_value_tc = ObStringTC;
      need_cs_check = true;
      break;
    }
    case ObTimestampType: {
      if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type) {
        expect_value_tc = ObStringTC;
        need_cs_check = true;
        break;
      }
    }
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType: {
      expect_value_tc = ObStringTC;
      need_cs_check = true;
      if (is_oracle_mode && CS_TYPE_UTF8MB4_GENERAL_CI == coll_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition key with CS_TYPE_UTF8MB4_GENERAL_CI collation");
        LOG_WARN("Partition key is string with CS_TYPE_UTF8MB4_GENERAL_CI", K(ret));
      }
      break;
    }
    case ObIntervalYMType:
    case ObIntervalDSType: {
      expect_value_tc = ob_obj_type_class(part_column_expr_type);
      break;
    }
    case ObFloatType:
    case ObDoubleType: {
      if (is_oracle_mode || (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type)) {
        expect_value_tc = ObStringTC;
        break;
      }
    }
    case ObNumberType:
    case ObNumberFloatType: {
      if (is_oracle_mode) {
        expect_value_tc = ObNumberTC;
        break;
      }
    }
    case ObDecimalIntType: {
      if (is_oracle_mode || (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type)) {
        expect_value_tc = ObDecimalIntTC;
        break;
      }
    }
    default: {
      ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
      LOG_USER_ERROR(OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
                    column_name.length(),
                    column_name.ptr());
      LOG_WARN("Field is not a allowed type for this type of partitioning", K(ret), K(part_column_expr_type));
    }
  }
  UNUSED(need_cs_check);
  return ret;
}

/**
 * hash/range 只能允许 partition by 中的列为int或uint类型，year类型mysql文档没写明，但是mysql5.6可以测试通过
 * 这里只用于partition by range(expr)/hash(expr) expr是column的表达式进行检查
 * range columns(column1, column2) 可以允许列的类型为integer, time,和 varchar等类型
 */
int ObResolverUtils::check_column_valid_for_partition(const ObRawExpr &part_expr,
                                                      const ObPartitionFuncType part_func_type,
                                                      const ObTableSchema &tbl_schema)
{
  int ret = OB_SUCCESS;
  if (part_expr.is_column_ref_expr()) {
    const ObColumnRefRawExpr &column_ref = static_cast<const ObColumnRefRawExpr&>(part_expr);
    const ObColumnSchemaV2 *col_schema = NULL;
    if (!is_valid_partition_column_type(column_ref.get_data_type(), part_func_type, false)) {
      ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
      LOG_USER_ERROR(OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
                     column_ref.get_column_name().length(),
                     column_ref.get_column_name().ptr());
      LOG_WARN("Field is not a allowed type for this type of partitioning", K(ret),
          "type", column_ref.get_data_type());
    } else if (OB_ISNULL(col_schema = tbl_schema.get_column_schema(column_ref.get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column", K(tbl_schema), K(column_ref.get_column_id()), K(ret));
    } else if (lib::is_oracle_mode() && col_schema->is_generated_column()) {
      ObSEArray<uint64_t, 5> cascaded_columns;
      if (OB_FAIL(col_schema->get_cascaded_column_ids(cascaded_columns))) {
        LOG_WARN("failed to get cascaded_column_ids", KPC(col_schema));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cascaded_columns.count(); ++i) {
        uint64_t column_id = cascaded_columns.at(i);
        const ObColumnSchemaV2 *cascaded_col_schema = tbl_schema.get_column_schema(column_id);
        if (OB_ISNULL(cascaded_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get column", K(tbl_schema), K(column_id), K(ret));
        } else if (!is_valid_oracle_partition_data_type(cascaded_col_schema->get_data_type(),
                                                        false)) {
          ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
          LOG_USER_ERROR(OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
                         cascaded_col_schema->get_column_name_str().length(),
                         cascaded_col_schema->get_column_name_str().ptr());
          LOG_WARN("Field is not a allowed type for this type of partitioning", K(ret),
          "type", cascaded_col_schema->get_data_type());
        }
      }
    }
  } else {
    if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type) {
      ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
      LOG_WARN("partition by range columns should be column_ref expr", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::check_partition_value_expr_for_range(const ObString &part_name,
                                                          const ObRawExpr &part_func_expr,
                                                          ObRawExpr &part_value_expr,
                                                          const ObPartitionFuncType part_type,
                                                          const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;

  //对value less than (xxx) 中expr的类型进行检查, 具体可以参考mysql的白名单
  if (OB_SUCC(ret)) {
    bool gen_col_check = false;
    bool accept_charset_function = in_tablegroup;
    ObRawExprPartFuncChecker part_func_checker(gen_col_check, accept_charset_function);
    if (OB_FAIL(part_value_expr.preorder_accept(part_func_checker))) {
      if (lib::is_oracle_mode() && OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED == ret) {
        ret = (share::schema::is_list_part(part_type)
               ? OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR
               : OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
      }
      LOG_WARN("check partition function failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (PARTITION_FUNC_TYPE_RANGE == part_type || PARTITION_FUNC_TYPE_LIST == part_type
        || PARTITION_FUNC_TYPE_INTERVAL == part_type) {
      ObObjType value_type = part_value_expr.get_data_type();
      if (lib::is_oracle_mode() && is_valid_oracle_partition_data_type(value_type, true)) {
        // oracle mode support int/numeric/datetime/timestamp/string/raw
      } else if (lib::is_mysql_mode() && ob_is_integer_type(value_type)) {
        // partition by range(xx) partition p0 values less than (expr) 中expr只允许integer类型
      } else if (ObNullTC == part_value_expr.get_type_class() && PARTITION_FUNC_TYPE_LIST == part_type) {
        //do nothing
      } else {
        ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
        LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                       part_name.length(), part_name.ptr());
        LOG_WARN("part_value_expr type is not correct", K(ret),
                 "data_type", part_value_expr.get_data_type(), K(lib::is_oracle_mode()), K(lib::is_mysql_mode()));
      }
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
      if (!part_func_expr.is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition by range columns() should be column expr", K(ret),
                 "type", part_func_expr.get_expr_type());
      } else {
        const ObColumnRefRawExpr &column_ref = static_cast<const ObColumnRefRawExpr &>(part_func_expr);
        if (OB_FAIL(check_partition_range_value_result_type(part_type, column_ref, part_value_expr))) {
            LOG_WARN("get partition range value result type failed", K(ret), K(part_name));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition type", K(ret), K(part_type));
    }
  }
  return ret;
}

int ObResolverUtils::check_partition_value_expr_for_range(const ObString &part_name,
                                                          ObRawExpr &part_value_expr,
                                                          const ObPartitionFuncType part_type,
                                                          const bool &in_tablegroup,
                                                          const bool interval_check)
{
  int ret = OB_SUCCESS;

  //对value less than (xxx) 中expr的类型进行检查, 具体可以参考mysql的白名单
  if (OB_SUCC(ret)) {
    bool gen_col_check = false;
    bool accept_charset_function = in_tablegroup;
    ObRawExprPartFuncChecker part_func_checker(gen_col_check, accept_charset_function, interval_check);
    if (OB_FAIL(part_value_expr.preorder_accept(part_func_checker))) {
      LOG_WARN("check partition function failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (PARTITION_FUNC_TYPE_RANGE == part_type || PARTITION_FUNC_TYPE_LIST == part_type
        || PARTITION_FUNC_TYPE_INTERVAL == part_type) {
      //partition by range(xx) partition p0 values less than (expr) 中expr只允许integer类型
      if (!ob_is_integer_type(part_value_expr.get_data_type())) {
        ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
        LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                       part_name.length(), part_name.ptr());
        LOG_WARN("part_value_expr type is not correct", K(ret),
                 "date_type", part_value_expr.get_data_type());
      }
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type
               || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
      const ObObjType value_type = part_value_expr.get_data_type();
      if (!is_valid_partition_column_type(value_type, part_type, true)) {
        ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
        LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
        LOG_WARN("object type is invalid ", K(ret), K(value_type));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition type", K(ret), K(part_type));
    }
  }
  return ret;
}

//用于oracle模式hash分区的有效性检查: 1), 可以是多key; 2), 数据类型限定; 3),char/varchar的字符集限定
int ObResolverUtils::check_partition_expr_for_oracle_hash(ObRawExpr &expr,
                                                          const ObPartitionFuncType part_type,
                                                          const ObTableSchema &tbl_schema,
                                                          const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  UNUSED(in_tablegroup);
  if (T_FUN_SYS_PART_HASH != expr.get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition func should be T_FUN_SYS_PART_HASH ",K(ret), K(expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    ObRawExpr *param_expr = expr.get_param_expr(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr should not be null", K(ret));
    } else if (OB_FAIL(check_column_valid_for_partition(*param_expr, part_type, tbl_schema))) {
      LOG_WARN("chck_valid_column_for_hash_func failed", K(ret));
    } else if (ObVarcharType == param_expr->get_data_type() || ObCharType == param_expr->get_data_type()
               || ob_is_nstring_type(param_expr->get_data_type())){
       if (CS_TYPE_UTF8MB4_GENERAL_CI == param_expr->get_collation_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Partition key with CS_TYPE_UTF8MB4_GENERAL_CI");
        LOG_WARN("Partition key is string with CS_TYPE_UTF8MB4_GENERAL_CI", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_expr_valid_for_partition(ObRawExpr &expr,
                                                    ObSQLSessionInfo &session_info,
                                                    const ObPartitionFuncType part_type,
                                                    const ObTableSchema &tbl_schema,
                                                    const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObRawExpr *part_expr = NULL;
  LOG_DEBUG("check_expr_valid_for_partition", K(ret), K(expr), K(part_type), K(in_tablegroup));

  if (is_hash_part(part_type)) {
    //因为partition by hash(xx) 这里的expr是hash(xx)函数，这里只检查xx
    if (expr.get_param_count() != 1 || T_FUN_SYS_PART_HASH != expr.get_expr_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param count of hash func should be 1", K(ret),
                "param count", expr.get_param_count(),
                "expr type", expr.get_expr_type());
    } else {
      part_expr = expr.get_param_expr(0);
    }
  } else if (is_key_part(part_type)) {
    if (expr.get_param_count() < 1 || T_FUN_SYS_PART_KEY != expr.get_expr_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error param count or expr func type", K(ret),
                "param count", expr.get_param_count(),
                "expr type", expr.get_expr_type());
    } else {
      // do nothing.
      part_expr = &expr;
    }
  } else if (is_range_part(part_type) || is_list_part(part_type) || is_key_part(part_type)) {
    //对于partition by range(xx) 这里的expr是xx
    part_expr = &expr;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition type is invalid", K(ret));
  }
  
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(part_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part expr should not be null", K(ret));
    } else if (is_key_part(part_type)) {
      // For key part, 
      // 1. check whether each ref column is valid.
      // 2. skip part func check and part expr check.
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
        ObRawExpr *param_expr = expr.get_param_expr(i);
        CK (OB_NOT_NULL(param_expr));
        OZ (ObResolverUtils::check_column_valid_for_partition(
          *param_expr, part_type, tbl_schema));
      }
    } else if (OB_FAIL(check_column_valid_for_partition(*part_expr, part_type, tbl_schema))) {
      LOG_WARN("chck_valid_column_for_hash_func failed", K(ret));
    } else {
      //对partition by hash(to_days(c1)) 对hash中允许的函数进行白名单的检查·
      //对parttiion by range(xxx) 中允许的函数进行白名单检查
      bool gen_col_check = false;
      bool accept_charset_function = in_tablegroup;
      ObRawExprPartFuncChecker part_func_checker(gen_col_check, accept_charset_function);
      if (OB_FAIL(part_expr->preorder_accept(part_func_checker))) {
        LOG_WARN("check partition function failed", K(ret));
      }
    }
    
    if (OB_FAIL(ret)) {
    } else if (is_key_part(part_type)) {
      // do not check part expr.
    } else {
      //对允许的函数的入参进行检查，例如partition by hash(to_days(c1)) c1 只能是date或者是datetime类型
      //partition by range(to_days(c1))同理
      ObRawExprPartExprChecker part_expr_checker(part_type);
      if (OB_FAIL(part_expr->formalize(&session_info))) {
        LOG_WARN("part expr formalize failed", K(ret));
      } else if (OB_FAIL(part_expr->preorder_accept(part_expr_checker))) {
        LOG_WARN("check_part_expr failed", K(ret), KPC(part_expr));
      }
    }
  }
  return ret;
}

int ObResolverUtils::log_err_msg_for_partition_value(const ObQualifiedName &name)
{
  int ret = OB_SUCCESS;
  if (!name.tbl_name_.empty() && !name.col_name_.empty()) {
    ret = OB_ERR_UNKNOWN_TABLE;
    if (!name.database_name_.empty()) {
      LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE,
                     name.tbl_name_.length(), name.tbl_name_.ptr(),
                     name.database_name_.length(), name.database_name_.ptr());
    } else {
      ObString scope_name("partition function");
      LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE,
                     name.tbl_name_.length(), name.tbl_name_.ptr(),
                     scope_name.length(), scope_name.ptr());
    }
    LOG_WARN("unknown table in partition function", K(name));
  } else if (!name.col_name_.empty()) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    ObString scope_name("partition function");
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                   name.col_name_.length(), name.col_name_.ptr(),
                   scope_name.length(), scope_name.ptr());
  } else if (!name.access_idents_.empty()) {
    const ObString &func_name = name.access_idents_.at(name.access_idents_.count() - 1).access_name_;
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("Invalid function name in partition function", K(name.access_idents_.count()), K(ret), K(func_name));
    LOG_USER_ERROR(OB_ERR_FUNCTION_UNKNOWN, "FUNCTION", func_name.length(), func_name.ptr());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name is invalid", K(name), K(ret));
  }
  return ret;
}

int ObResolverUtils::resolve_partition_list_value_expr(ObResolverParams &params,
                                                       const ParseNode &node,
                                                       const ObString &part_name,
                                                       const ObPartitionFuncType part_type,
                                                       const ObIArray<ObRawExpr *> &part_func_exprs,
                                                       ObIArray<ObRawExpr *> &part_value_expr_array,
                                                       const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (node.type_ == T_EXPR_LIST) {
    if (node.num_child_ != part_func_exprs.count()) {
      ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
      LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret),
               "node_child_num", node.num_child_, "part_func_expr", part_func_exprs.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; i ++) {
      ObRawExpr *part_value_expr = NULL;
      ObRawExpr *part_func_expr = NULL;
      if (OB_FAIL(part_func_exprs.at(i, part_func_expr))) {
        LOG_WARN("get part expr failed", K(i), "size", part_func_exprs.count(), K(ret));
      } else if (OB_ISNULL(part_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_func_expr is invalid", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params,
                                                                             *(node.children_[i]),
                                                                             part_name,
                                                                             part_type,
                                                                             *part_func_expr,
                                                                             part_value_expr,
                                                                             in_tablegroup))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
        LOG_WARN("array push back fail", K(ret));
      } else { }
    }
  } else {
    ObRawExpr *part_value_expr = NULL;
    ObRawExpr *part_func_expr = NULL;
    if (OB_FAIL(part_func_exprs.at(0, part_func_expr))) {
      LOG_WARN("get part expr failed", "size", part_func_exprs.count(), K(ret));
    } else if (OB_ISNULL(part_func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_func_expr is invalid", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params,
                                                                           node,
                                                                           part_name,
                                                                           part_type,
                                                                           *part_func_expr,
                                                                           part_value_expr,
                                                                           in_tablegroup))) {
      LOG_WARN("resolve partition expr failed", K(ret));
    } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
      LOG_WARN("array push back fail", K(ret));
    } else { }
  }
  return ret;
}

//这个函数需要自己写，其他的函数可以重用。
//这个函数要分析
int ObResolverUtils::resolve_partition_range_value_expr(ObResolverParams &params,
                                                        const ParseNode &node,
                                                        const ObString &part_name,
                                                        const ObPartitionFuncType part_type,
                                                        const ObRawExpr &part_func_expr,
                                                        ObRawExpr *&part_value_expr,
                                                        const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (part_name.empty()
      && !lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part name is invalid", K(ret), K(part_name));
  } else if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObArray<ObQualifiedName> columns;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObArray<ObUDFInfo> udf_info;
    ObArray<ObColumnRefRawExpr*> part_column_refs;
    ObArray<ObOpRawExpr*> op_exprs;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    ObArray<ObInListInfo> inlist_infos;
    ObSEArray<ObMatchFunRawExpr*, 1> match_exprs;
    ObExprResolveContext ctx(*params.expr_factory_, params.session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = ObCharset::charset_type_by_coll(part_func_expr.get_collation_type());
    ctx.param_list_ = params.param_list_;
    ctx.session_info_ = params.session_info_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node, part_value_expr, columns, sys_vars,
                                             sub_query_info, aggr_exprs, win_exprs, udf_info,
                                             op_exprs, user_var_exprs, inlist_infos, match_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;
    } else if (OB_FAIL(resolve_columns_for_partition_range_value_expr(part_value_expr, columns))) {
      LOG_WARN("resolve columns failed", K(ret));
    } else if (udf_info.count() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "udf");
    } else if (OB_UNLIKELY(!inlist_infos.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in_expr should not been found here", K(ret));
    } else if (OB_UNLIKELY(match_exprs.count() > 0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "fulltext search func");
    } else { /* do nothing */ }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_value_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part expr should not be null", K(ret));
      } else if (OB_FAIL(part_value_expr->formalize(params.session_info_))) {
        LOG_WARN("formailize expr failed", K(ret));
      } else if (OB_FAIL(check_partition_value_expr_for_range(part_name,
                                                              part_func_expr,
                                                              *part_value_expr,
                                                              part_type,
                                                              in_tablegroup))) {
        LOG_WARN("check_valid_column_for_hash or range func failed",
                 K(part_type), K(part_name), K(ret));
      } else {
        LOG_DEBUG("succ to check_partition_value_expr_for_range",
                 K(part_type), K(part_name), K(part_func_expr), KPC(part_value_expr), K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_partition_list_value_expr(ObResolverParams &params,
                                                       const ParseNode &node,
                                                       const ObString &part_name,
                                                       const ObPartitionFuncType part_type,
                                                       int64_t &expr_num,
                                                       ObIArray<ObRawExpr *> &part_value_expr_array,
                                                       const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (node.type_ == T_EXPR_LIST) {
    if (OB_INVALID_COUNT != expr_num
        && node.num_child_ != expr_num) {
      ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
      LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret), K(expr_num), "child_num", node.num_child_);
    } else {
      expr_num = node.num_child_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; i++) {
      ObRawExpr *part_value_expr = NULL;
      if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params,
                                                                      *(node.children_[i]),
                                                                      part_name,
                                                                      part_type,
                                                                      part_value_expr,
                                                                      in_tablegroup))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
        LOG_WARN("array push back fail", K(ret));
      }
    }
  } else {
    expr_num = 1;
    ObRawExpr *part_value_expr = NULL;
    if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params,
                                                                    node,
                                                                    part_name,
                                                                    part_type,
                                                                    part_value_expr,
                                                                    in_tablegroup))) {
      LOG_WARN("resolve partition expr failed", K(ret));
    } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
      LOG_WARN("array push back fail", K(ret));
    } else { }
  }
  return ret;
}

//for recursively process columns item in resolve_partition_range_value_expr
//just wrap columns process logic in resolve_partition_range_value_expr
int ObResolverUtils::resolve_columns_for_partition_range_value_expr(ObRawExpr *&expr,
                                                                    ObArray<ObQualifiedName> &columns)
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<ObRawExpr *, ObRawExpr *>> real_sys_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ObRawExpr *real_ref_expr = q_name.access_idents_.at(0).sys_func_expr_;
      for (int64_t j = 0; OB_SUCC(ret) && j < real_sys_exprs.count(); ++j) {
        if (OB_FAIL(ObRawExprUtils::replace_ref_column(real_ref_expr,
                                                       real_sys_exprs.at(j).first,
                                                       real_sys_exprs.at(j).second))) {
          LOG_WARN("failed to replace ref column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(q_name.access_idents_.at(0).check_param_num())) {
        LOG_WARN("faield to check param num", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr,
                                                            q_name.ref_expr_,
                                                            real_ref_expr))) {
        LOG_WARN("failed to replace ref column", K(ret));
      } else if (OB_FAIL(real_sys_exprs.push_back(
                  std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, real_ref_expr)))) {
        LOG_WARN("failed to push back pari exprs", K(ret));
      }
    } else {
      if (OB_FAIL(log_err_msg_for_partition_value(q_name))) {
        LOG_WARN("log error msg for range value expr faield", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_partition_range_value_expr(ObResolverParams &params,
                                                        const ParseNode &node,
                                                        const ObString &part_name,
                                                        const ObPartitionFuncType part_type,
                                                        ObRawExpr *&part_value_expr,
                                                        const bool &in_tablegroup,
                                                        const bool interval_check)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (part_name.empty()
      && !lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part name is invalid", K(ret), K(part_name));
  } else if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObArray<ObQualifiedName> columns;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObArray<ObUDFInfo> udf_info;
    ObArray<ObColumnRefRawExpr*> part_column_refs;
    ObArray<ObOpRawExpr*> op_exprs;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    ObArray<ObInListInfo> inlist_infos;
    ObSEArray<ObMatchFunRawExpr*, 1> match_exprs;
    ObExprResolveContext ctx(*params.expr_factory_,
                             params.session_info_->get_timezone_info(),
                             OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params.param_list_;
    ctx.session_info_ = params.session_info_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node,
                                             part_value_expr,
                                             columns,
                                             sys_vars,
                                             sub_query_info,
                                             aggr_exprs,
                                             win_exprs,
                                             udf_info,
                                             op_exprs,
                                             user_var_exprs,
                                             inlist_infos,
                                             match_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;

    } else if (OB_FAIL(resolve_columns_for_partition_range_value_expr(part_value_expr, columns))) {
      LOG_WARN("resolve columns failed", K(ret));
    } else if (OB_UNLIKELY(udf_info.count() > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("UDFInfo should not found be here!!!", K(ret));
    } else if (OB_UNLIKELY(!inlist_infos.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in_expr should not been found here", K(ret));
    } else if (OB_UNLIKELY(match_exprs.count() > 0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "fulltext search func");
    } else {/* do nothing */}

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_value_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part expr should not be null", K(ret));
      } else if (OB_FAIL(part_value_expr->formalize(params.session_info_))) {
        LOG_WARN("formailize expr failed", K(ret));
      } else if (OB_FAIL(check_partition_value_expr_for_range(part_name,
                                                              *part_value_expr,
                                                              part_type,
                                                              in_tablegroup,
                                                              interval_check))) {
        LOG_WARN("check_valid_column_for_hash or range func failed",
                 K(part_type), K(part_name), K(ret));
      }
    }
  }
  return ret;
}

//for recursively process columns item in resolve_partition_expr
//just wrap columns process logic in resolve_partition_expr
int ObResolverUtils::resolve_columns_for_partition_expr(ObRawExpr *&expr,
                                                        ObIArray<ObQualifiedName> &columns,
                                                        const ObTableSchema &tbl_schema,
                                                        ObPartitionFuncType part_func_type,
                                                        int64_t partition_key_start,
                                                        ObIArray<ObString> &partition_keys)
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<ObRawExpr*, ObRawExpr*>> real_sys_exprs;
  ObArray<ObColumnRefRawExpr*> part_column_refs;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    const ObQualifiedName &q_name = columns.at(i);
    ObRawExpr *real_ref_expr = NULL;
    if (q_name.is_sys_func()) {
      if (OB_SUCC(ret)) {
        real_ref_expr = q_name.access_idents_.at(0).sys_func_expr_;
        for (int64_t i = 0; OB_SUCC(ret) && i < real_sys_exprs.count(); ++i) {
          OZ (ObRawExprUtils::replace_ref_column(real_ref_expr, real_sys_exprs.at(i).first, real_sys_exprs.at(i).second));
        }

        OZ (q_name.access_idents_.at(0).check_param_num());
        OZ (ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr));
        OZ (real_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, real_ref_expr)));
      }
    } else if (q_name.is_pl_udf() || q_name.is_pl_var()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("pl variable is not invalid for partition", K(ret));
    } else if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "partition function";
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    } else if (OB_ISNULL(q_name.ref_expr_) || OB_UNLIKELY(!q_name.ref_expr_->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref expr is null", K_(q_name.ref_expr));
    } else {
      //check column name whether duplicated, if partition by key, duplicated column means error
      //if partition by hash, add partition keys without duplicated name
      const ObColumnSchemaV2 *col_schema = NULL;
      bool is_duplicated = false;
      int64_t j = partition_key_start;
      ObColumnRefRawExpr *col_expr = q_name.ref_expr_;
      for (j = partition_key_start; OB_SUCC(ret) && j < partition_keys.count(); ++j) {
        common::ObString &temp_name = partition_keys.at(j);
        if (ObCharset::case_insensitive_equal(temp_name, q_name.col_name_)) {
          is_duplicated = true;
          break;
        }
      }
      if (!is_duplicated) {
        if (NULL == (col_schema = tbl_schema.get_column_schema(q_name.col_name_)) || col_schema->is_hidden()) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          ObString scope_name = "partition function";
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, q_name.col_name_.length(), q_name.col_name_.ptr(),
                         scope_name.length(), scope_name.ptr());
        } else if (OB_FAIL(partition_keys.push_back(q_name.col_name_))) {
          LOG_WARN("add column name failed", K(ret), K_(q_name.col_name));
        } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *col_expr))) {
          LOG_WARN("init column expr failed", K(ret));
        } else if (OB_FAIL(part_column_refs.push_back(col_expr))) {
          LOG_WARN("push back column expr failed", K(ret));
        } else { /*do nothing*/ }
      } else {
        // for partition by key, duplicated column is forbidden
        // for partition by hash, duplicated column must use the same column ref expr
        ObColumnRefRawExpr *ref_expr = NULL;
        if (PARTITION_FUNC_TYPE_KEY == part_func_type) {
          ret = OB_ERR_SAME_NAME_PARTITION_FIELD;
          LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION_FIELD, q_name.col_name_.length(), q_name.col_name_.ptr());
        } else if (lib::is_oracle_mode() && PARTITION_FUNC_TYPE_HASH == part_func_type) {
          ret = OB_ERR_FIELD_SPECIFIED_TWICE;
        } else if (OB_FAIL(part_column_refs.at(j - partition_key_start, ref_expr))) {
          LOG_WARN("Failed to get part expr", K(ret),
                   "idx", j - partition_key_start, "count", part_column_refs.count());
        } else if (OB_ISNULL(ref_expr)
                   || OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Part expr should not be NULL", K(ret), KPC(ref_expr), KPC(col_expr));
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, col_expr, ref_expr))) {
          LOG_WARN("replace column ref failed", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_partition_expr(ObResolverParams &params,
                                            const ParseNode &node,
                                            const ObTableSchema &tbl_schema,
                                            ObPartitionFuncType part_func_type,
                                            ObRawExpr *&part_expr,
                                            ObIArray<ObString> *part_keys)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObString> tmp_part_keys;
  ObArray<ObOpRawExpr*> op_exprs;
  ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
  ObArray<ObInListInfo> inlist_infos;
  ObSEArray<ObMatchFunRawExpr*, 1> match_exprs;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  //part_keys is not null, means that need output partition keys
  ObIArray<ObString> &partition_keys = (part_keys != NULL ? *part_keys : tmp_part_keys);
  int64_t partition_key_start = partition_keys.count();
  if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObExprResolveContext ctx(*params.expr_factory_, params.session_info_->get_timezone_info(),
                             OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params.param_list_;
    ctx.session_info_ = params.session_info_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node, part_expr, columns, sys_vars,
                                             sub_query_info, aggr_exprs, win_exprs, udf_info,
                                             op_exprs, user_var_exprs, inlist_infos, match_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;

    } else if (columns.size() <= 0) {
      //处理partition中为常量表达式的情况 partition by hash(1+1+1) /partition by range (1+1+1)
      //用于限制 partition by hash(1)
      ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
      LOG_WARN("const expr is invalid for thie type of partitioning", K(ret));
    } else if (udf_info.count() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "udf");
    } else if (OB_UNLIKELY(!inlist_infos.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in_expr should not been found here", K(ret));
    } else if (OB_UNLIKELY(match_exprs.count() > 0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "fulltext search func");
    } else if (OB_FAIL(resolve_columns_for_partition_expr(part_expr, columns, tbl_schema, part_func_type,
                       partition_key_start, partition_keys))) {
      LOG_WARN("resolve columns for partition expr failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part expr should not be null", K(ret));
      } else if (lib::is_oracle_mode() && is_hash_part(part_func_type)) {
        if (OB_FAIL(check_partition_expr_for_oracle_hash(*part_expr, part_func_type, tbl_schema))) {
          LOG_WARN("check_partition_expr_for_oracle_hash func failed", K(ret));
        }
      } else if (is_hash_part(part_func_type) || is_range_part(part_func_type) 
              || is_list_part(part_func_type) || PARTITION_FUNC_TYPE_KEY == part_func_type) {
        if (OB_FAIL(check_expr_valid_for_partition(
            *part_expr, *params.session_info_, part_func_type, tbl_schema))) {
          LOG_WARN("check_valid_column_for_hash or range func failed", K(ret), KPC(part_expr));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(part_expr->formalize(params.session_info_))) {
      LOG_WARN("formailize expr failed", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::resolve_generated_column_expr(ObResolverParams &params,
                                                   const ObString &expr_str,
                                                   ObTableSchema &tbl_schema,
                                                   ObColumnSchemaV2 &generated_column,
                                                   ObRawExpr *&expr,
                                                   const PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  const ParseNode *expr_node = NULL;
  if (OB_ISNULL(params.allocator_) || OB_ISNULL(params.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(
                expr_str, params.session_info_->get_charsets4parser(),
                *params.allocator_, expr_node))) {
    LOG_WARN("parse expr node from str failed", K(ret), K(expr_str));
  } else if (OB_FAIL(resolve_generated_column_expr(params, expr_node, tbl_schema,
                                                   generated_column, expr,
                                                   check_status))) {
    LOG_WARN("resolve generated column expr failed", K(ret), K(expr_str));
  }
  return ret;
}

int ObResolverUtils::resolve_generated_column_expr(ObResolverParams &params,
                                                   const ParseNode *node,
                                                   ObTableSchema &tbl_schema,
                                                   ObColumnSchemaV2 &generated_column,
                                                   ObRawExpr *&expr,
                                                   const PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnSchemaV2 *> dummy_array;
  OZ (resolve_generated_column_expr(params, node, tbl_schema, dummy_array,
                                    generated_column, expr, check_status));
  return ret;
}

int ObResolverUtils::resolve_generated_column_expr(ObResolverParams &params,
                                                   const ObString &expr_str,
                                                   ObTableSchema &tbl_schema,
                                                   ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                                   ObColumnSchemaV2 &generated_column,
                                                   ObRawExpr *&expr,
                                                   const PureFunctionCheckStatus check_status,
                                                   bool coltype_not_defined)
{
  int ret = OB_SUCCESS;
  const ParseNode *expr_node = NULL;
  if (OB_ISNULL(params.allocator_) || OB_ISNULL(params.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator or session is null", K(ret),
             KP(params.allocator_), KP(params.session_info_));
  } else if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(
                      expr_str, params.session_info_->get_charsets4parser(),
                      *params.allocator_, expr_node))) {
    LOG_WARN("parse expr node from str failed", K(ret), K(expr_str));
  } else if (OB_FAIL(resolve_generated_column_expr(params,
                                                   expr_node,
                                                   tbl_schema,
                                                   resolved_cols,
                                                   generated_column,
                                                   expr,
                                                   check_status,
                                                   coltype_not_defined))) {
    LOG_WARN("resolve generated column expr failed", K(ret), K(expr_str));
  }
  return ret;
}

int ObResolverUtils::calc_file_column_idx(const ObString &column_name, uint64_t &file_column_idx)
{
  int ret = OB_SUCCESS;
  if (column_name.case_compare(N_EXTERNAL_FILE_URL) == 0) {
    file_column_idx = UINT64_MAX;
  } else {
    int32_t PREFIX_LEN = column_name.prefix_match_ci(N_PARTITION_LIST_COL) ?
                            str_length(N_PARTITION_LIST_COL) :
                              column_name.prefix_match_ci(N_EXTERNAL_FILE_COLUMN_PREFIX)?
                                str_length(N_EXTERNAL_FILE_COLUMN_PREFIX) : -1;
    if (column_name.length() <= PREFIX_LEN) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      int err = 0;
      file_column_idx = ObCharset::strntoull(column_name.ptr() + PREFIX_LEN, column_name.length() - PREFIX_LEN, 10, &err);
      if (err != 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid file column name", K(column_name));
      }
    }
  }
  return ret;
}

ObRawExpr *ObResolverUtils::find_file_column_expr(ObIArray<ObRawExpr *> &pseudo_exprs,
                                        int64_t table_id,
                                        int64_t column_idx,
                                        const ObString &expr_name)
{
  ObRawExpr *expr = nullptr;
  for (int i = 0; i < pseudo_exprs.count(); ++i) {
    ObPseudoColumnRawExpr *pseudo_expr = static_cast<ObPseudoColumnRawExpr *>(pseudo_exprs.at(i));
    if (pseudo_expr->get_table_id() == table_id
        && pseudo_expr->get_extra() == column_idx
        && pseudo_expr->get_expr_name().prefix_match_ci(expr_name)) {
      expr = pseudo_expr;
      break;
    }
  }
  return expr;
}

int ObResolverUtils::resolve_external_table_column_def(ObRawExprFactory &expr_factory,
                                                       const ObSQLSessionInfo &session_info,
                                                       const ObQualifiedName &q_name,
                                                       ObIArray<ObRawExpr *> &real_exprs,
                                                       ObRawExpr *&expr,
                                                       const ObColumnSchemaV2 *gen_col_schema)
{
  int ret = OB_SUCCESS;
  ObRawExpr *file_column_expr = nullptr;
  uint64_t file_column_idx = UINT64_MAX;
  if (!ObResolverUtils::is_external_pseudo_column_name(q_name.col_name_)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    ObString scope_name = "external file column";
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, q_name.col_name_.length(), q_name.col_name_.ptr(),
                   scope_name.length(), scope_name.ptr());
  } else {
    if (0 == q_name.col_name_.case_compare(N_EXTERNAL_FILE_URL)) {
      if (nullptr == (file_column_expr = ObResolverUtils::find_file_column_expr(
                               real_exprs, OB_INVALID_ID, UINT64_MAX, q_name.col_name_))) {
        if (OB_FAIL(ObResolverUtils::build_file_column_expr_for_file_url(expr_factory, session_info,
                                    OB_INVALID_ID, ObString(), q_name.col_name_, file_column_expr))) {
          LOG_WARN("fail to build external table file column expr", K(ret));
        }
      }
    } else if (q_name.col_name_.prefix_match_ci(N_PARTITION_LIST_COL)) {
      if (OB_FAIL(ObResolverUtils::calc_file_column_idx(q_name.col_name_, file_column_idx))) {
        LOG_WARN("fail to calc file column idx", K(ret));
      } else if (nullptr == (file_column_expr = ObResolverUtils::find_file_column_expr(
                               real_exprs, OB_INVALID_ID, file_column_idx, q_name.col_name_))) {
        if (OB_FAIL(ObResolverUtils::build_file_column_expr_for_partition_list_col(expr_factory,
                                            session_info, OB_INVALID_ID, ObString(),
                                            q_name.col_name_, file_column_idx, file_column_expr, gen_col_schema))) {
          LOG_WARN("fail to build external table file column expr", K(ret));
        }
      }
    } else if (ObExternalFileFormat::CSV_FORMAT == ObResolverUtils::resolve_external_file_column_type(q_name.col_name_)) {
      if (OB_FAIL(ObResolverUtils::calc_file_column_idx(q_name.col_name_, file_column_idx))) {
        LOG_WARN("fail to calc file column idx", K(ret));
      } else if (nullptr == (file_column_expr = ObResolverUtils::find_file_column_expr(
                               real_exprs, OB_INVALID_ID, file_column_idx, q_name.col_name_))) {
        ObExternalFileFormat temp_format;
        temp_format.csv_format_.init_format(ObDataInFileStruct(), 0, CS_TYPE_UTF8MB4_BIN);
        if (OB_FAIL(ObResolverUtils::build_file_column_expr_for_csv(expr_factory, session_info,
                    OB_INVALID_ID, ObString(), q_name.col_name_, file_column_idx, file_column_expr, temp_format))) {
          LOG_WARN("fail to build external table file column expr", K(ret));
        }
      }
    } else {
      if (OB_FAIL(ObResolverUtils::build_file_row_expr_for_parquet(expr_factory, session_info,
                                                                   OB_INVALID_ID, ObString(),
                                                                   q_name.col_name_, file_column_expr))) {
        LOG_WARN("fail to build file column expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(real_exprs.push_back(file_column_expr))) {
        LOG_WARN("fail to push back expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::replace_expr(q_name.ref_expr_, file_column_expr, expr))) {
      LOG_WARN("fail replace expr", K(ret));
    }
  }
  LOG_TRACE("resolve external table column ref", K(q_name.col_name_), KP(expr), KPC(expr));
  return ret;
}

bool ObResolverUtils::is_external_pseudo_column_name(const ObString &name)
{
  return is_external_file_column_name(name)
        || 0 == name.case_compare(N_EXTERNAL_FILE_URL)
        || name.prefix_match_ci(N_PARTITION_LIST_COL);
}

bool ObResolverUtils::is_external_file_column_name(const ObString &name)
{
  ObExternalFileFormat::FormatType type = resolve_external_file_column_type(name);
  return (type > ObExternalFileFormat::INVALID_FORMAT && type < ObExternalFileFormat::MAX_FORMAT);
}

ObExternalFileFormat::FormatType ObResolverUtils::resolve_external_file_column_type(const ObString &name)
{
  ObExternalFileFormat::FormatType type = ObExternalFileFormat::INVALID_FORMAT;
  if (name.prefix_match_ci(N_EXTERNAL_FILE_COLUMN_PREFIX)) {
    type = ObExternalFileFormat::CSV_FORMAT;
  } else if (0 == name.case_compare(N_EXTERNAL_FILE_ROW)) {
    type = ObExternalFileFormat::PARQUET_FORMAT;
  }
  return type;
}

int ObResolverUtils::build_file_column_expr_for_parquet(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    const uint64_t table_id,
    const ObString &table_name,
    const ObString &column_name,
    ObRawExpr *get_path_expr,
    ObRawExpr *cast_expr,
    const ObColumnSchemaV2 *generated_column,
    ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObPseudoColumnRawExpr *file_column_expr = nullptr;
  ObRawExpr *path_expr = nullptr;

  if (OB_FAIL(expr_factory.create_raw_expr(T_PSEUDO_EXTERNAL_FILE_COL, file_column_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_ISNULL(file_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    file_column_expr->set_expr_name(column_name);
    file_column_expr->set_table_name(table_name);
    file_column_expr->set_table_id(table_id);
    file_column_expr->set_explicited_reference();

    if (OB_ISNULL(get_path_expr) || OB_ISNULL(path_expr = get_path_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      //get type
      if (OB_NOT_NULL(cast_expr)) {
        bool enable_decimalint = false;
        ObExprResType dst_type;
        ObConstRawExpr *const_cast_type_expr = static_cast<ObConstRawExpr *>(cast_expr->get_param_expr(1));
        if (!const_cast_type_expr->get_value().is_int()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support non-const expr", K(ret), KPC(const_cast_type_expr));
        } else if (OB_FAIL(const_cast_type_expr->formalize(&session_info))) {
          LOG_WARN("fail to formalize expr", K(ret));
        } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(&session_info, enable_decimalint))) {
          LOG_WARN("fail to check_enable_decimalint", K(ret));
        } else if (OB_FAIL(ObExprCast::get_cast_type(enable_decimalint,
                                                     const_cast_type_expr->get_result_type(),
                                                     cast_expr->get_extra(), dst_type))) {
          LOG_WARN("get cast dest type failed", K(ret));
        } else {
          if (dst_type.is_string_or_lob_locator_type()) {
            // string data stored in parquet file as UTF8 format
            dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
          file_column_expr->set_result_type(dst_type);
        }
      } else if (OB_NOT_NULL(generated_column)) {
        ObColumnRefRawExpr *column_expr = nullptr;
        if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *generated_column, column_expr))) {
          LOG_WARN("failed to build column expr", K(ret));
        } else {
          file_column_expr->set_accuracy(column_expr->get_accuracy());
          file_column_expr->set_data_type(column_expr->get_data_type());
          file_column_expr->set_collation_type(column_expr->get_collation_type());
          file_column_expr->set_collation_level(column_expr->get_collation_level());
          if (column_expr->get_result_type().is_string_or_lob_locator_type()
              && ObCharset::charset_type_by_coll(column_expr->get_collation_type()) != CHARSET_UTF8MB4) {
            // string data stored in parquet file as UTF8 format
            file_column_expr->set_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      //get path
      if (OB_FAIL(path_expr->formalize(&session_info))) {
        LOG_WARN("fail to formalize expr", K(ret));
      } else if (!path_expr->is_static_const_expr()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support non-const expr", K(ret), KPC(path_expr));
      } else {
        ObConstRawExpr *const_path_expr = static_cast<ObConstRawExpr *>(path_expr);
        if (!const_path_expr->get_value().is_string_type()) {
          ret = OB_NOT_SUPPORTED;
        } else {
          file_column_expr->set_data_access_path(const_path_expr->get_value().get_string());
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(file_column_expr->formalize(&session_info))) {
      LOG_WARN("failed to extract info", K(ret));
    } else {
      expr = file_column_expr;
    }
  }

  return ret;
}

int ObResolverUtils::build_file_row_expr_for_parquet(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    const uint64_t table_id,
    const ObString &table_name,
    const ObString &column_name,
    ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObPseudoColumnRawExpr *file_column_expr = nullptr;

  if (OB_FAIL(expr_factory.create_raw_expr(T_PSEUDO_EXTERNAL_FILE_ROW, file_column_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_ISNULL(file_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    file_column_expr->set_expr_name(column_name);
    file_column_expr->set_table_name(table_name);
    file_column_expr->set_table_id(table_id);
    file_column_expr->set_explicited_reference();
    file_column_expr->set_data_type(ObVarcharType);
    file_column_expr->set_collation_type(CS_TYPE_BINARY);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(file_column_expr->formalize(&session_info))) {
      LOG_WARN("failed to extract info", K(ret));
    } else {
      expr = file_column_expr;
    }
  }

  return ret;
}

int ObResolverUtils::build_file_column_expr_for_csv(ObRawExprFactory &expr_factory,
                                                    const ObSQLSessionInfo &session_info,
                                                    const uint64_t table_id,
                                                    const ObString &table_name,
                                                    const ObString &column_name,
                                                    int64_t column_idx,
                                                    ObRawExpr *&expr,
                                                    const ObExternalFileFormat &format)
{
  int ret = OB_SUCCESS;
  ObPseudoColumnRawExpr *file_column_expr = nullptr;
  ObItemType type = T_PSEUDO_EXTERNAL_FILE_COL;
  uint64_t extra = column_idx;

  if (OB_FAIL(expr_factory.create_raw_expr(type, file_column_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_ISNULL(file_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    file_column_expr->set_expr_name(column_name);
    file_column_expr->set_table_name(table_name);
    file_column_expr->set_table_id(table_id);
    file_column_expr->set_explicited_reference();
    file_column_expr->set_extra(extra);

    file_column_expr->set_data_type(ObVarcharType);
    file_column_expr->set_collation_type(ObCharset::get_default_collation(format.csv_format_.cs_type_));
    file_column_expr->set_collation_level(CS_LEVEL_IMPLICIT);
    file_column_expr->set_length(OB_MAX_VARCHAR_LENGTH);
    if (lib::is_oracle_mode()) {
      file_column_expr->set_length_semantics(LS_BYTE);
    }
    if (OB_FAIL(file_column_expr->formalize(&session_info))) {
      LOG_WARN("failed to extract info", K(ret));
    } else {
      expr = file_column_expr;
    }
  }

  return ret;
}

int ObResolverUtils::build_file_column_expr_for_partition_list_col(
  ObRawExprFactory &expr_factory,
  const ObSQLSessionInfo &session_info,
  const uint64_t table_id,
  const ObString &table_name,
  const ObString &column_name,
  int64_t column_idx,
  ObRawExpr *&expr,
  const ObColumnSchemaV2 *generated_column)
{
  int ret = OB_SUCCESS;
  ObPseudoColumnRawExpr *file_column_expr = nullptr;
  ObColumnRefRawExpr *column_expr = nullptr;
  ObItemType type = T_PSEUDO_PARTITION_LIST_COL;
  uint64_t extra = column_idx;

  if (OB_FAIL(expr_factory.create_raw_expr(type, file_column_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_ISNULL(file_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_ISNULL(generated_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gen column schema is null", K(ret));
  } else {
    file_column_expr->set_expr_name(column_name);
    file_column_expr->set_table_name(table_name);
    file_column_expr->set_table_id(table_id);
    file_column_expr->set_explicited_reference();
    file_column_expr->set_extra(extra);

    if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *generated_column, column_expr))) {
      LOG_WARN("failed to build column expr", K(ret));
    } else {
      file_column_expr->set_accuracy(column_expr->get_accuracy());
      file_column_expr->set_data_type(column_expr->get_data_type());
      file_column_expr->set_collation_type(column_expr->get_collation_type());
      file_column_expr->set_collation_level(column_expr->get_collation_level());
      if (OB_FAIL(file_column_expr->formalize(&session_info))) {
        LOG_WARN("failed to extract info", K(ret));
      } else {
        expr = file_column_expr;
      }
    }
  }

  return ret;
}

int ObResolverUtils::build_file_column_expr_for_file_url(
  ObRawExprFactory &expr_factory,
  const ObSQLSessionInfo &session_info,
  const uint64_t table_id,
  const ObString &table_name,
  const ObString &column_name,
  ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObPseudoColumnRawExpr *file_column_expr = nullptr;
  ObItemType type = T_PSEUDO_EXTERNAL_FILE_URL;
  uint64_t extra = UINT64_MAX;

  if (OB_FAIL(expr_factory.create_raw_expr(type, file_column_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_ISNULL(file_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    file_column_expr->set_expr_name(column_name);
    file_column_expr->set_table_name(table_name);
    file_column_expr->set_table_id(table_id);
    file_column_expr->set_explicited_reference();
    file_column_expr->set_extra(extra);

    file_column_expr->set_data_type(ObVarcharType);
    file_column_expr->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    file_column_expr->set_collation_level(CS_LEVEL_IMPLICIT);
    file_column_expr->set_length(OB_MAX_VARCHAR_LENGTH);
    if (lib::is_oracle_mode()) {
      file_column_expr->set_length_semantics(LS_BYTE);
    }
    if (OB_FAIL(file_column_expr->formalize(&session_info))) {
      LOG_WARN("failed to extract info", K(ret));
    } else {
      expr = file_column_expr;
    }
  }

  return ret;
}

// 解析生成列表达式时，首先在table_schema中的column_schema中寻找依赖的列，如果找不到，再在 resolved_cols中找
int ObResolverUtils::resolve_generated_column_expr(ObResolverParams &params,
                                                   const ParseNode *node,
                                                   ObTableSchema &tbl_schema,
                                                   ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                                   ObColumnSchemaV2 &generated_column,
                                                   ObRawExpr *&expr,
                                                   const PureFunctionCheckStatus check_status,
                                                   bool coltype_not_defined)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *col_schema = NULL;
  ObSEArray<ObQualifiedName, 2> columns;
  ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 2> ref_sys_exprs;
  ObSEArray<ObRawExpr *, 6> real_exprs;
  ObSQLSessionInfo *session_info = params.session_info_;
  ObRawExprFactory *expr_factory = params.expr_factory_;
  // sequence in generated column not allowed.
  if (OB_ISNULL(expr_factory) || OB_ISNULL(session_info) || OB_ISNULL(node)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K(session_info), K(node));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(*expr_factory, *session_info,
                                                                 *node, expr, columns,
                                                                 &tbl_schema,
                                                                 false, /* allow_sequence */
                                                                 NULL,
                                                                 params.schema_checker_,
                                                                 check_status))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated column expr has invalid qualified name", K(q_name));
    } else if (lib::is_oracle_mode() && q_name.is_pl_udf()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "using udf as generated column");
      LOG_WARN("using udf as generated column is not supported", K(ret), K(q_name));
//      OZ (ObRawExprUtils::resolve_gen_column_udf_expr(expr,
//                                                      const_cast<ObQualifiedName &>(q_name),
//                                                      *expr_factory,
//                                                      *session_info,
//                                                      params.schema_checker_,
//                                                      columns,
//                                                      real_exprs,
//                                                      NULL));
    } else if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "generated column function";
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    } else if (FALSE_IT(col_schema = tbl_schema.get_column_schema(q_name.col_name_) == NULL ?
                                     get_column_schema_from_array(resolved_cols, q_name.col_name_) :
                                     tbl_schema.get_column_schema(q_name.col_name_))) {
    }

    if (OB_SUCC(ret)) {
      if (tbl_schema.is_external_table()) {
        // c1 int as( concat(file$col1, file$col2) )
        // defination of external generated column can only contain file columns
        if (NULL != col_schema) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Refering a normal external table column");
        } else if (OB_FAIL(ObResolverUtils::resolve_external_table_column_def(*params.expr_factory_,
                                                                              *params.session_info_,
                                                                              q_name,
                                                                              real_exprs,
                                                                              expr,
                                                                              &generated_column))) {
          LOG_WARN("fail to resolve external table column def", K(ret));
        }
      } else {
        if (NULL == col_schema || (col_schema->is_hidden() && !col_schema->is_udt_hidden_column())) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          ObString scope_name = "generated column function";
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, q_name.col_name_.length(), q_name.col_name_.ptr(),
                        scope_name.length(), scope_name.ptr());
        } else if (col_schema->is_generated_column()) {
          ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
          LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN,
                        "Defining a generated column on generated column(s)");
        } else if (lib::is_oracle_mode() && col_schema->get_meta_type().is_blob() &&
                  !col_schema->is_udt_hidden_column()) { // generated column depends on xml: gen_expr(sys_makexml(blob))
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Define a blob column in generated column def is not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "blob column in generated column definition");
        } else if (lib::is_mysql_mode() && col_schema->is_autoincrement()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("generated column cannot refer to auto-increment column", K(ret), K(*expr));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "generated column refer to auto-increment column");
        } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
          LOG_WARN("init column expr failed", K(ret));
        } else if (OB_FAIL(generated_column.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column id to generated column failed", K(ret));
        } else {
          if (col_schema->get_udt_set_id() > 0) {
            ObSEArray<ObColumnSchemaV2 *, 1> hidden_cols;
            if (OB_FAIL(tbl_schema.get_column_schema_in_same_col_group(col_schema->get_column_id(), col_schema->get_udt_set_id(), hidden_cols))) {
              LOG_WARN("get column schema in same col group failed", K(ret), K(col_schema->get_udt_set_id()));
            } else {
              for (int i = 0; i < hidden_cols.count() && OB_SUCC(ret); i++) {
                uint64_t cascaded_column_id = hidden_cols.at(i)->get_column_id();
                if (OB_FAIL(generated_column.add_cascaded_column_id(cascaded_column_id))) {
                  LOG_WARN("add cascaded column id to generated column failed", K(ret), K(cascaded_column_id));
                }
              }
            }
          }
          col_schema->add_column_flag(GENERATED_DEPS_CASCADE_FLAG);
          OZ (real_exprs.push_back(q_name.ref_expr_));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (lib::is_oracle_mode()) {
      bool expr_changed = false;
      OZ (ObRawExprUtils::try_modify_expr_for_gen_col_recursively(*session_info,
                                                                  NULL,
                                                                  *expr_factory,
                                                                  expr,
                                                                  expr_changed));
    }
    OZ (expr->formalize(session_info));
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      if (OB_FAIL(ObRawExprUtils::try_modify_udt_col_expr_for_gen_col_recursively(*session_info,
                                                                                  tbl_schema,
                                                                                  resolved_cols,
                                                                                  *expr_factory,
                                                                                  expr))) {
        LOG_WARN("transform udt col expr for generated column failed", K(ret));
      }
    }
    const ObObjType expr_datatype = expr->get_result_type().get_type();
    const ObCollationType expr_cs_type = expr->get_result_type().get_collation_type();
    const ObObjType dst_datatype = generated_column.get_data_type();
    const ObCollationType dst_cs_type = generated_column.get_collation_type();

    /* implicit data conversion judgement */
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      if (!cast_supported(expr_datatype,
                          expr_cs_type,
                          dst_datatype,
                          dst_cs_type)
       || OB_FAIL(ObDatumCast::check_can_cast(expr_datatype,
                                              expr_cs_type,
                                              dst_datatype,
                                              dst_cs_type))) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP,
                       ob_obj_type_str(expr_datatype),
                       ob_obj_type_str(dst_datatype));
        LOG_WARN("cast to expected type not supported",
                 K(ret),
                 K(expr_datatype),
                 K(dst_datatype));
      } else {
        ObObjType tmp_datatype = dst_datatype;
        if (ObNullType == tmp_datatype) {
          tmp_datatype = expr->get_result_type().get_type();
        }
        switch (tmp_datatype) {
          case ObURowIDType:
            ret = OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
            LOG_WARN("rowid data type in generated column definition", K(ret));
            break;
          case ObLobType:
          case ObLongTextType:
          case ObUserDefinedSQLType:
          case ObCollectionSQLType:
            ret = OB_ERR_RESULTANT_DATA_TYPE_OF_VIRTUAL_COLUMN_IS_NOT_SUPPORTED;
            LOG_WARN("lob data type in generated column definition", K(ret));
            break;
          default:
            /* nothing to do */
            break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (tbl_schema.is_external_table()) {
      //skip length check
    } else if (lib::is_oracle_mode() && ob_is_string_type(dst_datatype)) {
      // generated_column是用户定义的生成列，其长度以及长度语义(按照字符计算长度还是字节计算长度)依赖于SQL语句的定义
      // expr是用于生成generated_column数据的表达式。expr计算结果的长度不能大于generated_column定义的长度
      // 需要注意的是在进行长度比较时要考虑二者的长度语义，这里统一使用LS_BYTE语义来进行长度的比较
      //
      int64_t generated_column_length_in_byte = 0;
      int64_t expr_length_in_byte = 0;
      common::ObCollationType cs_type = generated_column.get_collation_type();
      int64_t mbmaxlen = 1;
      if (LS_CHAR == generated_column.get_length_semantics()) {
        if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
          LOG_WARN("fail to get mbmaxlen for generated_column", K(cs_type), K(ret));
        } else {
          generated_column_length_in_byte = generated_column.get_data_length() * mbmaxlen;
        }
      } else {
        generated_column_length_in_byte = generated_column.get_data_length();
      }
      if (OB_SUCC(ret)) {
        if (LS_CHAR == expr->get_accuracy().get_length_semantics()) {
          cs_type = expr->get_collation_type();
          if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
            LOG_WARN("fail to get mbmaxlen for expr", K(cs_type), K(ret));
          } else {
            expr_length_in_byte = expr->get_accuracy().get_length() * mbmaxlen;
          }
        } else {
          expr_length_in_byte = expr->get_accuracy().get_length();
        }
        /* clob/blob, raise error , according oracle:
        SQL> CREATE TABLE Z0CASE(z0_test0 char(10) , z0_test1 varchar2(4000),z0_test2  char(200 byte)  GENERATED ALWAYS AS (to_blob('')) VIRTUAL);
        CREATE TABLE Z0CASE(z0_test0 char(10) , z0_test1 varchar2(4000),z0_test2  char(200 byte)  GENERATED ALWAYS AS (to_blob('')) VIRTUAL)
                                                                *
        ERROR at line 1:
        ORA-12899: value too large for column "Z0_TEST2" (actual: 4000, maximum: 200) */
        if (ObLongTextType == expr->get_result_type().get_type()) {
          expr_length_in_byte = OB_MAX_ORACLE_CHAR_LENGTH_BYTE * 2;
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(generated_column_length_in_byte < expr_length_in_byte)) {
        ret = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("the length of generated column expr is more than the length of specified type",
                 K(generated_column_length_in_byte), K(expr_length_in_byte));
      }
    } else if (ob_is_raw(dst_datatype)) {
      if (ob_is_string_type(expr->get_data_type())) {
        if (OB_UNLIKELY(generated_column.get_data_length() * 2 < (expr->get_accuracy()).get_length())) {
          ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("the length of generated column expr is more than the length of specified type");
        }
      } else if (ob_is_raw(expr->get_data_type())) {
        if (OB_UNLIKELY(generated_column.get_data_length() < (expr->get_accuracy()).get_length())) {
          ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("the length of generated column expr is more than the length of specified type");
        }
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent datatypes", K(expr->get_result_type().get_type()));
      }
    }
    if (OB_SUCC(ret) && generated_column.is_generated_column()) {
      //set local session info for generate columns
      ObExprResType cast_dst_type;
      cast_dst_type.set_meta(generated_column.get_meta_type());
      cast_dst_type.set_accuracy(generated_column.get_accuracy());
      ObRawExpr *expr_with_implicit_cast = NULL;
      //only formalize once
      if (OB_FAIL(ObRawExprUtils::erase_operand_implicit_cast(expr, expr))) {
        LOG_WARN("fail to remove implicit cast", K(ret));
      } else if (coltype_not_defined) {
        expr_with_implicit_cast = expr;
        if (OB_FAIL(expr_with_implicit_cast->formalize(session_info, true))) {
          LOG_WARN("fail to formalize with local session info", K(ret));
        }
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(expr_factory, session_info,
                          *expr, cast_dst_type, expr_with_implicit_cast))) {
        LOG_WARN("try add cast above failed", K(ret));
      } else if (OB_FAIL(expr_with_implicit_cast->formalize(session_info, true))) {
        LOG_WARN("fail to formalize with local session info", K(ret));
      }
      if (OB_SUCC(ret) &&
        (ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status ||
         ObResolverUtils::CHECK_FOR_GENERATED_COLUMN == check_status)) {
        if (OB_FAIL(ObRawExprUtils::check_is_valid_generated_col(expr_with_implicit_cast, expr_factory->get_allocator()))) {
          if (OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION == ret
                  && ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status) {
            ret = OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED;
            LOG_WARN("sysfunc in expr is not valid for generated column", K(ret), K(*expr));
          } else {
            LOG_WARN("fail to check if the sysfunc exprs are valid in generated columns", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(session_info->get_effective_tenant_id(), tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_2_2_0) {
          //do nothing
        } else if (OB_ISNULL(expr_with_implicit_cast)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), KP(expr_with_implicit_cast));
        } else {
          if (OB_FAIL(ObRawExprUtils::extract_local_vars_for_gencol(expr_with_implicit_cast,
                                                                    session_info->get_sql_mode(),
                                                                    generated_column))) {
            LOG_WARN("fail to set local sysvars", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLResolver::print_expr_to_default_value(*expr, generated_column,
                                                                  params.schema_checker_, session_info->get_timezone_info()))) {
      LOG_WARN("fail to print_expr_to_default_value", KPC(expr), K(generated_column), K(ret));
    } else if (OB_FAIL(ObRawExprUtils::check_generated_column_expr_str(
        generated_column.get_cur_default_value().get_string(), *session_info, tbl_schema))) {
      LOG_WARN("fail to check printed generated column expr", K(ret));
    }
  }
  return ret;
}

// This function is used to resolve the dependent columns of generated column when retrieve schema.
// We use this function instead of build_generated_column_expr because there is not a thread-safe
// mem_context and the expr is not necessary.
int ObResolverUtils::resolve_generated_column_info(const ObString &expr_str,
                                                   ObIAllocator &allocator,
                                                   ObItemType &root_expr_type,
                                                   ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = NULL;
  if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(
              expr_str, ObCharsets4Parser(),
              allocator, node))) {
    LOG_WARN("parse expr node from string failed", K(ret), K(expr_str));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else if (OB_FAIL(SMART_CALL(resolve_column_info_recursively(node, column_names)))) {
    LOG_WARN("failed to resolve column into");
  } else {
    ObItemType type = node->type_;
    if (T_FUN_SYS == type) {
      if (OB_UNLIKELY(1 > node->num_child_) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0])) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid node children for fun_sys node", K(ret));
      } else {
        ObString func_name(node->children_[0]->str_len_, node->children_[0]->str_value_);
        type = ObExprOperatorFactory::get_type_by_name(func_name);
        if (OB_UNLIKELY(T_INVALID == (type))) {
          ret = OB_ERR_FUNCTION_UNKNOWN;
          LOG_WARN("function not exist", K(func_name), K(ret));
        }
      }
    }
    OX (root_expr_type = type);
  }
  return ret;
}

int ObResolverUtils::resolve_column_info_recursively(const ParseNode *node,
                                                     ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (T_COLUMN_REF == node->type_) {
    if (OB_UNLIKELY(node->num_child_ != 3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node type", K_(node->type), K(node->num_child_), K(ret));
    } else if (OB_UNLIKELY(node->children_[0] != NULL || node->children_[1] != NULL)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database node or table node is not null", K_(node->type), K(ret));
    } else if (OB_ISNULL(node->children_[2])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column node is null", K_(node->type), K(ret));
    } else if (OB_UNLIKELY(T_STAR == node->children_[2]->type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column node can't be T_STAR", K_(node->type), K(ret));
    } else {
      ObString column_name(static_cast<int32_t>(node->children_[2]->str_len_),
                           node->children_[2]->str_value_);
      if (OB_FAIL(column_names.push_back(column_name))) {
        LOG_WARN("Add column failed", K(ret));
      }
    }
  } else if (T_OBJ_ACCESS_REF == node->type_) {
    if (OB_UNLIKELY(node->num_child_ != 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node type", K_(node->type), K(node->num_child_), K(ret));
    } else if (OB_ISNULL(node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is NULL", K(node->num_child_));
    } else if (T_IDENT == node->children_[0]->type_ && NULL == node->children_[1]) {
      ObString column_name(static_cast<int32_t>(node->children_[0]->str_len_),
                           node->children_[0]->str_value_);
      OZ (column_names.push_back(column_name));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
    const ParseNode *child_node = node->children_[i];
    if (NULL == child_node) {
      // do nothing
    } else if (OB_FAIL(SMART_CALL(resolve_column_info_recursively(child_node, column_names)))) {
      LOG_WARN("recursive resolve column node failed", K(ret));
    }
  }
  return ret;
}

ObColumnSchemaV2* ObResolverUtils::get_column_schema_from_array(
                                     ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                     const ObString &column_name)
{
  for (int64_t i = 0; i < resolved_cols.count(); ++i) {
    if (OB_ISNULL(resolved_cols.at(i))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "resolved_cols has null pointer", K(i), K(resolved_cols));
      return NULL;
    } else if (0 == resolved_cols.at(i)->get_column_name_str().case_compare(column_name)) {
      return resolved_cols.at(i);
    }
  }
  return NULL;
}

int ObResolverUtils::resolve_default_expr_v2_column_expr(ObResolverParams &params,
                                                      const ObString &expr_str,
                                                      ObColumnSchemaV2 &default_expr_v2_column,
                                                      ObRawExpr *&expr,
                                                      bool allow_sequence)
{
  int ret = OB_SUCCESS;
  const ParseNode *expr_node = NULL;
  if (OB_ISNULL(params.allocator_) || OB_ISNULL(params.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator or session is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(
              expr_str, params.session_info_->get_charsets4parser(),
              *params.allocator_, expr_node))) {
    LOG_WARN("parse expr node from str failed", K(ret), K(expr_str));
  } else if (OB_FAIL(resolve_default_expr_v2_column_expr(params, expr_node,  default_expr_v2_column, expr, allow_sequence))) {
    LOG_WARN("resolve default_expr_v2_column expr failed", K(ret), K(expr_str));
  }
  return ret;
}

int ObResolverUtils::resolve_default_expr_v2_column_expr(ObResolverParams &params,
                                                         const ParseNode *node,
                                                         ObColumnSchemaV2 &default_expr_v2_column,
                                                         ObRawExpr *&expr,
                                                         bool allow_sequence)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObSQLSessionInfo *session_info = params.session_info_;
  ObRawExprFactory *expr_factory = params.expr_factory_;
  if (OB_ISNULL(expr_factory) || OB_ISNULL(session_info) || OB_ISNULL(node)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K(session_info), K(node));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(*expr_factory,
                                                                 *session_info,
                                                                 *node,
                                                                 expr,
                                                                 columns,
                                                                 NULL,
                                                                 allow_sequence,
                                                                 NULL,
                                                                 params.schema_checker_,
                                                            PureFunctionCheckStatus::DISABLE_CHECK,
                                                                 false))) {
    LOG_WARN("build expr_default column expr failed", "is_oracle_compatible", session_info->get_compatibility_mode(), K(ret));
  } else if (lib::is_oracle_mode() && columns.count() > 0) {
    // oracle模式下，default值如果是一个函数，函数会被挂在T_OBJ_ACCESS_REF下面，resolve之后可能会生成column
    // 比如create table tjianhua12(c1 int default length('hello'));
    // length('hello')会在resolve后产生一个column，需要将column.ref_expr_替换成真正的sys_func
    bool is_all_sys_func = true;
    ObRawExpr *real_ref_expr = NULL;
    ObArray<ObRawExpr*> real_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && is_all_sys_func && i < columns.count(); i++) {
      ObQualifiedName &q_name = columns.at(i);
      if (q_name.is_pl_udf()) { // only default constructer is supported
        if (OB_FAIL(ObResolverUtils::resolve_external_symbol(*params.allocator_,
                                                             *params.expr_factory_,
                                                             *params.session_info_,
                                                             *params.schema_checker_->get_schema_guard(),
                                                             params.sql_proxy_,
                                                             &(params.external_param_info_),
                                                             params.secondary_namespace_,
                                                             q_name,
                                                             columns,
                                                             real_exprs,
                                                             real_ref_expr,
                                                             params.package_guard_,
                                                             params.is_prepare_protocol_,
                                                             false, /*is_check_mode*/
                                                             true /*is_sql_scope*/))) {
          LOG_WARN_IGNORE_COL_NOTFOUND(ret, "failed to resolve var", K(q_name), K(ret));
        } else if (OB_ISNULL(real_ref_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid expr", K(expr), K(ret));
        } else if (real_ref_expr->is_udf_expr()
                   && (expr->get_expr_type() != T_FUN_PL_OBJECT_CONSTRUCT)
                   && (expr->get_expr_type() != T_FUN_PL_COLLECTION_CONSTRUCT)) {
          // only default constructor is supported currently
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "using udf as default value");
          LOG_WARN("using udf as default value is not supported",
                   K(ret), K(default_expr_v2_column.get_column_name_str()));
        } else {
          // column_ref is replaced inside build_generated_column_expr with udf,
          // here replace udf with object/collection constructor
          if (OB_FAIL(real_exprs.push_back(real_ref_expr))) {
            LOG_WARN("push back error", K(ret));
          }
          // handle flatterned obj access: a(b,c)->b,c,a repalce ref_expr once an ObQualifiedName handled
          for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
            ObQualifiedName &q_name = columns.at(i);
            const ObUDFInfo &udf_info = q_name.access_idents_.at(q_name.access_idents_.count() - 1).udf_info_;
            if (OB_NOT_NULL(udf_info.ref_expr_)) {
              if (OB_FAIL(ObRawExprUtils::replace_ref_column(real_ref_expr,
                                                             udf_info.ref_expr_,
                                                             real_exprs.at(i)))) {
                LOG_WARN("replace column ref expr failed", K(ret));
              }
            }
          }
          // replace expr, only outside ref_expr_ is equal to expr
          const ObUDFInfo &udf_info = q_name.access_idents_.at(q_name.access_idents_.count() - 1).udf_info_;
          if (OB_SUCC(ret) && OB_NOT_NULL(udf_info.ref_expr_)) {
            if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, udf_info.ref_expr_, real_ref_expr))) {
              LOG_WARN("replace column ref expr failed", K(ret));
            }
          }
          real_ref_expr = NULL;
        }
      } else if (!q_name.is_sys_func()) {
        is_all_sys_func = false;
      } else if (OB_ISNULL(q_name.access_idents_.at(0).sys_func_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret));
      } else if (OB_FAIL(q_name.access_idents_.at(0).check_param_num())) {
        LOG_WARN("sys func param number not match", K(ret));
      } else {
        real_ref_expr = static_cast<ObRawExpr *>(q_name.access_idents_.at(0).sys_func_expr_);
        if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr))) {
          LOG_WARN("replace column ref expr failed", K(ret));
        } else {
          real_ref_expr = NULL;
        }
      }
    }
    if (!is_all_sys_func) {
      // log user error
      ret = OB_ERR_BAD_FIELD_ERROR;
      const ObQualifiedName &q_name = columns.at(0);
      ObString scope_name = default_expr_v2_column.get_column_name_str();
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("formalize expr failed", K(ret));
    } else {
      LOG_DEBUG("succ to resolve_default_expr_v2_column_expr",
                "is_const_expr", expr->is_const_raw_expr(),
                KPC(expr), K(ret));
    }
  } else if (OB_UNLIKELY(!columns.empty())) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    const ObQualifiedName &q_name = columns.at(0);
    ObString scope_name = default_expr_v2_column.get_column_name_str();
    ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
  } else if (OB_FAIL(expr->formalize(session_info))) {
    LOG_WARN("formalize expr failed", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_ROWNUM))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("rownum in default value is not allowed", K(ret));
  } else {
    LOG_DEBUG("succ to resolve_default_expr_v2_column_expr",
              "is_const_expr", expr->is_const_raw_expr(),
              KPC(expr), K(ret));
  }
  return ret;
}

int ObResolverUtils::check_comment_length(
    ObSQLSessionInfo *session_info,
    char *str,
    int64_t *str_len,
    const int64_t max_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info) || (NULL == str  && *str_len != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null pointer", K(session_info), K(str), K(ret));
  } else {
    size_t tmp_len = ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, str, *str_len, max_len);
    if (tmp_len < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect error", K(ret));
    } else if (tmp_len < *str_len) {
      if (!is_strict_mode(session_info->get_sql_mode())) {
        ret = OB_SUCCESS;
        *str_len = tmp_len;
        LOG_USER_WARN(OB_ERR_TOO_LONG_FIELD_COMMENT, max_len);
      } else {
        ret = OB_ERR_TOO_LONG_FIELD_COMMENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_FIELD_COMMENT, max_len);
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_user_variable_length(char *str, int64_t str_len)
{
  int ret = OB_SUCCESS;
  char *name = str;
  char *end = str + str_len;
  int name_length = 0;
  bool last_char_is_space = true;
  if ((NULL == str  && str_len != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null pointer", K(str), K(ret));
  } else {
    const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_GENERAL_CI);
    while (name < end) {
      last_char_is_space = ObCharset::is_space(CS_TYPE_UTF8MB4_GENERAL_CI, *str);
      if (use_mb(cs)) {
        uint64_t char_len = ob_ismbchar(cs, str, str+cs->mbmaxlen);
        if (char_len) {
          name += char_len;
          name_length++;
        } else {
          name++;
          name_length++;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (last_char_is_space || (name_length > MAX_NAME_CHAR_LEN)) {
      ret = OB_ERR_ILLEGAL_USER_VAR;
      LOG_USER_ERROR(OB_ERR_ILLEGAL_USER_VAR, name_length, str);
    }
  }
  return ret;
}

int ObResolverUtils::resolve_check_constraint_expr(
    ObResolverParams &params,
    const ParseNode *node,
    const ObTableSchema &tbl_schema,
    ObConstraint &constraint,
    ObRawExpr *&expr,
    const share::schema::ObColumnSchemaV2 *column_schema,
    ObIArray<ObQualifiedName> *res_columns /*default null*/)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<std::pair<ObRawExpr*, ObRawExpr*>> ref_sys_exprs;
  ObSQLSessionInfo *session_info = params.session_info_;
  ObRawExprFactory *expr_factory = params.expr_factory_;
  bool is_col_level_cst = false; // table level cst
  common::ObSEArray<uint64_t, common::SEARRAY_INIT_NUM> column_ids;

  if (NULL != column_schema) {
    // column_schema 为 NULL 表示约束为表级约束
    is_col_level_cst = true;
  }
  if (OB_ISNULL(expr_factory) || OB_ISNULL(session_info) || OB_ISNULL(node) || OB_ISNULL(params.schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K(session_info), K(node), K(params.schema_checker_));
  } else if (OB_FAIL(ObRawExprUtils::build_check_constraint_expr(*expr_factory, *session_info, *node, expr, columns))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ObRawExpr *sys_func = q_name.access_idents_.at(0).sys_func_expr_;
      CK (OB_NOT_NULL(sys_func));
      if (OB_FAIL(ret)) {
      } else if (lib::is_oracle_mode()
                 && (T_FUN_SYS_SYS_CONTEXT == sys_func->get_expr_type()
                     || T_FUN_SYS_USERENV == sys_func->get_expr_type())) {
        ret = OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST;
        LOG_WARN("date or system variable wrongly specified in CHECK constraint", K(ret));
      } else if (lib::is_mysql_mode()) {
        bool is_non_pure_func = false;
        if (OB_FAIL(sys_func->is_non_pure_sys_func_expr(is_non_pure_func))) {
          LOG_WARN("check is non pure sys func expr failed", K(ret));
        } else if (is_non_pure_func) {
          ret = OB_ERR_CHECK_CONSTRAINT_NAMED_FUNCTION_IS_NOT_ALLOWED;
          LOG_WARN("date or system variable wrongly specified in CHECK constraint", K(ret), K(sys_func->get_expr_type()));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ref_sys_exprs.count(); ++i) {
        OZ (ObRawExprUtils::replace_ref_column(sys_func, ref_sys_exprs.at(i).first, ref_sys_exprs.at(i).second));
      }
      OZ (q_name.access_idents_.at(0).check_param_num());
      OZ (ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, sys_func));
      OZ (ref_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, sys_func)));
    } else if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "constraint column function";
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    } else {
      if (!is_col_level_cst) {
        if (OB_ISNULL(column_schema = tbl_schema.get_column_schema(q_name.col_name_))
            || column_schema->is_hidden()) {
          if (lib::is_oracle_mode()) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            ObString scope_name = "constraint column function";
            LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                q_name.col_name_.length(),
                q_name.col_name_.ptr(),
                scope_name.length(),
                scope_name.ptr());
          } else {
            ret = OB_ERR_CHECK_CONSTRAINT_REFERS_UNKNOWN_COLUMN;
            LOG_USER_ERROR(OB_ERR_CHECK_CONSTRAINT_REFERS_UNKNOWN_COLUMN,
                constraint.get_constraint_name_str().length(),
                constraint.get_constraint_name_str().ptr(),
                q_name.col_name_.length(),
                q_name.col_name_.ptr());
          }
        }
      } else { // is_col_level_cst
        if (0 != columns.at(i).col_name_.compare(column_schema->get_column_name_str())) {
          ret = OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL;
          if (lib::is_mysql_mode()) {
            LOG_USER_ERROR(OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL,
                constraint.get_constraint_name_str().length(),
                constraint.get_constraint_name_str().ptr());
          }
          LOG_WARN("column check constraint cannot reference other columns",
                   K(ret),
                   K(columns.at(i).col_name_),
                   K(column_schema->get_column_name_str()));
        }
      }
      if (OB_SUCC(ret) && lib::is_mysql_mode() && column_schema->is_autoincrement()) {
        ret = OB_ERR_CHECK_CONSTRAINT_REFERS_AUTO_INCREMENT_COLUMN;
        LOG_WARN("Check constraint cannot refer to an auto-increment column", K(ret), K(column_schema->get_column_id()));
      }
      if (OB_SUCC(ret)) {
        // 当添加重复的 column id 时，需要去重
        if (column_ids.end() == std::find(column_ids.begin(), column_ids.end(), column_schema->get_column_id())) {
          if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
            LOG_WARN("push back to column_ids failed", K(ret), K(column_schema->get_column_id()));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObRawExprUtils::init_column_expr(*column_schema,
                                                     *q_name.ref_expr_))) {
          LOG_WARN("init column expr failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(constraint.assign_column_ids(column_ids))) {
      LOG_WARN("fail to assign_column_ids", K(column_ids));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(expr->formalize(session_info))) {
    LOG_WARN("formalize expr failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (lib::is_mysql_mode()) {
    if (T_INT == expr->get_expr_type()
        || T_TINYINT == expr->get_expr_type()
        || IS_BOOL_OP(expr->get_expr_type())) {
      // 1, 0, true, false, <=> are permitted
    } else if (expr->is_sys_func_expr() && ObTinyIntType == expr->get_result_type().get_type()) {
      // sys func returns bool are permitted
    } else {
      ret = OB_ERR_NON_BOOLEAN_EXPR_FOR_CHECK_CONSTRAINT;
      LOG_USER_ERROR(OB_ERR_NON_BOOLEAN_EXPR_FOR_CHECK_CONSTRAINT,
         constraint.get_constraint_name_str().length(), constraint.get_constraint_name_str().ptr());
      LOG_WARN("expr result type is not boolean", K(ret), K(expr->get_result_type().get_type()));
    }
  } else {
    if (expr->get_expr_type() == T_FUN_SYS_IS_JSON) {
      ObObjType in_type = column_schema->get_data_type();
      if (!(in_type == ObVarcharType
            || in_type == ObNVarchar2Type
            || in_type == ObLongTextType
            || in_type == ObJsonType
            || in_type == ObNVarchar2Type
            || in_type == ObRawType)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", ob_obj_type_str(in_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    HEAP_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf) {
      MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
      int64_t pos = 0;
      ObString expr_def;
      ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos,
                                    params.schema_checker_->get_schema_guard(), session_info->get_timezone_info());
      if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
        LOG_WARN("print expr definition failed", K(ret));
      } else if (FALSE_IT(expr_def.assign_ptr(expr_str_buf, static_cast<int32_t>(pos)))) {
      } else if (OB_FAIL(constraint.set_check_expr(expr_def))) {
        LOG_WARN("set check expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && res_columns != NULL) {
    if (OB_FAIL(append(*res_columns, columns))) {
      LOG_WARN("failed to append", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::create_not_null_expr_str(const ObString &column_name,
                                              common::ObIAllocator &allocator,
                                              ObString &expr_str,
                                              const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  const int64_t buf_len = column_name.length() + NOT_NULL_STR_EXTRA_SIZE + 1;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(column_name));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
              is_oracle_mode ? "\"%.*s\" IS NOT NULL" : "`%.*s` IS NOT NULL",
              column_name.length(), column_name.ptr()))) {
    LOG_WARN("print not null constraint expr str failed", K(ret));
  } else if (OB_UNLIKELY(buf_len != pos + 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length of not null constraint expr str not expected", K(ret),
            K(buf_len), K(pos), K(column_name));
  } else {
    expr_str.assign_ptr(buf, pos);
  }
  return ret;
}

int ObResolverUtils::build_partition_key_expr(ObResolverParams &params,
                                              const share::schema::ObTableSchema &table_schema,
                                              ObRawExpr *&partition_key_expr,
                                              ObIArray<ObQualifiedName> *qualified_names)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *func_expr = NULL;
  if (OB_ISNULL(params.expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr factory is null", K(params.expr_factory_));
  } else if (OB_FAIL(params.expr_factory_->create_raw_expr(T_FUN_SYS_PART_KEY, func_expr))) {
    LOG_WARN("create sysfunc expr failed", K(ret));
  } else {
    ObString partition_fun_name("partition_key");
    func_expr->set_func_name(partition_fun_name);
    if (OB_FAIL(func_expr->add_flag(IS_FUNC))) {
      LOG_WARN("failed to add flag IS_FUNC");
    } else if (OB_FAIL(func_expr->add_flag(CNT_COLUMN))) {
      LOG_WARN("failed to add flag CNT_COLUMN");
    } else if (OB_FAIL(func_expr->add_flag(CNT_FUNC))) {
      LOG_WARN("failed to add flag CNT_FUNC");
    } else {}
  }
  //use primary key column to build a ObColumnRefRawExpr
  for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
      OB_SUCC(ret) && iter != table_schema.column_end(); ++iter) {
    const ObColumnSchemaV2 &column_schema = (**iter);
    if (!column_schema.is_original_rowkey_column() || column_schema.is_hidden()) {
      //parition by key() use primary key to create partition key not hidden auto_increment primary key
      continue;
    } else if (column_schema.is_autoincrement()) {
      ret = OB_ERR_AUTO_PARTITION_KEY;
      LOG_USER_ERROR(OB_ERR_AUTO_PARTITION_KEY, column_schema.get_column_name_str().length(),
                     column_schema.get_column_name_str().ptr());
    } else {
      ObColumnRefRawExpr *column_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_column_expr(*params.expr_factory_, column_schema, column_expr))) {
        LOG_WARN("failed to build column schema!", K(column_expr), K(column_schema));
      } else if (OB_FAIL(func_expr->add_param_expr(column_expr))) {
        LOG_WARN("failed to add param expr!", K(ret));
      } else if (qualified_names != NULL) {
        ObQualifiedName name;
        name.col_name_.assign_ptr(column_schema.get_column_name(),
                                  column_schema.get_column_name_str().length());
        name.ref_expr_ = column_expr;
        name.is_star_ = false;
        if (OB_FAIL(qualified_names->push_back(name))) {
          LOG_WARN("failed to push back qualified name", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    partition_key_expr = func_expr;
    if (OB_FAIL(partition_key_expr->formalize(params.session_info_))) {
      LOG_WARN("deduce type failed", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::check_column_name(const ObSQLSessionInfo *session_info,
                                       const ObQualifiedName &q_name,
                                       const ObColumnRefRawExpr &col_ref,
                                       bool &is_hit)
{
  int ret = OB_SUCCESS;
  is_hit = true;
  if (!q_name.database_name_.empty()) {
    if (OB_FAIL(name_case_cmp(session_info, q_name.database_name_,
                              col_ref.get_database_name(), OB_TABLE_NAME_CLASS, is_hit))) {
      LOG_WARN("cmp database name failed",  K(ret), K(q_name), K(col_ref.get_database_name()));
    }
  }
  if (OB_SUCC(ret) && !q_name.tbl_name_.empty() && is_hit) {
    ObString table_name = col_ref.get_synonym_name().empty() ? col_ref.get_table_name() : col_ref.get_synonym_name();
    if (OB_FAIL(name_case_cmp(session_info, q_name.tbl_name_, table_name,
                              OB_TABLE_NAME_CLASS, is_hit))) {
      LOG_WARN("compare table name failed", K(q_name), K(q_name), K(col_ref));
    }
  }
  if (OB_SUCC(ret) && is_hit) {
    is_hit = ObCharset::case_insensitive_equal(q_name.col_name_, col_ref.get_column_name());
  }
  return ret;
}

int ObResolverUtils::create_generate_table_column(ObRawExprFactory &expr_factory,
                                                  const TableItem &table_item,
                                                  uint64_t column_id,
                                                  ColumnItem &col_item)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *col_expr = NULL;
  if (OB_UNLIKELY(!table_item.is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not generated table", K_(table_item.type));
  } else {
    int64_t item_index = column_id - OB_APP_MIN_COLUMN_ID;
    ObSelectStmt *ref_stmt = table_item.ref_query_;
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_NOT_INIT;
      LOG_WARN("generate table ref stmt is null");
    } else if (OB_UNLIKELY(item_index < 0 || item_index >= ref_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item index is invalid", K(item_index), K(ref_stmt->get_select_item_size()));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, col_expr))) {
      LOG_WARN("create column ref expr failed", K(ret));
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col expr is null");
    } else if (OB_FAIL(resolve_default_value_and_expr_from_select_item(ref_stmt->get_select_item(item_index),
                                                              col_item,
                                                              ref_stmt))) {
      LOG_WARN("fail to resolve select item's default value");
    } else {
      col_expr->set_ref_id(table_item.table_id_, column_id);
      col_expr->set_result_type(ref_stmt->get_select_item(item_index).expr_->get_result_type());
      col_expr->set_column_attr(table_item.alias_name_, ref_stmt->get_select_item(item_index).alias_name_);
    }
  }
  //init column item
  if (OB_SUCC(ret)) {
    col_item.expr_ = col_expr;
    col_item.table_id_ = col_expr->get_table_id();
    col_item.column_id_ = col_expr->get_column_id();
    col_item.column_name_ = col_expr->get_column_name();
  }
  return ret;
}

int ObResolverUtils::check_unique_index_cover_partition_column(const ObTableSchema &table_schema,
                                                               const ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_partitioned_table()
      || (INDEX_TYPE_PRIMARY != arg.index_type_
          && INDEX_TYPE_UNIQUE_LOCAL != arg.index_type_
          && INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL != arg.index_type_
          && INDEX_TYPE_UNIQUE_GLOBAL != arg.index_type_)) {
    //nothing to do
  } else {
    const common::ObPartitionKeyInfo &partition_info = table_schema.get_partition_key_info();
    const common::ObPartitionKeyInfo &subpartition_info = table_schema.get_subpartition_key_info();
    ObSEArray<uint64_t, 5> idx_col_ids;
    if (OB_FAIL(get_index_column_ids(table_schema, arg.index_columns_, idx_col_ids))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get index column ids", K(ret), K(table_schema), K(arg.index_columns_));
    } else if (OB_FAIL(unique_idx_covered_partition_columns(table_schema, idx_col_ids, partition_info))) {
      LOG_WARN("Unique index covered partition columns failed", K(ret));
    } else if (OB_FAIL(unique_idx_covered_partition_columns(table_schema, idx_col_ids, subpartition_info))) {
      LOG_WARN("Unique index convered partition columns failed", K(ret));
    } else { }//do nothing
  }
  return ret;
}

int ObResolverUtils::get_index_column_ids(
    const ObTableSchema &table_schema,
    const ObIArray<ObColumnSortItem> &columns,
    ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;
  for  (int64_t idx = 0; OB_SUCC(ret) && idx < columns.count(); ++idx) {
    if (OB_ISNULL(column_schema = table_schema.get_column_schema(columns.at(idx).column_name_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get column schema", K(ret), "column id", columns.at(idx));
    } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("Failed to add column id", K(ret));
    } else { }//do nothing
  }
  return ret;
}

int ObResolverUtils::unique_idx_covered_partition_columns(
    const ObTableSchema &table_schema,
    const ObIArray<uint64_t> &index_columns,
    const ObPartitionKeyInfo &partition_info)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;
  const ObPartitionKeyColumn *column = NULL;
  bool is_oracle_mode = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_info.get_size(); i++) {
    column = partition_info.get_column(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get parition info", K(ret), K(column));
    } else if (!has_exist_in_array(index_columns, column->column_id_)) {
      if (OB_ISNULL(column_schema = table_schema.get_column_schema(column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column schema is NULL", K(ret));
      } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("failed to check if oralce compat mode", K(ret));
      } else if (is_oracle_mode && column_schema->is_generated_column()) {
        ObSEArray<uint64_t, 5> cascaded_columns;
        if (OB_FAIL(column_schema->get_cascaded_column_ids(cascaded_columns))) {
          LOG_WARN("Failed to get cascaded column ids", K(ret));
        } else {
          for (int64_t idx = 0; OB_SUCC(ret) && idx < cascaded_columns.count(); ++idx) {
            if (!has_exist_in_array(index_columns, cascaded_columns.at(idx))) {
              ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
              LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "UNIQUE INDEX");
            }
          }
        }
      } else {
        ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
        LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "UNIQUE INDEX");
      }
    } else { }//do nothing
  } // end for
  return ret;
}

int ObResolverUtils::resolve_data_type(const ParseNode &type_node,
                                       const ObString &ident_name,
                                       ObDataType &data_type,
                                       const int is_oracle_mode/*1:Oracle, 0:MySql */,
                                       const bool is_for_pl_type,
                                       const ObSessionNLSParams &nls_session_param,
                                       uint64_t tenant_id,
                                       const bool enable_decimal_int_type,
                                       const bool convert_real_type_to_decimal /*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ob_is_valid_obj_type(static_cast<ObObjType>(type_node.type_)))) {
    ret = OB_ERR_ILLEGAL_TYPE;
    SQL_RESV_LOG(WARN, "Unsupport data type of column definiton",
                        K(ident_name), K((type_node.type_)), K(ret));
  } else {
  data_type.set_obj_type(static_cast<ObObjType>(type_node.type_));
  int32_t length = type_node.int32_values_[0];
  int16_t precision = type_node.int16_values_[0];
  int16_t scale = type_node.int16_values_[1];
  const int16_t number_type = type_node.int16_values_[2];
  const bool has_specify_scale = (1 == type_node.int16_values_[2]);
  uint64_t data_version = 0;

  if (convert_real_type_to_decimal && !is_oracle_mode &&
      (precision >= 0 && scale >= 0)) {
      if (static_cast<ObObjType>(type_node.type_) == ObFloatType ||
          static_cast<ObObjType>(type_node.type_) == ObDoubleType) {
        data_type.set_obj_type(ObNumberType);
      } else if (static_cast<ObObjType>(type_node.type_) == ObUFloatType ||
                  static_cast<ObObjType>(type_node.type_) == ObUDoubleType) {
        data_type.set_obj_type(ObUNumberType);
      }
  }
  const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode][data_type.get_obj_type()];

  LOG_DEBUG("resolve_data_type", K(ret), K(has_specify_scale), K(type_node.type_), K(type_node.param_num_), K(number_type), K(scale), K(precision), K(length));
  switch (data_type.get_type_class()) {
    case ObIntTC:
      // fallthrough
    case ObUIntTC:
      if (precision <= 0) {
        precision = default_accuracy.get_precision();
      }
      if (precision > OB_MAX_INTEGER_DISPLAY_WIDTH) {
        ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH, ident_name.ptr(), OB_MAX_INTEGER_DISPLAY_WIDTH);
      } else {
        data_type.set_precision(precision);
        data_type.set_scale(0);
      }
      data_type.set_zero_fill(static_cast<bool>(type_node.int16_values_[2]));
      break;
    case ObFloatTC:
      // fallthrough
    case ObDoubleTC: {
      if(is_oracle_mode) {
        data_type.set_precision(precision);
        data_type.set_scale(scale);
      } else {
        if (OB_UNLIKELY(scale > OB_MAX_DOUBLE_FLOAT_SCALE)) {
          ret = OB_ERR_TOO_BIG_SCALE;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_SCALE, scale, ident_name.ptr(), OB_MAX_DOUBLE_FLOAT_SCALE);
          LOG_WARN("scale of double overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(OB_DECIMAL_NOT_SPECIFIED == scale &&
                              precision > OB_MAX_DOUBLE_FLOAT_PRECISION)) {
          ret = OB_ERR_COLUMN_SPEC;
          LOG_USER_ERROR(OB_ERR_COLUMN_SPEC, ident_name.length(), ident_name.ptr());
          LOG_WARN("precision of double overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(OB_DECIMAL_NOT_SPECIFIED != scale &&
                  precision > OB_MAX_DOUBLE_FLOAT_DISPLAY_WIDTH ||
                  (0 == scale && 0 == precision))) {
          ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH,
                        ident_name.ptr(),
                        OB_MAX_INTEGER_DISPLAY_WIDTH);
        } else if (OB_UNLIKELY(precision < scale)) {
          ret = OB_ERR_M_BIGGER_THAN_D;
          LOG_USER_ERROR(OB_ERR_M_BIGGER_THAN_D, to_cstring(ident_name));
          LOG_WARN("precision less then scale", K(ret), K(scale), K(precision));
        } else {
          // mysql> create table t1(a decimal(0, 0));
          // mysql> desc t1;
          // +-------+---------------+------+-----+---------+-------+
          // | Field | Type          | Null | Key | Default | Extra |
          // +-------+---------------+------+-----+---------+-------+
          // | a     | decimal(10,0) | YES  |     | NULL    |       |
          // +-------+---------------+------+-----+---------+-------+
          // the same as float and double.
          if (precision <= 0 && scale <= 0) {
            precision = default_accuracy.get_precision();
            scale = default_accuracy.get_scale();
          }
          //A precision from 24 to 53 results in an 8-byte double-precision DOUBLE column.
          if (T_FLOAT == type_node.type_
              && precision > OB_MAX_FLOAT_PRECISION
              && -1 == scale) {
            data_type.set_obj_type(static_cast<ObObjType>(T_DOUBLE));
          }
          data_type.set_precision(precision);
          data_type.set_scale(scale);
          data_type.set_zero_fill(static_cast<bool>(type_node.int16_values_[2]));
        }
      }
      break;
    }
    case ObNumberTC: {
      if (data_type.get_meta_type().is_number_float()) {
        if (precision != PRECISION_UNKNOWN_YET
            && (OB_UNLIKELY(precision < OB_MIN_NUMBER_FLOAT_PRECISION)
                || OB_UNLIKELY(precision > OB_MAX_NUMBER_FLOAT_PRECISION))) {
          ret = OB_FLOAT_PRECISION_OUT_RANGE;
          LOG_WARN("precision of float out of range", K(ret), K(precision));
        } else if (-86 == scale) {
          if (is_for_pl_type) {
            ret = OB_NUMERIC_PRECISION_NOT_INTEGER;
            LOG_USER_ERROR(OB_NUMERIC_PRECISION_NOT_INTEGER, (int)type_node.str_len_, type_node.str_value_);
            LOG_WARN("non-integral numeric literal value is inappropriate in this context", K(ret), K(scale), K(precision));
          } else {
            ret = OB_ERR_REQUIRE_INTEGER;
            LOG_USER_ERROR(OB_ERR_REQUIRE_INTEGER);
            LOG_WARN("non-integral numeric literal value is inappropriate in this context", K(ret), K(scale), K(precision));
          }
        } else {
          data_type.set_precision(precision);
          data_type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
        }
      } else if (is_oracle_mode) {
        if ((number_type == NPT_PERC_SCALE || number_type == NPT_PERC)
            &&(OB_UNLIKELY(precision < OB_MIN_NUMBER_PRECISION)
              || OB_UNLIKELY(precision > OB_MAX_NUMBER_PRECISION))) {
          ret = OB_NUMERIC_PRECISION_OUT_RANGE;
          LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
        } else if (-86 == scale) {
          ret = OB_NUMERIC_PRECISION_NOT_INTEGER;
          LOG_USER_ERROR(OB_NUMERIC_PRECISION_NOT_INTEGER, (int)type_node.str_len_, type_node.str_value_);
          LOG_WARN("non-integral numeric literal value is inappropriate in this context", K(ret), K(scale), K(precision));
        }

        if (OB_SUCC(ret)) {
          if ((number_type == NPT_PERC_SCALE || number_type == NPT_STAR_SCALE)
              && (OB_UNLIKELY(scale < OB_MIN_NUMBER_SCALE)
                  || OB_UNLIKELY(scale > OB_MAX_NUMBER_SCALE))) {
            ret = OB_NUMERIC_SCALE_OUT_RANGE;
            LOG_WARN("scale of number out of range", K(ret), K(scale));
          }
        }

        if (OB_SUCC(ret)) {
          data_type.set_precision(precision);
          data_type.set_scale(scale);
        }
        // oracle 模式下，目前只有满足以下情况时才使用decimal int
        // 1. precision >= 1 && precision <= 38
        // 2. scale >= 0
        // 3. precision >= scale
        // 也就是和mysql一样，只有(P, S)都确定，且scale不会超过Precision时才使用ObDecimalIntType
        // TODO: 支持任意合理的P, S使用DecimalIntType
        if (enable_decimal_int_type && !convert_real_type_to_decimal
            && (precision >= OB_MIN_NUMBER_PRECISION && scale >= 0 && precision >= scale)) {
          data_type.set_obj_type(ObDecimalIntType);
        }
        LOG_DEBUG("set decimalint in oracle mode", K(enable_decimal_int_type),
                  K(convert_real_type_to_decimal), K(precision), K(scale));
      } else {
        if (OB_UNLIKELY(precision > OB_MAX_DECIMAL_PRECISION)) {
          ret = OB_ERR_TOO_BIG_PRECISION;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, precision, ident_name.ptr(), OB_MAX_DECIMAL_PRECISION);
          LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(scale > OB_MAX_DECIMAL_SCALE)) {
          ret = OB_ERR_TOO_BIG_SCALE;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_SCALE, scale, ident_name.ptr(), OB_MAX_DECIMAL_SCALE);
          LOG_WARN("scale of number overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(precision < scale)) {
          ret = OB_ERR_M_BIGGER_THAN_D;
          LOG_USER_ERROR(OB_ERR_M_BIGGER_THAN_D, to_cstring(ident_name));
          LOG_WARN("precision less then scale", K(ret), K(scale), K(precision));
        } else {
          // mysql> create table t1(a decimal(0, 0));
          // mysql> desc t1;
          // +-------+---------------+------+-----+---------+-------+
          // | Field | Type          | Null | Key | Default | Extra |
          // +-------+---------------+------+-----+---------+-------+
          // | a     | decimal(10,0) | YES  |     | NULL    |       |
          // +-------+---------------+------+-----+---------+-------+
          // the same as float and double.
          if (precision <= 0 && scale <= 0) {
            precision = default_accuracy.get_precision();
            scale = default_accuracy.get_scale();
          }
          data_type.set_precision(precision);
          data_type.set_scale(scale);
          data_type.set_zero_fill(static_cast<bool>(type_node.int16_values_[2]));
          if (enable_decimal_int_type && !convert_real_type_to_decimal
              && !data_type.get_meta_type().is_unumber()) {
            data_type.set_obj_type(ObDecimalIntType);
            LOG_DEBUG("set decimalint in mysql mode", K(data_type), K(precision), K(scale));
          }
        }
      }
      break;
    }
    case ObOTimestampTC:
      if (is_oracle_mode && -86 == type_node.int16_values_[2]) {
          if (is_for_pl_type) {
            ret = OB_NUMERIC_PRECISION_NOT_INTEGER;
            LOG_USER_ERROR(OB_NUMERIC_PRECISION_NOT_INTEGER, (int)type_node.str_len_, type_node.str_value_);
            LOG_WARN("non-integral numeric literal value is inappropriate in this context", K(ret), K(scale), K(precision));
          } else {
            ret = OB_ERR_REQUIRE_INTEGER;
            LOG_USER_ERROR(OB_ERR_REQUIRE_INTEGER);
            LOG_WARN("non-integral numeric literal value is inappropriate in this context", K(ret), K(scale), K(precision));
          }
      } else {
        if (!has_specify_scale) {
          scale = default_accuracy.get_scale();
        }
        if (OB_UNLIKELY(scale > OB_MAX_TIMESTAMP_TZ_PRECISION)) {
          ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

        } else {
          data_type.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + scale));
          data_type.set_scale(scale);
        }
      }
      break;
    case ObDateTimeTC:
      if (scale > OB_MAX_DATETIME_PRECISION) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, ident_name.ptr(), OB_MAX_DATETIME_PRECISION);
      } else {
        // TODO@nijia.nj 这里precision应该算上小数点的一位, ob_schama_macro_define.h中也要做相应修改
        data_type.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + scale));
        data_type.set_scale(scale);
      }
      break;
    case ObDateTC:
      // nothing to do.
      data_type.set_precision(default_accuracy.get_precision());
      data_type.set_scale(default_accuracy.get_scale());
      break;
    case ObTimeTC:
      if (scale > OB_MAX_DATETIME_PRECISION) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, ident_name.ptr(), OB_MAX_DATETIME_PRECISION);
      } else {
        if (scale < 0) {
          scale = default_accuracy.get_scale();
        }
        // TODO@nijia.nj 这里precision应该算上小数点的一位, ob_schama_macro_define.h中也要做相应修改
        data_type.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + scale));
        data_type.set_scale(scale);
      }
      break;
    case ObYearTC:
      data_type.set_precision(default_accuracy.get_precision());
      data_type.set_scale(default_accuracy.get_scale());
      // nothing to do.
      break;
    case ObStringTC:
      data_type.set_length(length);
      if (length < -1) { // length is more than 32 bit
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        LOG_WARN("column data length is invalid", K(ret), K(length), K(data_type));
        LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, ident_name.ptr(),
            static_cast<int>((ObVarcharType == data_type.get_obj_type() || ObNVarchar2Type == data_type.get_obj_type())
                ? OB_MAX_ORACLE_VARCHAR_LENGTH :
                (is_for_pl_type ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE : OB_MAX_ORACLE_CHAR_LENGTH_BYTE)));
      } else if (ObVarcharType != data_type.get_obj_type()
          && ObCharType != data_type.get_obj_type()
          && ObNVarchar2Type != data_type.get_obj_type()
          && ObNCharType != data_type.get_obj_type()) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(ERROR,"column type must be ObVarcharType or ObCharType", K(ret));
      } else if (type_node.int32_values_[1]/*is binary*/) {
        if (is_for_pl_type && ObVarcharType == data_type.get_obj_type()
                          && OB_MAX_MYSQL_VARCHAR_LENGTH < length) {
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("column data length is invalid", K(ret), K(length), K(data_type));
          LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, ident_name.ptr(),
                        static_cast<int>(OB_MAX_MYSQL_VARCHAR_LENGTH));
        } else {
          data_type.set_charset_type(CHARSET_BINARY);
          data_type.set_collation_type(CS_TYPE_BINARY);
        }
      } else if (OB_FAIL(resolve_str_charset_info(type_node, data_type, is_for_pl_type))) {
        SQL_RESV_LOG(WARN, "fail to resolve string charset and collation", K(ret), K(data_type));
      } else if (is_oracle_mode && data_type.get_meta_type().get_collation_type() != CS_TYPE_ANY) {
        int64_t nchar_mbminlen = 0;
        ObCollationType cs_type = ob_is_nstring_type(data_type.get_obj_type()) ?
                      nls_session_param.nls_nation_collation_ : nls_session_param.nls_collation_;

        if (OB_UNLIKELY(0 == length)) {
          ret = OB_ERR_ZERO_LEN_COL;
          LOG_WARN("Oracle not allowed zero length", K(ret));
        } else if (OB_FAIL(ObCharset::get_mbminlen_by_coll(
                            nls_session_param.nls_nation_collation_, nchar_mbminlen))) {
          LOG_WARN("fail to get mbminlen of nchar", K(ret), K(nls_session_param));
        } else if (((ObVarcharType == data_type.get_obj_type() || ObNVarchar2Type == data_type.get_obj_type()) && OB_MAX_ORACLE_VARCHAR_LENGTH < length)
                  || (ObCharType == data_type.get_obj_type()
                  && (is_for_pl_type ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE < length :
                                        OB_MAX_ORACLE_CHAR_LENGTH_BYTE < length))
                  || (ObNCharType == data_type.get_obj_type()
                      && (is_for_pl_type ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE < length * nchar_mbminlen:
                                            OB_MAX_ORACLE_CHAR_LENGTH_BYTE < length * nchar_mbminlen))) {
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("column data length is invalid",
                  K(ret), K(length), K(data_type), K(nchar_mbminlen));
          LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, ident_name.ptr(),
            static_cast<int>((ObVarcharType == data_type.get_obj_type() || ObNVarchar2Type == data_type.get_obj_type())
                ? OB_MAX_ORACLE_VARCHAR_LENGTH :
                (is_for_pl_type ? OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE : OB_MAX_ORACLE_CHAR_LENGTH_BYTE)));
        } else if (type_node.length_semantics_ == LS_DEFAULT) {
          data_type.set_length_semantics(nls_session_param.nls_length_semantics_);
        } else if (OB_UNLIKELY(type_node.length_semantics_ != LS_BYTE && type_node.length_semantics_ != LS_CHAR)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("length_semantics_ is invalid", K(ret), K(type_node.length_semantics_));
        } else {
          data_type.set_length_semantics(type_node.length_semantics_);
        }
        data_type.set_charset_type(ObCharset::charset_type_by_coll(cs_type));
        data_type.set_collation_type(cs_type);
        LOG_DEBUG("check data type after resolve", K(ret), K(data_type));
      } else if (!is_oracle_mode && ObCharType == data_type.get_obj_type()
                                && OB_MAX_CHAR_LENGTH < length) {
        // varchar length check , TODO:
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        LOG_WARN("column data length is invalid", K(ret), K(length), K(data_type));
        LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, ident_name.ptr(),
                      static_cast<int>(OB_MAX_CHAR_LENGTH));
      } else {}
      break;
    case ObRawTC:
      data_type.set_length(length);
      data_type.set_charset_type(CHARSET_BINARY);
      data_type.set_collation_type(CS_TYPE_BINARY);
      break;
    case ObTextTC:
    case ObLobTC:
      data_type.set_length(length);
      data_type.set_scale(default_accuracy.get_scale());
      if (type_node.int32_values_[1]/*is binary*/) {
        data_type.set_charset_type(CHARSET_BINARY);
        data_type.set_collation_type(CS_TYPE_BINARY);
      } else if (OB_FAIL(resolve_str_charset_info(type_node, data_type, is_for_pl_type))) {
        SQL_RESV_LOG(WARN, "fail to resolve string charset and collation", K(ret), K(data_type));
      } else {
        // do nothing
      }
      break;
    case ObJsonTC:
      if (is_oracle_mode && !is_for_pl_type) {
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (data_version < DATA_VERSION_4_1_0_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "create json column before cluster min version 4.1.");
        }
      }
      if (OB_SUCC(ret)) {
        data_type.set_length(length);
        data_type.set_scale(default_accuracy.get_scale());
        data_type.set_charset_type(CHARSET_UTF8MB4);
        data_type.set_collation_type(CS_TYPE_UTF8MB4_BIN); // ToDo: oracle, allow utf16
      }
      break;
    case ObGeometryTC: {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_1_0_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.1, geometry type");
      } else {
        data_type.set_length(length);
        data_type.set_scale(default_accuracy.get_scale());
        data_type.set_charset_type(CHARSET_BINARY);
        data_type.set_collation_type(CS_TYPE_BINARY);
      }
      break;
    }
    case ObBitTC:
      if (precision < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("precision of bit is negative", K(ret), K(precision));
      } else if (precision > OB_MAX_BIT_LENGTH) {
        ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH, ident_name.ptr(), OB_MAX_BIT_LENGTH);
      } else if (0 == precision ) {//暂时与5.6兼容，5.7中会报错
        data_type.set_precision(default_accuracy.get_precision());
        data_type.set_scale(default_accuracy.get_scale());
      } else {
        data_type.set_precision(precision);
        data_type.set_scale(default_accuracy.get_scale());
      }
      break;
    case ObEnumSetTC:
      if (OB_FAIL(resolve_str_charset_info(type_node, data_type, is_for_pl_type))) {
        LOG_WARN("fail to resolve column charset and collation", K(ident_name), K(ret));
      }
      break;
    case ObIntervalTC:
      if (is_oracle_mode && -86 == type_node.int16_values_[1]) {
          if (is_for_pl_type) {
            ret = OB_NUMERIC_PRECISION_NOT_INTEGER;
            LOG_USER_ERROR(OB_NUMERIC_PRECISION_NOT_INTEGER, (int)type_node.str_len_, type_node.str_value_);
            LOG_WARN("non-integral numeric literal value is inappropriate in this context", K(ret), K(scale), K(precision));
          } else {
            ret = OB_ERR_REQUIRE_INTEGER;
            LOG_USER_ERROR(OB_ERR_REQUIRE_INTEGER);
            LOG_WARN("non-integral numeric literal value is inappropriate in this context", K(ret), K(scale), K(precision));
          }
      } else if (data_type.get_meta_type().is_interval_ym()) {
        if (0 == type_node.int16_values_[1]) {
          data_type.set_scale(default_accuracy.get_scale());
        } else {
          if (!ObIntervalScaleUtil::scale_check(type_node.int16_values_[0])) {
            ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

          } else {
            ObScale scale = ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(
                  static_cast<int8_t>(type_node.int16_values_[0]));
            data_type.set_scale(scale);
          }
        }
      } else {  //interval ds
        int8_t day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(
              static_cast<int8_t>(default_accuracy.get_scale()));
        int8_t fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(
              static_cast<int8_t>(default_accuracy.get_scale()));
        if (OB_SUCC(ret) && 0 != type_node.int16_values_[1]) {
          if (!ObIntervalScaleUtil::scale_check(type_node.int16_values_[0])) {
            ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

          } else {
            day_scale = static_cast<int8_t>(type_node.int16_values_[0]);
          }
        }
        if (OB_SUCC(ret) && 0 != type_node.int16_values_[3]) {
          if (!ObIntervalScaleUtil::scale_check(type_node.int16_values_[2])) {
            ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

          } else {
            fs_scale = static_cast<int8_t>(type_node.int16_values_[2]);
          }
        }
        ObScale scale = ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(day_scale, fs_scale);
        data_type.set_scale(scale);
      }
      break;
    case ObRowIDTC:
      if (ob_is_urowid(data_type.get_obj_type())) {
        if (length > OB_MAX_USER_ROW_KEY_LENGTH) {
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("column data length is invalid", K(ret), K(length), K(data_type));
          LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, ident_name.ptr(),
            static_cast<int>(OB_MAX_USER_ROW_KEY_LENGTH));
        } else {
          data_type.set_length(length);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "signed rowid data type");
        SQL_RESV_LOG(WARN, "only support urowid type for now", K(ret), K(data_type));
      }
      break;
    case ObExtendTC:
      //to do: udt type compatibility
      // maybe we should not use udt type, or should change column schema
      // if (!is_for_pl_type) {
      //  data_type.set_obj_type(ObUserDefinedSQLType);
      //  data_type.set_subschema_id(ObXMLSqlType);
      //} else {
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (data_version < DATA_VERSION_4_2_0_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "create extend column before cluster min version 4.2.");
        } else if (is_oracle_mode) {
          data_type.set_length(length);
          data_type.set_charset_type(CHARSET_BINARY);
          data_type.set_collation_type(CS_TYPE_INVALID);
        }
      //}
      break;
    case ObRoaringBitmapTC: {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_3_2_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.3.2, roaringbitmap type");
      } else {
        data_type.set_length(length);
        data_type.set_scale(default_accuracy.get_scale());
        data_type.set_charset_type(CHARSET_BINARY);
        data_type.set_collation_type(CS_TYPE_BINARY);
      }
      break;
    }
    default:
      ret = OB_ERR_ILLEGAL_TYPE;
      SQL_RESV_LOG(WARN, "Unsupport data type of column definiton", K(ident_name), K(data_type), K(ret));
      break;
  }
  }
  LOG_DEBUG("resolve data type", K(ret), K(data_type), K(lbt()));
  return ret;
}

int ObResolverUtils::resolve_str_charset_info(const ParseNode &type_node,
                                              ObDataType &data_type,
                                              const bool is_for_pl_type)
{
  int ret = OB_SUCCESS;
  bool is_binary = false;
  ObString charset;
  ObString collation;
  ObCharsetType charset_type = CHARSET_INVALID;
  ObCollationType collation_type = CS_TYPE_INVALID;
  const ParseNode *charset_node = NULL;
  const ParseNode *collation_node = NULL;
  const ParseNode *binary_node = NULL;

  if (OB_ISNULL(type_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type node children is null");
  } else {
    charset_node = type_node.children_[0];
    collation_node = type_node.children_[1];
    if (type_node.num_child_ >= 3) { // CLOB only has two child node without binary_node
      binary_node = type_node.children_[2];
    }
  }
  if (OB_SUCC(ret) && NULL != binary_node) {
    is_binary = true;
  }
  if (OB_SUCC(ret) && NULL != charset_node) {
    charset.assign_ptr(charset_node->str_value_,
                       static_cast<int32_t>(charset_node->str_len_));
    if (lib::is_oracle_mode()) {
      if (is_for_pl_type && 0 == charset.case_compare("any_cs")) {
        charset_type = CHARSET_ANY;
        collation_type = CS_TYPE_ANY;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "set charset in oracle mode");
        LOG_WARN("set charset in oracle mode is not supported now", K(ret));
      }
    } else {
      if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(charset))) {
        ret = OB_ERR_UNKNOWN_CHARSET;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
      }
    }
  }
  if (OB_SUCC(ret) && NULL != collation_node) {
    if (lib::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "set collate in oracle mode");
      LOG_WARN("set collate in oracle mode is not supported now", K(ret));
    } else {
      collation.assign_ptr(collation_node->str_value_,
                           static_cast<int32_t>(collation_node->str_len_));
      if (CS_TYPE_INVALID == (collation_type = ObCharset::collation_type(collation))) {
        ret = OB_ERR_UNKNOWN_COLLATION;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
      }
    }
  }

  if (OB_SUCC(ret)) {
    data_type.set_charset_type(charset_type);
    data_type.set_collation_type(collation_type);
    data_type.set_binary_collation(is_binary);
  }
  return ret;
}

// WARNING: is_sync_ddl_user=true means outside program won't wait ddl, which is so misleading
int ObResolverUtils::check_sync_ddl_user(ObSQLSessionInfo *session_info, bool &is_sync_ddl_user)
{
  int ret = OB_SUCCESS;
  is_sync_ddl_user = false;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Empty pointer session_info", K(ret));
  } else {
    const ObString current_user(session_info->get_user_name());
    // if this is a pl inner sql, don't mark it as sync ddl user, otherwise ddl in pl will be async
    if ((session_info->is_inner() && nullptr == session_info->get_pl_context())
        || (ObCharset::case_insensitive_equal(current_user, OB_RESTORE_USER_NAME))
        || (ObCharset::case_insensitive_equal(current_user, OB_DRC_USER_NAME))) {
      is_sync_ddl_user = true;
    } else {
      is_sync_ddl_user = false;
    }
  }
  return ret;
}

bool ObResolverUtils::is_restore_user(ObSQLSessionInfo &session_info)
{
  int bret = false;
  const ObString current_user(session_info.get_user_name());
  if (ObCharset::case_insensitive_equal(current_user, OB_RESTORE_USER_NAME)) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

bool ObResolverUtils::is_drc_user(ObSQLSessionInfo &session_info)
{
  int bret = false;
  const ObString current_user(session_info.get_user_name());
  if (ObCharset::case_insensitive_equal(current_user, OB_DRC_USER_NAME)) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

int ObResolverUtils::set_sync_ddl_id_str(ObSQLSessionInfo *session_info, ObString &ddl_id_str)
{
  int ret = OB_SUCCESS;
  ddl_id_str.reset();

  bool is_sync_ddl_user = false;
  if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info, is_sync_ddl_user))) {
    LOG_WARN("Failed to check_sync_ddl_user", K(ret));
  } else if (session_info->is_inner()) {
    // do-nothing
  } else if (is_sync_ddl_user) {
    const ObString var_name(common::OB_DDL_ID_VAR_NAME);
    common::ObObj var_obj;
    if (OB_FAIL(session_info->get_user_variable_value(var_name, var_obj))) {
      if (OB_ERR_USER_VARIABLE_UNKNOWN == ret) {
        LOG_DEBUG("no __oceanbase_ddl_id user variable: ", K(ddl_id_str));
        ret = OB_SUCCESS; // 没有设置session变量，需要正常返回
      } else {
        LOG_WARN("failed to get value of __oceanbase_ddl_id user variable", K(ret), K(var_name));
      }
    } else {
      if (ob_is_string_type(var_obj.get_type())) {
        ddl_id_str = var_obj.get_string();
        LOG_DEBUG("__oceanbase_ddl_id user variable: ", K(ddl_id_str));
      } else {
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("data type of __oceanbase_ddl_id user variable is not string", K(ret), K(var_obj));
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_udf_name_by_parse_node(
  const ParseNode *node, const common::ObNameCaseMode case_mode, ObUDFInfo &udf_info)
{
  int ret = OB_SUCCESS;
  ObString udf_name;
  ObString package_name;
  ObString database_name;
  if (OB_ISNULL(node) || node->type_ != T_FUN_UDF) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse udf node is invalid", K(ret), K(node));
  } else if (OB_ISNULL(node->children_[0])) {
	    ret = OB_ERR_UNEXPECTED;
	    LOG_WARN("parse node is invalid", K(ret));
  } else {
    udf_info.udf_name_.assign_ptr(const_cast<char*>(node->children_[0]->str_value_),
        static_cast<int32_t>(node->children_[0]->str_len_));
    if (OB_NOT_NULL(node->children_[3])) {
      udf_info.udf_package_.assign_ptr(const_cast<char*>(node->children_[3]->str_value_),
          static_cast<int32_t>(node->children_[3]->str_len_));
    }
    if (OB_NOT_NULL(node->children_[2])) {
      udf_info.udf_database_.assign_ptr(const_cast<char*>(node->children_[2]->str_value_),
          static_cast<int32_t>(node->children_[2]->str_len_));
      bool perserve_lettercase = lib::is_oracle_mode() ?
          true : (case_mode != OB_LOWERCASE_AND_INSENSITIVE);
      if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(
          CS_TYPE_UTF8MB4_GENERAL_CI,
          perserve_lettercase,
          udf_info.udf_database_))) {
        LOG_WARN("fail convert database name", K(ret), K(udf_info));
      }
    }
  }
  return ret;
}

// for create table with fk in oracle mode
int ObResolverUtils::check_dup_foreign_keys_exist(
    const common::ObSArray<obrpc::ObCreateForeignKeyArg> &fk_args)
{
  int ret = OB_SUCCESS;

  for (int i = 0; OB_SUCC(ret) && (i < fk_args.count() - 1); ++i) {
    for (int j = i + 1; OB_SUCC(ret) && (j < fk_args.count()); ++j) {
      if (0 == fk_args.at(i).parent_database_.case_compare(fk_args.at(j).parent_database_)
          && 0 == fk_args.at(i).parent_table_.case_compare(fk_args.at(j).parent_table_)) {
        if (is_match_columns_with_order(
            fk_args.at(i).child_columns_, fk_args.at(i).parent_columns_,
            fk_args.at(j).child_columns_, fk_args.at(j).parent_columns_)) {
          ret = OB_ERR_DUP_FK_IN_TABLE;
          LOG_WARN("duplicate fks in table", K(ret), K(i), K(j), K(fk_args));
        }
      }
    }
  }

  return ret;
}

// for alter table add fk in oracle mode
int ObResolverUtils::check_dup_foreign_keys_exist(
    const common::ObIArray<share::schema::ObForeignKeyInfo> &fk_infos,
    const common::ObIArray<uint64_t> &child_column_ids,
    const common::ObIArray<uint64_t> &parent_column_ids,
    const uint64_t parent_table_id,
    const uint64_t child_table_id)
{
  int ret = OB_SUCCESS;

  for (int i = 0; OB_SUCC(ret) && (i < fk_infos.count()); ++i) {
    if ((parent_table_id == fk_infos.at(i).parent_table_id_)
        && (child_table_id == fk_infos.at(i).child_table_id_)) {
      if (is_match_columns_with_order(
          fk_infos.at(i).child_column_ids_, fk_infos.at(i).parent_column_ids_,
          child_column_ids, parent_column_ids)) {
        ret = OB_ERR_DUP_FK_EXISTS;
        LOG_WARN("duplicate fk already exists in the table", K(ret), K(i), K(fk_infos.at(i)));
      }
    }
  }

  return ret;
}

// description: 检查主表的外键列是否满足唯一约束或者主键约束
//
// @param [in] parent_table_schema  父表 schema
// @param [in] schema_checker       ObSchemaChecker
// @param [in] parent_columns       父表外键列的列名
// @param [in] index_arg_list       子表所有索引的 arg 构成的数组（注意：只有自引用的时候，index_arg_list 里才会有子表的索引信息）
// @param [out] is_match            主表的外键列是否满足唯一约束或者主键约束的检查结果
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObResolverUtils::foreign_key_column_match_uk_pk_column(const ObTableSchema &parent_table_schema,
                                                           ObSchemaChecker &schema_checker,
                                                           const ObIArray<ObString> &parent_columns,
                                                           const ObSArray<ObCreateIndexArg> &index_arg_list,
                                                           const bool is_oracle_mode,
                                                           share::schema::ObConstraintType &ref_cst_type,
                                                           uint64_t &ref_cst_id,
                                                           bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  // 检查 parent columns（父表中的外键列） 是否和 rowkey columns（父表中的主键列） 匹配
  // 其实就是在检查父表的外键列在父表中是否是主键
  const ObRowkeyInfo &rowkey_info = parent_table_schema.get_rowkey_info();
  // 通过 rowkey_info 把父表的主键列列名拿出来，然后放到 pk_columns 里面
  common::ObSEArray<ObString, 8> pk_columns;

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    uint64_t column_id = 0;
    const ObColumnSchemaV2 *col_schema = NULL;
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get rowkey info", K(ret), K(i), K(rowkey_info));
    } else if (NULL == (col_schema = parent_table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index column schema failed", K(ret));
    } else if (col_schema->is_hidden() || col_schema->is_shadow_column()) {
      // do nothing
    } else if(OB_FAIL(pk_columns.push_back(col_schema->get_column_name()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("push back index column failed", K(ret));
    } else { } // do nothing
  }
  if (OB_SUCC(ret)) {
    // 检查父表外键列是否和主键列匹配
    if (OB_FAIL(check_match_columns(parent_columns, pk_columns, is_match))) {
      LOG_WARN("Failed to check_match_columns", K(ret));
    } else if (is_match) {
      // 不需要再对 uk 列进行配对检查，因为 parent columns 已经和父表的主键列相匹配
      ref_cst_type = CONSTRAINT_TYPE_PRIMARY_KEY;
      if (is_oracle_mode) {
        for (ObTableSchema::const_constraint_iterator iter = parent_table_schema.constraint_begin(); iter != parent_table_schema.constraint_end(); ++iter) {
          if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()) {
            ref_cst_id = (*iter)->get_constraint_id();
            break;
          }
        }
      }
    } else if (index_arg_list.count() > 0) {
      // 只有 create table 时创建自引用外键的时候， index_arg_list 里才会有子表的索引信息
      // 当出现自依赖的情况, 父表的 index 信息需要从子表的 CreateTableArg 中获取,
      // 因为父表和子表为同一张表，所以父表信息此时还没有 publish 到 table schema 中
      // 现在把子表里的所有索引依次拿出来和父表的外键列作比较，查看是否满足自引用
      for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < index_arg_list.count(); ++i) {
        SMART_VAR(ObCreateIndexArg, index_arg) {
          if (OB_FAIL(index_arg.assign(index_arg_list.at(i)))) {
            LOG_WARN("fail to assign schema", K(ret));
          } else if (INDEX_TYPE_UNIQUE_LOCAL == index_arg.index_type_
              || INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
            ObSEArray<ObString, 8> uk_columns;
            // 通过 index_arg 把子表有唯一约束的列的列名拿出来，然后放到 uk_columns 里面
            for (int64_t j = 0; OB_SUCC(ret) && j < index_arg.index_columns_.count(); ++j) {
              const ObColumnSortItem &sort_item = index_arg.index_columns_.at(j);
              if(OB_FAIL(uk_columns.push_back(sort_item.column_name_))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("push back index column failed", K(ret), K(sort_item.column_name_));
              }
            }
            if (OB_FAIL(check_match_columns(parent_columns, uk_columns, is_match))) {
              LOG_WARN("Failed to check_match_columns", K(ret));
            } else if (is_match) {
              ref_cst_type = CONSTRAINT_TYPE_UNIQUE_KEY;
              // RS 端会在 ObDDLService::get_uk_cst_id_for_self_ref 填上 arg.ref_cst_id_
            }
          }
        }
      }
    } else {
      // 如果外键列不是参考父表的 pk 列，那么还需要继续比较是否是父表的 uk 列
      // 检查 parent columns 是否和父表的 unique key columns 匹配
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(parent_table_schema.get_simple_index_infos(simple_index_infos))) {
        LOG_WARN("get simple_index_infos failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < simple_index_infos.count(); ++i) {
        const ObTableSchema *index_table_schema = NULL;
        if (OB_FAIL(schema_checker.get_table_schema(parent_table_schema.get_tenant_id(), simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema should not be null", K(ret));
        } else if (index_table_schema->is_unique_index()) {
          const ObColumnSchemaV2 *index_col = NULL;
          const ObIndexInfo &index_info = index_table_schema->get_index_info();
          ObSEArray<ObString, 8> uk_columns;
          for (int64_t i = 0; OB_SUCC(ret) && i < index_info.get_size(); ++i) {
            if (OB_ISNULL(index_col = index_table_schema->get_column_schema(index_info.get_column(i)->column_id_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get index column schema failed", K(ret));
            } else if (index_col->is_hidden() || index_col->is_shadow_column()) {
              // do nothing
            } else if(OB_FAIL(uk_columns.push_back(index_col->get_column_name()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("push back index column failed", K(ret));
            } else { } // do nothing
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(check_match_columns(parent_columns, uk_columns, is_match))) {
              LOG_WARN("Failed to check_match_columns", K(ret));
            } else if (is_match) {
              ref_cst_type = CONSTRAINT_TYPE_UNIQUE_KEY;
              ref_cst_id = index_table_schema->get_table_id();
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_self_reference_fk_columns_satisfy(
    const obrpc::ObCreateForeignKeyArg &arg)
{
  int ret = OB_SUCCESS;
  if (arg.parent_columns_.empty() || arg.child_columns_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg.parent_columns_), K(arg.child_columns_));
  } else {
    // 如果属于自引用，则参考列必须都不同
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.parent_columns_.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < arg.child_columns_.count(); ++j) {
        if (0 == arg.parent_columns_.at(i).compare(arg.child_columns_.at(j))) {
          ret = OB_ERR_CANNOT_ADD_FOREIGN;
          LOG_WARN("cannot support that parent column is in child column list", K(ret), K(arg));
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_foreign_key_set_null_satisfy(
    const obrpc::ObCreateForeignKeyArg &arg,
    const share::schema::ObTableSchema &child_table_schema,
    const bool is_mysql_compat_mode)
{
  int ret = OB_SUCCESS;
  if (arg.delete_action_ == ACTION_SET_NULL || arg.update_action_ == ACTION_SET_NULL) {
    // To compatible with oracle and mysql, check if set null ref action is valid
    // More detail can be found in:
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.child_columns_.count(); ++i) {
      const ObString &fk_col_name = arg.child_columns_.at(i);
      const ObColumnSchemaV2 *fk_col_schema = child_table_schema.get_column_schema(fk_col_name);
      if (OB_ISNULL(fk_col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("foreign key column schema is null", K(ret), K(i));
      } else if (fk_col_schema->is_generated_column()) {
        ret = OB_ERR_UNSUPPORTED_FK_SET_NULL_ON_GENERATED_COLUMN;
        LOG_WARN("foreign key column is generated column", K(ret), K(i), K(fk_col_name));
      } else if (!fk_col_schema->is_nullable() && is_mysql_compat_mode) {
        ret = OB_ERR_FK_COLUMN_NOT_NULL;
        LOG_USER_ERROR(OB_ERR_FK_COLUMN_NOT_NULL, to_cstring(fk_col_name), to_cstring(arg.foreign_key_name_));
      } else if (is_mysql_compat_mode) {
        // check if fk column is base column of virtual generated column in MySQL mode
        const uint64_t fk_col_id = fk_col_schema->get_column_id();
        bool is_stored_base_col = false;
        if (OB_FAIL(child_table_schema.check_is_stored_generated_column_base_column(fk_col_id, is_stored_base_col))) {
          LOG_WARN("failed to check foreign key column is virtual generated column base column", K(ret), K(i));
        } else if (is_stored_base_col) {
          ret = OB_ERR_CANNOT_ADD_FOREIGN;
          LOG_WARN("foreign key column is the base column of stored generated column in mysql mode", K(ret), K(i), K(fk_col_name));
        }
      }
    }
  }
  return ret;
}

// description: oracle 模式下检查两个外键子表中的外键列和父表中的关联列匹配关系是否一致
// oracle 模式下 (c1, c2) references t1(c1, c2) 和 (c2, c1) references t1(c2, c1) 被认为是相同外键
//
// eg: in oracle mode
// create table t1(c1 int, c2 int, primary key(c1, c2));
//
//
// create table t2(c1 int, c2 int,
//                 constraint fk foreign key (c1, c2) references t1(c1, c2),
//                 constraint fk2 foreign key (c2, c1) references t1(c2, c1));
// ORA-02274: duplicate referential constraint specifications
//
// @return oceanbase error code defined in lib/ob_errno.def
bool ObResolverUtils::is_match_columns_with_order(
     const common::ObIArray<ObString> &child_columns_1,
     const common::ObIArray<ObString> &parent_columns_1,
     const common::ObIArray<ObString> &child_columns_2,
     const common::ObIArray<ObString> &parent_columns_2)
{
  bool dup_foreign_keys_exist = true;

  if (child_columns_1.count() != child_columns_2.count()) {
    dup_foreign_keys_exist = false;
  } else {
    for (int i = 0; dup_foreign_keys_exist && (i < child_columns_1.count()); ++i) {
      int j = 0;
      bool find_same_child_column = false;
      for (j = 0; !find_same_child_column && (j < child_columns_2.count()); ++j) {
        if (0 == child_columns_1.at(i).case_compare(child_columns_2.at(j))) {
          find_same_child_column = true;
          if (0 != parent_columns_1.at(i).case_compare(parent_columns_2.at(j))) {
            dup_foreign_keys_exist = false;
          }
        }
      }
      if (!find_same_child_column) {
        dup_foreign_keys_exist = false;
      }
    }
  }

  return dup_foreign_keys_exist;
}

bool ObResolverUtils::is_match_columns_with_order(
     const common::ObIArray<uint64_t> &child_column_ids_1,
     const common::ObIArray<uint64_t> &parent_column_ids_1,
     const common::ObIArray<uint64_t> &child_column_ids_2,
     const common::ObIArray<uint64_t> &parent_column_ids_2)
{
  bool dup_foreign_keys_exist = true;

  if (child_column_ids_1.count() != child_column_ids_2.count()) {
    dup_foreign_keys_exist = false;
  } else {
    for (int i = 0; dup_foreign_keys_exist && (i < child_column_ids_1.count()); ++i) {
      int j = 0;
      bool find_same_child_column = false;
      for (j = 0; !find_same_child_column && (j < child_column_ids_2.count()); ++j) {
        if (child_column_ids_1.at(i) == child_column_ids_2.at(j)) {
          find_same_child_column = true;
          if (parent_column_ids_1.at(i) != parent_column_ids_2.at(j)) {
            dup_foreign_keys_exist = false;
          }
        }
      }
      if (!find_same_child_column) {
        dup_foreign_keys_exist = false;
      }
    }
  }

  return dup_foreign_keys_exist;
}

// description: 检查子表中的外键列和父表中的主键列或者唯一索引列是否匹配, 顺序可以不一致
//              eg: parent table: unique key(c1, c2)
//                  child table:  references(c2, c1)
//
// @param [in] parent_columns  父表中的外键列名
// @param [in] key_columns     父表中的主键列名或者唯一索引列名
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObResolverUtils::check_match_columns(const ObIArray<ObString> &parent_columns,
                                         const ObIArray<ObString> &key_columns,
                                         bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObSEArray<ObString, 8> tmp_parent_columns;
  ObSEArray<ObString, 8> tmp_key_columns;
  if (parent_columns.count() == key_columns.count() && parent_columns.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_columns.count(); ++i) {
      if(OB_FAIL(tmp_parent_columns.push_back(parent_columns.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else if(OB_FAIL(tmp_key_columns.push_back(key_columns.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (tmp_parent_columns.count() == tmp_key_columns.count()
            && tmp_parent_columns.count() > 0) {
        lib::ob_sort(tmp_parent_columns.begin(), tmp_parent_columns.end());
        lib::ob_sort(tmp_key_columns.begin(), tmp_key_columns.end());
        bool is_tmp_match = true;
        for (int64_t i = 0; is_tmp_match && i < tmp_parent_columns.count(); ++i) {
          if (0 != tmp_parent_columns.at(i).case_compare(tmp_key_columns.at(i))) {
            is_tmp_match = false;
          }
        }
        if (is_tmp_match) {
          is_match = true;
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_match_columns(
    const common::ObIArray<uint64_t> &parent_columns,
    const common::ObIArray<uint64_t> &key_columns,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObSEArray<uint64_t, 8> tmp_parent_columns;
  ObSEArray<uint64_t, 8> tmp_key_columns;
  if (parent_columns.count() == key_columns.count() && parent_columns.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_columns.count(); ++i) {
      if(OB_FAIL(tmp_parent_columns.push_back(parent_columns.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else if(OB_FAIL(tmp_key_columns.push_back(key_columns.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (tmp_parent_columns.count() == tmp_key_columns.count()
          && tmp_parent_columns.count() > 0) {
        lib::ob_sort(tmp_parent_columns.begin(), tmp_parent_columns.end());
        lib::ob_sort(tmp_key_columns.begin(), tmp_key_columns.end());
        bool is_tmp_match = true;
        for (int64_t i = 0; is_tmp_match && i < tmp_parent_columns.count(); ++i) {
          if (tmp_parent_columns.at(i) != tmp_key_columns.at(i)) {
            is_tmp_match = false;
          }
        }
        if (is_tmp_match) {
          is_match = true;
        }
      }
    }
  }
  return ret;
}

// description: 用于在 oracle 模式下检查同一表中的主键列和唯一约束列是否完全匹配, 各个列的顺序必须一致才算完全匹配
//              eg: index(c1, c2) 和 index(c2, c1) 认为是匹配
//                  index(c1, c2) 和 index(c1, c2) 认为是不匹配
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObResolverUtils::check_match_columns_strict(const ObIArray<ObString> &columns_array_1,
                                                const ObIArray<ObString> &columns_array_2,
                                                bool &is_match)
{
  int ret = OB_SUCCESS;
  bool is_tmp_match = true;
  is_match = false;
  if (columns_array_1.count() == columns_array_2.count() && columns_array_1.count() > 0) {
    for (int64_t i = 0; is_tmp_match && i < columns_array_1.count(); ++i) {
      if (columns_array_1.at(i) != columns_array_2.at(i)) {
        is_tmp_match = false;
      }
    }
    if (is_tmp_match) {
      is_match = true;
    }
  }
  return ret;
}

// description: 用于在 oracle 模式下检查同一表中的两个索引的列是否完全匹配, 各列的顺序一致且各列的 order 顺序一致才算完全匹配
//              eg: index(c1, c2) 和 index(c2, c1) 认为是两个不同的索引
//                  index(c1, c2 asc) 和 index(c1, c2 desc) 认为是两个不同的索引
//                  index(c1 desc, c2) 和 index(c1 desc, c2) 认为是两个相同的索引
//                  index(c1) 和 index(C1) oracle 认为是两个不同的索引
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObResolverUtils::check_match_columns_strict_with_order(const ObTableSchema *index_table_schema,
                                                           const ObCreateIndexArg &create_index_arg,
                                                           bool &is_match)
{
  int ret = OB_SUCCESS;
  bool is_tmp_match = true;
  ObString tmp_col_name_1;
  ObString tmp_col_name_2;
  ObOrderType tmp_order_type_1;
  ObOrderType tmp_order_type_2;
  const ObColumnSchemaV2 *tmp_index_col = NULL;
  const ObIndexInfo &index_info = index_table_schema->get_index_info();
  is_match = false;
  bool is_oracle_mode = false;
  if (OB_FAIL(index_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", K(ret));
  } else if (index_info.get_size() == create_index_arg.index_columns_.count()
      && create_index_arg.index_columns_.count() > 0) {
    for (int64_t idx = 0; is_tmp_match && idx < index_info.get_size(); ++idx) {
      if (OB_ISNULL(tmp_index_col = index_table_schema->get_column_schema(index_info.get_column(idx)->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get index column schema failed", K(ret));
      } else {
        tmp_col_name_1 = tmp_index_col->get_column_name_str();
        tmp_order_type_1 = tmp_index_col->get_order_in_rowkey();
        tmp_col_name_2 = create_index_arg.index_columns_.at(idx).column_name_;
        tmp_order_type_2 = create_index_arg.index_columns_.at(idx).order_type_;
        const bool name_eq = is_oracle_mode ? tmp_col_name_1 == tmp_col_name_2 : 0 == tmp_col_name_1.case_compare(tmp_col_name_2);
        if (!name_eq || (tmp_order_type_1 != tmp_order_type_2)) {
          is_tmp_match = false;
        }
      }
    }
    if (is_tmp_match) {
      is_match = true;
    }
  }

  return ret;
}

int ObResolverUtils::check_pk_idx_duplicate(const ObTableSchema &table_schema,
                                            const ObCreateIndexArg &create_index_arg,
                                            const ObIArray<ObString> &input_uk_columns_name,
                                            bool &is_match)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey = table_schema.get_rowkey_info();
  const ObColumnSchemaV2 *column = NULL;
  uint64_t column_id = OB_INVALID_ID;
  ObSEArray<ObString, 8> pk_columns_name;
  is_match = false;

  // generate pk_columns_name_array
  for (int64_t rowkey_idx = 0; rowkey_idx < rowkey.get_size(); ++rowkey_idx) {
    if (OB_FAIL(rowkey.get_column_id(rowkey_idx, column_id))) {
      LOG_WARN("fail to get column id", K(ret));
    } else if (OB_ISNULL(column = table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(ret), K(column_id), K(rowkey));
    } else if (column->is_hidden()) {
      // skip hidden pk col
    } else if (OB_FAIL(pk_columns_name.push_back(column->get_column_name_str()))){
      LOG_WARN("fail to push back to pk_columns_name array", K(ret), K(rowkey_idx), K(column_id), K(column->get_column_name_str()));
    }
  }
  // check if pk uk duplicate
  if (OB_SUCC(ret) && rowkey.get_size() != 0) {
    if (OB_FAIL(ObResolverUtils::check_match_columns_strict(input_uk_columns_name, pk_columns_name, is_match))) {
      LOG_WARN("Failed to check_match_columns", K(ret));
    } else if (is_match) {
      for (int64_t i = 0; is_match && i < create_index_arg.index_columns_.count(); ++i) {
        if (ObOrderType::DESC == create_index_arg.index_columns_.at(i).order_type_) {
          // 主键默认的列 order 顺序是 ASC，如果 uk 存在一列的 order 顺序为 DESC，则认为不匹配
          is_match = false;
        }
      }
    }
  }

  return ret;
}

// description: 检查创建外键时父表和子表的外键列的列数和类型是否匹配
//
// @param [in] child_table_schema   子表的 schema
// @param [in] parent_table_schema  父表的 schema
// @param [in] child_columns        子表外键列各个列的列名
// @param [in] parent_columns       父表外键列各个列的列名

// @return oceanbase error code defined in lib/ob_errno.def
int ObResolverUtils::check_foreign_key_columns_type(const bool is_mysql_compat_mode,
                                                    const ObTableSchema &child_table_schema,
                                                    const ObTableSchema &parent_table_schema,
                                                    const ObIArray<ObString> &child_columns,
                                                    const ObIArray<ObString> &parent_columns,
                                                    const share::schema::ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  // 子表和父表外键列的列数必须相同
  if (child_columns.count() != parent_columns.count()) {
    ret = OB_ERR_WRONG_FK_DEF;
    LOG_WARN("the count of foreign key columns is not equal to the count of reference columns", K(ret), K(child_columns.count()), K(parent_columns.count()));
  } else if (child_columns.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    LOG_WARN("the count of foreign key columns should be between [1,64]", K(ret), K(child_columns.count()), K(parent_columns.count()));
  } else {
    uint64_t child_table_id = child_table_schema.get_table_id();
    uint64_t parent_table_id = parent_table_schema.get_table_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_columns.count(); ++i) {
      if (child_table_id == parent_table_id && 0 == parent_columns.at(i).compare(child_columns.at(i))) {
        ret = OB_ERR_CANNOT_ADD_FOREIGN;
        LOG_WARN("Child table is same as parent table and child column is same as parant column", K(ret), K(child_table_id), K(parent_table_id), K(parent_columns.at(i)), K(child_columns.at(i)));
      } else {
        const ObColumnSchemaV2 *child_col = NULL;
        const ObColumnSchemaV2 *parent_col = parent_table_schema.get_column_schema(parent_columns.at(i));
        if (NULL == column) { // table-level fk
          child_col = child_table_schema.get_column_schema(child_columns.at(i));
        } else { // column level fk
          child_col = column;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(child_col)) {
          ret = OB_ERR_COLUMN_NOT_FOUND;
          LOG_WARN("child column is not exist", K(ret));
        } else if (OB_ISNULL(parent_col)) {
          ret = OB_ERR_COLUMN_NOT_FOUND;
          LOG_WARN("parent column is not exist", K(ret));
        } else if ((child_col->get_data_type() != parent_col->get_data_type())
                      && !is_synonymous_type(child_col->get_data_type(),
                                              parent_col->get_data_type())) {
          // 这里类型必须相同的
          ret = OB_ERR_CANNOT_ADD_FOREIGN;
          LOG_WARN("Column data types between child table and parent table are different", K(ret),
              K(child_col->get_data_type()),
              K(parent_col->get_data_type()));
        } else if (ob_is_string_type(child_col->get_data_type())) {
          // 列类型一致，对于数据宽度要求子表大于父表, 目前只考虑 string 类型,
          if (child_col->get_collation_type() != parent_col->get_collation_type()) {
            ret = OB_ERR_CANNOT_ADD_FOREIGN;
            LOG_WARN("The collation types are different", K(ret),
                K(child_col->get_collation_type()),
                K(parent_col->get_collation_type()));
          } else if (is_mysql_compat_mode &&
                    (child_col->get_data_length() < parent_col->get_data_length())) {
            ret = OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK;
            LOG_USER_ERROR(OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK,
                child_col->get_column_name_str().length(),
                child_col->get_column_name_str().ptr(),
                parent_col->get_column_name_str().length(),
                parent_col->get_column_name_str().ptr());
          } else { } // 对于其他bit/int/number /datetime/time/year 不做data_length要求
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::transform_sys_func_to_objaccess(
  ObIAllocator *allocator, const ParseNode *sys_func, ParseNode *&obj_access)
{
  int ret = OB_SUCCESS;
  if (allocator == nullptr || sys_func == nullptr || sys_func->type_ != T_FUN_SYS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func_sys node is null", K(ret));
  } else if (OB_ISNULL(obj_access
      = new_non_terminal_node(allocator, T_OBJ_ACCESS_REF, 2, sys_func, nullptr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("make T_OBJ_ACCESS node failed", K(ret));
  }
  return ret;
}

int ObResolverUtils::transform_func_sys_to_udf(ObIAllocator *allocator, const ParseNode *func_sys,
                                               const ObString &db_name, const ObString &pkg_name,
                                               ParseNode *&func_udf)
{
  int ret = OB_SUCCESS;
  ParseNode *db_name_node = NULL;
  ParseNode *pkg_name_node = NULL;
  func_udf = NULL;
  if (OB_ISNULL(allocator) || OB_ISNULL(func_sys)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator or sys func node is null", K(ret));
  } else if (OB_ISNULL(func_udf = new_non_terminal_node(allocator, T_FUN_UDF, 4, nullptr, nullptr, nullptr, nullptr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("make T_FUN_UDF node failed", K(ret));
  } else {
    //assign db name node
    if (!db_name.empty()) {
      if (OB_ISNULL(db_name_node = new_terminal_node(allocator, T_IDENT))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("make db name T_IDENT node failed", K(ret));
      } else {
        func_udf->children_[2] = db_name_node;
        db_name_node->str_value_ = parse_strndup(db_name.ptr(), db_name.length(), allocator);
        if (OB_ISNULL(db_name_node->str_value_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("copy db name failed", K(ret));
        } else {
          db_name_node->str_len_ = db_name.length();
        }
      }
    }

    //assign pkg name node
    if (!pkg_name.empty()) {
      if (OB_ISNULL(pkg_name_node = new_terminal_node(allocator, T_IDENT))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("make pkg name T_IDENT node failed", K(ret));
      } else {
        func_udf->children_[3] = pkg_name_node;
        pkg_name_node->str_value_ = parse_strndup(pkg_name.ptr(), pkg_name.length(), allocator);
        if (OB_ISNULL(pkg_name_node->str_value_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("copy pkg name failed", K(ret));
        } else {
          pkg_name_node->str_len_ = pkg_name.length();
        }
      }
    }

    if (OB_SUCC(ret)) {
      //sys node and udf node have the same memory life cycle
      //we share the func name and param node;
      func_udf->children_[0] = func_sys->children_[0];  //func name
      if (2 <= func_sys->num_child_) {
        func_udf->children_[1] = func_sys->children_[1];  //func param
      } else {
        func_udf->children_[1] = NULL;
      }
    }
  }
  return ret;
}

int ObResolverUtils::get_columns_name_from_index_table_schema(const ObTableSchema &index_table_schema,
                                                              ObIArray<ObString> &index_columns_name)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *index_col = NULL;
  const ObIndexInfo &index_info = index_table_schema.get_index_info();
  index_columns_name.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < index_info.get_size(); ++i) {
    if (OB_ISNULL(index_col = index_table_schema.get_column_schema(index_info.get_column(i)->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index column schema failed", K(ret));
    } else if (index_col->is_hidden() || index_col->is_shadow_column()) {
      // do nothing
    } else if(OB_FAIL(index_columns_name.push_back(index_col->get_column_name()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("push back index column failed", K(ret));
    } else { } // do nothing
  }
  return ret;
}

int ObResolverUtils::resolve_string(const ParseNode *node, ObString &string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null");
  } else if (OB_UNLIKELY(T_VARCHAR != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_VARCHAR", "type", get_type_name(node->type_));
  } else if (OB_UNLIKELY(node->str_len_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty string");
  } else {
    string = ObString(node->str_len_, node->str_value_);
  }
  return ret;
}

// judge whether pdml stmt contain udf can parallel execute or not has two stage:
// stage1:check has dml write stmt or read/write package var info in this funciton;
// stage2:record udf has select stmt info, and when optimize this stmt,
// according outer stmt type to determine, if udf has select stmt:
// case1: if outer stmt is select, can paralllel
// case2: if outer stmt is dml write stmt, forbid parallel
int ObResolverUtils::set_parallel_info(sql::ObSQLSessionInfo &session_info,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       ObRawExpr &expr,
                                       ObQueryCtx &ctx,
                                       ObIArray<ObSchemaObjVersion> &return_value_version)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  ObUDFRawExpr &udf_raw_expr = static_cast<ObUDFRawExpr&>(expr);

  if (udf_raw_expr.is_parallel_enable()) {
    //do nothing
  } else {
    uint64_t tenant_id = session_info.get_effective_tenant_id();
    bool enable_parallel = true;
    if (udf_raw_expr.get_is_udt_udf()) {
      tenant_id = pl::get_tenant_id_by_object_id(udf_raw_expr.get_pkg_id());
      OZ (schema_guard.get_routine_info_in_udt(tenant_id, udf_raw_expr.get_pkg_id(), udf_raw_expr.get_udf_id(), routine_info));
    } else if (udf_raw_expr.get_pkg_id() != OB_INVALID_ID) {
      tenant_id = pl::get_tenant_id_by_object_id(udf_raw_expr.get_pkg_id());
      OZ (schema_guard.get_routine_info_in_package(tenant_id, udf_raw_expr.get_pkg_id(), udf_raw_expr.get_udf_id(), routine_info));
    } else {
      OZ (schema_guard.get_routine_info(tenant_id,
                                        udf_raw_expr.get_udf_id(),
                                        routine_info));
      if (OB_FAIL(ret)) {
        ret = OB_ERR_PRIVATE_UDF_USE_IN_SQL;
        LOG_WARN("function 'string' may not be used in SQL", K(ret), K(udf_raw_expr));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(routine_info)) {
      if (!routine_info->is_valid() ||
          routine_info->is_modifies_sql_data() ||
          routine_info->is_wps() ||
          routine_info->is_rps() ||
          routine_info->is_has_sequence() ||
          routine_info->is_external_state()) {
        enable_parallel = false;
      }
      if (routine_info->is_reads_sql_data()) {
        ctx.udf_has_select_stmt_ = true;
      }
      /*
      create table t1(c0 int);
      create function f1() returns int deterministic
      begin
      insert into t1 value(2);
      insert into t1 value('dd');
      return 1;
      end

      create function f2() returns int deterministic
      begin
      set @a = f1();
      return 2;
      end
      f2 can not know whether f1 has dml, so if external_state is true, we assume f2 has dml */
      if (routine_info->is_modifies_sql_data() || routine_info->is_external_state()) {
        ctx.udf_has_dml_stmt_ = true;
      }
      OX (udf_raw_expr.set_parallel_enable(enable_parallel));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(routine_info)) {
      ObArenaAllocator alloc;
      ObPLDataType param_type;
      ObRoutineParam *param = nullptr;
      common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
      ObArray<ObSchemaObjVersion> version;
      CK (routine_info->get_routine_params().count() > 0);
      OX (param = routine_info->get_routine_params().at(0));
      CK (OB_NOT_NULL(sql_proxy));
      OZ (pl::ObPLDataType::transform_from_iparam(param,
                                                  schema_guard,
                                                  session_info,
                                                  alloc,
                                                  *sql_proxy,
                                                  param_type,
                                                  &return_value_version));
    }
  }
  return ret;
}


int ObResolverUtils::resolve_external_symbol(common::ObIAllocator &allocator,
                                             sql::ObRawExprFactory &expr_factory,
                                             sql::ObSQLSessionInfo &session_info,
                                             share::schema::ObSchemaGetterGuard &schema_guard,
                                             common::ObMySQLProxy *sql_proxy,
                                             ExternalParams *extern_param_info,
                                             pl::ObPLBlockNS *ns,
                                             ObQualifiedName &q_name,
                                             ObIArray<ObQualifiedName> &columns,
                                             ObIArray<ObRawExpr*> &real_exprs,
                                             ObRawExpr *&expr,
                                             pl::ObPLPackageGuard *package_guard,
                                             bool is_prepare_protocol,
                                             bool is_check_mode,
                                             bool is_sql_scope)
{
  int ret = OB_SUCCESS;
  if (NULL == package_guard) {
    // patch bugfix from 42x: 55397384
    if (NULL != session_info.get_cur_exec_ctx()) {
      OZ (session_info.get_cur_exec_ctx()->get_package_guard(package_guard));
      CK (OB_NOT_NULL(package_guard));
    } else {
      ret = OB_ERR_SP_UNDECLARED_VAR;
      LOG_WARN("exec context is NULL", K(ret));
      if (q_name.access_idents_.count() >= 0) {
        ret = OB_ERR_SP_UNDECLARED_VAR;
        LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR,
                       q_name.access_idents_.at(0).access_name_.length(),
                       q_name.access_idents_.at(0).access_name_.ptr());
      }
    }
  }

  if (OB_SUCC(ret)) {
    pl::ObPLResolver pl_resolver(allocator,
                                session_info,
                                schema_guard,
                                *package_guard,
                                NULL == sql_proxy ? (NULL == ns ? *GCTX.sql_proxy_ : ns->get_external_ns()->get_resolve_ctx().sql_proxy_) : *sql_proxy,
                                expr_factory,
                                NULL == ns ? NULL : ns->get_external_ns()->get_parent_ns(),
                                is_prepare_protocol,
                                is_check_mode,
                                is_sql_scope,
                                NULL/*param store*/,
                                extern_param_info);
    HEAP_VAR(pl::ObPLFunctionAST, func_ast, allocator) {
      if (OB_FAIL(pl_resolver.init(func_ast))) {
        LOG_WARN("pl resolver init failed", K(ret));
      } else if (NULL != ns) {
        pl_resolver.get_current_namespace() = *ns;
      } else { /*do nothing*/ }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(pl_resolver.resolve_qualified_name(q_name, columns, real_exprs, func_ast, expr))) {
          if (is_check_mode) {
            LOG_INFO("failed to resolve var", K(q_name), K(ret));
          } else {
            LOG_WARN_IGNORE_COL_NOTFOUND(ret, "failed to resolve var", K(q_name), K(ret));
          }
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid expr", K(expr), K(ret));
        } else if (!expr->is_const_raw_expr()
                    && !expr->is_obj_access_expr()
                    && !expr->is_sys_func_expr()
                    && !expr->is_udf_expr()
                    && T_FUN_PL_GET_CURSOR_ATTR != expr->get_expr_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr type is invalid", K(expr->get_expr_type()));
        } else if (OB_NOT_NULL(ns) && OB_NOT_NULL(ns->get_external_ns()) && !is_check_mode) {
          ObPLDependencyTable &src_dep_tbl = func_ast.get_dependency_table();
          for (int64_t i = 0; OB_SUCC(ret) && i < src_dep_tbl.count(); ++i) {
            OZ (ns->get_external_ns()->add_dependency_object(src_dep_tbl.at(i)));
          }
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::revert_external_param_info(ExternalParams &param_infos, ObRawExprFactory &expr_factory, ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (NULL == expr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *&child = expr->get_param_expr(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < param_infos.count(); ++j) {
        if (child == param_infos.at(j).element<1>()) {
          child = param_infos.at(j).element<0>();
          param_infos.at(j).element<2>()--;
          if (0 == param_infos.at(j).element<2>()) {
            ObConstRawExpr *null_expr = nullptr;
            if (OB_FAIL(expr_factory.create_raw_expr(T_NULL, null_expr))) {
              LOG_WARN("fail to create null expr", K(ret));
            } else {
              ObObjParam null_val;
              null_val.set_null();
              null_val.set_param_meta();
              null_expr->set_param(null_val);
              null_expr->set_value(null_val);
              param_infos.at(j).element<0>() = null_expr;
            }
          }
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(revert_external_param_info(param_infos, expr_factory, child))) {
          LOG_WARN("failed to revert external param info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_external_param_info(ExternalParams &param_infos,
                                                 const ObSQLSessionInfo &session_info,
                                                 ObRawExprFactory &expr_factory,
                                                 int64_t &prepare_param_count,
                                                 ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  int64_t same_idx = OB_INVALID_INDEX;
  if (param_infos.by_name_) {
    for (int64_t i = 0;
        OB_SUCC(ret) && OB_INVALID_INDEX == same_idx && i < param_infos.count();
        ++i) {
      ObRawExpr *original_expr = param_infos.at(i).element<0>();
      if (OB_ISNULL(original_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else if (original_expr->same_as(*expr)) {
        same_idx = i;
      } else { /*do nothing*/ }
    }
  }
#define SET_RESULT_TYPE(encode_num)  \
  do {   \
    if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory, encode_num, expr))) {   \
      LOG_WARN("create param expr failed", K(ret));   \
    } else if (OB_ISNULL(expr)) {   \
      ret = OB_ERR_UNEXPECTED;  \
      LOG_WARN("access idxs is empty", K(ret));  \
    } else {  \
      sql::ObExprResType result_type = expr->get_result_type();  \
      if (result_type.get_length() == -1) {   \
        if (result_type.is_varchar() || result_type.is_nvarchar2()) {  \
          result_type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);   \
        } else if (result_type.is_char() || result_type.is_nchar()) {  \
          result_type.set_length(OB_MAX_ORACLE_CHAR_LENGTH_BYTE);   \
        }  \
      }  \
      if (LS_INVALIED == result_type.get_length_semantics() && ob_is_string_tc(result_type.get_type())) {  \
        const ObLengthSemantics default_length_semantics =  LS_INVALIED != session_info.get_actual_nls_length_semantics() \
                                                             ? session_info.get_actual_nls_length_semantics()  \
                                                             : LS_BYTE;  \
        result_type.set_length_semantics(default_length_semantics);  \
      }  \
      expr->set_result_type(result_type); \
      param_expr = static_cast<ObConstRawExpr*>(expr); \
      const_cast<sql::ObExprResType &>(param_expr->get_result_type()).set_param(param_expr->get_value());  \
    }  \
  } while (0)

  if (OB_SUCC(ret)) {
    ObRawExpr *original_ref = expr;
    ObConstRawExpr *param_expr = nullptr;
    if (OB_INVALID_INDEX != same_idx) {
      if (param_infos.at(same_idx).element<2>() > 0) {
        expr = param_infos.at(same_idx).element<1>();
        param_infos.at(same_idx).element<2>()++;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref count unexpected", K(ret));
      }
    } else {
      int64_t available_idx = OB_INVALID_INDEX;
      for (int64_t i = 0;
          OB_SUCC(ret) && OB_INVALID_INDEX == available_idx && i < param_infos.count();
          ++i) {
        if (0 == param_infos.at(i).element<2>()) {
          available_idx = i;
        }
      }
      if (OB_INVALID_INDEX != available_idx) {
        int64_t encode_num = param_infos.at(available_idx).element<1>()->get_value().get_unknown();
        SET_RESULT_TYPE(encode_num);
        if (OB_SUCC(ret)) {
          param_infos.at(available_idx) = ExternalParamInfo(original_ref, param_expr, 1);
        }
      } else {
        /*
        * 把Stmt里的替换成QuestionMark，以便reconstruct_sql的时候会被打印成？，按prepare_param_count_编号
        * 如果原本就是QuestionMark，也需要重新生成一个按照prepare_param_count_从0开始编号的QuestionMark，
        * 以此保证传递给PL的参数顺序和prepare出来的参数化语句里的编号一致
        */
        SET_RESULT_TYPE(prepare_param_count++);
        if (OB_SUCC(ret)) {
          ExternalParamInfo param_info(original_ref, param_expr, 1);
          if (OB_FAIL(param_infos.push_back(param_info))) {
            LOG_WARN("push_back error", K(ret));
          }
        }
      }
    }
  }
#undef SET_RESULT_TYPE
  return ret;
}

int ObResolverUtils::uv_check_basic(ObSelectStmt &stmt, const bool is_insert)
{
  int ret = OB_SUCCESS;
  if (stmt.get_table_items().count() == 0) {
    // create view as select 1 a;
    ret = lib::is_mysql_mode() ? (is_insert ? OB_ERR_NON_INSERTABLE_TABLE : OB_ERR_NON_UPDATABLE_TABLE) : OB_ERR_ILLEGAL_VIEW_UPDATE;
    LOG_WARN("no table in select", K(ret));
  } else {
    if (lib::is_mysql_mode()) {
      if (stmt.has_group_by() || stmt.has_having() || stmt.get_aggr_item_size() > 0 || stmt.has_window_function()
          || stmt.is_distinct()
          || stmt.is_set_stmt()
          || stmt.has_limit()) {
        ret = is_insert ? OB_ERR_NON_INSERTABLE_TABLE : OB_ERR_NON_UPDATABLE_TABLE;
        LOG_WARN("not updatable", K(ret));
      }
    //oracle mode下，含有fetch的insert/update/delete统一报错，兼容oracle行为
    } else if (stmt.has_fetch()) {
      ret = OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
      LOG_WARN("subquery with fetch can't occur in insert/update/delete stmt", K(ret));
    } else {
      bool has_rownum = false;
      if (OB_FAIL(stmt.has_rownum(has_rownum))) {
        LOG_WARN("check select stmt has rownum failed", K(ret));
      }
      if (stmt.has_window_function()
          || stmt.is_set_stmt()
          || has_rownum
          || (!is_insert && (stmt.has_group_by() || stmt.has_having() || stmt.get_aggr_item_size() > 0))) {
        ret = OB_ERR_ILLEGAL_VIEW_UPDATE;
        LOG_WARN("not updatable", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_select_item_subquery(ObSelectStmt &stmt,
    bool &has_subquery, bool &has_dependent_subquery,
    const uint64_t base_tid, bool &ref_update_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr *, 1> query_exprs;
  FOREACH_CNT_X(item, stmt.get_select_items(), OB_SUCC(ret)) {
    if (OB_ISNULL(item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(item->expr_, query_exprs))) {
      LOG_WARN("extract sub query expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && query_exprs.count() > 0) {
    has_subquery = true;
    FOREACH_CNT_X(expr, query_exprs, OB_SUCC(ret) && !has_dependent_subquery) {
      if (OB_ISNULL(*expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else {
        ObSelectStmt *ref_stmt = (*expr)->get_ref_stmt();
        if (NULL != ref_stmt) {
          FOREACH_CNT_X(item, ref_stmt->get_table_items(), OB_SUCC(ret) && !ref_update_table) {
            if (OB_ISNULL(*item)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL table item", K(ret));
            } else if ((*item)->ref_id_ == base_tid) {
              ref_update_table = true;
            }
          }
          if (OB_SUCC(ret)) {
            has_dependent_subquery = (*expr)->has_exec_param();
          }
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::set_direction_by_mode(const ParseNode &sort_node, OrderItem &order_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_node.children_) || sort_node.num_child_ < 2 || OB_ISNULL(sort_node.children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (T_SORT_ASC == sort_node.children_[1]->type_) {
    if (lib::is_oracle_mode()) {
      if (1 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_LAST_ASC;
      } else if (2 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_FIRST_ASC;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value for null position", K(ret), K(sort_node.children_[1]->value_));
      }
    } else {
      order_item.order_type_ = NULLS_FIRST_ASC;
    }
  } else if (T_SORT_DESC == sort_node.children_[1]->type_) {
    if (lib::is_oracle_mode()) {
      if (1 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_LAST_DESC;
      } else if (2 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_FIRST_DESC;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value for null position", K(ret), K(sort_node.children_[1]->value_));
      }
    } else {
      order_item.order_type_ = NULLS_LAST_DESC;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sort type", K(ret), K(sort_node.children_[1]->type_));
  }
  return ret;
}

int ObResolverUtils::uv_check_select_item_subquery(const TableItem &table_item,
    bool &has_subquery, bool &has_dependent_subquery, bool &ref_update_table)
{
  int ret = OB_SUCCESS;
  const TableItem *item = &table_item;
  const uint64_t base_tid = table_item.get_base_table_item().ref_id_;
  while (OB_SUCC(ret) && NULL != item && item->is_generated_table() && !has_dependent_subquery) {
    if (OB_ISNULL(item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is NULL for generate table", K(ret));
    } else if (OB_FAIL(check_select_item_subquery(*item->ref_query_,
        has_subquery, has_dependent_subquery, base_tid, ref_update_table))) {
      LOG_WARN("check select item subquery failed", K(ret));
    } else {
      item = item->view_base_item_;
    }
  }
  return ret;
}

int ObResolverUtils::check_table_referred(ObSelectStmt &stmt, const uint64_t base_tid, bool &referred)
{
  int ret = OB_SUCCESS;
  OZ(check_stack_overflow());
  FOREACH_CNT_X(t, stmt.get_table_items(), OB_SUCC(ret) && !referred) {
    if (OB_ISNULL(*t)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is NULL", K(ret));
    } else {
      if ((*t)->ref_id_ == base_tid) {
        referred = true;
      } else if ((*t)->is_generated_table() && NULL != (*t)->ref_query_) {
        if (OB_FAIL(check_table_referred(*(*t)->ref_query_, base_tid, referred))) {
          LOG_WARN("check table referred failed", K(ret));
        }
      }
    }
  }

  FOREACH_CNT_X(expr, stmt.get_subquery_exprs(), OB_SUCC(ret) && !referred) {
    if (OB_ISNULL(*expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (NULL != (*expr)->get_ref_stmt()) {
      if (OB_FAIL(check_table_referred(*(*expr)->get_ref_stmt(), base_tid, referred))) {
        LOG_WARN("check table referred in subquery failed", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::uv_check_where_subquery(const TableItem &table_item, bool &ref_update_table)
{
  int ret = OB_SUCCESS;
  const TableItem *item = &table_item;
  uint64_t update_tid = table_item.get_base_table_item().ref_id_;
  ObSEArray<ObQueryRefRawExpr *, 1> query_exprs;
  while (OB_SUCC(ret) && NULL != item && item->is_generated_table()) {
    if (OB_ISNULL(item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is NULL for generate table", K(ret));
    } else {
      FOREACH_CNT_X(expr, item->ref_query_->get_condition_exprs(), OB_SUCC(ret)) {
        if (OB_ISNULL(*expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(*expr, query_exprs))) {
          LOG_WARN("extract subquery failed", K(ret));
        }
      }
    }
    item = item->view_base_item_;
  }

  FOREACH_CNT_X(expr, query_exprs, OB_SUCC(ret) && !ref_update_table) {
    if (OB_ISNULL(*expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (NULL != (*expr)->get_ref_stmt()) {
      if (OB_FAIL(check_table_referred(*(*expr)->get_ref_stmt(), update_tid, ref_update_table))) {
        LOG_WARN("check table referred in subquery failed", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_has_non_inner_join(ObSelectStmt &stmt, bool &has_non_inner_join)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(join, stmt.get_joined_tables(), !has_non_inner_join) {
    if (OB_ISNULL(*join)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join table is NULL", K(ret));
    } else if (!(*join)->is_inner_join()) {
      has_non_inner_join = true;
    }
  }
  return ret;
}

int ObResolverUtils::uv_check_has_non_inner_join(const TableItem &table_item, bool &has_non_inner_join)
{
  int ret = OB_SUCCESS;
  const TableItem *item = &table_item;
  while (OB_SUCC(ret) && NULL != item && item->is_generated_table() && !has_non_inner_join) {
    if (OB_ISNULL(item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is NULL for generate table", K(ret));
    } else if (OB_FAIL(check_has_non_inner_join(*item->ref_query_, has_non_inner_join))) {
      LOG_WARN("check select item subquery failed", K(ret));
    } else {
      item = item->view_base_item_;
    }
  }
  return ret;
}

int ObResolverUtils::uv_check_dup_base_col(const TableItem &table_item,
    bool &has_dup, bool &has_non_col_ref)
{
  int ret = OB_SUCCESS;
  has_dup = false;
  has_non_col_ref = false;
  if (table_item.is_generated_table() && NULL != table_item.ref_query_) {
    const uint64_t base_tid = table_item.get_base_table_item().ref_id_;
    const ObIArray<SelectItem> &select_items = table_item.ref_query_->get_select_items();
    ObSEArray<int64_t, 32> cids;
    FOREACH_CNT_X(si, select_items, OB_SUCC(ret) && !has_dup) {
      if (si->implicit_filled_) {
        continue;
      }
      ObRawExpr *expr = si->expr_;
      if (T_REF_ALIAS_COLUMN == expr->get_expr_type()) {
        expr = static_cast<ObAliasRefRawExpr*>(expr)->get_ref_expr();
      }
      if (T_REF_COLUMN != expr->get_expr_type()) {
        has_non_col_ref = true;
      } else {
        ColumnItem *col_item = table_item.ref_query_->get_column_item_by_id(
            static_cast<ObColumnRefRawExpr *>(expr)->get_table_id(),
            static_cast<ObColumnRefRawExpr *>(expr)->get_column_id());
        if (OB_ISNULL(col_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column item by id failed", K(ret), K(*expr));
        } else if (base_tid == col_item->base_tid_) {
          FOREACH_X(c, cids, !has_dup) {
            if (*c == col_item->base_cid_) {
              has_dup = true;
            }
          }
          if (OB_FAIL(cids.push_back(col_item->base_cid_))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// mysql insertable view:
// 1. must be inner join
// 2. all join components:
//    - must be view
//    - can not reference the update table
//    - pass uv_check_basic
int ObResolverUtils::uv_mysql_insertable_join(const TableItem &table_item, const uint64_t base_tid, bool &insertable)
{
  int ret = OB_SUCCESS;
  if (table_item.is_generated_table() && NULL != table_item.ref_query_) {
    OZ(check_stack_overflow());
    FOREACH_CNT_X(join, table_item.ref_query_->get_joined_tables(), OB_SUCC(ret) && insertable) {
      if (OB_ISNULL(*join)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join table is NULL", K(ret));
      } else if (!(*join)->is_inner_join()) {
        insertable = false;
      }
    }

    FOREACH_CNT_X(it, table_item.ref_query_->get_table_items(), OB_SUCC(ret) && insertable) {
      const TableItem *item = *it;
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null join table item", K(ret));
      } else if (item->is_basic_table()) {
        if (table_item.view_base_item_ != item && item->ref_id_ == base_tid) {
          LOG_DEBUG("reference to insert table");
          insertable = false;
        }
      } else if (item->is_generated_table()) {
        if (OB_ISNULL(item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ref query is NULL", K(ret));
        }

        if (OB_SUCC(ret) && insertable) {
          if (!item->is_view_table_) {
            insertable = false;
          }
        }

        if (OB_SUCC(ret) && insertable && item != table_item.view_base_item_) {
          bool ref = false;
          if (OB_FAIL(check_table_referred(*item->ref_query_, base_tid, ref))) {
            LOG_WARN("check table referred failed", K(ret));
          } else if (ref) {
            insertable = false;
          }
        }

        if (OB_SUCC(ret) && insertable) {
          const bool is_insert = true;
          int tmp_ret = uv_check_basic(*item->ref_query_, is_insert);
          if (OB_SUCCESS != tmp_ret) {
            if (tmp_ret == OB_ERR_NON_INSERTABLE_TABLE) {
              insertable = false;
            } else {
              ret = tmp_ret;
              LOG_WARN("check basic updatable view failed", K(ret));
            }
          }
        }

        if (OB_SUCC(ret) && insertable) {
          if (OB_FAIL(uv_mysql_insertable_join(*item, base_tid, insertable))) {
            LOG_WARN("check insertable join failed", K(ret));
          }
        }
      }
    } // end FOREACH
  }
  return ret;
}

int ObResolverUtils::uv_check_oracle_distinct(const TableItem &table_item,
                                              ObSQLSessionInfo &session_info,
                                              ObSchemaChecker &schema_checker,
                                              bool &has_distinct)
{
  int ret = OB_SUCCESS;
  has_distinct = false;
  const TableItem *item = &table_item;
  ObSEArray<ObRawExpr *, 16> select_exprs;
  while (NULL != item && item->is_generated_table() && NULL != item->ref_query_ && !has_distinct) {
    if (item->ref_query_->has_distinct()) {
      bool unique = false;
      select_exprs.reuse();
      // Can not call ObOptimizerUtil::get_select_exprs(),
      // since we need ignore implicit filled select items.
      FOREACH_CNT_X(si, item->ref_query_->get_select_items(), OB_SUCC(ret)) {
        if (OB_ISNULL(si->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL in select item", K(ret));
        } else {
          if (!si->implicit_filled_) {
            if (OB_FAIL(select_exprs.push_back(si->expr_))) {
              LOG_WARN("array push back failed", K(ret));
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(item->ref_query_, &session_info,
                                            &schema_checker, select_exprs, true /* strict */,
                                            unique, FLAGS_IGNORE_DISTINCT /* ignore distinct */))) {
        LOG_WARN("check stmt unique failed", K(ret));
      } else {
        // distinct will be removed latter if unique.
        has_distinct = !unique;
      }
    }
    item = item->view_base_item_;
  }
  return ret;
}

// 1. need to be updatable view in mysql mode.
// 2. no subquery in conditions.
// 3. only one basic table.
int ObResolverUtils::view_with_check_option_allowed(const ObSelectStmt *stmt,
                                                    bool &with_check_option)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = stmt;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref query is null", K(ret));
  } else if (with_check_option || VIEW_CHECK_OPTION_NONE != select_stmt->get_check_option()) {
    with_check_option = true;
    if (!is_oracle_mode() &&
        OB_FAIL(ObResolverUtils::uv_check_basic(const_cast<ObSelectStmt &>(*select_stmt), false))) {
      ret = OB_ERR_CHECK_OPTION_ON_NONUPDATABLE_VIEW;
      LOG_WARN("with check option on non updatable view not allowed", K(ret));
    } else if (OB_UNLIKELY(select_stmt->get_table_items().count() > 1)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "join view with check option");
      LOG_WARN("view on joined table with check option not allowed", K(ret));
    } else {
      FOREACH_CNT_X(expr, select_stmt->get_condition_exprs(), OB_SUCC(ret)) {
        if (OB_ISNULL(*expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else if (OB_UNLIKELY((*expr)->has_flag(CNT_SUB_QUERY))) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("view with subquery in conditions with check option not allowed", K(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "view with subquery in condition with check option");
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (select_stmt->is_set_stmt()) {
      const bool tmp_with_check_option = with_check_option;
      FOREACH_CNT_X(set_query, select_stmt->get_set_query(), OB_SUCC(ret)) {
        bool sub_with_check_option = tmp_with_check_option;
        if (OB_ISNULL(*set_query)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set query is null", K(ret));
        } else if (OB_FAIL(view_with_check_option_allowed(*set_query, sub_with_check_option))) {
          LOG_WARN("view with check option not allowed", K(ret), KPC(*set_query));
        } else {
          with_check_option |= sub_with_check_option;
        }
      }
    } else {
      const bool tmp_with_check_option = with_check_option;
      FOREACH_CNT_X(table_item, select_stmt->get_table_items(), OB_SUCC(ret)) {
        bool sub_with_check_option = tmp_with_check_option;
        if (OB_ISNULL(*table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item is null", K(ret));
        } else if ((*table_item)->is_generated_table()) {
          if (OB_FAIL(view_with_check_option_allowed((*table_item)->ref_query_, sub_with_check_option))) {
            LOG_WARN("view with check option not allowed", K(ret), KPC((*table_item)->ref_query_));
          } else {
            with_check_option |= sub_with_check_option;
          }
        }
      }
    }
  }
  return ret;
}

ObString ObResolverUtils::get_stmt_type_string(stmt::StmtType stmt_type)
{
  return ((stmt::T_NONE <= stmt_type && stmt_type <= stmt::T_MAX) ?
          stmt_type_string[stmt::get_stmt_type_idx(stmt_type)] : ObString::make_empty_string());
}

ParseNode *ObResolverUtils::get_select_into_node(const ParseNode &node)
{
  ParseNode *into_node = NULL;
  if (OB_LIKELY(node.type_ == T_SELECT)) {
    if (NULL != node.children_[PARSE_SELECT_INTO]) {
      into_node = node.children_[PARSE_SELECT_INTO];
    } else {
      into_node = node.children_[PARSE_SELECT_INTO_EXTRA];
    }
  }
  return into_node;
}

int ObResolverUtils::get_select_into_node(const ParseNode &node, ParseNode* &into_node, bool top_level)
{
  int ret = OB_SUCCESS;
  ParseNode *child_into_node = NULL;
  if (OB_LIKELY(node.type_ == T_SELECT)) {
    if (NULL != node.children_[PARSE_SELECT_SET] &&
        NULL != node.children_[PARSE_SELECT_FORMER] &&
        NULL != node.children_[PARSE_SELECT_LATER]) {
        if (OB_FAIL(SMART_CALL(get_select_into_node(*node.children_[PARSE_SELECT_FORMER], child_into_node, false))) ||
            NULL != child_into_node) {
          ret = OB_SUCCESS == ret ? OB_ERR_SET_USAGE : ret;
          LOG_WARN("invalid into clause", K(ret));
        } else {
          child_into_node = get_select_into_node(*node.children_[PARSE_SELECT_LATER]);
          if (!top_level && NULL != child_into_node) {
            ret = OB_ERR_SET_USAGE;
            LOG_WARN("invalid into clause", K(ret));
          }
          into_node = child_into_node;
        }
    } else {
      into_node = get_select_into_node(node);
    }
  }
  return ret;
}

int ObResolverUtils::parse_interval_ym_type(char *cstr, ObDateUnitType &part_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cstr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is null", K(ret));
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_YEAR)))) {
    part_type = DATE_UNIT_YEAR;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_MONTH)))) {
    part_type = DATE_UNIT_MONTH;
  } else {
    part_type = DATE_UNIT_MAX;
  }
  return ret;
}

int ObResolverUtils::parse_interval_ds_type(char *cstr, ObDateUnitType &part_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cstr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is null", K(ret));
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_DAY)))) {
    part_type = DATE_UNIT_DAY;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_HOUR)))) {
    part_type = DATE_UNIT_HOUR;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_MINUTE)))) {
    part_type = DATE_UNIT_MINUTE;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_SECOND)))) {
    part_type = DATE_UNIT_SECOND;
  } else {
    part_type = DATE_UNIT_MAX;
  }
  return ret;
}

int ObResolverUtils::parse_interval_precision(char *cstr,
                                              int16_t &precision,
                                              int16_t default_precision)
{
  int ret = OB_SUCCESS;
  const char *brackt_pos1 = strchr(cstr, '(');
  const char *brackt_pos2 = strchr(cstr, ')');
  if (OB_NOT_NULL(brackt_pos1) && OB_NOT_NULL(brackt_pos2)) {
    precision = atoi(brackt_pos1 + 1);
  } else {
    precision = default_precision;
  }
  return ret;
}

int ObResolverUtils::get_user_var_value(const ParseNode *node,
                                        ObSQLSessionInfo *session_info,
                                        ObObj &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(session_info));
  } else if (OB_UNLIKELY(1 != node->num_child_) || OB_ISNULL(node->children_) ||
             OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children for get user_val", K(ret), K(node->num_child_));
  } else {
    ObString str = ObString(static_cast<int32_t>(node->children_[0]->str_len_),
                            node->children_[0]->str_value_);
    ObSessionVariable osv;
    ret = session_info->get_user_variable(str, osv);
    if (OB_SUCC(ret)) {
      value = osv.value_;
      value.set_meta_type(osv.meta_);
    } else if (OB_ERR_USER_VARIABLE_UNKNOWN == ret) {
      value.set_null();
      value.set_collation_level(CS_LEVEL_IMPLICIT);
      ret = OB_SUCCESS;//always return success no matter found or not
    } else {
      LOG_WARN("Unexpected ret code", K(ret), K(str), K(osv));
    }
  }
  return ret;
}

int ObResolverUtils::check_duplicated_column(ObSelectStmt &select_stmt,
                                             bool can_skip/*default false*/)
{
  int ret = OB_SUCCESS;
   /*oracle模式允许sel/upd/del stmt中的generated table含有重复列，只要外层没有引用到重复列就行，同时对于外层引用
  * 到的列是否为重复列会在检查column时进行检测，eg: select 1 from (select c1,c1 from t1);
  * 因此对于oracle模式下sel/upd/del stmt进行检测时，检测到重复列时只需skip，但是仍然需要添加相关plan cache约束
  *
   */
  if (!can_skip) {
    for (int64_t i = 1; OB_SUCC(ret) && i < select_stmt.get_select_item_size(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
        if (ObCharset::case_compat_mode_equal(select_stmt.get_select_item(i).alias_name_,
                                              select_stmt.get_select_item(j).alias_name_)) {
           if (lib::is_oracle_mode() &&
               OB_NOT_NULL(select_stmt.get_select_item(i).expr_) &&
               OB_NOT_NULL(select_stmt.get_select_item(j).expr_)) {
             if ((select_stmt.get_select_item(i).expr_->is_column_ref_expr() &&
                  static_cast<ObColumnRefRawExpr *>(select_stmt.get_select_item(i).expr_)
                                                      ->is_joined_dup_column()) ||
                 (select_stmt.get_select_item(j).expr_->is_column_ref_expr() &&
                  static_cast<ObColumnRefRawExpr *>(select_stmt.get_select_item(j).expr_)
                                                      ->is_joined_dup_column())){
             } else if (select_stmt.get_select_item(i).expr_->is_aggr_expr()) {
               // bugfix:
               // aggr_expr in cte shouldn't raise error.
             } else {
               ret = OB_NON_UNIQ_ERROR;
               ObString scope_name = ObString::make_string(get_scope_name(T_FIELD_LIST_SCOPE));
               LOG_USER_ERROR(OB_NON_UNIQ_ERROR,
                              select_stmt.get_select_item(i).alias_name_.length(),
                              select_stmt.get_select_item(i).alias_name_.ptr(),
                              scope_name.length(),
                              scope_name.ptr());
             }
          } else {
            ret = OB_ERR_COLUMN_DUPLICATE;
            LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE,
                           select_stmt.get_select_item(i).alias_name_.length(),
                           select_stmt.get_select_item(i).alias_name_.ptr());
          }
          break;
        }
      }
    }
  }

  // mark all need_check_dup_name_ if neccessary
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_select_item_size(); i++) {
    if (select_stmt.get_select_item(i).is_real_alias_
        || 0 == select_stmt.get_select_item(i).paramed_alias_name_.length()
        || select_stmt.get_select_item(i).need_check_dup_name_) {
      // do nothing
    } else {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < select_stmt.get_select_item_size(); j++) {
        if (select_stmt.get_select_item(i).is_real_alias_
            || 0 == select_stmt.get_select_item(i).paramed_alias_name_.length()
            || select_stmt.get_select_item(j).need_check_dup_name_) {
          // do nothing
        } else if (ObCharset::case_compat_mode_equal(
                                              select_stmt.get_select_item(i).paramed_alias_name_,
                                              select_stmt.get_select_item(j).paramed_alias_name_)) {
          select_stmt.get_select_item(i).need_check_dup_name_ = true;
          select_stmt.get_select_item(j).need_check_dup_name_ = true;
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::escape_char_for_oracle_mode(ObIAllocator &allocator,
                                                 ObString &str,
                                                 ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = str.length();
  int64_t pos = 0;
  bool is_escaped_flag = false;
  auto do_escape = [&is_escaped_flag, &cs_type, &buf, &buf_len, &pos]
      (ObString encoded_char, int wchar) -> int {
    int ret = OB_SUCCESS;

    if (!is_escaped_flag && wchar == '\\') {
      is_escaped_flag = true;
    } else {
      if (is_escaped_flag && wchar < 128) {
        int with_back_slash = 0;
        int len = 0;
        int new_wchar = escaped_char(static_cast<unsigned char>(wchar), &with_back_slash);
        OZ (ObCharset::wc_mb(cs_type, new_wchar, buf + pos, buf_len - pos, len));
        pos += len;
      } else {
        if (pos + encoded_char.length() > buf_len) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("size overflow", K(ret), K(pos), K(encoded_char));
        } else {
          MEMCPY(buf + pos, encoded_char.ptr(), encoded_char.length());
          pos += encoded_char.length();
        }
      }
      is_escaped_flag = false;
    }
    return ret;
  };
  if (!str.empty()) {
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    }
    OZ (ObCharsetUtils::foreach_char(str, cs_type, do_escape));
    if (OB_SUCC(ret)) {
      str = ObString(pos, buf);
    }
  }
  return ret;
}

// Submit a product behavior change request before modifying the whitelist.
static const char * const sys_tenant_white_list[] = {
  "log/alert"
};

int ObResolverUtils::check_secure_path(const common::ObString &secure_file_priv, const common::ObString &full_path)
{
  int ret = OB_SUCCESS;

  const char *access_denied_notice_message =
    "Access denied, please set suitable variable 'secure-file-priv' first, such as: SET GLOBAL secure_file_priv = '/'";

  if (secure_file_priv.empty() || 0 == secure_file_priv.case_compare(N_NULL)) {
    ret = OB_ERR_NO_PRIVILEGE;
  } else if (OB_UNLIKELY(secure_file_priv.length() >= DEFAULT_BUF_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("secure file priv string length exceeds default buf length", K(ret),
        K(secure_file_priv), LITERAL_K(DEFAULT_BUF_LENGTH));
  } else {
    char buf[DEFAULT_BUF_LENGTH] = { 0 };
    MEMCPY(buf, secure_file_priv.ptr(), secure_file_priv.length());

    struct stat path_stat;
    stat(buf, &path_stat);
    if (0 == S_ISDIR(path_stat.st_mode)) {
      ret = OB_ERR_NO_PRIVILEGE;
    } else {
      MEMSET(buf, 0, sizeof(buf));
      char *real_secure_file = nullptr;
      if (NULL == (real_secure_file = ::realpath(to_cstring(secure_file_priv), buf))) {
        // pass
      } else {
        ObString secure_file_priv_tmp(real_secure_file);
        const int64_t pos = secure_file_priv_tmp.length();
        if (full_path.length() < secure_file_priv_tmp.length()) {
          ret = OB_ERR_NO_PRIVILEGE;
        } else if (!full_path.prefix_match(secure_file_priv_tmp)) {
          ret = OB_ERR_NO_PRIVILEGE;
        } else if (full_path.length() > secure_file_priv_tmp.length()
                   && secure_file_priv_tmp != "/" && full_path[pos] != '/') {
          ret = OB_ERR_NO_PRIVILEGE;
        }
      }
    }
  }
  if (OB_ERR_NO_PRIVILEGE == ret && OB_SYS_TENANT_ID == MTL_ID()) {
    char buf[DEFAULT_BUF_LENGTH] = { 0 };
    const int list_size = ARRAYSIZEOF(sys_tenant_white_list);
    for (int i = 0; OB_ERR_NO_PRIVILEGE == ret && i < list_size; i++) {
      const char * const secure_file_path = sys_tenant_white_list[i];
      struct stat path_stat;
      stat(secure_file_path, &path_stat);
      if (0 == S_ISDIR(path_stat.st_mode)) {
        // continue
      } else {
        MEMSET(buf, 0, sizeof(buf));
        char *real_secure_file = nullptr;
        if (NULL == (real_secure_file = ::realpath(secure_file_path, buf))) {
          // continue
        } else {
          ObString secure_file_path_tmp(real_secure_file);
          const int64_t pos = secure_file_path_tmp.length();
          if (full_path.length() < secure_file_path_tmp.length()) {
            // continue
          } else if (!full_path.prefix_match(secure_file_path_tmp)) {
            // continue
          } else if (full_path.length() > secure_file_path_tmp.length()
                    && secure_file_path_tmp != "/" && full_path[pos] != '/') {
            // continue
          } else {
            ret = OB_SUCCESS;
            LOG_INFO("check sys tenant whitelist success.", K(ret), K(secure_file_path), K(full_path));
          }
        }
      }
    }
  }
  if (OB_ERR_NO_PRIVILEGE == ret) {
    FORWARD_USER_ERROR_MSG(ret, "%s", access_denied_notice_message);
    LOG_WARN("no priv", K(ret), K(secure_file_priv), K(full_path));
  }

  return ret;
}

double ObResolverUtils::strntod(const char *str, size_t str_len,
                                ObItemType type, char **endptr, int *err)
{
  double result = 0.0;
  if ((ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str,
                    str_len,"BINARY_DOUBLE_NAN", strlen("BINARY_DOUBLE_NAN")) == 0)
            || (ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str,
                    str_len, "BINARY_FLOAT_NAN", strlen("BINARY_FLOAT_NAN")) == 0)
            || (ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str,
                    str_len, "-BINARY_FLOAT_NAN", strlen("-BINARY_FLOAT_NAN")) == 0)
            || (ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str,
                    str_len, "-BINARY_DOUBLE_NAN", strlen("-BINARY_DOUBLE_NAN")) == 0)) {
    result = NAN;
  } else if ((ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str, str_len,
                            "BINARY_DOUBLE_INFINITY", strlen("BINARY_DOUBLE_INFINITY")) == 0)
           || (ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str, str_len,
                            "BINARY_FLOAT_INFINITY", strlen("BINARY_FLOAT_INFINITY")) == 0)) {
    result = INFINITY;
  } else if ((ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str, str_len,
                            "-BINARY_DOUBLE_INFINITY", strlen("-BINARY_DOUBLE_INFINITY")) == 0)
           || (ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, str, str_len,
                            "-BINARY_FLOAT_INFINITY", strlen("-BINARY_FLOAT_INFINITY")) == 0)) {
    result = -INFINITY;
  } else {
    result = ObCharset::strntodv2(str, str_len, endptr, err);

    // Oracle behaves differently with MySQL on float/double boundary check.
    // Oracle reports number overflow when value's lower boundery is out of range
    // (value underflow). While MySQL has NO value underflow, and returns zero
    // under such cases. The following block is compatible with oracle underflow
    // logic.
    // Note: ObCharset::strntodv2 is compatiable with MySQL.
    if (*err != EOVERFLOW && lib::is_oracle_mode() && is_overflow(str, type, result)) {
      *err = EOVERFLOW;
    }
  }
  return result;
}

bool ObResolverUtils::is_overflow(const char *str, ObItemType type, double val)
{
  bool ret = false;
  char * stopstr = nullptr;
  if (type == T_FLOAT && val != 0) {
    LOG_DEBUG("number overflow check", K(val),
              K(std::numeric_limits<float>::max()),
              K(std::numeric_limits<float>::denorm_min()));
    if (fabs(val) > std::numeric_limits<float>::max() ||
        fabs(val) < std::numeric_limits<float>::denorm_min()) {
      ret = true;
    }
  } else {
    if (val == 0.0) {
      // acceleration: underflow shows up when val is zero, otherwise don't check
      errno = 0;
      std::strtod(str, &stopstr);
      if (errno == ERANGE)  {
        ret = true;
      }
    }
  }
  return ret;
}

bool ObResolverUtils::is_synonymous_type(ObObjType type1, ObObjType type2)
{
  bool ret = false;
  if (lib::is_oracle_mode()) {
    if (ObNumberType == type1 && ObNumberFloatType == type2) {
      ret = true;
    } else if (ObNumberFloatType == type1 && ObNumberType == type2) {
      ret = true;
    }
  }
  if (ob_is_decimal_int_tc(type1) && ob_is_number_tc(type2)) {
    ret = true;
  } else if (ob_is_number_tc(type1) && ob_is_decimal_int_tc(type2)) {
    ret = true;
  }
  return ret;
}

int ObResolverUtils::resolve_default_value_and_expr_from_select_item(const SelectItem &select_item,
                                                                     ColumnItem &column_item,
                                                                     const ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *column_expr = NULL;
  ColumnItem *tmp_column_item = NULL;
  if (OB_ISNULL(select_item.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (select_item.expr_->is_column_ref_expr()) {
    if (NULL == (column_expr = static_cast<ObColumnRefRawExpr *>(select_item.expr_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to convert rawexpr to columnrefrawexpr", K(ret));
    } else if (NULL == (tmp_column_item = stmt->get_column_item_by_id(column_expr->get_table_id(), column_expr->get_column_id()))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("fail to find the column item", K(ret));
    } else {
      column_item.set_default_value(tmp_column_item->default_value_);
      column_item.set_default_value_expr(tmp_column_item->default_value_expr_);
    }
  } else if (is_mysql_mode() && select_item.expr_->is_win_func_expr()) {
    const ObWinFunRawExpr *win_expr = reinterpret_cast<const ObWinFunRawExpr*>(select_item.expr_);
    if (T_WIN_FUN_RANK == win_expr->get_func_type() ||
        T_WIN_FUN_DENSE_RANK == win_expr->get_func_type() ||
        T_WIN_FUN_ROW_NUMBER == win_expr->get_func_type()) {
      ObObj temp_default;
      temp_default.set_uint64(0);
      column_item.set_default_value(temp_default);
    } else if (T_WIN_FUN_CUME_DIST == win_expr->get_func_type() ||
               T_WIN_FUN_PERCENT_RANK == win_expr->get_func_type()) {
      ObObj temp_default;
      temp_default.set_double(0);
      column_item.set_default_value(temp_default);
    } else {
      //do nothing
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObResolverUtils::check_whether_assigned(const ObDMLStmt *stmt,
                                            const common::ObIArray<ObAssignment> &assigns,
                                            uint64_t table_id,
                                            uint64_t base_column_id,
                                            bool &exist)
{
  int ret = OB_SUCCESS;
  int64_t N = assigns.count();
  exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && !exist && i < N; ++i) {
    const ObAssignment &as = assigns.at(i);
    const ColumnItem *column_item = nullptr;
    if (OB_ISNULL(as.column_expr_) ||
        OB_ISNULL(column_item = stmt->get_column_item_by_id(as.column_expr_->get_table_id(),
                                                            as.column_expr_->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_expr is null", K(ret), K(as.column_expr_), K(column_item));
    } else if (column_item->base_cid_ == base_column_id
        && as.column_expr_->get_table_id() == table_id) {
      exist = true;
    }
  }
  return ret;
}

// relevant issue :
int ObResolverUtils::prune_check_constraints(const ObIArray<ObAssignment> &assignments,
                                             ObIArray<ObRawExpr*> &check_exprs)
{
  int ret = OB_SUCCESS;
  if (check_exprs.empty()) {
    /*do nothing*/
  } else {
    ObSEArray<ObRawExpr*, 16> tmp_check_exprs;
    if (OB_FAIL(tmp_check_exprs.assign(check_exprs))) {
      LOG_WARN("failed to assign check exprs to tmp_check_exprs", K(ret));
    } else {
      check_exprs.reset();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_check_exprs.count(); ++i) {
      ObRawExpr *&check_expr = tmp_check_exprs.at(i);
      ObSEArray<ObRawExpr*, 16> column_exprs;
      if (OB_ISNULL(check_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check constraint expr is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(check_expr, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else {
        bool need_add_cst_to_stmt = false;
        for (int64_t i = 0; !need_add_cst_to_stmt && i < assignments.count(); ++i) {
          need_add_cst_to_stmt = has_exist_in_array(column_exprs,
                                 static_cast<ObRawExpr *>(assignments.at(i).column_expr_));
        }
        if (need_add_cst_to_stmt &&
            OB_FAIL(check_exprs.push_back(check_expr))) {
          LOG_WARN("add to check_constraint_exprs failed", K(ret));
        }
      }
    }
  }
  return ret;
}

ColumnItem *ObResolverUtils::find_col_by_base_col_id(ObDMLStmt &stmt,
    const uint64_t table_id, const uint64_t base_column_id, const uint64_t base_table_id)
{
  ColumnItem *c = NULL;
  const TableItem *t = stmt.get_table_item_by_id(table_id);
  if (OB_ISNULL(t)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "get table item failed", K(table_id));
  } else {
    c = find_col_by_base_col_id(stmt, *t, base_column_id, base_table_id);
  }
  return c;
}

ColumnItem *ObResolverUtils::find_col_by_base_col_id(ObDMLStmt &stmt,
    const TableItem &table_item, const uint64_t base_column_id, const uint64_t base_table_id)
{
  ColumnItem *item = NULL;
  bool has_tg = stmt.has_instead_of_trigger();
  FOREACH_CNT_X(col, stmt.get_column_items(), NULL == item) {
    if (!has_tg) {
      if (col->table_id_ == table_item.table_id_
          && col->base_cid_ == base_column_id
          && in_updatable_view_path(table_item, *col->expr_)) {
        item = &(*col);
      }
    } else {
      if (col->table_id_ == table_item.table_id_
          && col->base_cid_ == base_column_id
          && col->base_tid_ == base_table_id) {
        item = &(*col);
      }
    }
  }
  return item;
}

const ColumnItem *ObResolverUtils::find_col_by_base_col_id(const ObDMLStmt &stmt,
    const uint64_t table_id, const uint64_t base_column_id, const uint64_t base_table_id,
    bool ignore_updatable_check)
{
  const ColumnItem *c = NULL;
  const TableItem *t = stmt.get_table_item_by_id(table_id);
  if (OB_ISNULL(t)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "get table item failed", K(table_id));
  } else {
    c = find_col_by_base_col_id(stmt, *t, base_column_id, base_table_id, ignore_updatable_check);
  }
  return c;
}

const ColumnItem *ObResolverUtils::find_col_by_base_col_id(const ObDMLStmt &stmt,
    const TableItem &table_item, const uint64_t base_column_id, const uint64_t base_table_id,
    bool ignore_updatable_check)
{
  const ColumnItem *item = NULL;
  bool has_tg = stmt.has_instead_of_trigger();
  FOREACH_CNT_X(col, stmt.get_column_items(), NULL == item) {
    if (!has_tg) {
      if (col->table_id_ == table_item.table_id_
          && col->base_cid_ == base_column_id
          && (ignore_updatable_check || in_updatable_view_path(table_item, *col->expr_))) {
        item = &(*col);
      }
    } else {
      if (col->table_id_ == table_item.table_id_
          && col->base_cid_ == base_column_id
          && col->base_tid_ == base_table_id) {
        item = &(*col);
      }
    }
  }
  return item;
}

bool ObResolverUtils::in_updatable_view_path(const TableItem &table_item,
                                             const ObColumnRefRawExpr &col)
{
  bool in_path = false;
  if (table_item.table_id_ == col.get_table_id()) {
    if (table_item.is_generated_table() || table_item.is_temp_table()) {
      const int64_t cid = col.get_column_id() - OB_APP_MIN_COLUMN_ID;
      if (NULL != table_item.ref_query_ && NULL != table_item.view_base_item_
          && cid >= 0 && cid < table_item.ref_query_->get_select_item_size()) {
        auto expr = table_item.ref_query_->get_select_item(cid).expr_;
        if (expr->is_column_ref_expr()) {
          in_path = in_updatable_view_path(*table_item.view_base_item_,
              *static_cast<const ObColumnRefRawExpr *>(expr));
        }
      }
    } else {
      in_path = true;
    }
  }
  return in_path;
}

int ObResolverUtils::resolve_file_format_string_value(const ParseNode *node,
                                                      const ObCharsetType &format_charset,
                                                      ObResolverParams &params,
                                                      ObString &result_value)
{
  int ret = OB_SUCCESS;
  // 1. resolve expr
  ObRawExpr *expr = NULL;
  ObRawExprFactory *expr_factory = params.expr_factory_;
  ObSQLSessionInfo *session_info = params.session_info_;
  if (OB_ISNULL(node) || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. get unexpect NULL ptr", K(ret), K(node), K(expr_factory), K(session_info));
  } else if (OB_FAIL(resolve_const_expr(params, *node, expr, NULL))) {
    LOG_WARN("fail to resolve const expr", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. invalid param", K(ret), K(node->type_));
  } else if (OB_FAIL(expr->formalize(session_info))) {
    LOG_WARN("failed to formalize expr", K(ret), K(*expr));
  } else if (!expr->is_static_scalar_const_expr()) {
    ret = OB_NOT_SUPPORTED;
    ObSqlString err_msg;
    err_msg.append_fmt("using '%s' as format value", get_type_name(expr->get_expr_type()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
    LOG_WARN("failed. invalid params", K(ret), K(*expr));
  }

  // 2. try case convert
  if (OB_SUCC(ret)) {
    ObRawExpr *new_expr = NULL;
    const int64_t max_len = 64;
    ObCastMode cast_mode = CM_NONE;
    ObExprResType expr_output_type = expr->get_result_type();
    ObCollationType result_collation_type = ObCharset::get_bin_collation(format_charset);
    ObExprResType cast_dst_type;
    cast_dst_type.set_type(ObVarcharType);
    cast_dst_type.set_length(max_len);
    cast_dst_type.set_calc_meta(ObObjMeta());
    cast_dst_type.set_collation_type(result_collation_type);
    if (!(expr_output_type.is_varchar() ||
          expr_output_type.is_nvarchar2() ||
          expr_output_type.is_char() ||
          expr_output_type.is_nchar())) {
      if (result_collation_type == CS_TYPE_INVALID) {
        ret = OB_ERR_PARAM_INVALID;
        LOG_WARN("failed. get invalid collaction", K(ret), K(format_charset));
      } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session_info, cast_mode))) {
        LOG_WARN("get default cast mode failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(expr_factory,
                                                                 session_info,
                                                                 *expr,
                                                                 cast_dst_type,
                                                                 cast_mode,
                                                                 new_expr))) {
        LOG_WARN("try add cast expr above failed", K(ret), K(*expr));
      } else if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed. get unexpect NULL ptr", K(ret));
      } else if (OB_FAIL(new_expr->add_flag(IS_OP_OPERAND_IMPLICIT_CAST))) {
        LOG_WARN("failed to add flag", K(ret));
      } else {
        expr = new_expr;
      }
    }
  }

  // 3. compute expr result
  if (OB_SUCC(ret)) {
    RowDesc row_desc;
    ObNewRow tmp_row;
    ObObj value_obj;
    ObTempExpr *temp_expr = NULL;
    ObExecContext *exec_ctx = session_info->get_cur_exec_ctx();
    if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_sql_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. get unexpected NULL", K(ret));
    } else if (OB_FAIL(ObStaticEngineExprCG::gen_expr_with_row_desc(expr,
                                                            row_desc,
                                                            exec_ctx->get_allocator(),
                                                            exec_ctx->get_my_session(),
                                                            exec_ctx->get_sql_ctx()->schema_guard_,
                                                            temp_expr))) {
      LOG_WARN("fail to fill sql expression", K(ret));
    } else if (OB_ISNULL(temp_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to gen temp expr", K(ret));
    } else if (OB_FAIL(temp_expr->eval(*exec_ctx, tmp_row, value_obj))) {
      LOG_WARN("fail to calc value", K(ret), K(*expr));
    } else if (value_obj.is_null()) {
      result_value = ObString();
    } else {
      result_value = value_obj.get_string();
    }
  }
  return ret;
}

int ObResolverUtils::check_keystore_status(const uint64_t tenant_id,
                                           ObSchemaChecker &schema_checker)
{
  int ret = OB_SUCCESS;
  const ObKeystoreSchema *keystore_schema = NULL;
  if (OB_FAIL(schema_checker.get_keystore_schema(tenant_id, keystore_schema))) {
    LOG_WARN("fail to get keystore schema", K(ret));
  } else if (OB_ISNULL(keystore_schema)) {
    ret = OB_KEYSTORE_NOT_EXIST;
    LOG_WARN("the keystore is not exist", K(ret));
  } else if (0 == keystore_schema->get_status()) {
    ret = OB_KEYSTORE_NOT_OPEN;
    LOG_WARN("the keystore is not open", K(ret));
  } else if (2 == keystore_schema->get_status()) {
    ret = OB_KEYSTORE_OPEN_NO_MASTER_KEY;
    LOG_WARN("the keystore dont have any master key", K(ret));
  }
  return ret;
}

int ObResolverUtils::check_encryption_name(ObString &encryption_name, bool &need_encrypt)
{
  int ret = OB_SUCCESS;
  bool is_oracle = lib::is_oracle_mode();
  if (0 == encryption_name.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("encryption name cannot be empty", K(ret));
  } else if (0 == encryption_name.case_compare("aes-128") ||
             0 == encryption_name.case_compare("aes-192") ||
             0 == encryption_name.case_compare("aes-256") ||
             0 == encryption_name.case_compare("aes-128-gcm") ||
             0 == encryption_name.case_compare("aes-192-gcm") ||
#ifdef OB_USE_BABASSL
             0 == encryption_name.case_compare("sm4-cbc") ||
             0 == encryption_name.case_compare("sm4-gcm") ||
#endif
             0 == encryption_name.case_compare("aes-256-gcm")) {
    need_encrypt = true;
  } else if (!is_oracle && 0 == encryption_name.case_compare("y")) {
    need_encrypt = true;
    encryption_name = common::ObString::make_string("aes-256");
  } else if (!is_oracle && 0 == encryption_name.case_compare("n")) {
    need_encrypt = false;
  } else if (is_oracle && 0 == encryption_name.case_compare("none")){
    need_encrypt = false;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the encryption name is invalid", K(ret), K(encryption_name));
  }
  return ret;
}

int ObResolverUtils::check_not_supported_tenant_name(const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tenant_name.find('$'))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "since 4.2.1, manually creating a tenant name containing '$' is");
  }
  if (OB_SUCC(ret)) {
    const char *const forbid_list[] = {"all", "all_user", "all_meta"};
    int64_t list_len = ARRAYSIZEOF(forbid_list);
    for (int64_t i = 0; OB_SUCC(ret) && (i < list_len); ++i) {
      if (0 == tenant_name.case_compare(forbid_list[i])) {
        ret = OB_NOT_SUPPORTED;
        char err_info[128] = {'\0'};
        snprintf(err_info, sizeof(err_info), "since 4.2.1, using \"%s\" (case insensitive) "
            "as a tenant name is", forbid_list[i]);
        LOG_USER_ERROR(OB_NOT_SUPPORTED, err_info);
      }
    }
  }
  return ret;
}

int ObResolverUtils::rm_space_for_neg_num(ParseNode *param_node, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t idx = 0;
  if (param_node->str_len_ <= 0) {
    // do nothing
  } else if ('-' != param_node->str_value_[idx]) {
     // 'select - 1.2 from dual' and 'select 1.2 from dual' will hit the same plan, the key is
     // select ? from dual, so '- 1.2' and '1.2' will all go here, if '-' is not presented,
     // do nothing
    LOG_TRACE("rm space for neg num", K(idx), K(ObString(param_node->str_len_, param_node->str_value_)));
  } else if (OB_ISNULL(buf = (char *)allocator.alloc(param_node->str_len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator memory", K(ret), K(param_node->str_len_));
  } else {
    buf[pos++] =  '-';
    idx += 1;
    for (; idx < param_node->str_len_ && isspace(param_node->str_value_[idx]); idx++);
    int32_t len = (int32_t)(param_node->str_len_ - idx);
    if (len > 0) {
      MEMCPY(buf + pos, param_node->str_value_ + idx, len);
    }
    pos += len;
    param_node->str_value_ = buf;
    param_node->str_len_ = pos;
    LOG_TRACE("rm space for neg num", K(idx), K(ObString(param_node->str_len_, param_node->str_value_)));
  }
  return ret;
}

int ObResolverUtils::handle_varchar_charset(ObCharsetType charset_type,
                                            ObIAllocator &allocator,
                                            ParseNode *&node)
{
  int ret = OB_SUCCESS;
  if ((T_HEX_STRING == node->type_ || T_VARCHAR == node->type_)
      && CHARSET_INVALID != charset_type) {
    ParseNode *charset_node = new_node(&allocator, T_CHARSET, 0);
    ParseNode *varchar_node = NULL;
    if (T_HEX_STRING == node->type_) {
      varchar_node = new_non_terminal_node(&allocator, T_VARCHAR, 1, charset_node);
    } else if (T_VARCHAR == node->type_) {
      varchar_node = new_non_terminal_node(&allocator, T_VARCHAR, 2, charset_node, node);
    }

    if (OB_ISNULL(charset_node) || OB_ISNULL(varchar_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      const char *name = ObCharset::charset_name(charset_type);
      charset_node->str_value_ = parse_strdup(name, &allocator, &(charset_node->str_len_));
      if (NULL == charset_node->str_value_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        varchar_node->str_value_ = node->str_value_;
        varchar_node->str_len_ = node->str_len_;
        varchar_node->raw_text_ = node->raw_text_;
        varchar_node->text_len_ = node->text_len_;
        varchar_node->type_ = T_VARCHAR;

        node = varchar_node;
      }
    }
  }

  return ret;
}

int ObResolverUtils::resolver_param(ObPlanCacheCtx &pc_ctx,
                                    ObSQLSessionInfo &session,
                                    const ParamStore &phy_ctx_params,
                                    const stmt::StmtType stmt_type,
                                    const ObCharsetType param_charset_type,
                                    const ObBitSet<> &neg_param_index,
                                    const ObBitSet<> &not_param_index,
                                    const ObBitSet<> &must_be_positive_idx,
                                    const ObPCParam *pc_param,
                                    const int64_t param_idx,
                                    ObObjParam &obj_param,
                                    bool &is_param,
                                    const bool enable_decimal_int)
{
  int ret = OB_SUCCESS;
  ParseNode *raw_param = NULL;
  ObString literal_prefix;
  const bool is_paramlize = false;
  int64_t server_collation = CS_TYPE_INVALID;
  obj_param.reset();
  ObCompatType compat_type = COMPAT_MYSQL57;
  if (OB_ISNULL(pc_param) || OB_ISNULL(raw_param = pc_param->node_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(session.get_compatibility_control(compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  } else if (not_param_index.has_member(param_idx)) {
    /* do nothing */
    is_param = false;
    SQL_PC_LOG(TRACE, "not_param", K(param_idx), K(raw_param->type_), K(raw_param->value_),
                      "str_value", ObString(raw_param->str_len_, raw_param->str_value_));
  } else {
          // select -  1.2 from dual
          // "-  1.2" will be treated as a const node with neg sign
          // however, ObNumber::from("-  1.2") will throw a error, for there are spaces between neg sign and num
          // so remove spaces before resolve_const is called
    if (neg_param_index.has_member(param_idx) &&
        OB_FAIL(rm_space_for_neg_num(raw_param, pc_ctx.allocator_))) {
      SQL_PC_LOG(WARN, "fail to remove spaces for neg node", K(ret));
    } else if (OB_FAIL(handle_varchar_charset(param_charset_type, pc_ctx.allocator_, raw_param))) {
      SQL_PC_LOG(WARN, "fail to handle varchar charset");
    } else if (T_QUESTIONMARK == raw_param->type_) {
      int64_t idx = raw_param->value_;
      CK (idx >= 0 && idx < phy_ctx_params.count());
      OX (obj_param.set_is_boolean(phy_ctx_params.at(idx).is_boolean()));
    }
    if (OB_FAIL(ret)) {
    } else if (lib::is_oracle_mode() &&
               OB_FAIL(session.get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
      LOG_WARN("get sys variable failed", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_const(raw_param, stmt_type, pc_ctx.allocator_,
                       static_cast<ObCollationType>(session.get_local_collation_connection()),
                       session.get_nls_collation_nation(), session.get_timezone_info(),
                       obj_param, is_paramlize, literal_prefix,
                       session.get_actual_nls_length_semantics(),
                       static_cast<ObCollationType>(server_collation), NULL,
                       session.get_sql_mode(),
                       enable_decimal_int,
                       compat_type))) {
      SQL_PC_LOG(WARN, "fail to resolve const", K(ret));
    } else if (FALSE_IT(obj_param.set_raw_text_info(static_cast<int32_t>(raw_param->raw_sql_offset_),
                                                    static_cast<int32_t>(raw_param->text_len_)))) {
      /* nothing */
    } else if (ob_is_numeric_type(obj_param.get_type())) {
      // -0 is also counted as negative
      bool is_neg = false, is_zero = false;
      if (must_be_positive_idx.has_member(param_idx)) {
        if (obj_param.is_boolean()) {
          // boolean will skip this check
        } else if (lib::is_oracle_mode()) {
          if (OB_FAIL(is_negative_ora_nmb(obj_param, is_neg, is_zero))) {
            LOG_WARN("check oracle negative number failed", K(ret));
          } else if (is_neg || (is_zero && '-' == raw_param->str_value_[0])) {
            ret = OB_ERR_UNEXPECTED;
            pc_ctx.should_add_plan_ = false; // 内部主动抛出not supported时候需要设置这个标志，以免新计划add plan导致锁冲突
            LOG_TRACE("param must be positive", K(ret), K(param_idx), K(obj_param));
          }
        } else if (lib::is_mysql_mode()) {
          if (obj_param.is_integer_type() &&
              (obj_param.get_int() < 0 || (0 == obj_param.get_int() && '-' == raw_param->str_value_[0]))) {
            ret = OB_ERR_UNEXPECTED;
            pc_ctx.should_add_plan_ = false;
            LOG_TRACE("param must be positive", K(ret), K(param_idx), K(obj_param));
          }
        }
      }
    }
    is_param = true;
    LOG_DEBUG("is_param", K(param_idx), K(obj_param), K(raw_param->type_), K(raw_param->value_),
              "str_value", ObString(raw_param->str_len_, raw_param->str_value_));
  }
  return ret;
}

int ObResolverUtils::is_negative_ora_nmb(const ObObjParam &obj_param, bool &is_neg, bool &is_zero)
{
  int ret = OB_SUCCESS;
  if (obj_param.is_decimal_int()) {
    is_neg = wide::is_negative(obj_param.get_decimal_int(), obj_param.get_int_bytes());
    is_zero = wide::is_zero(obj_param.get_decimal_int(), obj_param.get_int_bytes());
  } else if (obj_param.is_number()) {
    is_neg = obj_param.is_negative_number();
    is_zero = obj_param.is_zero_decimalint();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type", K(obj_param));
  }
  return ret;
}

/* If ParseNode is a const param, fast know obj_type、collation_type、collation level */
int ObResolverUtils::fast_get_param_type(const ParseNode &node,
                                         const ParamStore *param_store,
                                         const ObCollationType connect_collation,
                                         const ObCollationType nchar_collation,
                                         const ObCollationType server_collation,
                                         const bool enable_decimal_int,
                                         ObIAllocator &alloc,
                                         ObObjType &obj_type,
                                         ObCollationType &coll_type,
                                         ObCollationLevel &coll_level)
{
  int ret = OB_SUCCESS;
  if (T_QUESTIONMARK == node.type_) {
    if (OB_ISNULL(param_store) ||
        OB_UNLIKELY(node.value_ < 0 || node.value_ >= param_store->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret));
    } else {
      obj_type = param_store->at(node.value_).get_param_meta().get_type();
      coll_type = param_store->at(node.value_).get_param_meta().get_collation_type();
      coll_level = param_store->at(node.value_).get_param_meta().get_collation_level();
    }
  } else if (IS_DATATYPE_OP(node.type_)) {
    if (T_VARCHAR == node.type_ || T_CHAR == node.type_ ||
        T_NVARCHAR2 == node.type_ || T_NCHAR == node.type_) {
      bool is_nchar = T_NVARCHAR2 == node.type_ || T_NCHAR == node.type_;
      obj_type = lib::is_mysql_mode() && is_nchar ? ObVarcharType :
                                                    static_cast<ObObjType>(node.type_);
      coll_level = CS_LEVEL_COERCIBLE;
      if (OB_UNLIKELY(node.str_len_ > OB_MAX_LONGTEXT_LENGTH)) {
        ret = OB_ERR_INVALID_INPUT_ARGUMENT;
      } else if (lib::is_oracle_mode()) {
        coll_type = is_nchar ? nchar_collation : server_collation;
        if (node.str_len_ == 0) {
          obj_type = is_nchar ? ObNCharType : ObCharType;
        }
      } else {
        if (0 == node.num_child_) {
          coll_type = is_nchar ? CS_TYPE_UTF8MB4_GENERAL_CI : connect_collation;
        } else if (NULL != node.children_[0] && T_CHARSET == node.children_[0]->type_) {
          ObString charset(node.children_[0]->str_len_, node.children_[0]->str_value_);
          ObCharsetType charset_type = ObCharset::charset_type(charset.trim());
          coll_type = ObCharset::get_default_collation(charset_type);
        } else {
          coll_type = connect_collation;
        }
      }
    } else if (T_IEEE754_NAN == node.type_ || T_IEEE754_INFINITE == node.type_) {
      obj_type = ObDoubleType;
      coll_type = CS_TYPE_BINARY;
      coll_level = CS_LEVEL_NUMERIC;
    } else if (T_BOOL == node.type_) {
      obj_type = ObTinyIntType;
      coll_type = CS_TYPE_BINARY;
      coll_level = CS_LEVEL_NUMERIC;
    } else if (T_UINT64 == node.type_ || T_INT == node.type_ || T_NUMBER == node.type_) {
      if ((lib::is_oracle_mode() && NULL != node.str_value_) || T_NUMBER == node.type_) {
        bool use_decimalint_as_result = false;
        int tmp_ret = OB_E(EventTable::EN_ENABLE_ORA_DECINT_CONST) OB_SUCCESS;
        int16_t precision = PRECISION_UNKNOWN_YET;
        int16_t scale = SCALE_UNKNOWN_YET;
        ObDecimalInt *decint = nullptr;
        if (OB_SUCCESS == tmp_ret && enable_decimal_int) {
          int32_t val_len = 0;
          ret = wide::from_string(node.str_value_, node.str_len_, alloc, scale, precision, val_len, decint);
          use_decimalint_as_result = precision <= OB_MAX_DECIMAL_POSSIBLE_PRECISION &&
                                     scale <= OB_MAX_DECIMAL_POSSIBLE_PRECISION &&
                                     scale >= 0 &&
                                     precision >= scale &&
                                     precision <= OB_MAX_NUMBER_PRECISION;
        }
        if (use_decimalint_as_result) {
          obj_type = ObDecimalIntType;
        } else {
          obj_type = ObNumberType;
        }
      } else {
        obj_type = T_INT == node.type_ ? ObIntType : ObUInt64Type;
      }
      coll_type = CS_TYPE_BINARY;
      coll_level = CS_LEVEL_NUMERIC;
    } else {
      obj_type = static_cast<ObObjType>(node.type_);
      coll_type = CS_TYPE_BINARY;
      coll_level = CS_LEVEL_NUMERIC;
    }
    if (OB_SUCC(ret)) {
      if (ObMaxType == obj_type || CS_TYPE_INVALID == coll_type || CS_LEVEL_INVALID == coll_level) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resolve node type failed.", K(ret), K(obj_type), K(coll_type), K(coll_level));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_allowed_alter_operations_for_mlog(
    const uint64_t tenant_id,
    const obrpc::ObAlterTableArg &arg,
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_version))) {
    SQL_RESV_LOG(WARN, "failed to get data version", K(ret));
  } else if (tenant_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview before 4.3 is");
  } else if (table_schema.is_mlog_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter materialized view log is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter materialized view log is");
  } else if (table_schema.has_mlog_table()) {
    bool is_alter_pk = false;
    ObIndexArg::IndexActionType pk_action_type;
    for (int64_t i = 0; OB_SUCC(ret) && (i < arg.index_arg_list_.count()); ++i) {
      const ObIndexArg *index_arg = arg.index_arg_list_.at(i);
      if (OB_ISNULL(index_arg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index arg is null", KR(ret));
      } else if ((ObIndexArg::ADD_PRIMARY_KEY == index_arg->index_action_type_)
          || (ObIndexArg::DROP_PRIMARY_KEY == index_arg->index_action_type_)
          || (ObIndexArg::ALTER_PRIMARY_KEY == index_arg->index_action_type_)) {
        is_alter_pk = true;
        pk_action_type = index_arg->index_action_type_;
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if ((arg.is_alter_indexs_ && !is_alter_pk)
        || (arg.is_update_global_indexes_ && !arg.is_alter_partitions_)
        || (arg.is_alter_options_ // the following allowed options change does not affect mlog
            && (arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::TABLE_DOP)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::CHARSET_TYPE)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::COLLATION_TYPE)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::COMMENT)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::EXPIRE_INFO)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::PRIMARY_ZONE)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::REPLICA_NUM)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::SEQUENCE_COLUMN_ID)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::USE_BLOOM_FILTER)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::LOCALITY)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::SESSION_ID)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::SESSION_ACTIVE_TIME)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::ENABLE_ROW_MOVEMENT)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::FORCE_LOCALITY)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::ENCRYPTION)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::TABLESPACE_ID)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::TTL_DEFINITION)
                || arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::KV_ATTRIBUTES)))
        || (lib::is_oracle_mode() // for "comment on table" command in oracle mode
            && arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::COMMENT))) {
      // supported operations
    } else if (!arg.is_alter_columns_
        && ((ObAlterTableArg::ADD_CONSTRAINT == arg.alter_constraint_type_)
        || (ObAlterTableArg::DROP_CONSTRAINT == arg.alter_constraint_type_)
        || (ObAlterTableArg::ALTER_CONSTRAINT_STATE == arg.alter_constraint_type_))) {
      // add/drop constraint is supported
    } else {
      // unsupported operations
      ret = OB_NOT_SUPPORTED;

      // generate more specific error messages
      if (is_alter_pk) {
        if (ObIndexArg::ADD_PRIMARY_KEY == pk_action_type) {
          LOG_WARN("add primary key to table with materialized view log is not supported",
              KR(ret), K(table_schema.get_table_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "add primary key to table with materialized view log is");
        } else if (ObIndexArg::DROP_PRIMARY_KEY == pk_action_type) {
          LOG_WARN("drop the primary key of table with materialized view log is not supported",
              KR(ret), K(table_schema.get_table_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop the primary key of table with materialized view log is");
        } else {
          LOG_WARN("alter the primary key of table with materialized view log is not supported",
              KR(ret), K(table_schema.get_table_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter the primary key of table with materialized view log is");
        }
      } else if (arg.is_alter_columns_) {
        LOG_WARN("alter column of table with materialized view log is not supported",
            KR(ret), K(table_schema.get_table_name()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter column of table with materialized view log is");
      } else if (arg.is_alter_partitions_) {
        LOG_WARN("alter partition of table with materialized view log is not supported",
            KR(ret), K(table_schema.get_table_name()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter partition of table with materialized view log is");
      } else if (arg.is_alter_options_) {
        if (arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::TABLE_NAME)) {
          LOG_WARN("alter name of table with materialized view log is not supported",
              KR(ret), K(table_schema.get_table_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter name of table with materialized view log is");
        } else {
          LOG_WARN("alter option of table with materialized view log is not supported",
              KR(ret), K(table_schema.get_table_name()), K(arg));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter option of table with materialized view log is");
        }
      } else {
        LOG_WARN("alter table with materialized view log is not supported",
            KR(ret), K(table_schema.get_table_name()), K(arg));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table with materialized view log is");
      }
    }
  }
  return ret;
}

int ObResolverUtils::create_values_table_query(ObSQLSessionInfo *session_info,
                                               ObIAllocator *allocator,
                                               ObRawExprFactory *expr_factory,
                                               ObQueryCtx *query_ctx,
                                               ObSelectStmt *select_stmt,
                                               ObValuesTableDef *table_def)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObString alias_name;
  if (OB_ISNULL(session_info) || OB_ISNULL(allocator) || OB_ISNULL(expr_factory) ||
      OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected ptr", K(ret));
  } else if (OB_ISNULL(table_item = select_stmt->create_table_item(*allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else if (OB_FAIL(select_stmt->generate_values_table_name(*allocator, alias_name))) {
    LOG_WARN("failed to generate func table name", K(ret));
  } else {
    table_item->table_id_ = query_ctx->available_tb_id_--;
    table_item->table_name_ = alias_name;
    table_item->alias_name_ = alias_name;
    table_item->type_ = TableItem::VALUES_TABLE;
    table_item->is_view_table_ = false;
    table_item->values_table_def_ = table_def;
    if (OB_FAIL(select_stmt->add_table_item(session_info, table_item))) {
      LOG_WARN("add table item failed", K(ret));
    } else if (OB_FAIL(select_stmt->add_from_item(table_item->table_id_))) {
      LOG_WARN("add from table failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t column_cnt = table_def->column_cnt_;
    ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    bool has_select_item = !select_items.empty();
    if (OB_UNLIKELY(table_def->column_types_.count() != column_cnt) ||
        OB_UNLIKELY(has_select_item && select_items.count() != column_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected ptr", K(ret), K(column_cnt), K(table_def->column_types_.count()), K(select_items.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        ObColumnRefRawExpr *column_expr = NULL;
        ObSqlString tmp_col_name;
        char *buf = NULL;
        if (OB_FAIL(expr_factory->create_raw_expr(T_REF_COLUMN, column_expr))) {
          LOG_WARN("create column ref raw expr failed", K(ret));
        } else if (OB_ISNULL(column_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(("value desc is null"), K(ret));
        } else if (OB_FAIL(column_expr->add_flag(IS_COLUMN))) {
          LOG_WARN("failed to add flag IS_COLUMN", K(ret));
        } else if (OB_FAIL(tmp_col_name.append_fmt("column_%ld", i))) {
          LOG_WARN("failed to append fmt", K(ret));
        } else if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(tmp_col_name.length())))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(buf));
        } else {
          column_expr->set_result_type(table_def->column_types_.at(i));
          column_expr->set_ref_id(table_item->table_id_, i + OB_APP_MIN_COLUMN_ID);
          MEMCPY(buf, tmp_col_name.ptr(), tmp_col_name.length());
          ObString column_name(tmp_col_name.length(), buf);
          column_expr->set_column_attr(table_item->table_name_, column_name);
          ColumnItem column_item;
          column_item.expr_ = column_expr;
          column_item.table_id_ = column_expr->get_table_id();
          column_item.column_id_ = column_expr->get_column_id();
          column_item.column_name_ = column_expr->get_column_name();
          if (OB_FAIL(select_stmt->add_column_item(column_item))) {
            LOG_WARN("failed to add column item", K(ret));
          } else if (has_select_item) {
            SelectItem &select_item = select_items.at(i);
            select_item.expr_ = column_expr;
          } else {
            SelectItem select_item;
            select_item.alias_name_ = column_expr->get_column_name();
            select_item.expr_name_ = column_expr->get_column_name();
            select_item.is_real_alias_ = false;
            select_item.expr_ = column_expr;
            if (OB_FAIL(select_stmt->add_select_item(select_item))) {
              LOG_WARN("failed to add select item", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int64_t ObResolverUtils::get_mysql_max_partition_num(const uint64_t tenant_id)
{
  int64_t max_partition_num = OB_MAX_PARTITION_NUM_MYSQL;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    max_partition_num = tenant_config->max_partition_num;
  }
  return max_partition_num;
}

int ObResolverUtils::check_schema_valid_for_mview(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < table_schema.get_column_count()); ++i) {
    const ObColumnSchemaV2 *column_schema = nullptr;
    if (OB_ISNULL(column_schema = table_schema.get_column_schema_by_idx(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", KR(ret));
    } else if (column_schema->is_xmltype()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("create materialized view on xmltype columns is not supported", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "create materialized view on xmltype columns is");
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
