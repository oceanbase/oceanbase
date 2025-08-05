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

#define USING_LOG_PREFIX RS


#include "ob_alter_table_constraint_checker.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_service.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_ddl_service.h"


namespace oceanbase
{
using namespace share::schema;
using namespace share;
using namespace obrpc;

namespace rootserver
{
int ObAlterTableConstraintChecker::check_can_change_cst_column_name(
    const obrpc::ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    const uint64_t tenant_data_version,
    bool &can_change_cst_column_name)
{
  int ret = OB_SUCCESS;
  can_change_cst_column_name = false;
  const share::schema::AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;

  if (DATA_VERSION_4_3_5_2 <= tenant_data_version
   &&  ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_
   && alter_table_arg.is_only_alter_column()
   && 1 == alter_table_schema.get_constraint_count()) {
    ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin();
    if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("constraint iter is null", K(ret));
    } else if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
      can_change_cst_column_name = true;
      ObTableSchema::const_column_iterator iter_begin = alter_table_schema.column_begin();
      ObTableSchema::const_column_iterator iter_end = alter_table_schema.column_end();
      const uint64_t col_id = *((*iter)->cst_col_begin());
      AlterColumnSchema *alter_column_schema = nullptr;
      for(; OB_SUCC(ret) && can_change_cst_column_name && iter_begin != iter_end; iter_begin++) {
        const ObColumnSchemaV2 *col_schema = nullptr;
        if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*iter_begin))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter is NULL", K(ret));
        } else if (OB_ISNULL(col_schema = orig_table_schema.get_column_schema(alter_column_schema->get_column_id()))) {
        } else if (col_schema->get_column_name_str() != alter_column_schema->get_column_name_str() && col_schema->get_column_id() != col_id) {
          // ensures that the column being renamed and the column to which the constraint is added are the same.
          can_change_cst_column_name = false;
        }
      }
    }
  }
  return ret;
}

int ObAlterTableConstraintChecker::check_can_add_cst_on_multi_column(
    const obrpc::ObAlterTableArg &alter_table_arg,
    const uint64_t tenant_data_version,
    bool &can_add_cst_on_multi_column) {
  int ret = OB_SUCCESS;
  can_add_cst_on_multi_column = false;
  const share::schema::AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  int64_t allow_change_cst_column_count =  DATA_VERSION_4_3_5_4 <= tenant_data_version ? INT64_MAX : 2;
  if (DATA_VERSION_4_3_5_2 <= tenant_data_version
  && ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_
  && alter_table_arg.is_only_alter_column()
  && alter_table_schema.get_constraint_count() <= allow_change_cst_column_count
  && alter_table_schema.get_column_count() <= allow_change_cst_column_count) {
    can_add_cst_on_multi_column = true;
    ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin();
    for (; OB_SUCC(ret) && can_add_cst_on_multi_column && iter != alter_table_schema.constraint_end(); iter++) {
      if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constraint iter is null", K(ret));
      } else if (CONSTRAINT_TYPE_NOT_NULL != (*iter)->get_constraint_type()) {
        can_add_cst_on_multi_column = false;
      } else if (OB_UNLIKELY(1 != (*iter)->get_column_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column count of not null constraint", K(ret), KPC(*iter));
      } else if (OB_INVALID_ID == *(*iter)->cst_col_begin()) {
        can_add_cst_on_multi_column = false;
      }
    }
  }
  return ret;
}


int ObAlterTableConstraintChecker::check_is_change_cst_column_name(const ObTableSchema &table_schema,
                                                  const AlterTableSchema &alter_table_schema,
                                                  bool &change_cst_column_name)
{
  int ret = OB_SUCCESS;
  change_cst_column_name = false;
  ObTableSchema::const_column_iterator iter = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator iter_end = alter_table_schema.column_end();
  AlterColumnSchema *alter_column_schema = nullptr;
  for(; OB_SUCC(ret) && !change_cst_column_name && iter != iter_end; iter++) {
    const ObColumnSchemaV2 *col_schema = nullptr;
    if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*iter))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", K(ret));
    } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(alter_column_schema->get_column_id()))) {
    } else if (col_schema->get_column_name_str() != alter_column_schema->get_column_name_str()) {
      change_cst_column_name = true;
    }
  }
  return ret;
}

int ObAlterTableConstraintChecker::check_alter_table_constraint(
    rootserver::ObDDLService &ddl_service,
    const obrpc::ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    const uint64_t tenant_data_version,
    share::ObDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  char err_msg[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
  const ObAlterTableArg::AlterConstraintType type = alter_table_arg.alter_constraint_type_;
  bool change_cst_column_name = false;
  bool is_alter_decimal_int_offline = false;
  bool is_column_group_store = false;
  bool can_change_cst_column_name = false;
  bool can_add_cst_on_multi_column = false;
  switch(type) {
    case obrpc::ObAlterTableArg::ADD_CONSTRAINT:
    case obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE: {
      if (OB_FAIL(check_is_change_cst_column_name(orig_table_schema,
                                                  alter_table_arg.alter_table_schema_,
                                                  change_cst_column_name))) {
        LOG_WARN("failed to check change cst column name", K(ret));
      } else if (change_cst_column_name && OB_FAIL(check_can_change_cst_column_name(alter_table_arg, orig_table_schema, tenant_data_version, can_change_cst_column_name))) {
        LOG_WARN("failed to check can modify column name and constraint", K(ret), K(alter_table_arg), K(orig_table_schema));
      } else if ((share::ObDDLType::DDL_TABLE_REDEFINITION == ddl_type || share::ObDDLType::DDL_MODIFY_COLUMN == ddl_type)
                  && !change_cst_column_name) {
        ddl_type = share::ObDDLType::DDL_TABLE_REDEFINITION;
      } else if (can_change_cst_column_name) {
        ddl_type = share::ObDDLType::DDL_TABLE_REDEFINITION;
      } else if (is_long_running_ddl(ddl_type)) {
        // if modify auto_increment and constraint together, treat it as normal modify column
        ret = OB_NOT_SUPPORTED;
      } else if (change_cst_column_name) {
        ddl_type = share::ObDDLType::DDL_CHANGE_COLUMN_NAME;
        ret = OB_NOT_SUPPORTED;
      } else if (alter_table_arg.alter_table_schema_.get_constraint_count() > 1
              && OB_FAIL(check_can_add_cst_on_multi_column(alter_table_arg, tenant_data_version, can_add_cst_on_multi_column))) {
        LOG_WARN("failed to check can modify column name and constraint", K(ret), K(alter_table_arg));
      } else if (can_add_cst_on_multi_column) {
        ddl_type = share::ObDDLType::DDL_TABLE_REDEFINITION;
      } else {
        ddl_type = share::ObDDLType::DDL_NORMAL_TYPE;
      }
      break;
    }
    // to avoid ddl type being modified from DROP_COLUMN to NORMAL_TYPE
    case obrpc::ObAlterTableArg::DROP_CONSTRAINT: {
      bool is_drop_col_only = false;
      if (share::ObDDLType::DDL_DROP_COLUMN == ddl_type) {
        // In oracle mode, we support to drop constraint implicitly caused by drop column.
      } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(orig_table_schema, is_column_group_store))) {
        LOG_WARN("fail to check schema is column group store", K(ret));
      } else if (OB_FAIL(ObSchemaUtils::is_drop_column_only(alter_table_arg.alter_table_schema_, is_drop_col_only))) {
        LOG_WARN("fail to check is drop column only", K(ret), K(alter_table_arg.alter_table_schema_));
      } else if (share::ObDDLType::DDL_TABLE_REDEFINITION == ddl_type && is_drop_col_only && is_column_group_store) {
        // for column store, drop column is table redefinition
      } else if (OB_FAIL(ddl_service.check_is_alter_decimal_int_offline(ddl_type,
                                                            orig_table_schema,
                                                            alter_table_arg.alter_table_schema_,
                                                            is_alter_decimal_int_offline))) {
        LOG_WARN("fail to check is alter decimal int offline ddl", K(ret));
      } else if (is_long_running_ddl(ddl_type) && !is_alter_decimal_int_offline) {
        ret = OB_NOT_SUPPORTED;
      } else if (is_alter_decimal_int_offline) {
        ddl_type = share::ObDDLType::DDL_MODIFY_COLUMN;
      } else {
        ddl_type = share::ObDDLType::DDL_NORMAL_TYPE;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unknown alter constraint action type!", K(ret), K(type));
    }
  }
  if (OB_NOT_SUPPORTED == ret) {
    (void)snprintf(err_msg, sizeof(err_msg), "%s and alter constraint in single statement", ddl_service.ddl_type_str(ddl_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
  }
  return ret;
}

// check whether it's modify column not null or modify constraint state, which need send two rpc.
int ObAlterTableConstraintChecker::need_modify_not_null_constraint_validate(
  rootserver::ObDDLService &ddl_service,
  const obrpc::ObAlterTableArg &alter_table_arg,
  const uint64_t tenant_data_version,
  bool &is_add_not_null_col,
  bool &need_modify)
{
  int ret = OB_SUCCESS;
  need_modify = false;
  is_add_not_null_col = false;
  bool can_add_cst_on_multi_column = false;
  ObSchemaGetterGuard schema_guard;
  schema_guard.set_session_id(alter_table_arg.session_id_);
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  const uint64_t tenant_id = alter_table_schema.get_tenant_id();
  const ObString &origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString &origin_table_name = alter_table_schema.get_origin_table_name();
  const ObTableSchema *orig_table_schema = NULL;
  if (!ddl_service.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (obrpc::ObAlterTableArg::ADD_CONSTRAINT != alter_table_arg.alter_constraint_type_
             && obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE != alter_table_arg.alter_constraint_type_) {
    // skip
  } else if (OB_FAIL(ddl_service.get_schema_service().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   origin_database_name,
                                                   origin_table_name,
                                                   false,
                                                   orig_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(origin_database_name),
             K(origin_table_name));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("NULL ptr", K(ret), KR(tenant_id), K(alter_table_arg), K(schema_guard.get_session_id()));
  } else if (alter_table_arg.alter_table_schema_.get_constraint_count() == 1) {
    ObTableSchema::const_constraint_iterator iter =
        alter_table_arg.alter_table_schema_.constraint_begin();
    if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("constraint is null", K(ret));
    } else if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
      if (OB_UNLIKELY(1 != (*iter)->get_column_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column count of not null constraint", K(ret), KPC(*iter));
      } else if (!(*iter)->get_need_validate_data()) {
        // don't need validate data, do nothing.
      } else if (OB_INVALID_ID == *(*iter)->cst_col_begin()) {
        is_add_not_null_col = true;
      } else {
        need_modify = true;
      }
    }
  } else if (alter_table_arg.alter_table_schema_.get_constraint_count() > 1) {
    // more than one constraint, check column_id of all not null constraint must be invalid.
    // since we only support add more than one not null column in one ddl,
    // not support modify more than one column not null in one ddl.
    ObTableSchema::const_constraint_iterator iter =
        alter_table_arg.alter_table_schema_.constraint_begin();
    for(; iter != alter_table_arg.alter_table_schema_.constraint_end() && OB_SUCC(ret); iter++) {
      if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constraint is null", K(ret));
      } else if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
        if (OB_UNLIKELY(1 != (*iter)->get_column_cnt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column count of not null constraint", K(ret), KPC(*iter));
        } else if (OB_UNLIKELY(OB_INVALID_ID != *(*iter)->cst_col_begin())) {
          if (OB_FAIL(check_can_add_cst_on_multi_column(alter_table_arg, tenant_data_version, can_add_cst_on_multi_column))) {
            LOG_WARN("failed to check can add cst on multi column", K(ret), K(alter_table_arg));
          } else if (can_add_cst_on_multi_column) {
            need_modify = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("modify not null column is not allowed with other DDL", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add/modify not null constraint together with other DDLs");
          }
        }
      }
    }
    is_add_not_null_col = can_add_cst_on_multi_column ? false : true;
  }
  return ret;
}

int ObAlterTableConstraintChecker::modify_not_null_constraint_validate(
      const obrpc::ObAlterTableArg &alter_table_arg,
      AlterTableSchema &alter_table_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema::constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin_for_non_const_iter();
  ObTableSchema::constraint_iterator it_end = alter_table_arg.alter_table_schema_.constraint_end_for_non_const_iter();
  for (; OB_SUCC(ret) && iter != it_end; iter++) {
    if (OB_ISNULL(iter) || OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema not found", K(ret), K(alter_table_arg.alter_table_schema_));
    } else {
      const uint64_t col_id = *((*iter)->cst_col_begin());
      ObColumnSchemaV2 *col_schema = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < alter_table_schema.get_column_count(); i++) {
        if (OB_ISNULL(alter_table_schema.get_column_schema_by_idx(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema not found", K(ret), K(alter_table_arg));
        } else if (alter_table_schema.get_column_schema_by_idx(i)->get_column_id() == col_id) {
          col_schema = alter_table_schema.get_column_schema_by_idx(i);
        }
      }
      if OB_FAIL(ret) {
      } else if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema not found", K(ret), K(alter_table_arg));
      } else {
        col_schema->del_column_flag(NOT_NULL_VALIDATE_FLAG);
      }
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
