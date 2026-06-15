/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "object/ob_obj_type.h"

#define USING_LOG_PREFIX SQL_RESV
#include "ob_ddl_resolver.h"
#include "share/ob_index_builder_util.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "sql/resolver/ddl/ob_interval_partition_resolver.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace sql
{

int ObDDLResolver::check_ttl_definition(const ParseNode *node, const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;

  const ObTableSchema *tbl_schema = nullptr;

  if (OB_ISNULL(schema_checker_) || OB_ISNULL(stmt_) || OB_ISNULL(session_info_) || OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "unexpected null value", K(ret), K_(schema_checker), K_(stmt), K_(session_info), K(node));
  } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    tbl_schema = &create_table_stmt->get_create_table_arg().schema_;
    if (OB_FAIL(ttl_flag_.init(tenant_data_version))) {
      LOG_WARN("failed to init ttl flag", KR(ret), K(tenant_data_version));
    }
  } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
    if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                  database_name_,
                                                  table_name_,
                                                  false,
                                                  tbl_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(session_info_->get_effective_tenant_id()), K(alter_table_stmt->get_alter_table_arg()));
    } else if (OB_ISNULL(tbl_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(database_name_), K(table_name_));
    } else {
      ttl_flag_ = tbl_schema->get_ttl_flag();
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported statement for TTL expression", K(ret), K(stmt_->get_stmt_type()));
  }

  if (OB_FAIL(ret)) {
  } else if (node->type_ == T_TTL_DEFINITION_WITH_TYPE) {
    if (OB_UNLIKELY(node->num_child_ < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ttl type", K(ret), K(node));
    } else if (static_cast<ObTTLDefinition::ObTTLType>(node->children_[1]->value_) == ObTTLDefinition::DELETING) {
      if (OB_FAIL(check_deleting_ttl_expr(node->children_[0], tbl_schema, tenant_data_version))) {
        LOG_WARN("failed to check ttl expr", KR(ret), K(node));
      }
    } else if (static_cast<ObTTLDefinition::ObTTLType>(node->children_[1]->value_) == ObTTLDefinition::COMPACTION) {
      if (OB_FAIL(check_compaction_ttl_expr(node->children_[0], tbl_schema, tenant_data_version))) {
        LOG_WARN("failed to check ttl expr", KR(ret), K(node));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ttl type", K(ret), K(node));
    }
  } else if (node->type_ == T_TTL_DEFINITION) {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      if (OB_FAIL(check_deleting_ttl_expr(node->children_[i], tbl_schema, tenant_data_version))) {
        LOG_WARN("failed to check ttl expr", KR(ret), K(i));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ttl type", K(ret), K(node->type_));
  }

  return ret;
}

int ObDDLResolver::check_compaction_ttl_expr(const ParseNode *ttl_expr_node,
                                             const ObTableSchema *tbl_schema,
                                             const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;

  const ObColumnSchemaV2 *column_schema = nullptr;

  if (OB_ISNULL(ttl_expr_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ttl expr is null", K(ret));
  } else if (OB_UNLIKELY(nullptr == ttl_expr_node || T_TTL_EXPR != ttl_expr_node->type_ || ttl_expr_node->num_child_ != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child node of ttl definition is wrong", KR(ret), K(ttl_expr_node), K(ttl_expr_node->type_), K(ttl_expr_node->num_child_));
  } else if (OB_UNLIKELY(nullptr == ttl_expr_node->children_[0] || T_COLUMN_REF != ttl_expr_node->children_[0]->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child node of ttl expr is wrong", KR(ret), K(ttl_expr_node->children_[0]), K(ttl_expr_node->children_[0]->type_));
  } else {
    ObTTLFlag tmp_ttl_flag;
    ObString column_name(ttl_expr_node->children_[0]->str_len_, ttl_expr_node->children_[0]->str_value_);
    ObTTLFlag::TTLColumnType ttl_column_type = ObTTLFlag::TTLColumnType::NONE;
    int64_t ttl_column_id = 0;

    if (ObCompactionTTLUtil::is_rowscn_column(column_name)) {
      if (tenant_data_version < ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("rowscn column as ttl column is not supported in data version less than 4.5.1", K(ret), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "rowscn column as ttl column in this version is");
      } else {
        ttl_column_type = ObTTLFlag::TTLColumnType::ROWSCN;
      }
    } else if (tenant_data_version < ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION_V2) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("non-rowscn column as ttl column is not supported in data version less than 4.6.1", K(ret), K(tenant_data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-rowscn column as ttl column in data version less than 4.6.1 is");
    } else if (OB_ISNULL(tbl_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_ISNULL(column_schema = tbl_schema->get_column_schema(column_name))) {
      ret = OB_TTL_COLUMN_NOT_EXIST;
      LOG_USER_ERROR(OB_TTL_COLUMN_NOT_EXIST, column_name.length(), column_name.ptr());
      LOG_WARN("ttl column is not exists", K(ret), K(column_name));
    } else {
      // any check like data type, hbase, etc. will be handled in ObCompactionTTLUtil::check_ttl_column_valid(both ddl_resolver and rootserver)
      // don't need to check again here
      ttl_column_type = ObTTLFlag::TTLColumnType::USER;
      ttl_column_id = column_schema->get_column_id();

      if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
        // in create table process, we can directly set the has_used_as_ttl flag
        //                                      and set skip-index for ttl column
        ObSkipIndexColumnAttr skip_idx_attr = column_schema->get_skip_index_attr();
        skip_idx_attr.set_min_max();
        const_cast<ObColumnSchemaV2*>(column_schema)->set_has_used_as_ttl(true);
        const_cast<ObColumnSchemaV2*>(column_schema)->set_skip_index_attr(skip_idx_attr.get_packed_value());
      } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
        // in alter table process, add AlterColumnSchema to set has_used_as_ttl, same pattern as set_comment
        ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
        AlterColumnSchema alter_column_schema;
        if (OB_FAIL(alter_column_schema.assign(*column_schema))) {
          LOG_WARN("fail to assign column schema", K(ret), K(column_name));
        } else if (!alter_column_schema.has_used_as_ttl()) {
          // We need to add skip-index and mark has_used_as_ttl only when it's not set before
          ObSkipIndexColumnAttr skip_idx_attr = column_schema->get_skip_index_attr();
          skip_idx_attr.set_min_max();
          alter_column_schema.set_has_used_as_ttl(true);
          alter_column_schema.set_skip_index_attr(skip_idx_attr.get_packed_value());
          alter_column_schema.alter_type_ = OB_DDL_MODIFY_COLUMN;
          alter_table_stmt->set_alter_table_column();
          if (OB_FAIL(alter_column_schema.set_origin_column_name(column_name))) {
            LOG_WARN("failed to set origin column name", K(ret), K(column_name));
          } else if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
            LOG_WARN("add alter column schema failed", K(ret), K(column_name));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected statement type", K(ret), K(stmt_->get_stmt_type()));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tmp_ttl_flag.init(tenant_data_version, ObTTLDefinition::COMPACTION, ttl_column_type, ttl_column_id))) {
      LOG_WARN("failed to init ttl flag", KR(ret), K(tenant_data_version), K(ttl_column_type));
    } else {
      ttl_flag_.fuse(tmp_ttl_flag);
    }
  }

  return ret;
}

int ObDDLResolver::check_deleting_ttl_expr(const ParseNode *ttl_expr_node,
                                           const ObTableSchema *tbl_schema,
                                           const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;

  const ObColumnSchemaV2 *column_schema = nullptr;

  if (OB_ISNULL(ttl_expr_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ttl expr is null", K(ret));
  } else if (OB_UNLIKELY(T_TTL_EXPR != ttl_expr_node->type_ || ttl_expr_node->num_child_ != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child node of ttl definition is wrong", KR(ret), K(ttl_expr_node), K(ttl_expr_node->type_), K(ttl_expr_node->num_child_));
  } else if (OB_UNLIKELY(nullptr == ttl_expr_node->children_[0] || T_COLUMN_REF != ttl_expr_node->children_[0]->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child node of ttl expr is wrong", KR(ret), K(ttl_expr_node->children_[0]), K(ttl_expr_node->children_[0]->type_));
  } else {
    ObTTLFlag tmp_ttl_flag;
    ObString column_name(ttl_expr_node->children_[0]->str_len_, ttl_expr_node->children_[0]->str_value_);

    if (OB_ISNULL(tbl_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_ISNULL(column_schema = tbl_schema->get_column_schema(column_name))) {
      ret = OB_TTL_COLUMN_NOT_EXIST;
      LOG_USER_ERROR(OB_TTL_COLUMN_NOT_EXIST, column_name.length(), column_name.ptr());
      LOG_WARN("ttl column is not exists", K(ret), K(column_name));
    } else if ((!ob_is_datetime_tc(column_schema->get_data_type()) &&
                !ob_is_mysql_datetime_tc(column_schema->get_data_type()))) {
      ret = OB_TTL_COLUMN_TYPE_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_TTL_COLUMN_TYPE_NOT_SUPPORTED, column_name.length(), column_name.ptr());
      LOG_WARN("invalid ttl expression, ttl column type should be datetime or timestamp",
                K(ret), K(column_name), K(column_schema->get_data_type()));
    } else if (OB_FAIL(tmp_ttl_flag.init(tenant_data_version,
                                         ObTTLDefinition::DELETING,
                                         ObTTLFlag::TTLColumnType::NONE,
                                         0))) {
      LOG_WARN("failed to init ttl flag", KR(ret));
    } else {
      ttl_flag_.fuse(tmp_ttl_flag);
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
