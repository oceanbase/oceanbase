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

#include "sql/resolver/ddl/ob_alter_table_stmt.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObAlterTableStmt::ObAlterTableStmt(common::ObIAllocator *name_pool)
    : ObTableStmt(name_pool, stmt::T_ALTER_TABLE), is_comment_table_(false), 
      is_alter_system_(false), fts_arg_allocator_(nullptr), is_alter_triggers_(false),
      interval_expr_(NULL), transition_expr_(NULL), alter_table_action_count_(0),
      alter_external_table_type_(0)
{
}

ObAlterTableStmt::ObAlterTableStmt()
    : ObTableStmt(stmt::T_ALTER_TABLE), is_comment_table_(false), is_alter_system_(false),
      fts_arg_allocator_(nullptr), is_alter_triggers_(false), interval_expr_(NULL), transition_expr_(NULL), alter_table_action_count_(0),
      alter_external_table_type_(0)
{
}

ObAlterTableStmt::~ObAlterTableStmt()
{
  for (int64_t i = 0; i < index_arg_list_.count(); ++i) {
    obrpc::ObCreateIndexArg *index_arg = index_arg_list_.at(i);
    if (is_fts_index(index_arg->index_type_)
        || is_multivalue_index(index_arg->index_type_)) {
      index_arg->~ObCreateIndexArg();
      fts_arg_allocator_->free(index_arg);
    }
  }
  index_arg_list_.reuse();
  fts_arg_allocator_ = nullptr;
}

int ObAlterTableStmt::add_column(const share::schema::AlterColumnSchema &column_schema)
{
  int ret = OB_SUCCESS;
  share::schema::AlterTableSchema &alter_table_schema =
      get_alter_table_arg().alter_table_schema_;
  if (OB_FAIL(alter_table_schema.add_alter_column(column_schema, true))){
    SQL_RESV_LOG(WARN, "failed to add column schema to alter table schema", K(ret));
  }
  return ret;
}

int ObAlterTableStmt::add_column_group(const ObColumnGroupSchema &column_group)
{
  int ret = OB_SUCCESS;
  share::schema::AlterTableSchema &alter_table_schema =
      get_alter_table_arg().alter_table_schema_;
  if (OB_FAIL(alter_table_schema.add_column_group(column_group))){
    SQL_RESV_LOG(WARN, "failed to add column schema to alter table schema", K(ret), K(column_group), K(alter_table_schema));
  }
  return ret;
}

int ObAlterTableStmt::add_index_arg(obrpc::ObIndexArg *index_arg)
{
  int ret = OB_SUCCESS;
  if (index_arg == NULL) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "index arg should not be null!", K(ret));
  } else if (OB_FAIL(alter_table_arg_.index_arg_list_.push_back(index_arg))) {
    SQL_RESV_LOG(WARN, "failed to add index arg to alter table arg!", K(ret));
  }
  return ret;
}

int ObAlterTableStmt::check_drop_fk_arg_exist(
    obrpc::ObDropForeignKeyArg *drop_fk_arg, bool &has_same_fk_arg)
{
  int ret = OB_SUCCESS;
  has_same_fk_arg = false;

  if (OB_ISNULL(drop_fk_arg)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "drop_fk_arg should not be null", K(ret));
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) && !has_same_fk_arg && i < alter_table_arg_.index_arg_list_.count();
         ++i) {
      if (OB_ISNULL(alter_table_arg_.index_arg_list_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "index_arg from index_arg_list_ is null", K(ret), K(i));
      } else if (obrpc::ObIndexArg::IndexActionType::DROP_FOREIGN_KEY
                 != alter_table_arg_.index_arg_list_.at(i)->index_action_type_) {
        continue; // skip
      } else if (0 == static_cast<obrpc::ObDropForeignKeyArg*>(alter_table_arg_.index_arg_list_.at(i))->
                        foreign_key_name_.compare(drop_fk_arg->foreign_key_name_)) {
        has_same_fk_arg = true;
      }
    }
  }

  return ret;
}

int ObAlterTableStmt::check_drop_cst_exist(
    const ObConstraint &constraint, bool &has_same_cst)
{
  int ret = OB_SUCCESS;
  has_same_cst = false;

  if (obrpc::ObAlterTableArg::DROP_CONSTRAINT != get_alter_table_arg().alter_constraint_type_) {
    // skip
  } else {
    AlterTableSchema &alter_table_schema = get_alter_table_arg().alter_table_schema_;
    for (ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin();
        OB_SUCC(ret) && !has_same_cst && iter != alter_table_schema.constraint_end();
         ++iter) {
      if (0 == (*iter)->get_constraint_name_str().compare(constraint.get_constraint_name_str())) {
        has_same_cst = true;
      }
    }
  }

  return ret;
}

int ObAlterTableStmt::set_database_name(const ObString &db_name)
{
  return alter_table_arg_.alter_table_schema_.set_database_name(db_name);
}

int ObAlterTableStmt::set_origin_database_name(const ObString &origin_db_name)
{
  return alter_table_arg_.alter_table_schema_.set_origin_database_name(origin_db_name);
}

int ObAlterTableStmt::set_table_name(const ObString &table_name)
{
  return alter_table_arg_.alter_table_schema_.set_table_name(table_name);
}

int ObAlterTableStmt::set_origin_table_name(const ObString &origin_table_name)
{
  return alter_table_arg_.alter_table_schema_.set_origin_table_name(origin_table_name);
}

void ObAlterTableStmt::set_table_id(const uint64_t table_id)
{
  alter_table_arg_.alter_table_schema_.set_table_id(table_id);
}

int ObAlterTableStmt::fill_session_vars(const ObBasicSessionInfo &session) {
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(session.get_effective_tenant_id(), tenant_data_version))) {
    SQL_RESV_LOG(WARN, "get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_2_2_0) {
    //do nothing
  } else if (OB_FAIL(alter_table_arg_.local_session_var_.load_session_vars(&session))) {
    SQL_RESV_LOG(WARN, "load local session vars failed", K(ret));
  }
  return ret;
}

int ObAlterTableStmt::set_exchange_partition_arg(const obrpc::ObExchangePartitionArg &exchange_partition_arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exchange_partition_arg_.assign(exchange_partition_arg))) {
    SQL_RESV_LOG(WARN, "failed to assign", K(ret), K(exchange_partition_arg));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
