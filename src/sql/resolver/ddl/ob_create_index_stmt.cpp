/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_create_index_stmt.h"

namespace oceanbase
{
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
namespace sql
{
ObCreateIndexStmt::ObCreateIndexStmt(ObIAllocator *name_pool)
    : ObPartitionedStmt(name_pool, stmt::T_CREATE_INDEX), create_index_arg_(&allocator_), table_id_(OB_INVALID_ID)
{
}

ObCreateIndexStmt::ObCreateIndexStmt()
    : ObPartitionedStmt(NULL, stmt::T_CREATE_INDEX), create_index_arg_(&allocator_), table_id_(OB_INVALID_ID)
{
}

ObCreateIndexStmt::~ObCreateIndexStmt()
{
}

ObCreateIndexArg &ObCreateIndexStmt::get_create_index_arg()
{
  return create_index_arg_;
}

int ObCreateIndexStmt::add_sort_column(const ObColumnSortItem &sort_column)
{
  int ret = OB_SUCCESS;
  if (OB_USER_MAX_ROWKEY_COLUMN_NUMBER == create_index_arg_.index_columns_.count()) {
    ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
  } else if (OB_FAIL(create_index_arg_.index_columns_.push_back(sort_column))) {
    LOG_WARN("add index column failed", K(ret));
  } else {}
  return ret;
}

int ObCreateIndexStmt::add_storing_column(const ObString &column_name)
{
  return create_index_arg_.store_columns_.push_back(column_name);
}

int ObCreateIndexStmt::add_hidden_storing_column(const ObString &column_name)
{
  return create_index_arg_.hidden_store_columns_.push_back(column_name);
}

void ObCreateIndexStmt::set_compress_method(const ObString &compress_method)
{
  create_index_arg_.index_option_.compress_method_ = compress_method;
}

void ObCreateIndexStmt::set_comment(const ObString &comment)
{
  create_index_arg_.index_option_.comment_ = comment;
}

void ObCreateIndexStmt::set_storage_cache_policy(const ObString &storage_cache_policy)
{
  create_index_arg_.index_option_.storage_cache_policy_ = storage_cache_policy;
}

void ObCreateIndexStmt::set_index_name(const ObString &index_name)
{
  create_index_arg_.index_name_ = index_name;
}

int ObCreateIndexStmt::invalidate_backup_index_id()
{
  int ret = OB_SUCCESS;
  create_index_arg_.index_table_id_ = OB_INVALID_ID;
  create_index_arg_.data_table_id_ = OB_INVALID_ID;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
