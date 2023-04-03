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
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
namespace sql
{
ObCreateIndexStmt::ObCreateIndexStmt(ObIAllocator *name_pool)
    : ObPartitionedStmt(name_pool, stmt::T_CREATE_INDEX), create_index_arg_() ,table_id_(OB_INVALID_ID)
{
}

ObCreateIndexStmt::ObCreateIndexStmt()
    : ObPartitionedStmt(NULL, stmt::T_CREATE_INDEX), create_index_arg_(), table_id_(OB_INVALID_ID)
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
