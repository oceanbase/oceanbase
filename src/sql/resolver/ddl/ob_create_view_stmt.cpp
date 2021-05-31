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

#include "sql/resolver/ddl/ob_create_view_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObCreateViewStmt::ObCreateViewStmt() : ObStmt(stmt::T_CREATE_VIEW), name_pool_(NULL), definition_stmt_(NULL)
{}

ObCreateViewStmt::ObCreateViewStmt(ObIAllocator* name_pool)
    : ObStmt(stmt::T_CREATE_VIEW), name_pool_(name_pool), definition_stmt_(NULL)
{}

ObCreateViewStmt::~ObCreateViewStmt()
{}

void ObCreateViewStmt::set_name_pool(common::ObIAllocator* name_pool)
{
  name_pool_ = name_pool;
}

int ObCreateViewStmt::set_view_name(const common::ObString& view_name)
{
  OB_ASSERT(name_pool_);
  OB_ASSERT(view_name.length() > 0 && view_name.ptr());

  int ret = OB_SUCCESS;
  if ((ret = ob_write_string(*name_pool_, view_name, view_name_)) != OB_SUCCESS) {
    _OB_LOG(WARN, "write view name failed, ret=%d", ret);
  }
  return ret;
}

int ObCreateViewStmt::set_view_definition(const common::ObString& view_definition)
{
  OB_ASSERT(name_pool_);
  OB_ASSERT(view_definition.length() > 0 && view_definition.ptr());

  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, view_definition, view_definition_))) {
    _OB_LOG(WARN, "write view definition failed, ret=%d", ret);
  }
  return ret;
}

void ObCreateViewStmt::set_definition_stmt(ObSelectStmt* definition_stmt)
{
  OB_ASSERT(definition_stmt);
  definition_stmt_ = definition_stmt;
}

const ObString& ObCreateViewStmt::get_view_name() const
{
  return view_name_;
}

const ObString& ObCreateViewStmt::get_view_definition() const
{
  return view_definition_;
}

const ObSelectStmt* ObCreateViewStmt::get_definition_stmt() const
{
  return definition_stmt_;
}

void ObCreateViewStmt::print(FILE* fp, int32_t level, int32_t index)
{
  OB_ASSERT(fp && definition_stmt_);

  print_indentation(fp, level);
  fprintf(fp, "ObCreateViewStmt %d Begin\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "View Name ::= %.*s\n", view_name_.length(), view_name_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "View Definition ::='%.*s'", view_definition_.length(), view_definition_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "View Definition Stmt ::=\n");
  definition_stmt_->print(fp, level + 2, index);
  print_indentation(fp, level);
  fprintf(fp, "ObCreateTableStmt %d End\n", index);
}
