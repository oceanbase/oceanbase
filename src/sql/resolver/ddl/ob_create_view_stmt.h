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

#ifndef OCEANBASE_SQL_OB_CREATE_VIEW_STMT_H
#define OCEANBASE_SQL_OB_CREATE_VIEW_STMT_H

#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/parser/parse_node.h"

namespace oceanbase {
namespace sql {
class ObCreateViewStmt : public ObStmt {
public:
  explicit ObCreateViewStmt(common::ObIAllocator* name_pool);
  ObCreateViewStmt();
  virtual ~ObCreateViewStmt();

  void set_name_pool(common::ObIAllocator* name_pool);
  int set_view_name(const common::ObString& view_name);
  int set_view_definition(const common::ObString& view_definition);
  void set_definition_stmt(ObSelectStmt* definition_stmt);
  const common::ObString& get_view_name() const;
  const common::ObString& get_view_definition() const;
  const ObSelectStmt* get_definition_stmt() const;
  virtual void print(FILE* fp, int32_t level, int32_t index = 0);
  virtual bool cause_implicit_commit() const
  {
    return true;
  }

protected:
  common::ObIAllocator* name_pool_;

private:
  ObSelectStmt* definition_stmt_;
  common::ObString view_name_;
  common::ObString view_definition_;
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_CREATE_VIEW_STMT_H
