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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_OB_HELP_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_OB_HELP_STMT_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "common/row/ob_row_store.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObHelpStmt : public ObDMLStmt
{
public:
  ObHelpStmt();
  virtual ~ObHelpStmt();
  int add_row(const common::ObNewRow &row) { return row_store_.add_row(row); }
  int clear_row_store()
  {
    row_store_.clear_rows();
    return common::OB_SUCCESS;
  }
  const common::ObRowStore& get_row_store() const { return row_store_; }
  int rollback_last_row() {  return row_store_.rollback_last_row(); }
  int set_col_count(int64_t col_count) { return row_store_.set_col_count(col_count); }
  int64_t get_col_count() const { return row_store_.get_col_count(); }
  int get_col_name(int64_t idx, common::ObString &col_name);
  int64_t get_row_count() const { return row_store_.get_row_count(); }
  int add_col_name(common::ObString col_name);
private:
  common::ObRowStore row_store_;
  common::ObSEArray<common::ObString, 3, common::ModulePageAllocator, true> col_names_;
  DISALLOW_COPY_AND_ASSIGN(ObHelpStmt);
};
}//sql
}//observer
#endif /*OCEANBASE_SQL_RESOLVER_CMD_OB_OB_HELP_STMT_*/
