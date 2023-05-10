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

#ifndef OCEANBASE_SQL_RESOLVER_DML_EXPLAIN_STMT_
#define OCEANBASE_SQL_RESOLVER_DML_EXPLAIN_STMT_

#include "sql/resolver/dml/ob_dml_stmt.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

struct ObExplainDisplayOpt
{
  ObExplainDisplayOpt()
    : with_tree_line_(false),
    with_color_(false) ,
    with_real_info_(false)
    {}

  bool with_tree_line_;
  bool with_color_;
  bool with_real_info_;
};

class ObExplainStmt : public ObDMLStmt
{
public:
  ObExplainStmt();
  virtual ~ObExplainStmt();
  void set_explain_format(ExplainType format) { format_ = format; }
  ExplainType get_explain_type() const { return format_; }
  void set_display_opt(const ObExplainDisplayOpt &opt) { display_opt_ = opt; }
  const ObExplainDisplayOpt &get_display_opt() const { return display_opt_; }
  ObDMLStmt* get_explain_query_stmt() const { return explain_query_stmt_; }
  void set_explain_query_stmt(ObDMLStmt *stmt) { explain_query_stmt_ = stmt; }
  bool is_select_explain() const;
  bool is_dml_explain() const;
  const common::ObString& get_into_table() const { return into_table_; }
  void set_into_table(const common::ObString& into_table) { into_table_ = into_table; }
  const common::ObString& get_statement_id() const { return statement_id_; }
  void set_statement_id(const common::ObString& statement_id) { statement_id_ = statement_id; }
  virtual bool is_affect_found_rows() const { return is_select_explain(); }
  bool is_explain_extended() const { return EXPLAIN_EXTENDED == format_
                                            || EXPLAIN_EXTENDED_NOADDR == format_; }

  DECLARE_VIRTUAL_TO_STRING;
private:
  //bool  verbose_;
  ExplainType format_;
  ObExplainDisplayOpt display_opt_;
  ObDMLStmt *explain_query_stmt_;
  common::ObString into_table_;
  common::ObString statement_id_;
  DISALLOW_COPY_AND_ASSIGN(ObExplainStmt);
};

}
}

#endif //OCEANBASE_SQL_RESOLVER_DML_EXPLAIN_STMT_
