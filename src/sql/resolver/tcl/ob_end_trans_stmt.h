/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_END_TRANS_STMT_H
#define _OB_END_TRANS_STMT_H
#include "sql/resolver/tcl/ob_tcl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObEndTransStmt: public ObTCLStmt
{
public:
  ObEndTransStmt(): ObTCLStmt(stmt::T_END_TRANS), is_rollback_(false), hint_() {}
  virtual ~ObEndTransStmt() {}
  virtual void print(FILE *fp, int32_t level, int32_t index);

  void set_is_rollback(bool val) {is_rollback_ = val;}
  bool get_is_rollback() const {return is_rollback_;}
  const ObString &get_hint() const { return hint_; }
  void set_hint(const ObString hint) { hint_ = hint; }
private:
  // types and constants
  // function members
private:
  // data members
  bool is_rollback_;
  ObString hint_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObEndTransStmt);
};
inline void ObEndTransStmt::print(FILE *fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObEndTransStmt id=%d>\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "IsRollback := %c\n", is_rollback_ ? 'Y' : 'N');
  print_indentation(fp, level);
  fprintf(fp, "</ObEndTransStmt>\n");
}
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_END_TRANS_STMT_H */
