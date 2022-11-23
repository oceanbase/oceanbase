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

#ifndef _OB_START_TRANS_STMT_H
#define _OB_START_TRANS_STMT_H
#include "sql/resolver/tcl/ob_tcl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObStartTransStmt: public ObTCLStmt
{
public:
  ObStartTransStmt();
  virtual ~ObStartTransStmt();
  virtual void print(FILE *fp, int32_t level, int32_t index);
  void set_read_only(bool val);
  bool get_read_only() const;
  void set_with_consistent_snapshot(bool val);
  bool get_with_consistent_snapshot() const;
  virtual bool cause_implicit_commit() const { return true; }
  const ObString &get_hint() const { return hint_; }
  void set_hint(const ObString hint) { hint_ = hint; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
      K_(read_only),
      K_(with_consistent_snapshot),
      K_(hint));
private:
  // types and constants
private:
  // disallow copy
  ObStartTransStmt(const ObStartTransStmt &other);
  ObStartTransStmt &operator=(const ObStartTransStmt &other);
  // function members
private:
  // data members
  bool with_consistent_snapshot_;
  bool read_only_;
  ObString hint_;
};

inline ObStartTransStmt::ObStartTransStmt()
    : ObTCLStmt(stmt::T_START_TRANS),
    with_consistent_snapshot_(false),
    read_only_(false), hint_()
{
}
inline ObStartTransStmt::~ObStartTransStmt()
{
}
inline void ObStartTransStmt::set_read_only(bool val)
{
  read_only_ = val;
}
inline bool ObStartTransStmt::get_read_only() const
{
  return read_only_;
}
inline void ObStartTransStmt::set_with_consistent_snapshot(bool val)
{
  with_consistent_snapshot_ = val;
}
inline bool ObStartTransStmt::get_with_consistent_snapshot() const
{
  return with_consistent_snapshot_;
}
inline void ObStartTransStmt::print(FILE *fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObStartTransStmt id=%d>\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "WithConsistentSnapshot := %c\n", with_consistent_snapshot_ ? 'Y' : 'N');
  print_indentation(fp, level);
  fprintf(fp, "</ObStartTransStmt>\n");
}
} // end namespace sql
} // end namespace oceanbase

#endif // _OB_START_TRANS_STMT_H
