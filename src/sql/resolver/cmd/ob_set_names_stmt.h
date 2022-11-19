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

#ifndef _OB_SET_NAMES_STMT_H
#define _OB_SET_NAMES_STMT_H
#include "lib/string/ob_string.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
namespace oceanbase
{
namespace sql
{
// statement for both SET NAMES and SET CHARSET
class ObSetNamesStmt: public ObCMDStmt
{
public:
  ObSetNamesStmt()
      :ObCMDStmt(stmt::T_SET_NAMES),
      is_set_names_(true),
      is_default_charset_(false),
      is_default_collation_(false)
  {}
  virtual ~ObSetNamesStmt() {}

  bool is_set_names() const { return this->is_set_names_; }
  void set_is_set_names(bool is_set_names) { this->is_set_names_ = is_set_names; }

  bool is_default_charset() const { return is_default_charset_; }
  void set_is_default_charset(bool is_default) { is_default_charset_ = is_default; }

  bool is_default_collation() const { return is_default_collation_; }
  void set_is_default_collation(bool is_default) { is_default_collation_ = is_default; }

  const common::ObString& get_charset() const { return this->charset_; }
  void set_charset(const common::ObString &charset) { this->charset_ = charset; }

  const common::ObString& get_collation() const { return this->collation_; }
  void set_collation(const common::ObString &collation) { this->collation_ = collation; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
               K_(is_set_names),
               K_(is_default_charset),
               K_(is_default_collation),
               K_(charset),
               K_(collation));
private:
  // types and constants
private:
  // function members
private:
  // data members
  bool is_set_names_;  // SET NAMES or SET CHARSET?
  bool is_default_charset_;
  bool is_default_collation_;
  common::ObString charset_;
  common::ObString collation_;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSetNamesStmt);
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OB_SET_NAMES_STMT_H
