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

#ifndef _OB_LOCATION_UTILS_STMT_H
#define _OB_LOCATION_UTILS_STMT_H
#include "lib/string/ob_string.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObLocationUtilsStmt: public ObCMDStmt
{
public:
  ObLocationUtilsStmt()
      :ObCMDStmt(stmt::T_LOCATION_UTILS), op_type_(OB_INVALID_ID), location_name_(), sub_path_(), pattern_()
  {}
  virtual ~ObLocationUtilsStmt() {}
  uint64_t get_op_type() const { return this->op_type_; }
  const common::ObString& get_location_name() const { return this->location_name_; }
  const common::ObString& get_sub_path() const { return this->sub_path_; }
  const common::ObString& get_pattern() const { return this->pattern_; }
  void set_op_type(uint64_t op_type) { this->op_type_ = op_type; }
  void set_location_name(const common::ObString &location_name) { this->location_name_ = location_name; }
  void set_sub_path(const common::ObString &sub_path) { this->sub_path_ = sub_path; }
  void set_pattern(const common::ObString &pattern) { this->pattern_ = pattern; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(op_type), K_(location_name), K_(sub_path), K_(pattern));
private:
  uint64_t op_type_;
  common::ObString location_name_;
  common::ObString sub_path_;
  common::ObString pattern_;
  DISALLOW_COPY_AND_ASSIGN(ObLocationUtilsStmt);
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OB_LOCATION_UTILS_STMT_H
