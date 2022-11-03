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

#ifndef OCEANBASE_SQL_OB_COLUMN_REF_
#define OCEANBASE_SQL_OB_COLUMN_REF_

#include "lib/ob_name_def.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace sql
{
class ObColumnRef
{
public:
  ObColumnRef()
      :database_name_(),
      table_name_(),
      column_name_(),
      is_star_(false),
      collation_type_(common::CS_TYPE_INVALID)
  {
  }

  virtual ~ObColumnRef() {}

  void set_database_name(common::ObString &database_name)
  {
    database_name_.assign_ptr(database_name.ptr(), database_name.length());
  }

  void set_table_name(common::ObString &table_name)
  {
    table_name_.assign_ptr(table_name.ptr(), table_name.length());
  }

  void set_column_name(common::ObString &column_name)
  {
    column_name_.assign_ptr(column_name.ptr(), column_name.length());
  }

  void set_star()
  {
    is_star_ = true;
  }

  bool is_star() const
  {
    return is_star_;
  }

  void get_database_name(common::ObString &database_name)const
  {
    database_name.assign_ptr(database_name_.ptr(), database_name_.length());
  }

  void get_table_name(common::ObString &table_name) const
  {
    table_name.assign_ptr(table_name_.ptr(), table_name_.length());
  }

  void get_column_name(common::ObString &column_name) const
  {
    column_name.assign_ptr(column_name_.ptr(), column_name_.length());
  }
  const common::ObString &get_database_name() const { return database_name_; }
  const common::ObString &get_table_name() const { return table_name_; }
  const common::ObString &get_column_name() const { return column_name_; }

  const common::ObCollationType& get_collation_type() const { return collation_type_; }
  void set_collation_type(const common::ObCollationType &collation_type)
  {
    collation_type_ = collation_type;
  }

  TO_STRING_KV(K(database_name_),
               K(table_name_),
               K(column_name_),
               K(is_star_),
               N_COLLATION, common::ObCharset::collation_name(collation_type_));
private:
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString column_name_;
  bool is_star_;
  common::ObCollationType collation_type_;
};
}//namespace sql
}//namespace oceanbase

#endif
