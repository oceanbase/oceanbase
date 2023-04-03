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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_CREATE_RROFILE_STMT_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_CREATE_RROFILE_STMT_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObUserProfileStmt: public ObDDLStmt
{
public:
  explicit ObUserProfileStmt(common::ObIAllocator *name_pool);
  ObUserProfileStmt();
  virtual ~ObUserProfileStmt();

  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObProfileDDLArg &get_ddl_arg() { return create_profile_arg_; }
  TO_STRING_KV(K_(create_profile_arg));
private:
  // data members
  obrpc::ObProfileDDLArg create_profile_arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUserProfileStmt);
};
} // end namespace sql
} // end namespace oceanbase
#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_CREATE_PROFILE_STMT_
