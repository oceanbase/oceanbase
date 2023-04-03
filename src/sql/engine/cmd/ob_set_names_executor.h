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

#ifndef _OB_SET_NAMES_EXECUTOR_H
#define _OB_SET_NAMES_EXECUTOR_H 1
#include "sql/resolver/cmd/ob_set_names_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObSetNamesExecutor
{
public:
  ObSetNamesExecutor() {}
  virtual ~ObSetNamesExecutor() {}

  int execute(ObExecContext &ctx, ObSetNamesStmt &stmt);
private:
  int get_global_sys_var_character_set_client(ObExecContext &ctx, common::ObString &character_set_client) const;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSetNamesExecutor);
  // function members
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SET_NAMES_EXECUTOR_H */
