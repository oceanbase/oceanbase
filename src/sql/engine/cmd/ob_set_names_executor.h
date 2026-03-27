/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
