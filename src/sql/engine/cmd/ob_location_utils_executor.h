/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_LOCATION_UTILS_EXECUTOR_H
#define _OB_LOCATION_UTILS_EXECUTOR_H 1
#include "sql/resolver/cmd/ob_location_utils_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObLocationUtilsExecutor
{
public:
  ObLocationUtilsExecutor() {}
  virtual ~ObLocationUtilsExecutor() {}

  int execute(ObExecContext &ctx, ObLocationUtilsStmt &stmt);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLocationUtilsExecutor);
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_LOCATION_UTILS_EXECUTOR_H */
