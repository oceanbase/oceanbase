/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_CATALOG_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_CATALOG_EXECUTOR_H_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCatalogStmt;
class ObCatalogExecutor
{
public:
  ObCatalogExecutor() {}
  virtual ~ObCatalogExecutor() {}
  int execute(ObExecContext &ctx, ObCatalogStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogExecutor);
};

class ObSetCatalogExecutor
{
public:
  ObSetCatalogExecutor() {}
  virtual ~ObSetCatalogExecutor() {}
  int execute(ObExecContext &ctx, ObCatalogStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetCatalogExecutor);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_CATALOG_EXECUTOR_H_ */
