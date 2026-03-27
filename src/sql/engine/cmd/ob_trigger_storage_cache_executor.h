/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SQL_ENGINE_CMD_OB_TRIGGER_STORAGE_CACHE_EXECUTOR_H_
#define SQL_ENGINE_CMD_OB_TRIGGER_STORAGE_CACHE_EXECUTOR_H_
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
class ObTriggerStorageCacheStmt;

class ObTriggerStorageCacheExecutor
{
public:
  ObTriggerStorageCacheExecutor() {}
  virtual ~ObTriggerStorageCacheExecutor() {}
  int execute(ObExecContext &ctx, ObTriggerStorageCacheStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTriggerStorageCacheExecutor);
};
} // namespace sql
} // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_TRIGGER_EXECUTOR_H_ */
