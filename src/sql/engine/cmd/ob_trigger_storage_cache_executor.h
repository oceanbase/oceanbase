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
