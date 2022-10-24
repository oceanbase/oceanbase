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

#ifndef OB_UPGRADE_STORAGE_FORMAT_VERSION_EXECUTOR_H_
#define OB_UPGRADE_STORAGE_FORMAT_VERSION_EXECUTOR_H_

#include "lib/thread/ob_async_task_queue.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace rootserver
{
class ObUpgradeStorageFormatVersionExecutor;
class ObRootService;
class ObDDLService;
class ObUpgradeStorageFormatVersionTask : public share::ObAsyncTask
{
public:
  explicit ObUpgradeStorageFormatVersionTask(ObUpgradeStorageFormatVersionExecutor &executor)
    : executor_(&executor)
  {}
  virtual ~ObUpgradeStorageFormatVersionTask() = default;
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  virtual int process() override;
private:
  ObUpgradeStorageFormatVersionExecutor *executor_;
};

class ObUpgradeStorageFormatVersionExecutor
{
public:
  ObUpgradeStorageFormatVersionExecutor();
  ~ObUpgradeStorageFormatVersionExecutor() = default;
  int init(ObRootService &root_service, ObDDLService &ddl_service);
  int execute();
  int can_execute();
  void start();
  int stop();
private:
  int set_execute_mark();
  int check_stop();
  int upgrade_storage_format_version();
  int check_schema_sync();
private:
  bool is_inited_;
  bool is_stopped_;
  bool execute_;
  common::SpinRWLock rwlock_;
  ObRootService *root_service_;
  ObDDLService *ddl_service_;
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeStorageFormatVersionExecutor);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OB_UPGRADE_STORAGE_FORMAT_VERSION_EXECUTOR_H_
