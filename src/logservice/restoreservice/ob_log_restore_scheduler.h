/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_SCHEDULER_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_SCHEDULER_H_

#include "lib/utility/ob_macro_utils.h"
#include "share/restore/ob_log_restore_source.h"   // ObLogRestoreSourceType
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
class ObLogRestoreAllocator;
class ObRemoteFetchWorker;
class ObLogRestoreScheduler
{
public:
  ObLogRestoreScheduler();
  ~ObLogRestoreScheduler();

  int init(const uint64_t tenant_id, ObLogRestoreAllocator *allocator, ObRemoteFetchWorker *worker);
  void destroy();
  int schedule(const share::ObLogRestoreSourceType &source_type);

private:
  int modify_thread_count_(const share::ObLogRestoreSourceType &source_type);
  int purge_cached_buffer_();

private:
  bool inited_;
  uint64_t tenant_id_;
  ObRemoteFetchWorker *worker_;
  ObLogRestoreAllocator *allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreScheduler);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_SCHEDULER_H_ */
