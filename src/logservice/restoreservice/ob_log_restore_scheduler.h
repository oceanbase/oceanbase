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
