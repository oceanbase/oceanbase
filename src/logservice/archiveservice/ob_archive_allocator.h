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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_ALLOCATOR_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_ALLOCATOR_H_

#include "lib/allocator/ob_small_allocator.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "large_buffer_pool.h"
#include <cstdint>

namespace oceanbase
{
namespace share
{
class ObLSID;
}

namespace archive
{
class ObArchiveLogFetchTask;
class ObArchiveSendTask;
class ObArchiveTaskStatus;
using oceanbase::share::ObLSID;
class ObArchiveAllocator
{
public:
  ObArchiveAllocator();
  ~ObArchiveAllocator();
  int init(const uint64_t tenant_id);
  void destroy();

public:
  // allocate ObArchiveLogFetchTask
  ObArchiveLogFetchTask *alloc_log_fetch_task();

  // free ObArchiveLogFetchTask
  void free_log_fetch_task(ObArchiveLogFetchTask *task);

  // allocate ObArchiveSendTask, include SendTask and log buffer
  char *alloc_send_task(const int64_t buf_len);

  // free ObArchiveSendTask, include SendTask and log buffer
  void free_send_task(void *buf);

  // week out cached send task buffer
  void weed_out_send_task();

  // alloc log handle buffer
  void *alloc_log_handle_buffer(const int64_t size);

  // free log handle buffer
  void free_log_handle_buffer(char *buf);

  // alloc send task status
  ObArchiveTaskStatus *alloc_send_task_status(const ObLSID &id);

  // free send task status
  void free_send_task_status(ObArchiveTaskStatus *status);

  void print_state();
private:
  typedef LargeBufferPool ArchiveSendAllocator;
  bool                                  inited_;
  common::ObSmallAllocator              log_fetch_task_allocator_;
  //common::ObConcurrentFIFOAllocator     send_task_allocator_;
  ArchiveSendAllocator                  send_task_allocator_;
  common::ObSmallAllocator              send_task_status_allocator_;
};
} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_ALLOCATOR_H_*/
