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

#ifndef OCEANBASE_ARCHIVE_ALLOCATOR_H_
#define OCEANBASE_ARCHIVE_ALLOCATOR_H_
#include "lib/allocator/ob_small_allocator.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace archive {
class ObPGArchiveCLogTask;
class ObArchiveSendTask;
class ObArchiveSendTaskStatus;
class ObArchiveCLogTaskStatus;
class ObArchiveAllocator {
public:
  ObArchiveAllocator();
  ~ObArchiveAllocator();
  int init();

public:
  ObPGArchiveCLogTask* alloc_clog_split_task();
  void free_clog_split_task(ObPGArchiveCLogTask* task);

  ObArchiveSendTask* alloc_send_task(const int64_t buf_len);
  void free_send_task(ObArchiveSendTask* task);
  int64_t get_send_task_capacity();

  ObArchiveSendTaskStatus* alloc_send_task_status(const common::ObPGKey& pg_key);
  void free_send_task_status(ObArchiveSendTaskStatus* status);

  ObArchiveCLogTaskStatus* alloc_clog_task_status(const common::ObPGKey& pg_key);
  void free_clog_task_status(ObArchiveCLogTaskStatus* status);

  int set_archive_batch_buffer_limit();

private:
  bool inited_;
  common::ObSmallAllocator clog_task_allocator_;
  common::ObConcurrentFIFOAllocator send_task_allocator_;
  common::ObSmallAllocator send_task_status_allocator_;
  common::ObSmallAllocator clog_task_status_allocator_;
};
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_ALLOCATOR_H_ */
