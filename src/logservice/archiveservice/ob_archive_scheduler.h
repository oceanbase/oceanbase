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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_SCHEDULER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_SCHEDULER_H_

#include "lib/utility/ob_macro_utils.h"
#include <cstdint>
namespace oceanbase
{
namespace archive
{
class ObArchiveFetcher;
class ObArchiveSender;
class ObArchiveAllocator;
// modify archive resource parameters and monitor memory usage and so on
// until now, it modify sender and fetcher threads, cached buffer usage
class ObArchiveScheduler
{
public:
  ObArchiveScheduler();
  ~ObArchiveScheduler();

  int init(const uint64_t tenant_id,
      ObArchiveFetcher *fetcher,
      ObArchiveSender *sender,
      ObArchiveAllocator *allocator);
  void destroy();
  int schedule();

private:
  int modify_thread_count_();
  int purge_cached_buffer_();
private:
  bool inited_;
  uint64_t tenant_id_;
  ObArchiveFetcher *fetcher_;
  ObArchiveSender *sender_;
  ObArchiveAllocator *allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveScheduler);
};
} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_SCHEDULER_H_ */
