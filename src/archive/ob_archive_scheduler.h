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

#ifndef OCEANBASE_ARCHIVE_SCHEDULER_H_
#define OCEANBASE_ARCHIVE_SCHEDULER_H_

#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace archive {
class ObArCLogSplitEngine;
class ObArchiveSender;
class ObArchiveAllocator;
class ObArchiveScheduler : public share::ObThreadPool {
public:
  ObArchiveScheduler();
  ~ObArchiveScheduler();

public:
  int init(ObArchiveAllocator* archive_allocator, ObArCLogSplitEngine* clog_split_eg, ObArchiveSender* sender);
  void destroy();
  int start();
  void stop();
  void wait();

private:
  void run1();
  void do_thread_task_();

private:
  bool inited_;
  bool stop_flag_;

  ObArchiveAllocator* archive_allocator_;
  ObArCLogSplitEngine* clog_split_eg_;
  ObArchiveSender* archive_sender_;
};

}  // namespace archive
}  // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_SCHEDULER_H_ */
