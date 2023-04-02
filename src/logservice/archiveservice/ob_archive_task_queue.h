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

#ifndef OCEANBASE_ARCHIVE_TASK_QUEUE_H_
#define OCEANBASE_ARCHIVE_TASK_QUEUE_H_

#include "share/ob_ls_id.h"     // ObLSID
#include "ob_archive_util.h"
#include <cstdint>

namespace oceanbase
{
namespace archive
{
class ObArchiveWorker;
using oceanbase::share::ObLSID;
struct ObArchiveTaskStatus : common::ObLink
{
  static const int64_t PRINT_WARN_THRESHOLD = 30 * 1000 * 1000L;   // 30s
public:
  ObArchiveTaskStatus(const ObLSID &id);
  virtual ~ObArchiveTaskStatus();
  int64_t count();
  void inc_ref();
  int push(common::ObLink *task, ObArchiveWorker &worker);
  int pop(ObLink *&link, bool &task_exist);
  int top(ObLink *&link, bool &task_exist);
  int get_next(ObLink *&link, bool &task_exist);
  int retire(bool &is_empty, bool &is_discarded);  // 从全局公共队列释放
  void free(bool &is_discarded);   // 释放该结构体指针
  bool mark_io_error();
  void clear_error_info();
  void print_self();

  TO_STRING_KV(K_(issue),
               K_(ref),
               K_(num),
               K_(id));
private:
  int retire_unlock_(bool &is_discarded);  // 从全局公共队列释放
  void print_self_();

  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;
protected:
  bool                      issue_;     // 标记该结构是否被挂到公共队列
  int64_t                   ref_;       // 该结构的引用计数
  int64_t                   num_;       // 任务总数
  int64_t                   last_issue_timestamp_;
  ObLSID                    id_;
  SimpleQueue               queue_;     // 任务队列
  mutable RWLock            rwlock_;
};
}
}

#endif /* OCEANBASE_ARCHIVE_TASK_QUEUE_H_ */
