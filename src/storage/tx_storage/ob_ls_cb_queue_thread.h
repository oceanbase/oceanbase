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

#ifndef OCEANBASE_STORAGE_OB_LS_CB_QUEUE_THREAD_
#define OCEANBASE_STORAGE_OB_LS_CB_QUEUE_THREAD_

#include "lib/lock/ob_spin_lock.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/cpu/ob_cpu_topology.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{

class ObRoleHandler;

enum ObRoleChangeCbTaskType : char
{
  // just for retry
  LS_ON_LEADER_TAKEOVER_TASK = 0,

  LS_LEADER_REVOKE_TASK = 1,
  LS_LEADER_TAKEOVER_TASK = 2,
  LS_LEADER_ACTIVE_TASK = 3
};

class ObRoleChangeCbTask : public common::ObLink
{
public:
  ObRoleChangeCbTask()
    : succeed_(true),
      task_type_(),
      ret_code_(OB_SUCCESS),
      retry_cnt_(0),
      ls_id_(),
      role_handler_(nullptr) {}
  ~ObRoleChangeCbTask() { reset(); }
  void reset()
  {
    succeed_ = true;
    ret_code_ = OB_SUCCESS;
    retry_cnt_ = 0;
    ls_id_.reset();
    role_handler_ = nullptr;
  }

  bool is_valid() const
  {
    return (ls_id_.is_valid()
            && retry_cnt_>= 0
            && OB_NOT_NULL(role_handler_));
  }
  TO_STRING_KV("task_type", task_type_,
               "ls_id", ls_id_,
               "succeed", succeed_,
               "ret_code", ret_code_,
               "retry_count", retry_cnt_);
public:
  bool succeed_;
  ObRoleChangeCbTaskType task_type_;
  int ret_code_;
  int64_t retry_cnt_;
  share::ObLSID ls_id_;
  ObRoleHandler *role_handler_;
};

class ObRoleChangeCbQueueThread : public lib::TGTaskHandler
{
public:
  static int64_t QUEUE_THREAD_NUM;
  static const int64_t MINI_MODE_QUEUE_THREAD_NUM = 2;
  static const int64_t MAX_FREE_TASK_NUM = 1024;
  ObRoleChangeCbQueueThread();
  virtual ~ObRoleChangeCbQueueThread();
public:
  virtual int init(uint64_t tenant_id, int tg_id);
  virtual int push(const ObRoleChangeCbTask *task);
  virtual void handle(void *task);
  virtual void destroy();
  int get_tg_id() const { return tg_id_; }
private:
  int get_task(ObRoleChangeCbTask *& task);
  void free_task(ObRoleChangeCbTask * task);
  int alloc_task(ObRoleChangeCbTask *& task);
private:
  bool inited_;
  uint64_t tenant_id_;
  common::ObLinkQueue free_queue_;
  int tg_id_;
  int free_task_num_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRoleChangeCbQueueThread);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LS_CB_QUEUE_THREAD_
