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

#ifndef OCEANBASE_EXECUTOR_OB_BKGD_DIST_TASK_H_
#define OCEANBASE_EXECUTOR_OB_BKGD_DIST_TASK_H_
#include "share/scheduler/ob_dag_scheduler.h"
#include "sql/executor/ob_task_id.h"
#include "sql/executor/ob_task.h"

namespace oceanbase {
namespace sql {

// dag information for background executing distributed task
class ObBKGDDistTaskDag : public share::ObIDag {
public:
  ObBKGDDistTaskDag();

  int init(const uint64_t tenant_id, const ObTaskID& task_id, const uint64_t scheduler_id);

  virtual ~ObBKGDDistTaskDag();

  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  virtual int64_t get_tenant_id() const override
  {
    return tenant_id_;
  };
  virtual int fill_comment(char* buf, const int64_t len) const override;

  const ObTaskID& get_task_id() const
  {
    return task_id_;
  }
  uint64_t get_scheduler_id() const
  {
    return scheduler_id_;
  }
  virtual int64_t get_compat_mode() const override;

private:
  uint64_t tenant_id_;
  ObTaskID task_id_;
  uint64_t scheduler_id_;

  DISALLOW_COPY_AND_ASSIGN(ObBKGDDistTaskDag);
};

// background executing distributed task
class ObBKGDDistTask : public share::ObITask {
public:
  ObBKGDDistTask();
  virtual ~ObBKGDDistTask();

  int init(const common::ObAddr& addr, const common::ObString& task, const int64_t abs_timeout_us);

  virtual int process() override;
  int get_index_tid(const ObTask& task, uint64_t& tid) const;

private:
  class ObDistTaskProcessor;

  common::ObAddr return_addr_;
  common::ObString serialized_task_;
  int64_t abs_timeout_us_;
  int64_t create_time_us_;  // task create time
  common::ObCurTraceId::TraceId trace_id_;
  common::ObMalloc task_allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObBKGDDistTask);
};

// background executing distributed task global schedule info (scheduled by RS).
// Memory are self managed, so we disable default copy constructor and assign function.
class ObSchedBKGDDistTask {
public:
  ObSchedBKGDDistTask() : tenant_id_(common::OB_INVALID_ID), abs_timeout_us_(0), scheduler_id_(0)
  {}

  virtual ~ObSchedBKGDDistTask()
  {
    destroy();
  }

  ObSchedBKGDDistTask(const ObSchedBKGDDistTask&) = delete;
  ObSchedBKGDDistTask& operator=(const ObSchedBKGDDistTask&) = delete;

  int init(const uint64_t tenant_id, const int64_t abs_timeout_us, const ObTaskID& task_id, uint64_t scheduler_id,
      const common::ObPartitionKey& pkey, const common::ObAddr& des, const common::ObString& serialized_task);
  void destroy();

  int init_execute_over_task(const ObTaskID& task_id);

  int assign(const ObSchedBKGDDistTask& o);

  // RS use partition key to identify task, partition key must be unique in schedule queue.
  // We use task id instead, so we wrap task id to partition key for RS scheduler.
  // (can not use %pkey_ directly)
  void to_schedule_pkey(common::ObPartitionKey& pkey) const;

  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ && abs_timeout_us_ > 0 && task_id_.is_valid() && scheduler_id_ > 0 &&
           pkey_.is_valid() && dest_.is_valid() && !serialized_task_.empty();
  }

  TO_STRING_KV(K_(tenant_id), K_(abs_timeout_us), K_(pkey), K_(dest), K(serialized_task_.length()));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_abs_timeout_us() const
  {
    return abs_timeout_us_;
  }
  const ObTaskID& get_task_id() const
  {
    return task_id_;
  }
  uint64_t get_scheduler_id() const
  {
    return scheduler_id_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const common::ObAddr& get_dest() const
  {
    return dest_;
  }
  const common::ObString& get_serialized_task() const
  {
    return serialized_task_;
  }

private:
  uint64_t tenant_id_;
  int64_t abs_timeout_us_;
  ObTaskID task_id_;
  uint64_t scheduler_id_;
  common::ObPartitionKey pkey_;
  common::ObAddr dest_;
  common::ObString serialized_task_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_EXECUTOR_OB_BKGD_DIST_TASK_H_
