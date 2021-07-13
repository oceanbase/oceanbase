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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_SCHEDULER_THREAD_CTX_
#define OCEANBASE_SQL_EXECUTOR_OB_SCHEDULER_THREAD_CTX_

#include "sql/ob_sql_context.h"
#include "sql/executor/ob_task_info.h"
namespace oceanbase {
namespace sql {
class ObDistributedExecContext;

class ObSchedulerThreadCtx {
public:
  explicit ObSchedulerThreadCtx(ObIAllocator& allocator)
      : scheduler_retry_info_(),
        dis_exec_ctx_(NULL),
        is_plain_select_stmt_(false),
        is_build_index_plan_(false),
        sche_allocator_(allocator),
        last_failed_partitions_(sche_allocator_)
  {}
  virtual ~ObSchedulerThreadCtx()
  {}

  inline const ObQueryRetryInfo& get_scheduler_retry_info() const
  {
    return scheduler_retry_info_;
  }
  inline ObQueryRetryInfo& get_scheduler_retry_info_for_update()
  {
    return scheduler_retry_info_;
  }
  const ObDistributedExecContext* get_dis_exec_ctx() const
  {
    return dis_exec_ctx_;
  }
  ObDistributedExecContext* get_dis_exec_ctx_for_update() const
  {
    return dis_exec_ctx_;
  }
  void set_dis_exec_ctx(ObDistributedExecContext* dis_exec_ctx)
  {
    dis_exec_ctx_ = dis_exec_ctx;
  }
  void set_plain_select_stmt(bool is_plain_select)
  {
    is_plain_select_stmt_ = is_plain_select;
  }
  bool is_plain_select_stmt() const
  {
    return is_plain_select_stmt_;
  }
  int init_last_failed_partition(int64_t count)
  {
    return last_failed_partitions_.init(count);
  }
  int add_last_failed_partition(const ObTaskInfo::ObRangeLocation& loc);
  inline const common::ObIArray<ObTaskInfo::ObRangeLocation*>& get_last_failed_partitions() const
  {
    return last_failed_partitions_;
  }
  inline void clear_last_failed_partitions()
  {
    last_failed_partitions_.reset();
  }
  inline TransResult& get_trans_result()
  {
    return trans_result_;
  }
  inline void clear_trans_result()
  {
    trans_result_.clear_stmt_result();
  }

  void set_build_index_plan(const bool v)
  {
    is_build_index_plan_ = v;
  }
  bool is_build_index_plan(void) const
  {
    return is_build_index_plan_;
  }

private:
  ObQueryRetryInfo scheduler_retry_info_;
  ObDistributedExecContext* dis_exec_ctx_;
  bool is_plain_select_stmt_;
  bool is_build_index_plan_;
  ObIAllocator& sche_allocator_;
  // record info of failed task, used for retry.
  common::ObFixedArray<ObTaskInfo::ObRangeLocation*, common::ObIAllocator> last_failed_partitions_;
  TransResult trans_result_;

private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObSchedulerThreadCtx);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_SCHEDULER_THREAD_CTX_ */
