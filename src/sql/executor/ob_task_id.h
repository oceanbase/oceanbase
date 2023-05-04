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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_TASK_ID_
#define OCEANBASE_SQL_EXECUTOR_OB_TASK_ID_

#include "sql/executor/ob_job_id.h"
#include "lib/json/ob_yson.h"

namespace oceanbase
{
namespace sql
{

class ObTaskID final
{
  OB_UNIS_VERSION(1);
public:
  ObTaskID(const ObJobID &ob_job_id, uint64_t task_id) :
      ob_job_id_(ob_job_id),
      task_id_(task_id),
      task_cnt_(0)
  {
  }
  ObTaskID() :
      ob_job_id_(),
      task_id_(common::OB_INVALID_ID),
      task_cnt_(0)
  {
  }

  inline void set_ob_job_id(const ObJobID &ob_job_id) { ob_job_id_ = ob_job_id; }
  inline void set_server(const common::ObAddr &server) { ob_job_id_.set_server(server); }
  inline void set_execution_id(const uint64_t execution_id)
  {
    ob_job_id_.set_execution_id(execution_id);
  }
  inline void set_job_id(const uint64_t job_id) { ob_job_id_.set_job_id(job_id); }
  inline void set_task_id(const uint64_t task_id) { task_id_ = task_id; }
  inline void set_task_cnt(const uint64_t cnt) { task_cnt_ = cnt; }
  inline void set_dist_task_type() { ob_job_id_.set_dist_task_type(); }
  inline void set_mini_task_type() { ob_job_id_.set_mini_task_type(); }
  inline const ObJobID &get_ob_job_id() const { return ob_job_id_; }
  inline const common::ObAddr &get_server() const { return ob_job_id_.get_server(); }
  inline uint64_t get_execution_id() const { return ob_job_id_.get_execution_id(); }
  inline const ObExecutionID &get_ob_execution_id() const
  { return ob_job_id_.get_ob_execution_id(); }
  inline uint64_t get_job_id() const { return ob_job_id_.get_job_id(); }
  inline uint64_t get_root_op_id() const { return ob_job_id_.get_root_op_id(); }
  inline uint64_t get_task_id() const { return task_id_; }
  inline uint64_t get_task_cnt() const { return task_cnt_; }
  inline uint64_t get_task_type() const { return ob_job_id_.get_task_type(); }
  inline bool is_dist_task_type() const { return ob_job_id_.is_dist_task_type(); }
  inline bool is_mini_task_type() const { return ob_job_id_.is_mini_task_type(); }

  inline int64_t hash() const { return ob_job_id_.hash() + task_id_; }
  inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  inline bool equal(const ObTaskID &id) const
  {
    return id.ob_job_id_.equal(ob_job_id_) && id.task_id_ == task_id_;
  }
  inline bool operator==(const ObTaskID &id) const
  {
    return equal(id);
  }
  inline bool is_valid() const
  {
    return ob_job_id_.is_valid() && common::OB_INVALID_ID != task_id_;
  }
  inline void reset()
  {
    ob_job_id_.reset();
    task_id_ = common::OB_INVALID_ID;
    task_cnt_ = 0;
  }

  TO_STRING_KV(N_OB_JOB_ID, ob_job_id_,
               N_TASK_ID, task_id_,
               K_(task_cnt));
  TO_YSON_KV(OB_ID(job_id), ob_job_id_,
             OB_ID(task_id), task_id_);
private:
  /* variables */
  ObJobID ob_job_id_;
  uint64_t task_id_;
  // Task cnt only used in index build task for macro sequence id partition,
  //   // may not set in other tasks.
  uint64_t task_cnt_;
};

}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_ID_ */
//// end of header file
