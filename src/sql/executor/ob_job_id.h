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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_JOB_ID_
#define OCEANBASE_SQL_EXECUTOR_OB_JOB_ID_

#include "sql/executor/ob_execution_id.h"

namespace oceanbase
{
namespace sql
{
class ObJobID final
{
  OB_UNIS_VERSION(1);
public:
  ObJobID(const ObExecutionID &ob_execution_id, uint64_t job_id) :
      ob_execution_id_(ob_execution_id),
      job_id_(job_id),
      root_op_id_(0)
  {
  }
  ObJobID() :
      ob_execution_id_(),
      job_id_(common::OB_INVALID_ID),
      root_op_id_(0)
  {
  }

  inline void set_ob_execution_id(const ObExecutionID &ob_execution_id) { ob_execution_id_ = ob_execution_id; }
  inline void set_server(const common::ObAddr &server) { ob_execution_id_.set_server(server); }
  inline void set_dist_task_type() { ob_execution_id_.set_dist_task_type(); }
  inline void set_mini_task_type() { ob_execution_id_.set_mini_task_type(); }
  inline void set_execution_id(const uint64_t execution_id) { ob_execution_id_.set_execution_id(execution_id); }
  inline void set_job_id(const uint64_t job_id) { job_id_ = job_id; }
  inline void set_root_op_id(uint64_t root_op_id) { root_op_id_ = root_op_id; }
  inline const ObExecutionID &get_ob_execution_id() const { return ob_execution_id_; }
  inline const common::ObAddr &get_server() const { return ob_execution_id_.get_server(); }
  inline uint64_t get_task_type() const { return ob_execution_id_.get_task_type(); }
  inline bool is_dist_task_type() const { return ob_execution_id_.is_dist_task_type(); }
  inline bool is_mini_task_type() const { return ob_execution_id_.is_mini_task_type(); }
  inline uint64_t get_execution_id() const { return ob_execution_id_.get_execution_id(); }
  inline uint64_t get_job_id() const { return job_id_; }
  inline uint64_t get_root_op_id() const { return root_op_id_; }

  inline int64_t hash() const { return ob_execution_id_.hash() + job_id_; }
  inline bool equal(const ObJobID &id) const
  {
    return id.ob_execution_id_.equal(ob_execution_id_) && id.job_id_ == job_id_;
  }
  inline bool operator==(const ObJobID &id) const
  {
    return equal(id);
  }
  inline bool is_valid() const
  {
    return ob_execution_id_.is_valid() &&
           common::OB_INVALID_ID != job_id_;
  }
  inline void reset()
  {
    ob_execution_id_.reset();
    job_id_ = common::OB_INVALID_ID;
  }
  TO_STRING_KV(N_OB_EXECUTION_ID, ob_execution_id_,
               N_JOB_ID, job_id_);
  DECLARE_TO_YSON_KV;
private:
  /* variables */
  ObExecutionID ob_execution_id_;
  uint64_t job_id_;
  uint64_t root_op_id_;
};

} /* ns sql */
} /* ns oceanbase */
#endif /* OCEANBASE_SQL_EXECUTOR_OB_JOB_ID_ */
//// end of header file
