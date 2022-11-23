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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_SLICE_ID_
#define OCEANBASE_SQL_EXECUTOR_OB_SLICE_ID_

#include "share/ob_define.h"
#include "lib/ob_name_def.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/net/ob_addr.h"
#include "sql/executor/ob_task_id.h"

namespace oceanbase
{
namespace sql
{
class ObSliceID final
{
  OB_UNIS_VERSION(1);
public:
  ObSliceID() : ob_task_id_(), slice_id_(common::OB_INVALID_ID) {};

  inline void set_ob_task_id(const ObTaskID &ob_task_id) { ob_task_id_ = ob_task_id; }
  inline void set_ob_job_id(const ObJobID &ob_job_id) { ob_task_id_.set_ob_job_id(ob_job_id); }
  inline void set_server(const common::ObAddr &server) { ob_task_id_.set_server(server); }
  inline void set_execution_id(const uint64_t execution_id) { ob_task_id_.set_execution_id(execution_id); }
  inline void set_job_id(const uint64_t job_id) { ob_task_id_.set_job_id(job_id); }
  inline void set_task_id(const uint64_t task_id) { ob_task_id_.set_task_id(task_id); }
  inline void set_slice_id(const uint64_t slice_id) { slice_id_ = slice_id; }
  inline const ObTaskID &get_ob_task_id() const { return ob_task_id_; }
  inline const ObJobID &get_ob_job_id() const { return ob_task_id_.get_ob_job_id(); }
  inline const common::ObAddr &get_server() const { return ob_task_id_.get_server(); }
  inline uint64_t get_execution_id() const { return ob_task_id_.get_execution_id(); }
  inline uint64_t get_job_id() const { return ob_task_id_.get_job_id(); }
  inline uint64_t get_task_id() const { return ob_task_id_.get_task_id(); }
  inline uint64_t get_slice_id() const { return slice_id_; }

  inline int64_t hash() const { return ob_task_id_.hash() + slice_id_; }
  inline bool equal(const ObSliceID &id) const
  {
    return id.ob_task_id_.equal(ob_task_id_)
        && id.slice_id_ == slice_id_;
  }
  inline bool operator==(const ObSliceID &id) const
  {
    return equal(id);
  }
  inline bool is_valid() const
  {
    return ob_task_id_.is_valid() &&
           common::OB_INVALID_ID != slice_id_;
  }

  inline void reset()
  {
    ob_task_id_.reset();
    slice_id_ = common::OB_INVALID_ID;
  }

  TO_STRING_KV(N_OB_TASK_ID, ob_task_id_,
               N_SLICE_ID, slice_id_);

private:
  /* variables */
  ObTaskID ob_task_id_;
  uint64_t slice_id_;
};

}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_SLICE_ID_ */
//// end of header file
