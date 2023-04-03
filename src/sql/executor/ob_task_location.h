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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_TASK_LOCATION_
#define OCEANBASE_SQL_EXECUTOR_OB_TASK_LOCATION_
#include "sql/executor/ob_task_id.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
namespace oceanbase
{
namespace sql
{
class ObTaskLocation final
{
  OB_UNIS_VERSION(1);
public:
  ObTaskLocation(const common::ObAddr &server, const ObTaskID &ob_task_id);
  ObTaskLocation();

  ObTaskLocation& operator=(const ObTaskLocation &task_location);
  void reset();

  inline void set_server(const common::ObAddr &server) {server_ = server;}
  inline const common::ObAddr &get_server() const {return server_;}
  inline void set_ob_task_id(const ObTaskID &ob_task_id) { ob_task_id_ = ob_task_id; }
  inline const ObTaskID &get_ob_task_id() const { return ob_task_id_; }
  inline void set_ob_job_id(const ObJobID &ob_job_id) {ob_task_id_.set_ob_job_id(ob_job_id);}
  inline const ObJobID &get_ob_job_id() const {return ob_task_id_.get_ob_job_id();}
  inline void set_ctrl_server(const common::ObAddr &server) {return ob_task_id_.set_server(server);}
  inline const common::ObAddr &get_ctrl_server() const {return ob_task_id_.get_server();}
  inline void set_execution_id(uint64_t execution_id) {ob_task_id_.set_execution_id(execution_id);}
  inline uint64_t get_execution_id() const {return ob_task_id_.get_execution_id();}
  inline void set_job_id(uint64_t job_id) {ob_task_id_.set_job_id(job_id);}
  inline uint64_t get_job_id() const {return ob_task_id_.get_job_id();}
  inline void set_task_id(uint64_t task_id) {ob_task_id_.set_task_id(task_id);}
  inline uint64_t get_task_id() const {return ob_task_id_.get_task_id();}
  inline bool is_valid() const {return server_.is_valid() && ob_task_id_.is_valid();}
  inline int64_t hash() const {return server_.hash() + ob_task_id_.hash();}
  inline bool equal(const ObTaskLocation &task_loc) const
  {
    return task_loc.server_ == server_
        && task_loc.ob_task_id_.equal(ob_task_id_);
  }
  TO_STRING_KV(N_SERVER, server_,
               N_OB_TASK_ID, ob_task_id_);
private:
  common::ObAddr server_;//中间结果所在的server
  ObTaskID ob_task_id_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_LOCATION_ */
