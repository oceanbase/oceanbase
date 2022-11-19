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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_EXECUTION_ID_
#define OCEANBASE_SQL_EXECUTOR_OB_EXECUTION_ID_

#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace sql
{
// execution_type, no more than 255.
static const uint64_t ET_DIST_TASK = 0;   // dist task must be 0 for compatibility.
static const uint64_t ET_MINI_TASK = 1;

class ObExecutionID final
{
  OB_UNIS_VERSION(1);
public:
public:
  ObExecutionID(const common::ObAddr &server, uint64_t execution_id)
    : server_(server),
      execution_id_(execution_id),
      task_type_(ET_DIST_TASK)
  {}
  ObExecutionID()
    : server_(),
      execution_id_(common::OB_INVALID_ID),
      task_type_(ET_DIST_TASK)
  {}

  inline void set_server(const common::ObAddr &server) { server_ = server; }
  inline void set_execution_id(const uint64_t execution_id) { execution_id_ = execution_id; }
  inline void set_task_type(uint64_t task_type) { task_type_ = task_type; }
  inline void set_dist_task_type() { task_type_ = ET_DIST_TASK; }
  inline void set_mini_task_type() { task_type_ = ET_MINI_TASK; }
  inline const common::ObAddr &get_server() const { return server_; }
  inline uint64_t get_execution_id() const { return execution_id_; }
  inline uint64_t get_task_type() const { return task_type_; }
  inline bool is_dist_task_type() const { return task_type_ == ET_DIST_TASK; }
  inline bool is_mini_task_type() const { return task_type_ == ET_MINI_TASK; }

  inline bool equal(const ObExecutionID &id) const
  {
    return id.server_ == server_
        && id.execution_id_ == execution_id_
        && id.task_type_ == task_type_;
  }
  inline int64_t hash() const
  {
    //server地址一般都相同，这里计算server的hash值意义不大，在并发较大的情况下还白白浪费CPU
    return common::murmurhash(&execution_id_, sizeof(execution_id_), 0);
  }
  int compare(const ObExecutionID &other) const;
  inline bool operator==(const ObExecutionID &id) const
  {
    return equal(id);
  }
  inline bool is_valid() const
  {
    return server_.is_valid()
        && common::OB_INVALID_ID != execution_id_;
  }
  inline void reset()
  {
    server_.reset();
    execution_id_ = common::OB_INVALID_ID;
  }
  //日志中输出hash value，便于排查问题的时候在日志中关联各个阶段的执行逻辑
  TO_STRING_KV(N_SERVER, server_,
               N_EXECUTION_ID, execution_id_,
               N_TASK_TYPE, task_type_,
               "hash", static_cast<uint64_t>(hash()));
  DECLARE_TO_YSON_KV;

  // fake control server address for global execution_id
  static const common::ObAddr &global_id_addr();
private:
  common::ObAddr server_;
  uint64_t execution_id_;
  union
  {
    uint64_t execution_flag_;
    struct {
      uint64_t task_type_:8;
      uint64_t reserved_:56;
    };
  };
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_EXECUTION_ID_ */
