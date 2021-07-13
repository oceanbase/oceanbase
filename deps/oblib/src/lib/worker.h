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

#ifndef _OCEABASE_LIB_WORKER_H_
#define _OCEABASE_LIB_WORKER_H_

#include <cstdint>
#include "lib/coro/co.h"
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_sql_arena_allocator.h"
#include "lib/rc/context.h"
#include "lib/runtime.h"

namespace oceanbase {
namespace lib {
using common::ObArenaAllocator;
using common::ObSQLArenaAllocator;

class Worker {
public:
  enum class CompatMode { INVALID = -1, MYSQL, ORACLE };

  Worker();
  virtual ~Worker() = default;

  virtual int check_status()
  {
    return common::OB_SUCCESS;
  }

  // This function is called before worker waiting for some resources
  // and starting to give cpu out so that Multi-Tenancy would be aware
  // of this and assign cpu resource to another worker if necessary.
  //
  // Return:
  //   1. true    wait successfully
  //   2. false   wait fail, should cancel this invocation
  bool sched_wait();

  // This function is opposite to `omt_sched_wait'. It notify
  // Multi-Tenancy that this worker has got enough resource and want to
  // run. Then Multi-Tenancy would judge whether the worker own the
  // right to go ahead.
  //
  // Return:
  //   1. true   the worker has right to go ahead
  //   2. false  the worker hasn't right to go ahead
  bool sched_run(int64_t waittime = 0);

  ObIAllocator& get_sql_arena_allocator();
  ObIAllocator& get_allocator();

  void set_req_flag(bool v);
  bool has_req_flag();

  void set_worker_level(const int32_t level)
  {
    worker_level_ = level;
  }
  int32_t get_worker_level() const
  {
    return worker_level_;
  }

  void set_curr_request_level(const int32_t level)
  {
    curr_request_level_ = level;
  }
  int32_t get_curr_request_level() const
  {
    return curr_request_level_;
  }

  void set_group_id(int32_t group_id)
  {
    group_id_ = group_id;
  }
  int32_t get_group_id() const
  {
    return group_id_;
  }

public:
  // static variables
  static Worker& self();

protected:
  static RLOCAL(Worker*, self_);

protected:
  // Thread runtime-memory is allocated from this allocator
  // Initial tenant_id=500, when request is processed, it is updated to tenant id of request
  // Ctx_id can be specified separately, this ctx_id remains unchanged
  ObIAllocator* allocator_;

private:
  bool req_flag_;

  int32_t worker_level_;
  int32_t curr_request_level_;
  int32_t group_id_;

  DISALLOW_COPY_AND_ASSIGN(Worker);
};  // end of class Worker

extern void* alloc_worker();
inline Worker& Worker::self()
{
  // wbuf won't been NULL.
  if (OB_ISNULL(self_)) {
    self_ = reinterpret_cast<Worker*>(alloc_worker());
  }
  return *self_;
}

inline void Worker::set_req_flag(bool v)
{
  req_flag_ = v;
}

inline bool Worker::has_req_flag()
{
  return req_flag_;
}

inline ObIAllocator& Worker::get_sql_arena_allocator()
{
  return CURRENT_CONTEXT.get_arena_allocator();
}

inline ObIAllocator& Worker::get_allocator()
{
  // Expected to be called only during request processing
  abort_unless(allocator_ != nullptr);
  return *allocator_;
}

inline Worker& this_worker()
{
  return oceanbase::lib::Worker::self();
}

// used to check compatibility mode.
class ObRuntimeContext : public lib::RuntimeContext {
  OB_UNIS_VERSION(1);

public:
  ObRuntimeContext() : compat_mode_(Worker::CompatMode::MYSQL)
  {}
  Worker::CompatMode compat_mode_;
};

inline ObRuntimeContext& get_ob_runtime_context()
{
  return *static_cast<ObRuntimeContext*>(get_runtime_context());
}

inline Worker::CompatMode get_compat_mode()
{
  return get_ob_runtime_context().compat_mode_;
}

inline void set_compat_mode(Worker::CompatMode mode)
{
  get_ob_runtime_context().compat_mode_ = mode;
}

inline bool is_oracle_mode()
{
  return get_compat_mode() == Worker::CompatMode::ORACLE;
}
inline bool is_mysql_mode()
{
  return get_compat_mode() == Worker::CompatMode::MYSQL;
}

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _OCEABASE_LIB_OB_WORKER_H_ */
