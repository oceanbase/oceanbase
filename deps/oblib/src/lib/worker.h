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
#include "lib/ob_define.h"
#include "lib/rc/context.h"
#include "lib/runtime.h"
#include "common/errsim_module/ob_errsim_module_type.h"

namespace oceanbase
{
namespace rpc { class ObRequest; }
namespace sql { class ObSQLSessionInfo; }
namespace lib
{
using common::ObIAllocator;

enum class LogReductionMode {NONE = 0, REFINED, COMPRESSED};

class Worker
{
public:
  enum class CompatMode {INVALID = -1, MYSQL, ORACLE };
  enum Status { WS_NOWAIT, WS_INVALID, WS_OUT_OF_THROTTLE };

  Worker();
  virtual ~Worker();

  virtual Status check_wait();
  virtual int check_status() { check_wait(); return common::OB_SUCCESS; }
  virtual int check_large_query_quota() { return common::OB_SUCCESS; }
  // check if retry disabled for the query
  virtual bool can_retry() const { return false; }
  // Set retry flag so that scheduler will reprocess this request then
  virtual void set_need_retry() {}
  virtual void unset_need_retry() {}
  // It's used to check whether query need retry. Whenever worker has
  // observed this query need retry, it should stop processing this
  // query immediately.
  virtual bool need_retry() const { return false; }
  virtual void resume() {}

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
  bool sched_run(int64_t waittime=0);

  OB_INLINE ObIAllocator& get_sql_arena_allocator() { return CURRENT_CONTEXT->get_arena_allocator(); }
  ObIAllocator &get_allocator() ;

  void set_req_flag(const rpc::ObRequest *cur_request) { cur_request_ = cur_request; }
  bool has_req_flag() { return OB_NOT_NULL(cur_request_); }
  const rpc::ObRequest *get_cur_request() { return cur_request_; }
  OB_INLINE void set_worker_level(const int32_t level) { worker_level_ = level; }
  OB_INLINE int32_t get_worker_level() const { return worker_level_; }

  OB_INLINE void set_curr_request_level(const int32_t level) { curr_request_level_ = level; }
  OB_INLINE int32_t get_curr_request_level() const { return curr_request_level_; }
  OB_INLINE bool is_th_worker() const { return is_th_worker_; }
  OB_INLINE void set_group_id_(const uint64_t group_id) { group_id_ = group_id;}

  OB_INLINE uint64_t get_group_id() const { return group_id_; }
  OB_INLINE void set_group(void *group) { group_ = group; };
  OB_INLINE void *get_group() { return group_;};
  OB_INLINE bool is_group_worker() const { return OB_NOT_NULL(group_); }

  //OB_INLINE void set_group_id(int32_t group_id) { group_id_ = group_id; }

  OB_INLINE void set_func_type_(uint8_t func_type) { func_type_ = func_type; }
  OB_INLINE uint8_t get_func_type() const { return func_type_; }
  OB_INLINE void set_rpc_stat_srv(void *rpc_stat_srv) { rpc_stat_srv_ = rpc_stat_srv; }
  OB_INLINE void *get_rpc_stat_srv() const { return rpc_stat_srv_; }

  OB_INLINE bool is_timeout_ts_valid() { return INT64_MAX != timeout_ts_;}
  OB_INLINE void set_timeout_ts(int64_t timeout_ts) { timeout_ts_ = timeout_ts; }
  OB_INLINE int64_t get_timeout_ts() const { return timeout_ts_; }
  OB_INLINE void set_ntp_offset(int64_t offset) { ntp_offset_ = offset; }
  OB_INLINE int64_t get_ntp_offset() const { return ntp_offset_; }
  int64_t get_timeout_remain() const;
  bool is_timeout() const;

  OB_INLINE void set_rpc_tenant(uint64_t tenant_id) { rpc_tenant_id_ = tenant_id; }
  OB_INLINE void reset_rpc_tenant() { rpc_tenant_id_ = 0; }
  OB_INLINE uint64_t get_rpc_tenant() const { return rpc_tenant_id_; }

  // check wait is disabled if f is true
  void set_disable_wait_flag(bool f) { disable_wait_ = f; }
  bool get_disable_wait_flag() const { return disable_wait_; }

  void set_sql_throttle_current_priority(int64_t st_current_priority)
  { st_current_priority_ = st_current_priority; }
  void reset_sql_throttle_current_priority()
  { set_sql_throttle_current_priority(100); }

  OB_INLINE void set_session(sql::ObSQLSessionInfo* session) { session_ = session; }
  OB_INLINE sql::ObSQLSessionInfo *get_session() { return session_; }

public:
  static void set_compatibility_mode(CompatMode mode);
  static CompatMode get_compatibility_mode();
  static LogReductionMode get_log_reduction_mode();
  static void set_log_reduction_mode(const LogReductionMode log_reduction_mode);
  static Worker& self();
  static void set_worker_to_thread_local(Worker *worker);

#ifdef ERRSIM
  static void set_module_type(const ObErrsimModuleType &module_type);
  static ObErrsimModuleType get_module_type();
#endif
protected:
  OB_INLINE void set_is_th_worker(bool is_th_worker) { is_th_worker_ = is_th_worker; }
public:
  static __thread Worker *self_;

public:
  common::ObDLinkNode<Worker*> worker_node_;
  void *group_;
protected:
  // 线程运行时内存从此分配器分配
  // 初始tenant_id=500, 在处理request时，tenant_id被更新成request的租户id
  // 可单独指定ctx_id, 此ctx_id保持不变
  ObIAllocator *allocator_;
  int64_t st_current_priority_;
  sql::ObSQLSessionInfo *session_;
private:
  const rpc::ObRequest *cur_request_;
  // whether worker is in blocking
  int32_t worker_level_;
  int32_t curr_request_level_;
  bool is_th_worker_;
  uint64_t group_id_;
  uint8_t func_type_;
  void *rpc_stat_srv_;

  int64_t timeout_ts_;

  //ingnore net time, equal to (receive_ts - send_ts).
  int64_t ntp_offset_;

  uint64_t rpc_tenant_id_;

  // Used to prevent the thread holding the lock from being suspended by check_wait
  bool disable_wait_;

  DISALLOW_COPY_AND_ASSIGN(Worker);
}; // end of class Worker

extern void *alloc_worker();
extern int common_yield();

inline void Worker::set_worker_to_thread_local(Worker *worker)
{
  self_ = worker;
}

inline Worker &Worker::self()
{
  // wbuf won't been NULL.
  if (OB_ISNULL(self_)) {
    self_ = reinterpret_cast<Worker*>(alloc_worker());
  }
  return *self_;
}

inline ObIAllocator &Worker::get_allocator()
{
  // 预期只在处理请求过程中调用
  abort_unless(allocator_ != nullptr);
  return *allocator_;
}

inline Worker &this_worker()
{
  return oceanbase::lib::Worker::self();
}

#define THIS_WORKER oceanbase::lib::Worker::self()

#define GET_FUNC_TYPE() (THIS_WORKER.get_func_type())
#define SET_FUNCTION_TYPE(func_type) (THIS_WORKER.set_func_type_(func_type))

#define GET_GROUP_ID() (THIS_WORKER.get_group_id())

int SET_GROUP_ID(uint64_t group_id, bool is_background = false);

int CONVERT_FUNCTION_TYPE_TO_GROUP_ID(const uint8_t function_type, uint64_t &group_id);

class ConsumerGroupIdGuard
{
public:
  ConsumerGroupIdGuard(uint64_t group_id)
    : thread_group_id_(GET_GROUP_ID()), group_changed_(false), ret_(OB_SUCCESS)
  {
    group_changed_ = group_id != thread_group_id_;
    if (group_changed_) {
      ret_ = SET_GROUP_ID(group_id);
    }
  }
  ~ConsumerGroupIdGuard()
  {
    if (group_changed_) {
      SET_GROUP_ID(thread_group_id_);
    }
  }
  int get_ret()
  {
    return ret_;
  }

private:
  uint64_t thread_group_id_;
  bool group_changed_;
  int ret_;
};
#define CONSUMER_GROUP_ID_GUARD(group_id) oceanbase::lib::ConsumerGroupIdGuard consumer_group_id_guard_(group_id)

class ConsumerGroupFuncGuard
{
public:
  ConsumerGroupFuncGuard(uint8_t func_type)
    : thread_group_id_(GET_GROUP_ID()), thread_func_type_(GET_FUNC_TYPE()), group_changed_(false), ret_(OB_SUCCESS)
  {
    THIS_WORKER.set_func_type_(func_type);
    if (is_resource_manager_group(thread_group_id_)) {
      // has set group id. do nothing.
    } else {
      uint64_t group_id = 0;
      ret_ = CONVERT_FUNCTION_TYPE_TO_GROUP_ID(func_type, group_id);
      if (OB_SUCCESS == ret_ && group_id != thread_group_id_) {
        group_changed_ = true;
        ret_ = SET_GROUP_ID(group_id, true /* is_background */);
      }
    }
  }
  ~ConsumerGroupFuncGuard()
  {
    THIS_WORKER.set_func_type_(thread_func_type_);
    if (group_changed_) {
      SET_GROUP_ID(thread_group_id_);
    }
  }
  int get_ret()
  {
    return ret_;
  }

private:
  uint64_t thread_group_id_;
  uint8_t thread_func_type_;
  bool group_changed_;
  int ret_;
};
#define CONSUMER_GROUP_FUNC_GUARD(func_type) oceanbase::lib::ConsumerGroupFuncGuard consumer_group_func_guard_(func_type)

class DisableSchedInterGuard
{
public:
  DisableSchedInterGuard()
  {
    last_flag_ = THIS_WORKER.get_disable_wait_flag();
    THIS_WORKER.set_disable_wait_flag(true);
  }
  ~DisableSchedInterGuard()
  {
    THIS_WORKER.set_disable_wait_flag(last_flag_);
  }
private:
  bool last_flag_;
};

class CompatModeGuard
{
public:
  CompatModeGuard(Worker::CompatMode mode)
  {
    last_compat_mode_ = THIS_WORKER.get_compatibility_mode();
    THIS_WORKER.set_compatibility_mode(mode);
  }

  ~CompatModeGuard()
  {
    THIS_WORKER.set_compatibility_mode(last_compat_mode_);
  }

private:
  Worker::CompatMode last_compat_mode_;
};


#ifdef ERRSIM
//set current errsim module in code snippet and set last errsim module when guard destructor
class ErrsimModuleGuard final
{
public:
  ErrsimModuleGuard(ObErrsimModuleType::TYPE type)
  {
    last_type_ = THIS_WORKER.get_module_type().type_;
    ObErrsimModuleType curr_type(type);
    THIS_WORKER.set_module_type(curr_type);
  }

  ~ErrsimModuleGuard()
  {
    ObErrsimModuleType curr_type(last_type_);
    THIS_WORKER.set_module_type(curr_type);
  }

private:
  ObErrsimModuleType::TYPE last_type_;
};
#endif

struct ObExtraRpcHeader {
  OB_UNIS_VERSION(1);
public:
  ObExtraRpcHeader()
      : src_addr_()
  {}
  ObExtraRpcHeader(const ObAddr &addr)
      : src_addr_(addr)
  {}
  int assign(const ObExtraRpcHeader &arg)
  {
    int ret = OB_SUCCESS;
    src_addr_ = arg.src_addr_;
    return ret;
  }
  void reset()
  {
    src_addr_.reset();
  }
  ObAddr src_addr_;
  TO_STRING_KV(K(src_addr_));
};

// used to check compatibility mode and save extra rpc packet header.
class ObRuntimeContext
{
  OB_UNIS_VERSION(1);
public:
  ObRuntimeContext()
      : compat_mode_(Worker::CompatMode::MYSQL),
        log_reduction_mode_(LogReductionMode::NONE)
  {}
  Worker::CompatMode compat_mode_;
#ifdef ERRSIM
  ObErrsimModuleType module_type_;
#endif
  LogReductionMode log_reduction_mode_;
  ObExtraRpcHeader extra_rpc_header_;
};

inline ObRuntimeContext &get_ob_runtime_context()
{
  RLOCAL_INLINE(ObRuntimeContext, default_rtctx);
  return default_rtctx;
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

OB_INLINE void Worker::set_compatibility_mode(Worker::CompatMode mode)
{
  set_compat_mode(mode);
}

OB_INLINE Worker::CompatMode Worker::get_compatibility_mode()
{
  return get_compat_mode();
}

OB_INLINE bool is_log_reduction() { return get_ob_runtime_context().log_reduction_mode_ != LogReductionMode::NONE; }

OB_INLINE LogReductionMode get_log_reduction() { return get_ob_runtime_context().log_reduction_mode_; }

OB_INLINE void set_log_reduction(const LogReductionMode log_reduction_mode)
{
  get_ob_runtime_context().log_reduction_mode_ = log_reduction_mode;
}

OB_INLINE LogReductionMode Worker::get_log_reduction_mode() { return get_log_reduction(); }

OB_INLINE void Worker::set_log_reduction_mode(const LogReductionMode log_reduction_mode)
{
  set_log_reduction(log_reduction_mode);
}


OB_INLINE ObAddr &get_rpc_src_addr()
{
  return get_ob_runtime_context().extra_rpc_header_.src_addr_;
}

#ifdef ERRSIM
OB_INLINE void Worker::set_module_type(const ObErrsimModuleType &module_type)
{
  get_ob_runtime_context().module_type_ = module_type;
}

OB_INLINE ObErrsimModuleType Worker::get_module_type()
{
  return get_ob_runtime_context().module_type_;
}
#endif


} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_OB_WORKER_H_ */
