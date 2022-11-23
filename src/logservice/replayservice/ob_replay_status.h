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

#ifndef OCEANBASE_LOGSERVICE_OB_REPLAY_STATUS_
#define OCEANBASE_LOGSERVICE_OB_REPLAY_STATUS_

#include <stdint.h>
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/lsn.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/palf/palf_handle.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/thread/ob_thread_lease.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace palf
{
class PalfEnv;
}
namespace logservice
{
class ObLogReplayService;
// replay status中含有几种任务类型，它们的含义分别为:
// 1.ObReplayServiceTask: 基类任务, 用于提交到replay service的全局队列中
// 2.ObLogReplayTask: 每一条日志生成的具体回放任务, 聚合日志中的每一条子日志都对应一个独立的ObLogReplayTask
// 3.ObReplayServiceSubmitTask: submit类型任务, 继承ObReplayServiceTask,
//                              在replay status中对应submit_log_task_,
//                              记录该日志流需要回放的日志的起点和终点,
// 4.ObReplayServiceReplayTask: replay类型任务, 继承ObReplayServiceTask,
//                              在replay status中对应task_queues_[i],
//                              用于存放ObLogReplayTask
class ObReplayStatus;
enum class ObReplayServiceTaskType
{
  INVALID_LOG_TASK = 0,
  SUBMIT_LOG_TASK = 1,
  REPLAY_LOG_TASK = 2,
};

//虚拟表统计
struct LSReplayStat
{
  int64_t ls_id_;
  common::ObRole role_;
  palf::LSN end_lsn_;
  bool enabled_;
  palf::LSN unsubmitted_lsn_;
  int64_t unsubmitted_log_ts_ns_;
  int64_t pending_cnt_;

  TO_STRING_KV(K(ls_id_),
               K(role_),
               K(end_lsn_),
               K(enabled_),
               K(unsubmitted_lsn_),
               K(unsubmitted_log_ts_ns_),
               K(pending_cnt_));
};

struct ReplayDiagnoseInfo
{
  palf::LSN max_replayed_lsn_;
  int64_t max_replayed_scn_;
  ObSqlString diagnose_str_;
  TO_STRING_KV(K(max_replayed_lsn_),
               K(max_replayed_scn_));
};

//此类型为前向barrier日志专用, 与ObLogReplayTask分开分配
//因此此结构的内存需要单独释放
struct ObLogReplayBuffer
{
public:
  ObLogReplayBuffer()
  {
    reset();
  }
  ~ObLogReplayBuffer()
  {
    reset();
  }
  void reset();
  int64_t dec_replay_ref();
  void inc_replay_ref();
  int64_t get_replay_ref();
public:
  int64_t ref_; //for pre barrier
  void *log_buf_;
};

struct ObLogReplayTask : common::ObLink
{
public:
  ObLogReplayTask()
  {
    reset();
  }
  virtual ~ObLogReplayTask()
  {
    reset();
  }
  int init(const share::ObLSID &ls_id,
           const ObLogBaseHeader &header,
           const palf::LSN &lsn,
           const int64_t log_ts,
           const int64_t log_size,
           const bool is_raw_write,
           void *log_buf);
  void reset();
  bool is_valid();
  void shallow_copy(const ObLogReplayTask &other);
public:
  share::ObLSID ls_id_;
  ObLogBaseType log_type_;
  palf::LSN lsn_;
  int64_t log_ts_;
  bool is_pre_barrier_;
  bool is_post_barrier_;
  int64_t log_size_;
  int64_t replay_hint_;
  //for standby replay control, need record for cached log replay task;
  bool is_raw_write_;
  int64_t first_handle_ts_;
  int64_t print_error_ts_;
  int64_t replay_cost_; //此任务重试的总耗时时间
  int64_t retry_cost_; //此任务回放成功时的当次处理时间
  void *log_buf_;

  TO_STRING_KV(K(ls_id_),
               K(log_type_),
               K(lsn_),
               K(log_ts_),
               K(is_pre_barrier_),
               K(is_post_barrier_),
               K(log_size_),
               K(replay_hint_),
               K(is_raw_write_),
               K(first_handle_ts_),
               K(replay_cost_),
               K(retry_cost_),
               KP(log_buf_));
};

// replay service task基类
class ObReplayServiceTask
{
public:
  ObReplayServiceTask();
  virtual ~ObReplayServiceTask();
  virtual void reset();
  virtual void destroy();
public:
  //record info after replay failed
  struct TaskErrInfo
  {
  public:
    TaskErrInfo() {reset();}
    ~TaskErrInfo() {reset();}
    void reset();
    TO_STRING_KV(K(has_fatal_error_), K(fail_ts_), K(fail_cost_), K(ret_code_));
  public:
    bool has_fatal_error_;
    int ret_code_;
    int64_t fail_ts_;
    int64_t fail_cost_;
  };

  ObReplayStatus *get_replay_status()
  {
    return replay_status_;
  }
  bool acquire_lease()
  {
    return lease_.acquire();
  }
  bool revoke_lease()
  {
    return lease_.revoke();
  }

  ObReplayServiceTaskType get_type() const
  {
    return type_;
  }
  void set_enqueue_ts(int64_t ts)
  {
    enqueue_ts_ = ts;
  }
  int64_t get_enqueue_ts() const
  {
    return enqueue_ts_;
  }
  void clear_err_info()
  {
    err_info_.reset();
  }
  void clear_err_info(const int64_t cur_ts);
  void set_simple_err_info(const int ret_code, const int64_t fail_ts);
  int get_err_info_ret_code() const
  {
    return err_info_.ret_code_;
  }
  void override_err_info_ret_code(const int ret_code)
  {
    err_info_.ret_code_ = ret_code;
  }
  void set_fatal_err_info(const int ret_code, const int64_t fail_ts);
  bool has_fatal_error() const
  {
    return err_info_.has_fatal_error_;
  }
  bool need_replay_immediately() const;

  VIRTUAL_TO_STRING_KV(K(type_), K(enqueue_ts_), K(err_info_));
protected:
  mutable common::ObSpinLock lock_;
  ObReplayServiceTaskType type_;
  //for debug: task wait in queue too much time
  int64_t enqueue_ts_;
  // 如果只通过linkhashmap管理replay status的生命周期,ObReplayServiceTask里面只存ls_id,
  // 那么在ABA场景下残留的任务会get到新的replay status并且回放,
  // 因此需要任务自己记录replay status,而这样会使得replay status不仅在linkhashmap处被用到.
  // 所以需要replay status自己管理引用计数, linkhashmap的引用计数是冗余的.
  ObReplayStatus *replay_status_;
  TaskErrInfo err_info_;
  //control state transition of queue
  common::ObThreadLease lease_;
};

// need be protected by lock
class ObReplayServiceSubmitTask : public ObReplayServiceTask
{
public:
  ObReplayServiceSubmitTask(): ObReplayServiceTask(),
    next_to_submit_lsn_(),
    committed_end_lsn_(),
    next_to_submit_log_ts_(common::OB_INVALID_TIMESTAMP),
    base_lsn_(),
    base_log_ts_(common::OB_INVALID_TIMESTAMP),
    iterator_(),
    cache_replay_task_(NULL)
  {
    type_ = ObReplayServiceTaskType::SUBMIT_LOG_TASK;
  }
  ~ObReplayServiceSubmitTask()
  {
    destroy();
  }
  int init(const palf::LSN &base_lsn,
           const int64_t base_log_ts,
           palf::PalfHandle *palf_handle,
           ObReplayStatus *replay_status);
  void reset() override;
  void destroy() override;

public:
  // 迭代器是否迭代到终点
  bool has_remained_submit_log();
  // 不允许回退
  int update_next_to_submit_log_info(const palf::LSN &lsn, const int64_t log_ts);
  int update_next_to_submit_lsn(const palf::LSN &lsn);
  int update_next_to_submit_log_ts_allow_equal(const int64_t log_ts);
  int update_committed_end_offset(const palf::LSN &lsn);
  int get_next_to_submit_log_info(palf::LSN &lsn,
                                  int64_t &log_ts) const;
  int get_committed_end_lsn(palf::LSN &lsn) const;
  int get_base_lsn(palf::LSN &lsn) const;
  int get_base_log_ts(int64_t &log_ts) const;
  int need_skip(const int64_t log_ts,
                bool &need_skip);
  bool is_cached_replay_task_exist() const
  {
    return NULL != cache_replay_task_;
  }
  ObLogReplayTask *get_cached_replay_task()
  {
    return cache_replay_task_;
  }
  void cache_replay_task(ObLogReplayTask *log_replay_task)
  {
    cache_replay_task_ = log_replay_task;
  }
  //不释放内存,在cache_replay_task_提交成功后调用
  void clear_cached_replay_task()
  {
    cache_replay_task_ = NULL;
  }
  //释放内存,在废弃cache_replay_task时调用
  void revert_cached_replay_task();

  int get_log(const char *&buffer, int64_t &nbytes, int64_t &ts, palf::LSN &offset, bool &is_raw_write);
  int next_log();
  // 以当前的终点作为新起点重置迭代器
  int reset_iterator(palf::PalfHandle &palf_handle,
                     const palf::LSN &begin_lsn);

  INHERIT_TO_STRING_KV("ObReplayServiceSubmitTask", ObReplayServiceTask,
                       K(next_to_submit_lsn_),
                       K(committed_end_lsn_),
                       K(next_to_submit_log_ts_),
                       K(base_lsn_),
                       K(base_log_ts_));
private:
  int update_next_to_submit_lsn_(const palf::LSN &lsn);
  int update_next_to_submit_log_ts_(const int64_t log_ts);
  int update_next_to_submit_log_ts_allow_equal_(const int64_t log_ts);
  int update_committed_end_lsn_(const palf::LSN &lsn);
  void set_next_to_submit_log_info_(const palf::LSN &lsn, const int64_t log_ts);
  int get_next_to_submit_log_info_(palf::LSN &lsn, int64_t &log_ts) const;
  int get_base_lsn_(palf::LSN &lsn) const;
  int get_base_log_ts_(int64_t &log_ts) const;

private:
  //location of next log after the last log that has already been submit to replay, consider as left side of iterator
  palf::LSN next_to_submit_lsn_;
  //location of the last log that need submit to replay, consider as right side of iterator
  palf::LSN committed_end_lsn_;
  int64_t next_to_submit_log_ts_;
  //initial log lsn when enable replay, for stat replay process
  palf::LSN base_lsn_;
  //initial log ts when enable replay, logs which ts small than this value should skip replay
  int64_t base_log_ts_;
  //for unittest, should be a member not pointer
  palf::PalfBufferIterator iterator_;
  //缓存需要重试的log replay task
  ObLogReplayTask *cache_replay_task_;
};

class ObReplayServiceReplayTask : public ObReplayServiceTask
{
public:
  typedef common::ObLink Link;
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
public:
  ObReplayServiceReplayTask() : ObReplayServiceTask()
  {
    type_ = ObReplayServiceTaskType::REPLAY_LOG_TASK;
    idx_ = -1;
  }
  ~ObReplayServiceReplayTask() { destroy(); }
  // use base_log_ts init min_unreplayed_log_ts;
  int init(ObReplayStatus *replay_status,
           const int64_t idx);
  void reset() override;
  void destroy() override;
public:
  int64_t idx() const;
  Link *top()
  {
    return queue_.top();
  }
  Link *pop()
  {
    ObLockGuard<ObSpinLock> guard(lock_);
    return pop_();
  }
  void push(Link *p)
  {
    queue_.push(p);
  }
  int get_min_unreplayed_log_info(palf::LSN &lsn,
                                  int64_t &log_ts,
                                  int64_t &replay_hint,
                                  ObLogBaseType &log_type,
                                  int64_t &first_handle_ts,
                                  int64_t &replay_cost,
                                  int64_t &retry_cost,
                                  bool &is_queue_empty);
private:
  Link *pop_()
  {
    return queue_.pop();
  }
private:
  common::ObSpScLinkQueue queue_;   //place ObLogReplayTask
  int64_t idx_; //热点行优化
};

class ObReplayFsCb : public palf::PalfFSCb
{
public:
  ObReplayFsCb() : replay_status_(NULL) {}
  ObReplayFsCb(ObReplayStatus *replay_status)
  {
    replay_status_ = replay_status;
  }
  ~ObReplayFsCb()
  {
    destroy();
  }
  void destroy()
  {
    replay_status_ = NULL;
  }
  // 回调接口,调用replay status的update_end_offset接口
  int update_end_lsn(int64_t id, const palf::LSN &end_offset, const int64_t proposal_id);
private:
  ObReplayStatus *replay_status_;
};

class ObReplayStatus
{
public:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef RWLock::WLockGuardWithRetryInterval WLockGuardWithRetryInterval;
public:
  struct LSErrInfo
  {
  public:
    LSErrInfo()
    {
      reset();
    }
    ~LSErrInfo()
    {
      reset();
    }
    void reset() {
      lsn_.reset();
      scn_ = 0;
      log_type_ = ObLogBaseType::INVALID_LOG_BASE_TYPE;
      is_submit_err_ = false;
      err_ts_ = 0;
      err_ret_ = common::OB_SUCCESS;
    }
    TO_STRING_KV(K(lsn_), K(scn_), K(log_type_),
                 K(is_submit_err_), K(err_ts_), K(err_ret_));
  public:
    palf::LSN lsn_;
    uint64_t scn_;
    ObLogBaseType log_type_;
    int64_t replay_hint_;
    bool is_submit_err_;  //is submit log task error occured
    int64_t err_ts_;  //the timestamp that partition encounts fatal error
    int err_ret_;  //the ret code of fatal error
  };
public:
  ObReplayStatus();
  ~ObReplayStatus();
  int init(const share::ObLSID &id,
           const common::ObReplicaType &replica_type,
           palf::PalfEnv *palf_env,
           ObLogReplayService *rp_sv);
  void destroy();
public:
  int enable(const palf::LSN &base_lsn,
             const int64_t base_log_ts);
  int disable();
  // if is_enabled_ is false,
  // means log stream will bedestructed and no logs need to replayed any more.
  bool is_enabled() const;
  // for replay service when holding rdlock
  bool is_enabled_without_lock() const;
  // for follower speed_limit
  // 1. avoid more replay cause OOM because speed_limit cannot work when freeze
  // 2. quick improving max_undecided_log to reduce freeze cost
  void set_pending();
  void erase_pending();

  bool need_submit_log() const;
  bool try_rdlock()
  {
    return rwlock_.try_rdlock();
  }
  void unlock()
  {
    rwlock_.unlock();
  }

  void switch_to_leader();
  void switch_to_follower(const palf::LSN &begin_lsn);
  // check whether all logs has finished replaying
  //
  // during Leader Reconfirm->Leader Takeover，demanding that there is no log that need to be replayed,
  // this function will be invoked and check whether the returned is_done is true
  // @param [out] is_done，true if all logs have been replayed
  //
  // @return : OB_SUCCESS : success
  //           OB_NOT_INIT: ObReplayStatus has not been inited
  int is_replay_done(const palf::LSN &lsn,
                     bool &is_done);
  // 存在待提交的日志
  bool has_remained_submit_log();
  // 存在待回放的已提交日志任务
  bool has_remained_replay_task() const;
  // update right margin of logs that need to replay
  int update_end_offset(const palf::LSN &lsn);

  int push_log_replay_task(ObLogReplayTask &task);
  void inc_pending_task(const int64_t log_size);
  void dec_pending_task(const int64_t log_size);
  //通用的replay task释放内存接口, 前向barrier的任务不会单独释放log buf内存
  //前向barrier完整释放申请的内存需要同时调用
  //free_replay_task_log_buf()和free_replay_task()
  void free_replay_task(ObLogReplayTask *task);
  //单独释放ObLogReplayTask中特殊的log_buf, 仅前向barrier日志生效
  void free_replay_task_log_buf(ObLogReplayTask *task);

  int get_ls_id(share::ObLSID &id);
  int get_min_unreplayed_lsn(palf::LSN &lsn);
  int get_min_unreplayed_log_ts_ns(int64_t &log_ts);
  int get_min_unreplayed_log_info(palf::LSN &lsn,
                                  int64_t &log_ts,
                                  int64_t &replay_hint,
                                  ObLogBaseType &log_type,
                                  int64_t &first_handle_ts,
                                  int64_t &replay_cost,
                                  int64_t &retry_cost);
  int get_replay_process(int64_t &replayed_log_size, int64_t &unreplayed_log_size);
  //提交日志检查barrier状态
  int check_submit_barrier();
  //回放日志检查barrier状态
  int check_replay_barrier(ObLogReplayTask *replay_task,
                           ObLogReplayBuffer *&replay_log_buf,
                           bool &need_replay,
                           const int64_t replay_queue_idx);
  void set_post_barrier_submitted(const palf::LSN &lsn);
  int set_post_barrier_finished(const palf::LSN &lsn);
  int stat(LSReplayStat &stat) const;
  int diagnose(ReplayDiagnoseInfo &diagnose_info);
  inline void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_cnt_, 1);
  }
  inline int64_t calc_replay_queue_idx(const int64_t replay_hint)
  {
    return replay_hint & (REPLAY_TASK_QUEUE_SIZE - 1);
  }
  // 用于记录日志流级别的错误, 此类错误不可恢复
  void set_err_info(const palf::LSN &lsn,
                    const uint64_t scn,
                    const ObLogBaseType &log_type,
                    const int64_t replay_hint,
                    const bool is_submit_err,
                    const int64_t err_ts,
                    const int err_ret);
  bool has_fatal_error() const
  {
    return is_fatal_error(err_info_.err_ret_);
  }
  bool is_fatal_error(const int ret) const;
  bool need_check_memstore(const palf::LSN &lsn)
  {
    return (lsn - last_check_memstore_lsn_) > LS_CHECK_MEMSTORE_INTERVAL_THRESHOLD;
  }
  void set_last_check_memstore_lsn(const palf::LSN &lsn)
  {
    last_check_memstore_lsn_ = lsn;
  }

  TO_STRING_KV(K(ls_id_),
               K(is_enabled_),
               K(is_submit_blocked_),
               K(role_),
               K(err_info_),
               K(ref_cnt_),
               K(post_barrier_lsn_),
               K(pending_task_count_),
               K(submit_log_task_));

private:
  void set_next_to_submit_log_info_(const palf::LSN &lsn, const int64_t log_ts);
  int submit_task_to_replay_service_(ObReplayServiceTask &task);
  // 注册回调并提交当前初始化的submit_log_task
  int enable_(const palf::LSN &base_lsn,
              const int64_t base_log_ts);

  // 注销回调并清空任务
  int disable_();
  bool is_replay_enabled_() const;
private:
  static const int64_t PENDING_COUNT_THRESHOLD = 100;
  static const int64_t EAGAIN_COUNT_THRESHOLD = 50000;
  static const int64_t EAGAIN_INTERVAL_THRESHOLD = 10 * 60 * 1000 * 1000LL;
  static const int64_t REPLAY_TASK_MAGNIFICATION_THRESHOLD = 10;
  //单日志流每次提交16MB日志时需要检查当前租户memstore剩余值是否超限
  static const int64_t LS_CHECK_MEMSTORE_INTERVAL_THRESHOLD = 16 * (1LL << 20);
  //预期一条日志的回放不会超过1s
  static const int64_t WRLOCK_TRY_THRESHOLD = 1000 * 1000;
  static const int64_t WRLOCK_RETRY_INTERVAL = 20 * 1000; //20ms
  bool is_inited_;
  bool is_enabled_;  // forbidden replay and fetch log if false
  bool is_submit_blocked_; // allow replay log if true
  common::ObRole role_;  // leader do not need replay
  share::ObLSID ls_id_;
  // guarantee the effectiveness of self memory:
  // inc_ref() before push task into replay_service, dec_ref() after replay_service finished handling task
  int64_t ref_cnt_;
  // used for barrier demand
  palf::LSN post_barrier_lsn_;
  // record error info, reported when handle submit or replay type task
  LSErrInfo err_info_;
  int64_t pending_task_count_;
  palf::LSN last_check_memstore_lsn_;
  // protect is_enabled_ and submit_log_task_
  // 回放一条日志时会一直持有读锁直到回放完成
  // 保证拿写锁disable后一定不会有任何日志回放
  mutable RWLock rwlock_;
  // protect is_submit_blocked_ and role_
  mutable common::ObSpinLock spinlock_;

  ObLogReplayService *rp_sv_;
  // be sure to clear these queues when the partition is offline to prevent old replay task is replayed in situation of migrating out and then migrating in
  ObReplayServiceReplayTask task_queues_[common::REPLAY_TASK_QUEUE_SIZE];
  ObReplayServiceSubmitTask submit_log_task_;

  palf::PalfEnv *palf_env_;
  palf::PalfHandle palf_handle_;
  ObReplayFsCb fs_cb_;
  mutable int64_t get_log_info_debug_time_;
  mutable int64_t try_wrlock_debug_time_;
  mutable int64_t check_enable_debug_time_;
  DISALLOW_COPY_AND_ASSIGN(ObReplayStatus);
};

// get replay status with ref protection, for map in replay service
class ObReplayStatusGuard
{
public:
  ObReplayStatusGuard(): replay_status_(NULL) {}
  ~ObReplayStatusGuard()
  {
    if (NULL != replay_status_) {
      if (0 == replay_status_->dec_ref()) {
        CLOG_LOG(INFO, "free replay status", KPC(replay_status_));
        replay_status_->~ObReplayStatus();
        share::mtl_free(replay_status_);
      }
      replay_status_ = NULL;
    }
  }
  void set_replay_status(ObReplayStatus *replay_status) {
    replay_status_ = replay_status;
    replay_status_->inc_ref();
  }
  inline ObReplayStatus *get_replay_status() { return replay_status_; }
private:
  ObReplayStatus *replay_status_;
  DISALLOW_COPY_AND_ASSIGN(ObReplayStatusGuard);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_REPLAY_STATUS_
