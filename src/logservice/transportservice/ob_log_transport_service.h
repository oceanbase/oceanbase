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

#ifndef OCEANBASE_LOGSERVICE_TRANSPORT_SERVICE_
#define OCEANBASE_LOGSERVICE_TRANSPORT_SERVICE_

#include "common/ob_role.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/thread/ob_thread_lease.h"
#include "lib/queue/ob_link_queue.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/palf/palf_handle.h"
#include "logservice/ipalf/ipalf_handle.h"
#include "logservice/ipalf/ipalf_iterator.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "logservice/transportservice/ob_log_transport_rpc_proxy.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_sync_standby_dest_parser.h"

namespace oceanbase
{
namespace share
{
class ObLogRestoreProxyUtil;
class ObRestoreSourceServiceAttr;
}
namespace common
{
class ObAddr;
}
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}
namespace ipalf
{
class IPalfEnv;
class IPalfHandle;
template <class LogEntryType> class IPalfIterator;
}
namespace obrpc
{
class ObLogTransportRpcProxy;
}
namespace logservice
{
class ObLSAdapter;
class ObLogApplyService;
class ObLogReplayService;
class ObLogTransportService;
class LogTransportStatus;
struct LogTransportStat;
struct ObLogTransportReq;

// ObTransportServiceSubmitTask - 从PALF读取日志的任务
// ObTransportServiceStatusTask - 通过RPC发送日志到备库的任务
// ObTransportServiceInitTask - Leader上任后的异步初始化任务
enum class ObTransportServiceTaskType
{
  INVALID_TRANSPORT_TASK = 0,
  TRANSPORT_SUBMIT_TASK = 1,
  TRANSPORT_STATUS_TASK = 2,
  TRANSPORT_INIT_TASK = 3,
};

class ObTransportServiceTask
{
public:
  ObTransportServiceTask();
  virtual ~ObTransportServiceTask();
  virtual void reset();
  virtual void destroy();
public:
  LogTransportStatus *get_transport_status();
  bool acquire_lease();
  bool revoke_lease();
  ObTransportServiceTaskType get_type() const;
  VIRTUAL_TO_STRING_KV(K(type_));
protected:
  mutable common::ObSpinLock lock_;
  ObTransportServiceTaskType type_;
  LogTransportStatus *transport_status_;
  common::ObThreadLease lease_;
};

// 日志传输任务 - 存储在队列中
struct ObLogTransportTask : public common::ObLink
{
public:
  ObLogTransportTask() : ls_id_(), lsn_(), scn_(), log_buf_(nullptr), log_size_(0) {}
  ObLogTransportTask(const share::ObLSID &ls_id,
                    //  const ObLogBaseHeader &header,
                     const palf::LSN &lsn,
                     const share::SCN &scn,
                     const int64_t log_size)
      : ls_id_(ls_id),
      // log_type_(header.get_log_type()),
      lsn_(lsn),
      scn_(scn),
      log_buf_(nullptr),
      log_size_(log_size) {}
  ~ObLogTransportTask() {}

  int init(void *log_buf);
  share::ObLSID ls_id_;
  // ObLogBaseType log_type_;
  palf::LSN lsn_;
  share::SCN scn_;
  const char *log_buf_;
  int64_t log_size_;

  TO_STRING_KV(K(ls_id_), K(lsn_), K(scn_), K(log_size_));
};

// 读取日志任务
class ObTransportServiceSubmitTask : public ObTransportServiceTask
{
public:
  ObTransportServiceSubmitTask();
  ~ObTransportServiceSubmitTask();
  int init(const share::ObLSID &ls_id,
           const palf::LSN &base_lsn,
           const share::SCN &base_scn,
           LogTransportStatus *transport_status);
  void reset() override;
  void destroy() override;

  bool has_remained_submit_log(const share::SCN &transported_point,
                               bool &iterate_end_by_transported_point);
  int update_submit_log_meta_info(const palf::LSN &lsn, const share::SCN &scn);
  int get_next_to_submit_log_info(palf::LSN &lsn, share::SCN &scn) const;
  int get_log(const char *&buffer, int64_t &nbytes, share::SCN &scn,
              palf::LSN &lsn, palf::LSN &end_lsn);
  int next_log(const share::SCN &transported_point,
               bool &iterate_end_by_transported_point);
  int need_skip(const share::SCN &scn, bool &need_skip);
  int reset_iterator(const share::ObLSID &id, const palf::LSN &begin_lsn);

  INHERIT_TO_STRING_KV("ObTransportServiceSubmitTask", ObTransportServiceTask,
                       K(base_lsn_),
                       K(base_scn_),
                       K(next_to_submit_lsn_),
                       K(next_to_submit_scn_),
                       K(iterator_));
private:
  int update_next_to_submit_lsn_(const palf::LSN &lsn);
  int update_next_to_submit_scn_(const share::SCN &scn);
  int get_next_to_submit_log_info_(palf::LSN &lsn, share::SCN &scn) const;

private:
  palf::LSN base_lsn_;   //TODO by ziqi: 确认是否必需
  share::SCN base_scn_;  //TODO by ziqi: 确认是否必需
  palf::LSN next_to_submit_lsn_;
  share::SCN next_to_submit_scn_;
  ipalf::IPalfIterator<ipalf::IGroupEntry> iterator_;
};

// 传输日志任务
class ObTransportServiceStatusTask : public ObTransportServiceTask
{
public:
  typedef common::ObLink Link;
public:
  ObTransportServiceStatusTask();
  ~ObTransportServiceStatusTask();
  int init(LogTransportStatus *transport_status,
           const int64_t idx);
  void reset() override;
  void destroy() override;
public:
  Link *top();
  Link *pop();
  int push(Link *p);
  void inc_total_submit_cb_cnt();
  void inc_total_transport_cb_cnt();
  int64_t get_total_submit_cb_cnt() const;
  int64_t get_total_transport_cb_cnt() const;
  bool is_transport_done();
  int64_t idx() const;
  int get_ls_id(share::ObLSID &id) const;
  // 批量提交相关方法
  bool need_batch_push() const { return need_batch_push_; }
  void set_need_batch_push() { need_batch_push_ = true; }
  void set_batch_push_finish() { need_batch_push_ = false; }
  INHERIT_TO_STRING_KV("ObTransportServiceStatusTask", ObTransportServiceTask,
                       K(total_submit_cb_cnt_),
                       K(last_check_submit_cb_cnt_),
                       K(total_transport_cb_cnt_),
                       K(idx_));
private:
  int64_t total_submit_cb_cnt_;
  int64_t last_check_submit_cb_cnt_;
  int64_t total_transport_cb_cnt_;
  common::ObSpLinkQueue queue_;
  int64_t idx_;
  bool need_batch_push_;  // 标记是否需要提交到全局队列
};

// 后台初始化任务：用于在switch_to_leader后异步执行初始化操作
class ObTransportServiceInitTask : public ObTransportServiceTask
{
public:
  ObTransportServiceInitTask();
  virtual ~ObTransportServiceInitTask();

  int init(const share::ObLSID &ls_id,
           const int64_t proposal_id,
           const palf::SyncMode &sync_mode,
           const palf::LSN &begin_lsn,
           LogTransportStatus *transport_status);

  virtual void reset() override;
  virtual void destroy() override;

  // 执行初始化逻辑
  int do_init();

  VIRTUAL_TO_STRING_KV(K(ls_id_), K(proposal_id_), K(sync_mode_), K(begin_lsn_));

private:
  share::ObLSID ls_id_;
  int64_t proposal_id_;
  palf::SyncMode sync_mode_;
  palf::LSN begin_lsn_;
  int enable_sync_status_(LogTransportStatus *transport_status);
  int update_standby_info_(LogTransportStatus *transport_status,
                           palf::LSN &standby_end_lsn,
                           share::SCN &standby_end_scn);
  int calc_start_point_(LogTransportStatus *transport_status,
                        const palf::LSN &standby_end_lsn,
                        const share::SCN &standby_end_scn,
                        palf::LSN &start_lsn,
                        share::SCN &start_scn);
  int query_standby_ls_location_(LogTransportStatus *transport_status);
};

class ObTransportFsCb : public palf::PalfFSCb
{
public:
  ObTransportFsCb();
  ObTransportFsCb(class LogTransportStatus *transport_status);
  ~ObTransportFsCb();
  void destroy();
  int update_end_lsn(int64_t id,
                     const palf::LSN &end_lsn,
                     const share::SCN &end_scn,
                     const int64_t proposal_id);
private:
  class LogTransportStatus *transport_status_;
};

// 日志传输统计信息
struct LogTransportStat
{
  LogTransportStat() : ls_id_(0), role_(common::ObRole::INVALID_ROLE), proposal_id_(0),
                       palf_committed_end_lsn_(), standby_committed_end_lsn_(),
                       last_sent_lsn_(), last_acked_lsn_() {}
  ~LogTransportStat() {}

  void reset() {
    ls_id_ = 0;
    role_ = common::ObRole::INVALID_ROLE;
    proposal_id_ = 0;
    palf_committed_end_lsn_.reset();
    standby_committed_end_lsn_.reset();
    last_sent_lsn_.reset();
    last_acked_lsn_.reset();
  }

  share::ObLSID ls_id_;
  common::ObRole role_;
  int64_t proposal_id_;
  palf::LSN palf_committed_end_lsn_;
  share::SCN palf_committed_end_scn_;
  palf::LSN standby_committed_end_lsn_;
  share::SCN standby_committed_end_scn_;
  palf::LSN min_committed_end_lsn_;
  palf::LSN last_sent_lsn_;
  palf::LSN last_acked_lsn_;

  TO_STRING_KV(K(ls_id_), K(role_), K(proposal_id_),
               K(palf_committed_end_lsn_), K(standby_committed_end_lsn_),
               K(last_sent_lsn_), K(last_acked_lsn_));
};

struct StandbyAckInfo
{
  StandbyAckInfo() : ls_id_(), committed_end_lsn_(), committed_end_scn_() {}
  ~StandbyAckInfo() {}

  share::ObLSID ls_id_;
  palf::LSN committed_end_lsn_;
  share::SCN committed_end_scn_;

  TO_STRING_KV(K(ls_id_), K(committed_end_lsn_), K(committed_end_scn_));
};

// 日志流传输状态类
// 管理单个日志流的强同步传输状态，包括：
// - PALF端的已提交位点 (palf_committed_end_lsn/scn)
// - 备库端的已确认位点 (standby_committed_end_lsn/scn)
// - 传输任务管理 (submit_task + status_tasks)
//
// 线程安全：
// - lock_ 保护角色切换相关的状态 (role_, proposal_id_, is_enabled_)
// - 位点变量通过原子操作更新，支持无锁读取
// - 通过引用计数 (ref_cnt_) 管理生命周期
class LogTransportStatus
{
  friend class ObTransportServiceInitTask;
public:
  LogTransportStatus();
  ~LogTransportStatus();

public:
  int init(const share::ObLSID &id,
           ipalf::IPalfEnv *palf_env,
           ObLogTransportService *transport_sv);
  void destroy();
  int stop();
  inline void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  inline int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  int switch_to_leader(const int64_t new_proposal_id,
                       const palf::SyncMode &sync_mode,
                       const palf::LSN &begin_lsn);
  int switch_to_follower();
  void mark_ls_gc_state();
  bool is_ls_gc_state() const { return true == ATOMIC_LOAD(&is_ls_gc_state_); }
  int update_palf_committed_end_lsn(const palf::LSN &end_lsn, const share::SCN &end_scn);
  // share::SCN get_palf_committed_end_scn() const;
  int update_standby_committed_end_lsn(const palf::LSN &end_lsn, const share::SCN &end_scn);
  void clear_standby_committed_end_lsn();  // 清除备库位点（降级时使用）
  palf::LSN get_standby_committed_end_lsn() const { return palf::LSN(ATOMIC_LOAD(&standby_committed_end_lsn_.val_)); }
  palf::LSN get_palf_committed_end_lsn() const { return palf::LSN(ATOMIC_LOAD(&palf_committed_end_lsn_.val_)); }
  share::SCN get_palf_committed_end_scn() const { return palf_committed_end_scn_.atomic_load(); }
  share::SCN get_standby_committed_end_scn() const;
  void update_last_sent_lsn(const palf::LSN &lsn);
  void update_last_sent_scn(const share::SCN &scn);
  palf::LSN get_last_sent_lsn() const;
  palf::LSN get_last_acked_lsn() const;
  int register_file_size_cb();
  int unregister_file_size_cb();
  void close_palf_handle();
  int enable_status(const int64_t proposal_id);
  int disable_status(const int64_t proposal_id);
  int get_standby_committed_info(palf::LSN &standby_committed_end_lsn, share::SCN &standby_committed_end_scn) const;
  int submit_read_log_task(const palf::LSN &start_lsn, const share::SCN &start_scn);

  // 统计信息
  int stat(LogTransportStat &stat) const;

  // 获取日志流ID
  int get_ls_id(share::ObLSID &id) const;

  // offline相关
  void reset_proposal_id();
  void reset_meta();

  // 任务管理
  int push_transport_task(ObTransportServiceTask *task);
  int try_submit_transport_tasks();
  int try_handle_transport_task(ObTransportServiceTask *task, bool &is_timeslice_run_out);

  // RPC发送日志相关接口
  int do_transport_task_(ObLogTransportTask *transport_task);

  // 备库地址管理
  int set_standby_addr(const common::ObAddr &standby_addr);
  const common::ObAddr &get_standby_addr() const { return standby_addr_; }
  bool is_standby_addr_valid() const { return standby_addr_valid_; }

  // 获取PALF handle
  ipalf::IPalfHandle *get_palf_handle() const { return palf_handle_; }

  // 获取proposal_id
  int64_t get_proposal_id() const { return proposal_id_; }

  // 状态检查方法
  bool is_enabled() const;
  bool is_enabled_without_lock() const;
  bool need_submit_log() const;
  bool is_sync_mode_enabled() const;  // 检查当前 sync_mode 是否为 SYNC
  bool try_rdlock() { return lock_.try_rdlock(); }
  void unlock() { lock_.unlock(); }
  int push_log_transport_task(ObLogTransportTask &task);
  // 批量提交所有 status_task 到全局队列
  int batch_push_all_status_task_queue();
  // 当sync_standby_dest恢复时，重新提交所有有任务的status_task到全局队列
  int retry_all_status_task_queue();
  int is_standby_sync_done(bool &is_done);
  int get_sync_end_scn(share::SCN &sync_end_scn) const;

  // 检查是否还有未发送的已提交日志
  // 通过比较 last_sent_lsn 和 palf end_lsn 来判断
  // @return OB_EAGAIN 表示还有未发送的日志，OB_SUCCESS 表示没有未发送的日志
  int check_has_remained_committed_log();

  // 周期性重传方法
  int periodic_retry_transport_log();
  // 检查是否需要执行周期性重传
  // - 返回 OB_SUCCESS 且 start_lsn 有效：需要重传（从 start_lsn 开始）
  // - 返回 OB_SUCCESS 且 start_lsn 无效：不需要重传
  // - 返回 OB_EAGAIN：检查太频繁（上层可忽略/稍后再试）
  // - 其他错误码：异常情况
  int reset_retry_iterator(const palf::LSN &start_lsn);

  // 异步提交初始化任务
  int submit_init_task(const int64_t proposal_id,
                       const palf::SyncMode &sync_mode,
                       const palf::LSN &begin_lsn);

  TO_STRING_KV(K(ls_id_),
               K(role_),
               K(proposal_id_),
               K(is_enabled_),
               K(palf_committed_end_lsn_),
               K(palf_committed_end_scn_),
               K(standby_committed_end_lsn_),
               K(last_sent_lsn_),
               K(last_acked_lsn_));

private:
  // 任务管理相关
  int submit_task_to_transport_service_(ObTransportServiceTask &task);

  // RPC发送日志
  int send_log_via_rpc_(const ObLogTransportReq &req, const int64_t timeout_us);
  int get_rpc_proxy_(obrpc::ObLogTransportRpcProxy *&rpc_proxy);

  // 周期性重传方法
  int check_need_periodic_retry_(palf::LSN &start_lsn, bool &need_reset);
  int read_and_send_single_log_();
  int read_and_send_logs_();

private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef RWLock::WLockGuardWithRetryInterval WLockGuardWithRetryInterval;
  const int64_t WRLOCK_RETRY_INTERVAL_US = 20 * 1000;  // 20ms

private:
  bool is_inited_;
  bool is_in_stop_state_; // stop后不能上任, 残留的任务会继续处理
  bool is_ls_gc_state_;  // LS已写过offline日志的标记，为true时强同步不再等待备库位点
  bool is_enabled_; // 传输是否启用
  share::ObLSID ls_id_;
  common::ObRole role_;
  int64_t proposal_id_;
  ObLogTransportService *tp_sv_;
  palf::LSN palf_committed_end_lsn_;
  share::SCN palf_committed_end_scn_;
  palf::LSN standby_committed_end_lsn_;
  share::SCN standby_committed_end_scn_; //TODO: 确认是否必需
  palf::LSN last_sent_lsn_; // 最后发送的LSN
  share::SCN last_sent_scn_; // 最后发送的SCN
  palf::LSN last_acked_lsn_; // 最后确认的LSN
  ipalf::IPalfEnv *palf_env_;
  ipalf::IPalfHandle *palf_handle_;
  ObTransportFsCb fs_cb_;
  int64_t ref_cnt_; // guarantee the effectiveness of self memory
  mutable RWLock lock_; // 保护role_, proposal_id_及is_in_stop_state_
  // 保护周期性重传相关状态，避免在 lock_ 上长时间持锁（get_entry/next 可能触发IO）
  // 仅用于保护：retry_iterator_ / retry_start_lsn_ / last_retry_check_time_us_
  //            / last_standby_lsn_ / last_standby_lsn_update_time_us_
  mutable RWLock retry_lock_;
  // mutable RWLock role_lock_;  // 保护 role_ 的修改和读取

  // 任务管理相关
  ObTransportServiceSubmitTask tp_submit_task_;
  ObTransportServiceStatusTask tp_status_tasks_[TRANSPORT_TASK_QUEUE_SIZE];
  ObTransportServiceInitTask tp_init_task_;

  // 强同步锁相关
  mutable common::SpinRWLock sync_mode_lock_;
  // 备库地址
  common::ObAddr standby_addr_;
  bool standby_addr_valid_;
  mutable int64_t check_enable_debug_time_;

  // 周期性重传相关
  ipalf::IPalfIterator<ipalf::IGroupEntry> retry_iterator_;  // 独立的 iterator 用于周期性重传
  palf::LSN retry_start_lsn_;  // 当前重传的起始 LSN
  int64_t last_retry_check_time_us_;  // 上次检查重传的时间
  palf::LSN last_standby_lsn_;  // 上次检查时的备库 LSN
  int64_t last_standby_lsn_update_time_us_;  // 上次备库 LSN 变化的时间
};

// RPC异步回调类，用于处理日志传输RPC的响应
// 注意：通过引用计数保证 transport_status_ 在回调期间的有效性
template<obrpc::ObRpcPacketCode pcode>
class ObLogTransportRespCallback : public obrpc::ObLogTransportRpcProxy::AsyncCB<pcode>
{
public:
  explicit ObLogTransportRespCallback(LogTransportStatus *transport_status)
    : transport_status_(transport_status)
  {
    // 增加引用计数，防止回调期间 transport_status 被释放
    if (OB_NOT_NULL(transport_status_)) {
      transport_status_->inc_ref();
    }
  }

  ~ObLogTransportRespCallback()
  {
    // 释放引用计数
    if (OB_NOT_NULL(transport_status_)) {
      transport_status_->dec_ref();
      transport_status_ = NULL;
    }
  }

  void set_args(const typename obrpc::ObLogTransportRpcProxy::AsyncCB<pcode>::Request &args) {
    UNUSED(args);
  }

  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const {
    void *buf = alloc(sizeof(*this));
    ObLogTransportRespCallback<pcode> *newcb = NULL;
    if (NULL != buf) {
      // clone 时会自动增加引用计数（在构造函数中）
      newcb = new (buf) ObLogTransportRespCallback<pcode>(transport_status_);
    }
    return newcb;
  }

  int process() {
    int ret = OB_SUCCESS;
    const common::ObAddr &dst = obrpc::ObLogTransportRpcProxy::AsyncCB<pcode>::dst_;
    obrpc::ObRpcResultCode &rcode = obrpc::ObLogTransportRpcProxy::AsyncCB<pcode>::rcode_;
    const typename obrpc::ObLogTransportRpcProxy::AsyncCB<pcode>::Response &resp =
        obrpc::ObLogTransportRpcProxy::AsyncCB<pcode>::result_;

    if (OB_SUCCESS != rcode.rcode_) {
      CLOG_LOG_RET(WARN, rcode.rcode_, "log transport rpc error", K(rcode), K(dst), K(pcode));
    } else if (OB_ISNULL(transport_status_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG_RET(WARN, ret, "transport_status is NULL", K(dst), K(pcode));
    } else {
      // 处理响应：更新备库的committed_end_lsn等信息
      // resp是ObLogSyncStandbyInfo类型
      const logservice::ObLogSyncStandbyInfo &transport_resp =
          static_cast<const logservice::ObLogSyncStandbyInfo &>(resp);

      if (OB_SUCCESS != transport_resp.ret_code_) {
        CLOG_LOG_RET(WARN, transport_resp.ret_code_, "standby returned error",
                     K(dst), K(transport_resp.ret_code_), K(transport_resp.ls_id_));
      } else {
        CLOG_LOG(TRACE, "log transport rpc success", K(dst), K(transport_resp));
      }

      if (OB_SUCCESS != transport_resp.refresh_info_ret_code_) {
        CLOG_LOG_RET(WARN, transport_resp.refresh_info_ret_code_, "refresh info returned error",
                     K(dst), K(transport_resp.refresh_info_ret_code_), K(transport_resp.ls_id_));
      } else if (!transport_resp.standby_committed_end_lsn_.is_valid() || !transport_resp.standby_committed_end_scn_.is_valid()) {
        ret = OB_ERR_UNDEFINED;
        CLOG_LOG_RET(ERROR, ret, "standby committed_end_lsn or standby committed_end_scn is invalid",
                     K(dst), K(transport_resp.ls_id_));
      // RPC接收端会best-effort更新备库的committed位点
      } else {
        // 更新备库的committed_end_lsn
        if (OB_FAIL(transport_status_->update_standby_committed_end_lsn(
                     transport_resp.standby_committed_end_lsn_,
                     transport_resp.standby_committed_end_scn_))) {
          CLOG_LOG_RET(WARN, ret, "update_standby_committed_end_lsn failed",
                       K(dst), K(transport_resp.standby_committed_end_lsn_));
        } else {
          CLOG_LOG(TRACE, "log transport rpc success, updated standby committed_end_lsn",
                   K(resp.ls_id_), K(dst), K(transport_resp.standby_committed_end_lsn_),
                   K(transport_resp.standby_committed_end_scn_));
        }
      }
    }

    return ret;
  }

  void on_timeout() {
    const common::ObAddr &dst = obrpc::ObLogTransportRpcProxy::AsyncCB<pcode>::dst_;
    CLOG_LOG_RET(WARN, OB_TIMEOUT, "log transport rpc timeout", K(dst), K(pcode));
  }

private:
  LogTransportStatus *transport_status_;
};

// for transport_status_map in transport service
class ObTpStatusGuard
{
public:
  ObTpStatusGuard();
  ~ObTpStatusGuard();
  LogTransportStatus *get_transport_status();
private:
  void set_transport_status_(LogTransportStatus *transport_status);
private:
  friend class ObLogTransportService;
  LogTransportStatus *transport_status_;
  DISALLOW_COPY_AND_ASSIGN(ObTpStatusGuard);
};

class ObLogTransportService : public lib::TGTaskHandler
{
  friend class LogTransportStatus;
public:
  ObLogTransportService();
  virtual ~ObLogTransportService();

public:
  int init(ipalf::IPalfEnv *palf_env,
           ObLogApplyService *apply_service,
           ObLogReplayService *replay_service,
           rpc::frame::ObReqTransport *transport);
  int start();
  void stop();
  void wait();
  void destroy();

  // 增/删日志流
  int add_ls(const share::ObLSID &id);
  int remove_ls(const share::ObLSID &id);

  // 处理备库ACK
  int handle_standby_ack(const StandbyAckInfo &ack_info);

  int get_transport_status(const share::ObLSID &id, ObTpStatusGuard &guard);

  // 任务管理
  int push_task(ObTransportServiceTask *task);
  // int submit_service_task(ObTransportServiceTask *task);

  // TGTaskHandler接口实现
  void handle(void *task) override;

  // 强同步模式相关接口
  int enable_sync_mode(const share::ObLSID &id, const int64_t proposal_id);
  int disable_sync_mode(const share::ObLSID &id, const int64_t proposal_id);
  int query_sync_standby_dest(const share::ObLSID &id);
  int clear_standby_info(const share::ObLSID &id);
  // if sync_mode is SYNC, return min(palf_committed_end_scn, standby_committed_end_scn)
  // otherwise, return palf_committed_end_scn
  int get_sync_end_scn(const share::ObLSID &id, share::SCN &sync_end_scn);
  int get_standby_sync_scn(const share::ObLSID &id,
                           share::SCN &palf_sync_scn,
                           share::SCN &standby_sync_scn);

  // 通知其他服务更新standby_committed_end_lsn, 这里需要做成回调通知的方式
  int notify_apply_service(const share::ObLSID &id, const palf::LSN &standby_lsn, const share::SCN &standby_scn);

  // 角色切换接口
  int switch_to_leader(const share::ObLSID &id,
                       const int64_t new_proposal_id,
                       const palf::SyncMode &sync_mode,
                       const palf::LSN &begin_lsn);
  int switch_to_follower(const share::ObLSID &id);
  int mark_ls_gc_state(const share::ObLSID &id);

  // 获取RPC代理
  obrpc::ObLogTransportRpcProxy *get_rpc_proxy();

  // 获取sync_standby_dest结构
  int set_sync_standby_dest(const share::ObSyncStandbyDestStruct &sync_standby_dest);
  int get_sync_standby_dest(share::ObSyncStandbyDestStruct &sync_standby_dest) const;
  int is_standby_sync_done(const share::ObLSID &ls_id, bool &is_done);

private:
  // 查询备库地址
  // @param[in] restore_proxy, ObLogRestoreProxyUtil instance for rpc helper
  // @param[in] service_attr, service attributes of standby cluster
  // @param[in] ls_id, log stream id
  // @param[out] standby_addr, leader address of the log stream in standby cluster
  int query_standby_addr_(share::ObLogRestoreProxyUtil *restore_proxy,
                          const share::ObRestoreSourceServiceAttr &service_attr,
                          const share::ObLSID &ls_id,
                          common::ObAddr &standby_addr);

  // 定时任务：更新备库地址
  class UpdateStandbyAddrTask : public common::ObTimerTask
  {
  public:
    UpdateStandbyAddrTask(ObLogTransportService &transport_service)
      : transport_service_(transport_service) {}
    virtual ~UpdateStandbyAddrTask() {}
    void runTimerTask();
  private:
    // 收集APPEND模式的日志流ID
    int collect_ls_list_(common::ObArray<share::ObLSID> &ls_id_array);
    // 读取并更新全局的sync_standby_dest
    int read_and_update_sync_standby_dest_(share::ObSyncStandbyDestStruct &sync_standby_dest,
                                           bool &is_empty,
                                           bool &sync_standby_dest_changed);
    // 初始化restore_proxy
    int init_restore_proxy_(const share::ObSyncStandbyDestStruct &sync_standby_dest,
                            bool is_empty,
                            share::ObLogRestoreProxyUtil &restore_proxy);
    // 更新单个日志流的备库地址
    int update_single_ls_standby_addr_(const share::ObLSID &ls_id,
                                        const share::ObSyncStandbyDestStruct &sync_standby_dest,
                                        share::ObLogRestoreProxyUtil &restore_proxy,
                                        bool sync_standby_dest_changed);
    // 更新所有日志流的备库地址
    int update_all_ls_standby_addr_(const common::ObArray<share::ObLSID> &ls_id_array,
                                     const share::ObSyncStandbyDestStruct &sync_standby_dest,
                                     share::ObLogRestoreProxyUtil &restore_proxy,
                                     bool sync_standby_dest_changed);
  private:
    ObLogTransportService &transport_service_;
  };

  // 定时任务：周期性重传日志给强同步备库
  class PeriodicRetryTransportTask : public common::ObTimerTask
  {
  public:
    PeriodicRetryTransportTask(ObLogTransportService &transport_service)
      : transport_service_(transport_service) {}
    virtual ~PeriodicRetryTransportTask() {}
    void runTimerTask();
  private:
    ObLogTransportService &transport_service_;
  };

private:
  friend class ObTransportServiceInitTask;
  // 发送日志到备库
  int wait_transport_tasks_complete_(const share::ObLSID &id);
  share::SCN inner_get_replayed_point_();
  int fetch_and_submit_single_log_(LogTransportStatus &transport_status,
                                   ObTransportServiceSubmitTask *submit_task,
                                   palf::LSN &cur_lsn,
                                   share::SCN &cur_log_scn,
                                   int64_t &log_size);

  // 处理不同类型的传输任务
  int handle_submit_task_(ObTransportServiceSubmitTask *submit_task, bool &is_timeslice_run_out);
  int handle_status_task_(ObTransportServiceStatusTask *status_task, bool &is_timeslice_run_out);
  int handle_init_task_(ObTransportServiceInitTask *init_task);
  int remove_all_ls_();
  int revert_transport_status_(LogTransportStatus *transport_status);

public:
  class RemoveTransportStatusFunctor
  {
  public:
    RemoveTransportStatusFunctor() : ret_code_(OB_SUCCESS) {}
    ~RemoveTransportStatusFunctor() {}
    bool operator()(const share::ObLSID &id, LogTransportStatus *transport_status);
    int get_ret_code() const { return ret_code_; }
    TO_STRING_KV(K(ret_code_));
  private:
    int ret_code_;
  };

private:
  static const int64_t TRANSPORT_THREAD_COUNT = 1;
  static const int64_t TRANSPORT_INTERVAL_US = 100 * 1000; // 100ms

private:
  bool is_inited_;
  bool is_running_;
  int tg_id_;

  ipalf::IPalfEnv *palf_env_;
  ObLogApplyService *apply_service_;
  ObLogReplayService *replay_service_;
  common::ObLinearHashMap<share::ObLSID, LogTransportStatus*> transport_status_map_;
  obrpc::ObLogTransportRpcProxy *rpc_proxy_;
  mutable common::SpinRWLock sync_standby_dest_lock_;  // 保护 sync_standby_dest_ 的读写锁
  share::ObSyncStandbyDestStruct sync_standby_dest_;

  // 定时任务相关
  UpdateStandbyAddrTask update_standby_addr_task_;
  common::ObTimer update_standby_addr_timer_;
  PeriodicRetryTransportTask periodic_retry_transport_task_;
  common::ObTimer periodic_retry_transport_timer_;

  static const int64_t UPDATE_STANDBY_ADDR_INTERVAL_US = 1 * 1000 * 1000; // 10秒
  static const int64_t PERIODIC_RETRY_TRANSPORT_INTERVAL_US = 1 * 1000 * 1000; // 1秒

  DISALLOW_COPY_AND_ASSIGN(ObLogTransportService);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_TRANSPORT_SERVICE_