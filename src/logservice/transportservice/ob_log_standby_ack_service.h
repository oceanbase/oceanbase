/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOGSERVICE_STANDBY_ACK_SERVICE_
#define OCEANBASE_LOGSERVICE_STANDBY_ACK_SERVICE_

#include "lib/hash/ob_link_hashmap.h"
#include "lib/thread/ob_thread_lease.h"
#include "lib/queue/ob_link_queue.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_handle.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "logservice/transportservice/ob_log_transport_rpc_proxy.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"
#include "logservice/ipalf/ipalf_env.h"
#include "logservice/ipalf/ipalf_handle.h"
#include "lib/task/ob_timer.h"
#include "observer/ob_uniq_task_queue.h"

namespace oceanbase
{
namespace ipalf
{
class IPalfEnv;
class IPalfHandle;
}
namespace logservice
{
class ObLogStandbyAckService;
class StandbyAckTask;
class StandbyAckStatus;
using StandbyAckTaskQueue = observer::ObUniqTaskQueue<StandbyAckTask, ObLogStandbyAckService>;

// 定时发送 ACK 任务
class StandbyPeriodicAckTask : public common::ObTimerTask
{
public:
  StandbyPeriodicAckTask() : ack_service_(nullptr) {}
  virtual ~StandbyPeriodicAckTask() {}

  void set_ack_service(ObLogStandbyAckService *ack_service) { ack_service_ = ack_service; }
  virtual void runTimerTask() override;

private:
  ObLogStandbyAckService *ack_service_;
};

// 主库信息结构体
struct PrimaryInfo
{
  PrimaryInfo()
    : primary_addr_(),
      primary_cluster_id_(OB_INVALID_CLUSTER_ID),
      primary_tenant_id_(OB_INVALID_TENANT_ID),
      cached_ts_(0) {}

  ~PrimaryInfo() {}

  void reset()
  {
    primary_addr_.reset();
    primary_cluster_id_ = OB_INVALID_CLUSTER_ID;
    primary_tenant_id_ = OB_INVALID_TENANT_ID;
    cached_ts_ = 0;
  }

  common::ObAddr primary_addr_;
  int64_t primary_cluster_id_;
  uint64_t primary_tenant_id_;
  int64_t cached_ts_;  // 缓存时间戳

  TO_STRING_KV(K(primary_addr_), K(primary_cluster_id_), K(primary_tenant_id_), K(cached_ts_));
};

// SyncMode 缓存信息结构体
struct SyncModeInfo
{
  SyncModeInfo()
    : mode_version_(0),
      sync_mode_(ipalf::SyncMode::INVALID_SYNC_MODE),
      is_sync_mode_(false),
      cached_ts_(0) {}

  ~SyncModeInfo() {}

  void reset()
  {
    mode_version_ = 0;
    sync_mode_ = ipalf::SyncMode::INVALID_SYNC_MODE;
    is_sync_mode_ = false;
    cached_ts_ = 0;
  }

  int64_t mode_version_;
  ipalf::SyncMode sync_mode_;
  bool is_sync_mode_;  // 是否为 SYNC 模式
  int64_t cached_ts_;  // 缓存时间戳

  TO_STRING_KV(K(mode_version_), K(sync_mode_), K(is_sync_mode_), K(cached_ts_));
};

// ACK 发送任务
// 同一个日志流在队列里通过ls_id去重，每次处理时取最大的lsn和scn。无需在task中存储lsn和scn。
struct StandbyAckTask : public observer::ObIUniqTaskQueueTask<StandbyAckTask>
{
public:
  StandbyAckTask() : ls_id_() {}

  StandbyAckTask(const share::ObLSID &ls_id)
    : ls_id_(ls_id) {}
  ~StandbyAckTask() {}

  // ObIUniqTaskQueueTask 接口实现
  bool is_barrier() const override { return false; }
  bool need_process_alone() const override { return true; }
  int64_t hash() const override { return ls_id_.hash(); }
  int hash(uint64_t &hash_val) const override { hash_val = hash(); return common::OB_SUCCESS; }
  bool compare_without_version(const StandbyAckTask &other) const override { return ls_id_ == other.ls_id_; }
  uint64_t get_group_id() const override { return 0; }
  void reset() override {
    ls_id_.reset();
  }
  bool is_valid() const override { return ls_id_.is_valid(); }
  bool operator ==(const StandbyAckTask &other) const override { return ls_id_ == other.ls_id_; }

  share::ObLSID get_ls_id() const { return ls_id_; }
  TO_STRING_KV(K(ls_id_));

private:
  share::ObLSID ls_id_;
};

// 备库的 PalfFSCb 回调类，用于在强同步模式下发送 committed_end_lsn 和 committed_end_scn 给主库
class ObStandbyFsCb : public palf::PalfFSCb
{
public:
  ObStandbyFsCb();
  ~ObStandbyFsCb();
  int init(const share::ObLSID &ls_id, obrpc::ObLogTransportRpcProxy *rpc_proxy, StandbyAckStatus *ack_status);
  void destroy();
  bool is_inited() const { return is_inited_; }
  int update_end_lsn(int64_t id,
                     const palf::LSN &end_lsn,
                     const share::SCN &end_scn,
                     const int64_t proposal_id);

  // 获取 ack_service 用于提交任务
  void set_ack_service(ObLogStandbyAckService *ack_service) { ack_service_ = ack_service; }
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  obrpc::ObLogTransportRpcProxy *rpc_proxy_;
  ObLogStandbyAckService *ack_service_;  // 用于提交任务到独立线程
  StandbyAckStatus *ack_status_;
};

// 备库 ACK 服务，用于在强同步模式下向主库发送 committed_end_lsn 和 committed_end_scn
class ObLogStandbyAckService
{
public:
  ObLogStandbyAckService();
  virtual ~ObLogStandbyAckService();

public:
  int init(ipalf::IPalfEnv *palf_env, rpc::frame::ObReqTransport *transport);
  int start();
  void stop();
  void wait();
  void destroy();

  // 增/删日志流
  int add_ls(const share::ObLSID &id);
  int remove_ls(const share::ObLSID &id);

  // 获取RPC代理
  obrpc::ObLogTransportRpcProxy *get_rpc_proxy();

  // 提交 ACK 发送任务
  int push_ack_task(const StandbyAckTask &task);

  // 更新 sync_mode 缓存（用于快速检查后更新缓存，避免独立线程重复加锁）
  int update_sync_mode_cache(const share::ObLSID &ls_id,
                              int64_t mode_version,
                              ipalf::SyncMode sync_mode,
                              bool is_sync_mode);

  // 从缓存读取 sync_mode（无锁，快速检查，供 ObStandbyFsCb 使用）
  int get_sync_mode_from_cache(const share::ObLSID &ls_id, bool &is_sync_mode);
  int init_sync_mode_cache(const share::ObLSID &ls_id);

  // 定时发送 ACK（遍历所有 LS 并发送，供 StandbyPeriodicAckTask 调用）
  int periodic_send_ack_();

  // ObUniqTaskQueue 需要的接口：批量处理任务
  int batch_process_tasks(const common::ObIArray<StandbyAckTask> &tasks, bool &stopped);
  int process_barrier(const StandbyAckTask &task, bool &stopped);

private:

  // 获取主库信息（地址、集群ID、租户ID）
  int get_primary_info_(const share::ObLSID &ls_id,
                        common::ObAddr &primary_addr,
                        int64_t &primary_cluster_id,
                        uint64_t &primary_tenant_id);

  // 检查是否处于强同步模式
  int check_sync_mode_(const share::ObLSID &ls_id, bool &is_sync_mode);

  // 发送 RPC 给主库
  int send_ack_to_primary_(const share::ObLSID &ls_id,
                           const palf::LSN &committed_end_lsn,
                           const share::SCN &committed_end_scn);

  int revert_ack_status_(class StandbyAckStatus *ack_status);
  int register_file_size_cb_(const share::ObLSID &id);
  int unregister_file_size_cb_(const share::ObLSID &id);
  int get_current_end_lsn_scn_(const share::ObLSID &id, palf::LSN &lsn, share::SCN &scn);
  int remove_all_ls();

public:
  class RemoveStandbyStatusFunctor
  {
    public:
      RemoveStandbyStatusFunctor() : ret_code_(OB_SUCCESS) {}
      ~RemoveStandbyStatusFunctor() {}
      bool operator()(const share::ObLSID &id, StandbyAckStatus *ack_status);
      int get_ret_code() const { return ret_code_; }
      TO_STRING_KV(K(ret_code_));
    private:
      int ret_code_;
  };

private:
  bool is_inited_;
  bool is_running_;
  StandbyAckTaskQueue ack_task_queue_;  // ACK 任务队列
  int timer_tg_id_;  // 定时器线程组 ID
  ipalf::IPalfEnv *palf_env_;
  common::ObLinearHashMap<share::ObLSID, class StandbyAckStatus*> ack_status_map_;
  obrpc::ObLogTransportRpcProxy *rpc_proxy_;
  StandbyPeriodicAckTask periodic_ack_task_;  // 定时任务

  // 主库信息缓存（按 LS 缓存）
  common::ObSpinLock cache_lock_;
  common::ObLinearHashMap<share::ObLSID, PrimaryInfo> primary_info_map_;
  static const int64_t PRIMARY_INFO_CACHE_EXPIRE_US = 10 * 1000 * 1000; // 10秒
  // SyncMode 缓存（按 LS 缓存）
  common::ObLinearHashMap<share::ObLSID, SyncModeInfo> sync_mode_map_;
  static const int64_t SYNC_MODE_CACHE_EXPIRE_US = 5 * 1000 * 1000; // 5秒
  // 定时发送 ACK 间隔（1秒）
  static const int64_t PERIODIC_ACK_INTERVAL_US = 1 * 1000 * 1000; // 1秒

  DISALLOW_COPY_AND_ASSIGN(ObLogStandbyAckService);
};

// 备库 ACK 服务状态
class StandbyAckStatus
{
public:
  StandbyAckStatus();
  ~StandbyAckStatus();

  int init(const share::ObLSID &ls_id,
           ipalf::IPalfEnv *palf_env,
           ObLogStandbyAckService *ack_service);
  void destroy();
  int stop();

  int register_file_size_cb();
  int unregister_file_size_cb();

  share::ObLSID get_ls_id() const { return ls_id_; }

  // 获取当前的 committed_end_lsn 和 committed_end_scn
  int get_current_end_lsn_scn(palf::LSN &end_lsn, share::SCN &end_scn);
  int get_current_end_lsn_scn_from_palf(palf::LSN &end_lsn, share::SCN &end_scn);
  int update_current_end_lsn_scn(const palf::LSN &end_lsn, const share::SCN &end_scn);
  int get_access_mode(palf::AccessMode &access_mode);
  inline void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  inline int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  TO_STRING_KV(K(ls_id_), K(ref_cnt_));

private:
  bool is_inited_;
  bool is_in_stop_state_;
  share::ObLSID ls_id_;
  ObLogStandbyAckService *ack_service_;
  ipalf::IPalfEnv *palf_env_;
  ipalf::IPalfHandle *palf_handle_;
  ObStandbyFsCb fs_cb_;
  int64_t ref_cnt_; // guarantee the effectiveness of self memory
  ObLatch end_lsn_scn_lock_;
  palf::LSN committed_end_lsn_;
  share::SCN committed_end_scn_;
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_STANDBY_ACK_SERVICE_
