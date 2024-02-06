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

#ifndef OCEANBASE_LOGSERVICE_LOG_MGR_
#define OCEANBASE_LOGSERVICE_LOG_MGR_
#include <sys/types.h>
#include "common/ob_member_list.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/ob_occam_timer.h"
#include "share/scn.h"
#include "fetch_log_engine.h"
#include "log_loop_thread.h"
#include "log_define.h"
#include "log_io_task_cb_thread_pool.h"
#include "log_rpc.h"
#include "palf_options.h"
#include "palf_handle_impl.h"
#include "log_io_worker_wrapper.h"
#include "block_gc_timer_task.h"
#include "log_updater.h"
#include "log_io_utils.h"
namespace oceanbase
{
namespace common
{
class ObILogAllocatr;
}
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}
namespace palf
{
class IPalfHandleImpl;
class PalfHandleImpl;
class PalfHandle;
class ILogBlockPool;

class PalfHandleImplFactory
{
public:
  static PalfHandleImpl *alloc();
  static void free(IPalfHandleImpl *palf_handle_impl);
};

class PalfHandleImplAlloc
{
public:
  typedef common::LinkHashNode<LSKey> Node;

  static PalfHandleImpl *alloc_value();

  static void free_value(IPalfHandleImpl *palf_handle_impl);

  static Node *alloc_node(IPalfHandleImpl *palf_handle_impl);

  static void free_node(Node *node);
};

class PalfDiskOptionsWrapper {
public:
  enum class Status {
    INVALID_STATUS = 0,
    NORMAL_STATUS = 1,
    SHRINKING_STATUS = 2,
    MAX_STATUS = 3
  };

  static bool is_shrinking(const Status &status)
  { return Status::SHRINKING_STATUS == status; }

  PalfDiskOptionsWrapper() : disk_opts_for_stopping_writing_(),
                             disk_opts_for_recycling_blocks_(),
                             status_(Status::INVALID_STATUS),
                             cur_unrecyclable_log_disk_size_(0),
                             sequence_(-1),
                             disk_opts_lock_(common::ObLatchIds::PALF_ENV_LOCK) {}
  ~PalfDiskOptionsWrapper() { reset(); }

  int init(const PalfDiskOptions &disk_opts);
  void reset();
  int update_disk_options(const PalfDiskOptions &disk_opts);

  void change_to_normal(const int64_t sequence);
  const PalfDiskOptions& get_disk_opts_for_stopping_writing() const
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    return disk_opts_for_stopping_writing_;
  }
  const PalfDiskOptions& get_disk_opts_for_recycling_blocks() const
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    return disk_opts_for_recycling_blocks_;
  }
  void set_cur_unrecyclable_log_disk_size(const int64_t unrecyclable_log_disk_size);
  bool need_throttling() const;

  void get_disk_opts(PalfDiskOptions &disk_opts_for_stopping_writing,
                     PalfDiskOptions &disk_opts_for_recycling_blocks,
                     Status &status,
                     int64_t &sequence) const
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    disk_opts_for_stopping_writing = disk_opts_for_stopping_writing_;
    disk_opts_for_recycling_blocks = disk_opts_for_recycling_blocks_;
    status = status_;
    sequence = sequence_;
  }

  void get_throttling_options(PalfThrottleOptions &options)
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    options.total_disk_space_ = disk_opts_for_stopping_writing_.log_disk_usage_limit_size_;
    options.stopping_writing_percentage_ = disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_;
    options.trigger_percentage_ = disk_opts_for_stopping_writing_.log_disk_throttling_percentage_;
    options.maximum_duration_ = disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_;
    options.unrecyclable_disk_space_ = cur_unrecyclable_log_disk_size_;
  }

  bool is_shrinking() const
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    return is_shrinking(status_);
  }

  void set_stop_writing_disk_options_with_status(const PalfDiskOptions &new_opts,
                                                 const Status &status)
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    status_ = status;
    disk_opts_for_stopping_writing_ = new_opts;
  }
  static constexpr int64_t MB = 1024*1024ll;
  TO_STRING_KV(K_(disk_opts_for_stopping_writing), K_(disk_opts_for_recycling_blocks), K_(status),
               "cur_unrecyclable_log_disk_size(MB)", cur_unrecyclable_log_disk_size_/MB,
               K_(sequence));

private:
  int update_disk_options_not_guarded_by_lock_(const PalfDiskOptions &new_opts);
  // 'disk_opts_for_stopping_writing' is used for stopping writing, 'disk_opts_for_recycling_blocks'
  // is used for recycling blocks.
  //
  // In process of shrinking, we just update 'disk_opts_for_recycling_blocks_', and the update
  // 'disk_opts_for_stopping_writing_' when there is impossible to stop writing.
  PalfDiskOptions disk_opts_for_stopping_writing_;
  PalfDiskOptions disk_opts_for_recycling_blocks_;
  Status status_;
  int64_t cur_unrecyclable_log_disk_size_;
  int64_t sequence_;
  mutable ObSpinLock disk_opts_lock_;
};

typedef common::ObLinkHashMap<LSKey, IPalfHandleImpl, PalfHandleImplAlloc> PalfHandleImplMap;

class IPalfHandleImplGuard;
class IPalfEnvImpl
{
public:
  IPalfEnvImpl() {}
  virtual ~IPalfEnvImpl() {}
public:
  virtual int get_palf_handle_impl(const int64_t palf_id,
                                   IPalfHandleImplGuard &palf_handle_impl) = 0;
  virtual int get_palf_handle_impl(const int64_t palf_id,
                                   IPalfHandleImpl *&palf_handle_impl) = 0;
  virtual int create_palf_handle_impl(const int64_t palf_id,
                                      const AccessMode &access_mode,
                                      const PalfBaseInfo &base_info,
                                      IPalfHandleImpl *&palf_handle_impl) = 0;
  virtual int remove_palf_handle_impl(const int64_t palf_id) = 0;
  virtual void revert_palf_handle_impl(IPalfHandleImpl *palf_handle_impl) = 0;
  virtual common::ObILogAllocator *get_log_allocator() = 0;
  virtual int for_each(const common::ObFunction<int(IPalfHandleImpl *ipalf_handle_impl)> &func) = 0;
  virtual int create_directory(const char *base_dir) = 0;
  virtual int remove_directory(const char *base_dir) = 0;
  virtual bool check_disk_space_enough() = 0;
  virtual int64_t get_rebuild_replica_log_lag_threshold() const = 0;
  virtual int get_io_start_time(int64_t &last_working_time) = 0;
  virtual int64_t get_tenant_id() = 0;
  // should be removed in version 4.2.0.0
  virtual int update_replayable_point(const SCN &replayable_scn) = 0;
  virtual int get_throttling_options(PalfThrottleOptions &option) = 0;
  VIRTUAL_TO_STRING_KV("IPalfEnvImpl", "Dummy");

};

// 日志服务的容器类，同时管理logservice对象的生命周期
class PalfEnvImpl : public IPalfEnvImpl
{
public:
  PalfEnvImpl();
  virtual ~PalfEnvImpl();
public:
  int init(const PalfOptions &options,
           const char *base_dir,
           const common::ObAddr &self,
           const int64_t cluster_id,
           const int64_t tenant_id,
           rpc::frame::ObReqTransport *transport,
           common::ObILogAllocator *alloc_mgr,
           ILogBlockPool *log_block_pool,
           PalfMonitorCb *monitor);

  // start函数包含两层含义：
  //
  // 1. 启动PalfEnvImpl所包含的各类工作线程
  // 2. 根据base_dir中包含的log_storage和meta_storage文件，加载所有日志流所需的元信息及日志，执行故障恢复
  //
  // @return :TODO
  int start();
  void stop();
  void wait();
  void destroy();
public:
  // 创建日志流接口
  // @param [in] palf_id，待创建日志流的标识符
  // @param [in] palf_base_info，palf的日志起点信息
  // @param [out] palf_handle_impl，创建成功后生成的palf_handle_impl对象
  //                           在不再使用palf_handle_impl对象时，需要调用者执行revert_palf_handle_impl
  int create_palf_handle_impl(const int64_t palf_id,
                              const AccessMode &access_mode,
                              const PalfBaseInfo &palf_base_info,
                              IPalfHandleImpl *&palf_handle_impl) override final;
  // 删除日志流, 由Garbage Collector调用
  //
  // @param [in] palf_id，删除的日志流标识符
  //
  // @return :TODO
  int remove_palf_handle_impl(const int64_t palf_id) override final;
  int get_palf_handle_impl(const int64_t palf_id,
                           IPalfHandleImpl *&palf_handle_impl) override final;
  int get_palf_handle_impl(const int64_t palf_id,
                           IPalfHandleImplGuard &guard) override final;
  void revert_palf_handle_impl(IPalfHandleImpl *palf_handle_impl) override final;

  // =================== disk space management ==================
  int try_recycle_blocks();
  bool check_disk_space_enough() override final;
  int get_disk_usage(int64_t &used_size_byte, int64_t &total_usable_size_byte);
  int get_stable_disk_usage(int64_t &used_size_byte, int64_t &total_usable_size_byte);
  int update_options(const PalfOptions &options);
  int get_options(PalfOptions &options);
  int64_t get_rebuild_replica_log_lag_threshold() const
  {return rebuild_replica_log_lag_threshold_;}
  int for_each(const common::ObFunction<int(const PalfHandle&)> &func);
  int for_each(const common::ObFunction<int(IPalfHandleImpl *ipalf_handle_impl)> &func) override final;
  common::ObILogAllocator* get_log_allocator() override final;
  int get_io_start_time(int64_t &last_working_time) override final;
  int64_t get_tenant_id() override final;
  int update_replayable_point(const SCN &replayable_scn) override final;
  int get_throttling_options(PalfThrottleOptions &option);
  INHERIT_TO_STRING_KV("IPalfEnvImpl", IPalfEnvImpl, K_(self), K_(log_dir), K_(disk_options_wrapper),
      KPC(log_alloc_mgr_));
  // =================== disk space management ==================
public:
  int create_directory(const char *base_dir) override final;
  int remove_directory(const char *base_dir) override final;

private:
  class ReloadPalfHandleImplFunctor : public ObBaseDirFunctor
  {
  public:
    ReloadPalfHandleImplFunctor(PalfEnvImpl *palf_env_impl);
    int func(const struct dirent *entry) override;
  private:
    PalfEnvImpl *palf_env_impl_;
  };
  int reload_palf_handle_impl_(const int64_t palf_id);

  struct LogGetRecycableFileCandidate {
    LogGetRecycableFileCandidate();
    ~LogGetRecycableFileCandidate();
    bool operator() (const LSKey &palf_id, IPalfHandleImpl *palf_handle_impl);
    int64_t id_;
    block_id_t min_block_id_;
    share::SCN min_block_max_scn_;
    block_id_t min_using_block_id_;
    int64_t oldest_palf_id_;
    share::SCN oldest_block_scn_;
    int ret_code_;
    TO_STRING_KV(K_(id), K_(min_block_max_scn), K_(min_block_id), K_(min_using_block_id), K_(oldest_palf_id), K_(oldest_block_scn), K_(ret_code));
  };
  struct GetTotalUsedDiskSpace
  {
    GetTotalUsedDiskSpace();
    ~GetTotalUsedDiskSpace();
    bool operator() (const LSKey &palf_id, IPalfHandleImpl *palf_handle_impl);
    TO_STRING_KV(K_(total_used_disk_space), K_(total_unrecyclable_disk_space), K_(ret_code));
    int64_t total_used_disk_space_;
    int64_t total_unrecyclable_disk_space_;
    int64_t maximum_used_size_;
    int64_t palf_id_;
    int ret_code_;
  };
  struct RemoveStaleIncompletePalfFunctor : public ObBaseDirFunctor {
    RemoveStaleIncompletePalfFunctor(PalfEnvImpl *palf_env_impl);
    ~RemoveStaleIncompletePalfFunctor();
    int func(const dirent *entry) override;
    PalfEnvImpl *palf_env_impl_;
  };

private:
  int create_palf_handle_impl_(const int64_t palf_id,
                               const AccessMode &access_mode,
                               const PalfBaseInfo &palf_base_info,
                               const LogReplicaType replica_type,
                               IPalfHandleImpl *&palf_handle_impl);
  int scan_all_palf_handle_impl_director_();
  const PalfDiskOptions &get_disk_options_guarded_by_lock_() const;
  int get_total_used_disk_space_(int64_t &total_used_disk_space,
                                 int64_t &total_unrecyclable_disk_space,
                                 int64_t &palf_id,
                                 int64_t &maximum_used_size);
  int get_disk_usage_(int64_t &used_size_byte);
  int get_disk_usage_(int64_t &used_size_byte,
                      int64_t &unrecyclable_disk_space,
                      int64_t &palf_id,
                      int64_t &maximum_used_size);
  int recycle_blocks_(bool &has_recycled, int64_t &oldest_palf_id, share::SCN &oldest_scn);
  int wait_until_reference_count_to_zero_(const int64_t palf_id);
  // check the diskspace whether is enough to hold a new palf instance.
  bool check_can_create_palf_handle_impl_() const;
  int remove_palf_handle_impl_from_map_not_guarded_by_lock_(const int64_t palf_id);
  int move_incomplete_palf_into_tmp_dir_(const int64_t palf_id);
  int check_tmp_log_dir_exist_(bool &exist) const;
  int remove_stale_incomplete_palf_();

  int init_log_io_worker_config_(const int log_writer_parallelism,
                                 const int64_t tenant_id,
                                 LogIOWorkerConfig &config);

  int check_can_update_log_disk_options_(const PalfDiskOptions &disk_options);

private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  RWLock palf_meta_lock_;
  common::ObILogAllocator *log_alloc_mgr_;
  ILogBlockPool *log_block_pool_;
  FetchLogEngine fetch_log_engine_;
  LogRpc log_rpc_;
  LogIOTaskCbThreadPool cb_thread_pool_;
  common::ObOccamTimer election_timer_;
  LogIOWorkerWrapper log_io_worker_wrapper_;
  BlockGCTimerTask block_gc_timer_task_;
  LogUpdater log_updater_;
  PalfMonitorCb *monitor_;

  PalfDiskOptionsWrapper disk_options_wrapper_;
  int64_t disk_not_enough_print_interval_;

  char log_dir_[common::MAX_PATH_SIZE];
  char tmp_log_dir_[common::MAX_PATH_SIZE];
  common::ObAddr self_;

  PalfHandleImplMap palf_handle_impl_map_;
  LogLoopThread log_loop_thread_;

  // last_palf_epoch_ is used to assign increasing epoch for each palf instance.
  int64_t last_palf_epoch_;
  int64_t rebuild_replica_log_lag_threshold_;//for rebuild test

  LogIOWorkerConfig log_io_worker_config_;
  bool diskspace_enough_;
  int64_t tenant_id_;
  bool is_inited_;
  bool is_running_;
private:
  DISALLOW_COPY_AND_ASSIGN(PalfEnvImpl);
};

} // end namespace palf
} // end namespace oceanbase

#endif
