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
#include "fetch_log_engine.h"
#include "log_loop_thread.h"
#include "log_define.h"
#include "log_io_worker.h"
#include "log_io_task_cb_thread_pool.h"
#include "log_rpc.h"
#include "palf_options.h"
#include "palf_handle_impl.h"
#include "log_io_worker.h"
#include "block_gc_timer_task.h"
namespace oceanbase
{
namespace common
{
class ObILogAllocator;
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
struct PalfHandleImplGuard;
class ILogBlockPool;

class PalfHandleImplFactory
{
public:
  static PalfHandleImpl *alloc();
  static void free(PalfHandleImpl *palf_handle_impl);
};

class PalfHandleImplAlloc
{
public:
  typedef common::LinkHashNode<LSKey> Node;

  static PalfHandleImpl *alloc_value()
  {
    return NULL;
  }

  static void free_value(PalfHandleImpl *palf_handle_impl)
  {
    PalfHandleImplFactory::free(palf_handle_impl);
    palf_handle_impl = NULL;
  }

  static Node *alloc_node(PalfHandleImpl *palf_handle_impl)
  {
    UNUSED(palf_handle_impl);
    return op_reclaim_alloc(Node);
  }

  static void free_node(Node *node)
  {
    op_reclaim_free(node);
    node = NULL;
  }
};

class PalfDiskOptionsWrapper {
public:
  enum class Status {
    INVALID_STATUS = 0,
    NORMAL_STATUS = 1,
    SHRINKING_STATUS = 2,
    MAX_STATUS = 3
  };

  PalfDiskOptionsWrapper() : disk_opts_for_stopping_writing_(), disk_opts_for_recycling_blocks_(), status_(Status::INVALID_STATUS) {}
  ~PalfDiskOptionsWrapper() { reset(); }

  int init(const PalfDiskOptions &disk_opts);
  void reset();
  int update_disk_options(const PalfDiskOptions &disk_opts);

  void change_to_normal();
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

  void get_disk_opts(PalfDiskOptions &disk_opts_for_stopping_writing,
                     PalfDiskOptions &disk_opts_for_recycling_blocks,
                     Status &status) const
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    disk_opts_for_stopping_writing = disk_opts_for_stopping_writing_;
    disk_opts_for_recycling_blocks = disk_opts_for_recycling_blocks_;
    status = status_;
  }

  bool is_shrinking() const
  {
    ObSpinLockGuard guard(disk_opts_lock_);
    return Status::SHRINKING_STATUS == status_;
  }
  TO_STRING_KV(K_(disk_opts_for_stopping_writing), K_(disk_opts_for_recycling_blocks), K_(status));

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
  mutable ObSpinLock disk_opts_lock_;
};

typedef common::ObLinkHashMap<LSKey, PalfHandleImpl, PalfHandleImplAlloc> PalfHandleImplMap;

// 日志服务的容器类，同时管理logservice对象的生命周期
class PalfEnvImpl
{
public:
  PalfEnvImpl();
  virtual ~PalfEnvImpl();
public:
  int init(const PalfDiskOptions &disk_options,
           const char *base_dir,
           const common::ObAddr &self,
           rpc::frame::ObReqTransport *transport,
           common::ObILogAllocator *alloc_mgr,
           ILogBlockPool *log_block_pool);

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
  // 新建日志流
  //
  // @param [in] palf_id，待创建日志流的标识符
  // @param [out] palf_handle_impl，创建成功后生成的palf_handle_impl对象
  //                           在不再使用palf_handle_impl对象时，需要调用者执行revert_palf_handle_impl
  //
  // @return :TODO
  int create_palf_handle_impl(const int64_t palf_id,
                              const AccessMode &access_mode,
                              PalfHandleImpl *&palf_handle_impl);
  // 创建迁移目的端的接口
  // @param [in] palf_id，待创建日志流的标识符
  // @param [in] palf_base_info，palf的日志起点信息
  // @param [out] palf_handle_impl，创建成功后生成的palf_handle_impl对象
  //                           在不再使用palf_handle_impl对象时，需要调用者执行revert_palf_handle_impl
  int create_palf_handle_impl(const int64_t palf_id,
                              const AccessMode &access_mode,
                              const PalfBaseInfo &palf_base_info,
                              PalfHandleImpl *&palf_handle_impl);
  // 删除日志流, 由Garbage Collector调用
  //
  // @param [in] palf_id，删除的日志流标识符
  //
  // @return :TODO
  int remove_palf_handle_impl(const int64_t palf_id);
  int get_palf_handle_impl(const int64_t palf_id,
                           PalfHandleImpl *&palf_handle_impl);
  int get_palf_handle_impl(const int64_t palf_id,
                           PalfHandleImplGuard &palf_handle_impl_guard);
  void revert_palf_handle_impl(PalfHandleImpl *palf_handle_impl);
  int try_switch_state_for_all();
  int check_and_switch_freeze_mode();
  int try_freeze_log_for_all();
  // =================== memory space management ==================
  bool check_tenant_memory_enough();
  // =================== disk space management ==================
  int try_recycle_blocks();
  bool check_disk_space_enough();
  int get_disk_usage(int64_t &used_size_byte, int64_t &total_usable_size_byte);
  int update_disk_options(const PalfDiskOptions &disk_options);
  int get_disk_options(PalfDiskOptions &disk_options);
  int for_each(const common::ObFunction<int(const PalfHandle&)> &func);
  common::ObILogAllocator* get_log_allocator();
  TO_STRING_KV(K_(self), K_(log_dir), K_(disk_options_wrapper));
  // =================== disk space management ==================
public:
  int create_directory(const char *base_dir);
  int remove_directory(const char *base_dir);

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
  class SwitchStateFunctor
  {
  public:
    SwitchStateFunctor() {}
    ~SwitchStateFunctor() {}
    bool operator() (const LSKey &palf_id, PalfHandleImpl *palf_handle_impl);
  };
  class FreezeLogFunctor
  {
  public:
    FreezeLogFunctor() {}
    ~FreezeLogFunctor() {}
    bool operator() (const LSKey &palf_id, PalfHandleImpl *palf_handle_impl);
  };
  class CheckFreezeModeFunctor
  {
  public:
    CheckFreezeModeFunctor() {}
    ~CheckFreezeModeFunctor() {}
    bool operator() (const LSKey &palf_id, PalfHandleImpl *palf_handle_impl);
  };
  struct LogGetRecycableFileCandidate {
    LogGetRecycableFileCandidate();
    ~LogGetRecycableFileCandidate();
    bool operator() (const LSKey &palf_id, PalfHandleImpl *palf_handle_impl);
    int64_t id_;
    block_id_t min_block_id_;
    int64_t min_block_max_ts_;
    block_id_t min_using_block_id_;
    int64_t oldest_palf_id_;
    int64_t oldest_block_ts_;
    int ret_code_;
    TO_STRING_KV(K_(id), K_(min_block_max_ts), K_(min_block_id), K_(min_using_block_id), K_(oldest_palf_id), K_(oldest_block_ts), K_(ret_code));
  };
  struct GetTotalUsedDiskSpace
  {
    GetTotalUsedDiskSpace();
    ~GetTotalUsedDiskSpace();
    bool operator() (const LSKey &palf_id, PalfHandleImpl *palf_handle_impl);
    TO_STRING_KV(K_(total_used_disk_space), K_(ret_code));
    int64_t total_used_disk_space_;
    int64_t maximum_used_size_;
    int64_t palf_id_;
    int ret_code_;
  };

private:
  int scan_all_palf_handle_impl_director_();
  void update_disk_options_guarded_by_lock_(const PalfDiskOptions &disk_options);
  const PalfDiskOptions &get_disk_options_guarded_by_lock_() const;
  int get_total_used_disk_space_(int64_t &total_used_disk_space,
                                 int64_t &palf_id,
                                 int64_t &maximum_used_size);
  int get_disk_usage_(int64_t &used_size_byte);
  int get_disk_usage_(int64_t &used_size_byte,
                      int64_t &palf_id,
                      int64_t &maximum_used_size);
  int recycle_blocks_(bool &has_recycled, int64_t &oldest_palf_id, int64_t &oldest_ts);
  int wait_until_reference_count_to_zero_(const int64_t palf_id);
  // check the diskspace whether is enough to hold a new palf instance.
  bool check_can_create_palf_handle_impl_() const;
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
  LogIOWorker log_io_worker_;
  BlockGCTimerTask block_gc_timer_task_;

  PalfDiskOptionsWrapper disk_options_wrapper_;
  int64_t check_disk_print_log_interval_;

  char log_dir_[common::MAX_PATH_SIZE];
  common::ObAddr self_;

  PalfHandleImplMap palf_handle_impl_map_;
  LogLoopThread log_loop_thread_;

  // last_palf_epoch_ is used to assign increasing epoch for each palf instance.
  int64_t last_palf_epoch_;

  LogIOWorkerConfig log_io_worker_config_;
  bool diskspace_enough_;
  bool is_inited_;
  bool is_running_;
private:
  DISALLOW_COPY_AND_ASSIGN(PalfEnvImpl);
};

} // end namespace palf
} // end namespace oceanbase

#endif
