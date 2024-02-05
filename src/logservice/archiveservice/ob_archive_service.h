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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_SERVICE_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_SERVICE_H_

#include "share/ob_thread_pool.h"       // ObThreadPool
#include "common/ob_queue_thread.h"     // ObCond
#include "ob_archive_allocator.h"       // ObArchiveAllocator
#include "ob_archive_sequencer.h"        // ObArchiveSequencer
#include "ob_archive_fetcher.h"         // ObArchiveFetcher
#include "ob_archive_sender.h"          // ObArchiveSender
#include "ob_archive_persist_mgr.h"     // ObArchivePersistMgr
#include "ob_archive_round_mgr.h"       // ObArchiveRoundMgr
#include "ob_ls_mgr.h"          // ObArchiveLSMgr
#include "ob_archive_scheduler.h"       // ObArchiveScheduler
#include "ob_archive_timer.h"           // ObArchiveTimer
#include "ob_ls_meta_recorder.h"        // ObLSMetaRecorder

namespace oceanbase
{
namespace share
{
class ObTenantArchiveRoundAttr;
}
namespace logservice
{
class ObLogService;
}

namespace palf
{
struct LSN;
}

namespace storage
{
class ObLSService;
}

namespace archive
{
using oceanbase::logservice::ObLogService;
using oceanbase::storage::ObLSService;
using oceanbase::palf::LSN;
using oceanbase::share::ObTenantArchiveRoundAttr;
class ObLSArchiveTask;

/*
 * Tenant Archive Service
 * */
class ObArchiveService : public share::ObThreadPool
{
public:
  ObArchiveService();
  virtual ~ObArchiveService();

public:
  static int mtl_init(ObArchiveService *&archive_svr);
  int init(ObLogService *log_service, ObLSService *ls_svr, const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  // 获取日志流归档进度, 供迁移/复制/GC参考归档进度
  //
  // @param [in], id 日志流ID
  // @param [out], lsn 日志流归档进度lsn
  // @param [out], scn 日志流归档进度scn
  // @param [out], force_wait 调用模块需要严格参考归档进度, 归档落后时卡调用模块
  // @param [out], ignore忽略归档进度
  //
  // @retval OB_SUCCESS 获取归档进度成功
  // @retval OB_EGAIN 需要调用者重试
  int get_ls_archive_progress(const ObLSID &id, LSN &lsn, share::SCN &scn, bool &force_wait, bool &ignore);

  // 实时SQL查表获取租户是否在归档模式, 非必要无需调此接口(当前仅GC需要)
  // @param [out], in_archive 租户处于归档模式
  //
  // @retval OB_SUCCESS 确认成功
  // @retval other_code 获取失败
  int check_tenant_in_archive(bool &in_archive);

  // 获取日志流归档速度
  //
  // @param [in], id 日志流ID
  // @param [out], speed 日志归档速度, 单位: Bytes/s
  // @param [out], force_wait 调用模块需要严格参考归档进度, 归档落后时卡调用模块
  // @param [out], ignore忽略归档进度
  //
  // @retval OB_SUCCESS 获取归档速度成功
  // @retval other_code 获取失败
  //
  // @note: 单位时间内真实归档日志数据量, 如果单位时间内没有日志归档出去, 获取到速度为0
  int get_ls_archive_speed(const ObLSID &id, int64_t &speed, bool &force_wait, bool &ignore);

  ///////////// RPC process functions /////////////////
  void wakeup();

  // Flush all logs to all archive destinations, in this interface a flush all flag is set and the flush action will be done in background.
  // The flush flag is a temporary state on the ls, so only ls in archive progress in this server will be affected.
  //
  // New ls after the function call and ls migrated to other servers are immune.
  void flush_all();

  int iterate_ls(const std::function<int(const ObLSArchiveTask&)> &func);

private:
  enum class ArchiveRoundOp
  {
    NONE = 0,
    START = 1,
    STOP = 2,
    FORCE_STOP = 3,
    MARK_INTERRUPT = 4,
    SUSPEND = 5,
  };
private:
  void run1();
  void do_thread_task_();
  bool need_check_switch_archive_() const;
  bool need_check_switch_stop_status_() const;
  bool need_print_archive_status_() const;

  // ============= 开启/关闭归档 ========== //
  void do_check_switch_archive_();
  // 1. 获取租户级归档配置项信息
  int load_archive_round_attr_(ObTenantArchiveRoundAttr &attr);

  // 2. 检查是否需要切归档状态
  int check_if_need_switch_log_archive_(const ObTenantArchiveRoundAttr &attr, ArchiveRoundOp &op);

  // 3. 处理开启归档, 状态推到DOING
  int start_archive_(const ObTenantArchiveRoundAttr &attr);
  // 3.1 设置归档信息
  int set_log_archive_info_(const ObTenantArchiveRoundAttr &attr);
  // 3.2 通知各模块开启归档
  void notify_start_();

  // 4. 处理关闭归档, 状态推到STOPPING
  void stop_archive_();
  // 4.1 检查残留任务清理完成
  void check_and_set_archive_stop_();
  // 4.1.1 清理日志流归档任务
  int clear_ls_task_();
  // 4.1.2 清理各模块归档信息
  void clear_archive_info_();
  // 4.1.3 检查归档任务清空
  bool check_archive_task_empty_() const;
  // 4.2 设置STOP
  int set_log_archive_stop_status_(const ArchiveKey &key);

  // 5. 归档轮次落后并且租户处于关闭归档状态, 强制stop, 状态推到STOP
  int force_stop_archive(const int64_t incarnation, const int64_t round);

  // 6. set archive suspend
  int suspend_archive_(const ObTenantArchiveRoundAttr &attr);

  // ====== 打印归档状态 =========== //
  void print_archive_status_();

private:
  const int64_t THREAD_RUN_INTERVAL = 5 * 1000 * 1000L;
private:
  bool inited_;
  uint64_t tenant_id_;
  ObArchiveAllocator allocator_;
  ObArchiveRoundMgr archive_round_mgr_;
  ObArchiveLSMgr ls_mgr_;
  ObArchiveSequencer sequencer_;
  ObArchiveFetcher fetcher_;
  ObArchiveSender sender_;
  ObArchivePersistMgr persist_mgr_;
  ObArchiveScheduler scheduler_;
  ObLSMetaRecorder ls_meta_recorder_;
  ObArchiveTimer timer_;
  logservice::ObLogService *log_service_;
  ObLSService *ls_svr_;
  common::ObCond cond_;
};

}
}

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_SERVICE_H_ */
