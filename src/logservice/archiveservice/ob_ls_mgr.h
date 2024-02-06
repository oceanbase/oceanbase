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

#ifndef OCEANBASE_ARCHIVE_OB_LOG_STREAM_MGR_H_
#define OCEANBASE_ARCHIVE_OB_LOG_STREAM_MGR_H_

#include "lib/hash/ob_link_hashmap.h"   // ObLinkHashMap
#include "ob_archive_define.h"
#include "share/ob_ls_id.h"             // ObLSID
#include "share/ob_thread_pool.h"       // ObThreadPool
#include "common/ob_role.h"             // ObRole
#include "common/ob_queue_thread.h"     // ObCond
#include "ob_ls_task.h"                 // ObLSArchiveTask
#include "ob_start_archive_helper.h"    // StartArchiveHelper
#include "share/scn.h"        // SCn
#include <cstdint>

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObLSService;
}

namespace logservice
{
class ObLogService;
}
namespace archive
{
class ObArchiveAllocator;
class ObArchiveSequencer;
class ObArchivePersistMgr;
class ObArchiveRoundMgr;
using oceanbase::share::ObLSID;
using oceanbase::storage::ObLS;
using oceanbase::logservice::ObLogService;
using oceanbase::storage::ObLSService;
typedef common::ObLinkHashMap<ObLSID, ObLSArchiveTask> LSArchiveMap;
/*
 * 日志流归档管理模块
 *
 * archive是为日志流提供日志备份服务, 不绑定日志流副本生命周期, 因此需要独立的日志流管理
 * 不设置backup zone时, 默认由日志流leader所在server提供归档服务
 * 设置backup zone时, 由leader指定日志流副本提供归档服务
 *
 * NB: 由于日志流本身限制, 暂时不提供无副本归档服务
 *
 * */
class ObArchiveLSMgr : public share::ObThreadPool
{
  static const int64_t THREAD_RUN_INTERVAL = 1 * 1000 * 1000L;
  static const int64_t DEFAULT_PRINT_INTERVAL = 30 * 1000 * 1000L;
public:
  ObArchiveLSMgr();
  virtual ~ObArchiveLSMgr();

public:
  int iterate_ls(const std::function<int (const ObLSArchiveTask &)> &func);
  template <typename Fn> int foreach_ls(Fn &fn) { return ls_map_.for_each(fn);  }
public:
  int init(const uint64_t tenant_id,
      ObLogService *log_service,
      ObLSService *ls_svr,
      ObArchiveAllocator *allocator,
      ObArchiveSequencer *sequencer,
      ObArchiveRoundMgr *round_mgr,
      ObArchivePersistMgr *persist_mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  int set_archive_info(const share::SCN &round_start_scn,
      const int64_t piece_interval, const share::SCN &genesis_scn, const int64_t base_piece_id);
  void clear_archive_info();
  void notify_start();
  void notify_stop();
  int revert_ls_task(ObLSArchiveTask *task);
  int get_ls_guard(const ObLSID &id, ObArchiveLSGuard &guard);
  int authorize_ls_archive_task(const ObLSID &id, const int64_t epoch, const share::SCN &start_scn);
  void reset_task();
  int64_t get_ls_task_count() const { return ls_map_.count(); }
  int mark_fatal_error(const ObLSID &id, const ArchiveKey &key, const ObArchiveInterruptReason &reason);
  int print_tasks();

private:
  void run1();
  void do_thread_task_();
  void gc_stale_ls_task_(const ArchiveKey &key, const bool is_in_doing);
  void add_ls_task_();
  int check_ls_archive_task_valid_(const ArchiveKey &key, ObLS *ls, bool &exist);
  int add_task_(const ObLSID &id, const ArchiveKey &key, const int64_t epoch);
  int insert_or_update_ls_(const StartArchiveHelper &helper);

private:
  class CheckDeleteFunctor;
  class ClearArchiveTaskFunctor;
private:
  bool                  inited_;
  uint64_t              tenant_id_;
  share::SCN             round_start_scn_;
  int64_t               piece_interval_;
  share::SCN             genesis_scn_;
  int64_t               base_piece_id_;
  LSArchiveMap          ls_map_;

  int64_t               last_print_ts_;
  ObArchiveAllocator    *allocator_;
  ObArchiveSequencer    *sequencer_;
  ObArchiveRoundMgr     *round_mgr_;
  ObLogService          *log_service_;
  ObLSService           *ls_svr_;
  ObArchivePersistMgr   *persist_mgr_;
  common::ObCond        cond_;
};
} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_LOG_STREAM_MGR_H_ */
