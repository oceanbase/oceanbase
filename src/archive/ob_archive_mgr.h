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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_MGR_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_MGR_H_

#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"  // ObAddr
#include "ob_archive_allocator.h"
#include "ob_ilog_fetcher.h"               // ObArchiveIlogFetcher
#include "ob_archive_clog_split_engine.h"  // ObArCLogSplitEngine
#include "ob_archive_sender.h"             // ObArchiveSender
#include "ob_archive_pg_mgr.h"             // ObArchivePGMgr
#include "ob_archive_log_wrapper.h"        // ObArchiveLogWrapper
#include "ob_archive_round_mgr.h"          // ObArchiveRoundMgr
#include "ob_archive_scheduler.h"          // ObArchiveScheduler
#include "ob_ilog_fetch_task_mgr.h"        // ObArchiveIlogFetchTaskMgr
#include "share/ob_thread_pool.h"          // ObThreadPool
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace clog {
class ObILogEngine;
struct ObPGLogArchiveStatus;
}  // namespace clog
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;
}  // namespace storage
namespace common {
struct ObPartitionKey;
}
namespace logservice {
class ObExtLogService;
}
namespace archive {
class ObArchiveMgr : public share::ObThreadPool {
  static const int64_t THREAD_INTERVAL = 50 * 1000L;
  static const int64_t CHECK_ARCHIVE_STOP_INTERVAL = 1000 * 1000L;

private:
  // 1. archive inital status is 0
  // 2. archive_mgr start archive if necessary, put all pg leader into pre_queue of pg_mgr,
  //    then set archive status 1
  // 3. pg_mgr help all leader produce kickoff log task while all archive module already work,
  //    then all module handle kickoff task in turn and archive it succ,
  //    at last pg status is set first_log_archive_succ
  //    if all pg status first_log_archive_succ is true, archive_mgr set archive status 2
  // 4. archive_mgr stop archive if necessary, first stop ilog_fetcher,
  //    clean pg_fetch task, then archive_mgr set archive status 3
  // 5. archive_mgr check no tasks remains in all modules and set archive status 4
  //
  //
  // every archive module work status correspond to archive status
  // 1. archive_mgr work all time and manage archive status
  // 2. ilog_fetcher work mode under archive_mgr control and not depend on archive status
  // 3. clog_splitter and sender work under archive status 1 and 2, and skip tasks under other status
  /*
  enum LogArchiveStatus
  {
    LOG_ARCHIVE_INVALID_STATUS = 0,
    LOG_ARCHIVE_BEGINNING,
    LOG_ARCHIVE_DOING,
    LOG_ARCHIVE_IN_STOPPING,
    LOG_ARCHIVE_STOPPED,
    LOG_ARCHIVE_MAX
  };
  */
public:
  // status 1, 2
  // pg_mgr dispatch_pg / clog_split / sender / leader_takeover add task
  bool is_in_archive_status() const;
  // status 1
  // pg_mgr check_start_archive
  bool is_in_archive_beginning_status() const;
  // status 2
  // pg_mgr reconfirm_pg
  bool is_in_archive_doing_status() const;
  // status 3
  // archive_mgr clear_archive
  bool is_in_archive_stopping_status() const;
  bool is_in_archive_stop_status() const;

  bool is_server_archive_stop(const int64_t incarnation, const int64_t round);

public:
  ObArchiveMgr();
  virtual ~ObArchiveMgr();

public:
  int init(clog::ObILogEngine* log_engine, logservice::ObExtLogService* ext_log_service,
      storage::ObPartitionService* partition_service, const common::ObAddr& addr);
  void destroy();
  int start();
  void stop();
  void wait();

public:
  // interface for outer call
  int add_pg_log_archive_task(ObIPartitionGroup* partition);
  int delete_pg_log_archive_task(ObIPartitionGroup* partition);
  int submit_checkpoint_task(const common::ObPartitionKey& pg_key, const uint64_t max_log_id,
      const int64_t max_log_submit_ts, const int64_t checkpoint_ts);
  int get_log_archive_status(common::ObPartitionKey& pg_key, clog::ObPGLogArchiveStatus& status, int64_t& epoch);
  int get_archive_pg_map(PGArchiveMap*& map);
  // occur fatal error, notify all archive modules stop
  int mark_encounter_fatal_err(const common::ObPartitionKey& pg_key, const int64_t incarnation, const int64_t round);

public:
  // interface for inner call
  void notify_all_archive_round_started();
  bool has_encounter_fatal_err(const int64_t incarnation, const int64_t round);
  ObArchivePGMgr* get_pg_mgr()
  {
    return &pg_mgr_;
  };
  ObArchiveIlogFetcher* get_ilog_fetcher()
  {
    return &ilog_fetcher_;
  };
  ObArchiveSender* get_sender()
  {
    return &archive_sender_;
  }

private:
  int handle_start_archive_(const int64_t incarnation, const int64_t archive_round);
  void run1();
  int init_components_();
  void destroy_components_();
  int start_threads_();
  void stop_threads_();
  int start_components_();

  bool check_archive_task_empty_();
  int set_log_archive_stop_status_();
  int clear_pg_archive_task_();
  int clear_archive_info_();
  int set_log_archive_info_(share::ObLogArchiveBackupInfo& info);

  int start_archive_(share::ObLogArchiveBackupInfo& info);
  int stop_archive_();

private:
  void do_thread_task_();

  bool need_check_switch_archive_();
  void do_check_switch_archive_();
  bool need_check_switch_stop_status_();
  void do_check_switch_stop_status_();
  bool need_print_archive_status_();
  void print_archive_status_();
  void do_force_stop_archive_work_threads_();

  bool check_if_need_switch_log_archive_(
      share::ObLogArchiveBackupInfo& info, bool& need_stop, bool& need_start, bool& need_force_stop);
  int check_and_set_start_archive_ts_(const int64_t incarnation, const int64_t round, int64_t& start_ts);

private:
  bool inited_;

  int64_t last_check_stop_ts_;
  int64_t server_start_archive_tstamp_;  // server start archive time

public:
  common::ObAddr self_;
  ObArchiveAllocator allocator_;
  ObArchiveScheduler archive_scheduler_;
  ObArchiveIlogFetcher ilog_fetcher_;
  ObArchiveSender archive_sender_;
  ObArCLogSplitEngine clog_split_engine_;
  ObArchiveLogWrapper log_wrapper_;
  ObArchiveRoundMgr archive_round_mgr_;

  ObArchivePGMgr pg_mgr_;
  ObArchiveIlogFetchTaskMgr ilog_fetch_task_mgr_;

  clog::ObILogEngine* log_engine_;
  logservice::ObExtLogService* ext_log_service_;
  storage::ObPartitionService* partition_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveMgr);
};

}  // namespace archive
}  // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_MGR_H_ */
