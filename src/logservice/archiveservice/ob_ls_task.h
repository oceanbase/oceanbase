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

#ifndef OCEANBASE_ARCHIVE_OB_LOG_STREAM_TASK_H_
#define OCEANBASE_ARCHIVE_OB_LOG_STREAM_TASK_H_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_link_hashmap.h"          // LinkHashNode
#include "lib/lock/ob_spin_rwlock.h"           // SpinRWLock
#include "lib/queue/ob_link_queue.h"           // ObSpLinkQueue
#include "ob_archive_define.h"                 // ArchiveWorkStation LogFileTuple
#include "share/ob_ls_id.h"                    // ObLSID
#include "logservice/palf/lsn.h"               // LSN
#include "share/scn.h"               // SCN
#include "lib/utility/ob_print_utils.h"        // print
#include <cstdint>

namespace oceanbase
{
namespace share
{
class ObLSID;
class ObLSArchivePersistInfo;
}

namespace palf
{
struct LSN;
}

namespace archive
{
class ObArchiveSendTask;
class ObArchiveLogFetchTask;
class ObArchiveLSMgr;
class StartArchiveHelper;
class ObArchiveAllocator;
class ObArchiveWorker;
class ObArchiveTaskStatus;
struct LSArchiveStat;

using oceanbase::palf::LSN;
using oceanbase::share::ObLSID;
using oceanbase::share::ObLSArchivePersistInfo;

typedef common::LinkHashValue<ObLSID> LSArchiveTaskValue;
class ObLSArchiveTask : public LSArchiveTaskValue
{
public:
  ObLSArchiveTask();
  ~ObLSArchiveTask();

public:
  int init(const StartArchiveHelper &helper, ObArchiveAllocator *allocator);

  // 更新日志流归档任务
  int update_ls_task(const StartArchiveHelper &helper);

  // 检查归档任务是否与当前leader匹配
  bool check_task_valid(const ArchiveWorkStation &station);

  // 销毁归档任务, 释放资源
  void destroy();

  // fetcher将读取到的clog添加到待发送队列, 该任务队列按照日志在clog文件偏移顺序串联
  int push_fetch_log(ObArchiveLogFetchTask &task);

  // 提交send task
  int push_send_task(ObArchiveSendTask &task, ObArchiveWorker &worker);

  // 获取sequencer模块进度
  int get_sequencer_progress(const ArchiveKey &key,
                             ArchiveWorkStation &station,
                             LSN &offset);

  // 更新sequencer模块进度
  int update_sequencer_progress(const ArchiveWorkStation &station,
                                const int64_t size,
                                const LSN &offset);

  // 获取已排序可以提交发送的数据
  // NB: 如果没有与当前归档进度连续的任务, 则返回NULL
  int get_sorted_fetch_log(ObArchiveLogFetchTask *&task);

  // 更新fetcher归档进度
  int update_fetcher_progress(const ArchiveWorkStation &station, const LogFileTuple &tuple);

  // 获取fetch进度
  int get_fetcher_progress(const ArchiveWorkStation &station,
                         palf::LSN &offset,
                         share::SCN &scn,
                         int64_t &last_fetch_timestamp);

  int compensate_piece(const ArchiveWorkStation &station,
                       const int64_t next_compensate_piece_id);
  // 更新日志流归档进度
  int update_archive_progress(const ArchiveWorkStation &station,
                              const int64_t file_id,
                              const int64_t file_offset,
                              const LogFileTuple &tuple);

  // 获取日志流归档进度
  int get_archive_progress(const ArchiveWorkStation &station,
                           int64_t &file_id,
                           int64_t &file_offset,
                           LogFileTuple &tuple);

  // get send task count in send_task_status
  // @param[in] station, the archive work station of ls
  // @param[out] count, the count of send_tasks in task_status
  int get_send_task_count(const ArchiveWorkStation &station, int64_t &count);

  // 获取归档参数
  int get_archive_send_arg(const ArchiveWorkStation &station,
                           ObArchiveSendDestArg &arg);

  int get_max_archive_info(const ArchiveKey &key,
                           ObLSArchivePersistInfo &info);

  int get_max_no_limit_lsn(const ArchiveWorkStation &station, LSN &lsn);

  // support flush all logs to archvie dest
  int update_no_limit_lsn(const palf::LSN &lsn);

  int mark_error(const ArchiveKey &key);

  int print_self();

  void stat(LSArchiveStat &ls_stat) const;

  void mock_init(const ObLSID &id, ObArchiveAllocator *allocotr);
  TO_STRING_KV(K_(id),
               K_(station),
               K_(round_start_scn),
               K_(dest));

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;

  class ArchiveDest
  {
    friend ObLSArchiveTask;
  public:
    ArchiveDest();
    ~ArchiveDest();

  public:
    int init(const LSN &max_no_limit_lsn,
        const LSN &piece_min_lsn, const LSN &lsn, const int64_t file_id,
        const int64_t file_offset, const share::ObArchivePiece &piece,
        const share::SCN &max_archived_scn, const bool is_log_gap_exist,
        ObArchiveAllocator *allocator);
    void destroy();
    void get_sequencer_progress(LSN &offset) const;
    int update_sequencer_progress(const int64_t size, const LSN &offset);
    void get_fetcher_progress(LogFileTuple &tuple, int64_t &last_fetch_timestamp) const;
    int update_fetcher_progress(const share::SCN &round_start_scn, const LogFileTuple &tuple);
    int push_fetch_log(ObArchiveLogFetchTask &task);
    int push_send_task(ObArchiveSendTask &task, ObArchiveWorker &worker);
    int get_top_fetch_log(ObArchiveLogFetchTask *&task);
    int pop_fetch_log(ObArchiveLogFetchTask *&task);
    int compensate_piece(const int64_t piece_id);
    void get_max_archive_progress(LSN &piece_min_lsn, LSN &lsn, share::SCN &scn, ObArchivePiece &piece,
        int64_t &file_id, int64_t &file_offset, bool &error_exist);
    int update_archive_progress(const share::SCN &round_start_scn, const int64_t file_id, const int64_t file_offset, const LogFileTuple &tuple);
    void get_archive_progress(int64_t &file_id, int64_t &file_offset, LogFileTuple &tuple);
    void get_send_task_count(int64_t &count);
    void get_archive_send_arg(ObArchiveSendDestArg &arg);
    void get_max_no_limit_lsn(LSN &lsn);
    void update_no_limit_lsn(const palf::LSN &lsn);
    void mark_error();
    void print_tasks_();
    int64_t to_string(char *buf, const int64_t buf_len) const;

  private:
    void free_send_task_status_();
    void free_fetch_log_tasks_();

  private:
    bool               has_encount_error_;
    bool               is_worm_;
    // archive_lag_target with noneffective for logs whose lsn smaller than this lsn
    palf::LSN          max_no_limit_lsn_;
    palf::LSN          piece_min_lsn_;
    // archived log description
    LogFileTuple       max_archived_info_;
    int64_t archive_file_id_;
    int64_t archive_file_offset_;
    bool piece_dir_exist_;

    LSN       max_seq_log_offset_;
    LogFileTuple       max_fetch_info_;
    int64_t last_fetch_timestamp_;
    ObArchiveLogFetchTask *wait_send_task_array_[MAX_FETCH_TASK_NUM];
    int64_t             wait_send_task_count_;
    ObArchiveTaskStatus *send_task_queue_;

    ObArchiveAllocator *allocator_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ArchiveDest);
  };

private:
  bool is_task_stale_(const ArchiveWorkStation &station) const;
  int update_unlock_(const StartArchiveHelper &helper, ObArchiveAllocator *allocator);

private:
  ObLSID id_;
  uint64_t tenant_id_;
  ArchiveWorkStation station_;
  share::SCN round_start_scn_;
  ArchiveDest dest_;
  ObArchiveAllocator *allocator_;
  mutable RWLock rwlock_;
};

struct LSArchiveStat
{
  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t dest_id_;
  int64_t incarnation_;
  int64_t round_;
  // dest_type dest_value
  int64_t lease_id_;
  share::SCN round_start_scn_;
  int64_t max_issued_log_lsn_;
  int64_t issued_task_count_;
  int64_t issued_task_size_;
  int64_t max_prepared_piece_id_;
  int64_t max_prepared_lsn_;
  share::SCN max_prepared_scn_;
  int64_t wait_send_task_count_;
  int64_t archive_piece_id_;
  int64_t archive_lsn_;
  share::SCN archive_scn_;
  int64_t archive_file_id_;
  int64_t archive_file_offset_;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(dest_id), K_(incarnation), K_(round),
      K_(lease_id), K_(round_start_scn), K_(max_issued_log_lsn), K_(issued_task_count),
      K_(issued_task_size), K_(max_prepared_piece_id), K_(max_prepared_lsn),
      K_(max_prepared_scn), K_(wait_send_task_count), K_(archive_piece_id), K_(archive_lsn),
      K_(archive_scn), K_(archive_file_id), K_(archive_file_offset));
};

class ObArchiveLSGuard final
{
public:
  explicit ObArchiveLSGuard(ObArchiveLSMgr *ls_mgr);
  ~ObArchiveLSGuard();

public:
  void set_ls_task(ObLSArchiveTask *task);
  ObLSArchiveTask * get_ls_task();

  TO_STRING_KV(KPC(ls_task_));
private:
  void revert_ls_task_();

private:
  ObLSArchiveTask          *ls_task_;
  ObArchiveLSMgr           *ls_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveLSGuard);
};

} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_LOG_STREAM_TASK_H_ */
