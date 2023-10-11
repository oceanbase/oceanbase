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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_SENDER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_SENDER_H_

#include "common/ob_queue_thread.h"
#include "logservice/archiveservice/ob_archive_define.h"
#include "logservice/archiveservice/ob_archive_round_mgr.h"
#include "share/backup/ob_backup_struct.h"  // ObBackupPathString
#include "share/ob_thread_pool.h"           // ObThreadPool
#include "ob_archive_task.h"                // ObArchiveSendTask
#include "ob_archive_worker.h"              // ObArchiveWorker
#include "lib/queue/ob_lighty_queue.h"      // ObLightyQueue
#include <cstdint>

namespace oceanbase
{
namespace share
{
class ObLSID;
class ObArchivePiece;
}
namespace backup
{
class ObBackupPath;
}
namespace archive
{
class ObArchiveAllocator;
class ObArchiveLSMgr;
class ObArchivePersistMgr;
class ObArchiveRoundMgr;
class ObArchiveSendTask;
class ObLSArchiveTask;
using oceanbase::share::ObLSID;
/*
 * ObArchiveSender调用底层存储接口, 最终将clog文件写到备份介质
 * 当前实现下, sender模块串行为单个日志流归档数据, 由底层存储接口保证写出数据的并发
 * sender模块是多线程的, 单个线程采用阻塞上传的方式消费SendTask, 并推高日志流归档进度
 * */
class ObArchiveSender : public share::ObThreadPool, public ObArchiveWorker
{
  static const int64_t MAX_SEND_NUM = 10;
  static const int64_t MAX_ARCHIVE_TASK_STATUS_POP_TIMEOUT = 5 * 1000 * 1000L;
public:
  ObArchiveSender();
  virtual ~ObArchiveSender();

public:
  int start();
  void stop();
  void wait();
  int init(const uint64_t tenant_id,
      ObArchiveAllocator *allocator,
      ObArchiveLSMgr *ls_mgr,
      ObArchivePersistMgr *persist_mgr,
      ObArchiveRoundMgr *round_mgr);
  void destroy();
  void release_send_task(ObArchiveSendTask *task);
  int submit_send_task(ObArchiveSendTask *task);
  int push_task_status(ObArchiveTaskStatus *task_status);
  int64_t get_send_task_status_count() const;

  int modify_thread_count(const int64_t thread_count);
private:
  enum class DestSendOperator
  {
    SEND = 1,
    WAIT = 2,
    COMPENSATE = 3,
  };

  enum class TaskConsumeStatus
  {
    INVALID = 0,
    DONE = 1,
    STALE_TASK = 2,
    NEED_RETRY = 3,
  };
private:
  int submit_send_task_(ObArchiveSendTask *task);
  void run1();
  void do_thread_task_();

  int try_consume_send_task_();

  int do_consume_send_task_();

  // 消费task status, 为日志流级别send_task队列, 目前为单线程消费单个日志流
  int get_send_task_(ObArchiveSendTask *&task, bool &exist);

  void handle(ObArchiveSendTask &task, TaskConsumeStatus &consume_status);

  // 1. 检查server归档状态
  bool in_normal_status_(const ArchiveKey &key) const;

  // 2. 检查该send_task是否可以归档
  int check_can_send_task_(const ObArchiveSendTask &task,
      const LogFileTuple &tuple);

  // 2.1 检查piece是否连续
  int check_piece_continuous_(const ObArchiveSendTask &task,
      const LogFileTuple &tuple,
      int64_t &next_piece_id,
      DestSendOperator &operation);
  int do_compensate_piece_(const share::ObLSID &id,
      const int64_t next_piece_id,
      const ArchiveWorkStation &station,
      const share::ObBackupDest &backup_dest,
      ObLSArchiveTask &ls_archive_task);

  // 3. 执行归档
  int archive_log_(const share::ObBackupDest &backup_dest,
      const ObArchiveSendDestArg &arg,
      ObArchiveSendTask &task,
      ObLSArchiveTask &ls_archive_task);

  // 3.1 decide archive file
  int decide_archive_file_(const ObArchiveSendTask &task,
      const int64_t pre_file_id,
      const int64_t pre_file_offset,
      const ObArchivePiece &pre_piece,
      int64_t &file_id,
      int64_t &file_offset);

  // 3.2 build archive dir if needed
  int build_archive_prefix_if_needed_(const ObLSID &id,
      const ArchiveWorkStation &station,
      const bool piece_dir_exist,
      const ObArchivePiece &pre_piece,
      const ObArchivePiece &cur_piece,
      const share::ObBackupDest &backup_dest);

  // 3.3 build archive path
  int build_archive_path_(const ObLSID &id,
      const int64_t file_id,
      const ArchiveWorkStation &station,
      const ObArchivePiece &piece,
      const share::ObBackupDest &backup_dest,
      share::ObBackupPath &path);

  // 3.4 fill file header
  //
  int fill_file_header_if_needed_(const ObArchiveSendTask &task,
      char *&filled_data,
      int64_t &filled_data_len);

  // 3.5 push log
  int push_log_(const share::ObLSID &id,
      const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      const bool is_full_file,
      const int64_t offset,
      char *data,
      const int64_t data_len);

  // 3.6 执行归档callback
  void update_archive_progress_(ObArchiveSendTask &task);

  // retire task status
  int try_retire_task_status_(ObArchiveTaskStatus &status);

  // free residual send_tasks when sender destroy
  int free_residual_task_();

  void handle_archive_ret_code_(const ObLSID &id,
      const ArchiveKey &key,
      const int ret_code);

  bool is_retry_ret_code_(const int ret_code) const;
  bool is_ignore_ret_code_(const int ret_code) const;

  void statistic(const int64_t log_size, const int64_t buf_size, const int64_t cost_ts);

  int try_free_send_task_();
  int do_free_send_task_();
private:
  bool                  inited_;
  uint64_t              tenant_id_;
  ObArchiveAllocator    *allocator_;
  ObArchiveLSMgr        *ls_mgr_;
  ObArchivePersistMgr   *persist_mgr_;
  ObArchiveRoundMgr     *round_mgr_;

  common::ObLightyQueue task_queue_;            // 存放ObArchiveTaskStatus的queue
  common::ObCond        send_cond_;
};

} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_SENDER_H_ */
