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

#ifndef OCEANBASE_ARCHIVE_CLOG_SPLIT_ENGINE_
#define OCEANBASE_ARCHIVE_CLOG_SPLIT_ENGINE_
#include "common/ob_queue_thread.h"
#include "clog/ob_log_engine.h"
#include "ob_log_archive_struct.h"
#include "ob_archive_sender.h"
#include "share/ob_thread_pool.h"
#include "ob_archive_thread_pool.h"

namespace oceanbase {
namespace logservice {
class ObExtLogService;
}
namespace archive {

class ObArchivePGMgr;
class ObArchiveSender;
struct ObArchiveSendTaskStatus;
class ObArCLogSplitEngine : public ObArchiveThreadPool  // share::ObThreadPool
{
public:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;

public:
  ObArCLogSplitEngine();
  virtual ~ObArCLogSplitEngine();
  int init(logservice::ObExtLogService* ext_log_service, ObArchiveAllocator* allocator, ObArchiveSender* archive_sender,
      ObArchiveMgr* archive_mgr);
  void destroy();
  int start();
  void wait();
  void stop();
  // call this function when a new archive round is started
  int notify_start(const int64_t archive_round, const int64_t incarnation);
  // stop an archive round or encounter fatal error
  // call this function when an archive round is stopped or a fatal error is encountered
  int notify_stop(const int64_t archive_round, const int64_t incarnation);
  void handle(void* task);
  void clear_archive_info();
  int64_t cal_work_thread_num();
  void set_thread_name_str(char* str);
  int handle_task_list(ObArchiveTaskStatus* task_status);

public:
  int submit_split_task(ObPGArchiveCLogTask* task);

private:
  struct ObArchiveSplitStat {
  public:
    ObArchiveSplitStat()
    {
      reset();
    }
    ~ObArchiveSplitStat()
    {
      reset();
    }
    void reset();

  public:
    int64_t send_task_count_;
    int64_t read_log_used_;
    int64_t read_log_size_;
    int64_t get_send_task_used_;
  };
  int try_retire_task_status_(ObArchiveCLogTaskStatus& task_status);
  int submit_split_task_(ObPGArchiveCLogTask* task);
  int handle_archive_inner_task_(ObPGArchiveCLogTask& clog_task, bool& need_update_progress);
  int handle_task_with_compress_(ObPGArchiveCLogTask& clog_task);
  int handle_task_without_compress_(ObPGArchiveCLogTask& clog_task);
  int handle_archive_log_task_(ObPGArchiveCLogTask& clog_task);
  int build_original_block_(ObPGArchiveCLogTask& clog_task);
  int build_compressed_block_(ObPGArchiveCLogTask& clog_task, const common::ObCompressorType compressor_type);
  int get_compress_config_(
      const ObPGArchiveCLogTask& clog_task, bool& need_compress, ObCompressorType& compressor_type) const;
  int release_clog_split_task_(ObPGArchiveCLogTask*& task);
  int get_send_task_(const ObPGArchiveCLogTask& clog_task, const int64_t buf_len, ObArchiveSendTask*& send_task);
  int get_tsi_read_buf_(const ObPGArchiveCLogTask& clog_task, ObTSIArchiveReadBuf*& read_buf);
  int get_tsi_compress_buf_(const ObPGArchiveCLogTask& clog_task, ObTSIArchiveCompressBuf*& compress_buf);
  void release_send_task_(ObArchiveSendTask* send_task);
  int submit_send_task_(ObArchiveSendTask*& send_task);
  int fill_read_buf_(ObPGArchiveCLogTask& task, char* read_buf, const int64_t read_buf_len, int64_t& read_buf_pos,
      ObArchiveSendTaskMeta& send_buf);
  int fetch_log_with_retry_(const ObPGArchiveCLogTask& clog_task, ObArchiveLogCursor& log_cursor, char* read_buf,
      int64_t read_buf_len, int64_t& read_buf_pos, clog::ObLogEntry& log_entry);
  int fetch_log_(const ObPGKey& pg_key, ObArchiveLogCursor& log_cursor, char* read_buf, int64_t read_buf_len,
      int64_t& read_buf_pos, clog::ObLogEntry& log_entry);
  void on_fatal_error_(int err_ret);
  int mark_fatal_error_(
      const ObPGKey& pg_key, const int64_t epoch_id, const int64_t incarnation, const int64_t log_archive_round);
  int check_current_round_stopped_(
      const ObPGKey& pg_key, const int64_t incarnation, const int64_t archive_round, bool& is_stopped) const;
  int check_if_task_is_expired_(
      const ObPartitionKey& pkey, const int64_t incarnation, const int64_t log_archive_round) const;

  int get_chunk_header_serialize_size_(
      const ObPartitionKey& pkey, const bool need_compress, int64_t& chunk_header_size);
  int build_single_compressed_block_(ObPGArchiveCLogTask& clog_task, ObTSIArchiveReadBuf* tsi_read_buf,
      ObTSIArchiveCompressBuf* tsi_compress_buf, const ObCompressorType compressor_type, ObCompressor* compressor,
      ObArchiveSplitStat& stat);
  int fill_compressed_block_(const ObPGArchiveCLogTask& clog_task, const ObTSIArchiveReadBuf* tsi_read_buf,
      const ObTSIArchiveCompressBuf* tsi_compress_buf, const int64_t block_meta_size, const int64_t chunk_header_size,
      const common::ObCompressorType compressor_type, const bool has_compressed, const int64_t orig_data_len,
      const int64_t compressed_data_len, ObArchiveSendTask* send_task);
  void statistic(const ObArchiveSplitStat& stat);

private:
  // maybe replace these three with parameters later
  static const int64_t WAIT_TIME_AFTER_EAGAIN = DEFAULT_ARCHIVE_WAIT_TIME_AFTER_EAGAIN;
  static const int64_t MINI_MODE_SPLITER_THREAD_NUM = 1;

private:
  RWLock rwlock_;  // for log_archive_round, incarnation_ and current_round_stopped_
  int64_t log_archive_round_;
  int64_t incarnation_;

  // TODO:some stat info
  logservice::ObExtLogService* ext_log_service_;
  ObArchiveSender* archive_sender_;
  ObArchiveMgr* archive_mgr_;
  ObArchivePGMgr* archive_pg_mgr_;
  ObArchiveAllocator* allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObArCLogSplitEngine);
};

}  // namespace archive
}  // namespace oceanbase
#endif  // OCEANBASE_ARCHIVE_CLOG_SPLIT_ENGINE_
