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

#ifndef OCEANBASE_ARCHIVE_OB_ILOG_FETCHER_H_
#define OCEANBASE_ARCHIVE_OB_ILOG_FETCHER_H_

#include "lib/utility/ob_print_utils.h"  // TO_STRING_KV
#include "lib/allocator/ob_small_allocator.h"
#include "ob_log_archive_struct.h"
#include "ob_archive_log_wrapper.h"  // ObArchiveLogWrapper
#include "ob_archive_pg_mgr.h"
#include "ob_ilog_fetch_task_mgr.h"
#include "share/ob_thread_pool.h"
#include "clog/ob_log_define.h"

namespace oceanbase {
namespace archive {
using oceanbase::clog::ObGetCursorResult;
using oceanbase::clog::ObILogEngine;
class StartArchiveHelper;
class ObArCLogSplitEngine;
class ObArchiveAllocator;

class ObArchiveIlogFetcher : public share::ObThreadPool {
  // only single thread work mode
  static const int64_t ARCHIVE_ILOG_FETCHER_NUM = 1;
  static const int64_t WAIT_TIME_AFTER_EAGAIN = 100;

public:
  ObArchiveIlogFetcher();
  virtual ~ObArchiveIlogFetcher();

public:
  int start();
  void stop();
  void wait();
  int init(ObArchiveAllocator* allocator, ObILogEngine* log_engine, ObArchiveLogWrapper* log_wrapper,
      ObArchivePGMgr* pg_mgr, ObArCLogSplitEngine* clog_split_engine, ObArchiveIlogFetchTaskMgr* ilog_fetch_mgr);
  void destroy();

public:
  int build_archive_clog_task(const uint64_t start_log_id, ObGetCursorResult& cursor_result, ObPGArchiveCLogTask& task,
      PGFetchTask& fetch_task);
  void notify_start_archive_round();
  void notify_stop();
  int generate_and_submit_checkpoint_task(const common::ObPartitionKey& pg_key, const uint64_t log_id,
      const int64_t log_submit_ts, const int64_t checkpoint_ts);
  int generate_and_submit_pg_first_log(StartArchiveHelper& helper);
  ObPGArchiveCLogTask* alloc_clog_split_task();
  void free_clog_split_task(ObPGArchiveCLogTask* task);

  int set_archive_round_info(const int64_t round, const int64_t incarnation);
  void clear_archive_info();

private:
  struct IlogPGClogTask;
  void run1();
  void do_thread_task_();

  int get_pg_ilog_fetch_task_(PGFetchTask& task, bool& task_exist);

  int construct_pg_next_ilog_fetch_task_(
      PGFetchTask& fetch_task, ObPGArchiveTask& pg_archive_task, PGFetchTask& new_task);

  int push_back_pg_ilog_fetch_task_(PGFetchTask& task);

  int update_pg_split_progress_(
      const uint64_t end_log_id, bool empty_task, ObPGArchiveTask& pg_archive_task, PGFetchTask& task);

  int handle_ilog_fetch_task_(
      PGFetchTask& task, ObPGArchiveTask& pg_archive_task, bool& empty_task, uint64_t& end_log_id);

  int affirm_pg_log_consume_progress_(PGFetchTask& task, ObPGArchiveTask& pg_archive_task);

  int update_ilog_fetch_info_(PGFetchTask& task, ObPGArchiveTask& pg_archive_task, uint64_t& end_log_id, bool& need_do);

  int init_log_cursor_result_(const ObPGKey& pg_key, const int64_t arr_len, ObGetCursorResult& cursor_result);

  int build_clog_cursor_(
      PGFetchTask& fetch_task, const uint64_t min_log_id, const uint64_t max_log_id, ObPGArchiveCLogTask& task);

  int init_clog_split_task_meta_info_(
      const ObPGKey& pg_key, const uint64_t start_log_id, ObPGArchiveTask& pg_archive_task, ObPGArchiveCLogTask& task);
  int consume_clog_task_(ObPGArchiveCLogTask*& task, file_id_t ilog_file_id = -1);

  int check_and_consume_clog_task_(PGFetchTask& fetch_task, PGFetchTask& next_task);

  void mark_fatal_error_(
      const ObPGKey& pg_key, const int64_t epoch, const int64_t incarnation, const int64_t round, const int ret_code);

private:
  bool inited_;
  bool start_flag_;
  int64_t log_archive_round_;
  int64_t incarnation_;
  file_id_t cur_consume_ilog_file_id_;
  file_id_t cur_handle_ilog_file_id_;
  ObILogEngine* log_engine_;
  ObArchiveLogWrapper* log_wrapper_;
  ObArchivePGMgr* pg_mgr_;
  ObArCLogSplitEngine* clog_split_engine_;
  ObArchiveIlogFetchTaskMgr* ilog_fetch_mgr_;

  ObArchiveAllocator* allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveIlogFetcher);
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ILOG_FETCHER_H_ */
