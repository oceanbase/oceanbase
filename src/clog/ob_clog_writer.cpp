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

#include "ob_clog_writer.h"
#include "storage/ob_partition_service.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/file/file_directory_utils.h"
#include "share/config/ob_server_config.h"
#include "share/ob_bg_thread_monitor.h"
#include "clog/ob_log_common.h"
#include "clog/ob_log_entry.h"
#include "clog/ob_log_define.h"
#include "clog/ob_log_file_trailer.h"
#include "clog/ob_clog_file_writer.h"
#include "observer/ob_server_struct.h"
#include "election/ob_election.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::election;

namespace oceanbase {
namespace clog {
ObCLogWriter::ObCLogWriter()
    : is_started_(false),
      is_disk_error_(false),
      file_mutex_(),
      file_writer_(NULL),
      type_(INVALID_WRITE_POOL),
      log_cache_(0),
      info_getter_(NULL),
      tail_(NULL)
{}

ObCLogWriter::~ObCLogWriter()
{
  destroy();
}

int ObCLogWriter::init(ObCLogWriterCfg& clog_cfg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "The ObCLogWriter has been inited, ", K(ret));
  } else if (OB_UNLIKELY(!clog_cfg.is_valid()) || OB_UNLIKELY(1 != clog_cfg.base_cfg_.group_commit_min_item_cnt_) ||
             OB_UNLIKELY(1 != clog_cfg.base_cfg_.group_commit_max_item_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "Invalid argument, ", K(clog_cfg), K(ret));
  } else if (OB_FAIL(ObBaseLogWriter::init(clog_cfg.base_cfg_))) {
    CLOG_LOG(WARN, "Fail to init base log writer, ", K(ret));
  } else {
    file_writer_ = clog_cfg.log_file_writer_;
    log_cache_ = clog_cfg.log_cache_;
    info_getter_ = clog_cfg.info_getter_;
    tail_ = clog_cfg.tail_ptr_;
    type_ = clog_cfg.type_;
  }

  if (!is_inited()) {
    destroy();
  }
  return ret;
}

int ObCLogWriter::start(file_id_t& file_id, offset_t& offset)
{
  int ret = OB_SUCCESS;
  uint32_t load_file_id = file_id;
  uint32_t load_offset = offset;
  ObAtomicFilePos file_pos;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "The ObCLogWriter has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(is_started_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "The ObCLogWriter has been started, ", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(file_id), K(offset));
  } else if (OB_FAIL(file_writer_->load_file(load_file_id, load_offset, false))) {
    CLOG_LOG(WARN, "sync shared memory fail", K(ret), K(file_id), K(offset));
  } else {
    is_started_ = true;
    file_id = load_file_id;
    offset = load_offset;
    CLOG_LOG(INFO, "log writer start success", K(file_id), K(offset));
  }
  return ret;
}

void ObCLogWriter::destroy()
{
  ObBaseLogWriter::destroy();
  type_ = INVALID_WRITE_POOL;
  file_writer_ = NULL;
  log_cache_ = NULL;
  info_getter_ = NULL;
  tail_ = NULL;
  is_disk_error_ = false;
  is_started_ = false;
}

int ObCLogWriter::switch_file()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "The ObCLogWriter has not been inited, ", K(ret));
  } else {
    lib::ObMutexGuard guard(file_mutex_);
    if (OB_FAIL(inner_switch_file())) {
      CLOG_LOG(WARN, "Fail to switch file, ", K(ret));
    }
  }
  return ret;
}

file_id_t ObCLogWriter::get_file_id() const
{
  return (NULL == file_writer_) ? 0 : file_writer_->get_cur_file_id();
}

bool ObCLogWriter::is_disk_error() const
{
  bool b_ret = ATOMIC_LOAD(&is_disk_error_);
  if (!b_ret && nullptr != file_writer_) {
    b_ret = file_writer_->is_write_hang();
  }
  return b_ret;
}

int ObCLogWriter::set_is_disk_error()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
  } else {
    ATOMIC_STORE(&is_disk_error_, true);
    CLOG_LOG(ERROR, "clog disk is error!!!");
  }
  return ret;
}

int ObCLogWriter::reset_is_disk_error()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
  } else {
    ATOMIC_STORE(&is_disk_error_, false);
    CLOG_LOG(INFO, "reset clog disk status to normal");
  }
  return ret;
}

void ObCLogWriter::set_clog_writer_thread_name()
{
  static __thread bool has_set_thread_name = false;
  if (!has_set_thread_name) {
    lib::set_thread_name("CLGWR");
    has_set_thread_name = true;
  }
}

void ObCLogWriter::process_log_items(common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt)
{
  int ret = OB_SUCCESS;
  ObICLogItem* item = NULL;
  int64_t sync_mode = ObServerConfig::get_instance().flush_log_at_trx_commit;
  int64_t cur_time = 0;
  int64_t io_time = 0;
  int64_t flush_time = 0;
  finish_cnt = 0;
  ObLogBlockMetaV2 block_meta;
  const int64_t block_meta_len = block_meta.get_serialize_size();
  int64_t meta_pos = 0;
  const bool is_disk_error = ATOMIC_LOAD(&is_disk_error_);
  set_clog_writer_thread_name();
  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "The ObCLogWriter has not been started, ", K(ret));
  } else if (OB_UNLIKELY(NULL == items) || OB_UNLIKELY(item_cnt <= 0) || OB_UNLIKELY(item_cnt > 1) ||
             NULL == (item = reinterpret_cast<ObICLogItem*>(items[0])) || OB_UNLIKELY(!item->is_valid()) ||
             OB_UNLIKELY(item->get_data_len() > OB_MAX_LOG_BUFFER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "Invalid argument, ", K(ret), KP(items), K(item_cnt));
  } else if (OB_UNLIKELY(is_disk_error)) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(ERROR, "The ObCLogWriter met disk error and been frozen, ", K(ret), K(is_disk_error));
    after_flush(item, block_meta_len, ret, file_writer_->get_cur_file_len(), finish_cnt);
  } else {
    const bool is_idempotent = false;
    const uint64_t write_len = block_meta_len + item->get_data_len();
    ObCLogDiskErrorCB* cb = NULL;
    if (CLOG_DISK_SYNC != sync_mode && CLOG_MEM_SYNC != sync_mode) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "Not supported sync mode, ", K(sync_mode));
      }
      sync_mode = CLOG_DISK_SYNC;
    }

    lib::ObMutexGuard guard(file_mutex_);

    BG_NEW_CALLBACK(cb, ObCLogDiskErrorCB, this);
    BG_MONITOR_GUARD(DEFAULT_CLOG_FLUSH_TIMEOUT_US, is_idempotent, cb);

    // The timestamp value in block header must be generated by the time order, so
    // call inner_switch_file first here.
    if (need_switch_file(write_len) && OB_FAIL(inner_switch_file())) {
      CLOG_LOG(ERROR, "Fail to switch file, ", K(ret), K(file_writer_->get_cur_file_id()));
    } else if (OB_FAIL(block_meta.build_serialized_block(item->get_buf() - block_meta_len,
                   block_meta_len,
                   item->get_buf(),
                   item->get_data_len(),
                   OB_DATA_BLOCK,
                   meta_pos))) {
      CLOG_LOG(ERROR, "build serialized block meta fail", K(ret));
    }

    // invoke callback when fail
    if (OB_FAIL(ret)) {
      after_flush(item, block_meta_len, ret, file_writer_->get_cur_file_len(), finish_cnt);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(file_writer_->append_log_entry(item->get_buf() - block_meta_len, (uint32_t)write_len))) {
        CLOG_LOG(ERROR, "fail to add log item to buf, ", K(ret));
      }
    }
    // invoke callback when memory sync
    if (OB_SUCC(ret) && CLOG_MEM_SYNC == sync_mode) {
      after_flush(item, block_meta_len, ret, file_writer_->get_cur_file_len(), finish_cnt);
    }

    int64_t flush_start_offset = -1;
    if (OB_SUCC(ret)) {
      cur_time = ObTimeUtility::current_time();
      do {
        // TODO: flush log will not return OB_TIMEOUT, other IO error will be treated as bug
        if (OB_FAIL(file_writer_->flush(info_getter_, log_cache_, tail_, flush_start_offset))) {
          set_is_disk_error();
          // flush log to disk until die when IO hang, other IO error will be treated as bug
          if (OB_TIMEOUT == ret && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
            ret = OB_IO_ERROR;
            CLOG_LOG(ERROR, "flush IO hang, retrying", K(ret));
          } else {
            CLOG_LOG(ERROR, "Fail to flush clog to disk, ", K(ret));
          }
        } else {
          reset_is_disk_error();
        }
      } while (!has_stoped() && OB_TIMEOUT == ret);
    }

    if (OB_SUCC(ret)) {
      io_time = ObTimeUtility::current_time() - cur_time;
      // log flush succeed, invoke callback when disk sync
      if (CLOG_DISK_SYNC == sync_mode) {
        after_flush(item, block_meta_len, ret, flush_start_offset, finish_cnt);
      }
      flush_time = ObTimeUtility::current_time() - cur_time - io_time;

      if (flush_time + io_time > 100 * 1000) {
        CLOG_LOG(WARN,
            "slow flush",
            K(flush_time),
            K(io_time),
            "file_id",
            file_writer_->get_cur_file_id(),
            K(flush_start_offset),
            "file_offset",
            file_writer_->get_cur_file_len());
      }

      if (CLOG_WRITE_POOL == type_) {
        EVENT_INC(CLOG_WRITE_COUNT);
        EVENT_ADD(CLOG_WRITE_TIME, io_time);
      } else if (ILOG_WRITE_POOL == type_) {
        EVENT_INC(ILOG_WRITE_COUNT);
        EVENT_ADD(ILOG_WRITE_TIME, io_time);
      } else {
        CLOG_LOG(ERROR, "unknown write pool type", K(type_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    CLOG_LOG(ERROR, "log writer write data error", K(ret));
    if (!has_stoped()) {
      on_fatal_error(ret);
    }
  }
  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    CLOG_LOG(INFO, "ObCLogWriter get_queued_item_cnt", "queued_cnt", get_queued_item_cnt());
  }
}

bool ObCLogWriter::need_switch_file(const uint64_t write_len) const
{
  // Left space is not enough for data or info block
  uint64_t max_switch_file_limit = 0;
  if (CLOG_WRITE_POOL == type_) {
    max_switch_file_limit = CLOG_MAX_SWITCH_FILE_LIMIT;
  } else {
    max_switch_file_limit = ILOG_MAX_SWITCH_FILE_LIMIT;
  }
  return !file_writer_->enough_file_space(write_len) || (info_getter_->get_entry_cnt() > max_switch_file_limit);
}

void ObCLogWriter::after_flush(ObICLogItem* item, const int64_t block_meta_len, const int err_code,
    const uint32_t file_offset, int64_t& finish_cnt)
{
  int ret = OB_SUCCESS;
  uint32_t file_id = file_writer_->get_cur_file_id();

  if (OB_SUCCESS == err_code) {
    const common::ObAddr addr = GCTX.self_addr_;
    if (OB_FAIL(log_cache_->append_data(addr,
            (char*)(item->get_buf() - block_meta_len),
            file_id,
            file_offset,
            item->get_data_len() + block_meta_len))) {
      CLOG_LOG(ERROR, "Fail to append data to cache, ", K(ret));
    }
    tail_->advance(file_id, file_offset + (uint32_t)item->get_data_len() + (uint32_t)block_meta_len);
  }

  if (OB_FAIL(item->after_flushed(file_id, file_offset + (uint32_t)block_meta_len, err_code, type_))) {
    CLOG_LOG(ERROR, "Fail to process item after flush, ", K(ret));
  }
  ++finish_cnt;
}

int ObCLogWriter::inner_switch_file()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  if (OB_FAIL(file_writer_->end_current_file(info_getter_, log_cache_, tail_))) {
    CLOG_LOG(ERROR, "switch file failed", K(ret));
  }

  // keep try open new file when too many open files, other IO error will be treated as bug
  if (OB_SUCC(ret)) {
    do {
      if (OB_FAIL(file_writer_->create_next_file())) {
        is_disk_error_ = true;
        if (OB_FILE_NOT_OPENED == ret) {
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
            CLOG_LOG(ERROR, "Too many open files, retrying", K(ret));
          }
          sleep(30);
        } else {
          CLOG_LOG(ERROR, "Fail to switch file, ", K(ret), K(file_writer_->get_cur_file_id()));
        }
      } else {
        is_disk_error_ = false;
      }
    } while (!has_stoped() && OB_FILE_NOT_OPENED == ret);
  }

  if (OB_SUCC(ret)) {
    if (CLOG_WRITE_POOL == type_) {
      EVENT_ADD(CLOG_FILE_TOTAL_SIZE, CLOG_FILE_SIZE);
    } else if (ILOG_WRITE_POOL == type_) {
      EVENT_ADD(ILOG_FILE_TOTAL_SIZE, CLOG_FILE_SIZE);
    }
    const int64_t end = ObTimeUtility::current_time();
    CLOG_LOG(INFO, "switch file success", K(*tail_), "new_file", file_writer_->get_cur_file_id(), "used", end - start);
  } else {
    CLOG_LOG(ERROR, "switch file error", K(ret), "new_file", file_writer_->get_cur_file_id());
  }

  return ret;
}

ObCLogDiskErrorCB::ObCLogDiskErrorCB(ObCLogWriter* host) : host_(host)
{}

ObCLogDiskErrorCB::~ObCLogDiskErrorCB()
{}

int ObCLogDiskErrorCB::callback()
{
  int ret = OB_SUCCESS;
  storage::ObPartitionService& partition_service = storage::ObPartitionService::get_instance();
  if (OB_ISNULL(host_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(host_->set_is_disk_error())) {
    CLOG_LOG(ERROR, "ObCLogDiskErrorCB set_is_disk_error failed", K(ret));
  } else if (OB_FAIL(partition_service.try_revoke_all_leader(ObElection::RevokeType::CLOG_DISK_HANG))) {
    CLOG_LOG(ERROR, "ObCLogDiskErrorCB try_revoke_all_leader failed", K(ret));
  }
  CLOG_LOG(INFO, "call leader revoke because of clog disk hang", K(ret));
  return ret;
}

void ObCLogDiskErrorCB::destroy()
{
  // If disk has real error, may cause is_disk_error be false,
  // however, observer will be killed.
  if (OB_NOT_NULL(host_)) {
    (void)host_->reset_is_disk_error();
  }
}

int locate_clog_tail(
    const int64_t timeout, ObILogFileStore* file_store, ObLogDirectReader* reader, file_id_t& file_id, offset_t& offset)
{
  ObLogFileTailLocatorImpl<ObLogEntry, ObIRawLogIterator> impl;
  return impl.locate_tail(timeout, file_store, reader, file_id, offset);
}

int locate_ilog_tail(
    const int64_t timeout, ObILogFileStore* file_store, ObLogDirectReader* reader, file_id_t& file_id, offset_t& offset)
{
  ObLogFileTailLocatorImpl<ObIndexEntry, ObIRawIndexIterator> impl;
  return impl.locate_tail(timeout, file_store, reader, file_id, offset);
}

}  // namespace clog
}  // namespace oceanbase
