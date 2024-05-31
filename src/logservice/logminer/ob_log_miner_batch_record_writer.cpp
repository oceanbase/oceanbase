/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LOGMNR

#include "lib/container/ob_ext_ring_buffer.h"
#include "ob_log_miner_batch_record_writer.h"
#include "ob_log_miner_batch_record.h"
#include "ob_log_miner_file_manager.h"
#include "ob_log_miner_resource_collector.h"
#include "ob_log_miner_data_manager.h"

namespace oceanbase
{
namespace oblogminer
{

ObLogMinerBatchRecordWriter::ObLogMinerBatchRecordWriter():
    is_inited_(false),
    current_file_id_(-1),
    current_file_offset_(-1),
    batch_record_sw_(),
    record_count_(0),
    batch_record_count_(0),
    data_manager_(nullptr),
    resource_collector_(nullptr),
    file_manager_(nullptr),
    err_handle_(nullptr) { }

int ObLogMinerBatchRecordWriter::init(ILogMinerDataManager *data_mgr,
    ILogMinerResourceCollector *res_collector,
    ILogMinerFileManager *file_mgr,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;
  const int64_t SLIDING_WINDOW_SIZE = 2L << 20;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("BatchRecordWriter has been initialized", K(is_inited_));
  } else if (OB_ISNULL(data_mgr) || OB_ISNULL(res_collector) || OB_ISNULL(file_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid argument when initializing BatchRecordWriter", K(data_mgr),
        K(res_collector), K(file_mgr));
  } else if (OB_FAIL(BatchWriterThreadPool::init(BATCH_WRITER_THREAD_NUM, BATCH_WRITER_QUEUE_SIZE))) {
    LOG_ERROR("BatchWriterThreadPool failed to init");
  } else if (OB_FAIL(batch_record_sw_.init(1, SLIDING_WINDOW_SIZE))) {
    LOG_ERROR("batch record sliding window failed to initialize", K(SLIDING_WINDOW_SIZE));
  } else {
    current_file_id_ = 0;
    current_file_offset_ = 0;
    data_manager_ = data_mgr;
    resource_collector_ = res_collector;
    file_manager_ = file_mgr;
    err_handle_ = err_handle;
    is_inited_ = true;
    LOG_INFO("ObLogMinerBatchRecordWriter finished to init", K(data_manager_), K(resource_collector_),
        K(file_manager_));
  }

  return ret;
}

int ObLogMinerBatchRecordWriter::start()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(BatchWriterThreadPool::start())) {
    LOG_ERROR("BatchWriterThreadPool failed to start");
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_ERROR("thread pool in batchrecord writer failed to start");
  } else {
    LOG_INFO("ObLogMinerBatchRecordWriter starts");
  }

  return ret;
}

void ObLogMinerBatchRecordWriter::stop()
{
  BatchWriterThreadPool::mark_stop_flag();
  lib::ThreadPool::stop();
  LOG_INFO("ObLogMinerBatchRecordWriter stopped");
}

void ObLogMinerBatchRecordWriter::wait()
{
  BatchWriterThreadPool::stop();
  lib::ThreadPool::wait();
  LOG_INFO("ObLogMinerBatchRecordWriter finished to wait");
}

void ObLogMinerBatchRecordWriter::destroy()
{
  if (IS_INIT) {
    BatchWriterThreadPool::destroy();
    lib::ThreadPool::destroy();
    current_file_id_ = -1;
    current_file_offset_ = -1;
    data_manager_ = nullptr;
    resource_collector_ = nullptr;
    file_manager_ = nullptr;
    err_handle_ = nullptr;
    record_count_ = 0;
    batch_record_count_ = 0;
    batch_record_sw_.destroy();
    is_inited_ = false;
  }
}


// only one thread will call this method
int ObLogMinerBatchRecordWriter::push(ObLogMinerBatchRecord *record)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("BatchRecordWriter hasn't been initialized", K(is_inited_));
  } else if (OB_ISNULL(record)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid record when trying to push record into BatchWriter", K(record));
  } else {
    LOG_TRACE("ObLogMinerBatchRecordWriter push batch record into queue", KPC(record));
    const int64_t DATA_OP_TIMEOUT = 1L * 1000 * 1000;
    static volatile int64_t seq_no = 0;
    const int64_t assigned_seq_no = ATOMIC_AAF(&seq_no, 1);
    if (OB_FAIL(set_file_info_(*record, assigned_seq_no))) {
      LOG_ERROR("set file info for record failed", K(record), K(current_file_id_), K(current_file_offset_));
    } else {
      ATOMIC_AAF(&record_count_, record->get_total_record_count());
      ATOMIC_INC(&batch_record_count_);
      RETRY_FUNC(is_stoped(), *(static_cast<ObMQThread*>(this)), push, record, record->get_file_id(), DATA_OP_TIMEOUT);
    }
  }

  return ret;
}

int ObLogMinerBatchRecordWriter::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("BatchRecordWriter hasn't been initialized", K(is_inited_));
  } else if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid data when handling data in BatchWriter", K(data));
  } else {
    ObLogMinerBatchRecord *record = static_cast<ObLogMinerBatchRecord*>(data);
    const char *data = nullptr;
    int64_t data_len = 0;
    record->get_data(data, data_len);
    if (OB_FAIL(file_manager_->append_records(*record))) {
      LOG_ERROR("write file through file manager failed", KPC(record), K(data_len));
    } else if (OB_FAIL(generate_meta_flush_task_(*record))) {
      LOG_ERROR("generate meta flush task failed", K(record));
    } else {
      ATOMIC_SAF(&record_count_, record->get_total_record_count());
      ATOMIC_DEC(&batch_record_count_);
      if(OB_FAIL(resource_collector_->revert(record))) {
        LOG_ERROR("resource collecter failed to revert record", K(record));
      }
    }
  }

  if (OB_FAIL(ret)) {
    err_handle_->handle_error(ret, "ObLogMinerBatchRecordWriter exit unexpected\n");
  }

  return ret;
}

int ObLogMinerBatchRecordWriter::get_total_record_count(int64_t &record_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("BatchRecordWriter hasn't been initialized", K(is_inited_));
  } else {
    record_count = ATOMIC_LOAD(&record_count_);
  }

  return ret;
}

int ObLogMinerBatchRecordWriter::get_total_batch_record_count(int64_t &batch_record_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("BatchRecordWriter hasn't been initialized", K(is_inited_));
  } else {
    batch_record_count = ATOMIC_LOAD(&batch_record_count_);
  }

  return ret;
}

void ObLogMinerBatchRecordWriter::run1()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("BatchRecordWriter hasn't been initialized", K(is_inited_));
  } else {
    const int64_t META_FLUSH_INTERVAL = 1000L * 1000; // 1s
    while (OB_SUCC(ret) && ! lib::Threads::has_set_stop()) {
      if (OB_FAIL(proceed_checkpoint_())) {
        LOG_ERROR("logminer batchrecord writer failed to proceed checkpoint");
      }
      ob_usleep(META_FLUSH_INTERVAL);
    }
  }

  if (OB_FAIL(ret)) {
    err_handle_->handle_error(ret, "Checkpointer exit unexpected\n");
  }
}

int ObLogMinerBatchRecordWriter::set_file_info_(ObLogMinerBatchRecord &record,
    const int64_t seq_no)
{
  int ret = OB_SUCCESS;
   if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("BatchRecordWriter hasn't been initialized", K(is_inited_));
  } else {
    const char *unused_data = nullptr;
    const int64_t record_max_commit_ts = record.get_progress_range().max_commit_ts_;
    const int64_t cur_trans_record_count = record.get_cur_trans_record_count();
    int64_t data_len = 0;
    record.get_data(unused_data, data_len);

    record.set_seq_no(seq_no);
    record.set_file_id(current_file_id_);
    record.set_file_offset(current_file_offset_);

    if (0 >= cur_trans_record_count &&
        data_manager_->reach_end_progress(record_max_commit_ts)) {
      record.set_file_end();
      LOG_TRACE("max commit ts of batch record exceed the end_progress and there is no "
          "pending transaction", K(record_max_commit_ts), K(cur_trans_record_count));
    }

    // invariant: current_file_offset_ < FILE_SPLIT_THRESHOLD
    ATOMIC_AAF(&current_file_offset_, data_len);

    if (current_file_offset_ >= FILE_SPLIT_THRESHOLD) {
      ATOMIC_INC(&current_file_id_);
      ATOMIC_STORE(&current_file_offset_, 0);
      record.set_file_end();
    }

  }

  return ret;
}

int ObLogMinerBatchRecordWriter::generate_meta_flush_task_(const ObLogMinerBatchRecord &record)
{
  int ret = OB_SUCCESS;
  BatchRecordMetaTask *task = reinterpret_cast<BatchRecordMetaTask*>(ob_malloc(
      sizeof(BatchRecordMetaTask), "LogMnrMetaTask"));
  if (OB_ISNULL(task)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("meta task is null, allocate memory failed", K(task));
  } else {
    task = new(task) BatchRecordMetaTask();
    task->file_id_ = record.get_file_id();
    task->is_file_complete_ = record.is_file_end();
    task->last_trans_end_ts_ = record.get_last_trans_end_ts();
    task->record_cnt_ = record.get_written_record_count();
    LOG_TRACE("generated meta flush task", KPC(task), "seq_no", record.get_seq_no());
    if (OB_FAIL(batch_record_sw_.set(record.get_seq_no(), task))) {
      LOG_ERROR("set task into batch_record sliding window failed", K(record));
    }
  }

  return ret;
}

int ObLogMinerBatchRecordWriter::proceed_checkpoint_()
{
  int ret = OB_SUCCESS;
  BatchRecordMetaTaskPopFunc func;
  BatchRecordMetaTask *last_poped_task = nullptr, *curr_task = nullptr;
  ObLogMinerCheckpoint ckpt;
  bool popped = true;
  constexpr int64_t PRINT_INTERVAL = 10L * 1000 * 1000;

  while (OB_SUCC(ret) && popped && OB_SUCC(batch_record_sw_.pop(func, curr_task, popped))) {
    // continuity check
    if (popped && nullptr != curr_task) {
      if (OB_INVALID_TIMESTAMP == ckpt.progress_ || ckpt.progress_ <= curr_task->last_trans_end_ts_) {
        ckpt.progress_ = curr_task->last_trans_end_ts_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("the progress of meta_flush task rollbacked", K(ckpt), KPC(curr_task));
      }

      if (-1 == ckpt.cur_file_id_ || ckpt.cur_file_id_ <= curr_task->file_id_) {
        ckpt.cur_file_id_ = curr_task->file_id_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("the file id of meta_flush task rollbacked", K(ckpt), KPC(curr_task));
      }

      if (curr_task->is_file_complete_) {
        if (-1 == ckpt.max_file_id_ || ckpt.max_file_id_ == curr_task->file_id_ - 1) {
          ckpt.max_file_id_ = curr_task->file_id_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("max file id of meta_flush task not expected", K(ckpt), KPC(curr_task));
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(data_manager_->increase_record_count(curr_task->record_cnt_))) {
        LOG_ERROR("failed to increase record count", K(curr_task->record_cnt_));
      }

      LOG_TRACE("task poped", KPC(curr_task));
      if (nullptr != last_poped_task) {
        ob_free(last_poped_task);
      }
      // cannot access last_poped_task afterwards
      last_poped_task = curr_task;
    } else {
      last_poped_task = nullptr;
    }
  }

  if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_ERROR("failed to proceed checkpoint", K(curr_task));
  } else {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    if (nullptr != curr_task) {
      ckpt.cur_file_id_ = curr_task->file_id_;
      ckpt.progress_ = curr_task->last_trans_end_ts_;
      ckpt.max_file_id_ = curr_task->is_file_complete_ ? curr_task->file_id_ : curr_task->file_id_ - 1;

      if (OB_FAIL(file_manager_->write_checkpoint(ckpt))) {
        LOG_ERROR("file_manager failed to write checkpoint", K(ckpt), K(curr_task));
      } else if (OB_FAIL(data_manager_->update_output_progress(ckpt.progress_))) {
        LOG_ERROR("failed to update output progress", K(ckpt));
      } else {
        if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
          LOG_INFO("succeed to proceed checkpoint", K(ckpt), K(curr_task));
        }
      }

      if (OB_SUCC(ret)) {
        // cannot access curr_task afterwards
        LOG_TRACE("proceed checkpoint", K(ckpt), KPC(curr_task));
        ob_free(curr_task);
      }
    } else {
      // only one thread
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        LOG_INFO("try to proceed checkpoint but no batch record meta task", K(popped));
      }
    }
  }

  return ret;
}

}
}