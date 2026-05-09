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
 *
 * Storager
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_storager.h"
#include "ob_log_instance.h"
#include "ob_log_store_service.h"
#include "ob_log_factory.h"                      // ObLogStoreTaskFactory
#include "ob_log_trace_id.h"                     // ObLogTraceIdGuard
#include "ob_log_config.h"                       // TCONF

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

// static __thread DrcMsgBuf *lmb = NULL;
// Note: serialize binlog record requie a thread_local DrcMsgBuf for serialize thread

ObLogStorager::ObLogStorager() :
    inited_(false),
    round_value_(0),
    rps_stat_(),
    last_stat_time_(0),
    block_count_(0),
    log_task_count_(0),
    store_service_stat_(),
    store_service_(NULL),
    err_handler_(NULL)
{
}

ObLogStorager::~ObLogStorager()
{
  destroy();
}

int ObLogStorager::init(const int64_t thread_num,
    const int64_t queue_size,
    IObStoreService &store_service,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogStorager has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)) {
    LOG_ERROR("invalid arguments", K(thread_num), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(StoragerThread::init(thread_num, queue_size))) {
    LOG_ERROR("init storager queue thread fail", K(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(disk_io_monitor_.init())) {
    LOG_ERROR("disk_io_monitor_ init fail", KR(ret));
  } else {
    round_value_ = 0;
    rps_stat_.reset();
    last_stat_time_ = get_timestamp();
    store_service_ = &store_service;
    block_count_ = 0;
    log_task_count_ = 0;
    err_handler_ = &err_handler;
    inited_ = true;
  }

  return ret;
}

void ObLogStorager::destroy()
{
  stop();
  if (inited_) {
    LOG_INFO("store_service destroy begin");
    StoragerThread::destroy();
    disk_io_monitor_.destroy();

    inited_ = false;
    round_value_ = 0;
    rps_stat_.reset();
    last_stat_time_ = 0;
    block_count_ = 0;
    log_task_count_ = 0;
    store_service_stat_.reset();
    store_service_ = NULL;
    err_handler_ = NULL;
    LOG_INFO("store_service destroy end");
  }
}

int ObLogStorager::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(StoragerThread::start())) {
    LOG_ERROR("start storager thread fail", K(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start storager threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogStorager::stop()
{
  mark_stop_flag();
  if (inited_) {
    StoragerThread::stop();
    LOG_INFO("stop storager threads succ", "thread_num", get_thread_num());
  }
}

int ObLogStorager::submit(IObLogBatchBufTask *task)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogStorager has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(! task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), KPC(task));
  } else if (OB_UNLIKELY(is_stoped())) {
    ret = OB_IN_STOP_STATE;
    LOG_INFO("obcdc storager is in stop state", KR(ret));
  } else {
    int64_t sub_task_count = task->get_subtask_count();
    uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    void *push_task = static_cast<void *>(task);

    Block *block = static_cast<Block *>(task);
    IObLogBufTask *curr_task = task->get_header_task();
    ObLogStoreTask *store_task = static_cast<ObLogStoreTask *>(curr_task);
    LOG_DEBUG("Storager submit succ", "addr", &task, KPC(task), KPC(block), KPC(store_task));

    if (OB_FAIL(StoragerThread::push(push_task, hash_value, timeout))) {
      if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push task into storager fail", K(ret), K(push_task), K(hash_value));
      }
    } else {
      // succ
      ATOMIC_INC(&block_count_);
      ATOMIC_AAF(&log_task_count_, sub_task_count);
    }
  }

  return ret;
}

void ObLogStorager::get_task_count(int64_t &block_count, int64_t &log_task_count) const
{
  block_count = ATOMIC_LOAD(&block_count_);
  log_task_count = ATOMIC_LOAD(&log_task_count_);
}

int ObLogStorager::thread_begin()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObLogStorager::thread_end()
{
}

int ObLogStorager::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  IObLogBatchBufTask *task = static_cast<IObLogBatchBufTask *>(data);
  Block *block = static_cast<Block *>(task);
  LOG_DEBUG("Storager handle succ", "addr", &task, KPC(task), KPC(block));

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    LOG_ERROR("invalid arguments", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_stoped())) {
    ret = OB_IN_STOP_STATE;
    LOG_INFO("obcdc storager is in stop state", KR(ret));
  } else {
    int64_t sub_task_count = task->get_subtask_count();

    if (OB_FAIL(handle_task_(*task, thread_index, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_task_ fail", KR(ret), KPC(task), K(thread_index));
      }
    } else {
      task = NULL;
      ATOMIC_DEC(&block_count_);
      ATOMIC_AAF(&log_task_count_, -sub_task_count);
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "storager thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObLogStorager::handle_task_(IObLogBatchBufTask &batch_task,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const char *batch_buf = batch_task.get_batch_buffer();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(batch_buf)) {
    LOG_ERROR("batch_buf is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Check if batch write is enabled
    const bool enable_batch_write = (1 == TCONF.enable_batch_write.get());

    if (enable_batch_write) {
      ret = handle_task_batch_write_(batch_task, thread_index, stop_flag, batch_buf);
    } else {
      ret = handle_task_single_write_(batch_task, thread_index, stop_flag, batch_buf);
    }

    // Common cleanup logic for both paths.
    // NOTE: on failure, BigBlock/Block is not freed here. The caller will invoke
    // err_handler_ which triggers process exit, so the leak is acceptable.
    if (OB_SUCC(ret)) {
      const bool is_big_block = batch_task.is_big_block();
      if (is_big_block) {
        BigBlock *big_block = static_cast<BigBlock *>(&batch_task);
        big_block->reuse();
        BigBlockFactory::free(big_block);
        big_block = NULL;
      } else {
        const int64_t seq = batch_task.get_seq();
        const int64_t next_flush_block_id = seq + 1;
        batch_task.reuse();

        IObLogBatchBuffer *batch_buffer = TCTX.batch_buffer_;
        int tmp_ret = OB_SUCCESS;

        if (OB_ISNULL(batch_buffer)) {
          LOG_ERROR("batch_buffer is NULL");
          ret = OB_ERR_UNEXPECTED;
        } else {
          batch_buffer->update_next_flush_block_id(next_flush_block_id);
          if (OB_SUCCESS != (tmp_ret = batch_buffer->try_freeze(next_flush_block_id))) {
            LOG_ERROR("batch_buffer try_freeze failed", K(tmp_ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObLogStorager::handle_task_batch_write_(IObLogBatchBufTask &batch_task,
    const int64_t thread_index,
    volatile bool &stop_flag,
    const char *batch_buf)
{
  int ret = OB_SUCCESS;
  IObLogBufTask *curr_task = batch_task.get_header_task();
  IObLogBufTask *next_task = NULL;
  TenantBatchMap tenant_batch_map;

  // Initialize hash map
  if (OB_FAIL(tenant_batch_map.create(16, ObModIds::OB_HASH_NODE))) {
    LOG_ERROR("tenant_batch_map create fail", KR(ret));
  } else {
    // First pass: collect all tasks and group by tenant_id
    while (OB_SUCC(ret) && ! stop_flag && NULL != curr_task) {
      next_task = curr_task->next_;
      ObLogStoreTask *store_task = static_cast<ObLogStoreTask *>(curr_task);

      if (OB_ISNULL(store_task)) {
        LOG_ERROR("store_task is NULL", KPC(store_task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObLogStoreKey &store_key = store_task->get_store_key();
        const uint64_t tenant_id = store_key.get_tenant_id();

        // Get or create task array for this tenant
        common::ObArray<ObLogStoreTask *> task_array_value;
        int tmp_ret = tenant_batch_map.get_refactored(tenant_id, task_array_value);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          // Create new task array on stack and set into map (will be copied into map)
          common::ObArray<ObLogStoreTask *> new_task_array;
          if (OB_FAIL(tenant_batch_map.set_refactored(tenant_id, new_task_array))) {
            LOG_ERROR("tenant_batch_map set_refactored fail", KR(ret), K(tenant_id));
          }
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_ERROR("tenant_batch_map get_refactored fail", KR(ret), K(tenant_id));
        }

        if (OB_SUCC(ret)) {
          // Get pointer to the value in map for modification
          common::ObArray<ObLogStoreTask *> *task_array = tenant_batch_map.get(tenant_id);
          if (OB_ISNULL(task_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("task_array is NULL", KR(ret), K(tenant_id));
          } else {
            // Add task to tenant batch (directly modify the value in map)
            if (OB_FAIL(task_array->push_back(store_task))) {
              LOG_ERROR("task_array push_back fail", KR(ret), K(tenant_id));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        curr_task = next_task;
      }
    } // while

    // Execute batch write for each tenant
    if (OB_SUCC(ret)) {
      for (TenantBatchMap::iterator it = tenant_batch_map.begin();
          OB_SUCC(ret) && it != tenant_batch_map.end(); ++it) {
        uint64_t tenant_id = it->first;
        common::ObArray<ObLogStoreTask *> &task_array = it->second;

        if (!task_array.empty()) {
          // Get tenant guard and cf_handle
          ObLogTenantGuard guard;
          ObLogTenant *tenant = NULL;
          void *column_family_handle = NULL;

          if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
            LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
          } else {
            tenant = guard.get_tenant();
            column_family_handle = tenant->get_redo_storage_cf_handle();

            if (OB_ISNULL(column_family_handle)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("column_family_handle is NULL", KR(ret), K(tenant_id));
            } else {
              // Prepare keys and values from tasks
              common::ObArray<ObLogStoreKey> batch_keys;
              common::ObArray<ObSlice> batch_values;

              for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
                ObLogStoreTask *task = task_array.at(i);
                if (OB_ISNULL(task)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_ERROR("task is NULL", KR(ret), K(tenant_id), K(i));
                } else {
                  ObLogStoreKey &store_key = task->get_store_key();
                  const offset_t start_pos = task->get_offset();
                  const int64_t data_len = task->get_data_len();

                  if (OB_FAIL(batch_keys.push_back(store_key))) {
                    LOG_ERROR("batch_keys push_back fail", KR(ret), K(tenant_id), K(i));
                  } else if (OB_FAIL(batch_values.push_back(ObSlice(batch_buf + start_pos, data_len)))) {
                    LOG_ERROR("batch_values push_back fail", KR(ret), K(tenant_id), K(i));
                  }
                }
              }

              // Execute batch write
              if (OB_SUCC(ret) && !batch_keys.empty()) {
                if (OB_FAIL(batch_write_store_service_(column_family_handle,
                        batch_keys, batch_values, thread_index))) {
                  if (OB_IN_STOP_STATE != ret) {
                    LOG_ERROR("batch_write_store_service_ fail", KR(ret), K(tenant_id), "key_count", batch_keys.count());
                  }
                  // NOTE: tasks in task_array are not cleaned up here on failure.
                  // The caller (handle_task_) will invoke err_handler_ which triggers process exit,
                  // so the leak is acceptable and will be reclaimed on process termination.
                } else {
                  // Process all tasks after successful batch write
                  for (int64_t i = 0; i < task_array.count(); ++i) {
                    ObLogStoreTask *task = task_array.at(i);
                    if (NULL != task) {
                      int tmp_ret = task->st_after_consume(OB_SUCCESS);
                      if (OB_SUCCESS != tmp_ret) {
                        LOG_ERROR("st_after_consume fail", KR(tmp_ret), KPC(task));
                      }
                      ObLogStoreTaskFactory::free(task);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    // Destroy hash map
    tenant_batch_map.destroy();
  }

  return ret;
}

int ObLogStorager::handle_task_single_write_(IObLogBatchBufTask &batch_task,
    const int64_t thread_index,
    volatile bool &stop_flag,
    const char *batch_buf)
{
  int ret = OB_SUCCESS;
  IObLogBufTask *curr_task = batch_task.get_header_task();
  IObLogBufTask *next_task = NULL;

  // Write tasks one by one
  while (OB_SUCC(ret) && ! stop_flag && NULL != curr_task) {
    next_task = curr_task->next_;
    ObLogStoreTask *store_task = static_cast<ObLogStoreTask *>(curr_task);

    if (OB_ISNULL(store_task)) {
      LOG_ERROR("store_task is NULL", KPC(store_task));
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObLogStoreKey &store_key = store_task->get_store_key();
      const uint64_t tenant_id = store_key.get_tenant_id();
      ObLogTenantGuard guard;
      ObLogTenant *tenant = NULL;
      std::string key;

      if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
        LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
      } else {
        tenant = guard.get_tenant();
        void *column_family_handle = tenant->get_redo_storage_cf_handle();

        if (OB_FAIL(store_key.get_key(key))) {
          LOG_ERROR("store_key get_key fail", KR(ret));
        } else {
          const offset_t start_pos = store_task->get_offset();
          const int64_t data_len = store_task->get_data_len();

          if (OB_FAIL(write_store_service_(key.c_str(), batch_buf + start_pos, data_len,
                  column_family_handle, thread_index))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("write_store_service_ fail", KR(ret), K(store_task));
            }
          } else if (OB_FAIL(store_task->st_after_consume(OB_SUCCESS))) {
            LOG_ERROR("st_after_consume fail", KR(ret));
          } else {
            ObLogStoreTaskFactory::free(store_task);
            store_task = NULL;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      curr_task = next_task;
    }
  } // while

  return ret;
}

int ObLogStorager::write_store_service_(const char *key,
    const char *log_str,
    const size_t log_str_len,
    void *column_family_handle,
    const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key) || OB_ISNULL(log_str) || OB_UNLIKELY(log_str_len <= 0)) {
    LOG_ERROR("invalid argument");
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(store_service_->put(column_family_handle, key, ObSlice(log_str, log_str_len)))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("store_service_ put fail", KR(ret), K(thread_index), K(key), K(log_str_len));
      }
    } else {
      // Statistics rps
      rps_stat_.do_rps_stat(1);
      store_service_stat_.do_data_stat(log_str_len);

      LOG_DEBUG("store_service_ put succ", K(key), K(log_str_len));
    }
  }

  return ret;
}

int ObLogStorager::batch_write_store_service_(void *column_family_handle,
    const common::ObArray<ObLogStoreKey> &keys,
    const common::ObArray<ObSlice> &values,
    const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogStorager has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(column_family_handle) || keys.empty() || values.empty()) {
    LOG_ERROR("invalid argument", K(column_family_handle), "key_count", keys.count(), "value_count", values.count());
    ret = OB_INVALID_ARGUMENT;
  } else if (keys.count() != values.count()) {
    LOG_ERROR("keys and values size mismatch", "key_count", keys.count(), "value_count", values.count());
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(store_service_->batch_write(column_family_handle, keys, values))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("store_service_ batch_write fail", KR(ret), K(thread_index), "key_count", keys.count());
      }
    } else {
      // Statistics rps and data size
      int64_t total_data_size = 0;
      for (int64_t i = 0; i < values.count(); ++i) {
        total_data_size += values.at(i).buf_len_;
      }
      rps_stat_.do_rps_stat(keys.count());
      store_service_stat_.do_data_stat(total_data_size);

      LOG_TRACE("store_service_ batch_write succ", "key_count", keys.count(), "total_data_size", total_data_size);
    }
  }

  return ret;
}

void ObLogStorager::print_task_count_()
{
  int ret = OB_SUCCESS;
  int64_t total_thread_num = get_thread_num();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < total_thread_num && ! is_stoped(); ++idx) {
    int64_t task_count = 0;
    if (OB_FAIL(get_task_num(idx, task_count))) {
      LOG_ERROR("get_task_num fail", K(ret));
    } else {
      _LOG_INFO("[STAT] [STORAGER] [%ld/%ld] COUNT=%ld", idx, total_thread_num, task_count);
    }
  }
}

void ObLogStorager::print_stat_info()
{
  int64_t current_timestamp = get_timestamp();
  int64_t local_last_stat_time = last_stat_time_;
  int64_t delta_time = current_timestamp - local_last_stat_time;
  // Update last statistic
  last_stat_time_ = current_timestamp;

  // ObIncBackUpUploader
  double storager_rps = rps_stat_.calc_rps(delta_time);
  double write_rate = store_service_stat_.calc_rate(delta_time);
  double write_total_size = store_service_stat_.get_total_data_size();
  _LOG_INFO("[STORAGE] [STAT] RPS=%.3lf WRITE_RATE=%.5fM/s WRITE_TOTAL_SIZE=%.5fG",
      storager_rps, write_rate, write_total_size);

  // Print storager stat info
  print_task_count_();

  // Print disk IO statistics
  disk_io_monitor_.print_stat();

  // rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  // _LOG_INFO("[STORAGE] [STAT] perf=%s", rocksdb::get_perf_context()->ToString().c_str());
  // rocksdb::get_perf_context()->Reset();
  // rocksdb::get_iostats_context()->Reset();
  // rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime); //开启profiling
}

void ObLogStorager::get_rocksdb_stats(std::string& perf_stats, std::string& io_stats) const
{
  perf_stats = "RocksDB perf stats collection not implemented";
  io_stats = "RocksDB IO stats collection not implemented";
}

int ObLogStorager::read_store_service_(const std::string &key)
{
  int ret = OB_SUCCESS;
  std::string br_string_res;

  if (OB_FAIL(store_service_->get(key, br_string_res))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("store_service_ get fail", KR(ret), K(key.c_str()), K(br_string_res.length()));
    }
  } else {
    LOG_DEBUG("store_service_ get succ", KR(ret), K(key.c_str()), K(br_string_res.length()), K(br_string_res.c_str()));
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
