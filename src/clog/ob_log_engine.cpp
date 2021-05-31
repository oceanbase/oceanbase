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

#include <sys/vfs.h>
#include "ob_log_engine.h"
#include "common/ob_member_list.h"
#include "lib/file/file_directory_utils.h"
#include "lib/thread_local/thread_buffer.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_blacklist.h"
#include "share/rpc/ob_batch_rpc.h"
#include "share/redolog/ob_log_store_factory.h"
#include "share/redolog/ob_log_file_reader.h"
#include "storage/ob_partition_service.h"
#include "ob_batch_submit_task.h"
#include "ob_log_flush_task.h"
#include "ob_log_info_block_reader.h"
#include "ob_log_type.h"
#include "ob_log_task.h"
#include "ob_log_rpc_proxy.h"
#include "ob_clog_file_writer.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace clog {

bool ObLogEnv::Config::is_valid() const
{
  return NULL != log_dir_ && NULL != index_log_dir_ && NULL != log_shm_path_ && NULL != index_log_shm_path_ &&
         NULL != cache_name_ && NULL != index_cache_name_ && cache_priority_ > 0 && index_cache_priority_ > 0 &&
         file_size_ > 0 && read_timeout_ > 0 && write_timeout_ > 0 && write_queue_size_ > 0 &&
         disk_log_buffer_cnt_ > 0 && disk_log_buffer_size_ > 0 && ethernet_speed_ > 0;
}

void ObLogEnv::Config::reset()
{
  log_shm_path_ = NULL;
  index_log_shm_path_ = NULL;
  index_cache_name_ = NULL;
  cache_priority_ = 0;
  index_cache_priority_ = 0;
  file_size_ = 0;
  read_timeout_ = 0;
  write_timeout_ = 0;
  write_queue_size_ = 0;
  disk_log_buffer_cnt_ = 0;
  disk_log_buffer_size_ = 0;
  ethernet_speed_ = 0;
}

ObLogEnv::~ObLogEnv()
{
  if (is_inited_) {
    destroy();
    is_inited_ = false;
  }
}

int ObLogEnv::init(const Config& cfg, const ObAddr& self_addr, ObIInfoBlockHandler* info_block_handler,
    ObLogTailLocator* tail_locator, ObICallbackHandler* callback_handler,
    storage::ObPartitionService* partition_service, const char* cache_name, const int64_t hot_cache_size,
    const file_id_t start_file_id, const int64_t disk_buffer_size, const ObLogWritePoolType write_pool_type,
    const bool enable_log_cache)
{
  int ret = OB_SUCCESS;
  ObReadParam param;
  param.timeout_ = cfg.read_timeout_;
  const bool use_log_cache = true;
  if (NULL == info_block_handler || NULL == tail_locator || NULL == callback_handler || NULL == partition_service ||
      NULL == cache_name || !is_valid_file_id(start_file_id) || 0 > disk_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        KP(info_block_handler),
        KP(tail_locator),
        KP(callback_handler),
        KP(partition_service),
        KP(cache_name),
        K(start_file_id),
        K(disk_buffer_size));
  } else if (OB_FAIL(log_cache_.init(self_addr, cache_name, cfg.cache_priority_, hot_cache_size))) {
    CLOG_LOG(WARN, "log cache init error", K(ret), K(cache_name), K(hot_cache_size));
  } else if (NULL == (file_store_ = ObLogStoreFactory::create(cfg.log_dir_, cfg.file_size_, write_pool_type))) {
    ret = OB_INIT_FAIL;
    CLOG_LOG(WARN, "create file store failed.", K(ret));
  } else if (OB_FAIL(direct_reader_.init(
                 cfg.log_dir_, cfg.log_shm_path_, use_log_cache, &log_cache_, &log_tail_, write_pool_type))) {
    CLOG_LOG(WARN, "direct reader init error", K(ret), K(enable_log_cache), K(write_pool_type));
  } else if (OB_FAIL(init_log_file_writer(cfg.log_dir_, cfg.log_shm_path_, file_store_))) {
    CLOG_LOG(WARN, "Fail to init log file writer ", K(ret));
  } else {
    // do nothing
  }
  if (OB_SUCCESS == ret) {
    ObCLogWriterCfg log_cfg;
    log_cfg.log_file_writer_ = log_file_writer_;
    log_cfg.info_getter_ = info_block_handler;
    log_cfg.log_cache_ = &log_cache_;
    log_cfg.tail_ptr_ = &log_tail_;
    log_cfg.type_ = write_pool_type;
    log_cfg.use_cache_ = enable_log_cache;
    log_cfg.base_cfg_.max_buffer_item_cnt_ = DEFAULT_WRITER_MAX_BUFFER_ITEM_CNT;
    log_cfg.base_cfg_.group_commit_max_item_cnt_ = 1;
    log_cfg.base_cfg_.group_commit_min_item_cnt_ = 1;
    log_cfg.base_cfg_.group_commit_max_wait_us_ = 1000;

    if (OB_FAIL(log_writer_.init(log_cfg))) {
      CLOG_LOG(WARN, "log writer init error", K(ret));
    } else if (OB_FAIL(
                   disk_log_buffer_.init(disk_buffer_size, cfg.disk_log_buffer_cnt_, &log_writer_, callback_handler))) {
      CLOG_LOG(WARN, "disk log buffer init error", K(ret));
    } else {
      config_ = cfg;
      partition_service_ = partition_service;
      min_start_file_id_ = start_file_id;
      tail_locator_ = tail_locator;
      is_inited_ = true;
      CLOG_LOG(INFO, "log engine init clog env success", K(cfg));
    }
  }
  return ret;
}

int ObLogEnv::start(file_id_t& start_file_id, offset_t& offset)
{
  int ret = OB_SUCCESS;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!is_valid_file_id(start_file_id) || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(start_file_id), K(offset));
  } else {
    if (OB_FAIL(log_writer_.start(start_file_id, offset))) {
      CLOG_LOG(ERROR, "log writer start failed", K(ret));
    } else if (OB_FAIL(file_store_->get_file_id_range(min_file_id, max_file_id))) {
      CLOG_LOG(WARN, "get file id range error", K(ret));
    } else {
      log_file_writer_->update_min_using_file_id(min_file_id);
      log_file_writer_->update_min_file_id(min_file_id);
    }
  }
  if (OB_SUCCESS == ret) {
    log_tail_.init(start_file_id, offset);
    if (OB_FAIL(warm_up_cache(direct_reader_, log_tail_, log_cache_))) {
      CLOG_LOG(WARN, "warm up cache error", K(ret));
    } else {
      CLOG_LOG(INFO, "log env start success", K(min_file_id), K(start_file_id));
    }
  }
  return ret;
}

int ObLogEnv::start()
{
  int ret = OB_SUCCESS;
  file_id_t file_id = OB_INVALID_FILE_ID;
  offset_t offset = 0;
  ObReadParam param;
  param.timeout_ = config_.read_timeout_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(tail_locator_(param.timeout_, file_store_, &direct_reader_, file_id, offset))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // rewrite ret
      ret = OB_SUCCESS;
      file_id = min_start_file_id_;
      offset = 0;
    } else {
      CLOG_LOG(WARN, "clog file locator init error", K(ret), K(file_id), K(offset));
    }
  }

  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "clog file tail locator", K(file_id), K(offset));
    if (OB_FAIL(start(file_id, offset))) {
      CLOG_LOG(WARN, "start from tail locator fail", K(ret), K(file_id), K(offset));
    } else {
      CLOG_LOG(INFO, "start from tail locator", K(file_id), K(offset));
    }
  }
  return ret;
}

int ObLogEnv::set_min_using_file_id(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_FILE_ID != file_id) {
    log_file_writer_->update_min_using_file_id(file_id);
  }
  return ret;
}

void ObLogEnv::stop()
{
  log_writer_.set_stoped();
}

void ObLogEnv::wait()
{
  log_writer_.wait();
}

void ObLogEnv::destroy()
{
  if (OB_NOT_NULL(log_file_writer_)) {
    OB_DELETE(ObCLogBaseFileWriter, ObModIds::OB_LOG_WRITER, log_file_writer_);
    log_file_writer_ = NULL;
  }
  ObLogStoreFactory::destroy(file_store_);
  log_cache_.destroy();
  direct_reader_.destroy();
  log_writer_.destroy();
}

int ObLogEnv::warm_up_cache(ObLogDirectReader& reader, ObTailCursor& tail, ObLogCache& cache)
{
  return ObHotCacheWarmUpHelper::warm_up(reader, tail, cache);
}

int64_t ObLogEnv::get_free_quota() const
{
  int ret = OB_SUCCESS;
  int64_t free_quota = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else {
    free_quota = log_file_writer_->get_free_quota();
  }
  return free_quota;
}

int ObLogEnv::check_is_clog_writer_congested(bool& is_congested) const
{
  int ret = OB_SUCCESS;
  is_congested = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else {
    const int64_t queued_item_cnt = log_writer_.get_queued_item_cnt();
    const int64_t log_buffer_cnt = disk_log_buffer_.get_log_buffer_cnt();
    is_congested =
        (queued_item_cnt > 0) && (queued_item_cnt >= (log_buffer_cnt * BUFFER_ITEM_CONGESTED_PERCENTAGE / 100));
  }
  return ret;
}

int ObLogEnv::update_free_quota()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(log_file_writer_->update_free_quota())) {
    CLOG_LOG(WARN, "update free quota failed.", K(ret));
  }
  return ret;
}

uint32_t ObLogEnv::get_min_using_file_id() const
{
  int ret = OB_SUCCESS;
  uint32_t min_using_file_id = OB_INVALID_FILE_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else {
    min_using_file_id = log_file_writer_->get_min_using_file_id();
  }
  return min_using_file_id;
}

uint32_t ObLogEnv::get_min_file_id() const
{
  int ret = OB_SUCCESS;
  uint32_t min_file_id = OB_INVALID_FILE_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else {
    min_file_id = log_file_writer_->get_min_file_id();
  }
  return min_file_id;
}

uint32_t ObLogEnv::get_max_file_id() const
{
  int ret = OB_SUCCESS;
  uint32_t cur_file_id = OB_INVALID_FILE_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else {
    cur_file_id = log_file_writer_->get_cur_file_id();
  }
  return cur_file_id;
}

void ObLogEnv::try_recycle_file()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else {
    log_file_writer_->try_recycle_file();
  }
}

int ObLogEnv::get_total_disk_space(int64_t& total_space) const
{
  return file_store_->get_total_disk_space(total_space);
}

bool ObLogEnv::cluster_version_before_2000_() const
{
  return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000;
}

int ObLogEnv::init_log_file_writer(const char* log_dir, const char* shm_path, const ObILogFileStore* file_store)
{
  int ret = OB_SUCCESS;
  if (nullptr ==
      (log_file_writer_ = static_cast<ObCLogBaseFileWriter*>(OB_NEW(ObCLogLocalFileWriter, ObModIds::OB_LOG_WRITER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc file writer failed, ", K(ret));
  } else if (OB_FAIL(log_file_writer_->init(log_dir, shm_path, CLOG_DIO_ALIGN_SIZE, file_store))) {
    CLOG_LOG(WARN, "Fail to init file writer, ", K(ret));
  }

  return ret;
}

int ObCommitLogEnv::init(const ObLogEnv::Config& cfg, const ObAddr& self_addr, ObICallbackHandler* callback_handler,
    storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  const int64_t hot_cache_size = get_hot_cache_size(true);
  const file_id_t start_file_id = 1;
  const int64_t disk_buffer_size = cfg.disk_log_buffer_size_;
  const ObLogWritePoolType write_pool_type = CLOG_WRITE_POOL;
  const bool enable_log_cache = ENABLE_CLOG_CACHE;
  const char* cache_name = cfg.cache_name_;
  if (OB_FAIL(info_block_handler_.init())) {
    CLOG_LOG(WARN, "info block handler init error", K(ret));
  } else if (OB_FAIL(partition_hash_map_.init(ObModIds::OB_CLOG_MGR))) {
    CLOG_LOG(WARN, "partition_hash_map_ init failed", K(ret));
  } else {
    preread_file_id_ = 0;
    ret = ObLogEnv::init(cfg,
        self_addr,
        &info_block_handler_,
        locate_clog_tail,
        callback_handler,
        partition_service,
        cache_name,
        hot_cache_size,
        start_file_id,
        disk_buffer_size,
        write_pool_type,
        enable_log_cache);
  }
  return ret;
}

void ObCommitLogEnv::destroy()
{
  partition_hash_map_.destroy();
  info_block_handler_.destroy();
  preread_file_id_ = 0;
  for (int64_t i = 0; i < PREREAD_FILE_NUM; i++) {
    freeze_array_[i].destroy();
  }
  ObLogEnv::destroy();
}

int ObCommitLogEnv::update_min_using_file_id(
    NeedFreezePartitionArray& partition_array, const bool need_freeze_based_on_used_space)
{
  int ret = OB_SUCCESS;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  file_id_t min_using_file_id = OB_INVALID_FILE_ID;
  ObILogInfoBlockReader* reader = NULL;
  NeedFreezePartitionArray tmp_partition_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(file_store_->get_file_id_range(min_file_id, max_file_id))) {
    CLOG_LOG(WARN, "get log file id range failed", K(ret));
  } else if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid file id", K(ret), K(min_file_id), K(max_file_id));
  } else {
    min_using_file_id = log_file_writer_->get_min_using_file_id();
    if (min_using_file_id > max_file_id) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "min_using_file_id is too large", K(ret), K(min_file_id), K(max_file_id), K(min_using_file_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_log_info_block_reader(reader))) {
      CLOG_LOG(WARN, "get info block reader failed", K(ret));
    } else {
      ObReadParam param;
      param.timeout_ = config_.read_timeout_;
      ObReadRes res;
      file_id_t file_id = min_using_file_id;
      if (min_using_file_id < min_file_id) {
        CLOG_LOG(WARN, "min_using_file_id is too small", K(min_using_file_id), K(min_file_id));
        file_id = min_file_id;
      }
      // ignore the last file
      bool can_skip = true;
      for (; OB_SUCC(ret) && can_skip && file_id < max_file_id; file_id++) {
        param.file_id_ = file_id;
        if (OB_FAIL(reader->read_info_block_data(param, res))) {
          CLOG_LOG(WARN, "read info block data failed", K(ret), K(param), K(file_id), K(max_file_id));
        } else {
          const bool need_record =
              (((file_id == min_file_id) && (log_file_writer_->free_quota_warn())) || need_freeze_based_on_used_space);
          int64_t pos = 0;

          ObCommitInfoBlockHandler handler;
          int64_t max_decided_trans_version = OB_INVALID_TIMESTAMP;
          int64_t max_submit_timestamp = OB_INVALID_TIMESTAMP;
          can_skip = false;
          if (OB_FAIL(handler.init())) {
            CLOG_LOG(WARN, "ObCommitInfoBlockHandler init failed", K(ret));
          } else if (OB_FAIL(handler.resolve_info_block(res.buf_, res.data_len_, pos))) {
            CLOG_LOG(WARN, "resolve log info block failed", K(ret), K(file_id));
          } else if (OB_FAIL(partition_service_->get_global_max_decided_trans_version(max_decided_trans_version))) {
            CLOG_LOG(WARN, "get_global_max_decided_trans_version failed", K(ret), K(file_id));
          } else if (OB_FAIL(handler.get_max_submit_timestamp(max_submit_timestamp))) {
            CLOG_LOG(WARN, "get_max_submit_timestamp failed", K(ret));
          } else {
            // 1. Determine whether the clog file can be recycled according to the timestamp of the max decided
            // transaction
            if (!cluster_version_before_2000_() && OB_INVALID_TIMESTAMP != max_submit_timestamp &&
                GCONF.enable_one_phase_commit) {
              if (OB_FAIL(handler.can_skip_based_on_submit_timestamp(max_decided_trans_version, can_skip))) {
                CLOG_LOG(ERROR, "can_skip_based_on_submit_timestamp failed", K(ret), K(max_decided_trans_version));
                break;
              }
              if (!can_skip) {
                CLOG_LOG(INFO,
                    "cannot recycle because global_max_decided_trans_version",
                    K(ret),
                    K(file_id),
                    K(min_file_id));
                break;
              } else {
                // The commit timestamp of the max decided transaction has been pushed past the max_log_timestamp
                // corresponding to the clog file, but the clog file cannot be recycled directly according to this
                // condition
              }
            }

            // 2. Determine whether the clog file can be recycled according to the clog_info of the partition,
            // and here can determine whether the clog file can be recycled
            {
              can_skip = false;
              if (OB_FAIL(handler.can_skip_based_on_log_id(
                      partition_service_, tmp_partition_array, need_record, can_skip))) {
                CLOG_LOG(WARN, "can_skip_based_on_log_id failed", K(ret));
                break;
              }
              if (!can_skip) {
                break;
              }
            };
          }
        }
        if (OB_SUCC(ret)) {
          log_file_writer_->update_min_using_file_id(file_id);
        }
      }
      if (OB_SUCCESS == ret) {
        log_file_writer_->update_min_file_id(min_file_id);
        log_file_writer_->update_min_using_file_id(file_id);
        CLOG_LOG(INFO, "clog update min using file id success", K(min_file_id), K(max_file_id), K(file_id));

        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(append_freeze_array_(partition_array, tmp_partition_array))) {
          CLOG_LOG(WARN, "append_freeze_array_ failed", K(ret), K(partition_array), K(tmp_partition_array));
        } else if (need_freeze_based_on_used_space &&
                   OB_SUCCESS != (tmp_ret = preread_freeze_array_(file_id, max_file_id, reader, partition_array))) {
          CLOG_LOG(WARN, "preread_freeze_array_ failed", K(tmp_ret), K(file_id));
        }
        partition_hash_map_.reset();
      }
      revert_log_info_block_reader(reader);
      reader = NULL;
    }
  }
  return ret;
}

int ObCommitLogEnv::preread_freeze_array_(const file_id_t file_id, const file_id_t max_file_id,
    ObILogInfoBlockReader* reader, NeedFreezePartitionArray& partition_array)
{
  int ret = OB_SUCCESS;
  ObReadParam param;
  param.timeout_ = config_.read_timeout_;
  ObReadRes res;
  for (file_id_t tmp_file_id = file_id + 1;
       OB_SUCC(ret) && tmp_file_id < max_file_id && tmp_file_id < file_id + PREREAD_FILE_NUM;
       tmp_file_id++) {
    NeedFreezePartitionArray tmp_partition_array;
    if (tmp_file_id > preread_file_id_) {
      param.file_id_ = tmp_file_id;
      if (OB_FAIL(reader->read_info_block_data(param, res))) {
        CLOG_LOG(WARN, "read info block data failed", K(ret), K(param), K(tmp_file_id));
      } else {
        int64_t pos = 0;
        ObCommitInfoBlockHandler handler;
        if (OB_FAIL(handler.init())) {
          CLOG_LOG(WARN, "ObCommitInfoBlockHandler init failed", K(ret));
        } else if (OB_FAIL(handler.resolve_info_block(res.buf_, res.data_len_, pos))) {
          CLOG_LOG(WARN, "resolve log info block failed", K(ret), K(tmp_file_id));
        } else {
          bool can_skip = false;
          if (OB_FAIL(handler.can_skip_based_on_log_id(partition_service_, tmp_partition_array, true, can_skip))) {
            CLOG_LOG(ERROR, "can_skip_based_on_log_id_failed", K(ret));
            break;
          } else if (OB_FAIL(freeze_array_[tmp_file_id % PREREAD_FILE_NUM].assign(tmp_partition_array))) {
            CLOG_LOG(WARN, "freeze_array assign failed", K(ret), K(tmp_file_id), K(tmp_partition_array));
          } else {
            preread_file_id_ = tmp_file_id;
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(append_freeze_array_(partition_array, freeze_array_[tmp_file_id % PREREAD_FILE_NUM]))) {
      CLOG_LOG(WARN, "append_freeze_array_ failed", K(ret), K(partition_array));
    }
  }
  return ret;
}

int ObCommitLogEnv::append_freeze_array_(
    NeedFreezePartitionArray& partition_array, const NeedFreezePartitionArray& tmp_partition_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_partition_array.count(); i++) {
    const NeedFreezePartition& partition = tmp_partition_array[i];
    bool val = false;
    if (OB_FAIL(partition_hash_map_.get(partition.get_partition_key(), val)) && OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "partition_hash_map_ get failed", K(ret), K(partition));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(partition_array.push_back(partition))) {
        CLOG_LOG(WARN, "partition_array push_back failed", K(ret), K(partition));
      } else if (OB_FAIL(partition_hash_map_.insert(partition.get_partition_key(), true))) {
        CLOG_LOG(WARN, "partition_hash_map_ insert failed", K(ret), K(partition));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObCommitLogEnv::get_log_info_block_reader(ObILogInfoBlockReader*& reader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObLogInfoBlockReader* tmp_reader = nullptr;
    if (OB_ISNULL(tmp_reader = OB_NEW(ObLogInfoBlockReader, common::ObModIds::OB_CLOG_MGR))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (OB_FAIL(tmp_reader->init(&direct_reader_))) {
        revert_log_info_block_reader(tmp_reader);
        tmp_reader = nullptr;
      } else {
        reader = tmp_reader;
      }
    }
  }
  return ret;
}

void ObCommitLogEnv::revert_log_info_block_reader(ObILogInfoBlockReader* reader)
{
  if (NULL != reader) {
    reader->~ObILogInfoBlockReader();
    ob_free(reader);
    reader = NULL;
  }
}

int ObCommitLogEnv::get_using_disk_space(int64_t& using_space) const
{
  int ret = OB_SUCCESS;
  const uint32_t min_using_file_id = get_min_using_file_id();
  const uint32_t max_file_id = get_max_file_id();
  using_space = (max_file_id - min_using_file_id + 1) * CLOG_FILE_SIZE;
  return ret;
}

int ObCommitLogEnv::get_min_file_mtime(time_t& min_clog_mtime)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObCommitLogEnv is not inited", K(ret));
  } else {
    while (OB_EAGAIN == (ret = get_min_file_mtime_(min_clog_mtime))) {
      usleep(1000);
    }
  }

  return ret;
}

int ObCommitLogEnv::get_min_file_mtime_(time_t& min_clog_mtime)
{
  int ret = OB_SUCCESS;

  file_id_t file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (OB_FAIL(file_store_->get_file_id_range(file_id, max_file_id)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(ERROR, "get_min_file_id failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // do nothing
  } else if (OB_FAIL(file_store_->get_file_st_time(file_id, min_clog_mtime))) {
    if (OB_FILE_NOT_EXIST == ret) {  // file could be reused
      ret = OB_EAGAIN;
    } else {
      CLOG_LOG(ERROR, "get_file_st_time failed", K(ret));
    }
  }

  return ret;
}

int ObLogEngine::start()
{
  int ret = OB_SUCCESS;
  int err = 0;
  if (OB_FAIL(clog_env_.start())) {
    CLOG_LOG(WARN, "clog env start failed", K(ret));
  } else {
    CLOG_LOG(INFO, "old log env start success");
  }
  if (OB_SUCCESS == ret) {
    if (OB_FAIL(ObThreadPool::start())) {
      CLOG_LOG(ERROR, "log engine thread failed to start", K(ret), K(err));
    }
  }
  if (OB_SUCCESS == ret) {
    if (OB_FAIL(ilog_storage_.start())) {
      CLOG_LOG(WARN, "ilog_storage_ failed to start", K(ret));
    }
  }
  if (OB_SUCCESS == ret) {
    CLOG_LOG(INFO, "log engine start success");
  } else {
    CLOG_LOG(ERROR, "log engine start failed", K(ret));
  }
  return ret;
}

void ObLogEngine::stop()
{
  clog_env_.stop();
  ObThreadPool::stop();
  ilog_storage_.stop();
  CLOG_LOG(INFO, "log engine stop success");
}

void ObLogEngine::wait()
{
  clog_env_.wait();
  ilog_storage_.wait();
  CLOG_LOG(INFO, "log engine wait success");
}

void ObLogEngine::destroy()
{
  if (is_inited_) {
    clog_env_.destroy();
    ilog_storage_.destroy();
    ilog_log_cache_.destroy();
    OB_LOG_FILE_READER.destroy();
    batch_rpc_ = NULL;
    rpc_ = NULL;
    is_inited_ = false;
    CLOG_LOG(INFO, "log engine destroyed");
  }
}

void ObLogEngine::run1()
{
  lib::set_thread_name("LogEngine");
  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    ObTimeGuard timeguard("log engine task");
    // Task 2. update free quota
    if (OB_FAIL(update_free_quota())) {
      CLOG_LOG(WARN, "update free quota failed", K(ret));
    }
    timeguard.click();
    // rewrite ret
    ret = OB_SUCCESS;
    // Task 3. try recycle file
    try_recycle_file();
    timeguard.click();
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      CLOG_LOG(INFO, "log engine run task finish", K(timeguard));
    }
    const int64_t used = timeguard.get_diff();
    if (PREPARE_CACHE_FILE_INTERVAL > used) {
      usleep((int)(PREPARE_CACHE_FILE_INTERVAL - used));
    }
  }
}

int ObLogEngine::init(const ObLogEnv::Config& cfg, const ObAddr& self_addr, obrpc::ObBatchRpc* batch_rpc,
    obrpc::ObLogRpcProxy* rpc, ObICallbackHandler* cb_handler, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  const int64_t ilog_hot_cache_size = get_hot_cache_size(false /*is_clog*/);
  // TODO: local value is 0 by default, to be updated later
  const int64_t server_seq = 0;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!cfg.is_valid() || !self_addr.is_valid() || NULL == rpc || NULL == cb_handler ||
             NULL == partition_service) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(cfg), K(self_addr), KP(rpc), KP(cb_handler));
  } else if (OB_FAIL(OB_LOG_FILE_READER.init())) {
    CLOG_LOG(WARN, "init log file reader failed.", K(ret));
  } else if (OB_FAIL(clog_env_.init(cfg, self_addr, cb_handler, partition_service))) {
    CLOG_LOG(WARN, "init old clog env failed", K(ret));
  } else if (OB_FAIL(network_limit_manager_.init(cfg.ethernet_speed_))) {
    CLOG_LOG(WARN, "network_limit_manager_ init failed", K(ret));
  } else if (OB_FAIL(ilog_log_cache_.init(
                 self_addr, cfg.index_cache_name_, cfg.index_cache_priority_, ilog_hot_cache_size))) {
    CLOG_LOG(WARN, "failed to init ilog_log_cache", K(ret));
  } else if (OB_FAIL(ilog_storage_.init(cfg.index_log_dir_,
                 cfg.index_log_shm_path_,
                 server_seq,
                 self_addr,
                 &ilog_log_cache_,
                 partition_service,
                 &clog_env_))) {
    CLOG_LOG(WARN, "ilog_storage_ init failed", K(ret));
  } else {
    batch_rpc_ = batch_rpc;
    rpc_ = rpc;
    cfg_ = cfg;
    is_inited_ = true;
  }
  if (OB_SUCC(ret)) {
    CLOG_LOG(INFO, "log engine init success", K(ret), K(cfg));
  } else {
    CLOG_LOG(WARN, "log engine init error", K(ret), K(cfg));
  }

  return ret;
}

ObIRawLogIterator* ObLogEngine::alloc_raw_log_iterator(
    const file_id_t start_file_id, const file_id_t end_file_id, const offset_t offset, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObRawLogIterator* iterator = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == (iterator = OB_NEW(ObRawLogIterator, ObModIds::OB_CLOG_MGR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObCommitLogEnv* log_env = get_clog_env_();
    if (OB_FAIL(iterator->init(&(log_env->get_reader()), start_file_id, offset, end_file_id, timeout))) {
      revert_raw_log_iterator(iterator);
      iterator = NULL;
    }
  }
  return iterator;
}

void ObLogEngine::revert_raw_log_iterator(ObIRawLogIterator* iter)
{
  if (NULL != iter) {
    OB_DELETE(ObIRawLogIterator, ObModIds::OB_CLOG_MGR, iter);
    iter = NULL;
  }
}

int ObLogEngine::read_log_by_location(const ObReadParam& param, ObReadBuf& buf, ObLogEntry& entry)
{
  ObReadCost dummy_cost;
  return read_log_by_location(param, buf, entry, dummy_cost);
}

int ObLogEngine::read_log_by_location(const ObLogTask& log_task, ObReadBuf& buf, ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  ObReadParam read_param;
  read_param.file_id_ = log_task.get_log_cursor().file_id_;
  read_param.offset_ = log_task.get_log_cursor().offset_;
  read_param.read_len_ = log_task.get_log_cursor().size_;
  ObReadCost dummy_cost;
  CLOG_LOG(DEBUG, "read_log_by_location", K(log_task), K(read_param));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(read_log_by_location(read_param, buf, entry, dummy_cost))) {
    CLOG_LOG(ERROR, "read_log_by_location failed", K(ret), K(log_task), K(read_param));
  } else {
    if (entry.get_header().get_data_checksum() != log_task.get_data_checksum()) {
      // checksum fail.
      // fatal error
      ret = OB_CHECKSUM_ERROR;
      CLOG_LOG(ERROR, "read_log_by_location error, checksum error", K(ret), K(entry), K(log_task));
    }
  }
  return ret;
}

int ObLogEngine::read_log_by_location(const ObReadParam& param, ObReadBuf& buf, ObLogEntry& entry, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  const common::ObAddr addr = GCTX.self_addr_;
  const int64_t seq = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid buf", K(ret), K(param), K(buf));
  } else if (OB_FAIL(clog_env_.get_reader().read_log(param, addr, seq, buf, entry, cost))) {
    CLOG_LOG(WARN, "read log fail", K(ret), K(param), K(buf));
  }
  return ret;
}

int ObLogEngine::get_clog_real_length(const ObReadParam& param, int64_t& real_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(param), KR(ret));
  } else {
    const common::ObAddr addr = GCTX.self_addr_;
    const int64_t seq = 0;
    ret = clog_env_.get_reader().get_clog_real_length(addr, seq, param, real_length);
  }
  return ret;
}

int ObLogEngine::read_data_from_hot_cache(
    const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf)
{
  int ret = OB_SUCCESS;
  ObCommitLogEnv* log_env = get_clog_env_();
  const common::ObAddr addr = GCTX.self_addr_;
  // TODO: local value is 0 by default
  const int64_t seq = 0;
  ret = log_env->get_reader().read_data_from_hot_cache(addr, seq, want_file_id, want_offset, want_size, user_buf);
  return ret;
}

// Return the original content of the log entry, if compressed, need to decompress
int ObLogEngine::read_uncompressed_data_from_hot_cache(const common::ObAddr& addr, const int64_t seq,
    const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf,
    const int64_t buf_size, int64_t& origin_data_len)
{
  int ret = OB_SUCCESS;
  ObCommitLogEnv* log_env = get_clog_env_();
  ret = log_env->get_reader().read_uncompressed_data_from_hot_cache(
      addr, seq, want_file_id, want_offset, want_size, user_buf, buf_size, origin_data_len);
  return ret;
}

int ObLogEngine::read_data_direct(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  ObCommitLogEnv* log_env = get_clog_env_();
  ret = log_env->get_reader().read_data_direct(param, rbuf, res, cost);
  return ret;
}

int ObLogEngine::submit_flush_task(FlushTask* task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObCommitLogEnv* log_env = get_clog_env_();
    ret = log_env->get_disk_log_buffer().submit(task);
  }
  return ret;
}

int ObLogEngine::submit_flush_task(ObBatchSubmitDiskTask* task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObCommitLogEnv* log_env = get_clog_env_();
    ret = log_env->get_disk_log_buffer().submit(task);
  }
  return ret;
}

void ObLogEngine::get_dst_list_(const share::ObCascadMemberList& mem_list, share::ObCascadMemberList& dst_list) const
{
  int tmp_ret = OB_SUCCESS;
  ObCascadMember member;
  for (int64_t i = 0; i < mem_list.get_member_number(); i++) {
    if (OB_SUCCESS != (tmp_ret = mem_list.get_member_by_index(i, member))) {
      CLOG_LOG(WARN, "get_server_by_index failed", K(mem_list), K(i));
    } else if (!SVR_BLACK_LIST.is_in_blacklist(member)) {
      if (OB_SUCCESS != (tmp_ret = dst_list.add_member(member))) {
        CLOG_LOG(WARN, "add_server failed", K(member));
      }
    } else {
      // do nothing
    }
  }
}

int ObLogEngine::submit_fetch_log_resp(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const int64_t network_limit, const ObPushLogMode push_mode, ObILogNetTask* task)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObPushLogReq req(task->get_proposal_id(), task->get_data_buffer(), task->get_data_len(), push_mode);
    (void)network_limit_manager_.try_limit(server, task->get_data_len(), network_limit);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_FETCH_LOG_RESP, req);
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(DEBUG, "submit_fetch_log_resp", K(server), K(dst_cluster_id), K(key));
    }
  }
  return ret;
}

int ObLogEngine::submit_push_ms_log_req(
    const common::ObAddr& server, const int64_t dst_cluster_id, const common::ObPartitionKey& key, ObILogNetTask* task)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObPushMsLogReq req(task->get_proposal_id(), task->get_data_buffer(), task->get_data_len());
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_PUSH_RENEW_MS_LOG, req);
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(DEBUG, "submit_push_ms_log", K(server), K(dst_cluster_id), K(key));
    }
  }
  return ret;
}

int ObLogEngine::submit_net_task(const ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
    const ObPushLogMode push_mode, ObILogNetTask* task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObPushLogReq req(task->get_proposal_id(), task->get_data_buffer(), task->get_data_len(), push_mode);
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2100 && !SVR_BLACK_LIST.is_empty()) {
      share::ObCascadMemberList dst_list;
      get_dst_list_(mem_list, dst_list);
      ret = post_packet(dst_list, key, OB_PUSH_LOG, req);
    } else {
      ret = post_packet(mem_list, key, OB_PUSH_LOG, req);
    }
  }
  return ret;
}

static bool is_need_batch(int pcode)
{
  return OB_FETCH_LOG == pcode || OB_BROADCAST_INFO_REQ == pcode || OB_REJECT_MSG == pcode ||
         OB_REPLACE_SICK_CHILD_REQ == pcode || OB_FETCH_LOG_V2 == pcode || OB_FETCH_REGISTER_SERVER_REQ_V2 == pcode ||
         OB_KEEPALIVE_MSG == pcode || OB_LEADER_MAX_LOG_MSG == pcode || OB_FETCH_REGISTER_SERVER_RESP_V2 == pcode ||
         OB_REREGISTER_MSG == pcode || OB_CHECK_REBUILD_REQ == pcode || OB_FAKE_ACK_LOG == pcode ||
         OB_RESTORE_LEADER_TAKEOVER_MSG == pcode || OB_RESTORE_ALIVE_REQ == pcode || OB_RESTORE_ALIVE_RESP == pcode ||
         OB_RENEW_MS_CONFIRMED_INFO_REQ == pcode || OB_RENEW_MS_LOG_ACK == pcode || OB_FAKE_PUSH_LOG == pcode ||
         OB_SYNC_LOG_ARCHIVE_PROGRESS == pcode;
}

template <typename Req>
int ObLogEngine::post_packet(
    const uint64_t tenant_id, const ObAddr& server, const ObPartitionKey& key, ObLogReqType type, Req& req)
{
  int ret = OB_SUCCESS;
  // dst server's cluster is same with self by default
  const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  ret = post_packet(tenant_id, server, cluster_id, key, type, req);
  return ret;
}

template <typename Req>
int ObLogEngine::post_packet(const uint64_t tenant_id, const ObAddr& server, const int64_t cluster_id,
    const ObPartitionKey& key, ObLogReqType type, Req& req)
{
  int ret = OB_SUCCESS;
  rpc_stat_.stat(type);
  int32_t batch_req_type = is_need_batch(type) ? obrpc::CLOG_BATCH_REQ : obrpc::CLOG_BATCH_REQ_NODELAY;
  ret = batch_rpc_->post(tenant_id, server, cluster_id, batch_req_type, type, key, req);
  return ret;
}

template <typename Req>
int ObLogEngine::post_packet(
    const common::ObMemberList& mem_list, const ObPartitionKey& key, ObLogReqType type, Req& req)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  int64_t server_num = mem_list.get_member_number();
  ObAddr server;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < server_num; idx++) {
    if (OB_SUCCESS == (ret = mem_list.get_server_by_index(idx, server))) {
      int post_ret = OB_SUCCESS;
      if (OB_SUCCESS != (post_ret = post_packet(tenant_id, server, key, type, req))) {
        CLOG_LOG(WARN, "post packet failed", K(post_ret), K(server), K(mem_list), K(req));
      }
    }
  }
  return ret;
}

template <typename Req>
int ObLogEngine::post_packet(const ObCascadMemberList& mem_list, const ObPartitionKey& key, ObLogReqType type, Req& req)
{
  int ret = OB_SUCCESS;
  int64_t server_num = mem_list.get_member_number();
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  ObCascadMember member;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < server_num; idx++) {
    if (OB_SUCCESS == (ret = mem_list.get_member_by_index(idx, member))) {
      int post_ret = OB_SUCCESS;
      if (OB_SUCCESS !=
          (post_ret = post_packet(tenant_id, member.get_server(), member.get_cluster_id(), key, type, req))) {
        CLOG_LOG(WARN, "post packet failed", K(post_ret), K(member), K(mem_list), K(req));
      }
    }
  }
  return ret;
}

int ObLogEngine::submit_fake_ack(const common::ObAddr& server, const common::ObPartitionKey& key, const uint64_t log_id,
    const ObProposalID proposal_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid() || OB_IS_INVALID_LOG_ID(log_id) || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key), K(log_id), K(proposal_id));
  } else {
    CLOG_LOG(DEBUG, "-->submit_fake_ack", K(server), K(key), K(log_id), K(proposal_id));
    ObFakeAckReq req(log_id, proposal_id);
    ret = post_packet(tenant_id, server, key, OB_FAKE_ACK_LOG, req);
  }
  return ret;
}

int ObLogEngine::submit_fake_push_log_req(const common::ObMemberList& member_list, const common::ObPartitionKey& key,
    const uint64_t log_id, const ObProposalID proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!member_list.is_valid() || !key.is_valid() || OB_IS_INVALID_LOG_ID(log_id) || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(member_list), K(key), K(log_id), K(proposal_id));
  } else {
    CLOG_LOG(DEBUG, "-->submit_fake_push_log_req", K(member_list), K(key), K(log_id), K(proposal_id));
    ObFakePushLogReq req(log_id, proposal_id);
    ret = post_packet(member_list, key, OB_FAKE_PUSH_LOG, req);
  }
  return ret;
}

int ObLogEngine::submit_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const uint64_t log_id, const ObProposalID proposal_id)
{
  // Only paxos member need send log_ack to leader,
  // and paxos member_list must be in one cluster,
  // so its cluster_id must be same with self
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || (dst_cluster_id != OB_INVALID_CLUSTER_ID && dst_cluster_id != self_cluster_id) ||
             !key.is_valid() || OB_IS_INVALID_LOG_ID(log_id) || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(
        WARN, "invalid argument", K(server), K(key), K(log_id), K(proposal_id), K(self_cluster_id), K(dst_cluster_id));
  } else {
    CLOG_LOG(DEBUG,
        "-->submit_log_ack",
        K(server),
        K(dst_cluster_id),
        K(self_cluster_id),
        K(key),
        K(log_id),
        K(proposal_id));
    ObAckLogReqV2 req(log_id, proposal_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_ACK_LOG_V2, req);
  }
  return ret;
}

int ObLogEngine::standby_query_sync_start_id(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const int64_t send_ts)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || dst_cluster_id == OB_INVALID_CLUSTER_ID || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key), K(dst_cluster_id));
  } else {
    CLOG_LOG(DEBUG, "-->standby_query_sync_start_id", K(server), K(dst_cluster_id), K(key));
    ObStandbyQuerySyncStartIdReq req(send_ts);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_STANDBY_QUERY_SYNC_START_ID, req);
  }
  return ret;
}

int ObLogEngine::submit_sync_start_id_resp(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const int64_t original_send_ts, const uint64_t sync_start_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || dst_cluster_id == OB_INVALID_CLUSTER_ID || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key), K(dst_cluster_id));
  } else {
    CLOG_LOG(DEBUG, "-->submit_sync_start_id_resp", K(server), K(dst_cluster_id), K(key));
    ObStandbyQuerySyncStartIdResp req(original_send_ts, sync_start_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_STANDBY_SYNC_START_ID_RESP, req);
  }
  return ret;
}

int ObLogEngine::submit_standby_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const uint64_t log_id, const ObProposalID proposal_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || dst_cluster_id == OB_INVALID_CLUSTER_ID || !key.is_valid() ||
             OB_IS_INVALID_LOG_ID(log_id) || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key), K(log_id), K(proposal_id), K(dst_cluster_id));
  } else {
    CLOG_LOG(DEBUG, "-->submit_standby_log_ack", K(server), K(dst_cluster_id), K(key), K(log_id), K(proposal_id));
    ObStandbyAckLogReq req(log_id, proposal_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_STANDBY_ACK_LOG, req);
  }
  return ret;
}

int ObLogEngine::submit_renew_ms_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const uint64_t log_id, const int64_t submit_timestamp,
    const ObProposalID ms_proposal_id)
{
  // Only paxos member need send ack to leader,
  // and paxos member_list must be in one cluster,
  // so its cluster_id must be same with self
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || (dst_cluster_id != OB_INVALID_CLUSTER_ID && dst_cluster_id != self_cluster_id) ||
             !key.is_valid() || OB_IS_INVALID_LOG_ID(log_id) || !ms_proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(server),
        K(key),
        K(log_id),
        K(ms_proposal_id),
        K(self_cluster_id),
        K(dst_cluster_id));
  } else {
    CLOG_LOG(DEBUG,
        "-->submit_renew_ms_log_ack",
        K(server),
        K(dst_cluster_id),
        K(self_cluster_id),
        K(key),
        K(log_id),
        K(submit_timestamp),
        K(ms_proposal_id));
    ObRenewMsLogAckReq req(log_id, submit_timestamp, ms_proposal_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_RENEW_MS_LOG_ACK, req);
  }
  return ret;
}

int ObLogEngine::fetch_log_from_all_follower(const common::ObMemberList& mem_list, const ObPartitionKey& key,
    const uint64_t start_id, const uint64_t end_id, const ObProposalID proposal_id, const uint64_t max_confirmed_log_id)
{
  int ret = OB_SUCCESS;
  int64_t network_limit = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || OB_IS_INVALID_LOG_ID(start_id) || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(key), K(start_id), K(proposal_id));
  } else if (mem_list.get_member_number() == 0) {
    // single replica situation, do nothing
  } else if (OB_FAIL(network_limit_manager_.get_limit(mem_list, network_limit))) {
    CLOG_LOG(ERROR, "network_limit_manager_ get_limit failed", K(ret), K(mem_list));
  } else {
    common::ObReplicaType replica_type = REPLICA_TYPE_FULL;
    ObFetchLogReqV2 req(start_id,
        end_id,
        OB_FETCH_LOG_LEADER_RECONFIRM,
        proposal_id,
        replica_type,
        network_limit,
        max_confirmed_log_id);
    ret = post_packet(mem_list, key, OB_FETCH_LOG_V2, req);
  }
  return ret;
}

int ObLogEngine::fetch_log_from_leader(const common::ObAddr& server, const int64_t dst_cluster_id,
    const ObPartitionKey& key, const ObFetchLogType fetch_type, const uint64_t start_id, const uint64_t end_id,
    const ObProposalID proposal_id, const common::ObReplicaType replica_type, const uint64_t max_confirmed_log_id)
{
  int ret = OB_SUCCESS;
  int64_t network_limit = 0;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid() || OB_IS_INVALID_LOG_ID(start_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(server), K(key), K(start_id), K(proposal_id));
  } else if (OB_FAIL(network_limit_manager_.get_limit(server, network_limit))) {
    CLOG_LOG(ERROR, "network_limit_manager_ get_limit failed", K(ret), K(server));
  } else {
    ObFetchLogReqV2 req(start_id, end_id, fetch_type, proposal_id, replica_type, network_limit, max_confirmed_log_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_FETCH_LOG_V2, req);
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(DEBUG,
          "fetch_log_from_leader",
          K(server),
          K(dst_cluster_id),
          K(key),
          K(start_id),
          K(end_id),
          K(proposal_id),
          K(max_confirmed_log_id));
    }
  }
  return ret;
}

int ObLogEngine::submit_check_rebuild_req(
    const common::ObAddr& server, const int64_t dst_cluster_id, const ObPartitionKey& key, const uint64_t start_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid() || OB_IS_INVALID_LOG_ID(start_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(server), K(key), K(start_id));
  } else {
    ObCheckRebuildReq req(start_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_CHECK_REBUILD_REQ, req);
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(DEBUG, "submit_check_rebuild_req", K(server), K(dst_cluster_id), K(key), K(start_id));
    }
  }
  return ret;
}

int ObLogEngine::fetch_register_server(const common::ObAddr& server, const int64_t dst_cluster_id,
    const ObPartitionKey& key, const common::ObRegion& region, const common::ObIDC& idc,
    const common::ObReplicaType replica_type, const int64_t next_replay_log_ts, const bool is_request_leader,
    const bool is_need_force_register)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(server),
        K(key),
        K(dst_cluster_id),
        K(region),
        K(replica_type),
        K(next_replay_log_ts),
        K(is_request_leader));
  } else {
    ObFetchRegisterServerReqV2 req(
        replica_type, next_replay_log_ts, is_request_leader, is_need_force_register, region, idc);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_FETCH_REGISTER_SERVER_REQ_V2, req);
    CLOG_LOG(DEBUG,
        "fetch_register_server",
        K(server),
        K(key),
        K(dst_cluster_id),
        K(region),
        K(replica_type),
        K(next_replay_log_ts),
        K(is_request_leader),
        K(lbt()));
  }
  return ret;
}

int ObLogEngine::response_register_server(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const bool is_assign_parent_succeed, const ObCascadMemberList& candidate_list,
    const int32_t msg_type)
{
  // candidate_list can be empty
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key), K(is_assign_parent_succeed), K(candidate_list));
  } else {
    ObFetchRegisterServerRespV2 req(is_assign_parent_succeed, msg_type);
    if (OB_FAIL(req.set_candidate_list(candidate_list))) {
      CLOG_LOG(WARN, "set_candidate_list failed", K(key), K(ret), K(candidate_list));
    } else {
      ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_FETCH_REGISTER_SERVER_RESP_V2, req);
    }
  }
  return ret;
}

int ObLogEngine::request_replace_sick_child(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const common::ObAddr& sick_child)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid() || !sick_child.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key), K(sick_child));
  } else {
    ObReplaceSickChildReq req(sick_child);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_REPLACE_SICK_CHILD_REQ, req);
  }
  return ret;
}

int ObLogEngine::reject_server(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const int32_t msg_type)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid() || OB_REPLICA_MSG_TYPE_UNKNOWN == msg_type) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key), K(msg_type));
  } else {
    int64_t now = ObTimeUtility::current_time();
    ObRejectMsgReq req(msg_type, now);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_REJECT_MSG, req);
  }
  return ret;
}

int ObLogEngine::notify_restore_log_finished(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key));
  } else {
    ObRestoreLogFinishMsg req(log_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_RESTORE_LOG_FINISH_MSG, req);
  }
  return ret;
}

int ObLogEngine::notify_reregister(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& key, const share::ObCascadMember& new_leader)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !new_leader.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(new_leader), K(key));
  } else {
    int64_t now = ObTimeUtility::current_time();
    ObReregisterMsg req(now, new_leader);
    ret = post_packet(tenant_id, server, dst_cluster_id, key, OB_REREGISTER_MSG, req);
  }
  return ret;
}

int ObLogEngine::submit_prepare_rqst(
    const common::ObMemberList& mem_list, const ObPartitionKey& key, const ObProposalID proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(key), K(proposal_id));
  } else {
    ObPrepareReq req(proposal_id);
    ret = post_packet(mem_list, key, OB_PREPARE_REQ, req);
  }
  return ret;
}

int ObLogEngine::submit_standby_prepare_rqst(
    const common::ObMemberList& mem_list, const ObPartitionKey& key, const ObProposalID proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(key), K(proposal_id));
  } else {
    ObStandbyPrepareReq req(proposal_id);
    ret = post_packet(mem_list, key, OB_STANDBY_PREPARE_REQ, req);
  }
  return ret;
}

int ObLogEngine::broadcast_info(const common::ObMemberList& mem_list, const ObPartitionKey& key,
    const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!mem_list.is_valid() || !key.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type) ||
             OB_INVALID_ID == max_confirmed_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(mem_list), K(key), K(replica_type), K(max_confirmed_log_id));
  } else {
    ObBroadcastInfoReq req(replica_type, max_confirmed_log_id);
    ret = post_packet(mem_list, key, OB_BROADCAST_INFO_REQ, req);
  }
  return ret;
}

int ObLogEngine::submit_confirmed_info(const ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || OB_IS_INVALID_LOG_ID(log_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObConfirmedInfoReq req(log_id, confirmed_info, batch_committed);
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2100 && !SVR_BLACK_LIST.is_empty()) {
      share::ObCascadMemberList dst_list;
      get_dst_list_(mem_list, dst_list);
      ret = post_packet(dst_list, key, OB_CONFIRMED_INFO_REQ, req);
    } else {
      ret = post_packet(mem_list, key, OB_CONFIRMED_INFO_REQ, req);
    }
  }
  return ret;
}

int ObLogEngine::submit_renew_ms_confirmed_info(const ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
    const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || !ms_proposal_id.is_valid() || OB_IS_INVALID_LOG_ID(log_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObRenewMsLogConfirmedInfoReq req(log_id, ms_proposal_id, confirmed_info);
    if (!SVR_BLACK_LIST.is_empty()) {
      share::ObCascadMemberList dst_list;
      get_dst_list_(mem_list, dst_list);
      ret = post_packet(dst_list, key, OB_RENEW_MS_CONFIRMED_INFO_REQ, req);
      CLOG_LOG(DEBUG,
          "-->submit_renew_ms_confirmed_info",
          K(dst_list),
          K(key),
          K(log_id),
          K(ms_proposal_id),
          K(confirmed_info));
    } else {
      ret = post_packet(mem_list, key, OB_RENEW_MS_CONFIRMED_INFO_REQ, req);
    }
  }
  return ret;
}

int ObLogEngine::prepare_response(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& partition_key, const ObProposalID proposal_id, const uint64_t max_log_id,
    const int64_t max_log_ts)
{
  // Only paxos member need send prepare response to leader,
  // so their cluster_id must be same.
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || (dst_cluster_id != OB_INVALID_CLUSTER_ID && dst_cluster_id != self_cluster_id) ||
             !partition_key.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(server),
        K(partition_key),
        K(max_log_id),
        K(proposal_id),
        K(dst_cluster_id),
        K(self_cluster_id));
  } else {
    CLOG_LOG(INFO,
        "-->prepare_respone",
        K(partition_key),
        K(proposal_id),
        K(max_log_id),
        K(max_log_ts),
        K(server),
        K(dst_cluster_id));
    ObPrepareRespV2 req(proposal_id, max_log_id, max_log_ts);
    ret = post_packet(tenant_id, server, partition_key, OB_PREPARE_RESP_V2, req);
  }
  return ret;
}

int ObLogEngine::standby_prepare_response(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& partition_key, const ObProposalID proposal_id, const uint64_t ms_log_id,
    const int64_t membership_version, const common::ObMemberList& member_list)
{
  // Only paxos member need send standby prepare response to standby_leader,
  // so their cluster_id must be same.
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || (dst_cluster_id != OB_INVALID_CLUSTER_ID && dst_cluster_id != self_cluster_id) ||
             !partition_key.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(server),
        K(partition_key),
        K(proposal_id),
        K(ms_log_id),
        K(membership_version),
        K(dst_cluster_id),
        K(self_cluster_id));
  } else {
    CLOG_LOG(INFO,
        "-->standby_prepare_respone",
        K(partition_key),
        K(proposal_id),
        K(ms_log_id),
        K(membership_version),
        K(member_list),
        K(server),
        K(dst_cluster_id));
    ObStandbyPrepareResp req(proposal_id, ms_log_id, membership_version);
    if (OB_FAIL(req.set_member_list(member_list))) {
      CLOG_LOG(WARN,
          "set_member_list failed",
          K(ret),
          K(partition_key),
          K(proposal_id),
          K(membership_version),
          K(member_list));
    } else {
      ret = post_packet(tenant_id, server, partition_key, OB_STANDBY_PREPARE_RESP, req);
    }
  }
  return ret;
}

int ObLogEngine::send_keepalive_msg(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& partition_key, const uint64_t next_log_id, const int64_t next_log_ts_lb,
    const uint64_t deliver_cnt)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid() || next_log_ts_lb < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(partition_key), K(next_log_id), K(next_log_ts_lb), K(deliver_cnt));
  } else {
    ObLogKeepaliveMsg req(next_log_id, next_log_ts_lb, deliver_cnt);
    ret = post_packet(tenant_id, server, dst_cluster_id, partition_key, OB_KEEPALIVE_MSG, req);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(
          INFO, "send_keepalive_msg", K(server), K(partition_key), K(next_log_id), K(next_log_ts_lb), K(deliver_cnt));
    }
  }
  return ret;
}

int ObLogEngine::send_restore_alive_msg(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& partition_key, const uint64_t start_log_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(partition_key));
  } else {
    ObRestoreAliveMsg req(start_log_id);
    ret = post_packet(tenant_id, server, dst_cluster_id, partition_key, OB_RESTORE_ALIVE_MSG, req);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "send_restore_alive_msg", K(server), K(partition_key), K(start_log_id));
    }
  }
  return ret;
}

int ObLogEngine::send_restore_alive_req(const common::ObAddr& server, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(partition_key));
  } else {
    int64_t now = ObTimeUtility::current_time();
    ObRestoreAliveReq req(now);
    int64_t dst_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    ret = post_packet(tenant_id, server, dst_cluster_id, partition_key, OB_RESTORE_ALIVE_REQ, req);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "send_restore_alive_req", K(server), K(partition_key), K(now));
    }
  }
  return ret;
}

int ObLogEngine::send_restore_alive_resp(
    const common::ObAddr& server, const int64_t dst_cluster_id, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(partition_key));
  } else {
    int64_t now = ObTimeUtility::current_time();
    ObRestoreAliveResp req(now);
    ret = post_packet(tenant_id, server, dst_cluster_id, partition_key, OB_RESTORE_ALIVE_RESP, req);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "send_restore_alive_resp", K(server), K(partition_key), K(now));
    }
  }
  return ret;
}

int ObLogEngine::notify_restore_leader_takeover(const common::ObAddr& server, const common::ObPartitionKey& key)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(key));
  } else {
    CLOG_LOG(DEBUG, "-->notify_restore_leader_takeover", K(server), K(key));
    int64_t now = ObTimeUtility::current_time();
    ObRestoreTakeoverMsg req(now);
    int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    ret = post_packet(tenant_id, server, cluster_id, key, OB_RESTORE_LEADER_TAKEOVER_MSG, req);
  }
  return ret;
}

int ObLogEngine::send_leader_max_log_msg(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& partition_key, const int64_t switchover_epoch, const uint64_t max_log_id,
    const int64_t next_log_ts)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid() || switchover_epoch < 0 || max_log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(partition_key), K(switchover_epoch), K(max_log_id));
  } else {
    ObLeaderMaxLogMsg req(switchover_epoch, max_log_id, next_log_ts);
    ret = post_packet(tenant_id, server, dst_cluster_id, partition_key, OB_LEADER_MAX_LOG_MSG, req);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO,
          "send_leader_max_log_msg",
          K(server),
          K(partition_key),
          K(switchover_epoch),
          K(max_log_id),
          K(next_log_ts));
    }
  }
  return ret;
}

int ObLogEngine::send_sync_log_archive_progress_msg(const common::ObAddr& server, const int64_t cluster_id,
    const common::ObPartitionKey& partition_key, const ObPGLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(partition_key), K(status));
  } else {
    ObSyncLogArchiveProgressMsg req(status);
    ret = post_packet(tenant_id, server, cluster_id, partition_key, OB_SYNC_LOG_ARCHIVE_PROGRESS, req);
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      CLOG_LOG(INFO, "send_sync_log_archive_progress", K(server), K(partition_key), K(status));
    }
  }
  return ret;
}

int ObLogEngine::notify_follower_log_missing(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& partition_key, const uint64_t start_log_id, const bool is_in_member_list,
    const int32_t msg_type)
{
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  CLOG_LOG(INFO, "notify_follower_log_missing", K(server), K(partition_key), K(start_log_id), K(is_in_member_list));
  ObNotifyLogMissingReq req(start_log_id, is_in_member_list, msg_type);
  return post_packet(tenant_id, server, dst_cluster_id, partition_key, OB_NOTIFY_LOG_MISSING, req);
}

void ObLogEngine::update_clog_info(const int64_t max_submit_timestamp)
{
  ObCommitLogEnv* log_env = get_clog_env_();
  log_env->get_info_block_handler().update_info(max_submit_timestamp);
}

void ObLogEngine::update_clog_info(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp)
{
  ObCommitLogEnv* log_env = get_clog_env_();
  log_env->get_info_block_handler().update_info(partition_key, log_id, submit_timestamp);
}

ObILogInfoBlockReader* ObLogEngine::get_clog_info_block_reader()
{
  int ret = OB_SUCCESS;
  ObILogInfoBlockReader* reader = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObCommitLogEnv* log_env = get_clog_env_();
    ret = log_env->get_log_info_block_reader(reader);
  }
  UNUSED(ret);
  return reader;
}

void ObLogEngine::revert_clog_info_block_reader(ObILogInfoBlockReader* reader)
{
  if (NULL != reader) {
    reader->~ObILogInfoBlockReader();
    ob_free(reader);
    reader = NULL;
  }
}

int ObLogEngine::reset_clog_info_block()
{
  int ret = OB_SUCCESS;
  ObCommitLogEnv* log_env = get_clog_env_();
  ret = log_env->get_info_block_handler().reset();
  return ret;
}

int ObLogEngine::get_clog_info_handler(const file_id_t file_id, ObCommitInfoBlockHandler& clog_handler)
{
  int ret = OB_SUCCESS;
  ObILogInfoBlockReader* reader = NULL;

  if (OB_UNLIKELY(!is_valid_file_id(file_id))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(file_id));
  } else if (OB_ISNULL((reader = get_clog_info_block_reader()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    int64_t pos = 0;
    ObReadParam param;
    ObReadRes res;
    param.file_id_ = file_id;
    param.timeout_ = DEFAULT_READ_TIMEOUT;
    if (OB_FAIL(reader->read_info_block_data(param, res))) {
      if (OB_READ_NOTHING == ret) {
        // trying to read last file
        // do nothing
        // keep ret = OB_READ_NOTHING;
      } else {
        CLOG_LOG(WARN, "read clog info block data error", K(ret), K(param));
      }
    } else if (OB_FAIL(clog_handler.resolve_info_block(res.buf_, res.data_len_, pos))) {
      CLOG_LOG(WARN, "clog handler resolve info block error", K(ret), K(res), K(pos));
    } else {
      // success
    }
  }
  if (NULL != reader) {
    revert_clog_info_block_reader(reader);
  }
  return ret;
}

int ObLogEngine::get_remote_membership_status(const common::ObAddr& server, const int64_t dst_cluster_id,
    const common::ObPartitionKey& partition_key, int64_t& timestamp, uint64_t& max_confirmed_log_id,
    bool& remote_replica_is_normal)
{
  int ret = OB_SUCCESS;
  obrpc::ObLogGetMCTsRequest request;
  obrpc::ObLogGetMCTsResponse result;
  request.set_partition_key(partition_key);
  timestamp = OB_INVALID_TIMESTAMP;
  const int64_t TIMEOUT = 400 * 1000;  // 400 ms, 200 ms for rpc,200 ms for handle
  const int64_t tenant_id = partition_key.get_tenant_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(rpc_->to(server)
                         .dst_cluster_id(dst_cluster_id)
                         .by(tenant_id)
                         .timeout(TIMEOUT)
                         .get_mc_ts(request, result))) {
    CLOG_LOG(WARN, "rpc get_mc_ts failed", K(server), K(partition_key), K(ret));
  } else {
    timestamp = result.get_membership_timestamp();
    max_confirmed_log_id = result.get_max_confirmed_log_id();
    remote_replica_is_normal = result.get_is_normal_partition();
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2275) {
      remote_replica_is_normal = true;
    }
  }
  CLOG_LOG(INFO,
      "get_remote_membership_status",
      K(ret),
      K(server),
      K(dst_cluster_id),
      K(partition_key),
      K(timestamp),
      K(max_confirmed_log_id));
  return ret;
}

int ObLogEngine::get_remote_priority_array(const common::ObAddr& server,
    const common::ObPartitionIArray& partition_array, election::PriorityArray& priority_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObLogGetPriorityArrayRequest request;
  obrpc::ObLogGetPriorityArrayResponse result;
  const int64_t TIMEOUT = 9 * 1000 * 1000;  // 1s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || partition_array.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(server), K(partition_array));
  } else if (OB_FAIL(request.set_partition_array(partition_array))) {
    CLOG_LOG(WARN, "set_partition_array failed", K(ret), K(server), K(partition_array));
  } else if (OB_FAIL(rpc_->to(server).by(OB_SERVER_TENANT_ID).timeout(TIMEOUT).get_priority_array(request, result))) {
    CLOG_LOG(WARN, "rpc get_priority_array failed", K(server), K(partition_array), K(ret));
  } else {
    priority_array = result.get_priority_array();
  }
  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    CLOG_LOG(INFO, "finish get_remote_priority_array", K(ret), K(server), K(partition_array), K(priority_array));
  }
  return ret;
}

int ObLogEngine::get_remote_mc_ctx_array(
    const common::ObAddr& server, const common::ObPartitionArray& partition_array, McCtxArray& mc_ctx_array)
{
  int ret = OB_SUCCESS;
  obrpc::ObLogGetMcCtxArrayRequest request;
  obrpc::ObLogGetMcCtxArrayResponse result;
  request.set_partition_array(partition_array);
  const int64_t TIMEOUT = 400 * 1000;  // 400 ms, 200 ms for rpc, 200 ms for handle
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || partition_array.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(server), K(partition_array));
  } else if (OB_FAIL(rpc_->to(server).by(OB_SERVER_TENANT_ID).timeout(TIMEOUT).get_mc_ctx_array(request, result))) {
    CLOG_LOG(WARN, "rpc get_mc_ctx_array failed", K(server), K(partition_array), K(ret));
  } else {
    mc_ctx_array = result.get_mc_ctx_array();
  }
  CLOG_LOG(INFO, "get_remote_mc_ctx_array", K(ret), K(server), K(partition_array), K(mc_ctx_array));
  return ret;
}

// Load the effective content of the last file into hotcache, and set the corresponding offset, etc.
int ObHotCacheWarmUpHelper::warm_up(ObLogDirectReader& reader, ObTailCursor& tail, ObLogCache& cache)
{
  int ret = OB_SUCCESS;
  ObReadParam param;
  ObReadRes res;
  ObReadCost dummy_cost;
  // DirectReader requires buf alignment, in order to avoid byte_arr_ length dependent on DirectReader logic,
  // the DirectReader interface is used here, instead of directly using the large buffer of hot_cache, it uses memcpy.
  // Because the rbuf allocated by DirectReader is 1.875M, it needs to be split
  ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
  ObReadBuf& rbuf = guard.get_read_buf();
  if (OB_UNLIKELY(!rbuf.is_valid())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "direct reader alloc read_buf error when warm up", K(ret));
  } else if (!tail.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "warmup error, invalid tail cursor", K(ret), K(tail.file_id_), K(tail.offset_));
  } else if (NULL == cache.hot_cache_.byte_arr_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "warm up error, null hot cache buffer", K(ret));
  } else {
    const int64_t max_log_buf_size = OB_MAX_LOG_BUFFER_SIZE;  // 1.875M
    int64_t warm_len = 0;
    param.file_id_ = tail.file_id_;
    while (OB_SUCC(ret) && warm_len < tail.offset_) {
      param.offset_ = static_cast<offset_t>(warm_len);
      param.read_len_ = std::min(max_log_buf_size, tail.offset_ - warm_len);
      if (OB_FAIL(reader.read_data_direct(param, rbuf, res, dummy_cost))) {
        CLOG_LOG(WARN, "read data direct error", K(ret));
      } else {
        CLOG_LOG(INFO, "warm up", K(warm_len), K(param), K(rbuf), K(res));
        MEMCPY(cache.hot_cache_.byte_arr_ + warm_len, res.buf_, res.data_len_);
        warm_len += res.data_len_;
      }
    }  // end while
    if (OB_SUCC(ret)) {
      cache.hot_cache_.base_offset_ = get_long_offset(tail.file_id_, 0);
      cache.hot_cache_.head_offset_ = cache.hot_cache_.base_offset_;
      cache.hot_cache_.tail_offset_ = get_long_offset(tail.file_id_, tail.offset_);
      cache.hot_cache_.is_inited_ = true;
      CLOG_LOG(INFO, "warm up success", K(ret), K(tail.file_id_), K(tail.offset_), K(cache.hot_cache_));
    }
  }
  return ret;
}

int ObLogEngine::update_free_quota()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clog_env_.update_free_quota())) {
    CLOG_LOG(WARN, "update free quota for old clog env failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogEngine::update_min_using_file_id()
{
  int ret = OB_SUCCESS;
  NeedFreezePartitionArray partition_array;
  bool need_freeze_based_on_used_space = false;
  if (OB_FAIL(check_need_freeze_based_on_used_space_(need_freeze_based_on_used_space))) {
    CLOG_LOG(WARN, "check need_freeze_base_on_used_space failed", K(ret));
  } else if (OB_FAIL(clog_env_.update_min_using_file_id(partition_array, need_freeze_based_on_used_space))) {
    CLOG_LOG(WARN, "update min using file id for old clog failed", K(ret));
  } else {
    // do nothing
  }
  (void)set_need_freeze_partition_array_(partition_array);
  return ret;
}

uint32_t ObLogEngine::get_clog_min_using_file_id() const
{
  return clog_env_.get_min_using_file_id();
}

uint32_t ObLogEngine::get_clog_min_file_id() const
{
  return clog_env_.get_min_file_id();
}

uint32_t ObLogEngine::get_clog_max_file_id() const
{
  return clog_env_.get_max_file_id();
}

int64_t ObLogEngine::get_free_quota() const
{
  const ObCommitLogEnv* log_env = get_clog_env_();
  return log_env->get_free_quota();
}

bool ObLogEngine::is_disk_space_enough() const
{
  return get_free_quota() >= 0;
}

void ObLogEngine::try_recycle_file()
{
  clog_env_.try_recycle_file();
}

int ObLogEngine::set_need_freeze_partition_array_(const NeedFreezePartitionArray& partition_array)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(need_freeze_partition_array_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(need_freeze_partition_array_.assign(partition_array))) {
    CLOG_LOG(WARN, "need_freeze_partition_array_ assign failed", K(ret));
  }
  CLOG_LOG(INFO, "set_need_freeze_partition_array", K(ret), K(need_freeze_partition_array_));
  return ret;
}

int ObLogEngine::get_need_freeze_partition_array(NeedFreezePartitionArray& partition_array) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(need_freeze_partition_array_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(partition_array.assign(need_freeze_partition_array_))) {
    CLOG_LOG(WARN, "need_freeze_partition_array_ failed", K(ret));
  }
  return ret;
}

int ObLogEngine::check_need_freeze_based_on_used_space_(bool& is_need) const
{
  int ret = OB_SUCCESS;
  is_need = false;
  int64_t total_space = 0;
  int64_t clog_used_space = 0;
  int64_t ilog_used_space = 0;
  const int64_t RESERVED_DISK_USAGE_PERCENT = 80;
  if (OB_FAIL(clog_env_.get_total_disk_space(total_space))) {
    CLOG_LOG(WARN, "get_total_disk_space failed", K(ret));
  } else if (OB_FAIL(clog_env_.get_using_disk_space(clog_used_space))) {
    CLOG_LOG(WARN, "clog_env_ get_used_disk_space failed", K(ret));
  } else if (OB_FAIL(ilog_storage_.get_used_disk_space(ilog_used_space))) {
    CLOG_LOG(WARN, "ilog_storage_ get_used_disk_space failed", K(ret));
  } else {
    const int64_t clog_disk_reuse_percent = ObServerConfig::get_instance().clog_disk_utilization_threshold;
    is_need = ((clog_used_space + ilog_used_space) >=
               (total_space * clog_disk_reuse_percent / 100LL * RESERVED_DISK_USAGE_PERCENT / 100LL));
  }
  CLOG_LOG(INFO, "need_freeze_based_on_used_space", K(is_need), K(clog_used_space), K(ilog_used_space), K(total_space));
  return ret;
}

int ObLogEngine::check_need_block_log(bool& is_need) const
{
  int ret = OB_SUCCESS;
  is_need = false;
  const uint32_t clog_max_file_id = clog_env_.get_max_file_id();
  const uint32_t clog_min_file_id = clog_env_.get_min_file_id();
  const uint32_t clog_min_using_file_id = clog_env_.get_min_using_file_id();

  // clog disk has been used
  if (clog_min_file_id > 1) {
    is_need = (clog_max_file_id - clog_min_using_file_id) * 100LL >
              (clog_max_file_id - clog_min_file_id) * RESERVED_DISK_USAGE_PERFERT;
  }
  return ret;
}

int ObLogEngine::submit_batch_log(const common::ObMemberList& member_list, const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array)
{
  int ret = OB_SUCCESS;

  ObBatchPushLogReq req;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (!member_list.is_valid() || !trans_id.is_valid() || 0 == partition_array.count() ||
             partition_array.count() != log_info_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(member_list), K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_FAIL(req.init(trans_id, partition_array, log_info_array))) {
    CLOG_LOG(WARN, "req init failed", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_FAIL(post_packet(member_list, DUMMY_PARTITION_ID, OB_BATCH_PUSH_LOG, req))) {
    CLOG_LOG(WARN, "post_packet failed", K(ret), K(trans_id), K(member_list));
  }

  return ret;
}

int ObLogEngine::submit_batch_ack(
    const common::ObAddr& leader, const transaction::ObTransID& trans_id, const ObBatchAckArray& batch_ack_array)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;

  ObBatchAckLogReq req;
  // the caller guarantees cluster_id is same with leader
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (!leader.is_valid() || !trans_id.is_valid() || 0 == batch_ack_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(leader), K(trans_id), K(batch_ack_array));
  } else if (OB_FAIL(req.init(trans_id, batch_ack_array))) {
    CLOG_LOG(WARN, "req init failed", K(ret), K(trans_id), K(batch_ack_array));
  } else if (OB_FAIL(post_packet(tenant_id, leader, DUMMY_PARTITION_ID, OB_BATCH_ACK_LOG, req))) {
    CLOG_LOG(WARN, "post packet failed", K(ret), K(trans_id), K(leader), K(batch_ack_array));
  }

  return ret;
}

int ObLogEngine::query_remote_log(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
    const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp)
{
  int ret = OB_SUCCESS;
  obrpc::ObLogGetRemoteLogRequest request;
  obrpc::ObLogGetRemoteLogResponse result;
  request.set(partition_key, log_id);
  const int64_t TIMEOUT = 5 * 1000 * 1000;  // 5s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition_key.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(server), K(partition_key), K(log_id));
  } else if (OB_FAIL(rpc_->to(server).by(OB_SERVER_TENANT_ID).timeout(TIMEOUT).get_remote_log(request, result))) {
    CLOG_LOG(WARN, "rpc get_remote_log failed", K(ret), K(server), K(partition_key), K(log_id));
  } else {
    ret = result.get_ret_value();
    if (OB_SUCCESS == ret) {
      trans_id = result.get_trans_id();
      submit_timestamp = result.get_submit_timestamp();
    } else {
      trans_id.reset();
      submit_timestamp = OB_INVALID_TIMESTAMP;
    }
  }
  CLOG_LOG(INFO,
      "query_remote_log finished",
      K(ret),
      K(server),
      K(partition_key),
      K(log_id),
      K(trans_id),
      K(submit_timestamp));
  return ret;
}

int ObLogEngine::get_clog_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else {
    if (OB_FAIL(get_clog_env_()->get_log_file_store()->get_file_id_range(min_file_id, max_file_id))) {
      CLOG_LOG(WARN, "get_file_id_range failed", K(ret));
    }
  }
  return ret;
}

int ObLogEngine::delete_all_clog_files()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_clog_env_()->get_log_file_store()->delete_all_files())) {
    CLOG_LOG(WARN, "delete_all_files failed", K(ret));
  } else if (OB_FAIL(delete_file_(cfg_.log_shm_path_))) {
    CLOG_LOG(WARN, "delete_file failed", K(ret));
  } else {
    // TODO: close & delete cfg_.log_shm_path_
    CLOG_LOG(INFO, "delete_all_clog_files success");
  }
  return ret;
}

// ================== interface for ObIlogStorage begin====================
int ObLogEngine::get_cursor_batch(
    const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.get_cursor_batch(pkey, query_log_id, result)) && OB_ERR_OUT_OF_UPPER_BOUND != ret &&
             OB_CURSOR_NOT_EXIST != ret && OB_NEED_RETRY != ret) {
    CLOG_LOG(WARN, "get_cursor_batch failed", K(ret));
  }
  return ret;
}

int ObLogEngine::get_cursor_batch(const common::ObPartitionKey& pkey, const uint64_t query_log_id,
    ObLogCursorExt& log_cursor_ext, ObGetCursorResult& result, uint64_t& cursor_start_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogEngine is not inited", K(ret));
  } else if (!pkey.is_valid() || !is_valid_log_id(query_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(pkey), K(query_log_id));
  } else if (query_log_id >= cursor_start_log_id && query_log_id < cursor_start_log_id + result.ret_len_) {
    log_cursor_ext = result.csr_arr_[query_log_id - cursor_start_log_id];
  } else if (OB_FAIL(get_cursor_batch(pkey, query_log_id, result))) {
    result.reset();
    cursor_start_log_id = OB_INVALID_ID;
    CLOG_LOG(WARN, "get_cursor_batch failed", K(ret), K(pkey), K(query_log_id));
  } else {
    cursor_start_log_id = query_log_id;
    log_cursor_ext = result.csr_arr_[query_log_id - cursor_start_log_id];
  }
  return ret;
}

int ObLogEngine::get_cursor_batch_from_file(
    const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.get_cursor_batch_from_file(pkey, query_log_id, result)) &&
             OB_ERR_OUT_OF_UPPER_BOUND != ret && OB_CURSOR_NOT_EXIST != ret && OB_NEED_RETRY != ret) {
    CLOG_LOG(WARN, "get_cursor_batch_from_file failed", K(ret));
  }
  return ret;
}

int ObLogEngine::get_cursor(
    const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.get_cursor(pkey, query_log_id, log_cursor_ext))) {
    CLOG_LOG(WARN, "get_cursor failed", K(ret));
  }
  return ret;
}

int ObLogEngine::submit_cursor(
    const common::ObPartitionKey& pkey, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.submit_cursor(pkey, log_id, log_cursor_ext))) {
    CLOG_LOG(WARN, "submit_cursor failed", K(ret));
  }
  return ret;
}

int ObLogEngine::submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
    const int64_t memberlist_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.submit_cursor(
                 partition_key, log_id, log_cursor_ext, memberlist, replica_num, memberlist_version))) {
    CSR_LOG(WARN, "submit_cursor failed", K(ret));
  }
  return ret;
}

int ObLogEngine::query_max_ilog_id(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.query_max_ilog_id(pkey, ret_max_ilog_id))) {
    CLOG_LOG(WARN, "query_max_ilog_id failed", K(ret));
  }
  return ret;
}

int ObLogEngine::query_max_flushed_ilog_id(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.query_max_flushed_ilog_id(pkey, ret_max_ilog_id)) && OB_PARTITION_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "query_max_flushed_ilog_id failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "query_max_flushed_ilog_id finished", K(ret), K(pkey), K(ret_max_ilog_id));
  }
  return ret;
}

int ObLogEngine::get_ilog_memstore_min_log_id_and_ts(
    const common::ObPartitionKey& pkey, uint64_t& min_log_id, int64_t& min_log_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.get_ilog_memstore_min_log_id_and_ts(pkey, min_log_id, min_log_ts))) {
    CLOG_LOG(WARN, "get_ilog_memstore_min_log_id_and_ts failed", K(ret));
  }
  return ret;
}

int ObLogEngine::get_ilog_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.get_file_id_range(min_file_id, max_file_id))) {
    CSR_LOG(WARN, "get_file_id_range failed", K(ret));
  }
  return ret;
}

int ObLogEngine::query_next_ilog_file_id(file_id_t& next_ilog_file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.get_next_ilog_file_id_from_memory(next_ilog_file_id))) {
    CSR_LOG(WARN, "get_next_ilog_file_id failed", K(ret));
  }
  return ret;
}

int ObLogEngine::delete_all_ilog_files()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ilog_storage_.get_log_file_store()->delete_all_files())) {
    CLOG_LOG(WARN, "delete_all_files failed", K(ret));
  } else if (OB_FAIL(delete_file_(cfg_.index_log_shm_path_))) {
    CLOG_LOG(WARN, "delete_file failed", K(ret));
  } else {
    // TODO: close & delete cfg_.index_log_shm_path_
    CLOG_LOG(INFO, "delete_all_ilog_files success");
  }
  return ret;
}

int ObLogEngine::locate_by_timestamp(
    const common::ObPartitionKey& pkey, const int64_t start_ts, uint64_t& target_log_id, int64_t& target_log_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.locate_by_timestamp(pkey, start_ts, target_log_id, target_log_timestamp))) {
    CLOG_LOG(WARN, "locate_by_timestamp failed", K(ret));
  }
  return ret;
}

int ObLogEngine::locate_ilog_file_by_log_id(
    const common::ObPartitionKey& pkey, const uint64_t start_log_id, uint64_t& end_log_id, file_id_t& ilog_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (!pkey.is_valid() || !is_valid_log_id(start_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(pkey), K(start_log_id));
  } else if (OB_FAIL(ilog_storage_.locate_ilog_file_by_log_id(pkey, start_log_id, end_log_id, ilog_id))) {
    if (REACH_TIME_INTERVAL(1000 * 1000L)) {
      CLOG_LOG(WARN, "locate_ilog_file_by_log_id failed", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObLogEngine::fill_file_id_cache()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else if (OB_FAIL(ilog_storage_.fill_file_id_cache())) {
    CLOG_LOG(WARN, "fill_file_id_cache failed", K(ret));
  }
  return ret;
}

int ObLogEngine::ensure_log_continuous_in_file_id_cache(const ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogEngine is not inited", K(ret));
  } else if (!pkey.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(pkey), K(log_id));
  } else if (OB_FAIL(ilog_storage_.ensure_log_continuous_in_file_id_cache(pkey, log_id))) {
    CLOG_LOG(WARN, "ensure_log_continuous_in_file_id_cache failed", K(ret), K(pkey), K(log_id));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogEngine::get_index_info_block_map(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogEngine is not inited", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(file_id));
  } else if (OB_FAIL(ilog_storage_.get_index_info_block_map(file_id, index_info_block_map))) {
    CLOG_LOG(ERROR, "get_index_info_block_map failed", K(ret), K(file_id));
  }
  return ret;
}

int ObLogEngine::check_is_clog_obsoleted(
    const ObPartitionKey& pkey, const file_id_t file_id, const offset_t offset, bool& is_obsoleted) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogEngine is not inited", KR(ret), K(pkey), K(file_id), K(offset));
  } else if (OB_FAIL(ilog_storage_.check_is_clog_obsoleted(pkey, file_id, offset, is_obsoleted))) {
    CLOG_LOG(WARN, "failed to check_is_clog_obsoleted", KR(ret), K(pkey), K(file_id), K(offset));
  } else { /*do nothing*/
  }
  return ret;
}
// ================== interface for ObIlogStorage end  ====================

int ObLogEngine::get_clog_using_disk_space(int64_t& space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else {
    ret = clog_env_.get_using_disk_space(space);
  }
  return ret;
}

int ObLogEngine::get_ilog_using_disk_space(int64_t& space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogEngine is not inited", K(ret));
  } else {
    ret = ilog_storage_.get_used_disk_space(space);
  }
  return ret;
}

bool ObLogEngine::is_clog_disk_error() const
{
  bool is_disk_error = false;
  const ObCommitLogEnv* env = get_clog_env_();
  if (IS_NOT_INIT) {
    is_disk_error = false;
  } else if (OB_LIKELY(NULL != env)) {
    is_disk_error = (env->get_writer()).is_disk_error();
  }
  return is_disk_error;
}

NetworkLimitManager::NetworkLimitManager() : is_inited_(false), addr_array_(), ethernet_speed_(0), hash_map_()
{}

NetworkLimitManager::~NetworkLimitManager()
{
  destroy();
}

int NetworkLimitManager::init(const int64_t ethernet_speed)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "NetworkLimitManager is inited twice");
  } else if (ethernet_speed <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(ethernet_speed));
  } else if (OB_FAIL(hash_map_.init(ObModIds::OB_CLOG_MGR))) {
    CLOG_LOG(WARN, "hash_map_ init failed");
  } else {
    ethernet_speed_ = ethernet_speed;
    is_inited_ = true;
  }
  return ret;
}

void NetworkLimitManager::destroy()
{
  is_inited_ = false;
  addr_array_.reset();
  ethernet_speed_ = 0;
  hash_map_.reset();
}

int NetworkLimitManager::get_limit(const common::ObAddr& addr, int64_t& network_limit)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "NetworkLimitManager is not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(addr));
  } else {
    bool need_write_lock = false;
    do {
      RLockGuard guard(lock_);
      if (addr_contains_(addr)) {
        network_limit = ethernet_speed_ * NET_PERCENTAGE / 100 / addr_array_.count();
      } else {
        need_write_lock = true;
      }
    } while (0);

    if (need_write_lock) {
      WLockGuard guard(lock_);
      if (!addr_contains_(addr) && OB_FAIL(addr_array_.push_back(addr))) {
        CLOG_LOG(WARN, "addr_array_ push_back failed", K(ret), K(addr));
      } else {
        network_limit = ethernet_speed_ * NET_PERCENTAGE / 100 / addr_array_.count();
      }
    }

    try_reset_addr_array_();
  }

  return ret;
}

int NetworkLimitManager::get_limit(const common::ObMemberList& member_list, int64_t& network_limit)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "NetworkLimitManager is not inited", K(ret));
  } else if (!member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(member_list));
  } else {
    bool need_write_lock = false;
    do {
      RLockGuard guard(lock_);
      if (addr_contains_(member_list)) {
        network_limit = ethernet_speed_ * NET_PERCENTAGE / 100 / addr_array_.count();
      } else {
        need_write_lock = true;
      }
    } while (0);

    if (need_write_lock) {
      WLockGuard guard(lock_);
      if (OB_FAIL(update_addr_array_(member_list))) {
        CLOG_LOG(WARN, "update_addr_array_ failed", K(ret), K(member_list));
      } else {
        network_limit = ethernet_speed_ * NET_PERCENTAGE / 100 / addr_array_.count();
      }
    }

    try_reset_addr_array_();
  }

  return ret;
}

bool NetworkLimitManager::addr_contains_(const common::ObAddr& addr) const
{
  bool bool_ret = false;

  for (int64_t index = 0; !bool_ret && index < addr_array_.count(); index++) {
    bool_ret = (addr == addr_array_[index]);
  }

  return bool_ret;
}

bool NetworkLimitManager::addr_contains_(const common::ObMemberList& member_list) const
{
  bool bool_ret = true;
  int tmp_ret = OB_SUCCESS;

  for (int64_t index = 0; bool_ret && index < member_list.get_member_number(); index++) {
    ObAddr addr;
    if (OB_SUCCESS != (tmp_ret = member_list.get_server_by_index(index, addr))) {
      CLOG_LOG(WARN, "member_list get_server_by_index failed", K(tmp_ret), K(index));
      bool_ret = false;
    } else {
      bool_ret = addr_contains_(addr);
    }
  }

  return bool_ret;
}

int NetworkLimitManager::update_addr_array_(const common::ObMemberList& member_list)
{
  int ret = OB_SUCCESS;

  for (int64_t index = 0; OB_SUCC(ret) && index < member_list.get_member_number(); index++) {
    ObAddr addr;
    if (OB_FAIL(member_list.get_server_by_index(index, addr))) {
      CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K(index));
    } else if (!addr_contains_(addr) && OB_FAIL(addr_array_.push_back(addr))) {
      CLOG_LOG(WARN, "addr_array_ push_back failed", K(ret), K(addr));
    } else {
      // do nothing
    }
  }

  return ret;
}

void NetworkLimitManager::try_reset_addr_array_()
{
  if (REACH_TIME_INTERVAL(RESET_ADDR_ARRAY_INTERVAL)) {
    WLockGuard guard(lock_);
    addr_array_.reset();
  }
}

int NetworkLimitManager::try_limit(const common::ObAddr& addr, const int64_t size, const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  HashV value;
  value.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "NetworkLimitManager is not inited");
  } else if (!addr.is_valid() || size < 0 || network_limit < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(addr), K(size), K(network_limit));
  } else if (network_limit == 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(TRACE, "no need to limit", K(ret));
  } else if (OB_FAIL(hash_map_.get(addr, value))) {
    value.reset();
    if (OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "hash_map_ get failed", K(ret), K(addr));
    } else if (OB_FAIL(hash_map_.insert(addr, value))) {
      if (OB_ENTRY_EXIST != ret) {
        CLOG_LOG(WARN, "hash_map_ insert failed", K(ret), K(addr));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  const int64_t limit = std::max(network_limit / (1000 * 1000 / RESET_NETWORK_LIMIT_INTERVAL), size);
  if (OB_SUCC(ret)) {
    CheckFunction check_fn(size, limit);
    while (OB_EAGAIN == (ret = hash_map_.operate(addr, check_fn))) {
      usleep(SLEEP_TS);
    }
    if (OB_SUCCESS != ret) {
      CLOG_LOG(WARN, "hash_map_ operate failed", K(ret), K(addr), K(size), K(limit));
    }
  }
  return ret;
}

int ObLogEngine::delete_file_(const char* name)
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(name));
  } else {
    bool is_exist = false;
    n = snprintf(fname, sizeof(fname), "%s", name);
    if (n <= 0 || n >= sizeof(fname)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(common::FileDirectoryUtils::is_exists(fname, is_exist))) {
      COMMON_LOG(ERROR, "check file failed", K(ret), K(fname));
    } else if (is_exist && (0 != ::unlink(fname))) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "unlink file failed", K(ret), K(fname), K(errno));
    } else {
      COMMON_LOG(INFO, "unlink file success", K(ret), K(fname), K(is_exist));
    }
  }

  return ret;
}

};  // end namespace clog
};  // end namespace oceanbase
