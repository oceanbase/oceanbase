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

#define USING_LOG_PREFIX STORAGE_REDO
#include "ob_storage_logger.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_running_mode.h"
#include "lib/file/ob_file.h"
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_debug_sync.h"
#include "share/ob_force_print_log.h"
#include "common/log/ob_log_dir_scanner.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "storage/slog/ob_storage_log_batch_header.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "share/rc/ob_tenant_base.h"
#include <inttypes.h>

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{
ObStorageLogger::ObStorageLogger()
  : is_inited_(false), log_writer_(nullptr),
    tenant_log_writer_(), server_log_writer_(),
    slogger_mgr_(nullptr), log_seq_(0), build_log_mutex_(common::ObLatchIds::SLOG_PROCESSING_MUTEX),
    log_file_spec_(), is_start_(false)
{
}

ObStorageLogger::~ObStorageLogger()
{
  destroy();
}

int ObStorageLogger::init(ObStorageLoggerManager &slogger_manager, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t max_log_file_size = slogger_manager.max_log_file_size_;
  const blocksstable::ObLogFileSpec &log_file_spec = slogger_manager.log_file_spec_;
  const int64_t max_log_size = ObStorageLoggerManager::NORMAL_LOG_ITEM_SIZE;
  int pret = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "The ObStorageLogger has been inited.", K(ret));
  } else {
    if (OB_SERVER_TENANT_ID == tenant_id) {
      log_writer_ = &server_log_writer_;
      pret = snprintf(tnt_slog_dir_, MAX_PATH_SIZE, "%s/server", slogger_manager.get_root_dir());
    } else if (is_virtual_tenant_id(tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(WARN, "Virtual tenant shouldn't create slogger.", K(ret), K(tenant_id));
    } else {
      log_writer_ = &tenant_log_writer_;
      pret = snprintf(tnt_slog_dir_, MAX_PATH_SIZE, "%s/tenant_%" PRIu64,
                    slogger_manager.get_root_dir(), tenant_id);
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (pret < 0 || pret >= MAX_PATH_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_REDO_LOG(ERROR, "construct tenant slog path fail", K(ret), K(tenant_id));
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(tnt_slog_dir_))) {
    STORAGE_REDO_LOG(WARN, "fail to create tenant dir.", K(ret), K_(tnt_slog_dir));
  } else if (OB_FAIL(log_writer_->init(tnt_slog_dir_, max_log_file_size, max_log_size, log_file_spec, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "fail to init log_writer_", K(ret), K_(tnt_slog_dir), K(max_log_file_size));
  } else {
    slogger_mgr_ = &slogger_manager;
    log_file_spec_ = log_file_spec;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

int ObStorageLogger::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Slogger has not been inited.");
  } else if (OB_FAIL(log_writer_->start())) {
    STORAGE_REDO_LOG(WARN, "fail to start log_writer_");
  }
  return ret;
}

int ObStorageLogger::mtl_init(ObStorageLogger* &slogger)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(nullptr == slogger)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Slogger is null.", K(ret), KP(slogger));
  } else if (OB_FAIL(slogger->init(ObStorageLoggerManager::get_instance(), tenant_id))) {
    STORAGE_REDO_LOG(WARN, "failed to init slogger", K(ret));
  }

  return ret;
}

int ObStorageLogger::mtl_start(ObStorageLogger* &slogger)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(slogger)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Slogger is null.", K(ret));
  } else if (OB_UNLIKELY(!slogger->is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Slogger has not been inited.", K(ret));
  } else if (OB_FAIL(slogger->log_writer_->start())) {
    STORAGE_REDO_LOG(WARN, "Fail to start slog writer.", K(ret));
  }
  return ret;
}

void ObStorageLogger::destroy()
{
  if (nullptr != log_writer_) {
    log_writer_->destroy();
    log_writer_ = nullptr;
  }
  slogger_mgr_ = nullptr;
  MEMSET(tnt_slog_dir_, 0, sizeof(tnt_slog_dir_));
  is_inited_ = false;
  log_seq_ = 0;
}

void ObStorageLogger::mtl_stop(ObStorageLogger* &slogger)
{
  if (OB_LIKELY(nullptr != slogger) && OB_LIKELY(nullptr != slogger->log_writer_)) {
    slogger->log_writer_->stop();
  }
}

void ObStorageLogger::mtl_wait(ObStorageLogger* &slogger)
{
  if (OB_LIKELY(nullptr != slogger) && OB_LIKELY(nullptr != slogger->log_writer_)) {
    slogger->log_writer_->wait();
  }
}

int ObStorageLogger::start_log(const ObLogCursor &start_cursor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(is_start_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "SLogger has been started", K(ret));
  } else if (OB_UNLIKELY(!start_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid arguments", K(ret), K(start_cursor));
  } else if (OB_FAIL(log_writer_->start_log(start_cursor))) {
    STORAGE_REDO_LOG(WARN, "fail to pass the start_cursor to log_writer_", K(ret), K(start_cursor));
  } else {
    log_seq_ = start_cursor.log_id_;
    is_start_ = true;
  }
  return ret;
}

int ObStorageLogger::get_active_cursor(ObLogCursor &log_cursor)
{
  int ret = OB_SUCCESS;
  log_cursor = log_writer_->get_cur_cursor();
  return ret;
}

int ObStorageLogger::write_log(ObStorageLogParam &param)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageLogItem *log_item = nullptr;
  param.disk_addr_.reset();

  if (OB_UNLIKELY(!is_start_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Slogger has not started.", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param.data_->is_valid() || param.cmd_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid argument", K(ret), K(param.cmd_));
  } else {
    {
      lib::ObMutexGuard guard(build_log_mutex_);
      if (OB_FAIL(build_log_item(param, log_item))) {
        STORAGE_REDO_LOG(WARN, "fail to build log item", K(ret));
      } else if (OB_FAIL(log_writer_->append_log(*log_item, MAX_APPEND_WAIT_TIME_MS))) {
        STORAGE_REDO_LOG(WARN, "fail to append log", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(log_item)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(WARN, "log_item shouldn't be null", K(ret));
    } else if (OB_FAIL(log_item->wait_flush_log(MAX_FLUSH_WAIT_TIME_MS))) {
      STORAGE_REDO_LOG(WARN, "fail to wait_flush_log", K(ret));
    } else {
      const int64_t file_id = log_item->start_cursor_.file_id_;
      const int64_t offset = log_item->start_cursor_.offset_ + log_item->get_offset(0);
      const int64_t size = log_item->end_cursor_.offset_ - offset;
      if (OB_FAIL(param.disk_addr_.set_file_addr(file_id, offset, size))) {
        STORAGE_REDO_LOG(WARN, "fail to set file address", K(ret), K(file_id), K(offset), K(size));
      } else if (OB_UNLIKELY(0 == param.disk_addr_.size())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(ERROR, "The size of disk_addr_ is 0", K(ret), K(param.disk_addr_),
            K(log_item->get_offset(0)), K(log_item->get_log_cnt()));
      }
    }
  }

  if (nullptr != log_item && OB_TMP_FAIL(slogger_mgr_->free_item(log_item))) {
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
    STORAGE_REDO_LOG(WARN, "fail to free log item.", K(tmp_ret));
  }

  return ret;
}

int ObStorageLogger::write_log(ObIArray<ObStorageLogParam> &param_arr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t num = param_arr.count();
  ObStorageLogItem *log_item = nullptr;

  if (OB_UNLIKELY(!is_start_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Slogger has not started", K(ret), K(is_inited_));
  } else if (INT16_MAX < num) {
    // batch header count is int16_t type, we should not write too much logs in one batch.
    ret = OB_SIZE_OVERFLOW;
    STORAGE_REDO_LOG(ERROR, "batch size too large", K(ret), K(num));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < num; i++) {
      ObStorageLogParam &param = param_arr.at(i);
      if (OB_UNLIKELY(!param.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_REDO_LOG(WARN, "invalid argument", K(ret), K(param), K(i));
      } else {
        param.disk_addr_.reset();
      }
    }
  }

  if (OB_SUCC(ret)) {
    {
      lib::ObMutexGuard guard(build_log_mutex_);
      if (OB_FAIL(build_log_item(param_arr, log_item))) {
        STORAGE_REDO_LOG(WARN, "Fail to build log item.", K(ret));
      } else if (OB_ISNULL(log_item)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "log_item shouldn't be null", K(ret));
      } else if (OB_FAIL(log_writer_->append_log(*log_item, MAX_APPEND_WAIT_TIME_MS))) {
        STORAGE_REDO_LOG(WARN, "Fail to append log.", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(log_item->wait_flush_log(MAX_FLUSH_WAIT_TIME_MS))) {
      STORAGE_REDO_LOG(WARN, "Fail to wait_flush_log.", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < num; i++) {
        ObStorageLogParam &param = param_arr.at(i);
        const int64_t file_id = log_item->start_cursor_.file_id_;
        const int64_t offset = log_item->start_cursor_.offset_ + log_item->get_offset(i);
        int64_t size = 0;
        if (num - 1 != i) {
          size = log_item->get_offset(i + 1) - log_item->get_offset(i);
        } else {
          size = log_item->end_cursor_.offset_ - offset;
        }

        if (OB_FAIL(param.disk_addr_.set_file_addr(file_id, offset, size))) {
          STORAGE_REDO_LOG(WARN, "Fail to set file address.", K(ret), K(file_id), K(offset), K(size));
        } else if (OB_UNLIKELY(0 == param.disk_addr_.size())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_REDO_LOG(ERROR, "The size of disk_addr_ is 0", K(ret), K(param.disk_addr_), K(i),
              K(log_item->get_offset(i)), K(log_item->get_log_cnt()));
        }
      }
    }
  }

  if (nullptr != log_item && OB_TMP_FAIL(slogger_mgr_->free_item(log_item))) {
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
    STORAGE_REDO_LOG(WARN, "Fail to free log item.", K(ret), K(tmp_ret));
  }

  return ret;
}

int ObStorageLogger::remove_useless_log_file(const int64_t end_file_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t start_file_id = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "slogger has not been inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(get_start_file_id(start_file_id, tenant_id))) {
    STORAGE_REDO_LOG(ERROR, "Fail to get file id. ", K(ret), K(start_file_id));
  } else {
    for (; OB_SUCC(ret) && start_file_id < end_file_id; ++start_file_id) {
      if (OB_FAIL(log_writer_->delete_log_file(start_file_id))) {
        STORAGE_REDO_LOG(ERROR, "Fail to delete log file, ", K(ret), K(start_file_id));
      } else {
        STORAGE_REDO_LOG(INFO, "Success to remove useless log file, ", K(start_file_id));
      }
    }
  }

  return ret;
}

int ObStorageLogger::get_using_disk_space(int64_t &using_space) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "slogger has not been inited", K(ret), K(is_inited_));
  } else {
    ret = log_writer_->get_using_disk_space(using_space);
  }
  return ret;
}

int ObStorageLogger::build_log_item(const ObStorageLogParam &param, ObStorageLogItem *&log_item)
{
  int ret = OB_SUCCESS;
  ObStorageLogEntry entry;
  ObStorageLogBatchHeader batch_header;
  const int64_t total_batch_size = 2 * batch_header.get_serialize_size() + // batch header
                             2 * entry.get_serialize_size() + // data and nop's entry
                             param.data_->get_serialize_size() + // data
                             ObLogConstants::LOG_FILE_ALIGN_SIZE; // nop
  const int32_t total_data_len = entry.get_serialize_size() + param.data_->get_serialize_size();

  if (OB_FAIL(slogger_mgr_->alloc_item(total_batch_size, log_item, 1))) {
    STORAGE_REDO_LOG(WARN, "Fail to alloc memory for slog item", K(ret), K(total_batch_size));
  } else if (OB_UNLIKELY(nullptr == log_item)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "Log item is null", K(ret));
  } else if (OB_FAIL(log_item->set_data_len(batch_header.get_serialize_size()))) {
    STORAGE_REDO_LOG(WARN, "Fail to set log item's data length", K(ret));
  } else if (OB_FAIL(log_item->fill_log(log_seq_, param, 0))) {
    STORAGE_REDO_LOG(WARN, "Fail to fill log", K(ret), K(log_seq_));
  } else if (OB_FAIL(log_item->fill_batch_header(total_data_len, 1, 0))) {
    STORAGE_REDO_LOG(WARN, "Fail to fill batch header", K(ret), K(total_data_len));
  } else {
    log_seq_++;
    STORAGE_REDO_LOG(DEBUG, "the slog seq plus plus", K(log_seq_));
  }

  return ret;
}

int ObStorageLogger::build_log_item(
    const ObIArray<ObStorageLogParam> &param_arr,
    ObStorageLogItem *&log_item)
{
  int ret = OB_SUCCESS;
  ObStorageLogEntry entry;
  ObStorageLogBatchHeader batch_header;
  const int64_t num = param_arr.count();
  int32_t total_data_len = num * entry.get_serialize_size();
  for (int i = 0; i < num; i++) {
    total_data_len += param_arr.at(i).data_->get_serialize_size();
  }
  const int64_t total_batch_size = 2 * batch_header.get_serialize_size() + // data and nop's batch header
                             ObLogConstants::LOG_FILE_ALIGN_SIZE + // nop
                             entry.get_serialize_size() + // nop's entry
                             total_data_len; // data and their entries
  if (OB_FAIL(slogger_mgr_->alloc_item(total_batch_size, log_item, num))) {
    STORAGE_REDO_LOG(WARN, "Fail to alloc memory for slog item", K(ret), K(total_batch_size));
  } else if (OB_ISNULL(log_item)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "Log item is null", K(ret));
  } else if (OB_FAIL(log_item->set_data_len(batch_header.get_serialize_size()))) {
    STORAGE_REDO_LOG(WARN, "Fail to set log item's data length", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < num; i++) {
      if (OB_FAIL(log_item->fill_log(log_seq_, param_arr.at(i), i))) {
        STORAGE_REDO_LOG(WARN, "Fail to fill log", K(ret), K(log_seq_));
      } else {
        log_seq_++;
        STORAGE_REDO_LOG(DEBUG, "the slog seq plus plus", K(log_seq_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(log_item->fill_batch_header(total_data_len, num, 0))) {
      STORAGE_REDO_LOG(WARN, "Fail to fill batch header", K(ret), K(total_data_len));
    }
  }

  return ret;
}

int ObStorageLogger::get_start_file_id(int64_t &start_file_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObLogFileHandler file_handler;
  int64_t min_log_id = 0;
  int64_t max_log_id = 0;

  if (OB_FAIL(file_handler.init(tnt_slog_dir_, 256 << 20/*file_size*/, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log file handler.", K(ret));
  } else if (OB_FAIL(file_handler.get_file_id_range(min_log_id, max_log_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    STORAGE_REDO_LOG(WARN, "Fail to get log id range.", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    min_log_id = 1;
  }

  if (OB_SUCC(ret)) {
    start_file_id = min_log_id;
  }

  return ret;
}

}
}
