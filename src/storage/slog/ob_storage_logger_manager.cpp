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
#include "lib/ob_define.h"
#include "lib/ob_running_mode.h"
#include "ob_storage_logger_manager.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;

namespace storage
{
ObStorageLoggerManager::ObStorageLoggerManager()
    : allocator_(SET_USE_UNEXPECTED_500("StorageLoggerM")),
      log_dir_(nullptr),
      max_log_file_size_(0),
      is_inited_(false),
      log_file_spec_(),
      server_slogger_(),
      need_reserved_(false)
{
}

ObStorageLoggerManager::~ObStorageLoggerManager()
{
  destroy();
}

ObStorageLoggerManager &ObStorageLoggerManager::get_instance()
{
  static ObStorageLoggerManager instance_;
  return instance_;
}

int ObStorageLoggerManager::init(
    const char *log_dir,
    const int64_t max_log_file_size,
    const blocksstable::ObLogFileSpec &log_file_spec,
    const bool need_reserved)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "The ObStorageLoggerManager has been inited.", K(ret));
  } else if (OB_UNLIKELY(nullptr == log_dir || max_log_file_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid arguments", K(ret), KP(log_dir), K(max_log_file_size));
  } else if (OB_FAIL(prepare_log_buffers(MAX_CONCURRENT_ITEM_CNT, NORMAL_LOG_ITEM_SIZE))) {
    STORAGE_REDO_LOG(WARN, "fail to prepare log buffers", K(ret),
        LITERAL_K(MAX_CONCURRENT_ITEM_CNT), LITERAL_K(NORMAL_LOG_ITEM_SIZE));
  } else if (OB_FAIL(prepare_log_items(MAX_CONCURRENT_ITEM_CNT))) {
    STORAGE_REDO_LOG(WARN, "fail to prepare log items", K(ret), LITERAL_K(MAX_CONCURRENT_ITEM_CNT));
  } else {
    log_dir_ = log_dir;
    max_log_file_size_ = max_log_file_size;
    log_file_spec_ = log_file_spec;
    need_reserved_ = need_reserved;
    if (OB_FAIL(server_slogger_.init(*this, OB_SERVER_TENANT_ID))) {
      STORAGE_REDO_LOG(WARN, "fail to init server slogger", K(ret));
    } else if (OB_FAIL(server_slogger_.start())) {
      STORAGE_REDO_LOG(WARN,  "fail to start server slogger", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObStorageLoggerManager::destroy()
{
  log_file_spec_.reset();
  max_log_file_size_ = 0;
  server_slogger_.destroy();

  log_buffers_.destroy();
  slog_items_.destroy();
  allocator_.reset();

  log_dir_ = nullptr;
  is_inited_ = false;
  need_reserved_ = false;
}

int ObStorageLoggerManager::get_server_slogger(ObStorageLogger *&slogger)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObStorageLoggerManager has not been inited.", K(ret));
  } else {
    slogger = &server_slogger_;
  }

  return ret;
}

int ObStorageLoggerManager::alloc_item(
    const int64_t buf_size,
    ObStorageLogItem *&log_item,
    const int64_t num)
{
  int ret = OB_SUCCESS;
  void *log_buffer = nullptr;
  bool alloc_locally = buf_size > NORMAL_LOG_ITEM_SIZE;
  int64_t total_size = buf_size;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObStorageLoggerManager has not been inited.", K(ret));
  } else if (OB_UNLIKELY(buf_size <= 0 || num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments.", K(ret), K(buf_size), K(num));
  } else if (alloc_locally) {
    total_size = upper_align(buf_size, ObLogConstants::LOG_FILE_ALIGN_SIZE);
    STORAGE_REDO_LOG(INFO, "Large log item", LITERAL_K(NORMAL_LOG_ITEM_SIZE), K(total_size));
  } else {
    total_size = NORMAL_LOG_ITEM_SIZE;
  }

  if (OB_SUCC(ret)) {
    if (total_size > ObLogConstants::LOG_ITEM_MAX_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_REDO_LOG(ERROR, "Log item is too large", K(ret),
          K(total_size), LITERAL_K(ObLogConstants::LOG_ITEM_MAX_LENGTH));
    } else if (!alloc_locally && OB_FAIL(alloc_log_buffer(log_buffer))) {
      STORAGE_REDO_LOG(WARN, "Fail to alloc memory for log buffer", K(ret));
    } else if (OB_FAIL(alloc_log_item(log_item))) {
      STORAGE_REDO_LOG(WARN, "Fail to alloc memory for log item", K(ret));
    } else if (OB_FAIL(log_item->init(reinterpret_cast<char *>(log_buffer),
        total_size, ObLogConstants::LOG_FILE_ALIGN_SIZE, num))) {
      STORAGE_REDO_LOG(WARN, "Fail to init log item", K(ret));
    } else {
      STORAGE_REDO_LOG(DEBUG, "Successfully alloc memory", K(ret));
    }
  }

  return ret;
}

int ObStorageLoggerManager::free_item(ObStorageLogItem *log_item)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "The ObStorageLoggerManager has not been inited.", K(ret));
  } else if (OB_UNLIKELY(nullptr == log_item)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument.", K(ret));
  } else if (!log_item->is_local() && NULL != log_item->get_buf()) {
    if (OB_FAIL(free_log_buffer(log_item->get_buf()))) {
      STORAGE_REDO_LOG(ERROR, "fail to free slog buffer", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(free_log_item(log_item))) {
    STORAGE_REDO_LOG(ERROR, "fail to free the slog item", K(ret));
  }

  return ret;
}

int ObStorageLoggerManager::alloc_log_buffer(void *&log_buffer)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(log_buffers_.pop(log_buffer))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SLOG_REACH_MAX_CONCURRENCY;
      STORAGE_REDO_LOG(WARN, "Log buffer reach maximum", K(ret), K(log_buffers_.capacity()));
    } else {
      STORAGE_REDO_LOG(WARN, "Fail to pop log buffer", K(ret));
    }
  }

  return ret;
}

int ObStorageLoggerManager::free_log_buffer(void *log_buffer)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(log_buffers_.push(log_buffer))) {
    STORAGE_REDO_LOG(WARN, "Fail to push log buffer", K(ret));
  }
  return ret;
}

int ObStorageLoggerManager::alloc_log_item(ObStorageLogItem *&log_item)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(slog_items_.pop(log_item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SLOG_REACH_MAX_CONCURRENCY;
      STORAGE_REDO_LOG(WARN, "Log items reach maximum", K(ret), K(slog_items_.capacity()));
    } else {
      STORAGE_REDO_LOG(WARN, "Fail to pop log item", K(ret));
    }
  }

  return ret;
}

int ObStorageLoggerManager::free_log_item(ObStorageLogItem *log_item)
{
  int ret = OB_SUCCESS;
  log_item->destroy();

  if (OB_FAIL(slog_items_.push(log_item))) {
    STORAGE_REDO_LOG(ERROR, "Fail to push log item", K(ret), KP(log_item));
  }
  return ret;
}

int ObStorageLoggerManager::prepare_log_buffers(const int64_t count, const int64_t log_buf_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument", K(ret), K(count));
  } else if (OB_FAIL(log_buffers_.init(count))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log buffers", K(ret), K(count));
  } else {
    void *buf = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_ISNULL(buf = allocator_.alloc_aligned(log_buf_size, ObLogConstants::LOG_FILE_ALIGN_SIZE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_REDO_LOG(ERROR, "Fail to alloc memory for buffers", K(ret), K(log_buf_size));
      } else if (OB_FAIL(log_buffers_.push(buf))) {
        STORAGE_REDO_LOG(ERROR, "Fail to push log buffer", K(ret), KP(buf));
      }
    }
  }

  return ret;
}

int ObStorageLoggerManager::prepare_log_items(const int64_t count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(count<= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument", K(ret), K(count));
  } else if (OB_FAIL(slog_items_.init(count))) {
    STORAGE_REDO_LOG(WARN, "Fail to init log_items_", K(ret), K(count));
  } else {
    void *ptr = nullptr;
    ObStorageLogItem *log_item = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (nullptr == (ptr = allocator_.alloc(sizeof(ObStorageLogItem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_REDO_LOG(ERROR, "Fail to alloc memory for log item",
          K(ret), "size", sizeof(ObStorageLogItem));
      } else {
        log_item = new (ptr) ObStorageLogItem();
        if (OB_FAIL(slog_items_.push(log_item))) {
          STORAGE_REDO_LOG(ERROR, "Fail to push log item", K(ret), KP(log_item));
        }
      }
    }
  }

  return ret;
}

int ObStorageLoggerManager::get_tenant_slog_dir(
    const uint64_t tenant_id,
    char (&tenant_slog_dir)[common::MAX_PATH_SIZE])
{
  int ret = OB_SUCCESS;
  int pret = 0;
  pret = snprintf(tenant_slog_dir, MAX_PATH_SIZE, "%s/tenant_%" PRIu64,
                  log_dir_, tenant_id);
  if (pret < 0 || pret >= MAX_PATH_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_REDO_LOG(ERROR, "construct tenant slog path fail", K(ret), K(tenant_id));
  }
  return ret;
}


int ObStorageLoggerManager::get_using_disk_space(int64_t &using_space) const
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  using_space = 0;
  if (OB_FAIL(server_slogger_.get_using_disk_space(using_space))) {
    STORAGE_REDO_LOG(WARN, "fail to get using disk space", K(ret), K(using_space));
  } else if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "unexpected error, omt is nullptr", K(ret), KP(omt));
  } else {
    common::ObSEArray<uint64_t, 8> mtl_tenant_ids;
    omt->get_mtl_tenant_ids(mtl_tenant_ids);
    for (int64_t i = 0; OB_SUCC(ret) && i < mtl_tenant_ids.count(); i++) {
      const uint64_t tenant_id = mtl_tenant_ids.at(i);
      MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
      if (OB_FAIL(guard.switch_to(tenant_id, false))) {
        STORAGE_REDO_LOG(WARN, "fail to switch tenant", K(ret), K(tenant_id));
      } else {
        int64_t tenant_using_size = 0;
        ObStorageLogger *slogger = nullptr;
        if (OB_ISNULL(slogger = MTL(ObStorageLogger*))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_REDO_LOG(WARN, "slogger is null", K(ret), KP(slogger));
        } else if (OB_FAIL(slogger->get_using_disk_space(tenant_using_size))) {
          STORAGE_REDO_LOG(WARN, "fail to get the disk space that slog used", K(ret));
        } else {
          using_space += tenant_using_size;
        }
      }
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObStorageLoggerManager::get_reserved_size(int64_t &reserved_size) const
{
  int ret = OB_SUCCESS;
  reserved_size = 0;
  if (need_reserved_) {
    int64_t used_size = 0;
    if (OB_FAIL(get_using_disk_space(used_size))) {
      STORAGE_REDO_LOG(WARN, "fail to get using size for slog", K(ret));
    } else {
      reserved_size = std::max(0L, RESERVED_DISK_SIZE - used_size);
    }
  }
  return ret;
}

}
}
