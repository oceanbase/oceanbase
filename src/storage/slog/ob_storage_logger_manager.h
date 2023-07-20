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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOGGER_MANAGER_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOGGER_MANAGER_H_

#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log_item.h"
#include "lib/queue/ob_fixed_queue.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "common/log/ob_log_cursor.h"

#define SLOGGERMGR (oceanbase::storage::ObStorageLoggerManager::get_instance())

namespace oceanbase
{
namespace storage
{
class ObStorageLoggerManager final
{
  friend class ObStorageLogger;
public:
  static ObStorageLoggerManager &get_instance();
  int init(
      const char *log_dir,
      const int64_t max_log_file_size,
      const blocksstable::ObLogFileSpec &log_file_spec,
      const bool need_reserved = false);
  void destroy();

  // allocate item and its buffer
  int alloc_item(const int64_t buf_size, ObStorageLogItem *&log_item, const int64_t num);
  // free item and its buffer
  int free_item(ObStorageLogItem *log_item);

  int get_server_slogger(ObStorageLogger *&slogger);
  int get_tenant_slog_dir(const uint64_t tenant_id, char (&tenant_clog_dir)[common::MAX_PATH_SIZE]);
  const char *get_root_dir() { return log_dir_; }
  int get_reserved_size(int64_t &reserved_size) const;

private:
  ObStorageLoggerManager();
  ~ObStorageLoggerManager();
  ObStorageLoggerManager(const ObStorageLoggerManager &) = delete;
  ObStorageLoggerManager &operator = (const ObStorageLoggerManager &) = delete;

  // prepare for log items and their buffers
  int prepare_log_buffers(const int64_t count, const int64_t log_buf_size);
  int prepare_log_items(const int64_t count);

  // allocate buffer
  int alloc_log_buffer(void *&log_buffer);
  // allocate item
  int alloc_log_item(ObStorageLogItem *&log_item);
  // free buffer
  int free_log_buffer(void *log_buffer);
  // free item
  int free_log_item(ObStorageLogItem *log_item);

  int get_using_disk_space(int64_t &using_space) const;

private:
  static constexpr int64_t NORMAL_LOG_ITEM_SIZE = 8 * 1024; //8KB
  static constexpr int64_t MAX_CONCURRENT_ITEM_CNT = 1024;
  static constexpr int64_t RESERVED_DISK_SIZE = 4 * 1024 * 1024 * 1024L; // 4G

  common::ObArenaAllocator allocator_;
  const char *log_dir_;
  int64_t max_log_file_size_;
  bool is_inited_;
  blocksstable::ObLogFileSpec log_file_spec_;

  common::ObFixedQueue<void> log_buffers_;
  common::ObFixedQueue<ObStorageLogItem> slog_items_;
  ObStorageLogger server_slogger_;
  bool need_reserved_;
};

}
}

#endif
