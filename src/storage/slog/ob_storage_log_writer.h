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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_WRITER_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_WRITER_H_

#include "lib/oblog/ob_base_log_writer.h"
#include "lib/lock/ob_mutex.h"
#include "lib/queue/ob_fixed_queue.h"
#include "common/log/ob_log_generator.h"
#include "common/log/ob_log_data_writer.h"
#include "common/storage/ob_io_device.h"
#include "share/redolog/ob_log_file_handler.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "storage/slog/ob_storage_log_write_buffer.h"
#include "storage/slog/ob_storage_log_nop_log.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase
{
namespace storage
{
class ObStorageLogItem;

class ObStorageLogWriter : public common::ObBaseLogWriter
{
public:
  ObStorageLogWriter();
  virtual ~ObStorageLogWriter();

  int init(
      const char *log_dir,
      const int64_t log_file_size,
      const int64_t max_log_size,
      const blocksstable::ObLogFileSpec &log_file_spec,
      const uint64_t tenant_id);
  virtual int start() override;
  virtual void wait() override;
  void destroy();
  common::ObLogCursor get_cur_cursor();

  int delete_log_file(const int64_t file_id);
  int get_using_disk_space(int64_t &using_space) const;

  int start_log(const common::ObLogCursor &start_cursor);

private:
  static const int64_t FLUSH_THREAD_IDLE_INTERVAL_US = 10 * 1000; // 10ms

private:
  int fill_nop_log(ObStorageLogItem *log_item, const int64_t occupied_len);
  int update_log_item_cursor(ObStorageLogItem *log_item, const int64_t begin_offset);
  virtual void process_log_items(common::ObIBaseLogItem **items,
      const int64_t item_cnt, int64_t &finish_cnt) override;
  int batch_process_log_items(common::ObIBaseLogItem **items,
      const int64_t item_cnt, int64_t &finish_cnt);
  int advance_file_id();
  int batch_write_logs(
      common::ObIBaseLogItem **items,
      const int64_t begin,
      const int64_t end,
      int64_t &finish_cnt);
  int aggregate_single_large_log_item(
      ObStorageLogItem *log_item,
      char *&write_buf,
      int64_t &write_len);
  int aggregate_multi_log_items(
      common::ObIBaseLogItem **items,
      const int64_t begin,
      const int64_t end,
      char *&write_buf,
      int64_t &write_len);
  int aggregate_logs(
      common::ObIBaseLogItem **items,
      const int64_t begin,
      const int64_t end,
      char *&write_buf,
      int64_t &write_len);
  int write_logs(
      common::ObIBaseLogItem **items,
      const int64_t begin,
      const int64_t end,
      char *write_buf,
      const int64_t write_len);
  // TODO: optimization: notify after copying to the batch_write_buffer
  int notify_flush(
      common::ObIBaseLogItem **items,
      const int64_t begin,
      const int64_t end);
  bool is_buffer_enough(
			const ObStorageLogItem &log_item,
      const int64_t agg_len);
  bool is_log_file_reach_end(
			const ObStorageLogItem &log_item,
			const int64_t used_len);

  static int check_server_lease();

private:
  class ObSLogWriteRunner : public lib::TGRunnable
  {
  public:
    ObSLogWriteRunner();
    ~ObSLogWriteRunner();

    int init(ObStorageLogWriter *log_writer);
    int start();
    void destroy();
    void stop();
    void wait();
    virtual void run1() override;

  private:
    ObStorageLogWriter *log_writer_;
    int tg_id_;
    bool is_inited_;
  };

private:
  bool is_inited_;
  int64_t flush_seq_;
  int64_t write_align_size_;
  int64_t file_size_;
  // this is next log's offset, but isn't accurate
  // because it won't count the offset on the write buffer
  int64_t write_offset_;
  common::ObLogCursor cursor_;
  common::ObSpinLock cursor_lock_;
  common::ObLogRetryWritePolicy retry_write_policy_;
  common::ObLogWritePolicy log_write_policy_;

  ObStorageLogNopLog nop_log_;
  ObStorageLogParam nop_data_param_;

  common::ObLogFileHandler file_handler_;
  ObStorageLogWriteBuffer batch_write_buf_;
  ObSLogWriteRunner slog_write_runner_;
};


}
}
#endif
