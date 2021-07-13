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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_STORAGE_LOG_WRITER_H_
#define OCEANBASE_BLOCKSSTABLE_OB_STORAGE_LOG_WRITER_H_

#include "lib/oblog/ob_base_log_writer.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/queue/ob_fixed_queue.h"
#include "common/log/ob_log_generator.h"
#include "common/log/ob_log_data_writer.h"
#include "share/redolog/ob_log_file_store.h"
#include "ob_storage_log_struct.h"

namespace oceanbase {
namespace blocksstable {
class ObStorageLogItem : public common::ObIBaseLogItem {
public:
  ObStorageLogItem();
  virtual ~ObStorageLogItem();
  int init(char* buf, const int64_t buf_size);
  bool is_valid() const;
  void reset();
  int wait_flush_log(const uint64_t max_wait_time);
  void finish_flush(const int flush_ret);
  virtual const char* get_buf() const
  {
    return buf_;
  }
  virtual char* get_buf()
  {
    return buf_;
  }
  virtual int64_t get_data_len() const
  {
    return len_;
  }
  int set_data_len(const int64_t len);
  int64_t get_buf_size() const
  {
    return buf_size_;
  }
  bool is_local() const
  {
    return is_local_;
  }
  TO_STRING_KV(K_(start_cursor), K_(end_cursor), K_(is_inited), K_(is_local), K_(buf_size), KP_(buf), K_(len),
      K_(flush_finish), K_(flush_ret));

  common::ObLogCursor start_cursor_;
  common::ObLogCursor end_cursor_;

private:
  bool is_inited_;
  bool is_local_;  // indicate whether buf_ is allocated locally or not
  int64_t buf_size_;
  char* buf_;
  int64_t len_;
  common::ObThreadCond flush_cond_;
  bool flush_finish_;
  int flush_ret_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageLogItem);
};

class ObStorageLogWriter : public common::ObBaseLogWriter {
public:
  static const int64_t LOG_FILE_ALIGN_SIZE = 4 * 1024;                   // 4KB
  static const int64_t LOG_BUF_RESERVED_SIZE = 3 * LOG_FILE_ALIGN_SIZE;  // NOP + switch_log
  static const int64_t LOG_ITEM_MAX_LENGTH = 32 << 20;                   // 32MB

  ObStorageLogWriter();
  virtual ~ObStorageLogWriter();

  int init(const char* log_dir, const int64_t log_file_size, const int64_t max_log_size, const int64_t max_trans_cnt);
  void destroy() override;
  int start_log(const common::ObLogCursor& cursor);
  // attention: not thread safe
  int flush_log(
      const common::LogCommand cmd, const ObBaseStorageLogBuffer& log_buffer, common::ObLogCursor& start_cursor);
  common::ObLogCursor get_cur_cursor() const
  {
    return flush_cursor_;
  }

  int delete_log_file(int64_t file_id);
  int get_using_disk_space(int64_t& using_space) const;

  OB_INLINE bool is_ok() const
  {
    return ATOMIC_LOAD(&is_ok_);
  }
  OB_INLINE void set_ok(bool ok)
  {
    ATOMIC_STORE(&is_ok_, ok);
  }

private:
  static const int64_t FLUSH_THREAD_IDLE_INTERVAL_US = 10 * 1000;  // 10ms
  static const int64_t MAX_FLUSH_WAIT_TIME_MS = 60 * 1000;         // 60s

  struct NopLog {
    enum { MAGIC = 0xde6a9de6a901 };
    NopLog() : server_(), ctime_(0), last_ctime_(0)
    {}
    ~NopLog()
    {}
    void reset()
    {
      server_.reset();
      ctime_ = 0;
      last_ctime_ = 0;
    }
    int advance();
    int serialize(char* buf, int64_t limit, int64_t& pos) const;
    int deserialize(const char* buf, int64_t limit, int64_t& pos);
    TO_STRING_KV(K_(server), K_(ctime), K_(last_ctime));

    common::ObAddr server_;
    int64_t ctime_;
    int64_t last_ctime_;
  };

  struct SwitchLog {
    SwitchLog() : next_file_id_(0)
    {}
    ~SwitchLog()
    {}
    void reset()
    {
      next_file_id_ = -1;
    }
    void set_next_file_id(const int64_t next_file_id)
    {
      next_file_id_ = next_file_id;
    }
    int serialize(char* buf, int64_t limit, int64_t& pos) const;
    TO_STRING_KV(K_(next_file_id));
    int64_t next_file_id_;
  };

  struct ObSerializableBuffer {
    ObSerializableBuffer() : buf_(NULL), len_(0)
    {}
    ObSerializableBuffer(char* buf, const int64_t len) : buf_(buf), len_(len)
    {}
    ~ObSerializableBuffer()
    {}
    int serialize(char* buf, int64_t limit, int64_t& pos) const;
    int64_t get_serialize_size() const
    {
      return len_;
    }
    TO_STRING_KV(KP_(buf), K_(len));
    char* buf_;
    int64_t len_;
  };

private:
  int prepare_log_buffers(const int64_t count, const int64_t log_buf_size);
  void destroy_log_buffers();
  int prepare_log_items(const int64_t count);

  int get_log_item(const common::LogCommand cmd, const ObBaseStorageLogBuffer& log_buffer, ObStorageLogItem*& log_item);
  int build_log_item(
      const common::LogCommand cmd, const ObBaseStorageLogBuffer& log_buffer, ObStorageLogItem* log_item);
  template <typename T>
  int fill_log(ObStorageLogItem* log_item, const int64_t reserved_len, const common::LogCommand cmd, const T& data);
  int fill_nop_log(ObStorageLogItem* log_item);
  static int64_t get_align_padding_size(const int64_t x, const int64_t mask)
  {
    return -x & mask;
  }
  static int64_t calc_nop_log_len(int64_t pos, int64_t min_log_size);
  int switch_log_on_demand(ObStorageLogItem* log_item);
  void release_log_item(ObStorageLogItem* log_item);

  virtual void process_log_items(common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt) override;
  int process_logs_in_batch(common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt);
  int prepare_fd(const int64_t file_id);
  int write_logs_local(common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& sync_idx, const int64_t cur_idx,
      char* write_buf, const int64_t write_len);
  int write_and_sync_logs(
      common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& sync_idx, const int64_t cur_idx);
  int aggregate_logs_to_buffer(common::ObIBaseLogItem** items, const int64_t item_cnt, const int64_t sync_idx,
      const int64_t cur_idx, char*& write_buf, int64_t& write_len);

  int advance_log_items(common::ObIBaseLogItem** items, const int64_t item_cnt, const int64_t cur_idx);
  int advance_single_item(const int64_t cur_file_id, ObStorageLogItem& log_item);
  int sync_log(common::ObIBaseLogItem** items, int64_t& sync_index, const int64_t write_index);

private:
  bool is_inited_;
  common::ObFixedQueue<void> log_buffers_;
  int64_t log_buffer_size_;
  common::ObArenaAllocator log_item_allocator_;
  common::ObFixedQueue<ObStorageLogItem> slog_items_;

  // build log context
  int64_t file_size_;
  lib::ObMutex build_log_mutex_;
  common::ObLogCursor build_cursor_;
  NopLog nop_log_;
  char nop_log_buf_[LOG_FILE_ALIGN_SIZE * 2];
  SwitchLog switch_log_;
  char switch_log_buf_[LOG_FILE_ALIGN_SIZE * 2];

  // flush log context
  const char* log_dir_;
  int64_t cur_file_id_;
  common::ObLogCursor write_cursor_;
  common::ObLogCursor flush_cursor_;

  // indicate whether slog writer is ok for outside checking
  bool is_ok_;
  int64_t write_failed_times_;

  common::ObILogFileStore* file_store_;
  char* batch_write_buf_;
  int64_t batch_write_len_;
  int64_t batch_limit_size_;
};

template <typename T>
int ObStorageLogWriter::fill_log(
    ObStorageLogItem* log_item, const int64_t reserved_len, const common::LogCommand cmd, const T& data)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "not init", K(ret));
  } else if (NULL == log_item || reserved_len < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid arguments", K(ret), KP(log_item), K(reserved_len));
  } else {
    common::ObLogEntry entry;
    int64_t entry_pos = log_item->get_data_len();
    int64_t data_start_pos = log_item->get_data_len() + entry.get_serialize_size();
    int64_t data_pos = data_start_pos;
    const int64_t buf_size = log_item->get_buf_size() - reserved_len;
    if (OB_FAIL(data.serialize(log_item->get_buf(), buf_size, data_pos))) {
      if (entry.get_serialize_size() + data.get_serialize_size() > buf_size) {
        ret = common::OB_LOG_TOO_LARGE;
        STORAGE_REDO_LOG(WARN,
            "log too large",
            K(ret),
            K(buf_size),
            "log_size",
            (data.get_serialize_size() + entry.get_serialize_size()));
      } else {
        ret = common::OB_BUF_NOT_ENOUGH;
      }
    } else if (OB_FAIL(build_cursor_.next_entry(
                   entry, cmd, log_item->get_buf() + data_start_pos, data_pos - data_start_pos))) {
      STORAGE_REDO_LOG(ERROR, "next_entry failed", K(ret), K_(build_cursor));
    } else if (OB_FAIL(entry.serialize(log_item->get_buf(), entry_pos + entry.get_serialize_size(), entry_pos))) {
      STORAGE_REDO_LOG(ERROR, "entry serialize failed", K(ret), "buf_size", entry.get_serialize_size(), K(entry_pos));
    } else if (OB_FAIL(build_cursor_.advance(entry))) {
      STORAGE_REDO_LOG(ERROR, "build_cursor advance failed", K(ret), K_(build_cursor), K(entry));
    } else if (OB_FAIL(log_item->set_data_len(data_pos))) {
      STORAGE_REDO_LOG(ERROR, "set data len fail", K(ret), K(data_pos));
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_BLOCKSSTABLE_OB_STORAGE_LOG_WRITER_H_
