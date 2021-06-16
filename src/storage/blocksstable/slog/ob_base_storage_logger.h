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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_BASE_STORAGE_LOGGER_H_
#define OCEANBASE_BLOCKSSTABLE_OB_BASE_STORAGE_LOGGER_H_

#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/objectpool/ob_pool.h"
#include "lib/oblog/ob_log.h"
#include "lib/lock/ob_drw_lock.h"
#include "common/log/ob_log_generator.h"
#include "common/log/ob_log_data_writer.h"
#include "ob_storage_log_writer.h"
#include "ob_storage_log_replayer.h"

#define SLOGGER (oceanbase::blocksstable::ObBaseStorageLogger::get_instance())
namespace oceanbase {
namespace blocksstable {
// The redo log module of base storage. It is responsible for the write and recovery of all redo logs
// of block sstable.
class ObBaseStorageLogger : public ObStorageLogReplayer {
public:
  static ObBaseStorageLogger& get_instance();
  // NOT thread safe.
  // Init the redo log and do recovery if there is redo logs in log_dir.
  virtual int init(const char* log_dir, const int64_t max_log_file_size, ObISLogFilter* filter_before_parse = nullptr,
      ObISLogFilter* filter_after_parse = nullptr);
  virtual void destroy() override;

  // Thread safe.
  // begin a transaction.
  virtual int begin(const enum common::LogCommand cmd);
  virtual bool is_in_trans() const
  {
    return thread_trans_id_ != 0;
  }
  // Thread safe.
  // write redo log to internal buffer. If the size of internal buffer is too big, it will flush the buffer
  // to disk, then clear the buffer and write log.
  virtual int write_log(const int64_t subcmd, const ObStorageLogAttribute& log_attr, ObIBaseStorageLogEntry& data);
  // Thread safe.
  // commit a transaction, it will flush redo log to disk
  virtual int commit(int64_t& log_seq_num);
  virtual int do_savepoint(int64_t& savepoint);
  virtual int rollback(int64_t savepoint);
  // Thread safe.
  // abort a transaction
  virtual int abort();
  int is_logger_ok(bool& is_ok);
  // Thread safe
  virtual int get_active_cursor(common::ObLogCursor& log_cursor) override;

  void remove_useless_log_file(const int64_t end_file_id);
  // parse log to stream
  int parse_log(const char* log_dir, const int64_t log_file_id, FILE* stream);
  int get_using_disk_space(int64_t& using_space) const;

protected:
  virtual int replay_over() override;

private:
  struct FindMinLogCursor {
    inline void operator()(common::hash::HashMapPair<int64_t, ObStorageLogActiveTrans*>& entry);
    common::ObLogCursor log_cursor_;
  };

  // SLog is written to file in form of ObStorageLogItem. The log item on disk format looks like:
  // |--ObLogEntry--|--ActiveTransEntry--|--NopEntry--|--SwitchEntry--|
  // The buffer size in ActiveTransEntry is calculated by below,
  static const int64_t NORMAL_LOG_ITEM_SIZE = 512 << 10;  // 512KB
  static const int64_t TRANS_ENTRY_BUF_SIZE =
      NORMAL_LOG_ITEM_SIZE - ObStorageLogWriter::LOG_BUF_RESERVED_SIZE - ObStorageLogWriter::LOG_FILE_ALIGN_SIZE;

private:
  ObBaseStorageLogger();
  virtual ~ObBaseStorageLogger();
  int write_log(ObStorageLogActiveTrans& trans_entry, const int64_t subcmd, const ObStorageLogAttribute& log_attr,
      ObIBaseStorageLogEntry& data);
  int flush_log(ObStorageLogActiveTrans& trans_entry, int64_t* log_seq_num = NULL);
  int erase_trans(const int64_t trans_id);

private:
  ObStorageLogWriter log_writer_;
  common::hash::ObHashMap<int64_t, ObStorageLogActiveTrans*> active_trans_;
  common::ObFixedQueue<ObStorageLogActiveTrans> trans_entries_;
  common::DRWLock log_sync_lock_;
  static RLOCAL(int64_t, thread_trans_id_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseStorageLogger);
};

//------------------------------------------------------------------------------------------------------------------

inline void ObBaseStorageLogger::FindMinLogCursor::operator()(
    common::hash::HashMapPair<int64_t, ObStorageLogActiveTrans*>& entry)
{
  if (NULL != entry.second) {
    if (entry.second->start_cursor_.is_valid()) {
      if (!log_cursor_.is_valid() || log_cursor_.newer_than(entry.second->start_cursor_)) {
        log_cursor_ = entry.second->start_cursor_;
      }
    }
  }
}
} /* namespace blocksstable */
} /* namespace oceanbase */

#endif /* OCEANBASE_BLOCKSSTABLE_OB_BASE_STORAGE_LOGGER_H_ */
