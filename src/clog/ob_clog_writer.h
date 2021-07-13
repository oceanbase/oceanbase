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

#ifndef OB_CLOG_WRITER_H_
#define OB_CLOG_WRITER_H_

#include "lib/oblog/ob_base_log_writer.h"
#include "share/redolog/ob_log_file_store.h"
#include "share/ob_bg_thread_monitor.h"
#include "ob_log_file_pool.h"
#include "ob_log_common.h"
#include "ob_log_define.h"
#include "ob_log_cache.h"
#include "ob_info_block_handler.h"
#include "ob_log_direct_reader.h"
#include "ob_log_reader_interface.h"
#include "ob_raw_entry_iterator.h"

namespace oceanbase {
namespace clog {
class ObCLogBaseFileWriter;

enum ObCLogSyncMode { CLOG_NO_SYNC = 0, CLOG_DISK_SYNC = 1, CLOG_MEM_SYNC = 2 };

struct ObCLogWriterCfg {
  ObCLogWriterCfg()
      : type_(INVALID_WRITE_POOL),
        log_file_writer_(NULL),
        log_cache_(NULL),
        info_getter_(NULL),
        tail_ptr_(NULL),
        use_cache_(false),
        base_cfg_()
  {}
  bool is_valid() const
  {
    return (CLOG_WRITE_POOL == type_ || ILOG_WRITE_POOL == type_) && NULL != log_file_writer_ && NULL != log_cache_ &&
           NULL != info_getter_ && NULL != tail_ptr_ && base_cfg_.is_valid();
  }
  TO_STRING_KV(K_(type), KP_(log_file_writer), KP_(log_cache), KP_(info_getter), KP_(tail_ptr), K_(use_cache));
  ObLogWritePoolType type_;
  ObCLogBaseFileWriter* log_file_writer_;
  ObLogCache* log_cache_;
  ObIInfoBlockHandler* info_getter_;
  ObTailCursor* tail_ptr_;
  bool use_cache_;
  common::ObBaseLogWriterCfg base_cfg_;
};

class ObCLogWriter : public common::ObBaseLogWriter {
public:
  ObCLogWriter();
  virtual ~ObCLogWriter();
  int init(ObCLogWriterCfg& clog_cfg);
  int start(file_id_t& file_id, offset_t& offset);
  virtual void destroy();
  int switch_file();
  file_id_t get_file_id() const;
  bool is_disk_error() const;
  int set_is_disk_error();
  int reset_is_disk_error();

protected:
  virtual void process_log_items(common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt);

public:
  // InfoBlock size is fixed(1.875M). It contains a group of hash table, which has the
  // maximum limitation of partition count.
  // In order to avoid serialization failure, the partition count is limited by following:
  //  Partition logs in one batch_buffer mustn't exceed 5000.
  //  A single clog info block size is 40 bytes, so
  //  CLOG_MAX_SWITCH_FILE_LIMIT = (1.875 M )/40 - 5000 = 44000
  //
  //  A single ilog info block size is 40 + 44 = 88 bytes, so
  //  ILOG_MAX_SWITCH_FILE_LIMIT = (1.875 M )/88 - 5000 = 17000
  //
  // Since clog traffic among partitions are different, usually there won't be many partitions
  // have clog write clog simultaneously.
  // An exception is rotate major freeze. Each partition will write START_MEMBERSHIP log
  // when switch leader. In this case, clog file will switch file before write to the EOF.
  static const int64_t CLOG_MAX_SWITCH_FILE_LIMIT = 44000;
  static const int64_t ILOG_MAX_SWITCH_FILE_LIMIT = 17000;

private:
  static const int TASK_NUM = 1024;
  void set_clog_writer_thread_name();
  bool need_switch_file(const uint64_t write_len) const;
  void after_flush(ObICLogItem* item, const int64_t block_meta_len, const int err_code, const uint32_t file_offset,
      int64_t& finish_cnt);
  int inner_switch_file();
  bool is_started_;
  bool is_disk_error_;
  lib::ObMutex file_mutex_;
  ObCLogBaseFileWriter* file_writer_;
  ObLogWritePoolType type_;
  ObLogCache* log_cache_;  // inited by log_engine
  ObIInfoBlockHandler* info_getter_;
  ObTailCursor* tail_;
};

class ObCLogDiskErrorCB : public share::IBGCallback {
public:
  ObCLogDiskErrorCB(ObCLogWriter* host);
  virtual ~ObCLogDiskErrorCB();
  int callback() final;
  virtual void destroy();

private:
  ObCLogWriter* host_;
};

// This class will locate the next write position of log files
template <class Type, class Interface>
class ObLogFileTailLocatorImpl {
public:
  ObLogFileTailLocatorImpl()
  {}
  ~ObLogFileTailLocatorImpl()
  {}

  // locate_tail return successful when
  // 1. find the last block end position
  // 2. file is end, return the next file begin position
  int locate_tail(const int64_t timeout, common::ObILogFileStore* file_store, ObLogDirectReader* reader,
      file_id_t& file_id, offset_t& offset) const
  {
    int ret = common::OB_SUCCESS;
    file_id_t range_min_file_id = common::OB_INVALID_FILE_ID;
    file_id_t range_max_file_id = common::OB_INVALID_FILE_ID;
    // When empty folder, because the file header block occupies DIO_ALIGN_SIZE,
    // so the file start offset begins from CLOG_DIO_ALIGN_SIZE
    ObRawEntryIterator<Type, Interface> iter;

    if (timeout <= 0 || NULL == file_store || NULL == reader) {
      ret = common::OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", K(timeout), KP(file_store), KP(reader));
    } else if (OB_FAIL(file_store->get_file_id_range(range_min_file_id, range_max_file_id)) &&
               common::OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "get file_id range fail", K(ret), KP(file_store));
    } else if (common::OB_ENTRY_NOT_EXIST == ret) {
      CLOG_LOG(INFO, "empty file directory", K(ret));
    } else if (OB_FAIL(iter.init(reader, range_max_file_id, 0, range_max_file_id, timeout))) {
      CLOG_LOG(WARN, "raw log iterator init fail", K(ret), K(range_max_file_id));
    } else {
      Type entry;
      ObReadParam param;
      int64_t persistent_len = 0;
      while (OB_SUCC(iter.next_entry(entry, param, persistent_len))) {
        // iterate to the EOF
      }
      if (common::OB_ITER_END == ret) {
        file_id = param.file_id_;
        offset = param.offset_;
        CLOG_LOG(INFO, "iter next_entry finish", K(ret), K(param), K(entry));  // FIXME
        ret = common::OB_SUCCESS;
      } else {
        CLOG_LOG(ERROR, "get cursor fail", K(ret), K(param), K(entry), K(range_max_file_id));
      }
    }
    return ret;
  }
};

int locate_clog_tail(const int64_t timeout, common::ObILogFileStore* file_store, ObLogDirectReader* reader,
    file_id_t& file_id, offset_t& offset);
int locate_ilog_tail(const int64_t timeout, common::ObILogFileStore* file_store, ObLogDirectReader* reader,
    file_id_t& file_id, offset_t& offset);

typedef int ObLogTailLocator(const int64_t timeout, common::ObILogFileStore* file_store, ObLogDirectReader* reader,
    file_id_t& file_id, offset_t& offset);

}  // namespace clog
}  // namespace oceanbase

#endif /* OB_CLOG_WRITER_H_ */
