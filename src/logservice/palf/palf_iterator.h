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

#ifndef OCEANBASE_LOGSERVICE_PALF_ITERATOR_
#define OCEANBASE_LOGSERVICE_PALF_ITERATOR_
#include "log_iterator_impl.h"           // LogIteratorImpl
#include "log_iterator_storage.h"        // LogIteratorStorage
namespace oceanbase
{
namespace palf
{
template <class PalfIteratorStorage, class LogEntryType>
class PalfIterator
{
public:
  PalfIterator() : iterator_storage_(), iterator_impl_(), is_inited_(false) {}
  ~PalfIterator() {destroy();}
  int init(const LSN &start_offset,
           ILogStorage *log_storage,
           const GetFileEndLSN &get_file_end_lsn)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(iterator_storage_.init(start_offset, LogEntryType::BLOCK_SIZE, get_file_end_lsn, log_storage))) {
      PALF_LOG(WARN, "IteratorStorage init failed", K(ret));
    } else if (OB_FAIL(iterator_impl_.init(&iterator_storage_))) {
      PALF_LOG(WARN, "PalfIterator init failed", K(ret));
    } else {
      PALF_LOG(TRACE, "PalfIterator init success", K(ret), K(start_offset), KPC(this));
      is_inited_ = true;
    }
    return ret;
  }

  int reuse(const LSN &start_lsn)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      (void)iterator_impl_.reuse();
      (void)iterator_storage_.reuse(start_lsn);
    }
    return ret;
  }
  void destroy()
  {
    if (IS_INIT) {
      is_inited_ = false;
      iterator_impl_.destroy();
      iterator_storage_.destroy();
    }
  }
  int next()
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.next()) && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator next failed", K(ret), K(iterator_impl_));
    } else {
      PALF_LOG(TRACE, "PalfIterator next success", K(iterator_impl_), K(ret), KPC(this));
    }
    return ret;
  }
  int get_entry(LogEntryType &entry, LSN &lsn)
  {
    int ret = OB_SUCCESS;
    bool unused_is_raw_write = false;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.get_entry(entry, lsn, unused_is_raw_write)) && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator get_entry failed", K(ret), K(entry), K(lsn), KPC(this));
    } else {
      PALF_LOG(TRACE, "PalfIterator get_entry success", K(iterator_impl_), K(ret), KPC(this),
          K(entry), K(lsn));
    }
    return ret;
  }
  int get_entry(const char *&buffer, int64_t &nbytes, int64_t &ts, LSN &lsn, bool &is_raw_write)
  {
    int ret = OB_SUCCESS;
    LogEntryType entry;
    OB_ASSERT((std::is_same<LogEntryType, LogEntry>::value) == true);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.get_entry(entry, lsn, is_raw_write)) && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator get_entry failed", K(ret), K(entry), K(lsn), KPC(this));
    } else {
      buffer = entry.get_data_buf();
      nbytes = entry.get_data_len();
      ts = entry.get_log_ts();
      PALF_LOG(TRACE, "PalfIterator get_entry success", K(iterator_impl_), K(ret), KPC(this), K(entry), K(is_raw_write));
    }
    return ret;
  }
  int get_entry(const char *&buffer, int64_t &nbytes, int64_t &ts, LSN &lsn)
  {
    int ret = OB_SUCCESS;
    LogEntryType entry;
    bool unused_is_raw_write = false;
    OB_ASSERT((std::is_same<LogEntryType, LogEntry>::value) == true);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.get_entry(entry, lsn, unused_is_raw_write)) && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator get_entry failed", K(ret), K(entry), K(lsn), KPC(this));
    } else {
      buffer = entry.get_data_buf();
      nbytes = entry.get_data_len();
      ts = entry.get_log_ts();
      PALF_LOG(TRACE, "PalfIterator get_entry success", K(iterator_impl_), K(ret), KPC(this), K(entry));
    }
    return ret;
  }
  bool is_inited() const
  {
    return true == is_inited_;
  }
  bool is_valid() const
  {
    return iterator_impl_.is_valid();
  }
  bool check_is_the_last_entry()
  {
    return iterator_impl_.check_is_the_last_entry();
  }
  LSN get_curr_read_lsn() const
  {
    return iterator_impl_.get_curr_read_lsn();
  }
  TO_STRING_KV(K_(iterator_impl));
private:
  PalfIteratorStorage iterator_storage_;
  LogIteratorImpl<LogEntryType> iterator_impl_;
  bool is_inited_;
};

typedef PalfIterator<MemoryIteratorStorage, LogEntry> MemPalfBufferIterator;
typedef PalfIterator<MemoryIteratorStorage, LogGroupEntry> MemPalfGroupBufferIterator;
typedef PalfIterator<MemoryIteratorStorage, LogMetaEntry> MemPalfMetaBufferIterator;
typedef PalfIterator<DiskIteratorStorage, LogEntry> PalfBufferIterator;
typedef PalfIterator<DiskIteratorStorage, LogGroupEntry> PalfGroupBufferIterator;
typedef PalfIterator<DiskIteratorStorage, LogMetaEntry> PalfMetaBufferIterator;;
}
}
#endif
