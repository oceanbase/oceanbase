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
//#include "log_define.h"                  // PALF_INITIAL_PROPOSAL_ID
namespace oceanbase
{
namespace share
{
class SCN;
}
namespace palf
{
template <class PalfIteratorStorage, class LogEntryType>
class PalfIterator
{
public:
  PalfIterator() : iterator_storage_(), iterator_impl_(), need_print_error_(true), is_inited_(false) {}
  ~PalfIterator() {destroy();}

  int init(const LSN &start_offset,
           const GetFileEndLSN &get_file_end_lsn,
           ILogStorage *log_storage)
  {
    int ret = OB_SUCCESS;
    auto get_mode_version = []() { return PALF_INITIAL_PROPOSAL_ID; };
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(do_init_(start_offset, get_file_end_lsn, get_mode_version, log_storage))) {
      PALF_LOG(WARN, "PalfIterator init failed", K(ret));
    } else {
      PALF_LOG(TRACE, "PalfIterator init success", K(ret), K(start_offset), KPC(this));
      is_inited_ = true;
    }
    return ret;
  }

  int init(const LSN &start_offset,
           const GetFileEndLSN &get_file_end_lsn,
           const GetModeVersion &get_mode_version,
           ILogStorage *log_storage)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(do_init_(start_offset, get_file_end_lsn, get_mode_version, log_storage))) {
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

  // @brief access next log entry of palf
  // @retval
  //   OB_SUCCESS.
  //   OB_INVALID_DATA.
  //   OB_ITER_END, has iterated to the end of block.
  //   OB_NEED_RETRY, the data in cache is not integrity, and the integrity data has been truncate from disk,
  //                  need read data from storage eagain.(data in cache will not been clean up, therefore,
  //                  user need used a new iterator to read data again)
  //   OB_ERR_OUT_LOWER_BOUND, block has been recycled
  //   OB_PARTIAL_LOG, this replica has not finished flashback, and iterator start lsn is not the header of LogGroupEntry.
  int next()
  {
    int ret = OB_SUCCESS;
    const share::SCN replayable_point_scn = SCN::max_scn();
    bool iterate_end_by_replayable_point = false;
    SCN next_min_scn;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      ret = next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
    }
    return ret;
  }

  // @brief access next log entry of palf
  // parma[in] replayable point scn, iterate will ensure that no log will return when the log scn is greater
  //           than 'replayable_point_scn' and the log is raw write
  // @retval
  //   OB_SUCCESS.
  //   OB_INVALID_DATA.
  //   OB_ITER_END, has iterated to the end of block.
  //   OB_NEED_RETRY, the data in cache is not integrity, and the integrity data has been truncate from disk,
  //                  need read data from storage eagain.(data in cache will not been clean up, therefore,
  //                  user need used a new iterator to read data again)
  //   OB_ERR_OUT_LOWER_BOUND, block has been recycled
  //   OB_PARTIAL_LOG, this replica has not finished flashback, and iterator start lsn is not the header of LogGroupEntry.
  int next(const share::SCN &replayable_point_scn)
  {
    int ret = OB_SUCCESS;
    bool iterate_end_by_replayable_point = false;
    SCN next_min_scn;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      ret = next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
    }
    return ret;
  }

  // @brief access next log entry of palf
  // parma[in] replayable point scn, iterate will ensure that no log will return when the log scn is greater
  //           than 'replayable_point_scn' and the log is raw write
  // param[out] the min log scn of next log, it's valid only when return value is OB_ITER_END
  // param[out] return OB_ITER_END whether casused by replayable_point_scn, it's valid only when return value is OB_ITER_END
  // @retval
  //   OB_SUCCESS.
  //   OB_INVALID_DATA.
  //   OB_ITER_END, has iterated to the end of block.
  //   OB_NEED_RETRY:
  //     1. the data in cache is not integrity, and the integrity data has been truncate from disk,
  //       need read data from storage eagain.
  //     2. during read data from disk, there is a concurrently flashback.
  //   OB_ERR_OUT_LOWER_BOUND, block has been recycled
  //   OB_PARTIAL_LOG, this replica has not finished flashback, and iterator start lsn is not the header of LogGroupEntry.
  int next(const share::SCN &replayable_point_scn,
           share::SCN &next_min_scn,
           bool &iterate_end_by_replayable_point)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point))
        && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator next failed", K(ret), KPC(this));
      print_error_log(ret);
    } else {
      PALF_LOG(TRACE, "PalfIterator next success", K(iterator_impl_), K(ret), KPC(this),
               K(replayable_point_scn), K(next_min_scn), K(iterate_end_by_replayable_point));
    }
    return ret;
  }
  // @brief get log entry from iterator
  // @retval
  //  OB_SUCCESS
  //  OB_INVALID_DATA
  //  OB_ITER_END
  int get_entry(LogEntryType &entry, LSN &lsn)
  {
    int ret = OB_SUCCESS;
    bool unused_is_raw_write = false;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.get_entry(entry, lsn, unused_is_raw_write)) && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator get_entry failed", K(ret), K(entry), K(lsn), KPC(this));
    } else {
      PALF_LOG(TRACE, "PalfIterator get_entry success", K(ret), KPC(this),
          K(entry), K(lsn));
    }
    return ret;
  }
  int get_entry(const char *&buffer, LogEntryType &entry, LSN& lsn)
  {
    int ret = OB_SUCCESS;
    bool unused_is_raw_write = false;
    // OB_ASSERT((std::is_same<LogEntryType, LogGroupEntry>::value) == true);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.get_entry(entry, lsn, unused_is_raw_write)) && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator get_entry failed", K(ret), K(entry), K(lsn), KPC(this));
    } else {
      buffer = entry.get_data_buf() - entry.get_header_size();
      PALF_LOG(TRACE, "PalfIterator get_entry success", K(ret), KPC(this), K(entry));
    }
    return ret;
  }
  int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, LSN &lsn)
  {
    bool unused_is_raw_write = false;
    return get_entry_(buffer, nbytes, scn, lsn, unused_is_raw_write);
  }
  int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, LSN &lsn, bool &is_raw_write)
  {
    return get_entry_(buffer, nbytes, scn, lsn, is_raw_write);
  }
  int get_entry(const char *&buffer, int64_t &nbytes, LSN &lsn, int64_t &log_proposal_id)
  {
    share::SCN unused_scn;
    bool unused_is_raw_write = false;
    return get_entry_(buffer, nbytes, unused_scn, lsn, log_proposal_id, unused_is_raw_write);
  }
  int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, LSN &lsn, int64_t &log_proposal_id,
                bool &is_raw_write)
  {
    return get_entry_(buffer, nbytes, scn, lsn, log_proposal_id, is_raw_write);
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
  void print_error_log(int ret) const
  {
    if (need_print_error_ && (OB_INVALID_DATA == ret || OB_CHECKSUM_ERROR == ret)) {
      PALF_LOG_RET(ERROR, ret, "invalid data or checksum error!!!", KPC(this));
    }
  }
  void set_need_print_error(const bool need_print_error)
  {
    need_print_error_ = need_print_error;
  }
  TO_STRING_KV(K_(iterator_impl));

private:
  int do_init_(const LSN &start_offset,
               const GetFileEndLSN &get_file_end_lsn,
               const GetModeVersion &get_mode_version,
               ILogStorage *log_storage)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (!get_file_end_lsn.is_valid() || !get_mode_version.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(ret), K(start_offset), K(get_file_end_lsn), K(get_mode_version));
    } else if (OB_FAIL(iterator_storage_.init(start_offset, LogEntryType::BLOCK_SIZE, get_file_end_lsn, log_storage))) {
      PALF_LOG(WARN, "IteratorStorage init failed", K(ret));
    } else if (OB_FAIL(iterator_impl_.init(get_mode_version, &iterator_storage_))) {
      PALF_LOG(WARN, "PalfIterator init failed", K(ret));
    } else {
      PALF_LOG(TRACE, "PalfIterator init success", K(ret), K(start_offset), KPC(this));
      is_inited_ = true;
    }
    return ret;
  }

  int get_entry_(const char *&buffer, int64_t &nbytes, share::SCN &scn, LSN &lsn, bool &is_raw_write)
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
      scn = entry.get_scn();
      PALF_LOG(TRACE, "PalfIterator get_entry success", K(iterator_impl_), K(ret), KPC(this), K(entry));
    }
    return ret;
  }

  int get_entry_(const char *&buffer, int64_t &nbytes, share::SCN &scn, LSN &lsn, int64_t &log_proposal_id,
                 bool &is_raw_write)
  {
    int ret = OB_SUCCESS;
    LogEntryType entry;
    OB_ASSERT((std::is_same<LogEntryType, LogGroupEntry>::value) == true);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(iterator_impl_.get_entry(entry, lsn, is_raw_write)) && OB_ITER_END != ret) {
      PALF_LOG(WARN, "PalfIterator get_group_entry failed", K(ret), K(entry), K(lsn), KPC(this));
    } else {
      buffer = entry.get_data_buf() - entry.get_header_size();
      nbytes = entry.get_serialize_size();
      scn = entry.get_scn();
      log_proposal_id = entry.get_header().get_log_proposal_id();
      PALF_LOG(TRACE, "PalfIterator get_group_entry success", K(iterator_impl_), K(ret), KPC(this), K(entry));
    }
    return ret;
  }

private:
  PalfIteratorStorage iterator_storage_;
  LogIteratorImpl<LogEntryType> iterator_impl_;
  bool need_print_error_;
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
