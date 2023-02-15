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

#ifndef OCEANBASE_LOGSERVICE_LOG_ITERATOR_
#define OCEANBASE_LOGSERVICE_LOG_ITERATOR_

#include <type_traits>
#include "lib/ob_errno.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/utility/ob_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h" // TO_STRING_KV
#include "log_define.h"                 // LogItemType
#include "log_block_header.h"           // LogBlockHeader
#include "lsn.h"                        // LSN
#include "log_reader_utils.h"           // ReadBuf
#include "log_entry.h"                  // LogEntry
#include "log_group_entry.h"            // LogGroupEntry
#include "log_meta_entry.h"             // LogMetaEntry
#include "log_iterator_storage.h"       // LogIteratorStorage

namespace oceanbase
{
using namespace share;
namespace palf
{
typedef ObFunction<int64_t()> GetModeVersion;
// =========== LogEntryType start =============
enum class LogEntryType
{
  GROUP_ENTRY_HEADER = 0,
  LOG_ENTRY_HEADER = 1,
  LOG_INFO_BLOCK_HEADER = 2,
  LOG_META_ENTRY_HEADER = 3,
  LOG_TYPE_MAX = 4
};
// =========== LogEntryType end =============

// LogIteratorImpl provide the ability to iterate all log entry.
template <class ENTRY>
class LogIteratorImpl
{
public:
  LogIteratorImpl();
  ~LogIteratorImpl();
  int init(const GetModeVersion &mode_version,
           IteratorStorage *log_storage);
  void destroy();
  void reuse();

  // @retval
  //   OB_SUCCESS.
  //   OB_INVALID_DATA.
  //   OB_ITER_END, has iterated to the end of block.
  //   OB_NEED_RETRY, the data in cache is not integrity, and the integrity data has been truncate from disk,
  //                  need read data from storage eagin.(data in cache will not been clean up, therefore,
  //                  user need used a new iterator to read data again)
  //   OB_ERR_OUT_LOWER_BOUND, block has been recycled
  int next(const share::SCN &replayable_point_scn);

  // param[in] replayable point scn, iterator will ensure that no log will return when the log scn is greater than
  //           'replayable_point_scn' and the log is raw write
  // param[out] the min log scn of next log, is's valid only when return value is OB_ITER_END
  // param[out] iterate_end_by_replayable_point, return OB_ITER_END whether caused by replayable_point_scn.
  // @retval
  //   OB_SUCCESS.
  //   OB_INVALID_DATA.
  //   OB_ITER_END, has iterated to the end of block.
  //   OB_NEED_RETRY, the data in cache is not integrity, and the integrity data has been truncate from disk,
  //                  need read data from storage eagin.(data in cache will not been clean up, therefore,
  //                  user need used a new iterator to read data again)
  //   OB_ERR_OUT_LOWER_BOUND, block has been recycled
  //
  int next(const share::SCN &replayable_point_scn,
           share::SCN &next_min_scn,
           bool &iterate_end_by_replayable_point);
  // @retval
  //  OB_SUCCESS
  //  OB_INVALID_DATA
  //  OB_ITER_END
  //  OB_ITER_END
  //  NB: if the last write option success, but the data has been
  //       corrupted, we also regard it as the last write option is
  //       not atomic.
  int get_entry(ENTRY &entry, LSN &lsn, bool &is_raw_write);

  bool is_valid() const;
  bool check_is_the_last_entry();

  LSN get_curr_read_lsn() const;

  TO_STRING_KV(KP(buf_), K_(next_round_pread_size), K_(curr_read_pos), K_(curr_read_buf_start_pos),
      K_(curr_read_buf_end_pos), KPC(log_storage_), K_(curr_entry_is_raw_write), K_(curr_entry_size),
      K_(prev_entry_scn), K_(curr_entry), K_(init_mode_version));

private:
  // @brief get next entry from data storage or cache.
  // @rtval
  //   OB_SUCCESS
  //   OB_INVALID_DATA
  //   OB_ITER_END
  //   OB_ERR_OUT_LOWER_BOUND
  //   OB_NEED_RETRY: means the data has been truncate concurrently
  int get_next_entry_();

  // According to LogEntryType, deserialize different log entry
  // The log format
  // |--------------|------------|-------------
  // | Group Header | Log Header | Log Header |
  // |--------------|------------|-------------
  // @retval
  //   OB_SUCCESS.
  //   OB_BUF_NOT_ENOUGH.
  //   OB_INVALID_DATA, means log entry is not integrity, need check this
  //   log entry whether is the last one.
  int parse_one_entry_();

  template <
    class TMP_ENTRY,
    class ACTUAL_ENTRY>
  int parse_one_specific_entry_(TMP_ENTRY &entry, ACTUAL_ENTRY &actual_entry)
  {
    int ret = OB_SUCCESS;
    const bool matched_type = std::is_same<ACTUAL_ENTRY, TMP_ENTRY>::value;
    int64_t pos = curr_read_pos_;
    if (true == matched_type) {
      if (OB_FAIL(entry.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      }
    } else if (OB_FAIL(actual_entry.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      PALF_LOG(TRACE, "deserialize entry failed", K(ret), KPC(this));
    } else {
      ret = OB_EAGAIN;
      advance_read_lsn_(actual_entry.get_payload_offset());
      PALF_LOG(TRACE, "advance_read_lsn_ payload offset", K(ret), KPC(this), K(actual_entry), "payload offset",
          actual_entry.get_payload_offset());
    }
    return ret;
  }

  template <
   class ACTUAL_ENTRY>
  int parse_one_specific_entry_(LogGroupEntry &entry, ACTUAL_ENTRY &actual_entry)
  {
    int ret = OB_SUCCESS;
    const bool matched_type = std::is_same<ACTUAL_ENTRY, LogGroupEntry>::value;
    int64_t pos = curr_read_pos_;
    if (true == matched_type) {
      if (OB_FAIL(entry.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      } else {
        curr_entry_is_raw_write_ = entry.get_header().is_raw_write();
      }
    } else if (OB_FAIL(actual_entry.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      PALF_LOG(TRACE, "deserialize entry failed", K(ret), KPC(this));
    } else {
      ret = OB_EAGAIN;
      advance_read_lsn_(actual_entry.get_payload_offset());
      PALF_LOG(TRACE, "advance_read_lsn_ payload offset", K(ret), KPC(this), K(actual_entry), "payload offset",
          actual_entry.get_payload_offset());
    }
    return ret;
  }


  int parse_log_block_header_();

  int get_log_entry_type_(LogEntryType &log_entry_type);

  // @retval
  //   OB_SUCCESS.
  //   OB_ITER_END.
  //   OB_ERR_OUT_LOWER_BOUND
  int read_data_from_storage_();

  void advance_read_lsn_(const offset_t step);
  void try_clean_up_cache_();

private:
  static constexpr int MAX_READ_TIMES_IN_EACH_NEXT = 2;
  // In each `next_entry` round, need read data from `LogStorage` directlly,
  // to amortized reading cost, use `read_buf` to cache the last read result.
  //
  // NB: each log must not exceed than 2MB + 4KB.
  //
  // TODO by runlin, we can take `pre-read` to reduce the cost of reading
  // disk in work thread.
  //
  // The layout of `read_buf`
  //  ┌─────────────────────────────────────────────┐
  //  │                   read_buf                  │
  //  └─────────────────────────────────────────────┘
  //   ▲                       ▲                   ▲
  //   │                       │                   │
  //   │                       │                   │
  // start                    curr                end
  offset_t curr_read_pos_;
  offset_t curr_read_buf_start_pos_;
  offset_t curr_read_buf_end_pos_;
  char *buf_;
  int64_t next_round_pread_size_;
  IteratorStorage *log_storage_;
  ENTRY curr_entry_;
  bool curr_entry_is_raw_write_;
  // this fields record the entry size of curr readable entry.
  // NB: when 'curr_entry_size_' is 0, means it's not readable.
  int64_t curr_entry_size_;
  int64_t init_mode_version_;
  // The log scn of prev entry, only effect when 'curr_entry_' is invalid.
  //
  // Add this field is only used for interface next(replayable_point_scn, &next_min_scn)
  //
  // when 'next' return OB_SUCCESS, use 'prev_entry_scn_' to record the log ts of 'curr_entry_'.
  //
  share::SCN prev_entry_scn_;
  GetModeVersion get_mode_version_;
  bool is_inited_;
};

template <class ENTRY>
LogIteratorImpl<ENTRY>::LogIteratorImpl()
  : curr_read_pos_(0),
    curr_read_buf_start_pos_(0),
    curr_read_buf_end_pos_(0),
    buf_(NULL),
    next_round_pread_size_(0),
    log_storage_(NULL),
    curr_entry_(),
    curr_entry_is_raw_write_(false),
    curr_entry_size_(0),
    init_mode_version_(0),
    prev_entry_scn_(),
    is_inited_(false)
{
}

template <class ENTRY>
LogIteratorImpl<ENTRY>::~LogIteratorImpl()
{
  destroy();
}

template <class ENTRY>
int LogIteratorImpl<ENTRY>::init(const GetModeVersion &get_mode_version,
                                 IteratorStorage *log_storage)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    curr_read_pos_ = 0;
    curr_read_buf_start_pos_ = 0;
    curr_read_buf_end_pos_ = 0;
    next_round_pread_size_ = MAX_LOG_BUFFER_SIZE;
    log_storage_ = log_storage;
    curr_entry_size_ = 0;
    init_mode_version_ = PALF_INITIAL_PROPOSAL_ID;
    get_mode_version_ = get_mode_version;
    is_inited_ = true;
    PALF_LOG(TRACE, "LogIteratorImpl init success", K(ret), KPC(this));
  }
  return ret;
}

template <class ENTRY>
void LogIteratorImpl<ENTRY>::reuse()
{
  curr_read_pos_ = 0;
  curr_read_buf_start_pos_ = 0;
  curr_read_buf_end_pos_ = 0;
  next_round_pread_size_ = MAX_LOG_BUFFER_SIZE;
  curr_entry_size_ = 0;
  prev_entry_scn_.reset();
  init_mode_version_ = PALF_INITIAL_PROPOSAL_ID;
}

template <class ENTRY>
void LogIteratorImpl<ENTRY>::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    prev_entry_scn_.reset();
    curr_entry_size_ = 0;
    log_storage_ = NULL;
    next_round_pread_size_ = 0;
    curr_read_buf_end_pos_ = 0;
    curr_read_buf_start_pos_ = 0;
    curr_read_pos_ = 0;
    init_mode_version_ = 0;
  }
}

template <class ENTRY>
bool LogIteratorImpl<ENTRY>::is_valid() const
{
  return 0 != curr_entry_size_;
}

template <class ENTRY>
LSN LogIteratorImpl<ENTRY>::get_curr_read_lsn() const
{
  return log_storage_->get_lsn(curr_read_pos_);
}

// step1. parse one entry from `read_buf`, if buf not enough, read data from disk;
// step2. if parse entry success, according to 'wanted_log_entry_type',
//        advance 'curr_read_lsn_';
// step3. for restarting, if there is an invalid entry, check whether this entry is the
// last entry.
// NB: for restarting, the committed offset of sliding window is invalid.
template <class ENTRY>
int LogIteratorImpl<ENTRY>::get_next_entry_()
{
  int ret = OB_SUCCESS;
  // NB: check need read next enty
  // Assume that read size must greater or equal than 1 in each round.
  if (true == log_storage_->check_iterate_end(curr_read_pos_ + 1)) {
    ret = OB_ITER_END;
    PALF_LOG(TRACE, "get_next_entry_ iterate end, not read new data", K(ret), KPC(this));
  } else {
    // In truncate log case, the 'read_buf_' in LogIteratorStorage will not has
    // integrity data, to avoid read data in dead loop, return OB_NEED_RETRY.
    //
    // For example, the end lsn of this log is 64MB, the start lsn of this log
    // is 62M, the end lsn of 'read_buf_' is 63MB, however, this log has been
    // truncate from disk, and then new log which length is 1.5MB has writen,
    // the log tail is 63.5MB. even if we read data from storage, the data is
    // always not integrity.
    //
    // We can limit the number of disk reads to 2, the reason is that: when
    // we pasrs a PADDING entry with PalfBufferIterator, we need read data
    // from disk again.
    //
    int read_times = 0;
    do {
      int64_t header_size = 0;
      if (OB_SUCC(parse_one_entry_())) {
        curr_entry_size_ = curr_entry_.get_serialize_size();
      } else if (OB_BUF_NOT_ENOUGH == ret) {
        if (OB_FAIL(read_data_from_storage_()) && OB_ITER_END != ret
            && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
          PALF_LOG(WARN, "read_data_from_storage_ failed", K(ret), KPC(this));
        } else if (OB_ITER_END == ret) {
          PALF_LOG(WARN, "has iterate to end of block", K(ret), KPC(this));
        } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          PALF_LOG(WARN, "the block may be unlinked", K(ret), KPC(this));
        } else {
          // read data success, need retry in next round
          read_times++;
          ret = OB_EAGAIN;
          if (read_times > MAX_READ_TIMES_IN_EACH_NEXT) {
            ret = OB_NEED_RETRY;
            PALF_LOG(INFO, "read data from storage too many times, maybe in flashback", K(ret), KPC(this));
            break;
          }
        }
      } else {
      }
    } while (OB_EAGAIN == ret);

    // NB: check curr entry can be readable
    if (OB_SUCC(ret)
        && true == log_storage_->check_iterate_end(curr_read_pos_ + curr_entry_size_)) {
      ret = OB_ITER_END;
      PALF_LOG(TRACE, "get_next_entry_ iterate end, read new data", K(ret), KPC(this));
    }
  }
  return ret;
}

template <class ENTRY>
int LogIteratorImpl<ENTRY>::next(const share::SCN &replayable_point_scn)
{
  share::SCN next_min_scn;
  bool unused_bool = false;
  return next(replayable_point_scn, next_min_scn, unused_bool);
}

template <class ENTRY>
int LogIteratorImpl<ENTRY>::next(const share::SCN &replayable_point_scn,
                                 share::SCN &next_min_scn,
                                 bool &iterate_end_by_replayable_point)
{
  int ret = OB_SUCCESS;
  next_min_scn.reset();
  advance_read_lsn_(curr_entry_size_);
  curr_entry_size_ = 0;
  iterate_end_by_replayable_point = false;

  // NB: when return OB_ITER_END, we need try to clean up cache, and we should not clean up cache only when
  // the log ts of curr entry is greater than 'replayable_point_scn', otherwise, we would return some logs
  // which has been flasback, consider following case:
  // 1. T1, 'replayable_point_scn' is 10, the log ts of curr entry is 15, but there is no flashback option.
  // 2. T2, 'replayable_point_scn' is 10, the logs on disk which the log ts after 10 has been flashbacked, and
  //    return OB_ITER_END because of 'file end lsn'.
  // 3. T3, 'replayable_point_scn' has been advanced to 16, and write several logs on disk, however, the cache
  //    of iterator has not been clean up, the old logs will be returned.
  //
  // 1. T1, 'replayable_point_scn' is 10, the log ts of curr entry is 15, but there is no flashback option.
  // 2. T2, 'replayable_point_scn' is 10, the logs on disk which the log ts after 10 has been flashbacked, and
  //    has append new logs.
  // 3. T3, 'replayable_point_scn' has been advanced to 16, however, the cache of iterator has not been clean up,
  //    the old logs will be returned.
  //
  // Therefore, we should try_clean_up_cache_ in the beginning of each round of next.
  (void) try_clean_up_cache_();
  if (OB_FAIL(get_next_entry_())) {
    // NB: if get_next_entry_ failed, set 'curr_entry_size_' to 0, ensure 'is_valid'
    // return false.
    // NB: if the data which has been corrupted, clean cache.
    if (OB_INVALID_DATA == ret) {
      PALF_LOG(WARN, "read invalid data, need clean cache", K(ret), KPC(this));
      log_storage_->reuse(log_storage_->get_lsn(curr_read_pos_));
      curr_read_buf_end_pos_ = curr_read_buf_start_pos_ = curr_read_pos_ = 0;
      PALF_LOG(WARN, "read invalid data, has clean cache", K(ret), KPC(this));
    }
    if (OB_ITER_END != ret) {
      PALF_LOG(WARN, "get_next_entry_ failed", K(ret), KPC(this));
    }
  } else {
    // NB: when current entry is raw write, and the log scn of current entry is greater than replayable_point_scn
    //     clean up cache when mode version has been changed.
    if (true == curr_entry_is_raw_write_ && curr_entry_.get_scn() > replayable_point_scn) {
      ret = OB_ITER_END;
      iterate_end_by_replayable_point = true;
    }
  }
  // If 'curr_entry_' can be iterate at this round, set 'prev_entry_scn_' to the log ts of 'curr_entry_'.
  if (OB_SUCC(ret)) {
    prev_entry_scn_ = curr_entry_.get_scn();
  }

  // In case of 'next' return OB_ITER_END, we should set 'next_min_scn' which is the out parameter of 'next' to:
  //
  // 1. if 'prev_entry_scn_' is OB_INVALID_TIMESTAMP and 'curr_entry_' is not valid, means that there is no log before
  //    'file end lsn', we should set 'next_min_scn' to OB_INVALID_TIMESTAMP
  //
  // 2. if 'curr_entry_' is valid but not readable, means that there is no readable log before 'file end lsn' or replayable_point_scn,
  //    we should set next_min_scn to std::min(replayable_point_scn, the log ts of 'curr_entry_'), however, replayable_point_scn may
  //    be smaller than 'prev_entry_scn_'(for example, the log entry correspond to'prev_entry_scn_' was writen by APPEND, its' scn
  //    may be greater than replayable_point_scn), we shoud set next_min_scn to std::max(std::min(replayable_point_scn + 1, the log ts
  //    of 'curr_entry_'), 'prev_entry_scn_' + 1).
  //
  // 3. if 'curr_entry_' is not valid and 'prev_entry_scn_' is not OB_INVALID_TIMESTAMP, means there is no log after 'file end lsn'
  //    or 'replayable_point_scn', we should set 'next_min_scn' to prev_entry_scn_ + 1;
  //
  if (OB_ITER_END == ret) {
    if (0 == curr_entry_size_ && !prev_entry_scn_.is_valid()) {
      next_min_scn.reset();
      PALF_LOG(WARN, "there is no readable log, set next_min_scn to OB_INVALID_TIMESTAMP", K(ret), KPC(this));
    } else if (0 != curr_entry_size_) {
      if (!curr_entry_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "unexpected error, curr_entry_size_ is not zero but curr_entry_ is invalid", K(ret), KPC(this));
      } else {
        next_min_scn = MIN(
            (replayable_point_scn.is_valid() ? SCN::plus(replayable_point_scn, 1) : SCN::min_scn()),
            curr_entry_.get_scn());
      }
      next_min_scn = MAX(
          (prev_entry_scn_.is_valid() ? SCN::plus(prev_entry_scn_, 1) : SCN::min_scn()),
          next_min_scn);
      // NB: we need update 'prev_entry_scn_' to the max of 'replayable_point_scn' and 'prev_entry_scn_'
      // when iterate end by replayable point, otherwise, 'next_min_scn' may be smaller than previous
      // result, consider following case:
      // 1. T1, next return OB_ITER_END because of replayable point, the log ts of curr_entry_ is 10,
      //    'prev_entry_scn_' is 5, 'replayable_point_scn' is 9, next_min_scn would be set to 10 because
      //    of 'curr_entry_' is valid.
      // 2. T2, several logs after 9(log_ts) has been truncate, next return OB_ITER_END because of
      //    'file end lsn', 'curr_entry_' is invalid, 'replayable_point_scn' is 9, 'prev_entry_scn_'
      //    is 5, 'next_min_scn' would be set to 5.
      if (replayable_point_scn < curr_entry_.get_scn()) {
        prev_entry_scn_ = MAX(replayable_point_scn, prev_entry_scn_);
      }
    } else {
      // prev_entry_scn_ must be invalid
      next_min_scn = SCN::plus(prev_entry_scn_, 1);
    }
  }
  if (OB_FAIL(ret)) {
    curr_entry_size_ = 0;
  }
  return ret;
}

template<class ENTRY>
int LogIteratorImpl<ENTRY>::get_entry(ENTRY &entry, LSN &lsn, bool &is_raw_write)
{
  int ret = OB_SUCCESS;
  int64_t pos = curr_read_pos_;
  if (0 == curr_entry_size_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(entry.shallow_copy(curr_entry_))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "shallow_copy failed", K(ret), KPC(this));
  } else if (false == entry.check_integrity()) {
    ret = OB_INVALID_DATA;
    PALF_LOG(WARN, "data has been corrupted, attention!!!", K(ret), KPC(this));
  } else {
    lsn = log_storage_->get_lsn(curr_read_pos_);
    is_raw_write = curr_entry_is_raw_write_;
  }
  return ret;
}

// step1. according to magic number, acquire log entry type;
// step2. deserialize ENTRY from 'read_buf_', if buf not enough, return and run in next
// round; step3. check entry integrity, if failed, return OB_INVALID_DATA; step4. if the
// entry type is not 'wanted_log_entry_type', skip the header of this entry. NB: for
// LOG_PADDING, the header size include data_len.
template <class ENTRY>
int LogIteratorImpl<ENTRY>::parse_one_entry_()
{
  int ret = OB_SUCCESS;
  const int MAGIC_NUMBER_SIZE = sizeof(int16_t);
  int16_t magic_number = 0;
  LogEntryType actual_log_entry_type = LogEntryType::LOG_TYPE_MAX;
  do {
    if (OB_FAIL(get_log_entry_type_(actual_log_entry_type))) {
    } else {
      switch (actual_log_entry_type) {
        case LogEntryType::GROUP_ENTRY_HEADER:
          {
              LogGroupEntry entry;
              ret = parse_one_specific_entry_(curr_entry_, entry);
              if (true == entry.is_valid()) {
                curr_entry_is_raw_write_ = entry.get_header().is_raw_write();
              }
            break;
          }
        case LogEntryType::LOG_ENTRY_HEADER:
          {
            LogEntry entry;
            ret = parse_one_specific_entry_(curr_entry_, entry);
            break;
          }
        case LogEntryType::LOG_META_ENTRY_HEADER:
          {
            LogMetaEntry entry;
            ret = parse_one_specific_entry_(curr_entry_, entry);
            break;
          }
        case LogEntryType::LOG_INFO_BLOCK_HEADER:
          {
            ret = parse_log_block_header_();
            if (OB_SUCC(ret)) ret = OB_EAGAIN;
            break;
          }
        default:
          ret = OB_ERR_UNEXPECTED;
          break;
      }
    }
  } while (OB_EAGAIN == ret);

  return ret;
}

template <class ENTRY>
int LogIteratorImpl<ENTRY>::parse_log_block_header_()
{
  int ret = OB_SUCCESS;
  int64_t pos = curr_read_pos_;
  LogBlockHeader actual_entry;
  if (OB_FAIL(actual_entry.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
    PALF_LOG(TRACE, "deserialize entry failed", K(ret), KPC(this));
  } else {
    ret = OB_EAGAIN;
    advance_read_lsn_(MAX_INFO_BLOCK_SIZE);
    PALF_LOG(INFO, "parse_log_block_header_ success", K(ret), KPC(this));
  }
  if (OB_BUF_NOT_ENOUGH == ret) {
    OB_ASSERT(true);
  }
  return ret;
}

template <class ENTRY>
void LogIteratorImpl<ENTRY>::advance_read_lsn_(const offset_t step)
{
  curr_read_pos_ += step;
}

// @brief, start from `curr_read_lsn_`, check whether the entry is valid.
template <class ENTRY>
bool LogIteratorImpl<ENTRY>::check_is_the_last_entry()
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  // TODO by runlin, check whether it's the last block.
  const LSN origin_start_lsn = log_storage_->get_lsn(curr_read_pos_);
  // Because `curr_read_lsn_` is the end of the last valid entry or entry header
  // (log entry header or group entry header), we just only need check from
  // `curr_read_lsn_ + 1`, if there isn't any valid group entry header, this entry
  // is the last entry.
  // 1. reset start position of LogIteratorStorage to 'curr_read_lsn_ + 1';
  // 2. reset current cursor of LogIteratorImpl.
  log_storage_->reuse(log_storage_->get_lsn(curr_read_pos_+1));
  curr_read_buf_end_pos_ = curr_read_buf_start_pos_ = curr_read_pos_ = 0;
  while (OB_SUCC(ret)) {
    using LogEntryHeaderType = typename ENTRY::LogEntryHeaderType;
    LogEntryHeaderType header;
    const int64_t header_size = header.get_serialize_size();
    if (OB_FAIL(read_data_from_storage_()) && OB_ITER_END != ret) {
      PALF_LOG(ERROR, "read_data_from_storage_ failed", K(ret), KPC(this));
    } else if (OB_ITER_END == ret) {
      PALF_LOG(INFO, "has iterate end", K(ret), KPC(this));
      // NB: compatibility
      // If the size of current readable data is smaller than header size, return OB_ITER_END.
    } else if (curr_read_buf_end_pos_ - curr_read_buf_start_pos_ < header.get_serialize_size()) {
      ret = OB_ITER_END;
      PALF_LOG(INFO, "there is no enough data, has iterate end", K(ret), KPC(this), K(header_size));
    } else {
      offset_t round_count = curr_read_buf_end_pos_ - curr_read_pos_;
      while (OB_SUCC(ret) && round_count-- > 0) {
        int64_t pos = curr_read_pos_;
        int64_t valid_buf_len = curr_read_buf_end_pos_ - pos;
        LogEntryType log_entry_type = LogEntryType::LOG_TYPE_MAX;
        if (OB_SUCC(get_log_entry_type_(log_entry_type))
            && OB_SUCC(header.deserialize(buf_, valid_buf_len, pos))
            && true == header.check_header_integrity()) {
          ret = OB_INVALID_DATA;
          PALF_LOG(ERROR, "the block has been corrupted!!!", K(ret), KPC(this), K(header));
          // curr readable buffer can not deserialize a integrity header, need read data from storage
          // again
        } else if (OB_BUF_NOT_ENOUGH == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          ret = OB_SUCCESS;
          advance_read_lsn_(1);
        }
      }
    }
  }

  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  if (OB_ITER_END == ret) {
    PALF_LOG(INFO, "the entry is the last entry", K(ret), KPC(this), K(cost_ts));
    bool_ret = true;
  }
  return bool_ret;
}

template <class ENTRY>
int LogIteratorImpl<ENTRY>::get_log_entry_type_(LogEntryType &log_entry_type)
{
  int ret = OB_SUCCESS;
  int16_t magic_number;
  int64_t pos = curr_read_pos_;
  // ensure that we can get the magic number of each log entry
  if (OB_FAIL(
          serialization::decode_i16(buf_, curr_read_buf_end_pos_, pos, &magic_number))) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (LogGroupEntryHeader::MAGIC == magic_number) {
    log_entry_type = LogEntryType::GROUP_ENTRY_HEADER;
  } else if (LogEntryHeader::MAGIC == magic_number) {
    log_entry_type = LogEntryType::LOG_ENTRY_HEADER;
  } else if (LogMetaEntryHeader::MAGIC == magic_number) {
    log_entry_type = LogEntryType::LOG_META_ENTRY_HEADER;
  } else if (LogBlockHeader::MAGIC == magic_number) {
    log_entry_type = LogEntryType::LOG_INFO_BLOCK_HEADER;
  } else {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

// The layout of `read_buf`
//  ┌─────────────────────────────────────────────┐
//  │                   read_buf                  │
//  └─────────────────────────────────────────────┘
//   ▲                       ▲                   ▲
//   │                       │                   │
//   │                       │                   │
// start                    curr                end
//                             valid_tail_part_size
// When buf not enough to hold a complete log, need read data from 'curr',
// however, to avoid read amplification, we need read data from 'end':
// 1. limit read size into LOG_MAX_LOG_BUFFER_SIZE - valid_tail_part_size.
// 2. memove 'valid_tail_part' to the header of read_buf_.
// 3. read data from 'end' and memcpy these data into read_buf_ + valid_tail_part_size.
//
// NB: when iterate to the end of block, need switch to next block.
template <class ENTRY>
int LogIteratorImpl<ENTRY>::read_data_from_storage_()
{
  int ret = OB_SUCCESS;
  int64_t out_read_size = 0;
  if (OB_FAIL(log_storage_->pread(curr_read_pos_, next_round_pread_size_, buf_,
                                  out_read_size))
      && OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    PALF_LOG(WARN, "IteratorStorage pread failed", K(ret), KPC(this));
  } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
    ret = OB_ITER_END;
    PALF_LOG(TRACE, "IteratorStorage pread failed", K(ret), KPC(this));
  } else {
    curr_read_buf_start_pos_ = 0;
    curr_read_pos_ = 0;
    curr_read_buf_end_pos_ = out_read_size;
  }
  return ret;
}

template <class ENTRY>
void LogIteratorImpl<ENTRY>::try_clean_up_cache_()
{
  const bool matched_type = std::is_same<LogMetaEntry, ENTRY>::value;
  const int64_t current_mode_version = get_mode_version_();
  if (true == matched_type) {
    // do nothing
  } else if (INVALID_PROPOSAL_ID == current_mode_version || init_mode_version_ > current_mode_version) {
    PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "current_mode_version is unexpected", K(current_mode_version), KPC(this));
  } else if (init_mode_version_ < current_mode_version) {
    PALF_LOG_RET(WARN, OB_SUCCESS, "mode version has been changed, need reset cache buf", KPC(this), K(current_mode_version));
    init_mode_version_ = current_mode_version;
    LSN curr_read_lsn = log_storage_->get_lsn(curr_read_pos_);
    log_storage_->reuse(curr_read_lsn);
    curr_read_buf_start_pos_ = 0;
    curr_read_pos_ = 0;
    curr_read_buf_end_pos_ = 0;
  }
}
} // end namespace palf
} // end namespace oceanbase
#endif
