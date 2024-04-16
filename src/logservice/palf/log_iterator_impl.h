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
#include "lib/alloc/alloc_assist.h"
#include "lib/utility/ob_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"     // TO_STRING_KV
#include "share/ob_errno.h"                 // OB_PARTIAL_LOG
#include "log_define.h"                     // LogItemType
#include "log_block_header.h"               // LogBlockHeader
#include "lsn.h"                            // LSN
#include "log_reader_utils.h"               // ReadBuf
#include "log_entry.h"                      // LogEntry
#include "log_group_entry.h"                // LogGroupEntry
#include "log_meta_entry.h"                 // LogMetaEntry
#include "log_iterator_storage.h"           // LogIteratorStorage
#include "log_checksum.h"                   // LogChecksum

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

enum class IterateEndReason {
  DUE_TO_REPLAYABLE_POINT_SCN_LOG_GROUP_ENTRY = 0,
  DUE_TO_REPLAYABLE_POINT_SCN_LOG_ENTRY = 1,
  DUE_TO_FILE_END_LSN_NOT_READ_NEW_DATA = 2,
  DUE_TO_FILE_END_LSN_READ_NEW_DATA = 3,
  MAX_TYPE = 4
};
struct IterateEndInfo {
  IterateEndInfo()
  {
    reset();
  }
  ~IterateEndInfo()
  {
    reset();
  }
  void reset()
  {
    reason_ = IterateEndReason::MAX_TYPE;
    log_scn_.reset();
  }
  bool is_valid() const
  {
    return IterateEndReason::MAX_TYPE != reason_
           && log_scn_.is_valid();
  }
  bool is_iterate_end_by_replayable_point_scn() const
  {
    return IterateEndReason::DUE_TO_REPLAYABLE_POINT_SCN_LOG_ENTRY == reason_
           || IterateEndReason::DUE_TO_REPLAYABLE_POINT_SCN_LOG_GROUP_ENTRY == reason_;
  }
  IterateEndReason reason_;
  SCN log_scn_;
  TO_STRING_KV(K_(reason), K_(log_scn));
};

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
  //   OB_ITER_END
  //       - has iterated to the end of block.
  //   OB_NEED_RETRY
  //      - the data in cache is not integrity, and the integrity data has been truncate from disk,
  //        need read data from storage again.(data in cache will not been clean up, therefore,
  //        user need used a new iterator to read data again)
  //      - if the end_lsn get from get_file_end_lsn is smaller than 'log_tail_' of LogStorage, and it's
  //        not the exact boundary of LogGroupEntry(for PalfGroupeBufferIterator, or LogEntry for PalfBufferIterator),
  //        OB_NEED_RETRY may be return.
  //      - if read_data_from_storage_ is concurrent with the last step of flashback, opening last block on disk may be failed
  //        due to rename, return OB_NEED_RETRY in this case.(TODO by runlin: retry by myself)
  //   OB_ERR_OUT_LOWER_BOUND
  //      - block has been recycled
  //   OB_CHECKSUM_ERROR
  //      - the accumulate checksum calc by accum_checksum_ and the data checksum of LogGroupEntry is not
  //        same as the accumulate checksum of LogGroupEntry
  //   OB_PARTIAL_LOG
  //      - this replica has not finished flashback, and iterator start lsn is not the header of LogGroupEntry.
  int next(const share::SCN &replayable_point_scn, LogIOContext &io_ctx);

  // param[in] replayable point scn, iterator will ensure that no log will return when the log scn is greater than
  //           'replayable_point_scn' and the log is raw write
  // param[out] the min log scn of next log, is's valid only when return value is OB_ITER_END
  // param[out] iterate_end_by_replayable_point, return OB_ITER_END whether caused by replayable_point_scn.
  //   OB_SUCCESS.
  //   OB_INVALID_DATA.
  //   OB_ITER_END
  //       - has iterated to the end of block.
  //   OB_NEED_RETRY
  //      - the data in cache is not integrity, and the integrity data has been truncate from disk,
  //        need read data from storage again.(data in cache will not been clean up, therefore,
  //        user need used a new iterator to read data again)
  //      - if the end_lsn get from get_file_end_lsn is smaller than 'log_tail_' of LogStorage, and it's
  //        not the exact boundary of LogGroupEntry(for PalfGroupeBufferIterator, or LogEntry for PalfBufferIterator),
  //        OB_NEED_RETRY may be return.
  //      - if read_data_from_storage_ is concurrent with the last step of flashback, opening last block on disk may be failed
  //        due to rename, return OB_NEED_RETRY in this case.(TODO by runlin: retry by myself)
  //   OB_ERR_OUT_LOWER_BOUND
  //      - block has been recycled
  //   OB_CHECKSUM_ERROR
  //      - the accumulate checksum calc by accum_checksum_ and the data checksum of LogGroupEntry is not
  //        same as the accumulate checksum of LogGroupEntry
  //   OB_PARTIAL_LOG
  //      - this replica has not finished flashback, and iterator start lsn is not the header of LogGroupEntry.
  int next(const share::SCN &replayable_point_scn,
           share::SCN &next_min_scn,
           bool &iterate_end_by_replayable_point,
           LogIOContext &io_ctx);
  // @retval
  //  OB_SUCCESS
  //  OB_INVALID_DATA
  //  OB_ITER_END
  //  NB: if the last write option success, but the data has been
  //       corrupted, we also regard it as the last write option is
  //       not atomic.
  int get_entry(ENTRY &entry, LSN &lsn, bool &is_raw_write);

  bool is_valid() const;
  bool check_is_the_last_entry(LogIOContext &io_ctx);

  LSN get_curr_read_lsn() const;

  TO_STRING_KV(KP(buf_), K_(next_round_pread_size), K_(curr_read_pos), K_(curr_read_buf_start_pos),
      K_(curr_read_buf_end_pos), KPC(log_storage_), K_(curr_entry_is_raw_write), K_(curr_entry_size),
      K_(prev_entry_scn), K_(curr_entry), K_(init_mode_version), K_(accumulate_checksum),
      K_(curr_entry_is_padding), K_(padding_entry_size), K_(padding_entry_scn));

private:
  // @brief get next entry from data storage or cache.
  // @retval
  //   OB_SUCCESS
  //   OB_INVALID_DATA
  //   OB_ITER_END
  //   OB_ERR_OUT_LOWER_BOUND
  //   OB_NEED_RETRY: means the data has been truncate concurrently
  //   OB_PARTIAL_LOG: this replica has not finished flashback, and iterator start lsn
  //                   is not the header of LogGroupEntry.
  int get_next_entry_(const SCN &replayable_point_scn,
                      IterateEndInfo &info,
                      LogIOContext &io_ctx);

  // According to LogEntryType, deserialize different log entry
  // The log format
  // |--------------|------------|-------------
  // | Group Header | Log Header | Log Header |
  // |--------------|------------|-------------
  // @retval
  //   OB_SUCCESS.
  //   OB_BUF_NOT_ENOUGH.
  //   OB_INVALID_DATA
  //      -- means log entry is not integrity, need check this log entry whether is the last one.
  //   OB_CHECKSUM_ERROR
  //      -- means accumulate checksum is not matched.
  //   OB_ITER_END
  //      -- means log entry is iterated end by replayable_point_scn
  //   OB_PARTIAL_LOG
    //    -- this replica has not finished flashback, and iterator start lsn is not the header of
    //       LogGroupEntry.
  int parse_one_entry_(const SCN &replayable_point_scn,
                       IterateEndInfo &info);

  int parse_meta_entry_()
  {
    int ret = OB_SUCCESS;
    const bool matched_type = std::is_same<LogMetaEntry, ENTRY>::value;
    int64_t pos = curr_read_pos_;
    if (true == matched_type) {
      if (OB_FAIL(curr_entry_.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      }
    } else {
      ret = OB_INVALID_DATA;
      PALF_LOG(ERROR, "parse LogMetaEntry failed, unexpected error", K(ret), KPC(this));
    }
    return ret;
  }

  int parse_log_entry_(const SCN &replayable_point_scn,
                       IterateEndInfo &info)
  {
    int ret = OB_SUCCESS;
    const bool matched_type = std::is_same<LogEntry, ENTRY>::value;
    int64_t pos = curr_read_pos_;
    if (true == matched_type) {
      if (curr_entry_is_padding_ && OB_FAIL(construct_padding_log_entry_(pos, padding_entry_size_))) {
        PALF_LOG(WARN, "construct_padding_log_entry_ failed", KPC(this));
      } else if (OB_FAIL(curr_entry_.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      } else if (curr_entry_is_raw_write_ && curr_entry_.get_scn() > replayable_point_scn) {
        ret = OB_ITER_END;
        info.log_scn_ = curr_entry_.get_scn();
        info.reason_ = IterateEndReason::DUE_TO_REPLAYABLE_POINT_SCN_LOG_ENTRY;
        PALF_LOG(TRACE, "iterate end by replayable_point", KPC(this), K(replayable_point_scn), K(info));
      }
    } else {
      ret = OB_PARTIAL_LOG;
      PALF_LOG(WARN, "parse LogEntry failed, may be in flashback mode and this replica has not finished flashback",
               KPC(this), K(replayable_point_scn), K(info));
    }
    return ret;
  }

  // When entry in file is LogGroupEntry, handle it specifically.
  // 1. for raw write LogGroupEntry, if it's controlled by replayable_point_scn, no need update
  //    'curr_read_pos_', 'accum_checksum_', because this log may be flashback.
  // 2. for append LogGroupEntry, handle it normally.
  int parse_log_group_entry_(const SCN &replayable_point_scn,
                             IterateEndInfo &info)
  {
    int ret = OB_SUCCESS;
    const bool matched_type = std::is_same<LogGroupEntry, ENTRY>::value;
    LogGroupEntry actual_entry;
    int64_t pos = curr_read_pos_;
    if (true == matched_type) {
      if (OB_FAIL(curr_entry_.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      } else if (OB_FAIL(handle_each_log_group_entry_(curr_entry_, replayable_point_scn, info))) {
        if (OB_ITER_END != ret) {
          PALF_LOG(WARN, "handle_each_log_group_entry_ failed", KPC(this), K(info), K(replayable_point_scn));
        } else {
          PALF_LOG(TRACE, "handle_each_log_group_entry_ failed", KPC(this), K(info), K(replayable_point_scn));
        }
      }
    } else if (OB_FAIL(actual_entry.deserialize(buf_, curr_read_buf_end_pos_, pos))) {
      PALF_LOG(TRACE, "deserialize entry failed", K(ret), KPC(this));
    } else if (OB_FAIL(handle_each_log_group_entry_(actual_entry, replayable_point_scn, info))) {
      if (OB_ITER_END != ret) {
        PALF_LOG(WARN, "handle_each_log_group_entry_ failed", KPC(this), K(actual_entry), K(info), K(replayable_point_scn));
      } else {
        PALF_LOG(TRACE, "handle_each_log_group_entry_ failed", KPC(this), K(actual_entry), K(info), K(replayable_point_scn));
      }
    } else {
      ret = OB_EAGAIN;
      advance_read_lsn_(actual_entry.get_payload_offset());
      PALF_LOG(TRACE, "advance_read_lsn_ payload offset", K(ret), KPC(this), K(actual_entry), "payload offset",
          actual_entry.get_payload_offset(), K(info), K(replayable_point_scn));
    }
    return ret;
  }

  int get_log_entry_type_(LogEntryType &log_entry_type);

  // @retval
  //   OB_SUCCESS.
  //   OB_ITER_END.
  //   OB_ERR_OUT_LOWER_BOUND
  int read_data_from_storage_(LogIOContext &io_ctx);

  void advance_read_lsn_(const offset_t step);
  void try_clean_up_cache_();

  template <class T>
  // when T is not LogGroupEntry, no need do anything
  int handle_each_log_group_entry_(const T&entry,
                                   const SCN &replayable_point_scn,
                                   IterateEndInfo &info)
  {
    PALF_LOG(TRACE, "T is not LogGroupEntry, do no thing", K(entry));
    return OB_SUCCESS;
  }

  template <>
  // When T is LogGroupEntry, need do:
  // 1. check accumulate checksum:
  //   - if accumulate checksum is not match, return OB_CHECKSUM_ERROR
  //   - if data checksum is not match, return OB_INVALID_DATA
  // 2. check this entry whether need control by 'replayable_point_scn':
  //   - if control by 'replayable_point_scn', return OB_ITER_END, and don't modify
  //     several fields in LogIteratorImpl('curr_read_pos_', 'curr_entry_is_raw_write_')
  //   - if not control by 'replayable_point_scn', return OB_SUCCESS.
  int handle_each_log_group_entry_(const LogGroupEntry&entry,
                                   const SCN &replayable_point_scn,
                                   IterateEndInfo &info)
  {
    int ret = OB_SUCCESS;
    bool curr_entry_is_raw_write = entry.get_header().is_raw_write();
    int64_t new_accumulate_checksum = -1;
    PALF_LOG(TRACE, "T is LogGroupEntry", K(entry));
    if (OB_FAIL(verify_accum_checksum_(entry, new_accumulate_checksum))) {
      PALF_LOG(WARN, "verify_accum_checksum_ failed", K(ret), KPC(this), K(entry));
    // NB: when current entry is raw write, and the log scn of current entry is greater than
    //     replayable_point_scn, this log may be clean up, therefore we can not update several fields of
    //     LogIteratorImpl, return OB_ITER_END directly, otherwise, we may not parse new LogGroupEntryHeader
    //     after flashback position, this will cause one log which is append, but control by replayable_point_scn.
    //
    // NB: we need check the min scn of LogGroupEntry whether has been greater than
    //     replayable_point_scn:
    //     - if LogGroupEntry has two LogEntry, the scn of them are 10, 15 respectively,
    //       replayable_point_scn is 12. in this case, we can read first LogEntry, and
    //       we can update several fields like 'curr_entry_is_raw_write_', 'accum_checksum_'
    //       and the others('curr_read_pos_'...). when the flashback scn is 12, the LogEntry
    //       after 10 will be truncated, the new LogGroupEntry will be generated, meanwhile,
    //       we will advanced 'curr_read_pos_' to the end of first LogEntry and read new LogGroupEntry
    //       correctly.
    //     - if LogGroupEntry has one LogEntry, the scn of it is 13, the several fields are
    //       not been updated because of it's controlled by replayable_point_scn, when the flashback
    //       scn is 12, we don't need rollback these fields.
    //
    // NB: for PalfGroupBufferIterator, we should use max scn to control replay and use min scn
    //     as the log_scn_ of info. consider that, replayable_point_scn is 12, the min scn of group log
    //     is 7 and max scn of group 15, we should not return this log. meanwhile, we should use
    //     scn 7 to update next_min_scn.
    } else if (true == curr_entry_is_raw_write) {
      SCN min_scn;
      bool is_group_iterator = std::is_same<ENTRY, LogGroupEntry>::value;
      if (OB_FAIL(entry.get_log_min_scn(min_scn))) {
        PALF_LOG(ERROR, "get_log_min_scn failed", K(ret), KPC(this), K(min_scn),
            K(entry), K(replayable_point_scn));
      } else if ((is_group_iterator && entry.get_scn() > replayable_point_scn)
                 || (!is_group_iterator && min_scn > replayable_point_scn)) {
        info.log_scn_ = min_scn;
        info.reason_ = IterateEndReason::DUE_TO_REPLAYABLE_POINT_SCN_LOG_GROUP_ENTRY;
        ret = OB_ITER_END;
        PALF_LOG(TRACE, "iterate end by replayable_point", K(ret), KPC(this), K(min_scn),
            K(entry), K(replayable_point_scn), K(info), K(is_group_iterator));
      } else {
      }
    }
    if (OB_SUCC(ret)) {
      curr_entry_is_raw_write_ = entry.get_header().is_raw_write();
      accumulate_checksum_ = new_accumulate_checksum;
      // To support get PADDING entry, need record the meta info of PADDING entry
      set_padding_info_(entry);
    }
    return ret;
  }
  // @brief: accumulate checksum verify, only verify checksum when accum_checksum_ is not -1.
  // ret val:
  //    OB_SUCCESS
  //    OB_CHECKSUM_ERROR
  int verify_accum_checksum_(const LogGroupEntry &entry,
                             int64_t &new_accumulate_checksum);

  int construct_padding_log_entry_(const int64_t memset_start_pos,
                                   const int64_t padding_log_entry_len);
  bool is_padding_entry_end_lsn_(const LSN &lsn)
  {
    return 0 == lsn_2_offset(lsn, PALF_BLOCK_SIZE);
  }

  void set_padding_info_(const LogGroupEntry &entry)
  {
    curr_entry_is_padding_ = entry.get_header().is_padding_log();
    padding_entry_size_ = entry.get_header().get_data_len();
    padding_entry_scn_ = entry.get_header().get_max_scn();
  }

  void reset_padding_info_()
  {
    curr_entry_is_padding_ = false;
    padding_entry_size_ = 0;
    padding_entry_scn_.reset();
  }

  bool need_clean_cache_(const int ret) const
  {
    // NB: several storage devices cannot guarantee linear consistency reading in scenarios where 4K is overwritten,
    // therefore, we should clean the cache of IteratorStorage, and re-read data from disk in next time.
    return OB_INVALID_DATA == ret || OB_CHECKSUM_ERROR == ret;
  }
private:
static constexpr int MAX_READ_TIMES_IN_EACH_NEXT = 2;
  // In each `next_entry` round, need read data from `LogStorage` directly,
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
  int64_t accumulate_checksum_;
  // To support get PADDING ENTRY, add following fields:
  int64_t curr_entry_is_padding_;
  int64_t padding_entry_size_;
  SCN padding_entry_scn_;
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
    accumulate_checksum_(-1),
    curr_entry_is_padding_(false),
    padding_entry_size_(0),
    padding_entry_scn_(),
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
    curr_entry_.reset();
    curr_entry_is_raw_write_ = false;
    curr_entry_size_ = 0;
    init_mode_version_ = PALF_INITIAL_PROPOSAL_ID;
    get_mode_version_ = get_mode_version;
    prev_entry_scn_.reset();
    accumulate_checksum_ = -1;
    curr_entry_is_padding_ = false;
    padding_entry_size_ = 0;
    padding_entry_scn_.reset();
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
  curr_entry_.reset();
  curr_entry_is_raw_write_ = false;
  curr_entry_size_ = 0;
  init_mode_version_ = PALF_INITIAL_PROPOSAL_ID;
  prev_entry_scn_.reset();
  accumulate_checksum_ = -1;
  curr_entry_is_padding_ = false;
  padding_entry_size_ = 0;
  padding_entry_scn_.reset();
}

template <class ENTRY>
void LogIteratorImpl<ENTRY>::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    padding_entry_scn_.reset();
    padding_entry_size_ = 0;
    curr_entry_is_padding_ = false;
    accumulate_checksum_ = -1;
    prev_entry_scn_.reset();
    init_mode_version_ = PALF_INITIAL_PROPOSAL_ID;
    curr_entry_size_ = 0;
    curr_entry_is_raw_write_ = false;
    curr_entry_.reset();
    log_storage_ = NULL;
    next_round_pread_size_ = 0;
    buf_ = NULL;
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
int LogIteratorImpl<ENTRY>::get_next_entry_(const SCN &replayable_point_scn,
                                            IterateEndInfo &info,
                                            LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  // NB: check need read next enty
  // Assume that read size must greater or equal than 1 in each round.
  if (true == log_storage_->check_iterate_end(curr_read_pos_ + 1)) {
    ret = OB_ITER_END;
    info.reason_ = IterateEndReason::DUE_TO_FILE_END_LSN_NOT_READ_NEW_DATA;
    info.log_scn_.reset();
    PALF_LOG(TRACE, "get_next_entry_ iterate end, not read new data", K(ret), KPC(this), K(info));
  } else {
    // In truncate log case, the 'read_buf_' in LogIteratorStorage will not has
    // integrity data, to avoid read data in dead loop, return OB_NEED_RETRY.
    //
    // For example, the end lsn of this log is 64MB, the start lsn of this log
    // is 62M, the end lsn of 'read_buf_' is 63MB, however, this log has been
    // truncate from disk, and then new log which length is 1.5MB has written,
    // the log tail is 63.5MB. even if we read data from storage, the data is
    // always not integrity.
    //
    // We can limit the number of disk reads to 2, the reason is that: when
    // we parse a PADDING entry with PalfBufferIterator, we need read data
    // from disk again.
    //
    int read_times = 0;
    do {
      int64_t header_size = 0;
      if (OB_SUCC(parse_one_entry_(replayable_point_scn, info))) {
        curr_entry_size_ = curr_entry_.get_serialize_size();
      } else if (OB_BUF_NOT_ENOUGH == ret) {
        if (OB_FAIL(read_data_from_storage_(io_ctx)) && OB_ITER_END != ret
            && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
          PALF_LOG(WARN, "read_data_from_storage_ failed", K(ret), KPC(this));
        } else if (OB_ITER_END == ret) {
          info.reason_ = IterateEndReason::DUE_TO_FILE_END_LSN_NOT_READ_NEW_DATA;
          info.log_scn_.reset();
          PALF_LOG(WARN, "has iterate to end of block", K(ret), KPC(this), K(info));
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

    // NB: check curr entry can be readable by file end lsn.
    if (OB_SUCC(ret)
        && true == log_storage_->check_iterate_end(curr_read_pos_ + curr_entry_size_)) {
      ret = OB_ITER_END;
      info.reason_ = IterateEndReason::DUE_TO_FILE_END_LSN_READ_NEW_DATA;
      info.log_scn_ = curr_entry_.get_scn();
      PALF_LOG(WARN, "get_next_entry_ iterate end, read new data", K(ret), KPC(this), K(info), K(replayable_point_scn));
    }
  }
  return ret;
}

template <class ENTRY>
int LogIteratorImpl<ENTRY>::next(const share::SCN &replayable_point_scn, LogIOContext &io_ctx)
{
  share::SCN next_min_scn;
  bool unused_bool = false;
  return next(replayable_point_scn, next_min_scn, unused_bool, io_ctx);
}

template <class ENTRY>
int LogIteratorImpl<ENTRY>::next(const share::SCN &replayable_point_scn,
                                 share::SCN &next_min_scn,
                                 bool &iterate_end_by_replayable_point,
                                 LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  next_min_scn.reset();
  advance_read_lsn_(curr_entry_size_);
  curr_entry_size_ = 0;
  iterate_end_by_replayable_point = false;
  IterateEndInfo info;

  // NB: when return OB_ITER_END, we need try to clean up cache, and we should clean up cache only when
  // the log ts of curr entry is greater than 'replayable_point_scn', otherwise, we would return some logs
  // which has been flashback, consider following case:
  // 1. T1, 'replayable_point_scn' is 10, the log ts of curr entry is 15, but there is no flashback option.(no any bad effect)
  // 2. T2, 'replayable_point_scn' is 10, the logs on disk which the log ts after 10 has been flashback, and
  //    return OB_ITER_END because of 'file end lsn'.(no any bad effect)
  // 3. T3, 'replayable_point_scn' has been advanced to 16, and write several logs on disk, however, the cache
  //    of iterator has not been clean up, the old logs will be returned.(bad effect)
  //
  // Therefore, we should try_clean_up_cache_ in the beginning of each round of next.
  (void) try_clean_up_cache_();
  if (!replayable_point_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(replayable_point_scn), KPC(this));
  } else if (OB_FAIL(get_next_entry_(replayable_point_scn, info, io_ctx))) {
    // NB: if the data which has been corrupted or accum_checksum_ is not match, clean cache.
    if (need_clean_cache_(ret)) {
      PALF_LOG(WARN, "read invalid data, need clean cache, maybe storage device cann't guarantee linear consistency reading",
               K(ret), KPC(this));
    // NB: several storage devices cannot guarantee linear consistency reading in scenarios where 4K is overwritten,
    // therefore, we should clean the cache of IteratorStorage, and re-read data from disk in next time.
      log_storage_->reuse(log_storage_->get_lsn(curr_read_pos_));
      curr_read_buf_end_pos_ = curr_read_buf_start_pos_ = curr_read_pos_ = 0;
    }
    if (OB_ITER_END != ret) {
      PALF_LOG(WARN, "get_next_entry_ failed", K(ret), KPC(this));
    }
    PALF_LOG(TRACE, "get_next_entry_ failed", K(ret), KPC(this), K(info), K(replayable_point_scn));
    iterate_end_by_replayable_point = info.is_iterate_end_by_replayable_point_scn();
  }
  // If 'curr_entry_' can be iterate at this round, set 'prev_entry_scn_' to the log ts of 'curr_entry_'.
  if (OB_SUCC(ret)) {
    prev_entry_scn_ = curr_entry_.get_scn();
  }

  // if 'curr_entry_' is not readable, we should set 'next_min_scn' to the scn of 'prev_entry_' + 1,
  // and if 'curr_entry_' is not readable due to replayable_point_scn, we should set 'next_min_scn'
  // to replayable_point_scn + 1
  //
  // In case of 'next' return OB_ITER_END, we should set 'next_min_scn' which is the out parameter of 'next' to:
  //
  // 1. if 'curr_entry_' iterate end by replayable point scn:
  //    we should set next_min_scn to std::min(replayable_point_scn+1, the log scn of 'curr_entry_'),
  //    however, replayable_point_scn may be smaller than 'prev_entry_scn_'(for example, the log
  //    entry correspond to'prev_entry_scn_' was written by APPEND, its' scn may be greater than
  //    replayable_point_scn), we should set next_min_scn to std::max(
  //    std::min(replayable_point_scn + 1, the log scn of 'curr_entry_'), 'prev_entry_scn_' + 1).
  //
  // 2. otherwise, iterate end by file end lsn:
  //    - case 1: if 'curr_entry_' has been parsed from LogIteratorStorage, however, it's not readable
  //              due to file end lsn(consider that someone set the file end lsn to the middle of one
  //              LogGroupEntry), we should set next_min_scn to the max of min(curr_entry_'s scn,
  //              replayable_point_scn + 1) and prev_entry_scn_.
  //    - case 2: if 'curr_entry_' has not been parsed from LogIteratorStorage, we just se next_min_scn
  //              to prev_entry_scn_ + 1.
  if (OB_ITER_END == ret) {
    if (!info.is_valid() && !prev_entry_scn_.is_valid()) {
      next_min_scn.reset();
      PALF_LOG(WARN, "there is no readable log, set next_min_scn to OB_INVALID_TIMESTAMP",
          K(ret), KPC(this), K(info), K(replayable_point_scn));
    } else if (iterate_end_by_replayable_point || IterateEndReason::DUE_TO_FILE_END_LSN_READ_NEW_DATA == info.reason_) {
      // when there is no log between [replayable_point_scn, 'curr_entry'), we can advance next_log_min_scn.
      next_min_scn = MIN(
          (replayable_point_scn.is_valid() ? SCN::plus(replayable_point_scn, 1) : SCN::max_scn()),
          info.log_scn_);
      PALF_LOG(TRACE, "update next_min_scn to min of replayable_point_scn and log_group_entry_min_scn_", KPC(this), K(info));

      next_min_scn = MAX(
          (prev_entry_scn_.is_valid() ? SCN::plus(prev_entry_scn_, 1) : SCN::min_scn()),
          next_min_scn);
      PALF_LOG(TRACE, "update next_min_scn to max of prev_entry_scn_ and next_min_scn",
          KPC(this), K(info), K(replayable_point_scn));
      // NB: To make 'next_min_scn' newly, advance 'prev_entry_scn_' when 'curr_entry_' need control by readable point scn.
      //     consider follower case:
      //     T1, iterate an entry successfully, and make prev_entry_scn to 5;
      //     T2, iterate control by readable point scn, scn of 'curr_entry_' is 10, and readable point scn is 8.
      //     T3, the logs after 8 have been flashback, and no log has been written.
      //     if we don't advance 'prev_entry_scn_' to 8, the continuous point of replay service can not update
      //     to 8.
      if (info.log_scn_ > replayable_point_scn) {
        prev_entry_scn_ = MAX(replayable_point_scn, prev_entry_scn_);
        PALF_LOG(TRACE, "update prev_entry_scn_ to replayable_point_scn", KPC(this), K(info), K(replayable_point_scn));
      }
      // NB: if has not read any log, we should set next_min_scn to invalid.
    } else if (IterateEndReason::DUE_TO_FILE_END_LSN_NOT_READ_NEW_DATA == info.reason_) {
        next_min_scn = prev_entry_scn_.is_valid() ? SCN::plus(prev_entry_scn_, 1) : SCN::min_scn();
        PALF_LOG(TRACE, "update next_min_scn to prev_entry_scn_ + 1", KPC(this), K(info), K(replayable_point_scn));
    } else {
    }
  }
  if (OB_FAIL(ret)) {
    curr_entry_size_ = 0;
    // To debug easily, don't reset 'curr_entry_'
    // curr_entry_.reset();
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
  // If the 'start_lsn' of PalfBufferIterator is not pointed to LogGroupEntry,
  // before iterate next LogGroupEntry, the LogEntry will not be checked integrity,
  // therefore, when accumulate_checksum_ is -1, check the integrity of entry.
  } else if (-1 == accumulate_checksum_ && !entry.check_integrity()) {
    ret = OB_INVALID_DATA;
    PALF_LOG(WARN, "invalid data", K(ret), KPC(this), K(entry));
  } else {
    lsn = log_storage_->get_lsn(curr_read_pos_);
    is_raw_write = curr_entry_is_raw_write_;
  }
  return ret;
}

template<class ENTRY>
int LogIteratorImpl<ENTRY>::verify_accum_checksum_(const LogGroupEntry &entry,
                                                   int64_t &new_accumulate_checksum)
{
  int ret = OB_SUCCESS;
  int64_t data_checksum = -1;
  int64_t expected_verify_checksum = entry.get_header().get_accum_checksum();
  if (!entry.check_integrity(data_checksum)) {
    ret = OB_INVALID_DATA;
    PALF_LOG(WARN, "invalid data", K(ret), KPC(this), K(entry));
  } else if (-1 == accumulate_checksum_) {
    new_accumulate_checksum = expected_verify_checksum;
    PALF_LOG(TRACE, "init accumulate_checksum to first LogGroupEntry", K(entry), KPC(this),
        K(new_accumulate_checksum));
  } else if (OB_FAIL(LogChecksum::verify_accum_checksum(
                accumulate_checksum_, data_checksum,
                expected_verify_checksum, new_accumulate_checksum))) {
    PALF_LOG(WARN, "verify accumlate checksum failed", K(ret), KPC(this), K(entry));
  } else {
    PALF_LOG(TRACE, "verify_accum_checksum_ success", K(ret), KPC(this), K(entry));
  }
  return ret;
}

template<class ENTRY>
int LogIteratorImpl<ENTRY>::construct_padding_log_entry_(const int64_t memset_start_pos,
                                                         const int64_t padding_log_entry_len)
{
  int ret = OB_SUCCESS;
  LSN padding_log_entry_start_lsn = log_storage_->get_lsn(memset_start_pos);
  // defense code
  if (!curr_entry_is_padding_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "only call this function when LogGroupEntry is padding", KPC(this));
  } else if (!is_padding_entry_end_lsn_(padding_log_entry_start_lsn + padding_log_entry_len)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, the end lsn of padding log entry is not the header of nexet block!!!",
        KPC(this), K(memset_start_pos), K(padding_log_entry_len));
  } else if (memset_start_pos + padding_log_entry_len > curr_read_buf_end_pos_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, the end pos of 'curr_read_buf_' is not enough!!!",
        KPC(this), K(memset_start_pos), K(padding_log_entry_len));
  } else {
    // set memory which contained padding log entry to PADDING_LOG_CONTENT_CHAR
    memset(buf_+memset_start_pos, PADDING_LOG_CONTENT_CHAR, padding_log_entry_len);
    // The format of LogEntry
    // | LogEntryHeader | ObLogBaseHeader | data |
    // NB: construct padding log entry by self
    int64_t header_size = LogEntryHeader::HEADER_SER_SIZE;
    int64_t padding_log_entry_data_len = padding_log_entry_len - header_size;
    int64_t serialize_log_entry_header_pos = memset_start_pos;
    if (OB_FAIL(LogEntryHeader::generate_padding_log_buf(padding_log_entry_data_len,
                                                         padding_entry_scn_,
                                                         buf_+serialize_log_entry_header_pos,
                                                         LogEntryHeader::PADDING_LOG_ENTRY_SIZE))) {
      PALF_LOG(ERROR, "generate_padding_log_buf failed", KPC(this), K(padding_log_entry_data_len),
          K(header_size), K(padding_log_entry_len), K(serialize_log_entry_header_pos));
    } else {
      PALF_LOG(INFO, "generate padding log entry successfully", KPC(this));
      reset_padding_info_();
    }
  }
  return ret;
}

// step1. according to magic number, acquire log entry type;
// step2. deserialize ENTRY from 'read_buf_', if buf not enough, return and run in next
// round; step3. check entry integrity, if failed, return OB_INVALID_DATA; step4. if the
// entry type is not 'wanted_log_entry_type', skip the header of this entry. NB: for
// LOG_PADDING, the header size include data_len.
template <class ENTRY>
int LogIteratorImpl<ENTRY>::parse_one_entry_(const SCN &replayable_point_scn,
                                             IterateEndInfo &info)
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
            ret = parse_log_group_entry_(replayable_point_scn, info);
            break;
          }
        case LogEntryType::LOG_ENTRY_HEADER:
          {
            ret = parse_log_entry_(replayable_point_scn, info);
            break;
          }
        case LogEntryType::LOG_META_ENTRY_HEADER:
          {
            ret = parse_meta_entry_();
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
void LogIteratorImpl<ENTRY>::advance_read_lsn_(const offset_t step)
{
  curr_read_pos_ += step;
}

// @brief, start from `curr_read_lsn_`, check whether the entry is valid.
template <class ENTRY>
bool LogIteratorImpl<ENTRY>::check_is_the_last_entry(LogIOContext &io_ctx)
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
    if (OB_FAIL(read_data_from_storage_(io_ctx)) && OB_ITER_END != ret) {
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
  bool is_log_entry_iterator = std::is_same<ENTRY, LogEntry>::value;
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
  } else if (is_log_entry_iterator && curr_entry_is_padding_
             // defense code
             // after iterate padding log entry successfully, assume next block has been corrupted,
             // iterate next log entry in next block will be failed, we mustn't return padding log
             // entry again.(In this case, curr_entry_is_padding_ is the value of prev log entry)
             && !is_padding_entry_end_lsn_(log_storage_->get_lsn(curr_read_pos_))) {
    log_entry_type = LogEntryType::LOG_ENTRY_HEADER;
    PALF_LOG(INFO, "need consume padding log", KPC(this));
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
// 2. memmove 'valid_tail_part' to the header of read_buf_.
// 3. read data from 'end' and memcpy these data into read_buf_ + valid_tail_part_size.
//
// NB: when iterate to the end of block, need switch to next block.
template <class ENTRY>
int LogIteratorImpl<ENTRY>::read_data_from_storage_(LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t out_read_size = 0;
  if (OB_FAIL(log_storage_->pread(curr_read_pos_, next_round_pread_size_, buf_,
                                  out_read_size, io_ctx))
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
    LSN curr_read_lsn = log_storage_->get_lsn(curr_read_pos_);
    // reuse LogIteratorStorage firstly, only the log before 'start_lsn_' + 'curr_read_pos_'
    // has been consumed.
    // NB: need ensure that we can not update 'curr_read_pos_' to 'curr_read_pos_' + sizeof(LogGroupEntryHeader)
    //     when the LogGroupEntryHeader after 'curr_read_pos_' need be controlled by replayable_point_scn.
    log_storage_->reuse(curr_read_lsn);
    curr_read_buf_start_pos_ = 0;
    curr_read_pos_ = 0;
    curr_read_buf_end_pos_ = 0;
    curr_entry_.reset();

    // NB: we can not reset curr_entry_is_raw_write_, otherwise, the log entry after replayable_point_scn may no be
    //     controlled by replayable_point_scn.
    //     consider that:
    //     1. At T1 timestamp, the LogGroupEntry has three LogEntry, the first LogEntry has been seen by ReplayService,
    //        however, the remained LogEntry can not been seen due to replayable_point_scn (current replayable_point_scn
    //        may be lag behind others).
    //     2. At T2 timestamp, this replica is in flashback mode, the logs are not been flashback(flashback_scn is greater
    //        than replayable_point_scn, and there are several logs which SCN is greater than flashback_scn)
    //     3. At T3 timestamp, ReplayService use next() function, iterator will try_clean_up_cache_ because mode version
    //        has been changed. if reset curr_entry_is_raw_write_, the second LogEntry may be seen by ReplayService even
    //        if the SCN of this LogEntry is greater than flashback_scn.
    // curr_entry_is_raw_write_ = false;
    curr_entry_size_ = 0;
    init_mode_version_ = current_mode_version;

    // we can not reset prev_entry_scn_, otherwise, after flashback, if there is no logs which can be readable on disk,
    // we can not return a valid next_min_scn.
    // - prev_entry_.reset();

    // we need reset accum_checksum_, otherwise, the accum_checksum_ is the log before flashback, and iterate new
    // group log will fail.
    accumulate_checksum_ = -1;
    reset_padding_info_();
  }
}
} // end namespace palf
} // end namespace oceanbase
#endif
