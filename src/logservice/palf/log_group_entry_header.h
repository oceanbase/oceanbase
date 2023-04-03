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

#ifndef OCEANBASE_LOGSERVICE_LOG_GROUP_ENTRY_HEADER_
#define OCEANBASE_LOGSERVICE_LOG_GROUP_ENTRY_HEADER_

#include "lib/ob_define.h"                      // Serialization
#include "lib/ob_name_def.h"
#include "lib/utility/ob_print_utils.h"         // Print*
#include "share/scn.h"                                // SCN
#include "lsn.h"                                // LSN

namespace oceanbase
{
namespace palf
{
class LogWriteBuf;
class LogGroupEntry;
class LogGroupEntryHeader
{
public:
  LogGroupEntryHeader();
  ~LogGroupEntryHeader();
public:
  using ENTRYTYPE = LogGroupEntry;
  // @brief generate an object used to serialize and deserialize
  // @param[in] is_raw_write: whether this log generaete in RAW_WRITE mode
  // @param[in] is_padding_log: whether this log is a padding log
  // @param[in] buf: the data pointer of this log
  // @param[in] data_len: the data len of this log
  // @param[in] max_timestamp: the max timestamp of this log(group buffer)
  // @param[in] log_id: the log id of this log, just only used for sliding window
  // @param[in] committed_end_lsn: the last committed log lsn before this log
  // @param[in] log_proposal_id: the proposal id of this log, used for consensus
  int generate(const bool is_raw_write,
               const bool is_padding_log,
               const LogWriteBuf &log_write_buf,
               const int64_t data_len,
               const share::SCN &max_scn,
               const int64_t log_id,
               const LSN &committed_end_lsn,
               const int64_t &log_proposal_id,
               int64_t &data_checksum);
  bool is_valid() const;
  void reset();
  LogGroupEntryHeader& operator=(const LogGroupEntryHeader &header);
  int32_t get_data_len() const { return group_size_; }
  int64_t get_accum_checksum() const { return accumulated_checksum_; }
  const share::SCN &get_max_scn() const { return max_scn_; }
  int64_t get_log_id() const { return log_id_; }
  const int64_t &get_log_proposal_id() const { return proposal_id_; }
  const LSN &get_committed_end_lsn() const { return committed_end_lsn_; }
  bool is_padding_log() const;
  bool is_raw_write() const;

  bool operator==(const LogGroupEntryHeader &header) const;
  // This function used to check the checksum of buf is as same as
  // the data_checksum_
  bool check_header_integrity() const;
  bool check_integrity(const char *buf, int64_t data_len) const;
  bool check_integrity(const char *buf, int64_t data_len, int64_t &group_log_checksum) const;
  // The follow function used to update a few fields.
  //
  // NB: The data integrity is the responsibility of
  // the caller.
  //
  // Used to update the proposal id of this log, for standby cluster
  int update_log_proposal_id(const int64_t &log_proposal_id);
  // Used to update the committed end lsn, for standby cluster
  int update_committed_end_lsn(const LSN &lsn);
  // Used to update write mode of this log, for standby cluster
  void update_write_mode(const bool is_raw_write);

  // Used to update header checksum
  void update_header_checksum();
  void update_accumulated_checksum(int64_t accumulated_checksum);

  int truncate(const char *buf,
               const int64_t data_len,
               const share::SCN &cut_scn,
               const int64_t pre_accum_checksum);

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV("magic", magic_,
               "version", version_,
               "group_size", group_size_,
               "proposal_id", proposal_id_,
               "committed_lsn", committed_end_lsn_,
               "max_scn", max_scn_,
               "accumulated_checksum", accumulated_checksum_,
               "log_id", log_id_,
               "flag", flag_);

private:
  void update_header_checksum_();
  int calculate_log_checksum_(const bool is_padding_log,
                              const LogWriteBuf &log_write_buf,
                              const int64_t data_len,
                              int64_t &data_checksum);
  bool check_header_checksum_() const;
  bool check_log_checksum_(const char *buf, const int64_t data_len, int64_t &group_log_checksum) const;
  bool get_header_parity_check_res_() const;
public:
  // Update this variable when modifying header's member
  static const int64_t HEADER_SER_SIZE;
  //GR means record
  static constexpr int16_t MAGIC = 0x4752;
private:
  static constexpr int16_t LOG_GROUP_ENTRY_HEADER_VERSION = 1;
  static constexpr int64_t PADDING_TYPE_MASK = 1 << 1;
  static constexpr int64_t RAW_WRITE_MASK = 1 << 2;
  static constexpr int64_t PADDING_LOG_DATA_CHECKSUM = 0;  // padding log的data_checksum为0
private:
  // Binary visualization, for LogGroupEntryHeader, its' magic number
  // is 0x4752, means GR(group header)
  int16_t magic_;
  // Upgrade compatible
  int16_t version_;
  // The length of data, not including the header
  int32_t group_size_;
  // The proposal id of this log
  int64_t proposal_id_;
  // The max committed log offset before this log
  LSN committed_end_lsn_;
  // The max scn of this log
  share::SCN max_scn_;
  // The accumulated checksum before this log, including this log,
  // not including log header
  int64_t accumulated_checksum_;
  // The log id of this log, this field just only used for
  // sliding window
  int64_t log_id_;
  // The lowest bit is used for parity check.
  // The second bit from last is used for padding type flag.
  // The third bit from last is used for checking whether is RAW_WRITE
  int64_t flag_;
};

} // end namespace palf
} // end namespace oceanbase

#endif
