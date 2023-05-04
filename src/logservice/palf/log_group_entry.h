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

#ifndef OCEANBASE_LOGSERVICE_LOG_GROUP_ENTRY_
#define OCEANBASE_LOGSERVICE_LOG_GROUP_ENTRY_

#include "log_group_entry_header.h"              // LogGroupEntryHeader
#include "lib/ob_define.h"                    // Serialization
#include "lib/utility/ob_print_utils.h"       // Print*
#include "lib/utility/ob_macro_utils.h"       // DISALLOW_COPY_AND_ASSIGN

namespace oceanbase
{
namespace palf
{
class LogGroupEntry
{
public:
  LogGroupEntry();
  ~LogGroupEntry();

public:
  // @brief generate an LogGroupEntry used to serialize or deserialize
  // @param[in] header is an object on the stack
  // @param[in] buf is a block of memory space which has allocated by heap
  int generate(const LogGroupEntryHeader &header,
               const char *buf);
  int shallow_copy(const LogGroupEntry &entry);
  bool is_valid() const;
  void reset();
  bool check_integrity() const;
  bool check_integrity(int64_t &data_checksum) const;
  int64_t get_header_size() const { return header_.get_serialize_size(); }
  int64_t get_payload_offset() const { return header_.get_serialize_size(); }
  int64_t get_data_len() const { return header_.get_data_len(); }
  // return total size of header and body, including the length of padding log
  int64_t get_group_entry_size() const { return header_.get_serialize_size() +
    header_.get_data_len(); }
  int get_log_min_scn(share::SCN &min_scn) const;
  const share::SCN get_scn() const { return header_.get_max_scn(); }
  LSN get_committed_end_lsn() const { return header_.get_committed_end_lsn(); }
  const LogGroupEntryHeader &get_header() const { return header_; }
  const char *get_data_buf() const { return buf_; }
  // @brief truncate log group entry the upper_limit_scn, only log entries with scn not bigger than which can reserve
  // param[in] upper_limit_scn the upper bound to determain which log entries can reserve
  // param[in] pre_accum_checksum, the accum_checksum of the pre log
  int truncate(const share::SCN &upper_limit_scn, const int64_t pre_accum_checksum);

  TO_STRING_KV("LogGroupEntryHeader", header_);
  NEED_SERIALIZE_AND_DESERIALIZE;
  static const int64_t BLOCK_SIZE = PALF_BLOCK_SIZE;
  using LogEntryHeaderType=LogGroupEntryHeader;
private:
  LogGroupEntryHeader header_;
  const char *buf_;
  DISALLOW_COPY_AND_ASSIGN(LogGroupEntry);
};
} // end namespace palf
} // end namespace oceanbase

#endif
