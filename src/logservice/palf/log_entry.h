/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_ENTRY_
#define OCEANBASE_LOGSERVICE_LOG_ENTRY_

#include "log_entry_header.h"              // LogGroupEntryHeader
#include "lib/ob_define.h"                 // Serialization
#include "lib/utility/ob_print_utils.h"    // Print*
#include "lib/utility/ob_macro_utils.h"    // DISALLOW_COPY_AND_ASSIGN
#include "share/scn.h"
#include "log_define.h"

namespace oceanbase
{
namespace palf
{
class LogEntry
{
public:
  LogEntry();
  ~LogEntry();

public:
  int shallow_copy(const LogEntry &input);
  bool is_valid() const;
  void reset();
  // TODO by runlin, need check header checsum?
  bool check_integrity() const;
  int64_t get_header_size() const { return header_.get_serialize_size(); }
  int64_t get_payload_offset() const { return header_.get_serialize_size(); }
  int64_t get_data_len() const { return header_.get_data_len(); }
  const share::SCN get_scn() const { return header_.get_scn(); }
  const char *get_data_buf() const { return buf_; }
  const LogEntryHeader &get_header() const { return header_; }

  TO_STRING_KV("LogEntryHeader", header_);
  NEED_SERIALIZE_AND_DESERIALIZE;
  static const int64_t BLOCK_SIZE = PALF_BLOCK_SIZE;
  using LogEntryHeaderType=LogEntryHeader;
private:
  LogEntryHeader header_;
  const char *buf_;
  DISALLOW_COPY_AND_ASSIGN(LogEntry);
};
} // end namespace palf
} // end namespace oceanbase

#endif
