/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_META_ENTRY_
#define OCEANBASE_LOGSERVICE_LOG_META_ENTRY_

#include "lib/ob_define.h"                  // Serialization
#include "lib/utility/ob_print_utils.h"     // Print*
#include "log_meta_entry_header.h"          // LogMetaEntryHeader
#include "share/scn.h"                      // SCN
#include "log_define.h"                     // PALF_META_BLOCK_SIZE
namespace oceanbase
{
namespace palf
{
class LogMetaEntry
{
public:
  LogMetaEntry();
  ~LogMetaEntry();

public:
  int generate(const LogMetaEntryHeader &header,
               const char *buf);
  bool is_valid() const;
  void reset();
  int shallow_copy(const LogMetaEntry &entry);
  const LogMetaEntryHeader &get_log_meta_entry_header() const;
  const char *get_buf() const;
  bool check_integrity() const;
  int64_t get_header_size() const { return header_.get_serialize_size(); }
  int64_t get_payload_offset() const { return header_.get_serialize_size(); }
  int64_t get_data_len() const { return header_.get_data_len(); }
  int64_t get_entry_size() const { return header_.get_serialize_size() + get_data_len(); }
  share::SCN get_scn() const { return share::SCN::invalid_scn(); };
  const LogMetaEntryHeader &get_header() const { return header_; }
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(header), KP(buf_));
  static const int64_t BLOCK_SIZE = PALF_META_BLOCK_SIZE;
  using LogEntryHeaderType=LogMetaEntryHeader;
private:
  LogMetaEntryHeader header_;
  const char *buf_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
