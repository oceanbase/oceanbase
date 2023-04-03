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

#ifndef OCEANBASE_LOGSERVICE_LOG_META_HEADER_
#define OCEANBASE_LOGSERVICE_LOG_META_HEADER_

#include <stdlib.h>                           // int64_t
#include "lib/ob_define.h"                    // Serialization
#include "lib/utility/ob_print_utils.h"       // Print*

namespace oceanbase
{
namespace palf
{
class LogMetaEntry;
class LogMetaEntryHeader
{
public:
  LogMetaEntryHeader();
  ~LogMetaEntryHeader();

public:
  using ENTRYTYPE = LogMetaEntry;
  int generate(const char *buf, int32_t data_len);
  bool is_valid() const;
  void reset();

  LogMetaEntryHeader& operator=(const LogMetaEntryHeader &header);
  int32_t get_data_len() const { return data_len_; }
  bool check_integrity(const char *buf, int32_t data_len) const;
  bool check_header_integrity() const;
  bool operator==(const LogMetaEntryHeader &header) const;
  TO_STRING_KV(K_(magic), K_(version), K_(data_len), K_(data_checksum),
      K_(header_checksum));
  static const int64_t HEADER_SER_SIZE;
  NEED_SERIALIZE_AND_DESERIALIZE;
  // 0x4C4D means LM(LogMeta)
  static const int16_t MAGIC = 0x4C4D;
private:
  int64_t calc_data_checksum_(const char *buf, int32_t data_len) const;
  int64_t calc_header_checksum_() const;
  bool check_data_checksum_(const char *buf, int32_t data_len) const;
  bool check_header_checksum_() const;

private:
  static const int16_t LOG_META_ENTRY_HEADER_VERSION = 1;

private:
  int16_t magic_;
  int16_t version_;
  int32_t data_len_;
  int64_t data_checksum_;
  int64_t header_checksum_;
};
} // end namespace palf
} // end namespace namespace

#endif
