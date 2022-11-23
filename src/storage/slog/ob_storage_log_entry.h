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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_ENTRY_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_ENTRY_H_

#include "common/ob_record_header.h"

namespace oceanbase
{
namespace storage
{

struct ObStorageLogEntry
{
  int16_t magic_;
  int16_t version_;
  int16_t entry_len_;
  int16_t rez_;
  int32_t cmd_;
  int32_t data_len_;
  uint64_t seq_;
  int64_t timestamp_;
  uint64_t data_checksum_;
  uint64_t entry_checksum_;

  static const int16_t MAGIC_NUMBER = static_cast<int16_t>(0xAAAAL);
  static const int16_t ENTRY_VERSION = 1;

  ObStorageLogEntry();
  ~ObStorageLogEntry();

  TO_STRING_KV(K_(magic),
               K_(version),
               K_(entry_len),
               K_(entry_checksum),
               K_(cmd),
               K_(data_len),
               K_(seq),
               K_(data_checksum),
               K_(timestamp))

  // set fields of ObRecordHeader
  int fill_entry(
      const char *log_data,
      const int64_t data_len,
      const int32_t cmd,
      const uint64_t seq);
  void reset();
  // Calculate the checksum of data
  uint64_t calc_data_checksum(const char *log_data, const int64_t data_len) const;
  // Calculate the checksum of entry
  uint64_t calc_entry_checksum() const;

  int check_entry_integrity(const bool dump_content = true) const;

  // check the integrity of data
  int check_data_integrity(const char *log_data, const bool dump_content = true) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
};

}
}

#endif