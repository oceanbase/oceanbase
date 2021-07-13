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

#ifndef OCEANBASE_CLOG_OB_LOG_BLOCK_
#define OCEANBASE_CLOG_OB_LOG_BLOCK_

// This file defines ObLogBlockMetaV2.  The struct of a reading/writing block is:
//
//  -----------------------------------------------------------------
//  |  BlockMeta | ObLogEntryHeader | data | ObLogEntryHeader | data|
//  |  ObLogEntryHeader  | data | ObLogEntryHeader | data |   ...   |
//  |            ...          | data |             padding          |
//  -----------------------------------------------------------------
//
// The struct of a complete file is:
//
//  ----------------------------------------------------------------
//  | OB_DATA_BLOCK  | one or multiple logs                        |
//  ----------------------------------------------------------------
//  | OB_DATA_BLOCK  |                                             |
//  ----------------------------------------------------------------
//  |                           ...                                |
//  ----------------------------------------------------------------
//  | OB_INFO_BLOCK   |            info block content              |
//  ----------------------------------------------------------------
//  |                       | OB_TRAILER_BLOCK |start_pos + file_id|
//  ----------------------------------------------------------------
//
// The struct of the last file may be:
//
//  ----------------------------------------------------------------
//  | OB_DATA_BLOCK |                                              |
//  ----------------------------------------------------------------
//  | OB_DATA_BLOCK |                                              |
//  ----------------------------------------------------------------
//  |                           ...                                |
//  ----------------------------------------------------------------
//
//  ObLogBlockMetaV2::MetaContent includes:
//  1. maigc_, data_checksum_, meta_checksum_:checks block integrity
//  2. type_:uses ObBlockType;
//  3. timestamp_:block generation time for debugging
//  4. total_len_, padding_len_:block adds padding for 512 aligned

#include "lib/utility/ob_print_utils.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
class ObLogBlockMetaV2 {
public:
  ObLogBlockMetaV2();
  ~ObLogBlockMetaV2();

  void reset();

  int16_t get_magic() const
  {
    return magic_;
  }
  int16_t get_version() const
  {
    return version_;
  }
  ObBlockType get_block_type() const
  {
    return static_cast<ObBlockType>(type_);
  }
  int64_t get_total_len() const
  {
    return total_len_;
  }
  int64_t get_data_len() const
  {
    return (total_len_ - padding_len_ - get_serialize_size());
  }
  int64_t get_padding_len() const
  {
    return padding_len_;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }
  int64_t get_data_checksum() const
  {
    return data_checksum_;
  }
  int64_t get_meta_checksum() const
  {
    return meta_checksum_;
  }

  void set_type(const ObBlockType type)
  {
    type_ = type;
  }
  void set_total_len(const int64_t len)
  {
    total_len_ = len;
  }
  void set_padding_len(const int64_t len)
  {
    padding_len_ = len;
  }
  void set_timestamp(const int64_t timestamp)
  {
    timestamp_ = timestamp;
  }

  // @brief Check data integrity
  // @param [in] data_buf     the start address of data part
  // @param [in] data_len     length of data part
  bool check_integrity(const char* data_buf, const int64_t data_len) const;
  bool check_meta_checksum() const;

  // build block content by given data_buf and data_len, and then serialized to buf
  int build_serialized_block(char* buf, const int64_t buf_len, const char* data_buf, const int64_t data_len,
      const ObBlockType type, int64_t& pos);

  static bool check_magic_number(const int16_t magic_number)
  {
    return META_MAGIC == magic_number;
  }

  TO_STRING_KV(N_MAGIC, magic_, N_TYPE, type_, N_TOTAL_LEN, total_len_, N_PADDING_LEN, padding_len_, N_TIMESTAMP,
      timestamp_, N_DATA_CHECKSUM, data_checksum_, N_META_CHECKSUM, meta_checksum_);
  NEED_SERIALIZE_AND_DESERIALIZE;
  static const int16_t META_MAGIC = 0x4C42;  // BL means block

private:
  int generate_block(const char* buf, const int64_t data_len, const ObBlockType type);
  int64_t calc_data_checksum_(const char* buf, const int64_t data_len) const;
  int64_t calc_meta_checksum_() const;
  bool check_data_checksum_(const char* buf, const int64_t data_len) const;

private:
  static const int16_t BLOCK_VERSION = 1;

  int16_t magic_;
  int16_t version_;
  int32_t type_;           // block type: DATA_BLOCK, HEADER_BLOCK, INFO_BLOCK, TRAILER_BLOCK
  int64_t total_len_;      // block length, aligned by CLOG_DIO_ALIGN_SIZE
  int64_t padding_len_;    // total_len - data_len - meta_len
  int64_t timestamp_;      // block meta generation time, used for debug
  int64_t data_checksum_;  // data checksum

  // memta checksum calculation requirements:
  // 1. Located in the last line of the member variable declaration
  // 2. no virtual function in class
  // 3. The member variable before this declaration is 64-bit aligned
  int64_t meta_checksum_;

  DISALLOW_COPY_AND_ASSIGN(ObLogBlockMetaV2);
};
}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_BLOCK_
