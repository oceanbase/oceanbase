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

#ifndef OCEANBASE_CLOG_OB_LOG_FILE_TRAILER_
#define OCEANBASE_CLOG_OB_LOG_FILE_TRAILER_

// This file defines ObLogFileTrailer.
//
// The ObLogFileTrailer is a special block in the end of each complete file for
// searching the info block and getting the file id of the following file.
//
// The ObLogFileTrailer is used by ObLogReader and ObLogWriter.
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
//  | OB_INFO_BLOCK   | the partition meta info                    |
//  ----------------------------------------------------------------
//  |                       | OB_TRAILER_BLOCK |start_pos + file_id|
//  ----------------------------------------------------------------
//
// ObLogFileTrailer is the last block in the file.
// 1.start_pos is the offset of OB_INFO_BLOCK, which stores partition meta.
// 2.file_id is the id of the following file
// 3.block_meta_ stores meta information for this block

#include "ob_log_block.h"

namespace oceanbase {
namespace clog {
class ObLogFileTrailer {
public:
  ObLogFileTrailer();
  ~ObLogFileTrailer();

  // the user needs to update the block_meta_ after setting a new value
  int set_start_pos(const offset_t start_pos);
  int set_file_id(const file_id_t file_id);
  offset_t get_start_pos() const
  {
    return start_pos_;
  }
  file_id_t get_file_id() const
  {
    return file_id_;
  }

  TO_STRING_KV(N_BLOCK_META, block_meta_, N_VERSION, version_, N_START_POS, start_pos_, N_FILE_ID, file_id_);

  int build_serialized_trailer(
      char* buf, const int64_t buf_len, const offset_t start_pos, const file_id_t file_id, int64_t& pos);

  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size(void) const;

private:
  static const int16_t CLOG_FILE_VERSION = 1;

  ObLogBlockMetaV2 block_meta_;
  int16_t version_;
  offset_t start_pos_;
  file_id_t file_id_;

  DISALLOW_COPY_AND_ASSIGN(ObLogFileTrailer);
};

class ObIlogFileTrailerV2 {
public:
  ObIlogFileTrailerV2();
  ~ObIlogFileTrailerV2();

public:
  int init(const offset_t info_block_start_offset, const int32_t info_block_size,
      const offset_t memberlist_block_start_offset, const int32_t memberlist_block_size,
      const int64_t file_content_checksum);
  void reset();

  // Check magic_number_, confirm whether the data in buf
  // is a valid type
  static int check_magic_number(const char* buf, const int64_t len, bool& is_valid);
  offset_t get_info_block_start_offset() const;
  int32_t get_info_block_size() const;
  int64_t get_file_content_checksum() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  // 0x4c42 == BL means Block, 0x4c43 plus one
  static const int16_t MAGIC_NUMBER = 0x4c43;
  TO_STRING_KV(K(magic_number_), K(info_block_start_offset_), K(info_block_size_), K(file_content_checksum_),
      K(trailer_checksum_));

private:
  void update_trailer_checksum_();
  int16_t magic_number_;
  int16_t padding1_;
  int32_t padding2_;
  offset_t info_block_start_offset_;
  int32_t info_block_size_;
  offset_t memberlist_block_start_offset_;
  int32_t memberlist_block_size_;
  int64_t file_content_checksum_;
  int64_t trailer_checksum_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIlogFileTrailerV2);
};
}  // end namespace clog
}  // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_FILE_TRAILER_
