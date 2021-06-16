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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_BLOCK_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_BLOCK_

#include "lib/utility/ob_print_utils.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace archive {

class ObArchiveBlockMeta {
public:
  ObArchiveBlockMeta();
  ~ObArchiveBlockMeta();

  void reset();

  int16_t get_magic() const
  {
    return magic_;
  }
  int16_t get_version() const
  {
    return version_;
  }
  int64_t get_total_len() const
  {
    return total_len_;
  }
  int64_t get_data_len() const
  {
    return (total_len_ - get_serialize_size());
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

  void set_total_len(const int64_t len)
  {
    total_len_ = len;
  }

  // @brief Check data integrity
  // @param [in] data_buf     the start address of data part
  // @param [in] data_len     length of data part
  bool check_integrity(const char* data_buf, const int64_t data_len) const;
  bool check_meta_checksum() const;

  // build block content by given data_buf and data_len, and then serialized to buf
  int build_serialized_block(
      char* buf, const int64_t buf_len, const char* data_buf, const int64_t data_len, int64_t& pos);

  static bool check_magic_number(const int16_t magic_number)
  {
    return META_MAGIC == magic_number;
  }

  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K(magic_), K(total_len_), K(timestamp_), K(clog_epoch_id_), K(accum_checksum_), K(min_log_id_in_file_),
      K(min_log_ts_in_file_), K(max_log_id_), K(max_checkpoint_ts_), K(max_log_submit_ts_), K(input_bytes_),
      K(output_bytes_), K(data_checksum_), K(meta_checksum_));

private:
  int generate_block_(const char* buf, const int64_t data_len);
  int64_t calc_data_checksum_(const char* buf, const int64_t data_len) const;
  int64_t calc_meta_checksum_() const;
  bool check_data_checksum_(const char* buf, const int64_t data_len) const;
  // 0x4142 AB means ARCHIVE BLOCK
  static const int16_t META_MAGIC = oceanbase::share::ObBackupFileType::BACKUP_ARCHIVE_BLOCK_META;

public:
  static const int16_t ARCHIVE_BLOCK_VERSION = 1;
  //!!!!attention: remember to modify RESERVED_FOR_BLOCK when sizeof(*this) exceeds 200
  int16_t magic_;
  // ATTENTION: version must be push up when block meta is modified
  int16_t version_;

  /*------start of members those values are assigned by clog_split_engine*/
  int32_t total_len_;  // total block length
  int64_t timestamp_;  // generation timestamp of the meta, for debug
  /*------end of members those values are assigned by clog_split_engine*/

  /*------start of members those values are assigned by sender*/
  // log_info
  int64_t clog_epoch_id_;        // checkpoint log has no epoch_id
  int64_t accum_checksum_;       // accum_checksum_ of the max log in the block
  uint64_t min_log_id_in_file_;  // min log id in the archived file
  int64_t min_log_ts_in_file_;   // min log submit ts in the archived file
  uint64_t max_log_id_;          // max log id in the block
  int64_t max_checkpoint_ts_;    // max checkpoint ts in the block
  int64_t max_log_submit_ts_;    // max log submit ts in the block
  /*------end of members those values are assigned by sender*/

  // stat info
  int64_t input_bytes_;
  int64_t output_bytes_;

  // ATTENTION: checksum must put in the last line
  int64_t data_checksum_;  // checksum of data part
  int64_t meta_checksum_;  // checksum of whole meta, calculate by sender
};

}  // end namespace archive
}  // end namespace oceanbase

#endif  // OCEANBASE_ARCHIVE_OB_ARCHIVE_BLOCK_
