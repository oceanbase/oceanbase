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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_WRITER_H_
#define OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_WRITER_H_

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace blocksstable {
enum MICRO_BLOCK_MERGE_VERIFY_LEVEL {
  NONE = 0,
  ENCODING = 1,
  ENCODING_AND_COMPRESSION = 2,
  ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE = 3,
};

// Some common interface of ObMicroBlockWriter and ObMicroBlockEncoder, not all features.
class ObIMicroBlockWriter {
public:
  ObIMicroBlockWriter()
      : row_count_delta_(0),
        micro_block_checksum_(0),
        micro_block_merge_verify_level_(MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION),
        max_merged_trans_version_(0),
        contain_uncommitted_row_(false)
  {}
  virtual ~ObIMicroBlockWriter()
  {}
  virtual int append_row(const storage::ObStoreRow& row) = 0;
  virtual int build_block(char*& buf, int64_t& size) = 0;
  virtual common::ObString get_last_rowkey() const = 0;
  virtual int64_t get_row_count() const = 0;
  virtual int64_t get_data_size() const = 0;
  virtual int64_t get_block_size() const = 0;
  virtual int64_t get_column_count() const = 0;
  virtual void reuse()
  {
    row_count_delta_ = 0;
    micro_block_checksum_ = 0;
    max_merged_trans_version_ = 0;
    contain_uncommitted_row_ = false;
    set_micro_block_merge_verify_level();
  }
  int32_t get_row_count_delta() const
  {
    return row_count_delta_;
  }
  int64_t get_micro_block_merge_verify_level() const
  {
    return micro_block_merge_verify_level_;
  }

  static int64_t cal_row_checksum(const storage::ObStoreRow& row, int64_t checksum)
  {
    if (row.is_sparse_row_) {  // for sparse row
      for (int64_t i = 0; i < row.row_val_.count_; ++i) {
        checksum = row.row_val_.cells_[i].checksum(checksum + row.column_ids_[i]);
      }
    } else {  // flat row
      for (int64_t i = 0; i < row.row_val_.count_; ++i) {
        checksum = row.row_val_.cells_[i].checksum(checksum);
      }
    }
    return checksum;
  }

  int64_t get_micro_block_checksum() const
  {
    return micro_block_checksum_;
  }
  int64_t get_max_merged_trans_version() const
  {
    return max_merged_trans_version_;
  }
  void update_max_merged_trans_version(const int64_t max_merged_trans_version)
  {
    if (max_merged_trans_version > max_merged_trans_version_) {
      max_merged_trans_version_ = max_merged_trans_version;
    }
  }
  bool is_contain_uncommitted_row() const
  {
    return contain_uncommitted_row_;
  }
  void set_contain_uncommitted_row()
  {
    contain_uncommitted_row_ = true;
  }

  VIRTUAL_TO_STRING_KV(
      K_(row_count_delta), K_(micro_block_checksum), K_(contain_uncommitted_row), K_(max_merged_trans_version));

protected:
  OB_INLINE void cal_delta(const storage::ObStoreRow& row)
  {
    if (row.row_type_flag_.is_first_multi_version_row()) {
      // only add delta once for each row key
      row_count_delta_ += row.get_delta();
    }
    STORAGE_LOG(DEBUG, "cal delta", K(row), K(row.row_type_flag_), K_(row_count_delta));
  }

  OB_INLINE bool need_cal_row_checksum() const
  {
    return MICRO_BLOCK_MERGE_VERIFY_LEVEL::NONE < micro_block_merge_verify_level_;
  }

  void set_micro_block_merge_verify_level()
  {
    micro_block_merge_verify_level_ = GCONF.micro_block_merge_verify_level;
  }

protected:
  // row count delta of the current micro block
  int32_t row_count_delta_;
  int64_t micro_block_checksum_;
  int64_t micro_block_merge_verify_level_;
  int64_t max_merged_trans_version_;
  bool contain_uncommitted_row_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_WRITER_H_
