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
#include "share/ob_define.h"
#include "ob_datum_rowkey.h"
#include "storage/ob_i_store.h"
#include "ob_macro_block_id.h"
#include "ob_micro_block_hash_index.h"
#include "ob_micro_block_header.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObMicroBlockDesc
{
  ObDatumRowkey last_rowkey_;
  const char *buf_; // buf does not contain any header
  const ObMicroBlockHeader *header_;
  int64_t buf_size_;
  int64_t data_size_; // encoding data size
  int64_t original_size_; // original data size
  int64_t row_count_;
  int64_t column_count_;
  int64_t max_merged_trans_version_;
  // <macro_id_, block_offset_> used for index
  MacroBlockId macro_id_;
  int64_t block_offset_;
  int64_t block_checksum_;
  int32_t row_count_delta_;
  bool contain_uncommitted_row_;
  bool can_mark_deletion_;
  bool has_string_out_row_;
  bool has_lob_out_row_;
  bool is_last_row_last_flag_;

  ObMicroBlockDesc() { reset(); }
  bool is_valid() const;
  void reset();
  int64_t get_block_size() const { return buf_size_ + header_->header_size_; }

  TO_STRING_KV(
      K_(last_rowkey),
      KPC_(header),
      KP_(buf),
      K_(buf_size),
      K_(data_size),
      K_(row_count),
      K_(column_count),
      K_(max_merged_trans_version),
      K_(macro_id),
      K_(block_offset),
      K_(block_checksum),
      K_(row_count_delta),
      K_(contain_uncommitted_row),
      K_(can_mark_deletion),
      K_(has_string_out_row),
      K_(has_lob_out_row),
      K_(is_last_row_last_flag),
      K_(original_size));
};
enum MICRO_BLOCK_MERGE_VERIFY_LEVEL
{
  NONE = 0,
  ENCODING = 1,
  ENCODING_AND_COMPRESSION = 2,
  ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE = 3,
};

// Some common interface of ObMicroBlockWriter and ObMicroBlockEncoder, not all features.
class ObIMicroBlockWriter
{
  static const int64_t DEFAULT_UPPER_BOUND = 1 << 30;
public:
  ObIMicroBlockWriter() :
    row_count_delta_(0),
    last_rows_count_(0),
    micro_block_checksum_(0),
    micro_block_merge_verify_level_(MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION),
    max_merged_trans_version_(0),
    block_size_upper_bound_(DEFAULT_UPPER_BOUND),
    contain_uncommitted_row_(false),
    has_string_out_row_(false),
    has_lob_out_row_(false),
    need_check_lob_(false),
    is_last_row_last_flag_(false),
    header_(nullptr)
  {
  }
  virtual ~ObIMicroBlockWriter() {}
  virtual int append_row(const ObDatumRow &row) = 0;
  virtual int build_block(char *&buf, int64_t &size) = 0;
  virtual int64_t get_row_count() const = 0;
  virtual int64_t get_data_size() const = 0;
  virtual int64_t get_block_size() const = 0;
  virtual int64_t get_column_count() const = 0;
  virtual int64_t get_original_size() const = 0;
  virtual void reset() = 0;
  virtual void dump_diagnose_info() const {};
  virtual int append_hash_index(ObMicroBlockHashIndexBuilder& hash_index_builder)
  {
    int ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Unspported micro block format for hash index", K(ret));
    return ret;
  }
  virtual bool has_enough_space_for_hash_index(const int64_t hash_index_size) const
  {
    return false;
  }
  virtual void reuse()
  {
    row_count_delta_ = 0;
    last_rows_count_ = 0;
    micro_block_checksum_ = 0;
    max_merged_trans_version_ = 0;
    block_size_upper_bound_ = DEFAULT_UPPER_BOUND;
    contain_uncommitted_row_ = false;
    has_string_out_row_ = false;
    has_lob_out_row_ = false;
    is_last_row_last_flag_ = false;
  }
  void set_block_size_upper_bound(const int64_t &size) { block_size_upper_bound_ = size; }
  int build_micro_block_desc(ObMicroBlockDesc &micro_block_desc);
  int32_t get_row_count_delta() const { return row_count_delta_; }
  int64_t get_micro_block_merge_verify_level() const
  {
    return micro_block_merge_verify_level_;
  }

  static int64_t cal_row_checksum(const ObDatumRow &row, int64_t checksum)
  {
    for (int64_t i = 0; i < row.get_column_count(); ++i) {
      checksum = row.storage_datums_[i].checksum(checksum);
    }
    return checksum;
  }
  static int cal_column_checksum(const ObDatumRow &row, int64_t *curr_micro_column_checksum)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!row.is_valid() || nullptr == curr_micro_column_checksum)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), K(row), K(curr_micro_column_checksum));
    } else {
      for (int64_t i = 0; i < row.get_column_count(); ++i) {
        curr_micro_column_checksum[i] += row.storage_datums_[i].checksum(0);
      }
    }
    return ret;
  }

  int64_t get_micro_block_checksum() const { return micro_block_checksum_; }
  int64_t get_max_merged_trans_version() const { return max_merged_trans_version_; }
  void update_max_merged_trans_version(const int64_t max_merged_trans_version)
  {
    if (max_merged_trans_version > max_merged_trans_version_) {
      max_merged_trans_version_ = max_merged_trans_version;
    }
  }
  bool is_contain_uncommitted_row() const { return contain_uncommitted_row_; }
  inline bool has_string_out_row() const { return has_string_out_row_; }
  inline bool has_lob_out_row() const { return has_lob_out_row_; }
  inline bool is_last_row_last_flag() const { return is_last_row_last_flag_; }
  void set_contain_uncommitted_row()
  {
    contain_uncommitted_row_ = true;
  }

  void set_micro_block_merge_verify_level(const int64_t verify_level)
  {
    micro_block_merge_verify_level_ = verify_level;
  }
  VIRTUAL_TO_STRING_KV(K_(row_count_delta), K_(micro_block_checksum), K_(contain_uncommitted_row),
      K_(max_merged_trans_version));

protected:
  OB_INLINE void cal_row_stat(const ObDatumRow &row)
  {
    if (row.mvcc_row_flag_.is_first_multi_version_row()) {
      // only add delta once for each row key
      row_count_delta_ += row.get_delta();
    }
    //TODO @hanhui use single flag
    if ((row.row_flag_.is_insert() || row.row_flag_.is_update()) &&
        !row.is_uncommitted_row() &&
        !row.is_ghost_row() &&
        row.is_last_multi_version_row()) {
      ++last_rows_count_;
    }
    is_last_row_last_flag_ = row.is_last_multi_version_row();
    STORAGE_LOG(DEBUG, "cal row stat", K(row), K(row.mvcc_row_flag_), K_(row_count_delta), K_(last_rows_count));
  }

  OB_INLINE bool need_cal_row_checksum() const
  {
    return MICRO_BLOCK_MERGE_VERIFY_LEVEL::NONE < micro_block_merge_verify_level_;
  }

public:
  static const int64_t DEFAULT_MICRO_MAX_SIZE = 256 * 1024; //256K

protected:
  // row count delta of the current micro block
  int32_t row_count_delta_;
  // count of rows contain last flag
  int32_t last_rows_count_;
  int64_t micro_block_checksum_;
  int64_t micro_block_merge_verify_level_;
  int64_t max_merged_trans_version_;
  int64_t block_size_upper_bound_;
  bool contain_uncommitted_row_;
  bool has_string_out_row_;
  bool has_lob_out_row_;
  bool need_check_lob_;
  bool is_last_row_last_flag_;
  ObMicroBlockHeader *header_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_WRITER_H_
