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
#include "ob_micro_block_checksum_helper.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObMicroBlockDesc
{
  ObDatumRowkey last_rowkey_;
  const char *buf_; // buf does not contain any header
  const ObMicroBlockHeader *header_;
  const ObDatumRow *aggregated_row_;
  int64_t buf_size_;
  int64_t data_size_; // encoding data size
  int64_t original_size_; // original data size
  int64_t row_count_;
  int64_t column_count_;
  int64_t max_merged_trans_version_;
  // <macro_id_, block_offset_> used for index
  MacroBlockId macro_id_;
  ObLogicMicroBlockId logic_micro_id_;
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
  const char *get_block_buf() const { return reinterpret_cast<const char *>(header_); }

  int deep_copy(
    common::ObIAllocator& allocator,
    ObMicroBlockDesc& dst) const;

  TO_STRING_KV(
      K_(last_rowkey),
      KPC_(header),
      KP_(buf),
      KPC_(aggregated_row),
      K_(buf_size),
      K_(data_size),
      K_(row_count),
      K_(column_count),
      K_(max_merged_trans_version),
      K_(macro_id),
      K_(logic_micro_id),
      K_(block_offset),
      K_(block_checksum),
      K_(row_count_delta),
      K_(contain_uncommitted_row),
      K_(can_mark_deletion),
      K_(has_string_out_row),
      K_(has_lob_out_row),
      K_(is_last_row_last_flag),
      K_(original_size));

  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockDesc);
};

enum MICRO_BLOCK_MERGE_VERIFY_LEVEL
{
  NONE = 0,
  ENCODING = 1,
  ENCODING_AND_COMPRESSION = 2,
  ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE = 3,
};

class ObMicroBufferWriter : public compaction::ObCompactionBuffer
{
public:
  ObMicroBufferWriter(const int64_t page_size = DEFAULT_MIDDLE_BLOCK_SIZE)
    : ObCompactionBuffer("MicroBuffer", page_size)
  {}
  virtual ~ObMicroBufferWriter() { reset(); };
  int write_row(const ObDatumRow &row, const int64_t rowkey_cnt, int64_t &len);
};

// Some common interface of ObMicroBlockWriter and ObMicroBlockEncoder, not all features.
class ObIMicroBlockWriter
{
  static const int64_t DEFAULT_UPPER_BOUND = 1 << 30;
public:
  ObIMicroBlockWriter() :
    row_count_delta_(0),
    last_rows_count_(0),
    micro_block_merge_verify_level_(MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION),
    max_merged_trans_version_(0),
    min_merged_trans_version_(INT64_MAX),
    block_size_upper_bound_(DEFAULT_UPPER_BOUND),
    contain_uncommitted_row_(false),
    has_string_out_row_(false),
    has_lob_out_row_(false),
    need_check_lob_(false),
    is_last_row_last_flag_(false),
    checksum_helper_()
  {
  }
  virtual ~ObIMicroBlockWriter() {}
  virtual int append_row(const ObDatumRow &row) = 0;
  virtual int append_batch(const ObBatchDatumRows &vec_batch,
                           const int64_t start,
                           const int64_t row_count)
  {
    int ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "Unspport append_batch", K(ret));
    return ret;
  }
  virtual int build_block(char *&buf, int64_t &size) = 0;
  virtual int64_t get_row_count() const = 0;
  virtual int64_t get_block_size() const = 0; // estimate block size after encoding
  virtual int64_t get_original_size() const = 0; // estimate block size before ecnoding
  virtual int64_t get_column_count() const = 0;
  virtual void reset()
  {
    ObIMicroBlockWriter::reuse();
    checksum_helper_.reset();
  }
  virtual void dump_diagnose_info() { STORAGE_LOG(INFO, "IMicroBlockWriter", K(checksum_helper_)); }
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
    checksum_helper_.reuse();
    max_merged_trans_version_ = 0;
    min_merged_trans_version_ = INT64_MAX;
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

  int64_t get_micro_block_checksum() const { return checksum_helper_.get_row_checksum(); }
  int64_t get_max_merged_trans_version() const { return max_merged_trans_version_; }
  inline void update_merged_trans_version(const int64_t merged_trans_version)
  {
    update_max_merged_trans_version(merged_trans_version);
    update_min_merged_trans_version(merged_trans_version);
  }
  void update_max_merged_trans_version(const int64_t max_merged_trans_version)
  {
    if (max_merged_trans_version > max_merged_trans_version_) {
      max_merged_trans_version_ = max_merged_trans_version;
    }
  }
  inline void update_min_merged_trans_version(const int64_t min_merged_trans_version)
  {
    if (OB_UNLIKELY(min_merged_trans_version < min_merged_trans_version_)) {
      min_merged_trans_version_ = min_merged_trans_version;
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
  VIRTUAL_TO_STRING_KV(K_(row_count_delta), K_(contain_uncommitted_row),
      K_(max_merged_trans_version), K_(checksum_helper));

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

  OB_INLINE ObMicroBlockHeader* get_header(ObMicroBufferWriter &data_buffer)
  {
    ObMicroBlockHeader *header = reinterpret_cast<ObMicroBlockHeader*>(data_buffer.data());
    calc_column_checksums_ptr(data_buffer);
    return header;
  }

  OB_INLINE void calc_column_checksums_ptr(ObMicroBufferWriter &data_buffer)
  {
    ObMicroBlockHeader *header = reinterpret_cast<ObMicroBlockHeader*>(data_buffer.data());
    if (header->column_checksums_ != nullptr && header->has_column_checksum_) {
      header->column_checksums_ = reinterpret_cast<int64_t *>(
            data_buffer.data() + ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET);
    }
  }
public:
  static const int64_t DEFAULT_MICRO_MAX_SIZE = 256 * 1024; //256K

protected:
  // row count delta of the current micro block
  int32_t row_count_delta_;
  // count of rows contain last flag
  int32_t last_rows_count_;
  int64_t micro_block_merge_verify_level_;
  int64_t max_merged_trans_version_;
  int64_t min_merged_trans_version_;
  int64_t block_size_upper_bound_;
  bool contain_uncommitted_row_;
  bool has_string_out_row_;
  bool has_lob_out_row_;
  bool need_check_lob_;
  bool is_last_row_last_flag_;
  ObMicroBlockChecksumHelper checksum_helper_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_WRITER_H_
