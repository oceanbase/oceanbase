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

class ObMicroBufferWriter final
{
public:
  ObMicroBufferWriter(const int64_t page_size = DEFAULT_MIDDLE_BLOCK_SIZE)
    : allocator_(MTL_ID(), "MicroBuffer"),
      is_inited_(false),
      capacity_(0),
      buffer_size_(0),
      len_(0),
      data_(nullptr),
      reset_memory_threshold_(0),
      memory_reclaim_cnt_(0),
      has_expand_(false),
      lazy_move_(false),
      old_buf_(nullptr),
      old_size_(0)
  {}
  ~ObMicroBufferWriter() { reset(); };
  int init(const int64_t capacity, const int64_t reserve_size = DEFAULT_MIDDLE_BLOCK_SIZE);
  inline bool is_inited() const { return is_inited_; }
  inline int64_t remain() const { return capacity_ - len_; }
  inline int64_t remain_buffer_size() const { return buffer_size_ - len_; }
  inline int64_t size() const { return buffer_size_; } //curr buffer size
  inline bool has_expand() const { return has_expand_; }
  inline char *data() { assert(old_buf_ == nullptr); return data_; }
  inline char *current() { return data_ + len_; }
  int reserve(const int64_t size);
  int ensure_space(const int64_t append_size);
  // don't use it, only for encoding
  int set_lazy_move_cur_buf()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(old_buf_ != nullptr)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected old buf", K(ret));
    } else {
      lazy_move_ = true;
    }
    return ret;
  }
  void move_buf()
  {
    lazy_move_ = false;
    if (old_buf_ != nullptr) {
      MEMCPY(data_, old_buf_, old_size_);
      allocator_.free(old_buf_);
      old_buf_ = nullptr;
      old_size_ = 0;
    }
  }
  inline void pop_back(const int64_t size) { len_ = MAX(0, len_ - size); }
  int write_nop(const int64_t size, bool is_zero = false);
  int write(const ObDatumRow &row, const int64_t rowkey_cnt, int64_t &len);
  int write(const void *buf, int64_t size);
  template<typename T>
  int write(const T &value)
  {
    int ret = OB_SUCCESS;
    static_assert(std::is_trivially_copyable<T>::value, "invalid type");
    if (OB_FAIL(ensure_space(sizeof(T)))) {
      if (ret != OB_BUF_NOT_ENOUGH) {
        STORAGE_LOG(WARN, "failed to ensure space", K(ret), K(sizeof(T)));
      }
    } else {
      *((T *)(data_ + len_)) = value;
      len_ += sizeof(T);
    }
    return ret;
  }
  int advance(const int64_t size);
  int set_length(const int64_t len);

  void reuse();
  void reset();
  inline int64_t length() const { return len_; }
  TO_STRING_KV(K_(capacity), K_(buffer_size), K_(len), K_(data), K_(default_reserve), K_(reset_memory_threshold),
      K_(memory_reclaim_cnt), K_(has_expand), K_(lazy_move), K_(old_buf), K_(old_size));
private:
  int expand(const int64_t size);
private:
  compaction::ObLocalAllocator<common::DefaultPageAllocator> allocator_;
  bool is_inited_;
  int64_t capacity_;
  int64_t buffer_size_; //curr buffer size
  int64_t len_; //curr pos
  char *data_;

  // for reclaim memory
  int64_t default_reserve_;
  int64_t reset_memory_threshold_;
  int64_t memory_reclaim_cnt_;
  bool has_expand_;

  bool lazy_move_;
  char *old_buf_;
  int64_t old_size_;

private:
  static const int64_t MIN_BUFFER_SIZE = 1 << 12; //4kb
  static const int64_t MAX_DATA_BUFFER_SIZE = 2 * common::OB_DEFAULT_MACRO_BLOCK_SIZE; // 4m
  static const int64_t DEFAULT_MIDDLE_BLOCK_SIZE = 1 << 16; //64K
  static const int64_t DEFAULT_RESET_MEMORY_THRESHOLD = 5;
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
  virtual void dump_diagnose_info() const { STORAGE_LOG(INFO, "IMicroBlockWriter", K(checksum_helper_)); }
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
    if (header->column_checksums_ != nullptr) {
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
