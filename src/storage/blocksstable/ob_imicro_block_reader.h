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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_READER_H_
#define OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_READER_H_

#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/ob_store_format.h"
#include "common/ob_store_range.h"
#include "ob_block_sstable_struct.h"
#include "ob_row_reader.h"
#include "ob_macro_block_meta_mgr.h"

namespace oceanbase {
namespace storage {
class ObStoreRow;
struct ObStoreRowLockState;
}  // namespace storage

namespace memtable {
class ObIMvccCtx;
}

namespace blocksstable {

class ObColumnMap;
class ObRowCacheValue;
class ObIRowReader;

struct ObRowIndexIterator {
public:
  typedef ObRowIndexIterator self_t;
  typedef std::random_access_iterator_tag iterator_category;
  typedef int64_t value_type;
  typedef int64_t difference_type;
  typedef int64_t* pointer;
  typedef int64_t& reference;

  static const self_t& invalid_iterator()
  {
    static self_t invalid_iter(INT64_MIN);
    return invalid_iter;
  }

  ObRowIndexIterator() : row_id_(0)
  {}
  explicit ObRowIndexIterator(const int64_t id) : row_id_(id)
  {}

  int64_t operator*() const
  {
    return row_id_;
  }
  bool operator==(const self_t& r) const
  {
    return row_id_ == r.row_id_;
  }
  bool operator!=(const self_t& r) const
  {
    return row_id_ != r.row_id_;
  }
  bool operator<(const self_t& r) const
  {
    return row_id_ < r.row_id_;
  }
  bool operator>(const self_t& r) const
  {
    return row_id_ > r.row_id_;
  }
  bool operator>=(const self_t& r) const
  {
    return row_id_ >= r.row_id_;
  }
  bool operator<=(const self_t& r) const
  {
    return row_id_ <= r.row_id_;
  }
  difference_type operator-(const self_t& r) const
  {
    return row_id_ - r.row_id_;
  }
  self_t operator-(difference_type step) const
  {
    return self_t(row_id_ - step);
  }
  self_t operator+(difference_type step) const
  {
    return self_t(row_id_ + step);
  }
  self_t& operator-=(difference_type step)
  {
    row_id_ -= step;
    return *this;
  }
  self_t& operator+=(difference_type step)
  {
    row_id_ += step;
    return *this;
  }
  self_t& operator++()
  {
    row_id_++;
    return *this;
  }
  self_t operator++(int)
  {
    return self_t(row_id_++);
  }
  self_t& operator--()
  {
    row_id_--;
    return *this;
  }
  self_t operator--(int)
  {
    return self_t(row_id_--);
  }

  TO_STRING_KV(K_(row_id));

  int64_t row_id_;
};

struct ObMicroBlockData {
public:
  ObMicroBlockData() : buf_(NULL), size_(0), extra_buf_(0), extra_size_(0)
  {}
  ObMicroBlockData(const char* buf, const int64_t size, const char* extra_buf = NULL, const int64_t extra_size = 0)
      : buf_(buf), size_(size), extra_buf_(extra_buf), extra_size_(extra_size)
  {}
  bool is_valid() const
  {
    return NULL != buf_ && size_ > 0;
  }
  const char*& get_buf()
  {
    return buf_;
  }
  const char* get_buf() const
  {
    return buf_;
  }
  int64_t& get_buf_size()
  {
    return size_;
  }
  int64_t get_buf_size() const
  {
    return size_;
  }

  const char*& get_extra_buf()
  {
    return extra_buf_;
  }
  const char* get_extra_buf() const
  {
    return extra_buf_;
  }
  int64_t get_extra_size() const
  {
    return extra_size_;
  }
  int64_t& get_extra_size()
  {
    return extra_size_;
  }

  int64_t total_size() const
  {
    return size_ + extra_size_;
  }

  void reset()
  {
    *this = ObMicroBlockData();
  }

  TO_STRING_KV(KP_(buf), K_(size), K_(extra_size));

  const char* buf_;
  int64_t size_;
  const char* extra_buf_;
  int64_t extra_size_;
};

class ObIMicroBlockGetReader {
public:
  ObIMicroBlockGetReader(){};
  virtual ~ObIMicroBlockGetReader(){};
  virtual int get_row(const uint64_t tenant_id, const ObMicroBlockData& block_data, const common::ObStoreRowkey& rowkey,
      const ObColumnMap& column_map, const ObFullMacroBlockMeta& macro_meta,
      const storage::ObSSTableRowkeyHelper* rowkey_helper, storage::ObStoreRow& row) = 0;
  virtual int get_row(const uint64_t tenant_id, const ObMicroBlockData& block_data, const common::ObStoreRowkey& rowkey,
      const ObFullMacroBlockMeta& macro_meta, const storage::ObSSTableRowkeyHelper* rowkey_helper,
      storage::ObStoreRow& row) = 0;
  virtual int exist_row(const uint64_t tenant_id, const ObMicroBlockData& block_data,
      const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
      const storage::ObSSTableRowkeyHelper* rowkey_helper, bool& exist, bool& found) = 0;
  virtual int check_row_locked(memtable::ObIMvccCtx& ctx, const transaction::ObTransStateTableGuard& trans_table_guard,
      const transaction::ObTransID& read_trans_id, const ObMicroBlockData& block_data,
      const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& full_meta,
      const storage::ObSSTableRowkeyHelper* rowkey_helper, storage::ObStoreRowLockState& lock_state)
  {
    int ret = common::OB_NOT_SUPPORTED;
    UNUSED(ctx);
    UNUSED(trans_table_guard);
    UNUSED(read_trans_id);
    UNUSED(block_data);
    UNUSED(rowkey);
    UNUSED(full_meta);
    UNUSED(lock_state);
    UNUSED(rowkey_helper);
    return ret;
  }
};

class ObIMicroBlockReader {
public:
  typedef const common::ObIArray<int32_t> Projector;
  static const int64_t OB_MAX_BATCH_ROW_COUNT = 64;
  static const int64_t INVALID_ROW_INDEX = -1;

  ObIMicroBlockReader()
      : is_inited_(false),
        begin_(0),
        end_(INVALID_ROW_INDEX),
        column_map_(nullptr),
        reader_(nullptr),
        output_row_type_(common::MAX_ROW_STORE)
  {}
  virtual ~ObIMicroBlockReader()
  {}
  virtual int init(const ObMicroBlockData& block_data, const ObColumnMap* column_map,
      const common::ObRowStoreType out_type = common::FLAT_ROW_STORE) = 0;
  virtual void reset();
  virtual int get_row(const int64_t index, storage::ObStoreRow& row) = 0;
  // [begin_index, end_index)
  virtual int get_rows(const int64_t begin_index, const int64_t end_index, const int64_t row_capacity,
      storage::ObStoreRow* rows, int64_t& row_count) = 0;
  virtual int get_row_header(const int64_t row_idx, const ObRowHeader*& row_header) = 0;
  virtual int get_row_count(int64_t& row_count) = 0;
  virtual int get_multi_version_info(const int64_t row_idx, const int64_t version_column_idx,
      const int64_t sql_sequence_idx, storage::ObMultiVersionRowFlag& flag, transaction::ObTransID& trans_id,
      int64_t& version, int64_t& sql_sequence) = 0;
  int locate_rowkey(const common::ObStoreRowkey& rowkey, int64_t& row_idx);
  int locate_range(const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
      int64_t& begin_idx, int64_t& end_idx);

  inline bool is_inited() const
  {
    return is_inited_;
  }
  inline int64_t begin() const
  {
    return begin_;
  }
  inline int64_t end() const
  {
    return end_;
  }

protected:
  virtual int find_bound(const common::ObStoreRowkey& key, const bool lower_bound, const int64_t begin_idx,
      const int64_t end_idx, int64_t& row_idx, bool& equal) = 0;

protected:
  bool is_inited_;
  int64_t begin_;  // begin of row index, inclusive
  int64_t end_;    // end of row index, not inclusive
  const ObColumnMap* column_map_;
  ObIRowReader* reader_;
  common::ObRowStoreType output_row_type_;
};

class ObMicroBlock {
public:
  ObMicroBlock()
      : range_(),
        data_(),
        column_map_(NULL),
        row_store_type_(common::FLAT_ROW_STORE),
        row_count_(0),
        column_cnt_(0),
        column_checksums_(nullptr),
        meta_(),
        origin_data_size_(0),
        header_version_(0)
  {}

  inline bool is_valid() const
  {
    return range_.is_valid() && data_.is_valid() && NULL != column_map_ &&
           common::ObStoreFormat::is_row_store_type_valid(row_store_type_) && meta_.is_valid();
  }

  TO_STRING_KV(K_(range), K_(data), K_(column_map), K_(row_store_type), K_(row_count), K_(column_cnt),
      KP_(column_checksums), K_(meta), K_(origin_data_size));

  common::ObStoreRange range_;
  ObMicroBlockData data_;
  ObMicroBlockData payload_data_;
  ObColumnMap* column_map_;
  common::ObRowStoreType row_store_type_;
  int64_t row_count_;
  int64_t column_cnt_;
  int64_t* column_checksums_;
  blocksstable::ObFullMacroBlockMeta meta_;
  int64_t origin_data_size_;
  int64_t header_version_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_BLOCKSSTABLE_OB_IMICRO_BLOCK_READER_H_
