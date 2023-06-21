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

#ifndef OB_STORAGE_BLOCKSSTABLE_OB_IMICRO_BLOCK_READER_H_
#define OB_STORAGE_BLOCKSSTABLE_OB_IMICRO_BLOCK_READER_H_

//#include "ob_imicro_block_reader.h"
#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_store_format.h"
#include "common/ob_store_range.h"
#include "share/schema/ob_table_param.h"
#include "storage/access/ob_table_read_info.h"
#include "ob_block_sstable_struct.h"
#include "ob_datum_range.h"
#include "ob_micro_block_hash_index.h"
#include "ob_micro_block_header.h"

namespace oceanbase
{
namespace sql
{
class ObPushdownFilterExecutor;
class ObWhiteFilterExecutor;
};
using namespace storage;
namespace memtable {
class ObIMvccCtx;
};
namespace blocksstable
{
struct ObMicroIndexInfo;

#define FREE_PTR_FROM_CONTEXT(ctx, ptr, T)                                  \
  do {                                                                      \
    if (nullptr != ptr) {                                                   \
      ptr->~T();                                                            \
      if (OB_LIKELY(nullptr != ctx && nullptr != ctx->stmt_allocator_)) {   \
        ctx->stmt_allocator_->free(ptr);                                    \
      }                                                                     \
      ptr = nullptr;                                                        \
    }                                                                       \
  } while (0)

template<typename T>
class ObMicroBlockAggInfo {
public:
  ObMicroBlockAggInfo(bool is_min, const ObDatumCmpFuncType cmp_fun, T &result_datum) :
      is_min_(is_min), cmp_fun_(cmp_fun), result_datum_(result_datum) {}
  int update_min_or_max(const T& datum)
  {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
    } else if (result_datum_.is_null()) {
      result_datum_ = datum;
    } else {
      int cmp_ret = 0;
      if (OB_FAIL(cmp_fun_(result_datum_, datum, cmp_ret))) {
        STORAGE_LOG(WARN, "failed to compare", K(ret));
      } else if ((is_min_ && cmp_ret > 0) || (!is_min_ && cmp_ret < 0)) {
        result_datum_ = datum;
      }
    }
    return ret;
  }
  TO_STRING_KV(K_(is_min), K_(cmp_fun), K_(result_datum));
private:
  bool is_min_;
  const ObDatumCmpFuncType cmp_fun_;
  T &result_datum_;
};

struct ObRowIndexIterator
{
public:
  typedef ObRowIndexIterator self_t;
  typedef std::random_access_iterator_tag iterator_category;
  typedef int64_t value_type;
  typedef int64_t difference_type;
  typedef int64_t *pointer;
  typedef int64_t &reference;

  static const self_t &invalid_iterator()
  { static self_t invalid_iter(INT64_MIN); return invalid_iter; }

  ObRowIndexIterator() : row_id_(0) {}
  explicit ObRowIndexIterator(const int64_t id) : row_id_(id) {}

  int64_t operator *() const { return row_id_; }
  bool operator ==(const self_t &r) const { return row_id_ == r.row_id_; }
  bool operator !=(const self_t &r) const { return row_id_ != r.row_id_; }
  bool operator <(const self_t &r) const { return row_id_ < r.row_id_; }
  bool operator >(const self_t &r) const { return row_id_ > r.row_id_; }
  bool operator >=(const self_t &r) const { return row_id_ >= r.row_id_; }
  bool operator <=(const self_t &r) const { return row_id_ <= r.row_id_; }
  difference_type operator -(const self_t &r) const { return row_id_ - r.row_id_; }
  self_t operator -(difference_type step) const { return self_t(row_id_ - step); }
  self_t operator +(difference_type step) const { return self_t(row_id_ + step); }
  self_t &operator -=(difference_type step) { row_id_ -= step; return *this; }
  self_t &operator +=(difference_type step) { row_id_ += step; return *this; }
  self_t &operator ++() { row_id_ ++; return *this; }
  self_t operator ++(int) { return self_t(row_id_++); }
  self_t &operator --() { row_id_ --; return *this; }
  self_t operator --(int) { return self_t(row_id_--); }

  TO_STRING_KV(K_(row_id));
  int64_t row_id_;
};

struct ObMicroBlockData
{
  enum Type
  {
    DATA_BLOCK,
    INDEX_BLOCK,
    DDL_BLOCK_TREE,
    MAX_TYPE
  };
public:
  ObMicroBlockData(): buf_(NULL), size_(0), extra_buf_(0), extra_size_(0), type_(DATA_BLOCK) {}
  ObMicroBlockData(const char *buf,
                   const int64_t size,
                   const char *extra_buf = nullptr,
                   const int64_t extra_size = 0,
                   const Type block_type = DATA_BLOCK)
      : buf_(buf), size_(size), extra_buf_(extra_buf), extra_size_(extra_size), type_(block_type) {}
  bool is_valid() const { return NULL != buf_ && size_ > 0 && type_ < MAX_TYPE; }
  const char *&get_buf() { return buf_; }
  const char *get_buf() const { return buf_; }
  int64_t &get_buf_size() { return size_; }
  int64_t get_buf_size() const { return size_; }

  const char *&get_extra_buf() { return extra_buf_; }
  const char *get_extra_buf() const { return extra_buf_; }
  int64_t get_extra_size() const { return extra_size_; }
  int64_t &get_extra_size() { return extra_size_; }

  int64_t total_size() const { return size_ + extra_size_; }
  bool is_index_block() const { return INDEX_BLOCK == type_ || DDL_BLOCK_TREE == type_;}

  void reset() { *this = ObMicroBlockData(); }
  OB_INLINE const ObMicroBlockHeader *get_micro_header() const
  {
    const ObMicroBlockHeader *micro_header = reinterpret_cast<const ObMicroBlockHeader *>(buf_);
    const bool is_valid_micro_header =
        size_ >= ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET  && micro_header->is_valid();
    return is_valid_micro_header ? micro_header : nullptr;
  }
  OB_INLINE ObRowStoreType get_store_type() const
  {
    const ObMicroBlockHeader *micro_header = reinterpret_cast<const ObMicroBlockHeader *>(buf_);
    const bool is_valid_micro_header =
        size_ >= ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET && micro_header->is_valid();
    return is_valid_micro_header
      ? static_cast<ObRowStoreType>(micro_header->row_store_type_)
      : MAX_ROW_STORE;
  }

  TO_STRING_KV(KP_(buf), K_(size), KP_(extra_buf), K_(extra_size), K_(type));

  const char *buf_;
  int64_t size_;
  const char *extra_buf_;
  int64_t extra_size_;
  Type type_;
};

class ObMicroBlock
{
public:
  ObMicroBlock()
    : range_(), data_(), payload_data_(), read_info_(nullptr), micro_index_info_(nullptr) {}

  inline bool is_valid() const
  {
    return range_.is_valid() && header_.is_valid() && data_.is_valid() && nullptr != read_info_
      && nullptr != micro_index_info_;
  }

  TO_STRING_KV(K_(range), K_(header), K_(data), K_(payload_data),
      KP_(read_info), KP_(micro_index_info));

  ObDatumRange range_;
  ObMicroBlockHeader header_;
  ObMicroBlockData data_;
  ObMicroBlockData payload_data_;
  const ObITableReadInfo *read_info_;
  const ObMicroIndexInfo *micro_index_info_;
};

struct ObIMicroBlockReaderInfo
{
public:
  static const int64_t INVALID_ROW_INDEX = -1;
  ObIMicroBlockReaderInfo()
      : is_inited_(false),
      row_count_(-1),
      read_info_(nullptr),
	  datum_utils_(nullptr)
  {}
  virtual ~ObIMicroBlockReaderInfo() { reset(); }
  OB_INLINE int64_t row_count() const { return row_count_; }
  OB_INLINE void reset()
  {
    row_count_ = -1;
    read_info_ = nullptr;
    datum_utils_ = nullptr;
    is_inited_ = false;
  }

  bool is_inited_;
  int64_t row_count_;
  const ObITableReadInfo *read_info_;
  const ObStorageDatumUtils *datum_utils_;
};

class ObIMicroBlockGetReader : public ObIMicroBlockReaderInfo
{
public:
  ObIMicroBlockGetReader()
   : ObIMicroBlockReaderInfo()
  {
  }
  virtual ~ObIMicroBlockGetReader() {};
  virtual int get_row(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const ObITableReadInfo &read_info,
      ObDatumRow &row) = 0;
  virtual int exist_row(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const ObITableReadInfo &read_info,
      bool &exist,
      bool &found) = 0;
protected:
  OB_INLINE static int init_hash_index(
      const ObMicroBlockData &block_data,
      ObMicroBlockHashIndex &hash_index,
      const ObMicroBlockHeader *header)
  {
    int ret = OB_SUCCESS;
    hash_index.reset();
    if (header->is_contain_hash_index() && OB_FAIL(hash_index.init(block_data))) {
      STORAGE_LOG(WARN, "failed to init micro block hash index", K(ret), K(block_data));
    }
    return ret;
  }
};

class ObIMicroBlockReader : public ObIMicroBlockReaderInfo
{
public:
  enum ObReaderType
  {
    Reader,
    Decoder,
  };
  ObIMicroBlockReader()
    : ObIMicroBlockReaderInfo()
  {}
  virtual ~ObIMicroBlockReader() {}
  virtual ObReaderType get_type() = 0;
  virtual void reset() { ObIMicroBlockReaderInfo::reset(); }
  virtual int init(
      const ObMicroBlockData &block_data,
      const ObITableReadInfo &read_info) = 0;
  //when there is not read_info in input parameters, it indicates reading all columns from all rows
  //when the incoming datum_utils is nullptr, it indicates not calling locate_range or find_bound
  virtual int init(
      const ObMicroBlockData &block_data,
	  const ObStorageDatumUtils *datum_utils) = 0;
  virtual int get_row(const int64_t index, ObDatumRow &row) = 0;
  virtual int get_row_header(
      const int64_t row_idx,
      const ObRowHeader *&row_header) = 0;
  virtual int get_row_count(int64_t &row_count) = 0;
  virtual int get_multi_version_info(
      const int64_t row_idx,
      const int64_t schema_rowkey_cnt,
      const ObRowHeader *&row_header,
      int64_t &trans_version,
      int64_t &sql_sequence) = 0;
  int locate_range(
      const ObDatumRange &range,
      const bool is_left_border,
      const bool is_right_border,
      int64_t &begin_idx,
      int64_t &end_idx,
      const bool is_index_block = false);
  virtual int get_row_count(
      int32_t col_id,
      const int64_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      int64_t &count)
  {
    UNUSEDx(col_id, row_ids, row_cap, contains_null, count);
    return OB_NOT_SUPPORTED;
  }
  virtual int64_t get_column_count() const = 0;

protected:
  virtual int find_bound(
      const ObDatumRowkey &key,
      const bool lower_bound,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal) = 0;
  virtual int find_bound(const ObDatumRange &range,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal,
      int64_t &end_key_begin_idx,
      int64_t &end_key_end_idx) = 0;
  int validate_filter_info(
      const sql::ObPushdownFilterExecutor &filter,
      const void* col_buf,
      const int64_t col_capacity,
      const ObMicroBlockHeader *header);
  int filter_white_filter(
      const sql::ObWhiteFilterExecutor &filter,
      const common::ObObj &obj,
      bool &filtered);
};

} //end namespace blocksstable
} //end namespace oceanbase
#endif
