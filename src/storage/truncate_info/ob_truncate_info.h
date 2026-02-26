//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_TRUNCATE_INFO_H_
#define OB_STORAGE_TRUNCATE_INFO_H_
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/schema/ob_list_row_values.h" // ObListRowValues
namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
struct ObTruncateInfoKey final
{
public:
  OB_UNIS_VERSION(1);
  static constexpr uint8_t MAGIC_NUMBER = 0xFF; // if meet compat case, abort directly for now
public:
  ObTruncateInfoKey()
    : tx_id_(0),
      inc_seq_(0)
  {}
  ObTruncateInfoKey(const ObTruncateInfoKey &other)
    : tx_id_(other.tx_id_),
      inc_seq_(other.inc_seq_)
  {}
  ~ObTruncateInfoKey() = default;
  ObTruncateInfoKey &operator=(const ObTruncateInfoKey &other)
  {
    tx_id_ = other.tx_id_;
    inc_seq_ = other.inc_seq_;
    return *this;
  }
  bool operator==(const ObTruncateInfoKey &other) const
  {
    return tx_id_ == other.tx_id_ && inc_seq_ == other.inc_seq_;
  }
  bool operator!=(const ObTruncateInfoKey &other) const
  {
    return !(*this == other);
  }
  bool operator<(const ObTruncateInfoKey &other) const
  {
    return ((tx_id_ < other.tx_id_) || (tx_id_ == other.tx_id_ && inc_seq_ < other.inc_seq_));
  }

  void reset() { tx_id_ = 0; inc_seq_ = 0;}
  bool is_valid() const { return tx_id_ > 0 && inc_seq_ >= 0; }
  int mds_serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int mds_deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int64_t mds_get_serialize_size() const;
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;

  TO_STRING_KV(K_(tx_id), K_(inc_seq));
public:
  int64_t tx_id_;
  int64_t inc_seq_;
};

struct ObStorageListRowValues final
{
public:
  ObStorageListRowValues()
    : cnt_(0), values_(nullptr)
  {}
  virtual ~ObStorageListRowValues();
  void destroy(common::ObIAllocator &allocator);
  void reset()
  {
    cnt_ = 0;
    values_ = nullptr;
  }
  bool is_valid() const { return 0 == cnt_ || (cnt_ > 0 && nullptr != values_); }
  int64_t count() const { return cnt_; }
  const ObNewRow *get_values() const { return values_; }
  const common::ObNewRow &at(const int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < cnt_ && nullptr != values_);
    return values_[idx];
  }
  int init(
    ObIAllocator &allocator,
    const common::ObIArray<common::ObNewRow> &row_values);
  int assign(ObIAllocator &allocator, const ObStorageListRowValues &other);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_deep_copy_size() const;
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObStorageListRowValues &dest) const;
  void shallow_copy(ObStorageListRowValues &dest) const
  { // Careful to use
    dest.values_ = values_;
    dest.cnt_ = cnt_;
  }
  int compare(const ObStorageListRowValues &other, bool &equal) const;
  bool operator ==(const ObStorageListRowValues &other) const = delete;
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int64_t cnt_;
  ObNewRow *values_; // row array & new rows memory is allocated together
  DISALLOW_COPY_AND_ASSIGN(ObStorageListRowValues);
};

struct ObPartKeyIdxArray final
{
public:
  ObPartKeyIdxArray()
    : cnt_(0),
      col_idxs_(nullptr)
  {}
  ~ObPartKeyIdxArray();
  void reset()
  {
    cnt_ = 0;
    col_idxs_ = nullptr;
  }
  void destroy(common::ObIAllocator &allocator);
  bool is_valid() const;
  int64_t count() const { return cnt_; }
  int64_t at(const int64_t i) const { OB_ASSERT(i >= 0 && i < cnt_); return col_idxs_[i]; }
  int init(
    common::ObIAllocator &allocator,
    const ObIArray<int64_t> &part_key_idxs);
  int assign(common::ObIAllocator &allocator, const ObPartKeyIdxArray &other);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_deep_copy_size() const;
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObPartKeyIdxArray &dest) const;
  void shallow_copy(ObPartKeyIdxArray &dest) const
  { // Careful to use
    dest.cnt_ = cnt_;
    dest.col_idxs_ = col_idxs_;
  }
  int compare(const ObPartKeyIdxArray &other, bool &equal) const;
  bool operator ==(const ObPartKeyIdxArray &other) const = delete;

  TO_STRING_KV(K_(cnt), "array", ObArrayWrap<int64_t>(col_idxs_, cnt_));
private:
  int inner_init(
     common::ObIAllocator &allocator,
     const int64_t count,
     const int64_t *data);
private:
  int64_t cnt_;
  int64_t *col_idxs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPartKeyIdxArray);
};

struct ObTruncatePartition
{
  enum TruncatePartType : uint8_t
  {
    RANGE_PART = 0,
    RANGE_COLUMNS_PART = 1,
    LIST_PART = 2,
    LIST_COLUMNS_PART = 3,
    PART_TYPE_MAX
  };
  // when truncate list partition of DEFAULT value, need record all other LIST_VALUES, and set EXCEPT OP
  // otherwhise, set INCLUDE OP, means all rows in recording range & list is truncated/dropped
  enum TruncatePartOp : uint8_t
  {
    INCLUDE = 0,
    EXCEPT = 1,
    ALL = 2, // when truncate only list part of DEFAULT, will sync truncate ALL
    PART_OP_MAX
  };
  static bool is_valid_part_type(const TruncatePartType type) { return type >= RANGE_PART && type < PART_TYPE_MAX; }
  static bool is_range_part(const TruncatePartType type) { return RANGE_PART == type || RANGE_COLUMNS_PART == type; }
  static bool is_list_part(const TruncatePartType type) { return LIST_PART == type || LIST_COLUMNS_PART == type; }
  static bool is_valid_part_op(const TruncatePartOp op) { return op >= INCLUDE && op < PART_OP_MAX; }
  static const char *part_type_to_str(const TruncatePartType &type);
  static const char *part_op_to_str(const TruncatePartOp &op);
  ObTruncatePartition()
    : part_type_(PART_TYPE_MAX),
      part_op_(PART_OP_MAX),
      part_key_idxs_(),
      low_bound_val_(),
      high_bound_val_(),
      list_row_values_()
  {}
  bool is_valid() const;
  void reset() // be careful, if use allocator to init, need use destroy to free
  {
    part_type_ = PART_TYPE_MAX;
    part_op_ = PART_OP_MAX;
    part_key_idxs_.reset();
    low_bound_val_.reset();
    high_bound_val_.reset();
    list_row_values_.reset();
  }
  void destroy(common::ObIAllocator &allocator)
  {
    part_type_ = PART_TYPE_MAX;
    part_op_ = PART_OP_MAX;
    part_key_idxs_.destroy(allocator);
    low_bound_val_.destroy(allocator);
    high_bound_val_.destroy(allocator);
    list_row_values_.destroy(allocator);
  }
  template <typename T>
  int init_truncate_part(
    ObIAllocator &allocator,
    const TruncatePartType part_type,
    const T &found_part,
    const T *prev_part);
  int init_range_part(
    ObIAllocator &allocator,
    const TruncatePartType part_type,
    const common::ObRowkey &low_bound_val,
    const common::ObRowkey &high_bound_val);
  int init_list_part(
    ObIAllocator &allocator,
    const TruncatePartType part_type,
    const TruncatePartOp part_op,
    const share::schema::ObListRowValues &list_row_values);
  int assign(ObIAllocator &allocator, const ObTruncatePartition &other);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_deep_copy_size() const;
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObTruncatePartition &dest) const;
  int shallow_copy(ObTruncatePartition &dest); // Careful to use
  int compare(const ObTruncatePartition &other, bool &equal) const;
  bool operator ==(const ObTruncatePartition &other) const = delete;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  TruncatePartType part_type_;
  TruncatePartOp part_op_;
  ObPartKeyIdxArray part_key_idxs_;
  common::ObRowkey low_bound_val_;
  common::ObRowkey high_bound_val_;
  ObStorageListRowValues list_row_values_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTruncatePartition);
};

struct ObTruncateInfo final
{
public:
  ObTruncateInfo();
  ~ObTruncateInfo() { destroy(); }
  void destroy();
  void reset() // be careful, if use allocator to init, need use destroy to free
  {
    truncate_part_.reset();
    truncate_subpart_.reset();
  }
  bool is_valid() const;
  int assign(ObIAllocator &allocator, const ObTruncateInfo &other);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_deep_copy_size() const;
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObTruncateInfo &dest) const;
  int shallow_copy(ObTruncateInfo &dest); // Careful to use
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn);
  int compare_truncate_part_info(const ObTruncateInfo &other, bool &equal) const;
  bool operator ==(const ObTruncateInfo &other) const = delete;
  TO_STRING_KV(K_(key), K_(commit_version), K_(schema_version), K_(truncate_part),
    K_(is_sub_part), K_(truncate_subpart), KP_(allocator));
  static const int64_t TRUNCATE_INFO_VERSION_V1 = 1;
  static const int64_t TRUNCATE_INFO_VERSION_LATEST = TRUNCATE_INFO_VERSION_V1;
public:
  static const int32_t TI_ONE_BYTE = 8;
  static const int32_t TI_ONE_BIT = 1;
  static const int32_t TI_RESERVED_BITS = 55;
  union {
    uint64_t info_;
    struct
    {
      uint64_t version_     : TI_ONE_BYTE;
      uint64_t is_sub_part_ : TI_ONE_BIT;
      uint64_t reserved_    : TI_RESERVED_BITS;
    };
  };
  ObTruncateInfoKey key_;
  int64_t commit_version_;
  int64_t schema_version_;
  ObTruncatePartition truncate_part_;
  ObTruncatePartition truncate_subpart_;
  ObIAllocator *allocator_;
private:
  int compare(const ObTruncateInfo &other, bool &equal) const; // for unittest
  DISALLOW_COPY_AND_ASSIGN(ObTruncateInfo);
};

template <typename T>
int ObTruncatePartition::init_truncate_part(
  ObIAllocator &allocator,
  const TruncatePartType part_type,
  const T &found_part,
  const T *prev_part)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_part_type(part_type))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "input prev part is null", K(ret), K(part_type));
  } else if (is_range_part(part_type)) {
    if (OB_FAIL(init_range_part(
          allocator,
          part_type,
          nullptr == prev_part ? ObRowkey::MIN_ROWKEY : prev_part->get_high_bound_val(),
          found_part.get_high_bound_val()))) {
      STORAGE_LOG(WARN, "failed to init truncate range part", K(ret), K(part_type), K(found_part));
    } else {
      STORAGE_LOG(INFO, "success to init range part", K(ret), KPC(this), KPC(prev_part));
    }
  } else if (is_list_part(part_type)) {
    if (OB_FAIL(init_list_part(
        allocator,
        part_type,
        ObTruncatePartition::INCLUDE,
        found_part.get_list_row_values_struct()))) {
      STORAGE_LOG(WARN, "failed to init truncate list part", K(ret), K(part_type), K(found_part));
    } else {
      STORAGE_LOG(INFO, "success to init list part", K(ret), KPC(this), KPC(prev_part));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected part type", K(ret), K(part_type));
  }
  return ret;
}

} // namespace strorage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_H_
