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

#ifndef OCEANBASE_COMMON_OB_ROW_DESC_
#define OCEANBASE_COMMON_OB_ROW_DESC_

#include "lib/ob_define.h"
#include "lib/hash/ob_array_index_hash_set.h"

namespace oceanbase
{
namespace common
{

/// Line description
class ObRowDesc
{
public:
  struct Desc
  {
    uint64_t table_id_;
    uint64_t column_id_;

    Desc()
    : table_id_(OB_INVALID_ID), column_id_(OB_INVALID_ID)
    {
    }

    Desc(const uint64_t table_id, const uint64_t column_id)
        : table_id_(table_id), column_id_(column_id)
    {
    }

    inline bool is_invalid() const;
    inline bool operator== (const Desc &other) const;
    bool operator!= (const Desc &other) const;

    inline uint64_t hash() const {return ((table_id_ << 16) | ((column_id_ * 29 + 7) & 0xFFFF));};
  };

public:
  ObRowDesc();
  ~ObRowDesc();
  /**
   * 根据表ID和列ID获得改列在元素数组中的下标
   *
   * @param table_id 表ID
   * @param column_id 列ID
   *
   * @return 下标或者OB_INVALID_INDEX
   */
  int64_t get_idx(const uint64_t table_id, const uint64_t column_id) const;
  /**
   * 根据列下标获得表ID和列ID
   *
   * @param idx
   * @param table_id [out]
   * @param column_id [out]
   *
   * @return OB_SUCCESS或错误码
   */
  int get_tid_cid(const int64_t idx, uint64_t &table_id, uint64_t &column_id) const;

  /// Number of columns in a row
  inline int64_t get_column_num() const;

  /// Add the description information of the next column
  int add_column_desc(const uint64_t table_id, const uint64_t column_id);

  /// Reset
  void reset();

  int64_t get_rowkey_cell_count() const;
  void set_rowkey_cell_count(int64_t rowkey_cell_count);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  NEED_SERIALIZE_AND_DESERIALIZE;

  /// Obtain the total number of hash collisions encountered during internal operation for monitoring and tuning
  uint64_t get_hash_collisions_count() const;

  const Desc *get_cells_desc_array(int64_t &array_size) const;

  ObRowDesc &operator = (const ObRowDesc &r);
  inline bool operator== (const ObRowDesc &other) const;
  inline bool operator!= (const ObRowDesc &other) const;

  ObRowDesc(const ObRowDesc &other);

  int assign(const ObRowDesc &other);

private:
  struct DescIndex
  {
    Desc desc_;
    int64_t idx_;
  };
private:
  static const int64_t MAX_COLUMNS_COUNT = common::OB_ROW_MAX_COLUMNS_COUNT; // 4224
  static uint64_t HASH_COLLISIONS_COUNT;
  // data members
  typedef Desc *CellDescArray;
  CellDescArray cells_desc_;
  char cells_desc_buf_[MAX_COLUMNS_COUNT *sizeof(Desc)];
  int64_t cells_desc_count_;
  int64_t rowkey_cell_count_;
  hash::ObArrayIndexHashSet<CellDescArray, Desc, 733> hash_map_;
};

inline bool ObRowDesc::Desc::operator== (const Desc &other) const
{
  return table_id_ == other.table_id_ && column_id_ == other.column_id_;
}

inline bool ObRowDesc::Desc::operator!= (const Desc &other) const
{
  return !(this->operator ==(other));
}

inline bool ObRowDesc::Desc::is_invalid() const
{
  return (OB_INVALID_ID == table_id_ || OB_INVALID_ID == column_id_);
}

inline void ObRowDesc::reset()
{
  if (0 != cells_desc_count_) {
    cells_desc_count_ = 0;
    rowkey_cell_count_ = 0;
    hash_map_.reset();
  }
}

inline const ObRowDesc::Desc *ObRowDesc::get_cells_desc_array(int64_t &array_size) const
{
  array_size = cells_desc_count_;
  return cells_desc_;
}

inline int64_t ObRowDesc::get_column_num() const
{
  return cells_desc_count_;
}

inline int ObRowDesc::get_tid_cid(const int64_t idx, uint64_t &table_id, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  if (idx < 0
      || idx >= cells_desc_count_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    table_id = cells_desc_[idx].table_id_;
    column_id = cells_desc_[idx].column_id_;
  }
  return ret;
}

inline uint64_t ObRowDesc::get_hash_collisions_count() const
{
  return hash_map_.get_collision_count();
}

inline int64_t ObRowDesc::get_rowkey_cell_count() const
{
  return rowkey_cell_count_;
}

inline void ObRowDesc::set_rowkey_cell_count(int64_t rowkey_cell_count)
{
  this->rowkey_cell_count_ = rowkey_cell_count;
}

inline bool ObRowDesc::operator ==(const ObRowDesc &other) const
{
  bool is_equal = (cells_desc_count_ == other.cells_desc_count_);
  for (int64_t i = 0; is_equal && i < cells_desc_count_; i++) {
    is_equal = (cells_desc_[i] == other.cells_desc_[i]);
  }
  return is_equal;
}

inline bool ObRowDesc::operator !=(const ObRowDesc &other) const
{
  return !(this->operator ==(other));
}

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_ROW_DESC_ */
