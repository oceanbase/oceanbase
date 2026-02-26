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

#ifndef _OB_OCEANBASE_SCHEMA_NEW_ROW_H
#define _OB_OCEANBASE_SCHEMA_NEW_ROW_H


namespace oceanbase
{
namespace common
{
class ObNewRow;
}
namespace share
{
namespace schema
{
class ObNewRowKey
{
public:
 ObNewRowKey(const common::ObNewRow &row) : row_(row)
 {
  hash_val_ = 0;
  for (int64_t i = 0; i < row.get_count(); i++) {
    const ObObj &obj = row.get_cell(i);
    uint64_t tmp_hash_val = 0;
    if (obj.is_string_type()) {
      hash_val_ = common::murmurhash(obj.get_string_ptr(),
                                     obj.get_string_len(), hash_val_);
    } else {
      uint64_t tmp_hash_val = obj.hash(hash_val_);
      hash_val_ = common::murmurhash(&tmp_hash_val, sizeof(uint64_t), hash_val_);
    }
  }
 }
  ~ObNewRowKey() {};
  uint64_t hash() const { return hash_val_; }
  const common::ObNewRow& get_row() const { return row_; }
  bool operator==(const ObNewRowKey &other) const { return row_ == other.row_ ? true : false; }
private:
  const common::ObNewRow &row_;
  uint64_t hash_val_;
};

class ObNewRowValue
{
public:
  ObNewRowValue(const common::ObNewRow &row, const int64_t part_idx)
    : key_(row), part_idx_(part_idx) {}
  ~ObNewRowValue() {};
  const ObNewRowKey& get_key() const { return key_; }
  int64_t get_part_idx() const { return part_idx_; }
private:
  ObNewRowKey key_;
  //part_idx_ means level one part's position in row value
  int64_t part_idx_;
};

template<class K, class V>
struct ObGetNewRowKey
{
  void operator()(const K &k, const V &v) const
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template<>
struct ObGetNewRowKey<ObNewRowKey, const ObNewRowValue*>
{
  ObNewRowKey operator()(const ObNewRowValue *value) const
  {
    ObNewRow row;
    return OB_NOT_NULL(value) ?  value->get_key() : ObNewRowKey(row);
  }
};
typedef common::hash::ObPointerHashArray<ObNewRowKey, const ObNewRowValue*, ObGetNewRowKey> ListIdxHashArray;
} //namespace share
} //namespace schema
} //namespace oceanbase

#endif /*_OB_OCEANBASE_SCHEMA_NEW_ROW_H*/