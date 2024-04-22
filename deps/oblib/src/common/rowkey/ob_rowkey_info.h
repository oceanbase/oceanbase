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

#ifndef OCEANBASE_ROWKEY_INFO_H_
#define OCEANBASE_ROWKEY_INFO_H_

#include "lib/ob_define.h"
#include "common/object/ob_object.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_bit_set.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
namespace oceanbase
{
namespace common
{

typedef ObObjType ColumnType;
typedef ObObjTypeClass ColumnTypeClass;
struct ObRowkeyColumn
{
  ObRowkeyColumn() { memset(this, 0, sizeof(ObRowkeyColumn)); }
  bool is_valid() const
  {
    return (ObURowIDType == type_.get_type() || length_ >= 0) && common::OB_INVALID_ID != column_id_
           && common::ob_is_valid_obj_type(static_cast<ObObjType>(type_.get_type()));
  }
  bool is_equal_except_column_id(const ObRowkeyColumn &other) const;
  ObRowkeyColumn &operator=(const ObRowkeyColumn &other);
  bool operator==(const ObRowkeyColumn &other) const;
  const ObObjMeta get_meta_type() const { return type_; }
  TO_STRING_KV(K_(length), K_(column_id), K_(type), K_(order), K_(fulltext_flag), K(spatial_flag_));

  int64_t length_;
  uint64_t column_id_;
  ObObjMeta type_;
  ObOrderType order_;
  bool fulltext_flag_;
  bool spatial_flag_;
  bool multivalue_flag_;
  NEED_SERIALIZE_AND_DESERIALIZE;
};

class ObRowkeyInfo
{
public:
  ObRowkeyInfo();
  explicit ObRowkeyInfo(ObIAllocator *allocator);
  ~ObRowkeyInfo();

  //ObRowkeyInfo& operator =(const ObRowkeyInfo &rowkey);

  inline int64_t get_size() const { return size_; }


  /**
   * Get rowkey column by index
   * @param[in]  index   column index in RowkeyInfo
   * @param[out] column
   *
   * @return int  return OB_SUCCESS if get the column, otherwist return OB_ERROR
   */
  int get_column(const int64_t index, ObRowkeyColumn &column) const;
  const ObRowkeyColumn *get_column(const int64_t index) const;

  /**
   * Get rowkey column id by index
   * @param[in]  index   column index in RowkeyInfo
   * @param[out] column_id in ObRowkeyInfo
   *
   * @return int  return OB_SUCCESS if get the column, otherwist return OB_ERROR
   */
  int get_column_id(const int64_t index, uint64_t &column_id) const;

  /**
   * Add column to rowkey info
   * @param column column to add
   * @return itn  return OB_SUCCESS if add success, otherwise return OB_ERROR
   */
  int add_column(const ObRowkeyColumn &column);

  int get_index(const uint64_t column_id, int64_t &index, ObRowkeyColumn &column) const;
  int get_index(const uint64_t column_id, int64_t &index) const;
  int is_rowkey_column(const uint64_t column_id, bool &is_rowkey) const;
  int get_spatial_cellid_col_id(uint64_t &column_id) const;
  int get_spatial_mbr_col_id(uint64_t &column_id) const;
  int set_column(const int64_t idx, const ObRowkeyColumn &column);
  void reset();
  bool contain_timestamp_ltz_column() const;

  int reserve(const int64_t capacity);
  bool is_valid() const;
  int64_t get_convert_size() const;
  TO_STRING_KV("columns", common::ObArrayWrap<ObRowkeyColumn>(columns_, size_), K_(capacity));
  int get_column_ids(ObIArray<uint64_t> &column_ids) const;
  int get_column_ids(ObBitSet<> &column_ids) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  int expand(const int64_t size);
  int get_fulltext_col_id_by_type(const ObObjType type, uint64_t &column_id) const;
  int get_spatial_col_id_by_type(uint64_t &column_id, ObObjType type) const;
  static const int64_t DEFAULT_ROWKEY_COLUMN_ARRAY_CAPACITY = 8;
  ObRowkeyColumn *columns_;
  int64_t size_;
  int64_t capacity_;
  ObArenaAllocator arena_;
  ObIAllocator *allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObRowkeyInfo);
};

//used for index info
typedef ObRowkeyColumn ObIndexColumn;
typedef ObRowkeyInfo ObIndexInfo;
//used for partition expr
typedef ObRowkeyColumn ObPartitionKeyColumn;
typedef ObRowkeyInfo ObPartitionKeyInfo;

}
}

#endif //OCEANBASE_ROWKEY_INFO_H_
