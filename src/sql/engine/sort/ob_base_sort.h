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

#ifndef UNITTEST_SQL_ENGINE_SORT_OB_BASE_SORT_H_
#define UNITTEST_SQL_ENGINE_SORT_OB_BASE_SORT_H_

#include "common/row/ob_row_iterator.h"
#include "common/object/ob_obj_type.h"
#include "sql/engine/sort/ob_merge_sort_interface.h"
#include "sql/engine/ob_phy_operator.h"
#include "share/ob_cluster_version.h"

namespace oceanbase {
namespace sql {
// ObSortColumn does not set OB_UNIS_VERSION, and cannot follow the logic of the serialization framework,
// so there is no way to add new types
// Define an ObSortColumnExtra to solve the expansion problem of ObSortColumn
// is_ascending_ This member is serialized according to the size of 1 byte when serializing,
// But the bool value only needs 1 bit, then the remaining 7 bits can be used as a version control,
// If the version number is found to be set during parsing, then continue to deserialize the ObSortColumnExtra behind
struct ObSortColumnExtra {
  OB_UNIS_VERSION(1);

public:
  static const uint8_t SORT_COL_EXTRA_MASK = 0x7F;
  static const uint8_t SORT_COL_EXTRA_BIT = 0x80;
  static const uint8_t SORT_COL_ASC_MASK = 0xFE;
  static const uint8_t SORT_COL_ASC_BIT = 0x01;
  ObSortColumnExtra() : obj_type_(common::ObMaxType), order_type_(default_asc_direction())
  {}
  ObSortColumnExtra(common::ObObjType obj_type, ObOrderDirection order_type)
      : obj_type_(obj_type), order_type_(order_type)
  {}

  TO_STRING_KV(K_(obj_type), K_(order_type));
  common::ObObjType obj_type_;
  ObOrderDirection order_type_;
};

class ObSortColumn : public common::ObColumnInfo, public ObSortColumnExtra {
public:
  // +--------------------------------+----+---------+
  // |      7      | 6 | 5 | 4 | 3 | 2 | 1 |     0   |
  // +-------------------------------------+---------+
  // | version bit |                       | asc bit |
  // +-------------+-----------------------+---------+
  uint8_t extra_info_;
  ObSortColumn() : ObColumnInfo(), ObSortColumnExtra(), extra_info_(0)
  {
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2240) {
      extra_info_ |= SORT_COL_EXTRA_BIT;
    }
  }
  ObSortColumn(int64_t index, common::ObCollationType cs_type, bool is_asc) : ObSortColumnExtra()
  {
    index_ = index;
    cs_type_ = cs_type;
    if (is_asc) {
      extra_info_ |= SORT_COL_ASC_BIT;
    } else {
      extra_info_ &= SORT_COL_ASC_MASK;
    }
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2240) {
      extra_info_ |= SORT_COL_EXTRA_BIT;
    } else {
      extra_info_ &= SORT_COL_EXTRA_MASK;
    }
  }
  ObSortColumn(int64_t index, common::ObCollationType cs_type, bool is_asc, common::ObObjType obj_type,
      ObOrderDirection order_type)
      : ObSortColumnExtra(obj_type, order_type)
  {
    index_ = index;
    cs_type_ = cs_type;
    extra_info_ &= SORT_COL_ASC_MASK;
    if (is_asc) {
      extra_info_ |= SORT_COL_ASC_BIT;
    }
    extra_info_ &= SORT_COL_EXTRA_MASK;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2240) {
      extra_info_ |= SORT_COL_EXTRA_BIT;
    }
  }
  bool is_ascending() const
  {
    return (extra_info_ & SORT_COL_ASC_BIT) > 0;
  }
  void set_is_ascending(bool is_ascending)
  {
    extra_info_ &= SORT_COL_ASC_MASK;
    if (is_ascending) {
      extra_info_ |= SORT_COL_ASC_BIT;
    }
  }
  inline bool is_null_first() const
  {
    return NULLS_FIRST_ASC == order_type_ || NULLS_FIRST_DESC == order_type_;
  }
  inline ObOrderDirection get_order_type() const
  {
    return order_type_;
  }
  inline common::ObObjType get_obj_type() const
  {
    return obj_type_;
  }
  inline common::ObCmpNullPos get_cmp_null_pos() const
  {
    common::ObCmpNullPos ret_pos = common::default_null_pos();
    switch (order_type_) {
      case NULLS_FIRST_ASC: {
        ret_pos = common::NULL_FIRST;
      } break;
      case NULLS_FIRST_DESC: {
        ret_pos = common::NULL_LAST;
      } break;
      case NULLS_LAST_ASC: {
        ret_pos = common::NULL_LAST;
      } break;
      case NULLS_LAST_DESC: {
        ret_pos = common::NULL_FIRST;
      } break;
      default:
        break;
    }
    return ret_pos;
  }
  TO_STRING_KV3(N_INDEX_ID, index_, N_COLLATION_TYPE, common::ObCharset::collation_name(cs_type_), N_ASCENDING,
      is_ascending() ? N_ASC : N_DESC, "ObjType", obj_type_, "OrderType", order_type_);
  NEED_SERIALIZE_AND_DESERIALIZE;
};

class ObSortableTrait {
public:
  ObSortableTrait(common::ObIAllocator& alloc) : sort_columns_(alloc)
  {}
  ~ObSortableTrait()
  {
    sort_columns_.reset();
  }
  int init_sort_columns(int64_t count);
  int add_sort_column(const int64_t index, common::ObCollationType cs_type, bool is_ascending,
      common::ObObjType obj_type, ObOrderDirection order_type);
  const common::ObIArray<ObSortColumn>& get_sort_columns() const
  {
    return sort_columns_;
  }
  int64_t get_sort_column_size() const
  {
    return sort_columns_.count();
  }
  void reset()
  {
    sort_columns_.reset();
  }
  void reuse()
  {
    sort_columns_.reuse();
  }

protected:
  common::ObFixedArray<ObSortColumn, common::ObIAllocator> sort_columns_;  // for merge sort
};

class ObBaseSort : public common::ObOuterRowIterator {
  // whether it is a strongly typed comparison
  struct TypedRowComparer;
  struct TypelessRowComparer;

public:
  struct StrongTypeRow {
    common::ObObj* objs_;
    const common::ObNewRow* row_;

    StrongTypeRow() : objs_(nullptr), row_(nullptr)
    {}

    int init(common::ObIAllocator& alloc, int64_t obj_cnt);

    ~StrongTypeRow()
    {
      reset();
    }

    void reset()
    {
      objs_ = nullptr;
      row_ = nullptr;
    }

    TO_STRING_KV(K_(objs), K_(row));
  };

public:
  explicit ObBaseSort();
  virtual ~ObBaseSort(){};
  virtual void reset();
  virtual void reuse();
  virtual void rescan();
  virtual int set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos);
  virtual int add_row(const common::ObNewRow& row, bool& need_sort);
  int add_row(const common::ObNewRow& row, bool& need_sort, bool deep_copy);
  virtual int sort_rows();
  virtual int get_next_row(common::ObNewRow& row);
  int get_next_row(const common::ObNewRow*& row);
  static int attach_row(const common::ObNewRow& src, common::ObNewRow& dst);
  virtual void set_topn(int64_t topn)
  {
    topn_cnt_ = topn;
  }

  virtual int64_t get_row_count() const
  {
    return sort_array_.count();
  }
  virtual int64_t get_used_mem_size() const
  {
    return cur_alloc_->used();
  }
  virtual int init_tenant_id(uint64_t tenant_id)
  {
    row_alloc0_.set_tenant_id(tenant_id);
    row_alloc1_.set_tenant_id(tenant_id);
    return common::OB_SUCCESS;
  }
  virtual int get_next_compact_row(common::ObString& compact_row)
  {
    UNUSED(compact_row);
    return common::OB_SUCCESS;
  }
  inline int64_t get_topn_cnt()
  {
    return topn_cnt_;
  }
  inline const common::ObIArray<ObSortColumn>* get_sort_columns()
  {
    return sort_columns_;
  }
  inline bool has_topn() const
  {
    return INT64_MAX != topn_cnt_;
  }
  inline bool has_prefix_pos() const
  {
    return prefix_keys_pos_ > 0;
  }
  int check_block_row(const common::ObNewRow& row, const common::ObNewRow* last_row, bool& is_cur_block);
  virtual int dump(ObIMergeSort& merge_sort);
  virtual int final_dump(ObIMergeSort& merge_sort);

  const common::ObNewRow* get_last_row() const;
  TO_STRING_KV(K_(topn_cnt), K_(row_count), K_(prefix_keys_pos), K_(row_array_pos));

  static int enable_typed_sort(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos,
      const common::ObNewRow& row, bool& is_typed_sort);

private:
  int inner_sort_rows(const bool is_typed_sort);

protected:
  int add_typed_row(const common::ObNewRow& row, common::ObIAllocator& alloc);

protected:
  int64_t topn_cnt_;  // for topn
private:
  // current allocator && next block allocator
  common::ObArenaAllocator row_alloc0_;
  common::ObArenaAllocator row_alloc1_;

protected:
  common::ObArenaAllocator* cur_alloc_;         // deep copy current block row
  common::ObArenaAllocator* next_block_alloc_;  // deep copy next block row
  common::ObArray<const StrongTypeRow*> sort_array_;
  // Optimize the sorting column with only one column,
  // put all nulls in one buffer, and put non-nulls in another buffer
  common::ObArray<const StrongTypeRow*> null_array_;

private:
  int64_t row_count_;
  int64_t prefix_keys_pos_;           // prefix columns keys pos
  int64_t row_array_pos_;             // for get next row, cur array pos
  common::ObNewRow* next_block_row_;  // next sort block row
  const common::ObIArray<ObSortColumn>* sort_columns_;
  common::ObSEArray<ObSortColumn, 8> prefix_sort_columns_;
  DISALLOW_COPY_AND_ASSIGN(ObBaseSort);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* UNITTEST_SQL_ENGINE_SORT_OB_BASE_SORT_H_ */
