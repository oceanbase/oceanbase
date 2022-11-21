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
#include "sql/ob_sql_define.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace sql
{
// ObSortColumn没有设置OB_UNIS_VERSION，不能走序列化框架的逻辑，也就没有办法新加类型
// 定义一个ObSortColumnExtra是为了解决ObSortColumn的扩展问题
// is_ascending_这个成员在序列化的时候是按照1个字节的大小来序列化的，
// 但是bool值只需要用到1位，那么剩余7位可以用一位来作为一个版本控制，
// 如果解析时发现设置了版本号，那么就继续反序列化后面的ObSortColumnExtra
struct ObSortColumnExtra
{
  OB_UNIS_VERSION(1);
public:
  static const uint8_t SORT_COL_EXTRA_MASK = 0x7F;
  static const uint8_t SORT_COL_EXTRA_BIT = 0x80;
  static const uint8_t SORT_COL_ASC_MASK = 0xFE;
  static const uint8_t SORT_COL_ASC_BIT = 0x01;
  ObSortColumnExtra()
    : obj_type_(common::ObMaxType), order_type_(default_asc_direction()) {}
  ObSortColumnExtra(common::ObObjType obj_type, ObOrderDirection order_type)
    : obj_type_(obj_type), order_type_(order_type) {}

  TO_STRING_KV(K_(obj_type), K_(order_type));
  common::ObObjType obj_type_;
  ObOrderDirection order_type_;
};

class ObSortColumn : public common::ObColumnInfo, public ObSortColumnExtra
{
public:
  // +--------------------------------+----+---------+
  // |      7      | 6 | 5 | 4 | 3 | 2 | 1 |     0   |
  // +-------------------------------------+---------+
  // | version bit |                       | asc bit |
  // +-------------+-----------------------+---------+
  uint8_t extra_info_;
  ObSortColumn()
    : ObColumnInfo(),
      ObSortColumnExtra(),
      extra_info_(0)
  {
    extra_info_ |= SORT_COL_EXTRA_BIT;
  }
  ObSortColumn(int64_t index,
               common::ObCollationType cs_type,
               bool is_asc)
    : ObSortColumnExtra()
  {
    index_ = index;
    cs_type_ = cs_type;
    if (is_asc) {
      extra_info_ |= SORT_COL_ASC_BIT;
    } else {
      extra_info_ &= SORT_COL_ASC_MASK;
    }
    extra_info_ |= SORT_COL_EXTRA_BIT;
  }
  ObSortColumn(int64_t index,
               common::ObCollationType cs_type,
               bool is_asc,
               common::ObObjType obj_type,
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
    extra_info_ |= SORT_COL_EXTRA_BIT;
  }
  bool is_ascending() const {
    return (extra_info_ & SORT_COL_ASC_BIT) > 0;
  }
  void set_is_ascending(bool is_ascending) {
    extra_info_ &= SORT_COL_ASC_MASK;
    if (is_ascending) {
      extra_info_ |= SORT_COL_ASC_BIT;
    }
  }
  inline bool is_null_first() const {
    return NULLS_FIRST_ASC == order_type_
           || NULLS_FIRST_DESC == order_type_;
  }
  inline ObOrderDirection get_order_type() const {
    return order_type_;
  }
  inline common::ObObjType get_obj_type() const {
    return obj_type_;
  }
  inline common::ObCmpNullPos get_cmp_null_pos() const {
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
  TO_STRING_KV3(N_INDEX_ID, index_,
                N_COLLATION_TYPE, common::ObCharset::collation_name(cs_type_),
                N_ASCENDING, is_ascending() ? N_ASC : N_DESC,
                "ObjType", obj_type_,
                "OrderType", order_type_);
  NEED_SERIALIZE_AND_DESERIALIZE;
};

} // namespace sql
} // namespace oceanbase

#endif /* UNITTEST_SQL_ENGINE_SORT_OB_BASE_SORT_H_ */
