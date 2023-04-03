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

#define USING_LOG_PREFIX SERVER
#include "ob_htable_utils.h"
#include <endian.h>  // be64toh
using namespace oceanbase::common;
using namespace oceanbase::table;

ObHTableCellEntity::ObHTableCellEntity(common::ObNewRow *ob_row)
    :ob_row_(ob_row)
{}

ObHTableCellEntity::ObHTableCellEntity()
    :ob_row_(NULL)
{}

ObHTableCellEntity::~ObHTableCellEntity()
{}

ObString ObHTableCellEntity::get_rowkey() const
{
  return ob_row_->get_cell(ObHTableConstants::COL_IDX_K).get_varchar();
}

ObString ObHTableCellEntity::get_qualifier() const
{
  return ob_row_->get_cell(ObHTableConstants::COL_IDX_Q).get_varchar();
}

int64_t ObHTableCellEntity::get_timestamp() const
{
  return ob_row_->get_cell(ObHTableConstants::COL_IDX_T).get_int();
}

ObString ObHTableCellEntity::get_value() const
{
  return ob_row_->get_cell(ObHTableConstants::COL_IDX_V).get_varchar();
}
////////////////////////////////////////////////////////////////
ObString ObHTableCellEntity2::get_rowkey() const
{
  ObString rowkey_str;
  int ret = OB_SUCCESS;
  ObObj val;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::ROWKEY_CNAME_STR, val))) {
    LOG_WARN("failed to get property K", K(ret));
  } else {
    rowkey_str = val.get_varchar();
  }
  return rowkey_str;
}

ObString ObHTableCellEntity2::get_qualifier() const
{
  ObString rowkey_str;
  int ret = OB_SUCCESS;
  ObObj val;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::CQ_CNAME_STR, val))) {
    LOG_WARN("failed to get property K", K(ret));
  } else {
    rowkey_str = val.get_varchar();
  }
  return rowkey_str;
}

int64_t ObHTableCellEntity2::get_timestamp() const
{
  int64_t ts = -1;
  int ret = OB_SUCCESS;
  ObObj val;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::VERSION_CNAME_STR, val))) {
    LOG_WARN("failed to get property K", K(ret));
  } else {
    ts = val.get_int();
  }
  return ts;
}

ObString ObHTableCellEntity2::get_value() const
{
  ObString rowkey_str;
  int ret = OB_SUCCESS;
  ObObj val;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::VALUE_CNAME_STR, val))) {
    LOG_WARN("failed to get property K", K(ret));
  } else {
    rowkey_str = val.get_varchar();
  }
  return rowkey_str;
}
////////////////////////////////////////////////////////////////
ObString ObHTableCellEntity3::get_rowkey() const
{
  int ret = OB_SUCCESS;
  last_get_is_null_ = false;
  ObObj obj;
  ObString val;
  if (OB_FAIL(entity_->get_rowkey_value(ObHTableConstants::COL_IDX_K, obj))) {
    LOG_WARN("failed to get K from entity", K(ret), K_(entity));
  } else if (obj.is_null()) {
    last_get_is_null_ = true;
  } else {
    val = obj.get_varchar();
  }
  return val;
}

ObString ObHTableCellEntity3::get_qualifier() const
{
  int ret = OB_SUCCESS;
  last_get_is_null_ = false;
  ObObj obj;
  ObString val;
  if (OB_FAIL(entity_->get_rowkey_value(ObHTableConstants::COL_IDX_Q, obj))) {
    LOG_WARN("failed to get T from entity", K(ret), K_(entity));
  } else if (obj.is_null()) {
    last_get_is_null_ = true;
  } else {
    val = obj.get_varchar();
  }
  return val;
}

int64_t ObHTableCellEntity3::get_timestamp() const
{
  int ret = OB_SUCCESS;
  last_get_is_null_ = false;
  ObObj obj;
  int64_t val = 0;
  if (OB_FAIL(entity_->get_rowkey_value(ObHTableConstants::COL_IDX_T, obj))) {
    LOG_WARN("failed to get T from entity", K(ret), K_(entity));
  } else if (obj.is_null()) {
    last_get_is_null_ = true;
  } else if (OB_FAIL(obj.get_int(val))) {
    LOG_WARN("invalid obj type for T", K(ret), K(obj));
    last_get_is_null_ = true;
  }
  return val;
}

ObString ObHTableCellEntity3::get_value() const
{
  int ret = OB_SUCCESS;
  last_get_is_null_ = false;
  ObObj obj;
  ObString str;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::VALUE_CNAME_STR, obj))) {
    LOG_WARN("failed to get property V", K(ret), K_(entity));
  } else if (obj.is_null()) {
    last_get_is_null_ = true;
  } else if (!obj.is_string_type()) {
    LOG_WARN("invalid obj type", K(ret), K(obj));
    last_get_is_null_ = true;
  } else {
    str = obj.get_varchar();
  }
  return str;
}

////////////////////////////////////////////////////////////////
int ObHTableUtils::create_last_cell_on_row_col(common::ObArenaAllocator &allocator,
                                               const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  ObString qualifier_clone;
  if (OB_FAIL(ob_write_string(allocator, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, cell.get_qualifier(), qualifier_clone))) {
    LOG_WARN("failed to clone qualifier", K(ret));
  } else {
    new_cell = OB_NEWx(ObHTableLastOnRowColCell, (&allocator), rowkey_clone, qualifier_clone);
    if (NULL == new_cell) {
      LOG_WARN("no memory", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}

int ObHTableUtils::create_first_cell_on_row_col(common::ObArenaAllocator &allocator,
                                                const ObHTableCell &cell,
                                                const common::ObString &qualifier,
                                                ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  ObString qualifier_clone;
  if (OB_FAIL(ob_write_string(allocator, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, qualifier, qualifier_clone))) {
    LOG_WARN("failed to clone qualifier", K(ret));
  } else {
    new_cell = OB_NEWx(ObHTableFirstOnRowColCell, (&allocator), rowkey_clone, qualifier_clone);
    if (NULL == new_cell) {
      LOG_WARN("no memory", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}

int ObHTableUtils::create_last_cell_on_row(common::ObArenaAllocator &allocator,
                                           const ObHTableCell &cell, ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  if (OB_FAIL(ob_write_string(allocator, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else {
    new_cell = OB_NEWx(ObHTableLastOnRowCell, (&allocator), rowkey_clone);
    if (NULL == new_cell) {
      LOG_WARN("no memory", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}

int ObHTableUtils::compare_cell(const ObHTableCell &cell1, const ObHTableCell &cell2, common::ObQueryFlag::ScanOrder &scan_order)
{
  // compare rowkey
  int cmp_ret;
  if (common::ObQueryFlag::Reverse == scan_order) {
    cmp_ret = cell2.get_rowkey().compare(cell1.get_rowkey());
  } else {
    cmp_ret = cell1.get_rowkey().compare(cell2.get_rowkey());
  }
  if (0 == cmp_ret) {
    // the same rowkey
    if (ObHTableCell::Type::LAST_ON_ROW == cell1.get_type()) {
      // cell1 is last cell on row
      cmp_ret = 1;
    } else if (ObHTableCell::Type::LAST_ON_ROW == cell2.get_type()) {
      // cell2 is last cell on row
      cmp_ret = -1;
    } else {
      // compare qualifiers
      ObString qualifier1 = cell1.get_qualifier();
      ObString qualifier2 = cell2.get_qualifier();
      if(common::ObQueryFlag::Reverse == scan_order){ 
        cmp_ret = qualifier2.compare(qualifier1);
      } else {
        cmp_ret = qualifier1.compare(qualifier2);
      }
      if (0 == cmp_ret) {
        // compare timestamps in ascending order (the value of timestamp is negative)
        int64_t ts1 = cell1.get_timestamp();
        int64_t ts2 = cell2.get_timestamp();
        if (ts1 == ts2) {
          // one of the cells could be ObHTableFirstOnRowCell or ObHTableFirstOnRowColCell
          if (common::ObQueryFlag::Reverse == scan_order) { 
            cmp_ret = static_cast<int>(cell2.get_type()) - static_cast<int>(cell1.get_type());
          } else {
            cmp_ret = static_cast<int>(cell1.get_type()) - static_cast<int>(cell2.get_type());
          }
        } else if (ts1 < ts2) {
          cmp_ret = -1;
        } else {
          cmp_ret = 1;
        }
      }
    }
  }
  return cmp_ret;
}

int ObHTableUtils::compare_qualifier(const common::ObString &cq1, const common::ObString &cq2)
{
  return cq1.compare(cq2);
}

int ObHTableUtils::compare_rowkey(const common::ObString &rk1, const common::ObString &rk2)
{
  return rk1.compare(rk2);
}

int ObHTableUtils::compare_rowkey(const ObHTableCell &cell1, const ObHTableCell &cell2)
{
  return compare_rowkey(cell1.get_rowkey(), cell2.get_rowkey());
}

int ObHTableUtils::java_bytes_to_int64(const ObString &bytes, int64_t &val)
{
  int ret = OB_SUCCESS;
  if (bytes.length() != sizeof(int64_t)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("length should be 8 bytes", K(ret), "len", bytes.length());
  } else {
    // In Java, data is stored in big-endian format (also called network order).
    const uint64_t *big_endian_64bits = reinterpret_cast<const uint64_t*>(bytes.ptr());
    val = be64toh(*big_endian_64bits);
  }
  return ret;
}

int ObHTableUtils::int64_to_java_bytes(int64_t val, char bytes[8])
{
  uint64_t big_endian_64bits = htobe64(val);
  memcpy(bytes, &big_endian_64bits, sizeof(int64_t));
  return OB_SUCCESS;
}
