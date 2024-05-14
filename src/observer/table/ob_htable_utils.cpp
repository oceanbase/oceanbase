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
#include "ob_htable_filters.h"
#include "ob_htable_filter_operator.h"
#include "share/table/ob_table.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share::schema;

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

int ObHTableCellEntity2::get_value(ObString &str) const
{
  int ret = OB_SUCCESS;
  ObObj val;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::VALUE_CNAME_STR, val))) {
    LOG_WARN("failed to get property K", K(ret));
  } else {
    str = val.get_varchar();
  }
  return ret;
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
    ret = OB_KV_HBASE_INCR_FIELD_IS_NOT_LONG;
    LOG_USER_ERROR(OB_KV_HBASE_INCR_FIELD_IS_NOT_LONG, bytes.length());
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

int ObHTableUtils::lock_htable_rows(uint64_t table_id, const ObTableBatchOperation &mutations, ObHTableLockHandle &handle, ObHTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  const int64_t N = mutations.count();
  if (table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObITableEntity &entity = mutations.at(i).entity();
    ObHTableCellEntity3 htable_cell(&entity);
    ObString row = htable_cell.get_rowkey();
    if (row.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null htable rowkey", K(ret));
    } else if (OB_FAIL(HTABLE_LOCK_MGR->lock_row(table_id, row, lock_mode, handle))) {
      LOG_WARN("fail to lock htable row", K(ret), K(table_id), K(row), K(lock_mode));
    }
  }
  return ret;
}

int ObHTableUtils::lock_htable_row(uint64_t table_id, const ObTableQuery &htable_query, ObHTableLockHandle &handle, ObHTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  if (table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret));
  } else if (!htable_query.get_htable_filter().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query", K(ret));
  } else {
    const ObIArray<common::ObNewRange> &key_ranges = htable_query.get_scan_ranges();
    const ObObj *start_key_ptr = key_ranges.at(0).start_key_.get_obj_ptr();
    const ObObj *end_key_ptr = key_ranges.at(0).end_key_.get_obj_ptr();
    if (OB_ISNULL(start_key_ptr) || OB_ISNULL(end_key_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null pointer", K(ret), KP(start_key_ptr), KP(end_key_ptr), K(key_ranges.at(0)));
    } else {
      ObString start_key = start_key_ptr->get_string();
      ObString end_key = end_key_ptr->get_string();
      if (start_key.empty() || end_key.empty() || start_key != end_key) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument, check operation must only has one row", K(ret), K(start_key), K(end_key));
      } else if (OB_FAIL(HTABLE_LOCK_MGR->lock_row(table_id, start_key, lock_mode, handle))) {
        LOG_WARN("fail to lock htable row", K(ret), K(table_id), K(start_key), K(lock_mode));
      }
    }
  }
  return ret;
}
int ObHTableUtils::check_htable_schema(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *rowkey_schema = NULL;
  const ObColumnSchemaV2 *qualifier_schema = NULL;
  const ObColumnSchemaV2 *version_schema = NULL;
  const ObColumnSchemaV2 *value_schema = NULL;

  if (OB_ISNULL(rowkey_schema = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_K))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can't get rowkey column schema");
  } else if (ObHTableConstants::ROWKEY_CNAME_STR.case_compare(rowkey_schema->get_column_name()) != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the first column should be K", K(ret), K(rowkey_schema->get_column_name()));
  } else if (OB_ISNULL(qualifier_schema = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_Q))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can't get qualifier column schema");
  } else if (ObHTableConstants::CQ_CNAME_STR.case_compare(qualifier_schema->get_column_name()) != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the second column should be Q", K(ret), K(qualifier_schema->get_column_name()));
  } else if (OB_ISNULL(version_schema = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_T))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can't get version column schema");
  } else if (ObHTableConstants::VERSION_CNAME_STR.case_compare(version_schema->get_column_name()) != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the third column should be T", K(ret), K(version_schema->get_column_name()));
  } else if (OB_ISNULL(value_schema = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_V))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can't get version column schema");
  } else if (ObHTableConstants::VALUE_CNAME_STR.case_compare(value_schema->get_column_name()) != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the fourth column should be V", K(ret), K(value_schema->get_column_name()));
  }

  return ret;
}
