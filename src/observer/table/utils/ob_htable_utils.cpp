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
#include "src/observer/table/ob_table_filter.h"
#include "lib/random/ob_random.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share::schema;

ObHTableCellEntity::ObHTableCellEntity(common::ObNewRow *ob_row)
    : ob_row_(ob_row),
      type_(Type::NORMAL)
{}

ObHTableCellEntity::ObHTableCellEntity(common::ObNewRow *ob_row, Type type) 
    : ob_row_(ob_row),
      type_(type)
{}

ObHTableCellEntity::ObHTableCellEntity()
    : ob_row_(NULL),
      type_(Type::NORMAL)
{}

ObString ObHTableCellEntity::get_rowkey() const
{
  ObString rowkey_str;
  if (OB_ISNULL(ob_row_)) {
    LOG_INFO("get_rowkey but ob_row is null", K(ob_row_));
    rowkey_str = NULL;
  } else {
    rowkey_str = ob_row_->get_cell(ObHTableConstants::COL_IDX_K).get_varchar();
  }
  return rowkey_str;
}

int ObHTableCellEntity::deep_copy_ob_row(const common::ObNewRow *ob_row, common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ob_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deep copy param ob_row is null", K(ret));
  } else {
    int64_t buf_size = ob_row->get_deep_copy_size() + sizeof(ObNewRow);
    char *tmp_row_buf = static_cast<char *>(allocator.alloc(buf_size));
    if (OB_ISNULL(tmp_row_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new row", KR(ret));
    } else {
      int64_t pos = sizeof(ObNewRow);
      ob_row_ = new (tmp_row_buf) ObNewRow();
      if (OB_FAIL(ob_row_->deep_copy(*ob_row, tmp_row_buf, buf_size, pos))) {
        allocator.free(tmp_row_buf);
        LOG_WARN("fail to deep copy ob_row", KR(ret));
      }
    }
  }
  return ret;
}

void ObHTableCellEntity::reset(common::ObArenaAllocator &allocator)
{
  if (OB_NOT_NULL(ob_row_)) {
    allocator.free(ob_row_);
    ob_row_ = NULL;
  }
}

ObString ObHTableCellEntity::get_qualifier() const
{
  ObString qualifier_str;
  if (OB_ISNULL(ob_row_)) {
    LOG_INFO("get_qualifier but ob_row is null", K(ob_row_));
  } else {
    qualifier_str = ob_row_->get_cell(ObHTableConstants::COL_IDX_Q).get_varchar();
  }
  return qualifier_str;
}

int64_t ObHTableCellEntity::get_timestamp() const
{
  int64_t timestamp = 0;
  if (OB_ISNULL(ob_row_)) {
    LOG_INFO("get_timestamp but ob_row is null", K(ob_row_));
  } else {
    timestamp = ob_row_->get_cell(ObHTableConstants::COL_IDX_T).get_int();
  }
  return timestamp;
}

ObString ObHTableCellEntity::get_value() const
{
  ObString value_str;
  if (OB_ISNULL(ob_row_)) {
    LOG_INFO("get_value but ob_row is null", K(ob_row_));
  } else {
    value_str = ob_row_->get_cell(ObHTableConstants::COL_IDX_V).get_varchar();
  }
  return value_str;
}

ObString ObHTableCellEntity::get_family() const
{
  return family_;
}

int ObHTableCellEntity::get_ttl(int64_t &ttl) const
{
  int ret = OB_SUCCESS;
  ttl = INT64_MAX;
  if (OB_ISNULL(ob_row_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("get_ttl but ob_row is null", K(ob_row_));
  } else if (ob_row_->count_ <= ObHTableConstants::COL_IDX_TTL) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("failed to get property TTL", K(ret), K(ob_row_->count_));
  } else if (ob_row_->get_cell(ObHTableConstants::COL_IDX_TTL).is_null()) {
    ttl = INT64_MAX;
  } else {
    ttl = ob_row_->get_cell(ObHTableConstants::COL_IDX_TTL).get_int();
  }
  return ret;
}

void ObHTableCellEntity::set_family(const ObString family)
{
  family_ = family;
}
////////////////////////////////////////////////////////////////
ObString ObHTableCellEntity2::get_rowkey() const
{
  ObString rowkey_str;
  int ret = OB_SUCCESS;
  ObObj val;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::ROWKEY_CNAME_STR, val))) {
    LOG_WARN("failed to get property K", K(ret), K(entity_));
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
    LOG_WARN("failed to get property Q", K(ret), K(entity_));
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
    LOG_WARN("failed to get property T", K(ret), K(entity_));
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

int ObHTableCellEntity2::get_ttl(int64_t &ttl) const
{
  int ret = OB_SUCCESS;
  ttl = INT64_MAX;
  ObObj val;
  if (OB_ISNULL(entity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(entity_->get_property(ObHTableConstants::TTL_CNAME_STR, val))) {
    LOG_WARN("failed to get property TTL", K(ret));
  } else if (val.is_null()) {
    ttl = INT64_MAX;
  } else {
    ttl = val.get_int();
  }
  return ret;
}

int ObHTableCellEntity2::get_value(ObString &str) const
{
  int ret = OB_SUCCESS;
  ObObj val;
  if (OB_FAIL(entity_->get_property(ObHTableConstants::VALUE_CNAME_STR, val))) {
    LOG_WARN("failed to get property V", K(ret));
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
    LOG_WARN("failed to get Q from entity", K(ret), K_(entity));
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

int ObHTableCellEntity3::get_ttl(int64_t &ttl) const
{
  int ret = OB_SUCCESS;
  ttl = INT64_MAX;
  last_get_is_null_ = false;
  ObObj obj;
  if (OB_ISNULL(entity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(entity_->get_property(ObHTableConstants::TTL_CNAME_STR, obj))) {
    LOG_WARN("failed to get property TTL", K(ret));
  } else if (obj.is_null()) {
    last_get_is_null_ = true;
    ttl = INT64_MAX;
  } else {
    ttl = obj.get_int();
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObHTableUtils::create_last_cell_on_row_col(common::ObIAllocator &allocator,
                                               const ObHTableCell &cell, 
                                               ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  ObString qualifier_clone;
  ObObj *last_cell = nullptr;
  ObNewRow *ob_row = nullptr;
  if (OB_FAIL(ob_write_string(allocator, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, cell.get_qualifier(), qualifier_clone))) {
    LOG_WARN("failed to clone qualifier", K(ret));
  } else if (OB_ISNULL(last_cell = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else {
    last_cell[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey_clone);
    last_cell[ObHTableConstants::COL_IDX_Q].set_varbinary(qualifier_clone);
    last_cell[ObHTableConstants::COL_IDX_T].set_max_value();
    if (OB_ISNULL(ob_row = OB_NEWx(ObNewRow, (&allocator), last_cell, 3))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator last NewRow on column.", K(ret));
    } else if (OB_ISNULL(new_cell = OB_NEWx(ObHTableCellEntity, (&allocator), ob_row, ObHTableCell::Type::LAST_ON_COL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator last entity on column.", K(ret));
    }
  }
  return ret;
}

int ObHTableUtils::create_first_cell_on_row_col(common::ObIAllocator &allocator, 
                                                const ObHTableCell &cell,
                                                const common::ObString &qualifier, 
                                                ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  ObString qualifier_clone;
  ObObj *first_cell = nullptr;
  ObNewRow *ob_row = nullptr;
  if (OB_FAIL(ob_write_string(allocator, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, qualifier, qualifier_clone))) {
    LOG_WARN("failed to clone qualifier", K(ret));
  } else if (OB_ISNULL(first_cell = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else {
    first_cell[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey_clone);
    first_cell[ObHTableConstants::COL_IDX_Q].set_varbinary(qualifier_clone);
    first_cell[ObHTableConstants::COL_IDX_T].set_min_value();
    if (OB_ISNULL(ob_row = OB_NEWx(ObNewRow, (&allocator), first_cell, 3))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator first NewRow on column.", K(ret));
    } else if (OB_ISNULL(new_cell = OB_NEWx(ObHTableCellEntity, (&allocator), ob_row, ObHTableCell::Type::FIRST_ON_COL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator first entity on column.", K(ret));
    }
  }
  return ret;
}

int ObHTableUtils::create_last_cell_on_row(common::ObIAllocator &allocator,
                                           const ObHTableCell &cell, 
                                           ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  ObObj *last_cell = nullptr;
  ObNewRow *ob_row = nullptr;
  if (OB_FAIL(ob_write_string(allocator, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else if (OB_ISNULL(last_cell = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else {
    last_cell[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey_clone);
    last_cell[ObHTableConstants::COL_IDX_Q].set_max_value();
    last_cell[ObHTableConstants::COL_IDX_T].set_max_value();
    if (OB_ISNULL(ob_row = OB_NEWx(ObNewRow, (&allocator), last_cell, 3))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator last NewRow on row.", K(ret));
    } else if (OB_ISNULL(new_cell = OB_NEWx(ObHTableCellEntity, (&allocator), ob_row, ObHTableCell::Type::LAST_ON_ROW))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator last entity on row.", K(ret));
    }
  }
  return ret;
}

int ObHTableUtils::create_first_cell_on_row_col_ts(common::ObIAllocator &allocator,
                                                   const ObHTableCell &cell,
                                                   const int64_t timestamp, 
                                                   ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  ObString qualifier_clone;
  ObObj *last_cell = nullptr;
  ObNewRow *ob_row = nullptr;
  if (OB_FAIL(ob_write_string(allocator, cell.get_rowkey(), rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, cell.get_qualifier(), qualifier_clone))) {
    LOG_WARN("failed to clone qualifier", K(ret));
  } else if (OB_ISNULL(last_cell = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else {
    last_cell[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey_clone);
    last_cell[ObHTableConstants::COL_IDX_Q].set_varbinary(qualifier_clone);
    last_cell[ObHTableConstants::COL_IDX_T].set_int(timestamp);
    if (OB_ISNULL(ob_row = OB_NEWx(ObNewRow, (&allocator), last_cell, 3))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator last NewRow on column timestamp.", K(ret));
    } else if (OB_ISNULL(new_cell = OB_NEWx(ObHTableCellEntity, (&allocator), ob_row, ObHTableCell::Type::NORMAL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator first entity on column timestamp.", K(ret));
    }
  }
  return ret;
}

int ObHTableUtils::create_first_cell_on_row(common::ObIAllocator &allocator,
                                            const ObHTableCell &cell, 
                                            ObHTableCell *&new_cell)
{
  return create_first_cell_on_row(allocator, cell.get_rowkey(), new_cell);
}

int ObHTableUtils::create_first_cell_on_row(common::ObIAllocator &allocator, 
                                            const ObString &row_key, 
                                            ObHTableCell *&new_cell)
{
  int ret = OB_SUCCESS;
  ObString rowkey_clone;
  ObObj *first_cell = nullptr;
  ObNewRow *ob_row = nullptr;
  if (OB_FAIL(ob_write_string(allocator, row_key, rowkey_clone))) {
    LOG_WARN("failed to clone rowkey", K(ret));
  } else if (OB_ISNULL(first_cell = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else {
    first_cell[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey_clone);
    first_cell[ObHTableConstants::COL_IDX_Q].set_min_value();
    first_cell[ObHTableConstants::COL_IDX_T].set_min_value();
    if (OB_ISNULL(ob_row = OB_NEWx(ObNewRow, (&allocator), first_cell, 3))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator last NewRow on row.", K(ret));
    } else if (OB_ISNULL(new_cell = OB_NEWx(ObHTableCellEntity, (&allocator), ob_row, ObHTableCell::Type::FIRST_ON_ROW))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator last entity on row.", K(ret));
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
      cmp_ret = qualifier1.compare(qualifier2);
      if (0 == cmp_ret) {
        // one of the cells could be ObHTableFirstOnRowColCell or ObHTableLastOnRowColCell
        cmp_ret = static_cast<int>(cell1.get_type()) - static_cast<int>(cell2.get_type());
        if (0 == cmp_ret) {
          // compare timestamps in ascending order (the value of timestamp is negative)
          int64_t ts1 = cell1.get_timestamp();
          int64_t ts2 = cell2.get_timestamp();
          if (ts1 == ts2) {
          } else if (ts1 < ts2) {
            cmp_ret = -1;
          } else {
            cmp_ret = 1;
          }
        }
      }
    }
  }
  return cmp_ret;
}

int ObHTableUtils::compare_cell(const ObHTableCell &cell1, const ObHTableCell &cell2, bool is_reversed)
{
  common::ObQueryFlag::ScanOrder scan_order = 
    is_reversed? common::ObQueryFlag::ScanOrder::Reverse : common::ObQueryFlag::ScanOrder::Forward;
  return compare_cell(cell1, cell2, scan_order);
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

int ObHTableUtils::lock_htable_rows(uint64_t table_id, const ObIArray<ObTableOperation> &ops, ObHTableLockHandle &handle, ObHTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  const int64_t N = ops.count();
  if (table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObITableEntity &entity = ops.at(i).entity();
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

int ObHTableUtils::lock_htable_rows(uint64_t table_id, const ObIArray<ObTableSingleOp> &ops, ObHTableLockHandle &handle, ObHTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;

  if (table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); ++i) {
    const ObIArray<ObTableSingleOpEntity> &entities = ops.at(i).get_entities();
    for (int64_t j = 0; OB_SUCC(ret) && j < entities.count(); ++j) {
      const ObITableEntity &entity = entities.at(j);
      ObHTableCellEntity3 htable_cell(&entity);
      ObString row = htable_cell.get_rowkey();
      if (row.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null htable rowkey", K(ret));
      } else if (OB_FAIL(HTABLE_LOCK_MGR->lock_row(table_id, row, lock_mode, handle))) {
        LOG_WARN("fail to lock htable row", K(ret), K(table_id), K(row), K(lock_mode));
      }
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

int ObHTableUtils::lock_redis_key(uint64_t table_id, const ObString &lock_key, ObHTableLockHandle &handle, ObHTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  if (table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret));
  } else if (lock_key.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null redis lock_key", K(ret));
  } else if (OB_FAIL(HTABLE_LOCK_MGR->lock_row(table_id, lock_key, lock_mode, handle))) {
    LOG_WARN("fail to lock redis key", K(ret), K(table_id), K(lock_key), K(lock_mode));
  }
  
  return ret;
}

int ObHTableUtils::get_mode_type(const ObTableSchema &table_schema, ObHbaseModeType &mode_type)
{
  int ret = OB_SUCCESS;
  bool matched = false;
  mode_type = ObHbaseModeType::OB_INVALID_MODE_TYPE;
  const ObColumnSchemaV2 *second_schema = NULL;
  if (OB_ISNULL(second_schema = table_schema.get_column_schema_by_idx(1))) {
    // do nothing
  } else if (ObHTableConstants::CQ_CNAME_STR.case_compare(second_schema->get_column_name()) == 0) {
    if (OB_FAIL(match_hbase_normal_mode(table_schema, matched))) {
      LOG_WARN("fail to match hbase normal mode", K(ret), K(table_schema));
    } else if (matched) {
      mode_type = ObHbaseModeType::OB_HBASE_NORMAL_TYPE;
    }
  } else if (ObHTableConstants::VERSION_CNAME_STR.case_compare(second_schema->get_column_name()) == 0) {
    if (OB_FAIL(match_hbase_series_mode(table_schema, matched))) {
      LOG_WARN("fail to match hbase series mode", K(ret), K(table_schema));
    } else if (matched) {
      mode_type = ObHbaseModeType::OB_HBASE_SERIES_TYPE;
    }
  }
  return ret;
}

int ObHTableUtils::match_hbase_normal_mode(const ObTableSchema &table_schema, bool &matched)
{
  int ret = OB_SUCCESS;
  UNUSED(ret);
  matched = false;
  const ObColumnSchemaV2 *key_column = NULL;
  const ObColumnSchemaV2 *version_column = NULL;
  const ObColumnSchemaV2 *value_column = NULL;
  if (OB_ISNULL(key_column = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_K))) {
    // do nothing
  } else if (ObHTableConstants::ROWKEY_CNAME_STR.case_compare(key_column->get_column_name()) != 0) {
    // do nothing
  } else if (OB_ISNULL(version_column = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_T))) {
    // do nothing
  } else if (ObHTableConstants::VERSION_CNAME_STR.case_compare(version_column->get_column_name()) != 0) {
    // do nothing
  } else if (OB_ISNULL(value_column = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_V))) {
    // do nothing
  } else if (ObHTableConstants::VALUE_CNAME_STR.case_compare(value_column->get_column_name()) != 0) {
    // do nothing
  } else {
    matched = true;
  }
  return ret;
}

int ObHTableUtils::match_hbase_series_mode(const ObTableSchema &table_schema, bool &matched)
{
  int ret = OB_SUCCESS;
  matched = false;
  const ObColumnSchemaV2 *key_column = NULL;
  const ObColumnSchemaV2 *series_column = NULL;
  const ObColumnSchemaV2 *value_column = NULL;
  const ObColumnSchemaV2 *add_schema = NULL;
  if (OB_ISNULL(key_column = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_K))) {
    // do nothing
  } else if (ObHTableConstants::ROWKEY_CNAME_STR.case_compare(key_column->get_column_name()) != 0) {
    // do nothing
  } else if (OB_ISNULL(series_column = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_S))) {
    // do nothing
  } else if (ObHTableConstants::SEQ_CNAME_STR.case_compare(series_column->get_column_name()) != 0) {
    // do nothing
  } else if (OB_ISNULL(value_column = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_V))) {
    // do nothing
  } else if (ObHTableConstants::VALUE_CNAME_STR.case_compare(value_column->get_column_name()) != 0) {
    // do nothing
  } else if (value_column->get_data_type() != ObJsonType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "series table value column type must be json");
    LOG_WARN("series table value column type must be json", K(ret), K(value_column->get_data_type()));
  } else if (OB_NOT_NULL(add_schema = table_schema.get_column_schema_by_idx(ObHTableConstants::COL_IDX_TTL)) &&
                ObHTableConstants::TTL_CNAME_STR.case_compare(add_schema->get_column_name()) == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "series table not support cell ttl");
    LOG_WARN("series table not support cell ttl", K(ret));
  } else {
    matched = true;
  }
  return ret;
}

int ObHTableUtils::get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos, const char *name)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "%s", name);
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
    }
  }

  return ret;
}

int ObHTableUtils::get_hbase_scanner_timeout(uint64_t tenant_id)
{
  int value = HBASE_SCANNER_TIMEOUT_DEFAULT_VALUE;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    value = tenant_config->kv_hbase_client_scanner_timeout_period;
  }
  return value;
}

int ObHTableUtils::generate_hbase_bytes(ObIAllocator& allocator, int32_t len, char*& val)
{
  int ret = OB_SUCCESS;
  val = static_cast<char*>(allocator.alloc(sizeof(len)));
  if (val == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret));
  } else {
    // Hbase use big endian.
    for (int i = 3; i >= 0; i--) {
      val[i] = static_cast<int8_t>(len);
      len >>= sizeof(char) * 8;
    }
  }
  return ret;
}

int ObHTableUtils::get_prefix_key_range(common::ObIAllocator &allocator, ObString prefix, KeyRange *range)
{
  int ret = OB_SUCCESS;
  bool is_max = true;
  ObString prefix_end;
  if (OB_FAIL(ob_write_string(allocator, prefix, prefix_end))) {
    LOG_WARN("failed to clone prefix", K(ret), K(prefix));
  } else if (!prefix_end.empty()) {
    char* ptr = prefix_end.ptr();
    bool loop = true;
    for (int i = prefix_end.length() - 1; i >= 0 && loop; i--) {
      if (ptr[i] != -1) {
        is_max = false;
        ptr[i] += 1;
        loop = false;
        prefix_end.set_length(i + 1);
      }
    }
    if (is_max) {
      prefix_end.reset();
    }
    range->set_max(prefix_end);
    range->set_max_inclusive(false);
  }
  return ret;
}

int ObHTableUtils::merge_key_range(ObKeyRangeTree& tree)
{
  int ret = OB_SUCCESS;
  ObKeyRangeNode* curr_range = tree.get_first();
  ObKeyRangeNode* merge_range = nullptr;
  if (nullptr == curr_range) {
  } else if (!curr_range->get_value()->valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key range", KR(ret), KPC(curr_range));
  } else if (OB_FAIL(tree.get_next(curr_range, merge_range))) {
    LOG_WARN("fail to get next node from tree", KR(ret), KPC(merge_range));
  }

  bool loop = true;
  while (OB_SUCC(ret) && merge_range != nullptr && loop) {
    if (!merge_range->get_value()->valid()) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid row range", KR(ret), KPC(merge_range));
    } else if (curr_range->get_value()->max().empty()) {
      loop = false;
    } else {
      KeyRange* l_val = curr_range->get_value();
      KeyRange* r_val = merge_range->get_value();
      // With overlap in the ranges
      if (l_val->max().compare(r_val->min()) > 0
      || (l_val->max().compare(r_val->min()) == 0 && 
      (l_val->max_inclusive() || r_val->min_inclusive()) )) {
        if (r_val->max().empty()) {
          l_val->set_max(ObString());
          tree.remove(merge_range);
          loop = false;
        } else {
          int cmp = l_val->max().compare(r_val->max());
          if (cmp < 0) {
            l_val->set_max(r_val->max());
            l_val->set_max_inclusive(r_val->max_inclusive());
          } else if (cmp == 0) {
            l_val->set_max_inclusive(l_val->max_inclusive() | r_val->max_inclusive());
          }
          tree.remove(merge_range);
        }
      } else {
        curr_range = merge_range;
      }
  
      if (loop && OB_FAIL(tree.get_next(curr_range, merge_range))) {
        LOG_WARN("fail to get next node from tree", KR(ret), KPC(merge_range));
      }
    }
  }

  loop = (nullptr != curr_range);
  while (OB_SUCC(ret) && loop) {
    if (OB_FAIL(tree.get_next(curr_range, merge_range))) {
      LOG_WARN("fail to get next node from tree", KR(ret), KPC(merge_range));
    } else if (merge_range == NULL) {
      loop = false;
    } else {
      tree.remove(merge_range);
    }
  }

  return ret;
}

int ObHTableUtils::cons_query_by_entity(const ObITableEntity &entity, ObIAllocator &allocator, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObHTableFilter &filter = query.htable_filter();
  if (OB_FAIL(build_range_by_entity(entity, allocator, query))) {
    LOG_WARN("fail to build range from delete entity", K(ret), K(entity));
  } else if (OB_FAIL(gen_filter_by_entity(entity, filter))) {
    LOG_WARN("fail to gen filter from delete entity", K(ret), K(entity));
  } else {}
  return ret;
}

int ObHTableUtils::build_range_by_entity(const ObITableEntity &entity, ObIAllocator &allocator, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObIArray<ObNewRange> &scan_ranges = query.get_scan_ranges();
  scan_ranges.reset();
  ObHTableCellEntity3 htable_cell(&entity);
  ObString row =  htable_cell.get_rowkey();
  ObObj *pk_objs_start = nullptr;
  ObObj *pk_objs_end = nullptr;
  int64_t buf_size = sizeof(ObObj) * ObHTableConstants::HTABLE_ROWKEY_SIZE; 

  if (OB_ISNULL(pk_objs_start = static_cast<ObObj *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(buf_size));
  } else if (OB_ISNULL(pk_objs_end = (ObObj *)allocator.alloc(buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(buf_size));
  } else if (htable_cell.last_get_is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("K is null", K(ret), K(entity));
  } else {
    ObString qualifier = htable_cell.get_qualifier();
    ObNewRange range;
    pk_objs_start[0].set_varbinary(row);
    pk_objs_start[2].set_min_value();
    pk_objs_end[0].set_varbinary(row);
    pk_objs_end[2].set_max_value();
    // delete column family when qualifier is null
    if (htable_cell.last_get_is_null()) {
      pk_objs_start[1].set_min_value();
      pk_objs_end[1].set_max_value();
    } else {
      pk_objs_start[1].set_varbinary(qualifier);
      pk_objs_end[1].set_varbinary(qualifier);
    }
    range.start_key_.assign(pk_objs_start, 3);
    range.end_key_.assign(pk_objs_end, 3);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    
    if (OB_FAIL(scan_ranges.push_back(range))) {
      LOG_WARN("fail to push back hbase delete scan range", K(ret), K(range));
    }
  }

  return ret;
}

int ObHTableUtils::gen_filter_by_entity(const ObITableEntity &entity, ObHTableFilter &filter)
{
  int ret = OB_SUCCESS;
  ObHTableCellEntity3 htable_cell(&entity);
  ObString row =  htable_cell.get_rowkey();
  ObString qualifier;
  filter.set_valid(true);
  filter.clear_columns();
  if (htable_cell.last_get_is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("K is null", K(ret), K(entity));
  } else if (FALSE_IT(qualifier = htable_cell.get_qualifier())) {
    // do nothing
  } else if (htable_cell.last_get_is_null()) {
    // delete column family, so we need to scan all qualifier
    // wildcard scan
  } else if (OB_FAIL(filter.add_column(qualifier))) {
    LOG_WARN("failed to add column", K(ret));
  }
  int64_t timestamp = -htable_cell.get_timestamp();        // negative to get the original value
  if (-ObHTableConstants::LATEST_TIMESTAMP == timestamp) {  // INT64_MAX
    // delete the most recently added cell
    filter.set_max_versions(1);
    filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, ObHTableConstants::INITIAL_MAX_STAMP);
  } else if (timestamp > 0) {
    // delete the specific version
    filter.set_max_versions(1);
    filter.set_timestamp(timestamp);
  } else if (ObHTableConstants::LATEST_TIMESTAMP == timestamp) { // -INT64_MAX
    // delete all version
    filter.set_max_versions(INT32_MAX);
    filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, ObHTableConstants::INITIAL_MAX_STAMP);
  } else {
    // delete all versions less than or equal to the timestamp
    filter.set_max_versions(INT32_MAX);
    filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, (-timestamp) + 1);
  }

  return ret;
}

int ObHTableUtils::construct_entity_from_row(const ObNewRow &row, 
                                             ObKvSchemaCacheGuard &schema_cache_guard, 
                                             ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableColumnInfo *> &column_infos = schema_cache_guard.get_column_info_array();
  if (column_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column infos is empty", K(ret));
  } else if (row.get_count() < ObHTableConstants::HTABLE_ROWKEY_SIZE || row.get_count() > column_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row count is invalid", K(ret), K(row.get_count()), K(column_infos.count()));
  } else {
    uint32_t i = 0;
    for (; OB_SUCC(ret) && i < ObHTableConstants::HTABLE_ROWKEY_SIZE; ++i) {
      if (OB_ISNULL(column_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is null", K(ret), K(i));
      } else if (OB_FAIL(entity.set_rowkey(column_infos.at(i)->column_name_, row.get_cell(i)))) {
        LOG_WARN("fail to set rowkey", K(ret), K(i), K(row.get_cell(i)), KPC(column_infos.at(i)));
      }
    }
    for (; OB_SUCC(ret) && i < row.get_count(); ++i) {
      if (OB_ISNULL(column_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is null", K(ret), K(i));
      } else if (OB_FAIL(entity.set_property(column_infos.at(i)->column_name_, row.get_cell(i)))) {
        LOG_WARN("fail to set property", K(ret), K(i), K(row.get_cell(i)), KPC(column_infos.at(i)));
      }
    }
  }
  return ret;
}

int ObHTableUtils::process_columns(const ObIArray<ObString>& columns,
                                   ObIArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                   ObIArray<ObString>& real_columns)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObString column = columns.at(i);
    ObString real_column = column.after('.');
    ObString family = column.split_on('.');
    if (family.empty()) {
      // do nothing
    } else if (OB_FAIL(real_columns.push_back(real_column))) {
      LOG_WARN("fail to push column name", K(ret));
    } else if (OB_FAIL(family_addfamily_flag_pairs.push_back(std::make_pair(family, real_column.empty())))) {
      LOG_WARN("fail to push family name and addfamily flag", K(ret));
    }
  }
  return ret;
}

bool ObHTableUtils::is_get_all_qualifier(const ObIArray<ObString>& columns)
{
  bool is_get_all = false;
  // we process format of all column string is like: '<family>.<qualifier>' here,
  // and we can make sure it is in batch_get
  for (int i = 0; i < columns.count() && !is_get_all; ++i) {
    ObString column = columns.at(i);
    ObString qualifier = column.after('.');
    if (qualifier.empty()) {
      is_get_all = true;
    }
  }
  return is_get_all;
}

int ObHTableUtils::check_family_existence_with_base_name(const ObString& table_name,
                                                         const ObString& base_tablegroup_name,
                                                         table::ObTableEntityType entity_type,
                                                         const ObIArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                                         std::pair<ObString, bool>& flag,
                                                         bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_name;
  bool is_found = false;
  for (int i = 0; !is_found && i < family_addfamily_flag_pairs.count(); ++i) {
    std::pair<ObString, bool> family_addfamily_flag = family_addfamily_flag_pairs.at(i);
    if (!ObHTableUtils::is_tablegroup_req(base_tablegroup_name, entity_type)) {
      // here is for ls batch hbase get
      // ls batch hbsae get will carry family in its qualifier
      // to determine whether this get is a tablegroup operation or not
      // the base_tablegroup_name will be a real table name like: "test$family1"
      if (OB_FAIL(tmp_name.append(base_tablegroup_name))) {
        LOG_WARN("fail to append", K(ret), K(base_tablegroup_name));
      }
    } else {
      if (OB_FAIL(tmp_name.append(base_tablegroup_name))) {
        LOG_WARN("fail to append", K(ret), K(base_tablegroup_name));
      } else if (OB_FAIL(tmp_name.append("$"))) {
        LOG_WARN("fail to append", K(ret));
      } else if (OB_FAIL(tmp_name.append(family_addfamily_flag.first))) {
        LOG_WARN("fail to append", K(ret), K(family_addfamily_flag.first));
      }
    }
    if (OB_SUCC(ret)) {
      if (table_name.case_compare(tmp_name.string()) == 0) {
        flag = family_addfamily_flag;
        is_found = true;
      }
    }
    tmp_name.reuse();
  }
  if (OB_SUCC(ret)) {
    exist = is_found;
  }
  return ret;
}

int ObHTableUtils::update_query_columns(ObTableQuery& query,
                                        const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                        const ObArray<ObString>& real_columns,
                                        const std::pair<ObString, bool>& family_addfamily_flag)
{
  int ret = OB_SUCCESS;
  query.htable_filter().clear_columns();
  bool is_all_family = family_addfamily_flag.second;
  if (!is_all_family) {
    const ObString &family_name = family_addfamily_flag.first;
    for (int i = 0; OB_SUCC(ret) && i < family_addfamily_flag_pairs.count(); ++i) {
      ObString curr_family = family_addfamily_flag_pairs.at(i).first;
      if (curr_family == family_name) {
        ObString real_column = real_columns.at(i);
        if (OB_FAIL(query.htable_filter().add_column(real_column))) {
          LOG_WARN("fail to add column to htable_filter", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObHTableUtils::init_schema_info(const ObString &arg_table_name,
                                    uint64_t arg_table_id,
                                    ObTableApiCredential &credential,
                                    bool is_tablegroup_req,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const ObSimpleTableSchemaV2 *&simple_table_schmea,
                                    ObKvSchemaCacheGuard &schema_cache_guard)

{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_schema_info(arg_table_name, credential, is_tablegroup_req, schema_guard,
                               simple_table_schmea, schema_cache_guard))) {
    LOG_WARN("fail to init schema info", K(ret), K(arg_table_name), K(credential), K(is_tablegroup_req));
  }
  // TODO: @dazhi 
  // else if (simple_table_schmea->get_table_id() != arg_table_id) {
  //   ret = OB_SCHEMA_ERROR;
  //   LOG_WARN("arg table id is not equal to schema table id", K(ret), K(arg_table_id),
  //           K(simple_table_schmea->get_table_id()));
  // }
  return ret;
}

int ObHTableUtils::init_schema_info(const ObString &arg_table_name,
                                    ObTableApiCredential &credential,
                                    bool is_tablegroup_req,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const ObSimpleTableSchemaV2 *&simple_table_schema,
                                    ObKvSchemaCacheGuard &schema_cache_guard)
{
int ret = OB_SUCCESS;
if (schema_cache_guard.is_inited()) {
  // skip and do nothing
} else if (OB_ISNULL(GCTX.schema_service_)) {
  ret = OB_ERR_UNEXPECTED;
  LOG_WARN("invalid schema service", K(ret));
} else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
  LOG_WARN("fail to get schema guard", K(ret), K(credential.tenant_id_));
/*When is_tablegroup_req is true, simple_table_schema is not properly initialized.  
  Defaulting to use the first element (index 0). */
} else if (is_tablegroup_req &&
           OB_FAIL(init_tablegroup_schema(schema_guard, credential, arg_table_name, simple_table_schema))) { 
  LOG_WARN("fail to get table schema from table group name", K(ret), K(credential.tenant_id_), 
            K(credential.database_id_), K(arg_table_name));
} else if (!is_tablegroup_req
            && OB_FAIL(schema_guard.get_simple_table_schema(credential.tenant_id_,
                                                            credential.database_id_,
                                                            arg_table_name,
                                                            false, /* is_index */
                                                            simple_table_schema))) {
  LOG_WARN("fail to get table schema", K(ret), K(credential.tenant_id_), 
            K(credential.database_id_), K(arg_table_name));
} else if (OB_ISNULL(simple_table_schema) || simple_table_schema->get_table_id() == OB_INVALID_ID) {
  ret = OB_TABLE_NOT_EXIST;
  ObString db("");
  LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, arg_table_name.length(), arg_table_name.ptr(), db.length(), db.ptr());
  LOG_WARN("table not exist", K(ret), K(credential.tenant_id_), K(credential.database_id_), K(arg_table_name));
} else if (simple_table_schema->is_in_recyclebin()) {
  ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
  LOG_USER_ERROR(OB_ERR_OPERATION_ON_RECYCLE_OBJECT);
  LOG_WARN("table is in recycle bin, not allow to do operation", K(ret), K(credential.tenant_id_), 
              K(credential.database_id_), K(arg_table_name));
} else if (OB_FAIL(schema_cache_guard.init(credential.tenant_id_,
                                           simple_table_schema->get_table_id(), 
                                           simple_table_schema->get_schema_version(),
                                           schema_guard))) {
  LOG_WARN("fail to init schema cache guard", K(ret));
}
return ret;
}

/// Get all table schemas based on the tablegroup name
/// Since we only have one table in tablegroup, we could considered tableID as the target table now
int ObHTableUtils::init_tablegroup_schema(share::schema::ObSchemaGetterGuard &schema_guard,
                                          ObTableApiCredential &credential,
                                          const ObString &arg_tablegroup_name,
                                          const ObSimpleTableSchemaV2 *&simple_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = OB_INVALID_ID;
  ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> table_schemas;
  if (OB_FAIL(schema_guard.get_tablegroup_id(credential.tenant_id_, arg_tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", K(ret), K(credential.tenant_id_), 
              K(credential.database_id_), K(arg_tablegroup_name));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(credential.tenant_id_, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schema from table group", K(ret), K(credential.tenant_id_), 
              K(credential.database_id_), K(arg_tablegroup_name), K(tablegroup_id));
  } else {
    if (table_schemas.count() != 1) {
      // TODO: @dazhi
      // ret = OB_NOT_SUPPORTED;
      // LOG_USER_ERROR(OB_NOT_SUPPORTED, "each Table has not one Family currently");
      // LOG_WARN("number of table in table gourp must be equal to one now", K(arg_tablegroup_name), K(table_schemas.count()), K(ret));
      simple_table_schema = table_schemas.at(0);
    } else {
      simple_table_schema = table_schemas.at(0);
    }
  }
  return ret;
}

// Get all table schemas based on the tablegroup name, return ObTableSchema
int ObHTableUtils::init_tablegroup_schema(share::schema::ObSchemaGetterGuard &schema_guard,
                                          ObTableApiCredential &credential,
                                          const ObString &arg_tablegroup_name,
                                          const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = OB_INVALID_ID;
  ObSEArray<const schema::ObTableSchema*, 8> table_schemas;
  if (OB_FAIL(schema_guard.get_tablegroup_id(credential.tenant_id_, arg_tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", K(ret), K(credential.tenant_id_), 
              K(credential.database_id_), K(arg_tablegroup_name));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(credential.tenant_id_, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schema from table group", K(ret), K(credential.tenant_id_), 
              K(credential.database_id_), K(arg_tablegroup_name), K(tablegroup_id));
  } else {
    if (table_schemas.count() != 1) {
      table_schema = table_schemas.at(0);
    } else {
      table_schema = table_schemas.at(0);
    }
  }
  return ret;
}

int ObHTableUtils::init_schema_info(const ObString &arg_table_name,
                                    uint64_t arg_table_id,
                                    ObTableApiCredential &credential,
                                    bool is_tablegroup_req,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const ObTableSchema *&table_schmea,
                                    ObKvSchemaCacheGuard &schema_cache_guard)

{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_schema_info(arg_table_name, credential, is_tablegroup_req, schema_guard,
                               table_schmea, schema_cache_guard))) {
    LOG_WARN("fail to init schema info", K(ret), K(arg_table_name), K(credential), K(is_tablegroup_req));
  }

  return ret;
}

int ObHTableUtils::init_schema_info(const ObString &arg_table_name,
                                    ObTableApiCredential &credential,
                                    bool is_tablegroup_req,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const ObTableSchema *&table_schema,
                                    ObKvSchemaCacheGuard &schema_cache_guard)
{
  int ret = OB_SUCCESS;
  if (schema_cache_guard.is_inited()) {
    // skip and do nothing
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential.tenant_id_));
  /*When is_tablegroup_req is true, table_schema is not properly initialized.  
    Defaulting to use the first element (index 0). */
  } else if (is_tablegroup_req &&
            OB_FAIL(init_tablegroup_schema(schema_guard, credential, arg_table_name, table_schema))) { 
    LOG_WARN("fail to get table schema from table group name", K(ret), K(credential.tenant_id_), 
              K(credential.database_id_), K(arg_table_name));
  } else if (!is_tablegroup_req
              && OB_FAIL(schema_guard.get_table_schema(credential.tenant_id_,
                                                       credential.database_id_,
                                                       arg_table_name,
                                                       false, /* is_index */
                                                       table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(credential.tenant_id_), 
              K(credential.database_id_), K(arg_table_name));
  } else if (OB_ISNULL(table_schema) || table_schema->get_table_id() == OB_INVALID_ID) {
    ret = OB_TABLE_NOT_EXIST;
    ObString db("");
    LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, arg_table_name.length(), arg_table_name.ptr(), db.length(), db.ptr());
    LOG_WARN("table not exist", K(ret), K(credential.tenant_id_), K(credential.database_id_), K(arg_table_name));
  } else if (table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_USER_ERROR(OB_ERR_OPERATION_ON_RECYCLE_OBJECT);
    LOG_WARN("table is in recycle bin, not allow to do operation", K(ret), K(credential.tenant_id_), 
                K(credential.database_id_), K(arg_table_name));
  } else if (OB_FAIL(schema_cache_guard.init(credential.tenant_id_,
                                             table_schema->get_table_id(), 
                                             table_schema->get_schema_version(),
                                             schema_guard))) {
    LOG_WARN("fail to init schema cache guard", K(ret));
  }
  return ret;
}

int ObHTableUtils::adjust_htable_timestamps_for_retry(common::ObIArray<ObTableOperation> &ops)
{
  int ret = OB_SUCCESS;
  // Maximum random offset: 0-999 milliseconds
  // This allows up to 1000 different timestamps within the same millisecond,
  // significantly reducing the probability of row lock conflicts for hot keys
  const int64_t MAX_RANDOM_OFFSET = 999;

  // Iterate through all operations and adjust timestamps for HBase operations
  for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); ++i) {
    ObTableOperation &op = ops.at(i);
    ObITableEntity *entity = nullptr;
    
    if (OB_FAIL(op.get_entity(entity))) {
      LOG_WARN("fail to get entity", K(ret), K(i));
    } else if (OB_ISNULL(entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity is null", K(ret), K(i));
    } else if (!entity->is_user_specific_T()) {
      // Only adjust timestamps for operations where user didn't specify timestamp T
      // When is_user_specific_T() returns false, it means the timestamp was auto-generated
      // by the server (LATEST_TIMESTAMP), which may cause conflicts for hot keys.
      // We adjust these timestamps to avoid subsequent lock conflicts.
      
      ObRowkey rowkey = entity->get_rowkey();
      ObObj *obj_ptr = const_cast<ObRowkey &>(rowkey).get_obj_ptr();
      if (OB_ISNULL(obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj_ptr is nullptr", K(ret), K(rowkey));
      } else {
        ObObj &t_obj = obj_ptr[ObHTableConstants::COL_IDX_T];
        int64_t current_ts = t_obj.get_int();
        
        // Decrease the timestamp by a random offset to avoid lock conflicts (current_ts is negative here)
        // We believe that the conflicting version should be the old version rather than the new one
        int64_t random_offset = ObRandom::rand(0, MAX_RANDOM_OFFSET);
        int64_t new_ts = current_ts + random_offset;
        t_obj.set_int(new_ts);
        
        LOG_DEBUG("adjust htable timestamp for retry", K(i), K(current_ts), K(random_offset), K(new_ts));
      }
    }
    // Skip operations where user explicitly specified timestamp T (is_user_specific_T() == true)
    // These timestamps should not be modified as they are user-defined
  }
  
  return ret;
}
