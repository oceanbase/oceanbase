/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_column_checksum_calculator.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::compaction;
using namespace oceanbase::share::schema;

ObColumnChecksumCalculator::ObColumnChecksumCalculator()
  : is_inited_(false), allocator_(ObModIds::OB_SSTABLE_CHECKSUM_CALCULATOR),
    column_checksum_(NULL), column_cnt_(0)
{
}

ObColumnChecksumCalculator::~ObColumnChecksumCalculator()
{
}

int ObColumnChecksumCalculator::init(const int64_t column_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column_cnt));
  } else {
    if (is_inited_) {
      column_cnt_ = 0;
      column_checksum_ = NULL;
      is_inited_ = false;
      allocator_.reuse();
    }
    if (OB_ISNULL(column_checksum_ = static_cast<int64_t *>(allocator_.alloc(column_cnt * sizeof(int64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for column checksum", K(ret), K(column_cnt));
    } else {
      column_cnt_ = column_cnt;
      MEMSET(column_checksum_, 0, column_cnt * sizeof(int64_t));
      is_inited_ = true;
    }
  }
  return ret;
}

int ObColumnChecksumCalculator::calc_column_checksum(
    const ObIArray<share::schema::ObColDesc> &col_descs,
    const blocksstable::ObDatumRow *new_row,
    const blocksstable::ObDatumRow *old_row,
    const bool *is_column_changed)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnChecksumCalculator has not been inited", K(ret));
  } else if (OB_ISNULL(new_row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(new_row), KP(old_row));
  } else if (new_row->row_flag_.is_delete()) {
    // When the row is not in base sstable, it will not be purged
    // NULL means checksums of all columns need to be calculated
    // false means checksum of the row needs to be reduced
    // true means checksum of the row needes to be added
    if (OB_ISNULL(old_row)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "error unexpected, old row must not be NULL", K(ret), KP(old_row));
    } else if (old_row->row_flag_.is_exist_without_delete()) {
      if (OB_FAIL(calc_column_checksum(col_descs, *old_row, false, NULL, column_checksum_))) {
        STORAGE_LOG(WARN, "fail to calculate checksum of old row", K(*old_row), K(ret));
      }
    }
  } else if (new_row->row_flag_.is_exist_without_delete()) {
    // notice that, old row may be nullptr.
    if (nullptr != old_row) {
      if (old_row->row_flag_.is_exist_without_delete()) {
        if (OB_FAIL(calc_column_checksum(col_descs, *old_row, false, is_column_changed, column_checksum_))) {
          STORAGE_LOG(WARN, "fail to calculate checksum of old row", K(*old_row), K(ret));
        } else if (OB_FAIL(calc_column_checksum(col_descs, *new_row, true, is_column_changed, column_checksum_))) {
          STORAGE_LOG(WARN, "fail to calculate checksum of new row", K(*new_row), K(ret));
        }
      } else if (old_row->row_flag_.is_not_exist()) {
        if (OB_FAIL(calc_column_checksum(col_descs, *new_row, true, is_column_changed, column_checksum_))) {
          STORAGE_LOG(WARN, "fail to calculate checksum of new row", K(*new_row), K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected flag of old row ", K(*old_row), K(ret));
      }
    } else if (OB_FAIL(calc_column_checksum(col_descs, *new_row, true, is_column_changed, column_checksum_))) {
      STORAGE_LOG(WARN, "fail to calculate checksum of new row", K(*new_row), K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected flag of new row ", K(*new_row), K(ret));
  }
  return ret;
}

int ObColumnChecksumCalculator::calc_column_checksum(
    const ObIArray<share::schema::ObColDesc> &col_descs,
    const blocksstable::ObDatumRow &row,
    const bool new_row,
    const bool *column_changed,
    int64_t *column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnChecksumCalculator has not been inited", K(ret));
  } else if (NULL == column_checksum || !row.is_valid() || row.count_ != column_cnt_ || row.count_ < col_descs.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row was invalid", K(ret), KP(column_checksum), K(new_row), K_(row.count),
         K_(column_cnt), KP(column_changed), K(row.is_valid()), K(row));
  } else {
    int64_t tmp_checksum = 0;
    for (int64_t i = 0; i < row.count_; ++i) {
      const share::schema::ObColDesc &col_desc = col_descs.at(i);
      if ((NULL != column_changed && !column_changed[i]) || col_desc.col_type_.is_lob_storage()) {
        continue;
      }
      tmp_checksum = row.storage_datums_[i].checksum(0);
      if (new_row) {
        column_checksum[i] += tmp_checksum;
      } else {
        column_checksum[i] -= tmp_checksum;
      }
    }
  }
  return ret;
}

