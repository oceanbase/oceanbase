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

#define USING_LOG_PREFIX STORAGE

#include "ob_column_checksum_calculator.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_column_schema.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/ob_task_define.h"
#include "share/ob_force_print_log.h"

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

  if (is_inited_) {
    column_cnt_ = 0;
    column_checksum_ = NULL;
    is_inited_ = false;
    allocator_.reuse();
  }

  if (OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column_cnt));
  } else if (OB_ISNULL(column_checksum_ = static_cast<int64_t *>(allocator_.alloc(column_cnt * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for column checksum", K(ret), K(column_cnt));
  } else {
    column_cnt_ = column_cnt;
    MEMSET(column_checksum_, 0, column_cnt * sizeof(int64_t));
    is_inited_ = true;
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

ObColumnChecksumAccumulator::ObColumnChecksumAccumulator()
  : is_inited_(false), column_checksum_(NULL), allocator_(ObModIds::OB_SSTABLE_CHECKSUM_CALCULATOR), column_cnt_(0), lock_()
{
}

ObColumnChecksumAccumulator::~ObColumnChecksumAccumulator()
{
}

int ObColumnChecksumAccumulator::init(const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObColumnChecksumAccumulator has been inited twice", K(ret));
  } else if (OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column_cnt));
  } else if (OB_ISNULL(column_checksum_ = static_cast<int64_t *>(allocator_.alloc(column_cnt * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(column_cnt));
  } else {
    column_cnt_ = column_cnt;
    MEMSET(column_checksum_, 0, column_cnt * sizeof(int64_t));
    is_inited_ = true;
  }
  return ret;
}

int ObColumnChecksumAccumulator::add_column_checksum(const int64_t column_cnt,
    const int64_t *column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnChecksumAccumulator has not been inited", K(ret));
  } else if (OB_UNLIKELY(column_cnt != column_cnt_ || NULL == column_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column_cnt), K(column_cnt_), KP(column_checksum));
  } else {
    lib::ObMutexGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      column_checksum_[i] += column_checksum[i];
    }
  }
  return ret;
}

ObSSTableColumnChecksum::ObSSTableColumnChecksum()
  : is_inited_(false), checksums_(), accumulator_(), allocator_(ObModIds::OB_SSTABLE_CHECKSUM_CALCULATOR)
{
}

ObSSTableColumnChecksum::~ObSSTableColumnChecksum()
{
  destroy();
}

//int ObSSTableColumnChecksum::init(const int64_t concurrent_cnt,
//    const share::schema::ObTableSchema &table_schema,
//    const bool need_org_checksum,
//    const ObIArray<const blocksstable::ObSSTableMeta *> &org_sstable_metas)
//{
//  int ret = OB_SUCCESS;
//  if (OB_UNLIKELY(is_inited_)) {
//    ret = OB_INIT_TWICE;
//    LOG_WARN("ObSSTableColumnChecksum has been inited twice", K(ret));
//  } else if (OB_UNLIKELY(concurrent_cnt <= 0 || !table_schema.is_valid()
//        || (need_org_checksum && org_sstable_metas.count() < 1))) {
//    ret = OB_INVALID_ARGUMENT;
//    LOG_WARN("invalid arguments", K(ret), K(concurrent_cnt), K(table_schema),
//        K(need_org_checksum), K(org_sstable_metas.count()));
//  } else if (OB_FAIL(init_checksum_calculators(concurrent_cnt, table_schema))) {
//    LOG_WARN("fail to init checksum calculators", K(ret));
//  } else if (OB_FAIL(init_column_checksum(table_schema, need_org_checksum, org_sstable_metas))) {
//    LOG_WARN("fail to init column checksum", K(ret));
//  } else {
//    is_inited_ = true;
//  }
//  return ret;
//}

int ObSSTableColumnChecksum::init_checksum_calculators(
    const int64_t concurrent_cnt,
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = 0;
  if (OB_UNLIKELY(concurrent_cnt <= 0 || !table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(concurrent_cnt), K(table_schema));
  } else if (OB_FAIL(table_schema.get_store_column_count(column_cnt, false))) {
    LOG_WARN("failed to get store column count", K(ret));
  } else {
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(sizeof(ObColumnChecksumCalculator) * concurrent_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for checksum calculator", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < concurrent_cnt; ++i) {
        ObColumnChecksumCalculator *checksum = new (buf) ObColumnChecksumCalculator();
        if (OB_FAIL(checksum->init(column_cnt))) {
          LOG_WARN("fail to init column checksum calculators", K(ret));
        } else if (OB_FAIL(checksums_.push_back(checksum))) {
          LOG_WARN("fail to push back column checksum calculator", K(ret));
        } else {
          buf += sizeof(ObColumnChecksumCalculator);
        }
        if (OB_FAIL(ret) && NULL != checksum) {
          checksum->~ObColumnChecksumCalculator();
          checksum = NULL;
        }
      }
    }
  }
  return ret;
}

//int ObSSTableColumnChecksum::init_column_checksum(
//    const share::schema::ObTableSchema &table_schema,
//    const bool need_org_checksum,
//    const int64_t checksum_method,
//    const ObIArray<const blocksstable::ObSSTableMeta *> &sstable_metas)
//{
//  int ret = OB_SUCCESS;
//  int64_t *column_checksum = NULL;
//  int64_t column_cnt = 0;
//  if (OB_UNLIKELY(!table_schema.is_valid()
//      || (need_org_checksum && sstable_metas.count() < 1))) {
//    ret = OB_INVALID_ARGUMENT;
//    LOG_WARN("invalid arguments", K(ret), K(table_schema), K(need_org_checksum), K(sstable_metas.count()));
//  } else if (OB_FAIL(table_schema.get_store_column_count(column_cnt, false))) {
//    LOG_WARN("failed to get store column count", K(ret));
//  } else {
//    if (OB_ISNULL(column_checksum = static_cast<int64_t *>(allocator_.alloc(column_cnt * sizeof(int64_t))))) {
//      ret = OB_ALLOCATE_MEMORY_FAILED;
//      LOG_WARN("fail to allocate memory", K(ret));
//    } else {
//      MEMSET(column_checksum, 0, column_cnt * sizeof(int64_t));
//      if (need_org_checksum) {
//        ObSEArray<ObColDesc, OB_DEFAULT_SE_ARRAY_COUNT> columns;
//        if (OB_FAIL(table_schema.get_column_ids(columns))) {
//          LOG_WARN("fail to get column ids", K(ret), K(table_schema));
//        } else {
//          int64_t col_idx = 0;
//          for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
//            const uint64_t col_id = columns.at(i).col_id_;
//            const ObColumnSchemaV2 *col_schema = table_schema.get_column_schema(col_id);
//            if (OB_ISNULL(col_schema)) {
//              ret = OB_ERR_SYS;
//              STORAGE_LOG(ERROR, "column schema must not be NULL", K(ret), K(col_id), K(table_schema));
//            } else if (!col_schema->is_valid()) {
//              ret = OB_ERR_SYS;
//              STORAGE_LOG(ERROR, "invalid col schema", K(ret), K(col_schema));
//              //checksum of virtual columns for index sstable needs to be calculated and stored, but not need for data sstable
//            } else if (!col_schema->is_column_stored_in_sstable()
//                && !table_schema.is_storage_index_table()) {
//              //skip virtual column
//            } else {
//              for (int64_t idx = 0; idx < sstable_metas.count(); idx++) {
//                if (ob_is_large_text(col_schema->get_data_type()) || ob_is_json(col_schema->get_data_type())) {
//                  column_checksum[col_idx] = 0;
//                } else {
//                  bool is_new_column = true;
//                  for (int64_t j = 0; j < sstable_metas.at(idx)->column_metas_.count(); j++) {
//                    if (sstable_metas.at(idx)->column_metas_.at(j).column_id_ == col_id) {
//                      column_checksum[col_idx] += sstable_metas.at(idx)->column_metas_.at(j).column_checksum_;
//                      is_new_column = false;
//                      break;
//                    }
//                  }
//                  if (is_new_column) {
//                    const int64_t default_checksum = CCM_TYPE_AND_VALUE == checksum_method ?
//                      col_schema->get_orig_default_value().checksum(0) : col_schema->get_orig_default_value().checksum_v2(0);
//                    column_checksum[col_idx] += sstable_metas.at(idx)->row_count_ * default_checksum;
//                  }
//
//                }
//              }
//              col_idx++;
//            }
//          }
//        }
//      }
//    }
//
//    if (OB_SUCC(ret)) {
//      if (OB_FAIL(accumulator_.init(column_cnt))) {
//        STORAGE_LOG(WARN, "fail to init checksum accumulator", K(ret));
//      } else if (OB_FAIL(accumulator_.add_column_checksum(column_cnt, column_checksum))) {
//        STORAGE_LOG(WARN, "fail to add column checksum", K(ret));
//      }
//    }
//  }
//  return ret;
//}

int ObSSTableColumnChecksum::get_checksum_calculator(
    const int64_t idx,
    ObColumnChecksumCalculator *&checksum_calc)
{
  int ret = OB_SUCCESS;
  checksum_calc = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableColumnChecksum has not been inited", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= checksums_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(idx), K(checksums_.count()));
  } else {
    checksum_calc = checksums_.at(idx);
  }
  return ret;
}

int ObSSTableColumnChecksum::accumulate_task_checksum()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to init ObSSTableColumnChecksum", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < checksums_.count(); ++i) {
      ObColumnChecksumCalculator *checksum = checksums_.at(i);
      int64_t *column_checksum = NULL;
      if (OB_ISNULL(checksum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, checksum must not be NULL", K(ret));
      } else if (OB_ISNULL(column_checksum = checksum->get_column_checksum())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column schema must not be NULL", K(ret));
      } else if (OB_FAIL(accumulator_.add_column_checksum(checksum->get_column_count(), column_checksum))) {
        LOG_WARN("fail to add column checksum", K(ret));
      }
    }
  }
  return ret;
}

void ObSSTableColumnChecksum::destroy()
{
  is_inited_ = false;
  for (int64_t i = 0; i < checksums_.count(); ++i) {
    ObColumnChecksumCalculator *checksum = checksums_.at(i);
    if (NULL != checksum) {
      checksum->~ObColumnChecksumCalculator();
      checksum = NULL;
    }
  }
  checksums_.reset();
}
