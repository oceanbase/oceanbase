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
#include "ob_table_estimator.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet.h"
#include "ob_index_sstable_estimator.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"


namespace oceanbase
{
using namespace blocksstable;
namespace storage
{

int ObTableEstimator::estimate_row_count_for_get(
    ObTableEstimateBaseInput &base_input,
    const ObExtSRowkeyArray &rowkeys,
    ObPartitionEst &part_estimate)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(base_input.is_table_invalid() || rowkeys.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(base_input.table_id_),
                K(rowkeys), K(base_input.tables_.count()));
  } else {
    part_estimate.logical_row_count_ = part_estimate.physical_row_count_ = rowkeys.count();
  }
  return ret;
}

int ObTableEstimator::estimate_row_count_for_scan(
    ObTableEstimateBaseInput &base_input,
    const ObExtSRangeArray &ranges,
    ObPartitionEst &part_estimate,
    ObEstRCRecArray &est_records)
{
  int ret = OB_SUCCESS;
  part_estimate.reset();
  est_records.reuse();
  if (OB_UNLIKELY(base_input.is_table_invalid() || ranges.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(base_input.table_id_),
                K(ranges), K(base_input.tables_.count()));
  } else {
    ObPartitionEst table_est;
    ObEstRowCountRecord record;
    for (int64_t i = 0; OB_SUCC(ret) && i < base_input.tables_.count(); ++i) {
      int64_t start_time = common::ObTimeUtility::current_time();
      table_est.reset();
      ObITable *table = base_input.tables_.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, store shouldn't be null", K(ret), KP(table));
      } else if (OB_FAIL(estimate_multi_scan_row_count(base_input, table, ranges, table_est))) {
        LOG_WARN("failed to estimate cost", K(ret), K(ranges), K(table->get_key()), K(i));
      } else if (OB_FAIL(part_estimate.add(table_est))) {
        LOG_WARN("failed to add table estimation", K(ret), K(i), K(table_est), K(table->get_key()));
      } else {
        record.table_id_ = base_input.table_id_;
        record.table_type_ = table->get_key().table_type_;
        record.logical_row_count_ = table_est.logical_row_count_;
        record.physical_row_count_ = table_est.physical_row_count_;
        if (OB_FAIL(est_records.push_back(record))) {
          LOG_WARN("failed to push back est row count record", K(ret));
        } else {
          LOG_DEBUG("table estimate row count", K(ret), K(table->get_key()), K(table_est),
             "cost time", common::ObTimeUtility::current_time() - start_time);
        }
      }
    }

    part_estimate.logical_row_count_ =
        part_estimate.logical_row_count_ < 0 ? 1 : part_estimate.logical_row_count_;
    part_estimate.physical_row_count_ =
        part_estimate.physical_row_count_ < 0 ? 1 : part_estimate.physical_row_count_;
    if (part_estimate.logical_row_count_ > part_estimate.physical_row_count_) {
      part_estimate.logical_row_count_ = part_estimate.physical_row_count_;
    }
    LOG_DEBUG("final estimate", K(ret), K(part_estimate));
  }
  return ret;
}

int ObTableEstimator::estimate_multi_scan_row_count(
    const ObTableEstimateBaseInput &base_input,
    ObITable *current_table,
    const ObExtSRangeArray &ranges,
    ObPartitionEst &part_est)
{
  int ret = OB_SUCCESS;
  ObPartitionEst tmp_cost;
  const ObITableReadInfo &rowkey_read_info = base_input.tablet_handle_.get_obj()->get_rowkey_read_info();
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    const ObDatumRange &range = ranges.at(i);
    bool is_single_rowkey = false;
    if (OB_FAIL(range.is_single_rowkey(rowkey_read_info.get_datum_utils(), is_single_rowkey))) {
      STORAGE_LOG(WARN, "Failed to check range is single rowkey", K(ret), K(range));
    } else if (is_single_rowkey) {
      // Back off to get mode if range contains single row key.
      tmp_cost.logical_row_count_ = tmp_cost.physical_row_count_ = 1;
    } else if (current_table->is_sstable()) {
      if (OB_FAIL(estimate_sstable_scan_row_count(
          base_input, static_cast<ObSSTable *>(current_table), range, tmp_cost))) {
        LOG_WARN("failed to estimate sstable row count", K(ret), K(*current_table));
      }
    } else if (current_table->is_data_memtable()) {
      if (OB_FAIL(estimate_memtable_scan_row_count(base_input,
          static_cast<const memtable::ObMemtable*>(current_table), range, tmp_cost))) {
        LOG_WARN("failed to estimate memtable row count", K(ret), K(*current_table));
      } else if (tmp_cost.is_invalid_memtable_result()) {
        const static int64_t sub_range_cnt = 3;
        ObSEArray<ObStoreRange, sub_range_cnt> store_ranges;
        if (OB_FAIL((static_cast<memtable::ObMemtable*>(current_table))->get_split_ranges(
            &range.get_start_key().get_store_rowkey(),
            &range.get_end_key().get_store_rowkey(),
            sub_range_cnt,
            store_ranges))) {
          LOG_WARN("Failed to split ranges", K(ret));
        } else if (store_ranges.count() > 1) {
          LOG_TRACE("estimated logical row count may be not right, split range and do estimating again", K(tmp_cost), K(store_ranges));
          tmp_cost.reset();
          common::ObArenaAllocator allocator("OB_STORAGE_EST", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          for (int64_t i = 0; OB_SUCC(ret) && i < store_ranges.count(); ++i) {
            ObPartitionEst sub_cost;
            ObDatumRange datum_range;
            if (OB_FAIL(datum_range.from_range(store_ranges.at(i), allocator))) {
              LOG_WARN("Failed to convert range", K(ret), K(i));
            } else if (OB_FAIL(estimate_memtable_scan_row_count(base_input,
                static_cast<const memtable::ObMemtable*>(current_table), datum_range, sub_cost))) {
              LOG_WARN("failed to estimate memtable row count", K(ret), K(*current_table));
            } else {
              tmp_cost.add(sub_cost);
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table type", K(ret), K(*current_table));
    }
    if (OB_SUCC(ret)) {
      part_est.add(tmp_cost);
    }
  }
  return ret;
}

int ObTableEstimator::estimate_sstable_scan_row_count(
    const ObTableEstimateBaseInput &base_input,
    ObSSTable *sstable,
    const ObDatumRange &key_range,
    ObPartitionEst &part_est)
{
  int ret = OB_SUCCESS;
  part_est.reset();
  if (OB_UNLIKELY(!sstable->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSSStore has not been inited ", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(base_input.table_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid get arguments.", K(ret), K(base_input.table_id_), K(key_range));
  } else {
    const ObIndexSSTableEstimateContext context(
        *sstable, base_input.tablet_handle_, base_input.query_flag_, key_range);
    ObIndexBlockScanEstimator scan_estimator(context);
    if (OB_FAIL(scan_estimator.estimate_row_count(part_est))) {
      LOG_WARN("Fail to estimate cost of scan.", K(ret), K(base_input.table_id_));
    } else {
      LOG_DEBUG("estimate_scan_cost", K(ret), K(base_input.table_id_),
          K(key_range), K(part_est), K(sizeof(scan_estimator)));
    }
  }
  LOG_DEBUG("[STORAGE ESTIMATE ROW]", K(ret), K(base_input.table_id_),
      K(base_input.tx_id_), K(part_est), K(key_range), KPC(sstable));
  return ret;
}

int ObTableEstimator::estimate_memtable_scan_row_count(
    const ObTableEstimateBaseInput &base_input,
    const memtable::ObMemtable *memtable,
    const ObDatumRange &key_range,
    storage::ObPartitionEst &part_est)
{
  int ret = OB_SUCCESS;

  part_est.reset();
  if (!memtable->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN( "memtable not init", K(ret), K(base_input.table_id_));
  } else if (OB_UNLIKELY(!key_range.is_memtable_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range to estimate row", K(ret), K(key_range));
  } else {
    memtable::ObMvccScanRange mvcc_scan_range;
    ObDatumRange real_range;
    memtable->m_get_real_range(real_range, key_range, base_input.query_flag_.is_reverse_scan());
    memtable::ObMemtableKey start_key(&(real_range.get_start_key().get_store_rowkey()));
    memtable::ObMemtableKey end_key(&(real_range.get_end_key().get_store_rowkey()));
    mvcc_scan_range.border_flag_ = real_range.get_border_flag();
    mvcc_scan_range.start_key_ = &start_key;
    mvcc_scan_range.end_key_ = &end_key;
    if (OB_FAIL(memtable->get_mvcc_engine().estimate_scan_row_count(base_input.tx_id_, mvcc_scan_range, part_est))){
      LOG_WARN("Fail to estimate cost of scan.", K(ret), K(base_input.table_id_));
    }
    LOG_DEBUG("[STORAGE ESTIMATE ROW]", K(ret), K(base_input.table_id_), K(base_input.tx_id_),
        K(part_est), K(key_range), KPC(memtable));
  }

  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */
