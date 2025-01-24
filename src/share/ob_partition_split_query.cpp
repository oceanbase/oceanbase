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

#define USING_LOG_PREFIX SHARE

#include "ob_partition_split_query.h"
#include "src/storage/ls/ob_ls.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;
using namespace oceanbase::storage;

int ObPartitionSplitQuery::get_tablet_handle(
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  const ObTabletMapKey key(ls_id, tablet_id);
  const int64_t snapshot_version = INT64_MAX; // MAX_TRANS_VERSION
  if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle,
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      ObMDSGetTabletMode::READ_ALL_COMMITED,
      snapshot_version))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("fail to check and get tablet", K(ret), K(key), K(snapshot_version));
    }
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", K(ret), K(tablet_handle));
  }
  return ret;
}

/*
 * Atention!!!
 *  split range is left close right open, like: [1,10)
 * note:
 *  if split start key >= src end key or split end key <= src start key, not need to split
 *  new start key = max (split start key, src start key)
 *  new end key = min (split end key, src end key)
 *  if split start key > src start key, set left bounder closed
 *  if split end key < src end key, set right bounder closed
*/
int ObPartitionSplitQuery::get_tablet_split_range(
    const ObTablet &tablet,
    const blocksstable::ObStorageDatumUtils &datum_utils,
    const storage::ObTabletSplitTscInfo &split_info,
    ObIAllocator &allocator,
    blocksstable::ObDatumRange &src_range,
    bool &is_empty_range)
{
  int ret = OB_SUCCESS;
  int compare_ret = 0;

  if (!src_range.is_valid() || !split_info.is_split_dst_with_partkey()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to split range, invalid argument", K(ret), K(src_range), K(split_info));
  } else {
    is_empty_range = false;
    if (split_info.split_type_ == ObTabletSplitType::RANGE && split_info.partkey_is_rowkey_prefix_) {
      const ObDatumRowkey &split_start_key = split_info.start_partkey_;
      const ObDatumRowkey &split_end_key = split_info.end_partkey_;
      const ObDatumRowkey &src_start_key = src_range.get_start_key();
      const ObDatumRowkey &src_end_key = src_range.get_end_key();
      // ObDatumRowkey
      if (OB_FAIL(split_start_key.compare(split_end_key, datum_utils, compare_ret))) {
        LOG_WARN("fail to split range, compare error.", K(ret), K(split_info));
      } else if (compare_ret >= 0) { // invalid split key range
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to split range, left key should smaller than right key", K(ret), K(split_info));
      } else if (OB_FAIL(src_start_key.compare(src_end_key, datum_utils, compare_ret))) {
        LOG_WARN("fail to split range, compare error.", K(ret), K(src_range));
      } else if (compare_ret > 0 ||
           (compare_ret == 0 && (src_range.is_left_open() || src_range.is_right_open()))) {
        /* src range left/right bounder not all closed but left key equal to right,
         * like (1,1] or [1,1) or (1,1), return empty range  */
        is_empty_range = true;
      } else {
        if (OB_FAIL(split_start_key.compare(src_end_key, datum_utils, compare_ret))) {
          LOG_WARN("fail to split range, compare error.", K(ret), K(split_info));
        } else if (compare_ret >= 0) {
          is_empty_range = true;
          if (compare_ret == 0) {
            // when split start key == src end key, src range is only src end key
            // when compare_ret is 0, split key column cnt must be equal to src key column cnt
            if (src_range.is_right_closed()) {
              src_range.set_start_key(split_start_key);
              src_range.set_end_key(split_start_key);
              src_range.set_left_closed();
              is_empty_range = false;
            }
          }
        } else if (OB_FAIL(split_end_key.compare(src_start_key, datum_utils, compare_ret))) {
          LOG_WARN("fail to split range, compare error.", K(ret), K(split_info));
        } else if (compare_ret <= 0) {
          /* split end key <= src start key, no need change src range,
           * the result should be empty because split range is right bounder open.
           * like: src=[3,5], split=[1,3), result=[] */
          is_empty_range = true;
        } else {
          if (OB_FAIL(split_start_key.compare(src_start_key, datum_utils, compare_ret))) {
            LOG_WARN("fail to compare start key to src start key", K(ret), K(split_start_key), K(src_range));
          } else if (compare_ret > 0) {
            // split start key > src start key, set src start key to split start key
            ObDatumRowkey new_key;
            if (split_start_key.get_datum_cnt() < src_start_key.get_datum_cnt()) {
              if (OB_FAIL(copy_split_key(split_start_key, src_start_key, new_key, allocator))) {
                LOG_WARN("fail to copy split key.", K(ret));
              } else {
                src_range.set_start_key(new_key);
                src_range.set_left_open();
              }
            } else {
              src_range.set_start_key(split_start_key);
              src_range.set_left_closed();
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(split_end_key.compare(src_end_key, datum_utils, compare_ret))) {
              LOG_WARN("fail to compare end key to src start key", K(ret), K(split_end_key), K(src_range));
            } else if (compare_ret <= 0) {
              /* split end key <= src end key, set end key of split end key and right bounder open
               * like: src=[1,5], split=[2,5), result=[2,5) */
              ObDatumRowkey new_key;
              if (split_end_key.get_datum_cnt() < src_end_key.get_datum_cnt()) {
                if (OB_FAIL(copy_split_key(split_end_key, src_end_key, new_key, allocator))) {
                  LOG_WARN("fail to copy split key.", K(ret));
                } else {
                  src_range.set_end_key(new_key);
                }
              } else {
                src_range.set_end_key(split_end_key);
              }
              if (OB_SUCC(ret)) {
                src_range.set_right_open();
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (is_empty_range) {
          src_range.reset(); // set empty range
        } else {
          const ObColDescIArray &col_descs = tablet.get_rowkey_read_info().get_columns_desc();
          if (OB_FAIL(src_range.prepare_memtable_readable(col_descs, allocator))) {
            STORAGE_LOG(WARN, "Fail to transfer store rowkey", K(ret), K(src_range), K(col_descs));
          }
        }
      }
    } else {
      LOG_INFO("No need to split range", K(split_info));
    }
  }
  return ret;
}

int ObPartitionSplitQuery::copy_split_key(
    const ObDatumRowkey &split_key,
    const ObDatumRowkey &src_key,
    ObDatumRowkey &new_key,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  // copy column datum
  ObStorageDatum *datums = nullptr;
  const int src_datum_cnt = src_key.get_datum_cnt();
  if (OB_ISNULL(datums = (ObStorageDatum*) allocator.alloc(sizeof(ObStorageDatum) * src_datum_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "Failed to alloc memory for datum key", K(ret), K(src_datum_cnt));
  } else {
    datums = new (datums) ObStorageDatum[src_datum_cnt];
    for (int64_t i = 0; i < src_datum_cnt; ++ i) {
      if (i < split_key.get_datum_cnt()) {
        datums[i] = split_key.get_datum(i);
      } else {
        datums[i].set_min();
      }
    }
    if (OB_FAIL(new_key.assign(datums, src_datum_cnt))) {
      STORAGE_LOG(WARN, "Failed to assign datum rowkey", K(ret), KP(datums), K(src_datum_cnt));
      new_key.reset();
      allocator.free(datums);
      datums = nullptr;
    }
  }
  return ret;
}

int ObPartitionSplitQuery::get_tablet_split_ranges(
    const common::ObIArray<common::ObStoreRange> &ori_ranges,
    common::ObIArray<common::ObStoreRange> &new_ranges,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (tablet_handle_.is_valid() && split_info_.is_split_dst_with_partkey()) {
    if (ObTabletSplitType::RANGE  == split_info_.split_type_) { // RANGE
      new_ranges.reset();
      ObDatumRange datum_range;
      common::ObStoreRange tmp_range;

      const ObStorageDatumUtils &datum_utils =
        tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils();
      const ObColDescIArray &col_descs =
        tablet_handle_.get_obj()->get_rowkey_read_info().get_columns_desc();
      bool is_empty_range = false;

      for (int64_t i = 0; OB_SUCC(ret) && i < ori_ranges.count(); i++) {
        tmp_range.reset();
        if (OB_FAIL(ori_ranges.at(i).deep_copy(allocator, tmp_range))) {
          LOG_WARN("Fail to deep copy src range", K(ret), K(ori_ranges.at(i)));
        } else if (OB_FAIL(datum_range.from_range(tmp_range, allocator))) {
          LOG_WARN("Failed to transfer store range", K(ret), K(tmp_range));
        } else if (OB_FAIL(get_tablet_split_range(*tablet_handle_.get_obj(), datum_utils, split_info_, allocator, datum_range, is_empty_range))) {
          LOG_WARN("Fail to get tabelt split range", K(ret), K(split_info_));
        } else if (is_empty_range) {
          LOG_INFO("Range after split is empty", K(ori_ranges.count()), K(i), "tablet_id", tablet_handle_.get_obj()->get_tablet_meta().tablet_id_, K(ori_ranges.at(i)));
        } else if (OB_FAIL(datum_range.to_store_range(col_descs, allocator, tmp_range))) {
          LOG_WARN("fail to transfer to store range", K(ret), K(datum_range));
        } else if (OB_FALSE_IT(tmp_range.set_table_id(ori_ranges.at(i).get_table_id()))) {
        } else if (OB_FAIL(new_ranges.push_back(tmp_range))) {
          LOG_WARN("Fail to push back to new ranges", K(ret), K(tmp_range));
        }
      }
    }
  }
  return ret;
}

int ObPartitionSplitQuery::get_split_datum_range(
    const blocksstable::ObStorageDatumUtils *datum_utils,
    ObIAllocator &allocator,
    blocksstable::ObDatumRange &datum_range,
    bool &is_empty_range)
{
  int ret = OB_SUCCESS;
  if (tablet_handle_.is_valid() && split_info_.is_split_dst_with_partkey()) {
    if (OB_ISNULL(datum_utils)) {
      datum_utils = &tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_tablet_split_range(*tablet_handle_.get_obj(), *datum_utils, split_info_, allocator, datum_range, is_empty_range))) {
      STORAGE_LOG(WARN, "Failed to split range", K(ret), K(split_info_));
    }
  }
  return ret;
}

int ObPartitionSplitQuery::get_tablet_split_info(
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (!tablet_id.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(get_tablet_handle(tablet_id, ls_id, tablet_handle_))) {
    LOG_WARN("Fail to get tablet handle", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Tablet handle get obj is null", K(ret), K(tablet_handle_));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::get_split_info(*tablet_handle_.get_obj(), allocator, split_info_))) {
    LOG_WARN("fail to get tablet split info.", K(ret));
  } else if (!split_info_.is_split_dst_with_partkey() && !split_info_.is_split_dst_without_partkey()) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("maybe split dst finished, retry", K(ret), K(tablet_id), K(ls_id));
  }
  return ret;
}

int ObPartitionSplitQuery::set_tablet_handle(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle already has a valid handle", K(ret), K(tablet_handle_));
  } else {
    tablet_handle_ = tablet_handle;
  }
  return ret;
}

int ObPartitionSplitQuery::set_split_info(const ObTabletSplitTscInfo &split_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(split_info_.is_split_dst_with_partkey())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split info is already valid", K(ret), K(split_info_));
  } else {
    split_info_ = split_info;
  }
  return ret;
}

int ObPartitionSplitQuery::split_multi_ranges_if_need(
    const ObIArray<ObStoreRange> &src_ranges,
    ObIArray<ObStoreRange> &new_ranges,
    ObIAllocator &allocator,
    const ObTabletHandle &tablet_handle,
    bool &is_splited_range)
{
  int ret = OB_SUCCESS;
  is_splited_range = false;
  ObTabletSplitTscInfo split_info;
  bool is_tablet_spliting = false;

  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::get_is_spliting(*tablet_handle.get_obj(), is_tablet_spliting))) {
    STORAGE_LOG(WARN, "fail to get tablet split status", K(ret));
  } else if (!is_tablet_spliting) {
    // do nothing
  } else if (OB_FAIL(ObTabletSplitMdsHelper::get_split_info(*tablet_handle.get_obj(), allocator, split_info))) {
    LOG_WARN("fail to get split info", K(ret));
  } else if (split_info.is_split_dst_with_partkey()) {
    /* No matter local index or main table, we have to get current and origin tablet to calculate
      dividing a large range to smaller range in the function. Specifically for local index,
      it's no need to multiple a split partition ratio to estimate size of tablet */
    if (OB_FAIL(set_tablet_handle(tablet_handle))) {
      LOG_WARN("fail to set tablet handle", K(ret));
    } else if (OB_FAIL(set_split_info(split_info))) { // shadow copy
      LOG_WARN("fail to set split info", K(ret));
    } else if (OB_FAIL(get_tablet_split_ranges(src_ranges, new_ranges, allocator))) {
      LOG_INFO("get tablet split new ranges err, maybe no spilitng is happening",
        K(ret), K(src_ranges), K(new_ranges));
    } else {
      is_splited_range = true;
    }
  } else if (!split_info_.is_split_dst_without_partkey()) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("maybe split dst finished, retry", K(ret), K(tablet_handle.get_obj()->get_tablet_meta()));
  }
  return ret;
}

int ObPartitionSplitQuery::check_rowkey_is_included(
    const ObDatumRowkey &target_rowkey,
    const blocksstable::ObStorageDatumUtils *datum_utils,
    bool &is_included)
{
  int ret = OB_SUCCESS;
  if (tablet_handle_.is_valid() && split_info_.is_split_dst_with_partkey()) {
    if (ObTabletSplitType::RANGE  == split_info_.split_type_) { //  range
      if (OB_ISNULL(datum_utils)) {
        datum_utils = &tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils();
      }
      const ObDatumRowkey &split_start_key = split_info_.start_partkey_;
      const ObDatumRowkey &split_end_key = split_info_.end_partkey_;
      int compare_ret = 0;
      if (OB_FAIL(split_start_key.compare(split_end_key, *datum_utils, compare_ret))) {
        LOG_WARN("fail to check rowkey included, compare error.", K(ret), K(split_info_));
      } else if (compare_ret >= 0) { // invalid split key range
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("split left key should smaller than split right key", K(ret), K(split_info_));
      } else if (OB_FAIL(target_rowkey.compare(split_start_key, *datum_utils, compare_ret))) {
        LOG_WARN("fail to check rowkey included, compare error.",
          K(ret), K(target_rowkey), K(split_start_key));
      } else if (compare_ret < 0) { // target rowkey less than split start key
        is_included = false;
        LOG_DEBUG("target rowkey is less than split range start key, not include in split range",
          K(ret), K(target_rowkey), K(split_start_key));
      } else if (OB_FAIL(target_rowkey.compare(split_end_key, *datum_utils, compare_ret))) {
        LOG_WARN("fail to check rowkey included, compare error.",
          K(ret), K(target_rowkey), K(split_end_key));
      } else if (compare_ret >= 0) { // target rowkey large than split end key, Attention: end key should not included in split range.
        is_included = false;
      } else {
        LOG_DEBUG("rowkey is include in split range",
          K(ret), K(target_rowkey), K(split_start_key), K(split_end_key));
      }
    }
  }
  return ret;
}

int ObPartitionSplitQuery::fill_auto_split_params(
    ObTablet &tablet,
    const bool is_split_dst,
    sql::ObPushdownOperator *op,
    const uint64_t filter_type,
    sql::ExprFixedArray *filter_params,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  ObTabletSplitTscInfo split_info;

#ifdef ERRSIM
  DEBUG_SYNC(BEFORE_FILL_AUTO_SPLIT_PARAMS);
#endif

  if (!tablet_id.is_valid() || !ls_id.is_valid()
      || OB_ISNULL(op) || OB_INVALID_ID == filter_type || OB_ISNULL(filter_params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
      K(ret), K(tablet_id), K(ls_id), KP(op), K(filter_type), KP(filter_params));
  } else if (OB_FAIL(filter_params->count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("auto split filter params is zero", K(ret));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::get_split_info(tablet, allocator, split_info))) {
    LOG_WARN("fail to get tablet split info", K(ret));
  } else {
    sql::ObEvalCtx &eval_ctx = op->get_eval_ctx();
    sql::ObExpr *expr = nullptr;
    if (OB_FAIL(ret)) {
    } else if (split_info.is_split_dst_with_partkey()) {
      if (filter_type == static_cast<uint64_t>(ObTabletSplitType::RANGE)) {
        if (OB_FAIL(fill_range_filter_param(split_info, eval_ctx, filter_params))) {
          LOG_WARN("fail to fill range filter param", K(ret));
        }
      } else if (filter_type == static_cast<uint64_t>(ObTabletSplitType::MAX_TYPE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter type is not expected", K(ret), K(filter_type));
      }
    } else if (!split_info.is_split_dst_without_partkey() && is_split_dst) {
      // newly fetched get_split_info's split mds says not split dst, but caller say is_split_dst
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("maybe split dst finished, retry", K(ret), K(tablet_id), K(ls_id));
    } else {
      // only need to fill bypass expr.
      if (OB_ISNULL(expr = filter_params->at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(expr));
      } else {
        ObDatum &expr_datum = expr->locate_datum_for_write(eval_ctx);
        expr_datum.set_int(1);  // all pass
      }
    }
  }
  return ret;
}

/*
 * auto split range filter params order:
 * |-- passby --|-- lower1 --|-- lower2 --|..|-- upper1 --|-- upper2 --|...
 *
 * if tablet is in spliting, fill param expr with actual value.
 * if tablet is not in spliting, only need to fill bypass expr with true.
*/
int ObPartitionSplitQuery::fill_range_filter_param(
    ObTabletSplitTscInfo &split_info,
    sql::ObEvalCtx &eval_ctx,
    sql::ExprFixedArray *filter_params)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRowkey &lower_bound = split_info.start_partkey_;
  blocksstable::ObDatumRowkey &upper_bound = split_info.end_partkey_;

  if (OB_ISNULL(filter_params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(filter_params));
  } else if (lower_bound.get_datum_cnt() != upper_bound.get_datum_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition datum cnt", K(ret), K(lower_bound), K(upper_bound));
  } else {
    const int64_t filter_params_cnt = filter_params->count();
    const int64_t part_column_cnt = lower_bound.get_datum_cnt();
    const int64_t total_filled_cnt = part_column_cnt * 2 + 1;

    if (filter_params_cnt != total_filled_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter param cnt is not equal to need filled cnt",
        K(ret), K(filter_params_cnt), K(total_filled_cnt));
    }
    sql::ObExpr *expr = nullptr;
    const bool need_copy = true;
    for (int i = 0; OB_SUCC(ret) && i < filter_params->count(); ++i) {
      if (OB_ISNULL(expr = filter_params->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(expr));
      }
      LOG_DEBUG("print filter batch result flag", K(expr->batch_result_));
      if (OB_SUCC(ret)) {
        const int col_idx = (i - 1) % part_column_cnt;
        ObDatum &expr_datum = expr->locate_datum_for_write(eval_ctx);
        if (i == 0) { // 1. bypass param expr.
          expr_datum.set_int(0);
        } else if (i <= part_column_cnt) { // 2. lower bound param expr.
          const ObDatum &lower_datum = lower_bound.get_datum(col_idx);
          if (lower_datum.is_min()) {
            expr_datum.set_outrow(); // min
            if (expr_datum.is_outrow()) {
              LOG_DEBUG("set is outrow", K(ret), K(expr_datum));
            }
          } else if (lower_datum.is_max()) {
            expr_datum.set_ext(); // max
            if (expr_datum.is_ext()) {
              LOG_DEBUG("set is ext", K(ret), K(expr_datum));
            }
          } else if (OB_FAIL(expr_datum.from_storage_datum(
              lower_datum,
              expr->obj_datum_map_,
              need_copy))) {
            LOG_WARN("fail to from storage datum", K(ret), K(lower_datum));
          }
        } else if (i <= part_column_cnt * 2) { // 3. upper bound param expr.
          const ObDatum &upper_datum = upper_bound.get_datum(col_idx);
          if (upper_datum.is_max()) {
            expr_datum.set_ext();  // max
            if (expr_datum.is_ext()) {
              LOG_DEBUG("set is ext", K(ret), K(expr_datum));
            }
          } else if (upper_datum.is_min()) {
            expr_datum.set_outrow();  // min
            if (expr_datum.is_outrow()) {
              LOG_DEBUG("set is outrow", K(ret), K(expr_datum));
            }
          } else if (OB_FAIL(expr_datum.from_storage_datum(
              upper_datum,
              expr->obj_datum_map_,
              need_copy))) {
            LOG_WARN("fail to from storage datum", K(ret), K(upper_datum));
          }
        }
      }
    }
  }
  LOG_DEBUG("fill range filter param", K(ret), K(lower_bound), K(upper_bound));
  return ret;
}
