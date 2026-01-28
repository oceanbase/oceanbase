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

#include "ob_index_sstable_estimator.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/ob_trans_version_skip_index_util.h"
#include "storage/access/ob_index_block_tree_traverser.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
using namespace blocksstable;
ObPartitionEst::ObPartitionEst()
    : logical_row_count_(0),
      physical_row_count_(0)
{
}

int ObPartitionEst::add(const ObPartitionEst &pe)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ += pe.logical_row_count_;
  physical_row_count_ += pe.physical_row_count_;

  return ret;
}


int ObPartitionEst::deep_copy(const ObPartitionEst &src)
{
  int ret = common::OB_SUCCESS;

  logical_row_count_ = src.logical_row_count_;
  physical_row_count_ = src.physical_row_count_;

  return ret;
}

ObIndexBlockScanEstimator::ObIndexBlockScanEstimator(const ObIndexSSTableEstimateContext &context)
  : context_(context)
{
  tenant_id_ = MTL_ID();
}

int ObIndexBlockScanEstimator::estimate_row_count(ObSSTable &sstable,
    const blocksstable::ObDatumRange &datum_range,
    ObPartitionEst &part_est)
{
  int ret = OB_SUCCESS;
  ObEstimatedResult result;
  if (OB_FAIL(cal_total_estimate_result(sstable, datum_range, result))) {
    STORAGE_LOG(WARN, "Failed to get total estimate result", K(ret));
  } else {
    part_est.physical_row_count_ = result.total_row_count_;
    if (sstable.is_multi_version_minor_sstable()) {
      part_est.logical_row_count_ = result.total_row_count_delta_;
    } else {
      part_est.logical_row_count_ = part_est.physical_row_count_;
    }
  }
  STORAGE_LOG(DEBUG, "estimate row count result", K(ret), K(result), K(part_est));
  return ret;
}

int ObIndexBlockScanEstimator::estimate_block_count(ObSSTable &sstable,
                                                    const blocksstable::ObDatumRange &datum_range,
                                                    int64_t &macro_block_cnt,
                                                    int64_t &micro_block_cnt)
{
  int ret = OB_SUCCESS;
  ObEstimatedResult result(true /* for block */);
  if (OB_FAIL(cal_total_estimate_result(sstable, datum_range, result))) {
    STORAGE_LOG(WARN, "Failed to get total estimate result", K(ret));
  } else {
    macro_block_cnt = MAX(result.macro_block_cnt_, 1);
    micro_block_cnt = MAX(result.micro_block_cnt_, 1);
    STORAGE_LOG(TRACE, "estimate block count result", K(ret), K(result), K(macro_block_cnt), K(micro_block_cnt));
  }
  return ret;
}

class EstimateRowContext : public ObIIndexTreeTraverserContext
{
public:
  static constexpr int64_t RANGE_ROWS_IN_AND_BORDER_RATIO_THRESHOLD = 1000;

  EstimateRowContext(ObSSTable &sstable,
                     const ObDatumRange &range,
                     const int64_t base_version,
                     const int64_t schema_rowkey_count,
                     ObEstimatedResult &result)
      : ObIIndexTreeTraverserContext(true /* is_never_estimate */,
                                     sstable.is_multi_version_minor_sstable() /* consider_multi_version */),
        sstable_(sstable), range_(range), base_version_(base_version),
        schema_rowkey_count_(schema_rowkey_count), result_(result), upper_trans_version_(INT64_MAX),
        is_ended_(false)
  {
  }

  ~EstimateRowContext() override = default;

  int init_trans_version_if_inc_major()
  {
    int ret = OB_SUCCESS;

    if (sstable_.is_inc_major_type_sstable()) {
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(sstable_.get_meta(meta_handle))) {
        STORAGE_LOG(WARN, "Failed to get sstable meta", K(ret), K(sstable_));
      } else {
        const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
        upper_trans_version_ = basic_meta.get_upper_trans_version();
      }
    }

    return ret;
  }

  bool is_valid() const override { return true; }

  bool is_ended() const override { return is_ended_; }

  const ObDatumRange &get_curr_range() const override { return range_; }

  int next_range() override
  {
    is_ended_ = true;
    return OB_SUCCESS;
  }

  int on_inner_node(const ObMicroIndexInfo &index_row,
                    const bool is_coverd_by_range,
                    ObTraverserOperationType &operation)
  {
    int ret = OB_SUCCESS;

    bool is_filtered = false;
    if (OB_FAIL(is_filter_by_base_version(index_row, is_filtered))) {
      STORAGE_LOG(WARN, "Failed to check if filter by base version", K(ret));
    } else if (is_filtered) {
      operation = NOTHING;
    } else if (is_coverd_by_range) {
      result_.total_row_count_ += index_row.get_row_count();
      result_.total_row_count_delta_ += index_row.get_row_count_delta();
      result_.macro_block_cnt_ += index_row.get_macro_block_count();
      result_.micro_block_cnt_ += index_row.get_micro_block_count();
      operation = NOTHING;
    } else if (sstable_.is_major_sstable() && index_row.get_row_count() > 0
               && result_.total_row_count_ / index_row.get_row_count() >= RANGE_ROWS_IN_AND_BORDER_RATIO_THRESHOLD) {
      operation = NOTHING;
    } else if (result_.only_block_ && index_row.is_leaf_block()) {
      result_.total_row_count_ += index_row.get_row_count() / 2;
      result_.total_row_count_delta_ += index_row.get_row_count_delta() / 2;
      result_.micro_block_cnt_ += 1;
      operation = NOTHING;
    } else {
      operation = GOTO_NEXT_LEVEL;
    }

    return ret;
  }

  int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range)
  {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected call to on_node_estimate", K(index_row), K(is_coverd_by_range));
    return ret;
  }

  int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est)
  {
    result_.total_row_count_ += est.physical_row_count_;
    result_.total_row_count_delta_ += est.logical_row_count_;
    result_.micro_block_cnt_ += 1;
    return OB_SUCCESS;
  }

  int is_filter_by_base_version(const ObMicroIndexInfo &index_row, bool &is_filtered)
  {
    int ret = OB_SUCCESS;

    is_filtered = false;

    if (base_version_ <= 0) {
      // no base version, don't filter
    } else if (sstable_.is_major_sstable()) {
      // major sstable will never be filtered by base version
    } else if (sstable_.is_inc_major_type_sstable()) {
      // upper_trans_version does not correspond to all trans_version of rows in the sstable, but we cannot calculate it more precisely
      if (upper_trans_version_ <= base_version_) {
        is_filtered = true;
      }
    } else {
      ObTransVersionSkipIndexReader reader;
      ObTransVersionSkipIndexInfo info;
      if (OB_FAIL(reader.read_min_max_snapshot(index_row, schema_rowkey_count_, info))) {
        STORAGE_LOG(WARN, "Failed to read min max snapshot", K(ret));
      } else if (info.is_inited() && info.max_snapshot_ <= base_version_) {
        is_filtered = true;
      }
    }

    return ret;
  }

  ObSSTable &sstable_;
  const ObDatumRange &range_;
  int64_t base_version_;
  int64_t schema_rowkey_count_;
  ObEstimatedResult &result_;
  int64_t upper_trans_version_;
  bool is_ended_;

  VIRTUAL_TO_STRING_KV(K_(range),
                       K_(base_version),
                       K_(schema_rowkey_count),
                       K_(result),
                       K_(upper_trans_version),
                       K_(is_ended));
};

int ObIndexBlockScanEstimator::cal_total_estimate_result(
    ObSSTable &sstable,
    const blocksstable::ObDatumRange &datum_range,
    ObEstimatedResult &result)
{
  int ret = OB_SUCCESS;

  // TODO(menglan.sm): estimate row interface can reuse partition range spliter class.
  //                   we can estimate all range in once.
  ObIndexBlockTreeTraverser traverser;
  EstimateRowContext context(sstable, datum_range, context_.base_version_, context_.index_read_info_.get_schema_rowkey_count(), result);

  if (OB_FAIL(context.init_trans_version_if_inc_major())) {
    STORAGE_LOG(WARN, "Failed to init trans version if inc major", K(ret));
  } else if (OB_FAIL(traverser.init(sstable, context_.index_read_info_, context_.tablet_handle_.get_obj(), "OB_STORAGE_EST"))) {
    STORAGE_LOG(WARN, "Failed to init index block tree traverser", K(ret), K(context), K(traverser));
  } else if (OB_FAIL(traverser.traverse(context))) {
    STORAGE_LOG(WARN, "Failed to traverse index block tree", K(ret), K(context));
  }

  return ret;
}

}
}
