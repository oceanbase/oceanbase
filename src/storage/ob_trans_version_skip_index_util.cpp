//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/ob_trans_version_skip_index_util.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"
namespace oceanbase
{
using namespace blocksstable;
namespace storage
{

int ObTransVersionSkipIndexInfo::set(
  const ObStorageDatum &min_datum,
  const ObStorageDatum &max_datum)
{
  int ret = OB_SUCCESS;
  if (!min_datum.is_null() && !min_datum.is_nop()) {
    max_snapshot_ = -min_datum.get_int();
  }
  if (!max_datum.is_null() && !max_datum.is_nop()) {
    min_snapshot_ = -max_datum.get_int();
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skip index info is not valid", K(ret), KPC(this));
  }
  return ret;
}

int ObTransVersionSkipIndexInfo::set(
  const ObStorageDatum *min_datum,
  const ObStorageDatum *max_datum)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(min_datum) && !min_datum->is_null() && !min_datum->is_nop()) {
    max_snapshot_ = -min_datum->get_int();
  }
  if (OB_NOT_NULL(max_datum) && !max_datum->is_null() && !max_datum->is_nop()) {
    min_snapshot_ = -max_datum->get_int();
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skip index info is not valid", K(ret), KPC(this));
  }
  return ret;
}


int ObTransVersionSkipIndexReader::read_min_max_snapshot(
  const ObMacroBlockDesc &macro_desc,
  const int64_t trans_version_col_idx,
  ObTransVersionSkipIndexInfo &skip_index_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_desc.is_valid_with_macro_meta() || trans_version_col_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro block desc", K(ret), K(macro_desc));
  } else {
    const ObDataBlockMetaVal &macro_meta_val = macro_desc.macro_meta_->get_meta_val();
    const bool has_agg_data = (nullptr != macro_meta_val.agg_row_buf_) && (macro_meta_val.agg_row_len_ > 0);
    if (has_agg_data) {
      ObAggRowReader reader;
      if (OB_FAIL(reader.init(macro_meta_val.agg_row_buf_, macro_meta_val.agg_row_len_))) {
        LOG_WARN("Failed to init reader", K(ret), K(macro_desc));
      } else if (OB_FAIL(inner_read_min_max_snapshot(
          trans_version_col_idx, macro_meta_val.agg_row_buf_, macro_meta_val.agg_row_len_, skip_index_info))) {
        LOG_WARN("Failed to read min max snapshot", K(ret), K(macro_desc));
      }
    }
    if (OB_SUCC(ret) && (!has_agg_data || !skip_index_info.is_inited())) {
      skip_index_info.set(0, macro_desc.max_merged_trans_version_);
    }
    LOG_TRACE("read min max snapshot from macro block", K(trans_version_col_idx), K(skip_index_info), K(macro_desc));
  }
  return ret;
}

int ObTransVersionSkipIndexReader::read_min_max_snapshot(
  const ObMicroBlock &micro_block,
  const int64_t trans_version_col_idx,
  ObTransVersionSkipIndexInfo &skip_index_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_block.is_valid() || trans_version_col_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid micro block", K(ret), K(micro_block));
  } else {
    const ObMicroIndexInfo *micro_index_info = micro_block.micro_index_info_;
    if (OB_ISNULL(micro_index_info) || !micro_index_info->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid micro index info", K(ret), K(micro_block));
    } else if (micro_index_info->has_agg_data()
      && OB_FAIL(inner_read_min_max_snapshot(
        trans_version_col_idx, micro_index_info->agg_row_buf_, micro_index_info->agg_buf_size_, skip_index_info))) {
      LOG_WARN("Failed to read min max snapshot", K(ret), K(micro_block));
    } else {
      if (!micro_index_info->has_agg_data() || !skip_index_info.is_inited()) { // no agg data, use max version on micro block
        skip_index_info.set(0, micro_block.header_.max_merged_trans_version_);
      }
      LOG_TRACE("read min max snapshot from micro block", K(skip_index_info), K(micro_block));
    }
  }
  return ret;
}

int ObTransVersionSkipIndexReader::read_min_max_snapshot(
    const blocksstable::ObMicroIndexInfo &micro_index_info,
    const int64_t trans_version_col_idx,
    ObTransVersionSkipIndexInfo &skip_index_info)
{
  // TODO(menglan): agg row in index_info will be parsed many times, should tie the agg_row result and index_info together to avoid parsing repeatedly
  int ret = OB_SUCCESS;
  if (!micro_index_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid micro index info", K(ret), K(micro_index_info));
  } else if (micro_index_info.has_agg_data()
    && OB_FAIL(inner_read_min_max_snapshot(
      trans_version_col_idx, micro_index_info.agg_row_buf_, micro_index_info.agg_buf_size_, skip_index_info))) {
    LOG_WARN("Failed to read min max snapshot", K(ret), K(micro_index_info));
  } else {
    if (!micro_index_info.has_agg_data() || !skip_index_info.is_inited()) { // no agg data
      skip_index_info.set(0, INT64_MAX);
    }
    LOG_TRACE("read min max snapshot from micro block", K(skip_index_info), K(micro_index_info));
  }
  return ret;
}

int ObTransVersionSkipIndexReader::read_min_max_snapshot(
    const blocksstable::ObIndexBlockAggregator &aggregator,
    const int64_t trans_version_col_idx,
    ObTransVersionSkipIndexInfo &skip_index_info)
{
  int ret = OB_SUCCESS;
  const ObSkipIndexColMeta min_col_meta(trans_version_col_idx, SK_IDX_MIN);
  const ObSkipIndexColMeta max_col_meta(trans_version_col_idx, SK_IDX_MAX);
  const ObStorageDatum *min_datum = nullptr;
  const ObStorageDatum *max_datum = nullptr;
  if (OB_FAIL(aggregator.get_agg_datum(min_col_meta, min_datum))) {
    LOG_WARN("fail to get aggregated row", K(ret), K(min_col_meta), K(min_datum));
  } else if (OB_FAIL(aggregator.get_agg_datum(max_col_meta, max_datum))) {
    LOG_WARN("fail to get aggregated row", K(ret), K(max_col_meta), K(max_datum));
  } else if (OB_FAIL(skip_index_info.set(min_datum, max_datum))) {
    LOG_WARN("fail to set skip index info", K(ret), K(min_datum), K(max_datum));
  }
  return ret;
}

int ObTransVersionSkipIndexReader::inner_read_min_max_snapshot(
  const int64_t col_idx,
  const char *agg_row_buf,
  const int64_t agg_row_len,
  ObTransVersionSkipIndexInfo &skip_index_info)
{
  int ret = OB_SUCCESS;
  ObAggRowReader reader;
  skip_index_info.reset();
  if (OB_FAIL(reader.init(agg_row_buf, agg_row_len))) {
    LOG_WARN("Failed to init reader", K(ret), K(agg_row_buf), K(agg_row_len));
  } else {
    const ObSkipIndexColMeta min_col_meta(col_idx, SK_IDX_MIN);
    const ObSkipIndexColMeta max_col_meta(col_idx, SK_IDX_MAX);
    ObStorageDatum min_datum;
    ObStorageDatum max_datum;
    if (OB_FAIL(reader.read(min_col_meta, min_datum))) {
      LOG_WARN("Failed to read agg row", K(ret), K(min_col_meta));
    } else if (OB_FAIL(reader.read(max_col_meta, max_datum))) {
      LOG_WARN("Failed to read agg row", K(ret), K(max_col_meta));
    } else if (OB_FAIL(skip_index_info.set(min_datum, max_datum))) {
      LOG_WARN("Failed to set skip index info", K(ret), K(min_datum), K(max_datum));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
