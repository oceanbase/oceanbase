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
#include "ob_block_row_store.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;
namespace storage
{

ObBlockRowStore::ObBlockRowStore(ObTableAccessContext &context)
    : is_inited_(false),
      pd_filter_info_(),
      context_(context),
      iter_param_(nullptr),
      disabled_(false),
      is_aggregated_in_prefetch_(false),
      where_optimizer_(nullptr)
{}

ObBlockRowStore::~ObBlockRowStore()
{
}

void ObBlockRowStore::reset()
{
  is_inited_ = false;
  pd_filter_info_.reset();
  disabled_ = false;
  is_aggregated_in_prefetch_ = false;
  iter_param_ = nullptr;
  if (where_optimizer_ != nullptr) {
    where_optimizer_->~ObWhereOptimizer();
    context_.stmt_allocator_->free(where_optimizer_);
    where_optimizer_ = nullptr;
  }
}

void ObBlockRowStore::reuse()
{
  disabled_ = false;
  is_aggregated_in_prefetch_ = false;
}

int ObBlockRowStore::init(const ObTableAccessParam &param, common::hash::ObHashSet<int32_t> *agg_col_mask)
{
  UNUSED(agg_col_mask);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBlockRowStore init twice", K(ret));
  } else if (OB_ISNULL(context_.stmt_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init store pushdown filter", K(ret));
  } else if (OB_FAIL(pd_filter_info_.init(param.iter_param_, *context_.stmt_allocator_))) {
    LOG_WARN("Fail to init pd filter info", K(ret));
  } else if (nullptr != context_.sample_filter_ 
              && OB_FAIL(context_.sample_filter_->combine_to_filter_tree(pd_filter_info_.filter_))) {
      LOG_WARN("Failed to combine sample filter to filter tree", K(ret), K_(pd_filter_info), KP_(context_.sample_filter));
  } else if (nullptr != pd_filter_info_.filter_ && !param.iter_param_.is_use_column_store() && param.iter_param_.enable_pd_filter_reorder()) {
    if (OB_UNLIKELY(nullptr != where_optimizer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected where optimizer", K(ret), KP_(where_optimizer));
    } else if (OB_ISNULL(where_optimizer_ = OB_NEWx(ObWhereOptimizer, context_.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for ObWhereOptimizer", K(ret));
    } else if (OB_FAIL(where_optimizer_->init(&param.iter_param_, pd_filter_info_.filter_))) {
      LOG_WARN("Failed to init where optimizer", K(ret), K(param.iter_param_), K(pd_filter_info_.filter_));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    iter_param_ = &param.iter_param_;
  } else {
    reset();
  }
  return ret;
}

int ObBlockRowStore::open(ObTableIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  const bool need_padding = is_pad_char_to_full_length(context_.sql_mode_);
  bool filter_valid = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_UNLIKELY(!iter_param.is_valid() ||
        nullptr == iter_param.get_col_params() ||
        nullptr == iter_param.out_cols_project_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init store pushdown filter", K(ret), K(iter_param));
  } else if (nullptr != context_.truncate_part_filter_
             && context_.truncate_part_filter_->need_combined_to_pd_filter()
             && OB_FAIL(context_.truncate_part_filter_->combine_to_filter_tree(pd_filter_info_.filter_))) {
    LOG_WARN("Failed to combine truncate filter to filter tree", K(ret), KP_(context_.truncate_part_filter));
  } else if (nullptr == pd_filter_info_.filter_) {
    // nothing to do
  } else if (OB_FAIL(pd_filter_info_.filter_->init_evaluated_datums(filter_valid))) {
    LOG_WARN("Failed to init pushdown filter evaluated datums", K(ret));
  } else {
    if (OB_UNLIKELY(!filter_valid)) {
      iter_param.disable_pd_filter();
      pd_filter_info_.is_pd_filter_ = false;
    }
    if (OB_FAIL(iter_param.build_index_filter_for_row_store(context_.allocator_))) {
      LOG_WARN("Failed to build skip index for row store", K(ret));
    } else if (iter_param.is_use_column_store()) {
      if (OB_FAIL(pd_filter_info_.filter_->init_co_filter_param(iter_param, need_padding))) {
        LOG_WARN("Failed to init pushdown filter executor", K(ret));
      }
    } else if (OB_FAIL(pd_filter_info_.filter_->init_filter_param(
                *iter_param.get_col_params(), *iter_param.out_cols_project_, need_padding))) {
      LOG_WARN("Failed to init pushdown filter executor", K(ret));
    }
  }
  return ret;
}

}
}

