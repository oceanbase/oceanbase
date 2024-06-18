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
#include "lib/stat/ob_diagnose_info.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/access/ob_table_access_context.h"

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
      can_blockscan_(false),
      filter_applied_(false),
      disabled_(false),
      is_aggregated_in_prefetch_(false)
{}

ObBlockRowStore::~ObBlockRowStore()
{
}

void ObBlockRowStore::reset()
{
  is_inited_ = false;
  can_blockscan_ = false;
  filter_applied_ = false;
  pd_filter_info_.reset();
  disabled_ = false;
  is_aggregated_in_prefetch_ = false;
  iter_param_ = nullptr;
}

void ObBlockRowStore::reuse()
{
  can_blockscan_ = false;
  filter_applied_ = false;
  disabled_ = false;
  is_aggregated_in_prefetch_ = false;
}

int ObBlockRowStore::init(const ObTableAccessParam &param)
{
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
  } else {
    is_inited_ = true;
    iter_param_ = &param.iter_param_;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObBlockRowStore::apply_blockscan(
    blocksstable::ObIMicroBlockRowScanner &micro_scanner,
    const bool can_pushdown,
    ObTableScanStoreStat &table_store_stat)
{
  int ret = OB_SUCCESS;
  int64_t access_count = micro_scanner.get_access_cnt();
  if (iter_param_->has_lob_column_out()) {
    context_.reuse_lob_locator_helper();
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockRowStore is not inited", K(ret), K(*this));
  } else if (!pd_filter_info_.is_pd_filter_ || !can_pushdown) {
    filter_applied_ = false;
  } else if (nullptr == pd_filter_info_.filter_) {
    // nothing to do
    filter_applied_ = true;
  } else if (OB_FAIL(micro_scanner.filter_micro_block_in_blockscan(pd_filter_info_))) {
    LOG_WARN("Failed to apply pushdown filter in block reader", K(ret), K(*this));
  } else {
    filter_applied_ = true;
  }

  if (OB_SUCC(ret)) {
    // Check pushdown filter successed
    can_blockscan_ = true;
    ++table_store_stat.pushdown_micro_access_cnt_;
    if (!filter_applied_ || nullptr == pd_filter_info_.filter_) {
    } else {
      int64_t select_cnt = pd_filter_info_.filter_->get_result()->popcnt();
      EVENT_ADD(ObStatEventIds::PUSHDOWN_STORAGE_FILTER_ROW_CNT, select_cnt);
    }
    if (iter_param_->has_lob_column_out()) {
      context_.reuse_lob_locator_helper();
    }
    EVENT_ADD(ObStatEventIds::BLOCKSCAN_ROW_CNT, access_count);
    LOG_DEBUG("[PUSHDOWN] apply blockscan succ", K(access_count), KPC(pd_filter_info_.filter_), K(*this));
  }
  return ret;
}

int ObBlockRowStore::get_filter_result(ObFilterResult &res)
{
  int ret = OB_SUCCESS;
  res.bitmap_ = nullptr;
  res.filter_start_ = pd_filter_info_.start_;
  if (nullptr == pd_filter_info_.filter_ || !filter_applied_) {
  } else if (OB_ISNULL(res.bitmap_ = pd_filter_info_.filter_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter bitmap", K(ret));
  } else if (res.bitmap_->is_all_true()) {
    res.bitmap_ = nullptr;
  }
  return ret;
}

int ObBlockRowStore::open(const ObTableIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  const bool need_padding = is_pad_char_to_full_length(context_.sql_mode_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_UNLIKELY(!iter_param.is_valid() ||
        nullptr == iter_param.get_col_params() ||
        nullptr == iter_param.out_cols_project_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init store pushdown filter", K(ret), K(iter_param));
  } else if (nullptr == pd_filter_info_.filter_) {
    // nothing to do
  } else {
    if (iter_param.is_use_column_store()) {
      if (OB_FAIL(pd_filter_info_.filter_->init_co_filter_param(iter_param, need_padding))) {
        LOG_WARN("Failed to init pushdown filter executor", K(ret));
      }
    } else if (OB_FAIL(pd_filter_info_.filter_->init_filter_param(
            *iter_param.get_col_params(), *iter_param.out_cols_project_, need_padding))) {
      LOG_WARN("Failed to init pushdown filter executor", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pd_filter_info_.filter_->init_evaluated_datums())) {
      LOG_WARN("Failed to init pushdown filter evaluated datums", K(ret));
    }
  }
  return ret;
}

}
}
