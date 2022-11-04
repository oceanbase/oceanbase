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
    context_(context),
    can_blockscan_(false),
    filter_applied_(false),
    disabled_(false)
{}
ObBlockRowStore::~ObBlockRowStore()
{
  reset();
}

void ObBlockRowStore::reset()
{
  is_inited_ = false;
  can_blockscan_ = false;
  filter_applied_ = false;
  if (nullptr != context_.stmt_allocator_ && nullptr != pd_filter_info_.col_buf_) {
    context_.stmt_allocator_->free(pd_filter_info_.col_buf_);
    pd_filter_info_.col_buf_ = nullptr;
  }
  if (nullptr != context_.stmt_allocator_ && nullptr != pd_filter_info_.datum_buf_) {
    context_.stmt_allocator_->free(pd_filter_info_.datum_buf_);
    pd_filter_info_.datum_buf_ = nullptr;
  }
  pd_filter_info_.col_capacity_ = 0;
  pd_filter_info_.filter_ = nullptr;
  disabled_ = false;
}

void ObBlockRowStore::reuse()
{
  can_blockscan_ = false;
  filter_applied_ = false;
  disabled_ = false;
}

int ObBlockRowStore::init(const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  const ObTableIterParam &iter_param = param.iter_param_;
  int64_t out_col_cnt = iter_param.get_out_col_cnt();
  pd_filter_info_.is_pd_filter_ = iter_param.enable_pd_filter();
  const bool need_padding = is_pad_char_to_full_length(context_.sql_mode_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBlockRowStore init twice", K(ret));
  } else if (OB_UNLIKELY(!iter_param.is_valid() || nullptr == context_.stmt_allocator_
                         || nullptr == iter_param.get_col_params() || nullptr == iter_param.out_cols_project_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init store pushdown filter", K(ret), K(iter_param));
  } else if (nullptr == iter_param.pushdown_filter_) {
    // nothing to do without filter exprs
    is_inited_ = true;
  } else if (OB_UNLIKELY(0 == out_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpceted empty output", K(ret), K(iter_param));
  } else if (OB_ISNULL((buf = context_.stmt_allocator_->alloc(sizeof(ObObj) * out_col_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for pushdown filter col buf", K(ret), K(out_col_cnt));
  } else if (FALSE_IT(pd_filter_info_.col_buf_ = new (buf) ObObj[out_col_cnt]())) {
  } else if (OB_ISNULL((buf = context_.stmt_allocator_->alloc(sizeof(blocksstable::ObStorageDatum) * out_col_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for pushdown filter col buf", K(ret), K(out_col_cnt));
  } else if (FALSE_IT(pd_filter_info_.datum_buf_ = new (buf) blocksstable::ObStorageDatum[out_col_cnt]())) {
  } else if (OB_FAIL(iter_param.pushdown_filter_->init_filter_param(
              *iter_param.get_col_params(), *iter_param.out_cols_project_, need_padding))) {
    LOG_WARN("Failed to init pushdown filter executor", K(ret));
  } else {
    pd_filter_info_.filter_ = iter_param.pushdown_filter_;
    pd_filter_info_.col_capacity_ = out_col_cnt;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObBlockRowStore::apply_blockscan(
    blocksstable::ObIMicroBlockRowScanner &micro_scanner,
    const int64_t row_count,
    const bool can_pushdown,
    ObTableStoreStat &table_store_stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockRowStore is not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpceted row count of micro block", K(ret), K(row_count));
  } else if (!pd_filter_info_.is_pd_filter_ || !can_pushdown) {
    filter_applied_ = false;
  } else if (nullptr == pd_filter_info_.filter_) {
    // nothing to do
    filter_applied_ = true;
  } else if (OB_FAIL(filter_micro_block(row_count,
                                        micro_scanner,
                                        nullptr,
                                        pd_filter_info_.filter_))) {
    LOG_WARN("Failed to apply pushdown filter in block reader", K(ret), K(*this));
  } else {
    filter_applied_ = true;
  }

  if (OB_SUCC(ret)) {
    // Check pushdown filter successed
    can_blockscan_ = true;
    ++table_store_stat.pushdown_micro_access_cnt_;
    table_store_stat.pushdown_row_access_cnt_ += row_count;
    if (!filter_applied_ || nullptr == pd_filter_info_.filter_) {
      table_store_stat.pushdown_row_select_cnt_ += row_count;
    } else {
      table_store_stat.pushdown_row_select_cnt_ += pd_filter_info_.filter_->get_result()->popcnt();
      EVENT_ADD(ObStatEventIds::PUSHDOWN_STORAGE_FILTER_ROW_CNT, pd_filter_info_.filter_->get_result()->popcnt());
    }
    EVENT_ADD(ObStatEventIds::BLOCKSCAN_ROW_CNT, row_count);
    LOG_DEBUG("[PUSHDOWN] apply blockscan succ", K(row_count), KPC(pd_filter_info_.filter_), K(*this));
  }
  return ret;
}

int ObBlockRowStore::filter_micro_block(
    const int64_t row_count,
     blocksstable::ObIMicroBlockRowScanner &micro_scanner,
    sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor *filter)
{
  int ret = OB_SUCCESS;
  common::ObBitmap *result = nullptr;
  if (OB_UNLIKELY(nullptr == filter || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(filter), K(row_count));
  } else if (OB_FAIL(filter->init_bitmap(row_count, result))) {
    LOG_WARN("Failed to get filter bitmap", K(ret));
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter bitmap", K(ret));
  } else if (nullptr != parent && OB_FAIL(parent->prepare_skip_filter())) {
    LOG_WARN("Failed to check parent blockscan", K(ret));
  } else if (filter->is_filter_node()) {
    if (OB_FAIL(micro_scanner.filter_pushdown_filter(parent, filter, pd_filter_info_, *result))) {
      LOG_WARN("Failed to filter pushdown filter", K(ret), KPC(filter));
    }
  } else if (filter->is_logic_op_node()) {
    if (OB_UNLIKELY(filter->get_child_count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected child count of filter executor", K(ret), K(filter->get_child_count()), KP(filter));
    } else {
      sql::ObPushdownFilterExecutor **children = filter->get_childs();
      for (uint32_t i = 0; OB_SUCC(ret) && i < filter->get_child_count(); i++) {
        const common::ObBitmap *child_result = nullptr;
        if (OB_ISNULL(children[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child filter", K(ret));
        } else if (OB_FAIL(filter_micro_block(row_count, micro_scanner, filter, children[i]))) {
          LOG_WARN("Failed to filter micro block", K(ret), K(i), KP(children[i]));
        } else if (OB_ISNULL(child_result = children[i]->get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected get null filter bitmap", K(ret));
        } else {
          if (filter->is_logic_and_node()) {
            if (OB_FAIL(result->bit_and(*child_result))) {
              LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
            } else if (result->is_all_false()) {
              break;
            }
          } else  {
            if (OB_FAIL(result->bit_or(*child_result))) {
              LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
            } else if (result->is_all_true()) {
              break;
            }
          }
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported filter executor type", K(ret), K(filter->get_type()));
  }
  return ret;
}

int ObBlockRowStore::get_result_bitmap(const common::ObBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  bitmap = nullptr;
  if (nullptr == pd_filter_info_.filter_ || !filter_applied_) {
  } else if (OB_ISNULL(bitmap = pd_filter_info_.filter_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter bitmap", K(ret));
  }
  return ret;
}

int ObBlockRowStore::open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (nullptr == pd_filter_info_.filter_) {
    // nothing to do
  } else if (OB_FAIL(pd_filter_info_.filter_->init_evaluated_datums())) {
    LOG_WARN("Failed to init pushdown filter evaluated datums", K(ret));
  }
  return ret;
}

}
}
