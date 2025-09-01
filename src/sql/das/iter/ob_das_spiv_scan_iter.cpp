/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_spiv_scan_iter.h"
#include "ob_das_scan_iter.h"
#include "ob_das_vec_scan_utils.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"

namespace oceanbase
{
namespace sql
{

int ObDASSPIVScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_SPIV_SCAN != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for spiv scan iter", K(ret), K(param));
  } else {
    ObDASSPIVScanIterParam spiv_scan_param = static_cast<ObDASSPIVScanIterParam &>(param);
    scan_iter_ = static_cast<ObDASScanIter *>(spiv_scan_param.scan_iter_);
    scan_ctdef_ = spiv_scan_param.scan_ctdef_;
    scan_rtdef_ = spiv_scan_param.scan_rtdef_;
    ls_id_ = spiv_scan_param.ls_id_;
    tx_desc_ = spiv_scan_param.tx_desc_;
    snapshot_ = spiv_scan_param.snapshot_;
    dim_ = spiv_scan_param.dim_;
    dim_docid_value_tablet_id_ = spiv_scan_param.dim_docid_value_tablet_id_;

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "SPIV_SCAN", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector spiv_scan memory context", K(ret));
      }
    }

    ObNewRange scan_range;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_range(scan_range, scan_ctdef_->ref_table_id_))){
      LOG_WARN("failed to build scan range", K(ret));
    } else if (OB_FAIL(scan_iter_param_.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to push lookup range", K(ret));
    } else if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, dim_docid_value_tablet_id_, scan_ctdef_, scan_rtdef_,
                                      tx_desc_, snapshot_, scan_iter_param_, false))) {
      LOG_WARN("failed to init scan param", K(ret), K(dim_docid_value_tablet_id_));
    } else {
      scan_iter_->set_scan_param(scan_iter_param_);
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDASSPIVScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(scan_iter_) && OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, scan_iter_, scan_iter_param_, dim_docid_value_tablet_id_))) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (is_first_scan_) {
    scan_iter_param_.need_switch_param_ = false;
  }

  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  if (OB_SUCC(ret)) {
    ObNewRange scan_range;
    if (OB_FAIL(build_range(scan_range, scan_ctdef_->ref_table_id_))){
      LOG_WARN("failed to build scan range", K(ret));
    } else if (OB_FAIL(scan_iter_param_.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to set lookup range", K(ret));
    }
  }

  return ret;
}

int ObDASSPIVScanIter::inner_release()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(scan_iter_) && OB_FAIL(scan_iter_->release())) {
    LOG_WARN("failed to release scan iter", K(ret));
  }

  ObDasVecScanUtils::release_scan_param(scan_iter_param_);

  scan_iter_ = nullptr;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  scan_ctdef_ = nullptr;
  scan_rtdef_ = nullptr;

  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }

  return ret;
}

int ObDASSPIVScanIter::build_range(ObNewRange &range, uint64_t table_id)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  ObObj *start_key_ptr = nullptr;
  ObObj *end_key_ptr = nullptr;
  ObRowkey start_key;
  ObRowkey end_key;

  if (OB_ISNULL(start_key_ptr = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_ISNULL(end_key_ptr = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    start_key_ptr[0].set_uint32(dim_);
    start_key_ptr[1].set_min_value();
    start_key.assign(start_key_ptr, 2);

    end_key_ptr[0].set_uint32(dim_);
    end_key_ptr[1].set_max_value();
    end_key.assign(end_key_ptr, 2);

    range.table_id_ = table_id;
    range.start_key_ = start_key;
    range.end_key_ = end_key;
  }

  return ret;
}

int ObDASSPIVScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter null", K(ret));
  } else if (is_first_scan_) {
    ret = scan_iter_->do_table_scan();
    is_first_scan_ = false;
  } else {
    ret = scan_iter_->rescan();
  }

  return ret;
}

int ObDASSPIVScanIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter null", K(ret));
  } else if (OB_FAIL(scan_iter_->rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

void ObDASSPIVScanIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(scan_iter_)) {
    scan_iter_->clear_evaluated_flag();
  }
}

int ObDASSPIVScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter null", K(ret));
  } else if (OB_FAIL(scan_iter_->get_next_row())) {
    LOG_WARN("failed to get next row", K(ret));
  }
  return ret;
}

int ObDASSPIVScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter null", K(ret));
  } else if (OB_FAIL(scan_iter_->get_next_rows(count, capacity))) {
    LOG_WARN("failed to get next row", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
