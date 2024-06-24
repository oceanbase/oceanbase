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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (param.type_ != ObDASIterType::DAS_ITER_SCAN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    const ObDASScanCtDef *scan_ctdef = (static_cast<ObDASScanIterParam&>(param)).scan_ctdef_;
    output_ = &scan_ctdef->result_output_;
    tsc_service_ = is_virtual_table(scan_ctdef->ref_table_id_) ? GCTX.vt_par_ser_
                              : scan_ctdef->is_external_table_ ? GCTX.et_access_service_
                                                               : MTL(ObAccessService *);
  }

  return ret;
}

int ObDASScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  // NOTE: need_switch_param_ should have been set before call reuse().
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_FAIL(tsc_service_->reuse_scan_iter(scan_param_->need_switch_param_, result_))) {
    LOG_WARN("failed to reuse storage scan iter", K(ret));
  } else {
    scan_param_->key_ranges_.reuse();
    scan_param_->ss_key_ranges_.reuse();
    scan_param_->mbr_filters_.reuse();
  }
  return ret;
}

int ObDASScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_)) {
    if (OB_FAIL(tsc_service_->revert_scan_iter(result_))) {
      LOG_WARN("failed to revert storage scan iter", K(ret));
    }
    result_ = nullptr;
  }
  return ret;
}

int ObDASScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  result_ = nullptr;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_FAIL(tsc_service_->table_scan(*scan_param_, result_))) {
    if (OB_SNAPSHOT_DISCARDED == ret && scan_param_->fb_snapshot_.is_valid()) {
      ret = OB_INVALID_QUERY_TIMESTAMP;
    } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", KPC_(scan_param), K(ret));
    }
  }
  LOG_DEBUG("das scan iter do table scan", KPC_(scan_param), K(ret));

  return ret;
}

int ObDASScanIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_FAIL(tsc_service_->table_rescan(*scan_param_, result_))) {
    LOG_WARN("failed to rescan tablet", K(scan_param_->tablet_id_), K(ret));
  } else {
    // reset need_switch_param_ after real rescan.
    scan_param_->need_switch_param_ = false;
  }
  LOG_DEBUG("das scan iter rescan", KPC_(scan_param), K(ret));

  return ret;
}

int ObDASScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(result_->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObDASScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(result_->get_next_rows(count, capacity))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  return ret;
}

void ObDASScanIter::clear_evaluated_flag()
{
  OB_ASSERT(nullptr != scan_param_);
  scan_param_->op_->clear_evaluated_flag();
}

}  // namespace sql
}  // namespace oceanbase
