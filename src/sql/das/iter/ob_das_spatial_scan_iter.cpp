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
#include "sql/das/iter/ob_das_spatial_scan_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASSpatialScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDASScanIter::inner_init(param))) {
    LOG_WARN("failed to init das scan iter", K(ret));
  } else {
    ObDASSpatialScanIterParam& scan_param = static_cast<ObDASSpatialScanIterParam&>(param);
    scan_rtdef_ = scan_param.scan_rtdef_;
    scan_ctdef_ = scan_param.scan_ctdef_;
  }

  return ret;
}

void ObDASSpatialScanIter::set_scan_param(storage::ObTableScanParam &scan_param) 
{ 
  mbr_filters_ = &scan_param.mbr_filters_;
  for (int64_t i = 0; i < scan_param.key_ranges_.count(); i++) {
    if (scan_param.key_ranges_.at(i).is_whole_range()) {
      is_whole_range_ = true;
    }
  }
  is_whole_range_ |= (mbr_filters_->count() == 0);

  ObDASScanIter::set_scan_param(scan_param);
}
  

int ObDASSpatialScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  bool got_row = false;
  do {
    if (OB_FAIL(ObDASScanIter::inner_get_next_row())) {
      LOG_WARN("failed to get next row", K(ret));
    } else if (OB_FAIL(filter_by_mbr(got_row))){
      LOG_WARN("failed to process data table rowkey", K(ret));
    }
  } while (OB_SUCC(ret) && !got_row);
  
  return ret;
}

int ObDASSpatialScanIter::filter_by_mbr(bool &got_row)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = max_rowkey_cnt_;
  if (max_rowkey_cnt_ < 0 || OB_ISNULL(obj_ptr_)) {
    rowkey_cnt = scan_ctdef_->result_output_.count() - 1;
    if (scan_ctdef_->trans_info_expr_ != nullptr) {
      rowkey_cnt = rowkey_cnt - 1;
    }
    max_rowkey_cnt_ = rowkey_cnt;

    void *buf = nullptr;
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
    } else {
      obj_ptr_ = new(buf) ObObj[rowkey_cnt];
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObExpr *expr = scan_ctdef_->result_output_.at(i);
    if (T_PSEUDO_GROUP_ID == expr->type_) {
      // do nothing
    } else {
      ObDatum &col_datum = expr->locate_expr_datum(*scan_rtdef_->eval_ctx_);
      if (OB_FAIL(col_datum.to_obj(obj_ptr_[i], expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      }
    }    
  }
  
  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr_, rowkey_cnt);
    bool pass_through = true;

    ObObj mbr_obj;
    ObExpr *mbr_expr = scan_ctdef_->result_output_.at(rowkey_cnt);
    ObDatum &mbr_datum = mbr_expr->locate_expr_datum(*scan_rtdef_->eval_ctx_);
    if (OB_FAIL(mbr_datum.to_obj(mbr_obj, mbr_expr->obj_meta_, mbr_expr->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else if (!is_whole_range_ && OB_FAIL(filter_by_mbr(mbr_obj, pass_through))) {
      LOG_WARN("filter mbr failed", K(ret));
    } else if (!is_whole_range_ && pass_through) {
      // not target
      mbr_filter_cnt_++;
    } else {
      got_row = true;
    }
  }

  return ret;
}

int ObDASSpatialScanIter::filter_by_mbr(const ObObj &mbr_obj, bool &pass_through)
{
  int ret = OB_SUCCESS;

  pass_through = true;
  ObString mbr_str = mbr_obj.get_varchar();
  bool is_point = (WKB_POINT_DATA_SIZE == mbr_str.length());
  ObSpatialMBR idx_spa_mbr;

  if (OB_FAIL(ObSpatialMBR::from_string(mbr_str, ObDomainOpType::T_INVALID, idx_spa_mbr, is_point))) {
    LOG_WARN("fail to create index spatial mbr", K(ret), K(mbr_obj));
  } else {
    idx_spa_mbr.is_point_ = is_point;
    for (int64_t i = 0; OB_SUCC(ret) && i < mbr_filters_->count() && pass_through; i++) {
      const ObSpatialMBR &spa_mbr = mbr_filters_->at(i);
      idx_spa_mbr.is_geog_ = spa_mbr.is_geog();
      if (OB_FAIL(idx_spa_mbr.filter(spa_mbr, spa_mbr.get_type(), pass_through))) {
        LOG_WARN("fail to filter by s2", K(ret), K(spa_mbr), K(idx_spa_mbr));
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
