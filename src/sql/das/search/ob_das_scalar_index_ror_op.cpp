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
#include "ob_das_scalar_index_ror_op.h"
#include "sql/das/ob_das_def_reg.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/das/search/ob_das_search_context.h"

namespace oceanbase
{
namespace sql
{

int ObDASScalarIndexROROpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  // leaf node, no children
  return OB_SUCCESS;
}

int ObDASScalarIndexROROp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASScalarIndexROROpParam &scalar_op_param = static_cast<const ObDASScalarIndexROROpParam &>(op_param);
  if (OB_ISNULL(scalar_op_param.get_scan_ctdef()) || OB_ISNULL(scalar_op_param.get_scan_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    scalar_ctdef_ = scalar_op_param.get_scan_ctdef();
    scalar_rtdef_ = scalar_op_param.get_scan_rtdef();
  }
  return ret;
}

int ObDASScalarIndexROROp::advance_skip_scan(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  const ObDASRowIDType rowid_type = get_rowid_type();
  ObRangeArray &key_ranges = scan_param_.key_ranges_;
  const ObIArray<ObExpr *> &rowkeys = scalar_ctdef_->rowkey_exprs_;
  if (OB_UNLIKELY(key_ranges.count() != 1)) {
    // do nothing
  } else if (OB_UNLIKELY(key_ranges.at(0).start_key_.get_obj_cnt() <= rowkeys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey count", K(ret), K(rowkeys), K(key_ranges));
  } else {
    ObObj *start_rowkey_objs = key_ranges.at(0).start_key_.get_obj_ptr();
    int64_t rowkey_offset = key_ranges.at(0).start_key_.get_obj_cnt() - rowkeys.count();
    for (int64_t i = 0 ; i < rowkeys.count() ; i++) {
      const ObExpr *rowkey_expr = rowkeys.at(i);
      if (OB_ISNULL(rowkey_expr) || OB_ISNULL(start_rowkey_objs + rowkey_offset + i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr rowkey expr", K(ret));
      } else {
        ObDatum target_datum;
        if (OB_FAIL(get_datum_from_rowid(target, target_datum, i))) {
          LOG_WARN("failed to get datum from target rowid", K(ret), K(i));
        } else if (OB_FAIL(target_datum.to_obj(start_rowkey_objs[rowkey_offset + i], rowkey_expr->obj_meta_))) {
          LOG_WARN("failed to convert target datum to obj", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      key_ranges.at(0).border_flag_.set_inclusive_start();

      if (OB_ISNULL(tsc_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr tsc service", K(ret));
      } else if (OB_FAIL(tsc_service_->table_advance_scan(scan_param_, result_))) {
        LOG_WARN("failed to advance scan", K(ret));
      } else {
        LOG_TRACE("advance skip scan", K(key_ranges.at(0)));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
