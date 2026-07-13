/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_scalar_primary_ror_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASScalarPrimaryROROpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  // leaf node, no children
  return OB_SUCCESS;
}

int ObDASScalarPrimaryROROp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASScalarPrimaryROROpParam &scalar_op_param = static_cast<const ObDASScalarPrimaryROROpParam &>(op_param);
  if (OB_ISNULL(scalar_op_param.get_scan_ctdef()) || OB_ISNULL(scalar_op_param.get_scan_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    scalar_ctdef_ = scalar_op_param.get_scan_ctdef();
    scalar_rtdef_ = scalar_op_param.get_scan_rtdef();
    is_probe_mode_ = scalar_op_param.get_is_probe_mode();
  }
  LOG_TRACE("do init", K(ret), K(is_probe_mode_));
  return ret;
}

int ObDASScalarPrimaryROROp::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASScalarROROp::do_open())) {
    LOG_WARN("failed to open", K(ret));
  } else if (is_probe_mode_) {
    // Probe mode: set up an independent get_param_ used by do_probe / point_get. This
    // iterator is decoupled from scan_param_ so probing does not perturb the scan
    // iterator's position used by the base ROR semantics.
    if (OB_ISNULL(tsc_service_) || OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(tsc_service_), K(scalar_ctdef_), K(scalar_rtdef_));
    } else if (OB_FAIL(get_related_tablet_id(scalar_ctdef_, tablet_id_))) {
      LOG_WARN("failed to get related tablet id", K(ret));
    } else if (OB_FAIL(search_ctx_.init_scan_param(tablet_id_, scalar_ctdef_, scalar_rtdef_, get_param_))) {
      LOG_WARN("failed to init get param", K(ret), K(tablet_id_));
    } else {
      get_param_.op_ = nullptr;
      get_param_.is_get_ = true;
      get_param_.key_ranges_.reset();

      common::ObIAllocator &allocator = ctx_allocator();
      const ObIArray<ObNewRange> &rt_ranges = scalar_rtdef_->key_ranges_;
      for (int64_t i = 0; OB_SUCC(ret) && i < rt_ranges.count(); ++i) {
        const ObNewRange &src_range = rt_ranges.at(i);
        ObNewRange range;
        if (OB_FAIL(deep_copy_range(allocator, src_range, range))) {
          LOG_WARN("failed to deep copy range", K(ret));
        } else if (OB_FAIL(get_param_.key_ranges_.push_back(range))) {
          LOG_WARN("failed to push back key range", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_get_pd_op())) {
        LOG_WARN("failed to init get pd op", K(ret));
      } else if (OB_FAIL(tsc_service_->table_scan(get_param_, get_result_))) {
        LOG_WARN("failed to do table scan", K(ret));
      } else if (OB_ISNULL(get_result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr get result", K(ret));
      }
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::init_get_pd_op()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_rtdef_) || OB_ISNULL(scalar_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    ObEvalCtx *eval_ctx = scalar_rtdef_->eval_ctx_;
    if (OB_ISNULL(eval_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr eval ctx", K(ret));
    } else {
      new(&get_pd_expr_op_) ObPushdownOperator(*eval_ctx,
                                               scalar_ctdef_->pd_expr_spec_,
                                               scalar_rtdef_->scan_flag_.enable_rich_format_);
      if (OB_FAIL(get_pd_expr_op_.init_pushdown_storage_filter())) {
        LOG_WARN("failed to init pushdown storage filter for get param", K(ret));
      } else {
        get_param_.op_ = &get_pd_expr_op_;
      }
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASScalarROROp::do_close())) {
    LOG_WARN("failed to close", K(ret));
  }
  if (OB_NOT_NULL(get_result_) && OB_NOT_NULL(tsc_service_)) {
    int tmp_ret = OB_SUCCESS;
    common::ObIAllocator &allocator = ctx_allocator();
    if (OB_TMP_FAIL(tsc_service_->revert_scan_iter(get_result_))) {
      LOG_WARN("failed to revert scan iter", K(ret));
    }
    ret = ret == OB_SUCCESS ? tmp_ret : ret;
    get_result_ = nullptr;
    get_pd_expr_op_.~ObPushdownOperator();

    for (int64_t i = 0; i < get_param_.key_ranges_.count(); ++i) {
      ObNewRange &range = get_param_.key_ranges_.at(i);
      if (nullptr != range.start_key_.get_obj_ptr()) {
        allocator.free(range.start_key_.get_obj_ptr());
        range.start_key_.assign(nullptr, 0);
      }
      if (nullptr != range.end_key_.get_obj_ptr()) {
        allocator.free(range.end_key_.get_obj_ptr());
        range.end_key_.assign(nullptr, 0);
      }
    }

    get_param_.destroy_schema_guard();
    get_param_.snapshot_.reset();
    get_param_.destroy();
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASScalarROROp::do_rescan())) {
    LOG_WARN("failed to do rescan from parent", K(ret));
  } else if (is_probe_mode_) {
    if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_) || OB_ISNULL(tsc_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_), K(tsc_service_));
    } else if (OB_FAIL(get_related_tablet_id(scalar_ctdef_, tablet_id_))) {
      LOG_WARN("failed to get related tablet id", K(ret));
    } else if (FALSE_IT(ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), tablet_id_, get_param_))) {
    } else if (OB_FAIL(tsc_service_->reuse_scan_iter(get_param_.need_switch_param_, get_result_))) {
      LOG_WARN("failed to reuse scan iter", K(ret));
    } else {
      // reset key ranges to ensure correctness
      ObRangeArray &key_ranges = get_param_.key_ranges_;
      for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
        ObNewRange &range = key_ranges.at(i);
        ObObj *start_objs = range.start_key_.get_obj_ptr();
        ObObj *end_objs = range.end_key_.get_obj_ptr();
        int64_t obj_cnt = range.start_key_.get_obj_cnt();
        if (OB_ISNULL(start_objs) || OB_ISNULL(end_objs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr objs", K(ret), K(range));
        } else {
          for (int64_t j = 0; j < obj_cnt; ++j) {
            start_objs[j].set_min_value();
            end_objs[j].set_max_value();
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tsc_service_->table_rescan(get_param_, get_result_))) {
        LOG_WARN("failed to rescan table", K(ret), K(get_param_));
      } else {
        get_param_.need_switch_param_ = false;
      }
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (is_probe_mode_) {
    // Probe mode: this op is a non-driver follower coordinated by the parent conjunction,
    // which must only call probe() against it. Iterating the scan iterator is forbidden.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("advance_to not supported in primary probe mode", K(ret), K(target));
  } else if (OB_FAIL(ObDASScalarROROp::do_advance_to(target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance to", K(ret), K(target));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (is_probe_mode_) {
    // Probe mode: this op is a non-driver follower coordinated by the parent conjunction,
    // which must only call probe() against it. Iterating the scan iterator is forbidden.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("next_rowid not supported in primary probe mode", K(ret));
  } else if (OB_FAIL(ObDASScalarROROp::do_next_rowid(next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do next rowid", K(ret));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_advance_shallow(const ObDASRowID &target,
                                                const bool inclusive,
                                                const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (is_probe_mode_) {
    if (inclusive) {
      max_score_tuple_.set(target, target, 0.0);
    } else {
      ObDASRowID next_id;
      next_id.set_uint64(target.get_uint64() + 1);
      max_score_tuple_.set(next_id, next_id, 0.0);
    }
    max_score_tuple = &max_score_tuple_;
  } else if (OB_FAIL(ObIDASSearchOp::do_advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance shallow", K(ret), K(target));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_probe(const ObDASRowID &target, bool &hit)
{
  int ret = OB_SUCCESS;
  hit = false;
  if (!is_probe_mode_) {
    // Only the probe-mode supports probe.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("probe not supported in non-probe primary ROR mode", K(ret), K(target));
  } else if (OB_FAIL(point_get(target, hit))) {
    LOG_WARN("failed to do point get for probe", K(ret), K(target));
  } else {
    LOG_TRACE("primary probe", K(target), K(hit));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::point_get(const ObDASRowID &target, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;

  if (OB_ISNULL(tsc_service_) || OB_ISNULL(scalar_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(tsc_service_), K(scalar_ctdef_));
  } else {
    const ObIArray<ObExpr *> &rowkeys = get_rowid_exprs();
    ObRangeArray &key_ranges = get_param_.key_ranges_;

    if (OB_UNLIKELY(key_ranges.count() != 1)) {
      // do nothing
      LOG_TRACE("has more than one key range", K(key_ranges));
    } else if (OB_UNLIKELY(key_ranges.at(0).start_key_.get_obj_cnt() != rowkeys.count() ||
                           key_ranges.at(0).end_key_.get_obj_cnt() != rowkeys.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey count", K(ret), K(rowkeys), K(key_ranges));
    } else {
      ObObj *start_rowkey_objs = key_ranges.at(0).start_key_.get_obj_ptr();
      ObObj *end_rowkey_objs = key_ranges.at(0).end_key_.get_obj_ptr();

      for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
        const ObExpr *rowkey_expr = rowkeys.at(i);
        if (OB_ISNULL(rowkey_expr) || OB_ISNULL(start_rowkey_objs + i) || OB_ISNULL(end_rowkey_objs + i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr rowkey expr", K(ret));
        } else {
          ObDatum target_datum;
          if (OB_FAIL(get_datum_from_rowid(target, target_datum, i))) {
            LOG_WARN("failed to get datum from target rowid", K(ret), K(i));
          } else if (OB_FAIL(target_datum.to_obj(start_rowkey_objs[i], rowkey_expr->obj_meta_))) {
            LOG_WARN("failed to convert datum to obj", K(ret));
          } else if (OB_FAIL(target_datum.to_obj(end_rowkey_objs[i], rowkey_expr->obj_meta_))) {
            LOG_WARN("failed to convert datum to obj for end key", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        key_ranges.at(0).border_flag_.set_inclusive_start();
        key_ranges.at(0).border_flag_.set_inclusive_end();
        LOG_TRACE("point get", K(key_ranges.at(0)));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tsc_service_->reuse_scan_iter(false, get_result_))) {
        LOG_WARN("failed to reuse scan iter", K(ret));
      } else if (OB_FAIL(tsc_service_->table_rescan(get_param_, get_result_))) {
        LOG_WARN("failed to rescan table", K(ret));
      } else if (OB_FAIL(get_result_->get_next_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          found = false;
        } else {
          LOG_WARN("failed to get next row from point get result", K(ret));
        }
      } else {
        found = true;
      }
    }
  }

  return ret;
}

int ObDASScalarPrimaryROROp::advance_skip_scan(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  if (is_probe_mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("advance_skip_scan not supported in primary probe mode", K(ret), K(target));
  } else {
    ObRangeArray &key_ranges = scan_param_.key_ranges_;
    const ObIArray<ObExpr *> &rowkeys = get_rowid_exprs();
    if (OB_UNLIKELY(key_ranges.count() != 1)) {
      // do nothing
    } else if (OB_UNLIKELY(key_ranges.at(0).start_key_.get_obj_cnt() != rowkeys.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey count", K(ret), K(rowkeys), K(key_ranges));
    } else {
      ObObj *start_rowkey_objs = key_ranges.at(0).start_key_.get_obj_ptr();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); i++) {
        const ObExpr *rowkey_expr = rowkeys.at(i);
        if (OB_ISNULL(rowkey_expr) || OB_ISNULL(start_rowkey_objs + i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr rowkey expr", K(ret));
        } else {
          ObDatum target_datum;
          if (OB_FAIL(get_datum_from_rowid(target, target_datum, i))) {
            LOG_WARN("failed to get datum from target", K(ret), K(i));
          } else if (OB_FAIL(target_datum.to_obj(start_rowkey_objs[i], rowkey_expr->obj_meta_))) {
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
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
