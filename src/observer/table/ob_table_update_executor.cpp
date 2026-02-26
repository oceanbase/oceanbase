/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_update_executor.h"
#include "ob_table_cg_service.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{

int ObTableApiUpdateSpec::init_ctdefs_array(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(udp_ctdefs_.allocate_array(alloc_, size))) {
    LOG_WARN("fail to alloc ctdefs array", K(ret), K(size));
  } else {
    // init each element as nullptr
    for (int64_t i = 0; i < size; i++) {
      udp_ctdefs_.at(i) = nullptr;
    }
  }
  return ret;
}

ObTableApiUpdateSpec::~ObTableApiUpdateSpec()
{
  for (int64_t i = 0; i < udp_ctdefs_.count(); i++) {
    if (OB_NOT_NULL(udp_ctdefs_.at(i))) {
      udp_ctdefs_.at(i)->~ObTableUpdCtDef();
    }
  }
  udp_ctdefs_.reset();
}

int ObTableApiUpdateExecutor::process_single_operation(const ObTableEntity *entity, bool &is_row_changed)
{
  int ret = OB_SUCCESS;
  common::ObIArray<ObNewRange> &key_ranges = tb_ctx_.get_key_ranges();

  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else {
    ObRowkey rowkey = entity->get_rowkey();
    ObNewRange range;
    // init key_ranges_
    key_ranges.reset();
    if (OB_FAIL(range.build_range(tb_ctx_.get_ref_table_id(), rowkey))) {
      LOG_WARN("fail to build key range", K(ret), K(tb_ctx_), K(rowkey));
    } else if (OB_FAIL(key_ranges.push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret), K(range));
    } else {
      clear_evaluated_flag();
      const ObTableUpdCtDef *upd_ctdef = upd_spec_.get_ctdefs().at(0);
      const ObIArray<ObExpr *> &new_row = tb_ctx_.is_inc_or_append() ? upd_ctdef->delta_row_ : upd_ctdef->new_row_;
      if (OB_FAIL(child_->open())) {
        LOG_WARN("fail to open child executor", K(ret));
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(ObTableExprCgService::refresh_update_exprs_frame(tb_ctx_, new_row, *entity))) {
        LOG_WARN("fail to refresh update exprs frame", K(ret), K(*entity), K(cur_idx_));
      } else {
        const ObIArray<ObExpr *> &old_row = upd_ctdef->old_row_;
        if (OB_FAIL(check_whether_row_change(new_row, old_row, eval_ctx_, *upd_ctdef, is_row_changed))) {
          LOG_WARN("fail to check whether row change", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableApiUpdateExecutor::get_next_row_from_child(bool& is_row_changed)
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());

  if (cur_idx_ >= 1) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(process_single_operation(entity, is_row_changed))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to process single update operation", K(ret));
    }
  }

  return ret;
}

int ObTableApiUpdateExecutor::update_row_to_das()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < upd_rtdefs_.count() && OB_SUCC(ret); i++) {
    const ObTableUpdCtDef &upd_ctdef = *upd_spec_.get_ctdefs().at(i);
    ObTableUpdRtDef &upd_rtdef = upd_rtdefs_.at(i);
    if (OB_FAIL(ObTableApiModifyExecutor::update_row_to_das(upd_ctdef, upd_rtdef, dml_rtctx_))) {
      LOG_WARN("fail to update row to das", K(ret));
    }
  }

  return ret;
}

int ObTableApiUpdateExecutor::open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(inner_open_with_das())) {
    LOG_WARN("fail to open update executor", K(ret));
  }

  return ret;
}

int ObTableApiUpdateExecutor::inner_open_with_das()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(upd_rtdefs_.allocate_array(allocator_, upd_spec_.get_ctdefs().count()))) {
    LOG_WARN("fail to alloc ins rtdefs", K(ret));
  }
  for (int64_t i = 0; i < upd_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableUpdRtDef &upd_rtdef = upd_rtdefs_.at(i);
    const ObTableUpdCtDef *upd_ctdef = nullptr;
    if (OB_ISNULL(upd_ctdef = upd_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(generate_upd_rtdef(*upd_ctdef, upd_rtdef))) {
      LOG_WARN("fail to generate update rtdef", K(ret));
    }
  }
  return ret;
}

int ObTableApiUpdateExecutor::upd_rows_post_proc()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(submit_all_dml_task())) {
    LOG_WARN("fail to execute all update das task", K(ret));
  } else {
    affected_rows_ += upd_rtdefs_.at(0).das_rtdef_.affected_rows_;
  }
  return ret;
}

int ObTableApiUpdateExecutor::get_next_row()
{
  int ret = OB_SUCCESS;

  bool has_row_changed = false;
  while (OB_SUCC(ret)) {
    // wether this row changed
    bool is_row_changed = false;
    if (OB_FAIL(get_next_row_from_child(is_row_changed))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else if (!is_row_changed) {
      LOG_INFO("update row not changed", K(tb_ctx_.get_entity()));
    } else if (OB_FAIL(update_row_to_das())) {
      LOG_WARN("fail tp update row to das", K(ret));
    } else {
      has_row_changed = true;
    }
    int tmp_ret = ret;
    if (OB_FAIL(child_->close())) { // 需要写到das后才close child算子，否则扫描的行已经被析构
      LOG_WARN("fail to close scan executor", K(ret));
    }
    ret = OB_SUCC(tmp_ret) ? ret : tmp_ret;
    cur_idx_++;
  }

  if (has_row_changed && OB_ITER_END == ret) {
    if (OB_FAIL(upd_rows_post_proc())) {
      LOG_WARN("fail to do update rows post process", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObTableApiUpdateExecutor::close()
{
  int ret = OB_SUCCESS;

  if (!is_opened_) {
    // do nothing
  } else if (OB_FAIL(ObTableApiModifyExecutor::close())) {
    LOG_WARN("fail to close ObTableApiModifyExecutor", K(ret));
  }

  return ret;
}
}  // namespace table
}  // namespace oceanbase
