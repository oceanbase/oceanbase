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
#include "sql/engine/dml/ob_dml_service.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{
int ObTableApiUpdateExecutor::process_single_operation(const ObTableEntity *entity)
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
      const ObTableUpdCtDef *upd_ctdef = &upd_spec_.get_ctdef();
      if (OB_FAIL(child_->open())) {
        LOG_WARN("fail to open child executor", K(ret));
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(ObTableExprCgService::refresh_update_exprs_frame(tb_ctx_,
                                                                          upd_ctdef->new_row_,
                                                                          *entity))) {
        LOG_WARN("fail to refresh update exprs frame", K(ret), K(*entity), K(cur_idx_));
      }
    }
  }

  return ret;
}

int ObTableApiUpdateExecutor::get_next_row_from_child()
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());

  if (cur_idx_ >= 1) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(process_single_operation(entity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to process single update operation", K(ret));
    }
  }

  return ret;
}

int ObTableApiUpdateExecutor::update_row_to_das()
{
  int ret = OB_SUCCESS;
  const ObTableUpdCtDef &upd_ctdef = upd_spec_.get_ctdef();
  ObDASTabletLoc *tablet_loc = nullptr;
  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("calc partition key failed", K(ret));
  } else {
    clear_evaluated_flag();
    if (OB_FAIL(ObDMLService::update_row(upd_ctdef.das_ctdef_,
                                         upd_rtdef_.das_rtdef_,
                                         tablet_loc,
                                         dml_rtctx_,
                                         upd_ctdef.full_row_))) {
      LOG_WARN("fail to update row to das op", K(ret), K(upd_ctdef), K(upd_rtdef_));
    }
  }

  return ret;
}

int ObTableApiUpdateExecutor::open()
{
  int ret = OB_SUCCESS;
  const ObTableUpdCtDef &ctdef = upd_spec_.get_ctdef();

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_upd_rtdef(ctdef, upd_rtdef_))) {
    LOG_WARN("fail to generate update rtdef", K(ret));
  }

  return ret;
}

int ObTableApiUpdateExecutor::upd_rows_post_proc()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(submit_all_dml_task())) {
    LOG_WARN("fail to execute all update das task", K(ret));
  } else {
    affected_rows_ += upd_rtdef_.das_rtdef_.affected_rows_;
  }
  return ret;
}

int ObTableApiUpdateExecutor::get_next_row()
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else if (OB_FAIL(update_row_to_das())) {
      LOG_WARN("fail tp update row to das", K(ret));
    }
    int tmp_ret = ret;
    if (OB_FAIL(child_->close())) { // 需要写到das后才close child算子，否则扫描的行已经被析构
      LOG_WARN("fail to close scan executor", K(ret));
    }
    ret = OB_SUCC(tmp_ret) ? ret : tmp_ret;
    cur_idx_++;
  }

  if (OB_ITER_END == ret) {
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
