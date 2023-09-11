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
#include "ob_table_delete_executor.h"
#include "ob_table_scan_executor.h"
#include "ob_htable_utils.h"
#include "ob_htable_filter_operator.h"
#include "ob_table_cg_service.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{

int ObTableApiDeleteExecutor::open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_del_rtdef(del_spec_.get_ctdef(), del_rtdef_))) {
    LOG_WARN("fail to generate delete rtdef");
  } else {
    if (tb_ctx_.is_skip_scan()) {
      set_entity(tb_ctx_.get_entity());
      set_skip_scan(true);
    }
  }

  return ret;
}

int ObTableApiDeleteExecutor::process_single_operation(const ObTableEntity *entity)
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
      LOG_WARN("fail to build key range", K(ret), K_(tb_ctx), K(rowkey));
    } else if (OB_FAIL(key_ranges.push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret), K(range));
    } else {
      clear_evaluated_flag();
      if (OB_FAIL(child_->open())) {
        LOG_WARN("fail to open child executor", K(ret));
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableApiDeleteExecutor::get_next_row_from_child()
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());

  if (cur_idx_ >= 1) {
    // prevent the second time process operation if the first one successfully
    ret = OB_ITER_END;
  } else if (OB_FAIL(process_single_operation(entity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to process single delete operation", K(ret));
    }
  }

  return ret;
}

int ObTableApiDeleteExecutor::del_rows_post_proc()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(submit_all_dml_task())) {
    LOG_WARN("fail to execute all delete das task", K(ret));
  } else {
    affected_rows_ = del_rtdef_.das_rtdef_.affected_rows_;
  }

  return ret;
}

int ObTableApiDeleteExecutor::delete_row_skip_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is NULL", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_delete_exprs_frame(tb_ctx_,
                                                                      del_spec_.get_ctdef().old_row_,
                                                                      static_cast<const ObTableEntity &>(*entity_)))) {
    LOG_WARN("fail to refresh delete exprs frame", K(ret));
  } else if (OB_FAIL(delete_row_to_das(del_spec_.get_ctdef(), del_rtdef_))) {
    LOG_WARN("fail to delete row to das", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(del_rows_post_proc())) {
    LOG_WARN("fail to post process after delete row", K(ret));
  }

  return ret;
}

int ObTableApiDeleteExecutor::get_next_row()
{
  int ret = OB_SUCCESS;

  if (is_skip_scan()) {
    if (OB_FAIL(delete_row_skip_scan())) {
      LOG_WARN("fail to process delete", K(ret));
    }
  } else {
    while(OB_SUCC(ret)) {
      if (OB_FAIL(get_next_row_from_child())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(delete_row_to_das(del_spec_.get_ctdef(), del_rtdef_))) {
        LOG_WARN("fail to delete row to das", K(ret));
      }

      int tmp_ret = ret;
      if (OB_FAIL(child_->close())) { // 需要写到das后才close child算子，否则扫描的行已经被析构
        LOG_WARN("fail to close scan executor", K(ret));
      }
      ret = OB_SUCC(tmp_ret) ? ret : tmp_ret;
      cur_idx_++;
    }

    if (OB_ITER_END == ret) {
      if (OB_FAIL(del_rows_post_proc())) {
        LOG_WARN("fail to post process after delete row", K(ret));
      } else {
        ret = OB_ITER_END;
      }
    }
  }

  return ret;
}

int ObTableApiDeleteExecutor::close()
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