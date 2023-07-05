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
#include "ob_table_insert_executor.h"
#include "ob_table_cg_service.h"
#include "sql/engine/dml/ob_dml_service.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{
int ObTableApiInsertExecutor::open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_ins_rtdef(ins_spec_.get_ctdef(), ins_rtdef_))) {
    LOG_WARN("fail to generate insert rtdef", K(ret));
  }

  return ret;
}

int ObTableApiInsertExecutor::insert_row_to_das()
{
  return ObTableApiModifyExecutor::insert_row_to_das(ins_spec_.get_ctdef(), ins_rtdef_);
}

int ObTableApiInsertExecutor::get_next_row()
{
  int ret = OB_SUCCESS;

  while(OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else if (OB_FAIL(insert_row_to_das())) {
      LOG_WARN("fail to insert row to das", K(ret));
    } else {
      cur_idx_++;
    }
  }

  if (OB_ITER_END == ret) {
    if (OB_FAIL(ins_rows_post_proc())) {
      LOG_WARN("fail to post process after insert row", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObTableApiInsertExecutor::process_single_operation(const ObTableEntity *entity)
{
  int ret = OB_SUCCESS;

  clear_evaluated_flag();
  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_insert_exprs_frame(tb_ctx_,
                                                                      ins_spec_.get_ctdef().new_row_,
                                                                      *entity))) {
    LOG_WARN("fail to refresh insert exprs frame", K(ret), K(*entity));
  }

  return ret;
}

int ObTableApiInsertExecutor::get_next_row_from_child()
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());

  if (cur_idx_ >= 1) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(process_single_operation(entity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to process single insert operation", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertExecutor::ins_rows_post_proc()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(submit_all_dml_task())) {
    LOG_WARN("fail to execute all insert das task", K(ret));
  } else {
    affected_rows_ = 1;
    // auto inc 操作中, 同步全局自增值value
    if (tb_ctx_.has_auto_inc() && OB_FAIL(tb_ctx_.update_auto_inc_value())) {
      LOG_WARN("fail to update auto inc value", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertExecutor::close()
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
