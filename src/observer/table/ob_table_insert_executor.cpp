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

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{
int ObTableApiInsertSpec::init_ctdefs_array(int64_t size) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ins_ctdefs_.allocate_array(alloc_, size))) {
    LOG_WARN("fail to alloc ctdefs array", K(ret), K(size));
  } else {
    // init each element as nullptr
    for (int64_t i = 0; i < size; i++) {
      ins_ctdefs_.at(i) = nullptr;
    }
  }
  return ret;
}

ObTableApiInsertSpec::~ObTableApiInsertSpec()
{
  for (int64_t i = 0; i < ins_ctdefs_.count(); i++) {
    if (OB_NOT_NULL(ins_ctdefs_.at(i))) {
      ins_ctdefs_.at(i)->~ObTableInsCtDef();
    }
  }
  ins_ctdefs_.reset();
}

int ObTableApiInsertExecutor::open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(inner_open_with_das())) {
    LOG_WARN("fail to open insert executor", K(ret));
  }

  return ret;
}

int ObTableApiInsertExecutor::inner_open_with_das()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ins_rtdefs_.allocate_array(allocator_, ins_spec_.get_ctdefs().count()))) {
    LOG_WARN("fail to alloc ins rtdefs", K(ret));
  }
  for (int64_t i = 0; i < ins_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableInsRtDef &ins_rtdef = ins_rtdefs_.at(i);
    const ObTableInsCtDef *ins_ctdef = ins_spec_.get_ctdefs().at(i);
    if (OB_ISNULL(ins_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ins_ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(generate_ins_rtdef(*ins_ctdef, ins_rtdef))) {
      LOG_WARN("fail to generate insert rt_defs", K(ret), K(i));
    } else {
      ins_rtdef.das_rtdef_.use_put_ = tb_ctx_.is_client_use_put();
    }
  }
  return ret;
}

int ObTableApiInsertExecutor::insert_row_to_das()
{
  int ret = OB_SUCCESS;
  int64_t ctdef_count = ins_spec_.get_ctdefs().count();
  if (OB_UNLIKELY(ctdef_count != ins_rtdefs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of ins ctdef and rtdef is not equal ", K(ret), K(ctdef_count), K(ins_rtdefs_.count()));
  }
  for (int64_t i = 0; i < ins_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableInsRtDef &ins_rtdef = ins_rtdefs_.at(i);
    const ObTableInsCtDef *ins_ctdef = ins_spec_.get_ctdefs().at(i);
    if (OB_ISNULL(ins_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ins ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(ObTableApiModifyExecutor::insert_row_to_das(*ins_ctdef, ins_rtdef))) {
      LOG_WARN("fail to insert row to das", K(ret), K(i), K(ins_ctdef), K(ins_rtdef));
    }
  }
  return ret;
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
  // clean all rt_exprs evaluated flag before each operation refresh value
  clear_all_evaluated_flag();
  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (tb_ctx_.is_htable() && OB_FAIL(modify_htable_timestamp(entity))) {
    LOG_WARN("fail to modify htable timestamp", K(ret));
  } else if (ins_spec_.get_ctdefs().count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ins ctdef count is less than 1", K(ret));
  } else if (OB_ISNULL(ins_spec_.get_ctdefs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the first ins ctdef is NULL", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_insert_exprs_frame(tb_ctx_,
                                                                      ins_spec_.get_ctdefs().at(0)->new_row_,
                                                                      *entity))) {
    LOG_WARN("fail to refresh insert exprs frame", K(ret), K(*entity));
  }

  return ret;
}

int ObTableApiInsertExecutor::get_next_row_from_child()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableOperation> *ops = tb_ctx_.get_batch_operation();
  bool is_batch = (OB_NOT_NULL(ops) && !tb_ctx_.is_htable()) || (OB_NOT_NULL(ops) && tb_ctx_.is_client_use_put());

  // single operation
  if (!is_batch) {
    const ObTableEntity *entity = static_cast<const ObTableEntity *>(tb_ctx_.get_entity());
    if (cur_idx_ >= 1) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(process_single_operation(entity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to process single insert operation", K(ret));
      }
    }
  } else { // batch operation
    const ObIArray<ObTabletID> *tablet_ids = tb_ctx_.get_batch_tablet_ids();
    if (cur_idx_ >= ops->count()) {
      ret = OB_ITER_END;
    } else {
      const ObTableEntity *entity = static_cast<const ObTableEntity *>(&ops->at(cur_idx_).entity());
      if (tb_ctx_.is_multi_tablet_get()) {
        if (OB_UNLIKELY(tablet_ids->count() != ops->count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("use multi tablets batch but tablet ids is not equal to ops", K(tablet_ids->count()), K(ops->count()));
        } else {
          ObTabletID tablet_id= tablet_ids->at(cur_idx_);
          tb_ctx_.set_tablet_id(tablet_id);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(process_single_operation(entity))) {
        LOG_WARN("fail to process single insert operation", K(ret));
      }
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
