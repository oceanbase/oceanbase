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
#include "ob_table_lock_executor.h"
#include "sql/engine/dml/ob_dml_service.h"

namespace oceanbase
{
namespace table
{
int ObTableApiLockExecutor::generate_lock_rtdef()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(lock_spec_.get_ctdef().das_ctdef_,
                                 lock_rtdef_.das_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  }

  return ret;
}

int ObTableApiLockExecutor::open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_lock_rtdef())) {
    LOG_WARN("fail to generate lock rtdef");
  }

  return ret;
}

int ObTableApiLockExecutor::get_next_row_from_child()
{
  int ret = OB_SUCCESS;

  if (cur_idx_ >= 1) {
    ret = OB_ITER_END;
  } else {
    common::ObIArray<ObNewRange> &key_ranges = tb_ctx_.get_key_ranges();
    const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());
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
        if (OB_FAIL(child_->open())) {
          LOG_WARN("fail to open child executor", K(ret));
        } else if (OB_FAIL(child_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          }
        }
        int tmp_ret = ret;
        if (OB_FAIL(child_->close())) {
          LOG_WARN("fail to close child executor", K(ret));
        } else {
          ret = tmp_ret;
        }
      }
    }
  }

  return ret;
}

int ObTableApiLockExecutor::lock_row_to_das()
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;

  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail tp calc tablet location", K(ret));
  } else if (OB_FAIL(ObDMLService::lock_row(lock_spec_.get_ctdef().das_ctdef_,
                                            lock_rtdef_.das_rtdef_,
                                            tablet_loc,
                                            dml_rtctx_,
                                            lock_spec_.get_ctdef().old_row_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret &&
        OB_TRANSACTION_SET_VIOLATION != ret &&
        OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
      LOG_WARN("fail to lock row with das", K(ret));
    }
  }

  return ret;
}

int ObTableApiLockExecutor::lock_rows_post_proc()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(submit_all_dml_task())) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret &&
        OB_TRANSACTION_SET_VIOLATION != ret &&
        OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
      LOG_WARN("fail to lock row with das", K(ret));
    }
  }

  return ret;
}

int ObTableApiLockExecutor::get_next_row()
{
  int ret = OB_SUCCESS;

  while(OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else if (OB_FAIL(lock_row_to_das())) {
      LOG_WARN("fail to lock row to das", K(ret));
    }
  }

  if (OB_ITER_END == ret) {
    if (OB_FAIL(lock_rows_post_proc())) {
      LOG_WARN("fail to post process after lock row", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObTableApiLockExecutor::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    // do nothing
  } else {
    ret = ObTableApiModifyExecutor::close();
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
