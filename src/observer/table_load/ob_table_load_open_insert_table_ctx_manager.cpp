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

#include "observer/table_load/ob_table_load_open_insert_table_ctx_manager.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"

#define USING_LOG_PREFIX SERVER
namespace oceanbase
{
namespace observer
{
int ObTableLoadOpenInsertTableCtxManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadOpenInsertTableCtxManager init twice", KR(ret), K(is_inited_));
  } else {
    common::hash::ObHashMap<common::ObTabletID, ObDirectLoadInsertTabletContext *> &data_insert_tablet_ctxs =
      store_ctx_->data_store_table_ctx_->insert_table_ctx_->get_tablet_ctx_map();
    FOREACH_X(iter, data_insert_tablet_ctxs, OB_SUCC(ret)) {
      if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert tablet ctx is null", KR(ret));
      } else if (OB_FAIL(insert_tablet_ctxs_.push_back(iter->second))) {
        LOG_WARN("fail to push back insert tablet ctx", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); i++) {
      common::hash::ObHashMap<common::ObTabletID, ObDirectLoadInsertTabletContext *>
        &index_insert_tablet_ctxs =
          store_ctx_->index_store_table_ctxs_.at(i)->insert_table_ctx_->get_tablet_ctx_map();
      FOREACH_X(iter, index_insert_tablet_ctxs, OB_SUCC(ret))
      {
        if (OB_ISNULL(iter->second)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insert tablet ctx is null", KR(ret));
        } else if (OB_FAIL(insert_tablet_ctxs_.push_back(iter->second))) {
          LOG_WARN("fail to push back insert tablet ctx", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadOpenInsertTableCtxManager::get_next_insert_tablet_ctx(
  ObDirectLoadInsertTabletContext *&tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadOpenInsertTableCtxManager not init", KR(ret), K(is_inited_));
  } else {
    ObMutexGuard guard(op_lock_);
    if (insert_tablet_context_idx_ < insert_tablet_ctxs_.count()) {
      tablet_ctx = insert_tablet_ctxs_[insert_tablet_context_idx_];
      insert_tablet_context_idx_++;
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

void ObTableLoadOpenInsertTableCtxManager::handle_open_insert_tablet_ctx_finish(bool &is_finish)
{
  ObMutexGuard guard(op_lock_);
  is_finish = false;
  opened_insert_tablet_count_++;
  if (opened_insert_tablet_count_ == insert_tablet_ctxs_.count()) {
    is_finish = true;
  }
}

}
} // namespace oceanbase