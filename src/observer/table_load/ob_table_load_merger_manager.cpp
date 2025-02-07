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

#include "observer/table_load/ob_table_load_merger_manager.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_merger.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"

#define USING_LOG_PREFIX SERVER
namespace oceanbase
{
namespace observer
{
void ObTableLoadMergerManager::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMergerManager not init", KR(ret));
  } else {
    if (OB_ISNULL(store_ctx_->data_store_table_ctx_) || OB_ISNULL(store_ctx_->data_store_table_ctx_->merger_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected main store table ctx or merger is NULL", KR(ret));
    } else {
      store_ctx_->data_store_table_ctx_->merger_->stop();
    }
    for (int64_t i = 0; i < store_ctx_->index_store_table_ctxs_.count(); i++) {
      ObTableLoadStoreTableCtx * index_store_ctx = store_ctx_->index_store_table_ctxs_.at(i);
      if (OB_ISNULL(index_store_ctx) || OB_ISNULL(index_store_ctx->merger_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index store table ctx or merger is NULL", KR(ret), KP(index_store_ctx));
      } else {
        index_store_ctx->merger_->stop();
      }
    }

  }
}

int ObTableLoadMergerManager::handle_merge_finish()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMergerManager not init", KR(ret));
  } else {
    if (index_merger_idx_ < index_store_array_.size()) {
      if (OB_FAIL(index_store_array_.at(index_merger_idx_)->close_index_table_builder())) {
        LOG_WARN("fail to close index table builder", KR(ret));
      } else if (OB_FAIL(index_store_array_.at(index_merger_idx_)->merger_->start())) {
        LOG_WARN("fail to start index ObTableLoadMerger", KR(ret), K(index_merger_idx_));
      }
      LOG_INFO ("merge index start", K(index_merger_idx_));
      index_merger_idx_++;
    } else {
      if (OB_FAIL(store_ctx_->set_status_merged())) {
        LOG_WARN("fail to set status merged", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMergerManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMergerManager init twice", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; i < store_ctx_->index_store_table_ctxs_.count(); i++) {
      ObTableLoadStoreTableCtx * index_store_ctx = store_ctx_->index_store_table_ctxs_.at(i);
      if (OB_ISNULL(index_store_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index store table ctx is NULL", KR(ret));
      } else if (OB_FAIL(index_store_array_.push_back(index_store_ctx))) {
        LOG_WARN("fail to push back index store table ctx", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("INDEX MERGER COUNT", K(index_store_array_.size()));
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadMergerManager::commit(table::ObTableLoadDmlStat &dml_stats, table::ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMergerManager not init", KR(ret));
  } else if (OB_ISNULL(store_ctx_->data_store_table_ctx_) || OB_ISNULL(store_ctx_->data_store_table_ctx_->insert_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected main store table ctx or insert_table_ctx is NULL", KR(ret));
  } else if (OB_FAIL(store_ctx_->data_store_table_ctx_->insert_table_ctx_->commit(dml_stats, sql_statistics))) {
    LOG_WARN("fail to commit main ObTableLoadMerger", KR(ret));
  }
  return ret;
}

int ObTableLoadMergerManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMergerManager not init", KR(ret));
  } else if (OB_ISNULL(store_ctx_->data_store_table_ctx_) || OB_ISNULL(store_ctx_->data_store_table_ctx_->merger_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected main store table ctx or merger is NULL", KR(ret));
  } else if (OB_FAIL(store_ctx_->data_store_table_ctx_->merger_->start())) {
    LOG_WARN("fail to start main ObTableLoadMerger", KR(ret));
  }
  return ret;
}
}
}