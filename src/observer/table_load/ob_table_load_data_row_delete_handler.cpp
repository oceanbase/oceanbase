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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_data_row_delete_handler.h"
#include "observer/table_load/ob_table_load_index_table_builder.h"
#include "observer/table_load/ob_table_load_lob_table_builder.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace storage;

ObTableLoadDataRowDeleteHandler::ObTableLoadDataRowDeleteHandler()
  : store_ctx_(nullptr), is_inited_(false)
{
}

ObTableLoadDataRowDeleteHandler::~ObTableLoadDataRowDeleteHandler() {}

int ObTableLoadDataRowDeleteHandler::init(ObTableLoadStoreCtx *store_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDataRowDeleteHandler init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx));
  } else if (OB_UNLIKELY(!ObDirectLoadMethod::is_incremental(store_ctx->ctx_->param_.method_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is not incremental direct load", KR(ret));
  } else {
    store_ctx_ = store_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadDataRowDeleteHandler::handle_delete_row(const ObTabletID &tablet_id,
                                                       const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowDeleteHandler not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
      ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
      if (!index_table_ctx->schema_->is_local_unique_index()) {
        ObTableLoadIndexTableBuilder *index_builder = nullptr;
        if (OB_FAIL(index_table_ctx->get_delete_table_builder(index_builder))) {
          LOG_WARN("fail to get index table builder", KR(ret));
        } else if (OB_FAIL(index_builder->append_delete_row(tablet_id, datum_row))) {
          LOG_WARN("fail to append delete row", KR(ret), K(tablet_id), K(datum_row));
        }
      }
    }
    if (OB_SUCC(ret) && nullptr != store_ctx_->data_store_table_ctx_->lob_table_ctx_) {
      ObTableLoadStoreLobTableCtx *lob_table_ctx =
        store_ctx_->data_store_table_ctx_->lob_table_ctx_;
      ObTableLoadLobTableBuilder *lob_builder = nullptr;
      if (OB_FAIL(lob_table_ctx->get_delete_table_builder(lob_builder))) {
        LOG_WARN("fail to get lob table builder", KR(ret));
      } else if (OB_FAIL(lob_builder->append_delete_row(tablet_id, datum_row))) {
        LOG_WARN("fail to append delete row", KR(ret), K(tablet_id), K(datum_row));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase