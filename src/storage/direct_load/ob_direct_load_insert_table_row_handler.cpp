/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_insert_table_row_handler.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_lob_builder.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;

ObDirectLoadInsertTableRowHandler::ObDirectLoadInsertTableRowHandler()
  : insert_tablet_ctx_(nullptr),
    sql_statistics_(nullptr),
    lob_builder_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadInsertTableRowHandler::~ObDirectLoadInsertTableRowHandler() { reset(); }

void ObDirectLoadInsertTableRowHandler::reset()
{
  is_inited_ = false;
  insert_tablet_ctx_ = nullptr;
  sql_statistics_ = nullptr;
  if (nullptr != lob_builder_) {
    ObMemAttr mem_attr(MTL_ID(), "TLD_LobBuilder");
    OB_DELETE(ObDirectLoadLobBuilder, mem_attr, lob_builder_);
    lob_builder_ = nullptr;
  }
}

int ObDirectLoadInsertTableRowHandler::init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
                                            ObIAllocator *lob_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableRowHandler init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == insert_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(insert_tablet_ctx));
  } else {
    if (insert_tablet_ctx->get_online_opt_stat_gather()) {
      if (OB_FAIL(insert_tablet_ctx->get_table_ctx()->get_sql_statistics(sql_statistics_))) {
        LOG_WARN("fail to get sql statistics", KR(ret));
      }
    }
    if (OB_SUCC(ret) && insert_tablet_ctx->has_lob_storage()) {
      ObMemAttr mem_attr(MTL_ID(), "TLD_LobBuilder");
      if (OB_ISNULL(lob_builder_ = OB_NEW(ObDirectLoadLobBuilder, mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadLobBuilder", KR(ret));
      } else if (OB_FAIL(lob_builder_->init(insert_tablet_ctx, lob_allocator))) {
        LOG_WARN("fail to init lob builder", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      insert_tablet_ctx_ = insert_tablet_ctx;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowHandler::handle_row(ObDatumRow &datum_row, const bool skip_lob)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != sql_statistics_ &&
        OB_FAIL(insert_tablet_ctx_->get_table_ctx()->update_sql_statistics(*sql_statistics_,
                                                                           datum_row))) {
      LOG_WARN("fail to update sql statistics", KR(ret));
    } else if (!skip_lob && nullptr != lob_builder_ && insert_tablet_ctx_->get_is_insert_lob() &&
               OB_FAIL(lob_builder_->append_lob(datum_row))) {
      LOG_WARN("fail to append lob", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowHandler::handle_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != sql_statistics_ &&
        OB_FAIL(insert_tablet_ctx_->get_table_ctx()->update_sql_statistics(*sql_statistics_,
                                                                           datum_rows))) {
      LOG_WARN("fail to update sql statistics", KR(ret));
    } else if (nullptr != lob_builder_ &&
               OB_FAIL(lob_builder_->append_lob(datum_rows))) {
      LOG_WARN("fail to append lob", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowHandler::handle_row(ObDirectLoadDatumRow &datum_row,
                                                  const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != sql_statistics_ &&
        OB_FAIL(insert_tablet_ctx_->get_table_ctx()->update_sql_statistics(*sql_statistics_,
                                                                           datum_row,
                                                                           row_flag))) {
      LOG_WARN("fail to update sql statistics", KR(ret));
    } else if (nullptr != lob_builder_ &&
               OB_FAIL(lob_builder_->append_lob(datum_row, row_flag))) {
      LOG_WARN("fail to append lob", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowHandler::handle_row(const IVectorPtrs &vectors,
                                                  const int64_t row_idx,
                                                  const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != sql_statistics_ &&
        OB_FAIL(insert_tablet_ctx_->get_table_ctx()->update_sql_statistics(*sql_statistics_,
                                                                           vectors,
                                                                           row_idx,
                                                                           row_flag))) {
      LOG_WARN("fail to update sql statistics", KR(ret));
    } else if (nullptr != lob_builder_ &&
               OB_FAIL(lob_builder_->append_lob(vectors, row_idx, row_flag))) {
      LOG_WARN("fail to append lob", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowHandler::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != lob_builder_ && OB_FAIL(lob_builder_->close())) {
      LOG_WARN("fail to close lob", KR(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
