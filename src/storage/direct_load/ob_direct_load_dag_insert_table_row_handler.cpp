/**
 * Copyright (c) 2025 OceanBase
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

#include "storage/direct_load/ob_direct_load_dag_insert_table_row_handler.h"
#include "storage/direct_load/ob_direct_load_dag_lob_builder.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace observer;

ObDirectLoadDagInsertTableRowHandler::ObDirectLoadDagInsertTableRowHandler()
  : insert_tablet_ctx_(nullptr), sql_statistics_(nullptr), lob_builder_(nullptr), is_inited_(false)
{
}

ObDirectLoadDagInsertTableRowHandler::~ObDirectLoadDagInsertTableRowHandler()
{
  OB_DELETE(ObDirectLoadDagLobBuilder, ObMemAttr(MTL_ID(), "TLD_LobBuilder"), lob_builder_);
}

int ObDirectLoadDagInsertTableRowHandler::init(ObDirectLoadInsertTabletContext *insert_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDagInsertTableRowHandler init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == insert_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(insert_tablet_ctx));
  } else {
    insert_tablet_ctx_ = insert_tablet_ctx;
    if (insert_tablet_ctx_->get_online_opt_stat_gather()) {
      if (OB_FAIL(insert_tablet_ctx_->get_table_ctx()->get_sql_statistics(sql_statistics_))) {
        LOG_WARN("fail to get sql statistics", KR(ret));
      }
    }
    if (OB_SUCC(ret) && insert_tablet_ctx_->has_lob_storage() &&
        insert_tablet_ctx_->get_is_insert_lob()) {
      if (OB_ISNULL(lob_builder_ =
                      OB_NEW(ObDirectLoadDagLobBuilder, ObMemAttr(MTL_ID(), "TLD_LobBuilder")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadDagLobBuilder", KR(ret));
      } else if (OB_FAIL(lob_builder_->init(insert_tablet_ctx_))) {
        LOG_WARN("fail to init lob builder", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableRowHandler::switch_slice(const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != lob_builder_ && OB_FAIL(lob_builder_->switch_slice(slice_idx))) {
      LOG_WARN("fail to switch slice", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableRowHandler::handle_row(ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != sql_statistics_ &&
        OB_FAIL(insert_tablet_ctx_->get_table_ctx()->update_sql_statistics(*sql_statistics_,
                                                                           datum_row))) {
      LOG_WARN("fail to update sql statistics", KR(ret));
    } else if (nullptr != lob_builder_ && OB_FAIL(lob_builder_->append_lob(datum_row))) {
      LOG_WARN("fail to append lob", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableRowHandler::handle_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != sql_statistics_ &&
        OB_FAIL(insert_tablet_ctx_->get_table_ctx()->update_sql_statistics(*sql_statistics_,
                                                                           datum_rows))) {
      LOG_WARN("fail to update sql statistics", KR(ret));
    } else if (nullptr != lob_builder_ && OB_FAIL(lob_builder_->append_lob(datum_rows))) {
      LOG_WARN("fail to append lob", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadDagInsertTableRowHandler::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagInsertTableRowHandler not init", KR(ret), KP(this));
  } else {
    if (nullptr != lob_builder_ && OB_FAIL(lob_builder_->close())) {
      LOG_WARN("fail to close lob", KR(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
