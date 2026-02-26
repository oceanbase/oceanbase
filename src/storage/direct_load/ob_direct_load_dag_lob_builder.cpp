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

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_dag_lob_builder.h"
#include "storage/ddl/ob_lob_macro_block_writer.h"
#include "storage/direct_load/ob_direct_load_insert_lob_table_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadDagLobBuilder
 */

ObDirectLoadDagLobBuilder::ObDirectLoadDagLobBuilder()
  : insert_tablet_ctx_(nullptr),
    slice_idx_(-1),
    lob_writer_(nullptr),
    lob_allocator_("TLD_LobAlloc"),
    is_inited_(false)
{
  lob_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadDagLobBuilder::~ObDirectLoadDagLobBuilder()
{
  OB_DELETE(ObLobMacroBlockWriter, ObMemAttr(MTL_ID(), "lob_writer"), lob_writer_);
}

int ObDirectLoadDagLobBuilder::init(ObDirectLoadInsertTabletContext *insert_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDagLobBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == insert_tablet_ctx || !insert_tablet_ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(insert_tablet_ctx));
  } else if (OB_UNLIKELY(!insert_tablet_ctx->has_lob_storage())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected has no lob", KR(ret), KPC(insert_tablet_ctx));
  } else {
    insert_tablet_ctx_ = insert_tablet_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadDagLobBuilder::switch_slice(const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagLobBuilder not init", KR(ret), KP(this));
  } else {
    if (nullptr != lob_writer_ && OB_FAIL(lob_writer_->close())) {
      LOG_WARN("fail to close lob writer", KR(ret));
    } else if (OB_FAIL(ObDDLUtil::fill_writer_param(insert_tablet_ctx_->get_tablet_id(), slice_idx,
                                                    -1 /*cg_idx*/, insert_tablet_ctx_->get_dag(),
                                                    0 /*max_batch_size*/, write_param_))) {
      LOG_WARN("fail to fill writer param", K(ret));
    } else {
      slice_idx_ = slice_idx;
      // 清理上一轮的lob_writer_
      OB_DELETE(ObLobMacroBlockWriter, ObMemAttr(MTL_ID(), "lob_writer"), lob_writer_);
    }
  }
  return ret;
}

int ObDirectLoadDagLobBuilder::append_lob(ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagLobBuilder not init", KR(ret), KP(this));
  } else {
    lob_allocator_.reuse();
    if (OB_FAIL(ObDDLUtil::handle_lob_columns(insert_tablet_ctx_->get_tablet_id(), slice_idx_,
                                              write_param_, lob_writer_, lob_allocator_,
                                              datum_row))) {
      LOG_WARN("fail to handle lob column", KR(ret), K(datum_row));
    }
  }
  return ret;
}

int ObDirectLoadDagLobBuilder::append_lob(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagLobBuilder not init", KR(ret), KP(this));
  } else {
    lob_allocator_.reuse();
    if (OB_FAIL(ObDDLUtil::handle_lob_columns(insert_tablet_ctx_->get_tablet_id(), slice_idx_,
                                              write_param_, lob_writer_, lob_allocator_,
                                              datum_rows))) {
      LOG_WARN("fail to convert to storage row", KR(ret), K(datum_rows));
    }
  }
  return ret;
}

int ObDirectLoadDagLobBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDagLobBuilder not init", KR(ret), KP(this));
  } else if (lob_writer_ != nullptr && OB_FAIL(lob_writer_->close())) {
    LOG_WARN("fail to close lob writer", KR(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
