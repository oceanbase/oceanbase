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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_data_channel.h"
#include "observer/table_load/ob_table_load_row_projector.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

/**
 * ObTableLoadTableChannel
 */

ObTableLoadTableChannel::ObTableLoadTableChannel(ObTableLoadTableOp *up_table_op,
                                                 ObTableLoadTableOp *down_table_op)
  : up_table_op_(up_table_op),
    down_table_op_(down_table_op),
    allocator_("TLD_TblChannel"),
    row_projector_(nullptr),
    is_closed_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadTableChannel::~ObTableLoadTableChannel()
{
  OB_DELETEx(ObTableLoadRowProjector, &allocator_, row_projector_);
}

int ObTableLoadTableChannel::open()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadTableChannel init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(down_table_op_->prepare())) {
      LOG_WARN("fail to prepare", KR(ret));
    } else if (OB_FAIL(create_row_projector())) {
      LOG_WARN("fail to create row projector", KR(ret));
    } else {
      if (down_table_op_->op_ctx_->table_store_.get_table_type() ==
          ObDirectLoadTableType::INVALID_TABLE_TYPE) {
        down_table_op_->op_ctx_->table_store_.set_table_type(get_table_type());
      } else if (down_table_op_->op_ctx_->table_store_.get_table_type() != get_table_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type", KR(ret),
                 K(down_table_op_->op_ctx_->table_store_.get_table_type()), K(get_table_type()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_builder_mgr_.init(
                 &down_table_op_->op_ctx_->table_store_,
                 down_table_op_->get_plan()->get_store_ctx()->tmp_file_mgr_,
                 down_table_op_->get_plan()->get_store_ctx()->table_mgr_))) {
      LOG_WARN("fail to init table builder mgr", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTableChannel::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret), K(is_closed_));
  } else {
    if (OB_FAIL(table_builder_mgr_.close())) {
      LOG_WARN("fail to close table builder mgr", KR(ret));
    } else {
      table_builder_mgr_.reset();
      is_closed_ = true;
    }
  }
  return ret;
}

int64_t ObTableLoadTableChannel::simple_to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  databuff_printf(buf, buf_len, pos, "%p", reinterpret_cast<const void *>(this));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "up_table_op: ");
  pos += up_table_op_->simple_to_string(buf + pos, buf_len - pos, false);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "down_table_op: ");
  pos += down_table_op_->simple_to_string(buf + pos, buf_len - pos, false);
  J_OBJ_END();
  return pos;
}

} // namespace observer
} // namespace oceanbase
