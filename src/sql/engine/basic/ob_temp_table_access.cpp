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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_temp_table_access.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
namespace sql {

ObTempTableAccessInput::ObTempTableAccessInput()
    : deserialize_allocator_(NULL), closed_count_(0), unfinished_count_ptr_(0), interm_result_ids_()
{}

ObTempTableAccessInput::~ObTempTableAccessInput()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  closed_count_ = 0;
  interm_result_ids_.reset();
}

void ObTempTableAccessInput::reset()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  closed_count_ = 0;
  interm_result_ids_.reset();
}

ObPhyOperatorType ObTempTableAccessInput::get_phy_op_type() const
{
  return PHY_TEMP_TABLE_ACCESS;
}

void ObTempTableAccessInput::set_deserialize_allocator(common::ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

int ObTempTableAccessInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return ret;
}

int ObTempTableAccessInput::check_closed_finish(bool& is_end)
{
  int ret = OB_SUCCESS;
  uint64_t closed_cnt = 0;
  uint64_t* count_ptr = reinterpret_cast<uint64_t*>(closed_count_);
  if (NULL != count_ptr) {
    closed_cnt = ATOMIC_AAF(count_ptr, -1);
    if (0 >= closed_cnt) {
      is_end = true;
    } else {
      is_end = false;
    }
  } else {
    is_end = true;
  }
  return ret;
}

int ObTempTableAccessInput::check_finish(bool& is_end, int64_t& index)
{
  int ret = OB_SUCCESS;
  uint64_t* count_ptr = reinterpret_cast<uint64_t*>(unfinished_count_ptr_);
  if (NULL != count_ptr) {
    index = ATOMIC_AAF(count_ptr, -1);
    if (-1 >= index) {
      is_end = true;
    } else {
      is_end = false;
    }
  } else {
    is_end = true;
  }
  return ret;
}

int ObTempTableAccessInput::assign_ids(common::ObIArray<uint64_t>& interm_res_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(interm_result_ids_.assign(interm_res_ids))) {
    LOG_WARN("failed to assign to interm result ids.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTempTableAccessInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTempTableAccessInput);
  LST_DO_CODE(OB_UNIS_ENCODE, closed_count_);
  LST_DO_CODE(OB_UNIS_ENCODE, unfinished_count_ptr_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, interm_result_ids_.count()))) {
      LOG_WARN("fail to encode key ranges count", K(ret), K(interm_result_ids_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < interm_result_ids_.count(); ++i) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, interm_result_ids_.at(i)))) {
        LOG_WARN("fail to serialize key range", K(ret), K(i));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTempTableAccessInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObTempTableAccessInput);
  LST_DO_CODE(OB_UNIS_DECODE, closed_count_);
  LST_DO_CODE(OB_UNIS_DECODE, unfinished_count_ptr_);
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    interm_result_ids_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    } else { /*do nothing.*/
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      int64_t interm_result_id = 0;
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &interm_result_id))) {
        LOG_WARN("failed to decode vi64.", K(ret));
      } else if (OB_FAIL(interm_result_ids_.push_back(interm_result_id))) {
        LOG_WARN("failed to push back into interm result ids.", K(ret));
      } else { /*do nothing.*/
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTempTableAccessInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTempTableAccessInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, closed_count_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, unfinished_count_ptr_);
  len += serialization::encoded_length_vi64(interm_result_ids_.count());
  for (int64_t i = 0; i < interm_result_ids_.count(); ++i) {
    len += serialization::encoded_length_vi64(interm_result_ids_.at(i));
  }
  return len;
}

int ObTempTableAccess::ObTempTableAccessCtx::locate_interm_result(dtl::ObDTLIntermResultKey& dtl_int_key)
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultInfo result_info;
  if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().get_interm_result_info(dtl_int_key, result_info))) {
    LOG_WARN("failed to create row store.", K(ret));
  } else if (FALSE_IT(row_store_ = result_info.row_store_)) {
  } else if (OB_FAIL(row_store_->begin(row_store_it_))) {
    LOG_WARN("failed to begin chunk row store.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

void ObTempTableAccess::reset()
{
  ObNoChildrenPhyOperator::reset();
}

void ObTempTableAccess::reuse()
{
  ObNoChildrenPhyOperator::reuse();
}

int ObTempTableAccess::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  return ret;
}

int ObTempTableAccess::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init op ctx.", K(ret));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("failed to handle operator context.", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

int ObTempTableAccess::destory_interm_results(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  ObTempTableAccessCtx* tta_ctx = NULL;
  ObTempTableAccessInput* tta_input = NULL;
  if (OB_ISNULL(tta_ctx = GET_PHY_OPERATOR_CTX(ObTempTableAccessCtx, exec_ctx, get_id()))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL(tta_input = tta_ctx->tta_input_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("temp table access input is null.", K(ret));
  } else if (is_distributed_) {
    if (!need_release_) {
      // do nothing.
    } else if (OB_FAIL(tta_input->check_closed_finish(is_end))) {
      LOG_WARN("failed to check closed finish.", K(ret));
    } else if (is_end) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tta_input->interm_result_ids_.count(); ++i) {
        dtl::ObDTLIntermResultKey dtl_int_key;
        dtl_int_key.channel_id_ = tta_input->interm_result_ids_.at(i);
        if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().erase_interm_result_info(dtl_int_key))) {
          LOG_WARN("failed to erase interm result info in manager.", K(ret));
        } else { /*do nothing.*/
        }
      }
    }
  } else if (need_release_) {
    dtl::ObDTLIntermResultKey dtl_int_key;
    dtl_int_key.channel_id_ = temp_table_id_;
    if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().erase_interm_result_info(dtl_int_key))) {
      LOG_WARN("failed to erase interm result info in manager.", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableAccess::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(destory_interm_results(exec_ctx))) {
    LOG_WARN("failed to destory interm results.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableAccess::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTempTableAccessCtx* op_ctx = NULL;
  ObTempTableAccessInput* op_input = NULL;
  if (OB_UNLIKELY(filter_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter exprs should be empty", K(ret), K(filter_exprs_.get_size()));
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTempTableAccessCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("fail to int cur row", K(ret));
  } else if (OB_ISNULL(op_input = GET_PHY_OP_INPUT(ObTempTableAccessInput, ctx, get_id()))) {
    if (OB_FAIL(create_operator_input(ctx))) {
      LOG_WARN("fail to create operator input", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(op_input = GET_PHY_OP_INPUT(ObTempTableAccessInput, ctx, get_id()))) {
    LOG_WARN("failed to get op input.", K(ret));
  } else {
    op_ctx->tta_input_ = op_input;
  }
  return ret;
}

int ObTempTableAccess::construct_row_from_interm_row(
    ObExecContext& ctx, ObNewRow* input_row, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTempTableAccessCtx* tta_ctx = NULL;
  if (OB_ISNULL(tta_ctx = GET_PHY_OPERATOR_CTX(ObTempTableAccessCtx, ctx, get_id()))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else { /*do nothing.*/
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_indexs_.count(); ++i) {
    int64_t col_idx = output_indexs_.at(i);
    if (OB_UNLIKELY(i >= tta_ctx->get_cur_row().count_) || OB_UNLIKELY(col_idx >= input_row->get_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row not match", K(i), K(*input_row));
    } else {
      tta_ctx->get_cur_row().cells_[i] = input_row->get_cell(col_idx);
    }
  }
  if (OB_SUCC(ret)) {
    row = &(tta_ctx->get_cur_row());
  }
  return ret;
}

int ObTempTableAccess::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  common::ObNewRow* input_row = NULL;
  ObTempTableAccessCtx* tta_ctx = NULL;
  if (OB_ISNULL(tta_ctx = GET_PHY_OPERATOR_CTX(ObTempTableAccessCtx, ctx, get_id()))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (!tta_ctx->is_started_ && OB_FAIL(locate_next_interm_result(ctx, is_end))) {
    LOG_WARN("failed to locate next iterm result.", K(ret));
  } else if (!tta_ctx->is_started_ && is_end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(tta_ctx->row_store_it_.get_next_row(input_row))) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(locate_next_interm_result(ctx, is_end))) {
        LOG_WARN("failed to locate next iterm result.", K(ret));
      } else if (is_end) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(tta_ctx->row_store_it_.get_next_row(input_row))) {
        LOG_WARN("failed to get next row.", K(ret));
      } else { /*do nothing.*/
      }
    } else {
      LOG_WARN("failed to get next row.", K(ret));
    }
  } else { /*do nothing.*/
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(construct_row_from_interm_row(ctx, input_row, row))) {
    LOG_WARN("failed to construct row from interm row.", K(ret));
  } else {
    tta_ctx->is_started_ = true;
  }
  if (OB_FAIL(ret)) {
    if (OB_ITER_END != ret && OB_FAIL(destory_interm_results(ctx))) {
      LOG_WARN("failed to destory interm results.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTempTableAccess::get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ret = ObPhyOperator::get_next_row(ctx, row);
  return ret;
}

int ObTempTableAccess::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTempTableAccessInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("failed to create phy op input.", K(ret));
  } else { /*do nothing.*/
  }
  UNUSED(input);
  return ret;
}

int ObTempTableAccess::locate_next_interm_result(ObExecContext& ctx, bool& is_end) const
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  ObTempTableAccessCtx* tta_ctx = NULL;
  dtl::ObDTLIntermResultKey dtl_int_key;
  if (OB_ISNULL(tta_ctx = GET_PHY_OPERATOR_CTX(ObTempTableAccessCtx, ctx, get_id()))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (!is_distributed_) {
    if (!tta_ctx->is_started_) {
      dtl_int_key.channel_id_ = temp_table_id_;
      if (OB_FAIL(tta_ctx->locate_interm_result(dtl_int_key))) {
        LOG_WARN("failed to locate interm result.", K(ret));
      } else { /*do nothing.*/
      }
    } else {
      is_end = true;
    }
  } else if (OB_FAIL(tta_ctx->tta_input_->check_finish(is_end, index))) {
    LOG_WARN("failed to check finish.", K(ret));
  } else if (is_end) {
    /*do nothing.*/
  } else {
    dtl_int_key.channel_id_ = tta_ctx->tta_input_->interm_result_ids_.at(index);
    if (OB_FAIL(tta_ctx->locate_interm_result(dtl_int_key))) {
      LOG_WARN("failed to locate interm result.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    (ObTempTableAccess, ObNoChildrenPhyOperator), output_indexs_, temp_table_id_, is_distributed_, need_release_);

int64_t ObTempTableAccess::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("output_index", output_indexs_);
  J_OBJ_END();
  return pos;
}

}  // end namespace sql
}  // end namespace oceanbase
