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

// Copyright 2014 Alibaba Inc. All Rights Reserved.
// Author:
//

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_temp_table_access.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
namespace sql
{

ObTempTableAccessInput::ObTempTableAccessInput()
  : deserialize_allocator_(NULL),
    unfinished_count_ptr_(0),
    interm_result_ids_()
{
}

ObTempTableAccessInput::~ObTempTableAccessInput()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

void ObTempTableAccessInput::reset()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

ObPhyOperatorType ObTempTableAccessInput::get_phy_op_type() const
{
  return PHY_TEMP_TABLE_ACCESS;
}

void ObTempTableAccessInput::set_deserialize_allocator(common::ObIAllocator *allocator)
{
  deserialize_allocator_ = allocator;
}

int ObTempTableAccessInput::init(ObExecContext &ctx, ObTaskInfo &task_info, const ObPhyOperator &op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return ret;
}

int ObTempTableAccessInput::check_finish(bool &is_end, int64_t &index)
{
  int ret = OB_SUCCESS;
  uint64_t *count_ptr = reinterpret_cast<uint64_t *>(unfinished_count_ptr_);
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

int ObTempTableAccessInput::assign_ids(common::ObIArray<uint64_t> &interm_res_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(interm_result_ids_.assign(interm_res_ids))) {
    LOG_WARN("failed to assign to interm result ids.", K(ret));
  } else { /*do nothing.*/ }
  return ret;
}

OB_DEF_SERIALIZE(ObTempTableAccessInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTempTableAccessInput);
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
  LST_DO_CODE(OB_UNIS_DECODE, unfinished_count_ptr_);
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    interm_result_ids_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    } else { /*do nothing.*/ }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
      int64_t interm_result_id = 0;
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &interm_result_id))) {
        LOG_WARN("failed to decode vi64.", K(ret));
      } else if (OB_FAIL(interm_result_ids_.push_back(interm_result_id))) {
        LOG_WARN("failed to push back into interm result ids.", K(ret));
      } else { /*do nothing.*/ }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTempTableAccessInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTempTableAccessInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, unfinished_count_ptr_);
  len += serialization::encoded_length_vi64(interm_result_ids_.count());
  for (int64_t i = 0; i < interm_result_ids_.count(); ++i) {
    len += serialization::encoded_length_vi64(interm_result_ids_.at(i));
  }
  return len;
}

int ObTempTableAccess::ObTempTableAccessCtx::rescan(const ObTempTableAccess *access_op, ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  int64_t result_id = 0;
  cur_idx_ = 0;
  is_started_ = false;
  can_rescan_ = true;
  if (!is_distributed_) {
    interm_result_ids_.reuse();
    if (OB_FAIL(get_local_interm_result_id(ctx, result_id))) {
      LOG_WARN("failed to get local result id", K(ret));
    } else if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
      LOG_WARN("failed to push back result id", K(ret));
    }
  } else {
    int64_t index = 0;
    bool is_end = false;
    if (OB_ISNULL(tta_input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null input", K(ret));
    }
    while (!is_end && OB_SUCC(ret)) {
      if (OB_FAIL(access_op->check_status(ctx))) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(tta_input_->check_finish(is_end, index))) {
        LOG_WARN("failed to check finish.", K(ret));
      } else if (!is_end) {
        result_id = tta_input_->interm_result_ids_.at(index);
        if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
          LOG_WARN("failed to push back result id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTempTableAccess::ObTempTableAccessCtx::get_next_row(ObExecContext &ctx, common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  if (!is_started_ && OB_FAIL(locate_next_interm_result(ctx, is_end))) {
    LOG_WARN("failed to locate next interm result.", K(ret));
  } else {
    is_started_ = true;
  }
  while (OB_SUCC(ret) && !is_end) {
    if (OB_FAIL(row_store_it_.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (OB_FAIL(locate_next_interm_result(ctx, is_end))) {
        LOG_WARN("failed to locate next interm result.", K(ret));
      }
    } else {
      break;
    }
  }
  if (OB_SUCC(ret) && is_end) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTempTableAccess::ObTempTableAccessCtx::locate_next_interm_result(ObExecContext &ctx, bool &is_end)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  int64_t result_id = 0;
  is_end = false;
  if (can_rescan_) {
    //rescan从context的缓存中拿result id
    if (cur_idx_ >= interm_result_ids_.count()) {
      is_end = true;
    } else {
      result_id = interm_result_ids_.at(cur_idx_);
      ++cur_idx_;
      if (OB_FAIL(locate_interm_result(result_id))) {
        LOG_WARN("failed to get interm reuslt", K(ret));
      }
    }
  } else if (!is_distributed_) {
    //本地结果集从exec ctx中拿result id，并且缓存下来
    if (is_started_) {
      is_end = true;
    } else if (OB_FAIL(get_local_interm_result_id(ctx, result_id))) {
      LOG_WARN("failed to get local result id", K(ret));
    } else if (OB_FAIL(locate_interm_result(result_id))) {
      LOG_WARN("failed to get interm reuslt", K(ret));
    } else if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
      LOG_WARN("failed to push back result id", K(ret));
    }
  } else {
    //分布式结果集从任务池抢占result id，并且缓存下来
    int64_t index = 0;
    if (OB_ISNULL(tta_input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null input", K(ret));
    } else if (OB_FAIL(tta_input_->check_finish(is_end, index))) {
      LOG_WARN("failed to check finish.", K(ret));
    } else if (!is_end) {
      result_id = tta_input_->interm_result_ids_.at(index);
      if (OB_FAIL(locate_interm_result(result_id))) {
        LOG_WARN("failed to get interm reuslt", K(ret));
      } else if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
        LOG_WARN("failed to push back result id", K(ret));
      }
    }
  }
  return ret;
}

int ObTempTableAccess::ObTempTableAccessCtx::get_local_interm_result_id(ObExecContext &ctx, int64_t &result_id)
{
  int ret = OB_SUCCESS;
  bool get_result_id = false;
  for (int64_t i = 0; OB_SUCC(ret) && !get_result_id && i < ctx.get_temp_table_ctx().count(); ++i) {
    ObSqlTempTableCtx &temp_table_ctx = ctx.get_temp_table_ctx().at(i);
    if (temp_table_ctx.temp_table_id_ != temp_table_id_) {
      /* do nothing */
    } else if (OB_UNLIKELY(!temp_table_ctx.is_local_interm_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local interm result ids is empty.", K(ret));
    } else if (temp_table_ctx.interm_result_infos_.count() != 1 ||
               temp_table_ctx.interm_result_infos_.at(0).interm_result_ids_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local interm result ids expect one.", K(ret));
    } else {
      // local_interm_result_ids_ count is 1 now.
      get_result_id = true;
      result_id = temp_table_ctx.interm_result_infos_.at(0).interm_result_ids_.at(0);
    }
  }
  if (OB_SUCC(ret) && !get_result_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get temp table interm result id ", K(ret));
  }
  return ret;
}

int ObTempTableAccess::ObTempTableAccessCtx::locate_interm_result(int64_t result_id)
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultKey dtl_int_key;
  dtl::ObDTLIntermResultInfo *result_info = NULL;
  // The current operation of obtaining intermediate results and
  // the operation of the background thread of dumping intermediate results
  // are mutually exclusive
  dtl_int_key.channel_id_ = result_id;
  row_store_it_.reset();
  if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().atomic_get_interm_result_info(
       dtl_int_key, result_info_guard_))) {
     LOG_WARN("failed to create row store.", K(ret));
  } else if (FALSE_IT(result_info = result_info_guard_.result_info_)) {
  // After getting the intermediate result, need to judge whether the result is readable.
  } else if (OB_SUCCESS != result_info->ret_) {
    ret = result_info->ret_;
    LOG_WARN("the interm result info meet a error", K(ret));
  } else if (!result_info->is_store_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no row store in internal result", K(ret));
  } else if (OB_FAIL(result_info->row_store_->begin(row_store_it_))) {
    LOG_WARN("failed to begin chunk row store.", K(ret));
  } else { /*do nothing.*/ }
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

int ObTempTableAccess::rescan(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObTempTableAccessCtx *tta_ctx = NULL;
  if (OB_ISNULL(tta_ctx = GET_PHY_OPERATOR_CTX(ObTempTableAccessCtx,
                                              exec_ctx,
                                              get_id()))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_FAIL(tta_ctx->rescan(this, exec_ctx))) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObTempTableAccess::inner_open(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init op ctx.", K(ret));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("failed to handle operator context.", K(ret));
  } else { /* do nothing. */ }
  return ret;
}

int ObTempTableAccess::inner_close(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  return ret;
}

int ObTempTableAccess::init_op_ctx(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  ObTempTableAccessCtx *op_ctx = NULL;
  ObTempTableAccessInput *op_input = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTempTableAccessCtx,
                                      ctx,
                                      get_id(),
                                      get_type(),
                                      op_ctx))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("fail to int cur row", K(ret));
  } else if (OB_ISNULL(op_input = GET_PHY_OP_INPUT(ObTempTableAccessInput,
                                                   ctx,
                                                   get_id()))) {
    if (OB_FAIL(create_operator_input(ctx))) {
      LOG_WARN("fail to create operator input", K(ret));
    } else { /*do nothing.*/ }
  } else { /*do nothing.*/ }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(op_input = GET_PHY_OP_INPUT(ObTempTableAccessInput,
                                                   ctx,
                                                   get_id()))) {
    LOG_WARN("failed to get op input.", K(ret));
  } else {
    op_ctx->tta_input_ = op_input;
    op_ctx->is_distributed_ = is_distributed_;
    op_ctx->temp_table_id_ = temp_table_id_;
  }
  return ret;
}

int ObTempTableAccess::inner_get_next_row(ObExecContext &ctx,
                                          const common::ObNewRow *&row) const
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  common::ObNewRow *input_row = NULL;
  ObTempTableAccessCtx *tta_ctx = NULL;
  if (OB_ISNULL(tta_ctx = GET_PHY_OPERATOR_CTX(ObTempTableAccessCtx,
                                               ctx,
                                               get_id()))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_FAIL(tta_ctx->get_next_row(ctx, input_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row.", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && 0 == (++tta_ctx->iterated_rows_ % ObTempTableAccessCtx::CHECK_STATUS_ROWS_INTERVAL)
      && OB_FAIL(ctx.check_status())) {
    LOG_WARN("check status failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_indexs_.count(); ++i) {
    int64_t col_idx = output_indexs_.at(i);
    if (OB_UNLIKELY(i >= tta_ctx->get_cur_row().count_) ||
        OB_UNLIKELY(col_idx >= input_row->get_count())) {
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

int ObTempTableAccess::create_operator_input(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput *input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTempTableAccessInput,
                                  ctx,
                                  get_id(),
                                  get_type(),
                                  input))) {
    LOG_WARN("failed to create phy op input.", K(ret));
  } else { /*do nothing.*/ }
  UNUSED(input);
  return ret;
}

OB_SERIALIZE_MEMBER((ObTempTableAccess, ObNoChildrenPhyOperator),
                    output_indexs_,
                    temp_table_id_,
                    is_distributed_);

int64_t ObTempTableAccess::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("output_index", output_indexs_);
  J_OBJ_END();
  return pos;
}

} // end namespace sql
} // end namespace oceanbase
