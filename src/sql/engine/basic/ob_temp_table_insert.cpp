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

#include "sql/engine/basic/ob_temp_table_insert.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
namespace sql {

using dtl::ObDTLIntermResultInfo;

ObTempTableInsertInput::ObTempTableInsertInput()
    : deserialize_allocator_(NULL), unfinished_count_ptr_(0), interm_result_ids_()
{}

ObTempTableInsertInput::~ObTempTableInsertInput()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

void ObTempTableInsertInput::reset()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

ObPhyOperatorType ObTempTableInsertInput::get_phy_op_type() const
{
  return PHY_TEMP_TABLE_INSERT;
}

void ObTempTableInsertInput::set_deserialize_allocator(common::ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

int ObTempTableInsertInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return ret;
}

int ObTempTableInsertInput::assign_ids(common::ObIArray<uint64_t>& interm_res_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(interm_result_ids_.assign(interm_res_ids))) {
    LOG_WARN("failed to assign to interm result ids.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTempTableInsertInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTempTableInsertInput);
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

OB_DEF_DESERIALIZE(ObTempTableInsertInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObTempTableInsertInput);
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

OB_DEF_SERIALIZE_SIZE(ObTempTableInsertInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTempTableInsertInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, unfinished_count_ptr_);
  len += serialization::encoded_length_vi64(interm_result_ids_.count());
  for (int64_t i = 0; i < interm_result_ids_.count(); ++i) {
    len += serialization::encoded_length_vi64(interm_result_ids_.at(i));
  }
  return len;
}

void ObTempTableInsert::reset()
{
  ObSingleChildPhyOperator::reset();
}

void ObTempTableInsert::reuse()
{
  ObSingleChildPhyOperator::reuse();
}

int ObTempTableInsert::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSingleChildPhyOperator::rescan(exec_ctx))) {
    LOG_WARN("failed to rescan.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableInsert::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTempTableInsertInput* tti_input = NULL;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init op ctx.", K(ret));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("failed to handle operator context.", K(ret));
  } else if (OB_FAIL(prepare_interm_result(ctx))) {
    LOG_WARN("failed to prepare interm result.", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session.", K(ret));
  } else if (is_distributed_ && OB_ISNULL(tti_input = GET_PHY_OP_INPUT(ObTempTableInsertInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else {
    const common::ObNewRow* row = NULL;
    ObPhysicalPlanCtx* phy_plan_ctx = NULL;
    ObTempTableInsertCtx* tti_ctx = NULL;
    dtl::ObDTLIntermResultKey dtl_int_key;
    ObDTLIntermResultInfo chunk_row_store;
    uint64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
    int64_t chuck_cnt = is_distributed_ ? tti_input->interm_result_ids_.count() : 1;
    int64_t chuck_dump_size = 0;
    if (chuck_cnt > 0) {
      chuck_dump_size = lib::get_tenant_memory_limit(tenant_id) / (2 * chuck_cnt);
    }
    ObMemAttr mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA);
    if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().create_interm_result_info(mem_attr, chunk_row_store))) {
      LOG_WARN("failed to create row store.", K(ret));
    } else if (chunk_row_store.row_store_->init(chuck_dump_size,
                   ctx.get_my_session()->get_effective_tenant_id(),
                   ObCtxIds::WORK_AREA,
                   "SqlTempTableRowSt",
                   true,
                   ObChunkRowStore::WITHOUT_PROJECTOR,
                   sizeof(uint64_t))) {
      LOG_WARN("failed to init the chunk row store.", K(ret));
    } else if (OB_ISNULL(tti_ctx = GET_PHY_OPERATOR_CTX(ObTempTableInsertCtx, ctx, get_id()))) {
      LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
    } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_plan_ctx is NULL", K(ret));
    } else {
      dtl_int_key.channel_id_ = tti_ctx->interm_result_id_;
      dtl_int_key.start_time_ = oceanbase::common::ObTimeUtility::current_time();
      dtl_int_key.time_us_ = phy_plan_ctx->get_timeout_timestamp();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row from child.", K(ret), K(child_op_->get_type()));
          } else { /*do nothing.*/
          }
        } else if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is NULL", K(ret));
        } else if (OB_FAIL(chunk_row_store.row_store_->add_row(*row))) {
          LOG_WARN("failed to add row to chunk row store.", K(ret));
        } else { /*do nothing.*/
        }
      }
      if (OB_ITER_END != ret) {
        chunk_row_store.row_store_->reset();
      } else {
        ret = OB_SUCCESS;
        LOG_DEBUG("all rows are fetched");
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              dtl::ObDTLIntermResultManager::getInstance().insert_interm_result_info(dtl_int_key, chunk_row_store))) {
        LOG_WARN("failed to insert row store.", K(ret), K(dtl_int_key.channel_id_));
      } else { /*do nothing.*/
      }
    }
  }
  return ret;
}

int ObTempTableInsert::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  return ret;
}

int ObTempTableInsert::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  uint64_t interm_r_id = 0;
  if (OB_UNLIKELY(calc_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc exprs should be empty", K(ret), K(calc_exprs_.get_size()));
  } else if (OB_UNLIKELY(filter_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter exprs should be empty", K(ret), K(filter_exprs_.get_size()));
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTempTableInsertCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ctx is NULL", K(ret));
  } else {
    op_ctx->set_op_id(interm_r_id);
    if (OB_FAIL(init_cur_row(*op_ctx, false))) {
      LOG_WARN("fail to int cur row", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTempTableInsert::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  UNUSED(ctx);
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTempTableInsert::get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ret = ObPhyOperator::get_next_row(ctx, row);
  return ret;
}

int ObTempTableInsert::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTempTableInsertInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  } else { /*do nothing.*/
  }
  UNUSED(input);
  return ret;
}

int ObTempTableInsert::prepare_interm_result(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTempTableInsertInput* insert_input = NULL;
  ObTempTableInsertCtx* tti_ctx = NULL;
  if (OB_ISNULL(tti_ctx = GET_PHY_OPERATOR_CTX(ObTempTableInsertCtx, ctx, get_id()))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (!is_distributed_) {
    tti_ctx->set_iterm_result_id(temp_table_id_);
  } else if (OB_ISNULL(insert_input = GET_PHY_OP_INPUT(ObTempTableInsertInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else {
    uint64_t index = 0;
    uint64_t* count_ptr = reinterpret_cast<uint64_t*>(insert_input->unfinished_count_ptr_);
    if (NULL != count_ptr) {
      index = ATOMIC_AAF(count_ptr, -1);
      tti_ctx->set_iterm_result_id(insert_input->interm_result_ids_.at(index));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObTempTableInsert, ObSingleChildPhyOperator), temp_table_id_, is_distributed_);

int64_t ObTempTableInsert::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_OBJ_END();
  return pos;
}

}  // end namespace sql
}  // end namespace oceanbase
