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

#include "ob_temp_table_insert_op.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {

#define USE_MULTI_GET_ARRAY_BINDING 1

ObTempTableInsertOpInput::ObTempTableInsertOpInput(ObExecContext& ctx, const ObOpSpec& spec)
    : ObOpInput(ctx, spec), deserialize_allocator_(NULL), unfinished_count_ptr_(0), interm_result_ids_()
{}

ObTempTableInsertOpInput::~ObTempTableInsertOpInput()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

void ObTempTableInsertOpInput::reset()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

ObPhyOperatorType ObTempTableInsertOpInput::get_phy_op_type() const
{
  return PHY_TEMP_TABLE_INSERT;
}

void ObTempTableInsertOpInput::set_deserialize_allocator(common::ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

int ObTempTableInsertOpInput::init(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  UNUSED(task_info);
  return ret;
}

int ObTempTableInsertOpInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return ret;
}

OB_DEF_SERIALIZE(ObTempTableInsertOpInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTempTableInsertOpInput);
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

OB_DEF_DESERIALIZE(ObTempTableInsertOpInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObTempTableInsertOpInput);
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

OB_DEF_SERIALIZE_SIZE(ObTempTableInsertOpInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTempTableInsertOpInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, unfinished_count_ptr_);
  len += serialization::encoded_length_vi64(interm_result_ids_.count());
  for (int64_t i = 0; i < interm_result_ids_.count(); ++i) {
    len += serialization::encoded_length_vi64(interm_result_ids_.at(i));
  }
  return len;
}

DEF_TO_STRING(ObTempTableInsertOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K(temp_table_id_), K(is_distributed_));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_INHERIT(ObTempTableInsertOpSpec, ObOpSpec, temp_table_id_, is_distributed_);

int ObTempTableInsertOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("failed to rescan.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx session is null");
  } else if (OB_FAIL(prepare_scan_param())) {
    LOG_WARN("failed to prepare scan param.", K(ret));
  } else {
    dtl::ObDTLIntermResultKey dtl_int_key;
    dtl::ObDTLIntermResultInfo chunk_row_store;
    ObPhysicalPlanCtx* phy_plan_ctx = NULL;
    uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    int64_t chuck_cnt = MY_SPEC.is_distributed_ ? MY_INPUT.interm_result_ids_.count() : 1;
    int64_t chuck_dump_size = 0;
    if (chuck_cnt > 0) {
      chuck_dump_size = lib::get_tenant_memory_limit(tenant_id) / (2 * chuck_cnt);
    }
    ObMemAttr mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA);
    chunk_row_store.is_datum_ = true;
    if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().create_interm_result_info(mem_attr, chunk_row_store))) {
      LOG_WARN("failed to create row store.", K(ret));
    } else if (OB_ISNULL(chunk_row_store.datum_store_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum store is null");
    } else if (chunk_row_store.datum_store_->init(chuck_dump_size,
                   ctx_.get_my_session()->get_effective_tenant_id(),
                   ObCtxIds::WORK_AREA,
                   "SqlTempTableRowSt",
                   true,
                   sizeof(uint64_t))) {
      LOG_WARN("failed to init the chunk row store.", K(ret));
    } else if (OB_ISNULL(child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child operator is null");
    } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx is null.", K(ret));
    } else {
      dtl_int_key.channel_id_ = interm_result_id_;
      dtl_int_key.start_time_ = oceanbase::common::ObTimeUtility::current_time();
      dtl_int_key.time_us_ = phy_plan_ctx->get_timeout_timestamp();
      while (OB_SUCC(ret)) {
        clear_evaluated_flag();
        if (OB_FAIL(child_->get_next_row())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row from child.", K(ret));
          } else { /*do nothing.*/
          }
        } else if (OB_FAIL(chunk_row_store.datum_store_->add_row(child_->get_spec().output_, &eval_ctx_))) {
          LOG_WARN("failed to add row to chunk row store.", K(ret));
        } else { /*do nothing.*/
        }
      }
      if (OB_ITER_END != ret) {
        chunk_row_store.datum_store_->reset();
      } else {
        ret = OB_SUCCESS;
        LOG_DEBUG("all rows are fetched");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().insert_interm_result_info(
                   dtl_int_key, chunk_row_store))) {
      LOG_WARN("failed to insert row store.", K(ret), K(dtl_int_key));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTempTableInsertOp::inner_close()
{
  int ret = OB_SUCCESS;
  interm_result_id_ = 0;
  return ret;
}

void ObTempTableInsertOp::destroy()
{
  ObOperator::destroy();
}

int ObTempTableInsertOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed.", K(ret));
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTempTableInsertOp::prepare_scan_param()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_distributed_) {
    interm_result_id_ = MY_SPEC.temp_table_id_;
  } else {
    uint64_t index = 0;
    uint64_t* count_ptr = reinterpret_cast<uint64_t*>(MY_INPUT.unfinished_count_ptr_);
    if (NULL != count_ptr) {
      index = ATOMIC_AAF(count_ptr, -1);
      interm_result_id_ = MY_INPUT.interm_result_ids_.at(index);
    } else { /*do nothing.*/
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
