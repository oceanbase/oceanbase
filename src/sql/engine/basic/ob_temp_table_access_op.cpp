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

#include "ob_temp_table_access_op.h"
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

ObTempTableAccessOpInput::ObTempTableAccessOpInput(ObExecContext& ctx, const ObOpSpec& spec)
    : ObOpInput(ctx, spec),
      deserialize_allocator_(NULL),
      closed_count_(0),
      unfinished_count_ptr_(0),
      interm_result_ids_()
{}

ObTempTableAccessOpInput::~ObTempTableAccessOpInput()
{
  deserialize_allocator_ = NULL;
  closed_count_ = 0;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

void ObTempTableAccessOpInput::reset()
{
  deserialize_allocator_ = NULL;
  closed_count_ = 0;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

ObPhyOperatorType ObTempTableAccessOpInput::get_phy_op_type() const
{
  return PHY_TEMP_TABLE_ACCESS;
}

void ObTempTableAccessOpInput::set_deserialize_allocator(common::ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

int ObTempTableAccessOpInput::init(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  UNUSED(task_info);
  return ret;
}

int ObTempTableAccessOpInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return ret;
}

int ObTempTableAccessOpInput::check_finish(bool& is_end, int64_t& index)
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

int ObTempTableAccessOpInput::check_closed_finish(bool& is_end)
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

OB_DEF_SERIALIZE(ObTempTableAccessOpInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTempTableAccessOpInput);
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

OB_DEF_DESERIALIZE(ObTempTableAccessOpInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObTempTableAccessOpInput);
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

OB_DEF_SERIALIZE_SIZE(ObTempTableAccessOpInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTempTableAccessOpInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, closed_count_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, unfinished_count_ptr_);
  len += serialization::encoded_length_vi64(interm_result_ids_.count());
  for (int64_t i = 0; i < interm_result_ids_.count(); ++i) {
    len += serialization::encoded_length_vi64(interm_result_ids_.at(i));
  }
  return len;
}

DEF_TO_STRING(ObTempTableAccessOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K(temp_table_id_), K(is_distributed_), K(output_indexs_), K(access_exprs_));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_INHERIT(
    ObTempTableAccessOpSpec, ObOpSpec, output_indexs_, temp_table_id_, is_distributed_, need_release_, access_exprs_);

int ObTempTableAccessOp::rescan()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObTempTableAccessOp::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObTempTableAccessOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(destory_interm_results())) {
    LOG_WARN("failed to destroy interm results.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableAccessOp::destory_interm_results()
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  if (MY_SPEC.is_distributed_) {
    if (!MY_SPEC.need_release_) {
      // do nothing.
    } else if (OB_FAIL(MY_INPUT.check_closed_finish(is_end))) {
      LOG_WARN("failed to check closed finish.", K(ret));
    } else if (is_end) {
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_INPUT.interm_result_ids_.count(); ++i) {
        dtl::ObDTLIntermResultKey dtl_int_key;
        dtl_int_key.channel_id_ = MY_INPUT.interm_result_ids_.at(i);
        if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().erase_interm_result_info(dtl_int_key))) {
          LOG_WARN("failed to erase interm result info in manager.", K(ret));
        } else { /*do nothing.*/
        }
      }
    }
  } else if (MY_SPEC.need_release_) {
    dtl::ObDTLIntermResultKey dtl_int_key;
    dtl_int_key.channel_id_ = MY_SPEC.temp_table_id_;
    if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().erase_interm_result_info(dtl_int_key))) {
      LOG_WARN("failed to erase interm result info in manager.", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }
  return ret;
}

void ObTempTableAccessOp::destroy()
{
  ObOperator::destroy();
}

int ObTempTableAccessOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  bool is_end = false;
  const ObChunkDatumStore::StoredRow* tmp_sr = NULL;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (!is_started_ && OB_FAIL(locate_next_interm_result(is_end))) {
    LOG_WARN("failed to locate next interm result.", K(ret));
  } else if (!is_started_ && is_end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(datum_store_it_.get_next_row(tmp_sr))) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(locate_next_interm_result(is_end))) {
        LOG_WARN("failed to locate next iterm result.", K(ret));
      } else if (is_end && ctx_.is_restart_plan()) {
        ret = OB_ITER_END;
        datum_store_it_.reset();
      } else if (OB_FAIL(datum_store_it_.get_next_row(tmp_sr))) {
        LOG_WARN("failed to get next row.", K(ret));
      } else { /*do nothing.*/
      }
    } else {
      LOG_WARN("failed to get next row.", K(ret));
    }
  } else { /*do nothing.*/
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tmp_sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("temp src row is null.", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.output_indexs_.count() != MY_SPEC.access_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(MY_SPEC.output_indexs_.count()), K(MY_SPEC.access_exprs_.count()));
  } else {
    is_started_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_indexs_.count(); ++i) {
      int64_t col_idx = MY_SPEC.output_indexs_.at(i);
      if (OB_UNLIKELY(col_idx >= tmp_sr->cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(col_idx), K(tmp_sr->cnt_));
      } else if (OB_ISNULL(MY_SPEC.access_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("spec output is null.", K(ret));
      } else {
        MY_SPEC.access_exprs_.at(i)->locate_expr_datum(eval_ctx_) = tmp_sr->cells()[col_idx];
        MY_SPEC.access_exprs_.at(i)->get_eval_info(eval_ctx_).evaluated_ = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_ITER_END != ret && OB_FAIL(destory_interm_results())) {
      LOG_WARN("failed to destory interm results.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTempTableAccessOp::locate_next_interm_result(bool& is_end)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  dtl::ObDTLIntermResultKey dtl_int_key;
  if ((!MY_SPEC.is_distributed_)) {
    if (!is_started_) {
      dtl_int_key.channel_id_ = MY_SPEC.temp_table_id_;
      if (OB_FAIL(locate_interm_result(dtl_int_key))) {
        LOG_WARN("failed to locate interm result.", K(ret));
      } else { /*do nothing.*/
      }
    } else {
      is_end = true;
    }
  } else if (OB_FAIL(MY_INPUT.check_finish(is_end, index))) {
    LOG_WARN("failed to check finish.", K(ret));
  } else if (is_end) {
    /*do nothing.*/
  } else {
    dtl_int_key.channel_id_ = MY_INPUT.interm_result_ids_.at(index);
    if (OB_FAIL(locate_interm_result(dtl_int_key))) {
      LOG_WARN("failed to locate interm result.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTempTableAccessOp::locate_interm_result(dtl::ObDTLIntermResultKey& dtl_int_key)
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultInfo result_info;
  if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().get_interm_result_info(dtl_int_key, result_info))) {
    LOG_WARN("failed to create row store.", K(ret));
  } else {
    datum_store_ = result_info.datum_store_;
    if (OB_ISNULL(datum_store_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum store is null.", K(ret));
    } else if (OB_FAIL(datum_store_->begin(datum_store_it_))) {
      LOG_WARN("failed to begin chunk row store.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTempTableAccessOp::prepare_scan_param()
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultKey dtl_int_key;
  dtl::ObDTLIntermResultInfo result_info;
  if (!MY_SPEC.is_distributed_) {
    dtl_int_key.channel_id_ = MY_SPEC.temp_table_id_;
  } else {
    uint64_t index = 0;
    uint64_t* count_ptr = reinterpret_cast<uint64_t*>(MY_INPUT.unfinished_count_ptr_);
    if (MY_INPUT.interm_result_ids_.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("interm result ids count is zero.", K(ret));
    } else if (NULL != count_ptr) {
      index = ATOMIC_AAF(count_ptr, -1);
      dtl_int_key.channel_id_ = MY_INPUT.interm_result_ids_.at(index);
    } else { /*do nothing.*/
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dtl::ObDTLIntermResultManager::getInstance().get_interm_result_info(dtl_int_key, result_info))) {
    LOG_WARN("failed to create row store.", K(ret));
  } else if (FALSE_IT(datum_store_ = result_info.datum_store_)) {
  } else if (OB_ISNULL(datum_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum store is null.", K(ret));
  } else if (OB_FAIL(datum_store_->begin(datum_store_it_))) {
    LOG_WARN("failed to begin chunk row store.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
