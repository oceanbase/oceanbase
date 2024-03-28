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

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql
{

#define USE_MULTI_GET_ARRAY_BINDING 1

ObTempTableAccessOpInput::ObTempTableAccessOpInput(ObExecContext &ctx, const ObOpSpec &spec)
  : ObOpInput(ctx, spec),
    deserialize_allocator_(NULL),
    unfinished_count_ptr_(0),
    interm_result_ids_()
{
}

ObTempTableAccessOpInput::~ObTempTableAccessOpInput()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

void ObTempTableAccessOpInput::reset()
{
  deserialize_allocator_ = NULL;
  unfinished_count_ptr_ = 0;
  interm_result_ids_.reset();
}

ObPhyOperatorType ObTempTableAccessOpInput::get_phy_op_type() const
{
  return PHY_TEMP_TABLE_ACCESS;
}

void ObTempTableAccessOpInput::set_deserialize_allocator(common::ObIAllocator *allocator)
{
  deserialize_allocator_ = allocator;
}

int ObTempTableAccessOpInput::init(ObTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  UNUSED(task_info);
  return ret;
}

int ObTempTableAccessOpInput::check_finish(bool &is_end, int64_t &index)
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

OB_DEF_SERIALIZE(ObTempTableAccessOpInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTempTableAccessOpInput);
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

OB_DEF_SERIALIZE_SIZE(ObTempTableAccessOpInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTempTableAccessOpInput);
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
  J_KV(K(temp_table_id_),
       K(is_distributed_),
       K(output_indexs_),
       K(access_exprs_));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_INHERIT(ObTempTableAccessOpSpec, ObOpSpec,
                            output_indexs_,
                            temp_table_id_,
                            is_distributed_,
                            access_exprs_);

int ObTempTableAccessOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  int64_t result_id = 0;
  cur_idx_ = 0;
  is_started_ = false;
  can_rescan_ = true;
  if (!MY_SPEC.is_distributed_) {
    interm_result_ids_.reuse();
    if (OB_FAIL(get_local_interm_result_id(result_id))) {
      LOG_WARN("failed to get local result id", K(ret));
    } else if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
      LOG_WARN("failed to push back result id", K(ret));
    }
  } else {
    int64_t index = 0;
    bool is_end = false;
    while (!is_end && OB_SUCC(ret)) {
      if (OB_FAIL(check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else
      if (OB_FAIL(MY_INPUT.check_finish(is_end, index))) {
        LOG_WARN("failed to check finish.", K(ret));
      } else if (!is_end) {
        result_id = MY_INPUT.interm_result_ids_.at(index);
        if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
          LOG_WARN("failed to push back result id", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObTempTableAccessOp::inner_open()
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = sizeof(ObChunkDatumStore::StoredRow *) * MY_SPEC.max_batch_size_;
  if (OB_UNLIKELY(MY_SPEC.output_indexs_.count() != MY_SPEC.access_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(MY_SPEC.output_indexs_.count()),
                                     K(MY_SPEC.access_exprs_.count()));
  } else if (alloc_size > 0
             && OB_ISNULL(stored_rows_ = static_cast<const ObChunkDatumStore::StoredRow **>
                         (ctx_.get_allocator().alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(alloc_size));
  }
  return ret;
}

int ObTempTableAccessOp::inner_close()
{
  int ret = OB_SUCCESS;
  datum_store_it_.reset();
  return ret;
}

void ObTempTableAccessOp::destroy()
{
  result_info_guard_.reset();
  ObOperator::destroy();
  interm_result_ids_.reset();
}

int ObTempTableAccessOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  bool is_end = false;
  const ObChunkDatumStore::StoredRow *tmp_sr = NULL;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (!is_started_ && OB_FAIL(locate_next_interm_result(is_end))) {
    LOG_WARN("failed to locate next interm result.", K(ret));
  }
  while (OB_SUCC(ret) && !is_end) {
    if (OB_FAIL(datum_store_it_.get_next_row(tmp_sr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (OB_FAIL(locate_next_interm_result(is_end))) {
        LOG_WARN("failed to locate next interm result.", K(ret));
      }
    } else {
      break;
    }
  }
  if (OB_SUCC(ret) && is_end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tmp_sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("temp src row is null.", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.output_indexs_.count() != MY_SPEC.access_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(MY_SPEC.output_indexs_.count()),
                                     K(MY_SPEC.access_exprs_.count()));
  } else {
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
        MY_SPEC.access_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObTempTableAccessOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  clear_datum_eval_flag();
  bool is_end = false;
  int64_t read_rows = -1;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (!is_started_ && OB_FAIL(locate_next_interm_result(is_end))) {
    LOG_WARN("failed to locate next interm result.", K(ret));
  }
  while (OB_SUCC(ret) && !is_end) {
    if (OB_FAIL(datum_store_it_.get_next_batch(stored_rows_,
            std::min(MY_SPEC.max_batch_size_, max_row_cnt), read_rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (OB_FAIL(locate_next_interm_result(is_end))) {
        LOG_WARN("failed to locate next interm result.", K(ret));
      }
    } else {
      for (auto i = 0; OB_SUCC(ret) && i < MY_SPEC.output_indexs_.count(); ++i) {
        // performance critial: no nullptr check for access_exprs_.at(i)
        int64_t col_idx = MY_SPEC.output_indexs_.at(i);
        ObExpr *expr = MY_SPEC.access_exprs_.at(i);
        ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
        if (expr->is_batch_result()) {
          for (auto j = 0; j < read_rows; j++) {
            datums[j] = stored_rows_[j]->cells()[col_idx];
          }
        } else {
          datums[0] = stored_rows_[0]->cells()[col_idx];
        }
      }
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_end) {
      brs_.size_ = 0;
      brs_.end_ = true;
    } else {
      brs_.size_ = read_rows;
    }
  }

  return ret;
}

int ObTempTableAccessOp::locate_next_interm_result(bool &is_end)
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
  } else if (!MY_SPEC.is_distributed_) {
    //本地结果集从exec ctx中拿result id，并且缓存下来
    if (is_started_) {
      is_end = true;
    } else if (OB_FAIL(get_local_interm_result_id(result_id))) {
      LOG_WARN("failed to get local result id", K(ret));
    } else if (OB_FAIL(locate_interm_result(result_id))) {
      LOG_WARN("failed to get interm reuslt", K(ret));
    } else if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
      LOG_WARN("failed to push back result id", K(ret));
    }
  } else {
    //分布式结果集从任务池抢占result id，并且缓存下来
    int64_t index = 0;
    if (OB_FAIL(MY_INPUT.check_finish(is_end, index))) {
      LOG_WARN("failed to check finish.", K(ret));
    } else if (!is_end) {
      result_id = MY_INPUT.interm_result_ids_.at(index);
      if (OB_FAIL(locate_interm_result(result_id))) {
        LOG_WARN("failed to get interm reuslt", K(ret));
      } else if (OB_FAIL(interm_result_ids_.push_back(result_id))) {
        LOG_WARN("failed to push back result id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !is_started_) {
    is_started_ = true;
  }
  return ret;
}

int ObTempTableAccessOp::locate_interm_result(int64_t result_id)
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultKey dtl_int_key;
  dtl::ObDTLIntermResultInfo *result_info = NULL;
  dtl_int_key.channel_id_ = result_id;
  datum_store_it_.reset();
  // The current operation of obtaining intermediate results and
  // the operation of the background thread of dumping intermediate results
  // are mutually exclusive
  if (OB_FAIL(MTL(dtl::ObDTLIntermResultManager*)->atomic_get_interm_result_info(
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
  } else if (OB_ISNULL(result_info->datum_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum store is null.", K(ret));
  } else if (OB_FAIL(result_info->datum_store_->begin(datum_store_it_))) {
    LOG_WARN("failed to begin chunk row store.", K(ret));
  } else { /*do nothing.*/ }
  return ret;
}

int ObTempTableAccessOp::get_local_interm_result_id(int64_t &result_id)
{
  int ret = OB_SUCCESS;
  bool get_result_id = false;
  ObIArray<ObSqlTempTableCtx> &temp_table_ctx = get_exec_ctx().get_temp_table_ctx();
  for (int64_t i = 0; OB_SUCC(ret) && !get_result_id && i < temp_table_ctx.count(); ++i) {
    ObSqlTempTableCtx &temp_ctx = temp_table_ctx.at(i);
    if (temp_ctx.temp_table_id_ != MY_SPEC.temp_table_id_) {
      /* do nothing */
    } else if (OB_UNLIKELY(!temp_ctx.is_local_interm_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local interm result ids is empty.", K(ret));
    } else if (temp_ctx.interm_result_infos_.count() != 1 ||
               temp_ctx.interm_result_infos_.at(0).interm_result_ids_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local interm result ids expect one.", K(ret));
    } else {
      // local_interm_result_ids_ count is 1 now.
      get_result_id = true;
      result_id = temp_ctx.interm_result_infos_.at(0).interm_result_ids_.at(0);
    }
  }
  if (OB_SUCC(ret) && !get_result_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get temp table interm result id ", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

