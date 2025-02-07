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

#include "ob_temp_table_insert_vec_op.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "share/detect/ob_detect_manager_utils.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql
{

#define USE_MULTI_GET_ARRAY_BINDING 1

using dtl::ObDTLIntermResultInfo;
static const uint64_t TEMP_TABLE_VEC_PAGE_SIZE = 100000;

OB_SERIALIZE_MEMBER(ObTempTableInsertVecOpInput, qc_id_, dfo_id_, sqc_id_);

DEF_TO_STRING(ObTempTableInsertVecOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K(temp_table_id_),
       K(is_distributed_));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_INHERIT(ObTempTableInsertVecOpSpec, ObOpSpec,
                            temp_table_id_,
                            is_distributed_);

int ObTempTableInsertVecOp::inner_rescan()
{
  // Rescan is currently not supported.
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}

int ObTempTableInsertVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx session is null");
  } else if (OB_ISNULL(mem_context_)) {
    lib::ContextParam param;
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    param.set_mem_attr(tenant_id, "TempTableInsert", ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(
        &mem_context_->get_malloc_allocator(),
        tenant_id,
        TEMP_TABLE_VEC_PAGE_SIZE * MY_SPEC.width_,
        MY_SPEC.type_,
        MY_SPEC.id_,
        &ctx_))) {
      LOG_WARN("failed to init sql memory manager processor", K(ret));
    }
  }
  return ret;
}

int ObTempTableInsertVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_distributed_) {
    //do nothing
  } else if (OB_ISNULL(task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null task", K(ret));
  } else if (!task_->interm_result_ids_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result ids should be empty", K(ret));
  } else if (OB_FAIL(task_->interm_result_ids_.assign(interm_result_ids_))) {
    LOG_WARN("failed to assign result ids", K(ret));
  } else {
    task_->temp_table_id_ = MY_SPEC.temp_table_id_;
  }
  int temp_ret = ret;
  if (OB_FAIL(clear_all_interm_res_info())) {
    // overwrite ret, has temp_ret
    LOG_WARN("failed to clear row store", K(ret));
  }
  ret = temp_ret;
  sql_mem_processor_.unregister_profile();
  return ret;
}

void ObTempTableInsertVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  destroy_mem_context();
  ObOperator::destroy();
}

int ObTempTableInsertVecOp::inner_get_next_row()
{
  int ret = common::OB_NOT_IMPLEMENT;
  LOG_WARN("Vec Operator call inner_get_next_row()", K(ret));
  return ret;
}

int ObTempTableInsertVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (init_temp_table_) {
    if (OB_FAIL(do_get_next_vector(max_row_cnt))) {
      LOG_WARN("failed to insert chunk row to temp table in vectorized mode", K(ret));
    } else if (OB_FAIL(insert_interm_result_info())) {
      LOG_WARN("failed to insert chunk row store to dtl", K(ret));
    } else {
      init_temp_table_ = false;
    }
  }
  brs_.end_ = true;
  brs_.size_ = 0;
  return ret;
}

int ObTempTableInsertVecOp::do_get_next_vector(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultInfo *result_info = NULL;
  const ObBatchRows * child_brs = nullptr;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
      LOG_WARN("fail to get next row from child.", K(ret));
    } else if (OB_FAIL(add_rows_to_temp_table(result_info, child_brs))) {
      LOG_WARN("failed to add row to chunk row store.", K(ret));
    }
    if (child_brs->end_) {
      break;
    }
    LOG_DEBUG("finish processing batch result", KPC(child_brs));
  }
  return ret;
}

int ObTempTableInsertVecOp::add_rows_to_temp_table(
    ObDTLIntermResultInfo *&interm_res_info, const ObBatchRows *brs)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = -1;
  if (NULL == interm_res_info && OB_FAIL(create_interm_result_info(interm_res_info))) {
    LOG_WARN("failed to init chunk row store", K(ret));
  } else if (OB_FAIL(process_dump(*interm_res_info))) {
    LOG_WARN("failed to process dump", K(ret));
  } else if (OB_FAIL(interm_res_info->get_column_store()->add_batch(
                 MY_SPEC.get_child()->output_, eval_ctx_, *brs, read_rows))) {
    LOG_WARN("failed to add rows to chunk row store.", K(ret), KPC(brs));
  } else if (!MY_SPEC.is_distributed_) {
    //do nothing
  } else if (interm_res_info->get_column_store()->get_row_cnt() <
             TEMP_TABLE_VEC_PAGE_SIZE) {
    //do nothing
  } else {
    interm_res_info = NULL;
  }
  return ret;
}

int ObTempTableInsertVecOp::create_interm_result_info(ObDTLIntermResultInfo *&interm_res_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session.", K(ret));
  } else {
    uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    ObMemAttr mem_attr(tenant_id, "TempTableInsert", ObCtxIds::WORK_AREA);
    dtl::ObDTLIntermResultInfoGuard result_info_guard;
    if (OB_FAIL(MTL(dtl::ObDTLIntermResultManager *)->create_interm_result_info(mem_attr,
                                          result_info_guard,
                                          dtl::ObDTLIntermResultMonitorInfo(
                                            MY_INPUT.qc_id_,
                                            MY_INPUT.dfo_id_,
                                            MY_INPUT.sqc_id_),
                                          ObDTLIntermResultInfo::StoreType::COLUMN))) {
      LOG_WARN("failed to create row store.", K(ret));
    } else if (FALSE_IT(interm_res_info = result_info_guard.result_info_)) {
    } else if (OB_FAIL(interm_res_info->get_column_store()->init(MY_SPEC.get_child()->output_,
                                                 MY_SPEC.max_batch_size_,
                                                 mem_attr,
                                                 0 /*mem_limit*/,
                                                 true /*enable_dump*/,
                                                 false /*reuse_vector_array*/,
                                                 MY_SPEC.compress_type_))) {
      LOG_WARN("failed to init the chunk row store.", K(ret));
    } else if (OB_FAIL(all_interm_res_info_.push_back(interm_res_info))) {
      LOG_WARN("failed to push back row store", K(ret));
    } else {
      interm_res_info->get_column_store()->set_callback(&sql_mem_processor_);
      interm_res_info->get_column_store()->set_dir_id(sql_mem_processor_.get_dir_id());
      interm_res_info->get_column_store()->set_io_event_observer(&io_event_observer_);

      // inc ref count because refered by all_interm_res_info_. decrease when remove from all_interm_res_info_.
      dtl::ObDTLIntermResultManager::inc_interm_result_ref_count(interm_res_info);
      LOG_TRACE("trace init sql mem mgr for temp table insert", K(MY_SPEC.width_),
        K(profile_.get_cache_size()), K(profile_.get_expect_size()));
    }
  }
  return ret;
}

int ObTempTableInsertVecOp::insert_interm_result_info()
{
  int ret = OB_SUCCESS;
  uint64_t interm_result_id;
  ObSEArray<dtl::ObDTLIntermResultKey, 8> keys_insert;
  dtl::ObDTLIntermResultKey dtl_int_key;
  ObDTLIntermResultInfo *interm_res_info = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx is NULL", K(ret));
  } else if (!MY_SPEC.is_distributed_ &&
             all_interm_res_info_.empty() &&
             OB_FAIL(create_interm_result_info(interm_res_info))) {
    // An empty row store is required as a placeholder for the local temp table.
    LOG_WARN("failed to init chunk row store", K(ret));
  } else if (!MY_SPEC.is_distributed_ && all_interm_res_info_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local temp table shoud have one chunk row store", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_interm_res_info_.count(); ++i) {
    ObDTLIntermResultInfo *&cur_interm_res_info = all_interm_res_info_.at(i);
    if (OB_UNLIKELY(NULL == cur_interm_res_info || !cur_interm_res_info->is_store_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect invalid row store", K(ret));
    } else if (!MY_SPEC.is_distributed_ &&
             OB_FAIL(prepare_interm_result_id_for_local(interm_result_id))) {
      LOG_WARN("failed to prepare interm result id", K(ret));
    } else if (MY_SPEC.is_distributed_ &&
              OB_FAIL(prepare_interm_result_id_for_distribute(interm_result_id))) {
      LOG_WARN("failed to prepare interm result id", K(ret));
    } else if (OB_FAIL(cur_interm_res_info->get_column_store()->finish_add_row())) {
      LOG_WARN("failed to finish add row", K(ret));
    } else {
      dtl_int_key.channel_id_ = interm_result_id;
      dtl_int_key.start_time_ = oceanbase::common::ObTimeUtility::current_time();
      dtl_int_key.timeout_ts_ = phy_plan_ctx->get_timeout_timestamp();
      cur_interm_res_info->set_eof(true);
      // The chunk row store does not require managing the dump logic.
      cur_interm_res_info->is_read_ = true;
      if (OB_FAIL(MTL(dtl::ObDTLIntermResultManager *)->insert_interm_result_info(
                                        dtl_int_key, cur_interm_res_info))) {
        LOG_WARN("failed to insert row store.", K(ret), K(dtl_int_key.channel_id_));
      } else if (OB_FAIL(keys_insert.push_back(dtl_int_key))) {
        LOG_WARN("failed to push back key", K(ret));
        MTL(dtl::ObDTLIntermResultManager *)->erase_interm_result_info(dtl_int_key);
      } else {
        cur_interm_res_info->get_column_store()->reset_callback();
        ObPxSqcHandler *handler = ctx_.get_sqc_handler();
        if (MY_SPEC.is_distributed_ && OB_NOT_NULL(handler) &&
            handler->get_phy_plan().is_enable_px_fast_reclaim()) {
          int reg_dm_ret = ObDetectManagerUtils::temp_table_register_check_item_into_dm(
              handler->get_sqc_init_arg().sqc_.get_px_detectable_ids().qc_detectable_id_,
              handler->get_sqc_init_arg().sqc_.get_qc_addr(),
              dtl_int_key, cur_interm_res_info);
          if (OB_SUCCESS != reg_dm_ret) {
            LOG_WARN("[DM] failed register_check_item_into_dm", K(dtl_int_key), K(dtl_int_key));
          }
          LOG_TRACE("[DM] temptable register a check item", K(reg_dm_ret), K(dtl_int_key),
              K(handler->get_sqc_init_arg().sqc_.get_px_detectable_ids().qc_detectable_id_),
              K(handler->get_sqc_init_arg().sqc_.get_qc_addr()));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    // Exception handling
    for (int64_t i = 0; i < keys_insert.count(); ++i) {
      MTL(dtl::ObDTLIntermResultManager *)->erase_interm_result_info(keys_insert.at(i));
    }
  } else {
    clear_all_interm_res_info();
  }
  return ret;
}

// clear all_col_store and decrease ref count of each result_info in it.
int ObTempTableInsertVecOp::clear_all_interm_res_info()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_interm_res_info_.count(); ++i) {
    ObDTLIntermResultInfo *col_store = all_interm_res_info_.at(i);
    if (NULL != col_store) {
      MTL(dtl::ObDTLIntermResultManager*)->dec_interm_result_ref_count(col_store);
    }
  }
  all_interm_res_info_.reset();
  return ret;
}

int ObTempTableInsertVecOp::prepare_interm_result_id_for_local(uint64_t &interm_result_id)
{
  int ret = OB_SUCCESS;
  ObSqlTempTableCtx temp_table_ctx;
  temp_table_ctx.temp_table_id_ = MY_SPEC.temp_table_id_;
  temp_table_ctx.is_local_interm_result_ = true;
  interm_result_id = dtl::ObDtlChannel::generate_id();
  ObTempTableResultInfo info;
  info.addr_ = GCTX.self_addr();
  if (OB_FAIL(info.interm_result_ids_.push_back(interm_result_id))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(temp_table_ctx.interm_result_infos_.push_back(info))) {
    LOG_WARN("failed to push back info", K(ret));
  } else if (OB_FAIL(get_exec_ctx().get_temp_table_ctx().push_back(temp_table_ctx))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObTempTableInsertVecOp::prepare_interm_result_id_for_distribute(uint64_t &interm_result_id)
{
  int ret = OB_SUCCESS;
  interm_result_id = dtl::ObDtlChannel::generate_id();
  if (OB_FAIL(interm_result_ids_.push_back(interm_result_id))) {
    LOG_WARN("failed to push back result id", K(ret));
  }
  return ret;
}

int ObTempTableInsertVecOp::process_dump(dtl::ObDTLIntermResultInfo &interm_res_info)
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  UNUSED(updated);
  // For material, due to the need for sequential preservation,
  // the implementation of dump is to dump everything while also preserving the final memory data.
  // Currently, the chosen approach is to dump from the beginning to the end.
  // Another implementation approach is to dump the remaining data up to the maximum memory capacity,
  // and any future data writes would require a dump. However, this approach would require memory scaling.
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      [&](int64_t cur_cnt){ return interm_res_info.get_column_store()->get_row_cnt_in_memory() > cur_cnt; },
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump() && GCONF.is_sql_operator_dump_enabled()
          && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              return sql_mem_processor_.get_data_size() > max_memory_size;
            },
            dumped, sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    int64_t dump_start_time = oceanbase::common::ObTimeUtility::current_time();
    int64_t dump_end_time = dump_start_time;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_interm_res_info_.count(); ++i) {
      // The last block of the last chunk may still need to insert data and should not be dumped.
      bool dump_last_block = i < all_interm_res_info_.count() - 1;
      if (OB_FAIL(all_interm_res_info_.at(i)->get_column_store()->dump(dump_last_block))) {
        LOG_WARN("failed to dump row store", K(ret));
      } else {
        dump_end_time = oceanbase::common::ObTimeUtility::current_time();
        all_interm_res_info_.at(i)->dump_time_ = 0 == all_interm_res_info_.at(i)->dump_time_ ?
                                            dump_start_time :
                                            all_interm_res_info_.at(i)->dump_time_;
        all_interm_res_info_.at(i)->dump_cost_ += dump_end_time - dump_start_time;
        dump_start_time = dump_end_time;
      }
    }
    if (OB_SUCC(ret)) {
      sql_mem_processor_.set_number_pass(1);
      LOG_WARN("trace temp table insert dump",
        K(sql_mem_processor_.get_data_size()),
        K(all_interm_res_info_.count()),
        K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
