/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_group_execute.h"
#include "observer/ob_req_time_service.h"
#include "observer/table/ob_table_move_response.h"

using namespace oceanbase::observer;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace table
{

int ObTableGroupCommitCreateCbFunctor::init(ObTableGroupCommitOps *group,
                                            bool add_failed_group,
                                            ObTableFailedGroups *failed_groups,
                                            ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                                            ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_ISNULL(group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("group is null", K(ret));
    } else if (OB_ISNULL(failed_groups)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed_groups is null", K(ret));
    } else if (OB_ISNULL(group_factory)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("group_factory is null", K(ret));
    } else if (OB_ISNULL(op_factory)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("op_factory is null", K(ret));
    } else {
      group_ = group;
      add_failed_group_ = add_failed_group;
      failed_groups_ = failed_groups;
      group_factory_ = group_factory;
      op_factory_ = op_factory;
      is_inited_ = true;
    }
  }

  return ret;
}

ObTableAPITransCb* ObTableGroupCommitCreateCbFunctor::new_callback()
{
  ObTableGroupCommitEndTransCb *tmp_cb = nullptr;
  if (is_inited_) {
    if (OB_NOT_NULL(cb_)) {
      tmp_cb = cb_;
    } else {
      tmp_cb = OB_NEW(ObTableGroupCommitEndTransCb,
                      ObMemAttr(MTL_ID(), "TbGcExuCb"),
                      group_,
                      add_failed_group_,
                      failed_groups_,
                      group_factory_,
                      op_factory_);
      if (NULL != tmp_cb) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(tmp_cb->init())) {
          LOG_WARN("fail to init callback", K(ret));
          tmp_cb->~ObTableGroupCommitEndTransCb();
          tmp_cb = NULL;
          ob_free(tmp_cb);
        } else {
          cb_ = tmp_cb;
        }
      }
    }
  }
  return tmp_cb;
}

int ObTableGroupCommitEndTransCb::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_ISNULL(group_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group is null", K(ret));
    } else if (OB_FAIL(results_.init(group_->ops_.count()))) {
      LOG_WARN("fail to init results", K(ret), K(group_->ops_.count()));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObTableGroupCommitEndTransCb::callback(int cb_param)
{
  int ret = OB_SUCCESS;

  check_callback_timeout();
  if (OB_UNLIKELY(!has_set_need_rollback_)) {
    LOG_ERROR("is_need_rollback_ has not been set", K_(has_set_need_rollback), K_(is_need_rollback));
  } else if (OB_UNLIKELY(ObExclusiveEndTransCallback::END_TRANS_TYPE_INVALID == end_trans_type_)) {
    LOG_ERROR("end trans type is invalid", K(cb_param), K(end_trans_type_));
  } else if (OB_NOT_NULL(tx_desc_)) {
    MTL(ObTransService*)->release_tx(*tx_desc_);
    tx_desc_ = NULL;
  }
  if (lock_handle_ != nullptr) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle_);
  }
  this->handin();
  CHECK_BALANCE("[table group commit async callback]");

  if (OB_FAIL(cb_param)) {
    if (add_failed_group_) {
      // move group to failed_groups if execute failed.
      if (OB_ISNULL(failed_groups_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed groups is null", K(ret));
      } else if (OB_FAIL(failed_groups_->add(group_))) {
        LOG_WARN("fail to add failed group", K(ret), KPC_(group));
      }
    } else {
      // response failed result directly
      if (OB_FAIL(ObTableGroupExecuteService::response_failed_results(cb_param,
                                                                      *group_,
                                                                      group_factory_,
                                                                      op_factory_))) {
        LOG_WARN("fail to response failed results", K(ret), KPC_(group), K(cb_param));
      }
    }
  } else { // commit success
    if (OB_FAIL(ObTableGroupExecuteService::response(*group_,
                                                     group_factory_,
                                                     op_factory_,
                                                     results_))) {
      LOG_WARN("fail to response", K(ret), KPC_(group), K_(results));
    }
  }

  this->destroy_cb_if_no_ref();
}

int ObTableGroupExecuteService::init_table_ctx(ObTableGroupCommitOps &group, ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  ObTableOperationType::Type op_type = ObTableOperationType::Type::INVALID;
  ObTableGroupCommitSingleOp *single_op = nullptr;

  if (group.ops_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops is empty", K(ret));
  } else if (FALSE_IT(single_op = group.ops_.at(0))) {
  } else if (OB_ISNULL(single_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("single_op is null", K(ret));
  } else {
    op_type = single_op->op_.type();
    tb_ctx.set_entity(&single_op->op_.entity());
    tb_ctx.set_entity_type(group.entity_type_);
    tb_ctx.set_operation_type(op_type);
    tb_ctx.set_ls_id(group.ls_id_);
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tb_ctx.init_common(group.credential_,
                                        group.tablet_id_,
                                        group.timeout_ts_))) {
    LOG_WARN("fail to init table tb_ctx common part", K(ret), K(group));
  } else {
    switch (op_type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx.init_get())) {
          LOG_WARN("fail to init get tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx.init_put())) {
          LOG_WARN("fail to init put tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", op_type);
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec tb_ctx", K(ret), K(tb_ctx));
  }

  return ret;
}

int ObTableGroupExecuteService::init_batch_ctx(ObTableGroupCommitOps &group, ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitSingleOp *single_op = group.ops_.at(0);

  if (group.ops_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops is empty", K(ret));
  } else if (FALSE_IT(single_op = group.ops_.at(0))) {
  } else if (OB_ISNULL(single_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("single_op is null", K(ret));
  } else {
    bool is_readonly = (single_op->op_.type() == ObTableOperationType::Type::GET);
    batch_ctx.table_id_ = group.table_id_;
    batch_ctx.tablet_id_ = group.tablet_id_;
    batch_ctx.is_atomic_ = true;
    batch_ctx.is_readonly_ = is_readonly;
    batch_ctx.is_same_type_ = true;
    batch_ctx.is_same_properties_names_ = true;
    batch_ctx.entity_type_ = group.entity_type_;
    batch_ctx.consistency_level_ = table::ObTableConsistencyLevel::STRONG;
    batch_ctx.credential_ = &group.credential_;
  }

  return ret;
}

int ObTableGroupExecuteService::start_trans(ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = batch_ctx.tb_ctx_;
  ObTableTransParam *trans_param = batch_ctx.trans_param_;
  int64_t timeout_ts = tb_ctx.get_timeout_ts();
  if (timeout_ts - ObClockGenerator::getClock() < 0) {
    timeout_ts = ObClockGenerator::getClock() + DEFAULT_TRANS_TIMEOUT;
  }

  if (OB_ISNULL(trans_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans param is null", K(ret));
  } else if (batch_ctx.is_readonly_) {
    if (OB_FAIL(trans_param->init(batch_ctx.consistency_level_,
                                  tb_ctx.get_ls_id(),
                                  timeout_ts,
                                  tb_ctx.need_dist_das()))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::init_read_trans(*trans_param))) {
      LOG_WARN("fail to init read trans", K(ret), KPC(trans_param));
    }
  } else { // other batch operation
    if (OB_FAIL(trans_param->init(batch_ctx.is_readonly_,
                                  batch_ctx.consistency_level_,
                                  tb_ctx.get_ls_id(),
                                  timeout_ts,
                                  tb_ctx.need_dist_das()))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::start_trans(*trans_param))) {
      LOG_WARN("fail to start trans", K(ret), KPC(trans_param));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(tb_ctx.init_trans(trans_param->trans_desc_, trans_param->tx_snapshot_))) {
    LOG_WARN("fail to init trans", K(ret));
  }

  return ret;
}

int ObTableGroupExecuteService::end_trans(const ObTableBatchCtx &batch_ctx,
                                          ObTableGroupCommitCreateCbFunctor *create_cb_functor,
                                          bool is_rollback)
{
  int ret = OB_SUCCESS;
  ObTableTransParam *trans_param = batch_ctx.trans_param_;

  if (OB_ISNULL(trans_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans param is null", K(ret));
  } else if (batch_ctx.is_readonly_) {
    ObTableTransUtils::release_read_trans(trans_param->trans_desc_);
  } else { // other batch operation
    trans_param->is_rollback_ = is_rollback;
    trans_param->req_ = nullptr;
    trans_param->use_sync_ = false;
    trans_param->create_cb_functor_ = create_cb_functor;
    if (OB_FAIL(ObTableTransUtils::end_trans(*trans_param))) {
      LOG_WARN("fail to end trans", K(ret), KPC(trans_param));
    }
  }

  return ret;
}

void ObTableGroupExecuteService::free_ops(ObTableGroupCommitOps &group,
                                          ObTableGroupFactory<ObTableGroupCommitSingleOp> &op_factory)
{
  ObTableGroupCommitSingleOp *op = nullptr;
  for (int64_t i = 0; i < group.ops_.count(); i++) {
      // free op even if response failed
      op_factory.free(group.ops_.at(i));
  }
}

int ObTableGroupExecuteService::response(ObTableGroupCommitOps &group,
                                         ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                                         ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory,
                                         ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  const int64_t ops_count = group.ops_.count();
  const int64_t res_count = results.count();

  if (ops_count == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operation size", K(ret));
  } else if (ops_count != res_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid result size", K(ret), K(ops_count), K(res_count));
  } else {
    ObTableRpcResponseSender<ObTableOperationResult> sender;
    ObTableGroupCommitSingleOp *op = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < ops_count; i++) {
      op = group.ops_.at(i);
      int ret_code = results.at(i).get_errno();
      rpc::ObRequest *req = nullptr;
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op is null", K(ret), K(i));
      } else if (OB_ISNULL(req = op->req_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("request is null", K(ret), K(i));
      } else {
        const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(req->get_packet());
        if (ObTableRpcProcessorUtil::need_do_move_response(ret_code, *rpc_pkt)) { // rerouting pkg
          ObTableMoveResponseSender move_sender(req, ret_code);
          if (OB_ISNULL(GCTX.schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("gctx schema_service is null", K(ret), K(i));
          } else if (OB_FAIL(move_sender.init(group.table_id_, group.tablet_id_, *GCTX.schema_service_))) {
            LOG_WARN("fail to init move response sender", K(ret), K_(group.table_id), K_(group.tablet_id));
          } else if (OB_FAIL(move_sender.response())) {
            LOG_WARN("fail to do move response", K(ret), K(i));
          }
          // if response rerouting fail, response the common failure packet
          if (OB_FAIL(ret)) {
            sender.set_req(req);
            sender.set_result(&results.at(i));
            if (OB_FAIL(sender.response(ret_code))) {
              LOG_WARN("fail to send response", K(ret), KPC(op), K(results.at(i)), K(i));
            }
          }
        } else {
          sender.set_req(req);
          sender.set_result(&results.at(i));
          if (OB_FAIL(sender.response(ret_code))) {
            LOG_WARN("fail to send response", K(ret), KPC(op), K(results.at(i)), K(i));
          }
        }
      }
    } // end for
  }

  // free group even if response failed
  if (OB_NOT_NULL(op_factory)) {
    free_ops(group, *op_factory);
  }

  if (OB_NOT_NULL(group_factory)) {
    group_factory->free(&group);
  }

  return ret;
}

int ObTableGroupExecuteService::generate_failed_results(int ret_code,
                                                        ObITableEntity &result_entity,
                                                        ObTableGroupCommitOps &group,
                                                        ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableOperationResult result;
  result.set_entity(&result_entity);
  for (int64_t i = 0; OB_SUCC(ret) && i < group.ops_.count(); i++) {
    result.set_type(group.ops_.at(i)->op_.type());
    result.set_errno(ret_code);
    if (OB_FAIL(results.push_back(result))) {
      LOG_WARN("fail to push back result", K(ret));
    }
  }

  return ret;
}

int ObTableGroupExecuteService::response_failed_results(int ret_code,
                                                        ObTableGroupCommitOps &group,
                                                        ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                                                        ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory)
{
  int ret = OB_SUCCESS;
  ObTableEntity result_entity;
  ObSEArray<ObTableOperationResult, 1> failed_results;

  if (OB_FAIL(generate_failed_results(ret_code, result_entity, group, failed_results))) {
    LOG_WARN("fail to generate failed results", K(ret), K(ret_code), K(group));
  } else if (OB_FAIL(response(group, group_factory, op_factory, failed_results))) {
    LOG_WARN("fail to response", K(ret), K(group), K(failed_results));
  }

  return ret;
}

int ObTableGroupExecuteService::execute_read(const ObTableGroupCtx &ctx,
                                             ObTableGroupCommitOps &group,
                                             bool add_failed_group,
                                             bool &had_do_response)
{
  int ret = OB_SUCCESS;
  had_do_response = false;
  ObArenaAllocator tmp_allocator("TbGroupRead", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  int32_t stat_event_type = ObTableProccessType::TABLE_API_PROCESS_TYPE_INVALID;
  ObTableEntityFactory<ObTableEntity> entity_factory;
  ObTableEntity result_entity;
  OpFixedArray ops(tmp_allocator);
  ResultFixedArray results(tmp_allocator);
  SMART_VAR(ObTableBatchCtx, batch_ctx, tmp_allocator) {
    batch_ctx.trans_param_ = ctx.trans_param_;
    batch_ctx.stat_event_type_ = &stat_event_type;
    batch_ctx.ops_ = &ops;

    if (OB_FAIL(ops.init(group.ops_.count()))) {
      LOG_WARN("fail to init ops", K(ret), K(group.ops_.count()));
    } else if (OB_FAIL(results.init(group.ops_.count()))) {
      LOG_WARN("fail to init results", K(ret), K(group.ops_.count()));
    } else if (OB_FAIL(group.get_ops(ops))) {
      LOG_WARN("fail to get ops", K(ret));
    } else {
      batch_ctx.entity_factory_ = &entity_factory;
      batch_ctx.result_entity_ = &result_entity;
      batch_ctx.results_ = &results;
      batch_ctx.tb_ctx_.set_schema_cache_guard(ctx.schema_cache_guard_);
      batch_ctx.tb_ctx_.set_schema_guard(ctx.schema_guard_);
      batch_ctx.tb_ctx_.set_simple_table_schema(ctx.simple_schema_);
      batch_ctx.tb_ctx_.set_sess_guard(ctx.sess_guard_);
      group.timeout_ts_ = ctx.timeout_ts_;

      if (OB_FAIL(init_table_ctx(group, batch_ctx.tb_ctx_))) {
        LOG_WARN("fail to init table ctx", K(ret), K(group), K(add_failed_group));
      } else if (OB_FAIL(init_batch_ctx(group, batch_ctx))) {
        LOG_WARN("fail to init batch ctx", K(ret), K(group));
      } else if (OB_FAIL(start_trans(batch_ctx))) {
        LOG_WARN("fail to start trans", K(ret), K(batch_ctx));
      } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx))) {
        LOG_WARN("fail to execute batch operation", K(batch_ctx), K(add_failed_group), K(group));
      } else if (OB_FAIL(response(group, ctx.group_factory_, ctx.op_factory_, *batch_ctx.results_))) {
        LOG_WARN("fail to response", K(ret), K(group), KPC(batch_ctx.results_));
      } else {
        had_do_response = true;
      }
    }

    int tmp_ret = ret;
    bool is_rollback = (OB_SUCCESS != ret);
    if (OB_FAIL(end_trans(batch_ctx, nullptr/*callback*/, is_rollback))) {
      LOG_WARN("fail to end trans", K(ret), K(is_rollback), K(group));
    }
    ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

    if (OB_FAIL(ret) && !had_do_response) {
      if (add_failed_group) {
        // move group to failed_groups if execute failed.
        ret = OB_SUCCESS; // cover ret code
        if (OB_ISNULL(ctx.failed_groups_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed groups is null", K(ret));
        } else if (OB_FAIL(ctx.failed_groups_->add(&group))) {
          LOG_WARN("fail to add failed group", K(ret), K(group));
        }
      } else {
        // response failed result directly
        int ret_code = ret;
        if (OB_FAIL(ObTableGroupExecuteService::response_failed_results(ret_code,
                                                                        group,
                                                                        ctx.group_factory_,
                                                                        ctx.op_factory_))) {
          LOG_WARN("fail to response failed results", K(ret), K(group));
        } else {
          had_do_response = true;
        }
      }
    }
  }

  return ret;
}

int ObTableGroupExecuteService::execute_dml(const ObTableGroupCtx &ctx,
                                            ObTableGroupCommitOps &group,
                                            bool add_failed_group,
                                            bool &had_do_response)
{
  int ret = OB_SUCCESS;
  had_do_response = false;
  ObArenaAllocator tmp_allocator("TbGroupDml", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  int32_t stat_event_type = ObTableProccessType::TABLE_API_PROCESS_TYPE_INVALID;
  OpFixedArray ops(tmp_allocator);
  SMART_VAR(ObTableBatchCtx, batch_ctx, tmp_allocator) {
    batch_ctx.trans_param_ = ctx.trans_param_;
    batch_ctx.stat_event_type_ = &stat_event_type;
    batch_ctx.ops_ = &ops;

    // @note cb can not be accessed any more after end trans
    ObTableGroupCommitCreateCbFunctor functor;
    ObTableGroupCommitEndTransCb *cb = nullptr;
    if (OB_FAIL(functor.init(&group, add_failed_group, ctx.failed_groups_, ctx.group_factory_, ctx.op_factory_))) {
      LOG_WARN("fail to init create group commit callback functor", K(ret), K(group), K(add_failed_group));
    } else if (OB_FAIL(ops.init(group.ops_.count()))) {
      LOG_WARN("fail to init ops", K(ret), K(group.ops_.count()));
    } else if (OB_FAIL(group.get_ops(ops))) {
      LOG_WARN("fail to get ops", K(ret));
    } else if (OB_ISNULL(cb = static_cast<ObTableGroupCommitEndTransCb *>(functor.new_callback()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new group commit end trans callback", K(ret));
    } else {
      batch_ctx.entity_factory_ = &cb->entity_factory_;
      batch_ctx.result_entity_ = &cb->result_entity_;
      batch_ctx.results_ = &cb->results_;
      batch_ctx.tb_ctx_.set_schema_cache_guard(ctx.schema_cache_guard_);
      batch_ctx.tb_ctx_.set_schema_guard(ctx.schema_guard_);
      batch_ctx.tb_ctx_.set_simple_table_schema(ctx.simple_schema_);
      batch_ctx.tb_ctx_.set_sess_guard(ctx.sess_guard_);
      group.timeout_ts_ = ctx.timeout_ts_;

      if (OB_FAIL(init_table_ctx(group, batch_ctx.tb_ctx_))) {
        LOG_WARN("fail to init table ctx", K(ret), K(group), K(add_failed_group));
      } else if (OB_FAIL(init_batch_ctx(group, batch_ctx))) {
        LOG_WARN("fail to init batch ctx", K(ret), K(group));
      } else if (OB_FAIL(start_trans(batch_ctx))) {
        LOG_WARN("fail to start trans", K(ret), K(batch_ctx));
      } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx))) {
        LOG_WARN("fail to execute batch operation", K(batch_ctx), K(add_failed_group), K(group));
      }
    }

    int tmp_ret = ret;
    bool is_rollback = (OB_SUCCESS != ret);
    if (is_rollback) {
      // if is_rollback == true, cb need to be released because end_trans wouldn't release it
      OB_DELETE(ObTableGroupCommitEndTransCb, "TbGcExuCb", cb);
    }

    if (OB_FAIL(end_trans(batch_ctx, &functor, is_rollback))) {
      LOG_WARN("fail to end trans", K(ret), K(is_rollback), K(group));
    }
    ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

    had_do_response = batch_ctx.trans_param_->had_do_response_;
    if (OB_FAIL(ret) && !had_do_response) {
      if (add_failed_group) {
        // move group to failed_groups if execute failed.
        ret = OB_SUCCESS; // cover ret code
        if (OB_ISNULL(ctx.failed_groups_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed groups is null", K(ret));
        } else if (OB_FAIL(ctx.failed_groups_->add(&group))) {
          LOG_WARN("fail to add failed group", K(ret), K(group));
        }
      } else {
        // response failed result directly
        int ret_code = ret;
        if (OB_FAIL(ObTableGroupExecuteService::response_failed_results(ret_code,
                                                                        group,
                                                                        ctx.group_factory_,
                                                                        ctx.op_factory_))) {
          LOG_WARN("fail to response failed results", K(ret), K(group));
        } else {
          had_do_response = true;
        }
      }
    }
  }

  return ret;
}

int ObTableGroupExecuteService::execute(const ObTableGroupCtx &ctx,
                                        ObTableGroupCommitOps &group,
                                        bool add_failed_group,
                                        bool &had_do_response)
{
  int ret = OB_SUCCESS;

  if (group.is_get()) {
    if (OB_FAIL(execute_read(ctx, group, add_failed_group, had_do_response))) {
      LOG_WARN("fail to execute read group", K(ret), K(ctx), K(group), K(add_failed_group));
    }
  } else {
    if (OB_FAIL(execute_dml(ctx, group, add_failed_group, had_do_response))) {
      LOG_WARN("fail to execute dml group", K(ret), K(ctx), K(group), K(add_failed_group));
    }
  }

  return ret;
}

int ObTableGroupExecuteService::execute(ObTableGroupCommitOps &group,
                                        ObTableFailedGroups *failed_groups,
                                        ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                                        ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory,
                                        bool add_failed_group,
                                        bool &had_do_response)
{
  int ret = OB_SUCCESS;

  ObTableGroupCtx ctx;
  ObSchemaGetterGuard schema_guard;
  observer::ObReqTimeGuard req_timeinfo_guard_; // use for clear schema_cache_guard
  ObKvSchemaCacheGuard schema_cache_guard;
  ObTableApiSessGuard sess_guard;
  ObTableTransParam trans_param;
  const schema::ObSimpleTableSchemaV2 *simple_schema = nullptr;
  if (OB_FAIL(ObTableGroupUtils::init_schema_cache_guard(group, schema_guard, schema_cache_guard, simple_schema))) {
    LOG_WARN("fail to init schema cache guard", K(ret), K(group));
  } else if (OB_FAIL(ObTableGroupUtils::init_sess_guard(group, sess_guard))) {
    LOG_WARN("fail to init sess guard", K(ret), K(group));
  } else {
    // when execute timeout groups or failed group, maybe the group timeout_ts is less than current timestamp
    // so we need to reset timeout_ts or we will fail to start the transaction
    int64_t timeout_ts = ObClockGenerator::getClock() + group.timeout_;
    ctx.entity_type_ = group.entity_type_;
    ctx.credential_ = group.credential_;
    ctx.tablet_id_ = group.tablet_id_;
    ctx.timeout_ts_ = timeout_ts;
    ctx.trans_param_ = &trans_param;
    ctx.schema_guard_ = &schema_guard;
    ctx.simple_schema_ = simple_schema;
    ctx.schema_cache_guard_ = &schema_cache_guard;
    ctx.sess_guard_ = &sess_guard;
    ctx.failed_groups_ = failed_groups;
    ctx.group_factory_ = group_factory;
    ctx.op_factory_ = op_factory;

    if (OB_FAIL(ObTableGroupExecuteService::execute(ctx, group, add_failed_group, had_do_response))) {
      LOG_WARN("fail to execute group", K(ret), K(ctx), K(group), K(add_failed_group));
    }
  }

  return ret;
}

int ObTableGroupExecuteService::execute_one_by_one(ObTableGroupCommitOps &group,
                                                   ObTableFailedGroups *failed_groups,
                                                   ObTableGroupFactory<ObTableGroupCommitOps> *group_factory,
                                                   ObTableGroupFactory<ObTableGroupCommitSingleOp> *op_factory)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableGroupCommitSingleOp*> &ops = group.ops_;
  bool add_failed_group = false;
  bool had_do_response = false;

  for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
    ObTableGroupCommitOps *group_with_one_op = nullptr;
    if (OB_ISNULL(group_with_one_op = group_factory->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc group", K(ret));
    } else {
      ObTableGroupCommitSingleOp *op = ops.at(i);
      if (OB_FAIL(group_with_one_op->init(group.credential_,
                                          group.ls_id_,
                                          group.table_id_,
                                          group.tablet_id_,
                                          group.entity_type_,
                                          group.timeout_ts_))) {
        LOG_WARN("fail to init group", K(ret), K(group));
      } else if (OB_FAIL(group_with_one_op->add_op(op))) {
        LOG_WARN("fail to add op", K(ret), K(group_with_one_op));
      } else if (OB_FAIL(execute(*group_with_one_op, failed_groups, group_factory, op_factory, add_failed_group, had_do_response))) {
        LOG_WARN("fail to execute group", K(ret), K(group_with_one_op), K(had_do_response));
      }
      if (OB_FAIL(ret)) {
        if (!had_do_response) {
          int ret_code = ret;
          if (OB_FAIL(response_failed_results(ret_code, *group_with_one_op, group_factory, op_factory))) {
            LOG_WARN("fail to response failed results", K(ret), KPC(group_with_one_op));
          }
        }
      }
    }
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase
