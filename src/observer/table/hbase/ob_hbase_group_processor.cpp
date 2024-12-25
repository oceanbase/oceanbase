/**
 * Copyright (c) 2024 OceanBase
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

#include "ob_hbase_group_processor.h"
#include "observer/table/group/ob_table_group_execute.h"
#include "observer/table/ob_table_multi_batch_service.h"


using namespace oceanbase::table;
using namespace oceanbase::common;
using namespace oceanbase::observer;

int ObHbaseOpProcessor::init_table_ctx(const ObTableSingleOp &single_op, ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;

  ObLSID ls_id = static_cast<ObHbaseOp*>(ops_->at(0))->ls_req_.ls_op_.get_ls_id();
  ObTabletID tablet_id = static_cast<ObHbaseOp*>(ops_->at(0))->ls_req_.ls_op_.at(0).get_tablet_id();
  ObTableOperationType::Type op_type = single_op.get_op_type();
  tb_ctx.set_schema_guard(group_ctx_->schema_guard_);
  tb_ctx.set_schema_cache_guard(group_ctx_->schema_cache_guard_);
  tb_ctx.set_simple_table_schema(group_ctx_->simple_schema_);
  tb_ctx.set_sess_guard(group_ctx_->sess_guard_);
  tb_ctx.set_audit_ctx(&group_ctx_->audit_ctx_);
  tb_ctx.set_entity(&single_op.get_entities().at(0));
  tb_ctx.set_entity_type(ObTableEntityType::ET_HKV);
  tb_ctx.set_operation_type(op_type);
  tb_ctx.set_need_dist_das(false);
  tb_ctx.set_ls_id(ls_id);

  if (OB_FAIL(tb_ctx.init_common(group_ctx_->credential_, tablet_id, group_ctx_->timeout_ts_))) {
    LOG_WARN("fail to init table tb_ctx common part", K(ret));
  } else {
    switch (op_type) {
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(tb_ctx.init_insert_up(false))) {
          LOG_WARN("fail to init insert up tb_ctx", K(ret), K(tb_ctx));
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

int ObHbaseOpProcessor::init_multi_batch_ctx()
{
  int ret = OB_SUCCESS;

  if (ops_->count() == 0 || OB_ISNULL(ops_->at(0)) ||
      ops_->at(0)->type() != ObTableGroupType::TYPE_HBASE_GROUP) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC_(ops));
  } else {
    ObHbaseOp *op = static_cast<ObHbaseOp*>(ops_->at(0));
    if (op->ls_req_.ls_op_.count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tablet op count is 0", K(ret));
    } else {
      const ObTableTabletOp &tablet_op = op->ls_req_.ls_op_.at(0);
      if (tablet_op.count() == 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("single op count is 0", K(ret));
      } else {
        const ObTableSingleOp &single_op = tablet_op.at(0);
        if (OB_FAIL(init_table_ctx(single_op, multi_batch_ctx_->tb_ctx_))) {
          LOG_WARN("fail to init table context", K(ret));
        } else {
          multi_batch_ctx_->trans_param_ = group_ctx_->trans_param_;
          multi_batch_ctx_->is_atomic_ = true;
          multi_batch_ctx_->is_readonly_ = tablet_op.is_readonly();
          multi_batch_ctx_->is_same_type_ = op->ls_req_.ls_op_.is_same_type();
          multi_batch_ctx_->is_same_properties_names_ = op->ls_req_.ls_op_.is_same_properties_names();
          multi_batch_ctx_->consistency_level_ = op->ls_req_.consistency_level_;
          multi_batch_ctx_->credential_ = &group_ctx_->credential_;
        }
      }
    }
  }

  return ret;
}

int ObHbaseOpProcessor::init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObITableOpProcessor::init(group_ctx, ops))) {
    LOG_WARN("fail to init processor", K(ret));
  } else if (OB_ISNULL(multi_batch_ctx_ = OB_NEWx(ObTableMultiBatchCtx,
                                                  (&allocator_),
                                                  allocator_,
                                                  group_ctx.audit_ctx_,
                                                  default_entity_factory_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc multi batch ctx", K(ret));
  } else if (OB_FAIL(init_multi_batch_ctx())) {
    LOG_WARN("fail to init multi batch ctx", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObHbaseOpProcessor::fill_batch_op(const ObTableTabletOp &tablet_op, ObTableBatchOperation &batch_op)
{
  int ret = OB_SUCCESS;
  const int64_t count = tablet_op.count();

  if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("single op entity is 0", K(ret), K(tablet_op));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    const ObTableSingleOpEntity &entity = tablet_op.at(i).get_entities().at(0);
    if (OB_FAIL(batch_op.insert_or_update(entity))) {
      LOG_WARN("fail to push back insert or update op", K(ret), K(entity));
    }
  }

  return ret;
}

int ObHbaseOpProcessor::init_result(const int64_t total_tablet_count, ObTableMultiBatchResult &result)
{
  int ret = OB_SUCCESS;
  const int64_t op_count = ops_->count();

  if (OB_FAIL(result.prepare_allocate(total_tablet_count))) {
    LOG_WARN("fail to prepare allocate results", K(ret), K(total_tablet_count));
  } else {
    int64_t batch_result_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < op_count; i++) { // loop group op
      ObHbaseOp *op = static_cast<ObHbaseOp*>(ops_->at(i));
      int64_t tablet_count = op->ls_req_.ls_op_.count();
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_count; j++, batch_result_idx++) { // loop tablet
        ObTableTabletOpResult &tablet_result = op->result_.at(j);
        ObTableBatchOperationResult &batch_result = result.get_results().at(batch_result_idx);
        int64_t single_op_count = tablet_result.count();
        for (int64_t k = 0; OB_SUCC(ret) && k < single_op_count; k++) { // loop single op
          if (OB_FAIL(batch_result.push_back(tablet_result.at(k)))) {
            LOG_WARN("fail to push back ObTableOperationResult", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObHbaseOpProcessor::process()
{
  int ret = OB_SUCCESS;
  ObTableMultiBatchRequest *req = nullptr;
  ObTableMultiBatchResult *result = nullptr;
  const int64_t op_count = ops_->count();
  const int64_t total_tablet_count = get_tablet_count();

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("processor not init", K(ret));
  } else if (OB_ISNULL(req = OB_NEWx(ObTableMultiBatchRequest, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc multi batch request", K(ret));
  } else if (OB_ISNULL(result = OB_NEWx(ObTableMultiBatchResult, (&allocator_), allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc multi batch result", K(ret));
  } else if (OB_FAIL(req->get_ops().prepare_allocate(total_tablet_count))) {
    LOG_WARN("fail to reserve request ops", K(ret), K(total_tablet_count));
  } else {
    int64_t batch_req_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < op_count; i++) { // loop op
      ObHbaseOp *op = static_cast<ObHbaseOp*>(ops_->at(i));
      int64_t tablet_count = op->ls_req_.ls_op_.count();
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_count; j++, batch_req_idx++) { // loop tablet
        const ObTableTabletOp &tablet_op = op->ls_req_.ls_op_.at(j);
        ObTableBatchOperation &batch_op = req->get_ops().at(batch_req_idx);
        batch_op.set_entity_factory(&default_entity_factory_);
        if (OB_FAIL(fill_batch_op(tablet_op, batch_op))) {
          LOG_WARN("fail to fill batch op", K(ret), KPC(op));
        } else if (OB_FAIL(req->get_tablet_ids().push_back(tablet_op.get_tablet_id()))) {
          LOG_WARN("fail to add tablet id", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_result(total_tablet_count, *result))) {
      LOG_WARN("fail to init result", K(ret));
    } else if (OB_FAIL(ObTableGroupExecuteService::start_trans(*multi_batch_ctx_))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(ObTableMultiBatchService::execute(*multi_batch_ctx_, *req, *result))) {
      LOG_WARN("fail to execute multi batch operation", K(ret));
    }

    // end trans
    int tmp_ret = OB_SUCCESS;
    bool is_rollback = ret != OB_SUCCESS;
    if (OB_TMP_FAIL(ObTableGroupExecuteService::end_trans(*multi_batch_ctx_, group_ctx_->create_cb_functor_, is_rollback))) {
      LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
    }
  }

  return ret;
}
