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
#include "observer/table/ob_table_move_response.h"
#include "ob_table_tenant_group.h"
#include "storage/memtable/ob_lock_wait_mgr.h"

using namespace oceanbase::observer;
using namespace oceanbase::transaction;

void __attribute__((weak)) request_finish_callback();

namespace oceanbase
{
namespace table
{
int ObTableGroupCommitCreateCbFunctor::init(ObTableGroup *group,
                                            bool add_failed_group,
                                            ObTableFailedGroups *failed_groups,
                                            ObTableGroupFactory<ObTableGroup> *group_factory,
                                            ObTableGroupOpFactory *op_factory)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_ISNULL(group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tablet_groups is null", K(ret));
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
                      *group_,
                      add_failed_group_,
                      failed_groups_,
                      group_factory_,
                      op_factory_);
      if (NULL != tmp_cb) {
          cb_ = tmp_cb;
      }
    }
  }
  return tmp_cb;
}

int ObTableGroupCommitEndTransCb::add_failed_groups()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(failed_groups_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed groups is null", K(ret));
  } else if (OB_FAIL(failed_groups_->add(&group_))) {
    LOG_WARN("fail to add failed group", K(ret), K(group_));
  }

  return ret;
}

int ObTableGroupCommitEndTransCb::response()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(group_factory_) || OB_ISNULL(op_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("factory is NULL", KP(group_factory_), KP(op_factory_));
  } else if (OB_FAIL(ObTableGroupExecuteService::response(group_,
                                                          *group_factory_,
                                                          *op_factory_))) {
    LOG_WARN("fail to response", K(ret), K(group_));
  }

  return ret;
}

int ObTableGroupCommitEndTransCb::response_failed_results(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(group_factory_) || OB_ISNULL(op_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("factory is NULL", KP(group_factory_), KP(op_factory_));
  } else if (OB_FAIL(ObTableGroupExecuteService::response_failed_results(ret_code,
                                                                         group_,
                                                                         *group_factory_,
                                                                         *op_factory_))) {
    LOG_WARN("fail to response", K(ret), K_(group));
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
      if (OB_FAIL(add_failed_groups())) {
        LOG_WARN("fail to add failed group", K(ret));
      }
    } else {
      // response failed result directly
      if (OB_FAIL(response_failed_results(cb_param))) {
        LOG_WARN("fail to response failed results", K(ret), K(cb_param));
      }
    }
  } else { // commit success
    if (OB_FAIL(response())) {
      LOG_WARN("fail to response", K(ret));
    }
  }

  this->destroy_cb_if_no_ref();
}

uint64_t ObTableGroupKey::hash() const
{
  int64_t hash_val = 0;
  uint64_t seed = 0;
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), seed);
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&schema_version_, sizeof(schema_version_), hash_val);
  if (ObTableOperationType::is_group_support_type(op_type_)) {
    if (ObTableGroupUtils::is_hybird_op(op_type_)) {
      ObTableOperationType::Type type = ObTableOperationType::Type::INVALID;
      hash_val = murmurhash(&type, sizeof(type), hash_val);
    } else {
      hash_val = murmurhash(&op_type_, sizeof(op_type_), hash_val);
    }
  } else {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport op type in group commit", K(ret), K(op_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "op type in group commit is");
  }
  return hash_val;
}

int ObTableGroupKey::deep_copy(ObIAllocator &allocator, const ObITableGroupKey &other)
{
  int ret = OB_SUCCESS;
  if (type_ != other.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(type_));
  } else {
    const ObTableGroupKey &other_key = static_cast<const ObTableGroupKey &>(other);
    ls_id_ = other_key.ls_id_;
    table_id_ = other_key.table_id_;
    schema_version_ = other_key.schema_version_;
    op_type_ = other_key.op_type_;
    is_insup_use_put_ = other_key.is_insup_use_put_;
  }
  return ret;
}

bool ObTableGroupKey::is_equal(const ObITableGroupKey &other) const
{
  bool is_equal = false;
  if (type_ == other.type_) {
    const ObTableGroupKey &other_key = static_cast<const ObTableGroupKey &>(other);
    is_equal = (ls_id_ == other_key.ls_id_) &&
              (table_id_ == other_key.table_id_) &&
              (schema_version_ == other_key.schema_version_) &&
              (op_type_ == other_key.op_type_) &&
              (is_insup_use_put_ == other_key.is_insup_use_put_);
  }

  return is_equal;
}

int ObTableOpProcessor::init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObITableOpProcessor::init(group_ctx, ops))) {
    LOG_WARN("fail to init processor", K(ret));
  } else if (OB_ISNULL(batch_ctx_ = OB_NEWx(ObTableBatchCtx, (&allocator_), allocator_, group_ctx.audit_ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc batch ctx", K(ret));
  } else if (ops->count() == 0 || OB_ISNULL(ops->at(0)) ||
        ops->at(0)->type() != ObTableGroupType::TYPE_TABLE_GROUP) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ops count is 0", K(ret));
  } else {
    ObTableOp *op = static_cast<ObTableOp *>(ops->at(0));
    is_get_ = op->op_.type() == ObTableOperationType::Type::GET;
    is_inited_ = true;
  }
  return ret;
}

int ObTableOpProcessor::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ops_) || OB_ISNULL(group_ctx_) || OB_ISNULL(batch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops_ or group_ctx_ is NULL", K(ret), KP(ops_), KP(group_ctx_), KP(batch_ctx_));
  } else if (is_get_) {
    if (OB_FAIL(execute_read())) {
      LOG_WARN("fail to execute read group", K(ret));
    }
  } else {
    if (OB_FAIL(execute_dml())) {
      LOG_WARN("fail to execute dml group", K(ret));
    }
  }
  return ret;
}

int ObTableOpProcessor::execute_dml()
{
  int ret = OB_SUCCESS;
  OpFixedArray batch_ops(allocator_);
  ResultFixedArray batch_result(allocator_);
  int64_t ops_count = ops_->count();
  if (OB_FAIL(batch_ops.init(ops_count))) {
    LOG_WARN("fail to init ops array", K(ret), K(ops_count));
  } else if (OB_FAIL(batch_result.init(ops_count))) {
    LOG_WARN("fail to init result array", K(ret), K(ops_count));
  } else {
    ObTableBatchCtx &batch_ctx = *batch_ctx_;
    if (OB_FAIL(init_batch_params(batch_ctx, batch_ops, batch_result))) {
      LOG_WARN("fail to init batch ctx", K(ret));
    } else if (OB_FAIL(init_table_ctx(batch_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(ObTableGroupExecuteService::start_trans(batch_ctx))) {
      LOG_WARN("fail to start trans", K(batch_ctx));
    }  else if (OB_FAIL(ObTableBatchService::execute(batch_ctx, batch_ops, batch_result))) {
      LOG_WARN("fail to execute batch operation", K(batch_ctx));
    } else if (OB_FAIL(dispatch_batch_result(batch_result))) {
      LOG_WARN("fail to dispatch batch result", K(ret));
    }
    // end trans
    int tmp_ret = OB_SUCCESS;
    bool is_rollback = (ret != OB_SUCCESS);
    if (OB_TMP_FAIL(ObTableGroupExecuteService::end_trans(batch_ctx, group_ctx_->create_cb_functor_, is_rollback))) {
      LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
    }
    LOG_DEBUG("[group commit debug] execute dml", K(ret), K(tmp_ret));
  }
  return ret;
}

int ObTableOpProcessor::execute_read()
{
  int ret = OB_SUCCESS;
  bool had_do_response = false;
  ObArenaAllocator tmp_allocator("TbGroupRead", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  OpFixedArray batch_ops(allocator_);
  ResultFixedArray batch_result(allocator_);
  int64_t ops_count = ops_->count();
  if (OB_FAIL(batch_ops.init(ops_count))) {
    LOG_WARN("fail to init ops array", K(ret), K(ops_count));
  } else if (OB_FAIL(batch_result.init(ops_count))) {
    LOG_WARN("fail to init result array", K(ret), K(ops_count));
  } else {
    ObTableBatchCtx &batch_ctx = *batch_ctx_;
    if (OB_FAIL(init_batch_params(batch_ctx, batch_ops, batch_result))) {
      LOG_WARN("fail to init batch ctx", K(ret));
    } else if (OB_FAIL(init_table_ctx(batch_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(ObTableGroupExecuteService::start_trans(batch_ctx))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx, batch_ops, batch_result))) {
      LOG_WARN("fail to execute batch operation", K(ret));
    } else if (OB_FAIL(dispatch_batch_result(batch_result))) {
      LOG_WARN("fail to dispatch batch result", K(ret));
    }

    // end trans
    int tmp_ret = OB_SUCCESS;
    bool is_rollback = ret != OB_SUCCESS;
    if (OB_TMP_FAIL(ObTableGroupExecuteService::end_trans(batch_ctx, nullptr, is_rollback))) {
      LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
    }
  }

  return ret;
}

int ObTableOpProcessor::init_batch_params(ObTableBatchCtx &batch_ctx,
                                          ObIArray<ObTableOperation> &batch_ops,
                                          ObIArray<ObTableOperationResult> &batch_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(group_ctx_) || OB_ISNULL(ops_) || ops_->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops is empty", K(ret), KP_(group_ctx), KP_(ops));
  } else {
    bool is_readonly = is_get_;
    batch_ctx.trans_param_ = group_ctx_->trans_param_;
    batch_ctx.is_atomic_ = true;
    batch_ctx.is_readonly_ = is_readonly;
    batch_ctx.is_same_properties_names_ = is_readonly; // get op in group commit use multi get default
    batch_ctx.consistency_level_ = table::ObTableConsistencyLevel::STRONG;
    batch_ctx.credential_ = &group_ctx_->credential_;
    for (int64_t i = 0; i < ops_->count() && OB_SUCC(ret); i++) {
      ObTableOp *single_op = static_cast<ObTableOp *>(ops_->at(i));
      if (OB_FAIL(batch_ops.push_back(single_op->op_))) {
        LOG_WARN("fail to push back table operation", K(single_op->op_), K(i));
      } else if (OB_FAIL(batch_ctx.tablet_ids_.push_back(single_op->tablet_id()))) {
        LOG_WARN("fail to push back tablet id", K(ret), K(single_op->tablet_id()), K(i));
      } else if (OB_FAIL(batch_result.push_back(single_op->result_))) {
        LOG_WARN("fail to push back table operation result", K(single_op->result_), K(i));
      }
    }
  }

  return ret;
}

int ObTableOpProcessor::init_table_ctx(ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = batch_ctx.tb_ctx_;
  ObTableOperationType::Type op_type = ObTableOperationType::Type::INVALID;
  ObTableOp *single_op = nullptr;
  bool is_skip_init_exec_ctx = false;
  if (OB_ISNULL(group_ctx_) || OB_ISNULL(ops_) || ops_->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops is empty", K(ret), KP_(group_ctx), KP_(ops));
  } else if (OB_ISNULL(single_op = static_cast<ObTableOp *>(ops_->at(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("single_op is null", K(ret));
  } else {
    // for inertUp op which can use put, we will execute it in a multi_put batch, and check has been done before enqueue
    op_type = single_op->op_.type() == ObTableOperationType::INSERT_OR_UPDATE && single_op->is_insup_use_put() ?
            ObTableOperationType::PUT : single_op->op_.type();
    batch_ctx.is_same_type_ = !ObTableGroupUtils::is_hybird_op(op_type);
    tb_ctx.set_entity(&single_op->op_.entity());
    tb_ctx.set_entity_type(group_ctx_->entity_type_);
    tb_ctx.set_operation_type(op_type);
    tb_ctx.set_ls_id(single_op->ls_id_);
    tb_ctx.set_schema_cache_guard(group_ctx_->schema_cache_guard_);
    tb_ctx.set_schema_guard(group_ctx_->schema_guard_);
    tb_ctx.set_simple_table_schema(group_ctx_->simple_schema_);
    tb_ctx.set_sess_guard(group_ctx_->sess_guard_);
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tb_ctx.init_common(*batch_ctx.credential_,
                                        single_op->tablet_id(),
                                        group_ctx_->timeout_ts_))) {
    LOG_WARN("fail to init table tb_ctx common part", K(ret),
              KPC(batch_ctx.credential_), K(group_ctx_->timeout_ts_));
  } else {
    switch (op_type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx.init_get())) {
          LOG_WARN("fail to init get tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx.init_put(true /* allow_insup */))) {
          LOG_WARN("fail to init put tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (OB_FAIL(tb_ctx.init_insert())) {
          LOG_WARN("fail to init insert tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(tb_ctx.init_delete())) {
          LOG_WARN("fail to init delete tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(tb_ctx.init_replace())) {
          LOG_WARN("fail to init replace tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::UPDATE:
      case ObTableOperationType::INSERT_OR_UPDATE:
      case ObTableOperationType::INCREMENT:
      case ObTableOperationType::APPEND:
        is_skip_init_exec_ctx = true;
        break; // do nothing
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", op_type);
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!is_skip_init_exec_ctx && OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec tb_ctx", K(ret), K(tb_ctx));
  }

  return ret;
}

int ObTableOpProcessor::dispatch_batch_result(ObIArray<ObTableOperationResult> &batch_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops is NULL", K(ret));
  } else if (batch_result.count() != ops_->count()) {
     ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch result count is not equal to ops count", K(ret), K(ops_->count()), K(batch_result.count()));
  } else {
    for (int64_t i = 0; i < ops_->count() && OB_SUCC(ret); i++) {
      ObTableOp *single_op = nullptr;
      if (OB_ISNULL(single_op = static_cast<ObTableOp *>(ops_->at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op is NULL", K(ret), K(i));
      } else {
        single_op->result_ = batch_result.at(i); // shallow copy
      }
    }
  }
  LOG_DEBUG("[group commit debug] batch result:", K(batch_result));
  return ret;
}

int ObTableGroupExecuteService::start_trans(ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = batch_ctx.tb_ctx_;
  ObTableTransParam *trans_param = batch_ctx.trans_param_;
  int64_t timeout_ts = tb_ctx.get_timeout_ts();
  bool need_global_snapshot = tb_ctx.need_dist_das();
  if (timeout_ts - ObClockGenerator::getClock() < 0) {
    timeout_ts = ObClockGenerator::getClock() + DEFAULT_TRANS_TIMEOUT;
  }

  if (OB_ISNULL(trans_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans param is null", K(ret));
  } else if (OB_FAIL(trans_param->init(batch_ctx.is_readonly_,
                                       batch_ctx.consistency_level_,
                                       tb_ctx.get_ls_id(),
                                       timeout_ts,
                                       need_global_snapshot))) {
    LOG_WARN("fail to init trans param", K(ret), K(batch_ctx));
  } else if (batch_ctx.is_readonly_) {
    if (OB_FAIL(ObTableTransUtils::init_read_trans(*trans_param))) {
      LOG_WARN("fail to init read trans", K(ret), KPC(trans_param));
    }
  } else { // other batch operation
    if (OB_FAIL(ObTableTransUtils::start_trans(*trans_param))) {
      LOG_WARN("fail to start trans", K(ret), KPC(trans_param));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(tb_ctx.init_trans(trans_param->trans_desc_, trans_param->tx_snapshot_))) {
    LOG_WARN("fail to init trans", K(ret));
  }
  return ret;
}

int ObTableGroupExecuteService::end_trans(const ObTableBatchCtx &batch_ctx,
                                          ObTableCreateCbFunctor *create_cb_functor,
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

int ObTableGroupExecuteService::response(ObTableGroup &group,
                                         ObTableGroupFactory<ObTableGroup> &group_factory,
                                         ObTableGroupOpFactory &op_factory)
{
  int ret = OB_SUCCESS;

  ObTableGroupMeta &group_meta = group.group_meta_;
  ObIArray<ObITableOp *> &ops = group.ops_;
  if (ops.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operation size", K(ret));
  } else {
    ObTableRpcResponseSender sender;
    ObITableOp *op = nullptr;
    ObITableResult *res = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); i++) {
      op = ops.at(i);
      rpc::ObRequest *req = nullptr;
       if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op is null", K(ret), K(i));
      } else if (OB_ISNULL(req = op->req_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("request is null", K(ret), K(i));
      } else if (OB_FAIL(op->get_result(res))) {
        LOG_WARN("fail to get result", K(ret));
      } else {
        int ret_code = res->get_errno();
        const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(req->get_packet());
        if (observer::ObTableRpcProcessorUtil::need_do_move_response(ret_code, *rpc_pkt)) { // rerouting pkg
          observer::ObTableMoveResponseSender move_sender(req, ret_code);
          if (OB_ISNULL(GCTX.schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("gctx schema_service is null", K(ret), K(i));
          } else if (OB_FAIL(move_sender.init(group_meta.table_id_, op->tablet_id(), *GCTX.schema_service_))) {
            LOG_WARN("fail to init move response sender", K(ret), K(op->tablet_id()), K_(group_meta.table_id));
          } else if (OB_FAIL(move_sender.response())) {
            LOG_WARN("fail to do move response", K(ret), K(i));
          }
          // if response rerouting fail, response the common failure packet
          if (OB_FAIL(ret)) {
            sender.set_req(req);
            sender.set_result(res);
            if (OB_FAIL(sender.response(ret_code))) {
              LOG_WARN("fail to send response", K(ret), KPC(op), K(i), K(ret_code));
            }
          }
        } else {
          sender.set_req(req);
          sender.set_result(res);
          if (OB_FAIL(sender.response(ret_code))) {
            LOG_WARN("fail to send response", K(ret), KPC(op), K(i), K(ret_code));
          }
        }
      }
    } // end for
  }
  // free ops
  for (int64_t i = 0; i < ops.count(); i++) {
    op_factory.free(ops.at(i));
  }
  // free group
  group_factory.free(&group);
  return ret;
}

int ObTableGroupExecuteService::response_failed_results(int ret_code,
                                                        ObTableGroup &group,
                                                        ObTableGroupFactory<ObTableGroup> &group_factory,
                                                        ObTableGroupOpFactory &op_factory)
{
  int ret = OB_SUCCESS;
  ObTableGroupMeta &group_meta = group.group_meta_;
  ObIArray<ObITableOp *> &ops = group.ops_;
  ObTableEntity result_entity;
  // 1. generate failed result
  for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); i++) {
    if (OB_ISNULL(ops.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table op is NULL", K(ret), K(i));
    } else {
      ops.at(i)->set_failed_result(ret_code, result_entity, group_meta.op_type_);
    }
  }
  // 2. response fail result
  if (OB_SUCC(ret)) {
    if (OB_FAIL(response(group, group_factory, op_factory))) {
      LOG_WARN("fail to response fail result", K(ret));
    }
  }
  return ret;
}

int ObTableGroupExecuteService::process_result(int ret_code,
                                               ObTableGroup &group,
                                               bool is_direct_execute,
                                               bool add_failed_group)
{
  int ret = OB_SUCCESS;
  ObTableGroupFactory<ObTableGroup> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObTableGroupOpFactory &op_factory = TABLEAPI_GROUP_COMMIT_MGR->get_op_factory();
  if (is_direct_execute) { // execute directly
    if (ret_code == OB_SUCCESS) {
      if (OB_FAIL(response(group, group_factory, op_factory))) {
        LOG_WARN("fail to response result", K(ret));
      }
    } else { // execute fail, need response fail result
      if (OB_FAIL(response_failed_results(ret_code, group, group_factory, op_factory))) {
        LOG_WARN("fail to response fail result", K(ret), K(ret_code));
      }
    }
  } else { // executed by group commit
    if (ret_code == OB_SUCCESS) {
      if (OB_FAIL(response(group, group_factory, op_factory))) {
        LOG_WARN("fail to response result", K(ret));
      }
    } else { // execute fail
      if (add_failed_group) { // add failed group
        ObTableFailedGroups &failed_groups = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();
        if (OB_FAIL(failed_groups.add(&group))) {
          LOG_WARN("fail to add failed group", K(ret), K(group));
        }
      } else { // need resonse fail result
        if (OB_FAIL(response_failed_results(ret_code, group, group_factory, op_factory))) {
          LOG_WARN("fail to response fail result", K(ret), K(ret_code));
        }
      }
    }
  }
  return ret;
}

int ObTableGroupExecuteService::execute(ObTableGroup &group, bool add_fail_group)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("TbGroupExec", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTableGroupCtx group_ctx(tmp_allocator);
  ObSchemaGetterGuard schema_guard;
  observer::ObReqTimeGuard req_timeinfo_guard_; // use for clear schema_cache_guard
  ObKvSchemaCacheGuard schema_cache_guard;
  ObTableApiSessGuard sess_guard;
  ObTableTransParam trans_param;
  ObITableOpProcessor *op_processor = nullptr;
  const schema::ObSimpleTableSchemaV2 *simple_schema = nullptr;
  if (!group.is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group is not inited", K(ret));
  } else if (OB_FAIL(ObTableGroupUtils::init_schema_cache_guard(group, schema_guard, schema_cache_guard, simple_schema))) {
    LOG_WARN("fail to init schema cache guard", K(ret), K(group));
  } else if (OB_FAIL(ObTableGroupUtils::init_sess_guard(group, sess_guard))) {
    LOG_WARN("fail to init sess guard", K(ret), K(group));
  } else {
    ObTableGroupFactory<ObTableGroup> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
    ObTableFailedGroups &failed_groups = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();
    ObTableGroupOpFactory &op_factory = TABLEAPI_GROUP_COMMIT_MGR->get_op_factory();
    ObTableGroupCommitCreateCbFunctor functor;
    group_ctx.group_type_ = group.group_meta_.type_;
    group_ctx.type_ = group.group_meta_.op_type_;
    group_ctx.table_id_ = group.group_meta_.table_id_;
    group_ctx.entity_type_ = group.group_meta_.entity_type_;
    group_ctx.credential_ = group.group_meta_.credential_;
    // when execute timeout groups or failed group, maybe the group timeout_ts is less than current timestamp
    // so we need to reset timeout_ts. Otherwise, we will fail to start the transaction
    int64_t timeout_ts = ObClockGenerator::getClock() + group.timeout_;
    group_ctx.timeout_ts_ = timeout_ts;
    group_ctx.trans_param_ = &trans_param;
    group_ctx.schema_guard_ = &schema_guard;
    group_ctx.simple_schema_ = simple_schema;
    group_ctx.schema_cache_guard_ = &schema_cache_guard;
    group_ctx.sess_guard_ = &sess_guard;
    group_ctx.create_cb_functor_ = &functor;
    if (OB_FAIL(GROUP_PROC_ALLOC[group_ctx.group_type_](tmp_allocator, op_processor))) {
      LOG_WARN("fail to alloc op processor", K(ret));
    } else if (OB_FAIL(functor.init(&group, add_fail_group, &failed_groups, &group_factory, &op_factory))) {
      LOG_WARN("fail to init group commit callback functor", K(ret));
    } else if (OB_FAIL(op_processor->init(group_ctx, &group.ops_))) {
      LOG_WARN("fail to init op processor", K(ret));
    } else if (OB_FAIL(op_processor->process())) {
      LOG_WARN("fail to process op", K(ret));
    }
  }

  // clear thread local variables used to wait in queue
  request_finish_callback();

  int tmp_ret = ret;
  // overwrite ret
  if (!trans_param.had_do_response_ && OB_FAIL(process_result(tmp_ret,
                                                              group,
                                                              false /* is_direct_execute */,
                                                              add_fail_group))) {
    LOG_WARN("fail to process result", K(ret), K(tmp_ret));
  } else {
    LOG_DEBUG("[group commit debug] process result", K(ret), K(tmp_ret),
                  KPC(group_ctx.trans_param_), K(add_fail_group), K(group_ctx));
  }
  if (OB_NOT_NULL(op_processor)) {
    op_processor->~ObITableOpProcessor();
    op_processor = nullptr;
  }
  LOG_DEBUG("[group commit debug] execute batch", K(ret), KPC(group_ctx.trans_param_), K(add_fail_group), K(group_ctx));
  return ret;
}

} // end namespace table
} // end namespace oceanbase
