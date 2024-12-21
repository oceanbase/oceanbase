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
#include "ob_table_group_common.h"
#include "ob_table_tenant_group.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/table/ob_table_session_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;
using namespace oceanbase::obsys;

namespace oceanbase
{
namespace table
{

int ObTableGroupMeta::deep_copy(ObIAllocator &allocator, const ObITableGroupMeta &other)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  if (other.type_ < ObTableGroupType::TYPE_INVALID || other.type_ >= TYPE_MAX || type_ != other.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is invalid", K(ret), K(type_), K(other.type_));
  } else {
    const ObTableGroupMeta& meta = static_cast<const ObTableGroupMeta&>(other);
    type_ = meta.type_;
    op_type_ = meta.op_type_;
    entity_type_ = meta.entity_type_;
    credential_ = meta.credential_;
    table_id_ = meta.table_id_;
  }
  return ret;
}

void ObTableGroupMeta::reset()
{
  ObITableGroupMeta::reset();
  op_type_ = ObTableOperationType::INVALID;
  entity_type_ = ObTableEntityType::ET_DYNAMIC;
  credential_.reset();
  table_id_ = OB_INVALID_ID;
}

int ObTableGroup::init(const ObTableGroupMeta &meta, ObIArray<ObITableOp *> &ops)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("group has been inited", K(ret));
  } else {
    group_meta_ = meta;
    for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
      ObITableOp *op = ops.at(i);
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("single op is NULL", K(ret), K(i));
      } else if (OB_FAIL(ops_.push_back(op))) {
        LOG_WARN("fail to push back op", K(ret), K(i), KPC(op));
      } else {
        if (timeout_ts_ == 0 || timeout_ts_ > op->timeout_ts_) {
          timeout_ts_ = op->timeout_ts_;
          timeout_ = op->timeout_ts_ - ObClockGenerator::getClock();
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableGroup::init(const ObTableGroupMeta &meta, ObITableOp *op)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("group has been inited", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is NULL", K(ret));
  } else if (OB_FAIL(ops_.push_back(op))) {
    LOG_WARN("fail to push back op", K(ret), KPC(op));
  } else {
    group_meta_ = meta;
    timeout_ts_ = op->timeout_ts_;
    timeout_ = op->timeout_ts_ - ObClockGenerator::getClock();

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableGroup::init(const ObTableGroupCtx &ctx, ObIArray<ObITableOp *> &ops)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("group has been inited", K(ret));
  } else {
    group_meta_.type_ = ctx.group_type_;
    group_meta_.op_type_ = ctx.type_;
    group_meta_.entity_type_ = ctx.entity_type_;
    group_meta_.credential_ = ctx.credential_;
    group_meta_.table_id_ = ctx.table_id_;
    for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
      ObITableOp *op = ops.at(i);
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("single op is NULL", K(ret), K(i));
      } else if (OB_FAIL(ops_.push_back(op))) {
        LOG_WARN("fail to push back op", K(ret), K(i), KPC(op));
      } else {
        if (timeout_ts_ == 0 || timeout_ts_ > op->timeout_ts_) {
          timeout_ts_ = op->timeout_ts_;
          timeout_ = op->timeout_ts_ - ObClockGenerator::getClock();
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableGroup::add_op(ObITableOp *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is NULL", K(ret));
  } else if (OB_FAIL(ops_.push_back(op))) {
    LOG_WARN("fail to push back op", K(ret));
  } else {
    // update timeout and timeout_ts
    if (timeout_ts_ == 0 || timeout_ts_ > op->timeout_ts_) {
      timeout_ts_ = op->timeout_ts_;
      timeout_ = op->timeout_ts_ - ObClockGenerator::getClock();
    }
  }
  return ret;
}

int ObTableFailedGroups::init() {
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail groups has been inited", K(ret));
  } else {
    group_info_.client_addr_ = GCTX.self_addr();
    group_info_.tenant_id_ = MTL_ID();
    group_info_.table_id_ = OB_INVALID_ID;
    group_info_.ls_id_ = ObLSID::INVALID_LS_ID;
    group_info_.schema_version_ = OB_INVALID_VERSION;
    group_info_.group_type_ = ObTableGroupInfo::GroupType::FAIL;
    group_info_.queue_size_ = count();
    group_info_.batch_size_ =  TABLEAPI_GROUP_COMMIT_MGR->get_group_size();
    group_info_.gmt_created_ = ObClockGenerator::getClock();
    group_info_.gmt_modified_ = group_info_.gmt_created_;
    is_inited_ = true;
  }

  return ret;
}

int ObTableFailedGroups::add(ObTableGroup *group)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (OB_FAIL(failed_ops_.push_back(group))) {
    LOG_WARN("fail to push back group", K(ret));
  } else {
    group_info_.gmt_modified_ = ObClockGenerator::getClock();
  }
  return ret;
}

ObTableGroup* ObTableFailedGroups::get()
{
  int ret = OB_SUCCESS;
  ObTableGroup *group = nullptr;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (OB_FAIL(failed_ops_.pop_front(group))) {
    LOG_WARN("fail to pop front group", K(ret));
  } else {
    group_info_.gmt_modified_ = ObClockGenerator::getClock();
  }
  return group;
}

int ObTableFailedGroups::construct_trigger_requests(ObIAllocator &request_allocator,
                                                    ObIArray<ObTableGroupTriggerRequest*> &requests)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  FOREACH_X(tmp_node, failed_ops_, OB_SUCC(ret)) {
    ObTableGroup *group = *tmp_node;
    ObTableGroupTriggerRequest *request = OB_NEWx(ObTableGroupTriggerRequest, &request_allocator);
    if (OB_ISNULL(request)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc request", K(ret));
    } else if (OB_ISNULL(group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group is NULL", K(ret));
    } else if (OB_FAIL(request->init(group->group_meta_.credential_))) {
      LOG_WARN("fail to init request", K(ret));
    } else if (OB_FAIL(requests.push_back(request))) {
      LOG_WARN("fail to push back request");
    }
  }

  return ret;
}

int ObTableExpiredGroups::construct_trigger_requests(ObIAllocator &request_allocator,
                                                     ObIArray<ObTableGroupTriggerRequest*> &requests)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  FOREACH_X(tmp_node, expired_groups_, OB_SUCC(ret)) {
    ObITableGroupValue *group = *tmp_node;
    ObTableGroupTriggerRequest *request = nullptr;
    if (OB_ISNULL(group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expired group is null", K(ret));
    } else if (OB_ISNULL(request = OB_NEWx(ObTableGroupTriggerRequest, &request_allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc request", K(ret));
    } else {
      ObTableGroupMeta &group_meta = static_cast<ObTableGroupValue*>(group)->group_meta_;
      const ObTableApiCredential &credential = group_meta.credential_;
      if (OB_FAIL(request->init(group_meta.credential_))) {
        LOG_WARN("fail to init request", K(ret));
      } else if (OB_FAIL(requests.push_back(request))) {
        LOG_WARN("fail to push back request");
      }
    }
  }

  return ret;
}

int ObTableGroupValue::init(const ObTableGroupCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObMemAttr memattr(MTL_ID(), "ObTbGropQue");
  if (!is_inited_) {
    type_ = ctx.group_type_;
    if (OB_FAIL(ObITableGroupValue::init(ctx))) {
      LOG_WARN("fail to init group value", K(ret));
    } else if (OB_FAIL(queue_.init(MAX_SLOT_SIZE, &allocator_, memattr))) {
      LOG_WARN("fail to init queues", K(ret));
    } else {
      // init group_meta
      group_meta_.type_ = type_;
      group_meta_.op_type_ = ctx.type_;
      group_meta_.entity_type_ = ctx.entity_type_;
      group_meta_.credential_ = ctx.credential_;
      group_meta_.table_id_ = ctx.table_id_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableGroupValue::get_executable_group(int64_t batch_size, ObIArray<ObITableOp *> &ops, bool check_queue_size)
{
  int ret = OB_SUCCESS;
  if (check_queue_size && queue_.get_curr_total() < batch_size * EXECUTABLE_BATCH_SIZE_FACTOR) {
    // do nothing
  } else {
    ObITableOp *op = nullptr;
    bool is_get_next = true;
    for (int i = 0; i < batch_size && OB_SUCC(ret) && is_get_next; i++) {
      if (OB_FAIL(queue_.pop(op))) {
        if (ret != OB_ENTRY_NOT_EXIST) {
          int64_t queue_curr_size = queue_.get_curr_total();
          LOG_WARN("fail to pop op from queue", K(ret), K(queue_curr_size), K(i));
        } else {
          is_get_next = false;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(ops.push_back(op))) {
        LOG_WARN("fail to push back op", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && ops.count() > 0) {
      group_info_.gmt_modified_ = common::ObTimeUtility::fast_current_time();
    }
    LOG_DEBUG("[group commit] get executable batch size", K(ret), K(group_meta_), K(batch_size), K(check_queue_size));
  }

  return ret;
}

int ObTableGroupValue::add_op_to_group(ObITableOp *op)
{
  int ret = OB_SUCCESS;
  bool add_op_success = false;
  int64_t cur_ts = common::ObTimeUtility::fast_current_time();
  while(OB_SUCC(ret) && !add_op_success) {
    if (OB_FAIL(queue_.push(op))) {
      if (ret != OB_SIZE_OVERFLOW) {
        int64_t queue_size = queue_.get_curr_total();
        LOG_WARN("fail to push op to queue", K(queue_size), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      add_op_success = true;
      group_info_.gmt_modified_ = cur_ts;
    }
    if (OB_SUCC(ret) && !add_op_success) {
      int64_t op_timeout = op->timeout_ts_ - cur_ts;
      if (op_timeout <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("the adding operation is timeout", K(ret), K(cur_ts), K(op_timeout));
      }
    }
  }
  return ret;
}

int ObTableGroupTriggerRequest::init(const ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = credential.tenant_id_;
  int64_t pos = 0;

  if (OB_FAIL(serialization::encode(credential_buf_,
                                    ObTableApiCredential::CREDENTIAL_BUF_SIZE,
                                    pos,
                                    credential))) {
    LOG_WARN("failed to serialize credential", K(ret), K(pos), K(credential));
  } else {
    op_request_.credential_.assign_ptr(credential_buf_, static_cast<int32_t>(pos));
    op_request_.table_operation_.set_type(ObTableOperationType::Type::TRIGGER);
    op_request_.table_operation_.set_entity(&request_entity_);
    is_inited_ = true;
  }

  return ret;
}

int ObTableGroupUtils::init_sess_guard(ObTableGroup &group,
                                       ObTableApiSessGuard &sess_guard)
{
  int ret = OB_SUCCESS;
  ObTableApiCredential &credential = group.group_meta_.credential_;

  if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->get_sess_info(credential, sess_guard))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
  }

  return ret;
}

int ObTableGroupUtils::init_schema_cache_guard(const ObTableGroup &group,
                                               ObSchemaGetterGuard &schema_guard,
                                               ObKvSchemaCacheGuard &cache_guard,
                                               const schema::ObSimpleTableSchemaV2 *&simple_schema)
{
  int ret = OB_SUCCESS;
  const ObTableApiCredential &credential = group.group_meta_.credential_;
  uint64_t tenant_id = credential.tenant_id_;
  uint64_t table_id = group.group_meta_.table_id_;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                          table_id,
                                                          simple_schema))) {
    LOG_WARN("fail to get simple schema", K(ret), K(tenant_id), K(group.group_meta_));
  } else if (OB_ISNULL(simple_schema)) {
    ret = OB_SCHEMA_ERROR; // let client refresh table
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(group.group_meta_));
  } else if (simple_schema->is_in_recyclebin()) {
    ret = OB_SCHEMA_ERROR; // let client refresh table
    LOG_WARN("table not exist, is in recyclebin", K(ret), K(tenant_id), K(group.group_meta_));
  } else if (OB_FAIL(cache_guard.init(tenant_id, table_id, simple_schema->get_schema_version(), schema_guard))) {
    LOG_WARN("fail to init schema cache guard", K(ret));
  }

  return ret;
}

int ObTableGroupUtils::trigger(const ObTableGroupTriggerRequest &request)
{
  int ret = OB_SUCCESS;
  ObTableEntity result_entity;
  ObTableGroupTriggerResult result;
  result.set_entity(&result_entity);
  ObTableRpcProxy &proxy = OBSERVER.get_table_rpc_proxy();

  if (!request.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trigger request not init", K(ret));
  } else if (OB_FAIL(proxy.to(GCTX.self_addr())
                          .timeout(DEFAULT_TIMEOUT_US)
                          .by(MTL_ID())
                          .execute(request.op_request_, result))) {
    LOG_WARN("fail to send trigger request", K(ret), K(request));
  } else if (OB_FAIL(result.get_errno())) {
    LOG_WARN("fail to execute trigger request", K(ret), K(request), K(result));
  } else {
    LOG_DEBUG("trigger group commit successfully", K(ret), K(result));
  }

  return ret;
}

bool ObTableGroupUtils::is_group_commit_config_enable()
{
  return TABLEAPI_SESS_POOL_MGR->get_kv_group_commit_batch_size() > 1;
}

bool ObTableGroupUtils::is_group_commit_config_enable(ObTableOperationType::Type op_type)
{
  bool bret = false;
  int64_t batch_size = TABLEAPI_SESS_POOL_MGR->get_kv_group_commit_batch_size();
  ObTableGroupRwMode mode = TABLEAPI_SESS_POOL_MGR->get_group_rw_mode();
  if (batch_size > 1) {
    if (mode == ObTableGroupRwMode::ALL) { // 'ALL'
      bret = true;
    } else if (mode == ObTableGroupRwMode::READ && is_read_op(op_type)) {
      bret = true;
    } else if (mode == ObTableGroupRwMode::WRITE == 0 && is_write_op(op_type)) {
      bret = true;
    }
  }
  return bret;
}

bool ObTableGroupUtils::is_group_commit_enable(ObTableOperationType::Type op_type)
{
  bool bret = false;
  if (ObTableOperationType::is_group_support_type(op_type) && is_group_commit_config_enable(op_type)) {
    TABLEAPI_GROUP_COMMIT_MGR->get_ops_counter().inc(op_type); // statistics OPS
    if (!TABLEAPI_GROUP_COMMIT_MGR->is_group_commit_disable()) {
      bret = true;
    }
  }
  return bret;
}

} // end namespace table
} // end namespace oceanbase
