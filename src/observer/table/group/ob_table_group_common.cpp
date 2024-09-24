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

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;
using namespace oceanbase::obsys;

namespace oceanbase
{
namespace table
{

int ObTableGroupCommitKey::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    uint64_t seed = 0;
    hash_ = murmurhash(&ls_id_, sizeof(ls_id_), seed);
    hash_ = murmurhash(&table_id_, sizeof(table_id_), hash_);
    hash_ = murmurhash(&schema_version_, sizeof(schema_version_), hash_);
    if (is_fail_group_key_) {
      is_inited_ = true;
    } else if (ObTableOperationType::is_group_support_type(op_type_)) {
      if (ObTableGroupUtils::is_hybird_op(op_type_)) {
        ObTableOperationType::Type type = ObTableOperationType::Type::INVALID;
        hash_ = murmurhash(&type, sizeof(type), hash_);
      } else {
        hash_ = murmurhash(&op_type_, sizeof(op_type_), hash_);
      }
      is_inited_ = true;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupport op type in group commit", K(ret), K(op_type_), K(is_fail_group_key_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "op type in group commit is");
    }
  }
  return ret;
}

int ObTableGroupCommitOps::get_ops(ObIArray<ObTableOperation> &ops)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < ops_.count() && OB_SUCC(ret); i++) {
    ObTableGroupCommitSingleOp *op = ops_.at(i);
    if (OB_NOT_NULL(op) && OB_FAIL(ops.push_back(op->op_))) {
      LOG_WARN("fail to push back op", K(ret), KPC(op));
    }
  }

  return ret;
}

int ObTableGroupCommitOps::init(const ObTableGroupMeta &meta, ObIArray<ObTableGroupCommitSingleOp *> &ops)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_FAIL(meta_.init(meta))) {
      LOG_WARN("fail to init meta", K(ret), K(meta));
    } else {
      tablet_ids_.reuse();
      for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
        ObTableGroupCommitSingleOp *op = ops.at(i);
        if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("single op is NULL", K(ret), K(i));
        } else if (OB_FAIL(ops_.push_back(op))) {
          LOG_WARN("fail to push back op", K(ret), K(i), KPC(op));
        } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id_))) {
          LOG_WARN("fail to push back tablet_id", K(ret), K(i), K(op->tablet_id_));
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
  }

  return ret;
}

int ObTableGroupCommitOps::init(const ObTableGroupMeta &meta,
                                ObTableGroupCommitSingleOp *op,
                                int64_t timeout,
                                int64_t timeout_ts)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_FAIL(meta_.init(meta))) {
      LOG_WARN("fail to init meta", K(ret), K(meta));
    } else if (OB_ISNULL(op)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("op is null", K(ret));
    } else if (OB_FAIL(ops_.push_back(op))) {
      LOG_WARN("fail to push back op", K(ret));
    } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id_))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(op->tablet_id_));
    } else {
      meta_.is_same_type_ = false;
      timeout_ = timeout;
      timeout_ts_ = timeout_ts;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableFailedGroups::construct_trigger_requests(ObIAllocator &request_allocator,
                                                    ObIArray<ObTableGroupTriggerRequest*> &requests)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  FOREACH_X(tmp_node, failed_ops_, OB_SUCC(ret)) {
    ObTableGroupCommitOps *group = *tmp_node;
    ObTableGroupTriggerRequest *request = OB_NEWx(ObTableGroupTriggerRequest, &request_allocator);
    if (OB_ISNULL(request)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc request", K(ret));
    } else if (OB_FAIL(request->init(group->meta_.credential_))) {
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
    ObTableLsGroup *group = *tmp_node;
    ObTableGroupTriggerRequest *request = nullptr;
    if (OB_ISNULL(group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expired group is null", K(ret));
    } else if (OB_ISNULL(request = OB_NEWx(ObTableGroupTriggerRequest, &request_allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc request", K(ret));
    } else if (OB_FAIL(request->init(group->meta_.credential_))) {
      LOG_WARN("fail to init request", K(ret));
    } else if (OB_FAIL(requests.push_back(request))) {
      LOG_WARN("fail to push back request");
    }
  }

  return ret;
}

int ObTableGroupMeta::init(bool is_get,
                           bool is_same_type,
                           const ObTableApiCredential &credential,
                           const ObLSID &ls_id,
                           uint64_t table_id,
                           ObTableEntityType entity_type)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (!ls_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls id is invalid", K(ret), K(ls_id));
    } else {
      is_get_ = is_get;
      is_same_type_ = is_same_type;
      credential_ = credential;
      ls_id_ = ls_id;
      table_id_ = table_id;
      entity_type_ = entity_type;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableGroupMeta::init(const ObTableGroupMeta &other)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(this != &other)) {
    if (!other.ls_id_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls id is invalid", K(ret), K(other));
    } else {
      credential_ = other.credential_;
      ls_id_ = other.ls_id_;
      table_id_ = other.table_id_;
      entity_type_ = other.entity_type_;
      is_get_ = other.is_get_;
      is_same_type_ = other.is_same_type_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableLsGroup::init(const ObTableApiCredential &credential,
                         const ObLSID &ls_id,
                         uint64_t table_id,
                         ObTableEntityType entity_type,
                         ObTableOperationType::Type op_type)
{
  int ret = OB_SUCCESS;
  ObMemAttr memattr(MTL_ID(), "ObTbLsGropQue");
  if (!is_inited_) {
    bool is_get = (op_type == ObTableOperationType::Type::GET);
    bool is_same_type = !ObTableGroupUtils::is_hybird_op(op_type);
    if (OB_FAIL(meta_.init(is_get, is_same_type, credential, ls_id, table_id, entity_type))) {
      LOG_WARN("fail to init meta infomation", K(ret), K(credential), K(ls_id), K(table_id), K(entity_type));
    } else if (OB_FAIL(queue_.init(MAX_SLOT_SIZE, &allocator_, memattr))) {
      LOG_WARN("fail to init queues", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableLsGroup::get_executable_batch(int64_t batch_size, ObIArray<ObTableGroupCommitSingleOp*> &batch_ops, bool check_queue_size)
{
  int ret = OB_SUCCESS;
  if (check_queue_size && queue_.get_curr_total() < batch_size * EXECUTABLE_BATCH_SIZE_FACTOR) {
    // do nothing
  } else {
    ObTableGroupCommitSingleOp *op = nullptr;
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
      } else if (OB_FAIL(batch_ops.push_back(op))) {
        LOG_WARN("fail to push back op", K(ret), K(i));
      }
    }
    LOG_DEBUG("[group commit] get executable batch size", K(ret), K(meta_), K(batch_ops.count()), K(batch_size), K(check_queue_size));
  }

  return ret;
}

int ObTableLsGroup::add_op_to_queue(ObTableGroupCommitSingleOp *op, bool &add_op_success)
{
  int ret = OB_SUCCESS;
  add_op_success = false;
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
    }

    if (OB_SUCC(ret) && !add_op_success) {
      int64_t cur_ts = common::ObTimeUtility::fast_current_time();
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

int ObTableGroupUtils::init_sess_guard(ObTableGroupCommitOps &group,
                                       ObTableApiSessGuard &sess_guard)
{
  int ret = OB_SUCCESS;
  ObTableApiCredential &credential = group.meta_.credential_;

  if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->get_sess_info(credential, sess_guard))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
  }

  return ret;
}

int ObTableGroupUtils::init_schema_cache_guard(const ObTableGroupCommitOps &group,
                                               ObSchemaGetterGuard &schema_guard,
                                               ObKvSchemaCacheGuard &cache_guard,
                                               const schema::ObSimpleTableSchemaV2 *&simple_schema)
{
  int ret = OB_SUCCESS;
  const ObTableApiCredential &credential = group.meta_.credential_;
  uint64_t tenant_id = credential.tenant_id_;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                          group.meta_.table_id_,
                                                          simple_schema))) {
    LOG_WARN("fail to get simple schema", K(ret), K(tenant_id), K(group.meta_));
  } else if (OB_ISNULL(simple_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(group.meta_));
  } else if (OB_FAIL(cache_guard.init(tenant_id, group.meta_.table_id_, simple_schema->get_schema_version(), schema_guard))) {
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
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  return tenant_config.is_valid() && (tenant_config->kv_group_commit_batch_size > 1);
}

bool ObTableGroupUtils::is_group_commit_config_enable(ObTableOperationType::Type op_type)
{
  bool bret = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    int64_t batch_size = tenant_config->kv_group_commit_batch_size;
    ObString group_rw_mode = tenant_config->kv_group_commit_rw_mode.get_value_string();
    if (batch_size > 1) {
      if (group_rw_mode.case_compare("all") == 0) { // 'ALL'
        bret = true;
      } else if (group_rw_mode.case_compare("read") == 0 && is_read_op(op_type)) {
        bret = true;
      } else if (group_rw_mode.case_compare("write") == 0 && is_write_op(op_type)) {
        bret = true;
      }
    }
  }
  return bret;
}

int ObTableLsGroupInfo::init(ObTableGroupCommitKey &commit_key)
{
  int ret = OB_SUCCESS;
  bool is_read = false;
  client_addr_ = GCTX.self_addr();
  tenant_id_ = MTL_ID();
  table_id_ = commit_key.table_id_;
  ls_id_ = commit_key.ls_id_;
  schema_version_ = commit_key.schema_version_;
  switch (commit_key.op_type_) {
    case ObTableOperationType::Type::PUT:
      group_type_ = GroupType::PUT;
      break;
    case ObTableOperationType::Type::GET:
      group_type_ = GroupType::GET;
      is_read = true;
      break;
    case ObTableOperationType::Type::INSERT:
      group_type_ = GroupType::INSERT;
      break;
    case ObTableOperationType::Type::DEL:
      group_type_ = GroupType::DEL;
      break;
    case ObTableOperationType::Type::REPLACE:
      group_type_ = GroupType::REPLACE;
      break;
    case ObTableOperationType::Type::INVALID:
      group_type_ = GroupType::FAIL;
      break;
    default:
      group_type_ = GroupType::HYBIRD;
      break;
  }
  if (is_normal_group()) {
    queue_size_ = 0;
  } else if (is_fail_group()) {
    queue_size_ = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups().count();
  }
  batch_size_ = TABLEAPI_GROUP_COMMIT_MGR->get_group_size(is_read);
  gmt_created_ = ObClockGenerator::getClock();
  gmt_modified_ = gmt_created_;
  return ret;
}

const char* ObTableLsGroupInfo::get_group_type_str()
{
  const char* group_type_str = "UNKNOWN";
  switch(group_type_) {
    case GroupType::PUT:
      group_type_str = "PUT";
      break;
    case GroupType::GET:
      group_type_str = "GET";
      break;
    case GroupType::INSERT:
      group_type_str = "INSERT";
      break;
    case GroupType::REPLACE:
      group_type_str = "REPLACE";
      break;
    case GroupType::DEL:
      group_type_str = "DELETE";
      break;
    case GroupType::HYBIRD:
      group_type_str = "HYBIRD";
      break;
    case GroupType::FAIL:
      group_type_str = "FAIL";
      break;
    default:
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "unknown group type", K(group_type_));
      break;
  }
  return group_type_str;
}

} // end namespace table
} // end namespace oceanbase
