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
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace table
{

ObTableGroupCommitKey::ObTableGroupCommitKey(ObLSID ls_id,
                                             ObTabletID tablet_id,
                                             ObTableID table_id,
                                             int64_t schema_version,
                                             ObTableOperationType::Type op_type)
    : ls_id_(ls_id),
      tablet_id_(tablet_id),
      table_id_(table_id),
      schema_version_(schema_version),
      op_type_(op_type)
{
  uint64_t seed = 0;
  hash_ = murmurhash(&ls_id_, sizeof(ls_id_), seed);
  hash_ = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_);
  hash_ = murmurhash(&table_id_, sizeof(table_id_), hash_);
  hash_ = murmurhash(&schema_version_, sizeof(schema_version_), hash_);
  hash_ = murmurhash(&op_type_, sizeof(op_type_), hash_);
}

int ObTableGroupCommitOps::init(ObTableApiCredential credential,
                                const ObLSID &ls_id,
                                uint64_t table_id,
                                ObTabletID tablet_id,
                                ObTableEntityType entity_type,
                                int64_t timeout_ts)
{
  int ret = OB_SUCCESS;

  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id or tablet id is invalid", K(ret), K(ls_id), K(tablet_id));
  } else {
    credential_ = credential;
    ls_id_ = ls_id;
    table_id_ = table_id;
    tablet_id_ = tablet_id;
    entity_type_ = entity_type;
    timeout_ts_ = timeout_ts;
    is_inited_ = true;
  }

  return ret;
}

int ObTableGroupCommitOps::get_ops(ObIArray<ObTableOperation> &ops)
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitSingleOp *op = nullptr;

  for (int64_t i = 0; i < ops_.count() && OB_SUCC(ret); i++) {
    op = ops_.at(i);
    if (OB_NOT_NULL(op) && OB_FAIL(ops.push_back(op->op_))) {
      LOG_WARN("fail to push back op", K(ret), KPC(op));
    }
  }

  return ret;
}

int ObTableGroupCommitOps::add_op(ObTableGroupCommitSingleOp *op)
{
  int ret = OB_SUCCESS;
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("group is not init", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", K(ret));
  } else if (!op->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op is invalid", K(ret), KPC(op));
  } else if (ops_.push_back(op)) {
    LOG_WARN("fail to add op to op list", K(ret));
  } else if (cur_ts >= op->timeout_ts_) {
    ret = OB_TIMEOUT;
    LOG_WARN("op is timeout", K(ret), K(cur_ts), K(op->timeout_ts_));
  } else {
    int64_t op_timeout = op->timeout_ts_ - cur_ts;
    if (op_timeout < timeout_ || timeout_ == 0) { // timeout_ = 0 means the first op
      timeout_ = op_timeout;
      timeout_ts_ = cur_ts + (timeout_ / 2); // Use half the timeout time set by the user
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
    } else if (OB_FAIL(request->init(group->credential_))) {
      LOG_WARN("fail to init request", K(ret));
    } else if (OB_FAIL(requests.push_back(request))) {
      LOG_WARN("fail to push back request");
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
  ObTableApiCredential &credential = group.credential_;

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
  const ObTableApiCredential &credential = group.credential_;
  uint64_t tenant_id = credential.tenant_id_;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                          group.table_id_,
                                                          simple_schema))) {
    LOG_WARN("fail to get simple schema", K(ret), K(tenant_id), K(group.table_id_));
  } else if (OB_ISNULL(simple_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(group.table_id_));
  } else if (OB_FAIL(cache_guard.init(tenant_id, group.table_id_, simple_schema->get_schema_version(), schema_guard))) {
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

} // end namespace table
} // end namespace oceanbase
