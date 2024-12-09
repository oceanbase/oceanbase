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
#include "ob_net_queue_traver.h"
#define USING_LOG_PREFIX RS
namespace oceanbase
{
namespace rpc
{
int ObNetQueueTraver::traverse(ObINetTraverProcess &process)
{
  int ret = OB_SUCCESS;
  oceanbase::omt::TenantIdList tenant_list;
  GCTX.omt_->get_tenant_ids(tenant_list);
  oceanbase::omt::ObTenant *tenant_ptr = nullptr;
  common::ObLDHandle handle;
  for (int i = 0; OB_SUCC(ret) && i < tenant_list.size(); i++) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(GCTX.omt_->get_tenant_with_tenant_lock(tenant_list[i], handle, tenant_ptr))) {
      LOG_WARN("get tenant failed", K(tmp_ret), K(ret), K(tenant_list[i]));
    } else if (OB_TMP_FAIL(traverse_one_tenant(tenant_ptr, process))) {
      LOG_WARN("traverse one tenant failed", K(tmp_ret), K(ret), K(tenant_list[i]));
    }

    if (OB_NOT_NULL(tenant_ptr)) {
      tenant_ptr->unlock(handle);
      tenant_ptr = nullptr;
    }
  }
  return ret;
}
int ObNetQueueTraver::traverse_one_tenant(oceanbase::omt::ObTenant *tenant_ptr, ObINetTraverProcess &process)
{
  int ret = OB_SUCCESS;
  // traverse all tenant queue, types are as follow:
  // 1、tenant req queue, multi_level_queue
  // 2、group req queue, multi_level_queue
  oceanbase::omt::ObResourceGroupNode *iter = nullptr;
  oceanbase::omt::ObResourceGroup *group = nullptr;
  int32_t group_id = 0;  // default group id for mysql request without resource_mgr is 0.
  if (OB_ISNULL(tenant_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("tenant_ptr is nullptr", K(ret));
  } else if (OB_FAIL(traverse_one_tenant_queue(tenant_ptr->get_req_queue(), tenant_ptr->get_multi_level_queue(), group_id, process))) {  // 1、tenant req queue
    LOG_ERROR("traverse tenant's req queue failed", K(ret));
  } else {
    oceanbase::omt::GroupMap& group_map = tenant_ptr->get_group_map();
    // 2、group req queue
    while (OB_NOT_NULL(iter = group_map.quick_next(iter))) {
      group = static_cast<oceanbase::omt::ObResourceGroup *>(iter);
      group_id = group->get_group_id();
      if (OB_FAIL(traverse_one_tenant_group_queue(group->get_req_queue(), group->get_multi_level_queue(), group_id, process))) {
        LOG_ERROR("traverse tenant's group queue failed", K(ret));
      }
    }  // group_map
  }
  return ret;
}
int ObNetQueueTraver::traverse_one_tenant_queue(oceanbase::omt::ReqQueue &tenant_req_queue, oceanbase::omt::ObMultiLevelQueue *tenant_multi_level_queue, int32_t group_id, ObINetTraverProcess &process)
{
  int ret = OB_SUCCESS;
  // normal queue
  int64_t prio_cnt = tenant_req_queue.get_prio_cnt();
  for (int64_t i = 0; OB_SUCC(ret) && i < prio_cnt; i++) {
    ObLinkQueue *link_queue = tenant_req_queue.get_queue(i);
    if (OB_ISNULL(link_queue)) {
      ret = OB_NULL_CHECK_ERROR;
      LOG_ERROR("link_queue is nullptr", K(ret));
    } else if (OB_FAIL(traverse_one_tenant_one_link_queue(link_queue, group_id, process))) {
      LOG_ERROR("traverse one tenant one queue failed", K(ret));
    }
  }
  // multi level queue
  if (OB_ISNULL(tenant_multi_level_queue)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("tenant_req_queue is nullptr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MULTI_LEVEL_QUEUE_SIZE; i++) {
      common::ObPriorityQueue<1>* pq = tenant_multi_level_queue->get_pq_queue(i);
      if (OB_ISNULL(pq)) {
        ret = OB_NULL_CHECK_ERROR;
        LOG_ERROR("pq is nullptr", K(ret));
      } else {
        int64_t multi_prio_cnt = pq->get_prio_cnt();
        for (int64_t j = 0; OB_SUCC(ret) && j < multi_prio_cnt; j++) {
          ObLinkQueue *link_queue = pq->get_queue(j);
          if (OB_ISNULL(link_queue)) {
            ret = OB_NULL_CHECK_ERROR;
            LOG_ERROR("link_queue is nullptr", K(ret));
          } else if (OB_FAIL(traverse_one_tenant_one_link_queue(link_queue, group_id, process))) {
            LOG_ERROR("traverse one tenant one queue failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int ObNetQueueTraver::traverse_one_tenant_group_queue(TenantReqQueue &tenant_group_queue, oceanbase::omt::ObMultiLevelQueue *tenant_multi_level_queue, int32_t group_id, ObINetTraverProcess &process)
{
  int ret = OB_SUCCESS;
  // normal queue
  int64_t prio_cnt = tenant_group_queue.get_prio_cnt();
  for (int64_t i = 0; OB_SUCC(ret) && i < prio_cnt; i++) {
    ObLinkQueue *link_queue = tenant_group_queue.get_queue(i);
    if (OB_ISNULL(link_queue)) {
      ret = OB_NULL_CHECK_ERROR;
      LOG_ERROR("link_queue is nullptr", K(ret));
    } else if (OB_FAIL(traverse_one_tenant_one_link_queue(link_queue, group_id, process))) {
      LOG_ERROR("traverse one tenant one queue failed", K(ret));
    }
  }
  // multi level queue
  if (OB_ISNULL(tenant_multi_level_queue)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("tenant_req_queue is nullptr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MULTI_LEVEL_QUEUE_SIZE; i++) {
      common::ObPriorityQueue<1>* pq = tenant_multi_level_queue->get_pq_queue(i);
      if (OB_ISNULL(pq)) {
        ret = OB_NULL_CHECK_ERROR;
        LOG_ERROR("pq is nullptr", K(ret));
      } else {
        int64_t multi_prio_cnt = pq->get_prio_cnt();
        for (int64_t j = 0; OB_SUCC(ret) && j < multi_prio_cnt; j++) {
          ObLinkQueue *link_queue = pq->get_queue(j);
          if (OB_ISNULL(link_queue)) {
            ret = OB_NULL_CHECK_ERROR;
            LOG_ERROR("link_queue is nullptr", K(ret));
          } else if (OB_FAIL(traverse_one_tenant_one_link_queue(link_queue, group_id, process))) {
            LOG_ERROR("traverse one tenant one queue failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObNetQueueTraver::traverse_one_tenant_one_link_queue(ObLinkQueue *link_queue, int32_t group_id, ObINetTraverProcess &process)
{
  int ret = OB_SUCCESS;
  const int64_t current_time = ObTimeUtility::current_time();
  ObLink *cur = nullptr;
  ObRequest *req_cur = nullptr;
  bool is_end = false;
  if (OB_ISNULL(link_queue)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("link_queue is nullptr", K(ret));
  } else {
    int64_t size = link_queue->size();
    for (int64_t i = 0; OB_SUCC(ret) && (i < size) && (!is_end); i++) {
      if (OB_FAIL(link_queue->pop(cur))) {
        ret = OB_SUCCESS;
        is_end = true;
      } else {
        if (OB_ISNULL(cur)){
          ret = OB_NULL_CHECK_ERROR;
          LOG_ERROR("cur is nullptr", K(ret));
        } else if (OB_ISNULL(req_cur = static_cast<ObRequest *>(cur))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("req_cur is nullptr", K(ret));
        } else if (req_cur->get_traverse_index() >= current_time) {
          is_end = true;
        } else if (OB_FAIL(req_cur->set_traverse_index(current_time))) {
          LOG_ERROR("req_cur set_traverse_index failed", K(ret));
        } else if (OB_FAIL(process.process_request(req_cur, group_id))) {
          LOG_ERROR("process request failed", K(ret));
        }
        // push req
        if (OB_NOT_NULL(cur) && OB_FAIL(link_queue->push(cur))) {
          LOG_ERROR("link_queue push req failed", K(ret));
        }
      }
    }  // for end
  }
  return ret;
}

int ObNetTraverProcessAutoDiag::process_request(ObRequest *req_cur, int32_t group_id)
{
  int ret = OB_SUCCESS;
  ObNetQueueTraRes tmp_info;
  if (OB_FAIL(get_trav_req_info(req_cur, tmp_info, group_id))) {
    LOG_ERROR("get_trav_req_info failed", K(ret));
  } else {
    net_recorder_(tmp_info);
  }
  return ret;
}
int ObNetTraverProcessAutoDiag::get_trav_req_info(ObRequest *cur, ObNetQueueTraRes &tmp_info, int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("cur is nullptr", K(ret));
  } else {
    tmp_info.type_ = cur->get_type();
    tmp_info.enqueue_timestamp_ = cur->get_enqueue_timestamp();
    tmp_info.trace_id_ = cur->get_trace_id();
    tmp_info.retry_times_ = cur->get_retry_times();
    tmp_info.group_id_ = group_id;
    if (ObRequest::OB_RPC == cur->get_type()) {
      const obrpc::ObRpcPacket &pkt = static_cast<const obrpc::ObRpcPacket &>(cur->get_packet());
      tmp_info.tenant_id_ = pkt.get_tenant_id();
      tmp_info.pcode_ = static_cast<int32_t>(pkt.get_pcode());
      tmp_info.req_level_ = min(pkt.get_request_level(), omt::MAX_REQUEST_LEVEL - 1);
      tmp_info.priority_ = pkt.get_priority();
    } else if (ObRequest::OB_MYSQL == cur->get_type()) {
      void *sess = SQL_REQ_OP.get_sql_session(cur);
      oceanbase::observer::ObSMConnection *conn = nullptr;
      const obmysql::ObMySQLRawPacket &pkt =
          reinterpret_cast<const obmysql::ObMySQLRawPacket &>(cur->get_packet());
      if (OB_ISNULL(sess)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sess is nullptr", K(ret));
      } else {
        conn = static_cast<oceanbase::observer::ObSMConnection *>(sess);
        tmp_info.tenant_id_ = static_cast<int64_t>(conn->tenant_id_);
        tmp_info.sql_session_id_ = static_cast<uint32_t>(conn->sessid_);
        tmp_info.mysql_cmd_ = pkt.get_cmd();
      }
    }
  }
  return ret;
}
}  // namespace rpc
}  // namespace oceanbase