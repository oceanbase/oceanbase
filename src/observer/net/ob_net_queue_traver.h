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
#ifndef OCEANBASE_RPC_OB_NET_QUEUE_TRAVER_H
#define OCEANBASE_RPC_OB_NET_QUEUE_TRAVER_H

#include "deps/oblib/src/lib/queue/ob_link_queue.h"
#include "deps/oblib/src/lib/queue/ob_priority_queue.h"
#include "deps/oblib/src/rpc/frame/ob_req_queue_thread.h"
#include "deps/oblib/src/rpc/obmysql/ob_mysql_packet.h"
#include "deps/oblib/src/rpc/obmysql/obsm_struct.h"
#include "share/ob_define.h"
#include "src/observer/ob_server_struct.h"
#include "src/observer/omt/ob_multi_tenant.h"
#include "src/observer/omt/ob_tenant.h"
#include "src/observer/omt/ob_multi_level_queue.h"
namespace oceanbase
{
namespace rpc
{
class ObINetTraverProcess
{
public:
  ObINetTraverProcess(){};
  virtual ~ObINetTraverProcess(){};
  virtual int process_request(ObRequest *req_cur, int32_t group_id) = 0;
  DISALLOW_COPY_AND_ASSIGN(ObINetTraverProcess);
};
class ObNetTraverProcessAutoDiag : public ObINetTraverProcess
{
public:
  struct ObNetQueueTraRes;
  explicit ObNetTraverProcessAutoDiag(
      const std::function<void(const ObNetQueueTraRes &)> &net_recorder)
      : ObINetTraverProcess(), net_recorder_(net_recorder)
  {}
  ~ObNetTraverProcessAutoDiag() = default;
  int process_request(ObRequest *req_cur, int32_t group_id) override;
  struct ObNetQueueTraRes
  {
    ObNetQueueTraRes()
        : enqueue_timestamp_(0),
          retry_times_(0),
          tenant_id_(-1),
          sql_session_id_(0),
          pcode_(-1),
          group_id_(-1),
          mysql_cmd_(obmysql::COM_MAX_NUM),
          req_level_(-1),
          priority_(0)
    {}
    ObRequest::Type type_;
    int64_t enqueue_timestamp_;
    rpc::TraceId trace_id_;  // not init
    int32_t retry_times_;
    int64_t tenant_id_;
    uint32_t sql_session_id_;
    int32_t pcode_;
    int32_t group_id_;
    obmysql::ObMySQLCmd mysql_cmd_;
    int32_t req_level_;
    uint8_t priority_;
    TO_STRING_KV(
        K(type_), K(enqueue_timestamp_), K_(trace_id), K(retry_times_), K(tenant_id_), K(sql_session_id_), K(pcode_), K(group_id_), K(mysql_cmd_), K(req_level_), K(priority_));
  };
  static int get_trav_req_info(ObRequest *cur, ObNetQueueTraRes &tmp_info, int32_t group_id);
  std::function<void(const ObNetQueueTraRes&)> net_recorder_;
  DISALLOW_COPY_AND_ASSIGN(ObNetTraverProcessAutoDiag);
};
class ObNetQueueTraver
{
public:
  ObNetQueueTraver(){};
  ~ObNetQueueTraver() = default;
  int traverse(ObINetTraverProcess &process);
  int traverse_one_tenant(oceanbase::omt::ObTenant *tenant_ptr, ObINetTraverProcess &process);
private:
  typedef oceanbase::common::ObPriorityQueue2<0, 1> TenantReqQueue;
  int traverse_one_tenant_queue(oceanbase::omt::ReqQueue &tenant_req_queue, oceanbase::omt::ObMultiLevelQueue *tenant_multi_level_queue, int32_t group_id, ObINetTraverProcess &process);
  int traverse_one_tenant_group_queue(TenantReqQueue &tenant_group_queue,oceanbase::omt::ObMultiLevelQueue *tenant_multi_level_queue, int32_t group_id, ObINetTraverProcess &process);
  int traverse_one_tenant_one_link_queue(ObLinkQueue *link_queue, int32_t group_id, ObINetTraverProcess &process);
  DISALLOW_COPY_AND_ASSIGN(ObNetQueueTraver);
};

}  // namespace rpc
}  // namespace oceanbase

#endif
