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

#include "ob_election_rpc.h"
#include "ob_election_part_array_buf.h"
#include "storage/ob_partition_service.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/utility.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace election;

namespace obrpc {

OB_SERIALIZE_MEMBER(ObElectionRpcResult, status_, send_timestamp_);

int64_t ObElectionP::total_fly_us_ = 0;
int64_t ObElectionP::total_wait_us_ = 0;
int64_t ObElectionP::total_handle_us_ = 0;
int64_t ObElectionP::total_process_ = 0;

void ObElectionRpcResult::reset()
{
  status_ = OB_SUCCESS;
  send_timestamp_ = 0;
}

int ObElectionP::process()
{
  int ret = OB_SUCCESS;
  const int64_t send_ts = get_send_timestamp();
  const int64_t receive_ts = get_receive_timestamp();
  const int64_t run_ts = ObTimeUtility::current_time();
  const uint64_t packet_id = rpc_pkt_->get_packet_id();

  if (OB_ISNULL(partition_service_)) {
    ELECT_ASYNC_LOG(WARN, "partition service is null", KP_(partition_service));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObIElectionMgr* election_mgr = partition_service_->get_election_mgr();
    if (OB_ISNULL(election_mgr)) {
      ELECT_ASYNC_LOG(WARN, "election manager is null", KP(election_mgr));
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = election_mgr->handle_election_msg(arg_, result_);
      const int64_t cur_ts = ObTimeUtility::current_time();
      // check rt
      statistics(receive_ts - send_ts, run_ts - receive_ts, cur_ts - run_ts);
      if (OB_FAIL(ret)) {
        ELECT_ASYNC_LOG(WARN,
            "handle election message error",
            K(ret),
            K(packet_id),
            K(send_ts),
            K(receive_ts),
            K(run_ts),
            K(cur_ts),
            "fly_ts",
            receive_ts - send_ts,
            "wait_ts",
            run_ts - receive_ts,
            "handle_ts",
            cur_ts - run_ts,
            "peer",
            get_peer());
      }
    }
  }

  // always return OB_SUCCESS, so rpc can send result_ back
  return OB_SUCCESS;
}

void ObElectionP::statistics(const int64_t fly_us, const int64_t wait_us, const int64_t handle_us)
{
  const int64_t total_fly_us = ATOMIC_AAF(&total_fly_us_, fly_us);
  const int64_t total_wait_us = ATOMIC_AAF(&total_wait_us_, wait_us);
  const int64_t total_handle_us = ATOMIC_AAF(&total_handle_us_, handle_us);
  const int64_t total_process = ATOMIC_AAF(&total_process_, 1);

  if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL)) {
    ELECT_ASYNC_LOG(INFO,
        "election rpc statistics",
        K(total_process),
        "avg_fly_us",
        total_fly_us / (total_process + 1),
        "avg_wait_us",
        total_wait_us / (total_process + 1),
        "avg_handle_us",
        total_handle_us / (total_process + 1));
    (void)ATOMIC_SET(&total_fly_us_, 0);
    (void)ATOMIC_SET(&total_wait_us_, 0);
    (void)ATOMIC_SET(&total_handle_us_, 0);
    (void)ATOMIC_SET(&total_process_, 0);
  }
}

}  // namespace obrpc

namespace election {

int ObElectionRpc::init(ObElectionRpcProxy* rpc_proxy, ObIElectionMgr* election_mgr, const ObAddr& self)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionRpc inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(rpc_proxy) || OB_ISNULL(election_mgr) || !self.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", KP(rpc_proxy), KP(election_mgr), K(self));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = election_cb_.init(election_mgr))) {
    ELECT_ASYNC_LOG(WARN, "election callback init error", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    election_mgr_ = election_mgr;
    self_ = self;
    is_inited_ = true;
    ELECT_ASYNC_LOG(INFO, "election rpc inited success");
  }

  return ret;
}

void ObElectionRpc::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    election_mgr_ = NULL;
    ELECT_ASYNC_LOG(INFO, "election rpc destroyed");
  }
}

int ObElectionRpc::post_election_msg(
    const ObAddr& server, const int64_t cluster_id, const ObPartitionKey& partition, const ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;
  ObElectionMsgBuffer msgbuf;

  if (!is_inited_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionRpc not inited");
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition.is_valid() || !msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(server), K(partition), K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = serialization::encode_i32(
                                msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), msg.get_msg_type()))) {
    ELECT_ASYNC_LOG(WARN, "serialize msg_type error", K(ret), "msg_type", msg.get_msg_type());
  } else if (OB_SUCCESS !=
             (ret = partition.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    ELECT_ASYNC_LOG(WARN, "seralize partition error", K(ret), K(partition));
  } else if (OB_SUCCESS != (ret = msg.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
    ELECT_ASYNC_LOG(WARN, "seralize msg error", K(ret), K(msg));

  } else {
    if (OB_SUCCESS != (ret = rpc_proxy_->to(server)
                                 .dst_cluster_id(cluster_id)
                                 .by(OB_ELECT_TENANT_ID)
                                 .trace_time(true)
                                 .max_process_handler_time(MAX_PROCESS_HANDLER_TIME)
                                 .post_election_msg(msgbuf, &election_cb_))) {
      ELECT_ASYNC_LOG(WARN, "post election message error", K(ret), K(server), K(partition), K(msg));
    }
  }

  return ret;
}

int ObElectionRpc::post_election_group_msg(const ObAddr& server, const int64_t cluster_id,
    const ObElectionGroupId& eg_id, const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;
  ObElectionMsgBuffer msgbuf;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionRpc not inited", K(ret));
  } else if (!server.is_valid() || !eg_id.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(server), K(eg_id), K(msg));
  } else {
    const int64_t part_array_ser_size = part_array_buf.get_serialize_size();
    if (OB_FAIL(serialization::encode_i32(
            msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), msg.get_msg_type()))) {
      ELECT_ASYNC_LOG(WARN, "serialize msg_type error", K(ret), "msg_type", msg.get_msg_type());
    } else if (OB_FAIL(eg_id.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      ELECT_ASYNC_LOG(WARN, "seralize eg_id error", K(ret), K(eg_id));
    } else if (OB_FAIL(serialization::encode_i64(
                   msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), part_array_buf.get_eg_version()))) {
      ELECT_ASYNC_LOG(WARN, "serialize eg_version error", K(ret));
    } else if (OB_FAIL(serialization::encode_i64(
                   msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), part_array_ser_size))) {
      ELECT_ASYNC_LOG(WARN, "serialize serialize_size error", K(ret));
    } else {
      void* dst_buf = msgbuf.alloc(part_array_ser_size);
      if (NULL == dst_buf) {
        ret = OB_ERR_UNEXPECTED;
        ELECT_ASYNC_LOG(ERROR, "msgbuf.alloc failed", K(ret));
      } else {
        MEMCPY(dst_buf, part_array_buf.get_data_buf(), part_array_ser_size);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(msg.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
      ELECT_ASYNC_LOG(WARN, "seralize msg error", K(ret), K(msg));
    } else {
      if (OB_FAIL(rpc_proxy_->to(server)
                      .dst_cluster_id(cluster_id)
                      .by(OB_ELECT_TENANT_ID)
                      .trace_time(true)
                      .max_process_handler_time(MAX_PROCESS_HANDLER_TIME)
                      .post_election_msg(msgbuf, &election_cb_))) {
        ELECT_ASYNC_LOG(WARN, "post election message error", K(ret), K(server), K(eg_id), K(msg));
      }
    }
  }

  return ret;
}

int ObElectionBatchRpc::post_election_msg(
    const ObAddr& server, const int64_t cluster_id, const ObPartitionKey& partition, const ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_ELECT_TENANT_ID;

  if (NULL == rpc_) {
    ELECT_ASYNC_LOG(WARN, "ObElectionRpc not inited");
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !partition.is_valid() || !msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(server), K(partition), K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(
            rpc_->post(tenant_id, server, cluster_id, obrpc::ELECTION_BATCH_REQ, msg.get_msg_type(), partition, msg))) {
      ELECT_ASYNC_LOG(WARN, "post msg fail", K(ret), K(partition), K(msg));
    }
  }
  return ret;
}

int ObElectionBatchRpc::post_election_group_msg(const common::ObAddr& server, const int64_t cluster_id,
    const ObElectionGroupId& eg_id, const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_ELECT_TENANT_ID;

  if (!server.is_valid() || !eg_id.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(server), K(eg_id), K(msg));
  } else {
    ObEGBatchReq eg_req(&part_array_buf, &msg);
    if (OB_FAIL(rpc_->post(
            tenant_id, server, cluster_id, obrpc::ELECTION_GROUP_BATCH_REQ, msg.get_msg_type(), eg_id, eg_req))) {
      ELECT_ASYNC_LOG(WARN, "post msg fail", K(ret), K(eg_id), K(msg));
    }
  }

  return ret;
}

}  // namespace election
}  // namespace oceanbase
