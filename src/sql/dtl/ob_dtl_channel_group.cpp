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

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_task.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

/**
 *  This interface is similar to make_receive_channel, and is mainly used to construct a channel
 *  after the tranmist and receive end receive channel information
 *  The previous make_channel was created on the PX (coord) side, and then the sqc was sent
 *  separately, that is, to the transmit and receive sides,
 *  The transmit and receive sides directly new channel instances based on the created channel
 *  information. But this method has performance problems
 *  The algorithm complexity of the previous method is: transmit_dfo_task_cnt * receive_dfo_task_cnt,
 *  assuming dop=512, then at least 512*512
 *  DTL channel map optimization design"
 *  The new scheme no longer constructs all channel specific information from the PX side,
 *  Instead, PX constructs the overall information of the channel, and sends the overall information
 *  of the channel to the sqc of all dfos,
 *  Then each (task) worker constructs its own information according to the overall channel information,
 *  Replace the previous make_channel with make_transmit(receive)_channel
 *  That is, the channel provider provides the get_data_ch interface, and then transmti and receive
 *  build their own channels respectively
 *  Assumption: M: the number of workers in Transmit dfo
 *              N: The number of workers receiving dfo
 *              start_ch_id: is the start id of the number of channels applying for (M * N)
 * For a worker, the general formula for building a channel is:
 * transmit side:
 * i = cur_worker_idx;
 * for (j = 0; j < N; j++) {
 *  chid = start_ch_id + j + i * N;
 * }
 * receive side:
 * i = cur_worker_idx;
 * for (j = 0; j < M; j++) {
 *  chid = start_ch_id + j * N + i;
 * }
 **/
void ObDtlChannelGroup::make_transmit_channel(
    const uint64_t tenant_id, const ObAddr& peer_exec_addr, uint64_t chid, ObDtlChannelInfo& ci_producer, bool is_local)
{
  UNUSED(is_local);
  ci_producer.chid_ = chid << 1;
  if (is_local) {
    ci_producer.type_ = DTL_CT_LOCAL;
  } else {
    ci_producer.type_ = DTL_CT_RPC;
  }
  ci_producer.peer_ = peer_exec_addr;
  ci_producer.role_ = DTL_CR_PUSHER;
  ci_producer.tenant_id_ = tenant_id;
}

void ObDtlChannelGroup::make_receive_channel(
    const uint64_t tenant_id, const ObAddr& peer_exec_addr, uint64_t chid, ObDtlChannelInfo& ci_consumer, bool is_local)
{
  UNUSED(is_local);
  ci_consumer.chid_ = (chid << 1) + 1;
  if (is_local) {
    ci_consumer.type_ = DTL_CT_LOCAL;
  } else {
    ci_consumer.type_ = DTL_CT_RPC;
  }
  ci_consumer.peer_ = peer_exec_addr;
  ci_consumer.role_ = DTL_CR_PUSHER;
  ci_consumer.tenant_id_ = tenant_id;
}

int ObDtlChannelGroup::make_channel(const uint64_t tenant_id, const ObAddr& producer_exec_addr,
    const ObAddr& consumer_exec_addr, ObDtlChannelInfo& ci_producer, ObDtlChannelInfo& ci_consumer)
{
  int ret = OB_SUCCESS;
  const uint64_t chid = ObDtlChannel::generate_id();
  if (producer_exec_addr != consumer_exec_addr) {
    // @TODO: rpc channel isn't supported right now
    ci_producer.chid_ = chid << 1;
    ci_producer.type_ = DTL_CT_RPC;
    ci_producer.peer_ = consumer_exec_addr;
    ci_producer.role_ = DTL_CR_PUSHER;
    ci_producer.tenant_id_ = tenant_id;
    ci_consumer.chid_ = (chid << 1) + 1;
    ci_consumer.type_ = DTL_CT_RPC;
    ci_consumer.peer_ = producer_exec_addr;
    ci_consumer.role_ = DTL_CR_PULLER;
    ci_consumer.tenant_id_ = tenant_id;
  } else {
    // If producer and consumer are in the same execution process, we
    // can use in memory channel.
    ci_producer.chid_ = chid << 1;
    ci_producer.type_ = DTL_CT_LOCAL;
    ci_producer.peer_ = consumer_exec_addr;
    ci_producer.role_ = DTL_CR_PUSHER;
    ci_producer.tenant_id_ = tenant_id;
    ci_consumer.chid_ = (chid << 1) + 1;
    ci_consumer.type_ = DTL_CT_LOCAL;
    ci_consumer.peer_ = producer_exec_addr;
    ci_consumer.role_ = DTL_CR_PULLER;
    ci_consumer.tenant_id_ = tenant_id;
  }
  return ret;
}

int ObDtlChannelGroup::link_channel(const ObDtlChannelInfo& ci, ObDtlChannel*& chan, ObDtlFlowControl* dfc)
{
  int ret = OB_SUCCESS;
  const auto chid = ci.chid_;
  chan = NULL;
  if (nullptr != dfc && ci.type_ == DTL_CT_LOCAL) {
    if (OB_FAIL(DTL.create_local_channel(ci.tenant_id_, ci.chid_, ci.peer_, chan, dfc))) {
      LOG_WARN("create local channel fail", KP(chid), K(ret));
    }
    LOG_TRACE("trace create local channel", KP(chid), K(ret), K(ci.peer_), K(ci.type_));
  } else {
    if (OB_FAIL(DTL.create_rpc_channel(ci.tenant_id_, ci.chid_, ci.peer_, chan, dfc))) {
      LOG_WARN("create rpc channel fail", KP(chid), K(ret), K(ci.peer_));
    }
    LOG_TRACE("trace create rpc channel", KP(chid), K(ret), K(ci.peer_), K(ci.type_));
  }
  return ret;
}

int ObDtlChannelGroup::unlink_channel(const ObDtlChannelInfo& ci)
{
  return DTL.destroy_channel(ci.chid_);
}

int ObDtlChannelGroup::remove_channel(const ObDtlChannelInfo& ci, ObDtlChannel*& ch)
{
  return DTL.remove_channel(ci.chid_, ch);
}

int ObDtlChannelGroup::make_channel(ObDtlChSet& producer, ObDtlChSet& consumer)
{
  int ret = OB_SUCCESS;
  UNUSEDx(producer, consumer);
  return ret;
}

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase
