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
 * 该接口与make_receive_channel类似，主要用于tranmist和receive端收到channel信息后构建channel
 * 之前的make_channel是在PX(coord)端创建好，然后分别发送sqc,即给transmit和receive端，
 * transmit和receive端直接根据创建好的channel信息来new channel实例。但这种方式存在性能问题
 * 之前方式实现算法复杂度是：transmit_dfo_task_cnt * receive_dfo_task_cnt，假设dop=512，则至少512*512
 * 随着dop增大耗时更长。见bug
 * 新方案：
 *        新方案从PX端不再构建所有channel具体信息，
 *        而是PX构建channel的总体信息，将channel总体信息发给所有的dfo的sqc，
 *        然后每个(task)worker根据channel总体信息各自构建自己的信息,
 *        将之前 make_channel 替换成 make_transmit(receive)_channel
 *        即channel provider提供get_data_ch接口，然后transmti和receive分别构建属于自己的channel
 * 假设： M: Transmit dfo的worker数量
 *       N: Receive dfo的worker数量
 *       start_ch_id: 是申请(M * N)的channel个数的start id
 * 对于某个worker来讲，构建channel大致公式为：
 * transmit端：
 * i = cur_worker_idx;
 * for (j = 0; j < N; j++) {
 *  chid = start_ch_id + j + i * N;
 * }
 * receive端：
 * i = cur_worker_idx;
 * for (j = 0; j < M; j++) {
 *  chid = start_ch_id + j * N + i;
 * }
 **/
void ObDtlChannelGroup::make_transmit_channel(const uint64_t tenant_id,
                                    const ObAddr &peer_exec_addr,
                                    uint64_t chid,
                                    ObDtlChannelInfo &ci_producer,
                                    bool is_local)
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

void ObDtlChannelGroup::make_receive_channel(const uint64_t tenant_id,
                                    const ObAddr &peer_exec_addr,
                                    uint64_t chid,
                                    ObDtlChannelInfo &ci_consumer,
                                    bool is_local)
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

int ObDtlChannelGroup::make_channel(const uint64_t tenant_id,
                                    const ObAddr &producer_exec_addr,
                                    const ObAddr &consumer_exec_addr,
                                    ObDtlChannelInfo &ci_producer,
                                    ObDtlChannelInfo &ci_consumer)
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

int ObDtlChannelGroup::link_channel(const ObDtlChannelInfo &ci, ObDtlChannel *&chan, ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  const auto chid = ci.chid_;
  //流控则可以使用local，即data channel
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

int ObDtlChannelGroup::unlink_channel(const ObDtlChannelInfo &ci)
{
  return DTL.destroy_channel(ci.chid_);
}

//从dtl的map中删除channel
int ObDtlChannelGroup::remove_channel(const ObDtlChannelInfo &ci, ObDtlChannel *&ch)
{
  return DTL.remove_channel(ci.chid_, ch);
}

}  // dtl
}  // sql
}  // oceanbase
