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

#ifndef OB_DTL_CHANNEL_GROUP_H
#define OB_DTL_CHANNEL_GROUP_H

#include <stdint.h>
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlTask;
class ObDtlChannel;
class ObDtlChSet;
struct ObDtlChannelInfo;
class ObDtlFlowControl;

// Channel group is a bunch of channels than support communication
// from tasks in one job to tasks in another job. To put it simply,
// channel group is between job and job as same as channel is between
// task and task.
//
// Channel group is owner of channels so the group object should exist
// before the two jobs both have finished, otherwise task may visit
// destroyed channel when it is still alive.
//
// Channel group is typically prepared before relating tasks start and
// destroyed after relating tasks end.
//
// Typical usage:
//
//   auto dtl_cg = OB_NEW(ObDtlChannelGroup);
//   if (dtl_cg != nullptr) {
//      dtl_ct->init(DTL_CG_MEMORY_LIMIT);
//      dtl_ct->make_channel(p1, c1);
//      dtl_ct->make_channel(p1, c2);
//      dtl_ct->make_channel(p2, c1);
//      dtl_ct->make_channel(p2, c2);
//   }
//
class ObDtlChannelGroup
{
public:
  static int make_channel(
      const uint64_t tenant_id,
      const common::ObAddr &addr1,
      const common::ObAddr &addr2,
      ObDtlChannelInfo &ci1,
      ObDtlChannelInfo &ci2);
  static int link_channel(const ObDtlChannelInfo &ci, ObDtlChannel *&ch, ObDtlFlowControl *dfc = nullptr);
  static int unlink_channel(const ObDtlChannelInfo &ci);
  static int remove_channel(const ObDtlChannelInfo &ci, ObDtlChannel *&ch);

  static void make_transmit_channel(const uint64_t tenant_id,
                                  const common::ObAddr &peer_exec_addr,
                                  uint64_t ch_id,
                                  ObDtlChannelInfo &ci_producer,
                                  bool is_local);
  static void make_receive_channel(const uint64_t tenant_id,
                                  const common::ObAddr &peer_exec_addr,
                                  uint64_t ch_id,
                                  ObDtlChannelInfo &ci_consumer,
                                  bool is_local);
};


}  // dtl
}  // sql
}  // oceanbase


#endif /* OB_DTL_CHANNEL_GROUP_H */
