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

#ifndef OB_DTL_LOCAL_CHANNEL_H
#define OB_DTL_LOCAL_CHANNEL_H

#include <stdint.h>
#include <functional>
#include "lib/queue/ob_fixed_queue.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "share/ob_scanner.h"
#include "observer/ob_server_struct.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "sql/dtl/ob_dtl.h"
#include "ob_dtl_interm_result_manager.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlLocalChannel : public ObDtlBasicChannel
{
public:
  explicit ObDtlLocalChannel(const uint64_t tenant_id,
     const uint64_t id, const common::ObAddr &peer, DtlChannelType type);
  explicit ObDtlLocalChannel(const uint64_t tenant_id,
     const uint64_t id, const common::ObAddr &peer, const int64_t hash_val, DtlChannelType type);
  virtual ~ObDtlLocalChannel();

  virtual int init() override;
  virtual void destroy();
  
  virtual int feedup(ObDtlLinkedBuffer *&buffer) override;
  virtual int send_message(ObDtlLinkedBuffer *&buf);
private:
  int send_shared_message(ObDtlLinkedBuffer *&buf);
};

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_LOCAL_CHANNEL_H */
