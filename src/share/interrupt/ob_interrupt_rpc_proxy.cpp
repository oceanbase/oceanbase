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

#define USING_LOG_PREFIX SERVER

#include "lib/ob_errno.h"
#include "share/interrupt/ob_interrupt_rpc_proxy.h"
#include "share/interrupt/ob_global_interrupt_call.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obrpc
{

OB_SERIALIZE_MEMBER(ObInterruptStackInfo, buf1_);
OB_SERIALIZE_MEMBER(ObInterruptMessage, first_, last_, code_, info_);

int ObInterruptProcessor::process()
{
  int ret = OB_SUCCESS;
  const ObInterruptMessage &msg = arg_;
  ObInterruptibleTaskID tid(msg.first_, msg.last_);
  LIB_LOG(TRACE, "receive a interrupt from peer",
          "peer", get_peer(), K(tid), "int_code", msg.code_, "info", msg.info_);
  ObInterruptCode code(msg.code_, msg.info_);
  ObGlobalInterruptManager::getInstance()->interrupt(tid, code);
  return ret;
}
} // namespace obrpc
} // namespace oceanbase
