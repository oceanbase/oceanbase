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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_px_dtl_proc.h"
#include "ob_px_dtl_msg.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

// put your code here

int ObPxFinishSqcResultP::process(const ObPxFinishSqcResultMsg &pkt)
{
  return msg_proc_.on_sqc_finish_msg(ctx_, pkt);
}

int ObPxInitSqcResultP::process(const ObPxInitSqcResultMsg &pkt)
{
  return msg_proc_.on_sqc_init_msg(ctx_, pkt);
}

int ObPxQcInterruptedP::process(const ObInterruptCode &pkt)
{
  return msg_proc_.on_interrupted(ctx_, pkt);
}

int ObPxReceiveDataChannelMsgP::process(const ObPxReceiveDataChannelMsg &pkt)
{
  return msg_proc_.on_receive_data_ch_msg(pkt);
}

int ObPxTransmitDataChannelMsgP::process(const ObPxTransmitDataChannelMsg &pkt)
{
  return msg_proc_.on_transmit_data_ch_msg(pkt);
}

int ObPxCreateBloomFilterChannelMsgP::process(const ObPxCreateBloomFilterChannelMsg &pkt)
{
  return msg_proc_.on_create_filter_ch_msg(pkt);
}
int ObPxSqcInterruptedP::process(const ObInterruptCode &pkt)
{
  return msg_proc_.on_interrupted(pkt);
}

int ObPxReceiveRowP::process(const ObDtlLinkedBuffer &buffer, bool &transferred)
{
  int ret = OB_SUCCESS;
  if (NULL == reader_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader not set while receive data message", K(ret));
  } else if (OB_FAIL(reader_->add_buffer(const_cast<ObDtlLinkedBuffer &>(buffer), transferred))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("add buffer failed", K(ret));
    }
  }
  return ret;
}

int ObPxInterruptP::process(const ObInterruptCode &ic)
{
  return ic.code_;
}
