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
#include "sql/engine/px/ob_sqc_ctx.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/dtl/ob_dtl_msg_type.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

// ObDhWholeeMsgProc 仅用于本 cpp 文件，所以可以放在这里
// 专用于处理 datahub whole 消息的逻辑
template <typename WholeMsg>
class ObDhWholeeMsgProc
{
public:
  ObDhWholeeMsgProc() = default;
  ~ObDhWholeeMsgProc() = default;
  int on_whole_msg(ObSqcCtx &sqc_ctx, dtl::ObDtlMsgType msg_type, const WholeMsg &pkt) const
  {
    int ret = OB_SUCCESS;
    ObPxDatahubDataProvider *p = nullptr;
    if (OB_FAIL(sqc_ctx.get_whole_msg_provider(pkt.op_id_, msg_type, p))) {
      LOG_WARN("fail get whole msg provider", K(ret));
    } else {
      typename WholeMsg::WholeMsgProvider *provider =
          static_cast<typename WholeMsg::WholeMsgProvider *>(p);
      if (OB_FAIL(provider->add_msg(pkt))) {
        LOG_WARN("fail set whole msg to provider", K(ret));
      }
    }
    return ret;
  }
};


int ObPxSubCoordMsgProc::on_transmit_data_ch_msg(
    const ObPxTransmitDataChannelMsg &pkt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_ctx_.transmit_data_ch_provider_.add_msg(pkt))) {
    LOG_WARN("fail set transmit channel msg to ch provider", K(ret));
  }
  return ret;
}


int ObPxSubCoordMsgProc::on_receive_data_ch_msg(
    const ObPxReceiveDataChannelMsg &pkt) const
{
  int ret = OB_SUCCESS;
  // FIXME:
  // 如果 dfo 里有两个 receive，他们调用获取 channel 的时机不确定，
  // 那么它们可能拿到错误的 channel（搞反了）
  // 为了避免这种情况，采取共享内存的方式，由 receive op 自己去判断 channel 是否属于自己
  if (OB_FAIL(sqc_ctx_.receive_data_ch_provider_.add_msg(pkt))) {
    LOG_WARN("fail set receive channel msg to ch provider", K(ret));
  }
  return ret;
}

// NOTE：QC、Task 都可以中断 SQC，如果 SQC 处于收消息流程中，会调用本方法
// 如果 SQC 已经离开了收消息流程，则不会触发本方法。
int ObPxSubCoordMsgProc::on_interrupted(const ObInterruptCode &ic) const
{
  int ret = OB_SUCCESS;
  sqc_ctx_.interrupted_ = true;
  // 抛出错误码到主处理路程，结束 SQC
  ret = ic.code_;
  LOG_TRACE("sqc received a interrupt and throw out of msg proc", K(ic));
  return ret;
}

int ObPxSubCoordMsgProc::on_create_filter_ch_msg(
    const ObPxCreateBloomFilterChannelMsg &pkt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sqc_ctx_.bf_ch_provider_.add_msg(pkt))) {
    LOG_WARN("fail set transmit channel msg to ch provider", K(ret));
  }
  return ret;
}

int ObPxSubCoordMsgProc::on_whole_msg(
    const ObBarrierWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObBarrierWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_BARRIER_WHOLE_MSG, pkt);
}
int ObPxSubCoordMsgProc::on_whole_msg(
    const ObWinbufWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObWinbufWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_WINBUF_WHOLE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(
    const ObDynamicSampleWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObDynamicSampleWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_DYNAMIC_SAMPLE_WHOLE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(
    const ObRollupKeyWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObRollupKeyWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_ROLLUP_KEY_WHOLE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(
    const ObRDWFWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObRDWFWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_RANGE_DIST_WF_PIECE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(
    const ObInitChannelWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObInitChannelWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_INIT_CHANNEL_WHOLE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(
    const ObReportingWFWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObReportingWFWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(
    const ObOptStatsGatherWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<ObOptStatsGatherWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_OPT_STATS_GATHER_WHOLE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(const SPWinFuncPXWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<SPWinFuncPXWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_SP_WINFUNC_PX_WHOLE_MSG, pkt);
}

int ObPxSubCoordMsgProc::on_whole_msg(const RDWinFuncPXWholeMsg &pkt) const
{
  ObDhWholeeMsgProc<RDWinFuncPXWholeMsg> proc;
  return proc.on_whole_msg(sqc_ctx_, dtl::DH_RD_WINFUNC_PX_WHOLE_MSG, pkt);
}