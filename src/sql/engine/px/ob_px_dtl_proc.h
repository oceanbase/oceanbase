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

#ifndef _OB_SQL_PX_DTL_PROC_H_
#define _OB_SQL_PX_DTL_PROC_H_

#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_processor.h"

namespace oceanbase {
namespace sql {

class ObPxCoordMsgProc;
class ObExecContext;

////////////////////////////  FOR QC ////////////////////////////

class ObPxFinishSqcResultP : public dtl::ObDtlPacketProc<ObPxFinishSqcResultMsg> {
public:
  ObPxFinishSqcResultP(ObExecContext& ctx, ObIPxCoordMsgProc& msg_proc) : ctx_(ctx), msg_proc_(msg_proc)
  {}
  virtual ~ObPxFinishSqcResultP() = default;
  int process(const ObPxFinishSqcResultMsg& pkt) override;

private:
  ObExecContext& ctx_;
  ObIPxCoordMsgProc& msg_proc_;
};

class ObPxInitSqcResultP : public dtl::ObDtlPacketProc<ObPxInitSqcResultMsg> {
public:
  ObPxInitSqcResultP(ObExecContext& ctx, ObIPxCoordMsgProc& msg_proc) : ctx_(ctx), msg_proc_(msg_proc)
  {}
  virtual ~ObPxInitSqcResultP() = default;
  int process(const ObPxInitSqcResultMsg& pkt) override;

private:
  ObExecContext& ctx_;
  ObIPxCoordMsgProc& msg_proc_;
};

class ObPxQcInterruptedP : public dtl::ObDtlInterruptProc {
public:
  ObPxQcInterruptedP(ObExecContext& ctx, ObIPxCoordMsgProc& msg_proc) : ctx_(ctx), msg_proc_(msg_proc)
  {}
  virtual ~ObPxQcInterruptedP() = default;
  int process(const common::ObInterruptCode& ic) override;

private:
  ObExecContext& ctx_;
  ObIPxCoordMsgProc& msg_proc_;
};

////////////////////////////  FOR SQC ////////////////////////////

class ObPxReceiveDataChannelMsgP : public dtl::ObDtlPacketProc<ObPxReceiveDataChannelMsg> {
public:
  ObPxReceiveDataChannelMsgP(ObIPxSubCoordMsgProc& msg_proc) : msg_proc_(msg_proc)
  {}
  virtual ~ObPxReceiveDataChannelMsgP() = default;
  int process(const ObPxReceiveDataChannelMsg& pkt) override;

private:
  ObIPxSubCoordMsgProc& msg_proc_;
};

class ObPxTransmitDataChannelMsgP : public dtl::ObDtlPacketProc<ObPxTransmitDataChannelMsg> {
public:
  ObPxTransmitDataChannelMsgP(ObIPxSubCoordMsgProc& msg_proc) : msg_proc_(msg_proc)
  {}
  virtual ~ObPxTransmitDataChannelMsgP() = default;
  int process(const ObPxTransmitDataChannelMsg& pkt) override;

private:
  ObIPxSubCoordMsgProc& msg_proc_;
};

class ObPxSqcInterruptedP : public dtl::ObDtlInterruptProc {
public:
  ObPxSqcInterruptedP(ObIPxSubCoordMsgProc& msg_proc) : msg_proc_(msg_proc)
  {}
  virtual ~ObPxSqcInterruptedP() = default;
  int process(const common::ObInterruptCode& ic) override;

private:
  ObIPxSubCoordMsgProc& msg_proc_;
};

class ObPxReceiveRowP : public dtl::ObDtlPacketProc<ObPxNewRow> {
public:
  explicit ObPxReceiveRowP(ObPxNewRow& px_row) : row_allocator_(common::ObModIds::OB_SQL_PX), px_row_(px_row)
  {}
  virtual ~ObPxReceiveRowP() = default;
  int process(const ObPxNewRow& pkt);
  void destroy()
  {
    row_allocator_.reset();
  }

  virtual void set_iter(dtl::ObDtlMsgIterator* iter)
  {
    px_row_.set_iter(iter);
  }
  virtual bool has_iter()
  {
    return px_row_.has_iter();
  }

private:
  ObArenaAllocator row_allocator_;
  ObPxNewRow& px_row_;
};

class ObPxInterruptP : public dtl::ObDtlInterruptProc {
public:
  virtual ~ObPxInterruptP() = default;
  int process(const common::ObInterruptCode& ic) override;
};

}  // namespace sql
}  // namespace oceanbase

#endif
