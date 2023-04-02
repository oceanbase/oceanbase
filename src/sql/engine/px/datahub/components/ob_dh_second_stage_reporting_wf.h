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

#ifndef OCEANBASE_COMPONENTS_OB_DH_SECOND_STAGE_REPORTING_WF_H_
#define OCEANBASE_COMPONENTS_OB_DH_SECOND_STAGE_REPORTING_WF_H_

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

class ObReportingWFPieceMsg;
class ObReportingWFWholeMsg;
typedef ObPieceMsgP<ObReportingWFPieceMsg> ObReportingWFPieceMsgP;
typedef ObWholeMsgP<ObReportingWFWholeMsg> ObReportingWFWholeMsgP;
class ObReportingWFPieceMsgListener;
class ObReportingWFPieceMsgCtx;
class ObPxCoordInfo;

typedef ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_SECOND_STAGE_REPORTING_WF_PIECE_MSG>
  ObReportingWFPieceMsgBase;
class ObReportingWFPieceMsg : public ObReportingWFPieceMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObReportingWFPieceMsgListener;
  using PieceMsgCtx = ObReportingWFPieceMsgCtx;
public:
  ObReportingWFPieceMsg() : pby_hash_value_array_(NULL){}
  ~ObReportingWFPieceMsg() = default;
  void reset()
  {
    pby_hash_value_array_.reset();
  }
  INHERIT_TO_STRING_KV(
      "meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_SECOND_STAGE_REPORTING_WF_PIECE_MSG>,
      K_(op_id));
public:
  // TODO:@xiaofeng.lby, add a max/min row, use it to decide whether following rows can by pass
  common::ObSArray<uint64_t> pby_hash_value_array_;
};

typedef ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG>
  ObReportingWFWholeMsgBase;
class ObReportingWFWholeMsg : public ObReportingWFWholeMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObReportingWFWholeMsg>;
  ObReportingWFWholeMsg() : pby_hash_value_array_() {}
  ~ObReportingWFWholeMsg() = default;
  int assign(const ObReportingWFWholeMsg &other);
  void reset()
  {
    pby_hash_value_array_.reset();
  }
  // TODO:@xiaofeng.lby, add a max/min row, use it to decide whether following rows can by pass
  common::ObSArray<uint64_t> pby_hash_value_array_;
};

class ObReportingWFPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObReportingWFPieceMsgCtx(
      uint64_t op_id, int64_t task_cnt, int64_t timeout_ts, int64_t tenant_id)
      : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), received_(0), tenant_id_(tenant_id),
        whole_msg_() {}
  virtual ~ObReportingWFPieceMsgCtx() = default;
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received));
  static int alloc_piece_msg_ctx(const ObReportingWFPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  int received_;
  int64_t tenant_id_;
  ObReportingWFWholeMsg whole_msg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObReportingWFPieceMsgCtx);
};

class ObReportingWFPieceMsgListener
{
public:
  ObReportingWFPieceMsgListener() = default;
  ~ObReportingWFPieceMsgListener() = default;
  static int on_message(
      ObReportingWFPieceMsgCtx &ctx,
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      const ObReportingWFPieceMsg &pkt);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObReportingWFPieceMsgListener);
};

}
}
#endif /* OCEANBASE_COMPONENTS_OB_DH_SECOND_STAGE_REPORTING_WF_H_ */
//// end of header file
