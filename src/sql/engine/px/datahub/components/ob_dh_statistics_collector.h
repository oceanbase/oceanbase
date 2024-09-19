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

#ifndef __OB_SQL_ENG_PX_DH_STATISTICS_COLLECTOR_H__
#define __OB_SQL_ENG_PX_DH_STATISTICS_COLLECTOR_H__

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"

namespace oceanbase
{
namespace sql
{

class ObStatisticsCollectorPieceMsg;
class ObStatisticsCollectorWholeMsg;
typedef ObPieceMsgP<ObStatisticsCollectorPieceMsg> ObStatisticsCollectorPieceMsgP;
typedef ObWholeMsgP<ObStatisticsCollectorWholeMsg> ObStatisticsCollectorWholeMsgP;
class ObStatisticsCollectorPieceMsgListener;
class ObStatisticsCollectorPieceMsgCtx;

/* 各种 datahub 子类消息定义如下 */
class ObStatisticsCollectorPieceMsg
  : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_STATISTICS_COLLECTOR_PIECE_MSG>

{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObStatisticsCollectorPieceMsgListener;
  using PieceMsgCtx = ObStatisticsCollectorPieceMsgCtx;
public:
  ObStatisticsCollectorPieceMsg() : use_hash_join_(false) {}
  ~ObStatisticsCollectorPieceMsg() = default;
  void reset()
  {
    use_hash_join_ = false;
  }
  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_STATISTICS_COLLECTOR_PIECE_MSG>,
                       K_(use_hash_join));
  bool use_hash_join_;
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObStatisticsCollectorPieceMsg);
};


class ObStatisticsCollectorWholeMsg
    : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_STATISTICS_COLLECTOR_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObStatisticsCollectorWholeMsg>;
public:
  ObStatisticsCollectorWholeMsg() : use_hash_join_(false) {}
  ~ObStatisticsCollectorWholeMsg() = default;
  int assign(const ObStatisticsCollectorWholeMsg &other)
  {
    use_hash_join_ = other.use_hash_join_;
    return common::OB_SUCCESS;
  }
  void reset()
  {
    use_hash_join_ = false;
  }
  VIRTUAL_TO_STRING_KV(K_(use_hash_join));
  bool use_hash_join_;
};

class ObStatisticsCollectorPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObStatisticsCollectorPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts)
    : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), use_hash_join_(false), received_cnt_(0) {}
  ~ObStatisticsCollectorPieceMsgCtx() = default;
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  static int alloc_piece_msg_ctx(const ObStatisticsCollectorPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(use_hash_join), K_(received_cnt));
  bool use_hash_join_;
  int received_cnt_;
  static const int SEND_ALL = -1;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStatisticsCollectorPieceMsgCtx);
};

class ObStatisticsCollectorPieceMsgListener
{
public:
  ObStatisticsCollectorPieceMsgListener() = default;
  ~ObStatisticsCollectorPieceMsgListener() = default;
  static int on_message(
      ObStatisticsCollectorPieceMsgCtx &ctx,
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      const ObStatisticsCollectorPieceMsg &pkt);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObStatisticsCollectorPieceMsgListener);
};

}
}
#endif /* __OB_SQL_ENG_PX_DH_STATISTICS_COLLECTOR_H__ */
//// end of header file
