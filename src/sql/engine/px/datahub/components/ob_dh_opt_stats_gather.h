/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef __OB_SQL_ENG_PX_DH_OPT_STATS_GATHER_H__
#define __OB_SQL_ENG_PX_DH_OPT_STATS_GATHER_H__

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {

class ObOptStatsGatherPieceMsg;
class ObOptStatsGatherWholeMsg;
typedef ObPieceMsgP<ObOptStatsGatherPieceMsg> ObOptStatsGatherPieceMsgP;
typedef ObWholeMsgP<ObOptStatsGatherWholeMsg> ObOptStatsGatherWholeMsgP;
class ObOptStatsGatherPieceMsgListener;
class ObOptStatsGatherPieceMsgCtx;
class ObPxCoordInfo;

class ObOptStatsGatherPieceMsg
  : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_OPT_STATS_GATHER_PIECE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObOptStatsGatherPieceMsgListener;
  using PieceMsgCtx = ObOptStatsGatherPieceMsgCtx;
public:
  ObOptStatsGatherPieceMsg() :
    column_stats_(),
    table_stats_(),
    arena_("ObOSGPieceMsg"),
    target_osg_id_(0) {};
  ~ObOptStatsGatherPieceMsg() { reset(); };
  void reset() {
    for (int64_t i = 0; i < column_stats_.count(); i++) {
      if (OB_NOT_NULL(column_stats_.at(i))) {
        column_stats_.at(i)->reset();
        column_stats_.at(i) = NULL;
      }
    }
    column_stats_.reset();
    table_stats_.reset();
    arena_.reset();
  };
  int assign(const ObOptStatsGatherPieceMsg &other) {
    // do assign
    return common::OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_OPT_STATS_GATHER_PIECE_MSG>,
                        K_(op_id),
                        K_(column_stats),
                        K_(table_stats),
                        K_(target_osg_id));
  inline void set_osg_id(uint64_t target_id) { target_osg_id_ = target_id; };
  inline uint64_t get_osg_id() { return target_osg_id_; };

  inline void set_tenant_id(uint64_t tenant_id) { arena_.set_tenant_id(tenant_id); };
  common::ObSEArray<ObOptColumnStat*, 4> column_stats_;
  common::ObSEArray<ObOptTableStat*, 4> table_stats_;

  // to restore history
  ObArenaAllocator arena_;
  uint64_t target_osg_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOptStatsGatherPieceMsg);
};

// whole msg is useless, since we don't need the feed back. px only pass the piece msg to osg.
class ObOptStatsGatherWholeMsg
  : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_OPT_STATS_GATHER_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObOptStatsGatherWholeMsg>;
public:
  ObOptStatsGatherWholeMsg() : ready_state_(0) {};
  ~ObOptStatsGatherWholeMsg() = default;
  int assign(const ObOptStatsGatherWholeMsg &other)
  {
    ready_state_ = other.ready_state_;
    return common::OB_SUCCESS;
  }
  void reset()
  {
    ready_state_ = 0;
  }
  VIRTUAL_TO_STRING_KV(K_(ready_state));
  int ready_state_;
};

class ObOptStatsGatherPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObOptStatsGatherPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts)
    : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), received_(0) {}
  ~ObOptStatsGatherPieceMsgCtx() = default;
  virtual void reset_resource() {};
  static int alloc_piece_msg_ctx(const ObOptStatsGatherPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received));
  int received_;
  ObOperatorKit *op_kit_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOptStatsGatherPieceMsgCtx);
};

class ObOptStatsGatherPieceMsgListener
{
public:
  ObOptStatsGatherPieceMsgListener() = default;
  ~ObOptStatsGatherPieceMsgListener() = default;
  static int on_message(
    ObOptStatsGatherPieceMsgCtx &piece_ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObOptStatsGatherPieceMsg &pkt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObOptStatsGatherPieceMsgListener);
};

}
}
#endif