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

#ifndef __OB_SQL_ENG_PX_DH_ROLLUP_KEY_H__
#define __OB_SQL_ENG_PX_DH_ROLLUP_KEY_H__

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"

namespace oceanbase
{
namespace sql
{

class ObRollupKeyPieceMsg;
class ObRollupKeyWholeMsg;
typedef ObPieceMsgP<ObRollupKeyPieceMsg> ObRollupKeyPieceMsgP;
typedef ObWholeMsgP<ObRollupKeyWholeMsg> ObRollupKeyWholeMsgP;
class ObRollupKeyPieceMsgListener;
class ObRollupKeyPieceMsgCtx;
class ObPxCoordInfo;

struct ObRollupNDVInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObRollupNDVInfo() : ndv_(0), n_keys_(0), dop_(0), max_keys_(0) {}

  TO_STRING_KV(K_(ndv), K_(n_keys), K_(dop), K_(max_keys));

  void reset()
  {
    ndv_ = 0;
    n_keys_ = 0;
    dop_ = 0;
    max_keys_ = 0;
  }

  int64_t ndv_;
  int64_t n_keys_;
  int64_t dop_;
  int64_t max_keys_;
};

/* 各种 datahub 子类消息定义如下 */
class ObRollupKeyPieceMsg
  : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_ROLLUP_KEY_PIECE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObRollupKeyPieceMsgListener;
  using PieceMsgCtx = ObRollupKeyPieceMsgCtx;
public:
  ObRollupKeyPieceMsg() : rollup_ndv_() {}
  ~ObRollupKeyPieceMsg() = default;
  void reset() { }
  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_ROLLUP_KEY_PIECE_MSG>,
                       K_(op_id));
public:
  ObRollupNDVInfo rollup_ndv_;
};


class ObRollupKeyWholeMsg
    : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_ROLLUP_KEY_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObRollupKeyWholeMsg>;
public:
  ObRollupKeyWholeMsg() : rollup_ndv_() {}
  ~ObRollupKeyWholeMsg() = default;
  int assign(const ObRollupKeyWholeMsg &other);
  void reset() {}
  VIRTUAL_TO_STRING_KV(K_(rollup_ndv));
  ObRollupNDVInfo rollup_ndv_;
};

class ObRollupKeyPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObRollupKeyPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts, int64_t tenant_id)
    : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), received_(0),
                    tenant_id_(tenant_id), whole_msg_(), received_msgs_() {}
  ~ObRollupKeyPieceMsgCtx() = default;
  virtual void destroy()
  {
    received_msgs_.reset();
  }
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received));
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  static int alloc_piece_msg_ctx(const ObRollupKeyPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);

  int process_ndv();

  static const int64_t FAR_GREATER_THAN_RATIO = 16;
public:
  int64_t received_; // 已经收到的 piece 数量
  int64_t tenant_id_;
  ObRollupKeyWholeMsg whole_msg_;
  ObSEArray<ObRollupKeyPieceMsg, 4> received_msgs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRollupKeyPieceMsgCtx);
};

class ObRollupKeyPieceMsgListener
{
public:
  ObRollupKeyPieceMsgListener() = default;
  ~ObRollupKeyPieceMsgListener() = default;
  static int on_message(
      ObRollupKeyPieceMsgCtx &ctx,
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      const ObRollupKeyPieceMsg &pkt);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObRollupKeyPieceMsgListener);
};


}
}
#endif /* __OB_SQL_ENG_PX_DH_ROLLUP_KEY_H__ */
//// end of header file

