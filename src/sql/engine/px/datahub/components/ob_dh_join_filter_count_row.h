/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {

class ObJoinFilterCountRowPieceMsg;
class ObJoinFilterCountRowWholeMsg;
typedef ObPieceMsgP<ObJoinFilterCountRowPieceMsg> ObJoinFilterCountRowPieceMsgP;
typedef ObWholeMsgP<ObJoinFilterCountRowWholeMsg> ObJoinFilterCountRowWholeMsgP;
class ObJoinFilterCountRowPieceMsgListener;
class ObJoinFilterCountRowPieceMsgCtx;
class ObPxCoordInfo;

class ObJoinFilterCountRowPieceMsg
  : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_JOIN_FILTER_COUNT_ROW_PIECE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObJoinFilterCountRowPieceMsgListener;
  using PieceMsgCtx = ObJoinFilterCountRowPieceMsgCtx;
public:
  ObJoinFilterCountRowPieceMsg() {}
  ~ObJoinFilterCountRowPieceMsg() { reset(); }

  void reset() {
    total_rows_ = 0;
  };

  int assign(const ObJoinFilterCountRowPieceMsg &other) {
    int ret = OB_SUCCESS;
    each_sqc_has_full_data_ = each_sqc_has_full_data_;
    sqc_id_ = other.sqc_id_;
    total_rows_ = other.total_rows_;
    return ret;
  }
  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_JOIN_FILTER_COUNT_ROW_PIECE_MSG>,
                        K_(op_id), K_(each_sqc_has_full_data), K_(sqc_id), K_(total_rows));
  bool each_sqc_has_full_data_{false}; // True iff in shared hash join
  int64_t sqc_id_{OB_INVALID}; // From which sqc
  int64_t total_rows_{0}; // row count of one thread

private:
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterCountRowPieceMsg);
};


class ObJoinFilterCountRowWholeMsg
  : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_JOIN_FILTER_COUNT_ROW_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObJoinFilterCountRowWholeMsg>;
public:
  ObJoinFilterCountRowWholeMsg() {};
  ~ObJoinFilterCountRowWholeMsg() = default;
  int assign(const ObJoinFilterCountRowWholeMsg &other)
  {
    total_rows_ = other.total_rows_;
    return common::OB_SUCCESS;
  }

  inline int64_t get_total_rows() const { return total_rows_; }
  void reset()
  {
    total_rows_ = 0;
  }
  VIRTUAL_TO_STRING_KV(K_(total_rows));
  int64_t total_rows_{0};
};

struct JoinFilterSqcRowInfo
{
  JoinFilterSqcRowInfo() {}

  TO_STRING_KV(K(sqc_id_), K(expected_), K(received_), K(total_rows_));
  int64_t sqc_id_{OB_INVALID};
  int64_t expected_{0};
  int64_t received_{0};
  int64_t total_rows_{0};
};

class ObJoinFilterCountRowPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObJoinFilterCountRowPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts)
    : ObPieceMsgCtx(op_id, task_cnt, timeout_ts) {}
  ~ObJoinFilterCountRowPieceMsgCtx() = default;
  void reset_resource() override { sqc_row_infos_.reset(); }
  int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  int send_whole_msg_to_one_sqc(ObPxSqcMeta *sqc, int64_t total_row_in_sqc);
  static int alloc_piece_msg_ctx(const ObJoinFilterCountRowPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received), K_(sqc_row_infos));
  /*
  If one sqc only hash partitial data of the left, we use received_、total_rows_.
  e.g. for (none pkey)、 (hash hash), we should reveive k piece(k=dop) message and calculate the
  total rows.

  If each sqc has full data of the left, we use sqc_row_infos_.
  e.g. for shared hash join (bc2host none), each sqc hash the full data, we only need to calculate the
  total rows of one sqc.
  */
  int64_t received_{0};
  int64_t total_rows_{0};
  ObTMArray<JoinFilterSqcRowInfo> sqc_row_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterCountRowPieceMsgCtx);
};

class ObJoinFilterCountRowPieceMsgListener
{
public:
  ObJoinFilterCountRowPieceMsgListener() = default;
  ~ObJoinFilterCountRowPieceMsgListener() = default;
  static int on_message(
    ObJoinFilterCountRowPieceMsgCtx &piece_ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObJoinFilterCountRowPieceMsg &pkt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterCountRowPieceMsgListener);
};


}
}