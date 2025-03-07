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
#include "lib/container/ob_se_array.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/utility/ob_hyperloglog.h"

namespace oceanbase {
namespace sql {

class ObJoinFilterCountRowPieceMsg;
class ObJoinFilterCountRowWholeMsg;
typedef ObPieceMsgP<ObJoinFilterCountRowPieceMsg> ObJoinFilterCountRowPieceMsgP;
typedef ObWholeMsgP<ObJoinFilterCountRowWholeMsg> ObJoinFilterCountRowWholeMsgP;
class ObJoinFilterCountRowPieceMsgListener;
class ObPxCoordInfo;

class ObJoinFilterNdv final
{
  OB_UNIS_VERSION(1);
public:
  static int gather_piece_ndv(const ObJoinFilterNdv &piece_ndv, ObJoinFilterNdv &total_ndv);
public:
  ObJoinFilterNdv(): in_filter_active_(false) {}

  ~ObJoinFilterNdv() { reset(); }

  int init_ndv_info(int32_t n_bit) {
    int ret = OB_SUCCESS;
    if (n_bit <= 0) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "n_bit should greater than 0", K(ret), K(n_bit));
    } else {
      use_hllc_estimate_ndv_ = true;
      ret = hllc_.init(&allocator_, n_bit);
    }
    return ret;
  }

  int assign(const ObJoinFilterNdv &other) {
    in_filter_active_ = other.in_filter_active_;
    in_filter_ndv_ = other.in_filter_ndv_;
    use_hllc_estimate_ndv_ = other.use_hllc_estimate_ndv_;
    bf_ndv_ = other.bf_ndv_;
    return hllc_.assign(other.hllc_, &allocator_);
  }

  void copy_without_hllc(const ObJoinFilterNdv &other) {
    in_filter_active_ = other.in_filter_active_;
    in_filter_ndv_ = other.in_filter_ndv_;
    use_hllc_estimate_ndv_ = other.use_hllc_estimate_ndv_;
    bf_ndv_ = other.bf_ndv_;
  }

  inline void reset() {
    in_filter_active_ = false;
    in_filter_ndv_ = 0;
    bf_ndv_ = 0;
    hllc_.destroy();
  }

  TO_STRING_KV(K_(in_filter_active), K_(hllc));
  bool in_filter_active_{false};
  uint64_t in_filter_ndv_{0};
  // == 435 BP1 Begin to Use ==
  bool use_hllc_estimate_ndv_{false};
  uint64_t bf_ndv_{0};
  common::ObArenaAllocator allocator_;  //help to init hllc when deserialization, don't need to be serialized
  ObHyperLogLogCalculator hllc_;
  // == 435 BP1 End to Use ==
private:
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterNdv);
};

typedef ObSEArray<ObJoinFilterNdv, 4> ObJoinFilterNdvInfo;

struct JoinFilterSqcRowInfo
{
  JoinFilterSqcRowInfo() {}

  TO_STRING_KV(K(sqc_id_), K(expected_), K(received_), K(total_rows_), K(ndv_info_));
  int64_t sqc_id_{OB_INVALID};
  int64_t expected_{0};
  int64_t received_{0};
  int64_t total_rows_{0};
  ObJoinFilterNdvInfo ndv_info_;
};

class ObJoinFilterCountRowPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObJoinFilterCountRowPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts)
    : ObPieceMsgCtx(op_id, task_cnt, timeout_ts) {}
  ~ObJoinFilterCountRowPieceMsgCtx() = default;
  void reset_resource() override
  {
    received_ = 0;
    total_rows_ = 0;
    ndv_info_.reset();
    sqc_row_infos_.reset();
  }
  int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  int send_whole_msg_to_one_sqc(ObPxSqcMeta *sqc, JoinFilterSqcRowInfo &sqc_row_info);

  static int init_target_ndv_info(const ObJoinFilterNdvInfo &source_ndv_info,
                                  ObJoinFilterNdvInfo &target_ndv_info)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(target_ndv_info.prepare_allocate(source_ndv_info.count()))) {
      SQL_LOG(WARN, "failed to prepare_allocate");
    } else {
      for (int64_t i = 0; i < source_ndv_info.count() && OB_SUCC(ret); ++i) {
        target_ndv_info.at(i).in_filter_active_ = true;
        // pkt.ndv_info_.at(i).hllc_.get_n_bit() may == 0, when there is a lower version pkt send to
        // this higher version DataHub
        // So we set target_ndv_info.at(i).in_filter_active_ = true above explicitly
        int32_t n_bit = source_ndv_info.at(i).hllc_.get_n_bit();
        if (source_ndv_info.at(i).use_hllc_estimate_ndv_ && OB_FAIL(target_ndv_info.at(i).init_ndv_info(n_bit))) {
          SQL_LOG(WARN, "fail to init dh or sqc's hyperloglog in joinFilter senario", K(ret));
        }
      }
    }
    return ret;
  }
  static int alloc_piece_msg_ctx(const ObJoinFilterCountRowPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received), K_(sqc_row_infos));
  /*
  If one sqc only has partial data of the left, we use total_rows_.
  e.g. for (none pkey)、 (hash hash), we should receive k piece(k=dop) message and calculate the
  total rows.

  If each sqc has full data of the left, we use sqc_row_infos_.
  e.g. for shared hash join (bc2host none), each sqc hash the full data, we only need to calculate the
  total rows of one sqc.
  */
  int64_t received_{0};
  int64_t total_rows_{0};
  ObJoinFilterNdvInfo ndv_info_;
  ObTMArray<JoinFilterSqcRowInfo> sqc_row_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterCountRowPieceMsgCtx);
};

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
    ndv_info_.reset();
  };

  inline int assign(const ObJoinFilterCountRowPieceMsg &other)
  {
    int ret = OB_SUCCESS;
    each_sqc_has_full_data_ = other.each_sqc_has_full_data_;
    sqc_id_ = other.sqc_id_;
    total_rows_ = other.total_rows_;
    if (OB_FAIL(ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_JOIN_FILTER_COUNT_ROW_PIECE_MSG>::assign(
            other))) {
      SQL_LOG(WARN, "failed to assign base");
    } else if (OB_FAIL(ndv_info_.assign(other.ndv_info_))) {
      SQL_LOG(WARN, "failed to assign ndv_info_");
    }
    return ret;
  }

  inline int aggregate_piece(const dtl::ObDtlMsg &piece) override final
  {
    int ret = OB_SUCCESS;
    const ObJoinFilterCountRowPieceMsg &detail_other_piece =
      static_cast<const ObJoinFilterCountRowPieceMsg &>(piece);
    if (piece_count_ == 0) {
      if (OB_FAIL(assign(detail_other_piece))) {
        SQL_LOG(WARN, "failed to assign");
      }
    } else {
      for (int64_t i = 0; i < detail_other_piece.ndv_info_.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(ObJoinFilterNdv::gather_piece_ndv(detail_other_piece.ndv_info_.at(i), ndv_info_.at(i)))) {
          SQL_LOG(WARN, "fail to gather piece ndv", K(ret), K(i));
        }
        SQL_LOG(TRACE, "[NDV_BLOOM_FILTER][SQC_LOCAL_AGGR] aggregate piece to *piece*:", K(i),
                  K(detail_other_piece.ndv_info_.at(i)),
                  K(detail_other_piece.ndv_info_.at(i).hllc_.estimate()), K(ndv_info_.at(i)),
                  K(ndv_info_.at(i).hllc_.estimate()));
      }
      total_rows_ += detail_other_piece.total_rows_;
      piece_count_++;
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_JOIN_FILTER_COUNT_ROW_PIECE_MSG>,
                        K_(op_id), K_(each_sqc_has_full_data), K_(sqc_id), K_(total_rows), K(ndv_info_));
  bool each_sqc_has_full_data_{false}; // True iff in shared hash join
  int64_t sqc_id_{OB_INVALID}; // From which sqc
  int64_t total_rows_{0}; // row count of one thread
  ObJoinFilterNdvInfo ndv_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterCountRowPieceMsg);
};


class ObJoinFilterCountRowWholeMsg
  : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_JOIN_FILTER_COUNT_ROW_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider =
      ObWholeMsgProvider<ObJoinFilterCountRowPieceMsg, ObJoinFilterCountRowWholeMsg>;

public:
  ObJoinFilterCountRowWholeMsg() {};
  ~ObJoinFilterCountRowWholeMsg() = default;
  inline int64_t get_total_rows() const { return total_rows_; }
  inline const ObJoinFilterNdvInfo &get_ndv_info() const { return ndv_info_; }

  inline int assign(const ObJoinFilterCountRowWholeMsg &other)
  {
    total_rows_ = other.total_rows_;
    return ndv_info_.assign(other.get_ndv_info());
  }
  void reset()
  {
    total_rows_ = 0;
    ndv_info_.reset();
  }

  inline int aggregate_piece(const dtl::ObDtlMsg &piece) override final
  {
    int ret = OB_SUCCESS;
    const ObJoinFilterCountRowPieceMsg &detail_other_piece =
      static_cast<const ObJoinFilterCountRowPieceMsg &>(piece);
    if (ndv_info_.empty()) {
      if (OB_FAIL(ndv_info_.assign(detail_other_piece.ndv_info_))) {
        SQL_LOG(WARN, "failed to assign ndv_info_");
      } else {
        total_rows_ = detail_other_piece.total_rows_;
      }
    } else {
      for (int64_t i = 0; i < detail_other_piece.ndv_info_.count(); ++i) {
        ObJoinFilterNdv::gather_piece_ndv(detail_other_piece.ndv_info_.at(i), ndv_info_.at(i));
        SQL_LOG(TRACE, "[NDV_BLOOM_FILTER][SQC_LOCAL_AGGR] aggregate piece to *whole*:", K(i),
                  K(detail_other_piece.ndv_info_.at(i)),
                  K(detail_other_piece.ndv_info_.at(i).hllc_.estimate()), K(ndv_info_.at(i)),
                  K(ndv_info_.at(i).hllc_.estimate()));
      }
      total_rows_ += detail_other_piece.total_rows_;
    }
    return ret;
  }

  inline int after_aggregate_piece() override final
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < ndv_info_.count(); ++i) {
      ndv_info_.at(i).bf_ndv_ = ndv_info_.at(i).hllc_.estimate();
      //we need is the ndv that already computed by hllc
      //we don't need to send the hllc data structure back to each px thread
      ndv_info_.at(i).hllc_.destroy();
    }
    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(total_rows), K_(ndv_info));
  int64_t total_rows_{0};
  ObJoinFilterNdvInfo ndv_info_;
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