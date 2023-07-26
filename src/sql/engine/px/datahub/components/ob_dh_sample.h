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

#ifndef __OB_SQL_ENG_PX_DH_SAMPLE_H__
#define __OB_SQL_ENG_PX_DH_SAMPLE_H__

#include "lib/lock/ob_spin_lock.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "sql/engine/px/ob_px_basic_info.h"

namespace oceanbase
{
namespace sql
{

class ObDynamicSamplePieceMsg;
class ObDynamicSampleWholeMsg;
typedef ObPieceMsgP<ObDynamicSamplePieceMsg> ObDynamicSamplePieceMsgP;
typedef ObWholeMsgP<ObDynamicSampleWholeMsg> ObDynamicSampleWholeMsgP;
class ObDynamicSamplePieceMsgListener;
class ObDynamicSamplePieceMsgCtx;
class ObPxCoordSpec;
class ObPxCoordOp;
class ObPxCoordInfo;

class ObDynamicSamplePieceMsg
  : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_DYNAMIC_SAMPLE_PIECE_MSG>

{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObDynamicSamplePieceMsgListener;
  using PieceMsgCtx = ObDynamicSamplePieceMsgCtx;
  using Parent = ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_DYNAMIC_SAMPLE_PIECE_MSG>;
public:
  ObDynamicSamplePieceMsg();
  virtual ~ObDynamicSamplePieceMsg() = default;
  void reset();
  bool is_valid() const;
  bool is_row_sample() const { return ObPxSampleType::HEADER_INPUT_SAMPLE == sample_type_||
                                      ObPxSampleType::FULL_INPUT_SAMPLE == sample_type_; }
  bool is_object_sample() const { return ObPxSampleType::OBJECT_SAMPLE == sample_type_; }
  int merge_piece_msg(int64_t, ObDynamicSamplePieceMsg &, bool &);
  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_DYNAMIC_SAMPLE_PIECE_MSG>,
                       K(expect_range_count_), K(tablet_ids_), K(row_stores_));
public:
  int64_t expect_range_count_;
  ObSEArray<uint64_t, 1> tablet_ids_;
  ObPxSampleType sample_type_;
  Ob2DArray<ObPxTabletRange> part_ranges_;
  ObArray<ObChunkDatumStore *> row_stores_;
  ObArenaAllocator arena_; // for deserialize
  common::ObSpinLock spin_lock_; // for merge piece msg
private:
  DISALLOW_COPY_AND_ASSIGN(ObDynamicSamplePieceMsg);
};

class ObDynamicSampleWholeMsg
    : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_DYNAMIC_SAMPLE_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObDynamicSampleWholeMsg>;
public:
  ObDynamicSampleWholeMsg();
  virtual ~ObDynamicSampleWholeMsg() = default;
  int assign(const ObDynamicSampleWholeMsg &other, common::ObIAllocator *allocator = NULL);
  void reset();
  INHERIT_TO_STRING_KV("meta", ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_DYNAMIC_SAMPLE_WHOLE_MSG>,
                       K_(op_id), K_(part_ranges));
public:
  common::Ob2DArray<ObPxTabletRange> part_ranges_;
  common::ObArenaAllocator assign_allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDynamicSampleWholeMsg);
};

class ObDynamicSamplePieceMsgCtx : public ObPieceMsgCtx
{
public:
  struct SortDef
  {
    SortDef(): exprs_(NULL), collations_(NULL), cmp_funs_(NULL) {}

    const ExprFixedArray *exprs_;
    const ObSortCollations *collations_;
    const ObSortFuncs *cmp_funs_;
  };
public:
  ObDynamicSamplePieceMsgCtx(
      uint64_t op_id,
      int64_t task_cnt,
      int64_t timeout_ts,
      int64_t tenant_id,
      ObExecContext &exec_ctx,
      ObPxCoordOp &coord,
      const SortDef &sort_def);
  virtual ~ObDynamicSamplePieceMsgCtx() = default;
  int init(const ObIArray<uint64_t> &tablet_ids);
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  virtual void destroy();
  int process_piece(const ObDynamicSamplePieceMsg &piece);
  int split_range(
      const ObChunkDatumStore *sample_store,
      const int64_t expect_range_count,
      ObPxTabletRange::RangeCut &range_cut);
  int sort_row_store(ObChunkDatumStore &row_store);
  int build_whole_msg(ObDynamicSampleWholeMsg &whole_msg);
  int on_message(
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      const ObDynamicSamplePieceMsg &piece);
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K(is_inited_), K(tenant_id_), K(received_), K(succ_count_), K(tablet_ids_));
  static int alloc_piece_msg_ctx(const ObDynamicSamplePieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
private:
    int append_object_sample_data(const ObDynamicSamplePieceMsg &piece,
        int64_t index, ObChunkDatumStore* sample_store);
    void destroy_sample_stores();
public:
  bool is_inited_;
  int64_t tenant_id_;
  int received_; // received piece count
  int succ_count_;
  ObArray<uint64_t> tablet_ids_;
  int64_t expect_range_count_;
  ObArray<ObChunkDatumStore *> sample_stores_;
  ObMonitorNode op_monitor_info_;
  ObSortOpImpl sort_impl_;
  ObExecContext &exec_ctx_;
  ObChunkDatumStore::ShadowStoredRow last_store_row_;
  ObPxCoordOp &coord_;
  SortDef sort_def_;
  ObArenaAllocator arena_;
  lib::ObMutex mutex_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDynamicSamplePieceMsgCtx);
};

class ObDynamicSamplePieceMsgListener
{
public:
  ObDynamicSamplePieceMsgListener() = default;
  ~ObDynamicSamplePieceMsgListener() = default;
  static int on_message(
      ObDynamicSamplePieceMsgCtx &ctx,
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      const ObDynamicSamplePieceMsg &pkt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDynamicSamplePieceMsgListener);
};


}
}
#endif /* __OB_SQL_ENG_PX_DH_SAMPLE_H__ */
//// end of header file

