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

#ifndef OCEANBASE_COMPONENTS_OB_DH_RANGE_DIST_WF_H_
#define OCEANBASE_COMPONENTS_OB_DH_RANGE_DIST_WF_H_

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"

namespace oceanbase
{
namespace sql
{

class ObRDWFPieceMsgListener;
class ObRDWFPieceMsgCtx;
class ObPxCoordInfo;

// Range distribution window function partial window info
class ObRDWFPartialInfo
{
  OB_UNIS_VERSION_V(1);
public:
  typedef int64_t RowExtType;
  explicit ObRDWFPartialInfo(common::ObArenaAllocator &alloc)
      : sqc_id_(0), thread_id_(0), first_row_(NULL), last_row_(NULL), alloc_(alloc)
  {
  }

  TO_STRING_KV(K(sqc_id_),
               K(thread_id_),
               K(first_row_),
               "first_row_frame_offset",
               NULL == first_row_ ? -1 : first_row_->extra_payload<RowExtType>(),
               K(last_row_),
               "last_row_frame_offset",
               NULL == last_row_ ? -1 : last_row_->extra_payload<RowExtType>());

  ObRDWFPartialInfo *dup(common::ObArenaAllocator &alloc) const;
  static ObStoredDatumRow *dup_store_row(common::ObArenaAllocator &alloc,
                                         const ObStoredDatumRow &row);
  void reset()
  {
    sqc_id_ = 0;
    thread_id_ = 0;
    first_row_ = NULL;
    last_row_ = NULL;
  }

  int64_t sqc_id_;
  int64_t thread_id_;
  ObStoredDatumRow *first_row_;
  ObStoredDatumRow *last_row_;
  common::ObArenaAllocator &alloc_;;
};

typedef ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_RANGE_DIST_WF_PIECE_MSG> ObRDWFPieceMsgBase;
class ObRDWFPieceMsg : public ObRDWFPieceMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
  using PieceMsgListener = ObRDWFPieceMsgListener;
  using PieceMsgCtx = ObRDWFPieceMsgCtx;

  ObRDWFPieceMsg() : info_(arena_alloc_)
  {
  }

  void reset()
  {
    info_.reset();
    arena_alloc_.reset();
  }

  common::ObArenaAllocator arena_alloc_;
  ObRDWFPartialInfo info_;
};

typedef ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_RANGE_DIST_WF_PIECE_MSG> ObRDWFWholeMsgBase;
class ObRDWFWholeMsg : public ObRDWFWholeMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<ObRDWFWholeMsg>;

  ObRDWFWholeMsg() : infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(arena_alloc_))
  {
  }

  void reset()
  {
    infos_.reset();
    arena_alloc_.reset();
  }

  int assign(const ObRDWFWholeMsg &msg);

  common::ObArenaAllocator arena_alloc_;
  common::ObSEArray<ObRDWFPartialInfo *, 16, common::ModulePageAllocator> infos_;
};

class ObRDWFPieceMsgCtx : public ObPieceMsgCtx
{
public:
  ObRDWFPieceMsgCtx(uint64_t op_id,
                           int64_t task_cnt,
                           int64_t timeout_ts,
                           ObExecContext &exec_ctx)
      : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), received_(0),
      infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(arena_alloc_)),
      exec_ctx_(exec_ctx), eval_ctx_(exec_ctx)
  {
  }
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  static int alloc_piece_msg_ctx(const ObRDWFPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  // When store row updated, the memory may not compact, formalize is
  // need to make memory layout compact for serialization.
  int formalize_store_row();

public:
  int64_t received_;
  common::ObArenaAllocator arena_alloc_;
  common::ObSEArray<ObRDWFPartialInfo *, 16, common::ModulePageAllocator> infos_;
  ObExecContext &exec_ctx_;
  ObEvalCtx eval_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRDWFPieceMsgCtx);
};

class ObRDWFPieceMsgListener
{
public:
  static int on_message(ObRDWFPieceMsgCtx &ctx,
                        common::ObIArray<ObPxSqcMeta *> &sqcs,
                        const ObRDWFPieceMsg &pkt);
};

typedef ObPieceMsgP<ObRDWFPieceMsg> ObRDWFPieceMsgP;
typedef ObWholeMsgP<ObRDWFWholeMsg> ObRDWFWholeMsgP;

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_COMPONENTS_OB_DH_RANGE_DIST_WF_H_
