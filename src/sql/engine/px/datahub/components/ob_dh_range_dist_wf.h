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

// Piece/Whole Msg used for vectorization 2.0
class RDWinFuncPXPieceMsgListener;
class RDWinFuncPXPieceMsgCtx;
class RDWinFuncPXPartialInfo
{
  OB_UNIS_VERSION_V(1);
public:
  RDWinFuncPXPartialInfo(common::ObArenaAllocator &alloc) :
    sqc_id_(0), thread_id_(0), row_meta_(&alloc), first_row_(nullptr), last_row_(nullptr),
    alloc_(alloc)
  {}
  RDWinFuncPXPartialInfo *dup(common::ObArenaAllocator &alloc) const;

  static ObCompactRow *dup_store_row(common::ObArenaAllocator &alloc, const ObCompactRow &row);

  TO_STRING_KV(K(sqc_id_),
             K(thread_id_),
             K(first_row_),
             "first_row_frame_offset",
             NULL == first_row_ ? -1 : *reinterpret_cast<const int64_t *>(first_row_->get_extra_payload(row_meta_)),
             K(last_row_),
             "last_row_frame_offset",
             NULL == last_row_ ? -1 : *reinterpret_cast<const int64_t *>(last_row_->get_extra_payload(row_meta_)));
  void reset()
  {
    sqc_id_ = 0;
    thread_id_ = 0;
    row_meta_.reset();
    first_row_ = nullptr;
    last_row_ = nullptr;
  }

  int64_t &first_row_frame_offset()
  {
    return *reinterpret_cast<int64_t *>(first_row_->get_extra_payload(row_meta_));
  }

  int64_t &last_row_frame_offset()
  {
    return *reinterpret_cast<int64_t *>(last_row_->get_extra_payload(row_meta_));
  }

  void get_cell(int64_t cell_idx, bool first_row, const char *&payload, int32_t &len)
  {
    (first_row) ? (first_row_->get_cell_payload(row_meta_, cell_idx, payload, len)) :
                  (last_row_->get_cell_payload(row_meta_, cell_idx, payload, len));
  }

  const char* get_cell(int64_t cell_idx, bool first_row)
  {
    return (first_row) ? (first_row_->get_cell_payload(row_meta_, cell_idx)) :
                         (last_row_->get_cell_payload(row_meta_, cell_idx));
  }

  bool is_null(int64_t cell_idx, bool first_row) const
  {
    if (first_row && first_row_ == nullptr) {
      return true;
    } else if (!first_row && last_row_ == nullptr) {
      return true;
    } else {
      return (first_row) ? (first_row_->is_null(cell_idx)) : (last_row_->is_null(cell_idx));
    }
  }

public:
  int64_t sqc_id_;
  int64_t thread_id_;
  RowMeta row_meta_;
  ObCompactRow *first_row_;
  ObCompactRow *last_row_;
  common::ObArenaAllocator &alloc_;
};

class RDWinFuncPXPieceMsg : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_RD_WINFUNC_PX_PIECE_MSG>
{
  OB_UNIS_VERSION_V(1);

public:
  using PieceMsgListener = RDWinFuncPXPieceMsgListener;
  using PieceMsgCtx = RDWinFuncPXPieceMsgCtx;
  RDWinFuncPXPieceMsg(const common::ObMemAttr &mem_attr) :
    arena_alloc_(mem_attr), info_(arena_alloc_)
  {}

  RDWinFuncPXPieceMsg() : arena_alloc_(), info_(arena_alloc_)
  {}

  void reset()
  {
    info_.reset();
    arena_alloc_.reset();
  }

  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_RD_WINFUNC_PX_PIECE_MSG>,
                       K_(info));

  common::ObArenaAllocator arena_alloc_;
  RDWinFuncPXPartialInfo info_;
};

class RDWinFuncPXWholeMsg: public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_RD_WINFUNC_PX_WHOLE_MSG>
{
  OB_UNIS_VERSION_V(1);
public:
  using WholeMsgProvider = ObWholeMsgProvider<RDWinFuncPXWholeMsg>;
  RDWinFuncPXWholeMsg(): infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(arena_alloc_)) {}

  RDWinFuncPXWholeMsg(const common::ObMemAttr &attr) :
    arena_alloc_(attr), infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(arena_alloc_))
  {}

  void reset()
  {
    infos_.reset();
    arena_alloc_.reset();
  }

  int assign(const RDWinFuncPXWholeMsg &other);
  common::ObArenaAllocator arena_alloc_;
  common::ObSEArray<RDWinFuncPXPartialInfo *, 16, common::ModulePageAllocator> infos_;
};

class RDWinFuncPXPieceMsgCtx: public ObPieceMsgCtx
{
public:
  RDWinFuncPXPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts,
                         ObExecContext &exec_ctx, const common::ObMemAttr &attr) :
    ObPieceMsgCtx(op_id, task_cnt, timeout_ts),
    received_(0), arena_alloc_(attr),
    infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(arena_alloc_)), exec_ctx_(exec_ctx),
    eval_ctx_(exec_ctx)
  {}

  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) override;
  virtual void reset_resource() override;
  static int alloc_piece_msg_ctx(const RDWinFuncPXPieceMsg &pkt,
                                 ObPxCoordInfo &coord_info,
                                 ObExecContext &ctx,
                                 int64_t task_cnt,
                                 ObPieceMsgCtx *&msg_ctx);
  int64_t received_;
  common::ObArenaAllocator arena_alloc_;
  common::ObSEArray<RDWinFuncPXPartialInfo *, 16, common::ModulePageAllocator> infos_;
  ObExecContext &exec_ctx_;
  ObEvalCtx eval_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(RDWinFuncPXPieceMsgCtx);
};

class RDWinFuncPXPieceMsgListener
{
public:
  static int on_message(RDWinFuncPXPieceMsgCtx &ctx,
                        common::ObIArray<ObPxSqcMeta *> &sqcs,
                        const RDWinFuncPXPieceMsg &pkt);
};

typedef ObPieceMsgP<RDWinFuncPXPieceMsg> ObRDWinFuncPXPieceMsgP;
typedef ObWholeMsgP<RDWinFuncPXWholeMsg> ObRDWinFuncPXWholeMsgP;

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_COMPONENTS_OB_DH_RANGE_DIST_WF_H_
