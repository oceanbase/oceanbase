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
 *
 * Compact serialize opt omits derivable ObExpr members and frame offsets.
 * The receiver restores them using the deterministic CG layout algorithm.
 * CG freezes the tenant switch and plan version decision in the physical plan;
 * DAS and PX consume that flag. Each record envelope is parsed once.
 * Legacy requests retain fixed-width references. PX retains legacy selected
 * Expr records and uses compact hole records when a partial layout needs them.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SER_COMPACT_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SER_COMPACT_H_

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "lib/utility/ob_unis_versioned.h"

namespace oceanbase
{
namespace sql
{
using common::serialization::SerCompactState;

class ObExecContext;

// ---------------------------------------------------------------------------
// Compact (compact serialize opt) expr record format
//
//   envelope : vi64 version(=SER_COMPACT_OPT_VERSION) + vi32 body_len
//   fixed    : vi32 type_ | 8B datum_meta_ | vi64 flag_ | vi32 frame_idx_ | vi64 bitmap
//   optional : segments in ascending bitmap-bit order, present iff bit set
//
// Members never on wire (receiver derives): obj_datum_map_, vec_value_tc_,
// len_, batch_idx_mask_, basic_funcs_, and all frame layout offsets
// (datum_off_/res_buf_off_/eval_info_off_/...). For plain fixed-length types
// obj_meta_/max_length_/res_buf_len_ are also omitted and rebuilt from
// datum_meta_.
// ---------------------------------------------------------------------------
// presence bitmap bits of the compact format
static const uint16_t V4_HAS_OBJ_META = 1 << 0;
static const uint16_t V4_HAS_MAX_LENGTH = 1 << 1;
static const uint16_t V4_HAS_RES_BUF_LEN = 1 << 2;
static const uint16_t V4_HAS_ARGS = 1 << 3;
static const uint16_t V4_HAS_PARENTS = 1 << 4;
static const uint16_t V4_HAS_INNER_FUNCS = 1 << 5;
static const uint16_t V4_HAS_EXTRA = 1 << 6;
static const uint16_t V4_HAS_EXPR_CTX_ID = 1 << 7;
static const uint16_t V4_HAS_EXTRA_INFO = 1 << 8;
static const uint16_t V4_HAS_LOCAL_VAR = 1 << 9;
static const uint16_t V4_HAS_ATTRS = 1 << 10;
static const uint16_t V4_HAS_EVAL_FUNCS = 1 << 11;
// Hole placeholder used to retain a complete Expr member sequence without
// serializing an unselected expression body.
static const uint16_t V4_IS_HOLE = 1 << 12;

struct ObExprReplayCtx;

OB_INLINE int64_t serialized_expr_count(const ObExprFrameInfo &expr_frame_info, const bool complete_members)
{
  return complete_members || !expr_frame_info.is_mark_serialize() ? expr_frame_info.rt_exprs_.count() :
                                                                    expr_frame_info.ser_expr_marks_.count();
}

OB_INLINE bool expr_frame_member_selected(const ObExprFrameInfo &expr_frame_info, const int64_t idx)
{
  return idx >= 0 && idx < expr_frame_info.rt_exprs_.count() && T_INVALID != expr_frame_info.rt_exprs_.at(idx).type_
         && (!expr_frame_info.is_mark_serialize()
             || (idx < expr_frame_info.ser_expr_marks_.count() && expr_frame_info.ser_expr_marks_.at(idx)));
}

// Standard UNIS compact serialize opt record. expr_ is a non-owning bridge to optional ObExpr
// members; it is never serialized. Fixed logical fields live in this header so
// encode/decode/size share one record definition and one standard UNIS
// version/body-size envelope.
struct ObExprCompactHeader
{
  ObExprCompactHeader() : type_(T_INVALID), datum_meta_(), flag_(0), frame_idx_(0), bitmap_(0), expr_(NULL)
  {}
  explicit ObExprCompactHeader(ObExpr &expr) : ObExprCompactHeader()
  {
    expr_ = &expr;
  }
  ObExprCompactHeader(const ObExpr &expr, bool as_hole);
  OB_UNIS_VERSION(common::serialization::SER_COMPACT_OPT_VERSION);

public:
  int replay_to(ObExpr &dst, ObExprReplayCtx &ctx) const;
  TO_STRING_KV(K_(type), K_(datum_meta), K_(flag), K_(frame_idx), K_(bitmap));

  ObExprOperatorType type_;
  ObDatumMeta datum_meta_;
  uint64_t flag_;
  uint32_t frame_idx_;
  uint16_t bitmap_;
  ObExpr *expr_;
};

enum ObExprFrameReplayKind
{
  EXPR_FRAME_REPLAY_CONST = 0,
  EXPR_FRAME_REPLAY_PARAM,
  EXPR_FRAME_REPLAY_DYNAMIC,
  EXPR_FRAME_REPLAY_ITEM,
  EXPR_FRAME_REPLAY_VECTOR
};

struct ObExprFrameReplayState
{
  ObExprFrameReplayState() : frame_(NULL), kind_(EXPR_FRAME_REPLAY_ITEM), members_(), replayed_(false)
  {}
  TO_STRING_KV(KP_(frame), K_(kind), K_(members), K_(replayed));

  const ObFrameInfo *frame_;
  ObExprFrameReplayKind kind_;
  common::ObSEArray<int64_t, 16> members_;
  bool replayed_;
};

struct ObExprReplayCtx
{
  ObExprReplayCtx(common::ObIArray<ObExpr> &exprs, const common::ObIArray<ObFrameInfo> &const_frames,
                  const common::ObIArray<ObFrameInfo> &param_frames,
                  const common::ObIArray<ObFrameInfo> &dynamic_frames,
                  const common::ObIArray<ObFrameInfo> &datum_frames, int64_t cg_batch_size, int64_t original_param_cnt,
                  bool partial_frame = false) :
    exprs_(exprs), const_frames_(const_frames), param_frames_(param_frames), dynamic_frames_(dynamic_frames),
    datum_frames_(datum_frames), cg_batch_size_(cg_batch_size), original_param_cnt_(original_param_cnt),
    frame_states_(), replayed_cnt_(0), prepared_(false), partial_frame_(partial_frame)
  {}

  int prepare(const common::ObIArray<ObExprCompactHeader> &headers);
  int replay(ObExpr &dst);
  int check() const;

  common::ObIArray<ObExpr> &exprs_;
  const common::ObIArray<ObFrameInfo> &const_frames_;
  const common::ObIArray<ObFrameInfo> &param_frames_;
  const common::ObIArray<ObFrameInfo> &dynamic_frames_;
  const common::ObIArray<ObFrameInfo> &datum_frames_;
  int64_t cg_batch_size_;
  int64_t original_param_cnt_;
  common::ObSEArray<ObExprFrameReplayState, 4> frame_states_;
  int64_t replayed_cnt_;
  bool prepared_;
  bool partial_frame_;
};

struct ObDatumMetaReplayCtx
{};

struct ObDatumMetaCompact
{
  ObDatumMetaCompact() : meta_(NULL)
  {}
  explicit ObDatumMetaCompact(const ObDatumMeta &meta) : meta_(const_cast<ObDatumMeta *>(&meta))
  {}
  OB_UNIS_VERSION(common::serialization::SER_COMPACT_OPT_VERSION);

public:
  int replay_to(ObDatumMeta &dst, ObDatumMetaReplayCtx &) const;
  TO_STRING_KV(KPC_(meta));

  ObDatumMeta *meta_;
};

// ---------------------------------------------------------------------------
// Single-expr compact serialize opt codec (free functions over ObExpr's public members).
// Record format documented above.
// ---------------------------------------------------------------------------
int encode_expr_v4(const ObExpr &expr, char *buf, int64_t buf_len, int64_t &pos, bool as_hole);
int64_t expr_v4_size(const ObExpr &expr, bool as_hole);

// Leaf encoding helpers. Request boundaries establish the reference codec context.
int encode_v4(const ObExpr *expr, char *buf, int64_t buf_len, int64_t &pos, bool as_hole);
int64_t get_serialize_size_v4(const ObExpr *expr, bool as_hole);
int encode_v4(const ObDatumMeta *meta, char *buf, int64_t buf_len, int64_t &pos);
int64_t get_serialize_size_v4(const ObDatumMeta *meta);

// ---------------------------------------------------------------------------
// Rebuild all frame layout offsets of rt_exprs on the receiver side by
// replaying the deterministic CG layout algorithm
// (ObStaticEngineExprCG::cg_all_frame_layout). Only meaningful for exprs
// decoded from compact records, which do not carry frame offsets.
// Returns OB_ERR_UNEXPECTED if the replayed layout does not fit the received
// ObFrameInfo sizes (indicates a replay/CG divergence bug).
// ---------------------------------------------------------------------------
int replay_expr_frame_layout(common::ObIArray<ObExpr> &rt_exprs, const common::ObIArray<ObFrameInfo> &const_frames,
                             const common::ObIArray<ObFrameInfo> &param_frames,
                             const common::ObIArray<ObFrameInfo> &dynamic_frames,
                             const common::ObIArray<ObFrameInfo> &datum_frames, const int64_t cg_batch_size,
                             const int64_t original_param_cnt, const bool partial_frame = false);

// ---------------------------------------------------------------------------
// DAS Expr-frame adapter. DAS installs SerCompactEncodingGuard at its request
// boundary. Direct PX
// call sites keep legacy selected Expr records; ObPxTreeSerializer may add compact serialize opt
// holes only to preserve the complete layout of a partial frame.
// ---------------------------------------------------------------------------
int encode_das_expr_frame_head(char *buf, int64_t buf_len, int64_t &pos, ObExecContext &ctx,
                               const ObExprFrameInfo &expr_frame_info);
int decode_das_expr_frame_head(const char *buf, int64_t data_len, int64_t &pos, ObExecContext &ctx,
                               const ObExprFrameInfo &expr_frame_info);
int64_t das_expr_frame_head_size(ObExecContext &ctx, const ObExprFrameInfo &expr_frame_info);

struct ObDASExprFrameLayout
{
  ObDASExprFrameLayout() : cg_batch_size_(0), original_param_cnt_(0), frame_cnt_(0), partial_frame_(false)
  {}
  int init(ObExecContext &ctx, bool partial_frame);
  int serialize_meta(char *buf, int64_t buf_len, int64_t &pos, const ObExprFrameInfo &info) const;
  int deserialize_meta(const char *buf, int64_t data_len, int64_t &pos, const ObExprFrameInfo &info);
  int64_t meta_size(const ObExprFrameInfo &info) const;
  int64_t cg_batch_size_;
  int64_t original_param_cnt_;
  int64_t frame_cnt_;
  bool partial_frame_;
};

int encode_das_exprs(char *buf, int64_t buf_len, int64_t &pos, const ObExprFrameInfo &expr_frame_info);
int64_t das_exprs_size(const ObExprFrameInfo &expr_frame_info);

// ---------------------------------------------------------------------------
// RAII guards (moved here so every channel shares one definition).
// ---------------------------------------------------------------------------

// Point ObExpr::get_serialize_array() at the rt_exprs array for the duration
// of encode/decode so expr references can travel as array indexes; restores
// the previous value on scope exit (fixes the legacy leak of the global).
class ObExprSerializeArrayGuard final
{
public:
  ObExprSerializeArrayGuard(const bool has_expr, const ObExprFrameInfo *frame_info) :
    saved_(ObExpr::get_serialize_array())
  {
    ObExpr::get_serialize_array() =
      has_expr && frame_info != nullptr ? &const_cast<ObExprFrameInfo *>(frame_info)->rt_exprs_ : nullptr;
  }
  explicit ObExprSerializeArrayGuard(ObExpr::ObExprIArray *array) : saved_(ObExpr::get_serialize_array())
  {
    ObExpr::get_serialize_array() = array;
  }
  ~ObExprSerializeArrayGuard()
  {
    ObExpr::get_serialize_array() = saved_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSerializeArrayGuard);
  ObExpr::ObExprIArray *saved_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SER_COMPACT_H_
