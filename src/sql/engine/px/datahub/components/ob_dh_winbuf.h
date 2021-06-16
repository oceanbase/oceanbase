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

#ifndef __OB_SQL_ENG_PX_DH_WINBUF_H__
#define __OB_SQL_ENG_PX_DH_WINBUF_H__

#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase {
namespace sql {

class ObWinbufPieceMsg;
class ObWinbufWholeMsg;
typedef ObPieceMsgP<ObWinbufPieceMsg> ObWinbufPieceMsgP;
typedef ObWholeMsgP<ObWinbufWholeMsg> ObWinbufWholeMsgP;
class ObWinbufPieceMsgListener;
class ObWinbufPieceMsgCtx;

class ObWinbufPieceMsg : public ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_WINBUF_PIECE_MSG>

{
  OB_UNIS_VERSION_V(1);

public:
  using PieceMsgListener = ObWinbufPieceMsgListener;
  using PieceMsgCtx = ObWinbufPieceMsgCtx;

public:
  ObWinbufPieceMsg()
      : is_end_(false),
        is_datum_(false),
        col_count_(0),
        row_(),
        datum_row_(NULL),
        row_size_(0),
        payload_len_(0),
        deseria_allocator_()
  {}
  ~ObWinbufPieceMsg() = default;
  void reset()
  {}
  INHERIT_TO_STRING_KV("meta", ObDatahubPieceMsg<dtl::ObDtlMsgType::DH_WINBUF_PIECE_MSG>, K_(op_id));

public:
  /* functions */
  /* variables */

  bool is_end_;
  bool is_datum_;  // data is ObNewRow or datum array (static engine) format.
  int64_t col_count_;
  ObNewRow row_;
  ObChunkDatumStore::StoredRow* datum_row_;
  uint32_t row_size_;    // for datum row use
  int64_t payload_len_;  // for datum row use
  common::ObArenaAllocator deseria_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObWinbufPieceMsg);
};

class ObWinbufWholeMsg : public ObDatahubWholeMsg<dtl::ObDtlMsgType::DH_WINBUF_WHOLE_MSG> {
  OB_UNIS_VERSION_V(1);

public:
  using WholeMsgProvider = ObWholeMsgProvider<ObWinbufWholeMsg>;

public:
  ObWinbufWholeMsg()
      : ready_state_(0), is_empty_(true), is_datum_(false), row_store_(), datum_store_(), assign_allocator_()
  {}
  ~ObWinbufWholeMsg() = default;
  int assign(const ObWinbufWholeMsg& other, common::ObIAllocator* allocator = NULL);
  void reset()
  {
    ready_state_ = 0;
    is_empty_ = true;
    is_datum_ = false;
    row_store_.reset();
    datum_store_.reset();
  }
  VIRTUAL_TO_STRING_KV(K_(ready_state));
  int ready_state_;
  bool is_empty_;
  bool is_datum_;  // data is ObNewRow or datum array (static engine) format.
  sql::ObChunkRowStore row_store_;
  sql::ObChunkDatumStore datum_store_;
  common::ObArenaAllocator assign_allocator_;
};

class ObWinbufPieceMsgCtx : public ObPieceMsgCtx {
public:
  ObWinbufPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts, int64_t tenant_id)
      : ObPieceMsgCtx(op_id, task_cnt, timeout_ts), received_(0), tenant_id_(tenant_id), whole_msg_()
  {}
  ~ObWinbufPieceMsgCtx() = default;
  INHERIT_TO_STRING_KV("meta", ObPieceMsgCtx, K_(received));
  static int alloc_piece_msg_ctx(
      const ObWinbufPieceMsg& pkt, ObExecContext& ctx, int64_t task_cnt, ObPieceMsgCtx*& msg_ctx);
  int received_;
  int64_t tenant_id_;
  ObWinbufWholeMsg whole_msg_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObWinbufPieceMsgCtx);
};

class ObWinbufPieceMsgListener {
public:
  ObWinbufPieceMsgListener() = default;
  ~ObWinbufPieceMsgListener() = default;
  static int on_message(ObWinbufPieceMsgCtx& ctx, common::ObIArray<ObPxSqcMeta*>& sqcs, const ObWinbufPieceMsg& pkt);

private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObWinbufPieceMsgListener);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_ENG_PX_DH_WINBUF_H__ */
//// end of header file
