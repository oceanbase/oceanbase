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

#ifndef __OB_SQL_ENGINE_PX_DATAHUB_BARRIER_PIECE_MSG_CTX_H__
#define __OB_SQL_ENGINE_PX_DATAHUB_BARRIER_PIECE_MSG_CTX_H__

#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace sql {

class ObPieceMsgCtx {
public:
  ObPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts)
      : op_id_(op_id), task_cnt_(task_cnt), timeout_ts_(timeout_ts)
  {}
  VIRTUAL_TO_STRING_KV(K_(op_id), K_(task_cnt));
  uint64_t op_id_;
  int64_t task_cnt_;
  int64_t timeout_ts_;
};

class ObPieceMsgCtxMgr {
public:
  ObPieceMsgCtxMgr() = default;
  ~ObPieceMsgCtxMgr() = default;
  void reset()
  {
    ctxs_.reset();
  }
  int find_piece_ctx(uint64_t op_id, ObPieceMsgCtx*& ctx)
  {
    int ret = common::OB_ENTRY_NOT_EXIST;
    ;
    for (int i = 0; i < ctxs_.count(); ++i) {
      if (ctxs_.at(i)->op_id_ == op_id) {
        ret = common::OB_SUCCESS;
        ctx = ctxs_.at(i);
        break;
      }
    }
    return ret;
  }
  int add_piece_ctx(ObPieceMsgCtx* ctx)
  {
    return ctxs_.push_back(ctx);
  }

private:
  common::ObSEArray<ObPieceMsgCtx*, 2> ctxs_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_ENGINE_PX_DATAHUB_BARRIER_PIECE_MSG_CTX_H__ */
//// end of header file
