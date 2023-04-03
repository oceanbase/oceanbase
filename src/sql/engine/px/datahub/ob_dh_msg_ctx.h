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

namespace oceanbase
{
namespace sql
{

class ObPieceMsgCtx
{
public:
  ObPieceMsgCtx(uint64_t op_id, int64_t task_cnt, int64_t timeout_ts)
      : op_id_(op_id), task_cnt_(task_cnt), timeout_ts_(timeout_ts) {}
  virtual ~ObPieceMsgCtx() {}
  virtual int send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs) { return OB_SUCCESS; };
  virtual void reset_resource() = 0;
  VIRTUAL_TO_STRING_KV(K_(op_id), K_(task_cnt));
  virtual void destroy() {}
  uint64_t op_id_;    // 哪个算子使用 datahub 服务
  int64_t task_cnt_;  // 这个 dfo 下实际执行的 task 数, 即：期望收到的 piece 数量
  int64_t timeout_ts_; //超时时间, DTL发送消息时会使用
};

class ObPieceMsgCtxMgr
{
public:
  ObPieceMsgCtxMgr() = default;
  ~ObPieceMsgCtxMgr() = default;
  void reset()
  {
    for (int i = 0; i < ctxs_.count(); ++i) {
      if (OB_NOT_NULL(ctxs_[i])) {
        ctxs_[i]->destroy();
        ctxs_[i]->~ObPieceMsgCtx();
      }
    }
    ctxs_.reset();
    types_.reset();
  }
  int find_piece_ctx(uint64_t op_id, dtl::ObDtlMsgType type, ObPieceMsgCtx *&ctx)
  {
    int ret = common::OB_ENTRY_NOT_EXIST;;
    for (int i = 0; i < ctxs_.count(); ++i) {
      if (ctxs_.at(i)->op_id_ == op_id && types_.at(i) == type) {
        ret = common::OB_SUCCESS;
        ctx = ctxs_.at(i);
        break;
      }
    }
    return ret;
  }
  int add_piece_ctx(ObPieceMsgCtx *ctx, dtl::ObDtlMsgType type)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ctxs_.push_back(ctx))) {
    } else if (OB_FAIL(types_.push_back(type))) {
    }
    return ret;
  }
private:
  common::ObSEArray<ObPieceMsgCtx *, 2> ctxs_;
  common::ObSEArray<dtl::ObDtlMsgType, 2> types_;
};

}
}
#endif /* __OB_SQL_ENGINE_PX_DATAHUB_BARRIER_PIECE_MSG_CTX_H__ */
//// end of header file


