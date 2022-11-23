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

#ifndef _OB_SQL_ENGINE_PX_DATAHUB_DH_MSG_H__
#define _OB_SQL_ENGINE_PX_DATAHUB_DH_MSG_H__

#include "sql/dtl/ob_dtl_msg.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/ob_io_event_observer.h"

namespace oceanbase
{
namespace sql
{

/* base header for all piece and whole msgs*/

template <dtl::ObDtlMsgType T>
class ObDatahubPieceMsg : public dtl::ObDtlMsgTemp<T>
{
  OB_UNIS_VERSION_V(1);
public:
  ObDatahubPieceMsg()
      : op_id_(common::OB_INVALID_ID),
        dfo_id_(common::OB_INVALID_ID),
        thread_id_(0),
        piece_count_(0)
    {}
  virtual ~ObDatahubPieceMsg() = default;
  VIRTUAL_TO_STRING_KV(K_(op_id), K_(dfo_id), K_(thread_id));
  uint64_t op_id_;   // 在 piece 消息处理中，用于寻址 QC 端 ctx
  uint64_t dfo_id_;   // 偷懒代码：在 piece 消息处理中，用于寻址 QC 端 dfo
  uint64_t thread_id_; // debug
  uint64_t piece_count_; //record piece count
};

template <dtl::ObDtlMsgType T>
class ObDatahubWholeMsg : public dtl::ObDtlMsgTemp<T>
{
  OB_UNIS_VERSION_V(1);
public:
  ObDatahubWholeMsg() : op_id_(common::OB_INVALID_ID) {}
  virtual ~ObDatahubWholeMsg() = default;
  VIRTUAL_TO_STRING_KV(K_(op_id));
  uint64_t op_id_;   // 在 whole 消息处理中，用于寻址 SQC 端 msg provider
};

OB_SERIALIZE_MEMBER_TEMP(template<dtl::ObDtlMsgType T>, ObDatahubWholeMsg<T>,
                         op_id_);
OB_SERIALIZE_MEMBER_TEMP(template<dtl::ObDtlMsgType T>, ObDatahubPieceMsg<T>,
                         op_id_, dfo_id_, thread_id_, piece_count_);


}
}
#endif /* _OB_SQL_ENGINE_PX_DATAHUB_DH_MSG_H__ */
//// end of header file

