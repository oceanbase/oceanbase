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
#include "sql/engine/px/ob_dfo.h"

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
        source_dfo_id_(common::OB_INVALID_ID),
        target_dfo_id_(common::OB_INVALID_ID),
        thread_id_(0),
        piece_count_(0),
        child_dfo_(nullptr)
    {}
  virtual ~ObDatahubPieceMsg() = default;
  VIRTUAL_TO_STRING_KV(K_(op_id), K_(source_dfo_id), K_(thread_id), K_(target_dfo_id));
  uint64_t op_id_;   // 在 piece 消息处理中，用于寻址 QC 端 ctx
  /*
              piece     whole
      SRC_DFO  --->  QC  ---> TGT_DFO
      we need use source dfo id to add up piece count
      use target dfo id to find SQC gateway
      for most DH msg type, src is same with tgt
      but for init_channel msg, src is different from tgt
  */
  uint64_t source_dfo_id_;
  uint64_t target_dfo_id_;
  uint64_t thread_id_; // debug
  uint64_t piece_count_; //record piece count
  ObDfo *child_dfo_;
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

OB_DEF_SERIALIZE(ObDatahubPieceMsg<T>, template<dtl::ObDtlMsgType T>)
{
  int ret = common::OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              op_id_,
              source_dfo_id_,
              thread_id_,
              piece_count_,
              target_dfo_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDatahubPieceMsg<T>, template<dtl::ObDtlMsgType T>)
{
  int ret = common::OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              op_id_,
              source_dfo_id_,
              thread_id_,
              piece_count_);
  // for compat
  target_dfo_id_ = source_dfo_id_;
  LST_DO_CODE(OB_UNIS_DECODE,
              target_dfo_id_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatahubPieceMsg<T>, template<dtl::ObDtlMsgType T>)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              op_id_,
              source_dfo_id_,
              thread_id_,
              piece_count_,
              target_dfo_id_);
  return len;
}


}
}
#endif /* _OB_SQL_ENGINE_PX_DATAHUB_DH_MSG_H__ */
//// end of header file

