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

#ifndef OB_DTL_MSG_ID_H
#define OB_DTL_MSG_ID_H

namespace oceanbase {
namespace sql {
namespace dtl {

enum ObDtlMsgType {
  // assume ID below 10 is for testing.
  TESTING = 10,
  INIT_SQC_RESULT,
  FINISH_SQC_RESULT,
  FINISH_TASK_RESULT,
  PX_RECEIVE_DATA_CHANNEL,
  PX_TRANSMIT_DATA_CHANNEL,
  PX_CANCEL_DFO,
  PX_NEW_ROW,
  UNBLOCKING_DATA_FLOW,
  PX_CHUNK_ROW,
  DRAIN_DATA_FLOW,
  PX_DATUM_ROW,
  DH_BARRIER_PIECE_MSG,
  DH_BARRIER_WHOLE_MSG,
  DH_WINBUF_PIECE_MSG,
  DH_WINBUF_WHOLE_MSG,
  MAX
};

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase

#endif /* OB_DTL_MSG_ID_H */
