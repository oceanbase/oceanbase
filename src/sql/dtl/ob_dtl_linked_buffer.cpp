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

#include "ob_dtl_msg.h"
#include "ob_dtl_linked_buffer.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {
int64_t ObDtlDfoKey::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(server_id), K_(px_sequence_id), K_(qc_id), K_(dfo_id));
  J_OBJ_END();
  return pos;
}
int64_t ObDtlLinkedBuffer::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(size), K_(pos), K_(is_data_msg), K_(seq_no), K_(tenant_id), K_(allocated_chid), K_(is_eof),K_(timeout_ts), K(msg_type_), K_(flags), K(is_bcast()));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObDtlDfoKey, server_id_, px_sequence_id_, qc_id_, dfo_id_);

int ObDtlLinkedBuffer::deserialize_msg_header(
    const ObDtlLinkedBuffer& buffer, ObDtlMsgHeader& header, bool keep_pos /*= false*/)
{
  int ret = OB_SUCCESS;
  const char* buf = buffer.buf();
  int64_t size = buffer.size();
  int64_t& pos = buffer.pos();
  int64_t old_pos = buffer.pos();
  if (pos == size) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(serialization::decode(buf, size, pos, header))) {
    SQL_DTL_LOG(WARN, "decode DTL message header fail", K(size), K(pos), K(ret));
  } else if (header.type_ >= static_cast<int16_t>(ObDtlMsgType::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_DTL_LOG(WARN, "channel has received message with unknown type", K(header), K(size), K(pos));
  }
  if (keep_pos) {
    buffer.pos() = old_pos;
  }
  return ret;
}

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase
