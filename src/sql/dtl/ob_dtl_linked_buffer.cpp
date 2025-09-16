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
#include "sql/ob_sql_utils.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

OB_SERIALIZE_MEMBER(ObDtlDfoKey, server_id_, px_sequence_id_, qc_id_, dfo_id_);

OB_SERIALIZE_MEMBER(ObDtlBatchInfo, batch_id_, start_, end_, rows_);

OB_DEF_SERIALIZE(ObDtlOpInfo)
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, dop_, plan_id_, exec_id_, session_id_, database_id_);
  if (OB_SUCC(ret)) {
    MEMCPY(buf + pos, sql_id_, common::OB_MAX_SQL_ID_LENGTH + 1);
    pos += common::OB_MAX_SQL_ID_LENGTH + 1;
  }
  LST_DO_CODE(OB_UNIS_ENCODE, op_id_, input_rows_, input_width_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDtlOpInfo)
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, dop_, plan_id_, exec_id_, session_id_, database_id_);
  if (OB_SUCC(ret)) {
    MEMCPY(sql_id_, (char*)buf + pos, common::OB_MAX_SQL_ID_LENGTH + 1);
    pos += common::OB_MAX_SQL_ID_LENGTH + 1;
  }
  LST_DO_CODE(OB_UNIS_DECODE, op_id_, input_rows_, input_width_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDtlOpInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, dop_, plan_id_, exec_id_, session_id_, database_id_);
  len += common::OB_MAX_SQL_ID_LENGTH + 1;
  LST_DO_CODE(OB_UNIS_ADD_LEN, op_id_, input_rows_, input_width_);
  return len;
}

int ObDtlLinkedBuffer::deserialize_msg_header(const ObDtlLinkedBuffer &buffer,
                                              ObDtlMsgHeader &header,
                                              bool keep_pos /*= false*/)
{
  int ret = OB_SUCCESS;
  const char *buf = buffer.buf();
  int64_t size = buffer.size();
  int64_t &pos = buffer.pos();
  int64_t old_pos = buffer.pos();
  if (pos == size) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(serialization::decode(buf, size, pos, header))) {
    SQL_DTL_LOG(WARN, "decode DTL message header fail", K(size), K(pos), K(ret));
  } else if (header.type_ >= static_cast<int16_t>(ObDtlMsgType::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_DTL_LOG(WARN, "channel has received message with unknown type",
                K(header), K(size), K(pos));
  }
  if (keep_pos) {
    buffer.pos() = old_pos;
  }
  return ret;
}

int ObDtlLinkedBuffer::add_batch_info(int64_t batch_id, int64_t rows)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_data_msg() || (PX_DATUM_ROW != msg_type_ && PX_CHUNK_ROW != msg_type_))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_DTL_LOG(WARN, "unexpected msg type", K(ret), K(is_data_msg()), K(msg_type_));
  } else {
    const int64_t count = batch_info_.count();
    const int64_t header_size = PX_DATUM_ROW == msg_type_ ? sizeof(ObChunkDatumStore::Block)
                                : sizeof(ObChunkRowStore::Block);
    const int64_t start = 0 == count ? header_size : batch_info_.at(count - 1).end_;
    if (OB_UNLIKELY(pos_ < start || rows < rows_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DTL_LOG(WARN, "unexpected start and pos", K(ret), K(pos_), K(start), K(rows_cnt_),
                  K(rows), K(batch_info_));
    } else {
      ObDtlBatchInfo info(batch_id, start, pos_, rows - rows_cnt_);
      if (OB_FAIL(batch_info_.push_back(info))) {
        SQL_DTL_LOG(WARN, "push back failed", K(ret));
      } else {
        batch_info_valid_ = true;
        rows_cnt_ = rows;
      }
    }
  }
  return ret;
}

int ObDtlLinkedBuffer::push_batch_id(int64_t batch_id, int64_t rows)
{
  int ret = common::OB_SUCCESS;
  if (batch_info_valid_) {
    ret = add_batch_info(batch_id, rows);
  } else {
    batch_id_ = batch_id;
  }
  return ret;
}

int ObDtlLinkedBuffer::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (min_cluster_version < CLUSTER_VERSION_4_2_5_7) {
    int64_t compat_version = 1;
    OB_UNIS_ENCODE(compat_version);
  } else {
    OB_UNIS_ENCODE(UNIS_VERSION);
  }
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_FAIL(serialize_(buf, buf_len, pos))) {
      SQL_DTL_LOG(WARN, "serialize fail", K(ret));
    } else {
      int64_t serial_size = pos - pos_bak;
      int64_t tmp_pos = 0;
      CHECK_SERIALIZE_SIZE(ObDtlLinkedBuffer, serial_size);
      ret = NS_::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

int ObDtlLinkedBuffer::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(size_);
  if (OB_SUCC(ret)) {
    if (buf_len - pos < size_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(buf + pos, buf_, size_);
      pos += size_;
      LST_DO_CODE(OB_UNIS_ENCODE,
        is_data_msg_,
        seq_no_,
        tenant_id_,
        is_eof_,
        timeout_ts_,
        msg_type_,
        flags_,
        dfo_key_,
        use_interm_result_,
        batch_id_,
        batch_info_valid_);
      if (OB_SUCC(ret) && batch_info_valid_) {
        LST_DO_CODE(OB_UNIS_ENCODE, batch_info_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_ENCODE, dfo_id_, sqc_id_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_ENCODE, enable_channel_sync_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_ENCODE, register_dm_info_);
      }
      if (OB_SUCC(ret) && seq_no_ == 1) {
        LST_DO_CODE(OB_UNIS_ENCODE, op_info_);
      }
    }
  }
  return ret;
}

int ObDtlLinkedBuffer::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;

  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  if (OB_SUCC(ret)) {
    if (len < 0) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DTL_LOG(WARN, "can't decode object with negative length", K(len));
    } else if (data_len < len + pos) {
      ret = OB_DESERIALIZE_ERROR;
      SQL_DTL_LOG(WARN, "buf length not enough", K(len), K(pos), K(data_len));
    }
  }
  if (OB_SUCC(ret)) {
    const_cast<int64_t&>(data_len) = len;
    int64_t pos_orig = pos;
    buf = buf + pos_orig;
    pos = 0;

    using namespace oceanbase::common;
    OB_UNIS_DECODE(size_);
    if (OB_SUCC(ret)) {
      buf_ = (char*)buf + pos;
      pos += size_;
      LST_DO_CODE(OB_UNIS_DECODE,
        is_data_msg_,
        seq_no_,
        tenant_id_,
        is_eof_,
        timeout_ts_,
        msg_type_,
        flags_,
        dfo_key_,
        use_interm_result_,
        batch_id_,
        batch_info_valid_);
      if (OB_SUCC(ret) && batch_info_valid_) {
        LST_DO_CODE(OB_UNIS_DECODE, batch_info_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_DECODE, dfo_id_, sqc_id_);
      }
      if (OB_SUCC(ret)) {
        enable_channel_sync_ = false;
        LST_DO_CODE(OB_UNIS_DECODE, enable_channel_sync_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_DECODE, register_dm_info_);
      }
      if (OB_SUCC(ret) && version == 3) {
        // mock deserialize row meta
        int64_t row_meta_version = 0;
        int64_t row_meta_len = 0;
        OB_UNIS_DECODE(row_meta_version);
        OB_UNIS_DECODE(row_meta_len);
        if (OB_SUCC(ret)) {
          pos += row_meta_len;
        }
      }
      if (OB_SUCC(ret) && seq_no_ == 1) {
        LST_DO_CODE(OB_UNIS_DECODE, op_info_);
      }
    }
    if (OB_SUCC(ret)) {
      (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
    }

    pos = pos_orig + len;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDtlLinkedBuffer)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(size_);
  len += size_;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    is_data_msg_,
    seq_no_,
    tenant_id_,
    is_eof_,
    timeout_ts_,
    msg_type_,
    flags_,
    dfo_key_,
    use_interm_result_,
    batch_id_,
    batch_info_valid_);
  if (batch_info_valid_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, batch_info_);
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, dfo_id_, sqc_id_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, enable_channel_sync_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, register_dm_info_);
  if (seq_no_ == 1) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, op_info_);
  }
  return len;
}

}
}
}
