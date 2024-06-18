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
#include "sql/dtl/ob_dtl_vectors_buffer.h"

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
  LST_DO_CODE(OB_UNIS_ENCODE, op_id_, input_rows_, input_width_,
              disable_auto_mem_mgr_);
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
  LST_DO_CODE(OB_UNIS_DECODE, op_id_, input_rows_, input_width_,
              disable_auto_mem_mgr_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDtlOpInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, dop_, plan_id_, exec_id_, session_id_, database_id_);
  len += common::OB_MAX_SQL_ID_LENGTH + 1;
  LST_DO_CODE(OB_UNIS_ADD_LEN, op_id_, input_rows_, input_width_,
              disable_auto_mem_mgr_);
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

OB_DEF_SERIALIZE(ObDtlLinkedBuffer)
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(size_);
  if (OB_SUCC(ret)) {
    if (buf_len - pos < size_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      if (PX_VECTOR == msg_type_) {
        if (OB_FAIL(serialize_vector(buf, pos, size_))) {
          SQL_DTL_LOG(WARN, "serialize vector failed", K(ret));
        }
      } else if (PX_VECTOR_FIXED == msg_type_) {
        if (OB_FAIL(serialize_fixed_vector(buf, pos, size_))) {
          SQL_DTL_LOG(WARN, "serialize vector fixed failed", K(ret));
        }
      } else {
        MEMCPY(buf + pos, buf_, size_);
      }
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
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_ENCODE, row_meta_);
      }
      if (OB_SUCC(ret) && seq_no_ == 1) {
        LST_DO_CODE(OB_UNIS_ENCODE, op_info_);
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDtlLinkedBuffer)
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
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
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_DECODE, row_meta_);
    }
    if (OB_SUCC(ret) && seq_no_ == 1) {
      LST_DO_CODE(OB_UNIS_DECODE, op_info_);
    }
  }
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDtlLinkedBuffer)
{
  int64_t len = 0;
  if (PX_VECTOR == msg_type_) {
    int64_t new_size  = get_serialize_vector_size();
    if (OB_UNLIKELY(size_ < new_size)) {
      SQL_DTL_LOG(TRACE, "unexpected encode leads size overflow", K(size_), K(new_size));
    }
    const_cast<int64_t &> (size_) = new_size;
  } else if (PX_VECTOR_FIXED == msg_type_) {
    int64_t new_size = get_serialize_fixed_vector_size();
    if (OB_UNLIKELY(size_ < new_size)) {
      SQL_DTL_LOG(TRACE, "unexpected encode leads size overflow", K(size_), K(new_size));
    }
    const_cast<int64_t &> (size_) = new_size;
  }
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
  LST_DO_CODE(OB_UNIS_ADD_LEN, row_meta_);
  if (seq_no_ == 1) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, op_info_);
  }
  return len;
}


/*
vector buf serialize
magic_num : 4
col_cnt : 4
row_cnt : 4
(format : 4 + nulls offset : 4 + fixed len : 4 + offsets offset : 4 + data offset : 4) * col_cnt
(nulls : to_bit_vector(row_cnt) + offsets : 4 * (row_cnt + 1) + data)  * col_cnt
*/
/*struct VectorInfo {
  VectorFormat format_;
  int32_t nulls_offset_;
  int32_t fixed_len_;
  int32_t offsets_offset_;
  int32_t data_offset_;
};*/

int64_t ObDtlLinkedBuffer::get_serialize_vector_size() const
{
  int64_t serialize_size = 0;
  ObDtlVectorsBlock *vector_blk = reinterpret_cast<ObDtlVectorsBlock *> (buf_);
  ObDtlVectorsBuffer *vector_buf = vector_blk->get_buffer();
  int64_t col_cnt = vector_buf->get_col_cnt();
  int64_t row_cnt = vector_blk->rows();
  //magic, col cnt , row cnt
  serialize_size += 3 * sizeof(int32_t);
  if (row_cnt > 0 && col_cnt > 0) {
    serialize_size += (col_cnt * sizeof(VectorInfo));
    for (int64_t i = 0; i < col_cnt; ++i) {
      ObVectorSegment *beg_seg = vector_buf->get_start_seg(i);
      VectorFormat format = beg_seg->format_;
      serialize_size += ObBitVector::memory_size(row_cnt);
      ObVectorSegment *curr_seg = beg_seg;
      if (VectorFormat::VEC_CONTINUOUS == format) {
        serialize_size += (row_cnt + 1) * sizeof(uint32_t);
      }
      while (curr_seg) {
        int64_t copy_size = curr_seg->data_pos_ - curr_seg->data_start_pos_;
        serialize_size += copy_size;
        curr_seg = curr_seg->next_;
      }
    }
  }
  return serialize_size;
}

int ObDtlLinkedBuffer::serialize_vector(char *buf, int64_t pos, int64_t size) const
{
  int ret = OB_SUCCESS;
  ObDtlVectorsBlock *vector_blk = reinterpret_cast<ObDtlVectorsBlock *> (const_cast<char *> (buf_));
  ObDtlVectorsBuffer *vector_buf = vector_blk->get_buffer();
  int64_t col_cnt = vector_buf->get_col_cnt();
  int64_t row_cnt = vector_blk->rows();
  char *vector_head = buf + pos;
  int64_t header_offset = 0;
  //set magic
  *reinterpret_cast<int32_t *> (vector_head + header_offset) = ObDtlVectorsBuffer::MAGIC;
  header_offset += sizeof(int32_t);
  //set col cnt
  *reinterpret_cast<int32_t *> (vector_head + header_offset) = col_cnt;
  header_offset += sizeof(int32_t);
  //set row cnt
  *reinterpret_cast<int32_t *> (vector_head + header_offset) = row_cnt;
  header_offset += sizeof(int32_t);
  if (row_cnt > 0 && col_cnt > 0) {
    //set info
    VectorInfo *infos = reinterpret_cast<VectorInfo *> (vector_head + header_offset);
    header_offset += (col_cnt * sizeof(VectorInfo));
    //set data
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      ObVectorSegment *beg_seg = vector_buf->get_start_seg(i);
      infos[i].format_ = beg_seg->format_;
      infos[i].fixed_len_ = beg_seg->fixed_len_;
      //alloc nulls
      infos[i].nulls_offset_ = header_offset;
      ObBitVector *nulls = to_bit_vector(vector_head + header_offset);
      nulls->reset(row_cnt);
      header_offset += ObBitVector::memory_size(row_cnt);
      //alloc offsets
      infos[i].offsets_offset_ = header_offset;
      if (VectorFormat::VEC_CONTINUOUS == infos[i].format_) {
        header_offset += (row_cnt + 1) * sizeof(uint32_t);
      }
      //fill data
      infos[i].data_offset_ = header_offset;
      ObVectorSegment *curr_seg = beg_seg;
      int32_t sum_rows = 0;
      if (VectorFormat::VEC_CONTINUOUS == infos[i].format_) {
        uint32_t *offsets_array = reinterpret_cast<uint32_t *> (vector_head + infos[i].offsets_offset_);
        offsets_array[0] = header_offset;
        while (nullptr != curr_seg) {
          //fill nulls && fill offsets
          for (int32_t j = 0; j < curr_seg->seg_rows_; ++j) {
            if (curr_seg->nulls_->at(j)) {
              nulls->set(sum_rows);
            }
            offsets_array[sum_rows + 1] = offsets_array[sum_rows] + curr_seg->get_length(j);
            ++sum_rows;
          }
          //memcpy data
          int64_t copy_size = curr_seg->data_pos_ - curr_seg->data_start_pos_;
          memcpy(vector_head + header_offset, curr_seg->head(), copy_size);
          header_offset += copy_size;
          curr_seg = curr_seg->next_;
        }
      } else {
        while (nullptr != curr_seg) {
          //fill nulls && fill offsets
          for (int32_t j = 0; j < curr_seg->seg_rows_; ++j) {
            if (curr_seg->nulls_->at(j)) {
              nulls->set(sum_rows);
            }
            ++sum_rows;
          }
          //memcpy data
          int64_t copy_size = curr_seg->fixed_len_ * curr_seg->seg_rows_;
          memcpy(vector_head + header_offset, curr_seg->head(), copy_size);
          header_offset += copy_size;
          curr_seg = curr_seg->next_;
        }
      }
      if (sum_rows != row_cnt) {
        ret = OB_ERR_UNEXPECTED;
        SQL_DTL_LOG(WARN, "check row cnt failed", K(ret), K(sum_rows), K(row_cnt), K(sum_rows));
      }
    }
  }
  if (OB_SUCC(ret) && header_offset > size) {
    ret = OB_ERR_UNEXPECTED;
    SQL_DTL_LOG(WARN, "buffer is not enough", K(ret), K(pos), K(size), K(header_offset));
  }
  return ret;
}

int ObDtlLinkedBuffer::serialize_fixed_vector(char *buf, int64_t pos, int64_t size) const
{
  const int64_t MAGIC_OFFSET = 0;
  const int64_t COL_CNT_OFFSET = sizeof(int32_t) /*skip MAGIC*/;
  const int64_t ROW_CNT_OFFSET = COL_CNT_OFFSET + sizeof(int32_t);
  const int64_t INFOS_OFFSET = ROW_CNT_OFFSET + sizeof(int32_t);

  int64_t magic = *reinterpret_cast<int32_t *>(buf_ + MAGIC_OFFSET);
  int64_t col_cnt = *reinterpret_cast<int32_t *> (buf_ + COL_CNT_OFFSET);
  int64_t row_cnt = *reinterpret_cast<int32_t *> (buf_ + ROW_CNT_OFFSET);
  int ret = OB_SUCCESS;
  char *vector_head = buf + pos;
  int64_t header_offset = 0;
  //set magic
  *reinterpret_cast<int32_t *> (vector_head + header_offset) = ObDtlVectorsBuffer::MAGIC;
  header_offset += sizeof(int32_t);
  //set col cnt
  *reinterpret_cast<int32_t *> (vector_head + header_offset) = col_cnt;
  header_offset += sizeof(int32_t);
  //set row cnt
  *reinterpret_cast<int32_t *> (vector_head + header_offset) = row_cnt;
  header_offset += sizeof(int32_t);
  if (col_cnt > 0 && row_cnt > 0) {
    VectorInfo *orign_infos = reinterpret_cast<VectorInfo *> (buf_ + INFOS_OFFSET);
    //set info
    VectorInfo *infos = reinterpret_cast<VectorInfo *> (vector_head + header_offset);
    for (int64_t i = 0; i < col_cnt; ++i) {
      infos[i].format_ = orign_infos[i].format_;
      infos[i].fixed_len_ = orign_infos[i].fixed_len_;
    }
    header_offset += (col_cnt * sizeof(VectorInfo));
    //set data
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      //alloc nulls
      infos[i].nulls_offset_ = header_offset;
      ObBitVector *nulls = to_bit_vector(vector_head + header_offset);
      nulls->reset(row_cnt);
      header_offset += ObBitVector::memory_size(row_cnt);
      //fill data
      infos[i].data_offset_ = header_offset;
      ObBitVector *orign_nulls = to_bit_vector(buf_ + orign_infos[i].nulls_offset_);
      char *orign_data = buf_ + orign_infos[i].data_offset_;
      nulls->deep_copy(*orign_nulls, row_cnt);
      int64_t copy_size = infos[i].fixed_len_ * row_cnt;
      memcpy(vector_head + header_offset, orign_data, copy_size);
      header_offset += copy_size;
    }
  }
  if (OB_SUCC(ret) && header_offset > size) {
    ret = OB_ERR_UNEXPECTED;
    SQL_DTL_LOG(WARN, "buffer is not enough", K(ret), K(pos), K(size), K(header_offset));
  }
  return ret;
}

int64_t ObDtlLinkedBuffer::get_serialize_fixed_vector_size() const
{
  const int64_t COL_CNT_OFFSET = sizeof(int32_t) /*skip MAGIC*/;
  const int64_t ROW_CNT_OFFSET = COL_CNT_OFFSET + sizeof(int32_t);
  const int64_t INFOS_OFFSET = ROW_CNT_OFFSET + sizeof(int32_t);
  int64_t serialize_size = 0;
  int64_t col_cnt = *reinterpret_cast<int32_t *> (buf_ + COL_CNT_OFFSET);
  int64_t row_cnt = *reinterpret_cast<int32_t *> (buf_ + ROW_CNT_OFFSET);
  //magic, col cnt , row cnt
  serialize_size += INFOS_OFFSET;
  if (col_cnt > 0 && row_cnt > 0) {
    VectorInfo *infos = reinterpret_cast<VectorInfo *> (buf_ + INFOS_OFFSET);
    serialize_size += (col_cnt * sizeof(VectorInfo));
    for (int64_t i = 0; i < col_cnt; ++i) {
      serialize_size += ObBitVector::memory_size(row_cnt);
      serialize_size += infos[i].fixed_len_ * row_cnt;
    }
  }
  return serialize_size;
}

}
}
}
