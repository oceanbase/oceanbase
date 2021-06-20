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

#ifndef OB_DTL_LINKED_BUFFER_H
#define OB_DTL_LINKED_BUFFER_H

#include "lib/queue/ob_link.h"
#include "sql/dtl/ob_dtl_msg_type.h"

namespace oceanbase {
namespace sql {
namespace dtl {

#define DTL_BROADCAST (1ULL)

class ObDtlMsgHeader;
class ObDtlChannel;

class ObDtlDfoKey {
  OB_UNIS_VERSION(1);

public:
  ObDtlDfoKey() : server_id_(-1), px_sequence_id_(common::OB_INVALID_ID), qc_id_(-1), dfo_id_(common::OB_INVALID_ID)
  {}
  uint64_t hash() const
  {
    uint64_t val = common::murmurhash(&server_id_, sizeof(server_id_), 0);
    val = common::murmurhash(&px_sequence_id_, sizeof(px_sequence_id_), val);
    val = common::murmurhash(&qc_id_, sizeof(qc_id_), val);
    val = common::murmurhash(&dfo_id_, sizeof(dfo_id_), val);
    return val;
  }

  bool operator==(const ObDtlDfoKey other) const
  {
    return server_id_ == other.server_id_ && px_sequence_id_ == other.px_sequence_id_ && qc_id_ == other.qc_id_ &&
           dfo_id_ == other.dfo_id_;
  }

  void set(int32_t server_id, uint64_t px_sequence_id, int32_t qc_id, int64_t dfo_id)
  {
    server_id_ = server_id;
    px_sequence_id_ = px_sequence_id | PX_SEQ_MASK;
    qc_id_ = qc_id;
    dfo_id_ = dfo_id;
  }
  bool is_valid()
  {
    return -1 != server_id_ && common::OB_INVALID_ID != px_sequence_id_ && common::OB_INVALID_ID != dfo_id_;
  }
  int64_t get_dfo_id()
  {
    return dfo_id_;
  }
  uint64_t get_px_sequence_id()
  {
    return px_sequence_id_;
  }

  TO_STRING_KV(K_(server_id), K_(px_sequence_id), K_(qc_id), K_(dfo_id));

public:
  static const uint64_t PX_SEQ_MASK = 0x8000000000000000;
  int32_t server_id_;
  uint64_t px_sequence_id_;
  int32_t qc_id_;
  int64_t dfo_id_;
};

class ObDtlLinkedBuffer : public common::ObLink {
  OB_UNIS_VERSION(1);

public:
  ObDtlLinkedBuffer()
      : buf_(),
        size_(),
        pos_(),
        is_data_msg_(false),
        seq_no_(0),
        tenant_id_(0),
        allocated_chid_(0),
        is_eof_(false),
        timeout_ts_(0),
        msg_type_(ObDtlMsgType::MAX),
        flags_(0),
        dfo_key_(),
        use_interm_result_(false)
  {}
  ObDtlLinkedBuffer(char* buf, int64_t size)
      : buf_(buf),
        size_(size),
        pos_(),
        is_data_msg_(false),
        seq_no_(0),
        tenant_id_(0),
        allocated_chid_(0),
        is_eof_(false),
        timeout_ts_(0),
        msg_type_(ObDtlMsgType::MAX),
        flags_(0),
        dfo_key_(),
        use_interm_result_(false)
  {}

  TO_STRING_KV(K_(size), K_(pos), K_(is_data_msg), K_(seq_no), K_(tenant_id), K_(allocated_chid), K_(is_eof),
      K_(timeout_ts), K(msg_type_), K_(flags), K(is_bcast()));

  ObDtlLinkedBuffer* next() const
  {
    return reinterpret_cast<ObDtlLinkedBuffer*>(next_);
  }

  static int deserialize_msg_header(const ObDtlLinkedBuffer& buffer, ObDtlMsgHeader& header, bool keep_pos = false);

  void set_empty()
  {
    if (size_ > 0 && NULL != buf_) {
      buf_[0] = '\0';
    }
  }

  OB_INLINE char* buf()
  {
    return buf_;
  }

  OB_INLINE const char* buf() const
  {
    return static_cast<const char*>(buf_);
  }

  OB_INLINE int64_t size() const
  {
    return size_;
  }

  OB_INLINE int64_t& size()
  {
    return size_;
  }

  OB_INLINE int64_t& pos() const
  {
    return pos_;
  }

  OB_INLINE void set_pos(int64_t pos)
  {
    pos_ = pos;
  }

  OB_INLINE void set_data_msg(bool is_data_msg)
  {
    is_data_msg_ = is_data_msg;
  }

  OB_INLINE bool is_data_msg() const
  {
    return is_data_msg_;
  }

  OB_INLINE int64_t seq_no() const
  {
    return seq_no_;
  }

  OB_INLINE int64_t& seq_no()
  {
    return seq_no_;
  }

  OB_INLINE uint64_t tenant_id() const
  {
    return tenant_id_;
  }

  OB_INLINE uint64_t& tenant_id()
  {
    return tenant_id_;
  }

  OB_INLINE bool is_eof() const
  {
    return is_eof_;
  }

  OB_INLINE bool& is_eof()
  {
    return is_eof_;
  }

  void set_timeout_ts(int64_t timeout_ts)
  {
    timeout_ts_ = timeout_ts;
  }

  OB_INLINE int64_t timeout_ts() const
  {
    return timeout_ts_;
  }

  OB_INLINE int64_t& timeout_ts()
  {
    return timeout_ts_;
  }

  OB_INLINE ObDtlMsgType msg_type() const
  {
    return msg_type_;
  }

  OB_INLINE ObDtlMsgType& msg_type()
  {
    return msg_type_;
  }
  void set_msg_type(ObDtlMsgType type)
  {
    msg_type_ = type;
  }

  void set_size(int64_t size)
  {
    size_ = size;
  }

  bool is_bcast() const
  {
    return has_flag(DTL_BROADCAST);
  }

  void set_bcast()
  {
    add_flag(DTL_BROADCAST);
  }

  void remove_bcast()
  {
    remove_flag(DTL_BROADCAST);
  }

  static void assign(const ObDtlLinkedBuffer& src, ObDtlLinkedBuffer* dst)
  {
    MEMCPY(dst->buf_, src.buf_, src.size_);
    dst->size_ = src.size_;
    dst->is_data_msg_ = src.is_data_msg_;
    dst->seq_no_ = src.seq_no_;
    dst->tenant_id_ = src.tenant_id_;
    dst->is_eof_ = src.is_eof_;
    dst->timeout_ts_ = src.timeout_ts_;
    dst->pos_ = src.pos_;
    dst->msg_type_ = src.msg_type_;
    dst->flags_ = src.flags_;
    dst->dfo_key_ = src.dfo_key_;
    dst->use_interm_result_ = src.use_interm_result_;
  }

  void shallow_copy(const ObDtlLinkedBuffer& src)
  {
    buf_ = src.buf_;
    size_ = src.size_;
    is_data_msg_ = src.is_data_msg_;
    seq_no_ = src.seq_no_;
    tenant_id_ = src.tenant_id_;
    is_eof_ = src.is_eof_;
    timeout_ts_ = src.timeout_ts_;
    pos_ = src.pos_;
    msg_type_ = src.msg_type_;
    flags_ = src.flags_;
    dfo_key_ = src.dfo_key_;
  }

  OB_INLINE ObDtlDfoKey& get_dfo_key()
  {
    return dfo_key_;
  }

  OB_INLINE void set_dfo_key(ObDtlDfoKey& dfo_key)
  {
    dfo_key_ = dfo_key;
  }

  OB_INLINE bool has_dfo_key()
  {
    return dfo_key_.is_valid();
  }
  OB_INLINE int64_t allocated_chid() const
  {
    return allocated_chid_;
  }

  OB_INLINE int64_t& allocated_chid()
  {
    return allocated_chid_;
  }

  void add_flag(uint64_t attri)
  {
    flags_ |= attri;
  }

  bool has_flag(uint64_t attri) const
  {
    return !!(flags_ & attri);
  }

  void remove_flag(uint64_t attri)
  {
    flags_ &= ~attri;
  }
  void set_use_interm_result(bool flag)
  {
    use_interm_result_ = flag;
  }
  bool use_interm_result()
  {
    return use_interm_result_;
  }

private:
  char* buf_;
  int64_t size_;
  mutable int64_t pos_;
  bool is_data_msg_;
  int64_t seq_no_;
  uint64_t tenant_id_;
  int64_t allocated_chid_;
  bool is_eof_;
  int64_t timeout_ts_;
  ObDtlMsgType msg_type_;
  uint64_t flags_;
  ObDtlDfoKey dfo_key_;
  bool use_interm_result_;
};

OB_DEF_SERIALIZE(ObDtlLinkedBuffer, OB_INLINE)
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
          use_interm_result_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDtlLinkedBuffer, OB_INLINE)
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
        use_interm_result_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDtlLinkedBuffer, OB_INLINE)
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
      use_interm_result_);
  return len;
}

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase

#endif /* OB_DTL_LINKED_BUFFER_H */
