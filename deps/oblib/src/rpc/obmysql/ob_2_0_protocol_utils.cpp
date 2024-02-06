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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "ob_2_0_protocol_utils.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/checksum/ob_crc16.h"
#include "lib/checksum/ob_crc64.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/obmysql/ob_2_0_protocol_struct.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/obsm_struct.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obmysql
{
const int64_t ObProtoEncodeParam::MAX_PROTO20_PAYLOAD_LEN = OB_MYSQL_MAX_PAYLOAD_LENGTH - OB20_PROTOCOL_HEADER_TAILER_LENGTH;
const int64_t ObProtoEncodeParam::PROTO20_SPLIT_LEN = OB_MYSQL_MAX_PAYLOAD_LENGTH / 2 ; // 8MB

inline int ObProtoEncodeParam::save_large_packet(const char *start, const int64_t len) {
  INIT_SUCC(ret);
  if (OB_ISNULL(start) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input value", KP(start), K(len), K(ret));
  } else if (OB_UNLIKELY(NULL != large_pkt_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("large pkt buf has already exist", KP(start), K(len), K(ret));
  } else if (OB_ISNULL(large_pkt_buf_ = (char *)ob_malloc(len, "ProtoEncodePara"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem", K(len), K(ret));
  } else {
    MEMCPY(large_pkt_buf_, start, len);
    large_pkt_buf_len_ = len;
    large_pkt_buf_pos_ = 0;
  }

  return ret;
}

inline int ObProtoEncodeParam::add_pos(const int64_t delta) {
  INIT_SUCC(ret);
  large_pkt_buf_pos_ += delta;
  if ((large_pkt_buf_pos_ < 0) || (large_pkt_buf_pos_ > large_pkt_buf_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid pos", K(delta), K_(large_pkt_buf_pos), K_(large_pkt_buf_len),
              KPC(this), K(ret));
  }
  return ret;
}

int ObProto20Utils::fill_proto20_header_and_tailer(ObProtoEncodeParam &param)
{
  INIT_SUCC(ret);
  ObProto20Context &proto20_context = *param.proto20_context_;
  if (FILL_PAYLOAD_STEP == proto20_context.next_step_) {
    proto20_context.next_step_ = FILL_TAILER_STEP; // fill ob20 protocol header and tailer
    if (OB_FAIL(do_packet_encode(param))) {
      LOG_ERROR("fail to do packet encode", K(param), K(ret));
    } else {
      int64_t seri_size = param.seri_size_;
      EVENT_ADD(MYSQL_PACKET_OUT_BYTES, seri_size);
    }
  } else if (FILL_DONE_STEP == proto20_context.next_step_) {
    // has been filled, no need fill again
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid fill step", "next_step", proto20_context.next_step_, K(ret));
  }
  return ret;
}

int ObProto20Utils::do_packet_encode(ObProtoEncodeParam &param) {
  INIT_SUCC(ret);
  if (OB_UNLIKELY(!param.is_valid())) { // will no check again later
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(param), K(ret));
  } else {
    if (param.proto20_context_->is_proto20_used()) {
      if (OB_FAIL(do_proto20_packet_encode(param))) {
        LOG_ERROR("fail to do proto20 packet encode", K(param), K(ret));
      }
    } else {
      ObEasyBuffer easy_buffer(*param.ez_buf_);

      //SET_OB_LOG_TRACE_MODE();    // prevent printing log
      int32_t old_len = param.seri_size_;
      ret = param.pkt_->encode(easy_buffer.last(), easy_buffer.write_avail_size(), param.seri_size_);
      if (param.conn_->pkt_rec_wrapper_.enable_proto_dia()) {
        param.conn_->pkt_rec_wrapper_.record_send_mysql_pkt(*param.pkt_, param.seri_size_ - old_len);
      }
      //if (((OB_SIZE_OVERFLOW != ret) && (OB_BUF_NOT_ENOUGH != ret) && (common::OB_SUCCESS != ret))
      //     || ((IS_LOG_ENABLED(INFO) && (OB_LOG_NEED_TO_PRINT(DEBUG))))) {
      //  PRINT_OB_LOG_TRACE_BUF(INFO);
      //}
      //CANCLE_OB_LOG_TRACE_MODE();

      if (OB_SUCC(ret)) {
        easy_buffer.write(param.seri_size_);
      } else {
        param.encode_ret_ = ret;
        param.need_flush_ = true;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

inline int ObProto20Utils::do_proto20_packet_encode(ObProtoEncodeParam &param)
{
  INIT_SUCC(ret);
  ObProto20Context &proto20_context = *param.proto20_context_;
  char *origin_pos = param.ez_buf_->pos;
  char *origin_last = param.ez_buf_->last;
  bool need_break = false;

  // just for defense
  int64_t curr_len = param.ez_buf_->last - origin_pos;
  if (OB_UNLIKELY(curr_len < proto20_context.curr_proto20_packet_start_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid pos", K(curr_len), K(proto20_context), K(ret));
  } else {
    param.ez_buf_->pos += proto20_context.curr_proto20_packet_start_pos_;
  }

  while (OB_SUCC(ret) && !need_break) {
    switch (proto20_context.next_step_) {
      case START_TO_FILL_STEP: {
        proto20_context.next_step_ = RESERVE_HEADER_STEP;
        if (param.conn_->pkt_rec_wrapper_.enable_proto_dia()) {
          param.conn_->pkt_rec_wrapper_.begin_seal_obp20_pkt();
        }
        break;
      }
      case RESERVE_HEADER_STEP: {
        ObEasyBuffer easy_buffer(*param.ez_buf_);
        if (OB_UNLIKELY(easy_buffer.read_avail_size() != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid easy buffer", K(easy_buffer), K(ret));
        }
        // buffer must enough to fill header and tailer
        else if (easy_buffer.write_avail_size() <= (proto20_context.header_len_ + proto20_context.tailer_len_)) {
          proto20_context.next_step_ = START_TO_FILL_STEP;
          param.need_flush_ = true; // not enough, flush or alloc more buffer
          need_break = true;
        } else {
          // reserve header
          easy_buffer.write(proto20_context.header_len_);
          proto20_context.next_step_ = FILL_PAYLOAD_STEP;
        }
        break;
      }
      case FILL_PAYLOAD_STEP: {
        if (OB_FAIL(fill_proto20_payload(param, need_break))) {
          LOG_ERROR("fail to fill payload", K(param), K(need_break), K(ret));
        }
        break;
      }
      case FILL_TAILER_STEP: {
        if (OB_FAIL(fill_proto20_tailer(param))) {
          LOG_ERROR("fail to fill tailer", K(param), K(ret));
        }
        break;
      }
      case FILL_HEADER_STEP: {
        if (OB_FAIL(fill_proto20_header(param))) {
          LOG_ERROR("fail to fill header", K(param), K(ret));
        }
        break;
      }
      case FILL_DONE_STEP: {
        LOG_DEBUG("fill ob 20 packet succ");
        proto20_context.curr_proto20_packet_start_pos_ += (param.ez_buf_->last - param.ez_buf_->pos);

        // reset extra info
        if (param.proto20_context_->has_extra_info_ && !is_the_last_packet(param) && OB_FAIL(reset_extra_info(param))) {
          LOG_WARN("failed to reset extra info encoders", K(ret));
        } else {
          if (param.is_large_packet_cached_avail()) {
            // next packet will encode from beginning
            param.ez_buf_->pos = param.ez_buf_->last;
            // continue to encode next split packet
            proto20_context.next_step_ = START_TO_FILL_STEP;
          } else {
            param.need_flush_ = true; // fill succ, flush
            need_break = true;
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid fill step", K(proto20_context.next_step_), K(ret));
        break;
      }
    }
    LOG_DEBUG("proto20 encode", "current next step",
              get_proto20_encode_step_name(proto20_context.next_step_));
  }

  if (OB_SUCC(ret)) {
    // one ob20 protocol packet fill complete, ready to fill next
    if (proto20_context.next_step_ == FILL_DONE_STEP) {
      proto20_context.next_step_ = START_TO_FILL_STEP;
    }
  }

  param.ez_buf_->pos = origin_pos; // recover the orgin pos
  int64_t seri_size = param.ez_buf_->last - origin_last;
  param.seri_size_ = seri_size;

  if (seri_size < 0) { // just for defnese
    LOG_ERROR("seri_size should be >= 0", K(seri_size));
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_FAIL(ret)) {
    if (seri_size > 0) { // just for defnese
      LOG_ERROR("seri_size should be = 0 here", K(seri_size));
    }
  }

  return ret;
}

int ObProto20Utils::encode_extra_info(char *buffer, int64_t length, int64_t &pos,
                                      ObIArray<ObObjKV> *extra_info)
{
  int ret = OB_SUCCESS;

  if (NULL == extra_info) {
    // do nothing
  } else {
    int64_t extro_info_len = 0;
    int64_t orig_pos = pos;
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_info->count(); i++) {
      const ObObjKV &ob_obj_kv = extra_info->at(i);
      const int64_t key_size = ob_obj_kv.key_.get_serialize_size();
      const int64_t value_size = ob_obj_kv.value_.get_serialize_size();
      extro_info_len += (key_size + value_size);
    }

    if (OB_LIKELY(extro_info_len > 0)) {
      if (OB_FAIL(ObMySQLUtil::store_int4(buffer,
                                          length,
                                          static_cast<int32_t>(extro_info_len),
                                          pos))) {
        LOG_WARN("fail to store extro_info_len", K(extro_info_len), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < extra_info->count(); i++) {
          const ObObjKV &ob_obj_kv = extra_info->at(i);
          if (OB_FAIL(ob_obj_kv.key_.serialize(buffer, length, pos))) {
            LOG_WARN("fail to serialize key", K(i), "key", ob_obj_kv.key_, K(ret));
          } else if (OB_FAIL(ob_obj_kv.value_.serialize(buffer, length,pos))) {
            LOG_WARN("fail to serialize value", K(i), "value", ob_obj_kv.value_, K(ret));
          } else {
            LOG_TRACE("encode extra_info val", KPHEX(buffer+orig_pos, pos-orig_pos));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      pos = orig_pos;
    } else {
      // encoded extra info to the 20 packet, and reset extra_info array
      //extra_info->reset();
      // extra_info not need to be reset in here, it will be reset after sealing packet
    }
  }

  LOG_DEBUG("fill proto20 extra info succ", KPC(extra_info));

  return ret;
}

int ObProto20Utils::encode_new_extra_info(char *buffer, int64_t length, int64_t &pos,
                                          ObIArray<Obp20Encoder*> *extra_info)
{
  int ret = OB_SUCCESS;

  if (NULL == extra_info) {
    // do nothing
  } else {
    int64_t extro_info_len = 0;
    int64_t org_pos = pos;

    // reserver extra info len
    if (pos + 4 > length) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN,"buffer size overflow", K(ret), K(pos), K(length));
    } else {
      //MEMSET(buffer+pos, 0x00, length-pos);
      pos += 4;

      for (int64_t i = 0; OB_SUCC(ret) && i < extra_info->count(); i++) {
        Obp20Encoder* ecd = extra_info->at(i);
        if (OB_ISNULL(ecd)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid encoder", K(ecd), K(i), K(ret));
        } else if (OB_FAIL(ecd->serialize(buffer, length, pos))) {
          LOG_WARN("fail to serialize key", K(ret));
        } else if (FALSE_IT(ecd->is_serial_ = true)) {
        } else {
          LOG_TRACE("encode extra_info val", KPHEX(buffer+org_pos, pos-org_pos),
                                                              K(pos), K(org_pos));
        }
      }

      if(OB_FAIL(ret)) {
        // do nothing 
      } else {
        int32_t extro_info_len = pos - org_pos - 4;
        int64_t o_pos = org_pos;
        if (extro_info_len == 0) {
          // do nothing
          pos = o_pos;
        } else if (OB_FAIL(ObMySQLUtil::store_int4(buffer,
                                            length,
                                            static_cast<int32_t>(extro_info_len),
                                            o_pos))) {
          LOG_WARN("fail to store extro_info_len", K(extro_info_len), K(ret));
        } else {
          // do nothing
        }
      }
    }

    if (OB_FAIL(ret)) {
      pos = org_pos;
    } else if (OB_ISNULL(buffer)) {
      // do nothing
    } else {
      //extra_info->reset();
      //LOG_TRACE("encode extra_info val result", KPHEX(buffer, pos), K(pos));
      // extra_info not need to be reset in here, it will be reset after sealing packet
    }
  }

  LOG_DEBUG("fill proto20 extra info succ", KP(extra_info));

  return ret;
}

inline int ObProto20Utils::fill_proto20_payload(ObProtoEncodeParam &param, bool &need_break) {
  INIT_SUCC(ret);
  ObEasyBuffer easy_buffer(*param.ez_buf_);
  ObProto20Context &proto20_context = *param.proto20_context_;
  int64_t seri_size = 0;
  need_break = false;
  if (OB_ISNULL(param.pkt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input value", K(param), K(ret));
  } else if (param.is_large_packet_cached_avail()) {
    int64_t handle_len = std::min(ObProtoEncodeParam::PROTO20_SPLIT_LEN, param.get_remain_len());
    if (OB_UNLIKELY(handle_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid handle_len", K(param), K(handle_len), K(ret));
    } else if (OB_UNLIKELY(easy_buffer.write_avail_size() < handle_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("buff in not avail", K(handle_len), "easy_buff_len",
                easy_buffer.write_avail_size(), K(ret));
    } else if (OB_UNLIKELY(handle_len > ObProtoEncodeParam::MAX_PROTO20_PAYLOAD_LEN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid len", K(handle_len), K(ObProtoEncodeParam::MAX_PROTO20_PAYLOAD_LEN), K(ret));
    } else {
      MEMCPY(easy_buffer.last(), param.get_start(), handle_len);
      if (OB_FAIL(param.add_pos(handle_len))) {
        LOG_ERROR("fail to add_pos", K(handle_len), K(ret));
      } else {
        easy_buffer.write(handle_len);
        if (!param.is_large_packet_cached_avail()) {
          // this means the large packet has encode complete
          need_break = true;  // wait to encode next one
          param.is_pkt_encoded_ = true;
        } else {
          // there must be enough space for tailer
          proto20_context.next_step_ = FILL_TAILER_STEP;
        }
      }
    }
  } else {
    bool is_buffer_enough = true;
    int8_t origin_seq = param.pkt_->get_seq();
    if (easy_buffer.read_avail_size() > proto20_context.header_len_ &&
        has_extra_info(param)) {
      // should flush and encode this package again
      is_buffer_enough = false;
    } else if (easy_buffer.read_avail_size() > ObProtoEncodeParam::PROTO20_SPLIT_LEN) {
       // should flush and encode this package again
       is_buffer_enough = false;
    } else {
      char* ori = easy_buffer.last();
      param.proto20_context_->has_extra_info_ |= has_extra_info(param);
      if (param.proto20_context_->is_new_extra_info_ &&
          OB_FAIL(encode_new_extra_info(easy_buffer.last(),
                                    easy_buffer.write_avail_size(),
                                    seri_size,
                                    param.extra_info_ecds_))) {
        LOG_WARN("failed to encode extra info", K(ret));
      } else if (!param.proto20_context_->is_new_extra_info_ && 
          OB_FAIL(encode_extra_info(easy_buffer.last(),
                                    easy_buffer.write_avail_size(),
                                    seri_size,
                                    param.extra_info_kvs_))) {
        LOG_WARN("failed to encode extra info", K(ret));
      } else if (OB_FAIL(param.pkt_->encode(easy_buffer.last(),
                                            easy_buffer.write_avail_size(),
                                            seri_size))) {
        if (ret == OB_SIZE_OVERFLOW) {
          // do nothing
        } else {
          LOG_WARN("failed to do encode", K(ret));
        }
      } else {
        param.proto20_context_->has_extra_info_ |= has_extra_info(param);
      }
      if (OB_FAIL(ret)) {
        param.encode_ret_ = ret;
        ret = OB_SUCCESS;
        is_buffer_enough = false;
      }
    }

    int64_t split_count = 0;
    if (is_buffer_enough) {
      split_count = ((seri_size / ObProtoEncodeParam::PROTO20_SPLIT_LEN) + 1);
      if (0 == (seri_size % ObProtoEncodeParam::PROTO20_SPLIT_LEN)) {
        --split_count;
      }
      int64_t total_need_size = split_count * (proto20_context.header_len_ + proto20_context.tailer_len_)
                               + seri_size
                               + proto20_context.tailer_len_; // the potential pre packet taler
      if (easy_buffer.write_avail_size() < total_need_size) {
        // treat as buffer not enough
        is_buffer_enough = false;
        // ObMySQLPacket::encode will add hdr_.seq_ to pkt_count after encoding is successful, but at this time due to insufficient easy buffer
        // This packet will be re-encoded, and pkt_count will be added to hdr_.seq_ after success, resulting in discontinuous seq returned to the client
        // So if it fails here, you need to roll back the seq of mysql packet
        param.pkt_->set_seq(origin_seq);
        param.encode_ret_ = OB_SIZE_OVERFLOW;
        ret = OB_SUCCESS;
      }
    }

    if (is_buffer_enough) {
      if (1 == split_count) {
        easy_buffer.write(seri_size);
        param.is_pkt_encoded_ = true;
        // noting break, wait to encode next one
        if (param.conn_->pkt_rec_wrapper_.enable_proto_dia()) {
          param.conn_->pkt_rec_wrapper_.record_send_mysql_pkt(*param.pkt_, seri_size);
        }
        need_break = true;
      } else if (split_count > 1) {
        if (OB_FAIL(param.save_large_packet(easy_buffer.last(), seri_size))) {
          LOG_ERROR("fail to save large packet data", K(seri_size), K(ret));
        } else {
          if (easy_buffer.read_avail_size() > proto20_context.header_len_) {
            // this means some data already in buff, make it to be a proto20 packet
          } else {
            easy_buffer.write(ObProtoEncodeParam::PROTO20_SPLIT_LEN);
            if (OB_FAIL(param.add_pos(ObProtoEncodeParam::PROTO20_SPLIT_LEN))) {
              LOG_ERROR("fail to add pos", K(ObProtoEncodeParam::PROTO20_SPLIT_LEN), K(ret));
            }
            if (param.conn_->pkt_rec_wrapper_.enable_proto_dia()) {
              param.conn_->pkt_rec_wrapper_.record_send_mysql_pkt(*param.pkt_,
                                                ObProtoEncodeParam::PROTO20_SPLIT_LEN);
            }
          }
          proto20_context.next_step_ = FILL_TAILER_STEP;
        }
      }
    } else {
      if (easy_buffer.read_avail_size() == proto20_context.header_len_) {
        easy_buffer.fall_back(proto20_context.header_len_); // reset buffer to alloc more mem
        proto20_context.next_step_ = START_TO_FILL_STEP;
        param.need_flush_ = true; // break, alloc more memory
        need_break = true;
      } else if (easy_buffer.read_avail_size() < proto20_context.header_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("impossible", "read_avail", easy_buffer.read_avail_size(),
                  "header len", proto20_context.header_len_, K(ret));
      } else {
        // there must be enough space for tailer
        proto20_context.next_step_ = FILL_TAILER_STEP;
      }
    }
  }
  return ret;
}

inline int ObProto20Utils::fill_proto20_tailer(ObProtoEncodeParam &param) {
  INIT_SUCC(ret);
  ObEasyBuffer easy_buffer(*param.ez_buf_);
  ObProto20Context &proto20_context = *param.proto20_context_;

  if (OB_UNLIKELY(easy_buffer.read_avail_size() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid proto20 buffer", K(easy_buffer), K(ret));
  } else if (OB_UNLIKELY(easy_buffer.write_avail_size() < proto20_context.tailer_len_)) {
    ret = OB_ERR_UNEXPECTED; // impossible
    LOG_ERROR("impossible", "write avail", easy_buffer.write_avail_size(),
              "tailer len", proto20_context.tailer_len_, K(ret));
  } else {
    int64_t pos = 0;
    char *start = easy_buffer.begin() + proto20_context.header_len_;
    int64_t len = easy_buffer.read_avail_size() - proto20_context.header_len_;
    uint64_t crc64 = 0;
    if (!proto20_context.is_checksum_off_) {
      crc64 = ob_crc64(start, len);
    }
    if (OB_FAIL(ObMySQLUtil::store_int4(easy_buffer.last(), proto20_context.tailer_len_, (int32_t)(crc64), pos))) {
      LOG_ERROR("fail to store int4", K(ret));
    } else {
      easy_buffer.write(proto20_context.tailer_len_);
      proto20_context.next_step_ = FILL_HEADER_STEP;
      LOG_DEBUG("fill proto20 tailer succ", K(crc64));
    }
  }
  return ret;
}

inline int ObProto20Utils::fill_proto20_header(ObProtoEncodeParam &param) {
  INIT_SUCC(ret);
  ObEasyBuffer easy_buffer(*param.ez_buf_);
  ObProto20Context &proto20_context = *param.proto20_context_;

  uint32_t compress_len = static_cast<uint32_t>(easy_buffer.read_avail_size() - OB_MYSQL_COMPRESSED_HEADER_SIZE);
  uint8_t compress_seq = proto20_context.comp_seq_;
  ++proto20_context.comp_seq_;
  uint32_t uncompress_len = 0;
  int16_t magic_num = OB20_PROTOCOL_MAGIC_NUM;
  uint16_t version = OB20_PROTOCOL_VERSION_VALUE;
  uint32_t connid = param.conn_id_;
  uint32_t request_id = proto20_context.request_id_;
  uint8_t packet_seq = 0;
  ++proto20_context.proto20_seq_;
  int64_t payload_len = easy_buffer.read_avail_size() - proto20_context.header_len_ - proto20_context.tailer_len_;
  Ob20ProtocolFlags flag;
  flag.st_flags_.OB_EXTRA_INFO_EXIST = param.proto20_context_->has_extra_info_;

  flag.st_flags_.OB_IS_LAST_PACKET = (ObProto20Utils::is_the_last_packet(param) ? 1 : 0);
  flag.st_flags_.OB_IS_NEW_EXTRA_INFO = proto20_context.is_new_extra_info_;
  flag.st_flags_.OB_TXN_FREE_ROUTE = proto20_context.txn_free_route_ ? 1 : 0;
  uint16_t reserved = 0;
  uint16_t header_checksum = 0;
  int64_t pos = 0;
  char *start = easy_buffer.begin();
  if (OB_ISNULL(param.req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("request is null", K(ret));
  } else if (FALSE_IT(packet_seq = param.conn_->proto20_pkt_context_.proto20_last_pkt_seq_ + 1)) {
  } else if (OB_UNLIKELY(compress_len > OB_MYSQL_MAX_PAYLOAD_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid compress_len", K(compress_len), K(OB_MYSQL_MAX_PAYLOAD_LENGTH), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int3(start, proto20_context.header_len_, compress_len, pos))) {
    LOG_ERROR("fail to store int3", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(start, proto20_context.header_len_, compress_seq, pos))) {
    LOG_ERROR("fail to store int1", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int3(start, proto20_context.header_len_, uncompress_len, pos))) {
    LOG_ERROR("fail to store int3", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int2(start, proto20_context.header_len_, magic_num, pos))) {
    LOG_ERROR("fail to store int2", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int2(start, proto20_context.header_len_, version, pos))) {
    LOG_ERROR("fail to store int2", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int4(start, proto20_context.header_len_, connid, pos))) {
    LOG_ERROR("fail to store int4", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int3(start, proto20_context.header_len_, request_id, pos))) {
    LOG_ERROR("fail to store int4", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(start, proto20_context.header_len_, packet_seq, pos))) {
    LOG_ERROR("fail to store int4", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int4(start, proto20_context.header_len_, (uint32_t)(payload_len), pos))) {
    LOG_ERROR("fail to store int4", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int4(start, proto20_context.header_len_, flag.flags_, pos))) {
    LOG_ERROR("fail to store int4", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int2(start, proto20_context.header_len_, reserved, pos))) {
    LOG_ERROR("fail to store int2", K(ret));
  } else {
    ++param.conn_->proto20_pkt_context_.proto20_last_pkt_seq_;
    // calc header checksum
    if (param.conn_->pkt_rec_wrapper_.enable_proto_dia()) {
      param.conn_->pkt_rec_wrapper_.end_seal_obp20_pkt((uint32_t)(payload_len), flag, packet_seq, 0,
                                      request_id, compress_len, compress_seq);
    }
    if (!proto20_context.is_checksum_off_) {
      header_checksum = ob_crc16(0, reinterpret_cast<uint8_t *>(start), pos);
    }
    if (OB_FAIL(ObMySQLUtil::store_int2(start, proto20_context.header_len_, header_checksum, pos))) {
      LOG_ERROR("fail to store int2", K(ret));
    } else {
      proto20_context.next_step_ = FILL_DONE_STEP;
      LOG_TRACE("fill proto20 header succ", K(compress_len), K(compress_seq), K(uncompress_len),
                K(magic_num), K(version), K(connid), K(request_id), K(packet_seq), K(payload_len),
                K(flag.flags_), K(reserved), K(header_checksum));
    }
  }
  return ret;
}

inline bool ObProto20Utils::is_the_last_packet(const ObProtoEncodeParam &param)
{
  bool bret = false;
  if (param.is_last_) {
    if (NULL == param.pkt_) { // the last flush
      bret = true;
    } else if (param.is_pkt_encoded_) { // the last packet and has been encoded
      bret = true;
    }
  }
  return bret;
}

// if current packet has extra info and buf has other packet, 
// directly send last packet and current packet will do retry.
inline bool ObProto20Utils::has_extra_info(const ObProtoEncodeParam &param)
{
  bool bret = false;

  // use old extra info, obobj format
  if (!param.proto20_context_->is_new_extra_info_) {
      if ((NULL != param.extra_info_kvs_) &&
          (param.extra_info_kvs_->count() > 0)) {
        bret = true;
      } else {
        bret = false;
      }
  // use new extra info
  } else {
      if ((NULL != param.extra_info_ecds_) &&
             (param.extra_info_ecds_->count() > 0)) {
        bret = true;
      } else {
        bret = false;
      }
  }

  return bret;
}

int ObProto20Utils::reset_extra_info(ObProtoEncodeParam &param) {
  int ret = OB_SUCCESS;

  if(OB_ISNULL(param.proto20_context_)) {
    // do nothing
  } else if (param.proto20_context_->is_new_extra_info_) {
    if(OB_ISNULL(param.extra_info_ecds_)) {
      // do nothing
    } else {
      ObSEArray<Obp20Encoder*, 1> tmp_ecds;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.extra_info_ecds_->count(); i++) {
        if (OB_ISNULL(param.extra_info_ecds_->at(i))) {
          // do nothing
        } else if (!param.extra_info_ecds_->at(i)->is_serial_
            && OB_FAIL(tmp_ecds.push_back(param.extra_info_ecds_->at(i)))) {
          LOG_WARN("failed to push back encoders", K(ret), K(i));
        }
      }
      if (OB_FAIL(ret)) {
       // do nothing
      } else {
        param.extra_info_ecds_->reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_ecds.count(); i++) {
          if (OB_FAIL(param.extra_info_ecds_->push_back(tmp_ecds.at(i)))) {
            LOG_WARN("failed to push back encoders", K(ret), K(i));
          }
        }
        if (0 == param.extra_info_ecds_->count()) {
          param.proto20_context_->has_extra_info_ = false;
        }
      }
    }
  } else {
    if (OB_ISNULL(param.extra_info_kvs_)) {
      // do nothing
    } else {
      param.extra_info_kvs_->reset();
      param.proto20_context_->has_extra_info_ = false;
    }
  }
  return ret;
}

} //end of namespace obmysql
} //end of namespace oceanbase
