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

#include "rpc/obmysql/ob_mysql_protocol_processor.h"
#include "rpc/obmysql/ob_mysql_handler.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_handshake_response.h"
#include "rpc/obmysql/packet/ompk_ssl_request.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"

namespace oceanbase {
namespace obmysql {
using namespace oceanbase::common;
using namespace oceanbase::rpc;

int ObMysqlProtocolProcessor::decode(easy_message_t* m, rpc::ObPacket*& pkt)
{
  pkt = NULL;
  INIT_SUCC(ret);
  const uint32_t sessid = handler_.get_sessid(m->c);
  const int64_t header_size = OB_MYSQL_HEADER_LENGTH;
  // no need duplicated check 'm' valid, ObMySQLHandler::process() has already checked
  if (m->input->last - m->input->pos >= header_size) {
    // 1. decode length from net buffer
    // 2. decode seq from net buffer
    uint32_t pktlen = 0;
    uint8_t pktseq = 0;
    ObMySQLUtil::get_uint3(m->input->pos, pktlen);
    ObMySQLUtil::get_uint1(m->input->pos, pktseq);

    // received packet length, exclude packet header
    uint32_t rpktlen = static_cast<uint32_t>(m->input->last - m->input->pos);
    if (OB_FAIL(check_mysql_packet_len(pktlen))) {
      LOG_ERROR("fail to check mysql packet len", K(sessid), K(pktseq), K(pktlen), K(ret));
    } else if (pktlen > rpktlen) {  // one packet was not received complete
      int64_t delta_len = pktlen - rpktlen;
      // valid packet, but not sufficient data received by easy, tell easy read more.
      // go backward with MySQL packet header length
      if (OB_FAIL(set_next_read_len(m, header_size, delta_len))) {
        LOG_ERROR("fail to set next read len", K(sessid), K(pktlen), K(rpktlen), K(ret));
      }
    } else if (handler_.is_in_authed_phase(m->c)) {
      if (OB_FAIL(decode_body(*m, pktlen, pktseq, pkt))) {
        LOG_ERROR("fail to decode_body", K(sessid), K(pktseq), K(ret));
      }
    } else {
      if (OB_UNLIKELY(pktlen < ObMySQLPacket::MIN_CAPABILITY_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error ssl request packet", K(pktlen), K(ret));
      } else {
        ObMySQLCapabilityFlags capability;
        capability.capability_ = ob_uint2korr(m->input->pos);
        if (handler_.is_in_connected_phase(m->c)) {
          if (1 == capability.cap_flags_.OB_CLIENT_SSL) {
            if (OB_FAIL(decode_sslr_body(*m, pktlen, pktseq, pkt))) {
              LOG_WARN("fail to decode_sslr_body", K(sessid), K(pktseq), K(ret));
            } else {
              handler_.set_ssl_connect_phase(m->c);
            }
          } else {
            if (OB_FAIL(decode_hsr_body(*m, pktlen, pktseq, pkt))) {
              LOG_WARN("fail to decode_hsr_body", K(sessid), K(pktseq), K(ret));
            } else {
              handler_.set_connect_phase(m->c);
            }
          }
        } else {
          if (OB_UNLIKELY(1 != capability.cap_flags_.OB_CLIENT_SSL)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error capability from ssl request packet", K(ret));
          } else if (OB_FAIL(decode_hsr_body(*m, pktlen, pktseq, pkt))) {
            LOG_WARN("fail to decode_hsr_body", K(sessid), K(pktseq), K(ret));
          } else {
            handler_.set_connect_phase(m->c);
          }
        }
      }
    }
  }

  return ret;
}

int ObMysqlProtocolProcessor::process(easy_request_t* r, bool& need_decode_more)
{
  INIT_SUCC(ret);
  need_decode_more = true;
  if (OB_ISNULL(r) || OB_ISNULL(r->ms) || OB_ISNULL(r->ms->c) || OB_ISNULL(r->ms->pool) || OB_ISNULL(r->ipacket)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(r), K(ret));
  } else if (!handler_.is_in_authed_phase(r->ms->c)) {
    need_decode_more = false;
  } else if (OB_FAIL(process_mysql_packet(*(r->ms->c), *(r->ms->pool), r->ipacket, need_decode_more))) {
    LOG_ERROR("fail to process_mysql_packet", K(ret));
  }
  return ret;
}

inline int ObMysqlProtocolProcessor::decode_hsr_body(
    easy_message_t& m, const uint32_t pktlen, const uint8_t pktseq, rpc::ObPacket*& pkt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(m.input) || OB_ISNULL(m.input->pos) || OB_ISNULL(m.pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("easy callback message null pointer", K(m.input), K(m.input), K(m.pool), K(ret));
  } else {
    const int64_t alloc_size = sizeof(OMPKHandshakeResponse) + pktlen;
    OMPKHandshakeResponse* hsrpkt = reinterpret_cast<OMPKHandshakeResponse*>(handler_.easy_alloc(m.pool, alloc_size));
    if (OB_ISNULL(hsrpkt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory for OMPKHandshakeResponse", K(alloc_size), K(ret));
    } else {
      hsrpkt = new (hsrpkt) OMPKHandshakeResponse();
      hsrpkt->set_seq(pktseq);
      hsrpkt->set_content(reinterpret_cast<char*>(hsrpkt + 1), pktlen);
      MEMCPY((void*)(hsrpkt + 1), m.input->pos, pktlen);

      pkt = hsrpkt;
      m.input->pos += pktlen;
    }
  }
  return ret;
}

inline int ObMysqlProtocolProcessor::decode_sslr_body(
    easy_message_t& m, const uint32_t pktlen, const uint8_t pktseq, rpc::ObPacket*& pkt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(m.input) || OB_ISNULL(m.input->pos) || OB_ISNULL(m.pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("easy callback message null pointer", K(m.input), K(m.pool), K(ret));
  } else {
    const int64_t alloc_size = sizeof(OMPKSSLRequest) + pktlen;
    OMPKSSLRequest* sslrpkt = reinterpret_cast<OMPKSSLRequest*>(handler_.easy_alloc(m.pool, alloc_size));
    if (OB_ISNULL(sslrpkt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory for OMPKSSLRequest", K(alloc_size), K(ret));
    } else {
      sslrpkt = new (sslrpkt) OMPKSSLRequest();
      sslrpkt->set_seq(pktseq);
      sslrpkt->set_content(reinterpret_cast<char*>(sslrpkt + 1), pktlen);
      MEMCPY((void*)(sslrpkt + 1), m.input->pos, pktlen);

      pkt = sslrpkt;
      m.input->pos += pktlen;

      if (OB_FAIL(sslrpkt->decode())) {
        LOG_ERROR("failed to decode OMPKSSLRequest", KPC(sslrpkt), K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlProtocolProcessor::decode_header(char*& buf, uint32_t& buf_size, uint32_t& pktlen, uint8_t& pktseq)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size < OB_MYSQL_HEADER_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("can not decode header", KP(buf), K(buf_size), K(ret));
  } else {
    pktlen = 0;
    pktseq = 0;
    ObMySQLUtil::get_uint3(buf, pktlen);
    ObMySQLUtil::get_uint1(buf, pktseq);
    buf_size -= static_cast<uint32_t>(OB_MYSQL_HEADER_LENGTH);
  }
  return ret;
}

int ObMysqlProtocolProcessor::decode_body(
    easy_message_t& m, const uint32_t pktlen, const uint8_t pktseq, rpc::ObPacket*& pkt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(m.input) || OB_ISNULL(m.input->pos) || OB_ISNULL(m.pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("easy callback message null pointer", K(m.input), K(m.input), K(m.pool), K(ret));
  } else {
    const char* pkt_body = m.input->pos;
    m.input->pos += pktlen;

    ObMySQLRawPacket* raw_pkt = NULL;
    if (OB_ISNULL(pkt_body)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("easy callback message null pointer", K(pkt_body), K(ret));
    } else if (OB_ISNULL(raw_pkt = reinterpret_cast<ObMySQLRawPacket*>(
                             handler_.easy_alloc(m.pool, sizeof(ObMySQLRawPacket))))) {
      // reject this connection, for fear that network buffer
      // would be fulfilled with senseless data.
      //
      // TODO: Maybe skip this packet but preserve connection is
      //       more graceful.
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory available, close connection", "alloc_size", sizeof(ObMySQLRawPacket), K(ret));
    } else {
      raw_pkt = new (raw_pkt) ObMySQLRawPacket();
      raw_pkt->set_seq(pktseq);
      // Attention!! do not get cmd type, process() will handle;
      raw_pkt->set_content(pkt_body, pktlen);
      pkt = raw_pkt;
      LOG_DEBUG("decode body succ", KPC(raw_pkt));
    }
  }
  return ret;
}

int ObMysqlProtocolProcessor::read_header(
    ObMysqlPktContext& context, const char* start, const int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(context.header_buffered_len_ >= OB_MYSQL_HEADER_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("header is received complete, can not read again", K_(context.header_buffered_len), K(ret));
  } else {
    int64_t remain_len = len - pos;
    int64_t copy_len = std::min(remain_len, OB_MYSQL_HEADER_LENGTH - context.header_buffered_len_);
    MEMCPY(context.header_buf_ + context.header_buffered_len_, start + pos, copy_len);
    context.header_buffered_len_ += copy_len;
    if (OB_MYSQL_HEADER_LENGTH == context.header_buffered_len_) {
      // header received complete
      uint32_t pktlen = 0;
      uint8_t pktseq = 0;
      char* header_buf = context.header_buf_;
      uint32_t head_buf_size = static_cast<uint32_t>(OB_MYSQL_HEADER_LENGTH);
      if (OB_FAIL(decode_header(header_buf, head_buf_size, pktlen, pktseq))) {
        LOG_ERROR("fail to decode header", K(ret));
      } else {
        context.payload_len_ = pktlen;
        context.curr_pkt_seq_ = pktseq;
        context.payload_buffered_len_ = 0;
        context.next_read_step_ = ObMysqlPktContext::READ_BODY;
      }
    }
    pos += copy_len;
  }
  return ret;
}

int ObMysqlProtocolProcessor::read_body(ObMysqlPktContext& context, easy_pool_t& pool, const char* start,
    const int64_t len, void*& ipacket, bool& need_decode_more, int64_t& pos)
{
  int ret = OB_SUCCESS;
  need_decode_more = true;
  if (OB_UNLIKELY(context.header_buffered_len_ != OB_MYSQL_HEADER_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("here, header must be buffered", K_(context.header_buffered_len), K(ret));
  } else {
    const int64_t remain_len = len - pos;
    const int64_t handle_len = std::min(remain_len, (context.payload_len_ - context.payload_buffered_len_));
    const int64_t received_len = context.payload_buffered_len_ + handle_len;
    if (received_len == context.payload_len_) {
      // one mysql packet received completed
      context.raw_pkt_.set_seq(context.curr_pkt_seq_);
      context.raw_pkt_.set_content(start + pos, static_cast<uint32_t>(context.payload_len_));
      const int64_t actual_data_len = handle_len;
      void* tmp_ipacket = reinterpret_cast<void*>(&context.raw_pkt_);
      if (OB_FAIL(process_one_mysql_packet(context, pool, actual_data_len, tmp_ipacket, need_decode_more))) {
        LOG_ERROR("fail to process one mysql packet", K(context), K(ret));
      } else {
        if (need_decode_more) {  // mysql packet not received complete
          if (OB_MYSQL_MAX_PAYLOAD_LENGTH != context.payload_len_) {
            ret = OB_ERR_UNEXPECTED;  // just for defense
            LOG_ERROR(
                "payload len must be equal to 2^24-1", K(OB_MYSQL_MAX_PAYLOAD_LENGTH), K_(context.payload_len), K(ret));
          } else if (OB_FAIL(context.save_fragment_mysql_packet(start + pos, handle_len))) {
            LOG_ERROR("fail to save fragment mysql packet", K(pos), K(handle_len), KP(start), K(ret));
          }
        } else {
          ipacket = NULL;
          context.next_read_step_ = ObMysqlPktContext::READ_COMPLETE;
          ObMySQLRawPacket* final_raw_pkt = NULL;
          if (OB_ISNULL(final_raw_pkt = reinterpret_cast<ObMySQLRawPacket*>(
                            handler_.easy_alloc(&(pool), sizeof(ObMySQLRawPacket))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("no memory available", "alloc_size", sizeof(ObMySQLRawPacket), K(ret));
          } else {
            final_raw_pkt = new (final_raw_pkt) ObMySQLRawPacket();
            final_raw_pkt->assign(context.raw_pkt_);
            ipacket = final_raw_pkt;
          }
        }
      }
    } else if (received_len < context.payload_len_) {
      // save and continue to receive
      if (OB_FAIL(context.save_fragment_mysql_packet(start + pos, handle_len))) {
        LOG_ERROR("fail to save fragment mysql packet", K(pos), K(handle_len), KP(start), K(ret));
      }
    } else {
      // received_len > context.payload_len_,
      // impossible, just defense
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected branch", K(received_len), K_(context.payload_len), K(ret));
    }

    if (OB_SUCC(ret)) {
      pos += handle_len;
      if (ObMysqlPktContext::READ_COMPLETE == context.next_read_step_) {
        // nothing
        if (pos < len) {
          const int64_t MAX_DUMP_SIZE = 1024;
          int64_t dump_size = std::min((len - pos), MAX_DUMP_SIZE);
          hex_dump(start + pos, static_cast<int32_t>(dump_size), true, OB_LOG_LEVEL_WARN);
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("can not received two request", K(len), K(pos), K(context), K(ret));
        }
      } else {
        if (received_len == context.payload_len_) {
          // this packet received complete, continue to next
          context.next_read_step_ = ObMysqlPktContext::READ_HEADER;
          context.header_buffered_len_ = 0;
          context.payload_buffered_len_ = 0;
          context.payload_len_ = 0;
        } else {
          // continue read body
          context.next_read_step_ = ObMysqlPktContext::READ_BODY;
        }
      }
    }
  }

  return ret;
}

// maybe just a fragment of one mysql pkt
int ObMysqlProtocolProcessor::process_fragment_mysql_packet(easy_connection_t& c, easy_pool_t& pool, const char* start,
    const int64_t len, void*& ipacket, bool& need_decode_more)
{
  int ret = OB_SUCCESS;
  ObMysqlPktContext* context = NULL;
  need_decode_more = true;
  if (OB_ISNULL(start) || (len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input", KP(start), K(len), K(ret));
  } else if (OB_ISNULL(context = handler_.get_mysql_pkt_context(&c))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mysql pkt context is null", K(ret));
  } else {
    int64_t pos = 0;
    while (OB_SUCC(ret) && (pos < len)) {
      switch (context->next_read_step_) {
        case ObMysqlPktContext::READ_HEADER: {
          if (OB_FAIL(read_header(*context, start, len, pos))) {
            LOG_ERROR("fail to read header", KPC(context), K(ret));
          } else {
            // A special case:
            // when the mysql packet's payload is 16MB-1, then one extra empty
            // packet will received, and after READ_HEADER, the pos == len, and
            // here we handle this case, or it will break the while loop and never
            // recover.
            if ((pos == len) && (0 == context->payload_len_) &&
                (ObMysqlPktContext::READ_BODY == context->next_read_step_)) {
              if (OB_FAIL(read_body(*context, pool, start, len, ipacket, need_decode_more, pos))) {
                LOG_ERROR("fail to read body", KPC(context), K(ret));
              }
            }
          }
          break;
        }
        case ObMysqlPktContext::READ_BODY: {
          if (OB_FAIL(read_body(*context, pool, start, len, ipacket, need_decode_more, pos))) {
            LOG_ERROR("fail to read body", KPC(context), K(ret));
          }
          break;
        }
        case ObMysqlPktContext::READ_COMPLETE:
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected read step", KPC(context), K_(context->next_read_step), K(ret));
          break;
        }
      }
    }

    if (need_decode_more) {
      // continue
    } else {
      // one request received complete
      context->reset();
    }
  }

  return ret;
}

int ObMysqlProtocolProcessor::process_one_mysql_packet(ObMysqlPktContext& context, easy_pool_t& pool,
    const int64_t actual_data_len, void*& ipacket, bool& need_decode_more)
{
  int ret = OB_SUCCESS;
  need_decode_more = true;

  ObMySQLRawPacket* raw_pkt = NULL;
  if (OB_ISNULL(raw_pkt = reinterpret_cast<ObMySQLRawPacket*>(ipacket))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ipacket is null", K(ret));
  } else if (context.is_multi_pkt_) {
    const uint8_t curr_seq = raw_pkt->get_seq();
    const uint8_t expected_seq = static_cast<uint8_t>(context.last_pkt_seq_ + 1);
    if (OB_UNLIKELY(curr_seq != expected_seq)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mysql seq mismatch", K(curr_seq), K(expected_seq), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t data_len = raw_pkt->get_clen();
    const char* payload = raw_pkt->get_cdata();
    context.last_pkt_seq_ = raw_pkt->get_seq();
    int64_t total_data_len = 0;
    if (data_len < 0 || actual_data_len < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid len", K(data_len), K(actual_data_len), K(ret));
    } else if (data_len < OB_MYSQL_MAX_PAYLOAD_LENGTH) {
      if (NULL != context.payload_buf_) {
        const int64_t total_len = context.payload_buffered_total_len_ + actual_data_len;
        char* tmp_buffer = NULL;
        if (OB_ISNULL(tmp_buffer = reinterpret_cast<char*>(handler_.easy_alloc(&(pool), total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("no memory available", "alloc_size", total_len, K(ret));
        } else {
          MEMCPY(tmp_buffer, context.payload_buf_, context.payload_buffered_total_len_);
          MEMCPY((tmp_buffer + context.payload_buffered_total_len_), payload, actual_data_len);

          payload = tmp_buffer;
          total_data_len = total_len;
          if (OB_UNLIKELY((context.payload_buffered_len_ + actual_data_len) != context.payload_len_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("invalid payload len", K(actual_data_len), K(context));
          }
        }
      } else {
        total_data_len = actual_data_len;
      }
      if (OB_SUCC(ret)) {
        uint8_t cmd = 0;
        ObMySQLUtil::get_uint1(payload, cmd);
        raw_pkt->set_cmd(static_cast<ObMySQLCmd>(cmd));
        raw_pkt->set_content(payload, static_cast<uint32_t>(total_data_len));
        // no need set seq again
        need_decode_more = false;
        ipacket = raw_pkt;
        LOG_DEBUG("recevie one mysql packet complete", K(context), KPC(raw_pkt), K(total_data_len), K(actual_data_len));
      }
    } else if (data_len == OB_MYSQL_MAX_PAYLOAD_LENGTH) {
      // If the payload is larger than or equal to 2^24-1 bytes the length is
      // set to 2^24-1 (ff ff ff) and a additional packets are sent with the
      // rest of the payload until the payload of a packet is less than 2^24-1 bytes.
      // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
      context.is_multi_pkt_ = true;
    } else {
      // impossible, just defense
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid len", K(data_len), K(actual_data_len), K(OB_MYSQL_MAX_PAYLOAD_LENGTH), K(ret));
    }
  }

  return ret;
}

int ObMysqlProtocolProcessor::process_mysql_packet(
    easy_connection_t& c, easy_pool_t& pool, void*& ipacket, bool& need_decode_more)
{
  int ret = OB_SUCCESS;
  ObMysqlPktContext* context = NULL;
  need_decode_more = true;
  ObMySQLRawPacket* raw_pkt = NULL;
  if (OB_ISNULL(raw_pkt = reinterpret_cast<ObMySQLRawPacket*>(ipacket))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ipacket is null", K(ret));
  } else {
    int64_t data_len = raw_pkt->get_clen();
    const char* payload = raw_pkt->get_cdata();
    if (OB_ISNULL(context = handler_.get_mysql_pkt_context(&c))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mysql pkt context is null", K(ret));
    } else if (FALSE_IT(context->payload_len_ = data_len)) {
      // impossible
    } else if (OB_FAIL(process_one_mysql_packet(*context, pool, data_len, ipacket, need_decode_more))) {
      LOG_ERROR("fail to process one mysql packet", KPC(context), K(need_decode_more), K(ret));
    } else {
      if (need_decode_more) {
        if (OB_MYSQL_MAX_PAYLOAD_LENGTH != context->payload_len_) {
          ret = OB_ERR_UNEXPECTED;  // just for defense
          LOG_ERROR(
              "payload len must be equal to 2^24-1", K(OB_MYSQL_MAX_PAYLOAD_LENGTH), K_(context->payload_len), K(ret));
        } else if (OB_FAIL(context->save_fragment_mysql_packet(payload, data_len))) {
          // not received complete, continue
          LOG_ERROR("fail to save fragment mysql packet", KP(payload), K(data_len), KPC(context), K(ret));
        } else {
          context->payload_buffered_len_ = 0;
          context->payload_len_ = 0;
        }
      } else {
        // nothing
        context->reset();
      }
    }
  }
  return ret;
}

}  // end of namespace obmysql
}  // end of namespace oceanbase
