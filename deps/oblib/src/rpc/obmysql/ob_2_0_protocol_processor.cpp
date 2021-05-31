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

#include "rpc/obmysql/ob_2_0_protocol_processor.h"
#include "rpc/obmysql/ob_mysql_handler.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/obmysql/ob_2_0_protocol_struct.h"
#include "lib/checksum/ob_crc16.h"
#include "lib/checksum/ob_crc64.h"
#include "common/object/ob_object.h"

namespace oceanbase {
namespace obmysql {
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
//
// OceanBase 2.0 Client/Server Protocol Format:
//
//
//   0       1         2           3          4      Byte
//   +-----------------+----------------------+
//   |    Magic Num    |       Version        |
//   +-----------------+----------------------+
//   |            Connection Id               |
//   +-----------------------------+----------+
//   |         Request Id          |   Seq    |
//   +-----------------------------+----------+
//   |            PayLoad Length              |
//   +----------------------------------------+
//   |                Flag                    |
//   +-----------------+----------------------+
//   |    Reserved     |Header Checksum(CRC16)|
//   +-----------------+----------------------+
//   |        ... PayLoad  Data ...           |----------+
//   +----------------------------------------+          |
//   |    Tailer PayLoad Checksum (CRC32)     |          |
//   +----------------------------------------+          |
//                                                       |
//                                                       |
//                            +--------------------------+
//                            |
//                            |
//                            v
//   +-------------------+-------------------+-------------------------------------+
//   | Extra Len(4Byte)  |  Extra Info(K/V)  |  Basic Info(Standard MySQL Packet)  |
//   +-------------------+-------------------+-------------------------------------+

int Ob20ProtocolProcessor::decode(easy_message_t* m, rpc::ObPacket*& pkt)
{
  int ret = OB_SUCCESS;
  pkt = NULL;
  const uint32_t sessid = handler_.get_sessid(m->c);
  // together with mysql compress header, all treat as packet header
  const int64_t header_size = OB20_PROTOCOL_HEADER_LENGTH + OB_MYSQL_COMPRESSED_HEADER_SIZE;

  // no need duplicated check 'm' valid, ObMySQLHandler::process() has already checked
  if ((m->input->last - m->input->pos) >= header_size) {
    char* origin_start = m->input->pos;
    Ob20ProtocolHeader header20;

    // 1. decode mysql compress header
    uint32_t pktlen = 0;
    uint8_t pktseq = 0;
    uint32_t pktlen_before_compress = 0;  // here, must be 0
    ObMySQLUtil::get_uint3(m->input->pos, pktlen);
    ObMySQLUtil::get_uint1(m->input->pos, pktseq);
    ObMySQLUtil::get_uint3(m->input->pos, pktlen_before_compress);
    header20.cp_hdr_.comp_len_ = pktlen;
    header20.cp_hdr_.comp_seq_ = pktseq;
    header20.cp_hdr_.uncomp_len = pktlen_before_compress;

    // 2. decode proto2.0 header
    ObMySQLUtil::get_uint2(m->input->pos, header20.magic_num_);
    ObMySQLUtil::get_uint2(m->input->pos, header20.version_);
    ObMySQLUtil::get_uint4(m->input->pos, header20.connection_id_);
    ObMySQLUtil::get_uint3(m->input->pos, header20.request_id_);
    ObMySQLUtil::get_uint1(m->input->pos, header20.pkt_seq_);
    ObMySQLUtil::get_uint4(m->input->pos, header20.payload_len_);
    ObMySQLUtil::get_uint4(m->input->pos, header20.flag_.flags_);
    ObMySQLUtil::get_uint2(m->input->pos, header20.reserved_);
    ObMySQLUtil::get_uint2(m->input->pos, header20.header_checksum_);

    LOG_DEBUG("decode proto20 header succ", K(header20));
    // 3. crc16 for header checksum
    if (OB_FAIL(do_header_checksum(origin_start, header20))) {
      LOG_ERROR("fail to do header checksum", K(header20), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_MAGIC_NUM != header20.magic_num_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_ERROR("invalid magic num", K(OB20_PROTOCOL_MAGIC_NUM), K(header20.magic_num_), K(sessid), K(ret));
    } else if (OB_UNLIKELY(sessid != header20.connection_id_)) {
      ret = OB_UNKNOWN_CONNECTION;
      LOG_ERROR("connection id mismatch", K(sessid), K_(header20.connection_id), K(ret));
    } else if (0 != pktlen_before_compress) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("pktlen_before_compress must be 0 here", K(pktlen_before_compress), K(sessid), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_VERSION_VALUE != header20.version_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_ERROR("invalid version", K(OB20_PROTOCOL_VERSION_VALUE), K(header20.version_), K(sessid), K(ret));
    } else if (OB_UNLIKELY(
                   pktlen != (header20.payload_len_ + OB20_PROTOCOL_HEADER_LENGTH + OB20_PROTOCOL_TAILER_LENGTH))) {
      // must only contain one ob20 packet
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid pktlen len",
          K(pktlen),
          K(header20.payload_len_),
          K(sessid),
          K(OB20_PROTOCOL_HEADER_LENGTH),
          K(OB20_PROTOCOL_TAILER_LENGTH),
          K(ret));
    } else {
      // received packet length, include tailer, but exclude packet header
      uint32_t rpktlen = static_cast<uint32_t>(m->input->last - m->input->pos);

      // one packet was not received complete
      if ((header20.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH) > rpktlen) {
        int64_t delta_len = header20.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH - rpktlen;
        // valid packet, but not sufficient data received by easy, tell easy read more.
        // go backward with MySQL packet header length
        if (OB_FAIL(set_next_read_len(m, header_size, delta_len))) {
          LOG_ERROR("fail to set next read len", K(sessid), K(pktlen), K(rpktlen), K(ret));
        }
        // if received at least packet completed, do payload checksum
        // delat_len == 0, recevied one packet complete
        // delta_len < 0, received more than one packet
      } else if (OB_FAIL(do_body_checksum(*m, header20))) {
        LOG_ERROR("fail to do body checksum", K(header20), K(sessid), K(ret));
      } else if (OB_FAIL(decode_ob20_body(*m, header20, pkt))) {
        LOG_ERROR("fail to decode_compressed_body", K(sessid), K(header20), K(ret));
      }
    }
  }

  return ret;
}

inline int Ob20ProtocolProcessor::do_header_checksum(char* origin_start, const Ob20ProtocolHeader& hdr)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(origin_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input value", KP(origin_start), K(ret));
  } else if (0 == hdr.header_checksum_) {
    // 0 means skip checksum
  } else {
    char* header_start = origin_start;
    // mysql compress header len + proto20 header(except 2 byte checksum)
    int64_t check_len = OB20_PROTOCOL_HEADER_LENGTH - 2 + OB_MYSQL_COMPRESSED_HEADER_SIZE;

    // 3. crc16 for header checksum
    uint16_t local_header_checksum = 0;
    local_header_checksum = ob_crc16(0, reinterpret_cast<uint8_t*>(header_start), check_len);
    if (local_header_checksum != hdr.header_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("ob 2.0 protocol header checksum error!",
          K(local_header_checksum),
          K(hdr.header_checksum_),
          K(check_len),
          KP(header_start),
          K(hdr),
          K(ret));
    }
  }

  return ret;
}

inline int Ob20ProtocolProcessor::do_body_checksum(easy_message_t& m, const Ob20ProtocolHeader& hdr)
{
  INIT_SUCC(ret);

  uint32_t payload_checksum = 0;
  int64_t payload_len = hdr.payload_len_;
  char* payload_checksum_pos = m.input->pos + payload_len;
  ObMySQLUtil::get_uint4(payload_checksum_pos, payload_checksum);
  if (0 == payload_checksum) {
    // 0 means skip checksum
  } else {
    char* payload_start = m.input->pos;
    uint64_t local_payload_checksum = ob_crc64(payload_start, payload_len);  // actual is crc32
    local_payload_checksum = ob_crc64(payload_start, payload_len);
    if (OB_UNLIKELY(local_payload_checksum != payload_checksum)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("body checksum error", K(local_payload_checksum), K(payload_checksum), K(hdr), K(ret));
    }
  }

  return ret;
}

inline int Ob20ProtocolProcessor::decode_ob20_body(easy_message_t& m, const Ob20ProtocolHeader& hdr, ObPacket*& pkt)
{
  INIT_SUCC(ret);
  pkt = NULL;
  if (OB_ISNULL(m.c) || OB_ISNULL(m.pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("easy callback message null pointer", KP(m.c), KP(m.pool), K(ret));
  } else {
    const char* payload_start = m.input->pos;
    m.input->pos += (hdr.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH);

    Ob20ExtraInfo extra_info;
    // decode extra info, if needed
    if (hdr.flag_.is_extra_info_exist()) {
      // extra_info:
      // extra_len, key(ObObj), value(ObObj), key, value, ..., key, value
      ObMySQLUtil::get_uint4(payload_start, extra_info.extra_len_);
      if ((0 == extra_info.extra_len_) || (extra_info.extra_len_ > hdr.payload_len_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid extra_len", K(extra_info.extra_len_), K(hdr), K(ret));
      } else {
        int64_t pos = 0;
        const char* buf = payload_start;
        int64_t len = extra_info.extra_len_;
        while (pos < extra_info.extra_len_) {
          common::ObObj key;
          common::ObObj value;
          if (OB_FAIL(key.deserialize(buf, len, pos))) {
            LOG_WARN("fail to deserialize extra info", K(ret));
          } else if (!key.is_varchar()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid extra info key type", K(ret), K(key));
          } else if (OB_FAIL(value.deserialize(buf, len, pos))) {
            LOG_WARN("fail to deserialize extra info", K(ret));
          } else {
            LOG_INFO("extra info", K(key), K(value));
            if (0 == key.get_string().case_compare(ObString("ob_trace_info"))) {
              extra_info.exist_trace_info_ = true;
              if (!value.is_varchar()) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("invalid extra info value type", K(ret), K(key), K(value));
              } else {
                extra_info.trace_info_ = value.get_string();
              }
            } else {
              // do nothing
            }
          }
        }
        payload_start += extra_info.extra_len_;
      }
    }
    if (OB_SUCC(ret)) {
      Ob20Packet* pkt20 = NULL;
      if (OB_ISNULL(pkt20 = reinterpret_cast<Ob20Packet*>(handler_.easy_alloc(m.pool, sizeof(Ob20Packet))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory available, close connection", "alloc_size", sizeof(Ob20Packet), K(ret));
      } else {
        pkt20 = new (pkt20) Ob20Packet();
        pkt20->set_content(payload_start, hdr, extra_info);
        pkt = pkt20;
      }
    }
  }
  return ret;
}

int Ob20ProtocolProcessor::process(easy_request_t* r, bool& need_decode_more)
{
  INIT_SUCC(ret);
  need_decode_more = false;
  if (OB_ISNULL(r) || OB_ISNULL(r->ms) || OB_ISNULL(r->ms->c) || OB_ISNULL(r->ms->pool) || OB_ISNULL(r->ipacket)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(r), K(ret));
  } else if (OB_FAIL(process_ob20_packet(*(r->ms->c), *(r->ms->pool), r->ipacket, need_decode_more))) {
    LOG_ERROR("fail to process_ob20_packet", K(ret));
  }
  return ret;
}

inline int Ob20ProtocolProcessor::process_ob20_packet(
    easy_connection_t& c, easy_pool_t& pool, void*& ipacket, bool& need_decode_more)
{
  INIT_SUCC(ret);
  ObProto20PktContext* context = NULL;
  need_decode_more = true;
  Ob20Packet* pkt20 = NULL;
  if (OB_ISNULL(pkt20 = reinterpret_cast<Ob20Packet*>(ipacket))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ipacket is null", K(ret));
  } else if (OB_ISNULL(context = handler_.get_proto20_pkt_context(&c))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("req_context is null", K(ret));
  } else if (context->is_multi_pkt_) {
    // check sequence
    if (OB_UNLIKELY(pkt20->get_comp_seq() != static_cast<uint8_t>(context->comp_last_pkt_seq_ + 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("comp seq is unexpected",
          "last_seq",
          context->comp_last_pkt_seq_,
          "comp_seq",
          pkt20->get_comp_seq(),
          KPC(pkt20),
          K(ret));
    } else if (OB_UNLIKELY(pkt20->get_seq() != static_cast<uint8_t>(context->proto20_last_pkt_seq_ + 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ob20 packet seq is unexpected",
          "last_seq",
          context->proto20_last_pkt_seq_,
          "current seq",
          pkt20->get_seq(),
          KPC(pkt20),
          K(ret));
    } else if (OB_UNLIKELY(pkt20->get_request_id() != context->proto20_last_request_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ob20 packet request id is unexpected",
          "last request id",
          context->proto20_last_request_id_,
          "current request id",
          pkt20->get_request_id(),
          KPC(pkt20),
          K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    uint32_t mysql_data_size = pkt20->get_mysql_packet_len();
    char* mysql_data_start = const_cast<char*>(pkt20->get_cdata());
    if (OB_FAIL(process_fragment_mysql_packet(c, pool, mysql_data_start, mysql_data_size, ipacket, need_decode_more))) {
      LOG_ERROR("fail to process fragment mysql packet",
          KP(mysql_data_start),
          K(mysql_data_size),
          K(need_decode_more),
          K(ret));
    } else {
      context->comp_last_pkt_seq_ = pkt20->get_comp_seq();
      context->proto20_last_pkt_seq_ = pkt20->get_seq();            // remember the request proto20 seq
      context->proto20_last_request_id_ = pkt20->get_request_id();  // remember the request id
      if (need_decode_more) {
        context->is_multi_pkt_ = true;
      } else {
        // If a MySQL package is split into multiple 2.0 protocol packages,
        // Only after all the sub-packages have been received and the group package is completed, ipacket is set as a
        // complete MySQL package Only then can we set the flag of re-routing If a request is divided into multiple
        // MySQL packages, each MySQL package will also set the re-routing flag
        ObMySQLRawPacket* input_packet = reinterpret_cast<ObMySQLRawPacket*>(ipacket);
        input_packet->set_can_reroute_pkt(pkt20->get_flags().is_proxy_reroute());
        input_packet->set_exist_trace_info(pkt20->get_extra_info().exist_trace_info_);
        input_packet->set_trace_info(pkt20->get_extra_info().trace_info_);
        context->reset();
        // set again for sending response
        context->proto20_last_pkt_seq_ = pkt20->get_seq();
        context->proto20_last_request_id_ = pkt20->get_request_id();
      }
    }
  }

  return ret;
}

}  // end of namespace obmysql
}  // end of namespace oceanbase
