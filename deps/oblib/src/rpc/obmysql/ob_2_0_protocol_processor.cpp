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
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/obmysql/ob_2_0_protocol_struct.h"
#include "lib/checksum/ob_crc16.h"
#include "lib/checksum/ob_crc64.h"
#include "common/object/ob_object.h"
#include "rpc/obmysql/obsm_struct.h"

namespace oceanbase
{
namespace obmysql
{
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::observer;
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

int Ob20ProtocolProcessor::do_decode(ObSMConnection& conn, ObICSMemPool& pool, const char*& start, const char* end, rpc::ObPacket*& pkt, int64_t& next_read_bytes)
{
  int ret = OB_SUCCESS;
  pkt = NULL;
  const uint32_t sessid = conn.sessid_;
  // together with mysql compress header, all treat as packet header
  const int64_t header_size = OB20_PROTOCOL_HEADER_LENGTH + OB_MYSQL_COMPRESSED_HEADER_SIZE;

  // no need duplicated check 'm' valid, ObMySQLHandler::process() has already checked
  if ((end - start) >= header_size) {
    const char *origin_start = start;
    Ob20ProtocolHeader header20;

    // 1. decode mysql compress header
    uint32_t pktlen = 0;
    uint8_t pktseq = 0;
    uint32_t pktlen_before_compress = 0; // here, must be 0
    ObMySQLUtil::get_uint3(start, pktlen);
    ObMySQLUtil::get_uint1(start, pktseq);
    ObMySQLUtil::get_uint3(start, pktlen_before_compress);
    header20.cp_hdr_.comp_len_ = pktlen;
    header20.cp_hdr_.comp_seq_ = pktseq;
    header20.cp_hdr_.uncomp_len = pktlen_before_compress;

    // 2. decode proto2.0 header
    ObMySQLUtil::get_uint2(start, header20.magic_num_);
    ObMySQLUtil::get_uint2(start, header20.version_);
    ObMySQLUtil::get_uint4(start, header20.connection_id_);
    ObMySQLUtil::get_uint3(start, header20.request_id_);
    ObMySQLUtil::get_uint1(start, header20.pkt_seq_);
    ObMySQLUtil::get_uint4(start, header20.payload_len_);
    ObMySQLUtil::get_uint4(start, header20.flag_.flags_);
    ObMySQLUtil::get_uint2(start, header20.reserved_);
    ObMySQLUtil::get_uint2(start, header20.header_checksum_);

    LOG_DEBUG("decode proto20 header succ", K(header20));
    // 3. crc16 for header checksum
    if (OB_FAIL(do_header_checksum(origin_start, header20))) {
      LOG_ERROR("fail to do header checksum", K(header20), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_MAGIC_NUM != header20.magic_num_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_ERROR("invalid magic num", K(OB20_PROTOCOL_MAGIC_NUM),
                K(header20.magic_num_), K(sessid), K(ret));
    } else if (OB_UNLIKELY(sessid != header20.connection_id_)) {
      ret = OB_UNKNOWN_CONNECTION;
      LOG_ERROR("connection id mismatch", K(sessid), K_(header20.connection_id), K(ret));
    } else if (0 != pktlen_before_compress) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("pktlen_before_compress must be 0 here", K(pktlen_before_compress),
                K(sessid), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_VERSION_VALUE != header20.version_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_ERROR("invalid version", K(OB20_PROTOCOL_VERSION_VALUE),
                K(header20.version_), K(sessid), K(ret));
    } else if (OB_UNLIKELY(pktlen !=
                           (header20.payload_len_ + OB20_PROTOCOL_HEADER_LENGTH + OB20_PROTOCOL_TAILER_LENGTH))) {
      // must only contain one ob20 packet
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid pktlen len", K(pktlen), K(header20.payload_len_), K(sessid),
                K(OB20_PROTOCOL_HEADER_LENGTH), K(OB20_PROTOCOL_TAILER_LENGTH), K(ret));
    } else {
      // received packet length, include tailer, but exclude packet header
      uint32_t rpktlen = static_cast<uint32_t>(end - start);

      // one packet was not received complete
      if ((header20.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH) > rpktlen) {
        int64_t delta_len = header20.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH - rpktlen;
        // valid packet, but not sufficient data received by easy, tell easy read more.
        // go backward with MySQL packet header length
        start -= header_size;
        next_read_bytes = delta_len;
        // if received at least packet completed, do payload checksum
        // delat_len == 0, recevied one packet complete
        // delta_len < 0, received more than one packet
      } else if (OB_FAIL(do_body_checksum(start, header20))) {
        LOG_ERROR("fail to do body checksum", K(header20), K(sessid), K(ret));
      } else if (OB_FAIL(decode_ob20_body(pool, start, header20, pkt))) {
        LOG_ERROR("fail to decode_compressed_body", K(sessid), K(header20), K(ret));
      }
    }
  } else {
    /* read at least a header size*/
    next_read_bytes = header_size - (end - start);
  }

  return ret;
}

inline int Ob20ProtocolProcessor::do_header_checksum(const char *origin_start, const Ob20ProtocolHeader &hdr) {
  INIT_SUCC(ret);
  if (OB_ISNULL(origin_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input value", KP(origin_start), K(ret));
  } else if (0 == hdr.header_checksum_) {
    // 0 means skip checksum
  } else {
    const char *header_start = origin_start;
    // mysql compress header len + proto20 header(except 2 byte checksum)
    int64_t check_len = OB20_PROTOCOL_HEADER_LENGTH - 2 + OB_MYSQL_COMPRESSED_HEADER_SIZE;

    // 3. crc16 for header checksum
    uint16_t local_header_checksum = 0;
    local_header_checksum = ob_crc16(0, reinterpret_cast<const uint8_t *>(header_start), check_len);
    if (local_header_checksum != hdr.header_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("ob 2.0 protocol header checksum error!", K(local_header_checksum),
                K(hdr.header_checksum_), K(check_len), KP(header_start), K(hdr), K(ret));
    }
  }

  return ret;
}

inline int Ob20ProtocolProcessor::do_body_checksum(const char* buf, const Ob20ProtocolHeader &hdr) {
  INIT_SUCC(ret);

  uint32_t payload_checksum = 0;
  int64_t payload_len = hdr.payload_len_;
  const char *payload_checksum_pos = buf + payload_len;
  ObMySQLUtil::get_uint4(payload_checksum_pos, payload_checksum);
  if (0 == payload_checksum) {
    // 0 means skip checksum
  } else {
    const char *payload_start = buf;
    uint64_t local_payload_checksum = ob_crc64(payload_start, payload_len); // actual is crc32
    local_payload_checksum = ob_crc64(payload_start, payload_len);
    if (OB_UNLIKELY(local_payload_checksum != payload_checksum))  {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("body checksum error", K(local_payload_checksum),
                K(payload_checksum), K(hdr), K(ret));
    }
  }

  return ret;
}

inline int Ob20ProtocolProcessor::decode_ob20_body(
    ObICSMemPool& pool,
    const char*& buf,
    const Ob20ProtocolHeader &hdr,
    ObPacket *&pkt)
{
  INIT_SUCC(ret);
  pkt = NULL;
  const char *payload_start = buf;
  buf += (hdr.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH);

  Ob20ExtraInfo extra_info;
  // decode extra info, if needed
  if (hdr.flag_.is_new_extra_info() &&
      OB_FAIL(decode_new_extra_info(hdr, payload_start, extra_info))) {
    LOG_WARN("fail to decode extra info", K(hdr), K(extra_info), K(ret),
                                          K(hdr.flag_.is_new_extra_info()));
  } else if (!hdr.flag_.is_new_extra_info() &&
            OB_FAIL(decode_extra_info(hdr, payload_start, extra_info))) {
    LOG_WARN("fail to decode extra info", K(hdr), K(extra_info), K(ret),
                                          K(hdr.flag_.is_new_extra_info()));
  } else {
    Ob20Packet *pkt20 = NULL;
    if (OB_ISNULL(pkt20 = reinterpret_cast<Ob20Packet *>(pool.alloc(sizeof(Ob20Packet))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory available, close connection", "alloc_size", sizeof(Ob20Packet), K(ret));
    } else {
      pkt20 = new (pkt20) Ob20Packet();
      pkt20->set_content(payload_start, hdr, extra_info);
      pkt = pkt20;
    }
  }

  return ret;
}

int Ob20ProtocolProcessor::decode_extra_info(const Ob20ProtocolHeader &hdr,
                                             const char*& payload_start,
                                             Ob20ExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;
  if (hdr.flag_.is_extra_info_exist()) {
    // extra_info:
    // extra_len, key(ObObj), value(ObObj), key, value, ..., key, value
    ObMySQLUtil::get_uint4(payload_start, extra_info.extra_len_);
    if ((0 == extra_info.extra_len_) || (extra_info.extra_len_ > hdr.payload_len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid extra_len", K(extra_info.extra_len_), K(hdr), K(ret));
    } else {
      int64_t pos = 0;
      const char *buf = payload_start;
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
          LOG_TRACE("extra info", K(key), K(value));

          if (0 == key.get_string().case_compare(ObString("sess_inf"))) {
            if (!value.is_varchar()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid extra info value type", K(ret), K(key), K(value));
            } else {
              extra_info.sync_sess_info_ = value.get_string();
              LOG_DEBUG("receive extra_info", KPHEX(extra_info.sync_sess_info_.ptr(),
                                              extra_info.sync_sess_info_.length()));
            }
          } else if (0 == key.get_string().case_compare(ObString("full_trc"))) {
            if (!value.is_varchar()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid extra info value type", K(ret), K(key), K(value));
            } else {
              extra_info.full_link_trace_ = value.get_string();
            }
          } else if (0 == key.get_string().case_compare(ObString("ob_trace_info"))) {
            extra_info.exist_trace_info_ = true;
            if (!value.is_varchar()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid extra info value type", K(ret), K(key), K(value));
            } else {
              extra_info.trace_info_ = value.get_string();
            }
          } else if (0 == key.get_string().case_compare(ObString("sess_ver"))) {
            if (!value.is_varchar()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid extra info value type", K(ret), K(key), K(value));
            } else {
              extra_info.sess_info_veri_ = value.get_string();
            }
          } else {
            // do nothing
          }
        }
      }
      payload_start += extra_info.extra_len_;
    }
  }

  return ret;
}

int Ob20ProtocolProcessor::decode_new_extra_info(const Ob20ProtocolHeader &hdr,
                            const char*& payload_start, Ob20ExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;

  if (hdr.flag_.is_extra_info_exist()) {
    // new extra_info:
    // extra_len, (extra_info_key, len, val), ..., (extra_info_key, len, val)
    extra_info.reset();
    ObMySQLUtil::get_uint4(payload_start, extra_info.extra_len_);
    if ((0 == extra_info.extra_len_) || (extra_info.extra_len_ > hdr.payload_len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid extra_len", K(extra_info.extra_len_), K(hdr), K(ret));
    } else {
      int64_t pos = 0;
      const char *buf = payload_start;
      int64_t len = extra_info.extra_len_;
      while (OB_SUCC(ret) && pos < extra_info.extra_len_) {
        int16_t extra_id = 0;
        if (OB_FAIL(ObProtoTransUtil::resolve_type(buf, len, pos, extra_id))) {
          OB_LOG(WARN,"failed to get extra_info", K(ret), KP(buf));
        } else if (FALSE_IT(pos -= sizeof(int16_t))) {
          // do nothing, reset pos to original
        } else if (extra_id <= OBP20_PROXY_MAX_TYPE && extra_id >= OBP20_SVR_END) {
          // invalid extra_id, skip it
        } else if (OB_ISNULL(svr_decoders_[extra_id-OBP20_PROXY_MAX_TYPE-1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("get a null encoder", K(extra_id), K(extra_id-OBP20_PROXY_MAX_TYPE-1), K(ret));
        } else if (OB_FAIL(svr_decoders_[extra_id-OBP20_PROXY_MAX_TYPE-1]->
                                                    deserialize(buf, len, pos, extra_info))) {
          LOG_WARN("failed to decode", K(ret),  KP(buf), K(len), K(pos), K(extra_id));
        } else {
          // do nothing
        }
      }

      if(OB_FAIL(ret)) {
        // do nothing
      } else {
        payload_start += extra_info.extra_len_;
      }
    }
  }
  return ret;
}


int Ob20ProtocolProcessor::do_splice(ObSMConnection& conn, ObICSMemPool& pool, void*& pkt, bool& need_decode_more)
{
  INIT_SUCC(ret);
  if (OB_FAIL(process_ob20_packet(conn.proto20_pkt_context_, conn.mysql_pkt_context_,
                                    conn.pkt_rec_wrapper_, pool, pkt, need_decode_more))) {
    LOG_ERROR("fail to process_ob20_packet", K(ret));
  }
  return ret;
}

inline int Ob20ProtocolProcessor::process_ob20_packet(ObProto20PktContext& context,
                                                      ObMysqlPktContext &mysql_pkt_context,
                                                      obmysql::ObPacketRecordWrapper &pkt_rec_wrapper,
                                                      ObICSMemPool& pool,
                                                      void *&ipacket, bool &need_decode_more)
{
  INIT_SUCC(ret);
  need_decode_more = true;
  Ob20Packet *pkt20 = NULL;
  if (OB_ISNULL(pkt20 = reinterpret_cast<Ob20Packet *>(ipacket))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ipacket is null", K(ret));
  } else if (context.is_multi_pkt_) {
    // check sequence
    if (OB_UNLIKELY(pkt20->get_comp_seq() != static_cast<uint8_t>(context.comp_last_pkt_seq_ + 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("comp seq is unexpected", "last_seq", context.comp_last_pkt_seq_,
                "comp_seq", pkt20->get_comp_seq(), KPC(pkt20), K(ret));
    } else if (OB_UNLIKELY(pkt20->get_seq() != static_cast<uint8_t>(context.proto20_last_pkt_seq_ + 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ob20 packet seq is unexpected", "last_seq", context.proto20_last_pkt_seq_,
                "current seq", pkt20->get_seq(), KPC(pkt20), K(ret));
    } else if (OB_UNLIKELY(pkt20->get_request_id() != context.proto20_last_request_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ob20 packet request id is unexpected", "last request id",
                context.proto20_last_request_id_, "current request id", pkt20->get_request_id(),
                KPC(pkt20), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    uint32_t mysql_data_size = pkt20->get_mysql_packet_len();
    char *mysql_data_start = const_cast<char *>(pkt20->get_cdata());
    if (pkt_rec_wrapper.enable_proto_dia()) {
      pkt_rec_wrapper.record_recieve_mysql_pkt_fragment(mysql_data_size);
    }
    if (mysql_data_size == 0) {
      // waitting for a not empty packet
      need_decode_more = true;
    } else if (OB_FAIL(process_fragment_mysql_packet(
                        mysql_pkt_context, pool, mysql_data_start,
                        mysql_data_size, ipacket, need_decode_more))) {
      LOG_ERROR("fail to process fragment mysql packet", KP(mysql_data_start),
                K(mysql_data_size), K(need_decode_more), K(ret));
    } else if (!context.extra_info_.exist_extra_info()
        && pkt20->get_extra_info().exist_extra_info()) {
      char* tmp_buffer = NULL;
      int64_t total_len = pkt20->get_extra_info().get_total_len();
      if (OB_ISNULL(tmp_buffer = reinterpret_cast<char *>(context.arena_.alloc(total_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory available", "alloc_size", total_len, K(ret));
      } else if (OB_FAIL(context.extra_info_.assign(pkt20->get_extra_info(), tmp_buffer, total_len))) {
        LOG_ERROR("failed to deep copy extra info", K(ret));
      }
    } else {
      // do nothing
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      context.comp_last_pkt_seq_ = pkt20->get_comp_seq();
      context.proto20_last_pkt_seq_ = pkt20->get_seq(); // remember the request proto20 seq
      context.proto20_last_request_id_ = pkt20->get_request_id(); // remember the request id
      if (need_decode_more) {
        context.is_multi_pkt_ = true;
      } else {
        // If a MySQL package is split into multiple 2.0 protocol packages,
        // Only after all the sub-packages have been received and the group package is completed, ipacket is set as a complete MySQL package
        // Only then can we set the flag of re-routing
        // If a request is divided into multiple MySQL packages, each MySQL package will also set the re-routing flag
        ObMySQLRawPacket *input_packet = reinterpret_cast<ObMySQLRawPacket *>(ipacket);
        input_packet->set_can_reroute_pkt(pkt20->get_flags().is_proxy_reroute());
        input_packet->set_is_weak_read(pkt20->get_flags().is_weak_read());
        // need test proxy_switch_route flag.
        input_packet->set_proxy_switch_route(pkt20->get_flags().proxy_switch_route());
        const int64_t t_len = context.extra_info_.get_total_len();
        char *t_buffer = NULL;
        if (OB_ISNULL(t_buffer = reinterpret_cast<char *>(pool.alloc(t_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("no memory available", "alloc_size", t_len, K(ret));
        } else if (OB_FAIL(input_packet->extra_info_.assign(context.extra_info_, t_buffer, t_len))) {
          LOG_ERROR("failed to assign extra info", K(ret));
        }

        input_packet->set_txn_free_route(pkt20->get_flags().txn_free_route());
        context.reset();
        // set again for sending response
        context.proto20_last_pkt_seq_ = pkt20->get_seq();
        context.proto20_last_request_id_ = pkt20->get_request_id();
        if (pkt_rec_wrapper.enable_proto_dia()) {
          pkt_rec_wrapper.record_recieve_obp20_packet(*pkt20, *input_packet);
        }
      }
    }
  }

  return ret;
}

} // end of namespace obmysql
} // end of namespace oceanbase
