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

#include "rpc/obmysql/ob_mysql_compress_protocol_processor.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "lib/compress/zlib/ob_zlib_compressor.h"
#include "rpc/obmysql/obsm_struct.h"
#include "rpc/obmysql/ob_packet_record.h"

namespace oceanbase
{
namespace obmysql
{
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::observer;

int ObMysqlCompressProtocolProcessor::do_decode(ObSMConnection& conn, ObICSMemPool& pool, const char*& start, const char* end, rpc::ObPacket*& pkt, int64_t& next_read_bytes)
{
  int ret = OB_SUCCESS;
  pkt = NULL;
  const uint32_t sessid = conn.sessid_;
  const int64_t header_size = OB_MYSQL_COMPRESSED_HEADER_SIZE;
  // no need duplicated check 'm' valid, ObMySQLHandler::process() has already checked
  if ((end - start) >= header_size) {
    //1. decode length from net buffer
    //2. decode seq from net buffer
    uint32_t pktlen = 0;
    uint8_t pktseq  = 0;
    /*
     * when use compress, packet header looks like:
     *  3B  length of compressed payload
     *  1B  compressed sequence id
     *  3B  length of payload before compression
     */
    uint32_t pktlen_before_compress = 0;
    ObMySQLUtil::get_uint3(start, pktlen);
    ObMySQLUtil::get_uint1(start, pktseq);
    ObMySQLUtil::get_uint3(start, pktlen_before_compress);

    // received packet length, exclude packet header
    uint32_t rpktlen = static_cast<uint32_t>(end - start);

    if (0 == pktlen) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid arugment", K(sessid), K(pktlen), K(pktlen_before_compress), K(ret));
    } else if (pktlen > rpktlen) { // one packet was not received complete
      int64_t delta_len = pktlen - rpktlen;
      // valid packet, but not sufficient data received by easy, tell easy read more.
      // go backward with MySQL packet header length
      start -= header_size;
      next_read_bytes = delta_len;
    // Attention!! when arrive here, all mysql compress protocols are in command phase
    } else if (OB_FAIL(decode_compressed_body(pool, start, pktlen, pktseq, pktlen_before_compress, pkt))) {
      LOG_ERROR("fail to decode_compressed_body", K(sessid), K(pktseq), K(ret));
    }
  } else {
    /* read at least a header size*/
    next_read_bytes = header_size - (end - start);
  }

  return ret;
}

int ObMysqlCompressProtocolProcessor::do_splice(observer::ObSMConnection& conn, ObICSMemPool& pool, void*& pkt, bool& need_decode_more)
{
  INIT_SUCC(ret);
  if (OB_FAIL(process_compressed_packet(conn.compressed_pkt_context_, conn.mysql_pkt_context_,
                                          conn.pkt_rec_wrapper_, pool, pkt, need_decode_more))) {
    LOG_ERROR("fail to process_compressed_packet", K(ret));
  }
  return ret;
}

inline int ObMysqlCompressProtocolProcessor::decode_compressed_body(ObICSMemPool& pool, const char*& buf,
    const uint32_t comp_pktlen, const uint8_t comp_pktseq, const uint32_t pktlen_before_compress,
    rpc::ObPacket *&pkt)
{
  INIT_SUCC(ret);
  const char *pkt_body = buf;
  buf += comp_pktlen;

  ObMySQLCompressedPacket *cmdpkt = NULL;
  if (OB_ISNULL(pkt_body)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("easy callback message null pointer", KP(pkt_body), K(ret));
  } else if (OB_ISNULL(cmdpkt =
                       reinterpret_cast<ObMySQLCompressedPacket*>(pool.alloc(sizeof(ObMySQLCompressedPacket))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory available, close connection", "alloc_size", sizeof(ObMySQLCompressedPacket), K(ret));
  } else {
    cmdpkt = new (cmdpkt) ObMySQLCompressedPacket();
    cmdpkt->set_content(pkt_body, comp_pktlen, comp_pktseq, pktlen_before_compress);
    pkt = cmdpkt;
    LOG_DEBUG("decompresse packet succ", KPC(cmdpkt));
  }
  return ret;
}

inline int ObMysqlCompressProtocolProcessor::decode_compressed_packet(
    const char *comp_buf, const uint32_t comp_pktlen,
    const uint32_t pktlen_before_compress, char *&pkt_body,
    const uint32_t pkt_body_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(comp_buf) || OB_ISNULL(pkt_body)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguement", KP(comp_buf),KP(pkt_body), K(ret));
  } else if ((0 == pktlen_before_compress && OB_UNLIKELY(comp_pktlen != pkt_body_size))
             || (0 != pktlen_before_compress && OB_UNLIKELY(pktlen_before_compress != pkt_body_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pkt_body_size is unexpected", K(pktlen_before_compress), K(comp_pktlen),
              K(pkt_body_size), K(ret));
  } else {
    // pktlen_before_compress==0 means do not use compress
    if (0 == pktlen_before_compress) {
      pkt_body = const_cast<char *>(comp_buf);
    } else {
      ObZlibCompressor compressor;
      int64_t decompress_data_len = 0;
      if (OB_FAIL(compressor.decompress(comp_buf, comp_pktlen, pkt_body,
                                        pktlen_before_compress, decompress_data_len))) {
        LOG_ERROR("failed to decompress packet", K(ret));
      } else if (OB_UNLIKELY(pktlen_before_compress != decompress_data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to decompress packet", K(pktlen_before_compress),
                   K(decompress_data_len), K(ret));
      }
    }
  }
  return ret;
}

inline int ObMysqlCompressProtocolProcessor::process_compressed_packet(
    ObCompressedPktContext& context, ObMysqlPktContext &mysql_pkt_context,
    obmysql::ObPacketRecordWrapper &pkt_rec_wrapper, ObICSMemPool& pool,
    void *&ipacket, bool &need_decode_more)
{
  int ret = OB_SUCCESS;
  need_decode_more = true;
  ObMySQLCompressedPacket *iraw_pkt = NULL;
  if (OB_ISNULL(iraw_pkt = reinterpret_cast<ObMySQLCompressedPacket*>(ipacket))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ipacket is null", K(ret));
  } else if (context.is_multi_pkt_
      && OB_UNLIKELY(iraw_pkt->get_comp_seq() != static_cast<uint8_t>(context.last_pkt_seq_  + 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("comp seq is unexpected", "last_seq", context.last_pkt_seq_,
              "comp_seq", iraw_pkt->get_comp_seq(), K(ret));
  } else {
    char *decompress_data_buf = NULL;
    uint32_t decompress_data_size = (0 == iraw_pkt->get_uncomp_len()
                                     ? iraw_pkt->get_comp_len()
                                     : iraw_pkt->get_uncomp_len());
    int64_t alloc_size = static_cast<int64_t>(decompress_data_size);
    if (pkt_rec_wrapper.enable_proto_dia()) {
      pkt_rec_wrapper.record_recieve_mysql_pkt_fragment(iraw_pkt->get_comp_len());
    }
    //in order to reuse optimize memory, we put decompressed data into raw_pkt directly
    char *tmp_buffer = NULL;
    if (OB_ISNULL(tmp_buffer = reinterpret_cast<char *>(pool.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory available", K(alloc_size), K(ret));
    } else {
      decompress_data_buf = tmp_buffer;
      if (OB_FAIL(decode_compressed_packet(iraw_pkt->get_cdata(), iraw_pkt->get_comp_len(),
                                           iraw_pkt->get_uncomp_len(), decompress_data_buf,
                                           decompress_data_size))) {
        LOG_ERROR("fail to decode_compressed_packet", K(ret));
      } else if (OB_FAIL(process_fragment_mysql_packet(mysql_pkt_context, pool, decompress_data_buf,
              decompress_data_size, ipacket, need_decode_more))) {
        LOG_ERROR("fail to process fragment mysql packet", KP(decompress_data_buf),
                  K(decompress_data_size), K(need_decode_more), K(ret));
      } else {
        context.last_pkt_seq_ = iraw_pkt->get_comp_seq();
        if (need_decode_more) {
          context.is_multi_pkt_ = true;
        } else {
          if (pkt_rec_wrapper.enable_proto_dia()) {
            ObMySQLRawPacket *raw_pkt = NULL;
            if (OB_ISNULL(raw_pkt = reinterpret_cast<ObMySQLRawPacket *>(ipacket))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("ipacket is null", K(ret));
            } else {
              pkt_rec_wrapper.record_recieve_comp_packet(*iraw_pkt, *raw_pkt);
            }
          }
          context.reset();
        }
      }
    }
  }
  return ret;
}

} // end of namespace obmysql
} // end of namespace oceanbase
