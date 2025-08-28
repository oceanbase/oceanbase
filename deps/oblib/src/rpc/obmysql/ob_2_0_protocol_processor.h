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

#ifndef _OB_2_0_PROTOCOL_PROCESSOR_H_
#define _OB_2_0_PROTOCOL_PROCESSOR_H_

#include "rpc/obmysql/ob_mysql_protocol_processor.h"
#include "rpc/obmysql/obp20_extra_info.h"
#include "rpc/obmysql/ob_packet_record.h"

namespace oceanbase
{
namespace rpc
{
class ObPacket;
}
namespace obmysql
{
class Ob20ProtocolHeader;
class ObProto20PktContext;

class Ob20ProtocolProcessor : public ObMysqlProtocolProcessor
{
public:
  Ob20ProtocolProcessor()
    : ObMysqlProtocolProcessor() {}
  virtual ~Ob20ProtocolProcessor() {}

  virtual int do_decode(observer::ObSMConnection& conn, ObICSMemPool& pool, const char*& start, const char* end, rpc::ObPacket*& pkt, int64_t& next_read_bytes);
  virtual int do_splice(observer::ObSMConnection& conn, ObICSMemPool& pool, void*& pkt, bool& need_decode_more);

private:
  int do_header_checksum(const char *origin_start, const Ob20ProtocolHeader &hdr, bool need_check_compress);
  int do_body_checksum(const char* buf, const Ob20ProtocolHeader &hdr);
  int decode_extra_info(const Ob20ProtocolHeader &hdr,
                        const char*& payload_start,
                        Ob20ExtraInfo &extra_info);
  int decode_new_extra_info(const Ob20ProtocolHeader &hdr, 
                            const char*& payload_start,
                            Ob20ExtraInfo &extra_info);
  int decode_ob20_body(ObICSMemPool& pool, const char*& buf, const Ob20ProtocolHeader &hdr, rpc::ObPacket *&pkt);
  int process_ob20_packet(ObProto20PktContext& context, ObMysqlPktContext &mysql_pkt_context,
                          obmysql::ObPacketRecordWrapper &pkt_rec_wrapper, ObICSMemPool& pool,
                          void *&ipacket, bool &need_decode_more);
  int decode_ob20_header(const char*& origin_start, const char*& start, const char* end,
                          Ob20ProtocolHeader &header20, uint32_t sessid, bool need_check_compress);
  int decode_compressed_packet(const char *comp_buf, const uint32_t comp_pktlen,
          const uint32_t pktlen_before_compress, char *&pkt_body, const uint32_t pkt_body_size);
  int process_compressed_ob20_packet(uint32_t sessid, ObProto20PktContext& context,
                                 ObMysqlPktContext &mysql_pkt_context,
                                 obmysql::ObPacketRecordWrapper &pkt_rec_wrapper, ObICSMemPool& pool,
                                 void *&ipacket, bool &need_decode_more);
  int after_process_mysql_packet(ObICSMemPool& pool, ObProto20PktContext& context,
                    obmysql::ObPacketRecordWrapper &pkt_rec_wrapper,
                    void *&ipacket, bool &need_decode_more, Ob20Packet *pkt20);
  int decode_compressed_body(ObICSMemPool& pool, const char*& buf,
                             const uint32_t comp_pktlen, const uint8_t comp_pktseq, 
                             const uint32_t pktlen_before_compress, rpc::ObPacket *&pkt);
  Obp20Decoder* svr_decoders_[OBP20_SVR_END-OBP20_PROXY_MAX_TYPE] = {
                              &trace_info_dcd_,
                              &sess_info_dcd_,
                              &full_trc_dcd_,
                              &sess_info_veri_dcd_
                            };
  Obp20TaceInfoDecoder trace_info_dcd_;
  Obp20SessInfoDecoder sess_info_dcd_;
  Obp20FullTrcDecoder full_trc_dcd_;
  Obp20SessInfoVeriDecoder sess_info_veri_dcd_;

private:
  DISALLOW_COPY_AND_ASSIGN(Ob20ProtocolProcessor);
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_2_0_PROTOCOL_PROCESSOR_H_ */
