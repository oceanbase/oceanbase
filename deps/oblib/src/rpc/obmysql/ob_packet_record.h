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

#ifndef _OB_MYSQL_OB_PACKET_RECORD_H_
#define _OB_MYSQL_OB_PACKET_RECORD_H_
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_2_0_protocol_struct.h"

namespace oceanbase
{
namespace observer{
bool __attribute__((weak)) enable_proto_dia();
}
namespace obmysql
{

struct ResRecordFlags {
  uint8_t is_send_: 1; // 0-send, 1-receive
  uint8_t processed_: 1; // 请求处理结束，发送包后将会标记该位。
  uint8_t reservered_: 8; // 其余位用于特殊标记
};

struct Obp20Header {
  uint32_t payload_len_; // 4byte
  Ob20ProtocolFlags flag_; // 4byte
  int32_t req_id_;         // 4byte
  uint8_t pkt_seq_;        // 1byte
  uint16_t ext_flags_;     // 2byte
  Obp20Header() {
    payload_len_ = 0;
    flag_.flags_ = 0;
    pkt_seq_ = 0;
    ext_flags_ = 0;
    req_id_ = 0;
  }
  bool is_valid() const {
    return req_id_ != 0;
  }
  ~Obp20Header() {}
  TO_STRING_KV(K_(payload_len), K_(pkt_seq), K_(req_id), K_(flag_.flags), K_(ext_flags));
};//12byte


/*
  // for send packet
  // 0-> mysql packet; 1->okp;
  // 2->error packet; 3->eof packet
  // 4-> row packet; 5-> field packet;
  // 6->piece packet; 7-> string packet;
  // 8-> prepare packet; 9 ->result header packet
  // 10-> prepare execute packet;

  // for recieve packet
  this field represents packet command
*/
struct ObpMysqHeader {
  union {
    uint32_t len_;
    uint32_t pkt_num_; // 表示row packet/feild packet的数量。
  } mysql_header_;
  uint32_t rec_; // 表示目前收到多少byte的mysql包。
  uint32_t com_len_; // compress head len
  uint8_t seq_;
  uint8_t type_;
  uint8_t com_seq_; // compress head sequence
  uint8_t is_send_;
  ObpMysqHeader() {
    rec_ = 0;
    seq_ = 0;
    mysql_header_.len_ = 0;
  }
  ~ObpMysqHeader() {}

  bool is_com_pkt_valid() const {
    return com_len_ != 0 && com_seq_ != 0;
  }
  TO_STRING_KV(K_(mysql_header_.len), K_(rec), K_(seq));
}; // 16byte

class ObPacketRecord
{
public:
  ObPacketRecord() {
    obp_mysql_header_.type_ = 0;
    obp_mysql_header_.is_send_ = 0;
  }
  ~ObPacketRecord() {}

  //for mysql fragment
  inline void record_recieve_mysql_pkt_fragment(int32_t rec) __restrict__ {
    obp_mysql_header_.rec_ += rec;
  }
  //for mysql fragment end

  // for mysql protocol
  inline void record_recieve_mysql_packet(ObMySQLRawPacket &__restrict__ pkt) __restrict__
  {
    obp_mysql_header_.mysql_header_.len_ = pkt.get_pkt_len();
    obp_mysql_header_.seq_ = pkt.get_seq();
    obp_mysql_header_.type_ = static_cast<uint8_t>(pkt.get_cmd());
    obp_mysql_header_.is_send_ = 0;
  }
  inline void record_send_mysql_packet(ObMySQLPacket &__restrict__ pkt, int32_t len) __restrict__
  {
    if (pkt.get_mysql_packet_type() == ObMySQLPacketType::PKT_ROW ||
        pkt.get_mysql_packet_type() == ObMySQLPacketType::PKT_FIELD) {
      obp_mysql_header_.mysql_header_.pkt_num_++;
    } else {
      obp_mysql_header_.mysql_header_.len_ = len;
    }
    obp_mysql_header_.seq_ = pkt.get_seq();
    obp_mysql_header_.type_ = static_cast<uint8_t>(pkt.get_mysql_packet_type());
    obp_mysql_header_.is_send_ = 1;
  }
  // for mysql protocol end

  // for ob20 protocol
  inline void record_send_obp20_packet(uint32_t payload_len, Ob20ProtocolFlags flag,
                                uint8_t pkt_seq, uint16_t ext_flags,
                                int32_t req_id, uint32_t com_len, uint8_t com_seq)
  {
      obp20_header_.payload_len_ = payload_len;
      obp20_header_.flag_ = flag;
      obp20_header_.pkt_seq_ = pkt_seq;
      obp20_header_.req_id_ = req_id;
      obp20_header_.ext_flags_ = ext_flags;
      obp_mysql_header_.com_len_ = com_len;
      obp_mysql_header_.com_seq_ = com_seq;
  }
  inline void record_recieve_obp20_packet(Ob20Packet& obp20_pkt)
  {
    obp20_header_.payload_len_ = obp20_pkt.get_payload_len();
    obp20_header_.flag_ = obp20_pkt.get_flags();
    obp20_header_.pkt_seq_ = obp20_pkt.get_seq();
    obp20_header_.req_id_ = obp20_pkt.get_request_id();
    obp20_header_.ext_flags_ = 0;
    obp_mysql_header_.com_len_ = obp20_pkt.get_comp_len();
    obp_mysql_header_.com_seq_ = obp20_pkt.get_comp_seq();
  }
  // for ob20 protocol end


  // for compress mysql protocol
  inline void record_send_comp_packet(uint32_t com_len, uint8_t com_seq) __restrict__  {
    obp_mysql_header_.com_len_ = com_len;
    obp_mysql_header_.com_seq_ = com_seq;
  }
  inline void record_recieve_comp_packet(ObMySQLCompressedPacket &com_pkt) __restrict__  {
    obp_mysql_header_.com_len_ = com_pkt.get_comp_len();
    obp_mysql_header_.com_seq_ = com_pkt.get_comp_seq();
  }
  // for compress mysql protocol end

  inline bool is_send_record() const __restrict__  {
    return obp_mysql_header_.is_send_ == 1;
  }

  inline void set_packet_type(ObMySQLPacketType type) __restrict__ {
    obp_mysql_header_.type_ = static_cast<uint8_t>(type);
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;
  Obp20Header obp20_header_;         // 16 byte
  ObpMysqHeader obp_mysql_header_;   // 16  byte

}__attribute((aligned(32)));; // end of class ObPacketRecord

class ObPacketRecordWrapper {
  public:
    static const int64_t REC_BUF_SIZE = 32;
    ObPacketRecordWrapper() {
      start_pkt_pos_ = 0;
      cur_pkt_pos_ = 0;
      last_type_ = obmysql::ObMySQLPacketType::INVALID_PKT;
      enable_proto_dia_ = false;
    }
    ~ObPacketRecordWrapper() {}
    void init() {
      start_pkt_pos_ = 0;
      cur_pkt_pos_ = 0;
      last_type_ = obmysql::ObMySQLPacketType::INVALID_PKT;
      enable_proto_dia_ = observer::enable_proto_dia();
    }
    int64_t to_string(char *buf, int64_t buf_len) const;

    // for 20 protocol
    inline void begin_seal_obp20_pkt() { start_pkt_pos_ = cur_pkt_pos_; }
    inline void end_seal_obp20_pkt(uint32_t payload_len, obmysql::Ob20ProtocolFlags flag,
                                    uint8_t pkt_seq, uint16_t ext_flags, int32_t req_id,
                                    uint32_t com_len, uint8_t com_seq)
    {
      for (int64_t i = start_pkt_pos_;  i < cur_pkt_pos_; i++) {
        int64_t idx = i % ObPacketRecordWrapper::REC_BUF_SIZE;
        obmysql::ObPacketRecord& rec = pkt_rec_[idx];
        rec.record_send_obp20_packet(payload_len, flag, pkt_seq,
                                        ext_flags, req_id, com_len, com_seq);
      }
    }

    inline void record_recieve_obp20_packet(Ob20Packet &obp20_pkt,
                                                            obmysql::ObMySQLRawPacket &pkt)
    {
      int64_t idx = cur_pkt_pos_ % ObPacketRecordWrapper::REC_BUF_SIZE;
      obmysql::ObPacketRecord& rec = pkt_rec_[idx];
      rec.record_recieve_obp20_packet(obp20_pkt);
      rec.record_recieve_mysql_packet(pkt);
      cur_pkt_pos_++;
    }
    // for 20 protocol end


    // for compress protocol
    inline void begin_seal_comp_pkt() { start_pkt_pos_ = cur_pkt_pos_; }
    inline void end_seal_comp_pkt(uint32_t com_len, uint8_t com_seq)
    {
      for (int64_t i = start_pkt_pos_;  i < cur_pkt_pos_; i++) {
        int64_t idx = i % ObPacketRecordWrapper::REC_BUF_SIZE;
        obmysql::ObPacketRecord& rec = pkt_rec_[idx];
        rec.record_send_comp_packet(com_len, com_seq);
      }
    }
    void record_recieve_comp_packet(ObMySQLCompressedPacket &com_pkt,
                                                            obmysql::ObMySQLRawPacket &pkt)
    {
      int64_t idx = cur_pkt_pos_ % ObPacketRecordWrapper::REC_BUF_SIZE;
      obmysql::ObPacketRecord& rec = pkt_rec_[idx];
      rec.record_recieve_comp_packet(com_pkt);
      rec.record_recieve_mysql_packet(pkt);
      cur_pkt_pos_++;
    }
    // for compress protocol end


    // for mysql protocol
    inline void record_send_mysql_pkt(obmysql::ObMySQLPacket &__restrict__ pkt, int32_t len) __restrict__
    {
      if (pkt.get_mysql_packet_type() == last_type_) {
        // do nothing
      } else {
        cur_pkt_pos_++;
      }
      int64_t idx = (cur_pkt_pos_-1) % ObPacketRecordWrapper::REC_BUF_SIZE;
      pkt_rec_[idx].record_send_mysql_packet(pkt, len);
      last_type_ = pkt.get_mysql_packet_type();
    }
    inline void record_recieve_mysql_packet(obmysql::ObMySQLRawPacket &__restrict__ pkt) __restrict__
    {
      int64_t idx = cur_pkt_pos_ % ObPacketRecordWrapper::REC_BUF_SIZE;
      pkt_rec_[idx].record_recieve_mysql_packet(pkt);
      cur_pkt_pos_++;
    }
    inline void record_recieve_mysql_pkt_fragment(int32_t recive) __restrict__
    {
      int64_t idx = cur_pkt_pos_ % ObPacketRecordWrapper::REC_BUF_SIZE;
      pkt_rec_[idx].record_recieve_mysql_pkt_fragment(recive);
    }
    // for mysql protocol end

    inline bool enable_proto_dia() {
      return enable_proto_dia_;
    }
  public:
    obmysql::ObPacketRecord pkt_rec_[REC_BUF_SIZE];
    uint32_t start_pkt_pos_;
    uint32_t cur_pkt_pos_;
    obmysql::ObMySQLPacketType last_type_;
    bool enable_proto_dia_;
};



} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_MYSQL_OB_PACKET_RECORD_H_ */
