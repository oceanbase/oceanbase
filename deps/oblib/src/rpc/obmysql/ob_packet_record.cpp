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

#include "rpc/obmysql/ob_packet_record.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace observer{
bool __attribute__((weak)) enable_proto_dia()
{
  return false;
}
}
namespace obmysql
{
static const char* pkt_type_name[14] =
{
  "INVALID_PKT",
  "PKT_MYSQL",     // 1 -> mysql packet;
  "PKT_OKP",       // 2 -> okp;
  "PKT_ERR",       // 3 -> error packet;
  "PKT_EOF",       // 4 -> eof packet;
  "PKT_ROW",       // 5 -> row packet;
  "PKT_FIELD",     // 6 -> field packet;
  "PKT_PIECE",     // 7 -> piece packet;
  "PKT_STR",       // 8 -> string packet;
  "PKT_PREPARE",   // 9 -> prepare packet;
  "PKT_RESHEAD",   // 10 -> result header packet
  "PKT_PREXEC",    // 11 -> prepare execute packet;
  "PKT_FILENAME",  // 12 -> file name packet(load local infile)
  "PKT_END"        // 13 -> end of packet type
};

static const char *get_receive_pkt_type_name(const ObpMysqHeader &header)
{
#define PKT_TYPE_NAME(command)  case command: { name = #command; } break

  const char *name = "UNKNOWN_PKT";

  if (header.is_file_content_) {
    name = "FILE_CONTENT";
  } else {
    uint8_t type = header.type_;

    switch (type) {
      PKT_TYPE_NAME(COM_SLEEP);
      PKT_TYPE_NAME(COM_QUIT);
      PKT_TYPE_NAME(COM_INIT_DB);
      PKT_TYPE_NAME(COM_QUERY);
      PKT_TYPE_NAME(COM_FIELD_LIST);
      PKT_TYPE_NAME(COM_CREATE_DB);
      PKT_TYPE_NAME(COM_DROP_DB);
      PKT_TYPE_NAME(COM_REFRESH);
      PKT_TYPE_NAME(COM_SHUTDOWN);
      PKT_TYPE_NAME(COM_STATISTICS);
      PKT_TYPE_NAME(COM_PROCESS_INFO);
      PKT_TYPE_NAME(COM_CONNECT);
      PKT_TYPE_NAME(COM_PROCESS_KILL);
      PKT_TYPE_NAME(COM_DEBUG);
      PKT_TYPE_NAME(COM_PING);
      PKT_TYPE_NAME(COM_TIME);
      PKT_TYPE_NAME(COM_DELAYED_INSERT);
      PKT_TYPE_NAME(COM_CHANGE_USER);
      PKT_TYPE_NAME(COM_BINLOG_DUMP);
      PKT_TYPE_NAME(COM_TABLE_DUMP);
      PKT_TYPE_NAME(COM_CONNECT_OUT);
      PKT_TYPE_NAME(COM_REGISTER_SLAVE);
      PKT_TYPE_NAME(COM_STMT_PREPARE);
      PKT_TYPE_NAME(COM_STMT_EXECUTE);
      PKT_TYPE_NAME(COM_STMT_SEND_LONG_DATA);
      PKT_TYPE_NAME(COM_STMT_CLOSE);
      PKT_TYPE_NAME(COM_STMT_RESET);
      PKT_TYPE_NAME(COM_SET_OPTION);
      PKT_TYPE_NAME(COM_STMT_FETCH);
      PKT_TYPE_NAME(COM_DAEMON);
      PKT_TYPE_NAME(COM_BINLOG_DUMP_GTID);
      PKT_TYPE_NAME(COM_RESET_CONNECTION);
      PKT_TYPE_NAME(COM_END);
      PKT_TYPE_NAME(COM_DELETE_SESSION);
      PKT_TYPE_NAME(COM_HANDSHAKE);
      PKT_TYPE_NAME(COM_LOGIN);
      PKT_TYPE_NAME(COM_STMT_PREXECUTE);
      PKT_TYPE_NAME(COM_STMT_SEND_PIECE_DATA);
      PKT_TYPE_NAME(COM_STMT_GET_PIECE_DATA);
      default: {
      } break;
    }
  }
#undef PKT_TYPE_NAME

  return name;
}

int64_t ObPacketRecord::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (is_send_record()) {
    databuff_printf(buf, buf_len, pos, "send:");
    if (obp_mysql_header_.is_com_pkt_valid()) {
      databuff_printf(buf, buf_len, pos, "obp_compess_header_");
      J_OBJ_START();
      J_KV("com_len", obp_mysql_header_.com_len_, "com_seq", obp_mysql_header_.com_seq_);
      J_OBJ_END();
      J_COMMA();
    }
    if (obp20_header_.is_valid()) {
      J_KV(K(obp20_header_));
      J_COMMA();
    }
    databuff_printf(buf, buf_len, pos, "obp_mysql_header_");
    J_OBJ_START();
    if (obp_mysql_header_.type_ == static_cast<uint8_t>(ObMySQLPacketType::PKT_ROW)
        || obp_mysql_header_.type_ == static_cast<uint8_t>(ObMySQLPacketType::PKT_FIELD)) {
      J_KV("pkt_num_", obp_mysql_header_.mysql_header_.pkt_num_,"seq_", obp_mysql_header_.seq_);
    } else {
      J_KV("len_", obp_mysql_header_.mysql_header_.len_, "seq_", obp_mysql_header_.seq_);
    }
    J_OBJ_END();
    J_COMMA();
    J_KV("pkt_name", pkt_type_name[obp_mysql_header_.type_], K(obp_mysql_header_.is_send_));
  } else {
    databuff_printf(buf, buf_len, pos, "receive:");
    if (obp_mysql_header_.is_com_pkt_valid()) {
      databuff_printf(buf, buf_len, pos, "obp_compess_header_");
      J_OBJ_START();
      J_KV("com_len", obp_mysql_header_.com_len_, "com_seq", obp_mysql_header_.com_seq_);
      J_OBJ_END();
      J_COMMA();
    }
    if (obp20_header_.is_valid()) {
      J_KV(K(obp20_header_));
      J_COMMA();
    }
    databuff_printf(buf, buf_len, pos, "obp_mysql_header_");
    J_OBJ_START();
    J_KV("len_", obp_mysql_header_.mysql_header_.len_,
         "rec_", obp_mysql_header_.rec_, "seq_", obp_mysql_header_.seq_);
    J_OBJ_END();
    J_COMMA();
    J_KV("pkt_name", get_receive_pkt_type_name(obp_mysql_header_),
         K(obp_mysql_header_.type_), K(obp_mysql_header_.is_send_));
  }
  J_OBJ_END();
  return pos;
}

int64_t ObPacketRecordWrapper::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  J_KV(K(start_pkt_pos_), K(cur_pkt_pos_));
  if (cur_pkt_pos_ > 0) {
    J_COMMA();
  }
  int64_t start = 0;
  if (cur_pkt_pos_-REC_BUF_SIZE > 0) {
    start = cur_pkt_pos_-REC_BUF_SIZE;
  }
  for (int64_t index = start; (index < cur_pkt_pos_); index++) {
    databuff_printf(buf, buf_len, pos, "pkt_rec[%ld]:", index);
    BUF_PRINTO(pkt_rec_[index % REC_BUF_SIZE]);
    if (index != cur_pkt_pos_-1) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  return pos;
}

} // end of namespace obmysql
} // end of namespace oceanbase
