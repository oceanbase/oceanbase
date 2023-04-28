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

#include "rpc/obmysql/ob_mysql_packet.h"

#include "lib/utility/ob_macro_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obmysql
{

int ObMySQLPacket::store_string_kv(char *buf, int64_t len, const ObStringKV &str, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMySQLUtil::store_obstr(buf, len, str.key_, pos))) {
    LOG_WARN("store stringkv key fail", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_obstr(buf, len, str.value_, pos))) {
    LOG_WARN("store stringkv value fail", K(ret));
  }
  return ret;
}

uint64_t ObMySQLPacket::get_kv_encode_len(const ObStringKV &string_kv)
{
  uint64_t len = 0;
  len += ObMySQLUtil::get_number_store_len(string_kv.key_.length());
  len += string_kv.key_.length();
  len += ObMySQLUtil::get_number_store_len(string_kv.value_.length());
  len += string_kv.value_.length();
  return len;
}

int ObMySQLPacket::encode(char *buffer, int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t pkt_count = 0;
  if (OB_FAIL(encode(buffer, length, pos, pkt_count))) {
    if (OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("fail to encode packet data", K(length), K(pos), K(ret));
    }
  } else if (OB_UNLIKELY(pkt_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid pkt_count", K(pkt_count), K(pos), K(length));
  } else {
    // here will point next avail seq
    hdr_.seq_ = static_cast<uint8_t>(hdr_.seq_ + pkt_count);
  }

  return ret;
}

int ObMySQLPacket::encode(char *buffer, int64_t length, int64_t &pos, int64_t &pkt_count) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length < 0) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos > length)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (length == pos) {
    ret = OB_SIZE_OVERFLOW;
    //LOG_DEBUG("buffer is not enough", KP(buffer), K(length), K(pos), K(ret));
  } else {
    const int64_t orig_pos = pos;
    // reserve the first packet header
    pos += OB_MYSQL_HEADER_LENGTH;

    if (OB_FAIL(serialize(buffer, length, pos))) {
      //LOG_WARN("encode packet data failed", K(ret));
    } else {
      // If the payload is larger than or equal to 2^24−1 bytes the length is set
      // to 2^24−1 (ff ff ff) and a additional packets are sent with the rest of
      // the payload until the payload of a packet is less than 2^24−1 bytes.
      //
      // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
      //
      const int64_t payload_len = pos - orig_pos - OB_MYSQL_HEADER_LENGTH;
      const int64_t sub_pkt_count = ((payload_len / OB_MYSQL_MAX_PAYLOAD_LENGTH) + 1);
      const int64_t delta_len = payload_len % OB_MYSQL_MAX_PAYLOAD_LENGTH;
      const int64_t header_total_len = sub_pkt_count * OB_MYSQL_HEADER_LENGTH;
      const int64_t total_len = payload_len + header_total_len;
      if (total_len > (length - orig_pos)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("payload is overflow", K(total_len), K(orig_pos), K(length), K(pos), K(ret));
      } else {
        pos = orig_pos;

        char *tmp_buffer = NULL;
        const int64_t alloc_len = payload_len - OB_MYSQL_MAX_PAYLOAD_LENGTH;
        // alloc tmp buffer to store packet data
        if (alloc_len > 0) {
          if (OB_ISNULL(tmp_buffer = reinterpret_cast<char *>(ob_malloc(alloc_len, ObModIds::OB_MYSQL_MULTI_PACKETS)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("fail to alloc memory", K(alloc_len), K(ret));
          } else {
            // copy from the second packet
            char *src_start = buffer + orig_pos + OB_MYSQL_HEADER_LENGTH + OB_MYSQL_MAX_PAYLOAD_LENGTH;
            MEMCPY(tmp_buffer, src_start, alloc_len);
          }
        }

        int64_t curr_payload_len = 0;
        uint8_t tmp_seq = hdr_.seq_;
        // bulid up packets
        for (int64_t i = 0; (i < sub_pkt_count) && OB_SUCC(ret); ++i) {
          curr_payload_len = ((i == (sub_pkt_count - 1)) ? delta_len : OB_MYSQL_MAX_PAYLOAD_LENGTH);

          if (OB_FAIL(ObMySQLUtil::store_int3(buffer, length, static_cast<int32_t>(curr_payload_len), pos))) {
            LOG_ERROR("failed to encode int3", K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, tmp_seq, pos))) {
            LOG_ERROR("failed to encode int1", K(ret));
          } else {
            if ((i > 0) && (NULL != tmp_buffer)) { // the first packet no need copy
              MEMCPY(buffer + pos, tmp_buffer + ((i - 1) * OB_MYSQL_MAX_PAYLOAD_LENGTH), curr_payload_len);
            }
            pos += curr_payload_len;
            ++tmp_seq;
          }
        }

        if (NULL != tmp_buffer) {
          ob_free(tmp_buffer);
          tmp_buffer = NULL;
        }
      }

      if (OB_SUCC(ret)) {
        pkt_count = sub_pkt_count;
      }
    }

    if (OB_FAIL(ret)) {
      pos = orig_pos;
    }
  }
  return ret;
}

int64_t ObMySQLPacket::get_serialize_size() const
{
  BACKTRACE_RET(ERROR, OB_ERROR, 1, "not a serializiable packet");
  return -1;
}

int64_t ObMySQLRawPacket::get_serialize_size() const
{
  return static_cast<int64_t>(get_clen()) + 1; // add 1 for cmd_
}

// serialize content in string<EOF> by default
int ObMySQLRawPacket::serialize(char *buf, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || length <= 0 || pos < 0
                  || length - pos < get_serialize_size() || NULL == cdata_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(length), K(get_serialize_size()), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, length, cmd_, pos))) {
    LOG_WARN("fail to store cmd", K(length), K(cmd_), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buf, length, get_cdata(), get_clen(), pos))) {
    LOG_WARN("fail to store content", K(length), KP(get_cdata()), K(get_clen()), K(pos), K(ret));
  }
  return ret;
}

int Ob20ExtraInfo::assign(const Ob20ExtraInfo &other, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  uint64_t total_len = other.get_total_len();
  if (total_len > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid alloc size", K(total_len), K(ret));
  } else {
    uint64_t len = 0;
    if (other.trace_info_.length() > 0) {
      MEMCPY(buf+len, other.trace_info_.ptr(), other.trace_info_.length());
      trace_info_.assign_ptr(buf+len, other.trace_info_.length());
      len += other.trace_info_.length();
    }
    if (other.sync_sess_info_.length() > 0) {
      MEMCPY(buf+len, other.sync_sess_info_.ptr(), other.sync_sess_info_.length());
      sync_sess_info_.assign_ptr(buf+len, other.sync_sess_info_.length());
      len += other.sync_sess_info_.length();
    }
    if (other.full_link_trace_.length() > 0) {
      MEMCPY(buf+len, other.full_link_trace_.ptr(), other.full_link_trace_.length());
      full_link_trace_.assign_ptr(buf+len, other.full_link_trace_.length());
      len += other.full_link_trace_.length();
    }
    if (other.sess_info_veri_.length() > 0) {
      MEMCPY(buf+len, other.sess_info_veri_.ptr(), other.sess_info_veri_.length());
      sess_info_veri_.assign_ptr(buf+len, other.sess_info_veri_.length());
      len += other.sess_info_veri_.length();
    }
  }
  return ret;
}

char const *get_info_func_name(const ObInformationFunctions func)
{
  const char *str = NULL;
  static const char *func_name_array[MAX_INFO_FUNC] =
  {
    "benchmark",
    "charset",
    "coercibility",
    "coliation",
    "connection_id",
    "current_user",
    "database",
    "found_rows",
    "last_insert_id",
    "row_count",
    "schema",
    "session_user",
    "system user",
    "user",
    "version",
  };

  if (func >= BENCHMARK_FUNC && func < MAX_INFO_FUNC) {
    str = func_name_array[func];
  }
  return str;
}

char const *get_mysql_cmd_str(ObMySQLCmd mysql_cmd)
{
  const char *str = "invalid";
  static const char *mysql_cmd_array[COM_MAX_NUM] =
  {
    "Sleep",  // COM_SLEEP,
    "Quit",  // COM_QUIT,
    "Init DB",  // COM_INIT_DB,
    "Query",  // COM_QUERY,
    "Field List",  // COM_FIELD_LIST,

    "Create DB",  // COM_CREATE_DB,
    "Drop DB",  // COM_DROP_DB,
    "Refresh",  // COM_REFRESH,
    "Shutdown",  // COM_SHUTDOWN,
    "Statistics",  // COM_STATISTICS,

    "Process Info",  // COM_PROCESS_INFO,
    "Connect",  // COM_CONNECT,
    "Process Kill",  // COM_PROCESS_KILL,
    "Debug",  // COM_DEBUG,
    "Ping",  // COM_PING,

    "Time",  // COM_TIME,
    "Delayed insert",  // COM_DELAYED_INSERT,
    "Change user",  // COM_CHANGE_USER,
    "Binlog dump",  // COM_BINLOG_DUMP,

    "Table dump",  // COM_TABLE_DUMP,
    "Connect out",  // COM_CONNECT_OUT,
    "Register slave",  // COM_REGISTER_SLAVE,

    "Prepare",  // COM_STMT_PREPARE,
    "Execute",  // COM_STMT_EXECUTE,
    "Stmt send long data",  // COM_STMT_SEND_LONG_DATA,
    "Close stmt",  // COM_STMT_CLOSE,

    "Stmt reset",  // COM_STMT_RESET,
    "Set option",  // COM_SET_OPTION,
    "Stmt fetch",  // COM_STMT_FETCH,
    "Daemno",  // COM_DAEMON,

    "Binlog dump gtid",  // COM_BINLOG_DUMP_GTID,
    "Reset connection",  // COM_RESET_CONNECTION,
    "End",  // COM_END,

    "Delete session", // COM_DELETE_SESSION
    "Handshake",  // COM_HANDSHAKE,
    "Login",  // COM_LOGIN,

    "Prexecute",  // COM_STMT_PREXECUTE,
    "Stmt send piece data",  // COM_STMT_SEND_PIECE_DATA,
    "Stmt get piece data" // COM_STMT_GET_PIECE_DATA,
  };

  if (mysql_cmd >= COM_SLEEP && mysql_cmd <= COM_END) {
    str = mysql_cmd_array[mysql_cmd];
  } else if (mysql_cmd > COM_END && mysql_cmd <= COM_LOGIN) {
    str = mysql_cmd_array[mysql_cmd + COM_END - OBPROXY_MYSQL_CMD_START + 1];
  } else if (mysql_cmd >= COM_STMT_PREXECUTE && mysql_cmd <= COM_STMT_GET_PIECE_DATA) {
    str = mysql_cmd_array[mysql_cmd + COM_END - PREXECUTE_CMD + 1];
  }
  return str;

}

} // end of namespace obmysql
} // end of namespace oceanbase
