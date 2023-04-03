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

#ifndef OCEANBASE_MYSQL_OBMP_PACKET_SENDER_H_
#define OCEANBASE_MYSQL_OBMP_PACKET_SENDER_H_
#include "observer/ob_server_struct.h"
#include "rpc/obmysql/obsm_struct.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"
#include "rpc/obmysql/obp20_extra_info.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace obmysql
{
class obmysqlpacket;
struct ObProtoEncodeParam;
class Obp20Encoder;
} // end of namespace obmysql

namespace share
{
class ObFeedbackRerouteInfo;
}

namespace observer
{
class ObMySQLResultSet;
struct ObSMConnection;

struct ObOKPParam
{
  ObOKPParam()
    : affected_rows_(0),
      message_(NULL),
      is_on_connect_(false),
      is_on_change_user_(false),
      is_partition_hit_(true),
      has_more_result_(false),
      take_trace_id_to_client_(false),
      warnings_count_(0),
      cursor_exist_(false),
      send_last_row_(false),
      has_pl_out_(false),
      lii_(0),
      reroute_info_(NULL)
  { }
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  uint64_t affected_rows_;
  char *message_;
  bool is_on_connect_;
  bool is_on_change_user_;
  bool is_partition_hit_;
  bool has_more_result_;
  // current, when return error packet or query fly time >= trace_log_slow_query_watermark(100ms default),
  // will take trace id to client for easy debugging;
  bool take_trace_id_to_client_;
  uint16_t warnings_count_;
  bool cursor_exist_;
  bool send_last_row_;
  bool has_pl_out_;
  // According to mysql behavior, when last_insert_id was changed by insert,
  // update or replace, no matter auto generated or manual specified, mysql
  // should send last_insert_id through ok packet to mysql client.
  // And if last_insert_id was changed by set @@last_insert_id=xxx, mysql
  // will not send changed last_insert_id to client.
  //
  // if last insert id has changed, send lii to client;
  // Attention, in this case, lii is in ok packet's last insert id field
  // (encode by int<lenenc>), not in session track field;
  uint64_t lii_;

  share::ObFeedbackRerouteInfo *reroute_info_;
};

class ObIMPPacketSender
{
public:
  virtual ~ObIMPPacketSender() { }
  virtual void disconnect() = 0;
  virtual void force_disconnect() = 0;
  virtual int update_last_pkt_pos() = 0;
  virtual int response_packet(obmysql::ObMySQLPacket &pkt, sql::ObSQLSessionInfo* session) = 0;
  virtual int send_error_packet(int err,
                                const char* errmsg,
                                bool is_partition_hit = true,
                                void *extra_err_info = NULL) = 0;
  virtual int send_ok_packet(sql::ObSQLSessionInfo &session, ObOKPParam &ok_param, obmysql::ObMySQLPacket* pkt=NULL) = 0;
  virtual int send_eof_packet(const sql::ObSQLSessionInfo &session,
                              const ObMySQLResultSet &result,
                              ObOKPParam *ok_param = NULL) = 0;
  virtual bool need_send_extra_ok_packet() = 0;
  virtual int flush_buffer(const bool is_last) = 0;
  virtual ObSMConnection* get_conn() const = 0;
};

class ObMPPacketSender : public ObIMPPacketSender
{
public:
  ObMPPacketSender();
  virtual ~ObMPPacketSender();

  void reset();
  virtual void disconnect() override;
  virtual void force_disconnect() override;
  virtual int update_last_pkt_pos() override;
  virtual int response_packet(obmysql::ObMySQLPacket &pkt, sql::ObSQLSessionInfo* session) override;
  // when connect with proxy, need to append extra ok packet to last statu packet
  int response_compose_packet(obmysql::ObMySQLPacket &pkt,
                              obmysql::ObMySQLPacket &okp,
                              sql::ObSQLSessionInfo* session,
                              bool update_comp_pos);
  virtual int send_error_packet(int err,
                                const char* errmsg,
                                bool is_partition_hit = true,
                                void *extra_err_info = NULL) override;
  virtual int send_ok_packet(sql::ObSQLSessionInfo &session, ObOKPParam &ok_param, obmysql::ObMySQLPacket* pkt=NULL) override;
  virtual int send_eof_packet(const sql::ObSQLSessionInfo &session,
                              const ObMySQLResultSet &result,
                              ObOKPParam *ok_param = NULL) override;
  virtual bool need_send_extra_ok_packet() override
  { return OB_NOT_NULL(get_conn()) && get_conn()->need_send_extra_ok_packet(); }
  virtual int flush_buffer(const bool is_last);
  int clone_from(ObMPPacketSender& that);
  int init(rpc::ObRequest* req);
  int do_init(rpc::ObRequest *req,
           uint8_t packet_seq,
           bool conn_status,
           bool req_has_wokenup,
           int64_t query_receive_ts);
  bool is_conn_valid() const { return conn_valid_; }
  int update_transmission_checksum_flag(const sql::ObSQLSessionInfo &session);
  void finish_sql_request();
  int get_conn_id(uint32_t &sessid) const;
  ObSMConnection* get_conn() const;
  int get_session(sql::ObSQLSessionInfo *&sess_info);
  int revert_session(sql::ObSQLSessionInfo *sess_info);
  void disable_response() { req_has_wokenup_ = true; }
  bool is_disable_response() const { return req_has_wokenup_; }
  int clean_buffer();
  bool has_pl();
  int alloc_ezbuf();

private:
  static const int64_t MAX_TRY_STEPS = 8;
  static int64_t TRY_EZ_BUF_SIZES[MAX_TRY_STEPS];

  int try_encode_with(obmysql::ObMySQLPacket &pkt,
                      int64_t current_size,
                      int64_t &seri_size,
                      int64_t try_steps);
  int build_encode_param_(obmysql::ObProtoEncodeParam &param,
                          obmysql::ObMySQLPacket *pkt,
                          const bool is_last);
  bool need_flush_buffer() const;
  int resize_ezbuf(const int64_t size);
protected:
  rpc::ObRequest *req_;
  uint8_t seq_;
  obmysql::ObCompressionContext comp_context_;
  obmysql::ObProto20Context proto20_context_;
  easy_buf_t *ez_buf_;
  bool conn_valid_;
  uint32_t sessid_;
  bool req_has_wokenup_;
  int64_t query_receive_ts_;
  int nio_protocol_;
  ObSMConnection *conn_;
  common::ObSEArray<obmysql::ObObjKV, 4> extra_info_kvs_;
  common::ObSEArray<obmysql::Obp20Encoder*, 4> extra_info_ecds_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMPPacketSender);
};

}; // end namespace observer
}; // end namespace oceanbase

#endif /* OCEANBASE_MYSQL_OBMP_PACKET_SENDER_H_ */

