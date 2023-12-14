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

#ifndef  _OB_2_0_PROTOCOL_UTILS_
#define  _OB_2_0_PROTOCOL_UTILS_
#include "io/easy_io.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_mod_define.h"
#include "common/object/ob_object.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/obp20_extra_info.h"
#include "rpc/obmysql/obsm_struct.h"

namespace oceanbase
{
namespace obmysql
{
class ObMySQLPacket;

enum ObProto20EncodeStep
{
  START_TO_FILL_STEP = 0,
  RESERVE_HEADER_STEP,
  FILL_PAYLOAD_STEP,
  FILL_TAILER_STEP,
  FILL_HEADER_STEP,
  FILL_DONE_STEP,
};

inline const char *get_proto20_encode_step_name(const ObProto20EncodeStep step)
{
  switch (step) {
    case START_TO_FILL_STEP:
      return "START_TO_FILL_STEP";
    case RESERVE_HEADER_STEP:
      return "RESERVE_HEADER_STEP";
    case FILL_PAYLOAD_STEP:
      return "FILL_PAYLOAD_STEP";
    case FILL_TAILER_STEP:
      return "FILL_TAILER_STEP";
    case FILL_HEADER_STEP:
      return "FILL_HEADER_STEP";
    case FILL_DONE_STEP:
      return "FILL_DONE_STEP";
    default:
      return "UNKNOWN_FILL_STEP";
  }
}

typedef obmysql::ObCommonKV<common::ObObj, common::ObObj> ObObjKV;
class ObProto20Context
{
public:
  ObProto20Context()
    : comp_seq_(0), proto20_seq_(0), request_id_(0), header_len_(0),
      tailer_len_(0), next_step_(START_TO_FILL_STEP),
      is_proto20_used_(false), is_checksum_off_(false),
      has_extra_info_(false), is_new_extra_info_(false),
      curr_proto20_packet_start_pos_(0), txn_free_route_(false),
      is_filename_packet_(false) {}
  ~ObProto20Context() {}

  inline void reset() { MEMSET(this, 0, sizeof(ObProto20Context)); }
  inline bool is_proto20_used() const { return is_proto20_used_; }
  inline bool txn_free_route() const { return txn_free_route_; }
   TO_STRING_KV(K_(comp_seq),
                K_(request_id),
                K_(proto20_seq),
                K_(header_len),
                K_(tailer_len),
                K_(next_step),
                K_(is_proto20_used),
                K_(is_checksum_off),
                K_(has_extra_info),
                K_(is_new_extra_info),
                K_(txn_free_route),
                K_(curr_proto20_packet_start_pos),
                K_(is_filename_packet));

public:
  uint8_t comp_seq_;
  uint8_t proto20_seq_;
  uint32_t request_id_;
  int64_t header_len_;
  int64_t tailer_len_;
  ObProto20EncodeStep next_step_;
  bool is_proto20_used_;
  bool is_checksum_off_;
  bool has_extra_info_;
  bool is_new_extra_info_;
  int64_t curr_proto20_packet_start_pos_;
  bool txn_free_route_;
  // used in local local.
  // We should set `is_filename_packet_` when sending PKT_FILENAME packet.
  bool is_filename_packet_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObProto20Context);
};

struct ObProtoEncodeParam
{
public:
  ObProto20Context *proto20_context_;
  rpc::ObRequest *req_;
  easy_buf_t *ez_buf_;
  ObMySQLPacket *pkt_;
  int64_t seri_size_;
  uint32_t conn_id_;
  int encode_ret_;
  bool need_flush_;
  bool is_last_;
  bool is_pkt_encoded_; // whether the pkt encoded into ze_buf_
  char *large_pkt_buf_; // when packet len >= 8MB, will tmp cache packet data
  int64_t large_pkt_buf_len_;
  int64_t large_pkt_buf_pos_;
  common::ObIArray<ObObjKV> *extra_info_kvs_;
  common::ObIArray<Obp20Encoder*> *extra_info_ecds_;
  observer::ObSMConnection* conn_;

  const static int64_t MAX_PROTO20_PAYLOAD_LEN;
  const static int64_t PROTO20_SPLIT_LEN;

public:
  ObProtoEncodeParam()
    : proto20_context_(NULL), req_(NULL), ez_buf_(NULL), pkt_(NULL),
      seri_size_(0), conn_id_(0), encode_ret_(common::OB_SUCCESS),
      need_flush_(false), is_last_(false), is_pkt_encoded_(false),
      large_pkt_buf_(NULL), large_pkt_buf_len_(0), large_pkt_buf_pos_(0),
      extra_info_kvs_(NULL), extra_info_ecds_(NULL), conn_(NULL)
  {}

  inline bool is_valid() const
  { return (NULL != proto20_context_) && (NULL != ez_buf_) && (NULL != req_) && (NULL != conn_); }

  inline static void build_param(ObProtoEncodeParam &param, ObMySQLPacket *pkt,
      easy_buf_t &ez_buf, const uint32_t sessid, const bool is_last,
      obmysql::ObProto20Context &proto20_context,
      rpc::ObRequest *req,
      common::ObIArray<ObObjKV> *extra_info,
      common::ObIArray<Obp20Encoder*> *extra_info_ecds) {
    param.proto20_context_ = &proto20_context;
    param.ez_buf_ = &ez_buf;
    param.pkt_ = pkt;
    param.is_last_ = is_last;
    param.conn_id_ = sessid;
    param.req_ = req;
    param.extra_info_kvs_ = extra_info;
    param.extra_info_ecds_ = extra_info_ecds;
    if (NULL != param.req_) {
      param.conn_ = reinterpret_cast<observer::ObSMConnection *>
                                (SQL_REQ_OP.get_sql_session(param.req_));
    }
  }

  inline int add_pos(const int64_t delta);
  inline int save_large_packet(const char *start, const int64_t len);
  inline int64_t get_remain_len() const { return large_pkt_buf_len_ - large_pkt_buf_pos_; }
  inline const char *get_start() const { return large_pkt_buf_ + large_pkt_buf_pos_; }

  inline bool is_large_packet_cached_avail() const {
    return (NULL != large_pkt_buf_) && (large_pkt_buf_len_ > large_pkt_buf_pos_);
  }

  inline bool is_large_packet_cached() const {
    return (NULL != large_pkt_buf_);
  }

  inline void reset() {
    if (NULL != large_pkt_buf_) {
      common::ob_free(large_pkt_buf_);
      large_pkt_buf_ = NULL;
    }
    large_pkt_buf_len_ = 0;
    large_pkt_buf_pos_ = 0;
  }

  ~ObProtoEncodeParam() { reset(); }
  TO_STRING_KV(KP_(proto20_context),
               KP_(req),
               KP_(ez_buf),
               KP_(pkt),
               K_(seri_size),
               K_(conn_id),
               K_(encode_ret),
               K_(need_flush),
               K_(is_last),
               K_(is_pkt_encoded),
               KP_(large_pkt_buf),
               K_(large_pkt_buf_len),
               K_(large_pkt_buf_pos),
               KP_(extra_info_kvs));
private:
  DISALLOW_COPY_AND_ASSIGN(ObProtoEncodeParam);
};

class ObProto20Utils
{
public:
  ObProto20Utils();
  virtual ~ObProto20Utils();

  static int fill_proto20_header_and_tailer(ObProtoEncodeParam &param);
  static int do_packet_encode(ObProtoEncodeParam &param);

private:
  inline static int do_proto20_packet_encode(ObProtoEncodeParam &param);
  inline static int encode_extra_info(char *buffer, int64_t length, int64_t &pos,
                                      common::ObIArray<ObObjKV> *extra_info);
  inline static int encode_new_extra_info(char *buffer, int64_t length, int64_t &pos,
                                          ObIArray<Obp20Encoder*> *extra_info);
  inline static int fill_proto20_payload(ObProtoEncodeParam &param, bool &is_break);
  inline static int fill_proto20_tailer(ObProtoEncodeParam &param);
  inline static int fill_proto20_header(ObProtoEncodeParam &param);
  inline static bool is_the_last_packet(const ObProtoEncodeParam &param);
  inline static bool has_extra_info(const ObProtoEncodeParam &param);
  static int reset_extra_info(ObProtoEncodeParam &param);
private:
  DISALLOW_COPY_AND_ASSIGN(ObProto20Utils);
};

} //end of namespace obmysql
} //end of namespace oceanbase

#endif /* _OB_2_0_PROTOCOL_UTILS_ */
