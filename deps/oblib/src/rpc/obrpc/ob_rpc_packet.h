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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PACKET_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PACKET_

#include "io/easy_io_struct.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/compress/ob_compressor.h"
#include "rpc/obrpc/ob_rpc_time.h"
#include "rpc/ob_packet.h"
#include "common/errsim_module/ob_errsim_module_type.h"

namespace oceanbase
{
namespace obrpc
{

#define OB_LOG_LEVEL_MASK (0x7)

enum ObRpcPriority
{
  ORPR_UNDEF = 0,
  ORPR1, ORPR2, ORPR3, ORPR4,
  ORPR5, ORPR6, ORPR7, ORPR8, ORPR9,
  ORPR_DDL = 10,
  ORPR11 = 11,
};

enum ObRpcPacketCode
{
  OB_INVALID_RPC_CODE = 0,

#define PCODE_DEF(name, id) name = id,
#include "rpc/obrpc/ob_rpc_packet_list.h"
#undef PCODE_DEF

  // don't use the packet code larger than or equal to
  // (PACKET_CODE_MASK - 1), because the first (32 - PACKET_CODE_BITS)
  // bits is used for special packet
  OB_PACKET_NUM,
};

class ObRpcPacketCodeChecker
{
public:
  ObRpcPacketCodeChecker() {}
  ~ObRpcPacketCodeChecker() {}
  static bool is_valid_pcode(const int32_t pcode)
  {
    return (pcode > OB_INVALID_RPC_CODE && pcode < OB_PACKET_NUM);
  }
};

class ObRpcPacketSet
{
  enum
  {
#define PCODE_DEF(name, id) name,
#include "rpc/obrpc/ob_rpc_packet_list.h"
#undef PCODE_DEF
    PCODE_COUNT
  };

  ObRpcPacketSet()
  {
#define PCODE_DEF(name, id)                     \
  names_[name] = #name;                         \
  labels_[name] = "[L]"#name;                   \
  pcode_[name] = obrpc::name;                   \
  index_[obrpc::name] = name;
#include "rpc/obrpc/ob_rpc_packet_list.h"
#undef PCODE_DEF
  }

public:
  int64_t idx_of_pcode(ObRpcPacketCode code) const
  {
    int64_t index = 0;
    if (code >= 0 && code < OB_PACKET_NUM) {
      index = index_[code];
    }
    return index;
  }

  const char *name_of_idx(int64_t idx) const
  {
    const char *name = "Unknown";
    if (idx >= 0 && idx < PCODE_COUNT) {
      name = names_[idx];
    }
    return name;
  }

  const char *label_of_idx(int64_t idx) const
  {
    const char *name = "Unknown";
    if (idx >= 0 && idx < PCODE_COUNT) {
      name = labels_[idx];
    }
    return name;
  }

  ObRpcPacketCode pcode_of_idx(int64_t idx) const
  {
    ObRpcPacketCode pcode = OB_INVALID_RPC_CODE;
    if (idx < PCODE_COUNT) {
      pcode = pcode_[idx];
    }
    return pcode;
  }

  static ObRpcPacketSet &instance()
  {
    return instance_;
  }

public:
  static const int64_t THE_PCODE_COUNT = PCODE_COUNT;

private:
  static ObRpcPacketSet instance_;

  const char *names_[PCODE_COUNT];
  const char *labels_[PCODE_COUNT];
  ObRpcPacketCode pcode_[PCODE_COUNT];
  int64_t index_[OB_PACKET_NUM];
};

class ObRpcPacketHeader
{
public:
#ifdef ERRSIM
  static const uint8_t  HEADER_SIZE = 144; // add 8 bit for errsim module
#else
  static const uint8_t  HEADER_SIZE = 136; // 112 -> 128: add 16 bytes for trace_id ipv6 extension. (Note yanyuan.cxf: but you should never change it)
#endif
  static const uint16_t RESP_FLAG              = 1 << 15;
  static const uint16_t STREAM_FLAG            = 1 << 14;
  static const uint16_t STREAM_LAST_FLAG       = 1 << 13;
  static const uint16_t DISABLE_DEBUGSYNC_FLAG = 1 << 12;
  static const uint16_t CONTEXT_FLAG           = 1 << 11;
  static const uint16_t UNNEED_RESPONSE_FLAG   = 1 << 10;
  static const uint16_t REQUIRE_REROUTING_FLAG = 1 << 9;
  static const uint16_t ENABLE_RATELIMIT_FLAG  = 1 << 8;
  static const uint16_t BACKGROUND_FLOW_FLAG   = 1 << 7;
  static const uint16_t TRACE_INFO_FLAG        = 1 << 6;
  static const uint16_t IS_KV_REQUEST_FALG     = 1 << 5;

  uint64_t checksum_;
  ObRpcPacketCode pcode_;
  uint8_t hlen_;
  uint8_t priority_;
  uint16_t flags_;
  uint64_t tenant_id_;
  uint64_t priv_tenant_id_;
  uint64_t session_id_;
  uint64_t trace_id_[4];  // 128 bits trace id change to 256 bits
  uint64_t timeout_;
  int64_t timestamp_;
  ObRpcCostTime cost_time_;
  int64_t dst_cluster_id_;

  common::ObCompressorType compressor_type_; // enum ObCompressorType
  int32_t original_len_;
  int64_t src_cluster_id_;
  uint64_t unis_version_;
  int32_t request_level_;
  int64_t seq_no_;
  int32_t group_id_;
  uint64_t cluster_name_hash_;
  uint64_t data_version_;

#ifdef ERRSIM
  ObErrsimModuleType module_type_;
#endif

  int serialize(char* buf, const int64_t buf_len, int64_t& pos);
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);

  TO_STRING_KV(K(checksum_), K(pcode_), K(hlen_), K(priority_),
               K(flags_), K(tenant_id_), K(priv_tenant_id_), K(session_id_),
               K(trace_id_), K(timeout_), K_(timestamp), K_(dst_cluster_id), K_(cost_time),
               K(compressor_type_), K(original_len_), K(src_cluster_id_), K(seq_no_),
               K(data_version_));

  ObRpcPacketHeader() { memset(this, 0, sizeof(*this)); flags_ |= (OB_LOG_LEVEL_NONE & OB_LOG_LEVEL_MASK); }
  static inline int64_t get_encoded_size()
  {
    return HEADER_SIZE + ObRpcCostTime::get_encoded_size() + 8 /* for seq no */ + 8 /* for data version*/;
  }

};

class ObRpcPacket
    : public rpc::ObPacket
{
  friend class ObPacketQueue;

public:
  static uint32_t global_chid;
  static uint64_t INVALID_CLUSTER_NAME_HASH;

public:
  ObRpcPacket();
  virtual ~ObRpcPacket();

  inline void set_checksum(uint64_t checksum);
  inline uint64_t get_checksum() const;
  inline void calc_checksum();
  inline int verify_checksum() const;

  inline uint32_t get_chid() const;
  inline void set_chid(const uint32_t chid);
  inline void set_packet_id(const uint64_t packet_id);
  inline void set_request_arrival_time(const int64_t timestamp);

  inline void set_arrival_push_diff(const int32_t diff);
  inline void set_push_pop_diff(const int32_t diff);
  inline void set_pop_process_start_diff(const int32_t diff);
  inline void set_process_start_end_diff(const int32_t diff);
  inline void set_process_end_response_diff(const int32_t diff);

  inline uint64_t get_packet_id() const;
  inline int64_t get_request_arrival_time() const;

  inline int32_t get_arrival_push_diff() const;
  inline int32_t get_push_pop_diff() const;
  inline int32_t get_pop_process_start_diff() const;
  inline int32_t get_process_start_end_diff() const;
  inline int32_t get_process_end_response_diff() const;

  inline int64_t get_request_push_time() const;
  inline int64_t get_request_pop_time() const;
  inline int64_t get_request_process_start_time() const;
  inline int64_t get_request_process_end_time() const;
  inline int64_t get_server_response_time() const;

  inline void set_content(const char *content, int64_t len);
  inline const char *get_cdata() const;
  inline uint32_t get_clen() const;

  inline int decode(const char *buf, int64_t len);
  inline int encode(char *buf, int64_t len, int64_t &pos);
  inline int encode_header(char *buf, int64_t len, int64_t &pos);
  inline int64_t get_encoded_size() const;

  inline void set_resp();
  inline void unset_resp();
  inline bool is_resp() const;

  inline bool is_stream() const;
  inline bool is_stream_next() const;
  inline bool is_stream_last() const;
  inline bool has_context() const;
  inline bool has_disable_debugsync() const;
  inline bool has_trace_info() const;
  inline void set_stream_next();
  inline void set_stream_last();
  inline void set_has_context();
  inline void set_disable_debugsync();
  inline void set_has_trace_info();
  inline void unset_stream();
  inline void set_unneed_response();
  inline bool unneed_response() const;
  inline void set_require_rerouting();
  inline bool require_rerouting() const;
  inline bool is_kv_request() const;
  inline void set_kv_request();

  inline bool ratelimit_enabled() const;
  inline void enable_ratelimit();
  inline bool is_background_flow() const;
  inline void set_background_flow();

  inline void set_packet_len(int length);

  inline void set_no_free();
  inline int32_t get_packet_stream_flag() const;
  inline ObRpcPacketCode get_pcode() const;
  inline void set_pcode(ObRpcPacketCode packet_code);

  inline int64_t get_session_id() const;
  inline void set_session_id(const int64_t session_id);

  inline void set_timeout(int64_t timeout) { hdr_.timeout_ = timeout; }
  inline int64_t get_timeout() const { return hdr_.timeout_; }

  inline void set_receive_ts(const int64_t receive_ts) {receive_ts_ = receive_ts;}
  inline int64_t get_receive_ts() const { return receive_ts_;}

  inline void set_priority(uint8_t priority);
  inline uint8_t get_priority() const;

  inline void set_trace_id(const uint64_t *trace_id);
  inline const uint64_t *get_trace_id() const;
  inline int8_t get_log_level() const;
  inline void set_log_level(const int8_t level);

  inline void set_tenant_id(const int64_t tenant_id);
  inline int64_t get_tenant_id() const;

  inline void set_priv_tenant_id(const int64_t tenant_id);
  inline int64_t get_priv_tenant_id() const;

  inline void set_timestamp(const int64_t timestamp);
  inline int64_t get_timestamp() const;

  inline void set_dst_cluster_id(const int64_t dst_cluster_id);
  inline int64_t get_dst_cluster_id() const;

  inline void set_cluster_name_hash(const uint64_t cluster_name_hash);
  inline uint64_t get_cluster_name_hash() const;

  inline uint64_t get_packet_len() const;

  inline void set_compressor_type(const common::ObCompressorType &compessor_type);
  inline enum common::ObCompressorType get_compressor_type() const;

  inline void set_original_len(const int32_t original_len);
  inline int32_t get_original_len() const;

  inline void set_src_cluster_id(const int64_t src_cluster_id);
  inline int64_t get_src_cluster_id() const;

  inline void set_unis_version(uint64_t version);
  inline uint64_t get_unis_version() const;

  inline void set_request_level(const int32_t level);
  inline int32_t get_request_level() const;
  inline void set_group_id(int32_t group_id);
  inline int32_t get_group_id() const;
  inline uint64_t get_data_version() const;

  int encode_ez_header(char *buf, int64_t len, int64_t &pos);
  TO_STRING_KV(K(hdr_), K(chid_), K(clen_), K_(assemble), K_(msg_count), K_(payload));

  static inline int64_t get_header_size()
  {
    return ObRpcPacketHeader::get_encoded_size();
  }
  static uint64_t get_self_cluster_name_hash();

#ifdef ERRSIM
  inline void set_module_type(const ObErrsimModuleType &module_type);
  inline const ObErrsimModuleType get_module_type() const;
#endif

private:
  ObRpcPacketHeader hdr_;
  const char *cdata_;
  uint32_t clen_;
  uint32_t chid_;         // channel id
  int64_t receive_ts_;  // do not serialize it
public:
  // for assemble
  bool assemble_;
  int32_t msg_count_;
  int64_t payload_;
  easy_list_t list_;
  static const uint8_t API_VERSION = 1;
  static const uint8_t MAGIC_HEADER_FLAG[4];
  static const uint8_t MAGIC_COMPRESS_HEADER_FLAG[4];
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcPacket);
};

void ObRpcPacket::set_checksum(uint64_t checksum)
{
  hdr_.checksum_ = checksum;
}

uint64_t ObRpcPacket::get_checksum() const
{
  return hdr_.checksum_;
}

void ObRpcPacket::calc_checksum()
{
  hdr_.checksum_ = common::ob_crc64(cdata_, clen_);
}

int ObRpcPacket::verify_checksum() const
{
  return hdr_.checksum_ == common::ob_crc64(cdata_, clen_) ?
      common::OB_SUCCESS :
      common::OB_CHECKSUM_ERROR;
}

void ObRpcPacket::set_content(const char *content, int64_t len)
{
  cdata_ = content;
  clen_ = static_cast<uint32_t>(len);
}

const char *ObRpcPacket::get_cdata() const
{
  return cdata_;
}

uint32_t ObRpcPacket::get_clen() const
{
  return clen_;
}

int ObRpcPacket::decode(const char *buf, int64_t len)
{
  int ret = common::OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(hdr_.deserialize(buf, len, pos))) {
    RPC_OBRPC_LOG(ERROR, "decode rpc packet header fail",
        K(ret), K(len), K(pos));
  } else if (len < hdr_.hlen_) {
    ret = common::OB_RPC_PACKET_INVALID;
    RPC_OBRPC_LOG(
        WARN,
        "rpc packet invalid", K_(hdr), K(len));
  } else {
    cdata_ = buf + hdr_.hlen_;
    clen_ = static_cast<uint32_t>(len - hdr_.hlen_);
  }
  return ret;
}

int ObRpcPacket::encode(char *buf, int64_t len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(hdr_.serialize(buf, len, pos))) {
    COMMON_LOG(WARN, "serialize ob packet header fail");
  } else if (clen_ > len - pos) {
    // buffer no enough to serialize packet
    ret = common::OB_BUF_NOT_ENOUGH;
  } else if (clen_ > 0) {
    MEMCPY(buf + pos, cdata_, clen_);
    pos += clen_;
  }
  return ret;
}

int ObRpcPacket::encode_header(char *buf, int64_t len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(hdr_.serialize(buf, len, pos))) {
    COMMON_LOG(WARN, "serialize ob packet header fail");
  }
  return ret;
}

void ObRpcPacket::set_resp()
{
  hdr_.flags_ |= ObRpcPacketHeader::RESP_FLAG;
}

void ObRpcPacket::unset_resp()
{
  hdr_.flags_ &= static_cast<uint16_t>(~ObRpcPacketHeader::RESP_FLAG);
}

bool ObRpcPacket::is_resp() const
{
  return hdr_.flags_ & ObRpcPacketHeader::RESP_FLAG;
}

bool ObRpcPacket::is_stream() const
{
  return hdr_.flags_ & ObRpcPacketHeader::STREAM_FLAG;
}

bool ObRpcPacket::is_stream_next() const
{
  return is_stream() && !(hdr_.flags_ & ObRpcPacketHeader::STREAM_LAST_FLAG);
}

bool ObRpcPacket::is_stream_last() const
{
  return is_stream() && (hdr_.flags_ & ObRpcPacketHeader::STREAM_LAST_FLAG);
}

bool ObRpcPacket::has_disable_debugsync() const
{
  return hdr_.flags_ & ObRpcPacketHeader::DISABLE_DEBUGSYNC_FLAG;
}

bool ObRpcPacket::has_context() const
{
  return hdr_.flags_ & ObRpcPacketHeader::CONTEXT_FLAG;
}

bool ObRpcPacket::has_trace_info() const
{
  return hdr_.flags_ & ObRpcPacketHeader::TRACE_INFO_FLAG;
}

bool ObRpcPacket::is_kv_request() const
{
  return hdr_.flags_ & ObRpcPacketHeader::IS_KV_REQUEST_FALG;
}

void ObRpcPacket::set_kv_request()
{
  hdr_.flags_ |= ObRpcPacketHeader::IS_KV_REQUEST_FALG;
}

void ObRpcPacket::set_stream_next()
{
  hdr_.flags_ &= static_cast<uint16_t>(~ObRpcPacketHeader::STREAM_LAST_FLAG);
  hdr_.flags_ |= ObRpcPacketHeader::STREAM_FLAG;
}

void ObRpcPacket::set_stream_last()
{
  hdr_.flags_ |= ObRpcPacketHeader::STREAM_LAST_FLAG;
  hdr_.flags_ |= ObRpcPacketHeader::STREAM_FLAG;
}

void ObRpcPacket::set_has_context()
{
  hdr_.flags_ |= ObRpcPacketHeader::CONTEXT_FLAG;
}

void ObRpcPacket::set_disable_debugsync()
{
  hdr_.flags_ |= ObRpcPacketHeader::DISABLE_DEBUGSYNC_FLAG;
}

void ObRpcPacket::set_has_trace_info()
{
  hdr_.flags_ |= ObRpcPacketHeader::TRACE_INFO_FLAG;
}

void ObRpcPacket::unset_stream()
{
  hdr_.flags_ &= static_cast<uint16_t>(~ObRpcPacketHeader::STREAM_FLAG);
}

void ObRpcPacket::set_unneed_response()
{
  hdr_.flags_ |= ObRpcPacketHeader::UNNEED_RESPONSE_FLAG;
}

bool ObRpcPacket::unneed_response() const
{
  return hdr_.flags_ & ObRpcPacketHeader::UNNEED_RESPONSE_FLAG;
}

void ObRpcPacket::set_require_rerouting()
{
  hdr_.flags_ |= ObRpcPacketHeader::REQUIRE_REROUTING_FLAG;
}

bool ObRpcPacket::require_rerouting() const
{
  return hdr_.flags_ & ObRpcPacketHeader::REQUIRE_REROUTING_FLAG;
}

bool ObRpcPacket::ratelimit_enabled() const
{
  return hdr_.flags_ & ObRpcPacketHeader::ENABLE_RATELIMIT_FLAG;
}

void ObRpcPacket::enable_ratelimit()
{
  hdr_.flags_ |= ObRpcPacketHeader::ENABLE_RATELIMIT_FLAG;
}

bool ObRpcPacket::is_background_flow() const
{
  return hdr_.flags_ & ObRpcPacketHeader::BACKGROUND_FLOW_FLAG;
}

void ObRpcPacket::set_background_flow()
{
  hdr_.flags_ |= ObRpcPacketHeader::BACKGROUND_FLOW_FLAG;
}

int64_t ObRpcPacket::get_encoded_size() const
{
  int64_t size = 0;
  if (assemble_) {
    size = payload_ + hdr_.get_encoded_size();
  } else {
    size = clen_ + hdr_.get_encoded_size();
  }
  return size;
}

uint32_t ObRpcPacket::get_chid() const
{
  return chid_;
}

void ObRpcPacket::set_chid(const uint32_t chid)
{
  chid_ = chid;
}

void ObRpcPacket::set_packet_id(const uint64_t packet_id)
{
  hdr_.cost_time_.packet_id_ = packet_id;
}

void ObRpcPacket::set_request_arrival_time(const int64_t timestamp)
{
  hdr_.cost_time_.request_arrival_time_ = timestamp;
}

void ObRpcPacket::set_arrival_push_diff(const int32_t diff)
{
  hdr_.cost_time_.arrival_push_diff_ = diff;
}

void ObRpcPacket::set_push_pop_diff(const int32_t diff)
{
  hdr_.cost_time_.push_pop_diff_ = diff;
}

void ObRpcPacket::set_pop_process_start_diff(const int32_t diff)
{
  hdr_.cost_time_.pop_process_start_diff_ = diff ;
}

void ObRpcPacket::set_process_start_end_diff(const int32_t diff)
{
  hdr_.cost_time_.process_start_end_diff_ = diff;
}

void ObRpcPacket::set_process_end_response_diff(const int32_t diff)
{
  hdr_.cost_time_.process_end_response_diff_ = diff;
}

uint64_t ObRpcPacket::get_packet_id() const
{
  return hdr_.cost_time_.packet_id_;
}

int64_t ObRpcPacket::get_request_arrival_time() const
{
  return hdr_.cost_time_.request_arrival_time_;
}

int32_t ObRpcPacket::get_arrival_push_diff() const
{
  return hdr_.cost_time_.arrival_push_diff_;
}

int32_t ObRpcPacket::get_push_pop_diff() const
{
  return hdr_.cost_time_.push_pop_diff_;
}

int32_t ObRpcPacket::get_pop_process_start_diff() const
{
  return hdr_.cost_time_.pop_process_start_diff_;
}

int32_t ObRpcPacket::get_process_start_end_diff() const
{
  return hdr_.cost_time_.process_start_end_diff_;
}

int32_t ObRpcPacket::get_process_end_response_diff() const
{
  return hdr_.cost_time_.process_end_response_diff_;
}

ObRpcPacketCode ObRpcPacket::get_pcode() const
{
  return hdr_.pcode_;
}

void ObRpcPacket::set_pcode(ObRpcPacketCode pcode)
{
  hdr_.pcode_ = pcode;
}

int64_t ObRpcPacket::get_session_id() const
{
  return hdr_.session_id_;
}

void ObRpcPacket::set_session_id(const int64_t session_id)
{
  hdr_.session_id_ = session_id;
}

void ObRpcPacket::set_priority(uint8_t priority)
{
  hdr_.priority_ = priority;
}

uint8_t ObRpcPacket::get_priority() const
{
  return hdr_.priority_;
}

const uint64_t *ObRpcPacket::get_trace_id() const
{
  return hdr_.trace_id_;
}

int8_t ObRpcPacket::get_log_level() const
{
  return hdr_.flags_ & OB_LOG_LEVEL_MASK;
}

void ObRpcPacket::set_trace_id(const uint64_t *trace_id)
{
  if (trace_id != NULL) {
    hdr_.trace_id_[0] = trace_id[0];
    hdr_.trace_id_[1] = trace_id[1];
    hdr_.trace_id_[2] = trace_id[2];
    hdr_.trace_id_[3] = trace_id[3];
  }
}

void ObRpcPacket::set_log_level(const int8_t log_level)
{
  hdr_.flags_ &= static_cast<uint16_t>(~OB_LOG_LEVEL_MASK);   //must set zero first
  hdr_.flags_ |= static_cast<uint16_t>(log_level);
}

void ObRpcPacket::set_tenant_id(const int64_t tenant_id)
{
  hdr_.tenant_id_ = tenant_id;
}

int64_t ObRpcPacket::get_tenant_id() const
{
  return hdr_.tenant_id_;
}

void ObRpcPacket::set_priv_tenant_id(const int64_t tenant_id)
{
  hdr_.priv_tenant_id_ = tenant_id;
}

int64_t ObRpcPacket::get_priv_tenant_id() const
{
  return hdr_.priv_tenant_id_;
}

void ObRpcPacket::set_timestamp(const int64_t timestamp)
{
  hdr_.timestamp_ = timestamp;
}

int64_t ObRpcPacket::get_timestamp() const
{
  return hdr_.timestamp_;
}

void ObRpcPacket::set_dst_cluster_id(const int64_t dst_cluster_id)
{
  hdr_.dst_cluster_id_ = dst_cluster_id;
}

int64_t ObRpcPacket::get_dst_cluster_id() const
{
  return hdr_.dst_cluster_id_;
}

void ObRpcPacket::set_cluster_name_hash(const uint64_t cluster_name_hash)
{
  hdr_.cluster_name_hash_ = cluster_name_hash;
}

uint64_t ObRpcPacket::get_cluster_name_hash() const
{
  return hdr_.cluster_name_hash_;
}

void ObRpcPacket::set_compressor_type(const common::ObCompressorType &compressor_type)
{
  hdr_.compressor_type_ = compressor_type;
}

enum common::ObCompressorType ObRpcPacket::get_compressor_type() const
{
  return hdr_.compressor_type_;
}

void ObRpcPacket::set_original_len(const int32_t original_len)
{
  hdr_.original_len_ = original_len;
}

int32_t ObRpcPacket::get_original_len() const
{
  return hdr_.original_len_;
}

void ObRpcPacket::set_src_cluster_id(const int64_t src_cluster_id)
{
  hdr_.src_cluster_id_ = src_cluster_id;
}

int64_t ObRpcPacket::get_src_cluster_id() const
{
  return hdr_.src_cluster_id_;
}

void ObRpcPacket::set_unis_version(uint64_t version)
{
  hdr_.unis_version_ = version;
}

uint64_t ObRpcPacket::get_unis_version() const
{
  return hdr_.unis_version_;
}

void ObRpcPacket::set_request_level(const int32_t level)
{
  hdr_.request_level_ = level;
}
int32_t ObRpcPacket::get_request_level() const
{
  return hdr_.request_level_;
}

void ObRpcPacket::set_group_id(int32_t group_id)
{
  hdr_.group_id_ = group_id;
}

int32_t ObRpcPacket::get_group_id() const
{
  return hdr_.group_id_;
}

uint64_t ObRpcPacket::get_data_version() const
{
  return hdr_.data_version_;
}

RLOCAL_EXTERN(int, g_pcode);
inline ObRpcPacketCode current_pcode()
{
  const int val = g_pcode;
  return static_cast<ObRpcPacketCode>(val);
}

inline void set_current_pcode(const ObRpcPacketCode pcode)
{
  g_pcode = pcode;
}

enum class ObRpcCheckSumCheckLevel
{
  INVALID,
  FORCE,
  OPTIONAL,
  DISABLE
};

extern ObRpcCheckSumCheckLevel g_rpc_checksum_check_level;

inline void set_rpc_checksum_check_level(
  const ObRpcCheckSumCheckLevel rpc_checksum_check_level)
{
  g_rpc_checksum_check_level = rpc_checksum_check_level;
}

inline ObRpcCheckSumCheckLevel get_rpc_checksum_check_level()
{
  return g_rpc_checksum_check_level;
}

inline ObRpcCheckSumCheckLevel get_rpc_checksum_check_level_from_string(
  const common::ObString &string)
{
  ObRpcCheckSumCheckLevel ret_type = ObRpcCheckSumCheckLevel::INVALID;
  if (0 == string.case_compare("Force")) {
    ret_type = ObRpcCheckSumCheckLevel::FORCE;
  } else if (0 == string.case_compare("Optional")) {
    ret_type = ObRpcCheckSumCheckLevel::OPTIONAL;
  } else if (0 == string.case_compare("Disable")) {
    ret_type = ObRpcCheckSumCheckLevel::DISABLE;
  }
  return ret_type;
}

#ifdef ERRSIM
void ObRpcPacket::set_module_type(const ObErrsimModuleType &module_type)
{
  hdr_.module_type_ = module_type;
}

const ObErrsimModuleType ObRpcPacket::get_module_type() const
{
  return hdr_.module_type_;
}
#endif

} // end of namespace rpc
} // end of namespace oceanbase

#endif // OCEANBASE_RPC_OBRPC_OB_RPC_PACKET_
