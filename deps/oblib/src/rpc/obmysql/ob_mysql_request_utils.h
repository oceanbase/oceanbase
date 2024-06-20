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

#ifndef  _OB_MYSQL_REQUEST
#define  _OB_MYSQL_REQUEST
#include "io/easy_io.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace common
{
class ObDataBuffer;
class ObArenaAllocator;
}
namespace rpc
{
class ObRequest;
}
namespace observer
{
class ObSMConnection;
}

namespace obmysql
{
class ObMySQLPacket;
class ObEasyBuffer;
class ObCompressionContext;

static const int64_t OB_PROXY_MAX_COMPRESSED_PACKET_LENGTH = (1L << 15); //32K
static const int64_t OB_MAX_COMPRESSED_PACKET_LENGTH = (1L << 20); //1M
static const int64_t MAX_COMPRESSED_BUF_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE;//2M-1k

class ObMysqlPktContext
{
public:
  enum ObMysqlPktReadStep {
    READ_HEADER = 0,
    READ_BODY,
    READ_COMPLETE
  };
   ObMysqlPktContext() : arena_("LibMultiPackets") { reset(); }
   ~ObMysqlPktContext() {}
  void reset()
  {
    static_assert(common::OB_MYSQL_HEADER_LENGTH == 4, "OB_MYSQL_HEADER_LENGTH != 4");
    *reinterpret_cast<uint32_t *>(header_buf_) = 0;
    header_buffered_len_ = 0;
    payload_buf_alloc_len_ = 0;
    payload_buf_ = NULL;
    payload_buffered_len_ = 0;
    payload_buffered_total_len_ = 0;
    payload_len_ = 0;
    last_pkt_seq_ = 0;
    curr_pkt_seq_ = 0;
    next_read_step_ = READ_HEADER;
    raw_pkt_.reset();
    is_multi_pkt_ = false;
    is_auth_switch_ = false;
    arena_.reset(); //fast free memory
  }

  int save_fragment_mysql_packet(const char *start, const int64_t len);

  static const char *get_read_step_str(const ObMysqlPktReadStep step)
  {
    switch (step) {
      case READ_HEADER:
        return "READ_HEADER";
      case READ_BODY:
        return "READ_BODY";
      case READ_COMPLETE:
        return "READ_COMPLETE";
      default:
        return "UNKNOWN";
    }
  }

  TO_STRING_KV(K_(header_buffered_len), K_(payload_buffered_len), K_(payload_buffered_total_len),
               K_(last_pkt_seq), K_(payload_len), K_(curr_pkt_seq), K_(payload_buf_alloc_len),
               "next_read_step", get_read_step_str(next_read_step_), K_(raw_pkt),
               "used", arena_.used(), "total", arena_.total(), K_(is_multi_pkt), K_(is_auth_switch));

public:
  char header_buf_[common::OB_MYSQL_HEADER_LENGTH];
  int64_t header_buffered_len_;
  char *payload_buf_;
  int64_t payload_buf_alloc_len_;
  int64_t payload_buffered_len_; // not include header
  int64_t payload_buffered_total_len_; // not include header
  int64_t payload_len_;
  uint8_t last_pkt_seq_;
  uint8_t curr_pkt_seq_;
  ObMysqlPktReadStep next_read_step_;
  ObMySQLRawPacket raw_pkt_;
  bool is_multi_pkt_;
  common::ObArenaAllocator arena_;
  bool is_auth_switch_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlPktContext);
};

class ObCompressedPktContext
{
public:
   ObCompressedPktContext() { reset(); }
   ~ObCompressedPktContext() { }
  void reset()
  {
    last_pkt_seq_ = 0;
    is_multi_pkt_ = false;
  }

  void reuse()
  {
    // keep the last_pkt_seq_ here
    is_multi_pkt_ = false;
  }

  TO_STRING_KV(K_(last_pkt_seq),
               K_(is_multi_pkt));

public:
  uint8_t last_pkt_seq_;
  bool is_multi_pkt_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCompressedPktContext);
};

class ObProto20PktContext
{
public:
  ObProto20PktContext() : arena_("LibMultiPackets"){ reset(); }
  ~ObProto20PktContext() { }
  void reset()
  {
    comp_last_pkt_seq_ = 0;
    is_multi_pkt_ = false;
    proto20_last_request_id_ = 0;
    proto20_last_pkt_seq_ = 0;
    extra_info_.reset();
    arena_.reset(); //fast free memory
  }

  TO_STRING_KV(K_(comp_last_pkt_seq),
               K_(is_multi_pkt),
               K_(proto20_last_request_id),
               K_(proto20_last_pkt_seq),
               K_(extra_info),
               "used", arena_.used(),
               "total", arena_.total());

public:
  uint8_t comp_last_pkt_seq_;
  bool is_multi_pkt_;
  uint32_t proto20_last_request_id_;
  uint8_t proto20_last_pkt_seq_;
  Ob20ExtraInfo extra_info_;
  common::ObArenaAllocator arena_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProto20PktContext);
};

class ObEasyBuffer
{
public:
  explicit ObEasyBuffer(easy_buf_t &ezbuf) : buf_(ezbuf), read_pos_(ezbuf.pos) { }
  ~ObEasyBuffer() {}
  int64_t read_avail_size() const { return buf_.last - read_pos_; }
  int64_t write_avail_size() const { return buf_.end - buf_.last; }
  int64_t proxy_read_avail_size(const char * const proxy_pos) const { return buf_.last - proxy_pos;}
  int64_t orig_data_size() const { return buf_.last - buf_.pos; }
  int64_t orig_buf_size() const { return buf_.end - buf_.pos; }
  bool is_valid() const { return (orig_buf_size() >= 0 && orig_data_size() >= 0); }
  bool is_read_avail() const { return buf_.last > read_pos_; }
  char *read_pos() const { return read_pos_; }
  char *begin() const { return buf_.pos; }
  char *last() const { return buf_.last; }
  char *end() const { return buf_.end; }
  void read(const int64_t size) { read_pos_ += size;}
  void write(const int64_t size) { buf_.last += size;}
  void fall_back(const int64_t size) { buf_.last -= size; }

  int64_t get_next_read_size(char *proxy_pos, const int64_t max_read_step)
  {
    int64_t ret = 0;
    bool is_last_proxy_pkt = false;
    if (NULL == proxy_pos) {
      ret = read_avail_size();
    } else if (proxy_pos <= read_pos()) {
      ret = read_avail_size();
      is_last_proxy_pkt = true;
    } else {
      ret = std::min(last(), proxy_pos) - read_pos();
    }

    if (!is_last_proxy_pkt && ret > max_read_step) {
      ret = max_read_step;
    }
    return ret;
  }

  TO_STRING_KV(KP_(read_pos), KP(buf_.pos), KP(buf_.last), KP(buf_.end),
               "orig_buf_size", orig_buf_size(),
               "orig_data_size", orig_data_size(),
               "read_avail_size", read_avail_size(),
               "write_avail_size", write_avail_size());

public:
  easy_buf_t &buf_;
  char *read_pos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObEasyBuffer);
};

enum ObCompressType
{
  NO_COMPRESS = 0,
  DEFAULT_COMPRESS, //compress the whole buf every 1M
  PROXY_COMPRESS,   //1. compress every 32K buf,
                    //2. put error+ok/eof+ok/ok in one compressed packet, and seq=last seq
  DEFAULT_CHECKSUM, //use level 0 compress based on DEFAULT_COMPRESS
  PROXY_CHECKSUM,   //use level 0 compress based on PROXY_COMPRESS
};

class ObCompressionContext
{
public:
  ObCompressionContext() { reset(); }
  ~ObCompressionContext() {}

  void reset() { memset(this, 0, sizeof(ObCompressionContext)); }
  bool use_compress() const { return NO_COMPRESS != type_; }
  bool use_uncompress() const { return NO_COMPRESS == type_; }
  bool is_proxy_compress() const { return PROXY_COMPRESS == type_; }
  bool is_default_compress() const { return DEFAULT_COMPRESS == type_; }
  bool is_default_checksum() const { return DEFAULT_CHECKSUM == type_; }
  bool is_proxy_checksum() const { return PROXY_CHECKSUM == type_; }
  bool is_proxy_compress_based() const { return is_proxy_checksum() || is_proxy_compress(); }
  bool use_checksum() const { return is_proxy_checksum() || is_default_checksum(); }
  void update_last_pkt_pos(char *pkt_pos)
  {
    if (is_proxy_compress_based() && NULL == last_pkt_pos_) {
      //if has updated, no need update again
      last_pkt_pos_ = pkt_pos;
    }
  }


  int64_t get_max_read_step() const
  {
    return (is_proxy_compress_based()
        ? OB_PROXY_MAX_COMPRESSED_PACKET_LENGTH
        : OB_MAX_COMPRESSED_PACKET_LENGTH);
  }

  bool need_hold_last_pkt(const bool is_last) const
  {
    //if error(eof) + ok can not in one buf, we need hold error(eof) packet for proxy
    return (is_proxy_compress_based()
            && NULL != last_pkt_pos_
            && !is_last );
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(sessid), K_(type), K_(is_checksum_off), K_(seq), KP_(last_pkt_pos));
    J_COMMA();
    if (NULL != send_buf_) {
      J_KV("send_buf", ObEasyBuffer(*send_buf_));
    } else {
      J_KV(KP_(send_buf));
    }
    J_OBJ_END();
    return pos;
  }

public:
  ObCompressType type_;
  bool is_checksum_off_;
  uint8_t seq_;//compressed pkt seq
  easy_buf_t *send_buf_;
  char *last_pkt_pos_;//proxy last pkt(error+ok, eof+ok, ok)'s pos in orig_ezbuf, default is null
  uint32_t sessid_;
  observer::ObSMConnection *conn_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCompressionContext);
};

class ObFlushBufferParam
{
public:
  ObFlushBufferParam(easy_buf_t &ez_buf, easy_request_t &ez_req, ObCompressionContext &context,
                     bool &conn_valid, bool &req_has_wokenup,
                     const bool pkt_has_completed)
    : orig_send_buf_(ez_buf), ez_req_(ez_req), comp_context_(context), conn_valid_(conn_valid),
      req_has_wokenup_(req_has_wokenup),
      pkt_has_completed_(pkt_has_completed)
    {}

public:
  ObEasyBuffer orig_send_buf_;
  easy_request_t &ez_req_;
  ObCompressionContext &comp_context_;
  bool &conn_valid_;
  bool &req_has_wokenup_;
  const bool pkt_has_completed_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFlushBufferParam);
};

class ObMySQLRequestUtils
{
public:
  ObMySQLRequestUtils();
  virtual ~ObMySQLRequestUtils();

  static int flush_buffer(ObFlushBufferParam &param);
  static int flush_compressed_buffer(bool pkt_has_completed, ObCompressionContext &comp_context, 
                                                  ObEasyBuffer &orig_send_buf, rpc::ObRequest &req);
private:
  static void disconnect(easy_request_t &ez_req);
  static void wakeup_easy_request(easy_request_t &ez_req, bool &req_has_wokenup);
  static int check_flush_param(ObFlushBufferParam &param);
  static int consume_compressed_buffer(ObFlushBufferParam &param,
                                       const bool flush_immediately = false);
  static int reuse_compressed_buffer(ObFlushBufferParam &param, int64_t comp_buf_size,
                                     const bool is_last_flush);

  //once last flushed, ObRequest may be destroyed
  static int flush_buffer_internal(easy_buf_t *send_buf, ObFlushBufferParam &param,
                                   const bool is_last_flush);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLRequestUtils);
};

extern void request_finish_callback();

} //end of namespace obmysql
} //end of namespace oceanbase


#endif
