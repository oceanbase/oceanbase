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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_BASE_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_BASE_

#include "lib/runtime.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/frame/ob_req_processor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace obrpc
{
extern int check_arb_white_list(int64_t cluster_id, bool& is_arb);
class ObRpcSessionHandler;
class ObRpcStreamCond;
class ObRpcProcessorBase : public rpc::frame::ObReqProcessor
{
public:
  static constexpr int64_t DEFAULT_WAIT_NEXT_PACKET_TIMEOUT = 30 * 1000 * 1000L;
public:
  ObRpcProcessorBase()
      : rpc_pkt_(NULL), sh_(NULL), sc_(NULL), is_stream_(false), is_stream_end_(false),
        require_rerouting_(false), preserve_recv_data_(false), preserved_buf_(NULL),
        uncompressed_buf_(NULL), using_buffer_(NULL), send_timestamp_(0), pkt_size_(0), tenant_id_(0),
        result_compress_type_(common::INVALID_COMPRESSOR)
  {}

  virtual ~ObRpcProcessorBase();
  void set_ob_request(rpc::ObRequest &req)
  {
    rpc::frame::ObReqProcessor::set_ob_request(req);
    rpc_pkt_ = &reinterpret_cast<const ObRpcPacket&>(req.get_packet());
    pkt_size_ = rpc_pkt_->get_clen();
    tenant_id_ = rpc_pkt_->get_tenant_id();
    send_timestamp_ = req.get_send_timestamp();
  }

  void set_session_handler(ObRpcSessionHandler &sh) { sh_ = &sh; }

  // timestamp of the packet
  int64_t get_send_timestamp() const
  { return send_timestamp_; }

  int64_t get_src_cluster_id() const
  {
    int64_t cluster_id = common::OB_INVALID_CLUSTER_ID;
    if (NULL != rpc_pkt_) {
      cluster_id = rpc_pkt_->get_src_cluster_id();
    }
    return cluster_id;
  }

  common::ObAddr get_peer() const;

  int run();
  virtual int process() = 0;
protected:
  int check_timeout() { return common::OB_SUCCESS; }
  virtual int check_cluster_id();
  int update_data_version();
  virtual int before_process() { return common::OB_SUCCESS; }
  virtual int after_process(int error_code);

  virtual int before_response(int error_code);

protected:
  struct Response {
    Response(int64_t sessid,
             bool is_stream,
             bool is_stream_last,
             bool require_rerouting,
             ObRpcPacket *pkt)
        : sessid_(sessid),
          is_stream_(is_stream),
          is_stream_last_(is_stream_last),
          require_rerouting_(require_rerouting),
          pkt_(pkt)
    { }

    // for stream options
    int64_t sessid_;
    bool is_stream_;
    bool is_stream_last_;

    // for routing check
    bool require_rerouting_;

    ObRpcPacket *pkt_;

    TO_STRING_KV(K_(sessid), K_(is_stream), K_(is_stream_last), K_(require_rerouting));
  };

  void reuse();
  virtual int deserialize();
  virtual int serialize();
  virtual int response(const int retcode) { return part_response(retcode, true); }
  virtual int flush(int64_t wait_timeout = 0, const ObAddr *src_addr = NULL);

  void set_preserve_recv_data() { preserve_recv_data_ = true; }
  void set_result_compress_type(common::ObCompressorType t) { result_compress_type_ = t; }
protected:
  int part_response(const int retcode, bool is_last);
  int part_response_error(rpc::ObRequest* req, const int retcode);
  int do_response(const Response &rsp);
  void compress_result(const char *src_buf, int64_t src_len,
                       char *dst_buf, int64_t dst_len, ObRpcPacket *pkt);
  int m_check_timeout()
  {
    int ret = common::OB_SUCCESS;
    if (NULL != req_ && NULL != rpc_pkt_) {
      const int64_t queue_time = common::ObClockGenerator::getClock() - req_->get_receive_timestamp();
      if (queue_time > rpc_pkt_->get_timeout()) {
        ret = common::OB_TIMEOUT;
        if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
          RPC_OBRPC_LOG(WARN, "rpc timeout when get out of queue",
                   K(ret), "packet", *rpc_pkt_, K(queue_time), "timeout", rpc_pkt_->get_timeout());
        }
      }
    }
    return ret;
  }
  virtual void cleanup();
protected:
  virtual int decode_base(const char *buf, const int64_t len, int64_t &pos) = 0;
  virtual int m_get_pcode() = 0;
  virtual int encode_base(char *buf, const int64_t len, int64_t &pos) = 0;
  virtual int64_t m_get_encoded_length() = 0;
protected:
  const ObRpcPacket *rpc_pkt_;
  ObRpcSessionHandler *sh_;
  ObRpcStreamCond *sc_;

  // mark if current request is in a stream.
  bool is_stream_;
  // If this request is a stream request, this mark means the stream
  // is end so that no need to response any packet back. When wait
  // client's next packet timeout, the req of this processor is
  // invalid, so the stream is end.
  bool is_stream_end_;

  // For rerouting in obkv
  bool require_rerouting_;

  // The flag marks received data must copy out from `easy buffer'
  // before we response packet back. Typical case is when we use
  // shadow copy when deserialize the argument but response before
  // process this argument.
  bool preserve_recv_data_;
  char *preserved_buf_;

  char *uncompressed_buf_;

  common::ObDataBuffer *using_buffer_;

  int64_t send_timestamp_;
  int64_t pkt_size_;
  int64_t tenant_id_;
  // compress the result if not INVALID_COMPRESSOR
  common::ObCompressorType result_compress_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcProcessorBase);
}; // end of class ObRpcProcessorBase
} // end of namespace observer
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_BASE_
