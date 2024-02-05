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

#ifndef OB_DTL_CHANNEL_AGENT_H_
#define OB_DTL_CHANNEL_AGENT_H_

#include "ob_dtl_buf_allocator.h"
#include "sql/dtl/ob_dtl_msg.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "sql/dtl/ob_dtl_local_channel.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_cluster_version.h"

namespace oceanbase {
namespace common {
class ObNewRow;
}
namespace sql {
namespace dtl {

class ObDtlRpcChannel;
class ObDtlLocalChannel;

class ObDtlBufEncoder
{
public:
  ObDtlBufEncoder()
  : use_row_store_(false),
    tenant_id_(500),
    buffer_(nullptr),
    msg_writer_(nullptr)
  {}
  ~ObDtlBufEncoder() {}
  void set_tenant_id(int64_t tenant_id) {
    tenant_id_ = tenant_id;
    use_row_store_ = true;
  }
  int switch_writer(const ObDtlMsg &msg);
  int need_new_buffer(
    const ObDtlMsg &msg, ObEvalCtx *eval_ctx, int64_t &need_size, bool &need_new);
  int write_data_msg(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, bool is_eof);
  int set_new_buffer(ObDtlLinkedBuffer *buffer) {
    buffer_ = buffer;
    return msg_writer_->init(buffer_, tenant_id_);
  }
  void reset_writer()
  {
    msg_writer_->reset();
  }
  int serialize() {
    int ret = OB_SUCCESS;
    if (CHUNK_DATUM_WRITER != msg_writer_->type()) {
      ret = msg_writer_->serialize();
    }
    if (OB_SUCC(ret)) {
      buffer_->pos() = msg_writer_->used();
    }
    return ret;
  }
  void write_msg_type(ObDtlLinkedBuffer* buffer)
  { msg_writer_->write_msg_type(buffer); }
  ObDtlLinkedBuffer *get_buffer() { return buffer_; }
private:
  int64_t use_row_store_;
  int64_t tenant_id_;
  ObDtlLinkedBuffer *buffer_;
  ObDtlControlMsgWriter ctl_msg_writer_;
  ObDtlRowMsgWriter row_msg_writer_;
  ObDtlDatumMsgWriter datum_msg_writer_;
  ObDtlVectorRowMsgWriter vector_row_msg_writer_;
  ObDtlVectorMsgWriter vector_msg_writer_;
  ObDtlVectorFixedMsgWriter vector_fixed_msg_writer_;
  ObDtlChannelEncoder *msg_writer_;
};

class ObDtlBcastService
{
public:
  ObDtlBcastService() : server_addr_(), bcast_buf_(nullptr), send_count_(0), bcast_ch_count_(0),
 ch_infos_(), resps_(), peer_ids_(), active_chs_count_(0) {}
  virtual ~ObDtlBcastService() {}
  int send_message(ObDtlLinkedBuffer *&bcast_buf, bool drain);
  void set_bcast_ch_count(int64_t ch_count) { bcast_ch_count_ = ch_count; }
  TO_STRING_KV(K_(server_addr), K_(bcast_ch_count), K_(peer_ids), K_(ch_infos));

  // the destination server that will receive the buffer.
  common::ObAddr server_addr_;
  // the buffer we are try to broadcast.
  ObDtlLinkedBuffer *bcast_buf_;
  // when send_count_ == channel count, we do send buffer by rpc.
  int64_t send_count_;
  // the channel count of this broadcast group.
  int64_t bcast_ch_count_;
  // dtl channel info
  common::ObArray<ObDtlChannelInfo> ch_infos_;
  // ptr to channel' data member ----- response
  common::ObArray<SendMsgResponse *> resps_;
  // the destination channel id of the broadcast service.
  common::ObArray<int64_t> peer_ids_;
  // active channel count, some of channel in this group may by drained.
  int64_t active_chs_count_;
};

class ObDtlChanAgent
{
  typedef uint64_t (*hj_hash_fun)(const common::ObObj &obj, const uint64_t hash);
  const static int64_t BROADCAST_CH_IDX = 0;
  struct ObServerChannel {
    ObDtlBasicChannel *ch_;
    int64_t ch_count_;
    common::ObAddr server_addr_;
    TO_STRING_KV(K(server_addr_), K(ch_count_));
  };
public:
  ObDtlChanAgent() : init_(false), local_channels_(), rpc_channels_(),
  bcast_channel_(nullptr), current_buffer_(nullptr), dtl_buf_encoder_(), dtl_buf_allocator_(),
  bc_services_(), dfo_key_(), sys_dtl_buf_size_(0)
    {};
  virtual ~ObDtlChanAgent() = default;
  int broadcast_row(const ObDtlMsg &msg, ObEvalCtx *eval_ctx = nullptr, bool is_eof = false);
  int flush();
  int init(dtl::ObDtlFlowControl &dfc,
           ObPxTaskChSet &task_ch_set,
           common::ObIArray<ObDtlChannel *> &channels,
           int64_t tenant_id,
           int64_t timeout_ts);
  int destroy();
private:
  int switch_buffer(int64_t need_size);
  int send_last_buffer(ObDtlLinkedBuffer *&last_buffer);
  int inner_broadcast_row(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, bool is_eof);
private:
  bool init_;
  // use to allocate broadcast service.
  common::ObArenaAllocator allocator_;
  // all local channel in this sqc.
  common::ObArray<ObDtlLocalChannel *> local_channels_;
  // all rpc channel in this sqc.
  common::ObArray<ObDtlRpcChannel *> rpc_channels_;
  // the represent channel use to allocate buf from data manager.
  ObDtlBasicChannel *bcast_channel_;
  // the buffer we are now write on.
  ObDtlLinkedBuffer *current_buffer_;
  // use to encoder msg.
  ObDtlBufEncoder dtl_buf_encoder_;
  // warpper of dtl mem manager.
  ObDtlBufAllocator dtl_buf_allocator_;
  // broadcast channel group.
  common::ObArray<ObDtlBcastService *> bc_services_;
  // dfo infomation.
  ObDtlDfoKey dfo_key_;
  // sys config, default value is 64K.
  int64_t sys_dtl_buf_size_;
};

}
}
}

#endif
