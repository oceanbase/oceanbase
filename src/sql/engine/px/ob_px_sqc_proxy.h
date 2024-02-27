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

#ifndef __OB_SQL_PX_SQC_PROXY_H__
#define __OB_SQL_PX_SQC_PROXY_H__

#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"


namespace oceanbase
{
namespace sql
{

class ObSqcLeaderTokenGuard
{
public:
  ObSqcLeaderTokenGuard(common::ObSpinLock &lock, common::ObThreadCond &msg_ready_cond)
      : lock_(lock), hold_lock_(false), msg_ready_cond_(msg_ready_cond)
  {
    if (common::OB_SUCCESS == lock_.trylock()) {
      hold_lock_ = true;
    }
  }
  ~ObSqcLeaderTokenGuard()
  {
    if (hold_lock_) {
      lock_.unlock();
    }
    msg_ready_cond_.broadcast();
  }
  bool hold_token() const { return hold_lock_; }
private:
  common::ObSpinLock &lock_;
  bool hold_lock_;
  common::ObThreadCond &msg_ready_cond_;
};

class ObBloomFilterSendCtx
{
public:
  ObBloomFilterSendCtx() :
    bloom_filter_ready_(false),
    filter_data_(NULL),
    filter_indexes_(),
    per_addr_bf_count_(0),
    filter_addr_idx_(0),
    bf_compressor_type_(common::ObCompressorType::NONE_COMPRESSOR),
    each_group_size_(0)
  {}
  ~ObBloomFilterSendCtx() {}
  bool bloom_filter_ready() const { return bloom_filter_ready_; }
  void set_bloom_filter_ready(bool flag) { bloom_filter_ready_ = flag; }
  int64_t &get_filter_addr_idx() { return filter_addr_idx_; }
  common::ObIArray<BloomFilterIndex> &get_filter_indexes() { return filter_indexes_; }
  void set_filter_data(ObPxBloomFilterData *data) { filter_data_ = data; }
  ObPxBloomFilterData *get_filter_data() { return filter_data_; }
  int generate_filter_indexes(int64_t each_group_size, int64_t addr_count);
  void set_per_addr_bf_count(int64_t count) { per_addr_bf_count_ = count; }
  int64_t get_per_addr_bf_count() { return per_addr_bf_count_; }
  void set_bf_compress_type(common::ObCompressorType type)
      { bf_compressor_type_ = type; }
  common::ObCompressorType get_bf_compress_type() { return bf_compressor_type_; }
  int64_t &get_each_group_size() { return each_group_size_; }
  TO_STRING_KV(K_(bloom_filter_ready));
private:
  bool bloom_filter_ready_;
  ObPxBloomFilterData *filter_data_;
  common::ObArray<BloomFilterIndex> filter_indexes_;
  int64_t per_addr_bf_count_;
  int64_t filter_addr_idx_;
  common::ObCompressorType bf_compressor_type_;
  int64_t each_group_size_;
};

class ObPxSQCProxy
{
public:
  typedef hash::ObHashMap<int64_t, common::ObSArray<ObAddr> *,
      hash::NoPthreadDefendMode> SQCP2PDhMap;
public:
  ObPxSQCProxy(ObSqcCtx &sqc_ctx, ObPxRpcInitSqcArgs &arg);
  virtual ~ObPxSQCProxy();

  // basics
  int init();
  void destroy();

  // for transmit op
  int get_part_ch_map(ObPxPartChInfo &map, int64_t timeout_ts);
  int get_transmit_data_ch(const int64_t sqc_id,
                          const int64_t task_id,
                          int64_t timeout_ts,
                          ObPxTaskChSet &task_ch_set,
                          dtl::ObDtlChTotalInfo **ch_info);

  // for receive op
  int get_receive_data_ch(int64_t child_dfo_id,
                          const int64_t sqc_id,
                          const int64_t task_id,
                          int64_t timeout_ts,
                          ObPxTaskChSet &task_ch_set,
                          dtl::ObDtlChTotalInfo *ch_info);
  // for px bloom filter op
  int get_bloom_filter_ch(ObPxBloomFilterChSet &ch_set,
                          int64_t &sqc_count,
                          int64_t timeout_ts,
                          bool is_transmit);

  template <class PieceMsg, class WholeMsg>
  int get_dh_msg_sync(uint64_t op_id,
      dtl::ObDtlMsgType msg_type,
      const PieceMsg &piece,
      const WholeMsg *&whole,
      int64_t timeout_ts,
      bool send_piece = true,
      bool need_wait_whole_msg = true);

  template <class PieceMsg, class WholeMsg>
  int get_dh_msg(uint64_t op_id,
      dtl::ObDtlMsgType msg_type,
      const PieceMsg &piece,
      const WholeMsg *&whole,
      int64_t timeout_ts,
      bool send_piece = true,
      bool need_wait_whole_msg = true);


  // 用于 worker 汇报执行结果
  int report_task_finish_status(int64_t task_idx, int rc);

  // for root thread
  int check_task_finish_status(int64_t timeout_ts);

  void get_self_sqc_info(dtl::ObDtlSqcInfo &sqc_info);
  void get_self_dfo_key(dtl::ObDtlDfoKey &key);

  void get_parent_dfo_key(dtl::ObDtlDfoKey &key);
  // 向qc汇报sqc的结束
  int report(int end_ret) const;

  bool get_transmit_use_interm_result() const { return sqc_arg_.sqc_.transmit_use_interm_result(); }
  bool get_recieve_use_interm_result() const { return sqc_arg_.sqc_.recieve_use_interm_result(); }
  bool adjoining_root_dfo() const { return sqc_arg_.sqc_.adjoining_root_dfo(); }
  int64_t get_dfo_id() { return sqc_arg_.sqc_.get_dfo_id(); }
  int64_t get_sqc_id() { return sqc_arg_.sqc_.get_sqc_id(); }
  const ObPxRpcInitSqcArgs &get_sqc_arg() { return sqc_arg_; }
  int make_sqc_sample_piece_msg(ObDynamicSamplePieceMsg &msg, bool &finish);
  ObDynamicSamplePieceMsg &get_piece_sample_msg() { return sample_msg_; }
  ObInitChannelPieceMsg &get_piece_init_channel_msg() { return init_channel_msg_; }
  common::ObIArray<ObBloomFilterSendCtx> &get_bf_send_ctx_array() { return bf_send_ctx_array_; }
  int append_bf_send_ctx(int64_t &bf_send_ctx_idx);
  int64_t get_task_count() const;
  common::ObThreadCond &get_msg_ready_cond() { return msg_ready_cond_; }
  int64_t get_dh_msg_cnt() const;
  void atomic_inc_dh_msg_cnt();
  int64_t atomic_add_and_fetch_dh_msg_cnt();
  int construct_p2p_dh_map(ObP2PDhMapInfo &map_info);
  SQCP2PDhMap &get_p2p_dh_map()  { return p2p_dh_map_; }
  int check_is_local_dh(int64_t p2p_dh_id, bool &is_local_dh, int64_t msg_cnt);
private:
  /* functions */
  int setup_loop_proc(ObSqcCtx &sqc_ctx);
  int process_dtl_msg(int64_t timeout_ts);
  int do_process_dtl_msg(int64_t timeout_ts);
  int link_sqc_qc_channel(ObPxRpcInitSqcArgs &sqc_arg);
  int unlink_sqc_qc_channel(ObPxRpcInitSqcArgs &sqc_arg);
  bool need_transmit_channel_map_via_dtl();
  bool need_receive_channel_map_via_dtl(int64_t child_dfo_id);
  int get_whole_msg_provider(uint64_t op_id, dtl::ObDtlMsgType msg_type, ObPxDatahubDataProvider *&provider);
  int64_t get_process_query_time();
  int64_t get_query_timeout_ts();
  int sync_wait_all(ObPxDatahubDataProvider &provider);
  // for peek datahub whole msg
  template <class PieceMsg, class WholeMsg>
  int inner_get_dh_msg(
      uint64_t op_id,
      dtl::ObDtlMsgType msg_type,
      const PieceMsg &piece,
      const WholeMsg *&whole,
      int64_t timeout_ts,
      bool need_sync,
      bool send_piece,
      bool need_wait_whole_msg);
  /* variables */
public:
  ObSqcCtx &sqc_ctx_;
private:
  ObPxRpcInitSqcArgs &sqc_arg_;
  // 所有 worker 都抢这个锁，抢到者为 leader，负责推进 msg loop
  common::ObSpinLock leader_token_lock_;

  // 这个锁是临时用，用于互斥多个线程同时用 sqc channel 发数据，
  // Dtl 支持并发访问后可以删掉
  common::ObSpinLock dtl_lock_;
  common::ObArray<ObBloomFilterSendCtx> bf_send_ctx_array_; // record bloom filters ready to be sent
  ObDynamicSamplePieceMsg sample_msg_;
  ObInitChannelPieceMsg init_channel_msg_;
  // msg cond is shared by transmit && rescive && bloom filter
  common::ObThreadCond msg_ready_cond_;
  SQCP2PDhMap p2p_dh_map_;
  DISALLOW_COPY_AND_ASSIGN(ObPxSQCProxy);
};


template <class PieceMsg, class WholeMsg>
int ObPxSQCProxy::get_dh_msg_sync(uint64_t op_id,
        dtl::ObDtlMsgType msg_type,
        const PieceMsg &piece,
        const WholeMsg *&whole,
        int64_t timeout_ts,
        bool send_piece,
        bool need_wait_whole_msg)
{
  return inner_get_dh_msg(op_id, msg_type, piece, whole,
                          timeout_ts, true, send_piece,
                          need_wait_whole_msg);
}

template <class PieceMsg, class WholeMsg>
int ObPxSQCProxy::get_dh_msg(uint64_t op_id,
    dtl::ObDtlMsgType msg_type,
    const PieceMsg &piece,
    const WholeMsg *&whole,
    int64_t timeout_ts,
    bool send_piece,
    bool need_wait_whole_msg)
{
  return inner_get_dh_msg(op_id, msg_type, piece, whole,
                          timeout_ts, false, send_piece,
                          need_wait_whole_msg);
}

template <class PieceMsg, class WholeMsg>
int ObPxSQCProxy::inner_get_dh_msg(
    uint64_t op_id,
    dtl::ObDtlMsgType msg_type,
    const PieceMsg &piece,
    const WholeMsg *&whole,
    int64_t timeout_ts,
    bool need_sync,
    bool send_piece /*= true*/,
    bool need_wait_whole_msg /*= true*/)
{
  int ret = common::OB_SUCCESS;
  ObPxDatahubDataProvider *provider = nullptr;
  if (OB_FAIL(get_whole_msg_provider(op_id, msg_type, provider))) {
    SQL_LOG(WARN, "fail get provider", K(ret));
  } else if (need_sync && OB_FAIL(sync_wait_all(*provider))) {
    SQL_LOG(WARN, "failed to sync wait", K(ret));
  } else {
    if (send_piece) {
      ObLockGuard<ObSpinLock> lock_guard(dtl_lock_);
      dtl::ObDtlChannel *ch = sqc_arg_.sqc_.get_sqc_channel();
      if (OB_ISNULL(ch)) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "empty channel", K(ret));
      } else if (OB_FAIL(ch->send(piece, timeout_ts))) { // 尽力而为，如果 push 失败就由其它机制处理
        SQL_LOG(WARN, "fail push data to channel", K(ret));
      } else if (OB_FAIL(ch->flush())) {
        SQL_LOG(WARN, "fail flush dtl data", K(ret));
      }
    }

    if (OB_SUCC(ret) && need_wait_whole_msg) {
      typename WholeMsg::WholeMsgProvider *p = static_cast<typename WholeMsg::WholeMsgProvider *>(provider);
      int64_t wait_count = 0;
      do {
        ret = OB_SUCCESS;
        ObSqcLeaderTokenGuard guard(leader_token_lock_, msg_ready_cond_);
        if (guard.hold_token()) {
          ret = process_dtl_msg(timeout_ts);
          SQL_LOG(DEBUG, "process dtl msg done", K(ret));
        }
        if (OB_EAGAIN == ret || OB_SUCCESS == ret) {
          const dtl::ObDtlMsg *msg = nullptr;
          if (OB_FAIL(p->get_msg_nonblock(msg, timeout_ts))) {
            SQL_LOG(TRACE, "fail get msg", K(timeout_ts), K(ret));
          } else {
            whole = static_cast<const WholeMsg *>(msg);
          }
        }
        if (common::OB_EAGAIN == ret) {
          if(0 == (++wait_count) % 100) {
            SQL_LOG(TRACE, "try to get datahub data repeatly",
                    K(timeout_ts), K(wait_count), K(ret));
          }
          // wait 50us
          ob_usleep(50);
        }
      } while (common::OB_EAGAIN == ret &&
               OB_SUCC(THIS_WORKER.check_status()));
    }
  }
  return ret;
}

}
}
#endif /* __OB_SQL_PX_SQC_PROXY_H__ */
//// end of header file

