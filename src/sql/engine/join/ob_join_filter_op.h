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

#ifndef _SQL_ENGINE_JOIN_OB_JOIN_FILTER_OP_H
#define _SQL_ENGINE_JOIN_OB_JOIN_FILTER_OP_H 1

#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_se_array.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_msg.h"
#include "share/detect/ob_detectable_id.h"


namespace oceanbase
{
namespace sql
{

class ObPxSQCProxy;


struct ObJoinFilterShareInfo
{
  ObJoinFilterShareInfo()
      : unfinished_count_ptr_(0), ch_provider_ptr_(0), release_ref_ptr_(0), filter_ptr_(0),
        shared_msgs_(0)
  {}
  uint64_t unfinished_count_ptr_; // send_filter引用计数, 初始值为worker个数
  uint64_t ch_provider_ptr_; // sqc_proxy, 由于序列化需要, 使用指针表示.
  uint64_t release_ref_ptr_; // 释放内存引用计数, 初始值为worker个数.
  uint64_t filter_ptr_;   //此指针将作为PX JOIN FILTER CREATE算子共享内存.
  uint64_t shared_msgs_;  //sqc-shared dh msgs
  OB_UNIS_VERSION_V(1);
};

struct ObJoinFilterRuntimeConfig
{
  OB_UNIS_VERSION_V(1);
public:
  TO_STRING_KV(K_(bloom_filter_ratio), K_(each_group_size), K_(bf_piece_size),
               K_(runtime_filter_wait_time_ms), K_(runtime_filter_max_in_num),
               K_(runtime_bloom_filter_max_size), K_(px_message_compression));
public:
  ObJoinFilterRuntimeConfig() :
      bloom_filter_ratio_(0.0),
      each_group_size_(OB_INVALID_ID),
      bf_piece_size_(0),
      runtime_filter_wait_time_ms_(0),
      runtime_filter_max_in_num_(0),
      runtime_bloom_filter_max_size_(0),
      px_message_compression_(false) {}
  double bloom_filter_ratio_;
  int64_t each_group_size_;
  int64_t bf_piece_size_;
  int64_t runtime_filter_wait_time_ms_;
  int64_t runtime_filter_max_in_num_;
  int64_t runtime_bloom_filter_max_size_;
  bool px_message_compression_;
};
class ObJoinFilterOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObJoinFilterOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
      share_info_(),
      task_id_(0),
      px_sequence_id_(OB_INVALID_ID),
      bf_idx_at_sqc_proxy_(-1),
      config_(),
      register_dm_info_()
  {}
  virtual ~ObJoinFilterOpInput() {}

  virtual int init(ObTaskInfo &task_info);
  virtual void reset() override
  {
    auto &ctx = exec_ctx_;
    auto &spec = spec_;
    void *ptr = this;
    this->~ObJoinFilterOpInput();
    new (ptr) ObJoinFilterOpInput(ctx, spec);
  }
  bool is_finish();
  bool check_release();
  // 每个worker共享同一块sqc_proxy
  void set_sqc_proxy(ObPxSQCProxy &sqc_proxy)
  {
    share_info_.ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  ObJoinFilterOp *get_filter()
  {
    return reinterpret_cast<ObJoinFilterOp *>(share_info_.filter_ptr_);
  }
  int init_share_info(
      const ObJoinFilterSpec &spec,
      ObExecContext &ctx,
      int64_t task_count,
      int64_t sqc_count);
  int init_shared_msgs(const ObJoinFilterSpec &spec,
      ObExecContext &ctx,
      int64_t sqc_count);
  static int construct_msg_details(const ObJoinFilterSpec &spec,
      ObPxSQCProxy *sqc_proxy,
      ObJoinFilterRuntimeConfig &config,
      ObP2PDatahubMsgBase &msg, int64_t sqc_count);
  void set_task_id(int64_t task_id)  { task_id_ = task_id; }

  inline void set_bf_idx_at_sqc_proxy(int64_t idx) { bf_idx_at_sqc_proxy_ = idx; }

  inline int64_t get_bf_idx_at_sqc_proxy() { return bf_idx_at_sqc_proxy_; }
  void set_px_sequence_id(int64_t id) { px_sequence_id_ = id; }
  int64_t get_px_sequence_id() { return px_sequence_id_; }
  int load_runtime_config(const ObJoinFilterSpec &spec, ObExecContext &ctx);
  void init_register_dm_info(const ObDetectableId &id, const common::ObAddr &addr)
  {
    register_dm_info_.detectable_id_ = id;
    register_dm_info_.addr_ = addr;
  }
public:
  ObJoinFilterShareInfo share_info_; //bloom filter共享内存
  int64_t task_id_; //在pwj join场景中会用到此task_id作为bf_key
  int64_t px_sequence_id_;
  int64_t bf_idx_at_sqc_proxy_;
  ObJoinFilterRuntimeConfig config_;
  common::ObRegisterDmInfo register_dm_info_;
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterOpInput);
};

struct ObRuntimeFilterInfo
{
  OB_UNIS_VERSION_V(1);
public:
  TO_STRING_KV(K_(filter_expr_id), K_(p2p_datahub_id), K_(filter_shared_type));
public:
  ObRuntimeFilterInfo() :
      filter_expr_id_(OB_INVALID_ID),
      p2p_datahub_id_(OB_INVALID_ID),
      filter_shared_type_(INVALID_TYPE),
      dh_msg_type_(ObP2PDatahubMsgBase::ObP2PDatahubMsgType::NOT_INIT)
      {}
  virtual ~ObRuntimeFilterInfo() = default;
  void reset () {
    filter_expr_id_ = OB_INVALID_ID;
    p2p_datahub_id_ = OB_INVALID_ID;
    dh_msg_type_ = ObP2PDatahubMsgBase::ObP2PDatahubMsgType::NOT_INIT;
  }
  int64_t filter_expr_id_;
  int64_t p2p_datahub_id_;
  JoinFilterSharedType filter_shared_type_;
  ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type_;
};

class ObJoinFilterSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(2);
public:
  ObJoinFilterSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(mode), K_(filter_id), K_(filter_len), K_(rf_infos));

  inline void set_mode(JoinFilterMode mode) { mode_ = mode; }
  inline JoinFilterMode get_mode() const { return mode_; }
  inline void set_filter_id(int64_t id) { filter_id_ = id; }
  inline int64_t get_filter_id() const { return filter_id_; }
  inline void set_filter_length(int64_t len) { filter_len_ = len; }
  inline int64_t get_filter_length() const { return filter_len_; }
  inline ObIArray<ObExpr*> &get_exprs() { return join_keys_; }
  inline bool is_create_mode() const { return JoinFilterMode::CREATE == mode_; }
  inline bool is_use_mode() const { return JoinFilterMode::USE == mode_; }
  inline bool is_partition_filter() const
  { return filter_shared_type_ == JoinFilterSharedType::NONSHARED_PARTITION_JOIN_FILTER ||
           filter_shared_type_ == JoinFilterSharedType::SHARED_PARTITION_JOIN_FILTER; };
  inline void set_shared_filter_type(JoinFilterSharedType type) { filter_shared_type_ = type; }
  inline bool is_shared_join_filter() const
  { return filter_shared_type_ == JoinFilterSharedType::SHARED_JOIN_FILTER ||
           filter_shared_type_ == JoinFilterSharedType::SHARED_PARTITION_JOIN_FILTER; }

  JoinFilterMode mode_;
  int64_t filter_id_;
  int64_t filter_len_;
  ExprFixedArray join_keys_;
  common::ObHashFuncs hash_funcs_;
  ObCmpFuncs cmp_funcs_;
  JoinFilterSharedType filter_shared_type_;
  ObExpr *calc_tablet_id_expr_;
  common::ObFixedArray<ObRuntimeFilterInfo, common::ObIAllocator> rf_infos_;
  common::ObFixedArray<bool, common::ObIAllocator> need_null_cmp_flags_;
  bool is_shuffle_;
  int64_t each_group_size_;
};

class ObJoinFilterOp : public ObOperator
{
  struct ObJoinFilterMsg {
    ObJoinFilterMsg() : bf_msg_(nullptr), range_msg_(nullptr), in_msg_(nullptr) {}
    ObRFBloomFilterMsg *bf_msg_;
    ObRFRangeFilterMsg *range_msg_;
    ObRFInFilterMsg *in_msg_;
  };
public:
  ObJoinFilterOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObJoinFilterOp();

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override; // for batch
  virtual int inner_drain_exch() override;
  virtual void destroy() override {
    lucky_devil_champions_.reset();
    local_rf_msgs_.reset();
    shared_rf_msgs_.reset();
    ObOperator::destroy();
  }
  static int link_ch_sets(ObPxBloomFilterChSets &ch_sets,
                          common::ObIArray<dtl::ObDtlChannel *> &channels);
private:
  bool is_valid();
  int destroy_filter();
  int insert_by_row();
  int insert_by_row_batch(const ObBatchRows *child_brs);
  int calc_expr_values(ObDatum *&datum);
  int do_create_filter_rescan();
  int do_use_filter_rescan();
  int try_send_join_filter();
  int try_merge_join_filter();
  int calc_each_bf_group_size(int64_t &);
  int update_plan_monitor_info();
  int prepre_bloom_filter_ctx(ObBloomFilterSendCtx *bf_ctx);
  int open_join_filter_create();
  int open_join_filter_use();
  int close_join_filter_create();
  int close_join_filter_use();
  int init_shared_msgs_from_input();
  int init_local_msg_from_shared_msg(ObP2PDatahubMsgBase &msg);
  int release_local_msg();
  int release_shared_msg();
  int mark_not_need_send_bf_msg();
private:
  static const int64_t ADAPTIVE_BF_WINDOW_ORG_SIZE = 4096;
  static constexpr double ACCEPTABLE_FILTER_RATE = 0.98;
public:
  ObJoinFilterMsg *filter_create_msg_;
  ObArray<ObP2PDatahubMsgBase *> shared_rf_msgs_; // sqc level share
  ObArray<ObP2PDatahubMsgBase *> local_rf_msgs_;
  uint64_t *batch_hash_values_;
  ObArray<bool> lucky_devil_champions_;
};

}
}

#endif /* _SQL_ENGINE_JOIN_OB_JOIN_FILTER_OP_H */


