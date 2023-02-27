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


namespace oceanbase
{
namespace sql
{

class ObPxSQCProxy;


struct ObJoinFilterShareInfo
{
  uint64_t unfinished_count_ptr_; // send_filter引用计数, 初始值为worker个数
  uint64_t ch_provider_ptr_; // sqc_proxy, 由于序列化需要, 使用指针表示.
  uint64_t release_ref_ptr_; // 释放内存引用计数, 初始值为worker个数.
  uint64_t filter_ptr_;   //此指针将作为PX JOIN FILTER CREATE算子共享内存.
  OB_UNIS_VERSION_V(1);
};

class ObJoinFilterOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObJoinFilterOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
      share_info_(),
      is_local_create_(true),
      task_id_(0),
      px_sequence_id_(OB_INVALID_ID)
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
  int check_finish(bool &is_end, bool is_shared);
  bool check_release(bool is_shared);
  // 每个worker共享同一块sqc_proxy
  void set_sqc_proxy(ObPxSQCProxy &sqc_proxy)
  {
    share_info_.ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  bool is_local_create() { return  is_local_create_; }
  ObJoinFilterOp *get_filter()
  {
    return reinterpret_cast<ObJoinFilterOp *>(share_info_.filter_ptr_);
  }
  int init_share_info(common::ObIAllocator &allocator, int64_t task_count);
  void set_task_id(int64_t task_id)  { task_id_ = task_id; }

  void set_px_sequence_id(int64_t id) { px_sequence_id_ = id; }
  int64_t get_px_sequence_id() { return px_sequence_id_; }
public:
  ObJoinFilterShareInfo share_info_; //bloom filter共享内存
  bool is_local_create_;  //用于标记create算子是否是local的.
  int64_t task_id_; //在pwj join场景中会用到此task_id作为bf_key
  int64_t px_sequence_id_;
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterOpInput);
};

class ObJoinFilterSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObJoinFilterSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(mode), K_(filter_id), K_(server_id), K_(filter_len),
                       K_(is_shuffle), K_(filter_expr_id));

  inline void set_mode(JoinFilterMode mode) { mode_ = mode; }
  inline JoinFilterMode get_mode() const { return mode_; }
  inline void set_filter_id(int64_t id) { filter_id_ = id; }
  inline int64_t get_filter_id() const { return filter_id_; }
  inline void set_server_id(int64_t id) { server_id_ = id; }
  inline int64_t get_server_id() const { return server_id_; }
  inline void set_filter_length(int64_t len) { filter_len_ = len; }
  inline int64_t get_filter_length() const { return filter_len_; }
  inline ObIArray<ObExpr*> &get_exprs() { return join_keys_; }
  inline bool is_create_mode() const { return JoinFilterMode::CREATE == mode_; }
  inline bool is_use_mode() const { return JoinFilterMode::USE == mode_; }
  inline bool is_shuffle() const { return is_shuffle_; }
  inline void set_is_shuffle(bool flag) { is_shuffle_ = flag; }
  inline bool is_partition_filter() const
  { return filter_type_ == JoinFilterType::NONSHARED_PARTITION_JOIN_FILTER ||
           filter_type_ == JoinFilterType::SHARED_PARTITION_JOIN_FILTER; };
  inline void set_filter_type(JoinFilterType type) { filter_type_ = type; }
  inline bool is_shared_join_filter() const
  { return filter_type_ == JoinFilterType::SHARED_JOIN_FILTER ||
           filter_type_ == JoinFilterType::SHARED_PARTITION_JOIN_FILTER; }

  JoinFilterMode mode_;
  int64_t filter_id_;
  int64_t server_id_;
  int64_t filter_len_;
  bool is_shuffle_; //filter create端检查filter use端是否需要做shuffle
  ExprFixedArray join_keys_;
  common::ObHashFuncs hash_funcs_;
  int64_t filter_expr_id_;
  JoinFilterType filter_type_;
  ObExpr *calc_tablet_id_expr_;
};

class ObJoinFilterOp : public ObOperator
{
public:
  ObJoinFilterOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObJoinFilterOp();

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override; // for batch
  virtual void destroy() override { ObOperator::destroy(); }
  static int link_ch_sets(ObPxBloomFilterChSets &ch_sets,
                          common::ObIArray<dtl::ObDtlChannel *> &channels);
private:
  bool is_valid();
  int destroy_filter();
  int send_filter();
  int send_local_filter();
  int mark_rpc_filter();

  int insert_by_row();
  int insert_by_row_batch(const ObBatchRows *child_brs);
  int check_contain_row(bool &match);
  int calc_hash_value(uint64_t &hash_value, bool &ignore);
  int calc_hash_value(uint64_t &hash_value);
  int do_create_filter_rescan();
  int do_use_filter_rescan();
public:
  ObPXBloomFilterHashWrapper bf_key_;
  ObPxBloomFilter *filter_use_;
  ObPxBloomFilter *filter_create_;
  ObPxBloomFilterChSets *bf_ch_sets_;
  uint64_t *batch_hash_values_;
};

}
}

#endif /* _SQL_ENGINE_JOIN_OB_JOIN_FILTER_OP_H */


