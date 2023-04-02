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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_TRANSMIT_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_TRANSMIT_OP_H_

#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{

class ObTransmitOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTransmitOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec)
  {}
  virtual ~ObTransmitOpInput() {}
  virtual void reset() override
  {}
};

class ObTransmitSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObTransmitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObTransmitSpec() {}

  void set_px_dop(const int64_t dop) { px_dop_ = dop; }
  void set_px_single(const bool single) { px_single_ = single; }

  int64_t get_px_dop() const { return px_dop_; }
  bool is_px_single() const { return px_single_; }
  // 为了在 explain 期间就可以展示出 qc id 和 dfo id
  // 需要在 transform 阶段就计算出 dfo id 并保存在这里
  inline void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  inline void set_px_id(int64_t px_id) { px_id_ = px_id; }
  inline int64_t get_dfo_id() const { return dfo_id_; }
  inline int64_t get_px_id() const { return px_id_; }

  inline void set_split_task_count(int64_t count)
  {
    if (OB_UNLIKELY(count <=0)) {
      split_task_count_ = 1;
    } else {
      split_task_count_ = count;
    }
  }

  inline int64_t get_split_task_count() const
  {
    return split_task_count_;
  }

  inline void set_parallel_server_count(int64_t count)
  {
    if (OB_UNLIKELY(count <=0)) {
      parallel_server_count_ = 1;
    } else {
      parallel_server_count_ = count;
    }
  }

  inline int64_t get_parallel_server_count() const
  {
    return parallel_server_count_;
  }

  inline void set_server_parallel_thread_count(int64_t count)
  {
    if (OB_UNLIKELY(count <=0)) {
      server_parallel_thread_count_ = 1;
    } else {
      server_parallel_thread_count_ = count;
    }
  }

  inline int64_t get_server_parallel_thread_count() const
  {
    return server_parallel_thread_count_;
  }

  inline void set_has_lgi(bool has_lgi) { has_lgi_ = has_lgi; }
  inline bool has_lgi() const { return has_lgi_; }

  void set_slave_mapping_type(SlaveMappingType slave_mapping_type) { slave_mapping_type_ = slave_mapping_type; }
  SlaveMappingType get_slave_mapping_type() const { return slave_mapping_type_; }
  bool is_slave_mapping() const { return SlaveMappingType::SM_NONE != slave_mapping_type_; }

  // 切分成多少个task
  int64_t split_task_count_;
  // 最多同时发送到多少台机器上并行执行
  int64_t parallel_server_count_;
  // 每台机器最多并行执行多少个该job的task线程
  int64_t server_parallel_thread_count_;

  int64_t px_dop_;
  bool px_single_;
  int64_t dfo_id_; // 在 CG 之前就给 dfo 定下 id
  int64_t px_id_; // 在 CG 之前就给多个 px 的 plan 定下每个 px 的 id

  inline bool is_repart_exchange() const
  { return OB_REPARTITION_NO_REPARTITION != repartition_type_;  }
  inline bool is_no_repart_exchange() const
  { return OB_REPARTITION_NO_REPARTITION == repartition_type_;  }

  int64_t repartition_ref_table_id_;
  ObRepartitionType repartition_type_;
  ObPQDistributeMethod::Type dist_method_;
  ObPQDistributeMethod::Type unmatch_row_dist_method_;
  ObNullDistributeMethod::Type null_row_dist_method_;
  SlaveMappingType slave_mapping_type_;
  //当前的job中包含light granule iterator operator
  bool has_lgi_;

  // for rollup distributor and collector
  bool is_rollup_hybrid_;

  // for window function adaptive pushdown
  bool is_wf_hybrid_;
};

class ObTransmitOp : public ObOperator
{
public:
  ObTransmitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObTransmitOp() {}

  virtual int inner_open() override { return ObOperator::inner_open(); }
  virtual int inner_rescan() override { return ObOperator::inner_rescan(); }
  virtual void destroy() override { ObOperator::destroy(); }
  virtual int inner_close() override { return ObOperator::inner_close(); }
  virtual int inner_get_next_row() override { return common::OB_NOT_SUPPORTED; }

};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_TRANSMIT_OP_H_
