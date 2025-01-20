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

#ifndef OCEANBASE_SQL_ENGINE_STATISTICS_COLLECTOR_OP_H_
#define OCEANBASE_SQL_ENGINE_STATISTICS_COLLECTOR_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_material_op_impl.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/engine/px/datahub/components/ob_dh_statistics_collector.h"
#include "src/sql/engine/ob_by_pass_operator.h"

namespace oceanbase
{
namespace sql
{

class ObStatisticsCollectorSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObStatisticsCollectorSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), row_threshold_(0), hj_op_ids_(alloc), nlj_op_ids_(alloc)
  {}
  virtual int register_to_datahub(ObExecContext &ctx) const override;
  TO_STRING_KV(K(get_name()), K(get_id()),
               K_(row_threshold), K_(hj_op_ids), K_(nlj_op_ids));
public:
  int64_t row_threshold_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> hj_op_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> nlj_op_ids_;
};

class ObStatisticsCollectorOp : public ObByPassOperator
{
public:
  ObStatisticsCollectorOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObStatisticsCollectorOp() {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  virtual int inner_close() override;
  int do_row_statistics_collect();
  int do_batch_statistics_collect();
  virtual int inner_drain_exch() override;

private:
  int init_material_impl();
  int set_op_passed(int64_t op_id);
  int set_join_passed(bool use_hash_join);
  int global_decide_join_method(bool &use_hash_join);
  int global_decide_join_method(bool &use_hash_join, bool need_wait_whole_msg);
private:
  int64_t read_row_cnt_;
  bool statistics_collect_done_;
  bool read_store_end_;
  ObMaterialOpImpl material_impl_;
  ObBatchResultHolder brs_holder_;

};

}
}
#endif
