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

#ifndef OCEANBASE_SHARE_AGGREGATE_AGGR_EXTRA_H_
#define OCEANBASE_SHARE_AGGREGATE_AGGR_EXTRA_H_

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/basic/ob_hp_infras_vec_mgr.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
class VecExtraResult
{
public:
  // %alloc is used to initialize the structures, can not be used to hold the data
  explicit VecExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info) :
    flags_(0), alloc_(alloc), op_monitor_info_(op_monitor_info)
  {}
  virtual ~VecExtraResult()
  {}
  bool is_inited() const
  {
    return is_inited_;
  }
  bool is_evaluated() const
  {
    return is_evaluated_;
  }
  void set_is_evaluated()
  {
    is_evaluated_ = true;
  }
  virtual void reuse()
  {}
  DECLARE_VIRTUAL_TO_STRING;

protected:
  union
  {
    uint8_t flags_;
    struct
    {
      int32_t is_inited_ : 1;
      int32_t is_evaluated_ : 1;
    };
  };
  common::ObIAllocator &alloc_;
  ObMonitorNode &op_monitor_info_;
};

class HashBasedDistinctVecExtraResult : public VecExtraResult
{
public:
  explicit HashBasedDistinctVecExtraResult(common::ObIAllocator &alloc,
                                           ObMonitorNode &op_monitor_info) :
    VecExtraResult(alloc, op_monitor_info),
    hash_values_for_batch_(nullptr), my_skip_(nullptr), aggr_info_(nullptr),
    hp_infras_mgr_(nullptr), hp_infras_(nullptr), need_rewind_(false), max_batch_size_(0),
    try_check_tick_(0), status_flags_(0), brs_holder_(&alloc)
  {}
  virtual ~HashBasedDistinctVecExtraResult();
  virtual void reuse();
  int rewind();
  int init_distinct_set(const ObAggrInfo &aggr_info, const bool need_rewind,
                        ObHashPartInfrasVecMgr &hp_infras_mgr, ObEvalCtx &eval_ctx);
  int insert_row_for_batch(const common::ObIArray<ObExpr *> &exprs, const int64_t batch_size,
                           const ObBitVector *skip = nullptr, const int64_t start_idx = 0);
  int get_next_unique_hash_table_batch(const common::ObIArray<ObExpr *> &exprs,
                                       const int64_t max_row_cnt, int64_t &read_rows);
  DECLARE_VIRTUAL_TO_STRING;

private:
  int init_hp_infras();
  int init_my_skip(const int64_t batch_size);
  int build_distinct_data_for_batch(const common::ObIArray<ObExpr *> &exprs,
                                    const int64_t batch_size);
  inline int try_check_status()
  {
    return ((++try_check_tick_) % 1024 == 0)
      ? THIS_WORKER.check_status()
      : common::OB_SUCCESS;
  }

protected:
  uint64_t *hash_values_for_batch_;
  ObBitVector *my_skip_;
  const ObAggrInfo *aggr_info_;
  ObHashPartInfrasVecMgr *hp_infras_mgr_;
  HashPartInfrasVec *hp_infras_;
  bool need_rewind_;
  int64_t max_batch_size_;
  int64_t try_check_tick_;
  union
  {
    uint8_t status_flags_;
    struct
    {
      uint32_t inited_hp_infras_ : 1;
      uint32_t got_row_ : 1;
    };
  };

public:
  ObVectorsResultHolder brs_holder_;
};

} // namespace aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_AGGR_EXTRA_H_