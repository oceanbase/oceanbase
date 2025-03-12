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
#include "sql/engine/sort/ob_sort_vec_op_provider.h"

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
  int init_vector_default(ObEvalCtx &ctx, const int64_t size);
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
class DataStoreVecExtraResult : public VecExtraResult
{
public:
  explicit DataStoreVecExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info,
                                   bool need_sort) :
    VecExtraResult(alloc, op_monitor_info),
    sort_(NULL), pvt_skip(nullptr), need_sort_(need_sort), data_store_inited_(false),
    data_store_brs_holder_(&alloc)
  {}

  virtual ~DataStoreVecExtraResult();

  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx,
                const sql::EvalBound &bound, const sql::ObBitVector &skip,
                const uint16_t selector[], const int64_t size, ObIAllocator &allocator);

  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx,
                const sql::EvalBound &bound, const sql::ObBitVector &skip, ObIAllocator &allocator);

  int get_next_batch(ObEvalCtx &ctx, const common::ObIArray<ObExpr *> &exprs, int64_t &read_rows);

  int prepare_for_eval();

  void reuse();

  bool data_store_is_inited() const
  {
    return data_store_inited_;
  }

  int init_data_set(ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, ObMonitorNode *op_monitor_info,
                    ObIOEventObserver *io_event_observer_, ObIAllocator &allocator,
                    bool need_rewind);

  int rewind();

protected:
  union
  {
    ObSortVecOpProvider *sort_;
    ObTempRowStore *store_;
  };

  ObTempRowStore::Iterator *vec_result_iter_;

  ObBitVector *pvt_skip;

  bool need_sort_;
  bool data_store_inited_;

public:
  ObVectorsResultHolder data_store_brs_holder_;
};

struct ExtraStores {
  class HashBasedDistinctVecExtraResult *distinct_extra_store;
  class DataStoreVecExtraResult *data_store;

  ~ExtraStores()
  {
    if (distinct_extra_store != nullptr) {
      distinct_extra_store->~HashBasedDistinctVecExtraResult();
      distinct_extra_store = nullptr;
    }
    if (data_store != nullptr) {
      data_store->~DataStoreVecExtraResult();
      data_store = nullptr;
    }
  }

  void reuse()
  {
    if (distinct_extra_store != nullptr) {
      distinct_extra_store->reuse();
    }
    if (data_store != nullptr) {
      data_store->reuse();
    }
  }

  void set_is_evaluated()
  {
    if (distinct_extra_store != nullptr) {
      distinct_extra_store->is_evaluated();
    }
    if (data_store != nullptr) {
      data_store->is_evaluated();
    }
  }

  bool is_evaluated()
  {
    bool evaluated = false;
    if (distinct_extra_store != nullptr) {
      evaluated |= distinct_extra_store->is_evaluated();
    }
    if (data_store != nullptr) {
      evaluated |= data_store->is_evaluated();
    }
    return evaluated;
  }

  int rewind()
  {
    int ret = OB_SUCCESS;
    if (distinct_extra_store != nullptr) {
      ret = distinct_extra_store->rewind();
    }
    if (data_store != nullptr && OB_SUCC(ret)) {
      ret = data_store->rewind();
    }
    return ret;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K(distinct_extra_store));
    J_KV(K(data_store));
    J_OBJ_END();
    return pos;
  }
};

} // namespace aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_AGGR_EXTRA_H_