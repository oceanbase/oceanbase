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

#ifndef _OB_SQ_OB_SLICE_IDX_CALC_H_
#define _OB_SQ_OB_SLICE_IDX_CALC_H_

#include "common/ob_partition_key.h"
#include "sql/executor/ob_task_event.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/executor/ob_shuffle_service.h"
#include "sql/executor/ob_transmit.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/ob_sql_define.h"

namespace oceanbase {

namespace common {
class ObNewRow;
}

namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share

namespace sql {
class ObRangeHashKeyGetter;
class ObExecContext;
class ObSliceIdxCalc {
public:
  static const int64_t DEFAULT_CHANNEL_CNT = 64;
  static const int64_t DEFAULT_CHANNEL_IDX_TO_DROP_ROW = -2;
  typedef common::ObSEArray<int64_t, DEFAULT_CHANNEL_CNT> SliceIdxArray;

  explicit ObSliceIdxCalc(common::ObIAllocator& allocator) : shuffle_service_(allocator){};
  virtual ~ObSliceIdxCalc() = default;

  virtual int get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array);
  virtual int get_slice_indexes(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, SliceIdxArray& slice_idx_array);
  // get partition_if of the row of previous get_slice_indexes() call.
  virtual int get_previous_row_partition_id(ObObj& partition_id);

protected:
  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx) = 0;
  virtual int get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx)
  {
    UNUSED(exprs);
    UNUSED(eval_ctx);
    UNUSED(slice_idx);
    return common::OB_NOT_IMPLEMENT;
    ;
  }

  ObShuffleService shuffle_service_;
};

// For transmit which need send one row to more than one channel. (e.g.: broadcast)
class ObMultiSliceIdxCalc : public ObSliceIdxCalc {
public:
  ObMultiSliceIdxCalc(common::ObIAllocator& alloc) : ObSliceIdxCalc(alloc)
  {}
  virtual int get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array) = 0;

protected:
  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx)
  {
    UNUSED(row);
    UNUSED(slice_idx);
    return common::OB_ERR_UNEXPECTED;
  }
};

// for root dfo only.
class ObAllToOneSliceIdxCalc : public ObSliceIdxCalc {
public:
  ObAllToOneSliceIdxCalc(common::ObIAllocator& alloc) : ObSliceIdxCalc(alloc)
  {}
  virtual ~ObAllToOneSliceIdxCalc() = default;
  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx) override;
  virtual int get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx);

protected:
};

class ObRepartSliceIdxCalc : virtual public ObSliceIdxCalc {
public:
  ObRepartSliceIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const ObSqlExpression* repart_func, const ObSqlExpression* repart_sub_func,
      const ObIArray<ObTransmitRepartColumn>* repart_columns,
      const ObIArray<ObTransmitRepartColumn>* repart_sub_columns, ObPQDistributeMethod::Type unmatch_row_dist_method,
      const ObPxPartChInfo& part_ch_info, ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        exec_ctx_(exec_ctx),
        table_schema_(table_schema),
        repart_func_(repart_func),
        repart_sub_func_(repart_sub_func),
        repart_columns_(repart_columns),
        repart_sub_columns_(repart_sub_columns),
        calc_part_id_expr_(NULL),
        unmatch_row_dist_method_(unmatch_row_dist_method),
        partition_id_(OB_INVALID_INDEX_INT64),
        round_robin_idx_(0),
        part_ch_info_(part_ch_info),
        repart_type_(repart_type)
  {}

  ObRepartSliceIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      ObExpr* calc_part_id_expr, ObPQDistributeMethod::Type unmatch_row_dist_method, const ObPxPartChInfo& part_ch_info,
      ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        exec_ctx_(exec_ctx),
        table_schema_(table_schema),
        repart_func_(NULL),
        repart_sub_func_(NULL),
        repart_columns_(NULL),
        repart_sub_columns_(NULL),
        calc_part_id_expr_(calc_part_id_expr),
        unmatch_row_dist_method_(unmatch_row_dist_method),
        round_robin_idx_(0),
        part_ch_info_(part_ch_info),
        repart_type_(repart_type)
  {}

  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> PartId2ArrayIdxMap;
  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> SubPartId2ArrayIdxMap;

  virtual ~ObRepartSliceIdxCalc()
  {
    // destory map
    if (part_id_to_part_array_idx_.created()) {
      part_id_to_part_array_idx_.destroy();
    }
    if (part_idx_to_part_id_.created()) {
      part_idx_to_part_id_.destroy();
    }
    if (subpart_id_to_subpart_array_idx_.created()) {
      subpart_id_to_subpart_array_idx_.destroy();
    }
    if (subpart_idx_to_subpart_id_.created()) {
      subpart_idx_to_subpart_id_.destroy();
    }
  }
  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx) override;

  virtual int get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx) override;

  virtual int get_partition_id(ObEvalCtx& eval_ctx, int64_t& partition_id);
  virtual int get_partition_id(const common::ObNewRow& row, int64_t& partition_id);

  virtual int get_previous_row_partition_id(ObObj& partition_id) override;

  int init_partition_cache_map();

  virtual int init();

  virtual int destroy()
  {
    int ret = OB_SUCCESS;
    if (px_repart_ch_map_.created()) {
      ret = px_repart_ch_map_.destroy();
    }
    return ret;
  }

  int build_repart_ch_map(ObPxPartChMap& map);

private:
  // this is a trick!
  // get part id from hashmap, implicate that only one level-1 part in the map
  virtual int get_part_id_by_one_level_sub_ch_map(int64_t& part_id);
  virtual int get_sub_part_id_by_one_level_first_ch_map(const int64_t part_id, int64_t& sub_part_id);
  int init_cache_map(hash::ObHashMap<int64_t, int64_t, hash::NoPthreadDefendMode>& map);

protected:
  ObExecContext& exec_ctx_;
  const share::schema::ObTableSchema& table_schema_;
  const ObSqlExpression* repart_func_;
  const ObSqlExpression* repart_sub_func_;
  const ObIArray<ObTransmitRepartColumn>* repart_columns_;
  const ObIArray<ObTransmitRepartColumn>* repart_sub_columns_;
  ObExpr* calc_part_id_expr_;
  ObPQDistributeMethod::Type unmatch_row_dist_method_;
  int64_t partition_id_;
  int64_t round_robin_idx_;
  const ObPxPartChInfo& part_ch_info_;
  ObPxPartChMap px_repart_ch_map_;

  const static int64_t DEFAULT_CACHE_MAP_BUCKET_NUM = 64;
  PartId2ArrayIdxMap part_id_to_part_array_idx_;
  ObShuffleService::PartIdx2PartIdMap part_idx_to_part_id_;
  SubPartId2ArrayIdxMap subpart_id_to_subpart_array_idx_;
  ObShuffleService::SubPartIdx2SubPartIdMap subpart_idx_to_subpart_id_;
  ObRepartitionType repart_type_;
};

class ObSlaveMapRepartIdxCalcBase : public ObRepartSliceIdxCalc {
protected:
  ObSlaveMapRepartIdxCalcBase(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const ObSqlExpression* repart_func, const ObSqlExpression* repart_sub_func,
      const ObIArray<ObTransmitRepartColumn>* repart_columns,
      const ObIArray<ObTransmitRepartColumn>* repart_sub_columns, ObPQDistributeMethod::Type unmatch_row_dist_method,
      const ObPxPartChInfo& part_ch_info, ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObRepartSliceIdxCalc(exec_ctx, table_schema, repart_func, repart_sub_func, repart_columns, repart_sub_columns,
            unmatch_row_dist_method, part_ch_info, repart_type)
  {}
  ObSlaveMapRepartIdxCalcBase(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      ObExpr* calc_part_id_expr, ObPQDistributeMethod::Type unmatch_row_dist_method, const ObPxPartChInfo& part_ch_info,
      ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObRepartSliceIdxCalc(
            exec_ctx, table_schema, calc_part_id_expr, unmatch_row_dist_method, part_ch_info, repart_type)
  {}
  ~ObSlaveMapRepartIdxCalcBase() = default;

protected:
  virtual int init() override;
  virtual int destroy() override;

protected:
  typedef common::ObSEArray<int64_t, 8> TaskIdxArray;
  typedef common::hash::ObHashMap<int64_t, TaskIdxArray, common::hash::NoPthreadDefendMode> PartId2TaskIdxArrayMap;

  PartId2TaskIdxArrayMap part_to_task_array_map_;
};

class ObRepartRandomSliceIdxCalc : public ObSlaveMapRepartIdxCalcBase {
public:
  ObRepartRandomSliceIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const ObSqlExpression* repart_func, const ObSqlExpression* repart_sub_func,
      const ObIArray<ObTransmitRepartColumn>* repart_columns,
      const ObIArray<ObTransmitRepartColumn>* repart_sub_columns, ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObPxPartChInfo& part_ch_info, ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObSlaveMapRepartIdxCalcBase(exec_ctx, table_schema, repart_func, repart_sub_func, repart_columns,
            repart_sub_columns, unmatch_row_dist_method, part_ch_info, repart_type)
  {}
  ObRepartRandomSliceIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      ObExpr* calc_part_id_expr, ObPQDistributeMethod::Type unmatch_row_dist_method, const ObPxPartChInfo& part_ch_info,
      ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObSlaveMapRepartIdxCalcBase(
            exec_ctx, table_schema, calc_part_id_expr, unmatch_row_dist_method, part_ch_info, repart_type)
  {}

  ~ObRepartRandomSliceIdxCalc() = default;

  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx) override;
  virtual int get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx) override;
  virtual int init() override;
  virtual int destroy() override;

private:
  int get_task_idx_by_partition_id(int64_t partition_id, int64_t& task_idx);
};

class ObAffinitizedRepartSliceIdxCalc : public ObRepartSliceIdxCalc {
public:
  ObAffinitizedRepartSliceIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const ObSqlExpression* repart_func, const ObSqlExpression* repart_sub_func,
      const ObIArray<ObTransmitRepartColumn>* repart_columns,
      const ObIArray<ObTransmitRepartColumn>* repart_sub_columns, ObPQDistributeMethod::Type unmatch_row_dist_method,
      int64_t task_count, ObPxPartChInfo& part_ch_info, ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObRepartSliceIdxCalc(exec_ctx, table_schema, repart_func, repart_sub_func, repart_columns, repart_sub_columns,
            unmatch_row_dist_method, part_ch_info, repart_type),
        task_count_(task_count)
  {}

  ObAffinitizedRepartSliceIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      ObExpr* calc_part_id_expr, int64_t task_count, const ObPxPartChInfo& part_ch_info,
      ObPQDistributeMethod::Type unmatch_row_dist_method, ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObRepartSliceIdxCalc(
            exec_ctx, table_schema, calc_part_id_expr, unmatch_row_dist_method, part_ch_info, repart_type),
        task_count_(task_count)
  {}

  ~ObAffinitizedRepartSliceIdxCalc() = default;
  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx) override;

  virtual int get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx) override;

private:
  const int64_t task_count_;
};

class ObSlaveMapBcastIdxCalc : virtual public ObRepartSliceIdxCalc {
public:
  ObSlaveMapBcastIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const ObSqlExpression* repart_func, const ObSqlExpression* repart_sub_func,
      const ObIArray<ObTransmitRepartColumn>* repart_columns,
      const ObIArray<ObTransmitRepartColumn>* repart_sub_columns, ObPQDistributeMethod::Type unmatch_row_dist_method,
      int64_t task_count, const ObPxPartChInfo& part_ch_info, ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObRepartSliceIdxCalc(exec_ctx, table_schema, repart_func, repart_sub_func, repart_columns, repart_sub_columns,
            unmatch_row_dist_method, part_ch_info, repart_type),
        task_count_(task_count)
  {}
  ~ObSlaveMapBcastIdxCalc() = default;

  virtual int get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array);

protected:
  const int64_t task_count_;
};

class ObBc2HostSliceIdCalc : public ObMultiSliceIdxCalc {
public:
  struct HostIndex {
    HostIndex() : begin_(0), end_(0), idx_(0)
    {}
    TO_STRING_KV(K(begin_), K(end_), K(idx_));

    uint64_t begin_;
    uint64_t end_;
    mutable uint64_t idx_;
  };

  typedef common::ObSEArray<int64_t, DEFAULT_CHANNEL_CNT> ChannelIdxArray;
  typedef common::ObSEArray<HostIndex, DEFAULT_CHANNEL_CNT> HostIdxArray;

  ObBc2HostSliceIdCalc(common::ObIAllocator& alloc, const ChannelIdxArray& channel_idx, const HostIdxArray& host_idx)
      : ObMultiSliceIdxCalc(alloc), channel_idx_(channel_idx), host_idx_(host_idx)
  {}

  virtual int get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array) override;
  virtual int get_slice_indexes(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, SliceIdxArray& slice_idx_array);

private:
  const ChannelIdxArray& channel_idx_;
  const HostIdxArray& host_idx_;
};

class ObRandomSliceIdCalc : public ObSliceIdxCalc {
public:
  ObRandomSliceIdCalc(common::ObIAllocator& alloc, const uint64_t slice_cnt)
      : ObSliceIdxCalc(alloc), idx_(0), slice_cnt_(slice_cnt)
  {}

  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx) override;
  virtual int get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx) override;

private:
  uint64_t idx_;
  uint64_t slice_cnt_;
};

class ObBroadcastSliceIdCalc : public ObMultiSliceIdxCalc {
public:
  ObBroadcastSliceIdCalc(common::ObIAllocator& alloc, uint64_t slice_cnt)
      : ObMultiSliceIdxCalc(alloc), slice_cnt_(slice_cnt)
  {}

  virtual int get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array) override;
  virtual int get_slice_indexes(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, SliceIdxArray& slice_idx_array);

private:
  uint64_t slice_cnt_;
};

class ObHashSliceIdCalc : virtual public ObSliceIdxCalc {
public:
  ObHashSliceIdCalc(ObIAllocator& alloc, common::ObExprCtx& expr_ctx,
      const common::ObIArray<ObHashColumn>& hash_dist_columns, const common::ObIArray<ObSqlExpression*>& dist_exprs,
      const int64_t task_cnt)
      : ObSliceIdxCalc(alloc),
        expr_ctx_(&expr_ctx),
        hash_dist_columns_(&hash_dist_columns),
        dist_exprs_(&dist_exprs),
        task_cnt_(task_cnt),
        obj_casted_(false),
        hash_dist_exprs_(NULL),
        hash_funcs_(NULL)
  {}

  ObHashSliceIdCalc(ObIAllocator& alloc, const int64_t task_cnt, const ObIArray<ObExpr*>* dist_exprs,
      const ObIArray<ObHashFunc>* hash_funcs)
      : ObSliceIdxCalc(alloc),
        expr_ctx_(NULL),
        hash_dist_columns_(NULL),
        dist_exprs_(NULL),
        task_cnt_(task_cnt),
        obj_casted_(false),
        hash_dist_exprs_(dist_exprs),
        hash_funcs_(hash_funcs)
  {}

  uint64_t obj_hash_value(const ObObj& obj, const ObCollationType cs_type, uint64_t hash_val);
  int calc_hash_value(const ObObj& obj, const ObObjType type, const ObCollationType cs_type, uint64_t& hash_val);
  int calc_hash_value(ObEvalCtx& eval_ctx, uint64_t& hash_val);
  int get_multi_hash_value(const ObNewRow& row, uint64_t& hash_val);
  virtual int get_slice_idx(const ObNewRow& row, int64_t& slice_idx);
  int get_slice_idx(const ObIArray<ObExpr*>& row, ObEvalCtx& eval_ctx, int64_t& slice_idx) override;

  common::ObExprCtx* expr_ctx_;
  const common::ObIArray<ObHashColumn>* hash_dist_columns_;
  const common::ObIArray<ObSqlExpression*>* dist_exprs_;
  const int64_t task_cnt_;
  // need to reset calc buffer if object casted.
  bool obj_casted_;

  const ObIArray<ObExpr*>* hash_dist_exprs_;
  const ObIArray<ObHashFunc>* hash_funcs_;
};

class ObSlaveMapPkeyHashIdxCalc : public ObSlaveMapRepartIdxCalcBase, public ObHashSliceIdCalc {
public:
  ObSlaveMapPkeyHashIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const ObSqlExpression* repart_func, const ObSqlExpression* repart_sub_func,
      const ObIArray<ObTransmitRepartColumn>* repart_columns,
      const ObIArray<ObTransmitRepartColumn>* repart_sub_columns, ObPQDistributeMethod::Type unmatch_row_dist_method,
      int64_t task_count, const ObPxPartChInfo& part_ch_info, common::ObExprCtx& expr_ctx,
      const common::ObIArray<ObHashColumn>& hash_dist_columns, const common::ObIArray<ObSqlExpression*>& dist_exprs,
      ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObSlaveMapRepartIdxCalcBase(exec_ctx, table_schema, repart_func, repart_sub_func, repart_columns,
            repart_sub_columns, unmatch_row_dist_method, part_ch_info, repart_type),
        ObHashSliceIdCalc(exec_ctx.get_allocator(), expr_ctx, hash_dist_columns, dist_exprs, task_count),
        affi_hash_map_()

  {}
  ObSlaveMapPkeyHashIdxCalc(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      ObExpr* calc_part_id_expr, ObPQDistributeMethod::Type unmatch_row_dist_method, const ObPxPartChInfo& part_ch_info,
      int64_t task_count, const ExprFixedArray& dist_exprs, const common::ObHashFuncs& dist_hash_funcs,
      ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator()),
        ObSlaveMapRepartIdxCalcBase(
            exec_ctx, table_schema, calc_part_id_expr, unmatch_row_dist_method, part_ch_info, repart_type),
        ObHashSliceIdCalc(exec_ctx.get_allocator(), task_count, &dist_exprs, &dist_hash_funcs),
        affi_hash_map_()
  {}
  ~ObSlaveMapPkeyHashIdxCalc() = default;

  int init() override;
  int destroy() override;

  // for classic engine
  virtual int get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx) override;
  // for static engine
  virtual int get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx) override;

private:
  virtual int get_part_id_by_one_level_sub_ch_map(int64_t& part_id) override;
  virtual int get_sub_part_id_by_one_level_first_ch_map(const int64_t part_id, int64_t& sub_part_id) override;
  int get_task_idx_by_partition_id(ObEvalCtx& eval_ctx, int64_t partition_id, int64_t& task_idx);
  int build_affi_hash_map(hash::ObHashMap<int64_t, ObPxPartChMapItem>& affi_hash_map);

private:
  hash::ObHashMap<int64_t, ObPxPartChMapItem> affi_hash_map_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
