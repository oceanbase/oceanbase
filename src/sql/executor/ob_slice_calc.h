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

#include "sql/executor/ob_task_event.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/executor/ob_shuffle_service.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{


namespace share
{
namespace schema
{
  class ObTableSchema;
}
}

namespace sql
{
class ObExecContext;

struct ObHashColumn : public common::ObColumnInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObHashColumn() : expr_idx_(OB_INVALID_INDEX), cmp_type_(common::ObNullType)
  {
  }

  INHERIT_TO_STRING_KV("col_info", ObColumnInfo,
      K_(expr_idx), K_(cmp_type));

  int64_t expr_idx_;
  common::ObObjType cmp_type_;
};

class ObSliceIdxCalc
{
public:
  static const uint64_t SLICE_CALC_HASH_SEED = 98764321261;
  static const int64_t DEFAULT_CHANNEL_CNT = 64;
  static const int64_t DEFAULT_BATCH_SIZE = 256;
  static const int64_t DEFAULT_CHANNEL_IDX_TO_DROP_ROW = -2;

  /*
  *  SliceIdxArray : used for row->channel in 1 to 1 and 1 to n, but only one row
  *    for example:
  *    if row0 to channel2, row1 to channel0, row2 to channel1
  *      the result of SliceIdxArray will be: [2,0,1].
  *    if row0 to channel1, SliceIdxArray will be: [1].
  *  SliceIdxFlattenArray + EndIdxArray : used for row->channel in 1 to 1 and 1 to n,
  *    not only one row. [end_indexes[i-1] , end_indexes[i]) will be the index
  *   (in SliceIdxFlattenArray) range that rowi will be send to.
  *    for example:
  *    if row0->channel0,1,2; row1->channel2;row2->channel2,3
  *      SliceIdxArray will be [0,1,2,2,2,3],
  *      EndIdxArray will be [3,4,6].
  */
  typedef common::ObSEArray<int64_t, DEFAULT_CHANNEL_CNT> SliceIdxArray;
  typedef common::ObSEArray<int64_t, DEFAULT_BATCH_SIZE> SliceIdxFlattenArray;
  typedef common::ObSEArray<int64_t, DEFAULT_BATCH_SIZE> EndIdxArray;
  enum SliceCalcType {
    ALL_TO_ONE,//ObAllToOneSliceIdxCalc
    SM_REPART_RANDOM,//ObSlaveMapPkeyRandomIdxCalc
    SM_REPART_HASH,//ObSlaveMapPkeyHashIdxCalc
    SM_REPART_RANGE,//ObSlaveMapPkeyRangeIdxCalc
    AFFINITY_REPART,//ObAffinitizedRepartSliceIdxCalc
    NULL_AWARE_AFFINITY_REPART,//ObNullAwareAffinitizedRepartSliceIdxCalc
    SM_BROADCAST,//ObSlaveMapBcastIdxCalc
    BC2HOST,//ObBc2HostSliceIdCalc
    RANDOM,//ObRandomSliceIdCalc
    BROADCAST,//ObBroadcastSliceIdCalc
    RANGE,//ObRangeSliceIdCalc
    HASH,//ObHashSliceIdCalc
    NULL_AWARE_HASH,//ObNullAwareHashSliceIdCalc
    HYBRID_HASH_BROADCAST,//ObHybridHashBroadcastSliceIdCalc
    HYBRID_HASH_RANDOM,//ObHybridHashRandomSliceIdCalc
    WF_HYBRID,//ObWfHybridDistSliceIdCalc
  };
  explicit ObSliceIdxCalc(common::ObIAllocator &allocator,
                          ObNullDistributeMethod::Type null_row_dist_method)
      : support_vectorized_calc_(false),
      is_multi_slice_calc_type_(false),
      alloc_(allocator),
      shuffle_service_(allocator),
      slice_indexes_(NULL),
      tablet_ids_(nullptr),
      is_first_row_(true),
      null_row_dist_method_(null_row_dist_method)
  {}
  virtual ~ObSliceIdxCalc() = default;
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE, bool USE_VEC>
  int get_slice_indexes(
    const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
    ObBitVector *skip = NULL);
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE, bool USE_VEC>
  int get_slice_idx_batch(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                                ObBitVector &skip, const int64_t batch_size,
                                int64_t *&indexes);
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
  int get_multi_slice_idx_vector(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                          ObBitVector &skip, const int64_t batch_size,
                          SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes);
  // calculate a group of indexes for one row.
  template <bool USE_VEC>
  int get_slice_indexes_inner(
    const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  // 获取前一次调用 get_slice_indexes 时传入的 row 对应的目标 partition
  // 本接口目前仅用于 ObRepartSliceIdxCalc 和 ObAffinitizedRepartSliceIdxCalc
  // 计算出的 tablet_id 用于告诉目标算子当前处理的行属于哪个分区
  virtual int get_previous_row_tablet_id(ObObj &tablet_id);

  // support vectorized slice indexes calculation.
  bool support_vectorized_calc() const { return support_vectorized_calc_; }
  bool is_multi_slice_calc_type() const { return is_multi_slice_calc_type_; }
  virtual void set_calc_hash_keys(int64_t n_keys) { UNUSED(n_keys); }
  // Calculate slice index vector for batch rows.
  // The function is called only support_vectorized_calc() is true.
  // This interface is for vectorization 1.0 which is row-oriented.
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                                ObBitVector &skip, const int64_t batch_size,
                                int64_t *&indexes)
  {
    UNUSEDx(exprs, eval_ctx, skip, batch_size, indexes);
    return common::OB_NOT_SUPPORTED;
  }
  int get_multi_slice_idx_vector_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                                ObBitVector &skip, const int64_t batch_size,
                                SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes)
  {
    UNUSEDx(exprs, eval_ctx, skip, batch_size, slice_flatten_indexes, end_indexes);
    return common::OB_NOT_SUPPORTED;
  }
protected:
  // used when calculate index id for single row. To make count of slice_idx_array = 1
  int setup_slice_index(SliceIdxArray &slice_idx_array);
  // used in vectorized execution, allocate memory to initialize slice_indexes_.
  int setup_slice_indexes(ObEvalCtx &ctx);
  int setup_tablet_ids(ObEvalCtx &ctx);
  // used for null aware anti join
  template <bool USE_VEC>
  int calc_for_null_aware(const ObExpr &expr, const int64_t task_cnt, ObEvalCtx &eval_ctx,
                          SliceIdxArray &slice_idx_array, bool &processed, ObBitVector *skip);
  bool support_vectorized_calc_;
  bool is_multi_slice_calc_type_;
  common::ObIAllocator &alloc_;
  ObShuffleService shuffle_service_;
  int64_t *slice_indexes_;
  int64_t *tablet_ids_;
  // used by null aware hash join
  bool is_first_row_;
  ObNullDistributeMethod::Type null_row_dist_method_;
};

// For transmit which need send one row to more than one channel. (e.g.: broadcast)
class ObMultiSliceIdxCalc : public ObSliceIdxCalc
{
public:
  ObMultiSliceIdxCalc(common::ObIAllocator &alloc,
                      ObNullDistributeMethod::Type null_row_dist_method)
      : ObSliceIdxCalc(alloc, null_row_dist_method) {}
};

// 专门针对顶层 DFO，给 QC 传数据用，多对一
class ObAllToOneSliceIdxCalc : public ObSliceIdxCalc
{
public:
  ObAllToOneSliceIdxCalc(common::ObIAllocator &alloc)
      : ObSliceIdxCalc(alloc, ObNullDistributeMethod::NONE)
  {
    support_vectorized_calc_ = true;
  }
  virtual ~ObAllToOneSliceIdxCalc() = default;
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                        ObBitVector &skip, const int64_t batch_size,
                        int64_t *&indexes);
protected:
};

class ObRepartSliceIdxCalc : public ObSliceIdxCalc
{
public:
  ObRepartSliceIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const ObSqlExpression *repart_func,
      const ObSqlExpression *repart_sub_func,
      const ObIArray<ObTransmitRepartColumn> *repart_columns,
      const ObIArray<ObTransmitRepartColumn> *repart_sub_columns,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      const ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator(), null_row_dist_method),
        exec_ctx_(exec_ctx),
        table_schema_(table_schema),
        repart_func_(repart_func),
        repart_sub_func_(repart_sub_func),
        repart_columns_(repart_columns),
        repart_sub_columns_(repart_sub_columns),
        calc_part_id_expr_(NULL),
        unmatch_row_dist_method_(unmatch_row_dist_method),
        tablet_id_(OB_INVALID_INDEX_INT64),
        round_robin_idx_(0),
        part_ch_info_(part_ch_info),
        repart_type_(repart_type)
  {
  }

  ObRepartSliceIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      const ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObSliceIdxCalc(exec_ctx.get_allocator(), null_row_dist_method),
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
        repart_type_(repart_type),
        part2tablet_id_map_()
  {
  }

  typedef common::hash::ObHashMap<int64_t, int64_t,
                          common::hash::NoPthreadDefendMode> PartId2ArrayIdxMap;
  typedef common::hash::ObHashMap<int64_t, int64_t,
                          common::hash::NoPthreadDefendMode> SubPartId2ArrayIdxMap;

  virtual ~ObRepartSliceIdxCalc() {}

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);

  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &, ObEvalCtx &eval_ctx,
                                ObBitVector &skip, const int64_t batch_size,
                                int64_t *&indexes);
  template <bool USE_VEC>
  int get_tablet_id(ObEvalCtx &eval_ctx, int64_t &tablet_id, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_tablet_ids(ObEvalCtx &eval_ctx, ObBitVector &skip,
                                const int64_t batch_size, int64_t *&tablet_ids);
  virtual int get_previous_row_tablet_id(ObObj &tablet_id) override;

  int init_partition_cache_map();

  virtual int init(uint64_t tenant_id = OB_SERVER_TENANT_ID);

  virtual int destroy() {
    int ret = OB_SUCCESS;
    if (px_repart_ch_map_.created()) {
      ret = px_repart_ch_map_.destroy();
    }
    return ret;
  }

  int build_repart_ch_map(ObPxPartChMap &map, uint64_t tenant_id);
  int build_part2tablet_id_map();
  int64_t get_last_tablet_id() { return tablet_id_; }
  int64_t *get_tablet_ids_ptr() { return tablet_ids_; }
  ObExpr *get_calc_part_id_expr() { return calc_part_id_expr_; }
protected:
  // this is a trick!
  // get part id from hashmap, implicate that only one level-1 part in the map
  // reference to
  virtual int get_part_id_by_one_level_sub_ch_map(int64_t &part_id);
  virtual int get_sub_part_id_by_one_level_first_ch_map(
    const int64_t part_id, int64_t &sub_part_id);
private:
  // for skip subpart
  typedef common::hash::ObHashMap<int64_t, int64_t,
      common::hash::NoPthreadDefendMode> ObPxPart2TabletIdMap;
  int setup_one_side_one_level_info();
protected:
  ObExecContext &exec_ctx_;
  const share::schema::ObTableSchema &table_schema_;
  const ObSqlExpression *repart_func_;
  const ObSqlExpression *repart_sub_func_;
  const ObIArray<ObTransmitRepartColumn> *repart_columns_;
  const ObIArray<ObTransmitRepartColumn> *repart_sub_columns_;
  ObExpr *calc_part_id_expr_;
  ObPQDistributeMethod::Type unmatch_row_dist_method_;
  int64_t tablet_id_;
  int64_t round_robin_idx_;
  const ObPxPartChInfo &part_ch_info_;
  ObPxPartChMap px_repart_ch_map_;
  ObRepartitionType repart_type_;
  ObPxPart2TabletIdMap part2tablet_id_map_;
};

// 作为 pkey+hash、pkey+random、pkey+range 等的父类
// 提取了一些他们的共用逻辑到基类，如 init 方法
class ObSlaveMapRepartIdxCalcBase : public ObRepartSliceIdxCalc
{
protected:
  ObSlaveMapRepartIdxCalcBase(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const ObSqlExpression *repart_func,
      const ObSqlExpression *repart_sub_func,
      const ObIArray<ObTransmitRepartColumn> *repart_columns,
      const ObIArray<ObTransmitRepartColumn> *repart_sub_columns,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      const ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObRepartSliceIdxCalc(exec_ctx,
                             table_schema,
                             repart_func,
                             repart_sub_func,
                             repart_columns,
                             repart_sub_columns,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type)
  {}
  ObSlaveMapRepartIdxCalcBase(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      const ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObRepartSliceIdxCalc(exec_ctx,
                             table_schema,
                             calc_part_id_expr,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type)
  {}
  ~ObSlaveMapRepartIdxCalcBase() = default;
protected:
  virtual int init(uint64_t tenant_id = OB_SERVER_TENANT_ID) override;
  virtual int destroy() override;
protected:
  // 存储同一个partition所对应的所有task id
  typedef sql::ObTMArray<int64_t> TaskIdxArray;
  // pkey random情况下：数据可以发送到对应partition所在的SQC的任意一个task上，因此每一个partition都对应着
  // 一组task id
  // key: tablet_id
  // value: task_ids
  typedef common::hash::ObHashMap<int64_t, TaskIdxArray,
                          common::hash::NoPthreadDefendMode> PartId2TaskIdxArrayMap;

  // pkey - random/hash/range 等情况下，每一个partition可以被其所在的SQC的特定task处理
  // 对于 random，随机选task
  // 对于 hash，用 hash 值定位 task
  // 对于 range，用 range 值定位 task
  PartId2TaskIdxArrayMap part_to_task_array_map_;
};

class ObSlaveMapPkeyRandomIdxCalc : public ObSlaveMapRepartIdxCalcBase
{
public:
  ObSlaveMapPkeyRandomIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const ObSqlExpression *repart_func,
      const ObSqlExpression *repart_sub_func,
      const ObIArray<ObTransmitRepartColumn> *repart_columns,
      const ObIArray<ObTransmitRepartColumn> *repart_sub_columns,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObSlaveMapRepartIdxCalcBase(
                             exec_ctx,
                             table_schema,
                             repart_func,
                             repart_sub_func,
                             repart_columns,
                             repart_sub_columns,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type)
  {
    support_vectorized_calc_ = true;
  }
  ObSlaveMapPkeyRandomIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      const ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObSlaveMapRepartIdxCalcBase(
                             exec_ctx,
                             table_schema,
                             calc_part_id_expr,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type)
  {
    support_vectorized_calc_ = true;
  }

  ~ObSlaveMapPkeyRandomIdxCalc() = default;

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &, ObEvalCtx &eval_ctx,
                              ObBitVector &skip, const int64_t batch_size,
                              int64_t *&indexes);
  virtual int init(uint64_t tenant_id) override;
  virtual int destroy() override;
private:
  int get_task_idx_by_tablet_id(int64_t tablet_id , int64_t &task_idx);
};

class ObAffinitizedRepartSliceIdxCalc : public ObRepartSliceIdxCalc
{
public:
  ObAffinitizedRepartSliceIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const ObSqlExpression *repart_func,
      const ObSqlExpression *repart_sub_func,
      const ObIArray<ObTransmitRepartColumn> *repart_columns,
      const ObIArray<ObTransmitRepartColumn> *repart_sub_columns,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      int64_t task_count,
      ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type,
      bool can_fast_calc_hash)
      : ObRepartSliceIdxCalc(exec_ctx,
                             table_schema,
                             repart_func,
                             repart_sub_func,
                             repart_columns,
                             repart_sub_columns,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type),
                             task_count_(task_count),
                             fast_calc_hash_slice_(can_fast_calc_hash)
  {
    support_vectorized_calc_ = true;
  }

  ObAffinitizedRepartSliceIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      int64_t task_count,
      const ObPxPartChInfo &part_ch_info,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      ObRepartitionType repart_type,
      const ObIArray<ObExpr*> *hash_dist_exprs,
      const ObIArray<ObHashFunc> *hash_funcs,
      bool can_fast_calc_hash)
      : ObRepartSliceIdxCalc(exec_ctx,
                             table_schema,
                             calc_part_id_expr,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type),
                             task_count_(task_count),
                             hash_dist_exprs_(hash_dist_exprs),
                             hash_funcs_(hash_funcs),
                             fast_calc_hash_slice_(can_fast_calc_hash)
  {
    support_vectorized_calc_ = true;
  }

  ~ObAffinitizedRepartSliceIdxCalc() = default;

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &, ObEvalCtx &eval_ctx,
                              ObBitVector &skip, const int64_t batch_size,
                              int64_t *&indexes);
protected:
  const int64_t task_count_;
  //const common::ObIArray<ObHashColumn> *hash_dist_columns_;
  //const common::ObIArray<ObSqlExpression *> *dist_exprs_;
  const ObIArray<ObExpr*> *hash_dist_exprs_;
  const ObIArray<ObHashFunc> *hash_funcs_;
  bool fast_calc_hash_slice_;
};

class ObSlaveMapBcastIdxCalc : public ObRepartSliceIdxCalc
{
public:
  ObSlaveMapBcastIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const ObSqlExpression *repart_func,
      const ObSqlExpression *repart_sub_func,
      const ObIArray<ObTransmitRepartColumn> *repart_columns,
      const ObIArray<ObTransmitRepartColumn> *repart_sub_columns,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      int64_t task_count,
      const ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObRepartSliceIdxCalc(exec_ctx,
                             table_schema,
                             repart_func,
                             repart_sub_func,
                             repart_columns,
                             repart_sub_columns,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type),
                             task_count_(task_count)
  {}
  ObSlaveMapBcastIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      int64_t task_count,
      const ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type)
      : ObRepartSliceIdxCalc(exec_ctx,
                             table_schema,
                             calc_part_id_expr,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type),
                             task_count_(task_count)
  {}
  ~ObSlaveMapBcastIdxCalc() = default;

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
protected:
  const int64_t task_count_;
};

class ObBc2HostSliceIdCalc : public ObMultiSliceIdxCalc
{
public:
  struct HostIndex {
    HostIndex() : begin_(0), end_(0), idx_(0) {}
    TO_STRING_KV(K(begin_), K(end_), K(idx_));

    uint64_t begin_;
    uint64_t end_;
    mutable uint64_t idx_;
  };

  typedef common::ObSEArray<int64_t, DEFAULT_CHANNEL_CNT> ChannelIdxArray;
  typedef common::ObSEArray<HostIndex, DEFAULT_CHANNEL_CNT> HostIdxArray;

  ObBc2HostSliceIdCalc(common::ObIAllocator &alloc,
                       const ChannelIdxArray &channel_idx,
                       const HostIdxArray &host_idx,
                       ObNullDistributeMethod::Type null_row_dist_method)
      : ObMultiSliceIdxCalc(alloc, null_row_dist_method), channel_idx_(channel_idx), host_idx_(host_idx)
  {
    support_vectorized_calc_ = true;
    is_multi_slice_calc_type_ = true;
  }

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    int64_t *&indexes);
  int get_multi_slice_idx_vector_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes);
private:
  const ChannelIdxArray &channel_idx_;
  const HostIdxArray &host_idx_;
};

class ObRandomSliceIdCalc : public ObSliceIdxCalc
{
public:
  ObRandomSliceIdCalc(common::ObIAllocator &alloc, const uint64_t slice_cnt)
      : ObSliceIdxCalc(alloc, ObNullDistributeMethod::NONE), idx_(0), slice_cnt_(slice_cnt)
  {
    support_vectorized_calc_ = true;
  }

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    int64_t *&indexes);
private:
  uint64_t idx_;
  uint64_t slice_cnt_;
};

class ObBroadcastSliceIdCalc : public ObMultiSliceIdxCalc
{
public:
  ObBroadcastSliceIdCalc(common::ObIAllocator &alloc,
                         uint64_t slice_cnt,
                         ObNullDistributeMethod::Type null_row_dist_method)
      : ObMultiSliceIdxCalc(alloc, null_row_dist_method), slice_cnt_(slice_cnt)
  {
    support_vectorized_calc_ = true;
    is_multi_slice_calc_type_ = true;
  }

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);

  int get_multi_slice_idx_vector_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes);
private:
  uint64_t slice_cnt_;
};

class ObRangeSliceIdCalc : public ObSliceIdxCalc
{
   struct Compare
  {
  public:
     explicit Compare(const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
                      const ObIArray<ObSortFieldCollation> *sort_collations)
       : ret_(common::OB_SUCCESS),
         sort_cmp_funs_(sort_cmp_funs),
         sort_collations_(sort_collations)
    {}
     bool operator()(const ObPxTabletRange::DatumKey &l,
                    const ObPxTabletRange::DatumKey &r);
  public:
     int ret_;
     const ObIArray<ObSortCmpFunc> *sort_cmp_funs_;
     const ObIArray<ObSortFieldCollation> *sort_collations_;
  };
public:
  ObRangeSliceIdCalc(ObIAllocator &alloc,
      const int64_t task_cnt,
      const ObPxTabletRange *range,
      const ObIArray<ObExpr*> *dist_exprs,
      const ObSortFuncs &sort_cmp_funs,
      const ObSortCollations &sort_collations,
      ObExpr *ddl_slice_id_expr)
      : ObSliceIdxCalc(alloc, ObNullDistributeMethod::NONE),
        task_cnt_(task_cnt),
        range_(range),
        dist_exprs_(dist_exprs),
        sort_cmp_funs_(sort_cmp_funs),
        sort_collations_(sort_collations),
        ddl_slice_id_expr_(ddl_slice_id_expr)
  {
    support_vectorized_calc_ = true;
  }

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    int64_t *&indexes);
  template <bool USE_VEC>
  int get_slice_idx_batch_vec(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    int64_t *&indexes);
  int64 task_cnt_;
  const ObPxTabletRange *range_;
  const ObIArray<ObExpr*> *dist_exprs_;
  const ObSortFuncs &sort_cmp_funs_;
  const ObSortCollations &sort_collations_;
  ObExpr *ddl_slice_id_expr_;
};

class ObHashSliceIdCalc : public ObSliceIdxCalc
{
public:
  ObHashSliceIdCalc(ObIAllocator &alloc,
                    common::ObExprCtx  &expr_ctx,
                    ObNullDistributeMethod::Type null_row_dist_method,
                    const common::ObIArray<ObHashColumn> &hash_dist_columns,
                    const common::ObIArray<ObSqlExpression *> &dist_exprs,
                    const int64_t task_cnt,
                    bool can_fast_calc_hash)
      : ObSliceIdxCalc(alloc, null_row_dist_method), expr_ctx_(&expr_ctx),
        hash_dist_columns_(&hash_dist_columns), dist_exprs_(&dist_exprs), task_cnt_(task_cnt),
        round_robin_idx_(0), obj_casted_(false), hash_dist_exprs_(NULL), hash_funcs_(NULL),
        n_keys_(0), null_dist_value_exist_(true), null_bitmap_(nullptr), malloc_alloc_(nullptr),
        fast_calc_hash_slice_(can_fast_calc_hash)
  {
    support_vectorized_calc_ = true;
  }

  ObHashSliceIdCalc(ObIAllocator &alloc,
                    const int64_t task_cnt,
                    ObNullDistributeMethod::Type null_row_dist_method,
                    const ObIArray<ObExpr*> *dist_exprs,
                    const ObIArray<ObHashFunc> *hash_funcs,
                    bool can_fast_calc_hash)
      : ObSliceIdxCalc(alloc, null_row_dist_method), expr_ctx_(NULL),
        hash_dist_columns_(NULL), dist_exprs_(NULL), task_cnt_(task_cnt), round_robin_idx_(0),
        obj_casted_(false), hash_dist_exprs_(dist_exprs), hash_funcs_(hash_funcs),
        n_keys_(dist_exprs->count()), null_dist_value_exist_(true), null_bitmap_(nullptr), malloc_alloc_(nullptr),
        fast_calc_hash_slice_(can_fast_calc_hash)
  {
    support_vectorized_calc_ = true;
  }
  virtual ~ObHashSliceIdCalc()
  {
    if (nullptr != malloc_alloc_ && nullptr != null_bitmap_) {
      malloc_alloc_->free(null_bitmap_);
      null_bitmap_ = nullptr;
      malloc_alloc_ = nullptr;
    }
  }
  template <bool USE_VEC>
  int calc_hash_value(ObEvalCtx &eval_ctx, uint64_t &hash_val, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int calc_slice_idx(ObEvalCtx &eval_ctx, int64_t slice_size, int64_t &slice_idx,
                     ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  virtual void set_calc_hash_keys(int64_t n_keys) { n_keys_ = n_keys; }
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                        ObBitVector &skip, const int64_t batch_size,
                        int64_t *&indexes);
  template <bool USE_VEC>
  int get_slice_idx_batch_vec(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                                  ObBitVector &skip, const int64_t batch_size,
                                  int64_t *&indexes);
  OB_INLINE bool use_special_null_dist() const { return ObNullDistributeMethod::NONE != null_row_dist_method_; }
  OB_INLINE void fast_calc_slice_idx_batch(int64_t *slice_idxes, uint64_t *hash_vals, int64_t batch_size, int64_t task_cnt)
  {
    OB_ASSERT(task_cnt < UINT32_MAX);
    for (int64_t i = 0; i < batch_size; i++) {
      slice_idxes[i] = ((uint32_t)(hash_vals[i]) * task_cnt) >> 32;
    }
  }
  OB_INLINE void fast_calc_slice_idx(int64_t &slice_idx, uint64_t hash_val, int64_t task_cnt)
  {
    OB_ASSERT(task_cnt < UINT32_MAX);
    slice_idx = ((uint32_t)(hash_val) * task_cnt) >> 32;
  }
  common::ObExprCtx *expr_ctx_;
  const common::ObIArray<ObHashColumn> *hash_dist_columns_;
  const common::ObIArray<ObSqlExpression *> *dist_exprs_;
  const int64_t task_cnt_;
  int64_t round_robin_idx_;
  // need to reset calc buffer if object casted.
  bool obj_casted_;

  // for static typing engine.
  // 例如：murmurhash(c1+c2)，那么
  //  - hash_dist_exprs_ 里面存 c1+c2 表达式
  //  - hash_funcs 里面存 murmurhash 表达式
  const ObIArray<ObExpr*> *hash_dist_exprs_;
  const ObIArray<ObHashFunc> *hash_funcs_;
  int64_t n_keys_;
  bool null_dist_value_exist_;
  ObBitVector *null_bitmap_;
  ObIAllocator *malloc_alloc_;
  bool fast_calc_hash_slice_;
};

class ObHybridHashSliceIdCalcBase
{
public:
  ObHybridHashSliceIdCalcBase(common::ObIAllocator &alloc,
                              const int64_t slice_cnt,
                              ObNullDistributeMethod::Type null_row_dist_method,
                              const ObIArray<ObExpr*> *dist_exprs,
                              const ObIArray<ObHashFunc> *hash_funcs,
                              const ObIArray<uint64_t> *popular_values_hash,
                              bool can_fast_calc_hash)
      : hash_calc_(alloc, slice_cnt, null_row_dist_method, dist_exprs, hash_funcs, can_fast_calc_hash),
        popular_values_hash_(popular_values_hash),
        use_hash_lookup_(false)
  {
    int ret = OB_SUCCESS;
    if (popular_values_hash && popular_values_hash->count() > 3) {
      // popular value is not ususally not large. 2x the hash bucket size for better performance
      if (OB_FAIL(popular_values_map_.create(popular_values_hash->count() * 2, "PopValBkt", "PopValNode"))) {
        SQL_LOG(WARN, "fail create popular values map", K(ret), K(popular_values_hash->count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < popular_values_hash_->count(); ++i) {
          if (OB_FAIL(popular_values_map_.set_refactored(popular_values_hash_->at(i), 0))) {
            SQL_LOG(WARN, "fail init popular values map",
                    K(ret), K(i), K(popular_values_hash->count()));
          }
        }
      }
      if (OB_SUCC(ret)) {
        use_hash_lookup_ = true;
      }
    }
    //support_vectorized_calc_ = true;
  }
  ~ObHybridHashSliceIdCalcBase()
  {
    if (popular_values_map_.created()) {
      (void) popular_values_map_.destroy();
    }
  }
protected:
  template <bool USE_VEC>
  int check_if_popular_value(ObEvalCtx &eval_ctx, bool &is_popular, ObBitVector *skip);
  ObHashSliceIdCalc hash_calc_;
  const common::ObIArray<uint64_t> *popular_values_hash_;
  common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> popular_values_map_;
  bool use_hash_lookup_;
};

// broadcast side of px hybrid hash send
class ObHybridHashBroadcastSliceIdCalc : public ObHybridHashSliceIdCalcBase,
                                         public ObMultiSliceIdxCalc
{
public:
  ObHybridHashBroadcastSliceIdCalc(common::ObIAllocator &alloc,
                                   const int64_t slice_cnt,
                                   ObNullDistributeMethod::Type null_row_dist_method,
                                   const ObIArray<ObExpr*> *dist_exprs,
                                   const ObIArray<ObHashFunc> *hash_funcs,
                                   const ObIArray<uint64_t> *popular_values_hash,
                                   bool can_fast_calc_hash)
      : ObHybridHashSliceIdCalcBase(alloc, slice_cnt, null_row_dist_method, dist_exprs,
                                    hash_funcs, popular_values_hash, can_fast_calc_hash),
        ObMultiSliceIdxCalc(alloc, null_row_dist_method),
        broadcast_calc_(alloc, slice_cnt, null_row_dist_method)
  {
    support_vectorized_calc_ = true;
    is_multi_slice_calc_type_ = true;
  }
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  int get_multi_slice_idx_vector_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes);
private:
  ObBroadcastSliceIdCalc broadcast_calc_;
};

// random side of px hybrid hash send
class ObHybridHashRandomSliceIdCalc : public ObHybridHashSliceIdCalcBase,
                                      public ObSliceIdxCalc
{
public:
  ObHybridHashRandomSliceIdCalc(common::ObIAllocator &alloc,
                                const int64_t slice_cnt,
                                ObNullDistributeMethod::Type null_row_dist_method,
                                const ObIArray<ObExpr*> *dist_exprs,
                                const ObIArray<ObHashFunc> *hash_funcs,
                                const ObIArray<uint64_t> *popular_values_hash,
                                bool can_fast_calc_hash)
      : ObHybridHashSliceIdCalcBase(alloc, slice_cnt, null_row_dist_method, dist_exprs,
                                    hash_funcs, popular_values_hash, can_fast_calc_hash),
        ObSliceIdxCalc(alloc, null_row_dist_method),
        random_calc_(alloc, slice_cnt)
  {
    support_vectorized_calc_ = true;
  }
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                                  ObBitVector &skip, const int64_t batch_size,
                                  int64_t *&indexes);
private:
  ObRandomSliceIdCalc random_calc_;
};


class ObSlaveMapPkeyRangeIdxCalc : public ObSlaveMapRepartIdxCalcBase
{
public:
  ObSlaveMapPkeyRangeIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      const ObPxPartChInfo &part_ch_info,
      const ObIArray<ObExpr *> &sort_exprs,
      const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
      const ObIArray<ObSortFieldCollation> *sort_collations,
      ObRepartitionType repart_type,
      ObExpr *ddl_slice_id_expr)
      : ObSlaveMapRepartIdxCalcBase(exec_ctx,
                                    table_schema,
                                    calc_part_id_expr,
                                    unmatch_row_dist_method,
                                    null_row_dist_method,
                                    part_ch_info,
                                    repart_type),
        is_inited_(false),
        sort_exprs_(sort_exprs),
        sort_cmp_(sort_cmp_funs, sort_collations),
        ddl_slice_id_expr_(ddl_slice_id_expr)
  {}
  virtual ~ObSlaveMapPkeyRangeIdxCalc();
  virtual int init(uint64_t tenant_id = OB_SERVER_TENANT_ID) override;
  virtual int destroy() override;
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
private:
  struct PartitionRangeChannelInfo
  {
    PartitionRangeChannelInfo(common::ObIAllocator &allocator) : channels_(allocator) { }
    int64_t tablet_id_;
    ObPxTabletRange::RangeCut range_cut_;
    common::ObFixedArray<int64_t, common::ObIAllocator> channels_;

    TO_STRING_KV(K(tablet_id_), K(range_cut_), K(channels_));
  };
  struct Compare
  {
  public:
    Compare(const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
                     const ObIArray<ObSortFieldCollation> *sort_collations)
      : ret_(common::OB_SUCCESS),
        sort_cmp_funs_(sort_cmp_funs),
        sort_collations_(sort_collations)
    {}
    bool operator()(const ObPxTabletRange::DatumKey &l,
                    const ObPxTabletRange::DatumKey &r);
  public:
    int ret_;
    const ObIArray<ObSortCmpFunc> *sort_cmp_funs_;
    const ObIArray<ObSortFieldCollation> *sort_collations_;
  };
  int build_partition_range_channel_map(
      common::hash::ObHashMap<int64_t, PartitionRangeChannelInfo *> &part_range_channel_map);

  int get_task_idx(
      const int64_t tablet_id,
      const ObPxTabletRange::DatumKey &sort_key,
      ObEvalCtx &eval_ctx,
      int64_t &task_idx);
private:
  static const int64_t DEFAULT_PARTITION_COUNT = 256;
private:
  bool is_inited_;
  const ObIArray<ObExpr *> &sort_exprs_;
  ObPxTabletRange::DatumKey sort_key_;
  common::hash::ObHashMap<int64_t/*tablet_id*/, PartitionRangeChannelInfo *> part_range_map_;
  Compare sort_cmp_;
  ObExpr *ddl_slice_id_expr_;
};

class ObSlaveMapPkeyHashIdxCalc : public ObSlaveMapRepartIdxCalcBase
{
public:
  ObSlaveMapPkeyHashIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const ObSqlExpression *repart_func,
      const ObSqlExpression *repart_sub_func,
      const ObIArray<ObTransmitRepartColumn> *repart_columns,
      const ObIArray<ObTransmitRepartColumn> *repart_sub_columns,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      int64_t task_count,
      const ObPxPartChInfo &part_ch_info,
      common::ObExprCtx  &expr_ctx,
      const common::ObIArray<ObHashColumn> &hash_dist_columns,
      const common::ObIArray<ObSqlExpression *> &dist_exprs,
      ObRepartitionType repart_type,
      bool can_fast_calc_hash)
      : ObSlaveMapRepartIdxCalcBase(
                             exec_ctx,
                             table_schema,
                             repart_func,
                             repart_sub_func,
                             repart_columns,
                             repart_sub_columns,
                             unmatch_row_dist_method,
                             null_row_dist_method,
                             part_ch_info,
                             repart_type),
       affi_hash_map_(),
       hash_calc_(exec_ctx.get_allocator(),
                         expr_ctx,
                         null_row_dist_method,
                         hash_dist_columns,
                         dist_exprs,
                         task_count,
                         can_fast_calc_hash)

  {
    support_vectorized_calc_ = false;
  }
  ObSlaveMapPkeyHashIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObNullDistributeMethod::Type null_row_dist_method,
      const ObPxPartChInfo &part_ch_info,
      int64_t task_count, /* 这个task count 不会被使用，实际会用 ch id array 的 count */
      const ExprFixedArray &dist_exprs,
      const common::ObHashFuncs &dist_hash_funcs,
      ObRepartitionType repart_type,
      bool can_fast_calc_hash)
      : ObSlaveMapRepartIdxCalcBase(exec_ctx,
                                      table_schema,
                                      calc_part_id_expr,
                                      unmatch_row_dist_method,
                                      null_row_dist_method,
                                      part_ch_info,
                                      repart_type),
          affi_hash_map_(),
          hash_calc_(exec_ctx.get_allocator(),
                            task_count,
                            null_row_dist_method,
                            &dist_exprs,
                            &dist_hash_funcs,
                            can_fast_calc_hash)
  {
    support_vectorized_calc_ = false;
  }
  ~ObSlaveMapPkeyHashIdxCalc() = default;

  int init(uint64_t tenant_id = OB_SERVER_TENANT_ID) override;
  int destroy() override;

  // for static engine
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);

  template <bool USE_VEC>
  int get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                                ObBitVector &skip, const int64_t batch_size,
                                int64_t *&indexes)
  {
    UNUSEDx(exprs, eval_ctx, skip, batch_size, indexes);
    return common::OB_NOT_SUPPORTED;
  }

private:
  int get_task_idx_by_tablet_id(ObEvalCtx &eval_ctx, int64_t tablet_id , int64_t &task_idx);
  int build_affi_hash_map(hash::ObHashMap<int64_t, ObPxPartChMapItem> &affi_hash_map);
private:
  hash::ObHashMap<int64_t, ObPxPartChMapItem> affi_hash_map_;
  ObHashSliceIdCalc hash_calc_;
};

class ObWfHybridDistSliceIdCalc : public ObSliceIdxCalc
{
public:
  enum SliceIdCalcType {
    INVALID = 0,
    BROADCAST = 1,
    RANDOM = 2,
    HASH = 3,
    MAX = 4,
  };
  ObWfHybridDistSliceIdCalc(
      ObIAllocator &alloc, const int64_t task_cnt, ObNullDistributeMethod::Type null_row_dist_method,
      const ObIArray<ObExpr*> *dist_exprs, const ObIArray<ObHashFunc> *hash_funcs,
      bool can_fast_calc_hash)
      : ObSliceIdxCalc(alloc, null_row_dist_method),
        slice_id_calc_type_(SliceIdCalcType::INVALID),
        broadcast_slice_id_calc_(alloc, task_cnt, null_row_dist_method),
        random_slice_id_calc_(alloc, task_cnt),
        hash_slice_id_calc_(alloc, task_cnt, null_row_dist_method, dist_exprs, hash_funcs, can_fast_calc_hash),
        wf_hybrid_aggr_status_expr_(nullptr),
        wf_hybrid_pby_exprs_cnt_array_(nullptr)
  {
    support_vectorized_calc_ = true;
    is_multi_slice_calc_type_ = true;
  }
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  virtual void set_calc_hash_keys(int64_t n_keys)
  {
    hash_slice_id_calc_.set_calc_hash_keys(n_keys);
  }
  void set_slice_id_calc_type(SliceIdCalcType slice_id_calc_type)
  {
    slice_id_calc_type_ = slice_id_calc_type;
  }
  int get_multi_slice_idx_vector_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                  ObBitVector &skip, const int64_t batch_size,
                  SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes);
  bool is_broad_cast_slice_calc() { return SliceIdCalcType::BROADCAST == slice_id_calc_type_; }
  void set_wf_hybrid_aggr_status_expr(ObExpr *wf_hybrid_aggr_status_expr)
  {
    wf_hybrid_aggr_status_expr_ = wf_hybrid_aggr_status_expr;
  }
  void set_wf_hybrid_pby_exprs_cnt_array(const common::ObFixedArray<int64_t, common::ObIAllocator>
    *wf_hybrid_pby_exprs_cnt_array)
  {
    wf_hybrid_pby_exprs_cnt_array_ = wf_hybrid_pby_exprs_cnt_array;
  }
private:
  SliceIdCalcType slice_id_calc_type_;
  ObBroadcastSliceIdCalc broadcast_slice_id_calc_;
  ObRandomSliceIdCalc random_slice_id_calc_;
  ObHashSliceIdCalc hash_slice_id_calc_;
  ObExpr *wf_hybrid_aggr_status_expr_;
  const common::ObFixedArray<int64_t, common::ObIAllocator> *wf_hybrid_pby_exprs_cnt_array_;
  SliceIdxArray slice_calc_array_one_row_;
};

class ObNullAwareHashSliceIdCalc : public ObHashSliceIdCalc
{
public:
  ObNullAwareHashSliceIdCalc(ObIAllocator &alloc,
                             const int64_t task_cnt,
                             const ObIArray<ObExpr*> *dist_exprs,
                             const ObIArray<ObHashFunc> *hash_funcs,
                             bool can_fast_calc_hash)
      : ObHashSliceIdCalc(alloc, task_cnt, ObNullDistributeMethod::NONE, dist_exprs,
                          hash_funcs, can_fast_calc_hash)
  {
    support_vectorized_calc_ = true;
    is_multi_slice_calc_type_ = true;
  }

  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  int get_multi_slice_idx_vector_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes);
};

class ObNullAwareAffinitizedRepartSliceIdxCalc : public ObAffinitizedRepartSliceIdxCalc
{
public:
  ObNullAwareAffinitizedRepartSliceIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const ObSqlExpression *repart_func,
      const ObSqlExpression *repart_sub_func,
      const ObIArray<ObTransmitRepartColumn> *repart_columns,
      const ObIArray<ObTransmitRepartColumn> *repart_sub_columns,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      int64_t task_count,
      ObPxPartChInfo &part_ch_info,
      ObRepartitionType repart_type,
      const ObIArray<ObExpr*> *repartition_exprs,
      bool can_fast_calc_hash)
      : ObAffinitizedRepartSliceIdxCalc(exec_ctx,
                                        table_schema,
                                        repart_func,
                                        repart_sub_func,
                                        repart_columns,
                                        repart_sub_columns,
                                        unmatch_row_dist_method,
                                        ObNullDistributeMethod::NONE,
                                        task_count,
                                        part_ch_info,
                                        repart_type,
                                        can_fast_calc_hash),
      repartition_exprs_(repartition_exprs)
  {
    support_vectorized_calc_ = true;
    is_multi_slice_calc_type_ = true;
  }

  ObNullAwareAffinitizedRepartSliceIdxCalc(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      ObExpr *calc_part_id_expr,
      int64_t task_count,
      const ObPxPartChInfo &part_ch_info,
      ObPQDistributeMethod::Type unmatch_row_dist_method,
      ObRepartitionType repart_type,
      const ObIArray<ObExpr*> *hash_dist_exprs,
      const ObIArray<ObHashFunc> *hash_funcs,
      const ObIArray<ObExpr*> *repartition_exprs,
      bool can_fast_calc_hash)
      : ObAffinitizedRepartSliceIdxCalc(exec_ctx,
                                       table_schema,
                                       calc_part_id_expr,
                                       task_count,
                                       part_ch_info,
                                       unmatch_row_dist_method,
                                       ObNullDistributeMethod::NONE,
                                       repart_type,
                                       hash_dist_exprs,
                                       hash_funcs,
                                       can_fast_calc_hash),
      repartition_exprs_(repartition_exprs)
  {
    support_vectorized_calc_ = true;
    is_multi_slice_calc_type_ = true;
  }

  ~ObNullAwareAffinitizedRepartSliceIdxCalc() = default;
  virtual int init(uint64_t tenant_id) override;
  template <bool USE_VEC>
  int get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              SliceIdxArray &slice_idx_array, ObBitVector *skip = NULL);
  virtual int get_slice_idx(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, int64_t &slice_idx)
  {
    UNUSED(exprs);
    UNUSED(eval_ctx);
    UNUSED(slice_idx);
    return common::OB_NOT_IMPLEMENT;;
  }
  int get_multi_slice_idx_vector_inner(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                    ObBitVector &skip, const int64_t batch_size,
                    SliceIdxFlattenArray &slice_flatten_indexes, EndIdxArray &end_indexes);
private:
  const ObIArray<ObExpr*> *repartition_exprs_;
};


}
}
#endif
