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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_TRUNCATE_FILTER_STRUCT_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_TRUNCATE_FILTER_STRUCT_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "src/storage/blocksstable/ob_storage_datum.h"
#include "ob_pushdown_filter.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace storage
{
struct ObTruncatePartition;
struct ObTruncateInfo;
class ObTruncateInfoArray;
struct ObStorageListRowValues;
}
namespace sql
{

enum ObTruncateItemType
{
  TRUNCATE_SCN = 0,
  TRUNCATE_RANGE_LEFT = 1,
  TRUNCATE_RANGE_RIGHT = 2,
  TRUNCATE_LIST = 3,
  MAX_TRUNCATE_PART_EXPR_TYPE
};

class ObITruncateFilterExecutor
{
public:
  ObITruncateFilterExecutor(common::ObIAllocator &alloc);
  virtual ~ObITruncateFilterExecutor();
  virtual int prepare_truncate_param(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncatePartition &truncate_partition) = 0;
  virtual int prepare_truncate_value(
      const int64_t truncate_commit_viersion,
      const storage::ObTruncatePartition &truncate_partition) = 0;
  virtual int filter(const blocksstable::ObDatumRow &row, bool &filtered);
  int filter(const blocksstable::ObStorageDatum *datums, int64_t count, bool &filtered) const
  {
    return inner_filter(datums, count, filtered);
  }
private:
  virtual int inner_filter(
      const blocksstable::ObStorageDatum *datums,
      int64_t count,
      bool &filtered,
      const common::ObIArray<int32_t> *col_idxs = nullptr) const = 0;
public:
  virtual int prepare_datum_buf(common::ObIAllocator &alloc, const int64_t count);
  OB_INLINE blocksstable::ObStorageDatum *get_tmp_datum_buffer()
  {
    return tmp_datum_buf_;
  }
  OB_INLINE const common::ObIArray<int32_t> &get_col_idxs()
  {
    return col_idxs_;
  }
  OB_INLINE void set_truncate_item_type(const ObTruncateItemType item_type)
  {
    truncate_item_type_ = item_type;
  }
  OB_INLINE void set_need_flip(const bool need_flip)
  {
    need_flip_ = need_flip;
  }
  OB_INLINE bool need_flip() const
  {
    return need_flip_;
  }
  OB_INLINE bool is_truncate_scn_item() const
  {
    return ObTruncateItemType::TRUNCATE_SCN == truncate_item_type_;
  }
  OB_INLINE bool is_truncate_value_invalid(const blocksstable::ObStorageDatum &datum)
  {
    return datum.is_nop() || datum.is_null();
  }
  VIRTUAL_TO_STRING_KV(K_(truncate_item_type), K_(need_flip), K_(col_idxs), KP_(tmp_datum_buf), KP_(datum_allocator));
  ObTruncateItemType truncate_item_type_;
  bool need_flip_;
  common::ObFixedArray<int32_t, common::ObIAllocator> col_idxs_;
  blocksstable::ObStorageDatum *tmp_datum_buf_;
  common::ObIAllocator *datum_allocator_;
};

// single column filter: c1 <= A
class ObTruncateWhiteFilterNode : public ObPushdownWhiteFilterNode
{
public:
  ObTruncateWhiteFilterNode(common::ObIAllocator &alloc)
    : ObPushdownWhiteFilterNode(alloc),
      obj_meta_()
  {
    type_ = PushdownFilterType::TRUNCATE_WHITE_FILTER;
  }
  virtual ~ObTruncateWhiteFilterNode() {}
  virtual int set_op_type(const ObRawExpr &raw_expr) override final;
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const override final;
  virtual ObObjType get_filter_arg_obj_type(int64_t arg_idx) const override final;
  void set_truncate_white_op_type(const ObWhiteFilterOperatorType &op_type)
  {
    op_type_ = op_type;
  }
  INHERIT_TO_STRING_KV("ObPushdownWhiteFilterNode", ObPushdownWhiteFilterNode, K_(obj_meta));
  ObObjMeta obj_meta_;
};

class ObTruncateWhiteFilterExecutor : public ObWhiteFilterExecutor, public ObITruncateFilterExecutor
{
public:
  struct FilterBatchGuard {
    FilterBatchGuard(ObTruncateWhiteFilterExecutor &executor) : executor_(executor)
    {
      if (executor.is_truncate_scn_item()) {
        executor.pre_filter_batch();
      }
    }
    ~FilterBatchGuard()
    {
      if (executor_.is_truncate_scn_item()) {
        executor_.post_filter_batch();
      }
    }
    ObTruncateWhiteFilterExecutor &executor_;
  };

  ObTruncateWhiteFilterExecutor(
      common::ObIAllocator &alloc,
      ObTruncateWhiteFilterNode &filter,
      ObPushdownOperator &op)
      : ObWhiteFilterExecutor(alloc, filter, op),
        ObITruncateFilterExecutor(alloc),
        storage_datum_param_(),
        in_storage_datum_params_(nullptr),
        in_param_cnt_(0),
        truncate_version_(0)
  {
  }
  virtual ~ObTruncateWhiteFilterExecutor() { release_storage_datum_space(); }
  OB_INLINE ObTruncateWhiteFilterNode &get_filter_node()
  {
    return static_cast<ObTruncateWhiteFilterNode &>(filter_);
  }
  OB_INLINE const ObTruncateWhiteFilterNode &get_filter_node() const
  {
    return static_cast<const ObTruncateWhiteFilterNode &>(filter_);
  }
  virtual common::ObIArray<uint64_t> &get_col_ids() override final
  {
    return filter_.get_col_ids();
  }
  virtual const common::ObIArray<ObExpr *> *get_cg_col_exprs() const override final;
  virtual int init_evaluated_datums(bool &is_valid) override final;
  virtual int filter(ObEvalCtx &eval_ctx, const ObBitVector &skip_bit, bool &filtered) override final;
  virtual int prepare_truncate_param(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncatePartition &truncate_partition) override final;
  virtual int prepare_truncate_value(
      const int64_t truncate_commit_viersion,
      const storage::ObTruncatePartition &truncate_partition) override final;
  void pre_filter_batch();
  void post_filter_batch();
  INHERIT_TO_STRING_KV("ObWhiteFilterExecutor", ObWhiteFilterExecutor,
                       K_(truncate_item_type), K_(need_flip), K_(col_idxs), KP_(tmp_datum_buf),
                       K_(storage_datum_param), KP_(in_storage_datum_params), K_(in_param_cnt), K_(truncate_version));
private:
  virtual int inner_filter(
      const blocksstable::ObStorageDatum *datums,
      int64_t count,
      bool &filtered,
      const common::ObIArray<int32_t> *col_idxs = nullptr) const override final;
  int prepare_truncate_list_value(const storage::ObStorageListRowValues &list_row_values);
  int reserve_storage_datum_space(const int64_t count);
  void release_storage_datum_space();
  blocksstable::ObStorageDatum storage_datum_param_;
  blocksstable::ObStorageDatum *in_storage_datum_params_;
  int64_t in_param_cnt_;
  int64_t truncate_version_;
};

// multiple columns filter: (c1,c2) <= (A1,A2)
class ObTruncateBlackFilterNode : public ObPushdownFilterNode
{
public:
  ObTruncateBlackFilterNode(common::ObIAllocator &alloc)
    : ObPushdownFilterNode(alloc, PushdownFilterType::TRUNCATE_BLACK_FILTER)
  {
  }
  ~ObTruncateBlackFilterNode() {}  
};

class ObTruncateBlackFilterExecutor : public ObPushdownFilterExecutor, public ObITruncateFilterExecutor
{
public:
  ObTruncateBlackFilterExecutor(
      common::ObIAllocator &alloc,
      ObTruncateBlackFilterNode &filter,
      ObPushdownOperator &op)
      : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::TRUNCATE_BLACK_FILTER_EXECUTOR),
        ObITruncateFilterExecutor(alloc),
        filter_(filter),
        obj_metas_(alloc),
        cmp_funcs_(alloc),
        storage_datum_params_(nullptr),
        storage_datum_cnt_(0),
        truncate_row_cnt_(0),
        row_obj_cnt_(0)
  {
  }
  virtual ~ObTruncateBlackFilterExecutor() { release_storage_datum_space(); }
  virtual common::ObIArray<uint64_t> &get_col_ids() override final
  {
    return filter_.get_col_ids();
  }
  virtual int init_evaluated_datums(bool &is_valid) override final;
  virtual int prepare_truncate_param(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncatePartition &truncate_partition) override final;
  virtual int prepare_truncate_value(
      const int64_t truncate_commit_viersion,
      const storage::ObTruncatePartition &truncate_partition) override final;
  INHERIT_TO_STRING_KV("ObPushdownFilterExecutor", ObPushdownFilterExecutor,
                       K_(truncate_item_type), K_(need_flip), K_(col_idxs), KP_(tmp_datum_buf),
                       K_(filter), K_(obj_metas), K_(cmp_funcs), KP_(storage_datum_params),
                       K_(storage_datum_cnt), K_(truncate_row_cnt), K_(row_obj_cnt));
private:
  virtual int inner_filter(
      const blocksstable::ObStorageDatum *datums,
      int64_t count,
      bool &filtered,
      const common::ObIArray<int32_t> *col_idxs = nullptr) const override final;
  int reserve_storage_datum_space(const int64_t count);
  void release_storage_datum_space();
  int prepare_truncate_list_value(const storage::ObStorageListRowValues &list_row_values);
  int prepare_metas_and_cmp_funcs(
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncatePartition &truncate_partition);
  ObTruncateBlackFilterNode &filter_;
  common::ObFixedArray<ObObjMeta, common::ObIAllocator> obj_metas_;
  common::ObFixedArray<common::ObDatumCmpFuncType, common::ObIAllocator> cmp_funcs_;
  blocksstable::ObStorageDatum *storage_datum_params_;
  int64_t storage_datum_cnt_;
  int64_t truncate_row_cnt_;
  int64_t row_obj_cnt_;
};

// truncate filter:
// 1. range
// [ scn > V OR c1 < A OR c1 >= B ] == ![ scn <= V ADD c1 >= A AND c1 < B ]}
// [ scn > V OR (c1,c2) < (A1,A2) OR (c1,c2) >= (B1,B2) ] == ![ scn <= V ADD (c1,c2) >= (A1,A2) ADD (c1,c2) < (B1,B2) ]
// 2. list
// [ scn > V OR c1 != A ] == ![ scn <= V AND c1 = A ]
// [ scn > V OR c1 NOT IN [A1,B1] ] == ![ scn <= V AND c1 IN [A1,B1] ]
// [ scn > V OR (c1,c2) != (A1,A2) ] == ![ scn <= V AND (c1,c2) = (A1,A2) ]
// [ scn > V OR (c1,c2)  NOT IN [(A1,A2) OR (B1,B2)] ] == ![ scn <= V AND (c1,c2) IN [(A1,A2), (B1,B2)] ]
class ObTruncateOrFilterNode : public ObPushdownFilterNode
{
public:
  ObTruncateOrFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::TRUNCATE_AND_FILTER)
  {}
};

class ObTruncateOrFilterExecutor : public ObPushdownFilterExecutor, public ObITruncateFilterExecutor
{
public:
  ObTruncateOrFilterExecutor(common::ObIAllocator &alloc,
                             ObTruncateOrFilterNode &filter,
                             ObPushdownOperator &op)
    : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::TRUNCATE_OR_FILTER_EXECUTOR),
      ObITruncateFilterExecutor(alloc),
      filter_(filter),
      part_child_count_(0)
  {}
  virtual common::ObIArray<uint64_t> &get_col_ids() override final
  {
    return filter_.get_col_ids();
  }
  virtual int init_evaluated_datums(bool &is_valid) override final;
  virtual int prepare_truncate_param(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncatePartition &truncate_partition) override final;
  virtual int prepare_truncate_value(
      const int64_t truncate_commit_viersion,
      const storage::ObTruncatePartition &truncate_partition) override final;
  virtual int filter(const blocksstable::ObDatumRow &row, bool &filtered) override final;
  OB_INLINE int64_t get_part_child_count() const
  {
    return part_child_count_;
  }
  OB_INLINE void set_part_child_count(const int64_t part_child_cnt)
  {
    part_child_count_ = part_child_cnt;
  }
  OB_INLINE bool is_subpart_filter() const
  {
    return part_child_count_ != get_child_count();
  }
  INHERIT_TO_STRING_KV("ObPushdownFilterExecutor", ObPushdownFilterExecutor, K_(truncate_item_type), K_(need_flip),
                       K_(filter), K_(part_child_count));
private:
  virtual int inner_filter(
      const blocksstable::ObStorageDatum *datums,
      int64_t count,
      bool &filtered,
      const common::ObIArray<int32_t> *col_idxs = nullptr) const override final;
  ObTruncateOrFilterNode &filter_;
  int64_t part_child_count_;
};

// multple truncate filters
class ObTruncateAndFilterNode : public ObPushdownFilterNode
{
public:
  ObTruncateAndFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::TRUNCATE_OR_FILTER)
  {}
};

class ObTruncateAndFilterExecutor : public ObPushdownFilterExecutor
{
public:
  ObTruncateAndFilterExecutor(
      common::ObIAllocator &alloc,
      ObTruncateAndFilterNode &filter,
      ObPushdownOperator &op);
  virtual ~ObTruncateAndFilterExecutor();
  virtual common::ObIArray<uint64_t> &get_col_ids() override final
  {
    return filter_.get_col_ids();
  }
  virtual int init_evaluated_datums(bool &is_valid) override final;
  int init(
      ObPushdownFilterFactory &filter_factory,
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const storage::ObTruncateInfoArray &truncate_info_array);
  int switch_info(
      ObPushdownFilterFactory &filter_factory,
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncateInfoArray &truncate_info_array);
  int filter(const blocksstable::ObDatumRow &row, bool &filtered);
  int execute_logic_filter(
      PushdownFilterInfo &filter_info,
      blocksstable::ObIMicroBlockRowScanner *micro_scanner,
      const bool use_vectorize,
      common::ObBitmap &result);
  OB_INLINE void reuse()
  {
    valid_truncate_filter_cnt_ = 0;
  }
  INHERIT_TO_STRING_KV("ObPushdownFilterExecutor", ObPushdownFilterExecutor, K_(is_inited), K_(has_nullable_col),
                       K_(valid_truncate_filter_cnt), K_(truncate_filters), KP_(item_buffer), K_(filter));
private:
  int inner_reuse();
  int try_use_cached_filter(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncateInfo &truncate_info,
      ObTruncateOrFilterExecutor *&filter);
  int build_single_truncate_filter(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncateInfo &truncate_info,
      ObPushdownFilterFactory &filter_factory,
      ObTruncateOrFilterExecutor *&filter);
  int build_single_part_filter(
      ObPushdownFilterFactory &filter_factory,
      const storage::ObTruncatePartition &truncate_partition,
      int64_t &filter_item_pos);
  int prepare_part_filter_param(
      const int64_t schema_rowkey_cnt,
      const int64_t truncate_commit_viersion,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const storage::ObTruncatePartition &truncate_partition,
      const int64_t filter_item_begin,
      const int64_t filter_item_end,
      ObPushdownFilterExecutor **childs,
      const bool need_prepare_param = true);
  static const int64_t MAX_FILTER_ITEM_CNT = 5;
  bool is_inited_;
  bool has_nullable_col_;
  int32_t valid_truncate_filter_cnt_;
  common::ObSEArray<ObTruncateOrFilterExecutor*, 4>truncate_filters_;
  ObPushdownFilterExecutor *item_buffer_[MAX_FILTER_ITEM_CNT];
  common::ObSEArray<ObTruncateOrFilterExecutor*, 4> part_filter_buffer_;
  common::ObSEArray<ObTruncateOrFilterExecutor*, 4> subpart_filter_buffer_;
  ObTruncateAndFilterNode &filter_;
};

class ObTruncateFilterFactory
{
public:
  static int build_scn_filter(
    ObPushdownOperator &op,
    ObPushdownFilterFactory &filter_factory,
    ObPushdownFilterExecutor *&scn_filter);
  static int build_range_filter(
      ObPushdownOperator &op,
      ObPushdownFilterFactory &filter_factory,
      const bool need_black,
      ObPushdownFilterExecutor *&low_filter,
      ObPushdownFilterExecutor *&high_filter);
  static int build_list_filter(
      ObPushdownOperator &op,
      ObPushdownFilterFactory &filter_factory,
      const bool need_black,
      ObPushdownFilterExecutor *&list_filter);
private:
  static int build_filter(
      ObPushdownOperator &op,
      ObPushdownFilterFactory &filter_factory,
      const bool need_black,
      ObPushdownFilterNode *&node,
      ObPushdownFilterExecutor *&executor);
};

#define RELEASE_TRUNCATE_PTR_WHEN_FAILED(type, ptr) \
  if (nullptr != ptr) {                             \
    ptr->~type();                                   \
    ptr = nullptr;                                  \
  }

}
}

#endif