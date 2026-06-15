/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_TTL_FILTER_STRUCT_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_TTL_FILTER_STRUCT_H_

#include "ob_mds_filter_struct.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"

namespace oceanbase
{
namespace storage
{
struct ObTTLFilterInitParams;
}
namespace sql
{

class ObTTLWhiteFilterNode : public ObPushdownWhiteFilterNode
{
public:
  ObTTLWhiteFilterNode(ObIAllocator &alloc) : ObPushdownWhiteFilterNode(alloc)
  {
    type_ = PushdownFilterType::TTL_WHITE_FILTER;
  }

  virtual ~ObTTLWhiteFilterNode() = default;

  virtual int set_op_type(const ObRawExpr &raw_expr) override final;
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const override final;
  virtual ObObjType get_filter_arg_obj_type(int64_t arg_idx) const override final;

  OB_INLINE void set_op_type(const ObWhiteFilterOperatorType type) { op_type_ = type; }

  TO_STRING_KV(K_(obj_meta), K_(op_type), K_(col_ids));

  ObObjMeta obj_meta_;
};

class ObTTLWhiteFilterExecutor : public ObWhiteFilterExecutor, public ObIMDSFilterExecutor
{
public:
  enum class FilterType : uint8_t
  {
    COMMIT_VERSION_FILTER = 0,
    IS_NULL_FILTER = 1,
    TTL_FILTER = 2,
    MAX_TYPE = 3,
  };

public:
  ObTTLWhiteFilterExecutor(ObIAllocator &alloc,
                           ObTTLWhiteFilterNode &filter,
                           ObPushdownOperator *op)
      : ObWhiteFilterExecutor(alloc, filter, op), ObIMDSFilterExecutor(), is_inited_(false)
  {
  }

  virtual ~ObTTLWhiteFilterExecutor() = default;

  int init(ObPushdownFilterFactory &filter_factory,
           const ObTTLFilterInfo &ttl_filter_info,
           const ObTTLFilterInitParams &init_params,
           const int64_t schema_rowkey_cnt,
           const FilterType executor_filter_type = FilterType::TTL_FILTER);

  int switch_info(ObPushdownFilterFactory &filter_factory,
                  const ObTTLFilterInfo &ttl_filter_info,
                  const ObTTLFilterInitParams &init_params,
                  const int64_t schema_rowkey_cnt,
                  const FilterType executor_filter_type = FilterType::TTL_FILTER);

  OB_INLINE int filter(const blocksstable::ObDatumRow &row, bool &filtered) const;
  OB_INLINE int filter(const blocksstable::ObStorageDatum *datums, int64_t count, bool &filtered) const override final;

  int skip_index_filter(blocksstable::ObAggRowCachedReader &agg_row_cached_reader,
                        sql::ObBoolMask &fal_desc,
                        const blocksstable::ObSkipIndexExtraParam &extra_param) const;

  virtual int64_t get_col_offset(const bool is_cg = false) const override final
  {
    if (!is_cg || col_type_ == ObTTLFilterColType::ROWSCN) {
      // in row store sstable or rowkey cg sstable
      return col_offset_;
    } else {
      // normal cg
      return 0;
    }
  }
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const override final
  {
    // just proxy to filter node
    return filter_.get_filter_val_meta(obj_meta);
  }

  virtual common::ObIArray<uint64_t> &get_col_ids() override final
  {
    return filter_.get_col_ids();
  }

  // below function all only for filter interface
  int init_evaluated_datums(bool &is_valid) override final
  {
    is_valid = true;
    return OB_SUCCESS;
  }

  TO_STRING_KV(K_(type), K_(col_offset), K_(col_type), K_(original_filter_val), K_(cache_datum), K_(is_inited), K(get_filter_node()));

private:
  int inner_init(const ObTTLFilterInfo &ttl_filter_info,
                 const ObTTLFilterInitParams &init_params,
                 const int64_t schema_rowkey_cnt,
                 const FilterType executor_filter_type);

  int inner_filter(const blocksstable::ObStorageDatum &datum, bool &filtered) const;

private:
  int64_t col_offset_;
  int64_t col_idx_;
  int64_t original_filter_val_;
  blocksstable::ObStorageDatum cache_datum_;
  ObTTLFilterColType col_type_;
  FilterType filter_type_;
  bool is_inited_;
};

class ObTTLOrFilterNode : public ObPushdownFilterNode
{
public:
  ObTTLOrFilterNode(ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::TTL_OR_FILTER)
  {
  }

  virtual ~ObTTLOrFilterNode() = default;
};

class ObTTLOrFilterExecutor : public ObPushdownFilterExecutor
{
public:
  //                OR
  //            /    |    \
  //           /     |     \
  //         SCN  IS_NULL TTL
  static constexpr int64_t CHILD_COUNT = 3;
  static constexpr ObTTLWhiteFilterExecutor::FilterType CHILD_FILTER_TYPES[CHILD_COUNT] = {
      ObTTLWhiteFilterExecutor::FilterType::COMMIT_VERSION_FILTER,
      ObTTLWhiteFilterExecutor::FilterType::IS_NULL_FILTER,
      ObTTLWhiteFilterExecutor::FilterType::TTL_FILTER,
  };

public:
  ObTTLOrFilterExecutor(ObIAllocator &alloc, ObTTLOrFilterNode &filter, ObPushdownOperator *op);
  virtual ~ObTTLOrFilterExecutor() = default;

  virtual common::ObIArray<uint64_t> &get_col_ids() override final
  {
    return filter_.get_col_ids();
  }

  virtual int init_evaluated_datums(bool &is_valid) override final
  {
    is_valid = true;
    return OB_SUCCESS;
  }

  int init(ObPushdownFilterFactory &filter_factory,
           const ObTTLFilterInfo &ttl_filter_info,
           const ObTTLFilterInitParams &init_params,
           const int64_t schema_rowkey_cnt);

  int switch_info(ObPushdownFilterFactory &filter_factory,
                  const ObTTLFilterInfo &ttl_filter_info,
                  const ObTTLFilterInitParams &init_params,
                  const int64_t schema_rowkey_cnt);

  int filter(const blocksstable::ObDatumRow &row, bool &filtered) const;
  int skip_index_filter(blocksstable::ObAggRowCachedReader &agg_row_cached_reader,
                        sql::ObBoolMask &fal_desc,
                        const blocksstable::ObSkipIndexExtraParam &extra_param) const;

private:
  ObTTLOrFilterNode &filter_;
  bool is_inited_;
};

class ObTTLAndFilterNode : public ObPushdownFilterNode
{
public:
  ObTTLAndFilterNode(ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::TTL_AND_FILTER)
  {
  }

  virtual ~ObTTLAndFilterNode() = default;
};

class ObTTLAndFilterExecutor : public ObPushdownFilterExecutor
{
public:
  ObTTLAndFilterExecutor(ObIAllocator &alloc, ObTTLAndFilterNode &filter, ObPushdownOperator *op);
  virtual ~ObTTLAndFilterExecutor();

  virtual common::ObIArray<uint64_t> &get_col_ids() override final
  {
    return filter_.get_col_ids();
  }

  virtual int init_evaluated_datums(bool &is_valid) override final
  {
    is_valid = true;
    return OB_SUCCESS;
  }

  int init(ObPushdownFilterFactory &filter_factory,
           const ObTTLFilterInfoArray &ttl_filter_info_array,
           const ObTTLFilterInitParams &init_params,
           const int64_t schema_rowkey_cnt);

  int switch_info(ObPushdownFilterFactory &filter_factory,
                  const ObTTLFilterInfoArray &ttl_filter_info_array,
                  const ObTTLFilterInitParams &init_params,
                  const int64_t schema_rowkey_cnt);

  int filter(const blocksstable::ObDatumRow &row, bool &filtered) const;

  int skip_index_filter(blocksstable::ObAggRowCachedReader &agg_row_cached_reader,
                        sql::ObBoolMask &fal_desc,
                        const blocksstable::ObSkipIndexExtraParam &extra_param) const;

  int execute_logic_filter(PushdownFilterInfo &filter_info,
                           blocksstable::ObIMicroBlockRowScanner *micro_scanner,
                           const bool use_vectorize,
                           ObBitmap &result);

  OB_INLINE void reuse()
  {
    valid_ttl_filter_cnt_ = 0;
  }

  TO_STRING_KV(K_(is_inited), K_(valid_ttl_filter_cnt), K_(ttl_filters), K_(ttl_filters_buffer));

private:
  int inner_reuse();
  int try_use_cached_filter(ObPushdownFilterFactory &filter_factory,
                            const ObTTLFilterInfo &ttl_filter_info,
                            const ObTTLFilterInitParams &init_params,
                            const int64_t schema_rowkey_cnt,
                            ObPushdownFilterExecutor *&filter);
  int build_ttl_filter(const ObTTLFilterInfo &ttl_filter_info,
                       const ObTTLFilterInitParams &init_params,
                       const int64_t schema_rowkey_cnt,
                       ObPushdownFilterFactory &filter_factory,
                       ObPushdownFilterExecutor *&filter);

  OB_INLINE void mock_children_array_for_skip_index()
  {
    set_childs(ttl_filters_.count(), ttl_filters_.get_data());
  }

private:
  bool is_inited_;
  int32_t valid_ttl_filter_cnt_;
  ObSEArray<ObPushdownFilterExecutor*, 4> ttl_filters_;
  ObSEArray<ObPushdownFilterExecutor*, 4> ttl_filters_buffer_;
  ObTTLAndFilterNode &filter_;
};

OB_INLINE int ObTTLWhiteFilterExecutor::filter(const blocksstable::ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "TTL filter executor not inited", KR(ret));
  } else if (OB_FAIL(inner_filter((col_offset_ >= row.count_ || row.storage_datums_[col_offset_].is_nop())
                                       ? default_datums_.at(0)                 // nop        ---> use default datum instead (such as add column)
                                       : row.storage_datums_[col_offset_],     // otherwise  ---> use original datum
                                   filtered))) {
    STORAGE_LOG(WARN, "Failed to inner filter", KR(ret), K(row), K(col_offset_), K(default_datums_.at(0)));
  }

  return ret;
}

OB_INLINE int ObTTLWhiteFilterExecutor::filter(const blocksstable::ObStorageDatum *datums,
                                               int64_t count,
                                               bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "TTL filter executor not inited", KR(ret));
  } else if (OB_UNLIKELY(count == 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected count", KR(ret), K(count));
  } else if (OB_FAIL(inner_filter(datums[0].is_nop_value() ? default_datums_.at(0) : datums[0], filtered))) {
    STORAGE_LOG(WARN, "Failed to inner filter", KR(ret), K(datums[0]));
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase

#endif
