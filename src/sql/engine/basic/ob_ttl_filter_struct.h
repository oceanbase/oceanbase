/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_TTL_FILTER_STRUCT_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_TTL_FILTER_STRUCT_H_

#include "ob_mds_filter_struct.h"
#include "share/schema/ob_table_param.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"

namespace oceanbase
{
namespace sql
{

class ObTTLWhiteFilterNode : public ObPushdownWhiteFilterNode
{
public:
  ObTTLWhiteFilterNode(ObIAllocator &alloc) : ObPushdownWhiteFilterNode(alloc)
  {
    type_ = PushdownFilterType::TTL_WHITE_FILTER;

    // TTL filter is always like column > ttl_value. We don't care whether the column is ora_rowscn.
    // Caller should ensure the compare value be positive.
    op_type_ = WHITE_OP_GT;
  }

  virtual ~ObTTLWhiteFilterNode() = default;

  virtual int set_op_type(const ObRawExpr &raw_expr) override final;
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const override final;
  virtual ObObjType get_filter_arg_obj_type(int64_t arg_idx) const override final;

  TO_STRING_KV(K_(obj_meta), K_(op_type), K_(col_ids));

  ObObjMeta obj_meta_;
};

class ObTTLWhiteFilterExecutor : public ObWhiteFilterExecutor, public ObIMDSFilterExecutor
{
public:
  ObTTLWhiteFilterExecutor(ObIAllocator &alloc, ObTTLWhiteFilterNode &filter, ObPushdownOperator &op)
      : ObWhiteFilterExecutor(alloc, filter, op), ObIMDSFilterExecutor(), is_inited_(false)
  {
  }

  virtual ~ObTTLWhiteFilterExecutor() = default;

  int init(const storage::ObTTLFilterInfo &ttl_filter_info,
           const ObIArray<share::schema::ObColDesc> &col_descs);

  int switch_info(const storage::ObTTLFilterInfo &ttl_filter_info,
                  const ObIArray<share::schema::ObColDesc> &col_descs);

  OB_INLINE int filter(const blocksstable::ObDatumRow &row, bool &filtered) const override final;
  OB_INLINE int filter(const blocksstable::ObStorageDatum *datums, int64_t count, bool &filtered) const override final;

  OB_INLINE bool is_ora_rowscn() const { return is_ora_rowscn_; }
  virtual int64_t get_col_idx() const override final { return col_idx_; }
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

  TO_STRING_KV(K_(type), K_(col_idx), K_(cache_datum), K_(is_inited), K(get_filter_node()));

private:
  int inner_init(const storage::ObTTLFilterInfo &ttl_filter_info,
                 const ObIArray<share::schema::ObColDesc> &col_descs);

  int inner_filter(const blocksstable::ObStorageDatum &datum, bool &filtered) const;

private:
  int64_t is_ora_rowscn_;
  int64_t col_idx_;
  blocksstable::ObStorageDatum cache_datum_;
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
  ObTTLAndFilterExecutor(ObIAllocator &alloc, ObTTLAndFilterNode &filter, ObPushdownOperator &op);
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
           const ObIArray<share::schema::ObColDesc> &cols_desc,
           const ObIArray<share::schema::ObColumnParam *> *cols_param,
           const storage::ObTTLFilterInfoArray &ttl_filter_info_array);

  int switch_info(ObPushdownFilterFactory &filter_factory,
                  const ObIArray<share::schema::ObColDesc> &cols_desc,
                  const storage::ObTTLFilterInfoArray &ttl_filter_info_array);

  int filter(const blocksstable::ObDatumRow &row, bool &filtered) const;

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
  int try_use_cached_filter(const ObIArray<share::schema::ObColDesc> &cols_desc,
                            const storage::ObTTLFilterInfo &ttl_filter_info,
                            ObTTLWhiteFilterExecutor *&filter);
  int build_ttl_filter(const ObIArray<share::schema::ObColDesc> &cols_desc,
                       const storage::ObTTLFilterInfo &ttl_filter_info,
                       ObPushdownFilterFactory &filter_factory,
                       ObTTLWhiteFilterExecutor *&filter);

  int mock_children_array_for_skip_index();

private:
  bool is_inited_;
  int32_t valid_ttl_filter_cnt_;
  ObSEArray<ObTTLWhiteFilterExecutor*, 4> ttl_filters_;
  ObSEArray<ObTTLWhiteFilterExecutor*, 4> ttl_filters_buffer_;
  ObSEArray<ObPushdownFilterExecutor*, 4> children_array_;
  ObTTLAndFilterNode &filter_;
};

OB_INLINE int ObTTLWhiteFilterExecutor::filter(const blocksstable::ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "TTL filter executor not inited", KR(ret));
  } else if (OB_UNLIKELY(row.count_ < col_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected col idx", KR(ret), K(col_idx_), K(row.count_));
  } else if (OB_FAIL(inner_filter(row.storage_datums_[col_idx_], filtered))) {
    STORAGE_LOG(WARN, "Failed to inner filter", KR(ret), K(row.storage_datums_[col_idx_]));
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
  } else if (OB_FAIL(inner_filter(datums[0], filtered))) {
    STORAGE_LOG(WARN, "Failed to inner filter", KR(ret), K(datums[0]));
  }

  return ret;
}


} // namespace sql
} // namespace oceanbase

#endif
