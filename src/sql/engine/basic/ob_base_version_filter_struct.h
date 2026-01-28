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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_BASE_VERSION_FILTER_STRUCT_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_BASE_VERSION_FILTER_STRUCT_H_

#include "ob_mds_filter_struct.h"
#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{
namespace sql
{

class ObBaseVersionFilterNode : public ObPushdownWhiteFilterNode
{
public:
  ObBaseVersionFilterNode(ObIAllocator &alloc) : ObPushdownWhiteFilterNode(alloc)
  {
    // select ...  where row.scn > base_version
    type_ = PushdownFilterType::BASE_VERSION_FILTER;
    op_type_ = WHITE_OP_GT;
    obj_meta_.set_int();
  }

  virtual ~ObBaseVersionFilterNode() = default;

  virtual int set_op_type(const ObRawExpr &raw_expr) override final;
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const override final;
  virtual ObObjType get_filter_arg_obj_type(int64_t arg_idx) const override final;

  TO_STRING_KV(K_(obj_meta), K_(op_type), K_(col_ids));

  ObObjMeta obj_meta_;
};

class ObBaseVersionFilterExecutor : public ObWhiteFilterExecutor, public ObIMDSFilterExecutor
{
public:
  ObBaseVersionFilterExecutor(ObIAllocator &alloc,
                              ObBaseVersionFilterNode &filter,
                              ObPushdownOperator &op)
      : ObWhiteFilterExecutor(alloc, filter, op), ObIMDSFilterExecutor(), is_inited_(false)
  {
  }

  virtual ~ObBaseVersionFilterExecutor() = default;

  int init(const int64_t schema_rowkey_cnt, const int64_t base_version);
  int switch_info(const int64_t schema_rowkey_cnt, const int64_t base_version);

  int filter(const blocksstable::ObDatumRow &row, bool &filtered) const override final;
  int filter(const blocksstable::ObStorageDatum *datums, int64_t count, bool &filtered) const override final;

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
  int inner_init(const int64_t schema_rowkey_cnt, const int64_t base_version);
  int inner_filter(const blocksstable::ObStorageDatum &datum, bool &filtered) const;

private:
  int64_t col_idx_;
  blocksstable::ObStorageDatum cache_datum_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif