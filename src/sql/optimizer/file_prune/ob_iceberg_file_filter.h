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

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_ICEBERG_FILE_FILTER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_ICEBERG_FILE_FILTER_H

#include "ob_i_lake_table_file_pruner.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"

namespace oceanbase
{
namespace sql
{
class ObDMLStmt;
class ObExecContext;

struct PartColDesc
{
public:
  OB_UNIS_VERSION(1);
public:
  PartColDesc()
  : spec_id_(-1),
    column_id_(OB_INVALID_ID),
    offset_(0)
  {}
  PartColDesc(int32_t spec_id, int64_t column_id, int64_t offset)
  : spec_id_(spec_id),
    column_id_(column_id),
    offset_(offset)
  {}
  TO_STRING_KV(K_(spec_id), K_(column_id), K_(offset));
  int32_t spec_id_;
  uint64_t column_id_;
  int64_t offset_;
};

// struct ObFileFilterNode
// {
//   ObFileFilterNode()
//   : col_idx_(-1),
//     spec_id_(-1),
//     is_min_max_filter_(false),
//     filter_(nullptr) {}
//   ObFileFilterNode(int64_t col_idx, bool is_min_max_filter, ObPhysicalFilterExecutor *filter)
//   : col_idx_(col_idx),
//     is_min_max_filter_(is_min_max_filter),
//     filter_(filter)
//   {}

//   TO_STRING_KV(K_(col_idx), K_(is_min_max_filter), KP_(filter));

//   int64_t col_idx_;
//   int64_t spec_id_;
//   bool is_min_max_filter_;
//   ObPhysicalFilterExecutor *filter_;
// };

// class ObIcebergFileFilter
// {
// public:
//   using ObFileFilterNodes = common::ObSEArray<ObFileFilterNode, 4>;
//   ObIcebergFileFilter(common::ObIAllocator &allocator,
//                       ObExecContext &exec_ctx,
//                       ObFileFilterSpec &file_filter_spec);
//   ~ObIcebergFileFilter();

//   static int generate_pd_filter_spec(ObIAllocator &allocator,
//                                      ObExecContext &exec_ctx,
//                                      const ObDMLStmt *stmt,
//                                      const ObIArray<ObRawExpr*> &filter_exprs,
//                                      ObFileFilterSpec &file_filter_sepc);

//   int init(common::ObIArray<int64_t> &column_ids,
//            common::ObIArray<ObObjMeta> &column_metas,
//            ObIArray<PartColDesc> &part_column_descs);
//   /// Check whether we can skip filtering.
//   int check_file(iceberg::ManifestEntry &manifest_entry,
//                  bool &is_filtered);
//   /// Check whether we can use skipping index.
//   bool can_use_skipping_index() const
//   {
//     return !skipping_filter_nodes_.empty();
//   }
//   const sql::ObPushdownFilterExecutor *get_pushdown_filter() { return pushdown_filter_; }
//   TO_STRING_KV(K_(is_inited), K_(skipping_filter_nodes));
// private:
//   DISALLOW_COPY_AND_ASSIGN(ObIcebergFileFilter);

//   int generate_pd_filter();
//   int build_skipping_filter_nodes(ObPushdownFilterExecutor &filter,
//                                   const ObIArray<PartColDesc> &part_column_descs);
//   int extract_skipping_filter_from_tree(ObPushdownFilterExecutor &filter,
//                                         const ObIArray<PartColDesc> &part_column_descs);


//   int is_filtered_by_skipping_index(iceberg::ManifestEntry &manifest_entry,
//                                     ObFileFilterNode &node);

//   int construnct_min_max_param(blocksstable::ObMinMaxFilterParam &param,
//                                iceberg::ManifestEntry &manifest_entry,
//                                const uint64_t column_id,
//                                bool &can_use_min_max);

// private:
//   bool is_inited_;
//   common::ObIAllocator &allocator_;
//   ObExecContext &exec_ctx_;
//   ObFileFilterSpec &file_filter_spec_;
//   ObEvalCtx *eval_ctx_;
//   ObPushdownOperator *pd_expr_op_;
//   char **ori_frames_;
//   uint64_t ori_frame_cnt_;

//   common::ObIArray<int64_t> *column_ids_;
//   common::ObIArray<ObObjMeta> *column_metas_;
//   common::ObIArray<PartColDesc> *identity_part_columns_;
//   sql::ObPushdownFilterExecutor *pushdown_filter_;
//   ObFileFilterNodes skipping_filter_nodes_;
//   blocksstable::ObSkipIndexFilterExecutor skip_filter_executor_;
//   char buf_[1];
//   ObBitVector *skip_bit_;
// };

class ObIcebergFileFilter : public ObLakeTablePushDownFilter
{
private:
  class IcebergMinMaxFilterParamBuilder : public MinMaxFilterParamBuilder
  {
  public:
    explicit IcebergMinMaxFilterParamBuilder(iceberg::ManifestEntry &manifest_entry,
                                             ObIAllocator &allocator)
    : manifest_entry_(manifest_entry),
      allocator_(allocator)
    {}
    virtual ~IcebergMinMaxFilterParamBuilder() {}
    int build(const int32_t ext_tbl_col_id, const ObColumnMeta &column_meta,
              blocksstable::ObMinMaxFilterParam &param) override;
    int next_range(const int64_t column_id, int64_t &offset, int64_t &rows)
    {
      return OB_NOT_SUPPORTED;
    }
  private:
    iceberg::ManifestEntry &manifest_entry_;
    ObIAllocator &allocator_;
  };

public:
  ObIcebergFileFilter(ObExecContext &exec_ctx,
                      ObLakeTablePushDownFilterSpec &file_filter_spec,
                      common::ObIArray<PartColDesc> *identity_part_columns)
      : ObLakeTablePushDownFilter(exec_ctx, file_filter_spec),
        identity_part_columns_(identity_part_columns)
  {
  }

  virtual ~ObIcebergFileFilter()
  {}

  int check_file(iceberg::ManifestEntry &manifest_entry,
                 bool &is_filtered);

private:
  common::ObIArray<PartColDesc> *identity_part_columns_;
};

}
}
#endif
