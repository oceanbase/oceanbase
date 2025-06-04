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

#ifndef OCEANBASE_STORAGE_OB_TRUNCATE_PARTITION_FILTER_
#define OCEANBASE_STORAGE_OB_TRUNCATE_PARTITION_FILTER_

#include "lib/allocator/ob_allocator.h"
#include "common/ob_common_types.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_truncate_filter_struct.h"
#include "ob_mds_info_distinct_mgr.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRow;
}

namespace share
{
namespace schema
{
struct ObColDesc;
}
}

namespace storage
{

enum class ObTruncateFilterType : uint8_t
{
  NORMAL_FILTER = 0,
  BASE_VERSION_FILTER,
  EMPTY_FILTER,
  FILTER_TYPE_MAX,
};

class ObTruncatePartitionFilter {
public:
  ObTruncatePartitionFilter();
  ~ObTruncatePartitionFilter();
  // for query and dml
  int init(
      ObTablet &tablet,
      const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const common::ObVersionRange &read_version_range,
      const bool has_truncate_flag,
      const bool has_truncate_info,
      ObIAllocator &outer_allocator);
  // for compaction
  int init(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const ObMdsInfoDistinctMgr &mds_info_mgr);
  int switch_info(
      ObTablet &tablet,
      const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const common::ObVersionRange &read_version_range,
      const bool has_truncate_flag,
      const bool has_truncate_info);
  void reuse();
  bool is_valid() const { return is_inited_; }
  // this interface is thread safe
  int filter(
      const blocksstable::ObDatumRow &row,
      bool &filtered,
      const bool check_filter = true,
      const bool check_version = false);
  int check_filter_row_complete(const blocksstable::ObDatumRow &row, bool &complete);
  int combine_to_filter_tree(sql::ObPushdownFilterExecutor *&root_filter);
  OB_INLINE common::ObIAllocator *get_outer_allocator()
  {
    return outer_allocator_;
  }
  OB_INLINE bool need_combined_to_pd_filter() const
  {
    return is_normal_filter() && !has_combined_to_pd_filter_;
  }
  OB_INLINE bool is_valid_filter() const
  {
    return is_normal_filter() || is_base_version_filter();
  }
  OB_INLINE bool is_normal_filter() const
  {
    return ObTruncateFilterType::NORMAL_FILTER == filter_type_;
  }
  OB_INLINE bool is_base_version_filter() const
  {
    return ObTruncateFilterType::BASE_VERSION_FILTER == filter_type_;
  }
  OB_INLINE void set_empty()
  {
    filter_type_ = ObTruncateFilterType::EMPTY_FILTER;
  }
  OB_INLINE void uncombined_from_pd_filter()
  {
    has_combined_to_pd_filter_ = false;
  }
  TO_STRING_KV(K_(is_inited), K_(filter_type), K_(schema_rowkey_cnt), K_(base_version), K_(has_combined_to_pd_filter),
               K_(mds_info_mgr), K_(truncate_info_array), KP_(truncate_filter_node), KP_(truncate_filter_executor),
               KP_(pd_filter_node_with_truncate), KP_(pd_filter_with_truncate), KP_(outer_allocator));
private:
  int init_truncate_filter(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const ObTruncateInfoArray &array);
  int init_column_idxs(const ObTruncateInfoArray &array);
  int init_column_idxs(const ObPartKeyIdxArray &key_idx_array);
  int do_normal_filter(const blocksstable::ObDatumRow &row, bool &filtered);
  int do_base_version_filter(const blocksstable::ObDatumRow &row, bool &filtered);
  static const int64_t COLUMN_IDX_CNT = 4;
  bool is_inited_;
  ObTruncateFilterType filter_type_;
  int64_t schema_rowkey_cnt_;
  int64_t base_version_;
  bool has_combined_to_pd_filter_;
  ObArenaAllocator filter_allocator_;
  ObArenaAllocator truncate_info_allocator_;
  sql::ObPushdownFilterFactory filter_factory_;
  ObMdsInfoDistinctMgr mds_info_mgr_;
  ObTruncateInfoArray truncate_info_array_;
  // for pushdown filter interface
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ObPushdownOperator op_;
  // the entire truncate filter node and executor
  sql::ObPushdownFilterNode *truncate_filter_node_;
  sql::ObTruncateAndFilterExecutor *truncate_filter_executor_;
  // the query pushdown filter after combining truncate filter
  sql::ObPushdownFilterNode *pd_filter_node_with_truncate_;
  sql::ObPushdownFilterExecutor *pd_filter_with_truncate_;
  common::ObIAllocator *outer_allocator_;
  // record all column idx needed for part_key or sub_part_key
  ObSEArray<uint64_t, COLUMN_IDX_CNT> ref_column_idxs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTruncatePartitionFilter);
};

struct ObTruncatePartitionFilterFactory
{
  static int build_truncate_partition_filter(
      ObTablet &tablet,
      const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const common::ObVersionRange &read_version_range,
      ObIAllocator *outer_allocator,
      ObTruncatePartitionFilter *&truncate_part_filter);
  static void destroy_truncate_partition_filter(ObTruncatePartitionFilter *&truncate_part_filter);
};

}
}

#endif
