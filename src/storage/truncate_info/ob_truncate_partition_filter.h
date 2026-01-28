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

enum class ObTruncateFilterType : uint8_t // FARM COMPAT WHITELIST
{
  NORMAL_FILTER = 0,
  EMPTY_FILTER,
  FILTER_TYPE_MAX,
};

class ObTruncatePartitionFilter {
public:
  ObTruncatePartitionFilter(ObMDSFilterMgr &mds_filter_mgr);
  ~ObTruncatePartitionFilter();
  // for query and dml
  int init(
      ObTablet &tablet,
      const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const common::ObVersionRange &read_version_range,
      const bool has_truncate_info);
  // for compaction
  int init(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const ObTruncateInfoDistinctMgr &mds_info_mgr);
  int switch_info(
      ObTablet &tablet,
      const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const common::ObVersionRange &read_version_range,
      const bool has_truncate_info);
  void reuse();
  bool is_valid() const { return is_inited_; }
  // this interface is thread safe
  int filter(const blocksstable::ObDatumRow &row, bool &filtered) const;
  int check_filter_row_complete(const blocksstable::ObDatumRow &row, bool &complete) const;
  OB_INLINE ObMDSFilterMgr &get_mds_filter_mgr() const { return mds_filter_mgr_; }
  OB_INLINE bool is_valid_filter() const
  {
    return is_normal_filter();
  }
  OB_INLINE bool is_normal_filter() const
  {
    return ObTruncateFilterType::NORMAL_FILTER == filter_type_;
  }
  OB_INLINE void set_empty()
  {
    filter_type_ = ObTruncateFilterType::EMPTY_FILTER;
  }
  OB_INLINE sql::ObTruncateAndFilterExecutor *get_truncate_filter_executor()
  {
    return truncate_filter_executor_;
  }

  OB_INLINE int64_t get_filter_max_val() const { return truncate_info_array_.get_array().at(truncate_info_array_.count() - 1)->commit_version_; }

  TO_STRING_KV(K_(is_inited),
               K_(filter_type),
               K_(schema_rowkey_cnt),
               K_(mds_info_mgr),
               K_(truncate_info_array),
               KP_(truncate_filter_node),
               KP_(truncate_filter_executor));

private:
  int init_truncate_filter(
      const int64_t schema_rowkey_cnt,
      const common::ObIArray<share::schema::ObColDesc> &cols_desc,
      const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
      const ObTruncateInfoArray &array);
  int init_column_idxs(const ObTruncateInfoArray &array);
  int init_column_idxs(const ObPartKeyIdxArray &key_idx_array);
  int do_normal_filter(const blocksstable::ObDatumRow &row, bool &filtered) const;
  static const int64_t COLUMN_IDX_CNT = 4;

  ObMDSFilterMgr &mds_filter_mgr_;
  bool is_inited_;
  ObTruncateFilterType filter_type_;
  int64_t schema_rowkey_cnt_;
  ObArenaAllocator truncate_info_allocator_;
  ObTruncateInfoDistinctMgr mds_info_mgr_;
  ObTruncateInfoArray truncate_info_array_;
  // the entire truncate filter node and executor
  sql::ObPushdownFilterNode *truncate_filter_node_;
  sql::ObTruncateAndFilterExecutor *truncate_filter_executor_;
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
      ObMDSFilterMgr &mds_filter_mgr,
      ObTruncatePartitionFilter *&truncate_part_filter);
  static void destroy_truncate_partition_filter(ObTruncatePartitionFilter *&truncate_part_filter);
};

}
}

#endif
