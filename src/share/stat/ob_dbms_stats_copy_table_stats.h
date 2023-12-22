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

#ifndef OB_DBMS_STATS_COPY_TABLE_STATS_H
#define OB_DBMS_STATS_COPY_TABLE_STATS_H

#include "share/stat/ob_opt_table_stat_cache.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_stat_define.h"
namespace oceanbase
{
using namespace sql;
namespace common
{
enum ObCopyLevel {
  InvalidCopyLevel = -1,
  CopyOnePartLevel,
  CopyTwoPartLevel
};

enum ObCopyResType {
  InvalidCopyResType = -1,
  CopyDstPartLowerBound,
  CopyDstPartUpperBound,
  CopyPrePartUpperBound,
  CopySrcPartMinVal,
  CopySrcPartMaxVal,
  CopyMaxSrcValDstBound
};

struct ObCopyPartInfo {
  ObCopyPartInfo():
    pre_part_upper_bound_(),
    part_lower_bound_(),
    part_upper_bound_(),
    src_part_lower_bound_(),
    min_res_type_(InvalidCopyResType),
    max_res_type_(InvalidCopyResType),
    is_normal_range_part_(false),
    is_hash_part_(false)
  { }
  common::ObObj pre_part_upper_bound_;
  common::ObObj part_lower_bound_;
  common::ObObj part_upper_bound_;
  common::ObObj src_part_lower_bound_;
  ObCopyResType min_res_type_;
  ObCopyResType max_res_type_;
  bool is_normal_range_part_;
  bool is_hash_part_;
  TO_STRING_KV(K(pre_part_upper_bound_), K(part_lower_bound_), K(part_upper_bound_),
               K(min_res_type_), K(max_res_type_), K(is_normal_range_part_), K(is_hash_part_));
};

struct CopyTableStatHelper {
  CopyTableStatHelper(ObIAllocator *alloc):
    tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    owner_(""),
    table_name_(""),
    srcpart_name_(""),
    dstpart_name_(""),
    scale_factor_(1),
    flags_(0.0),
    force_copy_(false),
    src_part_stat_(NULL),
    dst_part_id_(-1),
    dst_part_map_(),
    part_column_ids_(),
    allocator_(alloc)
  { }
  int copy_part_col_stat(bool is_subpart,
                        const ObIArray<ObOptColumnStatHandle> &col_handles,
                        ObIArray<ObOptTableStat *> &table_stats,
                        ObIArray<ObOptColumnStat *> &column_stats);
  int copy_part_stat(ObIArray<ObOptTableStat *> &table_stats);
  int copy_col_stat(bool is_subpart,
                    const ObIArray<ObOptColumnStatHandle> &col_handles,
                    ObIArray<ObOptColumnStat *> &column_stats);
  int copy_min_val(const common::ObObj &src_min_val,
                   common::ObObj &dst_min_val,
                   ObCopyPartInfo *dst_part_info);
  int copy_max_val(const common::ObObj &src_max_val,
                   common::ObObj &dst_max_val,
                   ObCopyPartInfo *dst_part_info);
  int check_range_part(bool is_subpart, const ObIArray<ObOptColumnStatHandle> &col_handles, bool &flag);
  // int copy_histogram(const ObHistogram &src_hist,
  //                    ObHistogram &dst_hist,
  //                    ObCopyPartInfo *dst_part_info);
  // int set_dst_hist_buckets(ObHistogram &dst_hist,
  //                          const common::ObObj &endpoint_val_1,
  //                          const common::ObObj &endpoint_val_2);
  uint64_t tenant_id_;
  uint64_t table_id_;
  ObString owner_;
  ObString table_name_;
  ObString srcpart_name_;
  ObString dstpart_name_;
  double scale_factor_;
  double flags_;  // unused
  bool force_copy_;

  ObOptTableStat *src_part_stat_;
  int64_t dst_part_id_;
  hash::ObHashMap<uint64_t, ObCopyPartInfo *> dst_part_map_;
  ObSEArray<uint64_t, 8> part_column_ids_;
  ObSEArray<uint64_t, 8> subpart_column_ids_;

  ObIAllocator *allocator_;
  TO_STRING_KV(K(tenant_id_), K(table_id_), K(owner_), K(table_name_),
               K(srcpart_name_), K(dstpart_name_), K(scale_factor_), K(flags_),
               K(force_copy_), K(part_column_ids_));
};

class ObDbmsStatsCopyTableStats
{
public:
  static int check_parts_valid(sql::ObExecContext &ctx,
                               const CopyTableStatHelper &helper,
                               const ObTableStatParam &table_stat_param,
                               ObCopyLevel &copy_level);

  static int extract_partition_column_ids(CopyTableStatHelper &copy_stat_helper,
                                          const ObTableSchema *table_schema);

  static int copy_tab_col_stats(sql::ObExecContext &ctx,
                                ObTableStatParam &table_stat_param,
                                CopyTableStatHelper &copy_stat_helper);

  static int find_src_tab_stat(const ObTableStatParam &table_stat_param,
                               const ObIArray<ObOptTableStatHandle> &history_tab_handles,
                               ObOptTableStat *&src_tab_stat);

  static int get_dst_part_infos(const ObTableStatParam &table_stat_param,
                                CopyTableStatHelper &helper,
                                const ObTableSchema *table_schema,
                                const ObCopyLevel copy_level,
                                bool &is_found);

  static int get_copy_part_info(const ObTableSchema *table_schema,
                                const ObCopyLevel copy_level,
                                const int64_t idx,
                                CopyTableStatHelper &helper,
                                ObCopyPartInfo *&dst_part_info);

  static int get_src_part_info(const ObTableSchema *table_schema,
                               CopyTableStatHelper &helper,
                               const ObBasePartition *part,
                               const ObBasePartition *pre_part,
                               const ObCopyLevel copy_level);

  static int get_dst_part_info(const ObTableSchema *table_schema,
                               CopyTableStatHelper &helper,
                               const ObBasePartition *part,
                               const ObBasePartition *pre_part,
                               const ObCopyLevel copy_level);

  static int get_hash_or_default_part_info(const ObTableSchema *table_schema,
                                           CopyTableStatHelper &helper,
                                           const ObCopyLevel copy_level,
                                           bool is_hash);

  static int get_normal_list_part_info(const ObTableSchema *table_schema,
                                       CopyTableStatHelper &helper,
                                       const ObBasePartition *part,
                                       const ObCopyLevel copy_level,
                                       int64_t list_val_cnt);

  static int get_range_part_info(const ObTableSchema *table_schema,
                                        CopyTableStatHelper &helper,
                                        const ObBasePartition *part,
                                        const ObBasePartition *pre_part,
                                        const ObCopyLevel copy_level,
                                        int64_t rowkey_idx);
};

}
}
#endif // OB_DBMS_STATS_COPY_TABLE_STATS_H