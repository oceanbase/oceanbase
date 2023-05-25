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

#ifndef SRC_SQL_RESOLVER_DDL_OB_ANALYZE_STMT_H_
#define SRC_SQL_RESOLVER_DDL_OB_ANALYZE_STMT_H_

#include "ob_ddl_stmt.h"
#include "share/stat/ob_stat_define.h"

namespace oceanbase
{
namespace common
{
struct ObTableStatParam;
}
namespace sql
{

enum StatisticType
{
  AllStatistics,
  TableStatistics,
  ColumnStatistics,
  InvalidStatistics
};

class ObAnalyzeStmt: public ObStmt, public ObICmd 
{
public:
  ObAnalyzeStmt();
  virtual ~ObAnalyzeStmt();
  virtual int get_cmd_type() const { return get_stmt_type(); }

  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_table(const ObString database_name,
                 const uint64_t database_id,
                 const ObString table_name,
                 const uint64_t table_id,
                 const share::schema::ObTableType table_type)
  {
    database_name_ = database_name;
    database_id_ = database_id;
    table_name_ = table_name;
    table_id_ = table_id;
    ref_table_type_ = table_type;
  }

  void set_sample_info(const ObAnalyzeSampleInfo &sample_info)
  { sample_info_ = sample_info; }
  void set_statistic_scope(const StatisticType statistic_type)
  { statistic_type_ = statistic_type; }
  void set_is_drop() { is_drop_ = true; }
  uint64_t get_tenant_id() { return tenant_id_; }
  uint64_t get_table_id() { return table_id_; }
  share::schema::ObTableType get_table_type() { return ref_table_type_; }
  StatisticType get_statistic_type() { return statistic_type_; }
  const ObAnalyzeSampleInfo &get_sample_info() const { return sample_info_; }
  ObAnalyzeSampleInfo &get_sample_info() { return sample_info_; }
  bool is_delete_histogram() { return is_drop_; }
  ObString &get_partition_name() { return partition_name_; }

  int set_column_params(const ObIArray<ObColumnStatParam> &col_params);
  ObColumnStatParam *get_column_param(const uint64_t column_id);

  ObIArray<ObColumnStatParam> &get_column_params() { return column_params_; }
  const ObIArray<ObColumnStatParam> &get_column_params() const { return column_params_; }

  void set_part_level(share::schema::ObPartitionLevel part_level)
  {
    part_level_ = part_level;
  }

  void set_part_cnt(int64_t total_part_cnt)
  {
    total_part_cnt_ = total_part_cnt;
  }

  void set_degree(uint64_t degree)
  {
    if (degree > 1) {
      parallel_degree_ = degree;
    }
  }
  int fill_table_stat_param(ObExecContext &ctx, common::ObTableStatParam &param);

  int set_part_infos(ObIArray<PartInfo> &part_infos);
  int set_subpart_infos(ObIArray<PartInfo> &subpart_infos);
  ObIArray<PartInfo> &get_part_infos() { return partition_infos_; }
  ObIArray<PartInfo> &get_subpart_infos() { return subpartition_infos_; }
  int set_all_part_infos(ObIArray<PartInfo> &all_part_infos);
  int set_all_subpart_infos(ObIArray<PartInfo> &all_subpart_infos);

  int set_part_ids(ObIArray<int64_t> &part_ids);
  int set_subpart_ids(ObIArray<int64_t> &subpart_ids);

  void set_gather_subpart_hist(bool gather_subpart_hist) { gather_subpart_hist_ = gather_subpart_hist; }


  TO_STRING_KV(K_(tenant_id),
               K_(table_id),
               K_(statistic_type),
               K_(sample_info),
               K_(parallel_degree),
               K_(partition_infos),
               K_(subpartition_infos),
               K_(part_ids),
               K_(subpart_ids),
               K_(ref_table_type),
               K_(gather_subpart_hist));

private:
  uint64_t tenant_id_;
  ObString database_name_;
  uint64_t database_id_;

  ObString table_name_;
  uint64_t table_id_;

  share::schema::ObPartitionLevel part_level_;
  int64_t total_part_cnt_;
  ObString partition_name_;

  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> partition_infos_;
  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> subpartition_infos_;

  common::ObSEArray<common::ObColumnStatParam, 4, common::ModulePageAllocator, true> column_params_;
  StatisticType statistic_type_;
  ObAnalyzeSampleInfo sample_info_;

  int64_t parallel_degree_;

  bool is_drop_;

  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> part_ids_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> subpart_ids_;
  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> all_partition_infos_;
  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> all_subpartition_infos_;
  share::schema::ObTableType ref_table_type_;
  bool gather_subpart_hist_;
  DISALLOW_COPY_AND_ASSIGN(ObAnalyzeStmt);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* SRC_SQL_RESOLVER_DDL_OB_ANALYZE_STMT_H_ */
