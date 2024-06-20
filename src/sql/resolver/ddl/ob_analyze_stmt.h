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

class ObAnalyzeTableInfo
{
public:
  ObAnalyzeTableInfo();
  virtual ~ObAnalyzeTableInfo();
  void set_database_name(const ObString &str) { database_name_ = str; }
  void set_database_id(const uint64_t id) { database_id_ = id; }
  void set_table_name(const ObString &str) { table_name_ = str; }
  void set_table_id(const uint64_t id) { table_id_ = id; }
  void set_ref_table_type(const share::schema::ObTableType type) { ref_table_type_ = type; }

  uint64_t get_database_id() { return database_id_; }
  uint64_t get_table_id() { return table_id_; }
  share::schema::ObTableType get_table_type() { return ref_table_type_; }
  ObString &get_partition_name() { return partition_name_; }
  int set_column_params(const ObIArray<ObColumnStatParam> &col_params);
  ObColumnStatParam *get_column_param(const uint64_t column_id);

  ObIArray<ObColumnStatParam> &get_column_params() { return column_params_; }
  const ObIArray<ObColumnStatParam> &get_column_params() const { return column_params_; }

   ObIArray<ObColumnGroupStatParam> &get_column_group_params() { return column_group_params_; }
  const ObIArray<ObColumnGroupStatParam> &get_column_group_params() const { return column_group_params_; }

  void set_part_level(share::schema::ObPartitionLevel part_level)
  {
    part_level_ = part_level;
  }

  int fill_table_stat_param(ObExecContext &ctx, common::ObTableStatParam &param);

  int set_part_infos(ObIArray<PartInfo> &part_infos);
  int set_subpart_infos(ObIArray<PartInfo> &subpart_infos);
  ObIArray<PartInfo> &get_part_infos() { return partition_infos_; }
  ObIArray<PartInfo> &get_subpart_infos() { return subpartition_infos_; }
  int set_all_part_infos(ObIArray<PartInfo> &all_part_infos);
  int set_all_subpart_infos(ObIArray<PartInfo> &all_subpart_infos);

  void set_gather_subpart_hist(bool gather_subpart_hist) { gather_subpart_hist_ = gather_subpart_hist; }
  void set_is_sepcify_subpart(bool is_sepcify_subpart) { is_sepcify_subpart_ = is_sepcify_subpart; }
  int assign(const ObAnalyzeTableInfo &other);

  TO_STRING_KV(K_(table_id),
               K_(partition_infos),
               K_(subpartition_infos),
               K_(column_params),
               K_(ref_table_type),
               K_(gather_subpart_hist),
               K_(is_sepcify_subpart),
               K_(column_params),
               K_(column_group_params));

private:
  ObString database_name_;
  uint64_t database_id_;

  ObString table_name_;
  uint64_t table_id_;

  share::schema::ObPartitionLevel part_level_;
  ObString partition_name_;

  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> partition_infos_;
  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> subpartition_infos_;

  common::ObSEArray<common::ObColumnStatParam, 4, common::ModulePageAllocator, true> column_params_;
  common::ObSEArray<common::ObColumnGroupStatParam, 4, common::ModulePageAllocator, true> column_group_params_;

  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> all_partition_infos_;
  common::ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> all_subpartition_infos_;
  share::schema::ObTableType ref_table_type_;
  bool gather_subpart_hist_;
  bool is_sepcify_subpart_;
};

class ObAnalyzeStmt: public ObStmt, public ObICmd
{
public:
  ObAnalyzeStmt();
  virtual ~ObAnalyzeStmt();
  virtual int get_cmd_type() const { return get_stmt_type(); }

  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int add_table(const ObString database_name,
                 const uint64_t database_id,
                 const ObString table_name,
                 const uint64_t table_id,
                 const share::schema::ObTableType table_type);
  ObIArray<ObAnalyzeTableInfo> & get_tables() { return tables_; }

  void set_sample_info(const ObAnalyzeSampleInfo &sample_info)
  { sample_info_ = sample_info; }
  void set_statistic_scope(const StatisticType statistic_type)
  { statistic_type_ = statistic_type; }
  void set_is_drop() { is_drop_ = true; }
  uint64_t get_tenant_id() { return tenant_id_; }
  StatisticType get_statistic_type() { return statistic_type_; }
  const ObAnalyzeSampleInfo &get_sample_info() const { return sample_info_; }
  ObAnalyzeSampleInfo &get_sample_info() { return sample_info_; }
  bool is_delete_histogram() { return is_drop_; }

  void set_degree(uint64_t degree)
  {
    if (degree > 1) {
      parallel_degree_ = degree;
    }
  }
  int fill_table_stat_params(ObExecContext &ctx, ObIArray<common::ObTableStatParam> &params);


  TO_STRING_KV(K_(tenant_id),
               K_(statistic_type),
               K_(sample_info),
               K_(parallel_degree),
               K_(tables));

private:
  uint64_t tenant_id_;
  //ANALYZE TABLE without histogram clause support multi tables.For example:
  //  ANALYZE TABLE db1.t1, db2.t1;
  //And in other cases, size of tables_ is always 1.
  ObSEArray<ObAnalyzeTableInfo, 1, common::ModulePageAllocator, true> tables_;

  StatisticType statistic_type_;
  ObAnalyzeSampleInfo sample_info_;

  int64_t parallel_degree_;

  bool is_drop_;

  DISALLOW_COPY_AND_ASSIGN(ObAnalyzeStmt);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* SRC_SQL_RESOLVER_DDL_OB_ANALYZE_STMT_H_ */
