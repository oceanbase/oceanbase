/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CATALOG_STATS_ESTIMATOR_H
#define OB_CATALOG_STATS_ESTIMATOR_H

#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/stat/catalog/ob_catalog_stat_item.h"

namespace oceanbase
{
namespace common
{

/**
 * Catalog table statistics estimator base class
 * Independent from internal table statistics estimator (ObStatsEstimator)
 * Used exclusively for catalog table statistics estimation
 *
 * Key differences from ObStatsEstimator:
 * - Uses catalog_name_ for three-part naming: `catalog`.`db`.`table`
 * - Uses partition_value_ for partition matching instead of partition_id
 * - Uses sql_schema_guard_ for catalog table schema access
 * - Collects partition_values_ automatically during estimation
 */
struct SessionCharsetState
{
  ObCharsetType client_charset_type_;
  ObCharsetType connection_charset_type_;
  ObCharsetType result_charset_type_;
  ObCollationType collation_type_;
};

class ObCatalogStatsEstimator
{
public:
  explicit ObCatalogStatsEstimator(sql::ObExecContext &ctx, ObIAllocator &allocator);

protected:
  int gen_select_filed();

  int64_t get_item_size() const
  {
    return stat_items_.count();
  }

  int decode(ObIAllocator &allocator);

  int add_result(ObObj &obj)
  {
    return results_.push_back(obj);
  }

  // Catalog table specific: use ObOptCatalogStat instead of ObOptStat
  int do_estimate(const ObOptCatalogStatGatherParam &gather_param,
                  const ObString &raw_sql,
                  bool need_copy_basic_stat,
                  ObOptCatalogStat &src_external_stat,
                  ObIArray<ObOptCatalogStat> &dst_external_stats);

public:
  int prepare_and_store_session(sql::ObSQLSessionInfo *session,
                                sql::ObSQLSessionInfo::StmtSavedValue *&session_value,
                                SessionCharsetState &old_state);
  int restore_session(sql::ObSQLSessionInfo *session,
                      sql::ObSQLSessionInfo::StmtSavedValue *session_value,
                      const SessionCharsetState &old_state);

  // Catalog table specific pack function: supports three-part naming `catalog`.`db`.`table`
  int pack(ObSqlString &raw_sql_str);

  inline void set_from_table(const ObString &from_table)
  {
    from_table_ = from_table;
  }

  int init_escape_char_names(common::ObIAllocator &allocator,
                             const ObOptCatalogStatGatherParam &param);

  int fill_sample_info(common::ObIAllocator &alloc, double est_percent, bool block_sample);
  int fill_sample_info(common::ObIAllocator &alloc, const ObCatalogAnalyzeSampleInfo &sample_info);

  int fill_parallel_info(common::ObIAllocator &alloc, int64_t degree);

  int fill_query_timeout_info(common::ObIAllocator &alloc, const int64_t duration_timeout);

  int fill_single_partition_info(const ObIArray<ObString> &part_col_names,
                                 ObIAllocator &allocator,
                                 const ObIArrayWrap<ObString> &partition_values);

  int fill_partition_info(const ObIArray<ObString> &part_col_names,
                          ObIAllocator &allocator,
                          const ObIArray<ObCatalogExtPartitionInfo> &partition_infos);

  int add_hint(const ObString &hint_str, common::ObIAllocator &alloc);

  // Catalog table specific add_stat_item method (外表专用，不复用内表方法)
  template <class T>
  int add_stat_item(const T &item) {
    int ret = OB_SUCCESS;
    ObCatalogStatItem *cpy = NULL;
    if (!item.is_needed()) {
      // do nothing
    } else if (OB_ISNULL(cpy = copy_catalog_stat_item(allocator_, item))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to copy catalog stat item", K(ret));
    } else if (OB_FAIL(stat_items_.push_back(cpy))) {
      LOG_WARN("failed to push back catalog stat item", K(ret));
    }
    return ret;
  }

  void reset_select_items()
  {
    stat_items_.reset();
    select_fields_.reset();
  }
  void reset_sample_hint()
  {
    sample_hint_.reset();
  }
  void reset_other_hint()
  {
    other_hints_.reset();
  }

private:
  int64_t scale_row_count_by_sample(int64_t row_cnt) const;
  int copy_basic_catalog_stat(const ObIArray<ObCatalogColumnStatParam> &column_params,
                              ObOptCatalogStat &src_catalog_stat,
                              ObIArray<ObOptCatalogStat> &dst_catalog_stats);
  int copy_basic_col_stats(const int64_t cur_row_cnt,
                           const int64_t total_row_cnt,
                           const ObIArray<ObCatalogColumnStatParam> &column_params,
                           ObIArray<ObOptCatalogColumnStat *> &src_col_stats,
                           ObIArray<ObOptCatalogColumnStat *> &dst_col_stats);

protected:
  sql::ObExecContext &ctx_;
  ObIAllocator &allocator_;

  ObString catalog_name_; // Catalog table: catalog name for three-part naming
  ObString db_name_;
  ObString tab_name_;
  ObString from_table_;
  ObSqlString select_fields_;
  ObString sample_hint_; // TODO: Catalog table sampling not yet implemented, placeholder for
                         // future SAMPLE FILES support
  ObString other_hints_;
  ObString partition_string_;
  ObString
      group_by_string_; // Catalog table: GROUP BY partition columns for multi-partition statistics
  ObString where_string_;

  // Catalog table stat items (外表专用，不复用内表的 ObStatItem)
  ObArray<ObCatalogStatItem *> stat_items_;
  ObArray<ObObj> results_;
  double sample_value_;
  bool is_block_sample_;

  // Catalog table: current row's partition_value (set from ObCatalogPartitionValue::decode, only used in
  // copy_basic_external_stat)
  ObString current_partition_value_;

  // Catalog table: collected all partition_values (automatically collected in do_estimate_external
  // if stat_items_ contains ObCatalogPartitionValue)
  ObSEArray<ObString, 4> collected_partition_values_;

  // Catalog table: partition column names (used to construct partition_value from SQL result)
  ObSEArray<ObString, 4> partition_column_names_;

  // Catalog table stat pointers (used by ObCatalogStatItem)
  share::ObOptCatalogTableStat *tmp_tab_stat_ptr_;
  ObIArray<share::ObOptCatalogColumnStat *> *tmp_col_stats_ptr_;
};
}
}

#endif // OB_CATALOG_STATS_ESTIMATOR_H
