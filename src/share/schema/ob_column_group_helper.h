/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_COLUMN_GROUP_HELPER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_COLUMN_GROUP_HELPER_H_

#include "share/schema/ob_table_schema.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
namespace schema
{

class ObColumnGroupHelper
{
public:
  static int batch_add_column_groups_for_create_table(common::ObISQLClient &sql_client,
                                                       const common::ObIArray<ObTableSchema> &tables);
  static int generate_dml_for_create_table(const ObTableSchema &table,
                                           const uint64_t data_version,
                                           const ObColumnGroupSchema &column_group,
                                           ObDMLSqlSplicer &cg_dml,
                                           ObDMLSqlSplicer &cg_history_dml,
                                           ObDMLSqlSplicer &mapping_dml,
                                           ObDMLSqlSplicer &mapping_history_dml,
                                           int64_t &column_group_cnt,
                                           int64_t &mapping_cnt);
  static int append_column_group_dml_for_create_table(const ObTableSchema &table,
                                                      const uint64_t data_version,
                                                      ObDMLSqlSplicer &cg_dml,
                                                      ObDMLSqlSplicer &cg_history_dml,
                                                      ObDMLSqlSplicer &mapping_dml,
                                                      ObDMLSqlSplicer &mapping_history_dml,
                                                      int64_t &column_group_cnt,
                                                      int64_t &mapping_cnt);
  static int add_column_groups(common::ObISQLClient &sql_client,
                               const ObTableSchema &table,
                               const int64_t schema_version,
                               const bool only_history = false);
  static int insert_column_ids_into_column_group(common::ObISQLClient &sql_client,
                                                 const ObTableSchema &table,
                                                 const int64_t schema_version,
                                                 const common::ObIArray<uint64_t> &column_ids,
                                                 const ObColumnGroupSchema &column_group,
                                                 const bool only_history = false);
  static int check_column_store_valid(const ObTableSchema &table, const uint64_t data_version);
  static int exec_insert_column_group(common::ObISQLClient &sql_client,
                                      const ObTableSchema &table,
                                      const int64_t schema_version,
                                      bool is_history);
  static int exec_insert_column_group_mapping(common::ObISQLClient &sql_client,
                                              const ObTableSchema &table,
                                              const int64_t schema_version,
                                              const ObColumnGroupSchema &column_group,
                                              const common::ObIArray<uint64_t> &column_ids,
                                              const bool is_history);
  static int exec_insert_column_group_mapping(common::ObISQLClient &sql_client,
                                              const ObTableSchema &table,
                                              const int64_t schema_version,
                                              bool is_history);
  static int exec_insert_single_column_group_mapping(common::ObISQLClient &sql_client,
                                                     const ObTableSchema &table,
                                                     const int64_t schema_version,
                                                     const ObColumnGroupSchema &column_group,
                                                     const bool is_history);
  static int gen_column_group_dml(const ObTableSchema &table_schema,
                                  const ObColumnGroupSchema &column_group_schema,
                                  const bool is_history,
                                  const bool is_deleted,
                                  const int64_t schema_version,
                                  ObDMLSqlSplicer &dml);
  static int gen_column_group_mapping_dml(const ObTableSchema &table_schema,
                                          const ObColumnGroupSchema &column_group_schema,
                                          const int64_t column_id,
                                          const bool is_history,
                                          const bool is_deleted,
                                          const int64_t schema_version,
                                          ObDMLSqlSplicer &dml);
  static int delete_from_column_group(common::ObISQLClient &sql_client,
                                      const ObTableSchema &table_schema,
                                      const int64_t schema_version,
                                      const bool is_history = false);
  static int delete_from_column_group_mapping(common::ObISQLClient &sql_client,
                                              const ObTableSchema &table_schema,
                                              const int64_t schema_version,
                                              const bool is_history = false);
  static int delete_column_group(common::ObISQLClient &sql_clinet,
                                 const ObTableSchema &table,
                                 const int64_t schema_version);
  static int update_single_column_group(common::ObISQLClient &sql_client,
                                        const ObTableSchema &new_table_schema,
                                        const ObColumnGroupSchema &ori_cg_schema,
                                        const ObColumnGroupSchema &new_cg_schema);
  static int update_origin_column_group_with_new_schema(common::ObISQLClient &sql_client,
                                                        const int64_t delete_schema_version,
                                                        const int64_t insert_schema_version,
                                                        const ObTableSchema &origin_table_schema,
                                                        const ObTableSchema &new_table_schema);

  static int check_column_group_valid(const ObTableSchema &table,
                                      ObColumnGroupSchema *const *cg_arr,
                                      int64_t cg_cnt,
                                      uint64_t max_used_cg_id,
                                      const ObColumnGroupSchema *hidden_rowkey_cg);
  static int get_store_column_group_count(const ObTableSchema &table,
                                          ObColumnGroupSchema *const *cg_arr,
                                          int64_t cg_cnt,
                                          int64_t &column_group_cnt,
                                          const bool filter_empty_cg = true);
  static int get_store_column_groups(const ObTableSchema &table,
                                     ObColumnGroupSchema *const *cg_arr,
                                     int64_t cg_cnt,
                                     common::ObIArray<const ObColumnGroupSchema *> &column_groups,
                                     const bool filter_empty_cg = true);
  static int has_all_column_group(const ObTableSchema &table,
                                  ObColumnGroupSchema *const *cg_arr,
                                  int64_t cg_cnt,
                                  bool &has_all_column_group);
  static int has_column_group(const ObTableSchema &table,
                              ObColumnGroupSchema *const *cg_arr,
                              int64_t cg_cnt,
                              const ObColumnGroupType &cg_type,
                              bool &has_column_group);
  static int get_column_group_by_id(const ObTableSchema &table,
                                    const CgIdHashArray *cg_id_hash_arr,
                                    uint64_t column_group_id,
                                    ObColumnGroupSchema *&column_group);
  static int get_column_group_by_name(const ObTableSchema &table,
                                      const CgNameHashArray *cg_name_hash_arr,
                                      const common::ObString &cg_name,
                                      ObColumnGroupSchema *&column_group);
  static int is_column_group_exist(const ObTableSchema &table,
                                   ObColumnGroupSchema *const *cg_arr,
                                   int64_t cg_cnt,
                                   const CgNameHashArray *cg_name_hash_arr,
                                   const common::ObString &cg_name,
                                   bool &exist);
  static int get_column_group_index(const ObTableSchema &table,
                                    ObColumnGroupSchema *const *cg_arr,
                                    int64_t cg_cnt,
                                    ObColumnSchemaV2 *const *col_arr,
                                    int64_t col_cnt,
                                    uint64_t max_used_cg_id,
                                    const share::schema::ObColumnParam &param,
                                    const bool need_calculate_cg_idx,
                                    int32_t &cg_idx);
  static int calc_column_group_index(const ObTableSchema &table,
                                     ObColumnSchemaV2 *const *col_arr,
                                     int64_t col_cnt,
                                     const uint64_t column_id,
                                     int32_t &cg_idx);
  static int get_base_rowkey_column_group_index(const ObTableSchema &table,
                                                ObColumnGroupSchema *const *cg_arr,
                                                int64_t cg_cnt,
                                                int32_t &cg_idx);
  static int check_is_normal_cgs_at_the_end(const ObTableSchema &table,
                                            ObColumnGroupSchema *const *cg_arr,
                                            int64_t cg_cnt,
                                            bool &is_normal_cgs_at_the_end);
  static int get_each_column_group(const ObTableSchema &table,
                                   ObColumnGroupSchema *const *cg_arr,
                                   int64_t cg_cnt,
                                   common::ObIArray<ObColumnGroupSchema*> &each_cgs);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_COLUMN_GROUP_HELPER_H_
