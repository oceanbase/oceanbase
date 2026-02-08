/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "pl/ob_pl.h"
#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace pl
{

using share::ObVectorIndexDistAlgorithm;

class ObDBMSVectorMySql
{
public:
  ObDBMSVectorMySql() {}
  virtual ~ObDBMSVectorMySql() {}

#define DECLARE_FUNC(func) \
  static int func(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(refresh_index);
  DECLARE_FUNC(rebuild_index);
  DECLARE_FUNC(refresh_index_inner);
  DECLARE_FUNC(rebuild_index_inner);
  DECLARE_FUNC(index_vector_memory_advisor);
  DECLARE_FUNC(index_vector_memory_estimate);
  DECLARE_FUNC(query_recall);
  DECLARE_FUNC(index_recall);
  DECLARE_FUNC(set_attribute);

#undef DECLARE_FUNC

  /**
   * Calculate recall rate from SQL query string
   * @param ctx: PL execution context
   * @param sql_query: the approximate search SQL query
   * @param recall_rate: output recall rate
   */
  static int calculate_recall_rate_sql(ObPLExecCtx &ctx,
                                       const common::ObString &sql_query,
                                       double &recall_rate);

  /**
   * Get distance function name from distance algorithm type
   * @param dist_algo: distance algorithm type (VIDA_L2, VIDA_IP, VIDA_COS)
   * @param func_name: output distance function name (l2_distance, inner_product, etc.)
   */
  static int get_distance_func_name(ObVectorIndexDistAlgorithm dist_algo,
                                    const char *&func_name);

  /**
   * Parse vectors string into individual vector strings
   * Format: "[0.1,0.2,0.3],[0.4,0.5,0.6]" -> ["[0.1,0.2,0.3]", "[0.4,0.5,0.6]"]
   * @param vectors_str: input vectors string
   * @param allocator: memory allocator
   * @param vector_list: output vector string array
   */
  static int parse_vectors_string(const common::ObString &vectors_str,
                                  common::ObIAllocator *allocator,
                                  common::ObIArray<common::ObString> &vector_list);

  /**
   * Sample vectors from table by random selection
   * @param ctx: PL execution context
   * @param table_name: table name
   * @param db_name: database name
   * @param vector_column: vector column name
   * @param num_vectors: number of vectors to sample
   * @param allocator: memory allocator
   * @param vector_list: output vector string array
   */
  static int sample_vectors_from_table(ObPLExecCtx &ctx,
                                       const common::ObString &table_name,
                                       const common::ObString &db_name,
                                       const common::ObString &vector_column,
                                       int64_t num_vectors,
                                       common::ObIAllocator *allocator,
                                       common::ObIArray<common::ObString> &vector_list);

  /**
   * Get vector index info by index name
   * @param ctx: PL execution context
   * @param table_name: table name
   * @param db_name: database name
   * @param index_name: index name
   * @param table_schema: output table schema
   * @param index_schema: output index schema
   * @param vector_column: output vector column name
   * @param dist_algo: output distance algorithm
   * @param schema_guard: schema guard (caller must keep alive)
   */
  static int get_vector_index_info(ObPLExecCtx &ctx,
                                   const common::ObString &table_name,
                                   const common::ObString &db_name,
                                   const common::ObString &index_name,
                                   const share::schema::ObTableSchema *&table_schema,
                                   const share::schema::ObTableSchema *&index_schema,
                                   common::ObString &vector_column,
                                   ObVectorIndexDistAlgorithm &dist_algo,
                                   share::schema::ObSchemaGetterGuard &schema_guard);

  /**
   * Calculate recall rate for a single vector
   * @param ctx: PL execution context
   * @param table_name: table name with database prefix
   * @param index_name: index name
   * @param vector_column: vector column name
   * @param query_vector: query vector string
   * @param dist_func_name: distance function name
   * @param top_k: top k results
   * @param filter: filter condition (without WHERE keyword)
   * @param table_schema: table schema
   * @param parallel: parallel degree
   * @param parameters: approximate search parameters (e.g., "IVF_NPROBES=100")
   * @param recall_rate: output recall rate
   */
  static int calculate_single_vector_recall(ObPLExecCtx &ctx,
                                            const common::ObString &table_name,
                                            const common::ObString &vector_column,
                                            const common::ObString &query_vector,
                                            const char *dist_func_name,
                                            int64_t top_k,
                                            const common::ObString &filter,
                                            const share::schema::ObTableSchema *table_schema,
                                            int64_t parallel,
                                            const common::ObString &parameters,
                                            double &recall_rate);

  static int parse_idx_param(const ObString &idx_type_str,
                             const ObString &idx_param_str,
                             uint32_t dim_count,
                             ObVectorIndexParam &index_param);

  /**
   * Build brute-force search SQL by removing APPROXIMATE from approximate search SQL
   * @param approx_sql: the approximate search SQL
   * @param brute_sql: output brute-force search SQL
   */
  static int build_brute_force_sql_from_approximate(const common::ObString &approx_sql,
                                                   common::ObSqlString &brute_sql);

  /**
   * Check if SQL query is valid for recall rate calculation
   * @param sql_query: the SQL query to validate
   * @return OB_SUCCESS if valid
   */
  static int check_sql_valid(const common::ObString &sql_query);

  /**
   * Add database prefix to table names in SQL query if not already present
   * @param sql_query: the input SQL query
   * @param database_name: the database name to prefix
   * @param modified_sql: output modified SQL query
   */
  static int add_database_prefix_to_sql(const common::ObString &sql_query,
                                       const common::ObString &database_name,
                                       common::ObSqlString &modified_sql);

private:
  static int get_estimate_memory_str(ObVectorIndexParam index_param,
                                     uint64_t num_vectors,
                                     uint64_t tablet_max_num_vectors,
                                     uint64_t tablet_count,
                                     ObStringBuffer &res_buf,
                                     uint32_t avg_sparse_length = 0);
  static int print_mem_size(uint64_t mem_size, ObStringBuffer &res_buf);
  static int sample_sparse_vectors_and_calc_avg_length(
      sql::ObExecContext &exec_ctx,
      const ObString &database_name,
      const ObString &table_name,
      const ObString &column_name,
      uint64_t num_vectors,
      uint32_t &avg_sparse_length);

  /**
   * Check user's SELECT privilege on the target table
   * @param session_info: SQL session info
   * @param schema_guard: schema guard
   * @param database_name: database name
   * @param table_name: table name
   * @return OB_SUCCESS if user has SELECT privilege
   */
  static int check_table_select_privilege(
      sql::ObSQLSessionInfo *session_info,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObString &database_name,
      const common::ObString &table_name);

  static int merge_session_search_params(sql::ObSQLSessionInfo *session_info,
                                        const share::schema::ObTableSchema *index_schema,
                                        const ObString &parameters_str,
                                        ObSqlString &merged_parameters);
};

} // namespace pl
} // namespace oceanbase