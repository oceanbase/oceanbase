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

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_RECALL_CALC_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_RECALL_CALC_UTIL_H_

#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/rowkey/ob_rowkey_info.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
  class ObTableSchema;
}
}
}

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObString;
}

namespace sql
{
// Result set for one query: top-k rowkey list
struct ObVectorSearchResult
{
  common::ObArray<common::ObRowkey> rowkeys_;  // rowkey list ordered by distance
  int64_t query_latency_us_;                   // query latency in microseconds

  ObVectorSearchResult();
  void reset();
  int assign(const ObVectorSearchResult &other);

  TO_STRING_KV(K_(rowkeys), K_(query_latency_us));
};

// Utility class for vector recall calculation
class ObVectorRecallCalcUtil final
{
public:
  // SQL timeout for vector recall rate calculation: 5 minutes
  static const int64_t VECTOR_RECALL_RATE_CALC_SQL_TIMEOUT = 5 * 60 * 1000 * 1000; // 300000000 us

  // Recall calculation functions

  /**
   * Execute search SQL and get result rowkeys
   * @param tenant_id: tenant ID
   * @param sql: search SQL to execute
   * @param sql_proxy: MySQL proxy for executing SQL
   * @param rowkey_info: rowkey information for constructing ObRowkey
   * @param allocator: allocator for ObRowkey memory
   * @param result: output search result
   * @param group_id: consumer group id for resource control
   */
  static int execute_search_sql(
      uint64_t tenant_id,
      const common::ObSqlString &sql,
      common::ObMySQLProxy *sql_proxy,
      const common::ObRowkeyInfo *rowkey_info,
      common::ObIAllocator *allocator,
      ObVectorSearchResult &result,
      const int32_t group_id = 0);

  /**
   * Calculate recall rate between approximate and brute-force results
   * @param approx_result: approximate search result
   * @param brute_result: brute-force search result
   * @param recall_rate: output recall rate (0.0 ~ 1.0)
   */
  static int calculate_recall_rate(
      const ObVectorSearchResult &approx_result,
      const ObVectorSearchResult &brute_result,
      double &recall_rate);


  // Helper functions
  /**
   * Parse table name and database name from SQL query
   * @param sql_query: input SQL query string
   * @param default_db_name: default database name if not specified in SQL
   * @param allocator: allocator for string memory
   * @param table_name: output table name
   * @param db_name: output database name
   */
  static int parse_table_name_from_sql(
      const common::ObString &sql_query,
      const common::ObString &default_db_name,
      common::ObIAllocator *allocator,
      common::ObString &table_name,
      common::ObString &db_name);

  /**
   * Get table schema by table name
   * @param tenant_id: tenant ID
   * @param db_name: database name
   * @param table_name: table name
   * @param table_schema: output table schema pointer
   * @param schema_guard: output schema guard (caller must keep it alive while using table_schema)
   */
  static int get_table_schema_by_name(
      uint64_t tenant_id,
      const common::ObString &db_name,
      const common::ObString &table_name,
      const share::schema::ObTableSchema *&table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);

  /**
   * Build modified SQL with primary key columns selection
   * @param sql_query: original SQL query
   * @param table_schema: table schema
   * @param db_name: database name
   * @param table_name: table name
   * @param modified_sql: output modified SQL
   */
  static int build_pk_select_sql(
      const common::ObString &sql_query,
      const share::schema::ObTableSchema *table_schema,
      const common::ObString &db_name,
      const common::ObString &table_name,
      common::ObSqlString &modified_sql);

  /**
   * Inject session-level search parameters (IVF_NPROBES, EF_SEARCH) into approximate search SQL.
   * If the SQL already has PARAMETERS(...), merge session values in; otherwise append PARAMETERS(...).
   * @param approx_sql: approximate search SQL (with APPROXIMATE / APPROX)
   * @param session_ivf_nprobes: session variable ob_ivf_nprobes
   * @param session_hnsw_ef_search: session variable ob_hnsw_ef_search
   * @param allocator: allocator for output buffer
   * @param output_sql: output SQL with parameters injected
   */
  static int inject_session_params_into_approx_sql(
      const common::ObString &approx_sql,
      uint64_t session_ivf_nprobes,
      uint64_t session_hnsw_ef_search,
      common::ObIAllocator *allocator,
      common::ObSqlString &output_sql);

  /**
   * Extract rowkeys from result set
   * @param result: MySQL result set
   * @param rowkey_info: rowkey information for constructing ObRowkey
   * @param allocator: allocator for ObRowkey memory
   * @param rowkeys: output rowkey array
   */
  static int extract_rowkeys_from_result(
      common::sqlclient::ObMySQLResult *result,
      const common::ObRowkeyInfo *rowkey_info,
      common::ObIAllocator *allocator,
      common::ObIArray<common::ObRowkey> &rowkeys);
private:
  // Disallow instantiation
  ObVectorRecallCalcUtil() = delete;
  ~ObVectorRecallCalcUtil() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObVectorRecallCalcUtil);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_RECALL_CALC_UTIL_H_