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

#pragma once

#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase {
namespace pl {
struct ObPLExecCtx;
}
namespace sql
{
class ObExecContext;
}

namespace storage {

// ObVectorRefreshIndexArg for DBMS_VECTOR.REFRESH_INDEX
struct ObVectorRefreshIndexArg {
public:
  static const int64_t DEFAULT_REFRESH_THRESHOLD = 10000;
  ObVectorRefreshIndexArg() : refresh_threshold_(DEFAULT_REFRESH_THRESHOLD) {}
  bool is_valid() const { return !idx_name_.empty() && !table_name_.empty(); }
  TO_STRING_KV(K_(idx_name), K_(table_name), K_(idx_vector_col),
               K_(refresh_type), K_(refresh_threshold));

public:
  ObString idx_name_;
  ObString table_name_;
  ObString idx_vector_col_;
  ObString refresh_type_; // COMPLETE / FAST.
  // If delta_buf_table's row count is greater than refresh_threshold_, refresh
  // is triggered.
  int64_t refresh_threshold_;
};

struct ObVectorRefreshIndexInnerArg {
public:
  static const int64_t DEFAULT_REFRESH_THRESHOLD = 10000;
  ObVectorRefreshIndexInnerArg() : idx_table_id_(OB_INVALID_ID), refresh_threshold_(DEFAULT_REFRESH_THRESHOLD) {}
  bool is_valid() const { return idx_table_id_ != OB_INVALID_ID; }
  TO_STRING_KV(K_(idx_table_id), K_(refresh_type), K_(refresh_threshold));

public:
  int64_t idx_table_id_;
  ObString refresh_type_; // COMPLETE / FAST.
  // If delta_buf_table's row count is greater than refresh_threshold_, refresh
  // is triggered.
  int64_t refresh_threshold_;
};

// ObVectorRebuildIndexArg for DBMS_VECTOR.REBUILD_INDEX
struct ObVectorRebuildIndexArg {
public:
  static constexpr double DEFAULT_REBUILD_THRESHOLD = 0.2;
  ObVectorRebuildIndexArg()
      : delta_rate_threshold_(DEFAULT_REBUILD_THRESHOLD),
        idx_parallel_creation_(1) {}
  bool is_valid() const { return !idx_name_.empty() && !table_name_.empty(); }
  TO_STRING_KV(K_(idx_name), K_(table_name), K_(idx_vector_col),
               K_(delta_rate_threshold), K_(idx_organization),
               K_(idx_distance_metrics), K_(idx_parameters),
               K_(idx_parallel_creation));

public:
  ObString idx_name_;
  ObString table_name_;
  ObString idx_vector_col_;
  // If (delta_buf_table's row count + index_id_table's row count) /
  // data_table's row count is greater than delta_rate_threshold_, rebuild is
  // triggered.
  double delta_rate_threshold_;
  ObString idx_organization_;     // DEFAULT: IN MEMORY NEIGHBOR GRAPH
  ObString idx_distance_metrics_; // DEFAULT: EUCLIDEAN
  ObString idx_parameters_; // parameters for different vector-index algorithm
  int64_t idx_parallel_creation_; // DEFAULT: 1
};

struct ObVectorRebuildIndexInnerArg {
public:
  static constexpr double DEFAULT_REBUILD_THRESHOLD = 0.2;
  ObVectorRebuildIndexInnerArg()
      : idx_table_id_(OB_INVALID_ID),
        delta_rate_threshold_(DEFAULT_REBUILD_THRESHOLD),
        idx_parallel_creation_(1) {}
  bool is_valid() const { return idx_table_id_ != OB_INVALID_ID; }
  TO_STRING_KV(K_(idx_table_id),
               K_(delta_rate_threshold), K_(idx_organization),
               K_(idx_distance_metrics), K_(idx_parameters),
               K_(idx_parallel_creation));

public:
  int64_t idx_table_id_;
  // If (delta_buf_table's row count + index_id_table's row count) /
  // data_table's row count is greater than delta_rate_threshold_, rebuild is
  // triggered.
  double delta_rate_threshold_;
  ObString idx_organization_;     // DEFAULT: IN MEMORY NEIGHBOR GRAPH
  ObString idx_distance_metrics_; // DEFAULT: EUCLIDEAN
  ObString idx_parameters_; // parameters for different vector-index algorithm
  int64_t idx_parallel_creation_; // DEFAULT: 1
};

class ObVectorRefreshIndexExecutor {
public:
  static const int MAX_REFRESH_RETRY_THRESHOLD = 3;

  enum class VectorIndexAuxType : int8_t {
    DOMAIN_INDEX = 0,   // FARM COMPAT WHITELIST
    INDEX_ID_INDEX = 1,
    MOCK_INDEX_1 = 2,
    MOCK_INDEX_2 = 3,
  };

  ObVectorRefreshIndexExecutor();
  ~ObVectorRefreshIndexExecutor();
  DISABLE_COPY_ASSIGN(ObVectorRefreshIndexExecutor);
  int execute_refresh(pl::ObPLExecCtx &ctx,
                      const ObVectorRefreshIndexArg &arg);
  int execute_refresh_inner(pl::ObPLExecCtx &ctx,
                      const ObVectorRefreshIndexInnerArg &arg);
  int execute_rebuild(pl::ObPLExecCtx &ctx,
                      const ObVectorRebuildIndexArg &arg);
  int execute_rebuild_inner(pl::ObPLExecCtx &ctx,
                      const ObVectorRebuildIndexInnerArg &arg);

private:
  static int check_min_data_version(const uint64_t tenant_id,
                                    const uint64_t min_data_version,
                                    const char *errmsg);
  static int resolve_table_name(const ObCollationType cs_type,
                                const ObNameCaseMode case_mode,
                                const bool is_oracle_mode, const ObString &name,
                                ObString &database_name, ObString &table_name);
  static void upper_db_table_name(const ObNameCaseMode case_mode,
                                  const bool is_oracle_mode, ObString &name);
  static int to_refresh_method(const ObString &arg_refresh_method,
                               share::schema::ObVectorRefreshMethod &method,
                               bool is_rebuild = false);
  static int to_vector_index_organization(
      const ObString &idx_organization_str,
      share::schema::ObVectorIndexOrganization &idx_organization);
  static int to_vector_index_distance_metric(
      const ObString &idx_distance_metric_str,
      share::schema::ObVetcorIndexDistanceMetric &idx_distance_metric);
  static int is_refresh_retry_ret_code(int ret_code);
  static int get_vector_index_column_name(
      const share::schema::ObTableSchema *base_table_schema,
      const share::schema::ObTableSchema *index_id_schema, ObString &col_name);
  int generate_vector_aux_index_name(VectorIndexAuxType index_type,
                                     const uint64_t data_table_id,
                                     const ObString &index_name,
                                     ObString &real_index_name);
  int mock_check_idx_col_name(
      const ObString &idx_col_name,
      const share::schema::ObTableSchema *base_table_schema,
      const share::schema::ObTableSchema *delta_buf_table_schema,
      const share::schema::ObTableSchema *index_id_table_schema);
  int check_idx_col_name(
      const ObString &idx_col_name,
      const share::schema::ObTableSchema *base_table_schema,
      const share::schema::ObTableSchema *delta_buf_table_schema,
      const share::schema::ObTableSchema *index_id_table_schema);
  // Only for mock testing.
  int mock_resolve_and_check_table_valid(
      const ObString &arg_idx_name, const ObString &arg_base_name,
      const ObString &idx_col_name,
      const share::schema::ObTableSchema *&base_table_schema,
      const share::schema::ObTableSchema *&delta_buf_table_schema,
      const share::schema::ObTableSchema *&index_id_table_schema);
  int resolve_and_check_table_valid(
      const ObString &arg_idx_name, const ObString &arg_base_name,
      const ObString &idx_col_name,
      const share::schema::ObTableSchema *&base_table_schema,
      const share::schema::ObTableSchema *&delta_buf_table_schema,
      const share::schema::ObTableSchema *&index_id_table_schema);
  int resolve_table_id_and_check_table_valid(
      const int64_t idx_table_id,
      const share::schema::ObTableSchema *&base_table_schema,
      const share::schema::ObTableSchema *&delta_buf_table_schema,
      const share::schema::ObTableSchema *&index_id_table_schema,
      bool& in_recycle_bin);
  int resolve_refresh_arg(const ObVectorRefreshIndexArg &arg);
  int resolve_refresh_inner_arg(const ObVectorRefreshIndexInnerArg &arg,
                                bool& in_recycle_bin);
  int resolve_rebuild_arg(const ObVectorRebuildIndexArg &arg);
  int resolve_rebuild_inner_arg(const ObVectorRebuildIndexInnerArg &arg,
                                bool& in_recycle_bin);

  int do_refresh();
  int do_refresh_with_retry();
  int do_rebuild();
  int do_rebuild_with_retry();

private:
  pl::ObPLExecCtx *pl_ctx_;
  sql::ObExecContext *ctx_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObSchemaChecker schema_checker_;

  uint64_t tenant_id_;
  uint64_t base_tb_id_;
  uint64_t domain_tb_id_;
  uint64_t index_id_tb_id_;
  share::schema::ObVectorRefreshMethod refresh_method_;
  share::schema::ObVectorIndexOrganization idx_organization_;
  share::schema::ObVetcorIndexDistanceMetric idx_distance_metrics_;
  ObString idx_parameters_;
  int64_t idx_parallel_creation_;

  double delta_rate_threshold_; // for rebuild index
  int64_t refresh_threshold_;   // for refresh index
};

} // namespace storage
} // namespace oceanbase