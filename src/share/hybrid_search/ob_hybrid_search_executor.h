/**
 * Copyright (c) 2025 OceanBase
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

#include "ob_query_parse.h"
#include "pl/ob_pl.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace share {

struct ObHybridSearchArg {
  ObHybridSearchArg() {}
  bool is_valid() const {
    return !table_name_.empty() && !search_params_.empty();
  }
  TO_STRING_KV(K_(table_name), K_(search_params));

  enum class SearchType : int8_t {
    SEARCH = 0,
    GET_SQL = 1,
  };

  ObString table_name_;
  ObString search_params_;
  SearchType search_type_;
};

class ObHybridSearchExecutor {
public:
  enum class SearchResultType : int8_t {
    JSON_RESULT = 0,
    SQL_RESULT = 1,
  };

  ObHybridSearchExecutor();
  ~ObHybridSearchExecutor();
  DISABLE_COPY_ASSIGN(ObHybridSearchExecutor);

  int init(const pl::ObPLExecCtx &ctx, const ObHybridSearchArg &arg);
  int init(sql::ObExecContext *ctx, const ObHybridSearchArg &arg);
  int execute(const ObString &query_str, ObIAllocator &allocator,
              ObString &result);

  int execute_search(ObObj &query_res);

  int execute_get_sql(ObString &sql_result);

  int init_search_arg(const ObHybridSearchArg &arg);

private:
  int parse_search_params(const ObString &search_params_str,
                          share::ObQueryReqFromJson *&query_req,
                          bool need_wrap_result);

  int do_search(ObString &json_result);
  /// int do_search_with_retry(ObString &json_result);

  int do_get_sql(const ObString &search_params_str, ObString &sql_result, bool need_wrap_result = false);
  /// int do_get_sql_with_retry(ObString &sql_result);

  int generate_sql_from_params(const ObString &search_params_str, ObString &sql_result);
  int construct_column_index_info(ObIAllocator &alloc, ObESQueryParser &parser);
  int get_basic_column_names(const ObTableSchema *table_schema, ObIArray<ObString> &col_names);
  int extract_partition_column_ids(const ObPartitionKeyInfo &part_key_info,
                                   hash::ObPlacementHashSet<uint64_t, 32> &column_id_set,
                                   ObIArray<uint64_t> &column_ids);
  int get_partition_info(const ObTableSchema *table_schema, ObESQueryParser &parser);

private:
  sql::ObExecContext *ctx_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObSchemaChecker schema_checker_;

  uint64_t tenant_id_;
  uint64_t table_id_;
  ObString search_params_;
  SearchResultType result_type_;
  ObHybridSearchArg search_arg_;
  ObArenaAllocator allocator_;
};
} // namespace share
} // namespace oceanbase