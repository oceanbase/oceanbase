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

#ifndef OCEANBASE_SHARE_DOMAIN_ID_DEFINE_H_
#define OCEANBASE_SHARE_DOMAIN_ID_DEFINE_H_

#include "deps/oblib/src/lib/allocator/ob_allocator_v2.h"
#include "deps/oblib/src/lib/container/ob_iarray.h"
#include "deps/oblib/src/lib/container/ob_se_array.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
namespace share
{

typedef common::ObSEArray<uint64_t, 2> DomainIdxs; // only support upper 5 col for each domain types

class ObDomainIdUtils final
{
public:
  enum ObDomainIDType
  {
      DOC_ID = 0,
      VID = 1,
      IVFFLAT_CID = 2,
      IVFSQ_CID = 3,
      IVFPQ_CID = 4,
      MAX
  };
public:
  typedef common::ObSEArray<ObString, 2> DomainIds;
  /* DML Resolver Begin */
  static bool is_domain_id_index_col(const void *col_schema);
  static bool check_table_need_column_ref_in_ddl(const void *table_schema);
  /* DML Resolver End */

  /* ObLogTableScan Begin */
  const static char* get_domain_str_by_id(ObDomainIDType type) { return ObDomainIDStrArray[type]; }
  static bool is_domain_id_index_table(const void *table_schema);
  static int check_table_need_domain_id_merge(ObDomainIDType type, const void *table_schema, bool &res);
  static int check_column_need_domain_id_merge(ObDomainIDType type, const void *col_expr, ObIndexType index_type, bool &res);
  static int get_domain_tid_table_by_type(ObDomainIDType type, const void *data_table_schema, uint64_t &domain_id_table_id);
  static int get_domain_tid_table_by_cid(
      ObDomainIDType type,
      void *schema_guard,
      const void *data_table_schema,
      const uint64_t domain_col_id,
      uint64_t &tid);
  // pq center ids need schema_guard because table_schema is index table
  static int get_domain_id_col(ObDomainIDType type, const void *table_schema, ObIArray<uint64_t>& col_id, sql::ObSqlSchemaGuard *schema_guard = nullptr);
  // pq center ids need schema_guard because table_schema is index table
  static int get_domain_id_cols(ObDomainIDType type, const void *table_schema, ObIArray<uint64_t>& rowkey_cids, sql::ObSqlSchemaGuard *schema_guard = nullptr);
  /* ObLogTableScan End */

  /* tsc cg Begin */
  static int get_domain_id_col_by_tid(ObDomainIDType type, const void *table_schema, void* sche_gd, const uint64_t domain_tid, ObIArray<uint64_t> &col_ids);
  static bool is_domain_id_index_col_expr(const void *col_expr);
  static int64_t get_domain_type_by_col_expr(const void *col_expr, ObIndexType index_type);
  /* tsc cg End */

  /* dml cg Begin */
  static int check_has_domain_index(
      const void *table_schema,
      ObIArray<int64_t> &domain_types,
      ObIArray<uint64_t> &domain_tids);
  /* dml cg End */

  /* domain iter Begin */
  static int fill_domain_id_datum(ObDomainIDType type, void *expr, void *eval_ctx, const ObString &domain_id);
  static int fill_batch_domain_id_datum(ObDomainIDType type, void *expr, void *eval_ctx, const ObIArray<DomainIds>& domain_ids, const int64_t idx);
  /* domain iter End */

private:
  static int get_pq_cids_col_id(const ObTableSchema &index_table_schema, const ObTableSchema &data_table_schema, uint64_t &pq_cids_col_id);

public:
  constexpr const static char* const ObDomainIDStrArray[ObDomainIDType::MAX] = {
    "doc_id",
    "vid",
    "ivfflat_cid",
    "ivfsq_cid",
    "ivfpq_cid"
  };
};


}
}

#endif