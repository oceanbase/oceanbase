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

#define USING_LOG_PREFIX SHARE

#include "ob_domain_id.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/ob_sql_context.h"
#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace share
{

bool ObDomainIdUtils::is_domain_id_index_col(const void *col_schema)
{
  bool bret = false;
  const schema::ObColumnSchemaV2 *col = reinterpret_cast<const schema::ObColumnSchemaV2*>(col_schema);
  if (OB_NOT_NULL(col)) {
    bret = col->is_doc_id_column() ||
           col->is_vec_hnsw_vid_column() ||
           col->is_vec_ivf_center_id_column() ||
           col->is_vec_ivf_pq_center_ids_column() ||
           col->is_hybrid_embedded_vec_column();
  }
  return bret;
}

bool ObDomainIdUtils::check_table_need_column_ref_in_ddl(const void *table_schema, const ObColumnSchemaV2 *col_schema)
{
  bool bret = false;
  const schema::ObTableSchema *table = reinterpret_cast<const schema::ObTableSchema*>(table_schema);
  if (OB_NOT_NULL(table)) {
    bret = table->is_rowkey_doc_id() ||
           table->is_vec_rowkey_vid_type() ||
           table->is_vec_ivfflat_rowkey_cid_index() ||
          // TODO(mengyi): use the expression calculation currently.
          //              use the merge_iter after the split post build step.
          //  table->is_vec_ivfflat_centroid_index() ||
           table->is_vec_ivfflat_cid_vector_index() ||
           table->is_vec_ivfsq8_cid_vector_index() ||
           table->is_vec_ivfsq8_rowkey_cid_index() ||
           table->is_vec_ivfpq_code_index() ||
           table->is_vec_ivfpq_rowkey_cid_index() ||
           (col_schema->is_hybrid_embedded_vec_column() && table->is_hybrid_vec_index_embedded_type());
  }
  return bret;
}

bool ObDomainIdUtils::is_domain_id_index_table(const void *table_schema)
{
  bool bret = false;
  const schema::ObTableSchema *table = reinterpret_cast<const schema::ObTableSchema*>(table_schema);
  if (OB_NOT_NULL(table)) {
    bret = table->is_fts_index() ||
           table->is_multivalue_index() ||
           table->is_vec_index();
  }
  return bret;
}

int ObDomainIdUtils::check_table_need_domain_id_merge(ObDomainIDType type, const void *table_schema, bool &res)
{
  int ret = OB_SUCCESS;
  res = false;
  const schema::ObTableSchema *ddl_table_schema = reinterpret_cast<const schema::ObTableSchema*>(table_schema);
  if (OB_ISNULL(ddl_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema ptr", K(ret), K(type), KPC(ddl_table_schema));
  } else {
    switch (type) {
      case ObDomainIDType::DOC_ID: {
        uint64_t docid_col_id = OB_INVALID_ID;
        if (OB_FAIL(ddl_table_schema->get_docid_col_id(docid_col_id)) && OB_ERR_INDEX_KEY_NOT_FOUND != ret) {
          LOG_WARN("failed to determine docid type", K(ret), KPC(ddl_table_schema));
        } else if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
          // no such docid column, so no need to merge
          ret = OB_SUCCESS;
          res = false;
        } else if (ddl_table_schema->is_doc_id_rowkey() ||
            ddl_table_schema->is_fts_index_aux() ||
            ddl_table_schema->is_fts_doc_word_aux() ||
            ddl_table_schema->is_multivalue_index_aux() ||
            ddl_table_schema->is_vec_spiv_index_aux()) {
          res = true;
        }
        break;
      }
      case ObDomainIDType::VID: {
        uint64_t vid_col_id = OB_INVALID_ID;
        if (OB_FAIL(ddl_table_schema->get_vec_index_vid_col_id(vid_col_id)) && OB_ERR_INDEX_KEY_NOT_FOUND != ret) {
          LOG_WARN("failed to determine vid type", K(ret), KPC(ddl_table_schema));
        } else if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
          // no such vid column, so no need to merge
          ret = OB_SUCCESS;
          res = false;
        } else if (ddl_table_schema->is_vec_vid_rowkey_type() ||
            ddl_table_schema->is_vec_delta_buffer_type() ||
            ddl_table_schema->is_vec_index_id_type() ||
            ddl_table_schema->is_vec_index_snapshot_data_type() ||
            ddl_table_schema->is_hybrid_vec_index_embedded_type() ||
            ddl_table_schema->is_hybrid_vec_index_log_type()) {
          res = true;
        }
        break;
      }
      case ObDomainIDType::IVFFLAT_CID: {
        // TODO(@liyao): 使用merge_iter补cid_vector
        // if (ddl_table_schema->is_vec_ivfflat_cid_vector_index()) {
        //   res = true;
        // }
        break;
      }
      case ObDomainIDType::IVFSQ_CID: {
        // if (ddl_table_schema->is_vec_ivfsq8_cid_vector_index()) {
        //   res = true;
        // }
        break;
      }
      case ObDomainIDType::IVFPQ_CID: {
        // if (ddl_table_schema->is_vec_ivf_pq_code_index()) {
        //   res = true;
        // }
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        bool is_aux_table_has_hybrid_column;
        if (OB_FAIL(ObVectorIndexUtil::check_index_table_has_hybrid_vec_column(*ddl_table_schema, is_aux_table_has_hybrid_column))) {
          LOG_WARN("fail to check the index table has hybrid vec column", K(ret), K(type), KPC(ddl_table_schema));
        } else if (is_aux_table_has_hybrid_column &&
                   (ddl_table_schema->is_vec_index_id_type() || ddl_table_schema->is_vec_index_snapshot_data_type())) {
          res = true;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::check_column_need_domain_id_merge(
    const share::schema::ObTableSchema &table_schema,
    const ObDomainIDType type,
    const void *col_expr,
    const ObIndexType index_type,
    sql::ObSqlSchemaGuard &schema_guard,
    bool &res)
{
  int ret = OB_SUCCESS;
  res = false;
  const sql::ObColumnRefRawExpr *expr = reinterpret_cast<const sql::ObColumnRefRawExpr *>(col_expr);
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null column item ptr", K(ret), K(type), KPC(expr));
  } else {
    switch (type) {
      case ObDomainIDType::DOC_ID: {
        if (expr->is_doc_id_column()) {
          ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
          if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
            LOG_WARN("fail to get simple index infos", K(ret), K(table_schema));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && !res && i < simple_index_infos.count(); ++i) {
              const ObIndexType index_type = simple_index_infos.at(i).index_type_;
              const uint64_t index_tid = simple_index_infos.at(i).table_id_;
              if (schema::is_fts_or_multivalue_index(index_type) && !schema::is_rowkey_doc_aux(index_type)) {
                // has doc_id column on table with valid fulltext / multivalue index
                const share::schema::ObTableSchema *index_schema = nullptr;
                const share::schema::ObTableSchema *rowkey_doc_schema = nullptr;
                uint64_t rowkey_doc_tid = OB_INVALID_ID;
                if (OB_FAIL(schema_guard.get_table_schema(index_tid, index_schema))) {
                  LOG_WARN("failed to get index table schema", K(ret), K(index_tid));
                } else if (OB_ISNULL(index_schema)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected nullptr to index schema", K(ret));
                } else if (OB_UNLIKELY(index_schema->is_final_invalid_index())) {
                  // skip invalid index
                } else if (OB_FAIL(table_schema.get_rowkey_doc_tid(rowkey_doc_tid))) {
                  LOG_WARN("failed to get rowkey doc table id", K(ret));
                } else if (OB_FAIL(schema_guard.get_table_schema(rowkey_doc_tid, rowkey_doc_schema))) {
                  LOG_WARN("failed to get rowkey doc table schema", K(ret), K(rowkey_doc_tid));
                } else if (OB_ISNULL(rowkey_doc_schema)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected nullptr to rowkey doc schema", K(ret));
                } else if (OB_UNLIKELY(!rowkey_doc_schema->can_read_index() || !rowkey_doc_schema->is_index_visible())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected rowkey doc table unreadable", K(ret), KPC(rowkey_doc_schema));
                } else {
                  res = true;
                }
              }
            }
          }
        }
        break;
      }
      case ObDomainIDType::VID: {
        res = expr->is_vec_hnsw_vid_column();
        break;
      }
      case ObDomainIDType::IVFFLAT_CID: {
        res = expr->is_vec_cid_column() && index_type == ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL;
        break;
      }
      case ObDomainIDType::IVFPQ_CID: {
        res = expr->is_vec_cid_column() && index_type == ObIndexType::INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL;
        break;
      }
      case ObDomainIDType::IVFSQ_CID: {
        res = expr->is_vec_cid_column() && index_type == ObIndexType::INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL;
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        res = expr->is_hybrid_embedded_vec_column();
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::get_domain_tid_table_by_type(ObDomainIDType type,
                                                  const void *data_table_schema,
                                                  uint64_t &domain_id_table_id)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *data_table = reinterpret_cast<const schema::ObTableSchema*>(data_table_schema);
  if (OB_ISNULL(data_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema ptr", K(ret), K(type), KPC(data_table));
  } else {
    switch (type) {
      case ObDomainIDType::DOC_ID: {
        if (OB_FAIL(data_table->get_rowkey_doc_tid(domain_id_table_id))) {
          if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
            LOG_WARN("fail to get rowkey doc table id, retry", K(ret), KPC(data_table));
            ret = OB_SCHEMA_EAGAIN;
          } else {
            LOG_WARN("fail to get rowkey doc table id", K(ret), KPC(data_table));
          }
        }
        break;
      }
      case ObDomainIDType::VID: {
        if (OB_FAIL(data_table->get_rowkey_vid_tid(domain_id_table_id))) {
          LOG_WARN("fail to get rowkey vid table id", K(ret), KPC(data_table));
        }
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        if (OB_FAIL(data_table->get_embedded_vec_tid(domain_id_table_id))) {
          LOG_WARN("fail to get embedded vec table id", K(ret), KPC(data_table));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::get_domain_tid_table_by_cid(
    ObDomainIDType type,
    void *schema_guard,
    const void *data_table_schema,
    const uint64_t domain_col_id,
    uint64_t &tid)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *data_table = reinterpret_cast<const schema::ObTableSchema*>(data_table_schema);
  sql::ObSqlSchemaGuard *sql_schema_guard = reinterpret_cast<sql::ObSqlSchemaGuard*>(schema_guard);
  if (OB_ISNULL(data_table) || OB_ISNULL(sql_schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema ptr", K(ret), K(type), KP(data_table), KP(sql_schema_guard));
  } else {
    switch (type) {
      case ObDomainIDType::DOC_ID: {
        if (OB_FAIL(data_table->get_rowkey_doc_tid(tid))) {
          if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
            LOG_WARN("fail to get rowkey doc table id, retry", K(ret), KPC(data_table));
            ret = OB_SCHEMA_EAGAIN;
          } else {
            LOG_WARN("fail to get rowkey doc table id", K(ret), KPC(data_table));
          }
        }
        break;
      }
      case ObDomainIDType::VID: {
        if (OB_FAIL(ObVectorIndexUtil::check_rowkey_tid_table_readable(sql_schema_guard->get_schema_guard(), *data_table, tid))) {
          LOG_WARN("fail to get rowkey vid table id", K(ret), KPC(data_table));
        }
        break;
      }
      case ObDomainIDType::IVFFLAT_CID: {
        if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid_check_valid(
            sql_schema_guard, *data_table, INDEX_TYPE_VEC_IVFFLAT_ROWKEY_CID_LOCAL, domain_col_id, tid))) {
          LOG_WARN("failed to get rowkey cid table", K(ret), KPC(data_table));
        }
        break;
      }
      case ObDomainIDType::IVFSQ_CID: {
        if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid_check_valid(
            sql_schema_guard, *data_table, INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL, domain_col_id, tid))) {
          LOG_WARN("failed to get rowkey cid table", K(ret), KPC(data_table));
        }
        break;
      }
      case ObDomainIDType::IVFPQ_CID: {
        if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid_check_valid(
            sql_schema_guard, *data_table, INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL, domain_col_id, tid))) {
          LOG_WARN("failed to get rowkey cid table", K(ret), KPC(data_table));
        }
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        if (OB_FAIL(ObVectorIndexUtil::get_hybrid_embedded_vector_tid_check_valid(
            sql_schema_guard, *data_table, INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL, domain_col_id, tid))) {
          LOG_WARN("failed to get hybrid embedded table", K(ret), KPC(data_table));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}


int ObDomainIdUtils::get_domain_id_col(
    ObDomainIDType type,
    const void *table_schema,
    ObIArray<uint64_t>& col_id,
    sql::ObSqlSchemaGuard *schema_guard /*= nullptr*/)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *table = reinterpret_cast<const schema::ObTableSchema*>(table_schema);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema ptr", K(ret), K(type), KPC(table));
  } else {
    switch (type) {
      case ObDomainIDType::DOC_ID: {
        uint64_t doc_id_col_id = OB_INVALID_ID;
        uint64_t ft_col_id = OB_INVALID_ID;
        if (OB_FAIL(table->get_fulltext_column_ids(doc_id_col_id, ft_col_id))) {
          LOG_WARN("fail to get fulltext column ids", K(ret), KPC(table));
        } else if (OB_FAIL(col_id.push_back(doc_id_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::VID: {
        uint64_t vec_vid_col_id = OB_INVALID_ID;
        if (OB_FAIL(table->get_vec_index_vid_col_id(vec_vid_col_id))) {
          LOG_WARN("fail to get vec index column ids", K(ret), KPC(table));
        } else if (OB_FAIL(col_id.push_back(vec_vid_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::IVFFLAT_CID:
      case ObDomainIDType::IVFSQ_CID: {
        uint64_t vec_cid_col_id = OB_INVALID_ID;
        if (OB_FAIL(table->get_vec_index_vid_col_id(vec_cid_col_id, true/*is_cid*/))) { // table schema must be index table here
          LOG_WARN("fail to get vec index column ids", K(ret), KPC(table));
        } else if (OB_FAIL(col_id.push_back(vec_cid_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::IVFPQ_CID: {
        // cid_col_id + ivf_pq_center_ids_col_id
        // need data_table_schema to get vec_ivf_pq_center_ids_column
        uint64_t vec_cid_col_id = OB_INVALID_ID;
        uint64_t pq_cids_col_id = OB_INVALID_ID;
        const ObTableSchema *data_table_schema = nullptr;
        if (OB_ISNULL(schema_guard)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("pq cids need schema gaurd to fetch table schema", K(ret));
        } else if (OB_FAIL(table->get_vec_index_vid_col_id(vec_cid_col_id, true/*is_cid*/))) { // table schema must be index table here
          LOG_WARN("fail to get vec index column ids", K(ret), KPC(table));
        } else if (OB_FAIL(schema_guard->get_table_schema(table->get_data_table_id(), data_table_schema))) {
          LOG_WARN("get table schema failed", K(ret), K(table->get_data_table_id()));
        } else if (OB_ISNULL(data_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", K(ret), K(table->get_data_table_id()));
        } else if (OB_FAIL(get_pq_cids_col_id(*table, *data_table_schema, pq_cids_col_id))) {
          LOG_WARN("fail to get pq cids col id", K(ret));
        } else if (OB_FAIL(col_id.push_back(vec_cid_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        } else if (OB_FAIL(col_id.push_back(pq_cids_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        uint64_t vec_cid_col_id = OB_INVALID_ID;
        if (OB_FAIL(table->get_hybrid_vec_embedded_column_id(vec_cid_col_id))) { // table schema must be index table here
          LOG_WARN("fail to get vec index column ids", K(ret), KPC(table));
        } else if (OB_FAIL(col_id.push_back(vec_cid_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::get_domain_id_cols(
    ObDomainIDType type,
    const void *table_schema,
    ObIArray<uint64_t>& rowkey_cids,
    sql::ObSqlSchemaGuard *schema_guard /*= nullptr*/)
{
  int ret = OB_SUCCESS;
  DomainIdxs domain_id_cids;
  // uint64_t domain_id_cid = OB_INVALID_ID;
  const schema::ObTableSchema *table = reinterpret_cast<const schema::ObTableSchema*>(table_schema);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema ptr", K(ret), K(type), KPC(table));
  } else if (OB_FAIL(ObDomainIdUtils::get_domain_id_col(type, table_schema, domain_id_cids, schema_guard))) {
    LOG_WARN("fail to get domain id cid", K(ret), KPC(table), K(domain_id_cids));
  } else if (OB_FAIL(append(rowkey_cids, domain_id_cids))) {
    LOG_WARN("fail to push back domain id col", K(ret));
  }
  return ret;
}

bool ObDomainIdUtils::is_domain_id_index_col_expr(const void *col_expr)
{
  bool bret = false;
  const sql::ObColumnRefRawExpr *expr = reinterpret_cast<const sql::ObColumnRefRawExpr *>(col_expr);
  if (OB_NOT_NULL(expr)) {
    bret = expr->is_doc_id_column() ||
           expr->is_vec_hnsw_vid_column() ||
           expr->is_vec_cid_column() ||
           expr->is_vec_pq_cids_column() ||
           expr->is_hybrid_embedded_vec_column();
  }
  return bret;
}

int64_t ObDomainIdUtils::get_domain_type_by_col_expr(const void *col_expr, ObIndexType index_type)
{
  int64_t type = ObDomainIDType::MAX;
  const sql::ObColumnRefRawExpr *expr = reinterpret_cast<const sql::ObColumnRefRawExpr *>(col_expr);
  if (expr->is_doc_id_column()) {
    type = ObDomainIDType::DOC_ID;
  } else if (expr->is_vec_hnsw_vid_column()) {
    type = ObDomainIDType::VID;
  } else if (expr->is_vec_cid_column()) {
    if (index_type == ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL) {
      type = ObDomainIDType::IVFFLAT_CID;
    } else if (index_type == ObIndexType::INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL) {
      type = ObDomainIDType::IVFPQ_CID;
    } else if (index_type == ObIndexType::INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL) {
      type = ObDomainIDType::IVFSQ_CID;
    }
  } else if (expr->is_vec_pq_cids_column()) {
    type = ObDomainIDType::IVFPQ_CID;
  } else if (expr->is_hybrid_embedded_vec_column()) {
    type = ObDomainIDType::EMB_VEC;
  }
  return type;
}

int ObDomainIdUtils::check_has_domain_index(const void *table_schema, ObIArray<int64_t> &domain_types, ObIArray<uint64_t> &domain_tids)
{
  int ret = OB_SUCCESS;
  ret = false;
  const schema::ObTableSchema *table = reinterpret_cast<const schema::ObTableSchema*>(table_schema);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema ptr", K(ret), KPC(table));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const ObSimpleTableSchemaV2 *index_schema = NULL;
    const uint64_t tenant_id = table->get_tenant_id();
    if (OB_FAIL(table->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      int64_t domain_type = ObDomainIDType::MAX;
      uint64_t domain_tid = simple_index_infos.at(i).table_id_;
      if (is_rowkey_doc_aux(simple_index_infos.at(i).index_type_)) {
        for (int64_t j = 0; OB_SUCC(ret) && j < simple_index_infos.count(); ++j) {
          const ObIndexType index_type = simple_index_infos.at(j).index_type_;
          if (schema::is_fts_or_multivalue_index(index_type) && !schema::is_rowkey_doc_aux(index_type)) {
            domain_type = ObDomainIDType::DOC_ID; // only one
          }
        }
      } else if (is_vec_rowkey_vid_type(simple_index_infos.at(i).index_type_)) {
        domain_type = ObDomainIDType::VID; // only one
      } else if (is_vec_ivfflat_rowkey_cid_index(simple_index_infos.at(i).index_type_)) {
        domain_type = ObDomainIDType::IVFFLAT_CID; // not only one
      } else if (is_vec_ivfsq8_rowkey_cid_index(simple_index_infos.at(i).index_type_)) {
        domain_type = ObDomainIDType::IVFSQ_CID; // not only one
      } else if (is_vec_ivfpq_rowkey_cid_index(simple_index_infos.at(i).index_type_)) {
        domain_type = ObDomainIDType::IVFPQ_CID; // not only one
      } else if (is_hybrid_vec_index_embedded_type(simple_index_infos.at(i).index_type_)) {
        domain_type = ObDomainIDType::EMB_VEC; // not only one
      }
      if (OB_SUCC(ret) && (ObDomainIDType::DOC_ID <= domain_type && domain_type < ObDomainIDType::MAX)) {
        if (OB_FAIL(domain_types.push_back(domain_type))) {
          LOG_WARN("failed to push back domain type", K(ret), K(domain_type));
        } else if (OB_FAIL(domain_tids.push_back(domain_tid))) {
        LOG_WARN("failed to push back domain tid", K(ret), K(domain_tid));
        }
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::get_domain_id_col_by_tid(
    ObDomainIDType type,
    const void *table_schema,
    void* sche_gd,
    const uint64_t domain_tid,
    ObIArray<uint64_t> &col_ids)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *table = reinterpret_cast<const schema::ObTableSchema*>(table_schema);
  sql::ObSqlSchemaGuard *sql_schema_guard = reinterpret_cast<sql::ObSqlSchemaGuard*>(sche_gd);
  if (OB_ISNULL(table) || OB_ISNULL(sql_schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema ptr", K(ret), K(type), KP(table), KP(sql_schema_guard));
  } else {
    switch (type) {
      case ObDomainIDType::DOC_ID: {
        uint64_t doc_id_col_id = OB_INVALID_ID;
        uint64_t ft_col_id = OB_INVALID_ID;
        if (OB_FAIL(table->get_fulltext_column_ids(doc_id_col_id, ft_col_id))) {
          LOG_WARN("fail to get fulltext column ids", K(ret), KPC(table));
        } else if (OB_FAIL(col_ids.push_back(doc_id_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::VID: {
        uint64_t vec_vid_col_id = OB_INVALID_ID;
        if (OB_FAIL(table->get_vec_index_vid_col_id(vec_vid_col_id))) {
          LOG_WARN("fail to get vec index column ids", K(ret), KPC(table));
        } else if (OB_FAIL(col_ids.push_back(vec_vid_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::IVFFLAT_CID:
      case ObDomainIDType::IVFSQ_CID: {
        uint64_t vec_cid_col_id = OB_INVALID_ID;
        const ObTableSchema *rowkey_cid_schema = nullptr;
        if (OB_FAIL(sql_schema_guard->get_table_schema(domain_tid, rowkey_cid_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(domain_tid));
        } else if (OB_ISNULL(rowkey_cid_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr to table schema", K(ret));
        } else if (OB_FAIL(rowkey_cid_schema->get_vec_index_vid_col_id(vec_cid_col_id, true/*is_cid*/))) {
          LOG_WARN("fail to get domain column id", K(ret));
        } else if (OB_FAIL(col_ids.push_back(vec_cid_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::IVFPQ_CID: {
        // cid_col_id + ivf_pq_center_ids_col_id
        // need data_table_schema to get vec_ivf_pq_center_ids_column
        uint64_t vec_cid_col_id = OB_INVALID_ID;
        uint64_t pq_cids_col_id = OB_INVALID_ID;
        const ObTableSchema *rowkey_cid_schema = nullptr;
        if (OB_FAIL(sql_schema_guard->get_table_schema(domain_tid, rowkey_cid_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(domain_tid));
        } else if (OB_ISNULL(rowkey_cid_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr to table schema", K(ret));
        } else if (OB_FAIL(rowkey_cid_schema->get_vec_index_vid_col_id(vec_cid_col_id, true/*is_cid*/))) {
          LOG_WARN("fail to get fulltext column ids", K(ret));
        } else if (OB_FAIL(get_pq_cids_col_id(*rowkey_cid_schema, *table, pq_cids_col_id))) {
          LOG_WARN("fail to get pq cids col id", K(ret));
        } else if (OB_FAIL(col_ids.push_back(vec_cid_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        } else if (OB_FAIL(col_ids.push_back(pq_cids_col_id))) {
          LOG_WARN("fail to push back col id", K(ret));
        }
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        uint64_t embedded_col_id = OB_INVALID_ID;
        const ObTableSchema *embedded_table_schema = nullptr;
        if (OB_FAIL(sql_schema_guard->get_table_schema(domain_tid, embedded_table_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(domain_tid));
        } else if (OB_ISNULL(embedded_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr to table schema", K(ret));
        } else if (OB_FAIL(embedded_table_schema->get_hybrid_vec_embedded_column_id(embedded_col_id))) {
          LOG_WARN("fail to get embedded vector index column ids", K(ret), KPC(embedded_table_schema));
        } else if (OB_FAIL(col_ids.push_back(embedded_col_id))) {
          LOG_WARN("fail to push back col id", K(ret), K(embedded_col_id));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::fill_domain_id_datum(ObDomainIDType type, void *expr, void *eval_ctx, const ObString &domain_id)
{
  int ret = OB_SUCCESS;
  sql::ObExpr *domain_id_expr = reinterpret_cast<sql::ObExpr*>(expr);
  sql::ObEvalCtx *ctx = reinterpret_cast<sql::ObEvalCtx*>(eval_ctx);
  bool set_null = (domain_id.length() == 0);
  if (OB_ISNULL(domain_id_expr) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ptr", K(ret), K(type), KPC(domain_id_expr), KPC(ctx));
  } else {
    ObDatum &dst_datum = domain_id_expr->locate_datum_for_write(*ctx);
    switch (type) {
      case ObDomainIDType::DOC_ID:
      case ObDomainIDType::IVFFLAT_CID:
      case ObDomainIDType::IVFSQ_CID:
      case ObDomainIDType::IVFPQ_CID: {
        if (set_null) {
          dst_datum.set_null();
        } else {
          char *buf = nullptr;
          if (OB_ISNULL(buf = static_cast<char *>(domain_id_expr->get_str_res_mem(*ctx, domain_id.length())))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret), KP(buf), K(domain_id.length()));
          } else {
            memcpy(buf, domain_id.ptr(), domain_id.length());
            dst_datum.set_string(reinterpret_cast<char*>(buf), domain_id.length());
          }
        }
        break;
      }
      case ObDomainIDType::VID: {
        if (domain_id.length() != sizeof(int64_t) && domain_id.length() != 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid domain id", K(ret), K(domain_id.length()));
        } else {
          if (set_null) {
            dst_datum.set_null();
          } else {
            int64_t vid = *reinterpret_cast<const int64_t*>(domain_id.ptr());
            dst_datum.set_int(vid);
          }
        }
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        if (set_null) {
          dst_datum.set_null();
        } else {
          // ? is set a id or a full vec str
          char *buf = nullptr;
          if (OB_ISNULL(buf = static_cast<char *>(domain_id_expr->get_str_res_mem(*ctx, domain_id.length())))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret), KP(buf), K(domain_id.length()));
          } else {
            memcpy(buf, domain_id.ptr(), domain_id.length());
            dst_datum.set_string(reinterpret_cast<char*>(buf), domain_id.length());
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::fill_batch_domain_id_datum(ObDomainIDType type, void *expr, void *eval_ctx, const ObIArray<DomainIds>& domain_ids, const int64_t idx)
{
  int ret = OB_SUCCESS;
  sql::ObExpr *domain_id_expr = reinterpret_cast<sql::ObExpr*>(expr);
  sql::ObEvalCtx *ctx = reinterpret_cast<sql::ObEvalCtx*>(eval_ctx);
  // bool set_null = (domain_id.length() == 0);
  ObDatum *datums = nullptr;
  if (OB_ISNULL(domain_id_expr) || OB_ISNULL(ctx) || domain_ids.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ptr", K(ret), K(type), K(domain_ids), KPC(domain_id_expr), KPC(ctx));
  } else if (OB_ISNULL(datums = domain_id_expr->locate_datums_for_update(*ctx, domain_ids.count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(domain_id_expr));
  } else {
    switch (type) {
      case ObDomainIDType::DOC_ID:
      case ObDomainIDType::IVFFLAT_CID:
      case ObDomainIDType::IVFSQ_CID:
      case ObDomainIDType::IVFPQ_CID: {
        char *buf = nullptr;
        uint64_t buf_pos = 0;
        uint64_t total_len = 0;
        if (ObDomainIDType::IVFPQ_CID != type && idx != 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected doc id idx", K(ret), K(idx), K(type));
        }
        // calc total buf len
        for (int64_t i = 0; OB_SUCC(ret) && i < domain_ids.count(); i++) {
          if (idx < 0 || idx > domain_ids.at(i).count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected idx", K(ret), K(idx), K(domain_ids.at(i).count()));
          } else {
            total_len += domain_ids.at(i).at(idx).length();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (total_len != 0 && OB_ISNULL(buf = static_cast<char *>(domain_id_expr->get_str_res_mem(*ctx, total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret), KP(buf), K(total_len));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < domain_ids.count(); ++i) {
            if (domain_ids.at(i).at(idx).length() == 0) {
              datums[i].set_null();
            } else {
              memcpy(buf + buf_pos, domain_ids.at(i).at(idx).ptr(), domain_ids.at(i).at(idx).length());
              datums[i].set_string(buf + buf_pos, domain_ids.at(i).at(idx).length());
              buf_pos += domain_ids.at(i).at(idx).length();
            }
            LOG_TRACE("Domain id merge fill a domain id", KP(buf + buf_pos), K(domain_ids.at(i).at(idx).length()), K(i));
          }
        }
        break;
      }
      case ObDomainIDType::VID: {
        if (idx != 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vec id idx", K(ret), K(idx));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < domain_ids.count(); i++) {
          if (idx < 0 || idx > domain_ids.at(i).count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected idx", K(ret), K(idx), K(domain_ids.at(i).count()));
          } else if (domain_ids.at(i).at(idx).length() == 0) {
            datums[i].set_null();
          } else if (domain_ids.at(i).at(idx).length() != sizeof(int64_t)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid domain id", K(ret), K(domain_ids.at(i).at(idx).length()));
          } else {
            int64_t vid = *reinterpret_cast<const int64_t*>(domain_ids.at(i).at(idx).ptr());
            datums[i].set_int(vid);
          }
        }
        break;
      }
      case ObDomainIDType::EMB_VEC: {
        char *buf = nullptr;
        uint64_t buf_pos = 0;
        uint64_t total_len = 0;
        // calc total buf len
        for (int64_t i = 0; OB_SUCC(ret) && i < domain_ids.count(); i++) {
          if (idx < 0 || idx > domain_ids.at(i).count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected idx", K(ret), K(idx), K(domain_ids.at(i).count()));
          } else {
            total_len += domain_ids.at(i).at(idx).length();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (total_len != 0 && OB_ISNULL(buf = static_cast<char *>(domain_id_expr->get_str_res_mem(*ctx, total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret), KP(buf), K(total_len));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < domain_ids.count(); ++i) {
            if (domain_ids.at(i).at(idx).length() == 0) {
              datums[i].set_null();
            } else {
              memcpy(buf + buf_pos, domain_ids.at(i).at(idx).ptr(), domain_ids.at(i).at(idx).length());
              datums[i].set_string(buf + buf_pos, domain_ids.at(i).at(idx).length());
              buf_pos += domain_ids.at(i).at(idx).length();
            }
            LOG_TRACE("Domain id merge fill a domain id", KP(buf + buf_pos), K(domain_ids.at(i).at(idx).length()), K(i));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObDomainIdUtils::get_pq_cids_col_id(
    const ObTableSchema &index_table_schema,
    const ObTableSchema &data_table_schema,
    uint64_t &pq_cids_col_id)
{
  int ret = OB_SUCCESS;
  pq_cids_col_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *col_schema = nullptr;
  for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema.get_column_count() && pq_cids_col_id == OB_INVALID_ID; j++) {
    if (OB_ISNULL(col_schema = index_table_schema.get_column_schema_by_idx(j))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), K(index_table_schema));
    } else {
      const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
      if (OB_ISNULL(ori_col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table column", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
      } else if (ori_col_schema->is_vec_ivf_pq_center_ids_column()) {
        pq_cids_col_id = ori_col_schema->get_column_id();
      }
    }
  }

  if (OB_SUCC(ret) && pq_cids_col_id == OB_INVALID_ID) {
    ret = OB_NOT_EXIST_COLUMN_ID;
    LOG_WARN("pq cids col id not exist", K(ret));
  }

  return ret;
}

int ObDomainIdUtils::resort_domain_info_by_base_cols(
    sql::ObSqlSchemaGuard &sql_schema_guard,
    const ObTableSchema &table_schema,
    const ObIArray<uint64_t> &base_col_ids,
    ObIArray<int64_t> &domain_types,
    ObIArray<uint64_t> &domain_tids) {
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 16> tmp_domain_types;
  ObSEArray<uint64_t, 16> tmp_domain_tids;
  if (OB_FAIL(tmp_domain_types.prepare_allocate(base_col_ids.count()))) {
    LOG_WARN("fail to reserve space", K(ret), K(base_col_ids.count()));
  } else if (OB_FAIL(tmp_domain_tids.prepare_allocate(base_col_ids.count()))) {
    LOG_WARN("fail to reserve space", K(ret), K(base_col_ids.count()));
  } else {
    for (int i = 0; i < base_col_ids.count(); ++i) {
      tmp_domain_types[i] = -1;
      tmp_domain_tids[i] = 0;
    }
  }

  DomainIdxs col_ids;
  int64_t idx = OB_INVALID_INDEX;
  int64_t sort_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < domain_types.count(); i++) {
    col_ids.reuse();
    ObDomainIdUtils::ObDomainIDType type = static_cast<ObDomainIdUtils::ObDomainIDType>(domain_types.at(i));
    if (OB_FAIL(ObDomainIdUtils::get_domain_id_col_by_tid(type, &table_schema, &sql_schema_guard, domain_tids.at(i), col_ids))) {
      LOG_WARN("fail to get domain id col id", K(ret), K(type), K(table_schema));
    } else if (is_contain(col_ids, OB_INVALID_ID) || col_ids.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid domain id col id", K(ret), K(type), K(table_schema));
    } else if (has_exist_in_array(base_col_ids, col_ids.at(0), &idx)) {
      tmp_domain_types[idx] = domain_types.at(i);
      tmp_domain_tids[idx] = domain_tids.at(i);
      ++sort_cnt;
    }
  }

  if (OB_SUCC(ret)) {
    if (sort_cnt != domain_types.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count of domain_types should be equal to sort_cnt", K(ret), K(sort_cnt), K(domain_types.count()));
    }

    int real_pos = 0;
    for (int i = 0; OB_SUCC(ret) && i < base_col_ids.count(); ++i) {
      if (tmp_domain_types[i] != -1) {
        if (real_pos >= domain_types.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid pos of domain_types", K(ret), K(i), K(real_pos), K(domain_types.count()));
        } else {
          domain_types.at(real_pos) = tmp_domain_types[i];
          domain_tids.at(real_pos) = tmp_domain_tids[i];
          ++real_pos;
        }
      }
    }
  }

  return ret;
}

}
}
