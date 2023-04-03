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

#include "share/tablet/ob_tablet_to_table_history_operator.h"
#include "share/ob_errno.h"                          // KR(ret)
#include "share/inner_table/ob_inner_table_schema.h" // OB_ALL_TABLET_TO_TABLE_HISTORY_TNAME
#include "share/schema/ob_schema_utils.h"            // ObSchemaUtils
#include "share/ob_dml_sql_splicer.h"                // ObDMLSqlSplicer
#include "lib/string/ob_sql_string.h"                // ObSqlString
#include "lib/mysqlclient/ob_mysql_proxy.h"          // ObISQLClient
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace share
{
using namespace schema;
int ObTabletToTableHistoryOperator::create_tablet_to_table_history(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t schema_version,
    const common::ObIArray<ObTabletTablePair> &pairs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || pairs.count() <= 0
      || schema_version <= 0
      || !ObSchemaService::is_formal_version(schema_version))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(schema_version), "pairs_cnt", pairs.count());
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    const int64_t BATCH_NUM = 10000;
    int64_t start_idx = 0;
    int64_t end_idx = min(pairs.count(), start_idx + BATCH_NUM);
    const int64_t is_deleted = 0;
    while (OB_SUCC(ret)
           && end_idx - start_idx > 0
           && end_idx <= pairs.count()) {
      sql.reset();
      dml_splicer.reset();
      for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; i++) {
        const ObTabletTablePair &pair = pairs.at(i);
        if (OB_UNLIKELY(!pair.is_valid()
            || !pair.get_tablet_id().is_valid_with_tenant(tenant_id))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tablet-table pair", KR(ret), K(tenant_id), K(pair));
        } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id",
                           ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))
                   || OB_FAIL(dml_splicer.add_pk_column("tablet_id", pair.get_tablet_id().id()))
                   || OB_FAIL(dml_splicer.add_pk_column("schema_version", schema_version))
                   || OB_FAIL(dml_splicer.add_column("table_id",
                              ObSchemaUtils::get_extract_schema_id(tenant_id, pair.get_table_id())))
                   || OB_FAIL(dml_splicer.add_column("is_deleted", is_deleted))
                   ) {
          LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(schema_version), K(pair));
        } else if (OB_FAIL(dml_splicer.finish_row())) {
          LOG_WARN("fail to finish row", KR(ret), K(tenant_id));
        }
      } // end for
      if (FAILEDx(dml_splicer.splice_batch_insert_sql(OB_ALL_TABLET_TO_TABLE_HISTORY_TNAME, sql))) {
        LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else if (affected_rows != (end_idx - start_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows not match", KR(ret), K(tenant_id),
                 K(affected_rows), "pairs_cnt", start_idx - end_idx);
      }
      start_idx = end_idx;
      end_idx = min(pairs.count(), start_idx + BATCH_NUM);
    } //end while
  }
  return ret;
}

int ObTabletToTableHistoryOperator::drop_tablet_to_table_history(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t schema_version,
    const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || tablet_ids.count() <= 0
      || schema_version <= 0
      || !ObSchemaService::is_formal_version(schema_version))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id),
             K(schema_version), "tablet_cnt", tablet_ids.count());
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    const int64_t BATCH_NUM = 10000;
    int64_t start_idx = 0;
    int64_t end_idx = min(tablet_ids.count(), start_idx + BATCH_NUM);
    const int64_t is_deleted = 1;
    const uint64_t table_id = OB_INVALID_ID; // means been dropped
    while (OB_SUCC(ret)
           && end_idx - start_idx > 0
           && end_idx <= tablet_ids.count()) {
      sql.reset();
      dml_splicer.reset();
      for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; i++) {
        const ObTabletID &tablet_id = tablet_ids.at(i);
        if (OB_UNLIKELY(!tablet_id.is_valid()
            || !tablet_id.is_valid_with_tenant(tenant_id))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tablet_id", KR(ret), K(tenant_id), K(tablet_id));
        } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id",
                           ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))
                   || OB_FAIL(dml_splicer.add_pk_column("tablet_id", tablet_id.id()))
                   || OB_FAIL(dml_splicer.add_pk_column("schema_version", schema_version))
                   || OB_FAIL(dml_splicer.add_column("table_id",
                              ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))
                   || OB_FAIL(dml_splicer.add_column("is_deleted", 1))
                   ) {
          LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(schema_version), K(tablet_id));
        } else if (OB_FAIL(dml_splicer.finish_row())) {
          LOG_WARN("fail to finish row", KR(ret), K(tenant_id));
        }
      } // end for
      if (FAILEDx(dml_splicer.splice_batch_insert_sql(OB_ALL_TABLET_TO_TABLE_HISTORY_TNAME, sql))) {
        LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else if (affected_rows != (end_idx - start_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows not match", KR(ret), K(tenant_id),
                 K(affected_rows), "tablet_cnt", start_idx - end_idx);
      }
      start_idx = end_idx;
      end_idx = min(tablet_ids.count(), start_idx + BATCH_NUM);
    } //end while
  }
  return ret;
}

} // end share
} // end oceanbase
