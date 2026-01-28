//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/table/ob_ttl_util.h"

namespace oceanbase
{
using namespace common;
namespace share
{
const uint64_t ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION;

bool ObCompactionTTLUtil::is_enable_compaction_ttl(uint64_t tenant_id)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  return tenant_config.is_valid() &&
         tenant_config->enable_ttl;
}

// check exist vec index
int ObCompactionTTLUtil::is_ttl_schema(
  const ObTableSchema &table_schema,
  bool &is_ttl_schema)
{
  int ret = OB_SUCCESS;
  is_ttl_schema = true;
  bool is_compaction_ttl = false;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos = table_schema.get_simple_index_infos();
  for (int64_t i = 0; i < simple_index_infos.count(); ++i) {
    const ObAuxTableMetaInfo &simple_index_info = simple_index_infos.at(i);
    if (is_vec_index_for_ttl(simple_index_info.index_type_)) {
      is_ttl_schema = false;
      break;
    }
  }
  if (is_ttl_schema && table_schema.is_parent_table()) {
    is_ttl_schema = false;
    COMMON_LOG(INFO, "[COMPACTION TTL] has foreign key", K(ret), K(tenant_id), K(table_id),
      K(table_schema.get_foreign_key_infos()));
  }
  return ret;
}

int ObCompactionTTLUtil::is_compaction_ttl_schema(
  const uint64_t tenant_data_version,
  const ObTableSchema &table_schema,
  bool &is_compaction_ttl)
{
  int ret = OB_SUCCESS;
  is_compaction_ttl = false;

  ObSimpleTableTTLChecker ttl_checker;
  const bool valid_data_version = tenant_data_version >= COMPACTION_TTL_CMP_DATA_VERSION;
  const uint64_t table_id = table_schema.get_table_id();
  if (OB_FAIL(ttl_checker.init(table_schema, table_schema.get_ttl_definition(), true/*in_full_column_order*/))) {
    COMMON_LOG(WARN, "fail to init ttl checker", KR(ret), K(table_schema));
  } else if (1 == ttl_checker.get_ttl_definition().count()) { // only one ttl expr
    const ObTableTTLExpr &ttl_expr = ttl_checker.get_ttl_definition().at(0);
    if (!ObCompactionTTLUtil::is_compaction_ttl_merge_engine(table_schema.get_merge_engine_type())) {
      COMMON_LOG(INFO, "[COMPACTION TTL] not support merge engine", K(ret), K(table_id),
        "merge_engine_type", table_schema.get_merge_engine_type());
    } else if (!ObCompactionTTLUtil::is_rowscn_column(ttl_expr.column_name_)) {
      COMMON_LOG(INFO, "[COMPACTION TTL] not rowscn column", K(ret), K(table_id), "column_name", ttl_expr.column_name_);
    } else if (!valid_data_version) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "[COMPACTION TTL] not support ttl table use rowscn column in old data version", K(ret), K(table_id), K(valid_data_version));
    } else {
      // TODO check is generated column in next feature
      is_compaction_ttl = true;
      COMMON_LOG(INFO, "[COMPACTION TTL] use compaction TTL", K(ret), K(table_id));
    }
    if (OB_FAIL(ret) || is_compaction_ttl) {
    } else if (table_schema.is_append_only_merge_engine()) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "append_only merge engine can only have compaction ttl definition", K(ret),
        K(ttl_checker.get_ttl_definition()), K(table_schema));
    } else if (ObCompactionTTLUtil::is_rowscn_column(ttl_expr.column_name_)) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "only compaction-ttl table can use ora_rowscn column", K(ret),
        K(ttl_checker.get_ttl_definition()), K(table_schema));
    }
  }
  return ret;
}

int ObCompactionTTLUtil::check_exist_user_defined_rowscn_column(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator iter = table_schema.column_begin();
  ObColumnSchemaV2 *col = NULL;
  for ( ; OB_SUCC(ret) && iter != table_schema.column_end(); iter++) {
    if (OB_ISNULL(col = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "The column is NULL", K(col));
    } else if (is_rowscn_column(col->get_column_name_str())) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "exist user defined rowscn column", K(ret), K(table_schema));
    }
  }
  return ret;
}

int ObCompactionTTLUtil::check_ttl_column_valid(const ObTableSchema &table_schema,
                                                const ObString &ttl_definition,
                                                const ObTTLFlag &ttl_flag,
                                                const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;

  if (!ttl_definition.empty() && ObTTLDefinition::COMPACTION == ttl_flag.ttl_type_) {
    // 1. don't support sql mode ttl table in old data version
    if (tenant_data_version < COMPACTION_TTL_CMP_DATA_VERSION) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "sql mode ttl is not supported in old data version", K(ret), K(tenant_data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sql mode ttl under this version is");
    } else {
      ObSimpleTableTTLChecker ttl_checker;
      if (OB_FAIL(ttl_checker.init(ttl_definition))) {
        COMMON_LOG(WARN, "Fail to parse ttl_definition");
      } else {
        const common::ObIArray<ObTableTTLExpr> &ttl_exprs = ttl_checker.get_ttl_definition();
        int64_t unused = 0;

        if (ttl_exprs.count() != 1) {
          // 2. don't support multi ttl columns in sql mode
          ret = OB_NOT_SUPPORTED;
          COMMON_LOG(WARN, "multi ttl columns count is not supported", K(ret), K(ttl_definition));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "multi ttl columns count in sql mode is");
        } else if (!is_rowscn_column(ttl_exprs.at(0).column_name_)) {
          // 3. now, only support ora_rowscn column in sql mode
          ret = OB_NOT_SUPPORTED;
          COMMON_LOG(WARN, "now, only support ora_rowscn column in sql mode", K(ret), K(table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-ora_rowscn column is not supported as ttl column in sql mode");
        } else if (!ObCompactionTTLUtil::is_compaction_ttl_merge_engine(table_schema.get_merge_engine_type())) {
          // 4. only support delete_insert and append_only merge engine
          ret = OB_NOT_SUPPORTED;
          COMMON_LOG(WARN, "compaction ttl only support delete_insert and append_only merge engine", K(ret), K(table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "compaction ttl for partial update merge engine is");
        } else if (OB_FAIL(check_exist_user_defined_rowscn_column(table_schema))) {
          COMMON_LOG(WARN, "table have user defined rowscn column, can't be compaction ttl table", K(ret), K(table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "table with user defined rowscn column is not supported as compaction ttl table");
        } else if (OB_FAIL(ttl_checker.get_ttl_filter_us(unused))) {
          COMMON_LOG(WARN, "fail to get ttl filter us", KR(ret), K(ttl_definition));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "negative or very large ttl time is");
        } else {
          COMMON_LOG(INFO, "ttl column is valid", K(ret), K(ttl_definition), K(unused));
        }
      }
    }
  }

  return ret;
}

void ObTTLDefinition::gene_info(
  char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0 || pos >= buf_len) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TTL = %.*s BY %s ",
    ttl_definition_.length(), ttl_definition_.ptr(), ttl_type_to_string(ttl_type_)))) {
    COMMON_LOG(WARN, "fail to print ttl definition", K(ret), K(ttl_definition_), K(ttl_type_to_string(ttl_type_)));
  }
}

} // namespace share
} // namespace oceanbase
