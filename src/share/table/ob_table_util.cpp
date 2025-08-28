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

#define USING_LOG_PREFIX SERVER
#include "ob_table_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace table
{
  const ObString ObTableUtils::KV_NORMAL_TRACE_INFO = ObString::make_string("OBKV Operation");
  const ObString ObTableUtils::KV_TTL_TRACE_INFO = ObString::make_string("TTL Delete");
  bool ObTableUtils::is_kv_trace_info(const ObString &trace_info)
  {
    return (trace_info.compare(KV_NORMAL_TRACE_INFO) == 0 || trace_info.compare(KV_TTL_TRACE_INFO) == 0);
  }

  bool ObTableUtils::has_exist_in_columns(const ObIArray<ObString> &columns, const ObString &name)
  {
    bool exist = false;
    int64_t num = columns.count();
    for (int64_t i = 0; i < num && !exist; i++) {
      if (0 == name.case_compare(columns.at(i))) {
        exist = true;
      }
    }
    return exist;
  }

  int ObTableUtils::get_part_idx_by_tablet_id(ObSchemaGetterGuard &schema_guard,
                                              uint64_t arg_table_id, ObTabletID arg_tablet_id,
                                              int64_t &part_idx, int64_t &subpart_idx)
  {
    int ret = OB_SUCCESS;
    const ObSimpleTableSchemaV2 *table_schema = NULL;
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, arg_table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(arg_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
    } else if (OB_FAIL(get_part_idx_by_tablet_id(*table_schema, arg_table_id, arg_tablet_id, part_idx, subpart_idx))) {
      LOG_WARN("fail to get part idx by tablet id", K(ret), K(arg_table_id), K(arg_tablet_id));
    } else {}
    return ret;
  }

  int ObTableUtils::get_part_idx_by_tablet_id(const ObSimpleTableSchemaV2 &table_schema,
                                              uint64_t arg_table_id, ObTabletID arg_tablet_id,
                                              int64_t &part_idx, int64_t &subpart_idx)
  {
    int ret = OB_SUCCESS;
    if (!table_schema.is_partitioned_table()) {
      // do nothing
    } else if (OB_FAIL(table_schema.get_part_idx_by_tablet(arg_tablet_id, part_idx, subpart_idx))) {
      LOG_WARN("fail to get part idx by tablet", K(ret));
    }
    return ret;
  }

  int ObTableUtils::get_tablet_id_by_part_idx(ObSchemaGetterGuard &schema_guard,
                                              const uint64_t table_id,
                                              const int64_t part_idx,
                                              const int64_t subpart_idx,
                                              ObTabletID &tablet_id)
  {
    int ret = OB_SUCCESS;
    const uint64_t tenant_id = MTL_ID();
    const ObSimpleTableSchemaV2 *table_schema = NULL;
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(get_tablet_id_by_part_idx(*table_schema, part_idx, subpart_idx, tablet_id))) {
      LOG_WARN("fail to get tablet id by part idx", K(ret), K(table_id), K(part_idx), K(subpart_idx));
    }
    return ret;
  }

  int ObTableUtils::get_tablet_id_by_part_idx(const ObSimpleTableSchemaV2 &table_schema,
                                              const int64_t part_idx,
                                              const int64_t subpart_idx,
                                              ObTabletID &tablet_id)
  {
    int ret = OB_SUCCESS;
    ObObjectID tmp_object_id = OB_INVALID_ID;
    ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
    const uint64_t tenant_id = MTL_ID();
    if (!table_schema.is_partitioned_table()) {
      tablet_id = table_schema.get_tablet_id();
    } else if (OB_FAIL(table_schema.get_part_id_and_tablet_id_by_idx(part_idx,
                                                                     subpart_idx,
                                                                     tmp_object_id,
                                                                     tmp_first_level_part_id,
                                                                     tablet_id))) {
      LOG_WARN("fail to get tablet by idx", K(ret));
    }
    return ret;
  }
}
}




