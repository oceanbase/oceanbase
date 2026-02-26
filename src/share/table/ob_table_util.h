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

#ifndef OCEANBASE_SHARE_TABLE_OB_TABLE_UTIL_
#define OCEANBASE_SHARE_TABLE_OB_TABLE_UTIL_

#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "common/ob_tablet_id.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

class ObTableUtils
{
public:
  static const ObString &get_kv_normal_trace_info() { return KV_NORMAL_TRACE_INFO; }
  static const ObString &get_kv_ttl_trace_info() { return KV_TTL_TRACE_INFO; }
  static bool is_kv_trace_info(const ObString &trace_info);
  static bool has_exist_in_columns(const ObIArray<ObString> &columns, const ObString &name);
  static int extract_tenant_id(const obrpc::ObRpcPacket &pkt, uint64_t &tenant_id);
public:
  static int get_tablet_id_by_part_idx(share::schema::ObSchemaGetterGuard &schema_guard,
                                       const uint64_t table_id,
                                       const int64_t part_idx,
                                       const int64_t subpart_idx,
                                       common::ObTabletID &tablet_id);

  static int get_part_idx_by_tablet_id(share::schema::ObSchemaGetterGuard &schema_guard,
                                       uint64_t arg_table_id,
                                       common::ObTabletID arg_tablet_id,
                                       int64_t &part_idx,
                                       int64_t &subpart_idx);

  static int get_tablet_id_by_part_idx(const share::schema::ObSimpleTableSchemaV2 &table_schema,
                                       const int64_t part_idx,
                                       const int64_t subpart_idx,
                                       common::ObTabletID &tablet_id);

  static int get_part_idx_by_tablet_id(const share::schema::ObSimpleTableSchemaV2 &table_schema,
                                       uint64_t arg_table_id,
                                       common::ObTabletID arg_tablet_id,
                                       int64_t &part_idx,
                                       int64_t &subpart_idx);

private:
  static const ObString KV_NORMAL_TRACE_INFO;
  static const ObString KV_TTL_TRACE_INFO;
};

}  // namespace table
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_TABLE_UTIL_ */