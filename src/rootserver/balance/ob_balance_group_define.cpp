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
#define USING_LOG_PREFIX BALANCE

#include "lib/string/ob_string.h"             // ObString
#include "lib/string/ob_sql_string.h"         // ObSqlString
#include "share/schema/ob_schema_mgr.h"       // ObSimpleTableSchemaV2
#include "share/schema/ob_table_schema.h"     // ObTableSchema
#include "share/schema/ob_schema_struct.h"    // ObPartition

#include "ob_balance_group_define.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver
{

const char* ObBalanceGroup::NON_PART_BG_NAME = "NON_PART_TABLE";

int ObBalanceGroup::init_by_tablegroup(const ObSimpleTablegroupSchema &tg,
    const int64_t max_part_level,
    const int64_t part_group_index/* = 0*/)
{
  int ret = OB_SUCCESS;
  ObSqlString bg_name_str;
  const ObString &tg_name = tg.get_tablegroup_name();

  if (tg.is_sharding_none()
      || tg.is_sharding_partition()
      || (tg.is_sharding_adaptive()
      && (PARTITION_LEVEL_ZERO == max_part_level || PARTITION_LEVEL_ONE == max_part_level))) {
    // Table Group is a independent balance group
    if (OB_FAIL(bg_name_str.append_fmt("TABLEGROUP_%s", tg_name.ptr()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tg));
    } else {
      id_ = ObBalanceGroupID(tg.get_tablegroup_id(), 0);
    }
  } else if (!tg.is_sharding_adaptive() || PARTITION_LEVEL_TWO != max_part_level || part_group_index < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(max_part_level), K(part_group_index), K(tg));
  } else {
    // Every one-level partition is an independent balance group
    if (OB_FAIL(bg_name_str.append_fmt("TABLEGROUP_%s_PART_GROUP_%ld", tg_name.ptr(), part_group_index))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tg));
    } else {
      id_ = ObBalanceGroupID(tg.get_tablegroup_id(), part_group_index);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(name_.assign(bg_name_str.ptr()))) {
    LOG_WARN("fail to assign bg name", KR(ret), K(bg_name_str));
  }
  return ret;
}

int ObBalanceGroup::hash(uint64_t &res) const
{
  return id_.hash(res);
}
int ObBalanceGroup::init_by_table(const ObSimpleTableSchemaV2 &table_schema,
    const ObPartition *partition/* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString bg_name_str;
  const uint64_t table_id = table_schema.get_table_id();
  const ObString &table_name = table_schema.get_table_name();
  const int64_t part_level = table_schema.get_part_level();
  const bool is_in_tablegroup = (OB_INVALID_ID != table_schema.get_tablegroup_id());

  // Table should not be in tablegroup
  // NOTE: global index should have no tablegroup id, if it is in tablegroup, it is BUG
  if (OB_UNLIKELY(is_in_tablegroup && ! table_schema.is_global_index_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is in tablegroup, should init balance group by tablegroup", KR(ret), K(table_schema));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    // All tenant's non-partition table is a balance group
    if (OB_FAIL(bg_name_str.append_fmt("%s", NON_PART_BG_NAME))) {
      LOG_WARN("fail to append fmt", KR(ret), K(table_schema));
    } else {
      id_ = ObBalanceGroupID(0, 0);
    }
  } else if (PARTITION_LEVEL_ONE == part_level) {
    // Level one partition table is a single balance group
    if (OB_FAIL(bg_name_str.append_fmt("TABLE_%s_%lu", table_name.ptr(), table_id))) {
      LOG_WARN("fail to append fmt", KR(ret), K(table_name), K(table_id));
    } else {
      id_ = ObBalanceGroupID(table_id, 0);
    }
  } else if (PARTITION_LEVEL_TWO == part_level) {
    // Every one-level partition is an independent balance group
    if (OB_ISNULL(partition)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(table_schema), KPC(partition));
    } else if (OB_FAIL(bg_name_str.append_fmt("TABLE_%s_%lu_PART_%s",
        table_name.ptr(), table_id, partition->get_part_name().ptr()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(table_name), K(table_id), KPC(partition));
    } else {
      id_ = ObBalanceGroupID(table_id, partition->get_part_id());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(name_.assign(bg_name_str.ptr()))) {
    LOG_WARN("fail to assign bg name", KR(ret), K(bg_name_str));
  }
  return ret;
}

}
}
