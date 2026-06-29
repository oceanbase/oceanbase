// Copyright (c) 2026 OceanBase
// SPDX-License-Identifier: Apache-2.0
// owner: ouyanghongrong.oyh

#define USING_LOG_PREFIX SHARE
#include "share/compaction/ob_compaction_queuing_utils.h"
#include "lib/random/ob_random.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace share
{
using namespace schema;

ERRSIM_POINT_DEF(EN_RANDOM_TABLE_MODE_FOR_TEST, "randomly assign table mode when create table for buffer table test");

ObTableModeFlag ObCompactionQueuingUtil::pick_random_table_mode_flag_(const int64_t rand_val)
{
  ObTableModeFlag mode = TABLE_MODE_NORMAL;
  if (rand_val < 60) {
    mode = TABLE_MODE_NORMAL;
  } else if (rand_val < 70) {
    mode = TABLE_MODE_QUEUING;
  } else if (rand_val < 80) {
    mode = TABLE_MODE_QUEUING_MODERATE;
  } else if (rand_val < 90) {
    mode = TABLE_MODE_QUEUING_SUPER;
  } else {
    mode = TABLE_MODE_QUEUING_EXTREME;
  }
  return mode;
}

void ObCompactionQueuingUtil::assign_random_table_mode_for_test(
    const bool user_specified_table_mode,
    const bool is_external_table,
    const bool is_oracle_temp_table,
    const uint64_t tenant_id,
    ObTableMode &table_mode,
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_UNLIKELY(EN_RANDOM_TABLE_MODE_FOR_TEST)) {
    if (user_specified_table_mode
        || is_external_table
        || is_oracle_temp_table
        || !table_schema.is_user_table()) {
      // skip
    } else if (OB_INVALID_TENANT_ID == tenant_id) {
      LOG_WARN("EN_RANDOM_TABLE_MODE_FOR_TEST: invalid tenant id, skip random table mode", K(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("get tenant data version failed, skip random table mode", K(ret), K(tenant_id));
    } else {
      const int64_t rand_val = common::ObRandom::rand(0, 99);
      const ObTableModeFlag mode = pick_random_table_mode_flag_(rand_val);
      if (TABLE_MODE_NORMAL == mode) {
        // keep default table mode
      } else if (not_compat_for_queuing_mode(tenant_data_version) && is_new_queuing_mode(mode)) {
        LOG_INFO("EN_RANDOM_TABLE_MODE_FOR_TEST: skip random table mode for incompatible version",
                 K(mode), K(rand_val), K(tenant_data_version), "table_name", table_schema.get_table_name_str());
      } else {
        table_mode.mode_flag_ = mode;
        LOG_INFO("EN_RANDOM_TABLE_MODE_FOR_TEST: assign random table mode",
                 K(mode), K(rand_val), "table_name", table_schema.get_table_name_str());
      }
    }
  }
}

} // namespace share
} // namespace oceanbase
