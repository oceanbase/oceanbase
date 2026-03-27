/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMPAT_VERSIONS_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMPAT_VERSIONS_
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

static bool is_mysql_lock_table_data_version(const int64_t data_version)
{
  return ((data_version >= MOCK_DATA_VERSION_4_2_5_0 && data_version < DATA_VERSION_4_3_0_0)
          || (data_version >= DATA_VERSION_4_3_5_2));
}

static bool is_mysql_lock_func_data_version(const int64_t data_version)
{
  return (data_version >= DATA_VERSION_4_3_1_0
          || (data_version >= MOCK_DATA_VERSION_4_2_5_0 && data_version < DATA_VERSION_4_3_0_0));
}

static bool is_dbms_lock_data_version(const int64_t data_version)
{
  return (data_version >= DATA_VERSION_4_3_1_0
          || (data_version >= MOCK_DATA_VERSION_4_2_5_0 && data_version < DATA_VERSION_4_3_0_0));
}

static bool is_rename_cluster_version(const int64_t cluster_version)
{
  return ((cluster_version >= MOCK_CLUSTER_VERSION_4_2_5_0 && cluster_version < CLUSTER_VERSION_4_3_0_0)
          || (cluster_version >= CLUSTER_VERSION_4_3_5_2));
}

} // tablelock
} // transaction
} // oceanbase

#endif // OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMPAT_VERSIONS_
