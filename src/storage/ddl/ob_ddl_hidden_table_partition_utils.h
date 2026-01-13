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

#ifndef OCEANBASE_STORAGE_DDL_HIDDEN_TABLE_PARTITION_UTILS_H_
#define OCEANBASE_STORAGE_DDL_HIDDEN_TABLE_PARTITION_UTILS_H_

#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
  class ObTableSchema;
}
}
namespace storage
{
// Currently, this class is only used for partition-level direct load,
// with _enable_direct_load_hidden_table_partition_pruning = true
class ObDDLHiddenTablePartitionUtils final
{
public:
  static int check_support_partition_pruning(
      const share::schema::ObTableSchema &orig_table_schema,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      const uint64_t tenant_id,
      int64_t &target_part_idx);

  static int rebuild_table_schema_with_partition_pruning(
      const share::schema::ObTableSchema &orig_table_schema,
      const int64_t target_part_idx,
      share::schema::ObTableSchema &new_table_schema);
};
} // end namespace storage
} // end namespace oceanbase
#endif // OCEANBASE_STORAGE_DDL_HIDDEN_TABLE_PARTITION_UTILS_H_