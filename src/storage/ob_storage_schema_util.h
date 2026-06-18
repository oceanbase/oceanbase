/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_STORAGE_SCHEMA_UTIL_
#define OCEANBASE_STORAGE_STORAGE_SCHEMA_UTIL_
#include "storage/ob_storage_schema.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObTabletID;
}

namespace  storage
{
class ObSSTableArray;

class ObStorageSchemaUtil
{
public:
  static int update_tablet_storage_schema(
      const common::ObTabletID &tablet_id,
      const bool is_major_merge,
      common::ObIAllocator &allocator,
      const ObStorageSchema &old_schema_on_tablet,
      const ObStorageSchema &param_schema,
      const storage::ObSSTableArray &major_sstables,
      ObStorageSchema *&chosen_schema,
      const bool is_tablet_split = false);
  static int update_storage_schema(
      common::ObIAllocator &allocator,
      const ObStorageSchema &other_schema,
      ObStorageSchema &input_schema);
  static int update_storage_schema_by_memtable(
    const ObTablet &tablet,
    const common::ObIArray<ObTableHandleV2> &memtable_handles,
    ObStorageSchema &schema);

  /* TODO(@DanLing) remove this func after column_store merged into master
   * This func is just for replace ObTabletObjLoadHelper::alloc_and_new on master
   */
  static int alloc_storage_schema(
      common::ObIAllocator &allocator,
      ObStorageSchema *&new_storage_schema);
  static void free_storage_schema(
      common::ObIAllocator &allocator,
      ObStorageSchema *&new_storage_schema);
  static int alloc_cs_replica_storage_schema(
      common::ObIAllocator &allocator,
      const ObStorageSchema *storage_schema,
      ObStorageSchema *&new_storage_schema);
private:
  static int get_schema_info_from_memtables_(
    const ObTablet &tablet,
    const common::ObIArray<ObTableHandleV2> &memtable_handles,
    const int64_t column_cnt_in_schema,
    int64_t &max_column_cnt_in_memtable,
    int64_t &max_schema_version_in_memtable);

};

} // namespace storage
} // namespace oceanbase


#endif /* OCEANBASE_STORAGE_STORAGE_SCHEMA_UTIL_ */
