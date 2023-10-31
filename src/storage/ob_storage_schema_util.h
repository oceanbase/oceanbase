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

// TODO(@DanLing) manage major/minor/mini's storage schema here
class ObStorageSchemaUtil
{
public:
  static int update_tablet_storage_schema(
      const common::ObTabletID &tablet_id,
      common::ObIAllocator &allocator,
      const ObStorageSchema &old_schema_on_tablet,
      const ObStorageSchema &param_schema,
      ObStorageSchema *&chosen_schema);
  static int update_storage_schema(
      common::ObIAllocator &allocator,
      const ObStorageSchema &other_schema,
      ObStorageSchema &input_schema);

  /* TODO(@DanLing) remove this func after column_store merged into master
   * This func is just for replace ObTabletObjLoadHelper::alloc_and_new on master
   */
  static int alloc_storage_schema(
      common::ObIAllocator &allocator,
      ObStorageSchema *&new_storage_schema);
  static void free_storage_schema(
      common::ObIAllocator &allocator,
      ObStorageSchema *&new_storage_schema);
};

} // namespace storage
} // namespace oceanbase


#endif /* OCEANBASE_STORAGE_STORAGE_SCHEMA_UTIL_ */
