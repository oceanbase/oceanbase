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

#define USING_LOG_PREFIX STORAGE

#include "ob_storage_schema_util.h"
#include "ob_storage_schema.h"
#include "lib/allocator/page_arena.h"
#include "storage/ob_storage_struct.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{

using namespace common;
using namespace share::schema;

namespace storage
{

int ObStorageSchemaUtil::update_tablet_storage_schema(
    const common::ObTabletID &tablet_id,
    common::ObIAllocator &allocator,
    const ObStorageSchema &old_schema_on_tablet,
    const ObStorageSchema &param_schema,
    ObStorageSchema *&new_storage_schema_ptr)
{
  int ret = OB_SUCCESS;
  int64_t tablet_schema_stored_col_cnt = 0;
  int64_t param_schema_stored_col_cnt = 0;

  if (OB_UNLIKELY(!old_schema_on_tablet.is_valid() || !param_schema.is_valid() || NULL != new_storage_schema_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input schema is invalid", K(ret), K(old_schema_on_tablet), K(param_schema), KPC(new_storage_schema_ptr));
  } else if (OB_FAIL(old_schema_on_tablet.get_store_column_count(tablet_schema_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get stored column count from schema", KR(ret), K(old_schema_on_tablet));
  } else if (OB_FAIL(param_schema.get_store_column_count(param_schema_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get stored column count from schema", KR(ret), K(param_schema));
  } else {
    const int64_t tablet_schema_version = old_schema_on_tablet.schema_version_;
    const int64_t param_schema_version = param_schema.schema_version_;
    const int64_t old_schema_column_group_cnt = old_schema_on_tablet.get_column_group_count();
    const int64_t param_schema_column_group_cnt = param_schema.get_column_group_count();
    const ObStorageSchema *column_group_schema = old_schema_column_group_cnt > param_schema_column_group_cnt
                        ? &old_schema_on_tablet
                        : &param_schema;
    const ObStorageSchema *input_schema = tablet_schema_stored_col_cnt > param_schema_stored_col_cnt
                        ? &old_schema_on_tablet
                        : &param_schema;
    if (OB_FAIL(alloc_storage_schema(allocator, new_storage_schema_ptr))) {
      LOG_WARN("failed to alloc mem for tmp storage schema", K(ret), K(param_schema), K(old_schema_on_tablet));
    } else if (OB_FAIL(new_storage_schema_ptr->init(allocator, *input_schema, false/*skip_solumn_info*/, column_group_schema))) {
      // use param_schema as default base schema to init
      LOG_WARN("fail to init new storage schema", K(ret), K(input_schema));
    } else {
      new_storage_schema_ptr->column_cnt_ = MAX(old_schema_on_tablet.get_column_count(), param_schema.get_column_count());
      new_storage_schema_ptr->store_column_cnt_ = MAX(tablet_schema_stored_col_cnt, param_schema_stored_col_cnt);
      new_storage_schema_ptr->schema_version_ = MAX(tablet_schema_version, param_schema_version);
      new_storage_schema_ptr->column_info_simplified_ =
        (new_storage_schema_ptr->column_cnt_ != new_storage_schema_ptr->get_store_column_schemas().count());
      if (param_schema_version > tablet_schema_version
          || param_schema_stored_col_cnt > tablet_schema_stored_col_cnt
          || param_schema_column_group_cnt > old_schema_column_group_cnt) {
        // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
        LOG_INFO("success to init storage schema from param_schema",
            K(tablet_id), K(tablet_schema_version), K(param_schema_version),
            K(tablet_schema_stored_col_cnt), K(param_schema_stored_col_cnt),
            K(old_schema_column_group_cnt), K(param_schema_column_group_cnt), KPC(new_storage_schema_ptr));
      }
    }
  }

  if (OB_FAIL(ret)) {
    free_storage_schema(allocator, new_storage_schema_ptr);
  }

  return ret;
}

int ObStorageSchemaUtil::update_storage_schema(
      common::ObIAllocator &allocator,
      const ObStorageSchema &src_schema,
      ObStorageSchema &dst_schema)
{
  int ret = OB_SUCCESS;
  int64_t src_schema_stored_col_cnt = 0;
  if (OB_FAIL(src_schema.get_stored_column_count_in_sstable(src_schema_stored_col_cnt))) {
    LOG_WARN("failed to get stored column count from schema", KR(ret), K(src_schema));
  } else {
    dst_schema.column_cnt_ = MAX(dst_schema.get_column_count(), src_schema.get_column_count());
    dst_schema.store_column_cnt_ = MAX(dst_schema.store_column_cnt_, src_schema_stored_col_cnt);
    dst_schema.schema_version_ = MAX(dst_schema.schema_version_, src_schema.get_schema_version());
    if (src_schema.get_column_group_count() > dst_schema.get_column_group_count()) {
      dst_schema.reset_column_group_array();
      if (OB_FAIL(dst_schema.deep_copy_column_group_array(allocator, src_schema))) {
        LOG_WARN("failed to deep copy column group array", KR(ret), K(src_schema));
      }
    }
  }
  return ret;
}

int ObStorageSchemaUtil::alloc_storage_schema(
    common::ObIAllocator &allocator,
    ObStorageSchema *&new_storage_schema)
{
  int ret = OB_SUCCESS;
  void *buffer = allocator.alloc(sizeof(ObStorageSchema));

  if (OB_ISNULL(buffer)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate mem for storage schema", K(ret));
  } else {
    new_storage_schema = new (buffer) ObStorageSchema();
  }
  return ret;
}

void ObStorageSchemaUtil::free_storage_schema(
    common::ObIAllocator &allocator,
    ObStorageSchema *&storage_schema)
{
  if (NULL != storage_schema) {
    storage_schema->~ObStorageSchema();
    allocator.free(storage_schema);
    storage_schema = nullptr;
  }
}

} // namespace storage
} // namespace oceanbase
