/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ob_i_table.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "ob_storage_schema_util.h"
namespace oceanbase
{

using namespace common;
using namespace share::schema;

namespace storage
{

int ObStorageSchemaUtil::update_tablet_storage_schema(
    const common::ObTabletID &tablet_id,
    const bool is_major_merge,
    common::ObIAllocator &allocator,
    const ObStorageSchema &old_schema_on_tablet,
    const ObStorageSchema &param_schema,
    const ObSSTableArray &major_sstables,
    ObStorageSchema *&new_storage_schema_ptr,
    const bool is_tablet_split)
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
    // param schema may from major merge, will have column info, so if col cnt equal use param schema instead of tablet schema
    const ObStorageSchema *input_schema = tablet_schema_stored_col_cnt > param_schema_stored_col_cnt
                        ? &old_schema_on_tablet
                        : &param_schema;
    const ObStorageSchema *other_schema = input_schema == &old_schema_on_tablet
                        ? &param_schema
                        : &old_schema_on_tablet;
    const ObStorageSchema *column_group_schema = old_schema_on_tablet.get_column_group_count(false/*has hidden cg*/) > param_schema.get_column_group_count(false/*has hidden cg*/)
                                               ? &old_schema_on_tablet
                                               : &param_schema;
    const int64_t result_schema_column_cnt = MAX(old_schema_on_tablet.get_column_count(), param_schema.get_column_count());
    const bool column_info_simplified = input_schema->get_store_column_schemas().count() != result_schema_column_cnt;
    const int64_t input_progressive_merge_round = input_schema->get_progressive_merge_round();
    const int64_t other_progressive_merge_round = other_schema->get_progressive_merge_round();
    ObStorageSchema::ColumnGroupLayout target_layout = ObStorageSchema::INVALID;
    bool need_rebuild_cg_schema = false;
    if (is_major_merge && !major_sstables.empty()) {
      const ObITable *last_major = major_sstables.get_boundary_table(true/*last*/);
      if (OB_ISNULL(last_major)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null last major sstable", KR(ret));
      } else if (last_major->is_co_sstable()) {
        const ObCOSSTableV2 *co_sstable = static_cast<const ObCOSSTableV2 *>(last_major);
        if (co_sstable->is_cgs_empty_co_table() || co_sstable->is_row_store_only_co_table()) {
          // do nothing
        } else if (co_sstable->is_all_cg_base() && !co_sstable->has_hidden_rowkey_cg()) {
          // do nothing, old version row-column mixed store table
        } else if (OB_UNLIKELY(column_group_schema->is_row_store())) {
          if (is_tablet_split) {
            target_layout = co_sstable->is_all_cg_base()
                          ? ObStorageSchema::ALL_CG_WITH_HIDDEN
                          : ObStorageSchema::PURE_COL;
            need_rebuild_cg_schema = true;
          }
        } else if (!co_sstable->has_hidden_rowkey_cg() && column_group_schema->has_hidden_rowkey_column_group()) {
          target_layout = ObStorageSchema::PURE_COL;
        } else if (co_sstable->has_hidden_rowkey_cg() && !column_group_schema->has_hidden_rowkey_column_group()) {
          target_layout = ObStorageSchema::ALL_CG_WITH_HIDDEN;
        }
      } else if (!column_group_schema->is_row_store()) {
        target_layout = ObStorageSchema::ROW_STORE;
      }
    }

    if (FAILEDx(alloc_storage_schema(allocator, new_storage_schema_ptr))) {
      LOG_WARN("failed to alloc mem for tmp storage schema", K(ret), K(param_schema), K(old_schema_on_tablet));
    } else if (!need_rebuild_cg_schema
             && OB_FAIL(new_storage_schema_ptr->init(allocator,
                                                     *input_schema,
                                                     column_info_simplified,
                                                     column_group_schema,
                                                     false/*generate_cs_replica_cg_array*/,
                                                     nullptr/*update_param*/,
                                                     target_layout))) {
      LOG_WARN("fail to init new storage schema", K(ret), K(input_schema), "target_layout", static_cast<int64_t>(target_layout));
    } else if (need_rebuild_cg_schema
            && OB_FAIL(new_storage_schema_ptr->init_for_split_rebuild_col_layout(allocator,
                                                                                 *other_schema,
                                                                                 target_layout))) {
      LOG_WARN("fail to rebuild col layout storage schema for split", K(ret), KPC(input_schema),
          "target_layout", static_cast<int64_t>(target_layout));
    } else {
      new_storage_schema_ptr->update_column_cnt_and_schema_version(
        result_schema_column_cnt,
        MAX(tablet_schema_stored_col_cnt, param_schema_stored_col_cnt),
        MAX(tablet_schema_version, param_schema_version));
      if (other_progressive_merge_round > input_progressive_merge_round) {
        new_storage_schema_ptr->progressive_merge_round_ = other_schema->get_progressive_merge_round();
        new_storage_schema_ptr->row_store_type_ = other_schema->get_row_store_type();
        new_storage_schema_ptr->block_size_ = other_schema->get_block_size();
        new_storage_schema_ptr->compressor_type_ = other_schema->get_compressor_type();
      }
      if (OB_UNLIKELY(!new_storage_schema_ptr->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("generated schema is invalid", KR(ret), KPC(new_storage_schema_ptr), K(old_schema_on_tablet), K(param_schema));
      } else if (param_schema_version > tablet_schema_version
          || param_schema_stored_col_cnt > tablet_schema_stored_col_cnt) {
        // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
        LOG_INFO("success to init storage schema from param_schema",
            K(tablet_id), K(tablet_schema_version), K(param_schema_version),
            K(tablet_schema_stored_col_cnt), K(param_schema_stored_col_cnt),
            K(input_progressive_merge_round), K(other_progressive_merge_round),
            K(old_schema_column_group_cnt), K(param_schema_column_group_cnt), KPC(new_storage_schema_ptr), K(lbt()));
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
  if (OB_FAIL(src_schema.get_store_column_count(src_schema_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get stored column count from schema", KR(ret), K(src_schema));
  } else {
    dst_schema.update_column_cnt_and_schema_version(
      src_schema.get_column_count(),
      src_schema_stored_col_cnt,
      src_schema.get_schema_version());
    if (src_schema.get_column_group_count() > dst_schema.get_column_group_count()) {
      dst_schema.reset_column_group_array();
      if (OB_FAIL(dst_schema.deep_copy_column_group_array(allocator, src_schema))) {
        LOG_WARN("failed to deep copy column group array", KR(ret), K(src_schema));
      }
    }
  }
  return ret;
}

int ObStorageSchemaUtil::update_storage_schema_by_memtable(
      const ObTablet &tablet,
      const common::ObIArray<ObTableHandleV2> &memtable_handles,
      ObStorageSchema &schema)
{
  int ret = OB_SUCCESS;
  int64_t max_column_cnt_in_memtable = 0;
  int64_t max_schema_version_in_memtable = 0;
  int64_t column_cnt_in_schema = 0;

  if (!tablet.is_valid() || !schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet), K(schema));
  } else if (OB_FAIL(schema.get_store_column_count(column_cnt_in_schema, true/*full_col*/))) {
    LOG_WARN("failed to get store column count from schema", KR(ret), K(schema));
  } else if (OB_FAIL(get_schema_info_from_memtables_(tablet, memtable_handles, column_cnt_in_schema,
      max_column_cnt_in_memtable, max_schema_version_in_memtable))) {
    LOG_WARN("failed to get schema info from memtables", KR(ret), K(tablet), K(memtable_handles), K(column_cnt_in_schema));
  } else if (max_column_cnt_in_memtable > column_cnt_in_schema || max_schema_version_in_memtable > schema.get_schema_version()) {
    schema.update_column_cnt_and_schema_version(
      max_column_cnt_in_memtable,
      MAX(column_cnt_in_schema, max_column_cnt_in_memtable),
      max_schema_version_in_memtable);
  }

  return ret;
}

int ObStorageSchemaUtil::get_schema_info_from_memtables_(
  const ObTablet &tablet,
  const common::ObIArray<ObTableHandleV2> &memtable_handles,
  const int64_t column_cnt_in_schema,
  int64_t &max_column_cnt_in_memtable,
  int64_t &max_schema_version_in_memtable)
{
  int ret = OB_SUCCESS;
  int64_t max_column_cnt_on_recorder = 0;
  max_column_cnt_in_memtable = 0;
  max_schema_version_in_memtable = 0;
  const ObITable *table = nullptr;
  const memtable::ObMemtable *memtable = nullptr;

  ARRAY_FOREACH(memtable_handles, idx) {
    table = memtable_handles.at(idx).get_table();
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table pointer", K(ret), K(idx));
    } else if (!table->is_memtable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is not a memtable", KR(ret), K(idx), KPC(table));
    } else if (OB_ISNULL(memtable = static_cast<const memtable::ObMemtable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table pointer does not point to a ObMemtable object", KR(ret), KPC(table));
    } else if (OB_FAIL(memtable->get_schema_info(column_cnt_in_schema,
        max_schema_version_in_memtable, max_column_cnt_in_memtable))) {
      LOG_WARN("failed to get schema info from memtable", KR(ret), KPC(memtable));
    }
  }

  if (FAILEDx(tablet.get_max_column_cnt_on_schema_recorder(max_column_cnt_on_recorder))) {
    LOG_WARN("failed to get max column cnt on schema recorder", KR(ret));
  } else {
    max_column_cnt_in_memtable = MAX(max_column_cnt_in_memtable, max_column_cnt_on_recorder);
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

int ObStorageSchemaUtil::alloc_cs_replica_storage_schema(
    common::ObIAllocator &allocator,
    const ObStorageSchema *storage_schema,
    ObStorageSchema *&cs_replica_storage_schema)
{
  int ret = OB_SUCCESS;
  cs_replica_storage_schema = nullptr;
  if (OB_UNLIKELY(nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input storage schema is null", K(ret));
  } else if (OB_FAIL(alloc_storage_schema(allocator,
                                          cs_replica_storage_schema))) {
    LOG_WARN("fail to allocate cs replica storage schema", K(ret));
  } else if (OB_FAIL(cs_replica_storage_schema->init(allocator,
                                                     *storage_schema,
                                                     false/*skip_column_info*/,
                                                     nullptr/*column_group_schema*/,
                                                     true/*generate_cs_replica_cg_array*/))) {
    LOG_WARN("fail to initialize cs replica storage schema", K(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
