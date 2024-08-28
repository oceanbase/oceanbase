/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

 #include "storage/tx_storage/ob_ls_service.h"
 #include "storage/tablet/ob_mds_schema_helper.h"
 #include "storage/column_store/ob_column_store_replica_util.h"
 #define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

int ObCSReplicaUtil::check_is_cs_replica(
    const ObTableSchema &table_schema,
    const ObTablet &tablet,
    bool &is_cs_replica)
{
  int ret = OB_SUCCESS;
  bool is_row_store = false;
  is_cs_replica = false;
  if (OB_FAIL(table_schema.get_is_row_store(is_row_store))) {
    LOG_WARN("fail to get is row store", K(ret), K(table_schema));
  } else {
    is_cs_replica = is_row_store
                 && table_schema.is_user_table()
                 && !tablet.is_row_store() // tablet in cs replica is not row store
                 && tablet.get_tablet_id().is_user_tablet();
  }
  return ret;
}

int ObCSReplicaUtil::check_local_is_cs_replica(
    const share::ObLSID &ls_id,
    bool &is_cs_replica)
{
  int ret = OB_SUCCESS;
  is_cs_replica = false;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      // Only use weak consistency locally read for cs replica, so local ls must exist.
      // If ls not exist, table param or dml param is constructed remotely, ignore it.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get nullptr ls", K(ret), K(ls_id), K(ls_handle));
  } else {
    is_cs_replica = ls->is_cs_replica();
  }
  return ret;
}

bool ObCSReplicaUtil::check_need_convert_cs_when_migration(
    const ObTablet &tablet,
    const ObStorageSchema& schema_on_tablet)
{
  return schema_on_tablet.is_row_store()
      && schema_on_tablet.is_user_data_table()
      && tablet.is_row_store()
      && tablet.get_tablet_id().is_user_tablet();
}

int ObCSReplicaUtil::check_has_cs_replica(
    const share::ObLSID &ls_id,
    bool &has_column_store_replica)
{
  int ret = OB_SUCCESS;
  has_column_store_replica = false;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get nullptr ls", K(ret), K(ls_id), K(ls_handle));
  } else if (OB_FAIL(ls->check_has_cs_replica(has_column_store_replica))) {
    LOG_WARN("failed to check ls replica set", K(ret), KPC(ls));
  }
  return ret;
}

int ObCSReplicaUtil::check_need_process_cs_replica(
    const ObLS &ls,
    const ObTabletID &tablet_id,
    const ObStorageSchema &schema,
    bool &need_process_cs_replica)
{
  int ret = OB_SUCCESS;
  need_process_cs_replica = ls.is_cs_replica()
                           && tablet_id.is_user_tablet()
                           && (schema.is_row_store() || schema.is_cs_replica_compat())
                           && schema.is_user_data_table();
  return ret;
}

int ObCSReplicaUtil::check_need_wait_major_convert(
    const ObLS &ls,
    const ObTabletID &tablet_id,
    const ObTablet &tablet,
    bool &need_wait_major_convert)
{
  int ret = OB_SUCCESS;
  bool need_process_cs_replica = false;
  ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator arena_allocator(common::ObMemAttr(MTL_ID(), "CkMjrCvrt"));
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  const ObTabletTableStore *table_store = nullptr;
  const ObITable *sstable = nullptr;
  need_wait_major_convert = false;
  if (OB_FAIL(tablet.load_storage_schema(arena_allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K(tablet));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is nullptr", K(ret), K(tablet));
  } else if (OB_FAIL(check_need_process_cs_replica(ls, tablet_id, *storage_schema, need_process_cs_replica))) {
    LOG_WARN("fail to check need process cs replica", K(ret), K(ls), K(tablet_id), KPC(storage_schema));
  } else if (need_process_cs_replica) {
    if (tablet.is_row_store()) {
      // tablet migration but not do co convert
      need_wait_major_convert = true;
    } else if (OB_FAIL(tablet.fetch_table_store(wrapper))) {
      LOG_WARN("failed to fetch table store", K(ret), K(tablet_id), K(tablet));
    } else if (OB_ISNULL(table_store = wrapper.get_member())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table store is nullptr", K(ret), K(tablet_id), K(tablet));
    } else if (OB_ISNULL(sstable = table_store->get_major_sstables().get_boundary_table(true /*is_last*/))) {
      if (!tablet.get_tablet_meta().table_store_flag_.with_major_sstable()) {
        ret = OB_SSTABLE_NOT_EXIST;
        LOG_WARN("latest major is nullptr", K(ret), K(tablet_id), K(tablet));
      }
    } else {
      // ddl write row store major
      need_wait_major_convert = ObITable::is_row_store_major_sstable(sstable->get_key().table_type_);
    }
  }
  ObTabletObjLoadHelper::free(arena_allocator, storage_schema);
  return ret;
}

int ObCSReplicaUtil::check_replica_set_need_process_cs_replica(
    const ObLS &ls,
    const ObTabletID &tablet_id,
    const ObStorageSchema &schema,
    bool &need_process_cs_replica)
{
  int ret = OB_SUCCESS;
  need_process_cs_replica = false;
  if (OB_FAIL(ls.check_has_cs_replica(need_process_cs_replica))) {
    LOG_WARN("failed to check ls replica set", K(ret), K(ls));
  } else if (need_process_cs_replica) {
    need_process_cs_replica = tablet_id.is_user_tablet()
                           && schema.is_row_store()
                           && schema.is_user_data_table();
  }
  return ret;
}

ObCSReplicaStorageSchemaGuard::ObCSReplicaStorageSchemaGuard()
  : is_inited_(false),
    schema_(nullptr)
{
}

ObCSReplicaStorageSchemaGuard::~ObCSReplicaStorageSchemaGuard()
{
  reset();
}

int ObCSReplicaStorageSchemaGuard::init(
    const ObTabletHandle &tablet_handle,
    compaction::ObCompactionMemoryContext &mem_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *schema_on_tablet = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(mem_ctx.get_allocator(), schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", K(ret), K(tablet_handle));
  } else {
    schema_ = schema_on_tablet;
    is_inited_ = true;
  }
  return ret;
}

void ObCSReplicaStorageSchemaGuard::reset()
{
  if (IS_INIT) {
    if (OB_NOT_NULL(schema_)) {
      schema_->~ObStorageSchema();
      schema_ = nullptr;
    }
    is_inited_ = false;
  }
}

int ObCSReplicaStorageSchemaGuard::load(ObStorageSchema *&storage_schema)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is nullptr", K(ret));
  } else {
    storage_schema = schema_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase