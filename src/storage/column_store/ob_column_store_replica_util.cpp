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
 #include "share/ls/ob_ls_table_operator.h"
 #define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
ERRSIM_POINT_DEF(EN_LS_NOT_SEE_CS_REPLICA);

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

  if (ls.is_cs_replica()) {
    LOG_INFO("[CS-Replica] Finish check need wait major convert", K(ret), K(tablet_id), K(need_process_cs_replica), K(need_wait_major_convert), K(ls), KPC(storage_schema), K(tablet), KPC(table_store), KPC(sstable));
  }
  ObTabletObjLoadHelper::free(arena_allocator, storage_schema);
  return ret;
}

int ObCSReplicaUtil::check_need_process_for_cs_replica_for_ddl(
    const ObTablet &tablet,
    const ObStorageSchema &schema,
    bool &need_process_cs_replica)
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  const bool cs_replica_visable = tablet_meta.is_cs_replica_global_visable_when_ddl();
  need_process_cs_replica = cs_replica_visable
                         && tablet_meta.tablet_id_.is_user_tablet()
                         && schema.is_row_store()
                         && schema.is_user_data_table();

  LOG_INFO("[CS-Replica] check replica set need process cs replica", K(ret), K(tablet_meta), K(schema), K(cs_replica_visable), K(need_process_cs_replica));
  return ret;
}

int ObCSReplicaUtil::check_cs_replica_global_visible(
    const ObLSInfo &ls_info,
    bool &is_global_visible)
{
  int ret = OB_SUCCESS;
  is_global_visible = false;
  bool has_leader = false;
  bool has_cs_replica = false;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < ls_info.get_replicas().count(); ++idx) {
    const ObLSReplica &replica = ls_info.get_replicas().at(idx);
    if (ObRole::LEADER == replica.get_role()) {
      has_leader = true;
    }
    if (replica.is_column_replica()) {
      has_cs_replica = true;
    }
  }
  is_global_visible = has_leader && has_cs_replica;
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (EN_LS_NOT_SEE_CS_REPLICA) {
      is_global_visible = false;
      LOG_INFO("ERRSIM EN_LS_NOT_SEE_CS_REPLICA", K(ret), K(is_global_visible));
    }
  }
#endif
  return ret;
}

int ObCSReplicaUtil::get_cs_replica_ls_set(
    const ObIArray<share::ObLSID> &ls_id_array,
    int64_t tenant_id,
    hash::ObHashSet<share::ObLSID> &contain_cs_replica_ls_id_set)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!contain_cs_replica_ls_id_set.created() || !contain_cs_replica_ls_id_set.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (ls_id_array.empty()) {
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst operator is null", K(ret));
  } else if (!is_user_tenant(tenant_id)) {
  } else {
    const int64_t cluster_id = GCONF.cluster_id;
    ObSEArray<ObLSInfo, 4> ls_infos;
    ObSEArray<share::ObLSID, 4> sorted_ls_id_array;
    ObSEArray<share::ObLSID, 4> unique_ls_id_array;
    if (OB_FAIL(sorted_ls_id_array.reserve(ls_id_array.count()))) {
      LOG_WARN("failed to reserve array", K(ret));
    } else if (OB_FAIL(sorted_ls_id_array.assign(ls_id_array))) {
      LOG_WARN("failed to assign array", K(ret));
    } else {
      // remove duplicated ls_id to do batch_get
      lib::ob_sort(sorted_ls_id_array.begin(), sorted_ls_id_array.end());
      for (int64_t idx = 0; OB_SUCC(ret) && idx < sorted_ls_id_array.count(); ++idx) {
        if (!sorted_ls_id_array.at(idx).is_user_ls()) {
        } else if (idx >= 1 && sorted_ls_id_array.at(idx) == sorted_ls_id_array.at(idx - 1)) {
        } else if (OB_FAIL(unique_ls_id_array.push_back(sorted_ls_id_array.at(idx)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }

    if (OB_FAIL(ret) || unique_ls_id_array.empty()) {
    } else if (OB_FAIL(GCTX.lst_operator_->batch_get(cluster_id, tenant_id, unique_ls_id_array, ObLSTable::DEFAULT_MODE, ls_infos))) {
      LOG_WARN("failed to get ls infos", K(ret), K(cluster_id), K(tenant_id), K(unique_ls_id_array));
    } else {
      // batch get will drop duplicate ls id, so no need to check duplicate
      for (int64_t idx = 0; OB_SUCC(ret) && idx < ls_infos.count(); ++idx) {
        const ObLSInfo &ls_info = ls_infos.at(idx);
        bool is_global_visible = false;
        if (OB_FAIL(check_cs_replica_global_visible(ls_info, is_global_visible))) {
          LOG_WARN("failed to check need process cs replica", K(ret), K(ls_info));
        } else if (!is_global_visible) {
        } else if (OB_FAIL(contain_cs_replica_ls_id_set.set_refactored(ls_info.get_ls_id()))) {
          LOG_WARN("failed to set ls id", K(ret), K(ls_info));
        }
      }
    }
  }
  return ret;
}

int ObCSReplicaUtil::check_need_process_cs_replica_for_offline_ddl(
    const ObTableSchema &orig_table_schema,
    bool &need_process)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  bool is_column_group_store = false;
  need_process = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    // not support column store replica
  } else if (!is_user_tenant(tenant_id) || !orig_table_schema.is_user_table()) {
  } else if (OB_FAIL(orig_table_schema.get_is_column_store(is_column_group_store))) {
    LOG_WARN("fail to check schema is column group store", K(ret));
  } else if (is_column_group_store) {
    // originally column store
  } else {
    ObSEArray<ObLSInfo, 8> ls_infos;
    ls_infos.set_attr(ObMemAttr(tenant_id, "DDLLSInfos"));
    if (OB_ISNULL(GCTX.lst_operator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lst_operator is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GCTX.lst_operator_->get_by_tenant(tenant_id, false/*inner_table_only*/, ls_infos))) {
      LOG_WARN("fail to get ls infos", KR(ret), K(tenant_id));
    } else {
      // 3. update ls_infos cached in memory
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_infos.count(); ++i) {
        const ObLSInfo &ls_info = ls_infos.at(i);
        bool is_global_visible = false;
        if (OB_FAIL(check_cs_replica_global_visible(ls_info, is_global_visible))) {
          LOG_WARN("failed to check need process cs replica", K(ret), K(ls_info));
        } else if (is_global_visible) {
          need_process = true;
          break;
        }
      }
    }
  }
  return ret;
}

/*---------------------------------- ObCSReplicaStorageSchemaGuard -------------------------------- */

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

/*---------------------------------- ObGlobalCSReplicaMgr -------------------------------- */

ObGlobalCSReplicaMgr::ObGlobalCSReplicaMgr()
  : cs_replica_ls_id_set_(),
    is_inited_(false)
{
}

ObGlobalCSReplicaMgr::~ObGlobalCSReplicaMgr()
{
  if (cs_replica_ls_id_set_.created()) {
    cs_replica_ls_id_set_.destroy();
  }
}

int ObGlobalCSReplicaMgr::try_init(const int64_t tenant_id, const ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(cs_replica_ls_id_set_.create(CS_REPLICA_LS_ID_SET_BUCKET_NUM, "CSRplLSIDSet", "CSRplLSIDSet"))) { // use 500 tenant memory since the tenant may not been created
    LOG_WARN("failed to create cs replica set", K(ret));
  } else if (OB_FAIL(ObCSReplicaUtil::get_cs_replica_ls_set(ls_id_array, tenant_id, cs_replica_ls_id_set_))) {
    LOG_WARN("failed to get cs replica ls set", K(ret), K(tenant_id), K(ls_id_array));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObGlobalCSReplicaMgr::check_cs_replica_global_visible(
    const share::ObLSID &ls_id,
    bool &is_global_visible) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  is_global_visible = false;
  if (IS_NOT_INIT) {
    // may init failed, it does not matter, take it as like cs replica not visiable
  } else if (OB_TMP_FAIL(cs_replica_ls_id_set_.exist_refactored(ls_id))) {
    if (OB_HASH_NOT_EXIST == tmp_ret) {
    } else if (OB_HASH_EXIST == tmp_ret) {
      is_global_visible = true;
    } else {
      ret = tmp_ret;
      LOG_WARN("failed to check ls id", K(ret), K(ls_id));
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase