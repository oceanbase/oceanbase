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
 #define USING_LOG_PREFIX STORAGE

#include "ob_column_store_replica_util.h"
 #include "storage/tx_storage/ob_ls_service.h"
 #include "storage/compaction/ob_medium_compaction_func.h"

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

int ObCSReplicaUtil::check_need_generate_cs_replica_cg_array(
    const ObLS &ls,
    const ObTabletID &tablet_id,
    const ObStorageSchema &schema,
    bool &need_generate_cs_replica_cg_array)
{
  int ret = OB_SUCCESS;
  need_generate_cs_replica_cg_array = ls.is_cs_replica()
                           && tablet_id.is_user_tablet()
                           && schema.is_row_store()
                           && schema.is_user_data_table();
  return ret;
}


int ObCSReplicaUtil::check_need_process_for_cs_replica_for_ddl(
    const ObTablet &tablet,
    const ObStorageSchema &schema,
    bool &need_process_cs_replica)
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  const bool cs_replica_visable = tablet_meta.is_cs_replica_global_visible_when_ddl();
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

int ObCSReplicaUtil::check_need_wait_for_report(
    const ObLS &ls,
    const ObTablet &tablet,
    bool &need_wait_for_report)
{
  int ret = OB_SUCCESS;
  need_wait_for_report = false;
  ObCSReplicaTabletStatus cs_replica_status = ObCSReplicaTabletStatus::MAX_STATUS;
  if (!ls.is_cs_replica()) {
  } else if (OB_FAIL(ObCSReplicaUtil::init_cs_replica_tablet_status(ls, tablet, cs_replica_status))) {
    LOG_WARN("fail to init cs replica tablet status", K(ret), K(ls), K(tablet));
  } else if (OB_UNLIKELY(!is_valid_cs_replica_status(cs_replica_status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cs replica status is invalid", K(ret), K(ls), K(tablet));
  } else if (!is_normal_status(cs_replica_status)) {
    need_wait_for_report = true;
    LOG_INFO("tablet status is not normal, try report later", K(ret), K(cs_replica_status), K(ls), K(tablet));
  }
  return ret;
}

int ObCSReplicaUtil::init_cs_replica_tablet_status(
    const ObLS &ls,
    const ObTablet &tablet,
    ObCSReplicaTabletStatus &cs_replica_status)
{
  int ret = OB_SUCCESS;
  cs_replica_status = ObCSReplicaTabletStatus::MAX_STATUS;
  if (!ls.is_cs_replica()) {
    cs_replica_status = ObCSReplicaTabletStatus::NORMAL;
  } else {
    bool need_procss_cs_replica = false;
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    if (OB_FAIL(ObCSReplicaUtil::check_need_process_cs_replica(ls, tablet, need_procss_cs_replica))) {
      LOG_WARN("fail to check need process cs replica", K(ret), K(ls), K(tablet));
    } else if (!need_procss_cs_replica) {
      cs_replica_status = ObCSReplicaTabletStatus::NORMAL;
    } else if (OB_FAIL(ls.get_ls_meta().get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(ls));
    } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
      cs_replica_status = ObCSReplicaTabletStatus::NOT_COMPLETE;
    } else if (tablet.get_last_major_snapshot_version() <= 0) {
      cs_replica_status = ObCSReplicaTabletStatus::NO_MAJOR_SSTABLE;
    } else {
      if (tablet.is_row_store()) {
        if (tablet.is_last_major_row_store()) {
          cs_replica_status = ObCSReplicaTabletStatus::NEED_CO_CONVERT_MERGE;
        } else {
          cs_replica_status = ObCSReplicaTabletStatus::NEED_CS_STORAGE_SCHEMA;
        }
      } else { // column store
        if (tablet.is_last_major_row_store()) {
          cs_replica_status = ObCSReplicaTabletStatus::NEED_CO_CONVERT_MERGE;
        } else {
          cs_replica_status = ObCSReplicaTabletStatus::NORMAL_CS_REPLICA;
        }
      }
      if (!is_normal_status(cs_replica_status)) {
        LOG_INFO("[CS-Replica] Finish init cs replica tablet status", K(ret), K(ls), K(tablet), K(cs_replica_status), K(need_procss_cs_replica));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_valid_cs_replica_status(cs_replica_status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cs replica status", K(ret), K(cs_replica_status));
  }

  return ret;
}

int ObCSReplicaUtil::check_need_process_cs_replica(
    const ObLS &ls,
    const ObTablet &tablet,
    bool &need_process_cs_replica)
{
  int ret = OB_SUCCESS;
  need_process_cs_replica = false;
  if (tablet.is_cs_replica_compat()) {
    need_process_cs_replica = true;
  } else {
    need_process_cs_replica = ls.is_cs_replica()
                           && tablet.is_user_tablet()
                           && tablet.is_user_data_table()
                           && tablet.is_row_store();
  }
  return ret;
}

int ObCSReplicaUtil::get_full_column_array_from_table_schema(
    common::ObIAllocator &allocator,
    const ObUpdateCSReplicaSchemaParam &update_param,
    const ObStorageSchema &simplified_schema,
    common::ObFixedArray<ObStorageColumnSchema, common::ObIAllocator> &column_array)
{
  int ret = OB_SUCCESS;
  int64_t tenant_schema_version = OB_INVALID_VERSION;
  uint64_t table_id = OB_INVALID_ID;
  const ObTabletID tablet_id = update_param.tablet_id_;
  const int64_t expected_stored_column_cnt = update_param.major_column_cnt_ - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t tenant_id = MTL_ID();
  const ObTableSchema *table_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, tenant_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(compaction::ObMediumCompactionScheduleFunc::get_table_id(schema_service, tablet_id, tenant_schema_version, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(tablet_id), K(tenant_schema_version));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("table is deleted", K(ret), K(table_id));
  } else {
    ObStorageSchema *full_storage_schema = nullptr;
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, full_storage_schema))) {
      LOG_WARN("alloc and new failed", K(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("unable to get tenant data version", K(ret));
    } else if (OB_FAIL(full_storage_schema->init(allocator, *table_schema, simplified_schema.get_compat_mode(),
                  false/*skip_column_info*/, tenant_data_version, true/*generate_cs_replica_cg_array*/))) {
      LOG_WARN("failed to init storage schema", K(ret), K(table_id));
    } else if (OB_FAIL(get_column_array_from_full_storage_schema(allocator, expected_stored_column_cnt, *full_storage_schema, column_array))) {
      LOG_WARN("failed to get column array from full storage schema", K(ret), K(update_param), K(expected_stored_column_cnt), K(full_storage_schema));
    } else {
      LOG_INFO("[CS-Replica] Successfully get column array", K(ret), K(update_param), K(expected_stored_column_cnt), K(column_array));
    }
    ObTabletObjLoadHelper::free(allocator, full_storage_schema);
  }
  return ret;
}

// The count of reconstructed column array must be equal to the column count of the lastest major in tablet.
// Otherwise, after convert co merge, the column count in co major will be diffrent with that in F replica, cause column data checksum check failed.
int ObCSReplicaUtil::get_column_array_from_full_storage_schema(
    common::ObIAllocator &allocator,
    const int64_t expected_stored_column_cnt,
    const ObStorageSchema &full_storage_schema,
    common::ObFixedArray<ObStorageColumnSchema, common::ObIAllocator> &column_array)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = 0;
  int64_t stored_column_cnt = 0;

  // make sure column array include all stored column in lastest major sstable
  for (int64_t i = 0; i < full_storage_schema.column_array_.count(); ++i) {
    const ObStorageColumnSchema &col_schema = full_storage_schema.column_array_.at(i);
    if (col_schema.is_column_stored_in_sstable()) {
      stored_column_cnt++;
      if (stored_column_cnt == expected_stored_column_cnt) {
        column_cnt = i + 1;
        break;
      }
    }
  }

  if (OB_UNLIKELY(column_cnt <= 0 || column_cnt > full_storage_schema.column_array_.count()
               || expected_stored_column_cnt <= 0 || expected_stored_column_cnt != stored_column_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column cnt", K(ret), K(column_cnt), K(stored_column_cnt), K(expected_stored_column_cnt), K(stored_column_cnt), K(full_storage_schema));
  } else if (OB_FAIL(column_array.init(column_cnt))) {
    LOG_WARN("failed to init column array", K(ret), K(column_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      ObStorageColumnSchema col_schema;
      const ObStorageColumnSchema &src_col_schema = full_storage_schema.column_array_.at(i);
      col_schema.info_ = src_col_schema.info_;
      col_schema.default_checksum_ = src_col_schema.default_checksum_;
      col_schema.meta_type_ = src_col_schema.meta_type_;
      if (OB_FAIL(col_schema.deep_copy_default_val(allocator, src_col_schema.orig_default_value_))) {
        STORAGE_LOG(WARN, "failed to deep copy col schema", K(ret), K(i), K(src_col_schema));
      } else if (OB_FAIL(column_array.push_back(col_schema))) {
        STORAGE_LOG(WARN, "failed to push back col schema", K(ret));
        col_schema.destroy(allocator);
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < column_array.count(); ++i) {
        column_array.at(i).destroy(allocator);
      }
      column_array.reset();
    }
  }
  return ret;
}

int ObCSReplicaUtil::get_rebuild_storage_schema(
    common::ObIAllocator &allocator,
    const ObUpdateCSReplicaSchemaParam &param,
    const ObStorageSchema &simplified_schema,
    ObStorageSchema *&full_storage_schema)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *schema = nullptr;
  if (OB_UNLIKELY(OB_NOT_NULL(full_storage_schema) || !param.is_valid()
                  || !simplified_schema.is_valid() || !simplified_schema.is_column_info_simplified())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(full_storage_schema), K(param), K(simplified_schema));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, schema))) {
    LOG_WARN("fail to alloc and new storage schema", K(ret));
  } else if (OB_FAIL(schema->init(allocator, simplified_schema, false /*skip_column_info*/,
                                  nullptr /*column_group_schema*/, true /*need_generate_cs_replica_cg_array*/, &param))) {
    LOG_WARN("fail to init full storage schema", K(ret), K(simplified_schema));
  }

  if (OB_FAIL(ret)) {
    ObTabletObjLoadHelper::free(allocator, schema);
  } else if (OB_UNLIKELY(!schema->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid schema", K(ret), KPC(schema));
    ObTabletObjLoadHelper::free(allocator, schema);
  } else {
    full_storage_schema = schema;
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