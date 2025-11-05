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
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_direct_load_mgr_v3.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/column_store/ob_column_store_replica_util.h"
namespace oceanbase
{
namespace storage
{

/* TODO @ zhuoran.zzr
 *        struct direct load mgr won't be used any more, wait to remove it
*/
int ObDirectLoadMgrUtil::check_cs_replica_exist(const ObLSID &ls_id, const ObTabletID &tablet_id, bool &is_cs_replica_exist)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTabletHandle tablet_handle;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log service should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
     LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else if (CS_REPLICA_VISIBLE_AND_REPLAY_COLUMN == tablet_handle.get_obj()->get_tablet_meta().ddl_replay_status_ ||
             CS_REPLICA_VISIBLE_AND_REPLAY_ROW    == tablet_handle.get_obj()->get_tablet_meta().ddl_replay_status_) {
    is_cs_replica_exist = true;
  }
  return ret;
}

int ObDirectLoadMgrUtil::get_lob_tablet_id(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletID &lob_tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletBindingMdsUserData ddl_data;

  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(ls_id), K(tablet_id));
  } else {
    lob_tablet_id = ddl_data.lob_piece_tablet_id_;
  }
  return ret;
}

/* use to check ddl need to do major merge,
*  notice don't use it to judge whether major exist
*/
int ObDirectLoadMgrUtil::is_ddl_need_major_merge(const ObTablet &tablet, bool &ddl_need_merging)
{
  int ret = OB_SUCCESS;
  ddl_need_merging = false;
  ObTableStoreIterator ddl_iter;
  ObTabletDDLCompleteMdsUserData ddl_complete;
  if (!tablet.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (tablet.get_major_table_count() > 0) { /*check major exist */
    ddl_need_merging = false;
  } else if (OB_FAIL(tablet.get_ddl_sstables(ddl_iter))) {
    LOG_WARN("failed to get ddl sstable", K(ret));
  } else if (ddl_iter.is_valid()) { // indicates the existence of ddl sstable
    ddl_need_merging = true;
    LOG_WARN("major sstable do not exit, need to wait ddl merge", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_);
  } else if (OB_FAIL(tablet.get_ddl_complete(SCN::max_scn(), ddl_complete))) {
    LOG_WARN("failed to check ddl complete", K(ret), K(tablet.get_tablet_meta()));
  } else if (ddl_complete.has_complete_) {
    ddl_need_merging = true;
    LOG_WARN("major sstable do not exit, need to wait ddl merge", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_);
  }
  return ret;
}


int ObDirectLoadMgrUtil::check_tablet_major_exist(const ObLSID &ls_id, const ObTabletID &tablet_id,  bool &is_major_sstable_exist)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(ls_id), K(tablet_id));
  } else {
    is_major_sstable_exist = nullptr != table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/);
  }
  return ret;
}

int ObDirectLoadMgrUtil::alloc_direct_load_mgr(
    ObIAllocator &allocator,
    const ObDirectLoadType &direct_load_type,
    ObBaseTabletDirectLoadMgr *&direct_load_mgr)
{
  int ret = OB_SUCCESS;
  direct_load_mgr = nullptr;
  switch (direct_load_type) {
    case ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL :
      if (OB_ISNULL(direct_load_mgr = OB_NEWx(ObSNTabletDirectLoadMgr, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc direct load mgr", K(ret), K(direct_load_type));
      }
      break;
    case ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DATA :
      if (OB_ISNULL(direct_load_mgr = OB_NEWx(ObSNTabletDirectLoadMgr, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc direct load mgr", K(ret), K(direct_load_type));
      }
      break;
    case ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DDL :
      if (OB_ISNULL(direct_load_mgr = OB_NEWx(ObSSTabletDirectLoadMgr, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      break;
    case ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DATA :
      if (OB_ISNULL(direct_load_mgr = OB_NEWx(ObSSTabletDirectLoadMgr, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported direct load type", K(ret), K(direct_load_type));
      break;
  }
  return ret;
}


/*
 * create a tablet direct load mgr in handle， which should be a temp param
 * !!!note don't return eagin
 * otherwise, direct load mgr may lead to px retry again and again
*/
int ObDirectLoadMgrUtil::create_idem_tablet_direct_load_mgr(const uint64_t tenant_id,
                                                            const int64_t execution_id,
                                                            ObIAllocator &allocator,
                                                            const ObTabletDirectLoadInsertParam &build_param,
                                                            bool &is_major_sstable_exist,
                                                            ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
                                                            ObTabletDirectLoadMgrHandle &lob_direct_load_mgr_handle)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = build_param.common_param_.ls_id_;
  const ObTabletID &tablet_id = build_param.common_param_.tablet_id_;
  ObTabletID lob_tablet_id;
  /* reset param*/
  is_major_sstable_exist = false;
  direct_load_mgr_handle.reset();
  lob_direct_load_mgr_handle.reset();
  /* get lob tablet_id*/
  if (!build_param.is_valid() || 0 >= tenant_id || 0 > execution_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(build_param), K(tenant_id), K(execution_id));
  } else if (OB_FAIL(get_lob_tablet_id(build_param.common_param_.ls_id_, build_param.common_param_.tablet_id_, lob_tablet_id))) {
    LOG_WARN("failed to get lob_tablet_id", K(ret), K(build_param.common_param_));
  } else if (OB_FAIL(check_tablet_major_exist(ls_id, tablet_id, is_major_sstable_exist))) {
    LOG_WARN("failed to check major sstable exist", K(ret), K(ls_id), K(tablet_id));
  } else  {
    ObBaseTabletDirectLoadMgr* direct_load_mgr = nullptr;
    ObBaseTabletDirectLoadMgr* lob_direct_load_mgr = nullptr;
    /* prepare lob direct load mgr*/
    ObTabletDirectLoadInsertParam lob_direct_load_param;
    if (OB_FAIL(ObTabletDirectLoadMgrV3::prepare_lob_param(build_param, lob_direct_load_param))) {
      LOG_WARN("fail to prepare lob direct load mgr", K(ret), K(build_param));
    } else if (!lob_direct_load_param.common_param_.tablet_id_.is_valid()) {
      FLOG_INFO("invalid lob tablet id, do not need to create lob tablet direct load mgr", K(ret), K(build_param));
    } else if (OB_FAIL(alloc_direct_load_mgr(allocator, lob_direct_load_param.common_param_.direct_load_type_, lob_direct_load_mgr))) {
      LOG_WARN("fail to allocate direct load mgr", K(ret), K(lob_direct_load_param.common_param_.direct_load_type_));
    } else if (OB_FAIL(lob_direct_load_mgr->init_v2(lob_direct_load_param, execution_id, ObDirectLoadMgrRole::LOB_TABLET_TYPE))) {
      LOG_WARN("fail to init lob direct load mgr", K(ret));
    } else if (FALSE_IT(lob_direct_load_mgr_handle.set_obj(lob_direct_load_mgr))) {
    } else {
      FLOG_INFO("[DIRECT LOAD MGR UTILS] success to create direct load mgr", K(ret), K(lob_direct_load_param.common_param_.tablet_id_));
    }

    /* prepare direct load mgr for main table */
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alloc_direct_load_mgr(allocator, build_param.common_param_.direct_load_type_, direct_load_mgr))) {
      LOG_WARN("fail to allocate direct load mgr", K(ret));
    } else if (OB_FAIL(direct_load_mgr->init_v2(build_param, execution_id, ObDirectLoadMgrRole::DATA_TABLET_TYPE))) {
      LOG_WARN("fail to init direct load mgr", K(ret), K(build_param), K(execution_id));
    } else if (FALSE_IT(direct_load_mgr_handle.set_obj(direct_load_mgr))) {
    } else {
      FLOG_INFO("[DIRECT LOAD MGR UTILS] success to create direct load mgr", K(ret), K(build_param.common_param_.tablet_id_));
    }
  }
  return ret;
}

// TODO @zhuoran.zzr: is_column_store is set becacuese column store is not supported yet, wait to remove it later
ObDirectLoadType ObDirectLoadMgrUtil::ddl_get_direct_load_type(const bool is_shared_storage_mode, const uint64_t data_format_version)
{
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INVALID;
  if (is_shared_storage_mode) {
    if (data_format_version < DDL_IDEM_DATA_FORMAT_VERSION) {
      direct_load_type =  ObDirectLoadType::DIRECT_LOAD_DDL_V2;
    } else {
      direct_load_type = ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DDL;
    }
  } else {
    if (data_format_version < DDL_IDEM_DATA_FORMAT_VERSION) {
      direct_load_type = ObDirectLoadType::DIRECT_LOAD_DDL;
    } else {
      direct_load_type = ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL;
    }
  }
  return direct_load_type;
}

ObDirectLoadType ObDirectLoadMgrUtil::load_data_get_direct_load_type(const bool is_incremental,
                                                                     const uint64_t data_format_version,
                                                                     const bool is_shared_storage_mode,
                                                                     const bool is_inc_major)
{
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INVALID;
  if (is_incremental)  {
    /* not supported yet, wait to replace with new type */
    direct_load_type = is_inc_major ? ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR
                                     : ObDirectLoadType::DIRECT_LOAD_INCREMENTAL;
  } else {
    if (!is_shared_storage_mode) {
      if (data_format_version >= DDL_IDEM_DATA_FORMAT_VERSION) {
        direct_load_type = ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DATA;
      } else {
        direct_load_type = ObDirectLoadType::DIRECT_LOAD_LOAD_DATA;
      }
    } else {
      if (data_format_version < DDL_IDEM_DATA_FORMAT_VERSION) {
        direct_load_type =  ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2;
      } else {
        direct_load_type = ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DATA;
      }
    }
  }
  return direct_load_type;
}

/*
 * tablet direct load mgr craetor, used for both previous and idme direct_load_mgr
 *    - for previous version, mgr handle should get from tenant direct load mgr
 *    - for idem version,     mgr handle are local param, get from the return param
*/
int ObDirectLoadMgrUtil::create_tablet_direct_load_mgr(const int64_t tenant_id,
                                                       const int64_t execution_id,
                                                       const int64_t context_id,
                                                       const ObTabletDirectLoadInsertParam &build_param,
                                                       ObIAllocator &allocator,
                                                       bool &is_major_eixst,
                                                       ObTabletDirectLoadMgrHandle &data_mgr_handle,
                                                       ObTabletDirectLoadMgrHandle &lob_mgr_handle)
{
  int ret = OB_SUCCESS;
  is_major_eixst = false;
  data_mgr_handle.reset();
  lob_mgr_handle.reset();
  if (tenant_id <= 0 || execution_id < 0 || !build_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(execution_id), K(build_param));
  } else if (!is_idem_type(build_param.common_param_.direct_load_type_)) {
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->create_tablet_direct_load(context_id, execution_id, build_param))) {
      LOG_WARN("create tablet manager failed", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(ls_service->get_ls(build_param.common_param_.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(build_param));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, build_param.common_param_.tablet_id_, tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      LOG_WARN("get tablet handle failed", K(ret), K(build_param));
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
    } else if (tablet_handle.get_obj()->get_tablet_meta().ddl_data_format_version_ >= DDL_IDEM_DATA_FORMAT_VERSION && is_full_direct_load(build_param.common_param_.direct_load_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid version", K(ret), K(tablet_handle.get_obj()->get_tablet_meta()), K(build_param));
    }
  } else if (is_idem_type(build_param.common_param_.direct_load_type_)) {
    if (OB_FAIL(ObDirectLoadMgrUtil::create_idem_tablet_direct_load_mgr(tenant_id, execution_id, allocator, build_param, is_major_eixst, data_mgr_handle, lob_mgr_handle))) {
      LOG_WARN("failed to create tablet direct load mgr", K(ret), K(tenant_id), K(execution_id), K(build_param));
    }
  }
  return ret;
}
int ObDirectLoadMgrUtil::generate_merge_param(const ObTabletDDLCompleteArg &arg, ObDDLTableMergeDagParam &merge_param)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    merge_param.direct_load_type_ = arg.direct_load_type_;
    merge_param.ls_id_ = arg.ls_id_;
    merge_param.tablet_id_ = arg.tablet_id_;
    merge_param.data_format_version_ = arg.data_format_version_;
    merge_param.snapshot_version_ = arg.snapshot_version_;
    merge_param.start_scn_ = arg.start_scn_;
    merge_param.table_key_ = arg.table_key_;
    merge_param.is_commit_ = true;
  }
  return ret;
}

int ObDirectLoadMgrUtil::generate_merge_param(const ObTabletDDLCompleteMdsUserData &data, ObTablet &tablet, ObDDLTableMergeDagParam &merge_param)
{
  int ret = OB_SUCCESS;
  share::SCN mock_scn;
  mock_scn.convert_for_tx(SS_DDL_START_SCN_VAL);

  if (!data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data));
  } else if (data.has_complete_) {  /* generate param for major merge */
    merge_param.direct_load_type_ = data.direct_load_type_;
    merge_param.ls_id_ = tablet.get_ls_id();
    merge_param.tablet_id_ = tablet.get_tablet_id();
    merge_param.data_format_version_ = data.data_format_version_;
    merge_param.snapshot_version_ =   data.snapshot_version_;
    merge_param.start_scn_ = mock_scn;
    merge_param.rec_scn_   = mock_scn;
    merge_param.table_key_ = data.table_key_;
    merge_param.is_commit_ = true;

    if (OB_FAIL(merge_param.user_data_.assign(data))) {
      LOG_WARN("failed to assgin value", K(ret));
    }
  } else {  /* generate param for freeze */
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    ObArray<ObDDLKVHandle> ddl_kvs_handle;
    if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle, false /*not create*/))) {
      LOG_WARN("failed to get ddl kv", K(ret));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(false/*frozen_only*/, ddl_kvs_handle))) {
      LOG_WARN("failed to get ddl kv handle", K(ret));
    } else if (OB_FAIL(ddl_kvs_handle.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ddl kv_mgr_handle", K(ret));
    } else {
      merge_param.direct_load_type_    = ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL; // mock type
      merge_param.ls_id_               = tablet.get_ls_id();
      merge_param.tablet_id_           = tablet.get_tablet_id();
      merge_param.data_format_version_ = ddl_kvs_handle.at(0).get_obj()->get_data_format_version();
      merge_param.snapshot_version_    = ddl_kvs_handle.at(0).get_obj()->get_snapshot_version();
      merge_param.start_scn_           = mock_scn;
      merge_param.is_commit_           = false;
      int64_t base_cg_idx = 0;
      ObArenaAllocator tmp_allocator;
      ObStorageSchema* storage_schema = nullptr;
      if (OB_FAIL(tablet.load_storage_schema(tmp_allocator, storage_schema))) {
        LOG_WARN("failed to get tablet handle", K(ret));
      } else if (OB_ISNULL(storage_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage schema is null", K(ret));
      } else if (OB_FAIL(ObCODDLUtil::get_base_cg_idx(storage_schema, base_cg_idx))) {
        LOG_WARN("failed to get base cg idx", K(ret), K(storage_schema));
      } else {
        merge_param.table_key_.table_type_ = ObITable::TableType::DDL_DUMP_SSTABLE;
        merge_param.table_key_.tablet_id_ = tablet.get_tablet_id();
        merge_param.table_key_.scn_range_.start_scn_ = SCN::scn_dec(mock_scn);
        merge_param.table_key_.scn_range_.end_scn_ = mock_scn;
        merge_param.table_key_.version_range_.snapshot_version_ = ddl_kvs_handle.at(0).get_obj()->get_snapshot_version();
        merge_param.table_key_.column_group_idx_ = base_cg_idx;
      }
    }
  }
  return ret;
}

int ObDirectLoadMgrUtil::prepare_schema_item_for_vec_idx_data(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *table_schema,
    const ObTableSchema *&data_table_schema,
    ObIAllocator &allocator,
    ObTableSchemaItem &schema_item)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t , 1> col_ids;
  uint64_t with_param_table_tid;
  // for hnsw, table_schema here is snapshot table, need to get related delta buffer table.
  ObIndexType index_type = INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL;

  // ivf param is saved in centroid table's schema
  if (table_schema->is_vec_ivfflat_index()) {
    index_type = INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL;
  } else if (table_schema->is_vec_ivfsq8_index()) {
    index_type = INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL;
  } else if (table_schema->is_vec_ivfpq_index()) {
    index_type = INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL;
  }
  const ObTableSchema *with_param_table_schema = nullptr;
  // get data schema
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_schema->get_data_table_id()));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(table_schema->get_data_table_id()));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_id(*data_table_schema, *table_schema, col_ids))) {
    LOG_WARN("fail to get vector index id", K(ret));
  } else if (col_ids.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid col id array", K(ret), K(col_ids));
  } else {
    if (index_type == INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL) {
      ObString index_prefix;
      if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*table_schema, index_prefix))) {
        LOG_WARN("failed to get index prefix", K(ret));
      } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid_with_index_prefix(&schema_guard,
                                                                                   *data_table_schema,
                                                                                   index_type,
                                                                                   col_ids.at(0),
                                                                                   index_prefix,
                                                                                   with_param_table_tid))) {
        LOG_WARN("failed to get index prefix", K(ret), K(index_prefix));
      }
    } else { // ivf centroid tables
      if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid(&schema_guard,
                                                          *data_table_schema,
                                                          index_type,
                                                          col_ids.at(0),
                                                          with_param_table_tid))) {
        LOG_WARN("fail to get spec vector delta buffer table id", K(ret), K(col_ids), KPC(data_table_schema));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, with_param_table_tid, with_param_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(with_param_table_tid));
  } else if (OB_ISNULL(with_param_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(with_param_table_tid));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*with_param_table_schema, *data_table_schema, schema_item.vec_dim_))) {
    LOG_WARN("fail to get vector col dim", K(ret));
  } else if (schema_item.vec_dim_ == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vector dim is zero, fail to calc", K(ret), K(schema_item.vec_dim_), KPC(with_param_table_schema));
  } else if (OB_FAIL(ob_write_string(allocator, with_param_table_schema->get_index_params(), schema_item.vec_idx_param_))) {
    LOG_WARN("fail to write string", K(ret), K(with_param_table_schema->get_index_params()));
  }
  return ret;
}

int ObDirectLoadMgrUtil::get_tablet_handle(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService * ls_service = nullptr;
  tablet_handle.reset();
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
     LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  }
  return ret;
}
} //namespace staroge
} //namespace oceanbase
