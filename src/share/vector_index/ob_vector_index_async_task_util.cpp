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

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_tenant_vector_index_async_task.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_hybrid_vector_refresh_task.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/table/ob_ttl_util.h"
#include "share/ob_common_id.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_value_row_iterator.h"
#include "share/vector_index/ob_ivf_async_task.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace share
{
ObVecIndexAsyncTaskCtx::~ObVecIndexAsyncTaskCtx()
{
  if (OB_NOT_NULL(extra_data_)) {
    ObIvfAuxTableInfo *aux_table = static_cast<ObIvfAuxTableInfo *>(extra_data_);
    aux_table->~ObIvfAuxTableInfo();
    allocator_.free(aux_table);
    extra_data_ = nullptr;
  }
  allocator_.reset();
}

int ObAsyncTaskMapFunc::operator()(const hash::HashMapPair<common::ObTabletID, ObVecIndexAsyncTaskCtx*> &entry)
{
  int ret = OB_SUCCESS;
  ObVecIndexAsyncTaskCtx* task_ctx(entry.second);
  if (OB_FAIL(array_.push_back(task_ctx))) {
    LOG_WARN("failed to push back task ctx", K(ret), K(array_), K(task_ctx));
  }
  return ret;
}

ObVecIndexAsyncTaskOption::~ObVecIndexAsyncTaskOption()
{
  destroy();
}

int ObVecIndexAsyncTaskOption::init(const int64_t capacity, const int64_t tenant_id, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_ctx_map_.create(capacity, mem_attr_.label_, mem_attr_.label_, tenant_id))) {
    LOG_WARN("fail to create vector index task ctx map", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

void ObVecIndexAsyncTaskOption::destroy()
{
  int ret = OB_SUCCESS;
  FOREACH(iter, task_ctx_map_) {
    const ObTabletID &tablet_id = iter->first;
    ObVecIndexAsyncTaskCtx *&task_ctx = iter->second;
    LOG_DEBUG("dump task_ctx_map_ info", K(tablet_id), KP(task_ctx));
    if (OB_NOT_NULL(task_ctx)) {
      allocator_.free(task_ctx);
      task_ctx = nullptr;
    }
  }
  task_ctx_map_.destroy();
  allocator_.reset();
}

int ObVecIndexAsyncTaskOption::add_task_ctx(ObTabletID &tablet_id, ObVecIndexAsyncTaskCtx *task, bool &inc_new_task)
{
  int ret = OB_SUCCESS;
  inc_new_task = false;
  if (OB_ISNULL(task) || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), KPC(task));
  } else if (OB_FAIL(task_ctx_map_.set_refactored(tablet_id, task))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to set_refactored", K(ret), K(tablet_id));
    }
  } else {
    inc_new_task = true;
  }
  return ret;
}

int ObVecIndexAsyncTaskOption::del_task_ctx(ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_ctx_map_.erase_refactored(tablet_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to delete task ctx from map", KR(ret), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/////////////////////////////
// ObVecIndexAsyncTaskUtil //
////////////////////////////
int ObVecIndexAsyncTaskOption::is_task_ctx_exist(ObTabletID &tablet_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObVecIndexAsyncTaskCtx *tmp_ctx = nullptr;
  is_exist = false;
  if (OB_FAIL(task_ctx_map_.get_refactored(tablet_id, tmp_ctx))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to delete task ctx from map", KR(ret), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    is_exist = true;
  }
  return ret;
}

void ObVecIndexAsyncTaskUtil::get_row_need_skip_for_compatibility(blocksstable::ObDatumRow &row, const bool is_need_unvisible_row, bool &skip_this_row)
{
  bool visible = true;
  if (row.get_column_count() == 2) { // old table 5 version
    skip_this_row = false;
  } else if (row.get_column_count() == 3) { // new table 5 version (has visible row)
    visible = row.storage_datums_[2].get_bool();
    skip_this_row = is_need_unvisible_row ? visible : !visible;
  }
  LOG_INFO("get_row_need_skip_for_compatibility", K(row), K(is_need_unvisible_row), K(visible), K(skip_this_row));
}

int ObVecIndexAsyncTaskUtil::set_inner_sql_ret_code(const int64_t task_id, int ret_code)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl slice info", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    tmp_info->ret_code_ = ret_code;
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::get_inner_sql_ret_code(const int64_t task_id, int &ret_code)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl slice info", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    ret_code = tmp_info->ret_code_;
  }
  return ret;
}



int ObVecIndexAsyncTaskUtil::set_inner_sql_adapter(const int64_t task_id, ObPluginVectorIndexAdaptor *adapter)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (OB_ISNULL(adapter) || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl slice info", K(ret), KP(adapter), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    tmp_info->adapter_ = adapter;
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::get_inner_sql_adapter(const int64_t task_id, ObPluginVectorIndexAdaptor *&adapter)
{
  int ret = OB_SUCCESS;
  adapter = nullptr;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl slice info", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    adapter = tmp_info->adapter_;
  }
  return ret;
}

// deep copy
int ObVecIndexAsyncTaskUtil::set_inner_sql_slice_info(const int64_t task_id, rootserver::ObDDLSliceInfo &ddl_slice_info)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (!ddl_slice_info.is_valid() || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl slice info", K(ret), K(ddl_slice_info), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else if (OB_FAIL(tmp_info->ddl_slice_info_.deep_copy(ddl_slice_info, tmp_info->allocator_))) {
    LOG_WARN("fail to copy ddl slice info", K(ret), K(ddl_slice_info));
  }
  return ret;
}

// deep copy
int ObVecIndexAsyncTaskUtil::get_inner_sql_slice_info(const int64_t task_id, ObIAllocator &allocator, rootserver::ObDDLSliceInfo &ddl_slice_info)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (OB_ISNULL(vector_index_service) || task_id <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id));
  } else if (OB_FAIL(ddl_slice_info.deep_copy(tmp_info->ddl_slice_info_, allocator))) {
    LOG_WARN("fail to copy ddl slice info", K(ret), K(tmp_info->ddl_slice_info_));
  }
  return ret;
}

// deep copy
int ObVecIndexAsyncTaskUtil::set_inner_sql_schema_version(const int64_t task_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (schema_version <= 0 || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected schema version", K(ret), K(schema_version), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    tmp_info->schema_version_ = schema_version;
  }
  return ret;
}

// deep copy
int ObVecIndexAsyncTaskUtil::set_inner_sql_snapshot_version(const int64_t task_id, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (snapshot_version <= 0 || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected snapshot version", K(ret), K(snapshot_version), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    tmp_info->snapshot_version_ = snapshot_version;
  }
  return ret;
}


// deep copy
int ObVecIndexAsyncTaskUtil::get_inner_sql_schema_version(const int64_t task_id, int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (task_id <= 0 || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected schema version", K(ret), K(schema_version), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    schema_version = tmp_info->schema_version_;
  }
  return ret;
}

// deep copy
int ObVecIndexAsyncTaskUtil::get_inner_sql_snapshot_version(const int64_t task_id, int64_t &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (task_id <= 0 || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected snapshot version", K(ret), K(task_id), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(task_id, tmp_info))) {
    LOG_WARN("fail to get vector index tmp info", K(task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    snapshot_version = tmp_info->snapshot_version_;
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::init_tablet_rebuild_new_adapter(ObPluginVectorIndexAdaptor *new_adapter, const ObString &row_key)
{
  int ret = OB_SUCCESS;

  ObVectorIndexAlgorithmType index_type;
  int64_t key_prefix_scn;
  ObString target_prefix;
  ObString key_prefix;
  ObArenaAllocator allocator;

  if (OB_ISNULL(new_adapter) || row_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_key), KP(new_adapter));
  } else if (OB_FAIL(ob_write_string(allocator, row_key, key_prefix))) {
    LOG_WARN("failed to write string", K(ret), K(row_key));
  } else if (OB_FALSE_IT(index_type = new_adapter->get_snap_index_type())) {
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_split_snapshot_prefix(index_type, key_prefix, target_prefix))) {
    LOG_WARN("fail to get split snapshot prefix", K(ret), K(index_type), K(key_prefix));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_key_prefix_scn(key_prefix, key_prefix_scn))) {
    LOG_WARN("fail to get key prefix scn", K(ret), K(key_prefix));
  } else if (OB_FAIL(new_adapter->set_snapshot_key_prefix(target_prefix))) {
    LOG_WARN("failed to set snapshot key prefix", K(ret), K(index_type), K(target_prefix));
  } else if (OB_FAIL(new_adapter->set_snapshot_key_scn(key_prefix_scn))) {
    LOG_WARN("fail to set snapshot key scn", K(ret), K(key_prefix_scn));
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::check_task_is_cancel(ObVecIndexAsyncTaskCtx *task_ctx, bool &is_cancel)
{
  int ret = OB_SUCCESS;
  is_cancel = false;
  if (OB_NOT_NULL(task_ctx)) {
    if (task_ctx->sys_task_id_.is_valid()) {
      if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(task_ctx->sys_task_id_, is_cancel))) {
        LOG_WARN("failed to check task is cancel", K(ret), K(task_ctx->sys_task_id_));
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::insert_new_task(uint64_t tenant_id, ObVecIndexTaskCtxArray &task_ctx_array)
{
  int ret = OB_SUCCESS;
  if (task_ctx_array.count() <= 0) {  // skip empty array
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
      LOG_WARN("fail start transaction", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::batch_insert_vec_task(
      tenant_id, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, task_ctx_array))) {
      LOG_WARN("fail to insert vec tasks", K(ret), K(tenant_id));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::in_active_time(
    const uint64_t tenant_id, bool& is_active_time)
{
  int ret = OB_SUCCESS;
  is_active_time = false;
  ObTTLDutyDuration duration;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));

  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTTLUtil::parse(tenant_config->vector_index_optimize_duty_time, duration))) {
    LOG_WARN("fail parse vector time duty time", KR(ret));
  } else if (ObTTLUtil::current_in_duration(duration)) {
    is_active_time = true;
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::fetch_new_task_id(const uint64_t tenant_id, int64_t &new_task_id)
{
  int ret = OB_SUCCESS;
  ObCommonID tmp_task_id;
  MTL_SWITCH(tenant_id) {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id, tmp_task_id))) {
      LOG_WARN("failed to gen unique id", K(ret));
    } else {
      new_task_id = tmp_task_id.id();
    }
  } else {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id, tmp_task_id))) {
      LOG_WARN("failed to gen unique id", K(ret));
    } else {
      new_task_id = tmp_task_id.id();
    }
  }
  return ret;
}

int64_t ObVecIndexAsyncTaskUtil::get_processing_task_cnt(ObVecIndexAsyncTaskOption &task_opt)
{
  int ret = OB_SUCCESS;
  int64_t processing_task_cnt = 0;
  FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret)) {
    ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
    if (OB_NOT_NULL(task_ctx)) {
      if (task_ctx->in_thread_pool_) {
        processing_task_cnt++;
        LOG_DEBUG("current running task", K(processing_task_cnt), KPC(task_ctx));
      }
    }
  }
  return processing_task_cnt;
}

// table 3, 4, 5 refactor the same adapter, here only need to get table 3 table_id
int ObVecIndexAsyncTaskUtil::get_table_id_from_adapter(
    ObPluginVectorIndexAdaptor *adapter, const ObTabletID &tablet_id, int64_t &table_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  if (OB_ISNULL(adapter) || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(adapter), K(tablet_id));
  } else if (adapter->is_inc_tablet_valid() && tablet_id == adapter->get_inc_tablet_id()) {
    table_id = adapter->get_inc_table_id();
  } else if (adapter->is_snap_tablet_valid() && tablet_id == adapter->get_snap_tablet_id()) {
  } else if (adapter->is_vbitmap_tablet_valid() && tablet_id == adapter->get_vbitmap_tablet_id()) {
  } else if (adapter->is_embedded_tablet_valid() && tablet_id == adapter->get_embedded_tablet_id()) {
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table id from adapter", K(ret), K(tablet_id));
  }
  return ret;
}

// TODO@xiajin: change data_version
bool ObVecIndexAsyncTaskUtil::check_can_do_work()
{
  bool bret = true;
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool is_oracle_mode = false;
  int64_t tenant_id = MTL_ID();
  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", K(ret), K(tenant_id));
  } else if (is_oracle_mode) {
    bret = false;
    LOG_DEBUG("vector index not support oracle mode", K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_3_5_2) {
    bret = false;
    LOG_DEBUG("vector index can not work with data version less than 4_3_3", K(tenant_data_version));
  }
  return bret;
}

int ObVecIndexAsyncTaskUtil::clear_history_expire_task_record(
    const uint64_t tenant_id,
    const int64_t batch_size,
    common::ObMySQLTransaction& proxy,
    int64_t &clear_rows)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  int64_t delete_timestamp = now - ObVectorIndexHistoryTask::OB_VEC_INDEX_TASK_HISTORY_SAVE_TIME_US;
  clear_rows = 0;
  ObSqlString sql;
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.3.5.2 is not support");
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE gmt_modified < usec_to_time(%ld) LIMIT %ld ",
                              share::OB_ALL_VECTOR_INDEX_TASK_HISTORY_TNAME,
                              delete_timestamp,
                              batch_size))) {
    LOG_WARN("fail to assign fmt sql string", KR(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), clear_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else {
    LOG_DEBUG("success to clear_history_expire_task_record", KR(ret), K(sql));
  }
  return ret;
}

// move finish task to history table no matter task is succ or failed
int ObVecIndexAsyncTaskUtil::move_task_to_history_table(
    const uint64_t tenant_id,
    const int64_t batch_size,
    common::ObMySQLTransaction& proxy,
    int64_t &move_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t insert_rows = 0;
  int64_t delete_rows = 0;
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.3.5.2 is not support");
  } else if (OB_FAIL(sql.assign_fmt("REPLACE INTO %s SELECT gmt_create, gmt_modified, tenant_id, table_id, tablet_id, task_id, trigger_type, task_type, status, target_scn, ret_code, trace_id FROM %s WHERE tenant_id = %ld AND status = 3 ORDER BY gmt_create LIMIT %ld",
              share::OB_ALL_VECTOR_INDEX_TASK_HISTORY_TNAME,
              share::OB_ALL_VECTOR_INDEX_TASK_TNAME,
              ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
              batch_size))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), insert_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s"
          " WHERE tenant_id = %ld AND status = 3 AND gmt_create <= (SELECT gmt_create FROM %s ORDER BY gmt_create desc LIMIT 1)",
          share::OB_ALL_VECTOR_INDEX_TASK_TNAME,
          ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
          share::OB_ALL_VECTOR_INDEX_TASK_HISTORY_TNAME))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), delete_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  } else {
    move_rows = delete_rows;
    LOG_DEBUG("batch move task to history table", K(ret), K(tenant_id), K(sql), K(insert_rows), K(delete_rows));
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::get_insert_task_ctx_array(
    ObVecIndexTaskCtxArray &in_task,
    ObVecIndexTaskCtxArray &out_task,
    common::hash::ObHashSet<uint64_t> &duplicate_tablet_task)
{
  int ret = OB_SUCCESS;
  if (duplicate_tablet_task.size() == 0) {
    if (OB_FAIL(out_task.assign(in_task))) {
      LOG_WARN("fail to assign array", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < in_task.count(); ++i) {
      ObVecIndexAsyncTaskCtx *task_ctx = in_task.at(i);
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), KP(task_ctx));
      } else {
        uint64_t tablet_id = task_ctx->task_status_.tablet_id_.id();
        if (OB_HASH_EXIST == duplicate_tablet_task.exist_refactored(tablet_id)) {
          // skip
          LOG_INFO("skip exist tablet id task", K(ret), K(tablet_id));
        } else if (OB_FAIL(out_task.push_back(task_ctx))) {
          LOG_WARN("fail to push back task", K(ret), K(i), K(task_ctx));
        }
      }
    }
  }
  LOG_DEBUG("get_insert_task_ctx_array", K(ret),
    K(in_task.count()), K(out_task.count()), K(duplicate_tablet_task.size()));
  return ret;
}

/* 分区拆分的情况下，这里的where条件需要扩展
*/
int ObVecIndexAsyncTaskUtil::get_duplicate_tablet_vec_task(
    uint64_t tenant_id,
    const char* tname,
    common::ObISQLClient& proxy,
    common::hash::ObHashSet<uint64_t> &duplicate_tablet_task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(" SELECT tablet_id FROM %s WHERE status != 3 ", tname))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult* result = nullptr;
      if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next row", K(ret));
            }
          } else {
            uint64_t tablet_id = 0;
            EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", tablet_id, uint64_t);
            if (OB_FAIL(duplicate_tablet_task.set_refactored(tablet_id, 0/*not cover exists object*/))) {
              LOG_WARN("put into set failed", K(ret), K(tablet_id));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  LOG_DEBUG("get_duplicate_tablet_vec_task", K(ret), K(duplicate_tablet_task.size()));
  return ret;
}

// batch insert (default 10)
int ObVecIndexAsyncTaskUtil::batch_insert_vec_task(
    uint64_t tenant_id,
    const char* tname,
    common::ObISQLClient& proxy,
    ObVecIndexTaskCtxArray &task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affect_rows = 0;
  int64_t batch_size = DEFAULT_VEC_INSERT_BATCH_SIZE;

  if (task.size() <= batch_size) {
    if (OB_FAIL(insert_vec_tasks(tenant_id, tname, task.size(), proxy, task))) {
      LOG_WARN("fail to insert vec tasks", K(ret));
    }
  } else {
    ObVecIndexTaskCtxArray tmp_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < task.size(); ++i) {
      // copy task to new array
      if (OB_FAIL(tmp_array.push_back(task.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(i));
      } else if (tmp_array.count() % batch_size == 0 || i == task.size() - 1) {
        if (OB_FAIL(insert_vec_tasks(tenant_id, tname, tmp_array.size(), proxy, tmp_array))) {
          LOG_WARN("fail to insert vec tasks", K(ret), K(tmp_array.size()));
        } else {
          tmp_array.reuse();
        }
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::insert_vec_tasks(
    uint64_t tenant_id,
    const char* tname,
    const int64_t batch_size,
    common::ObISQLClient& proxy,
    ObVecIndexTaskCtxArray& task_ctx_array)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affect_rows = 0;
  int64_t real_batch_size = 0;
  uint64_t data_version = 0;

  if (batch_size <= 0 || batch_size > task_ctx_array.size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(batch_size));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.3.5.2 is not support");
  } else if (OB_FAIL(sql.assign_fmt(" INSERT INTO %s"
                                    " (tenant_id, table_id, tablet_id,"
                                    " task_id, trigger_type, task_type, status, target_scn,"
                                    " ret_code, trace_id) VALUES",
                                    tname))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else {
    // values
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      ObVecIndexAsyncTaskCtx *task_ctx = task_ctx_array.at(i);
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr of task ctx", K(ret));
      } else {
        ObVecIndexTaskStatus &task = task_ctx->task_status_;
        char trace_id_str[256] = { 0 };
        task.trace_id_.to_string(trace_id_str, sizeof(trace_id_str));
        if (OB_FAIL(sql.append_fmt(" (%ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, '%s')",
                                  ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                  task.table_id_, task.tablet_id_.id(),
                                  task.task_id_, task.trigger_type_, task.task_type_,
                                  task.status_, task.target_scn_.get_val_for_sql(), task.ret_code_,
                                  trace_id_str))) {
          LOG_WARN("fail to assign fmt", K(ret));
        } else if ((i != batch_size - 1) && OB_FAIL(sql.append_fmt(","))) {
          LOG_WARN("fail to assign fmt", K(ret), K(i));
        }
      }
    }
  }
  LOG_INFO("insert vec tasks sql", K(sql.ptr()));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (affect_rows != batch_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("execute sql, affect rows != batch_size", K(ret), K(affect_rows), K(batch_size));
  } else {
    LOG_INFO("success to insert_vec_tasks", K(ret), K(tenant_id), K(sql));
  }

  return ret;
}

int ObVecIndexAsyncTaskUtil::update_vec_task(
    uint64_t tenant_id,
    const char* tname,
    common::ObISQLClient& proxy,
    ObVecIndexTaskKey& key,
    ObVecIndexFieldArray& update_fields,
    ObVecIndexTaskProgressInfo &progress_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.3.5.2 is not support");
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET ", tname))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  }
  for (size_t i = 0; OB_SUCC(ret) && i < update_fields.count(); ++i) {
    ObVecIndexTaskStatusField& field = update_fields.at(i);
    if (OB_FAIL(sql.append_fmt("%s =", field.field_name_.ptr()))) {
      LOG_WARN("sql assign fmt failed", K(ret));
    } else if (field.type_ == ObVecIndexTaskStatusField::INT_TYPE) {
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.int_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObVecIndexTaskStatusField::UINT_TYPE) {
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.uint_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObVecIndexTaskStatusField::STRING_TYPE) {
      if (OB_FAIL(sql.append_fmt("%s", field.data_.str_.ptr()))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql append fmt failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt("%s", i == update_fields.count() - 1 ? " " : ","))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    // do nothing
  } else if (progress_info.vec_opt_status_ == OB_VECTOR_ASYNC_OPT_STATUS_MAX) {
    if (OB_FAIL(sql.append_fmt(",progress_info = NULL"))) {
      LOG_WARN("failed to fill statistics", K(ret));
    }
  } else {
    char progress_info_buf[OB_MAX_ERROR_MSG_LEN] ={0};
    int64_t pos = 0;
    switch(progress_info.vec_opt_status_) {
      case OB_VECTOR_ASYNC_OPT_PREPARE: {
        if (OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos, "{\"status\":\"preparing\"}"))) {
          LOG_WARN("failed to fill statistics", K(ret));
        }
        break;
      }
      case OB_VECTOR_ASYNC_OPT_INSERTING: {
        if (OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos, "{\"status\":\"inserting vectors\""))) {
          LOG_WARN("failed to fill statistics", K(ret));
        } else if (OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos,
                      ", \"estimated_row\":%ld, \"finished_row\":%ld", progress_info.opt_esitimate_row_cnt_, progress_info.opt_finished_row_cnt_))) {
          LOG_WARN("failed to fill statistics", K(ret));
        } else if (progress_info.progress_ < 1 && OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos,
                      ", \"progress\":\"%.2f%%\"", progress_info.progress_ * 100.0))) {
          LOG_WARN("failed to fill statistics", K(ret));
        } else if (progress_info.progress_ < 1 && progress_info.opt_finished_row_cnt_ > 0
                   && OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos,
                      ", \"time_remaining(s)\":%ld", progress_info.remain_time_ / 1000 / 1000))) {
          LOG_WARN("failed to fill statistics", K(ret));
        } else if (OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos, "}"))) {
          LOG_WARN("failed to fill statistics", K(ret));
        }
        break;
      }
      case OB_VECTOR_ASYNC_OPT_SERIALIZE: {
        if (OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos, "{\"status\":\"serializing snap index\"}"))) {
          LOG_WARN("failed to fill statistics", K(ret));
        }
        break;
      }
      case OB_VECTOR_ASYNC_OPT_REPLACE: {
        if (OB_FAIL(databuff_printf(progress_info_buf, OB_MAX_ERROR_MSG_LEN, pos, "{\"status\":\"replacing old index\"}"))) {
          LOG_WARN("failed to fill statistics", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", K(ret), K(progress_info.vec_opt_status_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(",progress_info = '%s'", progress_info_buf))) {
        LOG_WARN("failed to fill statistics", K(ret));
      }
    }
  }

  // WHERE FILTER
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt(" WHERE "
                    "tenant_id = %ld AND table_id = %ld AND tablet_id = %ld AND task_id = %ld",
                    ObSchemaUtils::get_extract_tenant_id(key.tenant_id_, key.tenant_id_),
                    key.table_id_, key.tablet_id_, key.task_id_))) {
    LOG_WARN("sql append fmt failed", K(ret));
  }

  int64_t affect_rows = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
    if (ret == OB_ERR_EXCLUSIVE_LOCK_CONFLICT) {
      FLOG_INFO("fail to execute sql, this task/rowkey is locked by other thread, pls try again", K(ret), K(sql));
    }
  } else {
    LOG_INFO("success to execute vector inde task update sql", K(ret), K(sql), K(affect_rows));
  }

  return ret;
}

int ObVecIndexAsyncTaskUtil::delete_vec_task(
    uint64_t tenant_id,
    const char* tname,
    common::ObISQLClient& proxy,
    ObVecIndexTaskKey& key,
    int64_t &affect_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.3.5.2 is not support");
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE "
                             "tenant_id = %ld AND table_id = %ld "
                             "AND tablet_id = %ld AND task_id = %ld",
                             tname,
                             ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                             key.table_id_,
                             key.tablet_id_, key.task_id_))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

int ObVecIndexAsyncTaskUtil::resume_task_from_inner_table(
    const int64_t tenant_id,
    const char* tname,
    const bool for_update /*false*/,
    const ObVecIndexFieldArray &filters,
    ObLS *ls, /* null means get all tenant task */
    common::ObISQLClient &proxy,
    ObVecIndexAsyncTaskOption &async_task_opt)
{
  int ret = OB_SUCCESS;
  const bool is_read_tenant_async_task = OB_ISNULL(ls) ? true : false;
  ObSqlString sql;
  uint64_t data_version = 0;

  if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.3.5.2 is not support");
  } else if (OB_FAIL(construct_read_task_sql(tenant_id, tname, for_update, is_read_tenant_async_task, filters, proxy, sql))) {
    LOG_WARN("fail to construct read task sql", K(ret), K(tenant_id), K(ls->get_ls_id()));
  } else {
    ObIAllocator *allocator = async_task_opt.get_allocator();
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult* result = nullptr;
      if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            LOG_WARN("fail to get next row", K(ret));
          } else {
            ObVecIndexTaskStatus task_result;
            if (OB_FAIL(extract_one_task_sql_result(result, task_result))) {
              LOG_WARN("fail to extrace one result", K(ret));
            } else {
              ObTabletHandle unused_tablet_handle;
              ObTabletID tablet_id(task_result.tablet_id_);
              bool inc_new_task = false;
              bool need_resumed = true;
              if (is_read_tenant_async_task) { // skip ls tablet filtered
              } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(tablet_id, unused_tablet_handle))) {
                need_resumed = false;
                if (OB_TABLET_NOT_EXIST != ret) {
                  LOG_WARN("fail to get tablet", K(ret), K(task_result.tablet_id_));
                } else {
                  ret = OB_SUCCESS; // continue
                }
              }
              LOG_INFO("resume task", K(ret), K(need_resumed), K(ls->get_ls_id()), K(task_result));
              if (OB_FAIL(ret) || !need_resumed) {  // skip
              } else if (task_result.status_ != ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH) { // resume not finish task
                ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
                char *task_ctx_buf = nullptr;
                if (task_result.task_type_ == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING) {
                  task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObHybridVectorRefreshTaskCtx)));
                } else {
                  task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx)));
                }

                if (task_result.task_type_ != ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL) {
                  task_result.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
                }

                if (OB_ISNULL(task_ctx_buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("async task ctx is null", K(ret));
                } else if (task_result.task_type_ == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING && OB_FALSE_IT(task_ctx = new(task_ctx_buf) ObHybridVectorRefreshTaskCtx())) {
                } else if (task_result.task_type_ != ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING && OB_FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
                } else if (OB_FALSE_IT(task_ctx->ls_ = ls)) {
                } else if (OB_FALSE_IT(task_ctx->tenant_id_ = tenant_id)) {
                } else if (OB_FALSE_IT(task_ctx->task_status_ = task_result)) {
                } else if (OB_FALSE_IT(task_ctx->task_status_.tenant_id_ = tenant_id)) {
                } else if (OB_FAIL(async_task_opt.add_task_ctx(tablet_id, task_ctx, inc_new_task))) {  // add task to map
                  LOG_WARN("fail to push back task", K(ret), K(task_ctx));
                } else if (inc_new_task) {
                  LOG_INFO("resume task success", K(tenant_id), KPC(task_ctx));
                }
                // free on failed
                if (OB_FAIL(ret) || !inc_new_task) {
                  if (OB_NOT_NULL(task_ctx)) {
                    task_ctx->~ObVecIndexAsyncTaskCtx();
                    allocator->free(task_ctx);
                    task_ctx = nullptr;
                  }
                }
              }
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  return ret;
}

int ObVecIndexAsyncTaskUtil::read_vec_tasks(
    const uint64_t tenant_id,
    const char* tname,
    const bool for_update /*false*/,
    const ObVecIndexFieldArray& filters,
    ObLS *ls, /* null means get all tenant task */
    common::ObISQLClient& proxy,
    ObVecIndexTaskStatusArray& result_arr,
    common::ObIAllocator *allocator /*NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const bool is_read_tenant_async_task = OB_ISNULL(ls) ? true : false;
  uint64_t data_version = 0;

  if (tenant_id == OB_INVALID_TENANT_ID || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(allocator));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.3.5.2 is not support");
  } else if (OB_FAIL(construct_read_task_sql(
      tenant_id, tname, for_update, is_read_tenant_async_task, filters, proxy, sql))) {
    LOG_WARN("fail to construct read task sql", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult* result = nullptr;
      if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            LOG_WARN("fail to get next row", K(ret));
          } else {
            ObVecIndexTaskStatus task_result;
            if (OB_FAIL(extract_one_task_sql_result(result, task_result))) {
              LOG_WARN("fail to extrace one result", K(ret));
            } else if (OB_FAIL(result_arr.push_back(task_result))) {
              LOG_WARN("fail to push back task", K(ret), K(result_arr.count()));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::construct_task_key(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t tablet_id,
    const int64_t task_id,
    ObVecIndexFieldArray& task_key)
{
  int ret = OB_SUCCESS;
  ObVecIndexTaskStatusField tenant_id_field;
  tenant_id_field.field_name_ = ObString("tenant_id");
  tenant_id_field.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
  tenant_id_field.data_.uint_ = tenant_id ;

  ObVecIndexTaskStatusField table_id_field;
  table_id_field.field_name_ = ObString("table_id");
  table_id_field.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
  table_id_field.data_.uint_ = table_id;

  ObVecIndexTaskStatusField tablet_id_field;
  tablet_id_field.field_name_ = ObString("tablet_id");
  tablet_id_field.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
  tablet_id_field.data_.uint_ = tablet_id;

  ObVecIndexTaskStatusField task_type_field;
  task_type_field.field_name_ = ObString("task_id");
  task_type_field.type_ = ObVecIndexTaskStatusField::INT_TYPE;
  task_type_field.data_.uint_ = task_id;

  if (OB_FAIL(task_key.push_back(tenant_id_field))) {
    LOG_WARN("failt to push back", KR(ret));
  } else if (OB_FAIL(task_key.push_back(table_id_field))) {
    LOG_WARN("failt to push back", KR(ret));
  } else if (OB_FAIL(task_key.push_back(tablet_id_field))) {
    LOG_WARN("failt to push back", KR(ret));
  } else if (OB_FAIL(task_key.push_back(task_type_field))) {
    LOG_WARN("failt to push back", KR(ret));
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::construct_read_task_sql(
    const uint64_t tenant_id,
    const char *tname,
    const bool for_update /* select for update*/,
    const bool is_read_tenant_async_task,
    const ObVecIndexFieldArray &filters,
    common::ObISQLClient& proxy,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s where ", tname))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  }
  for (size_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    const ObVecIndexTaskStatusField &field = filters.at(i);
    if (OB_FAIL(sql.append_fmt("%s = ", field.field_name_.ptr()))) {
      LOG_WARN("sql assign fmt failed", K(ret));
    } else if (field.type_ == ObVecIndexTaskStatusField::INT_TYPE) {
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.int_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObVecIndexTaskStatusField::UINT_TYPE) {
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.uint_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObVecIndexTaskStatusField::STRING_TYPE) {
      if (OB_FAIL(sql.append_fmt("%s", field.data_.str_.ptr()))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql append fmt failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt("%s", i == filters.count() - 1 ? "" : " AND "))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // 1. tenant async task : tablet_id == -1;
    // 2. LS async task: tablet_id != -1;
    if (is_read_tenant_async_task) {
      if (OB_FAIL(sql.append_fmt(" AND tablet_id == -1 "))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql.append_fmt(" AND tablet_id != -1 "))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && for_update) {
    if (OB_FAIL(sql.append_fmt(" for update"))) {
      LOG_WARN("sql append fmt failed", K(ret));
    }
  }

  return ret;
}

int ObVecIndexAsyncTaskUtil::extract_one_task_sql_result(
    sqlclient::ObMySQLResult* result, ObVecIndexTaskStatus &task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    uint64_t target_scn = 0;
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", task.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "table_id", task.table_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", task.tablet_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task.task_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "task_type", task.task_type_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "trigger_type", task.trigger_type_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "status", task.status_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "target_scn", target_scn, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ret_code", task.ret_code_, int64_t);
    task.target_scn_.convert_for_sql(target_scn);
    if (OB_SUCC(ret)) {
      int64_t real_length = 0;
      char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
      common::ObCurTraceId::TraceId trace_id;
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "trace_id", trace_id_buf, sizeof(trace_id_buf), real_length);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trace_id.parse_from_buf(trace_id_buf))) {
          LOG_WARN("fail to parse trace id from buf", K(ret), K(trace_id_buf));
        } else {
          task.trace_id_.set(trace_id.get());
        }
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::add_sys_task(ObVecIndexAsyncTaskCtx *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else {
    share::ObSysTaskStat sys_task_status;
    sys_task_status.start_time_ = ObTimeUtility::fast_current_time();
    sys_task_status.task_id_ = task->task_status_.trace_id_;
    sys_task_status.tenant_id_ = task->tenant_id_;
    sys_task_status.task_type_ = VECTOR_INDEX_ASYNC_TASK;
    if (OB_FAIL(SYS_TASK_STATUS_MGR.add_task(sys_task_status))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("sys task already exist", K(sys_task_status.task_id_), KPC(task));
      } else {
        LOG_WARN("add task failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) { // if ret = OB_ENTRY_EXIST, return same sys_task_id to task
      task->sys_task_id_ = sys_task_status.task_id_;
      LOG_INFO("add sys task", K(sys_task_status.task_id_), KPC(task));
    }
  }

  return ret;
}

int ObVecIndexAsyncTaskUtil::remove_sys_task(ObVecIndexAsyncTaskCtx *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else {
    const ObCurTraceId::TraceId &task_id = task->sys_task_id_;
    if (!task_id.is_invalid()) {
      if (OB_FAIL(SYS_TASK_STATUS_MGR.del_task(task_id))) {
        LOG_WARN("del task failed", K(ret), K(task_id));
      } else {
        LOG_INFO("remove sys task", K(task_id), KPC(task));
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTaskUtil::fetch_new_trace_id(
    const uint64_t basic_num, ObIAllocator *allocator, TraceId &new_trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(allocator));
  } else {
    uint64_t *new_trace_buf = static_cast<uint64_t *>(allocator->alloc(sizeof(uint64_t) * 4));
    if (OB_ISNULL(new_trace_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("async task ctx is null", K(ret));
    } else {
      TraceId ori_trace = *ObCurTraceId::get_trace_id();
      const uint64_t *ori_trace_buf = ori_trace.get();
      if (OB_ISNULL(ori_trace_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null trace buf", K(ret));
      } else {
        new_trace_buf[0] = ori_trace_buf[0];
        new_trace_buf[1] = ori_trace_buf[1];
        new_trace_buf[2] = ori_trace_buf[2];
        new_trace_buf[3] = basic_num;
        if (OB_FAIL(new_trace_id.set(new_trace_buf))) {
          LOG_WARN("fail to set new trace id", K(ret));
        }
      }
    }
    if (OB_NOT_NULL(new_trace_buf)) {
      allocator->free(new_trace_buf);
      new_trace_buf = nullptr;
    }
  }
  return ret;
}


int ObVecIndexAsyncTaskUtil::update_status_and_ret_code(ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_ID;

  if (OB_ISNULL(task_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task ctx", K(ret), KP(task_ctx));
  } else {
    ObVecIndexTaskKey key(task_ctx->task_status_.tenant_id_,
                          task_ctx->task_status_.table_id_,
                          task_ctx->task_status_.tablet_id_.id(),
                          task_ctx->task_status_.task_id_);

    ObVecIndexFieldArray update_fields;
    ObVecIndexTaskStatusField task_status;
    ObVecIndexTaskStatusField ret_code;
    ObVecIndexTaskStatusField target_scn;

    task_status.field_name_ = "status";
    task_status.data_.uint_ = task_ctx->task_status_.status_;
    ret_code.field_name_ = "ret_code";
    ret_code.data_.uint_ = task_ctx->task_status_.ret_code_;
    target_scn.field_name_ = "target_scn";
    target_scn.data_.int_ = task_ctx->task_status_.target_scn_.get_val_for_sql();

    int64_t tenant_id = task_ctx->task_status_.tenant_id_;

    if (tenant_id == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
    } else if (OB_FAIL(update_fields.push_back(task_status))) {
      LOG_WARN("fail to push back update field", K(ret), K(task_status));
    } else if (OB_FAIL(update_fields.push_back(ret_code))) {
      LOG_WARN("fail to push back update field", K(ret), K(ret_code));
    } else if (OB_FAIL(update_fields.push_back(target_scn))) {
      LOG_WARN("fail to push back update field", K(ret), K(target_scn));
    } else {
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
        LOG_WARN("fail start transaction", K(ret), K(task_ctx));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_vec_task(
          tenant_id, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, key, update_fields,
          task_ctx->task_status_.progress_info_))) {
        LOG_WARN("fail to update task status", K(ret));
      } else {
        LOG_DEBUG("success to update_status_and_ret_code", KPC(task_ctx));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}


/**************************** ObVecIndexAsyncTaskHandler ******************************/
ObVecIndexAsyncTaskHandler::ObVecIndexAsyncTaskHandler()
  : is_inited_(false), tg_id_(INVALID_TG_ID), async_task_ref_cnt_(0), stopped_(false)
{
}

ObVecIndexAsyncTaskHandler::~ObVecIndexAsyncTaskHandler()
{
}

int ObVecIndexAsyncTaskHandler::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::VectorAsyncTaskPool, tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT failed", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObVecIndexAsyncTaskHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT, MAX_THREAD_COUNT))) { // must be call TG_SET_ADAPTIVE_THREAD
    LOG_WARN("TG_SET_ADAPTIVE_THREAD failed", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K_(tg_id));
  } else {
    LOG_INFO("succ to start vector index async task handler", K_(tg_id));
  }
  return ret;
}

void ObVecIndexAsyncTaskHandler::stop()
{
  LOG_INFO("vector index async task handler start to stop", K_(tg_id));
  set_stop();
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("vector index async task handler finish to stop", K_(tg_id));
}

void ObVecIndexAsyncTaskHandler::wait()
{
  LOG_INFO("vector index async task handler start to wait", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
  LOG_INFO("vector index async task handler finish to wait", K_(tg_id));
}

void ObVecIndexAsyncTaskHandler::destroy()
{
  LOG_INFO("vector index async task handler start to destroy");
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  is_inited_ = false;
  LOG_INFO("vector index async task handler finish to destroy");
}

int ObVecIndexAsyncTaskHandler::push_task(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObVecIndexAsyncTaskCtx *ctx,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  LOG_INFO("push back async task to thread pool", K(allocator), K(ctx->task_status_.tablet_id_), K(ctx->task_status_.task_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_ISNULL(ctx) || OB_ISNULL(allocator))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), KP(ctx), KP(allocator));
  } else if (ctx->task_status_.task_type_ == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL) {
    ObVecIndexAsyncTask *async_task = nullptr;
    if (OB_ISNULL(async_task = static_cast<ObVecIndexAsyncTask *>(allocator->alloc(sizeof(ObVecIndexAsyncTask))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory of ObVecIndexAsyncTask", K(ret), K(tenant_id), K(ls_id));
    } else if (FALSE_IT(async_task = new (async_task) ObVecIndexAsyncTask())) {
    } else if (OB_FAIL(async_task->init(tenant_id, ls_id, ctx->task_status_.task_type_, ctx))) {
      LOG_WARN("fail to init opt async task", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, async_task))) {
      LOG_WARN("fail to TG_PUSH_TASK", KR(ret), KPC(async_task));
    } else {
      // !!!! inc async task ref cnt;
      inc_async_task_ref();
    }
    // free memory
    if (OB_FAIL(ret) && OB_NOT_NULL(async_task)) {
      async_task->~ObVecIndexAsyncTask();
      allocator->free(async_task);  // arena need free? no
      async_task = nullptr;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(async_task)) {
      handle_ls_process_task_cnt(async_task->get_ls_id(), true);
    }
  } else if (ctx->task_status_.task_type_ == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING) {
    ObHybridVectorRefreshTask *task = nullptr;
    if (OB_ISNULL(task = static_cast<ObHybridVectorRefreshTask *>(allocator->alloc(sizeof(ObHybridVectorRefreshTask))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory of ObHybridVectorRefreshTask", K(ret), K(tenant_id), K(ls_id));
    } else if (FALSE_IT(task = new (task) ObHybridVectorRefreshTask())) {
    } else if (OB_FAIL(task->init(tenant_id, ls_id, ctx->task_status_.task_type_, ctx))) {
      LOG_WARN("fail to init opt async task", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
      LOG_WARN("fail to TG_PUSH_TASK", KR(ret), KPC(task));
    }
    // free memory
    if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
      task->~ObHybridVectorRefreshTask();
      allocator->free(task);  // arena need free? no
      task = nullptr;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(task) &&
        (task->current_status() == ObHybridVectorRefreshTaskStatus::TASK_PREPARE || task->current_status() == ObHybridVectorRefreshTaskStatus::TASK_FINISH)) {
      task->reset_status();
      handle_ls_process_task_cnt(task->get_ls_id(), true);
      inc_async_task_ref();
    }
  } else if (ctx->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_IVF_LOAD
          || ctx->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_IVF_CLEAN) {
    ObIvfAsyncTask *ivf_task = nullptr;
    if (OB_ISNULL(ivf_task = OB_NEWx(ObIvfAsyncTask, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory of ObIvfAsyncTask", K(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(ivf_task->init(tenant_id, ls_id, ctx->task_status_.task_type_, ctx))) {
      LOG_WARN("fail to init opt async task", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, ivf_task))) {
      LOG_WARN("fail to TG_PUSH_TASK", KR(ret), KPC(ivf_task));
    } else {
      // !!!! inc async task ref cnt;
      inc_async_task_ref();
    }
    // free memory
    if (OB_FAIL(ret) && OB_NOT_NULL(ivf_task)) {
      ivf_task->~ObIvfAsyncTask();
      allocator->free(ivf_task);  // arena need free? no
      ivf_task = nullptr;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(ivf_task)) {
      handle_ls_process_task_cnt(ivf_task->get_ls_id(), true);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task type", K(ret), K(ctx));
  }
  return ret;
}

int ObVecIndexAsyncTaskHandler::get_allocator_by_ls(const ObLSID &ls_id, ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vec_idx_mgr = nullptr;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", K(ret), K(ls_id));
  } else if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, vec_idx_mgr))) {
    LOG_WARN("fail to get vector index ls mgr", KR(ret), K(ls_id));
  } else if (OB_ISNULL(vec_idx_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(vec_idx_mgr));
  } else {
    ObVecIndexAsyncTaskOption &task_opt = vec_idx_mgr->get_async_task_opt();
    allocator = task_opt.get_allocator();
  }
  return ret;
}

void ObVecIndexAsyncTaskHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObVecIndexIAsyncTask *async_task = nullptr;
  bool is_cancel = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    async_task = static_cast<ObVecIndexIAsyncTask *>(task);
    ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
    if (async_task->get_task_type() == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL
    || async_task->get_task_type() == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING
    || async_task->get_task_type() == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_IVF_LOAD
    || async_task->get_task_type() == ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_IVF_CLEAN) {
      ObVecIndexAsyncTaskCtx *task_ctx = async_task->get_task_ctx();
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ctx nullptr", K(ret), KP(task_ctx));
      }
      if (OB_SUCC(ret)) {
        common::ObSpinLockGuard ctx_guard(task_ctx->lock_); // lock ctx
        if (task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE) {
          task_ctx->is_new_task_ = true;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING;
          if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
            LOG_WARN("fail to update task status to inner table", K(ret), K(*task_ctx));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_is_cancel(task_ctx, is_cancel))) {
        LOG_WARN("fail to check task is cancel", K(task_ctx));
      } else if (is_cancel || (OB_NOT_NULL(vector_index_service) && vector_index_service->get_vec_async_task_handle().is_stopped())) {
        async_task->check_task_free();
        async_task->get_task_ctx()->task_status_.ret_code_ = OB_CANCELED;
      } else if (OB_FAIL(async_task->do_work())) {
        LOG_WARN("fail to do task", KR(ret), KPC(async_task));
      }
    } else {  // TODO: will support index built later
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task type", K(ret), KPC(async_task));
    }
  }
  if (OB_NOT_NULL(async_task)
      && (async_task->get_task_type() != ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING || async_task->get_task_ctx()->task_status_.all_finished_)) {
    handle_ls_process_task_cnt(async_task->get_ls_id(), false);
    dec_async_task_ref();
  }
  // free memory
  if (OB_NOT_NULL(async_task)) {
    int tmp_ret = OB_SUCCESS;
    ObIAllocator *allocator = nullptr;
    async_task->~ObVecIndexIAsyncTask();
    if (OB_TMP_FAIL(get_allocator_by_ls(async_task->get_ls_id(), allocator))) {
      LOG_WARN("fail to get allocator by ls id", K(tmp_ret), K(async_task->get_ls_id()));
    } else if (OB_ISNULL(allocator)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null allocator", K(tmp_ret), K(async_task->get_ls_id()));
    } else {
      allocator->free(async_task);
    }
  }
}

void ObVecIndexAsyncTaskHandler::handle_drop(void *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    // thread has set stop.
    ObVecIndexAsyncTask *async_task = nullptr;
    async_task = static_cast<ObVecIndexAsyncTask *>(task);
    LOG_INFO("finish ObVecIndexAsyncTaskHandler::handle_drop", KPC(async_task));
    if (OB_NOT_NULL(async_task)) {
      handle_ls_process_task_cnt(async_task->get_ls_id(), false);
    }
    if (OB_NOT_NULL(async_task)) {
      int tmp_ret = OB_SUCCESS;
      ObIAllocator *allocator = nullptr;
      async_task->~ObVecIndexAsyncTask();
      if (OB_TMP_FAIL(get_allocator_by_ls(async_task->get_ls_id(), allocator))) {
        LOG_WARN("fail to get allocator by ls id", K(tmp_ret), K(async_task->get_ls_id()));
      } else if (OB_ISNULL(allocator)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null allocator", K(tmp_ret), K(async_task->get_ls_id()));
      } else {
        allocator->free(async_task);
      }
    }
    // !!!!! desc async task ref cnt
    dec_async_task_ref();
  }
}

void ObVecIndexAsyncTaskHandler::handle_ls_process_task_cnt(const ObLSID &ls_id, const bool is_inc)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vec_idx_mgr = nullptr;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[handle ls process task] invalid ls id", K(ret), K(ls_id));
  } else if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[handle ls process task] unexpected nullptr", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, vec_idx_mgr))) {
    LOG_WARN("[handle ls process task] fail to get vector index ls mgr", KR(ret), K(ls_id));
  } else if (OB_ISNULL(vec_idx_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[handle ls process task] unexpected nullptr", K(ret), KP(vec_idx_mgr));
  } else if (is_inc) {
    vec_idx_mgr->get_async_task_opt().inc_ls_task_cnt();
  } else {
    vec_idx_mgr->get_async_task_opt().dec_ls_task_cnt();
  }
}

/**************************** ObVecIndexIAsyncTask ******************************/
int ObVecIndexIAsyncTask::init(
  const uint64_t tenant_id,
  const ObLSID &ls_id,
  const int task_type,
  ObVecIndexAsyncTaskCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id,
                                                                                 vec_idx_mgr_))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
    }
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (!ls_id.is_valid()) || OB_ISNULL(ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(ls_id), KP(ctx));
  } else {
    ctx_ = ctx;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    task_type_ = task_type;
    is_inited_ = true;
  }
  return ret;
}

/**************************** ObVecIndexAsyncTask ******************************/
int ObVecIndexAsyncTask::do_work()
{
  int ret = OB_SUCCESS;
  bool task_started = false;
  bool has_visible_column = false;
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexAdaptor *new_adapter = nullptr;
  LOG_INFO("start do_work", K(ret), K(ctx_->task_status_), K(ls_id_));

  DEBUG_SYNC(HANDLE_VECTOR_INDEX_ASYNC_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIndexAsyncTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.tablet_id_, adpt_guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("can not get adapter, need wait", K(ret), KPC(ctx_));
    } else {
      LOG_WARN("fail to get adapter instance", KR(ret), KPC(ctx_));
    }
  } else if (OB_ISNULL(adpt_guard.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get vector index adapter", KR(ret), KPC(ctx_));
  } else if (FALSE_IT(task_started = true)) {
  } else if (adpt_guard.get_adatper()->has_doing_vector_index_task()) {
    ret = OB_EAGAIN;
    LOG_INFO("there is other vector index task running", K(ret), KP(adpt_guard.get_adatper()));
  } else if (!adpt_guard.get_adatper()->is_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter not complete, need wait", K(ret), KP(adpt_guard.get_adatper()));
  } else if (!check_task_satisfied_memory_limited(*adpt_guard.get_adatper())) {
    ret = OB_EAGAIN; // will retry
    LOG_INFO("skip to do async task due to tenant memory limit", KR(ret), KPC(ctx_));
  } else if (FALSE_IT(set_old_adapter(adpt_guard.get_adatper()))) {
  } else if (OB_FAIL(create_new_adapter(vector_index_service, adpt_guard, new_adapter))) {
    LOG_WARN("fail to get or create new adapter", K(ret), K(ctx_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(new_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(new_adapter));
  } else if (OB_FAIL(check_snapshot_table_has_visible_column(has_visible_column))) {
    LOG_WARN("fail to check snapshot table column", K(ret), K(ctx_));
  } else if (has_visible_column && !new_adapter->is_hybrid_index()) {
    if (OB_FAIL(parallel_optimize_vec_index())) {
      LOG_WARN("fail to inner do work", K(ret), K(ctx_));
    }
  } else if (OB_FAIL(optimize_vector_index(*new_adapter, *adpt_guard.get_adatper()))) {
    LOG_WARN("failed to optimize vector index", K(ret), K(ctx_));
  }

  if (task_started) {
    adpt_guard.get_adatper()->vector_index_task_finish();
    ctx_->task_status_.progress_info_.reset();
  }
  if (OB_FAIL(ret) && !has_replace_old_adapter_ && OB_NOT_NULL(new_adapter)) {
    LOG_INFO("release new adapter memory in failure", K(ret));
    new_adapter->~ObPluginVectorIndexAdaptor();
    vector_index_service->get_allocator().free(new_adapter);
    new_adapter = nullptr;
  }

  // clean tmp info
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(vector_index_service->release_vector_index_tmp_info(ctx_->task_status_.task_id_))) {
      LOG_WARN("fail to release vector index tmp info", K(ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
  }

  LOG_INFO("end do_work", K(ret), K(ctx_->task_status_));
  return ret;
}

int ObVecIndexAsyncTask::check_snapshot_table_has_visible_column(bool &has_visible_column)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *snapshot_schema = nullptr;
  has_visible_column = false;

  if (OB_ISNULL(new_adapter_) || tenant_id_ == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), KP(new_adapter_));
  } else {
    const int64_t data_table_id = new_adapter_->get_data_table_id();
    const int64_t snap_table_id = new_adapter_->get_snapshot_table_id();
    if (data_table_id == OB_INVALID_ID || snap_table_id == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(data_table_id), K(snap_table_id));
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, snap_table_id, snapshot_schema))) {
      LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(snap_table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_schema))) {
      LOG_WARN("failed to get simple schema", K(ret), K(tenant_id_), K(data_table_id));
    } else if (OB_ISNULL(snapshot_schema) || OB_ISNULL(data_schema) ||
        snapshot_schema->is_in_recyclebin() || data_schema->is_in_recyclebin()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema not exist", K(ret), K(data_table_id), K(snap_table_id), KP(snapshot_schema), KP(data_schema));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < snapshot_schema->get_column_count() && !has_visible_column; i++) {
        const ObColumnSchemaV2 *index_col_schema = nullptr;
        const ObColumnSchemaV2 *data_col_schema = nullptr;
        if (OB_ISNULL(index_col_schema = snapshot_schema->get_column_schema_by_idx(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index_col_schema, is nullptr", K(ret), K(i), KPC(snapshot_schema));
        } else if (OB_ISNULL(data_col_schema = data_schema->get_column_schema(index_col_schema->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data_col_schema, is nullptr", K(ret), KPC(index_col_schema));
        } else if (data_col_schema->is_vec_hnsw_visible_column()) {
          has_visible_column = true;
        }
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::create_new_adapter(
    ObPluginVectorIndexService *vector_index_service, ObPluginVectorIndexAdapterGuard &old_adapter_guard, ObPluginVectorIndexAdaptor *&new_adapter)
{
  int ret = OB_SUCCESS;
  // get adapter if exist
  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(vector_index_service));
  } else {
    ObVectorIndexTmpInfo *tmp_info = nullptr;
    new_adapter = nullptr;
    void *adpt_buff = vector_index_service->get_allocator().alloc(sizeof(ObPluginVectorIndexAdaptor));
    if (OB_ISNULL(adpt_buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for vector index adapter", KR(ret));
    } else {
      ctx_->task_status_.progress_info_.vec_opt_status_ = OB_VECTOR_ASYNC_OPT_PREPARE;
      new_adapter = new(adpt_buff)ObPluginVectorIndexAdaptor(&vector_index_service->get_allocator(), vec_idx_mgr_->get_memory_context(), tenant_id_);
      new_adapter->set_create_type(old_adapter_guard.get_adatper()->get_create_type());
      if (OB_FAIL(new_adapter->copy_meta_info(*old_adapter_guard.get_adatper()))) {
        LOG_WARN("failed to copy meta info", K(ret));
      } else if (OB_FAIL(new_adapter->init(vec_idx_mgr_->get_memory_context(), vec_idx_mgr_->get_all_vsag_use_mem()))) {
        LOG_WARN("failed to init adpt.", K(ret));
      } else if (OB_FAIL(new_adapter->set_index_identity(old_adapter_guard.get_adatper()->get_index_identity()))) {
        LOG_WARN("failed to set index identity", K(ret));
      } else if (OB_FALSE_IT(set_new_adapter(new_adapter))) {
      } else if (OB_FAIL(vector_index_service->get_vector_index_tmp_info(ctx_->task_status_.task_id_, tmp_info))) {
        LOG_WARN("fail to get vector index tmp info", K(ret), K(ctx_));
      } else {
        tmp_info->adapter_ = new_adapter;
      }
    }
  }
  return ret;
}

// 扫描5号表，得到key中scn=ctx.target_scn的记录，这些记录是写入到5号表的新纪录。
int ObVecIndexAsyncTask::try_deseriale_snapshot_data(common::ObNewRowIterator *snapshot_idx_iter, const bool need_unvisible)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(snapshot_idx_iter) || OB_ISNULL(new_adapter_) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(snapshot_idx_iter), KP(new_adapter_), KP(ctx_));
  } else {
    ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObHNSWDeserializeCallback::CbParam param;
    param.iter_ = snapshot_idx_iter;
    param.allocator_ = &tmp_allocator;
    param.is_vec_tablet_rebuild_ = true;
    param.is_need_unvisible_row_ = need_unvisible;

    ObHNSWDeserializeCallback callback(static_cast<void*>(new_adapter_));
    ObIStreamBuf::Callback cb = callback;
    ObVectorIndexSerializer index_seri(tmp_allocator);
    ObVectorIndexMemData *snap_memdata = new_adapter_->get_snap_data_();
    int64_t row_cnt = 0;

    if (OB_ISNULL(snap_memdata)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("snap memdata is null", K(ret));
    } else {
      TCWLockGuard lock_guard(snap_memdata->mem_data_rwlock_);
      if (OB_FAIL(index_seri.deserialize(snap_memdata->index_, param, cb, MTL_ID()))) {
        LOG_WARN("serialize index failed.", K(ret));
      } else if (OB_FAIL(new_adapter_->get_snap_index_row_cnt(row_cnt))) {
        LOG_WARN("fail to get snap index row cnt", K(ret));
      } else if (row_cnt > 0) { // deseriablize data from table 5
        new_adapter_->close_snap_data_rb_flag();
      }
      LOG_INFO("try to deseriable snapshot data", K(ret), K(row_cnt), K(new_adapter_->get_snap_index_type()), KPC(new_adapter_));
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::get_read_snapshot_table_scn(share::SCN &target_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task ctx", K(ret), K(ctx_));
  } else if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE) {
    target_scn = ctx_->task_status_.target_scn_;
  } else if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING ||
             ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN) {
    if (OB_FAIL(get_current_scn(target_scn))) {
      LOG_WARN("fail to get current scn", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task status", K(ret), K(ctx_));
  }
  return ret;
}

int ObVecIndexAsyncTask::check_and_refresh_new_adapter(bool &need_redo_current_status)
{
  int ret = OB_SUCCESS;

  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObTableScanIterator *table_scan_iter = nullptr;
  common::ObNewRowIterator *snapshot_idx_iter = nullptr;
  storage::ObTableScanParam snapshot_scan_param;
  schema::ObTableParam snapshot_table_param(allocator_);
  bool get_unvisible_row = false;
  need_redo_current_status = false;

  share::SCN current_scn;

  if (!ls_id_.is_valid() || OB_ISNULL(new_adapter_) || OB_ISNULL(tsc_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id_), KP(new_adapter_), KP(tsc_service));
  } else if (ctx_->is_new_task_) { // 非重启从running或之后状态恢复的场景
    need_redo_current_status = true;
    LOG_INFO("vector index task new task, need do next", KPC(ctx_));
  } else if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get snapshot table read scn", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_, new_adapter_, current_scn,
                                                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                                allocator_,
                                                                allocator_,
                                                                snapshot_scan_param,
                                                                snapshot_table_param,
                                                                snapshot_idx_iter))) { // read_local_tablet 5th aux index get rowkey
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id_));
  } else if (OB_FALSE_IT(table_scan_iter = static_cast<ObTableScanIterator *>(snapshot_idx_iter))) {
  } else {
    if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING) {
      get_unvisible_row = true;
      int64_t snap_row_cnt = 0;
      if (OB_FAIL(try_deseriale_snapshot_data(snapshot_idx_iter, get_unvisible_row))) {
        LOG_WARN("fail to desriable snapshot data", K(ret));
      } else if (OB_FAIL(new_adapter_->get_snap_index_row_cnt(snap_row_cnt))) {
        LOG_WARN("fail to get snap index row cnt", K(ret), KPC(ctx_));
      } else if (snap_row_cnt <= 0) {
        need_redo_current_status = true;
      } else {
        LOG_INFO("no need to continue to execute insert", KPC(ctx_), K(snap_row_cnt));
      }
    } else if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE) {
      bool is_finised_exchange = false;
      if (OB_FAIL(check_finished_exchange_before(current_scn, is_finised_exchange))) {
        LOG_WARN("fail to check finish exchange before", K(ret), K(ctx_));
      } else if (!is_finised_exchange) {
        need_redo_current_status = true;
      }
      get_unvisible_row = is_finised_exchange ? false : true;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(try_deseriale_snapshot_data(snapshot_idx_iter, get_unvisible_row))) {
        LOG_WARN("fail to desriable snapshot data", K(ret), KPC(ctx_));
      }
    } else if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN) {
      get_unvisible_row = false;
      if (OB_FAIL(try_deseriale_snapshot_data(snapshot_idx_iter, get_unvisible_row))) {
        LOG_WARN("fail to desriable snapshot data", K(ret));
      } else {
        need_redo_current_status = true; // alway do execute_clean
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task status", K(ret), K(ctx_->task_status_.status_));
    }
  }
  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snapshot_idx_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(snapshot_idx_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snapshot_idx_iter = nullptr;
  }
  return ret;
}

// 比对5号表里visible的row的scn，如果异步任务的target_scn相同，说明已经做完exchange
int ObVecIndexAsyncTask::check_finished_exchange_before(share::SCN &current_scn, bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;

  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObTableScanIterator *table_scan_iter = nullptr;
  common::ObNewRowIterator *snapshot_idx_iter = nullptr;
  storage::ObTableScanParam snapshot_scan_param;
  schema::ObTableParam snapshot_table_param(allocator_);
  blocksstable::ObDatumRow *row = nullptr;

  if (OB_ISNULL(tsc_service) || OB_ISNULL(new_adapter_) || !ls_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected nullptr", K(ret), KP(tsc_service), KP(new_adapter_), K(ls_id_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_snap_index_visible_row_iter(tsc_service, ls_id_,
                                                                new_adapter_,
                                                                current_scn,
                                                                allocator_,
                                                                snapshot_scan_param,
                                                                snapshot_table_param,
                                                                snapshot_idx_iter,
                                                                row))) {
    LOG_WARN("fail to get snap inde visible row iter", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task in exchange status, but table 5 has no visible row is unexpected", K(ret));
  } else {
    ObString key_str = row->storage_datums_[0].get_string();
    int64_t key_scn = 0;
    if (OB_FAIL(ObPluginVectorIndexUtils::get_table_key_scn(key_str, key_scn))) {
      LOG_WARN("fail to get table key scn", K(ret), K(key_str));
    } else if (key_scn == ctx_->task_status_.target_scn_.get_val_for_sql()) {
      is_finished = true;
    }
    LOG_INFO("check finish exchange before", K(ret),
      K(is_finished), K(key_scn), K(ctx_->task_status_.target_scn_));
  }
  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snapshot_idx_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(snapshot_idx_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snapshot_idx_iter = nullptr;
  }

  return ret;
}

/*
1. 任务失败后，需要清掉表上的数据
2. 重启恢复任务到特性状态时，需要处理：
   2.1 runing阶段，使用当前read_scn打开迭代器->反序列化加载unvisible的数据->判断adapter->snap_index.cnt是否大于0->小于0说明做过写5号表，则需要重新执行inner_sql
   2.2 exchange阶段，使用target_scn打开迭代器->如果迭代器能获取记录（说明数据未被交换，数据交换后版本号会被推高，使用target_scn将获取不到）,则获取un-visible记录，重新执行exchange；如果迭代器获取不到记录，则使用当前scn重新打开迭代器，获取visible的记录。
   2.3 clean阶段，使用当前scn打开迭代器->获取visible的数据。
*/
int ObVecIndexAsyncTask::parallel_optimize_vec_index()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_redo_current_status = false;
  LOG_INFO("start parallel optimize vec index", KPC(ctx_));
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KP(ctx_));
  } else if (OB_FAIL(check_and_refresh_new_adapter(need_redo_current_status))) {
    LOG_WARN("fail to check and refresh new adapter", K(ret), KPC(ctx_));
  }
  // running
  if (OB_FAIL(ret)) {
  } else if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING) {
    if (need_redo_current_status && OB_FAIL(execute_insert())) {
      LOG_WARN("fail to execute insert", K(ret), KPC(ctx_));
    } else {
      DEBUG_SYNC(PARALLEL_VEC_TASK_EXECUTE_INSERT);
      need_redo_current_status = true; // reset flag
      common::ObSpinLockGuard ctx_guard(ctx_->lock_);
      ctx_->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE;
      if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(ctx_))) {
        LOG_WARN("fail to update task status to inner table", K(ret), K(tenant_id_), KPC(ctx_));
      }
    }
  }
  // exchange
  if (OB_FAIL(ret)) {
  } else if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE) {
    if (need_redo_current_status && OB_FAIL(execute_exchange())) {
      LOG_WARN("fail to execute exchange", K(ret), K(ctx_));
    } else {
      DEBUG_SYNC(PARALLEL_VEC_TASK_EXECUTE_EXCHANGE);
      if (!need_redo_current_status) {
        RWLock::WLockGuard lock_guard(vec_idx_mgr_->get_adapter_map_lock());
        if (OB_FAIL(vec_idx_mgr_->replace_old_adapter(new_adapter_))) {
          LOG_WARN("failed to replace old adapter", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        has_replace_old_adapter_ = true;
        common::ObSpinLockGuard ctx_guard(ctx_->lock_);
        ctx_->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN;
        if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(ctx_))) {
          LOG_WARN("fail to update task status to inner table", K(ret), K(tenant_id_), KPC(ctx_));
        }
      }
    }
  }
  // clean.
  // delete table 5 un-visible data when fail or finish
  if (OB_FAIL(ret) || ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN) {
    if (OB_TMP_FAIL(execute_clean())) {
      LOG_WARN("fail to execute clean", K(ret), K(tmp_ret), KPC(ctx_));
    }
    DEBUG_SYNC(PARALLEL_VEC_TASK_EXECUTE_CLEAN);
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }
  LOG_INFO("end parallel optimize vec index", K(ret), KPC(ctx_));
  return ret;
}

int ObVecIndexAsyncTask::execute_insert()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_schema = nullptr;
  int64_t parallelism;
  int64_t data_table_id;
  int64_t dest_table_id;
  ObTabletID dest_tablet_id;
  ObString partition_names("");
  share::SCN current_scn;

  ObArenaAllocator allocator("atask_built");

  if (OB_ISNULL(ctx_) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(ctx_), K(new_adapter_));
  } else if (OB_FALSE_IT(data_table_id = new_adapter_->get_data_table_id())) {
  } else if (OB_FALSE_IT(dest_table_id = new_adapter_->get_snapshot_table_id())) {
  } else if (OB_FALSE_IT(dest_tablet_id = new_adapter_->get_snap_tablet_id())) {
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(data_table_id));
  } else if (OB_ISNULL(data_schema) || data_schema->is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema not exist", K(ret), K(data_table_id));
  } else if (OB_FAIL(get_task_paralellism(parallelism))) {
    LOG_WARN("fail to get task parallelism", K(ret), K(ctx_));
  } else if (OB_FAIL(get_partition_name(*data_schema, data_table_id, dest_table_id, dest_tablet_id, allocator, partition_names))) {
    LOG_WARN("fail to get partition name", K(ret), K(ctx_));
  } else if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get current scn", K(ret), K(ctx_));
  } else if (OB_FALSE_IT(ctx_->task_status_.target_scn_ = current_scn)) {
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::set_inner_sql_snapshot_version(ctx_->task_status_.task_id_, ctx_->task_status_.target_scn_.get_val_for_sql()))) {
    LOG_WARN("fail to set inner sql snapshot", K(ret), K(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::set_inner_sql_schema_version(ctx_->task_status_.task_id_, data_schema->get_schema_version()))) {
    LOG_WARN("fail to set inner sql snapshot", K(ret), K(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(ctx_))) { // update target_scn before inner sql
    LOG_WARN("fail to update target scn", K(ret), K(ctx_));
  } else if (OB_FAIL(execute_inner_sql(*data_schema, data_table_id, dest_table_id, ctx_->task_status_.task_id_,
      parallelism, partition_names, current_scn))) {
    LOG_WARN("fail to execute inner sql", K(ret), K(partition_names), K(ctx_));
  }
  return ret;
}

int ObVecIndexAsyncTask::get_task_paralellism(int64_t &parallelism)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  parallelism = 0;
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", KR(ret), K(tenant_id_));
  } else {
    int64_t user_config_parallelism = tenant_config->vector_index_optimization_concurrency;
    parallelism = user_config_parallelism; // init
    double min_cpu = 0.0;
    double max_cpu = 0.0;
    if (OB_FAIL(GCTX.omt_->get_tenant_cpu(tenant_id_, min_cpu, max_cpu))) {
      LOG_WARN("fail to get tenant cpu count", K(ret));
    } else {
      int64_t tenant_max_cpu = static_cast<int64_t>(max_cpu);
      parallelism = tenant_max_cpu < 4 ? 1 : user_config_parallelism;
      LOG_INFO("get execute inner sql parallelism", K(ret), K(tenant_max_cpu), K(user_config_parallelism), K(parallelism));
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::get_partition_name(
    const ObTableSchema &data_table_schema, const int64_t data_table_id, const int64_t index_table_id, const ObTabletID &tablet_id,
    common::ObIAllocator &allocator, ObString &partition_names)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObArray<ObTabletID> tablet_ids; // only one item
  ObArray<ObString> batch_partition_names;
  const ObPartitionOption &data_part_option = data_table_schema.get_part_option();
  if (!data_table_schema.is_partitioned_table()) { // TODO@xiajin: auto part is not supported
    // skip none partiton table
  } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("fail to push back tablet id", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObDDLUtil::get_index_table_batch_partition_names(tenant_id_, data_table_id, index_table_id, tablet_ids, allocator, batch_partition_names))) {
    LOG_WARN("fail to get index table batch partition names", K(ret), K(tenant_id_), K(data_table_id), K(index_table_id), K(tablet_ids), K(batch_partition_names));
  } else if (OB_FAIL(ObDDLUtil::generate_partition_names(batch_partition_names, is_oracle_mode, allocator, partition_names))) {
    LOG_WARN("fail to generate partition names", K(ret), K(batch_partition_names), K(is_oracle_mode), K(partition_names));
  } else if (partition_names.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty partition of partition table", K(ret), K(tablet_id), K(index_table_id), K(data_table_id));
  }
  return ret;
}

int ObVecIndexAsyncTask::get_current_scn(share::SCN &current_scn)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", KR(ret));
  } else {
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else if (OB_FAIL(txs->get_read_snapshot_version(timeout_ctx.get_abs_timeout(), current_scn))) {
      LOG_WARN("get read snapshot version", KR(ret));
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::execute_inner_sql(
    const ObTableSchema &data_schema, const int64_t data_table_id, const int64_t dest_table_id,
    const int64_t task_id, const int64_t parallelism, ObString &partition_names, share::SCN &current_scn)
{
  int ret = OB_SUCCESS;

  ObSqlString sql_string;
  bool need_padding = false;
  int ret_code = -1;

  if (data_table_id == OB_INVALID_ID || dest_table_id == OB_INVALID_ID || parallelism < 1 || !current_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_table_id), K(dest_table_id), K(parallelism), K(current_scn));
  } else if (OB_FAIL(ObDDLUtil::generate_build_replica_sql(tenant_id_, data_table_id,
                                                    dest_table_id,
                                                    data_schema.get_schema_version(),
                                                    current_scn.get_val_for_sql(),
                                                    1, /* execution_id */
                                                    task_id,
                                                    parallelism,
                                                    false/*use_heap_table_ddl*/,
                                                    !data_schema.is_user_hidden_table()/*use_schema_version_hint_for_src_table*/,
                                                    nullptr,
                                                    partition_names,
                                                    false/*is_alter_clustering_key_tbl_partition_by*/,
                                                    sql_string))) {
    LOG_WARN("fail to generate build replica sql", K(ret));
  } else if (OB_FAIL(data_schema.is_need_padding_for_generated_column(need_padding))) {
    LOG_WARN("fail to check need padding", K(ret));
  } else {
    common::ObCommonSqlProxy *user_sql_proxy = GCTX.ddl_sql_proxy_;
    int64_t affected_rows = 0;
    ObSQLMode sql_mode = SMO_STRICT_ALL_TABLES | (need_padding ? SMO_PAD_CHAR_TO_FULL_LENGTH : 0);
    ObSessionParam session_param;
    session_param.sql_mode_ = (int64_t *)&sql_mode;
    session_param.tz_info_wrap_ = nullptr;
    session_param.ddl_info_.set_is_ddl(true);
    session_param.ddl_info_.set_source_table_hidden(false);
    session_param.ddl_info_.set_dest_table_hidden(false);
    session_param.ddl_info_.set_is_vec_tablet_rebuild(true);
    //session_param.ddl_info_.set_retryable_ddl(is_retryable_ddl_);
    session_param.nls_formats_[ObNLSFormatEnum::NLS_DATE] = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
    session_param.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
    session_param.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
    session_param.use_external_session_ = true;  // means session id dispatched by session mgr
    //session_param.consumer_group_id_ = 0;

    ObTimeoutCtx timeout_ctx;
    const int64_t DDL_INNER_SQL_EXECUTE_TIMEOUT = ObDDLUtil::calc_inner_sql_execute_timeout();
    ObASHSetInnerSqlWaitGuard ash_inner_sql_guard(ObInnerSqlWaitTypeId::RS_CREATE_INDEX_BUILD_REPLICA);
    LOG_INFO("execute sql" , K(sql_string), K(current_scn), K(data_table_id), K(tenant_id_));
    if (OB_FAIL(timeout_ctx.set_trx_timeout_us(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
      LOG_WARN("set trx timeout failed", K(ret));
    } else if (OB_FAIL(timeout_ctx.set_timeout(DDL_INNER_SQL_EXECUTE_TIMEOUT))) {
      LOG_WARN("set timeout failed", K(ret));
    } else if (OB_FAIL(user_sql_proxy->write(tenant_id_, sql_string.ptr(), affected_rows, ObCompatibilityMode::MYSQL_MODE, &session_param))) {
      LOG_WARN("fail to execute build replica sql", K(ret), K(tenant_id_));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_inner_sql_ret_code(task_id, ret_code))) {
      LOG_WARN("inner sql execute fail", K(ret), K(ctx_));
    } else {
      ret = ret_code;
      LOG_INFO("execute inner sql ret code", K(ret), K(task_id));
    }
  }
  return ret;
}

// 1. 启动事务
// 2. 删除删除3，4号表记录
// 3. 交换5号表记录
// 4. 结束事务
// 5. 交换新旧adapter
int ObVecIndexAsyncTask::execute_exchange()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx_) || OB_ISNULL(new_adapter_) || OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(ctx_), K(new_adapter_), KP(vec_idx_mgr_));
  } else {
    transaction::ObTxDesc *tx_desc = nullptr;
    oceanbase::transaction::ObTxReadSnapshot snapshot;
    oceanbase::transaction::ObTransService *txs = MTL(transaction::ObTransService *);
    const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;

    const int64_t data_table_id = new_adapter_->get_data_table_id();
    const int64_t snapshot_table_id = new_adapter_->get_snapshot_table_id();
    const uint64_t target_scn_version = ctx_->task_status_.target_scn_.get_val_for_sql();
    const ObTableSchema *data_schema = nullptr;
    const ObTableSchema *snapshot_schema = nullptr;

    if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id_, false/*is_for_read*/, timeout_us, tx_desc))) {
      LOG_WARN("fail to get tx_desc", K(ret));
    } else if (OB_ISNULL(tx_desc) || OB_ISNULL(txs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
    } else if (OB_FAIL(prepare_schema_and_snapshot(data_schema, snapshot_schema, data_table_id, snapshot_table_id, target_scn_version, snapshot))) {
      LOG_WARN("fail to prepare schema and snapshot", K(ret), K(ctx_));
    } else if (OB_FAIL(delete_inc_index_rows(tx_desc, snapshot, snapshot_schema->get_schema_version(), timeout_us))) {
      LOG_WARN("fail to delete inc index rows", K(ret), K(ctx_));
    } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_us, snapshot))) { // get new snapshot version to exchange table 5 rows
      LOG_WARN("fail to get snapshot", K(ret));
    } else if (OB_FAIL(exchange_snap_index_rows(*data_schema, *snapshot_schema, tx_desc, snapshot, timeout_us))) {
      LOG_WARN("fail to exchange snap index rows", K(ret), K(ctx_));
    }
    /* Warning!!!
    * In the process of loading data for a query, the query_lock is acquired first, followed by the adapter_map_lock.
    * Therefore, the order of these two locks must not be reversed;
    * otherwise, a deadlock could occur between the query and asynchronous tasks. */
    RWLock::WLockGuard query_lock_guard(old_adapter_->get_query_lock()); // lock for query before end trans
    RWLock::WLockGuard lock_guard(vec_idx_mgr_->get_adapter_map_lock());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, timeout_us))) {
      ret = tmp_ret;
      LOG_WARN("fail to end trans", K(ret), KPC(tx_desc));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(vec_idx_mgr_->replace_old_adapter(new_adapter_))) {
      LOG_WARN("failed to replace old adapter", K(ret));
    }
  }
  LOG_INFO("end execute_exchange", K(ret));
  return ret;
}

// 1. 删除5号表visible=false的行
// 2. 删除对应的lob_meta
int ObVecIndexAsyncTask::execute_clean()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx_) || OB_ISNULL(new_adapter_) || OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(ctx_), K(new_adapter_), KP(vec_idx_mgr_));
  } else {
    transaction::ObTxDesc *tx_desc = nullptr;
    oceanbase::transaction::ObTxReadSnapshot snapshot;
    oceanbase::transaction::ObTransService *txs = MTL(transaction::ObTransService *);
    const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
    const int64_t data_table_id = new_adapter_->get_data_table_id();
    const int64_t snapshot_table_id = new_adapter_->get_snapshot_table_id();
    const uint64_t target_scn_version = ctx_->task_status_.target_scn_.get_val_for_sql();
    const ObTableSchema *data_schema = nullptr;
    const ObTableSchema *snapshot_schema = nullptr;

    if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id_, false/*is_for_read*/, timeout_us, tx_desc))) {
      LOG_WARN("fail to get tx_desc", K(ret));
    } else if (OB_ISNULL(tx_desc) || OB_ISNULL(txs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
    } else if (OB_FAIL(prepare_schema_and_snapshot(data_schema, snapshot_schema, data_table_id, snapshot_table_id, target_scn_version, snapshot))) {
      LOG_WARN("fail to prepare schema and snapshot", K(ret), K(ctx_));
    } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_us, snapshot))) { // get new snapshot version to exchange table 5 rows
      LOG_WARN("fail to get snapshot", K(ret));
    } else if (OB_FAIL(clean_snap_index_rows(*data_schema, *snapshot_schema, tx_desc, snapshot, timeout_us))) {
      LOG_WARN("fail to delete inc index rows", K(ret), K(ctx_));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, timeout_us))) {
      ret = tmp_ret;
      LOG_WARN("fail to end trans", K(ret), KPC(tx_desc));
    }
  }
  LOG_INFO("end execute_clean", K(ret));
  return ret;
}

int ObVecIndexAsyncTask::prepare_schema_and_snapshot(
    const ObTableSchema *&data_schema,
    const ObTableSchema *&snapshot_schema,
    const int64_t data_table_id,
    const int64_t snap_table_id,
    const uint64_t task_snapshot_version,
    oceanbase::transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  transaction::ObTxSnapshot tx_snapshot;
  data_schema = nullptr;
  snapshot_schema = nullptr;

  if (data_table_id == OB_INVALID_ID || snap_table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_table_id), K(snap_table_id));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, snap_table_id, snapshot_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(snap_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version_))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id_));
  } else if (OB_ISNULL(snapshot_schema) || OB_ISNULL(data_schema) ||
      snapshot_schema->is_in_recyclebin() || data_schema->is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema not exist", K(ret), K(data_table_id), K(snap_table_id), KP(snapshot_schema), KP(data_schema));
  } else if (OB_FAIL(tx_snapshot.version_.convert_for_tx(task_snapshot_version))) {
    LOG_WARN("failed to convert for tx", KR(ret), K(ctx_));
  } else {
    snapshot.init_ls_read(ls_id_, tx_snapshot);
  }
  return ret;
}

int ObVecIndexAsyncTask::clean_snap_index_rows(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &snapshot_table_schema,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObAccessService *oas = MTL(ObAccessService*);
  storage::ObValueRowIterator row_iter;
  ObDMLBaseParam dml_param;
  ObTableScanIterator *table_scan_iter = nullptr;
  storage::ObStoreCtxGuard store_ctx_guard;
  storage::ObTableScanParam snap_scan_param;
  schema::ObTableParam snap_table_param(allocator_);
  share::schema::ObTableDMLParam table_dml_param(allocator_);
  common::ObNewRowIterator *snap_data_iter = nullptr;
  common::ObCollationType cs_type = CS_TYPE_INVALID;
  const uint64_t schema_version = data_table_schema.get_schema_version();

  ObSEArray<uint64_t, 4> all_column_ids;
  ObSEArray<uint64_t, 4> dml_column_ids;
  ObSEArray<uint64_t, 4> extra_column_idxs;

  if (OB_ISNULL(ctx_) || OB_ISNULL(tx_desc) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(tx_desc), KP(ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                            new_adapter_,
                                            snapshot.version(), //ctx_->task_status_.target_scn_
                                            INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                            allocator_,
                                            allocator_,
                                            snap_scan_param,
                                            snap_table_param,
                                            snap_data_iter))) {
    LOG_WARN("fail to read data table local tablet.", K(ret));
  } else if (OB_FAIL(get_snap_index_column_info(data_table_schema, snapshot_table_schema, all_column_ids, dml_column_ids, extra_column_idxs, cs_type))) {
    LOG_WARN("fail to get snap index column info", K(ret));
  } else if (OB_FAIL(table_dml_param.convert(&snapshot_table_schema, snapshot_table_schema.get_schema_version(), dml_column_ids))) { // need this?
    LOG_WARN("fail to convert table dml param.", K(ret));
  } else if (OB_FALSE_IT(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
  } else if (OB_FAIL(prepare_dml_del_row_iter(tx_desc, cs_type, table_scan_iter, extra_column_idxs, row_iter, snapshot))) {
    LOG_WARN("fail to prepare dml iter", K(ret));
  } else if (OB_FAIL(prepare_dml_param(dml_param, table_dml_param, store_ctx_guard, tx_desc, snapshot, schema_version, timeout_us))) {
    LOG_WARN("fail to prepare lob meta dml", K(ret));
  } else if (OB_FAIL(oas->delete_rows(ls_id_, new_adapter_->get_snap_tablet_id(), *tx_desc, dml_param, dml_column_ids, &row_iter, affected_rows))) {
    LOG_WARN("failed to delete rows from snapshot table", K(ret), K(ctx_));
  } else {
    LOG_INFO("print clean rows", K(affected_rows));
  }
  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVecIndexATaskUpdIterator::init() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(old_row_.init())) {
    LOG_WARN("fail to init old rows iter", K(ret));
  } else if (OB_FAIL(new_row_.init())) {
    LOG_WARN("fail to init new rows iter", K(ret));
  }
  return ret;
}

int ObVecIndexATaskUpdIterator::add_row(blocksstable::ObDatumRow &old_datum_row, blocksstable::ObDatumRow &new_datum_row) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(old_row_.add_row(old_datum_row))) {
    LOG_WARN("failed to add row to iter", K(ret));
  } else if (OB_FAIL(new_row_.add_row(new_datum_row))) {
    LOG_WARN("fail to init new rows iter", K(ret));
  }
  return ret;
}

int ObVecIndexATaskUpdIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (!got_old_row_) {
    got_old_row_ = true;
    if (OB_FAIL(old_row_.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next old row", K(ret));
      }
    }
  } else {
    got_old_row_ = false;
    if (OB_FAIL(new_row_.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next new row", K(ret));
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::get_snap_index_column_info(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &snapshot_table_schema,
    ObIArray<uint64_t> &all_column_ids,
    ObIArray<uint64_t> &dml_column_ids,
    ObIArray<uint64_t> &extra_column_idxs,
    common::ObCollationType &cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(snapshot_table_schema.get_all_column_ids(all_column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), K(data_table_schema));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); i++) {
    const ObColumnSchemaV2 *column_schema;
    if (OB_ISNULL(column_schema = data_table_schema.get_column_schema(all_column_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(ret), K(all_column_ids.at(i)));
    } else if (column_schema->is_vec_hnsw_vid_column()) {
      vector_vid_col_idx_ = i;
      if (!column_schema->is_nullable()) {
        ObString index_name;
        if (OB_FAIL(snapshot_table_schema.get_index_name(index_name))) {
          LOG_WARN("failed to get index name", K(ret));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_INFO("vector index created before 4.3.5.2 do not support vector index optimize task, please rebuild vector index.", K(ret), K(index_name));
        }
      } else if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_hidden_pk_column_id(all_column_ids.at(i))) {
      vector_vid_col_idx_ = i;
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_vector_column()) {
      vector_col_idx_ = i;
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_key_column()) {
      vector_key_col_idx_ = i;
      key_col_id_ = all_column_ids.at(i);
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_data_column()) {
      vector_data_col_idx_ = i;
      data_col_id_ = all_column_ids.at(i);
      cs_type = column_schema->get_collation_type();
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_visible_column()) {
      vector_visible_col_idx_ = i;
      visible_col_id_ = all_column_ids.at(i);
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else { // set extra column id
      if (OB_FAIL(extra_column_idxs.push_back(i))) {
        LOG_WARN("failed to push back extra column idx", K(ret), K(i));
      } else if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    }
  } // end for.
  if (OB_SUCC(ret)) {
    if (vector_vid_col_idx_ == -1 || vector_col_idx_ == -1 || vector_key_col_idx_ == -1 || vector_data_col_idx_ == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_col_idx_), K(vector_vid_col_idx_),
              K(vector_key_col_idx_), K(vector_data_col_idx_), K(all_column_ids));
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::prepare_dml_param(
    ObDMLBaseParam &dml_param,
    share::schema::ObTableDMLParam &table_dml_param,
    storage::ObStoreCtxGuard &store_ctx_guard,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t schema_version,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);

  if (schema_version <= 0 || tenant_schema_version_ <= 0 || !snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version), K(tenant_schema_version_), K(snapshot.is_valid()));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed", K(ret), KP(oas));
  } else if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null", K(ret), KP(tx_desc));
  } else {
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.write_flag_.set_is_insert_up();
    dml_param.table_param_ = &table_dml_param;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout_us;
    dml_param.snapshot_.assign(snapshot);
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = schema_version;
    dml_param.tenant_schema_version_ = tenant_schema_version_;
    dml_param.dml_allocator_ = &allocator_;
    if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout_us, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::delete_inc_index_rows(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t schema_version,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam dml_param;
  share::schema::ObTableDMLParam table_dml_param(allocator_);
  storage::ObStoreCtxGuard store_ctx_guard;
  if (OB_ISNULL(ctx_) || OB_ISNULL(tx_desc) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(tx_desc), KP(ctx_));
  } else if (OB_FAIL(prepare_dml_param(dml_param, table_dml_param, store_ctx_guard, tx_desc, snapshot, schema_version, timeout_us))) {
    LOG_WARN("fail to prepare dml param", K(ret), K(ctx_));
  } else if (OB_FAIL(delete_incr_table_data(*new_adapter_, dml_param, tx_desc))) {
    LOG_WARN("failed to delete rows from snapshot table", K(ret), K(ctx_->task_status_.tablet_id_));
  }
  return ret;
}

int ObVecIndexAsyncTask::exchange_snap_index_rows(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &snapshot_table_schema,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObAccessService *oas = MTL(ObAccessService*);
  ObVecIndexATaskUpdIterator row_iter;
  ObDMLBaseParam dml_param;
  ObTableScanIterator *table_scan_iter = nullptr;
  storage::ObStoreCtxGuard store_ctx_guard;
  storage::ObTableScanParam snap_scan_param;
  schema::ObTableParam snap_table_param(allocator_);
  share::schema::ObTableDMLParam table_dml_param(allocator_);
  common::ObNewRowIterator *snap_data_iter = nullptr;
  common::ObCollationType cs_type = CS_TYPE_INVALID;
  const uint64_t schema_version = data_table_schema.get_schema_version();

  ObSEArray<uint64_t, 4> all_column_ids;
  ObSEArray<uint64_t, 4> dml_column_ids;
  ObSEArray<uint64_t, 1> upd_column_ids;
  ObSEArray<uint64_t, 4> extra_column_idxs;

  if (OB_ISNULL(ctx_) || OB_ISNULL(tx_desc) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(tx_desc), KP(ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                            new_adapter_,
                                            snapshot.version(), //ctx_->task_status_.target_scn_
                                            INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                            allocator_,
                                            allocator_,
                                            snap_scan_param,
                                            snap_table_param,
                                            snap_data_iter))) {
    LOG_WARN("fail to read data table local tablet.", K(ret));
  } else if (OB_FAIL(get_snap_index_column_info(data_table_schema, snapshot_table_schema, all_column_ids, dml_column_ids, extra_column_idxs, cs_type))) {
    LOG_WARN("fail to get snap index column info", K(ret));
  } else if (vector_visible_col_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector visible col idx", K(ret), K(vector_visible_col_idx_));
  } else if (OB_FAIL(upd_column_ids.push_back(all_column_ids.at(vector_visible_col_idx_)))) {
    LOG_WARN("fail to push back update column id", K(ret), K(vector_visible_col_idx_), K(all_column_ids));
  } else if (OB_FAIL(table_dml_param.convert(&snapshot_table_schema, snapshot_table_schema.get_schema_version(), dml_column_ids))) { // need this?
    LOG_WARN("fail to convert table dml param.", K(ret));
  } else if (OB_FALSE_IT(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
  } else if (OB_FAIL(prepare_dml_udp_row_iter(table_scan_iter, extra_column_idxs, row_iter))) {
    LOG_WARN("fail to prepare dml iter", K(ret));
  } else if (OB_FAIL(prepare_dml_param(dml_param, table_dml_param, store_ctx_guard, tx_desc, snapshot, schema_version, timeout_us))) {
    LOG_WARN("fail to prepare lob meta dml", K(ret));
  } else if (OB_FAIL(oas->update_rows(ls_id_, new_adapter_->get_snap_tablet_id(),
      *tx_desc, dml_param, dml_column_ids, upd_column_ids, &row_iter, affected_rows))) {
    LOG_WARN("fail to update_rows", K(ret), K(ctx_), K(dml_column_ids), K(upd_column_ids));
  } else {
    LOG_WARN("print update rows count", K(affected_rows));
  }
  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVecIndexAsyncTask::construct_vector_row(
    blocksstable::ObDatumRow *in_datum_row,
    ObIArray<uint64_t> &extra_column_idxs,
    const int64_t in_key_col_idx,
    const int64_t in_data_col_idx,
    const int64_t in_visible_col_idx,
    blocksstable::ObDatumRow &out_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in_datum_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(in_datum_row));
  } else if (invalid_snapshot_column_ids()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column idx", K(ret),
      K(vector_col_idx_), K(vector_vid_col_idx_), K(vector_key_col_idx_), K(vector_data_col_idx_), K(vector_visible_col_idx_));
  } else {
    out_row.storage_datums_[vector_key_col_idx_].set_string(in_datum_row->storage_datums_[in_key_col_idx].get_string());
    out_row.storage_datums_[vector_data_col_idx_].set_string(in_datum_row->storage_datums_[in_data_col_idx].get_string());
    out_row.storage_datums_[vector_visible_col_idx_].set_bool(in_datum_row->storage_datums_[in_visible_col_idx].get_bool());
    out_row.storage_datums_[vector_data_col_idx_].set_has_lob_header();
    out_row.storage_datums_[vector_vid_col_idx_].set_null();
    out_row.storage_datums_[vector_col_idx_].set_null();
    // set extra column default value
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs.count(); i++) {
      if (extra_column_idxs.at(i) == vector_key_col_idx_ ||
          extra_column_idxs.at(i) == vector_data_col_idx_ ||
          extra_column_idxs.at(i) == vector_vid_col_idx_ ||
          extra_column_idxs.at(i) == vector_col_idx_ ||
          extra_column_idxs.at(i) == vector_visible_col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs.at(i)),
          K(vector_key_col_idx_), K(vector_data_col_idx_),
          K(vector_vid_col_idx_), K(vector_col_idx_), K(vector_visible_col_idx_));
      } else {
        out_row.storage_datums_[extra_column_idxs.at(i)].set_null();
      }
    }
  }
  LOG_DEBUG("print construct out row", K(ret), K(out_row), K(*in_datum_row), K(vector_key_col_idx_), K(vector_data_col_idx_), K(vector_visible_col_idx_));
  return ret;
}

int ObVecIndexAsyncTask::prepare_dml_udp_row_iter(
    ObTableScanIterator *table_scan_iter,
    ObIArray<uint64_t> &extra_column_idxs,
    ObVecIndexATaskUpdIterator &row_iter)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_column_count = 5;
  const int64_t tmp_key_col_idx = 0;
  const int64_t tmp_data_col_idx = 1;
  const int64_t tmp_visible_col_idx = 2;
  int64_t loop_cnt = 0;

  if (OB_ISNULL(table_scan_iter) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table scan iter", K(ret), KP(table_scan_iter), KP(ctx_));
  } else if (OB_FAIL(row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  }
  HEAP_VARS_2((blocksstable::ObDatumRow, tmp_old_row, tenant_id_),
              (blocksstable::ObDatumRow, tmp_new_row, tenant_id_)) {
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_old_row.init(snapshot_column_count + extra_column_idxs.count()))) {
    LOG_WARN("fail to init old tmp row", K(ret), K(tmp_old_row));
  } else if (OB_FAIL(tmp_new_row.init(snapshot_column_count + extra_column_idxs.count()))) {
    LOG_WARN("fail to init new tmp row", K(ret), K(tmp_old_row));
  }
  while (OB_SUCC(ret)) {
    blocksstable::ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed.", K(ret));
      }
    } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get row invalid.", K(ret));
    } else if (datum_row->get_column_count() < 3) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
    } else if (OB_FAIL(construct_vector_row(datum_row, extra_column_idxs, tmp_key_col_idx,
                                            tmp_data_col_idx, tmp_visible_col_idx, tmp_old_row))) {
      LOG_WARN("fail to construct vector row", K(ret), K(ctx_));
    } else {
      bool is_visible = tmp_old_row.storage_datums_[vector_visible_col_idx_].get_bool();
      LOG_DEBUG("old row is visible", K(ret), K(is_visible), K(tmp_old_row.storage_datums_[vector_key_col_idx_]));
      // change visible column
      if (OB_FAIL(tmp_new_row.deep_copy(tmp_old_row, allocator_))) {
        LOG_WARN("fail to copy tmp_old_row", K(ret), K(ctx_));
      } else {
        tmp_new_row.storage_datums_[vector_visible_col_idx_].set_bool(!is_visible);
      }
      if (OB_SUCC(ret) && OB_FAIL(row_iter.add_row(tmp_old_row, tmp_new_row))) {
        LOG_WARN("failed to add row to iter", K(ret));
      }
      tmp_old_row.reuse();
      tmp_new_row.reuse();
    }
    CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
  } // end while
  } // end smart var
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexAsyncTask::prepare_dml_del_row_iter(
    transaction::ObTxDesc *tx_desc,
    common::ObCollationType cs_type,
    ObTableScanIterator *table_scan_iter,
    ObIArray<uint64_t> &extra_column_idxs,
    storage::ObValueRowIterator &row_iter,
    transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  const int64_t snapshot_column_count = 5;
  const int64_t tmp_key_col_idx = 0;
  const int64_t tmp_data_col_idx = 1;
  const int64_t tmp_visible_col_idx = 2;

  int64_t loop_cnt = 0;
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob mngr is null", K(ret));
  } else if (OB_ISNULL(table_scan_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table scan iter", K(ret));
  } else if (OB_ISNULL(new_adapter_) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KP(new_adapter_), KP(ctx_));
  } else if (OB_FAIL(row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  }
  HEAP_VAR(blocksstable::ObDatumRow, old_row, tenant_id_) {
    if (OB_SUCC(ret) && OB_FAIL(old_row.init(snapshot_column_count + extra_column_idxs.count()))) {
      LOG_WARN("fail to init datum row", K(ret), K(old_row));
    }
    while (OB_SUCC(ret)) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (datum_row->get_column_count() < 3) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
      } else if (!datum_row->storage_datums_[tmp_visible_col_idx].get_bool()) { // remove visible = false old row
        LOG_DEBUG("remove old row lob meta record", K(*datum_row));
        ObString data = datum_row->storage_datums_[tmp_data_col_idx].get_string();
        ObLobLocatorV2 lob(data, data.length() > 0);
        if (lob.has_inrow_data()) {
          // delete inrow lob no need to use the lob manager
        } else {
          ObLobAccessParam lob_param;
          lob_param.tx_desc_ = tx_desc;
          lob_param.tablet_id_ = new_adapter_->get_data_tablet_id(); // data tablet id
          if (!lob.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid src lob locator.", K(ret));
          } else if (OB_FAIL(lob_mngr->build_lob_param(lob_param, allocator_, cs_type, 0, UINT64_MAX, timeout_us, lob))) {
            LOG_WARN("fail to build lob param.", K(ret));
          } else if (OB_FAIL(lob_param.snapshot_.assign(snapshot))) {
            LOG_WARN("fail to assign snapshot", K(ret), K(snapshot));
          } else if (OB_FAIL(lob_mngr->erase(lob_param))) {
            LOG_WARN("lob meta row delete failed.", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(construct_vector_row(datum_row, extra_column_idxs, tmp_key_col_idx,
                                                tmp_data_col_idx, tmp_visible_col_idx, old_row))) {
          LOG_WARN("fail to construct vector row", K(ret));
        } else if (OB_FAIL(row_iter.add_row(old_row))) {
          LOG_WARN("failed to add row to iter", K(ret));
        }
        old_row.reuse();
      }
      CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
    } // end while
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

bool ObVecIndexAsyncTask::check_task_satisfied_memory_limited(ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  bool check_result = true;
  const int64_t snapshot_table_id = adaptor.get_snapshot_table_id();
  common::ObAddr addr;

  if (tenant_id_ != OB_INVALID_TENANT_ID && snapshot_table_id != OB_INVALID_ID) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;

    int64_t current_incr_count = 0;
    int64_t current_snapshot_count = 0;
    int64_t estimate_row_count = 0;
    // inc
    // tips: When there are many delete operations in inc data, the estimated final result may deviate significantly from the actual result.

    share::ObLocationService *location_service = nullptr;
    int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
    const int64_t retry_interval_us = 200 * 1000; // 200ms
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("location_cache is null", K(ret), KP(location_service));
    } else if (OB_FAIL(location_service->get_leader_with_retry_until_timeout(GCONF.cluster_id,
      tenant_id_, ls_id_, addr, rpc_timeout, retry_interval_us))) {
      LOG_WARN("fail to get ls locaiton leader", K(ret), K(tenant_id_), K(ls_id_));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(adaptor.get_inc_index_row_cnt(current_incr_count))) {
      LOG_WARN("fail to get incr index number", K(ret));
    } else if (OB_FAIL(adaptor.get_snap_index_row_cnt(current_snapshot_count))) {
      LOG_WARN("fail to get snap index number", K(ret));
    } else if (OB_FALSE_IT(estimate_row_count = current_incr_count + current_snapshot_count)) {
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, snapshot_table_id, index_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(snapshot_table_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("error unexpected, index table schema is null", K(ret), K(snapshot_table_id));
    } else if (!index_schema->can_read_index()) {
      check_result = false;
      LOG_INFO("snapshot data table is not avaliable now", K(ret));
    } else if (!ObVectorIndexUtil::check_vector_index_memory(schema_guard, *index_schema, addr, tenant_id_, estimate_row_count)) {
      check_result = false;
      LOG_INFO("current vsag memory maybe is not satisfy to execute async task", K(ret), K(snapshot_table_id));
    }
  }
  return check_result;
}

int ObVecIndexAsyncTask::process_data_for_index(ObPluginVectorIndexAdaptor &adaptor, ObPluginVectorIndexAdaptor &old_adaptor)
{
  int ret = OB_SUCCESS;

  LOG_INFO("start optimize vector index");

  int64_t dim = 0;
  float *vectors = nullptr;
  int64_t *vids = nullptr;
  int extra_column_count = 0;
  ObVecExtraInfoObj *out_extra_obj = nullptr;
  ObVidBound vid_bound;
  schema::ObTableParam vid_table_param(allocator_);
  schema::ObTableParam data_table_param(allocator_);
  //  hnsw:    [rowkey(extra_colun)]  [vid]              rowkey_vid_table.
  common::ObNewRowIterator *vid_id_iter = nullptr;
  //  hnsw:    [vector]  [rowkey(extra_colun)]           data_table.
  //  hybrid:  [vid]                  [vector]           embedded_table.
  common::ObNewRowIterator *data_iter = nullptr;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  const uint32_t VEC_INDEX_HNSWSQ_BUILD_COUNT_THRESHOLD = 10000;
  // (dim: 128 + vals: 128) * 4 = 1024
  const uint32_t VEC_INDEX_IPIVF_BUILD_COUNT_THRESHOLD = 10000 * 1024;
  uint32_t current_count = 0;
  int64_t loop_cnt = 0; // check task is cancel
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  const bool is_hybrid_index = adaptor.is_hybrid_index();
  const schema::ObIndexType vid_table_type = INDEX_TYPE_VEC_ROWKEY_VID_LOCAL;
  const schema::ObIndexType data_table_type = is_hybrid_index? INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL : INDEX_TYPE_IS_NOT;
  uint32_t *sparse_byte_lens = nullptr;
  SMART_VARS_2((storage::ObTableScanParam, vid_id_scan_param),
               (storage::ObTableScanParam, data_scan_param)) {
    if (OB_FAIL(adaptor.get_dim(dim))) {
      LOG_WARN("get dim failed", K(ret));
    } else if (adaptor.is_sparse_vector_index_type()) {
      if (OB_ISNULL(sparse_byte_lens = static_cast<uint32_t *>(allocator_.alloc(sizeof(uint32_t) * VEC_INDEX_IPIVF_BUILD_COUNT_THRESHOLD)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc sparse_byte_lens", K(ret));
      } else if (OB_ISNULL(vectors = static_cast<float *>(allocator_.alloc(sizeof(char) * VEC_INDEX_IPIVF_BUILD_COUNT_THRESHOLD)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc vectors for sparse", K(ret));
      }
    } else if (OB_ISNULL(vectors = static_cast<float *>(allocator_.alloc(sizeof(float) * dim * VEC_INDEX_HNSWSQ_BUILD_COUNT_THRESHOLD)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vectors for dense", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(vids = static_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * VEC_INDEX_HNSWSQ_BUILD_COUNT_THRESHOLD)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc new mem.", K(ret));
    } else if (adaptor.get_is_need_vid() && !is_hybrid_index && OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                  &adaptor,
                                  ctx_->task_status_.target_scn_,
                                  vid_table_type,
                                  allocator_,
                                  allocator_,
                                  vid_id_scan_param,
                                  vid_table_param,
                                  vid_id_iter))) {
      LOG_WARN("failed to read vid id table local tablet.", K(ret));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                        &adaptor,
                                        ctx_->task_status_.target_scn_,
                                        data_table_type,
                                        allocator_,
                                        allocator_,
                                        data_scan_param,
                                        data_table_param,
                                        data_iter,
                                        nullptr,
                                        is_hybrid_index))) {
      LOG_WARN("failed to read data table local tablet.", K(ret));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::get_extra_column_count(adaptor, extra_column_count))) {
      LOG_WARN("failed to get extra column count", K(ret), K(adaptor));
    } else if (extra_column_count > 0) {
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(sizeof(ObVecExtraInfoObj) * extra_column_count * VEC_INDEX_HNSWSQ_BUILD_COUNT_THRESHOLD)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(extra_column_count));
      } else if (OB_FALSE_IT(out_extra_obj = new (buf) ObVecExtraInfoObj[extra_column_count * VEC_INDEX_HNSWSQ_BUILD_COUNT_THRESHOLD])) {
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      ObTableScanIterator *vid_scan_iter = nullptr;
      ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(data_iter);
      int32_t data_table_rowkey_count = is_hybrid_index? data_table_param.get_output_projector().count() - 2: vid_table_param.get_output_projector().count() - 1;
      int64_t current_incr_count = 0;
      int64_t current_snapshot_count = 0;
      if (adaptor.get_is_need_vid()) {
        vid_scan_iter = static_cast<ObTableScanIterator *>(vid_id_iter);
      } else {
        data_table_rowkey_count = 1;  // pk_increrment
      }
      int32_t vid_column_pos = is_hybrid_index && !adaptor.get_is_need_vid()? 0: data_table_rowkey_count;
      if (OB_ISNULL(table_scan_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table scan iter", K(ret));
      } else if (OB_FAIL(old_adaptor.get_inc_index_row_cnt(current_incr_count))) {
        LOG_WARN("fail to get incr index number", K(ret));
      } else if (OB_FAIL(old_adaptor.get_snap_index_row_cnt(current_snapshot_count))) {
        LOG_WARN("fail to get snap index number", K(ret));
      } else {
        // tips: When there are many delete operations in inc data, the estimated final result may deviate significantly from the actual result.
        ctx_->task_status_.progress_info_.vec_opt_status_ = OB_VECTOR_ASYNC_OPT_INSERTING;
        ctx_->task_status_.progress_info_.start_progress(current_incr_count + current_snapshot_count);
      }
      // Note: The actual insert rows may be greater than the estimated rows if keep inserting data while async task is running.
      char *curr_vector_ptr = (char *)vectors;
      uint32_t curr_total_length = 0;
      while (OB_SUCC(ret)) {
        blocksstable::ObDatumRow *datum_vid = nullptr;
        blocksstable::ObDatumRow *datum_row = nullptr;
        blocksstable::ObDatumRow *vid_datum = nullptr;
        if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed.", K(ret));
          }
        } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row invalid.", K(ret));
        } else if (datum_row->get_column_count() < extra_column_count + 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
        } else if (is_hybrid_index || !adaptor.get_is_need_vid()) {
        } else if (OB_ISNULL(vid_scan_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null table scan iter", K(ret));
        } else if (OB_FAIL(vid_scan_iter->get_next_row(datum_vid))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed.", K(ret));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data table row count mismatched", K(ret));
          }
        } else if (OB_ISNULL(datum_vid) || !datum_vid->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row invalid.", K(ret));
        } else if (datum_vid->get_column_count() != data_table_rowkey_count + 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row column cnt invalid.", K(ret), K(datum_vid->get_column_count()));
        }
        vid_datum = is_hybrid_index || !adaptor.get_is_need_vid()? datum_row: datum_vid;
        if (OB_FAIL(ret)) {
        } else {
          //  hnsw:    [vector]                  [rowkey(extra_column)]
          //  hybrid:  [rowkey(extra_column)]    [vid]                    [vector]
          //  hybrid without vid_rowkey_tbl:  [pk(vid)] [vector]
          if (extra_column_count > 0) {
            const ObIArray<int32_t> &out_idxs = data_table_param.get_output_projector();
            const ObIArray<share::schema::ObColumnParam *> *out_col_param =
                data_scan_param.table_param_->get_read_info().get_columns();
            const int extra_column_offset = is_hybrid_index? 0: 1;
            if (OB_ISNULL(out_col_param) || out_idxs.count() < extra_column_count + extra_column_offset) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column count not equal.", K(ret), KP(out_col_param), K(out_idxs), K(extra_column_count));
            }
            for (int i = 0; OB_SUCC(ret) && i < extra_column_count; ++i) {
              ObObj tmp_obj;
              if (out_idxs.at(i + extra_column_offset) >= out_col_param->count()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column count not equal.", K(ret), KP(out_col_param), K(out_idxs), K(i));
              } else {
                ObObjMeta meta_type = out_col_param->at(out_idxs.at(i + extra_column_offset))->get_meta_type();
                const ObDatum &extra_datum = datum_row->storage_datums_[i + extra_column_offset];
                if (OB_FALSE_IT(out_extra_obj[current_count * extra_column_count + i].reset())) {
                } else if (OB_FAIL(out_extra_obj[current_count * extra_column_count + i].from_datum(
                               extra_datum, meta_type, &allocator_))) {
                  LOG_WARN("failed to from obj.", K(ret), K(extra_datum), K(meta_type), K(i));
                }
              }
            }
          }
          ObString vector_str;
          float *vector_ptr = nullptr;
          const int64_t vec_col_idx = 0;  // ObPluginVectorIndexUtils::read_local_tablet get from INDEX_TYPE_IS_NOT only output one vector column
          blocksstable::ObDatumRow *vector_row = datum_row;
          int32_t vector_column_pos = is_hybrid_index? data_table_rowkey_count + 1: vec_col_idx;
          if (is_hybrid_index && !adaptor.get_is_need_vid()) {
            //  hybrid without vid_rowkey_tbl:  [pk(vid)] [part_key] [vector]
            vector_column_pos = vector_row->get_column_count() - 1;
          }
          if (OB_FAIL(ret)) {
          } else if (vector_row->storage_datums_[vector_column_pos].is_null() || vector_row->storage_datums_[vector_column_pos].is_nop()) { // skip null row
          } else if (FALSE_IT(vector_str = vector_row->storage_datums_[vector_column_pos].get_string())) {
          } else if (vector_str.length() == 0) {  // skip null row
          } else if (!adaptor.is_sparse_vector_index_type() && vector_str.length() != dim * sizeof(float)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid string.", K(ret), K(vector_str), K(dim));
          } else if (OB_ISNULL(vector_ptr = reinterpret_cast<float *>(vector_str.ptr()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get float vector.", K(ret));
          } else {
            if (adaptor.is_sparse_vector_index_type()) {
              curr_total_length += vector_str.length();
              if(curr_total_length <= VEC_INDEX_IPIVF_BUILD_COUNT_THRESHOLD) {
                sparse_byte_lens[current_count] = vector_str.length();
                MEMCPY(curr_vector_ptr, vector_str.ptr(), vector_str.length());
                curr_vector_ptr += vector_str.length();
              }
            } else {
              for (int j = 0; OB_SUCC(ret) && j < dim; j++) {
                vectors[current_count * dim + j] = vector_ptr[j];
              }
            }
            vids[current_count] = vid_datum->storage_datums_[vid_column_pos].get_int();
            vid_bound.set_vid(vid_datum->storage_datums_[vid_column_pos].get_int());
            current_count += 1;
            if (adaptor.is_sparse_vector_index_type()) {
              if (curr_total_length >= VEC_INDEX_IPIVF_BUILD_COUNT_THRESHOLD) {
                if (OB_FAIL(adaptor.add_snap_index(
                        vectors, vids, out_extra_obj, extra_column_count, current_count, sparse_byte_lens))) {
                  LOG_WARN("failed to add sparse snap index", K(ret), K(vectors), K(vids), K(current_count));
                } else {
                  ctx_->task_status_.progress_info_.update_progress(current_count);
                  current_count = 0;
                  curr_vector_ptr = (char *)vectors;
                  curr_total_length = 0;
                  // copy next data
                  sparse_byte_lens[current_count] = vector_str.length();
                  MEMCPY(curr_vector_ptr, vector_str.ptr(), vector_str.length());
                  curr_vector_ptr += vector_str.length();
                }
              }
            } else {
              if (current_count >= VEC_INDEX_HNSWSQ_BUILD_COUNT_THRESHOLD) {
                if (OB_FAIL(adaptor.add_snap_index(vectors, vids, out_extra_obj, extra_column_count, current_count))) {
                  LOG_WARN("failed to add snap index", K(ret), K(vectors), K(vids), K(current_count));
                } else {
                  ctx_->task_status_.progress_info_.update_progress(current_count);
                  current_count = 0;
                }
              }
            }

          }
        }
        DEBUG_SYNC(CANCEL_VEC_TASK_ADD_SNAP_INDEX);
        CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(vid_id_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(vid_id_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    vid_id_iter = nullptr;
    if (OB_NOT_NULL(data_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert data_iter failed", K(ret));
      }
    }
    data_iter = nullptr;
  }
  if (OB_SUCC(ret) && current_count > 0) {
    if (adaptor.is_sparse_vector_index_type()) {
      if (OB_FAIL(adaptor.add_snap_index(vectors, vids, out_extra_obj, extra_column_count, current_count, sparse_byte_lens))) {
        LOG_WARN("failed to build sparse snap index", K(ret), K(vectors), K(vids));
      } else {
        ctx_->task_status_.progress_info_.update_progress(current_count);
      }
    } else {
      if (OB_FAIL(adaptor.add_snap_index(vectors, vids, out_extra_obj, extra_column_count, current_count))) {
        LOG_WARN("failed to build snap index", K(ret), K(vectors), K(vids));
      } else {
        ctx_->task_status_.progress_info_.update_progress(current_count);
      }
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::optimize_vector_index(ObPluginVectorIndexAdaptor &adaptor, ObPluginVectorIndexAdaptor &old_adaptor)
{
  int ret = OB_SUCCESS;
  transaction::ObTxDesc *tx_desc = nullptr;
  oceanbase::transaction::ObTxReadSnapshot snapshot;
  bool trans_start = false;
  oceanbase::transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id_, false/*is_for_read*/, timeout_us, tx_desc))) {
    LOG_WARN("fail to get tx_desc", K(ret));
  } else if (FALSE_IT(trans_start = true)) {
  } else if (OB_ISNULL(tx_desc) || OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_us, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret));
  } else if (FALSE_IT(ctx_->task_status_.target_scn_ = snapshot.version())) {
  } else if (OB_FAIL(process_data_for_index(adaptor, old_adaptor))) {
    LOG_WARN("fail to process data for index", K(ret), K(adaptor));
  }

  // refresh snapshot table data.
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(ctx_->task_status_.progress_info_.vec_opt_status_ = OB_VECTOR_ASYNC_OPT_SERIALIZE)) {
  } else if (OB_FAIL(refresh_snapshot_index_data(adaptor, tx_desc, snapshot))) {
    LOG_WARN("failed to refresh snapshot index data", K(ret));
  } else if (OB_FAIL(adaptor.renew_single_snap_index(adaptor.get_snap_index_type() == VIAT_HNSW_BQ))) {
    LOG_WARN("fail to renew single snap index", K(ret));
  }
  /* Warning!!!
  * In the process of loading data for a query, the query_lock is acquired first, followed by the adapter_map_lock.
  * Therefore, the order of these two locks must not be reversed;
  * otherwise, a deadlock could occur between the query and asynchronous tasks. */
  RWLock::WLockGuard query_lock_guard(old_adapter_->get_query_lock()); // lock for query before end trans
  RWLock::WLockGuard lock_guard(vec_idx_mgr_->get_adapter_map_lock());
  int tmp_ret = OB_SUCCESS;
  if (trans_start && OB_SUCCESS != (tmp_ret = ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, timeout_us))) {
    ret = tmp_ret;
    LOG_WARN("fail to end trans", K(ret), KPC(tx_desc));
  }
  if (OB_SUCC(ret)) {
    ctx_->task_status_.progress_info_.vec_opt_status_ = OB_VECTOR_ASYNC_OPT_REPLACE;
    if (OB_FAIL(vec_idx_mgr_->replace_old_adapter(&adaptor))) {
      LOG_WARN("failed to replace old adapter", K(ret));
    }
  }

  LOG_INFO("end optimize vector index", K(ret), K(ctx_));

  return ret;
}

int ObVecIndexAsyncTask::refresh_snapshot_index_data(ObPluginVectorIndexAdaptor &adaptor, transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ctx.ls_id_ = ls_id_;
  ctx.data_tablet_id_ = adaptor.get_data_tablet_id();

 // get lob tablet id
  HEAP_VARS_3((ObLSHandle, ls_handle), (ObTabletHandle, data_tablet_handle), (ObTabletBindingMdsUserData, ddl_data))
  {
    ObLSService *ls_service = nullptr;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(ls_service->get_ls(ctx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(ctx.ls_id_));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", K(ret));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(ctx.data_tablet_id_, data_tablet_handle))) {
      LOG_WARN("fail to get tablet handle", K(ret), K(ctx.data_tablet_id_));
    } else if (OB_FAIL(data_tablet_handle.get_obj()->get_ddl_data(ddl_data))) {
      LOG_WARN("failed to get ddl data from tablet", K(ret), K(data_tablet_handle));
    } else {
      ctx.lob_meta_tablet_id_ = ddl_data.lob_meta_tablet_id_;
      ctx.lob_piece_tablet_id_ = ddl_data.lob_piece_tablet_id_;
    }
  }

  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema;
  const ObTableSchema *snapshot_table_schema;
  int64_t lob_inrow_threshold;
  ObAccessService *oas = MTL(ObAccessService *);
  share::schema::ObTableDMLParam table_dml_param(allocator_);
  share::schema::ObTableDMLParam table_delete_dml_param(allocator_);
  ObDMLBaseParam dml_param;
  ObSEArray<uint64_t, 4> all_column_ids;
  ObSEArray<uint64_t, 4> dml_column_ids;
  ObSEArray<uint64_t, 4> delete_column_ids;
  storage::ObStoreCtxGuard store_ctx_guard;
  storage::ObValueRowIterator row_iter;
  storage::ObValueRowIterator delete_row_iter;
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObSEArray<uint64_t, 4> extra_column_idxs;
  int64_t snapshot_column_count = 4;    // key data vector vid
  common::ObCollationType cs_type = CS_TYPE_INVALID;
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  int64_t timeout = ObTimeUtility::fast_current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  int64_t loop_cnt = 0;
  if OB_FAIL(ret) {
  } else {
    HEAP_VARS_2((storage::ObTableScanParam, snap_scan_param), (schema::ObTableParam, snap_table_param, allocator_)) {
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, adaptor.get_snapshot_table_id(), snapshot_table_schema))) {
        LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(adaptor.get_snapshot_table_id()));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, adaptor.get_data_table_id(), data_table_schema))) {
        LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(adaptor.get_data_table_id()));
      } else if (OB_ISNULL(snapshot_table_schema) || snapshot_table_schema->is_in_recyclebin() || OB_ISNULL(data_table_schema) || data_table_schema->is_in_recyclebin()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema not exist", K(ret), K(adaptor.get_snapshot_table_id()), K(adaptor.get_data_table_id()),
          KP(snapshot_table_schema), KP(data_table_schema));
      } else if (FALSE_IT(lob_inrow_threshold = snapshot_table_schema->get_lob_inrow_threshold())) {
      } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                            &adaptor,
                                            ctx_->task_status_.target_scn_,
                                            INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                            allocator_,
                                            allocator_,
                                            snap_scan_param,
                                            snap_table_param,
                                            snap_data_iter))) {
          LOG_WARN("failed to read data table local tablet.", K(ret));
      } else if (OB_FAIL(get_snap_index_column_info(*data_table_schema, *snapshot_table_schema, all_column_ids, dml_column_ids, extra_column_idxs, cs_type))) {
        LOG_WARN("fail to get snap index column info", K(ret));
      } else if (OB_FAIL(table_dml_param.convert(snapshot_table_schema, snapshot_table_schema->get_schema_version(), dml_column_ids))) {
        LOG_WARN("failed to convert table dml param.", K(ret));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, dml_param.tenant_schema_version_))) {
        LOG_WARN("failed to get schema version", K(ret));
      } else if (OB_FAIL(delete_column_ids.push_back(key_col_id_))) {
        LOG_WARN("fail to push back column id", K(ret), K(key_col_id_));
      } else if (OB_FAIL(table_delete_dml_param.convert(snapshot_table_schema, snapshot_table_schema->get_schema_version(), dml_column_ids))) {
        LOG_WARN("failed to convert table dml param.", K(ret));
      }
      // get actual snapshot column cnt
      snapshot_column_count = dml_column_ids.count() - extra_column_idxs.count();
      // insert data to snapshot index table.
      ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter);
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
      } else if (OB_FAIL(get_old_snapshot_data(adaptor, tx_desc, snapshot_column_count, cs_type, extra_column_idxs, table_scan_iter, delete_row_iter, snapshot))) {
        LOG_WARN("failed to get old snapshot data", K(ret));
      } else if (OB_ISNULL(adaptor.get_snap_data_()) || !adaptor.get_snap_data_()->is_inited()) {  // adaptor created by vector index async task, there won't be access from other threads.
        LOG_INFO("data table is empty, won't create snapshot index");
      } else {
        ObHNSWSerializeCallback callback;
        ObOStreamBuf::Callback cb = callback;
        ObHNSWSerializeCallback::CbParam param;
        param.vctx_ = &ctx;
        param.allocator_ = &allocator_;
        param.tmp_allocator_ = &allocator_;
        param.lob_inrow_threshold_ = lob_inrow_threshold;
        param.timeout_ = timeout;
        param.snapshot_ = &snapshot;
        param.tx_desc_ = tx_desc;
        ObVectorIndexAlgorithmType index_type = VIAT_MAX;

        if (OB_FAIL(adaptor.set_snapshot_key_prefix(adaptor.get_snap_tablet_id().id(), ctx_->task_status_.target_scn_.get_val_for_inner_table_field(), ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH))) {
          LOG_WARN("failed to set snapshot key prefix", K(ret), K(adaptor.get_snap_tablet_id().id()), K(ctx_->task_status_.target_scn_.get_val_for_inner_table_field()));
        } else if (OB_FAIL(adaptor.check_snap_hnswsq_index())) {
          LOG_WARN("failed to check snap hnswsq index", K(ret));
        } else if (OB_FAIL(adaptor.serialize(&allocator_, param, cb))) {
          LOG_WARN("fail to do vsag serialize", K(ret));
        } else if (OB_FAIL(row_iter.init())) {
          LOG_WARN("fail to init row iter", K(ret));
        } else if (OB_FALSE_IT(index_type = adaptor.get_snap_index_type())) {
        } else if (index_type >= VIAT_MAX) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get index type invalid.", K(ret), K(index_type));
        } else {
          HEAP_VAR(blocksstable::ObDatumRow, datum_row, tenant_id_) {
            const int64_t new_snapshot_column_cnt = snapshot_column_count + extra_column_idxs.count();
            if (OB_FAIL(datum_row.init(new_snapshot_column_cnt))) {
              LOG_WARN("fail to init datum row", K(ret), K(new_snapshot_column_cnt), K(snapshot_column_count), K(datum_row));
            }
            for (int64_t row_id = 0; row_id < ctx.vals_.count() && OB_SUCC(ret); row_id++) {
              int64_t key_pos = 0;
              char *key_str = nullptr;
              key_str = static_cast<char*>(allocator_.alloc(ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH));
              if (OB_ISNULL(key_str)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc vec key", K(ret));
              } else if (index_type == VIAT_HNSW && OB_FAIL(databuff_printf(key_str, ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%lu_hnsw_data_part%05ld", adaptor.get_snap_tablet_id().id(), ctx_->task_status_.target_scn_.get_val_for_inner_table_field(), row_id))) {
                LOG_WARN("fail to build vec snapshot key str", K(ret), K(index_type));
              } else if (index_type == VIAT_HGRAPH &&
                OB_FAIL(databuff_printf(key_str, ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%lu_hgraph_data_part%05ld", adaptor.get_snap_tablet_id().id(), ctx_->task_status_.target_scn_.get_val_for_inner_table_field(), row_id))) {
                LOG_WARN("fail to build vec hgraph snapshot key str", K(ret), K(index_type));
              } else if (index_type == VIAT_HNSW_SQ && OB_FAIL(databuff_printf(key_str, ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%lu_hnsw_sq_data_part%05ld", adaptor.get_snap_tablet_id().id(), ctx_->task_status_.target_scn_.get_val_for_inner_table_field(), row_id))) {
                LOG_WARN("fail to build sq vec snapshot key str", K(ret), K(index_type));
              } else if (index_type == VIAT_HNSW_BQ && OB_FAIL(databuff_printf(key_str, ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%lu_hnsw_bq_data_part%05ld", adaptor.get_snap_tablet_id().id(), ctx_->task_status_.target_scn_.get_val_for_inner_table_field(), row_id))) {
                LOG_WARN("fail to build bq vec snapshot key str", K(ret), K(index_type));
              } else if (index_type == VIAT_IPIVF && OB_FAIL(databuff_printf(key_str, ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%lu_ipivf_data_part%05ld", adaptor.get_snap_tablet_id().id(), ctx_->task_status_.target_scn_.get_val_for_inner_table_field(), row_id))) {
                LOG_WARN("fail to build ipivf vec snapshot key str", K(ret), K(index_type));
              } else if (OB_ISNULL(key_str)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected nullptr key_str", K(ret), KP(key_str));
              } else {
                datum_row.storage_datums_[vector_key_col_idx_].set_string(key_str, key_pos);
                datum_row.storage_datums_[vector_data_col_idx_].set_string(ctx.vals_.at(row_id));
                datum_row.storage_datums_[vector_data_col_idx_].set_has_lob_header();
                datum_row.storage_datums_[vector_vid_col_idx_].set_null();
                datum_row.storage_datums_[vector_col_idx_].set_nop();

                if (vector_visible_col_idx_ >= 0 && vector_visible_col_idx_ < datum_row.get_column_count()) {
                  datum_row.storage_datums_[vector_visible_col_idx_].set_true();
                }
                // set extra column default value
                if (extra_column_idxs.count() > 0) {
                  for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs.count(); i++) {
                    if (extra_column_idxs.at(i) == vector_key_col_idx_ ||
                        extra_column_idxs.at(i) == vector_data_col_idx_ ||
                        extra_column_idxs.at(i) == vector_vid_col_idx_ ||
                        extra_column_idxs.at(i) == vector_col_idx_) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs.at(i)),
                        K(vector_key_col_idx_), K(vector_data_col_idx_), K(vector_vid_col_idx_), K(vector_col_idx_));
                    } else {
                      datum_row.storage_datums_[extra_column_idxs.at(i)].set_null();
                    }
                  }
                }
                LOG_DEBUG("[vec async task] print datum column ids", K(ret),
                  K(vector_key_col_idx_), K(vector_data_col_idx_), K(vector_vid_col_idx_), K(vector_col_idx_), K(extra_column_idxs));
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(row_iter.add_row(datum_row))) {
                  LOG_WARN("failed to add row to iter", K(ret));
                }
                datum_row.reuse();
              }
              CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
            } // end for
          }
        }
      }
    } // heap var.
  }

  if (OB_FAIL(ret)) {
  } else {
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.write_flag_.set_is_insert_up();
    dml_param.table_param_ = &table_dml_param;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout_us;
    dml_param.snapshot_.assign(snapshot);
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = snapshot_table_schema->get_schema_version();
    dml_param.dml_allocator_ = &allocator_;
    if (OB_ISNULL(adaptor.get_snap_data_()) || !adaptor.get_snap_data_()->is_inited()) {  // adaptor created by vector index async task, there won't be access from other threads.
      LOG_INFO("data table is empty, won't create snapshot index");
    } else if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout_us, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    } else {
      int64_t affected_rows = 0;
      if (OB_FAIL(oas->insert_rows(ls_id_, adaptor.get_snap_tablet_id(), *tx_desc, dml_param, dml_column_ids, &row_iter, affected_rows))) {
        LOG_WARN("failed to insert rows to snapshot table", K(ret), K(adaptor.get_snap_tablet_id()));
      }
      store_ctx_guard.reset();
    }
  }
  row_iter.reset();

  //delete old data from snapshot index table.
  dml_param.table_param_ = &table_delete_dml_param;
  int64_t affected_rows = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout_us, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))){
    LOG_WARN("failed to get write store context guard", K(ret));
  } else if (OB_FAIL(oas->delete_rows(ls_id_, adaptor.get_snap_tablet_id(), *tx_desc, dml_param, dml_column_ids, &delete_row_iter, affected_rows))) {
    LOG_WARN("failed to delete rows from snapshot table", K(ret), K(adaptor.get_snap_tablet_id()));
  }
  delete_row_iter.reset();
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = oas->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert snap_data_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }

  // delete 3, 4 index table data.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(delete_incr_table_data(adaptor, dml_param, tx_desc))) {
    LOG_WARN("failed to delete rows from snapshot table", K(ret), K(ctx_->task_status_.tablet_id_));
  }
  return ret;
}

int ObVecIndexAsyncTask::fetch_dml_write_row(
    ObVectorIndexRowIterator &iter,
    const int64_t key_col_idx,
    const int64_t data_col_idx,
    const int64_t visible_col_idx,
    ObIArray<uint64_t> &extra_column_idxs,
    storage::ObValueRowIterator &dml_row_iter)
{
  int ret = OB_SUCCESS;
  const static int64_t snap_table_row_cnt = 5;
  ObDatumRow dml_datum_row;

  if (OB_FAIL(dml_datum_row.init(snap_table_row_cnt + extra_column_idxs.count()))) {
    LOG_WARN("fail to init dml datum row", K(ret));
  } else {
    blocksstable::ObDatumRow *datum_row = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_row(datum_row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next vector data row", K(ret));
        }
      } else if ((key_col_idx < 0 || key_col_idx > datum_row->get_column_count()) ||
                 (data_col_idx < 0 || data_col_idx > datum_row->get_column_count()) ||
                 (visible_col_idx < 0 || visible_col_idx > datum_row->get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected visible col id", K(ret), K(key_col_idx), K(data_col_idx), K(visible_col_idx));
      } else if (datum_row->storage_datums_[visible_col_idx].get_bool()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected visible value in async task rebuild", K(ret), K(*datum_row));
      } else if (OB_FAIL(construct_vector_row(datum_row, extra_column_idxs, key_col_idx,
                                              data_col_idx, visible_col_idx, dml_datum_row))) {
      } else if (OB_FAIL(dml_row_iter.add_row(dml_datum_row))) { // TODO: batch
        LOG_WARN("failed to add row to iter", K(ret));
      }
      dml_datum_row.reuse();
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::execute_write_snap_index(
    transaction::ObTxDesc *tx_desc,
    ObVectorIndexRowIterator &iter,
    const ObTabletID &tablet_id,
    const int64_t key_col_idx,
    const int64_t data_col_idx,
    const int64_t visible_col_idx,
    const uint64_t snapshot_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_desc) || !tablet_id.is_valid() || snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(tx_desc), K(tablet_id), K(snapshot_version));
  } else if (OB_ISNULL(new_adapter_) || !ls_id_.is_valid() || tenant_id_ == OB_INVALID_TENANT_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(new_adapter_), K(ls_id_), K(tenant_id_));
  } else {
    const ObTableSchema *data_table_schema = nullptr;
    const ObTableSchema *snapshot_table_schema = nullptr;
    const int64_t data_table_id = new_adapter_->get_data_table_id();
    const int64_t snap_table_id = new_adapter_->get_snapshot_table_id();
    const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
    int64_t snapshot_table_schema_version = 0;
    int64_t affected_rows = 0;

    ObDMLBaseParam dml_param;
    storage::ObStoreCtxGuard store_ctx_guard;
    oceanbase::transaction::ObTxReadSnapshot snapshot;
    share::schema::ObTableDMLParam table_dml_param(allocator_);
    storage::ObValueRowIterator dml_row_iter;
    common::ObCollationType cs_type = CS_TYPE_INVALID;

    ObSEArray<uint64_t, 4> all_column_ids;
    ObSEArray<uint64_t, 4> dml_column_ids;
    ObSEArray<uint64_t, 4> extra_column_idxs;

    ObAccessService *oas = MTL(ObAccessService *);

    if (OB_ISNULL(oas)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
    } else if (OB_FAIL(dml_row_iter.init())) {
      LOG_WARN("fail to init dml row iter", K(ret));
    } else if (OB_FAIL(prepare_schema_and_snapshot(data_table_schema, snapshot_table_schema, data_table_id, snap_table_id, snapshot_version, snapshot))) {
      LOG_WARN("fail to prepare schema and snapshot", K(ret), K(tablet_id), K(snapshot_version));
    } else if (OB_FAIL(get_snap_index_column_info(*data_table_schema, *snapshot_table_schema, all_column_ids, dml_column_ids, extra_column_idxs, cs_type))) {
      LOG_WARN("fail to get snap index column info", K(ret));
    } else if (OB_FAIL(fetch_dml_write_row(iter, key_col_idx, data_col_idx, visible_col_idx, extra_column_idxs, dml_row_iter))) {
    } else if (OB_FALSE_IT(snapshot_table_schema_version = snapshot_table_schema->get_schema_version())) {
    } else if (OB_FAIL(table_dml_param.convert(snapshot_table_schema, snapshot_table_schema_version, dml_column_ids))) {
      LOG_WARN("fail to convert table dml param.", K(ret));
    } else if (OB_FAIL(prepare_dml_param(dml_param, table_dml_param, store_ctx_guard, tx_desc, snapshot, snapshot_table_schema_version, timeout_us))) {
      LOG_WARN("fail to prepare dml param", K(ret), K(snapshot_version));
    } else if (OB_FAIL(oas->insert_rows(ls_id_, tablet_id, *tx_desc, dml_param, dml_column_ids, &dml_row_iter, affected_rows))) {
      LOG_WARN("fail to insert rows to snapshot table", K(ret), K(tablet_id), K(ls_id_));
    } else {
      LOG_INFO("execute_write_snap_index", K(ret), K(affected_rows), K(dml_column_ids));
    }
  }
  return ret;
}

int ObVecIndexAsyncTask::get_old_snapshot_data(
    ObPluginVectorIndexAdaptor &adaptor,
    transaction::ObTxDesc *tx_desc,
    const int64_t snapshot_column_count,
    common::ObCollationType cs_type,
    ObSEArray<uint64_t, 4> &extra_column_idxs,
    ObTableScanIterator *table_scan_iter,
    storage::ObValueRowIterator &delete_row_iter,
    transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  int64_t loop_cnt = 0;
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob mngr is null", K(ret));
  } else if (OB_ISNULL(table_scan_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table scan iter", K(ret));
  } else if (OB_FAIL(delete_row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  }
  HEAP_VAR(blocksstable::ObDatumRow, d_row, tenant_id_) {
    if (OB_SUCC(ret) && OB_FAIL(d_row.init(snapshot_column_count + extra_column_idxs.count()))) {
      LOG_WARN("fail to init datum row", K(ret), K(d_row));
    }
    while (OB_SUCC(ret)) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (datum_row->get_column_count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
      } else {
        ObString data = datum_row->storage_datums_[1].get_string();
        ObLobLocatorV2 lob(data, data.length() > 0);
        if (lob.has_inrow_data()) {
          // delete inrow lob no need to use the lob manager
        } else {
          // 4.0 text tc compatiable
          ObLobAccessParam lob_param;
          lob_param.tx_desc_ = tx_desc;
          lob_param.tablet_id_ = adaptor.get_data_tablet_id(); // data tablet id
          if (!lob.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid src lob locator.", K(ret));
          } else if (OB_FAIL(lob_mngr->build_lob_param(lob_param, allocator_, cs_type, 0, UINT64_MAX, timeout_us, lob))) {
            LOG_WARN("fail to build lob param.", K(ret));
          } else if (OB_FAIL(lob_param.snapshot_.assign(snapshot))) {
            LOG_WARN("fail to assign snapshot", K(ret), K(snapshot));
          } else if (OB_FAIL(lob_mngr->erase(lob_param))) {
            LOG_WARN("lob meta row delete failed.", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          d_row.storage_datums_[vector_key_col_idx_].set_string(datum_row->storage_datums_[0].get_string());
          d_row.storage_datums_[vector_data_col_idx_].set_string(datum_row->storage_datums_[1].get_string());
          d_row.storage_datums_[vector_data_col_idx_].set_has_lob_header();
          d_row.storage_datums_[vector_vid_col_idx_].set_null();
          d_row.storage_datums_[vector_col_idx_].set_null();

          if (vector_visible_col_idx_ >= 0 && vector_visible_col_idx_ < d_row.get_column_count()) {
            d_row.storage_datums_[vector_visible_col_idx_].set_true();
          }

          // set extra column default value
          if (extra_column_idxs.count() > 0) {
            for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs.count(); i++) {
              if (extra_column_idxs.at(i) == vector_key_col_idx_ ||
                  extra_column_idxs.at(i) == vector_data_col_idx_ ||
                  extra_column_idxs.at(i) == vector_vid_col_idx_ ||
                  extra_column_idxs.at(i) == vector_col_idx_) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs.at(i)),
                  K(vector_key_col_idx_), K(vector_data_col_idx_), K(vector_vid_col_idx_), K(vector_col_idx_));
              } else {
                d_row.storage_datums_[extra_column_idxs.at(i)].set_null();
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(delete_row_iter.add_row(d_row))) {
          LOG_WARN("failed to add row to iter", K(ret));
        }
        d_row.reuse();
      }
      CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
    } // end while
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexAsyncTask::delete_tablet_data(
    ObPluginVectorIndexAdaptor &adaptor,
    ObTabletID& tablet_id,
    ObDMLBaseParam &dml_param,
    transaction::ObTxDesc *tx_desc,
    ObTableScanIterator *table_scan_iter,
    ObSEArray<uint64_t, 4> &dml_column_ids,
    bool check_null_chunk)
{
  int ret = OB_SUCCESS;
  int64_t loop_cnt = 0;
  ObStorageDatumUtils util;
  bool delete_unfinish = true;
  int64_t delta_table_affected_rows = 0;
  storage::ObValueRowIterator row_iter;
  ObAccessService *oas = MTL(ObAccessService *);
  if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else if (OB_ISNULL(table_scan_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table scan iter", K(ret));
  }
  while (OB_SUCC(ret) && delete_unfinish) {
    int cur_row_count = 0;
    if (OB_FAIL(row_iter.init())) {
      LOG_WARN("fail to init row iter", K(ret));
    }
    while (OB_SUCC(ret) && cur_row_count <= BATCH_CNT) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (check_null_chunk && !datum_row->storage_datums_[2].is_null()) {
        // log_table: [vid] [type] [chunk].
        // skip not null chunk, which means they are not embedded.
      } else if (OB_FAIL(row_iter.add_row(*datum_row))) {
        LOG_WARN("failed to add row to iter", K(ret));
      } else {
        cur_row_count += 1;
      }
      CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (cur_row_count == 0) {
      delete_unfinish = false;
    } else if (OB_FAIL(oas->delete_rows(ls_id_, tablet_id, *tx_desc, dml_param, dml_column_ids, &row_iter, delta_table_affected_rows))) {
      LOG_WARN("failed to delete rows from delta table", K(ret), K(tablet_id));
    } else if (delta_table_affected_rows != cur_row_count) {
      LOG_WARN("delete rows count unexpected", K(cur_row_count), K(delta_table_affected_rows));
    }
    delta_table_affected_rows = 0;
    row_iter.reset();
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexAsyncTask::delete_incr_table_data(ObPluginVectorIndexAdaptor &adaptor, ObDMLBaseParam &dml_param, transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  // 可以考虑一下使用inner sql删除数据。
  // 1. get 3, 4 index table scan iter.
  // 2. use iter to delete data.
  schema::ObTableParam delta_table_param(allocator_);
  schema::ObTableParam index_table_param(allocator_);
  common::ObNewRowIterator *delta_table_iter = nullptr;
  common::ObNewRowIterator *index_table_iter = nullptr;
  storage::ObValueRowIterator delta_row_iter;
  storage::ObValueRowIterator index_row_iter;
  ObSEArray<uint64_t, 4> delta_dml_column_ids;
  ObSEArray<uint64_t, 4> index_dml_column_ids;
  ObAccessService *oas = MTL(ObAccessService *);
  int64_t loop_cnt = 0;
  const ObTableSchema *delta_table_schema;
  const ObTableSchema *index_table_schema;
  share::schema::ObTableDMLParam table_dml_param(allocator_);
  share::schema::ObTableDMLParam bitmap_table_dml_param(allocator_);
  ObSchemaGetterGuard schema_guard;
  int64_t delta_table_affected_rows = 0;
  int64_t index_table_affected_rows = 0;
  SMART_VARS_2((storage::ObTableScanParam, delta_scan_param),
               (storage::ObTableScanParam, index_scan_param)) {
    if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, adaptor.get_inc_table_id(), delta_table_schema))) {
      LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(adaptor.get_inc_table_id()));
    } else if (OB_ISNULL(delta_table_schema) || delta_table_schema->is_in_recyclebin()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector index table not exist", K(ret), K(tenant_id_), K(adaptor.get_inc_table_id()));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, adaptor.get_vbitmap_table_id(), index_table_schema))) {
      LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(adaptor.get_vbitmap_table_id()));
    } else if (OB_ISNULL(index_table_schema) || index_table_schema->is_in_recyclebin()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector index table not exist", K(ret), K(tenant_id_), K(adaptor.get_vbitmap_table_id()));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                  &adaptor,
                                  ctx_->task_status_.target_scn_,
                                  INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
                                  allocator_,
                                  allocator_,
                                  delta_scan_param,
                                  delta_table_param,
                                  delta_table_iter,
                                  &delta_dml_column_ids,
                                  true))) {
      LOG_WARN("failed to read vid id table local tablet.", K(ret));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                        &adaptor,
                                        ctx_->task_status_.target_scn_,
                                        INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                        allocator_,
                                        allocator_,
                                        index_scan_param,
                                        index_table_param,
                                        index_table_iter,
                                        &index_dml_column_ids,
                                        true))) {
      LOG_WARN("failed to read data table local tablet.", K(ret));
    } else {
      ObTableScanIterator *delta_scan_iter = static_cast<ObTableScanIterator *>(delta_table_iter);
      ObTableScanIterator *index_scan_iter = static_cast<ObTableScanIterator *>(index_table_iter);
      if (OB_ISNULL(delta_scan_iter) || OB_ISNULL(index_scan_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table scan iter", K(ret));
      } else if (OB_FAIL(table_dml_param.convert(delta_table_schema, delta_table_schema->get_schema_version(), delta_dml_column_ids))) {
        LOG_WARN("failed to convert table dml param.", K(ret));
      } else if (FALSE_IT(dml_param.schema_version_ = delta_table_schema->get_schema_version())) {
      } else if (FALSE_IT(dml_param.table_param_ = &table_dml_param)) {
      } else if (OB_FAIL(delete_tablet_data(adaptor, adaptor.get_inc_tablet_id(), dml_param, tx_desc, delta_scan_iter, delta_dml_column_ids, adaptor.is_hybrid_index()))) {
        LOG_WARN("failed to delete delta table data", K(ret));
      } else if (OB_FAIL(bitmap_table_dml_param.convert(index_table_schema, index_table_schema->get_schema_version(), index_dml_column_ids))) {
        LOG_WARN("failed to convert table dml param.", K(ret));
      } else if (FALSE_IT(dml_param.schema_version_ = index_table_schema->get_schema_version())) {
      } else if (FALSE_IT(dml_param.table_param_ = &bitmap_table_dml_param)) {
      } else if (OB_FAIL(delete_tablet_data(adaptor, adaptor.get_vbitmap_tablet_id(), dml_param, tx_desc, index_scan_iter, index_dml_column_ids ))) {
        LOG_WARN("failed to delete index table data", K(ret));
      }
    }
  }

  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(delta_table_iter)) {
      tmp_ret = oas->revert_scan_iter(delta_table_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert delta_table_iter failed", K(ret));
      }
    }
    delta_table_iter = nullptr;
    if (OB_NOT_NULL(index_table_iter)) {
      tmp_ret = oas->revert_scan_iter(index_table_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert index_table_iter failed", K(ret));
      }
    }
    index_table_iter = nullptr;
  }
  return ret;
}

}
}
