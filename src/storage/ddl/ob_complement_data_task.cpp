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
#include "ob_complement_data_task.h"
#include "lib/utility/ob_tracepoint.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_struct.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_ddl_common.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/ob_ddl_sim_point.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/px/ob_granule_util.h"
#include "sql/ob_sql_utils.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ob_i_table.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/ob_partition_range_spliter.h"
#include "storage/ob_row_reshape.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/lob/ob_lob_util.h"
#include "logservice/ob_log_service.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace compaction;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace observer;
using namespace omt;
using namespace name;
using namespace transaction;
using namespace blocksstable;

namespace storage
{
void add_ddl_event(const ObComplementDataParam *param, const ObString &stmt)
{
  if (OB_NOT_NULL(param)) {
    char table_id_buffer[256];
    char tablet_id_buffer[256];
    snprintf(table_id_buffer, sizeof(table_id_buffer), "source_table_id:%ld, dest_table_id:%ld", param->orig_table_id_, param->dest_table_id_);
    snprintf(tablet_id_buffer, sizeof(tablet_id_buffer), "source_id:%lu, dest_id:%lu", param->orig_tablet_id_.id(), param->dest_tablet_id_.id());

    SERVER_EVENT_ADD("ddl", stmt.ptr(),
      "tenant_id", param->dest_tenant_id_,
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "task_id", param->task_id_,
      "table_id", table_id_buffer,
      "schema_version", param->dest_schema_version_,
      tablet_id_buffer);
  }
  LOG_INFO("complement data task.", K(ret), "ddl_event_info", ObDDLEventInfo(), K(stmt), KPC(param));
}

int ObComplementDataParam::init(const ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = nullptr;
  const ObTableSchema *orig_table_schema = nullptr;
  const ObTableSchema *dest_table_schema = nullptr;
  const uint64_t orig_tenant_id = arg.tenant_id_;
  const uint64_t dest_tenant_id = arg.dest_tenant_id_;
  const int64_t orig_table_id = arg.source_table_id_;
  const int64_t dest_table_id = arg.dest_schema_id_;
  const int64_t orig_schema_version = arg.schema_version_;
  const int64_t dest_schema_version = arg.dest_schema_version_;
  ObSchemaGetterGuard src_tenant_schema_guard;
  ObSchemaGetterGuard dst_tenant_schema_guard;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementDataParam has been inited before", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    MTL_SWITCH (OB_SYS_TENANT_ID) {
      if (OB_FAIL(ObDDLUtil::check_schema_version_refreshed(orig_tenant_id, orig_schema_version))) {
        if (OB_SCHEMA_EAGAIN != ret) {
          LOG_WARN("check schema version refreshed failed", K(ret), K(orig_tenant_id), K(orig_schema_version));
        }
      } else if (orig_tenant_id != dest_tenant_id
          && OB_FAIL(ObDDLUtil::check_schema_version_refreshed(dest_tenant_id, dest_schema_version))) {
        if (OB_SCHEMA_EAGAIN != ret) {
          LOG_WARN("check schema version refreshed failed", K(ret), K(dest_tenant_id), K(dest_schema_version));
        }
      } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                orig_tenant_id, src_tenant_schema_guard, orig_schema_version))) {
        LOG_WARN("fail to get tenant schema guard", K(ret), K(orig_tenant_id), K(orig_schema_version));
      } else if (OB_FAIL(src_tenant_schema_guard.get_tenant_info(orig_tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", K(ret), K(orig_tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("tenant not exist", K(ret), K(orig_tenant_id), K(orig_schema_version));
      } else if (OB_FAIL(src_tenant_schema_guard.get_table_schema(orig_tenant_id, orig_table_id, orig_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(orig_tenant_id), K(orig_table_id));
      } else if (OB_ISNULL(orig_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(orig_tenant_id), K(orig_table_id), K(orig_schema_version));
      } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                dest_tenant_id, dst_tenant_schema_guard, dest_schema_version))) {
        LOG_WARN("fail to get tenant schema guard", K(ret), K(dest_tenant_id), K(dest_schema_version));
      } else if (OB_FAIL(dst_tenant_schema_guard.get_tenant_info(dest_tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", K(ret), K(dest_tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("tenant not exist", K(ret), K(dest_tenant_id), K(dest_schema_version));
      } else if (OB_FAIL(dst_tenant_schema_guard.get_table_schema(dest_tenant_id, dest_table_id, dest_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(dest_tenant_id), K(dest_table_id));
      } else if (OB_ISNULL(dest_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(dest_tenant_id), K(dest_table_id), K(dest_schema_version));
      } else if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(orig_tenant_id, arg.source_table_id_, compat_mode_))) {
        LOG_WARN("failed to get compat mode", K(ret), K(arg));
      } else {
        snapshot_version_ = arg.snapshot_version_;
      }
    }

    if (OB_SUCC(ret)) {
      if (orig_tenant_id == dest_tenant_id) {
        if (OB_UNLIKELY(dest_table_schema->get_association_table_id() != arg.source_table_id_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret), K(arg), K(dest_table_schema->get_association_table_id()));
        } else if (OB_FAIL(split_task_ranges(arg.ls_id_, arg.source_tablet_id_, orig_table_schema->get_tablet_size(), arg.parallelism_))) {
          LOG_WARN("fail to init concurrent params", K(ret), K(arg));
        }
      } else {
        // TODO yiren, support parallel for remote scan.
        // 1. support split task range even if data in other nodes.
        // 2. support query the data by adding rowkey range(hidden_column_visible for heap table).
        // recover restore table ddl task.
        ObStoreRange whole_range;
        whole_range.set_whole_range();
        if (OB_FAIL(ranges_.push_back(whole_range))) {
          LOG_WARN("push back failed", K(ret));
        } else {
          concurrent_cnt_ = 1;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    orig_tenant_id_ = orig_tenant_id;
    dest_tenant_id_ = dest_tenant_id;
    orig_table_id_ = orig_table_id;
    dest_table_id_ = dest_table_id;
    orig_schema_version_ = orig_schema_version;
    dest_schema_version_ = dest_schema_version;
    orig_tablet_id_ = arg.source_tablet_id_;
    dest_tablet_id_ = arg.dest_tablet_id_;
    orig_ls_id_ = arg.ls_id_;
    dest_ls_id_ = arg.dest_ls_id_;
    task_id_ = arg.task_id_;
    execution_id_ = arg.execution_id_;
    tablet_task_id_ = arg.tablet_task_id_;
    data_format_version_ = arg.data_format_version_;
    FLOG_INFO("succeed to init ObComplementDataParam", K(ret), KPC(this));
  }
  return ret;
}

// split task ranges to do table scan based on the whole range on the specified tablet.
int ObComplementDataParam::split_task_ranges(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t tablet_size,
    const int64_t hint_parallelism)
{
  int ret = OB_SUCCESS;
  ObFreezeInfo frozen_status;
  const bool allow_not_ready = false;
  ObLSHandle ls_handle;
  ObTabletTableIterator iterator;
  ObLSTabletService *tablet_service = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementDataParam has been inited before", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("fail to get log stream", K(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_handle));
  } else if (OB_ISNULL(tablet_service = ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service is nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(MTL_ID(), task_id_, COMPLEMENT_DATA_TASK_SPLIT_RANGE_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(MTL_ID()), K(task_id_));
  } else {
    int64_t total_size = 0;
    int64_t expected_task_count = 0;
    ObStoreRange range;
    range.set_whole_range();
    ObSEArray<common::ObStoreRange, 32> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    ObParallelBlockRangeTaskParams params;
    params.parallelism_ = hint_parallelism;
    params.expected_task_load_ = tablet_size / 1024 / 1024 <= 0 ? sql::OB_EXPECTED_TASK_LOAD : tablet_size / 1024 / 1024;
    if (OB_FAIL(ranges.push_back(range))) {
      LOG_WARN("push back range failed", K(ret));
    } else if (OB_FAIL(tablet_service->get_multi_ranges_cost(tablet_id,
                                                             ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                                                             ranges,
                                                             total_size))) {
      LOG_WARN("get multi ranges cost failed", K(ret));
      if (OB_REPLICA_NOT_READABLE == ret) {
        ret = OB_EAGAIN;
      }
    } else if (OB_FALSE_IT(total_size = total_size / 1024 / 1024 /* Byte -> MB */)) {
    } else if (OB_FAIL(ObGranuleUtil::compute_total_task_count(params,
                                                               total_size,
                                                               expected_task_count))) {
      LOG_WARN("compute total task count failed", K(ret));
    } else if (OB_FAIL(tablet_service->split_multi_ranges(tablet_id,
                                                          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                                                          ranges,
                                                          min(min(max(expected_task_count, 1), hint_parallelism), ObMacroDataSeq::MAX_PARALLEL_IDX + 1),
                                                          allocator_,
                                                          multi_range_split_array))) {
      LOG_WARN("split multi ranges failed", K(ret));
      if (OB_REPLICA_NOT_READABLE == ret) {
        ret = OB_EAGAIN;
      }
    } else if (multi_range_split_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range split arr", K(ret), K(total_size), K(hint_parallelism),
        K(expected_task_count), K(params), K(multi_range_split_array));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < multi_range_split_array.count(); i++) {
        ObIArray<ObStoreRange> &storage_task_ranges = multi_range_split_array.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < storage_task_ranges.count(); j++) {
          if (OB_FAIL(ranges_.push_back(storage_task_ranges.at(j)))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        concurrent_cnt_ = ranges_.count();
        LOG_INFO("succeed to get range and concurrent cnt", K(ret), K(total_size), K(hint_parallelism),
          K(expected_task_count), K(params), K(multi_range_split_array), K(ranges_));
      }
    }
  }
  return ret;
}

int ObComplementDataParam::get_hidden_table_key(ObITable::TableKey &table_key) const
{
  int ret = OB_SUCCESS;
  table_key.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataParam is not inited", K(ret));
  } else {
    table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    table_key.tablet_id_ = dest_tablet_id_;
    table_key.version_range_.base_version_ = ObNewVersionRange::MIN_VERSION;
    table_key.version_range_.snapshot_version_ = snapshot_version_;
  }
  return ret;
}

int ObComplementDataContext::init(const ObComplementDataParam &param, const blocksstable::ObDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  void *builder_buf = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  const ObSSTable *first_major_sstable = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementDataContext has already been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param), K(desc));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.dest_ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               param.dest_tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == tablet_handle.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(param));
  } else if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(param.dest_ls_id_, param.dest_tablet_id_, first_major_sstable, table_store_wrapper))) {
    LOG_WARN("check if major sstable exist failed", K(ret), K(param));
  } else if (OB_FAIL(data_sstable_redo_writer_.init(param.dest_ls_id_,
                                                    param.dest_tablet_id_))) {
    LOG_WARN("fail to init data sstable redo writer", K(ret), K(param));
  } else if (nullptr != index_builder_) {
    LOG_INFO("index builder is already exist", K(ret));
  } else if (OB_ISNULL(builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(index_builder_ = new (builder_buf) ObSSTableIndexBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to new ObSSTableIndexBuilder", K(ret));
  } else if (OB_FAIL(index_builder_->init(desc,
                                          nullptr, // macro block flush callback
                                          ObSSTableIndexBuilder::DISABLE))) {
    LOG_WARN("failed to init index builder", K(ret), K(desc));
  } else {
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    ObTabletFullDirectLoadMgr *tablet_direct_load_mgr = nullptr;
    ObTabletDirectLoadInsertParam direct_load_param;
    direct_load_param.is_replay_ = false;
    direct_load_param.common_param_.direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_DDL;
    direct_load_param.common_param_.data_format_version_ = param.data_format_version_;
    direct_load_param.common_param_.read_snapshot_ = param.snapshot_version_;
    direct_load_param.common_param_.ls_id_ = param.dest_ls_id_;
    direct_load_param.common_param_.tablet_id_ = param.dest_tablet_id_;
    direct_load_param.runtime_only_param_.exec_ctx_ = nullptr;
    direct_load_param.runtime_only_param_.task_id_ = param.task_id_;
    direct_load_param.runtime_only_param_.table_id_ = param.dest_table_id_;
    direct_load_param.runtime_only_param_.schema_version_ = param.dest_schema_version_;
    direct_load_param.runtime_only_param_.task_cnt_ = 1; // default value.
    if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(tenant_direct_load_mgr->alloc_execution_context_id(context_id_))) {
      LOG_WARN("alloc execution context id failed", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->create_tablet_direct_load(context_id_, param.execution_id_, direct_load_param))) {
      LOG_WARN("create tablet manager failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_major_sstable_exist_ = nullptr != first_major_sstable ? true : false;
    concurrent_cnt_ = param.concurrent_cnt_;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != index_builder_) {
      index_builder_->~ObSSTableIndexBuilder();
      index_builder_ = nullptr;
    }
    if (nullptr != builder_buf) {
      allocator_.free(builder_buf);
      builder_buf = nullptr;
    }
  }
  return ret;
}

int ObComplementDataContext::write_start_log(const ObComplementDataParam &param)
{
  int ret = OB_SUCCESS;
  ObITable::TableKey hidden_table_key;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataContext not init", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(param.get_hidden_table_key(hidden_table_key))) {
    LOG_WARN("fail to get hidden table key", K(ret));
  } else if (OB_UNLIKELY(!hidden_table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table key", K(ret), K(hidden_table_key));
  } else {
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(tenant_direct_load_mgr->open_tablet_direct_load(true, /*is_full_direct_load*/
      param.dest_ls_id_, param.dest_tablet_id_, context_id_, start_scn_, tablet_direct_load_mgr_handle_))) {
      LOG_WARN("write ddl start log failed", K(ret));
    } else if (OB_UNLIKELY(!start_scn_.is_valid_and_not_min())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid start scn", K(ret), K(start_scn_));
    }
    LOG_INFO("complement task start ddl redo success", K(ret), K(param));
  }
  return ret;
}

int ObComplementDataContext::check_already_committed(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    bool &is_commited)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  is_commited = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataContext has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == tablet_handle.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(param));
  } else {
    is_commited = !tablet_direct_load_mgr_handle_.is_valid();
    if (!is_commited) {
      SCN commit_scn = tablet_direct_load_mgr_handle_.get_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta());
      is_commited = commit_scn.is_valid_and_not_min();
    }
  }
  return ret;
}

int ObComplementDataContext::add_column_checksum(const ObIArray<int64_t> &report_col_checksums,
    const ObIArray<int64_t> &report_col_ids)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (0 == report_col_checksums_.count()) {
    if (OB_FAIL(report_col_checksums_.prepare_allocate(report_col_checksums.count()))) {
      LOG_WARN("prepare allocate report column checksum array failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && 0 == report_col_ids_.count()) {
    if (OB_FAIL(report_col_ids_.prepare_allocate(report_col_ids.count()))) {
      LOG_WARN("prepare allocate report col checksum array failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (report_col_checksums_.count() != report_col_checksums.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, report col checksum array count is not equal", K(ret), K(report_col_checksums.count()), K(report_col_checksums_.count()));
    } else if (report_col_ids_.count() != report_col_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, report col ids array count is not equal", K(ret), K(report_col_ids.count()), K(report_col_ids_.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < report_col_checksums.count(); ++i) {
        report_col_checksums_.at(i) += report_col_checksums.at(i);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < report_col_ids.count(); ++i) {
        report_col_ids_.at(i) = report_col_ids.at(i);
      }
    }
  }
  return ret;
}

int ObComplementDataContext::get_column_checksum(ObIArray<int64_t> &report_col_checksums,
    ObIArray<int64_t> &report_col_ids)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(report_col_checksums.assign(report_col_checksums_))) {
    LOG_WARN("assign column checksum failed", K(ret));
  } else if (OB_FAIL(report_col_ids.assign(report_col_ids_))) {
    LOG_WARN("assign column ids failed", K(ret));
  }
  return ret;
}

void ObComplementDataContext::destroy()
{
  is_inited_ = false;
  is_major_sstable_exist_ = false;
  complement_data_ret_ = OB_SUCCESS;
  concurrent_cnt_ = 0;
  if (OB_NOT_NULL(index_builder_)) {
    index_builder_->~ObSSTableIndexBuilder();
    allocator_.free(index_builder_);
    index_builder_ = nullptr;
  }
  tablet_direct_load_mgr_handle_.reset();
  allocator_.reset();
  context_id_ = 0;
}

ObComplementDataDag::ObComplementDataDag()
  : ObIDag(ObDagType::DAG_TYPE_DDL), is_inited_(false), param_(), context_()
{
}


ObComplementDataDag::~ObComplementDataDag()
{
}

int ObComplementDataDag::init(const ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementDataDag has already been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(param_.init(arg))) {
    LOG_WARN("fail to init dag param", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", K(ret), K(param_));
  } else {
    consumer_group_id_ = arg.consumer_group_id_;
    is_inited_ = true;
  }
  LOG_INFO("finish to init complement data dag", K(ret), K(param_));
  return ret;
}

int ObComplementDataDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObComplementPrepareTask *prepare_task = nullptr;
  ObComplementWriteTask *write_task = nullptr;
  ObComplementMergeTask *merge_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("allocate task failed", K(ret));
  } else if (OB_ISNULL(prepare_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(prepare_task->init(param_, context_))) {
    LOG_WARN("init prepare task failed", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("add task failed", K(ret));
  } else if (OB_FAIL(alloc_task(write_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_ISNULL(write_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(write_task->init(0, param_, context_))) {
    LOG_WARN("init write task failed", K(ret));
  } else if (OB_FAIL(prepare_task->add_child(*write_task))) {
    LOG_WARN("add child task failed", K(ret));
  } else if (OB_FAIL(add_task(*write_task))) {
    LOG_WARN("add task failed", K(ret));
  } else if (OB_FAIL(alloc_task(merge_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_ISNULL(merge_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(merge_task->init(param_, context_))) {
    LOG_WARN("init merge task failed", K(ret));
  } else if (OB_FAIL(write_task->add_child(*merge_task))) {
    LOG_WARN("add child task failed", K(ret));
  } else if (OB_FAIL(add_task(*merge_task))) {
    LOG_WARN("add task failed");
  }
  return ret;
}

bool ObComplementDataDag::ignore_warning()
{
  return OB_EAGAIN == dag_ret_
    || OB_NEED_RETRY == dag_ret_
    || OB_TASK_EXPIRED == dag_ret_;
}

int ObComplementDataDag::prepare_context()
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc(true/*is_ddl*/);
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *hidden_table_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataDag not init", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", K(ret), K(param_));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
             param_.dest_tenant_id_, schema_guard, param_.dest_schema_version_))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(param_));
  } else if (OB_FAIL(schema_guard.get_table_schema(param_.dest_tenant_id_,
             param_.dest_table_id_, hidden_table_schema))) {
    LOG_WARN("fail to get hidden table schema", K(ret), K(param_));
  } else if (OB_ISNULL(hidden_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("hidden table schema not exist", K(ret), K(param_));
  } else if (OB_FAIL(data_desc.init(*hidden_table_schema,
                                    param_.dest_ls_id_,
                                    param_.dest_tablet_id_,
                                    MAJOR_MERGE,
                                    param_.snapshot_version_,
                                    param_.data_format_version_))) {
    LOG_WARN("fail to init data desc", K(ret));
  } else if (OB_FAIL(context_.init(param_, data_desc.get_desc()))) {
    LOG_WARN("fail to init context", K(ret), K(param_), K(data_desc));
  }
  LOG_INFO("finish to prepare complement context", K(ret), K(param_), K(context_));
  return ret;
}

int64_t ObComplementDataDag::hash() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t hash_val = 0;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid())) {
    tmp_ret = OB_ERR_SYS;
    LOG_ERROR("table schema must not be NULL", K(tmp_ret), K(is_inited_), K(param_));
  } else {
    hash_val = param_.orig_tenant_id_ + param_.dest_tenant_id_
             + param_.orig_table_id_ + param_.dest_table_id_
             + param_.orig_ls_id_.hash() + param_.dest_ls_id_.hash()
             + param_.orig_tablet_id_.hash() + param_.dest_tablet_id_.hash() + ObDagType::DAG_TYPE_DDL;
  }
  return hash_val;
}

bool ObComplementDataDag::operator==(const ObIDag &other) const
{
  int tmp_ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObComplementDataDag &dag = static_cast<const ObComplementDataDag &>(other);
    if (OB_UNLIKELY(!param_.is_valid() || !dag.param_.is_valid())) {
      tmp_ret = OB_ERR_SYS;
      LOG_ERROR("invalid argument", K(tmp_ret), K(param_), K(dag.param_));
    } else {
      is_equal = (param_.orig_tenant_id_ == dag.param_.orig_tenant_id_) && (param_.dest_tenant_id_ == dag.param_.dest_tenant_id_) &&
                 (param_.orig_table_id_ == dag.param_.orig_table_id_) && (param_.dest_table_id_ == dag.param_.dest_table_id_) &&
                 (param_.orig_ls_id_ == dag.param_.orig_ls_id_) && (param_.dest_ls_id_ == dag.param_.dest_ls_id_) &&
                 (param_.orig_tablet_id_ == dag.param_.orig_tablet_id_) && (param_.dest_tablet_id_ == dag.param_.dest_tablet_id_);
    }
  }
  return is_equal;
}

// build reponse here rather deconstruction of DAG, to avoid temporary dead lock of RS RPC queue.
//
int ObComplementDataDag::report_replica_build_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataDag has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_DDL_REPORT_REPLICA_BUILD_STATUS_FAIL) OB_SUCCESS;
      LOG_INFO("report replica build status errsim", K(ret));
    }
#endif
    obrpc::ObDDLBuildSingleReplicaResponseArg arg;
    ObAddr rs_addr;
    arg.tenant_id_ = param_.orig_tenant_id_;
    arg.dest_tenant_id_ = param_.dest_tenant_id_;
    arg.ls_id_ = param_.orig_ls_id_;
    arg.dest_ls_id_ = param_.dest_ls_id_;
    arg.tablet_id_ = param_.orig_tablet_id_;
    arg.source_table_id_ = param_.orig_table_id_;
    arg.dest_schema_id_ = param_.dest_table_id_;
    arg.ret_code_ = context_.complement_data_ret_;
    arg.snapshot_version_ = param_.snapshot_version_;
    arg.schema_version_ = param_.orig_schema_version_;
    arg.dest_schema_version_ = param_.dest_schema_version_;
    arg.task_id_ = param_.task_id_;
    arg.execution_id_ = param_.execution_id_;
    arg.row_scanned_ = context_.row_scanned_;
    arg.row_inserted_ = context_.row_inserted_;
    arg.server_addr_ = GCTX.self_addr();
    FLOG_INFO("send replica build status response to RS", K(ret), K(context_), K(arg));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("fail to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).build_ddl_single_replica_response(arg))) {
      LOG_WARN("fail to send build ddl single replica response", K(ret), K(arg));
    }
  }
  DEBUG_SYNC(HOLD_DDL_COMPLEMENT_DAG_AFTER_REPORT_FINISH);
  FLOG_INFO("complement data finished", K(ret), K(context_.complement_data_ret_));
  return ret;
}

int ObComplementDataDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataDag has not been initialized", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param_));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                param_.orig_ls_id_.id(),
                                static_cast<int64_t>(param_.orig_tablet_id_.id()),
                                static_cast<int64_t>(param_.dest_tablet_id_.id()),
                                static_cast<int64_t>(param_.orig_table_id_),
                                static_cast<int64_t>(param_.dest_table_id_),
                                param_.orig_schema_version_,
                                param_.snapshot_version_))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObComplementDataDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataDag has not been initialized", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(param_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "logstream_id=%ld source_tablet_id=%ld dest_tablet_id=%ld",
                              param_.orig_ls_id_.id(), param_.orig_tablet_id_.id(), param_.dest_tablet_id_.id()))) {
    LOG_WARN("fill dag key for ddl table merge dag failed", K(ret), K(param_));
  }
  return ret;
}

int ObComplementDataDag::check_and_exit_on_demand()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag has not been initialized", K(ret));
  } else {
    DEBUG_SYNC(HOLD_DDL_COMPLEMENT_DAG_WHEN_APPEND_ROW);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql_string;
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_TMP_FAIL(sql_string.assign_fmt("SELECT status FROM %s WHERE task_id = %lu", share::OB_ALL_DDL_TASK_STATUS_TNAME, param_.task_id_))) {
        LOG_WARN("assign sql string failed", K(tmp_ret), K(param_));
      } else if (OB_TMP_FAIL(GCTX.sql_proxy_->read(res, param_.dest_tenant_id_, sql_string.ptr()))) {
        LOG_WARN("fail to execute sql", K(tmp_ret), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(tmp_ret));
      } else if (OB_TMP_FAIL(result->next())) {
        if (OB_ENTRY_NOT_EXIST == tmp_ret) {
          ret = OB_CANCELED;
        }
        LOG_WARN("iterate next failed", K(ret), K(tmp_ret));
      } else {
        int task_status = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "status", task_status, int);
        if (OB_SUCC(ret)) {
          ret = task_status == ObDDLTaskStatus::REDEFINITION ? ret : OB_CANCELED;
        }
      }
    }
  }
  return ret;
}

ObComplementPrepareTask::ObComplementPrepareTask()
  : ObITask(TASK_TYPE_COMPLEMENT_PREPARE), is_inited_(false), param_(nullptr), context_(nullptr)
{
}

ObComplementPrepareTask::~ObComplementPrepareTask()
{
}

int ObComplementPrepareTask::init(ObComplementDataParam &param, ObComplementDataContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementPrepareTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param), K(context));
  } else {
    param_ = &param;
    context_ = &context;
    is_inited_ = true;
  }
  return ret;
}

int ObComplementPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  ObComplementDataDag *dag = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(tmp_dag) || ObDagType::DAG_TYPE_DDL != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is invalid", K(ret), KP(tmp_dag));
  } else if (FALSE_IT(dag = static_cast<ObComplementDataDag *>(tmp_dag))) {
  } else if (OB_FAIL(dag->prepare_context())) {
    LOG_WARN("prepare complement context failed", K(ret));
  } else if (context_->is_major_sstable_exist_) {
    FLOG_INFO("major sstable exists, all task should finish", K(ret), K(*param_));
  } else if (OB_FAIL(context_->write_start_log(*param_))) {
    LOG_WARN("write start log failed", K(ret), KPC(param_));
  } else if (!param_->use_new_checksum() && OB_FAIL(ObDDLChecksumOperator::delete_checksum(param_->dest_tenant_id_,
                                                            param_->execution_id_,
                                                            param_->orig_table_id_,
                                                            0/*use 0 just to avoid clearing target table chksum*/,
                                                            param_->task_id_,
                                                            *GCTX.sql_proxy_,
                                                            param_->tablet_task_id_))) {
    LOG_WARN("failed to delete checksum", K(ret), KPC(param_));
  } else {
    LOG_INFO("finish the complement prepare task", K(ret), KPC(param_), "ddl_event_info", ObDDLEventInfo());
  }

  if (OB_FAIL(ret)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }

  add_ddl_event(param_, "complement prepare task");
  return ret;
}

ObComplementWriteTask::ObComplementWriteTask()
  : ObITask(TASK_TYPE_COMPLEMENT_WRITE), allocator_("WriteTaskAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    is_inited_(false), task_id_(0), param_(nullptr),
    context_(nullptr), write_row_(),
    col_ids_(), org_col_ids_(), output_projector_()
{
}

ObComplementWriteTask::~ObComplementWriteTask()
{
  col_ids_.reset();
  org_col_ids_.reset();
  output_projector_.reset();
  write_row_.reset();
  allocator_.reset();
}

int ObComplementWriteTask::init(const int64_t task_id, ObComplementDataParam &param,
    ObComplementDataContext &context)
{
  int ret = OB_SUCCESS;
  int64_t schema_stored_column_cnt = 0;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *hidden_table_schema = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementWriteTask has already been inited", K(ret));
  } else if (task_id < 0 || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), K(param), K(context));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
             param.dest_tenant_id_, schema_guard, param.dest_schema_version_))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(param));
  } else if (OB_FAIL(schema_guard.get_table_schema(param.dest_tenant_id_,
             param.dest_table_id_, hidden_table_schema))) {
    LOG_WARN("fail to get hidden table schema", K(ret), K(param));
  } else if (OB_ISNULL(hidden_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("hidden table schema not exist", K(ret), K(param));
  } else if (OB_FAIL(hidden_table_schema->get_store_column_count(schema_stored_column_cnt))) {
    LOG_WARN("get stored column cnt failed", K(ret));
  } else if (OB_FAIL(write_row_.init(
              allocator_, schema_stored_column_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
    LOG_WARN("Fail to init write row", K(ret));
  } else {
    write_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    task_id_ = task_id;
    param_ = &param;
    context_ = &context;
    is_inited_ = true;
  }
  return ret;
}

int ObComplementWriteTask::process()
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObIDag *tmp_dag = get_dag();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementWriteTask has not been inited before", K(ret));
  } else if (OB_ISNULL(tmp_dag) || ObDagType::DAG_TYPE_DDL != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is invalid", K(ret), KP(tmp_dag));
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", "ret", context_->complement_data_ret_);
  } else if (context_->is_major_sstable_exist_) {
  } else if (OB_FAIL(guard.switch_to(param_->dest_tenant_id_, false))) {
    LOG_WARN("switch to tenant failed", K(ret), K(param_->dest_tenant_id_));
  } else if (param_->dest_tenant_id_ == param_->orig_tenant_id_) {
    if (OB_FAIL(local_scan_by_range())) {
      LOG_WARN("local scan and append row for column redefinition failed", K(ret), K(task_id_));
    } else {
      ObDDLEventInfo event_info;
      LOG_INFO("finish the complement write task", K(ret), "ddl_event_info", ObDDLEventInfo());
    }
  } else if (OB_FAIL(remote_scan())) {
    LOG_WARN("remote scan for recover restore table ddl failed", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }

  add_ddl_event(param_, "complement write task");
  return ret;
}

int ObComplementWriteTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  ObComplementDataDag *dag = nullptr;
  ObComplementWriteTask *write_task = nullptr;
  const int64_t next_task_id = task_id_ + 1;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementWriteTask has not been inited", K(ret));
  } else if (next_task_id == param_->concurrent_cnt_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(tmp_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag must not be NULL", K(ret));
  } else if (OB_UNLIKELY(ObDagType::DAG_TYPE_DDL != tmp_dag->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag type is invalid", K(ret), "dag type", dag->get_type());
  } else if (FALSE_IT(dag = static_cast<ObComplementDataDag *>(tmp_dag))) {
  } else if (OB_FAIL(dag->alloc_task(write_task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_FAIL(write_task->init(next_task_id, *param_, *context_))) {
    LOG_WARN("fail to init complement write task", K(ret));
  } else {
    next_task = write_task;
    LOG_INFO("generate next complement write task", K(ret), K(param_->dest_table_id_));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    if (OB_ITER_END != ret) {
      context_->complement_data_ret_ = ret;
    }
  }
  return ret;
}

//generate col_ids and projector based on table_schema
int ObComplementWriteTask::generate_col_param()
{
  int ret = OB_SUCCESS;
  col_ids_.reuse();
  org_col_ids_.reuse();
  output_projector_.reuse();
  ObArray<ObColDesc> tmp_col_ids;
  ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
  ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
  ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
  ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *hidden_table_schema = nullptr;
  MTL_SWITCH (OB_SYS_TENANT_ID) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(param_->orig_tenant_id_, param_->dest_tenant_id_,
        hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
        src_tenant_schema_guard, dst_tenant_schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), KPC(param_));
    } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(param_->orig_tenant_id_,
              param_->orig_table_id_, data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(arg));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table schema not exist", K(ret), K(arg));
    } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(param_->dest_tenant_id_,
              param_->dest_table_id_, hidden_table_schema))) {
      LOG_WARN("fail to get hidden table schema", K(ret), KPC(param_));
    } else if (OB_ISNULL(hidden_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("hidden table schema not exist", K(ret), KPC(param_));
    } else if (OB_FAIL(hidden_table_schema->get_store_column_ids(tmp_col_ids, false))) {
      LOG_WARN("fail to get column ids", K(ret), KPC(hidden_table_schema));
    } else if (OB_FAIL(org_col_ids_.assign(tmp_col_ids))) {
      LOG_WARN("fail to assign col descs", K(ret), K(tmp_col_ids));
    } else {
      // generate col_ids
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_col_ids.count(); i++) {
        const uint64_t hidden_column_id = tmp_col_ids.at(i).col_id_;
        const ObColumnSchemaV2 *hidden_column_schema = hidden_table_schema->get_column_schema(hidden_column_id);
        if (OB_ISNULL(hidden_column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema", K(ret), K(hidden_column_id));
        } else {
          const ObString &hidden_column_name = hidden_column_schema->get_column_name_str();
          const ObColumnSchemaV2 *data_column_schema = data_table_schema->get_column_schema(hidden_column_name);
          ObColDesc tmp_col_desc = tmp_col_ids.at(i);
          if (nullptr == data_column_schema) {
            // may be newly added column, can not find in data table.
          } else if (data_column_schema->is_udt_hidden_column()) {
            // do nothing
            // Hidden columns of udt are written when processing the parent column
          } else if (FALSE_IT(tmp_col_desc.col_id_ = data_column_schema->get_column_id())) {
          } else if (OB_FAIL(col_ids_.push_back(tmp_col_desc))) {
            LOG_WARN("fail to push back col desc", K(ret), K(tmp_col_ids.at(i)), K(tmp_col_desc));
          } else if (data_column_schema->is_extend()) {
            int64_t column_id = 0;
            int64_t index_col = 0;
            if (!data_column_schema->is_xmltype()) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("The udt type is not adapted", K(ret), K(*data_column_schema));
            } else if (OB_FAIL(ObTableSchema::get_xml_hidden_column_id(data_table_schema,
                                                                       data_column_schema,
                                                                       column_id))) {
              LOG_WARN("failed to get xml hidden column id.", K(ret));
            } else if (OB_FAIL(ObTableSchema::find_xml_hidden_column_index(hidden_table_schema,
                                                                           hidden_column_schema,
                                                                           tmp_col_ids,
                                                                           index_col))) {
              LOG_WARN("failed to find xml hidden column index.", K(ret));
            } else if (index_col >= tmp_col_ids.count() || index_col < 0 || column_id < 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid index_col.", K(ret), K(index_col), K(tmp_col_ids.count()), K(column_id));
            } else {
              ObColDesc find_col_desc = tmp_col_ids.at(index_col);
              find_col_desc.col_id_ = column_id;
              if (OB_FAIL(col_ids_.push_back(find_col_desc))) {
                LOG_WARN("fail to push back col desc", K(ret), K(find_col_desc));
              }
            }
          }
        }
      }
    }
    // generate output_projector.
    if (OB_FAIL(ret)) {
    } else {
      // notice that, can not find newly added column, get the row firstly, and then resolve it.
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_col_ids.count(); i++) {
        const ObColumnSchemaV2 *hidden_column_schema = hidden_table_schema->get_column_schema(tmp_col_ids.at(i).col_id_);
        const ObString &hidden_column_name = hidden_column_schema->get_column_name_str();
        for (int64_t j = 0; OB_SUCC(ret) && j < col_ids_.count(); j++) {
          const ObColumnSchemaV2 *data_column_schema = data_table_schema->get_column_schema(col_ids_.at(j).col_id_);
          if (nullptr == data_column_schema) {
            // may be newly added column.
          } else if (data_column_schema->is_udt_hidden_column()) {
            if (hidden_column_schema->is_udt_hidden_column()) {
              // hidden parent
              ObColumnSchemaV2 *parent_hidden_column_schema = nullptr;
              // data parent
              ObColumnSchemaV2 *parent_data_column_schema = nullptr;
              if (OB_ISNULL(parent_hidden_column_schema =
                            hidden_table_schema->get_xml_hidden_column_parent_col_schema(hidden_column_schema->get_column_id(),
                                                                                         hidden_column_schema->get_udt_set_id()))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get hidden column parent is null", K(ret), K(*hidden_column_schema), K(*hidden_table_schema));
              } else if (OB_ISNULL(parent_data_column_schema =
                                   data_table_schema->get_xml_hidden_column_parent_col_schema(data_column_schema->get_column_id(),
                                                                                              data_column_schema->get_udt_set_id()))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get data_ column parent is null", K(ret), K(*data_column_schema), K(*data_table_schema));
              } else if (parent_hidden_column_schema->get_column_name_str() == parent_data_column_schema->get_column_name_str()) {
                if (OB_FAIL(output_projector_.push_back(static_cast<int32_t>(j)))) {
                  LOG_WARN("fail to push back output projector", K(ret));
                }
                break;
              }
            }
          } else if (hidden_column_name == data_column_schema->get_column_name_str()) {
            if (OB_FAIL(output_projector_.push_back(static_cast<int32_t>(j)))) {
              LOG_WARN("fail to push back output projector", K(ret));
            }
            break;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(col_ids_.count() != output_projector_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K_(col_ids), K_(output_projector));
    }
  }
  return ret;
}

//For reordering column operations, such as drop column or add column after, we need to rewrite all
//storage data based on the newest table schema.
int ObComplementWriteTask::local_scan_by_range()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t concurrent_cnt = 0;
  if (OB_ISNULL(param_) || OB_ISNULL(context_) || OB_UNLIKELY(!param_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param_), KPC(context_));
  } else {
    concurrent_cnt = param_->concurrent_cnt_;
    LOG_INFO("start to do local scan by range", K(task_id_), K(concurrent_cnt), KPC(param_));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(generate_col_param())) {
    LOG_WARN("fail to get column ids", K(ret));
  } else if (OB_FAIL(do_local_scan())) {
    LOG_WARN("fail to do local scan", K(ret), K_(col_ids), K_(org_col_ids));
  } else {
    int64_t cost_time = ObTimeUtility::current_time() - start_time;
    LOG_INFO("finish local scan by range", K(ret), K(cost_time), K(task_id_), K(concurrent_cnt));
  }
  return ret;
}

int ObComplementWriteTask::do_local_scan()
{
  int ret = OB_SUCCESS;
  int end_trans_ret = OB_SUCCESS;
  SMART_VAR(ObLocalScan, local_scan) {
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        true, /*is daily merge scan*/
        true, /*is read multiple macro block*/
        false, /*sys task scan, read one macro block in single io*/
        false /*is full row scan?*/,
        false,
        false);
    ObStoreRange range;
    ObArenaAllocator allocator("cmplt_write", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObDatumRange datum_range;
    const bool allow_not_ready = false;
    ObLSHandle ls_handle;
    ObTabletTableIterator iterator;
    ObSSTable *sstable = nullptr;
    ObTransService *trans_service = nullptr;
    ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> sstables;
    const uint64_t tenant_id = param_->dest_tenant_id_;
    const int64_t schema_version = param_->dest_schema_version_;
    ObTxDesc *read_tx_desc = nullptr; // for reading lob column from aux_lob_table by table_scan

    if (OB_FAIL(MTL(ObLSService *)->get_ls(param_->orig_ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("fail to get log stream", K(ret), KPC(param_));
    } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls_handle));
    } else if (OB_FAIL(DDL_SIM(tenant_id, param_->task_id_, COMPLEMENT_DATA_TASK_LOCAL_SCAN_FAILED))) {
      LOG_WARN("ddl sim failure", K(ret), KPC(param_));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_read_tables(param_->orig_tablet_id_,
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
        param_->snapshot_version_, iterator, allow_not_ready))) {
      if (OB_REPLICA_NOT_READABLE == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("snapshot version has been discarded", K(ret));
      }
    } else if (OB_ISNULL(trans_service = MTL(ObTransService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans_service is null", K(ret));
    } else if (OB_FAIL(param_->ranges_.at(task_id_, range))) {
      LOG_WARN("fail to get range", K(ret));
    } else if (OB_FAIL(datum_range.from_range(range, allocator))) {
      STORAGE_LOG(WARN, "Failed to transfer datum range", K(ret), K(range));
    } else {
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *data_table_schema = nullptr;
      const ObTableSchema *hidden_table_schema = nullptr;
      if (OB_UNLIKELY(param_->orig_tenant_id_ != param_->dest_tenant_id_
                  || param_->orig_schema_version_ != param_->dest_schema_version_)) {
        ret = OB_ERR_SYS;
        LOG_WARN("err sys", K(ret), KPC(param_));
      } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                tenant_id, schema_guard, schema_version))) {
        LOG_WARN("fail to get tenant schema guard", K(ret), KPC(param_));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                param_->orig_table_id_, data_table_schema))) {
        LOG_WARN("fail to get data table schema", K(ret), KPC(param_));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("data table schema not exist", K(ret), KPC(param_));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                param_->dest_table_id_, hidden_table_schema))) {
        LOG_WARN("fail to get hidden table schema", K(ret), KPC(param_));
      } else if (OB_ISNULL(hidden_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("hidden table schema not exist", K(ret), KPC(param_));
      } else if (OB_FAIL(local_scan.init(col_ids_,
                                        org_col_ids_,
                                        output_projector_,
                                        *data_table_schema,
                                        param_->snapshot_version_,
                                        trans_service,
                                        *hidden_table_schema,
                                        false/*output all columns of hidden table*/))) {
        LOG_WARN("fail to init local scan param", K(ret), K(*param_));
      } else if (OB_FAIL(ObInsertLobColumnHelper::start_trans(
          param_->orig_ls_id_, true/*is_for_read*/, INT64_MAX, read_tx_desc))) {
        LOG_WARN("fail to get tx_desc", K(ret));
      } else if (OB_FAIL(local_scan.table_scan(*data_table_schema,
                                               param_->orig_ls_id_,
                                               param_->orig_tablet_id_,
                                               iterator,
                                               query_flag,
                                               datum_range, read_tx_desc))) {
        LOG_WARN("fail to do table scan", K(ret));
      }
    }
    if (FAILEDx(append_row(&local_scan))) {
      LOG_WARN("append row failed", K(ret));
    }

    const int64_t timeout_ts = ObTimeUtility::current_time() + 3000000; // 3s
    if (nullptr != read_tx_desc) {
      if (OB_SUCCESS != (end_trans_ret = ObInsertLobColumnHelper::end_trans(read_tx_desc, OB_SUCCESS != ret, timeout_ts))) {
        LOG_WARN("fail to end read trans", K(ret), K(end_trans_ret));
        ret = end_trans_ret;
      }
    }
  }

  return ret;
}

int ObComplementWriteTask::remote_scan()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(param_) || OB_ISNULL(context_) || OB_UNLIKELY(!param_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param_), KPC(context_));
  } else if (OB_FAIL(generate_col_param())) {
    LOG_WARN("fail to get column ids", K(ret));
  } else if (OB_FAIL(do_remote_scan())) {
    LOG_WARN("fail to do remote scan", K_(task_id), KPC(param_));
  } else {
    LOG_INFO("finish remote scan", K(ret), "cost_time", ObTimeUtility::current_time() - start_time , K_(task_id));
  }
  return ret;
}

int ObComplementWriteTask::do_remote_scan()
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObRemoteScan, remote_scan) {
    remote_scan.reset();
    if (OB_FAIL(remote_scan.init(param_->orig_tenant_id_,
                                  param_->orig_table_id_,
                                  param_->dest_tenant_id_,
                                  param_->dest_table_id_,
                                  param_->orig_schema_version_,
                                  param_->dest_schema_version_,
                                  param_->orig_tablet_id_))) {
      LOG_WARN("fail to remote_scan init", K(ret), KPC(param_));
    } else if (OB_FAIL(append_row(&remote_scan))) {
      LOG_WARN("append row remote scan failed", K(ret));
    }
  }
  return ret;
}

int ObComplementWriteTask::add_extra_rowkey(const int64_t rowkey_cnt,
                                            const int64_t extra_rowkey_cnt,
                                            const blocksstable::ObDatumRow &row,
                                            const int64_t sql_no)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_column_count = rowkey_cnt;
  if (OB_UNLIKELY(write_row_.get_capacity() < row.count_ + extra_rowkey_cnt ||
                  row.count_ < rowkey_column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row", K(ret), K(write_row_), K(row.count_), K(rowkey_column_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; i++) {
      if (i < rowkey_column_count) {
        write_row_.storage_datums_[i] = row.storage_datums_[i];
      } else {
        write_row_.storage_datums_[i + extra_rowkey_cnt] = row.storage_datums_[i];
      }
    }
    write_row_.storage_datums_[rowkey_column_count].set_int(-param_->snapshot_version_);
    write_row_.storage_datums_[rowkey_column_count + 1].set_int(0);
    write_row_.count_ = row.count_ + extra_rowkey_cnt;
  }
  return ret;
}

int ObComplementWriteTask::append_lob(
    const int64_t schema_rowkey_cnt,
    const int64_t extra_rowkey_cnt,
    ObDDLInsertRowIterator &iterator,
    ObArenaAllocator &lob_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(extra_rowkey_cnt + org_col_ids_.count() != write_row_.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(schema_rowkey_cnt), K(extra_rowkey_cnt), K(org_col_ids_), K(write_row_));
  } else {
    ObArray<int64_t> lob_column_idxs;
    ObArray<common::ObObjMeta> col_types;
    lob_column_idxs.set_attr(ObMemAttr(param_->dest_tenant_id_, "DL_lob_idxs"));
    col_types.set_attr(ObMemAttr(param_->dest_tenant_id_, "DL_col_types"));
    const int64_t storage_rowkey_cnt = schema_rowkey_cnt + extra_rowkey_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < write_row_.count_; i++) {
      int64_t index = 0;
      ObStorageDatum &datum = write_row_.storage_datums_[i];
      if (i < storage_rowkey_cnt || datum.is_nop() || datum.is_null()) {
        // do nothing
      } else if (OB_UNLIKELY((index = i - extra_rowkey_cnt) >= org_col_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), K(index), K(extra_rowkey_cnt), K(org_col_ids_));
      } else if (!org_col_ids_.at(index).col_type_.is_lob_storage()) {
        // not lob.
      } else if (OB_FAIL(lob_column_idxs.push_back(i))) {
        LOG_WARN("fail to push back storage_index", K(ret), K(i));
      } else if (OB_FAIL(col_types.push_back(org_col_ids_.at(index).col_type_))) {
        LOG_WARN("fail to push back col_type", K(ret), K(index), K(org_col_ids_.at(index)));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (lob_column_idxs.empty()) {
      // no lob.
    } else if (iterator.get_lob_id_cache().remain_count() < lob_column_idxs.count()
        && OB_FAIL(iterator.switch_to_new_lob_slice())) {
      LOG_WARN("switch to new lob slice failed", K(ret), K(iterator));
    } else {
      lob_allocator.reuse();
      ObDirectLoadSliceInfo slice_info;
      slice_info.is_full_direct_load_ = true;
      slice_info.is_lob_slice_ = true;
      slice_info.ls_id_ = param_->dest_ls_id_;
      slice_info.data_tablet_id_ = param_->dest_tablet_id_;
      slice_info.slice_id_ = iterator.get_lob_slice_id();
      slice_info.context_id_ = context_->context_id_;
      slice_info.src_tenant_id_ = param_->orig_tenant_id_;
      if (OB_FAIL(MTL(ObTenantDirectLoadMgr *)->fill_lob_sstable_slice(lob_allocator, slice_info,
          iterator.get_lob_id_cache(), lob_column_idxs, col_types, write_row_))) {
        LOG_WARN("fill batch lob sstable slice failed", K(ret), K(slice_info), K(write_row_));
      }
    }
  }
  return ret;
}

int ObComplementWriteTask::append_row(ObScan *scan)
{
  int ret = OB_SUCCESS;
  ObComplementDataDag *current_dag = nullptr;
  const int64_t CHECK_DAG_NEED_EXIT_INTERVAL = 10000; // 1w rows.
  ObDataStoreDesc data_desc;
  HEAP_VARS_4((ObMacroBlockWriter, writer),
              (ObSchemaGetterGuard, schema_guard),
              (ObRelativeTable, relative_table),
              (blocksstable::ObNewRowBuilder, new_row_builder)) {
  HEAP_VAR(ObWholeDataStoreDesc, data_desc, true) {
    ObArray<int64_t> report_col_checksums;
    ObArray<int64_t> report_col_ids;
    ObDDLRedoLogWriterCallback callback;
    ObITable::TableKey hidden_table_key;
    ObMacroDataSeq macro_start_seq(0);
    int64_t get_next_row_time = 0;
    int64_t append_row_time = 0;
    int64_t t1 = 0;
    int64_t t2 = 0;
    int64_t t3 = 0;
    int64_t lob_cnt = 0;
    int64_t row_scanned = 0;
    int64_t row_inserted = 0;
    ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObStoreRow reshaped_row;
    reshaped_row.flag_.set_flag(ObDmlFlag::DF_INSERT);
    ObArenaAllocator allocator(lib::ObLabel("CompDataTaskTmp"), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObRowReshape *reshape_ptr = nullptr;
    ObSQLMode sql_mode_for_ddl_reshape = SMO_TRADITIONAL;
    ObDatumRow datum_row;
    int64_t rowkey_column_cnt = 0;
    const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    bool ddl_committed = false;
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    ObTabletDirectLoadMgrHandle direct_load_hdl;
    bool is_major_sstable_exist = false;
    ObDDLInsertRowIterator row_iter;
    blocksstable::ObNewRowBuilder new_row_builder;
    int64_t lob_inrow_threshold = OB_DEFAULT_LOB_INROW_THRESHOLD;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObComplementWriteTask is not inited", K(ret));
    } else if (OB_ISNULL(scan)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret));
    } else if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(row_iter.init(nullptr/*ObPxMultiPartSSTableInsertOp*/,
                              false/*is_slice_empty*/,
                              param_->dest_ls_id_,
                              param_->dest_tablet_id_,
                              0/*unused_rowkey_num*/,
                              param_->snapshot_version_,
                              context_->context_id_,
                              task_id_))) {
      LOG_WARN("init failed", K(ret));
    } else if (OB_FAIL(macro_start_seq.set_parallel_degree(task_id_))) {
      LOG_WARN("set parallel degree failed", K(ret), K(task_id_));
    } else if (OB_FAIL(context_->check_already_committed(param_->dest_ls_id_, param_->dest_tablet_id_, ddl_committed))) {
      LOG_WARN("check tablet already committed failed", K(ret));
    } else {
      const ObTableSchema *hidden_table_schema = nullptr;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                param_->dest_tenant_id_, schema_guard, param_->dest_schema_version_))) {
        LOG_WARN("fail to get tenant schema guard", K(ret), KPC(param_));
      } else if (OB_FAIL(schema_guard.get_table_schema(param_->dest_tenant_id_,
                param_->dest_table_id_, hidden_table_schema))) {
        LOG_WARN("fail to get hidden table schema", K(ret), KPC(param_));
      } else if (OB_ISNULL(hidden_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(param_->dest_tenant_id_), K(param_->dest_table_id_));
      } else if (OB_FAIL(data_desc.init(*hidden_table_schema,
                                        param_->dest_ls_id_,
                                        param_->dest_tablet_id_,
                                        MAJOR_MERGE,
                                        param_->snapshot_version_,
                                        param_->data_format_version_))) {
        LOG_WARN("fail to init data store desc", K(ret), K(*param_), K(param_->dest_tablet_id_));
      } else if (FALSE_IT(data_desc.get_desc().sstable_index_builder_ = context_->index_builder_)) {
      } else if (OB_FAIL(param_->get_hidden_table_key(hidden_table_key))) {
        LOG_WARN("fail to get hidden table key", K(ret), K(*param_));
      } else if (OB_UNLIKELY(!hidden_table_key.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hidden table key is invalid", K(ret), K(hidden_table_key));
      } else if (OB_ISNULL(current_dag = static_cast<ObComplementDataDag *>(get_dag()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the dag of this task is null", K(ret));
      } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
              param_->dest_ls_id_,
              param_->dest_tablet_id_,
              true, /* is_full_direct_load */
              direct_load_hdl,
              is_major_sstable_exist))) {
        if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
          ret = OB_TASK_EXPIRED;
          LOG_INFO("major sstable already exist", K(ret), KPC(param_));
        } else {
          LOG_WARN("get tablet mgr failed", K(ret), KPC(param_));
        }
      } else if (OB_UNLIKELY(!direct_load_hdl.get_full_obj()->get_start_scn().is_valid_and_not_min())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), K(direct_load_hdl.get_full_obj()->get_start_scn()));
      } else if (OB_UNLIKELY(context_->start_scn_ != direct_load_hdl.get_full_obj()->get_start_scn())) {
        ret = OB_TASK_EXPIRED;
        LOG_WARN("task expired", K(ret), K(context_->start_scn_), "start_scn", direct_load_hdl.get_full_obj()->get_start_scn());
      } else if (OB_FAIL(callback.init(param_->dest_ls_id_,
                                       param_->dest_tablet_id_,
                                       DDL_MB_DATA_TYPE,
                                       hidden_table_key,
                                       param_->task_id_,
                                       context_->start_scn_,
                                       param_->data_format_version_,
                                       direct_load_hdl.get_full_obj()->get_direct_load_type()))) {
        LOG_WARN("fail to init data callback", K(ret), K(hidden_table_key));
      } else if (OB_FAIL(writer.open(data_desc.get_desc(), macro_start_seq, &callback))) {
        LOG_WARN("fail to open macro block writer", K(ret), K(data_desc));
      } else {
        rowkey_column_cnt = hidden_table_schema->get_rowkey_column_num();
        lob_inrow_threshold = hidden_table_schema->get_lob_inrow_threshold();
      }
      ObTableSchemaParam schema_param(allocator);
      // Hack to prevent row reshaping from converting empty string to null.
      //
      // Supposing we have a row of type varchar with some spaces and an index on this column,
      // and then we convert this column to char. In this case, the DDL routine will first rebuild
      // the data table and then rebuilding the index table. The row may be reshaped as follows.
      //
      // - without hack: '  '(varchar) => ''(char) => null(char)
      // - with hack: '  '(varchar) => ''(char) => ''(char)
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_param.convert(hidden_table_schema))) {
        LOG_WARN("failed to convert schema param", K(ret));
        if (OB_SCHEMA_ERROR == ret) {
          ret = OB_CANCELED;
        }
      } else if (OB_FAIL(relative_table.init(&schema_param, param_->dest_tablet_id_))) {
        LOG_WARN("fail to init relative_table", K(ret), K(schema_param), K(param_->dest_tablet_id_));
      } else if (OB_FAIL(ObRowReshapeUtil::malloc_rows_reshape_if_need(
                    allocator, data_desc.get_desc().get_full_stored_col_descs(), 1, relative_table, sql_mode_for_ddl_reshape, reshape_ptr))) {
        LOG_WARN("failed to malloc row reshape", K(ret));
      } else if (OB_FAIL(datum_row.init(allocator, data_desc.get_desc().get_full_stored_col_descs().count()))) {
        LOG_WARN("failed to init datum row", K(ret), K(data_desc.get_desc().get_full_stored_col_descs()));
      } else if (OB_FAIL(new_row_builder.init(data_desc.get_desc().get_full_stored_col_descs(), allocator))) {
        LOG_WARN("Failed to init ObNewRowBuilder", K(ret), K(data_desc.get_desc().get_full_stored_col_descs()));
      }
    }
    while (OB_SUCC(ret)) {      //get each row from row_iter
      const common::ObIArray<share::schema::ObColDesc> &cols_desc = data_desc.get_desc().get_full_stored_col_descs();
      const ObDatumRow *tmp_row = nullptr;
      const ObDatumRow *reshape_row_only_for_remote_scan = nullptr;
      common::ObNewRow *tmp_store_row;
      ObColumnChecksumCalculator *checksum_calculator = nullptr;
      t1 = ObTimeUtility::current_time();
      if (OB_FAIL(dag_yield())) {
        LOG_WARN("fail to yield dag", KR(ret));
      } else if (OB_FAIL(DDL_SIM(param_->dest_tenant_id_, param_->task_id_, DDL_INSERT_SSTABLE_GET_NEXT_ROW_FAILED))) {
        LOG_WARN("ddl sim failure", K(ret), KPC(param_));
      } else if (OB_FAIL(scan->get_next_row(tmp_row, reshape_row_only_for_remote_scan))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_ISNULL(tmp_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp_row is nullptr", K(ret));
      } else if (OB_FAIL(add_extra_rowkey(rowkey_column_cnt, extra_rowkey_cnt, *tmp_row))) {
        LOG_WARN("fail to add extra rowkey", K(ret));
      } else if (!ddl_committed && OB_FAIL(append_lob(rowkey_column_cnt, extra_rowkey_cnt, row_iter, lob_allocator))) {
        LOG_WARN("append lob into macro block failed", K(ret));
      } else if (OB_FAIL(new_row_builder.build(write_row_, tmp_store_row))) {
      } else if (OB_FAIL(ObRowReshapeUtil::reshape_table_rows(
          tmp_store_row, reshape_ptr, cols_desc.count(), &reshaped_row, 1, sql_mode_for_ddl_reshape))) {
        LOG_WARN("failed to malloc and reshape row", K(ret));
      } else if (OB_FAIL(datum_row.from_store_row(reshaped_row))) {
        STORAGE_LOG(WARN, "Failed to transfer store row ", K(ret), K(reshaped_row));
      } else {
        t2 = ObTimeUtility::current_time();
        get_next_row_time += t2 - t1;
        if (++row_scanned % 100 == 0) {
          (void) ATOMIC_AAF(&context_->row_scanned_, 100);
        }
        if (!ddl_committed && OB_FAIL(writer.append_row(datum_row))) {
          LOG_WARN("fail to append row to macro block", K(ret), K(datum_row));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(checksum_calculator = scan->get_checksum_calculator())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("checksum calculator is nullptr", K(ret), KP(checksum_calculator));
        } else if (param_->orig_tenant_id_ == param_->dest_tenant_id_) {
          if (OB_FAIL(checksum_calculator->calc_column_checksum(cols_desc, &write_row_, nullptr, nullptr))) {
            LOG_WARN("fail to calc column checksum", K(ret), K(write_row_));
          }
        } else if (param_->orig_tenant_id_ != param_->dest_tenant_id_) {
          if (OB_ISNULL(reshape_row_only_for_remote_scan)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected err", K(ret), K(write_row_));
          } else if (OB_FAIL(checksum_calculator->calc_column_checksum(org_col_ids_/*without extra rowkey*/,
              reshape_row_only_for_remote_scan, nullptr, nullptr))) {
            LOG_WARN("fail to calc column checksum", K(ret), K(write_row_), KPC(reshape_row_only_for_remote_scan));
          }
        }
        if (OB_SUCC(ret)) {
          t3 = ObTimeUtility::current_time();
          append_row_time += t3 - t2;
          if (++row_inserted % 100 == 0) {
            (void) ATOMIC_AAF(&context_->row_inserted_, 100);
          }
          if (row_inserted % CHECK_DAG_NEED_EXIT_INTERVAL == 0) {
            if (OB_FAIL(current_dag->check_and_exit_on_demand())) {
              LOG_WARN("dag check and exit on demand failed", K(ret));
            }
          }
        }
        lob_allocator.reuse(); // reuse after append_row to macro block to save memory
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      if (row_iter.get_lob_slice_id() > 0 && OB_FAIL(row_iter.close_lob_sstable_slice())) {
        LOG_WARN("close lob sstable slice failed", K(ret));
      }
    }
    (void) ATOMIC_AAF(&context_->row_scanned_, row_scanned % 100);
    (void) ATOMIC_AAF(&context_->row_inserted_, row_inserted % 100);
    LOG_INFO("print append row to macro block cost time", K(ret), K(task_id_), K(context_->row_inserted_),
        K(get_next_row_time), K(append_row_time));
    ObRowReshapeUtil::free_row_reshape(allocator, reshape_ptr, 1);
    if (OB_FAIL(ret)) {
    } else if (!ddl_committed && OB_FAIL(writer.close())) {
      LOG_WARN("fail to close writer", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scan->get_origin_table_checksum(report_col_checksums, report_col_ids))) {
      LOG_WARN("fail to get origin table columns checksum", K(ret));
    }
    /**
     * For DDL_RESTORE_TABLE, orig tenant id is differen to dest tenant id.
     * Meanwhile, the original tenant is a backup tenant, can not support write operation,
     * report its' checksum under the dest tenant, and origin_table_id + ddl_task_id will aviod the conflict.
    */
    else {
      if (param_->use_new_checksum()) {
        // add checksum to context and report checksum in merge task
        if (OB_FAIL(context_->add_column_checksum(report_col_checksums, report_col_ids))) {
          LOG_WARN("add column checksum failed", K(ret));
        } else {
          LOG_INFO("use new checksum", K(param_->orig_table_id_), K(report_col_checksums), K(param_->orig_tablet_id_));
        }
      } else {
        if (OB_FAIL(ObDDLChecksumOperator::update_checksum(param_->dest_tenant_id_,
                param_->orig_table_id_,
                param_->orig_tablet_id_.id(),
                param_->task_id_,
                report_col_checksums,
                report_col_ids,
                1/*execution_id*/,
                param_->tablet_task_id_ << ObDDLChecksumItem::PX_SQC_ID_OFFSET | task_id_,
                param_->data_format_version_,
                *GCTX.sql_proxy_))) {
          LOG_WARN("fail to report origin table checksum", K(ret));
        } else {
          LOG_INFO("update checksum successfully", K(param_->orig_table_id_), K(report_col_checksums), K(param_->orig_tablet_id_));
        }
      }
    }
  }
  }
  return ret;
}

ObComplementMergeTask::ObComplementMergeTask()
  : ObITask(TASK_TYPE_COMPLEMENT_MERGE), is_inited_(false), param_(nullptr), context_(nullptr)
{
}

ObComplementMergeTask::~ObComplementMergeTask()
{
}

int ObComplementMergeTask::init(ObComplementDataParam &param, ObComplementDataContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementMergeTask has already been inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param), K(context));
  } else {
    param_ = &param;
    context_ = &context;
    is_inited_ = true;
  }
  return ret;
}

int ObComplementMergeTask::process()
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  int tmp_ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  ObComplementDataDag *dag = nullptr;
  ObTablet *tablet = nullptr;
  ObArray<int64_t> report_col_checksums;
  ObArray<int64_t> report_col_ids;
  if (OB_ISNULL(tmp_dag) || ObDagType::DAG_TYPE_DDL != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is invalid", K(ret), KP(tmp_dag));
  } else if (FALSE_IT(dag = static_cast<ObComplementDataDag *>(tmp_dag))) {
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", "ret", context_->complement_data_ret_);
  } else if (OB_FAIL(guard.switch_to(param_->dest_tenant_id_, false))) {
    LOG_WARN("switch to tenant failed", K(ret), K(param_->dest_tenant_id_));
  } else if (context_->is_major_sstable_exist_) {
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    const ObSSTable *first_major_sstable = nullptr;
    ObSSTableMetaHandle sst_meta_hdl;
    if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(
        param_->dest_ls_id_, param_->dest_tablet_id_, first_major_sstable, table_store_wrapper))) {
      LOG_WARN("check if major sstable exist failed", K(ret), K(*param_));
    } else if (OB_ISNULL(first_major_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, major sstable shoud not be null", K(ret), K(*param_));
    } else if (OB_FAIL(first_major_sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else if (OB_FAIL(ObTabletDDLUtil::report_ddl_checksum(param_->dest_ls_id_,
                                                            param_->dest_tablet_id_,
                                                            param_->dest_table_id_,
                                                            1 /* execution_id */,
                                                            param_->task_id_,
                                                            sst_meta_hdl.get_sstable_meta().get_col_checksum(),
                                                            sst_meta_hdl.get_sstable_meta().get_col_checksum_cnt(),
                                                            param_->data_format_version_))) {
      LOG_WARN("report ddl column checksum failed", K(ret), K(*param_));
    } else if (OB_FAIL(MTL(ObTabletTableUpdater*)->submit_tablet_update_task(param_->dest_ls_id_, param_->dest_tablet_id_))) {
      LOG_WARN("fail to submit tablet update task", K(ret), K(*param_));
    }
  } else if (param_->use_new_checksum() && OB_FAIL(context_->get_column_checksum(report_col_checksums, report_col_ids))) {
    LOG_WARN("get column checksum failed", K(ret));
  } else if (param_->use_new_checksum() && OB_FAIL(ObDDLChecksumOperator::update_checksum(param_->dest_tenant_id_,
          param_->orig_table_id_,
          param_->orig_tablet_id_.id(),
          param_->task_id_,
          report_col_checksums,
          report_col_ids,
          1/*execution_id*/,
          param_->orig_tablet_id_.id(),
          param_->data_format_version_,
          *GCTX.sql_proxy_))) {
    LOG_WARN("fail to report origin table checksum", K(ret));
  } else if (OB_FAIL(add_build_hidden_table_sstable())) {
    LOG_WARN("fail to build new sstable and write macro redo", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(dag) &&
    OB_SUCCESS != (tmp_ret = dag->report_replica_build_status())) {
    // do not override ret if it has already failed.
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    LOG_WARN("fail to report replica build status", K(ret), K(tmp_ret));
  }

  add_ddl_event(param_, "complement merge task");
  return ret;
}

int ObComplementMergeTask::add_build_hidden_table_sstable()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObITable::TableKey hidden_table_key;
  SCN commit_scn;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementMergetask has not been inited", K(ret));
  } else if (OB_ISNULL(param_)
      || OB_ISNULL(context_)
      || OB_UNLIKELY(!param_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", K(ret), KP(param_), KP(context_));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param_->dest_ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param_->dest_ls_id_));
  } else if (OB_FAIL(param_->get_hidden_table_key(hidden_table_key))) {
    LOG_WARN("fail to get hidden table key", K(ret), K(hidden_table_key));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tenant_direct_load_mgr->close_tablet_direct_load(context_->context_id_, true, /*is_full_direct_load*/
    param_->dest_ls_id_, param_->dest_tablet_id_, true /*need_commit*/, true /*emergent_finish*/))) {
    LOG_WARN("close tablet direct load failed", K(ret), KPC(param_));
  }

  return ret;
}

/**
 * -----------------------------------ObLocalScan-----------------------------------------
 */

ObLocalScan::ObLocalScan() : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), table_id_(OB_INVALID_ID),
    dest_table_id_(OB_INVALID_ID), schema_version_(0), extended_gc_(), snapshot_version_(common::OB_INVALID_VERSION),
    txs_(nullptr), default_row_(), tmp_row_(), row_iter_(nullptr), scan_merge_(nullptr), ctx_(), access_param_(),
    access_ctx_(), get_table_param_(), allocator_("ObLocalScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    calc_buf_(ObModIds::OB_SQL_EXPR_CALC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), col_params_(), read_info_(),
    exist_column_mapping_(allocator_), checksum_calculator_()
{}

ObLocalScan::~ObLocalScan()
{
  if (OB_NOT_NULL(scan_merge_)) {
    scan_merge_->~ObMultipleScanMerge();
    scan_merge_ = NULL;
  }
  for (int64_t i = 0; i < col_params_.count(); i++) {
    ObColumnParam *&tmp_col_param = col_params_.at(i);
    if (OB_NOT_NULL(tmp_col_param)) {
      tmp_col_param->~ObColumnParam();
      allocator_.free(tmp_col_param);
      tmp_col_param = nullptr;
    }
  }
  default_row_.reset();
  tmp_row_.reset();
  access_ctx_.reset();
}

int ObLocalScan::init(
    const ObIArray<share::schema::ObColDesc> &col_ids,
    const ObIArray<share::schema::ObColDesc> &org_col_ids,
    const ObIArray<int32_t> &projector,
    const ObTableSchema &data_table_schema,
    const int64_t snapshot_version,
    ObTransService *txs,
    const ObTableSchema &hidden_table_schema,
    const bool output_org_cols_only)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLocalScan has been initialized before", K(ret));
  } else if (org_col_ids.count() < 1 || col_ids.count() < 1 || projector.count() < 1
      || !data_table_schema.is_valid() || !hidden_table_schema.is_valid() || snapshot_version < 1 || OB_ISNULL(txs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid auguments", K(ret), K(data_table_schema), K(hidden_table_schema),
        K(col_ids), K(org_col_ids), K(projector), K(snapshot_version));
  } else {
    snapshot_version_ = snapshot_version;
    txs_ = txs;
    output_org_cols_only_ = output_org_cols_only;
    ObDatumRow tmp_default_row;
    if (OB_FAIL(check_generated_column_exist(hidden_table_schema, org_col_ids))) {
      LOG_WARN("fail to init generated columns", K(ret), K(org_col_ids));
    } else if (OB_FAIL(extended_gc_.extended_col_ids_.assign(col_ids))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(extended_gc_.org_extended_col_ids_.assign(org_col_ids))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(extended_gc_.output_projector_.assign(projector))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(tmp_default_row.init(allocator_, org_col_ids.count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (OB_FAIL(default_row_.init(allocator_, org_col_ids.count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (OB_FAIL(tmp_row_.init(allocator_, org_col_ids.count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (OB_FAIL(get_exist_column_mapping(data_table_schema, hidden_table_schema))){
      LOG_WARN("fail to init positions for resolving row", K(ret));
    } else if (OB_FAIL(checksum_calculator_.init(extended_gc_.org_extended_col_ids_.count()
            + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to init checksum calculator", K(ret));
    } else {
      tmp_default_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT); // default_row.row_flag_ will be set by deep_copy
      tmp_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      if (OB_FAIL(hidden_table_schema.get_orig_default_row(org_col_ids, tmp_default_row))) {
        LOG_WARN("fail to get default row from table schema", K(ret));
      } else if (OB_FAIL(default_row_.deep_copy(tmp_default_row, allocator_))) {
        LOG_WARN("failed to deep copy default row", K(ret));
      } else {
        tenant_id_ = data_table_schema.get_tenant_id();
        table_id_ = data_table_schema.get_table_id();
        dest_table_id_ = hidden_table_schema.get_table_id();
        schema_version_ = hidden_table_schema.get_schema_version();
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObLocalScan::get_output_columns(
    const ObTableSchema &hidden_table_schema,
    ObIArray<ObColDesc> &col_ids)
{
  int ret = OB_SUCCESS;
  col_ids.reset();
  if (output_org_cols_only_) {
    if (OB_FAIL(col_ids.assign(extended_gc_.extended_col_ids_))) {
      LOG_WARN("assign tmp col ids failed", K(ret));
    }
  } else {
    if (OB_FAIL(hidden_table_schema.get_store_column_ids(col_ids, false))) {
      LOG_WARN("fail to get column ids", K(ret), K(hidden_table_schema));
    }
  }
  return ret;
}

// record the position of data table columns in hidden table by exist_column_mapping_.
int ObLocalScan::get_exist_column_mapping(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &hidden_table_schema)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObArray<ObColDesc> tmp_col_ids;

  if (OB_FAIL(get_output_columns(hidden_table_schema, tmp_col_ids))) {
    LOG_WARN("get output columns failed", K(ret), K(hidden_table_schema));
  } else if (exist_column_mapping_.is_inited() && OB_FAIL(exist_column_mapping_.reserve(tmp_col_ids.count()))) {
    LOG_WARN("fail to expand size of bitmap", K(ret));
  } else if (!exist_column_mapping_.is_inited() && OB_FAIL(exist_column_mapping_.init(tmp_col_ids.count(), false))) {
    LOG_WARN("fail to init exist column mapping", K(ret));
  } else {
    exist_column_mapping_.reuse(false);
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_col_ids.count(); i++) {
      const ObColumnSchemaV2 *hidden_column_schema = hidden_table_schema.get_column_schema(tmp_col_ids.at(i).col_id_);
      const ObString &hidden_column_name = hidden_column_schema->get_column_name_str();
      const ObColumnSchemaV2 *data_column_schema = data_table_schema.get_column_schema(hidden_column_name);
      if (nullptr == data_column_schema) {
        // newly added column, can not find in data table.
      } else if (data_column_schema->is_udt_hidden_column()) {
        // do nothing
        // Hidden columns of udt are written when processing the parent column
      } else if (OB_FAIL(exist_column_mapping_.set(i))) {
        LOG_WARN("fail to set bit map", K(ret), K(*data_column_schema));
      } else if (data_column_schema->is_extend()) {
        ObSEArray<ObColumnSchemaV2 *, 1> hidden_cols;
        int64_t index_col = 0;
        if (!data_column_schema->is_xmltype()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("The udt type is not adapted", K(ret), K(*data_column_schema));
        } else if (OB_FAIL(ObTableSchema::find_xml_hidden_column_index(&hidden_table_schema,
                                                                       hidden_column_schema,
                                                                       tmp_col_ids,
                                                                       index_col))) {
          LOG_WARN("failed to find xml hidden column index.", K(ret));
        } else if (OB_FAIL(exist_column_mapping_.set(index_col))) {
          LOG_WARN("fail to set bit map", K(ret), K(*data_column_schema));
        }
      } else {/* do nothing. */}
    }
  }
  return ret;
}

int ObLocalScan::check_generated_column_exist(
    const ObTableSchema &hidden_table_schema,
    const ObIArray<share::schema::ObColDesc> &org_col_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < org_col_ids.count(); ++i) {
    const ObColumnSchemaV2 *column_schema = nullptr;
    if (OB_ISNULL(column_schema = hidden_table_schema.get_column_schema(org_col_ids.at(i).col_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column schema should not be null", K(ret), K(org_col_ids.at(i)));
    } else if (OB_UNLIKELY(column_schema->is_stored_generated_column())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table redefinition is selected for table with stored column", K(ret), K(*column_schema));
    } else {/* do nothing. */}
  }
  return ret;
}

int ObLocalScan::table_scan(
    const ObTableSchema &data_table_schema,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObTabletTableIterator &table_iter,
    ObQueryFlag &query_flag,
    blocksstable::ObDatumRange &range,
    transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(construct_column_schema(data_table_schema))) {
    LOG_WARN("fail to construct column schema", K(ret), K(col_params_));
  } else if (OB_FAIL(construct_access_param(data_table_schema, tablet_id))) {
    LOG_WARN("fail to construct access param", K(ret), K(col_params_));
  } else if (OB_FAIL(construct_range_ctx(query_flag, ls_id, tx_desc))) {
    LOG_WARN("fail to construct range ctx", K(ret), K(query_flag));
  } else if (OB_FAIL(construct_multiple_scan_merge(table_iter, range))) {
    LOG_WARN("fail to construct multiple scan merge", K(ret), K(table_iter), K(range));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, extended_gc_.org_extended_col_ids_, default_row_))) {
    LOG_WARN("fail to fill lob header for default row", K(ret));
  }
  return ret;
}

//convert column schema to column param
int ObLocalScan::construct_column_schema(const ObTableSchema &data_table_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> &extended_col_ids = extended_gc_.extended_col_ids_;
  for (int64_t i = 0; OB_SUCC(ret) && i < extended_col_ids.count(); i++) {
    const ObColumnSchemaV2 *col = data_table_schema.get_column_schema(extended_col_ids.at(i).col_id_);
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(ret), K(extended_col_ids.at(i).col_id_));
    } else {
      void *buf = allocator_.alloc(sizeof(ObColumnParam));
      ObColumnParam *tmp_col_param = nullptr;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        tmp_col_param = new (buf) ObColumnParam(allocator_);
        if (OB_FAIL(ObTableParam::convert_column_schema_to_param(*col, *tmp_col_param))) {
          LOG_WARN("fail to convert column schema to param", K(ret));
        } else if (OB_FAIL(col_params_.push_back(tmp_col_param))) {
          LOG_WARN("fail to push to array", K(ret), K(tmp_col_param));
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(tmp_col_param)) {
          tmp_col_param->~ObColumnParam();
          allocator_.free(tmp_col_param);
          tmp_col_param = nullptr;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {     //clear col_params
    for (int64_t i = 0; i < col_params_.count(); i++) {
      ObColumnParam *&tmp_col_param = col_params_.at(i);
      if (OB_NOT_NULL(tmp_col_param)) {
        tmp_col_param->~ObColumnParam();
        allocator_.free(tmp_col_param);
        tmp_col_param = nullptr;
      }
    }
  }
  return ret;
}

//construct table access param
int ObLocalScan::construct_access_param(
    const ObTableSchema &data_table_schema,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  read_info_.reset();
  ObArray<int32_t> cols_index;
  ObArray<ObColDesc> tmp_col_ids;
  ObArray<int32_t> cg_idxs;
  bool is_oracle_mode = false;
  bool has_all_cg = true; /* default is row store*/
  // to construct column index, i.e., cols_index.
  if (OB_FAIL(data_table_schema.get_store_column_ids(tmp_col_ids, false))) {
    LOG_WARN("fail to get store columns id", K(ret), K(tmp_col_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extended_gc_.extended_col_ids_.count(); i++) {
      bool is_found = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < tmp_col_ids.count(); j++) {
        if (extended_gc_.extended_col_ids_.at(i).col_id_ == tmp_col_ids.at(j).col_id_) {
          if (OB_FAIL(cols_index.push_back(j))) {
            LOG_WARN("fail to push back cols index in data table", K(ret), K(cols_index));
          } else {
            is_found = true;
          }
        }
      }
      if (OB_SUCC(ret) && !is_found) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, column is not in data table", K(ret),
          K(extended_gc_.extended_col_ids_.at(i)), K(tmp_col_ids), K(data_table_schema));
      }
    }
  }

  /*construct cg_idx*/
  if (OB_FAIL(ret)) {
  } else if(OB_FAIL(data_table_schema.has_all_column_group(has_all_cg))) {
    LOG_WARN("fail to check whether table has all cg", K(ret), K(data_table_schema));
  } else if (!has_all_cg) {
    for (int64_t i = 0; i < col_params_.count(); i++) {
      int32_t tmp_cg_idx = -1;
      if (OB_FAIL(data_table_schema.get_column_group_index(*col_params_.at(i), tmp_cg_idx))) {
        LOG_WARN("fail to get column group idx", K(ret), K(data_table_schema));
      } else if (OB_FAIL(cg_idxs.push_back(tmp_cg_idx))) {
        LOG_WARN("fail to push back cg idx", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (cols_index.count() != extended_gc_.extended_col_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(cols_index), K(extended_gc_));
  } else if (OB_FAIL(data_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      STORAGE_LOG(WARN, "Failed to check oralce mode", K(ret));
  } else if (OB_FAIL(read_info_.init(allocator_,
                                     data_table_schema.get_column_count(),
                                     data_table_schema.get_rowkey_column_num(),
                                     is_oracle_mode,
                                     extended_gc_.extended_col_ids_, // TODO @yiren, remove column id.
                                     &cols_index,
                                     &col_params_,
                                     has_all_cg ? nullptr : &cg_idxs,
                                     nullptr, /* don't use skip scan*/
                                     has_all_cg))) {
    LOG_WARN("fail to init read info", K(ret));
  } else {
    ObArray<ObColDesc> &extended_col_ids = extended_gc_.extended_col_ids_;
    ObArray<int32_t> &output_projector = extended_gc_.output_projector_;
    access_param_.iter_param_.tablet_id_ = tablet_id;
    access_param_.iter_param_.table_id_ = data_table_schema.get_table_id();
    access_param_.iter_param_.out_cols_project_ = &output_projector;
    access_param_.iter_param_.read_info_ = &read_info_;
    if (OB_FAIL(access_param_.iter_param_.refresh_lob_column_out_status())) {
      STORAGE_LOG(WARN, "Failed to refresh lob column", K(ret), K(access_param_.iter_param_));
    } else {
      access_param_.is_inited_ = true;
    }
  }
  LOG_INFO("construct table access param", K(ret), K(tmp_col_ids), K(cols_index), K(extended_gc_.extended_col_ids_),
      K(extended_gc_.output_projector_), K(access_param_));
  return ret;
}

//construct version range and ctx
int ObLocalScan::construct_range_ctx(ObQueryFlag &query_flag,
                                     const share::ObLSID &ls_id,
                                     transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  common::ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version_;
  trans_version_range.multi_version_start_ = snapshot_version_;
  trans_version_range.base_version_ = 0;
  SCN tmp_scn;
  if (OB_FAIL(tmp_scn.convert_for_tx(snapshot_version_))) {
    LOG_WARN("convert fail", K(ret), K(ls_id), K_(snapshot_version));
  } else if (OB_FAIL(ctx_.init_for_read(ls_id,
                                        access_param_.iter_param_.tablet_id_,
                                        INT64_MAX,
                                        -1,
                                        tmp_scn))) {
    LOG_WARN("fail to init store ctx", K(ret), K(ls_id));
  } else if (FALSE_IT(ctx_.mvcc_acc_ctx_.tx_desc_ = tx_desc)) {

  } else if (OB_FAIL(access_ctx_.init(query_flag, ctx_, allocator_, allocator_, trans_version_range))) {
    LOG_WARN("fail to init accesss ctx", K(ret));
  } else if (OB_NOT_NULL(access_ctx_.lob_locator_helper_)) {
    int64_t tx_id = (tx_desc == nullptr) ? 0 : tx_desc->tid().get_id();
    access_ctx_.lob_locator_helper_->update_lob_locator_ctx(access_param_.iter_param_.table_id_,
                                                            access_param_.iter_param_.tablet_id_.id(),
                                                            tx_id);
  }
  return ret;
}

//construct multiple scan merge
int ObLocalScan::construct_multiple_scan_merge(
    ObTabletTableIterator &table_iter,
    ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  LOG_INFO("start to do output_store.scan");
  if (OB_FAIL(get_table_param_.tablet_iter_.assign(table_iter))) {
    LOG_WARN("fail to assign tablet iterator", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultipleScanMerge)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObMultipleScanMerge", K(ret));
  } else if (FALSE_IT(scan_merge_ = new(buf)ObMultipleScanMerge())) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to do placement new", K(ret));
  } else if (OB_FAIL(scan_merge_->init(access_param_, access_ctx_, get_table_param_))) {
    LOG_WARN("fail to init scan merge", K(ret), K(access_param_), K(access_ctx_));
  } else if (OB_FAIL(scan_merge_->open(range))) {
    LOG_WARN("fail to open scan merge", K(ret), K(access_param_), K(access_ctx_), K(range));
  } else {
    scan_merge_->disable_padding();
    scan_merge_->disable_fill_virtual_column();
    row_iter_ = scan_merge_;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(scan_merge_)) {
    scan_merge_->~ObMultipleScanMerge();
    allocator_.free(scan_merge_);
    scan_merge_ = nullptr;
    row_iter_ = nullptr;
  }
  return ret;
}

int ObLocalScan::get_origin_table_checksum(
    ObArray<int64_t> &report_col_checksums,
    ObArray<int64_t> &report_col_ids)
{
  int ret = OB_SUCCESS;
  report_col_checksums.reuse();
  report_col_ids.reuse();
  ObArray<ObColDesc> tmp_col_ids;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *hidden_table_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
             tenant_id_, schema_guard, schema_version_))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_), K(schema_version_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
             table_id_, data_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(tenant_id_), K(table_id_));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data table not exist", K(ret), K(tenant_id_), K(table_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
             dest_table_id_, hidden_table_schema))) {
    LOG_WARN("fail to get hidden table schema", K(ret), K(tenant_id_), K(dest_table_id_));
  } else if (OB_ISNULL(hidden_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("hidden table schema not exist", K(ret), K(tenant_id_), K(dest_table_id_));
  } else if (OB_FAIL(get_output_columns(*hidden_table_schema, tmp_col_ids))) {
    LOG_WARN("get output column failed", K(ret));
  } else if (tmp_col_ids.size() != exist_column_mapping_.size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(tmp_col_ids), K(exist_column_mapping_.size()));
  } else {
    const int64_t rowkey_cols_cnt = hidden_table_schema->get_rowkey_column_num();
    const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    // get data table columns id and corresponding checksum.
    for (int64_t i = 0; OB_SUCC(ret) && i < exist_column_mapping_.size(); i++) {
      if (exist_column_mapping_.test(i)) {
        const ObColumnSchemaV2 *hidden_col_schema = hidden_table_schema->get_column_schema(tmp_col_ids.at(i).col_id_);
        const ObString &hidden_column_name = hidden_col_schema->get_column_name_str();
        const ObColumnSchemaV2 *data_col_schema = data_table_schema->get_column_schema(hidden_column_name);
        const int64_t index_in_array = i < rowkey_cols_cnt ? i : i + extra_rowkey_cnt;
        if (hidden_col_schema->is_udt_hidden_column()) {
          // do nothing
          // Hidden columns of udt are written when processing the parent column
        } else if (OB_ISNULL(data_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data column schema should not be null", K(ret), K(hidden_column_name));
        } else if (data_col_schema->is_udt_hidden_column()) {
          // do nothing
        } else if (OB_FAIL(report_col_ids.push_back(data_col_schema->get_column_id()))) {
          LOG_WARN("fail to push back col id", K(ret), K(*data_col_schema));
        } else if (OB_FAIL(report_col_checksums.push_back(checksum_calculator_.get_column_checksum()[index_in_array]))) {
          LOG_WARN("fail to push back col checksum", K(ret));
        } else if (data_col_schema->is_extend()) {
          int64_t column_id = 0;
          int64_t index_col = 0;
          if (!data_col_schema->is_xmltype()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("The udt type is not adapted", K(ret), K(*data_col_schema));
          } else if (OB_FAIL(ObTableSchema::get_xml_hidden_column_id(data_table_schema, data_col_schema, column_id))) {
            LOG_WARN("failed to get xml hidden column id.", K(ret));
          } else if (OB_FAIL(ObTableSchema::find_xml_hidden_column_index(hidden_table_schema,
                                                                         hidden_col_schema,
                                                                         tmp_col_ids,
                                                                         index_col))) {
            LOG_WARN("failed to find xml hidden column index.", K(ret));
          } else if (!exist_column_mapping_.test(index_col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get xml udt hidden column not exist column mapping", K(ret), K(index_col));
          } else {
            const int64_t udt_hidden_index_in_array = index_col < rowkey_cols_cnt ? index_col : index_col + extra_rowkey_cnt;
            if (OB_FAIL(report_col_ids.push_back(column_id))) {
              LOG_WARN("fail to push back udt hidden col id", K(ret), K(*data_col_schema));
            } else if (OB_FAIL(report_col_checksums.push_back(checksum_calculator_.get_column_checksum()[udt_hidden_index_in_array]))) {
              LOG_WARN("fail to push back col checksum", K(ret));
            }
          }
        } else {/* do nothing. */}
      } else {/* do nothing. */}
    }
  }
  return ret;
}

int ObLocalScan::get_next_row(
    const ObDatumRow *&tmp_row,
    const ObDatumRow *&tmp_row_after_reshape)
{
  UNUSED(tmp_row_after_reshape);
  int ret = OB_SUCCESS;
  tmp_row = nullptr;
  calc_buf_.reuse();
  const ObDatumRow *row = nullptr;
  if (OB_FAIL(row_iter_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_ISNULL(row) || !row->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(row));
  } else {
    for (int64_t i = 0, j = 0; OB_SUCC(ret) && i < exist_column_mapping_.size(); i++) {
      ObObjMeta &obj_meta = extended_gc_.org_extended_col_ids_.at(i).col_type_;
      if (exist_column_mapping_.test(i)) {
        // fill with value stored in origin data table.
        if (OB_UNLIKELY(j >= extended_gc_.extended_col_ids_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret), K(j), K(extended_gc_.extended_col_ids_.count()));
        } else {
          tmp_row_.storage_datums_[i] = row->storage_datums_[j++];
        }
      } else {
        // the column is newly added, thus fill with default value.
        tmp_row_.storage_datums_[i] = default_row_.storage_datums_[i];
      }
      if (OB_FAIL(ret)) {
      } else if (obj_meta.is_fixed_len_char_type()
        && OB_FAIL(ObDDLUtil::reshape_ddl_column_obj(tmp_row_.storage_datums_[i], obj_meta))) {
        LOG_WARN("reshape failed", K(ret), K(obj_meta));
      }
    }
  }
  if (OB_SUCC(ret)) {
    tmp_row = &tmp_row_;
  }
  return ret;
}

ObRemoteScan::ObRemoteScan()
  : is_inited_(false),
    current_(0),
    tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    dest_tenant_id_(OB_INVALID_ID),
    dest_table_id_(OB_INVALID_ID),
    schema_version_(0),
    dest_schema_version_(0),
    row_without_reshape_(),
    row_with_reshape_(),
    res_(),
    result_(nullptr),
    allocator_("DDLRemoteScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    org_col_ids_(),
    column_names_(),
    checksum_calculator_()
{
}

ObRemoteScan::~ObRemoteScan()
{
  reset();
}

void ObRemoteScan::reset()
{
  is_inited_ = false;
  current_ = 0;
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  dest_tenant_id_ = OB_INVALID_ID;
  dest_table_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  dest_schema_version_ = 0;
  row_without_reshape_.reset();
  row_with_reshape_.reset();
  res_.reset();
  result_ = nullptr;
  org_col_ids_.reset();
  column_names_.reset();
  allocator_.reset();
}

int ObRemoteScan::init(const uint64_t tenant_id,
                       const int64_t table_id,
                       const uint64_t dest_tenant_id,
                       const int64_t dest_table_id,
                       const int64_t schema_version,
                       const int64_t dest_schema_version,
                       const ObTabletID &src_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id
      || OB_INVALID_ID == dest_tenant_id || OB_INVALID_ID == dest_table_id
      || schema_version <= 0 || dest_schema_version <= 0 || !src_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id),
      K(dest_tenant_id), K(dest_table_id), K(schema_version), K(dest_schema_version), K(src_tablet_id));
  } else {
    ObSqlString sql_string;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *hidden_table_schema = nullptr;
    bool is_oracle_mode = false;
    if (OB_FAIL((ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        dest_tenant_id, schema_guard, dest_schema_version)))) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(dest_tenant_id), K(dest_schema_version));
    } else if (OB_FAIL(schema_guard.get_table_schema(dest_tenant_id, dest_table_id, hidden_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(hidden_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(dest_tenant_id), K(dest_table_id));
    } else if (OB_FAIL(hidden_table_schema->get_store_column_ids(org_col_ids_))) {
      LOG_WARN("fail to get store column ids", K(ret));
    } else if (OB_FAIL(hidden_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("Failed to check oralce mode", K(ret));
    } else if (OB_UNLIKELY(org_col_ids_.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("org col ids count is 0", K(ret));
    } else if (OB_FAIL(row_without_reshape_.init(allocator_, org_col_ids_.count()))) {
      LOG_WARN("fail to init tmp_row", K(ret), K(org_col_ids_.count()));
    } else if (OB_FAIL(row_with_reshape_.init(allocator_, org_col_ids_.count()))) {
      LOG_WARN("fail to init tmp_row", K(ret), K(org_col_ids_.count()));
    } else if (OB_FAIL(checksum_calculator_.init(org_col_ids_.count()))) {
      LOG_WARN("fail to init checksum_calculator", K(ret));
    } else {
      tenant_id_ = tenant_id;
      table_id_ = table_id;
      dest_tenant_id_ = dest_tenant_id;
      dest_table_id_ = dest_table_id;
      schema_version_ = schema_version;
      dest_schema_version_ = dest_schema_version;
      src_tablet_id_ = src_tablet_id;
      row_without_reshape_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row_with_reshape_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      if (OB_FAIL(generate_build_select_sql(sql_string))) {
        LOG_WARN("fail to generate build replica sql", K(ret), K(sql_string));
      } else if (is_oracle_mode && OB_FAIL(prepare_iter(sql_string, GCTX.ddl_oracle_sql_proxy_))) {
        LOG_WARN("prepare iter under oracle mode failed", K(ret), K(sql_string));
      } else if (!is_oracle_mode && OB_FAIL(prepare_iter(sql_string, GCTX.ddl_sql_proxy_))) {
        LOG_WARN("prepare iter under mysql mode failed", K(ret), K(sql_string));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObRemoteScan::generate_build_select_sql(ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reset();
  ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
  ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
  ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
  ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
  bool is_oracle_mode = false;
  ObArray<ObColDesc> dest_column_ids;
  const ObDatabaseSchema *orig_db_schema = nullptr;
  const share::schema::ObTableSchema *orig_table_schema = nullptr;
  const share::schema::ObTableSchema *dest_table_schema = nullptr;
  MTL_SWITCH (OB_SYS_TENANT_ID) {
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("init twice", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(tenant_id_, dest_tenant_id_,
        hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
        src_tenant_schema_guard, dst_tenant_schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_), K(dest_tenant_id_));
    } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(tenant_id_, table_id_, orig_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id_), K(table_id_));
    } else if (OB_ISNULL(orig_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(tenant_id_), K(table_id_));
    }  else if (OB_FAIL(orig_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("Failed to check oralce mode", K(ret));
    } else if (OB_FAIL(src_tenant_schema_guard->get_database_schema(tenant_id_, orig_table_schema->get_database_id(), orig_db_schema))) {
      LOG_WARN("fail to get database schema", K(ret), K(tenant_id_));
    } else if (OB_ISNULL(orig_db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, database schema must not be nullptr", K(ret), KPC(orig_table_schema));
    } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(dest_tenant_id_, dest_table_id_, dest_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(dest_tenant_id_), K(dest_table_id_));
    } else if (OB_ISNULL(dest_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(dest_tenant_id_), K(dest_table_id_));
    } else if (OB_FAIL(dest_table_schema->get_store_column_ids(dest_column_ids, false))) {
      LOG_WARN("get store column ids failed", K(ret), KPC(dest_table_schema));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < dest_column_ids.count(); i++) {
        const uint64_t dest_column_id = dest_column_ids.at(i).col_id_;
        const ObColumnSchemaV2 *dest_column_schema = dest_table_schema->get_column_schema(dest_column_id);
        if (OB_ISNULL(dest_column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema", K(ret), K(dest_column_id));
        } else {
          const ObString &dest_column_name = dest_column_schema->get_column_name_str();
          const ObColumnSchemaV2 *orig_column_schema = orig_table_schema->get_column_schema(dest_column_name);
          if (OB_ISNULL(orig_column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column not exist", K(ret), K(dest_column_name), KPC(dest_table_schema));
          } else if (OB_FAIL(column_names_.push_back(ObColumnNameInfo(dest_column_name, is_shadow_column(dest_column_id),
              orig_column_schema->is_enum_or_set())))) {
            LOG_WARN("fail to push back column name failed", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObSqlString query_column_sql_string;
        if (OB_FAIL(ObDDLUtil::generate_column_name_str(column_names_, is_oracle_mode, true, true, false/*use_heap_table_ddl_plan*/, query_column_sql_string))) {
          LOG_WARN("fail to generate column name str", K(ret));
        } else {
          ObString orig_database_name_with_escape;
          ObString orig_table_name_with_escape;
          ObSqlString query_partition_sql;
          const bool is_part_table = orig_table_schema->is_partitioned_table();
          const char *split_char = is_oracle_mode ? "\"" : "`";
          if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator_, orig_db_schema->get_database_name_str(), orig_database_name_with_escape, is_oracle_mode))) {
            LOG_WARN("generate new name failed", K(ret));
          } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator_, orig_table_schema->get_table_name_str(), orig_table_name_with_escape, is_oracle_mode))) {
            LOG_WARN("generate new name failed", K(ret));
          } else if (is_part_table) {
            ObString partition_name_with_escape;
            const ObBasePartition *source_partition = nullptr;
            if (OB_FAIL(fetch_source_part_info(src_tablet_id_, *orig_table_schema, source_partition))) {
              LOG_WARN("fetch source part info failed", K(ret));
            } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator_, source_partition->get_part_name(), partition_name_with_escape, is_oracle_mode))) {
              LOG_WARN("generate new name failed", K(ret), KPC(source_partition));
            } else if (OB_FAIL(query_partition_sql.assign_fmt("%s%.*s%s.%s%.*s%s partition (%s%.*s%s)",
                split_char, static_cast<int>(orig_database_name_with_escape.length()), orig_database_name_with_escape.ptr(), split_char,
                split_char, static_cast<int>(orig_table_name_with_escape.length()), orig_table_name_with_escape.ptr(), split_char,
                split_char, static_cast<int>(partition_name_with_escape.length()), partition_name_with_escape.ptr(), split_char))) {
              LOG_WARN("add specified query partition failed", K(ret),
                K(orig_database_name_with_escape), K(orig_table_name_with_escape), K(partition_name_with_escape));
            }
          } else if (OB_FAIL(query_partition_sql.assign_fmt("%s%.*s%s.%s%.*s%s",
              split_char, static_cast<int>(orig_database_name_with_escape.length()), orig_database_name_with_escape.ptr(), split_char,
              split_char, static_cast<int>(orig_table_name_with_escape.length()), orig_table_name_with_escape.ptr(), split_char))) {
            LOG_WARN("add specified query partition failed", K(ret),
              K(orig_database_name_with_escape), K(orig_table_name_with_escape));
          }

          if (FAILEDx(sql_string.assign_fmt("SELECT /*+opt_param('hidden_column_visible' 'true')*/ %.*s from %.*s",
                            static_cast<int>(query_column_sql_string.length()), query_column_sql_string.ptr(),
                            static_cast<int>(query_partition_sql.length()), query_partition_sql.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret), K(query_column_sql_string), K(query_partition_sql));
          } else if (OB_FAIL(sql_string.append("order by "))) {
            LOG_WARN("append failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_schema->get_rowkey_column_num(); i++) {
              if (OB_FAIL(sql_string.append_fmt("%s %ld", i == 0 ? "": ",", i+1))) {
                LOG_WARN("append fmt failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  FLOG_INFO("generate query sql finished", K(ret), K(sql_string));
  return ret;
}

int ObRemoteScan::fetch_source_part_info(
    const common::ObTabletID &src_tablet_id,
    const share::schema::ObTableSchema &src_table_schema,
    const ObBasePartition*& source_partition)
{
  int ret = OB_SUCCESS;
  source_partition = nullptr;
  const ObPartition *part = nullptr;
  const ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  const bool has_subpart = src_table_schema.get_part_level() == share::schema::PARTITION_LEVEL_TWO;
  if (OB_UNLIKELY(!src_tablet_id.is_valid() || !src_table_schema.is_valid() || (!src_table_schema.is_partitioned_table()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(src_tablet_id), K(src_table_schema));
  } else {
    ObPartIterator iter(src_table_schema, check_partition_mode);
    while (OB_SUCC(ret) && nullptr == source_partition) {
      if (OB_FAIL(iter.next(part))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("iter partition failed", K(ret));
        }
      } else if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null partition", K(ret));
      } else if (!has_subpart) {
        if (src_tablet_id == part->get_tablet_id()) {
          source_partition = part;
        }
      } else {
        const ObSubPartition *subpart = nullptr;
        ObSubPartIterator sub_iter(src_table_schema, *part, check_partition_mode);
        while (OB_SUCC(ret) && nullptr == source_partition) {
          if (OB_FAIL(sub_iter.next(subpart))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("iter sub part failed", K(ret));
            }
          } else if (OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null subpartition", K(ret));
          } else if (src_tablet_id == subpart->get_tablet_id()) {
            source_partition = subpart;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && nullptr == source_partition) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(src_tablet_id), K(src_table_schema));
  }
  return ret;
}

int ObRemoteScan::get_next_row(
    const blocksstable::ObDatumRow *&tmp_row_without_reshape,
    const blocksstable::ObDatumRow *&tmp_row_with_reshape)
{
  int ret = OB_SUCCESS;
  tmp_row_without_reshape = nullptr;
  tmp_row_with_reshape = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is nullptr", K(ret));
  } else if (OB_FAIL(result_->next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("result next failed", K(ret));
    }
  } else {
    row_without_reshape_.reuse();
    row_with_reshape_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < org_col_ids_.count(); i++) {
      ObObj obj;
      if (OB_FAIL(result_->get_obj(i, obj))) {
        LOG_WARN("failed to get object", K(ret), "column_id", org_col_ids_.at(i).col_id_);
      } else if (OB_FAIL(row_without_reshape_.storage_datums_[i].from_obj_enhance(obj))) {
        LOG_WARN("failed to from obj enhance", K(ret));
      }
      /**
       * For fix-length type column, the select result is padded to full char length, and row needs to
       * be reshaped to keep the same format as lines written into macro block, avoiding checksum error.
      */
      else if (OB_FAIL(row_with_reshape_.storage_datums_[i].from_obj_enhance(obj))) {
        LOG_WARN("failed to from obj enhance", K(ret));
      } else if (obj.is_fixed_len_char_type()
        && OB_FAIL(ObDDLUtil::reshape_ddl_column_obj(row_with_reshape_.storage_datums_[i], obj.get_meta()))) {
        LOG_WARN("reshape failed", K(ret), K(obj));
      }
    }
    if (OB_SUCC(ret)) {
      tmp_row_without_reshape = &row_without_reshape_;
      tmp_row_with_reshape = &row_with_reshape_;
    }
  }
  return ret;
}

int ObRemoteScan::prepare_iter(const ObSqlString &sql_string, common::ObCommonSqlProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSessionParam session_param;
  ObSQLMode sql_mode = SMO_STRICT_ALL_TABLES;
  session_param.sql_mode_ = reinterpret_cast<int64_t *>(&sql_mode);
  session_param.tz_info_wrap_ = nullptr;
  session_param.ddl_info_.set_is_ddl(true);
  session_param.ddl_info_.set_source_table_hidden(false);
  session_param.ddl_info_.set_dest_table_hidden(false);
  const int64_t sql_total_timeout = ObDDLUtil::calc_inner_sql_execute_timeout();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else if (OB_FAIL(sql_proxy->read(res_, tenant_id_, sql_string.ptr(), &session_param, sql_total_timeout))) {
    LOG_WARN("fail to execute sql", K(ret), K_(tenant_id), K(sql_string));
  } else if (OB_ISNULL(result_ = res_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObMySQLResult is nullptr", K(ret), K(sql_string));
  }
  return ret;
}

int ObRemoteScan::get_origin_table_checksum(ObArray<int64_t> &report_col_checksums, ObArray<int64_t> &report_col_ids)
{
  int ret = OB_SUCCESS;
  report_col_checksums.reuse();
  report_col_ids.reuse();
  ObArray<ObColDesc> tmp_col_ids;
  ObSchemaGetterGuard hold_buf_src_tenant_schema_guard;
  ObSchemaGetterGuard hold_buf_dst_tenant_schema_guard;
  ObSchemaGetterGuard *src_tenant_schema_guard = nullptr;
  ObSchemaGetterGuard *dst_tenant_schema_guard = nullptr;
  const share::schema::ObTableSchema *dest_table_schema = nullptr;
  const share::schema::ObTableSchema *orig_table_schema = nullptr;
  MTL_SWITCH (OB_SYS_TENANT_ID) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_tenant_schema_guard(tenant_id_, dest_tenant_id_,
        hold_buf_src_tenant_schema_guard, hold_buf_dst_tenant_schema_guard,
        src_tenant_schema_guard, dst_tenant_schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_), K(dest_tenant_id_));
    } else if (OB_FAIL(src_tenant_schema_guard->get_table_schema(tenant_id_, table_id_, orig_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id_), K(table_id_));
    } else if (OB_ISNULL(orig_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(tenant_id_), K(table_id_));
    } else if (OB_FAIL(dst_tenant_schema_guard->get_table_schema(dest_tenant_id_, dest_table_id_, dest_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K_(dest_tenant_id), K_(dest_table_id));
    } else if (OB_ISNULL(dest_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("dest table schema is nullptr", K(ret));
    } else if (OB_FAIL(dest_table_schema->get_store_column_ids(tmp_col_ids, false))) {
      LOG_WARN("fail to get store column ids", K(ret), K(tmp_col_ids));
    } else {
      const int64_t rowkey_cols_cnt = dest_table_schema->get_rowkey_column_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_col_ids.size(); i++) {
        const ObColumnSchemaV2 *dest_col_schema = dest_table_schema->get_column_schema(tmp_col_ids.at(i).col_id_);
        const ObString &dest_column_name = dest_col_schema->get_column_name_str();
        const ObColumnSchemaV2 *orig_col_schema = orig_table_schema->get_column_schema(dest_column_name);
        const int64_t index_in_array = i;
        if (OB_ISNULL(orig_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data column schema should not be null", K(ret), K(dest_column_name));
        } else if (OB_FAIL(report_col_ids.push_back(orig_col_schema->get_column_id()))) {
          LOG_WARN("fail to push back col id", K(ret), KPC(orig_col_schema));
        } else if (OB_FAIL(report_col_checksums.push_back(checksum_calculator_.get_column_checksum()[index_in_array]))) {
          LOG_WARN("fail to push back col checksum", K(ret));
        } else {/* do nothing */}
      }
    }
  }
  return ret;
}

} //end namespace stroage
} //end namespace oceanbase
