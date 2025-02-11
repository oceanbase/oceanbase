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
#include "logservice/ob_log_service.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_sim_point.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_tablet_split_task.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "deps/oblib/src/lib/charset/ob_charset.h"

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
      } else if (orig_tenant_id == dest_tenant_id
        && OB_UNLIKELY(dest_table_schema->get_association_table_id() != arg.source_table_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(arg), K(dest_table_schema->get_association_table_id()));
      } else {
        snapshot_version_ = arg.snapshot_version_;
        orig_schema_tablet_size_ = orig_table_schema->get_tablet_size();
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
    user_parallelism_ = arg.parallelism_;
    is_no_logging_ = arg.is_no_logging_;
    FLOG_INFO("succeed to init ObComplementDataParam", K(ret), KPC(this));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
struct DatumRangeCompare
{
public:
  explicit DatumRangeCompare(const ObStorageDatumUtils *datum_utils)
    : ret_(OB_SUCCESS), datum_utils_(datum_utils) {}
  bool operator() (const ObDatumRange &left, const ObDatumRange &right)
  {
    int cmp_ret = 0;
    ret_ = (OB_SUCCESS == ret_) ? left.get_start_key().compare(right.get_start_key(), *datum_utils_, cmp_ret) : ret_;
    return cmp_ret < 0;
  }
public:
  int ret_;
  const ObStorageDatumUtils *datum_utils_;
};
#endif

int ObComplementDataParam::prepare_task_ranges()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(this));
  } else {
    ranges_.reset();
    concurrent_cnt_ = 0;
    if (user_parallelism_ <= 1) {
      ObDatumRange datum_range;
      datum_range.set_whole_range();
      if (OB_FAIL(ranges_.push_back(datum_range))) {
        LOG_WARN("push back range failed", K(ret), K(datum_range));
      } else {
        concurrent_cnt_ = 1;
        LOG_INFO("succeed to to init task ranges", K(ret), K(user_parallelism_), K(concurrent_cnt_), K(ranges_));
      }
    } else if (orig_tenant_id_ == dest_tenant_id_) {
      if (OB_FAIL(split_task_ranges(task_id_, data_format_version_, orig_ls_id_, orig_tablet_id_, orig_schema_tablet_size_, user_parallelism_))) {
        LOG_WARN("fail to init task ranges", K(ret), KPC(this));
      }
    } else if (OB_FAIL(split_task_ranges_remote(orig_tenant_id_,
                                                orig_ls_id_,
                                                orig_tablet_id_,
                                                orig_schema_tablet_size_,
                                                user_parallelism_))) {
      LOG_WARN("fail to init task ranges", K(ret), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    if (orig_tenant_id_ == dest_tenant_id_) {
      SERVER_EVENT_ADD("alter_table", "drop_column_data_complement",
        "tenant_id", dest_tenant_id_,
        "task_id", task_id_,
        "trace_id", *ObCurTraceId::get_trace_id(),
        "user_parallelism", user_parallelism_,
        "concurrent_cnt", concurrent_cnt_
      );
    } else {
      SERVER_EVENT_ADD("recover_table", "recover_table_data_complement",
        "tenant_id", dest_tenant_id_,
        "task_id", task_id_,
        "trace_id", *ObCurTraceId::get_trace_id(),
        "user_parallelism", user_parallelism_,
        "concurrent_cnt", concurrent_cnt_
      );
    }
  }
  return ret;
}

// split task ranges to do table scan based on the whole range on the specified tablet.
int ObComplementDataParam::split_task_ranges(
    const int64_t task_id,
    const uint64_t data_format_version,
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
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(task_id <= 0 || data_format_version <= 0 || !ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), K(data_format_version), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("fail to get log stream", K(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_handle));
  } else if (OB_ISNULL(tablet_service = ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service is nullptr", K(ret));
  } else if (OB_FAIL(DDL_SIM(MTL_ID(), task_id, COMPLEMENT_DATA_TASK_SPLIT_RANGE_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(MTL_ID()), K(task_id));
  } else {
    int64_t total_size = 0;
    int64_t expected_task_count = 0;
    ObStoreRange range;
    range.set_whole_range();
    ObSEArray<common::ObStoreRange, 32> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    ObParallelBlockRangeTaskParams params;
    params.parallelism_ = hint_parallelism;
    params.expected_task_load_ = tablet_size / 1024 <= 0 ? sql::OB_EXPECTED_TASK_LOAD : tablet_size / 1024;
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
    } else if (OB_FALSE_IT(total_size = total_size / 1024 /* Byte -> KB */)) {
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
          const ObStoreRange &store_range = storage_task_ranges.at(j);
          ObDatumRange datum_range;
          if (OB_FAIL(datum_range.from_range(store_range, allocator_))) {
            LOG_WARN("failed to transfer datum range", K(ret), K(store_range));
          } else if (OB_FAIL(ranges_.push_back(datum_range))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      }
#ifdef OB_BUILD_SHARED_STORAGE
      if (OB_SUCC(ret) && ObDDLUtil::use_idempotent_mode(data_format_version)) {
        storage::ObTabletHandle tablet_handle;
        if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle))) {
          LOG_WARN("get tablet failed", K(ret), K(ls_handle), K(tablet_id));
        } else {
          DatumRangeCompare cmp(&tablet_handle.get_obj()->get_rowkey_read_info().get_datum_utils());
          lib::ob_sort(ranges_.begin(), ranges_.end(), cmp);
          if (OB_FAIL(cmp.ret_)) {
            LOG_WARN("sort ranges failed", K(ret), K(task_id), K(tablet_id));
          } else if (!ranges_.at(0).get_start_key().is_min_rowkey()
              || !ranges_.at(ranges_.count() - 1).get_end_key().is_max_rowkey()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sorted range not correct", K(ret), K(ranges_.count()),
                "first_range", ranges_.at(0), "last_range", ranges_.at(ranges_.count() - 1));
          } else if (OB_FAIL(rootserver::ObDDLTaskRecordOperator::get_or_insert_tablet_schedule_info(
                  MTL_ID(), task_id, tablet_id, allocator_, ranges_))) {
            LOG_WARN("get or insert tablet schedule info failed", K(ret), "tenant_id", MTL_ID(), K(task_id), K(tablet_id), K(ranges_));
          }
        }
      }
#endif
      if (OB_SUCC(ret)) {
        concurrent_cnt_ = ranges_.count();
        FLOG_INFO("succeed to get range and concurrent cnt", K(ret), K(task_id), K(data_format_version), K(tablet_id),
            K(total_size), K(hint_parallelism), K(expected_task_count), K(params), K(multi_range_split_array), K(ranges_));
      }
    }
  }
  return ret;
}

int ObComplementDataParam::split_task_ranges_remote(
  const uint64_t src_tenant_id,
  const share::ObLSID &src_ls_id,
  const common::ObTabletID &src_tablet_id,
  const int64_t tablet_size,
  const int64_t hint_parallelism)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(common::OB_INVALID_TENANT_ID == src_tenant_id
    ||!src_ls_id.is_valid() || !src_tablet_id.is_valid() || tablet_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(src_tenant_id), K(ls_id), K(src_tablet_id), K(tablet_size));
  } else {
    common::ObAddr src_leader_addr;
    share::ObLocationService *location_service = nullptr;
    obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
    obrpc::ObPrepareSplitRangesArg arg;
    obrpc::ObPrepareSplitRangesRes result;
    arg.ls_id_ = src_ls_id;
    arg.tablet_id_ = src_tablet_id;
    arg.user_parallelism_ = MIN(MIN(MAX(hint_parallelism, 1), MAX_RPC_STREAM_WAIT_THREAD_COUNT),
      ObMacroDataSeq::MAX_PARALLEL_IDX + 1);
    arg.schema_tablet_size_ = RECOVER_TABLE_PARALLEL_MIN_TASK_SIZE; /*2M*/
    arg.ddl_type_ = ObDDLType::DDL_TABLE_RESTORE;
    const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
    const int64_t retry_interval_us = 200 * 1000; // 200ms
    /* recover table partition data complete: dest leader server send rpc to src leader server */
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      if (OB_ISNULL(location_service = GCTX.location_service_)) {
        ret = OB_ERR_SYS;
        LOG_WARN("location_cache is null", K(ret), KP(location_service));
      } else if (OB_FAIL(location_service->get_leader_with_retry_until_timeout(GCONF.cluster_id,
        src_tenant_id, src_ls_id, src_leader_addr, rpc_timeout, retry_interval_us))) {
        LOG_WARN("fail to get ls locaiton leader", K(ret), K(src_tenant_id), K(src_ls_id));
      }
    }
    if (OB_SUCC(ret)){
      if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
        ret = OB_ERR_SYS;
        LOG_WARN("storage_rpc_proxy is null", K(ret), KP(location_service));
      } else if (OB_FAIL(srv_rpc_proxy->to(src_leader_addr)
                                      .by(src_tenant_id)
                                      .timeout(GCONF._ob_ddl_timeout)
                                      .prepare_tablet_split_task_ranges(arg, result))) {
        LOG_WARN("failed to prepare tablet split task ranges", K(ret), K(arg));
      } else if (OB_FAIL(ObTabletSplitUtil::convert_datum_rowkey_to_range(
        allocator_, result.parallel_datum_rowkey_list_, ranges_))) {
        LOG_WARN("convert to range failed", K(ret), "parall_info", result.parallel_datum_rowkey_list_);
      } else if (ranges_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected range split array", K(ret), K(ranges_));
      } else {
        concurrent_cnt_ = ranges_.count();
        LOG_INFO("succeed to get range and concurrent cnt", K(ret), K(hint_parallelism),
          K(tablet_size), K(concurrent_cnt_), K(ranges_), K(result));
      }
    }
  }
  return ret;
}

int ObComplementDataContext::init(
    const ObComplementDataParam &param,
    const share::schema::ObTableSchema &hidden_table_schema)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  const ObSSTable *first_major_sstable = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementDataContext has already been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
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
  } else if (nullptr != first_major_sstable) {
    LOG_INFO("major exists, skip create tablet direct load mgr", K(ret), K(param));
  } else {
    direct_load_type_ = ObDDLUtil::use_idempotent_mode(param.data_format_version_) ?
      ObDirectLoadType::DIRECT_LOAD_DDL_V2 : ObDirectLoadType::DIRECT_LOAD_DDL;
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    ObTabletFullDirectLoadMgr *tablet_direct_load_mgr = nullptr;
    ObTabletDirectLoadInsertParam direct_load_param;
    direct_load_param.is_replay_ = false;
    direct_load_param.common_param_.direct_load_type_ = direct_load_type_;
    direct_load_param.common_param_.data_format_version_ = param.data_format_version_;
    direct_load_param.common_param_.read_snapshot_ = param.snapshot_version_;
    direct_load_param.common_param_.ls_id_ = param.dest_ls_id_;
    direct_load_param.common_param_.tablet_id_ = param.dest_tablet_id_;
    direct_load_param.common_param_.is_no_logging_ = param.is_no_logging_;
    direct_load_param.runtime_only_param_.exec_ctx_ = nullptr;
    direct_load_param.runtime_only_param_.task_id_ = param.task_id_;
    direct_load_param.runtime_only_param_.table_id_ = param.dest_table_id_;
    direct_load_param.runtime_only_param_.schema_version_ = param.dest_schema_version_;
    direct_load_param.runtime_only_param_.task_cnt_ = param.concurrent_cnt_; // real slice count.
    total_slice_cnt_ = param.ranges_.count();

    if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->alloc_execution_context_id(context_id_))) {
      LOG_WARN("alloc execution context id failed", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->create_tablet_direct_load(context_id_, param.execution_id_, direct_load_param))) {
      LOG_WARN("create tablet manager failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_major_sstable_exist_ = nullptr != first_major_sstable ? true : false;
    concurrent_cnt_ = param.concurrent_cnt_;
    lob_cols_cnt_ = hidden_table_schema.get_lob_columns_count();
    is_inited_ = true;
  }
  return ret;
}

int ObComplementDataContext::write_start_log(const ObComplementDataParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataContext not init", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(tenant_direct_load_mgr->open_tablet_direct_load(direct_load_type_,
      param.dest_ls_id_, param.dest_tablet_id_, context_id_))) {
      LOG_WARN("write ddl start log failed", K(ret));
    } else if (OB_FAIL(ddl_agent_.init(context_id_, param.dest_ls_id_, param.dest_tablet_id_, direct_load_type_))) {
      LOG_WARN("init agent failed", K(ret));
    }
    LOG_INFO("complement task start ddl redo success", K(ret), K(param));
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
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  is_major_sstable_exist_ = false;
  complement_data_ret_ = OB_SUCCESS;
  concurrent_cnt_ = 0;
  row_scanned_ = 0;
  row_inserted_ = 0;
  context_id_ = 0;
  lob_cols_cnt_ = 0;
  report_col_checksums_.reset();
  report_col_ids_.reset();
}

ObComplementDataDag::ObComplementDataDag()
  : ObIDag(ObDagType::DAG_TYPE_DDL), is_inited_(false), param_(), context_()
{
}


ObComplementDataDag::~ObComplementDataDag()
{
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (nullptr != tenant_direct_load_mgr) {
    (void) tenant_direct_load_mgr->close_tablet_direct_load(context_.context_id_, context_.direct_load_type_,
      param_.dest_ls_id_, param_.dest_tablet_id_, false/*need_commit*/, true/*emergent_finish*/,
      param_.task_id_, param_.dest_table_id_, 1/*execution_id*/);
  }
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

int ObComplementDataDag::calc_total_row_count()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has not been inited ", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param_));
  } else if (context_.physical_row_count_ != 0) {
    ret =  OB_INIT_TWICE;
    LOG_WARN("has calculated the row_count", K(ret), K(context_.physical_row_count_));
  } else if (param_.orig_tenant_id_ != param_.dest_tenant_id_) {
    // FIXME(YIREN), How to calc the row count of the source tablet for restore table.
    // RPC?
  } else if (OB_FAIL(ObDDLUtil::get_tablet_physical_row_cnt(
                                  param_.orig_ls_id_,
                                  param_.orig_tablet_id_,
                                  true, // calc_sstable = true
                                  true, // calc_memtable = true
                                  context_.physical_row_count_))) {
    LOG_WARN("failed to calc row count", K(ret), K(param_), K(context_));
  }
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
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *hidden_table_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataDag not init", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", K(ret), K(param_));
  } else if (OB_FAIL(param_.prepare_task_ranges())) {
    LOG_WARN("fail to parpare task range", K(ret), K(param_));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
             param_.dest_tenant_id_, schema_guard, param_.dest_schema_version_))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(param_));
  } else if (OB_FAIL(schema_guard.get_table_schema(param_.dest_tenant_id_,
             param_.dest_table_id_, hidden_table_schema))) {
    LOG_WARN("fail to get hidden table schema", K(ret), K(param_));
  } else if (OB_ISNULL(hidden_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("hidden table schema not exist", K(ret), K(param_));
  } else if (OB_FAIL(context_.init(param_, *hidden_table_schema))) {
    LOG_WARN("fail to init context", K(ret), K(param_));
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
    arg.row_inserted_ = context_.row_inserted_;
    arg.physical_row_count_ = context_.physical_row_count_;
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
  } else if (OB_FAIL(dag->calc_total_row_count())) { // only calc row count once time for a task
    LOG_WARN("failed to calc task row count", K(ret));
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
  : ObITask(TASK_TYPE_COMPLEMENT_WRITE),
    is_inited_(false), task_id_(0), param_(nullptr),
    context_(nullptr), col_ids_(), org_col_ids_(), output_projector_()
{
}

ObComplementWriteTask::~ObComplementWriteTask()
{
  col_ids_.reset();
  org_col_ids_.reset();
  output_projector_.reset();
}

int ObComplementWriteTask::init(
    const int64_t task_id,
    ObComplementDataParam &param,
    ObComplementDataContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObComplementWriteTask has already been inited", K(ret));
  } else if (task_id < 0 || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), K(param), K(context));
  } else {
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
  } else if (next_task_id >= param_->concurrent_cnt_) {
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
  if (OB_UNLIKELY(OB_ISNULL(param_) || OB_ISNULL(context_) || !param_->is_valid()
                  || !param_->has_generated_task_ranges())) {
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
  SMART_VAR(ObLocalScan, local_scan) {
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        true, /*is daily merge scan*/
        true, /*is read multiple macro block*/
        false, /*sys task scan, read one macro block in single io*/
        false /*is full row scan?*/,
        false,
        false);
    ObArenaAllocator allocator("cmplt_write", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObDatumRange datum_range;
    const bool allow_not_ready = false;
    ObLSHandle ls_handle;
    ObTabletTableIterator iterator;
    ObSSTable *sstable = nullptr;
    const uint64_t tenant_id = param_->dest_tenant_id_;
    const int64_t schema_version = param_->dest_schema_version_;

    if (OB_FAIL(MTL(ObLSService *)->get_ls(param_->orig_ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("fail to get log stream", K(ret), KPC(param_));
    } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls_handle));
    } else if (OB_FAIL(DDL_SIM(tenant_id, param_->task_id_, COMPLEMENT_DATA_TASK_LOCAL_SCAN_FAILED))) {
      LOG_WARN("ddl sim failure", K(ret), KPC(param_));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_read_tables(param_->orig_tablet_id_,
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
        param_->snapshot_version_, param_->snapshot_version_, iterator, allow_not_ready, false/*need_split_src_table*/, false/*need_split_dst_table*/))) {
      if (OB_REPLICA_NOT_READABLE == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("snapshot version has been discarded", K(ret));
      }
    } else {
      FLOG_INFO("local scan read tables", K(iterator), KPC(param_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param_->ranges_.at(task_id_, datum_range))) {
      LOG_WARN("fail to get range", K(ret));
    } else if (OB_FAIL(datum_range.prepare_memtable_readable(org_col_ids_, allocator))) {
      LOG_WARN("prepare datum range for memtable readable", K(ret));
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
                                        *hidden_table_schema,
                                        false/*unique_index_checking*/))) {
        LOG_WARN("fail to init local scan param", K(ret), K(*param_));
      } else if (OB_FAIL(local_scan.table_scan(*data_table_schema,
                                               param_->orig_ls_id_,
                                               param_->orig_tablet_id_,
                                               iterator,
                                               query_flag,
                                               datum_range))) {
        LOG_WARN("fail to do table scan", K(ret));
      }
    }
    if (FAILEDx(append_row(&local_scan))) {
      LOG_WARN("append row failed", K(ret));
    }
  }

  return ret;
}

int ObComplementWriteTask::remote_scan()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(param_) || OB_ISNULL(context_) || !param_->is_valid()
             || !param_->has_generated_task_ranges())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(idx), KP(param_));
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
    ObDatumRange datum_range;
    ObArenaAllocator allocator("cmplt_write", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (OB_FAIL(param_->ranges_.at(task_id_, datum_range))) {
      LOG_WARN("fail to get range", K(ret));
    } else if (OB_FAIL(datum_range.prepare_memtable_readable(org_col_ids_, allocator))) {
      LOG_WARN("prepare datum range for memtable readable", K(ret));
    } else if (OB_FAIL(remote_scan.init(param_->orig_tenant_id_,
                                  param_->orig_table_id_,
                                  param_->dest_tenant_id_,
                                  param_->dest_table_id_,
                                  param_->orig_schema_version_,
                                  param_->dest_schema_version_,
                                  param_->snapshot_version_,
                                  param_->orig_tablet_id_,
                                  datum_range))) {
      LOG_WARN("fail to remote_scan init", K(ret), KPC(param_));
    } else if (OB_FAIL(append_row(&remote_scan))) {
      LOG_WARN("append row remote scan failed", K(ret));
    }
  }
  return ret;
}

int ObComplementWriteTask::append_row(ObScan *scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> report_col_checksums;
  ObArray<int64_t> report_col_ids;
  ObMacroDataSeq macro_start_seq(0);
  const int64_t process_start_time = ObTimeUtility::current_time();;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementWriteTask is not inited", K(ret));
  } else if (OB_ISNULL(scan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(ObDDLUtil::init_macro_block_seq(task_id_, macro_start_seq))) {
    LOG_WARN("set parallel degree failed", K(ret), K(task_id_));
  } else {
    int64_t affected_rows = 0;
    blocksstable::ObMacroDataSeq unused_seq;
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = param_->dest_ls_id_;
    slice_info.data_tablet_id_ = param_->dest_tablet_id_;
    slice_info.context_id_ = context_->context_id_;
    slice_info.total_slice_cnt_ = context_->total_slice_cnt_;
    slice_info.slice_idx_ = task_id_;
    ObInsertMonitor insert_monitor(context_->row_scanned_, context_->row_inserted_, context_->cg_row_inserted_);
    ObDDLInsertRowIterator row_iter;
    ObTabletSliceParam tablet_slice_param(context_->concurrent_cnt_, task_id_);
    if (OB_FAIL(row_iter.init(param_->orig_tenant_id_, context_->ddl_agent_, scan,
            param_->dest_ls_id_, param_->dest_tablet_id_, context_->context_id_, tablet_slice_param, context_->lob_cols_cnt_, context_->total_slice_cnt_))) {
      LOG_WARN("init ddl insert row iterator failed", K(ret), K(context_->total_slice_cnt_));
    } else if (OB_FAIL(context_->ddl_agent_.open_sstable_slice(macro_start_seq, slice_info))) {
      LOG_WARN("open slice failed", K(ret), K(macro_start_seq), K(slice_info));
    } else if (OB_FAIL(context_->ddl_agent_.fill_sstable_slice(slice_info, &row_iter, affected_rows, &insert_monitor))) {
      LOG_WARN("fill sstable slice failed", K(ret), K(slice_info));
    } else if (OB_FAIL(context_->ddl_agent_.close_sstable_slice(slice_info, &insert_monitor, unused_seq))) {
      LOG_WARN("close sstable slice failed", K(ret));
    } else { /* do nothing.*/ }
  }
  int64_t total_time = ObTimeUtility::current_time() - process_start_time;
  if (param_->orig_tenant_id_ == param_->dest_tenant_id_) {
    SERVER_EVENT_ADD("alter_table", "drop_column_data_complement",
      "tenant_id", param_->dest_tenant_id_,
      "task_id", param_->task_id_,
      "complement_task_id", task_id_,
      "total_cost", total_time
    );
  } else {
    SERVER_EVENT_ADD("recover_table", "recover_table_complement_task",
      "tenant_id", param_->dest_tenant_id_,
      "task_id", param_->task_id_, /** import table task id */
      "complement_task_id", task_id_,
      "total_cost", total_time
    );
  }
  LOG_INFO("print append row to macro block cost time", K(ret), K(task_id_), K(affected_rows),
      "process_time", total_time);
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
        LOG_INFO("update checksum successfully", K(param_->orig_tenant_id_), K(param_->orig_table_id_), K(report_col_checksums));
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
  }

  if (OB_NOT_NULL(context_) && !context_->is_major_sstable_exist_) {
    // tablet direct load mgr will not create if major has already existed.
    // no matter succ or fail, should call it to cleanup tablet direct load mgr.
    const bool need_commit = OB_SUCC(ret) && OB_SUCC(context_->complement_data_ret_) ? true : false;
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_TMP_FAIL(tenant_direct_load_mgr->close_tablet_direct_load(context_->context_id_, context_->direct_load_type_,
      param_->dest_ls_id_, param_->dest_tablet_id_, need_commit, true/*emergent_finish*/,
      param_->task_id_, param_->dest_table_id_, 1/*execution_id*/))) {
      LOG_WARN("close tablet direct load failed", K(ret), K(tmp_ret), KPC(param_));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
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


/**
 * -----------------------------------ObLocalScan-----------------------------------------
 */

ObLocalScan::ObLocalScan() : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), table_id_(OB_INVALID_ID),
    dest_table_id_(OB_INVALID_ID), schema_version_(0), extended_gc_(),
    default_row_(), write_row_(), row_iter_(nullptr), scan_merge_(nullptr), ctx_(), access_param_(),
    access_ctx_(), get_table_param_(), allocator_("ObLocalScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    calc_buf_(ObModIds::OB_SQL_EXPR_CALC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), col_params_(), read_info_(),
    exist_column_mapping_(allocator_)
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
  write_row_.reset();
  access_ctx_.reset();
}

int ObLocalScan::init(
    const ObIArray<share::schema::ObColDesc> &col_ids,
    const ObIArray<share::schema::ObColDesc> &org_col_ids,
    const ObIArray<int32_t> &projector,
    const ObTableSchema &data_table_schema,
    const int64_t snapshot_version,
    const ObTableSchema &hidden_table_schema,
    const bool unique_index_checking)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLocalScan has been initialized before", K(ret));
  } else if (org_col_ids.count() < 1 || col_ids.count() < 1 || projector.count() < 1
      || !data_table_schema.is_valid() || !hidden_table_schema.is_valid() || snapshot_version < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid auguments", K(ret), K(data_table_schema), K(hidden_table_schema),
        K(col_ids), K(org_col_ids), K(projector), K(snapshot_version));
  } else {
    unique_index_checking_ = unique_index_checking;
    snapshot_version_ = snapshot_version;
    ObDatumRow tmp_default_row;
    const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (OB_FAIL(check_generated_column_exist(hidden_table_schema, org_col_ids))) {
      LOG_WARN("fail to init generated columns", K(ret), K(org_col_ids));
    } else if (OB_FAIL(extended_gc_.extended_col_ids_.assign(col_ids))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(extended_gc_.org_extended_col_ids_.assign(org_col_ids))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(extended_gc_.output_projector_.assign(projector))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(get_exist_column_mapping(data_table_schema, hidden_table_schema))){
      LOG_WARN("fail to init positions for resolving row", K(ret));
    } else if (OB_FAIL(checksum_calculator_.init(org_col_ids.count() + extra_rowkey_cnt))) {
      LOG_WARN("fail to init checksum calculator", K(ret));
    } else if (OB_FAIL(hidden_table_schema.get_multi_version_column_descs(mult_version_cols_desc_))) {
      LOG_WARN("get column descs failed", K(ret));
    } else if (OB_FAIL(tmp_default_row.init(allocator_, org_col_ids.count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (OB_FAIL(default_row_.init(allocator_, org_col_ids.count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (unique_index_checking && OB_FAIL(write_row_.init(allocator_, org_col_ids.count()))) { // without extra rowkey for unique index check.
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (!unique_index_checking && OB_FAIL(write_row_.init(allocator_, org_col_ids.count() + extra_rowkey_cnt))) { // with extra rowkey.
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else {
      tmp_default_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT); // default_row.row_flag_ will be set by deep_copy
      if (OB_FAIL(hidden_table_schema.get_orig_default_row(org_col_ids, tmp_default_row))) {
        LOG_WARN("fail to get default row from table schema", K(ret));
      } else if (OB_FAIL(default_row_.deep_copy(tmp_default_row, allocator_))) {
        LOG_WARN("failed to deep copy default row", K(ret));
      } else {
        tenant_id_ = data_table_schema.get_tenant_id();
        table_id_ = data_table_schema.get_table_id();
        dest_table_id_ = hidden_table_schema.get_table_id();
        schema_version_ = hidden_table_schema.get_schema_version();
        schema_rowkey_cnt_ = hidden_table_schema.get_rowkey_column_num();
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
  if (unique_index_checking_) {
    if (OB_FAIL(col_ids.assign(extended_gc_.org_extended_col_ids_))) {
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
    blocksstable::ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(construct_column_schema(data_table_schema))) {
    LOG_WARN("fail to construct column schema", K(ret), K(col_params_));
  } else if (OB_FAIL(construct_access_param(data_table_schema, tablet_id))) {
    LOG_WARN("fail to construct access param", K(ret), K(col_params_));
  } else if (OB_FAIL(construct_range_ctx(query_flag, ls_id))) {
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
  const ObArray<ObColDesc> &extended_col_ids = extended_gc_.extended_col_ids_;
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
      if (OB_FAIL(data_table_schema.get_column_group_index(*col_params_.at(i), false /*need_calculate_cg_idx*/, tmp_cg_idx))) {
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
    if (GCTX.is_shared_storage_mode()) {
      access_param_.iter_param_.table_scan_opt_.io_read_batch_size_ = 1024L * 1024L * 2L; // 2M
      access_param_.iter_param_.table_scan_opt_.io_read_gap_size_ = 0;
    }
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
                                     const share::ObLSID &ls_id)
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
  } else if (OB_FAIL(access_ctx_.init(query_flag, ctx_, allocator_, allocator_, trans_version_range))) {
    LOG_WARN("fail to init accesss ctx", K(ret));
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
    const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    // get data table columns id and corresponding checksum.
    for (int64_t i = 0; OB_SUCC(ret) && i < exist_column_mapping_.size(); i++) {
      if (exist_column_mapping_.test(i)) {
        const ObColumnSchemaV2 *hidden_col_schema = hidden_table_schema->get_column_schema(tmp_col_ids.at(i).col_id_);
        const ObString &hidden_column_name = hidden_col_schema->get_column_name_str();
        const ObColumnSchemaV2 *data_col_schema = data_table_schema->get_column_schema(hidden_column_name);
        const int64_t index_in_array = i < schema_rowkey_cnt_ ? i : i + extra_rowkey_cnt;
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
            const int64_t udt_hidden_index_in_array = index_col < schema_rowkey_cnt_ ? index_col : index_col + extra_rowkey_cnt;
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

int ObLocalScan::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  calc_buf_.reuse();
  const ObDatumRow *row = nullptr;
  if (OB_FAIL(row_iter_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_ISNULL(row) || !row->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(row));
  } else {
    write_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    for (int64_t i = 0, j = 0; OB_SUCC(ret) && i < exist_column_mapping_.size(); i++) {
      const int64_t in_row_index = unique_index_checking_ ? i : storaged_index_with_extra_rowkey(i);
      ObObjMeta &obj_meta = extended_gc_.org_extended_col_ids_.at(i).col_type_;
      if (exist_column_mapping_.test(i)) {
        // fill with value stored in origin data table.
        if (OB_UNLIKELY(j >= extended_gc_.extended_col_ids_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret), K(j), K(extended_gc_.extended_col_ids_.count()));
        } else {
          write_row_.storage_datums_[in_row_index] = row->storage_datums_[j++];
        }
      } else {
        // the column is newly added, thus fill with default value.
        write_row_.storage_datums_[in_row_index] = default_row_.storage_datums_[i];
      }
      if (OB_FAIL(ret)) {
      } else if (obj_meta.is_fixed_len_char_type()
        && OB_FAIL(ObDDLUtil::reshape_ddl_column_obj(write_row_.storage_datums_[in_row_index], obj_meta))) {
        LOG_WARN("reshape failed", K(ret), K(obj_meta));
      }
    }
    if (OB_SUCC(ret) && !unique_index_checking_) {
      write_row_.storage_datums_[schema_rowkey_cnt_].set_int(-snapshot_version_);
      write_row_.storage_datums_[schema_rowkey_cnt_ + 1].set_int(0);
      if (OB_FAIL(checksum_calculator_.calc_column_checksum(mult_version_cols_desc_, &write_row_, nullptr/*old_row*/, nullptr/*is_column_changed*/))) {
        LOG_WARN("calc column checksum failed", K(ret), K(mult_version_cols_desc_), K(write_row_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    datum_row = &write_row_;
  }
  return ret;
}

ObRemoteScan::ObRemoteScan()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    dest_tenant_id_(OB_INVALID_ID),
    dest_table_id_(OB_INVALID_ID),
    schema_version_(0),
    dest_schema_version_(0),
    row_with_reshape_(),
    write_row_(),
    res_(),
    result_(nullptr),
    datum_range_(nullptr),
    allocator_("DDLRemoteScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    org_col_ids_(),
    column_names_(),
    rowkey_col_accuracys_(),
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
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  dest_tenant_id_ = OB_INVALID_ID;
  dest_table_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  dest_schema_version_ = 0;
  row_with_reshape_.reset();
  write_row_.reset();
  res_.reset();
  result_ = nullptr;
  datum_range_ = nullptr;
  org_col_ids_.reset();
  column_names_.reset();
  rowkey_col_accuracys_.reset();
  allocator_.reset();
}

int ObRemoteScan::init(const uint64_t tenant_id,
                       const int64_t table_id,
                       const uint64_t dest_tenant_id,
                       const int64_t dest_table_id,
                       const int64_t schema_version,
                       const int64_t dest_schema_version,
                       const int64_t snapshot_version,
                       const ObTabletID &src_tablet_id,
                       const ObDatumRange &datum_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id
      || OB_INVALID_ID == dest_tenant_id || OB_INVALID_ID == dest_table_id
      || schema_version <= 0 || dest_schema_version <= 0 || snapshot_version <= 0
      || !src_tablet_id.is_valid() || !datum_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id),
      K(dest_tenant_id), K(dest_table_id), K(schema_version), K(dest_schema_version), K(snapshot_version), K(src_tablet_id));
  } else {
    ObSqlString sql_string;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *hidden_table_schema = nullptr;
    bool is_oracle_mode = false;
    const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    ObFixedLengthString<common::OB_MAX_TIMESTAMP_TZ_LENGTH> time_zone; // unused
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
    } else if (OB_FAIL(write_row_.init(allocator_, org_col_ids_.count() + extra_rowkey_cnt))) {
      LOG_WARN("fail to init tmp_row", K(ret), K(org_col_ids_.count()));
    } else if (OB_FAIL(row_with_reshape_.init(allocator_, org_col_ids_.count() + extra_rowkey_cnt))) {
      LOG_WARN("fail to init tmp_row", K(ret), K(org_col_ids_.count()));
    } else if (OB_FAIL(checksum_calculator_.init(org_col_ids_.count() + extra_rowkey_cnt))) {
      LOG_WARN("fail to init checksum_calculator", K(ret));
    } else if (OB_FAIL(hidden_table_schema->get_multi_version_column_descs(mult_version_cols_desc_))) {
      LOG_WARN("get col desc failed", K(ret));
    } else {
      tenant_id_ = tenant_id;
      table_id_ = table_id;
      dest_tenant_id_ = dest_tenant_id;
      dest_table_id_ = dest_table_id;
      schema_version_ = schema_version;
      dest_schema_version_ = dest_schema_version;
      src_tablet_id_ = src_tablet_id;
      datum_range_ = &datum_range;
      if (OB_FAIL(ObBackupUtils::get_tenant_sys_time_zone_wrap(dest_tenant_id_, time_zone, tz_info_wrap_))) {
        LOG_WARN("failed to get tenant sys time zone wrap", K(dest_tenant_id_));
      } else if (OB_FAIL(generate_build_select_sql(sql_string))) {
        LOG_WARN("fail to generate build replica sql", K(ret), K(sql_string));
      } else if (is_oracle_mode && OB_FAIL(prepare_iter(sql_string, GCTX.ddl_oracle_sql_proxy_))) {
        LOG_WARN("prepare iter under oracle mode failed", K(ret), K(sql_string));
      } else if (!is_oracle_mode && OB_FAIL(prepare_iter(sql_string, GCTX.ddl_sql_proxy_))) {
        LOG_WARN("prepare iter under mysql mode failed", K(ret), K(sql_string));
      } else {
        schema_rowkey_cnt_ = hidden_table_schema->get_rowkey_column_num();
        snapshot_version_ = snapshot_version;
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
  column_names_.reset();
  rowkey_col_accuracys_.reset();
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
    } else if (OB_ISNULL(datum_range_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), KPC(datum_range_));
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
      const int64_t dest_rowkey_cols_cnt = dest_table_schema->get_rowkey_column_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < dest_column_ids.count(); i++) {
        const uint64_t dest_column_id = dest_column_ids.at(i).col_id_;
        const ObColumnSchemaV2 *dest_column_schema = dest_table_schema->get_column_schema(dest_column_id);
        if (OB_ISNULL(dest_column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema", K(ret), K(dest_column_id));
        } else {
          const ObScale &scale = dest_column_schema->get_accuracy().get_scale();
          const ObString &dest_column_name = dest_column_schema->get_column_name_str();
          const ObColumnSchemaV2 *orig_column_schema = orig_table_schema->get_column_schema(dest_column_name);
          if (OB_ISNULL(orig_column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column not exist", K(ret), K(dest_column_name), KPC(dest_table_schema));
          } else if (OB_FAIL(column_names_.push_back(ObColumnNameInfo(dest_column_name, is_shadow_column(dest_column_id),
              orig_column_schema->is_enum_or_set())))) {
            LOG_WARN("fail to push back column name failed", K(ret));
          } else if (i < dest_rowkey_cols_cnt
                     && OB_FAIL(rowkey_col_accuracys_.push_back(orig_column_schema->get_accuracy()))) {
            LOG_WARN("fail to push back rowkey column accuacy", K(ret));
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
          } else if (OB_FAIL(generate_range_condition(*datum_range_, is_oracle_mode, sql_string))) {
            LOG_WARN("fail to generate range condition sql", K(ret), KPC(datum_range_), K(query_partition_sql));
          } else if (OB_FAIL(sql_string.append(" order by "))) {
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

int ObRemoteScan::convert_rowkey_to_sql_literal(
    const ObRowkey &rowkey,
    bool is_oracle_mode,
    char *buf,
    int64_t &pos,
    int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const ObObj *objs = nullptr;
  if (!rowkey.is_valid() || rowkey.get_obj_cnt() != rowkey_col_accuracys_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(rowkey), "rowkey obj cnt", rowkey.get_obj_cnt(),
      "rowkey col accuracys cnt", rowkey_col_accuracys_.count());
  } else if (OB_ISNULL(objs = rowkey.get_obj_ptr())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs is null", K(ret));
  } else {
    ObObjPrintParams print_params(tz_info_wrap_.get_time_zone_info());
    print_params.print_const_expr_type_ = true;
    print_params.need_cast_expr_ = true;
    print_params.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI; /*unused collation type*/
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
      ObObj tmp_obj = objs[i]; // shallow copy obj
      print_params.ob_obj_type_ = tmp_obj.get_type();
      print_params.accuracy_ = rowkey_col_accuracys_.at(i);
      /* safe hex representation of character types */
      print_params.character_hex_safe_represent_ =
        ob_is_character_type(tmp_obj.get_type(), tmp_obj.get_collation_type());
      /*  ObObj read from storage layer may loss ObObjMeta scale info and need to be obtained from schema. */
      tmp_obj.set_scale(rowkey_col_accuracys_.at(i).get_scale());
      if (0 != i) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
          LOG_WARN("failed to add comma", K(ret));
        }
      }
      if (FAILEDx(tmp_obj.print_sql_literal(buf, buf_len, pos, print_params))) {
        LOG_WARN("failed to print obj", K(ret), K(tmp_obj), K(print_params));
      }
    }
  }
  return ret;
}

int ObRemoteScan::generate_range_condition(
    const ObDatumRange &datum_range,
    bool is_oracle_mode,
    ObSqlString &sql)
{
  /*
   * use multi column comparison to generate sql literal range condition.
   * e.g. where (col1, col2, col3) > (1, 2, 3) and (col1, col2, col3) <= (3, 4, 5)
   */
  int ret = OB_SUCCESS;
  ObSqlString rowkey_cols_str;
  ObArray<ObColumnNameInfo> rowkey_cols_names;
  const int64_t rowkey_cols_cnt = rowkey_col_accuracys_.count();
  const ObRowkey &start_key = datum_range.start_key_.store_rowkey_.get_rowkey();
  const ObRowkey &end_key = datum_range.end_key_.store_rowkey_.get_rowkey();
  const ObBorderFlag border_flag = datum_range.border_flag_;
  if (OB_UNLIKELY(!datum_range.is_valid() || !datum_range.is_memtable_valid()
      || rowkey_cols_cnt <= 0 || rowkey_cols_cnt > column_names_.count()
      || start_key.is_max_row() || end_key.is_min_row())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(datum_range), K(rowkey_col_accuracys_), K(column_names_));
  } else if (datum_range.is_whole_range()) {
    // do nothing at whole range
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names_.count() && i < rowkey_cols_cnt; i++) {
      const ObColumnNameInfo &column_name = column_names_.at(i);
      if (OB_FAIL(rowkey_cols_names.push_back(column_name))) {
          LOG_WARN("failed to prepare allocate rowkey col array", K(ret), K(column_name));
      }
    }

    if (FAILEDx(ObDDLUtil::generate_column_name_str(rowkey_cols_names,
                                                    is_oracle_mode,
                                                    true,
                                                    false,
                                                    false/*use_heap_table_ddl_plan*/,
                                                    rowkey_cols_str))) {
      LOG_WARN("failed to generate rowkey column string", K(ret));
    } else if (OB_FAIL(sql.append(" WHERE "))) {
      LOG_WARN("failed to append string", K(ret));
    } else {
        ObArenaAllocator allocator("addCond");
        int64_t low_val_len = 0;
        int64_t high_val_len = 0;
        char *low_val_str = nullptr;
        char *high_val_str = nullptr;
        if (start_key.is_min_row()) {
          // do nothing when start_key is min_row
        } else if (OB_UNLIKELY(start_key.get_obj_cnt() != rowkey_cols_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid count", K(ret), K(start_key), K(rowkey_cols_cnt));
        } else if(OB_ISNULL(low_val_str = static_cast<char *>(allocator.alloc(OB_MAX_ROW_KEY_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("val str is nullptr", K(ret), K(low_val_str));
        } else if (OB_FAIL(convert_rowkey_to_sql_literal(start_key,
                                                         is_oracle_mode,
                                                         low_val_str,
                                                         low_val_len,
                                                         OB_MAX_ROW_KEY_LENGTH))) {
          LOG_WARN("failed to convert rowkey to sql literal", K(ret), K(start_key));
        } else if (OB_UNLIKELY(0 == low_val_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowkey sql literal length", K(ret), K(start_key));
        } else if (OB_FAIL(sql.append_fmt("(%.*s) %s (%.*s) ",
                                          static_cast<int>(rowkey_cols_str.length()),
                                          rowkey_cols_str.ptr(),
                                          (border_flag.inclusive_start() ? ">=" : ">"),
                                          static_cast<int>(low_val_len),
                                          low_val_str))) {
          LOG_WARN("failed to append string", K(ret), K(rowkey_cols_str), K(low_val_str));
        }

        if (OB_FAIL(ret)) {
        } else if (end_key.is_max_row()) {
          // do nothing when end_key is max_row
        } else if (OB_UNLIKELY(end_key.get_obj_cnt() != rowkey_cols_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid count", K(ret), K(end_key));
        } else if (low_val_len > 0 && OB_FAIL(sql.append(" AND "))) {
          LOG_WARN("failed to append string", K(ret));
        } else if (OB_ISNULL(high_val_str = static_cast<char *>(allocator.alloc(OB_MAX_ROW_KEY_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("val str is nullptr", K(ret), K(low_val_str));
        } else if (OB_FAIL(convert_rowkey_to_sql_literal(end_key,
                                                         is_oracle_mode,
                                                         high_val_str,
                                                         high_val_len,
                                                         OB_MAX_ROW_KEY_LENGTH))) {
          LOG_WARN("failed to convert rowkey to sql literal", K(ret), K(end_key));
        } else if (OB_UNLIKELY(0 == high_val_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowkey sql literal length", K(ret));
        } else if (OB_FAIL(sql.append_fmt("(%.*s) %s (%.*s) ",
                                          static_cast<int>(rowkey_cols_str.length()),
                                          rowkey_cols_str.ptr(),
                                          (border_flag.inclusive_end() ? "<=" : "<"),
                                          static_cast<int>(high_val_len),
                                          high_val_str))) {
          LOG_WARN("failed to append string", K(ret), K(rowkey_cols_str), K(high_val_str));
        }
      }
  }
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

int ObRemoteScan::get_next_row(const blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
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
    write_row_.reuse();
    row_with_reshape_.reuse(); // used for chksum calculated only.
    for (int64_t i = 0; OB_SUCC(ret) && i < org_col_ids_.count(); i++) {
      ObObj obj;
      const int64_t storage_index = storaged_index_with_extra_rowkey(i);
      if (OB_FAIL(result_->get_obj(i, obj))) {
        LOG_WARN("failed to get object", K(ret), "column_id", org_col_ids_.at(i).col_id_);
      } else if (OB_FAIL(write_row_.storage_datums_[storage_index].from_obj_enhance(obj))) {
        LOG_WARN("failed to from obj enhance", K(ret));
      }
      /**
       * For fix-length type column, the select result is padded to full char length, and row needs to
       * be reshaped to keep the same format as lines written into macro block, avoiding checksum error.
      */
      else if (OB_FAIL(row_with_reshape_.storage_datums_[storage_index].from_obj_enhance(obj))) {
        LOG_WARN("failed to from obj enhance", K(ret));
      } else if (obj.is_fixed_len_char_type()
        && OB_FAIL(ObDDLUtil::reshape_ddl_column_obj(row_with_reshape_.storage_datums_[storage_index], obj.get_meta()))) {
        LOG_WARN("reshape failed", K(ret), K(obj));
      }
    }
    if (OB_SUCC(ret)) {
      write_row_.storage_datums_[schema_rowkey_cnt_].set_int(-snapshot_version_);
      write_row_.storage_datums_[schema_rowkey_cnt_ + 1].set_int(0);
      row_with_reshape_.storage_datums_[schema_rowkey_cnt_].set_int(-snapshot_version_);
      row_with_reshape_.storage_datums_[schema_rowkey_cnt_ + 1].set_int(0);
      write_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row_with_reshape_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row = &write_row_;
      if (OB_FAIL(checksum_calculator_.calc_column_checksum(mult_version_cols_desc_, &row_with_reshape_, nullptr/*old_row*/, nullptr/*is_column_changed*/))) {
        LOG_WARN("calc column checksum failed", K(ret), K(mult_version_cols_desc_), K(write_row_), KPC(datum_row));
      }
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
  session_param.tz_info_wrap_ = &tz_info_wrap_;
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
      const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_col_ids.size(); i++) {
        const ObColumnSchemaV2 *dest_col_schema = dest_table_schema->get_column_schema(tmp_col_ids.at(i).col_id_);
        const ObString &dest_column_name = dest_col_schema->get_column_name_str();
        const ObColumnSchemaV2 *orig_col_schema = orig_table_schema->get_column_schema(dest_column_name);
        const int64_t index_in_array = i < schema_rowkey_cnt_ ? i : i + extra_rowkey_cnt;
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
