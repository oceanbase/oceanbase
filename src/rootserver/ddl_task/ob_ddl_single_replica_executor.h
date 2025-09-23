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

#ifndef OCEANBASE_ROOTSERVICE_OB_DDL_SINGLE_REPLICA_EXECUTOR_H
#define OCEANBASE_ROOTSERVICE_OB_DDL_SINGLE_REPLICA_EXECUTOR_H

#include "lib/container/ob_array.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace rootserver
{

struct ObDDLReplicaBuildExecutorParam final
{
public:
  ObDDLReplicaBuildExecutorParam ()
    : tenant_id_(OB_INVALID_TENANT_ID),
      dest_tenant_id_(OB_INVALID_TENANT_ID),
      ddl_type_(share::DDL_INVALID),
      source_tablet_ids_(),
      dest_tablet_ids_(),
      source_table_ids_(OB_INVALID_ID),
      dest_table_ids_(OB_INVALID_ID),
      source_schema_versions_(0),
      dest_schema_versions_(0),
      snapshot_version_(0),
      task_id_(0),
      parallelism_(0),
      execution_id_(-1),
      data_format_version_(0),
      consumer_group_id_(0),
      compaction_scns_(),
      lob_col_idxs_(),
      can_reuse_macro_blocks_(),
      parallel_datum_rowkey_list_(),
      min_split_start_scn_(),
      is_no_logging_(false),
      dest_cg_cnt_(0)
  {}
  ~ObDDLReplicaBuildExecutorParam () = default;
  bool is_valid() const {
    bool is_valid =  tenant_id_ != OB_INVALID_TENANT_ID &&
                     dest_tenant_id_ != OB_INVALID_TENANT_ID &&
                     ddl_type_ != share::DDL_INVALID &&
                     source_tablet_ids_.count() > 0 &&
                     dest_tablet_ids_.count() == source_tablet_ids_.count() &&
                     source_table_ids_.count() == source_tablet_ids_.count() &&
                     dest_table_ids_.count() == source_tablet_ids_.count() &&
                     source_schema_versions_.count() == source_tablet_ids_.count() &&
                     dest_schema_versions_.count() == source_tablet_ids_.count() &&
                     snapshot_version_ > 0 &&
                     task_id_ > 0 &&
                     execution_id_ >= 0 &&
                     data_format_version_ > 0 &&
                     consumer_group_id_ >= 0;
    if (is_tablet_split(ddl_type_)) {
      is_valid = is_valid && compaction_scns_.count() == source_tablet_ids_.count()
                          && can_reuse_macro_blocks_.count() == source_tablet_ids_.count()
                          && min_split_start_scn_.is_valid_and_not_min();
    } else {
      is_valid = (is_valid && compaction_scns_.count() == 0);
    }
    if (share::is_recover_table_task(ddl_type_)) {
      is_valid = is_valid && dest_cg_cnt_ > 0;
    }

    return is_valid;
  }

  TO_STRING_KV(K_(tenant_id), K_(dest_tenant_id), K_(ddl_type), K_(source_tablet_ids),
               K_(dest_tablet_ids), K_(source_table_ids), K_(dest_table_ids),
               K_(source_schema_versions), K_(dest_schema_versions), K_(snapshot_version),
               K_(task_id), K_(parallelism), K_(execution_id), 
               K_(data_format_version), K_(consumer_group_id), K_(can_reuse_macro_blocks),
               K_(parallel_datum_rowkey_list), K(min_split_start_scn_), K_(is_no_logging),
               K_(dest_cg_cnt));
public:
  uint64_t tenant_id_;
  uint64_t dest_tenant_id_;
  share::ObDDLType ddl_type_;
  ObArray<ObTabletID> source_tablet_ids_;
  ObSArray<ObTabletID> dest_tablet_ids_;
  ObSArray<uint64_t> source_table_ids_;
  ObSArray<uint64_t> dest_table_ids_;
  ObSArray<uint64_t> source_schema_versions_;
  ObSArray<uint64_t> dest_schema_versions_;
  int64_t snapshot_version_;
  int64_t task_id_;
  int64_t parallelism_;
  int64_t execution_id_;
  int64_t data_format_version_;
  int64_t consumer_group_id_;
  ObSArray<int64_t> compaction_scns_;
  ObSArray<uint64_t> lob_col_idxs_;
  ObSArray<bool> can_reuse_macro_blocks_;
  common::ObSEArray<common::ObSEArray<blocksstable::ObDatumRowkey, 8>, 8> parallel_datum_rowkey_list_;
  share::SCN min_split_start_scn_;
  int64_t is_no_logging_;
  int64_t dest_cg_cnt_;
};

enum class ObReplicaBuildStat
{
  BUILD_INIT = 0,
  BUILD_REQUESTED = 1,
  BUILD_SUCCEED = 2,
  BUILD_RETRY = 3,
  BUILD_FAILED = 4
};

struct ObSingleReplicaBuildCtx final
{
public:
  static const int64_t REPLICA_BUILD_HEART_BEAT_TIME = 10 * 1000 * 1000;
  ObSingleReplicaBuildCtx()
    : is_inited_(false),
      addr_(),
      ddl_type_(share::DDL_INVALID),
      src_table_id_(OB_INVALID_ID),
      dest_table_id_(OB_INVALID_ID),
      src_schema_version_(0),
      dest_schema_version_(0),
      tablet_task_id_(0),
      compaction_scn_(0),
      src_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      dest_tablet_id_(),
      can_reuse_macro_block_(false),
      parallel_datum_rowkey_list_(),
      stat_(ObReplicaBuildStat::BUILD_INIT),
      ret_code_(OB_SUCCESS),
      heart_beat_time_(0),
      row_inserted_(0),
      cg_row_inserted_(0),
      row_scanned_(0),
      physical_row_count_(0),
      sess_not_found_times_(0)
  { }
  ~ObSingleReplicaBuildCtx() = default;
  int init(const ObAddr& addr,
           const share::ObDDLType ddl_type,
           const int64_t src_table_id,
           const int64_t dest_table_id,
           const int64_t src_schema_version,
           const int64_t dest_schema_version,
           const int64_t tablet_task_id,
           const int64_t compaction_scn,
           const ObTabletID &src_tablet_id,
           const ObTabletID &dest_tablet_ids,
           const bool can_reuse_macro_block,
           const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list);
  void reset_build_stat();
  bool is_valid() const;
  int assign(const ObSingleReplicaBuildCtx &other);
  int check_need_schedule(bool &need_schedule) const;
  TO_STRING_KV(K(is_inited_), K(addr_), K(ddl_type_), K(src_table_id_),
               K(src_schema_version_), K(dest_schema_version_),
               K(dest_table_id_), K(tablet_task_id_), K(compaction_scn_),
               K(src_tablet_id_), K(dest_tablet_id_), K_(can_reuse_macro_block), 
               K(parallel_datum_rowkey_list_), K(stat_), K(ret_code_),
               K(heart_beat_time_), K(row_inserted_), K(cg_row_inserted_), K(row_scanned_), K(physical_row_count_),
               K(sess_not_found_times_));

public:
  bool is_inited_;
  ObAddr addr_;
  share::ObDDLType ddl_type_;
  int64_t src_table_id_;
  int64_t dest_table_id_;
  int64_t src_schema_version_;
  int64_t dest_schema_version_;
  int64_t tablet_task_id_;
  int64_t compaction_scn_;
  ObTabletID src_tablet_id_;
  ObTabletID dest_tablet_id_;
  bool can_reuse_macro_block_;
  common::ObSEArray<blocksstable::ObDatumRowkey, 8> parallel_datum_rowkey_list_;
  ObReplicaBuildStat stat_;
  int64_t ret_code_;
  int64_t heart_beat_time_;
  int64_t row_inserted_;
  int64_t cg_row_inserted_;
  int64_t row_scanned_;
  int64_t physical_row_count_;
  /* special variable is only used to reduce table recovery retry parallelism
   * when the session not found error code appear in table recovery data complement. */
  int64_t sess_not_found_times_;
};

class ObDDLReplicaBuildExecutor
{
public:
  ObDDLReplicaBuildExecutor()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      dest_tenant_id_(OB_INVALID_TENANT_ID),
      ddl_type_(share::ObDDLType::DDL_INVALID),
      ddl_task_id_(0),
      snapshot_version_(0),
      parallelism_(0),
      execution_id_(0),
      data_format_version_(0),
      consumer_group_id_(0),
      dest_cg_cnt_(0),
      lob_col_idxs_(),
      src_tablet_ids_(),
      dest_tablet_ids_(),
      replica_build_ctxs_(),
      min_split_start_scn_(),
      lock_()
  {}
  ~ObDDLReplicaBuildExecutor() = default;
  int build(const ObDDLReplicaBuildExecutorParam &param);
  int check_build_end(const bool need_checksum, bool &is_end, int64_t &ret_code);
  int update_build_progress(const ObTabletID &tablet_id,
                            const ObAddr &addr,
                            const int ret_code,
                            const int64_t row_scanned,
                            const int64_t row_inserted,
                            const int64_t cg_row_inserted,
                            const int64_t physical_row_count);
  int get_progress(int64_t &physical_row_count,
                   int64_t &row_inserted,
                   int64_t &cg_row_inserted,
                   double &row_percent,
                   double &cg_row_percent);
  TO_STRING_KV(K(is_inited_), K(tenant_id_), K(dest_tenant_id_), K(ddl_type_),
               K(ddl_task_id_), K(snapshot_version_), K(parallelism_),
               K(execution_id_), K(data_format_version_), K(consumer_group_id_), K_(dest_cg_cnt),
               K(lob_col_idxs_), K(src_tablet_ids_), K(dest_tablet_ids_),
               K(replica_build_ctxs_), K(min_split_start_scn_));
private:
  int schedule_task();
  int process_rpc_results(
      const ObArray<ObTabletID> &tablet_ids,
      const ObArray<ObAddr> addrs,
      const ObIArray<const obrpc::ObDDLBuildSingleReplicaRequestResult *> &result_array,
      const ObArray<int> &ret_array);
  int construct_rpc_arg(
      const ObSingleReplicaBuildCtx &replica_build_ctx,
      obrpc::ObDDLBuildSingleReplicaRequestArg &arg) const;
  int construct_replica_build_ctxs(
      const ObDDLReplicaBuildExecutorParam &param,
      ObArray<ObSingleReplicaBuildCtx> &replica_build_ctxs) const;
  int get_refreshed_replica_addrs(
      const bool send_to_all_replicas,
      ObArray<ObTabletID> &replica_tablet_ids,
      ObArray<ObAddr> &replica_addrs) const;
  int refresh_replica_build_ctxs(
      const ObArray<ObTabletID> &replica_tablet_ids,
      const ObArray<ObAddr> &replica_addrs);
  int update_build_ctx(
      ObSingleReplicaBuildCtx &build_ctx,
      const obrpc::ObDDLBuildSingleReplicaRequestResult *result,
      const int ret_code);
  int update_replica_build_ctx(
      ObSingleReplicaBuildCtx &build_ctx,
      const int64_t ret_code,
      const int64_t row_scanned,
      const int64_t row_inserted,
      const int64_t cg_row_inserted,
      const int64_t row_count,
      const bool is_rpc_request,
      const bool is_observer_report);
  int get_replica_build_ctx(
      const ObTabletID &tablet_id,
      const ObAddr &addr,
      ObSingleReplicaBuildCtx *&replica_build_ctx,
      bool &is_found);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t dest_tenant_id_;
  share::ObDDLType ddl_type_;
  int64_t ddl_task_id_;
  int64_t snapshot_version_;
  int64_t parallelism_;
  int64_t execution_id_;
  int64_t data_format_version_;
  int64_t consumer_group_id_;
  int64_t dest_cg_cnt_;
  ObSArray<uint64_t> lob_col_idxs_;
  ObArray<ObTabletID> src_tablet_ids_;
  ObSArray<ObTabletID> dest_tablet_ids_;
  ObArray<ObSingleReplicaBuildCtx> replica_build_ctxs_; // NOTE hold lock before access
  share::SCN min_split_start_scn_;
  ObSpinLock lock_; // NOTE keep rpc send out of lock scope
  bool is_no_logging_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVICE_OB_DDL_SINGLE_REPLICA_EXECUTOR_H
