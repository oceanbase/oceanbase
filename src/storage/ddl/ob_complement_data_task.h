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

#ifndef OCEANBASE_STORAGE_OB_COMPLEMENT_DATA_TASK_H
#define OCEANBASE_STORAGE_OB_COMPLEMENT_DATA_TASK_H

#include "storage/access/ob_table_access_context.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ob_store_row_comparer.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

namespace oceanbase
{
namespace sql
{
struct ObTempExpr;
}
namespace storage
{
class ObScan;
class ObLocalScan;
class ObRemoteScan;
class ObMultipleScanMerge;
struct ObComplementDataParam final
{
public:
  static const int64_t DEFAULT_COMPLEMENT_DATA_MEMORY_LIMIT = 128L * 1024L * 1024L;
  ObComplementDataParam():
    is_inited_(false), orig_tenant_id_(common::OB_INVALID_TENANT_ID), dest_tenant_id_(common::OB_INVALID_TENANT_ID),
    orig_ls_id_(share::ObLSID::INVALID_LS_ID), dest_ls_id_(share::ObLSID::INVALID_LS_ID), orig_table_id_(common::OB_INVALID_ID),
    dest_table_id_(common::OB_INVALID_ID), orig_tablet_id_(ObTabletID::INVALID_TABLET_ID), dest_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    allocator_("CompleteDataPar"), row_store_type_(common::ENCODING_ROW_STORE), orig_schema_version_(0), dest_schema_version_(0),
    snapshot_version_(0), concurrent_cnt_(0), task_id_(0), execution_id_(-1), tablet_task_id_(0), compat_mode_(lib::Worker::CompatMode::INVALID), data_format_version_(0)
  {}
  ~ObComplementDataParam() { destroy(); }
  int init(const ObDDLBuildSingleReplicaRequestArg &arg);
  int split_task_ranges(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id, const int64_t tablet_size, const int64_t hint_parallelism);

  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != orig_tenant_id_ && common::OB_INVALID_TENANT_ID != dest_tenant_id_
           && orig_ls_id_.is_valid() && dest_ls_id_.is_valid() && common::OB_INVALID_ID != orig_table_id_
           && common::OB_INVALID_ID != dest_table_id_ && orig_tablet_id_.is_valid() && dest_tablet_id_.is_valid() && 0 != concurrent_cnt_
           && snapshot_version_ > 0 && compat_mode_ != lib::Worker::CompatMode::INVALID && execution_id_ >= 0 && tablet_task_id_ > 0
           && data_format_version_ > 0;
  }

  int get_hidden_table_key(ObITable::TableKey &table_key) const;
  bool use_new_checksum() const { return data_format_version_ >= DATA_VERSION_4_2_1_1; }
  void destroy()
  {
    is_inited_ = false;
    orig_tenant_id_ = common::OB_INVALID_TENANT_ID;
    dest_tenant_id_ = common::OB_INVALID_TENANT_ID;
    orig_ls_id_.reset();
    dest_ls_id_.reset();
    orig_table_id_ = common::OB_INVALID_ID;
    dest_table_id_ = common::OB_INVALID_ID;
    orig_tablet_id_.reset();
    dest_tablet_id_.reset();
    ranges_.reset();
    allocator_.reset();
    row_store_type_ = common::ENCODING_ROW_STORE;
    orig_schema_version_ = 0;
    dest_schema_version_ = 0;
    snapshot_version_ = 0;
    concurrent_cnt_ = 0;
    task_id_ = 0;
    execution_id_ = -1;
    tablet_task_id_ = 0;
    compat_mode_ = lib::Worker::CompatMode::INVALID;
    data_format_version_ = 0;
  }
  TO_STRING_KV(K_(is_inited), K_(orig_tenant_id), K_(dest_tenant_id), K_(orig_ls_id), K_(dest_ls_id),
      K_(orig_table_id), K_(dest_table_id), K_(orig_tablet_id), K_(dest_tablet_id), K_(orig_schema_version),
      K_(tablet_task_id), K_(dest_schema_version), K_(snapshot_version), K_(concurrent_cnt), K_(task_id),
      K_(execution_id), K_(compat_mode), K_(data_format_version), K_(ranges));
public:
  bool is_inited_;
  uint64_t orig_tenant_id_;
  uint64_t dest_tenant_id_;
  share::ObLSID orig_ls_id_;
  share::ObLSID dest_ls_id_;
  uint64_t orig_table_id_;
  uint64_t dest_table_id_;
  ObTabletID orig_tablet_id_;
  ObTabletID dest_tablet_id_;
  common::ObArenaAllocator allocator_;
  common::ObRowStoreType row_store_type_;
  int64_t orig_schema_version_;
  int64_t dest_schema_version_;
  int64_t snapshot_version_;
  int64_t concurrent_cnt_;
  int64_t task_id_;
  int64_t execution_id_;
  int64_t tablet_task_id_;
  lib::Worker::CompatMode compat_mode_;
  int64_t data_format_version_;
  ObSEArray<common::ObStoreRange, 32> ranges_;
};

struct ObComplementDataContext final
{
public:
  ObComplementDataContext():
    is_inited_(false), is_major_sstable_exist_(false), complement_data_ret_(common::OB_SUCCESS),
    allocator_("CompleteDataCtx"), lock_(ObLatchIds::COMPLEMENT_DATA_CONTEXT_LOCK), concurrent_cnt_(0),
    data_sstable_redo_writer_(), index_builder_(nullptr), ddl_kv_mgr_handle_(), row_scanned_(0), row_inserted_(0)
  {}
  ~ObComplementDataContext() { destroy(); }
  int init(const ObComplementDataParam &param, const ObDataStoreDesc &desc);
  void destroy();
  int write_start_log(const ObComplementDataParam &param);
  int add_column_checksum(const ObIArray<int64_t> &report_col_checksums, const ObIArray<int64_t> &report_col_ids);
  int get_column_checksum(ObIArray<int64_t> &report_col_checksums, ObIArray<int64_t> &report_col_ids);
  TO_STRING_KV(K_(is_inited), K_(complement_data_ret), K_(concurrent_cnt), KP_(index_builder), K_(ddl_kv_mgr_handle), K_(row_scanned), K_(row_inserted));
public:
  bool is_inited_;
  bool is_major_sstable_exist_;
  int complement_data_ret_;
  common::ObArenaAllocator allocator_;
  ObSpinLock lock_;
  int64_t concurrent_cnt_;
  ObDDLSSTableRedoWriter data_sstable_redo_writer_;
  blocksstable::ObSSTableIndexBuilder *index_builder_;
  ObDDLKvMgrHandle ddl_kv_mgr_handle_; // for keeping ddl kv mgr alive
  int64_t row_scanned_;
  int64_t row_inserted_;
  ObArray<int64_t> report_col_checksums_;
  ObArray<int64_t> report_col_ids_;
};

class ObComplementPrepareTask;
class ObComplementWriteTask;
class ObComplementMergeTask;
class ObComplementDataDag final: public share::ObIDag
{
public:
  ObComplementDataDag();
  ~ObComplementDataDag();
  int init(const ObDDLBuildSingleReplicaRequestArg &arg);
  int prepare_context();
  int64_t hash() const;
  bool operator ==(const share::ObIDag &other) const;
  bool is_inited() const { return is_inited_; }
  ObComplementDataParam &get_param() { return param_; }
  ObComplementDataContext &get_context() { return context_; }
  void handle_init_failed_ret_code(int ret) { context_.complement_data_ret_ = ret; }
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;

  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return param_.compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual int create_first_task() override;
  virtual bool ignore_warning() override;
  virtual bool is_ha_dag() const override { return false; }
  // report replica build status to RS.
  int report_replica_build_status();
private:
  bool is_inited_;
  ObComplementDataParam param_;
  ObComplementDataContext context_;
  DISALLOW_COPY_AND_ASSIGN(ObComplementDataDag);
};

class ObComplementPrepareTask final : public share::ObITask
{
public:
  ObComplementPrepareTask();
  ~ObComplementPrepareTask();
  int init(ObComplementDataParam &param, ObComplementDataContext &context);
  int process() override;
private:
  bool is_inited_;
  ObComplementDataParam *param_;
  ObComplementDataContext *context_;
  DISALLOW_COPY_AND_ASSIGN(ObComplementPrepareTask);
};

class ObComplementWriteTask final : public share::ObITask
{
public:
  ObComplementWriteTask();
  ~ObComplementWriteTask();
  int init(const int64_t task_id, ObComplementDataParam &param, ObComplementDataContext &context);
  int process() override;
private:
  int generate_next_task(share::ObITask *&next_task);
  int generate_col_param();
  int local_scan_by_range();
  int remote_scan();
  int do_local_scan();
  int do_remote_scan();
  int append_row(ObScan *scan);
  int add_extra_rowkey(const int64_t rowkey_cnt,
                       const int64_t extra_rowkey_cnt,
                       const blocksstable::ObDatumRow &row,
                       const int64_t sql_no = 0);

private:
  static const int64_t RETRY_INTERVAL = 100 * 1000; // 100ms
  bool is_inited_;
  int64_t task_id_;
  ObComplementDataParam *param_;
  ObComplementDataContext *context_;
  blocksstable::ObDatumRow write_row_;
  ObArray<ObColDesc> col_ids_;
  ObArray<ObColDesc> org_col_ids_;
  ObArray<int32_t> output_projector_;
  DISALLOW_COPY_AND_ASSIGN(ObComplementWriteTask);
};

class ObComplementMergeTask final : public share::ObITask
{
public:
  ObComplementMergeTask();
  ~ObComplementMergeTask();
  int init(ObComplementDataParam &param, ObComplementDataContext &context);
  int process() override;
private:
  int add_build_hidden_table_sstable();
private:
  bool is_inited_;
  ObComplementDataParam *param_;
  ObComplementDataContext *context_;
  DISALLOW_COPY_AND_ASSIGN(ObComplementMergeTask);
};

struct ObExtendedGCParam final
{
public:
  ObExtendedGCParam():
    col_ids_(), org_col_ids_(), extended_col_ids_(), org_extended_col_ids_(), dependent_exprs_(), output_projector_()
  {}
  ~ObExtendedGCParam() {}
  common::ObArray<share::schema::ObColDesc> col_ids_;
  common::ObArray<share::schema::ObColDesc> org_col_ids_;
  common::ObArray<share::schema::ObColDesc> extended_col_ids_;
  common::ObArray<share::schema::ObColDesc> org_extended_col_ids_;
  ObArray<sql::ObTempExpr *> dependent_exprs_;
  common::ObArray<int32_t> output_projector_;
  TO_STRING_KV(K_(col_ids), K_(org_col_ids), K_(extended_col_ids), K_(org_extended_col_ids),
      K_(dependent_exprs), K_(output_projector));
};

class ObScan
{
public:
  ObScan() = default;
  virtual ~ObScan() = default;
  virtual int get_next_row(
      const blocksstable::ObDatumRow *&tmp_row,
      const blocksstable::ObDatumRow *&tmp_row_after_reshape) = 0;
  virtual compaction::ObColumnChecksumCalculator *get_checksum_calculator() = 0;
  virtual int get_origin_table_checksum(ObArray<int64_t> &report_col_checksums, ObArray<int64_t> &report_col_ids) = 0;
};

class ObLocalScan : public ObScan
{
public:
  ObLocalScan();
  virtual ~ObLocalScan();
  int init(const common::ObIArray<share::schema::ObColDesc> &col_ids,
           const common::ObIArray<share::schema::ObColDesc> &org_col_ids,
           const common::ObIArray<int32_t> &projector,
           const share::schema::ObTableSchema &data_table_schema,
           const int64_t snapshot_version,
           transaction::ObTransService *txs,
           const share::schema::ObTableSchema &hidden_table_schema,
           const bool output_org_cols_only);
  int table_scan(const share::schema::ObTableSchema &data_table_schema,
                 const share::ObLSID &ls_id,
                 const ObTabletID &tablet_id,
                 ObTabletTableIterator &table_iter,
                 common::ObQueryFlag &query_flag,
                 blocksstable::ObDatumRange &range,
                 transaction::ObTxDesc *tx_desc);
  virtual int get_next_row(
      const blocksstable::ObDatumRow *&tmp_row,
      const blocksstable::ObDatumRow *&tmp_row_after_reshape) override;
  int get_origin_table_checksum(
      ObArray<int64_t> &report_col_checksums,
      ObArray<int64_t> &report_col_ids) override;
  compaction::ObColumnChecksumCalculator *get_checksum_calculator() override
    {return &checksum_calculator_;}
private:
  int get_output_columns(
      const share::schema::ObTableSchema &hidden_table_schema,
      common::ObIArray<ObColDesc> &col_ids);
  int get_exist_column_mapping(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema); // to record data table columns position in hidden tables.
  int check_generated_column_exist(
      const share::schema::ObTableSchema &hidden_table_schema,
      const common::ObIArray<share::schema::ObColDesc> &org_col_ids);
  int construct_column_schema(
      const share::schema::ObTableSchema &data_table_schema);
  int construct_access_param(
      const share::schema::ObTableSchema &data_table_schema,
      const ObTabletID &tablet_id);
  int construct_range_ctx(common::ObQueryFlag &query_flag, const share::ObLSID &ls_id, transaction::ObTxDesc *tx_desc);
  int construct_multiple_scan_merge(ObTablet &tablet, blocksstable::ObDatumRange &range);
  int construct_multiple_scan_merge(
      ObTabletTableIterator &table_iter,
      blocksstable::ObDatumRange &range);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t dest_table_id_;
  int64_t schema_version_;
  ObExtendedGCParam extended_gc_;
  int64_t snapshot_version_;
  transaction::ObTransService *txs_;
  blocksstable::ObDatumRow default_row_;
  blocksstable::ObDatumRow tmp_row_;
  ObIStoreRowIterator *row_iter_;
  ObMultipleScanMerge *scan_merge_;
  ObStoreCtx ctx_;
  ObTableAccessParam access_param_;
  ObTableAccessContext access_ctx_;
  ObGetTableParam get_table_param_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator calc_buf_;
  ObExprCtx expr_ctx_;
  common::ObArray<share::schema::ObColumnParam *> col_params_;
  ObTableReadInfo read_info_;
  common::ObBitmap exist_column_mapping_;
  compaction::ObColumnChecksumCalculator checksum_calculator_;
  bool output_org_cols_only_;
};

class ObRemoteScan : public ObScan
{
public:
  ObRemoteScan();
  virtual ~ObRemoteScan();
  int init(const uint64_t tenant_id,
           const int64_t table_id,
           const uint64_t dest_tenant_id,
           const int64_t dest_table_id,
           const int64_t schema_version,
           const int64_t dest_schema_version,
           const common::ObTabletID &src_tablet_id);
  void reset();
  virtual int get_next_row(
      const blocksstable::ObDatumRow *&tmp_row,
      const blocksstable::ObDatumRow *&tmp_row_after_reshape) override;
  compaction::ObColumnChecksumCalculator *get_checksum_calculator() override
    {return &checksum_calculator_;}
  int get_origin_table_checksum(ObArray<int64_t> &report_col_checksums, ObArray<int64_t> &report_col_ids) override;
private:
  int prepare_iter(const ObSqlString &sql_string, common::ObCommonSqlProxy *sql_proxy);
  int generate_build_select_sql(ObSqlString &sql_string);
  // to fetch partiton/subpartition name for select sql.
  int fetch_source_part_info(
      const common::ObTabletID &src_tablet_id,
      const share::schema::ObTableSchema &src_table_schema,
      const ObBasePartition*& source_partition);
  int generate_column_name_str(const bool is_oracle_mode,
                               ObSqlString &sql_string);
  int generate_column_name_str(const ObString &column_name_info,
                               const bool with_comma,
                               const bool is_oracle_mode,
                               ObSqlString &sql_string);
private:
  bool is_inited_;
  int64_t current_;
  uint64_t tenant_id_;
  int64_t table_id_;
  uint64_t dest_tenant_id_;
  int64_t dest_table_id_;
  int64_t schema_version_;
  int64_t dest_schema_version_;
  common::ObTabletID src_tablet_id_;
  blocksstable::ObDatumRow row_without_reshape_;
  blocksstable::ObDatumRow row_with_reshape_; // for checksum calculate only.
  ObMySQLProxy::MySQLResult res_;
  sqlclient::ObMySQLResult *result_;
  common::ObArenaAllocator allocator_;
  ObArray<ObColDesc> org_col_ids_;
  common::ObArray<ObColumnNameInfo> column_names_;
  compaction::ObColumnChecksumCalculator checksum_calculator_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_COMPLEMENT_DATA_TASK_H
