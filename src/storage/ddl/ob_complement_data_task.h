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

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/ddl/ob_cg_macro_block_write_task.h"
#include "storage/ddl/ob_tablet_slice_row_iterator.h"

namespace oceanbase
{

namespace storage
{
template <typename T>
int add_dag_and_get_progress(
    T *dag,
    int64_t &row_inserted,
    int64_t &cg_row_inserted,
    int64_t &physical_row_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  row_inserted = 0;
  cg_row_inserted=0;
  physical_row_count = 0;
  if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(dag));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(dag))) {
    // caution ret = OB_EAGAIN or OB_SIZE_OVERFLOW
    if (OB_EAGAIN == ret
        && OB_TMP_FAIL(MTL(ObTenantDagScheduler*)->get_dag_progress<T>(dag, row_inserted, cg_row_inserted, physical_row_count))) {
      // tmp_ret is used to prevent the failure from affecting DDL_Task status
      LOG_WARN("get complement data progress failed", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

class ObScan;
class ObLocalScan;
class ObRemoteScan;
class ObMultipleScanMerge;
class ObChunk;
struct ObComplementDataParam final
{
public:
  ObComplementDataParam():
    is_inited_(false), orig_tenant_id_(common::OB_INVALID_TENANT_ID), dest_tenant_id_(common::OB_INVALID_TENANT_ID),
    orig_ls_id_(share::ObLSID::INVALID_LS_ID), dest_ls_id_(share::ObLSID::INVALID_LS_ID), orig_table_id_(common::OB_INVALID_ID),
    dest_table_id_(common::OB_INVALID_ID), orig_tablet_id_(ObTabletID::INVALID_TABLET_ID), dest_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    row_store_type_(common::ENCODING_ROW_STORE), orig_schema_version_(0), dest_schema_version_(0),
    snapshot_version_(0), task_id_(0), execution_id_(-1), tablet_task_id_(0), compat_mode_(lib::Worker::CompatMode::INVALID), data_format_version_(0),
    orig_schema_tablet_size_(0), dest_schema_cg_cnt_(0), user_parallelism_(0), concurrent_cnt_(0), ranges_(),
    is_no_logging_(false), dest_lob_meta_tablet_id_(), allocator_("CompleteDataPar", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  ~ObComplementDataParam() { destroy(); }
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  int prepare_task_ranges();
  int split_task_ranges(
      const int64_t task_id,
      const uint64_t data_format_version,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t tablet_size,
      const int64_t hint_parallelism);
  int split_task_ranges_remote(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t tablet_size,
    const int64_t hint_parallelism,
    const int64_t dest_schema_cg_cnt);
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != orig_tenant_id_ && common::OB_INVALID_TENANT_ID != dest_tenant_id_
           && orig_ls_id_.is_valid() && dest_ls_id_.is_valid() && common::OB_INVALID_ID != orig_table_id_
           && common::OB_INVALID_ID != dest_table_id_ && orig_tablet_id_.is_valid() && dest_tablet_id_.is_valid()
           && snapshot_version_ > 0 && compat_mode_ != lib::Worker::CompatMode::INVALID && execution_id_ >= 0 && tablet_task_id_ > 0
           && data_format_version_ > 0 && orig_schema_tablet_size_ > 0 && dest_schema_cg_cnt_ > 0 && user_parallelism_ > 0;
  }

  bool has_generated_task_ranges() const {
    return concurrent_cnt_ > 0 && !ranges_.empty() && concurrent_cnt_ == ranges_.count();
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
    dest_lob_meta_tablet_id_.reset();
    row_store_type_ = common::ENCODING_ROW_STORE;
    orig_schema_version_ = 0;
    dest_schema_version_ = 0;
    snapshot_version_ = 0;
    task_id_ = 0;
    execution_id_ = -1;
    tablet_task_id_ = 0;
    compat_mode_ = lib::Worker::CompatMode::INVALID;
    data_format_version_ = 0;
    orig_schema_tablet_size_ = 0;
    dest_schema_cg_cnt_ = 0;
    user_parallelism_ = 0;
    concurrent_cnt_ = 0;
    is_no_logging_ = false;
    ranges_.reset();
    direct_load_type_ = DIRECT_LOAD_INVALID;
    if (nullptr != tablet_param_.storage_schema_) {
      tablet_param_.storage_schema_->~ObStorageSchema();
      allocator_.free(tablet_param_.storage_schema_);
      tablet_param_.storage_schema_ = nullptr;
    }
    if (nullptr != lob_meta_tablet_param_.storage_schema_) {
      lob_meta_tablet_param_.storage_schema_->~ObStorageSchema();
      allocator_.free(lob_meta_tablet_param_.storage_schema_);
      lob_meta_tablet_param_.storage_schema_ = nullptr;
    }
    allocator_.reset();
  }
  TO_STRING_KV(K_(is_inited), K_(orig_tenant_id), K_(dest_tenant_id), K_(orig_ls_id), K_(dest_ls_id),
      K_(orig_table_id), K_(dest_table_id), K_(orig_tablet_id), K_(dest_tablet_id), K_(orig_schema_version),
      K_(tablet_task_id), K_(dest_schema_version), K_(snapshot_version), K_(task_id),
      K_(execution_id), K_(compat_mode), K_(data_format_version), K_(orig_schema_tablet_size),K_(user_parallelism),
      K_(concurrent_cnt), K_(ranges), K_(is_no_logging), K_(direct_load_type), K_(tablet_param), K_(lob_meta_tablet_param));
private:
  int fill_tablet_param();
  int get_complement_parallel_mode(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t schema_version,
      const lib::Worker::CompatMode compat_mode,
      const bool is_recover_table,
      bool &is_allow_parallel);
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
  common::ObRowStoreType row_store_type_;
  int64_t orig_schema_version_;
  int64_t dest_schema_version_;
  int64_t snapshot_version_;
  int64_t task_id_;
  int64_t execution_id_;
  int64_t tablet_task_id_;
  lib::Worker::CompatMode compat_mode_;
  int64_t data_format_version_;
  int64_t orig_schema_tablet_size_;
  int64_t dest_schema_cg_cnt_;
  int64_t user_parallelism_;  /* user input parallelism */
  /* complememt prepare task will initialize parallel task ranges */
  int64_t concurrent_cnt_; /* real complement tasks num */
  ObArray<blocksstable::ObDatumRange> ranges_;
  bool is_no_logging_;
  ObDDLTableSchema ddl_table_schema_;
  ObWriteTabletParam tablet_param_;
  ObWriteTabletParam lob_meta_tablet_param_;
  ObDirectLoadType direct_load_type_;
  ObTabletID dest_lob_meta_tablet_id_;
  common::ObArenaAllocator allocator_;
  static constexpr int64_t MAX_RPC_STREAM_WAIT_THREAD_COUNT = 100;
  static constexpr int64_t ROW_TABLE_PARALLEL_MIN_TASK_SIZE = 2 * 1024 * 1024L; /*2MB*/
  static constexpr int64_t COLUMN_TABLE_EACH_CG_PARALLEL_MIN_TASK_SIZE = 4 * 1024 * 1024L; /*4MB*/
};

void add_ddl_event(const ObComplementDataParam *param, const ObString &stmt);

struct ObComplementDataContext final
{
public:
  ObComplementDataContext():
    allocator_("DDL_COMPL_CTX", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    is_inited_(false), is_major_sstable_exist_(false), complement_data_ret_(common::OB_SUCCESS),
    lock_(ObLatchIds::COMPLEMENT_DATA_CONTEXT_LOCK), concurrent_cnt_(0),
    physical_row_count_(0), row_scanned_(0), row_inserted_(0), total_slice_cnt_(-1),
    tablet_ctx_(), cg_row_inserted_(0)
  {}
  ~ObComplementDataContext() { destroy(); }
  int init(
    const ObComplementDataParam &param,
    const share::schema::ObTableSchema &hidden_table_schema);
  void destroy();
  int add_column_checksum(const ObIArray<int64_t> &report_col_checksums, const ObIArray<int64_t> &report_col_ids);
  int get_column_checksum(ObIArray<int64_t> &report_col_checksums, ObIArray<int64_t> &report_col_ids);

  TO_STRING_KV(K_(is_inited), K_(is_major_sstable_exist), K_(complement_data_ret), K_(concurrent_cnt),
      K_(physical_row_count), K_(row_scanned), K_(row_inserted), K_(total_slice_cnt));
public:
  ObArenaAllocator allocator_;
  bool is_inited_;
  bool is_major_sstable_exist_;
  int complement_data_ret_;
  ObSpinLock lock_;
  int64_t concurrent_cnt_;
  int64_t physical_row_count_;
  int64_t row_scanned_;
  int64_t row_inserted_;
  ObArray<int64_t> report_col_checksums_;
  ObArray<int64_t> report_col_ids_;
  int64_t total_slice_cnt_;
  ObDDLTabletContext *tablet_ctx_;

  /* for compat, unused yet */
  int64_t cg_row_inserted_;
};

class ObComplementPrepareTask;
class ObComplementWriteTask;
class ObComplementMergeTask;
class ObComplementDataDag final: public share::ObIDag
{
public:
  ObComplementDataDag();
  ~ObComplementDataDag();
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  int prepare_context();
  virtual uint64_t hash() const override;
  bool operator ==(const share::ObIDag &other) const;
  bool is_inited() const { return is_inited_; }
  void handle_init_failed_ret_code(int ret) { context_.complement_data_ret_ = ret; }
  ObComplementDataParam &get_param() { return param_; }
  ObComplementDataContext &get_context() { return context_; }
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
  int calc_total_row_count();
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

class ObComplementWriteMacroOperator : public ObWriteMacroBaseOperator
{
public:
  explicit ObComplementWriteMacroOperator(ObPipeline *pipeline)
    : ObWriteMacroBaseOperator(pipeline)
  {}
  virtual ~ObComplementWriteMacroOperator() = default;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override;
  virtual int try_execute_finish(const ObChunk &input_chunk,
                                 ResultState &result_state,
                                 ObChunk &output_chunk);
};

class ObComplementRowIterator : public ObIStoreRowIterator
{
public:
  ObComplementRowIterator()
    : is_inited_(false), scan_(nullptr)
  {}
  virtual ~ObComplementRowIterator() = default;
  int init(ObScan *scan);
  virtual int get_next_row(const blocksstable::ObDatumRow *&row);
private:
  bool is_inited_;
  ObScan *scan_;
};

class ObComplementWriteTask final : public ObWriteMacroPipeline
{
public:
  ObComplementWriteTask();
  ~ObComplementWriteTask();
  int init(const int64_t task_id, ObComplementDataParam &param, ObComplementDataContext &context);
  virtual int preprocess() override;
  virtual void postprocess(int &ret_code) override;
protected:
  virtual int get_next_chunk(ObChunk *&next_chunk) override;
  virtual int fill_writer_param(ObWriteMacroParam &param);
private:
  virtual int generate_next_task(share::ObITask *&next_task) override;
  int generate_col_param();
  int local_scan_by_range();
  int remote_scan();
  int do_local_scan();
  int do_remote_scan();

private:
  static const int64_t RETRY_INTERVAL = 100 * 1000; // 100ms
  bool is_inited_;
  int64_t task_id_;
  ObComplementDataParam *param_;
  ObComplementDataContext *context_;
  ObArray<ObColDesc> col_ids_;
  ObArray<ObColDesc> org_col_ids_;
  ObArray<int32_t> output_projector_;
  ObComplementWriteMacroOperator write_op_;
  ObScan *scan_;
  common::ObArenaAllocator allocator_;
  ObComplementRowIterator row_iter_;
  ObTabletSliceRowIterator slice_row_iter_;
  ObChunk chunk_;
  blocksstable::ObDatumRange scan_range_;
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
  bool is_inited_;
  ObComplementDataParam *param_;
  ObComplementDataContext *context_;
  DISALLOW_COPY_AND_ASSIGN(ObComplementMergeTask);
};

struct ObExtendedGCParam final
{
public:
  ObExtendedGCParam():
    col_ids_(), org_col_ids_(), extended_col_ids_(), org_extended_col_ids_(), output_projector_()
  {}
  ~ObExtendedGCParam() {}
  common::ObArray<share::schema::ObColDesc> col_ids_;
  common::ObArray<share::schema::ObColDesc> org_col_ids_;
  common::ObArray<share::schema::ObColDesc> extended_col_ids_;
  common::ObArray<share::schema::ObColDesc> org_extended_col_ids_;
  common::ObArray<int32_t> output_projector_;
  TO_STRING_KV(K_(col_ids), K_(org_col_ids), K_(extended_col_ids), K_(org_extended_col_ids), K_(output_projector));
};

class ObScan : public ObIStoreRowIterator
{
public:
  ObScan() :
    schema_rowkey_cnt_(0),
    snapshot_version_(0),
    mult_version_cols_desc_(),
    checksum_calculator_()
  { }
  virtual ~ObScan() = default;
  virtual int get_origin_table_checksum(ObArray<int64_t> &report_col_checksums, ObArray<int64_t> &report_col_ids) = 0;
protected:
  int64_t storaged_index_with_extra_rowkey(const int64_t i)
  {
    return i < schema_rowkey_cnt_ ? i : i + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }
protected:
  int64_t schema_rowkey_cnt_;
  int64_t snapshot_version_;
  common::ObArray<share::schema::ObColDesc> mult_version_cols_desc_; // for checksum calculate, with extra rowkey.
  compaction::ObColumnChecksumCalculator checksum_calculator_;
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
           const share::schema::ObTableSchema &hidden_table_schema,
           const bool unique_index_checking);
  int table_scan(const share::schema::ObTableSchema &data_table_schema,
                 const share::ObLSID &ls_id,
                 const ObTabletID &tablet_id,
                 ObTabletTableIterator &table_iter,
                 common::ObQueryFlag &query_flag,
                 blocksstable::ObDatumRange &range);
  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;
  int get_origin_table_checksum(
      ObArray<int64_t> &report_col_checksums,
      ObArray<int64_t> &report_col_ids) override;
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
  int construct_range_ctx(common::ObQueryFlag &query_flag, const share::ObLSID &ls_id);
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
  transaction::ObTransService *txs_;
  blocksstable::ObDatumRow default_row_;
  blocksstable::ObDatumRow write_row_; // with extra rowkey.
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
  bool unique_index_checking_;
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
           const int64_t snapshot_version,
           const common::ObTabletID &src_tablet_id,
           const ObDatumRange &datum_range);
  void reset();
  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;
  int get_origin_table_checksum(ObArray<int64_t> &report_col_checksums, ObArray<int64_t> &report_col_ids) override;
private:
  int prepare_iter(const ObSqlString &sql_string, common::ObCommonSqlProxy *sql_proxy);
  int generate_build_select_sql(ObSqlString &sql_string);
  // to fetch partiton/subpartition name for select sql.
  int generate_range_condition(const ObDatumRange &datum_range,
                               bool is_oracle_mode,
                               ObSqlString &sql);
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
  int convert_rowkey_to_sql_literal(
      const ObRowkey &rowkey,
      bool is_oracle_mode,
      char *buf,
      int64_t &pos,
      int64_t buf_len);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t table_id_;
  uint64_t dest_tenant_id_;
  int64_t dest_table_id_;
  int64_t schema_version_;
  int64_t dest_schema_version_;
  common::ObTabletID src_tablet_id_;
  blocksstable::ObDatumRow row_with_reshape_; // for checksum calculate only.
  blocksstable::ObDatumRow write_row_; // with extra rowkey.
  ObMySQLProxy::MySQLResult res_;
  sqlclient::ObMySQLResult *result_;
  const blocksstable::ObDatumRange *datum_range_;
  common::ObArenaAllocator allocator_;
  ObArray<ObColDesc> org_col_ids_;
  common::ObArray<ObColumnNameInfo> column_names_;
  common::ObArray<ObAccuracy> rowkey_col_accuracys_;
  ObTimeZoneInfoWrap tz_info_wrap_; // for table recovery
  compaction::ObColumnChecksumCalculator checksum_calculator_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_COMPLEMENT_DATA_TASK_H
