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

#ifndef OCEANBASE_STORAGE_OB_BUILD_INDEX_TASK_H_
#define OCEANBASE_STORAGE_OB_BUILD_INDEX_TASK_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_store_row_comparer.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ddl/ob_complement_data_task.h"

namespace oceanbase
{
namespace storage
{
class ObIStoreRowIterator;
class ObUniqueCheckingParam;
class ObUniqueCheckingContext;

class ObUniqueIndexChecker
{
public:
  ObUniqueIndexChecker();
  virtual ~ObUniqueIndexChecker() = default;
  int init(ObUniqueCheckingParam &param, ObUniqueCheckingContext &context);
  int check_unique_index(share::ObIDag *dag, const int64_t task_id);
private:
  struct ObScanTableParam
  {
  public:
    ObScanTableParam()
      : data_table_schema_(NULL), index_schema_(NULL), snapshot_version_(0),
        col_ids_(NULL), org_col_ids_(), output_projector_(NULL), is_scan_index_(false), task_id_(0)
    {}
    bool is_valid() const
    {
      return NULL != data_table_schema_ && NULL != index_schema_ && snapshot_version_ > 0
          && NULL != col_ids_ && NULL != org_col_ids_ && NULL != output_projector_ && task_id_ >= 0;
    }
    TO_STRING_KV(KP_(data_table_schema), KP_(index_schema), K_(snapshot_version),
        KP_(col_ids), KP_(output_projector), K_(task_id));
    const share::schema::ObTableSchema *data_table_schema_;
    const share::schema::ObTableSchema *index_schema_;
    int64_t snapshot_version_;
    common::ObIArray<share::schema::ObColDesc> *col_ids_;
    common::ObIArray<share::schema::ObColDesc> *org_col_ids_;
    common::ObIArray<int32_t> *output_projector_;
    bool is_scan_index_;
    int64_t task_id_;
  };
  int wait_trans_end(share::ObIDag *dag);
  int check_global_index(share::ObIDag *dag, const int64_t task_id);
  int generate_index_output_param(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_schema,
      common::ObArray<share::schema::ObColDesc> &col_ids,
      common::ObArray<share::schema::ObColDesc> &org_col_ids,
      common::ObArray<int32_t> &output_projector);
  int get_index_columns_without_virtual_generated_and_shadow_columns(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_table_schema,
      common::ObIArray<ObColDesc> &cols);
  int scan_main_table_with_column_checksum(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_schema,
      const int64_t snapshot_version,
      const int64_t task_id,
      common::ObIArray<int64_t> &column_checksum,
      int64_t &row_count);
  int scan_index_table_with_column_checksum(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_schema,
      const int64_t snapshot_version,
      const int64_t task_id,
      common::ObIArray<int64_t> &column_checksum,
      int64_t &row_count);
  int scan_table_with_column_checksum(
      const ObScanTableParam &param,
      common::ObIArray<int64_t> &column_checksum,
      int64_t &row_count);
  int calc_column_checksum(
      const common::ObIArray<bool> &need_reshape,
      const ObColDescIArray &cols_desc,
      const common::ObIArray<int32_t> &output_projector,
      ObLocalScan &iterator,
      common::ObIArray<int64_t> &column_checksum,
      int64_t &row_count);
  int report_column_checksum(const common::ObIArray<int64_t> &column_checksum,
                             const int64_t report_table_id);
private:
  static const int64_t RETRY_INTERVAL = 10 * 1000L;
  bool is_inited_;
  ObUniqueCheckingParam *param_;
  ObUniqueCheckingContext *context_;
  ObTabletHandle tablet_handle_;
};

class ObIUniqueCheckingCompleteCallback
{
public:
  virtual int operator()(const int ret_code) = 0;
  virtual ~ObIUniqueCheckingCompleteCallback() {};
};

class ObGlobalUniqueIndexCallback : public ObIUniqueCheckingCompleteCallback
{
public:
  ObGlobalUniqueIndexCallback(const uint64_t tenant_id, const common::ObTabletID &tablet_id, const uint64_t index_id,
      const uint64_t data_table_id, const int64_t schema_version, const int64_t task_id);
  int operator()(const int ret_code) override;
private:
  uint64_t tenant_id_;
  common::ObTabletID tablet_id_;
  uint64_t index_id_;
  uint64_t data_table_id_;
  uint64_t schema_version_;
  int64_t task_id_;
};

class ObLocalUniqueIndexCallback : public ObIUniqueCheckingCompleteCallback
{
public:
  ObLocalUniqueIndexCallback();
  int operator()(const int ret_code) override;
};

struct ObUniqueCheckingParam final
{
public:
  ObUniqueCheckingParam():
    is_inited_(false), tenant_id_(common::OB_INVALID_TENANT_ID), ls_id_(share::ObLSID::INVALID_LS_ID), tablet_id_(),
    is_scan_index_(false), schema_service_(nullptr), schema_guard_(share::schema::ObSchemaMgrItem::MOD_UNIQ_CHECK),
    index_schema_(nullptr), data_table_schema_(nullptr), callback_(nullptr), execution_id_(0),
    snapshot_version_(0), task_id_(0), compat_mode_(lib::Worker::CompatMode::INVALID), user_parallelism_(0),
    concurrent_cnt_(0), ranges_(), allocator_("UniqueChecking", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
    {}
  ~ObUniqueCheckingParam() { destroy(); }
  int init(const uint64_t tenant_id,
          const ObLSID &ls_id,
          const ObTabletID &tablet_id,
          const bool is_scan_index,
          const uint64_t index_table_id,
          const int64_t schema_version,
          const int64_t task_id,
          const int64_t execution_id,
          const int64_t snapshot_version,
          const int64_t user_parallelism);
  int prepare_task_ranges();
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid() && tablet_id_.is_valid() && snapshot_version_ > 0
    && schema_service_ != nullptr && compat_mode_ != lib::Worker::CompatMode::INVALID && execution_id_ >= 0 && task_id_ > 0
    && user_parallelism_ > 0;
  }

  bool has_generated_task_ranges() const {
    return concurrent_cnt_ > 0 && !ranges_.empty() && concurrent_cnt_ == ranges_.count();
  }
  void destroy()
  {
    is_inited_ = false;
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    ls_id_.reset();
    tablet_id_.reset();
    is_scan_index_ = false;
    schema_service_ = nullptr;
    schema_guard_.reset();
    index_schema_ = nullptr;
    data_table_schema_ = nullptr;
    if (NULL != callback_) {
      callback_->~ObIUniqueCheckingCompleteCallback();
      ob_free(callback_);
      callback_ = NULL;
    }
    execution_id_ = 0;
    snapshot_version_ = 0;
    task_id_ = 0;
    user_parallelism_ = 0;
    concurrent_cnt_ = 0;
    ranges_.reset();
  }
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(is_scan_index), KP_(index_schema),
    KP_(data_table_schema), K_(execution_id), K_(snapshot_version), K_(task_id), K_(compat_mode),
    K_(user_parallelism), K_(concurrent_cnt), K_(ranges));
public:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  bool is_scan_index_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema *index_schema_;
  const share::schema::ObTableSchema *data_table_schema_;
  ObIUniqueCheckingCompleteCallback *callback_;
  int64_t execution_id_;
  int64_t snapshot_version_;
  int64_t task_id_;
  lib::Worker::CompatMode compat_mode_;
  int64_t user_parallelism_;
  int64_t concurrent_cnt_;
  ObArray<blocksstable::ObDatumRange> ranges_;
  common::ObArenaAllocator allocator_;
};

struct ObUniqueCheckingContext final
{
public:
  ObUniqueCheckingContext():
    is_inited_(false), lock_(ObLatchIds::UNIQUE_CHECKING_CONTEXT_LOCK), unique_checking_ret_(common::OB_SUCCESS)
  {}
  ~ObUniqueCheckingContext() { destroy(); }
  int init(const ObUniqueCheckingParam *param);
  void destroy()
  {
    is_inited_ = false;
    report_column_checksums_.reset();
    report_col_ids_.reset();
    unique_checking_ret_ = common::OB_SUCCESS;
  }
  int add_column_checksum(const ObIArray<int64_t> &report_col_checksums);
  int get_column_checksum_and_id(ObIArray<int64_t> &report_col_checksums, ObIArray<int64_t> &report_col_ids);
  TO_STRING_KV(K_(is_inited), K_(unique_checking_ret), K_(report_column_checksums), K_(report_col_ids));
public:
  bool is_inited_;
  ObSpinLock lock_;
  int64_t unique_checking_ret_;
  ObArray<int64_t> report_column_checksums_;
  ObArray<int64_t> report_col_ids_;
};

 /* Unique key checking is a time-consuming operation, so the operation is executed
 * int the dag thread pool. When it finishes unique key checking, it will call a rpc to inform
 * ROOTSERVICE the result of the unique key checking result
 */
class ObUniqueCheckingDag : public share::ObIDag
{
public:
  ObUniqueCheckingDag();
  virtual ~ObUniqueCheckingDag() = default;
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool is_scan_index,
      const uint64_t index_table_id, const int64_t schema_version,
      const int64_t task_id,
      const int64_t execution_id = -1,
      const int64_t snapshot_version = OB_INVALID_VERSION,
      const int64_t parallelism = 1);
  const share::schema::ObTableSchema *get_index_schema() const { return param_.index_schema_; }
  const share::schema::ObTableSchema *get_data_table_schema() const { return param_.data_table_schema_; }
  int64_t get_execution_id() const { return param_.execution_id_; }
  int64_t get_snapshot_version() const { return param_.snapshot_version_; }
  int64_t get_task_id() const { return param_.task_id_; }
  bool get_is_scan_index() const { return param_.is_scan_index_; }
  int alloc_unique_checking_prepare_task(ObUniqueCheckingParam &param, ObUniqueCheckingContext &context);
  int alloc_local_index_task_callback(ObLocalUniqueIndexCallback *&callback);
  int alloc_global_index_task_callback(
    const ObTabletID &tablet_id, const uint64_t index_id,
    const uint64_t data_table_id, const int64_t schema_version,
    const int64_t task_id,
    ObGlobalUniqueIndexCallback *&callback);
  virtual uint64_t hash() const override;
  virtual bool operator ==(const share::ObIDag &other) const;
  common::ObTabletID get_tablet_id() const { return param_.tablet_id_; }
  uint64_t get_tenant_id() const { return param_.tenant_id_; }
  share::ObLSID get_ls_id() const { return param_.ls_id_; }
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual bool ignore_warning() override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return param_.compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
  ObUniqueCheckingParam &get_param() { return param_; }
  ObUniqueCheckingContext &get_context() { return context_; }
  int prepare_context();
private:
  bool is_inited_;
  ObUniqueCheckingParam param_;
  ObUniqueCheckingContext context_;
};

class ObUniqueCheckingPrepareTask : public share::ObITask
{
public:
  ObUniqueCheckingPrepareTask();
  virtual ~ObUniqueCheckingPrepareTask() = default;
  int init(ObUniqueCheckingParam &param, ObUniqueCheckingContext &context);
  virtual int process() override;
private:
  int generate_unique_checking_task(ObUniqueCheckingDag *dag);
private:
  static const int64_t RETRY_INTERVAL = 10 * 1000L;
  bool is_inited_;
  ObUniqueCheckingParam *param_;
  ObUniqueCheckingContext *context_;
  int report_unique_check_error_(share::ObIDag *dag, int64_t &ret_code);
};

class ObSimpleUniqueCheckingTask : public share::ObITask
{
public:
  ObSimpleUniqueCheckingTask();
  virtual ~ObSimpleUniqueCheckingTask() = default;
  int init(const int64_t task_id, ObUniqueCheckingParam &param, ObUniqueCheckingContext &context);
  virtual int process() override;
  int generate_next_task(share::ObITask *&next_task);
private:
  bool is_inited_;
  ObUniqueIndexChecker unique_checker_;
  int64_t task_id_;
  ObUniqueCheckingParam *param_;
  ObUniqueCheckingContext *context_;
};

class ObUniqueCheckingMergeTask final : public share::ObITask
{
public:
  ObUniqueCheckingMergeTask();
  virtual ~ObUniqueCheckingMergeTask() = default;
  int init(ObUniqueCheckingParam &param, ObUniqueCheckingContext &context);
  virtual int process() override;
private:
  static const int64_t RETRY_INTERVAL = 10 * 1000L;
  bool is_inited_;
  ObUniqueCheckingParam *param_;
  ObUniqueCheckingContext *context_;
  int report_unique_check_error(share::ObIDag *dag, int64_t &ret_code);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_BUILD_INDEX_TASK_H_
