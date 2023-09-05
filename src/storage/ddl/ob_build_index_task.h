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

#include "share/scheduler/ob_dag_scheduler.h"
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

class ObUniqueIndexChecker
{
public:
  ObUniqueIndexChecker();
  virtual ~ObUniqueIndexChecker() = default;
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool is_scan_index,
      const share::schema::ObTableSchema *data_table_schema,
      const share::schema::ObTableSchema *index_schema,
      const int64_t task_id,
      const int64_t execution_id = -1,
      const int64_t snapshot_version = OB_INVALID_VERSION);
  int check_unique_index(share::ObIDag *dag);
private:
  struct ObScanTableParam
  {
  public:
    ObScanTableParam()
      : data_table_schema_(NULL), index_schema_(NULL), snapshot_version_(0),
        col_ids_(NULL), org_col_ids_(), output_projector_(NULL), is_scan_index_(false)
    {}
    bool is_valid() const
    {
      return NULL != data_table_schema_ && NULL != index_schema_ && snapshot_version_ > 0
          && NULL != col_ids_ && NULL != org_col_ids_ && NULL != output_projector_;
    }
    TO_STRING_KV(KP_(data_table_schema), KP_(index_schema), K_(snapshot_version),
        KP_(col_ids), KP_(output_projector));
    const share::schema::ObTableSchema *data_table_schema_;
    const share::schema::ObTableSchema *index_schema_;
    int64_t snapshot_version_;
    common::ObIArray<share::schema::ObColDesc> *col_ids_;
    common::ObIArray<share::schema::ObColDesc> *org_col_ids_;
    common::ObIArray<int32_t> *output_projector_;
    bool is_scan_index_;
  };
  int wait_trans_end(share::ObIDag *dag);
  int check_global_index(share::ObIDag *dag);
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
      common::ObIArray<int64_t> &column_checksum,
      int64_t &row_count);
  int scan_index_table_with_column_checksum(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_schema,
      const int64_t snapshot_version,
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
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  const share::schema::ObTableSchema *index_schema_;
  const share::schema::ObTableSchema *data_table_schema_;
  int64_t execution_id_;
  int64_t snapshot_version_;
  int64_t task_id_;
  ObTabletHandle tablet_handle_;
  bool is_scan_index_;
};

class ObIUniqueCheckingCompleteCallback
{
public:
  virtual int operator()(const int ret_code) = 0;
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

 /* Unique key checking is a time-consuming operation, so the operation is executed
 * int the dag thread pool. When it finishes unique key checking, it will call a rpc to inform
 * ROOTSERVICE the result of the unique key checking result
 */
class ObUniqueCheckingDag : public share::ObIDag
{
public:
  ObUniqueCheckingDag();
  virtual ~ObUniqueCheckingDag();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool is_scan_index,
      const uint64_t index_table_id, const int64_t schema_version,
      const int64_t task_id,
      const int64_t execution_id = -1,
      const int64_t snapshot_version = OB_INVALID_VERSION);
  const share::schema::ObTableSchema *get_index_schema() const { return index_schema_; }
  const share::schema::ObTableSchema *get_data_table_schema() const { return data_table_schema_; }
  int64_t get_execution_id() const { return execution_id_; }
  int64_t get_snapshot_version() const { return snapshot_version_; }
  int64_t get_task_id() const { return task_id_; }
  bool get_is_scan_index() const { return is_scan_index_; }
  int alloc_unique_checking_prepare_task(ObIUniqueCheckingCompleteCallback *callback);
  int alloc_local_index_task_callback(ObLocalUniqueIndexCallback *&callback);
  int alloc_global_index_task_callback(
    const ObTabletID &tablet_id, const uint64_t index_id,
    const uint64_t data_table_id, const int64_t schema_version,
    const int64_t task_id,
    ObGlobalUniqueIndexCallback *&callback);
  virtual int64_t hash() const;
  virtual bool operator ==(const share::ObIDag &other) const;
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual bool ignore_warning() override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
private:
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
};

class ObUniqueCheckingPrepareTask : public share::ObITask
{
public:
  ObUniqueCheckingPrepareTask();
  virtual ~ObUniqueCheckingPrepareTask() = default;
  int init(ObIUniqueCheckingCompleteCallback *callback);
  virtual int process() override;
private:
  int generate_unique_checking_task(ObUniqueCheckingDag *dag);
private:
  bool is_inited_;
  const share::schema::ObTableSchema *index_schema_;
  const share::schema::ObTableSchema *data_table_schema_;
  ObIUniqueCheckingCompleteCallback *callback_;
};

class ObSimpleUniqueCheckingTask : public share::ObITask
{
public:
  ObSimpleUniqueCheckingTask();
  virtual ~ObSimpleUniqueCheckingTask() = default;
  int init(
      const uint64_t tenant_id,
      const share::schema::ObTableSchema *data_table_schema,
      const share::schema::ObTableSchema *index_schema,
      ObIUniqueCheckingCompleteCallback *callback);
  virtual int process() override;
private:
  bool is_inited_;
  ObUniqueIndexChecker unique_checker_;
  uint64_t tenant_id_;
  const share::schema::ObTableSchema *index_schema_;
  const share::schema::ObTableSchema *data_table_schema_;
  common::ObTabletID tablet_id_;
  ObIUniqueCheckingCompleteCallback *callback_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_BUILD_INDEX_TASK_H_
