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

#ifndef OCEANBASE_STORAGE_OB_DELETE_LOB_META_ROW_TASK_H
#define OCEANBASE_STORAGE_OB_DELETE_LOB_META_ROW_TASK_H

#include "storage/access/ob_table_access_context.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"


namespace oceanbase
{
namespace storage
{

struct ObDeleteLobMetaRowParam final
{
public:
  ObDeleteLobMetaRowParam():
    is_inited_(false), tenant_id_(common::OB_INVALID_TENANT_ID),  
    table_id_(common::OB_INVALID_ID), schema_id_(common::OB_INVALID_ID), ls_id_(share::ObLSID::INVALID_LS_ID), 
    tablet_id_(ObTabletID::INVALID_TABLET_ID), dest_tablet_id_(ObTabletID::INVALID_TABLET_ID), 
    row_store_type_(common::ENCODING_ROW_STORE), schema_version_(0), 
    snapshot_version_(0), task_id_(0), execution_id_(-1), tablet_task_id_(0), delete_lob_meta_ret_(common::OB_SUCCESS),
    compat_mode_(lib::Worker::CompatMode::INVALID), data_format_version_(0),
    allocator_("CompleteDataPar", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  ~ObDeleteLobMetaRowParam() { destroy(); }
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid() && common::OB_INVALID_ID != schema_id_
           && common::OB_INVALID_ID != table_id_ && tablet_id_.is_valid() && dest_tablet_id_.is_valid()
           && snapshot_version_ > 0 && compat_mode_ != lib::Worker::CompatMode::INVALID 
           && execution_id_ >= 0 && tablet_task_id_ > 0 && data_format_version_ > 0;
  }
  
  int get_hidden_table_key(ObITable::TableKey &table_key) const;
  void destroy()
  {
    is_inited_ = false;
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    ls_id_.reset();
    table_id_ = common::OB_INVALID_ID;
    schema_id_ = common::OB_INVALID_ID;
    tablet_id_.reset();
    dest_tablet_id_.reset();
    allocator_.reset();
    row_store_type_ = common::ENCODING_ROW_STORE;
    schema_version_ = 0;
    snapshot_version_ = 0;
    task_id_ = 0;
    execution_id_ = -1;
    tablet_task_id_ = 0;
    compat_mode_ = lib::Worker::CompatMode::INVALID;
    data_format_version_ = 0;
  }
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_id), K_(table_id), K_(tablet_id),  
      K_(tablet_task_id), K_(schema_version), K_(snapshot_version), K_(task_id), 
      K_(execution_id), K_(compat_mode), K_(data_format_version));
public:
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t schema_id_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObTabletID dest_tablet_id_;
  common::ObRowStoreType row_store_type_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t task_id_;
  int64_t execution_id_;
  int64_t tablet_task_id_;
  int delete_lob_meta_ret_;
  lib::Worker::CompatMode compat_mode_;
  uint64_t data_format_version_;
  common::ObArenaAllocator allocator_;
};

class ObDeleteLobMetaRowDag final: public share::ObIDag
{
public:
  ObDeleteLobMetaRowDag();
  ~ObDeleteLobMetaRowDag();
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  int64_t hash() const override;
  bool operator==(const ObIDag& other) const override;
  bool is_inited() const { return is_inited_; }
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  int report_replica_build_status();
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return param_.compat_mode_; }
  void handle_init_failed_ret_code(int ret) { param_.delete_lob_meta_ret_ = ret; }
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const { return false; }
  virtual int create_first_task() override;
  virtual bool ignore_warning() override;
private:
  bool is_inited_;
  ObDeleteLobMetaRowParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObDeleteLobMetaRowDag);
};

class ObDeleteLobMetaRowTask : public share::ObITask
{
public:
  ObDeleteLobMetaRowTask();
  ~ObDeleteLobMetaRowTask();
  int init(ObDeleteLobMetaRowParam &param);
  virtual int process() override;
  int init_scan_param(ObTableScanParam& scan_param);

private:
  bool is_inited_;
  ObDeleteLobMetaRowParam *param_;
  ObCollationType collation_type_;
  DISALLOW_COPY_AND_ASSIGN(ObDeleteLobMetaRowTask);
};


} // end namespace table
} // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_DELETE_LOB_META_ROW_TASK_H
