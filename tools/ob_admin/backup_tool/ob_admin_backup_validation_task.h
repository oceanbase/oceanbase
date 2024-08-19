/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OB_ADMIN_BACKUP_VALIDATION_TASK_H_
#define OB_ADMIN_BACKUP_VALIDATION_TASK_H_
#include "../ob_admin_executor.h"
#include "logservice/archiveservice/large_buffer_pool.h"
#include "logservice/ob_log_external_storage_handler.h"
#include "ob_admin_backup_validation_ctx.h"

namespace oceanbase
{
namespace tools
{

// Dagnet
struct ObAdminDataBackupValidationDagInitParam final : public ObIDagInitParam
{
  ObAdminDataBackupValidationDagInitParam() {}
  ObAdminDataBackupValidationDagInitParam(ObAdminBackupValidationCtx *ctx) : ctx_(ctx) {}
  ~ObAdminDataBackupValidationDagInitParam() {}
  virtual bool is_valid() const override { return true; }
  ObAdminBackupValidationCtx *ctx_;
};
class ObAdminDataBackupValidationDagNet final : public share::ObIDagNet
{
public:
  ObAdminDataBackupValidationDagNet()
      : share::ObIDagNet(ObDagNetType::DAG_NET_DATA_BACKUP_VALIDATION), is_inited_(false), id_(0),
        ctx_(nullptr)
  {
  }
  ~ObAdminDataBackupValidationDagNet();
  int init(ObAdminBackupValidationCtx *ctx);
  int init_by_param(const ObIDagInitParam *param);
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual bool is_ha_dag_net() const override { return false; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminDataBackupValidationDagNet);
};
struct ObAdminLogArchiveValidationDagInitParam final : public ObIDagInitParam
{
  ObAdminLogArchiveValidationDagInitParam() {}
  ObAdminLogArchiveValidationDagInitParam(ObAdminBackupValidationCtx *ctx) : ctx_(ctx) {}
  ~ObAdminLogArchiveValidationDagInitParam() {}
  virtual bool is_valid() const override { return true; }
  ObAdminBackupValidationCtx *ctx_;
};
class ObAdminLogArchiveValidationDagNet final : public share::ObIDagNet
{
public:
  ObAdminLogArchiveValidationDagNet()
      : share::ObIDagNet(ObDagNetType::DAG_NET_LOG_ARCHIVE_VALIDATION), is_inited_(false), id_(0),
        ctx_(nullptr)
  {
  }
  ~ObAdminLogArchiveValidationDagNet();
  int init(ObAdminBackupValidationCtx *ctx);
  int init_by_param(const ObIDagInitParam *param);
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual bool is_ha_dag_net() const override { return false; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminLogArchiveValidationDagNet);
};

// Dag

class ObAdminPrepareDataBackupValidationDag final : public share::ObIDag
{
public:
  ObAdminPrepareDataBackupValidationDag()
      : share::ObIDag(ObDagType::DAG_TYPE_PREPARE_DATA_BACKUP_VALIDATION), is_inited_(false),
        id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminPrepareDataBackupValidationDag();
  int init(int64_t id, ObAdminBackupValidationCtx *ctx);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminPrepareDataBackupValidationDag);
};

class ObAdminBackupSetMetaValidationDag final : public share::ObIDag
{
public:
  ObAdminBackupSetMetaValidationDag()
      : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_SET_META_VALIDATION), is_inited_(false), id_(0),
        ctx_(nullptr)
  {
  }
  ~ObAdminBackupSetMetaValidationDag();
  int init(int64_t id, ObAdminBackupValidationCtx *ctx);
  virtual int create_first_task() override;
  virtual int generate_next_dag(ObIDag *&next_dag) override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupSetMetaValidationDag);
};

class ObAdminBackupTabletValidationDag final : public share::ObIDag
{
public:
  ObAdminBackupTabletValidationDag()
      : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_TABLET_VALIDATION), is_inited_(false), id_(0),
        ctx_(nullptr), generate_sibling_dag_(true), lock_(ObLatchIds::BACKUP_LOCK),
        time_identifier_(common::ObTimeUtility::fast_current_time()), scheduled_tablet_cnt_(0),
        stat_()
  {
  }
  ~ObAdminBackupTabletValidationDag();
  int init(int64_t id, ObAdminBackupValidationCtx *ctx, bool generate_sibling_dag = true);
  virtual int create_first_task() override;
  virtual int generate_next_dag(ObIDag *&next_dag) override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  bool generate_sibling_dag_;
  ObSpinLock lock_;
  int64_t time_identifier_; // only used for hash

public:
  // tablet_group ctx, only under dag sapce, same life span with dag
  int add_macro_block(const backup::ObBackupMacroBlockIDPair &macro_block_id_pair,
                      const share::ObBackupDataType &data_type);
  ObArray<ObArray<ObAdminBackupTabletValidationAttr *>> processing_tablet_group_;
  int64_t scheduled_tablet_cnt_;
  ObArray<ObArray<std::pair<backup::ObBackupMacroBlockIDPair, share::ObBackupDataType>>>
      processing_macro_block_array_;
  ObAdminBackupValidationStat stat_;

  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupTabletValidationDag);
};

class ObAdminFinishDataBackupValidationDag final : public share::ObIDag
{
public:
  ObAdminFinishDataBackupValidationDag()
      : share::ObIDag(ObDagType::DAG_TYPE_FINISH_DATA_BACKUP_VALIDATION), is_inited_(false), id_(0),
        ctx_(nullptr)
  {
  }
  ~ObAdminFinishDataBackupValidationDag();
  int init(int64_t id, ObAdminBackupValidationCtx *ctx);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminFinishDataBackupValidationDag);
};

class ObAdminPrepareLogArchiveValidationDag final : public share::ObIDag
{
public:
  ObAdminPrepareLogArchiveValidationDag()
      : share::ObIDag(ObDagType::DAG_TYPE_PREPARE_LOG_ARCHIVE_VALIDATION), is_inited_(false),
        id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminPrepareLogArchiveValidationDag();
  int init(int64_t id, ObAdminBackupValidationCtx *ctx);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminPrepareLogArchiveValidationDag);
};

class ObAdminBackupPieceValidationDag final : public share::ObIDag
{
public:
  ObAdminBackupPieceValidationDag()
      : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_PIECE_VALIDATION), is_inited_(false), id_(0),
        ctx_(nullptr)
  {
  }
  ~ObAdminBackupPieceValidationDag();
  int init(int64_t id, ObAdminBackupValidationCtx *ctx);
  virtual int create_first_task() override;
  virtual int generate_next_dag(ObIDag *&next_dag) override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  int64_t get_id() const { return id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupPieceValidationDag);

public:
  ObArray<std::pair<share::ObLSID, std::pair<palf::LSN, palf::LSN>>> processing_lsn_range_array_;
  logservice::ObLogExternalStorageHandler storage_handler_;
  archive::LargeBufferPool large_buffer_pool_;
};

class ObAdminFinishLogArchiveValidationDag final : public share::ObIDag
{
public:
  ObAdminFinishLogArchiveValidationDag()
      : share::ObIDag(ObDagType::DAG_TYPE_FINISH_LOG_ARCHIVE_VALIDATION), is_inited_(false), id_(0),
        ctx_(nullptr)
  {
  }
  ~ObAdminFinishLogArchiveValidationDag();
  int init(int64_t id, ObAdminBackupValidationCtx *ctx);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  int64_t id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminFinishLogArchiveValidationDag);
};

// Task

class ObAdminPrepareDataBackupValidationTask final : public share::ObITask
{
public:
  ObAdminPrepareDataBackupValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminPrepareDataBackupValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  int check_format_file_();
  int collect_backup_set_();
  int retrieve_backup_set_();
  void post_process_(int ret);

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminPrepareDataBackupValidationTask);
};

class ObAdminBackupSetMetaValidationTask final : public share::ObITask
{
public:
  ObAdminBackupSetMetaValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), sys_turn_id(1),
        ctx_(nullptr)
  {
  }

  ~ObAdminBackupSetMetaValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  int check_backup_set_info_();
  int check_locality_file_();

  int collect_tenant_ls_meta_info_();

  int collect_inner_tablet_meta_index_();
  // int check_inner_tablet_macro_block_range_index_();
  int inner_assign_meta_index_to_tablet_attr_(int64_t backup_set_id, const share::ObLSID &ls_id,
                                              const backup::ObBackupMetaIndex &meta_index,
                                              const share::ObBackupDataType &data_type);
  int check_inner_tablet_meta_index_();

  int collect_consistent_scn_tablet_id_();
  // int collect_minor_tablet_macro_block_range_index_();
  int collect_minor_tablet_meta_index_();
  int collect_major_tablet_id_();
  // int collect_major_tablet_macro_block_range_index_();
  int collect_major_tablet_meta_index_();
  int inner_check_tablet_meta_index_(const ObArray<backup::ObBackupMetaIndex> &meta_index_list,
                                     const ObArray<backup::ObBackupMetaIndex> &sec_meta_index_list);
  int check_tenant_tablet_meta_index(); // cross check between minor and major
  void post_process_(int ret);

private:
  bool is_inited_;
  int64_t task_id_;
  const int64_t sys_turn_id;
  ObBackupDataType backup_sys_data_type_;
  ObBackupDataType backup_minor_data_type_;
  ObBackupDataType backup_major_data_type_;
  common::ObArray<common::ObTabletID> inner_tablet_id_array_; // global share and global same
  ObAdminBackupValidationCtx *ctx_;

  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupSetMetaValidationTask);
};

class ObAdminPrepareTabletValidationTask final : public share::ObITask
{
public:
  ObAdminPrepareTabletValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminPrepareTabletValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  int collect_tablet_group_();
  int schedule_next_dag_();

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminPrepareTabletValidationTask);
};

class ObAdminTabletMetaValidationTask final : public share::ObITask
{
public:
  ObAdminTabletMetaValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr),
        local_allocator_("ObAdmBakVal")
  {
  }
  ~ObAdminTabletMetaValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  virtual int generate_next_task(ObITask *&next_task) override;
  int collect_tablet_meta_info_();
  int collect_sstable_meta_info_();
  int check_sstable_meta_info_and_prepare_macro_block_();

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;

private:
  // mid structure, same life span with this task
  common::ObArray<backup::ObBackupTabletMeta *> tablet_meta_;
  common::ObArray<common::ObArray<backup::ObBackupSSTableMeta>> sstable_metas_;
  common::ObArray<backup::ObBackupMacroBlockIDMappingsMeta *> id_mappings_meta_;
  common::ObArenaAllocator local_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminTabletMetaValidationTask);
};

class ObAdminMacroBlockDataValidationTask final : public share::ObITask
{
public:
  ObAdminMacroBlockDataValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminMacroBlockDataValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  virtual int generate_next_task(ObITask *&next_task) override;

  int check_macro_block_data_();

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminMacroBlockDataValidationTask);
};

class ObAdminFinishTabletValidationTask final : public share::ObITask
{
public:
  ObAdminFinishTabletValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminFinishTabletValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  int check_and_update_stat_();

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminFinishTabletValidationTask);
};

class ObAdminPrepareLogArchiveValidationTask final : public share::ObITask
{
public:
  ObAdminPrepareLogArchiveValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminPrepareLogArchiveValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  int check_format_file_();
  int collect_backup_piece_();
  int retrieve_backup_piece_();
  void post_process_(int ret);

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminPrepareLogArchiveValidationTask);
};

class ObAdminBackupPieceMetaValidationTask final : public share::ObITask
{
public:
  ObAdminBackupPieceMetaValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }

  ~ObAdminBackupPieceMetaValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  int check_backup_piece_info_();
  int collect_and_check_piece_ls_info_();
  int inner_collect_active_piece_ls_info_(const share::ObPieceKey &backup_piece_key,
                                          const ObAdminBackupPieceValidationAttr &backup_piece_attr);
  int collect_and_check_piece_ls_onefile_length_();
  void post_process_(int ret);

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;

  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupPieceMetaValidationTask);
};

class ObAdminBackupPieceLogIterationTask final : public share::ObITask
{
public:
  ObAdminBackupPieceLogIterationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminBackupPieceLogIterationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  virtual int generate_next_task(ObITask *&next_task) override;
  int iterate_log_();

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminBackupPieceLogIterationTask);
};

class ObAdminFinishLogArchiveValidationTask final : public share::ObITask
{
public:
  ObAdminFinishLogArchiveValidationTask()
      : share::ObITask(TASK_TYPE_VALIDATE_BACKUP), is_inited_(false), task_id_(0), ctx_(nullptr)
  {
  }
  ~ObAdminFinishLogArchiveValidationTask();
  int init(int64_t task_id, ObAdminBackupValidationCtx *ctx);
  virtual int process() override;

private:
  int cross_check_scn_continuity_();
  int inner_get_backup_set_scn_range_(
      common::ObArray<std::pair<share::SCN, share::SCN>> &scn_range);
  int inner_get_backup_piece_scn_range_(
      common::ObArray<std::pair<share::SCN, share::SCN>> &scn_range);

private:
  bool is_inited_;
  int64_t task_id_;
  ObAdminBackupValidationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminFinishLogArchiveValidationTask);
};
}; // namespace tools
}; // namespace oceanbase
#endif