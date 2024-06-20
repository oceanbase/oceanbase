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

#ifndef OCEANBASE_SHARE_OB_RESTORE_PERSIST_HELPER_H_
#define OCEANBASE_SHARE_OB_RESTORE_PERSIST_HELPER_H_

#include "common/ob_role.h"
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/ob_delegate.h"
#include "share/ob_inner_table_operator.h"
#include "share/restore/ob_ls_restore_status.h"
#include "share/restore/ob_restore_type.h"
#include "share/scn.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace share
{
struct ObPhysicalRestoreJob;
// Define restore job key.
struct ObRestoreJobPersistKey final : public ObIInnerTableKey
{
  uint64_t tenant_id_; // user tenant id
  int64_t job_id_; // valid job id must be bigger than 0.

  ObRestoreJobPersistKey() 
  {
    reset();
  }

  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    job_id_ = -1; 
  }
  // Return if primary key valid.
  bool is_pkey_valid() const override;

  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
  bool operator==(const ObRestoreJobPersistKey &other) const
  {
    return job_id_ == other.job_id_ && tenant_id_ == other.tenant_id_;
  }
  bool operator!=(const ObRestoreJobPersistKey &other) const
  {
    return job_id_ != other.job_id_ || tenant_id_ != other.tenant_id_;
  }



  TO_STRING_KV(K_(tenant_id), K_(job_id));
};

// Define restore job key.
struct ObInitiatorRestoreJobPersistKey final : public ObIInnerTableKey
{
  uint64_t initiator_tenant_id_; // user tenant id
  int64_t initiator_job_id_; // valid job id must be bigger than 0.

  ObInitiatorRestoreJobPersistKey() 
  {
    initiator_tenant_id_ = OB_INVALID_TENANT_ID;
    initiator_job_id_ = -1;
  }

  // Return if primary key valid.
  bool is_pkey_valid() const override;

  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;

  TO_STRING_KV(K_(initiator_tenant_id), K_(initiator_job_id));
};

// Define restore progress table row structure.
struct ObRestoreProgressPersistInfo final : public ObIInnerTableRow
{
  ObRestoreJobPersistKey key_;
  SCN restore_scn_;
  int64_t ls_count_; // to restore log stream replica number.
  int64_t finish_ls_count_;
  int64_t tablet_count_;
  int64_t finish_tablet_count_;
  int64_t total_bytes_;
  int64_t finish_bytes_;

  ObRestoreProgressPersistInfo() {
    restore_scn_ = share::SCN::min_scn();
    ls_count_ = 0;
    finish_ls_count_ = 0;
    tablet_count_ = 0;
    finish_tablet_count_ = 0;
    total_bytes_ = 0;
    finish_bytes_ = 0;
  }

  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }

  // Return if both primary key and value are valid.
  bool is_valid() const override;
  
  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;

  int init_initial_progress_info(const uint64_t tenant_id,
                                 const int64_t job_id,
                                 const uint64_t restore_scn,
                                 const int64_t ls_count);

  TO_STRING_KV(K_(key), K_(restore_scn), K_(ls_count), K_(finish_ls_count), 
    K_(tablet_count), K_(finish_tablet_count), K_(total_bytes), 
    K_(finish_bytes));
};


// Define log stream restore job key.
struct ObLSRestoreJobPersistKey final : public ObIInnerTableKey
{
  uint64_t tenant_id_; // user tenant id
  int64_t job_id_; // valid job id must be bigger than 0.
  ObLSID ls_id_;
  ObAddr addr_;

  ObLSRestoreJobPersistKey() 
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    job_id_ = -1;
  }

  ObRestoreJobPersistKey generate_restore_job_key() const;

  // Return if primary key valid.
  bool is_pkey_valid() const override;

  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(ls_id), K_(addr));
};


// Define log stream restore history table row structure.
struct ObLSHisRestorePersistInfo final : public ObIInnerTableRow
{
  ObLSRestoreJobPersistKey key_;
  SCN restore_scn_;
  SCN start_replay_scn_;
  SCN last_replay_scn_;
  int64_t tablet_count_;
  int64_t finish_tablet_count_;
  int64_t total_bytes_;
  int64_t finish_bytes_;
  int result_;
  ObTaskId trace_id_;
  common::ObSqlString comment_;

  ObLSHisRestorePersistInfo() {
    restore_scn_ = share::SCN::min_scn();
    start_replay_scn_ = share::SCN::min_scn();
    last_replay_scn_ = share::SCN::min_scn();
    tablet_count_ = 0;
    finish_tablet_count_ = 0;
    total_bytes_ = 0;
    finish_bytes_ = 0;
    result_ = OB_SUCCESS;
  }

  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }

  // Return if both primary key and value are valid.
  bool is_valid() const override;
  
  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;

  TO_STRING_KV(K_(key), K_(restore_scn), K_(start_replay_scn), 
    K_(last_replay_scn), K_(tablet_count), K_(finish_tablet_count),
    K_(total_bytes), K_(finish_bytes), K_(result), K_(trace_id));
};


// Define log stream restore progress table row structure.
struct ObLSRestoreProgressPersistInfo final : public ObIInnerTableRow
{
  ObLSRestoreJobPersistKey key_;
  ObLSRestoreStatus status_;
  SCN restore_scn_;
  SCN start_replay_scn_;
  SCN last_replay_scn_;
  int64_t tablet_count_;
  int64_t finish_tablet_count_;
  int64_t total_bytes_;
  int64_t finish_bytes_;
  ObTaskId trace_id_;
  ObHAResultInfo::Comment comment_;
  int result_;

  ObLSRestoreProgressPersistInfo() {
    restore_scn_ = share::SCN::min_scn();
    start_replay_scn_ = share::SCN::min_scn();
    last_replay_scn_ = share::SCN::min_scn();
    tablet_count_ = 0;
    finish_tablet_count_ = 0;
    total_bytes_ = 0;
    finish_bytes_ = 0;
    result_ = OB_SUCCESS;
  }
  int assign(const ObLSRestoreProgressPersistInfo &that);

  int generate_his_progress(ObLSHisRestorePersistInfo &his) const;
  
  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }

  // Return if both primary key and value are valid.
  bool is_valid() const override;
  
  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;

  TO_STRING_KV(K_(key), K_(status), K_(restore_scn),
    K_(start_replay_scn), K_(last_replay_scn), K_(tablet_count), K_(finish_tablet_count),
    K_(total_bytes), K_(finish_bytes), K_(trace_id),
    K_(result), K_(comment));
};


// Define history restore job table row structure.
struct ObHisRestoreJobPersistInfo final : public ObIInnerTableRow
{
  typedef common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH_STORE> TenantName;
  typedef common::ObFixedLengthString<common::OB_MAX_CLUSTER_NAME_LENGTH> ClusterName;
  typedef common::ObSqlString LongString;
  
  ObRestoreJobPersistKey key_;
  int64_t initiator_job_id_;
  uint64_t initiator_tenant_id_;
  ObRestoreType restore_type_;
  TenantName restore_tenant_name_;
  uint64_t restore_tenant_id_;
  TenantName backup_tenant_name_;
  uint64_t backup_tenant_id_;
  ClusterName backup_cluster_name_;
  LongString backup_dest_;
  SCN restore_scn_;
  LongString restore_option_;
  LongString table_list_; // white table list
  LongString remap_table_list_; // renamed table list
  LongString database_list_; // white database list
  LongString remap_database_list_; // renamed db list
  LongString backup_piece_list_;
  LongString backup_set_list_;

  int64_t backup_cluster_version_;

  int64_t ls_count_;
  int64_t start_time_; // job handle time
  int64_t finish_time_; // job finish time
  int64_t finish_ls_count_;
  int64_t tablet_count_;
  int64_t finish_tablet_count_;
  int64_t total_bytes_;
  int64_t finish_bytes_;

  int status_;
  LongString description_;
  ObHAResultInfo::Comment comment_;

  ObHisRestoreJobPersistInfo() : key_() {
    initiator_job_id_ = -1;
    initiator_tenant_id_ = OB_INVALID_TENANT_ID;
    start_time_ = 0;
    finish_time_ = 0;

    restore_tenant_id_ = OB_INVALID_TENANT_ID;
    backup_tenant_id_ = OB_INVALID_TENANT_ID;
    
    restore_scn_ = share::SCN::min_scn();
    backup_cluster_version_ = 0;

    ls_count_ = 0;
    finish_ls_count_ = 0;
    tablet_count_ = 0;
    finish_tablet_count_ = 0;
    total_bytes_ = 0;
    finish_bytes_ = 0;
    status_ = SUCCESS;
  }

  void set_success() 
  {
    status_ = SUCCESS; 
  }

  void set_fail() 
  { 
    status_ = FAIL; 
  }

  int32_t get_status() const 
  { 
    return status_; 
  }

  bool is_restore_success() const
  {
    return status_ == SUCCESS;
  }
  const char *get_status_str() const;

  int get_status(const ObString &str_str) const;
  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }
  int init_with_job_process(const share::ObPhysicalRestoreJob &job,
                            const ObRestoreProgressPersistInfo &progress);
  int init_with_job(const share::ObPhysicalRestoreJob &job);
  int init_initiator_job_history(const share::ObPhysicalRestoreJob &job,
                                 const ObHisRestoreJobPersistInfo &job_history);

  // Return if both primary key and value are valid.
  bool is_valid() const override;
  
  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;

  TO_STRING_KV(K_(key), K_(start_time), K_(finish_time), K_(restore_type), 
    K_(restore_tenant_name), K_(restore_tenant_id), K_(backup_tenant_id), K_(backup_dest),
    K_(restore_scn), K_(restore_option), K_(table_list), K_(remap_table_list), 
    K_(database_list), K_(remap_database_list), K_(backup_piece_list), K_(backup_set_list), K_(backup_cluster_version),
    K_(ls_count), K_(finish_ls_count), K_(tablet_count), K_(finish_tablet_count), K_(total_bytes), 
    K_(finish_bytes), K_(status), K_(description), K_(comment), K_(initiator_job_id), K_(initiator_tenant_id));

private:
  static const int32_t SUCCESS = 0;
  static const int32_t FAIL = 1;
};



class ObRestorePersistHelper final : public ObIExecTenantIdProvider
{
public:
  ObRestorePersistHelper();
  virtual ~ObRestorePersistHelper() {}

  uint64_t get_exec_tenant_id() const override;

  int init(const uint64_t tenant_id, const int32_t group_id);

  int insert_initial_restore_progress(
      common::ObISQLClient &proxy, const ObRestoreProgressPersistInfo &persist_info) const;
  //__all_restore_process
  int get_restore_process(
      common::ObISQLClient &proxy,
      const ObRestoreJobPersistKey &ls_key,
      ObRestoreProgressPersistInfo &persist_info) const;

  //__all_restore_job_history
  int insert_restore_job_history(
       common::ObISQLClient &proxy, const ObHisRestoreJobPersistInfo &persist_info) const;
  int get_restore_job_history(
      common::ObISQLClient &proxy,
      const int64_t initiator_job,
      const uint64_t initiator_tenant_id,
      ObHisRestoreJobPersistInfo &persist_info) const;

  int insert_initial_ls_restore_progress(
      common::ObISQLClient &proxy, const ObLSRestoreProgressPersistInfo &persist_info) const;

  int record_ls_his_restore_progress(
    common::ObISQLClient &proxy, const ObLSHisRestorePersistInfo &persist_info) const;

  int inc_need_restore_ls_count_by_one(
      common::ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key, int result) const;
      
  // One log stream restore finish.
  int inc_finished_ls_count_by_one(
      common::ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key) const;

  // One tablet restore finish. New info will be updated to both
  // restore progress table and log stream restore progress table.
  int inc_finished_tablet_count_by_one(
      common::ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key) const;

  // Some more major block bytes restore finished. New info will be updated to both
  // restore progress table and log stream restore progress table.
  int inc_finished_restored_block_bytes(
      common::ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key, 
      const int64_t inc_finished_bytes) const;

  // Update log restore progress will be updated to log stream restore progress table.
  int update_log_restore_progress(
    common::ObISQLClient &proxy, const ObLSRestoreJobPersistKey &ls_key,
    const SCN &last_replay_scn) const;

  int update_ls_restore_status(
      common::ObISQLClient &proxy, const ObLSRestoreJobPersistKey &ls_key,
      const share::ObTaskId &trace_id, const share::ObLSRestoreStatus &status, 
      const int result, const char *comment) const;
  
  int get_all_ls_restore_progress(common::ObISQLClient &proxy, 
      ObIArray<ObLSRestoreProgressPersistInfo> &ls_restore_progress_info);

  TO_STRING_KV(K_(is_inited), K_(tenant_id));

private:
  int do_parse_ls_restore_progress_result_(sqlclient::ObMySQLResult &result, 
      ObIArray<ObLSRestoreProgressPersistInfo> &ls_restore_progress_info);

private:
  DISALLOW_COPY_AND_ASSIGN(ObRestorePersistHelper);

  bool is_inited_;
  uint64_t tenant_id_; // sys or user tenant id
  int32_t group_id_;
};


}
}

#endif
