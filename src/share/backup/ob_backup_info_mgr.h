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

#ifndef SRC_SHARE_BACKUP_OB_BACKUP_INFO_MGR_H_
#define SRC_SHARE_BACKUP_OB_BACKUP_INFO_MGR_H_

#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace share {
class ObRsMgr;
class ObBackupDestDetector;
class ObLogArchiveInfoMgr final {
public:
  struct ObLogArchiveSimpleInfo final {
    int64_t update_ts_;
    int64_t checkpoint_ts_;
    int64_t start_ts_;
    share::ObLogArchiveStatus::STATUS status_;
    uint64_t tenant_id_;

    ObLogArchiveSimpleInfo();
    void reset();
    bool is_valid() const;
    TO_STRING_KV(K_(update_ts), K_(checkpoint_ts), K_(start_ts), K_(status), K_(tenant_id));
  };
  static ObLogArchiveInfoMgr& get_instance();

  int init(common::ObMySQLProxy& sql_proxy);
  int get_log_archive_status(const uint64_t tenant_id, const int64_t need_ts, ObLogArchiveSimpleInfo& status);
  TO_STRING_KV(K_(update_count));

private:
  ObLogArchiveInfoMgr();
  ~ObLogArchiveInfoMgr();
  int get_log_archive_status_(const uint64_t tenant_id, ObLogArchiveSimpleInfo& status);
  int try_retire_status_();
  int renew_log_archive_status_(const uint64_t tenant_id, ObLogArchiveSimpleInfo& status);

private:
  typedef common::hash::ObHashMap<uint64_t, ObLogArchiveSimpleInfo, common::hash::NoPthreadDefendMode> STATUS_MAP;
  bool is_inited_;
  common::ObMySQLProxy* sql_proxy_;
  common::SpinRWLock lock_;
  lib::ObMutex mutex_;
  STATUS_MAP status_map_;
  int64_t update_count_;
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveInfoMgr);
};

class ObBackupInfoMgr final {
public:
  typedef common::ObArray<ObPhysicalRestoreJob> RestoreJobArray;
  static ObBackupInfoMgr& get_instance();

  int init(common::ObMySQLProxy& sql_proxy, ObBackupDestDetector& backup_dest_detector);
  int start();
  void stop();
  void wait();
  void destroy();
  int get_log_archive_backup_info(ObLogArchiveBackupInfo& info);
  int get_backup_snapshot_version(int64_t& snapshot_version);
  int get_log_archive_checkpoint(int64_t& snapshot_version);
  int record_drop_tenant_log_archive_history(const uint64_t tenant_id);
  int get_delay_delete_schema_version(const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService& schema_service, bool& is_backup, int64_t& reserved_schema_version);
  int check_if_doing_backup(bool& is_doing);
  int get_restore_info(const uint64_t tenant_id, ObPhysicalRestoreInfo& info);
  int get_restore_status(const uint64_t tenant_id, PhysicalRestoreStatus& status);
  int get_restore_job_id(const uint64_t tenant_id, int64_t& job_id);
  int reload();
  int is_base_backup_start(bool& is_started);
  int get_base_data_restore_schema_version(const uint64_t tenant_id, int64_t& schema_version);

private:
  static const int64_t DEFAULT_UPDATE_INTERVAL_US = 10 * 1000 * 1000;  // 10s
  int get_restore_info_from_cache(const uint64_t tenant_id, ObSimplePhysicalRestoreJob& simple_job_info);
  int get_restore_status_from_cache(const uint64_t tenant_id, PhysicalRestoreStatus& status);
  int check_backup_dest_(ObLogArchiveBackupInfo& backup_info);

private:
  ObBackupInfoMgr();
  ~ObBackupInfoMgr();

  class ObBackupInfoUpdateTask : public common::ObTimerTask {
  public:
    ObBackupInfoUpdateTask()
    {}
    virtual ~ObBackupInfoUpdateTask()
    {}
    virtual void runTimerTask() override;
  };

private:
  bool is_inited_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObTimer timer_;
  ObBackupInfoUpdateTask update_task_;
  ObLogArchiveBackupInfo backup_infos_[2];
  ObLogArchiveBackupInfo* cur_backup_info_;
  RestoreJobArray restore_jobs_[2];
  RestoreJobArray* cur_restore_job_;
  bool is_base_backup_started_[2];
  bool* cur_base_backup_started_;
  common::SpinRWLock lock_;
  lib::ObMutex mutex_;
  bool is_loaded_;
  ObBackupDestDetector* backup_dest_detector_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupInfoMgr);
};

class ObRestoreBackupInfoUtil final {
public:
  struct GetRestoreBackupInfoParam final {
    GetRestoreBackupInfoParam();
    const char* backup_dest_;
    const char* backup_cluster_name_;
    int64_t cluster_id_;
    int64_t incarnation_;
    const char* backup_tenant_name_;
    int64_t restore_timestamp_;
    const char* passwd_array_;
  };
  static int get_restore_backup_info(const GetRestoreBackupInfoParam& param, ObRestoreBackupInfo& info);

  static int get_restore_sys_table_ids(
      const ObPhysicalRestoreInfo& info, common::ObIArray<common::ObPartitionKey>& pkey_list);
};

class ObRestoreFatalErrorReporter : public share::ObThreadPool {
public:
  static ObRestoreFatalErrorReporter& get_instance();

  int init(obrpc::ObCommonRpcProxy& rpc_proxy, share::ObRsMgr& rs_mgr);
  int add_restore_error_task(const uint64_t tenant_id, const PhysicalRestoreMod& mod, const int32_t result,
      const int64_t job_id, const common::ObAddr& addr);
  virtual int start() override;
  virtual void stop() override;
  virtual void wait() override;

private:
  ObRestoreFatalErrorReporter();
  virtual ~ObRestoreFatalErrorReporter();
  virtual void run1() override;
  int report_restore_errors();
  int report_restore_error_(const obrpc::ObPhysicalRestoreResult& result);
  int remove_restore_error_task_(const obrpc::ObPhysicalRestoreResult& result);

private:
  bool is_inited_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  share::ObRsMgr* rs_mgr_;
  lib::ObMutex mutex_;
  common::ObSEArray<obrpc::ObPhysicalRestoreResult, 16> report_results_;  // assume most time less than 16 restore task
  DISALLOW_COPY_AND_ASSIGN(ObRestoreFatalErrorReporter);
};

class ObBackupDestDetector : public share::ObThreadPool {
public:
  static ObBackupDestDetector& get_instance();
  int init();
  virtual int start() override;
  virtual void stop() override;
  virtual void wait() override;
  void wakeup();
  int get_is_backup_dest_bad(const int64_t round_id, bool& is_bad);
  int update_backup_info(const ObLogArchiveBackupInfo& info);

private:
  ObBackupDestDetector();
  virtual ~ObBackupDestDetector();
  virtual void run1() override;
  int check_backup_dest();
  int check_backup_dest_(ObLogArchiveBackupInfo& backup_info, bool& is_bad);
  void idle();

private:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObLogArchiveBackupInfo info_;
  bool is_bad_;
  common::ObThreadCond cond_;
  int64_t wakeup_count_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDestDetector);
};

}  // namespace share
}  // namespace oceanbase
#endif /* SRC_SHARE_BACKUP_OB_BACKUP_INFO_MGR_H_ */
