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

#ifndef OCEANBASE_SHARE_OB_BACKUP_DATA_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_BACKUP_DATA_TABLE_OPERATOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"


namespace oceanbase
{
namespace share
{

class ObBackupBaseTableOperator
{
protected:
  static uint64_t get_exec_tenant_id(uint64_t tenant_id);
};
  
class ObBackupTabletToLSOperator 
{
public:
  static int get_ls_and_tablet(common::ObISQLClient &proxy, const uint64_t tenant_id, const share::SCN &snapshot,
      common::hash::ObHashMap<ObLSID, ObArray<ObTabletID>> &tablet_to_ls);
  static int get_ls_of_tablet(common::ObISQLClient &proxy, const uint64_t tenant_id, const ObTabletID &tablet_id, ObLSID &ls_id, int64_t &transfer_seq);
private:
  static int group_tablets_by_ls_(const ObIArray<share::ObTabletLSPair> &tablet_ls_pairs,
      common::hash::ObHashMap<ObLSID, ObArray<ObTabletID>> &tablet_to_ls, ObTabletID &max_tablet_id);
  static int range_get_tablet_(common::ObISQLClient &sql_proxy, const uint64_t tenant_id, const share::SCN &snapshot,
      const oceanbase::common::ObTabletID &start_tablet_id, const int64_t range_size, common::ObIArray<ObTabletLSPair> &tablet_ls_pairs);
};

class ObBackupSkippedTabletOperator : public ObBackupBaseTableOperator
{
public:
  static int batch_move_skip_tablet(common::ObMySQLProxy &proxy, const uint64_t tenant_id, const int64_t task_id);
  static int get_skip_tablet(common::ObISQLClient &proxy, const bool need_lock, const uint64_t tenant_id, 
                             const int64_t task_id, const share::ObBackupSkippedType skipped_type,
                             common::hash::ObHashSet<ObBackupSkipTabletAttr> &skip_tablets);
  static int move_skip_tablet_to_his(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id);
private:  
  static int fill_select_skip_tablet_sql_(ObSqlString &sql);
  static int parse_skip_tablet_result_(sqlclient::ObMySQLResult &result, common::hash::ObHashSet<ObBackupSkipTabletAttr> &skip_tablets);
  static int do_parse_skip_tablet_result_(sqlclient::ObMySQLResult &result, ObBackupSkipTabletAttr &backup_set_desc);
};

class ObBackupSetFileOperator : public ObBackupBaseTableOperator
{
public:
  static int insert_backup_set_file(common::ObISQLClient &proxy, const ObBackupSetFileDesc &backup_set_desc);
  static int get_backup_set_files(common::ObISQLClient &proxy, const uint64_t tenant_id, ObIArray<ObBackupSetFileDesc> &tenant_backup_set_infos);
  static int get_backup_set_files_specified_dest(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t dest_id, ObIArray<ObBackupSetFileDesc> &backup_set_infos);
  static int get_backup_set_file(common::ObISQLClient &proxy, bool need_lock, const int64_t backup_set_id_, const int64_t incarnation, 
      const uint64_t teannt_id, const int64_t dest_id, ObBackupSetFileDesc &backup_set_desc);
  static int get_one_backup_set_file(common::ObISQLClient &proxy, bool need_lock, const int64_t backup_set_id_, const int64_t incarnation, 
      const uint64_t tenant_id, ObBackupSetFileDesc &backup_set_desc);
  static int update_backup_set_file(common::ObISQLClient &proxy, const ObBackupSetFileDesc &backup_set_desc);
  static int get_candidate_obsolete_backup_sets(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t expired_time,
      const char *backup_path_str,
      common::ObIArray<ObBackupSetFileDesc> &backup_set_descs);
  static int get_all_backup_set_between(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t incarnation, 
      const int64_t min_backup_set_id,
      const int64_t max_backup_set_id,
      common::ObIArray<ObBackupSetFileDesc> &backup_set_descs);
  static int update_backup_set_file_status(
      common::ObISQLClient &proxy,
      const ObBackupSetFileDesc &backup_set_desc);
      
  static int get_prev_backup_set_id(common::ObISQLClient &proxy, const uint64_t &tenant_id, const int64_t &backup_set_id, 
      const ObBackupType &backup_type, const ObBackupPathString &backup_path, int64_t &prev_full_backup_set_id, 
      int64_t &prev_inc_backup_set_id);
private:
  static int fill_dml_with_backup_set_(const ObBackupSetFileDesc &backup_set_desc, ObDMLSqlSplicer &dml);
  static int parse_backup_set_info_result_(sqlclient::ObMySQLResult &result, 
      ObIArray<ObBackupSetFileDesc> &backup_set_infos);
  static int do_parse_backup_set_(sqlclient::ObMySQLResult &result, ObBackupSetFileDesc &backup_set_desc);
  static int parse_backup_sets_(
      sqlclient::ObMySQLResult &result, 
      common::ObIArray<ObBackupSetFileDesc> &backup_set_descs);
};

class ObBackupJobOperator : public ObBackupBaseTableOperator
{
public:
  static int insert_job(common::ObISQLClient &proxy, const ObBackupJobAttr &job_attr);
  static int get_jobs(common::ObISQLClient &proxy, const uint64_t tenant_id, bool need_lock, 
      ObIArray<ObBackupJobAttr> &job_attrs);
  static int get_job(common::ObISQLClient &proxy, bool need_lock, const uint64_t tenant_id, const int64_t job_id, 
      const bool is_initiator, ObBackupJobAttr &job_attrs);
  static int cnt_jobs(common::ObISQLClient &proxy, const uint64_t tenant_id, const uint64_t initiator_tenant_id, int64_t &cnt);
  static int cancel_jobs(common::ObISQLClient &proxy, const uint64_t tenant_id);
  static int advance_job_status(common::ObISQLClient &proxy, const ObBackupJobAttr &job_attr,
      const ObBackupStatus &next_status, const int result = OB_SUCCESS, const int64_t end_ts = 0);
  static int move_job_to_his(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id);
  static int update_retry_count(common::ObISQLClient &proxy, const ObBackupJobAttr &job_attr); 
  static int update_comment(common::ObISQLClient &proxy, const ObBackupJobAttr &job_attr);
private:
  static int fill_dml_with_job_(const ObBackupJobAttr &job_attr, ObDMLSqlSplicer &dml);
  static int fill_select_job_sql_(ObSqlString &sql);
  static int parse_job_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObBackupJobAttr> &jobs);
  static int do_parse_job_result_(sqlclient::ObMySQLResult &result, ObBackupJobAttr &job);
  static int do_parse_desc_result_(sqlclient::ObMySQLResult &result, ObBackupJobAttr &job);
};

class ObBackupTaskOperator : public ObBackupBaseTableOperator
{
public:
  static int insert_backup_task(common::ObISQLClient &proxy, const ObBackupSetTaskAttr &backup_set_task);
  static int update_turn_id(common::ObISQLClient &proxy, share::ObBackupStatus &backup_status, const int64_t task_id,
      const uint64_t tenant_id, const int64_t turn_id);
  static int get_backup_task(common::ObISQLClient &proxy, const int64_t job_id, const uint64_t tenant_id, 
      const bool for_update, ObBackupSetTaskAttr &set_task_attr);
  static int advance_task_status(common::ObISQLClient &proxy, const ObBackupSetTaskAttr &set_task_attr,
      const ObBackupStatus &next_status, const int result, const SCN &end_scn, const int64_t end_ts);
  static int move_task_to_his(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id);
  static int update_stats(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id, 
      const ObBackupStats &stats);
  static int update_user_ls_start_scn(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id, 
      const SCN &scn);
private:
  static int fill_dml_with_backup_task_(const ObBackupSetTaskAttr &backup_set_task, ObDMLSqlSplicer &dml);
};

class ObBackupLSTaskOperator : public ObBackupBaseTableOperator
{
public:
  static int insert_ls_task(common::ObISQLClient &proxy, const ObBackupLSTaskAttr &ls_attr);
  static int report_ls_task(common::ObISQLClient &proxy, const ObBackupLSTaskAttr &ls_attr, const ObSqlString &extra_condition);
  static int report_ls_task(common::ObISQLClient &proxy, const ObBackupLSTaskAttr &ls_attr);
  static int get_ls_task(common::ObISQLClient &proxy, bool need_lock, const int64_t task_id,
      const uint64_t tenant_id, const ObLSID &ls_id, ObBackupLSTaskAttr &ls_attr); 
  static int get_ls_tasks(common::ObISQLClient &proxy, const int64_t job_id, const uint64_t tenant_id, bool need_lock, 
      ObIArray<ObBackupLSTaskAttr> &ls_attrs);
  static int update_dst_and_status(common::ObISQLClient &proxy, const int64_t task_id,
      const uint64_t tenant_id, const ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, share::ObTaskId task_trace_id, common::ObAddr &dst);
  static int insert_build_index_task(common::ObISQLClient &proxy, const ObBackupLSTaskAttr &build_index_attr);
  static int delete_build_index_task(common::ObISQLClient &proxy, const ObBackupLSTaskAttr &build_index_attr);
  static int move_ls_to_his(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id);
  static int delete_ls_task_without_sys(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id);
  static int update_stats_(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id,
      const ObLSID &ls_id, const ObBackupStats &stats);
  static int update_max_tablet_checkpoint_scn(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id,
      const ObLSID &ls_id, const SCN &max_tablet_checkpoint_scn);
private:
  static int fill_dml_with_ls_task_(const ObBackupLSTaskAttr &ls_attr, ObDMLSqlSplicer &dml);
  static int fill_select_ls_task_sql_(ObSqlString &sql);
  static int parse_ls_result_(sqlclient::ObMySQLResult &result, ObIArray<ObBackupLSTaskAttr> &ls_attrs);
  static int do_parse_ls_result_(sqlclient::ObMySQLResult &result, ObBackupLSTaskAttr &ls_attr);
};

class ObBackupLSTaskInfoOperator : public ObBackupBaseTableOperator
{
public:
  static int update_ls_task_info_final(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id,
      const ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id);
  static int get_statistics_info(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id, 
      const ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, ObIArray<ObBackupLSTaskInfoAttr> &task_infos);
  static int move_ls_task_info_to_his(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id);
private:
  static int parse_ls_task_info_(sqlclient::ObMySQLResult &result, ObIArray<ObBackupLSTaskInfoAttr> &task_infos);
  static int do_parse_ls_task_info_(sqlclient::ObMySQLResult &result, ObBackupLSTaskInfoAttr &task_info);
};

class ObLSBackupInfoOperator : public ObBackupBaseTableOperator
{
public:
  typedef common::ObFixedLengthString<common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH> Value;
  struct InfoItem
  {
    bool is_valid() const { return nullptr != name_; }
    TO_STRING_KV(K_(name), K_(value));
    const char *name_;
    Value value_;
  };
public:
  static int get_next_job_id(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &job_id);
  static int get_next_task_id(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &task_id);
  static int get_next_backup_set_id(common::ObISQLClient &trans, const uint64_t &tenant_id, int64_t &backup_set_id);
  static int get_next_dest_id(common::ObISQLClient &trans, const uint64_t &tenant_id, int64_t &dest_id);
  static int set_backup_version(common::ObISQLClient &trans, const uint64_t tenant_id, const uint64_t data_version);
  static int get_backup_version(common::ObISQLClient &trans, const uint64_t tenant_id, uint64_t &data_version);
  static int set_cluster_version(common::ObISQLClient &trans, const uint64_t tenant_id, const uint64_t cluster_version);
  static int get_cluster_version(common::ObISQLClient &trans, const uint64_t tenant_id, uint64_t &cluster_version);
private:
  static int get_item(common::ObISQLClient &proxy, const uint64_t tenant_id, InfoItem &item, const bool need_lock);
  static int insert_item_with_update(common::ObISQLClient &proxy, const uint64_t tenant_id, InfoItem &item);
  static int set_item_value(Value &dst_value, int64_t src_value);
  static int set_item_value(Value &dst_value, uint64_t src_value);
  static int set_item_value(Value &dst_value, const char *src_buf);
};

}
}
#endif  // OCEANBASE_SHARE_OB_BACKUP_DATA_TABLE_OPERATOR_H_
