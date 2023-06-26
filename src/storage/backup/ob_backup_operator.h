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

#ifndef STORAGE_LOG_STREAM_BACKUP_OPERATOR_H_
#define STORAGE_LOG_STREAM_BACKUP_OPERATOR_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/ob_dml_sql_splicer.h"
#include "storage/backup/ob_backup_data_struct.h"

namespace oceanbase {
namespace backup {

class ObLSBackupOperator {
public:
  // __all_backup_ls_task_info
  static int insert_ls_backup_task_info(const uint64_t tenant_id, const int64_t task_id, const int64_t turn_id,
      const int64_t retry_id, const share::ObLSID &ls_id, const int64_t backup_set_id,
      const share::ObBackupDataType &backup_data_type, common::ObISQLClient &sql_client);
  static int report_ls_backup_task_info(const uint64_t tenant_id, const int64_t task_id, const int64_t turn_id,
      const int64_t retry_id, const share::ObBackupDataType &backup_data_type, const ObLSBackupStat &stat,
      common::ObISQLClient &sql_client);
  static int get_backup_ls_task_info(const uint64_t tenant_id, const int64_t task_id, const share::ObLSID &ls_id,
      const int64_t turn_id, const int64_t retry_id, const share::ObBackupDataType &backup_data_type,
      const bool for_update, ObBackupLSTaskInfo &task_info, common::ObISQLClient &sql_client);
  static int get_all_retries(const int64_t task_id, const uint64_t tenant_id,
      const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id,
      common::ObIArray<ObBackupRetryDesc> &retry_list, common::ObISQLClient &sql_client);
  static int mark_ls_task_info_final(const int64_t task_id, const uint64_t tenant_id, const share::ObLSID &ls_id,
      const int64_t turn_id, const int64_t retry_id, const share::ObBackupDataType &backup_data_type,
      common::ObISQLClient &sql_client);
  // __all_backup_set_files
  static int get_prev_backup_set_desc(const uint64_t tenant_id, const int64_t backup_set_id, const int64_t dest_id,
      share::ObBackupSetFileDesc &prev_desc, common::ObISQLClient &sql_client);
  // __all_backup_ls_task
  static int report_ls_task_finish(const uint64_t tenant_id, const int64_t task_id, const share::ObLSID &ls_id,
      const int64_t turn_id, const int64_t retry_id, const int64_t result, common::ObISQLClient &sql_client);
  static int get_all_backup_ls_id(const uint64_t tenant_id, const int64_t task_id,
    common::ObIArray<share::ObLSID> &ls_array, common::ObISQLClient &sql_client);
  static int get_all_archive_ls_id(const uint64_t tenant_id, const int64_t dest_id,
    const share::SCN &start_scn, const share::SCN &end_scn, common::ObIArray<share::ObLSID> &ls_array,
    common::ObISQLClient &sql_client);

  // __all_backup_skipped_tablet
  static int report_tablet_skipped(
      const uint64_t tenant_id, const ObBackupSkippedTablet &skipped_tablet, common::ObISQLClient &sql_client);

private:
  static int fill_ls_task_info_(const ObBackupLSTaskInfo &task_info, share::ObDMLSqlSplicer &splicer);
  static int parse_ls_task_info_results_(
      sqlclient::ObMySQLResult &result, common::ObIArray<ObBackupLSTaskInfo> &task_info);
  static int fill_backup_skipped_tablet_(const ObBackupSkippedTablet &skipped_tablet, share::ObDMLSqlSplicer &splicer);
  static int get_distinct_ls_id_(const uint64_t tenant_id, const common::ObSqlString &sql,
      common::ObIArray<share::ObLSID> &ls_array, common::ObISQLClient &sql_client);
  static int get_piece_id_(const uint64_t tenant_id, const common::ObSqlString &sql, int64_t &piece_id, common::ObISQLClient &sql_client);
  static int get_start_piece_id_(const uint64_t tenant_id, const uint64_t dest_id,
      const share::SCN &start_scn, common::ObISQLClient &sql_client, int64_t &start_piece_id);
  static int get_end_piece_id_(const uint64_t tenant_id, const uint64_t dest_id, const share::SCN &end_scn,
      common::ObISQLClient &sql_client, int64_t &end_piece_id);
  static int construct_query_backup_sql_(const uint64_t tenant_id, const int64_t task_id, common::ObSqlString &sql);
  static int construct_query_archive_sql_(const uint64_t tenant_id, const int64_t dest_id, const int64_t start_piece_id,
      const int64_t end_piece_id, common::ObSqlString &sql);
};

}  // namespace backup
}  // namespace oceanbase

#endif
