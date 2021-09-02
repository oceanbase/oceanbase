// Copyright 2021 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_BACKUPPIECE_OPERATOR_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_BACKUPPIECE_OPERATOR_H_

#include "share/backup/ob_backup_struct.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase {
namespace share {

class ObIBackupBackupPieceJobOperator {
public:
  ObIBackupBackupPieceJobOperator() = default;
  virtual ~ObIBackupBackupPieceJobOperator() = default;
  static int fill_one_item(const ObBackupBackupPieceJobInfo& item, ObDMLSqlSplicer& splicer);
  static int extract_one_item(sqlclient::ObMySQLResult* result, ObBackupBackupPieceJobInfo& item);
  static int get_item_list(const common::ObSqlString& sql, common::ObISQLClient& sql_proxy,
      common::ObIArray<ObBackupBackupPieceJobInfo>& item_list);
};

class ObBackupBackupPieceJobOperator {
public:
  ObBackupBackupPieceJobOperator() = default;
  virtual ~ObBackupBackupPieceJobOperator() = default;
  static int insert_job_item(const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy);
  static int get_job_item(const int64_t job_id, common::ObISQLClient& proxy, ObBackupBackupPieceJobInfo& item);
  static int get_all_job_items(common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceJobInfo>& items);
  static int get_one_job(common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceJobInfo>& items);
  static int report_job_item(const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy);
  static int remove_job_item(const int64_t job_id, common::ObISQLClient& proxy);
};

class ObBackupBackupPieceJobHistoryOperator {
public:
  ObBackupBackupPieceJobHistoryOperator() = default;
  virtual ~ObBackupBackupPieceJobHistoryOperator() = default;
  static int insert_job_item(const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy);
  static int get_job_item(const int64_t job_id, common::ObISQLClient& proxy, ObBackupBackupPieceJobInfo& item);
  static int report_job_item(const ObBackupBackupPieceJobInfo& item, common::ObISQLClient& proxy);
  static int remove_job_item(const int64_t job_id, common::ObISQLClient& proxy);
};

class ObIBackupBackupPieceTaskOperator {
public:
  ObIBackupBackupPieceTaskOperator() = default;
  virtual ~ObIBackupBackupPieceTaskOperator() = default;
  static int fill_one_item(const ObBackupBackupPieceTaskInfo& item, ObDMLSqlSplicer& splicer);
  static int extract_one_item(sqlclient::ObMySQLResult* result, ObBackupBackupPieceTaskInfo& item);
  static int get_item_list(const common::ObSqlString& sql, common::ObISQLClient& sql_proxy,
      common::ObIArray<ObBackupBackupPieceTaskInfo>& item_list);
};

class ObBackupBackupPieceTaskOperator {
public:
  ObBackupBackupPieceTaskOperator() = default;
  virtual ~ObBackupBackupPieceTaskOperator() = default;
  static int get_job_task_count(
      const ObBackupBackupPieceJobInfo& job_info, common::ObISQLClient& sql_proxy, int64_t& task_count);
  static int insert_task_item(const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy);
  static int get_all_task_items(
      const int64_t job_id, common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceTaskInfo>& items);
  static int get_doing_task_items(const int64_t job_id, const uint64_t tenant_id, common::ObISQLClient& proxy,
      common::ObIArray<ObBackupBackupPieceTaskInfo>& items);
  static int get_task_item(const int64_t job_id, const int64_t tenant_id, const int64_t piece_id,
      common::ObISQLClient& proxy, ObBackupBackupPieceTaskInfo& item);
  static int get_smallest_doing_task(
      const int64_t job_id, const uint64_t tenant_id, common::ObISQLClient& proxy, ObBackupBackupPieceTaskInfo& item);
  static int update_task_finish(
      const uint64_t tenant_id, const int64_t job_id, const int64_t piece_id, common::ObISQLClient& proxy);
  static int report_task_item(const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy);
  static int remove_task_items(const int64_t job_id, common::ObISQLClient& proxy);
};

class ObBackupBackupPieceTaskHistoryOperator {
public:
  ObBackupBackupPieceTaskHistoryOperator() = default;
  virtual ~ObBackupBackupPieceTaskHistoryOperator() = default;
  static int insert_task_item(const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy);
  static int get_task_items(
      const int64_t job_id, common::ObISQLClient& proxy, common::ObIArray<ObBackupBackupPieceTaskInfo>& items);
  static int report_task_item(const ObBackupBackupPieceTaskInfo& item, common::ObISQLClient& proxy);
  static int report_task_items(
      const common::ObIArray<ObBackupBackupPieceTaskInfo>& task_items, common::ObISQLClient& proxy);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
