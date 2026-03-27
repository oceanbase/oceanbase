/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_SERVICE_PROXY_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_SERVICE_PROXY_H_

namespace oceanbase
{
namespace obrpc
{
struct ObBackupDatabaseArg;
struct ObBackupCleanArg;
struct ObDeletePolicyArg;
struct ObBackupCleanArg;
struct ObArchiveLogArg;
struct ObBackupManageArg;
struct ObArchiveLogArg;
struct ObBackupValidateArg;
}

namespace rootserver
{

class ObBackupServiceProxy
{
public:
  static int handle_backup_database(const obrpc::ObBackupDatabaseArg &arg);
  static int handle_backup_database_cancel(const obrpc::ObBackupManageArg &arg);
  static int handle_backup_delete(const obrpc::ObBackupCleanArg &arg);
  static int handle_delete_policy(const obrpc::ObDeletePolicyArg &arg);
  static int handle_backup_delete_obsolete(const obrpc::ObBackupCleanArg &arg);
  static int handle_archive_log(const obrpc::ObArchiveLogArg &arg);
  static int handle_backup_validate(const obrpc::ObBackupValidateArg &arg);
  static int handle_backup_validate_cancel(const obrpc::ObBackupManageArg &arg);
};

}
}

#endif
