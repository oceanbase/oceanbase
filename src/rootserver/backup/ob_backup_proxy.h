/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
};

}
}

#endif
