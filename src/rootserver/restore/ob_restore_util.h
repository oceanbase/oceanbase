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

#ifndef __OB_RS_RESTORE_UTIL_H__
#define __OB_RS_RESTORE_UTIL_H__

#include "share/ob_rpc_struct.h"
#include "share/restore/ob_physical_restore_table_operator.h"//PhysicalRestoreStatus

namespace oceanbase
{
namespace share
{
struct ObHisRestoreJobPersistInfo;
struct ObPhysicalRestoreJob;
}
namespace palf
{
  struct PalfBaseInfo;
}
namespace rootserver
{

class ObRestoreUtil
{
public:
  static int fill_physical_restore_job(
             const int64_t job_id,
             const obrpc::ObPhysicalRestoreTenantArg &arg,
             share::ObPhysicalRestoreJob &job);
  static int record_physical_restore_job(
             common::ObISQLClient &sql_client,
             const share::ObPhysicalRestoreJob &job);
  static int recycle_restore_job(const uint64_t tenant_id,
                         common::ObMySQLProxy &sql_proxy,
                         const share::ObPhysicalRestoreJob &job_info);
  static int recycle_restore_job(common::ObMySQLProxy &sql_proxy,
                                 const share::ObPhysicalRestoreJob &job_info,
                                const share::ObHisRestoreJobPersistInfo &history_info);
  static int check_has_physical_restore_job(
             common::ObISQLClient &sql_client,
             const common::ObString &tenant_name,
             bool &has_job);
  static int get_restore_source(
             const ObIArray<ObString>& tenant_path_array,
             const common::ObString &passwd_array,
             const uint64_t restore_timestamp_ns,
             ObIArray<share::ObRestoreBackupSetBriefInfo> &backup_set_list,
             ObIArray<share::ObBackupPiecePath> &backup_piece_list,
             ObIArray<share::ObBackupPathString> &log_path_list);
  static int insert_user_tenant_restore_job(
             common::ObISQLClient &sql_client,
             const ObString &tenant_name,
             const int64_t user_tenant_id);
  static int get_user_restore_job_history(common::ObISQLClient &sql_client,
                                          const uint64_t user_tenant_id,
                                          const uint64_t initiator_tenant_id,
                                          const int64_t initiator_job_id,
                                          share::ObHisRestoreJobPersistInfo &history_info);
  static int get_restore_ls_palf_base_info(const share::ObPhysicalRestoreJob &job_info,
                                           const share::ObLSID &ls_id,
                                           palf::PalfBaseInfo &palf_base_info);
  static int check_physical_restore_finish(common::ObISQLClient &proxy, uint64_t tenant_id, bool &is_finish, bool &is_failed);

private:
  static int fill_backup_info_(
             const obrpc::ObPhysicalRestoreTenantArg &arg,
             share::ObPhysicalRestoreJob &job);
  static int fill_multi_backup_path(
             const obrpc::ObPhysicalRestoreTenantArg &arg,
             share::ObPhysicalRestoreJob &job);
  static int fill_compat_backup_path(
             const obrpc::ObPhysicalRestoreTenantArg &arg,
             share::ObPhysicalRestoreJob &job);
  static int get_restore_backup_set_array_(
             const ObIArray<ObString> &tenant_path_array,
             const common::ObString &passwd_array,
             const int64_t restore_timestamp_ns,
             int64_t &restore_start_log_ts,
             ObIArray<share::ObRestoreBackupSetBriefInfo> &backup_set_list);
  static int get_restore_log_piece_array_(
             const ObIArray<ObString> &tenant_path_array,
             const int64_t restore_start_log_ts,
             const int64_t restore_end_log_ts,
             ObIArray<share::ObBackupPiecePath> &backup_piece_list,
             ObIArray<share::ObBackupPathString> &log_path_list);
  static int get_restore_backup_piece_list_(
      const share::ObBackupDest &dest,
      const ObArray<share::ObBackupPath> &piece_array,
      ObIArray<share::ObBackupPiecePath> &backup_piece_list);
  static int get_restore_log_path_list_(
      const share::ObBackupDest &dest,
      ObIArray<share::ObBackupPathString> &log_path_list);
  static int do_fill_backup_path_(
             const ObIArray<share::ObRestoreBackupSetBriefInfo> &backup_set_list,
             const ObIArray<share::ObBackupPiecePath> &backup_piece_list,
            const ObIArray<share::ObBackupPathString> &log_path_list,
             share::ObPhysicalRestoreJob &job);
  static int do_fill_backup_info_(
             const share::ObBackupSetPath & backup_set_path,
             share::ObPhysicalRestoreJob &job);
  static int get_encrypt_backup_dest_format_str(
      const ObArray<ObString> &original_dest_list,
      common::ObArenaAllocator &allocator,
      common::ObString &encrypt_dest_str);
  static int fill_restore_scn_(
      const obrpc::ObPhysicalRestoreTenantArg &arg, 
      const ObIArray<ObString> &tenant_path_array, 
      share::ObPhysicalRestoreJob &job);

  DISALLOW_COPY_AND_ASSIGN(ObRestoreUtil);
};

}
}
#endif /* __OB_RS_RESTORE_UTIL_H__ */
//// end of header file

