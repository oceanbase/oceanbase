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

#define USING_LOG_PREFIX RS_RESTORE

#include "ob_restore_util.h"
#include "lib/lock/ob_mutex.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_path.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "share/backup/ob_archive_store.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/restore/ob_restore_persist_helper.h"//ObRestorePersistHelper ObRestoreProgressPersistInfo
#include "logservice/palf/palf_base_info.h"//PalfBaseInfo
#include "storage/ls/ob_ls_meta_package.h"//ls_meta
#include "share/backup/ob_archive_path.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_unit_table_operator.h"

using namespace oceanbase::common;
using namespace oceanbase;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

/*-------------- physical restore --------------------------*/
int ObRestoreUtil::fill_physical_restore_job(
    const int64_t job_id,
    const obrpc::ObPhysicalRestoreTenantArg &arg,
    ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;

  if (job_id < 0 || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(job_id), K(arg));
  } else {
    job.reset();
    job.init_restore_key(OB_SYS_TENANT_ID, job_id); 
    job.set_status(PhysicalRestoreStatus::PHYSICAL_RESTORE_CREATE_TENANT);
    job.set_tenant_name(arg.tenant_name_);
    job.set_initiator_job_id(arg.initiator_job_id_);
    job.set_initiator_tenant_id(arg.initiator_tenant_id_);
    if (OB_FAIL(job.set_description(arg.description_))) {
      LOG_WARN("fail to set description", K(ret));
    }

    // check restore option
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPhysicalRestoreOptionParser::parse(arg.restore_option_, job))) {
        LOG_WARN("fail to parse restore_option", K(ret), K(arg), K(job_id));
      } else if (OB_FAIL(job.set_restore_option(arg.restore_option_))){
        LOG_WARN("failed to set restore option", KR(ret), K(arg));
      } else if (job.get_kms_encrypt()) {
        if (OB_FAIL(job.set_kms_info(arg.kms_info_))) {
          LOG_WARN("failed to fill kms info", KR(ret), K(arg));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_backup_info_(arg, job))) {
        LOG_WARN("failed to fill backup info", KR(ret), K(arg), K(job));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_encrypt_info_(arg, job))) {
        LOG_WARN("failed to fill encrypt info", KR(ret), K(arg), K(job));
      }
    }

    if (FAILEDx(job.set_passwd_array(arg.passwd_array_))) {
      LOG_WARN("failed to copy passwd array", K(ret), K(arg));
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.table_items_.count(); i++) {
        const obrpc::ObTableItem &item = arg.table_items_.at(i);
        if (OB_FAIL(job.get_white_list().add_table_item(item))) {
          LOG_WARN("fail to add table item", KR(ret), K(item));
        }
      }
    }
  }

  LOG_INFO("finish fill_physical_restore_job", K(job_id), K(arg), K(job));
  return ret;
}

int ObRestoreUtil::record_physical_restore_job(
    common::ObISQLClient &sql_client,
    const ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(job));
  } else {
    bool has_job = false;
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(check_has_physical_restore_job(sql_client,
                                               job.get_tenant_name(),
                                               has_job))) {
      LOG_WARN("fail to check if job exist", K(ret), K(job));
    } else if (has_job) {
      ret = OB_RESTORE_IN_PROGRESS;
      LOG_WARN("restore tenant job already exist", K(ret), K(job));
    } else if (OB_FAIL(restore_op.init(&sql_client, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail init restore op", K(ret));
    } else if (OB_FAIL(restore_op.insert_job(job))) {
      LOG_WARN("fail insert job and partitions", K(ret), K(job));
    }
  }
  return ret;
}

int ObRestoreUtil::insert_user_tenant_restore_job(
             common::ObISQLClient &sql_client,
             const ObString &tenant_name,
             const int64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(user_tenant_id));
  } else {
    ObPhysicalRestoreTableOperator restore_op;
    ObPhysicalRestoreJob initaitor_job_info;
    ObPhysicalRestoreJob job_info;
    if (OB_FAIL(restore_op.init(&sql_client, OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to init restore op", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(restore_op.get_job_by_tenant_name(
            tenant_name, initaitor_job_info))) {
      LOG_WARN("failed to get job by tenant name", KR(ret), K(tenant_name));
    } else if (OB_FAIL(job_info.assign(initaitor_job_info))) {
      LOG_WARN("failed to assign job info", KR(ret), K(initaitor_job_info));
    } else {
      ObMySQLTransaction trans;
      //TODO get tenant job_id, use tenant
      const int64_t job_id = initaitor_job_info.get_job_id();
      job_info.init_restore_key(user_tenant_id, job_id);
      job_info.set_tenant_id(user_tenant_id);
      job_info.set_status(share::PHYSICAL_RESTORE_PRE);
      job_info.set_initiator_job_id(job_info.get_job_id());
      job_info.set_initiator_tenant_id(OB_SYS_TENANT_ID);
      ObPhysicalRestoreTableOperator user_restore_op;
      ObRestorePersistHelper restore_persist_op;
      ObRestoreProgressPersistInfo persist_info;
      persist_info.key_.tenant_id_ = user_tenant_id;
      persist_info.key_.job_id_ = job_info.get_job_id();
      persist_info.restore_scn_ = job_info.get_restore_scn();
      const uint64_t exec_tenant_id = gen_meta_tenant_id(user_tenant_id);
      if (OB_FAIL(trans.start(&sql_client, exec_tenant_id))) {
        LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
      } else if (OB_FAIL(user_restore_op.init(&trans, user_tenant_id))) {
        LOG_WARN("failed to init restore op", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(restore_persist_op.init(user_tenant_id))) {
        LOG_WARN("failed to init restore persist op", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(user_restore_op.insert_job(job_info))) {
        LOG_WARN("failed to insert job", KR(ret), K(job_info));
      } else if (OB_FAIL(restore_persist_op.insert_initial_restore_progress(trans, persist_info))) {
        LOG_WARN("failed to insert persist info", KR(ret), K(persist_info));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        bool commit = OB_SUCC(ret);
        if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
          LOG_WARN("trans end failed", KR(ret), KR(temp_ret), K(commit));
        }
      }
    }
  }
  return ret;
}


int ObRestoreUtil::check_has_physical_restore_job(
    common::ObISQLClient &sql_client,
    const ObString &tenant_name,
    bool &has_job)
{
  int ret = OB_SUCCESS;
  ObArray<ObPhysicalRestoreJob> jobs;
  has_job = false;
  ObPhysicalRestoreTableOperator restore_op;
  if (OB_FAIL(restore_op.init(&sql_client, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail init restore op", K(ret));
  } else if (OB_FAIL(restore_op.get_jobs(jobs))) {
    LOG_WARN("fail get jobs", K(ret));
  } else {
    int64_t len = common::OB_MAX_TENANT_NAME_LENGTH_STORE;
    FOREACH_CNT_X(job, jobs, !has_job) {
      if (0 == job->get_tenant_name().case_compare(tenant_name)) {
        //nocase compare
        has_job = true;
      }
    }
  }
  return ret;
}

int ObRestoreUtil::fill_backup_info_(
    const obrpc::ObPhysicalRestoreTenantArg &arg,
    share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  const bool has_multi_url = arg.multi_uri_.length() > 0;
  LOG_INFO("start fill backup path", K(arg));
  if (has_multi_url) {
    if(OB_FAIL(fill_multi_backup_path(arg, job))) {
      LOG_WARN("failed to fill multi backup path", K(ret), K(arg));
    }
  } else {
    if (OB_FAIL(fill_compat_backup_path(arg, job))) {
      LOG_WARN("failed to fill compat backup path", K(ret), K(arg));
    }
  }
  FLOG_INFO("finish fill backup path", K(arg), K(job));
  return ret;
}

int ObRestoreUtil::fill_multi_backup_path(
    const obrpc::ObPhysicalRestoreTenantArg &arg,
    share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  // TODO: use restore preview url
  return ret;
}

int ObRestoreUtil::get_encrypt_backup_dest_format_str(
    const ObArray<ObString> &original_dest_list,
    common::ObArenaAllocator &allocator,
    common::ObString &encrypt_dest_str)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t length = OB_MAX_BACKUP_DEST_LENGTH * original_dest_list.count();
  if (0 == original_dest_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(original_dest_list));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", KR(ret));
  } else {
    ObBackupDest dest;
    char encrypt_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    int64_t pos = 0;
    for (int i = 0; OB_SUCC(ret) && i < original_dest_list.count(); i++) {
      const common::ObString &item = original_dest_list.at(i);
      if (OB_FAIL(dest.set_without_decryption(item))) {
        LOG_WARN("failed to push back", KR(ret), K(item));
      } else if (OB_FAIL(dest.get_backup_dest_str(encrypt_str, sizeof(encrypt_str)))) {
        LOG_WARN("failed to get backup dest str", KR(ret), K(item));
      } else if (OB_FAIL(databuff_printf(buf, length, pos, "%s%s", 0 == i ? "" : ",", encrypt_str))) {
        LOG_WARN("failed to append uri", KR(ret), K(encrypt_str), K(pos), K(buf)); 
      }
    }
    if (OB_FAIL(ret)) {
    } else if (strlen(buf) <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format str", KR(ret), K(buf)); 
    } else {
      encrypt_dest_str.assign_ptr(buf, strlen(buf));
      LOG_DEBUG("get format encrypt backup dest str", KR(ret), K(encrypt_dest_str));
    }
  }

  return ret;
}

int ObRestoreUtil::fill_compat_backup_path(
    const obrpc::ObPhysicalRestoreTenantArg &arg,
    share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObArray<ObString> tenant_path_array;
  ObArray<ObRestoreBackupSetBriefInfo> backup_set_list;
  ObArray<ObRestoreLogPieceBriefInfo> backup_piece_list;
  ObArray<ObBackupPathString> log_path_list;
  ObString tenant_dest_list;
  int64_t last_backup_set_idx = -1;
  bool restore_using_compl_log = false;
  share::SCN restore_scn;
  if (!arg.multi_uri_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else if (OB_FAIL(ObPhysicalRestoreUriParser::parse(arg.uri_, allocator, tenant_path_array))) {
    LOG_WARN("fail to parse uri", K(ret), K(arg));
  } else if (OB_FAIL(get_encrypt_backup_dest_format_str(tenant_path_array, allocator, tenant_dest_list))) {
    LOG_WARN("failed to convert uri", K(ret), K(arg), K(tenant_path_array)); 
  } else if (OB_FAIL(job.set_backup_dest(tenant_dest_list))) {
    LOG_WARN("failed to copy backup dest", K(ret), K(arg));
  } else if (OB_FAIL(check_restore_using_complement_log_(tenant_path_array, restore_using_compl_log))) {
    LOG_WARN("failed to check only contain backup set", K(ret), K(tenant_path_array));
  } else if (OB_FAIL(fill_restore_scn_(
      arg.restore_scn_, arg.restore_timestamp_, arg.with_restore_scn_, tenant_path_array, arg.passwd_array_,
      restore_using_compl_log, restore_scn))) {
    LOG_WARN("fail to fill restore scn", K(ret), K(arg), K(tenant_path_array));
  } else if (OB_FALSE_IT(job.set_restore_scn(restore_scn))) {
  } else if (OB_FAIL(get_restore_source(restore_using_compl_log, tenant_path_array, arg.passwd_array_, job.get_restore_scn(),
      backup_set_list, backup_piece_list, log_path_list))) {
    LOG_WARN("fail to get restore source", K(ret), K(tenant_path_array), K(arg));
  } else if (OB_FAIL(do_fill_backup_path_(backup_set_list, backup_piece_list, log_path_list, job))) {
    LOG_WARN("fail to do fill backup path", K(backup_set_list), K(backup_piece_list), K(log_path_list));
  } else if (OB_FALSE_IT(last_backup_set_idx = backup_set_list.count() - 1)) {
  } else if (last_backup_set_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid idx", K(ret), K(last_backup_set_idx), K(backup_set_list));
  } else if (OB_FAIL(do_fill_backup_info_(backup_set_list.at(last_backup_set_idx).backup_set_path_, job))) {
    LOG_WARN("fail to do fill backup info");
  }
  return ret;
}

int ObRestoreUtil::fill_restore_scn_(
    const share::SCN &src_scn,
    const ObString &timestamp,
    const bool with_restore_scn,
    const ObIArray<ObString> &tenant_path_array,
    const common::ObString &passwd,
    const bool restore_using_compl_log,
    share::SCN &restore_scn)
{
  int ret = OB_SUCCESS;
  if (tenant_path_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_path_array));
  } else if (with_restore_scn) {
    // restore scn which is specified by user
    restore_scn = src_scn;
  } else if (!with_restore_scn) {
    if (restore_using_compl_log) {
      SCN min_restore_scn = SCN::min_scn();
      ARRAY_FOREACH_X(tenant_path_array, i, cnt, OB_SUCC(ret)) {
        const ObString &tenant_path = tenant_path_array.at(i);
        storage::ObBackupDataStore store;
        share::ObBackupDest backup_dest;
        ObBackupFormatDesc format_desc;
        share::ObBackupSetFileDesc backup_set_file_desc;
        if (OB_FAIL(backup_dest.set(tenant_path.ptr()))) {
          LOG_WARN("fail to set backup dest", K(ret), K(tenant_path));
        } else if (OB_FAIL(store.init(backup_dest))) {
          LOG_WARN("failed to init backup store", K(ret), K(tenant_path));
        } else if (OB_FAIL(store.read_format_file(format_desc))) {
          LOG_WARN("failed to read format file", K(ret), K(store));
        } else if (ObBackupDestType::DEST_TYPE_BACKUP_DATA != format_desc.dest_type_) {
          LOG_INFO("skip log dir", K(tenant_path), K(format_desc));
        } else if (OB_FAIL(store.get_max_backup_set_file_info(passwd, backup_set_file_desc))) {
          LOG_WARN("fail to get backup set array", K(ret));
        } else {
          min_restore_scn = backup_set_file_desc.min_restore_scn_;
        }
      }
      if (OB_SUCC(ret)) {
        if (SCN::min_scn() == min_restore_scn) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid min restore scn, do not find available backup tenant path to restore", K(ret), K(tenant_path_array));
        } else {
          restore_scn = min_restore_scn;
        }
      }
    } else if (!timestamp.empty()) {
      common::ObTimeZoneInfoWrap time_zone_wrap;
      if (OB_FAIL(get_backup_sys_time_zone_(tenant_path_array, time_zone_wrap))) {
        LOG_WARN("failed to get backup sys time zone", K(ret), K(tenant_path_array));
      } else if (OB_FAIL(convert_restore_timestamp_to_scn_(timestamp, time_zone_wrap, restore_scn))) {
        LOG_WARN("failed to convert restore timestamp to scn", K(ret));
      }
    } else {
      int64_t round_id = 0;
      int64_t piece_id = 0;
      SCN max_checkpoint_scn = SCN::min_scn();
      // restore to max checkpoint scn of log
      ARRAY_FOREACH_X(tenant_path_array, i, cnt, OB_SUCC(ret)) {
        const ObString &tenant_path = tenant_path_array.at(i);
        ObArchiveStore store;
        ObBackupDest dest;
        ObBackupFormatDesc format_desc;
        SCN cur_max_checkpoint_scn = SCN::min_scn();
        if (OB_FAIL(dest.set(tenant_path))) {
          LOG_WARN("fail to set dest", K(ret), K(tenant_path));
        } else if (OB_FAIL(store.init(dest))) {
          LOG_WARN("failed to init archive store", K(ret), K(tenant_path));
        } else if (OB_FAIL(store.read_format_file(format_desc))) {
          LOG_WARN("failed to read format file", K(ret), K(tenant_path));
        } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG != format_desc.dest_type_) {
          LOG_INFO("skip data dir", K(tenant_path), K(format_desc));
        } else if (OB_FAIL(store.get_max_checkpoint_scn(format_desc.dest_id_, round_id, piece_id, cur_max_checkpoint_scn))) {
          LOG_WARN("fail to get max checkpoint scn", K(ret), K(format_desc));
        } else {
          max_checkpoint_scn = std::max(max_checkpoint_scn, cur_max_checkpoint_scn);
        }
      }
      if (OB_SUCC(ret)) {
        if (SCN::min_scn() == max_checkpoint_scn) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid max checkpoint scn, no archvie tenant path", K(ret), K(tenant_path_array));
        } else {
          restore_scn = max_checkpoint_scn;
        }
      }
    }
  } 
  return ret;
}

int ObRestoreUtil::fill_encrypt_info_(
    const obrpc::ObPhysicalRestoreTenantArg &arg,
    share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  ObArenaAllocator allocator;
  ObArray<ObString> kms_path_array;
  ObString kms_dest_str;
  ObBackupDest dest;
  ObBackupIoAdapter util;
  bool is_exist = false;
  if (OB_FAIL(job.set_encrypt_key(arg.encrypt_key_))) {
    LOG_WARN("failed to fill encrypt key", KR(ret), K(arg));
  } else if (arg.kms_uri_.empty()) {
    // do nothing
  } else if (OB_FAIL(ObPhysicalRestoreUriParser::parse(arg.kms_uri_, allocator, kms_path_array))) {
    LOG_WARN("fail to parse uri", K(ret), K(arg));
  } else if (OB_FAIL(get_encrypt_backup_dest_format_str(kms_path_array, allocator, kms_dest_str))) {
    LOG_WARN("failed to convert uri", K(ret), K(arg), K(kms_path_array));
  } else if (OB_FAIL(dest.set(kms_dest_str))) {
    LOG_WARN("failed to set dest", K(ret));
  } else if (OB_FAIL(util.is_exist(dest.get_root_path(), dest.get_storage_info(), is_exist))) {
    LOG_WARN("failed to check file is exists", K(ret));
  } else if (OB_UNLIKELY(!is_exist)) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    LOG_WARN("kms backup file is not exist", K(ret));
  } else if (OB_FAIL(job.set_kms_dest(kms_dest_str))) {
    LOG_WARN("failed to copy kms dest", K(ret), K(arg));
  } else if (OB_FAIL(job.set_kms_encrypt_key(arg.kms_encrypt_key_))) {
    LOG_WARN("failed to fill kms encrypt key", KR(ret), K(arg));
  }
#endif
  return ret;
}

int ObRestoreUtil::get_restore_source(
    const bool restore_using_compl_log,
    const ObIArray<ObString>& tenant_path_array,
    const common::ObString &passwd_array,
    const SCN &restore_scn,
    ObIArray<ObRestoreBackupSetBriefInfo> &backup_set_list,
    ObIArray<ObRestoreLogPieceBriefInfo> &backup_piece_list,
    ObIArray<ObBackupPathString> &log_path_list)
{
  int ret = OB_SUCCESS;
  SCN restore_start_scn = SCN::min_scn();
  if (OB_FAIL(get_restore_backup_set_array_(tenant_path_array, passwd_array, restore_scn,
      restore_start_scn, backup_set_list))) {
    LOG_WARN("fail to get restore backup set array", K(ret), K(tenant_path_array), K(restore_scn));
  } else if (!restore_using_compl_log && OB_FAIL(get_restore_log_piece_array_(
      tenant_path_array, restore_start_scn, restore_scn, backup_piece_list, log_path_list))) {
    LOG_WARN("fail to get restore log piece array", K(ret), K(tenant_path_array), K(restore_start_scn),
        K(restore_scn));
  } else if (restore_using_compl_log && OB_FAIL(get_restore_log_array_for_complement_log_(
      backup_set_list, restore_start_scn, restore_scn, backup_piece_list, log_path_list))) {
    LOG_WARN("fail to get restore log piece array", K(ret), K(backup_set_list), K(restore_start_scn), K(restore_scn));
  } else if (backup_set_list.empty() || backup_piece_list.empty() || log_path_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no backup set path or log piece can be used to restore", K(ret),
        K(tenant_path_array), K(backup_set_list), K(backup_piece_list), K(log_path_list), K(restore_start_scn),
        K(restore_scn));
  }
  return ret;
}

int ObRestoreUtil::check_restore_using_complement_log_(
    const ObIArray<ObString> &tenant_path_array,
    bool &restore_using_compl_log)
{
  int ret = OB_SUCCESS;
  restore_using_compl_log = true;
  if (tenant_path_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_path_array));
  } else {
    ARRAY_FOREACH_X(tenant_path_array, i, cnt, OB_SUCC(ret)) {
      const ObString &tenant_path = tenant_path_array.at(i);
      storage::ObBackupDataStore store;
      share::ObBackupDest backup_dest;
      ObBackupFormatDesc format_desc;
      if (OB_FAIL(backup_dest.set(tenant_path.ptr()))) {
        LOG_WARN("fail to set backup dest", K(ret), K(tenant_path));
      } else if (OB_FAIL(store.init(backup_dest))) {
        LOG_WARN("failed to init backup store", K(ret), K(tenant_path));
      } else if (OB_FAIL(store.read_format_file(format_desc))) {
        LOG_WARN("failed to read format file", K(ret), K(store));
      } else if (ObBackupDestType::DEST_TYPE_ARCHIVE_LOG == format_desc.dest_type_) {
        restore_using_compl_log = false;
        LOG_INFO("not only contain backup data path", K(tenant_path), K(format_desc));
        break;
      }
    }
  }
  return ret;
}

int ObRestoreUtil::get_restore_backup_set_array_(
    const ObIArray<ObString> &tenant_path_array,
    const common::ObString &passwd_array,
    const SCN &restore_scn,
    SCN &restore_start_scn,
    ObIArray<ObRestoreBackupSetBriefInfo> &backup_set_list)
{
  int ret = OB_SUCCESS;
  if (tenant_path_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaldi argument", K(ret), K(tenant_path_array));
  } else {
    ARRAY_FOREACH_X(tenant_path_array, i, cnt, OB_SUCC(ret)) {
      const ObString &tenant_path = tenant_path_array.at(i);
      storage::ObBackupDataStore store;
      share::ObBackupDest backup_dest;
      ObBackupFormatDesc format_desc;
      if (OB_FAIL(backup_dest.set(tenant_path.ptr()))) {
        LOG_WARN("fail to set backup dest", K(ret), K(tenant_path));
      } else if (OB_FAIL(store.init(backup_dest))) {
        LOG_WARN("failed to init backup store", K(ret), K(tenant_path));
      } else if (OB_FAIL(store.read_format_file(format_desc))) {
        LOG_WARN("failed to read format file", K(ret), K(store));
      } else if (ObBackupDestType::DEST_TYPE_BACKUP_DATA != format_desc.dest_type_) {
        LOG_INFO("skip log dir", K(tenant_path), K(format_desc));
      } else if (!backup_set_list.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("It is not support to restore from multiple tenant backup paths", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "It is not support to restore from multiple tenant backup paths.");
      } else if (OB_FAIL(store.get_backup_set_array(passwd_array, restore_scn, restore_start_scn, backup_set_list))) {
        LOG_WARN("fail to get backup set array", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreUtil::get_restore_backup_piece_list_(
    const ObBackupDest &dest,
    const ObArray<share::ObRestoreLogPieceBriefInfo> &piece_array,
    ObIArray<ObRestoreLogPieceBriefInfo> &backup_piece_list)
{
  int ret = OB_SUCCESS; 
  if (!dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dest is invalid", K(ret), K(dest));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < piece_array.count(); ++j) {
      const share::ObRestoreLogPieceBriefInfo &piece_path = piece_array.at(j);
      ObRestoreLogPieceBriefInfo backup_piece_path;
      backup_piece_path.piece_id_ = piece_path.piece_id_;
      backup_piece_path.start_scn_ = piece_path.start_scn_;
      backup_piece_path.checkpoint_scn_ = piece_path.checkpoint_scn_;
      ObBackupDest piece_dest;
      if (OB_FAIL(piece_dest.set(piece_path.piece_path_.ptr(), dest.get_storage_info()))) {
        LOG_WARN("fail to set piece dest", K(ret), K(piece_path), K(dest)); 
      } else if (OB_FAIL(piece_dest.get_backup_dest_str(backup_piece_path.piece_path_.ptr(), backup_piece_path.piece_path_.capacity()))) {
        LOG_WARN("fail to get piece dest str", K(ret), K(piece_dest));
      } else if (OB_FAIL(backup_piece_list.push_back(backup_piece_path))) {
        LOG_WARN("fail to push backup piece list", K(ret));
      }
    }
  }

  return ret;
}

int ObRestoreUtil::get_restore_backup_piece_list_(
    const ObBackupDest &dest,
    const ObArray<share::ObPieceKey> &piece_array,
    ObIArray<ObRestoreLogPieceBriefInfo> &backup_piece_list)
{
  int ret = OB_SUCCESS;
  if (!dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dest is invalid", K(ret), K(dest));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < piece_array.count(); ++j) {
      const share::ObPieceKey &piece_key = piece_array.at(j);
      ObRestoreLogPieceBriefInfo backup_piece_path;
      backup_piece_path.piece_id_ = piece_key.piece_id_;
      ObBackupPath backup_path;
      ObBackupDest piece_dest;
      if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(dest, piece_key.dest_id_,
          piece_key.round_id_, piece_key.piece_id_, backup_path))) {
        LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(piece_key));
      } else if (OB_FAIL(piece_dest.set(backup_path.get_ptr(), dest.get_storage_info()))) {
        LOG_WARN("fail to set piece dest", K(ret), K(backup_path), K(dest));
      } else if (OB_FAIL(piece_dest.get_backup_dest_str(backup_piece_path.piece_path_.ptr(), backup_piece_path.piece_path_.capacity()))) {
        LOG_WARN("fail to get piece dest str", K(ret), K(piece_dest));
      } else if (OB_FAIL(backup_piece_list.push_back(backup_piece_path))) {
        LOG_WARN("fail to push backup piece list", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreUtil::get_restore_log_path_list_(
    const ObBackupDest &dest,
    ObIArray<share::ObBackupPathString> &log_path_list)
{
  int ret = OB_SUCCESS;
  ObBackupPathString log_path;
  if (!dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dest is invalid", K(ret), K(dest));
  } else if (OB_FAIL(dest.get_backup_dest_str(log_path.ptr(), log_path.capacity()))) {
    LOG_WARN("fail to get backup dest str", K(ret), K(dest));
  } else if (OB_FAIL(log_path_list.push_back(log_path))) {
    LOG_WARN("fail to push backup log path", K(ret), K(log_path));
  }
  return ret;
}

int ObRestoreUtil::get_restore_log_piece_array_(
    const ObIArray<ObString> &tenant_path_array,
    const SCN &restore_start_scn,
    const SCN &restore_end_scn,
    ObIArray<ObRestoreLogPieceBriefInfo> &backup_piece_list,
    ObIArray<share::ObBackupPathString> &log_path_list)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObRestoreLogPieceBriefInfo> piece_array;
  if (tenant_path_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaldi argument", K(ret), K(tenant_path_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_path_array.count(); ++i) {
      piece_array.reset();
      const ObString &tenant_path = tenant_path_array.at(i);
      ObArchiveStore store;
      ObBackupDest dest;
      ObBackupFormatDesc format_desc;
      if (OB_FAIL(dest.set(tenant_path))) {
        LOG_WARN("fail to set dest", K(ret), K(tenant_path));
      } else if (OB_FAIL(store.init(dest))) {
        LOG_WARN("failed to init archive store", K(ret), K(tenant_path));
      } else if (OB_FAIL(store.read_format_file(format_desc))) {
        LOG_WARN("failed to read format file", K(ret), K(tenant_path));
      } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG != format_desc.dest_type_) {
        LOG_INFO("skip data dir", K(tenant_path), K(format_desc));
      } else if (OB_FAIL(store.get_piece_paths_in_range(restore_start_scn, restore_end_scn, piece_array))) {
        LOG_WARN("fail to get restore pieces", K(ret), K(restore_start_scn), K(restore_end_scn));
      } else if (OB_FAIL(get_restore_log_path_list_(dest, log_path_list))) {
        LOG_WARN("fail to get restore log path list", K(ret), K(dest));
      } else if (OB_FAIL(get_restore_backup_piece_list_(dest, piece_array, backup_piece_list))){
        LOG_WARN("fail to get restore backup piece list", K(ret), K(dest), K(piece_array));
      }
    }
  }
  return ret;
}

int ObRestoreUtil::get_restore_log_array_for_complement_log_(
    const ObIArray<ObRestoreBackupSetBriefInfo> &backup_set_list,
    const share::SCN &restore_start_scn,
    const share::SCN &restore_end_scn,
    ObIArray<share::ObRestoreLogPieceBriefInfo> &backup_piece_list,
    ObIArray<share::ObBackupPathString> &log_path_list)
{
  int ret = OB_SUCCESS;
  if (backup_set_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaldi argument", K(ret), K(backup_set_list));
  } else {
    const ObRestoreBackupSetBriefInfo &info = backup_set_list.at(backup_set_list.count() - 1);
    ObBackupDest dest;
    ObBackupDest compl_dest;
    ObArchiveStore archive_store;
    ObArray<ObPieceKey> piece_array;
    if (OB_FAIL(dest.set(info.backup_set_path_.str()))) {
      LOG_WARN("failed to set backup set path", K(ret), K(info));
    } else if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(dest, compl_dest))) {
      LOG_WARN("failed to construct backup complement log dest", K(ret), K(dest), K(info));
    } else if (OB_FAIL(archive_store.init(compl_dest))) {
        LOG_WARN("failed to init archive store", K(ret), K(compl_dest));
    } else if (OB_FAIL(get_restore_log_path_list_(compl_dest, log_path_list))) {
      LOG_WARN("fail to get restore log path list", K(ret), K(dest));
    } else if (OB_FAIL(archive_store.get_all_piece_keys(piece_array))) {
        LOG_WARN("fail to get restore pieces", K(ret), K(restore_start_scn), K(restore_end_scn));
    } else if (OB_FAIL(get_restore_backup_piece_list_(compl_dest, piece_array, backup_piece_list))){
        LOG_WARN("fail to get restore backup piece list", K(ret), K(dest), K(piece_array));
    } else {
      LOG_INFO("get restore log path list", K(backup_set_list), K(log_path_list));
    }
  }
  return ret;
}

int ObRestoreUtil::do_fill_backup_path_(
    const ObIArray<ObRestoreBackupSetBriefInfo> &backup_set_list,
    const ObIArray<ObRestoreLogPieceBriefInfo> &backup_piece_list,
    const ObIArray<ObBackupPathString> &log_path_list,
    share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (backup_set_list.empty() || backup_piece_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_list), K(backup_piece_list));
  } else {
    ObArray<share::ObBackupPiecePath> backup_piece_path_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_list.count(); ++i) {
      if (OB_FAIL(backup_piece_path_list.push_back(backup_piece_list.at(i).piece_path_))) {
        LOG_WARN("failed to push backup piece", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(job.get_multi_restore_path_list().set(backup_set_list, backup_piece_path_list, log_path_list))) {
      LOG_WARN("failed to set mutli restore path list", KR(ret));
    }
  }
  return ret;
}

int ObRestoreUtil::do_fill_backup_info_(
    const share::ObBackupSetPath & backup_set_path,
    share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataStore store;
  ObBackupDataLSAttrDesc ls_info;
  HEAP_VARS_2((ObExternBackupSetInfoDesc, backup_set_info),
    (ObExternTenantLocalityInfoDesc, locality_info)) {
    if (backup_set_path.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(backup_set_path));
    } else if (OB_FAIL(store.init(backup_set_path.ptr()))) {
      LOG_WARN("fail to init mgr", K(ret));
    } else if (OB_FAIL(store.read_backup_set_info(backup_set_info))) {
      LOG_WARN("fail to read backup set info", K(ret));
    } else if (OB_FAIL(store.read_tenant_locality_info(locality_info))) {
      LOG_WARN("fail to read locality info", K(ret));
    } else if (!backup_set_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid backup set file", K(ret), K(backup_set_info));
    } else if (OB_FAIL(store.read_ls_attr_info(backup_set_info.backup_set_file_.meta_turn_id_, ls_info))) {
      LOG_WARN("failed to read ls attr info", K(ret), K(backup_set_info));
    } else if (OB_FAIL(check_backup_set_version_match_(backup_set_info.backup_set_file_))) {
      LOG_WARN("failed to check backup set version match", K(ret));
    } else if (OB_FAIL(job.set_backup_tenant_name(locality_info.tenant_name_.ptr()))) {
      LOG_WARN("fail to set backup tenant name", K(ret), "tenant name", locality_info.tenant_name_);
    } else if (OB_FAIL(job.set_backup_cluster_name(locality_info.cluster_name_.ptr()))) {
      LOG_WARN("fail to set backup cluster name", K(ret), "cluster name", locality_info.cluster_name_);
    } else {
      job.set_source_data_version(backup_set_info.backup_set_file_.tenant_compatible_);
      job.set_source_cluster_version(backup_set_info.backup_set_file_.cluster_version_);
      job.set_compat_mode(locality_info.compat_mode_);
      job.set_backup_tenant_id(backup_set_info.backup_set_file_.tenant_id_);
      // becuase of no consistent scn in 4.1.x backup set, using ls_info.backup_scn to set the restore consisitent scn
      // ls_info.backup_scn is the default replayable scn when create restore tenant,
      // so using it as the consistet scn can also make recovery service work normally
      const SCN &scn = backup_set_info.backup_set_file_.tenant_compatible_ < DATA_VERSION_4_2_0_0
                     ? ls_info.backup_scn_
                     : backup_set_info.backup_set_file_.consistent_scn_;
      job.set_consistent_scn(scn);
    }
  }
  return ret;
}

int ObRestoreUtil::check_backup_set_version_match_(share::ObBackupSetFileDesc &backup_file_desc)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!backup_file_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_file_desc));
  } else if (!ObUpgradeChecker::check_cluster_version_exist(backup_file_desc.cluster_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster version are not exist", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cluster version of backup set");
  } else if (!ObUpgradeChecker::check_data_version_exist(backup_file_desc.tenant_compatible_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data version are not exist", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant compatible of backup set");
  } else if (GET_MIN_CLUSTER_VERSION() < backup_file_desc.cluster_version_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore from higher cluster version is not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore from higher cluster version is");
  } else if (OB_FAIL(ObUpgradeChecker::get_data_version_by_cluster_version(GET_MIN_CLUSTER_VERSION(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < backup_file_desc.tenant_compatible_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore from higher data version is not allowed", K(ret), K(data_version), K(backup_file_desc.tenant_compatible_));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore from higher data version is");
  } else if (backup_file_desc.tenant_compatible_ < DATA_VERSION_4_1_0_0 && data_version >= DATA_VERSION_4_1_0_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore from version 4.0 is not allowd", K(ret), K(backup_file_desc.tenant_compatible_), K(data_version));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore from version 4.0 is");
  }
  return ret;
}

int ObRestoreUtil::recycle_restore_job(const uint64_t tenant_id,
                               common::ObMySQLProxy &sql_proxy,
                               const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const int64_t job_id = job_info.get_job_id();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(trans.start(&sql_proxy, exec_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
  } else {
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(restore_op.init(&trans, tenant_id))) {
      LOG_WARN("failed to init restore op", KR(ret), K(tenant_id));
    } else if (OB_FAIL(restore_op.remove_job(job_id))) {
      LOG_WARN("failed to remove job", KR(ret), K(tenant_id), K(job_id));
    } else {
      ObHisRestoreJobPersistInfo history_info;
      ObRestoreProgressPersistInfo restore_progress;
      ObRestorePersistHelper persist_helper;
      ObRestoreJobPersistKey key;
      common::ObArray<share::ObLSRestoreProgressPersistInfo> ls_restore_progress_infos;
      key.tenant_id_ = tenant_id;
      key.job_id_ = job_info.get_job_id();
      if (OB_FAIL(persist_helper.init(tenant_id))) {
        LOG_WARN("failed to init persist helper", KR(ret), K(tenant_id));
      } else if (OB_FAIL(persist_helper.get_restore_process(
                     trans, key, restore_progress))) {
        LOG_WARN("failed to get restore progress", KR(ret), K(key));
      } else if (OB_FAIL(history_info.init_with_job_process(
                     job_info, restore_progress))) {
        LOG_WARN("failed to init history", KR(ret), K(job_info), K(restore_progress));
      } else if (history_info.is_restore_success()) { // restore succeed, no need to record comment
      } else if (OB_FAIL(persist_helper.get_all_ls_restore_progress(trans, ls_restore_progress_infos))) {
        LOG_WARN("failed to get ls restore progress", K(ret));
      } else {
        int64_t pos = 0;
        ARRAY_FOREACH_X(ls_restore_progress_infos, i, cnt, OB_SUCC(ret)) {
          const ObLSRestoreProgressPersistInfo &ls_restore_info = ls_restore_progress_infos.at(i);
          if (ls_restore_info.status_.is_restore_failed()) {
            if (OB_FAIL(databuff_printf(history_info.comment_.ptr(), history_info.comment_.capacity(), pos,
                                        "%s;", ls_restore_info.comment_.ptr()))) {
              if (OB_SIZE_OVERFLOW == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("failed to databuff printf comment", K(ret));
              }
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(persist_helper.insert_restore_job_history(
                     trans, history_info))) {
        LOG_WARN("failed to insert restore job history", KR(ret), K(history_info));
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObRestoreUtil::recycle_restore_job(common::ObMySQLProxy &sql_proxy,
                          const share::ObPhysicalRestoreJob &job_info,
                          const ObHisRestoreJobPersistInfo &history_info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const int64_t job_id = job_info.get_job_id();
  const int64_t tenant_id = job_info.get_restore_key().tenant_id_;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObRestorePersistHelper persist_helper;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(trans.start(&sql_proxy, exec_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(persist_helper.init(tenant_id))) {
    LOG_WARN("failed to init persist helper", KR(ret));
   } else if (OB_FAIL(persist_helper.insert_restore_job_history(trans, history_info))) {
    LOG_WARN("failed to insert restore job history", KR(ret), K(history_info));
  } else {
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(restore_op.init(&trans, tenant_id))) {
      LOG_WARN("failed to init restore op", KR(ret), K(tenant_id));
    } else if (OB_FAIL(restore_op.remove_job(job_id))) {
      LOG_WARN("failed to remove job", KR(ret), K(tenant_id), K(job_id));
    } else if (is_sys_tenant(tenant_id)) {
      //finish __all_rootservice_job
      int tmp_ret = PHYSICAL_RESTORE_SUCCESS == job_info.get_status() ? OB_SUCCESS : OB_ERROR;
      if (OB_FAIL(RS_JOB_COMPLETE(job_id, tmp_ret, trans))) {
        LOG_WARN("failed to complete job", KR(ret), K(job_id));
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
    }
  }
  return ret;
}
int ObRestoreUtil::get_user_restore_job_history(common::ObISQLClient &sql_client,
                                          const uint64_t user_tenant_id,
                                         const uint64_t initiator_tenant_id,
                                         const int64_t initiator_job_id,
                                         ObHisRestoreJobPersistInfo &history_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id)
                  || OB_INVALID_TENANT_ID == initiator_tenant_id
                  || 0 > initiator_job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id),
    K(initiator_job_id), K(initiator_tenant_id));
  } else {
    ObRestorePersistHelper user_persist_helper;
    if (OB_FAIL(user_persist_helper.init(user_tenant_id))) {
      LOG_WARN("failed to init persist helper", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(user_persist_helper.get_restore_job_history(
                   sql_client, initiator_job_id, initiator_tenant_id,
                   history_info))) {
      LOG_WARN("failed to get restore progress", KR(ret), K(initiator_job_id), K(initiator_tenant_id));
    }
  }
  return ret;
}

int ObRestoreUtil::get_restore_ls_palf_base_info(
    const share::ObPhysicalRestoreJob &job_info, const ObLSID &ls_id,
    palf::PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataStore store;
  const common::ObSArray<share::ObBackupSetPath> &backup_set_array = 
    job_info.get_multi_restore_path_list().get_backup_set_path_list();
  const int64_t idx = backup_set_array.count() - 1;
  storage::ObLSMetaPackage ls_meta_package;
  if (idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup_set_array can't empty", KR(ret), K(job_info));
  } else if (OB_FAIL(store.init(backup_set_array.at(idx).ptr()))) {
    LOG_WARN("fail to init backup data store", KR(ret));
  } else if (OB_FAIL(store.read_ls_meta_infos(ls_id, ls_meta_package))) {
    LOG_WARN("fail to read backup set info", KR(ret));
  } else if (!ls_meta_package.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup set info", KR(ret), K(ls_meta_package));
  } else {
    palf_base_info = ls_meta_package.palf_meta_;
    LOG_INFO("[RESTORE] get restore ls palf base info", K(palf_base_info));
  }
  return ret;
}

int ObRestoreUtil::check_physical_restore_finish(
    common::ObISQLClient &proxy, const int64_t job_id, bool &is_finish, bool &is_failed) {
  int ret = OB_SUCCESS;
  is_failed = false;
  is_finish = false;
  ObSqlString sql;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  int64_t real_length = 0;
  HEAP_VAR(ObMySQLProxy::ReadResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    int64_t cnt = 0;
    if (OB_FAIL(sql.assign_fmt("select status from %s where tenant_id=%lu and job_id=%ld",
        OB_ALL_RESTORE_JOB_HISTORY_TNAME, OB_SYS_TENANT_ID, job_id))) {
      LOG_WARN("failed to assign fmt", K(ret));
    } else if (OB_FAIL(proxy.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
      LOG_WARN("failed to exec sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next", K(ret), K(job_id));
      }
    } else {
      EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
      if (OB_SUCC(ret)) {
        is_finish = true;
        is_failed = 0 == STRCMP(status_str, "FAIL");
      }
    }
  }
  return ret;
}

int ObRestoreUtil::get_restore_tenant_cpu_count(
    common::ObMySQLProxy &proxy, const uint64_t tenant_id, double &cpu_count)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableOperator unit_op;
  common::ObArray<share::ObResourcePool> pools;
  common::ObArray<uint64_t> unit_config_ids;
  common::ObArray<ObUnitConfig> configs;
  if (OB_FAIL(unit_op.init(proxy))) {
    LOG_WARN("failed to init proxy", K(ret));
  } else if (OB_FAIL(unit_op.get_resource_pools(tenant_id, pools))) {
    LOG_WARN("failed to get resource pool", K(ret), K(tenant_id));
  }
  ARRAY_FOREACH(pools, i) {
    if (OB_FAIL(unit_config_ids.push_back(pools.at(i).unit_config_id_))) {
      LOG_WARN("failed to push back unit config", K(ret));
    }
  }
  if (FAILEDx(unit_op.get_unit_configs(unit_config_ids, configs))) {
    LOG_WARN("failed to get unit configs", K(ret));
  }
  double max_cpu = OB_MAX_CPU_NUM;
  ARRAY_FOREACH(configs, i) {
    max_cpu = std::min(max_cpu, configs.at(i).max_cpu());
  }
  if (OB_SUCC(ret)) {
    cpu_count = max_cpu;
  }
  return ret;
}

int ObRestoreUtil::convert_restore_timestamp_to_scn_(
    const ObString &timestamp,
    const common::ObTimeZoneInfoWrap &time_zone_wrap,
    share::SCN &scn)
{
  int ret = OB_SUCCESS;
  uint64_t scn_value = 0;
  const ObTimeZoneInfo *time_zone_info = time_zone_wrap.get_time_zone_info();
  if (timestamp.empty() || !time_zone_wrap.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid time zone wrap", K(ret));
  } else if (OB_FAIL(ObTimeConverter::str_to_scn_value(timestamp, time_zone_info, time_zone_info, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT, true/*oracle mode*/, scn_value))) {
    LOG_WARN("failed to str to scn value", K(ret), K(timestamp), K(time_zone_info));
  } else if (OB_FAIL(scn.convert_for_sql(scn_value))) {
    LOG_WARN("failed to convert for sql scn", K(ret), K(scn_value));
  }
  return ret;
}

int ObRestoreUtil::get_backup_sys_time_zone_(
    const ObIArray<ObString> &tenant_path_array,
    common::ObTimeZoneInfoWrap &time_zone_wrap)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(tenant_path_array, i, cnt, OB_SUCC(ret)) {
    const ObString &tenant_path = tenant_path_array.at(i);
    storage::ObBackupDataStore store;
    share::ObBackupDest backup_dest;
    ObBackupFormatDesc format_desc;
    if (OB_FAIL(backup_dest.set(tenant_path.ptr()))) {
      LOG_WARN("fail to set backup dest", K(ret), K(tenant_path));
    } else if (OB_FAIL(store.init(backup_dest))) {
      LOG_WARN("failed to init backup store", K(ret), K(tenant_path));
    } else if (OB_FAIL(store.read_format_file(format_desc))) {
      LOG_WARN("failed to read format file", K(ret), K(store));
    } else if (ObBackupDestType::DEST_TYPE_BACKUP_DATA != format_desc.dest_type_) {
      LOG_INFO("skip log dir", K(tenant_path), K(format_desc));
    } else if (OB_FAIL(store.get_backup_sys_time_zone_wrap(time_zone_wrap))) {
      LOG_WARN("fail to get locality_info", K(ret));
    } else {
      break;
    }
  }
  return ret;
}