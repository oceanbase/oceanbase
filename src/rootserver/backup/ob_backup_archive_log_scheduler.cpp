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

#define USING_LOG_PREFIX RS
#include "rootserver/backup/ob_backup_archive_log_scheduler.h"
#include "rootserver/backup/ob_backup_data_mgr.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/backup/ob_tenant_name_mgr.h"
#include "share/backup/ob_backup_manager.h"
#include "share/backup/ob_backup_path.h"
#include "archive/ob_archive_path.h"
#include "archive/ob_archive_file_utils.h"
#include "lib/restore/ob_storage.h"
#include "lib/hash/ob_hashset.h"

#include <algorithm>

using namespace oceanbase::archive;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

namespace oceanbase {
namespace rootserver {

int64_t ObBackupArchiveLogIdling::get_idle_interval_us()
{
  const int64_t idle_time = GCONF.backup_log_archive_checkpoint_interval;
  return idle_time;
}

ObBackupArchiveLogScheduler::ObBackupArchiveLogScheduler()
    : is_inited_(false),
      is_working_(false),
      need_fetch_sys_tenant_ts_(true),
      cur_sys_tenant_checkpoint_ts_(0),
      idling_(stop_),
      server_mgr_(NULL),
      rpc_proxy_(NULL),
      sql_proxy_(NULL),
      backup_lease_service_(NULL),
      allocator_(),
      archive_round_set_(),
      tenant_queue_map_(),
      server_status_map_()
{}

ObBackupArchiveLogScheduler::~ObBackupArchiveLogScheduler()
{
  allocator_.reset();
}

int ObBackupArchiveLogScheduler::init(ObServerManager& server_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
    common::ObMySQLProxy& sql_proxy, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup archive log scheduler init twice", KR(ret));
  } else if (OB_FAIL(create(thread_cnt, "BACKUP_ARCHIVE_LOG_SCHEDULER"))) {
    LOG_WARN("failed to create backup archive log scheduler thread", KR(ret));
  } else if (OB_FAIL(archive_round_set_.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create hash set", KR(ret));
  } else if (OB_FAIL(tenant_queue_map_.create(MAX_BUCKET_NUM, ObModIds::BACKUP))) {
    LOG_WARN("failed to create hash map", KR(ret));
  } else if (OB_FAIL(server_status_map_.create(MAX_BUCKET_NUM, ObModIds::BACKUP))) {
    LOG_WARN("failed to create server status map", KR(ret));
  } else if (OB_FAIL(round_stat_.init())) {
    LOG_WARN("failed to init round stat", KR(ret));
  } else {
    server_mgr_ = &server_mgr;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchiveLogScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReentrantThread::start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

void ObBackupArchiveLogScheduler::stop()
{
  if (IS_NOT_INIT) {
    LOG_WARN("not init");
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

int ObBackupArchiveLogScheduler::idle()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", KR(ret));
  } else {
    LOG_INFO("backup archive log idling", "idle time", idling_.get_idle_interval_us());
  }
  return ret;
}

void ObBackupArchiveLogScheduler::wakeup()
{
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("backup archive log scheduler do not init");
  } else {
    idling_.wakeup();
    LOG_INFO("backup archive log scheduler wakeup");
  }
}

void ObBackupArchiveLogScheduler::run3()
{
  int ret = OB_SUCCESS;
  bool in_history = true;
  int64_t round = 0;
  ObBackupDest src_backup_dest, dst_backup_dest;
  ObCurTraceId::init(GCONF.self_addr_);
  while (!stop_) {
    src_backup_dest.reset();
    dst_backup_dest.reset();
    if (OB_FAIL(idle())) {
      LOG_WARN("idle failed", K(ret));
    }
    if (OB_FAIL(check_can_do_work())) {
      LOG_WARN("failed to check can do work", KR(ret));
    } else if (OB_FAIL(get_next_round(round))) {
      LOG_WARN("failed to get next round", KR(ret));
    } else if (OB_FAIL(check_is_history_round(round, in_history))) {
      LOG_WARN("failed to check is history round", KR(ret), K(round));
    } else if (in_history && OB_FAIL(round_stat_.set_current_round_in_history())) {
      LOG_WARN("failed to set current round in history", KR(ret));
    } else if (OB_FAIL(get_backup_dest_info(OB_SYS_TENANT_ID, round, src_backup_dest, dst_backup_dest))) {
      LOG_WARN("failed to get backup dest info", KR(ret));
    } else if (OB_FAIL(update_extern_backup_info(src_backup_dest, dst_backup_dest))) {
      LOG_WARN("failed to update extern backup info", KR(ret));
    } else if (OB_FAIL(do_schedule(round, in_history, src_backup_dest, dst_backup_dest))) {
      LOG_WARN("failed to do schedule", KR(ret), K(round));
    }
  }
}

int ObBackupArchiveLogScheduler::set_server_busy(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ServerStatus status;
  status.status_ = ServerStatus::STATUS_BUSY;
  status.lease_time_ = ObTimeUtil::current_time();
  if (OB_FAIL(server_status_map_.set_refactored(addr, status, 1))) {
    LOG_WARN("failed to set server free", KR(ret));
  } else {
    LOG_INFO("set server busy", K(addr));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_server_free(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ServerStatus status;
  status.status_ = ServerStatus::STATUS_FREE;
  status.lease_time_ = ObTimeUtil::current_time();
  if (OB_FAIL(server_status_map_.set_refactored(addr, status, 1))) {
    LOG_WARN("failed to set server free", KR(ret));
  } else {
    LOG_INFO("set server free", K(addr));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_all_tenant_need_fetch_new()
{
  int ret = OB_SUCCESS;
  int64_t round = 0;
  ObArray<uint64_t> tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(get_next_round(round))) {
    LOG_WARN("failed to get next round", KR(ret));
  } else if (OB_FAIL(get_all_tenant_ids(round, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(round));
  } else if (OB_FAIL(prepare_tenant_if_needed(tenant_ids, true))) {
    LOG_WARN("set all tenant need fetch new", K(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_checkpoint_ts(const uint64_t tenant_id, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(checkpoint_ts));
  } else {
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // It may be because of the cut
      ret = hash_ret;
      LOG_WARN("tenant id do not exist", KR(ret));
    } else if (OB_SUCCESS == hash_ret) {
      queue_ptr->checkpoint_ts_ = checkpoint_ts;
    } else {
      ret = hash_ret;
      LOG_WARN("get refactored error", KR(ret), K(hash_ret));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_checkpoint_ts(const uint64_t tenant_id, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  checkpoint_ts = 0;
  TenantTaskQueue* queue_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(tenant_id), K(checkpoint_ts));
  } else {
    hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // It may be because of the cut
      ret = hash_ret;
      LOG_WARN("tenant id do not exist", KR(ret));
    } else if (OB_SUCCESS == hash_ret) {
      checkpoint_ts = queue_ptr->checkpoint_ts_;
    } else {
      ret = hash_ret;
      LOG_WARN("get refactored error", KR(ret), K(hash_ret));
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::set_pg_finish(
    const int64_t round, const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (round <= 0 && OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set pg finish move get invalid argument", KR(ret), K(round), K(tenant_id));
  } else if (OB_FAIL(round_stat_.mark_pg_list_finished(round, tenant_id, pg_list))) {
    LOG_WARN("failed to mark pg list finished", KR(ret), K(round), K(tenant_id));
  } else {
    LOG_INFO("set pg list finish", K(round), K(tenant_id), K(pg_list));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_can_do_work()
{
  int ret = OB_SUCCESS;
  bool can_do_work = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(backup_lease_service_->get_lease_status(can_do_work))) {
    LOG_WARN("failed to get lease status", KR(ret));
  } else if (!can_do_work) {
    ret = OB_RS_STATE_NOT_ALLOW;
    LOG_INFO("leader may switch, can not do work", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_status_line_count(const uint64_t tenant_id, int64_t& status_count)
{
  int ret = OB_SUCCESS;
  status_count = 0;
  ObSqlString sql;
  sqlclient::ObMySQLResult* result = NULL;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where tenant_id=%lu ",
            OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
            tenant_id))) {
      LOG_WARN("failed to init backup info sql", KR(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", KR(ret), K(sql));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", KR(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", status_count, int64_t);
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_backup_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t status_line_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_status_line_count(tenant_id, status_line_count))) {
    LOG_WARN("failed to get status line count", KR(ret), K(tenant_id));
  } else if (status_line_count > 0) {
    // do nothing
  } else if (OB_FAIL(insert_backup_archive_info(tenant_id))) {
    LOG_WARN("failed to insert backup archive info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::insert_backup_archive_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s)"
                                    " VALUES (%lu, %ld, 0, %ld, usec_to_time(0), usec_to_time(0), 'STOP')",
                 OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_COPY_ID,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_STATUS,
                 tenant_id,
                 OB_START_INCARNATION,
                 OB_START_COPY_ID))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_backup_dest_info(
    const uint64_t tenant_id, const int64_t round, ObBackupDest& src, ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  char dst_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(get_src_backup_dest(tenant_id, round, src))) {
    LOG_WARN("failed to get backup dest", KR(ret), K(tenant_id), K(round));
  } else if (OB_FAIL(get_dst_backup_dest(tenant_id, dst_backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to set backup backup dest if needed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dst.set(dst_backup_dest))) {
    LOG_WARN("failed to set dst backup dest", KR(ret), K(dst_backup_dest));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_extern_backup_info(
    const share::ObBackupDest& src, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest src_dest, dst_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(src_dest.set(src, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret));
  } else if (OB_FAIL(dst_dest.set(dst, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret));
  } else if (OB_FAIL(update_tenant_info(src_dest, dst_dest))) {
    LOG_WARN("failed to update tenant info", KR(ret), K(src_dest), K(dst_dest));
  } else if (OB_FAIL(update_tenant_name_info(src_dest, dst_dest))) {
    LOG_WARN("failed to update tenant name info", KR(ret), K(src_dest), K(dst_dest));
  } else if (OB_FAIL(update_clog_info_list(src_dest, dst_dest))) {
    LOG_WARN("failed to update clog info list", KR(ret), K(src_dest), K(dst_dest));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_info(
    const share::ObClusterBackupDest& src_dest, const share::ObClusterBackupDest& dst_dest)
{
  int ret = OB_SUCCESS;
  ObExternTenantInfoMgr src_mgr, dst_mgr;
  ObArray<ObExternTenantInfo> tenant_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.init(src_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init src extern tenant info mgr", KR(ret), K(src_dest));
  } else if (OB_FAIL(dst_mgr.init(dst_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init src extern tenant info mgr", KR(ret), K(dst_dest));
  } else if (OB_FAIL(src_mgr.get_extern_tenant_infos(tenant_infos))) {
    LOG_WARN("failed to get extern tenant infos", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_infos.count(); ++i) {
      const ObExternTenantInfo& tenant_info = tenant_infos.at(i);
      if (OB_FAIL(dst_mgr.add_tenant_info(tenant_info))) {
        LOG_WARN("failed to add tenant info", KR(ret), K(tenant_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dst_mgr.upload_tenant_infos())) {
        LOG_WARN("failed to upload tenant infos", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_name_info(
    const share::ObClusterBackupDest& src_dest, const share::ObClusterBackupDest& dst_dest)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = 0;
  ObTenantNameSimpleMgr tenant_name_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(tenant_name_mgr.init())) {
    LOG_WARN("failed to init tenant name simple mgr", KR(ret));
  } else if (OB_FAIL(tenant_name_mgr.read_backup_file(src_dest))) {
    LOG_WARN("failed to read backup file", KR(ret), K(src_dest));
  } else if (FALSE_IT(schema_version = tenant_name_mgr.get_schema_version())) {
    // do nothing
  } else if (OB_FAIL(tenant_name_mgr.complete(schema_version))) {
    LOG_WARN("failed to complete schema version", KR(ret), K(schema_version));
  } else if (OB_FAIL(tenant_name_mgr.write_backup_file(dst_dest))) {
    LOG_WARN("failed to write backup file", KR(ret), K(dst_dest));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_clog_info_list(
    const share::ObClusterBackupDest& src_dest, const share::ObClusterBackupDest& dst_dest)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObStorageUtil util(true /*need retry*/);
  ObBackupPath src_path, dst_path;
  ObArray<ObString> file_names;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info(src_dest, src_path))) {
    LOG_WARN("failed to get src cluster clog info path", KR(ret), K(src_dest));
  } else if (OB_FAIL(util.list_files(src_path.get_obstr(), src_dest.get_storage_info(), allocator, file_names))) {
    LOG_WARN("failed to list files", KR(ret), K(src_path), K(src_dest));
  } else if (file_names.empty()) {
    LOG_INFO("clog info list is empty", K(file_names));
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info(dst_dest, dst_path))) {
    LOG_WARN("failed to get dst cluster clog info path", KR(ret), K(dst_dest));
  } else if (OB_FAIL(util.mkdir(dst_path.get_obstr(), dst_dest.get_storage_info()))) {
    LOG_WARN("failed to make clog info directory", KR(ret), K(dst_path), K(dst_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
      const ObString& file_name = file_names.at(i);
      ObBackupPath src_path, dst_path;
      int64_t file_length = 0;
      int64_t read_length = 0;
      char* buf = NULL;
      if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info_file_path(src_dest, file_name, src_path))) {
        LOG_WARN("failed to get src cluster clog info file path", KR(ret), K(src_dest));
      } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_info_file_path(dst_dest, file_name, dst_path))) {
        LOG_WARN("failed to get dst clust4er clog info file path", KR(ret), K(dst_dest));
      } else if (OB_FAIL(util.get_file_length(src_path.get_obstr(), src_dest.get_storage_info(), file_length))) {
        LOG_WARN("failled to get file length", KR(ret), K(src_path), K(src_dest));
      } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(util.read_single_file(
                     src_path.get_obstr(), src_dest.get_storage_info(), buf, file_length, read_length))) {
        LOG_WARN("failed to read single file", KR(ret), K(src_path), K(src_dest));
      } else if (read_length != file_length) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read len not expected", KR(ret), K(read_length), K(file_length));
      } else if (OB_FAIL(util.write_single_file(dst_path.get_obstr(), dst_dest.get_storage_info(), buf, file_length))) {
        LOG_WARN("failed to write single file", KR(ret), K(dst_path), K(dst_dest));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_clog_backup_info(const uint64_t tenant_id, const int64_t archive_round,
    const share::ObBackupDest& dst, const share::ObLogArchiveStatus::STATUS& status)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;
  ObLogArchiveBackupInfo info;
  info.status_.tenant_id_ = tenant_id;
  info.status_.start_ts_ = 1;
  info.status_.checkpoint_ts_ = ObTimeUtil::current_time();  // TODO
  info.status_.incarnation_ = OB_START_INCARNATION;
  info.status_.round_ = archive_round;
  info.status_.status_ = status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant clog backup info get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dst.get_backup_dest_str(info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(dst));
  } else if (OB_FAIL(mgr.update_extern_log_archive_backup_info(info, *backup_lease_service_))) {
    LOG_WARN("failed to update extern log archive backup info", K(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_inner_table_cur_info(
    const uint64_t tenant_id, const int64_t archive_round, const int64_t checkpoint_ts, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObLogArchiveBackupInfo backup_info;
  ObLogArchiveBackupInfoMgr src_mgr, dst_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.set_copy_id(0))) {
    LOG_WARN("failed to set copy id for src mgr", KR(ret));
  } else if (OB_FAIL(dst_mgr.set_copy_id(OB_START_COPY_ID))) {
    LOG_WARN("failed to set copy id for dst mgr", KR(ret));
  } else if (OB_FAIL(src_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, backup_info))) {
    LOG_WARN("failed to get log archive backup info", KR(ret), K(tenant_id));
  } else if (archive_round != backup_info.status_.round_) {
    // do nothing
  } else if (FALSE_IT(backup_info.status_.checkpoint_ts_ = checkpoint_ts)) {
    // set checkpoint ts
  } else if (OB_FAIL(dst.get_backup_dest_str(backup_info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_info));
  } else if (OB_FAIL(dst_mgr.update_log_archive_backup_info(*sql_proxy_, backup_info))) {
    LOG_WARN("failed to update log archive backup info", KR(ret), K(backup_info));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::update_tenant_inner_table_his_info(
    const uint64_t tenant_id, const int64_t archive_round, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObLogArchiveBackupInfo info;
  ObLogArchiveBackupInfoMgr src_mgr, dst_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.set_copy_id(0))) {
    LOG_WARN("failed to set copy id for src mgr", KR(ret));
  } else if (OB_FAIL(dst_mgr.set_copy_id(OB_START_COPY_ID))) {
    LOG_WARN("failed to set copy id for dst mgr", KR(ret));
  } else if (OB_FAIL(src_mgr.get_log_archive_history_info(*sql_proxy_, tenant_id, archive_round, for_update, info))) {
    LOG_WARN("failed to get log archive history info", KR(ret), K(tenant_id), K(archive_round));
  } else if (FALSE_IT(info.status_.copy_id_ = OB_START_COPY_ID)) {
    // do nothing
  } else if (OB_FAIL(dst.get_backup_dest_str(info.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(info));
  } else if (OB_FAIL(dst_mgr.update_log_archive_status_history(*sql_proxy_, info, *backup_lease_service_))) {
    LOG_WARN("failed to update log archive status history", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_all_tenant_ids(
    const int64_t log_archive_round, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_backup_dest;
  ObTenantNameSimpleMgr tenant_name_mgr;
  hash::ObHashSet<uint64_t> tenant_id_set;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(tenant_id_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create tenant id set", KR(ret));
  } else if (OB_FAIL(get_src_backup_dest(OB_SYS_TENANT_ID, log_archive_round, backup_dest))) {
    LOG_WARN("failed to get backup dest", KR(ret), K(log_archive_round));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest));
  } else if (OB_FAIL(tenant_name_mgr.init())) {
    LOG_WARN("failed to init tenant name manager", KR(ret));
  } else if (OB_FAIL(tenant_name_mgr.read_backup_file(cluster_backup_dest))) {
    LOG_WARN("failed to read backup file", KR(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(tenant_name_mgr.get_tenant_ids(tenant_id_set))) {
    LOG_WARN("failed to get tenant ids", KR(ret));
  } else {
    typedef hash::ObHashSet<uint64_t>::iterator Iter;
    for (Iter iter = tenant_id_set.begin(); OB_SUCC(ret) && iter != tenant_id_set.end(); ++iter) {
      const uint64_t tenant_id = iter->first;
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("failed to push back tenant id", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_src_backup_dest(
    const uint64_t tenant_id, const int64_t round, share::ObBackupDest& backup_dest)
{
  int ret = OB_SUCCESS;
  backup_dest.reset();
  bool for_update = false;
  ObLogArchiveBackupInfo info;
  ObLogArchiveBackupInfoMgr archive_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round is not valid", KR(ret));
  } else if (OB_FAIL(archive_mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, info))) {
    LOG_WARN("failed to get log archive backup info", KR(ret), K(for_update), K(tenant_id));
  } else {
    if (info.status_.round_ == round) {
      if (OB_FAIL(backup_dest.set(info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(info));
      }
    } else {
      if (OB_FAIL(archive_mgr.get_log_archive_history_info(*sql_proxy_, tenant_id, round, for_update, info))) {
        LOG_WARN("failed to get log archive history info", KR(ret));
      } else if (OB_FAIL(backup_dest.set(info.backup_dest_))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(info));
      }
    }
  }
  return ret;
}

// TODO : what if noarchivelog
int ObBackupArchiveLogScheduler::get_current_log_archive_round(const uint64_t tenant_id, int64_t& archive_round)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObLogArchiveBackupInfo info;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is not valid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, info))) {
    LOG_WARN("failed to get log archive backup info", KR(ret), K(tenant_id));
  } else {
    archive_round = info.status_.round_;
    LOG_INFO("current log archive round", K(archive_round));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_log_archive_history_info(
    const uint64_t tenant_id, const int64_t round, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  bool for_update = false;
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is not valid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr.get_log_archive_history_info(*sql_proxy_, tenant_id, round, for_update, info))) {
    LOG_WARN("failed to get log archive history info", KR(ret), K(tenant_id), K(round));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::prepare_log_archive_round_if_needed(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObLogArchiveBackupInfo> infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (0 != archive_round_set_.size()) {
    LOG_INFO("still has archive round, no need to fetch");
  } else if (OB_FAIL(get_log_archive_info_list(tenant_id, infos))) {
    LOG_WARN("failed to get log archive infos", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      const ObLogArchiveBackupInfo& info = infos.at(i);
      const int64_t round = info.status_.round_;
      bool overwrite = true;
      if (OB_FAIL(archive_round_set_.set_refactored_1(round, overwrite))) {
        LOG_WARN("failed to set refactored", KR(ret), K(round));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::prepare_tenant_if_needed(
    const ObIArray<uint64_t>& tenant_ids, const bool need_fetch_new)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (tenant_ids.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      char* buf = NULL;
      TenantTaskQueue* queue_ptr = NULL;
      int hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(TenantTaskQueue))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate queue", KR(ret));
        } else {
          queue_ptr = new (buf) TenantTaskQueue;
          queue_ptr->tenant_id_ = tenant_id;
          queue_ptr->need_fetch_new_ = need_fetch_new;
          if (OB_FAIL(tenant_queue_map_.set_refactored(tenant_id, queue_ptr))) {
            LOG_WARN("failed to set map", KR(ret), K(tenant_id));
          }
        }
      } else if (OB_SUCCESS == hash_ret) {
        queue_ptr->need_fetch_new_ = need_fetch_new;
      } else {
        ret = hash_ret;
        LOG_WARN("get refactored error", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::prepare_server_if_needed(const common::ObIArray<common::ObAddr>& servers)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (servers.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr& addr = servers.at(i);
      ServerStatus status;
      int hash_ret = server_status_map_.get_refactored(addr, status);
      if (OB_SUCCESS == hash_ret) {
        continue;
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        status.status_ = ServerStatus::STATUS_FREE;
        status.lease_time_ = ObTimeUtil::current_time();
        if (OB_FAIL(server_status_map_.set_refactored(addr, status))) {
          LOG_WARN("failed to set server status", KR(ret), K(addr));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get refactored error", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_schedule(
    const int64_t round, const bool in_history, const share::ObBackupDest& src, const share::ObBackupDest& dst)
{
  int ret = OB_SUCCESS;
  int64_t checkpoint_ts = 0;
  ObArray<uint64_t> tenant_ids;
  bool all_one_round_finished = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(get_all_tenant_ids(round, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(round));
  } else if (OB_FAIL(get_sys_tenant_checkpoint_ts())) {
    LOG_WARN("failed to get sys tenant checkpoint ts", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      bool one_round_finished = true;
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(backup_lease_service_->check_lease())) {
        LOG_WARN("failed to check can backup", KR(ret));
      } else if (OB_FAIL(do_tenant_schedule(tenant_id, round, in_history, src, dst, one_round_finished))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("no server currently available", KR(ret), K(tenant_id));
          ret = OB_SUCCESS;
          break;
        }
      } else {
        all_one_round_finished &= one_round_finished;
      }
      if (OB_SUCC(ret) && one_round_finished) {
        if (OB_FAIL(update_tenant_clog_backup_info(tenant_id, round, dst, ObLogArchiveStatus::DOING))) {
          LOG_WARN("failed to update tenant clog backup info", KR(ret), K(tenant_id), K(round));
        } else if (in_history) {
          // do nothing
        } else if (OB_FAIL(check_backup_info(tenant_id))) {
          LOG_WARN("failed to check backup info", KR(ret), K(tenant_id));
        } else if (OB_FAIL(get_checkpoint_ts(tenant_id, checkpoint_ts))) {
          LOG_WARN("failed to get tenant checkpoint ts", KR(ret), K(tenant_id));
        } else if (OB_FAIL(update_tenant_inner_table_cur_info(tenant_id, round, checkpoint_ts, dst))) {
          LOG_WARN("failed to update tenant inner table current info", KR(ret), K(tenant_id), K(round));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (all_one_round_finished && !in_history) {
        if (OB_FAIL(check_backup_info(OB_SYS_TENANT_ID))) {
          LOG_WARN("failed to check sys tenant backup info", KR(ret));
        } else if (OB_FAIL(update_tenant_inner_table_cur_info(
                       OB_SYS_TENANT_ID, round, cur_sys_tenant_checkpoint_ts_, dst))) {
          LOG_WARN("failed to update sys tenant inner table current info", KR(ret), K(round));
        } else {
          need_fetch_sys_tenant_ts_ = true;
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_next_round(int64_t& round)
{
  int ret = OB_SUCCESS;
  int64_t prev_round = 0;
  bool round_changed = false;
  ObBackupDest src_backup_dest;
  ObBackupDest dst_backup_dest;
  ObArray<uint64_t> prev_tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log do not init", KR(ret));
  } else if (OB_FAIL(round_stat_.get_next_round(round, round_changed))) {
    LOG_WARN("failed to get next round", KR(ret));
  } else if (!round_changed) {
    LOG_INFO("round do not change", K(round), K(round_changed));
  } else if (FALSE_IT(prev_round = round - 1)) {
    // get previous round
  } else if (OB_FAIL(get_backup_dest_info(OB_SYS_TENANT_ID, prev_round, src_backup_dest, dst_backup_dest))) {
    LOG_WARN("failed to get backup dest info", KR(ret));
  } else if (OB_FAIL(get_all_tenant_ids(prev_round, prev_tenant_ids))) {
    LOG_WARN("failed to get prev tenant ids", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < prev_tenant_ids.count(); ++i) {
      const uint64_t tenant_id = prev_tenant_ids.at(i);
      if (OB_FAIL(update_tenant_clog_backup_info(
              tenant_id, prev_round, dst_backup_dest, ObLogArchiveStatus::STATUS::STOP))) {
        LOG_WARN("failed to update extern log archive backup info", KR(ret), K(dst_backup_dest));
      } else if (OB_FAIL(update_tenant_inner_table_his_info(tenant_id, prev_round, dst_backup_dest))) {
        LOG_WARN("failed to update tenant inner table history info", KR(ret), K(tenant_id), K(prev_round));
      }
    }
    wakeup();
  }
  return ret;
}

int ObBackupArchiveLogScheduler::do_tenant_schedule(const uint64_t tenant_id, const int64_t archive_round,
    const bool in_history, const share::ObBackupDest& src, const share::ObBackupDest& dst, bool& one_round_finished)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest backup_dest;
  ObArray<ObPGKey> pg_list;
  common::ObAddr server;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(backup_dest.set(src, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret));
  } else if (OB_FAIL(select_one_server(server))) {
    LOG_INFO("no server available currently", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_batch_pg_list(backup_dest, archive_round, tenant_id, pg_list))) {
    LOG_WARN("failed to get clog pg list", KR(ret), K(backup_dest), K(archive_round), K(tenant_id));
  } else if (pg_list.empty() && OB_FAIL(set_server_free(server))) {
    LOG_WARN("failed to set server free", KR(ret), K(server));
  } else if (OB_FAIL(
                 generate_backup_archive_log_task(tenant_id, archive_round, in_history, src, dst, server, pg_list))) {
    LOG_WARN("failed to generate backup archive log task",
        KR(ret),
        K(tenant_id),
        K(archive_round),
        K(src),
        K(dst),
        K(server));
  } else {
    one_round_finished = pg_list.empty();
    LOG_INFO("generate backup archive log task success", K(tenant_id), K(in_history), K(pg_list));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_log_archive_info_list(
    const uint64_t tenant_id, common::ObIArray<share::ObLogArchiveBackupInfo>& round_list)
{
  int ret = OB_SUCCESS;
  round_list.reset();
  const bool for_update = false;
  ObLogArchiveBackupInfoMgr mgr;
  ObLogArchiveBackupInfo cur_info;
  ObArray<ObLogArchiveBackupInfo> his_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, cur_info))) {
    LOG_WARN("failed to get log archive backup info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(round_list.push_back(cur_info))) {
    LOG_WARN("failed to push back", KR(ret), K(cur_info));
  } else if (OB_FAIL(mgr.get_log_archvie_history_infos(*sql_proxy_, tenant_id, for_update, his_infos))) {
    LOG_WARN("failed to get log archive history infos", KR(ret), K(tenant_id));
  } else if (OB_FAIL(add_array(round_list, his_infos))) {
    LOG_WARN("failed to push back history infos", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_batch_pg_list(const share::ObClusterBackupDest& backup_dest, const int64_t round,
    const uint64_t tenant_id, common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  pg_list.reset();
  ObLogArchiveBackupInfoMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else {
    int64_t checkpoint_ts = 0;
    ObArray<ObPGKey> pg_keys;
    TenantTaskQueue* queue_ptr = NULL;
    int hash_ret = tenant_queue_map_.get_refactored(tenant_id, queue_ptr);
    if (OB_SUCCESS == hash_ret) {
      if (queue_ptr->queue_.is_empty() && queue_ptr->need_fetch_new_) {
        if (OB_FAIL(get_clog_pg_list_v2(backup_dest, round, tenant_id, pg_keys))) {
          LOG_WARN("failed to get clog pg list", KR(ret), K(tenant_id), K(pg_keys.count()));
        } else if (pg_keys.empty()) {
          // do nothing
        } else if (OB_FAIL(push_pg_list(queue_ptr->queue_, round, pg_keys))) {
          LOG_WARN("failed to push pg list", KR(ret), K(tenant_id));
        } else if (OB_FAIL(round_stat_.add_pg_list(round, tenant_id, pg_keys))) {
          LOG_WARN("failed to add pg list", KR(ret), K(round), K(tenant_id), K(pg_keys));
        } else if (OB_FAIL(mgr.get_log_archive_checkpoint(*sql_proxy_, tenant_id, checkpoint_ts))) {
          LOG_WARN("failed to get log archive checkpoint", KR(ret), K(tenant_id));
        } else if (OB_FAIL(set_checkpoint_ts(tenant_id, checkpoint_ts))) {
          LOG_WARN("failed to set checkpoint ts", KR(ret));
        } else {
          queue_ptr->need_fetch_new_ = false;
          LOG_INFO("no need to fetch new pg list until finish", K(tenant_id), K(checkpoint_ts), K(pg_keys.count()));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant queue should exist", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pop_pg_list(queue_ptr->queue_, pg_list))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("no pg left to move", KR(ret), K(tenant_id));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::push_pg_list(
    common::ObSpLinkQueue& queue, const int64_t log_archive_round, const common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (pg_keys.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      const ObPGKey& pg_key = pg_keys.at(i);
      SimplePGWrapper* pg = NULL;
      if (OB_FAIL(alloc_pg_task(pg, log_archive_round, pg_key))) {
        LOG_WARN("failed to alloc pg task", KR(ret), K(pg), K(log_archive_round), K(pg_key));
      } else if (OB_FAIL(queue.push(pg))) {
        LOG_WARN("failed to push back pg", KR(ret), K(pg));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::pop_pg_list(common::ObSpLinkQueue& queue, common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  pg_keys.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (queue.is_empty()) {
    ret = OB_ITER_END;
    LOG_INFO("no pg available, wait until next time", KR(ret));
  } else {
    int64_t cnt = 0;
    while (OB_SUCC(ret) && !queue.is_empty()) {
      ObLink* link = NULL;
      SimplePGWrapper* pg = NULL;
      if (OB_FAIL(queue.pop(link))) {
        LOG_WARN("failed to pop task", KR(ret), K(link));
      } else if (OB_ISNULL(link)) {
        LOG_WARN("link is null", KR(ret), K(link));
      } else {
        pg = static_cast<SimplePGWrapper*>(link);
        const ObPGKey& pg_key = pg->pg_key_;
        if (OB_FAIL(pg_keys.push_back(pg_key))) {
          LOG_WARN("failed to push back pg key", K(pg_key));
        } else if (OB_FAIL(free_pg_task(pg))) {
          LOG_WARN("failed to free pg task", KR(ret), K(pg));
        } else {
          ++cnt;
        }
      }
      if (cnt >= MAX_BATCH_SIZE) {
        break;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::alloc_pg_task(
    SimplePGWrapper*& pg, const int64_t archive_round, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::BACKUP);
  void* buf = ob_malloc(sizeof(SimplePGWrapper), attr);
  pg = new (buf) SimplePGWrapper;
  pg->archive_round_ = archive_round;
  pg->pg_key_ = pg_key;
  return ret;
}

int ObBackupArchiveLogScheduler::free_pg_task(SimplePGWrapper* pg)
{
  int ret = OB_SUCCESS;
  ob_free(pg);
  return ret;
}

int ObBackupArchiveLogScheduler::get_clog_pg_list(const share::ObClusterBackupDest& backup_dest,
    const int64_t log_archive_round, const uint64_t tenant_id, common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  pg_keys.reset();
  ObBackupListDataMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(mgr.init(backup_dest, log_archive_round, tenant_id))) {
    LOG_WARN("failed to init backup list data mgr", KR(ret), K(backup_dest), K(log_archive_round), K(tenant_id));
  } else if (OB_FAIL(mgr.get_clog_pkey_list(pg_keys))) {
    LOG_WARN("failed to get clog pkey list", KR(ret), K(backup_dest), K(tenant_id));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_clog_pg_list_v2(const share::ObClusterBackupDest& backup_dest,
    const int64_t log_archive_round, const uint64_t tenant_id, common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need_retry*/);
  pg_keys.reset();
  ObString storage_info(backup_dest.get_storage_info());
  ObArenaAllocator allocator;
  ObArray<ObString> file_names;
  ObBackupPath path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_prefix(backup_dest, tenant_id, log_archive_round, path))) {
    LOG_WARN("failed to get clog archive key prefix", KR(ret), K(backup_dest), K(log_archive_round), K(tenant_id));
  } else if (OB_FAIL(util.list_files(path.get_obstr(), storage_info, allocator, file_names))) {
    LOG_WARN("failed to list files", KR(ret), K(path), K(storage_info));
  } else if (file_names.empty()) {
    LOG_INFO("clog pg list is empty", KR(ret), K(path), K(storage_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
      const ObString& file_name = file_names.at(i);
      common::ObPGKey pg_key;
      bool match = true;
      if (OB_FAIL(extract_pg_key_from_file_name(file_name, pg_key, match))) {
        LOG_WARN("failed to get pg key from file name", KR(ret), K(file_name));
      } else if (!match) {
        LOG_WARN("skip invalid archive key file name", K(file_name));
      } else if (OB_FAIL(pg_keys.push_back(pg_key))) {
        LOG_WARN("failed to push back pg key", KR(ret), K(pg_key));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::extract_pg_key_from_file_name(
    const common::ObString& file_name, common::ObPGKey& pg_key, bool& match)
{
  int ret = OB_SUCCESS;
  match = true;
  pg_key.reset();
  uint64_t table_id = 0;
  int64_t partition_id = 0;
  const char* str = file_name.ptr();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (file_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_name));
  } else {
    int64_t i = 0;
    uint64_t tmp_table_id = 0;
    int64_t tmp_partition_id = 0;
    for (i = 0; match && i < file_name.length(); ++i) {
      if (!isdigit(str[i]) && '_' != str[i]) {
        match = false;
      } else if ('_' == str[i]) {
        break;
      } else {
        if (tmp_table_id > UINT64_MAX / 10 || (tmp_table_id == UINT64_MAX / 10 && (str[i] - '0') > UINT64_MAX % 10)) {
          ret = OB_DECIMAL_OVERFLOW_WARN;
          LOG_WARN("table id is not valid", KR(ret), K(tmp_table_id));
        } else {
          tmp_table_id = tmp_table_id * 10 + (str[i] - '0');
        }
      }
    }
    for (i = i + 1 /*skip '_' */; match && i < file_name.length(); ++i) {
      if (!isdigit(str[i]) && '\0' != str[i]) {
        match = false;
      } else if ('\0' == str[i]) {
        break;
      } else {
        if (tmp_partition_id > INT64_MAX / 10 ||
            (tmp_partition_id == INT64_MAX / 10 && (str[i] - '0') > INT64_MAX % 10)) {
          ret = OB_DECIMAL_OVERFLOW_WARN;
          LOG_WARN("partition id is not valid", KR(ret), K(tmp_partition_id));
        } else {
          tmp_partition_id = tmp_partition_id * 10 + (str[i] - '0');
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!match) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid archive key", KR(ret), K(file_name));
      } else {
        table_id = tmp_table_id;
        partition_id = tmp_partition_id;
        if (OB_FAIL(pg_key.init(table_id, partition_id, 0))) {
          LOG_WARN("failed to init pg key", KR(ret), K(table_id), K(partition_id));
        }
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::generate_backup_archive_log_task(const uint64_t tenant_id, const int64_t archive_round,
    const bool in_history, const share::ObBackupDest& src, const share::ObBackupDest& dst, const common::ObAddr& server,
    const common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  obrpc::ObBackupArchiveLogBatchArg arg;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (pg_keys.empty()) {
    // do nothing
  } else if (OB_FAIL(build_backup_archive_log_arg(tenant_id, archive_round, in_history, src, dst, pg_keys, arg))) {
    LOG_WARN("failed to build backup archive arg", KR(ret), K(archive_round), K(tenant_id));
  } else if (OB_FAIL(rpc_proxy_->to(server).backup_archive_log_batch(arg))) {
    LOG_WARN("failed to send backup archive log batch rpc", KR(ret), K(arg));
  } else {
    LOG_INFO("send backup archive log rpc success", K(server), K(in_history), K(src), K(dst), K(archive_round));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::build_backup_archive_log_arg(const uint64_t tenant_id, const int64_t archive_round,
    const bool in_history, const share::ObBackupDest& src, const share::ObBackupDest& dst,
    const common::ObIArray<common::ObPGKey>& pg_keys, obrpc::ObBackupArchiveLogBatchArg& arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || archive_round <= 0 || !src.is_valid() || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build rpc arg get invalid argument", KR(ret), K(tenant_id), K(archive_round), K(src), K(dst));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.archive_round_ = archive_round;
    arg.in_history_ = in_history;
    STRNCPY(arg.src_root_path_, src.root_path_, OB_MAX_BACKUP_PATH_LENGTH);
    STRNCPY(arg.src_storage_info_, src.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
    STRNCPY(arg.dst_root_path_, dst.root_path_, OB_MAX_BACKUP_PATH_LENGTH);
    STRNCPY(arg.dst_storage_info_, dst.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      obrpc::ObPGBackupArchiveLogArg element;
      element.archive_round_ = archive_round;
      element.pg_key_ = pg_keys.at(i);
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("failed to push back element", KR(ret), K(element));
      } else {
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::select_one_server(common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(server_mgr_->get_all_server_list(server_list))) {
    LOG_WARN("failed to get all server list", KR(ret));
  } else if (OB_UNLIKELY(server_list.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no server exist", KR(ret));
  } else if (OB_FAIL(prepare_server_if_needed(server_list))) {
    LOG_WARN("failed to prepare server", KR(ret), K(server_list));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      const common::ObAddr& addr = server_list.at(i);
      ServerStatus status;
      int hash_ret = server_status_map_.get_refactored(addr, status);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server should exist", KR(ret));
      } else if (OB_SUCCESS == hash_ret) {
        if (ServerStatus::STATUS_BUSY == status.status_) {
          if (ObTimeUtil::current_time() - status.lease_time_ < SERVER_LEASE_TIME) {
            continue;
          } else {
            int flag = 1;  // overwrite
            status.lease_time_ = ObTimeUtil::current_time();
            if (OB_FAIL(server_status_map_.set_refactored(addr, status, flag))) {
              LOG_WARN("failed to set server status", KR(ret), K(addr));
            } else {
              server = addr;
              find = true;
              LOG_INFO("select one server to do work", K(addr));
              break;
            }
          }
        } else if (ServerStatus::STATUS_FREE == status.status_) {
          int flag = 1;  // overwrite
          status.status_ = ServerStatus::STATUS_BUSY;
          status.lease_time_ = ObTimeUtil::current_time();
          if (OB_FAIL(server_status_map_.set_refactored(addr, status, flag))) {
            LOG_WARN("failed to set server status", KR(ret), K(addr));
          } else {
            server = addr;
            find = true;
            LOG_INFO("select one server to do work", K(addr));
            break;
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get refactored error", KR(ret));
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("no free server currently");
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_dst_backup_dest(
    const uint64_t tenant_id, char* backup_backup_dest, const int64_t len)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObMySQLTransaction trans;
  ObBaseBackupInfoStruct::BackupDest backup_dest;
  ObBaseBackupInfo backup_info;
  ObBackupInfoManager info_manager;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup backup dest get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
    LOG_WARN("failed to push back tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(info_manager.get_backup_backup_dest(tenant_id, trans, backup_dest))) {
      LOG_WARN("failed to get backup backup dest", KR(ret));
    } else {
      if (!backup_dest.is_empty()) {
        backup_dest.to_string(backup_backup_dest, len);
      } else {
        if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest, len))) {
          LOG_WARN("failed to copy backup backup dest", KR(ret));
        } else if (OB_FAIL(info_manager.update_backup_backup_dest(tenant_id, trans, backup_backup_dest))) {
          LOG_WARN("failed to update backup info", KR(ret), K(tenant_id), K(backup_info));
        }
      }
    }

    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupArchiveLogScheduler::check_is_history_round(const int64_t round, bool& is_his_round)
{
  int ret = OB_SUCCESS;
  is_his_round = false;
  int64_t cur_round = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (OB_FAIL(get_current_log_archive_round(OB_SYS_TENANT_ID, cur_round))) {
    LOG_WARN("failed to get current log archive round", KR(ret));
  } else if (round >= cur_round) {
    LOG_INFO("archive round is still in progress", K(round), K(cur_round));
  } else {
    is_his_round = true;
    LOG_INFO("archive round is in history", K(round), K(cur_round));
  }
  return ret;
}

int ObBackupArchiveLogScheduler::get_sys_tenant_checkpoint_ts()
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfoMgr mgr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log scheduler do not init", KR(ret));
  } else if (!need_fetch_sys_tenant_ts_) {
    LOG_INFO("no need to fetch sys tenant ts");
  } else if (OB_FAIL(mgr.get_log_archive_checkpoint(*sql_proxy_, OB_SYS_TENANT_ID, cur_sys_tenant_checkpoint_ts_))) {
    LOG_WARN("failed to get log archive checkpoint ts", KR(ret));
  } else {
    need_fetch_sys_tenant_ts_ = false;
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
