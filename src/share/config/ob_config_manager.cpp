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

#define USING_LOG_PREFIX SHARE

#include "share/config/ob_config_manager.h"

#include "lib/file/file_directory_utils.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/thread_mgr.h"
#include "share/ob_cluster_version.h"
#include "lib/worker.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server.h"

namespace oceanbase
{
namespace common
{
ObConfigManager::~ObConfigManager()
{
  TG_STOP(lib::TGDefIDs::CONFIG_MGR);
  TG_WAIT(lib::TGDefIDs::CONFIG_MGR);
  TG_DESTROY(lib::TGDefIDs::CONFIG_MGR);
}

int ObConfigManager::base_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(system_config_.init())) {
    LOG_ERROR("init system config failed", K(ret));
  } else if (OB_FAIL(server_config_.init(system_config_))) {
    LOG_ERROR("init server config failed", K(ret));
  }
  update_task_.config_mgr_ = this;
  return ret;
}

int ObConfigManager::init(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  self_ = server;
  if (OB_FAIL(TG_START(lib::TGDefIDs::CONFIG_MGR))) {
    LOG_WARN("init timer failed", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObConfigManager::init(ObMySQLProxy &sql_proxy, const ObAddr &server)
{
  int ret = OB_SUCCESS;
  sql_proxy_ = &sql_proxy;
  self_ = server;
  if (OB_FAIL(TG_START(lib::TGDefIDs::CONFIG_MGR))) {
    LOG_WARN("init timer failed", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObConfigManager::stop()
{
  TG_STOP(lib::TGDefIDs::CONFIG_MGR);
}

void ObConfigManager::wait()
{
  TG_WAIT(lib::TGDefIDs::CONFIG_MGR);
}

void ObConfigManager::destroy()
{
  TG_DESTROY(lib::TGDefIDs::CONFIG_MGR);
}

int ObConfigManager::reload_config()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_config_.check_all())) {
    LOG_WARN("Check configuration failed, can't reload", K(ret));
  } else if (OB_FAIL(reload_config_func_())) {
    LOG_WARN("Reload configuration failed.", K(ret));
  } else if (OB_FAIL(OBSERVER.get_net_frame().reload_ssl_config())) {
    LOG_WARN("reload ssl config for net frame fail", K(ret));
  } else if (OB_FAIL(OBSERVER.get_rl_mgr().reload_config())) {
    LOG_WARN("reload config for ratelimit manager fail", K(ret));
  } else if (OB_FAIL(OBSERVER.get_net_frame().reload_sql_thread_config())) {
    LOG_WARN("reload config for mysql login thread count failed", K(ret));
  } else if (OB_FAIL(ObTdeEncryptEngineLoader::get_instance().reload_config())) {
    LOG_WARN("reload config for tde encrypt engine fail", K(ret));
  } else if (OB_FAIL(GCTX.omt_->update_hidden_sys_tenant())) {
    LOG_WARN("update hidden sys tenant failed", K(ret));
  }
  return ret;
}

int ObConfigManager::load_config(const char *path)
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;
  if (OB_ISNULL(path) || OB_UNLIKELY(STRLEN(path) <= 0)) {
    path = dump_path_;
  }

  char *buf = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(
      OB_MAX_PACKET_LENGTH, ObModIds::OB_BUFFER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc buffer failed", LITERAL_K(OB_MAX_PACKET_LENGTH), K(ret));
  } else if (OB_ISNULL(fp = fopen(path, "rb"))) {
    if (ENOENT == errno) {
      ret = OB_FILE_NOT_EXIST;
      LOG_INFO("Config file doesn't exist, read from command line", K(path), K(ret));
    } else {
      ret = OB_IO_ERROR;
      LOG_ERROR("Can't open file", K(path), K(errno), K(ret));
    }
  } else {
    LOG_INFO("Using config file", K(path));
    MEMSET(buf, 0, OB_MAX_PACKET_LENGTH);
    int64_t len = fread(buf, 1, OB_MAX_PACKET_LENGTH, fp);
    int64_t pos = 0;

    if (OB_UNLIKELY(0 != ferror(fp))) { // read with error
      ret = OB_IO_ERROR;
      LOG_ERROR("Read config file error", K(path), K(ret));
    } else if (OB_UNLIKELY(0 == feof(fp))) { // not end of file
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("Config file is too long", K(path), K(ret));
    } else {
      ret = server_config_.deserialize_with_compat(buf, len, pos);
    }
    if (OB_FAIL(ret)) {
      LOG_ERROR("Deserialize server config failed", K(path), K(ret));
    } else if (OB_UNLIKELY(pos != len)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_ERROR("Deserialize server config failed", K(path), K(ret));
    }
    if (OB_UNLIKELY(0 != fclose(fp))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Close config file failed", K(ret));
    }
  }
  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }

  //
  // 为了避免和 got_version 有并发问题，
  // 必须等到 load_config 调用后， got_version 才工作
  init_config_load_ = true;
  return ret;
}

int ObConfigManager::check_header_change(const char* path, const char* buf) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    PageArena<> pa;
    char *cmp_buf = nullptr;
    FILE* fp = NULL;
    ObRecordHeader header;
    int64_t header_len = header.get_serialize_size();
    if (OB_ISNULL(cmp_buf = pa.alloc(header_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buffer failed", K(header_len), K(ret));
    } else if (OB_ISNULL(fp = fopen(path, "rb"))) {
      if (ENOENT != errno) {
        ret = OB_IO_ERROR;
        LOG_ERROR("Can't open file", K(path), K(errno), K(ret));
      }
    } else {
      MEMSET(cmp_buf, 0, header_len);
      int64_t len = fread(cmp_buf, 1, header_len, fp);
      int64_t pos = 0;
      if (OB_UNLIKELY(0 != ferror(fp))) {  // read with error
        ret = OB_IO_ERROR;
        LOG_ERROR("Read config file error", K(path), K(ret));
      } else if (MEMCMP(buf, cmp_buf, header_len) == 0) {
        ret = OB_EAGAIN;
      }
      if (OB_UNLIKELY(0 != fclose(fp))) {
        ret = OB_IO_ERROR;
        LOG_ERROR("Close config file failed", K(errno));
      }
    }
  }
  return ret;
}

int ObConfigManager::dump2file(const char* path) const
{
  int ret = OB_SUCCESS;
  int fd = 0;
  ssize_t size = 0;
  if (OB_ISNULL(path)) {
    path = dump_path_;
  }

  if (OB_ISNULL(path) || STRLEN(path) <= 0) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("NO dump path specified!", K(ret));
  } else {
    const int64_t min_buffer_size = 2LL << 20;
    const int64_t max_buffer_size = 64LL << 20;
    int64_t buf_size = min_buffer_size;
    bool need_retry = true;
    while (OB_SUCC(ret) && need_retry) {
      PageArena<> pa;
      char *buf = nullptr;
      char *tmp_path = nullptr;
      char *hist_path = nullptr;
      int64_t pos = 0;
      need_retry = false;
      int tmp_ret = OB_SUCCESS;
      int tmp_ret_2 = OB_SUCCESS;
      if (OB_ISNULL(buf = pa.alloc(buf_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("ob tc malloc memory for buf failed", K(ret));
      }
      if (OB_ISNULL(tmp_path = pa.alloc(MAX_PATH_SIZE))) {
        tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_ERROR("ob tc malloc memory for tmp configure path failed", K(ret), K(tmp_ret));
      } else {
        snprintf(tmp_path, MAX_PATH_SIZE, "%s.tmp", path);
      }
      if (OB_ISNULL(hist_path = pa.alloc(MAX_PATH_SIZE))) {
        tmp_ret_2 = OB_ALLOCATE_MEMORY_FAILED;
        ret = OB_SUCC(ret) ? tmp_ret_2 : ret;
        LOG_ERROR("ob tc malloc memory for history configure path fail", K(ret), K(tmp_ret_2));
      } else {
        snprintf(hist_path, MAX_PATH_SIZE, "%s.history", path);
      }

      #ifdef ERRSIM
      ret = OB_E(EventTable::EN_WRITE_CONFIG_FILE_FAILED) OB_SUCCESS;;
      if (OB_FAIL(ret)) {
        ret = OB_IO_ERROR;
        LOG_WARN("ERRSIM, write config file failed", K(ret));
      }
      #endif
      if (OB_SUCC(ret)) {
        if (OB_FAIL(server_config_.serialize(buf, buf_size, pos))) {
          LOG_WARN("Serialize server config fail!", K(pos), K(buf_size), K(ret));
          if (OB_SIZE_OVERFLOW == ret && (buf_size << 1) <= max_buffer_size) {
            buf_size = buf_size << 1;
            need_retry = true;
          }
        } else if (OB_FAIL(check_header_change(path, buf)) && OB_EAGAIN == ret) {
          LOG_INFO("Header not change, no need to write server config!");
        } else if ((fd = ::open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC,
                                S_IRUSR  | S_IWUSR | S_IRGRP)) < 0) {
          ret = OB_IO_ERROR;
          LOG_WARN("fail to create config file", K(tmp_path), KERRMSG, K(ret));
        } else if (pos != (size = unintr_write(fd, buf, pos))) {
          ret = OB_IO_ERROR;
          LOG_WARN("Write server config fail!", K(errno), KERRMSG, K(pos), K(size), K(ret));
          if (0 != close(fd)) {
            LOG_WARN("fail to close file fd", K(fd), K(errno), KERRMSG, K(ret));
          }
        } else if (::fsync(fd) != 0) {
          ret = OB_IO_ERROR;
          LOG_WARN("Sync server config fail!", K(errno), KERRMSG, K(pos), K(size), K(ret));
          if (0 != close(fd)) {
            LOG_WARN("fail to close file fd", K(fd), K(errno), KERRMSG, K(ret));
          }
        } else if (0 != close(fd)) {
          ret = OB_IO_ERROR;
          LOG_WARN("fail to close file fd", K(fd), KERRMSG, K(ret));
        } else {
          LOG_INFO("Write server config successfully!", K(pos), K(buf_size));
        }
      }
      if (OB_SUCC(ret)) {
        if (0 != ::rename(path, hist_path) && errno != ENOENT) {
          ret = OB_ERR_SYS;
          LOG_WARN("fail to backup history config file", KERRMSG, K(ret));
        }
        // 运行到这里的时候可能掉电，导致没有 conf 文件，需要 DBA 手工拷贝  tmp 文件到这里
        if (0 != ::rename(tmp_path, path) && errno != ENOENT) {
          ret = OB_ERR_SYS;
          LOG_WARN("fail to move tmp config file", KERRMSG, K(ret));
        }
      } else if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else if (need_retry) {
        ret = OB_SUCCESS;
      }
    }

    // write server config

  }
  return ret;
}

int ObConfigManager::config_backup()
{
  int ret = OB_SUCCESS;
  char path[MAX_PATH_SIZE] = {};
  static const char *CONF_COPY_NAME = "/observer.conf.bin";

  for (int64_t idx = 0; idx < server_config_.config_additional_dir.size(); ++idx) {
    if (OB_SUCC(server_config_.config_additional_dir.get(idx, path, MAX_PATH_SIZE))) {
      if (STRLEN(path) > 0) {
        if (OB_FAIL(common::FileDirectoryUtils::create_full_path(path))) {
          LOG_ERROR("create additional configure directory fail", K(path), K(ret));
        } else if (STRLEN(path) + STRLEN(CONF_COPY_NAME) < static_cast<uint64_t>(MAX_PATH_SIZE)) {
          strcat(path, CONF_COPY_NAME);
          if (OB_FAIL(dump2file(path))) {
            LOG_WARN("make additional configure file copy fail", K(path), K(ret));
            ret = OB_SUCCESS;  // ignore ret code.
          }
        } else {
          LOG_ERROR("additional configure directory path is too long",
                    K(path), "len", STRLEN(path));
        }
      }
    }
  } // for
  return ret;
}

int ObConfigManager::update_local(int64_t expected_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      int64_t start = ObTimeUtility::current_time();
      const char *sqlstr = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
          "data_type, value, info, section, scope, source, edit_level "
          "from __all_sys_parameter";
      if (OB_FAIL(sql_client_retry_weak.read(result, sqlstr))) {
        LOG_WARN("read config from __all_sys_parameter failed", K(sqlstr), K(ret));
      } else if (OB_FAIL(system_config_.update(result))) {
        LOG_WARN("failed to load system config", K(ret));
      } else if (expected_version != ObSystemConfig::INIT_VERSION && (system_config_.get_version() < current_version_
                 || system_config_.get_version() < expected_version)) {
        ret = OB_EAGAIN;
        LOG_WARN("__all_sys_parameter is older than the expected version", K(ret),
                 "read_version", system_config_.get_version(),
                 "current_version", current_version_,
                 "expected_version", expected_version);
      } else {
        current_version_ = system_config_.get_version();
        LOG_INFO("read config from __all_sys_parameter succed", K(start), K(current_version_), K(expected_version));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if ('\0' == dump_path_[0]) {
      ret = OB_NOT_INIT;
      LOG_ERROR("Dump path doesn't set, stop read config", K(ret));
    } else if (OB_FAIL(server_config_.read_config())) {
      LOG_ERROR("Read server config failed", K(ret));
    } else if (OB_FAIL(reload_config())) {
      LOG_WARN("Reload configuration failed", K(ret));
    } else {
      DRWLock::RDLockGuard guard(OTC_MGR.rwlock_); // need protect tenant config because it will also serialize tenant config
      if (OB_FAIL(dump2file())) {
        LOG_WARN("Dump to file failed", K_(dump_path), K(ret));
      } else {
        GCONF.cluster.set_dumped_version(GCONF.cluster.version());
        LOG_INFO("Reload server config successfully!");
        ret = config_backup();
      }
    }
    server_config_.print();
  } else {
    LOG_WARN("Read system config from inner table error", K(ret));
  }
  return ret;
}

int ObConfigManager::got_version(int64_t version, const bool remove_repeat/* = false */)
{
  int ret = OB_SUCCESS;
  bool schedule_task = false;
  if (!init_config_load_) {
    schedule_task = false;
  } else if (version < 0) {
    // from rs_admin, do whatever
    update_task_.update_local_ = false;
    schedule_task = true;
  } else if (0 == version) {
    // do nothing
    LOG_DEBUG("root server restarting");
  } else if (current_version_ == version) {
    // no new version
  } else if (version < current_version_) {
    LOG_WARN("Local config is newer than rs, weird", K_(current_version), K(version));
  } else if (version > current_version_) {
    // local:current_version_, got:version
    LOG_INFO("Got new config version", K_(current_version), K(version));
    update_task_.update_local_ = true;
    schedule_task = true;
  }

  if (schedule_task) {
    bool schedule = true;
    if (!inited_) {
      // if got a config version before manager init, ignore this version
      schedule = false;
      ret = OB_NOT_INIT;
      LOG_WARN("Couldn't update config because timer is NULL", K(ret));
    } else if (true == remove_repeat) {
      bool task_exist = false;
      ret = TG_TASK_EXIST(lib::TGDefIDs::CONFIG_MGR, update_task_, task_exist);
      if (OB_SUCC(ret)) {
        schedule = !task_exist;
      }
      LOG_INFO("no need repeat schedule the same task");
    } else {
      // do nothing
    }

    if (schedule) {
      // 如果决策了本次要调度一个新 task，那么现将队列中排队的所有 task 全部移除
      // 有一点可以确保，到达这个点时无论移除的 task 是什么，下一个要添加的 task
      // 的 version 一定是最新的。
      bool task_exist = false;
      int tmp_ret = TG_TASK_EXIST(lib::TGDefIDs::CONFIG_MGR, update_task_, task_exist);
      if (task_exist) {
        TG_CANCEL(lib::TGDefIDs::CONFIG_MGR, update_task_);
        LOG_INFO("Cancel pending update task",
                 K(tmp_ret), K_(current_version), K(version));
      }
    }

    if (schedule) {
      update_task_.version_ = version;
      update_task_.scheduled_time_ = ObClockGenerator::getClock();
      if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::CONFIG_MGR, update_task_, 0, false))) {
        LOG_WARN("Update local config failed, may try later", K(ret));
      } else {
        LOG_INFO("Schedule update config task successfully!");
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

void ObConfigManager::UpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K_(config_mgr), K(ret));
  } else {
    const int64_t old_current_version = config_mgr_->current_version_;
    const int64_t version = version_;
    ObCurTraceId::init(config_mgr_->self_);
    THIS_WORKER.set_timeout_ts(INT64_MAX);
    if (config_mgr_->current_version_ == version) {
      ret = OB_ALREADY_DONE;
    } else if (config_mgr_->current_version_ > version) {
      ret = OB_CANCELED;
    } else if (update_local_) {
      config_mgr_->current_version_ = version;
      if (OB_FAIL(config_mgr_->system_config_.clear())) {
        // just print log, ignore ret
        LOG_WARN("Clear system config map failed", K(ret));
      } else {
        // do nothing
      }
      if (OB_FAIL(config_mgr_->update_local(version))) {
        LOG_WARN("Update local config failed", K(ret));
        // recovery current_version_
        config_mgr_->current_version_ = old_current_version;
        // retry update local config in 1s later
        if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::CONFIG_MGR, *this, 1000 * 1000L, false))) {
          LOG_WARN("Reschedule update local config failed", K(ret));
        }
      } else {
        // do nothing
        const int64_t read_version = config_mgr_->system_config_.get_version();
        LOG_INFO("loaded new config",
                 "read_version", read_version,
                 "old_version", old_current_version,
                 "current_version", config_mgr_->current_version_,
                 "expected_version", version);
      }
    } else if (OB_FAIL(config_mgr_->reload_config())) {
      LOG_WARN("Reload configuration failed", K(ret));
    } else {
      config_mgr_->server_config_.print();
    }
    ObCurTraceId::reset();
  }
}

} // namespace common
} // namespace oceanbase
