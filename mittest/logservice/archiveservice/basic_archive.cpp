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

#include "common/ob_smart_var.h"
#include "lib/string/ob_sql_string.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
#include "observer/ob_server_struct.h"
#include "share/backup/ob_archive_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_ls_id.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include <unistd.h>
#define protected public
#define private public

#include "logservice/archiveservice/ob_archive_service.h"
#include "logservice/archiveservice/ob_ls_task.h"
#undef private
#undef protected
#include "basic_archive.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/ob_errno.h"
#include <cstdint>
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::archive;
using namespace oceanbase::share;
using namespace oceanbase::palf;
#define SWITCH_TENANT(tenant_id) \
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard); \
  EXPECT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

int ObSimpleArchive::prepare()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;

  EXPECT_EQ(OB_SUCCESS, create_tenant());
  while (true) {
    if (OB_SUCC(get_tenant_id(tenant_id))) {
      break;
    } else {
      sleep(1);
    }
  }
  EXPECT_EQ(OB_SUCCESS, tenant_ids_.push_back(tenant_id));
  EXPECT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  EXPECT_EQ(OB_SUCCESS, create_table_());
  TenantArchiveEnv env;
  env.init();
  map_.insert({tenant_id, env});
  return ret;
}

int ObSimpleArchive::prepare_dest()
{
  int ret = OB_SUCCESS;
  EXPECT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("tt1", "oceanbase"));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  // 设置backup dest
  {
    ObSqlString sql;
    sql.assign_fmt("alter system set log_archive_dest = 'location=%s'", "file://./");
    int64_t affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "set backup_dest succ");
  }
  return ret;
}


int ObSimpleArchive::run_archive(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  {
    ObSqlString sql;
    sql.assign_fmt("alter system archivelog");
    int64_t affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "alter system archivelog succ");
  }
  {
    ObSqlString sql;
    sql.assign_fmt("alter system set log_archive_checkpoint_interval = '5s'");
    int64_t affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "alter system set log_archive_checkpoint_interval succ");
  }
  return ret;
}

int ObSimpleArchive::stop_archive()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  {
    ObSqlString sql;
    sql.assign_fmt("alter system noarchivelog");
    int64_t affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "alter system noarchivelog succ");
  }
  return ret;
}

int ObSimpleArchive::run_observer_archive(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr attr;
  EXPECT_EQ(OB_SUCCESS, fake_archive_attr_(tenant_id, attr));
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  EXPECT_EQ(OB_SUCCESS, guard.switch_to(tenant_id));
  EXPECT_EQ(tenant_id, MTL_ID());
  EXPECT_EQ(OB_SUCCESS, MTL(archive::ObArchiveService*)->start_archive_(attr));
  return ret;
}

int ObSimpleArchive::stop_observer_archive(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSimpleArchive::check_rs_beginning(const uint64_t tenant_id, const int64_t round_id)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr attr;
  SWITCH_TENANT(tenant_id);
  while (true) {
    ret = MTL(archive::ObArchiveService*)->persist_mgr_.load_archive_round_attr(attr);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (attr.round_id_ < round_id) {
    } else
    if (attr.state_.is_beginning() || attr.state_.is_doing()) {
      break;
    }
    ARCHIVE_LOG(INFO, "COME HERE not int doing", K(tenant_id), K(attr));
    sleep(1);
    signal_worker(tenant_id);
  }
  OB_LOG(INFO, "check archive BEGINNING status succ");
  return ret;
}

int ObSimpleArchive::check_rs_doing(const uint64_t tenant_id, const int64_t round_id)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr attr;
  SWITCH_TENANT(tenant_id);
  while (true) {
    ret = MTL(archive::ObArchiveService*)->persist_mgr_.load_archive_round_attr(attr);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (attr.round_id_ < round_id) {
    } else if (attr.state_.is_doing()) {
      break;
    }
    sleep(1);
    signal_worker(tenant_id);
  }
  OB_LOG(INFO, "check archive DOING status succ");
  return ret;
}

int ObSimpleArchive::check_rs_stop(const uint64_t tenant_id, const int64_t round_id)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr attr;
  SWITCH_TENANT(tenant_id);
  while (true) {
    ret = MTL(archive::ObArchiveService*)->persist_mgr_.load_archive_round_attr(attr);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (attr.state_.is_stop()) {
      break;
    }
    sleep(1);
    signal_worker(tenant_id);
  }
  OB_LOG(INFO, "check archive STOP status succ");
  return ret;
}

int ObSimpleArchive::check_rs_archive_progress(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr attr;
  const int64_t ts = common::ObTimeUtility::current_time();
  OB_LOG(INFO, "check_rs_archive_progress start", K(ts), K(attr));
  SWITCH_TENANT(tenant_id);
  while (true) {
    ret = MTL(archive::ObArchiveService*)->persist_mgr_.load_archive_round_attr(attr);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (attr.state_.is_doing() && attr.checkpoint_scn_.convert_to_ts() >= ts) {
      break;
    }
    sleep(1);
    signal_worker(tenant_id);
  }
  OB_LOG(INFO, "check archive progress succ", K(ts), K(attr));
  return ret;
}

// 测试日志流归档进度, 需要保证日志流归档进度已经持久化成功
int ObSimpleArchive::check_archive_progress(const uint64_t tenant_id, const bool check_piece_advance)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = nullptr;
  common::ObSharedGuard<ObLSIterator> guard;
  SWITCH_TENANT(tenant_id);
  OB_LOG(INFO, "COME HERE print", K(tenant_id), K(check_piece_advance));
  EXPECT_EQ(tenant_id, MTL_ID());
  ret = MTL(storage::ObLSService*)->get_ls_iter(guard, ObLSGetMod::ARCHIVE_MOD);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, nullptr != (iter = guard.get_ptr()));
  int64_t base_ts = common::ObTimeUtility::current_time() - 10L * 1000 * 1000;
  while (OB_SUCC(ret)) {
    ret = iter->get_next(ls);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(true, nullptr != ls);
    while (true) {
      palf::LSN lsn(0);
      SCN scn;
      bool force_wait = false;
      bool ignore = false;
      ObArchivePersistValue *value = nullptr;
      ret = MTL(archive::ObArchiveService*)->get_ls_archive_progress(ls->get_ls_id(), lsn, scn, force_wait, ignore);
      EXPECT_EQ(OB_SUCCESS, ret);
      ret = MTL(archive::ObArchiveService*)->persist_mgr_.map_.get(ls->get_ls_id(), value);
      EXPECT_EQ(OB_SUCCESS, ret);
      if (check_piece_advance) {
        if (value->info_.key_.piece_id_ > 1) {
          break;
        }
      } else if (value->info_.checkpoint_scn_.convert_to_ts() >= base_ts) {
        break;
      }
      sleep(1);
      signal_worker(tenant_id);
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "check archive progress succ");
  return ret;
}

int ObSimpleArchive::check_ls_archive_task(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SWITCH_TENANT(tenant_id);
  auto check_archive_task = [](const share::ObLSID &id, ObLSArchiveTask *task) -> bool
  {
    return true;
  };
  ret = MTL(archive::ObArchiveService*)->ls_mgr_.ls_map_.for_each(check_archive_task);
  return ret;
}

int ObSimpleArchive::create_table_()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  // 创建表
  OB_LOG(INFO, "create_table start");
  ObSqlString sql;
  sql.assign_fmt(
      "create table school (sid int, primary key(sid)) "
      "partition by range(sid) (partition p0 values less than (100), partition p1 values less than (200), partition p2 values less than MAXVALUE)");
  int64_t affected_rows = 0;
  EXPECT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  OB_LOG(INFO, "create_table succ");
  return ret;
}

int ObSimpleArchive::fake_stop_component(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  return ret;
}

int ObSimpleArchive::fake_restart_component(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "fake_restart_component start", K(tenant_id));
  SWITCH_TENANT(tenant_id);
  MTL(archive::ObArchiveService*)->sequencer_.notify_start();
  MTL(archive::ObArchiveService*)->ls_mgr_.notify_start();
  OB_LOG(INFO, "fake_restart_component succ", K(tenant_id));
  return ret;
}

int ObSimpleArchive::check_task_finish(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "check_task_finish start", K(tenant_id));
  SWITCH_TENANT(tenant_id);
  while (true) {
    int64_t ls_count =  MTL(archive::ObArchiveService*)->ls_mgr_.ls_map_.count();
    int64_t fetch_task_count = MTL(archive::ObArchiveService*)->fetcher_.get_log_fetch_task_count();
    int64_t send_task_count = MTL(archive::ObArchiveService*)->sender_.get_send_task_status_count();
    if (0 == ls_count && 0 == fetch_task_count && 0 == send_task_count) {
      break;
    } else {
      OB_LOG(WARN, "task not zero, wait", K(ls_count), K(fetch_task_count), K(send_task_count));
    }
    sleep(1);
    signal_worker(tenant_id);
  }
  OB_LOG(INFO, "check_task_finish succ", K(tenant_id));
  return ret;
}

int ObSimpleArchive::fake_piece_info_after_fake_stop(const uint64_t tenant_id, const int64_t piece_interval)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "fake_piece_info_after_fake_stop start", K(tenant_id));
  SWITCH_TENANT(tenant_id);
  ObTenantArchiveRoundAttr attr;
  ret = MTL(archive::ObArchiveService*)->persist_mgr_.load_archive_round_attr(attr);
  EXPECT_EQ(OB_SUCCESS, ret);
  share::SCN genesis_scn;
  genesis_scn.convert_from_ts(attr.start_scn_.convert_to_ts() - 2 * piece_interval);     // 由于piece_interval粒度小, 需要调高
  MTL(archive::ObArchiveService*)->fetcher_.piece_interval_ = piece_interval;
  MTL(archive::ObArchiveService*)->fetcher_.genesis_scn_ = genesis_scn;
  MTL(archive::ObArchiveService*)->ls_mgr_.piece_interval_ = piece_interval;
  MTL(archive::ObArchiveService*)->ls_mgr_.genesis_scn_ = genesis_scn;
  OB_LOG(INFO, "fake_piece_info_after_fake_stop succ", K(tenant_id), K(genesis_scn), K(piece_interval), K(attr));
  return ret;
}

int ObSimpleArchive::fake_remove_ls(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "fake_remove_ls start", K(tenant_id));
  SWITCH_TENANT(tenant_id);
  auto destroy_ls_task = [](const share::ObLSID &id, ObLSArchiveTask *task) -> bool
  {
    task->destroy();
    return true;
  };
  ret = MTL(archive::ObArchiveService*)->ls_mgr_.ls_map_.remove_if(destroy_ls_task);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, MTL(archive::ObArchiveService*)->ls_mgr_.ls_map_.count());
  OB_LOG(INFO, "fake_remove_ls succ", K(tenant_id));
  return ret;
}

int ObSimpleArchive::check_observer_component(const uint64_t tenant_id, const int64_t round_id)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "check_observer_component start", K(tenant_id));
  SWITCH_TENANT(tenant_id);
  while (true) {
    int64_t round = MTL(archive::ObArchiveService*)->archive_round_mgr_.key_.round_;
  }
  OB_LOG(INFO, "check_observer_component succ", K(tenant_id));
  return ret;
}

int ObSimpleArchive::fake_archive_attr_(const uint64_t tenant_id, ObTenantArchiveRoundAttr &attr)
{
  int64_t incarnation = 0;
  int64_t round = 0;
  TenantArchiveEnv env = map_.find(tenant_id)->second;

  env.get_next_round(incarnation, round);
  attr.key_.dest_no_ = 0;
  attr.key_.tenant_id_ = tenant_id;
  attr.round_id_ = round;
  attr.dest_id_ = 1;
  attr.incarnation_ = incarnation;
  attr.state_.set_beginning();
  attr.base_piece_id_ = 1;
  attr.piece_switch_interval_ = 24L * 60 * 60 * 1000 * 1000;
  attr.start_scn_.convert_from_ts(ObTimeUtility::current_time());
  attr.path_.assign("file:///data/1/shuning.tsn/");
  return OB_SUCCESS;
}

void ObSimpleArchive::signal_worker(const uint64_t tenant_id)
{
  SWITCH_TENANT(tenant_id);
  MTL(archive::ObArchiveService*)->fetcher_.unit_size_ = 100;
  MTL(archive::ObArchiveService*)->fetcher_.signal();
  MTL(archive::ObArchiveService*)->sender_.send_cond_.signal();
}

} // namespace unittest
} // namespace oceanbase
