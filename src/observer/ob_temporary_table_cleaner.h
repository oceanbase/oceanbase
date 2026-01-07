/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TEMP_TABLE_CLEANER_H
#define OCEANBASE_TEMP_TABLE_CLEANER_H

#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/task/ob_timer.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_queue_thread.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"

namespace oceanbase
{
namespace observer
{
/*
Session临时表清理逻辑
  *清理范围: 残留CTAS表;mysql临时表
  *清理规则:
          1.mtl注册租户级别清理逻辑，默认10分钟调度一次，分为本地清理任务以及孤儿清理任务
          2.本地清理任务:session断开时会注册table_session_id
            server直连:如果该table_id的create_host是本机,则将其删除,否则跳过不进行处理。全部处理成功后将该session注销
            proxy连接:对当前租户unit所在机器进行client_session_id探活，所有机器不存在session后清理相关表，否则跳过不进行处理
          2.孤儿清理任务
            本地孤儿清理:将all_temp_table中，所有create_host是本地的表捞出。进行探活，如果session已经断连则进行清理
            孤儿副本清理:如果本机是租户1号日志流leader所在节点，则对all_temp_table中，所有create_host不在租户unit所在机器的seesion_id探活
                       server不存在:直连session直接删除;proxy连接进行探活
                       server active:对该机器以及租户unit所在机器进行探活
                       server not active:不能确认状态，跳过
*/
class ObSessionTmpTableCleaner;
class ObNormalSessionTableCleanUpTask : public common::ObTimerTask
{
public:
  explicit ObNormalSessionTableCleanUpTask(ObSessionTmpTableCleaner &cleaner);
  virtual ~ObNormalSessionTableCleanUpTask() {}
  virtual void runTimerTask() override;
private:
  ObSessionTmpTableCleaner &cleaner_;
};

class ObStandAloneSessionTableCleanUpTask : public common::ObTimerTask
{
public:
  explicit ObStandAloneSessionTableCleanUpTask(ObSessionTmpTableCleaner &cleaner);
  virtual ~ObStandAloneSessionTableCleanUpTask() {}
  virtual void runTimerTask() override;
private:
  ObSessionTmpTableCleaner &cleaner_;
};

class ObDynamicScheduleCleanUpTask : public common::ObTimerTask
{
public:
  explicit ObDynamicScheduleCleanUpTask(ObSessionTmpTableCleaner &cleaner);
  virtual ~ObDynamicScheduleCleanUpTask() {}
  virtual void runTimerTask() override;
private:
ObSessionTmpTableCleaner &cleaner_;
};

class ObSessionTmpTableCleaner
{
const int64_t DEFAULT_SCHEDULE_INTERVAL = 10L * 60L * 1000L * 1000L;//10m
public:
  ObSessionTmpTableCleaner() : tenant_id_(OB_INVALID_TENANT_ID), inited_(false), stopped_(false), schema_service_(nullptr),
                               attr_(MTL_ID(), "TmpTableClean"), lock_(), tenant_sessions_(),
                               clean_tg_id_(OB_INVALID_INDEX), normal_table_clean_(*this), standalone_table_clean_(*this),
                               schedule_interval_(DEFAULT_SCHEDULE_INTERVAL), dynamic_schedule_(*this) {}
  virtual ~ObSessionTmpTableCleaner();
  static int mtl_init(ObSessionTmpTableCleaner *&cleaner);
  int init(share::schema::ObMultiVersionSchemaService *schema_service);
  int start();
  void stop();
  void wait();
  void destroy();
  int register_task(const uint64_t table_session_id);
  int remove_task(const uint64_t table_session_id);
  int clean_up_invalid_tables();
  int normal_table_clean();
  int schedule_normal_table_clean();
  int standalone_table_clean();
  int schedule_standalone_table_clean();
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  int cancel_and_schedule_task();
  int64_t get_schedule_interval() const { return schedule_interval_; }
  void set_schedule_interval(const int64_t schedule_interval) { schedule_interval_ = schedule_interval; }

private:
  int check_inner_stat_();
  int check_enable_clean_( bool &enable_clean);
  int gather_tables_(const uint64_t table_session_id, const bool check_host, ObArray<uint64_t> &tables);
  int drop_tables_(const uint64_t table_session_id, const ObArray<uint64_t> &tables);
  int detect_session_alive_(const uint64_t table_session_id, bool local_only, const ObArray<ObAddr> *dest_server, bool &is_alive);
  int gather_residual_sessions_local_();
  int gather_residual_sessions_global_();
  int clean_up_session_invalid_tables_(const uint64_t table_session_id, const bool check_host);
  int clean_residual_table_(const ObAddr addr, const uint64_t session_id, const uint64_t table_id,
                            const ObString &create_host, const ObArray<ObAddr> &server_list);
  int is_sys_ls_leader_(bool &is_leader);
  inline uint32_t table_sid_to_client_sid_ (const uint64_t table_session_id)
  { return static_cast<uint32_t>(table_session_id & transaction::tablelock::ObTableLockOwnerID:: CLIENT_SESS_ID_MASK); }

private:
  uint64_t tenant_id_;
  bool inited_;
  bool stopped_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObMemAttr attr_;
  common::SpinRWLock lock_;
  common::hash::ObHashSet<uint64_t> tenant_sessions_;
  int clean_tg_id_;
  ObNormalSessionTableCleanUpTask normal_table_clean_;
  ObStandAloneSessionTableCleanUpTask standalone_table_clean_;
  int64_t schedule_interval_;
  ObDynamicScheduleCleanUpTask dynamic_schedule_;
};

} // end observer
} // end oceanbase

#endif
