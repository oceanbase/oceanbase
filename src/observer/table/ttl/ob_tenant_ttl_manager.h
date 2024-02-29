/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_TABLE_OB_TENANT_TTL_MANAGER_H_
#define OCEANBASE_OBSERVER_TABLE_OB_TENANT_TTL_MANAGER_H_

#include "share/table/ob_ttl_util.h"
#include "share/tablet/ob_tablet_info.h"

namespace oceanbase
{
namespace table
{
class ObRootService;

class ObTTLServerInfo;
typedef common::ObArray<ObTTLServerInfo> TTLServerInfos;
typedef common::hash::ObHashSet<common::ObAddr> ServerSet;

/**
 * the task for clear ttl history task in __all_ttl_task_status_history
*/
class ObClearTTLHistoryTask : public common::ObTimerTask
{
public:
  ObClearTTLHistoryTask()
  : sql_proxy_(nullptr),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_paused_(false)
  {}
  ~ObClearTTLHistoryTask() {}
  int init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy);
  virtual void runTimerTask() override;
  void destroy() {}
  void pause();
  void resume();

  static const int64_t OB_KV_TTL_GC_INTERVAL =  30 * 1000L * 1000L; // 30s
  static const int64_t OB_KV_TTL_GC_COUNT_PER_TASK = 4096L;
private:
  common::ObMySQLProxy *sql_proxy_;
  bool is_inited_;
  uint64_t tenant_id_;
  bool is_paused_;
};

struct ObTTLServerInfo
{
public:
  ObTTLServerInfo() : addr_(), is_responsed_(false) {}
  ~ObTTLServerInfo() = default;
  TO_STRING_KV(K_(addr), K_(is_responsed));
public:
  common::ObAddr addr_;
  bool is_responsed_;
};

class ObTTLTenantTask
{
public:
  ObTTLTenantTask(uint64_t tenant_id = OB_INVALID_ID)
    : tenant_id_(tenant_id),
      ttl_status_(),
      is_finished_(true)
    {}
  ~ObTTLTenantTask() {}

  void reset() {
    is_finished_ = true;
    ttl_status_.status_ = OB_TTL_TASK_INVALID;
  }

  TO_STRING_KV(K_(tenant_id),
               K_(ttl_status),
               K_(is_finished));

public:
  uint64_t tenant_id_;
  common::ObTTLStatus ttl_status_;
  bool is_finished_;
};

class ObTTLTaskScheduler : public common::ObTimerTask
{
public:
  ObTTLTaskScheduler()
  : del_ten_arr_(), sql_proxy_(nullptr), is_inited_(false), periodic_launched_(false),
    need_reload_(true), is_leader_(true), need_do_for_switch_(true)
  {}
  ~ObTTLTaskScheduler() {}

  int init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy);

  int add_ttl_task(ObTTLTaskType task_type);

  void reset_local_tenant_task();

  // reload latest tenant task from system table
  int reload_tenant_task();

  void runTimerTask() override;

  int try_add_periodic_task();
  void set_need_reload(bool need_reload) { ATOMIC_STORE(&need_reload_, need_reload); }

  void pause();
  void resume();
private:
  virtual int delete_task(const uint64_t tenant_id, const uint64_t task_id);

  virtual int in_active_time(bool& is_active_time);

  virtual int insert_tenant_task(ObTTLStatus& ttl_task);

  virtual int update_task_status(uint64_t task_id,
                                 int64_t rs_new_status,
                                 common::ObISQLClient& proxy);
  virtual int fetch_ttl_task_id(uint64_t tenant_id, int64_t &new_task_id);

  int calc_next_task_state(ObTTLTaskType user_cmd_type,
                           ObTTLTaskStatus curr_state,
                           ObTTLTaskStatus &next_state);

  ObTTLTaskStatus next_status(int64_t curr);

  int add_ttl_task_internal(TRIGGER_TYPE trigger_type);

  int check_all_tablet_task();
  int check_one_tablet_task(common::ObISQLClient &sql_client,
                            const uint64_t table_id,
                            const ObTabletID tablet_id,
                            bool &is_finished);
  int check_is_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table);

  int check_task_need_move(bool &need_move);
private:
  int check_all_tablet_finished(bool &all_finished);
  int check_tablet_table_finished(common::ObIArray<share::ObTabletTablePair> &pairs, bool &all_finished);
  int move_all_task_to_history_table();
  OB_INLINE bool need_skip_run() { return ATOMIC_LOAD(&need_do_for_switch_); }
private:
  static const int64_t TBALE_CHECK_BATCH_SIZE = 200;
  static const int64_t TBALET_CHECK_BATCH_SIZE = 1024;
  static const int64_t DEFAULT_TABLE_ARRAY_SIZE = 200;
  static const int64_t DEFAULT_TABLET_PAIR_SIZE = 1024;
private:
  ObTTLTenantTask tenant_task_;
  ObArray<uint64_t> del_ten_arr_;
  common::ObMySQLProxy *sql_proxy_;
  bool is_inited_;
  uint64_t tenant_id_;

  bool periodic_launched_;

  bool need_reload_;
  lib::ObMutex mutex_;
  ObArray<share::ObTabletTablePair> tablet_table_pairs_;
  bool is_leader_; // current ttl manager in ls leader or not
  const int64_t OB_TTL_TASK_RETRY_INTERVAL = 15*1000*1000; // 15s
  bool need_do_for_switch_; // need wait follower finish after switch leader
};

class ObTenantTTLManager
{
public:
  static const int64_t SCHEDULE_PERIOD = 15 * 1000L * 1000L; // 15s
  explicit ObTenantTTLManager()
    : is_inited_(false),
      clear_ttl_history_task_(),
      tenant_id_(OB_INVALID_TENANT_ID),
      task_scheduler_(),
      tg_id_(0)
  {}

  virtual ~ObTenantTTLManager() {}
  int init(const uint64_t tenant_id, ObMySQLProxy &sql_proxy);
  int start();
  void wait();
  void stop();
  void destroy();
  int handle_user_ttl(const obrpc::ObTTLRequestArg& arg);
  void resume();
  void pause();
private:
  bool is_inited_;
  ObClearTTLHistoryTask clear_ttl_history_task_;
  uint64_t tenant_id_;
  ObTTLTaskScheduler task_scheduler_;
  int tg_id_;
};


} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_OB_TENANT_TTL_MANAGER_H_ */