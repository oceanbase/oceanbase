/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_BG_TASK_MGR_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_BG_TASK_MGR_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/task/ob_timer.h"
#include "share/ob_thread_mgr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{

template <typename TaskType>
class ObFTBackgroundTaskMgrBase
{
public:
  ObFTBackgroundTaskMgrBase()
      : task_(),
        is_inited_(false),
        is_started_(false),
        tg_id_(-1)
  {
  }

  virtual ~ObFTBackgroundTaskMgrBase() { destroy(); }

  int init(const int64_t tg_def_id)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(TG_CREATE_TENANT(tg_def_id, tg_id_))) {
      STORAGE_LOG(WARN, "fail to create tenant tg", K(ret), K(tg_def_id), K(tg_id_));
    } else if (OB_FAIL(task_.init())) {
      STORAGE_LOG(WARN, "fail to init background task", K(ret), K(tg_def_id), K(tg_id_));
      task_.destroy();
      TG_DESTROY(tg_id_);
      tg_id_ = -1;
    } else {
      is_inited_ = true;
    }
    return ret;
  }

  void destroy()
  {
    if (is_inited_) {
      stop();
      wait();
      task_.destroy();
      if (-1 != tg_id_) {
        TG_DESTROY(tg_id_);
        tg_id_ = -1;
      }
      is_inited_ = false;
      is_started_ = false;
    }
  }

  int start()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else if (is_started_) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(TG_START(tg_id_))) {
      STORAGE_LOG(WARN, "fail to start tg", K(ret), K(tg_id_));
    } else {
      is_started_ = true;
    }
    return ret;
  }

  void stop()
  {
    if (is_started_ && -1 != tg_id_) {
      TG_STOP(tg_id_);
      is_started_ = false;
    }
  }

  void wait()
  {
    if (-1 != tg_id_) {
      TG_WAIT(tg_id_);
    }
  }

  int schedule_task(const int64_t interval, const bool repeat = true)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task_, interval, repeat))) {
      STORAGE_LOG(WARN, "fail to schedule task", K(ret), K(tg_id_), K(interval), K(repeat));
    }
    return ret;
  }

  void cancel_task()
  {
    if (is_inited_ && -1 != tg_id_) {
      TG_CANCEL(tg_id_, task_);
    }
  }

  bool is_inited() const { return is_inited_; }
  bool is_started() const { return is_started_; }

protected:
  TaskType &get_task() { return task_; }
  const TaskType &get_task() const { return task_; }
  int get_tg_id() const { return tg_id_; }
  TaskType task_;

private:
  bool is_inited_;
  bool is_started_;
  int tg_id_;
};

class ObFTDictRefreshTask : public common::ObTimerTask
{
public:
  ObFTDictRefreshTask();
  ~ObFTDictRefreshTask() override = default;

  int init();
  void destroy();

  void runTimerTask() override;

private:
  struct DictTableInfo {
    uint64_t table_id_;
    common::ObString table_name_;
    int64_t collation_type_;
    TO_STRING_KV(K_(table_id), K_(table_name), K_(collation_type));
  };

  int get_sql_statement(common::ObSqlString &sql);
  int get_all_dict_tables(common::ObIAllocator &allocator, common::ObIArray<DictTableInfo> &dict_tables);
  int refresh_dict_cache(const DictTableInfo &table_info);

private:
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  bool is_inited_;
};

class ObFTDictRefreshTaskMgr : public ObFTBackgroundTaskMgrBase<ObFTDictRefreshTask>
{
public:
  ObFTDictRefreshTaskMgr() = default;
  ~ObFTDictRefreshTaskMgr() override = default;

  int init();

  static constexpr int64_t REFRESH_INTERVAL = 10 * 60 * 1000 * 1000L;
};

class ObFTAccessRowScnCacheTask : public common::ObTimerTask
{
public:
  ObFTAccessRowScnCacheTask();
  ~ObFTAccessRowScnCacheTask() override = default;

  int init();
  void destroy();

  void runTimerTask() override;

  void push(const uint64_t table_id);

private:
  bool pop(uint64_t &table_id);
  bool should_skip_by_cooldown(const uint64_t table_id, const int64_t now_us);
  void update_cooldown(const uint64_t table_id, const int64_t now_us);

private:
  static constexpr uint64_t FT_TABLE_ID_QUEUE_CAP = 64;
  static constexpr int64_t FT_ACCESS_COOLDOWN_CAP = 32;
  static constexpr int64_t FT_ACCESS_COOLDOWN_US = 1 * 60 * 1000 * 1000L;

private:
  uint64_t queue_[FT_TABLE_ID_QUEUE_CAP];
  uint64_t head_ CACHE_ALIGNED;
  uint64_t tail_ CACHE_ALIGNED;

  struct CooldownEntry {
    uint64_t table_id_;
    int64_t last_access_us_;
    TO_STRING_KV(K_(table_id), K_(last_access_us));
  };
  CooldownEntry cooldown_[FT_ACCESS_COOLDOWN_CAP];
  int64_t cooldown_cnt_;

  bool is_inited_;
};

class ObFTAccessRowScnCacheTaskMgr : public ObFTBackgroundTaskMgrBase<ObFTAccessRowScnCacheTask>
{
public:
  ObFTAccessRowScnCacheTaskMgr() = default;
  ~ObFTAccessRowScnCacheTaskMgr() override = default;

  int init();
  void push(const uint64_t table_id);

  static constexpr int64_t CONSUME_INTERVAL = 1 * 60 * 1000 * 1000L;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_BG_TASK_MGR_H_
