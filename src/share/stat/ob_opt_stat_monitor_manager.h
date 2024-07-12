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

#ifndef _OB_OPT_STAT_MONITOR_MANAGER_H_
#define _OB_OPT_STAT_MONITOR_MANAGER_H_

#include "share/ob_define.h"
#include "lib/task/ob_timer.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_define.h"
#include "observer/virtual_table/ob_all_virtual_dml_stats.h"
namespace oceanbase
{

namespace observer
{
class ObOptDmlStatMapGetter;
}

namespace common
{
typedef std::pair<uint64_t, uint64_t> StatKey;

typedef common::hash::ObHashMap<StatKey, int64_t> ColumnUsageMap;

typedef common::hash::ObHashMap<StatKey, ObOptDmlStat> DmlStatMap;

class ObMySQLProxy;
struct ObColumnStatParam;

struct ColumnUsageArg
{
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t flags_;
  TO_STRING_KV(K(table_id_), K(column_id_), K(flags_));
};

class ObOptStatMonitorFlushAllTask : public common::ObTimerTask
{
public:
  ObOptStatMonitorFlushAllTask() : optstat_monitor_mgr_(NULL) {}
  virtual ~ObOptStatMonitorFlushAllTask() {}
  virtual void runTimerTask() override;
  const static int64_t FLUSH_INTERVAL = 24L * 3600L * 1000L * 1000L; // 24h
  ObOptStatMonitorManager *optstat_monitor_mgr_;
};

class ObOptStatMonitorCheckTask : public common::ObTimerTask
{
public:
  ObOptStatMonitorCheckTask() : optstat_monitor_mgr_(NULL) {}
  virtual ~ObOptStatMonitorCheckTask() {}
  virtual void runTimerTask() override;
  const static int64_t CHECK_INTERVAL = 900L * 1000L * 1000L; // 15min
  ObOptStatMonitorManager *optstat_monitor_mgr_;
};

class ObOptStatMonitorManager
{
  friend class ObOptStatMonitorFlushAllTask;
  friend class ObOptStatMonitorCheckTask;

  // A callback struct used to update ColumnUsageMap value or DmlStatMap value
  struct UpdateValueAtomicOp
  {
  public:
    UpdateValueAtomicOp(int64_t flags) : flags_(flags), dml_stat_() {};
    UpdateValueAtomicOp(ObOptDmlStat &dml_stat) : flags_(0), dml_stat_(dml_stat) {};
    virtual ~UpdateValueAtomicOp() {};
    int operator() (common::hash::HashMapPair<StatKey, int64_t> &entry);
    int operator() (common::hash::HashMapPair<StatKey, ObOptDmlStat> &entry);
  private:
    DISALLOW_COPY_AND_ASSIGN(UpdateValueAtomicOp);
    int64_t flags_;
    ObOptDmlStat dml_stat_;
  };

public:
  ObOptStatMonitorManager()
    : inited_(false),
      tenant_id_(0),
      tg_id_(-1),
      destroyed_(false),
      mysql_proxy_(NULL)
      {}
  virtual ~ObOptStatMonitorManager() { if (inited_) { destroy(); }  }
  void destroy();
  static int mtl_init(ObOptStatMonitorManager* &optstat_monitor_mgr);
  static int mtl_start(ObOptStatMonitorManager* &optstat_monitor_mgr);
  static void mtl_stop(ObOptStatMonitorManager* &optstat_monitor_mgr);
  static void mtl_wait(ObOptStatMonitorManager* &optstat_monitor_mgr);
public:
  static int flush_database_monitoring_info(sql::ObExecContext &ctx,
                                            const bool is_flush_col_usage = true,
                                            const bool is_flush_dml_stat = true,
                                            const bool ignore_failed = true);
  int update_opt_stat_monitoring_info(const obrpc::ObFlushOptStatArg &arg);
  int update_local_cache(common::ObIArray<ColumnUsageArg> &args);
  int update_local_cache(ObOptDmlStat &dml_stat);
  int update_column_usage_info(const bool with_check);
  int update_dml_stat_info();
  int update_dml_stat_info(const ObIArray<ObOptDmlStat *> &dml_stats,
                           common::sqlclient::ObISQLConnection *conn = nullptr);
  int get_column_usage_sql(const StatKey &col_key,
                           const int64_t flags,
                           const bool need_add_comma,
                           ObSqlString &sql_string);
  int get_dml_stat_sql(const ObOptDmlStat &dml_stat,
                       const bool need_add_comma,
                       ObSqlString &sql_string);
  int exec_insert_column_usage_sql(ObSqlString &values_sql);
  int exec_insert_monitor_modified_sql(ObSqlString &values_sql,
                                       common::sqlclient::ObISQLConnection *conn = nullptr);
  static int get_column_usage_from_table(sql::ObExecContext &ctx,
                                         ObIArray<ObColumnStatParam *> &column_params,
                                         uint64_t tenant_id,
                                         uint64_t table_id);
  static int construct_get_column_usage_sql(ObIArray<ObColumnStatParam *> &column_params,
                                            const uint64_t tenant_id,
                                            const uint64_t table_id,
                                            ObSqlString &select_sql);

  int check_table_writeable(bool &is_writeable);
  int generate_opt_stat_monitoring_info_rows(observer::ObOptDmlStatMapGetter &getter);
  int clean_useless_dml_stat_info();
  static int update_dml_stat_info_from_direct_load(const ObIArray<ObOptDmlStat *> &dml_stats,
                                                   common::sqlclient::ObISQLConnection *conn = nullptr);
  int get_col_usage_info(const bool with_check,
                         ObIArray<StatKey> &col_stat_keys,
                         ObIArray<int64_t> &col_flags);
  int get_dml_stats(ObIArray<ObOptDmlStat> &dml_stats);
  ObOptStatMonitorFlushAllTask &get_flush_all_task() { return flush_all_task_; }
  ObOptStatMonitorCheckTask &get_check_task() { return check_task_; }
  int init(uint64_t tenant_id);

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptStatMonitorManager);
  const static int64_t UPDATE_OPT_STAT_BATCH_CNT = 200;
  const static int64_t info_count = 8;
  bool inited_;
  uint64_t tenant_id_;
  int tg_id_;
  bool destroyed_;
  ObMySQLProxy *mysql_proxy_;
  ColumnUsageMap column_usage_map_;
  DmlStatMap dml_stat_map_;
  common::SpinRWLock lock_;
  ObOptStatMonitorFlushAllTask flush_all_task_;
  ObOptStatMonitorCheckTask check_task_;
}; // end of class ObOptStatMonitorManager

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_OPT_STAT_MONITOR_MANAGER_H_ */
