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
class ObOptDmlStatMapsGetter;
}

namespace common
{
typedef std::pair<uint64_t, uint64_t> StatKey;

typedef common::hash::ObHashMap<StatKey, int64_t> ColumnUsageMap;
typedef common::hash::ObHashMap<uint64_t, ColumnUsageMap *> ColumnUsageMaps;

typedef common::hash::ObHashMap<StatKey, ObOptDmlStat> DmlStatMap;
typedef common::hash::ObHashMap<uint64_t, DmlStatMap *> DmlStatMaps;

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
  ObOptStatMonitorFlushAllTask()
    : is_inited_(false) {}
  virtual ~ObOptStatMonitorFlushAllTask() {}
  int init(int tg_id);
  virtual void runTimerTask() override;
private:
  const static int64_t FLUSH_INTERVAL = 24L * 3600L * 1000L * 1000L; // 24h
  bool is_inited_;
};

class ObOptStatMonitorCheckTask : public common::ObTimerTask
{
public:
  ObOptStatMonitorCheckTask()
    : is_inited_(false) {}
  virtual ~ObOptStatMonitorCheckTask() {}
  int init(int tg_id);
  virtual void runTimerTask() override;
  const static int64_t CHECK_INTERVAL = 900L * 1000L * 1000L; // 15min
private:
  bool is_inited_;
};

class ObOptStatMonitorManager
{
public:
  // A callback struct used to get all key of hash map
  struct GetAllKeyOp
  {
  public:
    GetAllKeyOp(common::ObIArray<uint64_t> *key_array, const bool with_check)
      : key_array_(key_array), with_check_(with_check) {}
    virtual ~GetAllKeyOp() {};
    int operator()(common::hash::HashMapPair<uint64_t, ColumnUsageMap *> &entry);
    int operator()(common::hash::HashMapPair<uint64_t, DmlStatMap *> &entry);
  private:
    DISALLOW_COPY_AND_ASSIGN(GetAllKeyOp);
    common::ObIArray<uint64_t> *key_array_;
    bool with_check_;
  };

  // A callback struct used to get ColumnUsageMap or ObOptDmlStat, and allocate a new one
  struct SwapMapAtomicOp
  {
  public:
    SwapMapAtomicOp() : column_usage_map_(NULL), dml_stat_map_(NULL) {};
    virtual ~SwapMapAtomicOp() {};
    int operator() (common::hash::HashMapPair<uint64_t, ColumnUsageMap *> &entry);
    int operator() (common::hash::HashMapPair<uint64_t, DmlStatMap *> &entry);
    ColumnUsageMap *get_column_usage_map() { return column_usage_map_; }
    DmlStatMap *get_dml_stat_map() { return dml_stat_map_; }
  private:
    DISALLOW_COPY_AND_ASSIGN(SwapMapAtomicOp);
    ColumnUsageMap *column_usage_map_;
    DmlStatMap *dml_stat_map_;
  };

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

  // A callback struct used to read ColumnUsageMaps value or DmlStatMaps value
  struct ReadMapAtomicOp
  {
  public:
    ReadMapAtomicOp(common::ObIArray<ColumnUsageArg> *col_usage_args) :
      col_usage_args_(col_usage_args), dml_stat_() {};
    ReadMapAtomicOp(ObOptDmlStat &dml_stat) : col_usage_args_(NULL), dml_stat_(dml_stat) {};
    virtual ~ReadMapAtomicOp() {};
    int operator() (common::hash::HashMapPair<uint64_t, ColumnUsageMap *> &entry);
    int operator() (common::hash::HashMapPair<uint64_t, DmlStatMap *> &entry);
  private:
    DISALLOW_COPY_AND_ASSIGN(ReadMapAtomicOp);
    common::ObIArray<ColumnUsageArg> *col_usage_args_;
    ObOptDmlStat dml_stat_;
  };

public:
  ObOptStatMonitorManager()
    : inited_(false), mysql_proxy_(NULL) {}
  virtual ~ObOptStatMonitorManager() { destroy(); }

  int init(ObMySQLProxy *mysql_proxy);
  void destroy();
  static ObOptStatMonitorManager &get_instance();
public:
  static int flush_database_monitoring_info(sql::ObExecContext &ctx,
                                            const bool is_flush_col_usage = true,
                                            const bool is_flush_dml_stat = true,
                                            const bool ignore_failed = true);
  int update_opt_stat_monitoring_info(const bool with_check);
  int update_opt_stat_monitoring_info(const obrpc::ObFlushOptStatArg &arg);
  int update_local_cache(uint64_t tenant_id, common::ObIArray<ColumnUsageArg> &args);
  int update_local_cache(uint64_t tenant_id, ObOptDmlStat &dml_stat);
  int update_column_usage_table(const bool with_check);
  int update_dml_stat_info(const bool with_check);
  int update_tenant_column_usage_info(uint64_t tenant_id);
  int update_tenant_dml_stat_info(uint64_t tenant_id);
  int erase_opt_stat_monitoring_info_map(uint64_t tenant_id);
  int erase_column_usage_map(uint64_t tenant_id);
  int erase_dml_stat_map(uint64_t tenant_id);
  int get_column_usage_sql(const uint64_t tenant_id,
                           const StatKey &col_key,
                           const int64_t flags,
                           const bool need_add_comma,
                           ObSqlString &sql_string);
  int get_dml_stat_sql(const uint64_t tenant_id,
                       const StatKey &dml_stat_key,
                       const ObOptDmlStat &dml_stat,
                       const bool need_add_comma,
                       ObSqlString &sql_string);
  int exec_insert_column_usage_sql(uint64_t tenant_id, ObSqlString &values_sql);
  int exec_insert_monitor_modified_sql(uint64_t tenant_id,
                                       ObSqlString &values_sql);
  int get_column_usage_from_table(sql::ObExecContext &ctx,
                                  ObIArray<ObColumnStatParam *> &column_params,
                                  uint64_t tenant_id,
                                  uint64_t table_id);
  int construct_get_column_usage_sql(ObIArray<ObColumnStatParam *> &column_params,
                                     const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     ObSqlString &select_sql);

  int check_table_writeable(const uint64_t tenant_id, bool &is_writeable);

  int generate_opt_stat_monitoring_info_rows(observer::ObOptDmlStatMapsGetter &getter);

  int clean_useless_dml_stat_info(uint64_t tenant_id);

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptStatMonitorManager);
  const static int64_t UPDATE_OPT_STAT_BATCH_CNT = 200;
  const static int64_t info_count = 8;
  ObOptStatMonitorFlushAllTask flush_all_task_;
  ObOptStatMonitorCheckTask check_task_;
  ColumnUsageMaps column_usage_maps_;
  DmlStatMaps dml_stat_maps_;
  bool inited_;
  ObMySQLProxy *mysql_proxy_;
}; // end of class ObOptStatMonitorManager

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_OPT_STAT_MONITOR_MANAGER_H_ */
