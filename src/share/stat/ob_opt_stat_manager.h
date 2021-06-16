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

#ifndef _OB_OPT_STAT_MANAGER_H_
#define _OB_OPT_STAT_MANAGER_H_

#include "lib/queue/ob_dedup_queue.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_stat_sql_service.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace common {

class ObOptStatService;
class ObOptColumnStatHandle;

struct ObOptTableStatVersion final {
public:
  ObOptTableStatVersion() : stat_version_(0)
  {
    key_.reset();
  }
  void reset()
  {
    key_.reset();
    stat_version_ = 0;
  }
  TO_STRING_KV(K_(key), K_(stat_version));
  ObOptTableStat::Key key_;
  int64_t stat_version_;
};

class ObOptStatManager {
public:
  ObOptStatManager();
  virtual ~ObOptStatManager()
  {}
  virtual int init(ObOptStatService* stat_service, ObMySQLProxy* proxy, ObServerConfig* config);
  virtual int get_column_stat(const ObOptColumnStat::Key& key, ObOptColumnStatHandle& handle);
  virtual int update_column_stat(const common::ObIArray<ObOptColumnStat*>& column_stats);
  virtual int delete_column_stat(const common::ObIArray<ObOptColumnStat*>& column_stats);
  virtual int refresh_column_stat(const ObOptColumnStat::Key& key);
  virtual int refresh_table_stat(const ObOptTableStat::Key& key);

  virtual int get_table_stat(const ObOptTableStat::Key& key, ObOptTableStat& tstat);
  virtual int add_refresh_stat_task(const obrpc::ObUpdateStatCacheArg& analyze_arg);
  static ObOptTableStat& get_default_table_stat();
  static ObOptStatManager& get_instance()
  {
    static ObOptStatManager instance_;
    return instance_;
  }
  bool is_inited() const
  {
    return inited_;
  }
  class ObRefreshStatTask : public common::IObDedupTask {
  public:
    explicit ObRefreshStatTask(ObOptStatManager* manager)
        : common::IObDedupTask(T_REFRESH_OPT_STAT), stat_manager_(manager)
    {}
    virtual ~ObRefreshStatTask()
    {}
    virtual int64_t hash() const
    {
      return 0;
    }
    virtual bool operator==(const common::IObDedupTask& other) const
    {
      UNUSED(other);
      return false;
    }
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual common::IObDedupTask* deep_copy(char* buffer, const int64_t buf_size) const;
    virtual int64_t get_abs_expired_time() const
    {
      return 0;
    }
    virtual int process();
    int init(const obrpc::ObUpdateStatCacheArg& analyze_arg)
    {
      return analyze_arg_.assign(analyze_arg);
    }

  protected:
    ObOptStatManager* stat_manager_;
    obrpc::ObUpdateStatCacheArg analyze_arg_;
  };

protected:
  static const int64_t REFRESH_STAT_TASK_NUM = 5;
  bool inited_;
  common::ObDedupQueue refresh_stat_task_queue_;
  ObOptStatService* stat_service_;
  ObOptStatSqlService sql_service_;
  int64_t last_schema_version_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* _OB_OPT_STAT_MANAGER_H_ */
