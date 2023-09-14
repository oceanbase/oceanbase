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

#ifndef OB_SERVER_SCHEMA_UPDATER_H
#define OB_SERVER_SCHEMA_UPDATER_H

#include "share/ob_define.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/net/ob_addr.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_uniq_task_queue.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
struct ObRefreshSchemaInfo;
}
}
namespace observer
{
class ObServerSchemaTask;
class ObServerSchemaUpdater;
typedef ObUniqTaskQueue<ObServerSchemaTask, ObServerSchemaUpdater> ObServerSchemaTaskQueue;

class ObServerSchemaTask : public common::ObDLinkBase<ObServerSchemaTask>
{
public:
  friend class ObServerSchemaUpdater;
  enum TYPE {
    ASYNC_REFRESH,  // async schema refresh task caused by sql
    REFRESH,        // schema refresh task caused by heartbeat
    RELEASE,        // schema memory release task
    INVALID
  };
  ObServerSchemaTask();
  // for refresh
  explicit ObServerSchemaTask(TYPE type,
                              bool did_retry);
  // for refresh
  explicit ObServerSchemaTask(TYPE type,
                              bool did_retry,
                              const share::schema::ObRefreshSchemaInfo &schema_info);
  // for release
  explicit ObServerSchemaTask(TYPE type);
  // for async refresh
  explicit ObServerSchemaTask(TYPE type,
                              const uint64_t tenant_id,
                              const int64_t schema_version);
  virtual ~ObServerSchemaTask() {}

  bool need_process_alone() const;
  bool is_valid() const;

  virtual int64_t hash() const;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  virtual bool operator ==(const ObServerSchemaTask &other) const;
  virtual bool compare_without_version(const ObServerSchemaTask &other) const;
  bool operator <(const ObServerSchemaTask &other) const;
  static bool greator_than(const ObServerSchemaTask &lt,
                           const ObServerSchemaTask &rt);
  uint64_t get_group_id() const;
  bool is_barrier() const;

  uint64_t get_tenant_id() const { return schema_info_.get_tenant_id(); }
  uint64_t get_schema_version() const { return schema_info_.get_schema_version(); }

  inline bool need_assign_when_equal() const { return false; }
  inline int assign_when_equal(const ObServerSchemaTask &other)
  {
    UNUSED(other);
    return common::OB_NOT_SUPPORTED;
  }

  TO_STRING_KV(K_(type), K_(did_retry), K_(schema_info));

private:
  TYPE type_;
  bool did_retry_;
  share::schema::ObRefreshSchemaInfo schema_info_;
};

class ObServerSchemaUpdater
{
public:
  ObServerSchemaUpdater() : schema_mgr_(NULL), inited_(false)
  {}
  ~ObServerSchemaUpdater() { destroy(); }
  int init(const common::ObAddr &host_, share::schema::ObMultiVersionSchemaService *schema_mgr);
  void destroy();
  void stop();
  void wait();

  int try_reload_schema(const share::schema::ObRefreshSchemaInfo &schema_info,
                        const bool set_received_schema_version);
  int try_release_schema();
  int async_refresh_schema(
      const uint64_t tenant_id,
      const int64_t schema_version);
  int process_barrier(const ObServerSchemaTask &task, bool &stopped);
  int batch_process_tasks(const common::ObIArray<ObServerSchemaTask> &batch_tasks, bool &stopped);
private:
  int process_refresh_task(const ObServerSchemaTask &task);
  int process_release_task();
  int process_async_refresh_tasks(const common::ObIArray<ObServerSchemaTask> &tasks);

  int try_load_baseline_schema_version_();
private:
  static const int32_t SSU_MAX_THREAD_NUM = 1;
  static const int64_t SSU_TASK_QUEUE_SIZE = 1024;
  static const int64_t SSU_TASK_MAP_SIZE = 1024;
  common::ObAddr host_;
  share::schema::ObMultiVersionSchemaService *schema_mgr_;
  ObServerSchemaTaskQueue task_queue_;
  bool inited_;
};

}
}

#endif
