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

#pragma once

#include "lib/task/ob_timer.h"
#include "observer/table_load/control/ob_table_load_control_rpc_proxy.h"
#include "observer/table_load/ob_table_load_assigned_memory_manager.h"
#include "observer/table_load/ob_table_load_assigned_task_manager.h"
#include "observer/table_load/ob_table_load_client_service.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_manager.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "observer/table_load/resource/ob_table_load_resource_rpc_proxy.h"
#include "observer/table_load/resource/ob_table_load_resource_service.h"
#include "storage/direct_load/ob_direct_load_struct.h"
#include "src/observer/table_load/ob_table_load_assigned_memory_manager.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadClientTask;

class ObTableLoadService
{
public:
  static int mtl_new(ObTableLoadService *&service);
  static void mtl_destroy(ObTableLoadService *&service);
  static int check_tenant();
  // 旁路导入内核获取加表锁后的schema进行检查
  static int check_support_direct_load(uint64_t table_id,
                                       const storage::ObDirectLoadMethod::Type method,
                                       const storage::ObDirectLoadInsertMode::Type insert_mode,
                                       const storage::ObDirectLoadMode::Type load_mode,
                                       const storage::ObDirectLoadLevel::Type load_level,
                                       const common::ObIArray<uint64_t> &column_ids);
  // 业务层指定schema_guard进行检查
  static int check_support_direct_load(share::schema::ObSchemaGetterGuard &schema_guard,
                                       uint64_t table_id,
                                       const storage::ObDirectLoadMethod::Type method,
                                       const storage::ObDirectLoadInsertMode::Type insert_mode,
                                       const storage::ObDirectLoadMode::Type load_mode,
                                       const storage::ObDirectLoadLevel::Type load_level,
                                       const common::ObIArray<uint64_t> &column_ids);
  static int check_support_direct_load(share::schema::ObSchemaGetterGuard &schema_guard,
                                       const share::schema::ObTableSchema *table_schema,
                                       const storage::ObDirectLoadMethod::Type method,
                                       const storage::ObDirectLoadInsertMode::Type insert_mode,
                                       const storage::ObDirectLoadMode::Type load_mode,
                                       const storage::ObDirectLoadLevel::Type load_level,
                                       const common::ObIArray<uint64_t> &column_ids);
  static int check_support_direct_load_for_columns(const share::schema::ObTableSchema *table_schema,
                                                   const storage::ObDirectLoadMode::Type load_mode);
  static int check_support_direct_load_for_default_value(const share::schema::ObTableSchema *table_schema,
                                                         const common::ObIArray<uint64_t> &column_ids);
  static int check_support_direct_load_for_partition_level(ObSchemaGetterGuard &schema_guard,
                                                           const ObTableSchema *table_schema,
                                                           const ObDirectLoadMethod::Type method,
                                                           const uint64_t compat_version);

  static int alloc_ctx(ObTableLoadTableCtx *&table_ctx);
  static void free_ctx(ObTableLoadTableCtx *table_ctx);
  static int add_ctx(ObTableLoadTableCtx *table_ctx);
  static int remove_ctx(ObTableLoadTableCtx *table_ctx);
  static int get_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *&table_ctx);
  static void put_ctx(ObTableLoadTableCtx *table_ctx);

  // for direct load control api
  static int direct_load_control(const ObDirectLoadControlRequest &request,
                                 ObDirectLoadControlResult &result, common::ObIAllocator &allocator)
  {
    return ObTableLoadControlRpcProxy::dispatch(request, result, allocator);
  }
  // for direct load resource api
  static int direct_load_resource(const ObDirectLoadResourceOpRequest &request,
                                  ObDirectLoadResourceOpResult &result, common::ObIAllocator &allocator)
  {
    return ObTableLoadResourceRpcProxy::dispatch(request, result, allocator);
  }
  static int get_memory_limit(int64_t &memory_limit);
  static int add_assigned_task(ObDirectLoadResourceApplyArg &arg);
  static int delete_assigned_task(ObDirectLoadResourceReleaseArg &arg);
  static int assign_memory(bool is_sort, int64_t assign_memory);
  static int recycle_memory(bool is_sort, int64_t assign_memory);
	static int get_sort_memory(int64_t &sort_memory);
  static int refresh_and_check_resource(ObDirectLoadResourceCheckArg &arg, ObDirectLoadResourceOpRes &res);
public:
  ObTableLoadService(const uint64_t tenant_id);
  int init();
  int start();
  int stop();
  void wait();
  void destroy();
  bool is_stop() const { return is_stop_; }
  ObTableLoadManager &get_manager() { return manager_; }
private:
  void abort_all_client_task(int error_code);
  void fail_all_ctx(int error_code);
  void release_all_ctx();
private:
  static const int64_t CHECK_TENANT_INTERVAL = 1LL * 1000 * 1000; // 1s
  static const int64_t HEART_BEEAT_INTERVAL = 10LL * 1000 * 1000; // 10s
  static const int64_t HEART_BEEAT_EXPIRED_TIME_US = 30LL * 1000 * 1000; // 30s
  static const int64_t GC_INTERVAL = 30LL * 1000 * 1000; // 30s
  static const int64_t RELEASE_INTERVAL = 1LL * 1000 * 1000; // 1s
  static const int64_t CLIENT_TASK_AUTO_ABORT_INTERVAL = 1LL * 1000 * 1000; // 1s
  static const int64_t CLIENT_TASK_PURGE_INTERVAL = 1LL * 1000 * 1000; // 1s
  class ObCheckTenantTask : public common::ObTimerTask
  {
  public:
    ObCheckTenantTask(ObTableLoadService &service) : service_(service) {}
    virtual ~ObCheckTenantTask() = default;
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
  };
  class ObHeartBeatTask : public common::ObTimerTask
  {
  public:
    ObHeartBeatTask(ObTableLoadService &service) : service_(service) {}
    virtual ~ObHeartBeatTask() = default;
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
  };
  class ObGCTask : public common::ObTimerTask
  {
  public:
    ObGCTask(ObTableLoadService &service) : service_(service) {}
    virtual ~ObGCTask() = default;
    void runTimerTask() override;
  private:
    bool gc_mark_delete(ObTableLoadTableCtx *table_ctx);
    bool gc_heart_beat_expired_ctx(ObTableLoadTableCtx *table_ctx);
    bool gc_table_not_exist_ctx(ObTableLoadTableCtx *table_ctx);
  private:
    ObTableLoadService &service_;
  };
  class ObReleaseTask : public common::ObTimerTask
  {
  public:
    ObReleaseTask(ObTableLoadService &service) : service_(service) {}
    virtual ~ObReleaseTask() = default;
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
  };
  class ObClientTaskAutoAbortTask : public common::ObTimerTask
  {
  public:
    ObClientTaskAutoAbortTask(ObTableLoadService &service) : service_(service) {}
    virtual ~ObClientTaskAutoAbortTask() = default;
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
  };
  class ObClientTaskPurgeTask : public common::ObTimerTask
  {
  public:
    static const int64_t CLIENT_TASK_BRIEF_RETENTION_PERIOD = 24LL * 60 * 60 * 1000 * 1000; // 1day
  public:
    ObClientTaskPurgeTask(ObTableLoadService &service) : service_(service) {}
    virtual ~ObClientTaskPurgeTask() = default;
    void runTimerTask() override;
  private:
    int add_client_task_brief(ObTableLoadClientTask *client_task);
    void purge_client_task();
    void purge_client_task_brief();
  private:
    ObTableLoadService &service_;
  };
private:
  const uint64_t tenant_id_;
  ObTableLoadManager manager_;
  ObTableLoadAssignedMemoryManager assigned_memory_manager_;
  ObTableLoadAssignedTaskManager assigned_task_manager_;
  common::ObTimer timer_;
  ObCheckTenantTask check_tenant_task_;
  ObHeartBeatTask heart_beat_task_;
  ObGCTask gc_task_;
  ObReleaseTask release_task_;
  ObClientTaskAutoAbortTask client_task_auto_abort_task_;
  ObClientTaskPurgeTask client_task_purge_task_;
  volatile bool is_stop_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
