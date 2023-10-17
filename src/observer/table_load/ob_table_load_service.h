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
#include "observer/table_load/ob_table_load_client_service.h"
#include "observer/table_load/ob_table_load_manager.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;

class ObTableLoadService
{
public:
  static int mtl_init(ObTableLoadService *&service);
  static int check_tenant();
  static int check_support_direct_load(uint64_t table_id);
  static ObTableLoadTableCtx *alloc_ctx();
  static void free_ctx(ObTableLoadTableCtx *table_ctx);
  static int add_ctx(ObTableLoadTableCtx *table_ctx);
  static int remove_ctx(ObTableLoadTableCtx *table_ctx);
  // get ctx
  static int get_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *&table_ctx);
  // get ctx by table_id
  static int get_ctx(const ObTableLoadKey &key, ObTableLoadTableCtx *&table_ctx);
  static void put_ctx(ObTableLoadTableCtx *table_ctx);

  // for direct load control api
  static int direct_load_control(const ObDirectLoadControlRequest &request,
                                 ObDirectLoadControlResult &result, common::ObIAllocator &allocator)
  {
    return ObTableLoadControlRpcProxy::dispatch(request, result, allocator);
  }
public:
  ObTableLoadService();
  int init(uint64_t tenant_id);
  int start();
  int stop();
  void wait();
  void destroy();
  ObTableLoadManager &get_manager() { return manager_; }
  ObTableLoadClientService &get_client_service() { return client_service_; }
private:
  void abort_all_client_task();
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
    ObCheckTenantTask(ObTableLoadService &service)
      : service_(service), tenant_id_(common::OB_INVALID_ID), is_inited_(false) {}
    virtual ~ObCheckTenantTask() = default;
    int init(uint64_t tenant_id);
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
    uint64_t tenant_id_;
    bool is_inited_;
  };
  class ObHeartBeatTask : public common::ObTimerTask
  {
  public:
    ObHeartBeatTask(ObTableLoadService &service)
      : service_(service), tenant_id_(common::OB_INVALID_ID), is_inited_(false) {}
    virtual ~ObHeartBeatTask() = default;
    int init(uint64_t tenant_id);
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
    uint64_t tenant_id_;
    bool is_inited_;
  };
  class ObGCTask : public common::ObTimerTask
  {
  public:
    ObGCTask(ObTableLoadService &service)
      : service_(service), tenant_id_(common::OB_INVALID_ID), is_inited_(false) {}
    virtual ~ObGCTask() = default;
    int init(uint64_t tenant_id);
    void runTimerTask() override;
  private:
    bool gc_heart_beat_expired_ctx(ObTableLoadTableCtx *table_ctx);
    bool gc_table_not_exist_ctx(ObTableLoadTableCtx *table_ctx);
  private:
    ObTableLoadService &service_;
    uint64_t tenant_id_;
    bool is_inited_;
  };
  class ObReleaseTask : public common::ObTimerTask
  {
  public:
    ObReleaseTask(ObTableLoadService &service)
      : service_(service), tenant_id_(common::OB_INVALID_ID), is_inited_(false) {}
    virtual ~ObReleaseTask() = default;
    int init(uint64_t tenant_id);
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
    uint64_t tenant_id_;
    bool is_inited_;
  };
  class ObClientTaskAutoAbortTask : public common::ObTimerTask
  {
  public:
    ObClientTaskAutoAbortTask(ObTableLoadService &service)
      : service_(service), tenant_id_(common::OB_INVALID_ID), is_inited_(false)
    {
    }
    virtual ~ObClientTaskAutoAbortTask() = default;
    int init(uint64_t tenant_id);
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
    uint64_t tenant_id_;
    bool is_inited_;
  };
  class ObClientTaskPurgeTask : public common::ObTimerTask
  {
  public:
    ObClientTaskPurgeTask(ObTableLoadService &service)
      : service_(service), tenant_id_(common::OB_INVALID_ID), is_inited_(false)
    {
    }
    virtual ~ObClientTaskPurgeTask() = default;
    int init(uint64_t tenant_id);
    void runTimerTask() override;
  private:
    ObTableLoadService &service_;
    uint64_t tenant_id_;
    bool is_inited_;
  };
private:
  ObTableLoadManager manager_;
  ObTableLoadClientService client_service_;
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

}  // namespace observer
}  // namespace oceanbase
