// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/task/ob_timer.h"
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
public:
  ObTableLoadService();
  int init(uint64_t tenant_id);
  int start();
  int stop();
  void wait();
  void destroy();
  ObTableLoadManager &get_manager() { return manager_; }
private:
  void fail_all_ctx(int error_code);
  void release_all_ctx();
private:
  static const int64_t CHECK_TENANT_INTERVAL = 1LL * 1000 * 1000; // 1s
  static const int64_t GC_INTERVAL = 30LL * 1000 * 1000; // 30s
  static const int64_t RELEASE_INTERVAL = 1LL * 1000 * 1000; // 1s
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
  class ObGCTask : public common::ObTimerTask
  {
  public:
    ObGCTask(ObTableLoadService &service)
      : service_(service), tenant_id_(common::OB_INVALID_ID), is_inited_(false) {}
    virtual ~ObGCTask() = default;
    int init(uint64_t tenant_id);
    void runTimerTask() override;
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
private:
  ObTableLoadManager manager_;
  common::ObTimer timer_;
  ObCheckTenantTask check_tenant_task_;
  ObGCTask gc_task_;
  ObReleaseTask release_task_;
  volatile bool is_stop_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
