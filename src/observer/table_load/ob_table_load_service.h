// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

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
  static int create_ctx(const ObTableLoadParam &param, ObTableLoadTableCtx *&ctx, bool &is_new);
  static int get_ctx(const ObTableLoadKey &key, ObTableLoadTableCtx *&ctx);
  static void put_ctx(ObTableLoadTableCtx *ctx);
  static int remove_ctx(ObTableLoadTableCtx *ctx);
public:
  ObTableLoadService();
  int init(uint64_t tenant_id);
  int start();
  int stop();
  void wait();
  void destroy();
public:
  int create_table_ctx(const ObTableLoadParam &param, ObTableLoadTableCtx *&ctx, bool &is_new);
  int remove_table_ctx(ObTableLoadTableCtx *ctx);
  int get_table_ctx(uint64_t table_id, ObTableLoadTableCtx *&ctx);
  void put_table_ctx(ObTableLoadTableCtx *ctx);
private:
  static const int64_t GC_INTERVAL = 30LL * 1000 * 1000; // 30s
  static const int64_t RELEASE_INTERVAL = 1LL * 1000 * 1000; // 1s
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
  common::ObTimer gc_timer_;
  ObGCTask gc_task_;
  ObReleaseTask release_task_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
