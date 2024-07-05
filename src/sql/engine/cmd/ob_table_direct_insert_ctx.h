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

#pragma once

#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadExecCtx;
class ObTableLoadInstance;
}

namespace sql
{
class ObExecContext;

class ObTableDirectInsertCtx
{
public:
  ObTableDirectInsertCtx()
    : load_exec_ctx_(nullptr),
      table_load_instance_(nullptr),
      is_inited_(false),
      is_direct_(false),
      is_online_gather_statistics_(false),
      online_sample_percent_(1.) {}
  ~ObTableDirectInsertCtx();
  TO_STRING_KV(K_(is_inited), K_(is_direct),
               K_(is_online_gather_statistics));
public:
  int init(sql::ObExecContext *exec_ctx,
           sql::ObPhysicalPlan &phy_plan,
           const uint64_t table_id,
           const int64_t parallel,
           const bool is_incremental,
           const bool enable_inc_replace,
           const bool is_insert_overwrite,
           const double online_sample_percent);
  int commit();
  int finish();
  void destroy();

  bool get_is_direct() const { return is_direct_; }
  void set_is_direct(bool is_direct) { is_direct_ = is_direct; }
  bool get_is_online_gather_statistics() const {
    return is_online_gather_statistics_;
  }

  void set_is_online_gather_statistics(const bool is_online_gather_statistics) {
    is_online_gather_statistics_ = is_online_gather_statistics;
  }

  void set_online_sample_percent(double online_sample_percent) {
    online_sample_percent_ = online_sample_percent;
  }

  double get_online_sample_percent() const {
    return online_sample_percent_;
  }

private:
  int get_compressor_type(const uint64_t tenant_id, const uint64_t table_id, const int64_t parallel,
                          ObCompressorType &compressor_type);
private:
  observer::ObTableLoadExecCtx *load_exec_ctx_;
  observer::ObTableLoadInstance *table_load_instance_;
  bool is_inited_;
  bool is_direct_; //indict whether the plan is direct load plan including insert into append and load data direct
  bool is_online_gather_statistics_;
  double online_sample_percent_;
};
} // namespace observer
} // namespace oceanbase
