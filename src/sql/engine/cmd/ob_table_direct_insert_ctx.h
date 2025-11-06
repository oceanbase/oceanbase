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
#include "lib/compress/ob_compress_util.h"
#include "src/storage/direct_load/ob_direct_load_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
}
}
namespace observer
{
class ObTableLoadExecCtx;
class ObTableLoadInstance;
}
namespace common
{
class ObTabletID;
}
namespace storage
{
struct ObDirectLoadLevel;
}

namespace sql
{
class ObExecContext;
class ObPhysicalPlan;
class ObSqlCtx;

class ObTableDirectInsertCtx
{
public:
  ObTableDirectInsertCtx()
    : load_exec_ctx_(nullptr),
      table_load_instance_(nullptr),
      is_inited_(false),
      is_direct_(false),
      force_inc_direct_write_(false) {}
  ~ObTableDirectInsertCtx();
  TO_STRING_KV(K_(is_inited), K_(is_direct), K_(force_inc_direct_write));

public:
  int init(sql::ObExecContext *exec_ctx,
           sql::ObPhysicalPlan &phy_plan,
           const uint64_t table_id,
           const int64_t parallel,
           const bool is_incremental,
           const bool enable_inc_replace,
           const bool is_insert_overwrite,
           const double online_sample_percent,
           const bool is_online_gather_statistics);
  int commit();
  int finish();
  void destroy();

  bool get_is_direct() const { return is_direct_; }
  void set_is_direct(bool is_direct) { is_direct_ = is_direct; }
  bool get_force_inc_direct_write() const { return force_inc_direct_write_; }
  void set_force_inc_direct_write(const bool force_inc_direct_write) {
    force_inc_direct_write_ = force_inc_direct_write;
  }

private:
  int get_partition_level_tablet_ids(const sql::ObPhysicalPlan &phy_plan,
                                     const share::schema::ObTableSchema *table_schema,
                                     common::ObIArray<common::ObTabletID> &tablet_ids);
private:
  observer::ObTableLoadExecCtx *load_exec_ctx_;
  observer::ObTableLoadInstance *table_load_instance_;
  bool is_inited_;
  bool is_direct_; //indict whether the plan is direct load plan including insert into append and load data direct
  bool force_inc_direct_write_;
};
} // namespace observer
} // namespace oceanbase
