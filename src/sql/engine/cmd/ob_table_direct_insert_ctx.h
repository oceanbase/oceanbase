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
class ObTableLoadSqlExecCtx;
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
      is_inited_(false) {}
  ~ObTableDirectInsertCtx();
  TO_STRING_KV(K_(is_inited));
public:
  int init(sql::ObExecContext *exec_ctx, const uint64_t table_id, const int64_t parallel);
  int commit();
  int finish();
  void destroy();
private:
  int init_store_column_idxs(const uint64_t tenant_id, const uint64_t table_id,
                             common::ObIArray<int64_t> &store_column_idxs);
private:
  observer::ObTableLoadSqlExecCtx *load_exec_ctx_;
  observer::ObTableLoadInstance *table_load_instance_;
  bool is_inited_;
};
} // namespace observer
} // namespace oceanbase