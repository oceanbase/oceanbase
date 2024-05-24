// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

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
      is_inited_(false),
      is_direct_(false) {}
  ~ObTableDirectInsertCtx();
  TO_STRING_KV(K_(is_inited));
public:
  int init(sql::ObExecContext *exec_ctx, const uint64_t table_id, const int64_t parallel);
  int commit();
  int finish();
  void destroy();

  bool get_is_direct() const { return is_direct_; }
  void set_is_direct(bool is_direct) { is_direct_ = is_direct; }

private:
  int init_store_column_idxs(const uint64_t tenant_id, const uint64_t table_id,
                             common::ObIArray<int64_t> &store_column_idxs);
  int get_compressor_type(const uint64_t tenant_id, const uint64_t table_id, const int64_t parallel,
                          ObCompressorType &compressor_type);
private:
  observer::ObTableLoadSqlExecCtx *load_exec_ctx_;
  observer::ObTableLoadInstance *table_load_instance_;
  bool is_inited_;
  bool is_direct_; //indict whether the plan is direct load plan including insert into append and load data direct
};
} // namespace observer
} // namespace oceanbase
