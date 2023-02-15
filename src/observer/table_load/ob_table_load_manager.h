// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <jianming.cjq@alipay.com>

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;

class ObTableLoadManager
{
public:
  ObTableLoadManager();
  ~ObTableLoadManager();
  int init();
  int add_table_ctx(uint64_t table_id, ObTableLoadTableCtx *table_ctx);
  int remove_table_ctx(uint64_t table_id);
  // table ctx holds a reference count
  int get_table_ctx(uint64_t table_id, ObTableLoadTableCtx *&table_ctx);
  // all table ctx hold a reference count
  int get_inactive_table_ctx_list(common::ObIArray<ObTableLoadTableCtx *> &table_ctx_array);
  void put_table_ctx(ObTableLoadTableCtx *table_ctx);
  // table ctx no reference counting
  int get_releasable_table_ctx_list(common::ObIArray<ObTableLoadTableCtx *> &table_ctx_array);
private:
  struct TableCtxHandle
  {
  public:
    TableCtxHandle();
    TableCtxHandle(ObTableLoadTableCtx *table_ctx);
    TableCtxHandle(const TableCtxHandle &other);
    ~TableCtxHandle();
    void reset();
    void set(ObTableLoadTableCtx *table_ctx);
    TableCtxHandle &operator=(const TableCtxHandle &other);
    OB_INLINE ObTableLoadTableCtx *get() const { return table_ctx_; }
    OB_INLINE ObTableLoadTableCtx *take()
    {
      ObTableLoadTableCtx *table_ctx = table_ctx_;
      table_ctx_ = nullptr;
      return table_ctx;
    }
  private:
    ObTableLoadTableCtx *table_ctx_;
  };
  typedef common::hash::ObHashMap<uint64_t, TableCtxHandle> TableCtxMap;
  TableCtxMap table_ctx_map_;
  lib::ObMutex mutex_;
  common::ObDList<ObTableLoadTableCtx> dirty_list_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
