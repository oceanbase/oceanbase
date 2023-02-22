// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <jianming.cjq@alipay.com>

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_struct.h"

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
  // table ctx holds a reference count
  int add_table_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *table_ctx);
  int remove_table_ctx(const ObTableLoadUniqueKey &key);
  // table ctx holds a reference count
  int get_table_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *&table_ctx);
  // table ctx holds a reference count
  int get_table_ctx_by_table_id(uint64_t table_id, ObTableLoadTableCtx *&table_ctx);
  // all table ctx hold a reference count
  int get_inactive_table_ctx_list(common::ObIArray<ObTableLoadTableCtx *> &table_ctx_array);
  void put_table_ctx(ObTableLoadTableCtx *table_ctx);
  // table ctx no reference counting
  int get_releasable_table_ctx_list(common::ObIArray<ObTableLoadTableCtx *> &table_ctx_array);
public:
  int add_dirty_list(ObTableLoadTableCtx *table_ctx);
private:
  struct TableHandle
  {
  public:
    TableHandle() : table_ctx_(nullptr) {}
    TO_STRING_KV(K_(key), KP_(table_ctx));
  public:
    ObTableLoadUniqueKey key_;
    ObTableLoadTableCtx *table_ctx_;
  };
  // key => table_ctx
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, ObTableLoadTableCtx *,
                                  common::hash::NoPthreadDefendMode>
    TableCtxMap;
  // table_id => table_handle
  typedef common::hash::ObHashMap<uint64_t, TableHandle, common::hash::NoPthreadDefendMode>
    TableHandleMap;
  mutable obsys::ObRWLock rwlock_;
  TableCtxMap table_ctx_map_;
  TableHandleMap table_handle_map_; // index of the latest task
  lib::ObMutex mutex_;
  common::ObDList<ObTableLoadTableCtx> dirty_list_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
