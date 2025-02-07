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
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadOpenInsertTableCtxManager
{
public:
  ObTableLoadOpenInsertTableCtxManager(ObTableLoadStoreCtx *store_ctx)
    : store_ctx_(store_ctx),
      insert_tablet_context_idx_(0),
      opened_insert_tablet_count_(0),
      is_inited_(false)
  {
  }
  int get_next_insert_tablet_ctx(ObDirectLoadInsertTabletContext *&tablet_ctx);
  void handle_open_insert_tablet_ctx_finish(bool &is_finish);
  int init();
private:
  ObTableLoadStoreCtx * store_ctx_;
  common::ObArray<ObDirectLoadInsertTabletContext *> insert_tablet_ctxs_;
  int insert_tablet_context_idx_;
  int opened_insert_tablet_count_;
  lib::ObMutex op_lock_;
  bool is_inited_;
};
}
} // namespace oceanbase