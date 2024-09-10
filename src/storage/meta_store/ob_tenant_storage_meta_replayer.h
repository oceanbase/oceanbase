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
#ifndef OCEANBASE_STORAGE_META_STORE_OB_TENANT_STORAGE_META_REPLAYER_H_
#define OCEANBASE_STORAGE_META_STORE_OB_TENANT_STORAGE_META_REPLAYER_H_

#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace blocksstable
{
class ObStorageObjectOpt;
}
namespace storage
{
class ObTenantSuperBlock;
class ObLSItem;
class ObTenantStorageMetaPersister;
class ObTenantCheckpointSlogHandler;
class ObTenantStorageMetaReplayer
{
public:
  ObTenantStorageMetaReplayer()
    : is_inited_(false),
      is_shared_storage_(false) {}
  ObTenantStorageMetaReplayer(const ObTenantStorageMetaReplayer &) = delete;
  ObTenantStorageMetaReplayer &operator=(const ObTenantStorageMetaReplayer &) = delete;

  int init(const bool is_share_storage,
           ObTenantStorageMetaPersister &persister,
           ObTenantCheckpointSlogHandler &ckpt_slog_handler);
  void destroy();
  int start_replay(const ObTenantSuperBlock &super_block);

private:
#ifdef OB_BUILD_SHARED_STORAGE
  int ss_start_replay_(const ObTenantSuperBlock &super_block);
  int s2_replay_create_ls_(ObArenaAllocator &allocator, const ObLSItem &item);
  int s2_replay_ls_dup_table_meta_(ObArenaAllocator &allocator, const ObLSItem &item);
  int s2_replay_ls_tablets_(ObArenaAllocator &allocator, const ObLSItem &item);
  int s2_replay_ls_tablets_for_trans_info_tmp_(ObArenaAllocator &allocator, const ObLSItem &item);
#endif

private:
  bool is_inited_;
  bool is_shared_storage_;
  ObTenantStorageMetaPersister *persister_;
  ObTenantCheckpointSlogHandler *ckpt_slog_handler_;
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTALE_OB_TENANT_META_REPLAYER_H_