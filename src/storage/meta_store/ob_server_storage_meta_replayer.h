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
#ifndef OCEANBASE_STORAGE_META_STORE_OB_SERVER_STORAGE_META_REPLAYER_H_
#define OCEANBASE_STORAGE_META_STORE_OB_SERVER_STORAGE_META_REPLAYER_H_

#include "observer/omt/ob_tenant_meta.h"
#include "lib/hash/ob_hashmap.h"


namespace oceanbase
{
namespace storage
{
class ObServerStorageMetaPersister;
class ObServerCheckpointSlogHandler;
class ObServerStorageMetaReplayer
{
public:
  ObServerStorageMetaReplayer()
    : is_inited_(false),
      is_shared_storage_(false),
      persister_(nullptr),
      ckpt_slog_handler_(nullptr) {}
  ObServerStorageMetaReplayer(const ObServerStorageMetaReplayer &) = delete;
  ObServerStorageMetaReplayer &operator=(const ObServerStorageMetaReplayer &) = delete;

  int init(const bool is_share_storage,
           ObServerStorageMetaPersister &persister,
           ObServerCheckpointSlogHandler &ckpt_slog_handler);
  int start_replay();
  void destroy();

private:
  typedef common::hash::ObHashMap<uint64_t, omt::ObTenantMeta> TENANT_META_MAP;
  int apply_replay_result_(const TENANT_META_MAP &tenant_meta_map);
  int handle_tenant_creating_(const uint64_t tenant_id, const omt::ObTenantMeta &tenant_meta);
  int handle_tenant_create_commit_(const omt::ObTenantMeta &tenant_meta);
  int handle_tenant_deleting_(const uint64_t tenant_id, const omt::ObTenantMeta &tenant_meta);
  static int finish_storage_meta_replay_();
  static int online_ls_();

#ifdef OB_BUILD_SHARED_STORAGE
  int ss_start_replay_(TENANT_META_MAP &tenant_meta_map) const;
  int ss_read_tenant_super_block_(ObArenaAllocator &allocator, const ObTenantItem &item, ObTenantSuperBlock &super_block) const;
  int ss_read_tenant_unit_(ObArenaAllocator &allocator, const ObTenantItem &item, share::ObUnitInfoGetter::ObTenantConfig &unit) const;
#endif

private:
  bool is_inited_;
  bool is_shared_storage_;
  ObServerStorageMetaPersister *persister_;
  ObServerCheckpointSlogHandler *ckpt_slog_handler_;
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTALE_OB_STORAGE_META_REPLAYER_H_