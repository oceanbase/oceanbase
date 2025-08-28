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
#ifndef OCEANBASE_STORAGE_META_STORE_OB_SERVER_STORAGE_META_PERSISTER_H_
#define OCEANBASE_STORAGE_META_STORE_OB_SERVER_STORAGE_META_PERSISTER_H_

#include "share/ob_unit_getter.h"

namespace oceanbase
{
namespace omt
{
class ObTenantMeta; 
class ObTenantConfig;
  
}
namespace storage
{
class ObStorageLogger;
class ObTenantSuperBlock;
class ObServerStorageMetaPersister
{
public:
  ObServerStorageMetaPersister()
    : is_inited_(false), is_shared_storage_(false),
      server_slogger_(nullptr) {}
  ObServerStorageMetaPersister(const ObServerStorageMetaPersister &) = delete;
  ObServerStorageMetaPersister &operator=(const ObServerStorageMetaPersister &) = delete;
      
  int init(const bool is_share_storage, ObStorageLogger *server_slogger);
  int start();
  void stop();
  void wait();
  void destroy();
  int prepare_create_tenant(const omt::ObTenantMeta &meta, int64_t &epoch);
  int commit_create_tenant(const uint64_t tenant_id, const int64_t epoch);
  int abort_create_tenant(const uint64_t tenant_id, const int64_t epoch);
  int prepare_delete_tenant(const uint64_t tenant_id, const int64_t epoch);
  int commit_delete_tenant(const uint64_t tenant_id, const int64_t epoch);
  int update_tenant_super_block(const int64_t tenant_epoch, const ObTenantSuperBlock &super_block);
  int update_tenant_unit(const int64_t epoch, const share::ObUnitInfoGetter::ObTenantConfig &unit);
  int clear_tenant_log_dir(const uint64_t tenant_id);
  
  
private:
  int write_prepare_create_tenant_slog_(const omt::ObTenantMeta &meta);
  int write_abort_create_tenant_slog_(uint64_t tenant_id);
  int write_commit_create_tenant_slog_(uint64_t tenant_id);
  int write_prepare_delete_tenant_slog_(uint64_t tenant_id);
  int write_commit_delete_tenant_slog_(uint64_t tenant_id);
  int write_update_tenant_super_block_slog_(const ObTenantSuperBlock &super_block);
  int write_update_tenant_unit_slog_(const share::ObUnitInfoGetter::ObTenantConfig &unit);

#ifdef OB_BUILD_SHARED_STORAGE
  int ss_prepare_create_tenant_(const omt::ObTenantMeta &meta, int64_t &epoch);
  int ss_commit_create_tenant_(const uint64_t tenant_id, const int64_t epoch);
  int ss_abort_create_tenant_(const uint64_t tenant_id, const int64_t epoch);
  int ss_prepare_delete_tenant_(const uint64_t tenant_id, const int64_t epoch);
  int ss_commit_delete_tenant_(const uint64_t tenant_id, const int64_t epoch);

  int ss_write_tenant_super_block_(const int64_t tenant_epoch, const ObTenantSuperBlock &tenant_super_block);
  int ss_write_unit_config_(
      const int64_t tenant_epoch,
      const share::ObUnitInfoGetter::ObTenantConfig &unit_config);

#endif 

private:
  bool is_inited_;
  bool is_shared_storage_;
  storage::ObStorageLogger *server_slogger_;
  common::ObConcurrentFIFOAllocator allocator_;
  
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTALE_OB_STORAGE_META_PERSISTER_H_