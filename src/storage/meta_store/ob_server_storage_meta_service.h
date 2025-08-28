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
#ifndef OCEANBASE_STORAGE_META_STORE_SERVER_STORAGE_META_SERVICE_
#define OCEANBASE_STORAGE_META_STORE_SERVER_STORAGE_META_SERVICE_

#include <stdint.h>
#include "storage/meta_store/ob_server_storage_meta_persister.h"
#include "storage/meta_store/ob_server_storage_meta_replayer.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/slog/ob_storage_logger_manager.h"

namespace oceanbase
{
namespace storage
{
class ObServerStorageMetaService
{
public:
  static ObServerStorageMetaService &get_instance();
  int init(const bool is_share_storage);
  int start();
  void stop();
  void wait();
  void destroy();
  ObServerStorageMetaPersister &get_persister() { return persister_; }
  bool is_started() const { return ATOMIC_LOAD(&is_started_); }
  
  int get_meta_block_list(ObIArray<blocksstable::MacroBlockId> &meta_block_list);
  ObStorageLoggerManager &get_slogger_manager() { return slogger_mgr_; }
  int get_reserved_size(int64_t &reserved_size) const;
  int get_server_slogger(ObStorageLogger *&slogger) const;
  int write_checkpoint(bool is_force);

  class ObTenantItemIterator final
  {
  public:
    explicit ObTenantItemIterator():
      idx_(0),
      is_inited_(false),
      server_super_block_()
      {}
    ~ObTenantItemIterator() = default;
    int init();
    int get_next_tenant_item(storage::ObTenantItem &item);
    TO_STRING_KV(K_(idx), K_(is_inited), K_(server_super_block));
  private:
    int64_t idx_;
    bool is_inited_;
    storage::ObServerSuperBlock server_super_block_;
    DISALLOW_COPY_AND_ASSIGN(ObTenantItemIterator);
  };

  int get_tenant_items_by_status(
      const storage::ObTenantCreateStatus status,
      ObIArray<storage::ObTenantItem> &tenant_items);

private:
  ObServerStorageMetaService();
  ~ObServerStorageMetaService() = default;
  ObServerStorageMetaService(const ObServerStorageMetaService &) = delete;
  ObServerStorageMetaService &operator=(const ObServerStorageMetaService &) = delete;

private:
  bool is_inited_;
  bool is_started_;
  bool is_shared_storage_;
  ObServerStorageMetaPersister persister_;
  ObServerStorageMetaReplayer replayer_;
  ObStorageLoggerManager slogger_mgr_;
  ObStorageLogger *server_slogger_;
  ObServerCheckpointSlogHandler ckpt_slog_handler_;
};

#define SERVER_STORAGE_META_SERVICE (oceanbase::storage::ObServerStorageMetaService::get_instance())
#define SERVER_STORAGE_META_PERSISTER (oceanbase::storage::ObServerStorageMetaService::get_instance().get_persister())



} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_META_STORE_SERVER_STORAGE_META_SERVICE_
