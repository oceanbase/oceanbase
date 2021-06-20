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

#ifndef SRC_STORAGE_OB_TENANT_CONFIG_MGR_H_
#define SRC_STORAGE_OB_TENANT_CONFIG_MGR_H_

#include "lib/lock/ob_mutex.h"
#include "share/ob_unit_getter.h"
#include "blocksstable/slog/ob_base_storage_logger.h"
#include "blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace storage {
class ObTenantConfigMgr : public blocksstable::ObIRedoModule {
public:
  static ObTenantConfigMgr& get_instance();

  int init();
  virtual int enable_write_log() override;
  void destroy();
  void stop();

  int write_tenant_units(const share::TenantUnits& tenant_units);
  int load_tenant_units(const share::TenantUnits& tenant_units);
  int get_tenant_units(share::TenantUnits& tenant_units);
  int get_compat_mode(const int64_t tenant_id, share::ObWorker::CompatMode& compat_mode);
  virtual int replay(const blocksstable::ObRedoModuleReplayParam& param) override;
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream) override;

private:
  ObTenantConfigMgr();
  virtual ~ObTenantConfigMgr();
  int replay_update_tenant_config(const char* buf, const int64_t buf_len);
  bool is_tenant_changed(const share::TenantUnits& old_tenant, const share::TenantUnits& new_tenant);

private:
  bool is_inited_;
  share::TenantUnits tenant_units_;
  mutable lib::ObMutex mutex_;
  mutable common::TCRWLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantConfigMgr);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_TENANT_CONFIG_MGR_H_ */
