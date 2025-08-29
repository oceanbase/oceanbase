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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_H_

#include "storage/slog/ob_storage_log_struct.h"
#include <inttypes.h>
#include "storage/ob_super_block_struct.h"
#include "observer/omt/ob_tenant_meta.h"
#include "share/ob_unit_getter.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_store/ob_startup_accelerate_info.h"

namespace oceanbase
{

namespace share
{
  class ObLSID;
}
namespace storage
{
class ObTablet;
struct ObUpdateTabletPointerParam;
struct ObCreateTenantPrepareLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObCreateTenantPrepareLog(omt::ObTenantMeta &meta);
  virtual ~ObCreateTenantPrepareLog() {}
  virtual bool is_valid() const override;
  TO_STRING_KV(K_(meta));
  OB_UNIS_VERSION_V(1);

private:
  omt::ObTenantMeta &meta_;
};

struct ObCreateTenantCommitLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObCreateTenantCommitLog(uint64_t &tenant_id);
  virtual ~ObCreateTenantCommitLog() {}
  virtual bool is_valid() const override;
  TO_STRING_KV(K_(tenant_id));
  OB_UNIS_VERSION_V(1);

private:
  uint64_t &tenant_id_;
};

struct ObCreateTenantAbortLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObCreateTenantAbortLog(uint64_t &tenant_id);
  virtual ~ObCreateTenantAbortLog() {}
  virtual bool is_valid() const override;
  TO_STRING_KV(K_(tenant_id));
  OB_UNIS_VERSION_V(1);

private:
  uint64_t &tenant_id_;
};

struct ObDeleteTenantPrepareLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObDeleteTenantPrepareLog(uint64_t &tenant_id);
  virtual ~ObDeleteTenantPrepareLog() {}
  virtual bool is_valid() const override;
  TO_STRING_KV(K_(tenant_id));
  OB_UNIS_VERSION_V(1);

private:
  uint64_t &tenant_id_;
};
struct ObDeleteTenantCommitLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObDeleteTenantCommitLog(uint64_t &tenant_id);
  virtual ~ObDeleteTenantCommitLog() {}
  virtual bool is_valid() const override;
  TO_STRING_KV(K_(tenant_id));
  OB_UNIS_VERSION_V(1);

private:
  uint64_t &tenant_id_;
};

struct ObUpdateTenantUnitLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObUpdateTenantUnitLog(share::ObUnitInfoGetter::ObTenantConfig &unit);
  virtual ~ObUpdateTenantUnitLog() {}
  virtual bool is_valid() const override;

  TO_STRING_KV(K_(unit));

  OB_UNIS_VERSION_V(1);

private:
  share::ObUnitInfoGetter::ObTenantConfig  &unit_;
};

struct ObUpdateTenantSuperBlockLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObUpdateTenantSuperBlockLog(ObTenantSuperBlock &super_block);
  virtual ~ObUpdateTenantSuperBlockLog() {}
  virtual bool is_valid() const override;

  TO_STRING_KV(K_(super_block));

  OB_UNIS_VERSION_V(1);

private:
  ObTenantSuperBlock &super_block_;
};

struct ObLSMetaLog : public ObIBaseStorageLogEntry
{
public:
  ObLSMetaLog() : ls_meta_() {}
  ObLSMetaLog(const ObLSMeta &ls_meta);
  const ObLSMeta &get_ls_meta() const { return ls_meta_; }
  virtual ~ObLSMetaLog() {}
  virtual bool is_valid() const override;

  DECLARE_TO_STRING;
  OB_UNIS_VERSION_V(1);

private:
  ObLSMeta ls_meta_;
};

struct ObDupTableCkptLog : public ObIBaseStorageLogEntry
{
public:
  ObDupTableCkptLog() {}
  int init(const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta)
  {
    return dup_ls_meta_.copy(dup_ls_meta);
  }

  const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &get_dup_ls_meta()
  {
    return dup_ls_meta_;
  }
  bool is_valid() const { return dup_ls_meta_.is_valid(); }

  TO_STRING_KV(K(dup_ls_meta_));
  OB_UNIS_VERSION(1);

private:
  transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta dup_ls_meta_;
};

struct ObLSIDLog : public ObIBaseStorageLogEntry
{
public:
  explicit ObLSIDLog(share::ObLSID &ls_id);
  virtual ~ObLSIDLog() {}
  virtual bool is_valid() const override;

  DECLARE_TO_STRING;
  OB_UNIS_VERSION_V(1);

protected:
  share::ObLSID &ls_id_;
};

using ObCreateLSPrepareSlog = ObLSMetaLog;
using ObCreateLSAbortSLog = ObLSIDLog;
using ObCreateLSCommitSLog = ObLSIDLog;
using ObDeleteLSLog = ObLSIDLog;

struct ObDeleteTabletLog : public ObIBaseStorageLogEntry
{
public:
  ObDeleteTabletLog();
  ObDeleteTabletLog(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);
  ObDeleteTabletLog(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t ls_epoch,
      const int64_t tablet_meta_version,
      const ObPendingFreeTabletStatus &status,
      const int64_t free_time,
      const GCTabletType &gc_type,
      const int64_t tablet_transfer_seq,
      const int64_t last_gc_version = -1);
  virtual ~ObDeleteTabletLog() {}
  virtual bool is_valid() const override;

  bool operator ==(const ObDeleteTabletLog &other) const;
  bool operator !=(const ObDeleteTabletLog &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;

  DECLARE_TO_STRING;
  OB_UNIS_VERSION_V(1);

public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t ls_epoch_;
  int64_t tablet_meta_version_;
  ObPendingFreeTabletStatus status_;
  int64_t free_time_;
  GCTabletType gc_type_;
  int64_t tablet_transfer_seq_;
  int64_t last_gc_version_;
};

class ObGCTabletLog : public ObIBaseStorageLogEntry
{
public:
  ObGCTabletLog();
  ObGCTabletLog(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const common::ObTabletID &tablet_id,
      const int64_t tablet_meta_version,
      const ObPendingFreeTabletStatus &status,
      const int64_t tablet_transfer_seq);
  virtual ~ObGCTabletLog() = default;

  virtual bool is_valid() const override;
  bool operator ==(const ObGCTabletLog &other) const;
  bool operator !=(const ObGCTabletLog &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;

  DECLARE_TO_STRING;
  OB_UNIS_VERSION_V(1);
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t ls_epoch_;
  int64_t tablet_meta_version_;
  ObPendingFreeTabletStatus status_;
  int64_t tablet_transfer_seq_;
};

struct ObUpdateTabletLog : public ObIBaseStorageLogEntry
{
public:
  ObUpdateTabletLog();
  ObUpdateTabletLog(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObUpdateTabletPointerParam &tablet_pointer_param,
      const int64_t ls_epoch);
  virtual ~ObUpdateTabletLog() = default;
  virtual bool is_valid() const override;
  DECLARE_TO_STRING;
  OB_UNIS_VERSION_V(1);
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObMetaDiskAddr disk_addr_;
  int64_t ls_epoch_;
  storage::ObTabletAttr tablet_attr_;
  storage::ObStartupTabletAccelerateInfo accelerate_info_;
};

struct ObEmptyShellTabletLog : public ObIBaseStorageLogEntry
{
public:
  const int64_t EMPTY_SHELL_SLOG_VERSION_1 = 1;
  const int64_t EMPTY_SHELL_SLOG_VERSION_2 = 2;
  const int64_t EMPTY_SHELL_SLOG_VERSION = EMPTY_SHELL_SLOG_VERSION_2;
public:
  ObEmptyShellTabletLog();
  ObEmptyShellTabletLog(
      const uint64_t data_version,
      const ObLSID &ls_id_,
      const ObTabletID &tablet_id,
      const int64_t ls_epoch,
      ObTablet *tablet);
  virtual ~ObEmptyShellTabletLog() {}
  virtual bool is_valid() const override;
  virtual int serialize(
      char* buf,
      const int64_t buf_len,
      int64_t& pos) const;
  virtual int deserialize(
      const char* buf,
      const int64_t data_len,
      int64_t& pos);
  int deserialize_id(
      const char* buf,
      const int64_t data_len,
      int64_t& pos);
  virtual int64_t get_serialize_size() const;

  DECLARE_TO_STRING;
public:
  int64_t version_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObTablet *tablet_;
  int64_t ls_epoch_;
  uint64_t data_version_;
};

}
}

#endif
