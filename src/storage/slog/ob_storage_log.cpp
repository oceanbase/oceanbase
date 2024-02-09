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

#include "ob_storage_log.h"
#include "storage/ls/ob_ls_meta.h"
#include "share/ob_ls_id.h"
#include "storage/tablet/ob_tablet.h"
#include "observer/omt/ob_tenant_meta.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObCreateTenantPrepareLog::ObCreateTenantPrepareLog(omt::ObTenantMeta &meta)
  : meta_(meta)
{
}

bool ObCreateTenantPrepareLog::is_valid() const
{
  return meta_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCreateTenantPrepareLog, meta_);

ObCreateTenantCommitLog::ObCreateTenantCommitLog(uint64_t &tenant_id)
  : tenant_id_(tenant_id)
{
}

bool ObCreateTenantCommitLog::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER(ObCreateTenantCommitLog, tenant_id_);
ObCreateTenantAbortLog::ObCreateTenantAbortLog(uint64_t &tenant_id)
  : tenant_id_(tenant_id)
{
}

bool ObCreateTenantAbortLog::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER(ObCreateTenantAbortLog, tenant_id_);

ObDeleteTenantPrepareLog::ObDeleteTenantPrepareLog(uint64_t &tenant_id)
  : tenant_id_(tenant_id)
{
}

bool ObDeleteTenantPrepareLog::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER(ObDeleteTenantPrepareLog, tenant_id_);

ObDeleteTenantCommitLog::ObDeleteTenantCommitLog(uint64_t &tenant_id)
  : tenant_id_(tenant_id)
{
}

bool ObDeleteTenantCommitLog::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER(ObDeleteTenantCommitLog, tenant_id_);

ObUpdateTenantUnitLog::ObUpdateTenantUnitLog(share::ObUnitInfoGetter::ObTenantConfig &unit)
  : unit_(unit)
{
}
bool ObUpdateTenantUnitLog::is_valid() const
{
  return unit_.is_valid();
}

OB_SERIALIZE_MEMBER(ObUpdateTenantUnitLog, unit_);

ObUpdateTenantSuperBlockLog::ObUpdateTenantSuperBlockLog(ObTenantSuperBlock &super_block)
  : super_block_(super_block)
{
}
bool ObUpdateTenantSuperBlockLog::is_valid() const
{
  return super_block_.is_valid();
}

OB_SERIALIZE_MEMBER(ObUpdateTenantSuperBlockLog, super_block_);

ObLSMetaLog::ObLSMetaLog(const ObLSMeta &ls_meta)
  : ls_meta_(ls_meta)
{
}

bool ObLSMetaLog::is_valid() const
{
  return ls_meta_.is_valid();
}


DEF_TO_STRING(ObLSMetaLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_meta));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObLSMetaLog, ls_meta_);

OB_SERIALIZE_MEMBER(ObDupTableCkptLog, dup_ls_meta_);

ObLSIDLog::ObLSIDLog(ObLSID &ls_id)
  : ls_id_(ls_id)
{
}

bool ObLSIDLog::is_valid() const
{
  return ls_id_.is_valid();
}

DEF_TO_STRING(ObLSIDLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObLSIDLog, ls_id_);

ObCreateTabletLog::ObCreateTabletLog(ObTablet *tablet)
  : tablet_(tablet)
{
}

int ObCreateTabletLog::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  return tablet_->serialize(buf, buf_len, pos);
}

int ObCreateTabletLog::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  // abandoned slog, skip deserialization
  return OB_SUCCESS;
}

int64_t ObCreateTabletLog::get_serialize_size() const
{
  return tablet_->get_serialize_size();
}

bool ObCreateTabletLog::is_valid() const
{
  return true;
}

DEF_TO_STRING(ObCreateTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KPC_(tablet));
  J_OBJ_END();
  return pos;
}

ObDeleteTabletLog::ObDeleteTabletLog()
  : ls_id_(), tablet_id_()
{
}

ObDeleteTabletLog::ObDeleteTabletLog(const ObLSID &ls_id, const ObTabletID &tablet_id)
  : ls_id_(ls_id), tablet_id_(tablet_id)
{
}

bool ObDeleteTabletLog::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid();
}

OB_SERIALIZE_MEMBER(ObDeleteTabletLog, ls_id_, tablet_id_);

DEF_TO_STRING(ObDeleteTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id), K_(tablet_id));
  J_OBJ_END();
  return pos;
}

ObUpdateTabletLog::ObUpdateTabletLog(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObMetaDiskAddr &disk_addr)
  : ls_id_(ls_id),
    tablet_id_(tablet_id),
    disk_addr_(disk_addr)
{
}

bool ObUpdateTabletLog::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && disk_addr_.is_valid();
}

OB_SERIALIZE_MEMBER(ObUpdateTabletLog, ls_id_, tablet_id_, disk_addr_);

DEF_TO_STRING(ObUpdateTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id), K_(tablet_id), K_(disk_addr));
  J_OBJ_END();
  return pos;
}

ObEmptyShellTabletLog::ObEmptyShellTabletLog(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObTablet *tablet)
  : version_(EMPTY_SHELL_SLOG_VERSION),
    ls_id_(ls_id),
    tablet_id_(tablet_id),
    tablet_(tablet)
{
}

int ObEmptyShellTabletLog::serialize(
    char *buf,
    const int64_t data_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode(buf, data_len, pos, version_))) {
    STORAGE_LOG(WARN, "deserialize version_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(ls_id_.serialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize ls_id_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(tablet_id_.serialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize tablet_id_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(tablet_->serialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize tablet failed", K(ret), KP(data_len), K(pos));
  }

  return ret;
}

int ObEmptyShellTabletLog::deserialize_id(
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, version_))) {
    STORAGE_LOG(WARN, "deserialize version_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(ls_id_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize ls_id_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(tablet_id_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize tablet_id_ failed", K(ret), KP(data_len), K(pos));
  }
  return ret;
}

// shouldn't be called, since we can't set tablet addr here, but tablet addr should be set before deserialization
int ObEmptyShellTabletLog::deserialize(
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, version_))) {
    STORAGE_LOG(WARN, "deserialize version_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(ls_id_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize ls_id_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(tablet_id_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize tablet_id_ failed", K(ret), KP(data_len), K(pos));
  } else if (OB_FAIL(tablet_->deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize tablet failed", K(ret), KP(data_len), K(pos));
  }

  return ret;
}

int64_t ObEmptyShellTabletLog::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length(version_);
  size += ls_id_.get_serialize_size();
  size += tablet_id_.get_serialize_size();
  size += tablet_->get_serialize_size();
  return size;
}

bool ObEmptyShellTabletLog::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid();
}

DEF_TO_STRING(ObEmptyShellTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id), K_(tablet_id));
  J_OBJ_END();
  return pos;
}

}
}
