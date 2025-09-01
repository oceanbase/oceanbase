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

ObDeleteTabletLog::ObDeleteTabletLog()
  : ls_id_(),
    tablet_id_(),
    ls_epoch_(0),
    tablet_meta_version_(0),
    status_(ObPendingFreeTabletStatus::MAX),
    free_time_(0),
    gc_type_(GCTabletType::DropTablet),
    tablet_transfer_seq_(share::OB_INVALID_TRANSFER_SEQ),
    last_gc_version_(-1)
{
}

ObDeleteTabletLog::ObDeleteTabletLog(const ObLSID &ls_id, const ObTabletID &tablet_id)
  : ls_id_(ls_id),
    tablet_id_(tablet_id),
    ls_epoch_(0),
    tablet_meta_version_(0),
    status_(ObPendingFreeTabletStatus::MAX),
    free_time_(0),
    gc_type_(GCTabletType::DropTablet),
    tablet_transfer_seq_(share::OB_INVALID_TRANSFER_SEQ),
    last_gc_version_(-1)
{
}

ObDeleteTabletLog::ObDeleteTabletLog(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t ls_epoch,
    const int64_t tablet_meta_version,
    const ObPendingFreeTabletStatus &status,
    const int64_t free_time,
    const GCTabletType &gc_type,
    const int64_t tablet_transfer_seq,
    const int64_t last_gc_version)
  : ls_id_(ls_id),
    tablet_id_(tablet_id),
    ls_epoch_(ls_epoch),
    tablet_meta_version_(tablet_meta_version),
    status_(status),
    free_time_(free_time),
    gc_type_(gc_type),
    tablet_transfer_seq_(tablet_transfer_seq),
    last_gc_version_(last_gc_version)
{
}

bool ObDeleteTabletLog::is_valid() const
{
  bool is_valid = ls_id_.is_valid() && tablet_id_.is_valid();
  if (GCTX.is_shared_storage_mode()) {
    is_valid = is_valid
            && ls_epoch_ >= 0
            && tablet_meta_version_ >= 0
            && ObPendingFreeTabletStatus::MAX != status_
            && tablet_transfer_seq_ != share::OB_INVALID_TRANSFER_SEQ
            && last_gc_version_ >= -1
            && last_gc_version_ < tablet_meta_version_;
  }
  return is_valid;
}

bool ObDeleteTabletLog::operator ==(const ObDeleteTabletLog &other) const
{
  return ls_id_               == other.ls_id_
      && tablet_id_           == other.tablet_id_
      && ls_epoch_            == other.ls_epoch_
      && tablet_meta_version_ == other.tablet_meta_version_
      && status_              == other.status_
      && free_time_           == other.free_time_
      && gc_type_             == other.gc_type_
      && tablet_transfer_seq_ == other.tablet_transfer_seq_
      && last_gc_version_     == other.last_gc_version_;
}

bool ObDeleteTabletLog::operator !=(const ObDeleteTabletLog &other) const
{
  return !(*this == other);
}

uint64_t ObDeleteTabletLog::hash() const
{
  uint64_t hash_val = ls_id_.hash() + tablet_id_.hash();
  hash_val = common::murmurhash(&ls_epoch_, sizeof(ls_epoch_), hash_val);
  hash_val = common::murmurhash(&tablet_meta_version_, sizeof(tablet_meta_version_), hash_val);
  hash_val = common::murmurhash(&tablet_transfer_seq_, sizeof(tablet_transfer_seq_), hash_val);
  hash_val = common::murmurhash(&last_gc_version_, sizeof(last_gc_version_), hash_val);
  return hash_val;
}

int ObDeleteTabletLog::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

OB_SERIALIZE_MEMBER(ObDeleteTabletLog,
                    ls_id_,
                    tablet_id_,
                    ls_epoch_,
                    tablet_meta_version_,
                    status_,
                    free_time_,
                    gc_type_,
                    tablet_transfer_seq_,
                    last_gc_version_);

DEF_TO_STRING(ObDeleteTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id), K_(tablet_id), K_(ls_epoch), K_(tablet_meta_version), K_(status), K_(free_time), K_(gc_type),
       K_(tablet_transfer_seq), K_(last_gc_version));
  J_OBJ_END();
  return pos;
}

ObGCTabletLog::ObGCTabletLog()
  : ls_id_(),
    tablet_id_(),
    ls_epoch_(0),
    tablet_meta_version_(0),
    status_(ObPendingFreeTabletStatus::MAX),
    tablet_transfer_seq_(0)
{}

ObGCTabletLog::ObGCTabletLog(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObTabletID &tablet_id,
    const int64_t tablet_meta_version,
    const ObPendingFreeTabletStatus &status,
    const int64_t tablet_transfer_seq)
  : ls_id_(ls_id),
    tablet_id_(tablet_id),
    ls_epoch_(ls_epoch),
    tablet_meta_version_(tablet_meta_version),
    status_(status),
    tablet_transfer_seq_(tablet_transfer_seq)
{
}

bool ObGCTabletLog::is_valid() const
{
  return ls_id_.is_valid()
      && ls_epoch_ >= 0
      && tablet_id_.is_valid()
      && tablet_meta_version_ >= 0
      && ObPendingFreeTabletStatus::MAX != status_
      && tablet_transfer_seq_ != share::OB_INVALID_TRANSFER_SEQ;
}

bool ObGCTabletLog::operator ==(const ObGCTabletLog &other) const
{
  return ls_id_               == other.ls_id_
      && tablet_id_           == other.tablet_id_
      && ls_epoch_            == other.ls_epoch_
      && tablet_meta_version_ == other.tablet_meta_version_
      && status_              == other.status_
      && tablet_transfer_seq_ == other.tablet_transfer_seq_;
}

bool ObGCTabletLog::operator !=(const ObGCTabletLog &other) const
{
  return !(*this == other);
}

uint64_t ObGCTabletLog::hash() const
{
  uint64_t hash_val = ls_id_.hash() + tablet_id_.hash();
  hash_val = common::murmurhash(&ls_epoch_, sizeof(ls_epoch_), hash_val);
  hash_val = common::murmurhash(&tablet_meta_version_, sizeof(tablet_meta_version_), hash_val);
  hash_val = common::murmurhash(&tablet_transfer_seq_, sizeof(tablet_transfer_seq_), hash_val);
  return hash_val;
}

int ObGCTabletLog::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

OB_SERIALIZE_MEMBER(ObGCTabletLog, ls_id_, ls_epoch_, tablet_id_, tablet_meta_version_, status_, tablet_transfer_seq_);

DEF_TO_STRING(ObGCTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id), K_(tablet_id), K_(ls_epoch), K_(tablet_meta_version), K_(status), K_(tablet_transfer_seq));
  J_OBJ_END();
  return pos;
}

ObUpdateTabletLog::ObUpdateTabletLog()
  : ls_id_(),
    tablet_id_(),
    disk_addr_(),
    ls_epoch_(0),
    tablet_attr_(),
    accelerate_info_()
{}

ObUpdateTabletLog::ObUpdateTabletLog(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObUpdateTabletPointerParam &param,
    const int64_t ls_epoch)
  : ls_id_(ls_id),
    tablet_id_(tablet_id),
    disk_addr_(param.resident_info_.addr_),
    ls_epoch_(ls_epoch),
    tablet_attr_(param.resident_info_.attr_),
    accelerate_info_(param.accelerate_info_)
{
}

bool ObUpdateTabletLog::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && disk_addr_.is_valid() && ls_epoch_ >= 0;
}

OB_SERIALIZE_MEMBER(ObUpdateTabletLog,
                    ls_id_,
                    tablet_id_,
                    disk_addr_,
                    ls_epoch_,
                    tablet_attr_,
                    accelerate_info_);

DEF_TO_STRING(ObUpdateTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id),
       K_(tablet_id),
       K_(disk_addr),
       K_(ls_epoch),
       K_(tablet_attr),
       K_(accelerate_info));
  J_OBJ_END();
  return pos;
}

ObEmptyShellTabletLog::ObEmptyShellTabletLog()
  : version_(EMPTY_SHELL_SLOG_VERSION),
    ls_id_(),
    tablet_id_(),
    tablet_(nullptr),
    ls_epoch_(0),
    data_version_(DATA_CURRENT_VERSION)
{}

ObEmptyShellTabletLog::ObEmptyShellTabletLog(
    const uint64_t data_version,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t ls_epoch,
    ObTablet *tablet)
  : version_(EMPTY_SHELL_SLOG_VERSION),
    ls_id_(ls_id),
    tablet_id_(tablet_id),
    tablet_(tablet),
    ls_epoch_(ls_epoch),
    data_version_(data_version)
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
  } else if (OB_FAIL(serialization::encode(buf, data_len, pos, ls_epoch_))) {
    STORAGE_LOG(WARN, "deserialize ls epoch failed", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(tablet_->serialize(data_version_, buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize tablet failed", K(ret), K(data_version_),  KP(data_len), K(pos));
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
  } else if (EMPTY_SHELL_SLOG_VERSION_2 == version_ && OB_FAIL(serialization::decode(buf, data_len, pos, ls_epoch_))) {
    STORAGE_LOG(WARN, "deserialize ls epoch failed", K(ret), K(data_len), K(pos));
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
  } else if (EMPTY_SHELL_SLOG_VERSION_1 == version_) {
    if (OB_FAIL(tablet_->deserialize(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "deserialize tablet failed", K(ret), KP(data_len), K(pos));
    }
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, ls_epoch_))) {
    STORAGE_LOG(WARN, "deserialize ls epoch failed", K(ret), K(data_len), K(pos));
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
  size += serialization::encoded_length(ls_epoch_);
  size += tablet_->get_serialize_size(data_version_);
  return size;
}

bool ObEmptyShellTabletLog::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && ls_epoch_ >= 0;
}

DEF_TO_STRING(ObEmptyShellTabletLog)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_id), K_(tablet_id), K_(ls_epoch));
  J_OBJ_END();
  return pos;
}

}
}
