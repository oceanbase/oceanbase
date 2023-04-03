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
  common::ObIAllocator &allocator = MTL(ObTenantMetaMemMgr*)->get_tenant_allocator();
  return tablet_->deserialize(allocator, buf, data_len, pos);
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

}
}
