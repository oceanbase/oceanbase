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
#define USING_LOG_PREFIX BALANCE

#include "share/transfer/ob_transfer_info.h"      // ObTransferPartInfo

#include "ob_tenant_ls_balance_group_info.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{

int ObTenantLSBalanceGroupInfo::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_bg_map_.create(MAP_BUCKET_NUM, "TntLSBGMap"))) {
    LOG_WARN("create map for tenant balance group info fail", KR(ret), LITERAL_K(MAP_BUCKET_NUM));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

void ObTenantLSBalanceGroupInfo::destroy()
{
  for (auto iter = ls_bg_map_.begin(); iter != ls_bg_map_.end(); iter++) {
    ObLSBalanceGroupInfo *ls_bg_info = iter->second;
    if (OB_NOT_NULL(ls_bg_info)) {
      ls_bg_info->~ObLSBalanceGroupInfo();
      alloc_.free(ls_bg_info);
      ls_bg_info = NULL;
    }
  }

  inited_ = false;
  ls_bg_map_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObTenantLSBalanceGroupInfo::build(const char *mod,
  common::ObMySQLProxy &sql_proxy,
  share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  ObAllBalanceGroupBuilder bg_builder;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSBalanceGroupInfo not init", KR(ret), KR(inited_));
  } else if (OB_FAIL(bg_builder.init(tenant_id_, mod, *this, sql_proxy, schema_service))) {
    LOG_WARN("balance group builder init fail", KR(ret), K(tenant_id_), K(mod));
  } else if (OB_FAIL(bg_builder.prepare())) {
    LOG_WARN("prepare for balance group builder fail", KR(ret));
  } else if (OB_FAIL(bg_builder.build())) {
    LOG_WARN("build balance group fail", KR(ret));
  } else {
    // succ
  }
  return ret;
}

int ObTenantLSBalanceGroupInfo::on_new_partition(
    const ObBalanceGroup &bg,
    const common::ObObjectID table_id,
    const common::ObObjectID part_object_id,
    const common::ObTabletID tablet_id,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const int64_t tablet_size,
    const bool in_new_partition_group,
    const uint64_t part_group_uid)
{
  UNUSEDx(tablet_id, dest_ls_id, in_new_partition_group);
  int ret = OB_SUCCESS;
  ObLSBalanceGroupInfo *ls_bg_info = NULL;
  ObTransferPartInfo part_info(table_id, part_object_id);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSBalanceGroupInfo not inited", KR(ret), K(inited_));
  } else if (OB_FAIL(ls_bg_map_.get_refactored(src_ls_id, ls_bg_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get ls balance group info from map fail", KR(ret), K(src_ls_id));
    } else if (OB_FAIL(create_new_ls_bg_info_(src_ls_id, ls_bg_info))) {
      LOG_WARN("create new ls balance group info fail", KR(ret), K(src_ls_id));
    } else if (OB_FAIL(ls_bg_map_.set_refactored(src_ls_id, ls_bg_info))) {
      LOG_WARN("set new ls balance group info fail", KR(ret), K(src_ls_id), K(ls_bg_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_bg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls balance group info", KR(ret), K(ls_bg_info), K(src_ls_id));
  } else if (OB_FAIL(ls_bg_info->append_part_into_balance_group(bg.id(), part_info, tablet_size, part_group_uid))) {
    LOG_WARN("append part into balance group for LS balance group info fail", KR(ret), K(bg),
        K(part_info), K(tablet_size), K(part_group_uid));
  }
  return ret;
}

int ObTenantLSBalanceGroupInfo::create_new_ls_bg_info_(const ObLSID ls_id,
    ObLSBalanceGroupInfo *&ls_bg_info)
{
  int ret = OB_SUCCESS;
  void *buf = alloc_.alloc(sizeof(ObLSBalanceGroupInfo));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for ObLSBalanceGroupInfo fail", KR(ret), K(buf));
  } else if (OB_ISNULL(ls_bg_info = new(buf) ObLSBalanceGroupInfo())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("construct ObLSBalanceGroupInfo fail", KR(ret));
  } else if (OB_FAIL(ls_bg_info->init(ls_id))) {
    LOG_WARN("init ls balance group info fail", KR(ret), K(ls_id), KPC(ls_bg_info));
  }
  return ret;
}

}
}
