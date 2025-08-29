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


#include "ob_tenant_ls_balance_group_info.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{

int ObTenantLSBalanceGroupInfo::init(
    const uint64_t tenant_id,
    const int64_t balanced_ls_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || balanced_ls_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balanced_ls_num));
  } else if (OB_FAIL(ls_bg_map_.create(MAP_BUCKET_NUM, "TntLSBGMap"))) {
    LOG_WARN("create map for tenant balance group info fail", KR(ret), LITERAL_K(MAP_BUCKET_NUM));
  } else {
    tenant_id_ = tenant_id;
    balanced_ls_num_ = balanced_ls_num;
    inited_ = true;
  }
  return ret;
}

void ObTenantLSBalanceGroupInfo::destroy()
{
  FOREACH(iter, ls_bg_map_) {
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
  balanced_ls_num_ = 0;
}

int ObTenantLSBalanceGroupInfo::build(const char *mod,
  common::ObMySQLProxy &sql_proxy,
  share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  ObAllBalanceGroupBuilder bg_builder;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSBalanceGroupInfo not init", KR(ret), K(inited_));
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
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const common::ObObjectID part_object_id,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const int64_t tablet_size,
    const uint64_t part_group_uid,
    const int64_t balance_weight)
{
  UNUSEDx(dest_ls_id);
  int ret = OB_SUCCESS;
  ObLSBalanceGroupInfo *ls_bg_info = NULL;
  ObTransferPartInfo part_info(table_schema.get_table_id(), part_object_id);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSBalanceGroupInfo not inited", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!bg.is_valid()
      || !table_schema.is_valid()
      || !is_valid_id(part_object_id)
      || !is_valid_id(part_group_uid)
      || !src_ls_id.is_valid_with_tenant(tenant_id_)
      || tablet_size < 0
      || balance_weight < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K(part_object_id),
        K(src_ls_id), K(tablet_size), K(part_group_uid), K(balance_weight));
  } else if (OB_FAIL(get_or_create(src_ls_id, ls_bg_info))) {
    LOG_WARN("get or create ls balance group info fail", KR(ret), K(src_ls_id));
  } else if (OB_ISNULL(ls_bg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls balance group info", KR(ret), K(ls_bg_info), K(src_ls_id));
  } else if (OB_FAIL(ls_bg_info->append_part_into_balance_group(
      bg.id(),
      table_schema,
      part_info,
      tablet_size,
      part_group_uid,
      balance_weight))) {
    LOG_WARN("append part into balance group for LS balance group info fail", KR(ret), K(bg),
        K(part_info), K(tablet_size), K(part_group_uid), K(balance_weight), K(table_schema));
  }
  return ret;
}

int ObTenantLSBalanceGroupInfo::get_or_create(const ObLSID ls_id, ObLSBalanceGroupInfo *&ls_bg_info)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ls_bg_info = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSBalanceGroupInfo not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is invalid", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_bg_map_.get_refactored(ls_id, ls_bg_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = alloc_.alloc(sizeof(ObLSBalanceGroupInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for ObLSBalanceGroupInfo failed", KR(ret), K(buf));
      } else if (OB_ISNULL(ls_bg_info = new(buf) ObLSBalanceGroupInfo(alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("construct ObLSBalanceGroupInfo failed", KR(ret));
      } else if (OB_FAIL(ls_bg_info->init(ls_id, balanced_ls_num_))) {
        LOG_WARN("init ls balance group info failed", KR(ret), K(ls_id), K_(balanced_ls_num), KPC(ls_bg_info));
      } else if (OB_FAIL(ls_bg_map_.set_refactored(ls_id, ls_bg_info))) {
        LOG_WARN("set ls balance group info to map failed", KR(ret), K(ls_id));
      }
    } else {
      LOG_WARN("get ls balance group info failed", KR(ret), K(ls_id));
    }
  }
  return ret;
}

}
}
