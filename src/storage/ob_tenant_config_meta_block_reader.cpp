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

#include "ob_tenant_config_meta_block_reader.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "ob_tenant_config_mgr.h"

using namespace oceanbase::blocksstable;
namespace oceanbase {
namespace storage {

ObTenantConfigMetaBlockReader::ObTenantConfigMetaBlockReader() : tenant_units_()
{}

ObTenantConfigMetaBlockReader::~ObTenantConfigMetaBlockReader()
{}

int ObTenantConfigMetaBlockReader::parse(const blocksstable::ObMacroBlockCommonHeader& common_header,
    const blocksstable::ObLinkedMacroBlockHeader& linked_header, const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_UNLIKELY(ObMacroBlockCommonHeader::TenantConfigMeta != common_header.get_attr())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The header attr is not tenant config meta, ", K(ret), K(common_header.get_attr()));
  } else if (OB_UNLIKELY(ObMacroBlockCommonHeader::TenantConfigMeta != linked_header.attr_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The header attr is not tenant config meta, ", K(ret), K(linked_header.attr_));
  } else if (OB_FAIL(tenant_units_.deserialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize units", K(ret));
  } else if (OB_FAIL(ObTenantConfigMgr::get_instance().load_tenant_units(tenant_units_))) {
    STORAGE_LOG(ERROR, "failed to update tenants from local storage", K(ret), K(tenant_units_));
  } else {
    STORAGE_LOG(INFO, "succ to load tenant units", K(tenant_units_));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
