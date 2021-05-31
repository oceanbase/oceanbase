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

#define USING_LOG_PREFIX STORAGE

#include "ob_tenant_config_meta_checkpoint_reader.h"
#include "share/ob_unit_getter.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantConfigMetaCheckpointReader::ObTenantConfigMetaCheckpointReader() : is_inited_(false), reader_()
{}

int ObTenantConfigMetaCheckpointReader::init(
    const blocksstable::MacroBlockId& entry_block, blocksstable::ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantConfigMetaCheckpointReader has already been inited", K(ret));
  } else if (OB_UNLIKELY(!entry_block.is_valid() || !file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(entry_block), K(file_handle));
  } else if (OB_FAIL(reader_.init(entry_block, file_handle))) {
    LOG_WARN("fail to init reader", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTenantConfigMetaCheckpointReader::read_checkpoint()
{
  int ret = OB_SUCCESS;
  ObPGMetaItemBuffer item;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantConfigMetaCheckpointReader has not been inited", K(ret));
  } else if (OB_FAIL(reader_.get_next_item(item))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next item", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    share::TenantUnits tenant_units;
    int64_t pos = 0;
    if (OB_FAIL(tenant_units.deserialize(item.buf_, item.buf_len_, pos))) {
      LOG_WARN("fail to deserialize tenant units", K(ret));
    } else if (OB_FAIL(ObTenantConfigMgr::get_instance().load_tenant_units(tenant_units))) {
      LOG_WARN("fail to load tenant units", K(ret));
    }
  }
  return ret;
}

ObIArray<MacroBlockId>& ObTenantConfigMetaCheckpointReader::get_meta_block_list()
{
  return reader_.get_meta_block_list();
}
