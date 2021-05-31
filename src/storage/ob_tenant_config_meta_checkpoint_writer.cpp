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

#include "ob_tenant_config_meta_checkpoint_writer.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantConfigMetaItem::ObTenantConfigMetaItem() : tenant_units_(nullptr), allocator_(ObModIds::OB_CHECKPOINT)
{}

int ObTenantConfigMetaItem::serialize(const char*& buf, int64_t& buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_units_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, tenant units must not be null", K(ret));
  } else {
    const int64_t serialize_size = tenant_units_->get_serialize_size();
    char* tmp_buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(tmp_buf = static_cast<char*>(allocator_.alloc(serialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(tenant_units_->serialize(tmp_buf, serialize_size, pos))) {
      LOG_WARN("fail to serialize tenant units", K(ret));
    } else {
      buf = tmp_buf;
      buf_len = pos;
    }
  }
  return ret;
}

void ObTenantConfigMetaItem::set_tenant_units(share::TenantUnits& tenant_units)
{
  tenant_units_ = &tenant_units;
}

ObTenantConfigMetaCheckpointWriter::ObTenantConfigMetaCheckpointWriter() : writer_()
{}

int ObTenantConfigMetaCheckpointWriter::write_checkpoint(blocksstable::ObSuperBlockMetaEntry& meta_entry)
{
  int ret = OB_SUCCESS;
  ObStorageFileHandle& server_root_handle = OB_FILE_SYSTEM.get_server_root_handle();
  reset();
  if (OB_UNLIKELY(!server_root_handle.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, server root must not be null", K(ret));
  } else if (OB_FAIL(writer_.init(server_root_handle))) {
    LOG_WARN("fail to init pg meta item writer", K(ret));
  } else {
    share::TenantUnits tenant_units;
    if (OB_FAIL(ObTenantConfigMgr::get_instance().get_tenant_units(tenant_units))) {
      LOG_WARN("fail to load tenant units", K(ret));
    } else {
      ObTenantConfigMetaItem item;
      item.set_tenant_units(tenant_units);
      if (OB_FAIL(writer_.write_item(&item))) {
        LOG_WARN("fail to write item", K(ret));
      } else if (OB_FAIL(writer_.close())) {
        LOG_WARN("fail to close writer", K(ret));
      } else if (OB_FAIL(writer_.get_entry_block_index(meta_entry.macro_block_id_))) {
        LOG_WARN("fail to get entry block index", K(ret));
      }
    }
  }
  return ret;
}

void ObTenantConfigMetaCheckpointWriter::reset()
{
  writer_.reset();
}

ObIArray<MacroBlockId>& ObTenantConfigMetaCheckpointWriter::get_meta_block_list()
{
  return writer_.get_meta_block_list();
}
