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

#include "storage/slog_ckpt/ob_server_checkpoint_reader.h"
#include "common/log/ob_log_cursor.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "observer/omt/ob_tenant_meta.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

int ObServerCheckpointReader::read_checkpoint(const ObServerSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("super block is invalid", K(ret), K(super_block));
  } else if (OB_FAIL(read_tenant_meta_checkpoint(super_block.body_.tenant_meta_entry_))) {
    LOG_WARN("fail to read tenant meta checkpoint", K(ret), K(super_block));
  }
  return ret;
}


int ObServerCheckpointReader::read_tenant_meta_checkpoint(const MacroBlockId &entry_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!entry_block.is_valid())) {
    LOG_INFO("has no tenant config checkpoint");
  } else if (OB_FAIL(tenant_meta_item_reader_.init(entry_block))) {
    LOG_WARN("fail to init tenant config item reader", K(ret));
  } else {
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    ObMetaDiskAddr addr;
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_meta_item_reader_.get_next_item(item_buf, item_buf_len, addr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next tenant meta item", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(deserialize_tenant_meta(item_buf, item_buf_len))) {
        LOG_WARN("failed to replay_tenant_meta_checkpoint", K(ret));
      }
    }
  }
  return ret;
}

int ObServerCheckpointReader::deserialize_tenant_meta(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  omt::ObTenantMeta tenant_meta;
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(tenant_meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize", K(ret));
  } else if (OB_FAIL(tenant_meta_list_.push_back(tenant_meta))) {
    LOG_WARN("fail to push back tenant meta", K(ret));
  }

  return ret;
}

ObIArray<MacroBlockId> &ObServerCheckpointReader::get_meta_block_list()
{
  return tenant_meta_item_reader_.get_meta_block_list();
}

int ObServerCheckpointReader::get_tenant_metas(hash::ObHashMap<uint64_t, omt::ObTenantMeta> &tenant_meta_map)
{
  int ret = OB_SUCCESS;
  tenant_meta_map.clear();
  for (int i = 0; OB_SUCC(ret) && i < tenant_meta_list_.count(); i++) {
    if (OB_FAIL(tenant_meta_map.set_refactored(
      tenant_meta_list_.at(i).super_block_.tenant_id_, tenant_meta_list_.at(i), 1))) {
      LOG_WARN("fail to get tenant meta", K(ret));
    }
  }

  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase