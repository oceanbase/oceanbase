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

#include "storage/slog_ckpt/ob_server_checkpoint_writer.h"
#include "common/log/ob_log_cursor.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "observer/omt/ob_tenant_meta.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

int ObServerCheckpointWriter::init()
{
  int ret = OB_SUCCESS;
  const int64_t MEM_LIMIT = 128 << 20;  // 128M
  const char *MEM_LABEL = "ObServerCheckpointWriter";
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, MEM_LABEL);

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerCheckpointWriter init twice", K(ret));
  } else if (OB_FAIL(allocator_.init(
               common::OB_MALLOC_NORMAL_BLOCK_SIZE, MEM_LABEL, OB_SERVER_TENANT_ID, MEM_LIMIT))) {
    LOG_WARN("fail to init fifo allocator", K(ret));
  } else if (OB_FAIL(tenant_meta_item_writer_.init(false /*whether need addr*/, mem_attr))) {
    LOG_WARN("fail to init tenant meta item writer", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObServerCheckpointWriter::write_checkpoint(const ObLogCursor &log_cursor)
{
  int ret = OB_SUCCESS;
  ObStorageLogger *server_slogger = nullptr;
  LOG_INFO("start to write server checkpoint", K(log_cursor));

  MacroBlockId tenant_meta_entry;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointWriter not init", K(ret));
  } else if (OB_UNLIKELY(!log_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(write_tenant_meta_checkpoint(tenant_meta_entry))) {
    LOG_WARN("fail to write tenant config checkpoint", K(ret));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.update_super_block(log_cursor, tenant_meta_entry))) {
    LOG_WARN("fail to update server super block", K(ret), K(log_cursor), K(tenant_meta_entry));
  } else if (OB_FAIL(SLOGGERMGR.get_server_slogger(server_slogger))) {
    LOG_WARN("fail to get server slogger", K(ret));
  } else if (OB_FAIL(server_slogger->remove_useless_log_file(log_cursor.file_id_, OB_SERVER_TENANT_ID))) {
    LOG_WARN("fail to remove_useless_log_file", K(ret));
  } else {
    LOG_INFO("succeed to write server checkpoint", K(log_cursor), K(tenant_meta_entry));
  }

  return ret;
}

int ObServerCheckpointWriter::write_tenant_meta_checkpoint(MacroBlockId &block_entry)
{
  int ret = OB_SUCCESS;

  common::ObArray<omt::ObTenantMeta> metas;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;

  omt::ObTenantMeta tenant_meta;
  if (OB_FAIL(GCTX.omt_->get_tenant_metas_for_ckpt(metas))) {
    LOG_WARN("fail to get_tenant_metas", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) {
    const omt::ObTenantMeta &meta = metas.at(i);
    buf_len = meta.get_serialize_size();
    pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_FAIL(meta.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize tenant meta", K(ret));
    } else if (OB_FAIL(tenant_meta_item_writer_.write_item(buf, buf_len, nullptr))) {
      LOG_WARN("fail to write tenant meta item", K(ret));
    }
    if (OB_LIKELY(nullptr != buf)) {
      allocator_.free(buf);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_meta_item_writer_.close())) {
      LOG_WARN("fail to close tenant_config_item_writer_", K(ret));
    } else if (OB_FAIL(tenant_meta_item_writer_.get_entry_block(block_entry))) {
      LOG_WARN("fail to get entry block", K(ret));
    }
  }

  return ret;
}

ObIArray<MacroBlockId> &ObServerCheckpointWriter::get_meta_block_list()
{
  return tenant_meta_item_writer_.get_meta_block_list();
}



}  // end namespace storage
}  // end namespace oceanbase
