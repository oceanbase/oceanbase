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

#include "ob_tenant_file_checkpoint_writer.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantFileCheckpointWriter::ObTenantFileCheckpointWriter()
    : pg_meta_writer_(), tenant_file_key_(), file_mgr_(nullptr), is_inited_(false)
{}

int ObTenantFileCheckpointWriter::init(
    ObPartitionMetaRedoModule& pg_meta_mgr, ObBaseFileMgr& server_file_mgr, const ObTenantFileKey& tenant_file_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantFileCheckpointWriter has already been inited", K(ret));
  } else if (OB_FAIL(pg_meta_writer_.init(pg_meta_mgr, server_file_mgr, tenant_file_key))) {
    LOG_WARN("fail to init pg meta writer", K(ret));
  } else {
    tenant_file_key_ = tenant_file_key;
    file_mgr_ = &server_file_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantFileCheckpointWriter::write_checkpoint()
{
  int ret = OB_SUCCESS;
  ObTenantFileCheckpointEntry file_entry;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFileCheckpointWriter has not been inited", K(ret));
  } else if (OB_FAIL(pg_meta_writer_.write_checkpoint())) {
    LOG_WARN("fail to write checkpoint", K(ret));
  } else if (OB_FAIL(pg_meta_writer_.get_file_checkpoint_entry(file_entry))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ObTenantFileInfo file_info;
      // overwrite ret
      if (OB_FAIL(file_mgr_->get_tenant_file_info(tenant_file_key_, file_info))) {
        LOG_WARN("fail to get tenant file info", K(ret));
      } else {
        file_entry.tenant_file_key_ = tenant_file_key_;
        file_entry.super_block_ = file_info.tenant_file_super_block_;
        file_entry.super_block_.macro_meta_entry_.reset();
        file_entry.super_block_.pg_meta_entry_.reset();
      }
    } else {
      LOG_WARN("fail to get file checkpoint entry", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(file_mgr_->update_tenant_file_meta_info(file_entry, true /*write checkpoint slog*/))) {
      LOG_WARN("fail to update tenant file meta info", K(ret));
    } else {
      LOG_INFO("tenant file checkpoint write success", K(ret), K(tenant_file_key_));
    }
  }
  return ret;
}

void ObTenantFileCheckpointWriter::reset()
{
  pg_meta_writer_.reset();
  tenant_file_key_.reset();
  file_mgr_ = nullptr;
  is_inited_ = false;
}
