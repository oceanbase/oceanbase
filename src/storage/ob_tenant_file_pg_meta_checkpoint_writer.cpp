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

#include "ob_tenant_file_pg_meta_checkpoint_writer.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantFilePGMetaCheckpointWriter::ObTenantFilePGMetaCheckpointWriter()
    : partition_meta_mgr_(nullptr), server_file_mgr_(nullptr), tenant_file_key_(), is_inited_(false)
{}

int ObTenantFilePGMetaCheckpointWriter::init(ObPartitionMetaRedoModule& partition_meta_mgr,
    ObBaseFileMgr& server_file_mgr, const ObTenantFileKey& tenant_file_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantFilePGMetaCheckpointWriter has already been inited", K(ret));
  } else {
    partition_meta_mgr_ = &partition_meta_mgr;
    server_file_mgr_ = &server_file_mgr, tenant_file_key_ = tenant_file_key;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantFilePGMetaCheckpointWriter::write_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantFilePGMetaCheckpointWriter has not been inited", K(ret));
  } else if (OB_FAIL(writer_.init(*partition_meta_mgr_, *server_file_mgr_, tenant_file_key_))) {
    LOG_WARN("fail to init ObTenantFilePGMetaCheckpointWriter", K(ret));
  } else if (OB_FAIL(writer_.write_checkpoint())) {
    LOG_WARN("fail to write checkpoint", K(ret));
  }
  return ret;
}

void ObTenantFilePGMetaCheckpointWriter::reset()
{
  partition_meta_mgr_ = nullptr;
  server_file_mgr_ = nullptr;
  tenant_file_key_.reset();
  writer_.reset();
  is_inited_ = false;
}

int ObTenantFilePGMetaCheckpointWriter::get_file_checkpoint_entry(ObTenantFileCheckpointEntry& entry)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry>& file_checkpoint_map =
      writer_.get_file_checkpoint_map();
  if (OB_FAIL(file_checkpoint_map.get_refactored(tenant_file_key_, entry))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get file checkpoint map", K(ret));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}
