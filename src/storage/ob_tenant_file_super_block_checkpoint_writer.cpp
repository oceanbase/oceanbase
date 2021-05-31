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

#include "ob_tenant_file_super_block_checkpoint_writer.h"
#include "storage/ob_tenant_file_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

OB_SERIALIZE_MEMBER(ObTenantFileSuperBlockCheckpointEntry, file_info_);

ObTenantFileSuperBlockItem::ObTenantFileSuperBlockItem() : allocator_(), buf_(nullptr), buf_size_(0), entry_(nullptr)
{}

ObTenantFileSuperBlockItem::~ObTenantFileSuperBlockItem()
{}

void ObTenantFileSuperBlockItem::set_tenant_file_entry(ObTenantFileSuperBlockCheckpointEntry& entry)
{
  entry_ = &entry;
}

int ObTenantFileSuperBlockItem::serialize(const char*& buf, int64_t& buf_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(entry_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, entry must not be null", K(ret));
  } else {
    const int64_t serialize_size = entry_->get_serialize_size();
    if (serialize_size > buf_size_) {
      if (OB_FAIL(extend_buf(serialize_size))) {
        LOG_WARN("fail to extend buf", K(ret), K(serialize_size));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      if (OB_FAIL(entry_->serialize(buf_, buf_size_, pos))) {
        LOG_WARN("fail to serialize pg super block", K(ret));
      } else {
        buf = buf_;
        buf_size = pos;
      }
    }
  }
  return ret;
}

int ObTenantFileSuperBlockItem::extend_buf(const int64_t request_size)
{
  int ret = OB_SUCCESS;
  if (request_size > buf_size_) {
    allocator_.reuse();
    buf_size_ = 0;
    if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(request_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(request_size));
    } else {
      buf_size_ = request_size;
    }
  }
  return ret;
}

ObTenantFileSuperBlockCheckpointWriter::ObTenantFileSuperBlockCheckpointWriter() : writer_()
{}

int ObTenantFileSuperBlockCheckpointWriter::write_checkpoint(blocksstable::ObStorageFileHandle& server_root_handle,
    ObBaseFileMgr& file_mgr, common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry>& file_checkpoint_map,
    blocksstable::ObSuperBlockMetaEntry& entry)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObArray<ObTenantFileInfo*> tenant_file_infos;
  writer_.reset();
  entry.reset();
  if (OB_UNLIKELY(!server_root_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(server_root_handle));
  } else if (OB_FAIL(file_mgr.get_all_tenant_file_infos(allocator, tenant_file_infos))) {
    LOG_WARN("fail to get all tenant file infos", K(ret));
  } else if (OB_FAIL(writer_.init(server_root_handle))) {
    LOG_WARN("fail to init checkpoint item writer", K(ret));
  } else {
    LOG_INFO("get all tenant file infos", K(tenant_file_infos));
    ObTenantFileSuperBlockItem item;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_file_infos.count(); ++i) {
      ObTenantFileInfo& file_info = *tenant_file_infos.at(i);
      ObTenantFileCheckpointEntry file_checkpoint_entry;
      if (OB_FAIL(file_checkpoint_map.get_refactored(file_info.tenant_key_, file_checkpoint_entry))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get from file checkpoint map", K(ret));
        }
      } else {
        // only update entry block id of macro meta and pg meta
        file_info.tenant_file_super_block_.macro_meta_entry_ = file_checkpoint_entry.super_block_.macro_meta_entry_;
        file_info.tenant_file_super_block_.pg_meta_entry_ = file_checkpoint_entry.super_block_.pg_meta_entry_;
      }

      if (OB_SUCC(ret)) {
        ObTenantFileSuperBlockCheckpointEntry entry(*tenant_file_infos.at(i));
        item.set_tenant_file_entry(entry);
        if (OB_FAIL(writer_.write_item(&item))) {
          LOG_WARN("fail to write item", K(ret));
        }
      }
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = E(EventTable::EN_SERVER_TENANT_FILE_SUPER_BLOCK_WRITE_HALF_FAILED) OB_SUCCESS;
        LOG_INFO("server tenant file iterator", K(i), K(ret));
        if (OB_FAIL(ret)) {
          if (i >= 2) {
            STORAGE_LOG(INFO, "server tenant file super block write checkpoint half fail", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
#endif
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(writer_.close())) {
        LOG_WARN("fail to close writer", K(ret));
      } else if (OB_FAIL(writer_.get_entry_block_index(entry.macro_block_id_))) {
        LOG_WARN("fail to get entry block index", K(ret));
      }
    }
  }
  for (int64_t i = 0; i < tenant_file_infos.count(); ++i) {
    if (nullptr != tenant_file_infos.at(i)) {
      tenant_file_infos.at(i)->~ObTenantFileInfo();
      tenant_file_infos.at(i) = nullptr;
    }
  }
  tenant_file_infos.reset();
  return ret;
}

void ObTenantFileSuperBlockCheckpointWriter::reset()
{
  writer_.reset();
}

ObIArray<MacroBlockId>& ObTenantFileSuperBlockCheckpointWriter::get_meta_block_list()
{
  return writer_.get_meta_block_list();
}
