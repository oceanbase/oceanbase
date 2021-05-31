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

#include "ob_pg_macro_meta_checkpoint_writer.h"
#include "storage/ob_sstable.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

OB_SERIALIZE_MEMBER(ObPGMacroBlockMetaCheckpointEntry, disk_no_, macro_block_id_, table_key_, meta_);

ObPGMacroMeta::ObPGMacroMeta() : allocator_(), buf_(nullptr), buf_size_(0), meta_entry_(nullptr)
{}

void ObPGMacroMeta::set_meta_entry(ObPGMacroBlockMetaCheckpointEntry& entry)
{
  meta_entry_ = &entry;
}

int ObPGMacroMeta::serialize(const char*& buf, int64_t& len)
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  len = 0;
  if (OB_ISNULL(meta_entry_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, sstable must not be null", K(ret));
  } else {
    const int64_t serialize_size = meta_entry_->get_serialize_size();
    if (serialize_size > buf_size_) {
      if (OB_FAIL(extend_buf(serialize_size))) {
        LOG_WARN("fail to extend buf", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      if (OB_FAIL(meta_entry_->serialize(buf_, buf_size_, pos))) {
        LOG_WARN("fail to serialize sstable", K(ret));
      } else if (pos != serialize_size) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, macro meta serialize size is not expected", K(ret), K(pos), K(serialize_size));
      } else {
        buf = buf_;
        len = serialize_size;
      }
    }
  }
  return ret;
}

int ObPGMacroMeta::extend_buf(const int64_t request_size)
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

ObPGMacroMetaIterator::ObPGMacroMetaIterator()
    : is_inited_(false),
      tables_handle_(),
      block_infos_(),
      table_idx_(0),
      macro_idx_(0),
      item_(),
      allocator_(),
      buf_(nullptr)
{}

int ObPGMacroMetaIterator::init(ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMacroMetaIterator has already been inited", K(ret));
  } else if (OB_FAIL(tables_handle_.assign(tables_handle))) {
    LOG_WARN("fail to assign tables handle", K(ret));
  } else if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(sizeof(ObPGMacroBlockMetaCheckpointEntry))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    if (tables_handle_.get_count() > 0) {
      ObSSTable* sstable = static_cast<ObSSTable*>(tables_handle_.get_table(0));
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, sstable must not be null", K(ret));
      } else if (OB_FAIL(sstable->get_all_macro_info(block_infos_))) {
        LOG_WARN("fail to get all block metas", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPGMacroMetaIterator::get_next_item(ObIPGMetaItem*& item)
{
  int ret = OB_SUCCESS;
  item = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMacroMetaIterator has not been inited", K(ret));
  } else if (table_idx_ >= tables_handle_.get_count() && macro_idx_ >= block_infos_.count()) {
    ret = OB_ITER_END;
  } else {
    if (macro_idx_ < block_infos_.count()) {
      // TODO(): replace with mask
      ObSSTable* sstable = static_cast<ObSSTable*>(tables_handle_.get_table(table_idx_));
      ObPGMacroBlockMetaCheckpointEntry* entry = new (buf_) ObPGMacroBlockMetaCheckpointEntry(0,
          block_infos_.at(macro_idx_).block_id_,
          sstable->get_key(),
          const_cast<ObMacroBlockMetaV2&>(*block_infos_.at(macro_idx_).meta_.meta_));
      item_.set_meta_entry(*entry);
      ++macro_idx_;
      item = &item_;
    } else {
      while (OB_SUCC(ret)) {
        ++table_idx_;
        if (table_idx_ >= tables_handle_.get_count()) {
          ret = OB_ITER_END;
        } else {
          ObSSTable* sstable = static_cast<ObSSTable*>(tables_handle_.get_table(table_idx_));
          if (OB_ISNULL(sstable)) {
            ret = OB_ERR_SYS;
            LOG_WARN("error sys, sstable must not be null", K(ret));
          } else if (OB_FAIL(sstable->get_all_macro_info(block_infos_))) {
            LOG_WARN("fail to get all block metas", K(ret));
          } else if (block_infos_.count() > 0) {
            macro_idx_ = 0;
            ObSSTable* sstable = static_cast<ObSSTable*>(tables_handle_.get_table(table_idx_));
            ObPGMacroBlockMetaCheckpointEntry* entry = new (buf_) ObPGMacroBlockMetaCheckpointEntry(0,
                block_infos_.at(macro_idx_).block_id_,
                sstable->get_key(),
                const_cast<ObMacroBlockMetaV2&>(*block_infos_.at(macro_idx_).meta_.meta_));
            item_.set_meta_entry(*entry);
            ++macro_idx_;
            item = &item_;
            break;
          }
        }
      }
    }
  }
  return ret;
}

void ObPGMacroMetaIterator::reset()
{
  is_inited_ = false;
  tables_handle_.reset();
  block_infos_.reset();
  table_idx_ = 0;
  macro_idx_ = 0;
  allocator_.reset();
  buf_ = nullptr;
}

ObPGMacroMetaCheckpointWriter::ObPGMacroMetaCheckpointWriter() : is_inited_(false), writer_(), iter_()
{}

int ObPGMacroMetaCheckpointWriter::init(ObTablesHandle& tables_handle, ObPGMetaItemWriter& writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMacroMetaBlockWriter has already been inited", K(ret));
  } else if (OB_FAIL(iter_.init(tables_handle))) {
    LOG_WARN("fail to init ObPGMacroMetaIterator", K(ret));
  } else {
    writer_ = &writer;
  }
  return ret;
}

int ObPGMacroMetaCheckpointWriter::write_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMacroMetaCheckpointWriter has not been inited", K(ret));
  } else {
    ObIPGMetaItem* item = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter_.get_next_item(item))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next item", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(writer_->write_item(item))) {
        LOG_WARN("fail to write item", K(ret));
      }
    }
  }
  return ret;
}

void ObPGMacroMetaCheckpointWriter::reset()
{
  is_inited_ = false;
  writer_ = nullptr;
  iter_.reset();
}
