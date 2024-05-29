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
#include "storage/ob_bloom_filter_task.h"
#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "storage/blocksstable/ob_bloom_filter_data_reader.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "lib/stat/ob_session_stat.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "share/ob_get_compat_mode.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::blocksstable;
using namespace share;


/*
 * ObBloomFilterBuildTask
 * */

ObBloomFilterBuildTask::ObBloomFilterBuildTask(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const blocksstable::MacroBlockId &macro_id,
    const int64_t prefix_len)
    : IObDedupTask(T_BLOOMFILTER),
      tenant_id_(tenant_id),
      table_id_(table_id),
      macro_id_(macro_id),
      macro_handle_(),
      prefix_len_(prefix_len),
      allocator_(ObModIds::OB_BLOOM_FILTER),
      io_buf_(nullptr)
{
  abort_unless(OB_SUCCESS == macro_handle_.set_macro_block_id(macro_id));
}

ObBloomFilterBuildTask::~ObBloomFilterBuildTask()
{
}

int64_t ObBloomFilterBuildTask::hash() const
{
  uint64_t hash_val = macro_id_.hash();
  hash_val = murmurhash(&tenant_id_, sizeof(uint64_t), hash_val);
  hash_val = murmurhash(&table_id_, sizeof(uint64_t), hash_val);
  hash_val = murmurhash(&prefix_len_, sizeof(int64_t), hash_val);
  return hash_val;
}

bool ObBloomFilterBuildTask::operator ==(const IObDedupTask &other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else {
    if (get_type() == other.get_type()) {
      // it's safe to do this transformation, we have checked the task's type
      const ObBloomFilterBuildTask &o = static_cast<const ObBloomFilterBuildTask &>(other);
      is_equal = o.tenant_id_ == tenant_id_ && o.table_id_ == table_id_
                 && o.macro_id_ == macro_id_ && o.prefix_len_ == prefix_len_;
    }
  }
  return is_equal;
}

int64_t ObBloomFilterBuildTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

IObDedupTask *ObBloomFilterBuildTask::deep_copy(char *buffer, const int64_t buf_size) const
{
  ObBloomFilterBuildTask *task = NULL;
  if (NULL != buffer && buf_size >= get_deep_copy_size()) {
    task = new (buffer) ObBloomFilterBuildTask(
        tenant_id_,
        table_id_,
        macro_id_,
        prefix_len_);
  }
  return task;
}

int ObBloomFilterBuildTask::process()
{
  int ret = OB_SUCCESS;
  ObBloomFilterCacheValue bfcache_value;

  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)
      || OB_UNLIKELY(!macro_id_.is_valid())
      || OB_UNLIKELY(prefix_len_ <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("The bloom filter build task is not valid, ", K_(tenant_id),
      K_(macro_id), K_(prefix_len), K(ret));
  } else if (OB_FAIL(build_bloom_filter())) {
    LOG_WARN("Fail to build bloom filter, ", K(ret));
  } else {
    LOG_INFO("Success to build bloom filter, ", K_(tenant_id), K_(table_id), K_(macro_id), K_(prefix_len));
  }

  return ret;
}

int ObBloomFilterBuildTask::build_bloom_filter()
{
  int ret = OB_SUCCESS;

  MTL_SWITCH(tenant_id_) {
    void *buf = nullptr;
    ObStoreCtx store_ctx;
    bool need_build = false;

    ObBloomFilterCacheValue bfcache_value;
    ObMacroBlockHandle macro_handle;
    ObMacroBlockReadInfo read_info;
    ObMacroBlockRowBareIterator *macro_bare_iter = nullptr;
    ObSSTableMacroBlockHeader macro_header;
    const ObDatumRow *row = nullptr;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(MTL_ID(), table_id_, compat_mode))) {
      LOG_WARN("failed to get compat mode", K(ret), K_(table_id));
    } else {
      THIS_WORKER.set_compatibility_mode(compat_mode);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_STORE_CACHE.get_bf_cache().check_need_build(ObBloomFilterCacheKey(
        tenant_id_, macro_id_, prefix_len_), need_build))) {
      STORAGE_LOG(WARN, "Fail to check need build, ", K(ret));
    } else if (!need_build) {
      //already in cache,do nothing
    } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObMacroBlockRowBareIterator), ObModIds::OB_BLOOM_FILTER))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", "size", sizeof(ObMacroBlockRowBareIterator), K(ret));
    } else {
      macro_bare_iter = new (buf) ObMacroBlockRowBareIterator(allocator_);
      // submit io
      read_info.macro_block_id_ = macro_id_;
      read_info.offset_ = 0;
      read_info.size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
      read_info.io_desc_.set_sys_module_id(ObIOModule::BLOOM_FILTER_IO);
      if (OB_ISNULL(io_buf_) && OB_ISNULL(io_buf_ =
          reinterpret_cast<char*>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(OB_DEFAULT_MACRO_BLOCK_SIZE));
      } else {
        read_info.buf_ = io_buf_;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObBlockManager::read_block(read_info, macro_handle))) {
        LOG_WARN("Fail to read macro block", K(ret), K(read_info));
      } else if (OB_FAIL(macro_bare_iter->open(
          read_info.buf_, macro_handle.get_data_size(), true /*check*/))) {
        LOG_WARN("Fail to open bare macro block iterator", K(ret), K(macro_handle));
      } else if (OB_FAIL(macro_bare_iter->get_macro_block_header(macro_header))) {
        LOG_WARN("Fail to get macro block header", K(ret));
      } else if (OB_UNLIKELY(!macro_header.is_valid() || macro_header.is_normal_cg_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid macro block header", K(ret), K(macro_header));
      } else if (OB_FAIL(bfcache_value.init(prefix_len_, macro_header.fixed_header_.row_count_))) {
        LOG_WARN("Fail to init bloom filter", K(ret));
      } else {
        ObStorageDatumUtils datum_utils;
        ObDatumRowkey rowkey;
        if (OB_FAIL(datum_utils.init(macro_bare_iter->get_rowkey_column_descs(),
                                     macro_header.fixed_header_.rowkey_column_count_,
                                     compat_mode == lib::Worker::CompatMode::ORACLE,
                                     allocator_))) {
          STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K(macro_header));
        }
        while (OB_SUCC(ret) && OB_SUCC(macro_bare_iter->get_next_row(row))) {
          uint64_t key_hash = 0;
          if (OB_FAIL(rowkey.assign(row->storage_datums_, prefix_len_))) {
            STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), KPC(row), K(prefix_len_));
          } else if (OB_FAIL(rowkey.murmurhash(0, datum_utils, key_hash))) {
            STORAGE_LOG(WARN, "Failed to calc rowkey hash", K(ret), K(rowkey), K(datum_utils));
          } else if (OB_FAIL(bfcache_value.insert(static_cast<uint32_t>(key_hash)))) {
            LOG_WARN("Fail to insert rowkey to bfcache", K(ret));
          }
        }
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to iterate macro block", K(ret));
        } else if (OB_FAIL(ObStorageCacheSuite::get_instance().get_bf_cache().put_bloom_filter(
            tenant_id_, macro_id_, bfcache_value, true/* adaptive */))) {
          LOG_WARN("Fail to put value to bloom filter cache", K(ret), K_(tenant_id), K_(macro_id));
        }
      }

      if (OB_NOT_NULL(macro_bare_iter)) {
        macro_bare_iter->~ObMacroBlockRowBareIterator();
        ob_free(macro_bare_iter);
      }
    }
    macro_handle_.reset();
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase

