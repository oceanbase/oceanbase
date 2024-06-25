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

#include "ob_storage_meta_cache.h"

#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "share/io/ob_io_struct.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
ObStorageMetaKey::ObStorageMetaKey()
  : tenant_id_(0),
    phy_addr_()
{
}

ObStorageMetaKey::ObStorageMetaKey(const uint64_t tenant_id, const ObMetaDiskAddr &phy_addr)
  : tenant_id_(tenant_id),
    phy_addr_(phy_addr)
{
}

ObStorageMetaKey::~ObStorageMetaKey()
{
}

bool ObStorageMetaKey::operator ==(const ObIKVCacheKey &other) const
{
  const ObStorageMetaKey &other_key = reinterpret_cast<const ObStorageMetaKey &> (other);
  return phy_addr_ == other_key.phy_addr_
      && tenant_id_ == other_key.tenant_id_;
}

uint64_t ObStorageMetaKey::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObStorageMetaKey::hash() const
{
  return murmurhash(this, sizeof(ObStorageMetaKey), 0);
}

int64_t ObStorageMetaKey::size() const
{
  return sizeof(*this);
}

int ObStorageMetaKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid storage meta cache key", K(ret), K(*this));
  } else {
    key = new (buf) ObStorageMetaKey(tenant_id_, phy_addr_);
  }
  return ret;
}

bool ObStorageMetaKey::is_valid() const
{
  return phy_addr_.is_valid() && tenant_id_ > 0;
}

const ObMetaDiskAddr &ObStorageMetaKey::get_meta_addr() const
{
  return phy_addr_;
}

ObStorageMetaValue::StorageMetaProcessor ObStorageMetaValue::processor[ObStorageMetaValue::MetaType::MAX]
  = { ObStorageMetaValue::process_sstable,
      ObStorageMetaValue::process_co_sstable,
      ObStorageMetaValue::process_table_store
  };

ObStorageMetaValue::StorageMetaBypassProcessor ObStorageMetaValue::bypass_processor[MetaType::MAX]
  = { ObStorageMetaValue::bypass_process_storage_meta<blocksstable::ObSSTable>,
      ObStorageMetaValue::bypass_process_storage_meta<storage::ObCOSSTableV2>,
      nullptr // not support bypass process table store.
  };


ObStorageMetaValue::ObStorageMetaValue()
  : type_(MetaType::MAX),
    obj_(nullptr)
{
}

ObStorageMetaValue::ObStorageMetaValue(
    const MetaType type,
    ObIStorageMetaObj *obj)
  : type_(type),
    obj_(obj)
{
}

ObStorageMetaValue::~ObStorageMetaValue()
{
}

bool ObStorageMetaValue::is_valid() const
{
  return nullptr != obj_;
}

int64_t ObStorageMetaValue::size() const
{
  int64_t len = sizeof(*this);
#if __aarch64__
  len += ObSSTable::AARCH64_CP_BUF_ALIGN;
#endif
  len +=  obj_->get_deep_copy_size();
  return len;
}

int ObStorageMetaValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  ObStorageMetaValue *pvalue = nullptr;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid storage meta cache value", K(ret));
  } else {
    char *new_buf = buf + sizeof(ObStorageMetaValue);
    int64_t pos = sizeof(ObStorageMetaValue);
#if __aarch64__
    new_buf = reinterpret_cast<char *>(common::upper_align(
        reinterpret_cast<int64_t>(new_buf), ObSSTable::AARCH64_CP_BUF_ALIGN));
    pos = reinterpret_cast<int64_t>(new_buf) - reinterpret_cast<int64_t>(buf);
#endif
    pvalue = new (buf) ObStorageMetaValue();
    if (OB_FAIL(obj_->deep_copy(new_buf, buf_len - pos, pvalue->obj_))) {
      LOG_WARN("fail to deep copy storage meta object", K(ret), KP(buf), K(buf_len));
    } else {
      pvalue->type_ = type_;
      value = pvalue;
    }
  }
  return ret;
}

int ObStorageMetaValue::get_sstable(const blocksstable::ObSSTable *&sstable) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(MetaType::SSTABLE != type_ && MetaType::CO_SSTABLE != type_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not sstable", K(ret), K(type_));
  } else {
    sstable = static_cast<blocksstable::ObSSTable *>(obj_);
  }
  return ret;
}

int ObStorageMetaValue::get_sstable(blocksstable::ObSSTable *&sstable) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(MetaType::SSTABLE != type_ && MetaType::CO_SSTABLE != type_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not sstable", K(ret), K(type_));
  } else {
    sstable = static_cast<blocksstable::ObSSTable *>(obj_);
  }
  return ret;
}

int ObStorageMetaValue::get_table_store(const ObTabletTableStore *&store) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(MetaType::TABLE_STORE != type_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not table store", K(ret), K(type_));
  } else {
    store = static_cast<ObTabletTableStore *>(obj_);
  }
  return ret;
}

int ObStorageMetaValue::process_sstable(
    ObStorageMetaValueHandle &handle,
    const ObStorageMetaKey &key,
    const char *buf,
    const int64_t size,
    const ObTablet *tablet)
{
  UNUSED(tablet);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "ProSStable"));
  blocksstable::ObSSTable sstable;
  ObIStorageMetaObj *tiny_meta = nullptr;
  char *tmp_buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(size), K(handle));
  } else if (OB_FAIL(sstable.deserialize(allocator, buf, size, pos))) {
    LOG_WARN("fail to deserialize sstable", K(ret), KP(buf), K(size));
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator.alloc(sstable.get_deep_copy_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret), K(sstable.get_deep_copy_size()));
  } else if (OB_FAIL(sstable.deep_copy(tmp_buf, sstable.get_deep_copy_size(), tiny_meta))) {
    LOG_WARN("fail to deep copy sstable", K(ret), KP(tmp_buf), K(sstable));
  } else {
    ObStorageMetaCacheValue *cache_value = handle.get_cache_value();
    ObStorageMetaValue value(MetaType::SSTABLE, tiny_meta);
    if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().put_and_fetch(key, value, cache_value->value_, cache_value->cache_handle_))) {
      LOG_WARN("fail to put and fetch value into storage meta cache", K(ret), K(key), K(value), K(cache_value));
    } else {
      LOG_DEBUG("succeed to process sstable", K(ret), K(value), KPC(cache_value));
    }
  }
  if (OB_NOT_NULL(tiny_meta)) {
    tiny_meta->~ObIStorageMetaObj();
  }
  return ret;
}

int ObStorageMetaValue::process_co_sstable(
    ObStorageMetaValueHandle &handle,
    const ObStorageMetaKey &key,
    const char *buf,
    const int64_t size,
    const ObTablet *tablet)
{
  UNUSED(tablet);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  storage::ObCOSSTableV2 co_sstable;
  ObIStorageMetaObj *tiny_meta = nullptr;
  char *tmp_buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(size), K(handle));
  } else if (OB_FAIL(co_sstable.deserialize(allocator, buf, size, pos))) {
    LOG_WARN("fail to deserialize co sstable", K(ret), KP(buf), K(size));
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator.alloc(co_sstable.get_deep_copy_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret), K(co_sstable.get_deep_copy_size()));
  } else if (OB_FAIL(co_sstable.deep_copy(tmp_buf, co_sstable.get_deep_copy_size(), tiny_meta))) {
    LOG_WARN("fail to deep copy co sstable", K(ret), KP(tmp_buf), K(co_sstable));
  } else {
    ObStorageMetaCacheValue *cache_value = handle.get_cache_value();
    ObStorageMetaValue value(MetaType::CO_SSTABLE, tiny_meta);
    if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().put_and_fetch(key, value, cache_value->value_, cache_value->cache_handle_))) {
      LOG_WARN("fail to put and fetch value into secondary meta cache", K(ret), K(key), K(value), K(cache_value));
    }
  }
  if (OB_NOT_NULL(tiny_meta)) {
    tiny_meta->~ObIStorageMetaObj();
  }
  return ret;
}

int ObStorageMetaValue::process_table_store(
    ObStorageMetaValueHandle &handle,
    const ObStorageMetaKey &key,
    const char *buf,
    const int64_t size,
    const ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "ProcMetaVaule"));
  ObTabletTableStore table_store;
  ObIStorageMetaObj *tiny_meta = nullptr;
  char *tmp_buf = nullptr;
  int64_t pos = 0;
  ObTimeGuard time_guard("cache_process", 10_ms); //10ms
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !handle.is_valid()) || OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(size), K(handle));
  } else if (OB_FAIL(table_store.deserialize(allocator, *tablet, buf, size, pos))) {
    LOG_WARN("fail to deserialize table store", K(ret), KP(buf), K(size));
  } else if (FALSE_IT(time_guard.click("deserialize"))) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator.alloc(table_store.get_deep_copy_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret), K(table_store.get_deep_copy_size()));
  } else if (FALSE_IT(time_guard.click("allocate"))) {
  } else if (OB_FAIL(table_store.deep_copy(tmp_buf, table_store.get_deep_copy_size(), tiny_meta))) {
    LOG_WARN("fail to deep copy table store", K(ret), KP(tmp_buf), K(table_store));
  } else {
    time_guard.click("deep_copy");
    ObStorageMetaCacheValue *cache_value = handle.get_cache_value();
    ObStorageMetaValue value(MetaType::TABLE_STORE, tiny_meta);
    if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().put_and_fetch(key, value, cache_value->value_, cache_value->cache_handle_))) {
      LOG_WARN("fail to put and fetch value into storage meta cache", K(ret), K(key), K(value), K(cache_value));
    }
    time_guard.click("put_cache");
  }
  if (OB_NOT_NULL(tiny_meta)) {
    tiny_meta->~ObIStorageMetaObj();
  }
  return ret;
}

ObStorageMetaValueHandle::ObStorageMetaValueHandle(const ObStorageMetaValueHandle &other)
  : cache_value_(nullptr),
    allocator_(nullptr)
{
  *this = other;
}
ObStorageMetaValueHandle &ObStorageMetaValueHandle::operator=(const ObStorageMetaValueHandle &other)
{
  if (this != &other) {
    reset();
    abort_unless(OB_SUCCESS == set_cache_value(other.cache_value_, other.allocator_));
  }
  return *this;
}

int ObStorageMetaValueHandle::new_value(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t size = sizeof(ObStorageMetaCacheValue);
  reset();
  void *buf = nullptr;
  allocator_ = &allocator;
  if (OB_ISNULL(cache_value_ = OB_NEWx(ObStorageMetaCacheValue, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size), KP(allocator_));
  } else {
    cache_value_->inc_ref();
  }
  return ret;
}

int ObStorageMetaValueHandle::set_cache_value(
    ObStorageMetaCacheValue *value,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr == value && nullptr == allocator) {
    reset();
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(value), KP(allocator));
  } else if (cache_value_ != value) {
    value->inc_ref();
    cache_value_ = value;
    allocator_ = allocator;
  } else if (OB_UNLIKELY(allocator_ != allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ isn't equal to allocator", K(ret), KP(allocator_), KP(allocator));
  }
  return ret;
}

void ObStorageMetaValueHandle::reset()
{
  if (nullptr != cache_value_) {
    if (OB_UNLIKELY(nullptr == allocator_)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "allocator is nullptr, and memory can be leak", KP_(cache_value), KP_(allocator));
    } else if (0 == cache_value_->dec_ref()) {
      cache_value_->~ObStorageMetaCacheValue();
      allocator_->free(cache_value_);
    }
  }
  cache_value_ = nullptr;
  allocator_ = nullptr;
}

ObStorageMetaHandle::ObStorageMetaHandle()
  : phy_addr_(),
    io_handle_(),
    cache_handle_()
{
}

ObStorageMetaHandle::~ObStorageMetaHandle()
{
  reset();
}

int ObStorageMetaHandle::get_value(const ObStorageMetaValue *&value)
{
  int ret = OB_SUCCESS;
  if (!io_handle_.is_empty() && OB_FAIL(wait())) { /*wait if not hit cache*/
    LOG_WARN("fail to wait", K(ret), KPC(this));
  } else {
    value = cache_handle_.get_cache_value()->value_;
  }
  return ret;
}

int ObStorageMetaHandle::get_sstable(blocksstable::ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  const ObStorageMetaValue *meta_value = nullptr;
  sstable = nullptr;
  if (OB_FAIL(get_value(meta_value))) {
    LOG_WARN("fail to get sstable value", K(ret), K_(phy_addr), K_(io_handle), K_(cache_handle));
  } else if (OB_FAIL(meta_value->get_sstable(sstable))) {
    LOG_WARN("fail to get loaded sstable", K(ret), KPC(meta_value));
  }
  return ret;
}

void ObStorageMetaHandle::reset()
{
  phy_addr_.reset();
  io_handle_.reset();
  cache_handle_.reset();
}

bool ObStorageMetaHandle::is_valid() const
{
  const bool valid_cache_handle = phy_addr_.is_valid() && cache_handle_.is_valid();
  const bool valid_io_handle = phy_addr_.is_block() && io_handle_.is_valid();
  return valid_cache_handle || valid_io_handle;
}

int ObStorageMetaHandle::wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!phy_addr_.is_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected meta address", K(ret), K_(phy_addr));
  } else if (OB_FAIL(io_handle_.wait())) {
    LOG_WARN("fail to wait io handle", K(ret), K(io_handle_));
  }
  return ret;
}

int ObStorageMetaCache::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((common::ObKVCache<ObStorageMetaKey, ObStorageMetaValue>::init(cache_name,  priority)))) {
    LOG_WARN("fail to init storage meta kv cache", K(ret), K(priority));
  }
  return ret;
}

void ObStorageMetaCache::destory()
{
  common::ObKVCache<ObStorageMetaKey, ObStorageMetaValue>::destroy();
}

ObStorageMetaCache::ObStorageMetaIOCallback::ObStorageMetaIOCallback(
    common::ObIAllocator *io_allocator,
    const ObStorageMetaValue::MetaType type,
    const ObStorageMetaKey &key,
    ObStorageMetaValueHandle &handle,
    const ObTablet *tablet,
    common::ObSafeArenaAllocator *arena_allocator)
  : ObSharedBlockIOCallback(io_allocator, key.get_meta_addr()),
    meta_type_(type),
    key_(key),
    handle_(handle),
    tablet_(tablet),
    arena_allocator_(arena_allocator)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObStorageMetaCache::ObStorageMetaIOCallback::~ObStorageMetaIOCallback()
{
  meta_type_ = ObStorageMetaValue::MetaType::MAX;
  handle_.reset();
}

int ObStorageMetaCache::ObStorageMetaIOCallback::do_process(const char *buf, const int64_t buf_len)
{
  // TODO: callback need to deal with block-crossed shared blocks,
  // in which scene we only store the first blocks' addr
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid storage meta cache callback", K(ret), K_(handle));
  } else if (OB_UNLIKELY(buf_len <= 0 || buf == nullptr)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data buffer size", K(ret), K(buf_len), KP(buf));
  } else if (OB_UNLIKELY(nullptr != arena_allocator_)) { // bypass cache processor
    if (OB_FAIL(ObStorageMetaValue::bypass_processor[meta_type_](meta_type_, *arena_allocator_,
        handle_, buf, buf_len))) {
      LOG_WARN("fail to process io buf", K(ret), K(meta_type_), KP(buf), K(buf_len));
    }
  } else if (OB_FAIL(ObStorageMetaValue::processor[meta_type_](handle_, key_, buf, buf_len, tablet_))) {
    LOG_WARN("fail to process io buf", K(ret), K(meta_type_), KP(buf), K(buf_len));
  }
  return ret;
}

int64_t ObStorageMetaCache::ObStorageMetaIOCallback::size() const
{
  return sizeof(*this);
}


bool ObStorageMetaCache::ObStorageMetaIOCallback::is_valid() const
{
  return ObSharedBlockIOCallback::is_valid() && key_.is_valid() && handle_.is_valid();
}

int ObStorageMetaCache::get_meta(
    const ObStorageMetaValue::MetaType type,
    const ObStorageMetaKey &key,
    ObStorageMetaHandle &meta_handle,
    const ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || type >= ObStorageMetaValue::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(type));
  } else if (OB_FAIL(meta_handle.cache_handle_.new_value(MTL(ObTenantMetaMemMgr *)->get_meta_cache_io_allocator()))) {
    LOG_WARN("fail to new cache handle value", K(ret));
  } else if (OB_FAIL(get(key, meta_handle.cache_handle_.get_cache_value()->value_,
      meta_handle.cache_handle_.get_cache_value()->cache_handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get storage meta from cache", K(ret), K(type), K(key));
    } else if (OB_FAIL(prefetch(type, key, meta_handle, tablet))) {
      LOG_WARN("fail to prefetch", K(ret), K(type), K(key));
    } else {
      EVENT_INC(ObStatEventIds::STORAGE_META_CACHE_MISS);
    }
  } else {
    meta_handle.phy_addr_ = key.get_meta_addr();
    EVENT_INC(ObStatEventIds::STORAGE_META_CACHE_HIT);
  }
  return ret;
}

int ObStorageMetaCache::bypass_get_meta(
    const ObStorageMetaValue::MetaType type,
    const ObStorageMetaKey &key,
    common::ObSafeArenaAllocator &allocator,
    ObStorageMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type >= ObStorageMetaValue::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(type));
  } else if (OB_UNLIKELY(ObStorageMetaValue::TABLE_STORE == type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Don't supported for table store", K(ret), K(type), K(key));
  } else if (OB_FAIL(get_meta_and_bypass_cache(type, key, allocator, meta_handle))) {
    LOG_WARN("fail to get meta and bypass cache", K(ret), K(type), K(key));
  }
  return ret;
}

int ObStorageMetaCache::batch_get_meta_and_bypass_cache(
      const common::ObIArray<ObStorageMetaValue::MetaType> &meta_types,
      const common::ObIArray<ObStorageMetaKey> &keys,
      common::ObSafeArenaAllocator &allocator,
      common::ObIArray<ObStorageMetaHandle> &meta_handles)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(meta_types.count() != keys.count()
               || keys.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(meta_types), K(keys));
  } else {
    // TODO: implement batch read in shared block reader.
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      const ObStorageMetaValue::MetaType &meta_type = meta_types.at(i);
      const ObStorageMetaKey &key = keys.at(i);
      ObStorageMetaHandle meta_handle;

      if (OB_UNLIKELY(ObStorageMetaValue::TABLE_STORE == meta_type)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Don't supported for table store", K(ret), K(meta_type), K(key));
      } else if (OB_FAIL(get_meta_and_bypass_cache(meta_type, key, allocator, meta_handle))) {
        LOG_WARN("fail to do get meta", K(ret), K(meta_type), K(key), K(meta_handle));
      } else if (OB_FAIL(meta_handles.push_back(meta_handle))) {
        LOG_WARN("fail to push back meta handle", K(ret), K(meta_handle));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(keys.count() != meta_handles.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, the number of keys and meta handles is not equal", K(ret), K(meta_types),
          K(keys), K(meta_handles));
    }
  }
  return ret;
}

int ObStorageMetaCache::prefetch(
    const ObStorageMetaValue::MetaType type,
    const ObStorageMetaKey &key,
    ObStorageMetaHandle &meta_handle,
    const ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || type >= ObStorageMetaValue::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(type));
  } else {
    void *buf = nullptr;
    common::ObIAllocator &io_allocator = MTL(ObTenantMetaMemMgr *)->get_meta_cache_io_allocator();
    ObStorageMetaIOCallback *callback = nullptr;
    if (OB_ISNULL(buf = io_allocator.alloc(sizeof(ObStorageMetaIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObStorageMetaIOCallback(&io_allocator,
                                                   type,
                                                   key,
                                                   meta_handle.cache_handle_,
                                                   tablet,
                                                   nullptr/*bypass_cache if nullptr*/);
      if (OB_FAIL(read_io(key.get_meta_addr(), *callback, meta_handle))) {
        LOG_WARN("fail to read storage meta from io", K(ret), K(key), K(meta_handle));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(callback->get_allocator())) { //Avoid double_free with io_handle
        callback->~ObStorageMetaIOCallback();
        io_allocator.free(callback);
      }
    }
  }
  return ret;
}

int ObStorageMetaCache::get_meta_and_bypass_cache(
    const ObStorageMetaValue::MetaType type,
    const ObStorageMetaKey &key,
    common::ObSafeArenaAllocator &allocator,
    ObStorageMetaHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || type >= ObStorageMetaValue::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(type));
  } else if (OB_FAIL(handle.cache_handle_.new_value(MTL(ObTenantMetaMemMgr *)->get_meta_cache_io_allocator()))) {
    LOG_WARN("fail to new cache handle value", K(ret));
  } else {
    void *buf = nullptr;
    common::ObIAllocator &io_allocator = MTL(ObTenantMetaMemMgr *)->get_meta_cache_io_allocator();
    ObStorageMetaIOCallback *callback = nullptr;
    if (OB_ISNULL(buf = io_allocator.alloc(sizeof(ObStorageMetaIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObStorageMetaIOCallback(&io_allocator,
                                                   type,
                                                   key,
                                                   handle.cache_handle_,
                                                   nullptr/*tablet*/,
                                                   &allocator/*bypass_cache if nullptr*/);
      if (OB_FAIL(read_io(key.get_meta_addr(), *callback, handle))) {
        LOG_WARN("fail to read storage meta from io", K(ret), K(key), K(handle));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(callback->get_allocator())) { //Avoid double_free with io_handle
        callback->~ObStorageMetaIOCallback();
        io_allocator.free(callback);
      }
    }
  }
  return ret;
}

int ObStorageMetaCache::read_io(
    const ObMetaDiskAddr &meta_addr,
    ObStorageMetaIOCallback &callback,
    ObStorageMetaHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(meta_addr), K(callback));
  } else if (OB_UNLIKELY(!meta_addr.is_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the meta disk address type hasn't be supported", K(ret), K(meta_addr), K(callback));
  } else {
    ObSharedBlockReadInfo read_info;
    read_info.addr_ = meta_addr;
    read_info.io_callback_ = &callback;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    handle.phy_addr_ = meta_addr;
    if (OB_FAIL(ObSharedBlockReaderWriter::async_read(read_info, handle.io_handle_))) {
      LOG_WARN("fail to async read", K(ret), K(read_info));
    }
  }
  return ret;
}

ObStorageMetaCache::ObStorageMetaCache()
{
}

ObStorageMetaCache::~ObStorageMetaCache()
{
}

} // end namespace storage
} // end namespace oceanbase
