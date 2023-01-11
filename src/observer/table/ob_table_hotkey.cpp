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
#define USING_LOG_PREFIX SERVER
#include "ob_table_hotkey.h"
#include "ob_table_utils.h"
#include "ob_table_throttle.h"
#include "ob_table_hotkey_kvcache.h"
#include "ob_table_throttle_manager.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

/**
 * -----------------------------------ObTableHotKeyHeap-----------------------------------
 */
ObTableHotKeyHeap::ObTableHotKeyHeap()
    :is_inited_(false),
     heap_size_(0),
     heap_(nullptr),
     cmp_(),
     allocator_(nullptr)
{
}

bool ObTableHotKeyHeap::HotkeyCompareFunctor::operator()(const ObTableHotkeyPair *a, const ObTableHotkeyPair *b) const
{
  bool bret = true;
  if (OB_ISNULL(a) || OB_ISNULL(b)) {
    bret = false;
    LOG_ERROR("Comparing request is NULL", K(a), K(b));
  } else {
    bret = a->second > b->second;
  }
  return bret;
}

/**
  * @param [in]   allocator arena allocator for alloc
  * @param [in]   heap_max_size heap size
  */
int ObTableHotKeyHeap::init(common::ObArenaAllocator &allocator, const int64_t &heap_max_size)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The Table Hotkey heap has been inited, ", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(heap_max_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(heap_max_size), K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(buf = allocator.alloc(sizeof(ObTableHotkeyPair *) * heap_max_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory, ", K(heap_max_size), K(ret));
  } else {
    heap_ = new (buf) ObTableHotkeyPair*[heap_max_size];
    MEMSET(heap_, 0, sizeof(ObTableHotkeyPair*) * heap_max_size);
    heap_size_ = heap_max_size;
    count_ = 0;
    is_inited_ = true;
    arena_allocator_ = &allocator;
  }
  return ret;
}

void ObTableHotKeyHeap::destroy()
{
  if (OB_NOT_NULL(heap_)) {
    arena_allocator_->free(heap_);
    heap_ = nullptr;
  }
  heap_size_ = 0;
  count_ = 0;
  is_inited_ = false;
}

/**
  * clear() will release the memory that used for hotkey
  * warn: Key in HotKeyMap use the same memory for a hotkey
  */
void ObTableHotKeyHeap::clear()
{
  if (nullptr == allocator_) {
    // do nothing
  } else {
    for (int64_t i = 0; i < count_; ++i) {
      // release hotkey first then pair that contains hotkey
      allocator_->free(heap_[i]->first);
      allocator_->free(heap_[i]);
    }
    count_ = 0;
  }
}

/**
  * @param [in]   hotkeypair insert hotkey pair
  * hotkey pair should construct outside of heap
  * Also, should pop a pair from a full heap before insert
  */
int ObTableHotKeyHeap::push(ObTableHotkeyPair &hotkeypair)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The HotKey Heap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(count_ >= heap_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("The HotKey Heap is full, ", K(ret));
  } else {
    heap_[count_++] = &hotkeypair;
    std::push_heap(&heap_[0], &heap_[count_], cmp_);
  }
  return ret;
}

/**
  * @param [in]     hotkeypair which hot key pair to pop from heap
  * @param [out]    get_cnt get_cnt of input hot key pair
  * pop() will not free the memory of hotkey pair, should free outside heap
  */
int ObTableHotKeyHeap::pop(ObTableHotkeyPair *&hotkeypair, ObTableHotKeyValue &get_cnt)
{
  int ret = OB_SUCCESS;
  hotkeypair = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The HotKey Heap has not been inited, ", K(ret));
  } else if (0 == count_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    std::pop_heap(&heap_[0], &heap_[count_--], cmp_);
    hotkeypair = heap_[count_];
    get_cnt = hotkeypair->second;
  }
  return ret;
}

int ObTableHotKeyHeap::top(ObTableHotkeyPair *&hotkeypair)
{
  int ret = OB_SUCCESS;
  if (heap_size_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to get top of the hotkey heap", K(ret));
  } else {
    hotkeypair = heap_[0];
  }
  return ret;
}

/**
 * -----------------------------------ObTableTenantHotKeyInfra-----------------------------------
 */
const int64_t ObTableTenantHotKey::ObTableTenantHotKeyInfra::MAX_HOT_KEY_SIZE;

int ObTableTenantHotKey::ObTableTenantHotKeyInfra::init(ObArenaAllocator &arena_allocator, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hotkey_heap_.init(arena_allocator, MAX_HOT_KEY_SIZE))) {
    LOG_WARN("fail to init hotkey_heap_", K(ret));
  } else if (OB_FAIL(hotkey_map_.create(2 * MAX_HOT_KEY_SIZE, ObModIds::OB_TABLE_HOTKEY, ObModIds::OB_TABLE_HOTKEY, tenant_id))) {
    hotkey_heap_.destroy();
    LOG_WARN("fail to init hotkey_map_", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTableTenantHotKey::ObTableTenantHotKeyInfra::clear()
{
  DRWLock::WRLockGuard guard(rwlock_);
  hotkey_map_.clear();
  hotkey_heap_.clear();
}

void ObTableTenantHotKey::ObTableTenantHotKeyInfra::destroy()
{
  DRWLock::WRLockGuard guard(rwlock_);
  hotkey_map_.destroy();
  hotkey_heap_.destroy();
  is_inited_ = false;
}

/**
  * @param [out]  heap_min min value in heap
  * get min value from heap
  */
int ObTableTenantHotKey::ObTableTenantHotKeyInfra::get_heap_min(ObTableHotKeyValue &heap_min)
{
  int ret = OB_SUCCESS;
  ObTableHotkeyPair* hotkeypair = nullptr;

  if (OB_FAIL(hotkey_heap_.top(hotkeypair))) {
    LOG_WARN("fail to get top of hotkey heap", K(ret));
  } else {
    heap_min = hotkeypair->second;
  }

  return ret;
}

int64_t ObTableTenantHotKey::ObTableTenantHotKeyInfra::get_heap_cnt()
{
  DRWLock::RDLockGuard guard(rwlock_);
  return hotkey_heap_.get_count();
}

/**
 * -----------------------------------ObTableTenantHotKey-----------------------------------
 */
const int64_t ObTableTenantHotKey::HOTKEY_WINDOWS_SIZE;
const int64_t ObTableTenantHotKey::TENANT_HOTKEY_INFRA_NUM;

int ObTableTenantHotKey::init()
{
  int ret = OB_SUCCESS;

  for (int i = 0; OB_SUCC(ret) && i < TENANT_HOTKEY_INFRA_NUM; ++i) {
    if (OB_FAIL(infra_[i].init(arena_allocator_, tenant_id_))) {
      LOG_WARN("fail to init infra", K(tenant_id_), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
      LOG_WARN("fail to init tenant hotkey allocator", K(tenant_id_), K(ret));
    } else if (OB_FAIL(hotkey_slide_window_map_.create(2*ObTableTenantHotKeyInfra::MAX_HOT_KEY_SIZE, ObModIds::OB_TABLE_HOTKEY, ObModIds::OB_TABLE_HOTKEY, tenant_id_))) {
      LOG_WARN("fail to init hotkey slide window map", K(tenant_id_), K(ret));
    } else {
      allocator_.set_label(ObModIds::OB_TABLE_HOTKEY);
      // allocator_ store the data, arena_allocator_ store the structure of heap
      for (int i = 0; i < TENANT_HOTKEY_INFRA_NUM; ++i) {
        infra_[i].hotkey_heap_.set_allocator(allocator_);
      }
    }
  }

  if (OB_FAIL(ret)) {
    for (int i = 0; i < TENANT_HOTKEY_INFRA_NUM; ++i) {
      if (infra_[i].is_inited_) {
        infra_[i].destroy();
      }
    }
  }
  return ret;
}

void ObTableTenantHotKey::clear()
{
  // data of hotkey store in heap
  for (int i = 0; i < TENANT_HOTKEY_INFRA_NUM; ++i) {
    if (infra_[i].is_inited_) {
      infra_[i].destroy();
    }
  }

  // clear slide window map
  for (ObTableTenantSlideWindowInfraMap::iterator iter = hotkey_slide_window_map_.begin();
       iter != hotkey_slide_window_map_.end(); ++iter) {
    // remove key
    allocator_.free(iter->first);
    // remove infra
    allocator_.free(iter->second);
    // remove structure in map
    hotkey_slide_window_map_.erase_refactored(iter->first);
  }
  hotkey_slide_window_map_.clear();
}

void ObTableTenantHotKey::destroy() {
  clear();
  for (int i = 0; i < TENANT_HOTKEY_INFRA_NUM; ++i) {
    if (infra_[i].is_inited_) {
      infra_[i].destroy();
    }
  }
  hotkey_slide_window_map_.destroy();
}

/**
  * @param [in]     hotkey hot key from heap that want to insert into slide window
  * add_hotkey_slide_window will deep copy hotkey
  */
int ObTableTenantHotKey::add_hotkey_slide_window(ObTableHotKey &hotkey, int64_t cnt)
{
  int ret = OB_SUCCESS;
  ObTableSlideWindowHotkey swhotkey = hotkey;
  ObTableSlideWinHotKeyInfra *slidewin_infra = nullptr;
  ObTableSlideWindowHotkey *photkey = nullptr;
  char* buf = nullptr;

  if (OB_FAIL(hotkey_slide_window_map_.get_refactored(&swhotkey, slidewin_infra))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // Add new key into slide window
      slidewin_infra = nullptr;

      // construct slide window element and put into map
      if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObTableSlideWinHotKeyInfra) + sizeof(int64_t) * HOTKEY_WINDOWS_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for slide window", K(ret));
      } else if (OB_FAIL(swhotkey.deep_copy(allocator_, photkey))) {
        LOG_WARN("fail to deep copy hotkey in add slide window", K(ret));
      } else if (OB_FAIL(hotkey_slide_window_map_.set_refactored(photkey, (ObTableSlideWinHotKeyInfra*)buf))) {
        LOG_WARN("fail to set hotkey to slide window", K(ret));
      } else {
        slidewin_infra = new(buf) ObTableSlideWinHotKeyInfra;
        slidewin_infra->cnts = (int64_t*)(buf + sizeof(ObTableSlideWinHotKeyInfra));
        MEMSET(slidewin_infra->cnts, 0, sizeof(int64_t) * HOTKEY_WINDOWS_SIZE);
        slidewin_infra->cnts[OBTableThrottleMgr::epoch_ % HOTKEY_WINDOWS_SIZE] = cnt;
        // set live time
        slidewin_infra->ttl = HOTKEY_WINDOWS_SIZE;
      }
    } else {
      // error
      LOG_WARN("fail to get hotkey from slide window map", K(swhotkey), K(ret));
    }
  } else {
    // exist in slide window map
    slidewin_infra->cnts[OBTableThrottleMgr::epoch_ % HOTKEY_WINDOWS_SIZE] = cnt;
    slidewin_infra->ttl = HOTKEY_WINDOWS_SIZE;
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(slidewin_infra)) {
      allocator_.free(slidewin_infra);
    }
    if (OB_NOT_NULL(photkey)) {
      allocator_.free(photkey);
    }
  }

  return ret;
};

/**
  * reduce all the live time, delete the key if live time is 0
  */
int ObTableTenantHotKey::refresh_slide_window()
{
  int ret = OB_SUCCESS;
  ObTableSlideWindowHotkey *hotkey = nullptr;
  ObTableSlideWinHotKeyInfra *slidewin_infra = nullptr;

  for (ObTableTenantSlideWindowInfraMap::iterator iter = hotkey_slide_window_map_.begin();
       iter != hotkey_slide_window_map_.end() && OB_SUCC(ret); ++iter) {
    hotkey = iter->first;
    slidewin_infra = iter->second;
    // ! whether a hotkey is in heap in priv epoch, if not then set this to zero (since epoch add in timer)
    if (HOTKEY_WINDOWS_SIZE != slidewin_infra->ttl) {
      slidewin_infra->cnts[OBTableThrottleMgr::epoch_ % HOTKEY_WINDOWS_SIZE] = 0;
    }

    if (0 == --(slidewin_infra->ttl)) {
      // live time to 0 then erase
      if (OB_FAIL(hotkey_slide_window_map_.erase_refactored(hotkey))) {
        LOG_WARN("fail to erase hotkey from slide window", KPC(hotkey), KPC(iter->first), K(OBTableThrottleMgr::epoch_), K(ret));
      } else {
        allocator_.free(hotkey);
        allocator_.free(slidewin_infra);
      }
    }
  }

  return ret;
}

/**
  * @param [in]   hotkey hotkey
  * @param [out]  averge_cnt averge count of slide window
  * get the average slide window count
  */
int ObTableTenantHotKey::get_slide_window_cnt(ObTableHotKey *hotkey, double &average_cnt)
{
  int ret = OB_SUCCESS;
  ObTableSlideWindowHotkey swhotkey = *hotkey;
  ObTableSlideWinHotKeyInfra *slidewin_infra = nullptr;

  if (OB_FAIL(hotkey_slide_window_map_.get_refactored(&swhotkey, slidewin_infra))) {
    LOG_WARN("fail to get slide window key from map", KPC(hotkey), K(swhotkey), K(OBTableThrottleMgr::epoch_), K(ret));
  } else if (OB_ISNULL(slidewin_infra)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null slide window infra", K(swhotkey), K(ret));
  } else {
    average_cnt = 0;
    for (int64_t i = 0; i < HOTKEY_WINDOWS_SIZE; ++i) {
      average_cnt += static_cast<double>(slidewin_infra->cnts[i]);
    }
    average_cnt = average_cnt / static_cast<double>(HOTKEY_WINDOWS_SIZE);
  }

  return ret;
}

/**
 * -----------------------------------ObTableHotKeyMgr-----------------------------------
 */

ObTableHotKeyMgr::ObTableHotKeyMgr()
    :hotkey_cache_(),
     tenant_hotkey_map_(),
     arena_allocator_(ObModIds::OB_TABLE_HOTKEY, OB_MALLOC_MIDDLE_BLOCK_SIZE),
     random_()
{
}

int64_t ObTableHotKeyMgr::inited_ = 0;
ObTableHotKeyMgr *ObTableHotKeyMgr::instance_ = NULL;
const ObTableHotKeyCacheValue ObTableHotKeyMgr::hotkey_cache_value_;

// 0,0 -> DYNAMIC_GET -> QUERY  | 0,1 -> DYNAMIC_INSERT -> INSERT
// 0,2 -> DYNAMIC_DEL -> DELETE | 0,3 -> DYNAMIC_UPDATE -> UPDATE
// 0,4 -> DYNAMIC_IOU -> INSERT | 0,5 -> DYNAMIC_REPLACE -> INSERT
// 0,6 -> DYNAMIC_INC -> UPDATE | 0,7 -> DYNAMIC_APPEND -> UPDATE

// 1,0 -> KV_GET -> QUERY  | 1,1 -> KV_INSERT -> INSERT
// 1,2 -> KV_DEL -> DELETE | 1,3 -> KV_UPDATE -> UPDATE
// 1,4 -> KV_IOU -> INSERT | 1,5 -> KV_REPLACE -> INSERT
// 1,6 -> KV_INC -> UPDATE | 1,7 -> KV_APPEND -> UPDATE

// 2,0 -> HKV_GET -> INVALID | 2,1 -> HKV_INSERT -> INVALID
// 2,2 -> HKV_DEL -> DELETE | 2,3 -> HKV_UPDATE -> INVALID
// 2,4 -> HKV_IOU -> INSERT | 2,5 -> HKV_REPLACE -> INVALID
// 2,6 -> HKV_INC -> INVALID | 2,7 -> HKV_APPEND -> INVALID
const HotKeyType ObTableHotKeyMgr::OPTYPE_HKTYPE_MAP[3][8] =
                    // ET_DYNAMIC
                    {{HotKeyType::TABLE_HOTKEY_QUERY, HotKeyType::TABLE_HOTKEY_INSERT,
                      HotKeyType::TABLE_HOTKEY_DELETE, HotKeyType::TABLE_HOTKEY_UPDATE,
                      HotKeyType::TABLE_HOTKEY_INSERT, HotKeyType::TABLE_HOTKEY_INSERT,
                      HotKeyType::TABLE_HOTKEY_UPDATE, HotKeyType::TABLE_HOTKEY_UPDATE},
                    // ET_KV
                    {HotKeyType::TABLE_HOTKEY_QUERY, HotKeyType::TABLE_HOTKEY_INSERT,
                      HotKeyType::TABLE_HOTKEY_DELETE, HotKeyType::TABLE_HOTKEY_UPDATE,
                      HotKeyType::TABLE_HOTKEY_INSERT, HotKeyType::TABLE_HOTKEY_INSERT,
                      HotKeyType::TABLE_HOTKEY_UPDATE, HotKeyType::TABLE_HOTKEY_UPDATE},
                    // ET_HKV
                     {HotKeyType::TABLE_HOTKEY_QUERY, HotKeyType::TABLE_HOTKEY_INVALID,
                      HotKeyType::TABLE_HOTKEY_DELETE, HotKeyType::TABLE_HOTKEY_INVALID,
                      HotKeyType::TABLE_HOTKEY_INSERT, HotKeyType::TABLE_HOTKEY_INVALID,
                      HotKeyType::TABLE_HOTKEY_INVALID, HotKeyType::TABLE_HOTKEY_INVALID}};


int ObTableHotKeyMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hotkey_cache_.init(hotkey_cache_.OB_TABLE_HOTKEY_CACHE, hotkey_cache_.OB_TABLE_HOTKEY_CACHE_PRIORITY))) {
    LOG_WARN("fail to init hotkey_cache_", K(ret));
  } else if (OB_FAIL(tenant_hotkey_map_.create(HOTKEY_TENANT_BUCKET_NUM, ObModIds::OB_TABLE_HOTKEY, ObModIds::OB_TABLE_HOTKEY))) {
    LOG_WARN("Fail to create tenant hotkey map", K(ret));
  } else if (OB_FAIL(tenant_absent_set_.create(HOTKEY_TENANT_BUCKET_NUM, ObModIds::OB_TABLE_HOTKEY, ObModIds::OB_TABLE_HOTKEY))) {
    LOG_WARN("Fail to create tenant alive map", K(ret));
  } else if (OB_FAIL(refresh_tenant_map())) {
    // create all tenant when init
    LOG_WARN("fail to create tenant map when init", K(ret));
  }

  if (OB_FAIL(ret)) {
    hotkey_cache_.destroy();
    tenant_hotkey_map_.destroy();
    tenant_absent_set_.destroy();
  }

  return ret;
}

ObTableHotKeyMgr &ObTableHotKeyMgr::get_instance()
{
  ObTableHotKeyMgr *instance = NULL;
  while(OB_UNLIKELY(inited_ < 2)) {
    if (ATOMIC_BCAS(&inited_, 0, 1)) {
      instance = OB_NEW(ObTableHotKeyMgr, ObModIds::OB_TABLE_HOTKEY);
      if (OB_LIKELY(OB_NOT_NULL(instance))) {
        if (common::OB_SUCCESS != instance->init()) {
          LOG_WARN("failed to init ObTableHotKeyMgr instance");
          OB_DELETE(ObTableHotKeyMgr, ObModIds::OB_TABLE_HOTKEY, instance);
          instance = NULL;
          ATOMIC_BCAS(&inited_, 1, 0);
        } else {
          instance_ = instance;
          (void)ATOMIC_BCAS(&inited_, 1, 2);
        }
      } else {
        (void)ATOMIC_BCAS(&inited_, 1, 0);
      }
    }
  }
  return *(ObTableHotKeyMgr *)instance_;
}

/**
  * clear() will release the memory that used for hotkey
  * warn: Key in HotKeyMap use the same memory for a hotkey
  * clear map first then clear heap and hotkey
  * warning: this will not affect alive set
  * useless now
  */
void ObTableHotKeyMgr::clear()
{
  // traverse each tenant hotkey
  for (ObTableTenantHotKeyMap::iterator iter = tenant_hotkey_map_.begin(); iter != tenant_hotkey_map_.end(); ++iter) {
    iter->second->clear();
  }
}

void ObTableHotKeyMgr::destroy()
{
  // destroy ObTableHotKeyCache
  hotkey_cache_.destroy();

  // destroy ObTableTenantHotKeyMap
  for (ObTableTenantHotKeyMap::iterator iter = tenant_hotkey_map_.begin(); iter != tenant_hotkey_map_.end(); ++iter) {
    if (OB_ISNULL(iter->second)) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected null tenant infra", K(iter->first), K(ret));
    } else {
      iter->second->destroy();
    }
  }
  tenant_hotkey_map_.destroy();

  // destroy ObTableTenantAliveSet
  tenant_absent_set_.destroy();

  OB_DELETE(ObTableHotKeyMgr, "TableHKMgrIns", instance_);
}

/**
  * every epoch need to refresh:
  *   1. refresh slide window
  *   2. clear()
  */
void ObTableHotKeyMgr::refresh()
{
  int ret = OB_SUCCESS;
  ObTableTenantHotKey *tenant_hotkey = nullptr;
  // traverse each tenant hotkey
  for (ObTableTenantHotKeyMap::iterator iter = tenant_hotkey_map_.begin(); iter != tenant_hotkey_map_.end(); ++iter) {
    if (OB_ISNULL(tenant_hotkey = iter->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null tenant infra", K(iter->first), K(ret));
    } else if (OB_FAIL(tenant_hotkey->refresh_slide_window())) {
      LOG_WARN("fail to refresh slide window", K(tenant_hotkey), K(ret));
    }
    tenant_hotkey->infra_[(OBTableThrottleMgr::epoch_ + 1) % ObTableTenantHotKey::TENANT_HOTKEY_INFRA_NUM].clear();
  }
}

/**
  * @param [in]     allocator   help to get memory of target
  * @param [in]     rowkey  rowkey
  * @param [in]     buf     where string rowkey is
  * @param [in]     entity_type TableAPI or HBase
  * @param [out]    target  rowkey.tostring()
  * create rowkey ObString by ObRowkey
  */
int ObTableHotKeyMgr::rowkey_serialize(ObArenaAllocator &allocator, const ObRowkey &rowkey, ObString &target)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = rowkey.get_serialize_size();
  int64_t pos = 0;

  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf for rowkey fail", K(ret));
  } else if (OB_FAIL(rowkey.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", K(buf_len), K(ret));
  } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get buf", K(pos), K(buf_len), K(ret));
  } else {
    target = ObString(buf_len, buf_len, buf);
  }

  return ret;
}

int ObTableHotKeyMgr::transform_hbase_rowkey(ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (!rowkey.is_legal() || rowkey.length() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("illeagl rowkey" , K(ret));
  } else {
    // only need K in hbase
    rowkey.set_length(1);
  }
  return ret;
}

/**
 * @param[in] tenant_id
 * @param[in] table_id
 * @param[in] rowkey_string
 * @param[in] partition_id
 * @param[in] epoch
 * @param[in] type
 * @param[in] cnt
 * @return int
 *
 * @brief
 */
int ObTableHotKeyMgr::set_cnt(uint64_t tenant_id,
                              uint64_t table_id,
                              ObString &rowkey_string,
                              int64_t partition_id,
                              int64_t epoch,
                              HotKeyType type,
                              int64_t cnt)
{
  int ret = OB_SUCCESS;
  // construct hotkey_cache_key
  ObTableHotKeyCacheKey hotkey_cache_key(tenant_id, table_id, partition_id, epoch, type, rowkey_string);

  // here we add ALL and operation two type into kvcache
  if (OB_FAIL(set_kvcache_cnt(hotkey_cache_key, cnt))) {
    // maintain kvcache and hotkey map&heap for the operation type
    LOG_WARN("fail to maintain kvcache and hotkey map&heap", K(ret));
  } else {
    hotkey_cache_key.type_ = HotKeyType::TABLE_HOTKEY_ALL;
    if (OB_FAIL(set_kvcache_cnt(hotkey_cache_key, cnt))) {
      // maintain kvcache and hotkey map&heap for the ALL type
      LOG_WARN("fail to maintain kvcache and hotkey map&heap", K(ret));
    }
  }
  return ret;
}

/**
  * @param [in]     infra infra of tenant hotkey
  * @param [in]     heap_min which pair to add hotkey
  * @param [in]     hotkey_pair hotkey pair that insert into heap
  * @param [out]    pop_hotkey_pair hotkey pair that need to pop from heap
  * @param [in]     get_cnt get_cnt of input hot key pair
  * after get the rwlock of heap, update heap by latest value of hotkey pair
  */
int ObTableHotKeyMgr::update_heap(ObTableTenantHotKey::ObTableTenantHotKeyInfra &infra, ObTableHotKeyValue &heap_min, ObTableHotkeyPair *&hotkey_pair, ObTableHotkeyPair *&pop_hotkey_pair, int64_t get_cnt)
{
  int ret = OB_SUCCESS;
  bool first_map_insert = true;

  while (heap_min < hotkey_pair->second && OB_SUCC(ret)) {
    if (OB_FAIL(infra.hotkey_heap_.pop(pop_hotkey_pair, heap_min))) {
      LOG_WARN("fail to pop hotkey heap", KPC(pop_hotkey_pair->first), K(ret));
    } else {
      // insert new hotkey
      if (OB_FAIL(infra.hotkey_heap_.push(*hotkey_pair))) {
        LOG_WARN("fail to set hotkey into hotkey heap", KPC(hotkey_pair->first), K(ret));
      } else if (first_map_insert && OB_FAIL(infra.hotkey_map_.set_refactored(hotkey_pair->first, get_cnt))) {
        LOG_WARN("fail to set hotkey into hotkey map", KPC(hotkey_pair->first), K(ret));
      }
      first_map_insert = false;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(infra.hotkey_map_.get_refactored(pop_hotkey_pair->first, pop_hotkey_pair->second))) {
        LOG_WARN("fail to get latest cnt of pop hot key", KPC(pop_hotkey_pair->first), K(ret));
      } else if (OB_FAIL(infra.get_heap_min(heap_min))) {
        LOG_WARN("fail to get top of hotkey heap", K(ret));
      } else {
        // refresh pop hotkey then try to insert back to heap if its cnt > heapmin
        hotkey_pair = pop_hotkey_pair;
      }
    }
  }

  return ret;
}

/**
  * @param [in]     hotkey which pair to add hotkey
  * @param [in]     get_cnt get_cnt of input hot key pair
  * try to add a new key into tenant's topk map and heap
  *   if need to insert then deep copy in generate_hotkey_pair
  */
int ObTableHotKeyMgr::try_add_hotkey(ObTableHotKey &hotkey, int64_t get_cnt)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table hotkey manager have not inited", K(ret));
  } else if (hotkey.epoch_ != OBTableThrottleMgr::epoch_) {
    // hotkey is expired
  } else {
    ObTableTenantHotKey *tenant_hotkey = nullptr;
    uint64_t infra_idx = hotkey.epoch_ % ObTableTenantHotKey::TENANT_HOTKEY_INFRA_NUM; // the same as hotkey

    if (OB_FAIL(tenant_hotkey_map_.get_refactored(hotkey.tenant_id_, tenant_hotkey))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // Do nothing (tenant will added into map by timer
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get tenant id from map", K(ret));
      }
    } else {
      ObTableTenantHotKey::ObTableTenantHotKeyInfra &infra = tenant_hotkey->infra_[infra_idx];
      ObTableHotKeyValue hotkey_value = 0;

      if (OB_FAIL(infra.hotkey_map_.get_refactored(&hotkey, hotkey_value))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // hotkey not in map
          ret = OB_SUCCESS;
          ObTableHotkeyPair *hotkey_pair = nullptr;

          if (infra.get_heap_cnt() == ObTableTenantHotKey::ObTableTenantHotKeyInfra::MAX_HOT_KEY_SIZE) {
            // hotkey heap is full
            ObTableHotKeyValue heap_min;
            if (OB_FAIL(infra.get_heap_min(heap_min))) {
              LOG_WARN("fail to get top of hotkey heap", K(ret));
            } else if (heap_min < get_cnt) {
              // heap need to adjust
              DRWLock::WRLockGuard guard(infra.rwlock_);
              if (hotkey.epoch_ != OBTableThrottleMgr::epoch_) {
                // hotkey is expired
              } else {
                if (OB_NOT_NULL(infra.hotkey_map_.get(&hotkey))) {
                  // hot key is in map right now, hotkey already in map
                  ObTableHotKeyMapCallBack updater(get_cnt);
                  if (OB_FAIL(infra.hotkey_map_.atomic_refactored(&hotkey, updater))) {
                    LOG_WARN("fail to set cnt to tenant hotkey map", K(hotkey.tenant_id_), K(ret));
                  }
                } else if (OB_FAIL(infra.get_heap_min(heap_min))) {
                  // first get the latest value top heap_min
                  LOG_WARN("fail to get top of hotkey heap", K(ret));
                } else if (heap_min < get_cnt) {
                  // heap_min is truely less than current value now
                  ObTableHotkeyPair *pop_hotkey_pair = nullptr;

                  // create pair for new hotkey first than update heap
                  if (OB_FAIL(tenant_hotkey->generate_hotkey_pair(hotkey_pair, hotkey, get_cnt))) {
                    LOG_WARN("fail to deep copy hotkey pair", K(ret));
                  } else if (OB_FAIL(update_heap(infra, heap_min, hotkey_pair, pop_hotkey_pair, get_cnt))) {
                    LOG_WARN("fail to update heap", KPC(hotkey_pair->first), K(ret));
                  }

                  if (OB_SUCC(ret)) {
                    // free pop hotkey
                    if (OB_FAIL(infra.hotkey_map_.erase_refactored(pop_hotkey_pair->first))) {
                      // delete from map
                      LOG_ERROR("fail to erase hotkey from map", K(ret), KPC(pop_hotkey_pair->first));
                    } else {
                      // free memory of hotkey
                      tenant_hotkey->allocator_.free(pop_hotkey_pair->first);
                      tenant_hotkey->allocator_.free(pop_hotkey_pair);
                    }
                  }
                }
              }
            }
          } else {
            // hotkey heap is not full then insert
            DRWLock::WRLockGuard guard(infra.rwlock_);
            if (hotkey.epoch_ == OBTableThrottleMgr::epoch_ &&
                infra.hotkey_heap_.get_count() < ObTableTenantHotKey::ObTableTenantHotKeyInfra::MAX_HOT_KEY_SIZE) {
              // heap count is truely less than max hotkey size, if not then do nothing
              if (OB_FAIL(tenant_hotkey->generate_hotkey_pair(hotkey_pair, hotkey, get_cnt))) {
                LOG_WARN("fail to deep copy hotkey pair", K(ret));
              } else {
                if (OB_FAIL(infra.hotkey_map_.set_refactored(hotkey_pair->first, get_cnt))) {
                  if (OB_HASH_EXIST == ret) {
                    // hotkey is alreaday in map, insert by other
                    ret = OB_SUCCESS;
                    tenant_hotkey->allocator_.free(hotkey_pair->first);
                    tenant_hotkey->allocator_.free(hotkey_pair);
                  } else {
                    LOG_WARN("fail to insert hotkey into not full map and heap", KPC(tenant_hotkey), KPC(hotkey_pair->first), K(ret));
                  }
                } else if (OB_FAIL(infra.hotkey_heap_.push(*hotkey_pair))) {
                  LOG_WARN("fail to set hotkey into hotkey heap", KPC(tenant_hotkey), KPC(hotkey_pair->first), K(ret));
                }
              }
            }
          }
        } else {
          // fail to infra.hotkey_map_.get_refactored(&hotkey, hotkey_value)
          LOG_WARN("fail to get hotkey from map", K(ret));
        }
      } else {
        // hotkey already in map then update
        ObTableHotKeyMapCallBack updater(get_cnt);
        if (OB_FAIL(infra.hotkey_map_.atomic_refactored(&hotkey, updater))) {
          if (OB_HASH_NOT_EXIST == ret) {
            // other thread has delete this hotkey, then dont insert or update this key into map heap and wait for next insert
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to set cnt to tenant hotkey map", K(hotkey.tenant_id_), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/**
  * refresh new tenant map
  * use tenant_hotkey_map_ to trace unexist tenant and delete them in the end
  * create tenant hotkey when tenant id is not in tenant hotkey map
  */
int ObTableHotKeyMgr::refresh_tenant_map()
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tmp_tenant_ids;
  ObTableTenantHotKey *tenant_hotkey = nullptr;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table hotkey manager have not inited", K(ret));
  } else if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tmp_tenant_ids))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else {
    // init tenant_absent_set_ (get all current tenant in tenant hotkey map)
    for (ObTableTenantHotKeyMap::iterator iter = tenant_hotkey_map_.begin(); OB_SUCC(ret) && iter != tenant_hotkey_map_.end(); ++iter) {
      if (OB_FAIL(tenant_absent_set_.set_refactored(iter->first))) {
        LOG_WARN("fail to init tenant alive set", K(ret));
      }
    }

    // erase exist tenant in tenant set and create new tenant hotkey
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tmp_tenant_ids.at(i);
        if (OB_FAIL(tenant_hotkey_map_.get_refactored(tenant_id, tenant_hotkey))) {
          if (OB_HASH_NOT_EXIST == ret) {
            // new tenant
            if (OB_FAIL(add_new_tenant(tenant_id))) {
              LOG_WARN("fail to create new tenant for hotkey manager", K(tenant_id), K(ret));
            }
          } else {
            LOG_WARN("fail to get tenant from tenant hotkey map", K(tenant_id), K(ret));
          }
        } else {
          if (OB_FAIL(tenant_absent_set_.erase_refactored(tenant_id))) {
            LOG_WARN("fail to erase tenantid in tenant alive set", K(ret));
          }
        }
      }
    }

    // erase absent tenant in tenant hotkey map
    for (ObTableTenantAliveSet::iterator iter = tenant_absent_set_.begin();
         OB_SUCC(ret) && iter != tenant_absent_set_.end(); ++iter) {
      if (OB_FAIL(tenant_hotkey_map_.erase_refactored(iter->first, &tenant_hotkey))) {
        LOG_WARN("fail to erase tenant hotkey when destroy", K(ret));
      } else {
        tenant_hotkey->destroy();
        arena_allocator_.free(tenant_hotkey);
      }
    }

    // clear tenant set
    tenant_absent_set_.clear();
  }
  return ret;
}

/**
  * @param [in]   tenant_id tenant that want to add to tenant map
  * add new tenant hotkey struct into tenant map
  */
int ObTableHotKeyMgr::add_new_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  ObTableTenantHotKey *tenant_hotkey = nullptr;

  if (OB_UNLIKELY(OB_ISNULL(buf = static_cast<char*>(arena_allocator_.alloc(sizeof(ObTableTenantHotKey)))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory, ", K(sizeof(ObTableTenantHotKey)), K(tenant_id), K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(tenant_hotkey = new (buf) ObTableTenantHotKey(tenant_id)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new a tenant hotkey", K(ret), K(buf), K(tenant_id));
  } else if (OB_FAIL(tenant_hotkey->init())) {
    LOG_WARN("fail to init tenant hotkey", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_hotkey_map_.set_refactored(tenant_id, tenant_hotkey))) {
    LOG_WARN("fail to set new tenant in tenant hotkey map", K(tenant_id), K(ret));
  } else {
    // to set hold_size, first create cache by kvcache get
    ObString str0(15, 15, "magic_start_251");
    ObString str1(15, 15, "magic_start_927");
    ObObj obj_array_[2];
    obj_array_[0].set_varchar(str0);
    obj_array_[0].set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    obj_array_[1].set_varchar(str1);
    obj_array_[1].set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    const int buf_size = 1024;
    char buf[buf_size];
    int64_t pos = 0;
    ObString tmp;
    ObRowkey start_rowkey(obj_array_, 2);
    if (OB_FAIL(start_rowkey.serialize(buf, buf_size, pos))) {
      LOG_WARN("fail to serialize start rowkey", K(ret));
    } else {
      tmp = ObString(pos, pos, buf);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_cnt(tenant_id, 0, tmp, 0, 0, HotKeyType::TABLE_HOTKEY_ALL, 1))) {
        LOG_WARN("fail to set cnt to init cache", K(ret), K(tenant_id));
        tenant_hotkey->destroy();
      } else if (OB_FAIL(ObKVGlobalCache::get_instance().set_hold_size(tenant_id, "TableHKCache", OB_TABLE_CACHE_HOLD_SIZE))) {
        LOG_WARN("fail to set hold size of kvcache", K(ret), K(tenant_id));
        tenant_hotkey->destroy();
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf)) {
      arena_allocator_.free(buf);
    }
    if (OB_NOT_NULL(tenant_hotkey)) {
      tenant_hotkey->destroy();
    }
  }

  return ret;
}

/**
  * @param [in]   hotkey cache hotkey
  * @param [in]   set_cnt cnt that want to set into kvcache
  * update kvcache & tenant's map&heap by rowkey type
  */
int ObTableHotKeyMgr::set_kvcache_cnt(ObTableHotKey &hotkey, int64_t set_cnt)
{
  int ret = OB_SUCCESS;
  int64_t get_cnt = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table hotkey manager have not inited", K(ret));
  } else if (OB_FAIL(get_hotkey_cache().get(hotkey, hotkey_cache_value_, set_cnt, get_cnt))) {
    LOG_WARN("fail to try add hotkey into hotkey cache", K(ret));
  } else if (OB_FAIL(try_add_hotkey(hotkey, get_cnt))) {
    // try to insert hotkey and will deep copy hotkey
    LOG_WARN("fail to try add hotkey into map and heap", K(ret));
  }
  return ret;
}

/**
  * @param [in]     src_hotkey source hotkey
  * @param [in]     src_cnt source count of hotkey
  * @param [out]    dest_pair copy to
  * deep copy hot key from hotkey and cnt and generate hotkey pair for map
  */
int ObTableTenantHotKey::generate_hotkey_pair(ObTableHotkeyPair *&dest_pair, ObTableHotKey &src_hotkey, int64_t src_cnt)
{
  int ret = OB_SUCCESS;

  void *buf = nullptr;
  if (OB_UNLIKELY(0 == src_hotkey.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to deep copy hotkey, size of rowkey should greater than 0", K(src_hotkey), K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableHotkeyPair))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to get memeory of Hotkey Pair", K(ret));
  } else {
    dest_pair = new(buf) ObTableHotkeyPair;
    if (OB_FAIL(src_hotkey.deep_copy(allocator_, dest_pair->first))) {
      LOG_WARN("fail to get memeory of Hotkey Pair", K(ret));
    } else {
      dest_pair->second = src_cnt;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf)) {
      allocator_.free(buf);
    }
    if (OB_NOT_NULL(dest_pair)) {
      dest_pair = nullptr;
    }
  }

  return ret;
}

/**
 * @return bool
 *
 * @brief check whether need to count this rowkey, we only count KV_HOTKEY_STAT_RATIO of req
 */
bool ObTableHotKeyMgr::check_need_ignore()
{
  // return true means dont count
  return random_.get(0, 1000000) > KV_HOTKEY_STAT_RATIO * 1000000.0;
}

/**
 * @param[in] allocator
 * @param[in] rowkey
 * @param[in] entity_type
 * @param[in] tenant_id
 * @param[in] table_id
 * @param[in] partition_id
 * @return int
 *
 * @brief set normal req hotkey status, ! partition_id may be wrong since we simply use id from arg_
 */
int ObTableHotKeyMgr::set_req_stat(ObArenaAllocator &allocator, const ObRowkey rowkey,
                                   ObTableOperationType::Type optype, uint64_t tenant_id,
                                   uint64_t table_id, int64_t partition_id)
{
  int ret = OB_SUCCESS;
  HotKeyType type = OPTYPE_HKTYPE_MAP[static_cast<int>(ObTableEntityType::ET_KV)][optype];
  ObString rowkey_string;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;

  if (throttle_threshold <= 0){
    // do nothing
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table hotkey manager have not inited", K(ret));
  } else if (HotKeyType::TABLE_HOTKEY_INVALID == type) {
    LOG_WARN("fail to get the process tpye of operation", K(type), K(ret));
  } else if (check_need_ignore()) {
    // do nothing
    // this key will not be count since we only count KV_HOTKEY_STAT_RATIO of input
  } else if (OB_FAIL(ObTableUtils::set_rowkey_collation(const_cast<ObRowkey&>(rowkey)))) {
      LOG_WARN("fail to set rowkey collation", K(rowkey), K(ret));
  } else if (OB_FAIL(rowkey_serialize(allocator, rowkey, rowkey_string))) {
    LOG_WARN("fail to get string of rowkey", K(ret), K(rowkey));
  } else if (OB_FAIL(set_cnt(tenant_id,
                             table_id,
                             rowkey_string,
                             partition_id,
                             OBTableThrottleMgr::get_instance().epoch_,
                             type,
                             1)))
  {
    // all operation affect 1 row in this function
    LOG_WARN("fail to set cnt to tenant hotkey map and cache", K(rowkey), K(ret));
  }

  // free temp rowkey string memory
  if (OB_NOT_NULL(rowkey_string.ptr())) {
    allocator.free(rowkey_string.ptr());
  }

  return ret;
}

int ObTableHotKeyMgr::set_batch_req_stat(ObArenaAllocator &allocator, const ObTableBatchOperation &batch_operation,
                                         ObTableBatchOperationResult &result, ObTableEntityType entity_type,
                                         uint64_t tenant_id, uint64_t table_id, int64_t partition_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;

  if (throttle_threshold <= 0){
    // do nothing
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table hotkey manager have not inited", K(ret));
  } else if (result.count() != batch_operation.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("number of result does not match the number of input operations", K(result.count()), K(batch_operation.count()));
  } else {
    ObString rowkey_string;

    for (int64_t i = 0; OB_SUCCESS == ret && i < batch_operation.count(); ++i) {
      if (check_need_ignore()) {
        // do nothing, this key will not be count since we only count KV_HOTKEY_STAT_RATIO of input
        continue;
      }
      ObRowkey rowkey = const_cast<ObITableEntity&>(batch_operation.at(0).entity()).get_rowkey();
      HotKeyType type = OPTYPE_HKTYPE_MAP[static_cast<int>(entity_type)][result.at(i).type()];
      int64_t affected_rows = result.at(i).get_affected_rows() > 0 ? result.at(i).get_affected_rows() : 1;
      if (HotKeyType::TABLE_HOTKEY_INVALID == type) {
        // invalid type will not affect the ret
        LOG_WARN("invalid type of hotkey", K(rowkey), K(entity_type), K(result.at(i).type()), K(ret));
      } else if (ObTableEntityType::ET_HKV == entity_type && OB_FAIL(transform_hbase_rowkey(rowkey))) {
        LOG_WARN("fail to transform hbase hotkey", K(rowkey), K(ret));
      } else if (OB_FAIL(ObTableUtils::set_rowkey_collation(rowkey))) {
        LOG_WARN("fail to set rowkey collation", K(rowkey), K(ret));
      } else if (OB_FAIL(rowkey_serialize(allocator, rowkey, rowkey_string))) {
        LOG_WARN("fail to get string of rowkey", K(ret), K(rowkey));
      } else if (OB_FAIL(set_cnt(tenant_id, table_id, rowkey_string, partition_id,
                                 OBTableThrottleMgr::get_instance().epoch_, type, affected_rows)))
      {
        LOG_WARN("fail to set cnt to tenant hotkey map and cache", K(rowkey), K(ret));
      }

      if (OB_NOT_NULL(rowkey_string.ptr())) {
        allocator.free(rowkey_string.ptr());
      }
    }
  }

  return ret;
}

int ObTableHotKeyMgr::set_query_req_stat(ObArenaAllocator &allocator, const ObTableQuery &query,
                                         ObTableEntityType entity_type, uint64_t tenant_id,
                                         uint64_t table_id, int64_t partition_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_query_req_stat_common(allocator, query, entity_type, HotKeyType::TABLE_HOTKEY_QUERY,
                                        tenant_id, table_id, partition_id, 1))) {
    LOG_WARN("fail to set hotkey stat", K(ret), K(query));
  }
  return ret;
}

int ObTableHotKeyMgr::set_query_req_stat_common(ObArenaAllocator &allocator, const ObTableQuery &query,
                                                ObTableEntityType entity_type, HotKeyType hotkey_type,
                                                uint64_t tenant_id, uint64_t table_id, int64_t partition_id, int64_t get_cnt)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;

  if (throttle_threshold <= 0){
    // do nothing
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table hotkey manager have not inited", K(ret));
  } else if (query.get_scan_ranges().count() != 1) {
    // do nothing, only deal with start_key == end_key
  } else if (get_cnt <= 0) {
    if (get_cnt < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cnt of query should greater than 0", K(get_cnt), K(query), K(ret));
    } else {
      // do nothing
    }
  } else {
    ObRowkey start_key = query.get_scan_ranges().at(0).start_key_;
    ObRowkey end_key = query.get_scan_ranges().at(0).end_key_;

    if (ObTableEntityType::ET_HKV == entity_type &&
        (OB_FAIL(transform_hbase_rowkey(start_key)) || OB_FAIL(transform_hbase_rowkey(end_key)))) {
      LOG_WARN("fail to transform hbase hotkey", K(ret), K(start_key), K(end_key));
    } else if (start_key != end_key) {
      // do nothing, only deal with start_key == end_key
    } else if (check_need_ignore()) {
      // do nothing, since we only count KV_HOTKEY_STAT_RATIO of input
    } else if (OB_FAIL(ObTableUtils::set_rowkey_collation(start_key))) {
      LOG_WARN("fail to set rowkey collation", K(start_key), K(ret));
    } else {
      ObString rowkey_string;
      if (OB_FAIL(rowkey_serialize(allocator, start_key, rowkey_string))) {
        LOG_WARN("fail to get string of rowkey", K(ret), K(start_key));
      } else if (OB_FAIL(set_cnt(tenant_id, table_id, rowkey_string, partition_id,
                                 OBTableThrottleMgr::get_instance().epoch_, hotkey_type, get_cnt))) {
        LOG_WARN("fail to set cnt to tenant hotkey map and cache", K(start_key), K(ret));
      }

      // free temp rowkey string memory
      if (OB_NOT_NULL(rowkey_string.ptr())) {
        allocator.free(rowkey_string.ptr());
      }
    }
  }

  return ret;
}

} /* namespace table */
} /* namespace oceanbase */