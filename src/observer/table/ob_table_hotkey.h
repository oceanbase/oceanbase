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
#ifndef _OB_TABLE_HOTKEY_H
#define _OB_TABLE_HOTKEY_H

#include "ob_table_rpc_processor_util.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "ob_table_hotkey_kvcache.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace table
{

typedef ObTableHotKeyCacheKey ObTableHotKey;
typedef int64_t ObTableHotKeyValue;

// store map ObTableHotKey* -> ObTableHotKeyValue (cnt of hotkey in heap/map)
typedef hash::ObHashMap<ObTableHotKey *, ObTableHotKeyValue> ObTableHotKeyMap;

// hotkey and hotkey's count
typedef std::pair<ObTableHotKey *, ObTableHotKeyValue> ObTableHotkeyPair;


struct ObTableSlideWinHotKeyInfra
{
  int64_t* cnts;  // store previous hotkey
  int64_t ttl;    // time to live
};

// store map slidewindowkey -> Infra
typedef hash::ObHashMap<ObTableSlideWindowHotkey*, ObTableSlideWinHotKeyInfra*> ObTableTenantSlideWindowInfraMap;

struct ObTableHotKeyHeap
{
public:
  ObTableHotKeyHeap();
  int init(common::ObArenaAllocator &allocator, const int64_t &heap_max_size);
  void destroy();
  void clear();
  void set_allocator(ObFIFOAllocator &allocator) { allocator_ = &allocator; }
  int push(ObTableHotkeyPair &hotkeypair);
  int pop(ObTableHotkeyPair *&hotkeypair, ObTableHotKeyValue &get_cnt);
  int top(ObTableHotkeyPair *&hotkeypair);
  int64_t get_heap_size() {return heap_size_;}
  int64_t get_count() {return count_;}
  ObTableHotkeyPair **get_heap() {return heap_; }

  TO_STRING_KV(K(is_inited_), K(heap_size_), KP(heap_));
private:
  struct HotkeyCompareFunctor {  bool operator() (const ObTableHotkeyPair *a, const ObTableHotkeyPair *b) const; };
  bool is_inited_;
  int64_t heap_size_;
  int64_t count_;
  ObTableHotkeyPair **heap_;
  HotkeyCompareFunctor cmp_;
  // both point to the allocator in ObTableTenantHotKey
  ObFIFOAllocator *allocator_;
  ObArenaAllocator *arena_allocator_;
};

class ObTableHotKeyMapCallBack
{
public:
  ObTableHotKeyMapCallBack(ObTableHotKeyValue value) : v_(value) {};
  void operator()(common::hash::HashMapPair<ObTableHotKey *, ObTableHotKeyValue>& v)
  {
    v.second = v_;
  };
private:
  ObTableHotKeyValue v_;
};

// store TopK heap and hotkeymap (accelerate) for a tenant
struct ObTableTenantHotKey
{
  // Key from hotkey map and heap use the same memory
  // use two heap and map to avoid using mutex when insert hotkey into throttle mgr
  struct ObTableTenantHotKeyInfra{
    static const int64_t MAX_HOT_KEY_SIZE = 10;
    ObTableHotKeyMap hotkey_map_;
    ObTableHotKeyHeap hotkey_heap_;
    common::DRWLock rwlock_;
    bool is_inited_;

    ObTableTenantHotKeyInfra()
      :hotkey_map_(),
       hotkey_heap_(),
       is_inited_(false) {}
    int init(ObArenaAllocator &arena_allocator, uint64_t tenant_id);
    void clear();
    void destroy();
    int get_heap_min(ObTableHotKeyValue &heap_min);
    int64_t get_heap_cnt();
  };
  static const int64_t HOTKEY_WINDOWS_SIZE = 4; // must > 1
  static const int64_t TENANT_HOTKEY_INFRA_NUM = 3;

  ObTableTenantHotKey(uint64_t tenant_id)
    :tenant_id_(tenant_id),
     allocator_(tenant_id),
     arena_allocator_(ObModIds::OB_TABLE_HOTKEY, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id) {}
  int init();
  void clear();
  void destroy();
  // generate_hotkey_pair will deep copy src_hotkey for heap / map by using memory from tenant's allocator_
  int generate_hotkey_pair(ObTableHotkeyPair *&dest_pair, ObTableHotKey &src_hotkey, int64_t src_cnt);
  int add_hotkey_slide_window(ObTableHotKey &hotkey, int64_t cnt);
  int refresh_slide_window();
  int get_slide_window_cnt(ObTableHotKey *hotkey, double &average_cnt);
  TO_STRING_KV(K(tenant_id_));

  uint64_t tenant_id_;
  ObTableTenantHotKeyInfra infra_[TENANT_HOTKEY_INFRA_NUM];

  // Key in slide window will deep copy hotkey from heap
  ObTableTenantSlideWindowInfraMap hotkey_slide_window_map_;
  ObFIFOAllocator allocator_;
  ObArenaAllocator arena_allocator_;
};

// store map tenant_id -> ObTableTenantHotKey
typedef hash::ObHashMap<uint64_t, ObTableTenantHotKey *> ObTableTenantHotKeyMap;
// To detect whether a tenant is still alive
typedef hash::ObHashSet<uint64_t> ObTableTenantAliveSet;

class ObTableHotKeyMgr
{
public:
  static const int64_t HOTKEY_TENANT_BUCKET_NUM = 50;
  static const int64_t OB_TABLE_CACHE_HOLD_SIZE = 24 * 1024 * 1024L; // 24M
  // only KV_HOTKEY_STAT_RATIO of input will be recorded
  // so the recorded number should be multi by 1 / KV_HOTKEY_STAT_RATIO (or decrease the threshold)
  // should be the same as ObTableHotKeyThrottle::KV_HOTKEY_STAT_RATIO
  static constexpr double KV_HOTKEY_STAT_RATIO = 0.2;
  // map of ObTableOperationType::type to HotKeyType, 3 -> 3 kinds of ObTableEntityType, 8 -> 8 kinds of OperationType
  static const HotKeyType OPTYPE_HKTYPE_MAP[3][8];

  int init();
  static ObTableHotKeyMgr &get_instance();
  void clear();
  void refresh();
  void destroy();
  ObTableTenantHotKeyMap &get_tenant_map() {return tenant_hotkey_map_; }
  ObTableHotKeyCache &get_hotkey_cache() {return hotkey_cache_; }
  common::ObArenaAllocator &get_arena_allocator(){return arena_allocator_; }
  // create rowkey ObString by ObRowkey
  int rowkey_serialize(ObArenaAllocator &allocator, const ObRowkey &rowkey, ObString &target);
  int transform_hbase_rowkey(ObRowkey &rowkey);
  int set_cnt(uint64_t tenant_id,
              uint64_t table_id,
              ObString &rowkey_string,
              int64_t partition_id,
              int64_t epoch,
              HotKeyType type,
              int64_t cnt);
  int update_heap(ObTableTenantHotKey::ObTableTenantHotKeyInfra &infra, ObTableHotKeyValue &heap_min, ObTableHotkeyPair *&hotkey_pair, ObTableHotkeyPair *&pop_hotkey_pair, int64_t get_cnt);
  int try_add_hotkey(ObTableHotKey &hotkey, const int64_t get_cnt);
  int refresh_tenant_map();
  int add_new_tenant(uint64_t tenant_id);
  int set_kvcache_cnt(ObTableHotKey &hotkey, int64_t get_cnt);
  bool check_need_ignore();
  int set_req_stat(ObArenaAllocator &allocator, const ObRowkey rowkey,
                   ObTableOperationType::Type optype, uint64_t tenant_id,
                   uint64_t table_id, int64_t partition_id);
  int set_batch_req_stat(ObArenaAllocator &allocator, const ObTableBatchOperation &batch_operation,
                         ObTableBatchOperationResult &result, ObTableEntityType entity_type,
                         uint64_t tenant_id, uint64_t table_id, int64_t partition_id);
  int set_query_req_stat(ObArenaAllocator &allocator, const ObTableQuery &query,
                         ObTableEntityType entity_type, uint64_t tenant_id,
                         uint64_t table_id, int64_t partition_id);
  int set_query_req_stat_common(ObArenaAllocator &allocator, const ObTableQuery &query,
                                  ObTableEntityType entity_type, HotKeyType hotkey_type,
                                  uint64_t tenant_id, uint64_t table_id, int64_t partition_id, int64_t get_cnt);
private:
  ObTableHotKeyMgr();
  virtual ~ObTableHotKeyMgr() {}

  static int64_t inited_;
  static ObTableHotKeyMgr *instance_;
  // (useless) for hotkey_cache put
  static const ObTableHotKeyCacheValue hotkey_cache_value_;

  ObTableHotKeyCache hotkey_cache_;
  ObTableTenantHotKeyMap tenant_hotkey_map_;
  ObTableTenantAliveSet tenant_absent_set_;
  common::ObArenaAllocator arena_allocator_;
  ObRandom random_;
};

} /* namespace table */
} /* namespace oceanbase */

#endif /* _OB_TABLE_HOTKEY_H */