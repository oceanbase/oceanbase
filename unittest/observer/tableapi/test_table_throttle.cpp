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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public

#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server.h"
#include "observer/table/ob_table_throttle.h"
#include "observer/table/ob_table_throttle_manager.h"
#include "observer/table/ob_table_hotkey.h"
#include "observer/table/ob_table_hotkey_kvcache.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

namespace oceanbase {
namespace observer {

class TestTableThrottle : public::testing::Test
{
public:
  TestTableThrottle();
  virtual ~TestTableThrottle() {}
  virtual void SetUp();
  virtual void TearDown();
private:
  void init();
protected:
  ObArenaAllocator arena_allocator_;
  ObFIFOAllocator allocator_;
};

class TestTableApiHotkey : public::testing::Test
{
public:
  TestTableApiHotkey();
  virtual ~TestTableApiHotkey() {}
  virtual void SetUp();
  virtual void TearDown();
private:
  void init();
protected:
  ObArenaAllocator arena_allocator_;
  ObFIFOAllocator allocator_;
};


/**
 * -----------------------------------TestTableThrottle-----------------------------------
 */

TestTableThrottle::TestTableThrottle() : arena_allocator_(ObModIds::TEST, OB_MALLOC_MIDDLE_BLOCK_SIZE)
{
}

void TestTableThrottle::SetUp()
{
  init();
  ObKVGlobalCache::get_instance().init();
}

void TestTableThrottle::init()
{
  ASSERT_EQ(OB_SUCCESS, allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE));
  allocator_.set_label(ObModIds::TEST);
}

void TestTableThrottle::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

/**
 * -----------------------------------TestTableApiHotkey-----------------------------------
 */

TestTableApiHotkey::TestTableApiHotkey() : arena_allocator_(ObModIds::TEST, OB_MALLOC_MIDDLE_BLOCK_SIZE)
{
}

void TestTableApiHotkey::SetUp()
{
  init();
  ObKVGlobalCache::get_instance().init();
}

void TestTableApiHotkey::init()
{
  ASSERT_EQ(OB_SUCCESS, allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE));
  allocator_.set_label(ObModIds::TEST);
}

void TestTableApiHotkey::TearDown() {
  ObKVGlobalCache::get_instance().destroy();
}

/**
 * -----------------------------------Test TableApiThrottle-----------------------------------
 */

TEST_F(TestTableThrottle, ObTableThrottleKey)
{
  ObTableThrottleKey key;
  key.key_ = ObString(22, 22, "test_tableapi_hotkey_0");
  ObTableThrottleKey *pkey = nullptr;
  ASSERT_EQ(OB_SUCCESS, key.deep_copy(allocator_, pkey));
  ASSERT_EQ(true, key == *pkey);
  ASSERT_EQ(key.hash(), pkey->hash());
  pkey->key_.ptr()[0] = 'T';
  ASSERT_NE(true, key == *pkey);
  ASSERT_NE(key.hash(), pkey->hash());
  allocator_.free(pkey);
}

TEST_F(TestTableThrottle, ObTableHotKeyThrottle)
{
  ObTableHotKeyThrottle &hotkey_throttle = ObTableHotKeyThrottle::get_instance();
  uint64_t epoch = 0;
  int64_t throttle = 0;

  // init hotkey manager
  ObTableHotKeyMgr hotkey_mgr;
  hotkey_mgr.inited_ = 2;
  hotkey_mgr.instance_ = &hotkey_mgr;
  ASSERT_EQ(OB_SUCCESS, hotkey_mgr.hotkey_cache_.init("TableHKCache", hotkey_mgr.hotkey_cache_.OB_TABLE_HOTKEY_CACHE_PRIORITY));
  ASSERT_EQ(OB_SUCCESS, hotkey_mgr.tenant_hotkey_map_.create(hotkey_mgr.HOTKEY_TENANT_BUCKET_NUM, ObModIds::TEST, ObModIds::TEST));
  ASSERT_EQ(OB_SUCCESS, hotkey_mgr.add_new_tenant(1000));
  ASSERT_EQ(OB_SUCCESS, hotkey_mgr.add_new_tenant(2000));

  // create hotkey
  char *rk_buf;
  char *tr_buf;
  ASSERT_NE(nullptr, rk_buf = static_cast<char*>(allocator_.alloc(sizeof(char) * 24 * 50)));
  ObString rk_strings[50];
  for (int i = 0; i < 5; ++i) {
    for (int j = 0; j < 10; ++j) {
      int pos = 10 * i + j;
      snprintf(&rk_buf[pos * 24], 24, "Test_Tableapi_Hotkey_%c%c", '0'+i, '0'+j);
      rk_strings[pos] = ObString(23, 23, &rk_buf[pos * 24]);
    }
  }
  ASSERT_NE(nullptr, tr_buf = static_cast<char*>(allocator_.alloc(sizeof(ObTableThrottleKey) * 50)));

  ObTableThrottleKey  *throttle_keys = new(tr_buf) ObTableThrottleKey[50];
  for (int i = 0; i < 50; ++i) {
    ObTableHotKey hotkey;
    if (i < 25) {
      hotkey.tenant_id_ = 1000;
    } else {
      hotkey.tenant_id_ = 2000;
    }
    hotkey.type_ = HotKeyType::TABLE_HOTKEY_INSERT;
    hotkey.rowkey_ = rk_strings[i];

    ASSERT_EQ(OB_SUCCESS, hotkey_throttle.create_throttle_hotkey(hotkey, throttle_keys[i]));
  }

  {
    // try insert
    for (int j = 0; j < 10; ++j) {
      ASSERT_EQ(OB_SUCCESS, hotkey_throttle.try_add_throttle_key(throttle_keys[j]));
      ASSERT_EQ(OB_SUCCESS, ObTableHotKeyMgr::get_instance().set_cnt(1000, 0, rk_strings[j], 0, epoch, HotKeyType::TABLE_HOTKEY_INSERT, 10));
      ASSERT_EQ(OB_SUCCESS, ObTableHotKeyMgr::get_instance().set_cnt(2000, 0, rk_strings[j], 0, epoch, HotKeyType::TABLE_HOTKEY_INSERT, 100));
    }
  }

  hotkey_throttle.destroy();
  hotkey_mgr.hotkey_cache_.destroy();
  hotkey_mgr.tenant_hotkey_map_.destroy();
  allocator_.free(rk_buf);
  allocator_.free(tr_buf);
}



/**
 * -----------------------------------Test TableApiHotkey-----------------------------------
 */

TEST_F(TestTableApiHotkey, ObTableHotKeyCache)
{
  ObTableHotKeyCacheKey key;
  key.rowkey_ = ObString(22, 22, "test_tableapi_hotkey_0");
  ObTableHotKeyCacheKey *pkey = nullptr;
  key.deep_copy(allocator_, pkey);

  // test ObTableHotKeyCacheKey
  {
    ASSERT_EQ(key.hash(), pkey->hash());
    ASSERT_EQ(1, key==*pkey);
    pkey->partition_id_ = 100;
    ASSERT_EQ(key.hash(), pkey->hash());
    ASSERT_EQ(1, key==*pkey);
    pkey->type_ = HotKeyType::TABLE_HOTKEY_ALL;
    key.type_ = HotKeyType::TABLE_HOTKEY_INSERT;
    ASSERT_NE(key.hash(), pkey->hash());
    ASSERT_NE(1, key==*pkey);
  }

  // Test ObTableSlideWindowHotkey
  {
    ObTableSlideWindowHotkey p1 = key;
    ObTableSlideWindowHotkey *p2 = nullptr;

    p1.deep_copy(allocator_, p2);
    ASSERT_EQ(p1.hash(), p2->hash());
    ASSERT_EQ(1, p1==*p2);
    p1.type_ = HotKeyType::TABLE_HOTKEY_ALL;
    p2->type_ = HotKeyType::TABLE_HOTKEY_INSERT;
    ASSERT_NE(p1.hash(), p2->hash());
    ASSERT_NE(1, p1==*p2);
    allocator_.free(p2);
  }

  // Test ObTableHotKeyCache
  {
    int64_t get_cnt = 0;
    ObTableHotKeyCache hotkey_cache;
    ObTableHotKeyCacheValue hotkey_cache_value;
    pkey->type_ = HotKeyType::TABLE_HOTKEY_ALL;
    key.type_ = HotKeyType::TABLE_HOTKEY_INSERT;
    ASSERT_EQ(OB_SUCCESS, hotkey_cache.init("TableHKCache_0"));
    ASSERT_EQ(OB_SUCCESS, hotkey_cache.get(key, hotkey_cache_value, 1, get_cnt));
    ASSERT_EQ(1, get_cnt);
    ASSERT_EQ(OB_SUCCESS, hotkey_cache.get(key, hotkey_cache_value, 5, get_cnt));
    // INSERT 6 times
    ASSERT_EQ(6, get_cnt);
    ASSERT_EQ(OB_SUCCESS, hotkey_cache.put(*pkey, hotkey_cache_value));
    ASSERT_EQ(OB_SUCCESS, hotkey_cache.get(*pkey, hotkey_cache_value, 1, get_cnt));
    // ALL 2 times
    ASSERT_EQ(2, get_cnt);

    pkey->type_ = HotKeyType::TABLE_HOTKEY_INSERT;
    // INSERT 7 times;
    ASSERT_EQ(OB_SUCCESS, hotkey_cache.get(*pkey, hotkey_cache_value, 1, get_cnt));
    ASSERT_EQ(7, get_cnt);
    // "test_tableapi_hotkey_0" -> "Test_tableapi_hotkey_0"
    pkey->rowkey_.ptr()[0] = 'T';
    key.type_ = HotKeyType::TABLE_HOTKEY_ALL;
    ASSERT_EQ(OB_SUCCESS, hotkey_cache.get(key, hotkey_cache_value, 1, get_cnt));
    // ALL 3 times
    ASSERT_EQ(3, get_cnt);
    hotkey_cache.destroy();
  }
  allocator_.free(pkey);
}

TEST_F(TestTableApiHotkey, ObTableHotkey)
{
  ObTableTenantHotKey tenant_hk(3000);
  ObTableHotKey *hotkeys;
  char *buf;
  ASSERT_EQ(OB_SUCCESS, tenant_hk.init());
  ASSERT_NE(nullptr, hotkeys = static_cast<ObTableHotKey*>(allocator_.alloc(sizeof(ObTableHotKey) * 100)));
  ASSERT_NE(nullptr, buf = static_cast<char*>(allocator_.alloc(sizeof(char) * 24 * 100)));
  uint64_t tenant_id = 1000;
  for (int i = 0; i < 10; ++i) {
    if (i == 5) tenant_id = 2000;
    for (int j = 0; j < 10; ++j) {
      int pos = 10 * i + j;
      ObTableHotKey *cur_hotkey = new(&hotkeys[pos]) ObTableHotKey;
      snprintf(&buf[pos * 24], 24, "test_tableapi_hotkey_%c%c", '0'+i, '0'+j);
      cur_hotkey->rowkey_ = ObString(23, 23, &buf[pos * 24]);
      cur_hotkey->tenant_id_ = tenant_id;
    }
  }

  // Test ObTableHotKeyHeap
  {
    ObTableHotKeyHeap hotkey_heap;
    ObTableHotkeyPair *ppair = nullptr;
    ObTableHotKeyValue cnt;
    ASSERT_EQ(OB_SUCCESS, hotkey_heap.init(tenant_hk.arena_allocator_, 10));
    hotkey_heap.set_allocator(tenant_hk.allocator_);

    for (int i = 0; i < 99; ++i) {
      if (hotkey_heap.get_count() == hotkey_heap.get_heap_size()) {
        hotkey_heap.pop(ppair, cnt);
        tenant_hk.allocator_.free(ppair->first);
        tenant_hk.allocator_.free(ppair);
      }
      ASSERT_EQ(OB_SUCCESS, tenant_hk.generate_hotkey_pair(ppair, hotkeys[i], i));
      hotkey_heap.push(*ppair);
      if (i >= 9) {
        hotkey_heap.top(ppair);
        ASSERT_EQ(i-9, ppair->second);
      }
    }
    hotkey_heap.pop(ppair, cnt);
    tenant_hk.allocator_.free(ppair->first);
    tenant_hk.allocator_.free(ppair);
    ASSERT_EQ(OB_SUCCESS, tenant_hk.generate_hotkey_pair(ppair, hotkeys[0], 10));
    hotkey_heap.push(*ppair);
    hotkey_heap.top(ppair);
    ASSERT_EQ(10, ppair->second);
    hotkey_heap.clear();
    hotkey_heap.destroy();
  }

  // Test ObTableTenantHotKey
  {
    // Here suppose HOTKEY_WINDOWS_SIZE = 4
    OBTableThrottleMgr::epoch_ = 0;
    double average_cnt = 0.0;

    // 1. get average cnt of hotkeys
    // 2. set cur cnt into slide window map
    ASSERT_EQ(OB_SUCCESS, tenant_hk.add_hotkey_slide_window(hotkeys[0], 20));
    ASSERT_EQ(OB_SUCCESS, tenant_hk.get_slide_window_cnt(&hotkeys[0], average_cnt));
    ASSERT_EQ(5, average_cnt);

    // every epoch do refresh
    ASSERT_EQ(OB_SUCCESS, tenant_hk.refresh_slide_window());
    ++OBTableThrottleMgr::epoch_;

    ASSERT_EQ(OB_SUCCESS, tenant_hk.add_hotkey_slide_window(hotkeys[0], 40));
    ASSERT_EQ(OB_SUCCESS, tenant_hk.get_slide_window_cnt(&hotkeys[0], average_cnt));
    ASSERT_EQ(15, average_cnt);

    // every epoch do refresh
    ASSERT_EQ(OB_SUCCESS, tenant_hk.refresh_slide_window());
    ++OBTableThrottleMgr::epoch_;

    ASSERT_EQ(OB_SUCCESS, tenant_hk.add_hotkey_slide_window(hotkeys[0], 60));
    ASSERT_EQ(OB_SUCCESS, tenant_hk.get_slide_window_cnt(&hotkeys[0], average_cnt));
    ASSERT_EQ(30, average_cnt);

    // every epoch do refresh
    ASSERT_EQ(OB_SUCCESS, tenant_hk.refresh_slide_window());
    ++OBTableThrottleMgr::epoch_;
    // do nothing

    // every epoch do refresh
    ASSERT_EQ(OB_SUCCESS, tenant_hk.refresh_slide_window());
    ++OBTableThrottleMgr::epoch_;

    ASSERT_EQ(OB_SUCCESS, tenant_hk.get_slide_window_cnt(&hotkeys[0], average_cnt));
    ASSERT_EQ(30, average_cnt);

    // every epoch do refresh
    ASSERT_EQ(OB_SUCCESS, tenant_hk.refresh_slide_window()); // erase 40
    ++OBTableThrottleMgr::epoch_;

    // get_slide_window_cnt will only be used after add_hotkey_slide_window
    // so 40 will be replaced by latest value if get_slide_window_cnts is used
    ASSERT_EQ(OB_SUCCESS, tenant_hk.get_slide_window_cnt(&hotkeys[0], average_cnt));
    ASSERT_EQ(25, average_cnt); // (40(which will be replace by latest value) + 60) / 4

    // every epoch do refresh
    ASSERT_EQ(OB_SUCCESS, tenant_hk.refresh_slide_window()); // erase 60
    ++OBTableThrottleMgr::epoch_;

    // in this epoch, every hotkey has been erase
    ASSERT_EQ(OB_HASH_NOT_EXIST, tenant_hk.get_slide_window_cnt(&hotkeys[0], average_cnt));
  }


  // Test ObTableHotKeyMgr
  {
    // since ObTableHotKeyMgr need ObMultiVersionSchemaService which can not been test in this ut
    // so we just simple function without init
    ObTableHotKeyMgr hotkey_mgr;
    int64_t cnt = 0;
    ObTableHotkeyPair *ppair = nullptr;
    hotkey_mgr.inited_ = 2;
    hotkey_mgr.instance_ = &hotkey_mgr;
    OBTableThrottleMgr::epoch_ = 0;
    // set rowkey
    ObObj key_objs[1];
    key_objs[0].set_int(1);
    ObRowkey rk(key_objs, 1);
    ObString rk_string;
    // test rowkey_serialize
    ASSERT_EQ(OB_SUCCESS, hotkey_mgr.rowkey_serialize(arena_allocator_, rk,  rk_string));
    ObRowkey rk_result;
    int64_t pos = 0;
    rk_result.deserialize(arena_allocator_, rk_string.ptr(), rk_string.length(), pos);
    ASSERT_EQ(rk, rk_result);
    // test deep_copy_hotkey_pair have been tested previous
    // test tenant hotkey and kvcache
    ASSERT_EQ(OB_SUCCESS, hotkey_mgr.hotkey_cache_.init("TableHKCache", hotkey_mgr.hotkey_cache_.OB_TABLE_HOTKEY_CACHE_PRIORITY));
    ASSERT_EQ(OB_SUCCESS, hotkey_mgr.tenant_hotkey_map_.create(hotkey_mgr.HOTKEY_TENANT_BUCKET_NUM, ObModIds::TEST, ObModIds::TEST));
    ASSERT_EQ(OB_SUCCESS, hotkey_mgr.add_new_tenant(1000));
    ASSERT_EQ(OB_SUCCESS, ObTableHotKeyMgr::get_instance().set_cnt(1000, 10, rk_string, 0, 0,
                                                                   HotKeyType::TABLE_HOTKEY_INSERT, 1));
    ASSERT_EQ(OB_SUCCESS, ObTableHotKeyMgr::get_instance().set_cnt(1000, 10, rk_string, 0, 0,
                                                                   HotKeyType::TABLE_HOTKEY_QUERY, 1));
    // check get_cnt
    // test tenant hotkey heap/map and kvcache
    char *rk_buf;
    ASSERT_NE(nullptr, rk_buf = static_cast<char*>(allocator_.alloc(sizeof(char) * 24 * 100)));
    ObString rk_strings[100];
    for (int i = 0; i < 10; ++i) {
      for (int j = 0; j < 10; ++j) {
        int pos = 10 * i + j;
        snprintf(&rk_buf[pos * 24], 24, "Test_Tableapi_Hotkey_%c%c", '0'+i, '0'+j);
        rk_strings[pos] = ObString(23, 23, &rk_buf[pos * 24]);
      }
    }

    for (int i = 1; i < 5; ++i) {
      for (int j = 0; j < 15; ++j) {
        HotKeyType type = static_cast<HotKeyType>(i);
        ASSERT_EQ(OB_SUCCESS, ObTableHotKeyMgr::get_instance().set_cnt(1000, 10, rk_strings[j], 0, 0,
                                                                       type, i*100 + j + 1));
      }
    }

    ObTableTenantHotKey *tenant_hotkey = nullptr;
    ASSERT_EQ(OB_SUCCESS, hotkey_mgr.get_tenant_map().get_refactored(1000, tenant_hotkey));
    ASSERT_NE(nullptr, tenant_hotkey);

    for (int i = 0; i < 10; ++i) {
      ASSERT_EQ(OB_SUCCESS, tenant_hotkey->infra_[OBTableThrottleMgr::epoch_ % ObTableTenantHotKey::TENANT_HOTKEY_INFRA_NUM].hotkey_heap_.pop(ppair, cnt));
      ASSERT_EQ(1024 + i*4, cnt); // 4 kinds of hotkeytype / 100 + 200 + 300 + 400 = 1000 / 1000 + 4*5 + 4*1 = 1024
      allocator_.free(ppair->first);
      allocator_.free(ppair);
    }

    hotkey_mgr.hotkey_cache_.destroy();
    hotkey_mgr.tenant_hotkey_map_.destroy();
    allocator_.free(rk_buf);
  }

  allocator_.free(hotkeys);
  allocator_.free(buf);
}

} // namespace observer
} // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_observer.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}