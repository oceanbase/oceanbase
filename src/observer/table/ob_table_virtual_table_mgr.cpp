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
#include "ob_table_virtual_table_mgr.h"
#include "ob_table_throttle_manager.h"
#include "ob_table_throttle.h"
#include "ob_table_hotkey.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::table;

ObTableVirtualTableMgr::ObTableVirtualTableMgr()
    : hotkey_throttle_(NULL),
      hotkey_mgr_(NULL),
      key_set_(),
      arena_allocator_(NULL),
      db_map_()
{
}

ObTableVirtualTableMgr::~ObTableVirtualTableMgr()
{
  db_map_.destroy();
  key_set_.destroy();
}

int ObTableVirtualTableMgr::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(db_map_.create(STAT_DB_MAP_BUCKET_NUM, ObModIds::OB_VIRTUAL_TABLE_HOTKEY, ObModIds::OB_VIRTUAL_TABLE_HOTKEY))) {
    LOG_WARN("fail to set db map for virtual table manger", K(ret));
  } else if (key_set_.create(STAT_KEY_SET_BUCKET_NUM)) {
    LOG_WARN("fail to create key set for virtual table manger", K(ret));
  }
  return ret;
}

int ObTableVirtualTableMgr::create_stat_hotkey(ObTableThrottleKey &hotkey,
                                               ObTableStatHotkey *&stat_hotkey)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  ObTableStatHotkey *new_key = nullptr;
  ObRowkey *rowkey = nullptr;

  if (nullptr == (rowkey = static_cast<ObRowkey*>(arena_allocator_->alloc(sizeof(ObRowkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (nullptr == (new_key = static_cast<ObTableStatHotkey*>(arena_allocator_->alloc(sizeof(ObTableStatHotkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    new_key = new(new_key) ObTableStatHotkey;
    rowkey = new(rowkey) ObRowkey;
    if (OB_FAIL(rowkey->deserialize(*arena_allocator_, hotkey.key_.ptr(), hotkey.key_.size(), pos))) {
      LOG_WARN("fail to deserialize rowkey", K(ret));
    } else {
      if (0 == hotkey.table_id_) {
        new_key->database_id_ = 0;
      } else if (OB_FAIL(db_map_.get_refactored(hotkey.table_id_, new_key->database_id_))) {
        if (OB_HASH_NOT_EXIST == ret) {
          const schema::ObSimpleTableSchemaV2 *table_schema = NULL;
          schema::ObSchemaGetterGuard schema_guard;
          if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(hotkey.tenant_id_, schema_guard))) {
            LOG_WARN("fail to get schema guard", K(ret), K(hotkey.tenant_id_));
          } else if (OB_FAIL(schema_guard.get_table_schema(hotkey.table_id_, table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), K(hotkey.table_id_));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("get null table schema", K(ret), K(hotkey.table_id_), K(hotkey.tenant_id_));
          } else {
            new_key->database_id_ = table_schema->get_database_id();
            if (OB_FAIL(db_map_.set_refactored(hotkey.table_id_, new_key->database_id_))) {
              LOG_WARN("fail to set database id to map", K(ret));
            }
          }
        } else {
          LOG_WARN("fail to get database id from map", K(ret));
        }
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        new_key->tenant_id_ = hotkey.tenant_id_;
        new_key->partition_id_ = hotkey.partition_id_;
        new_key->table_id_ = hotkey.table_id_;
        new_key->hotkey_ = *rowkey;
        new_key->hotkey_type_ = HotKeyType::TABLE_HOTKEY_ALL;

        stat_hotkey = new_key;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != new_key) {
      arena_allocator_->free(new_key);
    }
    if (nullptr != rowkey) {
      arena_allocator_->free(rowkey);
    }
  }

  return ret;
}

int ObTableVirtualTableMgr::create_stat_hotkey(ObTableHotKey &hotkey,
                                               ObTableStatHotkey *&stat_hotkey)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  ObTableStatHotkey *new_key = nullptr;
  ObRowkey *rowkey = nullptr;

  if (nullptr == (rowkey = static_cast<ObRowkey*>(arena_allocator_->alloc(sizeof(ObRowkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (nullptr == (new_key = static_cast<ObTableStatHotkey*>(arena_allocator_->alloc(sizeof(ObTableStatHotkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    new_key = new(new_key) ObTableStatHotkey;
    rowkey = new(rowkey) ObRowkey;
    if (OB_FAIL(rowkey->deserialize(*arena_allocator_, hotkey.rowkey_.ptr(), hotkey.rowkey_.size(), pos))) {
      LOG_WARN("fail to deserialize rowkey", K(ret));
    } else {
      if (0 == hotkey.table_id_) {
        new_key->database_id_ = 0;
      } else if (OB_FAIL(db_map_.get_refactored(hotkey.table_id_, new_key->database_id_))) {
        if (OB_HASH_NOT_EXIST == ret) {
          const schema::ObSimpleTableSchemaV2 *table_schema = NULL;
          schema::ObSchemaGetterGuard schema_guard;
          if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(hotkey.tenant_id_, schema_guard))) {
            LOG_WARN("fail to get schema guard", K(ret), K(hotkey.tenant_id_));
          } else if (OB_FAIL(schema_guard.get_table_schema(hotkey.table_id_, table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), K(hotkey.table_id_));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("get null table schema", K(ret), K(hotkey.table_id_));
          } else {
            new_key->database_id_ = table_schema->get_database_id();
            if (OB_FAIL(db_map_.set_refactored(hotkey.table_id_, new_key->database_id_))) {
              LOG_WARN("fail to set database id to map", K(ret));
            }
          }
        } else {
          LOG_WARN("fail to get database id from map", K(ret));
        }
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        new_key->tenant_id_ = hotkey.tenant_id_;
        new_key->partition_id_ = hotkey.partition_id_;
        new_key->table_id_ = hotkey.table_id_;
        new_key->hotkey_ = *rowkey;
        new_key->hotkey_type_ = hotkey.type_;

        stat_hotkey = new_key;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != new_key) {
      arena_allocator_->free(new_key);
    }
    if (nullptr != rowkey) {
      arena_allocator_->free(rowkey);
    }
  }

  return ret;
}

int ObTableVirtualTableMgr::set_iterator()
{
  int ret = OB_SUCCESS;

  hotkey_throttle_ = &ObTableHotKeyThrottle::get_instance();
  hotkey_mgr_ = &ObTableHotKeyMgr::get_instance();

  ObTableHotKey hotkey; // be reused
  const ObTableHotKeyCacheValue *hotkey_value = nullptr;

  // add hotkey from throttle key map
  for (ThrottleKeyHashMap::iterator iter = hotkey_throttle_->get_throttle_map().begin();
       OB_SUCC(ret) && iter != hotkey_throttle_->get_throttle_map().end(); ++iter)
  {
    ObTableStatHotkey *cur_stat_hotkey = nullptr;
    if (OB_FAIL(create_stat_hotkey(*iter->first, cur_stat_hotkey))) {
      LOG_WARN("fail to create stat hotkey", K(ret));
    } else {
      int64_t hotkey_freq = 0;
      ObTableTenantHotKey *tenant_hotkey = nullptr;
      if (OB_FAIL(hotkey_mgr_->get_tenant_map().get_refactored(iter->first->tenant_id_, tenant_hotkey))){
        LOG_WARN("fail to get tenant hotkey manager from tenant map", K(ret));
      } else {
        hotkey.tenant_id_ = iter->first->tenant_id_;
        hotkey.table_id_ = iter->first->table_id_;
        hotkey.epoch_ = OBTableThrottleMgr::epoch_-1;
        hotkey.type_ = HotKeyType::TABLE_HOTKEY_ALL;
        hotkey.rowkey_ = iter->first->key_;
        if (OB_FAIL(hotkey_mgr_->get_hotkey_cache().get_without_put(hotkey, hotkey_value, 1, hotkey_freq))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("fail to get key count from cache", K(ret));
          } else {
            hotkey_freq = 0;
            ret = OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret)) {
          cur_stat_hotkey->hotkey_freq_ = static_cast<int64_t>(static_cast<double>(hotkey_freq) / ObTableHotKeyMgr::KV_HOTKEY_STAT_RATIO);
          cur_stat_hotkey->throttle_percent = iter->second;
          if (OB_FAIL(key_set_.set_refactored(*cur_stat_hotkey))) {
            LOG_WARN("fail to set", K(ret), KPC(cur_stat_hotkey));
          }
        }
      }
    }
  }

  // add hotkey from heap, get each tenant topk hot key
  for (ObTableTenantHotKeyMap::iterator iter = hotkey_mgr_->get_tenant_map().begin();
       OB_SUCC(ret) && iter != hotkey_mgr_->get_tenant_map().end(); ++iter)
  {
    ObTableTenantHotKey *tenant_hotkey = iter->second;
    ObTableHotkeyPair *hotkey_pair;
    double average_cnt = 0.0;
    ObTableTenantHotKey::ObTableTenantHotKeyInfra &infra = tenant_hotkey->infra_[(OBTableThrottleMgr::epoch_-1) % ObTableTenantHotKey::TENANT_HOTKEY_INFRA_NUM];

    DRWLock::RDLockGuard guard(infra.rwlock_);

    // traverse all hotkey in a tenant's hotkey heap
    for (int64_t item = 0; OB_SUCC(ret) && item < infra.hotkey_heap_.get_count(); ++item) {
      hotkey_pair = infra.hotkey_heap_.get_heap()[item];
      ObTableHotKey *hotkey = hotkey_pair->first;
      ObTableStatHotkey *cur_stat_hotkey = nullptr;

      if (OB_FAIL(create_stat_hotkey(*hotkey, cur_stat_hotkey))) {
        LOG_WARN("fail to create stat hotkey", K(ret));
      } else {
        int tmp_ret = key_set_.exist_refactored(*cur_stat_hotkey);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          // get hotkey freq and restricted rate
          int64_t hotkey_freq = 0;
          ObTableThrottleKey throttle_hotkey;
          int64_t throttle_percent = 0;
          hotkey_throttle_->create_throttle_hotkey(*hotkey, throttle_hotkey);
          if (OB_FAIL(infra.hotkey_map_.get_refactored(hotkey, hotkey_freq))) {
            LOG_ERROR("fail to get hotkey from map", K(hotkey), K(ret));
          } else if (OB_FAIL(hotkey_throttle_->get_throttle_map().get_refactored(&throttle_hotkey, throttle_percent))) {
            if (OB_HASH_NOT_EXIST != ret) {
              // do nothing
              LOG_WARN("fail to get hotkey from throttle map", K(ret));
            } else {
              // not block yet
              ret = OB_SUCCESS;
            }
          }

          if (OB_SUCC(ret)){
            cur_stat_hotkey->hotkey_freq_ = static_cast<int64_t>(static_cast<double>(hotkey_freq) / (ObTableHotKeyMgr::KV_HOTKEY_STAT_RATIO));
            cur_stat_hotkey->throttle_percent = throttle_percent;
            if (OB_FAIL(key_set_.set_refactored(*cur_stat_hotkey))) {
              LOG_WARN("fail to set", K(ret), KPC(cur_stat_hotkey));
            }
          }
        } else if (OB_HASH_EXIST == tmp_ret) {
          // do nothing, key has been added
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error when set hashset", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    iter_ = key_set_.begin();
  }

  return ret;
}

ObTableStatHotkey &ObTableVirtualTableMgr::inner_get_next_row()
{
  return iter_++->first;
}

bool ObTableVirtualTableMgr::is_end() {
  return iter_ == key_set_.end();
}

void ObTableVirtualTableMgr::set_allocator(ObArenaAllocator *allocator)
{
  arena_allocator_ = allocator;
}
