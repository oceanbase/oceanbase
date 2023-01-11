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

#ifndef _OB_TABLE_VIRTUAL_TABLE_MGR_H
#define _OB_TABLE_VIRTUAL_TABLE_MGR_H

#include "ob_table_throttle_manager.h"
#include "ob_table_throttle.h"
#include "ob_table_hotkey.h"

using namespace oceanbase::table;

namespace oceanbase
{
namespace observer
{

/**
 * @brief used by __all_virtual_kv_hotkey_stat
 */
struct ObTableStatHotkey
{
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t partition_id_;
  uint64_t table_id_;
  ObRowkey hotkey_;
  HotKeyType hotkey_type_;
  int64_t hotkey_freq_;
  int64_t throttle_percent;
  ObTableStatHotkey(uint64_t tenant_id, uint64_t database_id, int64_t partition_id, uint64_t table_id,
                    ObRowkey hotkey, HotKeyType hotkey_type, int64_t hotkey_freq,
                    int64_t throttle_percent) : tenant_id_(tenant_id),
                    database_id_(database_id), partition_id_(partition_id), table_id_(table_id),
                    hotkey_(hotkey), hotkey_type_(hotkey_type),
                    hotkey_freq_(hotkey_freq), throttle_percent(throttle_percent)
  {
  }
  ObTableStatHotkey() : tenant_id_(0), database_id_(0), partition_id_(0), table_id_(0),
                        hotkey_(), hotkey_type_(HotKeyType::TABLE_HOTKEY_INVALID),
                        hotkey_freq_(0), throttle_percent(0)
  {
  }
  bool operator == (const ObTableStatHotkey &rhs) const
  {
    return (this->tenant_id_ == rhs.tenant_id_
            && this->table_id_ == rhs.table_id_
            && this->partition_id_ == rhs.partition_id_
            && this->hotkey_ == rhs.hotkey_
            && this->hotkey_type_ == rhs.hotkey_type_);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = hotkey_.hash();
    hash_val = murmurhash(&hotkey_type_, sizeof(HotKeyType), hash_val);
    hash_val = murmurhash(&tenant_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&table_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&partition_id_, sizeof(int64_t), hash_val);
    return hash_val;
  }
  // space of hotkey_.size() is used to store the rowkey string
  int64_t size() const { return sizeof(*this); }
  TO_STRING_KV(K(tenant_id_), K(table_id_), K(partition_id_), K(table_id_),
               K(hotkey_), K(hotkey_type_), K(hotkey_freq_), K(throttle_percent));
};

// store map table_id_ -> database_id_
typedef hash::ObHashMap<uint64_t, uint64_t> ObTableHotkeyStatDBMap;

/**
 * @brief used by __all_virtual_kv_hotkey_stat
 */
class ObTableVirtualTableMgr
{
public:
  ObTableVirtualTableMgr();
  virtual ~ObTableVirtualTableMgr();

  int init();
  int create_stat_hotkey(ObTableHotKey &hotkey,
                         int64_t hotkey_freq,
                         double restricted_rate,
                         ObTableStatHotkey *&stat_hotkey);
  int create_stat_hotkey(ObTableHotKey &hotkey, ObTableStatHotkey *&stat_hotkey);
  int create_stat_hotkey(ObTableThrottleKey &hotkey, ObTableStatHotkey *&stat_hotkey);
  int set_iterator();
  void set_allocator(ObArenaAllocator *allocator);
  bool is_end();
  ObTableStatHotkey &inner_get_next_row();

private:
  static const int64_t STAT_KEY_SET_BUCKET_NUM = 32;
  static const int64_t STAT_DB_MAP_BUCKET_NUM = 16;
  ObTableHotKeyThrottle *hotkey_throttle_;
  ObTableHotKeyMgr *hotkey_mgr_;
  common::hash::ObHashSet<ObTableStatHotkey> key_set_;
  common::hash::ObHashSet<ObTableStatHotkey>::iterator iter_;
  ObArenaAllocator *arena_allocator_;
  ObTableHotkeyStatDBMap db_map_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_VIRTUAL_TABLE_MGR_H */
