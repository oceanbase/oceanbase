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

#ifndef OB_OCEANBASE_SCHEMA_INTERVAL_PART_CONTROLLER_H_
#define OB_OCEANBASE_SCHEMA_INTERVAL_PART_CONTROLLER_H_
#include "share/cache/ob_kv_storecache.h"                // ObIKVCache

namespace oceanbase
{
namespace obrpc
{
class ObAlterTableArg;
}
namespace share
{
namespace schema
{
class AlterTableSchema;
class ObMultiVersionSchemaService;
class ObIntervalTableCacheKey : public common::ObIKVCacheKey
{
public:
  ObIntervalTableCacheKey();
  ObIntervalTableCacheKey(const uint64_t tenant_id,
                          const uint64_t table_id);
  virtual ~ObIntervalTableCacheKey() {}
  virtual uint64_t get_tenant_id() const;
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual uint64_t hash() const;
  virtual int hash(uint64_t &hash_value) const  { hash_value = hash(); return OB_SUCCESS; }
  virtual int64_t size() const;
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        ObIKVCacheKey *&key) const;
  TO_STRING_KV(K_(tenant_id),
               K_(table_id));

  uint64_t tenant_id_;
  uint64_t table_id_;
};

class ObAddIntervalPartitionController
{
public:
  ObAddIntervalPartitionController();
  ~ObAddIntervalPartitionController();
  int init();
  void destroy();
  static int mtl_init(ObAddIntervalPartitionController* &controller);
  int add_interval_partition_lock(AlterTableSchema &alter_table_schema,
                                  bool &need_clean_up);
  int cleanup_interval_partition_lock(const ObIntervalTableCacheKey &lock_key);
private:
  int check_and_set_key_(const ObIntervalTableCacheKey &lock_key, bool &is_set);
  int check_inner_stat_() const;
private:
  bool inited_;
  static const int64_t COND_SLOT_NUM = 256;
  static const int64_t WAIT_TIMEOUT_MS = 500; // 0.5s
  static const int64_t CONSTRUCTING_KEY_SET_SIZE = 256;
  ObSpinLock interval_partition_lock_;
  ObThreadCond cond_slots_[COND_SLOT_NUM];
  common::hash::ObHashSet<ObIntervalTableCacheKey> constructing_keys_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAddIntervalPartitionController);
};

class ObAddIntervalPartitionGuard
{
public:
  ObAddIntervalPartitionGuard(const uint64_t tenant_id, const uint64_t table_id)
  {
    need_cleanup_ = false;
    lock_key_ = ObIntervalTableCacheKey(tenant_id, table_id);
  }
  int lock(AlterTableSchema &alter_table_schema);
  ~ObAddIntervalPartitionGuard();

private:
  bool need_cleanup_;
  ObIntervalTableCacheKey lock_key_;
};

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_INTERVAL_PART_CONTROLLER_H_
