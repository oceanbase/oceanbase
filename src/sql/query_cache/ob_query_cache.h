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

#ifndef OCEANBASE_SQL_QUERY_CACHE_OB_QUERY_CACHE_
#define OCEANBASE_SQL_QUERY_CACHE_OB_QUERY_CACHE_

#include "common/ob_field.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"
#include "share/cache/ob_kv_storecache.h"
#include "sql/engine/basic/ob_ra_row_store.h"


namespace oceanbase
{

namespace sql
{

class ObSQLSessionInfo;

struct ObQueryCacheFlag
{
  ObQueryCacheFlag();
  ~ObQueryCacheFlag();
  int init(const ObSQLSessionInfo &session);
  int equal(const ObQueryCacheFlag &other, bool &equal) const;
  int hash(uint64_t &hash_value) const;
  TO_STRING_KV(K_(autocommit), K_(in_trans), K_(limit),
              K_(div_precision_increment), K_(time_zone_id), K_(time_zone_offset),
              K_(group_concat_max_len), K_(character_set_client),
              K_(character_set_connection), K_(character_set_results),
              K_(sql_mode));

  bool autocommit_;
  bool in_trans_;
  int64_t limit_;
  int64_t div_precision_increment_;
  int32_t time_zone_id_;
  int32_t time_zone_offset_;
  uint64_t group_concat_max_len_;
  common::ObCharsetType character_set_client_;
  common::ObCharsetType character_set_connection_;
  common::ObCharsetType character_set_results_;
  ObSQLMode sql_mode_;
};

class ObQueryCacheKey : public common::ObIKVCacheKey
{
public:
  ObQueryCacheKey();
  ObQueryCacheKey(const uint64_t tenant_id,
                const uint64_t &database_id,
                const ObString &sql,
                const ObSQLSessionInfo &session);
  virtual ~ObQueryCacheKey();
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const;
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  inline bool is_valid() const { return is_valid_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline ObString get_sql() const {return sql_;}
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(sql), K_(flag));
private:
  bool is_valid_;
  uint64_t tenant_id_;
  uint64_t database_id_;
  ObString sql_;
  ObQueryCacheFlag flag_;
};

struct ObQueryCacheValueWeight
{
  static constexpr double FREQUENCY_WEIGHT = 1;
  static constexpr double EXEC_TIME_WEIGHT = 1.0 / 10000;
  static constexpr double COST_WEIGHT = -1.0 / 10000;
  ObQueryCacheValueWeight();
  ~ObQueryCacheValueWeight();
  bool operator < (const ObQueryCacheValueWeight &other);
  int update(const ObQueryCacheValueWeight &weight);
  inline uint64_t get_frequency() { return frequency_; }
  inline int64_t get_exec_time() { return exec_time_; }
  inline int64_t get_cost() { return cost_; }
  TO_STRING_KV(K_(frequency), K_(exec_time), K_(cost));

  uint64_t frequency_;
  int64_t exec_time_;
  int64_t cost_;
};

class ObQueryCacheValue : public common::ObIKVCacheValue
{
public:
  ObQueryCacheValue(common::ObIAllocator *alloc);
  virtual ~ObQueryCacheValue();
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  inline void set_valid(bool valid) { valid_ = valid; }
  inline uint64_t *get_table_ids() const { return table_ids_; }
  inline uint64_t *get_trans_ids() const { return trans_ids_; }
  inline int64_t get_row_cnt() const { return row_array_.count(); }
  inline void add_frequency() { weight_.frequency_++; }
  inline void set_exec_time(int64_t exec_time) { weight_.exec_time_ = exec_time; }
  inline void set_cost(int64_t cost) { weight_.cost_ = cost; }
  inline int64_t get_row_mem_size() const { return mem_hold_; }
  uint64_t get_table_id(uint32_t idx) const;
  uint64_t get_trans_id(uint32_t idx) const;
  inline ObQueryCacheValueWeight get_weight() const { return weight_; }
  int add_row(const common::ObNewRow &row);
  int get_row(const int64_t row_id, const common::ObNewRow *&row);
  
  inline common::ObIAllocator *get_alloc() { return alloc_; }
  inline common::ColumnsFieldArray &get_fields() { return fields_; }
  void init_ref_table_info(uint32_t tables_num);
  inline void set_packed(bool is_packed) { is_packed_ = is_packed; }
  inline bool is_packed() { return is_packed_; }
  inline void set_table_id(uint64_t table_id, uint32_t idx) { table_ids_[idx] = table_id; }
  inline void set_trans_id(uint64_t trans_id, uint32_t idx) { trans_ids_[idx] = trans_id; }
  inline bool is_valid() const { return valid_; }
  TO_STRING_KV(K_(valid), K_(is_packed), K_(ref_table_cnt), K_(fields), K_(weight));
private:
  bool valid_;
  bool is_packed_;

  // table_ids_ and trans_ids_ is used for check consistency.
  // Before add item to them, you must alloc suitable memory for them.
  uint64_t *table_ids_;
  uint64_t *trans_ids_;
  uint64_t ref_table_cnt_;
  
  int64_t mem_hold_;
  ObQueryCacheValueWeight weight_;

  // For the storage of rows, allocate memory using 'alloc' and store the pointer in 'row_array'
  // Be aware of possible memory leaks.
  common::ObIAllocator *alloc_;
  common::ObSEArray<common::ObNewRow *, 8> row_array_;
  common::ColumnsFieldArray fields_;
};

struct ObQueryCacheValueHandle
{
  ObQueryCacheValue *query_cache_value_;
  common::ObKVCacheHandle handle_;
  ObQueryCacheValueHandle() : query_cache_value_(NULL), handle_() {}
  virtual ~ObQueryCacheValueHandle() {}
  inline bool is_valid() const { return NULL != query_cache_value_ && query_cache_value_->is_valid() && handle_.is_valid(); }
  inline void reset() { query_cache_value_ = NULL; handle_.reset(); }
  inline ObQueryCacheValue *get_value() { return query_cache_value_; }
  TO_STRING_KV(KP(query_cache_value_), K(handle_));
};

class ObQueryCache : public common::ObKVCache<ObQueryCacheKey, ObQueryCacheValue>
{
  static constexpr uint64_t PLACE_HOLDER = 0x1;
  static constexpr int64_t MAX_MEMORY_SIZE = 128 * 1024 * 1024; // 128MB
public:
  virtual ~ObQueryCache();
  static ObQueryCache* get_instance();
  inline void add_mem_size(int64_t row_mem_size) { row_mem_size_ += row_mem_size; }
  inline void add_row_cnt(int64_t row_cnt) { row_cnt_ += row_cnt; }
  inline uint64_t get_size() const { return size() + row_mem_size_; }
  inline DRWLock &get_lock() { return lock_; } 
  OB_INLINE bool is_valid() { return OB_NOT_NULL(instance_) && PLACE_HOLDER != (uint64_t)instance_; }
  int query(const ObQueryCacheKey &key, ObQueryCacheValueHandle &handle);
  int insert(const ObQueryCacheKey &key,
            const common::ColumnsFieldArray &fields,
            ObQueryCacheValueHandle &handle);
  int remove(const ObQueryCacheKey &key);
  int eviction(const ObSQLSessionInfo &session);
  int flush();
  void debug_info();
  DISALLOW_COPY_AND_ASSIGN(ObQueryCache);

private:
	ObQueryCache() : instance_(nullptr), row_mem_size_(0), row_cnt_(0), alloc_(inner_alloc_) {}

private:
  ObQueryCache *instance_;
  int64_t row_mem_size_;
  int64_t row_cnt_;
  common::ObArenaAllocator inner_alloc_;
  common::ObSafeArenaAllocator alloc_;
  DRWLock lock_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_QUERY_CACHE_OB_QUERY_CACHE_