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

#include "ob_query_cache.h"
#include "lib/allocator/ob_malloc.h"
#include "pl/sys_package/ob_dbms_sql.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

/**
 * -----------------------------------------------------ObQueryCacheFlag------------------------------------------------------
 */
ObQueryCacheFlag::ObQueryCacheFlag()
  : autocommit_(false),
    in_trans_(false),
    limit_(0),
    div_precision_increment_(0),
    time_zone_id_(0),
    time_zone_offset_(0),
    group_concat_max_len_(0),
    character_set_client_(CHARSET_INVALID),
    character_set_connection_(CHARSET_INVALID),
    character_set_results_(CHARSET_INVALID),
    sql_mode_(0)
{
}

int ObQueryCacheFlag::init(const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  in_trans_ = session.get_in_transaction();
  time_zone_id_ = session.get_timezone_info()->get_tz_id();
  time_zone_offset_ = session.get_timezone_info()->get_offset();
  sql_mode_ = session.get_sql_mode();
  session.get_autocommit(autocommit_);
  session.get_sql_select_limit(limit_);
  session.get_div_precision_increment(div_precision_increment_);
  session.get_group_concat_max_len(group_concat_max_len_);
  session.get_character_set_client(character_set_client_);
  session.get_character_set_connection(character_set_connection_);
  session.get_character_set_results(character_set_results_);
  return ret;
}


ObQueryCacheFlag::~ObQueryCacheFlag()
{
}

int ObQueryCacheFlag::equal(const ObQueryCacheFlag &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  equal = (autocommit_ == other.autocommit_);
  equal &= (in_trans_ == other.in_trans_);
  equal &= (limit_ == other.limit_);
  equal &= (div_precision_increment_ == other.div_precision_increment_);
  equal &= (group_concat_max_len_ == other.group_concat_max_len_);
  equal &= (character_set_client_ == other.character_set_client_);
  equal &= (character_set_connection_ == other.character_set_connection_);
  equal &= (character_set_results_ == other.character_set_results_);
  equal &= (time_zone_id_ == other.time_zone_id_);
  equal &= (time_zone_offset_ == other.time_zone_offset_);
  return ret;
}

int ObQueryCacheFlag::hash(uint64_t &hash_value) const
{
  int ret = OB_SUCCESS;
  hash_value = common::murmurhash(&group_concat_max_len_, sizeof(group_concat_max_len_), hash_value);
  hash_value = common::murmurhash(&autocommit_, sizeof(autocommit_), hash_value);
  hash_value = common::murmurhash(&in_trans_, sizeof(in_trans_), hash_value);
  hash_value = common::murmurhash(&limit_, sizeof(limit_), hash_value);
  hash_value = common::murmurhash(&div_precision_increment_, sizeof(div_precision_increment_), hash_value);
  hash_value = common::murmurhash(&time_zone_id_, sizeof(time_zone_id_), hash_value);
  hash_value = common::murmurhash(&time_zone_offset_, sizeof(time_zone_offset_), hash_value);
  hash_value = common::murmurhash(&character_set_client_, sizeof(character_set_client_), hash_value);
  hash_value = common::murmurhash(&character_set_connection_, sizeof(character_set_connection_), hash_value);
  hash_value = common::murmurhash(&character_set_results_, sizeof(character_set_results_), hash_value);
  hash_value = common::murmurhash(&sql_mode_, sizeof(sql_mode_), hash_value);
  return ret;
}

/**
 * -----------------------------------------------------ObQueryCacheValueWeight------------------------------------------------------
 */
ObQueryCacheValueWeight::ObQueryCacheValueWeight()
  : frequency_(1),
    exec_time_(0),
    cost_(0)
{
}

ObQueryCacheValueWeight::~ObQueryCacheValueWeight()
{
}

bool ObQueryCacheValueWeight::operator < (const ObQueryCacheValueWeight &other)
{
  double w1 = frequency_ * FREQUENCY_WEIGHT + exec_time_ * EXEC_TIME_WEIGHT + cost_ * COST_WEIGHT;
  double w2 = other.frequency_ * FREQUENCY_WEIGHT
              + other.exec_time_ * EXEC_TIME_WEIGHT
              + other.cost_ * COST_WEIGHT;
  return w1 < w2;
}

int ObQueryCacheValueWeight::update(const ObQueryCacheValueWeight &weight)
{
  int ret = OB_SUCCESS;
  frequency_ = weight.frequency_ + 1;
  exec_time_ = weight.exec_time_;
  cost_ = weight.cost_;
  return ret;
}

/**
 * -----------------------------------------------------ObQueryCacheKey------------------------------------------------------
 */
ObQueryCacheKey::ObQueryCacheKey()
  : is_valid_(false),
    tenant_id_(0),
    database_id_(0)
{
}

ObQueryCacheKey::ObQueryCacheKey(const uint64_t tenant_id,
                              const uint64_t &database_id,
                              const ObString &sql,
                              const ObSQLSessionInfo &session)
{
  tenant_id_ = tenant_id;
  database_id_ = database_id;
  sql_ = sql;
  flag_.init(session);
  is_valid_ = true;
}

ObQueryCacheKey::~ObQueryCacheKey()
{
}

int ObQueryCacheKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = static_cast<uint64_t>(tenant_id_);
  hash_val = common::murmurhash(&database_id_, sizeof(database_id_), hash_val);
  hash_val = common::murmurhash(sql_.ptr(), sql_.length(), hash_val);
  flag_.hash(hash_val);
  return ret;
}

int ObQueryCacheKey::equal(const ObIKVCacheKey &other, bool &eq) const
{
  int ret = OB_SUCCESS;
  const ObQueryCacheKey &other_key = reinterpret_cast<const ObQueryCacheKey&>(other);

  eq = (tenant_id_ == other_key.tenant_id_);
  eq &= database_id_ == other_key.database_id_;
  eq &= sql_ == other_key.sql_;
  bool flag_eq;
  
  flag_.equal(other_key.flag_, flag_eq);
  eq &= flag_eq;
  return ret;
}

uint64_t ObQueryCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

int64_t ObQueryCacheKey::size() const
{
  return sizeof(*this) + sql_.length();
}

int ObQueryCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObQueryCacheKey *pkey = new (buf) ObQueryCacheKey();
    pkey->is_valid_ = is_valid_;
    pkey->tenant_id_ = tenant_id_;
    pkey->database_id_ = database_id_;
    memcpy(buf + sizeof(*this), sql_.ptr(), sql_.length());
    pkey->sql_ = ObString(sql_.length(), sql_.length(), buf + sizeof(*this));
    pkey->flag_ = flag_;
    key = pkey;
    if (OB_FAIL(ret)) {
      pkey->~ObQueryCacheKey();
    }
  }
  return ret;
}

/**
 * -----------------------------------------------------ObQueryCacheValue------------------------------------------------------
 */
ObQueryCacheValue::ObQueryCacheValue(common::ObIAllocator *alloc)
  : valid_(false),
    is_packed_(false),
    table_ids_(nullptr),
    trans_ids_(nullptr),
    ref_table_cnt_(0),
    mem_hold_(0),
    alloc_(alloc)
{
  fields_.set_allocator(alloc);
}

ObQueryCacheValue::~ObQueryCacheValue()
{
  for (int i = 0; i < row_array_.count(); ++i) {
    alloc_->free(row_array_.at(i));
  }
  fields_.reset();
}

int64_t ObQueryCacheValue::size() const
{
  return sizeof(*this) + sizeof(uint64_t) * ref_table_cnt_ * 2;
}

int ObQueryCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "ObQueryCacheValue : buf is null or buf len is small", K(ret));
  } else {
    ObQueryCacheValue *pvalue = new (buf) ObQueryCacheValue(alloc_);
    pvalue->valid_ = valid_;

    if (ref_table_cnt_ > 0) {
      // ref tables info need deep copy.
      pvalue->ref_table_cnt_ = ref_table_cnt_;
      pvalue->table_ids_ = (uint64_t *)(buf + sizeof(*pvalue));
      memcpy(pvalue->table_ids_, table_ids_, ref_table_cnt_ * sizeof(uint64_t));
      pvalue->trans_ids_ = (uint64_t *)(buf + sizeof(*pvalue) + ref_table_cnt_ * sizeof(uint64_t));
      memcpy(pvalue->trans_ids_, trans_ids_, ref_table_cnt_ * sizeof(uint64_t));
    }

    pvalue->weight_ = weight_;

    // deep copy for row_array
    // pvalue->row_array_ = row_array_;
    pvalue->row_array_.reserve(row_array_.count());
    for (int i = 0; i < row_array_.count() && OB_SUCC(ret); ++i) {
      const common::ObNewRow &row = *row_array_.at(i);
      int buf_size = row.get_deep_copy_size() + sizeof(common::ObNewRow);
      void *buf = pvalue->alloc_->alloc(buf_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        common::ObNewRow *cur_row = static_cast<common::ObNewRow *>(buf);
        char *deep_copy_buf = (char *)buf + sizeof(common::ObNewRow);
        int64_t pos = 0;
        cur_row->deep_copy(row, deep_copy_buf, row.get_deep_copy_size(), pos);
        pvalue->row_array_.push_back(cur_row);
      }
    }
 
    if (fields_.count() > 0
        && OB_FAIL(pl::ObDbmsInfo::deep_copy_field_columns(
                *(pvalue->alloc_),
                fields_,
                pvalue->fields_))) {
      COMMON_LOG(WARN, "deep copy ObQueryCacheValue fields error", K(ret));
    }
    // this->~ObQueryCacheValue();
    value = pvalue;
  }
  return ret;
}

uint64_t ObQueryCacheValue::get_table_id(uint32_t idx) const
{
  if (idx >= ref_table_cnt_) {
    return -1;
  }
  return table_ids_[idx];
}

uint64_t ObQueryCacheValue::get_trans_id(uint32_t idx) const
{
  if (idx >= ref_table_cnt_) {
    return -1;
  }
  return trans_ids_[idx];
}

int ObQueryCacheValue::add_row(const common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  int buf_size = row.get_deep_copy_size() + sizeof(common::ObNewRow);
  void *buf = alloc_->alloc(buf_size);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    common::ObNewRow *cur_row = static_cast<common::ObNewRow *>(buf);
    char *deep_copy_buf = (char *)buf + sizeof(common::ObNewRow);
    int64_t pos = 0;
    cur_row->deep_copy(row, deep_copy_buf, row.get_deep_copy_size(), pos);
    mem_hold_ += buf_size;
    row_array_.push_back(cur_row);
  }
  return ret;
}

int ObQueryCacheValue::get_row(const int64_t row_id, const common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = row_array_.at(row_id);
  return ret;
}

void ObQueryCacheValue::init_ref_table_info(uint32_t tables_num)
{
  ref_table_cnt_ = tables_num;
  uint64_t *p = (uint64_t *)alloc_->alloc(sizeof(uint64_t) * ref_table_cnt_ * 2);
  table_ids_ = p;
  trans_ids_ = p + sizeof(uint64_t) * ref_table_cnt_;
}

/**
 * -----------------------------------------------------ObQueryCache------------------------------------------------------
 */
thread_local bool is_thread_in_exit = false;

ObQueryCache::~ObQueryCache()
{
  is_thread_in_exit = true;
  if (is_valid()) {
    lib::ObDisableDiagnoseGuard disable_diagnose_guard;
    ob_delete(instance_);
    instance_ = nullptr;
  }
}


int ObQueryCache::query(const ObQueryCacheKey &key, ObQueryCacheValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObQueryCacheValue *value = NULL;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "ObQueryCache : key is invalid", K(ret));
  } else {   
    if (OB_FAIL(get(key, value, handle.handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        COMMON_LOG(WARN, "ObQueryCache : get failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ObQueryCache : value is null", K(ret));
    } else if (!value->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "ObQueryCache : value is invalid", K(ret));
    } else {
      handle.query_cache_value_ = const_cast<ObQueryCacheValue*>(value);
    }
  }
  return ret;
}

int ObQueryCache::insert(const ObQueryCacheKey &key,
                        const common::ColumnsFieldArray &fields,
                        ObQueryCacheValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObQueryCacheValue *value = NULL;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "ObQueryCache : key is invalid", K(ret));
  } else {
    // Only allow one thread insert query.
    ObQueryCacheValue tmp_value((&alloc_));
    // Only for deep copy.
    // tmp_value.set_valid(true);
    if (OB_FAIL(put_and_fetch(key, tmp_value, value, handle.handle_, 0))) {
      if (OB_ENTRY_EXIST != ret) {
        COMMON_LOG(WARN, "ObQueryCache : put and fetch schema cache failed", K(ret));
      }
    } else if (OB_FAIL(ret) || OB_ISNULL(value)) {
      // do nothing
    } else {
      handle.query_cache_value_ = const_cast<ObQueryCacheValue*>(value);
      if (OB_FAIL(pl::ObDbmsInfo::deep_copy_field_columns(alloc_,
                                                          fields,
                                                          handle.query_cache_value_->get_fields()))) {
        COMMON_LOG(WARN, "push query cache fields error", K(ret));
      }
      // handle.query_cache_value_->get_fields() = fields;
    }
  }
  return ret;
}

int ObQueryCache::remove(const ObQueryCacheKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(erase(key))) {
    COMMON_LOG(WARN, "ObQueryCache erase key error", K(ret), K(key));
  }
  return ret;
}

int ObQueryCache::eviction(const ObSQLSessionInfo &session) 
{
  int ret = OB_SUCCESS;
  const int64_t query_cache_size = 128 * 1024 * 1024; // 128MB
  // prevent to dead loop
  int max_try_time = 10000;
  const ObQueryCacheKey *key = NULL;
  const ObQueryCacheValue *value = NULL;
  ObKVCacheHandle handle;
  const ObQueryCacheKey *next_key = NULL;
  const ObQueryCacheValue *next_value = NULL;
  ObKVCacheHandle next_handle;
  while (get_size() > query_cache_size && --max_try_time) {
    ObKVCacheIterator iter;
    get_iterator(iter);
    if (OB_FAIL(iter.get_next_kvpair(key, value, handle))) {
      if (OB_ITER_END == ret) {
        // do nothing
      } else {
        COMMON_LOG(WARN, "ObQueryCache get next kvpair Unexpected error", K(ret));
      }
    } else {
      while (OB_SUCC(iter.get_next_kvpair(next_key, next_value, next_handle))) {
        if (next_value->get_weight() < value->get_weight()) {
          key = next_key;
          value = next_value;
        }
        
      }
      if (OB_ITER_END == ret) {
        // do nothing
      } else {
        COMMON_LOG(WARN, "ObQueryCache get next kvpair Unexpected error", K(ret));
      }   
      if (OB_FAIL(remove(*key))) {
        // do nothing
      } else {
        // update query cache stat
        row_mem_size_ -= value->get_row_mem_size();
        row_cnt_ -= value->get_row_cnt();
      }
    }
  }
  return ret;
}

int ObQueryCache::flush()
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

void ObQueryCache::debug_info()
{
  int hit_cnt = get_hit_cnt();
  int miss_cnt = get_miss_cnt();
  double hit_rate = get_hit_rate();
  int cache_cnt = count();
  COMMON_LOG(INFO, "ObQueryCache ", K_(row_mem_size), K_(row_cnt), K(cache_cnt),
                                    K(hit_cnt), K(miss_cnt), K(hit_rate));
}

ObQueryCache* ObQueryCache::get_instance()
{
  int ret = OB_SUCCESS;
  static ObQueryCache query_cache;
  if (OB_LIKELY(!query_cache.is_valid() && !is_thread_in_exit)) {
    query_cache.instance_ = (ObQueryCache*)PLACE_HOLDER;
    // add tenant
    ObMemAttr attr(ob_thread_tenant_id(), "query_cache");
    SET_USE_500(attr);
    query_cache.instance_ = OB_NEW(ObQueryCache, attr);
    query_cache.instance_->init("ob_query_cache", 5);
    // COMMON_LOG(INFO, "ObQueryCache : create instance sucessful");
  }
  return query_cache.instance_;
}

} // end namespace sql
} // end namespace oceanbase