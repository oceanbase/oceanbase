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

#ifndef _OB_TABLE_TABLE_H
#define _OB_TABLE_TABLE_H 1
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/net/ob_addr.h"
#include "common/ob_common_types.h"
#include "common/ob_range.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"

#include "share/table/ob_table_ttl_common.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/ob_role.h"
namespace oceanbase
{
namespace common
{
class ObNewRow;
}

namespace table
{
using common::ObString;
using common::ObRowkey;
using common::ObObj;
using common::ObIArray;
using common::ObSEArray;

////////////////////////////////////////////////////////////////
// structs of a table storage interface
////////////////////////////////////////////////////////////////

/// A Table Entity
class ObITableEntity: public common::ObDLinkBase<ObITableEntity>
{
  OB_UNIS_VERSION_V(1);
public:
  ObITableEntity()
      :alloc_(NULL)
  {}
  virtual ~ObITableEntity() = default;
  virtual void reset() = 0;
  virtual bool is_empty() const { return 0 == get_rowkey_size() && 0 == get_properties_count(); }
  //@{ primary key contains partition key. Note that all values are shallow copy.
  virtual int set_rowkey(const ObRowkey &rowkey) = 0;
  virtual int set_rowkey(const ObITableEntity &other) = 0;
  virtual int set_rowkey_value(int64_t idx, const ObObj &value) = 0;
  virtual int add_rowkey_value(const ObObj &value) = 0;
  virtual int64_t get_rowkey_size() const = 0;
  virtual int get_rowkey_value(int64_t idx, ObObj &value) const = 0;
  virtual ObRowkey get_rowkey() const = 0;
  virtual int64_t hash_rowkey() const = 0;
  //@}
  //@{ property is a key-value pair.
  virtual int set_property(const ObString &prop_name, const ObObj &prop_value) = 0;
  virtual int get_property(const ObString &prop_name, ObObj &prop_value) const = 0;
  virtual int get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const = 0; // @todo property iterator
  virtual int get_properties_names(ObIArray<ObString> &properties) const = 0;
  virtual int get_properties_values(ObIArray<ObObj> &properties_values) const = 0;
  virtual int64_t get_properties_count() const = 0;
  //@}
  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other);
  int deep_copy_rowkey(common::ObIAllocator &allocator, const ObITableEntity &other);
  int deep_copy_properties(common::ObIAllocator &allocator, const ObITableEntity &other);
  virtual int add_retrieve_property(const ObString &prop_name);
  void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  common::ObIAllocator *get_allocator() { return alloc_; }
  VIRTUAL_TO_STRING_KV("ITableEntity", "");
protected:
  common::ObIAllocator *alloc_;  // for deep copy in deserialize
};

class ObITableEntityFactory
{
public:
  virtual ~ObITableEntityFactory() = default;
  virtual ObITableEntity *alloc() = 0;
  virtual void free(ObITableEntity *obj) = 0;
  virtual void free_and_reuse() = 0;
  virtual int64_t get_used_count() const = 0;
  virtual int64_t get_free_count() const = 0;
  virtual int64_t get_used_mem() const = 0;
  virtual int64_t get_total_mem() const = 0;
};

/// An implementation for ObITableEntity
class ObTableEntity: public ObITableEntity
{
public:
  ObTableEntity();
  ~ObTableEntity();
  virtual int set_rowkey(const ObRowkey &rowkey) override;
  virtual int set_rowkey(const ObITableEntity &other) override;
  virtual int set_rowkey_value(int64_t idx, const ObObj &value) override;
  virtual int add_rowkey_value(const ObObj &value) override;
  virtual int64_t get_rowkey_size() const override { return rowkey_.count(); };
  virtual int get_rowkey_value(int64_t idx, ObObj &value) const override;
  virtual int64_t hash_rowkey() const override;
  virtual int get_property(const ObString &prop_name, ObObj &prop_value) const override;
  virtual int set_property(const ObString &prop_name, const ObObj &prop_value) override;
  virtual int get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const override;
  virtual int get_properties_names(ObIArray<ObString> &properties_names) const override;
  virtual int get_properties_values(ObIArray<ObObj> &properties_values) const override;
  virtual int64_t get_properties_count() const override;
  virtual void reset() override;
  virtual ObRowkey get_rowkey() const override;
  const ObIArray<ObString> &get_properties_names() const { return properties_names_; }
  const ObIArray<ObObj> &get_properties_values() const { return properties_values_; }
  const ObSEArray<ObObj, 8> &get_rowkey_objs() const { return rowkey_; };
  DECLARE_TO_STRING;
private:
  bool has_exist_in_properties(const ObString &name, int64_t *idx = nullptr) const;
private:
  ObSEArray<ObObj, 8> rowkey_;
  ObSEArray<ObString, 8> properties_names_;
  ObSEArray<ObObj, 8> properties_values_;
};

enum class ObTableEntityType
{
  ET_DYNAMIC = 0,
  ET_KV = 1,
  ET_HKV = 2
};

// @note not thread-safe
template <typename T>
class ObTableEntityFactory: public ObITableEntityFactory
{
public:
  ObTableEntityFactory(const char *label = common::ObModIds::TABLE_PROC, uint64_t tenant_id = OB_SERVER_TENANT_ID)
      :alloc_(label, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id)
  {}
  virtual ~ObTableEntityFactory();
  virtual ObITableEntity *alloc() override;
  virtual void free(ObITableEntity *obj) override;
  virtual void free_and_reuse() override;
  virtual int64_t get_free_count() const { return free_list_.get_size(); }
  virtual int64_t get_used_count() const { return used_list_.get_size(); }
  virtual int64_t get_used_mem() const { return alloc_.used(); }
  virtual int64_t get_total_mem() const { return alloc_.total(); }
private:
  void free_all();
private:
  common::ObArenaAllocator alloc_;
  common::ObDList<ObITableEntity> used_list_;
  common::ObDList<ObITableEntity> free_list_;
};

template <typename T>
ObTableEntityFactory<T>::~ObTableEntityFactory()
{
  free_all();
}

template <typename T>
ObITableEntity *ObTableEntityFactory<T>::alloc()
{
  ObITableEntity *entity = free_list_.remove_first();
  if (NULL == entity) {
    void * ptr = alloc_.alloc(sizeof(T));
    if (NULL == ptr) {
      CLIENT_LOG_RET(WARN, common::OB_ALLOCATE_MEMORY_FAILED, "no memory for table entity");
    } else {
      entity = new(ptr) T();
      used_list_.add_last(entity);
    }
  } else {
    used_list_.add_last(entity);
  }
  return entity;
}

template <typename T>
void ObTableEntityFactory<T>::free(ObITableEntity *entity)
{
  if (NULL != entity) {
    entity->reset();
    entity->set_allocator(NULL);
    used_list_.remove(entity);
    free_list_.add_last(entity);
  }
}

template <typename T>
void ObTableEntityFactory<T>::free_and_reuse()
{
  while (!used_list_.is_empty()) {
    this->free(used_list_.get_first());
  }
}

template <typename T>
void ObTableEntityFactory<T>::free_all()
{
  ObITableEntity *entity = NULL;
  while (NULL != (entity = used_list_.remove_first())) {
    entity->~ObITableEntity();
  }
  while (NULL != (entity = free_list_.remove_first())) {
    entity->~ObITableEntity();
  }
}

enum class ObQueryOperationType : int {
  QUERY_START = 0,
  QUERY_NEXT = 1,
  QUERY_MAX
};

/// Table Operation Type
struct ObTableOperationType
{
  enum Type
  {
    GET = 0,
    INSERT = 1,
    DEL = 2,
    UPDATE = 3,
    INSERT_OR_UPDATE = 4,
    REPLACE = 5,
    INCREMENT = 6,
    APPEND = 7,
    SCAN = 8,
    TTL = 9, // internal type for ttl executor cache key
    INVALID = 15
  };
};

/// A table operation
class ObTableOperation
{
  OB_UNIS_VERSION(1);
public:
  /**
   * insert the entity.
   * @return ObTableOperationResult
   * In the case of insert success, the return errno is OB_SUCCESS, affected_rows is 1
   * In the case of insert failed, the affected_rows is 0
   * In the case of insert failed caused by primary key duplicated, the errno is OB_ERR_PRIMARY_KEY_DUPLICATE.
   * If the option returning_affected_rows is false (default value), then the return value of affected_rows is undefined, but with better performance.
   * Other common error code: OB_TIMEOUT indicates time out; OB_TRY_LOCK_ROW_CONFLICT indicate row lock conflict
   */
  static ObTableOperation insert(const ObITableEntity &entity);
  /**
   * delete the entity.
   * @return ObTableOperationResult
   * In the case of delete success, the errno is OB_SUCCESS and the affeceted_row is 1.
   * In the case of the row is NOT EXIST, the errno is OB_SUCCESS and the affected_row is 0.
   * If the option returning_affected_rows is false (default value), then the return value of affected_rows is undefined, but with better performance.
   * Other common error code: OB_TIMEOUT indicates time out; OB_TRY_LOCK_ROW_CONFLICT indicate row lock conflict
   */
  static ObTableOperation del(const ObITableEntity &entity);
  /**
   * update the entity.
   * @return ObTableOperationResult
   * In the case of update success, the errno is OB_SUCCESS and the affeceted_row is 1.
   * In the case of the row is NOT EXIST, the errno is OB_SUCCESS and the affected_row is 0.
   * If the option returning_affected_rows is false (default value), then the return value of affected_rows is undefined, but with better performance.
   * Other common error code: OB_TIMEOUT indicates time out; OB_TRY_LOCK_ROW_CONFLICT indicate row lock conflict
   */
  static ObTableOperation update(const ObITableEntity &entity);
  /**
   * insert_or_update the entity.
   * @return ObTableOperationResult
   * If the row is NOT exist, then insert the row. In the case of success, the return errno is OB_SUCCESS and the affected_rows is 1.
   * If the row is exist, then update the row. In the case of success, the return errno is OB_SUCCESS and the affected_rows i 1.
   * If the option returning_affected_rows is false (default value), then the return value of affected_rows is undefined, but with better performance.
   * Other common error code: OB_TIMEOUT; OB_TRY_LOCK_ROW_CONFLICT
   */
  static ObTableOperation insert_or_update(const ObITableEntity &entity);
  /**
   * replace the entity.
   * @return ObTableOperationResult
   * If the row is NOT EXIST, then insert the row. In the case of success,
   * the errno is OB_SUCCESS and the affected_row is 1.
   * Otherwise the row is EXIST, then delete the old row and insert the new row. In the case of success,
   * the errno is OB_SUCCESS and the affected_row is 1.
   * Specially, if there is uniq index conflict, then delete all rows cause conflict and insert the new row.
   * In the case of success, the errno is OB_SUCCESS and the affected_row >= 1.
   * If the option returning_affected_rows is false (default value), then the return value of affected_rows is undefined, but with better performance.
   * Other common error code: OB_TIMEOUT; OB_TRY_LOCK_ROW_CONFLICT
   */
  static ObTableOperation replace(const ObITableEntity &entity);
  /**
   * retrieve the entity.
   * @param entity Only return the given property
   * @return ObTableOperationResult
   * affected_rows is always 0
   * If the row is exist, then return the ObTableOperationResult.entity
   * Otherwise, entity is empty.
   * Other common error code: OB_TIMEOUT
   */
  static ObTableOperation retrieve(const ObITableEntity &entity);
  /**
   * Increase the value of given column.
   * The type of the column MUST be integer.
   * If the original value of given column is NULL, use the new value to replace it.
   */
  static ObTableOperation increment(const ObITableEntity &entity);
  /**
   * Append the given string to original string.
   * The type of the column MUST be string type, such as char, varchar, binary, varbinary or lob.
   * If the original value of given column is NULL, use the new value to replace it.
   */
  static ObTableOperation append(const ObITableEntity &entity);
public:
  const ObITableEntity &entity() const { return *entity_; }
  ObTableOperationType::Type type() const { return operation_type_; }
  void set_entity(const ObITableEntity &entity) { entity_ = &entity; }
  void set_type(ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  int get_entity(ObITableEntity *&entity);
  uint64_t get_checksum();
  TO_STRING_KV(K_(operation_type), "entity", to_cstring(entity_));
private:
  const ObITableEntity *entity_;
  ObTableOperationType::Type operation_type_;
};

class ObTableTTLOperation
{
public:
  ObTableTTLOperation(uint64_t tenant_id, uint64_t table_id, const ObTTLTaskParam &para,
                      uint64_t del_row_limit, ObRowkey start_rowkey)
  : tenant_id_(tenant_id), table_id_(table_id), max_version_(para.max_version_),
    time_to_live_(para.ttl_), is_htable_(para.is_htable_), del_row_limit_(del_row_limit),
    start_rowkey_(start_rowkey)
  {}

  ~ObTableTTLOperation() {}
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && common::OB_INVALID_ID != table_id_ &&
           (!is_htable_ || max_version_ > 0 || time_to_live_ > 0) && del_row_limit_ > 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(max_version),  K_(time_to_live), K_(is_htable), K_(del_row_limit), K_(start_rowkey));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int32_t max_version_;
  int32_t time_to_live_;
  bool is_htable_;
  uint64_t del_row_limit_;
  ObRowkey start_rowkey_;
};

/// common result for ObTable
class ObTableResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableResult()
      :errno_(common::OB_ERR_UNEXPECTED)
  {
    sqlstate_[0] = '\0';
    msg_[0] = '\0';
  }
  ~ObTableResult() = default;
  void set_errno(int err) { errno_ = err; }
  int get_errno() const { return errno_; }
  int assign(const ObTableResult &other);
  void reset()
  {
    errno_ = common::OB_ERR_UNEXPECTED;
    sqlstate_[0] = '\0';
    msg_[0] = '\0';
  }
  TO_STRING_KV(K_(errno));
private:
  static const int64_t MAX_MSG_SIZE = common::OB_MAX_ERROR_MSG_LEN;
protected:
  int32_t errno_;
  char sqlstate_[6];  // terminate with '\0'
  char msg_[MAX_MSG_SIZE]; // terminate with '\0'
};

/// result for ObTableOperation
class ObTableOperationResult final: public ObTableResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableOperationResult();
  ~ObTableOperationResult() = default;
  void reset();
  ObTableOperationType::Type type() const { return operation_type_; }
  int get_entity(const ObITableEntity *&entity) const;
  int get_entity(ObITableEntity *&entity);
  ObITableEntity *get_entity() { return entity_; }
  int64_t get_affected_rows() const { return affected_rows_; }

  void set_entity(ObITableEntity &entity) { entity_ = &entity; }
  void set_type(ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }

  int deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableOperationResult &other);
  DECLARE_TO_STRING;
private:
  ObTableOperationType::Type operation_type_;
  ObITableEntity *entity_;
  int64_t affected_rows_;
};

class ObIRetryPolicy
{
public:
  virtual bool need_retry(int32_t curr_retry_count, int last_errno, int64_t &retry_interval)
  {
    UNUSEDx(curr_retry_count, last_errno, retry_interval);
    return false;
  }
};

class ObLinearRetry : public ObIRetryPolicy
{};

class ObExponentialRetry : public ObIRetryPolicy
{};

class ObNoRetry : public ObIRetryPolicy
{};

/// consistency levels
enum class ObTableConsistencyLevel
{
  STRONG = 0,
  EVENTUAL = 1,
};
/// clog row image type
/// @see share::ObBinlogRowImage
enum class ObBinlogRowImageType
{
  MINIMAL = 0,
  NOBLOB = 1,
  FULL = 2,
};
/// request options for all the table operations
class ObTableRequestOptions final
{
public:
  ObTableRequestOptions();
  ~ObTableRequestOptions() = default;

  void set_consistency_level(ObTableConsistencyLevel consistency_level) { consistency_level_ = consistency_level; }
  ObTableConsistencyLevel consistency_level() const { return consistency_level_; }
  void set_server_timeout(int64_t server_timeout_us) { server_timeout_us_ = server_timeout_us; }
  int64_t server_timeout() const { return server_timeout_us_; }
  void set_execution_time(int64_t max_execution_time_us) { max_execution_time_us_ = max_execution_time_us; }
  int64_t max_execution_time() const { return max_execution_time_us_; }
  void set_retry_policy(ObIRetryPolicy *retry_policy) { retry_policy_ = retry_policy; }
  ObIRetryPolicy* retry_policy() { return retry_policy_; }
  void set_returning_affected_rows(bool returning) { returning_affected_rows_ = returning; }
  bool returning_affected_rows() const { return returning_affected_rows_; }
  void set_returning_rowkey(bool returning) { returning_rowkey_ = returning; }
  bool returning_rowkey() const { return returning_rowkey_; }
  void set_returning_affected_entity(bool returning) { returning_affected_entity_ = returning; }
  bool returning_affected_entity() const { return returning_affected_entity_; }
  void set_batch_operation_as_atomic(bool atomic) { batch_operation_as_atomic_ = atomic; }
  bool batch_operation_as_atomic() const { return batch_operation_as_atomic_; }
  void set_binlog_row_image_type(ObBinlogRowImageType type) { binlog_row_image_type_ = type; }
  ObBinlogRowImageType binlog_row_image_type() const { return binlog_row_image_type_; }
private:
  ObTableConsistencyLevel consistency_level_;
  int64_t server_timeout_us_;
  int64_t max_execution_time_us_;
  ObIRetryPolicy *retry_policy_;
  bool returning_affected_rows_;  // default: false
  bool returning_rowkey_;         // default: false
  bool returning_affected_entity_;  // default: false
  bool batch_operation_as_atomic_;  // default: false
  // int route_policy
  ObBinlogRowImageType binlog_row_image_type_;  // default: FULL
};

/// A batch operation
class ObTableBatchOperation
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MAX_BATCH_SIZE = 1000;
  static const int64_t COMMON_BATCH_SIZE = 8;
public:
  ObTableBatchOperation()
      :table_operations_(common::ObModIds::TABLE_BATCH_OPERATION, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
       is_readonly_(true),
       is_same_type_(true),
       is_same_properties_names_(true),
       entity_factory_(NULL)
  {}
  ~ObTableBatchOperation() = default;
  void reset();
  void set_entity_factory(ObITableEntityFactory *entity_factory) { entity_factory_ = entity_factory; }
  /// insert the entity if not exists
  int insert(const ObITableEntity &entity);
  /// delete the entity if exists
  int del(const ObITableEntity &entity);
  /// update the entity if exists
  int update(const ObITableEntity &entity);
  /// insert the entity if not exists, otherwise update it
  int insert_or_update(const ObITableEntity &entity);
  /// insert the entity if not exists, otherwise replace it
  int replace(const ObITableEntity &entity);
  /// get the entity if exists
  int retrieve(const ObITableEntity &entity);
  /// add one table operation
  int add(const ObTableOperation &table_operation);
  /// increment the value
  int increment(const ObITableEntity &entity);
  /// append to the value
  int append(const ObITableEntity &entity);

  int64_t count() const { return table_operations_.count(); }
  const ObTableOperation &at(int64_t idx) const { return table_operations_.at(idx); }
  bool is_readonly() const { return is_readonly_; }
  bool is_same_type() const { return is_same_type_; }
  bool is_same_properties_names() const { return is_same_properties_names_; }
  uint64_t get_checksum();
  TO_STRING_KV(K_(is_readonly),
               K_(is_same_type),
               K_(is_same_properties_names),
               "operatiton_count", table_operations_.count(),
               K_(table_operations));
private:
  ObSEArray<ObTableOperation, COMMON_BATCH_SIZE> table_operations_;
  bool is_readonly_;
  bool is_same_type_;
  bool is_same_properties_names_;
  // do not serialize
  ObITableEntityFactory *entity_factory_;
};

/// result for ObTableBatchOperation
typedef ObIArray<ObTableOperationResult> ObITableBatchOperationResult;
class ObTableBatchOperationResult: public common::ObSEArrayImpl<ObTableOperationResult, ObTableBatchOperation::COMMON_BATCH_SIZE>
{
  OB_UNIS_VERSION(1);
public:
  ObTableBatchOperationResult()
      :BaseType(common::ObModIds::TABLE_BATCH_OPERATION_RESULT, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
       entity_factory_(NULL),
       alloc_(NULL)
  {}
  virtual ~ObTableBatchOperationResult() = default;
  void set_entity_factory(ObITableEntityFactory *entity_factory) { entity_factory_ = entity_factory; }
  ObITableEntityFactory *get_entity_factory() { return entity_factory_; }
  void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  common::ObIAllocator *get_allocator() { return alloc_; }
private:
  typedef common::ObSEArrayImpl<ObTableOperationResult, ObTableBatchOperation::COMMON_BATCH_SIZE> BaseType;
  ObITableEntityFactory *entity_factory_;
  common::ObIAllocator *alloc_;
};

class ObHTableConstants
{
public:
  static constexpr int64_t LATEST_TIMESTAMP = -INT64_MAX;
  static constexpr int64_t OLDEST_TIMESTAMP = INT64_MAX;
  static constexpr int64_t INITIAL_MIN_STAMP = 0;
  static constexpr int64_t INITIAL_MAX_STAMP = INT64_MAX;

  static const char* const ROWKEY_CNAME;
  static const char* const CQ_CNAME;
  static const char* const VERSION_CNAME;
  static const char* const VALUE_CNAME;
  static const ObString ROWKEY_CNAME_STR;
  static const ObString CQ_CNAME_STR;
  static const ObString VERSION_CNAME_STR;
  static const ObString VALUE_CNAME_STR;

  // create table t1$cf1 (K varbinary(1024), Q varchar(256), T bigint, V varbinary(1024), primary key(K, Q, T));
  static const int64_t COL_IDX_K = 0;
  static const int64_t COL_IDX_Q = 1;
  static const int64_t COL_IDX_T = 2;
  static const int64_t COL_IDX_V = 3;
private:
  ObHTableConstants() = delete;
};

/// special filter for HTable
class ObHTableFilter final
{
  OB_UNIS_VERSION(1);
public:
  ObHTableFilter();
  ~ObHTableFilter() = default;
  void reset();
  void set_valid(bool valid) { is_valid_ = valid; }
  bool is_valid() const { return is_valid_; }

  /// Get the column with the specified qualifier.
  int add_column(const ObString &qualifier);
  /// Get versions of columns with the specified timestamp.
  int set_timestamp(int64_t timestamp) { min_stamp_ = timestamp; max_stamp_ = timestamp + 1; return common::OB_SUCCESS; }
  /// Get versions of columns only within the specified timestamp range, [minStamp, maxStamp).
  int set_time_range(int64_t min_stamp, int64_t max_stamp);
  /// Get up to the specified number of versions of each column.
  int set_max_versions(int32_t versions);
  /// Set the maximum number of values to return per row per Column Family
  /// @param limit - the maximum number of values returned / row / CF
  int set_max_results_per_column_family(int32_t limit);
  /// Set offset for the row per Column Family.
  /// @param offset - is the number of kvs that will be skipped.
  int set_row_offset_per_column_family(int32_t offset);
  /// Apply the specified server-side filter when performing the Query.
  /// @param filter - a file string using the hbase filter language
  /// @see the filter language at https://issues.apache.org/jira/browse/HBASE-4176
  int set_filter(const ObString &filter);

  const ObIArray<ObString> &get_columns() const { return select_column_qualifier_; }
  bool with_latest_timestamp() const { return with_all_time() && 1 == max_versions_; }
  bool with_timestamp() const { return min_stamp_ == max_stamp_ && min_stamp_ >= 0; }
  bool with_all_time() const { return ObHTableConstants::INITIAL_MIN_STAMP == min_stamp_ && ObHTableConstants::INITIAL_MAX_STAMP == max_stamp_; }
  int64_t get_min_stamp() const { return min_stamp_; }
  int64_t get_max_stamp() const { return max_stamp_; }
  int32_t get_max_versions() const { return max_versions_; }
  int32_t get_max_results_per_column_family() const { return limit_per_row_per_cf_; }
  int32_t get_row_offset_per_column_family() const { return offset_per_row_per_cf_; }
  const ObString &get_filter() const { return filter_string_; }
  void clear_columns() { select_column_qualifier_.reset(); }
  uint64_t get_checksum() const;
  int deep_copy(ObIAllocator &allocator, ObHTableFilter &dst) const;

  TO_STRING_KV(K_(is_valid),
               "column_qualifier", select_column_qualifier_,
               K_(min_stamp),
               K_(max_stamp),
               K_(max_versions),
               K_(limit_per_row_per_cf),
               K_(offset_per_row_per_cf),
               K_(filter_string));
private:
  bool is_valid_;
  ObSEArray<ObString, 16> select_column_qualifier_;
  int64_t min_stamp_;  // default -1
  int64_t max_stamp_;  // default -1
  int32_t max_versions_;  // default 1
  int32_t limit_per_row_per_cf_;  // default -1 means unlimited
  int32_t offset_per_row_per_cf_; // default 0
  ObString filter_string_;
};

enum ObTableAggregationType
{
  INVAILD = 0,
  MAX = 1,
  MIN = 2,
  COUNT = 3,
  SUM = 4,
  AVG = 5,
};

class ObTableAggregation
{
  OB_UNIS_VERSION(1);
public:
  ObTableAggregation()
      : type_(ObTableAggregationType::INVAILD),
        column_()
  {}
  ObTableAggregationType get_type() const { return type_; }
  const common::ObString &get_column() const { return column_; }
  bool is_agg_all_column() const { return column_ == "*"; };
  int deep_copy(common::ObIAllocator &allocator, ObTableAggregation &dst) const;
  TO_STRING_KV(K_(type), K_(column));
private:
  ObTableAggregationType type_; // e.g. max
  common::ObString column_; // e.g. age
};

/// A table query
/// 1. support multi range scan
/// 2. support reverse scan
/// 3. support secondary index scan
class ObTableQuery final
{
  OB_UNIS_VERSION(1);
public:
  ObTableQuery()
      :deserialize_allocator_(NULL),
      key_ranges_(),
      select_columns_(),
      filter_string_(),
      limit_(-1),
      offset_(0),
      scan_order_(common::ObQueryFlag::Forward),
      index_name_(),
      batch_size_(-1),
      max_result_size_(-1),
      htable_filter_(),
      scan_range_columns_(),
      aggregations_()
  {}
  ~ObTableQuery() = default;
  void reset();
  bool is_valid() const;

  /// add a scan range, the number of scan ranges should be >=1.
  int add_scan_range(common::ObNewRange &scan_range);
  /// Scan order: Forward (By default) and Reverse.
  int set_scan_order(common::ObQueryFlag::ScanOrder scan_order);
  /// Set the index to scan, could be 'PRIMARY' (by default) or any other secondary index.
  int set_scan_index(const ObString &index_name);
  /// Add select columns.
  int add_select_column(const ObString &columns);
  /// Set the max rows to return. The value of -1 represents there is NO limit. The default value is -1.
  /// For htable, set the limit of htable rows for this scan.
  int set_limit(int32_t limit);
  /// Set the offset to return. The default value is 0.
  int set_offset(int32_t offset);
  /// Add filter, currently NOT supported.
  int set_filter(const ObString &filter);
  /// Add filter only for htable.
  ObHTableFilter& htable_filter() { return htable_filter_; }
  /// Set max row count of each batch.
  /// For htable, set the maximum number of cells to return for each call to next().
  int set_batch(int32_t batch_size);
  /// Set the maximum result size.
  /// The default is -1; this means that no specific maximum result size will be set for this query.
  /// @param max_result_size - The maximum result size in bytes.
  int set_max_result_size(int64_t max_result_size);

  const ObIArray<ObString> &get_select_columns() const { return select_columns_; }
  const ObIArray<common::ObNewRange> &get_scan_ranges() const { return key_ranges_; }
  int32_t get_limit() const { return limit_; }
  int32_t get_offset() const { return offset_; }
  common::ObQueryFlag::ScanOrder get_scan_order() const { return scan_order_; }
  const ObString &get_index_name() const { return index_name_; }
  const ObHTableFilter& get_htable_filter() const { return htable_filter_; }
  int32_t get_batch() const { return batch_size_; }
  int64_t get_max_result_size() const { return max_result_size_; }
  int64_t get_range_count() const { return key_ranges_.count(); }
  uint64_t get_checksum() const;
  const ObString &get_filter_string() const { return filter_string_; }
  void clear_scan_range() { key_ranges_.reset(); }
  void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_allocator_ = allocator; }
  int deep_copy(ObIAllocator &allocator, ObTableQuery &dst) const;
  const common::ObIArray<ObTableAggregation> &get_aggregations() const { return aggregations_; }
  bool is_aggregate_query() const { return !aggregations_.empty(); }
  TO_STRING_KV(K_(key_ranges),
               K_(select_columns),
               K_(filter_string),
               K_(limit),
               K_(offset),
               K_(scan_order),
               K_(index_name),
               K_(batch_size),
               K_(max_result_size),
               K_(htable_filter),
               K_(scan_range_columns),
               K_(aggregations));
public:
  static ObString generate_filter_condition(const ObString &column, const ObString &op, const ObObj &value);
  static ObString combile_filters(const ObString &filter1, const ObString &op, const ObString &filter2);
  static common::ObNewRange generate_prefix_scan_range(const ObRowkey &rowkey_prefix);
private:
  common::ObIAllocator *deserialize_allocator_;
  ObSEArray<common::ObNewRange, 16> key_ranges_;
  ObSEArray<ObString, 16> select_columns_;
  ObString filter_string_;
  int32_t limit_;  // default -1 means unlimited
  int32_t offset_;
  common::ObQueryFlag::ScanOrder scan_order_;
  ObString index_name_;
  int32_t batch_size_;
  int64_t max_result_size_;
  ObHTableFilter htable_filter_;
  ObSEArray<ObString, 8> scan_range_columns_;
  ObSEArray<ObTableAggregation, 8> aggregations_;
};

/// result for ObTableQuery
class ObTableEntityIterator
{
public:
  ObTableEntityIterator() = default;
  virtual ~ObTableEntityIterator();
  /**
   * fetch the next entity
   * @return OB_ITER_END when finished
   */
  virtual int get_next_entity(const ObITableEntity *&entity) = 0;
};

/// query and mutate the selected rows.
class ObTableQueryAndMutate final
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryAndMutate()
      :return_affected_entity_(true)
  {}
  const ObTableQuery &get_query() const { return query_; }
  ObTableQuery &get_query() { return query_; }
  const ObTableBatchOperation &get_mutations() const { return mutations_; }
  ObTableBatchOperation &get_mutations() { return mutations_; }
  bool return_affected_entity() const { return return_affected_entity_; }

  void set_deserialize_allocator(common::ObIAllocator *allocator);
  void set_entity_factory(ObITableEntityFactory *entity_factory);
  uint64_t get_checksum();

  TO_STRING_KV(K_(query),
               K_(mutations));
private:
  ObTableQuery query_;
  ObTableBatchOperation mutations_;
  bool return_affected_entity_;
};

inline void ObTableQueryAndMutate::set_deserialize_allocator(common::ObIAllocator *allocator)
{
  query_.set_deserialize_allocator(allocator);
}

inline void ObTableQueryAndMutate::set_entity_factory(ObITableEntityFactory *entity_factory)
{
  mutations_.set_entity_factory(entity_factory);
}

class ObTableQueryResult: public ObTableEntityIterator
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryResult();
  virtual ~ObTableQueryResult() {}
  void reset();
  void reset_except_property();
  void rewind();
  virtual int get_next_entity(const ObITableEntity *&entity) override;
  int add_property_name(const ObString &name);
  int assign_property_names(const common::ObIArray<common::ObString> &other);
  // for aggregation
  int deep_copy_property_names(const common::ObIArray<common::ObString> &other);
  void reset_property_names() { properties_names_.reset(); }
  int add_row(const common::ObNewRow &row);
  int add_row(const common::ObIArray<ObObj> &row);
  int add_all_property(const ObTableQueryResult &other);
  int add_all_row(const ObTableQueryResult &other);
  int64_t get_row_count() const { return row_count_; }
  int64_t get_property_count() const { return properties_names_.count(); }
  int64_t get_result_size();
  int get_first_row(common::ObNewRow &row) const;
  bool reach_batch_size_or_result_size(const int32_t batch_count,
                                       const int64_t max_result_size);
  const common::ObIArray<common::ObString>& get_select_columns() const { return properties_names_; };
  static int64_t get_max_packet_buffer_length() { return obrpc::get_max_rpc_packet_size() - (1<<20); }
  static int64_t get_max_buf_block_size() { return get_max_packet_buffer_length() - (1024*1024LL); }
private:
  static const int64_t DEFAULT_BUF_BLOCK_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE - (1024*1024LL);
  int alloc_buf_if_need(const int64_t size);
private:
  common::ObSEArray<ObString, 16> properties_names_;  // serialize
  int64_t row_count_;                                 // serialize
  common::ObDataBuffer buf_;                          // serialize
  common::ObArenaAllocator allocator_;
  int64_t fixed_result_size_;
  // for deserialize and read
  int64_t curr_idx_;
  ObTableEntity curr_entity_;
};

class ObTableQueryAndMutateResult final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(affected_rows));
public:
  int64_t affected_rows_;
  // If return_affected_entity_ in ObTableQueryAndMutate is set, then return the respond entity.
  // In the case of delete and insert_or_update, return the old rows before modified.
  // In the case of increment and append, return the new rows after modified.
  ObTableQueryResult affected_entity_;
};

class ObTableQuerySyncResult: public ObTableQueryResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableQuerySyncResult()
    : is_end_(false),
      query_session_id_(0)
  {}
  virtual ~ObTableQuerySyncResult() {}
public:
  bool     is_end_;
  uint64_t  query_session_id_; // from server gen
};

struct ObTableApiCredential final
{
  OB_UNIS_VERSION(1);
public:
  ObTableApiCredential();
  ~ObTableApiCredential();
public:
  int64_t cluster_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  int64_t expire_ts_;
  uint64_t hash_val_;
public:
  int hash(uint64_t &hash_val, uint64_t seed = 0) const;
  TO_STRING_KV(K_(cluster_id),
               K_(tenant_id),
               K_(user_id),
               K_(database_id),
               K_(expire_ts),
               K_(hash_val));
};

/// Table Direct Load Operation Type
enum class ObTableDirectLoadOperationType {
  BEGIN = 0,
  COMMIT = 1,
  ABORT = 2,
  GET_STATUS = 3,
  INSERT = 4,
  HEART_BEAT = 5,
  MAX_TYPE
};

class ObTableTTLOperationResult
{
public:
  ObTableTTLOperationResult()
    : ttl_del_rows_(0),
      max_version_del_rows_(0),
      scan_rows_(0),
      end_rowkey_()
    {}
  ~ObTableTTLOperationResult() {}
  uint64_t get_ttl_del_row() { return ttl_del_rows_; }
  uint64_t get_max_version_del_row() { return max_version_del_rows_; }
  uint64_t get_del_row() { return ttl_del_rows_ + max_version_del_rows_; }
  uint64_t get_scan_row() { return scan_rows_; }
  common::ObString get_end_rowkey() { return end_rowkey_; }
  TO_STRING_KV(K_(ttl_del_rows), K_(max_version_del_rows), K_(scan_rows), K_(end_rowkey));
public:
  uint64_t ttl_del_rows_;
  uint64_t max_version_del_rows_;
  uint64_t scan_rows_;
  common::ObString end_rowkey_;
};

struct ObTableMoveReplicaInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTableMoveReplicaInfo()
      : table_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
        role_(common::ObRole::INVALID_ROLE),
        replica_type_(common::ObReplicaType::REPLICA_TYPE_MAX),
        part_renew_time_(0),
        reserved_(0)
  {}
  virtual ~ObTableMoveReplicaInfo() {}
  TO_STRING_KV(K_(table_id),
               K_(schema_version),
               K_(part_renew_time),
               K_(tablet_id),
               K_(server),
               K_(role),
               K_(replica_type),
               K_(reserved));
  OB_INLINE void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  OB_INLINE void set_schema_version(const uint64_t schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
public:
  uint64_t table_id_;
  uint64_t schema_version_;
  common::ObTabletID tablet_id_;
  common::ObAddr server_;
  common::ObRole role_;
  common::ObReplicaType replica_type_;
  int64_t part_renew_time_;
  uint64_t reserved_;
};

class ObTableMoveResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableMoveResult()
      : reserved_(0)
  {}
  virtual ~ObTableMoveResult() {}
  TO_STRING_KV(K_(replica_info),
               K_(reserved));

  OB_INLINE ObTableMoveReplicaInfo& get_replica_info() { return replica_info_; }
private:
  ObTableMoveReplicaInfo replica_info_;
  uint64_t reserved_;
};


} // end namespace table
} // end namespace oceanbase


#endif /* _OB_TABLE_TABLE_H */
