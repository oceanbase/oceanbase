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
#include "lib/list/ob_dlink_node.h"
#include "lib/net/ob_addr.h"
#include "common/ob_common_types.h"
#include "common/ob_range.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"

#include "share/table/ob_table_ttl_common.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/ob_role.h"
#include "common/row/ob_row.h"
#include "lib/oblog/ob_warning_buffer.h"
namespace oceanbase
{
namespace common
{
class ObNewRow;
}

namespace table
{

#define OB_TABLE_OPTION_DEFAULT INT64_C(0)
#define OB_TABLE_OPTION_RETURNING_ROWKEY (INT64_C(1) << 0)
#define OB_TABLE_OPTION_USE_PUT (INT64_C(1) << 1)
#define OB_TABLE_OPTION_RETURN_ONE_RES (INT64_C(1) << 2)

using common::ObString;
using common::ObRowkey;
using common::ObObj;
using common::ObIArray;
using common::ObSEArray;

////////////////////////////////////////////////////////////////
// structs of a table storage interface
////////////////////////////////////////////////////////////////
enum class ObTableGroupRwMode
{
  ALL = 0,
  READ = 1,
  WRITE = 2
};


enum class ObTableEntityType
{
  ET_DYNAMIC = 0,
  ET_KV = 1,
  ET_HKV = 2,
  ET_REDIS = 3
};
class ObHTableCellEntity;

class ObTableBitMap {
public:
  typedef uint8_t size_type;
  static const size_type SIZE_TYPE_MAX = UINT8_MAX;
  static const size_type BYTES_PER_BLOCK = sizeof(size_type);   // 1
  static const size_type BITS_PER_BLOCK = BYTES_PER_BLOCK * 8;  // 1 * 8 = 8
  static const size_type BLOCK_MOD_BITS = 3;                    // 2^3 = 8

public:
  ObTableBitMap()
    : block_count_(-1),
      valid_bits_num_(0)
  {
    datas_.set_attr(ObMemAttr(MTL_ID(), "TableBitMapData"));
  };
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size() const;
  // init bitmap wihtout allocating any blocks in datas_ (datas_.count() == 0)
  int init_bitmap_size(int64_t valid_bits_num);
  // init bitmap with blocks in datas_
  int init_bitmap(int64_t valid_bits_num);
  void clear()
  {
    datas_.reset();
    block_count_ = -1;
    valid_bits_num_ = 0;
  }
  int reset();

  int push_block_data(size_type data);

  int get_true_bit_positions(ObIArray<int64_t> &true_pos) const;

  int get_block_value(int64_t idx, size_type &value) const
  {
    return datas_.at(idx, value);
  }

  int set(int64_t bit_pos);

  int set_all_bits_true();

  OB_INLINE int64_t get_block_count() const
  {
    return block_count_;
  }

  OB_INLINE bool has_init() const
  {
    return block_count_ != -1;
  }

  OB_INLINE static int64_t get_need_blocks_num(int64_t num_bits)
  {
    return ((num_bits + BITS_PER_BLOCK - 1) & ~(BITS_PER_BLOCK - 1)) >> BLOCK_MOD_BITS;
  }

  OB_INLINE int64_t get_valid_bits_num() const { return valid_bits_num_; }
  DECLARE_TO_STRING;

private:
  ObSEArray<size_type, 8> datas_;
  // no need SERIALIZE
  int64_t block_count_;

  int64_t valid_bits_num_;
};

/// A Table Entity
class ObITableEntity: public common::ObDLinkBase<ObITableEntity>
{
  OB_UNIS_VERSION_V(1);
public:
  ObITableEntity()
      :alloc_(NULL)
  {}
  virtual ~ObITableEntity() = default;
  virtual ObTableEntityType get_entity_type() { return ObTableEntityType::ET_DYNAMIC; }
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
  virtual const ObObj &get_properties_value(int64_t idx) const = 0;
  virtual int64_t get_properties_count() const = 0;
  virtual void set_dictionary(const ObIArray<ObString> *all_rowkey_names, const ObIArray<ObString> *all_properties_names) = 0;
  virtual void set_is_same_properties_names(bool is_same_properties_names) = 0;
  virtual int construct_names_bitmap(const ObITableEntity& req_entity) = 0;
  virtual const ObTableBitMap *get_rowkey_names_bitmap() const = 0;
  virtual const ObTableBitMap *get_properties_names_bitmap() const = 0;
  virtual const ObIArray<ObString>* get_all_rowkey_names() const = 0;
  virtual const ObIArray<ObString>* get_all_properties_names() const = 0;
  //@}
  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other);
  int deep_copy_rowkey(common::ObIAllocator &allocator, const ObITableEntity &other);
  int deep_copy_properties(common::ObIAllocator &allocator, const ObITableEntity &other);
  virtual int add_retrieve_property(const ObString &prop_name);
  void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  common::ObIAllocator *get_allocator() { return alloc_; }

  virtual void set_properties_names(const ObIArray<ObString> *properties_names) {}
  virtual void set_rowkey_names(const ObIArray<ObString> *rowkey_names) {}
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
  virtual ~ObTableEntity();
  virtual ObTableEntityType get_entity_type() { return ObTableEntityType::ET_KV; }
  virtual int set_rowkey(const ObRowkey &rowkey) override;
  virtual int set_rowkey(const ObString &prop_name, const ObObj &rowkey_obj);
  virtual int set_rowkey(const ObITableEntity &other) override;
  virtual int set_rowkey_value(int64_t idx, const ObObj &value) override;
  virtual int add_rowkey_value(const ObObj &value) override;
  virtual int64_t get_rowkey_size() const override { return rowkey_.count(); };
  virtual int get_rowkey_value(int64_t idx, ObObj &value) const override;
  virtual int64_t hash_rowkey() const override;
  virtual int get_property(const ObString &prop_name, ObObj &prop_value) const override;
  virtual int set_property(const ObString &prop_name, const ObObj &prop_value) override;
  virtual int push_value(const ObObj &prop_value);
  virtual int get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const override;
  virtual int get_properties_names(ObIArray<ObString> &properties_names) const override;
  virtual int get_properties_values(ObIArray<ObObj> &properties_values) const override;
  virtual const ObObj &get_properties_value(int64_t idx) const override;
  virtual int64_t get_properties_count() const override;
  virtual void set_dictionary(const ObIArray<ObString> *all_rowkey_names, const ObIArray<ObString> *all_properties_names) override;
  virtual int construct_names_bitmap(const ObITableEntity& req_entity) override;
  virtual const ObTableBitMap *get_rowkey_names_bitmap() const override;
  virtual const ObTableBitMap * get_properties_names_bitmap() const override;
  virtual const ObIArray<ObString>* get_all_rowkey_names() const override;
  virtual const ObIArray<ObString>* get_all_properties_names() const override;
  virtual void set_is_same_properties_names(bool is_same_properties_names) override;
  virtual void reset() override;
  virtual ObRowkey get_rowkey() const override;
  OB_INLINE virtual const ObIArray<ObString>& get_rowkey_names() const { return rowkey_names_; }
  OB_INLINE virtual const ObIArray<ObString>& get_properties_names() const { return properties_names_; }
  OB_INLINE virtual const ObIArray<ObObj>& get_properties_values() const { return properties_values_; }
  OB_INLINE virtual const ObIArray<ObObj>& get_rowkey_objs() const { return rowkey_; };

  DECLARE_TO_STRING;
private:
  bool has_exist_in_properties(const ObString &name, int64_t *idx = nullptr) const;
protected:
  ObSEArray<ObObj, 8> rowkey_;
  ObSEArray<ObString, 32> properties_names_;
  ObSEArray<ObObj, 32> properties_values_;
  ObSEArray<ObString, 8> rowkey_names_;
};

// @note not thread-safe
template <typename T>
class ObTableEntityFactory: public ObITableEntityFactory
{
public:
  ObTableEntityFactory(const char *label = common::ObModIds::TABLE_PROC, uint64_t tenant_id = OB_SERVER_TENANT_ID)
      :alloc_(label, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id)
  {}
  ObTableEntityFactory(ObIAllocator &allocator) : alloc_(allocator, OB_MALLOC_NORMAL_BLOCK_SIZE, true)
  {}
  virtual ~ObTableEntityFactory();
  virtual ObITableEntity *alloc() override;
  virtual void free(ObITableEntity *obj) override;
  virtual void free_and_reuse() override;
  virtual int64_t get_free_count() const { return free_list_.get_size(); }
  virtual int64_t get_used_count() const { return used_list_.get_size(); }
  virtual int64_t get_used_mem() const { return alloc_.used(); }
  virtual int64_t get_total_mem() const { return alloc_.total(); }
  void reset() {
    free_all();
    alloc_.reset();
  }
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
  QUERY_END = 2,
  QUERY_RENEW = 3,
  QUERY_MAX
};

/// Table Operation Type
/// used for both table operation and single op
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
    CHECK_AND_INSERT_UP = 10,
    PUT = 11,
    TRIGGER = 12, // internal type for group commit trigger
    REDIS = 13,
    INVALID = 15
  };
  static bool is_group_support_type(ObTableOperationType::Type type)
  {
    return type == PUT || type == GET || type == INSERT ||
        type == DEL || type == UPDATE || type == INSERT_OR_UPDATE ||
        type == REPLACE || type == INCREMENT || type == APPEND || type == REDIS;
  }
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
   * put the entity.
   * @return ObTableOperationResult
   * In the case of put success, the return errno is OB_SUCCESS, affected_rows is 1
   * In the case of put failed, the affected_rows is 0
   * If the option returning_affected_rows is false (default value), then the return value of affected_rows is undefined, but with better performance.
   * Other common error code: OB_TIMEOUT indicates time out; OB_TRY_LOCK_ROW_CONFLICT indicate row lock conflict
   */
  static ObTableOperation put(const ObITableEntity &entity);
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
  int generate_stmt(const common::ObString &table_name, char *buf, int64_t buf_len, int64_t &pos) const;
  int64_t get_stmt_length(const common::ObString &table_name) const;
  void reset() { entity_ = nullptr; operation_type_ = ObTableOperationType::Type::INVALID; }
  const ObITableEntity &entity() const { return *entity_; }
  ObTableOperationType::Type type() const { return operation_type_; }
  void set_entity(const ObITableEntity &entity) { entity_ = &entity; }
  void set_entity(const ObITableEntity *entity) { entity_ = entity; }
  void set_type(ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  int get_entity(ObITableEntity *&entity);
  uint64_t get_checksum();
  TO_STRING_KV(K_(operation_type), "entity", entity_);
public:
  static const char *get_op_name(ObTableOperationType::Type op_type);
private:
  const ObITableEntity *entity_;
  ObTableOperationType::Type operation_type_;
};

class ObTableTTLOperation
{
public:
  ObTableTTLOperation(uint64_t tenant_id, uint64_t table_id, const ObTTLTaskParam &para,
                      uint64_t del_row_limit, ObRowkey start_rowkey, uint64_t hbase_cur_version)
  : tenant_id_(tenant_id), table_id_(table_id), max_version_(para.max_version_),
    time_to_live_(para.ttl_), is_htable_(para.is_htable_), del_row_limit_(del_row_limit),
    start_rowkey_(start_rowkey), hbase_cur_version_(hbase_cur_version)
  {}

  ~ObTableTTLOperation() {}
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && common::OB_INVALID_ID != table_id_ && del_row_limit_ > 0;
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
  uint64_t hbase_cur_version_;
};

enum ObTableResultType
{
  RESULT_TYPE_INVALID,
  TABLE_OPERATION_RESULT,
  REDIS_RESULT,
  RESULT_TYPE_MAX,
};

class ObITableResult
{
public:
  ObITableResult() {}
  ~ObITableResult() {}
  virtual int get_errno() const = 0;
  virtual void generate_failed_result(int ret_code,
                                      ObTableEntity &result_entity,
                                      ObTableOperationType::Type op_type) = 0;
  virtual void reset() = 0;
  virtual ObTableResultType get_type() const
  {
    return ObTableResultType::TABLE_OPERATION_RESULT;
  }
  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
};

/// common result for ObTable
class ObTableResult : public ObITableResult
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
  void set_err(int err)
  {
    errno_ = err;
    if (err != common::OB_SUCCESS) {
      common::ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
      if (OB_NOT_NULL(wb)) {
        (void)snprintf(msg_, common::OB_MAX_ERROR_MSG_LEN, "%s", wb->get_err_msg());
      }
    }
  }
  void set_errno(int err);
  virtual int get_errno() const override { return errno_; }
  int assign(const ObTableResult &other);
  virtual void reset() override
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
class ObTableOperationResult final : public ObTableResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableOperationResult();
  ~ObTableOperationResult() = default;
  virtual void reset() override;
  ObTableOperationType::Type type() const { return operation_type_; }
  int get_entity(const ObITableEntity *&entity) const;
  int get_entity(ObITableEntity *&entity);
  ObITableEntity *get_entity() { return entity_; }
  const ObITableEntity *get_entity() const { return entity_; }
  int64_t get_affected_rows() const { return affected_rows_; }
  int get_return_rows() const { return ((entity_ == NULL || entity_->is_empty()) ? 0 : 1); }
  OB_INLINE bool get_insertup_do_insert() { return is_insertup_do_insert_; }
  OB_INLINE bool get_is_insertup_do_put() { return is_insertup_do_put_; }
  OB_INLINE bool get_is_insertup_do_update() { return !is_insertup_do_put_ && !is_insertup_do_insert_; }
  OB_INLINE const ObNewRow *get_insertup_old_row() {return insertup_old_row_;}
  void set_entity(ObITableEntity &entity) { entity_ = &entity; }
  void set_entity(ObITableEntity *entity) { entity_ = entity; }
  void set_type(ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }
  void set_insertup_do_insert(bool do_insert) { is_insertup_do_insert_ = do_insert;}
  void set_insertup_do_put(bool do_put) { is_insertup_do_put_ = do_put;}
  void set_insertup_old_row(const ObNewRow *insertup_old_row)
  {
    insertup_old_row_ = insertup_old_row;
  }

  int deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableOperationResult &other);
  virtual void generate_failed_result(int ret_code,
                                      ObTableEntity &result_entity,
                                      ObTableOperationType::Type op_type) override
  {
    entity_ = &result_entity;
    operation_type_ = op_type;
    errno_ = ret_code;
  }
  DECLARE_TO_STRING;
private:
  ObTableOperationType::Type operation_type_;
  ObITableEntity *entity_;
  const ObNewRow *insertup_old_row_;
  int64_t affected_rows_;
  // for client compatibility, not serialize flags_ currently
  union {
    uint64_t flags_;
    struct {
      uint64_t is_insertup_do_insert_           : 1;
      uint64_t is_insertup_do_put_              : 1;
      uint64_t reserved_                        : 62;
    };
  };
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
  void set_returning_rowkey(bool returning)
  {
    if (returning) {
      option_flag_ |= OB_TABLE_OPTION_RETURNING_ROWKEY;
    }
  }
  bool returning_rowkey() const { return option_flag_ & OB_TABLE_OPTION_RETURNING_ROWKEY; }
  void set_returning_affected_entity(bool returning) { returning_affected_entity_ = returning; }
  bool returning_affected_entity() const { return returning_affected_entity_; }
  void set_batch_operation_as_atomic(bool atomic) { batch_operation_as_atomic_ = atomic; }
  bool batch_operation_as_atomic() const { return batch_operation_as_atomic_; }
  void set_binlog_row_image_type(ObBinlogRowImageType type) { binlog_row_image_type_ = type; }
  ObBinlogRowImageType binlog_row_image_type() const { return binlog_row_image_type_; }
  uint8_t get_option_flag() const { return option_flag_; }
private:
  ObTableConsistencyLevel consistency_level_;
  int64_t server_timeout_us_;
  int64_t max_execution_time_us_;
  ObIRetryPolicy *retry_policy_;
  bool returning_affected_rows_;  // default: false
  uint8_t option_flag_; // default: 0
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
  static const int64_t COMMON_BATCH_SIZE = 32;
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
  /// put the entity if not exists
  int put(const ObITableEntity &entity);
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

  OB_INLINE const ObIArray<table::ObTableOperation>& get_table_operations() const
  {
    return table_operations_;
  }
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
class ObTableBatchOperationResult : public common::ObSEArrayImpl<ObTableOperationResult, ObTableBatchOperation::COMMON_BATCH_SIZE>,
                                    public ObITableResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableBatchOperationResult()
      :BaseType(common::ObModIds::TABLE_BATCH_OPERATION_RESULT, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
       entity_factory_(NULL),
       alloc_(NULL)
  {}
  virtual ~ObTableBatchOperationResult() = default;
  void reset() override
  {
    BaseType::reset();
  }
  void set_entity_factory(ObITableEntityFactory *entity_factory) { entity_factory_ = entity_factory; }
  ObITableEntityFactory *get_entity_factory() { return entity_factory_; }
  void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  common::ObIAllocator *get_allocator() { return alloc_; }
  virtual int get_errno() const override { return OB_NOT_IMPLEMENT; }
  virtual void generate_failed_result(int ret_code,
                                      ObTableEntity &result_entity,
                                      ObTableOperationType::Type op_type) override
  {
    UNUSEDx(ret_code, result_entity, op_type);
  }
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
  static const char* const TTL_CNAME;
  static const ObString ROWKEY_CNAME_STR;
  static const ObString CQ_CNAME_STR;
  static const ObString VERSION_CNAME_STR;
  static const ObString VALUE_CNAME_STR;
  static const ObString TTL_CNAME_STR;

  // create table t1$cf1 (K varbinary(1024), Q varchar(256), T bigint, V varbinary(1024), primary key(K, Q, T));
  static const int64_t COL_IDX_K = 0;
  static const int64_t COL_IDX_Q = 1;
  static const int64_t COL_IDX_T = 2;
  static const int64_t COL_IDX_V = 3;
  static const int64_t COL_IDX_TTL = 4;
  static const int64_t HTABLE_ROWKEY_SIZE = 3;
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
  ObTableAggregation(ObTableAggregationType type, const ObString &column)
      : type_(type),
        column_(column)
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

enum class ParamType : int8_t {
    HBase = 0,
    Redis = 1,
    FTS = 2,
};

class ObKVParamsBase
{
public:
  ObKVParamsBase(): param_type_(ParamType::HBase) {}
  virtual ~ObKVParamsBase() = default;
  OB_INLINE ParamType get_param_type() { return param_type_; }
  virtual int32_t get_caching() const = 0;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const = 0;
  virtual int deserialize(const char *buf, const int64_t data_len, int64_t &pos) = 0;
  virtual int64_t get_serialize_size() const = 0;
  virtual int deep_copy(ObIAllocator &allocator, ObKVParamsBase *ob_params) const = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
protected:
  ParamType param_type_;
};

class ObFTSParam : public ObKVParamsBase
{
public:
  ObFTSParam()
    : ObKVParamsBase()
  {
    param_type_ = ParamType::FTS;
  }
  virtual ~ObFTSParam() {}
  virtual int deep_copy(ObIAllocator &allocator, ObKVParamsBase *ob_params) const override;
  OB_INLINE int32_t get_caching() const { return 0; } // unused
  OB_INLINE ParamType get_param_type() { return param_type_; }
  OB_INLINE common::ObString &get_search_text() { return search_text_; }
  NEED_SERIALIZE_AND_DESERIALIZE;

  VIRTUAL_TO_STRING_KV(K_(param_type),
                       K_(search_text));
private:
  ObString search_text_;
};

class ObHBaseParams : public ObKVParamsBase
{
public:
  const ObString DEFAULT_HBASE_VERSION = ObString("1.3.6");
  ObHBaseParams()
      : caching_(0),
        call_timeout_(0),
        allow_partial_results_(false),
        is_cache_block_(true),
        check_existence_only_(false),
        hbase_version_(DEFAULT_HBASE_VERSION)
  {
    param_type_ = ParamType::HBase;
  }
  ~ObHBaseParams() {};

  OB_INLINE ParamType get_param_type() { return param_type_; }
  OB_INLINE int32_t get_caching() const { return caching_; }
  OB_INLINE void set_caching(const int32_t caching) { caching_ = caching; }
  OB_INLINE void set_call_timeout_(const int32_t call_timeout) { call_timeout_ = call_timeout; }
  OB_INLINE void set_allow_partial_results(const bool allow_partial_results) { allow_partial_results_ = allow_partial_results; }
  OB_INLINE void set_is_cache_block(const bool is_cache_block) { is_cache_block_ = is_cache_block; }
  OB_INLINE void set_check_existence_only(const bool check_existence_only) {check_existence_only_ = check_existence_only; }
  int deep_copy(ObIAllocator &allocator, ObKVParamsBase *ob_params) const override;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV( K_(param_type),
              K_(caching),
              K_(call_timeout),
              K_(allow_partial_results),
              K_(is_cache_block),
              K_(check_existence_only),
              K_(hbase_version));
public:
  int32_t caching_;
  int32_t call_timeout_;
  union
  {
    int8_t flag_;
    struct {
      bool allow_partial_results_ : 1;
      bool is_cache_block_ : 1;
      bool check_existence_only_ : 1;
    };
  };
  ObString hbase_version_;
};

class ObKVParams
{
  OB_UNIS_VERSION(1);
public:
  ObKVParams(): allocator_(NULL), ob_params_(NULL){}
  ~ObKVParams() {};
  int deep_copy(ObIAllocator &allocator, ObKVParams &ob_params) const;
  void set_allocator(ObIAllocator *allocator) { allocator_ = allocator; }
  int init_ob_params_for_hfilter(const ObHBaseParams*& params) const;

  int alloc_ob_params(ParamType param_type, ObKVParamsBase* &params)
  {
    int ret = OB_SUCCESS;
    if (param_type == ParamType::HBase) {
      params = OB_NEWx(ObHBaseParams, allocator_);
      if (params == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        RPC_WARN("alloc params memory failed", K(ret));
      }
    } else if (param_type == ParamType::FTS) {
      params = OB_NEWx(ObFTSParam, allocator_);
      if (params == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        RPC_WARN("alloc params memory failed", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      RPC_WARN("not supported param_type", K(ret));
    }
    return ret;
  };
  TO_STRING_KV(K_(ob_params));

  common::ObIAllocator *allocator_;
  ObKVParamsBase* ob_params_;
};

/// A table query
/// 1. support multi range scan
/// 2. support reverse scan
/// 3. support secondary index scan
class ObTableQuery
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
      aggregations_(),
      ob_params_()
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
  const ObHTableFilter& htable_filter() const { return htable_filter_; }
  /// Set max row count of each batch.
  /// For htable, set the maximum number of cells to return for each call to next().
  int set_batch(int32_t batch_size);
  /// Set the maximum result size.
  /// The default is -1; this means that no specific maximum result size will be set for this query.
  /// @param max_result_size - The maximum result size in bytes.
  int set_max_result_size(int64_t max_result_size);
  /// @brief set ob_params for hbase or redis
  int set_ob_params(ObKVParams ob_params);
  int add_aggregation(ObTableAggregation &aggregation);

  const ObIArray<ObString> &get_select_columns() const { return select_columns_; }

  const ObIArray<common::ObNewRange> &get_scan_ranges() const { return key_ranges_; }

  ObIArray<common::ObNewRange> &get_scan_ranges() { return key_ranges_; }
  int32_t get_limit() const { return limit_; }
  int32_t get_offset() const { return offset_; }
  common::ObQueryFlag::ScanOrder get_scan_order() const { return scan_order_; }
  const ObString &get_index_name() const { return index_name_; }
  const ObHTableFilter& get_htable_filter() const { return htable_filter_; }
  int32_t get_batch() const { return batch_size_; }
  int64_t get_max_result_size() const { return max_result_size_; }
  const ObKVParams& get_ob_params() const {return ob_params_;}
  int64_t get_range_count() const { return key_ranges_.count(); }
  uint64_t get_checksum() const;
  const ObString &get_filter_string() const { return filter_string_; }
  void clear_scan_range() { key_ranges_.reset(); }
  void clear_select_columns() { select_columns_.reset(); }
  void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_allocator_ = allocator; }
  int deep_copy(ObIAllocator &allocator, ObTableQuery &dst) const;
  const common::ObIArray<ObTableAggregation> &get_aggregations() const { return aggregations_; }
  bool is_aggregate_query() const { return !aggregations_.empty(); }
  int generate_stmt(const common::ObString &table_name, char *buf, int64_t buf_len, int64_t &pos) const;
  int64_t get_stmt_length(const common::ObString &table_name) const;
  OB_INLINE const ObIArray<common::ObString> &get_scan_range_columns() const
  {
    return scan_range_columns_;
  }
  OB_INLINE int64_t get_scan_range_columns_count() const
  {
    return scan_range_columns_.count();
  }
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
               K_(aggregations),
               K_(ob_params));

public:
  static ObString generate_filter_condition(const ObString &column, const ObString &op, const ObObj &value);
  static ObString combile_filters(const ObString &filter1, const ObString &op, const ObString &filter2);
  static common::ObNewRange generate_prefix_scan_range(const ObRowkey &rowkey_prefix);
protected:
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
  ObKVParams ob_params_;
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

class ObITableQueryAndMutate
{
public:
  ObITableQueryAndMutate() = default;
  ~ObITableQueryAndMutate() = default;
  virtual const ObTableQuery &get_query() const = 0;
  virtual ObTableQuery &get_query() = 0;
  virtual const ObTableBatchOperation &get_mutations() const = 0;
  virtual ObTableBatchOperation &get_mutations() = 0;
  virtual bool return_affected_entity() const = 0;
  virtual bool is_check_and_execute() const = 0;
  virtual bool is_check_exists() const = 0;
  virtual bool rollback_when_check_failed() const = 0;
};

/// query and mutate the selected rows.
class ObTableQueryAndMutate : public ObITableQueryAndMutate
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryAndMutate()
      : return_affected_entity_(true),
        flag_(0)
  {}
  const ObTableQuery &get_query() const override { return query_; }
  ObTableQuery &get_query() override { return query_; }
  const ObTableBatchOperation &get_mutations() const override { return mutations_; }
  ObTableBatchOperation &get_mutations() override { return mutations_; }
  bool return_affected_entity() const override { return return_affected_entity_; }

  void set_deserialize_allocator(common::ObIAllocator *allocator);
  void set_entity_factory(ObITableEntityFactory *entity_factory);

  bool is_check_and_execute() const override { return is_check_and_execute_; }
  bool is_check_exists() const override { return is_check_and_execute_ && !is_check_no_exists_; }
  bool rollback_when_check_failed() const override { return is_check_and_execute_ && rollback_when_check_failed_; }
  uint64_t get_checksum();

  TO_STRING_KV(K_(query),
               K_(mutations),
               K_(return_affected_entity),
               K_(flag),
               K_(is_check_and_execute),
               K_(is_check_no_exists),
               K_(rollback_when_check_failed));
private:
  ObTableQuery query_;
  ObTableBatchOperation mutations_;
  bool return_affected_entity_;
  union
  {
    uint64_t flag_;
    struct
    {
      bool is_check_and_execute_ : 1;
      bool is_check_no_exists_ : 1;
      bool rollback_when_check_failed_ : 1;
      int64_t reserved : 61;
    };
  };
};

class ObTableSingleOp;
class ObTableSingleOpQAM : public ObITableQueryAndMutate
{
public:
  ObTableSingleOpQAM(const ObTableQuery &query,
                     bool is_check_and_execute,
                     bool is_check_no_exists,
                     bool rollback_when_check_failed)
    : query_(query),
      return_affected_entity_(false),
      is_check_and_execute_(is_check_and_execute),
      is_check_exists_(!is_check_no_exists),
      rollback_when_check_failed_(rollback_when_check_failed)
  {}
  ~ObTableSingleOpQAM() = default;

public:
  OB_INLINE const ObTableQuery &get_query() const override { return query_; }
  OB_INLINE ObTableQuery &get_query() override { return const_cast<ObTableQuery &>(query_); }
  OB_INLINE const ObTableBatchOperation &get_mutations() const override { return mutations_; }
  OB_INLINE ObTableBatchOperation &get_mutations() override { return const_cast<ObTableBatchOperation &>(mutations_); }
  OB_INLINE bool return_affected_entity() const override { return return_affected_entity_; };
  OB_INLINE bool is_check_and_execute() const override { return is_check_and_execute_; }
  OB_INLINE bool is_check_exists() const override { return is_check_exists_; }
  OB_INLINE bool rollback_when_check_failed() const override { return rollback_when_check_failed_; }
  int set_mutations(const ObTableSingleOp &single_op);

private:
  const ObTableQuery &query_;
  ObTableBatchOperation mutations_;
  bool return_affected_entity_;
  bool is_check_and_execute_;
  bool is_check_exists_;
  bool rollback_when_check_failed_;
};

inline void ObTableQueryAndMutate::set_deserialize_allocator(common::ObIAllocator *allocator)
{
  query_.set_deserialize_allocator(allocator);
}

inline void ObTableQueryAndMutate::set_entity_factory(ObITableEntityFactory *entity_factory)
{
  mutations_.set_entity_factory(entity_factory);
}

class ObTableQueryIterableResultBase
{
public:
  ObTableQueryIterableResultBase()
    : allocator_("TblQryIterRes", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      row_count_(0)
  {};
  virtual ~ObTableQueryIterableResultBase() {}
  virtual int add_row(const common::ObNewRow &row, ObString family_name) { UNUSED(row); UNUSED(family_name); return OB_SUCCESS; }
  virtual int add_row(const common::ObIArray<ObObj> &row) { UNUSED(row); return OB_SUCCESS; }
  virtual int add_row(const common::ObNewRow &row) { UNUSED(row); return OB_SUCCESS; };
  virtual int add_all_row(ObTableQueryIterableResultBase &other) { UNUSED(other); return OB_SUCCESS; };
  virtual bool reach_batch_size_or_result_size(const int32_t batch_count, const int64_t max_result_size) { UNUSED(batch_count); UNUSED(max_result_size); return true; }
  virtual int get_row(ObNewRow &row) { UNUSED(row); return OB_SUCCESS; }
  int64_t get_row_count() const { return row_count_; };
  int64_t &get_row_count() { return row_count_; }
protected:
  int append_family(ObNewRow &row, ObString family_name);
  int transform_lob_cell(ObNewRow &row, const int64_t lob_storage_count);
  OB_INLINE int64_t get_lob_storage_count(const common::ObNewRow &row) const
  {
    int64_t count = 0;
    for (int64_t i = 0; i < row.get_count(); ++i) {
      if (is_lob_storage(row.get_cell(i).get_type())) {
        count++;
      }
    }
    return count;
  }
protected:
  common::ObArenaAllocator allocator_;
  int64_t row_count_;
};

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
  virtual int get_htable_all_entity(ObIArray<ObITableEntity*> &entities);
  int add_property_name(const ObString &name);
  int assign_property_names(const common::ObIArray<common::ObString> &other);
  // for aggregation
  int deep_copy_property_names(const common::ObIArray<common::ObString> &other);
  void reset_property_names() { properties_names_.reset(); }
  virtual int add_row(const common::ObNewRow &row);
  virtual int add_row(const common::ObIArray<ObObj> &row);
  int add_all_property(const ObTableQueryResult &other);
  int append_property_names(const ObIArray<ObString> &property_names);
  int add_all_row(const ObTableQueryResult &other);
  int add_all_row(ObTableQueryIterableResultBase &other);
  void save_row_count_only(const int row_count) { reset(); row_count_ += row_count; }
  int64_t get_row_count() const { return row_count_; }
  int64_t get_property_count() const { return properties_names_.count(); }
  int64_t get_result_size();
  int get_first_row(common::ObNewRow &row) const;
  bool reach_batch_size_or_result_size(const int32_t batch_count,
                                       const int64_t max_result_size);
  const common::ObIArray<common::ObString>& get_select_columns() const { return properties_names_; };
  static int64_t get_max_packet_buffer_length() { return obrpc::get_max_rpc_packet_size() - (1<<20); }
  static int64_t get_max_buf_block_size() { return get_max_packet_buffer_length() - (1024*1024LL); }
  TO_STRING_KV(K(properties_names_), K(row_count_), K(buf_.get_position()));
private:
  static const int64_t DEFAULT_BUF_BLOCK_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE - (1024*1024LL);
  int alloc_buf_if_need(const int64_t size);
  OB_INLINE int64_t get_lob_storage_count(const common::ObNewRow &row) const
  {
    int64_t count = 0;
    for (int64_t i = 0; i < row.get_count(); ++i) {
      if (is_lob_storage(row.get_cell(i).get_type())) {
        count++;
      }
    }
    return count;
  }
  int process_lob_storage(ObNewRow& new_row);

private:
  common::ObSEArray<ObString, 16> properties_names_;  // serialize
  int64_t row_count_;                                 // serialize
  common::ObDataBuffer buf_;                          // serialize
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator prop_name_allocator_;
  int64_t fixed_result_size_;
  // for deserialize and read
  int64_t curr_idx_;
  ObTableEntity curr_entity_;
};

class ObTableQueryDListResult: public ObTableQueryIterableResultBase
{
  using ObCellNode = common::ObDLinkNode<ObHTableCellEntity*>;
  using ObCellDLinkedList = common::ObDList<ObCellNode>;
public:
  ObTableQueryDListResult();
  ~ObTableQueryDListResult();
  virtual int add_row(const common::ObNewRow &row, ObString family_name) override;
  virtual int add_row(const common::ObNewRow &row) override;
  virtual int add_all_row(ObTableQueryIterableResultBase &other) override;
  virtual bool reach_batch_size_or_result_size(const int32_t batch_count, const int64_t max_result_size) override;
  virtual int get_row(ObNewRow &row) override;
  int get_row(ObHTableCellEntity *&row);
  ObCellDLinkedList &get_cell_list() { return cell_list_; }
  void reset();
private:
  ObCellDLinkedList cell_list_;
};

class ObTableQueryIterableResult: public ObTableQueryIterableResultBase
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryIterableResult();
  virtual int add_row(const common::ObNewRow &row, ObString family_name) override;
  virtual int add_row(const common::ObNewRow &row) override;
  int add_all_row(ObTableQueryDListResult &other);
  virtual int add_row(const common::ObIArray<ObObj> &row) override;
  virtual bool reach_batch_size_or_result_size(const int32_t batch_count, const int64_t max_result_size) override;
  void save_row_count_only(const int row_count) { reset_except_property(); row_count_ += row_count; }
  virtual int get_row(ObNewRow &row) override;
  void reset_except_property();
private:
  int64_t current_ = 0;

public:
  common::ObArray<ObNewRow> rows_;
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

class ObTableQueryAsyncResult: public ObTableQueryResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryAsyncResult()
    : is_end_(false),
      query_session_id_(0)
  {}
  virtual ~ObTableQueryAsyncResult() {}
public:
  INHERIT_TO_STRING_KV("ObTableQueryResult", ObTableQueryResult, K_(is_end), K_(query_session_id));
public:
  bool     is_end_;
  uint64_t  query_session_id_; // from server gen
};

struct ObTableApiCredential final
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t CREDENTIAL_BUF_SIZE = 256;
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
  void reset();
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
      end_rowkey_(),
      iter_end_ts_(0)
    {}
  ~ObTableTTLOperationResult() {}
  uint64_t get_ttl_del_row() { return ttl_del_rows_; }
  uint64_t get_max_version_del_row() { return max_version_del_rows_; }
  uint64_t get_del_row() { return ttl_del_rows_ + max_version_del_rows_; }
  uint64_t get_scan_row() { return scan_rows_; }
  common::ObString get_end_rowkey() { return end_rowkey_; }
  int64_t get_end_ts() { return iter_end_ts_; }
  TO_STRING_KV(K_(ttl_del_rows), K_(max_version_del_rows), K_(scan_rows), K_(end_rowkey), K_(iter_end_ts));
public:
  uint64_t ttl_del_rows_;
  uint64_t max_version_del_rows_;
  uint64_t scan_rows_;
  common::ObString end_rowkey_;
  int64_t iter_end_ts_;
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
  void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    schema_version_ = common::OB_INVALID_VERSION;
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    role_ = common::ObRole::INVALID_ROLE;
    replica_type_ = common::ObReplicaType::REPLICA_TYPE_MAX;
    part_renew_time_ = 0;
    reserved_ = 0;
  }
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

class ObTableMoveResult final : public ObITableResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableMoveResult()
      : reserved_(0)
  {}
  virtual ~ObTableMoveResult() {}
  TO_STRING_KV(K_(replica_info),
               K_(reserved));

  void reset() override
  {
    replica_info_.reset();
    reserved_ = 0;
  }
  OB_INLINE ObTableMoveReplicaInfo& get_replica_info() { return replica_info_; }
  virtual int get_errno() const override { return OB_NOT_IMPLEMENT; }
  virtual void generate_failed_result(int ret_code,
                                      ObTableEntity &result_entity,
                                      ObTableOperationType::Type op_type) override
  {
    UNUSEDx(ret_code, result_entity, op_type);
  }
private:
  ObTableMoveReplicaInfo replica_info_;
  uint64_t reserved_;
};

template <typename T>
class ObTableAtomicValue final
{
public:
  ObTableAtomicValue(T init_value)
      : value_(init_value)
  {}
  ~ObTableAtomicValue() = default;
  TO_STRING_KV(K_(value));
  void set(T v) { ATOMIC_STORE(&value_, v); }
  T value() const { return ATOMIC_LOAD(&value_); }
  void inc(int64_t delta = 1) { ATOMIC_AAF(&value_, delta); }
  void dec(int64_t delta = 1) { ATOMIC_SAF(&value_, delta); }
private:
  T value_;
};
// Compared to ObTableQuery, ObTableSingleOpQuery only changed the serialization/deserialization method
class ObTableSingleOpQuery final : public ObTableQuery
{
  OB_UNIS_VERSION(1);
public:
  ObTableSingleOpQuery() : all_rowkey_names_(nullptr) {}
  ~ObTableSingleOpQuery() = default;

  void reset();

  OB_INLINE const common::ObIArray<common::ObNewRange> &get_scan_range() const
  {
    return key_ranges_;
  }
  OB_INLINE const common::ObString &get_index_name() const
  {
    return index_name_;
  }
  OB_INLINE const ObString &get_filter_string() const
  {
    return filter_string_;
  }
  OB_INLINE void set_dictionary(const ObIArray<ObString> *all_rowkey_names)
  {
    all_rowkey_names_ = all_rowkey_names;
  }
  OB_INLINE bool has_dictionary() const
  {
    return OB_NOT_NULL(all_rowkey_names_);
  }
  OB_INLINE const ObTableBitMap &get_scan_range_cols_bp() const
  {
    return scan_range_cols_bp_;
  }

  TO_STRING_KV(K_(index_name),
               K_(scan_range_columns),
               K_(key_ranges),
               K_(filter_string));
private:
  ObTableBitMap scan_range_cols_bp_;
  const ObIArray<ObString> *all_rowkey_names_; // do not serialize
};

class ObTableSingleOpEntity : public ObTableEntity {
  OB_UNIS_VERSION_V(1);

public:
  ObTableSingleOpEntity() : is_same_properties_names_(false), all_rowkey_names_(nullptr), all_properties_names_(nullptr)
  {}

  ~ObTableSingleOpEntity() = default;

  OB_INLINE virtual void set_is_same_properties_names(bool is_same_properties_names)
  {
    is_same_properties_names_ = is_same_properties_names;
  }

  OB_INLINE virtual void set_dictionary(const ObIArray<ObString> *all_rowkey_names,
                                        const ObIArray<ObString> *all_properties_names)
  {
    all_rowkey_names_ = all_rowkey_names;
    all_properties_names_ = all_properties_names;
  }

  OB_INLINE virtual const ObIArray<ObString> *get_all_rowkey_names() const
  {
    return all_rowkey_names_;
  }
  OB_INLINE virtual const ObIArray<ObString> *get_all_properties_names() const
  {
    return all_properties_names_;
  }

  OB_INLINE virtual const ObTableBitMap *get_rowkey_names_bitmap() const
  {
    return &rowkey_names_bp_;
  }

  OB_INLINE virtual const ObTableBitMap *get_properties_names_bitmap() const
  {
    return &properties_names_bp_;
  }

  virtual void reset() override;

  // must construct bitmap before serialization !!
  virtual int construct_names_bitmap(const ObITableEntity& req_entity) override;

  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other) override;

  int construct_names_bitmap_by_dict(const ObITableEntity& req_entity);

  int construct_properties_bitmap_by_dict(const ObITableEntity& req_entity);

  static int construct_column_names(const ObTableBitMap &names_bit_map,
                                              const ObIArray<ObString> &all_column_names,
                                              ObIArray<ObString> &column_names);

  virtual void set_properties_names(const ObIArray<ObString> *properties_names) { all_properties_names_ = properties_names; }
  virtual void set_rowkey_names(const ObIArray<ObString> *rowkey_names) { all_rowkey_names_ = rowkey_names; }
private:

  OB_INLINE bool has_dictionary() const
  {
    return OB_NOT_NULL(all_rowkey_names_) && OB_NOT_NULL(all_properties_names_);
  }

protected:
  ObTableBitMap rowkey_names_bp_;
  ObTableBitMap properties_names_bp_;

  // no need serialization
  bool is_same_properties_names_;
  const ObIArray<ObString> *all_rowkey_names_;
  const ObIArray<ObString> *all_properties_names_;
};

// basic execute unit
class ObTableSingleOp
{
  OB_UNIS_VERSION(1);
public:
  ObTableSingleOp()
      : op_type_(ObTableOperationType::INVALID),
        flag_(0),
        entities_(),
        deserialize_alloc_(nullptr),
        op_query_(nullptr),
        all_rowkey_names_(nullptr),
        all_properties_names_(nullptr),
        is_same_properties_names_(false)
  {
    entities_.set_attr(ObMemAttr(MTL_ID(), "SingleOpEntity"));
  }

  ~ObTableSingleOp() = default;

  OB_INLINE ObTableSingleOpQuery* get_query() { return op_query_; }
  OB_INLINE const ObTableSingleOpQuery* get_query() const { return op_query_; }

  OB_INLINE ObIArray<ObTableSingleOpEntity> &get_entities() { return entities_; }

  OB_INLINE const ObIArray<ObTableSingleOpEntity> &get_entities() const { return entities_; }

  OB_INLINE ObTableOperationType::Type get_op_type() const { return op_type_; }

  OB_INLINE const ObIArray<ObString>* get_all_rowkey_names() const { return all_rowkey_names_; }

  OB_INLINE const ObIArray<ObString>* get_all_properties_names() const { return all_properties_names_; }

  OB_INLINE void set_op_query(ObTableSingleOpQuery *op_query)
  {
    op_query_ = op_query;
  }

  OB_INLINE void set_deserialize_allocator(common::ObIAllocator *allocator)
  {
    deserialize_alloc_ = allocator;
  }

  OB_INLINE bool is_check_no_exists() const { return is_check_no_exists_; }
  OB_INLINE bool rollback_when_check_failed() const { return rollback_when_check_failed_; }

  OB_INLINE void set_dictionary(const ObIArray<ObString> *all_rowkey_names, const ObIArray<ObString> *all_properties_names) {
    all_rowkey_names_ = all_rowkey_names;
    all_properties_names_ = all_properties_names;
  }

  OB_INLINE void set_is_same_properties_names(bool is_same) {
    is_same_properties_names_ = is_same;
  }

  OB_INLINE bool need_query() const { return op_type_ == ObTableOperationType::CHECK_AND_INSERT_UP || op_type_ == ObTableOperationType::SCAN; }
  uint64_t get_checksum();

  OB_INLINE void set_operation_type(ObTableOperationType::Type type) { op_type_ = type; }
  void reset();

  TO_STRING_KV(K_(op_type),
               K_(flag),
               K_(is_check_no_exists),
               K_(op_query),
               K_(entities));
private:
  ObTableOperationType::Type op_type_;
  union
  {
    uint64_t flag_;
    struct
    {
      bool is_check_no_exists_ : 1;
      bool rollback_when_check_failed_ : 1;
      int64_t reserved : 62;
    };
  };
  // Note: Only the HBase checkAndMutate operation may have multiple entities,
  // In such cases, we decode the size first and prepare_allocate the entities at once.
  ObSEArray<ObTableSingleOpEntity, 1> entities_;
  table::ObTableEntityFactory<table::ObTableSingleOpEntity> *default_entity_factory_;
  common::ObIAllocator *deserialize_alloc_; // do not serialize
  ObTableSingleOpQuery *op_query_;
  const ObIArray<ObString>* all_rowkey_names_; // do not serialize
  const ObIArray<ObString>* all_properties_names_; // do not serialize
  bool is_same_properties_names_ = false;
};

// A collection of single operations for a specified tablet
class ObTableTabletOp
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t COMMON_OPS_SIZE = 8;
public:
  ObTableTabletOp()
      : tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
        option_flag_(0),
        all_rowkey_names_(nullptr),
        all_properties_names_(nullptr),
        is_ls_same_properties_names_(false)
  {
    single_ops_.set_attr(ObMemAttr(MTL_ID(), "TabletOpSingOps"));
  }
  ~ObTableTabletOp() = default;
  ObTableTabletOp(const ObTableTabletOp &other);
  OB_INLINE int64_t count() const { return single_ops_.count(); }
  OB_INLINE int64_t empty() const { return single_ops_.count() == 0; }
  OB_INLINE void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_alloc_ = allocator; }
  OB_INLINE const ObTableSingleOp &at(int64_t idx) const { return single_ops_.at(idx); }
  OB_INLINE ObTableSingleOp &at(int64_t idx) { return single_ops_.at(idx); }
  OB_INLINE void operator()(const ObTableTabletOp &other){
    reset();
    this->tablet_id_ = other.get_tablet_id();
    this->option_flag_ = other.get_option_flag();
    this->single_ops_ = other.single_ops_;
  }
  OB_INLINE void reset() {
    single_ops_.reset();
  }
  OB_INLINE const ObTabletID &get_tablet_id() const { return tablet_id_; }
  OB_INLINE const uint64_t &get_option_flag() const { return option_flag_; }
  OB_INLINE bool is_same_type() const { return is_same_type_; }
  OB_INLINE bool is_atomic() const { return is_automic_; }
  OB_INLINE bool is_readonly() const { return is_readonly_; }
  OB_INLINE bool is_same_properties_names() const { return is_same_properties_names_; }
  OB_INLINE bool is_use_put() const { return use_put_; }
  OB_INLINE bool is_returning_affected_entity() const { return returning_affected_entity_; }
  OB_INLINE bool is_returning_rowkey() const { return returning_rowkey_; }
  OB_INLINE void set_dictionary(const ObIArray<ObString> *all_rowkey_names, const ObIArray<ObString> *all_properties_names) {
    all_rowkey_names_ = all_rowkey_names;
    all_properties_names_ = all_properties_names;
  }

  const ObIArray<ObString>* get_all_rowkey_names() { return all_rowkey_names_; }

  const ObIArray<ObString>* get_all_properties_names() { return all_properties_names_; }

  OB_INLINE void set_is_ls_same_prop_name(bool is_same) { is_ls_same_properties_names_ = is_same; }

  OB_INLINE void set_tablet_id(ObTabletID tablet_id) { tablet_id_ = tablet_id; }
  OB_INLINE void set_option_flag(uint64_t option_flag) { option_flag_ = option_flag; }

  OB_INLINE int add_single_op(const ObTableSingleOp &single_op) { return single_ops_.push_back(single_op); }

  OB_INLINE void reuse()
  {
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    option_flag_ = 0;
    all_rowkey_names_ = nullptr;
    all_properties_names_ = nullptr;
    is_same_properties_names_ = false;
    single_ops_.reuse();
  }

  TO_STRING_KV(K_(tablet_id),
               K_(option_flag),
               K_(is_same_type),
               K_(is_same_properties_names),
               "single_ops_count", single_ops_.count(),
               K_(single_ops));
private:
  common::ObTabletID tablet_id_;
  union
  {
    uint64_t option_flag_;
    struct
    {
      bool is_same_type_ : 1;
      bool is_same_properties_names_ : 1;
      bool is_readonly_ : 1;
      bool is_automic_ : 1;
      bool use_put_ : 1;
      bool returning_affected_entity_ : 1;
      bool returning_rowkey_ : 1;
      uint64_t reserved : 57;
    };
  };
  common::ObSEArray<ObTableSingleOp, 1> single_ops_;
  common::ObIAllocator *deserialize_alloc_; // do not serialize
  const ObIArray<ObString>* all_rowkey_names_; // do not serialize
  const ObIArray<ObString>* all_properties_names_; // do not serialize
  bool is_ls_same_properties_names_;
};

/*
  ObTableLSOp contains multiple ObTableTabletOp belonging to the same LS,
  ObTableTabletOp contains multiple ObTableSingleOp belonging to the same Tablet,
  ObTableSingleOp is a basic unit of operation that is need to be executed.
*/
class ObTableLSOp
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MAX_BATCH_SIZE = 1000;
  static const int64_t COMMON_BATCH_SIZE = 8;
public:
  ObTableLSOp()
    : ls_id_(share::ObLSID::INVALID_LS_ID),
      table_name_(),
      table_id_(OB_INVALID_ID),
      rowkey_names_(),
      properties_names_(),
      option_flag_(0),
      entity_factory_(nullptr),
      deserialize_alloc_(nullptr),
      tablet_ops_()
  {
    rowkey_names_.set_attr(ObMemAttr(MTL_ID(), "LSOpRkNames"));
    properties_names_.set_attr(ObMemAttr(MTL_ID(), "LSOpPropNames"));
    tablet_ops_.set_attr(ObMemAttr(MTL_ID(), "LSOpTabletOps"));
  }
  void reset();
  OB_INLINE void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_alloc_ = allocator; }
  OB_INLINE int64_t count() const { return tablet_ops_.count(); }
  OB_INLINE const share::ObLSID &get_ls_id() const { return ls_id_; }
  OB_INLINE const ObTableTabletOp &at(int64_t idx) const { return tablet_ops_.at(idx); }
  OB_INLINE ObTableTabletOp &at(int64_t idx) { return tablet_ops_.at(idx); }
  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE const ObString& get_table_name() const { return table_name_; }
  OB_INLINE const ObIArray<ObString>& get_all_rowkey_names() {return rowkey_names_; }
  OB_INLINE const ObIArray<ObString>& get_all_properties_names() {return properties_names_; }
  OB_INLINE bool is_same_type() const { return is_same_type_; }
  OB_INLINE bool is_same_properties_names() const { return is_same_properties_names_; }
  OB_INLINE bool return_one_result() const { return return_one_result_; }
  OB_INLINE bool need_all_prop_bitmap() const { return need_all_prop_bitmap_; }

  TO_STRING_KV(K_(ls_id),
               K_(table_name),
               K_(table_id),
               K_(option_flag),
               K_(is_same_type),
               K_(is_same_properties_names),
               K_(return_one_result),
               K_(rowkey_names),
               K_(properties_names),
               "tablet_ops_count_", tablet_ops_.count(),
               K_(tablet_ops),
               K_(need_all_prop_bitmap));
private:
  share::ObLSID ls_id_;
  common::ObString table_name_;
  uint64_t table_id_;
  ObSEArray<ObString, 4> rowkey_names_;
  ObSEArray<ObString, 4> properties_names_;
  union
  {
    uint64_t option_flag_;
    struct
    {
      bool is_same_type_ : 1;
      bool is_same_properties_names_ : 1;
      bool return_one_result_ : 1;
      bool need_all_prop_bitmap_ : 1;
      uint64_t reserved : 60;
    };
  };

  ObTableEntityFactory<ObTableSingleOpEntity> *entity_factory_; // do not serialize
  common::ObIAllocator *deserialize_alloc_; // do not serialize
  ObSEArray<ObTableTabletOp, COMMON_BATCH_SIZE> tablet_ops_;
};

// result for OBKV Redis
class ObRedisResult : public ObITableResult
{
  OB_UNIS_VERSION(1);
public:
  ObRedisResult(common::ObIAllocator *allocator = nullptr)
      : ret_(common::OB_ERR_UNEXPECTED), allocator_(allocator), msg_()
  {
  }
  ~ObRedisResult() = default;
  int set_ret(int arg_ret, const ObString &redis_msg, bool need_deep_copy = true);
  int set_err(int err);
  int assign(const ObRedisResult &other);
  virtual void reset() override
  {
    ret_ = common::OB_ERR_UNEXPECTED;
    msg_.reset();
  }

  virtual int get_errno() const override { return ret_; }

  virtual void generate_failed_result(int ret_code,
                                      ObTableEntity &result_entity,
                                      ObTableOperationType::Type op_type) override
  {
    UNUSEDx(result_entity, op_type);
    set_err(ret_code);
  }

  int convert_to_table_op_result(ObTableOperationResult &result);

  virtual ObTableResultType get_type() const override { return ObTableResultType::REDIS_RESULT; }

  void set_allocator(common::ObIAllocator *allocator) { allocator_ = allocator; }

  TO_STRING_KV(K_(ret), KP(allocator_));

private:
  int ret_;
  common::ObIAllocator *allocator_;
  ObString msg_;
};

} // end namespace table
} // end namespace oceanbase


#endif /* _OB_TABLE_TABLE_H */
