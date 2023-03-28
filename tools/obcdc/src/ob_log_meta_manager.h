/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIBOBLOG_META_MANAGER_H__
#define OCEANBASE_LIBOBLOG_META_MANAGER_H__

#include <MetaInfo.h>                                     // ITableMeta, IDBMeta

#include "share/ob_errno.h"                               // OB_SUCCESS
#include "lib/lock/ob_spin_rwlock.h"                      // SpinRWLock, SpinRLockGuard, SpinWLockGuard
#include "lib/allocator/page_arena.h"                     // DefaultPageAllocator
#include "lib/allocator/ob_mod_define.h"                  // ObModIds
#include "lib/hash/ob_linear_hash_map.h"                  // ObLinearHashMap
#include "lib/allocator/ob_fifo_allocator.h"              // ObFIFOAllocator
#include "lib/allocator/ob_concurrent_fifo_allocator.h"   // ObConcurrentFIFOAllocator
#include "lib/allocator/ob_allocator.h"                   // ObIAllocator
#include "ob_log_schema_cache_info.h"                     // TableSchemaInfo

using namespace oceanbase::logmessage;
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObSimpleTableSchemaV2;
class ObColumnSchemaV2;
} // namespace schema
} // namespace share

namespace liboblog
{
class ObLogSchemaGuard;
class IObLogSchemaGetter;

typedef ObLogSchemaGuard ObLogSchemaGuard;

class ObObj2strHelper;
class ObLogAdaptString;
struct DBSchemaInfo;
struct TenantSchemaInfo;

class IObLogMetaManager
{
public:
  virtual ~IObLogMetaManager() {}

public:
  // add ref count of Table Meta
  // 1. try to get table meta by ObSimpleTableSchemaV2(default)
  // 2. try get table meta by ObTableSchema if meta info not exist，make sure ObTableSchema will only refresh exactly once
  //
  // @retval OB_SUCCESS                   success
  // @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
  // #retval other error code             fail
  virtual int get_table_meta(
      const int64_t global_schema_version,
      const share::schema::ObSimpleTableSchemaV2 *table_schema,
      IObLogSchemaGetter &schema_getter,
      ITableMeta *&table_meta,
      volatile bool &stop_flag) = 0;

  // get DDL Table Meta
  virtual ITableMeta *get_ddl_table_meta() = 0;

  // decrease ref count of Table Meta
  virtual void revert_table_meta(ITableMeta *table_meta) = 0;

  // add ref count of DB Meta
  //
  // @retval OB_SUCCESS                   success
  // @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
  // #retval other error code             fail
  virtual int get_db_meta(
      const DBSchemaInfo &db_schema_info,
      ObLogSchemaGuard &schema_mgr,
      IDBMeta *&db_meta,
      volatile bool &stop_flag) = 0;

  // decrease ref count of DB Meta
  virtual void revert_db_meta(IDBMeta *db_meta) = 0;

  // delete table
  // delete all data of the deleted table and decrease ref count of the TableMeta by 1 for all version
  virtual int drop_table(const int64_t table_id) = 0;

  // delete database
  // delete all data of the deleted database and decrease ref count of the database meta by 1 for all version
  virtual int drop_database(const int64_t database_id) = 0;

  virtual int get_table_schema_meta(const int64_t version,
    const uint64_t table_id,
    TableSchemaInfo *&tb_schema_info) = 0;
};

class ObLogMetaManager : public IObLogMetaManager
{
public:
  ObLogMetaManager();
  virtual ~ObLogMetaManager();

public:
  virtual int get_table_meta(
      const int64_t global_schema_version,
      const share::schema::ObSimpleTableSchemaV2 *table_schema,
      IObLogSchemaGetter &schema_getter,
      ITableMeta *&table_meta,
      volatile bool &stop_flag);
  virtual ITableMeta *get_ddl_table_meta() { return ddl_table_meta_; }
  virtual void revert_table_meta(ITableMeta *table_meta);
  virtual int get_db_meta(
      const DBSchemaInfo &db_schema_info,
      ObLogSchemaGuard &schema_mgr,
      IDBMeta *&db_meta,
      volatile bool &stop_flag);
  virtual void revert_db_meta(IDBMeta *db_meta);
  virtual int drop_table(const int64_t table_id);
  virtual int drop_database(const int64_t database_id);
  virtual int get_table_schema_meta(const int64_t version,
    const uint64_t table_id,
    TableSchemaInfo *&tb_schema_info);
public:
  int init(ObObj2strHelper *obj2str_helper,
      const bool enable_output_hidden_primary_key);
  void destroy();

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
  typedef common::ObConcurrentFIFOAllocator FIFOAllocator;

  static const int64_t ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t ALLOCATOR_HOLD_LIMIT = common::OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t ALLOCATOR_TOTAL_LIMIT = 10L * 1024L * 1024L * 1024L;
  static const int64_t GET_SCHEMA_TIMEOUT = 10L * 1000L * 1000L;

  template <class Type>
  struct MetaNode
  {
    Type            *value_;
    int64_t         version_;
    MetaNode<Type>  *before_;
    MetaNode<Type>  *next_;

    void reset();
  };

  template <class Type>
  struct MetaInfo
  {
    typedef common::DefaultPageAllocator BaseAllocator;

    int64_t                 num_;
    MetaNode<Type>          *head_;
    MetaNode<Type>          *tail_;
    RWLock                  lock_;
    BaseAllocator           base_allocator_;
    common::ObFIFOAllocator fifo_allocator_;

    MetaInfo();
    ~MetaInfo();

    int get(const int64_t target_version, Type *&meta);
    int set(const int64_t version, Type *meta);
  };

  struct MetaKey
  {
    uint64_t id_;

    bool is_valid() const { return common::OB_INVALID_ID != id_; }
    uint64_t hash() const { return id_; }
    bool operator== (const MetaKey & other) const { return id_ == other.id_; }
    TO_STRING_KV(K_(id));
  };

  // multi-version table
  struct MulVerTableKey
  {
    int64_t version_;
    uint64_t table_id_;

    MulVerTableKey(const int64_t version,
        const uint64_t table_id) : version_(version), table_id_(table_id) {}

    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = common::murmurhash(&version_, sizeof(version_), hash_val);
      hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);

      return hash_val;
    }
    bool operator== (const MulVerTableKey & other) const
    { return (version_ == other.version_) && (table_id_ == other.table_id_); }

    TO_STRING_KV(K_(version), K_(table_id));
  };

  typedef MetaNode<ITableMeta> TableMetaNode;
  typedef MetaInfo<ITableMeta> TableMetaInfo;
  typedef common::ObLinearHashMap<MetaKey,  TableMetaInfo *> TableMetaMap;

  typedef MetaNode<IDBMeta> DBMetaNode;
  typedef MetaInfo<IDBMeta> DBMetaInfo;
  typedef common::ObLinearHashMap<MetaKey,  DBMetaInfo *> DBMetaMap;
  typedef common::ObLinearHashMap<MulVerTableKey, TableSchemaInfo *> MulVerTableSchemaMap;

private:
  template <class MetaMapType, class MetaInfoType>
  int get_meta_info_(MetaMapType &meta_map, const MetaKey &key, MetaInfoType *&meta_info);
  template <class MetaInfoType, class MetaType>
  int get_meta_from_meta_info_(MetaInfoType *meta_info, const int64_t version, MetaType *&meta);
  int add_and_get_table_meta_(TableMetaInfo *meta_info,
      const share::schema::ObTableSchema *table_schema,
      ObLogSchemaGuard &schema_mgr,
      ITableMeta *&table_meta,
      volatile bool &stop_flag);
  int add_and_get_db_meta_(DBMetaInfo *meta_info,
      const DBSchemaInfo &db_schema_info,
      const TenantSchemaInfo &tenant_schema_info,
      IDBMeta *&db_meta);
  template <class MetaType> static int inc_meta_ref_(MetaType *meta);
  template <class MetaType> static int dec_meta_ref_(MetaType *meta, int64_t &ref_cnt);
  int build_table_meta_(const share::schema::ObTableSchema *schema,
      ObLogSchemaGuard &schema_mgr,
      ITableMeta *&table_meta,
      volatile bool &stop_flag);
  int build_db_meta_(
      const DBSchemaInfo &db_schema_info,
      const TenantSchemaInfo &tenant_schema_info,
      IDBMeta *&db_meta);
  int build_column_metas_(ITableMeta *table_meta,
      const share::schema::ObTableSchema *table_schema,
      TableSchemaInfo &tb_schema_info,
      ObLogSchemaGuard &schema_mgr,
      volatile bool &stop_flag);
  // 1. won't filter hidden pk for table without primary key, column_name=__pk_increment, column_id=1
  // 2. filter hidden column
  // 3. filter non-user column
  // 4. filter invisible column by default, won't filter if config enable_output_invisible_column = 1
  int filter_column_(const share::schema::ObTableSchema &table_schema,
      const bool is_hidden_pk_table,
      const share::schema::ObColumnSchemaV2 &column_schema,
      bool &is_filter,
      bool &is_hidden_pk_table_pk_increment_column);
  int set_column_meta_(IColMeta *col_meta,
      const share::schema::ObColumnSchemaV2 &column_schema,
      const share::schema::ObTableSchema &table_schema);
  int set_primary_keys_(ITableMeta *table_meta,
      const share::schema::ObTableSchema *schema,
      const TableSchemaInfo &tb_schema_info);
  int set_unique_keys_(ITableMeta *table_meta,
      const share::schema::ObTableSchema *table_schema,
      const TableSchemaInfo &tb_schema_info,
      ObLogSchemaGuard &schema_mgr,
      volatile bool &stop_flag);
  int set_unique_keys_from_unique_index_table_(const share::schema::ObTableSchema *table_schema,
      const TableSchemaInfo &tb_schema_info,
      const share::schema::ObTableSchema *index_table_schema,
      bool *is_uk_column_array,
      ObLogAdaptString &uk_info,
      int64_t &valid_uk_column_count);
  int set_unique_keys_from_all_index_table_(int64_t &valid_uk_table_count,
      const share::schema::ObTableSchema &table_schema,
      const TableSchemaInfo &tb_schema_info,
      ObLogSchemaGuard &schema_mgr,
      volatile bool &stop_flag,
      bool *is_uk_column_array,
      ObLogAdaptString &uk_info);
  int build_ddl_meta_();

  int build_col_meta_(const char *ddl_col_name,
      IColMeta *&col_meta);
  void destroy_ddl_meta_();

  int alloc_table_schema_info_(TableSchemaInfo *&tb_schema_info);
  int free_table_schema_info_(TableSchemaInfo *&tb_schema_info);
  int set_column_schema_info_(const share::schema::ObTableSchema &table_schema,
      TableSchemaInfo &tb_schema_info,
      const int64_t column_idx,
      const share::schema::ObColumnSchemaV2 &column_table_schema);
  int set_table_schema_(const int64_t version,
      const uint64_t table_id,
      const char *table_name,
      const int64_t non_hidden_column_cnt,
      TableSchemaInfo &tb_schema_info);
  int try_erase_table_schema_(
      const uint64_t table_id,
      const int64_t version);

private:
  bool                  inited_;
  bool                  enable_output_hidden_primary_key_;
  ObObj2strHelper       *obj2str_helper_;
  ITableMeta            *ddl_table_meta_;
  DBMetaMap             db_meta_map_;
  TableMetaMap          tb_meta_map_;
  MulVerTableSchemaMap  tb_schema_info_map_;
  FIFOAllocator         allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMetaManager);
};

template <class Type>
void ObLogMetaManager::MetaNode<Type>::reset()
{
  value_ = NULL;
  version_ = 0;
  before_ = NULL;
  next_ = NULL;
}

template <class Type>
ObLogMetaManager::MetaInfo<Type>::MetaInfo() :
    num_(0),
    head_(NULL),
    tail_(NULL),
    lock_(),
    base_allocator_(common::ObModIds::OB_LOG_META_INFO),
    fifo_allocator_()
{
  fifo_allocator_.init(&base_allocator_, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  fifo_allocator_.set_label(common::ObModIds::OB_LOG_META_INFO);
}

template <class Type>
ObLogMetaManager::MetaInfo<Type>::~MetaInfo() { }

template <class Type>
int ObLogMetaManager::MetaInfo<Type>::get(const int64_t target_version, Type *&meta)
{
  int ret = common::OB_SUCCESS;
  meta = NULL;

  if (num_ > 0) {
    MetaNode<Type> *meta_node = head_;

    while (NULL != meta_node) {
      if (meta_node->version_ == target_version) {
        meta = meta_node->value_;
        break;
      } else if (meta_node->version_ < target_version) {
        break;
      } else {
        meta_node = meta_node->next_;
      }
    }
  }

  if (NULL == meta) {
    ret = common::OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

template <class Type>
int ObLogMetaManager::MetaInfo<Type>::set(const int64_t version, Type *meta)
{
  int ret = common::OB_SUCCESS;
  if (NULL == meta) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    // create a node
    MetaNode<Type> *meta_node =
        static_cast<MetaNode<Type> *>(fifo_allocator_.alloc(sizeof(MetaNode<Type>)));

    if (OB_ISNULL(meta_node)) {
      OBLOG_LOG(ERROR, "allocate memory for MetaNode fail", K(sizeof(MetaNode<Type>)));
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      meta_node->reset();

      meta_node->value_ = meta;
      meta_node->version_ = version;

      // put Node info linkedlist with inverser order of version
      if (NULL == head_) {
        head_ = meta_node;
        tail_ = head_;
        num_ = 1;
      } else if (OB_ISNULL(tail_)) {
        OBLOG_LOG(ERROR, "tail node is NULL, but head node is not NULL", K(head_), K(tail_));
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        MetaNode<Type> *node = head_;
        bool inserted = false;

        while (NULL != node) {
          if (node->version_ < version) {
            // insert ahead of the first version which is smaller than self
            meta_node->next_ = node;
            meta_node->before_ = node->before_;
            // if ahead node exist
            // make sure node with higher version point to the new node
            if (NULL != node->before_) {
              node->before_->next_ = meta_node;
            }
            node->before_ = meta_node;

            // deal with situation if node is head
            // node can't be the tail node
            if (node == head_) {
              head_ = meta_node;
            }

            inserted = true;
            break;
          } else if (node->version_ == version) {
            // error if node with same version already exist
            ret = common::OB_ENTRY_EXIST;
          } else {
            node = node->next_;
          }
        }

        if (OB_SUCC(ret)) {
          // put at tail of linkedlist if can't find version smaller than self
          if (! inserted) {
            tail_->next_ = meta_node;
            meta_node->before_ = tail_;
            meta_node->next_ = NULL;
            tail_ = meta_node;
          }

          num_++;
        }
      }
    }

    if (common::OB_SUCCESS != ret && NULL != meta_node) {
      fifo_allocator_.free(static_cast<void *>(meta_node));
      meta_node = NULL;
    }
  }
  return ret;
}

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBLOG_META_MANAGER_H__ */
