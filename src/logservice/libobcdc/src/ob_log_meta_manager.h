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
 *
 * Meta Manager
 */

#ifndef OCEANBASE_LIBOBCDC_META_MANAGER_H__
#define OCEANBASE_LIBOBCDC_META_MANAGER_H__

#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#else
#include <drcmsg/MD.h>                                    // ITableMeta, IDBMeta
#include <drcmsg/DRCMessageFactory.h>                     // DRCMessageFactory
#endif
#include "share/ob_errno.h"                               // OB_SUCCESS
#include "lib/lock/ob_spin_rwlock.h"                      // SpinRWLock, SpinRLockGuard, SpinWLockGuard
#include "lib/allocator/page_arena.h"                     // DefaultPageAllocator
#include "lib/allocator/ob_mod_define.h"                  // ObModIds
#include "lib/hash/ob_linear_hash_map.h"                  // ObLinearHashMap
#include "lib/allocator/ob_fifo_allocator.h"              // ObFIFOAllocator
#include "lib/allocator/ob_concurrent_fifo_allocator.h"   // ObConcurrentFIFOAllocator
#include "lib/allocator/ob_allocator.h"                   // ObIAllocator
#include "deps/oblib/src/common/rowkey/ob_rowkey_info.h"  // ObRowkeyInfo, ObIndexInfo
#include "share/schema/ob_table_param.h"                  // ObColDesc

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
namespace datadict
{
class ObDictTableMeta;
}

namespace libobcdc
{
class ObLogSchemaGuard;
class IObLogSchemaGetter;
class TableSchemaInfo;
class ColumnSchemaInfo;
class ObDictTenantInfo;
class ObDictTenantInfoGuard;

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
      const uint64_t tenant_id,
      const int64_t global_schema_version,
      const share::schema::ObSimpleTableSchemaV2 *table_schema,
      ITableMeta *&table_meta,
      volatile bool &stop_flag) = 0;

  virtual int get_table_meta(
      const uint64_t tenant_id,
      const int64_t global_schema_version,
      const datadict::ObDictTableMeta *table_schema,
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
      const uint64_t tenant_id,
      const DBSchemaInfo &db_schema_info,
      ObLogSchemaGuard &schema_mgr,
      IDBMeta *&db_meta,
      volatile bool &stop_flag) = 0;

  // for using data_dict
  virtual int get_db_meta(
      const uint64_t tenant_id,
      const DBSchemaInfo &db_schema_info,
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

  virtual int get_table_schema_meta(
      const int64_t version,
      const uint64_t tenant_id,
      const uint64_t table_id,
      TableSchemaInfo *&tb_schema_info) = 0;
};

class ObLogMetaManager : public IObLogMetaManager
{
private:
  static void set_column_encoding_(
      const common::ObObjType &col_type,
      const common::ObCharsetType &cs_type,
      IColMeta *meta);

public:
  ObLogMetaManager();
  virtual ~ObLogMetaManager();

public:
  virtual int get_table_meta(
      const uint64_t tenant_id,
      const int64_t global_schema_version,
      const share::schema::ObSimpleTableSchemaV2 *table_schema,
      ITableMeta *&table_meta,
      volatile bool &stop_flag);
  virtual int get_table_meta(
      const uint64_t tenant_id,
      const int64_t global_schema_version,
      const datadict::ObDictTableMeta *table_schema,
      ITableMeta *&table_meta,
      volatile bool &stop_flag);
  virtual ITableMeta *get_ddl_table_meta() { return ddl_table_meta_; }
  virtual void revert_table_meta(ITableMeta *table_meta);
  virtual int get_db_meta(
      const uint64_t tenant_id,
      const DBSchemaInfo &db_schema_info,
      ObLogSchemaGuard &schema_mgr,
      IDBMeta *&db_meta,
      volatile bool &stop_flag);

  virtual int get_db_meta(
      const uint64_t tenant_id,
      const DBSchemaInfo &db_schema_info,
      IDBMeta *&db_meta,
      volatile bool &stop_flag);
  virtual void revert_db_meta(IDBMeta *db_meta);
  virtual int drop_table(const int64_t table_id);
  virtual int drop_database(const int64_t database_id);
  virtual int get_table_schema_meta(
      const int64_t version,
      const uint64_t tenant_id,
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

    TO_STRING_KV(K_(num), K_(head), K_(tail));

    int get(const int64_t target_version, Type *&meta);
    int set(const int64_t version, Type *meta);
  };

  class MetaKey
  {
  public:
    MetaKey(
        const uint64_t tenant_id,
        const uint64_t id) : tenant_id_(tenant_id), id_(id) {}

    bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_ && common::OB_INVALID_ID != id_; }
    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
      hash_val = common::murmurhash(&id_, sizeof(id_), hash_val);
      return hash_val;
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool operator==(const MetaKey &other) const
    { return (tenant_id_ == other.tenant_id_) && (id_ == other.id_); }

    OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
    OB_INLINE uint64_t get_id() const { return id_; }

    TO_STRING_KV(K_(tenant_id), K_(id));
  private:
    uint64_t tenant_id_;
    uint64_t id_; // schema key id
  };

  // multi-version table
  class MulVerTableKey
  {
  public:
    MulVerTableKey(
        const int64_t version,
        const uint64_t tenant_id,
        const uint64_t table_id) : version_(version), tenant_id_(tenant_id), table_id_(table_id) {}

    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = common::murmurhash(&version_, sizeof(version_), hash_val);
      hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
      hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);

      return hash_val;
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool operator==(const MulVerTableKey &other) const
    { return (version_ == other.version_) && (tenant_id_ == other.tenant_id_) && (table_id_ == other.table_id_); }

    OB_INLINE int64_t get_version() const { return version_; }
    OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
    OB_INLINE uint64_t get_table_id() const { return table_id_; }

    TO_STRING_KV(K_(version), K_(tenant_id), K_(table_id));
  private:
    int64_t version_;
    uint64_t tenant_id_;
    uint64_t table_id_;
  };

  typedef MetaNode<ITableMeta> TableMetaNode;
  typedef MetaInfo<ITableMeta> TableMetaInfo;
  typedef common::ObLinearHashMap<MetaKey,  TableMetaInfo *> TableMetaMap;

  typedef MetaNode<IDBMeta> DBMetaNode;
  typedef MetaInfo<IDBMeta> DBMetaInfo;
  typedef common::ObLinearHashMap<MetaKey,  DBMetaInfo *> DBMetaMap;
  typedef common::ObLinearHashMap<MulVerTableKey, TableSchemaInfo *> MulVerTableSchemaMap;

private:
  int get_dict_tenant_info_(
      const uint64_t tenant_id,
      ObDictTenantInfoGuard &dict_tenant_info_guard,
      ObDictTenantInfo *&tenant_info);
  template <class MetaMapType, class MetaInfoType>
  int get_meta_info_(MetaMapType &meta_map, const MetaKey &key, MetaInfoType *&meta_info);
  template <class MetaInfoType, class MetaType>
  int get_meta_from_meta_info_(MetaInfoType *meta_info, const int64_t version, MetaType *&meta);
  template<class SCHEMA_GUARD, class TABLE_SCHEMA>
  int add_and_get_table_meta_(
      TableMetaInfo *meta_info,
      const TABLE_SCHEMA *table_schema,
      const ObIArray<uint64_t> &usr_def_col_ids,
      SCHEMA_GUARD &schema_mgr,
      ITableMeta *&table_meta,
      volatile bool &stop_flag);
  int add_and_get_db_meta_(
      DBMetaInfo *meta_info,
      const DBSchemaInfo &db_schema_info,
      const TenantSchemaInfo &tenant_schema_info,
      IDBMeta *&db_meta);
  template <class MetaType> static int inc_meta_ref_(MetaType *meta);
  template <class MetaType> static int dec_meta_ref_(MetaType *meta, int64_t &ref_cnt);
  template<class SCHEMA_GUARD, class TABLE_SCHEMA>
  int build_table_meta_(
      const TABLE_SCHEMA *schema,
      const ObIArray<uint64_t> &usr_def_col_ids,
      SCHEMA_GUARD &schema_mgr,
      ITableMeta *&table_meta,
      volatile bool &stop_flag);
  int build_db_meta_(
      const DBSchemaInfo &db_schema_info,
      const TenantSchemaInfo &tenant_schema_info,
      IDBMeta *&db_meta);
  int get_usr_def_col_from_table_schema_(
      const share::schema::ObTableSchema &schema,
      ObIArray<uint64_t> &usr_def_col);
  template<class TABLE_SCHEMA>
  int build_column_idx_mappings_(
      const TABLE_SCHEMA *table_schema,
      const ObIArray<uint64_t> &usr_def_col_ids,
      const common::ObIArray<share::schema::ObColDesc> &column_ids,
      ObIArray<int16_t> &store_idx_to_usr_idx,
      int16_t &usr_column_cnt,
      volatile bool &stop_flag);
  template<class SCHEMA_GUARD, class TABLE_SCHEMA>
  int build_column_metas_(
      ITableMeta *table_meta,
      const TABLE_SCHEMA *table_schema,
      const ObIArray<uint64_t> &usr_def_col_ids,
      TableSchemaInfo &tb_schema_info,
      SCHEMA_GUARD &schema_mgr,
      volatile bool &stop_flag);
  // check_column and exact column properties useful for column_schema.
  //
  // @param [in]    table_schema          corresponding table_schema(get from OBServer or DICT).
  // @param [in]    column_schema         corresponding column_schema(get from OBServer or DICT),
  // @param [out]   is_usr_column         is column need by logmsg.
  // @param [out]   is_heap_table_pk_increment_column  is pk_increment column of heap table.
  template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
  int check_column_(
      const TABLE_SCHEMA &table_schema,
      const COLUMN_SCHEMA &column_schema,
      bool &is_usr_column,
      bool &is_heap_table_pk_increment_column);
  template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
  int set_column_meta_(
      IColMeta *col_meta,
      const COLUMN_SCHEMA &column_schema,
      const TABLE_SCHEMA &table_schema);
  void set_column_type_(IColMeta &col_meta, const obmysql::EMySQLFieldType &col_type);
  template<class TABLE_SCHEMA>
  int set_primary_keys_(
      ITableMeta *table_meta,
      const TABLE_SCHEMA*schema,
      const TableSchemaInfo &tb_schema_info);
  template<class TABLE_SCHEMA>
  int get_logic_primary_keys_for_heap_table_(
      const TABLE_SCHEMA &table_schema,
      ObIArray<uint64_t> &pk_list);
  template<class TABLE_SCHEMA>
  int fill_primary_key_info_(
      const TABLE_SCHEMA &table_schema,
      const ColumnSchemaInfo &column_schema_info,
      ObLogAdaptString &pks,
      ObLogAdaptString &pk_info,
      int64_t &valid_pk_num);
  template<class SCHEMA_GUARD, class TABLE_SCHEMA>
  int set_unique_keys_(
      ITableMeta *table_meta,
      const TABLE_SCHEMA *table_schema,
      const TableSchemaInfo &tb_schema_info,
      SCHEMA_GUARD &schema_mgr,
      volatile bool &stop_flag);

  int set_unique_keys_from_unique_index_table_(
      const share::schema::ObTableSchema *table_schema,
      const share::schema::ObTableSchema *index_table_schema,
      const TableSchemaInfo &tb_schema_info,
      bool *is_uk_column_array,
      ObLogAdaptString &uk_info,
      int64_t &valid_uk_column_count);
  int set_unique_keys_from_unique_index_table_(
      const datadict::ObDictTableMeta *table_schema,
      const datadict::ObDictTableMeta *index_table_schema,
      const TableSchemaInfo &tb_schema_info,
      bool *is_uk_column_array,
      ObLogAdaptString &uk_info,
      int64_t &valid_uk_column_count);
  template<class TABLE_SCHEMA>
  int build_unique_keys_with_index_column_(
      const TABLE_SCHEMA *table_schema,
      const TABLE_SCHEMA *index_table_schema,
      const common::ObIndexInfo &index_info,
      const int64_t index_column_idx,
      const TableSchemaInfo &tb_schema_info,
      bool *is_uk_column_array,
      ObLogAdaptString &uk_info,
      int64_t &valid_uk_column_count);
  template<class SCHEMA_GUARD, class TABLE_SCHEMA>
  int set_unique_keys_from_all_index_table_(
      const TABLE_SCHEMA &table_schema,
      const TableSchemaInfo &tb_schema_info,
      SCHEMA_GUARD &schema_mgr,
      volatile bool &stop_flag,
      bool *is_uk_column_array,
      ObLogAdaptString &uk_info,
      int64_t &valid_uk_table_count);
  int build_ddl_meta_();

  int build_col_meta_(const char *ddl_col_name,
      IColMeta *&col_meta);
  void destroy_ddl_meta_();

  int alloc_table_schema_info_(TableSchemaInfo *&tb_schema_info);
  int free_table_schema_info_(TableSchemaInfo *&tb_schema_info);
  template<class TABLE_SCHEMA, class COLUMN_SCHEMA>
  int set_column_schema_info_(
      const TABLE_SCHEMA &table_schema,
      const COLUMN_SCHEMA &column_table_schema,
      const int16_t column_stored_idx,
      const bool is_usr_column,
      const int16_t usr_column_idx,
      TableSchemaInfo &tb_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap);
  int set_table_schema_(
      const int64_t version,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const char *table_name,
      const int64_t non_hidden_column_cnt,
      TableSchemaInfo &tb_schema_info);
  int try_erase_table_schema_(
      const uint64_t tenant_id,
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
    lock_(common::ObLatchIds::OBCDC_METAINFO_LOCK),
    base_allocator_(common::ObModIds::OB_LOG_META_INFO),
    fifo_allocator_()
{
  fifo_allocator_.init(&base_allocator_, common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                       ObMemAttr(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_LOG_META_INFO));
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

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_META_MANAGER_H__ */
