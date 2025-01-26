/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_SCHEMA_CACHE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_SCHEMA_CACHE_H_
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "src/share/table/ob_ttl_util.h"
#include "share/schema/ob_schema_utils.h"
#include "ob_htable_utils.h"

namespace oceanbase
{

namespace table
{

struct ObTableColumnInfo
{
  ObTableColumnInfo()
      : col_idx_(common::OB_INVALID_ID),
        column_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        column_name_(),
        default_value_(),
        auto_filled_timestamp_(false),
        column_flags_(NON_CASCADE_FLAG),
        is_auto_increment_(false),
        is_nullable_(true),
        is_tbl_part_key_column_(false),
        is_rowkey_column_(false),
        rowkey_position_(-1),
        tbl_part_key_pos_(-1),
        type_(),
        generated_expr_str_(),
        cascaded_column_ids_()
  {
  }

  void reset() {
    col_idx_ = common::OB_INVALID_ID;
    column_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    column_name_.reset();
    default_value_.reset();
    auto_filled_timestamp_ = false;
    column_flags_ = NON_CASCADE_FLAG;
    is_auto_increment_ = false;
    is_nullable_ = false;
    is_tbl_part_key_column_ = false;
    is_rowkey_column_ = false;
    rowkey_position_ = -1;
    tbl_part_key_pos_ = -1;
    type_.reset();
    generated_expr_str_.reset();
    cascaded_column_ids_.reset();
  }

  TO_STRING_KV(K_(col_idx),
               K_(column_id),
               K_(table_id),
               K_(column_name),
               K_(default_value),
               K_(auto_filled_timestamp),
               K_(column_flags),
               K_(is_auto_increment),
               K_(is_nullable),
               K_(rowkey_position),
               K_(type),
               K_(generated_expr_str),
               K_(cascaded_column_ids));

  OB_INLINE bool has_column_flag(int64_t flag) const { return column_flags_ & flag; }
  OB_INLINE int64_t get_column_flags() const { return column_flags_; }
  OB_INLINE bool is_virtual_generated_column() const { return column_flags_ & VIRTUAL_GENERATED_COLUMN_FLAG; }
  OB_INLINE bool is_stored_generated_column() const { return column_flags_ & STORED_GENERATED_COLUMN_FLAG; }
  OB_INLINE bool is_generated_column() const { return (is_virtual_generated_column() || is_stored_generated_column()); }
  OB_INLINE bool is_fulltext_column() const { return ObSchemaUtils::is_fulltext_column(column_flags_); }
  OB_INLINE bool is_doc_id_column() const { return ObSchemaUtils::is_doc_id_column(column_flags_); }
  OB_INLINE bool is_word_segment_column() const { return ObSchemaUtils::is_word_segment_column(column_flags_); }
  OB_INLINE bool is_word_count_column() const { return ObSchemaUtils::is_word_count_column(column_flags_); }
  OB_INLINE bool is_doc_length_column() const { return ObSchemaUtils::is_doc_length_column(column_flags_); }

  uint64_t col_idx_;
  uint64_t column_id_;
  uint64_t table_id_;
  common::ObString column_name_;
  common::ObObj default_value_;
  bool auto_filled_timestamp_;
  int64_t column_flags_;
  bool is_auto_increment_;
  bool is_nullable_;
  bool is_tbl_part_key_column_;
  bool is_rowkey_column_;
  int64_t rowkey_position_; // greater than zero if this is rowkey column, 0 if this is common column
  union {
    struct {
      uint64_t part_key_pos_:8;//partition key pos
      uint64_t subpart_key_pos_:8;//subpartition key pos
    } part_pos_;//partition key
    int64_t tbl_part_key_pos_;
  };//greater than zero if this column is used to calc part expr
  sql::ObExprResType type_;
  common::ObString generated_expr_str_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> cascaded_column_ids_;
};

struct ObTableRowkeyInfo
{
  ObTableRowkeyInfo() : column_id_(common::OB_INVALID_ID) {}

  void reset()
  {
    column_id_ = common::OB_INVALID_ID;
  }

  TO_STRING_KV(K_(column_id));

  uint64_t column_id_;
};

  // store some marks so that can fast judge
union ObTableSchemaFlags{
  uint64_t value_;
  struct {
    bool has_auto_inc_          : 1;
    bool has_local_index_       : 1;
    bool has_global_index_      : 1;
    bool has_generated_column_  : 1;
    bool is_ttl_table_          : 1;
    bool is_partitioned_table_  : 1;
    bool has_lob_column_        : 1;
    bool has_fts_index_         : 1;
    bool has_hbase_ttl_column_  : 1;
    uint64_t reserved_          : 55;
  };
};

struct ObKvSchemaCacheKey: public sql::ObILibCacheKey
{
  ObKvSchemaCacheKey()
    : ObILibCacheKey(sql::ObLibCacheNameSpace::NS_KV_SCHEMA),
      table_id_(common::OB_INVALID_ID),
      schema_version_(-1)
  {}
  void reset();
  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other);
  virtual uint64_t hash() const;
  virtual bool is_equal(const ObILibCacheKey &other) const;
  TO_STRING_KV(K_(table_id),
               K_(schema_version));
  common::ObTableID table_id_;
  int64_t schema_version_;
};

typedef common::ObPooledAllocator<common::hash::HashMapTypes<ObColumnSchemaHashWrapper, int64_t>
      ::AllocType, common::ObWrapperAllocator> NameIdxMapAllocator;
typedef common::ObPooledAllocator<common::hash::HashMapTypes<uint64_t, int64_t>
      ::AllocType, common::ObWrapperAllocator> ColidIdxMapAllocator;
typedef common::ObFixedArray<ObTableColumnInfo*, common::ObIAllocator> ObTableColumInfoArray;
typedef common::ObFixedArray<ObTableRowkeyInfo*, common::ObIAllocator> ObTableRowkeyInfoArray;
typedef common::hash::ObHashMap<ObColumnSchemaHashWrapper, int64_t, common::hash::NoPthreadDefendMode,
                                common::hash::hash_func<ObColumnSchemaHashWrapper>,
                                common::hash::equal_to<ObColumnSchemaHashWrapper>,
                                NameIdxMapAllocator,
                                common::hash::NormalPointer,
                                common::ObWrapperAllocator> ColNameIdxMap;
typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode,
                                common::hash::hash_func<uint64_t>,
                                common::hash::equal_to<uint64_t>,
                                ColidIdxMapAllocator,
                                common::hash::NormalPointer,
                                common::ObWrapperAllocator> ColIdIdxMap;
class ObKvSchemaCacheObj: public sql::ObILibCacheObject
{
public:
	ObKvSchemaCacheObj(lib::MemoryContext &mem_context)
      : ObILibCacheObject(sql::ObLibCacheNameSpace::NS_KV_SCHEMA, mem_context),
        bucket_allocator_wrapper_(allocator_),
        name_idx_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, bucket_allocator_wrapper_),
        colid_idx_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, bucket_allocator_wrapper_),
        table_name_(),
        column_info_array_(allocator_),
        rowkey_info_array_(allocator_),
        local_index_tids_(allocator_),
        global_index_tids_(allocator_)
	{
    flags_.value_ = 0;
    column_cnt_ = 0;
    rowkey_cnt_ = 0;
    auto_inc_cache_size_ = 0;
  }
  virtual ~ObKvSchemaCacheObj();
  int cons_table_info(const ObTableSchema *table_schema);
  int cons_index_info(ObSchemaGetterGuard *schema_guard, uint64_t tenant_id, common::ObTableID table_id);
  int cons_columns_array(const ObTableSchema *table_schema);
  int cons_rowkey_array(const ObTableSchema *table_schema);

public:
  OB_INLINE const common::ObString &get_table_name_str() { return table_name_; }
  OB_INLINE const ObIArray<uint64_t>& get_local_index_tids() { return local_index_tids_; }
  OB_INLINE const ObIArray<uint64_t>& get_global_index_tids() { return global_index_tids_; }
  OB_INLINE ColNameIdxMap& get_col_name_map() { return col_name_idx_map_; }
  OB_INLINE ColIdIdxMap& get_col_id_map() { return col_id_idx_map_; }
  OB_INLINE ObTableColumInfoArray& get_column_info_array() { return column_info_array_; }
  OB_INLINE ObTableRowkeyInfoArray& get_rowkey_info_array() { return rowkey_info_array_; }
  OB_INLINE ObTableSchemaFlags get_schema_flags() { return flags_; }
  OB_INLINE void set_has_auto_inc(bool has_auto_inc) { flags_.has_auto_inc_ = has_auto_inc; }
  OB_INLINE void set_has_generated_column(bool has_generated_column) { flags_.has_generated_column_ = has_generated_column; }
  OB_INLINE void set_is_ttl_table(bool is_ttl_table) { flags_.is_ttl_table_ = is_ttl_table; }
  OB_INLINE void set_is_partitioned_table(bool is_partitioned_table) { flags_.is_partitioned_table_ = is_partitioned_table; }
  OB_INLINE void set_has_lob_column(bool has_lob_column) { flags_.has_lob_column_ = has_lob_column; }
  OB_INLINE int64_t get_column_count() { return column_cnt_; }
  OB_INLINE int64_t get_rowkey_count() { return rowkey_cnt_; }
  OB_INLINE const ObKVAttr& get_kv_attributes() { return kv_attributes_; }
  OB_INLINE const ObString& get_ttl_definition() { return ttl_definition_; }
  OB_INLINE int64_t get_auto_inc_cache_size() { return auto_inc_cache_size_; }
private:
  int build_index_map();
private:
  ObTableSchemaFlags flags_;
private:
  common::ObWrapperAllocator bucket_allocator_wrapper_;
  NameIdxMapAllocator name_idx_allocator_;
  ColidIdxMapAllocator colid_idx_allocator_;
  ObString table_name_;
  ObTableColumInfoArray column_info_array_;
  ObTableRowkeyInfoArray rowkey_info_array_;
  ColNameIdxMap col_name_idx_map_;
  ColIdIdxMap col_id_idx_map_;
  ObFixedArray<uint64_t, common::ObIAllocator> local_index_tids_;
  ObFixedArray<uint64_t, common::ObIAllocator> global_index_tids_;
  ObString ttl_definition_;
  int64_t column_cnt_;
  int64_t rowkey_cnt_;
  int64_t auto_inc_cache_size_;
  ObKVAttr kv_attributes_;
};

class ObKvSchemaCacheGuard
{
public:
  ObKvSchemaCacheGuard()
          : is_init_(false),
            is_use_cache_(false),
            lib_cache_(nullptr),
            cache_guard_(sql::CacheRefHandleID::KV_SCHEMA_INFO_HANDLE) {}
  ~ObKvSchemaCacheGuard() {}

  TO_STRING_KV(K_(is_init),
            K_(is_use_cache),
            K_(cache_key),
            KP_(lib_cache),
            K_(cache_guard));

  int init(uint64_t tenant_id, uint64_t table_id, int64_t schema_version, ObSchemaGetterGuard &schema_guard);
  int get_column_info(const ObString &col_name, const ObTableColumnInfo *&col_info);
  int get_column_info(uint64_t col_id, const ObTableColumnInfo *&col_info);
  int get_column_info_by_idx(const int64_t idx, const ObTableColumnInfo *&col_info);
  int get_column_info_idx(uint64_t col_id, int64_t &idx);
  int get_column_info_idx(const ObString &col_name, int64_t &idx);
  int get_cache_obj(ObKvSchemaCacheObj *&cache_obj);
  int get_column_count(int64_t& column_count);
  int get_rowkey_column_num(int64_t& rowkey_column_num);
  int get_rowkey_column_ids(common::ObIArray<uint64_t> &column_ids);
  int get_rowkey_column_id(const int64_t index, uint64_t &column_id);
  int get_column_ids(ObIArray<uint64_t> &column_ids);
  int has_generated_column(bool &has_generated_column);
  int is_ttl_table(bool &is_ttl_table);
  int has_hbase_ttl_column(bool &is_enable);
  int is_partitioned_table(bool &is_partitioned_table);
  int has_global_index(bool &has_gloabl_index);
  int get_kv_attributes(ObKVAttr &kv_attributes);
  int get_ttl_definition(ObString& ttl_definition);
  int get_or_create_cache_obj(ObSchemaGetterGuard &schema_guard);
  int create_schema_cache_obj(ObSchemaGetterGuard &schema_guard);
  int is_redis_ttl_table(bool &is_redis_ttl_table);
  void reset();
  OB_INLINE bool is_use_cache() { return is_use_cache_; }
  OB_INLINE bool is_inited() { return is_init_; }
  OB_INLINE const common::ObString &get_table_name_str()
  {
    return static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj())->get_table_name_str();
  }
  OB_INLINE const ObIArray<ObTableColumnInfo *>& get_column_info_array()
  {
    return static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj())->get_column_info_array();
  }
  OB_INLINE ObTableSchemaFlags get_schema_flags()
  {
    return static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj())->get_schema_flags();
  }
  OB_INLINE const ObIArray<uint64_t>& get_local_index_tids()
  {
    return static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj())->get_local_index_tids();
  }
  OB_INLINE const ObIArray<uint64_t>& get_global_index_tids()
  {
    return static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj())->get_global_index_tids();
  }
  OB_INLINE int64_t get_auto_inc_cache_size()
  {
    return static_cast<ObKvSchemaCacheObj *>(cache_guard_.get_cache_obj())->get_auto_inc_cache_size();
  }
private:
  bool is_init_;
  bool is_use_cache_;
  sql::ObPlanCache *lib_cache_;
  ObKvSchemaCacheKey cache_key_;
  sql::ObCacheObjGuard cache_guard_;
  sql::ObILibCacheCtx cache_ctx_;
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_SCHEMA_CACHE_H_ */