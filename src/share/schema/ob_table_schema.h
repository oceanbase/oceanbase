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

#ifndef OCEANBASE_SCHEMA_TABLE_SCHEMA
#define OCEANBASE_SCHEMA_TABLE_SCHEMA

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>
#include "lib/utility/utility.h"
#include "lib/charset/ob_charset.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/objectpool/ob_pool.h"
#include "common/row/ob_row.h"
#include "common/rowkey/ob_rowkey_info.h"
#include "common/object/ob_object.h"
#include "common/ob_range.h"
#include "common/ob_store_format.h"
#include "common/ob_partition_key.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObSchemaGetterGuard;
class ObColDesc;
class ObConstraint;
class ObColumnSchemaV2;
struct ObColumnIdKey {
  uint64_t column_id_;

  explicit ObColumnIdKey() : column_id_(common::OB_INVALID_ID)
  {}
  explicit ObColumnIdKey(const uint64_t column_id) : column_id_(column_id)
  {}

  ObColumnIdKey& operator=(const uint64_t column_id)
  {
    column_id_ = column_id;
    return *this;
  }

  inline operator uint64_t() const
  {
    return column_id_;
  }

  inline uint64_t hash() const
  {
    return ((column_id_ * 29 + 7) & 0xFFFF);
  }
};

template <class K, class V>
struct ObGetColumnKey {
  void operator()(const K& k, const V& v) const
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template <>
struct ObGetColumnKey<ObColumnIdKey, ObColumnSchemaV2*> {
  ObColumnIdKey operator()(const ObColumnSchemaV2* column_schema) const;
};

template <>
struct ObGetColumnKey<ObColumnSchemaHashWrapper, ObColumnSchemaV2*> {
  ObColumnSchemaHashWrapper operator()(const ObColumnSchemaV2* column_schema) const;
};

typedef common::hash::ObPointerHashArray<ObColumnIdKey, ObColumnSchemaV2*, ObGetColumnKey> IdHashArray;
typedef common::hash::ObPointerHashArray<ObColumnSchemaHashWrapper, ObColumnSchemaV2*, ObGetColumnKey> NameHashArray;
typedef const common::ObObj& (ObColumnSchemaV2::*get_default_value)() const;

extern const uint64_t HIDDEN_PK_COLUMN_IDS[3];
extern const char* HIDDEN_PK_COLUMN_NAMES[3];
const uint64_t BORDER_COLUMN_ID = 0;

typedef struct TableJoinType_ {
  std::pair<uint64_t, uint64_t> table_pair_;
  // sql::ObJoinType type_;
  int join_type_;
  TO_STRING_KV(K_(table_pair), K_(join_type));
} TableJoinType;

enum ObTableModeFlag {
  TABLE_MODE_NORMAL = 0,
  TABLE_MODE_RESERVED = 1,
  TABLE_MODE_MAX = 2,
};

enum ObTablePKMode {
  TPKM_OLD_NO_PK = 0,
  TPKM_NEW_NO_PK = 1,
  TPKM_MAX = 2,
};

struct ObTableMode {
  OB_UNIS_VERSION_V(1);

private:
  static const int32_t TM_MODE_FLAG_BITS = 8;
  static const int32_t TM_PK_MODE_OFFSET = 8;
  static const int32_t TM_PK_MODE_BITS = 4;
  static const int32_t TM_RESERVED = 20;

  static const uint32_t MODE_FLAG_MASK = (1U << TM_MODE_FLAG_BITS) - 1;
  static const uint32_t PK_MODE_MASK = (1U << TM_PK_MODE_BITS) - 1;

public:
  ObTableMode()
  {
    reset();
  }
  virtual ~ObTableMode()
  {
    reset();
  }
  void reset()
  {
    mode_ = 0;
  }
  bool operator==(const ObTableMode& other) const
  {
    return mode_ == other.mode_;
  }
  int assign(const ObTableMode& other);
  ObTableMode& operator=(const ObTableMode& other);
  bool is_valid() const;

  static ObTableModeFlag get_table_mode_flag(int32_t table_mode)
  {
    return (ObTableModeFlag)(table_mode & MODE_FLAG_MASK);
  }
  static ObTablePKMode get_table_pk_mode(int32_t table_mode)
  {
    return (ObTablePKMode)((table_mode >> TM_PK_MODE_OFFSET) & PK_MODE_MASK);
  }

  TO_STRING_KV("table_mode_flag", mode_flag_, "pk_mode", pk_mode_);
  union {
    int32_t mode_;
    struct {
      uint32_t mode_flag_ : TM_MODE_FLAG_BITS;
      uint32_t pk_mode_ : TM_PK_MODE_BITS;
      uint32_t reserved_ : TM_RESERVED;
    };
  };
};

struct ObBackUpTableModeOp {
  /*
      "NEW_NO_PK_MODE":TPKM_NEW_NO_PK
  */
  static common::ObString get_table_mode_str(const ObTableMode mode)
  {
    common::ObString ret_str = "";
    if (TPKM_NEW_NO_PK == mode.pk_mode_) {
      ret_str = "NEW_NO_PK_MODE";
    }
    return ret_str;
  }

  static int get_table_mode(const common::ObString str, ObTableMode& ret_mode)
  {
    int ret = common::OB_SUCCESS;
    ret_mode.reset();
    char* flag = nullptr;
    const char* delim = "|";
    char* save_ptr = NULL;
    char table_mode_str[str.length() + 1];
    MEMSET(table_mode_str, '\0', str.length() + 1);
    std::strncpy(table_mode_str, str.ptr(), str.length());
    flag = strtok_r(table_mode_str, delim, &save_ptr);
    while (OB_SUCC(ret) && OB_NOT_NULL(flag)) {
      common::ObString flag_str(0, strlen(flag), flag);
      if (0 == flag_str.case_compare("normal")) {
        // do nothing
      } else if (0 == flag_str.case_compare("new_no_pk_mode")) {
        ret_mode.pk_mode_ = TPKM_NEW_NO_PK;
      } else {
        ret = common::OB_ERR_PARSER_SYNTAX;
      }
      flag = strtok_r(NULL, delim, &save_ptr);
    }
    return ret;
  }
};

class ObSimpleTableSchemaV2 : public ObPartitionSchema {
public:
  ObSimpleTableSchemaV2();
  explicit ObSimpleTableSchemaV2(common::ObIAllocator* allocator);
  ObSimpleTableSchemaV2(const ObSimpleTableSchemaV2& src_schema) = delete;
  virtual ~ObSimpleTableSchemaV2();
  ObSimpleTableSchemaV2& operator=(const ObSimpleTableSchemaV2& other) = delete;
  int assign(const ObSimpleTableSchemaV2& src_schema);
  bool operator==(const ObSimpleTableSchemaV2& other) const;
  void reset() override;
  void reset_partition_schema();
  bool is_valid() const override;
  bool is_link_valid() const;
  int64_t get_convert_size() const override;
  inline void set_tenant_id(const uint64_t tenant_id) override
  {
    tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const override
  {
    return tenant_id_;
  }
  inline virtual void set_table_id(const uint64_t table_id) override
  {
    table_id_ = table_id;
  }
  inline virtual uint64_t get_table_id() const override
  {
    return table_id_;
  }
  inline void set_schema_version(const int64_t schema_version) override
  {
    schema_version_ = schema_version;
  }
  inline int64_t get_schema_version() const override
  {
    return schema_version_;
  }
  inline const char* get_locality() const
  {
    return extract_str(locality_str_);
  }
  virtual const common::ObString& get_locality_str() const override
  {
    return locality_str_;
  }
  virtual common::ObString& get_locality_str() override
  {
    return locality_str_;
  }
  inline const char* get_previous_locality() const
  {
    return extract_str(previous_locality_str_);
  }
  virtual const common::ObString& get_previous_locality_str() const override
  {
    return previous_locality_str_;
  }
  inline int set_locality(const char* locality)
  {
    return deep_copy_str(locality, locality_str_);
  }
  inline int set_locality(const common::ObString& locality)
  {
    return deep_copy_str(locality, locality_str_);
  }
  inline int set_previous_locality(const char* previous_locality)
  {
    return deep_copy_str(previous_locality, previous_locality_str_);
  }
  inline int set_previous_locality(const common::ObString& previous_locality)
  {
    return deep_copy_str(previous_locality, previous_locality_str_);
  }
  inline void set_database_id(const uint64_t database_id)
  {
    database_id_ = database_id;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  virtual void set_tablegroup_id(const uint64_t tablegroup_id) override
  {
    tablegroup_id_ = tablegroup_id;
  }
  virtual uint64_t get_tablegroup_id() const override
  {
    return tablegroup_id_;
  }
  inline void set_data_table_id(const uint64_t data_table_id)
  {
    data_table_id_ = data_table_id;
  }
  inline uint64_t get_data_table_id() const
  {
    return data_table_id_;
  }
  inline int set_table_name(const common::ObString& table_name)
  {
    return deep_copy_str(table_name, table_name_);
  }
  inline const char* get_table_name() const
  {
    return extract_str(table_name_);
  }
  virtual const char* get_entity_name() const override
  {
    return extract_str(table_name_);
  }
  inline const common::ObString& get_table_name_str() const
  {
    return table_name_;
  }
  inline const common::ObString& get_origin_index_name_str() const
  {
    return origin_index_name_;
  }
  inline void set_name_case_mode(const common::ObNameCaseMode cmp_mode)
  {
    name_case_mode_ = cmp_mode;
  }
  inline common::ObNameCaseMode get_name_case_mode() const
  {
    return name_case_mode_;
  }
  inline void set_table_type(const ObTableType table_type)
  {
    table_type_ = table_type;
  }
  inline ObTableType get_table_type() const
  {
    return table_type_;
  }
  inline void set_table_mode(const int32_t table_mode)
  {
    table_mode_.mode_ = table_mode;
  }
  inline int32_t get_table_mode() const
  {
    return table_mode_.mode_;
  }
  inline void set_table_mode_struct(const ObTableMode table_mode)
  {
    table_mode_ = table_mode;
  }
  inline ObTableMode get_table_mode_struct() const
  {
    return table_mode_;
  }
  inline ObTableModeFlag get_table_mode_flag() const
  {
    return (ObTableModeFlag)table_mode_.mode_flag_;
  }
  inline ObTablePKMode get_table_pk_mode() const
  {
    return (ObTablePKMode)table_mode_.pk_mode_;
  }
  // Is it a new table without a primary key
  inline bool is_new_no_pk_table() const
  {
    return TPKM_NEW_NO_PK == (enum ObTablePKMode)table_mode_.pk_mode_;
  }
  inline int set_primary_zone(const common::ObString& primary_zone)
  {
    return deep_copy_str(primary_zone, primary_zone_);
  }
  int set_primary_zone_array(const common::ObIArray<ObZoneScore>& primary_zone_array);
  inline virtual void set_min_partition_id(uint64_t partition_id) override
  {
    min_partition_id_ = partition_id;
  }
  inline void set_session_id(const uint64_t id)
  {
    session_id_ = id;
  }
  inline virtual uint64_t get_min_partition_id() const override
  {
    return min_partition_id_;
  }
  inline const common::ObString& get_primary_zone() const
  {
    return primary_zone_;
  }
  inline uint64_t get_session_id() const
  {
    return session_id_;
  }
  int set_zone_list(const common::ObIArray<common::ObString>& zone_list);
  int set_zone_list(const common::ObIArray<common::ObZone>& zone_list);
  int get_zone_list(common::ObIArray<common::ObZone>& zone_list) const;
  virtual int get_zone_list(
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<common::ObZone>& zone_list) const override;
  virtual int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard& schema_guard, share::schema::ObPrimaryZone& primary_zone) const override;
  virtual int get_paxos_replica_num(share::schema::ObSchemaGetterGuard& guard, int64_t& num) const override;
  virtual int check_is_duplicated(share::schema::ObSchemaGetterGuard& guard, bool& is_duplicated) const override;
  virtual int get_first_primary_zone_inherit(share::schema::ObSchemaGetterGuard& schema_guard,
      const rootserver::ObRandomZoneSelector& random_selector,
      const common::ObIArray<rootserver::ObReplicaAddr>& replica_addrs,
      common::ObZone& first_primary_zone) const override;
  virtual int get_zone_replica_attr_array(ZoneLocalityIArray& locality) const;
  virtual int get_zone_replica_attr_array_inherit(
      ObSchemaGetterGuard& guard, ZoneLocalityIArray& locality) const override;
  virtual int get_locality_str_inherit(
      share::schema::ObSchemaGetterGuard& guard, const common::ObString*& locality_str) const override;
  void reset_zone_replica_attr_array();
  int set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet>& src);
  int set_zone_replica_attr_array(const common::ObIArray<SchemaZoneReplicaAttrSet>& src);
  int set_specific_replica_attr_array(
      share::SchemaReplicaAttrArray& schema_replica_set, const common::ObIArray<ReplicaAttr>& src);
  int get_all_replica_num(share::schema::ObSchemaGetterGuard& guard,
      int64_t& num) const;  // except R{all_server}
  int get_full_replica_num(share::schema::ObSchemaGetterGuard& guard, int64_t& num) const;
  int check_is_readonly_at_all(share::schema::ObSchemaGetterGuard& guard, const common::ObZone& zone,
      const common::ObRegion& region, bool& readonly_at_all) const;
  int check_has_all_server_readonly_replica(share::schema::ObSchemaGetterGuard& guard, bool& has) const;
  int check_is_all_server_readonly_replica(share::schema::ObSchemaGetterGuard& guard, bool& is) const;
  virtual int check_has_own_not_f_replica(bool &has_not_f_replica) const override;

  void reset_locality_options();

  void reset_primary_zone_options();
  int get_raw_first_primary_zone(
      const rootserver::ObRandomZoneSelector& random_selector, common::ObZone& first_primary_zone) const;
  int get_primary_zone_score(const common::ObZone& zone, int64_t& zone_score) const;
  inline const common::ObIArray<ObZoneScore>& get_primary_zone_array() const
  {
    return primary_zone_array_;
  }
  int get_primary_zone_array(common::ObIArray<ObZoneScore>& primary_zone_array) const;

  void set_binding(const bool binding)
  {
    binding_ = binding;
  }
  bool get_binding() const
  {
    return binding_;
  }
  int add_simple_foreign_key_info(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id,
      const int64_t foreign_key_id, const common::ObString& foreign_key_name);
  int set_simple_foreign_key_info_array(const common::ObIArray<ObSimpleForeignKeyInfo>& simple_fk_info_array);
  inline const common::ObIArray<ObSimpleForeignKeyInfo>& get_simple_foreign_key_info_array() const
  {
    return simple_foreign_key_info_array_;
  }
  int add_simple_constraint_info(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id,
      const int64_t constraint_id, const common::ObString& constraint_name);
  int set_simple_constraint_info_array(const common::ObIArray<ObSimpleConstraintInfo>& simple_cst_info_array);
  inline const common::ObIArray<ObSimpleConstraintInfo>& get_simple_constraint_info_array() const
  {
    return simple_constraint_info_array_;
  }
  // @note: get_partition_cnt() != get_all_part_num()
  virtual int64_t get_partition_cnt() const override;
  int generate_mapping_pg_partition_array();
  // dblink.

  inline void set_dblink_id(const uint64_t dblink_id)
  {
    dblink_id_ = dblink_id;
  }
  inline uint64_t get_dblink_id() const
  {
    return dblink_id_;
  }
  inline void set_link_table_id(const uint64_t link_table_id)
  {
    link_table_id_ = link_table_id;
  }
  inline uint64_t get_link_table_id() const
  {
    return link_table_id_;
  }
  inline void set_link_schema_version(const int64_t version)
  {
    link_schema_version_ = version;
  }
  inline int64_t get_link_schema_version() const
  {
    return link_schema_version_;
  }
  inline void save_local_schema_version(const int64_t local_version)
  {
    link_schema_version_ = schema_version_;
    schema_version_ = local_version;
  }
  inline int set_link_database_name(const common::ObString& database_name)
  {
    return deep_copy_str(database_name, link_database_name_);
  }
  inline const common::ObString& get_link_database_name() const
  {
    return link_database_name_;
  }
  inline uint64_t extract_table_id() const
  {
    return common::extract_table_id(table_id_);
  }

  // only index table schema can invoke this function
  int get_index_name(common::ObString& index_name) const;
  template <typename Allocator>
  static int get_index_name(
      Allocator& allocator, uint64_t table_id, const common::ObString& src, common::ObString& dst);
  static int get_index_name(const common::ObString& src, common::ObString& dst);
  int generate_origin_index_name();
  int check_if_oracle_compat_mode(bool& is_oracle_mode) const;
  // interface derived
  // TODO: dup code, need merge with ObTableSchema
  //
  inline bool is_table() const
  {
    return is_user_table() || is_sys_table() || is_vir_table();
  }
  inline bool is_user_view() const
  {
    return USER_VIEW == table_type_;
  }
  inline bool is_sys_view() const
  {
    return SYSTEM_VIEW == table_type_;
  }
  inline bool is_storage_index_table() const
  {
    return is_index_table() || is_materialized_view();
  }
  inline static bool is_storage_index_table(ObTableType table_type)
  {
    return is_index_table(table_type) || is_materialized_view(table_type);
  }
  inline bool is_storage_local_index_table() const
  {
    return is_index_local_storage() || is_materialized_view();
  }
  inline bool is_user_table() const
  {
    return USER_TABLE == table_type_;
  }
  inline bool is_sys_table() const
  {
    return SYSTEM_TABLE == table_type_;
  }
  inline bool is_vir_table() const
  {
    return VIRTUAL_TABLE == table_type_;
  }
  inline bool is_view_table() const
  {
    return USER_VIEW == table_type_ || SYSTEM_VIEW == table_type_ || MATERIALIZED_VIEW == table_type_;
  }
  inline bool is_index_table() const
  {
    return is_index_table(table_type_);
  }
  inline static bool is_index_table(ObTableType table_type)
  {
    return USER_INDEX == table_type;
  }
  inline bool is_tmp_table() const
  {
    return TMP_TABLE == table_type_ || TMP_TABLE_ORA_SESS == table_type_ || TMP_TABLE_ORA_TRX == table_type_;
  }
  inline bool is_ctas_tmp_table() const
  {
    return 0 != session_id_ && !is_tmp_table();
  }
  inline bool is_mysql_tmp_table() const
  {
    return TMP_TABLE == table_type_;
  }
  inline bool is_oracle_tmp_table() const
  {
    return TMP_TABLE_ORA_SESS == table_type_ || TMP_TABLE_ORA_TRX == table_type_;
  }
  inline bool is_oracle_sess_tmp_table() const
  {
    return TMP_TABLE_ORA_SESS == table_type_;
  }
  inline bool is_oracle_trx_tmp_table() const
  {
    return TMP_TABLE_ORA_TRX == table_type_;
  }
  inline bool is_aux_table() const
  {
    return USER_INDEX == table_type_;
  }
  // when support global index, do not modify this local index interface
  inline bool is_materialized_view() const
  {
    return is_materialized_view(table_type_);
  }
  inline static bool is_materialized_view(ObTableType table_type)
  {
    return MATERIALIZED_VIEW == table_type;
  }
  inline bool is_in_recyclebin() const
  {
    return common::OB_RECYCLEBIN_SCHEMA_ID == common::extract_pure_id(database_id_) && !is_dropped_schema();
  }
  inline ObTenantTableId get_tenant_table_id() const
  {
    return ObTenantTableId(tenant_id_, table_id_);
  }
  inline ObTenantTableId get_tenant_data_table_id() const
  {
    return ObTenantTableId(tenant_id_, data_table_id_);
  }

  inline bool is_normal_index() const;
  inline bool is_unique_index() const;
  inline static bool is_unique_index(ObIndexType index_type);
  inline bool is_global_index_table() const;
  inline static bool is_global_index_table(const ObIndexType index_type);
  inline bool is_global_local_index_table() const;
  inline bool is_global_normal_index_table() const;
  inline bool is_global_unique_index_table() const;
  inline static bool is_global_unique_index_table(const ObIndexType index_type);
  inline bool is_local_unique_index_table() const;
  inline bool is_domain_index() const;
  inline static bool is_domain_index(ObIndexType index_type);
  inline bool is_index_local_storage() const;
  inline bool has_partition() const
  {
    return !(is_vir_table() || is_view_table() || is_index_local_storage());
  }
  // Introduced by pg, the stand alone table has its own physical partition
  virtual bool has_self_partition() const override
  {
    return has_partition() && !get_binding();
  }
  inline bool is_unavailable_index() const
  {
    return INDEX_STATUS_UNAVAILABLE == index_status_;
  }
  inline bool can_read_index() const
  {
    return can_read_index(index_status_, is_dropped_schema());
  }
  inline static bool can_read_index(ObIndexStatus index_status, const bool is_dropped_schema)
  {
    return INDEX_STATUS_AVAILABLE == index_status && !is_dropped_schema;
  }
  inline bool can_rereplicate_global_index_table() const
  {
    // 1. index which can rereplicate
    // 2. is mocked invalid for standby cluster
    return is_global_index_table() && !is_dropped_schema() &&
           (can_rereplicate_index_status(index_status_) || is_mock_global_index_invalid());
  }
  inline bool is_final_invalid_index() const;
  inline void set_index_status(const ObIndexStatus index_status)
  {
    index_status_ = index_status;
  }
  inline void set_index_type(const ObIndexType index_type)
  {
    index_type_ = index_type;
  }
  inline ObIndexStatus get_index_status() const
  {
    return index_status_;
  }
  inline ObIndexType get_index_type() const
  {
    return index_type_;
  }

  virtual bool is_user_partition_table() const override;
  virtual bool is_user_subpartition_table() const override;
  inline bool is_partitioned_table() const
  {
    return PARTITION_LEVEL_ONE == get_part_level() || PARTITION_LEVEL_TWO == get_part_level();
  }
  virtual ObPartitionLevel get_part_level() const override;
  virtual share::ObDuplicateScope get_duplicate_scope() const override
  {
    return duplicate_scope_;
  }
  virtual void set_duplicate_scope(const share::ObDuplicateScope duplicate_scope) override
  {
    duplicate_scope_ = duplicate_scope;
  }
  virtual void set_duplicate_scope(const int64_t duplicate_scope) override
  {
    duplicate_scope_ = static_cast<share::ObDuplicateScope>(duplicate_scope);
  }

  // for encrypt
  int set_encryption_str(const common::ObString& str)
  {
    return deep_copy_str(str, encryption_);
  }
  inline const common::ObString& get_encryption_str() const
  {
    return encryption_;
  }
  int get_encryption_id(int64_t& encrypt_id) const;
  bool need_encrypt() const;
  bool is_equal_encryption(const ObSimpleTableSchemaV2& t) const;
  inline virtual void set_tablespace_id(const uint64_t id)
  {
    tablespace_id_ = id;
  }
  inline virtual uint64_t get_tablespace_id() const
  {
    return tablespace_id_;
  }
  inline uint64_t get_master_key_id() const
  {
    return master_key_id_;
  }
  inline const common::ObString& get_encrypt_key() const
  {
    return encrypt_key_;
  }
  inline const char* get_encrypt_key_str() const
  {
    return extract_str(encrypt_key_);
  }
  inline int64_t get_encrypt_key_len() const
  {
    return encrypt_key_.length();
  }
  inline void set_master_key_id(uint64_t id)
  {
    master_key_id_ = id;
  }
  inline int set_encrypt_key(const common::ObString& key)
  {
    return deep_copy_str(key, encrypt_key_);
  }
  DECLARE_VIRTUAL_TO_STRING;

public:
  // pkey->pgkey
  // Note that the following two methods rely on sorted_part_id_partition_array_ and
  // sorted_part_id_def_subpartition_array_ arrays, These two arrays are generated when the schema is refreshed, so the
  // following three methods can only be called in the table_schema from the schema_guard get. Calling in other places
  // will cause problems
  bool is_binding_table() const;
  int get_pg_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key) const;
  int get_pg_key(const uint64_t table_id, const int64_t partition_id, common::ObPGKey& pg_key) const;

public:
  bool is_mock_global_index_invalid() const
  {
    return is_mock_global_index_invalid_;
  }
  void set_mock_global_index_invalid(const bool is_invalid)
  {
    is_mock_global_index_invalid_ = is_invalid;
  }

private:
  static bool compare_part_id(const ObPartition* part, const int64_t part_id);
  static bool compare_sub_part_id(const ObSubPartition* part, const int64_t sub_part_id);

protected:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t schema_version_;
  uint64_t database_id_;
  uint64_t tablegroup_id_;
  uint64_t data_table_id_;
  // Only in the process of querying the creation of the table and assigning the value of the temporary table to the
  // session_id at the time of creation, it is 0 in other occasions;
  uint64_t session_id_;
  common::ObString table_name_;
  common::ObNameCaseMode name_case_mode_;
  ObTableType table_type_;
  ObTableMode table_mode_;
  common::ObString primary_zone_;
  int64_t replica_num_;
  common::ObString locality_str_;
  common::ObString previous_locality_str_;
  common::ObArrayHelper<common::ObString> zone_list_;
  ObIndexStatus index_status_;
  ObIndexType index_type_;
  int64_t min_partition_id_;  // Only valid when the hash partition;
  common::ObSEArray<ObSimpleForeignKeyInfo, 4> simple_foreign_key_info_array_;
  common::ObSEArray<ObSimpleConstraintInfo, 4> simple_constraint_info_array_;
  // Only the type of index is valid in oracle mode, which means the original index name without prefix
  // (__idx__table_id_)
  common::ObString origin_index_name_;
  /*
   * The sorted_part_id_partition_array_ and sorted_part_id_def_subpartition_array_ arrays are used by the table_schema
   * of the binding, The partitions in the array are sorted according to part_id/subpart_id, used for pkey->pg_key
   * correspondence search, use part_id/subpart_id. Sorting is convenient for binary search This array is only valid
   * when the observer refreshes the schema. That is, it is only valid in the schema obtained in the schema guard,
   * therefore, these two fields do not need to be serialized
   */
  bool binding_;
  share::ObDuplicateScope duplicate_scope_;
  // When non-templated secondary partitions, iterate partition_array_ to take
  ObPartition** sorted_part_id_partition_array_;
  ObSubPartition** sorted_part_id_def_subpartition_array_;
  bool is_mock_global_index_invalid_;  // Standalone cluster use; no serialization required
  common::ObString encryption_;
  uint64_t tablespace_id_;
  ZoneLocalityArray zone_replica_attr_array_;
  common::ObSEArray<ObZoneScore, common::OB_MAX_MEMBER_NUMBER, common::ObNullAllocator> primary_zone_array_;
  common::ObString encrypt_key_;
  uint64_t master_key_id_;

  // dblink.
  // No serialization required
  uint64_t dblink_id_;
  uint64_t link_table_id_;
  int64_t link_schema_version_;
  common::ObString link_database_name_;
  // TODO: need link_table_name_?
};

class ObTableSchema : public ObSimpleTableSchemaV2 {
  OB_UNIS_VERSION(1);

public:
  friend class AlterTableSchema;
  friend class ObPrintableTableSchema;
  enum ObIndexAttributesFlag {
    INDEX_VISIBILITY = 0,
    INDEX_DROP_INDEX = 1,
    INDEX_VISIBILITY_SET_BEFORE = 2,
    INDEX_ROW_MOVEABLE = 3,
    MAX_INDEX_ATTRIBUTE = 64,
  };
  bool cmp_table_id(const ObTableSchema* a, const ObTableSchema* b)
  {
    return a->get_tenant_id() < b->get_tenant_id() || a->get_database_id() < b->get_database_id() ||
           a->get_table_id() < b->get_table_id();
  }
  static void construct_partition_key_column(
      const ObColumnSchemaV2& column, common::ObPartitionKeyColumn& partition_key_column);
  static int create_idx_name_automatically_oracle(
      common::ObString& idx_name, const common::ObString& table_name, common::ObIAllocator& allocator);
  static int create_cons_name_automatically(common::ObString& cst_name, const common::ObString& table_name,
      common::ObIAllocator& allocator, ObConstraintType cst_type);
  static int create_new_idx_name_after_flashback(ObTableSchema& new_table_schema, common::ObString& new_idx_name,
      common::ObIAllocator& allocator, ObSchemaGetterGuard& guard);

public:
  typedef ObColumnSchemaV2* const* const_column_iterator;
  typedef ObConstraint* const* const_constraint_iterator;
  typedef ObConstraint** constraint_iterator;
  ObTableSchema();
  explicit ObTableSchema(common::ObIAllocator* allocator);
  ObTableSchema(const ObTableSchema& src_schema) = delete;
  virtual ~ObTableSchema();
  ObTableSchema& operator=(const ObTableSchema& src_schema) = delete;
  int assign(const ObTableSchema& src_schema);
  // part splitting filter is needed during physical splitting
  bool need_part_filter() const
  {
    // At present, the conditions for supporting partition split are OLD tables without primary key, and user tables,
    // and do not include check constraints
    // is_in_physical_split() The interface does not take effect temporarily, please comment it out first
    return !is_old_no_pk_table() && is_user_table() && !has_check_constraint() && is_partitioned_table() &&
           is_in_splitting()
        /*&& is_in_physical_split()*/;
  }
  // set methods
  inline void set_max_used_column_id(const uint64_t id)
  {
    max_used_column_id_ = id;
  }
  inline void set_max_used_constraint_id(const uint64_t id)
  {
    max_used_constraint_id_ = id;
  }
  inline void set_sess_active_time(const int64_t t)
  {
    sess_active_time_ = t;
  }
  inline void set_first_timestamp_index(const uint64_t id)
  {
    first_timestamp_index_ = id;
  }
  inline void set_index_attributes_set(const uint64_t id)
  {
    index_attributes_set_ = id;
  }
  inline void set_index_visibility(const uint64_t index_visibility)
  {
    index_attributes_set_ &= ~((uint64_t)(1) << INDEX_VISIBILITY);
    index_attributes_set_ |= index_visibility << INDEX_VISIBILITY;
  }
  inline void set_enable_row_movement(const bool enable_row_move)
  {
    index_attributes_set_ &= ~((uint64_t)(1) << INDEX_ROW_MOVEABLE);
    if (enable_row_move) {
      index_attributes_set_ |= (1 << INDEX_ROW_MOVEABLE);
    }
  }
  inline void set_rowkey_column_num(const int64_t rowkey_column_num)
  {
    rowkey_column_num_ = rowkey_column_num;
  }
  inline void set_index_column_num(const int64_t index_column_num)
  {
    index_column_num_ = index_column_num;
  }
  inline void set_rowkey_split_pos(int64_t pos)
  {
    rowkey_split_pos_ = pos;
  }
  inline void set_part_key_column_num(const int64_t part_key_column_num)
  {
    part_key_column_num_ = part_key_column_num;
  }
  inline void set_subpart_key_column_num(const int64_t subpart_key_column_num)
  {
    subpart_key_column_num_ = subpart_key_column_num;
  }
  inline void set_progressive_merge_num(const int64_t progressive_merge_num)
  {
    progressive_merge_num_ = progressive_merge_num;
  }
  inline void set_progressive_merge_round(const int64_t progressive_merge_round)
  {
    progressive_merge_round_ = progressive_merge_round;
  }
  inline void set_replica_num(const int64_t replica_num)
  {
    replica_num_ = replica_num;
  }
  inline void set_tablet_size(const int64_t tablet_size)
  {
    tablet_size_ = tablet_size;
  }
  inline void set_pctfree(const int64_t pctfree)
  {
    pctfree_ = pctfree;
  }
  inline void set_autoinc_column_id(const int64_t autoinc_column_id)
  {
    autoinc_column_id_ = autoinc_column_id;
  }
  inline void set_auto_increment(const uint64_t auto_increment)
  {
    auto_increment_ = auto_increment;
  }
  inline void set_load_type(ObTableLoadType load_type)
  {
    load_type_ = load_type;
  }
  inline void set_def_type(ObTableDefType type)
  {
    def_type_ = type;
  }
  inline void set_partition_num(int64_t partition_num)
  {
    partition_num_ = partition_num;
  }
  inline void set_charset_type(const common::ObCharsetType type)
  {
    charset_type_ = type;
  }
  inline void set_collation_type(const common::ObCollationType type)
  {
    collation_type_ = type;
  }
  inline void set_create_mem_version(const int64_t version)
  {
    create_mem_version_ = version;
  }
  inline void set_code_version(const int64_t code_version)
  {
    code_version_ = code_version;
  }
  inline void set_last_modified_frozen_version(const int64_t frozen_version)
  {
    last_modified_frozen_version_ = frozen_version;
  }
  inline void set_index_using_type(const ObIndexUsingType index_using_type)
  {
    index_using_type_ = index_using_type;
  }
  inline void set_max_column_id(const uint64_t id)
  {
    max_used_column_id_ = id;
  }
  inline void set_is_use_bloomfilter(const bool is_use_bloomfilter)
  {
    is_use_bloomfilter_ = is_use_bloomfilter;
  }
  inline void set_block_size(const int64_t block_size)
  {
    block_size_ = block_size;
  }
  inline void set_read_only(const bool read_only)
  {
    read_only_ = read_only;
  }
  inline void set_store_format(const common::ObStoreFormatType store_format)
  {
    store_format_ = store_format;
  }
  inline void set_storage_format_version(const int64_t storage_format_version)
  {
    storage_format_version_ = storage_format_version;
  }
  int set_store_format(const common::ObString& store_format);
  inline void set_row_store_type(const common::ObRowStoreType row_store_type)
  {
    row_store_type_ = row_store_type;
  }
  int set_row_store_type(const common::ObString& row_store);
  int set_tablegroup_name(const char* tablegroup_name)
  {
    return deep_copy_str(tablegroup_name, tablegroup_name_);
  }
  int set_tablegroup_name(const common::ObString& tablegroup_name)
  {
    return deep_copy_str(tablegroup_name, tablegroup_name_);
  }
  int set_comment(const char* comment)
  {
    return deep_copy_str(comment, comment_);
  }
  int set_comment(const common::ObString& comment)
  {
    return deep_copy_str(comment, comment_);
  }
  int set_pk_comment(const char* comment)
  {
    return deep_copy_str(comment, pk_comment_);
  }
  int set_pk_comment(const common::ObString& comment)
  {
    return deep_copy_str(comment, pk_comment_);
  }
  int set_create_host(const char* create_host)
  {
    return deep_copy_str(create_host, create_host_);
  }
  int set_create_host(const common::ObString& create_host)
  {
    return deep_copy_str(create_host, create_host_);
  }
  int set_expire_info(const common::ObString& expire_info)
  {
    return deep_copy_str(expire_info, expire_info_);
  }
  int set_compress_func_name(const char* compressor);
  int set_compress_func_name(const common::ObString& compressor);
  inline void set_dop(int64_t table_dop)
  {
    table_dop_ = table_dop;
  }
  template <typename ColumnType>
  int add_column(const ColumnType& column);
  int delete_column(const common::ObString& column_name);
  int alter_column(ObColumnSchemaV2& column);
  int add_partition_key(const common::ObString& column_name);
  int add_subpartition_key(const common::ObString& column_name);
  int add_zone(const common::ObString& zone);
  int set_view_definition(const common::ObString& view_definition);
  int set_parser_name(const common::ObString& parser_name)
  {
    return deep_copy_str(parser_name, parser_name_);
  }
  int set_rowkey_info(const ObColumnSchemaV2& column);
  int set_foreign_key_infos(const common::ObIArray<ObForeignKeyInfo>& foreign_key_infos_);
  int set_simple_index_infos(const common::ObIArray<ObAuxTableMetaInfo>& simple_index_infos);
  // constraint related
  int add_constraint(const ObConstraint& constraint);
  int delete_constraint(const common::ObString& constraint_name);
  // Copy all constraint information in src_schema
  int assign_constraint(const ObTableSchema& other);
  // get methods
  bool is_valid() const;

  int get_generated_column_by_define(
      const common::ObString& col_def, const bool only_hidden_column, share::schema::ObColumnSchemaV2*& gen_col);
  const ObColumnSchemaV2* get_column_schema(const uint64_t column_id) const;
  const ObColumnSchemaV2* get_column_schema(const char* column_name) const;
  const ObColumnSchemaV2* get_column_schema(const common::ObString& column_name) const;
  const ObColumnSchemaV2* get_column_schema_by_idx(const int64_t idx) const;
  const ObColumnSchemaV2* get_column_schema(uint64_t table_id, uint64_t column_id) const;

  const ObColumnSchemaV2* get_fulltext_column(const ColumnReferenceSet& column_set) const;
  ObColumnSchemaV2* get_column_schema(const uint64_t column_id);
  ObColumnSchemaV2* get_column_schema(const char* column_name);
  ObColumnSchemaV2* get_column_schema(const common::ObString& column_name);
  ObColumnSchemaV2* get_column_schema_by_idx(const int64_t idx);
  ObColumnSchemaV2* get_column_schema_by_prev_next_id(const uint64_t column_id);
  const ObColumnSchemaV2* get_column_schema_by_prev_next_id(const uint64_t column_id) const;
  static uint64_t gen_materialized_view_column_id(uint64_t column_id);
  static uint64_t get_materialized_view_column_id(uint64_t column_id);

  ObConstraint* get_constraint(const uint64_t constraint_id);
  ObConstraint* get_constraint(const common::ObString& constraint_name);
  int get_pk_constraint_name(common::ObString& pk_name) const;

  int64_t get_column_idx(const uint64_t column_id, const bool ignore_hidden_column = false) const;
  int64_t get_replica_num() const;
  int64_t get_tablet_size() const
  {
    return tablet_size_;
  }
  int64_t get_pctfree() const
  {
    return pctfree_;
  }
  inline ObTenantTableId get_tenant_table_id() const
  {
    return ObTenantTableId(tenant_id_, table_id_);
  }
  inline int64_t get_index_tid_count() const
  {
    return simple_index_infos_.count();
  }
  inline int64_t get_mv_count() const
  {
    return mv_cnt_;
  }
  inline int64_t get_index_column_number() const
  {
    return index_column_num_;
  }
  inline uint64_t get_max_used_column_id() const
  {
    return max_used_column_id_;
  }
  inline uint64_t get_max_used_constraint_id() const
  {
    return max_used_constraint_id_;
  }
  inline int64_t get_sess_active_time() const
  {
    return sess_active_time_;
  }
  // Whether it is a temporary table created by ob proxy 64bit > uint max
  inline bool is_obproxy_create_tmp_tab() const
  {
    return is_tmp_table() && get_session_id() > 0xFFFFFFFFL;
  }
  inline int64_t get_rowkey_split_pos() const
  {
    return rowkey_split_pos_;
  }
  inline int64_t get_block_size() const
  {
    return block_size_;
  }
  inline bool is_use_bloomfilter() const
  {
    return is_use_bloomfilter_;
  }
  inline int64_t get_progressive_merge_num() const
  {
    return progressive_merge_num_;
  }
  inline int64_t get_progressive_merge_round() const
  {
    return progressive_merge_round_;
  }
  inline uint64_t get_autoinc_column_id() const
  {
    return autoinc_column_id_;
  }
  inline uint64_t get_auto_increment() const
  {
    return auto_increment_;
  }
  inline int64_t get_rowkey_column_num() const
  {
    return rowkey_info_.get_size();
  }
  inline int64_t get_shadow_rowkey_column_num() const
  {
    return shadow_rowkey_info_.get_size();
  }
  inline int64_t get_index_column_num() const
  {
    return index_info_.get_size();
  }
  inline int64_t get_partition_key_column_num() const
  {
    return partition_key_info_.get_size();
  }
  inline int64_t get_subpartition_key_column_num() const
  {
    return subpartition_key_info_.get_size();
  }
  inline ObTableLoadType get_load_type() const
  {
    return load_type_;
  }
  inline ObIndexUsingType get_index_using_type() const
  {
    return index_using_type_;
  }
  inline ObTableDefType get_def_type() const
  {
    return def_type_;
  }

  inline const char* get_compress_func_name() const
  {
    return extract_str(compress_func_name_);
  }
  inline bool is_compressed() const
  {
    return !compress_func_name_.empty() && compress_func_name_ != "none";
  }
  inline common::ObStoreFormatType get_store_format() const
  {
    return store_format_;
  }
  inline common::ObRowStoreType get_row_store_type() const
  {
    return row_store_type_;
  }
  inline int64_t get_storage_format_version() const
  {
    return storage_format_version_;
  }
  inline const char* get_tablegroup_name_str() const
  {
    return extract_str(tablegroup_name_);
  }
  inline const common::ObString& get_tablegroup_name() const
  {
    return tablegroup_name_;
  }
  inline const char* get_comment() const
  {
    return extract_str(comment_);
  }
  inline const common::ObString& get_comment_str() const
  {
    return comment_;
  }
  inline const char* get_pk_comment() const
  {
    return extract_str(pk_comment_);
  }
  inline const common::ObString& get_pk_comment_str() const
  {
    return pk_comment_;
  }
  inline const char* get_create_host() const
  {
    return extract_str(create_host_);
  }
  inline const common::ObString& get_create_host_str() const
  {
    return create_host_;
  }
  inline const common::ObRowkeyInfo& get_rowkey_info() const
  {
    return rowkey_info_;
  }
  inline const common::ObRowkeyInfo& get_shadow_rowkey_info() const
  {
    return shadow_rowkey_info_;
  }
  inline const common::ObIndexInfo& get_index_info() const
  {
    return index_info_;
  }
  inline const common::ObPartitionKeyInfo& get_partition_key_info() const
  {
    return partition_key_info_;
  }
  inline const common::ObPartitionKeyInfo& get_subpartition_key_info() const
  {
    return subpartition_key_info_;
  }
  inline common::ObCharsetType get_charset_type() const
  {
    return charset_type_;
  }
  inline common::ObCollationType get_collation_type() const
  {
    return collation_type_;
  }
  inline int64_t get_create_mem_version() const
  {
    return create_mem_version_;
  }
  inline common::ObNameCaseMode get_name_case_mode() const
  {
    return name_case_mode_;
  }
  inline int64_t get_code_version() const
  {
    return code_version_;
  }
  inline int64_t get_last_modified_frozen_version() const
  {
    return last_modified_frozen_version_;
  }
  inline const common::ObString& get_expire_info() const
  {
    return expire_info_;
  }
  inline ObViewSchema& get_view_schema()
  {
    return view_schema_;
  }
  inline common::ObSEArray<std::pair<uint64_t, uint64_t>, 3>& get_join_conds()
  {
    return join_conds_;
  }
  inline const common::ObSEArray<std::pair<uint64_t, uint64_t>, 3>& get_join_conds() const
  {
    return join_conds_;
  }
  inline common::ObSEArray<TableJoinType, 3>& get_join_types()
  {
    return join_types_;
  }
  inline const ObViewSchema& get_view_schema() const
  {
    return view_schema_;
  }
  virtual int check_in_locality_modification(
      share::schema::ObSchemaGetterGuard& schema_guard, bool& in_locality_modification) const;
  bool has_check_constraint() const;
  inline bool has_constraint() const
  {
    return cst_cnt_ > 0;
  }
  inline bool is_read_only() const
  {
    return read_only_;
  }
  inline const char* get_parser_name() const
  {
    return extract_str(parser_name_);
  }
  inline const common::ObString& get_parser_name_str() const
  {
    return parser_name_;
  }
  inline uint64_t get_index_attributes_set() const
  {
    return index_attributes_set_;
  }
  inline int64_t get_dop() const
  {
    return table_dop_;
  }
  inline bool is_index_visible() const
  {
    return 0 == (index_attributes_set_ & ((uint64_t)(1) << INDEX_VISIBILITY));
  }
  inline bool is_enable_row_movement() const
  {
    return 0 != (index_attributes_set_ & ((uint64_t)(1) << INDEX_ROW_MOVEABLE));
  }

  uint64 get_index_attributes_set()
  {
    return index_attributes_set_;
  }

  inline bool has_materialized_view() const
  {
    return mv_cnt_ > 0;
  }
  bool has_depend_table(uint64_t table_id) const;
  // Is it an old table without a primary key (add pk_increment/partition_id/cluster_id)
  bool is_old_no_pk_table() const;
  // Whether it is a non-primary key table, return true for the old non-primary key table and the new non-primary key
  // table
  bool is_no_pk_table() const
  {
    return is_old_no_pk_table() || is_new_no_pk_table();
  }
  int get_orig_default_row(const common::ObIArray<ObColDesc>& column_ids, common::ObNewRow& default_row) const;
  int get_cur_default_row(const common::ObIArray<ObColDesc>& column_ids, common::ObNewRow& default_row) const;
  inline int64_t get_column_count() const
  {
    return column_cnt_;
  }
  inline int64_t get_constraint_count() const
  {
    return cst_cnt_;
  }
  inline int64_t get_virtual_column_cnt() const
  {
    return virtual_column_cnt_;
  }
  inline const_column_iterator column_begin() const
  {
    return column_array_;
  }
  inline const_column_iterator column_end() const
  {
    return NULL == column_array_ ? NULL : &(column_array_[column_cnt_]);
  }
  inline const_constraint_iterator constraint_begin() const
  {
    return cst_array_;
  }
  inline const_constraint_iterator constraint_end() const
  {
    return NULL == cst_array_ ? NULL : &(cst_array_[cst_cnt_]);
  }
  inline constraint_iterator constraint_begin_for_non_const_iter() const
  {
    return cst_array_;
  }
  inline constraint_iterator constraint_end_for_non_const_iter() const
  {
    return NULL == cst_array_ ? NULL : &(cst_array_[cst_cnt_]);
  }
  int fill_column_collation_info();
  int has_column(const uint64_t column_id, bool& has) const;
  int has_lob_column(bool& has_lob, const bool check_large = false) const;
  int get_column_ids(common::ObIArray<uint64_t>& column_ids) const;
  int get_index_and_rowkey_column_ids(common::ObIArray<uint64_t>& column_ids) const;
  int get_column_ids(common::ObIArray<ObColDesc>& column_ids, const bool no_virtual = false) const;
  int get_rowkey_column_ids(common::ObIArray<ObColDesc>& column_ids) const;
  int get_rowkey_column_ids(common::ObIArray<uint64_t>& column_ids) const;
  int get_column_ids_without_rowkey(common::ObIArray<ObColDesc>& column_ids, const bool no_virtual = false) const;
  int get_generated_column_ids(common::ObIArray<uint64_t>& column_ids) const;
  inline bool has_generated_column() const
  {
    return generated_columns_.num_members() > 0;
  }
  int add_base_table_id(uint64_t base_table_id)
  {
    return base_table_ids_.push_back(base_table_id);
  }
  int add_depend_table_id(uint64_t depend_table_id)
  {
    return depend_table_ids_.push_back(depend_table_id);
  }
  const common::ObSEArray<uint64_t, 3>& get_base_table_ids() const
  {
    return base_table_ids_;
  }
  const common::ObSEArray<uint64_t, 3>& get_depend_table_ids() const
  {
    return depend_table_ids_;
  }
  uint64_t get_mv_tid(uint64_t idx) const
  {
    return (idx < mv_cnt_) ? mv_tid_array_[idx] : common::OB_INVALID_ID;
  }

  // get columns for building rowid
  int get_column_ids_serialize_to_rowid(common::ObIArray<uint64_t>& col_ids, int64_t& rowkey_cnt) const;

  // only used by storage layer, return all columns that need to be stored in sstable
  // 1. for storage_index_table (user_index or mv):
  //    return all index columns plus rowkey (including virtual columns)
  // 2. for user table:
  //    return all not virtual columns of the current table
  int get_store_column_ids(common::ObIArray<ObColDesc>& column_ids) const;
  // return the number of columns that will be stored in sstable
  // it is equal to the size of column_ids array returned by get_store_column_ids
  int get_store_column_count(int64_t& column_count) const;

  template <typename Allocator>
  static int build_index_table_name(Allocator& allocator, const uint64_t data_table_id,
      const common::ObString& index_name, common::ObString& index_table_name);

  //
  // materialized view related
  //
  int convert_to_depend_table_column(uint64_t column_id, uint64_t& convert_table_id, uint64_t& convert_column_id) const;

  bool is_depend_column(uint64_t column_id) const;

  bool has_table(uint64_t table_id) const;
  bool is_drop_index() const;
  void set_drop_index(const uint64_t drop_index_value);
  bool is_invisible_before() const;
  void set_invisible_before(const uint64_t invisible_before);

  // other methods
  int64_t get_convert_size() const;
  void reset();
  // int64_t to_string(char *buf, const int64_t buf_len) const;
  // whether the primary key or index is ordered
  inline bool is_ordered() const
  {
    return USING_BTREE == index_using_type_;
  }
  virtual int serialize_columns(char* buf, const int64_t data_len, int64_t& pos) const;
  virtual int deserialize_columns(const char* buf, const int64_t data_len, int64_t& pos);
  int serialize_constraints(char* buf, const int64_t data_len, int64_t& pos) const;
  int deserialize_constraints(const char* buf, const int64_t data_len, int64_t& pos);
  // FIXME: There is a problem with the logic, and it is not called
  // int get_partition(const common::ObString &part_name, ObPartition *&dst) const;
  int get_physical_partition_ids(const int64_t partition_id, common::ObIArray<int64_t>& partition_ids) const;
  int get_physical_partition_ids(common::ObIArray<int64_t>& partition_ids) const;
  virtual int alloc_partition(const ObPartition*& partition);
  virtual int alloc_partition(const ObSubPartition*& subpartition);
  int check_primary_key_cover_partition_column();
  int check_auto_partition_valid();
  int check_rowkey_cover_partition_keys(const common::ObPartitionKeyInfo& part_key);
  int check_index_table_cover_partition_keys(const common::ObPartitionKeyInfo& part_key) const;
  int check_create_index_on_hidden_primary_key(const ObTableSchema& index_table) const;

  int get_part(const common::ObNewRange& range, const bool reverse, common::ObIArray<int64_t>& part_ids) const;
  int get_part(const common::ObNewRow& row, common::ObIArray<int64_t>& part_ids) const;
  int get_subpart(const int64_t part_id, const common::ObNewRange& range, const bool reverse,
      common::ObIArray<int64_t>& subpart_ids) const;
  int get_subpart(const int64_t part_id, const common::ObNewRow& row, common::ObIArray<int64_t>& subpart_ids) const;
  int get_part_shuffle_key(const int64_t part_id, common::ObObj& part_key1, common::ObObj& part_key2) const;
  int get_subpart_shuffle_key(
      const int64_t part_id, const int64_t subpart_id, common::ObObj& subpart_key1, common::ObObj& subpart_key2) const;
  /*
   * IN     row, the partition values
   * OUT    part_ids, the one level partitions
   * OUT    subpart_ids, the two level partitions
   */
  int get_all_part_ids_by_level_one_partition_values(
      const common::ObNewRow& row, common::ObIArray<int64_t>& part_ids, common::ObIArray<int64_t>& subpart_ids) const;
  /*
   * IN     row, the sub partition values
   * OUT    part_ids, the one level partitions
   * OUT    subpart_ids, the two level partitions
   */
  int get_all_part_ids_by_level_two_partition_values(
      const common::ObNewRow& row, common::ObIArray<int64_t>& part_ids, common::ObIArray<int64_t>& subpart_ids) const;

  int get_all_part_ids(common::ObIArray<int64_t>& part_ids) const;
  int get_subpart_ids(const int64_t part_id, common::ObIArray<int64_t>& subpart_ids) const;

  int assign_tablegroup_partition(const ObTablegroupSchema& tablegroup);

  virtual int calc_part_func_expr_num(int64_t& part_func_expr_num) const;
  virtual int calc_subpart_func_expr_num(int64_t& subpart_func_expr_num) const;
  int is_partition_key(uint64_t column_id, bool& result) const;
  inline void reset_simple_index_infos()
  {
    simple_index_infos_.reset();
  }
  inline const common::ObIArray<ObAuxTableMetaInfo>& get_simple_index_infos() const
  {
    return simple_index_infos_;
  }
  int get_simple_index_infos(common::ObIArray<ObAuxTableMetaInfo>& simple_index_infos_array, bool with_mv = true) const;
  int get_simple_index_infos_without_delay_deleted_tid(
      common::ObIArray<ObAuxTableMetaInfo>& simple_index_infos_array, bool with_mv = true) const;

  // Foreign key
  inline const common::ObIArray<ObForeignKeyInfo>& get_foreign_key_infos() const
  {
    return foreign_key_infos_;
  }
  inline common::ObIArray<ObForeignKeyInfo>& get_foreign_key_infos()
  {
    return foreign_key_infos_;
  }
  // This function is used in ObCodeGeneratorImpl::convert_foreign_keys
  // For self-referential foreign keys:
  //   In foreign_key_infos_.count() only counts a foreign key
  //   This function will count two foreign keys
  int64_t get_foreign_key_real_count() const;
  bool is_parent_table() const;
  bool is_child_table() const;
  int add_foreign_key_info(const ObForeignKeyInfo& foreign_key_info);
  int remove_foreign_key_info(const uint64_t foreign_key_id);
  inline void reset_foreign_key_infos()
  {
    foreign_key_infos_.reset();
  }
  int add_simple_index_info(const ObAuxTableMetaInfo& simple_index_info);

  // For old old execution framework, we need offset to cale slice idx
  int get_part_offset(int64_t part_id, int64_t& part_offset) const;
  int get_sub_part_offset(int64_t part_id, int64_t subpart_id, int64_t& subpart_offset) const;

  // only for size_size test
  int set_column_encodings(const common::ObIArray<int64_t>& col_encodings);
  int get_column_encodings(common::ObIArray<int64_t>& col_encodings) const;

  const IdHashArray* get_id_hash_array() const
  {
    return id_hash_array_;
  }
  int generate_kv_schema(const int64_t tenant_id, const int64_t table_id);

  int is_partition_key_match_rowkey_prefix(bool& is_prefix) const;

  int generate_partition_key_from_rowkey(const common::ObRowkey& rowkey, common::ObRowkey& hign_bound_value) const;

  DECLARE_VIRTUAL_TO_STRING;

protected:
  int add_col_to_id_hash_array(ObColumnSchemaV2* column);
  int remove_col_from_id_hash_array(const ObColumnSchemaV2* column);
  int add_col_to_name_hash_array(ObColumnSchemaV2* column);
  int remove_col_from_name_hash_array(const ObColumnSchemaV2* column);
  int add_col_to_column_array(ObColumnSchemaV2* column);
  int remove_col_from_column_array(const ObColumnSchemaV2* column);
  int add_column_update_prev_id(ObColumnSchemaV2* local_column);
  int delete_column_update_prev_id(ObColumnSchemaV2* column);
  int64_t column_cnt_;

protected:
  // constraint related
  int add_cst_to_cst_array(ObConstraint* cst);
  int remove_cst_from_cst_array(const ObConstraint* cst);

private:
  int get_default_row(
      get_default_value func, const common::ObIArray<ObColDesc>& column_ids, common::ObNewRow& default_row) const;
  inline int64_t get_id_hash_array_mem_size(const int64_t column_cnt) const;
  inline int64_t get_name_hash_array_mem_size(const int64_t column_cnt) const;
  int delete_column_internal(ObColumnSchemaV2* column_schema);
  ObColumnSchemaV2* get_column_schema_by_id_internal(const uint64_t column_id) const;
  ObColumnSchemaV2* get_column_schema_by_name_internal(const common::ObString& column_name) const;
  int check_column_can_be_altered(const ObColumnSchemaV2* src_schema, ObColumnSchemaV2* dst_schema);
  int check_rowkey_column_can_be_altered(const ObColumnSchemaV2* src_schema, const ObColumnSchemaV2* dst_schema);
  int check_row_length(const ObColumnSchemaV2* src_schema, const ObColumnSchemaV2* dst_schema);
  ObConstraint* get_constraint_internal(std::function<bool(const ObConstraint* val)> func);
  const ObConstraint* get_constraint_internal(std::function<bool(const ObConstraint* val)> func) const;

protected:
  int add_mv_tid(const uint64_t mv_tid);

protected:
  uint64_t max_used_column_id_;
  uint64_t max_used_constraint_id_;
  // Only temporary table settings, according to the last active time of the session
  // to determine whether the table needs to be cleaned up;
  int64_t sess_active_time_;
  int64_t rowkey_column_num_;
  int64_t index_column_num_;
  int64_t rowkey_split_pos_;  // not used so far;reserved
  int64_t part_key_column_num_;
  int64_t subpart_key_column_num_;
  int64_t block_size_;       // KB
  bool is_use_bloomfilter_;  // used for prebuild bloomfilter when merge
  int64_t progressive_merge_num_;
  int64_t tablet_size_;
  int64_t pctfree_;
  uint64_t autoinc_column_id_;
  uint64_t auto_increment_;
  bool read_only_;
  ObTableLoadType load_type_;  // not used yet
  ObIndexUsingType index_using_type_;
  ObTableDefType def_type_;
  common::ObCharsetType charset_type_;      // default:utf8mb4
  common::ObCollationType collation_type_;  // default:utf8mb4_general_ci
  int64_t create_mem_version_;
  int64_t code_version_;                  // for compatible use, the version of the whole schema system
  int64_t last_modified_frozen_version_;  // the frozen version when last modified
  int64_t first_timestamp_index_;

  // just use one uint64 to store index attributes,
  // The lowest bit indicates the visibility of the index, the default value is 0, which means the index is visible,
  // and 1 means the index is invisible
  uint64_t index_attributes_set_;

  common::ObString tablegroup_name_;
  common::ObString comment_;
  common::ObString pk_comment_;
  common::ObString create_host_;
  common::ObString compress_func_name_;
  common::ObString expire_info_;
  common::ObString parser_name_;  // fulltext index parser name
  common::ObRowStoreType row_store_type_;
  common::ObStoreFormatType store_format_;
  int64_t storage_format_version_;
  int64_t progressive_merge_round_;

  // view schema
  ObViewSchema view_schema_;

  // materialized view
  // join conditions, only equal supported
  // example:
  // t1.c1=t2.c1 and t1.c2=t2.c2 ..  ==> <t1.c1, t2.c1>, <t1.c2, t2.c2> ...
  // note:
  // don't serialize it
  common::ObSEArray<std::pair<uint64_t, uint64_t>, 3> join_conds_;
  // join types
  // examples:
  // t1 join t2, t3 left join t4 ... ==> <t1, t2, inner_join>, <t3, t4, left_join> ...
  common::ObSEArray<TableJoinType, 3> join_types_;

  // all base table ids for materialized view
  common::ObSEArray<uint64_t, 3> base_table_ids_;
  common::ObSEArray<uint64_t, 3> depend_table_ids_;

  // index_tid_array_ and index_cnt_ are deprecated, now use simple_index_infos_ to store index information in the table
  // int64_t index_cnt_;
  // uint64_t index_tid_array_[common::OB_MAX_INDEX_PER_TABLE];
  common::ObSEArray<ObAuxTableMetaInfo, common::SEARRAY_INIT_NUM> simple_index_infos_;

  int64_t mv_cnt_;
  uint64_t mv_tid_array_[common::OB_MAX_INDEX_PER_TABLE];

  // Should encapsulate an Array structure, push calls T (allocator) construction
  ObColumnSchemaV2** column_array_;

  int64_t column_array_capacity_;
  // generated data
  common::ObRowkeyInfo rowkey_info_;
  common::ObRowkeyInfo shadow_rowkey_info_;
  common::ObIndexInfo index_info_;
  common::ObPartitionKeyInfo partition_key_info_;
  common::ObPartitionKeyInfo subpartition_key_info_;
  IdHashArray* id_hash_array_;
  NameHashArray* name_hash_array_;
  ColumnReferenceSet generated_columns_;
  int64_t virtual_column_cnt_;

  // constraint related
  ObConstraint** cst_array_;
  int64_t cst_array_capacity_;
  int64_t cst_cnt_;
  common::ObSEArray<ObForeignKeyInfo, 4> foreign_key_infos_;

  // table dop
  int64_t table_dop_;
};

class ObPrintableTableSchema final : public ObTableSchema {
public:
  DECLARE_VIRTUAL_TO_STRING;

private:
  ObPrintableTableSchema() = delete;
};

// The data storage form of the index is local storage, that is,
// the storage of the index and the main table are put together
inline bool ObSimpleTableSchemaV2::is_index_local_storage() const
{
  return USER_INDEX == table_type_ &&
         (INDEX_TYPE_NORMAL_LOCAL == index_type_ || INDEX_TYPE_UNIQUE_LOCAL == index_type_ ||
             INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type_ ||
             INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type_ || INDEX_TYPE_PRIMARY == index_type_ ||
             INDEX_TYPE_DOMAIN_CTXCAT == index_type_);
}

inline bool ObSimpleTableSchemaV2::is_global_index_table() const
{
  return is_global_index_table(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_global_index_table(const ObIndexType index_type)
{
  return INDEX_TYPE_NORMAL_GLOBAL == index_type || INDEX_TYPE_UNIQUE_GLOBAL == index_type;
}

inline bool ObSimpleTableSchemaV2::is_global_normal_index_table() const
{
  return INDEX_TYPE_NORMAL_GLOBAL == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_global_unique_index_table() const
{
  return INDEX_TYPE_UNIQUE_GLOBAL == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_global_unique_index_table(const ObIndexType index_type)
{
  return INDEX_TYPE_UNIQUE_GLOBAL == index_type;
}

inline bool ObSimpleTableSchemaV2::is_local_unique_index_table() const
{
  return INDEX_TYPE_UNIQUE_LOCAL == index_type_
         || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_global_local_index_table() const
{
  return INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type_ || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_normal_index() const
{
  return INDEX_TYPE_NORMAL_LOCAL == index_type_ || INDEX_TYPE_NORMAL_GLOBAL == index_type_ ||
         INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_unique_index() const
{
  return is_unique_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_unique_index(ObIndexType index_type)
{
  return INDEX_TYPE_UNIQUE_LOCAL == index_type || INDEX_TYPE_UNIQUE_GLOBAL == index_type ||
         INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type;
}

inline bool ObSimpleTableSchemaV2::is_domain_index() const
{
  return is_domain_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_domain_index(ObIndexType index_type)
{
  return INDEX_TYPE_DOMAIN_CTXCAT == index_type;
}

inline int64_t ObTableSchema::get_id_hash_array_mem_size(const int64_t column_cnt) const
{
  return common::max(IdHashArray::MIN_HASH_ARRAY_ITEM_COUNT, column_cnt * 2) * sizeof(void*) + sizeof(IdHashArray);
}

inline int64_t ObTableSchema::get_name_hash_array_mem_size(const int64_t column_cnt) const
{
  return common::max(NameHashArray::MIN_HASH_ARRAY_ITEM_COUNT, column_cnt * 2) * sizeof(void*) + sizeof(NameHashArray);
}

template <typename Allocator>
int ObTableSchema::build_index_table_name(Allocator& allocator, const uint64_t data_table_id,
    const common::ObString& index_name, common::ObString& index_table_name)
{
  int ret = common::OB_SUCCESS;
  int nwrite = 0;
  const int64_t buf_size = 64;
  char buf[buf_size];
  if ((nwrite = snprintf(buf, buf_size, "%lu", data_table_id)) >= buf_size || nwrite < 0) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SHARE_SCHEMA_LOG(WARN, "buf is not large enough", K(buf_size), K(data_table_id), K(ret));
  } else {
    common::ObString table_id_str = common::ObString::make_string(buf);
    int32_t src_len =
        table_id_str.length() + index_name.length() + static_cast<int32_t>(strlen(common::OB_INDEX_PREFIX)) + 1;
    char* ptr = NULL;
    // TODO(): refactor following code, use snprintf instead
    if (OB_UNLIKELY(0 >= src_len)) {
      index_table_name.assign(NULL, 0);
    } else if (NULL == (ptr = static_cast<char*>(allocator.alloc(src_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "alloc memory failed", K(ret), "size", src_len);
    } else {
      int64_t pos = 0;
      MEMCPY(ptr + pos, common::OB_INDEX_PREFIX, strlen(common::OB_INDEX_PREFIX));
      pos += strlen(common::OB_INDEX_PREFIX);
      MEMCPY(ptr + pos, table_id_str.ptr(), table_id_str.length());
      pos += table_id_str.length();
      MEMCPY(ptr + pos, "_", 1);
      pos += 1;
      MEMCPY(ptr + pos, index_name.ptr(), index_name.length());
      pos += index_name.length();
      if (pos == src_len) {
        index_table_name.assign_ptr(ptr, src_len);
      } else {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "length mismatch", K(ret));
      }
    }
  }

  return ret;
}

template <typename Allocator>
int ObSimpleTableSchemaV2::get_index_name(
    Allocator& allocator, uint64_t table_id, const common::ObString& src, common::ObString& dst)
{
  int ret = common::OB_SUCCESS;
  common::ObString::obstr_size_t dst_len = 0;
  char* ptr = NULL;
  common::ObString::obstr_size_t pos = 0;
  const int64_t BUF_SIZE = 64;  // table_id max length
  char table_id_buf[BUF_SIZE] = {'\0'};
  if (common::OB_INVALID_ID == table_id || src.empty()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(table_id), K(src));
  } else {
    int64_t n = snprintf(table_id_buf, BUF_SIZE, "%lu", table_id);
    if (n < 0 || n >= BUF_SIZE) {
      ret = common::OB_BUF_NOT_ENOUGH;
      SHARE_SCHEMA_LOG(WARN, "buffer not enough", K(ret), K(n), LITERAL_K(BUF_SIZE));
    } else {
      common::ObString table_id_str = common::ObString::make_string(table_id_buf);
      pos += static_cast<int32_t>(strlen(common::OB_INDEX_PREFIX));
      pos += table_id_str.length();
      pos += 1;
      dst_len = src.length() - pos;
      if (OB_UNLIKELY(0 >= dst_len)) {
        dst.assign(NULL, 0);
      } else if (NULL == (ptr = static_cast<char*>(allocator.alloc(dst_len)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SHARE_SCHEMA_LOG(WARN, "alloc memory failed", K(ret), "size", dst_len);
      } else {
        MEMCPY(ptr, src.ptr() + pos, dst_len);
        dst.assign_ptr(ptr, dst_len);
      }
    }
  }
  return ret;
}

template <typename ColumnType>
int ObTableSchema::add_column(const ColumnType& column)
{
  using namespace common;

  int ret = common::OB_SUCCESS;
  char* buf = NULL;
  ColumnType* local_column = NULL;
  if (!column.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "The column is not valid", K(ret));
  } else if (is_user_table() && common::OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID != column.get_column_id() &&
             column_cnt_ >= common::OB_USER_ROW_MAX_COLUMNS_COUNT) {
    // ignore rowid pseduo column
    ret = common::OB_ERR_TOO_MANY_COLUMNS;
  } else if (column.is_autoincrement() && (autoinc_column_id_ != 0) && (autoinc_column_id_ != column.get_column_id()) &&
             common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID != column.get_column_id()) {
    ret = common::OB_ERR_WRONG_AUTO_KEY;
    SHARE_SCHEMA_LOG(WARN, "Only one auto increment row is allowed", K(ret));
  } else if (NULL == (buf = static_cast<char*>(alloc(sizeof(ColumnType))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(ERROR, "Fail to allocate memory, ", "size", sizeof(ColumnType), K(ret));
  } else if (OB_FAIL(check_row_length(NULL, &column))) {
    SHARE_SCHEMA_LOG(WARN, "check row length failed", K(ret));
  } else {
    if (NULL == (local_column = new (buf) ColumnType(allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "Fail to new local_column", K(ret));
    } else {
      *local_column = column;
      ret = local_column->get_err_ret();
      local_column->set_table_id(table_id_);
      if (OB_FAIL(ret)) {
        SHARE_SCHEMA_LOG(WARN, "failed copy assign column", K(ret), K(column));
      } else if (!local_column->is_valid()) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "The local column is not valid", K(ret));
      } else if (OB_FAIL(add_column_update_prev_id(local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to update previous next column id", K(ret));
      } else if (OB_FAIL(add_col_to_id_hash_array(local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add column to id_hash_array", K(ret));
      } else if (OB_FAIL(add_col_to_name_hash_array(local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add column to name_hash_array", K(ret));
      } else if (OB_FAIL(add_col_to_column_array(local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to push column to array", K(ret));
      } else {
        if (column.is_rowkey_column()) {
          if (OB_FAIL(set_rowkey_info(column))) {
            SHARE_SCHEMA_LOG(WARN, "set rowkey info to table schema failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && column.is_index_column()) {
          common::ObIndexColumn index_column;
          index_column.column_id_ = column.get_column_id();
          index_column.length_ = column.get_data_length();
          index_column.type_ = column.get_meta_type();
          index_column.fulltext_flag_ = column.is_fulltext_column();
          if (OB_FAIL(index_info_.set_column(column.get_index_position() - 1, index_column))) {
            SHARE_SCHEMA_LOG(WARN, "Fail to set column to index info", K(ret));
          } else {
            if (index_column_num_ < index_info_.get_size()) {
              index_column_num_ = index_info_.get_size();
              if (is_user_table() && index_column_num_ > common::OB_MAX_ROWKEY_COLUMN_NUMBER) {
                ret = common::OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
                LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
              }
            }
          }
        }
        if (OB_SUCC(ret) && column.is_generated_column()) {
          if (OB_FAIL(generated_columns_.add_member(column.get_column_id() - common::OB_APP_MIN_COLUMN_ID))) {
            SHARE_SCHEMA_LOG(WARN, "add column id to generated columns failed", K(column));
          } else if (!column.is_column_stored_in_sstable()) {
            ++virtual_column_cnt_;
          }
        }
        if (OB_SUCC(ret) && column.is_rowid_pseudo_column()) {
          ++virtual_column_cnt_;
        }
      }
      if (OB_SUCC(ret)) {
        if (column.is_tbl_part_key_column()) {
          local_column->set_tbl_part_key_pos(column.get_tbl_part_key_pos());
          common::ObPartitionKeyColumn partition_key_column;
          construct_partition_key_column(column, partition_key_column);

          if (column.is_part_key_column()) {
            if (OB_FAIL(partition_key_info_.set_column(column.get_part_key_pos() - 1, partition_key_column))) {
              SHARE_SCHEMA_LOG(WARN, "Failed to set partition coumn");
            } else {
              part_key_column_num_ = partition_key_info_.get_size();
            }
          }

          if (OB_SUCC(ret)) {
            if (column.is_subpart_key_column()) {
              if (OB_FAIL(subpartition_key_info_.set_column(column.get_subpart_key_pos() - 1, partition_key_column))) {
                SHARE_SCHEMA_LOG(WARN, "Failed to set subpartition column", K(ret));
              } else {
                subpart_key_column_num_ = subpartition_key_info_.get_size();
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (column.is_autoincrement() && common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID != column.get_column_id()) {
          autoinc_column_id_ = column.get_column_id();
        }
      }
    }
  }

  // add shadow rowkey info
  if (OB_SUCC(ret)) {
    int64_t shadow_pk_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      const common::ObRowkeyColumn* tmp_column = NULL;
      if (NULL == (tmp_column = rowkey_info_.get_column(i))) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "the column is NULL, ", K(i));
      } else if (tmp_column->column_id_ > common::OB_MIN_SHADOW_COLUMN_ID) {
        if (OB_FAIL(shadow_rowkey_info_.set_column(shadow_pk_pos, *tmp_column))) {
          SHARE_SCHEMA_LOG(WARN, "fail to set column to shadow rowkey info", K(ret), K(*tmp_column));
        } else {
          ++shadow_pk_pos;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (get_max_used_column_id() < column.get_column_id()) {
      set_max_used_column_id(column.get_column_id());
    }
  }
  return ret;
}

class ObColumnIterByPrevNextID {
public:
  explicit ObColumnIterByPrevNextID(const ObTableSchema& table_schema)
      : is_end_(false), table_schema_(table_schema), last_column_schema_(NULL), last_iter_(NULL)
  {}
  ~ObColumnIterByPrevNextID()
  {}
  int next(const ObColumnSchemaV2*& column_schema);
  const ObColumnSchemaV2* get_first_column() const;

private:
  bool is_end_;
  const ObTableSchema& table_schema_;
  const ObColumnSchemaV2* last_column_schema_;
  ObTableSchema::const_column_iterator last_iter_;
};

inline bool ObSimpleTableSchemaV2::is_final_invalid_index() const
{
  return is_final_invalid_index_status(index_status_, is_dropped_schema());
}
}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OCEANBASE_SCHEMA_TABLE_SCHEMA
