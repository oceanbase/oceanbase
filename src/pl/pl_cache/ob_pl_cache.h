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

#ifndef OCEANBASE_PL_CACHE_H_
#define OCEANBASE_PL_CACHE_H_
#include "share/ob_define.h"
#include "sql/ob_sql_define.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "pl/ob_pl.h"
#include "pl/pl_cache/ob_pl_cache_object.h"

namespace oceanbase
{


namespace pl
{

struct ObPLCacheCtx;
class ObPLObjectSet;
class ObPLCacheMgr;

struct ObPLTableColumnInfo
{
  common::ObIAllocator *inner_alloc_;
  uint64_t column_id_;
  common::ObObjMeta meta_type_;
  common::ObAccuracy accuracy_;
  common::ObCharsetType charset_type_;
  common::ObString column_name_;
  common::ObArray<common::ObString> type_info_;//used for enum and set

  ObPLTableColumnInfo():
    inner_alloc_(nullptr),
    column_id_(common::OB_INVALID_ID),
    meta_type_(),
    accuracy_(),
    charset_type_(CHARSET_INVALID),
    column_name_(),
    type_info_()
    {}

  explicit ObPLTableColumnInfo(ObIAllocator *alloc):
    inner_alloc_(alloc),
    column_id_(common::OB_INVALID_ID),
    meta_type_(),
    accuracy_(),
    charset_type_(CHARSET_INVALID),
    column_name_(),
    type_info_()
    {}

  bool operator==(const ObPLTableColumnInfo &other) const
  {
    bool is_same = true;
    if (type_info_.count() != other.type_info_.count()) {
      is_same = false;
    } else {
      for (int64_t i = 0; is_same && i < type_info_.count(); ++i) {
        is_same = type_info_.at(i) == other.type_info_.at(i);
      }
      is_same = is_same &&
                column_id_ == other.column_id_ &&
                meta_type_ == other.meta_type_ &&
                accuracy_ == other.accuracy_ &&
                charset_type_ == other.charset_type_ &&
                column_name_ == other.column_name_;
    }
    return is_same;
  }

  bool operator!=(const ObPLTableColumnInfo &other) const
  {
    return !operator==(other);
  }

  void reset();
  int deep_copy_type_info(const common::ObIArray<common::ObString>& type_info);
  ~ObPLTableColumnInfo()
  {
    reset();
  }

  TO_STRING_KV(K_(column_id),
               K_(meta_type),
               K_(accuracy),
               K_(charset_type),
               K_(column_name));
};

//todo:when PCVSchemaObj has been moved to appropriate header file, use PCVSchemaObj to instead of PCVPlSchemaObj
struct PCVPlSchemaObj
{
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t schema_id_;
  int64_t schema_version_;
  share::schema::ObSchemaType schema_type_;
  share::schema::ObTableType table_type_;
  common::ObString table_name_;
  bool is_tmp_table_;
  bool is_explicit_db_name_;
  common::ObIAllocator *inner_alloc_;
  int64_t column_cnt_;
  common::ObFixedArray<ObPLTableColumnInfo *, common::ObIAllocator> column_infos_;

  PCVPlSchemaObj():
  tenant_id_(common::OB_INVALID_ID),
  database_id_(common::OB_INVALID_ID),
  schema_id_(common::OB_INVALID_ID),
  schema_version_(0),
  schema_type_(share::schema::OB_MAX_SCHEMA),
  table_type_(share::schema::MAX_TABLE_TYPE),
  table_name_(),
  is_tmp_table_(false),
  is_explicit_db_name_(false),
  inner_alloc_(nullptr),
  column_cnt_(0),
  column_infos_(inner_alloc_) {}

  explicit PCVPlSchemaObj(ObIAllocator *alloc):
    tenant_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    schema_id_(common::OB_INVALID_ID),
    schema_version_(0),
    schema_type_(share::schema::OB_MAX_SCHEMA),
    table_type_(share::schema::MAX_TABLE_TYPE),
    table_name_(),
    is_tmp_table_(false),
    is_explicit_db_name_(false),
    inner_alloc_(alloc),
    column_cnt_(0),
    column_infos_(inner_alloc_) {}

  int init(const share::schema::ObTableSchema *schema);
  int init_with_version_obj(const share::schema::ObSchemaObjVersion &schema_obj_version);
  int init_without_copy_name(const share::schema::ObSimpleTableSchemaV2 *schema);
  void set_allocator(common::ObIAllocator *alloc)
  {
    inner_alloc_ = alloc;
  }
  int deep_copy_column_infos(const ObTableSchema *schema);

  bool compare_schema(const share::schema::ObTableSchema &schema) const
  {
    bool ret = false;
    ret = tenant_id_ == schema.get_tenant_id() &&
          database_id_ == schema.get_database_id() &&
          schema_id_ == schema.get_table_id() &&
          schema_version_ == schema.get_schema_version() &&
          table_type_ == schema.get_table_type();
    return ret;
  }

  bool match_compare(const PCVPlSchemaObj &other) const
  {
    bool ret = true;
    ret = tenant_id_ == other.tenant_id_
          && database_id_ == other.database_id_
          && table_type_ == other.table_type_;
    return ret;
  }

  bool match_columns(ObIArray<ObPLTableColumnInfo> &column_infos) const;

  bool operator==(const PCVPlSchemaObj &other) const;

  bool operator!=(const PCVPlSchemaObj &other) const
  {
    return !operator==(other);
  }

  void reset();
  ~PCVPlSchemaObj();

  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(schema_id),
               K_(schema_version),
               K_(schema_type),
               K_(table_type),
               K_(table_name),
               K_(is_tmp_table),
               K_(is_explicit_db_name));
};

// standalone procedure/function & package
struct ObPLObjectKey : public ObILibCacheKey
{
  ObPLObjectKey()
  : ObILibCacheKey(ObLibCacheNameSpace::NS_INVALID),
    db_id_(common::OB_INVALID_ID),
    key_id_(common::OB_INVALID_ID),
    sessid_(0),
    name_(),
    mode_(ObjectMode::NORMAL),
    sys_vars_str_() {}
  ObPLObjectKey(uint64_t db_id, uint64_t key_id)
  : ObILibCacheKey(ObLibCacheNameSpace::NS_INVALID),
    db_id_(db_id),
    key_id_(key_id),
    sessid_(0),
    name_(),
    mode_(ObjectMode::NORMAL),
    sys_vars_str_() {}

  void reset();
  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other) override;
  void destory(common::ObIAllocator &allocator);
  virtual uint64_t hash() const override;
  virtual bool is_equal(const ObILibCacheKey &other) const;

  TO_STRING_KV(K_(db_id),
               K_(key_id),
               K_(namespace),
               K_(name));

  enum class ObjectMode
  {
    NORMAL,
    PROFILE,
  };

  uint64_t  db_id_;
  uint64_t  key_id_; // routine id or package id
  uint32_t sessid_;

  // sessid_ != 0 and mode_ == NORMAL marks DEBUG compile, for now
  // TODO: unify DEBUG and PROFILE compile or add DEBUG mode separately
  common::ObString name_;
  ObjectMode mode_;
  common::ObString sys_vars_str_;
};


class ObPLObjectValue : public common::ObDLinkBase<ObPLObjectValue>
{
public:
  ObPLObjectValue(common::ObIAllocator &alloc) :
    pc_alloc_(&alloc),
    sys_schema_version_(OB_INVALID_VERSION),
    tenant_schema_version_(OB_INVALID_VERSION),
    sessid_(OB_INVALID_ID),
    sess_create_time_(0),
    contain_sys_name_table_(false),
    contain_tmp_table_(false),
    contain_sys_pl_object_(false),
    stored_schema_objs_(pc_alloc_),
    params_info_(ObWrapperAllocator(alloc)),
    pl_routine_obj_(NULL) {}

  virtual ~ObPLObjectValue() { reset(); }
  int init(const ObILibCacheObject &cache_obj, ObPLCacheCtx &pc_ctx);
  int set_stored_schema_objs(const DependenyTableStore &dep_table_store,
                              share::schema::ObSchemaGetterGuard *schema_guard);
  int lift_tenant_schema_version(int64_t new_schema_version);
  int obtain_new_column_infos(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const PCVPlSchemaObj &schema_obj,
                                              ObIArray<ObPLTableColumnInfo> &column_infos);
  int check_value_version(share::schema::ObSchemaGetterGuard *schema_guard,
                                  bool need_check_schema,
                                  const ObIArray<PCVPlSchemaObj> &schema_array,
                                  bool &is_old_version);
  int need_check_schema_version(ObPLCacheCtx &pc_ctx,
                                int64_t &new_schema_version,
                                bool &need_check);
  int get_synonym_schema_version(ObPLCacheCtx &pc_ctx,
                                  uint64_t tenant_id,
                                  const PCVPlSchemaObj &pcv_schema,
                                  int64_t &new_version);
  int get_all_dep_schema(ObPLCacheCtx &pc_ctx,
                          const uint64_t database_id,
                          int64_t &new_schema_version,
                          bool &need_check_schema,
                          ObIArray<PCVPlSchemaObj> &schema_array);
  // get all dependency schemas, used for add plan
  static int get_all_dep_schema(share::schema::ObSchemaGetterGuard &schema_guard,
                                const DependenyTableStore &dep_schema_objs,
                                common::ObIArray<PCVPlSchemaObj> &schema_array);
  int match_dep_schema(const ObPLCacheCtx &pc_ctx,
                        const ObIArray<PCVPlSchemaObj> &schema_array,
                        bool &is_same);
  int add_match_info(ObILibCacheCtx &ctx,
                      ObILibCacheKey *key,
                      const ObILibCacheObject &cache_obj);

  bool match_params_info(const Ob2DArray<ObPlParamInfo,
                                OB_MALLOC_BIG_BLOCK_SIZE,
                                ObWrapperAllocator, false> &infos);

  int match_complex_type_info(const ObPlParamInfo &param_info,
                              const ObObjParam &param,
                              bool &is_same) const;

  int match_param_info(const ObPlParamInfo &param_info,
                              const ObObjParam &param,
                              bool &is_same) const;

  int match_params_info(const ParamStore *params,
                                 bool &is_same);

  void reset();
  int64_t get_mem_size();

  TO_STRING_KV(K_(sys_schema_version),
               K_(tenant_schema_version),
               K_(sessid),
               K_(sess_create_time),
               K_(contain_sys_name_table),
               K_(contain_tmp_table),
               K_(contain_sys_pl_object),
               K_(stored_schema_objs));

public:
  common::ObIAllocator *pc_alloc_;
  int64_t sys_schema_version_;
  int64_t tenant_schema_version_;
  uint64_t sessid_; // session id for temporary table
  uint64_t sess_create_time_; // sess_create_time_ for temporary table
  bool contain_sys_name_table_;
  bool contain_tmp_table_;
  /* The update of the system package/class will only push up the schema version of the system tenant.
     If the object under the common tenant depends on the system package/class,
     In the update scenario, since the schema_version of ordinary users is not pushed up,
     it may miss checking whether the system package/type is out of date,
     Causes routine objects that depend on system packages/classes to be unavailable after updating,
     so schema checks are always performed on classes containing system packages/classes*/
  bool contain_sys_pl_object_;
  common::ObFixedArray<PCVPlSchemaObj *, common::ObIAllocator> stored_schema_objs_;
  common::Ob2DArray<ObPlParamInfo, common::OB_MALLOC_BIG_BLOCK_SIZE,
                    common::ObWrapperAllocator, false> params_info_;
  pl::ObPLCacheObject *pl_routine_obj_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLObjectValue);
};


struct ObPLCacheCtx : public ObILibCacheCtx
{
  ObPLCacheCtx()
    : ObILibCacheCtx(),
      handle_id_(MAX_HANDLE),
      key_(),
      session_info_(NULL),
      schema_guard_(NULL),
      need_add_obj_stat_(true),
      cache_params_(NULL),
      raw_sql_(),
      compile_time_(0)
  {
    sql_id_[0] = '\0';
    sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
  }

  CacheRefHandleID handle_id_;
  ObPLObjectKey key_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  ObSQLSessionInfo *session_info_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  bool need_add_obj_stat_;
  ParamStore *cache_params_;
  ObString raw_sql_;
  int64_t compile_time_; // pl object cost time of compile
};


class ObPLObjectSet : public ObILibCacheNode
{
public:
  ObPLObjectSet(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
    : ObILibCacheNode(lib_cache, mem_context),
      is_inited_(false),
      key_()
  {
  }
  virtual ~ObPLObjectSet()
  {
    destroy();
  };
  virtual int init(ObILibCacheCtx &ctx, const ObILibCacheObject *cache_obj) override;
  virtual int inner_get_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *&cache_obj) override;
  virtual int inner_add_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *cache_obj) override;

  void destroy();

  common::ObString &get_sql_id() { return sql_id_; }

  int create_new_pl_object_value(ObPLObjectValue *&pl_object_value);
  void free_pl_object_value(ObPLObjectValue *pl_object_value);
  int64_t get_mem_size();

  TO_STRING_KV(K_(is_inited));
private:
  bool is_inited_;
  ObPLObjectKey key_;  //used for manager key memory
  common::ObString sql_id_;
	// a list of plan sets with different param types combination
  common::ObDList<ObPLObjectValue> object_value_sets_;
};


} // namespace pl end
} // namespace oceanbase end

#endif