/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PREPARE_STMT_STRUCT_H_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PREPARE_STMT_STRUCT_H_

#include "lib/string/ob_string.h"
#include "sql/ob_result_set.h"
#include "sql/plan_cache/ob_plan_cache.h"

namespace oceanbase
{
using common::ObPsStmtId;
using namespace share::schema;
namespace sql
{
class ObCallProcedureStmt;

// prepared statement stmt key
struct ObPsSqlKey
{
public:
  ObPsSqlKey()
    : flag_(0),
      db_id_(OB_INVALID_ID),
      inc_id_(OB_INVALID_ID),
      ps_sql_()
  {}
  ObPsSqlKey(uint64_t db_id,
             const common::ObString &ps_sql)
    : flag_(0),
      db_id_(db_id),
      inc_id_(OB_INVALID_ID),
      ps_sql_(ps_sql)
  {}
  ObPsSqlKey(uint32_t flag,
             uint64_t db_id,
             uint64_t inc_id,
             const common::ObString &ps_sql)
    : flag_(flag),
      db_id_(db_id),
      inc_id_(inc_id),
      ps_sql_(ps_sql)
  {}
  int deep_copy(const ObPsSqlKey &other, common::ObIAllocator &allocator);
  int64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  ObPsSqlKey &operator=(const ObPsSqlKey &other);
  bool operator==(const ObPsSqlKey &other) const;
  void set_is_client_return_rowid()
  {
    is_client_return_hidden_rowid_ = true;
  }
  bool get_is_client_return_rowid()
  {
    return is_client_return_hidden_rowid_;
  }
  void set_flag(uint32_t flag)
  {
    flag_ = flag;
  }
  uint32_t get_flag() const
  {
    return flag_;
  }
  void reset()
  {
    flag_ = 0;
    db_id_ = OB_INVALID_ID;
    inc_id_ = OB_INVALID_ID;
    ps_sql_.reset();
  }
  TO_STRING_KV(K_(flag), K_(db_id), K_(inc_id), K_(ps_sql));

public:
  union
  {
    uint32_t flag_;
    struct {
      uint32_t is_client_return_hidden_rowid_ : 1;
      uint32_t reserved_ : 31;
    };
  };
  uint64_t db_id_;
  // MySQL allows session-level temporary tables with the same name to have different schema definitions.
  // In order to distinguish this scenario, an incremental id is used to generate different prepared
  // statements each time.
  uint64_t inc_id_;
  common::ObString ps_sql_;
};

//ps stmt item
class ObPsStmtItem
{
public:
  ObPsStmtItem();
  explicit ObPsStmtItem(const ObPsStmtId stmt_id);
  explicit ObPsStmtItem(common::ObIAllocator *inner_allocator,
                        common::ObIAllocator *external_allocator);
  virtual ~ObPsStmtItem() {}

  int deep_copy(const ObPsStmtItem &other);
  ObPsStmtItem &operator=(const ObPsStmtItem &other);

  bool is_valid() const;
  ObPsStmtId get_ps_stmt_id() const { return stmt_id_; }

  bool check_erase_inc_ref_count();
  void dec_ref_count();
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }

  int get_convert_size(int64_t &cv_size) const;
  const ObPsSqlKey& get_sql_key() const { return ps_key_; }
  void assign_sql_key(const ObPsSqlKey &ps_sql_key)
  {
    ps_key_ = ps_sql_key;
  }
  bool *get_is_expired_evicted_ptr() { return &is_expired_evicted_; }

  ObIAllocator *get_external_allocator() { return external_allocator_; }

  TO_STRING_KV(K_(ref_count), K_(ps_key), K_(stmt_id), K_(is_expired_evicted));

private:
  volatile int64_t ref_count_;
  ObPsSqlKey ps_key_;
  ObPsStmtId stmt_id_;
  bool is_expired_evicted_;
  //ObDataBuffer用于ObPsStmtItem内部内存的使用，内存实质上来自ObPsPlancache中的inner_allocator_
  common::ObIAllocator *allocator_;
  //指向ObPsPlancache中的inner_allocator_, 用于释放整个ObPsStmtItem的内存
  common::ObIAllocator *external_allocator_;
};

struct ObPsSqlMeta
{
public:

  explicit ObPsSqlMeta(common::ObIAllocator *allocator)
    : allocator_(allocator),
      param_fields_(allocator),
      column_fields_(allocator)
  {}

  int reverse_fileds(int64_t param_size, int64_t column_size);

  int deep_copy(const ObPsSqlMeta &sql_meta);
  int get_convert_size(int64_t &cv_size) const;
  int64_t get_param_size() const { return param_fields_.count(); }
  int64_t get_column_size() const { return column_fields_.count(); }
  int add_param_field(const common::ObField &field);
  int add_column_field(const common::ObField &field);
  const common::ObIArray<ObField> &get_param_fields() const { return param_fields_; };
  const common::ObIArray<ObField> &get_column_fields() const { return column_fields_; };
private:
  common::ObIAllocator *allocator_;
  ObFixedArray<ObField, common::ObIAllocator> param_fields_;
  ObFixedArray<ObField, common::ObIAllocator> column_fields_;
};

class ObPsStmtInfo
{
public:
  explicit ObPsStmtInfo(common::ObIAllocator *inner_allocator);
  ObPsStmtInfo(common::ObIAllocator *inner_allocator,
               common::ObIAllocator *external_allocator);
  virtual ~ObPsStmtInfo() {};

  inline void set_question_mark_count(int64_t count) { question_mark_count_ = count; }
  inline int64_t get_question_mark_count() { return question_mark_count_; }
  inline int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  inline int64_t get_num_of_param() const { return ps_sql_meta_.get_param_size(); }
  inline int64_t get_num_of_column() const { return ps_sql_meta_.get_column_size(); }
  inline stmt::StmtType get_stmt_type() const { return stmt_type_; }
  inline void set_stmt_type(stmt::StmtType stmt_type) { stmt_type_ = stmt_type; }
  inline stmt::StmtType get_literal_stmt_type() const { return literal_stmt_type_; }
  inline void set_literal_stmt_type(stmt::StmtType stmt_type) { literal_stmt_type_ = stmt_type; }
  const ObPsSqlKey& get_sql_key() const { return ps_key_; }
  inline const common::ObString &get_ps_sql() const { return ps_key_.ps_sql_; }
  inline const common::ObString &get_no_param_sql() const { return no_param_sql_; }
  inline const common::ObIArray<int64_t> &get_raw_params_idx() const
  { return raw_params_idx_; }
  inline const common::ObIArray<ObPCParam *> &get_fixed_raw_params() const { return raw_params_; }
  inline const ObPsSqlMeta &get_ps_sql_meta() const { return ps_sql_meta_; }
  inline bool can_direct_use_param() const { return can_direct_use_param_; }
  inline void set_can_direct_use_param(bool v) { can_direct_use_param_ = v; }
  inline void set_ps_stmt_checksum(uint64_t ps_checksum) { ps_stmt_checksum_ = ps_checksum; }
  inline uint64_t get_ps_stmt_checksum() const { return ps_stmt_checksum_; }

  inline void set_num_of_returning_into(int32_t num_of_returning_into)
  { num_of_returning_into_ = num_of_returning_into; }
  inline int32_t get_num_of_returning_into() const { return num_of_returning_into_; }
  inline void set_is_sensitive_sql(const bool is_sensitive_sql) { is_sensitive_sql_ = is_sensitive_sql; }
  inline bool get_is_sensitive_sql() const { return is_sensitive_sql_; }
  inline const common::ObString &get_raw_sql() const { return raw_sql_; }

  bool is_valid() const;
  bool check_erase_inc_ref_count();
  void dec_ref_count();
  int deep_copy(const ObPsStmtInfo &other);
  int add_param_field(const common::ObField &param);
  int add_column_field(const common::ObField &column);
  int get_convert_size(int64_t &cv_size) const;
  int assign_raw_sql(const common::ObString &raw_sql);
  int assign_no_param_sql(const common::ObString &no_param_sql);
  int assign_fixed_raw_params(const common::ObIArray<int64_t> &param_idxs,
                              const common::ObIArray<ObPCParam *> &raw_params);
  int deep_copy_fixed_raw_params(const common::ObIArray<int64_t> &param_idxs,
                                 const common::ObIArray<ObPCParam *> &raw_params);
  int add_fixed_raw_param(const ObPCParam &node);

  void set_item_and_info_size(int64_t size) { item_and_info_size_ = size; }
  int64_t get_item_and_info_size() { return item_and_info_size_; }

  int64_t get_last_closed_timestamp() { return last_closed_timestamp_; }

  int reserve_ps_meta_fields(int64_t param_size, int64_t column_size)
  { return ps_sql_meta_.reverse_fileds(param_size, column_size); };

  void assign_sql_key(const ObPsStmtItem &ps_stmt_item)
  {
    ps_key_ = ps_stmt_item.get_sql_key();
  }
  ObIAllocator *get_external_allocator() { return external_allocator_; }
  void set_inner_allocator(common::ObIAllocator *allocator)
  {
    allocator_ = allocator;
  }
  ObIAllocator *get_inner_allocator() { return allocator_; }

  void set_dep_objs(ObSchemaObjVersion *dep_objs, int64_t dep_objs_cnt) {
    dep_objs_ = dep_objs;
    dep_objs_cnt_ = dep_objs_cnt;
  }
  ObSchemaObjVersion *get_dep_objs() { return dep_objs_; }
  const ObSchemaObjVersion *get_dep_objs() const { return dep_objs_; }
  int64_t get_dep_objs_cnt() const { return dep_objs_cnt_; }
  ObPsStmtItem *get_ps_item() const { return ps_item_; }
  void set_ps_item(ObPsStmtItem *ps_item) { ps_item_ = ps_item; }
  int64_t get_tenant_version() const { return tenant_version_; }
  void set_tenant_version(int64_t tenant_version) { tenant_version_ = tenant_version; }
  void set_is_expired() { ATOMIC_STORE(&is_expired_, true); }
  bool is_expired() { return ATOMIC_LOAD(&is_expired_); }
  bool *get_is_expired_evicted_ptr() { return &is_expired_evicted_; }
  bool try_erase() { return 1 == ATOMIC_VCAS(&ref_count_, 1, 0); }

  DECLARE_VIRTUAL_TO_STRING;

private:
  stmt::StmtType stmt_type_;
  uint64_t ps_stmt_checksum_;
  ObPsSqlKey ps_key_;
  ObPsSqlMeta ps_sql_meta_;
  volatile int64_t ref_count_;
  // simple prepare protocol协议不会填充ps_sql_meta, 这里记录下question mark cnt, 用于execute时对入参个数进行检查
  int64_t question_mark_count_;

  // for call procedure
  bool can_direct_use_param_;
  int64_t item_and_info_size_; // mem_used_;
  int64_t last_closed_timestamp_; //引用计数上次减到1时的时间;
  ObSchemaObjVersion *dep_objs_;
  int64_t dep_objs_cnt_;
  ObPsStmtItem *ps_item_;
  int64_t tenant_version_;
  bool is_expired_;
  //check whether has dec ref count for ps info expired
  bool is_expired_evicted_;

  //ObDataBuffer用于ObPsStmtItem内部内存的使用，
  //内存实质上来自ObPsPlancache中的inner_allocator_
  common::ObIAllocator *allocator_;
  //指向ObPsPlancache中的inner_allocator_, 用于释放整个ObPsStmtItem的内存
  common::ObIAllocator *external_allocator_;
  int32_t num_of_returning_into_;
  common::ObString no_param_sql_;
  bool is_sensitive_sql_;
  common::ObString raw_sql_;
  // raw_params_ records constants other than question mark in raw prepare sql
  // raw_params_idx_ records the offset of the constants in raw_params_ in param_store
  // E.g: prepare stmt from 'select 3 + ? + 2 from dual';
  // raw_params_: 3, 2
  // raw_params_idx_: 0, 2
  ObFixedArray<ObPCParam *, common::ObIAllocator> raw_params_;
  ObFixedArray<int64_t, common::ObIAllocator> raw_params_idx_;
  stmt::StmtType literal_stmt_type_;
};

struct TypeInfo {
  TypeInfo()
    : relation_name_(),
      package_name_(),
      type_name_(),
      elem_type_(),
      is_elem_type_(false),
      is_basic_type_(true) {}

  TypeInfo(const common::ObString &relation_name,
           const common::ObString &package_name,
           const common::ObString &type_name,
           const common::ObDataType &type,
           bool is_elem_type = false,
           bool is_basic_type = false)
    : relation_name_(relation_name),
      package_name_(package_name),
      type_name_(type_name),
      elem_type_(type),
      is_elem_type_(is_elem_type),
      is_basic_type_(is_basic_type) {}

  int deep_copy(common::ObIAllocator *allocator, const TypeInfo* other);

  common::ObString relation_name_;
  common::ObString package_name_;
  common::ObString type_name_;
  common::ObDataType elem_type_;
  bool is_elem_type_;
  bool is_basic_type_;

  TO_STRING_KV(K_(relation_name),
               K_(package_name),
               K_(type_name),
               K_(elem_type),
               K_(is_elem_type),
               K_(is_basic_type));
};

typedef common::ObSEArray<obmysql::EMySQLFieldType, 48> ParamTypeArray;
typedef common::ObSEArray<TypeInfo, 16> ParamTypeInfoArray;
typedef common::ObSEArray<bool, 16> ParamCastArray;

// 每个session中同一个statement的prepare只会记录一个stmt_id-->ps_session_info的映射
// 当有多个应用线程使用同一个session， 分别对同一个语句进行prepare时, 此时会出现在重复prepare的情况, 这些应用线程拿到的ps_stmt_id都时一样的
// 同时在每个线程多次execute后，会进行close，此时会出现对该session上同一个stmt_id进行多次close，为避免第一次close时session上stmt_id-->ps_session_info
// 已经被删除，导致其他线程execute和close时通过stmt_id找不到ps相关信息，因此添加一个引用计数。
// 在一个session上，对同一个statement进行prepare时，每次prepare将ps_session_info引用计数加1， 在每次close时将引用计数减1， 如果引用计数为0，
// 则释放ps_session_info信息。
//
// 对于ps cache， 每个session上对于某一个statement第一次被prepare时, session上会添加ps_session_info, 并会增加对ps_cache中ps item和ps info的引用,
// 当statement上ps session info引用计数被close到0时，会减去ps cache中ps item和ps info的引用，当ps item/info引用计数为0时, 将其从 ps cache中释放
class ObPsSessionInfo
{
public:
  ObPsSessionInfo(const int64_t tenant_id, const int64_t num_of_params) :
    stmt_id_(common::OB_INVALID_STMT_ID),
    stmt_type_(stmt::T_NONE),
    num_of_params_(num_of_params),
    ps_stmt_checksum_(0),
    ref_cnt_(0),
    inner_stmt_id_(0),
    num_of_returning_into_(common::OB_INVALID_STMT_ID) // num_of_returning_into_ init as -1
  {
    param_types_.set_attr(ObMemAttr(tenant_id, "ParamTypes"));
    param_type_infos_.set_attr(ObMemAttr(tenant_id, "ParamTypesInfo"));
    param_types_.reserve(num_of_params_);
  }
  //{ param_types_.set_label(common::ObModIds::OB_PS_SESSION_INFO_ARRAY); }
  virtual ~ObPsSessionInfo() {}

  void set_stmt_id(const ObPsStmtId stmt_id) { stmt_id_ = stmt_id; }
  ObPsStmtId get_stmt_id() const { return stmt_id_; }

  const ParamTypeArray &get_param_types() const { return param_types_; }
  ParamTypeArray &get_param_types() { return param_types_; }

  const ParamTypeInfoArray &get_param_type_infos() const { return param_type_infos_; }
  ParamTypeInfoArray &get_param_type_infos() { return param_type_infos_; }

  int64_t get_param_count() const { return num_of_params_; }
  void set_param_count(const int64_t num_of_params) { num_of_params_ = num_of_params; }

  int32_t get_num_of_returning_into() const { return num_of_returning_into_; }
  void set_num_of_returning_into(const int32_t num_of_returning_into)
  { num_of_returning_into_ = num_of_returning_into; }

  uint64_t get_ps_stmt_checksum() const { return ps_stmt_checksum_; }
  void set_ps_stmt_checksum(uint64_t ps_checksum) { ps_stmt_checksum_ = ps_checksum; }

  stmt::StmtType get_stmt_type() const { return stmt_type_; }
  void set_stmt_type(const stmt::StmtType stmt_type) { stmt_type_ = stmt_type; }
  void inc_ref_count() { ref_cnt_++; }
  void dec_ref_count() { ref_cnt_--; }
  bool need_erase() { return 0 == ref_cnt_; }

  inline void set_inner_stmt_id(ObPsStmtId id) { inner_stmt_id_ = id; }
  inline ObPsStmtId get_inner_stmt_id() { return inner_stmt_id_; }

  TO_STRING_KV(K_(stmt_id),
               K_(stmt_type),
               K_(num_of_params),
               K_(ref_cnt),
               K_(ps_stmt_checksum),
               K_(inner_stmt_id),
               K_(num_of_returning_into));

private:
  ObPsStmtId stmt_id_;
  stmt::StmtType stmt_type_;
  int64_t num_of_params_;
  uint64_t ps_stmt_checksum_; //actual is crc32
  ParamTypeArray param_types_;
  ParamTypeInfoArray param_type_infos_;
  int64_t ref_cnt_;
  ObPsStmtId inner_stmt_id_;
  int32_t num_of_returning_into_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPsSessionInfo);
};

class ObPsStmtInfoGuard
{
public:
  ObPsStmtInfoGuard()
    : ps_cache_(NULL), stmt_info_(NULL), stmt_id_(common::OB_INVALID_STMT_ID) {}
  virtual ~ObPsStmtInfoGuard();

  inline void set_ps_cache(ObPsCache &ps_cache) { ps_cache_ = &ps_cache; }
  inline void set_stmt_info(ObPsStmtInfo &stmt_info) { stmt_info_ = &stmt_info; }
  inline void set_ps_stmt_id(const ObPsStmtId ps_stmt_id) { stmt_id_ = ps_stmt_id; }
  inline ObPsStmtInfo *get_stmt_info() { return stmt_info_; }

  int get_ps_sql(common::ObString &ps_sql);

private:
  ObPsCache *ps_cache_;
  ObPsStmtInfo *stmt_info_;
  ObPsStmtId stmt_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtInfoGuard);
};

struct PsCacheInfoCtx
{
  PsCacheInfoCtx()
  : param_cnt_(0),
    num_of_returning_into_(-1),
    is_inner_sql_(false),
    is_sensitive_sql_(false),
    normalized_sql_(),
    raw_sql_(),
    no_param_sql_(),
    raw_params_(NULL),
    fixed_param_idx_(NULL),
    stmt_type_(stmt::T_NONE) {}


  TO_STRING_KV(K_(param_cnt),
               K_(num_of_returning_into),
               K_(is_inner_sql),
               K_(is_sensitive_sql),
               K_(normalized_sql),
               K_(raw_sql),
               K_(no_param_sql),
               K_(stmt_type));

  int64_t param_cnt_;
  int32_t num_of_returning_into_;
  bool is_inner_sql_;
  bool is_sensitive_sql_;
  common::ObString normalized_sql_;
  common::ObString raw_sql_;
  common::ObString no_param_sql_;
  common::ObIArray<ObPCParam*> *raw_params_;
  common::ObIArray<int64_t> *fixed_param_idx_;
  stmt::StmtType stmt_type_;
};

} //end of namespace sql
} //end of namespace oceanbase

#endif //OCEANBASE_SQL_PLAN_CACHE_OB_PREPARE_STMT_STRUCT_H_
