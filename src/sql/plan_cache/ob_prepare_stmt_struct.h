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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PREPARE_STMT_STRUCT_H_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PREPARE_STMT_STRUCT_H_

#include "lib/string/ob_string.h"
#include "sql/ob_result_set.h"
#include "sql/plan_cache/ob_plan_cache.h"

namespace oceanbase {
using common::ObPsStmtId;
using namespace share::schema;
namespace sql {

// ps stmt key
class ObPsSqlKey {
public:
  ObPsSqlKey() : db_id_(OB_INVALID_ID), ps_sql_(), allocator_(NULL)
  {}
  ObPsSqlKey(uint64_t db_id, const common::ObString& ps_sql) : db_id_(db_id), ps_sql_(ps_sql), allocator_(NULL)
  {}
  // for deep copy
  explicit ObPsSqlKey(common::ObIAllocator* allocator) : db_id_(), ps_sql_(), allocator_(allocator)
  {}
  int deep_copy(const ObPsSqlKey& other);

  int64_t hash() const;

  ObPsSqlKey& operator=(const ObPsSqlKey& other);
  bool operator==(const ObPsSqlKey& other) const;
  // need to reset allocator?
  void reset()
  {
    db_id_ = OB_INVALID_ID;
    ps_sql_.reset();
  }

  int get_convert_size(int64_t& cv_size) const;
  uint64_t get_db_id() const
  {
    return db_id_;
  }
  const common::ObString& get_ps_sql() const
  {
    return ps_sql_;
  }

  void set_db_id(uint64_t db_id)
  {
    db_id_ = db_id;
  }  // not deep copy
  void set_ps_sql(const common::ObString& ps_sql)
  {
    ps_sql_ = ps_sql;
  }  // not deep copy
  TO_STRING_KV(K_(db_id), K_(ps_sql));

private:
  uint64_t db_id_;  // tenant id encoded in database id
  common::ObString ps_sql_;
  common::ObIAllocator* allocator_;
};

// ps stmt item
class ObPsStmtItem {
public:
  ObPsStmtItem();
  explicit ObPsStmtItem(const ObPsStmtId stmt_id);
  explicit ObPsStmtItem(common::ObIAllocator* inner_allocator, common::ObIAllocator* external_allocator);
  virtual ~ObPsStmtItem()
  {}

  int deep_copy(const ObPsStmtItem& other);
  ObPsStmtItem& operator=(const ObPsStmtItem& other);

  bool is_valid() const;
  ObPsStmtId get_ps_stmt_id() const
  {
    return stmt_id_;
  }

  bool check_erase_inc_ref_count();
  void dec_ref_count_check_erase();
  int64_t get_ref_count() const
  {
    return ATOMIC_LOAD(&ref_count_);
  }

  int get_convert_size(int64_t& cv_size) const;
  uint64_t get_db_id() const
  {
    return db_id_;
  }
  const common::ObString& get_ps_sql() const
  {
    return ps_sql_;
  }

  void get_sql_key(ObPsSqlKey& ps_sql_key)
  {
    ps_sql_key.set_db_id(db_id_);
    ps_sql_key.set_ps_sql(ps_sql_);
  }
  void assign_sql_key(const ObPsSqlKey& ps_sql_key)
  {
    db_id_ = ps_sql_key.get_db_id();
    ps_sql_ = ps_sql_key.get_ps_sql();
  }

  ObIAllocator* get_external_allocator()
  {
    return external_allocator_;
  }

  TO_STRING_KV(K_(ref_count), K_(db_id), K_(ps_sql), K_(stmt_id));

private:
  volatile int64_t ref_count_;
  uint64_t db_id_;
  common::ObString ps_sql_;
  ObPsStmtId stmt_id_;
  // ObDataBuffer is used to use the internal memory of ObPsStmtItem.
  // The memory essentially comes from inner_allocator_ in ObPsPlancache
  common::ObIAllocator* allocator_;
  // Point to inner_allocator_ in ObPsPlancache, used to release the memory of the entire ObPsStmtItem
  common::ObIAllocator* external_allocator_;
};

struct ObPsSqlMeta {
public:
  explicit ObPsSqlMeta(common::ObIAllocator* allocator)
      : allocator_(allocator), param_fields_(allocator), column_fields_(allocator)
  {}

  int reverse_fileds(int64_t param_size, int64_t column_size);

  int deep_copy(const ObPsSqlMeta& sql_meta);
  int get_convert_size(int64_t& cv_size) const;
  int64_t get_param_size() const
  {
    return param_fields_.count();
  }
  int64_t get_column_size() const
  {
    return column_fields_.count();
  }
  int add_param_field(const common::ObField& field);
  int add_column_field(const common::ObField& field);
  const common::ObIArray<ObField>& get_param_fields() const
  {
    return param_fields_;
  };
  const common::ObIArray<ObField>& get_column_fields() const
  {
    return column_fields_;
  };

private:
  common::ObIAllocator* allocator_;
  ObFixedArray<ObField, common::ObIAllocator> param_fields_;
  ObFixedArray<ObField, common::ObIAllocator> column_fields_;
};

class ObPsStmtInfo {
public:
  explicit ObPsStmtInfo(common::ObIAllocator* inner_allocator);
  ObPsStmtInfo(common::ObIAllocator* inner_allocator, common::ObIAllocator* external_allocator);
  virtual ~ObPsStmtInfo()
  {}

  inline void set_question_mark_count(int64_t count)
  {
    question_mark_count_ = count;
  }
  inline int64_t get_question_mark_count()
  {
    return question_mark_count_;
  }
  inline int64_t get_ref_count() const
  {
    return ATOMIC_LOAD(&ref_count_);
  }
  inline int64_t get_num_of_param() const
  {
    return ps_sql_meta_.get_param_size();
  }
  inline int64_t get_num_of_column() const
  {
    return ps_sql_meta_.get_column_size();
  }
  inline stmt::StmtType get_stmt_type() const
  {
    return stmt_type_;
  }
  inline void set_stmt_type(stmt::StmtType stmt_type)
  {
    stmt_type_ = stmt_type;
  }
  inline uint64_t get_db_id() const
  {
    return db_id_;
  }
  inline const common::ObString& get_ps_sql() const
  {
    return ps_sql_;
  }
  inline const ObPsSqlMeta& get_ps_sql_meta() const
  {
    return ps_sql_meta_;
  }
  inline bool can_direct_use_param() const
  {
    return can_direct_use_param_;
  }
  inline void set_can_direct_use_param(bool v)
  {
    can_direct_use_param_ = v;
  }
  inline bool has_complex_argument() const
  {
    return has_complex_argument_;
  }
  inline void set_has_complex_argument(bool v)
  {
    has_complex_argument_ = v;
  }
  inline void set_ps_stmt_checksum(uint64_t ps_checksum)
  {
    ps_stmt_checksum_ = ps_checksum;
  }
  inline uint64_t get_ps_stmt_checksum() const
  {
    return ps_stmt_checksum_;
  }

  bool is_valid() const;
  bool check_erase_inc_ref_count();
  bool dec_ref_count_check_erase();
  int deep_copy(const ObPsStmtInfo& other);
  int add_param_field(const common::ObField& param);
  int add_column_field(const common::ObField& column);
  int get_convert_size(int64_t& cv_size) const;

  void set_item_and_info_size(int64_t size)
  {
    item_and_info_size_ = size;
  }
  int64_t get_item_and_info_size()
  {
    return item_and_info_size_;
  }

  int64_t get_last_closed_timestamp()
  {
    return last_closed_timestamp_;
  }

  int reserve_ps_meta_fields(int64_t param_size, int64_t column_size)
  {
    return ps_sql_meta_.reverse_fileds(param_size, column_size);
  };

  void assign_sql_key(const ObPsStmtItem& ps_stmt_item)
  {
    db_id_ = ps_stmt_item.get_db_id();
    ps_sql_ = ps_stmt_item.get_ps_sql();
  }
  ObIAllocator* get_external_allocator()
  {
    return external_allocator_;
  }
  void set_inner_allocator(common::ObIAllocator* allocator)
  {
    allocator_ = allocator;
  }
  ObIAllocator* get_inner_allocator()
  {
    return allocator_;
  }

  void set_dep_objs(ObSchemaObjVersion* dep_objs, int64_t dep_objs_cnt)
  {
    dep_objs_ = dep_objs;
    dep_objs_cnt_ = dep_objs_cnt;
  }
  ObSchemaObjVersion* get_dep_objs()
  {
    return dep_objs_;
  }
  const ObSchemaObjVersion* get_dep_objs() const
  {
    return dep_objs_;
  }
  int64_t get_dep_objs_cnt() const
  {
    return dep_objs_cnt_;
  }
  ObPsStmtItem* get_ps_item() const
  {
    return ps_item_;
  }
  void set_ps_item(ObPsStmtItem* ps_item)
  {
    ps_item_ = ps_item;
  }
  int64_t get_tenant_version() const
  {
    return tenant_version_;
  }
  void set_tenant_version(int64_t tenant_version)
  {
    tenant_version_ = tenant_version;
  }
  void set_is_expired()
  {
    ATOMIC_STORE(&is_expired_, true);
  }
  int64_t is_expired()
  {
    return ATOMIC_LOAD(&is_expired_);
  }

  DECLARE_VIRTUAL_TO_STRING;

private:
  stmt::StmtType stmt_type_;
  uint64_t ps_stmt_checksum_;
  uint64_t db_id_;
  common::ObString ps_sql_;
  ObPsSqlMeta ps_sql_meta_;
  volatile int64_t ref_count_;
  int64_t question_mark_count_;

  // for call procedure
  bool can_direct_use_param_;
  bool has_complex_argument_;
  int64_t item_and_info_size_;     // mem_used_;
  int64_t last_closed_timestamp_;  // The time when the reference count was last reduced to 1;
  ObSchemaObjVersion* dep_objs_;
  int64_t dep_objs_cnt_;
  ObPsStmtItem* ps_item_;
  int64_t tenant_version_;
  bool is_expired_;

  // ObDataBuffer is used to use the internal memory of ObPsStmtItem,
  // The memory essentially comes from inner_allocator_ in ObPsPlancache
  common::ObIAllocator* allocator_;
  // Point to inner_allocator_ in ObPsPlancache, used to release the memory of the entire ObPsStmtItem
  common::ObIAllocator* external_allocator_;
};

struct TypeInfo {
  TypeInfo() : relation_name_(), package_name_(), type_name_(), elem_type_(), is_elem_type_(false), is_basic_type_(true)
  {}

  TypeInfo(const common::ObString& relation_name, const common::ObString& package_name,
      const common::ObString& type_name, const common::ObDataType& type, bool is_elem_type = false,
      bool is_basic_type = false)
      : relation_name_(relation_name),
        package_name_(package_name),
        type_name_(type_name),
        elem_type_(type),
        is_elem_type_(is_elem_type),
        is_basic_type_(is_basic_type)
  {}

  int deep_copy(common::ObIAllocator* allocator, const TypeInfo* other);

  common::ObString relation_name_;
  common::ObString package_name_;
  common::ObString type_name_;
  common::ObDataType elem_type_;
  bool is_elem_type_;
  bool is_basic_type_;

  TO_STRING_KV(K_(relation_name), K_(package_name), K_(type_name), K_(elem_type), K_(is_elem_type), K_(is_basic_type));
};

typedef common::ObSEArray<obmysql::EMySQLFieldType, 48> ParamTypeArray;
typedef common::ObSEArray<TypeInfo, 16> ParamTypeInfoArray;
typedef common::ObSEArray<bool, 16> ParamCastArray;

// The prepare of the same statement in each session will only record a mapping of stmt_id-->ps_session_info
// When multiple application threads use the same session and prepare the same statement separately,
// there will be repeated prepares. The ps_stmt_id obtained by these application threads are all the same
// At the same time, each thread will be closed after multiple executions.
// At this time, the same stmt_id on the session will be closed multiple times, in order to avoid stmt_id on the session
// when it is closed for the first time-->ps_session_info Has been deleted, causing other threads to find ps-related
// information through stmt_id when executing and closing, so add a reference count. In a session, when the same
// statement is prepared, the ps_session_info reference count is incremented by 1 each time the prepare is performed,
// and the reference count is decremented by 1 at each close. If the reference count is 0,
// Release ps_session_info information.
// For ps cache, when a statement is prepared for the first time in each session,
// ps_session_info will be added to the session, and references to ps item and ps info in ps_cache will be added,
// When the ps session info reference count on the statement is closed to 0,
// the ps item and ps info references in the ps cache will be subtracted.
// When the ps item/info reference count is 0, it will be released from the ps cache
class ObPsSessionInfo {
public:
  ObPsSessionInfo(const int64_t num_of_params)
      : stmt_id_(common::OB_INVALID_STMT_ID),
        stmt_type_(stmt::T_NONE),
        num_of_params_(num_of_params),
        ref_cnt_(0),
        inner_stmt_id_(0)
  {
    param_types_.reserve(num_of_params_);
  }
  //{ param_types_.set_label(common::ObModIds::OB_PS_SESSION_INFO_ARRAY); }
  virtual ~ObPsSessionInfo()
  {}

  void set_stmt_id(const ObPsStmtId stmt_id)
  {
    stmt_id_ = stmt_id;
  }
  ObPsStmtId get_stmt_id() const
  {
    return stmt_id_;
  }

  const ParamTypeArray& get_param_types() const
  {
    return param_types_;
  }
  ParamTypeArray& get_param_types()
  {
    return param_types_;
  }

  const ParamTypeInfoArray& get_param_type_infos() const
  {
    return param_type_infos_;
  }
  ParamTypeInfoArray& get_param_type_infos()
  {
    return param_type_infos_;
  }

  int64_t get_param_count() const
  {
    return num_of_params_;
  }
  void set_param_count(const int64_t num_of_params)
  {
    num_of_params_ = num_of_params;
  }

  uint64_t get_ps_stmt_checksum() const
  {
    return ps_stmt_checksum_;
  }
  void set_ps_stmt_checksum(uint64_t ps_checksum)
  {
    ps_stmt_checksum_ = ps_checksum;
  }

  stmt::StmtType get_stmt_type() const
  {
    return stmt_type_;
  }
  void set_stmt_type(const stmt::StmtType stmt_type)
  {
    stmt_type_ = stmt_type;
  }
  void inc_ref_count()
  {
    ref_cnt_++;
  }
  void dec_ref_count()
  {
    ref_cnt_--;
  }
  bool need_erase()
  {
    return 0 == ref_cnt_;
  }

  inline void set_inner_stmt_id(ObPsStmtId id)
  {
    inner_stmt_id_ = id;
  }
  inline ObPsStmtId get_inner_stmt_id()
  {
    return inner_stmt_id_;
  }

  TO_STRING_KV(K_(stmt_id), K_(stmt_type), K_(num_of_params), K_(ref_cnt), K_(ps_stmt_checksum), K_(inner_stmt_id));

private:
  ObPsStmtId stmt_id_;
  stmt::StmtType stmt_type_;
  int64_t num_of_params_;
  uint64_t ps_stmt_checksum_;  // actual is crc32
  ParamTypeArray param_types_;
  ParamTypeInfoArray param_type_infos_;
  int64_t ref_cnt_;
  ObPsStmtId inner_stmt_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPsSessionInfo);
};

class ObPsStmtInfoGuard {
public:
  ObPsStmtInfoGuard() : ps_cache_(NULL), stmt_info_(NULL), stmt_id_(common::OB_INVALID_STMT_ID)
  {}
  virtual ~ObPsStmtInfoGuard();

  inline void set_ps_cache(ObPsCache& ps_cache)
  {
    ps_cache_ = &ps_cache;
  }
  inline void set_stmt_info(ObPsStmtInfo& stmt_info)
  {
    stmt_info_ = &stmt_info;
  }
  inline void set_ps_stmt_id(const ObPsStmtId ps_stmt_id)
  {
    stmt_id_ = ps_stmt_id;
  }
  inline ObPsStmtInfo* get_stmt_info()
  {
    return stmt_info_;
  }

  int get_ps_sql(common::ObString& ps_sql);

private:
  ObPsCache* ps_cache_;
  ObPsStmtInfo* stmt_info_;
  ObPsStmtId stmt_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPsStmtInfoGuard);
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_PLAN_CACHE_OB_PREPARE_STMT_STRUCT_H_
