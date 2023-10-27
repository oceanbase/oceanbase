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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_STMT_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_schema.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "pl/pl_cache/ob_pl_cache_object.h"

namespace oceanbase
{
namespace sql
{

class ObCallProcedureInfo : public pl::ObPLCacheObject
{
public:
  explicit ObCallProcedureInfo(lib::MemoryContext &mem_context)
      : pl::ObPLCacheObject(ObLibCacheNameSpace::NS_CALLSTMT, mem_context),
        can_direct_use_param_(false),
        package_id_(common::OB_INVALID_ID),
        routine_id_(common::OB_INVALID_ID),
        param_cnt_(0),
        out_idx_(),
        out_mode_(),
        out_name_(),
        out_type_(),
        db_name_(),
        is_udt_routine_(false) {
  }

  virtual ~ObCallProcedureInfo() {
  }

  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_package_id(const uint64_t package_id) { package_id_ = package_id; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline void set_routine_id(const uint64_t routine_id) { routine_id_ = routine_id; }

  inline int64_t get_output_count() { return out_idx_.num_members(); }
  inline bool is_out_param(int64_t i) { return out_idx_.has_member(i); }
  inline ObBitSet<> &get_out_idx() { return out_idx_; }
  inline void set_out_idx(ObBitSet<> &v) { out_idx_ = v; }
  inline ObIArray<int64_t>& get_out_mode() { return out_mode_; }
  inline ObIArray<ObString> &get_out_name() { return out_name_; }
  inline ObIArray<pl::ObPLDataType> &get_out_type() { return out_type_; }
  inline ObIArray<ObString> &get_out_type_name() { return out_type_name_; }
  inline ObIArray<ObString> &get_out_type_owner() { return out_type_owner_; }
  //inline ObNewRow &get_output() { return output_; }
  int add_out_param(int64_t i, int64_t mode, const ObString &name,
                    const pl::ObPLDataType &type,
                    const ObString &out_type_name, const ObString &out_type_owner);

  const ParamTypeInfoArray& get_type_infos() const {
    return in_type_infos_;
  }
  inline int set_db_name(const ObString db_name)
  {
    return ob_write_string(get_allocator(), db_name, db_name_);
  }
  const ObString& get_db_name() const { return db_name_; }

  //int get_convert_size(int64_t &cv_size) const;
  void set_can_direct_use_param(bool v) { can_direct_use_param_ = v;}
  bool can_direct_use_param() const { return can_direct_use_param_; }
  void set_param_cnt(int64_t v) { param_cnt_ = v; }
  int64_t get_param_cnt() const { return param_cnt_; }

  void set_is_udt_routine(bool v) { is_udt_routine_ = v; }
  bool is_udt_routine() const { return is_udt_routine_; }

  int prepare_expression(const common::ObIArray<sql::ObRawExpr*> &params);
  int final_expression(const common::ObIArray<sql::ObRawExpr*> &params,
                       ObSQLSessionInfo *session_info,
                       share::schema::ObSchemaGetterGuard *schema_guard);

  virtual void reset();
  virtual void dump_deleted_log_info(const bool is_debug_log = true) const;
  virtual int check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add);

  //virtual obrpc::ObDDLArg &get_ddl_arg() { return ddl_arg_; }
  TO_STRING_KV(K_(can_direct_use_param),
               K_(package_id),
               K_(routine_id),
               //K_(params),
               K_(out_idx),
               K_(out_name),
               K_(out_type),
               K_(out_type_name),
               K_(out_type_owner),
               K_(is_udt_routine));
private:
  bool can_direct_use_param_;
  uint64_t package_id_;
  uint64_t routine_id_;
  int64_t param_cnt_;
  ObBitSet<> out_idx_;
  ObSEArray<int64_t, 32> out_mode_;
  ObSEArray<ObString, 32> out_name_;
  ObSEArray<pl::ObPLDataType, 32> out_type_;
  ObSEArray<ObString, 32> out_type_name_;
  ObSEArray<ObString, 32> out_type_owner_;
  ParamTypeInfoArray in_type_infos_;
  ObString db_name_;
  bool is_udt_routine_;

  DISALLOW_COPY_AND_ASSIGN(ObCallProcedureInfo);
};

class ObCallProcedureStmt : public ObCMDStmt
{
public:
  explicit ObCallProcedureStmt()
      : ObCMDStmt(NULL, stmt::T_CALL_PROCEDURE),
        call_proc_info_(NULL),
        cache_call_info_guard_(MAX_HANDLE),
        dblink_routine_info_(NULL)
  {
  }

  virtual ~ObCallProcedureStmt() {
    call_proc_info_ = NULL;
  }

  void set_call_proc_info(ObCallProcedureInfo *info) {
    call_proc_info_ = info;
  }
  ObCallProcedureInfo *get_call_proc_info() { return call_proc_info_; }
  ObCacheObjGuard &get_cacheobj_guard() { return cache_call_info_guard_; }
  void set_dblink_routine_info(const ObRoutineInfo *routine_info) { dblink_routine_info_ = routine_info; }
  const ObRoutineInfo *get_dblink_routine_info() const { return dblink_routine_info_; }
private:
  ObCallProcedureInfo *call_proc_info_;
  ObCacheObjGuard cache_call_info_guard_;
  const ObRoutineInfo *dblink_routine_info_;
  DISALLOW_COPY_AND_ASSIGN(ObCallProcedureStmt);
};


}//namespace sql
}//namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_STMT_H_ */
