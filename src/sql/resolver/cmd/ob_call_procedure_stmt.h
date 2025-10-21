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

class ObCallProcedureInfo final : public pl::ObPLCacheObject
{
public:
  explicit ObCallProcedureInfo(lib::MemoryContext &mem_context)
      : pl::ObPLCacheObject(ObLibCacheNameSpace::NS_CALLSTMT, mem_context),
        can_direct_use_param_(false),
        package_id_(common::OB_INVALID_ID),
        routine_id_(common::OB_INVALID_ID),
        param_cnt_(0),
        out_bitmap_(),
        out_mode_(),
        out_name_(),
        out_type_(),
        out_type_name_(),
        out_type_owner_(),
        out_client_params_(),
        out_param_id_(),
        question_mark_idx_(),
        db_name_(),
        is_udt_routine_(false),
        enum_set_ctx_(allocator_) {
  }

  virtual ~ObCallProcedureInfo() {
  }

  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_package_id(const uint64_t package_id) { package_id_ = package_id; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline void set_routine_id(const uint64_t routine_id) { routine_id_ = routine_id; }

  inline int64_t get_output_count() const { return out_bitmap_.num_members(); }
  inline bool is_out_param(int64_t i) const { return out_bitmap_.has_member(i); }
  inline const ObIArray<int64_t>& get_out_mode() const { return out_mode_; }
  inline const ObIArray<ObString> &get_out_name() const { return out_name_; }
  inline const ObIArray<pl::ObPLDataType> &get_out_type() const { return out_type_; }
  inline const ObIArray<ObString> &get_out_type_name() const { return out_type_name_; }
  inline const ObIArray<ObString> &get_out_type_owner() const { return out_type_owner_; }
  inline int64_t get_client_output_count() const { return out_client_params_.num_members(); }
  inline bool is_client_out_param_by_param_id(int64_t i) const { return out_client_params_.has_member(i); }
  inline bool is_client_out_param_by_out_param_id(int64_t i) const {
    return i < out_param_id_.count() && is_client_out_param_by_param_id(out_param_id_.at(i));
  }
  inline bool is_out_param_by_question_mark_idx(int64_t i) const {
    return i < question_mark_idx_.count() && is_out_param(question_mark_idx_.at(i));
  }
  int add_out_param(int64_t i,
                    int64_t mode,
                    const ObString &name,
                    const pl::ObPLDataType &type,
                    const ObString &out_type_name,
                    const ObString &out_type_owner,
                    const bool is_client_out_param = true);
  int add_question_mark_idx(int64_t idx);

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
  pl::ObPLEnumSetCtx& get_enum_set_ctx() { return enum_set_ctx_; };

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
               K_(out_bitmap),
               K_(out_name),
               K_(out_type),
               K_(out_type_name),
               K_(out_type_owner),
               K_(out_client_params),
               K_(out_param_id),
               K_(question_mark_idx),
               K_(is_udt_routine));
private:
  bool can_direct_use_param_;
  uint64_t package_id_;
  uint64_t routine_id_;
  int64_t param_cnt_;

  /* out parameters information for
   * 1. returning out params to client
   * 2. setting user variables after procedure call */
  ObBitSet<> out_bitmap_;
  ObSEArray<int64_t, 32> out_mode_;
  ObSEArray<ObString, 32> out_name_;
  ObSEArray<pl::ObPLDataType, 32> out_type_;
  ObSEArray<ObString, 32> out_type_name_;
  ObSEArray<ObString, 32> out_type_owner_;
  /* MySQL mode does not return out parameters to non-standard drivers (obclient, opensource MySQL
   * driver) unless the parameter is bound with "?" */
  ObBitSet<> out_client_params_;
  ObSEArray<int64_t, 32> out_param_id_;
  /* record corresponding param idx of the question marks, for example:
   *     call proc(arg1, ?, arg3, ?, arg5);
   *     question_mark_idx_ = [1, 3]
   * means that the second and fourth parameters are question marks
   */
  ObSEArray<int64_t, 32> question_mark_idx_;

  ParamTypeInfoArray in_type_infos_;
  ObString db_name_;
  bool is_udt_routine_;
  pl::ObPLEnumSetCtx enum_set_ctx_;

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
