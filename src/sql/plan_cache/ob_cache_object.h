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

#ifndef DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_H_
#define DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_H_
#include "share/ob_define.h"
#include "lib/container/ob_2d_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_dlist.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_fixed_array.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/plan_cache/ob_param_info.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/plan_cache/ob_pc_ref_handle.h"
#include "share/stat/ob_opt_stat_manager.h"

namespace test {
class MockCacheObjectFactory;
}
namespace oceanbase {
namespace common {
struct ObExprCtx;
}  // namespace common
namespace sql {
class ObSqlExpression;
class ObPreCalcExprFrameInfo;

enum ObCacheObjType {
  T_CO_SQL_CRSR = 0,  // sql physical plan
  T_CO_PRCR = 1,      // store procedure cache
  T_CO_SFC = 2,       // store function cache
  T_CO_PKG = 3,       // store package cache
  T_CO_ANON = 4,      // anonymous cache
  T_CO_MAX
};

struct ObOutlineState {
  OB_UNIS_VERSION(1);

public:
  ObOutlineState() : outline_version_(), is_plan_fixed_(false)
  {}
  ~ObOutlineState()
  {}
  void reset()
  {
    outline_version_.reset();
    is_plan_fixed_ = false;
  }
  TO_STRING_KV(K(outline_version_), K(is_plan_fixed_));
  share::schema::ObSchemaObjVersion outline_version_;
  bool is_plan_fixed_;  // whether the plan will be fixed with outline_content
};

typedef oceanbase::common::ObFixedArray<common::ObOptTableStatVersion, common::ObIAllocator> TableStatVersions;
typedef oceanbase::common::Ob2DArray<common::ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator,
    false>
    ParamStore;

struct DeletedCacheObjInfo {
  uint64_t obj_id_;
  uint64_t tenant_id_;
  int64_t log_del_time_;
  int64_t real_del_time_;
  int64_t ref_count_;
  int64_t mem_used_;
  bool added_to_pc_;

  DeletedCacheObjInfo(uint64_t obj_id, uint64_t tenant_id, int64_t log_del_time, int64_t real_del_time,
      int64_t ref_count, int64_t mem_used, bool added_to_pc)
      : obj_id_(obj_id),
        tenant_id_(tenant_id),
        log_del_time_(log_del_time),
        real_del_time_(real_del_time),
        ref_count_(ref_count),
        mem_used_(mem_used),
        added_to_pc_(added_to_pc)
  {}

  DeletedCacheObjInfo()
      : obj_id_(OB_INVALID_ID),
        tenant_id_(OB_INVALID_ID),
        log_del_time_(INT64_MAX),
        real_del_time_(INT64_MAX),
        ref_count_(0),
        mem_used_(0),
        added_to_pc_(false)
  {}

  TO_STRING_KV(
      K_(obj_id), K_(tenant_id), K_(log_del_time), K_(real_del_time), K_(ref_count), K_(added_to_pc), K_(mem_used));
};

class ObCacheObject {
  friend class ObCacheObjectFactory;
  friend class ::test::MockCacheObjectFactory;

public:
  ObCacheObject(ObCacheObjType co_type, lib::MemoryContext& mem_context = CURRENT_CONTEXT);
  virtual ~ObCacheObject()
  {}

  virtual void reset();
  inline ObCacheObjType get_type() const
  {
    return type_;
  }
  inline void set_type(ObCacheObjType co_type)
  {
    type_ = co_type;
  }
  inline bool is_sql_crsr() const
  {
    return T_CO_SQL_CRSR == type_;
  }
  inline bool is_prcr() const
  {
    return T_CO_PRCR == type_;
  }
  inline bool is_sfc() const
  {
    return T_CO_SFC == type_;
  }
  inline bool is_pkg() const
  {
    return T_CO_PKG == type_;
  }
  inline bool is_anon() const
  {
    return T_CO_ANON == type_;
  }
  inline uint64_t get_object_id() const
  {
    return object_id_;
  }
  inline int64_t get_dependency_table_size() const
  {
    return dependency_tables_.count();
  }
  inline const DependenyTableStore& get_dependency_table() const
  {
    return dependency_tables_;
  }
  inline void set_sys_schema_version(int64_t schema_version)
  {
    sys_schema_version_ = schema_version;
  }
  inline void set_tenant_schema_version(int64_t schema_version)
  {
    tenant_schema_version_ = schema_version;
  }
  inline int64_t get_tenant_schema_version() const
  {
    return tenant_schema_version_;
  }
  inline int64_t get_sys_schema_version() const
  {
    return sys_schema_version_;
  }
  int init_dependency_table_store(int64_t dependency_table_cnt)
  {
    return dependency_tables_.init(dependency_table_cnt);
  }
  int add_dependency_table_version(const common::ObIArray<uint64_t>& dependency_ids,
      share::schema::ObDependencyTableType table_type, share::schema::ObSchemaGetterGuard& schema_guard);
  inline DependenyTableStore& get_dependency_table()
  {
    return dependency_tables_;
  }
  inline TableStatVersions& get_table_stat_versions()
  {
    return table_stat_versions_;
  }
  inline const TableStatVersions& get_table_stat_versions() const
  {
    return table_stat_versions_;
  }
  bool has_sequence() const;
  int set_synonym_version(
      const common::ObIArray<uint64_t>& dependency_ids, share::schema::ObSchemaGetterGuard& schema_guard);
  int get_base_table_version(const uint64_t table_id, int64_t& table_version) const;
  inline const ObOutlineState& get_outline_state() const
  {
    return outline_state_;
  }
  inline void set_outline_state(const ObOutlineState& state)
  {
    outline_state_ = state;
  }
  inline int64_t get_merged_version() const
  {
    return merged_version_;
  }
  inline void set_merged_version(int64_t merged_version)
  {
    merged_version_ = merged_version;
  }
  int set_params_info(const ParamStore& params);
  const common::Ob2DArray<ObParamInfo, common::OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator, false>& get_params_info()
      const
  {
    return params_info_;
  }
  inline void set_is_contain_virtual_table(bool is_contain_virtual_table)
  {
    is_contain_virtual_table_ = is_contain_virtual_table;
  }
  inline void set_is_contain_inner_table(bool is_contain_inner_table)
  {
    is_contain_inner_table_ = is_contain_inner_table;
  }
  inline bool is_contain_virtual_table() const
  {
    return is_contain_virtual_table_;
  }
  inline bool is_contain_inner_table() const
  {
    return is_contain_inner_table_;
  }
  inline int64_t get_mem_size() const
  {
    return allocator_.total();
  }
  int64_t get_ref_count() const
  {
    return ATOMIC_LOAD(&ref_count_);
  }
  int64_t inc_ref_count(const CacheRefHandleID ref_handle);
  virtual void inc_pre_expr_ref_count()
  {}
  virtual void dec_pre_expr_ref_count()
  {}
  virtual int64_t get_pre_expr_ref_count() const
  {
    return -1;
  }
  virtual void set_pre_calc_expr_handler(PreCalcExprHandler* handler)
  {
    UNUSED(handler);
  }
  virtual PreCalcExprHandler* get_pre_calc_expr_handler()
  {
    return NULL;
  }
  inline common::ObIAllocator& get_allocator()
  {
    return allocator_;
  }
  inline lib::MemoryContext& get_mem_context()
  {
    return mem_context_;
  }
  inline const common::ObDList<ObSqlExpression>& get_pre_calc_exprs() const
  {
    return pre_calc_exprs_;
  }
  inline common::ObDList<ObSqlExpression>& get_pre_calc_exprs()
  {
    return pre_calc_exprs_;
  }
  inline const common::ObDList<ObPreCalcExprFrameInfo>& get_pre_calc_frames() const
  {
    return pre_calc_frames_;
  }
  inline common::ObDList<ObPreCalcExprFrameInfo>& get_pre_calc_frames()
  {
    return pre_calc_frames_;
  }
  inline void set_fetch_cur_time(bool fetch_cur_time)
  {
    fetch_cur_time_ = fetch_cur_time;
  }
  inline bool get_fetch_cur_time() const
  {
    return fetch_cur_time_;
  }
  inline void set_ignore(bool ignore)
  {
    is_ignore_stmt_ = ignore;
  }
  inline bool is_ignore() const
  {
    return is_ignore_stmt_;
  }
  inline void set_stmt_type(stmt::StmtType stmt_type)
  {
    stmt_type_ = stmt_type;
  }
  inline stmt::StmtType get_stmt_type() const
  {
    return stmt_type_;
  }
  inline bool added_pc() const
  {
    return added_to_pc_;
  }
  inline void set_added_pc(const bool added_to_pc)
  {
    added_to_pc_ = added_to_pc;
  }
  inline int64_t get_logical_del_time() const
  {
    return log_del_time_;
  }
  inline void set_logical_del_time(const int64_t timestamp)
  {
    log_del_time_ = timestamp;
  }
  inline bool should_release(const int64_t safe_timestamp) const
  {
    // only free leaked cache object
    return 0 != get_ref_count() && get_logical_del_time() < safe_timestamp;
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  static int pre_calculation(const stmt::StmtType& stmt_type, const bool is_ignore_stmt,
      const common::ObDList<ObSqlExpression>& pre_calc_exprs, ObExecContext& ctx);

  static int pre_calculation(
      const bool is_ignore_stmt, ObPreCalcExprFrameInfo& pre_calc_frame, ObExecContext& exec_ctx);

  static int type_to_name(const ObCacheObjType type, common::ObIAllocator& allocator, common::ObString& type_name);

  void dump_deleted_log_info(const bool is_debug_log = true) const;
  VIRTUAL_TO_STRING_KV(K_(type), K_(ref_count), K_(tenant_schema_version), K_(sys_schema_version), K_(merged_version),
      K_(object_id), K_(dependency_tables), K_(outline_state), K_(pre_calc_exprs), K_(params_info),
      K_(is_contain_virtual_table), K_(is_contain_inner_table), K_(fetch_cur_time));

protected:
  static int construct_array_params(const ObSqlExpression& expr, common::ObIAllocator& allocator,
      common::ObExprCtx& expr_ctx, common::ObNewRow& row, common::ObObjParam& result);

private:
  int64_t dec_ref_count(const CacheRefHandleID ref_handle);

protected:
  lib::MemoryContext& mem_context_;
  common::ObIAllocator& allocator_;
  ObCacheObjType type_;
  volatile int64_t ref_count_;
  int64_t tenant_schema_version_;
  int64_t sys_schema_version_;
  int64_t merged_version_;
  uint64_t object_id_;
  DependenyTableStore dependency_tables_;
  TableStatVersions table_stat_versions_;
  // for outline use
  ObOutlineState outline_state_;  // TODO:check this need to be
  // store the expressions need to be calculated when plan beginning
  common::ObDList<ObSqlExpression> pre_calc_exprs_;
  common::ObDList<ObPreCalcExprFrameInfo> pre_calc_frames_;
  common::Ob2DArray<ObParamInfo, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false> params_info_;
  bool is_contain_virtual_table_;
  bool is_contain_inner_table_;
  bool fetch_cur_time_;
  bool is_ignore_stmt_;
  stmt::StmtType stmt_type_;
  int64_t log_del_time_;
  bool added_to_pc_;
  uint64_t tenant_id_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_H_ */
