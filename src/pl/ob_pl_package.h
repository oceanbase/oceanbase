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

#ifndef SRC_PL_OB_PL_PACKAGE_H_
#define SRC_PL_OB_PL_PACKAGE_H_

#include "pl/ob_pl_package_state.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_package_guard.h"
#include "sql/plan_cache/ob_cache_object_factory.h"

namespace oceanbase
{
using sql::ObExecContext;
namespace pl
{
enum ObPackageType
{
  PL_INVALID_PACKAGE_TYPE = 0,
  PL_PACKAGE_SPEC = 1,
  PL_PACKAGE_BODY = 2,
  PL_UDT_OBJECT_SPEC = 3,
  PL_UDT_OBJECT_BODY = 4,
};

class ObPLPackageBase
{
public:
  ObPLPackageBase() :
    package_type_(PL_INVALID_PACKAGE_TYPE),
    id_(OB_INVALID_ID),
    database_id_(OB_INVALID_ID),
    version_(OB_INVALID_VERSION),
    serially_reusable_(false) {}
  virtual ~ObPLPackageBase() {}

  inline const common::ObString &get_db_name() const { return db_name_; }
  inline const common::ObString &get_name() const { return name_; }
  inline ObPackageType get_package_type() const { return package_type_; }
  inline void set_package_type(ObPackageType type) { package_type_ = type; }
  inline uint64_t get_id() const { return id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline int64_t get_version() const { return version_; }
  inline void set_serially_reusable() { serially_reusable_ = true; }
  inline bool get_serially_reusable() const { return serially_reusable_; }
  inline void set_id(uint64_t id) { id_ = id; }
protected:
  common::ObString db_name_;
  common::ObString name_;
  ObPackageType package_type_;
  uint64_t id_;
  uint64_t database_id_;
  int64_t version_;
  bool serially_reusable_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLPackageBase);
};

class ObPLPackageAST : public ObPLPackageBase, public ObPLCompileUnitAST
{
public:
  ObPLPackageAST(common::ObIAllocator &allocator) :
    ObPLCompileUnitAST(allocator, PACKAGE_TYPE),
    inited_(false) {}
  virtual ~ObPLPackageAST() {};

  int init(const ObString &db_name, const ObString &package_name, ObPackageType package_type,
           uint64_t database_id, uint64_t package_id, int64_t package_version,
           ObPLPackageAST *parent_package_ast);
  int process_generic_type();
  inline bool is_inited() { return inited_; }

  virtual const common::ObString &get_db_name() const { return db_name_; }
  virtual const common::ObString &get_name() const { return name_; }
  virtual uint64_t get_id() const { return id_; }
  virtual uint64_t get_database_id() const { return database_id_; }
  virtual int64_t get_version() const { return version_; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLPackageAST);

  bool inited_;
};

class ObPLVar;
class ObUserDefinedType;
class ObPLPackage : public ObPLPackageBase, public ObPLCompileUnit
{
public:
  ObPLPackage(lib::MemoryContext &mem_context)
    : ObPLCompileUnit(sql::ObLibCacheNameSpace::NS_PKG, mem_context),
      inited_(false),
      var_table_(),
      condition_table_(),
      cursor_table_(allocator_) {}
  virtual ~ObPLPackage();

  int init(const ObPLPackageAST &package_ast);
  inline bool is_inited() { return inited_; }
  inline const ObIArray<ObPLVar *> &get_var_table() const { return var_table_; }
  inline const ObIArray<ObPLCondition *> &get_condition_table() const { return condition_table_; }
  inline const ObPLCursorTable &get_cursor_table() const { return cursor_table_; }
  inline const ObIArray<sql::ObSqlExpression *> &get_default_exprs() const { return get_expressions(); }
  inline ObIArray<sql::ObSqlExpression *> &get_default_exprs() { return get_expressions(); }
  inline sql::ObSqlExpression* get_default_expr(int64_t idx)
  {
    return (idx >= 0 && idx < get_expressions().count()) ? get_expressions().at(idx) : NULL;
  }
  int add_var(ObPLVar *var);
  int get_var(const common::ObString &var_name, const ObPLVar *&var, int64_t &var_idx) const;
  int get_var(int64_t var_idx, const ObPLVar *&var) const;
  int add_condition(ObPLCondition *condition);
  int get_condition(const common::ObString &condition_name, const ObPLCondition *&value);
  int add_type(ObUserDefinedType *type);
  int get_type(const common::ObString type_name, const ObUserDefinedType *&type) const;
  int get_type(uint64_t type_id, const ObUserDefinedType *&type) const;
  int get_cursor(int64_t cursor_idx, const ObPLCursor *&cursor) const;
  int get_cursor(
    int64_t package_id, int64_t routine_id, int64_t index, const ObPLCursor *&cursor) const;
  int get_cursor(const common::ObString &cursor_name,
                 const ObPLCursor *&cursor,
                 int64_t &cursor_idx) const;
  void set_init_routine(ObPLFunction *init_routine)
  {
    routine_table_.at(ObPLRoutineTable::INIT_ROUTINE_IDX) = init_routine;
  }
  inline ObPLFunction *get_init_routine()
  {
    return routine_table_.empty()?NULL:routine_table_.at(ObPLRoutineTable::INIT_ROUTINE_IDX);
  }
  int deep_copy(const ObPLPackage &package, common::ObIAllocator &alloc);
  int instantiate_package_state(const ObPLResolveCtx &resolve_ctx,
                                ObExecContext &exec_ctx,
                                ObPLPackageState &package_state);
  int execute_init_routine(ObIAllocator &allocator, ObExecContext &exec_ctx);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLPackage);

  bool inited_;
  common::ObArray<ObPLVar *> var_table_;
  common::ObArray<ObPLCondition *> condition_table_;
  ObPLCursorTable cursor_table_;
};

}
}
#endif /* SRC_PL_OB_PL_PACKAGE_H_ */
