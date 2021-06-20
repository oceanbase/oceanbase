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

#ifndef _OB_STMT_RESOLVER_H
#define _OB_STMT_RESOLVER_H 1
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log.h"
#include "share/schema/ob_column_schema.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/session/ob_basic_session_info.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace share {}
namespace sql {

class ObSynonymChecker;
/// base class of all statement resolver
class ObStmtResolver {
public:
  explicit ObStmtResolver(ObResolverParams& params)
      : allocator_(params.allocator_),
        schema_checker_(params.schema_checker_),
        session_info_(params.session_info_),
        params_(params),
        stmt_(NULL)
  {}
  virtual ~ObStmtResolver()
  {}

  virtual int resolve(const ParseNode& parse_tree) = 0;
  inline ObStmt* get_basic_stmt()
  {
    return stmt_;
  }
  int resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& database_id,
      common::ObString& table_name, common::ObString& synonym_name, common::ObString& db_name);
  int resolve_table_relation_factor(const ParseNode* node, uint64_t& database_id, common::ObString& table_name,
      common::ObString& synonym_name, common::ObString& db_name);
  /// @param org If org is true, means get original db name.
  /// Else, when db node is NULL, get session db name.
  int resolve_table_relation_node_v2(const ParseNode* node, common::ObString& table_name, common::ObString& db_name,
      bool& is_db_explicit, bool org = false, bool is_oracle_sys_view = false);

  int resolve_table_relation_node(const ParseNode* node, common::ObString& table_name, common::ObString& db_name,
      bool org = false, bool is_oracle_sys_view = false);
  /**
   * @brief  resolve a T_REF_FACTOR node, get database name and table name
   * @param [in] node  - parser node
   * @param [in] session_info  - session info
   * @param [out] table_name  - table name
   * @param [out] db_name  - database name
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   *
   */
  static int resolve_ref_factor(
      const ParseNode* node, ObSQLSessionInfo* session_info, common::ObString& table_name, common::ObString& db_name);
  int resolve_database_factor(
      const ParseNode* node, uint64_t tenant_id, uint64_t& database_id, common::ObString& db_name);

  virtual bool is_select_resolver() const
  {
    return false;
  }
  inline uint64_t generate_query_id()
  {
    return params_.new_gen_qid_++;
  }
  inline uint64_t generate_column_id()
  {
    return params_.new_gen_cid_--;
  }
  uint64_t generate_table_id();
  uint64_t generate_link_table_id();
  inline int64_t generate_when_number()
  {
    return params_.new_gen_wid_++;
  }
  inline uint64_t generate_database_id()
  {
    return params_.new_gen_did_--;
  }
  inline uint64_t generate_range_column_id()
  {
    uint64_t ret_cid = params_.new_gen_cid_;
    params_.new_gen_cid_ -= 10;
    return ret_cid;
  }
  inline uint64_t generate_cte_table_id()
  {
    return params_.new_cte_tid_++;
  }
  inline uint64_t generate_cte_column_base_id()
  {
    return common::OB_MIN_CTE_COLUMN_ID;
  }
  template <class T>
  T* create_stmt()
  {
    int ret = common::OB_SUCCESS;
    T* stmt = NULL;
    if (OB_ISNULL(params_.stmt_factory_)) {
      SQL_RESV_LOG(ERROR, "stmt_factory_ is null, not be init");
    } else if (OB_FAIL(params_.stmt_factory_->create_stmt(stmt))) {
      SQL_RESV_LOG(WARN, "create stmt failed", K(ret));
    } else if (OB_ISNULL(stmt)) {
      SQL_RESV_LOG(WARN, "create stmt success, but stmt is null");
    } else {
      stmt_ = stmt;
      stmt_->set_query_ctx(params_.query_ctx_);
      stmt_->set_stmt_id();
      // mark prepare stmt
      stmt_->set_is_prepare_stmt(params_.is_prepare_protocol_);
      stmt_->tz_info_ = get_timezone_info(params_.session_info_);
      stmt_->sql_stmt_coll_type_ = get_obj_print_params(params_.session_info_).cs_type_;
      if (OB_FAIL(init_stmt())) {
        stmt = NULL;
        SQL_RESV_LOG(ERROR, "init stmt failed", K(ret));
      }
    }
    return stmt;
  }

  // wrap ObSchemaChecker::get_column_schema with inner sql get hidden column logic.
  int get_column_schema(const uint64_t table_id, const common::ObString& column_name,
      const share::schema::ObColumnSchemaV2*& column_schema, const bool get_hidden = false);
  // wrap ObSchemaChecker::get_column_schema with inner sql get hidden column logic.
  int get_column_schema(const uint64_t table_id, const uint64_t column_id,
      const share::schema::ObColumnSchemaV2*& column_schema, const bool get_hidden = false);

protected:
  int normalize_table_or_database_names(common::ObString& name);
  virtual int init_stmt()
  {
    return common::OB_SUCCESS;
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObStmtResolver);

public:
  // data members
  common::ObIAllocator* allocator_;
  ObSchemaChecker* schema_checker_;
  ObSQLSessionInfo* session_info_;
  ObResolverParams& params_;

protected:
  ObStmt* stmt_;
};

class ObSynonymChecker {
public:
  ObSynonymChecker() : has_synonym_(false), synonym_ids_()
  {}
  ~ObSynonymChecker()
  {}
  int add_synonym_id(uint64_t synonym_id);
  const common::ObIArray<uint64_t>& get_synonym_ids() const
  {
    return synonym_ids_;
  }
  void set_synonym(bool has_synonym)
  {
    has_synonym_ = has_synonym;
  }
  bool has_synonym() const
  {
    return has_synonym_;
  }

private:
  bool has_synonym_;
  common::ObSEArray<uint64_t, 2> synonym_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObSynonymChecker);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_STMT_RESOLVER_H */
