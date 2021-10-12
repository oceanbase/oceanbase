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

#ifndef OCEANBASE_ROOTSERVER_OB_CONSTRAINT_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_CONSTRAINT_CHECKER_H_

#include "share/ob_define.h"

namespace oceanbase {
namespace rootserver {
class ObRootService;

enum ConstraintType { 
  FOREIGN_KEY = 0
};

struct ConstraintCreatingInfo{
public:
  int64_t tenant_id_;
  int64_t constraint_id_;
  int64_t table_id_;
  common::ObString ip_;
  int64_t port_;
  int64_t creating_time_;
  TO_STRING_KV(K_(tenant_id), K_(constraint_id), K_(table_id), K_(ip), K_(port), K_(creating_time));
};

class ObConstraintChecker {
public:
  ObConstraintChecker();
  virtual ~ObConstraintChecker();
  int init(ObRootService& root_service);
  int check();
  
  inline bool is_inited() const 
  {
    return inited_;
  }

private:
  int process_foreign_key();
  int acquire_foreign_key_name(const uint64_t tenant_id, const uint64_t fk_id,
    const uint64_t child_table_id, common::ObString& foreign_key_name);
  int check_fk_valid(ConstraintCreatingInfo& curr_creating_fk, bool& invalid);
  int rollback_foreign_key(const uint64_t tenant_id, const uint64_t fk_id, const uint64_t table_id);

  bool inited_;
  ObDDLService* ddl_service_;
  ObServerManager* server_manager_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_CONSTRAINT_CHECKER_H_
