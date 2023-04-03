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

#ifndef _OB_SHARE_RESOURCE_MANAGER_RES_MGR_PROXY_H_
#define _OB_SHARE_RESOURCE_MANAGER_RES_MGR_PROXY_H_

#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/resource_manager/ob_resource_plan_info.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObString;
class ObObj;
class ObMySQLProxy;
}
namespace share
{
class ObResourceManagerProxy
{
public:
  ObResourceManagerProxy();
  virtual ~ObResourceManagerProxy();
  int create_plan(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObObj &comment);
  // This procedure deletes the specified plan as well as
  // all the plan directives to which it refers.
  int delete_plan(
      uint64_t tenant_id,
      const common::ObString &plan);
  int create_consumer_group(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &consumer_group,
      const common::ObObj &comments,
      int64_t consumer_group_id = -1);
  int create_consumer_group(
      uint64_t tenant_id,
      const common::ObString &consumer_group,
      const common::ObObj &comment);
  int delete_consumer_group(
      uint64_t tenant_id,
      const common::ObString &consumer_group);
  int create_plan_directive(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      const common::ObObj &comment,
      const common::ObObj &mgmt_p1,
      const common::ObObj &utilization_limit,
      const common::ObObj &min_iops,
      const common::ObObj &max_iops,
      const common::ObObj &weight_iops);
  int create_plan_directive(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      const common::ObObj &comment,
      const common::ObObj &mgmt_p1,
      const common::ObObj &utilization_limit,
      const common::ObObj &min_iops,
      const common::ObObj &max_iops,
      const common::ObObj &weight_iops);
  // 这里之所以直接传入 ObObj，而不是传入 ObString 或 int
  // 是为了便于判断传入的参数是否是缺省，如果缺省则 ObObj.is_null() 是 true
  int update_plan_directive(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      const common::ObObj &comment,
      const common::ObObj &mgmt_p1,
      const common::ObObj &utilization_limit,
      const common::ObObj &min_iops,
      const common::ObObj &max_iops,
      const common::ObObj &weight_iops);
  int delete_plan_directive(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group);
  int get_all_plan_directives(
      uint64_t tenant_id,
      const common::ObString &plan,
      common::ObIArray<ObPlanDirective> &directives);

  // process mapping rules
  int replace_mapping_rule(
    uint64_t tenant_id,
    const common::ObString &attribute,
    const common::ObString &value,
    const common::ObString &consumer_group,
    const sql::ObSQLSessionInfo &session);
  int replace_user_mapping_rule(
    common::ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const common::ObString &attribute,
    const common::ObString &value,
    const common::ObString &consumer_group);
  int replace_function_mapping_rule(
    common::ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const common::ObString &attribute,
    const common::ObString &value,
    const common::ObString &consumer_group);
  int replace_column_mapping_rule(
    common::ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const common::ObString &attribute,
    const common::ObString &value,
    const common::ObString &consumer_group,
    const sql::ObSQLSessionInfo &session);
  int update_resource_mapping_version(common::ObMySQLTransaction &trans, uint64_t tenant_id);
  int get_all_resource_mapping_rules(
      uint64_t tenant_id,
      common::ObIArray<ObResourceMappingRule> &rules);
  int get_all_group_info(
    uint64_t tenant_id,
    const common::ObString &plan,
    common::ObIArray<ObResourceUserMappingRule> &rules);
  int get_all_resource_mapping_rules_by_function(
    uint64_t tenant_id,
    const common::ObString &plan,
    common::ObIArray<ObResourceMappingRule> &rules);
  int get_all_resource_mapping_rules_for_plan(
      uint64_t tenant_id,
      const common::ObString &plan,
      common::ObIArray<ObResourceIdNameMappingRule> &rules);
  int get_all_resource_mapping_rules_by_user(
      uint64_t tenant_id,
      const common::ObString &plan,
      common::ObIArray<ObResourceUserMappingRule> &rules);
  int check_if_plan_exist(
      uint64_t tenant_id,
      const common::ObString &plan,
      bool &exist);
  int get_resource_mapping_version(uint64_t tenant_id, int64_t &current_version);
  int get_all_resource_mapping_rules_by_column(
      uint64_t tenant_id,
      const common::ObString &plan,
      ObIAllocator &allocator,
      common::ObIArray<ObResourceColumnMappingRule> &rules);
  int get_next_element(
      const common::ObString &value,
      int64_t &pos,
      char wrapper,
      common::ObString end_chars,
      common::ObString &element,
      bool &is_wrapped);
  void upper_db_table_name(
      const bool need_modify_case,
      const bool is_oracle_mode,
      const common::ObNameCaseMode case_mode,
      const bool is_wrapped,
      common::ObString& name);
  int parse_column_mapping_rule(
      common::ObString &value,
      const sql::ObSQLSessionInfo *session,
      common::ObString &db_name,
      common::ObString &table_name,
      common::ObString &column_name,
      common::ObString &literal_value,
      common::ObString &user_name,
      const common::ObNameCaseMode case_mode);
  int get_iops_config(
      const uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      ObPlanDirective &directive);
  int reset_all_mapping_rules();

private:
  int allocate_consumer_group_id(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      int64_t &group_id);
  int check_if_plan_directive_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      bool &exist);
  int check_if_plan_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &plan,
      bool &exist);
  int check_if_consumer_group_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &group,
      bool &exist);
  int check_if_user_exist(
      uint64_t tenant_id,
      const common::ObString &user_name,
      bool &exist);
  int check_if_function_exist(const common::ObString &function_name, bool &exist);
  int check_if_column_exist(
      uint64_t tenant_id,
      const common::ObString &db_name,
      const common::ObString &table_name,
      const common::ObString &column_name);
  int formalize_column_mapping_value(
      const common::ObString &db_name,
      const common::ObString &table_name,
      const common::ObString &column_name,
      const common::ObString &literal_value,
      const common::ObString &user_name,
      bool is_oracle_mode,
      ObIAllocator &allocator,
      common::ObString &formalized_value);
  int check_if_column_and_user_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      common::ObString &value,
      const sql::ObSQLSessionInfo &session,
      ObIAllocator &allocator,
      bool &exist,
      common::ObString &formalized_value);
  // helper func, 便于集中获取百分比的值，数值范围为 [0, 100]
  int get_percentage(const char *name, const common::ObObj &obj, int64_t &v);
  // max_iops >= min_iops, 否则抛出错误
  int check_iops_validity(
      const uint64_t tenant_id,
      const common::ObString &plan_name,
      const common::ObString &group,
      const int64_t iops_minimum,
      const int64_t iops_maximum,
      bool &valid);

  // get user_info from inner mapping_table
  int get_user_mapping_info(
      const uint64_t tenant_id,
      const common::ObString &user,
      ObResourceUserMappingRule &rule);

public:
  class TransGuard {
  public:
    TransGuard(common::ObMySQLTransaction &trans,
               const uint64_t tenant_id,
               int &ret);
    ~TransGuard();
    // 判断 trans 是否成功初始化
    bool ready();
  private:
    common::ObMySQLTransaction &trans_;
    int &ret_;
  };
private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObResourceManagerProxy);
};
}
}
#endif /* _OB_SHARE_RESOURCE_MANAGER_RES_MGR_PROXY_H_ */
//// end of header file

