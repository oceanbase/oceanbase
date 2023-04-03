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

#ifndef OCEANBASE_SQL_RESOVER_CMD_RESOURCE_STMT_
#define OCEANBASE_SQL_RESOVER_CMD_RESOURCE_STMT_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{

class ObCreateResourcePoolStmt : public ObDDLStmt
{
public:
  ObCreateResourcePoolStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_RESOURCE_POOL),
      arg_()
  {};
  ObCreateResourcePoolStmt()
    : ObDDLStmt(stmt::T_CREATE_RESOURCE_POOL),
      arg_()
  {};
  virtual ~ObCreateResourcePoolStmt() {};
  virtual int get_cmd_type() const { return get_stmt_type(); }
  void set_resource_pool_name(const common::ObString &name)
  { arg_.pool_name_ = name; }
  void set_unit(const common::ObString &unit)
  { arg_.unit_ = unit; }
  void set_unit_num(const int32_t &unit_num)
  { arg_.unit_num_ = unit_num; }
  int add_zone(const common::ObZone &zone)
  { return arg_.zone_list_.push_back(zone); }
  void set_if_not_exist(const bool if_not_exist)
  { arg_.if_not_exist_ = if_not_exist; }
  obrpc::ObCreateResourcePoolArg &get_arg()
  { return arg_; }
  int fill_delete_unit_id(const uint64_t unit_id)
  { UNUSED(unit_id); return common::OB_NOT_SUPPORTED; }
  void set_replica_type(const ObReplicaType &type) { arg_.replica_type_ = type; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
private:
  obrpc::ObCreateResourcePoolArg arg_;
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObCreateResourcePoolStmt);
};

class ObSplitResourcePoolStmt : public ObDDLStmt
{
public:
  ObSplitResourcePoolStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_SPLIT_RESOURCE_POOL),
      arg_()
  {}
  ObSplitResourcePoolStmt()
    : ObDDLStmt(stmt::T_SPLIT_RESOURCE_POOL),
      arg_()
  {}
  virtual ~ObSplitResourcePoolStmt() {}
  virtual int get_cmd_type() const { return get_stmt_type(); }
  void set_resource_pool_name(const common::ObString &name)
  { arg_.pool_name_ = name; }
  int add_corresponding_zone(const common::ObZone &zone)
  { return arg_.zone_list_.push_back(zone); }
  int add_split_pool(const common::ObString &pool_name)
  { return arg_.split_pool_list_.push_back(pool_name); }
  obrpc::ObSplitResourcePoolArg &get_arg()
  { return arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_;}
private:
  obrpc::ObSplitResourcePoolArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObSplitResourcePoolStmt);
};

class ObAlterResourceTenantStmt : public ObDDLStmt
{
public:
  ObAlterResourceTenantStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_RESOURCE_TENANT),
      arg_()
  {}
  ObAlterResourceTenantStmt()
    : ObDDLStmt(stmt::T_ALTER_RESOURCE_TENANT),
      arg_()
  {}
  virtual ~ObAlterResourceTenantStmt() {}
public:
  virtual int get_cmd_type() const override { return get_stmt_type(); }
  void set_tenant_name(const common::ObString &tenant_name) {
    arg_.set_tenant_name(tenant_name);
  }
  void set_unit_num(const int64_t unit_num) {
    arg_.set_unit_num(unit_num);
  }
  int fill_unit_group_id(const uint64_t unit_group_id) {
    return arg_.fill_unit_group_id(unit_group_id);
  }
  obrpc::ObAlterResourceTenantArg &get_arg() {
    return arg_;
  }
  const obrpc::ObAlterResourceTenantArg &get_arg() const {
    return arg_;
  }
  virtual obrpc::ObDDLArg &get_ddl_arg() override {
    return arg_;
  }
private:
  obrpc::ObAlterResourceTenantArg arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourceTenantStmt);
};

class ObMergeResourcePoolStmt : public ObDDLStmt
{
public:
  ObMergeResourcePoolStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_MERGE_RESOURCE_POOL),
      arg_()
  {}
  ObMergeResourcePoolStmt()
    : ObDDLStmt(stmt::T_MERGE_RESOURCE_POOL),
      arg_()
  {}
  virtual ~ObMergeResourcePoolStmt() {}
  virtual int get_cmd_type() const { return get_stmt_type(); }
  int add_old_pool(const common::ObString &pool_name)//合并前
  { return arg_.old_pool_list_.push_back(pool_name); }
  int add_new_pool(const common::ObString &pool_name)//合并后
  { return arg_.new_pool_list_.push_back(pool_name); }
  obrpc::ObMergeResourcePoolArg &get_arg()
  { return arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_;}
private:
  obrpc::ObMergeResourcePoolArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeResourcePoolStmt);
};

class ObAlterResourcePoolStmt : public ObDDLStmt
{
public:
  ObAlterResourcePoolStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_RESOURCE_POOL),
      arg_()
  {};
  ObAlterResourcePoolStmt()
    : ObDDLStmt(stmt::T_ALTER_RESOURCE_POOL),
      arg_()
  {};
  virtual ~ObAlterResourcePoolStmt() {};
  virtual int get_cmd_type() const { return get_stmt_type(); }
  void set_resource_pool_name(const common::ObString &name)
  { arg_.pool_name_ = name; }
  void set_unit(const common::ObString &unit)
  { arg_.unit_ = unit; }
  void set_unit_num(const int32_t &unit_num)
  { arg_.unit_num_ = unit_num; }
  int add_zone(const common::ObZone &zone)
  { return arg_.zone_list_.push_back(zone); }
  obrpc::ObAlterResourcePoolArg &get_arg()
  { return arg_; }
  int fill_delete_unit_id(const uint64_t unit_id)
  { return arg_.delete_unit_id_array_.push_back(unit_id); }
  void set_replica_type(const ObReplicaType &type) { UNUSED(type); }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
private:
  /* functions */
  /* variables */
  obrpc::ObAlterResourcePoolArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourcePoolStmt);
};

class ObDropResourcePoolStmt : public ObDDLStmt
{
public:
  ObDropResourcePoolStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_RESOURCE_POOL),
      arg_()
  {};
  ObDropResourcePoolStmt()
    : ObDDLStmt(stmt::T_DROP_RESOURCE_POOL),
      arg_()
  {};
  virtual ~ObDropResourcePoolStmt() {};
  virtual int get_cmd_type() const { return get_stmt_type(); }
  void set_if_exist(const bool if_exist) { arg_.if_exist_ = if_exist; }
  void set_resource_pool_name(const common::ObString &name)
  { arg_.pool_name_ = name; }
  obrpc::ObDropResourcePoolArg &get_arg()
  { return arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
private:
  /* functions */
  /* variables */
  obrpc::ObDropResourcePoolArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropResourcePoolStmt);
};

class ObCreateResourceUnitStmt : public ObDDLStmt
{
public:
  ObCreateResourceUnitStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_RESOURCE_UNIT),
      arg_()
  {};
  ObCreateResourceUnitStmt()
    : ObDDLStmt(stmt::T_CREATE_RESOURCE_UNIT),
      arg_()
  {};
  virtual ~ObCreateResourceUnitStmt() {};
  virtual int get_cmd_type() const { return get_stmt_type(); }
  int init(const common::ObString &name, const share::ObUnitResource &ur, const bool if_not_exist)
  {
    return arg_.init(name, ur, if_not_exist);
  }
  const obrpc::ObCreateResourceUnitArg &get_arg() const { return arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
private:
  /* functions */
  /* variables */
  obrpc::ObCreateResourceUnitArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateResourceUnitStmt);
};

class ObAlterResourceUnitStmt : public ObDDLStmt
{
public:
  ObAlterResourceUnitStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_RESOURCE_UNIT),
      arg_()
  {};
  ObAlterResourceUnitStmt()
    : ObDDLStmt(stmt::T_ALTER_RESOURCE_UNIT),
      arg_()
  {};
  virtual ~ObAlterResourceUnitStmt() {};
  virtual int get_cmd_type() const { return get_stmt_type(); }
  int init(const common::ObString &name, const share::ObUnitResource &ur)
  {
    return arg_.init(name, ur);
  }
  obrpc::ObAlterResourceUnitArg &get_arg()
  { return arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
private:
  /* functions */
  /* variables */
  obrpc::ObAlterResourceUnitArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourceUnitStmt);
};

class ObDropResourceUnitStmt : public ObDDLStmt
{
public:
  ObDropResourceUnitStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_RESOURCE_UNIT),
      arg_()
  {}
  ObDropResourceUnitStmt()
    : ObDDLStmt(stmt::T_DROP_RESOURCE_UNIT),
      arg_()
  {}
  virtual ~ObDropResourceUnitStmt() {};
  virtual int get_cmd_type() const { return get_stmt_type(); }
  void set_if_exist(const bool if_exist) { arg_.if_exist_ = if_exist; }
  void set_resource_unit_name(const common::ObString &name)
  { arg_.unit_name_ = name; }
  obrpc::ObDropResourceUnitArg &get_arg()
  { return arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
private:
  /* functions */
  /* variables */
  obrpc::ObDropResourceUnitArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropResourceUnitStmt);
};

}
}
#endif //OCEANBASE_SQL_RESOVER_CMD_RESOURCE_STMT_
//// end of header file

