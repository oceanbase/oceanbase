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

#ifndef OCEANBASE_SHARE_SYSTEM_VARIABLE_OB_SYSTEM_VARIABLE_INIT_
#define OCEANBASE_SHARE_SYSTEM_VARIABLE_OB_SYSTEM_VARIABLE_INIT_
#include "share/system_variable/ob_sys_var_class_type.h"
#include <stdint.h>
#include "common/object/ob_object.h"
namespace oceanbase
{
namespace share
{
// ObSysVarFlag的值不可随意增删改, 有任何增删改要同时同步到sql/session/gen_ob_sys_variables.py的flag_value_dict变量中
struct ObSysVarFlag
{
  const static int64_t NONE = 0LL;
  const static int64_t GLOBAL_SCOPE = 1LL;
  const static int64_t SESSION_SCOPE = (1LL << 1);
  const static int64_t READONLY = (1LL << 2);
  const static int64_t SESSION_READONLY = (1LL << 3);
  const static int64_t INVISIBLE = (1LL << 4);
  const static int64_t NULLABLE = (1LL << 5);
  const static int64_t INFLUENCE_PLAN = (1LL << 6);
  const static int64_t NEED_SERIALIZE = (1LL << 7);
  const static int64_t QUERY_SENSITIVE = (1LL << 8);
  const static int64_t ORACLE_ONLY = (1LL << 9);
  const static int64_t WITH_CREATE = (1LL << 10);
  const static int64_t WITH_UPGRADE = (1LL << 11);
  const static int64_t MYSQL_ONLY = (1LL << 12);
};
struct ObSysVarFromJson{
  ObSysVarClassType id_;
  common::ObString name_;
  common::ObObjType data_type_;
  common::ObString default_value_; // used for init tenant
  common::ObString base_value_; // used for session sync
  common::ObString min_val_;
  common::ObString max_val_;
  common::ObString enum_names_;
  common::ObString info_;
  int64_t flags_;
  common::ObString alias_;
  common::ObString base_class_;
  common::ObString on_check_and_convert_func_;
  common::ObString on_update_func_;
  common::ObString to_select_obj_func_;
  common::ObString to_show_str_func_;
  common::ObString get_meta_type_func_;
  common::ObString session_special_update_func_;

  ObSysVarFromJson():id_(SYS_VAR_INVALID), name_(""), data_type_(common::ObNullType), default_value_(""), base_value_(""), min_val_(""), max_val_(""), enum_names_(""), info_(""), flags_(ObSysVarFlag::NONE), alias_(""), base_class_(""), on_check_and_convert_func_(), on_update_func_(), to_select_obj_func_(), to_show_str_func_(), get_meta_type_func_(), session_special_update_func_() {}
};

class ObSysVariables
{
public:
  static int64_t get_all_sys_var_count();
  static ObSysVarClassType get_sys_var_id(int64_t i);
  static common::ObString get_name(int64_t i);
  static common::ObObjType get_type(int64_t i);
  static common::ObString get_value(int64_t i);
  static common::ObString get_base_str_value(int64_t i);
  static common::ObString get_min(int64_t i);
  static common::ObString get_max(int64_t i);
  static common::ObString get_info(int64_t i);
  static int64_t get_flags(int64_t i);
  static bool need_serialize(int64_t i);
  static bool is_oracle_only(int64_t i);
  static bool is_mysql_only(int64_t i);
  static common::ObString get_alias(int64_t i);
  static const common::ObObj &get_default_value(int64_t i);
  static const common::ObObj &get_base_value(int64_t i);
  static int64_t get_amount();
  static int set_value(const char *name, const char * new_value);
  static int set_value(const common::ObString &name, const common::ObString &new_value);
  static int set_base_value(const char *name, const char * new_value);
  static int set_base_value(const common::ObString &name, const common::ObString &new_value);
  static int init_default_values();
};

class ObSysVarsToIdxMap
{
public:
  static int64_t get_store_idx(int64_t var_id);
  static bool has_invalid_sys_var_id();
};

} // end namespace share
} // end namespace oceanbase

#endif /* OCEANBASE_SHARE_SYSTEM_VARIABLE_OB_SYSTEM_VARIABLE_INIT_ */
