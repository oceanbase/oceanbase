#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import stat
import shutil
import sys

VARCHAR = "varchar"
INT = "int"
UINT = "uint"
TINY = "tinyint"
BOOL = "bool"
NUMERIC  = "numeric"
ENUM = "enum"

# `ob_max_sys_var_id`表示当前版本预分配的可以使用的system var id的最大值，
# 系统会根据ob_max_sys_var_id的值预分配sys var id -> sys var的映射数组，不能将其随意调大，
# 如果需要分配的系统变量的sys var id必须大于`ob_max_sys_var_id`，需要将`ob_max_sys_var_id`调大
ob_max_sys_var_id = 20000

flag_dict = {}
flag_dict["GLOBAL"] = "GLOBAL_SCOPE"
flag_dict["SESSION"] = "SESSION_SCOPE"
flag_dict["READONLY"] = "READONLY"
flag_dict["READ"] = "READONLY"
flag_dict["SESSION_READONLY"] = "SESSION_READONLY"
flag_dict["INVISIBLE"] = "INVISIBLE"
flag_dict["NULL"] = "NULLABLE"
flag_dict["NULLABLE"] = "NULLABLE"
flag_dict["INFLUENCE_PLAN"] = "INFLUENCE_PLAN"
flag_dict["NEED_SERIALIZE"] = "NEED_SERIALIZE"
flag_dict["QUERY_SENSITIVE"] = "QUERY_SENSITIVE"
flag_dict["ORACLE_ONLY"] = "ORACLE_ONLY"
flag_dict["WITH_CREATE"] = "WITH_CREATE"
flag_dict["WITH_UPGRADE"] = "WITH_UPGRADE"
flag_dict["MYSQL_ONLY"] = "MYSQL_ONLY"

flag_value_dict = {}
flag_value_dict["GLOBAL"] = 1L
flag_value_dict["SESSION"] = (1L << 1)
flag_value_dict["READONLY"] = (1L << 2)
flag_value_dict["SESSION_READONLY"] = (1L << 3)
flag_value_dict["INVISIBLE"] = (1L << 4)
flag_value_dict["NULL"] = (1L << 5)
flag_value_dict["NULLABLE"] = (1L << 5)
flag_value_dict["INFLUENCE_PLAN"] = (1L << 6)
flag_value_dict["NEED_SERIALIZE"] = (1L << 7)
flag_value_dict["QUERY_SENSITIVE"] = (1L << 8)
flag_value_dict["ORACLE_ONLY"] = (1L << 9)
flag_value_dict["WITH_CREATE"] = (1L << 10)
flag_value_dict["WITH_UPGRADE"] = (1L << 11)
flag_value_dict["MYSQL_ONLY"] = (1L << 12)

type_dict = {}
type_dict["tinyint"] = "ObTinyIntType"
type_dict["int"] = "ObIntType"
type_dict["uint"] = "ObUInt64Type"
type_dict["numeric"] = "ObNumberType"
type_dict["varchar"] = "ObVarcharType"
type_dict["bool"] = "ObIntType" # FIXME: tinyint?
type_dict["enum"] = "ObIntType"

# 这个映射是建立在ObObjType的值不改变的前提的，如果ObObjType的值改变，这里也要跟着改
type_value_dict = {}
type_value_dict["tinyint"] = 1
type_value_dict["int"] = 5
type_value_dict["uint"] = 10
type_value_dict["numeric"] = 15
type_value_dict["varchar"] = 22
type_value_dict["bool"] = 5 # FIXME: tinyint?
type_value_dict["enum"] = 5

required_attrs = ["publish_version", "info_cn", "background_cn", "ref_url"]
ignored_attrs = ["publish_version", "info_cn", "background_cn", "ref_url", "placeholder"]

file_head_annotation = """/**
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

"""

def parse_json(json_file_name):
  json_file = open(json_file_name,'r')
  all_the_vars = json_file.read( )
  json_Dict = json.loads(all_the_vars)
  # add new attribute placeholder
  # If placeholder is false, it means it is not a placeholder variable.
  # If it is true, it means it is a placeholder variable.
  filtered_json = filter(lambda d: 'placeholder' not in d[1] or d[1]['placeholder'] is False, json_Dict.iteritems())
  filtered_dict = dict(filtered_json)
  list_sorted_by_name= sorted(filtered_dict.iteritems(), key=lambda d:d[0])
  list_sorted_by_id= sorted(filtered_dict.iteritems(), key=lambda d:d[1]['id'])
  json_file.close()
  return (json_Dict, list_sorted_by_name, list_sorted_by_id)

def make_alias_file(pdir, alias_file_name, sorted_list):
  alias_file =open(alias_file_name,'w')
  alias_file.write(file_head_annotation);
  suffix_idx = alias_file_name.find(".h")
  file_def_str = "OCEANBASE_" + pdir.replace("/", "_").upper() + "_" + alias_file_name[0 : suffix_idx].upper() + "_"
  alias_file.write("#ifndef " + file_def_str + "\n");
  alias_file.write("#define " + file_def_str + "\n");
  alias_file.write("namespace oceanbase\n");
  alias_file.write("{\n");
  alias_file.write("namespace share\n");
  alias_file.write("{\n");
  for (name,attributes) in sorted_list:
    alias = attributes['name']
    alias_prefix = alias[0:3]
    if alias_prefix == str("ob_"):
      alias = alias[3:]
    alias_upper = alias.upper()
    out = "  static const char* const OB_SV_" + str(alias_upper) + " = \"" + str(name) + "\";\n"
    alias_file.write(out)

  alias_file.write("\n");
  alias_file.write("}\n");
  alias_file.write("}\n");
  alias_file.write("#endif //OCEANBASE_COMMON_OB_SYSTEM_VARIABLE_ALIAS_H_\n");
  alias_file.write("\n");

  alias_file.close()
  print("Generate " + str(alias_file_name) + " successfully!\n");
  return

def make_head_file(pdir, head_file_name, sorted_list):
  head_file = open(head_file_name,'w')
  head_file.write(file_head_annotation);
  suffix_idx = head_file_name.find(".h")
  file_def_str = "OCEANBASE_" + pdir.replace("/", "_").upper() + "_" + head_file_name[0 : suffix_idx].upper() + "_"
  head_file.write("#ifndef " + file_def_str + "\n");
  head_file.write("#define " + file_def_str + "\n");
  head_file.write("#include \"share/system_variable/ob_sys_var_class_type.h\"\n");
  head_file.write("#include <stdint.h>\n");
  head_file.write("#include \"common/object/ob_object.h\"\n");
  head_file.write("namespace oceanbase\n");
  head_file.write("{\n");
  head_file.write("namespace share\n");
  head_file.write("{\n");
  head_file.write("// ObSysVarFlag的值不可随意增删改, 有任何增删改要同时同步到sql/session/gen_ob_sys_variables.py的flag_value_dict变量中\n");
  head_file.write("struct ObSysVarFlag\n");
  head_file.write("{\n");
  head_file.write("  const static int64_t NONE = 0LL;\n");
  head_file.write("  const static int64_t GLOBAL_SCOPE = 1LL;\n");
  head_file.write("  const static int64_t SESSION_SCOPE = (1LL << 1);\n");
  head_file.write("  const static int64_t READONLY = (1LL << 2);\n");
  head_file.write("  const static int64_t SESSION_READONLY = (1LL << 3);\n");
  head_file.write("  const static int64_t INVISIBLE = (1LL << 4);\n");
  head_file.write("  const static int64_t NULLABLE = (1LL << 5);\n");
  head_file.write("  const static int64_t INFLUENCE_PLAN = (1LL << 6);\n");
  head_file.write("  const static int64_t NEED_SERIALIZE = (1LL << 7);\n");
  head_file.write("  const static int64_t QUERY_SENSITIVE = (1LL << 8);\n");
  head_file.write("  const static int64_t ORACLE_ONLY = (1LL << 9);\n");
  head_file.write("  const static int64_t WITH_CREATE = (1LL << 10);\n");
  head_file.write("  const static int64_t WITH_UPGRADE = (1LL << 11);\n");
  head_file.write("  const static int64_t MYSQL_ONLY = (1LL << 12);\n");
  head_file.write("};\n");
  head_file.write("struct ObSysVarFromJson{\n");
  head_file.write("  ObSysVarClassType id_;\n");
  head_file.write("  common::ObString name_;\n");
  head_file.write("  common::ObObjType data_type_;\n");
  head_file.write("  common::ObString default_value_; // used for init tenant\n");
  head_file.write("  common::ObString base_value_; // used for session sync\n");
  head_file.write("  common::ObString min_val_;\n");
  head_file.write("  common::ObString max_val_;\n");
  head_file.write("  common::ObString enum_names_;\n");
  head_file.write("  common::ObString info_;\n");
  head_file.write("  int64_t flags_;\n");
  head_file.write("  common::ObString alias_;\n");
  head_file.write("  common::ObString base_class_;\n");
  head_file.write("  common::ObString on_check_and_convert_func_;\n");
  head_file.write("  common::ObString on_update_func_;\n");
  head_file.write("  common::ObString to_select_obj_func_;\n");
  head_file.write("  common::ObString to_show_str_func_;\n");
  head_file.write("  common::ObString get_meta_type_func_;\n");
  head_file.write("  common::ObString session_special_update_func_;\n");
  head_file.write("\n");
  head_file.write("  ObSysVarFromJson():id_(SYS_VAR_INVALID), name_(\"\"), data_type_(common::ObNullType), default_value_(\"\"), base_value_(\"\"), min_val_(\"\"), max_val_(\"\"), enum_names_(\"\"), info_(\"\"), flags_(ObSysVarFlag::NONE), alias_(\"\"), base_class_(\"\"), on_check_and_convert_func_(""), on_update_func_(""), to_select_obj_func_(""), to_show_str_func_(""), get_meta_type_func_(""), session_special_update_func_("") {}\n");
  head_file.write("};\n");
  head_file.write("\n");
  head_file.write("class ObSysVariables\n");
  head_file.write("{\n");
  head_file.write("public:\n");
  head_file.write("  static int64_t get_all_sys_var_count();\n");
  head_file.write("  static ObSysVarClassType get_sys_var_id(int64_t i);\n");
  head_file.write("  static common::ObString get_name(int64_t i);\n");
  head_file.write("  static common::ObObjType get_type(int64_t i);\n");
  head_file.write("  static common::ObString get_value(int64_t i);\n");
  head_file.write("  static common::ObString get_base_str_value(int64_t i);\n");
  head_file.write("  static common::ObString get_min(int64_t i);\n");
  head_file.write("  static common::ObString get_max(int64_t i);\n");
  head_file.write("  static common::ObString get_info(int64_t i);\n");
  head_file.write("  static int64_t get_flags(int64_t i);\n");
  head_file.write("  static bool need_serialize(int64_t i);\n");
  head_file.write("  static bool is_oracle_only(int64_t i);\n");
  head_file.write("  static bool is_mysql_only(int64_t i);\n");
  head_file.write("  static common::ObString get_alias(int64_t i);\n");
  head_file.write("  static const common::ObObj &get_default_value(int64_t i);\n")
  head_file.write("  static const common::ObObj &get_base_value(int64_t i);\n")
  head_file.write("  static int64_t get_amount();\n");
  head_file.write("  static ObCollationType get_default_sysvar_collation();\n");
  head_file.write("  static int set_value(const char *name, const char * new_value);\n");
  head_file.write("  static int set_value(const common::ObString &name, const common::ObString &new_value);\n");
  head_file.write("  static int set_base_value(const char *name, const char * new_value);\n");
  head_file.write("  static int set_base_value(const common::ObString &name, const common::ObString &new_value);\n");
  head_file.write("  static int init_default_values();\n");
  head_file.write("};\n");
  head_file.write("\n");
  head_file.write("class ObSysVarsToIdxMap\n");
  head_file.write("{\n");
  head_file.write("public:\n");
  head_file.write("  static int64_t get_store_idx(int64_t var_id);\n");
  head_file.write("  static bool has_invalid_sys_var_id();\n");
  head_file.write("};\n");

  head_file.write("\n");
  head_file.write("} // end namespace share\n");
  head_file.write("} // end namespace oceanbase\n");
  head_file.write("\n");
  head_file.write("#endif /* " + file_def_str + " */\n");

  head_file.close( )
  print("Generate " + str(head_file_name) + " successfully!\n");
  return

def make_flags_value(flags):
  value = ""
  flags = flags.split("|")
  for i in range(0, len(flags)):
    value += ("ObSysVarFlag::" + flag_dict[flags[i].strip().upper()])
    if (i != len(flags) - 1):
      value += " | "
  return value

def make_type_value(type):
  return type_dict[type.strip()]

def make_cpp_file(pdir, cpp_file_name, sorted_list):
  cpp_file =  open(cpp_file_name,'w')
  cpp_file.write(file_head_annotation)
  cpp_file.write("#define USING_LOG_PREFIX SHARE\n")
  cpp_file.write("#include \"" + pdir + "/" + cpp_file_name.replace(".cpp", ".h") + "\"\n")
  cpp_file.write("#include \"share/system_variable/ob_system_variable_factory.h\"\n")
  cpp_file.write("#include \"share/object/ob_obj_cast.h\"\n")
  cpp_file.write("#include \"common/expression/ob_expr_string_buf.h\"\n")
  cpp_file.write("#include \"common/expression/ob_expr_string_buf.h\"\n")
  cpp_file.write("using namespace oceanbase::common;\n");
  cpp_file.write("\n")
  cpp_file.write("namespace oceanbase\n");
  cpp_file.write("{\n");
  cpp_file.write("namespace share\n");
  cpp_file.write("{\n");
  cpp_file.write("static ObSysVarFromJson ObSysVars[ObSysVarFactory::ALL_SYS_VARS_COUNT];\n")
  cpp_file.write("static ObObj ObSysVarDefaultValues[ObSysVarFactory::ALL_SYS_VARS_COUNT];\n")
  cpp_file.write("static ObArenaAllocator ObSysVarAllocator(ObModIds::OB_COMMON_SYS_VAR_DEFAULT_VALUE);\n")
  cpp_file.write("static ObObj ObSysVarBaseValues[ObSysVarFactory::ALL_SYS_VARS_COUNT];\n")
  cpp_file.write("static ObArenaAllocator ObBaseSysVarAllocator(ObModIds::OB_COMMON_SYS_VAR_DEFAULT_VALUE);\n")
  cpp_file.write("static int64_t ObSysVarsIdToArrayIdx[ObSysVarFactory::OB_MAX_SYS_VAR_ID];\n")
  cpp_file.write("// VarsInit中需要判断当前最大的SysVars对应的id，是否大于OB_MAX_SYS_VAR_ID\n")
  cpp_file.write("// 如果大于OB_MAX_SYS_VAR_ID表示存在无效的SysVarsId\n")
  cpp_file.write("static bool HasInvalidSysVar = false;\n")

  cpp_file.write("\n")
  cpp_file.write("static struct VarsInit{\n")
  cpp_file.write("  VarsInit(){\n")

  var_num = 0
  cpp_file.write("    // 保存当前系统变量的最大的id\n")
  cpp_file.write("    int64_t cur_max_var_id = 0;\n")
  cpp_file.write("    // ObSysVarsIdToArrayIdx数组默认初始值为-1，-1表示无效索引\n")
  cpp_file.write("    memset(ObSysVarsIdToArrayIdx, -1, sizeof(ObSysVarsIdToArrayIdx));\n")
  for (name,attributes) in sorted_list:
    cpp_file.write("    [&] (){\n")
    for required_attr in required_attrs:
      if required_attr not in attributes:
        sys.exit("attribute '" + str(required_attr) + "' is required for item '" + str(name) + "'")
    for (attribute_key, attribute_value) in attributes.items():
      if attribute_value == " ":
        attribute_value = str("")
      if attribute_key in ignored_attrs:
        continue
      elif attribute_key == "id":
        out = "      ObSysVars[" + str(var_num) + "]." + str(attribute_key) + "_ = SYS_VAR_" + str(name).upper() + " ;\n"
      elif attribute_key == "flags":
        out = "      ObSysVars[" + str(var_num) + "]." + str(attribute_key) + "_ = " + make_flags_value(attribute_value) + " ;\n"
      elif attribute_key == "data_type":
        out = "      ObSysVars[" + str(var_num) + "]." + str(attribute_key) + "_ = " + make_type_value(attribute_value) + " ;\n"
      else:
        out = "      ObSysVars[" + str(var_num) + "]." + str(attribute_key) + "_ = \"" + str(attribute_value) + "\" ;\n"
      cpp_file.write(out)
      if attribute_key == "id":
        out = "      cur_max_var_id = MAX(cur_max_var_id, "+"static_cast<int64_t>(SYS_VAR_" + str(name).upper()+")"+") ;\n"
        cpp_file.write(out)
        out = "      ObSysVarsIdToArrayIdx[" + "SYS_VAR_" + str(name).upper() + "] = " + str(var_num) + " ;\n"
        cpp_file.write(out)
    alias = attributes['name']
    alias_prefix = alias[0:3]
    if alias_prefix == str("ob_"):
      alias = alias[3:]
    alias_upper = alias.upper()
    out = "    ObSysVars[" + str(var_num) + "].alias_ = \"OB_SV_" + str(alias_upper) + "\" ;\n"
    cpp_file.write(out)
    var_num += 1
    cpp_file.write("    }();\n\n")

  cpp_file.write("    if (cur_max_var_id >= ObSysVarFactory::OB_MAX_SYS_VAR_ID) { \n")
  cpp_file.write("      HasInvalidSysVar = true;\n")
  cpp_file.write("    }\n")
  cpp_file.write("  }\n")
  cpp_file.write("}vars_init;\n")
  cpp_file.write("\n")
  cpp_file.write("static int64_t var_amount = " + str(var_num) + ";"  + "\n")
  cpp_file.write("\n")
  cpp_file.write("int64_t ObSysVariables::get_all_sys_var_count(){ return ObSysVarFactory::ALL_SYS_VARS_COUNT;}\n")
  cpp_file.write("ObSysVarClassType ObSysVariables::get_sys_var_id(int64_t i){ return ObSysVars[i].id_;}\n")
  cpp_file.write("ObString ObSysVariables::get_name(int64_t i){ return ObSysVars[i].name_;}\n")
  cpp_file.write("ObObjType ObSysVariables::get_type(int64_t i){ return ObSysVars[i].data_type_;}\n")
  cpp_file.write("ObString ObSysVariables::get_value(int64_t i){ return ObSysVars[i].default_value_;}\n")
  cpp_file.write("ObString ObSysVariables::get_base_str_value(int64_t i){ return ObSysVars[i].base_value_;}\n")
  cpp_file.write("ObString ObSysVariables::get_min(int64_t i){ return ObSysVars[i].min_val_;}\n")
  cpp_file.write("ObString ObSysVariables::get_max(int64_t i){ return ObSysVars[i].max_val_;}\n")
  cpp_file.write("ObString ObSysVariables::get_info(int64_t i){ return ObSysVars[i].info_;}\n")
  cpp_file.write("int64_t ObSysVariables::get_flags(int64_t i){ return ObSysVars[i].flags_;}\n")
  cpp_file.write("bool ObSysVariables::need_serialize(int64_t i){ return ObSysVars[i].flags_ & ObSysVarFlag::NEED_SERIALIZE;}\n")
  cpp_file.write("bool ObSysVariables::is_oracle_only(int64_t i){ return ObSysVars[i].flags_ & ObSysVarFlag::ORACLE_ONLY;}\n")
  cpp_file.write("bool ObSysVariables::is_mysql_only(int64_t i){ return ObSysVars[i].flags_ & ObSysVarFlag::MYSQL_ONLY;}\n")
  cpp_file.write("ObString ObSysVariables::get_alias(int64_t i){ return ObSysVars[i].alias_;}\n")
  cpp_file.write("const ObObj &ObSysVariables::get_default_value(int64_t i){ return ObSysVarDefaultValues[i];}\n")
  cpp_file.write("const ObObj &ObSysVariables::get_base_value(int64_t i){ return ObSysVarBaseValues[i];}\n")
  cpp_file.write("int64_t ObSysVariables::get_amount(){ return var_amount;}\n")
  cpp_file.write("ObCollationType ObSysVariables::get_default_sysvar_collation() { return CS_TYPE_UTF8MB4_GENERAL_CI;}\n")
  cpp_file.write("\n")
  cpp_file.write("int ObSysVariables::set_value(const char *name, const char * new_value)\n")
  cpp_file.write("{\n")
  cpp_file.write("  ObString tmp_name(static_cast<int32_t>(strlen(name)), name);\n")
  cpp_file.write("  ObString tmp_value(static_cast<int32_t>(strlen(new_value)), new_value);\n")
  cpp_file.write("  return set_value(tmp_name, tmp_value);\n")
  cpp_file.write("}\n")
  cpp_file.write("int ObSysVariables::set_value(const common::ObString &name, const common::ObString &new_value)\n")
  cpp_file.write("{\n")
  cpp_file.write("  int ret = OB_SUCCESS;\n")
  cpp_file.write("  bool name_exist = false;\n")
  cpp_file.write("  for (int64_t i = 0; OB_SUCC(ret) && false == name_exist && i < var_amount; ++i){\n")
  cpp_file.write("    if (0 == ObSysVars[i].name_.compare(name)) {\n")
  cpp_file.write("      ObSysVars[i].default_value_.assign_ptr(new_value.ptr(), new_value.length());\n")
  cpp_file.write("      name_exist = true;\n")
  cpp_file.write("    }\n")
  cpp_file.write("  }\n")
  cpp_file.write("  if (OB_SUCC(ret)) {\n")
  cpp_file.write("    if (false == name_exist) {\n")
  cpp_file.write("      ret = OB_ENTRY_NOT_EXIST;\n")
  cpp_file.write("    }\n")
  cpp_file.write("  }\n")
  cpp_file.write("  return ret;\n")
  cpp_file.write("}\n")
  cpp_file.write("\n")
  cpp_file.write("int ObSysVariables::set_base_value(const char *name, const char * new_value)\n")
  cpp_file.write("{\n")
  cpp_file.write("  ObString tmp_name(static_cast<int32_t>(strlen(name)), name);\n")
  cpp_file.write("  ObString tmp_value(static_cast<int32_t>(strlen(new_value)), new_value);\n")
  cpp_file.write("  return set_base_value(tmp_name, tmp_value);\n")
  cpp_file.write("}\n")
  cpp_file.write("int ObSysVariables::set_base_value(const common::ObString &name, const common::ObString &new_value)\n")
  cpp_file.write("{\n")
  cpp_file.write("  int ret = OB_SUCCESS;\n")
  cpp_file.write("  bool name_exist = false;\n")
  cpp_file.write("  for (int64_t i = 0; OB_SUCC(ret) && false == name_exist && i < var_amount; ++i){\n")
  cpp_file.write("    if (0 == ObSysVars[i].name_.compare(name)) {\n")
  cpp_file.write("      ObSysVars[i].base_value_.assign_ptr(new_value.ptr(), new_value.length());\n")
  cpp_file.write("      name_exist = true;\n")
  cpp_file.write("    }\n")
  cpp_file.write("  }\n")
  cpp_file.write("  if (OB_SUCC(ret)) {\n")
  cpp_file.write("    if (false == name_exist) {\n")
  cpp_file.write("      ret = OB_ENTRY_NOT_EXIST;\n")
  cpp_file.write("    }\n")
  cpp_file.write("  }\n")
  cpp_file.write("  return ret;\n")
  cpp_file.write("}\n")
  cpp_file.write("""
int ObSysVariables::init_default_values()
{
  int ret = OB_SUCCESS;
  int64_t sys_var_count = get_amount();
  for (int64_t i = 0; OB_SUCC(ret) && i < sys_var_count; ++i) {
    const ObString &sys_var_val_str = ObSysVariables::get_value(i);
    const ObString &base_sys_var_val_str = ObSysVariables::get_base_str_value(i);
    const ObObjType sys_var_type = ObSysVariables::get_type(i);
    if (OB_UNLIKELY(sys_var_type == ObTimestampType)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("need tz_info when cast to timestamp", K(ret), K(sys_var_val_str));
    } else {
      ObObj in_obj;
      ObObj out_obj;
      in_obj.set_varchar(sys_var_val_str);
      in_obj.set_collation_type(ObSysVariables::get_default_sysvar_collation());
      ObObj base_in_obj;
      ObObj base_out_obj;
      base_in_obj.set_varchar(base_sys_var_val_str);
      base_in_obj.set_collation_type(ObSysVariables::get_default_sysvar_collation());
      //varchar to others. so, no need to get collation from session
      ObCastCtx cast_ctx(&ObSysVarAllocator,
                         NULL,
                         0,
                         CM_NONE,
                         CS_TYPE_INVALID,
                         NULL);
      ObCastCtx fixed_cast_ctx(&ObBaseSysVarAllocator,
                    NULL,
                    0,
                    CM_NONE,
                    CS_TYPE_INVALID,
                    NULL);
      if (OB_FAIL(ObObjCaster::to_type(sys_var_type, cast_ctx, in_obj, out_obj))) {
        ObString sys_var_name = ObSysVariables::get_name(i);
        LOG_WARN("fail to cast object",
                 K(ret), "cell", in_obj, "from_type", ob_obj_type_str(in_obj.get_type()),
                 "to_type", ob_obj_type_str(sys_var_type), K(sys_var_name), K(i));
      } else if (OB_FAIL(ObObjCaster::to_type(sys_var_type, fixed_cast_ctx, base_in_obj, base_out_obj))) {
        ObString sys_var_name = ObSysVariables::get_name(i);
        LOG_WARN("fail to cast object",
                 K(ret), "cell", base_in_obj, "from_type", ob_obj_type_str(base_in_obj.get_type()),
                 "to_type", ob_obj_type_str(sys_var_type), K(sys_var_name), K(i));
      } else {
        if (ob_is_string_type(out_obj.get_type())) {
          out_obj.set_collation_level(CS_LEVEL_SYSCONST);
          base_out_obj.set_collation_level(CS_LEVEL_SYSCONST);
        }
        ObSysVarDefaultValues[i] = out_obj;
        ObSysVarBaseValues[i] = base_out_obj;
      }
    }
  }
  return ret;
}
""")
  cpp_file.write("\n")
  cpp_file.write("int64_t ObSysVarsToIdxMap::get_store_idx(int64_t var_id){ return ObSysVarsIdToArrayIdx[var_id];}\n")
  cpp_file.write("bool ObSysVarsToIdxMap::has_invalid_sys_var_id(){ return HasInvalidSysVar;}\n")
  cpp_file.write("\n")
  cpp_file.write("} // end namespace share\n")
  cpp_file.write("} // end namespace oceanbase\n")
  cpp_file.write("\n")

  cpp_file.close( )
  print("Generate " + str(cpp_file_name) + " successfully! Including " + str(var_num) + " system variables. \n")
  return

def write_sys_var_class_types(wfile, sorted_list):
  wfile.write("enum ObSysVarClassType\n{\n")
  wfile.write("  SYS_VAR_INVALID = -1,\n")
  for (name, attributes) in sorted_list:
    wfile.write("  SYS_VAR_" + str(name).upper() + " = " + str(attributes['id']) + ",\n")
  wfile.write("};\n")

def write_sys_var_classes(wfile, sorted_list):
  idx = 0
  for (name, attributes) in sorted_list:
    write_sys_var_class(\
        wfile, idx, str(name), \
        attributes["data_type"], \
        attributes["base_class"] if "base_class" in attributes.keys() else None, \
        attributes["on_check_and_convert_func"] if "on_check_and_convert_func" in attributes.keys() else None, \
        attributes["on_update_func"] if "on_update_func" in attributes.keys() else None, \
        attributes["to_select_obj_func"] if "to_select_obj_func" in attributes.keys() else None, \
        attributes["to_show_str_func"] if "to_show_str_func" in attributes.keys() else None, \
        attributes["get_meta_type_func"] if "get_meta_type_func" in attributes.keys() else None, \
        attributes["session_special_update_func"] if "session_special_update_func" in attributes.keys() else None)
    idx += 1

def write_sys_var_fac_class(wfile, sorted_list):
  mysql_sys_var_names_count = 0
  ob_sys_var_names_count = 0
  for (name, attributes) in sorted_list:
    if attributes['id'] < 10000:
      mysql_sys_var_names_count += 1
    else:
      ob_sys_var_names_count += 1
  wfile.write("""
class ObSysVarFactory
{
public:
  ObSysVarFactory(const int64_t tenant_id = OB_SERVER_TENANT_ID);
  virtual ~ObSysVarFactory();
  void destroy();
  int create_sys_var(ObSysVarClassType sys_var_id, ObBasicSysVar *&sys_var);
  int create_all_sys_vars();
  int free_sys_var(ObBasicSysVar *sys_var, int64_t sys_var_idx);
  static int create_sys_var(ObIAllocator &allocator_, ObSysVarClassType sys_var_id, ObBasicSysVar *&sys_var_ptr);
  static int calc_sys_var_store_idx(ObSysVarClassType sys_var_id, int64_t &store_idx);
  static int calc_sys_var_store_idx_by_name(const common::ObString &sys_var_name, int64_t &store_idx);
  static bool is_valid_sys_var_store_idx(int64_t store_idx);
  static ObSysVarClassType find_sys_var_id_by_name(const common::ObString &sys_var_name, bool is_from_sys_table = false); //二分查找
  static int get_sys_var_name_by_id(ObSysVarClassType sys_var_id, common::ObString &sys_var_name);
  static const common::ObString get_sys_var_name_by_id(ObSysVarClassType sys_var_id);

  const static int64_t MYSQL_SYS_VARS_COUNT = """)
  wfile.write(str(mysql_sys_var_names_count) + ";")
  wfile.write("""
  const static int64_t OB_SYS_VARS_COUNT = """)
  wfile.write(str(ob_sys_var_names_count) + ";")
  wfile.write("""
  const static int64_t ALL_SYS_VARS_COUNT = MYSQL_SYS_VARS_COUNT + OB_SYS_VARS_COUNT;
  const static int64_t INVALID_MAX_READ_STALE_TIME = -1;

  const static int16_t OB_SPECIFIC_SYS_VAR_ID_OFFSET = 10000;
  // 表示当前OB能够使用的sys var id的最大值，正常情况下，不需要申请大于OB_MAX_SYS_VAR_ID的sys var id，
  // 如果需要申请大于OB_MAX_SYS_VAR_ID的sys var id，需要先调整ob_max_sys_var_id的值
  const static int32_t OB_MAX_SYS_VAR_ID = """)
  wfile.write(str(ob_max_sys_var_id) + ";")
  wfile.write("""

private:
  static bool sys_var_name_case_cmp(const char *name1, const common::ObString &name2);
  const static char *SYS_VAR_NAMES_SORTED_BY_NAME[ALL_SYS_VARS_COUNT];
  const static ObSysVarClassType SYS_VAR_IDS_SORTED_BY_NAME[ALL_SYS_VARS_COUNT];
  const static char *SYS_VAR_NAMES_SORTED_BY_ID[ALL_SYS_VARS_COUNT];
  common::ObArenaAllocator allocator_;
  ObBasicSysVar *store_[ALL_SYS_VARS_COUNT];
  ObBasicSysVar *store_buf_[ALL_SYS_VARS_COUNT];
  bool all_sys_vars_created_;
};
""")

def write_sys_var_class(wfile, idx, sys_var_name, data_type, base_class, \
    on_check_and_convert_func, on_update_func, to_select_obj_func, \
    to_show_str_func, get_meta_type_func, session_special_update_func):
  name_segs = sys_var_name.split("_")
  sys_var_cls_name = "ObSysVar"
  for name_seg in name_segs:
    sys_var_cls_name += name_seg.capitalize()
  base_cls_name = "invalid"
  if base_class is not None:
    base_cls_name = base_class
  elif VARCHAR == data_type:
    base_cls_name = "ObVarcharSysVar"
  elif INT == data_type:
    base_cls_name = "ObIntSysVar"
  elif UINT == data_type:
    base_cls_name = "ObIntSysVar"
  elif TINY == data_type:
    base_cls_name = "ObTinyintSysVar"
  elif BOOL == data_type:
    base_cls_name = "ObBoolSysVar"
  elif NUMERIC == data_type:
    base_cls_name = "ObNumericSysVar"
  elif ENUM == data_type:
    base_cls_name = "ObEnumSysVar"

  wfile.write("class " + sys_var_cls_name + " : public " + base_cls_name + "\n")
  wfile.write("{\n");
  if ENUM == data_type:
    enum_name = sys_var_name.upper();
    enum_name += "_NAMES"
    wfile.write("public:\n");
    wfile.write("  const static char * " + enum_name + "[];\n");
  wfile.write("public:\n");
  if base_cls_name == "ObEnumSysVar":
    wfile.write("  " + sys_var_cls_name + "() : " + base_cls_name + "(" + enum_name \
        + ", " + ("NULL" if on_check_and_convert_func is None else on_check_and_convert_func) \
        + ", " + ("NULL" if on_update_func is None else on_update_func) \
        + ", " + ("NULL" if to_select_obj_func is None else to_select_obj_func) \
        + ", " + ("NULL" if to_show_str_func is None else to_show_str_func) \
        + ", " + ("NULL" if get_meta_type_func is None else get_meta_type_func) \
        + ") {}\n")
  elif base_cls_name == "ObSessionSpecialIntSysVar":
    wfile.write("  " + sys_var_cls_name + "() : " + base_cls_name + "(" \
        + ("NULL" if on_check_and_convert_func is None else on_check_and_convert_func) \
        + ", " + ("NULL" if session_special_update_func is None else session_special_update_func) \
        + ", " + ("NULL" if to_select_obj_func is None else to_select_obj_func) \
        + ", " + ("NULL" if to_show_str_func is None else to_show_str_func) \
        + ", " + ("NULL" if get_meta_type_func is None else get_meta_type_func) \
        + ") {}\n")
  elif base_cls_name == "ObSessionSpecialVarcharSysVar":
    wfile.write("  " + sys_var_cls_name + "() : " + base_cls_name + "(" \
        + ("NULL" if on_check_and_convert_func is None else on_check_and_convert_func) \
        + ", " + ("NULL" if on_update_func is None else on_update_func) \
        + ", " + ("NULL" if session_special_update_func is None else session_special_update_func) \
        + ", " + ("NULL" if to_select_obj_func is None else to_select_obj_func) \
        + ", " + ("NULL" if to_show_str_func is None else to_show_str_func) \
        + ", " + ("NULL" if get_meta_type_func is None else get_meta_type_func) \
        + ") {}\n")
  elif base_cls_name == "ObSessionSpecialBoolSysVar":
    wfile.write("  " + sys_var_cls_name + "() : " + base_cls_name + "(" \
        + ("NULL" if on_check_and_convert_func is None else on_check_and_convert_func) \
        + ", " + ("NULL" if on_update_func is None else on_update_func) \
        + ", " + ("NULL" if session_special_update_func is None else session_special_update_func) \
        + ", " + ("NULL" if to_select_obj_func is None else to_select_obj_func) \
        + ", " + ("NULL" if to_show_str_func is None else to_show_str_func) \
        + ", " + ("NULL" if get_meta_type_func is None else get_meta_type_func) \
        + ") {}\n")
  else:
    wfile.write("  " + sys_var_cls_name + "() : " + base_cls_name + "(" \
        + ("NULL" if on_check_and_convert_func is None else on_check_and_convert_func) \
        + ", " + ("NULL" if on_update_func is None else on_update_func) \
        + ", " + ("NULL" if to_select_obj_func is None else to_select_obj_func) \
        + ", " + ("NULL" if to_show_str_func is None else to_show_str_func) \
        + ", " + ("NULL" if get_meta_type_func is None else get_meta_type_func) \
        + ") {}\n")
  wfile.write("  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_" + sys_var_name.upper() + "; }\n")
  wfile.write("  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(" + str(idx) + "); }\n")
  wfile.write("};\n");

def make_sys_var_class_type_h(pdir, filename, sorted_list):
  wfile = open(filename, 'w')
  wfile.write(file_head_annotation);
  suffix_idx = filename.find(".h")
  file_def_str = "OCEANBASE_" + pdir.replace("/", "_").upper() + "_" + filename[0 : suffix_idx].upper() + "_"
  wfile.write("#ifndef " + file_def_str + "\n")
  wfile.write("#define " + file_def_str + "\n")
  wfile.write("#include <stdint.h>\n")
  wfile.write("""
namespace oceanbase
{
namespace share
{
""")
  write_sys_var_class_types(wfile, sorted_list)
  wfile.write("""
}
}
""")
  wfile.write("#endif //" + file_def_str)
  print("Generate " + str(filename) + " successfully!\n");

def make_sys_var_h(pdir, filename, sorted_list):
  wfile = open(filename, 'w')
  wfile.write(file_head_annotation);
  suffix_idx = filename.find(".h")
  file_def_str = "OCEANBASE_" + pdir.replace("/", "_").upper() + "_" + filename[0 : suffix_idx].upper() + "_"
  wfile.write("#ifndef " + file_def_str + "\n")
  wfile.write("#define " + file_def_str + "\n")
  wfile.write("#include \"common/object/ob_object.h\"\n")
  wfile.write("#include \"share/system_variable/ob_system_variable.h\"\n")
  wfile.write("#include \"share/system_variable/ob_system_variable_init.h\"\n")
  wfile.write("#include \"lib/allocator/page_arena.h\"\n")
  wfile.write("#include \"lib/string/ob_string.h\"\n");
  wfile.write("#include \"lib/container/ob_array.h\"\n")
  wfile.write("""
namespace oceanbase
{
namespace share
{
""")
  write_sys_var_classes(wfile, sorted_list)
  wfile.write("\n")
  write_sys_var_fac_class(wfile, sorted_list)
  wfile.write("""
}
}
""")
  wfile.write("#endif //" + file_def_str)
  print("Generate " + str(filename) + " successfully!\n");

def make_sys_var_cpp(pdir, filename, list_sorted_by_name, list_sorted_by_id):
  wfile = open(filename, 'w')
  wfile.write(file_head_annotation);
  wfile.write("#define USING_LOG_PREFIX SQL_SESSION\n");
  wfile.write("#include \"share/ob_define.h\"\n")
  wfile.write("#include \"" + pdir + "/" + filename.replace(".cpp", ".h") + "\"\n")
  # wfile.write("#include \"share/system_variable/ob_system_variable_init.cpp\"\n")
  wfile.write("#include \"share/ob_errno.h\"\n")
  wfile.write("#include <algorithm>\n")
  wfile.write("using namespace oceanbase::common;\n");
  wfile.write("""
namespace oceanbase
{
namespace share
{
""")
  for (name, attributes) in list_sorted_by_id:
    if ENUM == attributes["data_type"]:
        name_segs = str(name).split("_")
        sys_var_cls_name = "ObSysVar"
        for name_seg in name_segs:
          sys_var_cls_name += name_seg.capitalize();
        enum_name = str(name).upper();
        enum_name += "_NAMES";
        wfile.write("const char *" + sys_var_cls_name + "::" + enum_name + "[] = {");
        idx = 0
        for val in attributes["enum_names"]:
          if idx > 0:
            wfile.write(",")
          wfile.write("\n  \"" + val + "\"")
          idx += 1
        wfile.write(",\n  0\n};\n")

  wfile.write("""
const char *ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME[] = {
""")
  idx = 0
  for (name, attributes) in list_sorted_by_name:
    if idx > 0:
      wfile.write(",\n")
    wfile.write("  \"" + name + "\"")
    idx += 1
  wfile.write("""
};

const ObSysVarClassType ObSysVarFactory::SYS_VAR_IDS_SORTED_BY_NAME[] = {
""")
  idx = 0
  for (name, attributes) in list_sorted_by_name:
    if idx > 0:
      wfile.write(",\n")
    wfile.write("  SYS_VAR_" + str(name).upper())
    idx += 1
  wfile.write("""
};

const char *ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_ID[] = {
""")
  idx = 0
  for (name, attributes) in list_sorted_by_id:
    if idx > 0:
      wfile.write(",\n")
    wfile.write("  \"" + name + "\"")
    idx += 1
  wfile.write("""
};

bool ObSysVarFactory::sys_var_name_case_cmp(const char *name1, const ObString &name2)
{
  return name2.case_compare(name1) > 0;
}

ObSysVarClassType ObSysVarFactory::find_sys_var_id_by_name(const ObString &sys_var_name,
                                                           bool is_from_sys_table /*= false*/)
{
  int ret = OB_SUCCESS;
  ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
  int64_t lower_idx = std::lower_bound(ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME,
      ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME + ObSysVarFactory::ALL_SYS_VARS_COUNT,
      sys_var_name, ObSysVarFactory::sys_var_name_case_cmp) -
      ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME;
  if (OB_UNLIKELY(lower_idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid lower index", K(ret), K(sys_var_name), K(lower_idx), K(lbt()));
  } else if (OB_UNLIKELY(lower_idx > ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid lower index", K(ret), K(sys_var_name), K(lower_idx),
              LITERAL_K(ObSysVarFactory::ALL_SYS_VARS_COUNT), K(lbt()));
  } else if (OB_UNLIKELY(ObSysVarFactory::ALL_SYS_VARS_COUNT == lower_idx)) {
    // std::lower_bound返回ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME +
    // ObSysVarFactory::ALL_SYS_VARS_COUNT的地址，即是找不到，而不是出错
    ret = OB_SEARCH_NOT_FOUND;
  } else if (0 != sys_var_name.case_compare(
      ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_NAME[lower_idx])) {
    // 找不到
    ret = OB_SEARCH_NOT_FOUND;
  } else {
    sys_var_id = ObSysVarFactory::SYS_VAR_IDS_SORTED_BY_NAME[lower_idx]; // 找到了
  }
  if (OB_UNLIKELY(OB_SEARCH_NOT_FOUND == ret)) {
    if (is_from_sys_table) {
      LOG_INFO("new system variable is added , so can not found; don't worry", K(sys_var_name),
               K(lower_idx), LITERAL_K(ObSysVarFactory::ALL_SYS_VARS_COUNT), K(lbt()));
    } else {
      LOG_WARN("sys var name not found", K(sys_var_name), K(lower_idx),
               LITERAL_K(ObSysVarFactory::ALL_SYS_VARS_COUNT), K(lbt()));
    }
  }
  return sys_var_id;
}

int ObSysVarFactory::calc_sys_var_store_idx(ObSysVarClassType sys_var_id, int64_t &store_idx)
{
  int ret = OB_SUCCESS;
  int64_t real_idx = -1;
  int64_t var_id = static_cast<int64_t>(sys_var_id);
  if (ObSysVarsToIdxMap::has_invalid_sys_var_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("has invalid sys var id", K(ret), K(ObSysVarsToIdxMap::has_invalid_sys_var_id()));
  } else if (OB_UNLIKELY(var_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid sys var id", K(ret), K(var_id));
  } else {
    // 直接利用ObSysVarsIdToArrayIdx 索引数组查询到对应的store idx
    real_idx = ObSysVarsToIdxMap::get_store_idx(var_id);
    if (real_idx < 0) {
      ret = OB_SYS_VARS_MAYBE_DIFF_VERSION;
      LOG_WARN("invalid sys var id, maybe sys vars version is different", K(ret), K(var_id), K(real_idx),
          LITERAL_K(ObSysVarFactory::OB_SPECIFIC_SYS_VAR_ID_OFFSET),
          LITERAL_K(ObSysVarFactory::OB_SYS_VARS_COUNT));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    store_idx = real_idx;
  }
  return ret;
}

int ObSysVarFactory::calc_sys_var_store_idx_by_name(const common::ObString &sys_var_name,
                                                    int64_t &store_idx)
{
  int ret = OB_SUCCESS;
  ObSysVarClassType sys_var_id = find_sys_var_id_by_name(sys_var_name);
  if (OB_FAIL(calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_name), K(lbt()));
  }
  return ret;
}

bool ObSysVarFactory::is_valid_sys_var_store_idx(int64_t store_idx)
{
  return 0 <= store_idx && store_idx < ObSysVarFactory::ALL_SYS_VARS_COUNT;
}

int ObSysVarFactory::get_sys_var_name_by_id(ObSysVarClassType sys_var_id, ObString &sys_var_name)
{
  int ret = OB_SUCCESS;
  int64_t store_idx = -1;
  if (OB_FAIL(calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_id));
  } else {
    sys_var_name = ObString::make_string(ObSysVarFactory::SYS_VAR_NAMES_SORTED_BY_ID[store_idx]);
  }
  return ret;
}

const ObString ObSysVarFactory::get_sys_var_name_by_id(ObSysVarClassType sys_var_id)
{
  ObString sys_var_name;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_sys_var_name_by_id(sys_var_id, sys_var_name))) {
    sys_var_name = ObString::make_string("invalid_sys_var_name");
    LOG_WARN("invalid sys var id", K(ret), K(sys_var_id));
  }
  return sys_var_name;
}

ObSysVarFactory::ObSysVarFactory(const int64_t tenant_id)
  : allocator_(ObMemAttr(tenant_id, ObModIds::OB_COMMON_SYS_VAR_FAC)),
    all_sys_vars_created_(false)
{
  MEMSET(store_, 0, sizeof(store_));
  MEMSET(store_buf_, 0, sizeof(store_buf_));
}

ObSysVarFactory::~ObSysVarFactory()
{
  destroy();
}

void ObSysVarFactory::destroy()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ALL_SYS_VARS_COUNT; ++i) {
    if (OB_NOT_NULL(store_[i])) {
      store_[i]->~ObBasicSysVar();
      store_[i] = nullptr;
    }
    if (OB_NOT_NULL(store_buf_[i])) {
      store_buf_[i]->~ObBasicSysVar();
      store_buf_[i] = nullptr;
    }
  }
  allocator_.reset();
  all_sys_vars_created_ = false;
}

int ObSysVarFactory::free_sys_var(ObBasicSysVar *sys_var, int64_t sys_var_idx)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(sys_var));
  OV (is_valid_sys_var_store_idx(sys_var_idx));
  OV (sys_var == store_[sys_var_idx], OB_ERR_UNEXPECTED, sys_var, sys_var_idx);
  if (OB_NOT_NULL(store_buf_[sys_var_idx])) {
    OX (store_buf_[sys_var_idx]->~ObBasicSysVar());
    OX (allocator_.free(store_buf_[sys_var_idx]));
    OX (store_buf_[sys_var_idx] = nullptr);
  }
  OX (store_buf_[sys_var_idx] = store_[sys_var_idx]);
  OX (store_buf_[sys_var_idx]->clean_value());
  OX (store_[sys_var_idx] = nullptr);
  return ret;
}

int ObSysVarFactory::create_all_sys_vars()
{
  int ret = OB_SUCCESS;
  if (!all_sys_vars_created_) {
    int64_t store_idx = -1;
    ObBasicSysVar *sys_var_ptr = NULL;
    int64_t total_mem_size = 0
""")
  for (name, attributes) in list_sorted_by_id:
    name_segs = name.split("_")
    sys_var_cls_name = "ObSysVar"
    for name_seg in name_segs:
      sys_var_cls_name += name_seg.capitalize()
    wfile.write("        + sizeof(" + sys_var_cls_name + ")\n")
  wfile.write("        ;\n")
  wfile.write("    void *ptr = NULL;\n")
  wfile.write("    if (OB_ISNULL(ptr = allocator_.alloc(total_mem_size))) {\n")
  wfile.write("      ret = OB_ALLOCATE_MEMORY_FAILED;\n")
  wfile.write("      LOG_ERROR(\"fail to alloc memory\", K(ret));\n")
  wfile.write("    } else {\n")
  wfile.write("      all_sys_vars_created_ = true;\n")
  wfile.write("    }\n")

  for (name, attributes) in list_sorted_by_id:
    name_segs = name.split("_")
    sys_var_cls_name = "ObSysVar"
    for name_seg in name_segs:
      sys_var_cls_name += name_seg.capitalize()
    wfile.write("    if (OB_SUCC(ret)) {\n")
    wfile.write("      if (OB_ISNULL(sys_var_ptr = new (ptr)" + sys_var_cls_name + "())) {\n")
    wfile.write("        ret = OB_ALLOCATE_MEMORY_FAILED;\n")
    wfile.write("        LOG_ERROR(\"fail to new " + sys_var_cls_name + "\", K(ret));\n")
    wfile.write("      } else {\n")
    wfile.write("        store_buf_[ObSysVarsToIdxMap::get_store_idx(static_cast<int64_t>(SYS_VAR_" + name.upper()  + "))] = sys_var_ptr;\n")
    wfile.write("        ptr = (void *)((char *)ptr + sizeof(" + sys_var_cls_name + "));\n")
    wfile.write("      }\n")
    wfile.write("    }\n")

  wfile.write("""
  }
  return ret;
}

int ObSysVarFactory::create_sys_var(ObIAllocator &allocator_, ObSysVarClassType sys_var_id,
                                        ObBasicSysVar *&sys_var_ptr)
{
  int ret = OB_SUCCESS;
  switch(sys_var_id) {
""")
  for (name, attributes) in list_sorted_by_id:
    name_segs = name.split("_")
    sys_var_cls_name = "ObSysVar"
    for name_seg in name_segs:
      sys_var_cls_name += name_seg.capitalize()
    wfile.write("    case SYS_VAR_" + str(name).upper() + ": {\n")
    wfile.write("      void *ptr = NULL;\n")
    wfile.write("      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(" + sys_var_cls_name + ")))) {\n")
    wfile.write("        ret = OB_ALLOCATE_MEMORY_FAILED;\n")
    wfile.write("        LOG_ERROR(\"fail to alloc memory\", K(ret), K(sizeof(" + sys_var_cls_name + ")));\n")
    wfile.write("      } else if (OB_ISNULL(sys_var_ptr = new (ptr)" + sys_var_cls_name + "())) {\n")
    wfile.write("        ret = OB_ALLOCATE_MEMORY_FAILED;\n")
    wfile.write("        LOG_ERROR(\"fail to new " + sys_var_cls_name + "\", K(ret));\n")
    wfile.write("      }\n")
    wfile.write("      break;\n")
    wfile.write("    }\n")
  wfile.write("""
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid system variable id", K(ret), K(sys_var_id));
      break;
    }
  }
  return ret;
}

int ObSysVarFactory::create_sys_var(ObSysVarClassType sys_var_id, ObBasicSysVar *&sys_var)
{
  int ret = OB_SUCCESS;
  int64_t store_idx = -1;
  ObBasicSysVar *sys_var_ptr = NULL;
  if (OB_FAIL(calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_id));
  } else if (store_idx < 0 || store_idx >= ALL_SYS_VARS_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected store idx", K(ret), K(store_idx), K(sys_var_id));
  } else if (OB_NOT_NULL(store_[store_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store ptr shoule be null", K(ret), K(store_idx), K(sys_var_id));
  } else {
    if (OB_NOT_NULL(store_buf_[store_idx])) {
      sys_var_ptr = store_buf_[store_idx];
      store_buf_[store_idx] = nullptr;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(sys_var_ptr)) {
    if (OB_FAIL(create_sys_var(allocator_, sys_var_id, sys_var_ptr))) {
      LOG_WARN("fail to calc sys var", K(ret), K(sys_var_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(sys_var_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ret is OB_SUCCESS, but sys_var_ptr is NULL", K(ret), K(sys_var_id));
    } else if (OB_NOT_NULL(store_[store_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("store_[store_idx] should be NULL", K(ret), K(sys_var_id));
    } else {
      store_[store_idx] = sys_var_ptr;
      sys_var = sys_var_ptr;
    }
  }
  if (OB_FAIL(ret) && sys_var_ptr != nullptr) {
    sys_var_ptr->~ObBasicSysVar();
    sys_var_ptr = NULL;
  }
  return ret;
}
""")
  wfile.write("""
}
}
""")
  print("Generate " + str(filename) + " successfully!\n")

def calc_flags_from_str(flags_str):
  result = 0
  flags = flags_str.split("|")
  flags_len = len(flags)
  for i in range(0, flags_len):
    result = (result | flag_value_dict[flags[i].strip().upper()])
  return result

def gen_sys_vars_dict_script_for_upgrade(filename, list_sorted_by_id):
  os.chmod(filename, stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP + stat.S_IROTH)
  wfile = open(filename, 'w')
  annotation_is_written = False
  wfile.write('#!/usr/bin/env python\n')
  wfile.write('# -*- coding: utf-8 -*-\n')
  wfile.write('\n')
  wfile.write("# sys_vars_dict.py是由gen_ob_sys_variables.py根据ob_system_variable_init.json和upgrade_sys_var_base_script.py文件生成的，不可修改\n")
  wfile.write('sys_var_dict = {}\n')
  for (name, attributes) in list_sorted_by_id:
    wfile.write("sys_var_dict[\"" + name + "\"] = {\"id\": " + str(attributes["id"]) + ", \"name\": \"" + attributes["name"] + "\", \"value\": \"" + attributes["value"] + "\", \"data_type\": " + str(type_value_dict[attributes["data_type"]]) + ", \"info\": \"" + attributes["info"] + "\", \"flags\": " + str(calc_flags_from_str(attributes["flags"])) + ((", \"min_val\": \"" + attributes["min_val"] + "\"") if "min_val" in attributes.keys() else "") + ((", \"max_val\": \"" + attributes["max_val"] + "\"") if "max_val" in attributes.keys() else "") + "}\n")
  wfile.close()
  os.chmod(filename, stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)
  print("Generate " + str(filename) + " successfully!\n")

def gen_upgrade_script():
  print('\n=========run gen_upgrade_scripts.py, begin=========\n')
  info = os.popen('cd ../../../tools/upgrade/; ./gen_upgrade_scripts.py;')
  print(info.read())
  print('\n=========run gen_upgrade_scripts.py, end=========\n')

pdir = "share/system_variable"
json_file_name = "ob_system_variable_init.json"
head_file_name = "ob_system_variable_init.h"
cpp_file_name =  "ob_system_variable_init.cpp"
alias_file_name ="ob_system_variable_alias.h"
sys_var_class_type_head_file_name = "ob_sys_var_class_type.h"
sys_var_fac_head_file_name = "ob_system_variable_factory.h"
sys_var_fac_cpp_file_name = "ob_system_variable_factory.cpp"
#sys_vars_dict_script_file_name = "../../../tools/upgrade/sys_vars_dict.py"

(json_Dict, list_sorted_by_name, list_sorted_by_id) = parse_json(json_file_name)

make_head_file(pdir, head_file_name, list_sorted_by_id)
make_cpp_file(pdir, cpp_file_name, list_sorted_by_id)
make_alias_file(pdir, alias_file_name, list_sorted_by_id)

make_sys_var_class_type_h(pdir, sys_var_class_type_head_file_name, list_sorted_by_id)
make_sys_var_h(pdir, sys_var_fac_head_file_name, list_sorted_by_id)
make_sys_var_cpp(pdir, sys_var_fac_cpp_file_name, list_sorted_by_name, list_sorted_by_id)

#gen_sys_vars_dict_script_for_upgrade(sys_vars_dict_script_file_name, list_sorted_by_id)
#gen_upgrade_script()
