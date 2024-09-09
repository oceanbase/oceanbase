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

#define USING_LOG_PREFIX SHARE
#include "share/system_variable/ob_system_variable_init.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/object/ob_obj_cast.h"
#include "common/expression/ob_expr_string_buf.h"
#include "common/expression/ob_expr_string_buf.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
static ObSysVarFromJson ObSysVars[ObSysVarFactory::ALL_SYS_VARS_COUNT];
static ObObj ObSysVarDefaultValues[ObSysVarFactory::ALL_SYS_VARS_COUNT];
static ObArenaAllocator ObSysVarAllocator(ObModIds::OB_COMMON_SYS_VAR_DEFAULT_VALUE);
static ObObj ObSysVarBaseValues[ObSysVarFactory::ALL_SYS_VARS_COUNT];
static ObArenaAllocator ObBaseSysVarAllocator(ObModIds::OB_COMMON_SYS_VAR_DEFAULT_VALUE);
static int64_t ObSysVarsIdToArrayIdx[ObSysVarFactory::OB_MAX_SYS_VAR_ID];
// VarsInit中需要判断当前最大的SysVars对应的id，是否大于OB_MAX_SYS_VAR_ID
// 如果大于OB_MAX_SYS_VAR_ID表示存在无效的SysVarsId
static bool HasInvalidSysVar = false;

static struct VarsInit{
  VarsInit(){
    // 保存当前系统变量的最大的id
    int64_t cur_max_var_id = 0;
    // ObSysVarsIdToArrayIdx数组默认初始值为-1，-1表示无效索引
    memset(ObSysVarsIdToArrayIdx, -1, sizeof(ObSysVarsIdToArrayIdx));
    [&] (){
      ObSysVars[0].default_value_ = "1" ;
      ObSysVars[0].info_ = "" ;
      ObSysVars[0].name_ = "auto_increment_increment" ;
      ObSysVars[0].data_type_ = ObUInt64Type ;
      ObSysVars[0].min_val_ = "1" ;
      ObSysVars[0].max_val_ = "65535" ;
      ObSysVars[0].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[0].id_ = SYS_VAR_AUTO_INCREMENT_INCREMENT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_INCREMENT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_AUTO_INCREMENT_INCREMENT] = 0 ;
      ObSysVars[0].base_value_ = "1" ;
    ObSysVars[0].alias_ = "OB_SV_AUTO_INCREMENT_INCREMENT" ;
    }();

    [&] (){
      ObSysVars[1].default_value_ = "1" ;
      ObSysVars[1].info_ = "" ;
      ObSysVars[1].name_ = "auto_increment_offset" ;
      ObSysVars[1].data_type_ = ObUInt64Type ;
      ObSysVars[1].min_val_ = "1" ;
      ObSysVars[1].max_val_ = "65535" ;
      ObSysVars[1].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[1].id_ = SYS_VAR_AUTO_INCREMENT_OFFSET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_OFFSET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_AUTO_INCREMENT_OFFSET] = 1 ;
      ObSysVars[1].base_value_ = "1" ;
    ObSysVars[1].alias_ = "OB_SV_AUTO_INCREMENT_OFFSET" ;
    }();

    [&] (){
      ObSysVars[2].default_value_ = "1" ;
      ObSysVars[2].info_ = "" ;
      ObSysVars[2].name_ = "autocommit" ;
      ObSysVars[2].data_type_ = ObIntType ;
      ObSysVars[2].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[2].id_ = SYS_VAR_AUTOCOMMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTOCOMMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_AUTOCOMMIT] = 2 ;
      ObSysVars[2].base_value_ = "1" ;
    ObSysVars[2].alias_ = "OB_SV_AUTOCOMMIT" ;
    }();

    [&] (){
      ObSysVars[3].default_value_ = "45" ;
      ObSysVars[3].info_ = "The character set in which statements are sent by the client" ;
      ObSysVars[3].name_ = "character_set_client" ;
      ObSysVars[3].data_type_ = ObIntType ;
      ObSysVars[3].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[3].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[3].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[3].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[3].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null" ;
      ObSysVars[3].id_ = SYS_VAR_CHARACTER_SET_CLIENT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_CLIENT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_CLIENT] = 3 ;
      ObSysVars[3].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[3].base_value_ = "45" ;
    ObSysVars[3].alias_ = "OB_SV_CHARACTER_SET_CLIENT" ;
    }();

    [&] (){
      ObSysVars[4].default_value_ = "45" ;
      ObSysVars[4].info_ = "The character set which should be translated to after receiving the statement" ;
      ObSysVars[4].name_ = "character_set_connection" ;
      ObSysVars[4].data_type_ = ObIntType ;
      ObSysVars[4].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[4].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::NULLABLE ;
      ObSysVars[4].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[4].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[4].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null" ;
      ObSysVars[4].id_ = SYS_VAR_CHARACTER_SET_CONNECTION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_CONNECTION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_CONNECTION] = 4 ;
      ObSysVars[4].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[4].base_value_ = "45" ;
    ObSysVars[4].alias_ = "OB_SV_CHARACTER_SET_CONNECTION" ;
    }();

    [&] (){
      ObSysVars[5].default_value_ = "45" ;
      ObSysVars[5].info_ = "The character set of the default database" ;
      ObSysVars[5].name_ = "character_set_database" ;
      ObSysVars[5].data_type_ = ObIntType ;
      ObSysVars[5].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[5].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[5].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[5].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[5].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null" ;
      ObSysVars[5].id_ = SYS_VAR_CHARACTER_SET_DATABASE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_DATABASE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_DATABASE] = 5 ;
      ObSysVars[5].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[5].base_value_ = "45" ;
    ObSysVars[5].alias_ = "OB_SV_CHARACTER_SET_DATABASE" ;
    }();

    [&] (){
      ObSysVars[6].default_value_ = "45" ;
      ObSysVars[6].info_ = "The character set which server should translate to before shipping result sets or error message back to the client" ;
      ObSysVars[6].name_ = "character_set_results" ;
      ObSysVars[6].data_type_ = ObIntType ;
      ObSysVars[6].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[6].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[6].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[6].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[6].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset" ;
      ObSysVars[6].id_ = SYS_VAR_CHARACTER_SET_RESULTS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_RESULTS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_RESULTS] = 6 ;
      ObSysVars[6].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[6].base_value_ = "45" ;
    ObSysVars[6].alias_ = "OB_SV_CHARACTER_SET_RESULTS" ;
    }();

    [&] (){
      ObSysVars[7].default_value_ = "45" ;
      ObSysVars[7].info_ = "The server character set" ;
      ObSysVars[7].name_ = "character_set_server" ;
      ObSysVars[7].data_type_ = ObIntType ;
      ObSysVars[7].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[7].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[7].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[7].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[7].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null" ;
      ObSysVars[7].id_ = SYS_VAR_CHARACTER_SET_SERVER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_SERVER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_SERVER] = 7 ;
      ObSysVars[7].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[7].base_value_ = "45" ;
    ObSysVars[7].alias_ = "OB_SV_CHARACTER_SET_SERVER" ;
    }();

    [&] (){
      ObSysVars[8].default_value_ = "45" ;
      ObSysVars[8].info_ = "The character set used by the server for storing identifiers." ;
      ObSysVars[8].name_ = "character_set_system" ;
      ObSysVars[8].data_type_ = ObIntType ;
      ObSysVars[8].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[8].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY ;
      ObSysVars[8].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[8].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[8].id_ = SYS_VAR_CHARACTER_SET_SYSTEM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_SYSTEM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_SYSTEM] = 8 ;
      ObSysVars[8].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[8].base_value_ = "45" ;
    ObSysVars[8].alias_ = "OB_SV_CHARACTER_SET_SYSTEM" ;
    }();

    [&] (){
      ObSysVars[9].default_value_ = "45" ;
      ObSysVars[9].info_ = "The collation which the server should translate to after receiving the statement" ;
      ObSysVars[9].name_ = "collation_connection" ;
      ObSysVars[9].data_type_ = ObIntType ;
      ObSysVars[9].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_collation" ;
      ObSysVars[9].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[9].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[9].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_collation" ;
      ObSysVars[9].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_collation_not_null" ;
      ObSysVars[9].id_ = SYS_VAR_COLLATION_CONNECTION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_COLLATION_CONNECTION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_COLLATION_CONNECTION] = 9 ;
      ObSysVars[9].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[9].base_value_ = "45" ;
    ObSysVars[9].alias_ = "OB_SV_COLLATION_CONNECTION" ;
    }();

    [&] (){
      ObSysVars[10].default_value_ = "45" ;
      ObSysVars[10].info_ = "The collation of the default database" ;
      ObSysVars[10].name_ = "collation_database" ;
      ObSysVars[10].data_type_ = ObIntType ;
      ObSysVars[10].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_collation" ;
      ObSysVars[10].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[10].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[10].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_collation" ;
      ObSysVars[10].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_collation_not_null" ;
      ObSysVars[10].id_ = SYS_VAR_COLLATION_DATABASE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_COLLATION_DATABASE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_COLLATION_DATABASE] = 10 ;
      ObSysVars[10].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[10].base_value_ = "45" ;
    ObSysVars[10].alias_ = "OB_SV_COLLATION_DATABASE" ;
    }();

    [&] (){
      ObSysVars[11].default_value_ = "45" ;
      ObSysVars[11].info_ = "The server collation" ;
      ObSysVars[11].name_ = "collation_server" ;
      ObSysVars[11].data_type_ = ObIntType ;
      ObSysVars[11].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_collation" ;
      ObSysVars[11].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[11].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[11].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_collation" ;
      ObSysVars[11].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_collation_not_null" ;
      ObSysVars[11].id_ = SYS_VAR_COLLATION_SERVER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_COLLATION_SERVER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_COLLATION_SERVER] = 11 ;
      ObSysVars[11].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[11].base_value_ = "45" ;
    ObSysVars[11].alias_ = "OB_SV_COLLATION_SERVER" ;
    }();

    [&] (){
      ObSysVars[12].default_value_ = "28800" ;
      ObSysVars[12].info_ = "The number of seconds the server waits for activity on an interactive connection before closing it." ;
      ObSysVars[12].name_ = "interactive_timeout" ;
      ObSysVars[12].data_type_ = ObIntType ;
      ObSysVars[12].min_val_ = "1" ;
      ObSysVars[12].max_val_ = "31536000" ;
      ObSysVars[12].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[12].id_ = SYS_VAR_INTERACTIVE_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INTERACTIVE_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INTERACTIVE_TIMEOUT] = 12 ;
      ObSysVars[12].base_value_ = "28800" ;
    ObSysVars[12].alias_ = "OB_SV_INTERACTIVE_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[13].default_value_ = "0" ;
      ObSysVars[13].info_ = "" ;
      ObSysVars[13].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_last_insert_id" ;
      ObSysVars[13].name_ = "last_insert_id" ;
      ObSysVars[13].data_type_ = ObUInt64Type ;
      ObSysVars[13].min_val_ = "0" ;
      ObSysVars[13].max_val_ = "18446744073709551615" ;
      ObSysVars[13].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[13].base_class_ = "ObSessionSpecialIntSysVar" ;
      ObSysVars[13].id_ = SYS_VAR_LAST_INSERT_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LAST_INSERT_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LAST_INSERT_ID] = 13 ;
      ObSysVars[13].base_value_ = "0" ;
    ObSysVars[13].alias_ = "OB_SV_LAST_INSERT_ID" ;
    }();

    [&] (){
      ObSysVars[14].default_value_ = "16777216" ;
      ObSysVars[14].info_ = "Max packet length to send to or receive from the server" ;
      ObSysVars[14].name_ = "max_allowed_packet" ;
      ObSysVars[14].data_type_ = ObIntType ;
      ObSysVars[14].min_val_ = "1024" ;
      ObSysVars[14].max_val_ = "1073741824" ;
      ObSysVars[14].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::SESSION_READONLY | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[14].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_max_allowed_packet" ;
      ObSysVars[14].id_ = SYS_VAR_MAX_ALLOWED_PACKET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_ALLOWED_PACKET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_ALLOWED_PACKET] = 14 ;
      ObSysVars[14].base_value_ = "4194304" ;
    ObSysVars[14].alias_ = "OB_SV_MAX_ALLOWED_PACKET" ;
    }();

    [&] (){
      ObSysVars[15].default_value_ = "281018368" ;
      ObSysVars[15].on_update_func_ = "ObSysVarOnUpdateFuncs::update_sql_mode" ;
      ObSysVars[15].name_ = "sql_mode" ;
      ObSysVars[15].data_type_ = ObUInt64Type ;
      ObSysVars[15].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_sql_mode" ;
      ObSysVars[15].info_ = "" ;
      ObSysVars[15].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[15].base_class_ = "ObSqlModeVar" ;
      ObSysVars[15].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_sql_mode" ;
      ObSysVars[15].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_sql_mode" ;
      ObSysVars[15].id_ = SYS_VAR_SQL_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_MODE] = 15 ;
      ObSysVars[15].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[15].base_value_ = "12582912" ;
    ObSysVars[15].alias_ = "OB_SV_SQL_MODE" ;
    }();

    [&] (){
      ObSysVars[16].default_value_ = "+08:00" ;
      ObSysVars[16].info_ = "" ;
      ObSysVars[16].name_ = "time_zone" ;
      ObSysVars[16].data_type_ = ObVarcharType ;
      ObSysVars[16].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[16].base_class_ = "ObTimeZoneSysVar" ;
      ObSysVars[16].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_time_zone" ;
      ObSysVars[16].id_ = SYS_VAR_TIME_ZONE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TIME_ZONE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TIME_ZONE] = 16 ;
      ObSysVars[16].base_value_ = "+08:00" ;
    ObSysVars[16].alias_ = "OB_SV_TIME_ZONE" ;
    }();

    [&] (){
      ObSysVars[17].default_value_ = "READ-COMMITTED" ;
      ObSysVars[17].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_isolation" ;
      ObSysVars[17].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation" ;
      ObSysVars[17].name_ = "tx_isolation" ;
      ObSysVars[17].data_type_ = ObVarcharType ;
      ObSysVars[17].info_ = "Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE" ;
      ObSysVars[17].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[17].base_class_ = "ObSessionSpecialVarcharSysVar" ;
      ObSysVars[17].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_isolation" ;
      ObSysVars[17].id_ = SYS_VAR_TX_ISOLATION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TX_ISOLATION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TX_ISOLATION] = 17 ;
      ObSysVars[17].base_value_ = "READ-COMMITTED" ;
    ObSysVars[17].alias_ = "OB_SV_TX_ISOLATION" ;
    }();

    [&] (){
      ObSysVars[18].default_value_ = "OceanBase 1.0.0" ;
      ObSysVars[18].info_ = "" ;
      ObSysVars[18].name_ = "version_comment" ;
      ObSysVars[18].data_type_ = ObVarcharType ;
      ObSysVars[18].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[18].id_ = SYS_VAR_VERSION_COMMENT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION_COMMENT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VERSION_COMMENT] = 18 ;
      ObSysVars[18].base_value_ = "OceanBase 1.0.0" ;
    ObSysVars[18].alias_ = "OB_SV_VERSION_COMMENT" ;
    }();

    [&] (){
      ObSysVars[19].default_value_ = "28800" ;
      ObSysVars[19].info_ = "The number of seconds the server waits for activity on a noninteractive connection before closing it." ;
      ObSysVars[19].name_ = "wait_timeout" ;
      ObSysVars[19].data_type_ = ObIntType ;
      ObSysVars[19].min_val_ = "1" ;
      ObSysVars[19].max_val_ = "31536000" ;
      ObSysVars[19].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[19].id_ = SYS_VAR_WAIT_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_WAIT_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_WAIT_TIMEOUT] = 19 ;
      ObSysVars[19].base_value_ = "28800" ;
    ObSysVars[19].alias_ = "OB_SV_WAIT_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[20].default_value_ = "2" ;
      ObSysVars[20].info_ = "control row cells to logged" ;
      ObSysVars[20].name_ = "binlog_row_image" ;
      ObSysVars[20].data_type_ = ObIntType ;
      ObSysVars[20].enum_names_ = "[u'MINIMAL', u'NOBLOB', u'FULL']" ;
      ObSysVars[20].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[20].id_ = SYS_VAR_BINLOG_ROW_IMAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_ROW_IMAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_ROW_IMAGE] = 20 ;
      ObSysVars[20].base_value_ = "2" ;
    ObSysVars[20].alias_ = "OB_SV_BINLOG_ROW_IMAGE" ;
    }();

    [&] (){
      ObSysVars[21].default_value_ = "63" ;
      ObSysVars[21].info_ = "" ;
      ObSysVars[21].name_ = "character_set_filesystem" ;
      ObSysVars[21].data_type_ = ObIntType ;
      ObSysVars[21].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[21].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE ;
      ObSysVars[21].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[21].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[21].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null" ;
      ObSysVars[21].id_ = SYS_VAR_CHARACTER_SET_FILESYSTEM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_FILESYSTEM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_FILESYSTEM] = 21 ;
      ObSysVars[21].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[21].base_value_ = "63" ;
    ObSysVars[21].alias_ = "OB_SV_CHARACTER_SET_FILESYSTEM" ;
    }();

    [&] (){
      ObSysVars[22].default_value_ = "10" ;
      ObSysVars[22].info_ = "" ;
      ObSysVars[22].name_ = "connect_timeout" ;
      ObSysVars[22].data_type_ = ObIntType ;
      ObSysVars[22].min_val_ = "2" ;
      ObSysVars[22].max_val_ = "31536000" ;
      ObSysVars[22].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[22].id_ = SYS_VAR_CONNECT_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CONNECT_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CONNECT_TIMEOUT] = 22 ;
      ObSysVars[22].base_value_ = "10" ;
    ObSysVars[22].alias_ = "OB_SV_CONNECT_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[23].default_value_ = "/usr/local/mysql/data/" ;
      ObSysVars[23].info_ = "" ;
      ObSysVars[23].name_ = "datadir" ;
      ObSysVars[23].data_type_ = ObVarcharType ;
      ObSysVars[23].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[23].id_ = SYS_VAR_DATADIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DATADIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DATADIR] = 23 ;
      ObSysVars[23].base_value_ = "/usr/local/mysql/data/" ;
    ObSysVars[23].alias_ = "OB_SV_DATADIR" ;
    }();

    [&] (){
      ObSysVars[24].default_value_ = "" ;
      ObSysVars[24].info_ = "Debug sync facility" ;
      ObSysVars[24].name_ = "debug_sync" ;
      ObSysVars[24].data_type_ = ObVarcharType ;
      ObSysVars[24].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[24].id_ = SYS_VAR_DEBUG_SYNC ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEBUG_SYNC)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEBUG_SYNC] = 24 ;
      ObSysVars[24].base_value_ = "" ;
    ObSysVars[24].alias_ = "OB_SV_DEBUG_SYNC" ;
    }();

    [&] (){
      ObSysVars[25].default_value_ = "4" ;
      ObSysVars[25].info_ = "" ;
      ObSysVars[25].name_ = "div_precision_increment" ;
      ObSysVars[25].data_type_ = ObIntType ;
      ObSysVars[25].min_val_ = "0" ;
      ObSysVars[25].max_val_ = "30" ;
      ObSysVars[25].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[25].id_ = SYS_VAR_DIV_PRECISION_INCREMENT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DIV_PRECISION_INCREMENT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DIV_PRECISION_INCREMENT] = 25 ;
      ObSysVars[25].base_value_ = "4" ;
    ObSysVars[25].alias_ = "OB_SV_DIV_PRECISION_INCREMENT" ;
    }();

    [&] (){
      ObSysVars[26].default_value_ = "1" ;
      ObSysVars[26].info_ = "whether use traditional mode for timestamp" ;
      ObSysVars[26].name_ = "explicit_defaults_for_timestamp" ;
      ObSysVars[26].data_type_ = ObIntType ;
      ObSysVars[26].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[26].id_ = SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP] = 26 ;
      ObSysVars[26].base_value_ = "1" ;
    ObSysVars[26].alias_ = "OB_SV_EXPLICIT_DEFAULTS_FOR_TIMESTAMP" ;
    }();

    [&] (){
      ObSysVars[27].default_value_ = "1024" ;
      ObSysVars[27].info_ = "" ;
      ObSysVars[27].name_ = "group_concat_max_len" ;
      ObSysVars[27].data_type_ = ObUInt64Type ;
      ObSysVars[27].min_val_ = "4" ;
      ObSysVars[27].max_val_ = "18446744073709551615" ;
      ObSysVars[27].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[27].id_ = SYS_VAR_GROUP_CONCAT_MAX_LEN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_CONCAT_MAX_LEN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_CONCAT_MAX_LEN] = 27 ;
      ObSysVars[27].base_value_ = "1024" ;
    ObSysVars[27].alias_ = "OB_SV_GROUP_CONCAT_MAX_LEN" ;
    }();

    [&] (){
      ObSysVars[28].default_value_ = "0" ;
      ObSysVars[28].info_ = "This variable is a synonym for the last_insert_id variable. It exists for compatibility with other database systems." ;
      ObSysVars[28].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_identity" ;
      ObSysVars[28].name_ = "identity" ;
      ObSysVars[28].data_type_ = ObUInt64Type ;
      ObSysVars[28].min_val_ = "0" ;
      ObSysVars[28].max_val_ = "18446744073709551615" ;
      ObSysVars[28].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[28].base_class_ = "ObSessionSpecialIntSysVar" ;
      ObSysVars[28].id_ = SYS_VAR_IDENTITY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_IDENTITY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_IDENTITY] = 28 ;
      ObSysVars[28].base_value_ = "0" ;
    ObSysVars[28].alias_ = "OB_SV_IDENTITY" ;
    }();

    [&] (){
      ObSysVars[29].default_value_ = "1" ;
      ObSysVars[29].info_ = "how table database names are stored and compared, 0 means stored using the lettercase in the CREATE_TABLE or CREATE_DATABASE statement. Name comparisons are case sensitive; 1 means that table and database names are stored in lowercase abd name comparisons are not case sensitive." ;
      ObSysVars[29].name_ = "lower_case_table_names" ;
      ObSysVars[29].data_type_ = ObIntType ;
      ObSysVars[29].min_val_ = "0" ;
      ObSysVars[29].max_val_ = "2" ;
      ObSysVars[29].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[29].id_ = SYS_VAR_LOWER_CASE_TABLE_NAMES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOWER_CASE_TABLE_NAMES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOWER_CASE_TABLE_NAMES] = 29 ;
      ObSysVars[29].base_value_ = "1" ;
    ObSysVars[29].alias_ = "OB_SV_LOWER_CASE_TABLE_NAMES" ;
    }();

    [&] (){
      ObSysVars[30].default_value_ = "30" ;
      ObSysVars[30].info_ = "" ;
      ObSysVars[30].name_ = "net_read_timeout" ;
      ObSysVars[30].data_type_ = ObIntType ;
      ObSysVars[30].min_val_ = "1" ;
      ObSysVars[30].max_val_ = "31536000" ;
      ObSysVars[30].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[30].id_ = SYS_VAR_NET_READ_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NET_READ_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NET_READ_TIMEOUT] = 30 ;
      ObSysVars[30].base_value_ = "30" ;
    ObSysVars[30].alias_ = "OB_SV_NET_READ_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[31].default_value_ = "60" ;
      ObSysVars[31].info_ = "" ;
      ObSysVars[31].name_ = "net_write_timeout" ;
      ObSysVars[31].data_type_ = ObIntType ;
      ObSysVars[31].min_val_ = "1" ;
      ObSysVars[31].max_val_ = "31536000" ;
      ObSysVars[31].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[31].id_ = SYS_VAR_NET_WRITE_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NET_WRITE_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NET_WRITE_TIMEOUT] = 31 ;
      ObSysVars[31].base_value_ = "60" ;
    ObSysVars[31].alias_ = "OB_SV_NET_WRITE_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[32].default_value_ = "0" ;
      ObSysVars[32].info_ = "" ;
      ObSysVars[32].name_ = "read_only" ;
      ObSysVars[32].data_type_ = ObIntType ;
      ObSysVars[32].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[32].id_ = SYS_VAR_READ_ONLY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_READ_ONLY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_READ_ONLY] = 32 ;
      ObSysVars[32].base_value_ = "0" ;
    ObSysVars[32].alias_ = "OB_SV_READ_ONLY" ;
    }();

    [&] (){
      ObSysVars[33].default_value_ = "0" ;
      ObSysVars[33].info_ = "" ;
      ObSysVars[33].name_ = "sql_auto_is_null" ;
      ObSysVars[33].data_type_ = ObIntType ;
      ObSysVars[33].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[33].id_ = SYS_VAR_SQL_AUTO_IS_NULL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_AUTO_IS_NULL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_AUTO_IS_NULL] = 33 ;
      ObSysVars[33].base_value_ = "0" ;
    ObSysVars[33].alias_ = "OB_SV_SQL_AUTO_IS_NULL" ;
    }();

    [&] (){
      ObSysVars[34].default_value_ = "9223372036854775807" ;
      ObSysVars[34].info_ = "" ;
      ObSysVars[34].name_ = "sql_select_limit" ;
      ObSysVars[34].data_type_ = ObIntType ;
      ObSysVars[34].min_val_ = "0" ;
      ObSysVars[34].max_val_ = "9223372036854775807" ;
      ObSysVars[34].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[34].id_ = SYS_VAR_SQL_SELECT_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_SELECT_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_SELECT_LIMIT] = 34 ;
      ObSysVars[34].base_value_ = "9223372036854775807" ;
    ObSysVars[34].alias_ = "OB_SV_SQL_SELECT_LIMIT" ;
    }();

    [&] (){
      ObSysVars[35].default_value_ = "0" ;
      ObSysVars[35].info_ = "" ;
      ObSysVars[35].name_ = "timestamp" ;
      ObSysVars[35].data_type_ = ObNumberType ;
      ObSysVars[35].min_val_ = "0" ;
      ObSysVars[35].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[35].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_max_min_timestamp" ;
      ObSysVars[35].id_ = SYS_VAR_TIMESTAMP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TIMESTAMP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TIMESTAMP] = 35 ;
      ObSysVars[35].base_value_ = "0" ;
    ObSysVars[35].alias_ = "OB_SV_TIMESTAMP" ;
    }();

    [&] (){
      ObSysVars[36].default_value_ = "0" ;
      ObSysVars[36].info_ = "" ;
      ObSysVars[36].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only" ;
      ObSysVars[36].name_ = "tx_read_only" ;
      ObSysVars[36].data_type_ = ObIntType ;
      ObSysVars[36].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[36].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope" ;
      ObSysVars[36].base_class_ = "ObSessionSpecialBoolSysVar" ;
      ObSysVars[36].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_read_only" ;
      ObSysVars[36].id_ = SYS_VAR_TX_READ_ONLY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TX_READ_ONLY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TX_READ_ONLY] = 36 ;
      ObSysVars[36].base_value_ = "0" ;
    ObSysVars[36].alias_ = "OB_SV_TX_READ_ONLY" ;
    }();

    [&] (){
      ObSysVars[37].default_value_ = "" ;
      ObSysVars[37].info_ = "" ;
      ObSysVars[37].name_ = "version" ;
      ObSysVars[37].data_type_ = ObVarcharType ;
      ObSysVars[37].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[37].id_ = SYS_VAR_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VERSION] = 37 ;
      ObSysVars[37].base_value_ = "" ;
    ObSysVars[37].alias_ = "OB_SV_VERSION" ;
    }();

    [&] (){
      ObSysVars[38].default_value_ = "0" ;
      ObSysVars[38].info_ = "" ;
      ObSysVars[38].name_ = "sql_warnings" ;
      ObSysVars[38].data_type_ = ObIntType ;
      ObSysVars[38].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[38].id_ = SYS_VAR_SQL_WARNINGS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_WARNINGS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_WARNINGS] = 38 ;
      ObSysVars[38].base_value_ = "0" ;
    ObSysVars[38].alias_ = "OB_SV_SQL_WARNINGS" ;
    }();

    [&] (){
      ObSysVars[39].default_value_ = "0" ;
      ObSysVars[39].info_ = "" ;
      ObSysVars[39].name_ = "max_user_connections" ;
      ObSysVars[39].data_type_ = ObUInt64Type ;
      ObSysVars[39].min_val_ = "0" ;
      ObSysVars[39].max_val_ = "4294967295" ;
      ObSysVars[39].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::SESSION_READONLY ;
      ObSysVars[39].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_max_user_connections" ;
      ObSysVars[39].id_ = SYS_VAR_MAX_USER_CONNECTIONS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_USER_CONNECTIONS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_USER_CONNECTIONS] = 39 ;
      ObSysVars[39].base_value_ = "0" ;
    ObSysVars[39].alias_ = "OB_SV_MAX_USER_CONNECTIONS" ;
    }();

    [&] (){
      ObSysVars[40].default_value_ = "" ;
      ObSysVars[40].info_ = "" ;
      ObSysVars[40].name_ = "init_connect" ;
      ObSysVars[40].data_type_ = ObVarcharType ;
      ObSysVars[40].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[40].id_ = SYS_VAR_INIT_CONNECT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INIT_CONNECT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INIT_CONNECT] = 40 ;
      ObSysVars[40].base_value_ = "" ;
    ObSysVars[40].alias_ = "OB_SV_INIT_CONNECT" ;
    }();

    [&] (){
      ObSysVars[41].default_value_ = "" ;
      ObSysVars[41].info_ = "" ;
      ObSysVars[41].name_ = "license" ;
      ObSysVars[41].data_type_ = ObVarcharType ;
      ObSysVars[41].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[41].id_ = SYS_VAR_LICENSE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LICENSE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LICENSE] = 41 ;
      ObSysVars[41].base_value_ = "" ;
    ObSysVars[41].alias_ = "OB_SV_LICENSE" ;
    }();

    [&] (){
      ObSysVars[42].default_value_ = "16384" ;
      ObSysVars[42].info_ = "Buffer length for TCP/IP and socket communication" ;
      ObSysVars[42].name_ = "net_buffer_length" ;
      ObSysVars[42].data_type_ = ObIntType ;
      ObSysVars[42].min_val_ = "1024" ;
      ObSysVars[42].max_val_ = "1048576" ;
      ObSysVars[42].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::SESSION_READONLY ;
      ObSysVars[42].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_net_buffer_length" ;
      ObSysVars[42].id_ = SYS_VAR_NET_BUFFER_LENGTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NET_BUFFER_LENGTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NET_BUFFER_LENGTH] = 42 ;
      ObSysVars[42].base_value_ = "16384" ;
    ObSysVars[42].alias_ = "OB_SV_NET_BUFFER_LENGTH" ;
    }();

    [&] (){
      ObSysVars[43].default_value_ = "CST" ;
      ObSysVars[43].info_ = "The server system time zone" ;
      ObSysVars[43].name_ = "system_time_zone" ;
      ObSysVars[43].data_type_ = ObVarcharType ;
      ObSysVars[43].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[43].id_ = SYS_VAR_SYSTEM_TIME_ZONE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SYSTEM_TIME_ZONE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SYSTEM_TIME_ZONE] = 43 ;
      ObSysVars[43].base_value_ = "CST" ;
    ObSysVars[43].alias_ = "OB_SV_SYSTEM_TIME_ZONE" ;
    }();

    [&] (){
      ObSysVars[44].default_value_ = "0" ;
      ObSysVars[44].info_ = "The memory allocated to store results from old queries(not used yet)" ;
      ObSysVars[44].name_ = "query_cache_size" ;
      ObSysVars[44].data_type_ = ObUInt64Type ;
      ObSysVars[44].min_val_ = "0" ;
      ObSysVars[44].max_val_ = "18446744073709551615" ;
      ObSysVars[44].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[44].id_ = SYS_VAR_QUERY_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_CACHE_SIZE] = 44 ;
      ObSysVars[44].base_value_ = "0" ;
    ObSysVars[44].alias_ = "OB_SV_QUERY_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[45].default_value_ = "0" ;
      ObSysVars[45].info_ = "OFF = Do not cache or retrieve results. ON = Cache all results except SELECT SQL_NO_CACHE ... queries. DEMAND = Cache only SELECT SQL_CACHE ... queries(not used yet)" ;
      ObSysVars[45].name_ = "query_cache_type" ;
      ObSysVars[45].data_type_ = ObIntType ;
      ObSysVars[45].enum_names_ = "[u'OFF', u'ON', u'DEMAND']" ;
      ObSysVars[45].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[45].id_ = SYS_VAR_QUERY_CACHE_TYPE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_CACHE_TYPE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_CACHE_TYPE] = 45 ;
      ObSysVars[45].base_value_ = "0" ;
    ObSysVars[45].alias_ = "OB_SV_QUERY_CACHE_TYPE" ;
    }();

    [&] (){
      ObSysVars[46].default_value_ = "1" ;
      ObSysVars[46].info_ = "" ;
      ObSysVars[46].name_ = "sql_quote_show_create" ;
      ObSysVars[46].data_type_ = ObIntType ;
      ObSysVars[46].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[46].id_ = SYS_VAR_SQL_QUOTE_SHOW_CREATE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_QUOTE_SHOW_CREATE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_QUOTE_SHOW_CREATE] = 46 ;
      ObSysVars[46].base_value_ = "1" ;
    ObSysVars[46].alias_ = "OB_SV_SQL_QUOTE_SHOW_CREATE" ;
    }();

    [&] (){
      ObSysVars[47].default_value_ = "0" ;
      ObSysVars[47].info_ = "The number of times that any given stored procedure may be called recursively." ;
      ObSysVars[47].name_ = "max_sp_recursion_depth" ;
      ObSysVars[47].data_type_ = ObIntType ;
      ObSysVars[47].min_val_ = "0" ;
      ObSysVars[47].max_val_ = "255" ;
      ObSysVars[47].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[47].id_ = SYS_VAR_MAX_SP_RECURSION_DEPTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_SP_RECURSION_DEPTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_SP_RECURSION_DEPTH] = 47 ;
      ObSysVars[47].base_value_ = "0" ;
    ObSysVars[47].alias_ = "OB_SV_MAX_SP_RECURSION_DEPTH" ;
    }();

    [&] (){
      ObSysVars[48].default_value_ = "0" ;
      ObSysVars[48].info_ = "enable mysql sql safe updates" ;
      ObSysVars[48].name_ = "sql_safe_updates" ;
      ObSysVars[48].data_type_ = ObIntType ;
      ObSysVars[48].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[48].id_ = SYS_VAR_SQL_SAFE_UPDATES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_SAFE_UPDATES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_SAFE_UPDATES] = 48 ;
      ObSysVars[48].base_value_ = "0" ;
    ObSysVars[48].alias_ = "OB_SV_SQL_SAFE_UPDATES" ;
    }();

    [&] (){
      ObSysVars[49].default_value_ = "AUTO" ;
      ObSysVars[49].info_ = "" ;
      ObSysVars[49].name_ = "concurrent_insert" ;
      ObSysVars[49].data_type_ = ObVarcharType ;
      ObSysVars[49].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[49].id_ = SYS_VAR_CONCURRENT_INSERT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CONCURRENT_INSERT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CONCURRENT_INSERT] = 49 ;
      ObSysVars[49].base_value_ = "AUTO" ;
    ObSysVars[49].alias_ = "OB_SV_CONCURRENT_INSERT" ;
    }();

    [&] (){
      ObSysVars[50].default_value_ = "mysql_native_password" ;
      ObSysVars[50].info_ = "" ;
      ObSysVars[50].name_ = "default_authentication_plugin" ;
      ObSysVars[50].data_type_ = ObVarcharType ;
      ObSysVars[50].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[50].id_ = SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN] = 50 ;
      ObSysVars[50].base_value_ = "mysql_native_password" ;
    ObSysVars[50].alias_ = "OB_SV_DEFAULT_AUTHENTICATION_PLUGIN" ;
    }();

    [&] (){
      ObSysVars[51].default_value_ = "" ;
      ObSysVars[51].info_ = "" ;
      ObSysVars[51].name_ = "disabled_storage_engines" ;
      ObSysVars[51].data_type_ = ObVarcharType ;
      ObSysVars[51].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[51].id_ = SYS_VAR_DISABLED_STORAGE_ENGINES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DISABLED_STORAGE_ENGINES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DISABLED_STORAGE_ENGINES] = 51 ;
      ObSysVars[51].base_value_ = "" ;
    ObSysVars[51].alias_ = "OB_SV_DISABLED_STORAGE_ENGINES" ;
    }();

    [&] (){
      ObSysVars[52].default_value_ = "0" ;
      ObSysVars[52].info_ = "" ;
      ObSysVars[52].name_ = "error_count" ;
      ObSysVars[52].data_type_ = ObUInt64Type ;
      ObSysVars[52].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[52].id_ = SYS_VAR_ERROR_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_ERROR_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_ERROR_COUNT] = 52 ;
      ObSysVars[52].base_value_ = "0" ;
    ObSysVars[52].alias_ = "OB_SV_ERROR_COUNT" ;
    }();

    [&] (){
      ObSysVars[53].default_value_ = "0" ;
      ObSysVars[53].info_ = "" ;
      ObSysVars[53].name_ = "general_log" ;
      ObSysVars[53].data_type_ = ObIntType ;
      ObSysVars[53].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[53].id_ = SYS_VAR_GENERAL_LOG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GENERAL_LOG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GENERAL_LOG] = 53 ;
      ObSysVars[53].base_value_ = "0" ;
    ObSysVars[53].alias_ = "OB_SV_GENERAL_LOG" ;
    }();

    [&] (){
      ObSysVars[54].default_value_ = "YES" ;
      ObSysVars[54].info_ = "" ;
      ObSysVars[54].name_ = "have_openssl" ;
      ObSysVars[54].data_type_ = ObVarcharType ;
      ObSysVars[54].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[54].id_ = SYS_VAR_HAVE_OPENSSL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_OPENSSL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_OPENSSL] = 54 ;
      ObSysVars[54].base_value_ = "YES" ;
    ObSysVars[54].alias_ = "OB_SV_HAVE_OPENSSL" ;
    }();

    [&] (){
      ObSysVars[55].default_value_ = "NO" ;
      ObSysVars[55].info_ = "" ;
      ObSysVars[55].name_ = "have_profiling" ;
      ObSysVars[55].data_type_ = ObVarcharType ;
      ObSysVars[55].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[55].id_ = SYS_VAR_HAVE_PROFILING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_PROFILING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_PROFILING] = 55 ;
      ObSysVars[55].base_value_ = "YES" ;
    ObSysVars[55].alias_ = "OB_SV_HAVE_PROFILING" ;
    }();

    [&] (){
      ObSysVars[56].default_value_ = "YES" ;
      ObSysVars[56].info_ = "" ;
      ObSysVars[56].name_ = "have_ssl" ;
      ObSysVars[56].data_type_ = ObVarcharType ;
      ObSysVars[56].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[56].id_ = SYS_VAR_HAVE_SSL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_SSL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_SSL] = 56 ;
      ObSysVars[56].base_value_ = "YES" ;
    ObSysVars[56].alias_ = "OB_SV_HAVE_SSL" ;
    }();

    [&] (){
      ObSysVars[57].default_value_ = "" ;
      ObSysVars[57].info_ = "" ;
      ObSysVars[57].name_ = "hostname" ;
      ObSysVars[57].data_type_ = ObVarcharType ;
      ObSysVars[57].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[57].id_ = SYS_VAR_HOSTNAME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HOSTNAME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HOSTNAME] = 57 ;
      ObSysVars[57].base_value_ = "" ;
    ObSysVars[57].alias_ = "OB_SV_HOSTNAME" ;
    }();

    [&] (){
      ObSysVars[58].default_value_ = "en_US" ;
      ObSysVars[58].info_ = "" ;
      ObSysVars[58].name_ = "lc_messages" ;
      ObSysVars[58].data_type_ = ObVarcharType ;
      ObSysVars[58].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[58].id_ = SYS_VAR_LC_MESSAGES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LC_MESSAGES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LC_MESSAGES] = 58 ;
      ObSysVars[58].base_value_ = "en_US" ;
    ObSysVars[58].alias_ = "OB_SV_LC_MESSAGES" ;
    }();

    [&] (){
      ObSysVars[59].default_value_ = "1" ;
      ObSysVars[59].info_ = "" ;
      ObSysVars[59].name_ = "local_infile" ;
      ObSysVars[59].data_type_ = ObIntType ;
      ObSysVars[59].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[59].id_ = SYS_VAR_LOCAL_INFILE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOCAL_INFILE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOCAL_INFILE] = 59 ;
      ObSysVars[59].base_value_ = "1" ;
    ObSysVars[59].alias_ = "OB_SV_LOCAL_INFILE" ;
    }();

    [&] (){
      ObSysVars[60].default_value_ = "31536000" ;
      ObSysVars[60].info_ = "" ;
      ObSysVars[60].name_ = "lock_wait_timeout" ;
      ObSysVars[60].data_type_ = ObIntType ;
      ObSysVars[60].min_val_ = "1" ;
      ObSysVars[60].max_val_ = "31536000" ;
      ObSysVars[60].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[60].id_ = SYS_VAR_LOCK_WAIT_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOCK_WAIT_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOCK_WAIT_TIMEOUT] = 60 ;
      ObSysVars[60].base_value_ = "31536000" ;
    ObSysVars[60].alias_ = "OB_SV_LOCK_WAIT_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[61].default_value_ = "10" ;
      ObSysVars[61].info_ = "" ;
      ObSysVars[61].name_ = "long_query_time" ;
      ObSysVars[61].data_type_ = ObNumberType ;
      ObSysVars[61].min_val_ = "0" ;
      ObSysVars[61].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[61].id_ = SYS_VAR_LONG_QUERY_TIME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LONG_QUERY_TIME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LONG_QUERY_TIME] = 61 ;
      ObSysVars[61].base_value_ = "10" ;
    ObSysVars[61].alias_ = "OB_SV_LONG_QUERY_TIME" ;
    }();

    [&] (){
      ObSysVars[62].default_value_ = "2147483647" ;
      ObSysVars[62].info_ = "" ;
      ObSysVars[62].name_ = "max_connections" ;
      ObSysVars[62].data_type_ = ObUInt64Type ;
      ObSysVars[62].min_val_ = "1" ;
      ObSysVars[62].max_val_ = "2147483647" ;
      ObSysVars[62].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[62].id_ = SYS_VAR_MAX_CONNECTIONS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_CONNECTIONS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_CONNECTIONS] = 62 ;
      ObSysVars[62].base_value_ = "2147483647" ;
    ObSysVars[62].alias_ = "OB_SV_MAX_CONNECTIONS" ;
    }();

    [&] (){
      ObSysVars[63].default_value_ = "0" ;
      ObSysVars[63].info_ = "" ;
      ObSysVars[63].name_ = "max_execution_time" ;
      ObSysVars[63].data_type_ = ObIntType ;
      ObSysVars[63].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[63].id_ = SYS_VAR_MAX_EXECUTION_TIME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_EXECUTION_TIME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_EXECUTION_TIME] = 63 ;
      ObSysVars[63].base_value_ = "0" ;
    ObSysVars[63].alias_ = "OB_SV_MAX_EXECUTION_TIME" ;
    }();

    [&] (){
      ObSysVars[64].default_value_ = "10" ;
      ObSysVars[64].info_ = "" ;
      ObSysVars[64].name_ = "protocol_version" ;
      ObSysVars[64].data_type_ = ObIntType ;
      ObSysVars[64].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[64].id_ = SYS_VAR_PROTOCOL_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PROTOCOL_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PROTOCOL_VERSION] = 64 ;
      ObSysVars[64].base_value_ = "10" ;
    ObSysVars[64].alias_ = "OB_SV_PROTOCOL_VERSION" ;
    }();

    [&] (){
      ObSysVars[65].default_value_ = "1" ;
      ObSysVars[65].info_ = "This variable specifies the server ID(not used yet, only sys var compatible)" ;
      ObSysVars[65].name_ = "server_id" ;
      ObSysVars[65].data_type_ = ObIntType ;
      ObSysVars[65].min_val_ = "0" ;
      ObSysVars[65].max_val_ = "4294967295" ;
      ObSysVars[65].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[65].id_ = SYS_VAR_SERVER_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SERVER_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SERVER_ID] = 65 ;
      ObSysVars[65].base_value_ = "1" ;
    ObSysVars[65].alias_ = "OB_SV_SERVER_ID" ;
    }();

    [&] (){
      ObSysVars[66].default_value_ = "" ;
      ObSysVars[66].info_ = "" ;
      ObSysVars[66].name_ = "ssl_ca" ;
      ObSysVars[66].data_type_ = ObVarcharType ;
      ObSysVars[66].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[66].id_ = SYS_VAR_SSL_CA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CA] = 66 ;
      ObSysVars[66].base_value_ = "" ;
    ObSysVars[66].alias_ = "OB_SV_SSL_CA" ;
    }();

    [&] (){
      ObSysVars[67].default_value_ = "" ;
      ObSysVars[67].info_ = "" ;
      ObSysVars[67].name_ = "ssl_capath" ;
      ObSysVars[67].data_type_ = ObVarcharType ;
      ObSysVars[67].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[67].id_ = SYS_VAR_SSL_CAPATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CAPATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CAPATH] = 67 ;
      ObSysVars[67].base_value_ = "" ;
    ObSysVars[67].alias_ = "OB_SV_SSL_CAPATH" ;
    }();

    [&] (){
      ObSysVars[68].default_value_ = "" ;
      ObSysVars[68].info_ = "" ;
      ObSysVars[68].name_ = "ssl_cert" ;
      ObSysVars[68].data_type_ = ObVarcharType ;
      ObSysVars[68].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[68].id_ = SYS_VAR_SSL_CERT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CERT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CERT] = 68 ;
      ObSysVars[68].base_value_ = "" ;
    ObSysVars[68].alias_ = "OB_SV_SSL_CERT" ;
    }();

    [&] (){
      ObSysVars[69].default_value_ = "" ;
      ObSysVars[69].info_ = "" ;
      ObSysVars[69].name_ = "ssl_cipher" ;
      ObSysVars[69].data_type_ = ObVarcharType ;
      ObSysVars[69].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[69].id_ = SYS_VAR_SSL_CIPHER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CIPHER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CIPHER] = 69 ;
      ObSysVars[69].base_value_ = "" ;
    ObSysVars[69].alias_ = "OB_SV_SSL_CIPHER" ;
    }();

    [&] (){
      ObSysVars[70].default_value_ = "" ;
      ObSysVars[70].info_ = "" ;
      ObSysVars[70].name_ = "ssl_crl" ;
      ObSysVars[70].data_type_ = ObVarcharType ;
      ObSysVars[70].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[70].id_ = SYS_VAR_SSL_CRL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CRL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CRL] = 70 ;
      ObSysVars[70].base_value_ = "" ;
    ObSysVars[70].alias_ = "OB_SV_SSL_CRL" ;
    }();

    [&] (){
      ObSysVars[71].default_value_ = "" ;
      ObSysVars[71].info_ = "" ;
      ObSysVars[71].name_ = "ssl_crlpath" ;
      ObSysVars[71].data_type_ = ObVarcharType ;
      ObSysVars[71].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[71].id_ = SYS_VAR_SSL_CRLPATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CRLPATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CRLPATH] = 71 ;
      ObSysVars[71].base_value_ = "" ;
    ObSysVars[71].alias_ = "OB_SV_SSL_CRLPATH" ;
    }();

    [&] (){
      ObSysVars[72].default_value_ = "" ;
      ObSysVars[72].info_ = "" ;
      ObSysVars[72].name_ = "ssl_key" ;
      ObSysVars[72].data_type_ = ObVarcharType ;
      ObSysVars[72].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[72].id_ = SYS_VAR_SSL_KEY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_KEY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SSL_KEY] = 72 ;
      ObSysVars[72].base_value_ = "" ;
    ObSysVars[72].alias_ = "OB_SV_SSL_KEY" ;
    }();

    [&] (){
      ObSysVars[73].default_value_ = "%H:%i:%s" ;
      ObSysVars[73].info_ = "" ;
      ObSysVars[73].name_ = "time_format" ;
      ObSysVars[73].data_type_ = ObVarcharType ;
      ObSysVars[73].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[73].id_ = SYS_VAR_TIME_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TIME_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TIME_FORMAT] = 73 ;
      ObSysVars[73].base_value_ = "%H:%i:%s" ;
    ObSysVars[73].alias_ = "OB_SV_TIME_FORMAT" ;
    }();

    [&] (){
      ObSysVars[74].default_value_ = "" ;
      ObSysVars[74].info_ = "TLSv1,TLSv1.1,TLSv1.2" ;
      ObSysVars[74].name_ = "tls_version" ;
      ObSysVars[74].data_type_ = ObVarcharType ;
      ObSysVars[74].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[74].id_ = SYS_VAR_TLS_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TLS_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TLS_VERSION] = 74 ;
      ObSysVars[74].base_value_ = "" ;
    ObSysVars[74].alias_ = "OB_SV_TLS_VERSION" ;
    }();

    [&] (){
      ObSysVars[75].default_value_ = "16777216" ;
      ObSysVars[75].info_ = "" ;
      ObSysVars[75].name_ = "tmp_table_size" ;
      ObSysVars[75].data_type_ = ObUInt64Type ;
      ObSysVars[75].min_val_ = "1024" ;
      ObSysVars[75].max_val_ = "18446744073709551615" ;
      ObSysVars[75].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[75].id_ = SYS_VAR_TMP_TABLE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TMP_TABLE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TMP_TABLE_SIZE] = 75 ;
      ObSysVars[75].base_value_ = "16777216" ;
    ObSysVars[75].alias_ = "OB_SV_TMP_TABLE_SIZE" ;
    }();

    [&] (){
      ObSysVars[76].default_value_ = "" ;
      ObSysVars[76].info_ = "" ;
      ObSysVars[76].name_ = "tmpdir" ;
      ObSysVars[76].data_type_ = ObVarcharType ;
      ObSysVars[76].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[76].id_ = SYS_VAR_TMPDIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TMPDIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TMPDIR] = 76 ;
      ObSysVars[76].base_value_ = "" ;
    ObSysVars[76].alias_ = "OB_SV_TMPDIR" ;
    }();

    [&] (){
      ObSysVars[77].default_value_ = "1" ;
      ObSysVars[77].info_ = "" ;
      ObSysVars[77].name_ = "unique_checks" ;
      ObSysVars[77].data_type_ = ObIntType ;
      ObSysVars[77].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[77].id_ = SYS_VAR_UNIQUE_CHECKS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_UNIQUE_CHECKS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_UNIQUE_CHECKS] = 77 ;
      ObSysVars[77].base_value_ = "1" ;
    ObSysVars[77].alias_ = "OB_SV_UNIQUE_CHECKS" ;
    }();

    [&] (){
      ObSysVars[78].default_value_ = "" ;
      ObSysVars[78].info_ = "" ;
      ObSysVars[78].name_ = "version_compile_machine" ;
      ObSysVars[78].data_type_ = ObVarcharType ;
      ObSysVars[78].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[78].id_ = SYS_VAR_VERSION_COMPILE_MACHINE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION_COMPILE_MACHINE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VERSION_COMPILE_MACHINE] = 78 ;
      ObSysVars[78].base_value_ = "" ;
    ObSysVars[78].alias_ = "OB_SV_VERSION_COMPILE_MACHINE" ;
    }();

    [&] (){
      ObSysVars[79].default_value_ = "" ;
      ObSysVars[79].info_ = "" ;
      ObSysVars[79].name_ = "version_compile_os" ;
      ObSysVars[79].data_type_ = ObVarcharType ;
      ObSysVars[79].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[79].id_ = SYS_VAR_VERSION_COMPILE_OS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION_COMPILE_OS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VERSION_COMPILE_OS] = 79 ;
      ObSysVars[79].base_value_ = "" ;
    ObSysVars[79].alias_ = "OB_SV_VERSION_COMPILE_OS" ;
    }();

    [&] (){
      ObSysVars[80].default_value_ = "0" ;
      ObSysVars[80].info_ = "" ;
      ObSysVars[80].name_ = "warning_count" ;
      ObSysVars[80].data_type_ = ObUInt64Type ;
      ObSysVars[80].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[80].id_ = SYS_VAR_WARNING_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_WARNING_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_WARNING_COUNT] = 80 ;
      ObSysVars[80].base_value_ = "0" ;
    ObSysVars[80].alias_ = "OB_SV_WARNING_COUNT" ;
    }();

    [&] (){
      ObSysVars[81].default_value_ = "1" ;
      ObSysVars[81].info_ = "specifies whether return schema change info in ok packet" ;
      ObSysVars[81].name_ = "session_track_schema" ;
      ObSysVars[81].data_type_ = ObIntType ;
      ObSysVars[81].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[81].id_ = SYS_VAR_SESSION_TRACK_SCHEMA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SESSION_TRACK_SCHEMA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SESSION_TRACK_SCHEMA] = 81 ;
      ObSysVars[81].base_value_ = "1" ;
    ObSysVars[81].alias_ = "OB_SV_SESSION_TRACK_SCHEMA" ;
    }();

    [&] (){
      ObSysVars[82].default_value_ = "time_zone, autocommit, character_set_client, character_set_results, character_set_connection" ;
      ObSysVars[82].info_ = "specifies whether return system variables change info in ok packet" ;
      ObSysVars[82].name_ = "session_track_system_variables" ;
      ObSysVars[82].data_type_ = ObVarcharType ;
      ObSysVars[82].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[82].id_ = SYS_VAR_SESSION_TRACK_SYSTEM_VARIABLES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SESSION_TRACK_SYSTEM_VARIABLES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SESSION_TRACK_SYSTEM_VARIABLES] = 82 ;
      ObSysVars[82].base_value_ = "time_zone, autocommit, character_set_client, character_set_results, character_set_connection" ;
    ObSysVars[82].alias_ = "OB_SV_SESSION_TRACK_SYSTEM_VARIABLES" ;
    }();

    [&] (){
      ObSysVars[83].default_value_ = "0" ;
      ObSysVars[83].info_ = "specifies whether return session state change info in ok packet" ;
      ObSysVars[83].name_ = "session_track_state_change" ;
      ObSysVars[83].data_type_ = ObIntType ;
      ObSysVars[83].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[83].id_ = SYS_VAR_SESSION_TRACK_STATE_CHANGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SESSION_TRACK_STATE_CHANGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SESSION_TRACK_STATE_CHANGE] = 83 ;
      ObSysVars[83].base_value_ = "0" ;
    ObSysVars[83].alias_ = "OB_SV_SESSION_TRACK_STATE_CHANGE" ;
    }();

    [&] (){
      ObSysVars[84].default_value_ = "NO" ;
      ObSysVars[84].info_ = "Whether to have query cache or not(not used yet, only compatible)" ;
      ObSysVars[84].name_ = "have_query_cache" ;
      ObSysVars[84].data_type_ = ObVarcharType ;
      ObSysVars[84].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[84].id_ = SYS_VAR_HAVE_QUERY_CACHE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_QUERY_CACHE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_QUERY_CACHE] = 84 ;
      ObSysVars[84].base_value_ = "NO" ;
    ObSysVars[84].alias_ = "OB_SV_HAVE_QUERY_CACHE" ;
    }();

    [&] (){
      ObSysVars[85].default_value_ = "0" ;
      ObSysVars[85].info_ = "The maximum query result set that can be cached by the query cache(not used yet, only sys var compatible)" ;
      ObSysVars[85].name_ = "query_cache_limit" ;
      ObSysVars[85].data_type_ = ObUInt64Type ;
      ObSysVars[85].min_val_ = "0" ;
      ObSysVars[85].max_val_ = "18446744073709551615" ;
      ObSysVars[85].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[85].id_ = SYS_VAR_QUERY_CACHE_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_CACHE_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_CACHE_LIMIT] = 85 ;
      ObSysVars[85].base_value_ = "0" ;
    ObSysVars[85].alias_ = "OB_SV_QUERY_CACHE_LIMIT" ;
    }();

    [&] (){
      ObSysVars[86].default_value_ = "0" ;
      ObSysVars[86].info_ = "The smallest unit of memory allocated by the query cache(not used yet, only sys var compatible)" ;
      ObSysVars[86].name_ = "query_cache_min_res_unit" ;
      ObSysVars[86].data_type_ = ObUInt64Type ;
      ObSysVars[86].min_val_ = "0" ;
      ObSysVars[86].max_val_ = "18446744073709551615" ;
      ObSysVars[86].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[86].id_ = SYS_VAR_QUERY_CACHE_MIN_RES_UNIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_CACHE_MIN_RES_UNIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_CACHE_MIN_RES_UNIT] = 86 ;
      ObSysVars[86].base_value_ = "0" ;
    ObSysVars[86].alias_ = "OB_SV_QUERY_CACHE_MIN_RES_UNIT" ;
    }();

    [&] (){
      ObSysVars[87].default_value_ = "0" ;
      ObSysVars[87].info_ = "query cache wirte lock for MyISAM engine (not used yet, only sys var compatible)" ;
      ObSysVars[87].name_ = "query_cache_wlock_invalidate" ;
      ObSysVars[87].data_type_ = ObIntType ;
      ObSysVars[87].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[87].id_ = SYS_VAR_QUERY_CACHE_WLOCK_INVALIDATE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_CACHE_WLOCK_INVALIDATE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_CACHE_WLOCK_INVALIDATE] = 87 ;
      ObSysVars[87].base_value_ = "0" ;
    ObSysVars[87].alias_ = "OB_SV_QUERY_CACHE_WLOCK_INVALIDATE" ;
    }();

    [&] (){
      ObSysVars[88].default_value_ = "2" ;
      ObSysVars[88].info_ = "set the binary logging format(not used yet, only sys var compatible)" ;
      ObSysVars[88].name_ = "binlog_format" ;
      ObSysVars[88].data_type_ = ObIntType ;
      ObSysVars[88].enum_names_ = "[u'MIXED', u'STATEMENT', u'ROW']" ;
      ObSysVars[88].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[88].id_ = SYS_VAR_BINLOG_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_FORMAT] = 88 ;
      ObSysVars[88].base_value_ = "2" ;
    ObSysVars[88].alias_ = "OB_SV_BINLOG_FORMAT" ;
    }();

    [&] (){
      ObSysVars[89].default_value_ = "CRC32" ;
      ObSysVars[89].info_ = "this variable causes the source to write a checksum for each event in the binary log(not used yet, only sys var compatible)" ;
      ObSysVars[89].name_ = "binlog_checksum" ;
      ObSysVars[89].data_type_ = ObVarcharType ;
      ObSysVars[89].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[89].id_ = SYS_VAR_BINLOG_CHECKSUM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_CHECKSUM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_CHECKSUM] = 89 ;
      ObSysVars[89].base_value_ = "CRC32" ;
    ObSysVars[89].alias_ = "OB_SV_BINLOG_CHECKSUM" ;
    }();

    [&] (){
      ObSysVars[90].default_value_ = "0" ;
      ObSysVars[90].info_ = "This system variable affects row-based logging only(not used yet, only sys var compatible)" ;
      ObSysVars[90].name_ = "binlog_rows_query_log_events" ;
      ObSysVars[90].data_type_ = ObIntType ;
      ObSysVars[90].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[90].id_ = SYS_VAR_BINLOG_ROWS_QUERY_LOG_EVENTS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_ROWS_QUERY_LOG_EVENTS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_ROWS_QUERY_LOG_EVENTS] = 90 ;
      ObSysVars[90].base_value_ = "0" ;
    ObSysVars[90].alias_ = "OB_SV_BINLOG_ROWS_QUERY_LOG_EVENTS" ;
    }();

    [&] (){
      ObSysVars[91].default_value_ = "1" ;
      ObSysVars[91].info_ = "This variable reports only on the status of binary logging(not used yet, only sys var compatible)" ;
      ObSysVars[91].name_ = "log_bin" ;
      ObSysVars[91].data_type_ = ObIntType ;
      ObSysVars[91].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[91].id_ = SYS_VAR_LOG_BIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOG_BIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOG_BIN] = 91 ;
      ObSysVars[91].base_value_ = "1" ;
    ObSysVars[91].alias_ = "OB_SV_LOG_BIN" ;
    }();

    [&] (){
      ObSysVars[92].default_value_ = "ObExprUuid::gen_server_uuid" ;
      ObSysVars[92].info_ = "server uuid" ;
      ObSysVars[92].name_ = "server_uuid" ;
      ObSysVars[92].data_type_ = ObVarcharType ;
      ObSysVars[92].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[92].id_ = SYS_VAR_SERVER_UUID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SERVER_UUID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SERVER_UUID] = 92 ;
      ObSysVars[92].base_value_ = "ObExprUuid::gen_server_uuid" ;
    ObSysVars[92].alias_ = "OB_SV_SERVER_UUID" ;
    }();

    [&] (){
      ObSysVars[93].default_value_ = "OceanBase" ;
      ObSysVars[93].info_ = "The default storage engine of OceanBase" ;
      ObSysVars[93].name_ = "default_storage_engine" ;
      ObSysVars[93].data_type_ = ObVarcharType ;
      ObSysVars[93].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[93].id_ = SYS_VAR_DEFAULT_STORAGE_ENGINE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_STORAGE_ENGINE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_STORAGE_ENGINE] = 93 ;
      ObSysVars[93].base_value_ = "OceanBase" ;
    ObSysVars[93].alias_ = "OB_SV_DEFAULT_STORAGE_ENGINE" ;
    }();

    [&] (){
      ObSysVars[94].default_value_ = "1000" ;
      ObSysVars[94].info_ = "Abort a recursive common table expression if it does more than this number of iterations." ;
      ObSysVars[94].name_ = "cte_max_recursion_depth" ;
      ObSysVars[94].data_type_ = ObUInt64Type ;
      ObSysVars[94].min_val_ = "0" ;
      ObSysVars[94].max_val_ = "4294967295" ;
      ObSysVars[94].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[94].id_ = SYS_VAR_CTE_MAX_RECURSION_DEPTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CTE_MAX_RECURSION_DEPTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CTE_MAX_RECURSION_DEPTH] = 94 ;
      ObSysVars[94].base_value_ = "1000" ;
    ObSysVars[94].alias_ = "OB_SV_CTE_MAX_RECURSION_DEPTH" ;
    }();

    [&] (){
      ObSysVars[95].default_value_ = "8000000" ;
      ObSysVars[95].info_ = "The maximum available memory in bytes for the internal stack used for regular expression matching operations" ;
      ObSysVars[95].name_ = "regexp_stack_limit" ;
      ObSysVars[95].data_type_ = ObIntType ;
      ObSysVars[95].min_val_ = "0" ;
      ObSysVars[95].max_val_ = "2147483647" ;
      ObSysVars[95].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[95].id_ = SYS_VAR_REGEXP_STACK_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_REGEXP_STACK_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_REGEXP_STACK_LIMIT] = 95 ;
      ObSysVars[95].base_value_ = "8000000" ;
    ObSysVars[95].alias_ = "OB_SV_REGEXP_STACK_LIMIT" ;
    }();

    [&] (){
      ObSysVars[96].default_value_ = "32" ;
      ObSysVars[96].info_ = "The time limit for regular expression matching operations, default unit is milliseconds" ;
      ObSysVars[96].name_ = "regexp_time_limit" ;
      ObSysVars[96].data_type_ = ObIntType ;
      ObSysVars[96].min_val_ = "0" ;
      ObSysVars[96].max_val_ = "2147483647" ;
      ObSysVars[96].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[96].id_ = SYS_VAR_REGEXP_TIME_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_REGEXP_TIME_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_REGEXP_TIME_LIMIT] = 96 ;
      ObSysVars[96].base_value_ = "32" ;
    ObSysVars[96].alias_ = "OB_SV_REGEXP_TIME_LIMIT" ;
    }();

    [&] (){
      ObSysVars[97].default_value_ = "0" ;
      ObSysVars[97].info_ = "" ;
      ObSysVars[97].name_ = "profiling" ;
      ObSysVars[97].data_type_ = ObIntType ;
      ObSysVars[97].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[97].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[97].id_ = SYS_VAR_PROFILING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PROFILING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PROFILING] = 97 ;
      ObSysVars[97].base_value_ = "0" ;
    ObSysVars[97].alias_ = "OB_SV_PROFILING" ;
    }();

    [&] (){
      ObSysVars[98].default_value_ = "15" ;
      ObSysVars[98].info_ = "" ;
      ObSysVars[98].name_ = "profiling_history_size" ;
      ObSysVars[98].data_type_ = ObIntType ;
      ObSysVars[98].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[98].id_ = SYS_VAR_PROFILING_HISTORY_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PROFILING_HISTORY_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PROFILING_HISTORY_SIZE] = 98 ;
      ObSysVars[98].base_value_ = "15" ;
    ObSysVars[98].alias_ = "OB_SV_PROFILING_HISTORY_SIZE" ;
    }();

    [&] (){
      ObSysVars[99].default_value_ = "2147483648" ;
      ObSysVars[99].info_ = "Indicate how many bytes the interm result manager can alloc most for this tenant" ;
      ObSysVars[99].name_ = "ob_interm_result_mem_limit" ;
      ObSysVars[99].data_type_ = ObIntType ;
      ObSysVars[99].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[99].id_ = SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT] = 99 ;
      ObSysVars[99].base_value_ = "2147483648" ;
    ObSysVars[99].alias_ = "OB_SV_INTERM_RESULT_MEM_LIMIT" ;
    }();

    [&] (){
      ObSysVars[100].default_value_ = "1" ;
      ObSysVars[100].info_ = "Indicate whether sql stmt hit right partition, readonly to user, modify by ob" ;
      ObSysVars[100].name_ = "ob_proxy_partition_hit" ;
      ObSysVars[100].data_type_ = ObIntType ;
      ObSysVars[100].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[100].id_ = SYS_VAR_OB_PROXY_PARTITION_HIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_PARTITION_HIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_PARTITION_HIT] = 100 ;
      ObSysVars[100].base_value_ = "1" ;
    ObSysVars[100].alias_ = "OB_SV_PROXY_PARTITION_HIT" ;
    }();

    [&] (){
      ObSysVars[101].default_value_ = "disabled" ;
      ObSysVars[101].info_ = "log level in session" ;
      ObSysVars[101].name_ = "ob_log_level" ;
      ObSysVars[101].data_type_ = ObVarcharType ;
      ObSysVars[101].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[101].id_ = SYS_VAR_OB_LOG_LEVEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_LOG_LEVEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_LOG_LEVEL] = 101 ;
      ObSysVars[101].base_value_ = "disabled" ;
    ObSysVars[101].alias_ = "OB_SV_LOG_LEVEL" ;
    }();

    [&] (){
      ObSysVars[102].default_value_ = "10000000" ;
      ObSysVars[102].info_ = "Query timeout in microsecond(us)" ;
      ObSysVars[102].name_ = "ob_query_timeout" ;
      ObSysVars[102].data_type_ = ObIntType ;
      ObSysVars[102].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[102].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large" ;
      ObSysVars[102].id_ = SYS_VAR_OB_QUERY_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_QUERY_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_QUERY_TIMEOUT] = 102 ;
      ObSysVars[102].base_value_ = "10000000" ;
    ObSysVars[102].alias_ = "OB_SV_QUERY_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[103].default_value_ = "3" ;
      ObSysVars[103].info_ = "read consistency level: 3=STRONG, 2=WEAK, 1=FROZEN" ;
      ObSysVars[103].name_ = "ob_read_consistency" ;
      ObSysVars[103].data_type_ = ObIntType ;
      ObSysVars[103].enum_names_ = "[u'', u'FROZEN', u'WEAK', u'STRONG']" ;
      ObSysVars[103].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[103].id_ = SYS_VAR_OB_READ_CONSISTENCY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_READ_CONSISTENCY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_READ_CONSISTENCY] = 103 ;
      ObSysVars[103].base_value_ = "3" ;
    ObSysVars[103].alias_ = "OB_SV_READ_CONSISTENCY" ;
    }();

    [&] (){
      ObSysVars[104].default_value_ = "1" ;
      ObSysVars[104].info_ = "whether use transform in session" ;
      ObSysVars[104].name_ = "ob_enable_transformation" ;
      ObSysVars[104].data_type_ = ObIntType ;
      ObSysVars[104].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[104].id_ = SYS_VAR_OB_ENABLE_TRANSFORMATION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRANSFORMATION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_TRANSFORMATION] = 104 ;
      ObSysVars[104].base_value_ = "1" ;
    ObSysVars[104].alias_ = "OB_SV_ENABLE_TRANSFORMATION" ;
    }();

    [&] (){
      ObSysVars[105].default_value_ = "86400000000" ;
      ObSysVars[105].info_ = "The max duration of one transaction" ;
      ObSysVars[105].name_ = "ob_trx_timeout" ;
      ObSysVars[105].data_type_ = ObIntType ;
      ObSysVars[105].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[105].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large" ;
      ObSysVars[105].id_ = SYS_VAR_OB_TRX_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRX_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRX_TIMEOUT] = 105 ;
      ObSysVars[105].base_value_ = "86400000000" ;
    ObSysVars[105].alias_ = "OB_SV_TRX_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[106].default_value_ = "1" ;
      ObSysVars[106].info_ = "whether use plan cache in session" ;
      ObSysVars[106].name_ = "ob_enable_plan_cache" ;
      ObSysVars[106].data_type_ = ObIntType ;
      ObSysVars[106].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[106].id_ = SYS_VAR_OB_ENABLE_PLAN_CACHE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_PLAN_CACHE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_PLAN_CACHE] = 106 ;
      ObSysVars[106].base_value_ = "1" ;
    ObSysVars[106].alias_ = "OB_SV_ENABLE_PLAN_CACHE" ;
    }();

    [&] (){
      ObSysVars[107].default_value_ = "0" ;
      ObSysVars[107].info_ = "whether can select from index table" ;
      ObSysVars[107].name_ = "ob_enable_index_direct_select" ;
      ObSysVars[107].data_type_ = ObIntType ;
      ObSysVars[107].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[107].id_ = SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT] = 107 ;
      ObSysVars[107].base_value_ = "0" ;
    ObSysVars[107].alias_ = "OB_SV_ENABLE_INDEX_DIRECT_SELECT" ;
    }();

    [&] (){
      ObSysVars[108].default_value_ = "0" ;
      ObSysVars[108].info_ = "this value is true if we have executed set transaction stmt, until a transaction commit(explicit or implicit) successfully" ;
      ObSysVars[108].name_ = "ob_proxy_set_trx_executed" ;
      ObSysVars[108].data_type_ = ObIntType ;
      ObSysVars[108].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[108].id_ = SYS_VAR_OB_PROXY_SET_TRX_EXECUTED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_SET_TRX_EXECUTED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_SET_TRX_EXECUTED] = 108 ;
      ObSysVars[108].base_value_ = "0" ;
    ObSysVars[108].alias_ = "OB_SV_PROXY_SET_TRX_EXECUTED" ;
    }();

    [&] (){
      ObSysVars[109].default_value_ = "1" ;
      ObSysVars[109].info_ = "enable aggregation function to be push-downed through exchange nodes" ;
      ObSysVars[109].name_ = "ob_enable_aggregation_pushdown" ;
      ObSysVars[109].data_type_ = ObIntType ;
      ObSysVars[109].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[109].id_ = SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN] = 109 ;
      ObSysVars[109].base_value_ = "1" ;
    ObSysVars[109].alias_ = "OB_SV_ENABLE_AGGREGATION_PUSHDOWN" ;
    }();

    [&] (){
      ObSysVars[110].default_value_ = "0" ;
      ObSysVars[110].info_ = "" ;
      ObSysVars[110].name_ = "ob_last_schema_version" ;
      ObSysVars[110].data_type_ = ObIntType ;
      ObSysVars[110].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[110].id_ = SYS_VAR_OB_LAST_SCHEMA_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_LAST_SCHEMA_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_LAST_SCHEMA_VERSION] = 110 ;
      ObSysVars[110].base_value_ = "0" ;
    ObSysVars[110].alias_ = "OB_SV_LAST_SCHEMA_VERSION" ;
    }();

    [&] (){
      ObSysVars[111].default_value_ = "" ;
      ObSysVars[111].info_ = "Global debug sync facility" ;
      ObSysVars[111].name_ = "ob_global_debug_sync" ;
      ObSysVars[111].data_type_ = ObVarcharType ;
      ObSysVars[111].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[111].id_ = SYS_VAR_OB_GLOBAL_DEBUG_SYNC ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_GLOBAL_DEBUG_SYNC)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_GLOBAL_DEBUG_SYNC] = 111 ;
      ObSysVars[111].base_value_ = "" ;
    ObSysVars[111].alias_ = "OB_SV_GLOBAL_DEBUG_SYNC" ;
    }();

    [&] (){
      ObSysVars[112].default_value_ = "0" ;
      ObSysVars[112].info_ = "this value is global variables last modified time when server session create, used for proxy to judge whether global vars has changed between two server session" ;
      ObSysVars[112].name_ = "ob_proxy_global_variables_version" ;
      ObSysVars[112].data_type_ = ObIntType ;
      ObSysVars[112].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[112].id_ = SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION] = 112 ;
      ObSysVars[112].base_value_ = "0" ;
    ObSysVars[112].alias_ = "OB_SV_PROXY_GLOBAL_VARIABLES_VERSION" ;
    }();

    [&] (){
      ObSysVars[113].default_value_ = "0" ;
      ObSysVars[113].info_ = "control whether use show trace" ;
      ObSysVars[113].name_ = "ob_enable_show_trace" ;
      ObSysVars[113].data_type_ = ObIntType ;
      ObSysVars[113].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[113].id_ = SYS_VAR_OB_ENABLE_SHOW_TRACE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_SHOW_TRACE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_SHOW_TRACE] = 113 ;
      ObSysVars[113].base_value_ = "0" ;
    ObSysVars[113].alias_ = "OB_SV_ENABLE_SHOW_TRACE" ;
    }();

    [&] (){
      ObSysVars[114].default_value_ = "10485760" ;
      ObSysVars[114].info_ = "" ;
      ObSysVars[114].name_ = "ob_bnl_join_cache_size" ;
      ObSysVars[114].data_type_ = ObIntType ;
      ObSysVars[114].min_val_ = "1" ;
      ObSysVars[114].max_val_ = "9223372036854775807" ;
      ObSysVars[114].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[114].id_ = SYS_VAR_OB_BNL_JOIN_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_BNL_JOIN_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_BNL_JOIN_CACHE_SIZE] = 114 ;
      ObSysVars[114].base_value_ = "10485760" ;
    ObSysVars[114].alias_ = "OB_SV_BNL_JOIN_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[115].default_value_ = "0" ;
      ObSysVars[115].info_ = "Indicate current client session user privilege, readonly after modified by first observer" ;
      ObSysVars[115].name_ = "ob_proxy_user_privilege" ;
      ObSysVars[115].data_type_ = ObIntType ;
      ObSysVars[115].min_val_ = "0" ;
      ObSysVars[115].max_val_ = "9223372036854775807" ;
      ObSysVars[115].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[115].id_ = SYS_VAR_OB_PROXY_USER_PRIVILEGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_USER_PRIVILEGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_USER_PRIVILEGE] = 115 ;
      ObSysVars[115].base_value_ = "0" ;
    ObSysVars[115].alias_ = "OB_SV_PROXY_USER_PRIVILEGE" ;
    }();

    [&] (){
      ObSysVars[116].default_value_ = "0" ;
      ObSysVars[116].info_ = "When the DRC system copies data into the target cluster, it needs to be set to the CLUSTER_ID that should be written into commit log of OceanBase, in order to avoid loop replication of data. Normally, it does not need to be set, and OceanBase will use the default value, which is the CLUSTER_ID of current cluster of OceanBase. 0 indicates it is not set, please do not set it to 0" ;
      ObSysVars[116].name_ = "ob_org_cluster_id" ;
      ObSysVars[116].data_type_ = ObIntType ;
      ObSysVars[116].min_val_ = "0" ;
      ObSysVars[116].max_val_ = "4294967295" ;
      ObSysVars[116].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[116].base_class_ = "ObStrictRangeIntSysVar" ;
      ObSysVars[116].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_ob_org_cluster_id" ;
      ObSysVars[116].id_ = SYS_VAR_OB_ORG_CLUSTER_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ORG_CLUSTER_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ORG_CLUSTER_ID] = 116 ;
      ObSysVars[116].base_value_ = "0" ;
    ObSysVars[116].alias_ = "OB_SV_ORG_CLUSTER_ID" ;
    }();

    [&] (){
      ObSysVars[117].default_value_ = "5" ;
      ObSysVars[117].info_ = "percentage of tenant memory resources that can be used by plan cache" ;
      ObSysVars[117].name_ = "ob_plan_cache_percentage" ;
      ObSysVars[117].data_type_ = ObIntType ;
      ObSysVars[117].min_val_ = "0" ;
      ObSysVars[117].max_val_ = "100" ;
      ObSysVars[117].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[117].id_ = SYS_VAR_OB_PLAN_CACHE_PERCENTAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_PERCENTAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PLAN_CACHE_PERCENTAGE] = 117 ;
      ObSysVars[117].base_value_ = "5" ;
    ObSysVars[117].alias_ = "OB_SV_PLAN_CACHE_PERCENTAGE" ;
    }();

    [&] (){
      ObSysVars[118].default_value_ = "90" ;
      ObSysVars[118].info_ = "memory usage percentage of plan_cache_limit at which plan cache eviction will be trigger" ;
      ObSysVars[118].name_ = "ob_plan_cache_evict_high_percentage" ;
      ObSysVars[118].data_type_ = ObIntType ;
      ObSysVars[118].min_val_ = "0" ;
      ObSysVars[118].max_val_ = "100" ;
      ObSysVars[118].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[118].id_ = SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE] = 118 ;
      ObSysVars[118].base_value_ = "90" ;
    ObSysVars[118].alias_ = "OB_SV_PLAN_CACHE_EVICT_HIGH_PERCENTAGE" ;
    }();

    [&] (){
      ObSysVars[119].default_value_ = "50" ;
      ObSysVars[119].info_ = "memory usage percentage  of plan_cache_limit at which plan cache eviction will be stopped" ;
      ObSysVars[119].name_ = "ob_plan_cache_evict_low_percentage" ;
      ObSysVars[119].data_type_ = ObIntType ;
      ObSysVars[119].min_val_ = "0" ;
      ObSysVars[119].max_val_ = "100" ;
      ObSysVars[119].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[119].id_ = SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE] = 119 ;
      ObSysVars[119].base_value_ = "50" ;
    ObSysVars[119].alias_ = "OB_SV_PLAN_CACHE_EVICT_LOW_PERCENTAGE" ;
    }();

    [&] (){
      ObSysVars[120].default_value_ = "0" ;
      ObSysVars[120].info_ = "When the recycle bin is enabled, dropped tables and their dependent objects are placed in the recycle bin. When the recycle bin is disabled, dropped tables and their dependent objects are not placed in the recycle bin; they are just dropped." ;
      ObSysVars[120].name_ = "recyclebin" ;
      ObSysVars[120].data_type_ = ObIntType ;
      ObSysVars[120].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[120].id_ = SYS_VAR_RECYCLEBIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RECYCLEBIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RECYCLEBIN] = 120 ;
      ObSysVars[120].base_value_ = "0" ;
    ObSysVars[120].alias_ = "OB_SV_RECYCLEBIN" ;
    }();

    [&] (){
      ObSysVars[121].default_value_ = "0" ;
      ObSysVars[121].info_ = "Indicate features that observer supports, readonly after modified by first observer" ;
      ObSysVars[121].name_ = "ob_capability_flag" ;
      ObSysVars[121].data_type_ = ObUInt64Type ;
      ObSysVars[121].min_val_ = "0" ;
      ObSysVars[121].max_val_ = "18446744073709551615" ;
      ObSysVars[121].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[121].id_ = SYS_VAR_OB_CAPABILITY_FLAG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_CAPABILITY_FLAG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_CAPABILITY_FLAG] = 121 ;
      ObSysVars[121].base_value_ = "0" ;
    ObSysVars[121].alias_ = "OB_SV_CAPABILITY_FLAG" ;
    }();

    [&] (){
      ObSysVars[122].default_value_ = "1" ;
      ObSysVars[122].info_ = "when query is with topk hint, is_result_accurate indicates whether the result is acuurate or not " ;
      ObSysVars[122].name_ = "is_result_accurate" ;
      ObSysVars[122].data_type_ = ObIntType ;
      ObSysVars[122].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[122].id_ = SYS_VAR_IS_RESULT_ACCURATE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_IS_RESULT_ACCURATE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_IS_RESULT_ACCURATE] = 122 ;
      ObSysVars[122].base_value_ = "1" ;
    ObSysVars[122].alias_ = "OB_SV_IS_RESULT_ACCURATE" ;
    }();

    [&] (){
      ObSysVars[123].default_value_ = "0" ;
      ObSysVars[123].info_ = "The variable determines how OceanBase should handle an ambiguous boundary datetime value a case in which it is not clear whether the datetime is in standard or daylight saving time" ;
      ObSysVars[123].name_ = "error_on_overlap_time" ;
      ObSysVars[123].data_type_ = ObIntType ;
      ObSysVars[123].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[123].id_ = SYS_VAR_ERROR_ON_OVERLAP_TIME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_ERROR_ON_OVERLAP_TIME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_ERROR_ON_OVERLAP_TIME] = 123 ;
      ObSysVars[123].base_value_ = "0" ;
    ObSysVars[123].alias_ = "OB_SV_ERROR_ON_OVERLAP_TIME" ;
    }();

    [&] (){
      ObSysVars[124].default_value_ = "0" ;
      ObSysVars[124].info_ = "What DBMS is OceanBase compatible with? MYSQL means it behaves like MySQL while ORACLE means it behaves like Oracle." ;
      ObSysVars[124].name_ = "ob_compatibility_mode" ;
      ObSysVars[124].data_type_ = ObIntType ;
      ObSysVars[124].enum_names_ = "[u'MYSQL', u'ORACLE']" ;
      ObSysVars[124].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::WITH_UPGRADE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[124].id_ = SYS_VAR_OB_COMPATIBILITY_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_COMPATIBILITY_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_COMPATIBILITY_MODE] = 124 ;
      ObSysVars[124].base_value_ = "0" ;
    ObSysVars[124].alias_ = "OB_SV_COMPATIBILITY_MODE" ;
    }();

    [&] (){
      ObSysVars[125].default_value_ = "5" ;
      ObSysVars[125].info_ = "The percentage limitation of tenant memory for SQL execution." ;
      ObSysVars[125].name_ = "ob_sql_work_area_percentage" ;
      ObSysVars[125].data_type_ = ObIntType ;
      ObSysVars[125].min_val_ = "0" ;
      ObSysVars[125].max_val_ = "100" ;
      ObSysVars[125].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[125].id_ = SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE] = 125 ;
      ObSysVars[125].base_value_ = "5" ;
    ObSysVars[125].alias_ = "OB_SV_SQL_WORK_AREA_PERCENTAGE" ;
    }();

    [&] (){
      ObSysVars[126].default_value_ = "1" ;
      ObSysVars[126].info_ = "The safe weak read snapshot version in one server" ;
      ObSysVars[126].name_ = "ob_safe_weak_read_snapshot" ;
      ObSysVars[126].data_type_ = ObIntType ;
      ObSysVars[126].min_val_ = "0" ;
      ObSysVars[126].max_val_ = "9223372036854775807" ;
      ObSysVars[126].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[126].on_update_func_ = "ObSysVarOnUpdateFuncs::update_safe_weak_read_snapshot" ;
      ObSysVars[126].id_ = SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT] = 126 ;
      ObSysVars[126].base_value_ = "1" ;
    ObSysVars[126].alias_ = "OB_SV_SAFE_WEAK_READ_SNAPSHOT" ;
    }();

    [&] (){
      ObSysVars[127].default_value_ = "1" ;
      ObSysVars[127].info_ = "the routing policy of obproxy/java client and observer internal retry, 1=READONLY_ZONE_FIRST, 2=ONLY_READONLY_ZONE, 3=UNMERGE_ZONE_FIRST, 4=UNMERGE_FOLLOWER_FIRST" ;
      ObSysVars[127].name_ = "ob_route_policy" ;
      ObSysVars[127].data_type_ = ObIntType ;
      ObSysVars[127].enum_names_ = "[u'', u'READONLY_ZONE_FIRST', u'ONLY_READONLY_ZONE', u'UNMERGE_ZONE_FIRST', u'UNMERGE_FOLLOWER_FIRST', u'COLUMN_STORE_ONLY']" ;
      ObSysVars[127].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[127].id_ = SYS_VAR_OB_ROUTE_POLICY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ROUTE_POLICY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ROUTE_POLICY] = 127 ;
      ObSysVars[127].base_value_ = "1" ;
    ObSysVars[127].alias_ = "OB_SV_ROUTE_POLICY" ;
    }();

    [&] (){
      ObSysVars[128].default_value_ = "1" ;
      ObSysVars[128].info_ = "whether do the checksum of the packet between the client and the server" ;
      ObSysVars[128].name_ = "ob_enable_transmission_checksum" ;
      ObSysVars[128].data_type_ = ObIntType ;
      ObSysVars[128].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::QUERY_SENSITIVE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[128].id_ = SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM] = 128 ;
      ObSysVars[128].base_value_ = "1" ;
    ObSysVars[128].alias_ = "OB_SV_ENABLE_TRANSMISSION_CHECKSUM" ;
    }();

    [&] (){
      ObSysVars[129].default_value_ = "1" ;
      ObSysVars[129].info_ = "set to 1 (the default by MySQL), foreign key constraints are checked. If set to 0, foreign key constraints are ignored" ;
      ObSysVars[129].name_ = "foreign_key_checks" ;
      ObSysVars[129].data_type_ = ObIntType ;
      ObSysVars[129].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[129].id_ = SYS_VAR_FOREIGN_KEY_CHECKS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_FOREIGN_KEY_CHECKS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_FOREIGN_KEY_CHECKS] = 129 ;
      ObSysVars[129].base_value_ = "1" ;
    ObSysVars[129].alias_ = "OB_SV_FOREIGN_KEY_CHECKS" ;
    }();

    [&] (){
      ObSysVars[130].default_value_ = "Y0-0" ;
      ObSysVars[130].info_ = "the trace id of current executing statement" ;
      ObSysVars[130].name_ = "ob_statement_trace_id" ;
      ObSysVars[130].data_type_ = ObVarcharType ;
      ObSysVars[130].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[130].id_ = SYS_VAR_OB_STATEMENT_TRACE_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_STATEMENT_TRACE_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_STATEMENT_TRACE_ID] = 130 ;
      ObSysVars[130].base_value_ = "Y0-0" ;
    ObSysVars[130].alias_ = "OB_SV_STATEMENT_TRACE_ID" ;
    }();

    [&] (){
      ObSysVars[131].default_value_ = "0" ;
      ObSysVars[131].info_ = "Enable the flashback of table truncation." ;
      ObSysVars[131].name_ = "ob_enable_truncate_flashback" ;
      ObSysVars[131].data_type_ = ObIntType ;
      ObSysVars[131].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[131].id_ = SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK] = 131 ;
      ObSysVars[131].base_value_ = "0" ;
    ObSysVars[131].alias_ = "OB_SV_ENABLE_TRUNCATE_FLASHBACK" ;
    }();

    [&] (){
      ObSysVars[132].default_value_ = "127.0.0.1,::1" ;
      ObSysVars[132].info_ = "ip white list for tenant, support % and _ and multi ip(separated by commas), support ip match and wild match" ;
      ObSysVars[132].name_ = "ob_tcp_invited_nodes" ;
      ObSysVars[132].data_type_ = ObVarcharType ;
      ObSysVars[132].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[132].id_ = SYS_VAR_OB_TCP_INVITED_NODES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TCP_INVITED_NODES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_TCP_INVITED_NODES] = 132 ;
      ObSysVars[132].base_value_ = "127.0.0.1,::1" ;
    ObSysVars[132].alias_ = "OB_SV_TCP_INVITED_NODES" ;
    }();

    [&] (){
      ObSysVars[133].default_value_ = "100" ;
      ObSysVars[133].info_ = "current priority used for SQL throttling" ;
      ObSysVars[133].name_ = "sql_throttle_current_priority" ;
      ObSysVars[133].data_type_ = ObIntType ;
      ObSysVars[133].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[133].id_ = SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY] = 133 ;
      ObSysVars[133].base_value_ = "100" ;
    ObSysVars[133].alias_ = "OB_SV_SQL_THROTTLE_CURRENT_PRIORITY" ;
    }();

    [&] (){
      ObSysVars[134].default_value_ = "-1" ;
      ObSysVars[134].info_ = "sql throttle priority, query may not be allowed to execute if its priority isnt greater than this value." ;
      ObSysVars[134].name_ = "sql_throttle_priority" ;
      ObSysVars[134].data_type_ = ObIntType ;
      ObSysVars[134].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[134].id_ = SYS_VAR_SQL_THROTTLE_PRIORITY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_PRIORITY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_PRIORITY] = 134 ;
      ObSysVars[134].base_value_ = "-1" ;
    ObSysVars[134].alias_ = "OB_SV_SQL_THROTTLE_PRIORITY" ;
    }();

    [&] (){
      ObSysVars[135].default_value_ = "-1" ;
      ObSysVars[135].info_ = "query may not be allowed to execute if its rt isnt less than this value." ;
      ObSysVars[135].name_ = "sql_throttle_rt" ;
      ObSysVars[135].data_type_ = ObNumberType ;
      ObSysVars[135].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[135].id_ = SYS_VAR_SQL_THROTTLE_RT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_RT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_RT] = 135 ;
      ObSysVars[135].base_value_ = "-1" ;
    ObSysVars[135].alias_ = "OB_SV_SQL_THROTTLE_RT" ;
    }();

    [&] (){
      ObSysVars[136].default_value_ = "-1" ;
      ObSysVars[136].info_ = "query may not be allowed to execute if its CPU usage isnt less than this value." ;
      ObSysVars[136].name_ = "sql_throttle_cpu" ;
      ObSysVars[136].data_type_ = ObNumberType ;
      ObSysVars[136].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[136].id_ = SYS_VAR_SQL_THROTTLE_CPU ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_CPU)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_CPU] = 136 ;
      ObSysVars[136].base_value_ = "-1" ;
    ObSysVars[136].alias_ = "OB_SV_SQL_THROTTLE_CPU" ;
    }();

    [&] (){
      ObSysVars[137].default_value_ = "-1" ;
      ObSysVars[137].info_ = "query may not be allowed to execute if its number of IOs isnt less than this value." ;
      ObSysVars[137].name_ = "sql_throttle_io" ;
      ObSysVars[137].data_type_ = ObIntType ;
      ObSysVars[137].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[137].id_ = SYS_VAR_SQL_THROTTLE_IO ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_IO)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_IO] = 137 ;
      ObSysVars[137].base_value_ = "-1" ;
    ObSysVars[137].alias_ = "OB_SV_SQL_THROTTLE_IO" ;
    }();

    [&] (){
      ObSysVars[138].default_value_ = "-1" ;
      ObSysVars[138].info_ = "query may not be allowed to execute if its network usage isnt less than this value." ;
      ObSysVars[138].name_ = "sql_throttle_network" ;
      ObSysVars[138].data_type_ = ObNumberType ;
      ObSysVars[138].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[138].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_sql_throttle_queue_time" ;
      ObSysVars[138].id_ = SYS_VAR_SQL_THROTTLE_NETWORK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_NETWORK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_NETWORK] = 138 ;
      ObSysVars[138].base_value_ = "-1" ;
    ObSysVars[138].alias_ = "OB_SV_SQL_THROTTLE_NETWORK" ;
    }();

    [&] (){
      ObSysVars[139].default_value_ = "-1" ;
      ObSysVars[139].info_ = "query may not be allowed to execute if its number of logical reads isnt less than this value." ;
      ObSysVars[139].name_ = "sql_throttle_logical_reads" ;
      ObSysVars[139].data_type_ = ObIntType ;
      ObSysVars[139].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[139].id_ = SYS_VAR_SQL_THROTTLE_LOGICAL_READS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_LOGICAL_READS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_LOGICAL_READS] = 139 ;
      ObSysVars[139].base_value_ = "-1" ;
    ObSysVars[139].alias_ = "OB_SV_SQL_THROTTLE_LOGICAL_READS" ;
    }();

    [&] (){
      ObSysVars[140].default_value_ = "1000000" ;
      ObSysVars[140].info_ = "auto_increment service cache size" ;
      ObSysVars[140].name_ = "auto_increment_cache_size" ;
      ObSysVars[140].data_type_ = ObIntType ;
      ObSysVars[140].min_val_ = "1" ;
      ObSysVars[140].max_val_ = "100000000" ;
      ObSysVars[140].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[140].id_ = SYS_VAR_AUTO_INCREMENT_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_AUTO_INCREMENT_CACHE_SIZE] = 140 ;
      ObSysVars[140].base_value_ = "1000000" ;
    ObSysVars[140].alias_ = "OB_SV_AUTO_INCREMENT_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[141].default_value_ = "0" ;
      ObSysVars[141].info_ = "JIT execution engine mode, default is AUTO" ;
      ObSysVars[141].name_ = "ob_enable_jit" ;
      ObSysVars[141].data_type_ = ObIntType ;
      ObSysVars[141].enum_names_ = "[u'OFF', u'AUTO', u'FORCE']" ;
      ObSysVars[141].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[141].id_ = SYS_VAR_OB_ENABLE_JIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_JIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_JIT] = 141 ;
      ObSysVars[141].base_value_ = "0" ;
    ObSysVars[141].alias_ = "OB_SV_ENABLE_JIT" ;
    }();

    [&] (){
      ObSysVars[142].default_value_ = "0" ;
      ObSysVars[142].info_ = "the percentage limitation of some temp tablespace size in tenant disk." ;
      ObSysVars[142].name_ = "ob_temp_tablespace_size_percentage" ;
      ObSysVars[142].data_type_ = ObIntType ;
      ObSysVars[142].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[142].id_ = SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE] = 142 ;
      ObSysVars[142].base_value_ = "0" ;
    ObSysVars[142].alias_ = "OB_SV_TEMP_TABLESPACE_SIZE_PERCENTAGE" ;
    }();

    [&] (){
      ObSysVars[143].default_value_ = "./plugin_dir/" ;
      ObSysVars[143].info_ = "the dir to place plugin dll" ;
      ObSysVars[143].name_ = "plugin_dir" ;
      ObSysVars[143].data_type_ = ObVarcharType ;
      ObSysVars[143].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY ;
      ObSysVars[143].id_ = SYS_VAR_PLUGIN_DIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLUGIN_DIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PLUGIN_DIR] = 143 ;
      ObSysVars[143].base_value_ = "./plugin_dir/" ;
    ObSysVars[143].alias_ = "OB_SV_PLUGIN_DIR" ;
    }();

    [&] (){
      ObSysVars[144].default_value_ = "3" ;
      ObSysVars[144].info_ = "The limited percentage of tenant memory for sql audit" ;
      ObSysVars[144].name_ = "ob_sql_audit_percentage" ;
      ObSysVars[144].data_type_ = ObIntType ;
      ObSysVars[144].min_val_ = "0" ;
      ObSysVars[144].max_val_ = "80" ;
      ObSysVars[144].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[144].id_ = SYS_VAR_OB_SQL_AUDIT_PERCENTAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_SQL_AUDIT_PERCENTAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_SQL_AUDIT_PERCENTAGE] = 144 ;
      ObSysVars[144].base_value_ = "3" ;
    ObSysVars[144].alias_ = "OB_SV_SQL_AUDIT_PERCENTAGE" ;
    }();

    [&] (){
      ObSysVars[145].default_value_ = "1" ;
      ObSysVars[145].info_ = "wether use sql audit in session" ;
      ObSysVars[145].name_ = "ob_enable_sql_audit" ;
      ObSysVars[145].data_type_ = ObIntType ;
      ObSysVars[145].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[145].id_ = SYS_VAR_OB_ENABLE_SQL_AUDIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_SQL_AUDIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_SQL_AUDIT] = 145 ;
      ObSysVars[145].base_value_ = "1" ;
    ObSysVars[145].alias_ = "OB_SV_ENABLE_SQL_AUDIT" ;
    }();

    [&] (){
      ObSysVars[146].default_value_ = "0" ;
      ObSysVars[146].info_ = "Enable use sql plan baseline" ;
      ObSysVars[146].name_ = "optimizer_use_sql_plan_baselines" ;
      ObSysVars[146].data_type_ = ObIntType ;
      ObSysVars[146].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[146].id_ = SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES] = 146 ;
      ObSysVars[146].base_value_ = "0" ;
    ObSysVars[146].alias_ = "OB_SV_OPTIMIZER_USE_SQL_PLAN_BASELINES" ;
    }();

    [&] (){
      ObSysVars[147].default_value_ = "0" ;
      ObSysVars[147].info_ = "optimizer_capture_sql_plan_baselines enables or disables automitic capture plan baseline." ;
      ObSysVars[147].name_ = "optimizer_capture_sql_plan_baselines" ;
      ObSysVars[147].data_type_ = ObIntType ;
      ObSysVars[147].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[147].id_ = SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES] = 147 ;
      ObSysVars[147].base_value_ = "0" ;
    ObSysVars[147].alias_ = "OB_SV_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES" ;
    }();

    [&] (){
      ObSysVars[148].default_value_ = "0" ;
      ObSysVars[148].info_ = "number of threads allowed to run parallel statements before statement queuing will be used." ;
      ObSysVars[148].name_ = "parallel_servers_target" ;
      ObSysVars[148].data_type_ = ObIntType ;
      ObSysVars[148].min_val_ = "0" ;
      ObSysVars[148].max_val_ = "9223372036854775807" ;
      ObSysVars[148].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[148].id_ = SYS_VAR_PARALLEL_SERVERS_TARGET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PARALLEL_SERVERS_TARGET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PARALLEL_SERVERS_TARGET] = 148 ;
      ObSysVars[148].base_value_ = "0" ;
    ObSysVars[148].alias_ = "OB_SV_PARALLEL_SERVERS_TARGET" ;
    }();

    [&] (){
      ObSysVars[149].default_value_ = "0" ;
      ObSysVars[149].info_ = "If set true, transaction open the elr optimization." ;
      ObSysVars[149].name_ = "ob_early_lock_release" ;
      ObSysVars[149].data_type_ = ObIntType ;
      ObSysVars[149].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[149].id_ = SYS_VAR_OB_EARLY_LOCK_RELEASE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_EARLY_LOCK_RELEASE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_EARLY_LOCK_RELEASE] = 149 ;
      ObSysVars[149].base_value_ = "0" ;
    ObSysVars[149].alias_ = "OB_SV_EARLY_LOCK_RELEASE" ;
    }();

    [&] (){
      ObSysVars[150].default_value_ = "86400000000" ;
      ObSysVars[150].info_ = "The stmt interval timeout of transaction(us)" ;
      ObSysVars[150].name_ = "ob_trx_idle_timeout" ;
      ObSysVars[150].data_type_ = ObIntType ;
      ObSysVars[150].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[150].id_ = SYS_VAR_OB_TRX_IDLE_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRX_IDLE_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRX_IDLE_TIMEOUT] = 150 ;
      ObSysVars[150].base_value_ = "86400000000" ;
    ObSysVars[150].alias_ = "OB_SV_TRX_IDLE_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[151].default_value_ = "0" ;
      ObSysVars[151].info_ = "specifies the encryption algorithm used in the functions aes_encrypt and aes_decrypt" ;
      ObSysVars[151].name_ = "block_encryption_mode" ;
      ObSysVars[151].data_type_ = ObIntType ;
      ObSysVars[151].enum_names_ = "[u'aes-128-ecb', u'aes-192-ecb', u'aes-256-ecb', u'aes-128-cbc', u'aes-192-cbc', u'aes-256-cbc', u'aes-128-cfb1', u'aes-192-cfb1', u'aes-256-cfb1', u'aes-128-cfb8', u'aes-192-cfb8', u'aes-256-cfb8', u'aes-128-cfb128', u'aes-192-cfb128', u'aes-256-cfb128', u'aes-128-ofb', u'aes-192-ofb', u'aes-256-ofb', u'sm4-ecb', u'sm4-cbc', u'sm4-cfb', u'sm4-ofb']" ;
      ObSysVars[151].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[151].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_block_encryption_mode" ;
      ObSysVars[151].id_ = SYS_VAR_BLOCK_ENCRYPTION_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BLOCK_ENCRYPTION_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BLOCK_ENCRYPTION_MODE] = 151 ;
      ObSysVars[151].base_value_ = "0" ;
    ObSysVars[151].alias_ = "OB_SV_BLOCK_ENCRYPTION_MODE" ;
    }();

    [&] (){
      ObSysVars[152].default_value_ = "DD-MON-RR" ;
      ObSysVars[152].info_ = "specifies the default date format to use with the TO_CHAR and TO_DATE functions, (YYYY-MM-DD HH24:MI:SS) is Common value" ;
      ObSysVars[152].name_ = "nls_date_format" ;
      ObSysVars[152].data_type_ = ObVarcharType ;
      ObSysVars[152].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[152].id_ = SYS_VAR_NLS_DATE_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_DATE_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_DATE_FORMAT] = 152 ;
      ObSysVars[152].base_value_ = "DD-MON-RR" ;
    ObSysVars[152].alias_ = "OB_SV_NLS_DATE_FORMAT" ;
    }();

    [&] (){
      ObSysVars[153].default_value_ = "DD-MON-RR HH.MI.SSXFF AM" ;
      ObSysVars[153].info_ = "specifies the default date format to use with the TO_CHAR and TO_TIMESTAMP functions, (YYYY-MM-DD HH24:MI:SS.FF) is Common value" ;
      ObSysVars[153].name_ = "nls_timestamp_format" ;
      ObSysVars[153].data_type_ = ObVarcharType ;
      ObSysVars[153].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[153].id_ = SYS_VAR_NLS_TIMESTAMP_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_TIMESTAMP_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_TIMESTAMP_FORMAT] = 153 ;
      ObSysVars[153].base_value_ = "DD-MON-RR HH.MI.SSXFF AM" ;
    ObSysVars[153].alias_ = "OB_SV_NLS_TIMESTAMP_FORMAT" ;
    }();

    [&] (){
      ObSysVars[154].default_value_ = "DD-MON-RR HH.MI.SSXFF AM TZR" ;
      ObSysVars[154].info_ = "specifies the default timestamp with time zone format to use with the TO_CHAR and TO_TIMESTAMP_TZ functions, (YYYY-MM-DD HH24:MI:SS.FF TZR TZD) is common value" ;
      ObSysVars[154].name_ = "nls_timestamp_tz_format" ;
      ObSysVars[154].data_type_ = ObVarcharType ;
      ObSysVars[154].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[154].id_ = SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT] = 154 ;
      ObSysVars[154].base_value_ = "DD-MON-RR HH.MI.SSXFF AM TZR" ;
    ObSysVars[154].alias_ = "OB_SV_NLS_TIMESTAMP_TZ_FORMAT" ;
    }();

    [&] (){
      ObSysVars[155].default_value_ = "10" ;
      ObSysVars[155].info_ = "percentage of tenant memory resources that can be used by tenant meta data" ;
      ObSysVars[155].name_ = "ob_reserved_meta_memory_percentage" ;
      ObSysVars[155].data_type_ = ObIntType ;
      ObSysVars[155].min_val_ = "1" ;
      ObSysVars[155].max_val_ = "100" ;
      ObSysVars[155].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[155].id_ = SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE] = 155 ;
      ObSysVars[155].base_value_ = "10" ;
    ObSysVars[155].alias_ = "OB_SV_RESERVED_META_MEMORY_PERCENTAGE" ;
    }();

    [&] (){
      ObSysVars[156].default_value_ = "1" ;
      ObSysVars[156].info_ = "If set true, sql will update sys variable while schema version changed." ;
      ObSysVars[156].name_ = "ob_check_sys_variable" ;
      ObSysVars[156].data_type_ = ObIntType ;
      ObSysVars[156].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[156].id_ = SYS_VAR_OB_CHECK_SYS_VARIABLE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_CHECK_SYS_VARIABLE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_CHECK_SYS_VARIABLE] = 156 ;
      ObSysVars[156].base_value_ = "1" ;
    ObSysVars[156].alias_ = "OB_SV_CHECK_SYS_VARIABLE" ;
    }();

    [&] (){
      ObSysVars[157].default_value_ = "AMERICAN" ;
      ObSysVars[157].info_ = "specifies the default language of the database, used for messages, day and month names, the default sorting mechanism, the default values of NLS_DATE_LANGUAGE and NLS_SORT." ;
      ObSysVars[157].name_ = "nls_language" ;
      ObSysVars[157].data_type_ = ObVarcharType ;
      ObSysVars[157].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[157].id_ = SYS_VAR_NLS_LANGUAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_LANGUAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_LANGUAGE] = 157 ;
      ObSysVars[157].base_value_ = "AMERICAN" ;
    ObSysVars[157].alias_ = "OB_SV_NLS_LANGUAGE" ;
    }();

    [&] (){
      ObSysVars[158].default_value_ = "AMERICA" ;
      ObSysVars[158].info_ = "specifies the name of the territory whose conventions are to be followed for day and week numbering, establishes the default date format, the default decimal character and group separator, and the default ISO and local currency symbols." ;
      ObSysVars[158].name_ = "nls_territory" ;
      ObSysVars[158].data_type_ = ObVarcharType ;
      ObSysVars[158].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[158].id_ = SYS_VAR_NLS_TERRITORY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_TERRITORY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_TERRITORY] = 158 ;
      ObSysVars[158].base_value_ = "AMERICA" ;
    ObSysVars[158].alias_ = "OB_SV_NLS_TERRITORY" ;
    }();

    [&] (){
      ObSysVars[159].default_value_ = "BINARY" ;
      ObSysVars[159].info_ = "specifies the collating sequence for character value comparison in various SQL operators and clauses." ;
      ObSysVars[159].name_ = "nls_sort" ;
      ObSysVars[159].data_type_ = ObVarcharType ;
      ObSysVars[159].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[159].id_ = SYS_VAR_NLS_SORT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_SORT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_SORT] = 159 ;
      ObSysVars[159].base_value_ = "BINARY" ;
    ObSysVars[159].alias_ = "OB_SV_NLS_SORT" ;
    }();

    [&] (){
      ObSysVars[160].default_value_ = "BINARY" ;
      ObSysVars[160].info_ = "specifies the collation behavior of the database session. value can be BINARY | LINGUISTIC | ANSI" ;
      ObSysVars[160].name_ = "nls_comp" ;
      ObSysVars[160].data_type_ = ObVarcharType ;
      ObSysVars[160].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[160].id_ = SYS_VAR_NLS_COMP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_COMP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_COMP] = 160 ;
      ObSysVars[160].base_value_ = "BINARY" ;
    ObSysVars[160].alias_ = "OB_SV_NLS_COMP" ;
    }();

    [&] (){
      ObSysVars[161].default_value_ = "AL32UTF8" ;
      ObSysVars[161].info_ = "specifies the default characterset of the database, This parameter defines the encoding of the data in the CHAR, VARCHAR2, LONG and CLOB columns of a table." ;
      ObSysVars[161].name_ = "nls_characterset" ;
      ObSysVars[161].data_type_ = ObVarcharType ;
      ObSysVars[161].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::WITH_CREATE | ObSysVarFlag::READONLY ;
      ObSysVars[161].id_ = SYS_VAR_NLS_CHARACTERSET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_CHARACTERSET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_CHARACTERSET] = 161 ;
      ObSysVars[161].base_value_ = "AL32UTF8" ;
    ObSysVars[161].alias_ = "OB_SV_NLS_CHARACTERSET" ;
    }();

    [&] (){
      ObSysVars[162].default_value_ = "AL16UTF16" ;
      ObSysVars[162].info_ = "specifies the default characterset of the database, This parameter defines the encoding of the data in the NCHAR, NVARCHAR2 and NCLOB columns of a table." ;
      ObSysVars[162].name_ = "nls_nchar_characterset" ;
      ObSysVars[162].data_type_ = ObVarcharType ;
      ObSysVars[162].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[162].id_ = SYS_VAR_NLS_NCHAR_CHARACTERSET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_NCHAR_CHARACTERSET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_NCHAR_CHARACTERSET] = 162 ;
      ObSysVars[162].base_value_ = "AL16UTF16" ;
    ObSysVars[162].alias_ = "OB_SV_NLS_NCHAR_CHARACTERSET" ;
    }();

    [&] (){
      ObSysVars[163].default_value_ = "AMERICAN" ;
      ObSysVars[163].info_ = "specifies the language to use for the spelling of day and month names and date abbreviations (a.m., p.m., AD, BC) returned by the TO_DATE and TO_CHAR functions." ;
      ObSysVars[163].name_ = "nls_date_language" ;
      ObSysVars[163].data_type_ = ObVarcharType ;
      ObSysVars[163].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[163].id_ = SYS_VAR_NLS_DATE_LANGUAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_DATE_LANGUAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_DATE_LANGUAGE] = 163 ;
      ObSysVars[163].base_value_ = "AMERICAN" ;
    ObSysVars[163].alias_ = "OB_SV_NLS_DATE_LANGUAGE" ;
    }();

    [&] (){
      ObSysVars[164].default_value_ = "BYTE" ;
      ObSysVars[164].info_ = "specifies the default length semantics to use for VARCHAR2 and CHAR table columns, user-defined object attributes, and PL/SQL variables in database objects created in the session. SYS user use BYTE intead of NLS_LENGTH_SEMANTICS." ;
      ObSysVars[164].name_ = "nls_length_semantics" ;
      ObSysVars[164].data_type_ = ObVarcharType ;
      ObSysVars[164].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[164].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_nls_length_semantics_is_valid" ;
      ObSysVars[164].id_ = SYS_VAR_NLS_LENGTH_SEMANTICS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_LENGTH_SEMANTICS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_LENGTH_SEMANTICS] = 164 ;
      ObSysVars[164].base_value_ = "BYTE" ;
    ObSysVars[164].alias_ = "OB_SV_NLS_LENGTH_SEMANTICS" ;
    }();

    [&] (){
      ObSysVars[165].default_value_ = "FALSE" ;
      ObSysVars[165].info_ = "determines whether an error is reported when there is data loss during an implicit or explicit character type conversion between NCHAR/NVARCHAR2 and CHAR/VARCHAR2." ;
      ObSysVars[165].name_ = "nls_nchar_conv_excp" ;
      ObSysVars[165].data_type_ = ObVarcharType ;
      ObSysVars[165].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[165].id_ = SYS_VAR_NLS_NCHAR_CONV_EXCP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_NCHAR_CONV_EXCP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_NCHAR_CONV_EXCP] = 165 ;
      ObSysVars[165].base_value_ = "FALSE" ;
    ObSysVars[165].alias_ = "OB_SV_NLS_NCHAR_CONV_EXCP" ;
    }();

    [&] (){
      ObSysVars[166].default_value_ = "GREGORIAN" ;
      ObSysVars[166].info_ = "specifies which calendar system Oracle uses." ;
      ObSysVars[166].name_ = "nls_calendar" ;
      ObSysVars[166].data_type_ = ObVarcharType ;
      ObSysVars[166].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[166].id_ = SYS_VAR_NLS_CALENDAR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_CALENDAR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_CALENDAR] = 166 ;
      ObSysVars[166].base_value_ = "GREGORIAN" ;
    ObSysVars[166].alias_ = "OB_SV_NLS_CALENDAR" ;
    }();

    [&] (){
      ObSysVars[167].default_value_ = ".," ;
      ObSysVars[167].info_ = "specifies the characters to use as the decimal character and group separator, overrides those characters defined implicitly by NLS_TERRITORY." ;
      ObSysVars[167].name_ = "nls_numeric_characters" ;
      ObSysVars[167].data_type_ = ObVarcharType ;
      ObSysVars[167].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[167].id_ = SYS_VAR_NLS_NUMERIC_CHARACTERS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_NUMERIC_CHARACTERS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_NUMERIC_CHARACTERS] = 167 ;
      ObSysVars[167].base_value_ = ".," ;
    ObSysVars[167].alias_ = "OB_SV_NLS_NUMERIC_CHARACTERS" ;
    }();

    [&] (){
      ObSysVars[168].default_value_ = "1" ;
      ObSysVars[168].info_ = "enable batching of the RHS IO in NLJ" ;
      ObSysVars[168].name_ = "_nlj_batching_enabled" ;
      ObSysVars[168].data_type_ = ObIntType ;
      ObSysVars[168].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::QUERY_SENSITIVE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[168].id_ = SYS_VAR__NLJ_BATCHING_ENABLED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__NLJ_BATCHING_ENABLED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__NLJ_BATCHING_ENABLED] = 168 ;
      ObSysVars[168].base_value_ = "1" ;
    ObSysVars[168].alias_ = "OB_SV__NLJ_BATCHING_ENABLED" ;
    }();

    [&] (){
      ObSysVars[169].default_value_ = "" ;
      ObSysVars[169].info_ = "The name of tracefile." ;
      ObSysVars[169].name_ = "tracefile_identifier" ;
      ObSysVars[169].data_type_ = ObVarcharType ;
      ObSysVars[169].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[169].id_ = SYS_VAR_TRACEFILE_IDENTIFIER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRACEFILE_IDENTIFIER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TRACEFILE_IDENTIFIER] = 169 ;
      ObSysVars[169].base_value_ = "" ;
    ObSysVars[169].alias_ = "OB_SV_TRACEFILE_IDENTIFIER" ;
    }();

    [&] (){
      ObSysVars[170].default_value_ = "3" ;
      ObSysVars[170].info_ = "ratio used to decide whether push down should be done in distribtued query optimization." ;
      ObSysVars[170].name_ = "_groupby_nopushdown_cut_ratio" ;
      ObSysVars[170].data_type_ = ObUInt64Type ;
      ObSysVars[170].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[170].id_ = SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO] = 170 ;
      ObSysVars[170].base_value_ = "3" ;
    ObSysVars[170].alias_ = "OB_SV__GROUPBY_NOPUSHDOWN_CUT_RATIO" ;
    }();

    [&] (){
      ObSysVars[171].default_value_ = "100" ;
      ObSysVars[171].info_ = "set the tq broadcasting fudge factor percentage." ;
      ObSysVars[171].name_ = "_px_broadcast_fudge_factor" ;
      ObSysVars[171].data_type_ = ObIntType ;
      ObSysVars[171].min_val_ = "0" ;
      ObSysVars[171].max_val_ = "100" ;
      ObSysVars[171].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[171].id_ = SYS_VAR__PX_BROADCAST_FUDGE_FACTOR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_BROADCAST_FUDGE_FACTOR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__PX_BROADCAST_FUDGE_FACTOR] = 171 ;
      ObSysVars[171].base_value_ = "100" ;
    ObSysVars[171].alias_ = "OB_SV__PX_BROADCAST_FUDGE_FACTOR" ;
    }();

    [&] (){
      ObSysVars[172].default_value_ = "READ-COMMITTED" ;
      ObSysVars[172].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_isolation" ;
      ObSysVars[172].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation" ;
      ObSysVars[172].name_ = "transaction_isolation" ;
      ObSysVars[172].data_type_ = ObVarcharType ;
      ObSysVars[172].info_ = "Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE" ;
      ObSysVars[172].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[172].base_class_ = "ObSessionSpecialVarcharSysVar" ;
      ObSysVars[172].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_isolation" ;
      ObSysVars[172].id_ = SYS_VAR_TRANSACTION_ISOLATION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_ISOLATION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_ISOLATION] = 172 ;
      ObSysVars[172].base_value_ = "READ-COMMITTED" ;
    ObSysVars[172].alias_ = "OB_SV_TRANSACTION_ISOLATION" ;
    }();

    [&] (){
      ObSysVars[173].default_value_ = "-1" ;
      ObSysVars[173].info_ = "the max duration of waiting on row lock of one transaction" ;
      ObSysVars[173].name_ = "ob_trx_lock_timeout" ;
      ObSysVars[173].data_type_ = ObIntType ;
      ObSysVars[173].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[173].id_ = SYS_VAR_OB_TRX_LOCK_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRX_LOCK_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRX_LOCK_TIMEOUT] = 173 ;
      ObSysVars[173].base_value_ = "-1" ;
    ObSysVars[173].alias_ = "OB_SV_TRX_LOCK_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[174].default_value_ = "0" ;
      ObSysVars[174].info_ = "" ;
      ObSysVars[174].name_ = "validate_password_check_user_name" ;
      ObSysVars[174].data_type_ = ObIntType ;
      ObSysVars[174].enum_names_ = "[u'on', u'off']" ;
      ObSysVars[174].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[174].id_ = SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME] = 174 ;
      ObSysVars[174].base_value_ = "0" ;
    ObSysVars[174].alias_ = "OB_SV_VALIDATE_PASSWORD_CHECK_USER_NAME" ;
    }();

    [&] (){
      ObSysVars[175].default_value_ = "0" ;
      ObSysVars[175].info_ = "" ;
      ObSysVars[175].name_ = "validate_password_length" ;
      ObSysVars[175].data_type_ = ObUInt64Type ;
      ObSysVars[175].min_val_ = "0" ;
      ObSysVars[175].max_val_ = "2147483647" ;
      ObSysVars[175].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[175].id_ = SYS_VAR_VALIDATE_PASSWORD_LENGTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_LENGTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_LENGTH] = 175 ;
      ObSysVars[175].base_value_ = "0" ;
    ObSysVars[175].alias_ = "OB_SV_VALIDATE_PASSWORD_LENGTH" ;
    }();

    [&] (){
      ObSysVars[176].default_value_ = "0" ;
      ObSysVars[176].info_ = "" ;
      ObSysVars[176].name_ = "validate_password_mixed_case_count" ;
      ObSysVars[176].data_type_ = ObUInt64Type ;
      ObSysVars[176].min_val_ = "0" ;
      ObSysVars[176].max_val_ = "2147483647" ;
      ObSysVars[176].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[176].id_ = SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT] = 176 ;
      ObSysVars[176].base_value_ = "0" ;
    ObSysVars[176].alias_ = "OB_SV_VALIDATE_PASSWORD_MIXED_CASE_COUNT" ;
    }();

    [&] (){
      ObSysVars[177].default_value_ = "0" ;
      ObSysVars[177].info_ = "" ;
      ObSysVars[177].name_ = "validate_password_number_count" ;
      ObSysVars[177].data_type_ = ObUInt64Type ;
      ObSysVars[177].min_val_ = "0" ;
      ObSysVars[177].max_val_ = "2147483647" ;
      ObSysVars[177].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[177].id_ = SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT] = 177 ;
      ObSysVars[177].base_value_ = "0" ;
    ObSysVars[177].alias_ = "OB_SV_VALIDATE_PASSWORD_NUMBER_COUNT" ;
    }();

    [&] (){
      ObSysVars[178].default_value_ = "0" ;
      ObSysVars[178].info_ = "" ;
      ObSysVars[178].name_ = "validate_password_policy" ;
      ObSysVars[178].data_type_ = ObIntType ;
      ObSysVars[178].enum_names_ = "[u'low', u'medium']" ;
      ObSysVars[178].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[178].id_ = SYS_VAR_VALIDATE_PASSWORD_POLICY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_POLICY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_POLICY] = 178 ;
      ObSysVars[178].base_value_ = "0" ;
    ObSysVars[178].alias_ = "OB_SV_VALIDATE_PASSWORD_POLICY" ;
    }();

    [&] (){
      ObSysVars[179].default_value_ = "0" ;
      ObSysVars[179].info_ = "" ;
      ObSysVars[179].name_ = "validate_password_special_char_count" ;
      ObSysVars[179].data_type_ = ObUInt64Type ;
      ObSysVars[179].min_val_ = "0" ;
      ObSysVars[179].max_val_ = "2147483647" ;
      ObSysVars[179].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[179].id_ = SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT] = 179 ;
      ObSysVars[179].base_value_ = "0" ;
    ObSysVars[179].alias_ = "OB_SV_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT" ;
    }();

    [&] (){
      ObSysVars[180].default_value_ = "0" ;
      ObSysVars[180].info_ = "" ;
      ObSysVars[180].name_ = "default_password_lifetime" ;
      ObSysVars[180].data_type_ = ObUInt64Type ;
      ObSysVars[180].min_val_ = "0" ;
      ObSysVars[180].max_val_ = "65535" ;
      ObSysVars[180].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[180].id_ = SYS_VAR_DEFAULT_PASSWORD_LIFETIME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_PASSWORD_LIFETIME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_PASSWORD_LIFETIME] = 180 ;
      ObSysVars[180].base_value_ = "0" ;
    ObSysVars[180].alias_ = "OB_SV_DEFAULT_PASSWORD_LIFETIME" ;
    }();

    [&] (){
      ObSysVars[181].default_value_ = "" ;
      ObSysVars[181].info_ = "store all session labels for all label security policy." ;
      ObSysVars[181].name_ = "_ob_ols_policy_session_labels" ;
      ObSysVars[181].data_type_ = ObVarcharType ;
      ObSysVars[181].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[181].id_ = SYS_VAR__OB_OLS_POLICY_SESSION_LABELS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_OLS_POLICY_SESSION_LABELS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OB_OLS_POLICY_SESSION_LABELS] = 181 ;
      ObSysVars[181].base_value_ = "" ;
    ObSysVars[181].alias_ = "OB_SV__OB_OLS_POLICY_SESSION_LABELS" ;
    }();

    [&] (){
      ObSysVars[182].default_value_ = "" ;
      ObSysVars[182].info_ = "store trace info" ;
      ObSysVars[182].name_ = "ob_trace_info" ;
      ObSysVars[182].data_type_ = ObVarcharType ;
      ObSysVars[182].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[182].id_ = SYS_VAR_OB_TRACE_INFO ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRACE_INFO)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRACE_INFO] = 182 ;
      ObSysVars[182].base_value_ = "" ;
    ObSysVars[182].alias_ = "OB_SV_TRACE_INFO" ;
    }();

    [&] (){
      ObSysVars[183].default_value_ = "64" ;
      ObSysVars[183].info_ = "least number of partitions per slave to start partition-based scan" ;
      ObSysVars[183].name_ = "_px_partition_scan_threshold" ;
      ObSysVars[183].data_type_ = ObIntType ;
      ObSysVars[183].min_val_ = "0" ;
      ObSysVars[183].max_val_ = "100" ;
      ObSysVars[183].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[183].id_ = SYS_VAR__PX_PARTITION_SCAN_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_PARTITION_SCAN_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__PX_PARTITION_SCAN_THRESHOLD] = 183 ;
      ObSysVars[183].base_value_ = "64" ;
    ObSysVars[183].alias_ = "OB_SV__PX_PARTITION_SCAN_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[184].default_value_ = "1" ;
      ObSysVars[184].info_ = "broadcast optimization." ;
      ObSysVars[184].name_ = "_ob_px_bcast_optimization" ;
      ObSysVars[184].data_type_ = ObIntType ;
      ObSysVars[184].enum_names_ = "[u'WORKER', u'SERVER']" ;
      ObSysVars[184].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[184].id_ = SYS_VAR__OB_PX_BCAST_OPTIMIZATION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_PX_BCAST_OPTIMIZATION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OB_PX_BCAST_OPTIMIZATION] = 184 ;
      ObSysVars[184].base_value_ = "1" ;
    ObSysVars[184].alias_ = "OB_SV__OB_PX_BCAST_OPTIMIZATION" ;
    }();

    [&] (){
      ObSysVars[185].default_value_ = "200" ;
      ObSysVars[185].info_ = "percentage threshold to use slave mapping plan" ;
      ObSysVars[185].name_ = "_ob_px_slave_mapping_threshold" ;
      ObSysVars[185].data_type_ = ObIntType ;
      ObSysVars[185].min_val_ = "0" ;
      ObSysVars[185].max_val_ = "1000" ;
      ObSysVars[185].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[185].id_ = SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD] = 185 ;
      ObSysVars[185].base_value_ = "200" ;
    ObSysVars[185].alias_ = "OB_SV__OB_PX_SLAVE_MAPPING_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[186].default_value_ = "0" ;
      ObSysVars[186].info_ = "A DML statement can be parallelized only if you have explicitly enabled parallel DML in the session or in the SQL statement." ;
      ObSysVars[186].name_ = "_enable_parallel_dml" ;
      ObSysVars[186].data_type_ = ObIntType ;
      ObSysVars[186].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[186].id_ = SYS_VAR__ENABLE_PARALLEL_DML ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_DML)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_PARALLEL_DML] = 186 ;
      ObSysVars[186].base_value_ = "0" ;
    ObSysVars[186].alias_ = "OB_SV__ENABLE_PARALLEL_DML" ;
    }();

    [&] (){
      ObSysVars[187].default_value_ = "13" ;
      ObSysVars[187].info_ = "minimum number of rowid range granules to generate per slave." ;
      ObSysVars[187].name_ = "_px_min_granules_per_slave" ;
      ObSysVars[187].data_type_ = ObIntType ;
      ObSysVars[187].min_val_ = "0" ;
      ObSysVars[187].max_val_ = "100" ;
      ObSysVars[187].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[187].id_ = SYS_VAR__PX_MIN_GRANULES_PER_SLAVE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_MIN_GRANULES_PER_SLAVE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__PX_MIN_GRANULES_PER_SLAVE] = 187 ;
      ObSysVars[187].base_value_ = "13" ;
    ObSysVars[187].alias_ = "OB_SV__PX_MIN_GRANULES_PER_SLAVE" ;
    }();

    [&] (){
      ObSysVars[188].default_value_ = "" ;
      ObSysVars[188].info_ = "limit the effect of data import and export operations" ;
      ObSysVars[188].name_ = "secure_file_priv" ;
      ObSysVars[188].data_type_ = ObVarcharType ;
      ObSysVars[188].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NULLABLE ;
      ObSysVars[188].id_ = SYS_VAR_SECURE_FILE_PRIV ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SECURE_FILE_PRIV)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SECURE_FILE_PRIV] = 188 ;
      ObSysVars[188].base_value_ = "" ;
    ObSysVars[188].alias_ = "OB_SV_SECURE_FILE_PRIV" ;
    }();

    [&] (){
      ObSysVars[189].default_value_ = "ENABLE:ALL" ;
      ObSysVars[189].info_ = "enables or disables the reporting of warning messages by the PL/SQL compiler, and specifies which warning messages to show as errors." ;
      ObSysVars[189].name_ = "plsql_warnings" ;
      ObSysVars[189].data_type_ = ObVarcharType ;
      ObSysVars[189].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[189].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_plsql_warnings" ;
      ObSysVars[189].id_ = SYS_VAR_PLSQL_WARNINGS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLSQL_WARNINGS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PLSQL_WARNINGS] = 189 ;
      ObSysVars[189].base_value_ = "ENABLE:ALL" ;
    ObSysVars[189].alias_ = "OB_SV_PLSQL_WARNINGS" ;
    }();

    [&] (){
      ObSysVars[190].default_value_ = "1" ;
      ObSysVars[190].info_ = "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY in the session or in the SQL statement." ;
      ObSysVars[190].name_ = "_enable_parallel_query" ;
      ObSysVars[190].data_type_ = ObIntType ;
      ObSysVars[190].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[190].id_ = SYS_VAR__ENABLE_PARALLEL_QUERY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_QUERY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_PARALLEL_QUERY] = 190 ;
      ObSysVars[190].base_value_ = "1" ;
    ObSysVars[190].alias_ = "OB_SV__ENABLE_PARALLEL_QUERY" ;
    }();

    [&] (){
      ObSysVars[191].default_value_ = "1" ;
      ObSysVars[191].info_ = "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY in the session or in the SQL statement." ;
      ObSysVars[191].name_ = "_force_parallel_query_dop" ;
      ObSysVars[191].data_type_ = ObUInt64Type ;
      ObSysVars[191].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[191].id_ = SYS_VAR__FORCE_PARALLEL_QUERY_DOP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_QUERY_DOP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__FORCE_PARALLEL_QUERY_DOP] = 191 ;
      ObSysVars[191].base_value_ = "1" ;
    ObSysVars[191].alias_ = "OB_SV__FORCE_PARALLEL_QUERY_DOP" ;
    }();

    [&] (){
      ObSysVars[192].default_value_ = "1" ;
      ObSysVars[192].info_ = "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY in the session or in the SQL statement." ;
      ObSysVars[192].name_ = "_force_parallel_dml_dop" ;
      ObSysVars[192].data_type_ = ObUInt64Type ;
      ObSysVars[192].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[192].id_ = SYS_VAR__FORCE_PARALLEL_DML_DOP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_DML_DOP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__FORCE_PARALLEL_DML_DOP] = 192 ;
      ObSysVars[192].base_value_ = "1" ;
    ObSysVars[192].alias_ = "OB_SV__FORCE_PARALLEL_DML_DOP" ;
    }();

    [&] (){
      ObSysVars[193].default_value_ = "3216672000000000" ;
      ObSysVars[193].info_ = "PL/SQL timeout in microsecond(us)" ;
      ObSysVars[193].name_ = "ob_pl_block_timeout" ;
      ObSysVars[193].data_type_ = ObIntType ;
      ObSysVars[193].min_val_ = "0" ;
      ObSysVars[193].max_val_ = "9223372036854775807" ;
      ObSysVars[193].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[193].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large" ;
      ObSysVars[193].id_ = SYS_VAR_OB_PL_BLOCK_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PL_BLOCK_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_PL_BLOCK_TIMEOUT] = 193 ;
      ObSysVars[193].base_value_ = "3216672000000000" ;
    ObSysVars[193].alias_ = "OB_SV_PL_BLOCK_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[194].default_value_ = "0" ;
      ObSysVars[194].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope" ;
      ObSysVars[194].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only" ;
      ObSysVars[194].name_ = "transaction_read_only" ;
      ObSysVars[194].data_type_ = ObIntType ;
      ObSysVars[194].info_ = "Transaction access mode" ;
      ObSysVars[194].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[194].base_class_ = "ObSessionSpecialBoolSysVar" ;
      ObSysVars[194].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_read_only" ;
      ObSysVars[194].id_ = SYS_VAR_TRANSACTION_READ_ONLY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_READ_ONLY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_READ_ONLY] = 194 ;
      ObSysVars[194].base_value_ = "0" ;
    ObSysVars[194].alias_ = "OB_SV_TRANSACTION_READ_ONLY" ;
    }();

    [&] (){
      ObSysVars[195].default_value_ = "" ;
      ObSysVars[195].info_ = "specifies tenant resource plan." ;
      ObSysVars[195].name_ = "resource_manager_plan" ;
      ObSysVars[195].data_type_ = ObVarcharType ;
      ObSysVars[195].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[195].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_update_resource_manager_plan" ;
      ObSysVars[195].id_ = SYS_VAR_RESOURCE_MANAGER_PLAN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RESOURCE_MANAGER_PLAN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RESOURCE_MANAGER_PLAN] = 195 ;
      ObSysVars[195].base_value_ = "" ;
    ObSysVars[195].alias_ = "OB_SV_RESOURCE_MANAGER_PLAN" ;
    }();

    [&] (){
      ObSysVars[196].default_value_ = "0" ;
      ObSysVars[196].info_ = "indicate whether the Performance Schema is enabled" ;
      ObSysVars[196].name_ = "performance_schema" ;
      ObSysVars[196].data_type_ = ObIntType ;
      ObSysVars[196].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[196].id_ = SYS_VAR_PERFORMANCE_SCHEMA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PERFORMANCE_SCHEMA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PERFORMANCE_SCHEMA] = 196 ;
      ObSysVars[196].base_value_ = "0" ;
    ObSysVars[196].alias_ = "OB_SV_PERFORMANCE_SCHEMA" ;
    }();

    [&] (){
      ObSysVars[197].default_value_ = "$" ;
      ObSysVars[197].info_ = "specifies the string to use as the local currency symbol for the L number format element. The default value of this parameter is determined by NLS_TERRITORY." ;
      ObSysVars[197].name_ = "nls_currency" ;
      ObSysVars[197].data_type_ = ObVarcharType ;
      ObSysVars[197].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[197].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_nls_currency_too_long" ;
      ObSysVars[197].id_ = SYS_VAR_NLS_CURRENCY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_CURRENCY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_CURRENCY] = 197 ;
      ObSysVars[197].base_value_ = "$" ;
    ObSysVars[197].alias_ = "OB_SV_NLS_CURRENCY" ;
    }();

    [&] (){
      ObSysVars[198].default_value_ = "AMERICA" ;
      ObSysVars[198].info_ = "specifies the string to use as the international currency symbol for the C number format element. The default value of this parameter is determined by NLS_TERRITORY" ;
      ObSysVars[198].name_ = "nls_iso_currency" ;
      ObSysVars[198].data_type_ = ObVarcharType ;
      ObSysVars[198].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[198].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_nls_iso_currency_is_valid" ;
      ObSysVars[198].id_ = SYS_VAR_NLS_ISO_CURRENCY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_ISO_CURRENCY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_ISO_CURRENCY] = 198 ;
      ObSysVars[198].base_value_ = "AMERICA" ;
    ObSysVars[198].alias_ = "OB_SV_NLS_ISO_CURRENCY" ;
    }();

    [&] (){
      ObSysVars[199].default_value_ = "$" ;
      ObSysVars[199].info_ = "specifies the dual currency symbol for the territory. The default is the dual currency symbol defined in the territory of your current language environment." ;
      ObSysVars[199].name_ = "nls_dual_currency" ;
      ObSysVars[199].data_type_ = ObVarcharType ;
      ObSysVars[199].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[199].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_nls_currency_too_long" ;
      ObSysVars[199].id_ = SYS_VAR_NLS_DUAL_CURRENCY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_DUAL_CURRENCY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NLS_DUAL_CURRENCY] = 199 ;
      ObSysVars[199].base_value_ = "$" ;
    ObSysVars[199].alias_ = "OB_SV_NLS_DUAL_CURRENCY" ;
    }();

    [&] (){
      ObSysVars[200].default_value_ = "" ;
      ObSysVars[200].info_ = "Lets you control conditional compilation of each PL/SQL unit independently." ;
      ObSysVars[200].name_ = "plsql_ccflags" ;
      ObSysVars[200].data_type_ = ObVarcharType ;
      ObSysVars[200].flags_ = ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[200].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_plsql_ccflags" ;
      ObSysVars[200].id_ = SYS_VAR_PLSQL_CCFLAGS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLSQL_CCFLAGS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PLSQL_CCFLAGS] = 200 ;
      ObSysVars[200].base_value_ = "" ;
    ObSysVars[200].alias_ = "OB_SV_PLSQL_CCFLAGS" ;
    }();

    [&] (){
      ObSysVars[201].default_value_ = "0" ;
      ObSysVars[201].info_ = "this value is true if we have executed set transaction stmt, until a transaction commit(explicit or implicit) successfully" ;
      ObSysVars[201].name_ = "_ob_proxy_session_temporary_table_used" ;
      ObSysVars[201].data_type_ = ObIntType ;
      ObSysVars[201].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[201].id_ = SYS_VAR__OB_PROXY_SESSION_TEMPORARY_TABLE_USED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_PROXY_SESSION_TEMPORARY_TABLE_USED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OB_PROXY_SESSION_TEMPORARY_TABLE_USED] = 201 ;
      ObSysVars[201].base_value_ = "0" ;
    ObSysVars[201].alias_ = "OB_SV__OB_PROXY_SESSION_TEMPORARY_TABLE_USED" ;
    }();

    [&] (){
      ObSysVars[202].default_value_ = "1" ;
      ObSysVars[202].info_ = "A DDL statement can be parallelized only if you have explicitly enabled parallel DDL in the session or in the SQL statement." ;
      ObSysVars[202].name_ = "_enable_parallel_ddl" ;
      ObSysVars[202].data_type_ = ObIntType ;
      ObSysVars[202].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[202].id_ = SYS_VAR__ENABLE_PARALLEL_DDL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_DDL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_PARALLEL_DDL] = 202 ;
      ObSysVars[202].base_value_ = "1" ;
    ObSysVars[202].alias_ = "OB_SV__ENABLE_PARALLEL_DDL" ;
    }();

    [&] (){
      ObSysVars[203].default_value_ = "1" ;
      ObSysVars[203].info_ = "A DDL statement can be parallelized only if you have explicitly enabled parallel DDL in the session or in the SQL statement." ;
      ObSysVars[203].name_ = "_force_parallel_ddl_dop" ;
      ObSysVars[203].data_type_ = ObUInt64Type ;
      ObSysVars[203].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[203].id_ = SYS_VAR__FORCE_PARALLEL_DDL_DOP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_DDL_DOP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__FORCE_PARALLEL_DDL_DOP] = 203 ;
      ObSysVars[203].base_value_ = "1" ;
    ObSysVars[203].alias_ = "OB_SV__FORCE_PARALLEL_DDL_DOP" ;
    }();

    [&] (){
      ObSysVars[204].default_value_ = "0" ;
      ObSysVars[204].info_ = "whether needs to do parameterization? EXACT - query will not do parameterization; FORCE - query will do parameterization." ;
      ObSysVars[204].name_ = "cursor_sharing" ;
      ObSysVars[204].data_type_ = ObIntType ;
      ObSysVars[204].enum_names_ = "[u'FORCE', u'EXACT']" ;
      ObSysVars[204].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[204].id_ = SYS_VAR_CURSOR_SHARING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CURSOR_SHARING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CURSOR_SHARING] = 204 ;
      ObSysVars[204].base_value_ = "0" ;
    ObSysVars[204].alias_ = "OB_SV_CURSOR_SHARING" ;
    }();

    [&] (){
      ObSysVars[205].default_value_ = "1" ;
      ObSysVars[205].info_ = "specifies whether null aware anti join plan allow generated" ;
      ObSysVars[205].name_ = "_optimizer_null_aware_antijoin" ;
      ObSysVars[205].data_type_ = ObIntType ;
      ObSysVars[205].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[205].id_ = SYS_VAR__OPTIMIZER_NULL_AWARE_ANTIJOIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OPTIMIZER_NULL_AWARE_ANTIJOIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OPTIMIZER_NULL_AWARE_ANTIJOIN] = 205 ;
      ObSysVars[205].base_value_ = "1" ;
    ObSysVars[205].alias_ = "OB_SV__OPTIMIZER_NULL_AWARE_ANTIJOIN" ;
    }();

    [&] (){
      ObSysVars[206].default_value_ = "1" ;
      ObSysVars[206].info_ = "enable partial rollup push down optimization." ;
      ObSysVars[206].name_ = "_px_partial_rollup_pushdown" ;
      ObSysVars[206].data_type_ = ObIntType ;
      ObSysVars[206].enum_names_ = "[u'OFF', u'ADAPTIVE']" ;
      ObSysVars[206].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[206].id_ = SYS_VAR__PX_PARTIAL_ROLLUP_PUSHDOWN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_PARTIAL_ROLLUP_PUSHDOWN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__PX_PARTIAL_ROLLUP_PUSHDOWN] = 206 ;
      ObSysVars[206].base_value_ = "1" ;
    ObSysVars[206].alias_ = "OB_SV__PX_PARTIAL_ROLLUP_PUSHDOWN" ;
    }();

    [&] (){
      ObSysVars[207].default_value_ = "1" ;
      ObSysVars[207].info_ = "enable distinct aggregate function to partial rollup push down optimization." ;
      ObSysVars[207].name_ = "_px_dist_agg_partial_rollup_pushdown" ;
      ObSysVars[207].data_type_ = ObIntType ;
      ObSysVars[207].enum_names_ = "[u'OFF', u'ADAPTIVE']" ;
      ObSysVars[207].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[207].id_ = SYS_VAR__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN] = 207 ;
      ObSysVars[207].base_value_ = "1" ;
    ObSysVars[207].alias_ = "OB_SV__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN" ;
    }();

    [&] (){
      ObSysVars[208].default_value_ = "" ;
      ObSysVars[208].info_ = "control audit log trail job in mysql mode" ;
      ObSysVars[208].name_ = "_create_audit_purge_job" ;
      ObSysVars[208].data_type_ = ObVarcharType ;
      ObSysVars[208].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[208].id_ = SYS_VAR__CREATE_AUDIT_PURGE_JOB ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__CREATE_AUDIT_PURGE_JOB)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__CREATE_AUDIT_PURGE_JOB] = 208 ;
      ObSysVars[208].base_value_ = "" ;
    ObSysVars[208].alias_ = "OB_SV__CREATE_AUDIT_PURGE_JOB" ;
    }();

    [&] (){
      ObSysVars[209].default_value_ = "" ;
      ObSysVars[209].info_ = "drop audit log trail job in mysql mode" ;
      ObSysVars[209].name_ = "_drop_audit_purge_job" ;
      ObSysVars[209].data_type_ = ObVarcharType ;
      ObSysVars[209].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[209].id_ = SYS_VAR__DROP_AUDIT_PURGE_JOB ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__DROP_AUDIT_PURGE_JOB)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__DROP_AUDIT_PURGE_JOB] = 209 ;
      ObSysVars[209].base_value_ = "" ;
    ObSysVars[209].alias_ = "OB_SV__DROP_AUDIT_PURGE_JOB" ;
    }();

    [&] (){
      ObSysVars[210].default_value_ = "" ;
      ObSysVars[210].info_ = "set purge job interval in mysql mode, range in 1-999 days" ;
      ObSysVars[210].name_ = "_set_purge_job_interval" ;
      ObSysVars[210].data_type_ = ObVarcharType ;
      ObSysVars[210].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[210].id_ = SYS_VAR__SET_PURGE_JOB_INTERVAL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__SET_PURGE_JOB_INTERVAL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__SET_PURGE_JOB_INTERVAL] = 210 ;
      ObSysVars[210].base_value_ = "" ;
    ObSysVars[210].alias_ = "OB_SV__SET_PURGE_JOB_INTERVAL" ;
    }();

    [&] (){
      ObSysVars[211].default_value_ = "" ;
      ObSysVars[211].info_ = "set purge job status in mysql mode, range: true/false" ;
      ObSysVars[211].name_ = "_set_purge_job_status" ;
      ObSysVars[211].data_type_ = ObVarcharType ;
      ObSysVars[211].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[211].id_ = SYS_VAR__SET_PURGE_JOB_STATUS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__SET_PURGE_JOB_STATUS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__SET_PURGE_JOB_STATUS] = 211 ;
      ObSysVars[211].base_value_ = "" ;
    ObSysVars[211].alias_ = "OB_SV__SET_PURGE_JOB_STATUS" ;
    }();

    [&] (){
      ObSysVars[212].default_value_ = "" ;
      ObSysVars[212].info_ = "set last archive timestamp in mysql mode, must utc time in usec from 1970" ;
      ObSysVars[212].name_ = "_set_last_archive_timestamp" ;
      ObSysVars[212].data_type_ = ObVarcharType ;
      ObSysVars[212].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[212].id_ = SYS_VAR__SET_LAST_ARCHIVE_TIMESTAMP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__SET_LAST_ARCHIVE_TIMESTAMP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__SET_LAST_ARCHIVE_TIMESTAMP] = 212 ;
      ObSysVars[212].base_value_ = "" ;
    ObSysVars[212].alias_ = "OB_SV__SET_LAST_ARCHIVE_TIMESTAMP" ;
    }();

    [&] (){
      ObSysVars[213].default_value_ = "" ;
      ObSysVars[213].info_ = "clear last archive timestamp in mysql mode" ;
      ObSysVars[213].name_ = "_clear_last_archive_timestamp" ;
      ObSysVars[213].data_type_ = ObVarcharType ;
      ObSysVars[213].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[213].id_ = SYS_VAR__CLEAR_LAST_ARCHIVE_TIMESTAMP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__CLEAR_LAST_ARCHIVE_TIMESTAMP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__CLEAR_LAST_ARCHIVE_TIMESTAMP] = 213 ;
      ObSysVars[213].base_value_ = "" ;
    ObSysVars[213].alias_ = "OB_SV__CLEAR_LAST_ARCHIVE_TIMESTAMP" ;
    }();

    [&] (){
      ObSysVars[214].default_value_ = "0" ;
      ObSysVars[214].info_ = "Manually control some behaviors of aggregation" ;
      ObSysVars[214].name_ = "_aggregation_optimization_settings" ;
      ObSysVars[214].data_type_ = ObUInt64Type ;
      ObSysVars[214].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[214].id_ = SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS] = 214 ;
      ObSysVars[214].base_value_ = "0" ;
    ObSysVars[214].alias_ = "OB_SV__AGGREGATION_OPTIMIZATION_SETTINGS" ;
    }();

    [&] (){
      ObSysVars[215].default_value_ = "1" ;
      ObSysVars[215].info_ = "enable shared hash table hash join optimization." ;
      ObSysVars[215].name_ = "_px_shared_hash_join" ;
      ObSysVars[215].data_type_ = ObIntType ;
      ObSysVars[215].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[215].id_ = SYS_VAR__PX_SHARED_HASH_JOIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_SHARED_HASH_JOIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__PX_SHARED_HASH_JOIN] = 215 ;
      ObSysVars[215].base_value_ = "1" ;
    ObSysVars[215].alias_ = "OB_SV__PX_SHARED_HASH_JOIN" ;
    }();

    [&] (){
      ObSysVars[216].default_value_ = "0" ;
      ObSysVars[216].info_ = "" ;
      ObSysVars[216].name_ = "sql_notes" ;
      ObSysVars[216].data_type_ = ObIntType ;
      ObSysVars[216].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[216].id_ = SYS_VAR_SQL_NOTES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_NOTES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_NOTES] = 216 ;
      ObSysVars[216].base_value_ = "0" ;
    ObSysVars[216].alias_ = "OB_SV_SQL_NOTES" ;
    }();

    [&] (){
      ObSysVars[217].default_value_ = "1" ;
      ObSysVars[217].info_ = "in certain case, warnings would be transformed to errors" ;
      ObSysVars[217].name_ = "innodb_strict_mode" ;
      ObSysVars[217].data_type_ = ObIntType ;
      ObSysVars[217].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[217].id_ = SYS_VAR_INNODB_STRICT_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STRICT_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STRICT_MODE] = 217 ;
      ObSysVars[217].base_value_ = "1" ;
    ObSysVars[217].alias_ = "OB_SV_INNODB_STRICT_MODE" ;
    }();

    [&] (){
      ObSysVars[218].default_value_ = "0" ;
      ObSysVars[218].info_ = "settings for window function optimizations" ;
      ObSysVars[218].name_ = "_windowfunc_optimization_settings" ;
      ObSysVars[218].data_type_ = ObUInt64Type ;
      ObSysVars[218].min_val_ = "0" ;
      ObSysVars[218].max_val_ = "9223372036854775807" ;
      ObSysVars[218].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[218].id_ = SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS] = 218 ;
      ObSysVars[218].base_value_ = "0" ;
    ObSysVars[218].alias_ = "OB_SV__WINDOWFUNC_OPTIMIZATION_SETTINGS" ;
    }();

    [&] (){
      ObSysVars[219].default_value_ = "0" ;
      ObSysVars[219].info_ = "control whether print svr_ip,execute_time,trace_id" ;
      ObSysVars[219].name_ = "ob_enable_rich_error_msg" ;
      ObSysVars[219].data_type_ = ObIntType ;
      ObSysVars[219].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[219].id_ = SYS_VAR_OB_ENABLE_RICH_ERROR_MSG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_RICH_ERROR_MSG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_RICH_ERROR_MSG] = 219 ;
      ObSysVars[219].base_value_ = "0" ;
    ObSysVars[219].alias_ = "OB_SV_ENABLE_RICH_ERROR_MSG" ;
    }();

    [&] (){
      ObSysVars[220].default_value_ = "" ;
      ObSysVars[220].info_ = "control whether lob use partial update" ;
      ObSysVars[220].name_ = "log_row_value_options" ;
      ObSysVars[220].data_type_ = ObVarcharType ;
      ObSysVars[220].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[220].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_log_row_value_option_is_valid" ;
      ObSysVars[220].id_ = SYS_VAR_LOG_ROW_VALUE_OPTIONS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOG_ROW_VALUE_OPTIONS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOG_ROW_VALUE_OPTIONS] = 220 ;
      ObSysVars[220].base_value_ = "" ;
    ObSysVars[220].alias_ = "OB_SV_LOG_ROW_VALUE_OPTIONS" ;
    }();

    [&] (){
      ObSysVars[221].default_value_ = "-1" ;
      ObSysVars[221].info_ = "max stale time(us) for weak read query " ;
      ObSysVars[221].name_ = "ob_max_read_stale_time" ;
      ObSysVars[221].data_type_ = ObIntType ;
      ObSysVars[221].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[221].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large" ;
      ObSysVars[221].id_ = SYS_VAR_OB_MAX_READ_STALE_TIME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_MAX_READ_STALE_TIME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_MAX_READ_STALE_TIME] = 221 ;
      ObSysVars[221].base_value_ = "-1" ;
    ObSysVars[221].alias_ = "OB_SV_MAX_READ_STALE_TIME" ;
    }();

    [&] (){
      ObSysVars[222].default_value_ = "1" ;
      ObSysVars[222].info_ = "control wether we need to gather optimizer stats on insert into select/create table as select" ;
      ObSysVars[222].name_ = "_optimizer_gather_stats_on_load" ;
      ObSysVars[222].data_type_ = ObIntType ;
      ObSysVars[222].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[222].id_ = SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD] = 222 ;
      ObSysVars[222].base_value_ = "1" ;
    ObSysVars[222].alias_ = "OB_SV__OPTIMIZER_GATHER_STATS_ON_LOAD" ;
    }();

    [&] (){
      ObSysVars[223].default_value_ = "" ;
      ObSysVars[223].info_ = "used in the dblink write transaction, the TM side informs the RM side of the necessary information about establishing a reverse dblink by setting system variables" ;
      ObSysVars[223].name_ = "_set_reverse_dblink_infos" ;
      ObSysVars[223].data_type_ = ObVarcharType ;
      ObSysVars[223].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[223].id_ = SYS_VAR__SET_REVERSE_DBLINK_INFOS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__SET_REVERSE_DBLINK_INFOS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__SET_REVERSE_DBLINK_INFOS] = 223 ;
      ObSysVars[223].base_value_ = "" ;
    ObSysVars[223].alias_ = "OB_SV__SET_REVERSE_DBLINK_INFOS" ;
    }();

    [&] (){
      ObSysVars[224].default_value_ = "0" ;
      ObSysVars[224].info_ = "can control the behavior of set query, when true, set query will generate a serial plan, which ensure the output order of result set is ordered " ;
      ObSysVars[224].name_ = "_force_order_preserve_set" ;
      ObSysVars[224].data_type_ = ObIntType ;
      ObSysVars[224].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[224].id_ = SYS_VAR__FORCE_ORDER_PRESERVE_SET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__FORCE_ORDER_PRESERVE_SET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__FORCE_ORDER_PRESERVE_SET] = 224 ;
      ObSysVars[224].base_value_ = "0" ;
    ObSysVars[224].alias_ = "OB_SV__FORCE_ORDER_PRESERVE_SET" ;
    }();

    [&] (){
      ObSysVars[225].default_value_ = "0" ;
      ObSysVars[225].info_ = "When enabled, show create table will show the strict compatible results with the compatibility mode." ;
      ObSysVars[225].name_ = "_show_ddl_in_compat_mode" ;
      ObSysVars[225].data_type_ = ObIntType ;
      ObSysVars[225].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[225].id_ = SYS_VAR__SHOW_DDL_IN_COMPAT_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__SHOW_DDL_IN_COMPAT_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__SHOW_DDL_IN_COMPAT_MODE] = 225 ;
      ObSysVars[225].base_value_ = "0" ;
    ObSysVars[225].alias_ = "OB_SV__SHOW_DDL_IN_COMPAT_MODE" ;
    }();

    [&] (){
      ObSysVars[226].default_value_ = "0" ;
      ObSysVars[226].info_ = "specifies whether automatic degree of parallelism will be enabled" ;
      ObSysVars[226].name_ = "parallel_degree_policy" ;
      ObSysVars[226].data_type_ = ObIntType ;
      ObSysVars[226].enum_names_ = "[u'MANUAL', u'AUTO']" ;
      ObSysVars[226].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[226].id_ = SYS_VAR_PARALLEL_DEGREE_POLICY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PARALLEL_DEGREE_POLICY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PARALLEL_DEGREE_POLICY] = 226 ;
      ObSysVars[226].base_value_ = "0" ;
    ObSysVars[226].alias_ = "OB_SV_PARALLEL_DEGREE_POLICY" ;
    }();

    [&] (){
      ObSysVars[227].default_value_ = "0" ;
      ObSysVars[227].info_ = "limits the degree of parallelism used by the optimizer when automatic degree of parallelism is enabled" ;
      ObSysVars[227].name_ = "parallel_degree_limit" ;
      ObSysVars[227].data_type_ = ObUInt64Type ;
      ObSysVars[227].min_val_ = "0" ;
      ObSysVars[227].max_val_ = "9223372036854775807" ;
      ObSysVars[227].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[227].id_ = SYS_VAR_PARALLEL_DEGREE_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PARALLEL_DEGREE_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PARALLEL_DEGREE_LIMIT] = 227 ;
      ObSysVars[227].base_value_ = "0" ;
    ObSysVars[227].alias_ = "OB_SV_PARALLEL_DEGREE_LIMIT" ;
    }();

    [&] (){
      ObSysVars[228].default_value_ = "1000" ;
      ObSysVars[228].info_ = "specifies the minimum execution time a table scan should have before it's considered for automatic degree of parallelism, variable unit is milliseconds" ;
      ObSysVars[228].name_ = "parallel_min_scan_time_threshold" ;
      ObSysVars[228].data_type_ = ObUInt64Type ;
      ObSysVars[228].min_val_ = "10" ;
      ObSysVars[228].max_val_ = "9223372036854775807" ;
      ObSysVars[228].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[228].id_ = SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD] = 228 ;
      ObSysVars[228].base_value_ = "1000" ;
    ObSysVars[228].alias_ = "OB_SV_PARALLEL_MIN_SCAN_TIME_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[229].default_value_ = "1" ;
      ObSysVars[229].info_ = "control optimizer dynamic sample level" ;
      ObSysVars[229].name_ = "optimizer_dynamic_sampling" ;
      ObSysVars[229].data_type_ = ObUInt64Type ;
      ObSysVars[229].min_val_ = "0" ;
      ObSysVars[229].max_val_ = "1" ;
      ObSysVars[229].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[229].id_ = SYS_VAR_OPTIMIZER_DYNAMIC_SAMPLING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_DYNAMIC_SAMPLING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_DYNAMIC_SAMPLING] = 229 ;
      ObSysVars[229].base_value_ = "1" ;
    ObSysVars[229].alias_ = "OB_SV_OPTIMIZER_DYNAMIC_SAMPLING" ;
    }();

    [&] (){
      ObSysVars[230].default_value_ = "BLOOM_FILTER,RANGE,IN" ;
      ObSysVars[230].info_ = "set runtime filter type, including the bloom_filter/range/in filter" ;
      ObSysVars[230].name_ = "runtime_filter_type" ;
      ObSysVars[230].data_type_ = ObVarcharType ;
      ObSysVars[230].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[230].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_runtime_filter_type_is_valid" ;
      ObSysVars[230].id_ = SYS_VAR_RUNTIME_FILTER_TYPE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RUNTIME_FILTER_TYPE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RUNTIME_FILTER_TYPE] = 230 ;
      ObSysVars[230].base_value_ = "BLOOM_FILTER,RANGE,IN" ;
    ObSysVars[230].alias_ = "OB_SV_RUNTIME_FILTER_TYPE" ;
    }();

    [&] (){
      ObSysVars[231].default_value_ = "10" ;
      ObSysVars[231].info_ = "set default wait time ms for runtime filter, default is 10ms" ;
      ObSysVars[231].name_ = "runtime_filter_wait_time_ms" ;
      ObSysVars[231].data_type_ = ObIntType ;
      ObSysVars[231].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[231].id_ = SYS_VAR_RUNTIME_FILTER_WAIT_TIME_MS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RUNTIME_FILTER_WAIT_TIME_MS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RUNTIME_FILTER_WAIT_TIME_MS] = 231 ;
      ObSysVars[231].base_value_ = "10" ;
    ObSysVars[231].alias_ = "OB_SV_RUNTIME_FILTER_WAIT_TIME_MS" ;
    }();

    [&] (){
      ObSysVars[232].default_value_ = "1024" ;
      ObSysVars[232].info_ = "set max in number for runtime in filter, default is 1024" ;
      ObSysVars[232].name_ = "runtime_filter_max_in_num" ;
      ObSysVars[232].data_type_ = ObIntType ;
      ObSysVars[232].min_val_ = "0" ;
      ObSysVars[232].max_val_ = "10240" ;
      ObSysVars[232].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[232].id_ = SYS_VAR_RUNTIME_FILTER_MAX_IN_NUM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RUNTIME_FILTER_MAX_IN_NUM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RUNTIME_FILTER_MAX_IN_NUM] = 232 ;
      ObSysVars[232].base_value_ = "1024" ;
    ObSysVars[232].alias_ = "OB_SV_RUNTIME_FILTER_MAX_IN_NUM" ;
    }();

    [&] (){
      ObSysVars[233].default_value_ = "2147483648" ;
      ObSysVars[233].info_ = "set max size for single runtime bloom filter, default is 2GB" ;
      ObSysVars[233].name_ = "runtime_bloom_filter_max_size" ;
      ObSysVars[233].data_type_ = ObIntType ;
      ObSysVars[233].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[233].id_ = SYS_VAR_RUNTIME_BLOOM_FILTER_MAX_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RUNTIME_BLOOM_FILTER_MAX_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RUNTIME_BLOOM_FILTER_MAX_SIZE] = 233 ;
      ObSysVars[233].base_value_ = "2147483648" ;
    ObSysVars[233].alias_ = "OB_SV_RUNTIME_BLOOM_FILTER_MAX_SIZE" ;
    }();

    [&] (){
      ObSysVars[234].default_value_ = "4.3.3.0" ;
      ObSysVars[234].info_ = "enabling a series of optimizer features based on an OceanBase release number" ;
      ObSysVars[234].name_ = "optimizer_features_enable" ;
      ObSysVars[234].data_type_ = ObVarcharType ;
      ObSysVars[234].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[234].id_ = SYS_VAR_OPTIMIZER_FEATURES_ENABLE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_FEATURES_ENABLE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_FEATURES_ENABLE] = 234 ;
      ObSysVars[234].base_value_ = "" ;
    ObSysVars[234].alias_ = "OB_SV_OPTIMIZER_FEATURES_ENABLE" ;
    }();

    [&] (){
      ObSysVars[235].default_value_ = "0" ;
      ObSysVars[235].info_ = "In the weak read state, the replica status of the current machine is fed back to the proxy." ;
      ObSysVars[235].name_ = "_ob_proxy_weakread_feedback" ;
      ObSysVars[235].data_type_ = ObIntType ;
      ObSysVars[235].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[235].id_ = SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK] = 235 ;
      ObSysVars[235].base_value_ = "0" ;
    ObSysVars[235].alias_ = "OB_SV__OB_PROXY_WEAKREAD_FEEDBACK" ;
    }();

    [&] (){
      ObSysVars[236].default_value_ = "0" ;
      ObSysVars[236].info_ = "The national character set which should be translated to response nstring data" ;
      ObSysVars[236].name_ = "ncharacter_set_connection" ;
      ObSysVars[236].data_type_ = ObIntType ;
      ObSysVars[236].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset" ;
      ObSysVars[236].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::NULLABLE ;
      ObSysVars[236].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[236].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset" ;
      ObSysVars[236].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset" ;
      ObSysVars[236].id_ = SYS_VAR_NCHARACTER_SET_CONNECTION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NCHARACTER_SET_CONNECTION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NCHARACTER_SET_CONNECTION] = 236 ;
      ObSysVars[236].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[236].base_value_ = "0" ;
    ObSysVars[236].alias_ = "OB_SV_NCHARACTER_SET_CONNECTION" ;
    }();

    [&] (){
      ObSysVars[237].default_value_ = "1" ;
      ObSysVars[237].info_ = "the server automatically grants the EXECUTE and ALTER ROUTINE privileges to the creator of a stored routine" ;
      ObSysVars[237].name_ = "automatic_sp_privileges" ;
      ObSysVars[237].data_type_ = ObIntType ;
      ObSysVars[237].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[237].id_ = SYS_VAR_AUTOMATIC_SP_PRIVILEGES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTOMATIC_SP_PRIVILEGES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_AUTOMATIC_SP_PRIVILEGES] = 237 ;
      ObSysVars[237].base_value_ = "1" ;
    ObSysVars[237].alias_ = "OB_SV_AUTOMATIC_SP_PRIVILEGES" ;
    }();

    [&] (){
      ObSysVars[238].default_value_ = "" ;
      ObSysVars[238].info_ = "enabling a series of privilege features based on an OceanBase release number" ;
      ObSysVars[238].name_ = "privilege_features_enable" ;
      ObSysVars[238].data_type_ = ObVarcharType ;
      ObSysVars[238].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[238].id_ = SYS_VAR_PRIVILEGE_FEATURES_ENABLE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PRIVILEGE_FEATURES_ENABLE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PRIVILEGE_FEATURES_ENABLE] = 238 ;
      ObSysVars[238].base_value_ = "" ;
    ObSysVars[238].alias_ = "OB_SV_PRIVILEGE_FEATURES_ENABLE" ;
    }();

    [&] (){
      ObSysVars[239].default_value_ = "" ;
      ObSysVars[239].info_ = "whether turn on mysql privilege check" ;
      ObSysVars[239].name_ = "_priv_control" ;
      ObSysVars[239].data_type_ = ObVarcharType ;
      ObSysVars[239].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[239].id_ = SYS_VAR__PRIV_CONTROL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PRIV_CONTROL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__PRIV_CONTROL] = 239 ;
      ObSysVars[239].base_value_ = "" ;
    ObSysVars[239].alias_ = "OB_SV__PRIV_CONTROL" ;
    }();

    [&] (){
      ObSysVars[240].default_value_ = "0" ;
      ObSysVars[240].info_ = "specifies whether check the mysql routine priv" ;
      ObSysVars[240].name_ = "_enable_mysql_pl_priv_check" ;
      ObSysVars[240].data_type_ = ObIntType ;
      ObSysVars[240].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[240].id_ = SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK] = 240 ;
      ObSysVars[240].base_value_ = "0" ;
    ObSysVars[240].alias_ = "OB_SV__ENABLE_MYSQL_PL_PRIV_CHECK" ;
    }();

    [&] (){
      ObSysVars[241].default_value_ = "1" ;
      ObSysVars[241].info_ = "whether use pl cache in session" ;
      ObSysVars[241].name_ = "ob_enable_pl_cache" ;
      ObSysVars[241].data_type_ = ObIntType ;
      ObSysVars[241].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[241].id_ = SYS_VAR_OB_ENABLE_PL_CACHE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_PL_CACHE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_PL_CACHE] = 241 ;
      ObSysVars[241].base_value_ = "1" ;
    ObSysVars[241].alias_ = "OB_SV_ENABLE_PL_CACHE" ;
    }();

    [&] (){
      ObSysVars[242].default_value_ = "8192" ;
      ObSysVars[242].info_ = "default lob inrow threshold config" ;
      ObSysVars[242].name_ = "ob_default_lob_inrow_threshold" ;
      ObSysVars[242].data_type_ = ObIntType ;
      ObSysVars[242].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[242].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_default_lob_inrow_threshold" ;
      ObSysVars[242].id_ = SYS_VAR_OB_DEFAULT_LOB_INROW_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_DEFAULT_LOB_INROW_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_DEFAULT_LOB_INROW_THRESHOLD] = 242 ;
      ObSysVars[242].base_value_ = "4096" ;
    ObSysVars[242].alias_ = "OB_SV_DEFAULT_LOB_INROW_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[243].default_value_ = "1" ;
      ObSysVars[243].info_ = "whether use storage cardinality estimation" ;
      ObSysVars[243].name_ = "_enable_storage_cardinality_estimation" ;
      ObSysVars[243].data_type_ = ObIntType ;
      ObSysVars[243].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[243].id_ = SYS_VAR__ENABLE_STORAGE_CARDINALITY_ESTIMATION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_STORAGE_CARDINALITY_ESTIMATION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_STORAGE_CARDINALITY_ESTIMATION] = 243 ;
      ObSysVars[243].base_value_ = "1" ;
    ObSysVars[243].alias_ = "OB_SV__ENABLE_STORAGE_CARDINALITY_ESTIMATION" ;
    }();

    [&] (){
      ObSysVars[244].default_value_ = "en_US" ;
      ObSysVars[244].info_ = "The locale indicated by the lc_time_names system variable controls the language used to display day and month names and abbreviations" ;
      ObSysVars[244].name_ = "lc_time_names" ;
      ObSysVars[244].data_type_ = ObVarcharType ;
      ObSysVars[244].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[244].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_locale_type_is_valid" ;
      ObSysVars[244].id_ = SYS_VAR_LC_TIME_NAMES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LC_TIME_NAMES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LC_TIME_NAMES] = 244 ;
      ObSysVars[244].base_value_ = "en_US" ;
    ObSysVars[244].alias_ = "OB_SV_LC_TIME_NAMES" ;
    }();

    [&] (){
      ObSysVars[245].default_value_ = "0" ;
      ObSysVars[245].info_ = "whether to enable automatic activation of all granted roles when users log in to the server" ;
      ObSysVars[245].name_ = "activate_all_roles_on_login" ;
      ObSysVars[245].data_type_ = ObIntType ;
      ObSysVars[245].flags_ = ObSysVarFlag::GLOBAL_SCOPE ;
      ObSysVars[245].id_ = SYS_VAR_ACTIVATE_ALL_ROLES_ON_LOGIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_ACTIVATE_ALL_ROLES_ON_LOGIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_ACTIVATE_ALL_ROLES_ON_LOGIN] = 245 ;
      ObSysVars[245].base_value_ = "0" ;
    ObSysVars[245].alias_ = "OB_SV_ACTIVATE_ALL_ROLES_ON_LOGIN" ;
    }();

    [&] (){
      ObSysVars[246].default_value_ = "1" ;
      ObSysVars[246].info_ = "whether use rich vector format in vectorized execution engine" ;
      ObSysVars[246].name_ = "_enable_rich_vector_format" ;
      ObSysVars[246].data_type_ = ObIntType ;
      ObSysVars[246].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[246].id_ = SYS_VAR__ENABLE_RICH_VECTOR_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_RICH_VECTOR_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_RICH_VECTOR_FORMAT] = 246 ;
      ObSysVars[246].base_value_ = "1" ;
    ObSysVars[246].alias_ = "OB_SV__ENABLE_RICH_VECTOR_FORMAT" ;
    }();

    [&] (){
      ObSysVars[247].default_value_ = "1" ;
      ObSysVars[247].info_ = "Specifies whether InnoDB index statistics are persisted to disk." ;
      ObSysVars[247].name_ = "innodb_stats_persistent" ;
      ObSysVars[247].data_type_ = ObIntType ;
      ObSysVars[247].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[247].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[247].id_ = SYS_VAR_INNODB_STATS_PERSISTENT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_PERSISTENT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_PERSISTENT] = 247 ;
      ObSysVars[247].base_value_ = "1" ;
    ObSysVars[247].alias_ = "OB_SV_INNODB_STATS_PERSISTENT" ;
    }();

    [&] (){
      ObSysVars[248].default_value_ = "d:t:i:o,/tmp/mysqld.trace" ;
      ObSysVars[248].info_ = "This variable indicates the current debugging settings" ;
      ObSysVars[248].name_ = "debug" ;
      ObSysVars[248].data_type_ = ObVarcharType ;
      ObSysVars[248].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[248].id_ = SYS_VAR_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEBUG] = 248 ;
      ObSysVars[248].base_value_ = "d:t:i:o,/tmp/mysqld.trace" ;
    ObSysVars[248].alias_ = "OB_SV_DEBUG" ;
    }();

    [&] (){
      ObSysVars[249].default_value_ = "0" ;
      ObSysVars[249].info_ = "Sets a debug flag for InnoDB change buffering. " ;
      ObSysVars[249].name_ = "innodb_change_buffering_debug" ;
      ObSysVars[249].data_type_ = ObIntType ;
      ObSysVars[249].min_val_ = "0" ;
      ObSysVars[249].max_val_ = "2" ;
      ObSysVars[249].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[249].id_ = SYS_VAR_INNODB_CHANGE_BUFFERING_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_CHANGE_BUFFERING_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_CHANGE_BUFFERING_DEBUG] = 249 ;
      ObSysVars[249].base_value_ = "0" ;
    ObSysVars[249].alias_ = "OB_SV_INNODB_CHANGE_BUFFERING_DEBUG" ;
    }();

    [&] (){
      ObSysVars[250].default_value_ = "0" ;
      ObSysVars[250].info_ = "Compresses all tables using a specified compression algorithm without having to define a COMPRESSION attribute for each table." ;
      ObSysVars[250].name_ = "innodb_compress_debug" ;
      ObSysVars[250].data_type_ = ObIntType ;
      ObSysVars[250].enum_names_ = "[u'NONE', u'ZLIB', u'LZ4', u'LZ4HC']" ;
      ObSysVars[250].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[250].id_ = SYS_VAR_INNODB_COMPRESS_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_COMPRESS_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_COMPRESS_DEBUG] = 250 ;
      ObSysVars[250].base_value_ = "0" ;
    ObSysVars[250].alias_ = "OB_SV_INNODB_COMPRESS_DEBUG" ;
    }();

    [&] (){
      ObSysVars[251].default_value_ = "1" ;
      ObSysVars[251].info_ = "Disables resizing of the InnoDB buffer pool" ;
      ObSysVars[251].name_ = "innodb_disable_resize_buffer_pool_debug" ;
      ObSysVars[251].data_type_ = ObIntType ;
      ObSysVars[251].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[251].id_ = SYS_VAR_INNODB_DISABLE_RESIZE_BUFFER_POOL_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_DISABLE_RESIZE_BUFFER_POOL_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_DISABLE_RESIZE_BUFFER_POOL_DEBUG] = 251 ;
      ObSysVars[251].base_value_ = "1" ;
    ObSysVars[251].alias_ = "OB_SV_INNODB_DISABLE_RESIZE_BUFFER_POOL_DEBUG" ;
    }();

    [&] (){
      ObSysVars[252].default_value_ = "0" ;
      ObSysVars[252].info_ = "set to 1 (the default by MySQL), foreign key constraints are checked. If set to 0, foreign key constraints are ignored" ;
      ObSysVars[252].name_ = "innodb_fil_make_page_dirty_debug" ;
      ObSysVars[252].data_type_ = ObIntType ;
      ObSysVars[252].min_val_ = "0" ;
      ObSysVars[252].max_val_ = "4294967295" ;
      ObSysVars[252].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[252].id_ = SYS_VAR_INNODB_FIL_MAKE_PAGE_DIRTY_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FIL_MAKE_PAGE_DIRTY_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FIL_MAKE_PAGE_DIRTY_DEBUG] = 252 ;
      ObSysVars[252].base_value_ = "0" ;
    ObSysVars[252].alias_ = "OB_SV_INNODB_FIL_MAKE_PAGE_DIRTY_DEBUG" ;
    }();

    [&] (){
      ObSysVars[253].default_value_ = "0" ;
      ObSysVars[253].info_ = "Limits the number of records per B-tree page" ;
      ObSysVars[253].name_ = "innodb_limit_optimistic_insert_debug" ;
      ObSysVars[253].data_type_ = ObIntType ;
      ObSysVars[253].min_val_ = "0" ;
      ObSysVars[253].max_val_ = "4294967295" ;
      ObSysVars[253].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[253].id_ = SYS_VAR_INNODB_LIMIT_OPTIMISTIC_INSERT_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LIMIT_OPTIMISTIC_INSERT_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LIMIT_OPTIMISTIC_INSERT_DEBUG] = 253 ;
      ObSysVars[253].base_value_ = "0" ;
    ObSysVars[253].alias_ = "OB_SV_INNODB_LIMIT_OPTIMISTIC_INSERT_DEBUG" ;
    }();

    [&] (){
      ObSysVars[254].default_value_ = "50" ;
      ObSysVars[254].info_ = "Defines a page-full percentage value for index pages that overrides the current MERGE_THRESHOLD setting for all indexes that are currently in the dictionary cache" ;
      ObSysVars[254].name_ = "innodb_merge_threshold_set_all_debug" ;
      ObSysVars[254].data_type_ = ObIntType ;
      ObSysVars[254].min_val_ = "1" ;
      ObSysVars[254].max_val_ = "50" ;
      ObSysVars[254].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[254].id_ = SYS_VAR_INNODB_MERGE_THRESHOLD_SET_ALL_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_MERGE_THRESHOLD_SET_ALL_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_MERGE_THRESHOLD_SET_ALL_DEBUG] = 254 ;
      ObSysVars[254].base_value_ = "50" ;
    ObSysVars[254].alias_ = "OB_SV_INNODB_MERGE_THRESHOLD_SET_ALL_DEBUG" ;
    }();

    [&] (){
      ObSysVars[255].default_value_ = "0" ;
      ObSysVars[255].info_ = "Saves a page number. Setting the innodb_fil_make_page_dirty_debug option dirties the page defined by innodb_saved_page_number_debug" ;
      ObSysVars[255].name_ = "innodb_saved_page_number_debug" ;
      ObSysVars[255].data_type_ = ObIntType ;
      ObSysVars[255].min_val_ = "0" ;
      ObSysVars[255].max_val_ = "4294967295" ;
      ObSysVars[255].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[255].id_ = SYS_VAR_INNODB_SAVED_PAGE_NUMBER_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_SAVED_PAGE_NUMBER_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_SAVED_PAGE_NUMBER_DEBUG] = 255 ;
      ObSysVars[255].base_value_ = "0" ;
    ObSysVars[255].alias_ = "OB_SV_INNODB_SAVED_PAGE_NUMBER_DEBUG" ;
    }();

    [&] (){
      ObSysVars[256].default_value_ = "0" ;
      ObSysVars[256].info_ = "Pauses purging of delete-marked records while allowing the purge view to be updated" ;
      ObSysVars[256].name_ = "innodb_trx_purge_view_update_only_debug" ;
      ObSysVars[256].data_type_ = ObIntType ;
      ObSysVars[256].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[256].id_ = SYS_VAR_INNODB_TRX_PURGE_VIEW_UPDATE_ONLY_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_TRX_PURGE_VIEW_UPDATE_ONLY_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_TRX_PURGE_VIEW_UPDATE_ONLY_DEBUG] = 256 ;
      ObSysVars[256].base_value_ = "0" ;
    ObSysVars[256].alias_ = "OB_SV_INNODB_TRX_PURGE_VIEW_UPDATE_ONLY_DEBUG" ;
    }();

    [&] (){
      ObSysVars[257].default_value_ = "0" ;
      ObSysVars[257].info_ = "Sets a debug flag that limits TRX_RSEG_N_SLOTS to a given value for the trx_rsegf_undo_find_free function that looks for free slots for undo log segments" ;
      ObSysVars[257].name_ = "innodb_trx_rseg_n_slots_debug" ;
      ObSysVars[257].data_type_ = ObIntType ;
      ObSysVars[257].min_val_ = "0" ;
      ObSysVars[257].max_val_ = "1024" ;
      ObSysVars[257].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[257].id_ = SYS_VAR_INNODB_TRX_RSEG_N_SLOTS_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_TRX_RSEG_N_SLOTS_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_TRX_RSEG_N_SLOTS_DEBUG] = 257 ;
      ObSysVars[257].base_value_ = "0" ;
    ObSysVars[257].alias_ = "OB_SV_INNODB_TRX_RSEG_N_SLOTS_DEBUG" ;
    }();

    [&] (){
      ObSysVars[258].default_value_ = "256" ;
      ObSysVars[258].info_ = "Sets a soft upper limit for the number of cached stored routines per connection" ;
      ObSysVars[258].name_ = "stored_program_cache" ;
      ObSysVars[258].data_type_ = ObIntType ;
      ObSysVars[258].min_val_ = "16" ;
      ObSysVars[258].max_val_ = "524288" ;
      ObSysVars[258].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[258].id_ = SYS_VAR_STORED_PROGRAM_CACHE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_STORED_PROGRAM_CACHE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_STORED_PROGRAM_CACHE] = 258 ;
      ObSysVars[258].base_value_ = "256" ;
    ObSysVars[258].alias_ = "OB_SV_STORED_PROGRAM_CACHE" ;
    }();

    [&] (){
      ObSysVars[259].default_value_ = "0" ;
      ObSysVars[259].info_ = "specifies the compatible mode when the behaviors of MySQL 5.7 and MySQL 8.0 are different" ;
      ObSysVars[259].name_ = "ob_compatibility_control" ;
      ObSysVars[259].data_type_ = ObIntType ;
      ObSysVars[259].enum_names_ = "[u'MYSQL5.7', u'MYSQL8.0']" ;
      ObSysVars[259].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY ;
      ObSysVars[259].id_ = SYS_VAR_OB_COMPATIBILITY_CONTROL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_COMPATIBILITY_CONTROL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_COMPATIBILITY_CONTROL] = 259 ;
      ObSysVars[259].base_value_ = "0" ;
    ObSysVars[259].alias_ = "OB_SV_COMPATIBILITY_CONTROL" ;
    }();

    [&] (){
      ObSysVars[260].default_value_ = "17180000512" ;
      ObSysVars[260].info_ = "specifies the compatible verision when the behaviors of different release version are different" ;
      ObSysVars[260].name_ = "ob_compatibility_version" ;
      ObSysVars[260].data_type_ = ObUInt64Type ;
      ObSysVars[260].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_version" ;
      ObSysVars[260].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[260].base_class_ = "ObVersionSysVar" ;
      ObSysVars[260].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_version" ;
      ObSysVars[260].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_compat_version" ;
      ObSysVars[260].id_ = SYS_VAR_OB_COMPATIBILITY_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_COMPATIBILITY_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_COMPATIBILITY_VERSION] = 260 ;
      ObSysVars[260].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[260].base_value_ = "17180000512" ;
    ObSysVars[260].alias_ = "OB_SV_COMPATIBILITY_VERSION" ;
    }();

    [&] (){
      ObSysVars[261].default_value_ = "17180000512" ;
      ObSysVars[261].info_ = "specifies the security verision when the behaviors of different release version are different" ;
      ObSysVars[261].name_ = "ob_security_version" ;
      ObSysVars[261].data_type_ = ObUInt64Type ;
      ObSysVars[261].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_version" ;
      ObSysVars[261].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[261].base_class_ = "ObVersionSysVar" ;
      ObSysVars[261].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_version" ;
      ObSysVars[261].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_security_version" ;
      ObSysVars[261].id_ = SYS_VAR_OB_SECURITY_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_SECURITY_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_SECURITY_VERSION] = 261 ;
      ObSysVars[261].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[261].base_value_ = "17180000512" ;
    ObSysVars[261].alias_ = "OB_SV_SECURITY_VERSION" ;
    }();

    [&] (){
      ObSysVars[262].default_value_ = "1" ;
      ObSysVars[262].info_ = "Specifies the correlation model when the optimizer estimates cardinality" ;
      ObSysVars[262].name_ = "cardinality_estimation_model" ;
      ObSysVars[262].data_type_ = ObIntType ;
      ObSysVars[262].enum_names_ = "[u'INDEPENDENT', u'PARTIAL', u'FULL']" ;
      ObSysVars[262].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[262].id_ = SYS_VAR_CARDINALITY_ESTIMATION_MODEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CARDINALITY_ESTIMATION_MODEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CARDINALITY_ESTIMATION_MODEL] = 262 ;
      ObSysVars[262].base_value_ = "1" ;
    ObSysVars[262].alias_ = "OB_SV_CARDINALITY_ESTIMATION_MODEL" ;
    }();

    [&] (){
      ObSysVars[263].default_value_ = "0" ;
      ObSysVars[263].info_ = "Whether to enable the materialized view rewriting" ;
      ObSysVars[263].name_ = "query_rewrite_enabled" ;
      ObSysVars[263].data_type_ = ObIntType ;
      ObSysVars[263].enum_names_ = "[u'FALSE', u'TRUE', u'FORCE']" ;
      ObSysVars[263].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[263].id_ = SYS_VAR_QUERY_REWRITE_ENABLED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_REWRITE_ENABLED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_REWRITE_ENABLED] = 263 ;
      ObSysVars[263].base_value_ = "0" ;
    ObSysVars[263].alias_ = "OB_SV_QUERY_REWRITE_ENABLED" ;
    }();

    [&] (){
      ObSysVars[264].default_value_ = "0" ;
      ObSysVars[264].info_ = "Control the data integrity of materialized view rewriting" ;
      ObSysVars[264].name_ = "query_rewrite_integrity" ;
      ObSysVars[264].data_type_ = ObIntType ;
      ObSysVars[264].enum_names_ = "[u'ENFORCED', u'STALE_TOLERATED']" ;
      ObSysVars[264].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INFLUENCE_PLAN ;
      ObSysVars[264].id_ = SYS_VAR_QUERY_REWRITE_INTEGRITY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_REWRITE_INTEGRITY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_REWRITE_INTEGRITY] = 264 ;
      ObSysVars[264].base_value_ = "0" ;
    ObSysVars[264].alias_ = "OB_SV_QUERY_REWRITE_INTEGRITY" ;
    }();

    [&] (){
      ObSysVars[265].default_value_ = "0" ;
      ObSysVars[265].info_ = "If ON, the server flushes (synchronizes) all changes to disk after each SQL statement" ;
      ObSysVars[265].name_ = "flush" ;
      ObSysVars[265].data_type_ = ObIntType ;
      ObSysVars[265].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[265].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[265].id_ = SYS_VAR_FLUSH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_FLUSH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_FLUSH] = 265 ;
      ObSysVars[265].base_value_ = "0" ;
    ObSysVars[265].alias_ = "OB_SV_FLUSH" ;
    }();

    [&] (){
      ObSysVars[266].default_value_ = "0" ;
      ObSysVars[266].info_ = "if this is set to a nonzero value, all tables are closed every flush_time seconds to free up resources and synchronize unflushed data to disk" ;
      ObSysVars[266].name_ = "flush_time" ;
      ObSysVars[266].data_type_ = ObIntType ;
      ObSysVars[266].min_val_ = "0" ;
      ObSysVars[266].max_val_ = "31536000" ;
      ObSysVars[266].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[266].id_ = SYS_VAR_FLUSH_TIME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_FLUSH_TIME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_FLUSH_TIME] = 266 ;
      ObSysVars[266].base_value_ = "0" ;
    ObSysVars[266].alias_ = "OB_SV_FLUSH_TIME" ;
    }();

    [&] (){
      ObSysVars[267].default_value_ = "1" ;
      ObSysVars[267].info_ = "specifies whether to dynamically adjust the rate of flushing dirty pages in the InnoDB buffer pool based on the workload" ;
      ObSysVars[267].name_ = "innodb_adaptive_flushing" ;
      ObSysVars[267].data_type_ = ObIntType ;
      ObSysVars[267].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[267].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[267].id_ = SYS_VAR_INNODB_ADAPTIVE_FLUSHING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_ADAPTIVE_FLUSHING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_ADAPTIVE_FLUSHING] = 267 ;
      ObSysVars[267].base_value_ = "1" ;
    ObSysVars[267].alias_ = "OB_SV_INNODB_ADAPTIVE_FLUSHING" ;
    }();

    [&] (){
      ObSysVars[268].default_value_ = "10" ;
      ObSysVars[268].info_ = "Defines the low water mark representing percentage of redo log capacity at which adaptive flushing is enabled" ;
      ObSysVars[268].name_ = "innodb_adaptive_flushing_lwm" ;
      ObSysVars[268].data_type_ = ObIntType ;
      ObSysVars[268].min_val_ = "0" ;
      ObSysVars[268].max_val_ = "70" ;
      ObSysVars[268].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[268].id_ = SYS_VAR_INNODB_ADAPTIVE_FLUSHING_LWM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_ADAPTIVE_FLUSHING_LWM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_ADAPTIVE_FLUSHING_LWM] = 268 ;
      ObSysVars[268].base_value_ = "10" ;
    ObSysVars[268].alias_ = "OB_SV_INNODB_ADAPTIVE_FLUSHING_LWM" ;
    }();

    [&] (){
      ObSysVars[269].default_value_ = "1" ;
      ObSysVars[269].info_ = "Whether the InnoDB adaptive hash index is enabled or disabled. It may be desirable, depending on your workload, to dynamically enable or disable adaptive hash indexing to improve query performance" ;
      ObSysVars[269].name_ = "innodb_adaptive_hash_index" ;
      ObSysVars[269].data_type_ = ObIntType ;
      ObSysVars[269].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[269].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[269].id_ = SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX] = 269 ;
      ObSysVars[269].base_value_ = "1" ;
    ObSysVars[269].alias_ = "OB_SV_INNODB_ADAPTIVE_HASH_INDEX" ;
    }();

    [&] (){
      ObSysVars[270].default_value_ = "8" ;
      ObSysVars[270].info_ = "Partitions the adaptive hash index search system. Each index is bound to a specific partition, with each partition protected by a separate latch." ;
      ObSysVars[270].name_ = "innodb_adaptive_hash_index_parts" ;
      ObSysVars[270].data_type_ = ObIntType ;
      ObSysVars[270].min_val_ = "1" ;
      ObSysVars[270].max_val_ = "512" ;
      ObSysVars[270].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[270].id_ = SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX_PARTS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX_PARTS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX_PARTS] = 270 ;
      ObSysVars[270].base_value_ = "8" ;
    ObSysVars[270].alias_ = "OB_SV_INNODB_ADAPTIVE_HASH_INDEX_PARTS" ;
    }();

    [&] (){
      ObSysVars[271].default_value_ = "150000" ;
      ObSysVars[271].info_ = "Permits InnoDB to automatically adjust the value of innodb_thread_sleep_delay up or down according to the current workload" ;
      ObSysVars[271].name_ = "innodb_adaptive_max_sleep_delay" ;
      ObSysVars[271].data_type_ = ObIntType ;
      ObSysVars[271].min_val_ = "0" ;
      ObSysVars[271].max_val_ = "1000000" ;
      ObSysVars[271].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[271].id_ = SYS_VAR_INNODB_ADAPTIVE_MAX_SLEEP_DELAY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_ADAPTIVE_MAX_SLEEP_DELAY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_ADAPTIVE_MAX_SLEEP_DELAY] = 271 ;
      ObSysVars[271].base_value_ = "150000" ;
    ObSysVars[271].alias_ = "OB_SV_INNODB_ADAPTIVE_MAX_SLEEP_DELAY" ;
    }();

    [&] (){
      ObSysVars[272].default_value_ = "64" ;
      ObSysVars[272].info_ = "The increment size (in megabytes) for extending the size of an auto-extending InnoDB system tablespace file when it becomes full" ;
      ObSysVars[272].name_ = "innodb_autoextend_increment" ;
      ObSysVars[272].data_type_ = ObIntType ;
      ObSysVars[272].min_val_ = "1" ;
      ObSysVars[272].max_val_ = "1000" ;
      ObSysVars[272].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[272].id_ = SYS_VAR_INNODB_AUTOEXTEND_INCREMENT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_AUTOEXTEND_INCREMENT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_AUTOEXTEND_INCREMENT] = 272 ;
      ObSysVars[272].base_value_ = "64" ;
    ObSysVars[272].alias_ = "OB_SV_INNODB_AUTOEXTEND_INCREMENT" ;
    }();

    [&] (){
      ObSysVars[273].default_value_ = "0" ;
      ObSysVars[273].info_ = "Enabling the innodb_background_drop_list_empty debug option helps avoid test case failures by delaying table creation until the background drop list is empty" ;
      ObSysVars[273].name_ = "innodb_background_drop_list_empty" ;
      ObSysVars[273].data_type_ = ObIntType ;
      ObSysVars[273].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[273].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[273].id_ = SYS_VAR_INNODB_BACKGROUND_DROP_LIST_EMPTY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BACKGROUND_DROP_LIST_EMPTY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BACKGROUND_DROP_LIST_EMPTY] = 273 ;
      ObSysVars[273].base_value_ = "0" ;
    ObSysVars[273].alias_ = "OB_SV_INNODB_BACKGROUND_DROP_LIST_EMPTY" ;
    }();

    [&] (){
      ObSysVars[274].default_value_ = "1" ;
      ObSysVars[274].info_ = "Specifies whether to record the pages cached in the InnoDB buffer pool when the MySQL server is shut down, to shorten the warmup process at the next restart" ;
      ObSysVars[274].name_ = "innodb_buffer_pool_dump_at_shutdown" ;
      ObSysVars[274].data_type_ = ObIntType ;
      ObSysVars[274].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[274].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[274].id_ = SYS_VAR_INNODB_BUFFER_POOL_DUMP_AT_SHUTDOWN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_DUMP_AT_SHUTDOWN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_DUMP_AT_SHUTDOWN] = 274 ;
      ObSysVars[274].base_value_ = "1" ;
    ObSysVars[274].alias_ = "OB_SV_INNODB_BUFFER_POOL_DUMP_AT_SHUTDOWN" ;
    }();

    [&] (){
      ObSysVars[275].default_value_ = "0" ;
      ObSysVars[275].info_ = "Immediately makes a record of pages cached in the InnoDB buffer pool" ;
      ObSysVars[275].name_ = "innodb_buffer_pool_dump_now" ;
      ObSysVars[275].data_type_ = ObIntType ;
      ObSysVars[275].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[275].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[275].id_ = SYS_VAR_INNODB_BUFFER_POOL_DUMP_NOW ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_DUMP_NOW)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_DUMP_NOW] = 275 ;
      ObSysVars[275].base_value_ = "0" ;
    ObSysVars[275].alias_ = "OB_SV_INNODB_BUFFER_POOL_DUMP_NOW" ;
    }();

    [&] (){
      ObSysVars[276].default_value_ = "25" ;
      ObSysVars[276].info_ = "Specifies the percentage of the most recently used pages for each buffer pool to read out and dump" ;
      ObSysVars[276].name_ = "innodb_buffer_pool_dump_pct" ;
      ObSysVars[276].data_type_ = ObIntType ;
      ObSysVars[276].min_val_ = "1" ;
      ObSysVars[276].max_val_ = "100" ;
      ObSysVars[276].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[276].id_ = SYS_VAR_INNODB_BUFFER_POOL_DUMP_PCT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_DUMP_PCT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_DUMP_PCT] = 276 ;
      ObSysVars[276].base_value_ = "25" ;
    ObSysVars[276].alias_ = "OB_SV_INNODB_BUFFER_POOL_DUMP_PCT" ;
    }();

    [&] (){
      ObSysVars[277].default_value_ = "ib_buffer_pool" ;
      ObSysVars[277].info_ = "Specifies the name of the file that holds the list of tablespace IDs and page IDs produced by innodb_buffer_pool_dump_at_shutdown or innodb_buffer_pool_dump_now" ;
      ObSysVars[277].name_ = "innodb_buffer_pool_filename" ;
      ObSysVars[277].data_type_ = ObVarcharType ;
      ObSysVars[277].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[277].id_ = SYS_VAR_INNODB_BUFFER_POOL_FILENAME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_FILENAME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_FILENAME] = 277 ;
      ObSysVars[277].base_value_ = "" ;
    ObSysVars[277].alias_ = "OB_SV_INNODB_BUFFER_POOL_FILENAME" ;
    }();

    [&] (){
      ObSysVars[278].default_value_ = "0" ;
      ObSysVars[278].info_ = "Interrupts the process of restoring InnoDB buffer pool contents triggered by innodb_buffer_pool_load_at_startup or innodb_buffer_pool_load_now" ;
      ObSysVars[278].name_ = "innodb_buffer_pool_load_abort" ;
      ObSysVars[278].data_type_ = ObIntType ;
      ObSysVars[278].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[278].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[278].id_ = SYS_VAR_INNODB_BUFFER_POOL_LOAD_ABORT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_LOAD_ABORT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_LOAD_ABORT] = 278 ;
      ObSysVars[278].base_value_ = "0" ;
    ObSysVars[278].alias_ = "OB_SV_INNODB_BUFFER_POOL_LOAD_ABORT" ;
    }();

    [&] (){
      ObSysVars[279].default_value_ = "0" ;
      ObSysVars[279].info_ = "Immediately warms up the InnoDB buffer pool by loading data pages without waiting for a server restart" ;
      ObSysVars[279].name_ = "innodb_buffer_pool_load_now" ;
      ObSysVars[279].data_type_ = ObIntType ;
      ObSysVars[279].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[279].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[279].id_ = SYS_VAR_INNODB_BUFFER_POOL_LOAD_NOW ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_LOAD_NOW)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_LOAD_NOW] = 279 ;
      ObSysVars[279].base_value_ = "0" ;
    ObSysVars[279].alias_ = "OB_SV_INNODB_BUFFER_POOL_LOAD_NOW" ;
    }();

    [&] (){
      ObSysVars[280].default_value_ = "134217728" ;
      ObSysVars[280].info_ = "The size in bytes of the buffer pool, the memory area where InnoDB caches table and index data" ;
      ObSysVars[280].name_ = "innodb_buffer_pool_size" ;
      ObSysVars[280].data_type_ = ObUInt64Type ;
      ObSysVars[280].min_val_ = "5242880" ;
      ObSysVars[280].max_val_ = "18446744073709551615" ;
      ObSysVars[280].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[280].id_ = SYS_VAR_INNODB_BUFFER_POOL_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_SIZE] = 280 ;
      ObSysVars[280].base_value_ = "134217728" ;
    ObSysVars[280].alias_ = "OB_SV_INNODB_BUFFER_POOL_SIZE" ;
    }();

    [&] (){
      ObSysVars[281].default_value_ = "25" ;
      ObSysVars[281].info_ = "Maximum size for the InnoDB change buffer, as a percentage of the total size of the buffer pool." ;
      ObSysVars[281].name_ = "innodb_change_buffer_max_size" ;
      ObSysVars[281].data_type_ = ObIntType ;
      ObSysVars[281].min_val_ = "0" ;
      ObSysVars[281].max_val_ = "50" ;
      ObSysVars[281].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[281].id_ = SYS_VAR_INNODB_CHANGE_BUFFER_MAX_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_CHANGE_BUFFER_MAX_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_CHANGE_BUFFER_MAX_SIZE] = 281 ;
      ObSysVars[281].base_value_ = "25" ;
    ObSysVars[281].alias_ = "OB_SV_INNODB_CHANGE_BUFFER_MAX_SIZE" ;
    }();

    [&] (){
      ObSysVars[282].default_value_ = "5" ;
      ObSysVars[282].info_ = "Whether InnoDB performs change buffering, an optimization that delays write operations to secondary indexes so that the I/O operations can be performed sequentially" ;
      ObSysVars[282].name_ = "innodb_change_buffering" ;
      ObSysVars[282].data_type_ = ObIntType ;
      ObSysVars[282].enum_names_ = "[u'none', u'inserts', u'deletes', u'changes', u'purges', u'all']" ;
      ObSysVars[282].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[282].id_ = SYS_VAR_INNODB_CHANGE_BUFFERING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_CHANGE_BUFFERING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_CHANGE_BUFFERING] = 282 ;
      ObSysVars[282].base_value_ = "5" ;
    ObSysVars[282].alias_ = "OB_SV_INNODB_CHANGE_BUFFERING" ;
    }();

    [&] (){
      ObSysVars[283].default_value_ = "0" ;
      ObSysVars[283].info_ = "Specifies how to generate and verify the checksum stored in the disk blocks of InnoDB tablespaces" ;
      ObSysVars[283].name_ = "innodb_checksum_algorithm" ;
      ObSysVars[283].data_type_ = ObIntType ;
      ObSysVars[283].enum_names_ = "[u'crc32', u'strict_crc32', u'innodb', u'strict_innodb', u'none', u'strict_none']" ;
      ObSysVars[283].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[283].id_ = SYS_VAR_INNODB_CHECKSUM_ALGORITHM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_CHECKSUM_ALGORITHM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_CHECKSUM_ALGORITHM] = 283 ;
      ObSysVars[283].base_value_ = "0" ;
    ObSysVars[283].alias_ = "OB_SV_INNODB_CHECKSUM_ALGORITHM" ;
    }();

    [&] (){
      ObSysVars[284].default_value_ = "0" ;
      ObSysVars[284].info_ = "Enables per-index compression-related statistics in the Information Schema INNODB_CMP_PER_INDEX table" ;
      ObSysVars[284].name_ = "innodb_cmp_per_index_enabled" ;
      ObSysVars[284].data_type_ = ObIntType ;
      ObSysVars[284].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[284].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[284].id_ = SYS_VAR_INNODB_CMP_PER_INDEX_ENABLED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_CMP_PER_INDEX_ENABLED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_CMP_PER_INDEX_ENABLED] = 284 ;
      ObSysVars[284].base_value_ = "0" ;
    ObSysVars[284].alias_ = "OB_SV_INNODB_CMP_PER_INDEX_ENABLED" ;
    }();

    [&] (){
      ObSysVars[285].default_value_ = "0" ;
      ObSysVars[285].info_ = "The number of threads that can commit at the same time. A value of 0 (the default) permits any number of transactions to commit simultaneously" ;
      ObSysVars[285].name_ = "innodb_commit_concurrency" ;
      ObSysVars[285].data_type_ = ObIntType ;
      ObSysVars[285].min_val_ = "0" ;
      ObSysVars[285].max_val_ = "100" ;
      ObSysVars[285].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[285].id_ = SYS_VAR_INNODB_COMMIT_CONCURRENCY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_COMMIT_CONCURRENCY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_COMMIT_CONCURRENCY] = 285 ;
      ObSysVars[285].base_value_ = "0" ;
    ObSysVars[285].alias_ = "OB_SV_INNODB_COMMIT_CONCURRENCY" ;
    }();

    [&] (){
      ObSysVars[286].default_value_ = "5" ;
      ObSysVars[286].info_ = "Defines the compression failure rate threshold for a table, as a percentage, at which point MySQL begins adding padding within compressed pages to avoid expensive compression failures" ;
      ObSysVars[286].name_ = "innodb_compression_failure_threshold_pct" ;
      ObSysVars[286].data_type_ = ObIntType ;
      ObSysVars[286].min_val_ = "0" ;
      ObSysVars[286].max_val_ = "100" ;
      ObSysVars[286].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[286].id_ = SYS_VAR_INNODB_COMPRESSION_FAILURE_THRESHOLD_PCT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_COMPRESSION_FAILURE_THRESHOLD_PCT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_COMPRESSION_FAILURE_THRESHOLD_PCT] = 286 ;
      ObSysVars[286].base_value_ = "5" ;
    ObSysVars[286].alias_ = "OB_SV_INNODB_COMPRESSION_FAILURE_THRESHOLD_PCT" ;
    }();

    [&] (){
      ObSysVars[287].default_value_ = "6" ;
      ObSysVars[287].info_ = "Specifies the level of zlib compression to use for InnoDB compressed tables and indexes" ;
      ObSysVars[287].name_ = "innodb_compression_level" ;
      ObSysVars[287].data_type_ = ObIntType ;
      ObSysVars[287].min_val_ = "0" ;
      ObSysVars[287].max_val_ = "9" ;
      ObSysVars[287].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[287].id_ = SYS_VAR_INNODB_COMPRESSION_LEVEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_COMPRESSION_LEVEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_COMPRESSION_LEVEL] = 287 ;
      ObSysVars[287].base_value_ = "6" ;
    ObSysVars[287].alias_ = "OB_SV_INNODB_COMPRESSION_LEVEL" ;
    }();

    [&] (){
      ObSysVars[288].default_value_ = "50" ;
      ObSysVars[288].info_ = "Specifies the maximum percentage that can be reserved as free space within each compressed page, allowing room to reorganize the data and modification log within the page when a compressed table or index is updated and the data might be recompressed" ;
      ObSysVars[288].name_ = "innodb_compression_pad_pct_max" ;
      ObSysVars[288].data_type_ = ObIntType ;
      ObSysVars[288].min_val_ = "0" ;
      ObSysVars[288].max_val_ = "75" ;
      ObSysVars[288].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[288].id_ = SYS_VAR_INNODB_COMPRESSION_PAD_PCT_MAX ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_COMPRESSION_PAD_PCT_MAX)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_COMPRESSION_PAD_PCT_MAX] = 288 ;
      ObSysVars[288].base_value_ = "50" ;
    ObSysVars[288].alias_ = "OB_SV_INNODB_COMPRESSION_PAD_PCT_MAX" ;
    }();

    [&] (){
      ObSysVars[289].default_value_ = "5000" ;
      ObSysVars[289].info_ = "Determines the number of threads that can enter InnoDB concurrently" ;
      ObSysVars[289].name_ = "innodb_concurrency_tickets" ;
      ObSysVars[289].data_type_ = ObIntType ;
      ObSysVars[289].min_val_ = "1" ;
      ObSysVars[289].max_val_ = "4294967295" ;
      ObSysVars[289].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[289].id_ = SYS_VAR_INNODB_CONCURRENCY_TICKETS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_CONCURRENCY_TICKETS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_CONCURRENCY_TICKETS] = 289 ;
      ObSysVars[289].base_value_ = "5000" ;
    ObSysVars[289].alias_ = "OB_SV_INNODB_CONCURRENCY_TICKETS" ;
    }();

    [&] (){
      ObSysVars[290].default_value_ = "2" ;
      ObSysVars[290].info_ = "The innodb_default_row_format option defines the default row format for InnoDB tables and user-created temporary tables" ;
      ObSysVars[290].name_ = "innodb_default_row_format" ;
      ObSysVars[290].data_type_ = ObIntType ;
      ObSysVars[290].enum_names_ = "[u'REDUNDANT', u'COMPACT', u'DYNAMIC']" ;
      ObSysVars[290].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[290].id_ = SYS_VAR_INNODB_DEFAULT_ROW_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_DEFAULT_ROW_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_DEFAULT_ROW_FORMAT] = 290 ;
      ObSysVars[290].base_value_ = "2" ;
    ObSysVars[290].alias_ = "OB_SV_INNODB_DEFAULT_ROW_FORMAT" ;
    }();

    [&] (){
      ObSysVars[291].default_value_ = "0" ;
      ObSysVars[291].info_ = "Disables the operating system file system cache for merge-sort temporary files. The effect is to open such files with the equivalent of O_DIRECT" ;
      ObSysVars[291].name_ = "innodb_disable_sort_file_cache" ;
      ObSysVars[291].data_type_ = ObIntType ;
      ObSysVars[291].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[291].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[291].id_ = SYS_VAR_INNODB_DISABLE_SORT_FILE_CACHE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_DISABLE_SORT_FILE_CACHE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_DISABLE_SORT_FILE_CACHE] = 291 ;
      ObSysVars[291].base_value_ = "0" ;
    ObSysVars[291].alias_ = "OB_SV_INNODB_DISABLE_SORT_FILE_CACHE" ;
    }();

    [&] (){
      ObSysVars[292].default_value_ = "1" ;
      ObSysVars[292].info_ = "Enables an InnoDB file format for file-per-table tablespaces" ;
      ObSysVars[292].name_ = "innodb_file_format" ;
      ObSysVars[292].data_type_ = ObIntType ;
      ObSysVars[292].enum_names_ = "[u'Antelope', u'Barracuda']" ;
      ObSysVars[292].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[292].id_ = SYS_VAR_INNODB_FILE_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FILE_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FILE_FORMAT] = 292 ;
      ObSysVars[292].base_value_ = "1" ;
    ObSysVars[292].alias_ = "OB_SV_INNODB_FILE_FORMAT" ;
    }();

    [&] (){
      ObSysVars[293].default_value_ = "1" ;
      ObSysVars[293].info_ = "At server startup, InnoDB sets the value of this variable to the file format tag in the system tablespace (for example, Antelope or Barracuda)" ;
      ObSysVars[293].name_ = "innodb_file_format_max" ;
      ObSysVars[293].data_type_ = ObIntType ;
      ObSysVars[293].enum_names_ = "[u'Antelope', u'Barracuda']" ;
      ObSysVars[293].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[293].id_ = SYS_VAR_INNODB_FILE_FORMAT_MAX ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FILE_FORMAT_MAX)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FILE_FORMAT_MAX] = 293 ;
      ObSysVars[293].base_value_ = "1" ;
    ObSysVars[293].alias_ = "OB_SV_INNODB_FILE_FORMAT_MAX" ;
    }();

    [&] (){
      ObSysVars[294].default_value_ = "1" ;
      ObSysVars[294].info_ = "When innodb_file_per_table is enabled, tables are created in file-per-table tablespaces by default" ;
      ObSysVars[294].name_ = "innodb_file_per_table" ;
      ObSysVars[294].data_type_ = ObIntType ;
      ObSysVars[294].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[294].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[294].id_ = SYS_VAR_INNODB_FILE_PER_TABLE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FILE_PER_TABLE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FILE_PER_TABLE] = 294 ;
      ObSysVars[294].base_value_ = "1" ;
    ObSysVars[294].alias_ = "OB_SV_INNODB_FILE_PER_TABLE" ;
    }();

    [&] (){
      ObSysVars[295].default_value_ = "100" ;
      ObSysVars[295].info_ = "InnoDB performs a bulk load when creating or rebuilding indexes" ;
      ObSysVars[295].name_ = "innodb_fill_factor" ;
      ObSysVars[295].data_type_ = ObIntType ;
      ObSysVars[295].min_val_ = "10" ;
      ObSysVars[295].max_val_ = "100" ;
      ObSysVars[295].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[295].id_ = SYS_VAR_INNODB_FILL_FACTOR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FILL_FACTOR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FILL_FACTOR] = 295 ;
      ObSysVars[295].base_value_ = "100" ;
    ObSysVars[295].alias_ = "OB_SV_INNODB_FILL_FACTOR" ;
    }();

    [&] (){
      ObSysVars[296].default_value_ = "1" ;
      ObSysVars[296].info_ = "Specifies whether flushing a page from the InnoDB buffer pool also flushes other dirty pages in the same extent" ;
      ObSysVars[296].name_ = "innodb_flush_neighbors" ;
      ObSysVars[296].data_type_ = ObIntType ;
      ObSysVars[296].enum_names_ = "[u'0', u'1', u'2']" ;
      ObSysVars[296].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[296].id_ = SYS_VAR_INNODB_FLUSH_NEIGHBORS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FLUSH_NEIGHBORS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FLUSH_NEIGHBORS] = 296 ;
      ObSysVars[296].base_value_ = "1" ;
    ObSysVars[296].alias_ = "OB_SV_INNODB_FLUSH_NEIGHBORS" ;
    }();

    [&] (){
      ObSysVars[297].default_value_ = "1" ;
      ObSysVars[297].info_ = "The innodb_flush_sync variable, which is enabled by default, causes the innodb_io_capacity setting to be ignored during bursts of I/O activity that occur at checkpoints" ;
      ObSysVars[297].name_ = "innodb_flush_sync" ;
      ObSysVars[297].data_type_ = ObIntType ;
      ObSysVars[297].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[297].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[297].id_ = SYS_VAR_INNODB_FLUSH_SYNC ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FLUSH_SYNC)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FLUSH_SYNC] = 297 ;
      ObSysVars[297].base_value_ = "1" ;
    ObSysVars[297].alias_ = "OB_SV_INNODB_FLUSH_SYNC" ;
    }();

    [&] (){
      ObSysVars[298].default_value_ = "30" ;
      ObSysVars[298].info_ = "Number of iterations for which InnoDB keeps the previously calculated snapshot of the flushing state, controlling how quickly adaptive flushing responds to changing workloads" ;
      ObSysVars[298].name_ = "innodb_flushing_avg_loops" ;
      ObSysVars[298].data_type_ = ObIntType ;
      ObSysVars[298].min_val_ = "1" ;
      ObSysVars[298].max_val_ = "1000" ;
      ObSysVars[298].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[298].id_ = SYS_VAR_INNODB_FLUSHING_AVG_LOOPS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FLUSHING_AVG_LOOPS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FLUSHING_AVG_LOOPS] = 298 ;
      ObSysVars[298].base_value_ = "30" ;
    ObSysVars[298].alias_ = "OB_SV_INNODB_FLUSHING_AVG_LOOPS" ;
    }();

    [&] (){
      ObSysVars[299].default_value_ = "1024" ;
      ObSysVars[299].info_ = "A parameter that influences the algorithms and heuristics for the flush operation for the InnoDB buffer pool. Primarily of interest to performance experts tuning I/O-intensive workloads" ;
      ObSysVars[299].name_ = "innodb_lru_scan_depth" ;
      ObSysVars[299].data_type_ = ObUInt64Type ;
      ObSysVars[299].min_val_ = "100" ;
      ObSysVars[299].max_val_ = "18446744073709551615" ;
      ObSysVars[299].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[299].id_ = SYS_VAR_INNODB_LRU_SCAN_DEPTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LRU_SCAN_DEPTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LRU_SCAN_DEPTH] = 299 ;
      ObSysVars[299].base_value_ = "1024" ;
    ObSysVars[299].alias_ = "OB_SV_INNODB_LRU_SCAN_DEPTH" ;
    }();

    [&] (){
      ObSysVars[300].default_value_ = "75" ;
      ObSysVars[300].info_ = "InnoDB tries to flush data from the buffer pool so that the percentage of dirty pages does not exceed this value" ;
      ObSysVars[300].name_ = "innodb_max_dirty_pages_pct" ;
      ObSysVars[300].data_type_ = ObNumberType ;
      ObSysVars[300].min_val_ = "0" ;
      ObSysVars[300].max_val_ = "99.999" ;
      ObSysVars[300].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[300].id_ = SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT] = 300 ;
      ObSysVars[300].base_value_ = "75" ;
    ObSysVars[300].alias_ = "OB_SV_INNODB_MAX_DIRTY_PAGES_PCT" ;
    }();

    [&] (){
      ObSysVars[301].default_value_ = "0" ;
      ObSysVars[301].info_ = "Defines a low water mark representing the percentage of dirty pages at which preflushing is enabled to control the dirty page ratio" ;
      ObSysVars[301].name_ = "innodb_max_dirty_pages_pct_lwm" ;
      ObSysVars[301].data_type_ = ObNumberType ;
      ObSysVars[301].min_val_ = "0" ;
      ObSysVars[301].max_val_ = "99.999" ;
      ObSysVars[301].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[301].id_ = SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT_LWM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT_LWM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT_LWM] = 301 ;
      ObSysVars[301].base_value_ = "0" ;
    ObSysVars[301].alias_ = "OB_SV_INNODB_MAX_DIRTY_PAGES_PCT_LWM" ;
    }();

    [&] (){
      ObSysVars[302].default_value_ = "0" ;
      ObSysVars[302].info_ = "Defines the desired maximum purge lag. If this value is exceeded, a delay is imposed on INSERT, UPDATE, and DELETE operations to allow time for purge to catch up" ;
      ObSysVars[302].name_ = "innodb_max_purge_lag" ;
      ObSysVars[302].data_type_ = ObIntType ;
      ObSysVars[302].min_val_ = "0" ;
      ObSysVars[302].max_val_ = "4294967295" ;
      ObSysVars[302].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[302].id_ = SYS_VAR_INNODB_MAX_PURGE_LAG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_MAX_PURGE_LAG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_MAX_PURGE_LAG] = 302 ;
      ObSysVars[302].base_value_ = "0" ;
    ObSysVars[302].alias_ = "OB_SV_INNODB_MAX_PURGE_LAG" ;
    }();

    [&] (){
      ObSysVars[303].default_value_ = "0" ;
      ObSysVars[303].info_ = "Specifies the maximum delay in microseconds for the delay imposed when the innodb_max_purge_lag threshold is exceeded" ;
      ObSysVars[303].name_ = "innodb_max_purge_lag_delay" ;
      ObSysVars[303].data_type_ = ObIntType ;
      ObSysVars[303].min_val_ = "0" ;
      ObSysVars[303].max_val_ = "10000000" ;
      ObSysVars[303].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[303].id_ = SYS_VAR_INNODB_MAX_PURGE_LAG_DELAY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_MAX_PURGE_LAG_DELAY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_MAX_PURGE_LAG_DELAY] = 303 ;
      ObSysVars[303].base_value_ = "0" ;
    ObSysVars[303].alias_ = "OB_SV_INNODB_MAX_PURGE_LAG_DELAY" ;
    }();

    [&] (){
      ObSysVars[304].default_value_ = "1" ;
      ObSysVars[304].info_ = "This is required on Unix for support of the DATA DIRECTORY and INDEX DIRECTORY table options" ;
      ObSysVars[304].name_ = "have_symlink" ;
      ObSysVars[304].data_type_ = ObIntType ;
      ObSysVars[304].enum_names_ = "[u'NO', u'YES']" ;
      ObSysVars[304].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[304].id_ = SYS_VAR_HAVE_SYMLINK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_SYMLINK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_SYMLINK] = 304 ;
      ObSysVars[304].base_value_ = "1" ;
    ObSysVars[304].alias_ = "OB_SV_HAVE_SYMLINK" ;
    }();

    [&] (){
      ObSysVars[305].default_value_ = "0" ;
      ObSysVars[305].info_ = "In earlier versions of MySQL, enabling this variable caused the server to behave as if the built-in InnoDB were not present, which enabled the InnoDB Plugin to be used instead" ;
      ObSysVars[305].name_ = "ignore_builtin_innodb" ;
      ObSysVars[305].data_type_ = ObIntType ;
      ObSysVars[305].enum_names_ = "[u'NO', u'YES']" ;
      ObSysVars[305].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[305].id_ = SYS_VAR_IGNORE_BUILTIN_INNODB ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_IGNORE_BUILTIN_INNODB)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_IGNORE_BUILTIN_INNODB] = 305 ;
      ObSysVars[305].base_value_ = "0" ;
    ObSysVars[305].alias_ = "OB_SV_IGNORE_BUILTIN_INNODB" ;
    }();

    [&] (){
      ObSysVars[306].default_value_ = "134217728" ;
      ObSysVars[306].info_ = "defines the chunk size for InnoDB buffer pool resizing operations" ;
      ObSysVars[306].name_ = "innodb_buffer_pool_chunk_size" ;
      ObSysVars[306].data_type_ = ObIntType ;
      ObSysVars[306].min_val_ = "1048576" ;
      ObSysVars[306].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[306].id_ = SYS_VAR_INNODB_BUFFER_POOL_CHUNK_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_CHUNK_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_CHUNK_SIZE] = 306 ;
      ObSysVars[306].base_value_ = "134217728" ;
    ObSysVars[306].alias_ = "OB_SV_INNODB_BUFFER_POOL_CHUNK_SIZE" ;
    }();

    [&] (){
      ObSysVars[307].default_value_ = "8" ;
      ObSysVars[307].info_ = "defines the chunk size for InnoDB buffer pool resizing operations" ;
      ObSysVars[307].name_ = "innodb_buffer_pool_instances" ;
      ObSysVars[307].data_type_ = ObIntType ;
      ObSysVars[307].min_val_ = "1" ;
      ObSysVars[307].max_val_ = "64" ;
      ObSysVars[307].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[307].id_ = SYS_VAR_INNODB_BUFFER_POOL_INSTANCES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_INSTANCES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_INSTANCES] = 307 ;
      ObSysVars[307].base_value_ = "8" ;
    ObSysVars[307].alias_ = "OB_SV_INNODB_BUFFER_POOL_INSTANCES" ;
    }();

    [&] (){
      ObSysVars[308].default_value_ = "1" ;
      ObSysVars[308].info_ = "Specifies that, on MySQL server startup, the InnoDB buffer pool is automatically warmed up by loading the same pages it held at an earlier time" ;
      ObSysVars[308].name_ = "innodb_buffer_pool_load_at_startup" ;
      ObSysVars[308].data_type_ = ObIntType ;
      ObSysVars[308].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[308].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[308].id_ = SYS_VAR_INNODB_BUFFER_POOL_LOAD_AT_STARTUP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_BUFFER_POOL_LOAD_AT_STARTUP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_BUFFER_POOL_LOAD_AT_STARTUP] = 308 ;
      ObSysVars[308].base_value_ = "1" ;
    ObSysVars[308].alias_ = "OB_SV_INNODB_BUFFER_POOL_LOAD_AT_STARTUP" ;
    }();

    [&] (){
      ObSysVars[309].default_value_ = "1" ;
      ObSysVars[309].info_ = "InnoDB can use checksum validation on all tablespace pages read from disk to ensure extra fault tolerance against hardware faults or corrupted data files" ;
      ObSysVars[309].name_ = "innodb_checksums" ;
      ObSysVars[309].data_type_ = ObIntType ;
      ObSysVars[309].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[309].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[309].id_ = SYS_VAR_INNODB_CHECKSUMS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_CHECKSUMS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_CHECKSUMS] = 309 ;
      ObSysVars[309].base_value_ = "1" ;
    ObSysVars[309].alias_ = "OB_SV_INNODB_CHECKSUMS" ;
    }();

    [&] (){
      ObSysVars[310].default_value_ = "1" ;
      ObSysVars[310].info_ = "When enabled (the default), InnoDB stores all data twice, first to the doublewrite buffer, then to the actual data files" ;
      ObSysVars[310].name_ = "innodb_doublewrite" ;
      ObSysVars[310].data_type_ = ObIntType ;
      ObSysVars[310].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[310].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[310].id_ = SYS_VAR_INNODB_DOUBLEWRITE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_DOUBLEWRITE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_DOUBLEWRITE] = 310 ;
      ObSysVars[310].base_value_ = "1" ;
    ObSysVars[310].alias_ = "OB_SV_INNODB_DOUBLEWRITE" ;
    }();

    [&] (){
      ObSysVars[311].default_value_ = "1" ;
      ObSysVars[311].info_ = "This variable can be set to 1 or 0 at server startup to enable or disable whether InnoDB checks the file format tag in the system tablespace (for example, Antelope or Barracuda)" ;
      ObSysVars[311].name_ = "innodb_file_format_check" ;
      ObSysVars[311].data_type_ = ObIntType ;
      ObSysVars[311].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[311].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[311].id_ = SYS_VAR_INNODB_FILE_FORMAT_CHECK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FILE_FORMAT_CHECK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FILE_FORMAT_CHECK] = 311 ;
      ObSysVars[311].base_value_ = "1" ;
    ObSysVars[311].alias_ = "OB_SV_INNODB_FILE_FORMAT_CHECK" ;
    }();

    [&] (){
      ObSysVars[312].default_value_ = "0" ;
      ObSysVars[312].info_ = "Defines the method used to flush data to InnoDB data files and log files, which can affect I/O throughput" ;
      ObSysVars[312].name_ = "innodb_flush_method" ;
      ObSysVars[312].data_type_ = ObIntType ;
      ObSysVars[312].enum_names_ = "[u'null', u'fsync', u'O_DSYNC', u'littlesync', u'nosync', u'O_DIRECT', u'O_DIRECT_NO_FSYNC']" ;
      ObSysVars[312].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[312].id_ = SYS_VAR_INNODB_FLUSH_METHOD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FLUSH_METHOD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FLUSH_METHOD] = 312 ;
      ObSysVars[312].base_value_ = "0" ;
    ObSysVars[312].alias_ = "OB_SV_INNODB_FLUSH_METHOD" ;
    }();

    [&] (){
      ObSysVars[313].default_value_ = "0" ;
      ObSysVars[313].info_ = "Permits InnoDB to load tables at startup that are marked as corrupted" ;
      ObSysVars[313].name_ = "innodb_force_load_corrupted" ;
      ObSysVars[313].data_type_ = ObIntType ;
      ObSysVars[313].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[313].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[313].id_ = SYS_VAR_INNODB_FORCE_LOAD_CORRUPTED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FORCE_LOAD_CORRUPTED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FORCE_LOAD_CORRUPTED] = 313 ;
      ObSysVars[313].base_value_ = "0" ;
    ObSysVars[313].alias_ = "OB_SV_INNODB_FORCE_LOAD_CORRUPTED" ;
    }();

    [&] (){
      ObSysVars[314].default_value_ = "2" ;
      ObSysVars[314].info_ = "Specifies the page size for InnoDB tablespaces. Values can be specified in bytes or kilobytes" ;
      ObSysVars[314].name_ = "innodb_page_size" ;
      ObSysVars[314].data_type_ = ObIntType ;
      ObSysVars[314].enum_names_ = "[u'4096', u'8192', u'16384', u'32768', u'65536']" ;
      ObSysVars[314].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[314].id_ = SYS_VAR_INNODB_PAGE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_PAGE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_PAGE_SIZE] = 314 ;
      ObSysVars[314].base_value_ = "2" ;
    ObSysVars[314].alias_ = "OB_SV_INNODB_PAGE_SIZE" ;
    }();

    [&] (){
      ObSysVars[315].default_value_ = "0" ;
      ObSysVars[315].info_ = "The InnoDB version number" ;
      ObSysVars[315].name_ = "innodb_version" ;
      ObSysVars[315].data_type_ = ObIntType ;
      ObSysVars[315].enum_names_ = "[u'5.7.38']" ;
      ObSysVars[315].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[315].id_ = SYS_VAR_INNODB_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_VERSION] = 315 ;
      ObSysVars[315].base_value_ = "0" ;
    ObSysVars[315].alias_ = "OB_SV_INNODB_VERSION" ;
    }();

    [&] (){
      ObSysVars[316].default_value_ = "18446744073709551615" ;
      ObSysVars[316].info_ = "The maximum amount of memory to use for memory mapping compressed MyISAM files" ;
      ObSysVars[316].name_ = "myisam_mmap_size" ;
      ObSysVars[316].data_type_ = ObUInt64Type ;
      ObSysVars[316].min_val_ = "7" ;
      ObSysVars[316].max_val_ = "18446744073709551615" ;
      ObSysVars[316].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[316].id_ = SYS_VAR_MYISAM_MMAP_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYISAM_MMAP_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYISAM_MMAP_SIZE] = 316 ;
      ObSysVars[316].base_value_ = "18446744073709551615" ;
    ObSysVars[316].alias_ = "OB_SV_MYISAM_MMAP_SIZE" ;
    }();

    [&] (){
      ObSysVars[317].default_value_ = "16" ;
      ObSysVars[317].info_ = "The maximum amount of memory to use for memory mapping compressed MyISAM files" ;
      ObSysVars[317].name_ = "table_open_cache_instances" ;
      ObSysVars[317].data_type_ = ObIntType ;
      ObSysVars[317].min_val_ = "1" ;
      ObSysVars[317].max_val_ = "64" ;
      ObSysVars[317].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[317].id_ = SYS_VAR_TABLE_OPEN_CACHE_INSTANCES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TABLE_OPEN_CACHE_INSTANCES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TABLE_OPEN_CACHE_INSTANCES] = 317 ;
      ObSysVars[317].base_value_ = "16" ;
    ObSysVars[317].alias_ = "OB_SV_TABLE_OPEN_CACHE_INSTANCES" ;
    }();

    [&] (){
      ObSysVars[318].default_value_ = "" ;
      ObSysVars[318].info_ = "When used with global scope, this variable contains a representation of the set of all transactions executed on the server and GTIDs that have been set by a SET gtid_purged statement" ;
      ObSysVars[318].name_ = "gtid_executed" ;
      ObSysVars[318].data_type_ = ObVarcharType ;
      ObSysVars[318].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[318].id_ = SYS_VAR_GTID_EXECUTED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GTID_EXECUTED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GTID_EXECUTED] = 318 ;
      ObSysVars[318].base_value_ = "" ;
    ObSysVars[318].alias_ = "OB_SV_GTID_EXECUTED" ;
    }();

    [&] (){
      ObSysVars[319].default_value_ = "" ;
      ObSysVars[319].info_ = "When used with global scope, gtid_owned holds a list of all the GTIDs that are currently in use on the server, with the IDs of the threads that own them. When used with session scope, gtid_owned holds a single GTID that is currently in use by and owned by this session" ;
      ObSysVars[319].name_ = "gtid_owned" ;
      ObSysVars[319].data_type_ = ObVarcharType ;
      ObSysVars[319].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[319].id_ = SYS_VAR_GTID_OWNED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GTID_OWNED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GTID_OWNED] = 319 ;
      ObSysVars[319].base_value_ = "" ;
    ObSysVars[319].alias_ = "OB_SV_GTID_OWNED" ;
    }();

    [&] (){
      ObSysVars[320].default_value_ = "0" ;
      ObSysVars[320].info_ = "InnoDB rolls back only the last statement on a transaction timeout by default. If --innodb-rollback-on-timeout is specified, a transaction timeout causes InnoDB to abort and roll back the entire transaction" ;
      ObSysVars[320].name_ = "innodb_rollback_on_timeout" ;
      ObSysVars[320].data_type_ = ObIntType ;
      ObSysVars[320].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[320].id_ = SYS_VAR_INNODB_ROLLBACK_ON_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_ROLLBACK_ON_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_ROLLBACK_ON_TIMEOUT] = 320 ;
      ObSysVars[320].base_value_ = "0" ;
    ObSysVars[320].alias_ = "OB_SV_INNODB_ROLLBACK_ON_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[321].default_value_ = "0" ;
      ObSysVars[321].info_ = "completion_type affects transactions that begin with START TRANSACTION or BEGIN and end with COMMIT or ROLLBACK. It does not apply for XA COMMIT, XA ROLLBACK, or when autocommit=1" ;
      ObSysVars[321].name_ = "completion_type" ;
      ObSysVars[321].data_type_ = ObIntType ;
      ObSysVars[321].enum_names_ = "[u'NO_CHAIN', u'CHAIN', u'RELEASE']" ;
      ObSysVars[321].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[321].id_ = SYS_VAR_COMPLETION_TYPE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_COMPLETION_TYPE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_COMPLETION_TYPE] = 321 ;
      ObSysVars[321].base_value_ = "0" ;
    ObSysVars[321].alias_ = "OB_SV_COMPLETION_TYPE" ;
    }();

    [&] (){
      ObSysVars[322].default_value_ = "0" ;
      ObSysVars[322].info_ = "Depending on the value of this variable, the server enforces GTID consistency by allowing execution of only statements that can be safely logged using a GTID. You must set this variable to ON before enabling GTID based replication" ;
      ObSysVars[322].name_ = "enforce_gtid_consistency" ;
      ObSysVars[322].data_type_ = ObIntType ;
      ObSysVars[322].enum_names_ = "[u'OFF', u'ON', u'WARN']" ;
      ObSysVars[322].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[322].id_ = SYS_VAR_ENFORCE_GTID_CONSISTENCY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_ENFORCE_GTID_CONSISTENCY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_ENFORCE_GTID_CONSISTENCY] = 322 ;
      ObSysVars[322].base_value_ = "0" ;
    ObSysVars[322].alias_ = "OB_SV_ENFORCE_GTID_CONSISTENCY" ;
    }();

    [&] (){
      ObSysVars[323].default_value_ = "1000" ;
      ObSysVars[323].info_ = "Compress the mysql.gtid_executed table each time this many transactions have been processed" ;
      ObSysVars[323].name_ = "gtid_executed_compression_period" ;
      ObSysVars[323].data_type_ = ObIntType ;
      ObSysVars[323].min_val_ = "0" ;
      ObSysVars[323].max_val_ = "4294967295" ;
      ObSysVars[323].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[323].id_ = SYS_VAR_GTID_EXECUTED_COMPRESSION_PERIOD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GTID_EXECUTED_COMPRESSION_PERIOD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GTID_EXECUTED_COMPRESSION_PERIOD] = 323 ;
      ObSysVars[323].base_value_ = "1000" ;
    ObSysVars[323].alias_ = "OB_SV_GTID_EXECUTED_COMPRESSION_PERIOD" ;
    }();

    [&] (){
      ObSysVars[324].default_value_ = "0" ;
      ObSysVars[324].info_ = "Controls whether GTID based logging is enabled and what type of transactions the logs can contain" ;
      ObSysVars[324].name_ = "gtid_mode" ;
      ObSysVars[324].data_type_ = ObIntType ;
      ObSysVars[324].enum_names_ = "[u'OFF', u'OFF_PERMISSIVE', u'ON_PERMISSIVE', u'ON']" ;
      ObSysVars[324].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[324].id_ = SYS_VAR_GTID_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GTID_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GTID_MODE] = 324 ;
      ObSysVars[324].base_value_ = "0" ;
    ObSysVars[324].alias_ = "OB_SV_GTID_MODE" ;
    }();

    [&] (){
      ObSysVars[325].default_value_ = "0" ;
      ObSysVars[325].info_ = "This variable is used to specify whether and how the next GTID is obtained" ;
      ObSysVars[325].name_ = "gtid_next" ;
      ObSysVars[325].data_type_ = ObIntType ;
      ObSysVars[325].enum_names_ = "[u'AUTOMATIC', u'ANONYMOUS']" ;
      ObSysVars[325].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[325].id_ = SYS_VAR_GTID_NEXT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GTID_NEXT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GTID_NEXT] = 325 ;
      ObSysVars[325].base_value_ = "0" ;
    ObSysVars[325].alias_ = "OB_SV_GTID_NEXT" ;
    }();

    [&] (){
      ObSysVars[326].default_value_ = "" ;
      ObSysVars[326].info_ = "The global value of the gtid_purged system variable is a GTID set consisting of the GTIDs of all the transactions that have been committed on the server, but do not exist in any binary log file on the server. gtid_purged is a subset of gtid_executed" ;
      ObSysVars[326].name_ = "gtid_purged" ;
      ObSysVars[326].data_type_ = ObVarcharType ;
      ObSysVars[326].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[326].id_ = SYS_VAR_GTID_PURGED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GTID_PURGED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GTID_PURGED] = 326 ;
      ObSysVars[326].base_value_ = "" ;
    ObSysVars[326].alias_ = "OB_SV_GTID_PURGED" ;
    }();

    [&] (){
      ObSysVars[327].default_value_ = "5" ;
      ObSysVars[327].info_ = "How often to auto-commit idle connections that use the InnoDB memcached interface, in seconds" ;
      ObSysVars[327].name_ = "innodb_api_bk_commit_interval" ;
      ObSysVars[327].data_type_ = ObIntType ;
      ObSysVars[327].min_val_ = "1" ;
      ObSysVars[327].max_val_ = "1073741824" ;
      ObSysVars[327].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[327].id_ = SYS_VAR_INNODB_API_BK_COMMIT_INTERVAL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_API_BK_COMMIT_INTERVAL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_API_BK_COMMIT_INTERVAL] = 327 ;
      ObSysVars[327].base_value_ = "5" ;
    ObSysVars[327].alias_ = "OB_SV_INNODB_API_BK_COMMIT_INTERVAL" ;
    }();

    [&] (){
      ObSysVars[328].default_value_ = "0" ;
      ObSysVars[328].info_ = "Controls the transaction isolation level on queries processed by the memcached interface" ;
      ObSysVars[328].name_ = "innodb_api_trx_level" ;
      ObSysVars[328].data_type_ = ObIntType ;
      ObSysVars[328].min_val_ = "0" ;
      ObSysVars[328].max_val_ = "3" ;
      ObSysVars[328].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[328].id_ = SYS_VAR_INNODB_API_TRX_LEVEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_API_TRX_LEVEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_API_TRX_LEVEL] = 328 ;
      ObSysVars[328].base_value_ = "0" ;
    ObSysVars[328].alias_ = "OB_SV_INNODB_API_TRX_LEVEL" ;
    }();

    [&] (){
      ObSysVars[329].default_value_ = "1" ;
      ObSysVars[329].info_ = "Enables InnoDB support for two-phase commit in XA transactions, causing an extra disk flush for transaction preparation" ;
      ObSysVars[329].name_ = "innodb_support_xa" ;
      ObSysVars[329].data_type_ = ObIntType ;
      ObSysVars[329].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[329].id_ = SYS_VAR_INNODB_SUPPORT_XA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_SUPPORT_XA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_SUPPORT_XA] = 329 ;
      ObSysVars[329].base_value_ = "1" ;
    ObSysVars[329].alias_ = "OB_SV_INNODB_SUPPORT_XA" ;
    }();

    [&] (){
      ObSysVars[330].default_value_ = "0" ;
      ObSysVars[330].info_ = "Controls whether the server returns GTIDs to the client, enabling the client to use them to track the server state" ;
      ObSysVars[330].name_ = "session_track_gtids" ;
      ObSysVars[330].data_type_ = ObIntType ;
      ObSysVars[330].enum_names_ = "[u'OFF', u'OWN_GTID', u'ALL_GTIDS']" ;
      ObSysVars[330].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[330].id_ = SYS_VAR_SESSION_TRACK_GTIDS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SESSION_TRACK_GTIDS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SESSION_TRACK_GTIDS] = 330 ;
      ObSysVars[330].base_value_ = "0" ;
    ObSysVars[330].alias_ = "OB_SV_SESSION_TRACK_GTIDS" ;
    }();

    [&] (){
      ObSysVars[331].default_value_ = "0" ;
      ObSysVars[331].info_ = "Controls whether the server tracks the state and characteristics of transactions within the current session and notifies the client to make this information available" ;
      ObSysVars[331].name_ = "session_track_transaction_info" ;
      ObSysVars[331].data_type_ = ObIntType ;
      ObSysVars[331].enum_names_ = "[u'OFF', u'STATE', u'CHARACTERISTICS']" ;
      ObSysVars[331].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[331].id_ = SYS_VAR_SESSION_TRACK_TRANSACTION_INFO ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SESSION_TRACK_TRANSACTION_INFO)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SESSION_TRACK_TRANSACTION_INFO] = 331 ;
      ObSysVars[331].base_value_ = "0" ;
    ObSysVars[331].alias_ = "OB_SV_SESSION_TRACK_TRANSACTION_INFO" ;
    }();

    [&] (){
      ObSysVars[332].default_value_ = "8192" ;
      ObSysVars[332].info_ = "The amount in bytes by which to increase a per-transaction memory pool which needs memory" ;
      ObSysVars[332].name_ = "transaction_alloc_block_size" ;
      ObSysVars[332].data_type_ = ObIntType ;
      ObSysVars[332].min_val_ = "1024" ;
      ObSysVars[332].max_val_ = "131072" ;
      ObSysVars[332].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[332].id_ = SYS_VAR_TRANSACTION_ALLOC_BLOCK_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_ALLOC_BLOCK_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_ALLOC_BLOCK_SIZE] = 332 ;
      ObSysVars[332].base_value_ = "8192" ;
    ObSysVars[332].alias_ = "OB_SV_TRANSACTION_ALLOC_BLOCK_SIZE" ;
    }();

    [&] (){
      ObSysVars[333].default_value_ = "0" ;
      ObSysVars[333].info_ = "When set to 1 or ON, this variable enables batching of statements within the same transaction. To use this variable, autocommit must first be disabled by setting it to 0 or OFF; otherwise, setting transaction_allow_batching has no effect" ;
      ObSysVars[333].name_ = "transaction_allow_batching" ;
      ObSysVars[333].data_type_ = ObIntType ;
      ObSysVars[333].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[333].id_ = SYS_VAR_TRANSACTION_ALLOW_BATCHING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_ALLOW_BATCHING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_ALLOW_BATCHING] = 333 ;
      ObSysVars[333].base_value_ = "0" ;
    ObSysVars[333].alias_ = "OB_SV_TRANSACTION_ALLOW_BATCHING" ;
    }();

    [&] (){
      ObSysVars[334].default_value_ = "4096" ;
      ObSysVars[334].info_ = "There is a per-transaction memory pool from which various transaction-related allocations take memory. The initial size of the pool in bytes is transaction_prealloc_size" ;
      ObSysVars[334].name_ = "transaction_prealloc_size" ;
      ObSysVars[334].data_type_ = ObIntType ;
      ObSysVars[334].min_val_ = "1024" ;
      ObSysVars[334].max_val_ = "131072" ;
      ObSysVars[334].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[334].id_ = SYS_VAR_TRANSACTION_PREALLOC_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_PREALLOC_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_PREALLOC_SIZE] = 334 ;
      ObSysVars[334].base_value_ = "4096" ;
    ObSysVars[334].alias_ = "OB_SV_TRANSACTION_PREALLOC_SIZE" ;
    }();

    [&] (){
      ObSysVars[335].default_value_ = "0" ;
      ObSysVars[335].info_ = "Defines the algorithm used to generate a hash identifying the writes associated with a transaction" ;
      ObSysVars[335].name_ = "transaction_write_set_extraction" ;
      ObSysVars[335].data_type_ = ObIntType ;
      ObSysVars[335].enum_names_ = "[u'OFF', u'MURMUR32', u'XXHASH64']" ;
      ObSysVars[335].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[335].id_ = SYS_VAR_TRANSACTION_WRITE_SET_EXTRACTION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_WRITE_SET_EXTRACTION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_WRITE_SET_EXTRACTION] = 335 ;
      ObSysVars[335].base_value_ = "0" ;
    ObSysVars[335].alias_ = "OB_SV_TRANSACTION_WRITE_SET_EXTRACTION" ;
    }();

    [&] (){
      ObSysVars[336].default_value_ = "86400" ;
      ObSysVars[336].info_ = "The number of seconds after which the server will fetch data from storage engine and replace the data in cache." ;
      ObSysVars[336].name_ = "information_schema_stats_expiry" ;
      ObSysVars[336].data_type_ = ObUInt64Type ;
      ObSysVars[336].min_val_ = "0" ;
      ObSysVars[336].max_val_ = "31536000" ;
      ObSysVars[336].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[336].id_ = SYS_VAR_INFORMATION_SCHEMA_STATS_EXPIRY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INFORMATION_SCHEMA_STATS_EXPIRY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INFORMATION_SCHEMA_STATS_EXPIRY] = 336 ;
      ObSysVars[336].base_value_ = "86400" ;
    ObSysVars[336].alias_ = "OB_SV_INFORMATION_SCHEMA_STATS_EXPIRY" ;
    }();

    [&] (){
      ObSysVars[337].default_value_ = "9223372036854775807" ;
      ObSysVars[337].info_ = "used by JDBC setMaxRows() interface to specify limitation of row number in ResultSet" ;
      ObSysVars[337].name_ = "_oracle_sql_select_limit" ;
      ObSysVars[337].data_type_ = ObIntType ;
      ObSysVars[337].min_val_ = "0" ;
      ObSysVars[337].max_val_ = "9223372036854775807" ;
      ObSysVars[337].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::ORACLE_ONLY ;
      ObSysVars[337].id_ = SYS_VAR__ORACLE_SQL_SELECT_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ORACLE_SQL_SELECT_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ORACLE_SQL_SELECT_LIMIT] = 337 ;
      ObSysVars[337].base_value_ = "9223372036854775807" ;
    ObSysVars[337].alias_ = "OB_SV__ORACLE_SQL_SELECT_LIMIT" ;
    }();

    [&] (){
      ObSysVars[338].default_value_ = "0" ;
      ObSysVars[338].info_ = "Allows the server to join the group even if it has local transactions that are not present in the group" ;
      ObSysVars[338].name_ = "group_replication_allow_local_disjoint_gtids_join" ;
      ObSysVars[338].data_type_ = ObIntType ;
      ObSysVars[338].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[338].id_ = SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_DISJOINT_GTIDS_JOIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_DISJOINT_GTIDS_JOIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_DISJOINT_GTIDS_JOIN] = 338 ;
      ObSysVars[338].base_value_ = "0" ;
    ObSysVars[338].alias_ = "OB_SV_GROUP_REPLICATION_ALLOW_LOCAL_DISJOINT_GTIDS_JOIN" ;
    }();

    [&] (){
      ObSysVars[339].default_value_ = "0" ;
      ObSysVars[339].info_ = "Allows the current server to join the group even if it has a lower major version than the group" ;
      ObSysVars[339].name_ = "group_replication_allow_local_lower_version_join" ;
      ObSysVars[339].data_type_ = ObIntType ;
      ObSysVars[339].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[339].id_ = SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_LOWER_VERSION_JOIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_LOWER_VERSION_JOIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_LOWER_VERSION_JOIN] = 339 ;
      ObSysVars[339].base_value_ = "0" ;
    ObSysVars[339].alias_ = "OB_SV_GROUP_REPLICATION_ALLOW_LOCAL_LOWER_VERSION_JOIN" ;
    }();

    [&] (){
      ObSysVars[340].default_value_ = "7" ;
      ObSysVars[340].info_ = "Determines the interval between successive column values for transactions that execute on this server instance" ;
      ObSysVars[340].name_ = "group_replication_auto_increment_increment" ;
      ObSysVars[340].data_type_ = ObIntType ;
      ObSysVars[340].min_val_ = "1" ;
      ObSysVars[340].max_val_ = "65535" ;
      ObSysVars[340].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[340].id_ = SYS_VAR_GROUP_REPLICATION_AUTO_INCREMENT_INCREMENT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_AUTO_INCREMENT_INCREMENT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_AUTO_INCREMENT_INCREMENT] = 340 ;
      ObSysVars[340].base_value_ = "7" ;
    ObSysVars[340].alias_ = "OB_SV_GROUP_REPLICATION_AUTO_INCREMENT_INCREMENT" ;
    }();

    [&] (){
      ObSysVars[341].default_value_ = "0" ;
      ObSysVars[341].info_ = "Configure this server to bootstrap the group" ;
      ObSysVars[341].name_ = "group_replication_bootstrap_group" ;
      ObSysVars[341].data_type_ = ObIntType ;
      ObSysVars[341].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[341].id_ = SYS_VAR_GROUP_REPLICATION_BOOTSTRAP_GROUP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_BOOTSTRAP_GROUP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_BOOTSTRAP_GROUP] = 341 ;
      ObSysVars[341].base_value_ = "0" ;
    ObSysVars[341].alias_ = "OB_SV_GROUP_REPLICATION_BOOTSTRAP_GROUP" ;
    }();

    [&] (){
      ObSysVars[342].default_value_ = "31536000" ;
      ObSysVars[342].info_ = "Timeout, in seconds, that Group Replication waits for each of the components when shutting down" ;
      ObSysVars[342].name_ = "group_replication_components_stop_timeout" ;
      ObSysVars[342].data_type_ = ObIntType ;
      ObSysVars[342].min_val_ = "2" ;
      ObSysVars[342].max_val_ = "31536000" ;
      ObSysVars[342].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[342].id_ = SYS_VAR_GROUP_REPLICATION_COMPONENTS_STOP_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_COMPONENTS_STOP_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_COMPONENTS_STOP_TIMEOUT] = 342 ;
      ObSysVars[342].base_value_ = "31536000" ;
    ObSysVars[342].alias_ = "OB_SV_GROUP_REPLICATION_COMPONENTS_STOP_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[343].default_value_ = "1000000" ;
      ObSysVars[343].info_ = "The threshold value in bytes above which compression is applied to messages sent between group members" ;
      ObSysVars[343].name_ = "group_replication_compression_threshold" ;
      ObSysVars[343].data_type_ = ObIntType ;
      ObSysVars[343].min_val_ = "0" ;
      ObSysVars[343].max_val_ = "4294967295" ;
      ObSysVars[343].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[343].id_ = SYS_VAR_GROUP_REPLICATION_COMPRESSION_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_COMPRESSION_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_COMPRESSION_THRESHOLD] = 343 ;
      ObSysVars[343].base_value_ = "1000000" ;
    ObSysVars[343].alias_ = "OB_SV_GROUP_REPLICATION_COMPRESSION_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[344].default_value_ = "0" ;
      ObSysVars[344].info_ = "Enable or disable strict consistency checks for multi-primary update everywhere" ;
      ObSysVars[344].name_ = "group_replication_enforce_update_everywhere_checks" ;
      ObSysVars[344].data_type_ = ObIntType ;
      ObSysVars[344].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[344].id_ = SYS_VAR_GROUP_REPLICATION_ENFORCE_UPDATE_EVERYWHERE_CHECKS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_ENFORCE_UPDATE_EVERYWHERE_CHECKS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_ENFORCE_UPDATE_EVERYWHERE_CHECKS] = 344 ;
      ObSysVars[344].base_value_ = "0" ;
    ObSysVars[344].alias_ = "OB_SV_GROUP_REPLICATION_ENFORCE_UPDATE_EVERYWHERE_CHECKS" ;
    }();

    [&] (){
      ObSysVars[345].default_value_ = "1" ;
      ObSysVars[345].info_ = "Configures how Group Replication behaves when a server instance leaves the group unintentionally, for example after encountering an applier error, or in the case of a loss of majority, or when another member of the group expels it due to a suspicion timing out" ;
      ObSysVars[345].name_ = "group_replication_exit_state_action" ;
      ObSysVars[345].data_type_ = ObIntType ;
      ObSysVars[345].enum_names_ = "[u'ABORT_SERVER', u'READ_ONLY']" ;
      ObSysVars[345].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[345].id_ = SYS_VAR_GROUP_REPLICATION_EXIT_STATE_ACTION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_EXIT_STATE_ACTION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_EXIT_STATE_ACTION] = 345 ;
      ObSysVars[345].base_value_ = "1" ;
    ObSysVars[345].alias_ = "OB_SV_GROUP_REPLICATION_EXIT_STATE_ACTION" ;
    }();

    [&] (){
      ObSysVars[346].default_value_ = "25000" ;
      ObSysVars[346].info_ = "Specifies the number of waiting transactions in the applier queue that trigger flow control. This variable can be changed without resetting Group Replication" ;
      ObSysVars[346].name_ = "group_replication_flow_control_applier_threshold" ;
      ObSysVars[346].data_type_ = ObIntType ;
      ObSysVars[346].min_val_ = "0" ;
      ObSysVars[346].max_val_ = "2147483647" ;
      ObSysVars[346].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[346].id_ = SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_APPLIER_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_APPLIER_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_APPLIER_THRESHOLD] = 346 ;
      ObSysVars[346].base_value_ = "25000" ;
    ObSysVars[346].alias_ = "OB_SV_GROUP_REPLICATION_FLOW_CONTROL_APPLIER_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[347].default_value_ = "25000" ;
      ObSysVars[347].info_ = "Specifies the number of waiting transactions in the certifier queue that trigger flow control. This variable can be changed without resetting Group Replication" ;
      ObSysVars[347].name_ = "group_replication_flow_control_certifier_threshold" ;
      ObSysVars[347].data_type_ = ObIntType ;
      ObSysVars[347].min_val_ = "0" ;
      ObSysVars[347].max_val_ = "2147483647" ;
      ObSysVars[347].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[347].id_ = SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_CERTIFIER_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_CERTIFIER_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_CERTIFIER_THRESHOLD] = 347 ;
      ObSysVars[347].base_value_ = "25000" ;
    ObSysVars[347].alias_ = "OB_SV_GROUP_REPLICATION_FLOW_CONTROL_CERTIFIER_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[348].default_value_ = "1" ;
      ObSysVars[348].info_ = "Specifies the mode used for flow control. This variable can be changed without resetting Group Replication" ;
      ObSysVars[348].name_ = "group_replication_flow_control_mode" ;
      ObSysVars[348].data_type_ = ObIntType ;
      ObSysVars[348].enum_names_ = "[u'DISABLED', u'QUOTA']" ;
      ObSysVars[348].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[348].id_ = SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_MODE] = 348 ;
      ObSysVars[348].base_value_ = "1" ;
    ObSysVars[348].alias_ = "OB_SV_GROUP_REPLICATION_FLOW_CONTROL_MODE" ;
    }();

    [&] (){
      ObSysVars[349].default_value_ = "" ;
      ObSysVars[349].info_ = "A list of peer addresses as a comma separated list such as host1:port1,host2:port2" ;
      ObSysVars[349].name_ = "group_replication_force_members" ;
      ObSysVars[349].data_type_ = ObVarcharType ;
      ObSysVars[349].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[349].id_ = SYS_VAR_GROUP_REPLICATION_FORCE_MEMBERS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_FORCE_MEMBERS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_FORCE_MEMBERS] = 349 ;
      ObSysVars[349].base_value_ = "" ;
    ObSysVars[349].alias_ = "OB_SV_GROUP_REPLICATION_FORCE_MEMBERS" ;
    }();

    [&] (){
      ObSysVars[350].default_value_ = "" ;
      ObSysVars[350].info_ = "The name of the group which this server instance belongs to. Must be a valid UUID. This UUID is used internally when setting GTIDs for Group Replication events in the binary log" ;
      ObSysVars[350].name_ = "group_replication_group_name" ;
      ObSysVars[350].data_type_ = ObVarcharType ;
      ObSysVars[350].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[350].id_ = SYS_VAR_GROUP_REPLICATION_GROUP_NAME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_GROUP_NAME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_GROUP_NAME] = 350 ;
      ObSysVars[350].base_value_ = "" ;
    ObSysVars[350].alias_ = "OB_SV_GROUP_REPLICATION_GROUP_NAME" ;
    }();

    [&] (){
      ObSysVars[351].default_value_ = "" ;
      ObSysVars[351].info_ = "The number of consecutive GTIDs that are reserved for each member. Each member consumes its blocks and reserves more when needed" ;
      ObSysVars[351].name_ = "group_replication_gtid_assignment_block_size" ;
      ObSysVars[351].data_type_ = ObVarcharType ;
      ObSysVars[351].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[351].id_ = SYS_VAR_GROUP_REPLICATION_GTID_ASSIGNMENT_BLOCK_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_GTID_ASSIGNMENT_BLOCK_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_GTID_ASSIGNMENT_BLOCK_SIZE] = 351 ;
      ObSysVars[351].base_value_ = "" ;
    ObSysVars[351].alias_ = "OB_SV_GROUP_REPLICATION_GTID_ASSIGNMENT_BLOCK_SIZE" ;
    }();

    [&] (){
      ObSysVars[352].default_value_ = "AUTOMATIC" ;
      ObSysVars[352].info_ = "Specifies the allowlist of hosts that are permitted to connect to the group" ;
      ObSysVars[352].name_ = "group_replication_ip_whitelist" ;
      ObSysVars[352].data_type_ = ObVarcharType ;
      ObSysVars[352].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[352].id_ = SYS_VAR_GROUP_REPLICATION_IP_WHITELIST ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_IP_WHITELIST)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_IP_WHITELIST] = 352 ;
      ObSysVars[352].base_value_ = "AUTOMATIC" ;
    ObSysVars[352].alias_ = "OB_SV_GROUP_REPLICATION_IP_WHITELIST" ;
    }();

    [&] (){
      ObSysVars[353].default_value_ = "" ;
      ObSysVars[353].info_ = "The network address which the member provides for connections from other members, specified as a host:port formatted string" ;
      ObSysVars[353].name_ = "group_replication_local_address" ;
      ObSysVars[353].data_type_ = ObVarcharType ;
      ObSysVars[353].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[353].id_ = SYS_VAR_GROUP_REPLICATION_LOCAL_ADDRESS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_LOCAL_ADDRESS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_LOCAL_ADDRESS] = 353 ;
      ObSysVars[353].base_value_ = "" ;
    ObSysVars[353].alias_ = "OB_SV_GROUP_REPLICATION_LOCAL_ADDRESS" ;
    }();

    [&] (){
      ObSysVars[354].default_value_ = "50" ;
      ObSysVars[354].info_ = "A percentage weight that can be assigned to members to influence the chance of the member being elected as primary in the event of failover, for example when the existing primary leaves a single-primary group" ;
      ObSysVars[354].name_ = "group_replication_member_weight" ;
      ObSysVars[354].data_type_ = ObIntType ;
      ObSysVars[354].min_val_ = "0" ;
      ObSysVars[354].max_val_ = "100" ;
      ObSysVars[354].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[354].id_ = SYS_VAR_GROUP_REPLICATION_MEMBER_WEIGHT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_MEMBER_WEIGHT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_MEMBER_WEIGHT] = 354 ;
      ObSysVars[354].base_value_ = "50" ;
    ObSysVars[354].alias_ = "OB_SV_GROUP_REPLICATION_MEMBER_WEIGHT" ;
    }();

    [&] (){
      ObSysVars[355].default_value_ = "0" ;
      ObSysVars[355].info_ = "The number of times the group communication thread waits for the communication engine mutex to be released before the thread waits for more incoming network messages" ;
      ObSysVars[355].name_ = "group_replication_poll_spin_loops" ;
      ObSysVars[355].data_type_ = ObIntType ;
      ObSysVars[355].min_val_ = "0" ;
      ObSysVars[355].max_val_ = "4294967295" ;
      ObSysVars[355].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[355].id_ = SYS_VAR_GROUP_REPLICATION_POLL_SPIN_LOOPS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_POLL_SPIN_LOOPS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_POLL_SPIN_LOOPS] = 355 ;
      ObSysVars[355].base_value_ = "0" ;
    ObSysVars[355].alias_ = "OB_SV_GROUP_REPLICATION_POLL_SPIN_LOOPS" ;
    }();

    [&] (){
      ObSysVars[356].default_value_ = "1" ;
      ObSysVars[356].info_ = "Recovery policies when handling cached transactions after state transfer" ;
      ObSysVars[356].name_ = "group_replication_recovery_complete_at" ;
      ObSysVars[356].data_type_ = ObIntType ;
      ObSysVars[356].enum_names_ = "[u'TRANSACTIONS_CERTIFIED', u'TRANSACTIONS_APPLIED']" ;
      ObSysVars[356].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[356].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_COMPLETE_AT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_COMPLETE_AT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_COMPLETE_AT] = 356 ;
      ObSysVars[356].base_value_ = "1" ;
    ObSysVars[356].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_COMPLETE_AT" ;
    }();

    [&] (){
      ObSysVars[357].default_value_ = "60" ;
      ObSysVars[357].info_ = "The sleep time, in seconds, between reconnection attempts when no donor was found in the group" ;
      ObSysVars[357].name_ = "group_replication_recovery_reconnect_interval" ;
      ObSysVars[357].data_type_ = ObIntType ;
      ObSysVars[357].min_val_ = "0" ;
      ObSysVars[357].max_val_ = "31536000" ;
      ObSysVars[357].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[357].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_RECONNECT_INTERVAL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_RECONNECT_INTERVAL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_RECONNECT_INTERVAL] = 357 ;
      ObSysVars[357].base_value_ = "60" ;
    ObSysVars[357].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_RECONNECT_INTERVAL" ;
    }();

    [&] (){
      ObSysVars[358].default_value_ = "10" ;
      ObSysVars[358].info_ = "The number of times that the member that is joining tries to connect to the available donors before giving up" ;
      ObSysVars[358].name_ = "group_replication_recovery_retry_count" ;
      ObSysVars[358].data_type_ = ObIntType ;
      ObSysVars[358].min_val_ = "0" ;
      ObSysVars[358].max_val_ = "31536000" ;
      ObSysVars[358].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[358].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_RETRY_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_RETRY_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_RETRY_COUNT] = 358 ;
      ObSysVars[358].base_value_ = "10" ;
    ObSysVars[358].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_RETRY_COUNT" ;
    }();

    [&] (){
      ObSysVars[359].default_value_ = "" ;
      ObSysVars[359].info_ = "The path to a file that contains a list of trusted SSL certificate authorities" ;
      ObSysVars[359].name_ = "group_replication_recovery_ssl_ca" ;
      ObSysVars[359].data_type_ = ObVarcharType ;
      ObSysVars[359].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[359].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CA] = 359 ;
      ObSysVars[359].base_value_ = "" ;
    ObSysVars[359].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_CA" ;
    }();

    [&] (){
      ObSysVars[360].default_value_ = "" ;
      ObSysVars[360].info_ = "The path to a directory that contains trusted SSL certificate authority certificates" ;
      ObSysVars[360].name_ = "group_replication_recovery_ssl_capath" ;
      ObSysVars[360].data_type_ = ObVarcharType ;
      ObSysVars[360].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[360].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CAPATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CAPATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CAPATH] = 360 ;
      ObSysVars[360].base_value_ = "" ;
    ObSysVars[360].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_CAPATH" ;
    }();

    [&] (){
      ObSysVars[361].default_value_ = "" ;
      ObSysVars[361].info_ = "The name of the SSL certificate file to use for establishing a secure connection" ;
      ObSysVars[361].name_ = "group_replication_recovery_ssl_cert" ;
      ObSysVars[361].data_type_ = ObVarcharType ;
      ObSysVars[361].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[361].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CERT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CERT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CERT] = 361 ;
      ObSysVars[361].base_value_ = "" ;
    ObSysVars[361].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_CERT" ;
    }();

    [&] (){
      ObSysVars[362].default_value_ = "" ;
      ObSysVars[362].info_ = "The list of permissible ciphers for SSL encryption" ;
      ObSysVars[362].name_ = "group_replication_recovery_ssl_cipher" ;
      ObSysVars[362].data_type_ = ObVarcharType ;
      ObSysVars[362].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[362].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CIPHER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CIPHER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CIPHER] = 362 ;
      ObSysVars[362].base_value_ = "" ;
    ObSysVars[362].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_CIPHER" ;
    }();

    [&] (){
      ObSysVars[363].default_value_ = "" ;
      ObSysVars[363].info_ = "The path to a directory that contains files containing certificate revocation lists" ;
      ObSysVars[363].name_ = "group_replication_recovery_ssl_crl" ;
      ObSysVars[363].data_type_ = ObVarcharType ;
      ObSysVars[363].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[363].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRL] = 363 ;
      ObSysVars[363].base_value_ = "" ;
    ObSysVars[363].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_CRL" ;
    }();

    [&] (){
      ObSysVars[364].default_value_ = "" ;
      ObSysVars[364].info_ = "The path to a directory that contains files containing certificate revocation lists" ;
      ObSysVars[364].name_ = "group_replication_recovery_ssl_crlpath" ;
      ObSysVars[364].data_type_ = ObVarcharType ;
      ObSysVars[364].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[364].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRLPATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRLPATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRLPATH] = 364 ;
      ObSysVars[364].base_value_ = "" ;
    ObSysVars[364].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_CRLPATH" ;
    }();

    [&] (){
      ObSysVars[365].default_value_ = "" ;
      ObSysVars[365].info_ = "The name of the SSL key file to use for establishing a secure connection" ;
      ObSysVars[365].name_ = "group_replication_recovery_ssl_key" ;
      ObSysVars[365].data_type_ = ObVarcharType ;
      ObSysVars[365].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[365].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_KEY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_KEY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_KEY] = 365 ;
      ObSysVars[365].base_value_ = "" ;
    ObSysVars[365].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_KEY" ;
    }();

    [&] (){
      ObSysVars[366].default_value_ = "0" ;
      ObSysVars[366].info_ = "Make the recovery process check the server's Common Name value in the donor sent certificate" ;
      ObSysVars[366].name_ = "group_replication_recovery_ssl_verify_server_cert" ;
      ObSysVars[366].data_type_ = ObIntType ;
      ObSysVars[366].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[366].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_VERIFY_SERVER_CERT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_VERIFY_SERVER_CERT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_VERIFY_SERVER_CERT] = 366 ;
      ObSysVars[366].base_value_ = "0" ;
    ObSysVars[366].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_SSL_VERIFY_SERVER_CERT" ;
    }();

    [&] (){
      ObSysVars[367].default_value_ = "0" ;
      ObSysVars[367].info_ = "Whether Group Replication recovery connection should use SSL or not" ;
      ObSysVars[367].name_ = "group_replication_recovery_use_ssl" ;
      ObSysVars[367].data_type_ = ObIntType ;
      ObSysVars[367].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[367].id_ = SYS_VAR_GROUP_REPLICATION_RECOVERY_USE_SSL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_RECOVERY_USE_SSL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_RECOVERY_USE_SSL] = 367 ;
      ObSysVars[367].base_value_ = "0" ;
    ObSysVars[367].alias_ = "OB_SV_GROUP_REPLICATION_RECOVERY_USE_SSL" ;
    }();

    [&] (){
      ObSysVars[368].default_value_ = "0" ;
      ObSysVars[368].info_ = "Instructs the group to pick a single server automatically to be the one that handles read/write workload. This server is the primary and all others are secondaries" ;
      ObSysVars[368].name_ = "group_replication_single_primary_mode" ;
      ObSysVars[368].data_type_ = ObIntType ;
      ObSysVars[368].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[368].id_ = SYS_VAR_GROUP_REPLICATION_SINGLE_PRIMARY_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_SINGLE_PRIMARY_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_SINGLE_PRIMARY_MODE] = 368 ;
      ObSysVars[368].base_value_ = "0" ;
    ObSysVars[368].alias_ = "OB_SV_GROUP_REPLICATION_SINGLE_PRIMARY_MODE" ;
    }();

    [&] (){
      ObSysVars[369].default_value_ = "0" ;
      ObSysVars[369].info_ = "Specifies the security state of the connection between Group Replication members" ;
      ObSysVars[369].name_ = "group_replication_ssl_mode" ;
      ObSysVars[369].data_type_ = ObIntType ;
      ObSysVars[369].enum_names_ = "[u'DISABLED', u'REQUIRED', u'VERIFY_CA', u'VERIFY_IDENTITY']" ;
      ObSysVars[369].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[369].id_ = SYS_VAR_GROUP_REPLICATION_SSL_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_SSL_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_SSL_MODE] = 369 ;
      ObSysVars[369].base_value_ = "0" ;
    ObSysVars[369].alias_ = "OB_SV_GROUP_REPLICATION_SSL_MODE" ;
    }();

    [&] (){
      ObSysVars[370].default_value_ = "0" ;
      ObSysVars[370].info_ = "Whether the server should start Group Replication or not during server start" ;
      ObSysVars[370].name_ = "group_replication_start_on_boot" ;
      ObSysVars[370].data_type_ = ObIntType ;
      ObSysVars[370].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[370].id_ = SYS_VAR_GROUP_REPLICATION_START_ON_BOOT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_START_ON_BOOT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_START_ON_BOOT] = 370 ;
      ObSysVars[370].base_value_ = "0" ;
    ObSysVars[370].alias_ = "OB_SV_GROUP_REPLICATION_START_ON_BOOT" ;
    }();

    [&] (){
      ObSysVars[371].default_value_ = "0" ;
      ObSysVars[371].info_ = "Configures the maximum transaction size in bytes which the replication group accepts" ;
      ObSysVars[371].name_ = "group_replication_transaction_size_limit" ;
      ObSysVars[371].data_type_ = ObIntType ;
      ObSysVars[371].min_val_ = "0" ;
      ObSysVars[371].max_val_ = "2147483647" ;
      ObSysVars[371].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[371].id_ = SYS_VAR_GROUP_REPLICATION_TRANSACTION_SIZE_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_TRANSACTION_SIZE_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_TRANSACTION_SIZE_LIMIT] = 371 ;
      ObSysVars[371].base_value_ = "0" ;
    ObSysVars[371].alias_ = "OB_SV_GROUP_REPLICATION_TRANSACTION_SIZE_LIMIT" ;
    }();

    [&] (){
      ObSysVars[372].default_value_ = "0" ;
      ObSysVars[372].info_ = "Configures how long members that suffer a network partition and cannot connect to the majority wait before leaving the group" ;
      ObSysVars[372].name_ = "group_replication_unreachable_majority_timeout" ;
      ObSysVars[372].data_type_ = ObIntType ;
      ObSysVars[372].min_val_ = "0" ;
      ObSysVars[372].max_val_ = "31536000" ;
      ObSysVars[372].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[372].id_ = SYS_VAR_GROUP_REPLICATION_UNREACHABLE_MAJORITY_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_UNREACHABLE_MAJORITY_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_UNREACHABLE_MAJORITY_TIMEOUT] = 372 ;
      ObSysVars[372].base_value_ = "0" ;
    ObSysVars[372].alias_ = "OB_SV_GROUP_REPLICATION_UNREACHABLE_MAJORITY_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[373].default_value_ = "0" ;
      ObSysVars[373].info_ = "The replication thread delay in milliseconds on a replica server if innodb_thread_concurrency is reached" ;
      ObSysVars[373].name_ = "innodb_replication_delay" ;
      ObSysVars[373].data_type_ = ObIntType ;
      ObSysVars[373].min_val_ = "0" ;
      ObSysVars[373].max_val_ = "4294967295" ;
      ObSysVars[373].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[373].id_ = SYS_VAR_INNODB_REPLICATION_DELAY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_REPLICATION_DELAY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_REPLICATION_DELAY] = 373 ;
      ObSysVars[373].base_value_ = "0" ;
    ObSysVars[373].alias_ = "OB_SV_INNODB_REPLICATION_DELAY" ;
    }();

    [&] (){
      ObSysVars[374].default_value_ = "FILE" ;
      ObSysVars[374].info_ = "The setting of this variable determines whether the replica records metadata about the source, consisting of status and connection information, to an InnoDB table in the mysql system database, or as a file in the data directory" ;
      ObSysVars[374].name_ = "master_info_repository" ;
      ObSysVars[374].data_type_ = ObVarcharType ;
      ObSysVars[374].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[374].id_ = SYS_VAR_MASTER_INFO_REPOSITORY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MASTER_INFO_REPOSITORY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MASTER_INFO_REPOSITORY] = 374 ;
      ObSysVars[374].base_value_ = "FILE" ;
    ObSysVars[374].alias_ = "OB_SV_MASTER_INFO_REPOSITORY" ;
    }();

    [&] (){
      ObSysVars[375].default_value_ = "0" ;
      ObSysVars[375].info_ = "Enabling this variable causes the source to verify events read from the binary log by examining checksums, and to stop with an error in the event of a mismatch" ;
      ObSysVars[375].name_ = "master_verify_checksum" ;
      ObSysVars[375].data_type_ = ObIntType ;
      ObSysVars[375].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[375].id_ = SYS_VAR_MASTER_VERIFY_CHECKSUM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MASTER_VERIFY_CHECKSUM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MASTER_VERIFY_CHECKSUM] = 375 ;
      ObSysVars[375].base_value_ = "0" ;
    ObSysVars[375].alias_ = "OB_SV_MASTER_VERIFY_CHECKSUM" ;
    }();

    [&] (){
      ObSysVars[376].default_value_ = "0" ;
      ObSysVars[376].info_ = "Assists with the correct handling of transactions that originated on older or newer servers than the server currently processing them" ;
      ObSysVars[376].name_ = "pseudo_slave_mode" ;
      ObSysVars[376].data_type_ = ObIntType ;
      ObSysVars[376].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[376].id_ = SYS_VAR_PSEUDO_SLAVE_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PSEUDO_SLAVE_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PSEUDO_SLAVE_MODE] = 376 ;
      ObSysVars[376].base_value_ = "0" ;
    ObSysVars[376].alias_ = "OB_SV_PSEUDO_SLAVE_MODE" ;
    }();

    [&] (){
      ObSysVars[377].default_value_ = "2147483647" ;
      ObSysVars[377].info_ = "This variable is for internal server use" ;
      ObSysVars[377].name_ = "pseudo_thread_id" ;
      ObSysVars[377].data_type_ = ObIntType ;
      ObSysVars[377].min_val_ = "0" ;
      ObSysVars[377].max_val_ = "2147483647" ;
      ObSysVars[377].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[377].id_ = SYS_VAR_PSEUDO_THREAD_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PSEUDO_THREAD_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PSEUDO_THREAD_ID] = 377 ;
      ObSysVars[377].base_value_ = "2147483647" ;
    ObSysVars[377].alias_ = "OB_SV_PSEUDO_THREAD_ID" ;
    }();

    [&] (){
      ObSysVars[378].default_value_ = "0" ;
      ObSysVars[378].info_ = "For internal use by mysqlbinlog" ;
      ObSysVars[378].name_ = "rbr_exec_mode" ;
      ObSysVars[378].data_type_ = ObIntType ;
      ObSysVars[378].enum_names_ = "[u'STRICT', u'IDEMPOTENT']" ;
      ObSysVars[378].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[378].id_ = SYS_VAR_RBR_EXEC_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RBR_EXEC_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RBR_EXEC_MODE] = 378 ;
      ObSysVars[378].base_value_ = "0" ;
    ObSysVars[378].alias_ = "OB_SV_RBR_EXEC_MODE" ;
    }();

    [&] (){
      ObSysVars[379].default_value_ = "0" ;
      ObSysVars[379].info_ = "Use shared locks, and avoid unnecessary lock acquisitions, to improve performance for semisynchronous replication" ;
      ObSysVars[379].name_ = "replication_optimize_for_static_plugin_config" ;
      ObSysVars[379].data_type_ = ObIntType ;
      ObSysVars[379].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[379].id_ = SYS_VAR_REPLICATION_OPTIMIZE_FOR_STATIC_PLUGIN_CONFIG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_REPLICATION_OPTIMIZE_FOR_STATIC_PLUGIN_CONFIG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_REPLICATION_OPTIMIZE_FOR_STATIC_PLUGIN_CONFIG] = 379 ;
      ObSysVars[379].base_value_ = "0" ;
    ObSysVars[379].alias_ = "OB_SV_REPLICATION_OPTIMIZE_FOR_STATIC_PLUGIN_CONFIG" ;
    }();

    [&] (){
      ObSysVars[380].default_value_ = "0" ;
      ObSysVars[380].info_ = "Limit callbacks to improve performance for semisynchronous replication" ;
      ObSysVars[380].name_ = "replication_sender_observe_commit_only" ;
      ObSysVars[380].data_type_ = ObIntType ;
      ObSysVars[380].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[380].id_ = SYS_VAR_REPLICATION_SENDER_OBSERVE_COMMIT_ONLY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_REPLICATION_SENDER_OBSERVE_COMMIT_ONLY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_REPLICATION_SENDER_OBSERVE_COMMIT_ONLY] = 380 ;
      ObSysVars[380].base_value_ = "0" ;
    ObSysVars[380].alias_ = "OB_SV_REPLICATION_SENDER_OBSERVE_COMMIT_ONLY" ;
    }();

    [&] (){
      ObSysVars[381].default_value_ = "0" ;
      ObSysVars[381].info_ = "Controls whether semisynchronous replication is enabled on the source" ;
      ObSysVars[381].name_ = "rpl_semi_sync_master_enabled" ;
      ObSysVars[381].data_type_ = ObIntType ;
      ObSysVars[381].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[381].id_ = SYS_VAR_RPL_SEMI_SYNC_MASTER_ENABLED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_MASTER_ENABLED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_MASTER_ENABLED] = 381 ;
      ObSysVars[381].base_value_ = "0" ;
    ObSysVars[381].alias_ = "OB_SV_RPL_SEMI_SYNC_MASTER_ENABLED" ;
    }();

    [&] (){
      ObSysVars[382].default_value_ = "10000" ;
      ObSysVars[382].info_ = "A value in milliseconds that controls how long the source waits on a commit for acknowledgment from a replica before timing out and reverting to asynchronous replication" ;
      ObSysVars[382].name_ = "rpl_semi_sync_master_timeout" ;
      ObSysVars[382].data_type_ = ObIntType ;
      ObSysVars[382].min_val_ = "0" ;
      ObSysVars[382].max_val_ = "4294967295" ;
      ObSysVars[382].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[382].id_ = SYS_VAR_RPL_SEMI_SYNC_MASTER_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_MASTER_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_MASTER_TIMEOUT] = 382 ;
      ObSysVars[382].base_value_ = "10000" ;
    ObSysVars[382].alias_ = "OB_SV_RPL_SEMI_SYNC_MASTER_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[383].default_value_ = "32" ;
      ObSysVars[383].info_ = "The semisynchronous replication debug trace level on the source" ;
      ObSysVars[383].name_ = "rpl_semi_sync_master_trace_level" ;
      ObSysVars[383].data_type_ = ObIntType ;
      ObSysVars[383].min_val_ = "0" ;
      ObSysVars[383].max_val_ = "4294967295" ;
      ObSysVars[383].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[383].id_ = SYS_VAR_RPL_SEMI_SYNC_MASTER_TRACE_LEVEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_MASTER_TRACE_LEVEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_MASTER_TRACE_LEVEL] = 383 ;
      ObSysVars[383].base_value_ = "32" ;
    ObSysVars[383].alias_ = "OB_SV_RPL_SEMI_SYNC_MASTER_TRACE_LEVEL" ;
    }();

    [&] (){
      ObSysVars[384].default_value_ = "1" ;
      ObSysVars[384].info_ = "The number of replica acknowledgments the source must receive per transaction before proceeding" ;
      ObSysVars[384].name_ = "rpl_semi_sync_master_wait_for_slave_count" ;
      ObSysVars[384].data_type_ = ObIntType ;
      ObSysVars[384].min_val_ = "1" ;
      ObSysVars[384].max_val_ = "65535" ;
      ObSysVars[384].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[384].id_ = SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_FOR_SLAVE_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_FOR_SLAVE_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_FOR_SLAVE_COUNT] = 384 ;
      ObSysVars[384].base_value_ = "1" ;
    ObSysVars[384].alias_ = "OB_SV_RPL_SEMI_SYNC_MASTER_WAIT_FOR_SLAVE_COUNT" ;
    }();

    [&] (){
      ObSysVars[385].default_value_ = "0" ;
      ObSysVars[385].info_ = "Controls whether the source waits for the timeout period configured by rpl_semi_sync_master_timeout to expire, even if the replica count drops to less than the number of replicas configured by rpl_semi_sync_master_wait_for_slave_count during the timeout period" ;
      ObSysVars[385].name_ = "rpl_semi_sync_master_wait_no_slave" ;
      ObSysVars[385].data_type_ = ObIntType ;
      ObSysVars[385].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[385].id_ = SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_NO_SLAVE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_NO_SLAVE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_NO_SLAVE] = 385 ;
      ObSysVars[385].base_value_ = "0" ;
    ObSysVars[385].alias_ = "OB_SV_RPL_SEMI_SYNC_MASTER_WAIT_NO_SLAVE" ;
    }();

    [&] (){
      ObSysVars[386].default_value_ = "0" ;
      ObSysVars[386].info_ = "This variable controls the point at which a semisynchronous source waits for replica acknowledgment of transaction receipt before returning a status to the client that committed the transaction" ;
      ObSysVars[386].name_ = "rpl_semi_sync_master_wait_point" ;
      ObSysVars[386].data_type_ = ObIntType ;
      ObSysVars[386].enum_names_ = "[u'AFTER_SYNC', u'AFTER_COMMIT']" ;
      ObSysVars[386].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[386].id_ = SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_POINT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_POINT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_POINT] = 386 ;
      ObSysVars[386].base_value_ = "0" ;
    ObSysVars[386].alias_ = "OB_SV_RPL_SEMI_SYNC_MASTER_WAIT_POINT" ;
    }();

    [&] (){
      ObSysVars[387].default_value_ = "0" ;
      ObSysVars[387].info_ = "Controls whether semisynchronous replication is enabled on the replica" ;
      ObSysVars[387].name_ = "rpl_semi_sync_slave_enabled" ;
      ObSysVars[387].data_type_ = ObIntType ;
      ObSysVars[387].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[387].id_ = SYS_VAR_RPL_SEMI_SYNC_SLAVE_ENABLED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_SLAVE_ENABLED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_SLAVE_ENABLED] = 387 ;
      ObSysVars[387].base_value_ = "0" ;
    ObSysVars[387].alias_ = "OB_SV_RPL_SEMI_SYNC_SLAVE_ENABLED" ;
    }();

    [&] (){
      ObSysVars[388].default_value_ = "32" ;
      ObSysVars[388].info_ = "The semisynchronous replication debug trace level on the replica" ;
      ObSysVars[388].name_ = "rpl_semi_sync_slave_trace_level" ;
      ObSysVars[388].data_type_ = ObIntType ;
      ObSysVars[388].min_val_ = "0" ;
      ObSysVars[388].max_val_ = "4294967295" ;
      ObSysVars[388].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[388].id_ = SYS_VAR_RPL_SEMI_SYNC_SLAVE_TRACE_LEVEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_SEMI_SYNC_SLAVE_TRACE_LEVEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_SEMI_SYNC_SLAVE_TRACE_LEVEL] = 388 ;
      ObSysVars[388].base_value_ = "32" ;
    ObSysVars[388].alias_ = "OB_SV_RPL_SEMI_SYNC_SLAVE_TRACE_LEVEL" ;
    }();

    [&] (){
      ObSysVars[389].default_value_ = "31536000" ;
      ObSysVars[389].info_ = "You can control the length of time (in seconds) that STOP SLAVE waits before timing out by setting this variab" ;
      ObSysVars[389].name_ = "rpl_stop_slave_timeout" ;
      ObSysVars[389].data_type_ = ObIntType ;
      ObSysVars[389].min_val_ = "2" ;
      ObSysVars[389].max_val_ = "31536000" ;
      ObSysVars[389].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[389].id_ = SYS_VAR_RPL_STOP_SLAVE_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RPL_STOP_SLAVE_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RPL_STOP_SLAVE_TIMEOUT] = 389 ;
      ObSysVars[389].base_value_ = "31536000" ;
    ObSysVars[389].alias_ = "OB_SV_RPL_STOP_SLAVE_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[390].default_value_ = "0" ;
      ObSysVars[390].info_ = "Whether or not batched updates are enabled on NDB Cluster replicas" ;
      ObSysVars[390].name_ = "slave_allow_batching" ;
      ObSysVars[390].data_type_ = ObIntType ;
      ObSysVars[390].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[390].id_ = SYS_VAR_SLAVE_ALLOW_BATCHING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_ALLOW_BATCHING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_ALLOW_BATCHING] = 390 ;
      ObSysVars[390].base_value_ = "0" ;
    ObSysVars[390].alias_ = "OB_SV_SLAVE_ALLOW_BATCHING" ;
    }();

    [&] (){
      ObSysVars[391].default_value_ = "512" ;
      ObSysVars[391].info_ = "Sets the maximum number of transactions that can be processed by a multithreaded replica before a checkpoint operation is called to update its status as shown by SHOW SLAVE STATUS" ;
      ObSysVars[391].name_ = "slave_checkpoint_group" ;
      ObSysVars[391].data_type_ = ObIntType ;
      ObSysVars[391].min_val_ = "32" ;
      ObSysVars[391].max_val_ = "524280" ;
      ObSysVars[391].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[391].id_ = SYS_VAR_SLAVE_CHECKPOINT_GROUP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_CHECKPOINT_GROUP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_CHECKPOINT_GROUP] = 391 ;
      ObSysVars[391].base_value_ = "512" ;
    ObSysVars[391].alias_ = "OB_SV_SLAVE_CHECKPOINT_GROUP" ;
    }();

    [&] (){
      ObSysVars[392].default_value_ = "300" ;
      ObSysVars[392].info_ = "Sets the maximum time (in milliseconds) that is allowed to pass before a checkpoint operation is called to update the status of a multithreaded replica as shown by SHOW SLAVE STATUS" ;
      ObSysVars[392].name_ = "slave_checkpoint_period" ;
      ObSysVars[392].data_type_ = ObIntType ;
      ObSysVars[392].min_val_ = "1" ;
      ObSysVars[392].max_val_ = "4294967295" ;
      ObSysVars[392].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[392].id_ = SYS_VAR_SLAVE_CHECKPOINT_PERIOD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_CHECKPOINT_PERIOD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_CHECKPOINT_PERIOD] = 392 ;
      ObSysVars[392].base_value_ = "300" ;
    ObSysVars[392].alias_ = "OB_SV_SLAVE_CHECKPOINT_PERIOD" ;
    }();

    [&] (){
      ObSysVars[393].default_value_ = "0" ;
      ObSysVars[393].info_ = "Whether to use compression of the source/replica protocol if both source and replica support it" ;
      ObSysVars[393].name_ = "slave_compressed_protocol" ;
      ObSysVars[393].data_type_ = ObIntType ;
      ObSysVars[393].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[393].id_ = SYS_VAR_SLAVE_COMPRESSED_PROTOCOL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_COMPRESSED_PROTOCOL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_COMPRESSED_PROTOCOL] = 393 ;
      ObSysVars[393].base_value_ = "0" ;
    ObSysVars[393].alias_ = "OB_SV_SLAVE_COMPRESSED_PROTOCOL" ;
    }();

    [&] (){
      ObSysVars[394].default_value_ = "0" ;
      ObSysVars[394].info_ = "Controls how a replication thread resolves conflicts and errors during replication" ;
      ObSysVars[394].name_ = "slave_exec_mode" ;
      ObSysVars[394].data_type_ = ObIntType ;
      ObSysVars[394].enum_names_ = "[u'STRICT', u'IDEMPOTENT']" ;
      ObSysVars[394].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[394].id_ = SYS_VAR_SLAVE_EXEC_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_EXEC_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_EXEC_MODE] = 394 ;
      ObSysVars[394].base_value_ = "0" ;
    ObSysVars[394].alias_ = "OB_SV_SLAVE_EXEC_MODE" ;
    }();

    [&] (){
      ObSysVars[395].default_value_ = "1073741824" ;
      ObSysVars[395].info_ = "This variable sets the maximum packet size for the replication SQL and I/O threads, so that large updates using row-based replication do not cause replication to fail because an update exceeded max_allowed_packet" ;
      ObSysVars[395].name_ = "slave_max_allowed_packet" ;
      ObSysVars[395].data_type_ = ObIntType ;
      ObSysVars[395].min_val_ = "1024" ;
      ObSysVars[395].max_val_ = "1073741824" ;
      ObSysVars[395].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[395].id_ = SYS_VAR_SLAVE_MAX_ALLOWED_PACKET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_MAX_ALLOWED_PACKET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_MAX_ALLOWED_PACKET] = 395 ;
      ObSysVars[395].base_value_ = "1073741824" ;
    ObSysVars[395].alias_ = "OB_SV_SLAVE_MAX_ALLOWED_PACKET" ;
    }();

    [&] (){
      ObSysVars[396].default_value_ = "60" ;
      ObSysVars[396].info_ = "The number of seconds to wait for more data or a heartbeat signal from the source before the replica considers the connection broken, aborts the read, and tries to reconnect" ;
      ObSysVars[396].name_ = "slave_net_timeout" ;
      ObSysVars[396].data_type_ = ObIntType ;
      ObSysVars[396].min_val_ = "1" ;
      ObSysVars[396].max_val_ = "31536000" ;
      ObSysVars[396].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[396].id_ = SYS_VAR_SLAVE_NET_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_NET_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_NET_TIMEOUT] = 396 ;
      ObSysVars[396].base_value_ = "60" ;
    ObSysVars[396].alias_ = "OB_SV_SLAVE_NET_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[397].default_value_ = "0" ;
      ObSysVars[397].info_ = "When using a multithreaded replica (slave_parallel_workers is greater than 0), this variable specifies the policy used to decide which transactions are allowed to execute in parallel on the replica" ;
      ObSysVars[397].name_ = "slave_parallel_type" ;
      ObSysVars[397].data_type_ = ObIntType ;
      ObSysVars[397].enum_names_ = "[u'DATABASE', u'LOGICAL_CLOCK']" ;
      ObSysVars[397].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[397].id_ = SYS_VAR_SLAVE_PARALLEL_TYPE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_PARALLEL_TYPE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_PARALLEL_TYPE] = 397 ;
      ObSysVars[397].base_value_ = "0" ;
    ObSysVars[397].alias_ = "OB_SV_SLAVE_PARALLEL_TYPE" ;
    }();

    [&] (){
      ObSysVars[398].default_value_ = "0" ;
      ObSysVars[398].info_ = "Sets the number of applier threads for executing replication transactions in parallel" ;
      ObSysVars[398].name_ = "slave_parallel_workers" ;
      ObSysVars[398].data_type_ = ObIntType ;
      ObSysVars[398].min_val_ = "0" ;
      ObSysVars[398].max_val_ = "1024" ;
      ObSysVars[398].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[398].id_ = SYS_VAR_SLAVE_PARALLEL_WORKERS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_PARALLEL_WORKERS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_PARALLEL_WORKERS] = 398 ;
      ObSysVars[398].base_value_ = "0" ;
    ObSysVars[398].alias_ = "OB_SV_SLAVE_PARALLEL_WORKERS" ;
    }();

    [&] (){
      ObSysVars[399].default_value_ = "16777216" ;
      ObSysVars[399].info_ = "For multithreaded replicas, this variable sets the maximum amount of memory (in bytes) available to worker queues holding events not yet applied" ;
      ObSysVars[399].name_ = "slave_pending_jobs_size_max" ;
      ObSysVars[399].data_type_ = ObUInt64Type ;
      ObSysVars[399].min_val_ = "1024" ;
      ObSysVars[399].max_val_ = "18446744073709551615" ;
      ObSysVars[399].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[399].id_ = SYS_VAR_SLAVE_PENDING_JOBS_SIZE_MAX ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_PENDING_JOBS_SIZE_MAX)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_PENDING_JOBS_SIZE_MAX] = 399 ;
      ObSysVars[399].base_value_ = "16777216" ;
    ObSysVars[399].alias_ = "OB_SV_SLAVE_PENDING_JOBS_SIZE_MAX" ;
    }();

    [&] (){
      ObSysVars[400].default_value_ = "0" ;
      ObSysVars[400].info_ = "For multithreaded replicas, the setting 1 for this variable ensures that transactions are externalized on the replica in the same order as they appear in the replica's relay log, and prevents gaps in the sequence of transactions that have been executed from the relay lo" ;
      ObSysVars[400].name_ = "slave_preserve_commit_order" ;
      ObSysVars[400].data_type_ = ObIntType ;
      ObSysVars[400].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[400].id_ = SYS_VAR_SLAVE_PRESERVE_COMMIT_ORDER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_PRESERVE_COMMIT_ORDER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_PRESERVE_COMMIT_ORDER] = 400 ;
      ObSysVars[400].base_value_ = "0" ;
    ObSysVars[400].alias_ = "OB_SV_SLAVE_PRESERVE_COMMIT_ORDER" ;
    }();

    [&] (){
      ObSysVars[401].default_value_ = "0" ;
      ObSysVars[401].info_ = "Cause the replication SQL thread to verify data using the checksums read from the relay log" ;
      ObSysVars[401].name_ = "slave_sql_verify_checksum" ;
      ObSysVars[401].data_type_ = ObIntType ;
      ObSysVars[401].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[401].id_ = SYS_VAR_SLAVE_SQL_VERIFY_CHECKSUM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_SQL_VERIFY_CHECKSUM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_SQL_VERIFY_CHECKSUM] = 401 ;
      ObSysVars[401].base_value_ = "0" ;
    ObSysVars[401].alias_ = "OB_SV_SLAVE_SQL_VERIFY_CHECKSUM" ;
    }();

    [&] (){
      ObSysVars[402].default_value_ = "10" ;
      ObSysVars[402].info_ = "Deadlock or because the transaction's execution time exceeded InnoDB's innodb_lock_wait_timeout or NDB's TransactionDeadlockDetectionTimeout or TransactionInactiveTimeout, it automatically retries slave_transaction_retries times before stopping with an error. Transactions with a non-temporary error are not retried" ;
      ObSysVars[402].name_ = "slave_transaction_retries" ;
      ObSysVars[402].data_type_ = ObUInt64Type ;
      ObSysVars[402].min_val_ = "0" ;
      ObSysVars[402].max_val_ = "18446744073709551615" ;
      ObSysVars[402].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[402].id_ = SYS_VAR_SLAVE_TRANSACTION_RETRIES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_TRANSACTION_RETRIES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_TRANSACTION_RETRIES] = 402 ;
      ObSysVars[402].base_value_ = "10" ;
    ObSysVars[402].alias_ = "OB_SV_SLAVE_TRANSACTION_RETRIES" ;
    }();

    [&] (){
      ObSysVars[403].default_value_ = "0" ;
      ObSysVars[403].info_ = "The number of events from the source that a replica should ski" ;
      ObSysVars[403].name_ = "sql_slave_skip_counter" ;
      ObSysVars[403].data_type_ = ObIntType ;
      ObSysVars[403].min_val_ = "0" ;
      ObSysVars[403].max_val_ = "4294967295" ;
      ObSysVars[403].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[403].id_ = SYS_VAR_SQL_SLAVE_SKIP_COUNTER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_SLAVE_SKIP_COUNTER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_SLAVE_SKIP_COUNTER] = 403 ;
      ObSysVars[403].base_value_ = "0" ;
    ObSysVars[403].alias_ = "OB_SV_SQL_SLAVE_SKIP_COUNTER" ;
    }();

    [&] (){
      ObSysVars[404].default_value_ = "0" ;
      ObSysVars[404].info_ = "The crash recovery mode, typically only changed in serious troubleshooting situations. Possible values are from 0 to 6" ;
      ObSysVars[404].name_ = "innodb_force_recovery" ;
      ObSysVars[404].data_type_ = ObIntType ;
      ObSysVars[404].min_val_ = "0" ;
      ObSysVars[404].max_val_ = "6" ;
      ObSysVars[404].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[404].id_ = SYS_VAR_INNODB_FORCE_RECOVERY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FORCE_RECOVERY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FORCE_RECOVERY] = 404 ;
      ObSysVars[404].base_value_ = "0" ;
    ObSysVars[404].alias_ = "OB_SV_INNODB_FORCE_RECOVERY" ;
    }();

    [&] (){
      ObSysVars[405].default_value_ = "0" ;
      ObSysVars[405].info_ = "Tells the replica server not to start the replication threads when the server starts" ;
      ObSysVars[405].name_ = "skip_slave_start" ;
      ObSysVars[405].data_type_ = ObIntType ;
      ObSysVars[405].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[405].id_ = SYS_VAR_SKIP_SLAVE_START ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SKIP_SLAVE_START)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SKIP_SLAVE_START] = 405 ;
      ObSysVars[405].base_value_ = "0" ;
    ObSysVars[405].alias_ = "OB_SV_SKIP_SLAVE_START" ;
    }();

    [&] (){
      ObSysVars[406].default_value_ = "" ;
      ObSysVars[406].info_ = "The name of the directory where the replica creates temporary files" ;
      ObSysVars[406].name_ = "slave_load_tmpdir" ;
      ObSysVars[406].data_type_ = ObVarcharType ;
      ObSysVars[406].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[406].id_ = SYS_VAR_SLAVE_LOAD_TMPDIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_LOAD_TMPDIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_LOAD_TMPDIR] = 406 ;
      ObSysVars[406].base_value_ = "" ;
    ObSysVars[406].alias_ = "OB_SV_SLAVE_LOAD_TMPDIR" ;
    }();

    [&] (){
      ObSysVars[407].default_value_ = "OFF" ;
      ObSysVars[407].info_ = "This variable causes the replication SQL thread to continue replication when a statement returns any of the errors listed in the variable value" ;
      ObSysVars[407].name_ = "slave_skip_errors" ;
      ObSysVars[407].data_type_ = ObVarcharType ;
      ObSysVars[407].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[407].id_ = SYS_VAR_SLAVE_SKIP_ERRORS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_SKIP_ERRORS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_SKIP_ERRORS] = 407 ;
      ObSysVars[407].base_value_ = "OFF" ;
    ObSysVars[407].alias_ = "OB_SV_SLAVE_SKIP_ERRORS" ;
    }();

    [&] (){
      ObSysVars[408].default_value_ = "0" ;
      ObSysVars[408].info_ = "Enables sync debug checking for the InnoDB storage engine" ;
      ObSysVars[408].name_ = "innodb_sync_debug" ;
      ObSysVars[408].data_type_ = ObIntType ;
      ObSysVars[408].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[408].id_ = SYS_VAR_INNODB_SYNC_DEBUG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_SYNC_DEBUG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_SYNC_DEBUG] = 408 ;
      ObSysVars[408].base_value_ = "0" ;
    ObSysVars[408].alias_ = "OB_SV_INNODB_SYNC_DEBUG" ;
    }();

    [&] (){
      ObSysVars[409].default_value_ = "45" ;
      ObSysVars[409].info_ = "control default collation for utf8mb4" ;
      ObSysVars[409].name_ = "default_collation_for_utf8mb4" ;
      ObSysVars[409].data_type_ = ObIntType ;
      ObSysVars[409].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_collation" ;
      ObSysVars[409].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[409].base_class_ = "ObCharsetSysVar" ;
      ObSysVars[409].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_collation" ;
      ObSysVars[409].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_default_value_for_utf8mb4" ;
      ObSysVars[409].id_ = SYS_VAR_DEFAULT_COLLATION_FOR_UTF8MB4 ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_COLLATION_FOR_UTF8MB4)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_COLLATION_FOR_UTF8MB4] = 409 ;
      ObSysVars[409].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar" ;
      ObSysVars[409].base_value_ = "45" ;
    ObSysVars[409].alias_ = "OB_SV_DEFAULT_COLLATION_FOR_UTF8MB4" ;
    }();

    [&] (){
      ObSysVars[410].default_value_ = "0" ;
      ObSysVars[410].info_ = "control wether to enable old charset aggregation rule" ;
      ObSysVars[410].name_ = "_enable_old_charset_aggregation" ;
      ObSysVars[410].data_type_ = ObIntType ;
      ObSysVars[410].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::INVISIBLE ;
      ObSysVars[410].id_ = SYS_VAR__ENABLE_OLD_CHARSET_AGGREGATION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_OLD_CHARSET_AGGREGATION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_OLD_CHARSET_AGGREGATION] = 410 ;
      ObSysVars[410].base_value_ = "0" ;
    ObSysVars[410].alias_ = "OB_SV__ENABLE_OLD_CHARSET_AGGREGATION" ;
    }();

    [&] (){
      ObSysVars[411].default_value_ = "0" ;
      ObSysVars[411].info_ = "The value to be used by the following INSERT or ALTER TABLE statement when inserting an AUTO_INCREMENT value. Merely simulates MySQL 5.7." ;
      ObSysVars[411].name_ = "insert_id" ;
      ObSysVars[411].data_type_ = ObIntType ;
      ObSysVars[411].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[411].id_ = SYS_VAR_INSERT_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INSERT_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INSERT_ID] = 411 ;
      ObSysVars[411].base_value_ = "0" ;
    ObSysVars[411].alias_ = "OB_SV_INSERT_ID" ;
    }();

    [&] (){
      ObSysVars[412].default_value_ = "262144" ;
      ObSysVars[412].info_ = "The minimum size of the buffer that is used for plain index scans, range index scans, and joins that do not use indexes and thus perform full table scans. Merely simulates MySQL 5.7." ;
      ObSysVars[412].name_ = "join_buffer_size" ;
      ObSysVars[412].data_type_ = ObIntType ;
      ObSysVars[412].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[412].id_ = SYS_VAR_JOIN_BUFFER_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_JOIN_BUFFER_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_JOIN_BUFFER_SIZE] = 412 ;
      ObSysVars[412].base_value_ = "262144" ;
    ObSysVars[412].alias_ = "OB_SV_JOIN_BUFFER_SIZE" ;
    }();

    [&] (){
      ObSysVars[413].default_value_ = "18446744073709547520" ;
      ObSysVars[413].info_ = "Do not permit statements that probably need to examine more than max_join_size rows (for single-table statements) or row combinations (for multiple-table statements) or that are likely to do more than max_join_size disk seeks. Merely simulates MySQL 5.7." ;
      ObSysVars[413].name_ = "max_join_size" ;
      ObSysVars[413].data_type_ = ObUInt64Type ;
      ObSysVars[413].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[413].id_ = SYS_VAR_MAX_JOIN_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_JOIN_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_JOIN_SIZE] = 413 ;
      ObSysVars[413].base_value_ = "18446744073709547520" ;
    ObSysVars[413].alias_ = "OB_SV_MAX_JOIN_SIZE" ;
    }();

    [&] (){
      ObSysVars[414].default_value_ = "1024" ;
      ObSysVars[414].info_ = "The cutoff on the size of index values that determines which filesort algorithm to use. Merely simulates MySQL 5.7." ;
      ObSysVars[414].name_ = "max_length_for_sort_data" ;
      ObSysVars[414].data_type_ = ObIntType ;
      ObSysVars[414].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[414].id_ = SYS_VAR_MAX_LENGTH_FOR_SORT_DATA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_LENGTH_FOR_SORT_DATA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_LENGTH_FOR_SORT_DATA] = 414 ;
      ObSysVars[414].base_value_ = "1024" ;
    ObSysVars[414].alias_ = "OB_SV_MAX_LENGTH_FOR_SORT_DATA" ;
    }();

    [&] (){
      ObSysVars[415].default_value_ = "16382" ;
      ObSysVars[415].info_ = "This variable limits the total number of prepared statements in the server. Merely simulates MySQL 5.7." ;
      ObSysVars[415].name_ = "max_prepared_stmt_count" ;
      ObSysVars[415].data_type_ = ObIntType ;
      ObSysVars[415].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[415].id_ = SYS_VAR_MAX_PREPARED_STMT_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_PREPARED_STMT_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_PREPARED_STMT_COUNT] = 415 ;
      ObSysVars[415].base_value_ = "16382" ;
    ObSysVars[415].alias_ = "OB_SV_MAX_PREPARED_STMT_COUNT" ;
    }();

    [&] (){
      ObSysVars[416].default_value_ = "1024" ;
      ObSysVars[416].info_ = "The number of bytes to use when sorting data values. Merely simulates MySQL 5.7." ;
      ObSysVars[416].name_ = "max_sort_length" ;
      ObSysVars[416].data_type_ = ObIntType ;
      ObSysVars[416].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[416].id_ = SYS_VAR_MAX_SORT_LENGTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_SORT_LENGTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_SORT_LENGTH] = 416 ;
      ObSysVars[416].base_value_ = "1024" ;
    ObSysVars[416].alias_ = "OB_SV_MAX_SORT_LENGTH" ;
    }();

    [&] (){
      ObSysVars[417].default_value_ = "0" ;
      ObSysVars[417].info_ = "Queries that examine fewer than this number of rows are not logged to the slow query log. Merely simulates MySQL 5.7." ;
      ObSysVars[417].name_ = "min_examined_row_limit" ;
      ObSysVars[417].data_type_ = ObIntType ;
      ObSysVars[417].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[417].id_ = SYS_VAR_MIN_EXAMINED_ROW_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MIN_EXAMINED_ROW_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MIN_EXAMINED_ROW_LIMIT] = 417 ;
      ObSysVars[417].base_value_ = "0" ;
    ObSysVars[417].alias_ = "OB_SV_MIN_EXAMINED_ROW_LIMIT" ;
    }();

    [&] (){
      ObSysVars[418].default_value_ = "256" ;
      ObSysVars[418].info_ = "This variable has no effect. Merely simulates MySQL 5.7." ;
      ObSysVars[418].name_ = "multi_range_count" ;
      ObSysVars[418].data_type_ = ObIntType ;
      ObSysVars[418].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[418].id_ = SYS_VAR_MULTI_RANGE_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MULTI_RANGE_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MULTI_RANGE_COUNT] = 418 ;
      ObSysVars[418].base_value_ = "256" ;
    ObSysVars[418].alias_ = "OB_SV_MULTI_RANGE_COUNT" ;
    }();

    [&] (){
      ObSysVars[419].default_value_ = "30" ;
      ObSysVars[419].info_ = "The number of seconds X Plugin waits for the first packet to be received from newly connected clients. Merely simulates MySQL 5.7." ;
      ObSysVars[419].name_ = "mysqlx_connect_timeout" ;
      ObSysVars[419].data_type_ = ObIntType ;
      ObSysVars[419].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[419].id_ = SYS_VAR_MYSQLX_CONNECT_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQLX_CONNECT_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQLX_CONNECT_TIMEOUT] = 419 ;
      ObSysVars[419].base_value_ = "30" ;
    ObSysVars[419].alias_ = "OB_SV_MYSQLX_CONNECT_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[420].default_value_ = "60" ;
      ObSysVars[420].info_ = "The number of seconds after which idle worker threads are terminated. Merely simulates MySQL 5.7." ;
      ObSysVars[420].name_ = "mysqlx_idle_worker_thread_timeout" ;
      ObSysVars[420].data_type_ = ObIntType ;
      ObSysVars[420].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[420].id_ = SYS_VAR_MYSQLX_IDLE_WORKER_THREAD_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQLX_IDLE_WORKER_THREAD_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQLX_IDLE_WORKER_THREAD_TIMEOUT] = 420 ;
      ObSysVars[420].base_value_ = "60" ;
    ObSysVars[420].alias_ = "OB_SV_MYSQLX_IDLE_WORKER_THREAD_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[421].default_value_ = "67108864" ;
      ObSysVars[421].info_ = "The maximum size of network packets that can be received by X Plugin. Merely simulates MySQL 5.7." ;
      ObSysVars[421].name_ = "mysqlx_max_allowed_packet" ;
      ObSysVars[421].data_type_ = ObIntType ;
      ObSysVars[421].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[421].id_ = SYS_VAR_MYSQLX_MAX_ALLOWED_PACKET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQLX_MAX_ALLOWED_PACKET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQLX_MAX_ALLOWED_PACKET] = 421 ;
      ObSysVars[421].base_value_ = "67108864" ;
    ObSysVars[421].alias_ = "OB_SV_MYSQLX_MAX_ALLOWED_PACKET" ;
    }();

    [&] (){
      ObSysVars[422].default_value_ = "100" ;
      ObSysVars[422].info_ = "The maximum number of concurrent client connections X Plugin can accept. Merely simulates MySQL 5.7." ;
      ObSysVars[422].name_ = "mysqlx_max_connections" ;
      ObSysVars[422].data_type_ = ObIntType ;
      ObSysVars[422].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[422].id_ = SYS_VAR_MYSQLX_MAX_CONNECTIONS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQLX_MAX_CONNECTIONS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQLX_MAX_CONNECTIONS] = 422 ;
      ObSysVars[422].base_value_ = "100" ;
    ObSysVars[422].alias_ = "OB_SV_MYSQLX_MAX_CONNECTIONS" ;
    }();

    [&] (){
      ObSysVars[423].default_value_ = "2" ;
      ObSysVars[423].info_ = "The minimum number of worker threads used by X Plugin for handling client requests. Merely simulates MySQL 5.7." ;
      ObSysVars[423].name_ = "mysqlx_min_worker_threads" ;
      ObSysVars[423].data_type_ = ObIntType ;
      ObSysVars[423].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[423].id_ = SYS_VAR_MYSQLX_MIN_WORKER_THREADS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQLX_MIN_WORKER_THREADS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQLX_MIN_WORKER_THREADS] = 423 ;
      ObSysVars[423].base_value_ = "2" ;
    ObSysVars[423].alias_ = "OB_SV_MYSQLX_MIN_WORKER_THREADS" ;
    }();

    [&] (){
      ObSysVars[424].default_value_ = "0" ;
      ObSysVars[424].info_ = "The variable determines which SHOW PROCESSLIST implementation to use. Merely simulates MySQL 5.7." ;
      ObSysVars[424].name_ = "performance_schema_show_processlist" ;
      ObSysVars[424].data_type_ = ObIntType ;
      ObSysVars[424].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[424].id_ = SYS_VAR_PERFORMANCE_SCHEMA_SHOW_PROCESSLIST ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PERFORMANCE_SCHEMA_SHOW_PROCESSLIST)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PERFORMANCE_SCHEMA_SHOW_PROCESSLIST] = 424 ;
      ObSysVars[424].base_value_ = "0" ;
    ObSysVars[424].alias_ = "OB_SV_PERFORMANCE_SCHEMA_SHOW_PROCESSLIST" ;
    }();

    [&] (){
      ObSysVars[425].default_value_ = "8192" ;
      ObSysVars[425].info_ = "The allocation size in bytes of memory blocks that are allocated for objects created during statement parsing and execution. Merely simulates MySQL 5.7." ;
      ObSysVars[425].name_ = "query_alloc_block_size" ;
      ObSysVars[425].data_type_ = ObIntType ;
      ObSysVars[425].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[425].id_ = SYS_VAR_QUERY_ALLOC_BLOCK_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_ALLOC_BLOCK_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_ALLOC_BLOCK_SIZE] = 425 ;
      ObSysVars[425].base_value_ = "8192" ;
    ObSysVars[425].alias_ = "OB_SV_QUERY_ALLOC_BLOCK_SIZE" ;
    }();

    [&] (){
      ObSysVars[426].default_value_ = "8192" ;
      ObSysVars[426].info_ = "The size in bytes of the persistent buffer used for statement parsing and execution. Merely simulates MySQL 5.7." ;
      ObSysVars[426].name_ = "query_prealloc_size" ;
      ObSysVars[426].data_type_ = ObIntType ;
      ObSysVars[426].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[426].id_ = SYS_VAR_QUERY_PREALLOC_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_PREALLOC_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_PREALLOC_SIZE] = 426 ;
      ObSysVars[426].base_value_ = "8192" ;
    ObSysVars[426].alias_ = "OB_SV_QUERY_PREALLOC_SIZE" ;
    }();

    [&] (){
      ObSysVars[427].default_value_ = "0" ;
      ObSysVars[427].info_ = "Whether the slow query log is enabled. Merely simulates MySQL 5.7." ;
      ObSysVars[427].name_ = "slow_query_log" ;
      ObSysVars[427].data_type_ = ObIntType ;
      ObSysVars[427].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[427].id_ = SYS_VAR_SLOW_QUERY_LOG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLOW_QUERY_LOG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLOW_QUERY_LOG] = 427 ;
      ObSysVars[427].base_value_ = "0" ;
    ObSysVars[427].alias_ = "OB_SV_SLOW_QUERY_LOG" ;
    }();

    [&] (){
      ObSysVars[428].default_value_ = "/usr/local/mysql/data/obrd-slow.log" ;
      ObSysVars[428].info_ = "The name of the slow query log file. Merely simulates MySQL 5.7." ;
      ObSysVars[428].name_ = "slow_query_log_file" ;
      ObSysVars[428].data_type_ = ObVarcharType ;
      ObSysVars[428].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[428].id_ = SYS_VAR_SLOW_QUERY_LOG_FILE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLOW_QUERY_LOG_FILE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLOW_QUERY_LOG_FILE] = 428 ;
      ObSysVars[428].base_value_ = "/usr/local/mysql/data/obrd-slow.log" ;
    ObSysVars[428].alias_ = "OB_SV_SLOW_QUERY_LOG_FILE" ;
    }();

    [&] (){
      ObSysVars[429].default_value_ = "262144" ;
      ObSysVars[429].info_ = "Each session that must perform a sort allocates a buffer of this size. Merely simulates MySQL 5.7." ;
      ObSysVars[429].name_ = "sort_buffer_size" ;
      ObSysVars[429].data_type_ = ObIntType ;
      ObSysVars[429].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[429].id_ = SYS_VAR_SORT_BUFFER_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SORT_BUFFER_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SORT_BUFFER_SIZE] = 429 ;
      ObSysVars[429].base_value_ = "262144" ;
    ObSysVars[429].alias_ = "OB_SV_SORT_BUFFER_SIZE" ;
    }();

    [&] (){
      ObSysVars[430].default_value_ = "0" ;
      ObSysVars[430].info_ = "If enabled, sql_buffer_result forces results from SELECT statements to be put into temporary tables. Merely simulates MySQL 5.7." ;
      ObSysVars[430].name_ = "sql_buffer_result" ;
      ObSysVars[430].data_type_ = ObIntType ;
      ObSysVars[430].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[430].id_ = SYS_VAR_SQL_BUFFER_RESULT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_BUFFER_RESULT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_BUFFER_RESULT] = 430 ;
      ObSysVars[430].base_value_ = "0" ;
    ObSysVars[430].alias_ = "OB_SV_SQL_BUFFER_RESULT" ;
    }();

    [&] (){
      ObSysVars[431].default_value_ = "32768" ;
      ObSysVars[431].info_ = "binlog_cache_size sets the size for the transaction cache only. Merely simulates MySQL 5.7." ;
      ObSysVars[431].name_ = "binlog_cache_size" ;
      ObSysVars[431].data_type_ = ObIntType ;
      ObSysVars[431].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[431].id_ = SYS_VAR_BINLOG_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_CACHE_SIZE] = 431 ;
      ObSysVars[431].base_value_ = "32768" ;
    ObSysVars[431].alias_ = "OB_SV_BINLOG_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[432].default_value_ = "0" ;
      ObSysVars[432].info_ = "Enabling binlog_direct_non_transactional_updates causes updates to nontransactional tables to be written directly to the binary log, rather than to the transaction cache. Merely simulates MySQL 5.7." ;
      ObSysVars[432].name_ = "binlog_direct_non_transactional_updates" ;
      ObSysVars[432].data_type_ = ObIntType ;
      ObSysVars[432].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[432].id_ = SYS_VAR_BINLOG_DIRECT_NON_TRANSACTIONAL_UPDATES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_DIRECT_NON_TRANSACTIONAL_UPDATES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_DIRECT_NON_TRANSACTIONAL_UPDATES] = 432 ;
      ObSysVars[432].base_value_ = "0" ;
    ObSysVars[432].alias_ = "OB_SV_BINLOG_DIRECT_NON_TRANSACTIONAL_UPDATES" ;
    }();

    [&] (){
      ObSysVars[433].default_value_ = "1" ;
      ObSysVars[433].info_ = "Controls what happens when the server encounters an error. Merely simulates MySQL 5.7." ;
      ObSysVars[433].name_ = "binlog_error_action" ;
      ObSysVars[433].data_type_ = ObIntType ;
      ObSysVars[433].enum_names_ = "[u'IGNORE_ERROR', u'ABORT_SERVER']" ;
      ObSysVars[433].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[433].id_ = SYS_VAR_BINLOG_ERROR_ACTION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_ERROR_ACTION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_ERROR_ACTION] = 433 ;
      ObSysVars[433].base_value_ = "1" ;
    ObSysVars[433].alias_ = "OB_SV_BINLOG_ERROR_ACTION" ;
    }();

    [&] (){
      ObSysVars[434].default_value_ = "0" ;
      ObSysVars[434].info_ = "Controls how many microseconds the binary log commit waits before synchronizing the binary log file to disk. Merely simulates MySQL 5.7." ;
      ObSysVars[434].name_ = "binlog_group_commit_sync_delay" ;
      ObSysVars[434].data_type_ = ObIntType ;
      ObSysVars[434].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[434].id_ = SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_DELAY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_DELAY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_DELAY] = 434 ;
      ObSysVars[434].base_value_ = "0" ;
    ObSysVars[434].alias_ = "OB_SV_BINLOG_GROUP_COMMIT_SYNC_DELAY" ;
    }();

    [&] (){
      ObSysVars[435].default_value_ = "0" ;
      ObSysVars[435].info_ = "The maximum number of transactions to wait for before aborting the current delay as specified by binlog_group_commit_sync_delay. Merely simulates MySQL 5.7." ;
      ObSysVars[435].name_ = "binlog_group_commit_sync_no_delay_count" ;
      ObSysVars[435].data_type_ = ObIntType ;
      ObSysVars[435].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[435].id_ = SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_NO_DELAY_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_NO_DELAY_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_NO_DELAY_COUNT] = 435 ;
      ObSysVars[435].base_value_ = "0" ;
    ObSysVars[435].alias_ = "OB_SV_BINLOG_GROUP_COMMIT_SYNC_NO_DELAY_COUNT" ;
    }();

    [&] (){
      ObSysVars[436].default_value_ = "0" ;
      ObSysVars[436].info_ = "This variable no longer has any effect. Merely simulates MySQL 5.7." ;
      ObSysVars[436].name_ = "binlog_max_flush_queue_time" ;
      ObSysVars[436].data_type_ = ObIntType ;
      ObSysVars[436].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[436].id_ = SYS_VAR_BINLOG_MAX_FLUSH_QUEUE_TIME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_MAX_FLUSH_QUEUE_TIME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_MAX_FLUSH_QUEUE_TIME] = 436 ;
      ObSysVars[436].base_value_ = "0" ;
    ObSysVars[436].alias_ = "OB_SV_BINLOG_MAX_FLUSH_QUEUE_TIME" ;
    }();

    [&] (){
      ObSysVars[437].default_value_ = "1" ;
      ObSysVars[437].info_ = "When this variable is enabled on a replication source server, transaction commit instructions issued to storage engines are serialized on a single thread. Merely simulates MySQL 5.7." ;
      ObSysVars[437].name_ = "binlog_order_commits" ;
      ObSysVars[437].data_type_ = ObIntType ;
      ObSysVars[437].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[437].id_ = SYS_VAR_BINLOG_ORDER_COMMITS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_ORDER_COMMITS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_ORDER_COMMITS] = 437 ;
      ObSysVars[437].base_value_ = "1" ;
    ObSysVars[437].alias_ = "OB_SV_BINLOG_ORDER_COMMITS" ;
    }();

    [&] (){
      ObSysVars[438].default_value_ = "32768" ;
      ObSysVars[438].info_ = "This variable determines the size of the cache for the binary log to hold nontransactional statements issued during a transaction. Merely simulates MySQL 5.7." ;
      ObSysVars[438].name_ = "binlog_stmt_cache_size" ;
      ObSysVars[438].data_type_ = ObIntType ;
      ObSysVars[438].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[438].id_ = SYS_VAR_BINLOG_STMT_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_STMT_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_STMT_CACHE_SIZE] = 438 ;
      ObSysVars[438].base_value_ = "32768" ;
    ObSysVars[438].alias_ = "OB_SV_BINLOG_STMT_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[439].default_value_ = "25000" ;
      ObSysVars[439].info_ = "Sets an upper limit on the number of row hashes which are kept in memory and used for looking up the transaction that last modified a given row. Merely simulates MySQL 5.7." ;
      ObSysVars[439].name_ = "binlog_transaction_dependency_history_size" ;
      ObSysVars[439].data_type_ = ObIntType ;
      ObSysVars[439].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[439].id_ = SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE] = 439 ;
      ObSysVars[439].base_value_ = "25000" ;
    ObSysVars[439].alias_ = "OB_SV_BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE" ;
    }();

    [&] (){
      ObSysVars[440].default_value_ = "0" ;
      ObSysVars[440].info_ = "The source of dependency information that the source uses to determine which transactions can be executed in parallel by the replica's multithreaded applier. Merely simulates MySQL 5.7." ;
      ObSysVars[440].name_ = "binlog_transaction_dependency_tracking" ;
      ObSysVars[440].data_type_ = ObIntType ;
      ObSysVars[440].enum_names_ = "[u'COMMIT_ORDER', u'WRITESET', u'WRITESET_SESSION']" ;
      ObSysVars[440].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[440].id_ = SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_TRACKING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_TRACKING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_TRACKING] = 440 ;
      ObSysVars[440].base_value_ = "0" ;
    ObSysVars[440].alias_ = "OB_SV_BINLOG_TRANSACTION_DEPENDENCY_TRACKING" ;
    }();

    [&] (){
      ObSysVars[441].default_value_ = "0" ;
      ObSysVars[441].info_ = "The number of days for automatic binary log file removal. Merely simulates MySQL 5.7." ;
      ObSysVars[441].name_ = "expire_logs_days" ;
      ObSysVars[441].data_type_ = ObIntType ;
      ObSysVars[441].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[441].id_ = SYS_VAR_EXPIRE_LOGS_DAYS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_EXPIRE_LOGS_DAYS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_EXPIRE_LOGS_DAYS] = 441 ;
      ObSysVars[441].base_value_ = "0" ;
    ObSysVars[441].alias_ = "OB_SV_EXPIRE_LOGS_DAYS" ;
    }();

    [&] (){
      ObSysVars[442].default_value_ = "1" ;
      ObSysVars[442].info_ = "Write and flush the logs every N seconds. Merely simulates MySQL 5.7." ;
      ObSysVars[442].name_ = "innodb_flush_log_at_timeout" ;
      ObSysVars[442].data_type_ = ObIntType ;
      ObSysVars[442].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[442].id_ = SYS_VAR_INNODB_FLUSH_LOG_AT_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FLUSH_LOG_AT_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FLUSH_LOG_AT_TIMEOUT] = 442 ;
      ObSysVars[442].base_value_ = "1" ;
    ObSysVars[442].alias_ = "OB_SV_INNODB_FLUSH_LOG_AT_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[443].default_value_ = "1" ;
      ObSysVars[443].info_ = "Controls the balance between strict ACID compliance for commit operations and higher performance. Merely simulates MySQL 5.7." ;
      ObSysVars[443].name_ = "innodb_flush_log_at_trx_commit" ;
      ObSysVars[443].data_type_ = ObIntType ;
      ObSysVars[443].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[443].id_ = SYS_VAR_INNODB_FLUSH_LOG_AT_TRX_COMMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FLUSH_LOG_AT_TRX_COMMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FLUSH_LOG_AT_TRX_COMMIT] = 443 ;
      ObSysVars[443].base_value_ = "1" ;
    ObSysVars[443].alias_ = "OB_SV_INNODB_FLUSH_LOG_AT_TRX_COMMIT" ;
    }();

    [&] (){
      ObSysVars[444].default_value_ = "0" ;
      ObSysVars[444].info_ = "Enable this debug option to force InnoDB to write a checkpoint. Merely simulates MySQL 5.7." ;
      ObSysVars[444].name_ = "innodb_log_checkpoint_now" ;
      ObSysVars[444].data_type_ = ObIntType ;
      ObSysVars[444].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[444].id_ = SYS_VAR_INNODB_LOG_CHECKPOINT_NOW ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LOG_CHECKPOINT_NOW)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LOG_CHECKPOINT_NOW] = 444 ;
      ObSysVars[444].base_value_ = "0" ;
    ObSysVars[444].alias_ = "OB_SV_INNODB_LOG_CHECKPOINT_NOW" ;
    }();

    [&] (){
      ObSysVars[445].default_value_ = "1" ;
      ObSysVars[445].info_ = "Enables or disables checksums for redo log pages. Merely simulates MySQL 5.7." ;
      ObSysVars[445].name_ = "innodb_log_checksums" ;
      ObSysVars[445].data_type_ = ObIntType ;
      ObSysVars[445].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[445].id_ = SYS_VAR_INNODB_LOG_CHECKSUMS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LOG_CHECKSUMS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LOG_CHECKSUMS] = 445 ;
      ObSysVars[445].base_value_ = "1" ;
    ObSysVars[445].alias_ = "OB_SV_INNODB_LOG_CHECKSUMS" ;
    }();

    [&] (){
      ObSysVars[446].default_value_ = "1" ;
      ObSysVars[446].info_ = "Specifies whether images of re-compressed pages are written to the redo log. Merely simulates MySQL 5.7." ;
      ObSysVars[446].name_ = "innodb_log_compressed_pages" ;
      ObSysVars[446].data_type_ = ObIntType ;
      ObSysVars[446].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[446].id_ = SYS_VAR_INNODB_LOG_COMPRESSED_PAGES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LOG_COMPRESSED_PAGES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LOG_COMPRESSED_PAGES] = 446 ;
      ObSysVars[446].base_value_ = "1" ;
    ObSysVars[446].alias_ = "OB_SV_INNODB_LOG_COMPRESSED_PAGES" ;
    }();

    [&] (){
      ObSysVars[447].default_value_ = "8192" ;
      ObSysVars[447].info_ = "Defines the write-ahead block size for the redo log, in bytes. Merely simulates MySQL 5.7." ;
      ObSysVars[447].name_ = "innodb_log_write_ahead_size" ;
      ObSysVars[447].data_type_ = ObIntType ;
      ObSysVars[447].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[447].id_ = SYS_VAR_INNODB_LOG_WRITE_AHEAD_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LOG_WRITE_AHEAD_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LOG_WRITE_AHEAD_SIZE] = 447 ;
      ObSysVars[447].base_value_ = "8192" ;
    ObSysVars[447].alias_ = "OB_SV_INNODB_LOG_WRITE_AHEAD_SIZE" ;
    }();

    [&] (){
      ObSysVars[448].default_value_ = "1073741824" ;
      ObSysVars[448].info_ = "Defines a threshold size for undo tablespaces. Merely simulates MySQL 5.7." ;
      ObSysVars[448].name_ = "innodb_max_undo_log_size" ;
      ObSysVars[448].data_type_ = ObIntType ;
      ObSysVars[448].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[448].id_ = SYS_VAR_INNODB_MAX_UNDO_LOG_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_MAX_UNDO_LOG_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_MAX_UNDO_LOG_SIZE] = 448 ;
      ObSysVars[448].base_value_ = "1073741824" ;
    ObSysVars[448].alias_ = "OB_SV_INNODB_MAX_UNDO_LOG_SIZE" ;
    }();

    [&] (){
      ObSysVars[449].default_value_ = "134217728" ;
      ObSysVars[449].info_ = "Specifies an upper limit in bytes on the size of the temporary log files used during online DDL operations for InnoDB tables. Merely simulates MySQL 5.7." ;
      ObSysVars[449].name_ = "innodb_online_alter_log_max_size" ;
      ObSysVars[449].data_type_ = ObIntType ;
      ObSysVars[449].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[449].id_ = SYS_VAR_INNODB_ONLINE_ALTER_LOG_MAX_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_ONLINE_ALTER_LOG_MAX_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_ONLINE_ALTER_LOG_MAX_SIZE] = 449 ;
      ObSysVars[449].base_value_ = "134217728" ;
    ObSysVars[449].alias_ = "OB_SV_INNODB_ONLINE_ALTER_LOG_MAX_SIZE" ;
    }();

    [&] (){
      ObSysVars[450].default_value_ = "0" ;
      ObSysVars[450].info_ = "When enabled, undo tablespaces that exceed the threshold value defined by innodb_max_undo_log_size are marked for truncation. Merely simulates MySQL 5.7." ;
      ObSysVars[450].name_ = "innodb_undo_log_truncate" ;
      ObSysVars[450].data_type_ = ObIntType ;
      ObSysVars[450].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[450].id_ = SYS_VAR_INNODB_UNDO_LOG_TRUNCATE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_UNDO_LOG_TRUNCATE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_UNDO_LOG_TRUNCATE] = 450 ;
      ObSysVars[450].base_value_ = "0" ;
    ObSysVars[450].alias_ = "OB_SV_INNODB_UNDO_LOG_TRUNCATE" ;
    }();

    [&] (){
      ObSysVars[451].default_value_ = "128" ;
      ObSysVars[451].info_ = "Defines the number of rollback segments used by InnoDB. Merely simulates MySQL 5.7." ;
      ObSysVars[451].name_ = "innodb_undo_logs" ;
      ObSysVars[451].data_type_ = ObIntType ;
      ObSysVars[451].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[451].id_ = SYS_VAR_INNODB_UNDO_LOGS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_UNDO_LOGS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_UNDO_LOGS] = 451 ;
      ObSysVars[451].base_value_ = "128" ;
    ObSysVars[451].alias_ = "OB_SV_INNODB_UNDO_LOGS" ;
    }();

    [&] (){
      ObSysVars[452].default_value_ = "0" ;
      ObSysVars[452].info_ = "It controls whether stored function creators can be trusted not to create stored functions that causes unsafe events to be written to the binary log. Merely simulates MySQL 5.7." ;
      ObSysVars[452].name_ = "log_bin_trust_function_creators" ;
      ObSysVars[452].data_type_ = ObIntType ;
      ObSysVars[452].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[452].id_ = SYS_VAR_LOG_BIN_TRUST_FUNCTION_CREATORS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOG_BIN_TRUST_FUNCTION_CREATORS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOG_BIN_TRUST_FUNCTION_CREATORS] = 452 ;
      ObSysVars[452].base_value_ = "0" ;
    ObSysVars[452].alias_ = "OB_SV_LOG_BIN_TRUST_FUNCTION_CREATORS" ;
    }();

    [&] (){
      ObSysVars[453].default_value_ = "0" ;
      ObSysVars[453].info_ = "Whether Version 2 binary logging is in use. Merely simulates MySQL 5.7." ;
      ObSysVars[453].name_ = "log_bin_use_v1_row_events" ;
      ObSysVars[453].data_type_ = ObIntType ;
      ObSysVars[453].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[453].id_ = SYS_VAR_LOG_BIN_USE_V1_ROW_EVENTS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOG_BIN_USE_V1_ROW_EVENTS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOG_BIN_USE_V1_ROW_EVENTS] = 453 ;
      ObSysVars[453].base_value_ = "0" ;
    ObSysVars[453].alias_ = "OB_SV_LOG_BIN_USE_V1_ROW_EVENTS" ;
    }();

    [&] (){
      ObSysVars[454].default_value_ = "0" ;
      ObSysVars[454].info_ = "This variable affects binary logging of user-management statements. Merely simulates MySQL 5.7." ;
      ObSysVars[454].name_ = "log_builtin_as_identified_by_password" ;
      ObSysVars[454].data_type_ = ObIntType ;
      ObSysVars[454].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[454].id_ = SYS_VAR_LOG_BUILTIN_AS_IDENTIFIED_BY_PASSWORD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOG_BUILTIN_AS_IDENTIFIED_BY_PASSWORD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOG_BUILTIN_AS_IDENTIFIED_BY_PASSWORD] = 454 ;
      ObSysVars[454].base_value_ = "0" ;
    ObSysVars[454].alias_ = "OB_SV_LOG_BUILTIN_AS_IDENTIFIED_BY_PASSWORD" ;
    }();

    [&] (){
      ObSysVars[455].default_value_ = "18446744073709500416" ;
      ObSysVars[455].info_ = "If a transaction requires more than this many bytes, the server generates a Multi-statement transaction required more than 'max_binlog_cache_size' bytes of storage error. Merely simulates MySQL 5.7." ;
      ObSysVars[455].name_ = "max_binlog_cache_size" ;
      ObSysVars[455].data_type_ = ObUInt64Type ;
      ObSysVars[455].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[455].id_ = SYS_VAR_MAX_BINLOG_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_BINLOG_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_BINLOG_CACHE_SIZE] = 455 ;
      ObSysVars[455].base_value_ = "18446744073709500416" ;
    ObSysVars[455].alias_ = "OB_SV_MAX_BINLOG_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[456].default_value_ = "1073741824" ;
      ObSysVars[456].info_ = "If a write to the binary log causes the current log file size to exceed the value of this variable, the server rotates the binary logs. Merely simulates MySQL 5.7." ;
      ObSysVars[456].name_ = "max_binlog_size" ;
      ObSysVars[456].data_type_ = ObIntType ;
      ObSysVars[456].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[456].id_ = SYS_VAR_MAX_BINLOG_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_BINLOG_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_BINLOG_SIZE] = 456 ;
      ObSysVars[456].base_value_ = "1073741824" ;
    ObSysVars[456].alias_ = "OB_SV_MAX_BINLOG_SIZE" ;
    }();

    [&] (){
      ObSysVars[457].default_value_ = "18446744073709500416" ;
      ObSysVars[457].info_ = "If nontransactional statements within a transaction require more than this many bytes of memory, the server generates an error. Merely simulates MySQL 5.7." ;
      ObSysVars[457].name_ = "max_binlog_stmt_cache_size" ;
      ObSysVars[457].data_type_ = ObUInt64Type ;
      ObSysVars[457].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[457].id_ = SYS_VAR_MAX_BINLOG_STMT_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_BINLOG_STMT_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_BINLOG_STMT_CACHE_SIZE] = 457 ;
      ObSysVars[457].base_value_ = "18446744073709500416" ;
    ObSysVars[457].alias_ = "OB_SV_MAX_BINLOG_STMT_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[458].default_value_ = "0" ;
      ObSysVars[458].info_ = "The size at which the server rotates relay log files automatically. Merely simulates MySQL 5.7." ;
      ObSysVars[458].name_ = "max_relay_log_size" ;
      ObSysVars[458].data_type_ = ObIntType ;
      ObSysVars[458].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[458].id_ = SYS_VAR_MAX_RELAY_LOG_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_RELAY_LOG_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_RELAY_LOG_SIZE] = 458 ;
      ObSysVars[458].base_value_ = "0" ;
    ObSysVars[458].alias_ = "OB_SV_MAX_RELAY_LOG_SIZE" ;
    }();

    [&] (){
      ObSysVars[459].default_value_ = "FILE" ;
      ObSysVars[459].info_ = "The setting of this variable determines whether the replica server stores its applier metadata repository as an InnoDB table in the mysql system database, or as a file in the data directory. Merely simulates MySQL 5.7." ;
      ObSysVars[459].name_ = "relay_log_info_repository" ;
      ObSysVars[459].data_type_ = ObVarcharType ;
      ObSysVars[459].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[459].id_ = SYS_VAR_RELAY_LOG_INFO_REPOSITORY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RELAY_LOG_INFO_REPOSITORY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RELAY_LOG_INFO_REPOSITORY] = 459 ;
      ObSysVars[459].base_value_ = "FILE" ;
    ObSysVars[459].alias_ = "OB_SV_RELAY_LOG_INFO_REPOSITORY" ;
    }();

    [&] (){
      ObSysVars[460].default_value_ = "1" ;
      ObSysVars[460].info_ = "Disables or enables automatic purging of relay log files as soon as they are not needed any more. Merely simulates MySQL 5.7." ;
      ObSysVars[460].name_ = "relay_log_purge" ;
      ObSysVars[460].data_type_ = ObIntType ;
      ObSysVars[460].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[460].id_ = SYS_VAR_RELAY_LOG_PURGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RELAY_LOG_PURGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RELAY_LOG_PURGE] = 460 ;
      ObSysVars[460].base_value_ = "1" ;
    ObSysVars[460].alias_ = "OB_SV_RELAY_LOG_PURGE" ;
    }();

    [&] (){
      ObSysVars[461].default_value_ = "1" ;
      ObSysVars[461].info_ = "Controls how often the MySQL server synchronizes the binary log to disk. Merely simulates MySQL 5.7." ;
      ObSysVars[461].name_ = "sync_binlog" ;
      ObSysVars[461].data_type_ = ObIntType ;
      ObSysVars[461].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[461].id_ = SYS_VAR_SYNC_BINLOG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SYNC_BINLOG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SYNC_BINLOG] = 461 ;
      ObSysVars[461].base_value_ = "1" ;
    ObSysVars[461].alias_ = "OB_SV_SYNC_BINLOG" ;
    }();

    [&] (){
      ObSysVars[462].default_value_ = "10000" ;
      ObSysVars[462].info_ = "If the value of this variable is greater than 0, the MySQL server synchronizes its relay log to disk after every sync_relay_log events are written to the relay log. Merely simulates MySQL 5.7." ;
      ObSysVars[462].name_ = "sync_relay_log" ;
      ObSysVars[462].data_type_ = ObIntType ;
      ObSysVars[462].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[462].id_ = SYS_VAR_SYNC_RELAY_LOG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SYNC_RELAY_LOG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SYNC_RELAY_LOG] = 462 ;
      ObSysVars[462].base_value_ = "10000" ;
    ObSysVars[462].alias_ = "OB_SV_SYNC_RELAY_LOG" ;
    }();

    [&] (){
      ObSysVars[463].default_value_ = "10000" ;
      ObSysVars[463].info_ = "Setting this variable takes effect for all replication channels immediately, including running channels. Merely simulates MySQL 5.7." ;
      ObSysVars[463].name_ = "sync_relay_log_info" ;
      ObSysVars[463].data_type_ = ObIntType ;
      ObSysVars[463].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[463].id_ = SYS_VAR_SYNC_RELAY_LOG_INFO ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SYNC_RELAY_LOG_INFO)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SYNC_RELAY_LOG_INFO] = 463 ;
      ObSysVars[463].base_value_ = "10000" ;
    ObSysVars[463].alias_ = "OB_SV_SYNC_RELAY_LOG_INFO" ;
    }();

    [&] (){
      ObSysVars[464].default_value_ = "1" ;
      ObSysVars[464].info_ = "This option is used to disable deadlock detection. Merely simulates MySQL 5.7." ;
      ObSysVars[464].name_ = "innodb_deadlock_detect" ;
      ObSysVars[464].data_type_ = ObIntType ;
      ObSysVars[464].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[464].id_ = SYS_VAR_INNODB_DEADLOCK_DETECT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_DEADLOCK_DETECT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_DEADLOCK_DETECT] = 464 ;
      ObSysVars[464].base_value_ = "1" ;
    ObSysVars[464].alias_ = "OB_SV_INNODB_DEADLOCK_DETECT" ;
    }();

    [&] (){
      ObSysVars[465].default_value_ = "50" ;
      ObSysVars[465].info_ = "The length of time in seconds an InnoDB transaction waits for a row lock before giving up. Merely simulates MySQL 5.7." ;
      ObSysVars[465].name_ = "innodb_lock_wait_timeout" ;
      ObSysVars[465].data_type_ = ObIntType ;
      ObSysVars[465].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[465].id_ = SYS_VAR_INNODB_LOCK_WAIT_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LOCK_WAIT_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LOCK_WAIT_TIMEOUT] = 465 ;
      ObSysVars[465].base_value_ = "50" ;
    ObSysVars[465].alias_ = "OB_SV_INNODB_LOCK_WAIT_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[466].default_value_ = "0" ;
      ObSysVars[466].info_ = "When this option is enabled, information about all deadlocks in InnoDB user transactions is recorded in the mysqld error log. Merely simulates MySQL 5.7." ;
      ObSysVars[466].name_ = "innodb_print_all_deadlocks" ;
      ObSysVars[466].data_type_ = ObIntType ;
      ObSysVars[466].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[466].id_ = SYS_VAR_INNODB_PRINT_ALL_DEADLOCKS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_PRINT_ALL_DEADLOCKS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_PRINT_ALL_DEADLOCKS] = 466 ;
      ObSysVars[466].base_value_ = "0" ;
    ObSysVars[466].alias_ = "OB_SV_INNODB_PRINT_ALL_DEADLOCKS" ;
    }();

    [&] (){
      ObSysVars[467].default_value_ = "1" ;
      ObSysVars[467].info_ = "The default value is 1, which means that LOCK TABLES causes InnoDB to lock a table internally if autocommit = 0. Merely simulates MySQL 5.7." ;
      ObSysVars[467].name_ = "innodb_table_locks" ;
      ObSysVars[467].data_type_ = ObIntType ;
      ObSysVars[467].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[467].id_ = SYS_VAR_INNODB_TABLE_LOCKS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_TABLE_LOCKS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_TABLE_LOCKS] = 467 ;
      ObSysVars[467].base_value_ = "1" ;
    ObSysVars[467].alias_ = "OB_SV_INNODB_TABLE_LOCKS" ;
    }();

    [&] (){
      ObSysVars[468].default_value_ = "18446744073709500416" ;
      ObSysVars[468].info_ = "if max_write_lock_count is set to some low value (say, 10), read lock requests may be preferred over pending write lock requests if the read lock requests have already been passed over in favor of 10 write lock requests. Merely simulates MySQL 5.7." ;
      ObSysVars[468].name_ = "max_write_lock_count" ;
      ObSysVars[468].data_type_ = ObUInt64Type ;
      ObSysVars[468].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[468].id_ = SYS_VAR_MAX_WRITE_LOCK_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_WRITE_LOCK_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_WRITE_LOCK_COUNT] = 468 ;
      ObSysVars[468].base_value_ = "18446744073709500416" ;
    ObSysVars[468].alias_ = "OB_SV_MAX_WRITE_LOCK_COUNT" ;
    }();

    [&] (){
      ObSysVars[469].default_value_ = "" ;
      ObSysVars[469].info_ = "enabled roles for current session" ;
      ObSysVars[469].name_ = "_ob_enable_role_ids" ;
      ObSysVars[469].data_type_ = ObVarcharType ;
      ObSysVars[469].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE ;
      ObSysVars[469].id_ = SYS_VAR__OB_ENABLE_ROLE_IDS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_ENABLE_ROLE_IDS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR__OB_ENABLE_ROLE_IDS] = 469 ;
      ObSysVars[469].base_value_ = "" ;
    ObSysVars[469].alias_ = "OB_SV__OB_ENABLE_ROLE_IDS" ;
    }();

    [&] (){
      ObSysVars[470].default_value_ = "0" ;
      ObSysVars[470].info_ = "Starts InnoDB in read-only mode. For distributing database applications or data sets on read-only media. Can also be used in data warehouses to share the same data directory between multiple instances." ;
      ObSysVars[470].name_ = "innodb_read_only" ;
      ObSysVars[470].data_type_ = ObIntType ;
      ObSysVars[470].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[470].id_ = SYS_VAR_INNODB_READ_ONLY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_READ_ONLY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_READ_ONLY] = 470 ;
      ObSysVars[470].base_value_ = "0" ;
    ObSysVars[470].alias_ = "OB_SV_INNODB_READ_ONLY" ;
    }();

    [&] (){
      ObSysVars[471].default_value_ = "0" ;
      ObSysVars[471].info_ = "Use this option to disable row locks when InnoDB memcached performs DML operations. By default, innodb_api_disable_rowlock is disabled, which means that memcached requests row locks for get and set operations." ;
      ObSysVars[471].name_ = "innodb_api_disable_rowlock" ;
      ObSysVars[471].data_type_ = ObIntType ;
      ObSysVars[471].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[471].id_ = SYS_VAR_INNODB_API_DISABLE_ROWLOCK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_API_DISABLE_ROWLOCK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_API_DISABLE_ROWLOCK] = 471 ;
      ObSysVars[471].base_value_ = "0" ;
    ObSysVars[471].alias_ = "OB_SV_INNODB_API_DISABLE_ROWLOCK" ;
    }();

    [&] (){
      ObSysVars[472].default_value_ = "1" ;
      ObSysVars[472].info_ = "The lock mode to use for generating auto-increment values. Permissible values are 0, 1, or 2, for traditional, consecutive, or interleaved, respectively. The default setting is 1 (consecutive)." ;
      ObSysVars[472].name_ = "innodb_autoinc_lock_mode" ;
      ObSysVars[472].data_type_ = ObIntType ;
      ObSysVars[472].min_val_ = "0" ;
      ObSysVars[472].max_val_ = "2" ;
      ObSysVars[472].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[472].id_ = SYS_VAR_INNODB_AUTOINC_LOCK_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_AUTOINC_LOCK_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_AUTOINC_LOCK_MODE] = 472 ;
      ObSysVars[472].base_value_ = "1" ;
    ObSysVars[472].alias_ = "OB_SV_INNODB_AUTOINC_LOCK_MODE" ;
    }();

    [&] (){
      ObSysVars[473].default_value_ = "1" ;
      ObSysVars[473].info_ = "This is OFF if mysqld uses external locking (system locking), ON if external locking is disabled." ;
      ObSysVars[473].name_ = "skip_external_locking" ;
      ObSysVars[473].data_type_ = ObIntType ;
      ObSysVars[473].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[473].id_ = SYS_VAR_SKIP_EXTERNAL_LOCKING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SKIP_EXTERNAL_LOCKING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SKIP_EXTERNAL_LOCKING] = 473 ;
      ObSysVars[473].base_value_ = "1" ;
    ObSysVars[473].alias_ = "OB_SV_SKIP_EXTERNAL_LOCKING" ;
    }();

    [&] (){
      ObSysVars[474].default_value_ = "0" ;
      ObSysVars[474].info_ = "If the read_only system variable is enabled, the server permits no client updates except from users who have the SUPER privilege. If the super_read_only system variable is also enabled, the server prohibits client updates even from users who have SUPER." ;
      ObSysVars[474].name_ = "super_read_only" ;
      ObSysVars[474].data_type_ = ObIntType ;
      ObSysVars[474].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[474].id_ = SYS_VAR_SUPER_READ_ONLY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SUPER_READ_ONLY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SUPER_READ_ONLY] = 474 ;
      ObSysVars[474].base_value_ = "0" ;
    ObSysVars[474].alias_ = "OB_SV_SUPER_READ_ONLY" ;
    }();

    [&] (){
      ObSysVars[475].default_value_ = "2" ;
      ObSysVars[475].info_ = "PLSQL_OPTIMIZE_LEVEL specifies the optimization level that will be used to compile PL/SQL library units. The higher the setting of this parameter, the more effort the compiler makes to optimize PL/SQL library units." ;
      ObSysVars[475].name_ = "plsql_optimize_level" ;
      ObSysVars[475].data_type_ = ObIntType ;
      ObSysVars[475].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[475].id_ = SYS_VAR_PLSQL_OPTIMIZE_LEVEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLSQL_OPTIMIZE_LEVEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PLSQL_OPTIMIZE_LEVEL] = 475 ;
      ObSysVars[475].base_value_ = "2" ;
    ObSysVars[475].alias_ = "OB_SV_PLSQL_OPTIMIZE_LEVEL" ;
    }();

    [&] (){
      ObSysVars[476].default_value_ = "built-in" ;
      ObSysVars[476].info_ = "mock for mysql5.7" ;
      ObSysVars[476].name_ = "ft_stopword_file" ;
      ObSysVars[476].data_type_ = ObVarcharType ;
      ObSysVars[476].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[476].id_ = SYS_VAR_FT_STOPWORD_FILE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_FT_STOPWORD_FILE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_FT_STOPWORD_FILE] = 476 ;
      ObSysVars[476].base_value_ = "built-in" ;
    ObSysVars[476].alias_ = "OB_SV_FT_STOPWORD_FILE" ;
    }();

    [&] (){
      ObSysVars[477].default_value_ = "8000000" ;
      ObSysVars[477].info_ = "mock for mysql5.7" ;
      ObSysVars[477].name_ = "innodb_ft_cache_size" ;
      ObSysVars[477].data_type_ = ObIntType ;
      ObSysVars[477].min_val_ = "1600000" ;
      ObSysVars[477].max_val_ = "80000000" ;
      ObSysVars[477].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[477].id_ = SYS_VAR_INNODB_FT_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FT_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FT_CACHE_SIZE] = 477 ;
      ObSysVars[477].base_value_ = "8000000" ;
    ObSysVars[477].alias_ = "OB_SV_INNODB_FT_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[478].default_value_ = "2" ;
      ObSysVars[478].info_ = "mock for mysql5.7" ;
      ObSysVars[478].name_ = "innodb_ft_sort_pll_degree" ;
      ObSysVars[478].data_type_ = ObIntType ;
      ObSysVars[478].min_val_ = "1" ;
      ObSysVars[478].max_val_ = "16" ;
      ObSysVars[478].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[478].id_ = SYS_VAR_INNODB_FT_SORT_PLL_DEGREE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FT_SORT_PLL_DEGREE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FT_SORT_PLL_DEGREE] = 478 ;
      ObSysVars[478].base_value_ = "2" ;
    ObSysVars[478].alias_ = "OB_SV_INNODB_FT_SORT_PLL_DEGREE" ;
    }();

    [&] (){
      ObSysVars[479].default_value_ = "640000000" ;
      ObSysVars[479].info_ = "mock for mysql5.7" ;
      ObSysVars[479].name_ = "innodb_ft_total_cache_size" ;
      ObSysVars[479].data_type_ = ObIntType ;
      ObSysVars[479].min_val_ = "32000000" ;
      ObSysVars[479].max_val_ = "1600000000" ;
      ObSysVars[479].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[479].id_ = SYS_VAR_INNODB_FT_TOTAL_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FT_TOTAL_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FT_TOTAL_CACHE_SIZE] = 479 ;
      ObSysVars[479].base_value_ = "640000000" ;
    ObSysVars[479].alias_ = "OB_SV_INNODB_FT_TOTAL_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[480].default_value_ = "" ;
      ObSysVars[480].info_ = "mock for mysql5.7" ;
      ObSysVars[480].name_ = "mecab_rc_file" ;
      ObSysVars[480].data_type_ = ObVarcharType ;
      ObSysVars[480].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[480].id_ = SYS_VAR_MECAB_RC_FILE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MECAB_RC_FILE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MECAB_RC_FILE] = 480 ;
      ObSysVars[480].base_value_ = "" ;
    ObSysVars[480].alias_ = "OB_SV_MECAB_RC_FILE" ;
    }();

    [&] (){
      ObSysVars[481].default_value_ = "1024" ;
      ObSysVars[481].info_ = "mock for mysql5.7" ;
      ObSysVars[481].name_ = "metadata_locks_cache_size" ;
      ObSysVars[481].data_type_ = ObIntType ;
      ObSysVars[481].min_val_ = "1" ;
      ObSysVars[481].max_val_ = "1048576" ;
      ObSysVars[481].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[481].id_ = SYS_VAR_METADATA_LOCKS_CACHE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_METADATA_LOCKS_CACHE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_METADATA_LOCKS_CACHE_SIZE] = 481 ;
      ObSysVars[481].base_value_ = "1024" ;
    ObSysVars[481].alias_ = "OB_SV_METADATA_LOCKS_CACHE_SIZE" ;
    }();

    [&] (){
      ObSysVars[482].default_value_ = "8" ;
      ObSysVars[482].info_ = "mock for mysql5.7" ;
      ObSysVars[482].name_ = "metadata_locks_hash_instances" ;
      ObSysVars[482].data_type_ = ObIntType ;
      ObSysVars[482].min_val_ = "1" ;
      ObSysVars[482].max_val_ = "1024" ;
      ObSysVars[482].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[482].id_ = SYS_VAR_METADATA_LOCKS_HASH_INSTANCES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_METADATA_LOCKS_HASH_INSTANCES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_METADATA_LOCKS_HASH_INSTANCES] = 482 ;
      ObSysVars[482].base_value_ = "8" ;
    ObSysVars[482].alias_ = "OB_SV_METADATA_LOCKS_HASH_INSTANCES" ;
    }();

    [&] (){
      ObSysVars[483].default_value_ = "ibtmp1:12M:autoextend" ;
      ObSysVars[483].info_ = "mock for mysql5.7" ;
      ObSysVars[483].name_ = "innodb_temp_data_file_path" ;
      ObSysVars[483].data_type_ = ObVarcharType ;
      ObSysVars[483].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[483].id_ = SYS_VAR_INNODB_TEMP_DATA_FILE_PATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_TEMP_DATA_FILE_PATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_TEMP_DATA_FILE_PATH] = 483 ;
      ObSysVars[483].base_value_ = "ibtmp1:12M:autoextend" ;
    ObSysVars[483].alias_ = "OB_SV_INNODB_TEMP_DATA_FILE_PATH" ;
    }();

    [&] (){
      ObSysVars[484].default_value_ = "ibdata1:12M:autoextend" ;
      ObSysVars[484].info_ = "mock for mysql5.7" ;
      ObSysVars[484].name_ = "innodb_data_file_path" ;
      ObSysVars[484].data_type_ = ObVarcharType ;
      ObSysVars[484].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[484].id_ = SYS_VAR_INNODB_DATA_FILE_PATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_DATA_FILE_PATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_DATA_FILE_PATH] = 484 ;
      ObSysVars[484].base_value_ = "ibdata1:12M:autoextend" ;
    ObSysVars[484].alias_ = "OB_SV_INNODB_DATA_FILE_PATH" ;
    }();

    [&] (){
      ObSysVars[485].default_value_ = "" ;
      ObSysVars[485].info_ = "mock for mysql5.7" ;
      ObSysVars[485].name_ = "innodb_data_home_dir" ;
      ObSysVars[485].data_type_ = ObVarcharType ;
      ObSysVars[485].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[485].id_ = SYS_VAR_INNODB_DATA_HOME_DIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_DATA_HOME_DIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_DATA_HOME_DIR] = 485 ;
      ObSysVars[485].base_value_ = "" ;
    ObSysVars[485].alias_ = "OB_SV_INNODB_DATA_HOME_DIR" ;
    }();

    [&] (){
      ObSysVars[486].default_value_ = "0" ;
      ObSysVars[486].info_ = "mock for mysql5.7" ;
      ObSysVars[486].name_ = "avoid_temporal_upgrade" ;
      ObSysVars[486].data_type_ = ObIntType ;
      ObSysVars[486].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[486].id_ = SYS_VAR_AVOID_TEMPORAL_UPGRADE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AVOID_TEMPORAL_UPGRADE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_AVOID_TEMPORAL_UPGRADE] = 486 ;
      ObSysVars[486].base_value_ = "0" ;
    ObSysVars[486].alias_ = "OB_SV_AVOID_TEMPORAL_UPGRADE" ;
    }();

    [&] (){
      ObSysVars[487].default_value_ = "0" ;
      ObSysVars[487].info_ = "mock for mysql5.7" ;
      ObSysVars[487].name_ = "default_tmp_storage_engine" ;
      ObSysVars[487].data_type_ = ObIntType ;
      ObSysVars[487].enum_names_ = "[u'InnoDB']" ;
      ObSysVars[487].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[487].id_ = SYS_VAR_DEFAULT_TMP_STORAGE_ENGINE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_TMP_STORAGE_ENGINE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_TMP_STORAGE_ENGINE] = 487 ;
      ObSysVars[487].base_value_ = "0" ;
    ObSysVars[487].alias_ = "OB_SV_DEFAULT_TMP_STORAGE_ENGINE" ;
    }();

    [&] (){
      ObSysVars[488].default_value_ = "0" ;
      ObSysVars[488].info_ = "mock for mysql5.7" ;
      ObSysVars[488].name_ = "innodb_ft_enable_diag_print" ;
      ObSysVars[488].data_type_ = ObIntType ;
      ObSysVars[488].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[488].id_ = SYS_VAR_INNODB_FT_ENABLE_DIAG_PRINT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FT_ENABLE_DIAG_PRINT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FT_ENABLE_DIAG_PRINT] = 488 ;
      ObSysVars[488].base_value_ = "0" ;
    ObSysVars[488].alias_ = "OB_SV_INNODB_FT_ENABLE_DIAG_PRINT" ;
    }();

    [&] (){
      ObSysVars[489].default_value_ = "2000" ;
      ObSysVars[489].info_ = "mock for mysql5.7" ;
      ObSysVars[489].name_ = "innodb_ft_num_word_optimize" ;
      ObSysVars[489].data_type_ = ObIntType ;
      ObSysVars[489].min_val_ = "1000" ;
      ObSysVars[489].max_val_ = "10000" ;
      ObSysVars[489].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[489].id_ = SYS_VAR_INNODB_FT_NUM_WORD_OPTIMIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FT_NUM_WORD_OPTIMIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FT_NUM_WORD_OPTIMIZE] = 489 ;
      ObSysVars[489].base_value_ = "2000" ;
    ObSysVars[489].alias_ = "OB_SV_INNODB_FT_NUM_WORD_OPTIMIZE" ;
    }();

    [&] (){
      ObSysVars[490].default_value_ = "2000000000" ;
      ObSysVars[490].info_ = "mock for mysql5.7" ;
      ObSysVars[490].name_ = "innodb_ft_result_cache_limit" ;
      ObSysVars[490].data_type_ = ObUInt64Type ;
      ObSysVars[490].min_val_ = "1000000" ;
      ObSysVars[490].max_val_ = "4294967295" ;
      ObSysVars[490].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[490].id_ = SYS_VAR_INNODB_FT_RESULT_CACHE_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FT_RESULT_CACHE_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FT_RESULT_CACHE_LIMIT] = 490 ;
      ObSysVars[490].base_value_ = "2000000000" ;
    ObSysVars[490].alias_ = "OB_SV_INNODB_FT_RESULT_CACHE_LIMIT" ;
    }();

    [&] (){
      ObSysVars[491].default_value_ = "" ;
      ObSysVars[491].info_ = "mock for mysql5.7" ;
      ObSysVars[491].name_ = "innodb_ft_server_stopword_table" ;
      ObSysVars[491].data_type_ = ObVarcharType ;
      ObSysVars[491].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[491].id_ = SYS_VAR_INNODB_FT_SERVER_STOPWORD_TABLE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_FT_SERVER_STOPWORD_TABLE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_FT_SERVER_STOPWORD_TABLE] = 491 ;
      ObSysVars[491].base_value_ = "" ;
    ObSysVars[491].alias_ = "OB_SV_INNODB_FT_SERVER_STOPWORD_TABLE" ;
    }();

    [&] (){
      ObSysVars[492].default_value_ = "0" ;
      ObSysVars[492].info_ = "mock for mysql5.7" ;
      ObSysVars[492].name_ = "innodb_optimize_fulltext_only" ;
      ObSysVars[492].data_type_ = ObIntType ;
      ObSysVars[492].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[492].id_ = SYS_VAR_INNODB_OPTIMIZE_FULLTEXT_ONLY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_OPTIMIZE_FULLTEXT_ONLY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_OPTIMIZE_FULLTEXT_ONLY] = 492 ;
      ObSysVars[492].base_value_ = "0" ;
    ObSysVars[492].alias_ = "OB_SV_INNODB_OPTIMIZE_FULLTEXT_ONLY" ;
    }();

    [&] (){
      ObSysVars[493].default_value_ = "32" ;
      ObSysVars[493].info_ = "mock for mysql5.7" ;
      ObSysVars[493].name_ = "max_tmp_tables" ;
      ObSysVars[493].data_type_ = ObIntType ;
      ObSysVars[493].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[493].id_ = SYS_VAR_MAX_TMP_TABLES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_TMP_TABLES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_TMP_TABLES] = 493 ;
      ObSysVars[493].base_value_ = "32" ;
    ObSysVars[493].alias_ = "OB_SV_MAX_TMP_TABLES" ;
    }();

    [&] (){
      ObSysVars[494].default_value_ = "" ;
      ObSysVars[494].info_ = "mock for mysql5.7" ;
      ObSysVars[494].name_ = "innodb_tmpdir" ;
      ObSysVars[494].data_type_ = ObVarcharType ;
      ObSysVars[494].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[494].id_ = SYS_VAR_INNODB_TMPDIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_TMPDIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_TMPDIR] = 494 ;
      ObSysVars[494].base_value_ = "" ;
    ObSysVars[494].alias_ = "OB_SV_INNODB_TMPDIR" ;
    }();

    [&] (){
      ObSysVars[495].default_value_ = "" ;
      ObSysVars[495].info_ = "A list of group members to which a joining member can connect to obtain details of all the current group members" ;
      ObSysVars[495].name_ = "group_replication_group_seeds" ;
      ObSysVars[495].data_type_ = ObVarcharType ;
      ObSysVars[495].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[495].id_ = SYS_VAR_GROUP_REPLICATION_GROUP_SEEDS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_REPLICATION_GROUP_SEEDS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_REPLICATION_GROUP_SEEDS] = 495 ;
      ObSysVars[495].base_value_ = "" ;
    ObSysVars[495].alias_ = "OB_SV_GROUP_REPLICATION_GROUP_SEEDS" ;
    }();

    [&] (){
      ObSysVars[496].default_value_ = "0" ;
      ObSysVars[496].info_ = "When preparing batches of rows for row-based logging and replication, this variable controls how the rows are searched for matches" ;
      ObSysVars[496].name_ = "slave_rows_search_algorithms" ;
      ObSysVars[496].data_type_ = ObIntType ;
      ObSysVars[496].enum_names_ = "[u'TABLE_SCAN,INDEX_SCAN', u'INDEX_SCAN,HASH_SCAN', u'TABLE_SCAN,HASH_SCAN', u'TABLE_SCAN,INDEX_SCAN,HASH_SCAN']" ;
      ObSysVars[496].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[496].id_ = SYS_VAR_SLAVE_ROWS_SEARCH_ALGORITHMS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_ROWS_SEARCH_ALGORITHMS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_ROWS_SEARCH_ALGORITHMS] = 496 ;
      ObSysVars[496].base_value_ = "0" ;
    ObSysVars[496].alias_ = "OB_SV_SLAVE_ROWS_SEARCH_ALGORITHMS" ;
    }();

    [&] (){
      ObSysVars[497].default_value_ = "0" ;
      ObSysVars[497].info_ = "Controls the type conversion mode in effect on the replica when using row-based replication" ;
      ObSysVars[497].name_ = "slave_type_conversions" ;
      ObSysVars[497].data_type_ = ObIntType ;
      ObSysVars[497].enum_names_ = "[u'ALL_LOSSY', u'ALL_NON_LOSSY', u'ALL_SIGNED', u'ALL_UNSIGNED']" ;
      ObSysVars[497].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[497].id_ = SYS_VAR_SLAVE_TYPE_CONVERSIONS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SLAVE_TYPE_CONVERSIONS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SLAVE_TYPE_CONVERSIONS] = 497 ;
      ObSysVars[497].base_value_ = "0" ;
    ObSysVars[497].alias_ = "OB_SV_SLAVE_TYPE_CONVERSIONS" ;
    }();

    [&] (){
      ObSysVars[498].default_value_ = "64" ;
      ObSysVars[498].info_ = "The number of neighbor nodes considered during any HNSW vector index search on the session" ;
      ObSysVars[498].name_ = "ob_hnsw_ef_search" ;
      ObSysVars[498].data_type_ = ObUInt64Type ;
      ObSysVars[498].min_val_ = "1" ;
      ObSysVars[498].max_val_ = "1000" ;
      ObSysVars[498].flags_ = ObSysVarFlag::SESSION_SCOPE ;
      ObSysVars[498].id_ = SYS_VAR_OB_HNSW_EF_SEARCH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_HNSW_EF_SEARCH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_HNSW_EF_SEARCH] = 498 ;
      ObSysVars[498].base_value_ = "40" ;
    ObSysVars[498].alias_ = "OB_SV_HNSW_EF_SEARCH" ;
    }();

    [&] (){
      ObSysVars[499].default_value_ = "0" ;
      ObSysVars[499].info_ = "This variable specifies how to use delayed key writes. It applies only to MyISAM tables. Delayed key writing causes key buffers not to be flushed between writes, merely simulates MySQL 5.7" ;
      ObSysVars[499].name_ = "delay_key_write" ;
      ObSysVars[499].data_type_ = ObIntType ;
      ObSysVars[499].enum_names_ = "[u'ON', u'OFF', u'ALL']" ;
      ObSysVars[499].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[499].id_ = SYS_VAR_DELAY_KEY_WRITE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DELAY_KEY_WRITE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DELAY_KEY_WRITE] = 499 ;
      ObSysVars[499].base_value_ = "0" ;
    ObSysVars[499].alias_ = "OB_SV_DELAY_KEY_WRITE" ;
    }();

    [&] (){
      ObSysVars[500].default_value_ = "0" ;
      ObSysVars[500].info_ = "When this option is enabled, index key prefixes longer than 767 bytes (up to 3072 bytes) are allowed for InnoDB tables that use DYNAMIC or COMPRESSED row format, merely simulates MySQL 5.7" ;
      ObSysVars[500].name_ = "innodb_large_prefix" ;
      ObSysVars[500].data_type_ = ObIntType ;
      ObSysVars[500].enum_names_ = "[u'ON', u'OFF']" ;
      ObSysVars[500].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[500].id_ = SYS_VAR_INNODB_LARGE_PREFIX ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_LARGE_PREFIX)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_LARGE_PREFIX] = 500 ;
      ObSysVars[500].base_value_ = "0" ;
    ObSysVars[500].alias_ = "OB_SV_INNODB_LARGE_PREFIX" ;
    }();

    [&] (){
      ObSysVars[501].default_value_ = "8388608" ;
      ObSysVars[501].info_ = "The size of the buffer used for index blocks, merely simulates MySQL 5.7" ;
      ObSysVars[501].name_ = "key_buffer_size" ;
      ObSysVars[501].data_type_ = ObIntType ;
      ObSysVars[501].min_val_ = "0" ;
      ObSysVars[501].max_val_ = "4294967295" ;
      ObSysVars[501].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[501].id_ = SYS_VAR_KEY_BUFFER_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEY_BUFFER_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEY_BUFFER_SIZE] = 501 ;
      ObSysVars[501].base_value_ = "8388608" ;
    ObSysVars[501].alias_ = "OB_SV_KEY_BUFFER_SIZE" ;
    }();

    [&] (){
      ObSysVars[502].default_value_ = "300" ;
      ObSysVars[502].info_ = "This value controls the demotion of buffers from the hot sublist of a key cache to the warm sublist. Lower values cause demotion to happen more quickly, merely simulates MySQL 5.7" ;
      ObSysVars[502].name_ = "key_cache_age_threshold" ;
      ObSysVars[502].data_type_ = ObUInt64Type ;
      ObSysVars[502].min_val_ = "100" ;
      ObSysVars[502].max_val_ = "18446744073709551516" ;
      ObSysVars[502].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[502].id_ = SYS_VAR_KEY_CACHE_AGE_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEY_CACHE_AGE_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEY_CACHE_AGE_THRESHOLD] = 502 ;
      ObSysVars[502].base_value_ = "300" ;
    ObSysVars[502].alias_ = "OB_SV_KEY_CACHE_AGE_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[503].default_value_ = "100" ;
      ObSysVars[503].info_ = "The division point between the hot and warm sublists of the key cache buffer list, merely simulates MySQL 5.7" ;
      ObSysVars[503].name_ = "key_cache_division_limit" ;
      ObSysVars[503].data_type_ = ObIntType ;
      ObSysVars[503].min_val_ = "1" ;
      ObSysVars[503].max_val_ = "100" ;
      ObSysVars[503].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[503].id_ = SYS_VAR_KEY_CACHE_DIVISION_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEY_CACHE_DIVISION_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEY_CACHE_DIVISION_LIMIT] = 503 ;
      ObSysVars[503].base_value_ = "100" ;
    ObSysVars[503].alias_ = "OB_SV_KEY_CACHE_DIVISION_LIMIT" ;
    }();

    [&] (){
      ObSysVars[504].default_value_ = "18446744073709551615" ;
      ObSysVars[504].info_ = "Limit the assumed maximum number of seeks when looking up rows based on a key, merely simulates MySQL 5.7" ;
      ObSysVars[504].name_ = "max_seeks_for_key" ;
      ObSysVars[504].data_type_ = ObUInt64Type ;
      ObSysVars[504].min_val_ = "1" ;
      ObSysVars[504].max_val_ = "18446744073709551615" ;
      ObSysVars[504].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[504].id_ = SYS_VAR_MAX_SEEKS_FOR_KEY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_SEEKS_FOR_KEY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_SEEKS_FOR_KEY] = 504 ;
      ObSysVars[504].base_value_ = "18446744073709551615" ;
    ObSysVars[504].alias_ = "OB_SV_MAX_SEEKS_FOR_KEY" ;
    }();

    [&] (){
      ObSysVars[505].default_value_ = "0" ;
      ObSysVars[505].info_ = "When this variable is enabled, the server does not use the optimized method of processing an ALTER TABLE operation, merely simulates MySQL 5.7" ;
      ObSysVars[505].name_ = "old_alter_table" ;
      ObSysVars[505].data_type_ = ObIntType ;
      ObSysVars[505].enum_names_ = "[u'OFF', u'ON']" ;
      ObSysVars[505].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[505].id_ = SYS_VAR_OLD_ALTER_TABLE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OLD_ALTER_TABLE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OLD_ALTER_TABLE] = 505 ;
      ObSysVars[505].base_value_ = "0" ;
    ObSysVars[505].alias_ = "OB_SV_OLD_ALTER_TABLE" ;
    }();

    [&] (){
      ObSysVars[506].default_value_ = "-1" ;
      ObSysVars[506].info_ = "The number of table definitions that can be stored in the table definition cache, merely simulates MySQL 5.7" ;
      ObSysVars[506].name_ = "table_definition_cache" ;
      ObSysVars[506].data_type_ = ObIntType ;
      ObSysVars[506].min_val_ = "400" ;
      ObSysVars[506].max_val_ = "524288" ;
      ObSysVars[506].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[506].id_ = SYS_VAR_TABLE_DEFINITION_CACHE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TABLE_DEFINITION_CACHE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_TABLE_DEFINITION_CACHE] = 506 ;
      ObSysVars[506].base_value_ = "-1" ;
    ObSysVars[506].alias_ = "OB_SV_TABLE_DEFINITION_CACHE" ;
    }();

    [&] (){
      ObSysVars[507].default_value_ = "1048576" ;
      ObSysVars[507].info_ = "The sort buffer size for online DDL operations that create or rebuild secondary indexes, merely simulates MySQL 5.7" ;
      ObSysVars[507].name_ = "innodb_sort_buffer_size" ;
      ObSysVars[507].data_type_ = ObIntType ;
      ObSysVars[507].min_val_ = "65536" ;
      ObSysVars[507].max_val_ = "67108864" ;
      ObSysVars[507].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[507].id_ = SYS_VAR_INNODB_SORT_BUFFER_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_SORT_BUFFER_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_SORT_BUFFER_SIZE] = 507 ;
      ObSysVars[507].base_value_ = "1048576" ;
    ObSysVars[507].alias_ = "OB_SV_INNODB_SORT_BUFFER_SIZE" ;
    }();

    [&] (){
      ObSysVars[508].default_value_ = "1024" ;
      ObSysVars[508].info_ = "The size in bytes of blocks in the key cache, merely simulates MySQL 5.7" ;
      ObSysVars[508].name_ = "key_cache_block_size" ;
      ObSysVars[508].data_type_ = ObIntType ;
      ObSysVars[508].min_val_ = "512" ;
      ObSysVars[508].max_val_ = "16384" ;
      ObSysVars[508].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[508].id_ = SYS_VAR_KEY_CACHE_BLOCK_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEY_CACHE_BLOCK_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEY_CACHE_BLOCK_SIZE] = 508 ;
      ObSysVars[508].base_value_ = "1024" ;
    ObSysVars[508].alias_ = "OB_SV_KEY_CACHE_BLOCK_SIZE" ;
    }();

    [&] (){
      ObSysVars[509].default_value_ = "0" ;
      ObSysVars[509].info_ = "Use this variable to select the interface mode for the OBKV tenant. You can select one of 'ALL, TABLEAPI, HBASE, REDIS, NONE', where 'ALL' is the default and 'NONE' represents the non-OBKV interface mode." ;
      ObSysVars[509].name_ = "ob_kv_mode" ;
      ObSysVars[509].data_type_ = ObIntType ;
      ObSysVars[509].enum_names_ = "[u'ALL', u'TABLEAPI', u'HBASE', u'REDIS', u'NONE']" ;
      ObSysVars[509].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY ;
      ObSysVars[509].id_ = SYS_VAR_OB_KV_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_KV_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_KV_MODE] = 509 ;
      ObSysVars[509].base_value_ = "0" ;
    ObSysVars[509].alias_ = "OB_SV_KV_MODE" ;
    }();

    [&] (){
      ObSysVars[510].default_value_ = "1" ;
      ObSysVars[510].info_ = "wether use parameter anonymous_block" ;
      ObSysVars[510].name_ = "ob_enable_parameter_anonymous_block" ;
      ObSysVars[510].data_type_ = ObIntType ;
      ObSysVars[510].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE ;
      ObSysVars[510].id_ = SYS_VAR_OB_ENABLE_PARAMETER_ANONYMOUS_BLOCK ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_PARAMETER_ANONYMOUS_BLOCK)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_PARAMETER_ANONYMOUS_BLOCK] = 510 ;
      ObSysVars[510].base_value_ = "1" ;
    ObSysVars[510].alias_ = "OB_SV_ENABLE_PARAMETER_ANONYMOUS_BLOCK" ;
    }();

    [&] (){
      ObSysVars[511].default_value_ = "" ;
      ObSysVars[511].info_ = "The directory where character sets are installed" ;
      ObSysVars[511].name_ = "character_sets_dir" ;
      ObSysVars[511].data_type_ = ObVarcharType ;
      ObSysVars[511].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[511].id_ = SYS_VAR_CHARACTER_SETS_DIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SETS_DIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SETS_DIR] = 511 ;
      ObSysVars[511].base_value_ = "" ;
    ObSysVars[511].alias_ = "OB_SV_CHARACTER_SETS_DIR" ;
    }();

    [&] (){
      ObSysVars[512].default_value_ = "%Y-%m-%d" ;
      ObSysVars[512].info_ = "" ;
      ObSysVars[512].name_ = "date_format" ;
      ObSysVars[512].data_type_ = ObVarcharType ;
      ObSysVars[512].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[512].id_ = SYS_VAR_DATE_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DATE_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DATE_FORMAT] = 512 ;
      ObSysVars[512].base_value_ = "%Y-%m-%d" ;
    ObSysVars[512].alias_ = "OB_SV_DATE_FORMAT" ;
    }();

    [&] (){
      ObSysVars[513].default_value_ = "%Y-%m-%d %H:%i:%s" ;
      ObSysVars[513].info_ = "" ;
      ObSysVars[513].name_ = "datetime_format" ;
      ObSysVars[513].data_type_ = ObVarcharType ;
      ObSysVars[513].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[513].id_ = SYS_VAR_DATETIME_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DATETIME_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DATETIME_FORMAT] = 513 ;
      ObSysVars[513].base_value_ = "%Y-%m-%d %H:%i:%s" ;
    ObSysVars[513].alias_ = "OB_SV_DATETIME_FORMAT" ;
    }();

    [&] (){
      ObSysVars[514].default_value_ = "1" ;
      ObSysVars[514].info_ = "This variable controls how the server handles clients with expired passwords" ;
      ObSysVars[514].name_ = "disconnect_on_expired_password" ;
      ObSysVars[514].data_type_ = ObIntType ;
      ObSysVars[514].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[514].id_ = SYS_VAR_DISCONNECT_ON_EXPIRED_PASSWORD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DISCONNECT_ON_EXPIRED_PASSWORD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DISCONNECT_ON_EXPIRED_PASSWORD] = 514 ;
      ObSysVars[514].base_value_ = "1" ;
    ObSysVars[514].alias_ = "OB_SV_DISCONNECT_ON_EXPIRED_PASSWORD" ;
    }();

    [&] (){
      ObSysVars[515].default_value_ = "" ;
      ObSysVars[515].info_ = "The external user name used during the authentication process, as set by the plugin used to authenticate the client" ;
      ObSysVars[515].name_ = "external_user" ;
      ObSysVars[515].data_type_ = ObVarcharType ;
      ObSysVars[515].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::NULLABLE | ObSysVarFlag::READONLY ;
      ObSysVars[515].id_ = SYS_VAR_EXTERNAL_USER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_EXTERNAL_USER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_EXTERNAL_USER] = 515 ;
      ObSysVars[515].base_value_ = "" ;
    ObSysVars[515].alias_ = "OB_SV_EXTERNAL_USER" ;
    }();

    [&] (){
      ObSysVars[516].default_value_ = "YES" ;
      ObSysVars[516].info_ = "The external user name used during the authentication process, as set by the plugin used to authenticate the client" ;
      ObSysVars[516].name_ = "have_crypt" ;
      ObSysVars[516].data_type_ = ObVarcharType ;
      ObSysVars[516].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[516].id_ = SYS_VAR_HAVE_CRYPT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_CRYPT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_CRYPT] = 516 ;
      ObSysVars[516].base_value_ = "YES" ;
    ObSysVars[516].alias_ = "OB_SV_HAVE_CRYPT" ;
    }();

    [&] (){
      ObSysVars[517].default_value_ = "YES" ;
      ObSysVars[517].info_ = "YES if mysqld supports dynamic loading of plugins, NO if not. If the value is NO, you cannot use options such as --plugin-load to load plugins at server startup, or the INSTALL PLUGIN statement to load plugins at runtime" ;
      ObSysVars[517].name_ = "have_dynamic_loading" ;
      ObSysVars[517].data_type_ = ObVarcharType ;
      ObSysVars[517].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[517].id_ = SYS_VAR_HAVE_DYNAMIC_LOADING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_DYNAMIC_LOADING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_DYNAMIC_LOADING] = 517 ;
      ObSysVars[517].base_value_ = "YES" ;
    ObSysVars[517].alias_ = "OB_SV_HAVE_DYNAMIC_LOADING" ;
    }();

    [&] (){
      ObSysVars[518].default_value_ = "" ;
      ObSysVars[518].info_ = "The location of the configuration file for the keyring_aws plugin. This variable is unavailable unless that plugin is installed" ;
      ObSysVars[518].name_ = "keyring_aws_conf_file" ;
      ObSysVars[518].data_type_ = ObVarcharType ;
      ObSysVars[518].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[518].id_ = SYS_VAR_KEYRING_AWS_CONF_FILE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_AWS_CONF_FILE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_AWS_CONF_FILE] = 518 ;
      ObSysVars[518].base_value_ = "" ;
    ObSysVars[518].alias_ = "OB_SV_KEYRING_AWS_CONF_FILE" ;
    }();

    [&] (){
      ObSysVars[519].default_value_ = "" ;
      ObSysVars[519].info_ = "The location of the storage file for the keyring_aws plugin. This variable is unavailable unless that plugin is installed" ;
      ObSysVars[519].name_ = "keyring_aws_data_file" ;
      ObSysVars[519].data_type_ = ObVarcharType ;
      ObSysVars[519].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[519].id_ = SYS_VAR_KEYRING_AWS_DATA_FILE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_AWS_DATA_FILE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_AWS_DATA_FILE] = 519 ;
      ObSysVars[519].base_value_ = "" ;
    ObSysVars[519].alias_ = "OB_SV_KEYRING_AWS_DATA_FILE" ;
    }();

    [&] (){
      ObSysVars[520].default_value_ = "" ;
      ObSysVars[520].info_ = "The language to use for error messages" ;
      ObSysVars[520].name_ = "language" ;
      ObSysVars[520].data_type_ = ObVarcharType ;
      ObSysVars[520].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[520].id_ = SYS_VAR_LANGUAGE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LANGUAGE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LANGUAGE] = 520 ;
      ObSysVars[520].base_value_ = "" ;
    ObSysVars[520].alias_ = "OB_SV_LANGUAGE" ;
    }();

    [&] (){
      ObSysVars[521].default_value_ = "" ;
      ObSysVars[521].info_ = "The language to use for error messages" ;
      ObSysVars[521].name_ = "lc_messages_dir" ;
      ObSysVars[521].data_type_ = ObVarcharType ;
      ObSysVars[521].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[521].id_ = SYS_VAR_LC_MESSAGES_DIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LC_MESSAGES_DIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LC_MESSAGES_DIR] = 521 ;
      ObSysVars[521].base_value_ = "" ;
    ObSysVars[521].alias_ = "OB_SV_LC_MESSAGES_DIR" ;
    }();

    [&] (){
      ObSysVars[522].default_value_ = "0" ;
      ObSysVars[522].info_ = "This variable describes the case sensitivity of file names on the file system where the data directory is located" ;
      ObSysVars[522].name_ = "lower_case_file_system" ;
      ObSysVars[522].data_type_ = ObIntType ;
      ObSysVars[522].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[522].id_ = SYS_VAR_LOWER_CASE_FILE_SYSTEM ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOWER_CASE_FILE_SYSTEM)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_LOWER_CASE_FILE_SYSTEM] = 522 ;
      ObSysVars[522].base_value_ = "0" ;
    ObSysVars[522].alias_ = "OB_SV_LOWER_CASE_FILE_SYSTEM" ;
    }();

    [&] (){
      ObSysVars[523].default_value_ = "1024" ;
      ObSysVars[523].info_ = "The maximum number of bytes of memory reserved per session for computation of normalized statement digests" ;
      ObSysVars[523].name_ = "max_digest_length" ;
      ObSysVars[523].data_type_ = ObIntType ;
      ObSysVars[523].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[523].id_ = SYS_VAR_MAX_DIGEST_LENGTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_DIGEST_LENGTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_DIGEST_LENGTH] = 523 ;
      ObSysVars[523].base_value_ = "1024" ;
    ObSysVars[523].alias_ = "OB_SV_MAX_DIGEST_LENGTH" ;
    }();

    [&] (){
      ObSysVars[524].default_value_ = "ndbinfo" ;
      ObSysVars[524].info_ = "Shows the name used for the NDB information database" ;
      ObSysVars[524].name_ = "ndbinfo_database" ;
      ObSysVars[524].data_type_ = ObVarcharType ;
      ObSysVars[524].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[524].id_ = SYS_VAR_NDBINFO_DATABASE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDBINFO_DATABASE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDBINFO_DATABASE] = 524 ;
      ObSysVars[524].base_value_ = "ndbinfo" ;
    ObSysVars[524].alias_ = "OB_SV_NDBINFO_DATABASE" ;
    }();

    [&] (){
      ObSysVars[525].default_value_ = "ndb$" ;
      ObSysVars[525].info_ = "The prefix used in naming the ndbinfo database's base tables (normally hidden, unless exposed by setting ndbinfo_show_hidden" ;
      ObSysVars[525].name_ = "ndbinfo_table_prefix" ;
      ObSysVars[525].data_type_ = ObVarcharType ;
      ObSysVars[525].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[525].id_ = SYS_VAR_NDBINFO_TABLE_PREFIX ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDBINFO_TABLE_PREFIX)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDBINFO_TABLE_PREFIX] = 525 ;
      ObSysVars[525].base_value_ = "ndb$" ;
    ObSysVars[525].alias_ = "OB_SV_NDBINFO_TABLE_PREFIX" ;
    }();

    [&] (){
      ObSysVars[526].default_value_ = "" ;
      ObSysVars[526].info_ = "Shows the version of the ndbinfo engine in use" ;
      ObSysVars[526].name_ = "ndbinfo_version" ;
      ObSysVars[526].data_type_ = ObVarcharType ;
      ObSysVars[526].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[526].id_ = SYS_VAR_NDBINFO_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDBINFO_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDBINFO_VERSION] = 526 ;
      ObSysVars[526].base_value_ = "" ;
    ObSysVars[526].alias_ = "OB_SV_NDBINFO_VERSION" ;
    }();

    [&] (){
      ObSysVars[527].default_value_ = "32768" ;
      ObSysVars[527].info_ = "This sets the size in bytes that is used for NDB transaction batches" ;
      ObSysVars[527].name_ = "ndb_batch_size" ;
      ObSysVars[527].data_type_ = ObIntType ;
      ObSysVars[527].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[527].id_ = SYS_VAR_NDB_BATCH_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_BATCH_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_BATCH_SIZE] = 527 ;
      ObSysVars[527].base_value_ = "32768" ;
    ObSysVars[527].alias_ = "OB_SV_NDB_BATCH_SIZE" ;
    }();

    [&] (){
      ObSysVars[528].default_value_ = "1" ;
      ObSysVars[528].info_ = "a mysqld process can use multiple connections to the cluster, effectively mimicking several SQL nodes" ;
      ObSysVars[528].name_ = "ndb_cluster_connection_pool" ;
      ObSysVars[528].data_type_ = ObIntType ;
      ObSysVars[528].min_val_ = "1" ;
      ObSysVars[528].max_val_ = "63" ;
      ObSysVars[528].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[528].id_ = SYS_VAR_NDB_CLUSTER_CONNECTION_POOL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_CLUSTER_CONNECTION_POOL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_CLUSTER_CONNECTION_POOL] = 528 ;
      ObSysVars[528].base_value_ = "1" ;
    ObSysVars[528].alias_ = "OB_SV_NDB_CLUSTER_CONNECTION_POOL" ;
    }();

    [&] (){
      ObSysVars[529].default_value_ = "" ;
      ObSysVars[529].info_ = "Specifies a comma-separated list of node IDs for connections to the cluster used by an SQL node" ;
      ObSysVars[529].name_ = "ndb_cluster_connection_pool_nodeids" ;
      ObSysVars[529].data_type_ = ObVarcharType ;
      ObSysVars[529].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[529].id_ = SYS_VAR_NDB_CLUSTER_CONNECTION_POOL_NODEIDS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_CLUSTER_CONNECTION_POOL_NODEIDS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_CLUSTER_CONNECTION_POOL_NODEIDS] = 529 ;
      ObSysVars[529].base_value_ = "" ;
    ObSysVars[529].alias_ = "OB_SV_NDB_CLUSTER_CONNECTION_POOL_NODEIDS" ;
    }();

    [&] (){
      ObSysVars[530].default_value_ = "0" ;
      ObSysVars[530].info_ = "Causes a replica mysqld to log any updates received from its immediate source to the mysql.ndb_apply_status table in its own binary log using its own server ID rather than the server ID of the source" ;
      ObSysVars[530].name_ = "ndb_log_apply_status" ;
      ObSysVars[530].data_type_ = ObIntType ;
      ObSysVars[530].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[530].id_ = SYS_VAR_NDB_LOG_APPLY_STATUS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_LOG_APPLY_STATUS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_LOG_APPLY_STATUS] = 530 ;
      ObSysVars[530].base_value_ = "0" ;
    ObSysVars[530].alias_ = "OB_SV_NDB_LOG_APPLY_STATUS" ;
    }();

    [&] (){
      ObSysVars[531].default_value_ = "1" ;
      ObSysVars[531].info_ = "Causes updates to NDB tables to be written to the binary log. Setting this variable has no effect if binary logging is not already enabled for the server using log_bin" ;
      ObSysVars[531].name_ = "ndb_log_bin" ;
      ObSysVars[531].data_type_ = ObIntType ;
      ObSysVars[531].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[531].id_ = SYS_VAR_NDB_LOG_BIN ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_LOG_BIN)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_LOG_BIN] = 531 ;
      ObSysVars[531].base_value_ = "1" ;
    ObSysVars[531].alias_ = "OB_SV_NDB_LOG_BIN" ;
    }();

    [&] (){
      ObSysVars[532].default_value_ = "0" ;
      ObSysVars[532].info_ = "When this option is specified, and complete logging of all found row events is not possible, the mysqld process is terminated" ;
      ObSysVars[532].name_ = "ndb_log_fail_terminate" ;
      ObSysVars[532].data_type_ = ObIntType ;
      ObSysVars[532].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[532].id_ = SYS_VAR_NDB_LOG_FAIL_TERMINATE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_LOG_FAIL_TERMINATE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_LOG_FAIL_TERMINATE] = 532 ;
      ObSysVars[532].base_value_ = "0" ;
    ObSysVars[532].alias_ = "OB_SV_NDB_LOG_FAIL_TERMINATE" ;
    }();

    [&] (){
      ObSysVars[533].default_value_ = "0" ;
      ObSysVars[533].info_ = "Shows whether the originating server ID and epoch are logged in the ndb_binlog_index table" ;
      ObSysVars[533].name_ = "ndb_log_orig" ;
      ObSysVars[533].data_type_ = ObIntType ;
      ObSysVars[533].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[533].id_ = SYS_VAR_NDB_LOG_ORIG ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_LOG_ORIG)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_LOG_ORIG] = 533 ;
      ObSysVars[533].base_value_ = "0" ;
    ObSysVars[533].alias_ = "OB_SV_NDB_LOG_ORIG" ;
    }();

    [&] (){
      ObSysVars[534].default_value_ = "0" ;
      ObSysVars[534].info_ = "shows whether a replica mysqld writes NDB transaction IDs in the binary log" ;
      ObSysVars[534].name_ = "ndb_log_transaction_id" ;
      ObSysVars[534].data_type_ = ObIntType ;
      ObSysVars[534].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[534].id_ = SYS_VAR_NDB_LOG_TRANSACTION_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_LOG_TRANSACTION_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_LOG_TRANSACTION_ID] = 534 ;
      ObSysVars[534].base_value_ = "0" ;
    ObSysVars[534].alias_ = "OB_SV_NDB_LOG_TRANSACTION_ID" ;
    }();

    [&] (){
      ObSysVars[535].default_value_ = "3" ;
      ObSysVars[535].info_ = "" ;
      ObSysVars[535].name_ = "ndb_optimized_node_selection" ;
      ObSysVars[535].data_type_ = ObIntType ;
      ObSysVars[535].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[535].id_ = SYS_VAR_NDB_OPTIMIZED_NODE_SELECTION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_OPTIMIZED_NODE_SELECTION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_OPTIMIZED_NODE_SELECTION] = 535 ;
      ObSysVars[535].base_value_ = "3" ;
    ObSysVars[535].alias_ = "OB_SV_NDB_OPTIMIZED_NODE_SELECTION" ;
    }();

    [&] (){
      ObSysVars[536].default_value_ = "" ;
      ObSysVars[536].info_ = "If this MySQL Server is connected to an NDB cluster, this read-only variable shows the cluster system name. Otherwise, the value is an empty string" ;
      ObSysVars[536].name_ = "Ndb_system_name" ;
      ObSysVars[536].data_type_ = ObVarcharType ;
      ObSysVars[536].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[536].id_ = SYS_VAR_NDB_SYSTEM_NAME ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_SYSTEM_NAME)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_SYSTEM_NAME] = 536 ;
      ObSysVars[536].base_value_ = "" ;
    ObSysVars[536].alias_ = "OB_SV_NDB_SYSTEM_NAME" ;
    }();

    [&] (){
      ObSysVars[537].default_value_ = "0" ;
      ObSysVars[537].info_ = "Forces NDB to use copying of tables in the event of problems with online ALTER TABLE operations" ;
      ObSysVars[537].name_ = "ndb_use_copying_alter_table" ;
      ObSysVars[537].data_type_ = ObIntType ;
      ObSysVars[537].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[537].id_ = SYS_VAR_NDB_USE_COPYING_ALTER_TABLE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_USE_COPYING_ALTER_TABLE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_USE_COPYING_ALTER_TABLE] = 537 ;
      ObSysVars[537].base_value_ = "0" ;
    ObSysVars[537].alias_ = "OB_SV_NDB_USE_COPYING_ALTER_TABLE" ;
    }();

    [&] (){
      ObSysVars[538].default_value_ = "" ;
      ObSysVars[538].info_ = "NDB engine version in ndb-x.y.z format" ;
      ObSysVars[538].name_ = "ndb_version_string" ;
      ObSysVars[538].data_type_ = ObVarcharType ;
      ObSysVars[538].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[538].id_ = SYS_VAR_NDB_VERSION_STRING ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_VERSION_STRING)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_VERSION_STRING] = 538 ;
      ObSysVars[538].base_value_ = "" ;
    ObSysVars[538].alias_ = "OB_SV_NDB_VERSION_STRING" ;
    }();

    [&] (){
      ObSysVars[539].default_value_ = "30" ;
      ObSysVars[539].info_ = "This option sets the period of time that the MySQL server waits for connections to NDB Cluster management and data nodes to be established before accepting MySQL client connections." ;
      ObSysVars[539].name_ = "ndb_wait_connected" ;
      ObSysVars[539].data_type_ = ObIntType ;
      ObSysVars[539].min_val_ = "0" ;
      ObSysVars[539].max_val_ = "31536000" ;
      ObSysVars[539].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[539].id_ = SYS_VAR_NDB_WAIT_CONNECTED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_WAIT_CONNECTED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_WAIT_CONNECTED] = 539 ;
      ObSysVars[539].base_value_ = "30" ;
    ObSysVars[539].alias_ = "OB_SV_NDB_WAIT_CONNECTED" ;
    }();

    [&] (){
      ObSysVars[540].default_value_ = "30" ;
      ObSysVars[540].info_ = "This variable shows the period of time that the MySQL server waits for the NDB storage engine to complete setup before timing out and treating NDB as unavailable. The time is specified in seconds." ;
      ObSysVars[540].name_ = "ndb_wait_setup" ;
      ObSysVars[540].data_type_ = ObIntType ;
      ObSysVars[540].min_val_ = "0" ;
      ObSysVars[540].max_val_ = "31536000" ;
      ObSysVars[540].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[540].id_ = SYS_VAR_NDB_WAIT_SETUP ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_WAIT_SETUP)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_WAIT_SETUP] = 540 ;
      ObSysVars[540].base_value_ = "30" ;
    ObSysVars[540].alias_ = "OB_SV_NDB_WAIT_SETUP" ;
    }();

    [&] (){
      ObSysVars[541].default_value_ = "" ;
      ObSysVars[541].info_ = "If the current client is a proxy for another user, this variable is the proxy user account name" ;
      ObSysVars[541].name_ = "proxy_user" ;
      ObSysVars[541].data_type_ = ObVarcharType ;
      ObSysVars[541].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[541].id_ = SYS_VAR_PROXY_USER ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PROXY_USER)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PROXY_USER] = 541 ;
      ObSysVars[541].base_value_ = "" ;
    ObSysVars[541].alias_ = "OB_SV_PROXY_USER" ;
    }();

    [&] (){
      ObSysVars[542].default_value_ = "1" ;
      ObSysVars[542].info_ = "It controls whether the server autogenerates RSA private/public key-pair files in the data directory" ;
      ObSysVars[542].name_ = "sha256_password_auto_generate_rsa_keys" ;
      ObSysVars[542].data_type_ = ObIntType ;
      ObSysVars[542].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[542].id_ = SYS_VAR_SHA256_PASSWORD_AUTO_GENERATE_RSA_KEYS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SHA256_PASSWORD_AUTO_GENERATE_RSA_KEYS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SHA256_PASSWORD_AUTO_GENERATE_RSA_KEYS] = 542 ;
      ObSysVars[542].base_value_ = "1" ;
    ObSysVars[542].alias_ = "OB_SV_SHA256_PASSWORD_AUTO_GENERATE_RSA_KEYS" ;
    }();

    [&] (){
      ObSysVars[543].default_value_ = "private_key.pem" ;
      ObSysVars[543].info_ = "Its value is the path name of the RSA private key file for the sha256_password authentication plugin" ;
      ObSysVars[543].name_ = "sha256_password_private_key_path" ;
      ObSysVars[543].data_type_ = ObVarcharType ;
      ObSysVars[543].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[543].id_ = SYS_VAR_SHA256_PASSWORD_PRIVATE_KEY_PATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SHA256_PASSWORD_PRIVATE_KEY_PATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SHA256_PASSWORD_PRIVATE_KEY_PATH] = 543 ;
      ObSysVars[543].base_value_ = "private_key.pem" ;
    ObSysVars[543].alias_ = "OB_SV_SHA256_PASSWORD_PRIVATE_KEY_PATH" ;
    }();

    [&] (){
      ObSysVars[544].default_value_ = "public_key.pem" ;
      ObSysVars[544].info_ = "Its value is the path name of the RSA public key file for the sha256_password authentication plugin" ;
      ObSysVars[544].name_ = "sha256_password_public_key_path" ;
      ObSysVars[544].data_type_ = ObVarcharType ;
      ObSysVars[544].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[544].id_ = SYS_VAR_SHA256_PASSWORD_PUBLIC_KEY_PATH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SHA256_PASSWORD_PUBLIC_KEY_PATH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SHA256_PASSWORD_PUBLIC_KEY_PATH] = 544 ;
      ObSysVars[544].base_value_ = "public_key.pem" ;
    ObSysVars[544].alias_ = "OB_SV_SHA256_PASSWORD_PUBLIC_KEY_PATH" ;
    }();

    [&] (){
      ObSysVars[545].default_value_ = "0" ;
      ObSysVars[545].info_ = " If the variable value is ON, the SHOW DATABASES statement is permitted only to users who have the SHOW DATABASES privilege, and the statement displays all database names" ;
      ObSysVars[545].name_ = "skip_show_database" ;
      ObSysVars[545].data_type_ = ObVarcharType ;
      ObSysVars[545].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[545].id_ = SYS_VAR_SKIP_SHOW_DATABASE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SKIP_SHOW_DATABASE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SKIP_SHOW_DATABASE] = 545 ;
      ObSysVars[545].base_value_ = "0" ;
    ObSysVars[545].alias_ = "OB_SV_SKIP_SHOW_DATABASE" ;
    }();

    [&] (){
      ObSysVars[546].default_value_ = "" ;
      ObSysVars[546].info_ = "This option tells the server to load the named plugins at startup" ;
      ObSysVars[546].name_ = "plugin_load" ;
      ObSysVars[546].data_type_ = ObVarcharType ;
      ObSysVars[546].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[546].id_ = SYS_VAR_PLUGIN_LOAD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLUGIN_LOAD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PLUGIN_LOAD] = 546 ;
      ObSysVars[546].base_value_ = "" ;
    ObSysVars[546].alias_ = "OB_SV_PLUGIN_LOAD" ;
    }();

    [&] (){
      ObSysVars[547].default_value_ = "" ;
      ObSysVars[547].info_ = "adds a plugin or plugins to the set of plugins to be loaded at startup" ;
      ObSysVars[547].name_ = "plugin_load_add" ;
      ObSysVars[547].data_type_ = ObVarcharType ;
      ObSysVars[547].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[547].id_ = SYS_VAR_PLUGIN_LOAD_ADD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLUGIN_LOAD_ADD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PLUGIN_LOAD_ADD] = 547 ;
      ObSysVars[547].base_value_ = "" ;
    ObSysVars[547].alias_ = "OB_SV_PLUGIN_LOAD_ADD" ;
    }();

    [&] (){
      ObSysVars[548].default_value_ = "0" ;
      ObSysVars[548].info_ = "the server stores all temporary tables on disk rather than in memory" ;
      ObSysVars[548].name_ = "big_tables" ;
      ObSysVars[548].data_type_ = ObIntType ;
      ObSysVars[548].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[548].id_ = SYS_VAR_BIG_TABLES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BIG_TABLES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_BIG_TABLES] = 548 ;
      ObSysVars[548].base_value_ = "0" ;
    ObSysVars[548].alias_ = "OB_SV_BIG_TABLES" ;
    }();

    [&] (){
      ObSysVars[549].default_value_ = "0" ;
      ObSysVars[549].info_ = "If the check_proxy_users system variable is enabled, the server performs proxy user mapping for any authentication plugins that make such a request" ;
      ObSysVars[549].name_ = "check_proxy_users" ;
      ObSysVars[549].data_type_ = ObIntType ;
      ObSysVars[549].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[549].id_ = SYS_VAR_CHECK_PROXY_USERS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHECK_PROXY_USERS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CHECK_PROXY_USERS] = 549 ;
      ObSysVars[549].base_value_ = "0" ;
    ObSysVars[549].alias_ = "OB_SV_CHECK_PROXY_USERS" ;
    }();

    [&] (){
      ObSysVars[550].default_value_ = "0" ;
      ObSysVars[550].info_ = "The number of consecutive failed connection attempts permitted to accounts before the server adds a delay for subsequent connection attempts" ;
      ObSysVars[550].name_ = "connection_control_failed_connections_threshold" ;
      ObSysVars[550].data_type_ = ObIntType ;
      ObSysVars[550].min_val_ = "0" ;
      ObSysVars[550].max_val_ = "2147483647" ;
      ObSysVars[550].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[550].id_ = SYS_VAR_CONNECTION_CONTROL_FAILED_CONNECTIONS_THRESHOLD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CONNECTION_CONTROL_FAILED_CONNECTIONS_THRESHOLD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CONNECTION_CONTROL_FAILED_CONNECTIONS_THRESHOLD] = 550 ;
      ObSysVars[550].base_value_ = "0" ;
    ObSysVars[550].alias_ = "OB_SV_CONNECTION_CONTROL_FAILED_CONNECTIONS_THRESHOLD" ;
    }();

    [&] (){
      ObSysVars[551].default_value_ = "2147483647" ;
      ObSysVars[551].info_ = "The maximum delay in milliseconds for server response to failed connection attempts, if connection_control_failed_connections_threshold is greater than zero" ;
      ObSysVars[551].name_ = "connection_control_max_connection_delay" ;
      ObSysVars[551].data_type_ = ObIntType ;
      ObSysVars[551].min_val_ = "1000" ;
      ObSysVars[551].max_val_ = "2147483647" ;
      ObSysVars[551].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[551].id_ = SYS_VAR_CONNECTION_CONTROL_MAX_CONNECTION_DELAY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CONNECTION_CONTROL_MAX_CONNECTION_DELAY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CONNECTION_CONTROL_MAX_CONNECTION_DELAY] = 551 ;
      ObSysVars[551].base_value_ = "2147483647" ;
    ObSysVars[551].alias_ = "OB_SV_CONNECTION_CONTROL_MAX_CONNECTION_DELAY" ;
    }();

    [&] (){
      ObSysVars[552].default_value_ = "1000" ;
      ObSysVars[552].info_ = "The minmum delay in milliseconds for server response to failed connection attempts, if connection_control_failed_connections_threshold is greater than zero" ;
      ObSysVars[552].name_ = "connection_control_min_connection_delay" ;
      ObSysVars[552].data_type_ = ObIntType ;
      ObSysVars[552].min_val_ = "1000" ;
      ObSysVars[552].max_val_ = "2147483647" ;
      ObSysVars[552].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[552].id_ = SYS_VAR_CONNECTION_CONTROL_MIN_CONNECTION_DELAY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CONNECTION_CONTROL_MIN_CONNECTION_DELAY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_CONNECTION_CONTROL_MIN_CONNECTION_DELAY] = 552 ;
      ObSysVars[552].base_value_ = "1000" ;
    ObSysVars[552].alias_ = "OB_SV_CONNECTION_CONTROL_MIN_CONNECTION_DELAY" ;
    }();

    [&] (){
      ObSysVars[553].default_value_ = "0" ;
      ObSysVars[553].info_ = "The default mode value to use for the WEEK() function" ;
      ObSysVars[553].name_ = "default_week_format" ;
      ObSysVars[553].data_type_ = ObIntType ;
      ObSysVars[553].min_val_ = "0" ;
      ObSysVars[553].max_val_ = "7" ;
      ObSysVars[553].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[553].id_ = SYS_VAR_DEFAULT_WEEK_FORMAT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_WEEK_FORMAT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_WEEK_FORMAT] = 553 ;
      ObSysVars[553].base_value_ = "0" ;
    ObSysVars[553].alias_ = "OB_SV_DEFAULT_WEEK_FORMAT" ;
    }();

    [&] (){
      ObSysVars[554].default_value_ = "300" ;
      ObSysVars[554].info_ = "" ;
      ObSysVars[554].name_ = "delayed_insert_timeout" ;
      ObSysVars[554].data_type_ = ObIntType ;
      ObSysVars[554].min_val_ = "1" ;
      ObSysVars[554].max_val_ = "31536000" ;
      ObSysVars[554].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[554].id_ = SYS_VAR_DELAYED_INSERT_TIMEOUT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DELAYED_INSERT_TIMEOUT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DELAYED_INSERT_TIMEOUT] = 554 ;
      ObSysVars[554].base_value_ = "300" ;
    ObSysVars[554].alias_ = "OB_SV_DELAYED_INSERT_TIMEOUT" ;
    }();

    [&] (){
      ObSysVars[555].default_value_ = "1000" ;
      ObSysVars[555].info_ = "" ;
      ObSysVars[555].name_ = "delayed_queue_size" ;
      ObSysVars[555].data_type_ = ObUInt64Type ;
      ObSysVars[555].min_val_ = "1" ;
      ObSysVars[555].max_val_ = "18446744073709551615" ;
      ObSysVars[555].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[555].id_ = SYS_VAR_DELAYED_QUEUE_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DELAYED_QUEUE_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DELAYED_QUEUE_SIZE] = 555 ;
      ObSysVars[555].base_value_ = "1000" ;
    ObSysVars[555].alias_ = "OB_SV_DELAYED_QUEUE_SIZE" ;
    }();

    [&] (){
      ObSysVars[556].default_value_ = "200" ;
      ObSysVars[556].info_ = "This variable indicates the number of equality ranges in an equality comparison condition when the optimizer should switch from using index dives to index statistics in estimating the number of qualifying rows" ;
      ObSysVars[556].name_ = "eq_range_index_dive_limit" ;
      ObSysVars[556].data_type_ = ObIntType ;
      ObSysVars[556].min_val_ = "0" ;
      ObSysVars[556].max_val_ = "4294967295" ;
      ObSysVars[556].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[556].id_ = SYS_VAR_EQ_RANGE_INDEX_DIVE_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_EQ_RANGE_INDEX_DIVE_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_EQ_RANGE_INDEX_DIVE_LIMIT] = 556 ;
      ObSysVars[556].base_value_ = "200" ;
    ObSysVars[556].alias_ = "OB_SV_EQ_RANGE_INDEX_DIVE_LIMIT" ;
    }();

    [&] (){
      ObSysVars[557].default_value_ = "1" ;
      ObSysVars[557].info_ = "Causes InnoDB to automatically recalculate persistent statistics after the data in a table is changed substantially, merely simulates MySQL 5.7" ;
      ObSysVars[557].name_ = "innodb_stats_auto_recalc" ;
      ObSysVars[557].data_type_ = ObIntType ;
      ObSysVars[557].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[557].id_ = SYS_VAR_INNODB_STATS_AUTO_RECALC ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_AUTO_RECALC)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_AUTO_RECALC] = 557 ;
      ObSysVars[557].base_value_ = "1" ;
    ObSysVars[557].alias_ = "OB_SV_INNODB_STATS_AUTO_RECALC" ;
    }();

    [&] (){
      ObSysVars[558].default_value_ = "0" ;
      ObSysVars[558].info_ = "When innodb_stats_include_delete_marked is enabled, ANALYZE TABLE considers delete-marked records when recalculating statistics" ;
      ObSysVars[558].name_ = "innodb_stats_include_delete_marked" ;
      ObSysVars[558].data_type_ = ObIntType ;
      ObSysVars[558].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[558].id_ = SYS_VAR_INNODB_STATS_INCLUDE_DELETE_MARKED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_INCLUDE_DELETE_MARKED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_INCLUDE_DELETE_MARKED] = 558 ;
      ObSysVars[558].base_value_ = "0" ;
    ObSysVars[558].alias_ = "OB_SV_INNODB_STATS_INCLUDE_DELETE_MARKED" ;
    }();

    [&] (){
      ObSysVars[559].default_value_ = "0" ;
      ObSysVars[559].info_ = "How the server treats NULL values when collecting statistics about the distribution of index values for InnoDB tables" ;
      ObSysVars[559].name_ = "innodb_stats_method" ;
      ObSysVars[559].data_type_ = ObIntType ;
      ObSysVars[559].enum_names_ = "[u'nulls_equal', u'nulls_unequal', u'nulls_ignored']" ;
      ObSysVars[559].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[559].id_ = SYS_VAR_INNODB_STATS_METHOD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_METHOD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_METHOD] = 559 ;
      ObSysVars[559].base_value_ = "0" ;
    ObSysVars[559].alias_ = "OB_SV_INNODB_STATS_METHOD" ;
    }();

    [&] (){
      ObSysVars[560].default_value_ = "0" ;
      ObSysVars[560].info_ = "When innodb_stats_on_metadata is enabled, InnoDB updates non-persistent statistics when metadata statements such as SHOW TABLE STATUS or when accessing the Information Schema TABLES or STATISTICS tables" ;
      ObSysVars[560].name_ = "innodb_stats_on_metadata" ;
      ObSysVars[560].data_type_ = ObIntType ;
      ObSysVars[560].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[560].id_ = SYS_VAR_INNODB_STATS_ON_METADATA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_ON_METADATA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_ON_METADATA] = 560 ;
      ObSysVars[560].base_value_ = "0" ;
    ObSysVars[560].alias_ = "OB_SV_INNODB_STATS_ON_METADATA" ;
    }();

    [&] (){
      ObSysVars[561].default_value_ = "" ;
      ObSysVars[561].info_ = "The session value of this variable specifies the client version token list and indicates the tokens that the client session requires the server version token list to have, merely simulates MySQL 5.7" ;
      ObSysVars[561].name_ = "version_tokens_session" ;
      ObSysVars[561].data_type_ = ObVarcharType ;
      ObSysVars[561].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[561].id_ = SYS_VAR_VERSION_TOKENS_SESSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION_TOKENS_SESSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VERSION_TOKENS_SESSION] = 561 ;
      ObSysVars[561].base_value_ = "" ;
    ObSysVars[561].alias_ = "OB_SV_VERSION_TOKENS_SESSION" ;
    }();

    [&] (){
      ObSysVars[562].default_value_ = "20" ;
      ObSysVars[562].info_ = "The number of index pages to sample when estimating cardinality and other statistics for an indexed column, such as those calculated by ANALYZE TABLE" ;
      ObSysVars[562].name_ = "innodb_stats_persistent_sample_pages" ;
      ObSysVars[562].data_type_ = ObUInt64Type ;
      ObSysVars[562].min_val_ = "1" ;
      ObSysVars[562].max_val_ = "18446744073709551615" ;
      ObSysVars[562].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[562].id_ = SYS_VAR_INNODB_STATS_PERSISTENT_SAMPLE_PAGES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_PERSISTENT_SAMPLE_PAGES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_PERSISTENT_SAMPLE_PAGES] = 562 ;
      ObSysVars[562].base_value_ = "20" ;
    ObSysVars[562].alias_ = "OB_SV_INNODB_STATS_PERSISTENT_SAMPLE_PAGES" ;
    }();

    [&] (){
      ObSysVars[563].default_value_ = "8" ;
      ObSysVars[563].info_ = "The number of index pages to sample when estimating cardinality and other statistics for an indexed column, such as those calculated by ANALYZE TABLE" ;
      ObSysVars[563].name_ = "innodb_stats_sample_pages" ;
      ObSysVars[563].data_type_ = ObUInt64Type ;
      ObSysVars[563].min_val_ = "1" ;
      ObSysVars[563].max_val_ = "18446744073709551615" ;
      ObSysVars[563].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[563].id_ = SYS_VAR_INNODB_STATS_SAMPLE_PAGES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_SAMPLE_PAGES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_SAMPLE_PAGES] = 563 ;
      ObSysVars[563].base_value_ = "8" ;
    ObSysVars[563].alias_ = "OB_SV_INNODB_STATS_SAMPLE_PAGES" ;
    }();

    [&] (){
      ObSysVars[564].default_value_ = "8" ;
      ObSysVars[564].info_ = "The number of index pages to sample when estimating cardinality and other statistics for an indexed column, such as those calculated by ANALYZE TABLE" ;
      ObSysVars[564].name_ = "innodb_stats_transient_sample_pages" ;
      ObSysVars[564].data_type_ = ObUInt64Type ;
      ObSysVars[564].min_val_ = "1" ;
      ObSysVars[564].max_val_ = "18446744073709551615" ;
      ObSysVars[564].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[564].id_ = SYS_VAR_INNODB_STATS_TRANSIENT_SAMPLE_PAGES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INNODB_STATS_TRANSIENT_SAMPLE_PAGES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_INNODB_STATS_TRANSIENT_SAMPLE_PAGES] = 564 ;
      ObSysVars[564].base_value_ = "8" ;
    ObSysVars[564].alias_ = "OB_SV_INNODB_STATS_TRANSIENT_SAMPLE_PAGES" ;
    }();

    [&] (){
      ObSysVars[565].default_value_ = "" ;
      ObSysVars[565].info_ = "The customer master key (CMK) ID obtained from the AWS KMS server and used by the keyring_aws plugin" ;
      ObSysVars[565].name_ = "keyring_aws_cmk_id" ;
      ObSysVars[565].data_type_ = ObVarcharType ;
      ObSysVars[565].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[565].id_ = SYS_VAR_KEYRING_AWS_CMK_ID ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_AWS_CMK_ID)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_AWS_CMK_ID] = 565 ;
      ObSysVars[565].base_value_ = "" ;
    ObSysVars[565].alias_ = "OB_SV_KEYRING_AWS_CMK_ID" ;
    }();

    [&] (){
      ObSysVars[566].default_value_ = "19" ;
      ObSysVars[566].info_ = "The AWS region for the keyring_aws plugin. This variable is unavailable unless that plugin is installed" ;
      ObSysVars[566].name_ = "keyring_aws_region" ;
      ObSysVars[566].data_type_ = ObIntType ;
      ObSysVars[566].enum_names_ = "[u'af-south-1', u'ap-east-1', u'ap-northeast-1', u'ap-northeast-2', u'ap-northeast-3', u'ap-south-1', u'ap-southeast-1', u'ap-southeast-2', u'ca-central-1', u'cn-north-1', u'cn-northwest-1', u'eu-central-1', u'eu-north-1', u'eu-south-1', u'eu-west-1', u'eu-west-2', u'eu-west-3', u'me-south-1', u'sa-east-1', u'us-east-1', u'us-east-2', u'us-gov-east-1', u'us-iso-east-1', u'us-iso-west-1', u'us-isob-east-1', u'us-west-1', u'us-west-2']" ;
      ObSysVars[566].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[566].id_ = SYS_VAR_KEYRING_AWS_REGION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_AWS_REGION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_AWS_REGION] = 566 ;
      ObSysVars[566].base_value_ = "19" ;
    ObSysVars[566].alias_ = "OB_SV_KEYRING_AWS_REGION" ;
    }();

    [&] (){
      ObSysVars[567].default_value_ = "" ;
      ObSysVars[567].info_ = "The path name of the data file used for secure data storage by the keyring_encrypted_file plugin" ;
      ObSysVars[567].name_ = "keyring_encrypted_file_data" ;
      ObSysVars[567].data_type_ = ObVarcharType ;
      ObSysVars[567].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[567].id_ = SYS_VAR_KEYRING_ENCRYPTED_FILE_DATA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_ENCRYPTED_FILE_DATA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_ENCRYPTED_FILE_DATA] = 567 ;
      ObSysVars[567].base_value_ = "" ;
    ObSysVars[567].alias_ = "OB_SV_KEYRING_ENCRYPTED_FILE_DATA" ;
    }();

    [&] (){
      ObSysVars[568].default_value_ = "" ;
      ObSysVars[568].info_ = "The password used by the keyring_encrypted_file pluginn" ;
      ObSysVars[568].name_ = "keyring_encrypted_file_password" ;
      ObSysVars[568].data_type_ = ObVarcharType ;
      ObSysVars[568].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[568].id_ = SYS_VAR_KEYRING_ENCRYPTED_FILE_PASSWORD ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_ENCRYPTED_FILE_PASSWORD)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_ENCRYPTED_FILE_PASSWORD] = 568 ;
      ObSysVars[568].base_value_ = "" ;
    ObSysVars[568].alias_ = "OB_SV_KEYRING_ENCRYPTED_FILE_PASSWORD" ;
    }();

    [&] (){
      ObSysVars[569].default_value_ = "" ;
      ObSysVars[569].info_ = "The path name of the data file used for secure data storage by the keyring_file plugin" ;
      ObSysVars[569].name_ = "keyring_file_data" ;
      ObSysVars[569].data_type_ = ObVarcharType ;
      ObSysVars[569].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[569].id_ = SYS_VAR_KEYRING_FILE_DATA ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_FILE_DATA)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_FILE_DATA] = 569 ;
      ObSysVars[569].base_value_ = "" ;
    ObSysVars[569].alias_ = "OB_SV_KEYRING_FILE_DATA" ;
    }();

    [&] (){
      ObSysVars[570].default_value_ = "" ;
      ObSysVars[570].info_ = "The path name of the directory that stores configuration information used by the keyring_okv plugin" ;
      ObSysVars[570].name_ = "keyring_okv_conf_dir" ;
      ObSysVars[570].data_type_ = ObVarcharType ;
      ObSysVars[570].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[570].id_ = SYS_VAR_KEYRING_OKV_CONF_DIR ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_OKV_CONF_DIR)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_OKV_CONF_DIR] = 570 ;
      ObSysVars[570].base_value_ = "" ;
    ObSysVars[570].alias_ = "OB_SV_KEYRING_OKV_CONF_DIR" ;
    }();

    [&] (){
      ObSysVars[571].default_value_ = "1" ;
      ObSysVars[571].info_ = "Whether keyring operations are enabled. This variable is used during key migration operations" ;
      ObSysVars[571].name_ = "keyring_operations" ;
      ObSysVars[571].data_type_ = ObIntType ;
      ObSysVars[571].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[571].id_ = SYS_VAR_KEYRING_OPERATIONS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_KEYRING_OPERATIONS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_KEYRING_OPERATIONS] = 571 ;
      ObSysVars[571].base_value_ = "1" ;
    ObSysVars[571].alias_ = "OB_SV_KEYRING_OPERATIONS" ;
    }();

    [&] (){
      ObSysVars[572].default_value_ = "" ;
      ObSysVars[572].info_ = "enables control over optimizer behavior" ;
      ObSysVars[572].name_ = "optimizer_switch" ;
      ObSysVars[572].data_type_ = ObVarcharType ;
      ObSysVars[572].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[572].id_ = SYS_VAR_OPTIMIZER_SWITCH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_SWITCH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_SWITCH] = 572 ;
      ObSysVars[572].base_value_ = "" ;
    ObSysVars[572].alias_ = "OB_SV_OPTIMIZER_SWITCH" ;
    }();

    [&] (){
      ObSysVars[573].default_value_ = "100" ;
      ObSysVars[573].info_ = "After max_connect_errors successive connection requests from a host are interrupted without a successful connection, the server blocks that host from further connections" ;
      ObSysVars[573].name_ = "max_connect_errors" ;
      ObSysVars[573].data_type_ = ObUInt64Type ;
      ObSysVars[573].min_val_ = "1" ;
      ObSysVars[573].max_val_ = "18446744073709551615" ;
      ObSysVars[573].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[573].id_ = SYS_VAR_MAX_CONNECT_ERRORS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_CONNECT_ERRORS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MAX_CONNECT_ERRORS] = 573 ;
      ObSysVars[573].base_value_ = "100" ;
    ObSysVars[573].alias_ = "OB_SV_MAX_CONNECT_ERRORS" ;
    }();

    [&] (){
      ObSysVars[574].default_value_ = "0" ;
      ObSysVars[574].info_ = "Whether MySQL Enterprise Firewall is enabled (the default) or disabled" ;
      ObSysVars[574].name_ = "mysql_firewall_mode" ;
      ObSysVars[574].data_type_ = ObIntType ;
      ObSysVars[574].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[574].id_ = SYS_VAR_MYSQL_FIREWALL_MODE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQL_FIREWALL_MODE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQL_FIREWALL_MODE] = 574 ;
      ObSysVars[574].base_value_ = "0" ;
    ObSysVars[574].alias_ = "OB_SV_MYSQL_FIREWALL_MODE" ;
    }();

    [&] (){
      ObSysVars[575].default_value_ = "0" ;
      ObSysVars[575].info_ = "Whether the MySQL Enterprise Firewall trace is enabled or disabled (the default)" ;
      ObSysVars[575].name_ = "mysql_firewall_trace" ;
      ObSysVars[575].data_type_ = ObIntType ;
      ObSysVars[575].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[575].id_ = SYS_VAR_MYSQL_FIREWALL_TRACE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQL_FIREWALL_TRACE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQL_FIREWALL_TRACE] = 575 ;
      ObSysVars[575].base_value_ = "0" ;
    ObSysVars[575].alias_ = "OB_SV_MYSQL_FIREWALL_TRACE" ;
    }();

    [&] (){
      ObSysVars[576].default_value_ = "0" ;
      ObSysVars[576].info_ = "This variable controls whether the mysql_native_password built-in authentication plugin supports proxy users" ;
      ObSysVars[576].name_ = "mysql_native_password_proxy_users" ;
      ObSysVars[576].data_type_ = ObIntType ;
      ObSysVars[576].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[576].id_ = SYS_VAR_MYSQL_NATIVE_PASSWORD_PROXY_USERS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MYSQL_NATIVE_PASSWORD_PROXY_USERS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_MYSQL_NATIVE_PASSWORD_PROXY_USERS] = 576 ;
      ObSysVars[576].base_value_ = "0" ;
    ObSysVars[576].alias_ = "OB_SV_MYSQL_NATIVE_PASSWORD_PROXY_USERS" ;
    }();

    [&] (){
      ObSysVars[577].default_value_ = "10" ;
      ObSysVars[577].info_ = "If a read or write on a communication port is interrupted, retry this many times before giving up. This value should be set quite high on FreeBSD because internal interrupts are sent to all threads" ;
      ObSysVars[577].name_ = "net_retry_count" ;
      ObSysVars[577].data_type_ = ObUInt64Type ;
      ObSysVars[577].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[577].id_ = SYS_VAR_NET_RETRY_COUNT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NET_RETRY_COUNT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NET_RETRY_COUNT] = 577 ;
      ObSysVars[577].base_value_ = "10" ;
    ObSysVars[577].alias_ = "OB_SV_NET_RETRY_COUNT" ;
    }();

    [&] (){
      ObSysVars[578].default_value_ = "0" ;
      ObSysVars[578].info_ = "This variable was used in MySQL 4.0 to turn on some 4.1 behaviors, and is retained for backward compatibility. Its value is always OFF" ;
      ObSysVars[578].name_ = "new" ;
      ObSysVars[578].data_type_ = ObIntType ;
      ObSysVars[578].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[578].id_ = SYS_VAR_NEW ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NEW)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NEW] = 578 ;
      ObSysVars[578].base_value_ = "0" ;
    ObSysVars[578].alias_ = "OB_SV_NEW" ;
    }();

    [&] (){
      ObSysVars[579].default_value_ = "0" ;
      ObSysVars[579].info_ = "This variable controls the password hashing method used by the PASSWORD() function. It also influences password hashing performed by CREATE USER and GRANT statements that specify a password using an IDENTIFIED BY clause" ;
      ObSysVars[579].name_ = "old_passwords" ;
      ObSysVars[579].data_type_ = ObIntType ;
      ObSysVars[579].enum_names_ = "[u'0', u'1', u'2']" ;
      ObSysVars[579].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[579].id_ = SYS_VAR_OLD_PASSWORDS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OLD_PASSWORDS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OLD_PASSWORDS] = 579 ;
      ObSysVars[579].base_value_ = "0" ;
    ObSysVars[579].alias_ = "OB_SV_OLD_PASSWORDS" ;
    }();

    [&] (){
      ObSysVars[580].default_value_ = "1" ;
      ObSysVars[580].info_ = "Controls the heuristics applied during query optimization to prune less-promising partial plans from the optimizer search space" ;
      ObSysVars[580].name_ = "optimizer_prune_level" ;
      ObSysVars[580].data_type_ = ObIntType ;
      ObSysVars[580].min_val_ = "0" ;
      ObSysVars[580].max_val_ = "1" ;
      ObSysVars[580].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[580].id_ = SYS_VAR_OPTIMIZER_PRUNE_LEVEL ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_PRUNE_LEVEL)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_PRUNE_LEVEL] = 580 ;
      ObSysVars[580].base_value_ = "1" ;
    ObSysVars[580].alias_ = "OB_SV_OPTIMIZER_PRUNE_LEVEL" ;
    }();

    [&] (){
      ObSysVars[581].default_value_ = "62" ;
      ObSysVars[581].info_ = "The maximum depth of search performed by the query optimizer" ;
      ObSysVars[581].name_ = "optimizer_search_depth" ;
      ObSysVars[581].data_type_ = ObIntType ;
      ObSysVars[581].min_val_ = "0" ;
      ObSysVars[581].max_val_ = "62" ;
      ObSysVars[581].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[581].id_ = SYS_VAR_OPTIMIZER_SEARCH_DEPTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_SEARCH_DEPTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_SEARCH_DEPTH] = 581 ;
      ObSysVars[581].base_value_ = "62" ;
    ObSysVars[581].alias_ = "OB_SV_OPTIMIZER_SEARCH_DEPTH" ;
    }();

    [&] (){
      ObSysVars[582].default_value_ = "" ;
      ObSysVars[582].info_ = "This variable controls optimizer tracing" ;
      ObSysVars[582].name_ = "optimizer_trace" ;
      ObSysVars[582].data_type_ = ObVarcharType ;
      ObSysVars[582].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[582].id_ = SYS_VAR_OPTIMIZER_TRACE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_TRACE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_TRACE] = 582 ;
      ObSysVars[582].base_value_ = "" ;
    ObSysVars[582].alias_ = "OB_SV_OPTIMIZER_TRACE" ;
    }();

    [&] (){
      ObSysVars[583].default_value_ = "" ;
      ObSysVars[583].info_ = "This variable enables or disables selected optimizer tracing features" ;
      ObSysVars[583].name_ = "optimizer_trace_features" ;
      ObSysVars[583].data_type_ = ObVarcharType ;
      ObSysVars[583].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[583].id_ = SYS_VAR_OPTIMIZER_TRACE_FEATURES ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_TRACE_FEATURES)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_TRACE_FEATURES] = 583 ;
      ObSysVars[583].base_value_ = "" ;
    ObSysVars[583].alias_ = "OB_SV_OPTIMIZER_TRACE_FEATURES" ;
    }();

    [&] (){
      ObSysVars[584].default_value_ = "1" ;
      ObSysVars[584].info_ = "The maximum number of optimizer traces to display" ;
      ObSysVars[584].name_ = "optimizer_trace_limit" ;
      ObSysVars[584].data_type_ = ObIntType ;
      ObSysVars[584].min_val_ = "0" ;
      ObSysVars[584].max_val_ = "2147483647" ;
      ObSysVars[584].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[584].id_ = SYS_VAR_OPTIMIZER_TRACE_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_TRACE_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_TRACE_LIMIT] = 584 ;
      ObSysVars[584].base_value_ = "1" ;
    ObSysVars[584].alias_ = "OB_SV_OPTIMIZER_TRACE_LIMIT" ;
    }();

    [&] (){
      ObSysVars[585].default_value_ = "16384" ;
      ObSysVars[585].info_ = "The maximum cumulative size of stored optimizer traces" ;
      ObSysVars[585].name_ = "optimizer_trace_max_mem_size" ;
      ObSysVars[585].data_type_ = ObUInt64Type ;
      ObSysVars[585].min_val_ = "0" ;
      ObSysVars[585].max_val_ = "4294967295" ;
      ObSysVars[585].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[585].id_ = SYS_VAR_OPTIMIZER_TRACE_MAX_MEM_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_TRACE_MAX_MEM_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_TRACE_MAX_MEM_SIZE] = 585 ;
      ObSysVars[585].base_value_ = "16384" ;
    ObSysVars[585].alias_ = "OB_SV_OPTIMIZER_TRACE_MAX_MEM_SIZE" ;
    }();

    [&] (){
      ObSysVars[586].default_value_ = "-1" ;
      ObSysVars[586].info_ = "The offset of optimizer traces to display" ;
      ObSysVars[586].name_ = "optimizer_trace_offset" ;
      ObSysVars[586].data_type_ = ObIntType ;
      ObSysVars[586].min_val_ = "-2147483647" ;
      ObSysVars[586].max_val_ = "2147483647" ;
      ObSysVars[586].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[586].id_ = SYS_VAR_OPTIMIZER_TRACE_OFFSET ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_TRACE_OFFSET)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_TRACE_OFFSET] = 586 ;
      ObSysVars[586].base_value_ = "-1" ;
    ObSysVars[586].alias_ = "OB_SV_OPTIMIZER_TRACE_OFFSET" ;
    }();

    [&] (){
      ObSysVars[587].default_value_ = "18446744073709551615" ;
      ObSysVars[587].info_ = "The maximum amount of memory available to the parser" ;
      ObSysVars[587].name_ = "parser_max_mem_size" ;
      ObSysVars[587].data_type_ = ObUInt64Type ;
      ObSysVars[587].min_val_ = "10000000" ;
      ObSysVars[587].max_val_ = "18446744073709551615" ;
      ObSysVars[587].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[587].id_ = SYS_VAR_PARSER_MAX_MEM_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PARSER_MAX_MEM_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_PARSER_MAX_MEM_SIZE] = 587 ;
      ObSysVars[587].base_value_ = "18446744073709551615" ;
    ObSysVars[587].alias_ = "OB_SV_PARSER_MAX_MEM_SIZE" ;
    }();

    [&] (){
      ObSysVars[588].default_value_ = "0" ;
      ObSysVars[588].info_ = "For statements that invoke RAND(), the source passes two values to the replica, where they are used to seed the random number generator" ;
      ObSysVars[588].name_ = "rand_seed1" ;
      ObSysVars[588].data_type_ = ObUInt64Type ;
      ObSysVars[588].min_val_ = "0" ;
      ObSysVars[588].max_val_ = "4294967295" ;
      ObSysVars[588].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[588].id_ = SYS_VAR_RAND_SEED1 ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RAND_SEED1)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RAND_SEED1] = 588 ;
      ObSysVars[588].base_value_ = "0" ;
    ObSysVars[588].alias_ = "OB_SV_RAND_SEED1" ;
    }();

    [&] (){
      ObSysVars[589].default_value_ = "0" ;
      ObSysVars[589].info_ = "For statements that invoke RAND(), the source passes two values to the replica, where they are used to seed the random number generator" ;
      ObSysVars[589].name_ = "rand_seed2" ;
      ObSysVars[589].data_type_ = ObUInt64Type ;
      ObSysVars[589].min_val_ = "0" ;
      ObSysVars[589].max_val_ = "4294967295" ;
      ObSysVars[589].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[589].id_ = SYS_VAR_RAND_SEED2 ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RAND_SEED2)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RAND_SEED2] = 589 ;
      ObSysVars[589].base_value_ = "0" ;
    ObSysVars[589].alias_ = "OB_SV_RAND_SEED2" ;
    }();

    [&] (){
      ObSysVars[590].default_value_ = "4096" ;
      ObSysVars[590].info_ = "The size in bytes of blocks that are allocated when doing range optimization" ;
      ObSysVars[590].name_ = "range_alloc_block_size" ;
      ObSysVars[590].data_type_ = ObUInt64Type ;
      ObSysVars[590].min_val_ = "4096" ;
      ObSysVars[590].max_val_ = "18446744073709550592" ;
      ObSysVars[590].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[590].id_ = SYS_VAR_RANGE_ALLOC_BLOCK_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RANGE_ALLOC_BLOCK_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RANGE_ALLOC_BLOCK_SIZE] = 590 ;
      ObSysVars[590].base_value_ = "4096" ;
    ObSysVars[590].alias_ = "OB_SV_RANGE_ALLOC_BLOCK_SIZE" ;
    }();

    [&] (){
      ObSysVars[591].default_value_ = "8388608" ;
      ObSysVars[591].info_ = "The limit on memory consumption for the range optimizer" ;
      ObSysVars[591].name_ = "range_optimizer_max_mem_size" ;
      ObSysVars[591].data_type_ = ObUInt64Type ;
      ObSysVars[591].min_val_ = "0" ;
      ObSysVars[591].max_val_ = "18446744073709551615" ;
      ObSysVars[591].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[591].id_ = SYS_VAR_RANGE_OPTIMIZER_MAX_MEM_SIZE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RANGE_OPTIMIZER_MAX_MEM_SIZE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_RANGE_OPTIMIZER_MAX_MEM_SIZE] = 591 ;
      ObSysVars[591].base_value_ = "8388608" ;
    ObSysVars[591].alias_ = "OB_SV_RANGE_OPTIMIZER_MAX_MEM_SIZE" ;
    }();

    [&] (){
      ObSysVars[592].default_value_ = "1" ;
      ObSysVars[592].info_ = "Whether the Rewriter query rewrite plugin is enabled" ;
      ObSysVars[592].name_ = "rewriter_enabled" ;
      ObSysVars[592].data_type_ = ObIntType ;
      ObSysVars[592].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[592].id_ = SYS_VAR_REWRITER_ENABLED ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_REWRITER_ENABLED)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_REWRITER_ENABLED] = 592 ;
      ObSysVars[592].base_value_ = "1" ;
    ObSysVars[592].alias_ = "OB_SV_REWRITER_ENABLED" ;
    }();

    [&] (){
      ObSysVars[593].default_value_ = "0" ;
      ObSysVars[593].info_ = "For internal use in MySQL" ;
      ObSysVars[593].name_ = "rewriter_verbose" ;
      ObSysVars[593].data_type_ = ObIntType ;
      ObSysVars[593].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[593].id_ = SYS_VAR_REWRITER_VERBOSE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_REWRITER_VERBOSE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_REWRITER_VERBOSE] = 593 ;
      ObSysVars[593].base_value_ = "0" ;
    ObSysVars[593].alias_ = "OB_SV_REWRITER_VERBOSE" ;
    }();

    [&] (){
      ObSysVars[594].default_value_ = "1" ;
      ObSysVars[594].info_ = "If this variable is enabled, the server blocks connections by clients that attempt to use accounts that have passwords stored in the old (pre-4.1) format" ;
      ObSysVars[594].name_ = "secure_auth" ;
      ObSysVars[594].data_type_ = ObIntType ;
      ObSysVars[594].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[594].id_ = SYS_VAR_SECURE_AUTH ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SECURE_AUTH)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SECURE_AUTH] = 594 ;
      ObSysVars[594].base_value_ = "1" ;
    ObSysVars[594].alias_ = "OB_SV_SECURE_AUTH" ;
    }();

    [&] (){
      ObSysVars[595].default_value_ = "0" ;
      ObSysVars[595].info_ = "This variable controls whether the sha256_password built-in authentication plugin supports proxy users" ;
      ObSysVars[595].name_ = "sha256_password_proxy_users" ;
      ObSysVars[595].data_type_ = ObIntType ;
      ObSysVars[595].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[595].id_ = SYS_VAR_SHA256_PASSWORD_PROXY_USERS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SHA256_PASSWORD_PROXY_USERS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SHA256_PASSWORD_PROXY_USERS] = 595 ;
      ObSysVars[595].base_value_ = "0" ;
    ObSysVars[595].alias_ = "OB_SV_SHA256_PASSWORD_PROXY_USERS" ;
    }();

    [&] (){
      ObSysVars[596].default_value_ = "0" ;
      ObSysVars[596].info_ = "which affects whether MySQL 5.6 compatibility is enabled with respect to how system and status variable information is provided by the INFORMATION_SCHEMA and Performance Schema tables, and also by the SHOW VARIABLES and SHOW STATUS statements" ;
      ObSysVars[596].name_ = "show_compatibility_56" ;
      ObSysVars[596].data_type_ = ObIntType ;
      ObSysVars[596].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[596].id_ = SYS_VAR_SHOW_COMPATIBILITY_56 ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SHOW_COMPATIBILITY_56)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SHOW_COMPATIBILITY_56] = 596 ;
      ObSysVars[596].base_value_ = "0" ;
    ObSysVars[596].alias_ = "OB_SV_SHOW_COMPATIBILITY_56" ;
    }();

    [&] (){
      ObSysVars[597].default_value_ = "0" ;
      ObSysVars[597].info_ = "Enabling this variable causes SHOW CREATE TABLE to display ROW_FORMAT regardless of whether it is the default format" ;
      ObSysVars[597].name_ = "show_create_table_verbosity" ;
      ObSysVars[597].data_type_ = ObIntType ;
      ObSysVars[597].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[597].id_ = SYS_VAR_SHOW_CREATE_TABLE_VERBOSITY ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SHOW_CREATE_TABLE_VERBOSITY)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SHOW_CREATE_TABLE_VERBOSITY] = 597 ;
      ObSysVars[597].base_value_ = "0" ;
    ObSysVars[597].alias_ = "OB_SV_SHOW_CREATE_TABLE_VERBOSITY" ;
    }();

    [&] (){
      ObSysVars[598].default_value_ = "0" ;
      ObSysVars[598].info_ = "Whether SHOW CREATE TABLE output includes comments" ;
      ObSysVars[598].name_ = "show_old_temporals" ;
      ObSysVars[598].data_type_ = ObIntType ;
      ObSysVars[598].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[598].id_ = SYS_VAR_SHOW_OLD_TEMPORALS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SHOW_OLD_TEMPORALS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SHOW_OLD_TEMPORALS] = 598 ;
      ObSysVars[598].base_value_ = "0" ;
    ObSysVars[598].alias_ = "OB_SV_SHOW_OLD_TEMPORALS" ;
    }();

    [&] (){
      ObSysVars[599].default_value_ = "1" ;
      ObSysVars[599].info_ = "If set to OFF, MySQL aborts SELECT statements that are likely to take a very long time to execute" ;
      ObSysVars[599].name_ = "sql_big_selects" ;
      ObSysVars[599].data_type_ = ObIntType ;
      ObSysVars[599].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[599].id_ = SYS_VAR_SQL_BIG_SELECTS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_BIG_SELECTS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_SQL_BIG_SELECTS] = 599 ;
      ObSysVars[599].base_value_ = "1" ;
    ObSysVars[599].alias_ = "OB_SV_SQL_BIG_SELECTS" ;
    }();

    [&] (){
      ObSysVars[600].default_value_ = "1" ;
      ObSysVars[600].info_ = "This variable controls whether updates to a view can be made when the view does not contain all columns of the primary key defined in the underlying table, if the update statement contains a LIMIT clause" ;
      ObSysVars[600].name_ = "updatable_views_with_limit" ;
      ObSysVars[600].data_type_ = ObIntType ;
      ObSysVars[600].enum_names_ = "[u'OFF', u'ON', u'NO', u'YES']" ;
      ObSysVars[600].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[600].id_ = SYS_VAR_UPDATABLE_VIEWS_WITH_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_UPDATABLE_VIEWS_WITH_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_UPDATABLE_VIEWS_WITH_LIMIT] = 600 ;
      ObSysVars[600].base_value_ = "1" ;
    ObSysVars[600].alias_ = "OB_SV_UPDATABLE_VIEWS_WITH_LIMIT" ;
    }();

    [&] (){
      ObSysVars[601].default_value_ = "" ;
      ObSysVars[601].info_ = "The path name of the dictionary file that validate_password uses for checking passwords." ;
      ObSysVars[601].name_ = "validate_password_dictionary_file" ;
      ObSysVars[601].data_type_ = ObVarcharType ;
      ObSysVars[601].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[601].id_ = SYS_VAR_VALIDATE_PASSWORD_DICTIONARY_FILE ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_DICTIONARY_FILE)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_DICTIONARY_FILE] = 601 ;
      ObSysVars[601].base_value_ = "" ;
    ObSysVars[601].alias_ = "OB_SV_VALIDATE_PASSWORD_DICTIONARY_FILE" ;
    }();

    [&] (){
      ObSysVars[602].default_value_ = "100" ;
      ObSysVars[602].info_ = "" ;
      ObSysVars[602].name_ = "delayed_insert_limit" ;
      ObSysVars[602].data_type_ = ObUInt64Type ;
      ObSysVars[602].min_val_ = "1" ;
      ObSysVars[602].max_val_ = "18446744073709551615" ;
      ObSysVars[602].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY ;
      ObSysVars[602].id_ = SYS_VAR_DELAYED_INSERT_LIMIT ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DELAYED_INSERT_LIMIT)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_DELAYED_INSERT_LIMIT] = 602 ;
      ObSysVars[602].base_value_ = "100" ;
    ObSysVars[602].alias_ = "OB_SV_DELAYED_INSERT_LIMIT" ;
    }();

    [&] (){
      ObSysVars[603].default_value_ = "" ;
      ObSysVars[603].info_ = "NDB engine version in ndb-x.y.z format" ;
      ObSysVars[603].name_ = "ndb_version" ;
      ObSysVars[603].data_type_ = ObVarcharType ;
      ObSysVars[603].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[603].id_ = SYS_VAR_NDB_VERSION ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NDB_VERSION)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_NDB_VERSION] = 603 ;
      ObSysVars[603].base_value_ = "" ;
    ObSysVars[603].alias_ = "OB_SV_NDB_VERSION" ;
    }();

    [&] (){
      ObSysVars[604].default_value_ = "1" ;
      ObSysVars[604].info_ = "This variable is available if the server was compiled using OpenSSL. It controls whether the server autogenerates SSL key and certificate files in the data directory, if they do not already exist" ;
      ObSysVars[604].name_ = "auto_generate_certs" ;
      ObSysVars[604].data_type_ = ObIntType ;
      ObSysVars[604].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::MYSQL_ONLY | ObSysVarFlag::READONLY ;
      ObSysVars[604].id_ = SYS_VAR_AUTO_GENERATE_CERTS ;
      cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTO_GENERATE_CERTS)) ;
      ObSysVarsIdToArrayIdx[SYS_VAR_AUTO_GENERATE_CERTS] = 604 ;
      ObSysVars[604].base_value_ = "1" ;
    ObSysVars[604].alias_ = "OB_SV_AUTO_GENERATE_CERTS" ;
    }();

    if (cur_max_var_id >= ObSysVarFactory::OB_MAX_SYS_VAR_ID) { 
      HasInvalidSysVar = true;
    }
  }
}vars_init;

static int64_t var_amount = 605;

int64_t ObSysVariables::get_all_sys_var_count(){ return ObSysVarFactory::ALL_SYS_VARS_COUNT;}
ObSysVarClassType ObSysVariables::get_sys_var_id(int64_t i){ return ObSysVars[i].id_;}
ObString ObSysVariables::get_name(int64_t i){ return ObSysVars[i].name_;}
ObObjType ObSysVariables::get_type(int64_t i){ return ObSysVars[i].data_type_;}
ObString ObSysVariables::get_value(int64_t i){ return ObSysVars[i].default_value_;}
ObString ObSysVariables::get_base_str_value(int64_t i){ return ObSysVars[i].base_value_;}
ObString ObSysVariables::get_min(int64_t i){ return ObSysVars[i].min_val_;}
ObString ObSysVariables::get_max(int64_t i){ return ObSysVars[i].max_val_;}
ObString ObSysVariables::get_info(int64_t i){ return ObSysVars[i].info_;}
int64_t ObSysVariables::get_flags(int64_t i){ return ObSysVars[i].flags_;}
bool ObSysVariables::need_serialize(int64_t i){ return ObSysVars[i].flags_ & ObSysVarFlag::NEED_SERIALIZE;}
bool ObSysVariables::is_oracle_only(int64_t i){ return ObSysVars[i].flags_ & ObSysVarFlag::ORACLE_ONLY;}
bool ObSysVariables::is_mysql_only(int64_t i){ return ObSysVars[i].flags_ & ObSysVarFlag::MYSQL_ONLY;}
ObString ObSysVariables::get_alias(int64_t i){ return ObSysVars[i].alias_;}
const ObObj &ObSysVariables::get_default_value(int64_t i){ return ObSysVarDefaultValues[i];}
const ObObj &ObSysVariables::get_base_value(int64_t i){ return ObSysVarBaseValues[i];}
int64_t ObSysVariables::get_amount(){ return var_amount;}
ObCollationType ObSysVariables::get_default_sysvar_collation() { return CS_TYPE_UTF8MB4_GENERAL_CI;}

int ObSysVariables::set_value(const char *name, const char * new_value)
{
  ObString tmp_name(static_cast<int32_t>(strlen(name)), name);
  ObString tmp_value(static_cast<int32_t>(strlen(new_value)), new_value);
  return set_value(tmp_name, tmp_value);
}
int ObSysVariables::set_value(const common::ObString &name, const common::ObString &new_value)
{
  int ret = OB_SUCCESS;
  bool name_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && false == name_exist && i < var_amount; ++i){
    if (0 == ObSysVars[i].name_.compare(name)) {
      ObSysVars[i].default_value_.assign_ptr(new_value.ptr(), new_value.length());
      name_exist = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (false == name_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObSysVariables::set_base_value(const char *name, const char * new_value)
{
  ObString tmp_name(static_cast<int32_t>(strlen(name)), name);
  ObString tmp_value(static_cast<int32_t>(strlen(new_value)), new_value);
  return set_base_value(tmp_name, tmp_value);
}
int ObSysVariables::set_base_value(const common::ObString &name, const common::ObString &new_value)
{
  int ret = OB_SUCCESS;
  bool name_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && false == name_exist && i < var_amount; ++i){
    if (0 == ObSysVars[i].name_.compare(name)) {
      ObSysVars[i].base_value_.assign_ptr(new_value.ptr(), new_value.length());
      name_exist = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (false == name_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

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

int64_t ObSysVarsToIdxMap::get_store_idx(int64_t var_id){ return ObSysVarsIdToArrayIdx[var_id];}
bool ObSysVarsToIdxMap::has_invalid_sys_var_id(){ return HasInvalidSysVar;}

} // end namespace share
} // end namespace oceanbase

