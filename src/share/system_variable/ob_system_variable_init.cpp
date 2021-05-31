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

namespace oceanbase {
namespace share {
static ObSysVarFromJson ObSysVars[ObSysVarFactory::ALL_SYS_VARS_COUNT];
static ObObj ObSysVarDefaultValues[ObSysVarFactory::ALL_SYS_VARS_COUNT];
static ObArenaAllocator ObSysVarAllocator(ObModIds::OB_COMMON_SYS_VAR_DEFAULT_VALUE);
static int64_t ObSysVarsIdToArrayIdx[ObSysVarFactory::OB_MAX_SYS_VAR_ID];
static bool HasInvalidSysVar = false;

static struct VarsInit {
  VarsInit()
  {
    int64_t cur_max_var_id = 0;
    // default value of ObSysVarsIdToArrayIdx is -1, means invalid index.
    memset(ObSysVarsIdToArrayIdx, -1, sizeof(ObSysVarsIdToArrayIdx));
    ObSysVars[0].info_ = "";
    ObSysVars[0].name_ = "auto_increment_increment";
    ObSysVars[0].data_type_ = ObUInt64Type;
    ObSysVars[0].value_ = "1";
    ObSysVars[0].min_val_ = "1";
    ObSysVars[0].max_val_ = "65535";
    ObSysVars[0].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[0].id_ = SYS_VAR_AUTO_INCREMENT_INCREMENT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_INCREMENT));
    ObSysVarsIdToArrayIdx[SYS_VAR_AUTO_INCREMENT_INCREMENT] = 0;
    ObSysVars[0].alias_ = "OB_SV_AUTO_INCREMENT_INCREMENT";

    ObSysVars[1].info_ = "";
    ObSysVars[1].name_ = "auto_increment_offset";
    ObSysVars[1].data_type_ = ObUInt64Type;
    ObSysVars[1].value_ = "1";
    ObSysVars[1].min_val_ = "1";
    ObSysVars[1].max_val_ = "65535";
    ObSysVars[1].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[1].id_ = SYS_VAR_AUTO_INCREMENT_OFFSET;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_OFFSET));
    ObSysVarsIdToArrayIdx[SYS_VAR_AUTO_INCREMENT_OFFSET] = 1;
    ObSysVars[1].alias_ = "OB_SV_AUTO_INCREMENT_OFFSET";

    ObSysVars[2].info_ = "";
    ObSysVars[2].name_ = "autocommit";
    ObSysVars[2].data_type_ = ObIntType;
    ObSysVars[2].value_ = "1";
    ObSysVars[2].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[2].id_ = SYS_VAR_AUTOCOMMIT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTOCOMMIT));
    ObSysVarsIdToArrayIdx[SYS_VAR_AUTOCOMMIT] = 2;
    ObSysVars[2].alias_ = "OB_SV_AUTOCOMMIT";

    ObSysVars[3].info_ = "The character set in which statements are sent by the client";
    ObSysVars[3].base_class_ = "ObCharsetSysVar";
    ObSysVars[3].name_ = "character_set_client";
    ObSysVars[3].data_type_ = ObIntType;
    ObSysVars[3].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset";
    ObSysVars[3].value_ = "45";
    ObSysVars[3].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE |
                          ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[3].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset";
    ObSysVars[3].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null";
    ObSysVars[3].id_ = SYS_VAR_CHARACTER_SET_CLIENT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_CLIENT));
    ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_CLIENT] = 3;
    ObSysVars[3].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[3].alias_ = "OB_SV_CHARACTER_SET_CLIENT";

    ObSysVars[4].info_ = "The character set which should be translated to after receiving the statement";
    ObSysVars[4].base_class_ = "ObCharsetSysVar";
    ObSysVars[4].name_ = "character_set_connection";
    ObSysVars[4].data_type_ = ObIntType;
    ObSysVars[4].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset";
    ObSysVars[4].value_ = "45";
    ObSysVars[4].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                          ObSysVarFlag::NULLABLE;
    ObSysVars[4].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset";
    ObSysVars[4].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null";
    ObSysVars[4].id_ = SYS_VAR_CHARACTER_SET_CONNECTION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_CONNECTION));
    ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_CONNECTION] = 4;
    ObSysVars[4].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[4].alias_ = "OB_SV_CHARACTER_SET_CONNECTION";

    ObSysVars[5].info_ = "The character set of the default database";
    ObSysVars[5].base_class_ = "ObCharsetSysVar";
    ObSysVars[5].name_ = "character_set_database";
    ObSysVars[5].data_type_ = ObIntType;
    ObSysVars[5].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset";
    ObSysVars[5].value_ = "45";
    ObSysVars[5].flags_ =
        ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[5].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset";
    ObSysVars[5].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null";
    ObSysVars[5].id_ = SYS_VAR_CHARACTER_SET_DATABASE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_DATABASE));
    ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_DATABASE] = 5;
    ObSysVars[5].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[5].alias_ = "OB_SV_CHARACTER_SET_DATABASE";

    ObSysVars[6].info_ = "The character set which server should translate to before shipping result sets or error "
                         "message back to the client";
    ObSysVars[6].base_class_ = "ObCharsetSysVar";
    ObSysVars[6].name_ = "character_set_results";
    ObSysVars[6].data_type_ = ObIntType;
    ObSysVars[6].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset";
    ObSysVars[6].value_ = "45";
    ObSysVars[6].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE;
    ObSysVars[6].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset";
    ObSysVars[6].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset";
    ObSysVars[6].id_ = SYS_VAR_CHARACTER_SET_RESULTS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_RESULTS));
    ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_RESULTS] = 6;
    ObSysVars[6].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[6].alias_ = "OB_SV_CHARACTER_SET_RESULTS";

    ObSysVars[7].info_ = "The server character set";
    ObSysVars[7].base_class_ = "ObCharsetSysVar";
    ObSysVars[7].name_ = "character_set_server";
    ObSysVars[7].data_type_ = ObIntType;
    ObSysVars[7].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset";
    ObSysVars[7].value_ = "45";
    ObSysVars[7].flags_ =
        ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[7].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset";
    ObSysVars[7].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null";
    ObSysVars[7].id_ = SYS_VAR_CHARACTER_SET_SERVER;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_SERVER));
    ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_SERVER] = 7;
    ObSysVars[7].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[7].alias_ = "OB_SV_CHARACTER_SET_SERVER";

    ObSysVars[8].info_ = "The character set used by the server for storing identifiers.";
    ObSysVars[8].base_class_ = "ObCharsetSysVar";
    ObSysVars[8].name_ = "character_set_system";
    ObSysVars[8].data_type_ = ObIntType;
    ObSysVars[8].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset";
    ObSysVars[8].value_ = "45";
    ObSysVars[8].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY;
    ObSysVars[8].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset";
    ObSysVars[8].id_ = SYS_VAR_CHARACTER_SET_SYSTEM;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_SYSTEM));
    ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_SYSTEM] = 8;
    ObSysVars[8].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[8].alias_ = "OB_SV_CHARACTER_SET_SYSTEM";

    ObSysVars[9].info_ = "The collation which the server should translate to after receiving the statement";
    ObSysVars[9].base_class_ = "ObCharsetSysVar";
    ObSysVars[9].name_ = "collation_connection";
    ObSysVars[9].data_type_ = ObIntType;
    ObSysVars[9].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_collation";
    ObSysVars[9].value_ = "45";
    ObSysVars[9].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE |
                          ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[9].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_collation";
    ObSysVars[9].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_collation_not_null";
    ObSysVars[9].id_ = SYS_VAR_COLLATION_CONNECTION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_COLLATION_CONNECTION));
    ObSysVarsIdToArrayIdx[SYS_VAR_COLLATION_CONNECTION] = 9;
    ObSysVars[9].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[9].alias_ = "OB_SV_COLLATION_CONNECTION";

    ObSysVars[10].info_ = "The collation of the default database";
    ObSysVars[10].base_class_ = "ObCharsetSysVar";
    ObSysVars[10].name_ = "collation_database";
    ObSysVars[10].data_type_ = ObIntType;
    ObSysVars[10].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_collation";
    ObSysVars[10].value_ = "45";
    ObSysVars[10].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE |
                           ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[10].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_collation";
    ObSysVars[10].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_collation_not_null";
    ObSysVars[10].id_ = SYS_VAR_COLLATION_DATABASE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_COLLATION_DATABASE));
    ObSysVarsIdToArrayIdx[SYS_VAR_COLLATION_DATABASE] = 10;
    ObSysVars[10].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[10].alias_ = "OB_SV_COLLATION_DATABASE";

    ObSysVars[11].info_ = "The server collation";
    ObSysVars[11].base_class_ = "ObCharsetSysVar";
    ObSysVars[11].name_ = "collation_server";
    ObSysVars[11].data_type_ = ObIntType;
    ObSysVars[11].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_collation";
    ObSysVars[11].value_ = "45";
    ObSysVars[11].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE |
                           ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[11].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_collation";
    ObSysVars[11].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_collation_not_null";
    ObSysVars[11].id_ = SYS_VAR_COLLATION_SERVER;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_COLLATION_SERVER));
    ObSysVarsIdToArrayIdx[SYS_VAR_COLLATION_SERVER] = 11;
    ObSysVars[11].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[11].alias_ = "OB_SV_COLLATION_SERVER";

    ObSysVars[12].info_ =
        "The number of seconds the server waits for activity on an interactive connection before closing it.";
    ObSysVars[12].name_ = "interactive_timeout";
    ObSysVars[12].data_type_ = ObIntType;
    ObSysVars[12].value_ = "28800";
    ObSysVars[12].min_val_ = "1";
    ObSysVars[12].max_val_ = "31536000";
    ObSysVars[12].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[12].id_ = SYS_VAR_INTERACTIVE_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INTERACTIVE_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_INTERACTIVE_TIMEOUT] = 12;
    ObSysVars[12].alias_ = "OB_SV_INTERACTIVE_TIMEOUT";

    ObSysVars[13].info_ = "";
    ObSysVars[13].base_class_ = "ObSessionSpecialIntSysVar";
    ObSysVars[13].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_last_insert_id";
    ObSysVars[13].name_ = "last_insert_id";
    ObSysVars[13].data_type_ = ObUInt64Type;
    ObSysVars[13].value_ = "0";
    ObSysVars[13].min_val_ = "0";
    ObSysVars[13].max_val_ = "18446744073709551615";
    ObSysVars[13].flags_ = ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[13].id_ = SYS_VAR_LAST_INSERT_ID;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LAST_INSERT_ID));
    ObSysVarsIdToArrayIdx[SYS_VAR_LAST_INSERT_ID] = 13;
    ObSysVars[13].alias_ = "OB_SV_LAST_INSERT_ID";

    ObSysVars[14].info_ = "Max packet length to send to or receive from the server";
    ObSysVars[14].name_ = "max_allowed_packet";
    ObSysVars[14].data_type_ = ObIntType;
    ObSysVars[14].value_ = "4194304";
    ObSysVars[14].min_val_ = "1024";
    ObSysVars[14].max_val_ = "1073741824";
    ObSysVars[14].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::SESSION_READONLY |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[14].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_max_allowed_packet";
    ObSysVars[14].id_ = SYS_VAR_MAX_ALLOWED_PACKET;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_ALLOWED_PACKET));
    ObSysVarsIdToArrayIdx[SYS_VAR_MAX_ALLOWED_PACKET] = 14;
    ObSysVars[14].alias_ = "OB_SV_MAX_ALLOWED_PACKET";

    ObSysVars[15].on_update_func_ = "ObSysVarOnUpdateFuncs::update_sql_mode";
    ObSysVars[15].info_ = "";
    ObSysVars[15].name_ = "sql_mode";
    ObSysVars[15].data_type_ = ObUInt64Type;
    ObSysVars[15].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_sql_mode";
    ObSysVars[15].value_ = "4194304";
    ObSysVars[15].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[15].base_class_ = "ObSqlModeVar";
    ObSysVars[15].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_sql_mode";
    ObSysVars[15].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_sql_mode";
    ObSysVars[15].id_ = SYS_VAR_SQL_MODE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_MODE));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_MODE] = 15;
    ObSysVars[15].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[15].alias_ = "OB_SV_SQL_MODE";

    ObSysVars[16].info_ = "";
    ObSysVars[16].base_class_ = "ObTimeZoneSysVar";
    ObSysVars[16].name_ = "time_zone";
    ObSysVars[16].data_type_ = ObVarcharType;
    ObSysVars[16].value_ = "+8:00";
    ObSysVars[16].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[16].id_ = SYS_VAR_TIME_ZONE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TIME_ZONE));
    ObSysVarsIdToArrayIdx[SYS_VAR_TIME_ZONE] = 16;
    ObSysVars[16].alias_ = "OB_SV_TIME_ZONE";

    ObSysVars[17].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_isolation";
    ObSysVars[17].info_ = "Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE";
    ObSysVars[17].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation";
    ObSysVars[17].data_type_ = ObVarcharType;
    ObSysVars[17].value_ = "READ-COMMITTED";
    ObSysVars[17].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[17].base_class_ = "ObSessionSpecialVarcharSysVar";
    ObSysVars[17].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_isolation";
    ObSysVars[17].id_ = SYS_VAR_TX_ISOLATION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TX_ISOLATION));
    ObSysVarsIdToArrayIdx[SYS_VAR_TX_ISOLATION] = 17;
    ObSysVars[17].name_ = "tx_isolation";
    ObSysVars[17].alias_ = "OB_SV_TX_ISOLATION";

    ObSysVars[18].info_ = "";
    ObSysVars[18].name_ = "version_comment";
    ObSysVars[18].data_type_ = ObVarcharType;
    ObSysVars[18].value_ = "OceanBase 1.0.0";
    ObSysVars[18].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[18].id_ = SYS_VAR_VERSION_COMMENT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION_COMMENT));
    ObSysVarsIdToArrayIdx[SYS_VAR_VERSION_COMMENT] = 18;
    ObSysVars[18].alias_ = "OB_SV_VERSION_COMMENT";

    ObSysVars[19].info_ =
        "The number of seconds the server waits for activity on a noninteractive connection before closing it.";
    ObSysVars[19].name_ = "wait_timeout";
    ObSysVars[19].data_type_ = ObIntType;
    ObSysVars[19].value_ = "28800";
    ObSysVars[19].min_val_ = "1";
    ObSysVars[19].max_val_ = "31536000";
    ObSysVars[19].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[19].id_ = SYS_VAR_WAIT_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_WAIT_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_WAIT_TIMEOUT] = 19;
    ObSysVars[19].alias_ = "OB_SV_WAIT_TIMEOUT";

    ObSysVars[20].info_ = "control row cells to logged";
    ObSysVars[20].name_ = "binlog_row_image";
    ObSysVars[20].data_type_ = ObIntType;
    ObSysVars[20].enum_names_ = "[u'MINIMAL', u'NOBLOB', u'FULL']";
    ObSysVars[20].value_ = "2";
    ObSysVars[20].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[20].id_ = SYS_VAR_BINLOG_ROW_IMAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BINLOG_ROW_IMAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_BINLOG_ROW_IMAGE] = 20;
    ObSysVars[20].alias_ = "OB_SV_BINLOG_ROW_IMAGE";

    ObSysVars[21].info_ = "";
    ObSysVars[21].base_class_ = "ObCharsetSysVar";
    ObSysVars[21].name_ = "character_set_filesystem";
    ObSysVars[21].data_type_ = ObIntType;
    ObSysVars[21].to_show_str_func_ = "ObSysVarToStrFuncs::to_str_charset";
    ObSysVars[21].value_ = "63";
    ObSysVars[21].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE;
    ObSysVars[21].to_select_obj_func_ = "ObSysVarToObjFuncs::to_obj_charset";
    ObSysVars[21].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_charset_not_null";
    ObSysVars[21].id_ = SYS_VAR_CHARACTER_SET_FILESYSTEM;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CHARACTER_SET_FILESYSTEM));
    ObSysVarsIdToArrayIdx[SYS_VAR_CHARACTER_SET_FILESYSTEM] = 21;
    ObSysVars[21].get_meta_type_func_ = "ObSysVarGetMetaTypeFuncs::get_meta_type_varchar";
    ObSysVars[21].alias_ = "OB_SV_CHARACTER_SET_FILESYSTEM";

    ObSysVars[22].info_ = "";
    ObSysVars[22].name_ = "connect_timeout";
    ObSysVars[22].data_type_ = ObIntType;
    ObSysVars[22].value_ = "10";
    ObSysVars[22].min_val_ = "2";
    ObSysVars[22].max_val_ = "31536000";
    ObSysVars[22].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[22].id_ = SYS_VAR_CONNECT_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CONNECT_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_CONNECT_TIMEOUT] = 22;
    ObSysVars[22].alias_ = "OB_SV_CONNECT_TIMEOUT";

    ObSysVars[23].info_ = "";
    ObSysVars[23].name_ = "datadir";
    ObSysVars[23].data_type_ = ObVarcharType;
    ObSysVars[23].value_ = "/usr/local/mysql/data/";
    ObSysVars[23].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[23].id_ = SYS_VAR_DATADIR;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DATADIR));
    ObSysVarsIdToArrayIdx[SYS_VAR_DATADIR] = 23;
    ObSysVars[23].alias_ = "OB_SV_DATADIR";

    ObSysVars[24].info_ = "Debug sync facility";
    ObSysVars[24].name_ = "debug_sync";
    ObSysVars[24].data_type_ = ObVarcharType;
    ObSysVars[24].value_ = "";
    ObSysVars[24].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE;
    ObSysVars[24].id_ = SYS_VAR_DEBUG_SYNC;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEBUG_SYNC));
    ObSysVarsIdToArrayIdx[SYS_VAR_DEBUG_SYNC] = 24;
    ObSysVars[24].alias_ = "OB_SV_DEBUG_SYNC";

    ObSysVars[25].info_ = "";
    ObSysVars[25].name_ = "div_precision_increment";
    ObSysVars[25].data_type_ = ObIntType;
    ObSysVars[25].value_ = "4";
    ObSysVars[25].min_val_ = "0";
    ObSysVars[25].max_val_ = "30";
    ObSysVars[25].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[25].id_ = SYS_VAR_DIV_PRECISION_INCREMENT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DIV_PRECISION_INCREMENT));
    ObSysVarsIdToArrayIdx[SYS_VAR_DIV_PRECISION_INCREMENT] = 25;
    ObSysVars[25].alias_ = "OB_SV_DIV_PRECISION_INCREMENT";

    ObSysVars[26].info_ = "whether use traditional mode for timestamp";
    ObSysVars[26].name_ = "explicit_defaults_for_timestamp";
    ObSysVars[26].data_type_ = ObIntType;
    ObSysVars[26].value_ = "1";
    ObSysVars[26].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[26].id_ = SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP));
    ObSysVarsIdToArrayIdx[SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP] = 26;
    ObSysVars[26].alias_ = "OB_SV_EXPLICIT_DEFAULTS_FOR_TIMESTAMP";

    ObSysVars[27].info_ = "";
    ObSysVars[27].name_ = "group_concat_max_len";
    ObSysVars[27].data_type_ = ObUInt64Type;
    ObSysVars[27].value_ = "1024";
    ObSysVars[27].min_val_ = "4";
    ObSysVars[27].max_val_ = "18446744073709551615";
    ObSysVars[27].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[27].id_ = SYS_VAR_GROUP_CONCAT_MAX_LEN;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GROUP_CONCAT_MAX_LEN));
    ObSysVarsIdToArrayIdx[SYS_VAR_GROUP_CONCAT_MAX_LEN] = 27;
    ObSysVars[27].alias_ = "OB_SV_GROUP_CONCAT_MAX_LEN";

    ObSysVars[28].info_ = "";
    ObSysVars[28].base_class_ = "ObSessionSpecialIntSysVar";
    ObSysVars[28].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_identity";
    ObSysVars[28].name_ = "identity";
    ObSysVars[28].data_type_ = ObUInt64Type;
    ObSysVars[28].value_ = "0";
    ObSysVars[28].min_val_ = "0";
    ObSysVars[28].max_val_ = "18446744073709551615";
    ObSysVars[28].flags_ = ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[28].id_ = SYS_VAR_IDENTITY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_IDENTITY));
    ObSysVarsIdToArrayIdx[SYS_VAR_IDENTITY] = 28;
    ObSysVars[28].alias_ = "OB_SV_IDENTITY";

    ObSysVars[29].info_ =
        "how table database names are stored and compared, 0 means stored using the lettercase in the CREATE_TABLE or "
        "CREATE_DATABASE statement. Name comparisons are case sensitive; 1 means that table and database names are "
        "stored in lowercase abd name comparisons are not case sensitive.";
    ObSysVars[29].name_ = "lower_case_table_names";
    ObSysVars[29].data_type_ = ObIntType;
    ObSysVars[29].value_ = "1";
    ObSysVars[29].min_val_ = "0";
    ObSysVars[29].max_val_ = "2";
    ObSysVars[29].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[29].id_ = SYS_VAR_LOWER_CASE_TABLE_NAMES;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOWER_CASE_TABLE_NAMES));
    ObSysVarsIdToArrayIdx[SYS_VAR_LOWER_CASE_TABLE_NAMES] = 29;
    ObSysVars[29].alias_ = "OB_SV_LOWER_CASE_TABLE_NAMES";

    ObSysVars[30].info_ = "";
    ObSysVars[30].name_ = "net_read_timeout";
    ObSysVars[30].data_type_ = ObIntType;
    ObSysVars[30].value_ = "30";
    ObSysVars[30].min_val_ = "1";
    ObSysVars[30].max_val_ = "31536000";
    ObSysVars[30].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[30].id_ = SYS_VAR_NET_READ_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NET_READ_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_NET_READ_TIMEOUT] = 30;
    ObSysVars[30].alias_ = "OB_SV_NET_READ_TIMEOUT";

    ObSysVars[31].info_ = "";
    ObSysVars[31].name_ = "net_write_timeout";
    ObSysVars[31].data_type_ = ObIntType;
    ObSysVars[31].value_ = "60";
    ObSysVars[31].min_val_ = "1";
    ObSysVars[31].max_val_ = "31536000";
    ObSysVars[31].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[31].id_ = SYS_VAR_NET_WRITE_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NET_WRITE_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_NET_WRITE_TIMEOUT] = 31;
    ObSysVars[31].alias_ = "OB_SV_NET_WRITE_TIMEOUT";

    ObSysVars[32].info_ = "";
    ObSysVars[32].name_ = "read_only";
    ObSysVars[32].data_type_ = ObIntType;
    ObSysVars[32].value_ = "0";
    ObSysVars[32].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[32].id_ = SYS_VAR_READ_ONLY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_READ_ONLY));
    ObSysVarsIdToArrayIdx[SYS_VAR_READ_ONLY] = 32;
    ObSysVars[32].alias_ = "OB_SV_READ_ONLY";

    ObSysVars[33].info_ = "";
    ObSysVars[33].name_ = "sql_auto_is_null";
    ObSysVars[33].data_type_ = ObIntType;
    ObSysVars[33].value_ = "0";
    ObSysVars[33].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[33].id_ = SYS_VAR_SQL_AUTO_IS_NULL;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_AUTO_IS_NULL));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_AUTO_IS_NULL] = 33;
    ObSysVars[33].alias_ = "OB_SV_SQL_AUTO_IS_NULL";

    ObSysVars[34].info_ = "";
    ObSysVars[34].name_ = "sql_select_limit";
    ObSysVars[34].data_type_ = ObIntType;
    ObSysVars[34].value_ = "9223372036854775807";
    ObSysVars[34].min_val_ = "0";
    ObSysVars[34].max_val_ = "9223372036854775807";
    ObSysVars[34].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[34].id_ = SYS_VAR_SQL_SELECT_LIMIT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_SELECT_LIMIT));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_SELECT_LIMIT] = 34;
    ObSysVars[34].alias_ = "OB_SV_SQL_SELECT_LIMIT";

    ObSysVars[35].info_ = "";
    ObSysVars[35].name_ = "timestamp";
    ObSysVars[35].data_type_ = ObNumberType;
    ObSysVars[35].value_ = "0";
    ObSysVars[35].min_val_ = "0";
    ObSysVars[35].flags_ = ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[35].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_max_min_timestamp";
    ObSysVars[35].id_ = SYS_VAR_TIMESTAMP;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TIMESTAMP));
    ObSysVarsIdToArrayIdx[SYS_VAR_TIMESTAMP] = 35;
    ObSysVars[35].alias_ = "OB_SV_TIMESTAMP";

    ObSysVars[36].info_ = "";
    ObSysVars[36].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope";
    ObSysVars[36].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only";
    ObSysVars[36].name_ = "tx_read_only";
    ObSysVars[36].data_type_ = ObIntType;
    ObSysVars[36].value_ = "0";
    ObSysVars[36].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[36].base_class_ = "ObSessionSpecialBoolSysVar";
    ObSysVars[36].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_read_only";
    ObSysVars[36].id_ = SYS_VAR_TX_READ_ONLY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TX_READ_ONLY));
    ObSysVarsIdToArrayIdx[SYS_VAR_TX_READ_ONLY] = 36;
    ObSysVars[36].alias_ = "OB_SV_TX_READ_ONLY";

    ObSysVars[37].info_ = "";
    ObSysVars[37].name_ = "version";
    ObSysVars[37].data_type_ = ObVarcharType;
    ObSysVars[37].value_ = "5.6.25";
    ObSysVars[37].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[37].id_ = SYS_VAR_VERSION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION));
    ObSysVarsIdToArrayIdx[SYS_VAR_VERSION] = 37;
    ObSysVars[37].alias_ = "OB_SV_VERSION";

    ObSysVars[38].info_ = "";
    ObSysVars[38].name_ = "sql_warnings";
    ObSysVars[38].data_type_ = ObIntType;
    ObSysVars[38].value_ = "0";
    ObSysVars[38].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[38].id_ = SYS_VAR_SQL_WARNINGS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_WARNINGS));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_WARNINGS] = 38;
    ObSysVars[38].alias_ = "OB_SV_SQL_WARNINGS";

    ObSysVars[39].info_ = "";
    ObSysVars[39].name_ = "max_user_connections";
    ObSysVars[39].data_type_ = ObUInt64Type;
    ObSysVars[39].value_ = "0";
    ObSysVars[39].min_val_ = "0";
    ObSysVars[39].max_val_ = "4294967295";
    ObSysVars[39].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::SESSION_READONLY;
    ObSysVars[39].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_max_user_connections";
    ObSysVars[39].id_ = SYS_VAR_MAX_USER_CONNECTIONS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_USER_CONNECTIONS));
    ObSysVarsIdToArrayIdx[SYS_VAR_MAX_USER_CONNECTIONS] = 39;
    ObSysVars[39].alias_ = "OB_SV_MAX_USER_CONNECTIONS";

    ObSysVars[40].info_ = "";
    ObSysVars[40].name_ = "init_connect";
    ObSysVars[40].data_type_ = ObVarcharType;
    ObSysVars[40].value_ = "";
    ObSysVars[40].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[40].id_ = SYS_VAR_INIT_CONNECT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_INIT_CONNECT));
    ObSysVarsIdToArrayIdx[SYS_VAR_INIT_CONNECT] = 40;
    ObSysVars[40].alias_ = "OB_SV_INIT_CONNECT";

    ObSysVars[41].info_ = "";
    ObSysVars[41].name_ = "license";
    ObSysVars[41].data_type_ = ObVarcharType;
    ObSysVars[41].value_ = "";
    ObSysVars[41].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[41].id_ = SYS_VAR_LICENSE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LICENSE));
    ObSysVarsIdToArrayIdx[SYS_VAR_LICENSE] = 41;
    ObSysVars[41].alias_ = "OB_SV_LICENSE";

    ObSysVars[42].info_ = "Buffer length for TCP/IP and socket communication";
    ObSysVars[42].name_ = "net_buffer_length";
    ObSysVars[42].data_type_ = ObIntType;
    ObSysVars[42].value_ = "16384";
    ObSysVars[42].min_val_ = "1024";
    ObSysVars[42].max_val_ = "1048576";
    ObSysVars[42].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::SESSION_READONLY;
    ObSysVars[42].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_net_buffer_length";
    ObSysVars[42].id_ = SYS_VAR_NET_BUFFER_LENGTH;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NET_BUFFER_LENGTH));
    ObSysVarsIdToArrayIdx[SYS_VAR_NET_BUFFER_LENGTH] = 42;
    ObSysVars[42].alias_ = "OB_SV_NET_BUFFER_LENGTH";

    ObSysVars[43].info_ = "The server system time zone";
    ObSysVars[43].name_ = "system_time_zone";
    ObSysVars[43].data_type_ = ObVarcharType;
    ObSysVars[43].value_ = "CST";
    ObSysVars[43].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[43].id_ = SYS_VAR_SYSTEM_TIME_ZONE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SYSTEM_TIME_ZONE));
    ObSysVarsIdToArrayIdx[SYS_VAR_SYSTEM_TIME_ZONE] = 43;
    ObSysVars[43].alias_ = "OB_SV_SYSTEM_TIME_ZONE";

    ObSysVars[44].info_ = "The memory allocated to store results from old queries(not used yet)";
    ObSysVars[44].name_ = "query_cache_size";
    ObSysVars[44].data_type_ = ObUInt64Type;
    ObSysVars[44].value_ = "1048576";
    ObSysVars[44].min_val_ = "0";
    ObSysVars[44].max_val_ = "18446744073709551615";
    ObSysVars[44].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[44].id_ = SYS_VAR_QUERY_CACHE_SIZE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_CACHE_SIZE));
    ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_CACHE_SIZE] = 44;
    ObSysVars[44].alias_ = "OB_SV_QUERY_CACHE_SIZE";

    ObSysVars[45].info_ = "OFF = Do not cache or retrieve results. ON = Cache all results except SELECT SQL_NO_CACHE "
                          "... queries. DEMAND = Cache only SELECT SQL_CACHE ... queries(not used yet)";
    ObSysVars[45].name_ = "query_cache_type";
    ObSysVars[45].data_type_ = ObIntType;
    ObSysVars[45].enum_names_ = "[u'OFF', u'ON', u'DEMAND']";
    ObSysVars[45].value_ = "0";
    ObSysVars[45].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[45].id_ = SYS_VAR_QUERY_CACHE_TYPE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_QUERY_CACHE_TYPE));
    ObSysVarsIdToArrayIdx[SYS_VAR_QUERY_CACHE_TYPE] = 45;
    ObSysVars[45].alias_ = "OB_SV_QUERY_CACHE_TYPE";

    ObSysVars[46].info_ = "";
    ObSysVars[46].name_ = "sql_quote_show_create";
    ObSysVars[46].data_type_ = ObIntType;
    ObSysVars[46].value_ = "1";
    ObSysVars[46].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[46].id_ = SYS_VAR_SQL_QUOTE_SHOW_CREATE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_QUOTE_SHOW_CREATE));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_QUOTE_SHOW_CREATE] = 46;
    ObSysVars[46].alias_ = "OB_SV_SQL_QUOTE_SHOW_CREATE";

    ObSysVars[47].info_ = "The number of times that any given stored procedure may be called recursively.";
    ObSysVars[47].name_ = "max_sp_recursion_depth";
    ObSysVars[47].data_type_ = ObIntType;
    ObSysVars[47].value_ = "0";
    ObSysVars[47].min_val_ = "0";
    ObSysVars[47].max_val_ = "255";
    ObSysVars[47].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[47].id_ = SYS_VAR_MAX_SP_RECURSION_DEPTH;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_SP_RECURSION_DEPTH));
    ObSysVarsIdToArrayIdx[SYS_VAR_MAX_SP_RECURSION_DEPTH] = 47;
    ObSysVars[47].alias_ = "OB_SV_MAX_SP_RECURSION_DEPTH";

    ObSysVars[48].info_ = "enable mysql sql safe updates";
    ObSysVars[48].name_ = "sql_safe_updates";
    ObSysVars[48].data_type_ = ObIntType;
    ObSysVars[48].value_ = "0";
    ObSysVars[48].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                           ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[48].id_ = SYS_VAR_SQL_SAFE_UPDATES;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_SAFE_UPDATES));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_SAFE_UPDATES] = 48;
    ObSysVars[48].alias_ = "OB_SV_SQL_SAFE_UPDATES";

    ObSysVars[49].info_ = "";
    ObSysVars[49].name_ = "concurrent_insert";
    ObSysVars[49].data_type_ = ObVarcharType;
    ObSysVars[49].value_ = "AUTO";
    ObSysVars[49].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[49].id_ = SYS_VAR_CONCURRENT_INSERT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_CONCURRENT_INSERT));
    ObSysVarsIdToArrayIdx[SYS_VAR_CONCURRENT_INSERT] = 49;
    ObSysVars[49].alias_ = "OB_SV_CONCURRENT_INSERT";

    ObSysVars[50].info_ = "";
    ObSysVars[50].name_ = "default_authentication_plugin";
    ObSysVars[50].data_type_ = ObVarcharType;
    ObSysVars[50].value_ = "mysql_native_password";
    ObSysVars[50].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[50].id_ = SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN));
    ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN] = 50;
    ObSysVars[50].alias_ = "OB_SV_DEFAULT_AUTHENTICATION_PLUGIN";

    ObSysVars[51].info_ = "";
    ObSysVars[51].name_ = "disabled_storage_engines";
    ObSysVars[51].data_type_ = ObVarcharType;
    ObSysVars[51].value_ = "";
    ObSysVars[51].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[51].id_ = SYS_VAR_DISABLED_STORAGE_ENGINES;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DISABLED_STORAGE_ENGINES));
    ObSysVarsIdToArrayIdx[SYS_VAR_DISABLED_STORAGE_ENGINES] = 51;
    ObSysVars[51].alias_ = "OB_SV_DISABLED_STORAGE_ENGINES";

    ObSysVars[52].info_ = "";
    ObSysVars[52].name_ = "error_count";
    ObSysVars[52].data_type_ = ObUInt64Type;
    ObSysVars[52].value_ = "0";
    ObSysVars[52].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[52].id_ = SYS_VAR_ERROR_COUNT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_ERROR_COUNT));
    ObSysVarsIdToArrayIdx[SYS_VAR_ERROR_COUNT] = 52;
    ObSysVars[52].alias_ = "OB_SV_ERROR_COUNT";

    ObSysVars[53].info_ = "";
    ObSysVars[53].name_ = "general_log";
    ObSysVars[53].data_type_ = ObIntType;
    ObSysVars[53].value_ = "0";
    ObSysVars[53].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[53].id_ = SYS_VAR_GENERAL_LOG;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_GENERAL_LOG));
    ObSysVarsIdToArrayIdx[SYS_VAR_GENERAL_LOG] = 53;
    ObSysVars[53].alias_ = "OB_SV_GENERAL_LOG";

    ObSysVars[54].info_ = "";
    ObSysVars[54].name_ = "have_openssl";
    ObSysVars[54].data_type_ = ObVarcharType;
    ObSysVars[54].value_ = "YES";
    ObSysVars[54].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[54].id_ = SYS_VAR_HAVE_OPENSSL;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_OPENSSL));
    ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_OPENSSL] = 54;
    ObSysVars[54].alias_ = "OB_SV_HAVE_OPENSSL";

    ObSysVars[55].info_ = "";
    ObSysVars[55].name_ = "have_profiling";
    ObSysVars[55].data_type_ = ObVarcharType;
    ObSysVars[55].value_ = "YES";
    ObSysVars[55].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[55].id_ = SYS_VAR_HAVE_PROFILING;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_PROFILING));
    ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_PROFILING] = 55;
    ObSysVars[55].alias_ = "OB_SV_HAVE_PROFILING";

    ObSysVars[56].info_ = "";
    ObSysVars[56].name_ = "have_ssl";
    ObSysVars[56].data_type_ = ObVarcharType;
    ObSysVars[56].value_ = "YES";
    ObSysVars[56].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[56].id_ = SYS_VAR_HAVE_SSL;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HAVE_SSL));
    ObSysVarsIdToArrayIdx[SYS_VAR_HAVE_SSL] = 56;
    ObSysVars[56].alias_ = "OB_SV_HAVE_SSL";

    ObSysVars[57].info_ = "";
    ObSysVars[57].name_ = "hostname";
    ObSysVars[57].data_type_ = ObVarcharType;
    ObSysVars[57].value_ = "";
    ObSysVars[57].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[57].id_ = SYS_VAR_HOSTNAME;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_HOSTNAME));
    ObSysVarsIdToArrayIdx[SYS_VAR_HOSTNAME] = 57;
    ObSysVars[57].alias_ = "OB_SV_HOSTNAME";

    ObSysVars[58].info_ = "";
    ObSysVars[58].name_ = "lc_messages";
    ObSysVars[58].data_type_ = ObVarcharType;
    ObSysVars[58].value_ = "en_US";
    ObSysVars[58].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[58].id_ = SYS_VAR_LC_MESSAGES;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LC_MESSAGES));
    ObSysVarsIdToArrayIdx[SYS_VAR_LC_MESSAGES] = 58;
    ObSysVars[58].alias_ = "OB_SV_LC_MESSAGES";

    ObSysVars[59].info_ = "";
    ObSysVars[59].name_ = "local_infile";
    ObSysVars[59].data_type_ = ObIntType;
    ObSysVars[59].value_ = "1";
    ObSysVars[59].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[59].id_ = SYS_VAR_LOCAL_INFILE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOCAL_INFILE));
    ObSysVarsIdToArrayIdx[SYS_VAR_LOCAL_INFILE] = 59;
    ObSysVars[59].alias_ = "OB_SV_LOCAL_INFILE";

    ObSysVars[60].info_ = "";
    ObSysVars[60].name_ = "lock_wait_timeout";
    ObSysVars[60].data_type_ = ObIntType;
    ObSysVars[60].value_ = "31536000";
    ObSysVars[60].min_val_ = "1";
    ObSysVars[60].max_val_ = "31536000";
    ObSysVars[60].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[60].id_ = SYS_VAR_LOCK_WAIT_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LOCK_WAIT_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_LOCK_WAIT_TIMEOUT] = 60;
    ObSysVars[60].alias_ = "OB_SV_LOCK_WAIT_TIMEOUT";

    ObSysVars[61].info_ = "";
    ObSysVars[61].name_ = "long_query_time";
    ObSysVars[61].data_type_ = ObNumberType;
    ObSysVars[61].value_ = "10";
    ObSysVars[61].min_val_ = "0";
    ObSysVars[61].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[61].id_ = SYS_VAR_LONG_QUERY_TIME;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_LONG_QUERY_TIME));
    ObSysVarsIdToArrayIdx[SYS_VAR_LONG_QUERY_TIME] = 61;
    ObSysVars[61].alias_ = "OB_SV_LONG_QUERY_TIME";

    ObSysVars[62].info_ = "";
    ObSysVars[62].name_ = "max_connections";
    ObSysVars[62].data_type_ = ObIntType;
    ObSysVars[62].value_ = "151";
    ObSysVars[62].min_val_ = "1";
    ObSysVars[62].max_val_ = "100000";
    ObSysVars[62].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[62].id_ = SYS_VAR_MAX_CONNECTIONS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_CONNECTIONS));
    ObSysVarsIdToArrayIdx[SYS_VAR_MAX_CONNECTIONS] = 62;
    ObSysVars[62].alias_ = "OB_SV_MAX_CONNECTIONS";

    ObSysVars[63].info_ = "";
    ObSysVars[63].name_ = "max_execution_time";
    ObSysVars[63].data_type_ = ObIntType;
    ObSysVars[63].value_ = "0";
    ObSysVars[63].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[63].id_ = SYS_VAR_MAX_EXECUTION_TIME;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_MAX_EXECUTION_TIME));
    ObSysVarsIdToArrayIdx[SYS_VAR_MAX_EXECUTION_TIME] = 63;
    ObSysVars[63].alias_ = "OB_SV_MAX_EXECUTION_TIME";

    ObSysVars[64].info_ = "";
    ObSysVars[64].name_ = "protocol_version";
    ObSysVars[64].data_type_ = ObIntType;
    ObSysVars[64].value_ = "10";
    ObSysVars[64].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[64].id_ = SYS_VAR_PROTOCOL_VERSION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PROTOCOL_VERSION));
    ObSysVarsIdToArrayIdx[SYS_VAR_PROTOCOL_VERSION] = 64;
    ObSysVars[64].alias_ = "OB_SV_PROTOCOL_VERSION";

    ObSysVars[65].info_ = "";
    ObSysVars[65].name_ = "server_id";
    ObSysVars[65].data_type_ = ObIntType;
    ObSysVars[65].value_ = "0";
    ObSysVars[65].min_val_ = "0";
    ObSysVars[65].max_val_ = "4294967295";
    ObSysVars[65].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[65].id_ = SYS_VAR_SERVER_ID;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SERVER_ID));
    ObSysVarsIdToArrayIdx[SYS_VAR_SERVER_ID] = 65;
    ObSysVars[65].alias_ = "OB_SV_SERVER_ID";

    ObSysVars[66].info_ = "";
    ObSysVars[66].name_ = "ssl_ca";
    ObSysVars[66].data_type_ = ObVarcharType;
    ObSysVars[66].value_ = "";
    ObSysVars[66].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[66].id_ = SYS_VAR_SSL_CA;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CA));
    ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CA] = 66;
    ObSysVars[66].alias_ = "OB_SV_SSL_CA";

    ObSysVars[67].info_ = "";
    ObSysVars[67].name_ = "ssl_capath";
    ObSysVars[67].data_type_ = ObVarcharType;
    ObSysVars[67].value_ = "";
    ObSysVars[67].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[67].id_ = SYS_VAR_SSL_CAPATH;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CAPATH));
    ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CAPATH] = 67;
    ObSysVars[67].alias_ = "OB_SV_SSL_CAPATH";

    ObSysVars[68].info_ = "";
    ObSysVars[68].name_ = "ssl_cert";
    ObSysVars[68].data_type_ = ObVarcharType;
    ObSysVars[68].value_ = "";
    ObSysVars[68].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[68].id_ = SYS_VAR_SSL_CERT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CERT));
    ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CERT] = 68;
    ObSysVars[68].alias_ = "OB_SV_SSL_CERT";

    ObSysVars[69].info_ = "";
    ObSysVars[69].name_ = "ssl_cipher";
    ObSysVars[69].data_type_ = ObVarcharType;
    ObSysVars[69].value_ = "";
    ObSysVars[69].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[69].id_ = SYS_VAR_SSL_CIPHER;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CIPHER));
    ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CIPHER] = 69;
    ObSysVars[69].alias_ = "OB_SV_SSL_CIPHER";

    ObSysVars[70].info_ = "";
    ObSysVars[70].name_ = "ssl_crl";
    ObSysVars[70].data_type_ = ObVarcharType;
    ObSysVars[70].value_ = "";
    ObSysVars[70].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[70].id_ = SYS_VAR_SSL_CRL;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CRL));
    ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CRL] = 70;
    ObSysVars[70].alias_ = "OB_SV_SSL_CRL";

    ObSysVars[71].info_ = "";
    ObSysVars[71].name_ = "ssl_crlpath";
    ObSysVars[71].data_type_ = ObVarcharType;
    ObSysVars[71].value_ = "";
    ObSysVars[71].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[71].id_ = SYS_VAR_SSL_CRLPATH;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_CRLPATH));
    ObSysVarsIdToArrayIdx[SYS_VAR_SSL_CRLPATH] = 71;
    ObSysVars[71].alias_ = "OB_SV_SSL_CRLPATH";

    ObSysVars[72].info_ = "";
    ObSysVars[72].name_ = "ssl_key";
    ObSysVars[72].data_type_ = ObVarcharType;
    ObSysVars[72].value_ = "";
    ObSysVars[72].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[72].id_ = SYS_VAR_SSL_KEY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SSL_KEY));
    ObSysVarsIdToArrayIdx[SYS_VAR_SSL_KEY] = 72;
    ObSysVars[72].alias_ = "OB_SV_SSL_KEY";

    ObSysVars[73].info_ = "";
    ObSysVars[73].name_ = "time_format";
    ObSysVars[73].data_type_ = ObVarcharType;
    ObSysVars[73].value_ = "%H:%i:%s";
    ObSysVars[73].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[73].id_ = SYS_VAR_TIME_FORMAT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TIME_FORMAT));
    ObSysVarsIdToArrayIdx[SYS_VAR_TIME_FORMAT] = 73;
    ObSysVars[73].alias_ = "OB_SV_TIME_FORMAT";

    ObSysVars[74].info_ = "TLSv1,TLSv1.1,TLSv1.2";
    ObSysVars[74].name_ = "tls_version";
    ObSysVars[74].data_type_ = ObVarcharType;
    ObSysVars[74].value_ = "";
    ObSysVars[74].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[74].id_ = SYS_VAR_TLS_VERSION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TLS_VERSION));
    ObSysVarsIdToArrayIdx[SYS_VAR_TLS_VERSION] = 74;
    ObSysVars[74].alias_ = "OB_SV_TLS_VERSION";

    ObSysVars[75].info_ = "";
    ObSysVars[75].name_ = "tmp_table_size";
    ObSysVars[75].data_type_ = ObUInt64Type;
    ObSysVars[75].value_ = "16777216";
    ObSysVars[75].min_val_ = "1024";
    ObSysVars[75].max_val_ = "18446744073709551615";
    ObSysVars[75].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[75].id_ = SYS_VAR_TMP_TABLE_SIZE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TMP_TABLE_SIZE));
    ObSysVarsIdToArrayIdx[SYS_VAR_TMP_TABLE_SIZE] = 75;
    ObSysVars[75].alias_ = "OB_SV_TMP_TABLE_SIZE";

    ObSysVars[76].info_ = "";
    ObSysVars[76].name_ = "tmpdir";
    ObSysVars[76].data_type_ = ObVarcharType;
    ObSysVars[76].value_ = "";
    ObSysVars[76].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[76].id_ = SYS_VAR_TMPDIR;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TMPDIR));
    ObSysVarsIdToArrayIdx[SYS_VAR_TMPDIR] = 76;
    ObSysVars[76].alias_ = "OB_SV_TMPDIR";

    ObSysVars[77].info_ = "";
    ObSysVars[77].name_ = "unique_checks";
    ObSysVars[77].data_type_ = ObIntType;
    ObSysVars[77].value_ = "1";
    ObSysVars[77].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[77].id_ = SYS_VAR_UNIQUE_CHECKS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_UNIQUE_CHECKS));
    ObSysVarsIdToArrayIdx[SYS_VAR_UNIQUE_CHECKS] = 77;
    ObSysVars[77].alias_ = "OB_SV_UNIQUE_CHECKS";

    ObSysVars[78].info_ = "";
    ObSysVars[78].name_ = "version_compile_machine";
    ObSysVars[78].data_type_ = ObVarcharType;
    ObSysVars[78].value_ = "";
    ObSysVars[78].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[78].id_ = SYS_VAR_VERSION_COMPILE_MACHINE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION_COMPILE_MACHINE));
    ObSysVarsIdToArrayIdx[SYS_VAR_VERSION_COMPILE_MACHINE] = 78;
    ObSysVars[78].alias_ = "OB_SV_VERSION_COMPILE_MACHINE";

    ObSysVars[79].info_ = "";
    ObSysVars[79].name_ = "version_compile_os";
    ObSysVars[79].data_type_ = ObVarcharType;
    ObSysVars[79].value_ = "";
    ObSysVars[79].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[79].id_ = SYS_VAR_VERSION_COMPILE_OS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VERSION_COMPILE_OS));
    ObSysVarsIdToArrayIdx[SYS_VAR_VERSION_COMPILE_OS] = 79;
    ObSysVars[79].alias_ = "OB_SV_VERSION_COMPILE_OS";

    ObSysVars[80].info_ = "";
    ObSysVars[80].name_ = "warning_count";
    ObSysVars[80].data_type_ = ObUInt64Type;
    ObSysVars[80].value_ = "0";
    ObSysVars[80].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::MYSQL_ONLY;
    ObSysVars[80].id_ = SYS_VAR_WARNING_COUNT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_WARNING_COUNT));
    ObSysVarsIdToArrayIdx[SYS_VAR_WARNING_COUNT] = 80;
    ObSysVars[80].alias_ = "OB_SV_WARNING_COUNT";

    ObSysVars[81].info_ = "The default replica number of table per zone if not specified when creating table.";
    ObSysVars[81].name_ = "ob_default_replica_num";
    ObSysVars[81].data_type_ = ObIntType;
    ObSysVars[81].value_ = "1";
    ObSysVars[81].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[81].id_ = SYS_VAR_OB_DEFAULT_REPLICA_NUM;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_DEFAULT_REPLICA_NUM));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_DEFAULT_REPLICA_NUM] = 81;
    ObSysVars[81].alias_ = "OB_SV_DEFAULT_REPLICA_NUM";

    ObSysVars[82].info_ = "Indicate how many bytes the interm result manager can alloc most for this tenant";
    ObSysVars[82].name_ = "ob_interm_result_mem_limit";
    ObSysVars[82].data_type_ = ObIntType;
    ObSysVars[82].value_ = "2147483648";
    ObSysVars[82].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[82].id_ = SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT] = 82;
    ObSysVars[82].alias_ = "OB_SV_INTERM_RESULT_MEM_LIMIT";

    ObSysVars[83].info_ = "Indicate whether sql stmt hit right partition, readonly to user, modify by ob";
    ObSysVars[83].name_ = "ob_proxy_partition_hit";
    ObSysVars[83].data_type_ = ObIntType;
    ObSysVars[83].value_ = "1";
    ObSysVars[83].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE;
    ObSysVars[83].id_ = SYS_VAR_OB_PROXY_PARTITION_HIT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_PARTITION_HIT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_PARTITION_HIT] = 83;
    ObSysVars[83].alias_ = "OB_SV_PROXY_PARTITION_HIT";

    ObSysVars[84].info_ = "log level in session";
    ObSysVars[84].name_ = "ob_log_level";
    ObSysVars[84].data_type_ = ObVarcharType;
    ObSysVars[84].value_ = "disabled";
    ObSysVars[84].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[84].id_ = SYS_VAR_OB_LOG_LEVEL;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_LOG_LEVEL));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_LOG_LEVEL] = 84;
    ObSysVars[84].alias_ = "OB_SV_LOG_LEVEL";

    ObSysVars[85].info_ = "Max parellel sub request to chunkservers for one request";
    ObSysVars[85].name_ = "ob_max_parallel_degree";
    ObSysVars[85].data_type_ = ObIntType;
    ObSysVars[85].value_ = "16";
    ObSysVars[85].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[85].id_ = SYS_VAR_OB_MAX_PARALLEL_DEGREE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_MAX_PARALLEL_DEGREE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_MAX_PARALLEL_DEGREE] = 85;
    ObSysVars[85].alias_ = "OB_SV_MAX_PARALLEL_DEGREE";

    ObSysVars[86].info_ = "Query timeout in microsecond(us)";
    ObSysVars[86].name_ = "ob_query_timeout";
    ObSysVars[86].data_type_ = ObIntType;
    ObSysVars[86].value_ = "10000000";
    ObSysVars[86].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[86].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large";
    ObSysVars[86].id_ = SYS_VAR_OB_QUERY_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_QUERY_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_QUERY_TIMEOUT] = 86;
    ObSysVars[86].alias_ = "OB_SV_QUERY_TIMEOUT";

    ObSysVars[87].info_ = "read consistency level: 3=STRONG, 2=WEAK, 1=FROZEN";
    ObSysVars[87].name_ = "ob_read_consistency";
    ObSysVars[87].data_type_ = ObIntType;
    ObSysVars[87].enum_names_ = "[u'', u'FROZEN', u'WEAK', u'STRONG']";
    ObSysVars[87].value_ = "3";
    ObSysVars[87].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                           ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[87].id_ = SYS_VAR_OB_READ_CONSISTENCY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_READ_CONSISTENCY));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_READ_CONSISTENCY] = 87;
    ObSysVars[87].alias_ = "OB_SV_READ_CONSISTENCY";

    ObSysVars[88].info_ = "whether use transform in session";
    ObSysVars[88].name_ = "ob_enable_transformation";
    ObSysVars[88].data_type_ = ObIntType;
    ObSysVars[88].value_ = "1";
    ObSysVars[88].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[88].id_ = SYS_VAR_OB_ENABLE_TRANSFORMATION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRANSFORMATION));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_TRANSFORMATION] = 88;
    ObSysVars[88].alias_ = "OB_SV_ENABLE_TRANSFORMATION";

    ObSysVars[89].info_ = "The max duration of one transaction";
    ObSysVars[89].name_ = "ob_trx_timeout";
    ObSysVars[89].data_type_ = ObIntType;
    ObSysVars[89].value_ = "100000000";
    ObSysVars[89].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[89].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large";
    ObSysVars[89].id_ = SYS_VAR_OB_TRX_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRX_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRX_TIMEOUT] = 89;
    ObSysVars[89].alias_ = "OB_SV_TRX_TIMEOUT";

    ObSysVars[90].info_ = "whether use plan cache in session";
    ObSysVars[90].name_ = "ob_enable_plan_cache";
    ObSysVars[90].data_type_ = ObIntType;
    ObSysVars[90].value_ = "1";
    ObSysVars[90].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[90].id_ = SYS_VAR_OB_ENABLE_PLAN_CACHE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_PLAN_CACHE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_PLAN_CACHE] = 90;
    ObSysVars[90].alias_ = "OB_SV_ENABLE_PLAN_CACHE";

    ObSysVars[91].info_ = "whether can select from index table";
    ObSysVars[91].name_ = "ob_enable_index_direct_select";
    ObSysVars[91].data_type_ = ObIntType;
    ObSysVars[91].value_ = "0";
    ObSysVars[91].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[91].id_ = SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT] = 91;
    ObSysVars[91].alias_ = "OB_SV_ENABLE_INDEX_DIRECT_SELECT";

    ObSysVars[92].info_ = "this value is true if we have executed set transaction stmt, until a transaction "
                          "commit(explicit or implicit) successfully";
    ObSysVars[92].name_ = "ob_proxy_set_trx_executed";
    ObSysVars[92].data_type_ = ObIntType;
    ObSysVars[92].value_ = "0";
    ObSysVars[92].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE;
    ObSysVars[92].id_ = SYS_VAR_OB_PROXY_SET_TRX_EXECUTED;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_SET_TRX_EXECUTED));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_SET_TRX_EXECUTED] = 92;
    ObSysVars[92].alias_ = "OB_SV_PROXY_SET_TRX_EXECUTED";

    ObSysVars[93].info_ = "enable aggregation function to be push-downed through exchange nodes";
    ObSysVars[93].name_ = "ob_enable_aggregation_pushdown";
    ObSysVars[93].data_type_ = ObIntType;
    ObSysVars[93].value_ = "1";
    ObSysVars[93].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[93].id_ = SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN] = 93;
    ObSysVars[93].alias_ = "OB_SV_ENABLE_AGGREGATION_PUSHDOWN";

    ObSysVars[94].info_ = "";
    ObSysVars[94].name_ = "ob_last_schema_version";
    ObSysVars[94].data_type_ = ObIntType;
    ObSysVars[94].value_ = "0";
    ObSysVars[94].flags_ = ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[94].id_ = SYS_VAR_OB_LAST_SCHEMA_VERSION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_LAST_SCHEMA_VERSION));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_LAST_SCHEMA_VERSION] = 94;
    ObSysVars[94].alias_ = "OB_SV_LAST_SCHEMA_VERSION";

    ObSysVars[95].info_ = "Global debug sync facility";
    ObSysVars[95].name_ = "ob_global_debug_sync";
    ObSysVars[95].data_type_ = ObVarcharType;
    ObSysVars[95].value_ = "";
    ObSysVars[95].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE;
    ObSysVars[95].id_ = SYS_VAR_OB_GLOBAL_DEBUG_SYNC;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_GLOBAL_DEBUG_SYNC));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_GLOBAL_DEBUG_SYNC] = 95;
    ObSysVars[95].alias_ = "OB_SV_GLOBAL_DEBUG_SYNC";

    ObSysVars[96].info_ = "this value is global variables last modified time when server session create, used for "
                          "proxy to judge whether global vars has changed between two server session";
    ObSysVars[96].name_ = "ob_proxy_global_variables_version";
    ObSysVars[96].data_type_ = ObIntType;
    ObSysVars[96].value_ = "0";
    ObSysVars[96].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE;
    ObSysVars[96].id_ = SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION] = 96;
    ObSysVars[96].alias_ = "OB_SV_PROXY_GLOBAL_VARIABLES_VERSION";

    ObSysVars[97].info_ = "control whether use trace log";
    ObSysVars[97].name_ = "ob_enable_trace_log";
    ObSysVars[97].data_type_ = ObIntType;
    ObSysVars[97].value_ = "0";
    ObSysVars[97].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[97].id_ = SYS_VAR_OB_ENABLE_TRACE_LOG;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRACE_LOG));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_TRACE_LOG] = 97;
    ObSysVars[97].alias_ = "OB_SV_ENABLE_TRACE_LOG";

    ObSysVars[98].info_ = "";
    ObSysVars[98].name_ = "ob_enable_hash_group_by";
    ObSysVars[98].data_type_ = ObIntType;
    ObSysVars[98].value_ = "1";
    ObSysVars[98].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[98].id_ = SYS_VAR_OB_ENABLE_HASH_GROUP_BY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_HASH_GROUP_BY));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_HASH_GROUP_BY] = 98;
    ObSysVars[98].alias_ = "OB_SV_ENABLE_HASH_GROUP_BY";

    ObSysVars[99].info_ = "";
    ObSysVars[99].name_ = "ob_enable_blk_nestedloop_join";
    ObSysVars[99].data_type_ = ObIntType;
    ObSysVars[99].value_ = "0";
    ObSysVars[99].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                           ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[99].id_ = SYS_VAR_OB_ENABLE_BLK_NESTEDLOOP_JOIN;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_BLK_NESTEDLOOP_JOIN));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_BLK_NESTEDLOOP_JOIN] = 99;
    ObSysVars[99].alias_ = "OB_SV_ENABLE_BLK_NESTEDLOOP_JOIN";

    ObSysVars[100].info_ = "";
    ObSysVars[100].name_ = "ob_bnl_join_cache_size";
    ObSysVars[100].data_type_ = ObIntType;
    ObSysVars[100].value_ = "10485760";
    ObSysVars[100].min_val_ = "1";
    ObSysVars[100].max_val_ = "9223372036854775807";
    ObSysVars[100].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                            ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[100].id_ = SYS_VAR_OB_BNL_JOIN_CACHE_SIZE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_BNL_JOIN_CACHE_SIZE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_BNL_JOIN_CACHE_SIZE] = 100;
    ObSysVars[100].alias_ = "OB_SV_BNL_JOIN_CACHE_SIZE";

    ObSysVars[101].info_ = "Indicate current client session user privilege, readonly after modified by first observer";
    ObSysVars[101].name_ = "ob_proxy_user_privilege";
    ObSysVars[101].data_type_ = ObIntType;
    ObSysVars[101].value_ = "0";
    ObSysVars[101].min_val_ = "0";
    ObSysVars[101].max_val_ = "9223372036854775807";
    ObSysVars[101].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE;
    ObSysVars[101].id_ = SYS_VAR_OB_PROXY_USER_PRIVILEGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PROXY_USER_PRIVILEGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PROXY_USER_PRIVILEGE] = 101;
    ObSysVars[101].alias_ = "OB_SV_PROXY_USER_PRIVILEGE";

    ObSysVars[102].info_ =
        "When the DRC system copies data into the target cluster, it needs to be set to the CLUSTER_ID that should be "
        "written into commit log of OceanBase, in order to avoid loop replication of data. Normally, it does not need "
        "to be set, and OceanBase will use the default value, which is the CLUSTER_ID of current cluster of OceanBase. "
        "0 indicates it is not set, please do not set it to 0";
    ObSysVars[102].base_class_ = "ObStrictRangeIntSysVar";
    ObSysVars[102].name_ = "ob_org_cluster_id";
    ObSysVars[102].data_type_ = ObIntType;
    ObSysVars[102].value_ = "0";
    ObSysVars[102].min_val_ = "0";
    ObSysVars[102].max_val_ = "4294967295";
    ObSysVars[102].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[102].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_ob_org_cluster_id";
    ObSysVars[102].id_ = SYS_VAR_OB_ORG_CLUSTER_ID;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ORG_CLUSTER_ID));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ORG_CLUSTER_ID] = 102;
    ObSysVars[102].alias_ = "OB_SV_ORG_CLUSTER_ID";

    ObSysVars[103].info_ = "percentage of tenant memory resources that can be used by plan cache";
    ObSysVars[103].name_ = "ob_plan_cache_percentage";
    ObSysVars[103].data_type_ = ObIntType;
    ObSysVars[103].value_ = "5";
    ObSysVars[103].min_val_ = "0";
    ObSysVars[103].max_val_ = "100";
    ObSysVars[103].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[103].id_ = SYS_VAR_OB_PLAN_CACHE_PERCENTAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_PERCENTAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PLAN_CACHE_PERCENTAGE] = 103;
    ObSysVars[103].alias_ = "OB_SV_PLAN_CACHE_PERCENTAGE";

    ObSysVars[104].info_ = "memory usage percentage of plan_cache_limit at which plan cache eviction will be trigger";
    ObSysVars[104].name_ = "ob_plan_cache_evict_high_percentage";
    ObSysVars[104].data_type_ = ObIntType;
    ObSysVars[104].value_ = "90";
    ObSysVars[104].min_val_ = "0";
    ObSysVars[104].max_val_ = "100";
    ObSysVars[104].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[104].id_ = SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE] = 104;
    ObSysVars[104].alias_ = "OB_SV_PLAN_CACHE_EVICT_HIGH_PERCENTAGE";

    ObSysVars[105].info_ = "memory usage percentage  of plan_cache_limit at which plan cache eviction will be stopped";
    ObSysVars[105].name_ = "ob_plan_cache_evict_low_percentage";
    ObSysVars[105].data_type_ = ObIntType;
    ObSysVars[105].value_ = "50";
    ObSysVars[105].min_val_ = "0";
    ObSysVars[105].max_val_ = "100";
    ObSysVars[105].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[105].id_ = SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE] = 105;
    ObSysVars[105].alias_ = "OB_SV_PLAN_CACHE_EVICT_LOW_PERCENTAGE";

    ObSysVars[106].info_ = "When the recycle bin is enabled, dropped tables and their dependent objects are placed in "
                           "the recycle bin. When the recycle bin is disabled, dropped tables and their dependent "
                           "objects are not placed in the recycle bin; they are just dropped.";
    ObSysVars[106].name_ = "recyclebin";
    ObSysVars[106].data_type_ = ObIntType;
    ObSysVars[106].value_ = "0";
    ObSysVars[106].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[106].id_ = SYS_VAR_RECYCLEBIN;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RECYCLEBIN));
    ObSysVarsIdToArrayIdx[SYS_VAR_RECYCLEBIN] = 106;
    ObSysVars[106].alias_ = "OB_SV_RECYCLEBIN";

    ObSysVars[107].info_ = "Indicate features that observer supports, readonly after modified by first observer";
    ObSysVars[107].name_ = "ob_capability_flag";
    ObSysVars[107].data_type_ = ObUInt64Type;
    ObSysVars[107].value_ = "0";
    ObSysVars[107].min_val_ = "0";
    ObSysVars[107].max_val_ = "18446744073709551615";
    ObSysVars[107].flags_ = ObSysVarFlag::READONLY | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE;
    ObSysVars[107].id_ = SYS_VAR_OB_CAPABILITY_FLAG;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_CAPABILITY_FLAG));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_CAPABILITY_FLAG] = 107;
    ObSysVars[107].alias_ = "OB_SV_CAPABILITY_FLAG";

    ObSysVars[108].info_ =
        "The parallel degree of a job in a query, which represent how many tasks of a job can be run parallelly";
    ObSysVars[108].name_ = "ob_stmt_parallel_degree";
    ObSysVars[108].data_type_ = ObIntType;
    ObSysVars[108].value_ = "1";
    ObSysVars[108].min_val_ = "1";
    ObSysVars[108].max_val_ = "10240";
    ObSysVars[108].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                            ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[108].id_ = SYS_VAR_OB_STMT_PARALLEL_DEGREE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_STMT_PARALLEL_DEGREE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_STMT_PARALLEL_DEGREE] = 108;
    ObSysVars[108].alias_ = "OB_SV_STMT_PARALLEL_DEGREE";

    ObSysVars[109].info_ =
        "when query is with topk hint, is_result_accurate indicates whether the result is acuurate or not ";
    ObSysVars[109].name_ = "is_result_accurate";
    ObSysVars[109].data_type_ = ObIntType;
    ObSysVars[109].value_ = "1";
    ObSysVars[109].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[109].id_ = SYS_VAR_IS_RESULT_ACCURATE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_IS_RESULT_ACCURATE));
    ObSysVarsIdToArrayIdx[SYS_VAR_IS_RESULT_ACCURATE] = 109;
    ObSysVars[109].alias_ = "OB_SV_IS_RESULT_ACCURATE";

    ObSysVars[110].info_ = "The variable determines how OceanBase should handle an ambiguous boundary datetime value a "
                           "case in which it is not clear whether the datetime is in standard or daylight saving time";
    ObSysVars[110].name_ = "error_on_overlap_time";
    ObSysVars[110].data_type_ = ObIntType;
    ObSysVars[110].value_ = "0";
    ObSysVars[110].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[110].id_ = SYS_VAR_ERROR_ON_OVERLAP_TIME;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_ERROR_ON_OVERLAP_TIME));
    ObSysVarsIdToArrayIdx[SYS_VAR_ERROR_ON_OVERLAP_TIME] = 110;
    ObSysVars[110].alias_ = "OB_SV_ERROR_ON_OVERLAP_TIME";

    ObSysVars[111].info_ = "What DBMS is OceanBase compatible with? MYSQL means it behaves like MySQL while ORACLE "
                           "means it behaves like Oracle.";
    ObSysVars[111].name_ = "ob_compatibility_mode";
    ObSysVars[111].data_type_ = ObIntType;
    ObSysVars[111].enum_names_ = "[u'MYSQL', u'ORACLE']";
    ObSysVars[111].value_ = "0";
    ObSysVars[111].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY |
                            ObSysVarFlag::WITH_UPGRADE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[111].id_ = SYS_VAR_OB_COMPATIBILITY_MODE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_COMPATIBILITY_MODE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_COMPATIBILITY_MODE] = 111;
    ObSysVars[111].alias_ = "OB_SV_COMPATIBILITY_MODE";

    ObSysVars[112].info_ = "If set true, create all the replicas according to the locality or the operation will fail.";
    ObSysVars[112].name_ = "ob_create_table_strict_mode";
    ObSysVars[112].data_type_ = ObIntType;
    ObSysVars[112].value_ = "1";
    ObSysVars[112].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[112].id_ = SYS_VAR_OB_CREATE_TABLE_STRICT_MODE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_CREATE_TABLE_STRICT_MODE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_CREATE_TABLE_STRICT_MODE] = 112;
    ObSysVars[112].alias_ = "OB_SV_CREATE_TABLE_STRICT_MODE";

    ObSysVars[113].info_ = "The percentage limitation of tenant memory for SQL execution.";
    ObSysVars[113].name_ = "ob_sql_work_area_percentage";
    ObSysVars[113].data_type_ = ObIntType;
    ObSysVars[113].value_ = "5";
    ObSysVars[113].min_val_ = "0";
    ObSysVars[113].max_val_ = "100";
    ObSysVars[113].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[113].id_ = SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE] = 113;
    ObSysVars[113].alias_ = "OB_SV_SQL_WORK_AREA_PERCENTAGE";

    ObSysVars[114].info_ = "The safe weak read snapshot version in one server";
    ObSysVars[114].on_update_func_ = "ObSysVarOnUpdateFuncs::update_safe_weak_read_snapshot";
    ObSysVars[114].name_ = "ob_safe_weak_read_snapshot";
    ObSysVars[114].data_type_ = ObIntType;
    ObSysVars[114].value_ = "1";
    ObSysVars[114].min_val_ = "0";
    ObSysVars[114].max_val_ = "9223372036854775807";
    ObSysVars[114].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE;
    ObSysVars[114].id_ = SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT] = 114;
    ObSysVars[114].alias_ = "OB_SV_SAFE_WEAK_READ_SNAPSHOT";

    ObSysVars[115].info_ =
        "the routing policy of obproxy/java client and observer internal retry, 1=READONLY_ZONE_FIRST, "
        "2=ONLY_READONLY_ZONE, 3=UNMERGE_ZONE_FIRST, 4=UNMERGE_FOLLOWER_FIRST";
    ObSysVars[115].name_ = "ob_route_policy";
    ObSysVars[115].data_type_ = ObIntType;
    ObSysVars[115].enum_names_ =
        "[u'', u'READONLY_ZONE_FIRST', u'ONLY_READONLY_ZONE', u'UNMERGE_ZONE_FIRST', u'UNMERGE_FOLLOWER_FIRST']";
    ObSysVars[115].value_ = "1";
    ObSysVars[115].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INFLUENCE_PLAN |
                            ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[115].id_ = SYS_VAR_OB_ROUTE_POLICY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ROUTE_POLICY));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ROUTE_POLICY] = 115;
    ObSysVars[115].alias_ = "OB_SV_ROUTE_POLICY";

    ObSysVars[116].info_ = "whether do the checksum of the packet between the client and the server";
    ObSysVars[116].name_ = "ob_enable_transmission_checksum";
    ObSysVars[116].data_type_ = ObIntType;
    ObSysVars[116].value_ = "1";
    ObSysVars[116].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::QUERY_SENSITIVE |
                            ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[116].id_ = SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM] = 116;
    ObSysVars[116].alias_ = "OB_SV_ENABLE_TRANSMISSION_CHECKSUM";

    ObSysVars[117].info_ = "set to 1 (the default by MySQL), foreign key constraints are checked. If set to 0, foreign "
                           "key constraints are ignored";
    ObSysVars[117].name_ = "foreign_key_checks";
    ObSysVars[117].data_type_ = ObIntType;
    ObSysVars[117].value_ = "1";
    ObSysVars[117].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[117].id_ = SYS_VAR_FOREIGN_KEY_CHECKS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_FOREIGN_KEY_CHECKS));
    ObSysVarsIdToArrayIdx[SYS_VAR_FOREIGN_KEY_CHECKS] = 117;
    ObSysVars[117].alias_ = "OB_SV_FOREIGN_KEY_CHECKS";

    ObSysVars[118].info_ = "the trace id of current executing statement";
    ObSysVars[118].name_ = "ob_statement_trace_id";
    ObSysVars[118].data_type_ = ObVarcharType;
    ObSysVars[118].value_ = "Y0-0";
    ObSysVars[118].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::INVISIBLE;
    ObSysVars[118].id_ = SYS_VAR_OB_STATEMENT_TRACE_ID;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_STATEMENT_TRACE_ID));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_STATEMENT_TRACE_ID] = 118;
    ObSysVars[118].alias_ = "OB_SV_STATEMENT_TRACE_ID";

    ObSysVars[119].info_ = "Enable the flashback of table truncation.";
    ObSysVars[119].name_ = "ob_enable_truncate_flashback";
    ObSysVars[119].data_type_ = ObIntType;
    ObSysVars[119].value_ = "0";
    ObSysVars[119].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[119].id_ = SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK] = 119;
    ObSysVars[119].alias_ = "OB_SV_ENABLE_TRUNCATE_FLASHBACK";

    ObSysVars[120].info_ =
        "ip white list for tenant, support % and _ and multi ip(separated by commas), support ip match and wild match";
    ObSysVars[120].name_ = "ob_tcp_invited_nodes";
    ObSysVars[120].data_type_ = ObVarcharType;
    ObSysVars[120].value_ = "127.0.0.1,::1";
    ObSysVars[120].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[120].id_ = SYS_VAR_OB_TCP_INVITED_NODES;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TCP_INVITED_NODES));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_TCP_INVITED_NODES] = 120;
    ObSysVars[120].alias_ = "OB_SV_TCP_INVITED_NODES";

    ObSysVars[121].info_ = "current priority used for SQL throttling";
    ObSysVars[121].name_ = "sql_throttle_current_priority";
    ObSysVars[121].data_type_ = ObIntType;
    ObSysVars[121].value_ = "100";
    ObSysVars[121].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[121].id_ = SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY] = 121;
    ObSysVars[121].alias_ = "OB_SV_SQL_THROTTLE_CURRENT_PRIORITY";

    ObSysVars[122].info_ =
        "sql throttle priority, query may not be allowed to execute if its priority isnt greater than this value.";
    ObSysVars[122].name_ = "sql_throttle_priority";
    ObSysVars[122].data_type_ = ObIntType;
    ObSysVars[122].value_ = "-1";
    ObSysVars[122].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[122].id_ = SYS_VAR_SQL_THROTTLE_PRIORITY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_PRIORITY));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_PRIORITY] = 122;
    ObSysVars[122].alias_ = "OB_SV_SQL_THROTTLE_PRIORITY";

    ObSysVars[123].info_ = "query may not be allowed to execute if its rt isnt less than this value.";
    ObSysVars[123].name_ = "sql_throttle_rt";
    ObSysVars[123].data_type_ = ObNumberType;
    ObSysVars[123].value_ = "-1";
    ObSysVars[123].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[123].id_ = SYS_VAR_SQL_THROTTLE_RT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_RT));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_RT] = 123;
    ObSysVars[123].alias_ = "OB_SV_SQL_THROTTLE_RT";

    ObSysVars[124].info_ = "query may not be allowed to execute if its CPU usage isnt less than this value.";
    ObSysVars[124].name_ = "sql_throttle_cpu";
    ObSysVars[124].data_type_ = ObNumberType;
    ObSysVars[124].value_ = "-1";
    ObSysVars[124].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[124].id_ = SYS_VAR_SQL_THROTTLE_CPU;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_CPU));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_CPU] = 124;
    ObSysVars[124].alias_ = "OB_SV_SQL_THROTTLE_CPU";

    ObSysVars[125].info_ = "query may not be allowed to execute if its number of IOs isnt less than this value.";
    ObSysVars[125].name_ = "sql_throttle_io";
    ObSysVars[125].data_type_ = ObIntType;
    ObSysVars[125].value_ = "-1";
    ObSysVars[125].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[125].id_ = SYS_VAR_SQL_THROTTLE_IO;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_IO));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_IO] = 125;
    ObSysVars[125].alias_ = "OB_SV_SQL_THROTTLE_IO";

    ObSysVars[126].info_ = "query may not be allowed to execute if its network usage isnt less than this value.";
    ObSysVars[126].name_ = "sql_throttle_network";
    ObSysVars[126].data_type_ = ObNumberType;
    ObSysVars[126].value_ = "-1";
    ObSysVars[126].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[126].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_sql_throttle_queue_time";
    ObSysVars[126].id_ = SYS_VAR_SQL_THROTTLE_NETWORK;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_NETWORK));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_NETWORK] = 126;
    ObSysVars[126].alias_ = "OB_SV_SQL_THROTTLE_NETWORK";

    ObSysVars[127].info_ =
        "query may not be allowed to execute if its number of logical reads isnt less than this value.";
    ObSysVars[127].name_ = "sql_throttle_logical_reads";
    ObSysVars[127].data_type_ = ObIntType;
    ObSysVars[127].value_ = "-1";
    ObSysVars[127].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[127].id_ = SYS_VAR_SQL_THROTTLE_LOGICAL_READS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SQL_THROTTLE_LOGICAL_READS));
    ObSysVarsIdToArrayIdx[SYS_VAR_SQL_THROTTLE_LOGICAL_READS] = 127;
    ObSysVars[127].alias_ = "OB_SV_SQL_THROTTLE_LOGICAL_READS";

    ObSysVars[128].info_ = "auto_increment service cache size";
    ObSysVars[128].name_ = "auto_increment_cache_size";
    ObSysVars[128].data_type_ = ObIntType;
    ObSysVars[128].value_ = "1000000";
    ObSysVars[128].min_val_ = "1";
    ObSysVars[128].max_val_ = "100000000";
    ObSysVars[128].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[128].id_ = SYS_VAR_AUTO_INCREMENT_CACHE_SIZE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_AUTO_INCREMENT_CACHE_SIZE));
    ObSysVarsIdToArrayIdx[SYS_VAR_AUTO_INCREMENT_CACHE_SIZE] = 128;
    ObSysVars[128].alias_ = "OB_SV_AUTO_INCREMENT_CACHE_SIZE";

    ObSysVars[129].info_ = "JIT execution engine mode, default is AUTO";
    ObSysVars[129].name_ = "ob_enable_jit";
    ObSysVars[129].data_type_ = ObIntType;
    ObSysVars[129].enum_names_ = "[u'OFF', u'AUTO', u'FORCE']";
    ObSysVars[129].value_ = "0";
    ObSysVars[129].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[129].id_ = SYS_VAR_OB_ENABLE_JIT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_JIT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_JIT] = 129;
    ObSysVars[129].alias_ = "OB_SV_ENABLE_JIT";

    ObSysVars[130].info_ = "the percentage limitation of some temp tablespace size in tenant disk.";
    ObSysVars[130].name_ = "ob_temp_tablespace_size_percentage";
    ObSysVars[130].data_type_ = ObIntType;
    ObSysVars[130].value_ = "0";
    ObSysVars[130].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[130].id_ = SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE] = 130;
    ObSysVars[130].alias_ = "OB_SV_TEMP_TABLESPACE_SIZE_PERCENTAGE";

    ObSysVars[131].info_ = "Enable use of adaptive cursor sharing";
    ObSysVars[131].name_ = "_optimizer_adaptive_cursor_sharing";
    ObSysVars[131].data_type_ = ObIntType;
    ObSysVars[131].value_ = "0";
    ObSysVars[131].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE |
                            ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[131].id_ = SYS_VAR__OPTIMIZER_ADAPTIVE_CURSOR_SHARING;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OPTIMIZER_ADAPTIVE_CURSOR_SHARING));
    ObSysVarsIdToArrayIdx[SYS_VAR__OPTIMIZER_ADAPTIVE_CURSOR_SHARING] = 131;
    ObSysVars[131].alias_ = "OB_SV__OPTIMIZER_ADAPTIVE_CURSOR_SHARING";

    ObSysVars[132].info_ = "the type of timestamp service";
    ObSysVars[132].name_ = "ob_timestamp_service";
    ObSysVars[132].data_type_ = ObIntType;
    ObSysVars[132].enum_names_ = "[u'LTS', u'GTS', u'HA_GTS']";
    ObSysVars[132].value_ = "1";
    ObSysVars[132].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[132].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timestamp_service";
    ObSysVars[132].id_ = SYS_VAR_OB_TIMESTAMP_SERVICE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TIMESTAMP_SERVICE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_TIMESTAMP_SERVICE] = 132;
    ObSysVars[132].alias_ = "OB_SV_TIMESTAMP_SERVICE";

    ObSysVars[133].info_ = "the dir to place plugin dll";
    ObSysVars[133].name_ = "plugin_dir";
    ObSysVars[133].data_type_ = ObVarcharType;
    ObSysVars[133].value_ = "./plugin_dir/";
    ObSysVars[133].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY;
    ObSysVars[133].id_ = SYS_VAR_PLUGIN_DIR;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLUGIN_DIR));
    ObSysVarsIdToArrayIdx[SYS_VAR_PLUGIN_DIR] = 133;
    ObSysVars[133].alias_ = "OB_SV_PLUGIN_DIR";

    ObSysVars[134].info_ = "specifies (in seconds) the low threshold value of undo retention.";
    ObSysVars[134].name_ = "undo_retention";
    ObSysVars[134].data_type_ = ObIntType;
    ObSysVars[134].value_ = "0";
    ObSysVars[134].min_val_ = "0";
    ObSysVars[134].max_val_ = "4294967295";
    ObSysVars[134].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[134].id_ = SYS_VAR_UNDO_RETENTION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_UNDO_RETENTION));
    ObSysVarsIdToArrayIdx[SYS_VAR_UNDO_RETENTION] = 134;
    ObSysVars[134].alias_ = "OB_SV_UNDO_RETENTION";

    ObSysVars[135].info_ = "auto use parallel execution";
    ObSysVars[135].name_ = "_ob_use_parallel_execution";
    ObSysVars[135].data_type_ = ObIntType;
    ObSysVars[135].value_ = "1";
    ObSysVars[135].flags_ = ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE |
                            ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::INVISIBLE;
    ObSysVars[135].id_ = SYS_VAR__OB_USE_PARALLEL_EXECUTION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_USE_PARALLEL_EXECUTION));
    ObSysVarsIdToArrayIdx[SYS_VAR__OB_USE_PARALLEL_EXECUTION] = 135;
    ObSysVars[135].alias_ = "OB_SV__OB_USE_PARALLEL_EXECUTION";

    ObSysVars[136].info_ = "The limited percentage of tenant memory for sql audit";
    ObSysVars[136].name_ = "ob_sql_audit_percentage";
    ObSysVars[136].data_type_ = ObIntType;
    ObSysVars[136].value_ = "3";
    ObSysVars[136].min_val_ = "0";
    ObSysVars[136].max_val_ = "100";
    ObSysVars[136].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[136].id_ = SYS_VAR_OB_SQL_AUDIT_PERCENTAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_SQL_AUDIT_PERCENTAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_SQL_AUDIT_PERCENTAGE] = 136;
    ObSysVars[136].alias_ = "OB_SV_SQL_AUDIT_PERCENTAGE";

    ObSysVars[137].info_ = "wether use sql audit in session";
    ObSysVars[137].name_ = "ob_enable_sql_audit";
    ObSysVars[137].data_type_ = ObIntType;
    ObSysVars[137].value_ = "1";
    ObSysVars[137].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[137].id_ = SYS_VAR_OB_ENABLE_SQL_AUDIT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_SQL_AUDIT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_SQL_AUDIT] = 137;
    ObSysVars[137].alias_ = "OB_SV_ENABLE_SQL_AUDIT";

    ObSysVars[138].info_ = "Enable use sql plan baseline";
    ObSysVars[138].name_ = "optimizer_use_sql_plan_baselines";
    ObSysVars[138].data_type_ = ObIntType;
    ObSysVars[138].value_ = "1";
    ObSysVars[138].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[138].id_ = SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES));
    ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES] = 138;
    ObSysVars[138].alias_ = "OB_SV_OPTIMIZER_USE_SQL_PLAN_BASELINES";

    ObSysVars[139].info_ = "optimizer_capture_sql_plan_baselines enables or disables automitic capture plan baseline.";
    ObSysVars[139].name_ = "optimizer_capture_sql_plan_baselines";
    ObSysVars[139].data_type_ = ObIntType;
    ObSysVars[139].value_ = "1";
    ObSysVars[139].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[139].id_ = SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES));
    ObSysVarsIdToArrayIdx[SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES] = 139;
    ObSysVars[139].alias_ = "OB_SV_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES";

    ObSysVars[140].info_ = "number of threads created to run parallel statements for each observer.";
    ObSysVars[140].name_ = "parallel_max_servers";
    ObSysVars[140].data_type_ = ObIntType;
    ObSysVars[140].value_ = "0";
    ObSysVars[140].min_val_ = "0";
    ObSysVars[140].max_val_ = "1800";
    ObSysVars[140].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[140].id_ = SYS_VAR_PARALLEL_MAX_SERVERS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PARALLEL_MAX_SERVERS));
    ObSysVarsIdToArrayIdx[SYS_VAR_PARALLEL_MAX_SERVERS] = 140;
    ObSysVars[140].alias_ = "OB_SV_PARALLEL_MAX_SERVERS";

    ObSysVars[141].info_ =
        "number of threads allowed to run parallel statements before statement queuing will be used.";
    ObSysVars[141].name_ = "parallel_servers_target";
    ObSysVars[141].data_type_ = ObIntType;
    ObSysVars[141].value_ = "0";
    ObSysVars[141].min_val_ = "0";
    ObSysVars[141].max_val_ = "9223372036854775807";
    ObSysVars[141].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[141].id_ = SYS_VAR_PARALLEL_SERVERS_TARGET;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PARALLEL_SERVERS_TARGET));
    ObSysVarsIdToArrayIdx[SYS_VAR_PARALLEL_SERVERS_TARGET] = 141;
    ObSysVars[141].alias_ = "OB_SV_PARALLEL_SERVERS_TARGET";

    ObSysVars[142].info_ = "If set true, transaction open the elr optimization.";
    ObSysVars[142].name_ = "ob_early_lock_release";
    ObSysVars[142].data_type_ = ObIntType;
    ObSysVars[142].value_ = "0";
    ObSysVars[142].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[142].id_ = SYS_VAR_OB_EARLY_LOCK_RELEASE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_EARLY_LOCK_RELEASE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_EARLY_LOCK_RELEASE] = 142;
    ObSysVars[142].alias_ = "OB_SV_EARLY_LOCK_RELEASE";

    ObSysVars[143].info_ = "The stmt interval timeout of transaction(us)";
    ObSysVars[143].name_ = "ob_trx_idle_timeout";
    ObSysVars[143].data_type_ = ObIntType;
    ObSysVars[143].value_ = "120000000";
    ObSysVars[143].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[143].id_ = SYS_VAR_OB_TRX_IDLE_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRX_IDLE_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRX_IDLE_TIMEOUT] = 143;
    ObSysVars[143].alias_ = "OB_SV_TRX_IDLE_TIMEOUT";

    ObSysVars[144].info_ = "specifies the encryption algorithm used in the functions aes_encrypt and aes_decrypt";
    ObSysVars[144].name_ = "block_encryption_mode";
    ObSysVars[144].data_type_ = ObIntType;
    ObSysVars[144].enum_names_ =
        "[u'aes-128-ecb', u'aes-192-ecb', u'aes-256-ecb', u'aes-128-cbc', u'aes-192-cbc', u'aes-256-cbc', "
        "u'aes-128-cfb1', u'aes-192-cfb1', u'aes-256-cfb1', u'aes-128-cfb8', u'aes-192-cfb8', u'aes-256-cfb8', "
        "u'aes-128-cfb128', u'aes-192-cfb128', u'aes-256-cfb128', u'aes-128-ofb', u'aes-192-ofb', u'aes-256-ofb']";
    ObSysVars[144].value_ = "0";
    ObSysVars[144].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[144].id_ = SYS_VAR_BLOCK_ENCRYPTION_MODE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_BLOCK_ENCRYPTION_MODE));
    ObSysVarsIdToArrayIdx[SYS_VAR_BLOCK_ENCRYPTION_MODE] = 144;
    ObSysVars[144].alias_ = "OB_SV_BLOCK_ENCRYPTION_MODE";

    ObSysVars[145].info_ = "specifies the default date format to use with the TO_CHAR and TO_DATE functions, "
                           "(YYYY-MM-DD HH24:MI:SS) is Common value";
    ObSysVars[145].name_ = "nls_date_format";
    ObSysVars[145].data_type_ = ObVarcharType;
    ObSysVars[145].value_ = "DD-MON-RR";
    ObSysVars[145].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[145].id_ = SYS_VAR_NLS_DATE_FORMAT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_DATE_FORMAT));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_DATE_FORMAT] = 145;
    ObSysVars[145].alias_ = "OB_SV_NLS_DATE_FORMAT";

    ObSysVars[146].info_ = "specifies the default date format to use with the TO_CHAR and TO_TIMESTAMP functions, "
                           "(YYYY-MM-DD HH24:MI:SS.FF) is Common value";
    ObSysVars[146].name_ = "nls_timestamp_format";
    ObSysVars[146].data_type_ = ObVarcharType;
    ObSysVars[146].value_ = "DD-MON-RR HH.MI.SSXFF AM";
    ObSysVars[146].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[146].id_ = SYS_VAR_NLS_TIMESTAMP_FORMAT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_TIMESTAMP_FORMAT));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_TIMESTAMP_FORMAT] = 146;
    ObSysVars[146].alias_ = "OB_SV_NLS_TIMESTAMP_FORMAT";

    ObSysVars[147].info_ = "specifies the default timestamp with time zone format to use with the TO_CHAR and "
                           "TO_TIMESTAMP_TZ functions, (YYYY-MM-DD HH24:MI:SS.FF TZR TZD) is common value";
    ObSysVars[147].name_ = "nls_timestamp_tz_format";
    ObSysVars[147].data_type_ = ObVarcharType;
    ObSysVars[147].value_ = "DD-MON-RR HH.MI.SSXFF AM TZR";
    ObSysVars[147].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[147].id_ = SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT] = 147;
    ObSysVars[147].alias_ = "OB_SV_NLS_TIMESTAMP_TZ_FORMAT";

    ObSysVars[148].info_ = "percentage of tenant memory resources that can be used by tenant meta data";
    ObSysVars[148].name_ = "ob_reserved_meta_memory_percentage";
    ObSysVars[148].data_type_ = ObIntType;
    ObSysVars[148].value_ = "10";
    ObSysVars[148].min_val_ = "1";
    ObSysVars[148].max_val_ = "100";
    ObSysVars[148].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[148].id_ = SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE] = 148;
    ObSysVars[148].alias_ = "OB_SV_RESERVED_META_MEMORY_PERCENTAGE";

    ObSysVars[149].info_ = "If set true, sql will update sys variable while schema version changed.";
    ObSysVars[149].name_ = "ob_check_sys_variable";
    ObSysVars[149].data_type_ = ObIntType;
    ObSysVars[149].value_ = "1";
    ObSysVars[149].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[149].id_ = SYS_VAR_OB_CHECK_SYS_VARIABLE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_CHECK_SYS_VARIABLE));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_CHECK_SYS_VARIABLE] = 149;
    ObSysVars[149].alias_ = "OB_SV_CHECK_SYS_VARIABLE";

    ObSysVars[150].info_ = "specifies the default language of the database, used for messages, day and month names, "
                           "the default sorting mechanism, the default values of NLS_DATE_LANGUAGE and NLS_SORT.";
    ObSysVars[150].name_ = "nls_language";
    ObSysVars[150].data_type_ = ObVarcharType;
    ObSysVars[150].value_ = "AMERICAN";
    ObSysVars[150].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[150].id_ = SYS_VAR_NLS_LANGUAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_LANGUAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_LANGUAGE] = 150;
    ObSysVars[150].alias_ = "OB_SV_NLS_LANGUAGE";

    ObSysVars[151].info_ = "specifies the name of the territory whose conventions are to be followed for day and week "
                           "numbering, establishes the default date format, the default decimal character and group "
                           "separator, and the default ISO and local currency symbols.";
    ObSysVars[151].name_ = "nls_territory";
    ObSysVars[151].data_type_ = ObVarcharType;
    ObSysVars[151].value_ = "AMERICA";
    ObSysVars[151].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[151].id_ = SYS_VAR_NLS_TERRITORY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_TERRITORY));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_TERRITORY] = 151;
    ObSysVars[151].alias_ = "OB_SV_NLS_TERRITORY";

    ObSysVars[152].info_ =
        "specifies the collating sequence for character value comparison in various SQL operators and clauses.";
    ObSysVars[152].name_ = "nls_sort";
    ObSysVars[152].data_type_ = ObVarcharType;
    ObSysVars[152].value_ = "BINARY";
    ObSysVars[152].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[152].id_ = SYS_VAR_NLS_SORT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_SORT));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_SORT] = 152;
    ObSysVars[152].alias_ = "OB_SV_NLS_SORT";

    ObSysVars[153].info_ =
        "specifies the collation behavior of the database session. value can be BINARY | LINGUISTIC | ANSI";
    ObSysVars[153].name_ = "nls_comp";
    ObSysVars[153].data_type_ = ObVarcharType;
    ObSysVars[153].value_ = "BINARY";
    ObSysVars[153].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[153].id_ = SYS_VAR_NLS_COMP;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_COMP));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_COMP] = 153;
    ObSysVars[153].alias_ = "OB_SV_NLS_COMP";

    ObSysVars[154].info_ = "specifies the default characterset of the database, This parameter defines the encoding of "
                           "the data in the CHAR, VARCHAR2, LONG and CLOB columns of a table.";
    ObSysVars[154].name_ = "nls_characterset";
    ObSysVars[154].data_type_ = ObVarcharType;
    ObSysVars[154].value_ = "AL32UTF8";
    ObSysVars[154].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY |
                            ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::WITH_CREATE | ObSysVarFlag::READONLY;
    ObSysVars[154].id_ = SYS_VAR_NLS_CHARACTERSET;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_CHARACTERSET));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_CHARACTERSET] = 154;
    ObSysVars[154].alias_ = "OB_SV_NLS_CHARACTERSET";

    ObSysVars[155].info_ = "specifies the default characterset of the database, This parameter defines the encoding of "
                           "the data in the NCHAR, NVARCHAR2 and NCLOB columns of a table.";
    ObSysVars[155].name_ = "nls_nchar_characterset";
    ObSysVars[155].data_type_ = ObVarcharType;
    ObSysVars[155].value_ = "AL32UTF8";
    ObSysVars[155].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::ORACLE_ONLY |
                            ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[155].id_ = SYS_VAR_NLS_NCHAR_CHARACTERSET;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_NCHAR_CHARACTERSET));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_NCHAR_CHARACTERSET] = 155;
    ObSysVars[155].alias_ = "OB_SV_NLS_NCHAR_CHARACTERSET";

    ObSysVars[156].info_ = "specifies the language to use for the spelling of day and month names and date "
                           "abbreviations (a.m., p.m., AD, BC) returned by the TO_DATE and TO_CHAR functions.";
    ObSysVars[156].name_ = "nls_date_language";
    ObSysVars[156].data_type_ = ObVarcharType;
    ObSysVars[156].value_ = "AMERICAN";
    ObSysVars[156].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[156].id_ = SYS_VAR_NLS_DATE_LANGUAGE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_DATE_LANGUAGE));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_DATE_LANGUAGE] = 156;
    ObSysVars[156].alias_ = "OB_SV_NLS_DATE_LANGUAGE";

    ObSysVars[157].info_ = "specifies the default length semantics to use for VARCHAR2 and CHAR table columns, "
                           "user-defined object attributes, and PL/SQL variables in database objects created in the "
                           "session. SYS user use BYTE intead of NLS_LENGTH_SEMANTICS.";
    ObSysVars[157].name_ = "nls_length_semantics";
    ObSysVars[157].data_type_ = ObVarcharType;
    ObSysVars[157].value_ = "BYTE";
    ObSysVars[157].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[157].id_ = SYS_VAR_NLS_LENGTH_SEMANTICS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_LENGTH_SEMANTICS));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_LENGTH_SEMANTICS] = 157;
    ObSysVars[157].alias_ = "OB_SV_NLS_LENGTH_SEMANTICS";

    ObSysVars[158].info_ = "determines whether an error is reported when there is data loss during an implicit or "
                           "explicit character type conversion between NCHAR/NVARCHAR2 and CHAR/VARCHAR2.";
    ObSysVars[158].name_ = "nls_nchar_conv_excp";
    ObSysVars[158].data_type_ = ObVarcharType;
    ObSysVars[158].value_ = "FALSE";
    ObSysVars[158].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[158].id_ = SYS_VAR_NLS_NCHAR_CONV_EXCP;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_NCHAR_CONV_EXCP));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_NCHAR_CONV_EXCP] = 158;
    ObSysVars[158].alias_ = "OB_SV_NLS_NCHAR_CONV_EXCP";

    ObSysVars[159].info_ = "specifies which calendar system Oracle uses.";
    ObSysVars[159].name_ = "nls_calendar";
    ObSysVars[159].data_type_ = ObVarcharType;
    ObSysVars[159].value_ = "GREGORIAN";
    ObSysVars[159].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[159].id_ = SYS_VAR_NLS_CALENDAR;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_CALENDAR));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_CALENDAR] = 159;
    ObSysVars[159].alias_ = "OB_SV_NLS_CALENDAR";

    ObSysVars[160].info_ = "specifies the characters to use as the decimal character and group separator, overrides "
                           "those characters defined implicitly by NLS_TERRITORY.";
    ObSysVars[160].name_ = "nls_numeric_characters";
    ObSysVars[160].data_type_ = ObVarcharType;
    ObSysVars[160].value_ = ".,";
    ObSysVars[160].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[160].id_ = SYS_VAR_NLS_NUMERIC_CHARACTERS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_NUMERIC_CHARACTERS));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_NUMERIC_CHARACTERS] = 160;
    ObSysVars[160].alias_ = "OB_SV_NLS_NUMERIC_CHARACTERS";

    ObSysVars[161].info_ = "enable batching of the RHS IO in NLJ";
    ObSysVars[161].name_ = "_nlj_batching_enabled";
    ObSysVars[161].data_type_ = ObIntType;
    ObSysVars[161].value_ = "1";
    ObSysVars[161].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[161].id_ = SYS_VAR__NLJ_BATCHING_ENABLED;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__NLJ_BATCHING_ENABLED));
    ObSysVarsIdToArrayIdx[SYS_VAR__NLJ_BATCHING_ENABLED] = 161;
    ObSysVars[161].alias_ = "OB_SV__NLJ_BATCHING_ENABLED";

    ObSysVars[162].info_ = "The name of tracefile.";
    ObSysVars[162].name_ = "tracefile_identifier";
    ObSysVars[162].data_type_ = ObVarcharType;
    ObSysVars[162].value_ = "";
    ObSysVars[162].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[162].id_ = SYS_VAR_TRACEFILE_IDENTIFIER;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRACEFILE_IDENTIFIER));
    ObSysVarsIdToArrayIdx[SYS_VAR_TRACEFILE_IDENTIFIER] = 162;
    ObSysVars[162].alias_ = "OB_SV_TRACEFILE_IDENTIFIER";

    ObSysVars[163].info_ = "ratio used to decide whether push down should be done in distribtued query optimization.";
    ObSysVars[163].name_ = "_groupby_nopushdown_cut_ratio";
    ObSysVars[163].data_type_ = ObUInt64Type;
    ObSysVars[163].value_ = "3";
    ObSysVars[163].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE |
                            ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[163].id_ = SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO));
    ObSysVarsIdToArrayIdx[SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO] = 163;
    ObSysVars[163].alias_ = "OB_SV__GROUPBY_NOPUSHDOWN_CUT_RATIO";

    ObSysVars[164].info_ = "set the tq broadcasting fudge factor percentage.";
    ObSysVars[164].name_ = "_px_broadcast_fudge_factor";
    ObSysVars[164].data_type_ = ObIntType;
    ObSysVars[164].value_ = "100";
    ObSysVars[164].min_val_ = "0";
    ObSysVars[164].max_val_ = "100";
    ObSysVars[164].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[164].id_ = SYS_VAR__PX_BROADCAST_FUDGE_FACTOR;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_BROADCAST_FUDGE_FACTOR));
    ObSysVarsIdToArrayIdx[SYS_VAR__PX_BROADCAST_FUDGE_FACTOR] = 164;
    ObSysVars[164].alias_ = "OB_SV__PX_BROADCAST_FUDGE_FACTOR";

    ObSysVars[165].info_ = "statistic primary zone entity(table/tablegroup/database) count under tenant.";
    ObSysVars[165].name_ = "_primary_zone_entity_count";
    ObSysVars[165].data_type_ = ObIntType;
    ObSysVars[165].value_ = "-1";
    ObSysVars[165].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::READONLY | ObSysVarFlag::INVISIBLE;
    ObSysVars[165].id_ = SYS_VAR__PRIMARY_ZONE_ENTITY_COUNT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PRIMARY_ZONE_ENTITY_COUNT));
    ObSysVarsIdToArrayIdx[SYS_VAR__PRIMARY_ZONE_ENTITY_COUNT] = 165;
    ObSysVars[165].alias_ = "OB_SV__PRIMARY_ZONE_ENTITY_COUNT";

    ObSysVars[166].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_isolation";
    ObSysVars[166].info_ =
        "Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE";
    ObSysVars[166].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation";
    ObSysVars[166].data_type_ = ObVarcharType;
    ObSysVars[166].value_ = "READ-COMMITTED";
    ObSysVars[166].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[166].base_class_ = "ObSessionSpecialVarcharSysVar";
    ObSysVars[166].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_isolation";
    ObSysVars[166].id_ = SYS_VAR_TRANSACTION_ISOLATION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_ISOLATION));
    ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_ISOLATION] = 166;
    ObSysVars[166].name_ = "transaction_isolation";
    ObSysVars[166].alias_ = "OB_SV_TRANSACTION_ISOLATION";

    ObSysVars[167].info_ = "the max duration of waiting on row lock of one transaction";
    ObSysVars[167].name_ = "ob_trx_lock_timeout";
    ObSysVars[167].data_type_ = ObIntType;
    ObSysVars[167].value_ = "-1";
    ObSysVars[167].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[167].id_ = SYS_VAR_OB_TRX_LOCK_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRX_LOCK_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRX_LOCK_TIMEOUT] = 167;
    ObSysVars[167].alias_ = "OB_SV_TRX_LOCK_TIMEOUT";

    ObSysVars[168].info_ = "";
    ObSysVars[168].name_ = "validate_password_check_user_name";
    ObSysVars[168].data_type_ = ObIntType;
    ObSysVars[168].enum_names_ = "[u'on', u'off']";
    ObSysVars[168].value_ = "0";
    ObSysVars[168].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[168].id_ = SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME));
    ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME] = 168;
    ObSysVars[168].alias_ = "OB_SV_VALIDATE_PASSWORD_CHECK_USER_NAME";

    ObSysVars[169].info_ = "";
    ObSysVars[169].name_ = "validate_password_length";
    ObSysVars[169].data_type_ = ObUInt64Type;
    ObSysVars[169].value_ = "0";
    ObSysVars[169].min_val_ = "0";
    ObSysVars[169].max_val_ = "2147483647";
    ObSysVars[169].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[169].id_ = SYS_VAR_VALIDATE_PASSWORD_LENGTH;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_LENGTH));
    ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_LENGTH] = 169;
    ObSysVars[169].alias_ = "OB_SV_VALIDATE_PASSWORD_LENGTH";

    ObSysVars[170].info_ = "";
    ObSysVars[170].name_ = "validate_password_mixed_case_count";
    ObSysVars[170].data_type_ = ObUInt64Type;
    ObSysVars[170].value_ = "0";
    ObSysVars[170].min_val_ = "0";
    ObSysVars[170].max_val_ = "2147483647";
    ObSysVars[170].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[170].id_ = SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT));
    ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT] = 170;
    ObSysVars[170].alias_ = "OB_SV_VALIDATE_PASSWORD_MIXED_CASE_COUNT";

    ObSysVars[171].info_ = "";
    ObSysVars[171].name_ = "validate_password_number_count";
    ObSysVars[171].data_type_ = ObUInt64Type;
    ObSysVars[171].value_ = "0";
    ObSysVars[171].min_val_ = "0";
    ObSysVars[171].max_val_ = "2147483647";
    ObSysVars[171].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[171].id_ = SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT));
    ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT] = 171;
    ObSysVars[171].alias_ = "OB_SV_VALIDATE_PASSWORD_NUMBER_COUNT";

    ObSysVars[172].info_ = "";
    ObSysVars[172].name_ = "validate_password_policy";
    ObSysVars[172].data_type_ = ObIntType;
    ObSysVars[172].enum_names_ = "[u'low', u'medium']";
    ObSysVars[172].value_ = "0";
    ObSysVars[172].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[172].id_ = SYS_VAR_VALIDATE_PASSWORD_POLICY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_POLICY));
    ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_POLICY] = 172;
    ObSysVars[172].alias_ = "OB_SV_VALIDATE_PASSWORD_POLICY";

    ObSysVars[173].info_ = "";
    ObSysVars[173].name_ = "validate_password_special_char_count";
    ObSysVars[173].data_type_ = ObUInt64Type;
    ObSysVars[173].value_ = "0";
    ObSysVars[173].min_val_ = "0";
    ObSysVars[173].max_val_ = "2147483647";
    ObSysVars[173].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[173].id_ = SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT));
    ObSysVarsIdToArrayIdx[SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT] = 173;
    ObSysVars[173].alias_ = "OB_SV_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT";

    ObSysVars[174].info_ = "";
    ObSysVars[174].name_ = "default_password_lifetime";
    ObSysVars[174].data_type_ = ObUInt64Type;
    ObSysVars[174].value_ = "0";
    ObSysVars[174].min_val_ = "0";
    ObSysVars[174].max_val_ = "65535";
    ObSysVars[174].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[174].id_ = SYS_VAR_DEFAULT_PASSWORD_LIFETIME;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_DEFAULT_PASSWORD_LIFETIME));
    ObSysVarsIdToArrayIdx[SYS_VAR_DEFAULT_PASSWORD_LIFETIME] = 174;
    ObSysVars[174].alias_ = "OB_SV_DEFAULT_PASSWORD_LIFETIME";

    ObSysVars[175].info_ = "store trace info";
    ObSysVars[175].name_ = "ob_trace_info";
    ObSysVars[175].data_type_ = ObVarcharType;
    ObSysVars[175].value_ = "";
    ObSysVars[175].flags_ = ObSysVarFlag::SESSION_SCOPE;
    ObSysVars[175].id_ = SYS_VAR_OB_TRACE_INFO;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_TRACE_INFO));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_TRACE_INFO] = 175;
    ObSysVars[175].alias_ = "OB_SV_TRACE_INFO";

    ObSysVars[176].info_ = "enable use of batched multi statement";
    ObSysVars[176].name_ = "ob_enable_batched_multi_statement";
    ObSysVars[176].data_type_ = ObIntType;
    ObSysVars[176].value_ = "0";
    ObSysVars[176].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[176].id_ = SYS_VAR_OB_ENABLE_BATCHED_MULTI_STATEMENT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_ENABLE_BATCHED_MULTI_STATEMENT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_ENABLE_BATCHED_MULTI_STATEMENT] = 176;
    ObSysVars[176].alias_ = "OB_SV_ENABLE_BATCHED_MULTI_STATEMENT";

    ObSysVars[177].info_ = "least number of partitions per slave to start partition-based scan";
    ObSysVars[177].name_ = "_px_partition_scan_threshold";
    ObSysVars[177].data_type_ = ObIntType;
    ObSysVars[177].value_ = "64";
    ObSysVars[177].min_val_ = "0";
    ObSysVars[177].max_val_ = "100";
    ObSysVars[177].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[177].id_ = SYS_VAR__PX_PARTITION_SCAN_THRESHOLD;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_PARTITION_SCAN_THRESHOLD));
    ObSysVarsIdToArrayIdx[SYS_VAR__PX_PARTITION_SCAN_THRESHOLD] = 177;
    ObSysVars[177].alias_ = "OB_SV__PX_PARTITION_SCAN_THRESHOLD";

    ObSysVars[178].info_ = "broadcast optimization.";
    ObSysVars[178].name_ = "_ob_px_bcast_optimization";
    ObSysVars[178].data_type_ = ObIntType;
    ObSysVars[178].enum_names_ = "[u'WORKER', u'SERVER']";
    ObSysVars[178].value_ = "1";
    ObSysVars[178].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE |
                            ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[178].id_ = SYS_VAR__OB_PX_BCAST_OPTIMIZATION;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_PX_BCAST_OPTIMIZATION));
    ObSysVarsIdToArrayIdx[SYS_VAR__OB_PX_BCAST_OPTIMIZATION] = 178;
    ObSysVars[178].alias_ = "OB_SV__OB_PX_BCAST_OPTIMIZATION";

    ObSysVars[179].info_ = "percentage threshold to use slave mapping plan";
    ObSysVars[179].name_ = "_ob_px_slave_mapping_threshold";
    ObSysVars[179].data_type_ = ObIntType;
    ObSysVars[179].value_ = "200";
    ObSysVars[179].min_val_ = "0";
    ObSysVars[179].max_val_ = "1000";
    ObSysVars[179].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN |
                            ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[179].id_ = SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD));
    ObSysVarsIdToArrayIdx[SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD] = 179;
    ObSysVars[179].alias_ = "OB_SV__OB_PX_SLAVE_MAPPING_THRESHOLD";

    ObSysVars[180].info_ = "A DML statement can be parallelized only if you have explicitly enabled parallel DML in "
                           "the session or in the SQL statement.";
    ObSysVars[180].name_ = "_enable_parallel_dml";
    ObSysVars[180].data_type_ = ObIntType;
    ObSysVars[180].value_ = "0";
    ObSysVars[180].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE |
                            ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[180].id_ = SYS_VAR__ENABLE_PARALLEL_DML;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_DML));
    ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_PARALLEL_DML] = 180;
    ObSysVars[180].alias_ = "OB_SV__ENABLE_PARALLEL_DML";

    ObSysVars[181].info_ = "minimum number of rowid range granules to generate per slave.";
    ObSysVars[181].name_ = "_px_min_granules_per_slave";
    ObSysVars[181].data_type_ = ObIntType;
    ObSysVars[181].value_ = "13";
    ObSysVars[181].min_val_ = "0";
    ObSysVars[181].max_val_ = "100";
    ObSysVars[181].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::INVISIBLE | ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[181].id_ = SYS_VAR__PX_MIN_GRANULES_PER_SLAVE;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__PX_MIN_GRANULES_PER_SLAVE));
    ObSysVarsIdToArrayIdx[SYS_VAR__PX_MIN_GRANULES_PER_SLAVE] = 181;
    ObSysVars[181].alias_ = "OB_SV__PX_MIN_GRANULES_PER_SLAVE";

    ObSysVars[182].info_ = "limit the effect of data import and export operations";
    ObSysVars[182].name_ = "secure_file_priv";
    ObSysVars[182].data_type_ = ObVarcharType;
    ObSysVars[182].value_ = "NULL";
    ObSysVars[182].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::INFLUENCE_PLAN | ObSysVarFlag::NULLABLE;
    ObSysVars[182].id_ = SYS_VAR_SECURE_FILE_PRIV;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_SECURE_FILE_PRIV));
    ObSysVarsIdToArrayIdx[SYS_VAR_SECURE_FILE_PRIV] = 182;
    ObSysVars[182].alias_ = "OB_SV_SECURE_FILE_PRIV";

    ObSysVars[183].info_ = "enables or disables the reporting of warning messages by the PL/SQL compiler, and "
                           "specifies which warning messages to show as errors.";
    ObSysVars[183].name_ = "plsql_warnings";
    ObSysVars[183].data_type_ = ObVarcharType;
    ObSysVars[183].value_ = "ENABLE:ALL";
    ObSysVars[183].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[183].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_plsql_warnings";
    ObSysVars[183].id_ = SYS_VAR_PLSQL_WARNINGS;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PLSQL_WARNINGS));
    ObSysVarsIdToArrayIdx[SYS_VAR_PLSQL_WARNINGS] = 183;
    ObSysVars[183].alias_ = "OB_SV_PLSQL_WARNINGS";

    ObSysVars[184].info_ = "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY "
                           "in the session or in the SQL statement.";
    ObSysVars[184].name_ = "_enable_parallel_query";
    ObSysVars[184].data_type_ = ObIntType;
    ObSysVars[184].value_ = "1";
    ObSysVars[184].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE |
                            ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[184].id_ = SYS_VAR__ENABLE_PARALLEL_QUERY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__ENABLE_PARALLEL_QUERY));
    ObSysVarsIdToArrayIdx[SYS_VAR__ENABLE_PARALLEL_QUERY] = 184;
    ObSysVars[184].alias_ = "OB_SV__ENABLE_PARALLEL_QUERY";

    ObSysVars[185].info_ = "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY "
                           "in the session or in the SQL statement.";
    ObSysVars[185].name_ = "_force_parallel_query_dop";
    ObSysVars[185].data_type_ = ObUInt64Type;
    ObSysVars[185].value_ = "1";
    ObSysVars[185].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE |
                            ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[185].id_ = SYS_VAR__FORCE_PARALLEL_QUERY_DOP;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_QUERY_DOP));
    ObSysVarsIdToArrayIdx[SYS_VAR__FORCE_PARALLEL_QUERY_DOP] = 185;
    ObSysVars[185].alias_ = "OB_SV__FORCE_PARALLEL_QUERY_DOP";

    ObSysVars[186].info_ = "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY "
                           "in the session or in the SQL statement.";
    ObSysVars[186].name_ = "_force_parallel_dml_dop";
    ObSysVars[186].data_type_ = ObUInt64Type;
    ObSysVars[186].value_ = "1";
    ObSysVars[186].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE | ObSysVarFlag::INVISIBLE |
                            ObSysVarFlag::INFLUENCE_PLAN;
    ObSysVars[186].id_ = SYS_VAR__FORCE_PARALLEL_DML_DOP;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR__FORCE_PARALLEL_DML_DOP));
    ObSysVarsIdToArrayIdx[SYS_VAR__FORCE_PARALLEL_DML_DOP] = 186;
    ObSysVars[186].alias_ = "OB_SV__FORCE_PARALLEL_DML_DOP";

    ObSysVars[187].info_ = "PL/SQL timeout in microsecond(us)";
    ObSysVars[187].name_ = "ob_pl_block_timeout";
    ObSysVars[187].data_type_ = ObIntType;
    ObSysVars[187].value_ = "3216672000000000";
    ObSysVars[187].min_val_ = "0";
    ObSysVars[187].max_val_ = "9223372036854775807";
    ObSysVars[187].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[187].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large";
    ObSysVars[187].id_ = SYS_VAR_OB_PL_BLOCK_TIMEOUT;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_OB_PL_BLOCK_TIMEOUT));
    ObSysVarsIdToArrayIdx[SYS_VAR_OB_PL_BLOCK_TIMEOUT] = 187;
    ObSysVars[187].alias_ = "OB_SV_PL_BLOCK_TIMEOUT";

    ObSysVars[188].on_update_func_ = "ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope";
    ObSysVars[188].info_ = "Transaction access mode";
    ObSysVars[188].session_special_update_func_ = "ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only";
    ObSysVars[188].data_type_ = ObIntType;
    ObSysVars[188].value_ = "0";
    ObSysVars[188].flags_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE;
    ObSysVars[188].base_class_ = "ObSessionSpecialBoolSysVar";
    ObSysVars[188].on_check_and_convert_func_ = "ObSysVarOnCheckFuncs::check_and_convert_tx_read_only";
    ObSysVars[188].id_ = SYS_VAR_TRANSACTION_READ_ONLY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_TRANSACTION_READ_ONLY));
    ObSysVarsIdToArrayIdx[SYS_VAR_TRANSACTION_READ_ONLY] = 188;
    ObSysVars[188].name_ = "transaction_read_only";
    ObSysVars[188].alias_ = "OB_SV_TRANSACTION_READ_ONLY";

    ObSysVars[189].info_ = "specifies tenant resource plan.";
    ObSysVars[189].name_ = "resource_manager_plan";
    ObSysVars[189].data_type_ = ObVarcharType;
    ObSysVars[189].value_ = "";
    ObSysVars[189].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[189].id_ = SYS_VAR_RESOURCE_MANAGER_PLAN;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_RESOURCE_MANAGER_PLAN));
    ObSysVarsIdToArrayIdx[SYS_VAR_RESOURCE_MANAGER_PLAN] = 189;
    ObSysVars[189].alias_ = "OB_SV_RESOURCE_MANAGER_PLAN";

    ObSysVars[190].info_ = "indicate whether the Performance Schema is enabled";
    ObSysVars[190].name_ = "performance_schema";
    ObSysVars[190].data_type_ = ObIntType;
    ObSysVars[190].value_ = "0";
    ObSysVars[190].flags_ = ObSysVarFlag::GLOBAL_SCOPE;
    ObSysVars[190].id_ = SYS_VAR_PERFORMANCE_SCHEMA;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_PERFORMANCE_SCHEMA));
    ObSysVarsIdToArrayIdx[SYS_VAR_PERFORMANCE_SCHEMA] = 190;
    ObSysVars[190].alias_ = "OB_SV_PERFORMANCE_SCHEMA";

    ObSysVars[191].info_ = "specifies the string to use as the local currency symbol for the L number format element. "
                           "The default value of this parameter is determined by NLS_TERRITORY.";
    ObSysVars[191].name_ = "nls_currency";
    ObSysVars[191].data_type_ = ObVarcharType;
    ObSysVars[191].value_ = "$";
    ObSysVars[191].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[191].id_ = SYS_VAR_NLS_CURRENCY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_CURRENCY));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_CURRENCY] = 191;
    ObSysVars[191].alias_ = "OB_SV_NLS_CURRENCY";

    ObSysVars[192].info_ = "specifies the string to use as the international currency symbol for the C number format "
                           "element. The default value of this parameter is determined by NLS_TERRITORY";
    ObSysVars[192].name_ = "nls_iso_currency";
    ObSysVars[192].data_type_ = ObVarcharType;
    ObSysVars[192].value_ = "AMERICA";
    ObSysVars[192].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[192].id_ = SYS_VAR_NLS_ISO_CURRENCY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_ISO_CURRENCY));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_ISO_CURRENCY] = 192;
    ObSysVars[192].alias_ = "OB_SV_NLS_ISO_CURRENCY";

    ObSysVars[193].info_ = "specifies the dual currency symbol for the territory. The default is the dual currency "
                           "symbol defined in the territory of your current language environment.";
    ObSysVars[193].name_ = "nls_dual_currency";
    ObSysVars[193].data_type_ = ObVarcharType;
    ObSysVars[193].value_ = "$";
    ObSysVars[193].flags_ = ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::NEED_SERIALIZE |
                            ObSysVarFlag::ORACLE_ONLY;
    ObSysVars[193].id_ = SYS_VAR_NLS_DUAL_CURRENCY;
    cur_max_var_id = MAX(cur_max_var_id, static_cast<int64_t>(SYS_VAR_NLS_DUAL_CURRENCY));
    ObSysVarsIdToArrayIdx[SYS_VAR_NLS_DUAL_CURRENCY] = 193;
    ObSysVars[193].alias_ = "OB_SV_NLS_DUAL_CURRENCY";

    if (cur_max_var_id >= ObSysVarFactory::OB_MAX_SYS_VAR_ID) {
      HasInvalidSysVar = true;
    }
  }
} vars_init;

static int64_t var_amount = 194;

int64_t ObSysVariables::get_all_sys_var_count()
{
  return ObSysVarFactory::ALL_SYS_VARS_COUNT;
}
ObSysVarClassType ObSysVariables::get_sys_var_id(int64_t i)
{
  return ObSysVars[i].id_;
}
ObString ObSysVariables::get_name(int64_t i)
{
  return ObSysVars[i].name_;
}
ObObjType ObSysVariables::get_type(int64_t i)
{
  return ObSysVars[i].data_type_;
}
ObString ObSysVariables::get_value(int64_t i)
{
  return ObSysVars[i].value_;
}
ObString ObSysVariables::get_min(int64_t i)
{
  return ObSysVars[i].min_val_;
}
ObString ObSysVariables::get_max(int64_t i)
{
  return ObSysVars[i].max_val_;
}
ObString ObSysVariables::get_info(int64_t i)
{
  return ObSysVars[i].info_;
}
int64_t ObSysVariables::get_flags(int64_t i)
{
  return ObSysVars[i].flags_;
}
bool ObSysVariables::need_serialize(int64_t i)
{
  return ObSysVars[i].flags_ & ObSysVarFlag::NEED_SERIALIZE;
}
bool ObSysVariables::is_oracle_only(int64_t i)
{
  return ObSysVars[i].flags_ & ObSysVarFlag::ORACLE_ONLY;
}
bool ObSysVariables::is_mysql_only(int64_t i)
{
  return ObSysVars[i].flags_ & ObSysVarFlag::MYSQL_ONLY;
}
ObString ObSysVariables::get_alias(int64_t i)
{
  return ObSysVars[i].alias_;
}
const ObObj& ObSysVariables::get_default_value(int64_t i)
{
  return ObSysVarDefaultValues[i];
}
int64_t ObSysVariables::get_amount()
{
  return var_amount;
}

int ObSysVariables::set_value(const char* name, const char* new_value)
{
  ObString tmp_name(static_cast<int32_t>(strlen(name)), name);
  ObString tmp_value(static_cast<int32_t>(strlen(new_value)), new_value);
  return set_value(tmp_name, tmp_value);
}
int ObSysVariables::set_value(const common::ObString& name, const common::ObString& new_value)
{
  int ret = OB_SUCCESS;
  bool name_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && false == name_exist && i < var_amount; ++i) {
    if (0 == ObSysVars[i].name_.compare(name)) {
      ObSysVars[i].value_.assign_ptr(new_value.ptr(), new_value.length());
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
    const ObString& sys_var_val_str = ObSysVariables::get_value(i);
    const ObObjType sys_var_type = ObSysVariables::get_type(i);
    const ObSysVarClassType sys_var_id = ObSysVariables::get_sys_var_id(i);
    if (OB_UNLIKELY(sys_var_type == ObTimestampType)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("need tz_info when cast to timestamp", K(ret), K(sys_var_val_str));
    } else {
      ObObj in_obj;
      ObObj out_obj;
      in_obj.set_varchar(sys_var_val_str);
      in_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      // varchar to others. so, no need to get collation from session
      ObCastCtx cast_ctx(&ObSysVarAllocator, NULL, 0, CM_NONE, CS_TYPE_INVALID, NULL);
      if (OB_FAIL(ObObjCaster::to_type(sys_var_type, cast_ctx, in_obj, out_obj))) {
        ObString sys_var_name = ObSysVariables::get_name(i);
        LOG_WARN("fail to cast object",
            K(ret),
            "cell",
            in_obj,
            "from_type",
            ob_obj_type_str(in_obj.get_type()),
            "to_type",
            ob_obj_type_str(sys_var_type),
            K(sys_var_name),
            K(i));
      } else {
        if (ob_is_string_type(out_obj.get_type())) {
          out_obj.set_collation_level(CS_LEVEL_SYSCONST);
        }
        ObSysVarDefaultValues[i] = out_obj;
      }
    }
  }
  return ret;
}

int64_t ObSysVarsToIdxMap::get_store_idx(int64_t var_id)
{
  return ObSysVarsIdToArrayIdx[var_id];
}
bool ObSysVarsToIdxMap::has_invalid_sys_var_id()
{
  return HasInvalidSysVar;
}

}  // end namespace share
}  // end namespace oceanbase
