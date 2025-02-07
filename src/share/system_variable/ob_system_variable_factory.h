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

#ifndef OCEANBASE_SHARE_SYSTEM_VARIABLE_OB_SYSTEM_VARIABLE_FACTORY_
#define OCEANBASE_SHARE_SYSTEM_VARIABLE_OB_SYSTEM_VARIABLE_FACTORY_
#include "common/object/ob_object.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/system_variable/ob_system_variable_init.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace share
{
class ObSysVarAutoIncrementIncrement : public ObIntSysVar
{
public:
  ObSysVarAutoIncrementIncrement() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_AUTO_INCREMENT_INCREMENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(0); }
};
class ObSysVarAutoIncrementOffset : public ObIntSysVar
{
public:
  ObSysVarAutoIncrementOffset() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_AUTO_INCREMENT_OFFSET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(1); }
};
class ObSysVarAutocommit : public ObBoolSysVar
{
public:
  ObSysVarAutocommit() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_AUTOCOMMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(2); }
};
class ObSysVarCharacterSetClient : public ObCharsetSysVar
{
public:
  ObSysVarCharacterSetClient() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SET_CLIENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(3); }
};
class ObSysVarCharacterSetConnection : public ObCharsetSysVar
{
public:
  ObSysVarCharacterSetConnection() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SET_CONNECTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(4); }
};
class ObSysVarCharacterSetDatabase : public ObCharsetSysVar
{
public:
  ObSysVarCharacterSetDatabase() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SET_DATABASE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(5); }
};
class ObSysVarCharacterSetResults : public ObCharsetSysVar
{
public:
  ObSysVarCharacterSetResults() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SET_RESULTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(6); }
};
class ObSysVarCharacterSetServer : public ObCharsetSysVar
{
public:
  ObSysVarCharacterSetServer() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SET_SERVER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(7); }
};
class ObSysVarCharacterSetSystem : public ObCharsetSysVar
{
public:
  ObSysVarCharacterSetSystem() : ObCharsetSysVar(NULL, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SET_SYSTEM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(8); }
};
class ObSysVarCollationConnection : public ObCharsetSysVar
{
public:
  ObSysVarCollationConnection() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_collation_not_null, NULL, ObSysVarToObjFuncs::to_obj_collation, ObSysVarToStrFuncs::to_str_collation, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_COLLATION_CONNECTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(9); }
};
class ObSysVarCollationDatabase : public ObCharsetSysVar
{
public:
  ObSysVarCollationDatabase() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_collation_not_null, NULL, ObSysVarToObjFuncs::to_obj_collation, ObSysVarToStrFuncs::to_str_collation, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_COLLATION_DATABASE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(10); }
};
class ObSysVarCollationServer : public ObCharsetSysVar
{
public:
  ObSysVarCollationServer() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_collation_not_null, NULL, ObSysVarToObjFuncs::to_obj_collation, ObSysVarToStrFuncs::to_str_collation, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_COLLATION_SERVER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(11); }
};
class ObSysVarInteractiveTimeout : public ObIntSysVar
{
public:
  ObSysVarInteractiveTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INTERACTIVE_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(12); }
};
class ObSysVarLastInsertId : public ObSessionSpecialIntSysVar
{
public:
  ObSysVarLastInsertId() : ObSessionSpecialIntSysVar(NULL, ObSysVarSessionSpecialUpdateFuncs::update_last_insert_id, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LAST_INSERT_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(13); }
};
class ObSysVarMaxAllowedPacket : public ObIntSysVar
{
public:
  ObSysVarMaxAllowedPacket() : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_max_allowed_packet, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_ALLOWED_PACKET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(14); }
};
class ObSysVarSqlMode : public ObSqlModeVar
{
public:
  ObSysVarSqlMode() : ObSqlModeVar(ObSysVarOnCheckFuncs::check_and_convert_sql_mode, ObSysVarOnUpdateFuncs::update_sql_mode, ObSysVarToObjFuncs::to_obj_sql_mode, ObSysVarToStrFuncs::to_str_sql_mode, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(15); }
};
class ObSysVarTimeZone : public ObTimeZoneSysVar
{
public:
  ObSysVarTimeZone() : ObTimeZoneSysVar(ObSysVarOnCheckFuncs::check_and_convert_time_zone, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TIME_ZONE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(16); }
};
class ObSysVarTxIsolation : public ObSessionSpecialVarcharSysVar
{
public:
  ObSysVarTxIsolation() : ObSessionSpecialVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_isolation, ObSysVarOnUpdateFuncs::update_tx_isolation, ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TX_ISOLATION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(17); }
};
class ObSysVarVersionComment : public ObVarcharSysVar
{
public:
  ObSysVarVersionComment() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VERSION_COMMENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(18); }
};
class ObSysVarWaitTimeout : public ObIntSysVar
{
public:
  ObSysVarWaitTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_WAIT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(19); }
};
class ObSysVarBinlogRowImage : public ObEnumSysVar
{
public:
  const static char * BINLOG_ROW_IMAGE_NAMES[];
public:
  ObSysVarBinlogRowImage() : ObEnumSysVar(BINLOG_ROW_IMAGE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_ROW_IMAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(20); }
};
class ObSysVarCharacterSetFilesystem : public ObCharsetSysVar
{
public:
  ObSysVarCharacterSetFilesystem() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SET_FILESYSTEM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(21); }
};
class ObSysVarConnectTimeout : public ObIntSysVar
{
public:
  ObSysVarConnectTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CONNECT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(22); }
};
class ObSysVarDatadir : public ObVarcharSysVar
{
public:
  ObSysVarDatadir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DATADIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(23); }
};
class ObSysVarDebugSync : public ObVarcharSysVar
{
public:
  ObSysVarDebugSync() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEBUG_SYNC; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(24); }
};
class ObSysVarDivPrecisionIncrement : public ObIntSysVar
{
public:
  ObSysVarDivPrecisionIncrement() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DIV_PRECISION_INCREMENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(25); }
};
class ObSysVarExplicitDefaultsForTimestamp : public ObBoolSysVar
{
public:
  ObSysVarExplicitDefaultsForTimestamp() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(26); }
};
class ObSysVarGroupConcatMaxLen : public ObIntSysVar
{
public:
  ObSysVarGroupConcatMaxLen() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_CONCAT_MAX_LEN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(27); }
};
class ObSysVarIdentity : public ObSessionSpecialIntSysVar
{
public:
  ObSysVarIdentity() : ObSessionSpecialIntSysVar(NULL, ObSysVarSessionSpecialUpdateFuncs::update_identity, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_IDENTITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(28); }
};
class ObSysVarLowerCaseTableNames : public ObIntSysVar
{
public:
  ObSysVarLowerCaseTableNames() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOWER_CASE_TABLE_NAMES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(29); }
};
class ObSysVarNetReadTimeout : public ObIntSysVar
{
public:
  ObSysVarNetReadTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NET_READ_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(30); }
};
class ObSysVarNetWriteTimeout : public ObIntSysVar
{
public:
  ObSysVarNetWriteTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NET_WRITE_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(31); }
};
class ObSysVarReadOnly : public ObBoolSysVar
{
public:
  ObSysVarReadOnly() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_READ_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(32); }
};
class ObSysVarSqlAutoIsNull : public ObBoolSysVar
{
public:
  ObSysVarSqlAutoIsNull() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_AUTO_IS_NULL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(33); }
};
class ObSysVarSqlSelectLimit : public ObIntSysVar
{
public:
  ObSysVarSqlSelectLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_SELECT_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(34); }
};
class ObSysVarTimestamp : public ObNumericSysVar
{
public:
  ObSysVarTimestamp() : ObNumericSysVar(ObSysVarOnCheckFuncs::check_and_convert_max_min_timestamp, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TIMESTAMP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(35); }
};
class ObSysVarTxReadOnly : public ObSessionSpecialBoolSysVar
{
public:
  ObSysVarTxReadOnly() : ObSessionSpecialBoolSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_read_only, ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope, ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TX_READ_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(36); }
};
class ObSysVarVersion : public ObVarcharSysVar
{
public:
  ObSysVarVersion() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(37); }
};
class ObSysVarSqlWarnings : public ObBoolSysVar
{
public:
  ObSysVarSqlWarnings() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_WARNINGS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(38); }
};
class ObSysVarMaxUserConnections : public ObIntSysVar
{
public:
  ObSysVarMaxUserConnections() : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_max_user_connections, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_USER_CONNECTIONS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(39); }
};
class ObSysVarInitConnect : public ObVarcharSysVar
{
public:
  ObSysVarInitConnect() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INIT_CONNECT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(40); }
};
class ObSysVarLicense : public ObVarcharSysVar
{
public:
  ObSysVarLicense() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LICENSE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(41); }
};
class ObSysVarNetBufferLength : public ObIntSysVar
{
public:
  ObSysVarNetBufferLength() : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_net_buffer_length, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NET_BUFFER_LENGTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(42); }
};
class ObSysVarSystemTimeZone : public ObVarcharSysVar
{
public:
  ObSysVarSystemTimeZone() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SYSTEM_TIME_ZONE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(43); }
};
class ObSysVarQueryCacheSize : public ObIntSysVar
{
public:
  ObSysVarQueryCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(44); }
};
class ObSysVarQueryCacheType : public ObEnumSysVar
{
public:
  const static char * QUERY_CACHE_TYPE_NAMES[];
public:
  ObSysVarQueryCacheType() : ObEnumSysVar(QUERY_CACHE_TYPE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_CACHE_TYPE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(45); }
};
class ObSysVarSqlQuoteShowCreate : public ObBoolSysVar
{
public:
  ObSysVarSqlQuoteShowCreate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_QUOTE_SHOW_CREATE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(46); }
};
class ObSysVarMaxSpRecursionDepth : public ObIntSysVar
{
public:
  ObSysVarMaxSpRecursionDepth() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_SP_RECURSION_DEPTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(47); }
};
class ObSysVarSqlSafeUpdates : public ObBoolSysVar
{
public:
  ObSysVarSqlSafeUpdates() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_SAFE_UPDATES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(48); }
};
class ObSysVarConcurrentInsert : public ObVarcharSysVar
{
public:
  ObSysVarConcurrentInsert() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CONCURRENT_INSERT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(49); }
};
class ObSysVarDefaultAuthenticationPlugin : public ObVarcharSysVar
{
public:
  ObSysVarDefaultAuthenticationPlugin() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(50); }
};
class ObSysVarDisabledStorageEngines : public ObVarcharSysVar
{
public:
  ObSysVarDisabledStorageEngines() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DISABLED_STORAGE_ENGINES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(51); }
};
class ObSysVarErrorCount : public ObIntSysVar
{
public:
  ObSysVarErrorCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_ERROR_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(52); }
};
class ObSysVarGeneralLog : public ObBoolSysVar
{
public:
  ObSysVarGeneralLog() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GENERAL_LOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(53); }
};
class ObSysVarHaveOpenssl : public ObVarcharSysVar
{
public:
  ObSysVarHaveOpenssl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_OPENSSL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(54); }
};
class ObSysVarHaveProfiling : public ObVarcharSysVar
{
public:
  ObSysVarHaveProfiling() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_PROFILING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(55); }
};
class ObSysVarHaveSsl : public ObVarcharSysVar
{
public:
  ObSysVarHaveSsl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_SSL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(56); }
};
class ObSysVarHostname : public ObVarcharSysVar
{
public:
  ObSysVarHostname() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HOSTNAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(57); }
};
class ObSysVarLcMessages : public ObVarcharSysVar
{
public:
  ObSysVarLcMessages() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LC_MESSAGES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(58); }
};
class ObSysVarLocalInfile : public ObBoolSysVar
{
public:
  ObSysVarLocalInfile() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOCAL_INFILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(59); }
};
class ObSysVarLockWaitTimeout : public ObIntSysVar
{
public:
  ObSysVarLockWaitTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOCK_WAIT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(60); }
};
class ObSysVarLongQueryTime : public ObNumericSysVar
{
public:
  ObSysVarLongQueryTime() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LONG_QUERY_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(61); }
};
class ObSysVarMaxConnections : public ObIntSysVar
{
public:
  ObSysVarMaxConnections() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_CONNECTIONS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(62); }
};
class ObSysVarMaxExecutionTime : public ObIntSysVar
{
public:
  ObSysVarMaxExecutionTime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_EXECUTION_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(63); }
};
class ObSysVarProtocolVersion : public ObIntSysVar
{
public:
  ObSysVarProtocolVersion() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PROTOCOL_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(64); }
};
class ObSysVarServerId : public ObIntSysVar
{
public:
  ObSysVarServerId() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SERVER_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(65); }
};
class ObSysVarSslCa : public ObVarcharSysVar
{
public:
  ObSysVarSslCa() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SSL_CA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(66); }
};
class ObSysVarSslCapath : public ObVarcharSysVar
{
public:
  ObSysVarSslCapath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SSL_CAPATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(67); }
};
class ObSysVarSslCert : public ObVarcharSysVar
{
public:
  ObSysVarSslCert() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SSL_CERT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(68); }
};
class ObSysVarSslCipher : public ObVarcharSysVar
{
public:
  ObSysVarSslCipher() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SSL_CIPHER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(69); }
};
class ObSysVarSslCrl : public ObVarcharSysVar
{
public:
  ObSysVarSslCrl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SSL_CRL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(70); }
};
class ObSysVarSslCrlpath : public ObVarcharSysVar
{
public:
  ObSysVarSslCrlpath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SSL_CRLPATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(71); }
};
class ObSysVarSslKey : public ObVarcharSysVar
{
public:
  ObSysVarSslKey() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SSL_KEY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(72); }
};
class ObSysVarTimeFormat : public ObVarcharSysVar
{
public:
  ObSysVarTimeFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TIME_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(73); }
};
class ObSysVarTlsVersion : public ObVarcharSysVar
{
public:
  ObSysVarTlsVersion() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TLS_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(74); }
};
class ObSysVarTmpTableSize : public ObIntSysVar
{
public:
  ObSysVarTmpTableSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TMP_TABLE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(75); }
};
class ObSysVarTmpdir : public ObVarcharSysVar
{
public:
  ObSysVarTmpdir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TMPDIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(76); }
};
class ObSysVarUniqueChecks : public ObBoolSysVar
{
public:
  ObSysVarUniqueChecks() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_UNIQUE_CHECKS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(77); }
};
class ObSysVarVersionCompileMachine : public ObVarcharSysVar
{
public:
  ObSysVarVersionCompileMachine() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VERSION_COMPILE_MACHINE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(78); }
};
class ObSysVarVersionCompileOs : public ObVarcharSysVar
{
public:
  ObSysVarVersionCompileOs() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VERSION_COMPILE_OS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(79); }
};
class ObSysVarWarningCount : public ObIntSysVar
{
public:
  ObSysVarWarningCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_WARNING_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(80); }
};
class ObSysVarSessionTrackSchema : public ObBoolSysVar
{
public:
  ObSysVarSessionTrackSchema() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SESSION_TRACK_SCHEMA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(81); }
};
class ObSysVarSessionTrackSystemVariables : public ObVarcharSysVar
{
public:
  ObSysVarSessionTrackSystemVariables() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SESSION_TRACK_SYSTEM_VARIABLES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(82); }
};
class ObSysVarSessionTrackStateChange : public ObBoolSysVar
{
public:
  ObSysVarSessionTrackStateChange() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SESSION_TRACK_STATE_CHANGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(83); }
};
class ObSysVarHaveQueryCache : public ObVarcharSysVar
{
public:
  ObSysVarHaveQueryCache() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_QUERY_CACHE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(84); }
};
class ObSysVarQueryCacheLimit : public ObIntSysVar
{
public:
  ObSysVarQueryCacheLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_CACHE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(85); }
};
class ObSysVarQueryCacheMinResUnit : public ObIntSysVar
{
public:
  ObSysVarQueryCacheMinResUnit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_CACHE_MIN_RES_UNIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(86); }
};
class ObSysVarQueryCacheWlockInvalidate : public ObBoolSysVar
{
public:
  ObSysVarQueryCacheWlockInvalidate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_CACHE_WLOCK_INVALIDATE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(87); }
};
class ObSysVarBinlogFormat : public ObEnumSysVar
{
public:
  const static char * BINLOG_FORMAT_NAMES[];
public:
  ObSysVarBinlogFormat() : ObEnumSysVar(BINLOG_FORMAT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(88); }
};
class ObSysVarBinlogChecksum : public ObVarcharSysVar
{
public:
  ObSysVarBinlogChecksum() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_CHECKSUM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(89); }
};
class ObSysVarBinlogRowsQueryLogEvents : public ObBoolSysVar
{
public:
  ObSysVarBinlogRowsQueryLogEvents() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_ROWS_QUERY_LOG_EVENTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(90); }
};
class ObSysVarLogBin : public ObBoolSysVar
{
public:
  ObSysVarLogBin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_BIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(91); }
};
class ObSysVarServerUuid : public ObVarcharSysVar
{
public:
  ObSysVarServerUuid() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SERVER_UUID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(92); }
};
class ObSysVarDefaultStorageEngine : public ObVarcharSysVar
{
public:
  ObSysVarDefaultStorageEngine() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEFAULT_STORAGE_ENGINE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(93); }
};
class ObSysVarCteMaxRecursionDepth : public ObIntSysVar
{
public:
  ObSysVarCteMaxRecursionDepth() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CTE_MAX_RECURSION_DEPTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(94); }
};
class ObSysVarRegexpStackLimit : public ObIntSysVar
{
public:
  ObSysVarRegexpStackLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REGEXP_STACK_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(95); }
};
class ObSysVarRegexpTimeLimit : public ObIntSysVar
{
public:
  ObSysVarRegexpTimeLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REGEXP_TIME_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(96); }
};
class ObSysVarProfiling : public ObEnumSysVar
{
public:
  const static char * PROFILING_NAMES[];
public:
  ObSysVarProfiling() : ObEnumSysVar(PROFILING_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PROFILING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(97); }
};
class ObSysVarProfilingHistorySize : public ObIntSysVar
{
public:
  ObSysVarProfilingHistorySize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PROFILING_HISTORY_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(98); }
};
class ObSysVarObIntermResultMemLimit : public ObIntSysVar
{
public:
  ObSysVarObIntermResultMemLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(99); }
};
class ObSysVarObProxyPartitionHit : public ObBoolSysVar
{
public:
  ObSysVarObProxyPartitionHit() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PROXY_PARTITION_HIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(100); }
};
class ObSysVarObLogLevel : public ObVarcharSysVar
{
public:
  ObSysVarObLogLevel() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_LOG_LEVEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(101); }
};
class ObSysVarObQueryTimeout : public ObIntSysVar
{
public:
  ObSysVarObQueryTimeout() : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_QUERY_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(102); }
};
class ObSysVarObReadConsistency : public ObEnumSysVar
{
public:
  const static char * OB_READ_CONSISTENCY_NAMES[];
public:
  ObSysVarObReadConsistency() : ObEnumSysVar(OB_READ_CONSISTENCY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_READ_CONSISTENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(103); }
};
class ObSysVarObEnableTransformation : public ObBoolSysVar
{
public:
  ObSysVarObEnableTransformation() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_TRANSFORMATION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(104); }
};
class ObSysVarObTrxTimeout : public ObIntSysVar
{
public:
  ObSysVarObTrxTimeout() : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_TRX_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(105); }
};
class ObSysVarObEnablePlanCache : public ObBoolSysVar
{
public:
  ObSysVarObEnablePlanCache() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_PLAN_CACHE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(106); }
};
class ObSysVarObEnableIndexDirectSelect : public ObBoolSysVar
{
public:
  ObSysVarObEnableIndexDirectSelect() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(107); }
};
class ObSysVarObProxySetTrxExecuted : public ObBoolSysVar
{
public:
  ObSysVarObProxySetTrxExecuted() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PROXY_SET_TRX_EXECUTED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(108); }
};
class ObSysVarObEnableAggregationPushdown : public ObBoolSysVar
{
public:
  ObSysVarObEnableAggregationPushdown() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(109); }
};
class ObSysVarObLastSchemaVersion : public ObIntSysVar
{
public:
  ObSysVarObLastSchemaVersion() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_LAST_SCHEMA_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(110); }
};
class ObSysVarObGlobalDebugSync : public ObVarcharSysVar
{
public:
  ObSysVarObGlobalDebugSync() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_GLOBAL_DEBUG_SYNC; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(111); }
};
class ObSysVarObProxyGlobalVariablesVersion : public ObIntSysVar
{
public:
  ObSysVarObProxyGlobalVariablesVersion() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(112); }
};
class ObSysVarObEnableShowTrace : public ObBoolSysVar
{
public:
  ObSysVarObEnableShowTrace() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_SHOW_TRACE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(113); }
};
class ObSysVarObBnlJoinCacheSize : public ObIntSysVar
{
public:
  ObSysVarObBnlJoinCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_BNL_JOIN_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(114); }
};
class ObSysVarObProxyUserPrivilege : public ObIntSysVar
{
public:
  ObSysVarObProxyUserPrivilege() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PROXY_USER_PRIVILEGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(115); }
};
class ObSysVarObOrgClusterId : public ObStrictRangeIntSysVar
{
public:
  ObSysVarObOrgClusterId() : ObStrictRangeIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_ob_org_cluster_id, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ORG_CLUSTER_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(116); }
};
class ObSysVarObPlanCachePercentage : public ObIntSysVar
{
public:
  ObSysVarObPlanCachePercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PLAN_CACHE_PERCENTAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(117); }
};
class ObSysVarObPlanCacheEvictHighPercentage : public ObIntSysVar
{
public:
  ObSysVarObPlanCacheEvictHighPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(118); }
};
class ObSysVarObPlanCacheEvictLowPercentage : public ObIntSysVar
{
public:
  ObSysVarObPlanCacheEvictLowPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(119); }
};
class ObSysVarRecyclebin : public ObBoolSysVar
{
public:
  ObSysVarRecyclebin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RECYCLEBIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(120); }
};
class ObSysVarObCapabilityFlag : public ObIntSysVar
{
public:
  ObSysVarObCapabilityFlag() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_CAPABILITY_FLAG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(121); }
};
class ObSysVarIsResultAccurate : public ObBoolSysVar
{
public:
  ObSysVarIsResultAccurate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_IS_RESULT_ACCURATE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(122); }
};
class ObSysVarErrorOnOverlapTime : public ObBoolSysVar
{
public:
  ObSysVarErrorOnOverlapTime() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_ERROR_ON_OVERLAP_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(123); }
};
class ObSysVarObCompatibilityMode : public ObEnumSysVar
{
public:
  const static char * OB_COMPATIBILITY_MODE_NAMES[];
public:
  ObSysVarObCompatibilityMode() : ObEnumSysVar(OB_COMPATIBILITY_MODE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_COMPATIBILITY_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(124); }
};
class ObSysVarObSqlWorkAreaPercentage : public ObIntSysVar
{
public:
  ObSysVarObSqlWorkAreaPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(125); }
};
class ObSysVarObSafeWeakReadSnapshot : public ObIntSysVar
{
public:
  ObSysVarObSafeWeakReadSnapshot() : ObIntSysVar(NULL, ObSysVarOnUpdateFuncs::update_safe_weak_read_snapshot, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(126); }
};
class ObSysVarObRoutePolicy : public ObEnumSysVar
{
public:
  const static char * OB_ROUTE_POLICY_NAMES[];
public:
  ObSysVarObRoutePolicy() : ObEnumSysVar(OB_ROUTE_POLICY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ROUTE_POLICY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(127); }
};
class ObSysVarObEnableTransmissionChecksum : public ObBoolSysVar
{
public:
  ObSysVarObEnableTransmissionChecksum() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(128); }
};
class ObSysVarForeignKeyChecks : public ObBoolSysVar
{
public:
  ObSysVarForeignKeyChecks() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_FOREIGN_KEY_CHECKS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(129); }
};
class ObSysVarObStatementTraceId : public ObVarcharSysVar
{
public:
  ObSysVarObStatementTraceId() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_STATEMENT_TRACE_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(130); }
};
class ObSysVarObEnableTruncateFlashback : public ObBoolSysVar
{
public:
  ObSysVarObEnableTruncateFlashback() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(131); }
};
class ObSysVarObTcpInvitedNodes : public ObVarcharSysVar
{
public:
  ObSysVarObTcpInvitedNodes() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_TCP_INVITED_NODES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(132); }
};
class ObSysVarSqlThrottleCurrentPriority : public ObIntSysVar
{
public:
  ObSysVarSqlThrottleCurrentPriority() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(133); }
};
class ObSysVarSqlThrottlePriority : public ObIntSysVar
{
public:
  ObSysVarSqlThrottlePriority() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_THROTTLE_PRIORITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(134); }
};
class ObSysVarSqlThrottleRt : public ObNumericSysVar
{
public:
  ObSysVarSqlThrottleRt() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_THROTTLE_RT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(135); }
};
class ObSysVarSqlThrottleCpu : public ObNumericSysVar
{
public:
  ObSysVarSqlThrottleCpu() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_THROTTLE_CPU; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(136); }
};
class ObSysVarSqlThrottleIo : public ObIntSysVar
{
public:
  ObSysVarSqlThrottleIo() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_THROTTLE_IO; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(137); }
};
class ObSysVarSqlThrottleNetwork : public ObNumericSysVar
{
public:
  ObSysVarSqlThrottleNetwork() : ObNumericSysVar(ObSysVarOnCheckFuncs::check_and_convert_sql_throttle_queue_time, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_THROTTLE_NETWORK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(138); }
};
class ObSysVarSqlThrottleLogicalReads : public ObIntSysVar
{
public:
  ObSysVarSqlThrottleLogicalReads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_THROTTLE_LOGICAL_READS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(139); }
};
class ObSysVarAutoIncrementCacheSize : public ObIntSysVar
{
public:
  ObSysVarAutoIncrementCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_AUTO_INCREMENT_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(140); }
};
class ObSysVarObEnableJit : public ObEnumSysVar
{
public:
  const static char * OB_ENABLE_JIT_NAMES[];
public:
  ObSysVarObEnableJit() : ObEnumSysVar(OB_ENABLE_JIT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_JIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(141); }
};
class ObSysVarObTempTablespaceSizePercentage : public ObIntSysVar
{
public:
  ObSysVarObTempTablespaceSizePercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(142); }
};
class ObSysVarPluginDir : public ObVarcharSysVar
{
public:
  ObSysVarPluginDir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PLUGIN_DIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(143); }
};
class ObSysVarObSqlAuditPercentage : public ObIntSysVar
{
public:
  ObSysVarObSqlAuditPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_SQL_AUDIT_PERCENTAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(144); }
};
class ObSysVarObEnableSqlAudit : public ObBoolSysVar
{
public:
  ObSysVarObEnableSqlAudit() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_SQL_AUDIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(145); }
};
class ObSysVarOptimizerUseSqlPlanBaselines : public ObBoolSysVar
{
public:
  ObSysVarOptimizerUseSqlPlanBaselines() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(146); }
};
class ObSysVarOptimizerCaptureSqlPlanBaselines : public ObBoolSysVar
{
public:
  ObSysVarOptimizerCaptureSqlPlanBaselines() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(147); }
};
class ObSysVarParallelServersTarget : public ObIntSysVar
{
public:
  ObSysVarParallelServersTarget() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PARALLEL_SERVERS_TARGET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(148); }
};
class ObSysVarObEarlyLockRelease : public ObBoolSysVar
{
public:
  ObSysVarObEarlyLockRelease() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_EARLY_LOCK_RELEASE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(149); }
};
class ObSysVarObTrxIdleTimeout : public ObIntSysVar
{
public:
  ObSysVarObTrxIdleTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_TRX_IDLE_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(150); }
};
class ObSysVarBlockEncryptionMode : public ObEnumSysVar
{
public:
  const static char * BLOCK_ENCRYPTION_MODE_NAMES[];
public:
  ObSysVarBlockEncryptionMode() : ObEnumSysVar(BLOCK_ENCRYPTION_MODE_NAMES, ObSysVarOnCheckFuncs::check_and_convert_block_encryption_mode, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BLOCK_ENCRYPTION_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(151); }
};
class ObSysVarNlsDateFormat : public ObVarcharSysVar
{
public:
  ObSysVarNlsDateFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_DATE_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(152); }
};
class ObSysVarNlsTimestampFormat : public ObVarcharSysVar
{
public:
  ObSysVarNlsTimestampFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_TIMESTAMP_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(153); }
};
class ObSysVarNlsTimestampTzFormat : public ObVarcharSysVar
{
public:
  ObSysVarNlsTimestampTzFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(154); }
};
class ObSysVarObReservedMetaMemoryPercentage : public ObIntSysVar
{
public:
  ObSysVarObReservedMetaMemoryPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(155); }
};
class ObSysVarObCheckSysVariable : public ObBoolSysVar
{
public:
  ObSysVarObCheckSysVariable() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_CHECK_SYS_VARIABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(156); }
};
class ObSysVarNlsLanguage : public ObVarcharSysVar
{
public:
  ObSysVarNlsLanguage() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_LANGUAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(157); }
};
class ObSysVarNlsTerritory : public ObVarcharSysVar
{
public:
  ObSysVarNlsTerritory() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_TERRITORY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(158); }
};
class ObSysVarNlsSort : public ObVarcharSysVar
{
public:
  ObSysVarNlsSort() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_SORT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(159); }
};
class ObSysVarNlsComp : public ObVarcharSysVar
{
public:
  ObSysVarNlsComp() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_COMP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(160); }
};
class ObSysVarNlsCharacterset : public ObVarcharSysVar
{
public:
  ObSysVarNlsCharacterset() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_CHARACTERSET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(161); }
};
class ObSysVarNlsNcharCharacterset : public ObVarcharSysVar
{
public:
  ObSysVarNlsNcharCharacterset() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_NCHAR_CHARACTERSET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(162); }
};
class ObSysVarNlsDateLanguage : public ObVarcharSysVar
{
public:
  ObSysVarNlsDateLanguage() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_DATE_LANGUAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(163); }
};
class ObSysVarNlsLengthSemantics : public ObVarcharSysVar
{
public:
  ObSysVarNlsLengthSemantics() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_nls_length_semantics_is_valid, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_LENGTH_SEMANTICS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(164); }
};
class ObSysVarNlsNcharConvExcp : public ObVarcharSysVar
{
public:
  ObSysVarNlsNcharConvExcp() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_NCHAR_CONV_EXCP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(165); }
};
class ObSysVarNlsCalendar : public ObVarcharSysVar
{
public:
  ObSysVarNlsCalendar() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_CALENDAR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(166); }
};
class ObSysVarNlsNumericCharacters : public ObVarcharSysVar
{
public:
  ObSysVarNlsNumericCharacters() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_NUMERIC_CHARACTERS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(167); }
};
class ObSysVarNljBatchingEnabled : public ObBoolSysVar
{
public:
  ObSysVarNljBatchingEnabled() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__NLJ_BATCHING_ENABLED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(168); }
};
class ObSysVarTracefileIdentifier : public ObVarcharSysVar
{
public:
  ObSysVarTracefileIdentifier() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TRACEFILE_IDENTIFIER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(169); }
};
class ObSysVarGroupbyNopushdownCutRatio : public ObIntSysVar
{
public:
  ObSysVarGroupbyNopushdownCutRatio() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(170); }
};
class ObSysVarPxBroadcastFudgeFactor : public ObIntSysVar
{
public:
  ObSysVarPxBroadcastFudgeFactor() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__PX_BROADCAST_FUDGE_FACTOR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(171); }
};
class ObSysVarTransactionIsolation : public ObSessionSpecialVarcharSysVar
{
public:
  ObSysVarTransactionIsolation() : ObSessionSpecialVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_isolation, ObSysVarOnUpdateFuncs::update_tx_isolation, ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TRANSACTION_ISOLATION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(172); }
};
class ObSysVarObTrxLockTimeout : public ObIntSysVar
{
public:
  ObSysVarObTrxLockTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_TRX_LOCK_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(173); }
};
class ObSysVarValidatePasswordCheckUserName : public ObEnumSysVar
{
public:
  const static char * VALIDATE_PASSWORD_CHECK_USER_NAME_NAMES[];
public:
  ObSysVarValidatePasswordCheckUserName() : ObEnumSysVar(VALIDATE_PASSWORD_CHECK_USER_NAME_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(174); }
};
class ObSysVarValidatePasswordLength : public ObIntSysVar
{
public:
  ObSysVarValidatePasswordLength() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VALIDATE_PASSWORD_LENGTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(175); }
};
class ObSysVarValidatePasswordMixedCaseCount : public ObIntSysVar
{
public:
  ObSysVarValidatePasswordMixedCaseCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(176); }
};
class ObSysVarValidatePasswordNumberCount : public ObIntSysVar
{
public:
  ObSysVarValidatePasswordNumberCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(177); }
};
class ObSysVarValidatePasswordPolicy : public ObEnumSysVar
{
public:
  const static char * VALIDATE_PASSWORD_POLICY_NAMES[];
public:
  ObSysVarValidatePasswordPolicy() : ObEnumSysVar(VALIDATE_PASSWORD_POLICY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VALIDATE_PASSWORD_POLICY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(178); }
};
class ObSysVarValidatePasswordSpecialCharCount : public ObIntSysVar
{
public:
  ObSysVarValidatePasswordSpecialCharCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(179); }
};
class ObSysVarDefaultPasswordLifetime : public ObIntSysVar
{
public:
  ObSysVarDefaultPasswordLifetime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEFAULT_PASSWORD_LIFETIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(180); }
};
class ObSysVarObOlsPolicySessionLabels : public ObVarcharSysVar
{
public:
  ObSysVarObOlsPolicySessionLabels() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OB_OLS_POLICY_SESSION_LABELS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(181); }
};
class ObSysVarObTraceInfo : public ObVarcharSysVar
{
public:
  ObSysVarObTraceInfo() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_TRACE_INFO; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(182); }
};
class ObSysVarPxPartitionScanThreshold : public ObIntSysVar
{
public:
  ObSysVarPxPartitionScanThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__PX_PARTITION_SCAN_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(183); }
};
class ObSysVarObPxBcastOptimization : public ObEnumSysVar
{
public:
  const static char * _OB_PX_BCAST_OPTIMIZATION_NAMES[];
public:
  ObSysVarObPxBcastOptimization() : ObEnumSysVar(_OB_PX_BCAST_OPTIMIZATION_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OB_PX_BCAST_OPTIMIZATION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(184); }
};
class ObSysVarObPxSlaveMappingThreshold : public ObIntSysVar
{
public:
  ObSysVarObPxSlaveMappingThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(185); }
};
class ObSysVarEnableParallelDml : public ObBoolSysVar
{
public:
  ObSysVarEnableParallelDml() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ENABLE_PARALLEL_DML; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(186); }
};
class ObSysVarPxMinGranulesPerSlave : public ObIntSysVar
{
public:
  ObSysVarPxMinGranulesPerSlave() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__PX_MIN_GRANULES_PER_SLAVE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(187); }
};
class ObSysVarSecureFilePriv : public ObVarcharSysVar
{
public:
  ObSysVarSecureFilePriv() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SECURE_FILE_PRIV; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(188); }
};
class ObSysVarPlsqlWarnings : public ObVarcharSysVar
{
public:
  ObSysVarPlsqlWarnings() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_plsql_warnings, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PLSQL_WARNINGS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(189); }
};
class ObSysVarEnableParallelQuery : public ObBoolSysVar
{
public:
  ObSysVarEnableParallelQuery() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ENABLE_PARALLEL_QUERY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(190); }
};
class ObSysVarForceParallelQueryDop : public ObIntSysVar
{
public:
  ObSysVarForceParallelQueryDop() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__FORCE_PARALLEL_QUERY_DOP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(191); }
};
class ObSysVarForceParallelDmlDop : public ObIntSysVar
{
public:
  ObSysVarForceParallelDmlDop() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__FORCE_PARALLEL_DML_DOP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(192); }
};
class ObSysVarObPlBlockTimeout : public ObIntSysVar
{
public:
  ObSysVarObPlBlockTimeout() : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_PL_BLOCK_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(193); }
};
class ObSysVarTransactionReadOnly : public ObSessionSpecialBoolSysVar
{
public:
  ObSysVarTransactionReadOnly() : ObSessionSpecialBoolSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_read_only, ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope, ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TRANSACTION_READ_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(194); }
};
class ObSysVarResourceManagerPlan : public ObVarcharSysVar
{
public:
  ObSysVarResourceManagerPlan() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_update_resource_manager_plan, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RESOURCE_MANAGER_PLAN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(195); }
};
class ObSysVarPerformanceSchema : public ObBoolSysVar
{
public:
  ObSysVarPerformanceSchema() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(196); }
};
class ObSysVarNlsCurrency : public ObVarcharSysVar
{
public:
  ObSysVarNlsCurrency() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_nls_currency_too_long, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_CURRENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(197); }
};
class ObSysVarNlsIsoCurrency : public ObVarcharSysVar
{
public:
  ObSysVarNlsIsoCurrency() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_nls_iso_currency_is_valid, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_ISO_CURRENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(198); }
};
class ObSysVarNlsDualCurrency : public ObVarcharSysVar
{
public:
  ObSysVarNlsDualCurrency() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_nls_currency_too_long, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NLS_DUAL_CURRENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(199); }
};
class ObSysVarPlsqlCcflags : public ObVarcharSysVar
{
public:
  ObSysVarPlsqlCcflags() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_plsql_ccflags, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PLSQL_CCFLAGS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(200); }
};
class ObSysVarObProxySessionTemporaryTableUsed : public ObBoolSysVar
{
public:
  ObSysVarObProxySessionTemporaryTableUsed() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OB_PROXY_SESSION_TEMPORARY_TABLE_USED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(201); }
};
class ObSysVarEnableParallelDdl : public ObBoolSysVar
{
public:
  ObSysVarEnableParallelDdl() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ENABLE_PARALLEL_DDL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(202); }
};
class ObSysVarForceParallelDdlDop : public ObIntSysVar
{
public:
  ObSysVarForceParallelDdlDop() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__FORCE_PARALLEL_DDL_DOP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(203); }
};
class ObSysVarCursorSharing : public ObEnumSysVar
{
public:
  const static char * CURSOR_SHARING_NAMES[];
public:
  ObSysVarCursorSharing() : ObEnumSysVar(CURSOR_SHARING_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CURSOR_SHARING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(204); }
};
class ObSysVarOptimizerNullAwareAntijoin : public ObBoolSysVar
{
public:
  ObSysVarOptimizerNullAwareAntijoin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OPTIMIZER_NULL_AWARE_ANTIJOIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(205); }
};
class ObSysVarPxPartialRollupPushdown : public ObEnumSysVar
{
public:
  const static char * _PX_PARTIAL_ROLLUP_PUSHDOWN_NAMES[];
public:
  ObSysVarPxPartialRollupPushdown() : ObEnumSysVar(_PX_PARTIAL_ROLLUP_PUSHDOWN_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__PX_PARTIAL_ROLLUP_PUSHDOWN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(206); }
};
class ObSysVarPxDistAggPartialRollupPushdown : public ObEnumSysVar
{
public:
  const static char * _PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN_NAMES[];
public:
  ObSysVarPxDistAggPartialRollupPushdown() : ObEnumSysVar(_PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__PX_DIST_AGG_PARTIAL_ROLLUP_PUSHDOWN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(207); }
};
class ObSysVarCreateAuditPurgeJob : public ObVarcharSysVar
{
public:
  ObSysVarCreateAuditPurgeJob() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__CREATE_AUDIT_PURGE_JOB; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(208); }
};
class ObSysVarDropAuditPurgeJob : public ObVarcharSysVar
{
public:
  ObSysVarDropAuditPurgeJob() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__DROP_AUDIT_PURGE_JOB; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(209); }
};
class ObSysVarSetPurgeJobInterval : public ObVarcharSysVar
{
public:
  ObSysVarSetPurgeJobInterval() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__SET_PURGE_JOB_INTERVAL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(210); }
};
class ObSysVarSetPurgeJobStatus : public ObVarcharSysVar
{
public:
  ObSysVarSetPurgeJobStatus() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__SET_PURGE_JOB_STATUS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(211); }
};
class ObSysVarSetLastArchiveTimestamp : public ObVarcharSysVar
{
public:
  ObSysVarSetLastArchiveTimestamp() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__SET_LAST_ARCHIVE_TIMESTAMP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(212); }
};
class ObSysVarClearLastArchiveTimestamp : public ObVarcharSysVar
{
public:
  ObSysVarClearLastArchiveTimestamp() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__CLEAR_LAST_ARCHIVE_TIMESTAMP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(213); }
};
class ObSysVarAggregationOptimizationSettings : public ObIntSysVar
{
public:
  ObSysVarAggregationOptimizationSettings() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__AGGREGATION_OPTIMIZATION_SETTINGS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(214); }
};
class ObSysVarPxSharedHashJoin : public ObBoolSysVar
{
public:
  ObSysVarPxSharedHashJoin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__PX_SHARED_HASH_JOIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(215); }
};
class ObSysVarSqlNotes : public ObBoolSysVar
{
public:
  ObSysVarSqlNotes() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_NOTES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(216); }
};
class ObSysVarInnodbStrictMode : public ObBoolSysVar
{
public:
  ObSysVarInnodbStrictMode() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STRICT_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(217); }
};
class ObSysVarWindowfuncOptimizationSettings : public ObIntSysVar
{
public:
  ObSysVarWindowfuncOptimizationSettings() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(218); }
};
class ObSysVarObEnableRichErrorMsg : public ObBoolSysVar
{
public:
  ObSysVarObEnableRichErrorMsg() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_RICH_ERROR_MSG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(219); }
};
class ObSysVarLogRowValueOptions : public ObVarcharSysVar
{
public:
  ObSysVarLogRowValueOptions() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_log_row_value_option_is_valid, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_ROW_VALUE_OPTIONS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(220); }
};
class ObSysVarObMaxReadStaleTime : public ObIntSysVar
{
public:
  ObSysVarObMaxReadStaleTime() : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_MAX_READ_STALE_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(221); }
};
class ObSysVarOptimizerGatherStatsOnLoad : public ObBoolSysVar
{
public:
  ObSysVarOptimizerGatherStatsOnLoad() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(222); }
};
class ObSysVarSetReverseDblinkInfos : public ObVarcharSysVar
{
public:
  ObSysVarSetReverseDblinkInfos() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__SET_REVERSE_DBLINK_INFOS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(223); }
};
class ObSysVarForceOrderPreserveSet : public ObBoolSysVar
{
public:
  ObSysVarForceOrderPreserveSet() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__FORCE_ORDER_PRESERVE_SET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(224); }
};
class ObSysVarShowDdlInCompatMode : public ObBoolSysVar
{
public:
  ObSysVarShowDdlInCompatMode() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__SHOW_DDL_IN_COMPAT_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(225); }
};
class ObSysVarParallelDegreePolicy : public ObEnumSysVar
{
public:
  const static char * PARALLEL_DEGREE_POLICY_NAMES[];
public:
  ObSysVarParallelDegreePolicy() : ObEnumSysVar(PARALLEL_DEGREE_POLICY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PARALLEL_DEGREE_POLICY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(226); }
};
class ObSysVarParallelDegreeLimit : public ObIntSysVar
{
public:
  ObSysVarParallelDegreeLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PARALLEL_DEGREE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(227); }
};
class ObSysVarParallelMinScanTimeThreshold : public ObIntSysVar
{
public:
  ObSysVarParallelMinScanTimeThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(228); }
};
class ObSysVarOptimizerDynamicSampling : public ObIntSysVar
{
public:
  ObSysVarOptimizerDynamicSampling() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_DYNAMIC_SAMPLING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(229); }
};
class ObSysVarRuntimeFilterType : public ObVarcharSysVar
{
public:
  ObSysVarRuntimeFilterType() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_runtime_filter_type_is_valid, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RUNTIME_FILTER_TYPE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(230); }
};
class ObSysVarRuntimeFilterWaitTimeMs : public ObIntSysVar
{
public:
  ObSysVarRuntimeFilterWaitTimeMs() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RUNTIME_FILTER_WAIT_TIME_MS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(231); }
};
class ObSysVarRuntimeFilterMaxInNum : public ObIntSysVar
{
public:
  ObSysVarRuntimeFilterMaxInNum() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RUNTIME_FILTER_MAX_IN_NUM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(232); }
};
class ObSysVarRuntimeBloomFilterMaxSize : public ObIntSysVar
{
public:
  ObSysVarRuntimeBloomFilterMaxSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RUNTIME_BLOOM_FILTER_MAX_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(233); }
};
class ObSysVarOptimizerFeaturesEnable : public ObVarcharSysVar
{
public:
  ObSysVarOptimizerFeaturesEnable() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_FEATURES_ENABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(234); }
};
class ObSysVarObProxyWeakreadFeedback : public ObIntSysVar
{
public:
  ObSysVarObProxyWeakreadFeedback() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(235); }
};
class ObSysVarNcharacterSetConnection : public ObCharsetSysVar
{
public:
  ObSysVarNcharacterSetConnection() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NCHARACTER_SET_CONNECTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(236); }
};
class ObSysVarAutomaticSpPrivileges : public ObIntSysVar
{
public:
  ObSysVarAutomaticSpPrivileges() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_AUTOMATIC_SP_PRIVILEGES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(237); }
};
class ObSysVarPrivilegeFeaturesEnable : public ObVarcharSysVar
{
public:
  ObSysVarPrivilegeFeaturesEnable() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PRIVILEGE_FEATURES_ENABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(238); }
};
class ObSysVarPrivControl : public ObVarcharSysVar
{
public:
  ObSysVarPrivControl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__PRIV_CONTROL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(239); }
};
class ObSysVarEnableMysqlPlPrivCheck : public ObBoolSysVar
{
public:
  ObSysVarEnableMysqlPlPrivCheck() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(240); }
};
class ObSysVarObEnablePlCache : public ObBoolSysVar
{
public:
  ObSysVarObEnablePlCache() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_PL_CACHE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(241); }
};
class ObSysVarObDefaultLobInrowThreshold : public ObIntSysVar
{
public:
  ObSysVarObDefaultLobInrowThreshold() : ObIntSysVar(ObSysVarOnCheckFuncs::check_default_lob_inrow_threshold, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_DEFAULT_LOB_INROW_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(242); }
};
class ObSysVarEnableStorageCardinalityEstimation : public ObBoolSysVar
{
public:
  ObSysVarEnableStorageCardinalityEstimation() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ENABLE_STORAGE_CARDINALITY_ESTIMATION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(243); }
};
class ObSysVarLcTimeNames : public ObVarcharSysVar
{
public:
  ObSysVarLcTimeNames() : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_locale_type_is_valid, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LC_TIME_NAMES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(244); }
};
class ObSysVarActivateAllRolesOnLogin : public ObBoolSysVar
{
public:
  ObSysVarActivateAllRolesOnLogin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_ACTIVATE_ALL_ROLES_ON_LOGIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(245); }
};
class ObSysVarEnableRichVectorFormat : public ObBoolSysVar
{
public:
  ObSysVarEnableRichVectorFormat() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ENABLE_RICH_VECTOR_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(246); }
};
class ObSysVarInnodbStatsPersistent : public ObEnumSysVar
{
public:
  const static char * INNODB_STATS_PERSISTENT_NAMES[];
public:
  ObSysVarInnodbStatsPersistent() : ObEnumSysVar(INNODB_STATS_PERSISTENT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_PERSISTENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(247); }
};
class ObSysVarDebug : public ObVarcharSysVar
{
public:
  ObSysVarDebug() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(248); }
};
class ObSysVarInnodbChangeBufferingDebug : public ObIntSysVar
{
public:
  ObSysVarInnodbChangeBufferingDebug() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_CHANGE_BUFFERING_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(249); }
};
class ObSysVarInnodbCompressDebug : public ObEnumSysVar
{
public:
  const static char * INNODB_COMPRESS_DEBUG_NAMES[];
public:
  ObSysVarInnodbCompressDebug() : ObEnumSysVar(INNODB_COMPRESS_DEBUG_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_COMPRESS_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(250); }
};
class ObSysVarInnodbDisableResizeBufferPoolDebug : public ObBoolSysVar
{
public:
  ObSysVarInnodbDisableResizeBufferPoolDebug() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_DISABLE_RESIZE_BUFFER_POOL_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(251); }
};
class ObSysVarInnodbFilMakePageDirtyDebug : public ObIntSysVar
{
public:
  ObSysVarInnodbFilMakePageDirtyDebug() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FIL_MAKE_PAGE_DIRTY_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(252); }
};
class ObSysVarInnodbLimitOptimisticInsertDebug : public ObIntSysVar
{
public:
  ObSysVarInnodbLimitOptimisticInsertDebug() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LIMIT_OPTIMISTIC_INSERT_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(253); }
};
class ObSysVarInnodbMergeThresholdSetAllDebug : public ObIntSysVar
{
public:
  ObSysVarInnodbMergeThresholdSetAllDebug() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MERGE_THRESHOLD_SET_ALL_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(254); }
};
class ObSysVarInnodbSavedPageNumberDebug : public ObIntSysVar
{
public:
  ObSysVarInnodbSavedPageNumberDebug() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_SAVED_PAGE_NUMBER_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(255); }
};
class ObSysVarInnodbTrxPurgeViewUpdateOnlyDebug : public ObBoolSysVar
{
public:
  ObSysVarInnodbTrxPurgeViewUpdateOnlyDebug() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_TRX_PURGE_VIEW_UPDATE_ONLY_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(256); }
};
class ObSysVarInnodbTrxRsegNSlotsDebug : public ObIntSysVar
{
public:
  ObSysVarInnodbTrxRsegNSlotsDebug() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_TRX_RSEG_N_SLOTS_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(257); }
};
class ObSysVarStoredProgramCache : public ObIntSysVar
{
public:
  ObSysVarStoredProgramCache() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_STORED_PROGRAM_CACHE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(258); }
};
class ObSysVarObCompatibilityControl : public ObEnumSysVar
{
public:
  const static char * OB_COMPATIBILITY_CONTROL_NAMES[];
public:
  ObSysVarObCompatibilityControl() : ObEnumSysVar(OB_COMPATIBILITY_CONTROL_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_COMPATIBILITY_CONTROL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(259); }
};
class ObSysVarObCompatibilityVersion : public ObVersionSysVar
{
public:
  ObSysVarObCompatibilityVersion() : ObVersionSysVar(ObSysVarOnCheckFuncs::check_and_convert_compat_version, NULL, ObSysVarToObjFuncs::to_obj_version, ObSysVarToStrFuncs::to_str_version, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_COMPATIBILITY_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(260); }
};
class ObSysVarObSecurityVersion : public ObVersionSysVar
{
public:
  ObSysVarObSecurityVersion() : ObVersionSysVar(ObSysVarOnCheckFuncs::check_and_convert_security_version, NULL, ObSysVarToObjFuncs::to_obj_version, ObSysVarToStrFuncs::to_str_version, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_SECURITY_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(261); }
};
class ObSysVarCardinalityEstimationModel : public ObEnumSysVar
{
public:
  const static char * CARDINALITY_ESTIMATION_MODEL_NAMES[];
public:
  ObSysVarCardinalityEstimationModel() : ObEnumSysVar(CARDINALITY_ESTIMATION_MODEL_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CARDINALITY_ESTIMATION_MODEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(262); }
};
class ObSysVarQueryRewriteEnabled : public ObEnumSysVar
{
public:
  const static char * QUERY_REWRITE_ENABLED_NAMES[];
public:
  ObSysVarQueryRewriteEnabled() : ObEnumSysVar(QUERY_REWRITE_ENABLED_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_REWRITE_ENABLED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(263); }
};
class ObSysVarQueryRewriteIntegrity : public ObEnumSysVar
{
public:
  const static char * QUERY_REWRITE_INTEGRITY_NAMES[];
public:
  ObSysVarQueryRewriteIntegrity() : ObEnumSysVar(QUERY_REWRITE_INTEGRITY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_REWRITE_INTEGRITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(264); }
};
class ObSysVarFlush : public ObEnumSysVar
{
public:
  const static char * FLUSH_NAMES[];
public:
  ObSysVarFlush() : ObEnumSysVar(FLUSH_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_FLUSH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(265); }
};
class ObSysVarFlushTime : public ObIntSysVar
{
public:
  ObSysVarFlushTime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_FLUSH_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(266); }
};
class ObSysVarInnodbAdaptiveFlushing : public ObEnumSysVar
{
public:
  const static char * INNODB_ADAPTIVE_FLUSHING_NAMES[];
public:
  ObSysVarInnodbAdaptiveFlushing() : ObEnumSysVar(INNODB_ADAPTIVE_FLUSHING_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ADAPTIVE_FLUSHING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(267); }
};
class ObSysVarInnodbAdaptiveFlushingLwm : public ObIntSysVar
{
public:
  ObSysVarInnodbAdaptiveFlushingLwm() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ADAPTIVE_FLUSHING_LWM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(268); }
};
class ObSysVarInnodbAdaptiveHashIndex : public ObEnumSysVar
{
public:
  const static char * INNODB_ADAPTIVE_HASH_INDEX_NAMES[];
public:
  ObSysVarInnodbAdaptiveHashIndex() : ObEnumSysVar(INNODB_ADAPTIVE_HASH_INDEX_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(269); }
};
class ObSysVarInnodbAdaptiveHashIndexParts : public ObIntSysVar
{
public:
  ObSysVarInnodbAdaptiveHashIndexParts() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ADAPTIVE_HASH_INDEX_PARTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(270); }
};
class ObSysVarInnodbAdaptiveMaxSleepDelay : public ObIntSysVar
{
public:
  ObSysVarInnodbAdaptiveMaxSleepDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ADAPTIVE_MAX_SLEEP_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(271); }
};
class ObSysVarInnodbAutoextendIncrement : public ObIntSysVar
{
public:
  ObSysVarInnodbAutoextendIncrement() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_AUTOEXTEND_INCREMENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(272); }
};
class ObSysVarInnodbBackgroundDropListEmpty : public ObEnumSysVar
{
public:
  const static char * INNODB_BACKGROUND_DROP_LIST_EMPTY_NAMES[];
public:
  ObSysVarInnodbBackgroundDropListEmpty() : ObEnumSysVar(INNODB_BACKGROUND_DROP_LIST_EMPTY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BACKGROUND_DROP_LIST_EMPTY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(273); }
};
class ObSysVarInnodbBufferPoolDumpAtShutdown : public ObEnumSysVar
{
public:
  const static char * INNODB_BUFFER_POOL_DUMP_AT_SHUTDOWN_NAMES[];
public:
  ObSysVarInnodbBufferPoolDumpAtShutdown() : ObEnumSysVar(INNODB_BUFFER_POOL_DUMP_AT_SHUTDOWN_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_DUMP_AT_SHUTDOWN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(274); }
};
class ObSysVarInnodbBufferPoolDumpNow : public ObEnumSysVar
{
public:
  const static char * INNODB_BUFFER_POOL_DUMP_NOW_NAMES[];
public:
  ObSysVarInnodbBufferPoolDumpNow() : ObEnumSysVar(INNODB_BUFFER_POOL_DUMP_NOW_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_DUMP_NOW; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(275); }
};
class ObSysVarInnodbBufferPoolDumpPct : public ObIntSysVar
{
public:
  ObSysVarInnodbBufferPoolDumpPct() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_DUMP_PCT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(276); }
};
class ObSysVarInnodbBufferPoolFilename : public ObVarcharSysVar
{
public:
  ObSysVarInnodbBufferPoolFilename() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_FILENAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(277); }
};
class ObSysVarInnodbBufferPoolLoadAbort : public ObEnumSysVar
{
public:
  const static char * INNODB_BUFFER_POOL_LOAD_ABORT_NAMES[];
public:
  ObSysVarInnodbBufferPoolLoadAbort() : ObEnumSysVar(INNODB_BUFFER_POOL_LOAD_ABORT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_LOAD_ABORT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(278); }
};
class ObSysVarInnodbBufferPoolLoadNow : public ObEnumSysVar
{
public:
  const static char * INNODB_BUFFER_POOL_LOAD_NOW_NAMES[];
public:
  ObSysVarInnodbBufferPoolLoadNow() : ObEnumSysVar(INNODB_BUFFER_POOL_LOAD_NOW_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_LOAD_NOW; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(279); }
};
class ObSysVarInnodbBufferPoolSize : public ObIntSysVar
{
public:
  ObSysVarInnodbBufferPoolSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(280); }
};
class ObSysVarInnodbChangeBufferMaxSize : public ObIntSysVar
{
public:
  ObSysVarInnodbChangeBufferMaxSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_CHANGE_BUFFER_MAX_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(281); }
};
class ObSysVarInnodbChangeBuffering : public ObEnumSysVar
{
public:
  const static char * INNODB_CHANGE_BUFFERING_NAMES[];
public:
  ObSysVarInnodbChangeBuffering() : ObEnumSysVar(INNODB_CHANGE_BUFFERING_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_CHANGE_BUFFERING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(282); }
};
class ObSysVarInnodbChecksumAlgorithm : public ObEnumSysVar
{
public:
  const static char * INNODB_CHECKSUM_ALGORITHM_NAMES[];
public:
  ObSysVarInnodbChecksumAlgorithm() : ObEnumSysVar(INNODB_CHECKSUM_ALGORITHM_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_CHECKSUM_ALGORITHM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(283); }
};
class ObSysVarInnodbCmpPerIndexEnabled : public ObEnumSysVar
{
public:
  const static char * INNODB_CMP_PER_INDEX_ENABLED_NAMES[];
public:
  ObSysVarInnodbCmpPerIndexEnabled() : ObEnumSysVar(INNODB_CMP_PER_INDEX_ENABLED_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_CMP_PER_INDEX_ENABLED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(284); }
};
class ObSysVarInnodbCommitConcurrency : public ObIntSysVar
{
public:
  ObSysVarInnodbCommitConcurrency() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_COMMIT_CONCURRENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(285); }
};
class ObSysVarInnodbCompressionFailureThresholdPct : public ObIntSysVar
{
public:
  ObSysVarInnodbCompressionFailureThresholdPct() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_COMPRESSION_FAILURE_THRESHOLD_PCT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(286); }
};
class ObSysVarInnodbCompressionLevel : public ObIntSysVar
{
public:
  ObSysVarInnodbCompressionLevel() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_COMPRESSION_LEVEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(287); }
};
class ObSysVarInnodbCompressionPadPctMax : public ObIntSysVar
{
public:
  ObSysVarInnodbCompressionPadPctMax() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_COMPRESSION_PAD_PCT_MAX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(288); }
};
class ObSysVarInnodbConcurrencyTickets : public ObIntSysVar
{
public:
  ObSysVarInnodbConcurrencyTickets() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_CONCURRENCY_TICKETS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(289); }
};
class ObSysVarInnodbDefaultRowFormat : public ObEnumSysVar
{
public:
  const static char * INNODB_DEFAULT_ROW_FORMAT_NAMES[];
public:
  ObSysVarInnodbDefaultRowFormat() : ObEnumSysVar(INNODB_DEFAULT_ROW_FORMAT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_DEFAULT_ROW_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(290); }
};
class ObSysVarInnodbDisableSortFileCache : public ObEnumSysVar
{
public:
  const static char * INNODB_DISABLE_SORT_FILE_CACHE_NAMES[];
public:
  ObSysVarInnodbDisableSortFileCache() : ObEnumSysVar(INNODB_DISABLE_SORT_FILE_CACHE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_DISABLE_SORT_FILE_CACHE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(291); }
};
class ObSysVarInnodbFileFormat : public ObEnumSysVar
{
public:
  const static char * INNODB_FILE_FORMAT_NAMES[];
public:
  ObSysVarInnodbFileFormat() : ObEnumSysVar(INNODB_FILE_FORMAT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FILE_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(292); }
};
class ObSysVarInnodbFileFormatMax : public ObEnumSysVar
{
public:
  const static char * INNODB_FILE_FORMAT_MAX_NAMES[];
public:
  ObSysVarInnodbFileFormatMax() : ObEnumSysVar(INNODB_FILE_FORMAT_MAX_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FILE_FORMAT_MAX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(293); }
};
class ObSysVarInnodbFilePerTable : public ObEnumSysVar
{
public:
  const static char * INNODB_FILE_PER_TABLE_NAMES[];
public:
  ObSysVarInnodbFilePerTable() : ObEnumSysVar(INNODB_FILE_PER_TABLE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FILE_PER_TABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(294); }
};
class ObSysVarInnodbFillFactor : public ObIntSysVar
{
public:
  ObSysVarInnodbFillFactor() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FILL_FACTOR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(295); }
};
class ObSysVarInnodbFlushNeighbors : public ObEnumSysVar
{
public:
  const static char * INNODB_FLUSH_NEIGHBORS_NAMES[];
public:
  ObSysVarInnodbFlushNeighbors() : ObEnumSysVar(INNODB_FLUSH_NEIGHBORS_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FLUSH_NEIGHBORS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(296); }
};
class ObSysVarInnodbFlushSync : public ObEnumSysVar
{
public:
  const static char * INNODB_FLUSH_SYNC_NAMES[];
public:
  ObSysVarInnodbFlushSync() : ObEnumSysVar(INNODB_FLUSH_SYNC_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FLUSH_SYNC; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(297); }
};
class ObSysVarInnodbFlushingAvgLoops : public ObIntSysVar
{
public:
  ObSysVarInnodbFlushingAvgLoops() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FLUSHING_AVG_LOOPS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(298); }
};
class ObSysVarInnodbLruScanDepth : public ObIntSysVar
{
public:
  ObSysVarInnodbLruScanDepth() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LRU_SCAN_DEPTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(299); }
};
class ObSysVarInnodbMaxDirtyPagesPct : public ObNumericSysVar
{
public:
  ObSysVarInnodbMaxDirtyPagesPct() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(300); }
};
class ObSysVarInnodbMaxDirtyPagesPctLwm : public ObNumericSysVar
{
public:
  ObSysVarInnodbMaxDirtyPagesPctLwm() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MAX_DIRTY_PAGES_PCT_LWM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(301); }
};
class ObSysVarInnodbMaxPurgeLag : public ObIntSysVar
{
public:
  ObSysVarInnodbMaxPurgeLag() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MAX_PURGE_LAG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(302); }
};
class ObSysVarInnodbMaxPurgeLagDelay : public ObIntSysVar
{
public:
  ObSysVarInnodbMaxPurgeLagDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MAX_PURGE_LAG_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(303); }
};
class ObSysVarHaveSymlink : public ObEnumSysVar
{
public:
  const static char * HAVE_SYMLINK_NAMES[];
public:
  ObSysVarHaveSymlink() : ObEnumSysVar(HAVE_SYMLINK_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_SYMLINK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(304); }
};
class ObSysVarIgnoreBuiltinInnodb : public ObEnumSysVar
{
public:
  const static char * IGNORE_BUILTIN_INNODB_NAMES[];
public:
  ObSysVarIgnoreBuiltinInnodb() : ObEnumSysVar(IGNORE_BUILTIN_INNODB_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_IGNORE_BUILTIN_INNODB; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(305); }
};
class ObSysVarInnodbBufferPoolChunkSize : public ObIntSysVar
{
public:
  ObSysVarInnodbBufferPoolChunkSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_CHUNK_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(306); }
};
class ObSysVarInnodbBufferPoolInstances : public ObIntSysVar
{
public:
  ObSysVarInnodbBufferPoolInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(307); }
};
class ObSysVarInnodbBufferPoolLoadAtStartup : public ObEnumSysVar
{
public:
  const static char * INNODB_BUFFER_POOL_LOAD_AT_STARTUP_NAMES[];
public:
  ObSysVarInnodbBufferPoolLoadAtStartup() : ObEnumSysVar(INNODB_BUFFER_POOL_LOAD_AT_STARTUP_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_BUFFER_POOL_LOAD_AT_STARTUP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(308); }
};
class ObSysVarInnodbChecksums : public ObEnumSysVar
{
public:
  const static char * INNODB_CHECKSUMS_NAMES[];
public:
  ObSysVarInnodbChecksums() : ObEnumSysVar(INNODB_CHECKSUMS_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_CHECKSUMS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(309); }
};
class ObSysVarInnodbDoublewrite : public ObEnumSysVar
{
public:
  const static char * INNODB_DOUBLEWRITE_NAMES[];
public:
  ObSysVarInnodbDoublewrite() : ObEnumSysVar(INNODB_DOUBLEWRITE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_DOUBLEWRITE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(310); }
};
class ObSysVarInnodbFileFormatCheck : public ObEnumSysVar
{
public:
  const static char * INNODB_FILE_FORMAT_CHECK_NAMES[];
public:
  ObSysVarInnodbFileFormatCheck() : ObEnumSysVar(INNODB_FILE_FORMAT_CHECK_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FILE_FORMAT_CHECK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(311); }
};
class ObSysVarInnodbFlushMethod : public ObEnumSysVar
{
public:
  const static char * INNODB_FLUSH_METHOD_NAMES[];
public:
  ObSysVarInnodbFlushMethod() : ObEnumSysVar(INNODB_FLUSH_METHOD_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FLUSH_METHOD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(312); }
};
class ObSysVarInnodbForceLoadCorrupted : public ObEnumSysVar
{
public:
  const static char * INNODB_FORCE_LOAD_CORRUPTED_NAMES[];
public:
  ObSysVarInnodbForceLoadCorrupted() : ObEnumSysVar(INNODB_FORCE_LOAD_CORRUPTED_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FORCE_LOAD_CORRUPTED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(313); }
};
class ObSysVarInnodbPageSize : public ObEnumSysVar
{
public:
  const static char * INNODB_PAGE_SIZE_NAMES[];
public:
  ObSysVarInnodbPageSize() : ObEnumSysVar(INNODB_PAGE_SIZE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_PAGE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(314); }
};
class ObSysVarInnodbVersion : public ObEnumSysVar
{
public:
  const static char * INNODB_VERSION_NAMES[];
public:
  ObSysVarInnodbVersion() : ObEnumSysVar(INNODB_VERSION_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(315); }
};
class ObSysVarMyisamMmapSize : public ObIntSysVar
{
public:
  ObSysVarMyisamMmapSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYISAM_MMAP_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(316); }
};
class ObSysVarTableOpenCacheInstances : public ObIntSysVar
{
public:
  ObSysVarTableOpenCacheInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TABLE_OPEN_CACHE_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(317); }
};
class ObSysVarGtidExecuted : public ObVarcharSysVar
{
public:
  ObSysVarGtidExecuted() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GTID_EXECUTED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(318); }
};
class ObSysVarGtidOwned : public ObVarcharSysVar
{
public:
  ObSysVarGtidOwned() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GTID_OWNED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(319); }
};
class ObSysVarInnodbRollbackOnTimeout : public ObBoolSysVar
{
public:
  ObSysVarInnodbRollbackOnTimeout() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ROLLBACK_ON_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(320); }
};
class ObSysVarCompletionType : public ObEnumSysVar
{
public:
  const static char * COMPLETION_TYPE_NAMES[];
public:
  ObSysVarCompletionType() : ObEnumSysVar(COMPLETION_TYPE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_COMPLETION_TYPE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(321); }
};
class ObSysVarEnforceGtidConsistency : public ObEnumSysVar
{
public:
  const static char * ENFORCE_GTID_CONSISTENCY_NAMES[];
public:
  ObSysVarEnforceGtidConsistency() : ObEnumSysVar(ENFORCE_GTID_CONSISTENCY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_ENFORCE_GTID_CONSISTENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(322); }
};
class ObSysVarGtidExecutedCompressionPeriod : public ObIntSysVar
{
public:
  ObSysVarGtidExecutedCompressionPeriod() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GTID_EXECUTED_COMPRESSION_PERIOD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(323); }
};
class ObSysVarGtidMode : public ObEnumSysVar
{
public:
  const static char * GTID_MODE_NAMES[];
public:
  ObSysVarGtidMode() : ObEnumSysVar(GTID_MODE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GTID_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(324); }
};
class ObSysVarGtidNext : public ObEnumSysVar
{
public:
  const static char * GTID_NEXT_NAMES[];
public:
  ObSysVarGtidNext() : ObEnumSysVar(GTID_NEXT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GTID_NEXT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(325); }
};
class ObSysVarGtidPurged : public ObVarcharSysVar
{
public:
  ObSysVarGtidPurged() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GTID_PURGED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(326); }
};
class ObSysVarInnodbApiBkCommitInterval : public ObIntSysVar
{
public:
  ObSysVarInnodbApiBkCommitInterval() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_API_BK_COMMIT_INTERVAL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(327); }
};
class ObSysVarInnodbApiTrxLevel : public ObIntSysVar
{
public:
  ObSysVarInnodbApiTrxLevel() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_API_TRX_LEVEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(328); }
};
class ObSysVarInnodbSupportXa : public ObBoolSysVar
{
public:
  ObSysVarInnodbSupportXa() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_SUPPORT_XA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(329); }
};
class ObSysVarSessionTrackGtids : public ObEnumSysVar
{
public:
  const static char * SESSION_TRACK_GTIDS_NAMES[];
public:
  ObSysVarSessionTrackGtids() : ObEnumSysVar(SESSION_TRACK_GTIDS_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SESSION_TRACK_GTIDS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(330); }
};
class ObSysVarSessionTrackTransactionInfo : public ObEnumSysVar
{
public:
  const static char * SESSION_TRACK_TRANSACTION_INFO_NAMES[];
public:
  ObSysVarSessionTrackTransactionInfo() : ObEnumSysVar(SESSION_TRACK_TRANSACTION_INFO_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SESSION_TRACK_TRANSACTION_INFO; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(331); }
};
class ObSysVarTransactionAllocBlockSize : public ObIntSysVar
{
public:
  ObSysVarTransactionAllocBlockSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TRANSACTION_ALLOC_BLOCK_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(332); }
};
class ObSysVarTransactionAllowBatching : public ObBoolSysVar
{
public:
  ObSysVarTransactionAllowBatching() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TRANSACTION_ALLOW_BATCHING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(333); }
};
class ObSysVarTransactionPreallocSize : public ObIntSysVar
{
public:
  ObSysVarTransactionPreallocSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TRANSACTION_PREALLOC_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(334); }
};
class ObSysVarTransactionWriteSetExtraction : public ObEnumSysVar
{
public:
  const static char * TRANSACTION_WRITE_SET_EXTRACTION_NAMES[];
public:
  ObSysVarTransactionWriteSetExtraction() : ObEnumSysVar(TRANSACTION_WRITE_SET_EXTRACTION_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TRANSACTION_WRITE_SET_EXTRACTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(335); }
};
class ObSysVarInformationSchemaStatsExpiry : public ObIntSysVar
{
public:
  ObSysVarInformationSchemaStatsExpiry() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INFORMATION_SCHEMA_STATS_EXPIRY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(336); }
};
class ObSysVarOracleSqlSelectLimit : public ObIntSysVar
{
public:
  ObSysVarOracleSqlSelectLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ORACLE_SQL_SELECT_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(337); }
};
class ObSysVarGroupReplicationAllowLocalDisjointGtidsJoin : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationAllowLocalDisjointGtidsJoin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_DISJOINT_GTIDS_JOIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(338); }
};
class ObSysVarGroupReplicationAllowLocalLowerVersionJoin : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationAllowLocalLowerVersionJoin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_ALLOW_LOCAL_LOWER_VERSION_JOIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(339); }
};
class ObSysVarGroupReplicationAutoIncrementIncrement : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationAutoIncrementIncrement() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_AUTO_INCREMENT_INCREMENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(340); }
};
class ObSysVarGroupReplicationBootstrapGroup : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationBootstrapGroup() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_BOOTSTRAP_GROUP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(341); }
};
class ObSysVarGroupReplicationComponentsStopTimeout : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationComponentsStopTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_COMPONENTS_STOP_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(342); }
};
class ObSysVarGroupReplicationCompressionThreshold : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationCompressionThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_COMPRESSION_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(343); }
};
class ObSysVarGroupReplicationEnforceUpdateEverywhereChecks : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationEnforceUpdateEverywhereChecks() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_ENFORCE_UPDATE_EVERYWHERE_CHECKS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(344); }
};
class ObSysVarGroupReplicationExitStateAction : public ObEnumSysVar
{
public:
  const static char * GROUP_REPLICATION_EXIT_STATE_ACTION_NAMES[];
public:
  ObSysVarGroupReplicationExitStateAction() : ObEnumSysVar(GROUP_REPLICATION_EXIT_STATE_ACTION_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_EXIT_STATE_ACTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(345); }
};
class ObSysVarGroupReplicationFlowControlApplierThreshold : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationFlowControlApplierThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_APPLIER_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(346); }
};
class ObSysVarGroupReplicationFlowControlCertifierThreshold : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationFlowControlCertifierThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_CERTIFIER_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(347); }
};
class ObSysVarGroupReplicationFlowControlMode : public ObEnumSysVar
{
public:
  const static char * GROUP_REPLICATION_FLOW_CONTROL_MODE_NAMES[];
public:
  ObSysVarGroupReplicationFlowControlMode() : ObEnumSysVar(GROUP_REPLICATION_FLOW_CONTROL_MODE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_FLOW_CONTROL_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(348); }
};
class ObSysVarGroupReplicationForceMembers : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationForceMembers() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_FORCE_MEMBERS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(349); }
};
class ObSysVarGroupReplicationGroupName : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationGroupName() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_GROUP_NAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(350); }
};
class ObSysVarGroupReplicationGtidAssignmentBlockSize : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationGtidAssignmentBlockSize() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_GTID_ASSIGNMENT_BLOCK_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(351); }
};
class ObSysVarGroupReplicationIpWhitelist : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationIpWhitelist() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_IP_WHITELIST; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(352); }
};
class ObSysVarGroupReplicationLocalAddress : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationLocalAddress() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_LOCAL_ADDRESS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(353); }
};
class ObSysVarGroupReplicationMemberWeight : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationMemberWeight() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_MEMBER_WEIGHT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(354); }
};
class ObSysVarGroupReplicationPollSpinLoops : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationPollSpinLoops() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_POLL_SPIN_LOOPS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(355); }
};
class ObSysVarGroupReplicationRecoveryCompleteAt : public ObEnumSysVar
{
public:
  const static char * GROUP_REPLICATION_RECOVERY_COMPLETE_AT_NAMES[];
public:
  ObSysVarGroupReplicationRecoveryCompleteAt() : ObEnumSysVar(GROUP_REPLICATION_RECOVERY_COMPLETE_AT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_COMPLETE_AT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(356); }
};
class ObSysVarGroupReplicationRecoveryReconnectInterval : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationRecoveryReconnectInterval() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_RECONNECT_INTERVAL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(357); }
};
class ObSysVarGroupReplicationRecoveryRetryCount : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationRecoveryRetryCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_RETRY_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(358); }
};
class ObSysVarGroupReplicationRecoverySslCa : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslCa() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(359); }
};
class ObSysVarGroupReplicationRecoverySslCapath : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslCapath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CAPATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(360); }
};
class ObSysVarGroupReplicationRecoverySslCert : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslCert() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CERT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(361); }
};
class ObSysVarGroupReplicationRecoverySslCipher : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslCipher() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CIPHER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(362); }
};
class ObSysVarGroupReplicationRecoverySslCrl : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslCrl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(363); }
};
class ObSysVarGroupReplicationRecoverySslCrlpath : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslCrlpath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_CRLPATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(364); }
};
class ObSysVarGroupReplicationRecoverySslKey : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslKey() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_KEY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(365); }
};
class ObSysVarGroupReplicationRecoverySslVerifyServerCert : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationRecoverySslVerifyServerCert() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_SSL_VERIFY_SERVER_CERT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(366); }
};
class ObSysVarGroupReplicationRecoveryUseSsl : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationRecoveryUseSsl() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_RECOVERY_USE_SSL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(367); }
};
class ObSysVarGroupReplicationSinglePrimaryMode : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationSinglePrimaryMode() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_SINGLE_PRIMARY_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(368); }
};
class ObSysVarGroupReplicationSslMode : public ObEnumSysVar
{
public:
  const static char * GROUP_REPLICATION_SSL_MODE_NAMES[];
public:
  ObSysVarGroupReplicationSslMode() : ObEnumSysVar(GROUP_REPLICATION_SSL_MODE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_SSL_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(369); }
};
class ObSysVarGroupReplicationStartOnBoot : public ObBoolSysVar
{
public:
  ObSysVarGroupReplicationStartOnBoot() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_START_ON_BOOT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(370); }
};
class ObSysVarGroupReplicationTransactionSizeLimit : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationTransactionSizeLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_TRANSACTION_SIZE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(371); }
};
class ObSysVarGroupReplicationUnreachableMajorityTimeout : public ObIntSysVar
{
public:
  ObSysVarGroupReplicationUnreachableMajorityTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_UNREACHABLE_MAJORITY_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(372); }
};
class ObSysVarInnodbReplicationDelay : public ObIntSysVar
{
public:
  ObSysVarInnodbReplicationDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_REPLICATION_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(373); }
};
class ObSysVarMasterInfoRepository : public ObVarcharSysVar
{
public:
  ObSysVarMasterInfoRepository() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MASTER_INFO_REPOSITORY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(374); }
};
class ObSysVarMasterVerifyChecksum : public ObBoolSysVar
{
public:
  ObSysVarMasterVerifyChecksum() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MASTER_VERIFY_CHECKSUM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(375); }
};
class ObSysVarPseudoSlaveMode : public ObBoolSysVar
{
public:
  ObSysVarPseudoSlaveMode() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PSEUDO_SLAVE_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(376); }
};
class ObSysVarPseudoThreadId : public ObIntSysVar
{
public:
  ObSysVarPseudoThreadId() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PSEUDO_THREAD_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(377); }
};
class ObSysVarRbrExecMode : public ObEnumSysVar
{
public:
  const static char * RBR_EXEC_MODE_NAMES[];
public:
  ObSysVarRbrExecMode() : ObEnumSysVar(RBR_EXEC_MODE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RBR_EXEC_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(378); }
};
class ObSysVarReplicationOptimizeForStaticPluginConfig : public ObBoolSysVar
{
public:
  ObSysVarReplicationOptimizeForStaticPluginConfig() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REPLICATION_OPTIMIZE_FOR_STATIC_PLUGIN_CONFIG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(379); }
};
class ObSysVarReplicationSenderObserveCommitOnly : public ObBoolSysVar
{
public:
  ObSysVarReplicationSenderObserveCommitOnly() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REPLICATION_SENDER_OBSERVE_COMMIT_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(380); }
};
class ObSysVarRplSemiSyncMasterEnabled : public ObBoolSysVar
{
public:
  ObSysVarRplSemiSyncMasterEnabled() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_MASTER_ENABLED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(381); }
};
class ObSysVarRplSemiSyncMasterTimeout : public ObIntSysVar
{
public:
  ObSysVarRplSemiSyncMasterTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_MASTER_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(382); }
};
class ObSysVarRplSemiSyncMasterTraceLevel : public ObIntSysVar
{
public:
  ObSysVarRplSemiSyncMasterTraceLevel() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_MASTER_TRACE_LEVEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(383); }
};
class ObSysVarRplSemiSyncMasterWaitForSlaveCount : public ObIntSysVar
{
public:
  ObSysVarRplSemiSyncMasterWaitForSlaveCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_FOR_SLAVE_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(384); }
};
class ObSysVarRplSemiSyncMasterWaitNoSlave : public ObBoolSysVar
{
public:
  ObSysVarRplSemiSyncMasterWaitNoSlave() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_NO_SLAVE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(385); }
};
class ObSysVarRplSemiSyncMasterWaitPoint : public ObEnumSysVar
{
public:
  const static char * RPL_SEMI_SYNC_MASTER_WAIT_POINT_NAMES[];
public:
  ObSysVarRplSemiSyncMasterWaitPoint() : ObEnumSysVar(RPL_SEMI_SYNC_MASTER_WAIT_POINT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_MASTER_WAIT_POINT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(386); }
};
class ObSysVarRplSemiSyncSlaveEnabled : public ObBoolSysVar
{
public:
  ObSysVarRplSemiSyncSlaveEnabled() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_SLAVE_ENABLED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(387); }
};
class ObSysVarRplSemiSyncSlaveTraceLevel : public ObIntSysVar
{
public:
  ObSysVarRplSemiSyncSlaveTraceLevel() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_SEMI_SYNC_SLAVE_TRACE_LEVEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(388); }
};
class ObSysVarRplStopSlaveTimeout : public ObIntSysVar
{
public:
  ObSysVarRplStopSlaveTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RPL_STOP_SLAVE_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(389); }
};
class ObSysVarSlaveAllowBatching : public ObBoolSysVar
{
public:
  ObSysVarSlaveAllowBatching() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_ALLOW_BATCHING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(390); }
};
class ObSysVarSlaveCheckpointGroup : public ObIntSysVar
{
public:
  ObSysVarSlaveCheckpointGroup() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_CHECKPOINT_GROUP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(391); }
};
class ObSysVarSlaveCheckpointPeriod : public ObIntSysVar
{
public:
  ObSysVarSlaveCheckpointPeriod() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_CHECKPOINT_PERIOD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(392); }
};
class ObSysVarSlaveCompressedProtocol : public ObBoolSysVar
{
public:
  ObSysVarSlaveCompressedProtocol() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_COMPRESSED_PROTOCOL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(393); }
};
class ObSysVarSlaveExecMode : public ObEnumSysVar
{
public:
  const static char * SLAVE_EXEC_MODE_NAMES[];
public:
  ObSysVarSlaveExecMode() : ObEnumSysVar(SLAVE_EXEC_MODE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_EXEC_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(394); }
};
class ObSysVarSlaveMaxAllowedPacket : public ObIntSysVar
{
public:
  ObSysVarSlaveMaxAllowedPacket() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_MAX_ALLOWED_PACKET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(395); }
};
class ObSysVarSlaveNetTimeout : public ObIntSysVar
{
public:
  ObSysVarSlaveNetTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_NET_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(396); }
};
class ObSysVarSlaveParallelType : public ObEnumSysVar
{
public:
  const static char * SLAVE_PARALLEL_TYPE_NAMES[];
public:
  ObSysVarSlaveParallelType() : ObEnumSysVar(SLAVE_PARALLEL_TYPE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_PARALLEL_TYPE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(397); }
};
class ObSysVarSlaveParallelWorkers : public ObIntSysVar
{
public:
  ObSysVarSlaveParallelWorkers() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_PARALLEL_WORKERS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(398); }
};
class ObSysVarSlavePendingJobsSizeMax : public ObIntSysVar
{
public:
  ObSysVarSlavePendingJobsSizeMax() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_PENDING_JOBS_SIZE_MAX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(399); }
};
class ObSysVarSlavePreserveCommitOrder : public ObBoolSysVar
{
public:
  ObSysVarSlavePreserveCommitOrder() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_PRESERVE_COMMIT_ORDER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(400); }
};
class ObSysVarSlaveSqlVerifyChecksum : public ObBoolSysVar
{
public:
  ObSysVarSlaveSqlVerifyChecksum() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_SQL_VERIFY_CHECKSUM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(401); }
};
class ObSysVarSlaveTransactionRetries : public ObIntSysVar
{
public:
  ObSysVarSlaveTransactionRetries() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_TRANSACTION_RETRIES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(402); }
};
class ObSysVarSqlSlaveSkipCounter : public ObIntSysVar
{
public:
  ObSysVarSqlSlaveSkipCounter() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_SLAVE_SKIP_COUNTER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(403); }
};
class ObSysVarInnodbForceRecovery : public ObIntSysVar
{
public:
  ObSysVarInnodbForceRecovery() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FORCE_RECOVERY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(404); }
};
class ObSysVarSkipSlaveStart : public ObBoolSysVar
{
public:
  ObSysVarSkipSlaveStart() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SKIP_SLAVE_START; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(405); }
};
class ObSysVarSlaveLoadTmpdir : public ObVarcharSysVar
{
public:
  ObSysVarSlaveLoadTmpdir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_LOAD_TMPDIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(406); }
};
class ObSysVarSlaveSkipErrors : public ObVarcharSysVar
{
public:
  ObSysVarSlaveSkipErrors() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_SKIP_ERRORS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(407); }
};
class ObSysVarInnodbSyncDebug : public ObBoolSysVar
{
public:
  ObSysVarInnodbSyncDebug() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_SYNC_DEBUG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(408); }
};
class ObSysVarDefaultCollationForUtf8mb4 : public ObCharsetSysVar
{
public:
  ObSysVarDefaultCollationForUtf8mb4() : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_default_value_for_utf8mb4, NULL, ObSysVarToObjFuncs::to_obj_collation, ObSysVarToStrFuncs::to_str_collation, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEFAULT_COLLATION_FOR_UTF8MB4; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(409); }
};
class ObSysVarEnableOldCharsetAggregation : public ObBoolSysVar
{
public:
  ObSysVarEnableOldCharsetAggregation() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__ENABLE_OLD_CHARSET_AGGREGATION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(410); }
};
class ObSysVarEnableSqlPlanMonitor : public ObBoolSysVar
{
public:
  ObSysVarEnableSqlPlanMonitor() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_ENABLE_SQL_PLAN_MONITOR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(411); }
};
class ObSysVarInsertId : public ObIntSysVar
{
public:
  ObSysVarInsertId() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INSERT_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(412); }
};
class ObSysVarJoinBufferSize : public ObIntSysVar
{
public:
  ObSysVarJoinBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_JOIN_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(413); }
};
class ObSysVarMaxJoinSize : public ObIntSysVar
{
public:
  ObSysVarMaxJoinSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_JOIN_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(414); }
};
class ObSysVarMaxLengthForSortData : public ObIntSysVar
{
public:
  ObSysVarMaxLengthForSortData() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_LENGTH_FOR_SORT_DATA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(415); }
};
class ObSysVarMaxPreparedStmtCount : public ObIntSysVar
{
public:
  ObSysVarMaxPreparedStmtCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_PREPARED_STMT_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(416); }
};
class ObSysVarMaxSortLength : public ObIntSysVar
{
public:
  ObSysVarMaxSortLength() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_SORT_LENGTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(417); }
};
class ObSysVarMinExaminedRowLimit : public ObIntSysVar
{
public:
  ObSysVarMinExaminedRowLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MIN_EXAMINED_ROW_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(418); }
};
class ObSysVarMultiRangeCount : public ObIntSysVar
{
public:
  ObSysVarMultiRangeCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MULTI_RANGE_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(419); }
};
class ObSysVarMysqlxConnectTimeout : public ObIntSysVar
{
public:
  ObSysVarMysqlxConnectTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_CONNECT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(420); }
};
class ObSysVarMysqlxIdleWorkerThreadTimeout : public ObIntSysVar
{
public:
  ObSysVarMysqlxIdleWorkerThreadTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_IDLE_WORKER_THREAD_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(421); }
};
class ObSysVarMysqlxMaxAllowedPacket : public ObIntSysVar
{
public:
  ObSysVarMysqlxMaxAllowedPacket() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_MAX_ALLOWED_PACKET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(422); }
};
class ObSysVarMysqlxMaxConnections : public ObIntSysVar
{
public:
  ObSysVarMysqlxMaxConnections() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_MAX_CONNECTIONS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(423); }
};
class ObSysVarMysqlxMinWorkerThreads : public ObIntSysVar
{
public:
  ObSysVarMysqlxMinWorkerThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_MIN_WORKER_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(424); }
};
class ObSysVarPerformanceSchemaShowProcesslist : public ObBoolSysVar
{
public:
  ObSysVarPerformanceSchemaShowProcesslist() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_SHOW_PROCESSLIST; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(425); }
};
class ObSysVarQueryAllocBlockSize : public ObIntSysVar
{
public:
  ObSysVarQueryAllocBlockSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_ALLOC_BLOCK_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(426); }
};
class ObSysVarQueryPreallocSize : public ObIntSysVar
{
public:
  ObSysVarQueryPreallocSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_QUERY_PREALLOC_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(427); }
};
class ObSysVarSlowQueryLog : public ObIntSysVar
{
public:
  ObSysVarSlowQueryLog() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLOW_QUERY_LOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(428); }
};
class ObSysVarSlowQueryLogFile : public ObVarcharSysVar
{
public:
  ObSysVarSlowQueryLogFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLOW_QUERY_LOG_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(429); }
};
class ObSysVarSortBufferSize : public ObIntSysVar
{
public:
  ObSysVarSortBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SORT_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(430); }
};
class ObSysVarSqlBufferResult : public ObIntSysVar
{
public:
  ObSysVarSqlBufferResult() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_BUFFER_RESULT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(431); }
};
class ObSysVarBinlogCacheSize : public ObIntSysVar
{
public:
  ObSysVarBinlogCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(432); }
};
class ObSysVarBinlogDirectNonTransactionalUpdates : public ObIntSysVar
{
public:
  ObSysVarBinlogDirectNonTransactionalUpdates() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_DIRECT_NON_TRANSACTIONAL_UPDATES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(433); }
};
class ObSysVarBinlogErrorAction : public ObEnumSysVar
{
public:
  const static char * BINLOG_ERROR_ACTION_NAMES[];
public:
  ObSysVarBinlogErrorAction() : ObEnumSysVar(BINLOG_ERROR_ACTION_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_ERROR_ACTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(434); }
};
class ObSysVarBinlogGroupCommitSyncDelay : public ObIntSysVar
{
public:
  ObSysVarBinlogGroupCommitSyncDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(435); }
};
class ObSysVarBinlogGroupCommitSyncNoDelayCount : public ObIntSysVar
{
public:
  ObSysVarBinlogGroupCommitSyncNoDelayCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_GROUP_COMMIT_SYNC_NO_DELAY_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(436); }
};
class ObSysVarBinlogMaxFlushQueueTime : public ObIntSysVar
{
public:
  ObSysVarBinlogMaxFlushQueueTime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_MAX_FLUSH_QUEUE_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(437); }
};
class ObSysVarBinlogOrderCommits : public ObIntSysVar
{
public:
  ObSysVarBinlogOrderCommits() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_ORDER_COMMITS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(438); }
};
class ObSysVarBinlogStmtCacheSize : public ObIntSysVar
{
public:
  ObSysVarBinlogStmtCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_STMT_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(439); }
};
class ObSysVarBinlogTransactionDependencyHistorySize : public ObIntSysVar
{
public:
  ObSysVarBinlogTransactionDependencyHistorySize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(440); }
};
class ObSysVarBinlogTransactionDependencyTracking : public ObEnumSysVar
{
public:
  const static char * BINLOG_TRANSACTION_DEPENDENCY_TRACKING_NAMES[];
public:
  ObSysVarBinlogTransactionDependencyTracking() : ObEnumSysVar(BINLOG_TRANSACTION_DEPENDENCY_TRACKING_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_TRANSACTION_DEPENDENCY_TRACKING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(441); }
};
class ObSysVarExpireLogsDays : public ObIntSysVar
{
public:
  ObSysVarExpireLogsDays() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_EXPIRE_LOGS_DAYS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(442); }
};
class ObSysVarInnodbFlushLogAtTimeout : public ObIntSysVar
{
public:
  ObSysVarInnodbFlushLogAtTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FLUSH_LOG_AT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(443); }
};
class ObSysVarInnodbFlushLogAtTrxCommit : public ObIntSysVar
{
public:
  ObSysVarInnodbFlushLogAtTrxCommit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FLUSH_LOG_AT_TRX_COMMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(444); }
};
class ObSysVarInnodbLogCheckpointNow : public ObBoolSysVar
{
public:
  ObSysVarInnodbLogCheckpointNow() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_CHECKPOINT_NOW; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(445); }
};
class ObSysVarInnodbLogChecksums : public ObIntSysVar
{
public:
  ObSysVarInnodbLogChecksums() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_CHECKSUMS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(446); }
};
class ObSysVarInnodbLogCompressedPages : public ObIntSysVar
{
public:
  ObSysVarInnodbLogCompressedPages() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_COMPRESSED_PAGES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(447); }
};
class ObSysVarInnodbLogWriteAheadSize : public ObIntSysVar
{
public:
  ObSysVarInnodbLogWriteAheadSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_WRITE_AHEAD_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(448); }
};
class ObSysVarInnodbMaxUndoLogSize : public ObIntSysVar
{
public:
  ObSysVarInnodbMaxUndoLogSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MAX_UNDO_LOG_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(449); }
};
class ObSysVarInnodbOnlineAlterLogMaxSize : public ObIntSysVar
{
public:
  ObSysVarInnodbOnlineAlterLogMaxSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ONLINE_ALTER_LOG_MAX_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(450); }
};
class ObSysVarInnodbUndoLogTruncate : public ObIntSysVar
{
public:
  ObSysVarInnodbUndoLogTruncate() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_UNDO_LOG_TRUNCATE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(451); }
};
class ObSysVarInnodbUndoLogs : public ObIntSysVar
{
public:
  ObSysVarInnodbUndoLogs() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_UNDO_LOGS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(452); }
};
class ObSysVarLogBinTrustFunctionCreators : public ObIntSysVar
{
public:
  ObSysVarLogBinTrustFunctionCreators() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_BIN_TRUST_FUNCTION_CREATORS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(453); }
};
class ObSysVarLogBinUseV1RowEvents : public ObIntSysVar
{
public:
  ObSysVarLogBinUseV1RowEvents() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_BIN_USE_V1_ROW_EVENTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(454); }
};
class ObSysVarLogBuiltinAsIdentifiedByPassword : public ObIntSysVar
{
public:
  ObSysVarLogBuiltinAsIdentifiedByPassword() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_BUILTIN_AS_IDENTIFIED_BY_PASSWORD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(455); }
};
class ObSysVarMaxBinlogCacheSize : public ObIntSysVar
{
public:
  ObSysVarMaxBinlogCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_BINLOG_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(456); }
};
class ObSysVarMaxBinlogSize : public ObIntSysVar
{
public:
  ObSysVarMaxBinlogSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_BINLOG_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(457); }
};
class ObSysVarMaxBinlogStmtCacheSize : public ObIntSysVar
{
public:
  ObSysVarMaxBinlogStmtCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_BINLOG_STMT_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(458); }
};
class ObSysVarMaxRelayLogSize : public ObIntSysVar
{
public:
  ObSysVarMaxRelayLogSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_RELAY_LOG_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(459); }
};
class ObSysVarRelayLogInfoRepository : public ObVarcharSysVar
{
public:
  ObSysVarRelayLogInfoRepository() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG_INFO_REPOSITORY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(460); }
};
class ObSysVarRelayLogPurge : public ObIntSysVar
{
public:
  ObSysVarRelayLogPurge() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG_PURGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(461); }
};
class ObSysVarSyncBinlog : public ObIntSysVar
{
public:
  ObSysVarSyncBinlog() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SYNC_BINLOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(462); }
};
class ObSysVarSyncRelayLog : public ObIntSysVar
{
public:
  ObSysVarSyncRelayLog() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SYNC_RELAY_LOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(463); }
};
class ObSysVarSyncRelayLogInfo : public ObIntSysVar
{
public:
  ObSysVarSyncRelayLogInfo() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SYNC_RELAY_LOG_INFO; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(464); }
};
class ObSysVarInnodbDeadlockDetect : public ObIntSysVar
{
public:
  ObSysVarInnodbDeadlockDetect() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_DEADLOCK_DETECT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(465); }
};
class ObSysVarInnodbLockWaitTimeout : public ObIntSysVar
{
public:
  ObSysVarInnodbLockWaitTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOCK_WAIT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(466); }
};
class ObSysVarInnodbPrintAllDeadlocks : public ObIntSysVar
{
public:
  ObSysVarInnodbPrintAllDeadlocks() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_PRINT_ALL_DEADLOCKS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(467); }
};
class ObSysVarInnodbTableLocks : public ObIntSysVar
{
public:
  ObSysVarInnodbTableLocks() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_TABLE_LOCKS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(468); }
};
class ObSysVarMaxWriteLockCount : public ObIntSysVar
{
public:
  ObSysVarMaxWriteLockCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_WRITE_LOCK_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(469); }
};
class ObSysVarObEnableRoleIds : public ObVarcharSysVar
{
public:
  ObSysVarObEnableRoleIds() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OB_ENABLE_ROLE_IDS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(470); }
};
class ObSysVarInnodbReadOnly : public ObBoolSysVar
{
public:
  ObSysVarInnodbReadOnly() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_READ_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(471); }
};
class ObSysVarInnodbApiDisableRowlock : public ObBoolSysVar
{
public:
  ObSysVarInnodbApiDisableRowlock() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_API_DISABLE_ROWLOCK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(472); }
};
class ObSysVarInnodbAutoincLockMode : public ObIntSysVar
{
public:
  ObSysVarInnodbAutoincLockMode() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_AUTOINC_LOCK_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(473); }
};
class ObSysVarSkipExternalLocking : public ObBoolSysVar
{
public:
  ObSysVarSkipExternalLocking() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SKIP_EXTERNAL_LOCKING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(474); }
};
class ObSysVarSuperReadOnly : public ObBoolSysVar
{
public:
  ObSysVarSuperReadOnly() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SUPER_READ_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(475); }
};
class ObSysVarPlsqlOptimizeLevel : public ObIntSysVar
{
public:
  ObSysVarPlsqlOptimizeLevel() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PLSQL_OPTIMIZE_LEVEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(476); }
};
class ObSysVarLowPriorityUpdates : public ObBoolSysVar
{
public:
  ObSysVarLowPriorityUpdates() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOW_PRIORITY_UPDATES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(477); }
};
class ObSysVarMaxErrorCount : public ObIntSysVar
{
public:
  ObSysVarMaxErrorCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_ERROR_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(478); }
};
class ObSysVarMaxInsertDelayedThreads : public ObIntSysVar
{
public:
  ObSysVarMaxInsertDelayedThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_INSERT_DELAYED_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(479); }
};
class ObSysVarFtStopwordFile : public ObVarcharSysVar
{
public:
  ObSysVarFtStopwordFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_FT_STOPWORD_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(480); }
};
class ObSysVarInnodbFtCacheSize : public ObIntSysVar
{
public:
  ObSysVarInnodbFtCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FT_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(481); }
};
class ObSysVarInnodbFtSortPllDegree : public ObIntSysVar
{
public:
  ObSysVarInnodbFtSortPllDegree() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FT_SORT_PLL_DEGREE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(482); }
};
class ObSysVarInnodbFtTotalCacheSize : public ObIntSysVar
{
public:
  ObSysVarInnodbFtTotalCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FT_TOTAL_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(483); }
};
class ObSysVarMecabRcFile : public ObVarcharSysVar
{
public:
  ObSysVarMecabRcFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MECAB_RC_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(484); }
};
class ObSysVarMetadataLocksCacheSize : public ObIntSysVar
{
public:
  ObSysVarMetadataLocksCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_METADATA_LOCKS_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(485); }
};
class ObSysVarMetadataLocksHashInstances : public ObIntSysVar
{
public:
  ObSysVarMetadataLocksHashInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_METADATA_LOCKS_HASH_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(486); }
};
class ObSysVarInnodbTempDataFilePath : public ObVarcharSysVar
{
public:
  ObSysVarInnodbTempDataFilePath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_TEMP_DATA_FILE_PATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(487); }
};
class ObSysVarInnodbDataFilePath : public ObVarcharSysVar
{
public:
  ObSysVarInnodbDataFilePath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_DATA_FILE_PATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(488); }
};
class ObSysVarInnodbDataHomeDir : public ObVarcharSysVar
{
public:
  ObSysVarInnodbDataHomeDir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_DATA_HOME_DIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(489); }
};
class ObSysVarAvoidTemporalUpgrade : public ObBoolSysVar
{
public:
  ObSysVarAvoidTemporalUpgrade() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_AVOID_TEMPORAL_UPGRADE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(490); }
};
class ObSysVarDefaultTmpStorageEngine : public ObEnumSysVar
{
public:
  const static char * DEFAULT_TMP_STORAGE_ENGINE_NAMES[];
public:
  ObSysVarDefaultTmpStorageEngine() : ObEnumSysVar(DEFAULT_TMP_STORAGE_ENGINE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEFAULT_TMP_STORAGE_ENGINE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(491); }
};
class ObSysVarInnodbFtEnableDiagPrint : public ObBoolSysVar
{
public:
  ObSysVarInnodbFtEnableDiagPrint() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FT_ENABLE_DIAG_PRINT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(492); }
};
class ObSysVarInnodbFtNumWordOptimize : public ObIntSysVar
{
public:
  ObSysVarInnodbFtNumWordOptimize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FT_NUM_WORD_OPTIMIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(493); }
};
class ObSysVarInnodbFtResultCacheLimit : public ObIntSysVar
{
public:
  ObSysVarInnodbFtResultCacheLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FT_RESULT_CACHE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(494); }
};
class ObSysVarInnodbFtServerStopwordTable : public ObVarcharSysVar
{
public:
  ObSysVarInnodbFtServerStopwordTable() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FT_SERVER_STOPWORD_TABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(495); }
};
class ObSysVarInnodbOptimizeFulltextOnly : public ObBoolSysVar
{
public:
  ObSysVarInnodbOptimizeFulltextOnly() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_OPTIMIZE_FULLTEXT_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(496); }
};
class ObSysVarMaxTmpTables : public ObIntSysVar
{
public:
  ObSysVarMaxTmpTables() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_TMP_TABLES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(497); }
};
class ObSysVarInnodbTmpdir : public ObVarcharSysVar
{
public:
  ObSysVarInnodbTmpdir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_TMPDIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(498); }
};
class ObSysVarGroupReplicationGroupSeeds : public ObVarcharSysVar
{
public:
  ObSysVarGroupReplicationGroupSeeds() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_GROUP_REPLICATION_GROUP_SEEDS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(499); }
};
class ObSysVarSlaveRowsSearchAlgorithms : public ObEnumSysVar
{
public:
  const static char * SLAVE_ROWS_SEARCH_ALGORITHMS_NAMES[];
public:
  ObSysVarSlaveRowsSearchAlgorithms() : ObEnumSysVar(SLAVE_ROWS_SEARCH_ALGORITHMS_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_ROWS_SEARCH_ALGORITHMS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(500); }
};
class ObSysVarSlaveTypeConversions : public ObEnumSysVar
{
public:
  const static char * SLAVE_TYPE_CONVERSIONS_NAMES[];
public:
  ObSysVarSlaveTypeConversions() : ObEnumSysVar(SLAVE_TYPE_CONVERSIONS_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLAVE_TYPE_CONVERSIONS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(501); }
};
class ObSysVarObHnswEfSearch : public ObIntSysVar
{
public:
  ObSysVarObHnswEfSearch() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_HNSW_EF_SEARCH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(502); }
};
class ObSysVarNdbAllowCopyingAlterTable : public ObBoolSysVar
{
public:
  ObSysVarNdbAllowCopyingAlterTable() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_ALLOW_COPYING_ALTER_TABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(503); }
};
class ObSysVarNdbAutoincrementPrefetchSz : public ObIntSysVar
{
public:
  ObSysVarNdbAutoincrementPrefetchSz() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_AUTOINCREMENT_PREFETCH_SZ; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(504); }
};
class ObSysVarNdbBlobReadBatchBytes : public ObIntSysVar
{
public:
  ObSysVarNdbBlobReadBatchBytes() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_BLOB_READ_BATCH_BYTES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(505); }
};
class ObSysVarNdbBlobWriteBatchBytes : public ObIntSysVar
{
public:
  ObSysVarNdbBlobWriteBatchBytes() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_BLOB_WRITE_BATCH_BYTES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(506); }
};
class ObSysVarNdbCacheCheckTime : public ObIntSysVar
{
public:
  ObSysVarNdbCacheCheckTime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_CACHE_CHECK_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(507); }
};
class ObSysVarNdbClearApplyStatus : public ObBoolSysVar
{
public:
  ObSysVarNdbClearApplyStatus() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_CLEAR_APPLY_STATUS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(508); }
};
class ObSysVarNdbDataNodeNeighbour : public ObIntSysVar
{
public:
  ObSysVarNdbDataNodeNeighbour() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_DATA_NODE_NEIGHBOUR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(509); }
};
class ObSysVarNdbDefaultColumnFormat : public ObEnumSysVar
{
public:
  const static char * NDB_DEFAULT_COLUMN_FORMAT_NAMES[];
public:
  ObSysVarNdbDefaultColumnFormat() : ObEnumSysVar(NDB_DEFAULT_COLUMN_FORMAT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_DEFAULT_COLUMN_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(510); }
};
class ObSysVarNdbDeferredConstraints : public ObIntSysVar
{
public:
  ObSysVarNdbDeferredConstraints() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_DEFERRED_CONSTRAINTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(511); }
};
class ObSysVarNdbDistribution : public ObEnumSysVar
{
public:
  const static char * NDB_DISTRIBUTION_NAMES[];
public:
  ObSysVarNdbDistribution() : ObEnumSysVar(NDB_DISTRIBUTION_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_DISTRIBUTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(512); }
};
class ObSysVarNdbEventbufferFreePercent : public ObIntSysVar
{
public:
  ObSysVarNdbEventbufferFreePercent() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_EVENTBUFFER_FREE_PERCENT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(513); }
};
class ObSysVarNdbEventbufferMaxAlloc : public ObIntSysVar
{
public:
  ObSysVarNdbEventbufferMaxAlloc() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_EVENTBUFFER_MAX_ALLOC; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(514); }
};
class ObSysVarNdbExtraLogging : public ObIntSysVar
{
public:
  ObSysVarNdbExtraLogging() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_EXTRA_LOGGING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(515); }
};
class ObSysVarNdbForceSend : public ObBoolSysVar
{
public:
  ObSysVarNdbForceSend() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_FORCE_SEND; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(516); }
};
class ObSysVarNdbFullyReplicated : public ObBoolSysVar
{
public:
  ObSysVarNdbFullyReplicated() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_FULLY_REPLICATED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(517); }
};
class ObSysVarNdbIndexStatEnable : public ObBoolSysVar
{
public:
  ObSysVarNdbIndexStatEnable() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_INDEX_STAT_ENABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(518); }
};
class ObSysVarNdbIndexStatOption : public ObVarcharSysVar
{
public:
  ObSysVarNdbIndexStatOption() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_INDEX_STAT_OPTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(519); }
};
class ObSysVarNdbJoinPushdown : public ObBoolSysVar
{
public:
  ObSysVarNdbJoinPushdown() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_JOIN_PUSHDOWN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(520); }
};
class ObSysVarNdbLogBinlogIndex : public ObBoolSysVar
{
public:
  ObSysVarNdbLogBinlogIndex() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_BINLOG_INDEX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(521); }
};
class ObSysVarNdbLogEmptyEpochs : public ObBoolSysVar
{
public:
  ObSysVarNdbLogEmptyEpochs() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_EMPTY_EPOCHS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(522); }
};
class ObSysVarNdbLogEmptyUpdate : public ObBoolSysVar
{
public:
  ObSysVarNdbLogEmptyUpdate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_EMPTY_UPDATE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(523); }
};
class ObSysVarNdbLogExclusiveReads : public ObBoolSysVar
{
public:
  ObSysVarNdbLogExclusiveReads() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_EXCLUSIVE_READS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(524); }
};
class ObSysVarNdbLogUpdateAsWrite : public ObBoolSysVar
{
public:
  ObSysVarNdbLogUpdateAsWrite() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_UPDATE_AS_WRITE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(525); }
};
class ObSysVarNdbLogUpdateMinimal : public ObBoolSysVar
{
public:
  ObSysVarNdbLogUpdateMinimal() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_UPDATE_MINIMAL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(526); }
};
class ObSysVarNdbLogUpdatedOnly : public ObBoolSysVar
{
public:
  ObSysVarNdbLogUpdatedOnly() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_UPDATED_ONLY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(527); }
};
class ObSysVarNdbOptimizationDelay : public ObIntSysVar
{
public:
  ObSysVarNdbOptimizationDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_OPTIMIZATION_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(528); }
};
class ObSysVarNdbReadBackup : public ObBoolSysVar
{
public:
  ObSysVarNdbReadBackup() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_READ_BACKUP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(529); }
};
class ObSysVarNdbRecvThreadActivationThreshold : public ObIntSysVar
{
public:
  ObSysVarNdbRecvThreadActivationThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_RECV_THREAD_ACTIVATION_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(530); }
};
class ObSysVarNdbRecvThreadCpuMask : public ObVarcharSysVar
{
public:
  ObSysVarNdbRecvThreadCpuMask() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_RECV_THREAD_CPU_MASK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(531); }
};
class ObSysVarNdbReportThreshBinlogEpochSlip : public ObIntSysVar
{
public:
  ObSysVarNdbReportThreshBinlogEpochSlip() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_REPORT_THRESH_BINLOG_EPOCH_SLIP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(532); }
};
class ObSysVarNdbReportThreshBinlogMemUsage : public ObIntSysVar
{
public:
  ObSysVarNdbReportThreshBinlogMemUsage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_REPORT_THRESH_BINLOG_MEM_USAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(533); }
};
class ObSysVarNdbRowChecksum : public ObIntSysVar
{
public:
  ObSysVarNdbRowChecksum() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_ROW_CHECKSUM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(534); }
};
class ObSysVarNdbShowForeignKeyMockTables : public ObBoolSysVar
{
public:
  ObSysVarNdbShowForeignKeyMockTables() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_SHOW_FOREIGN_KEY_MOCK_TABLES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(535); }
};
class ObSysVarNdbSlaveConflictRole : public ObEnumSysVar
{
public:
  const static char * NDB_SLAVE_CONFLICT_ROLE_NAMES[];
public:
  ObSysVarNdbSlaveConflictRole() : ObEnumSysVar(NDB_SLAVE_CONFLICT_ROLE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_SLAVE_CONFLICT_ROLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(536); }
};
class ObSysVarNdbTableNoLogging : public ObBoolSysVar
{
public:
  ObSysVarNdbTableNoLogging() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_TABLE_NO_LOGGING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(537); }
};
class ObSysVarNdbTableTemporary : public ObBoolSysVar
{
public:
  ObSysVarNdbTableTemporary() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_TABLE_TEMPORARY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(538); }
};
class ObSysVarNdbUseExactCount : public ObBoolSysVar
{
public:
  ObSysVarNdbUseExactCount() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_USE_EXACT_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(539); }
};
class ObSysVarNdbUseTransactions : public ObBoolSysVar
{
public:
  ObSysVarNdbUseTransactions() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_USE_TRANSACTIONS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(540); }
};
class ObSysVarNdbinfoMaxBytes : public ObIntSysVar
{
public:
  ObSysVarNdbinfoMaxBytes() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDBINFO_MAX_BYTES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(541); }
};
class ObSysVarNdbinfoMaxRows : public ObIntSysVar
{
public:
  ObSysVarNdbinfoMaxRows() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDBINFO_MAX_ROWS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(542); }
};
class ObSysVarNdbinfoOffline : public ObBoolSysVar
{
public:
  ObSysVarNdbinfoOffline() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDBINFO_OFFLINE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(543); }
};
class ObSysVarNdbinfoShowHidden : public ObBoolSysVar
{
public:
  ObSysVarNdbinfoShowHidden() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDBINFO_SHOW_HIDDEN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(544); }
};
class ObSysVarMyisamDataPointerSize : public ObIntSysVar
{
public:
  ObSysVarMyisamDataPointerSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYISAM_DATA_POINTER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(545); }
};
class ObSysVarMyisamMaxSortFileSize : public ObIntSysVar
{
public:
  ObSysVarMyisamMaxSortFileSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYISAM_MAX_SORT_FILE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(546); }
};
class ObSysVarMyisamRepairThreads : public ObIntSysVar
{
public:
  ObSysVarMyisamRepairThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYISAM_REPAIR_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(547); }
};
class ObSysVarMyisamSortBufferSize : public ObIntSysVar
{
public:
  ObSysVarMyisamSortBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYISAM_SORT_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(548); }
};
class ObSysVarMyisamStatsMethod : public ObEnumSysVar
{
public:
  const static char * MYISAM_STATS_METHOD_NAMES[];
public:
  ObSysVarMyisamStatsMethod() : ObEnumSysVar(MYISAM_STATS_METHOD_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYISAM_STATS_METHOD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(549); }
};
class ObSysVarMyisamUseMmap : public ObBoolSysVar
{
public:
  ObSysVarMyisamUseMmap() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYISAM_USE_MMAP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(550); }
};
class ObSysVarPreloadBufferSize : public ObIntSysVar
{
public:
  ObSysVarPreloadBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PRELOAD_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(551); }
};
class ObSysVarReadBufferSize : public ObIntSysVar
{
public:
  ObSysVarReadBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_READ_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(552); }
};
class ObSysVarReadRndBufferSize : public ObIntSysVar
{
public:
  ObSysVarReadRndBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_READ_RND_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(553); }
};
class ObSysVarSyncFrm : public ObIntSysVar
{
public:
  ObSysVarSyncFrm() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SYNC_FRM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(554); }
};
class ObSysVarSyncMasterInfo : public ObIntSysVar
{
public:
  ObSysVarSyncMasterInfo() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SYNC_MASTER_INFO; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(555); }
};
class ObSysVarTableOpenCache : public ObIntSysVar
{
public:
  ObSysVarTableOpenCache() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TABLE_OPEN_CACHE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(556); }
};
class ObSysVarInnodbMonitorDisable : public ObVarcharSysVar
{
public:
  ObSysVarInnodbMonitorDisable() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MONITOR_DISABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(557); }
};
class ObSysVarInnodbMonitorEnable : public ObVarcharSysVar
{
public:
  ObSysVarInnodbMonitorEnable() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MONITOR_ENABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(558); }
};
class ObSysVarInnodbMonitorReset : public ObVarcharSysVar
{
public:
  ObSysVarInnodbMonitorReset() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MONITOR_RESET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(559); }
};
class ObSysVarInnodbMonitorResetAll : public ObVarcharSysVar
{
public:
  ObSysVarInnodbMonitorResetAll() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_MONITOR_RESET_ALL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(560); }
};
class ObSysVarInnodbOldBlocksPct : public ObIntSysVar
{
public:
  ObSysVarInnodbOldBlocksPct() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_OLD_BLOCKS_PCT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(561); }
};
class ObSysVarInnodbOldBlocksTime : public ObIntSysVar
{
public:
  ObSysVarInnodbOldBlocksTime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_OLD_BLOCKS_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(562); }
};
class ObSysVarInnodbPurgeBatchSize : public ObIntSysVar
{
public:
  ObSysVarInnodbPurgeBatchSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_PURGE_BATCH_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(563); }
};
class ObSysVarInnodbPurgeRsegTruncateFrequency : public ObIntSysVar
{
public:
  ObSysVarInnodbPurgeRsegTruncateFrequency() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_PURGE_RSEG_TRUNCATE_FREQUENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(564); }
};
class ObSysVarInnodbRandomReadAhead : public ObBoolSysVar
{
public:
  ObSysVarInnodbRandomReadAhead() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_RANDOM_READ_AHEAD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(565); }
};
class ObSysVarInnodbReadAheadThreshold : public ObIntSysVar
{
public:
  ObSysVarInnodbReadAheadThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_READ_AHEAD_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(566); }
};
class ObSysVarInnodbRollbackSegments : public ObIntSysVar
{
public:
  ObSysVarInnodbRollbackSegments() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_ROLLBACK_SEGMENTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(567); }
};
class ObSysVarInnodbSpinWaitDelay : public ObIntSysVar
{
public:
  ObSysVarInnodbSpinWaitDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_SPIN_WAIT_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(568); }
};
class ObSysVarInnodbStatusOutput : public ObBoolSysVar
{
public:
  ObSysVarInnodbStatusOutput() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATUS_OUTPUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(569); }
};
class ObSysVarInnodbStatusOutputLocks : public ObBoolSysVar
{
public:
  ObSysVarInnodbStatusOutputLocks() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATUS_OUTPUT_LOCKS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(570); }
};
class ObSysVarInnodbSyncSpinLoops : public ObIntSysVar
{
public:
  ObSysVarInnodbSyncSpinLoops() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_SYNC_SPIN_LOOPS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(571); }
};
class ObSysVarInternalTmpDiskStorageEngine : public ObEnumSysVar
{
public:
  const static char * INTERNAL_TMP_DISK_STORAGE_ENGINE_NAMES[];
public:
  ObSysVarInternalTmpDiskStorageEngine() : ObEnumSysVar(INTERNAL_TMP_DISK_STORAGE_ENGINE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INTERNAL_TMP_DISK_STORAGE_ENGINE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(572); }
};
class ObSysVarKeepFilesOnCreate : public ObBoolSysVar
{
public:
  ObSysVarKeepFilesOnCreate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEEP_FILES_ON_CREATE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(573); }
};
class ObSysVarMaxHeapTableSize : public ObIntSysVar
{
public:
  ObSysVarMaxHeapTableSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_HEAP_TABLE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(574); }
};
class ObSysVarBulkInsertBufferSize : public ObIntSysVar
{
public:
  ObSysVarBulkInsertBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BULK_INSERT_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(575); }
};
class ObSysVarHostCacheSize : public ObIntSysVar
{
public:
  ObSysVarHostCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HOST_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(576); }
};
class ObSysVarInitSlave : public ObVarcharSysVar
{
public:
  ObSysVarInitSlave() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INIT_SLAVE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(577); }
};
class ObSysVarInnodbFastShutdown : public ObIntSysVar
{
public:
  ObSysVarInnodbFastShutdown() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_FAST_SHUTDOWN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(578); }
};
class ObSysVarInnodbIoCapacity : public ObIntSysVar
{
public:
  ObSysVarInnodbIoCapacity() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_IO_CAPACITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(579); }
};
class ObSysVarInnodbIoCapacityMax : public ObIntSysVar
{
public:
  ObSysVarInnodbIoCapacityMax() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_IO_CAPACITY_MAX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(580); }
};
class ObSysVarInnodbThreadConcurrency : public ObIntSysVar
{
public:
  ObSysVarInnodbThreadConcurrency() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_THREAD_CONCURRENCY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(581); }
};
class ObSysVarInnodbThreadSleepDelay : public ObIntSysVar
{
public:
  ObSysVarInnodbThreadSleepDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_THREAD_SLEEP_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(582); }
};
class ObSysVarLogErrorVerbosity : public ObIntSysVar
{
public:
  ObSysVarLogErrorVerbosity() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_ERROR_VERBOSITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(583); }
};
class ObSysVarLogOutput : public ObVarcharSysVar
{
public:
  ObSysVarLogOutput() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_OUTPUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(584); }
};
class ObSysVarLogQueriesNotUsingIndexes : public ObBoolSysVar
{
public:
  ObSysVarLogQueriesNotUsingIndexes() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_QUERIES_NOT_USING_INDEXES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(585); }
};
class ObSysVarLogSlowAdminStatements : public ObBoolSysVar
{
public:
  ObSysVarLogSlowAdminStatements() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_SLOW_ADMIN_STATEMENTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(586); }
};
class ObSysVarLogSlowSlaveStatements : public ObBoolSysVar
{
public:
  ObSysVarLogSlowSlaveStatements() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_SLOW_SLAVE_STATEMENTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(587); }
};
class ObSysVarLogStatementsUnsafeForBinlog : public ObIntSysVar
{
public:
  ObSysVarLogStatementsUnsafeForBinlog() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_STATEMENTS_UNSAFE_FOR_BINLOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(588); }
};
class ObSysVarLogSyslog : public ObBoolSysVar
{
public:
  ObSysVarLogSyslog() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_SYSLOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(589); }
};
class ObSysVarLogSyslogFacility : public ObVarcharSysVar
{
public:
  ObSysVarLogSyslogFacility() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_SYSLOG_FACILITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(590); }
};
class ObSysVarLogSyslogIncludePid : public ObIntSysVar
{
public:
  ObSysVarLogSyslogIncludePid() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_SYSLOG_INCLUDE_PID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(591); }
};
class ObSysVarLogSyslogTag : public ObVarcharSysVar
{
public:
  ObSysVarLogSyslogTag() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_SYSLOG_TAG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(592); }
};
class ObSysVarLogThrottleQueriesNotUsingIndexes : public ObIntSysVar
{
public:
  ObSysVarLogThrottleQueriesNotUsingIndexes() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_THROTTLE_QUERIES_NOT_USING_INDEXES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(593); }
};
class ObSysVarLogTimestamps : public ObEnumSysVar
{
public:
  const static char * LOG_TIMESTAMPS_NAMES[];
public:
  ObSysVarLogTimestamps() : ObEnumSysVar(LOG_TIMESTAMPS_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_TIMESTAMPS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(594); }
};
class ObSysVarLogWarnings : public ObIntSysVar
{
public:
  ObSysVarLogWarnings() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_WARNINGS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(595); }
};
class ObSysVarMaxDelayedThreads : public ObIntSysVar
{
public:
  ObSysVarMaxDelayedThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_DELAYED_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(596); }
};
class ObSysVarOfflineMode : public ObIntSysVar
{
public:
  ObSysVarOfflineMode() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OFFLINE_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(597); }
};
class ObSysVarRequireSecureTransport : public ObBoolSysVar
{
public:
  ObSysVarRequireSecureTransport() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REQUIRE_SECURE_TRANSPORT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(598); }
};
class ObSysVarSlowLaunchTime : public ObIntSysVar
{
public:
  ObSysVarSlowLaunchTime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SLOW_LAUNCH_TIME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(599); }
};
class ObSysVarSqlLogOff : public ObBoolSysVar
{
public:
  ObSysVarSqlLogOff() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_LOG_OFF; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(600); }
};
class ObSysVarThreadCacheSize : public ObIntSysVar
{
public:
  ObSysVarThreadCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_CACHE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(601); }
};
class ObSysVarThreadPoolHighPriorityConnection : public ObIntSysVar
{
public:
  ObSysVarThreadPoolHighPriorityConnection() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_POOL_HIGH_PRIORITY_CONNECTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(602); }
};
class ObSysVarThreadPoolMaxUnusedThreads : public ObIntSysVar
{
public:
  ObSysVarThreadPoolMaxUnusedThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_POOL_MAX_UNUSED_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(603); }
};
class ObSysVarThreadPoolPrioKickupTimer : public ObIntSysVar
{
public:
  ObSysVarThreadPoolPrioKickupTimer() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_POOL_PRIO_KICKUP_TIMER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(604); }
};
class ObSysVarThreadPoolStallLimit : public ObIntSysVar
{
public:
  ObSysVarThreadPoolStallLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_POOL_STALL_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(605); }
};
class ObSysVarHaveStatementTimeout : public ObBoolSysVar
{
public:
  ObSysVarHaveStatementTimeout() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_STATEMENT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(606); }
};
class ObSysVarMysqlxBindAddress : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxBindAddress() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_BIND_ADDRESS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(607); }
};
class ObSysVarMysqlxPort : public ObIntSysVar
{
public:
  ObSysVarMysqlxPort() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_PORT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(608); }
};
class ObSysVarMysqlxPortOpenTimeout : public ObIntSysVar
{
public:
  ObSysVarMysqlxPortOpenTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_PORT_OPEN_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(609); }
};
class ObSysVarMysqlxSocket : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSocket() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SOCKET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(610); }
};
class ObSysVarMysqlxSslCa : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSslCa() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SSL_CA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(611); }
};
class ObSysVarMysqlxSslCapath : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSslCapath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SSL_CAPATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(612); }
};
class ObSysVarMysqlxSslCert : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSslCert() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SSL_CERT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(613); }
};
class ObSysVarMysqlxSslCipher : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSslCipher() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SSL_CIPHER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(614); }
};
class ObSysVarMysqlxSslCrl : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSslCrl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SSL_CRL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(615); }
};
class ObSysVarMysqlxSslCrlpath : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSslCrlpath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SSL_CRLPATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(616); }
};
class ObSysVarMysqlxSslKey : public ObVarcharSysVar
{
public:
  ObSysVarMysqlxSslKey() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQLX_SSL_KEY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(617); }
};
class ObSysVarOld : public ObBoolSysVar
{
public:
  ObSysVarOld() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(618); }
};
class ObSysVarPerformanceSchemaAccountsSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaAccountsSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_ACCOUNTS_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(619); }
};
class ObSysVarPerformanceSchemaDigestsSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaDigestsSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_DIGESTS_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(620); }
};
class ObSysVarPerformanceSchemaEventsStagesHistoryLongSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsStagesHistoryLongSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_STAGES_HISTORY_LONG_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(621); }
};
class ObSysVarPerformanceSchemaEventsStagesHistorySize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsStagesHistorySize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_STAGES_HISTORY_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(622); }
};
class ObSysVarPerformanceSchemaEventsStatementsHistoryLongSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsStatementsHistoryLongSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_STATEMENTS_HISTORY_LONG_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(623); }
};
class ObSysVarPerformanceSchemaEventsStatementsHistorySize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsStatementsHistorySize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_STATEMENTS_HISTORY_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(624); }
};
class ObSysVarPerformanceSchemaEventsTransactionsHistoryLongSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsTransactionsHistoryLongSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_TRANSACTIONS_HISTORY_LONG_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(625); }
};
class ObSysVarPerformanceSchemaEventsTransactionsHistorySize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsTransactionsHistorySize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_TRANSACTIONS_HISTORY_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(626); }
};
class ObSysVarPerformanceSchemaEventsWaitsHistoryLongSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsWaitsHistoryLongSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_WAITS_HISTORY_LONG_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(627); }
};
class ObSysVarPerformanceSchemaEventsWaitsHistorySize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaEventsWaitsHistorySize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_EVENTS_WAITS_HISTORY_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(628); }
};
class ObSysVarPerformanceSchemaHostsSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaHostsSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_HOSTS_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(629); }
};
class ObSysVarPerformanceSchemaMaxCondClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxCondClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_COND_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(630); }
};
class ObSysVarPerformanceSchemaMaxCondInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxCondInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_COND_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(631); }
};
class ObSysVarPerformanceSchemaMaxDigestLength : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxDigestLength() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_DIGEST_LENGTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(632); }
};
class ObSysVarPerformanceSchemaMaxFileClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxFileClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_FILE_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(633); }
};
class ObSysVarPerformanceSchemaMaxFileHandles : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxFileHandles() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_FILE_HANDLES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(634); }
};
class ObSysVarPerformanceSchemaMaxFileInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxFileInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_FILE_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(635); }
};
class ObSysVarPerformanceSchemaMaxIndexStat : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxIndexStat() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_INDEX_STAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(636); }
};
class ObSysVarPerformanceSchemaMaxMemoryClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxMemoryClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_MEMORY_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(637); }
};
class ObSysVarPerformanceSchemaMaxMetadataLocks : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxMetadataLocks() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_METADATA_LOCKS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(638); }
};
class ObSysVarPerformanceSchemaMaxMutexClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxMutexClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_MUTEX_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(639); }
};
class ObSysVarPerformanceSchemaMaxMutexInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxMutexInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_MUTEX_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(640); }
};
class ObSysVarPerformanceSchemaMaxPreparedStatementsInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxPreparedStatementsInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_PREPARED_STATEMENTS_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(641); }
};
class ObSysVarPerformanceSchemaMaxProgramInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxProgramInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_PROGRAM_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(642); }
};
class ObSysVarPerformanceSchemaMaxRwlockClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxRwlockClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_RWLOCK_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(643); }
};
class ObSysVarPerformanceSchemaMaxRwlockInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxRwlockInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_RWLOCK_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(644); }
};
class ObSysVarPerformanceSchemaMaxSocketClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxSocketClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_SOCKET_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(645); }
};
class ObSysVarPerformanceSchemaMaxSocketInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxSocketInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_SOCKET_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(646); }
};
class ObSysVarPerformanceSchemaMaxSqlTextLength : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxSqlTextLength() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_SQL_TEXT_LENGTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(647); }
};
class ObSysVarPerformanceSchemaMaxStageClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxStageClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_STAGE_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(648); }
};
class ObSysVarPerformanceSchemaMaxStatementClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxStatementClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_STATEMENT_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(649); }
};
class ObSysVarPerformanceSchemaMaxStatementStack : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxStatementStack() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_STATEMENT_STACK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(650); }
};
class ObSysVarPerformanceSchemaMaxTableHandles : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxTableHandles() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_TABLE_HANDLES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(651); }
};
class ObSysVarPerformanceSchemaMaxTableInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxTableInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_TABLE_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(652); }
};
class ObSysVarPerformanceSchemaMaxTableLockStat : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxTableLockStat() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_TABLE_LOCK_STAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(653); }
};
class ObSysVarPerformanceSchemaMaxThreadClasses : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxThreadClasses() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_THREAD_CLASSES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(654); }
};
class ObSysVarPerformanceSchemaMaxThreadInstances : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaMaxThreadInstances() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_MAX_THREAD_INSTANCES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(655); }
};
class ObSysVarPerformanceSchemaSessionConnectAttrsSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaSessionConnectAttrsSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_SESSION_CONNECT_ATTRS_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(656); }
};
class ObSysVarPerformanceSchemaSetupActorsSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaSetupActorsSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_SETUP_ACTORS_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(657); }
};
class ObSysVarPerformanceSchemaSetupObjectsSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaSetupObjectsSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_SETUP_OBJECTS_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(658); }
};
class ObSysVarPerformanceSchemaUsersSize : public ObIntSysVar
{
public:
  ObSysVarPerformanceSchemaUsersSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PERFORMANCE_SCHEMA_USERS_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(659); }
};
class ObSysVarVersionTokensSessionNumber : public ObIntSysVar
{
public:
  ObSysVarVersionTokensSessionNumber() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VERSION_TOKENS_SESSION_NUMBER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(660); }
};
class ObSysVarBackLog : public ObIntSysVar
{
public:
  ObSysVarBackLog() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BACK_LOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(661); }
};
class ObSysVarBasedir : public ObVarcharSysVar
{
public:
  ObSysVarBasedir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BASEDIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(662); }
};
class ObSysVarBindAddress : public ObVarcharSysVar
{
public:
  ObSysVarBindAddress() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BIND_ADDRESS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(663); }
};
class ObSysVarCoreFile : public ObBoolSysVar
{
public:
  ObSysVarCoreFile() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CORE_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(664); }
};
class ObSysVarHaveCompress : public ObBoolSysVar
{
public:
  ObSysVarHaveCompress() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_COMPRESS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(665); }
};
class ObSysVarIgnoreDbDirs : public ObVarcharSysVar
{
public:
  ObSysVarIgnoreDbDirs() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_IGNORE_DB_DIRS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(666); }
};
class ObSysVarInitFile : public ObVarcharSysVar
{
public:
  ObSysVarInitFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INIT_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(667); }
};
class ObSysVarInnodbNumaInterleave : public ObBoolSysVar
{
public:
  ObSysVarInnodbNumaInterleave() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_NUMA_INTERLEAVE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(668); }
};
class ObSysVarInnodbOpenFiles : public ObIntSysVar
{
public:
  ObSysVarInnodbOpenFiles() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_OPEN_FILES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(669); }
};
class ObSysVarInnodbPageCleaners : public ObIntSysVar
{
public:
  ObSysVarInnodbPageCleaners() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_PAGE_CLEANERS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(670); }
};
class ObSysVarInnodbPurgeThreads : public ObIntSysVar
{
public:
  ObSysVarInnodbPurgeThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_PURGE_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(671); }
};
class ObSysVarInnodbReadIoThreads : public ObIntSysVar
{
public:
  ObSysVarInnodbReadIoThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_READ_IO_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(672); }
};
class ObSysVarInnodbSyncArraySize : public ObIntSysVar
{
public:
  ObSysVarInnodbSyncArraySize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_SYNC_ARRAY_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(673); }
};
class ObSysVarInnodbUseNativeAio : public ObIntSysVar
{
public:
  ObSysVarInnodbUseNativeAio() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_USE_NATIVE_AIO; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(674); }
};
class ObSysVarInnodbWriteIoThreads : public ObIntSysVar
{
public:
  ObSysVarInnodbWriteIoThreads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_WRITE_IO_THREADS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(675); }
};
class ObSysVarLargeFilesSupport : public ObIntSysVar
{
public:
  ObSysVarLargeFilesSupport() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LARGE_FILES_SUPPORT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(676); }
};
class ObSysVarLargePages : public ObBoolSysVar
{
public:
  ObSysVarLargePages() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LARGE_PAGES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(677); }
};
class ObSysVarLargePageSize : public ObIntSysVar
{
public:
  ObSysVarLargePageSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LARGE_PAGE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(678); }
};
class ObSysVarLockedInMemory : public ObBoolSysVar
{
public:
  ObSysVarLockedInMemory() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOCKED_IN_MEMORY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(679); }
};
class ObSysVarLogError : public ObVarcharSysVar
{
public:
  ObSysVarLogError() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_ERROR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(680); }
};
class ObSysVarNamedPipe : public ObBoolSysVar
{
public:
  ObSysVarNamedPipe() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NAMED_PIPE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(681); }
};
class ObSysVarNamedPipeFullAccessGroup : public ObVarcharSysVar
{
public:
  ObSysVarNamedPipeFullAccessGroup() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NAMED_PIPE_FULL_ACCESS_GROUP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(682); }
};
class ObSysVarOpenFilesLimit : public ObIntSysVar
{
public:
  ObSysVarOpenFilesLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPEN_FILES_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(683); }
};
class ObSysVarReportHost : public ObVarcharSysVar
{
public:
  ObSysVarReportHost() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REPORT_HOST; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(684); }
};
class ObSysVarReportPassword : public ObVarcharSysVar
{
public:
  ObSysVarReportPassword() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REPORT_PASSWORD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(685); }
};
class ObSysVarReportPort : public ObVarcharSysVar
{
public:
  ObSysVarReportPort() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REPORT_PORT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(686); }
};
class ObSysVarReportUser : public ObVarcharSysVar
{
public:
  ObSysVarReportUser() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REPORT_USER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(687); }
};
class ObSysVarServerIdBits : public ObIntSysVar
{
public:
  ObSysVarServerIdBits() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SERVER_ID_BITS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(688); }
};
class ObSysVarSharedMemory : public ObBoolSysVar
{
public:
  ObSysVarSharedMemory() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHARED_MEMORY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(689); }
};
class ObSysVarSharedMemoryBaseName : public ObVarcharSysVar
{
public:
  ObSysVarSharedMemoryBaseName() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHARED_MEMORY_BASE_NAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(690); }
};
class ObSysVarSkipNameResolve : public ObBoolSysVar
{
public:
  ObSysVarSkipNameResolve() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SKIP_NAME_RESOLVE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(691); }
};
class ObSysVarSkipNetworking : public ObBoolSysVar
{
public:
  ObSysVarSkipNetworking() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SKIP_NETWORKING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(692); }
};
class ObSysVarThreadHandling : public ObEnumSysVar
{
public:
  const static char * THREAD_HANDLING_NAMES[];
public:
  ObSysVarThreadHandling() : ObEnumSysVar(THREAD_HANDLING_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_HANDLING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(693); }
};
class ObSysVarThreadPoolAlgorithm : public ObIntSysVar
{
public:
  ObSysVarThreadPoolAlgorithm() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_POOL_ALGORITHM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(694); }
};
class ObSysVarThreadPoolSize : public ObIntSysVar
{
public:
  ObSysVarThreadPoolSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_POOL_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(695); }
};
class ObSysVarThreadStack : public ObIntSysVar
{
public:
  ObSysVarThreadStack() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_THREAD_STACK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(696); }
};
class ObSysVarBinlogGtidSimpleRecovery : public ObIntSysVar
{
public:
  ObSysVarBinlogGtidSimpleRecovery() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BINLOG_GTID_SIMPLE_RECOVERY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(697); }
};
class ObSysVarInnodbApiEnableBinlog : public ObBoolSysVar
{
public:
  ObSysVarInnodbApiEnableBinlog() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_API_ENABLE_BINLOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(698); }
};
class ObSysVarInnodbLocksUnsafeForBinlog : public ObBoolSysVar
{
public:
  ObSysVarInnodbLocksUnsafeForBinlog() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOCKS_UNSAFE_FOR_BINLOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(699); }
};
class ObSysVarInnodbLogBufferSize : public ObIntSysVar
{
public:
  ObSysVarInnodbLogBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(700); }
};
class ObSysVarInnodbLogFilesInGroup : public ObIntSysVar
{
public:
  ObSysVarInnodbLogFilesInGroup() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_FILES_IN_GROUP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(701); }
};
class ObSysVarInnodbLogFileSize : public ObIntSysVar
{
public:
  ObSysVarInnodbLogFileSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_FILE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(702); }
};
class ObSysVarInnodbLogGroupHomeDir : public ObVarcharSysVar
{
public:
  ObSysVarInnodbLogGroupHomeDir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LOG_GROUP_HOME_DIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(703); }
};
class ObSysVarInnodbUndoDirectory : public ObVarcharSysVar
{
public:
  ObSysVarInnodbUndoDirectory() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_UNDO_DIRECTORY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(704); }
};
class ObSysVarInnodbUndoTablespaces : public ObIntSysVar
{
public:
  ObSysVarInnodbUndoTablespaces() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_UNDO_TABLESPACES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(705); }
};
class ObSysVarLogBinBasename : public ObVarcharSysVar
{
public:
  ObSysVarLogBinBasename() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_BIN_BASENAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(706); }
};
class ObSysVarLogBinIndex : public ObVarcharSysVar
{
public:
  ObSysVarLogBinIndex() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_BIN_INDEX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(707); }
};
class ObSysVarLogSlaveUpdates : public ObBoolSysVar
{
public:
  ObSysVarLogSlaveUpdates() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOG_SLAVE_UPDATES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(708); }
};
class ObSysVarRelayLog : public ObVarcharSysVar
{
public:
  ObSysVarRelayLog() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(709); }
};
class ObSysVarRelayLogBasename : public ObVarcharSysVar
{
public:
  ObSysVarRelayLogBasename() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG_BASENAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(710); }
};
class ObSysVarRelayLogIndex : public ObVarcharSysVar
{
public:
  ObSysVarRelayLogIndex() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG_INDEX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(711); }
};
class ObSysVarRelayLogInfoFile : public ObVarcharSysVar
{
public:
  ObSysVarRelayLogInfoFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG_INFO_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(712); }
};
class ObSysVarRelayLogRecovery : public ObBoolSysVar
{
public:
  ObSysVarRelayLogRecovery() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG_RECOVERY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(713); }
};
class ObSysVarRelayLogSpaceLimit : public ObIntSysVar
{
public:
  ObSysVarRelayLogSpaceLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RELAY_LOG_SPACE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(714); }
};
class ObSysVarDelayKeyWrite : public ObEnumSysVar
{
public:
  const static char * DELAY_KEY_WRITE_NAMES[];
public:
  ObSysVarDelayKeyWrite() : ObEnumSysVar(DELAY_KEY_WRITE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DELAY_KEY_WRITE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(715); }
};
class ObSysVarInnodbLargePrefix : public ObEnumSysVar
{
public:
  const static char * INNODB_LARGE_PREFIX_NAMES[];
public:
  ObSysVarInnodbLargePrefix() : ObEnumSysVar(INNODB_LARGE_PREFIX_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_LARGE_PREFIX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(716); }
};
class ObSysVarKeyBufferSize : public ObIntSysVar
{
public:
  ObSysVarKeyBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEY_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(717); }
};
class ObSysVarKeyCacheAgeThreshold : public ObIntSysVar
{
public:
  ObSysVarKeyCacheAgeThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEY_CACHE_AGE_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(718); }
};
class ObSysVarKeyCacheDivisionLimit : public ObIntSysVar
{
public:
  ObSysVarKeyCacheDivisionLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEY_CACHE_DIVISION_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(719); }
};
class ObSysVarMaxSeeksForKey : public ObIntSysVar
{
public:
  ObSysVarMaxSeeksForKey() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_SEEKS_FOR_KEY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(720); }
};
class ObSysVarOldAlterTable : public ObEnumSysVar
{
public:
  const static char * OLD_ALTER_TABLE_NAMES[];
public:
  ObSysVarOldAlterTable() : ObEnumSysVar(OLD_ALTER_TABLE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OLD_ALTER_TABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(721); }
};
class ObSysVarTableDefinitionCache : public ObIntSysVar
{
public:
  ObSysVarTableDefinitionCache() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_TABLE_DEFINITION_CACHE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(722); }
};
class ObSysVarInnodbSortBufferSize : public ObIntSysVar
{
public:
  ObSysVarInnodbSortBufferSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_SORT_BUFFER_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(723); }
};
class ObSysVarKeyCacheBlockSize : public ObIntSysVar
{
public:
  ObSysVarKeyCacheBlockSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEY_CACHE_BLOCK_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(724); }
};
class ObSysVarObKvMode : public ObEnumSysVar
{
public:
  const static char * OB_KV_MODE_NAMES[];
public:
  ObSysVarObKvMode() : ObEnumSysVar(OB_KV_MODE_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_KV_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(725); }
};
class ObSysVarObClientCapabilityFlag : public ObIntSysVar
{
public:
  ObSysVarObClientCapabilityFlag() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR___OB_CLIENT_CAPABILITY_FLAG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(726); }
};
class ObSysVarObEnableParameterAnonymousBlock : public ObBoolSysVar
{
public:
  ObSysVarObEnableParameterAnonymousBlock() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_ENABLE_PARAMETER_ANONYMOUS_BLOCK; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(727); }
};
class ObSysVarCharacterSetsDir : public ObVarcharSysVar
{
public:
  ObSysVarCharacterSetsDir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHARACTER_SETS_DIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(728); }
};
class ObSysVarDateFormat : public ObVarcharSysVar
{
public:
  ObSysVarDateFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DATE_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(729); }
};
class ObSysVarDatetimeFormat : public ObVarcharSysVar
{
public:
  ObSysVarDatetimeFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DATETIME_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(730); }
};
class ObSysVarDisconnectOnExpiredPassword : public ObBoolSysVar
{
public:
  ObSysVarDisconnectOnExpiredPassword() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DISCONNECT_ON_EXPIRED_PASSWORD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(731); }
};
class ObSysVarExternalUser : public ObVarcharSysVar
{
public:
  ObSysVarExternalUser() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_EXTERNAL_USER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(732); }
};
class ObSysVarHaveCrypt : public ObVarcharSysVar
{
public:
  ObSysVarHaveCrypt() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_CRYPT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(733); }
};
class ObSysVarHaveDynamicLoading : public ObVarcharSysVar
{
public:
  ObSysVarHaveDynamicLoading() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_HAVE_DYNAMIC_LOADING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(734); }
};
class ObSysVarKeyringAwsConfFile : public ObVarcharSysVar
{
public:
  ObSysVarKeyringAwsConfFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_AWS_CONF_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(735); }
};
class ObSysVarKeyringAwsDataFile : public ObVarcharSysVar
{
public:
  ObSysVarKeyringAwsDataFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_AWS_DATA_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(736); }
};
class ObSysVarLanguage : public ObVarcharSysVar
{
public:
  ObSysVarLanguage() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LANGUAGE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(737); }
};
class ObSysVarLcMessagesDir : public ObVarcharSysVar
{
public:
  ObSysVarLcMessagesDir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LC_MESSAGES_DIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(738); }
};
class ObSysVarLowerCaseFileSystem : public ObBoolSysVar
{
public:
  ObSysVarLowerCaseFileSystem() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_LOWER_CASE_FILE_SYSTEM; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(739); }
};
class ObSysVarMaxDigestLength : public ObIntSysVar
{
public:
  ObSysVarMaxDigestLength() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_DIGEST_LENGTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(740); }
};
class ObSysVarNdbinfoDatabase : public ObVarcharSysVar
{
public:
  ObSysVarNdbinfoDatabase() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDBINFO_DATABASE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(741); }
};
class ObSysVarNdbinfoTablePrefix : public ObVarcharSysVar
{
public:
  ObSysVarNdbinfoTablePrefix() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDBINFO_TABLE_PREFIX; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(742); }
};
class ObSysVarNdbinfoVersion : public ObVarcharSysVar
{
public:
  ObSysVarNdbinfoVersion() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDBINFO_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(743); }
};
class ObSysVarNdbBatchSize : public ObIntSysVar
{
public:
  ObSysVarNdbBatchSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_BATCH_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(744); }
};
class ObSysVarNdbClusterConnectionPool : public ObIntSysVar
{
public:
  ObSysVarNdbClusterConnectionPool() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_CLUSTER_CONNECTION_POOL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(745); }
};
class ObSysVarNdbClusterConnectionPoolNodeids : public ObVarcharSysVar
{
public:
  ObSysVarNdbClusterConnectionPoolNodeids() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_CLUSTER_CONNECTION_POOL_NODEIDS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(746); }
};
class ObSysVarNdbLogApplyStatus : public ObBoolSysVar
{
public:
  ObSysVarNdbLogApplyStatus() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_APPLY_STATUS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(747); }
};
class ObSysVarNdbLogBin : public ObBoolSysVar
{
public:
  ObSysVarNdbLogBin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_BIN; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(748); }
};
class ObSysVarNdbLogFailTerminate : public ObBoolSysVar
{
public:
  ObSysVarNdbLogFailTerminate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_FAIL_TERMINATE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(749); }
};
class ObSysVarNdbLogOrig : public ObBoolSysVar
{
public:
  ObSysVarNdbLogOrig() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_ORIG; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(750); }
};
class ObSysVarNdbLogTransactionId : public ObBoolSysVar
{
public:
  ObSysVarNdbLogTransactionId() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_LOG_TRANSACTION_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(751); }
};
class ObSysVarNdbOptimizedNodeSelection : public ObIntSysVar
{
public:
  ObSysVarNdbOptimizedNodeSelection() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_OPTIMIZED_NODE_SELECTION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(752); }
};
class ObSysVarNdbSystemName : public ObVarcharSysVar
{
public:
  ObSysVarNdbSystemName() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_SYSTEM_NAME; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(753); }
};
class ObSysVarNdbUseCopyingAlterTable : public ObBoolSysVar
{
public:
  ObSysVarNdbUseCopyingAlterTable() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_USE_COPYING_ALTER_TABLE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(754); }
};
class ObSysVarNdbVersionString : public ObVarcharSysVar
{
public:
  ObSysVarNdbVersionString() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_VERSION_STRING; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(755); }
};
class ObSysVarNdbWaitConnected : public ObIntSysVar
{
public:
  ObSysVarNdbWaitConnected() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_WAIT_CONNECTED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(756); }
};
class ObSysVarNdbWaitSetup : public ObIntSysVar
{
public:
  ObSysVarNdbWaitSetup() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_WAIT_SETUP; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(757); }
};
class ObSysVarProxyUser : public ObVarcharSysVar
{
public:
  ObSysVarProxyUser() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PROXY_USER; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(758); }
};
class ObSysVarSha256PasswordAutoGenerateRsaKeys : public ObBoolSysVar
{
public:
  ObSysVarSha256PasswordAutoGenerateRsaKeys() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHA256_PASSWORD_AUTO_GENERATE_RSA_KEYS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(759); }
};
class ObSysVarSha256PasswordPrivateKeyPath : public ObVarcharSysVar
{
public:
  ObSysVarSha256PasswordPrivateKeyPath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHA256_PASSWORD_PRIVATE_KEY_PATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(760); }
};
class ObSysVarSha256PasswordPublicKeyPath : public ObVarcharSysVar
{
public:
  ObSysVarSha256PasswordPublicKeyPath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHA256_PASSWORD_PUBLIC_KEY_PATH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(761); }
};
class ObSysVarSkipShowDatabase : public ObVarcharSysVar
{
public:
  ObSysVarSkipShowDatabase() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SKIP_SHOW_DATABASE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(762); }
};
class ObSysVarPluginLoad : public ObVarcharSysVar
{
public:
  ObSysVarPluginLoad() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PLUGIN_LOAD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(763); }
};
class ObSysVarPluginLoadAdd : public ObVarcharSysVar
{
public:
  ObSysVarPluginLoadAdd() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PLUGIN_LOAD_ADD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(764); }
};
class ObSysVarBigTables : public ObBoolSysVar
{
public:
  ObSysVarBigTables() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_BIG_TABLES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(765); }
};
class ObSysVarCheckProxyUsers : public ObBoolSysVar
{
public:
  ObSysVarCheckProxyUsers() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CHECK_PROXY_USERS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(766); }
};
class ObSysVarConnectionControlFailedConnectionsThreshold : public ObIntSysVar
{
public:
  ObSysVarConnectionControlFailedConnectionsThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CONNECTION_CONTROL_FAILED_CONNECTIONS_THRESHOLD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(767); }
};
class ObSysVarConnectionControlMaxConnectionDelay : public ObIntSysVar
{
public:
  ObSysVarConnectionControlMaxConnectionDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CONNECTION_CONTROL_MAX_CONNECTION_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(768); }
};
class ObSysVarConnectionControlMinConnectionDelay : public ObIntSysVar
{
public:
  ObSysVarConnectionControlMinConnectionDelay() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_CONNECTION_CONTROL_MIN_CONNECTION_DELAY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(769); }
};
class ObSysVarDefaultWeekFormat : public ObIntSysVar
{
public:
  ObSysVarDefaultWeekFormat() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DEFAULT_WEEK_FORMAT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(770); }
};
class ObSysVarDelayedInsertTimeout : public ObIntSysVar
{
public:
  ObSysVarDelayedInsertTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DELAYED_INSERT_TIMEOUT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(771); }
};
class ObSysVarDelayedQueueSize : public ObIntSysVar
{
public:
  ObSysVarDelayedQueueSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DELAYED_QUEUE_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(772); }
};
class ObSysVarEqRangeIndexDiveLimit : public ObIntSysVar
{
public:
  ObSysVarEqRangeIndexDiveLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_EQ_RANGE_INDEX_DIVE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(773); }
};
class ObSysVarInnodbStatsAutoRecalc : public ObBoolSysVar
{
public:
  ObSysVarInnodbStatsAutoRecalc() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_AUTO_RECALC; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(774); }
};
class ObSysVarInnodbStatsIncludeDeleteMarked : public ObBoolSysVar
{
public:
  ObSysVarInnodbStatsIncludeDeleteMarked() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_INCLUDE_DELETE_MARKED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(775); }
};
class ObSysVarInnodbStatsMethod : public ObEnumSysVar
{
public:
  const static char * INNODB_STATS_METHOD_NAMES[];
public:
  ObSysVarInnodbStatsMethod() : ObEnumSysVar(INNODB_STATS_METHOD_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_METHOD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(776); }
};
class ObSysVarInnodbStatsOnMetadata : public ObBoolSysVar
{
public:
  ObSysVarInnodbStatsOnMetadata() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_ON_METADATA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(777); }
};
class ObSysVarVersionTokensSession : public ObVarcharSysVar
{
public:
  ObSysVarVersionTokensSession() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VERSION_TOKENS_SESSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(778); }
};
class ObSysVarInnodbStatsPersistentSamplePages : public ObIntSysVar
{
public:
  ObSysVarInnodbStatsPersistentSamplePages() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_PERSISTENT_SAMPLE_PAGES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(779); }
};
class ObSysVarInnodbStatsSamplePages : public ObIntSysVar
{
public:
  ObSysVarInnodbStatsSamplePages() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_SAMPLE_PAGES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(780); }
};
class ObSysVarInnodbStatsTransientSamplePages : public ObIntSysVar
{
public:
  ObSysVarInnodbStatsTransientSamplePages() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_INNODB_STATS_TRANSIENT_SAMPLE_PAGES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(781); }
};
class ObSysVarKeyringAwsCmkId : public ObVarcharSysVar
{
public:
  ObSysVarKeyringAwsCmkId() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_AWS_CMK_ID; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(782); }
};
class ObSysVarKeyringAwsRegion : public ObEnumSysVar
{
public:
  const static char * KEYRING_AWS_REGION_NAMES[];
public:
  ObSysVarKeyringAwsRegion() : ObEnumSysVar(KEYRING_AWS_REGION_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_AWS_REGION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(783); }
};
class ObSysVarKeyringEncryptedFileData : public ObVarcharSysVar
{
public:
  ObSysVarKeyringEncryptedFileData() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_ENCRYPTED_FILE_DATA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(784); }
};
class ObSysVarKeyringEncryptedFilePassword : public ObVarcharSysVar
{
public:
  ObSysVarKeyringEncryptedFilePassword() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_ENCRYPTED_FILE_PASSWORD; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(785); }
};
class ObSysVarKeyringFileData : public ObVarcharSysVar
{
public:
  ObSysVarKeyringFileData() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_FILE_DATA; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(786); }
};
class ObSysVarKeyringOkvConfDir : public ObVarcharSysVar
{
public:
  ObSysVarKeyringOkvConfDir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_OKV_CONF_DIR; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(787); }
};
class ObSysVarKeyringOperations : public ObBoolSysVar
{
public:
  ObSysVarKeyringOperations() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_KEYRING_OPERATIONS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(788); }
};
class ObSysVarOptimizerSwitch : public ObVarcharSysVar
{
public:
  ObSysVarOptimizerSwitch() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_SWITCH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(789); }
};
class ObSysVarMaxConnectErrors : public ObIntSysVar
{
public:
  ObSysVarMaxConnectErrors() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MAX_CONNECT_ERRORS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(790); }
};
class ObSysVarMysqlFirewallMode : public ObBoolSysVar
{
public:
  ObSysVarMysqlFirewallMode() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQL_FIREWALL_MODE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(791); }
};
class ObSysVarMysqlFirewallTrace : public ObBoolSysVar
{
public:
  ObSysVarMysqlFirewallTrace() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQL_FIREWALL_TRACE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(792); }
};
class ObSysVarMysqlNativePasswordProxyUsers : public ObBoolSysVar
{
public:
  ObSysVarMysqlNativePasswordProxyUsers() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_MYSQL_NATIVE_PASSWORD_PROXY_USERS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(793); }
};
class ObSysVarNetRetryCount : public ObIntSysVar
{
public:
  ObSysVarNetRetryCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NET_RETRY_COUNT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(794); }
};
class ObSysVarNew : public ObBoolSysVar
{
public:
  ObSysVarNew() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NEW; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(795); }
};
class ObSysVarOldPasswords : public ObEnumSysVar
{
public:
  const static char * OLD_PASSWORDS_NAMES[];
public:
  ObSysVarOldPasswords() : ObEnumSysVar(OLD_PASSWORDS_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OLD_PASSWORDS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(796); }
};
class ObSysVarOptimizerPruneLevel : public ObIntSysVar
{
public:
  ObSysVarOptimizerPruneLevel() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_PRUNE_LEVEL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(797); }
};
class ObSysVarOptimizerSearchDepth : public ObIntSysVar
{
public:
  ObSysVarOptimizerSearchDepth() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_SEARCH_DEPTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(798); }
};
class ObSysVarOptimizerTrace : public ObVarcharSysVar
{
public:
  ObSysVarOptimizerTrace() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_TRACE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(799); }
};
class ObSysVarOptimizerTraceFeatures : public ObVarcharSysVar
{
public:
  ObSysVarOptimizerTraceFeatures() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_TRACE_FEATURES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(800); }
};
class ObSysVarOptimizerTraceLimit : public ObIntSysVar
{
public:
  ObSysVarOptimizerTraceLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_TRACE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(801); }
};
class ObSysVarOptimizerTraceMaxMemSize : public ObIntSysVar
{
public:
  ObSysVarOptimizerTraceMaxMemSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_TRACE_MAX_MEM_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(802); }
};
class ObSysVarOptimizerTraceOffset : public ObIntSysVar
{
public:
  ObSysVarOptimizerTraceOffset() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OPTIMIZER_TRACE_OFFSET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(803); }
};
class ObSysVarParserMaxMemSize : public ObIntSysVar
{
public:
  ObSysVarParserMaxMemSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PARSER_MAX_MEM_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(804); }
};
class ObSysVarRandSeed1 : public ObIntSysVar
{
public:
  ObSysVarRandSeed1() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RAND_SEED1; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(805); }
};
class ObSysVarRandSeed2 : public ObIntSysVar
{
public:
  ObSysVarRandSeed2() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RAND_SEED2; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(806); }
};
class ObSysVarRangeAllocBlockSize : public ObIntSysVar
{
public:
  ObSysVarRangeAllocBlockSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RANGE_ALLOC_BLOCK_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(807); }
};
class ObSysVarRangeOptimizerMaxMemSize : public ObIntSysVar
{
public:
  ObSysVarRangeOptimizerMaxMemSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RANGE_OPTIMIZER_MAX_MEM_SIZE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(808); }
};
class ObSysVarRewriterEnabled : public ObBoolSysVar
{
public:
  ObSysVarRewriterEnabled() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REWRITER_ENABLED; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(809); }
};
class ObSysVarRewriterVerbose : public ObIntSysVar
{
public:
  ObSysVarRewriterVerbose() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_REWRITER_VERBOSE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(810); }
};
class ObSysVarSecureAuth : public ObBoolSysVar
{
public:
  ObSysVarSecureAuth() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SECURE_AUTH; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(811); }
};
class ObSysVarSha256PasswordProxyUsers : public ObBoolSysVar
{
public:
  ObSysVarSha256PasswordProxyUsers() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHA256_PASSWORD_PROXY_USERS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(812); }
};
class ObSysVarShowCompatibility56 : public ObBoolSysVar
{
public:
  ObSysVarShowCompatibility56() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHOW_COMPATIBILITY_56; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(813); }
};
class ObSysVarShowCreateTableVerbosity : public ObBoolSysVar
{
public:
  ObSysVarShowCreateTableVerbosity() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHOW_CREATE_TABLE_VERBOSITY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(814); }
};
class ObSysVarShowOldTemporals : public ObBoolSysVar
{
public:
  ObSysVarShowOldTemporals() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SHOW_OLD_TEMPORALS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(815); }
};
class ObSysVarSqlBigSelects : public ObBoolSysVar
{
public:
  ObSysVarSqlBigSelects() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SQL_BIG_SELECTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(816); }
};
class ObSysVarUpdatableViewsWithLimit : public ObEnumSysVar
{
public:
  const static char * UPDATABLE_VIEWS_WITH_LIMIT_NAMES[];
public:
  ObSysVarUpdatableViewsWithLimit() : ObEnumSysVar(UPDATABLE_VIEWS_WITH_LIMIT_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_UPDATABLE_VIEWS_WITH_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(817); }
};
class ObSysVarValidatePasswordDictionaryFile : public ObVarcharSysVar
{
public:
  ObSysVarValidatePasswordDictionaryFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_VALIDATE_PASSWORD_DICTIONARY_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(818); }
};
class ObSysVarDelayedInsertLimit : public ObIntSysVar
{
public:
  ObSysVarDelayedInsertLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_DELAYED_INSERT_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(819); }
};
class ObSysVarNdbVersion : public ObVarcharSysVar
{
public:
  ObSysVarNdbVersion() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_NDB_VERSION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(820); }
};
class ObSysVarAutoGenerateCerts : public ObBoolSysVar
{
public:
  ObSysVarAutoGenerateCerts() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_AUTO_GENERATE_CERTS; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(821); }
};
class ObSysVarOptimizerCostBasedTransformation : public ObIntSysVar
{
public:
  ObSysVarOptimizerCostBasedTransformation() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR__OPTIMIZER_COST_BASED_TRANSFORMATION; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(822); }
};
class ObSysVarRangeIndexDiveLimit : public ObIntSysVar
{
public:
  ObSysVarRangeIndexDiveLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_RANGE_INDEX_DIVE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(823); }
};
class ObSysVarPartitionIndexDiveLimit : public ObIntSysVar
{
public:
  ObSysVarPartitionIndexDiveLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PARTITION_INDEX_DIVE_LIMIT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(824); }
};
class ObSysVarObTableAccessPolicy : public ObEnumSysVar
{
public:
  const static char * OB_TABLE_ACCESS_POLICY_NAMES[];
public:
  ObSysVarObTableAccessPolicy() : ObEnumSysVar(OB_TABLE_ACCESS_POLICY_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_TABLE_ACCESS_POLICY; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(825); }
};
class ObSysVarPidFile : public ObVarcharSysVar
{
public:
  ObSysVarPidFile() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PID_FILE; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(826); }
};
class ObSysVarPort : public ObIntSysVar
{
public:
  ObSysVarPort() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_PORT; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(827); }
};
class ObSysVarSocket : public ObVarcharSysVar
{
public:
  ObSysVarSocket() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_SOCKET; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(828); }
};
class ObSysVarEnableOptimizerRowgoal : public ObEnumSysVar
{
public:
  const static char * ENABLE_OPTIMIZER_ROWGOAL_NAMES[];
public:
  ObSysVarEnableOptimizerRowgoal() : ObEnumSysVar(ENABLE_OPTIMIZER_ROWGOAL_NAMES, NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_ENABLE_OPTIMIZER_ROWGOAL; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(829); }
};
class ObSysVarObIvfNprobes : public ObIntSysVar
{
public:
  ObSysVarObIvfNprobes() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL) {}
  inline virtual ObSysVarClassType get_type() const { return SYS_VAR_OB_IVF_NPROBES; }
  inline virtual const common::ObObj &get_global_default_value() const { return ObSysVariables::get_default_value(830); }
};


class ObSysVarFactory
{
public:
  ObSysVarFactory(const int64_t tenant_id = OB_SERVER_TENANT_ID);
  virtual ~ObSysVarFactory();
  void destroy();
  int create_sys_var(ObSysVarClassType sys_var_id, ObBasicSysVar *&sys_var, int64_t store_idx = -1);
  int create_all_sys_vars();
  int free_sys_var(ObBasicSysVar *sys_var, int64_t sys_var_idx);
  static int create_sys_var(ObIAllocator &allocator_, ObSysVarClassType sys_var_id, ObBasicSysVar *&sys_var_ptr);
  static int calc_sys_var_store_idx(ObSysVarClassType sys_var_id, int64_t &store_idx);
  static int calc_sys_var_store_idx_by_name(const common::ObString &sys_var_name, int64_t &store_idx);
  static bool is_valid_sys_var_store_idx(int64_t store_idx);
  static ObSysVarClassType find_sys_var_id_by_name(const common::ObString &sys_var_name, bool is_from_sys_table = false); //二分查找
  static int get_sys_var_name_by_id(ObSysVarClassType sys_var_id, common::ObString &sys_var_name);
  static const common::ObString get_sys_var_name_by_id(ObSysVarClassType sys_var_id);
private:
  int try_init_store_mem();

public:
  const static int64_t MYSQL_SYS_VARS_COUNT = 99;
  const static int64_t OB_SYS_VARS_COUNT = 732;
  const static int64_t ALL_SYS_VARS_COUNT = MYSQL_SYS_VARS_COUNT + OB_SYS_VARS_COUNT;
  const static int64_t INVALID_MAX_READ_STALE_TIME = -1;

  const static int16_t OB_SPECIFIC_SYS_VAR_ID_OFFSET = 10000;
  // 表示当前OB能够使用的sys var id的最大值，正常情况下，不需要申请大于OB_MAX_SYS_VAR_ID的sys var id，
  // 如果需要申请大于OB_MAX_SYS_VAR_ID的sys var id，需要先调整ob_max_sys_var_id的值
  const static int32_t OB_MAX_SYS_VAR_ID = 20000;

private:
  static bool sys_var_name_case_cmp(const char *name1, const common::ObString &name2);
  const static char *SYS_VAR_NAMES_SORTED_BY_NAME[ALL_SYS_VARS_COUNT];
  const static ObSysVarClassType SYS_VAR_IDS_SORTED_BY_NAME[ALL_SYS_VARS_COUNT];
  const static char *SYS_VAR_NAMES_SORTED_BY_ID[ALL_SYS_VARS_COUNT];
  common::ObArenaAllocator allocator_;
  ObBasicSysVar **store_;
  ObBasicSysVar **store_buf_;
  bool all_sys_vars_created_;
};

}
}
#endif //OCEANBASE_SHARE_SYSTEM_VARIABLE_OB_SYSTEM_VARIABLE_FACTORY_