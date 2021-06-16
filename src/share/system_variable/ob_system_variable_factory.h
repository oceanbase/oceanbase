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
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "common/object/ob_object.h"
#include "share/system_variable/ob_system_variable.h"

namespace oceanbase {
namespace share {
class ObSysVarAutoIncrementIncrement : public ObIntSysVar {
public:
  ObSysVarAutoIncrementIncrement() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_AUTO_INCREMENT_INCREMENT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(0);
  }
};
class ObSysVarAutoIncrementOffset : public ObIntSysVar {
public:
  ObSysVarAutoIncrementOffset() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_AUTO_INCREMENT_OFFSET;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(1);
  }
};
class ObSysVarAutocommit : public ObBoolSysVar {
public:
  ObSysVarAutocommit() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_AUTOCOMMIT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(2);
  }
};
class ObSysVarCharacterSetClient : public ObCharsetSysVar {
public:
  ObSysVarCharacterSetClient()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CHARACTER_SET_CLIENT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(3);
  }
};
class ObSysVarCharacterSetConnection : public ObCharsetSysVar {
public:
  ObSysVarCharacterSetConnection()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CHARACTER_SET_CONNECTION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(4);
  }
};
class ObSysVarCharacterSetDatabase : public ObCharsetSysVar {
public:
  ObSysVarCharacterSetDatabase()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CHARACTER_SET_DATABASE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(5);
  }
};
class ObSysVarCharacterSetResults : public ObCharsetSysVar {
public:
  ObSysVarCharacterSetResults()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset, NULL, ObSysVarToObjFuncs::to_obj_charset,
            ObSysVarToStrFuncs::to_str_charset, ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CHARACTER_SET_RESULTS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(6);
  }
};
class ObSysVarCharacterSetServer : public ObCharsetSysVar {
public:
  ObSysVarCharacterSetServer()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CHARACTER_SET_SERVER;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(7);
  }
};
class ObSysVarCharacterSetSystem : public ObCharsetSysVar {
public:
  ObSysVarCharacterSetSystem()
      : ObCharsetSysVar(NULL, NULL, ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CHARACTER_SET_SYSTEM;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(8);
  }
};
class ObSysVarCollationConnection : public ObCharsetSysVar {
public:
  ObSysVarCollationConnection()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_collation_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_collation, ObSysVarToStrFuncs::to_str_collation,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_COLLATION_CONNECTION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(9);
  }
};
class ObSysVarCollationDatabase : public ObCharsetSysVar {
public:
  ObSysVarCollationDatabase()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_collation_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_collation, ObSysVarToStrFuncs::to_str_collation,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_COLLATION_DATABASE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(10);
  }
};
class ObSysVarCollationServer : public ObCharsetSysVar {
public:
  ObSysVarCollationServer()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_collation_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_collation, ObSysVarToStrFuncs::to_str_collation,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_COLLATION_SERVER;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(11);
  }
};
class ObSysVarInteractiveTimeout : public ObIntSysVar {
public:
  ObSysVarInteractiveTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_INTERACTIVE_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(12);
  }
};
class ObSysVarLastInsertId : public ObSessionSpecialIntSysVar {
public:
  ObSysVarLastInsertId()
      : ObSessionSpecialIntSysVar(NULL, ObSysVarSessionSpecialUpdateFuncs::update_last_insert_id, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_LAST_INSERT_ID;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(13);
  }
};
class ObSysVarMaxAllowedPacket : public ObIntSysVar {
public:
  ObSysVarMaxAllowedPacket()
      : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_max_allowed_packet, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_MAX_ALLOWED_PACKET;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(14);
  }
};
class ObSysVarSqlMode : public ObSqlModeVar {
public:
  ObSysVarSqlMode()
      : ObSqlModeVar(ObSysVarOnCheckFuncs::check_and_convert_sql_mode, ObSysVarOnUpdateFuncs::update_sql_mode,
            ObSysVarToObjFuncs::to_obj_sql_mode, ObSysVarToStrFuncs::to_str_sql_mode,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_MODE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(15);
  }
};
class ObSysVarTimeZone : public ObTimeZoneSysVar {
public:
  ObSysVarTimeZone() : ObTimeZoneSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TIME_ZONE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(16);
  }
};
class ObSysVarTxIsolation : public ObSessionSpecialVarcharSysVar {
public:
  ObSysVarTxIsolation()
      : ObSessionSpecialVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_isolation,
            ObSysVarOnUpdateFuncs::update_tx_isolation, ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation, NULL,
            NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TX_ISOLATION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(17);
  }
};
class ObSysVarVersionComment : public ObVarcharSysVar {
public:
  ObSysVarVersionComment() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VERSION_COMMENT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(18);
  }
};
class ObSysVarWaitTimeout : public ObIntSysVar {
public:
  ObSysVarWaitTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_WAIT_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(19);
  }
};
class ObSysVarBinlogRowImage : public ObEnumSysVar {
public:
  const static char* BINLOG_ROW_IMAGE_NAMES[];

public:
  ObSysVarBinlogRowImage() : ObEnumSysVar(BINLOG_ROW_IMAGE_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_BINLOG_ROW_IMAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(20);
  }
};
class ObSysVarCharacterSetFilesystem : public ObCharsetSysVar {
public:
  ObSysVarCharacterSetFilesystem()
      : ObCharsetSysVar(ObSysVarOnCheckFuncs::check_and_convert_charset_not_null, NULL,
            ObSysVarToObjFuncs::to_obj_charset, ObSysVarToStrFuncs::to_str_charset,
            ObSysVarGetMetaTypeFuncs::get_meta_type_varchar)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CHARACTER_SET_FILESYSTEM;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(21);
  }
};
class ObSysVarConnectTimeout : public ObIntSysVar {
public:
  ObSysVarConnectTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CONNECT_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(22);
  }
};
class ObSysVarDatadir : public ObVarcharSysVar {
public:
  ObSysVarDatadir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_DATADIR;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(23);
  }
};
class ObSysVarDebugSync : public ObVarcharSysVar {
public:
  ObSysVarDebugSync() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_DEBUG_SYNC;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(24);
  }
};
class ObSysVarDivPrecisionIncrement : public ObIntSysVar {
public:
  ObSysVarDivPrecisionIncrement() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_DIV_PRECISION_INCREMENT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(25);
  }
};
class ObSysVarExplicitDefaultsForTimestamp : public ObBoolSysVar {
public:
  ObSysVarExplicitDefaultsForTimestamp() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(26);
  }
};
class ObSysVarGroupConcatMaxLen : public ObIntSysVar {
public:
  ObSysVarGroupConcatMaxLen() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_GROUP_CONCAT_MAX_LEN;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(27);
  }
};
class ObSysVarIdentity : public ObSessionSpecialIntSysVar {
public:
  ObSysVarIdentity()
      : ObSessionSpecialIntSysVar(NULL, ObSysVarSessionSpecialUpdateFuncs::update_identity, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_IDENTITY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(28);
  }
};
class ObSysVarLowerCaseTableNames : public ObIntSysVar {
public:
  ObSysVarLowerCaseTableNames() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_LOWER_CASE_TABLE_NAMES;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(29);
  }
};
class ObSysVarNetReadTimeout : public ObIntSysVar {
public:
  ObSysVarNetReadTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NET_READ_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(30);
  }
};
class ObSysVarNetWriteTimeout : public ObIntSysVar {
public:
  ObSysVarNetWriteTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NET_WRITE_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(31);
  }
};
class ObSysVarReadOnly : public ObBoolSysVar {
public:
  ObSysVarReadOnly() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_READ_ONLY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(32);
  }
};
class ObSysVarSqlAutoIsNull : public ObBoolSysVar {
public:
  ObSysVarSqlAutoIsNull() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_AUTO_IS_NULL;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(33);
  }
};
class ObSysVarSqlSelectLimit : public ObIntSysVar {
public:
  ObSysVarSqlSelectLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_SELECT_LIMIT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(34);
  }
};
class ObSysVarTimestamp : public ObNumericSysVar {
public:
  ObSysVarTimestamp()
      : ObNumericSysVar(ObSysVarOnCheckFuncs::check_and_convert_max_min_timestamp, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TIMESTAMP;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(35);
  }
};
class ObSysVarTxReadOnly : public ObSessionSpecialBoolSysVar {
public:
  ObSysVarTxReadOnly()
      : ObSessionSpecialBoolSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_read_only,
            ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope, ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only,
            NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TX_READ_ONLY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(36);
  }
};
class ObSysVarVersion : public ObVarcharSysVar {
public:
  ObSysVarVersion() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VERSION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(37);
  }
};
class ObSysVarSqlWarnings : public ObBoolSysVar {
public:
  ObSysVarSqlWarnings() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_WARNINGS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(38);
  }
};
class ObSysVarMaxUserConnections : public ObIntSysVar {
public:
  ObSysVarMaxUserConnections()
      : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_max_user_connections, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_MAX_USER_CONNECTIONS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(39);
  }
};
class ObSysVarInitConnect : public ObVarcharSysVar {
public:
  ObSysVarInitConnect() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_INIT_CONNECT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(40);
  }
};
class ObSysVarLicense : public ObVarcharSysVar {
public:
  ObSysVarLicense() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_LICENSE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(41);
  }
};
class ObSysVarNetBufferLength : public ObIntSysVar {
public:
  ObSysVarNetBufferLength()
      : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_net_buffer_length, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NET_BUFFER_LENGTH;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(42);
  }
};
class ObSysVarSystemTimeZone : public ObVarcharSysVar {
public:
  ObSysVarSystemTimeZone() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SYSTEM_TIME_ZONE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(43);
  }
};
class ObSysVarQueryCacheSize : public ObIntSysVar {
public:
  ObSysVarQueryCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_QUERY_CACHE_SIZE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(44);
  }
};
class ObSysVarQueryCacheType : public ObEnumSysVar {
public:
  const static char* QUERY_CACHE_TYPE_NAMES[];

public:
  ObSysVarQueryCacheType() : ObEnumSysVar(QUERY_CACHE_TYPE_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_QUERY_CACHE_TYPE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(45);
  }
};
class ObSysVarSqlQuoteShowCreate : public ObBoolSysVar {
public:
  ObSysVarSqlQuoteShowCreate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_QUOTE_SHOW_CREATE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(46);
  }
};
class ObSysVarMaxSpRecursionDepth : public ObIntSysVar {
public:
  ObSysVarMaxSpRecursionDepth() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_MAX_SP_RECURSION_DEPTH;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(47);
  }
};
class ObSysVarSqlSafeUpdates : public ObBoolSysVar {
public:
  ObSysVarSqlSafeUpdates() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_SAFE_UPDATES;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(48);
  }
};
class ObSysVarConcurrentInsert : public ObVarcharSysVar {
public:
  ObSysVarConcurrentInsert() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_CONCURRENT_INSERT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(49);
  }
};
class ObSysVarDefaultAuthenticationPlugin : public ObVarcharSysVar {
public:
  ObSysVarDefaultAuthenticationPlugin() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(50);
  }
};
class ObSysVarDisabledStorageEngines : public ObVarcharSysVar {
public:
  ObSysVarDisabledStorageEngines() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_DISABLED_STORAGE_ENGINES;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(51);
  }
};
class ObSysVarErrorCount : public ObIntSysVar {
public:
  ObSysVarErrorCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_ERROR_COUNT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(52);
  }
};
class ObSysVarGeneralLog : public ObBoolSysVar {
public:
  ObSysVarGeneralLog() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_GENERAL_LOG;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(53);
  }
};
class ObSysVarHaveOpenssl : public ObVarcharSysVar {
public:
  ObSysVarHaveOpenssl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_HAVE_OPENSSL;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(54);
  }
};
class ObSysVarHaveProfiling : public ObVarcharSysVar {
public:
  ObSysVarHaveProfiling() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_HAVE_PROFILING;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(55);
  }
};
class ObSysVarHaveSsl : public ObVarcharSysVar {
public:
  ObSysVarHaveSsl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_HAVE_SSL;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(56);
  }
};
class ObSysVarHostname : public ObVarcharSysVar {
public:
  ObSysVarHostname() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_HOSTNAME;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(57);
  }
};
class ObSysVarLcMessages : public ObVarcharSysVar {
public:
  ObSysVarLcMessages() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_LC_MESSAGES;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(58);
  }
};
class ObSysVarLocalInfile : public ObBoolSysVar {
public:
  ObSysVarLocalInfile() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_LOCAL_INFILE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(59);
  }
};
class ObSysVarLockWaitTimeout : public ObIntSysVar {
public:
  ObSysVarLockWaitTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_LOCK_WAIT_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(60);
  }
};
class ObSysVarLongQueryTime : public ObNumericSysVar {
public:
  ObSysVarLongQueryTime() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_LONG_QUERY_TIME;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(61);
  }
};
class ObSysVarMaxConnections : public ObIntSysVar {
public:
  ObSysVarMaxConnections() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_MAX_CONNECTIONS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(62);
  }
};
class ObSysVarMaxExecutionTime : public ObIntSysVar {
public:
  ObSysVarMaxExecutionTime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_MAX_EXECUTION_TIME;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(63);
  }
};
class ObSysVarProtocolVersion : public ObIntSysVar {
public:
  ObSysVarProtocolVersion() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_PROTOCOL_VERSION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(64);
  }
};
class ObSysVarServerId : public ObIntSysVar {
public:
  ObSysVarServerId() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SERVER_ID;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(65);
  }
};
class ObSysVarSslCa : public ObVarcharSysVar {
public:
  ObSysVarSslCa() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SSL_CA;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(66);
  }
};
class ObSysVarSslCapath : public ObVarcharSysVar {
public:
  ObSysVarSslCapath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SSL_CAPATH;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(67);
  }
};
class ObSysVarSslCert : public ObVarcharSysVar {
public:
  ObSysVarSslCert() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SSL_CERT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(68);
  }
};
class ObSysVarSslCipher : public ObVarcharSysVar {
public:
  ObSysVarSslCipher() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SSL_CIPHER;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(69);
  }
};
class ObSysVarSslCrl : public ObVarcharSysVar {
public:
  ObSysVarSslCrl() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SSL_CRL;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(70);
  }
};
class ObSysVarSslCrlpath : public ObVarcharSysVar {
public:
  ObSysVarSslCrlpath() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SSL_CRLPATH;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(71);
  }
};
class ObSysVarSslKey : public ObVarcharSysVar {
public:
  ObSysVarSslKey() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SSL_KEY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(72);
  }
};
class ObSysVarTimeFormat : public ObVarcharSysVar {
public:
  ObSysVarTimeFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TIME_FORMAT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(73);
  }
};
class ObSysVarTlsVersion : public ObVarcharSysVar {
public:
  ObSysVarTlsVersion() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TLS_VERSION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(74);
  }
};
class ObSysVarTmpTableSize : public ObIntSysVar {
public:
  ObSysVarTmpTableSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TMP_TABLE_SIZE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(75);
  }
};
class ObSysVarTmpdir : public ObVarcharSysVar {
public:
  ObSysVarTmpdir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TMPDIR;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(76);
  }
};
class ObSysVarUniqueChecks : public ObBoolSysVar {
public:
  ObSysVarUniqueChecks() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_UNIQUE_CHECKS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(77);
  }
};
class ObSysVarVersionCompileMachine : public ObVarcharSysVar {
public:
  ObSysVarVersionCompileMachine() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VERSION_COMPILE_MACHINE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(78);
  }
};
class ObSysVarVersionCompileOs : public ObVarcharSysVar {
public:
  ObSysVarVersionCompileOs() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VERSION_COMPILE_OS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(79);
  }
};
class ObSysVarWarningCount : public ObIntSysVar {
public:
  ObSysVarWarningCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_WARNING_COUNT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(80);
  }
};
class ObSysVarObDefaultReplicaNum : public ObIntSysVar {
public:
  ObSysVarObDefaultReplicaNum() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_DEFAULT_REPLICA_NUM;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(81);
  }
};
class ObSysVarObIntermResultMemLimit : public ObIntSysVar {
public:
  ObSysVarObIntermResultMemLimit() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(82);
  }
};
class ObSysVarObProxyPartitionHit : public ObBoolSysVar {
public:
  ObSysVarObProxyPartitionHit() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PROXY_PARTITION_HIT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(83);
  }
};
class ObSysVarObLogLevel : public ObVarcharSysVar {
public:
  ObSysVarObLogLevel() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_LOG_LEVEL;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(84);
  }
};
class ObSysVarObMaxParallelDegree : public ObIntSysVar {
public:
  ObSysVarObMaxParallelDegree() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_MAX_PARALLEL_DEGREE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(85);
  }
};
class ObSysVarObQueryTimeout : public ObIntSysVar {
public:
  ObSysVarObQueryTimeout()
      : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_QUERY_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(86);
  }
};
class ObSysVarObReadConsistency : public ObEnumSysVar {
public:
  const static char* OB_READ_CONSISTENCY_NAMES[];

public:
  ObSysVarObReadConsistency() : ObEnumSysVar(OB_READ_CONSISTENCY_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_READ_CONSISTENCY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(87);
  }
};
class ObSysVarObEnableTransformation : public ObBoolSysVar {
public:
  ObSysVarObEnableTransformation() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_TRANSFORMATION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(88);
  }
};
class ObSysVarObTrxTimeout : public ObIntSysVar {
public:
  ObSysVarObTrxTimeout()
      : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_TRX_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(89);
  }
};
class ObSysVarObEnablePlanCache : public ObBoolSysVar {
public:
  ObSysVarObEnablePlanCache() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_PLAN_CACHE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(90);
  }
};
class ObSysVarObEnableIndexDirectSelect : public ObBoolSysVar {
public:
  ObSysVarObEnableIndexDirectSelect() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(91);
  }
};
class ObSysVarObProxySetTrxExecuted : public ObBoolSysVar {
public:
  ObSysVarObProxySetTrxExecuted() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PROXY_SET_TRX_EXECUTED;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(92);
  }
};
class ObSysVarObEnableAggregationPushdown : public ObBoolSysVar {
public:
  ObSysVarObEnableAggregationPushdown() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(93);
  }
};
class ObSysVarObLastSchemaVersion : public ObIntSysVar {
public:
  ObSysVarObLastSchemaVersion() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_LAST_SCHEMA_VERSION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(94);
  }
};
class ObSysVarObGlobalDebugSync : public ObVarcharSysVar {
public:
  ObSysVarObGlobalDebugSync() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_GLOBAL_DEBUG_SYNC;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(95);
  }
};
class ObSysVarObProxyGlobalVariablesVersion : public ObIntSysVar {
public:
  ObSysVarObProxyGlobalVariablesVersion() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(96);
  }
};
class ObSysVarObEnableTraceLog : public ObBoolSysVar {
public:
  ObSysVarObEnableTraceLog() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_TRACE_LOG;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(97);
  }
};
class ObSysVarObEnableHashGroupBy : public ObBoolSysVar {
public:
  ObSysVarObEnableHashGroupBy() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_HASH_GROUP_BY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(98);
  }
};
class ObSysVarObEnableBlkNestedloopJoin : public ObBoolSysVar {
public:
  ObSysVarObEnableBlkNestedloopJoin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_BLK_NESTEDLOOP_JOIN;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(99);
  }
};
class ObSysVarObBnlJoinCacheSize : public ObIntSysVar {
public:
  ObSysVarObBnlJoinCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_BNL_JOIN_CACHE_SIZE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(100);
  }
};
class ObSysVarObProxyUserPrivilege : public ObIntSysVar {
public:
  ObSysVarObProxyUserPrivilege() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PROXY_USER_PRIVILEGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(101);
  }
};
class ObSysVarObOrgClusterId : public ObStrictRangeIntSysVar {
public:
  ObSysVarObOrgClusterId()
      : ObStrictRangeIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_ob_org_cluster_id, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ORG_CLUSTER_ID;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(102);
  }
};
class ObSysVarObPlanCachePercentage : public ObIntSysVar {
public:
  ObSysVarObPlanCachePercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PLAN_CACHE_PERCENTAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(103);
  }
};
class ObSysVarObPlanCacheEvictHighPercentage : public ObIntSysVar {
public:
  ObSysVarObPlanCacheEvictHighPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(104);
  }
};
class ObSysVarObPlanCacheEvictLowPercentage : public ObIntSysVar {
public:
  ObSysVarObPlanCacheEvictLowPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(105);
  }
};
class ObSysVarRecyclebin : public ObBoolSysVar {
public:
  ObSysVarRecyclebin() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_RECYCLEBIN;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(106);
  }
};
class ObSysVarObCapabilityFlag : public ObIntSysVar {
public:
  ObSysVarObCapabilityFlag() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_CAPABILITY_FLAG;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(107);
  }
};
class ObSysVarObStmtParallelDegree : public ObIntSysVar {
public:
  ObSysVarObStmtParallelDegree() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_STMT_PARALLEL_DEGREE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(108);
  }
};
class ObSysVarIsResultAccurate : public ObBoolSysVar {
public:
  ObSysVarIsResultAccurate() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_IS_RESULT_ACCURATE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(109);
  }
};
class ObSysVarErrorOnOverlapTime : public ObBoolSysVar {
public:
  ObSysVarErrorOnOverlapTime() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_ERROR_ON_OVERLAP_TIME;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(110);
  }
};
class ObSysVarObCompatibilityMode : public ObEnumSysVar {
public:
  const static char* OB_COMPATIBILITY_MODE_NAMES[];

public:
  ObSysVarObCompatibilityMode() : ObEnumSysVar(OB_COMPATIBILITY_MODE_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_COMPATIBILITY_MODE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(111);
  }
};
class ObSysVarObCreateTableStrictMode : public ObBoolSysVar {
public:
  ObSysVarObCreateTableStrictMode() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_CREATE_TABLE_STRICT_MODE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(112);
  }
};
class ObSysVarObSqlWorkAreaPercentage : public ObIntSysVar {
public:
  ObSysVarObSqlWorkAreaPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(113);
  }
};
class ObSysVarObSafeWeakReadSnapshot : public ObIntSysVar {
public:
  ObSysVarObSafeWeakReadSnapshot()
      : ObIntSysVar(NULL, ObSysVarOnUpdateFuncs::update_safe_weak_read_snapshot, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(114);
  }
};
class ObSysVarObRoutePolicy : public ObEnumSysVar {
public:
  const static char* OB_ROUTE_POLICY_NAMES[];

public:
  ObSysVarObRoutePolicy() : ObEnumSysVar(OB_ROUTE_POLICY_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ROUTE_POLICY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(115);
  }
};
class ObSysVarObEnableTransmissionChecksum : public ObBoolSysVar {
public:
  ObSysVarObEnableTransmissionChecksum() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(116);
  }
};
class ObSysVarForeignKeyChecks : public ObBoolSysVar {
public:
  ObSysVarForeignKeyChecks() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_FOREIGN_KEY_CHECKS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(117);
  }
};
class ObSysVarObStatementTraceId : public ObVarcharSysVar {
public:
  ObSysVarObStatementTraceId() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_STATEMENT_TRACE_ID;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(118);
  }
};
class ObSysVarObEnableTruncateFlashback : public ObBoolSysVar {
public:
  ObSysVarObEnableTruncateFlashback() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(119);
  }
};
class ObSysVarObTcpInvitedNodes : public ObVarcharSysVar {
public:
  ObSysVarObTcpInvitedNodes() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_TCP_INVITED_NODES;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(120);
  }
};
class ObSysVarSqlThrottleCurrentPriority : public ObIntSysVar {
public:
  ObSysVarSqlThrottleCurrentPriority() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(121);
  }
};
class ObSysVarSqlThrottlePriority : public ObIntSysVar {
public:
  ObSysVarSqlThrottlePriority() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_THROTTLE_PRIORITY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(122);
  }
};
class ObSysVarSqlThrottleRt : public ObNumericSysVar {
public:
  ObSysVarSqlThrottleRt() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_THROTTLE_RT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(123);
  }
};
class ObSysVarSqlThrottleCpu : public ObNumericSysVar {
public:
  ObSysVarSqlThrottleCpu() : ObNumericSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_THROTTLE_CPU;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(124);
  }
};
class ObSysVarSqlThrottleIo : public ObIntSysVar {
public:
  ObSysVarSqlThrottleIo() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_THROTTLE_IO;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(125);
  }
};
class ObSysVarSqlThrottleNetwork : public ObNumericSysVar {
public:
  ObSysVarSqlThrottleNetwork()
      : ObNumericSysVar(ObSysVarOnCheckFuncs::check_and_convert_sql_throttle_queue_time, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_THROTTLE_NETWORK;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(126);
  }
};
class ObSysVarSqlThrottleLogicalReads : public ObIntSysVar {
public:
  ObSysVarSqlThrottleLogicalReads() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SQL_THROTTLE_LOGICAL_READS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(127);
  }
};
class ObSysVarAutoIncrementCacheSize : public ObIntSysVar {
public:
  ObSysVarAutoIncrementCacheSize() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_AUTO_INCREMENT_CACHE_SIZE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(128);
  }
};
class ObSysVarObEnableJit : public ObEnumSysVar {
public:
  const static char* OB_ENABLE_JIT_NAMES[];

public:
  ObSysVarObEnableJit() : ObEnumSysVar(OB_ENABLE_JIT_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_JIT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(129);
  }
};
class ObSysVarObTempTablespaceSizePercentage : public ObIntSysVar {
public:
  ObSysVarObTempTablespaceSizePercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_TEMP_TABLESPACE_SIZE_PERCENTAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(130);
  }
};
class ObSysVarOptimizerAdaptiveCursorSharing : public ObBoolSysVar {
public:
  ObSysVarOptimizerAdaptiveCursorSharing() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__OPTIMIZER_ADAPTIVE_CURSOR_SHARING;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(131);
  }
};
class ObSysVarObTimestampService : public ObEnumSysVar {
public:
  const static char* OB_TIMESTAMP_SERVICE_NAMES[];

public:
  ObSysVarObTimestampService()
      : ObEnumSysVar(OB_TIMESTAMP_SERVICE_NAMES, ObSysVarOnCheckFuncs::check_and_convert_timestamp_service, NULL, NULL,
            NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_TIMESTAMP_SERVICE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(132);
  }
};
class ObSysVarPluginDir : public ObVarcharSysVar {
public:
  ObSysVarPluginDir() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_PLUGIN_DIR;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(133);
  }
};
class ObSysVarUndoRetention : public ObIntSysVar {
public:
  ObSysVarUndoRetention() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_UNDO_RETENTION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(134);
  }
};
class ObSysVarObUseParallelExecution : public ObBoolSysVar {
public:
  ObSysVarObUseParallelExecution() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__OB_USE_PARALLEL_EXECUTION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(135);
  }
};
class ObSysVarObSqlAuditPercentage : public ObIntSysVar {
public:
  ObSysVarObSqlAuditPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_SQL_AUDIT_PERCENTAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(136);
  }
};
class ObSysVarObEnableSqlAudit : public ObBoolSysVar {
public:
  ObSysVarObEnableSqlAudit() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_SQL_AUDIT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(137);
  }
};
class ObSysVarOptimizerUseSqlPlanBaselines : public ObBoolSysVar {
public:
  ObSysVarOptimizerUseSqlPlanBaselines() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(138);
  }
};
class ObSysVarOptimizerCaptureSqlPlanBaselines : public ObBoolSysVar {
public:
  ObSysVarOptimizerCaptureSqlPlanBaselines() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(139);
  }
};
class ObSysVarParallelMaxServers : public ObIntSysVar {
public:
  ObSysVarParallelMaxServers() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_PARALLEL_MAX_SERVERS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(140);
  }
};
class ObSysVarParallelServersTarget : public ObIntSysVar {
public:
  ObSysVarParallelServersTarget() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_PARALLEL_SERVERS_TARGET;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(141);
  }
};
class ObSysVarObEarlyLockRelease : public ObBoolSysVar {
public:
  ObSysVarObEarlyLockRelease() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_EARLY_LOCK_RELEASE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(142);
  }
};
class ObSysVarObTrxIdleTimeout : public ObIntSysVar {
public:
  ObSysVarObTrxIdleTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_TRX_IDLE_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(143);
  }
};
class ObSysVarBlockEncryptionMode : public ObEnumSysVar {
public:
  const static char* BLOCK_ENCRYPTION_MODE_NAMES[];

public:
  ObSysVarBlockEncryptionMode() : ObEnumSysVar(BLOCK_ENCRYPTION_MODE_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_BLOCK_ENCRYPTION_MODE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(144);
  }
};
class ObSysVarNlsDateFormat : public ObVarcharSysVar {
public:
  ObSysVarNlsDateFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_DATE_FORMAT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(145);
  }
};
class ObSysVarNlsTimestampFormat : public ObVarcharSysVar {
public:
  ObSysVarNlsTimestampFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_TIMESTAMP_FORMAT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(146);
  }
};
class ObSysVarNlsTimestampTzFormat : public ObVarcharSysVar {
public:
  ObSysVarNlsTimestampTzFormat() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(147);
  }
};
class ObSysVarObReservedMetaMemoryPercentage : public ObIntSysVar {
public:
  ObSysVarObReservedMetaMemoryPercentage() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(148);
  }
};
class ObSysVarObCheckSysVariable : public ObBoolSysVar {
public:
  ObSysVarObCheckSysVariable() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_CHECK_SYS_VARIABLE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(149);
  }
};
class ObSysVarNlsLanguage : public ObVarcharSysVar {
public:
  ObSysVarNlsLanguage() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_LANGUAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(150);
  }
};
class ObSysVarNlsTerritory : public ObVarcharSysVar {
public:
  ObSysVarNlsTerritory() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_TERRITORY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(151);
  }
};
class ObSysVarNlsSort : public ObVarcharSysVar {
public:
  ObSysVarNlsSort() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_SORT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(152);
  }
};
class ObSysVarNlsComp : public ObVarcharSysVar {
public:
  ObSysVarNlsComp() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_COMP;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(153);
  }
};
class ObSysVarNlsCharacterset : public ObVarcharSysVar {
public:
  ObSysVarNlsCharacterset() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_CHARACTERSET;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(154);
  }
};
class ObSysVarNlsNcharCharacterset : public ObVarcharSysVar {
public:
  ObSysVarNlsNcharCharacterset() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_NCHAR_CHARACTERSET;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(155);
  }
};
class ObSysVarNlsDateLanguage : public ObVarcharSysVar {
public:
  ObSysVarNlsDateLanguage() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_DATE_LANGUAGE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(156);
  }
};
class ObSysVarNlsLengthSemantics : public ObVarcharSysVar {
public:
  ObSysVarNlsLengthSemantics() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_LENGTH_SEMANTICS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(157);
  }
};
class ObSysVarNlsNcharConvExcp : public ObVarcharSysVar {
public:
  ObSysVarNlsNcharConvExcp() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_NCHAR_CONV_EXCP;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(158);
  }
};
class ObSysVarNlsCalendar : public ObVarcharSysVar {
public:
  ObSysVarNlsCalendar() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_CALENDAR;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(159);
  }
};
class ObSysVarNlsNumericCharacters : public ObVarcharSysVar {
public:
  ObSysVarNlsNumericCharacters() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_NUMERIC_CHARACTERS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(160);
  }
};
class ObSysVarNljBatchingEnabled : public ObBoolSysVar {
public:
  ObSysVarNljBatchingEnabled() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__NLJ_BATCHING_ENABLED;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(161);
  }
};
class ObSysVarTracefileIdentifier : public ObVarcharSysVar {
public:
  ObSysVarTracefileIdentifier() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TRACEFILE_IDENTIFIER;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(162);
  }
};
class ObSysVarGroupbyNopushdownCutRatio : public ObIntSysVar {
public:
  ObSysVarGroupbyNopushdownCutRatio() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(163);
  }
};
class ObSysVarPxBroadcastFudgeFactor : public ObIntSysVar {
public:
  ObSysVarPxBroadcastFudgeFactor() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__PX_BROADCAST_FUDGE_FACTOR;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(164);
  }
};
class ObSysVarPrimaryZoneEntityCount : public ObIntSysVar {
public:
  ObSysVarPrimaryZoneEntityCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__PRIMARY_ZONE_ENTITY_COUNT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(165);
  }
};
class ObSysVarTransactionIsolation : public ObSessionSpecialVarcharSysVar {
public:
  ObSysVarTransactionIsolation()
      : ObSessionSpecialVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_isolation,
            ObSysVarOnUpdateFuncs::update_tx_isolation, ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation, NULL,
            NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TRANSACTION_ISOLATION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(166);
  }
};
class ObSysVarObTrxLockTimeout : public ObIntSysVar {
public:
  ObSysVarObTrxLockTimeout() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_TRX_LOCK_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(167);
  }
};
class ObSysVarValidatePasswordCheckUserName : public ObEnumSysVar {
public:
  const static char* VALIDATE_PASSWORD_CHECK_USER_NAME_NAMES[];

public:
  ObSysVarValidatePasswordCheckUserName()
      : ObEnumSysVar(VALIDATE_PASSWORD_CHECK_USER_NAME_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(168);
  }
};
class ObSysVarValidatePasswordLength : public ObIntSysVar {
public:
  ObSysVarValidatePasswordLength() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VALIDATE_PASSWORD_LENGTH;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(169);
  }
};
class ObSysVarValidatePasswordMixedCaseCount : public ObIntSysVar {
public:
  ObSysVarValidatePasswordMixedCaseCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(170);
  }
};
class ObSysVarValidatePasswordNumberCount : public ObIntSysVar {
public:
  ObSysVarValidatePasswordNumberCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(171);
  }
};
class ObSysVarValidatePasswordPolicy : public ObEnumSysVar {
public:
  const static char* VALIDATE_PASSWORD_POLICY_NAMES[];

public:
  ObSysVarValidatePasswordPolicy() : ObEnumSysVar(VALIDATE_PASSWORD_POLICY_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VALIDATE_PASSWORD_POLICY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(172);
  }
};
class ObSysVarValidatePasswordSpecialCharCount : public ObIntSysVar {
public:
  ObSysVarValidatePasswordSpecialCharCount() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(173);
  }
};
class ObSysVarDefaultPasswordLifetime : public ObIntSysVar {
public:
  ObSysVarDefaultPasswordLifetime() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_DEFAULT_PASSWORD_LIFETIME;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(174);
  }
};
class ObSysVarObTraceInfo : public ObVarcharSysVar {
public:
  ObSysVarObTraceInfo() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_TRACE_INFO;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(175);
  }
};
class ObSysVarObEnableBatchedMultiStatement : public ObBoolSysVar {
public:
  ObSysVarObEnableBatchedMultiStatement() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_ENABLE_BATCHED_MULTI_STATEMENT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(176);
  }
};
class ObSysVarPxPartitionScanThreshold : public ObIntSysVar {
public:
  ObSysVarPxPartitionScanThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__PX_PARTITION_SCAN_THRESHOLD;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(177);
  }
};
class ObSysVarObPxBcastOptimization : public ObEnumSysVar {
public:
  const static char* _OB_PX_BCAST_OPTIMIZATION_NAMES[];

public:
  ObSysVarObPxBcastOptimization() : ObEnumSysVar(_OB_PX_BCAST_OPTIMIZATION_NAMES, NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__OB_PX_BCAST_OPTIMIZATION;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(178);
  }
};
class ObSysVarObPxSlaveMappingThreshold : public ObIntSysVar {
public:
  ObSysVarObPxSlaveMappingThreshold() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(179);
  }
};
class ObSysVarEnableParallelDml : public ObBoolSysVar {
public:
  ObSysVarEnableParallelDml() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__ENABLE_PARALLEL_DML;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(180);
  }
};
class ObSysVarPxMinGranulesPerSlave : public ObIntSysVar {
public:
  ObSysVarPxMinGranulesPerSlave() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__PX_MIN_GRANULES_PER_SLAVE;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(181);
  }
};
class ObSysVarSecureFilePriv : public ObVarcharSysVar {
public:
  ObSysVarSecureFilePriv() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_SECURE_FILE_PRIV;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(182);
  }
};
class ObSysVarPlsqlWarnings : public ObVarcharSysVar {
public:
  ObSysVarPlsqlWarnings()
      : ObVarcharSysVar(ObSysVarOnCheckFuncs::check_and_convert_plsql_warnings, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_PLSQL_WARNINGS;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(183);
  }
};
class ObSysVarEnableParallelQuery : public ObBoolSysVar {
public:
  ObSysVarEnableParallelQuery() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__ENABLE_PARALLEL_QUERY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(184);
  }
};
class ObSysVarForceParallelQueryDop : public ObIntSysVar {
public:
  ObSysVarForceParallelQueryDop() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__FORCE_PARALLEL_QUERY_DOP;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(185);
  }
};
class ObSysVarForceParallelDmlDop : public ObIntSysVar {
public:
  ObSysVarForceParallelDmlDop() : ObIntSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR__FORCE_PARALLEL_DML_DOP;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(186);
  }
};
class ObSysVarObPlBlockTimeout : public ObIntSysVar {
public:
  ObSysVarObPlBlockTimeout()
      : ObIntSysVar(ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_OB_PL_BLOCK_TIMEOUT;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(187);
  }
};
class ObSysVarTransactionReadOnly : public ObSessionSpecialBoolSysVar {
public:
  ObSysVarTransactionReadOnly()
      : ObSessionSpecialBoolSysVar(ObSysVarOnCheckFuncs::check_and_convert_tx_read_only,
            ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope, ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only,
            NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_TRANSACTION_READ_ONLY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(188);
  }
};
class ObSysVarResourceManagerPlan : public ObVarcharSysVar {
public:
  ObSysVarResourceManagerPlan() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_RESOURCE_MANAGER_PLAN;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(189);
  }
};
class ObSysVarPerformanceSchema : public ObBoolSysVar {
public:
  ObSysVarPerformanceSchema() : ObBoolSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_PERFORMANCE_SCHEMA;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(190);
  }
};
class ObSysVarNlsCurrency : public ObVarcharSysVar {
public:
  ObSysVarNlsCurrency() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_CURRENCY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(191);
  }
};
class ObSysVarNlsIsoCurrency : public ObVarcharSysVar {
public:
  ObSysVarNlsIsoCurrency() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_ISO_CURRENCY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(192);
  }
};
class ObSysVarNlsDualCurrency : public ObVarcharSysVar {
public:
  ObSysVarNlsDualCurrency() : ObVarcharSysVar(NULL, NULL, NULL, NULL, NULL)
  {}
  inline virtual ObSysVarClassType get_type() const
  {
    return SYS_VAR_NLS_DUAL_CURRENCY;
  }
  inline virtual const common::ObObj& get_global_default_value() const
  {
    return ObSysVariables::get_default_value(193);
  }
};

class ObSysVarFactory {
public:
  ObSysVarFactory();
  virtual ~ObSysVarFactory();
  void destroy();
  int create_sys_var(ObSysVarClassType sys_var_id, ObBasicSysVar*& sys_var);
  int free_sys_var(ObBasicSysVar* sys_var, int64_t sys_var_idx);
  static int calc_sys_var_store_idx(ObSysVarClassType sys_var_id, int64_t& store_idx);
  static int calc_sys_var_store_idx_by_name(const common::ObString& sys_var_name, int64_t& store_idx);
  static bool is_valid_sys_var_store_idx(int64_t store_idx);
  static ObSysVarClassType find_sys_var_id_by_name(
      const common::ObString& sys_var_name, bool is_from_sys_table = false);
  static int get_sys_var_name_by_id(ObSysVarClassType sys_var_id, common::ObString& sys_var_name);
  static const common::ObString get_sys_var_name_by_id(ObSysVarClassType sys_var_id);

  const static int64_t MYSQL_SYS_VARS_COUNT = 81;
  const static int64_t OB_SYS_VARS_COUNT = 113;
  const static int64_t ALL_SYS_VARS_COUNT = MYSQL_SYS_VARS_COUNT + OB_SYS_VARS_COUNT;

  const static int16_t OB_SPECIFIC_SYS_VAR_ID_OFFSET = 10000;
  // increase OB_MAX_SYS_VAR_ID if you need a sys_var_id greater than it.
  const static int32_t OB_MAX_SYS_VAR_ID = 20000;

private:
  static bool sys_var_name_case_cmp(const char* name1, const common::ObString& name2);
  const static char* SYS_VAR_NAMES_SORTED_BY_NAME[ALL_SYS_VARS_COUNT];
  const static ObSysVarClassType SYS_VAR_IDS_SORTED_BY_NAME[ALL_SYS_VARS_COUNT];
  const static char* SYS_VAR_NAMES_SORTED_BY_ID[ALL_SYS_VARS_COUNT];
  common::ObArenaAllocator allocator_;
  ObBasicSysVar* store_[ALL_SYS_VARS_COUNT];
  ObBasicSysVar* store_buf_[ALL_SYS_VARS_COUNT];
};

}  // namespace share
}  // namespace oceanbase
#endif  // OCEANBASE_SHARE_SYSTEM_VARIABLE_OB_SYSTEM_VARIABLE_FACTORY_