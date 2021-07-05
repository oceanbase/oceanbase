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

#include "ob_errno.h"
#include "ob_define.h"
using namespace oceanbase::common;

static const char* ERROR_NAME[OB_MAX_ERROR_CODE];
static int MYSQL_ERRNO[OB_MAX_ERROR_CODE];
static const char* SQLSTATE[OB_MAX_ERROR_CODE];
static const char* STR_ERROR[OB_MAX_ERROR_CODE];
static const char* STR_USER_ERROR[OB_MAX_ERROR_CODE];

static int ORACLE_ERRNO[OB_MAX_ERROR_CODE];
static const char* ORACLE_STR_ERROR[OB_MAX_ERROR_CODE];
static const char* ORACLE_STR_USER_ERROR[OB_MAX_ERROR_CODE];

static struct ObStrErrorInit {
  ObStrErrorInit()
  {
    memset(ERROR_NAME, 0, sizeof(ERROR_NAME));
    memset(MYSQL_ERRNO, 0, sizeof(MYSQL_ERRNO));
    memset(SQLSTATE, 0, sizeof(SQLSTATE));
    memset(STR_ERROR, 0, sizeof(STR_ERROR));
    memset(STR_USER_ERROR, 0, sizeof(STR_USER_ERROR));

    memset(ORACLE_ERRNO, 0, sizeof(ORACLE_ERRNO));
    memset(ORACLE_STR_ERROR, 0, sizeof(ORACLE_STR_ERROR));
    memset(ORACLE_STR_USER_ERROR, 0, sizeof(ORACLE_STR_USER_ERROR));

    ERROR_NAME[-OB_ERROR] = "OB_ERROR";
    MYSQL_ERRNO[-OB_ERROR] = -1;
    SQLSTATE[-OB_ERROR] = "HY000";
    STR_ERROR[-OB_ERROR] = "Common error";
    STR_USER_ERROR[-OB_ERROR] = "Common error";
    ORACLE_ERRNO[-OB_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERROR] = "ORA-00600: internal error code, arguments: -4000, Common error";
    ORACLE_STR_USER_ERROR[-OB_ERROR] = "ORA-00600: internal error code, arguments: -4000, Common error";
    ERROR_NAME[-OB_OBJ_TYPE_ERROR] = "OB_OBJ_TYPE_ERROR";
    MYSQL_ERRNO[-OB_OBJ_TYPE_ERROR] = -1;
    SQLSTATE[-OB_OBJ_TYPE_ERROR] = "HY004";
    STR_ERROR[-OB_OBJ_TYPE_ERROR] = "Object type error";
    STR_USER_ERROR[-OB_OBJ_TYPE_ERROR] = "Object type error";
    ORACLE_ERRNO[-OB_OBJ_TYPE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_OBJ_TYPE_ERROR] = "ORA-00600: internal error code, arguments: -4001, Object type error";
    ORACLE_STR_USER_ERROR[-OB_OBJ_TYPE_ERROR] = "ORA-00600: internal error code, arguments: -4001, Object type error";
    ERROR_NAME[-OB_INVALID_ARGUMENT] = "OB_INVALID_ARGUMENT";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT] = ER_WRONG_ARGUMENTS;
    SQLSTATE[-OB_INVALID_ARGUMENT] = "HY000";
    STR_ERROR[-OB_INVALID_ARGUMENT] = "Invalid argument";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT] = "Incorrect arguments to %s";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT] = "ORA-00600: internal error code, arguments: -4002, Invalid argument";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT] =
        "ORA-00600: internal error code, arguments: -4002, Incorrect arguments to %s";
    ERROR_NAME[-OB_ARRAY_OUT_OF_RANGE] = "OB_ARRAY_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ARRAY_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_ARRAY_OUT_OF_RANGE] = "42000";
    STR_ERROR[-OB_ARRAY_OUT_OF_RANGE] = "Array index out of range";
    STR_USER_ERROR[-OB_ARRAY_OUT_OF_RANGE] = "Array index out of range";
    ORACLE_ERRNO[-OB_ARRAY_OUT_OF_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_ARRAY_OUT_OF_RANGE] =
        "ORA-00600: internal error code, arguments: -4003, Array index out of range";
    ORACLE_STR_USER_ERROR[-OB_ARRAY_OUT_OF_RANGE] =
        "ORA-00600: internal error code, arguments: -4003, Array index out of range";
    ERROR_NAME[-OB_SERVER_LISTEN_ERROR] = "OB_SERVER_LISTEN_ERROR";
    MYSQL_ERRNO[-OB_SERVER_LISTEN_ERROR] = -1;
    SQLSTATE[-OB_SERVER_LISTEN_ERROR] = "08S01";
    STR_ERROR[-OB_SERVER_LISTEN_ERROR] = "Failed to listen to the port";
    STR_USER_ERROR[-OB_SERVER_LISTEN_ERROR] = "Failed to listen to the port";
    ORACLE_ERRNO[-OB_SERVER_LISTEN_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_LISTEN_ERROR] =
        "ORA-00600: internal error code, arguments: -4004, Failed to listen to the port";
    ORACLE_STR_USER_ERROR[-OB_SERVER_LISTEN_ERROR] =
        "ORA-00600: internal error code, arguments: -4004, Failed to listen to the port";
    ERROR_NAME[-OB_INIT_TWICE] = "OB_INIT_TWICE";
    MYSQL_ERRNO[-OB_INIT_TWICE] = -1;
    SQLSTATE[-OB_INIT_TWICE] = "HY000";
    STR_ERROR[-OB_INIT_TWICE] = "The object is initialized twice";
    STR_USER_ERROR[-OB_INIT_TWICE] = "The object is initialized twice";
    ORACLE_ERRNO[-OB_INIT_TWICE] = 600;
    ORACLE_STR_ERROR[-OB_INIT_TWICE] =
        "ORA-00600: internal error code, arguments: -4005, The object is initialized twice";
    ORACLE_STR_USER_ERROR[-OB_INIT_TWICE] =
        "ORA-00600: internal error code, arguments: -4005, The object is initialized twice";
    ERROR_NAME[-OB_NOT_INIT] = "OB_NOT_INIT";
    MYSQL_ERRNO[-OB_NOT_INIT] = -1;
    SQLSTATE[-OB_NOT_INIT] = "HY000";
    STR_ERROR[-OB_NOT_INIT] = "The object is not initialized";
    STR_USER_ERROR[-OB_NOT_INIT] = "The object is not initialized";
    ORACLE_ERRNO[-OB_NOT_INIT] = 600;
    ORACLE_STR_ERROR[-OB_NOT_INIT] = "ORA-00600: internal error code, arguments: -4006, The object is not initialized";
    ORACLE_STR_USER_ERROR[-OB_NOT_INIT] =
        "ORA-00600: internal error code, arguments: -4006, The object is not initialized";
    ERROR_NAME[-OB_NOT_SUPPORTED] = "OB_NOT_SUPPORTED";
    MYSQL_ERRNO[-OB_NOT_SUPPORTED] = ER_NOT_SUPPORTED_YET;
    SQLSTATE[-OB_NOT_SUPPORTED] = "0A000";
    STR_ERROR[-OB_NOT_SUPPORTED] = "Not supported feature or function";
    STR_USER_ERROR[-OB_NOT_SUPPORTED] = "%s not supported";
    ORACLE_ERRNO[-OB_NOT_SUPPORTED] = 600;
    ORACLE_STR_ERROR[-OB_NOT_SUPPORTED] =
        "ORA-00600: internal error code, arguments: -4007, Not supported feature or function";
    ORACLE_STR_USER_ERROR[-OB_NOT_SUPPORTED] = "ORA-00600: internal error code, arguments: -4007, %s not supported";
    ERROR_NAME[-OB_ITER_END] = "OB_ITER_END";
    MYSQL_ERRNO[-OB_ITER_END] = -1;
    SQLSTATE[-OB_ITER_END] = "HY000";
    STR_ERROR[-OB_ITER_END] = "End of iteration";
    STR_USER_ERROR[-OB_ITER_END] = "End of iteration";
    ORACLE_ERRNO[-OB_ITER_END] = 600;
    ORACLE_STR_ERROR[-OB_ITER_END] = "ORA-00600: internal error code, arguments: -4008, End of iteration";
    ORACLE_STR_USER_ERROR[-OB_ITER_END] = "ORA-00600: internal error code, arguments: -4008, End of iteration";
    ERROR_NAME[-OB_IO_ERROR] = "OB_IO_ERROR";
    MYSQL_ERRNO[-OB_IO_ERROR] = -1;
    SQLSTATE[-OB_IO_ERROR] = "58030";
    STR_ERROR[-OB_IO_ERROR] = "IO error";
    STR_USER_ERROR[-OB_IO_ERROR] = "IO error";
    ORACLE_ERRNO[-OB_IO_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_IO_ERROR] = "ORA-00600: internal error code, arguments: -4009, IO error";
    ORACLE_STR_USER_ERROR[-OB_IO_ERROR] = "ORA-00600: internal error code, arguments: -4009, IO error";
    ERROR_NAME[-OB_ERROR_FUNC_VERSION] = "OB_ERROR_FUNC_VERSION";
    MYSQL_ERRNO[-OB_ERROR_FUNC_VERSION] = -1;
    SQLSTATE[-OB_ERROR_FUNC_VERSION] = "HY000";
    STR_ERROR[-OB_ERROR_FUNC_VERSION] = "Wrong RPC command version";
    STR_USER_ERROR[-OB_ERROR_FUNC_VERSION] = "Wrong RPC command version";
    ORACLE_ERRNO[-OB_ERROR_FUNC_VERSION] = 600;
    ORACLE_STR_ERROR[-OB_ERROR_FUNC_VERSION] =
        "ORA-00600: internal error code, arguments: -4010, Wrong RPC command version";
    ORACLE_STR_USER_ERROR[-OB_ERROR_FUNC_VERSION] =
        "ORA-00600: internal error code, arguments: -4010, Wrong RPC command version";
    ERROR_NAME[-OB_PACKET_NOT_SENT] = "OB_PACKET_NOT_SENT";
    MYSQL_ERRNO[-OB_PACKET_NOT_SENT] = -1;
    SQLSTATE[-OB_PACKET_NOT_SENT] = "HY000";
    STR_ERROR[-OB_PACKET_NOT_SENT] = "Can not send packet";
    STR_USER_ERROR[-OB_PACKET_NOT_SENT] = "Can not send packet";
    ORACLE_ERRNO[-OB_PACKET_NOT_SENT] = 600;
    ORACLE_STR_ERROR[-OB_PACKET_NOT_SENT] = "ORA-00600: internal error code, arguments: -4011, Can not send packet";
    ORACLE_STR_USER_ERROR[-OB_PACKET_NOT_SENT] =
        "ORA-00600: internal error code, arguments: -4011, Can not send packet";
    ERROR_NAME[-OB_TIMEOUT] = "OB_TIMEOUT";
    MYSQL_ERRNO[-OB_TIMEOUT] = -1;
    SQLSTATE[-OB_TIMEOUT] = "HY000";
    STR_ERROR[-OB_TIMEOUT] = "Timeout";
    STR_USER_ERROR[-OB_TIMEOUT] = "Timeout";
    ORACLE_ERRNO[-OB_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_TIMEOUT] = "ORA-00600: internal error code, arguments: -4012, Timeout";
    ORACLE_STR_USER_ERROR[-OB_TIMEOUT] = "ORA-00600: internal error code, arguments: -4012, Timeout";
    ERROR_NAME[-OB_ALLOCATE_MEMORY_FAILED] = "OB_ALLOCATE_MEMORY_FAILED";
    MYSQL_ERRNO[-OB_ALLOCATE_MEMORY_FAILED] = -1;
    SQLSTATE[-OB_ALLOCATE_MEMORY_FAILED] = "HY001";
    STR_ERROR[-OB_ALLOCATE_MEMORY_FAILED] = "No memory or reach tenant memory limit";
    STR_USER_ERROR[-OB_ALLOCATE_MEMORY_FAILED] = "No memory or reach tenant memory limit";
    ORACLE_ERRNO[-OB_ALLOCATE_MEMORY_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_ALLOCATE_MEMORY_FAILED] =
        "ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit";
    ORACLE_STR_USER_ERROR[-OB_ALLOCATE_MEMORY_FAILED] =
        "ORA-00600: internal error code, arguments: -4013, No memory or reach tenant memory limit";
    ERROR_NAME[-OB_INNER_STAT_ERROR] = "OB_INNER_STAT_ERROR";
    MYSQL_ERRNO[-OB_INNER_STAT_ERROR] = -1;
    SQLSTATE[-OB_INNER_STAT_ERROR] = "HY000";
    STR_ERROR[-OB_INNER_STAT_ERROR] = "Inner state error";
    STR_USER_ERROR[-OB_INNER_STAT_ERROR] = "Inner state error";
    ORACLE_ERRNO[-OB_INNER_STAT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_INNER_STAT_ERROR] = "ORA-00600: internal error code, arguments: -4014, Inner state error";
    ORACLE_STR_USER_ERROR[-OB_INNER_STAT_ERROR] = "ORA-00600: internal error code, arguments: -4014, Inner state error";
    ERROR_NAME[-OB_ERR_SYS] = "OB_ERR_SYS";
    MYSQL_ERRNO[-OB_ERR_SYS] = -1;
    SQLSTATE[-OB_ERR_SYS] = "HY000";
    STR_ERROR[-OB_ERR_SYS] = "System error";
    STR_USER_ERROR[-OB_ERR_SYS] = "System error";
    ORACLE_ERRNO[-OB_ERR_SYS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SYS] = "ORA-00600: internal error code, arguments: -4015, System error";
    ORACLE_STR_USER_ERROR[-OB_ERR_SYS] = "ORA-00600: internal error code, arguments: -4015, System error";
    ERROR_NAME[-OB_ERR_UNEXPECTED] = "OB_ERR_UNEXPECTED";
    MYSQL_ERRNO[-OB_ERR_UNEXPECTED] = -1;
    SQLSTATE[-OB_ERR_UNEXPECTED] = "HY000";
    STR_ERROR[-OB_ERR_UNEXPECTED] = "Internal error";
    STR_USER_ERROR[-OB_ERR_UNEXPECTED] = "%s";
    ORACLE_ERRNO[-OB_ERR_UNEXPECTED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNEXPECTED] = "ORA-00600: internal error code, arguments: -4016, Internal error";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNEXPECTED] = "ORA-00600: internal error code, arguments: -4016, %s";
    ERROR_NAME[-OB_ENTRY_EXIST] = "OB_ENTRY_EXIST";
    MYSQL_ERRNO[-OB_ENTRY_EXIST] = -1;
    SQLSTATE[-OB_ENTRY_EXIST] = "HY000";
    STR_ERROR[-OB_ENTRY_EXIST] = "Entry already exist";
    STR_USER_ERROR[-OB_ENTRY_EXIST] = "%s";
    ORACLE_ERRNO[-OB_ENTRY_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ENTRY_EXIST] = "ORA-00600: internal error code, arguments: -4017, Entry already exist";
    ORACLE_STR_USER_ERROR[-OB_ENTRY_EXIST] = "ORA-00600: internal error code, arguments: -4017, %s";
    ERROR_NAME[-OB_ENTRY_NOT_EXIST] = "OB_ENTRY_NOT_EXIST";
    MYSQL_ERRNO[-OB_ENTRY_NOT_EXIST] = -1;
    SQLSTATE[-OB_ENTRY_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ENTRY_NOT_EXIST] = "Entry not exist";
    STR_USER_ERROR[-OB_ENTRY_NOT_EXIST] = "%s";
    ORACLE_ERRNO[-OB_ENTRY_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ENTRY_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4018, Entry not exist";
    ORACLE_STR_USER_ERROR[-OB_ENTRY_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4018, %s";
    ERROR_NAME[-OB_SIZE_OVERFLOW] = "OB_SIZE_OVERFLOW";
    MYSQL_ERRNO[-OB_SIZE_OVERFLOW] = -1;
    SQLSTATE[-OB_SIZE_OVERFLOW] = "HY000";
    STR_ERROR[-OB_SIZE_OVERFLOW] = "Size overflow";
    STR_USER_ERROR[-OB_SIZE_OVERFLOW] = "Size overflow";
    ORACLE_ERRNO[-OB_SIZE_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_SIZE_OVERFLOW] = "ORA-00600: internal error code, arguments: -4019, Size overflow";
    ORACLE_STR_USER_ERROR[-OB_SIZE_OVERFLOW] = "ORA-00600: internal error code, arguments: -4019, Size overflow";
    ERROR_NAME[-OB_REF_NUM_NOT_ZERO] = "OB_REF_NUM_NOT_ZERO";
    MYSQL_ERRNO[-OB_REF_NUM_NOT_ZERO] = -1;
    SQLSTATE[-OB_REF_NUM_NOT_ZERO] = "HY000";
    STR_ERROR[-OB_REF_NUM_NOT_ZERO] = "Reference count is not zero";
    STR_USER_ERROR[-OB_REF_NUM_NOT_ZERO] = "Reference count is not zero";
    ORACLE_ERRNO[-OB_REF_NUM_NOT_ZERO] = 600;
    ORACLE_STR_ERROR[-OB_REF_NUM_NOT_ZERO] =
        "ORA-00600: internal error code, arguments: -4020, Reference count is not zero";
    ORACLE_STR_USER_ERROR[-OB_REF_NUM_NOT_ZERO] =
        "ORA-00600: internal error code, arguments: -4020, Reference count is not zero";
    ERROR_NAME[-OB_CONFLICT_VALUE] = "OB_CONFLICT_VALUE";
    MYSQL_ERRNO[-OB_CONFLICT_VALUE] = -1;
    SQLSTATE[-OB_CONFLICT_VALUE] = "HY000";
    STR_ERROR[-OB_CONFLICT_VALUE] = "Conflict value";
    STR_USER_ERROR[-OB_CONFLICT_VALUE] = "Conflict value";
    ORACLE_ERRNO[-OB_CONFLICT_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_CONFLICT_VALUE] = "ORA-00600: internal error code, arguments: -4021, Conflict value";
    ORACLE_STR_USER_ERROR[-OB_CONFLICT_VALUE] = "ORA-00600: internal error code, arguments: -4021, Conflict value";
    ERROR_NAME[-OB_ITEM_NOT_SETTED] = "OB_ITEM_NOT_SETTED";
    MYSQL_ERRNO[-OB_ITEM_NOT_SETTED] = -1;
    SQLSTATE[-OB_ITEM_NOT_SETTED] = "HY000";
    STR_ERROR[-OB_ITEM_NOT_SETTED] = "Item not set";
    STR_USER_ERROR[-OB_ITEM_NOT_SETTED] = "Item not set";
    ORACLE_ERRNO[-OB_ITEM_NOT_SETTED] = 600;
    ORACLE_STR_ERROR[-OB_ITEM_NOT_SETTED] = "ORA-00600: internal error code, arguments: -4022, Item not set";
    ORACLE_STR_USER_ERROR[-OB_ITEM_NOT_SETTED] = "ORA-00600: internal error code, arguments: -4022, Item not set";
    ERROR_NAME[-OB_EAGAIN] = "OB_EAGAIN";
    MYSQL_ERRNO[-OB_EAGAIN] = -1;
    SQLSTATE[-OB_EAGAIN] = "HY000";
    STR_ERROR[-OB_EAGAIN] = "Try again";
    STR_USER_ERROR[-OB_EAGAIN] = "Try again";
    ORACLE_ERRNO[-OB_EAGAIN] = 600;
    ORACLE_STR_ERROR[-OB_EAGAIN] = "ORA-00600: internal error code, arguments: -4023, Try again";
    ORACLE_STR_USER_ERROR[-OB_EAGAIN] = "ORA-00600: internal error code, arguments: -4023, Try again";
    ERROR_NAME[-OB_BUF_NOT_ENOUGH] = "OB_BUF_NOT_ENOUGH";
    MYSQL_ERRNO[-OB_BUF_NOT_ENOUGH] = -1;
    SQLSTATE[-OB_BUF_NOT_ENOUGH] = "HY000";
    STR_ERROR[-OB_BUF_NOT_ENOUGH] = "Buffer not enough";
    STR_USER_ERROR[-OB_BUF_NOT_ENOUGH] = "Buffer not enough";
    ORACLE_ERRNO[-OB_BUF_NOT_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_BUF_NOT_ENOUGH] = "ORA-00600: internal error code, arguments: -4024, Buffer not enough";
    ORACLE_STR_USER_ERROR[-OB_BUF_NOT_ENOUGH] = "ORA-00600: internal error code, arguments: -4024, Buffer not enough";
    ERROR_NAME[-OB_PARTIAL_FAILED] = "OB_PARTIAL_FAILED";
    MYSQL_ERRNO[-OB_PARTIAL_FAILED] = -1;
    SQLSTATE[-OB_PARTIAL_FAILED] = "HY000";
    STR_ERROR[-OB_PARTIAL_FAILED] = "Partial failed";
    STR_USER_ERROR[-OB_PARTIAL_FAILED] = "Partial failed";
    ORACLE_ERRNO[-OB_PARTIAL_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_PARTIAL_FAILED] = "ORA-00600: internal error code, arguments: -4025, Partial failed";
    ORACLE_STR_USER_ERROR[-OB_PARTIAL_FAILED] = "ORA-00600: internal error code, arguments: -4025, Partial failed";
    ERROR_NAME[-OB_READ_NOTHING] = "OB_READ_NOTHING";
    MYSQL_ERRNO[-OB_READ_NOTHING] = -1;
    SQLSTATE[-OB_READ_NOTHING] = "02000";
    STR_ERROR[-OB_READ_NOTHING] = "Nothing to read";
    STR_USER_ERROR[-OB_READ_NOTHING] = "Nothing to read";
    ORACLE_ERRNO[-OB_READ_NOTHING] = 1403;
    ORACLE_STR_ERROR[-OB_READ_NOTHING] = "ORA-01403: no data found";
    ORACLE_STR_USER_ERROR[-OB_READ_NOTHING] = "ORA-01403: no data found";
    ERROR_NAME[-OB_FILE_NOT_EXIST] = "OB_FILE_NOT_EXIST";
    MYSQL_ERRNO[-OB_FILE_NOT_EXIST] = ER_FILE_NOT_FOUND;
    SQLSTATE[-OB_FILE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_FILE_NOT_EXIST] = "File not exist";
    STR_USER_ERROR[-OB_FILE_NOT_EXIST] = "File not exist";
    ORACLE_ERRNO[-OB_FILE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_FILE_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4027, File not exist";
    ORACLE_STR_USER_ERROR[-OB_FILE_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4027, File not exist";
    ERROR_NAME[-OB_DISCONTINUOUS_LOG] = "OB_DISCONTINUOUS_LOG";
    MYSQL_ERRNO[-OB_DISCONTINUOUS_LOG] = -1;
    SQLSTATE[-OB_DISCONTINUOUS_LOG] = "HY000";
    STR_ERROR[-OB_DISCONTINUOUS_LOG] = "Log entry not continuous";
    STR_USER_ERROR[-OB_DISCONTINUOUS_LOG] = "Log entry not continuous";
    ORACLE_ERRNO[-OB_DISCONTINUOUS_LOG] = 600;
    ORACLE_STR_ERROR[-OB_DISCONTINUOUS_LOG] =
        "ORA-00600: internal error code, arguments: -4028, Log entry not continuous";
    ORACLE_STR_USER_ERROR[-OB_DISCONTINUOUS_LOG] =
        "ORA-00600: internal error code, arguments: -4028, Log entry not continuous";
    ERROR_NAME[-OB_SCHEMA_ERROR] = "OB_SCHEMA_ERROR";
    MYSQL_ERRNO[-OB_SCHEMA_ERROR] = -1;
    SQLSTATE[-OB_SCHEMA_ERROR] = "HY000";
    STR_ERROR[-OB_SCHEMA_ERROR] = "Schema error";
    STR_USER_ERROR[-OB_SCHEMA_ERROR] = "Schema error";
    ORACLE_ERRNO[-OB_SCHEMA_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_SCHEMA_ERROR] = "ORA-00600: internal error code, arguments: -4029, Schema error";
    ORACLE_STR_USER_ERROR[-OB_SCHEMA_ERROR] = "ORA-00600: internal error code, arguments: -4029, Schema error";
    ERROR_NAME[-OB_TENANT_OUT_OF_MEM] = "OB_TENANT_OUT_OF_MEM";
    MYSQL_ERRNO[-OB_TENANT_OUT_OF_MEM] = -1;
    SQLSTATE[-OB_TENANT_OUT_OF_MEM] = "HY000";
    STR_ERROR[-OB_TENANT_OUT_OF_MEM] = "Over tenant memory limits";
    STR_USER_ERROR[-OB_TENANT_OUT_OF_MEM] = "Over tenant memory limits";
    ORACLE_ERRNO[-OB_TENANT_OUT_OF_MEM] = 600;
    ORACLE_STR_ERROR[-OB_TENANT_OUT_OF_MEM] =
        "ORA-00600: internal error code, arguments: -4030, Over tenant memory limits";
    ORACLE_STR_USER_ERROR[-OB_TENANT_OUT_OF_MEM] =
        "ORA-00600: internal error code, arguments: -4030, Over tenant memory limits";
    ERROR_NAME[-OB_UNKNOWN_OBJ] = "OB_UNKNOWN_OBJ";
    MYSQL_ERRNO[-OB_UNKNOWN_OBJ] = -1;
    SQLSTATE[-OB_UNKNOWN_OBJ] = "HY004";
    STR_ERROR[-OB_UNKNOWN_OBJ] = "Unknown object";
    STR_USER_ERROR[-OB_UNKNOWN_OBJ] = "Unknown object";
    ORACLE_ERRNO[-OB_UNKNOWN_OBJ] = 600;
    ORACLE_STR_ERROR[-OB_UNKNOWN_OBJ] = "ORA-00600: internal error code, arguments: -4031, Unknown object";
    ORACLE_STR_USER_ERROR[-OB_UNKNOWN_OBJ] = "ORA-00600: internal error code, arguments: -4031, Unknown object";
    ERROR_NAME[-OB_NO_MONITOR_DATA] = "OB_NO_MONITOR_DATA";
    MYSQL_ERRNO[-OB_NO_MONITOR_DATA] = -1;
    SQLSTATE[-OB_NO_MONITOR_DATA] = "02000";
    STR_ERROR[-OB_NO_MONITOR_DATA] = "No monitor data";
    STR_USER_ERROR[-OB_NO_MONITOR_DATA] = "No monitor data";
    ORACLE_ERRNO[-OB_NO_MONITOR_DATA] = 600;
    ORACLE_STR_ERROR[-OB_NO_MONITOR_DATA] = "ORA-00600: internal error code, arguments: -4032, No monitor data";
    ORACLE_STR_USER_ERROR[-OB_NO_MONITOR_DATA] = "ORA-00600: internal error code, arguments: -4032, No monitor data";
    ERROR_NAME[-OB_SERIALIZE_ERROR] = "OB_SERIALIZE_ERROR";
    MYSQL_ERRNO[-OB_SERIALIZE_ERROR] = -1;
    SQLSTATE[-OB_SERIALIZE_ERROR] = "HY000";
    STR_ERROR[-OB_SERIALIZE_ERROR] = "Serialize error";
    STR_USER_ERROR[-OB_SERIALIZE_ERROR] = "Serialize error";
    ORACLE_ERRNO[-OB_SERIALIZE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_SERIALIZE_ERROR] = "ORA-00600: internal error code, arguments: -4033, Serialize error";
    ORACLE_STR_USER_ERROR[-OB_SERIALIZE_ERROR] = "ORA-00600: internal error code, arguments: -4033, Serialize error";
    ERROR_NAME[-OB_DESERIALIZE_ERROR] = "OB_DESERIALIZE_ERROR";
    MYSQL_ERRNO[-OB_DESERIALIZE_ERROR] = -1;
    SQLSTATE[-OB_DESERIALIZE_ERROR] = "HY000";
    STR_ERROR[-OB_DESERIALIZE_ERROR] = "Deserialize error";
    STR_USER_ERROR[-OB_DESERIALIZE_ERROR] = "Deserialize error";
    ORACLE_ERRNO[-OB_DESERIALIZE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_DESERIALIZE_ERROR] = "ORA-00600: internal error code, arguments: -4034, Deserialize error";
    ORACLE_STR_USER_ERROR[-OB_DESERIALIZE_ERROR] =
        "ORA-00600: internal error code, arguments: -4034, Deserialize error";
    ERROR_NAME[-OB_AIO_TIMEOUT] = "OB_AIO_TIMEOUT";
    MYSQL_ERRNO[-OB_AIO_TIMEOUT] = -1;
    SQLSTATE[-OB_AIO_TIMEOUT] = "HY000";
    STR_ERROR[-OB_AIO_TIMEOUT] = "Asynchronous IO error";
    STR_USER_ERROR[-OB_AIO_TIMEOUT] = "Asynchronous IO error";
    ORACLE_ERRNO[-OB_AIO_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_AIO_TIMEOUT] = "ORA-00600: internal error code, arguments: -4035, Asynchronous IO error";
    ORACLE_STR_USER_ERROR[-OB_AIO_TIMEOUT] = "ORA-00600: internal error code, arguments: -4035, Asynchronous IO error";
    ERROR_NAME[-OB_NEED_RETRY] = "OB_NEED_RETRY";
    MYSQL_ERRNO[-OB_NEED_RETRY] = -1;
    SQLSTATE[-OB_NEED_RETRY] = "HY000";
    STR_ERROR[-OB_NEED_RETRY] = "Need retry";
    STR_USER_ERROR[-OB_NEED_RETRY] = "Need retry";
    ORACLE_ERRNO[-OB_NEED_RETRY] = 600;
    ORACLE_STR_ERROR[-OB_NEED_RETRY] = "ORA-00600: internal error code, arguments: -4036, Need retry";
    ORACLE_STR_USER_ERROR[-OB_NEED_RETRY] = "ORA-00600: internal error code, arguments: -4036, Need retry";
    ERROR_NAME[-OB_TOO_MANY_SSTABLE] = "OB_TOO_MANY_SSTABLE";
    MYSQL_ERRNO[-OB_TOO_MANY_SSTABLE] = -1;
    SQLSTATE[-OB_TOO_MANY_SSTABLE] = "HY000";
    STR_ERROR[-OB_TOO_MANY_SSTABLE] = "Too many sstable";
    STR_USER_ERROR[-OB_TOO_MANY_SSTABLE] = "Too many sstable";
    ORACLE_ERRNO[-OB_TOO_MANY_SSTABLE] = 600;
    ORACLE_STR_ERROR[-OB_TOO_MANY_SSTABLE] = "ORA-00600: internal error code, arguments: -4037, Too many sstable";
    ORACLE_STR_USER_ERROR[-OB_TOO_MANY_SSTABLE] = "ORA-00600: internal error code, arguments: -4037, Too many sstable";
    ERROR_NAME[-OB_NOT_MASTER] = "OB_NOT_MASTER";
    MYSQL_ERRNO[-OB_NOT_MASTER] = -1;
    SQLSTATE[-OB_NOT_MASTER] = "HY000";
    STR_ERROR[-OB_NOT_MASTER] = "The observer or zone is not the master";
    STR_USER_ERROR[-OB_NOT_MASTER] = "The observer or zone is not the master";
    ORACLE_ERRNO[-OB_NOT_MASTER] = 600;
    ORACLE_STR_ERROR[-OB_NOT_MASTER] =
        "ORA-00600: internal error code, arguments: -4038, The observer or zone is not the master";
    ORACLE_STR_USER_ERROR[-OB_NOT_MASTER] =
        "ORA-00600: internal error code, arguments: -4038, The observer or zone is not the master";
    ERROR_NAME[-OB_KILLED_BY_THROTTLING] = "OB_KILLED_BY_THROTTLING";
    MYSQL_ERRNO[-OB_KILLED_BY_THROTTLING] = -1;
    SQLSTATE[-OB_KILLED_BY_THROTTLING] = "HY000";
    STR_ERROR[-OB_KILLED_BY_THROTTLING] = "Request has killed by sql throttle";
    STR_USER_ERROR[-OB_KILLED_BY_THROTTLING] = "Request has killed by sql throttle";
    ORACLE_ERRNO[-OB_KILLED_BY_THROTTLING] = 600;
    ORACLE_STR_ERROR[-OB_KILLED_BY_THROTTLING] =
        "ORA-00600: internal error code, arguments: -4039, Request has killed by sql throttle";
    ORACLE_STR_USER_ERROR[-OB_KILLED_BY_THROTTLING] =
        "ORA-00600: internal error code, arguments: -4039, Request has killed by sql throttle";
    ERROR_NAME[-OB_DECRYPT_FAILED] = "OB_DECRYPT_FAILED";
    MYSQL_ERRNO[-OB_DECRYPT_FAILED] = -1;
    SQLSTATE[-OB_DECRYPT_FAILED] = "HY000";
    STR_ERROR[-OB_DECRYPT_FAILED] = "Decrypt error";
    STR_USER_ERROR[-OB_DECRYPT_FAILED] = "Decrypt error";
    ORACLE_ERRNO[-OB_DECRYPT_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_DECRYPT_FAILED] = "ORA-00600: internal error code, arguments: -4041, Decrypt error";
    ORACLE_STR_USER_ERROR[-OB_DECRYPT_FAILED] = "ORA-00600: internal error code, arguments: -4041, Decrypt error";
    ERROR_NAME[-OB_USER_NOT_EXIST] = "OB_USER_NOT_EXIST";
    MYSQL_ERRNO[-OB_USER_NOT_EXIST] = ER_PASSWORD_NO_MATCH;
    SQLSTATE[-OB_USER_NOT_EXIST] = "42000";
    STR_ERROR[-OB_USER_NOT_EXIST] = "Can not find any matching row in the user table";
    STR_USER_ERROR[-OB_USER_NOT_EXIST] = "Can not find any matching row in the user table'%.*s'";
    ORACLE_ERRNO[-OB_USER_NOT_EXIST] = 1918;
    ORACLE_STR_ERROR[-OB_USER_NOT_EXIST] = "ORA-01918: user does not exist";
    ORACLE_STR_USER_ERROR[-OB_USER_NOT_EXIST] = "ORA-01918: user '%.*s' does not exist";
    ERROR_NAME[-OB_PASSWORD_WRONG] = "OB_PASSWORD_WRONG";
    MYSQL_ERRNO[-OB_PASSWORD_WRONG] = ER_ACCESS_DENIED_ERROR;
    SQLSTATE[-OB_PASSWORD_WRONG] = "42000";
    STR_ERROR[-OB_PASSWORD_WRONG] = "Access denied for user";
    STR_USER_ERROR[-OB_PASSWORD_WRONG] = "Access denied for user '%.*s'@'%.*s' (using password: %s)";
    ORACLE_ERRNO[-OB_PASSWORD_WRONG] = 600;
    ORACLE_STR_ERROR[-OB_PASSWORD_WRONG] = "ORA-00600: internal error code, arguments: -4043, Access denied for user";
    ORACLE_STR_USER_ERROR[-OB_PASSWORD_WRONG] =
        "ORA-00600: internal error code, arguments: -4043, Access denied for user '%.*s'@'%.*s' (using password: %s)";
    ERROR_NAME[-OB_SKEY_VERSION_WRONG] = "OB_SKEY_VERSION_WRONG";
    MYSQL_ERRNO[-OB_SKEY_VERSION_WRONG] = -1;
    SQLSTATE[-OB_SKEY_VERSION_WRONG] = "HY000";
    STR_ERROR[-OB_SKEY_VERSION_WRONG] = "Wrong skey version";
    STR_USER_ERROR[-OB_SKEY_VERSION_WRONG] = "Wrong skey version";
    ORACLE_ERRNO[-OB_SKEY_VERSION_WRONG] = 600;
    ORACLE_STR_ERROR[-OB_SKEY_VERSION_WRONG] = "ORA-00600: internal error code, arguments: -4044, Wrong skey version";
    ORACLE_STR_USER_ERROR[-OB_SKEY_VERSION_WRONG] =
        "ORA-00600: internal error code, arguments: -4044, Wrong skey version";
    ERROR_NAME[-OB_NOT_REGISTERED] = "OB_NOT_REGISTERED";
    MYSQL_ERRNO[-OB_NOT_REGISTERED] = -1;
    SQLSTATE[-OB_NOT_REGISTERED] = "HY000";
    STR_ERROR[-OB_NOT_REGISTERED] = "Not registered";
    STR_USER_ERROR[-OB_NOT_REGISTERED] = "Not registered";
    ORACLE_ERRNO[-OB_NOT_REGISTERED] = 600;
    ORACLE_STR_ERROR[-OB_NOT_REGISTERED] = "ORA-00600: internal error code, arguments: -4048, Not registered";
    ORACLE_STR_USER_ERROR[-OB_NOT_REGISTERED] = "ORA-00600: internal error code, arguments: -4048, Not registered";
    ERROR_NAME[-OB_WAITQUEUE_TIMEOUT] = "OB_WAITQUEUE_TIMEOUT";
    MYSQL_ERRNO[-OB_WAITQUEUE_TIMEOUT] = 4012;
    SQLSTATE[-OB_WAITQUEUE_TIMEOUT] = "HY000";
    STR_ERROR[-OB_WAITQUEUE_TIMEOUT] = "Task timeout and not executed";
    STR_USER_ERROR[-OB_WAITQUEUE_TIMEOUT] = "Task timeout and not executed";
    ORACLE_ERRNO[-OB_WAITQUEUE_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_WAITQUEUE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4049, Task timeout and not executed";
    ORACLE_STR_USER_ERROR[-OB_WAITQUEUE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4049, Task timeout and not executed";
    ERROR_NAME[-OB_NOT_THE_OBJECT] = "OB_NOT_THE_OBJECT";
    MYSQL_ERRNO[-OB_NOT_THE_OBJECT] = -1;
    SQLSTATE[-OB_NOT_THE_OBJECT] = "HY000";
    STR_ERROR[-OB_NOT_THE_OBJECT] = "Not the object";
    STR_USER_ERROR[-OB_NOT_THE_OBJECT] = "Not the object";
    ORACLE_ERRNO[-OB_NOT_THE_OBJECT] = 600;
    ORACLE_STR_ERROR[-OB_NOT_THE_OBJECT] = "ORA-00600: internal error code, arguments: -4050, Not the object";
    ORACLE_STR_USER_ERROR[-OB_NOT_THE_OBJECT] = "ORA-00600: internal error code, arguments: -4050, Not the object";
    ERROR_NAME[-OB_ALREADY_REGISTERED] = "OB_ALREADY_REGISTERED";
    MYSQL_ERRNO[-OB_ALREADY_REGISTERED] = -1;
    SQLSTATE[-OB_ALREADY_REGISTERED] = "HY000";
    STR_ERROR[-OB_ALREADY_REGISTERED] = "Already registered";
    STR_USER_ERROR[-OB_ALREADY_REGISTERED] = "Already registered";
    ORACLE_ERRNO[-OB_ALREADY_REGISTERED] = 600;
    ORACLE_STR_ERROR[-OB_ALREADY_REGISTERED] = "ORA-00600: internal error code, arguments: -4051, Already registered";
    ORACLE_STR_USER_ERROR[-OB_ALREADY_REGISTERED] =
        "ORA-00600: internal error code, arguments: -4051, Already registered";
    ERROR_NAME[-OB_LAST_LOG_RUINNED] = "OB_LAST_LOG_RUINNED";
    MYSQL_ERRNO[-OB_LAST_LOG_RUINNED] = -1;
    SQLSTATE[-OB_LAST_LOG_RUINNED] = "HY000";
    STR_ERROR[-OB_LAST_LOG_RUINNED] = "Corrupted log entry";
    STR_USER_ERROR[-OB_LAST_LOG_RUINNED] = "Corrupted log entry";
    ORACLE_ERRNO[-OB_LAST_LOG_RUINNED] = 600;
    ORACLE_STR_ERROR[-OB_LAST_LOG_RUINNED] = "ORA-00600: internal error code, arguments: -4052, Corrupted log entry";
    ORACLE_STR_USER_ERROR[-OB_LAST_LOG_RUINNED] =
        "ORA-00600: internal error code, arguments: -4052, Corrupted log entry";
    ERROR_NAME[-OB_NO_CS_SELECTED] = "OB_NO_CS_SELECTED";
    MYSQL_ERRNO[-OB_NO_CS_SELECTED] = -1;
    SQLSTATE[-OB_NO_CS_SELECTED] = "HY000";
    STR_ERROR[-OB_NO_CS_SELECTED] = "No ChunkServer selected";
    STR_USER_ERROR[-OB_NO_CS_SELECTED] = "No ChunkServer selected";
    ORACLE_ERRNO[-OB_NO_CS_SELECTED] = 600;
    ORACLE_STR_ERROR[-OB_NO_CS_SELECTED] = "ORA-00600: internal error code, arguments: -4053, No ChunkServer selected";
    ORACLE_STR_USER_ERROR[-OB_NO_CS_SELECTED] =
        "ORA-00600: internal error code, arguments: -4053, No ChunkServer selected";
    ERROR_NAME[-OB_NO_TABLETS_CREATED] = "OB_NO_TABLETS_CREATED";
    MYSQL_ERRNO[-OB_NO_TABLETS_CREATED] = -1;
    SQLSTATE[-OB_NO_TABLETS_CREATED] = "HY000";
    STR_ERROR[-OB_NO_TABLETS_CREATED] = "No tablets created";
    STR_USER_ERROR[-OB_NO_TABLETS_CREATED] = "No tablets created";
    ORACLE_ERRNO[-OB_NO_TABLETS_CREATED] = 600;
    ORACLE_STR_ERROR[-OB_NO_TABLETS_CREATED] = "ORA-00600: internal error code, arguments: -4054, No tablets created";
    ORACLE_STR_USER_ERROR[-OB_NO_TABLETS_CREATED] =
        "ORA-00600: internal error code, arguments: -4054, No tablets created";
    ERROR_NAME[-OB_INVALID_ERROR] = "OB_INVALID_ERROR";
    MYSQL_ERRNO[-OB_INVALID_ERROR] = -1;
    SQLSTATE[-OB_INVALID_ERROR] = "HY000";
    STR_ERROR[-OB_INVALID_ERROR] = "Invalid entry";
    STR_USER_ERROR[-OB_INVALID_ERROR] = "Invalid entry";
    ORACLE_ERRNO[-OB_INVALID_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ERROR] = "ORA-00600: internal error code, arguments: -4055, Invalid entry";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ERROR] = "ORA-00600: internal error code, arguments: -4055, Invalid entry";
    ERROR_NAME[-OB_DECIMAL_OVERFLOW_WARN] = "OB_DECIMAL_OVERFLOW_WARN";
    MYSQL_ERRNO[-OB_DECIMAL_OVERFLOW_WARN] = -1;
    SQLSTATE[-OB_DECIMAL_OVERFLOW_WARN] = "HY000";
    STR_ERROR[-OB_DECIMAL_OVERFLOW_WARN] = "Decimal overflow warning";
    STR_USER_ERROR[-OB_DECIMAL_OVERFLOW_WARN] = "Decimal overflow warning";
    ORACLE_ERRNO[-OB_DECIMAL_OVERFLOW_WARN] = 600;
    ORACLE_STR_ERROR[-OB_DECIMAL_OVERFLOW_WARN] =
        "ORA-00600: internal error code, arguments: -4057, Decimal overflow warning";
    ORACLE_STR_USER_ERROR[-OB_DECIMAL_OVERFLOW_WARN] =
        "ORA-00600: internal error code, arguments: -4057, Decimal overflow warning";
    ERROR_NAME[-OB_DECIMAL_UNLEGAL_ERROR] = "OB_DECIMAL_UNLEGAL_ERROR";
    MYSQL_ERRNO[-OB_DECIMAL_UNLEGAL_ERROR] = -1;
    SQLSTATE[-OB_DECIMAL_UNLEGAL_ERROR] = "HY000";
    STR_ERROR[-OB_DECIMAL_UNLEGAL_ERROR] = "Decimal overflow error";
    STR_USER_ERROR[-OB_DECIMAL_UNLEGAL_ERROR] = "Decimal overflow error";
    ORACLE_ERRNO[-OB_DECIMAL_UNLEGAL_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_DECIMAL_UNLEGAL_ERROR] =
        "ORA-00600: internal error code, arguments: -4058, Decimal overflow error";
    ORACLE_STR_USER_ERROR[-OB_DECIMAL_UNLEGAL_ERROR] =
        "ORA-00600: internal error code, arguments: -4058, Decimal overflow error";
    ERROR_NAME[-OB_OBJ_DIVIDE_ERROR] = "OB_OBJ_DIVIDE_ERROR";
    MYSQL_ERRNO[-OB_OBJ_DIVIDE_ERROR] = -1;
    SQLSTATE[-OB_OBJ_DIVIDE_ERROR] = "HY000";
    STR_ERROR[-OB_OBJ_DIVIDE_ERROR] = "Divide error";
    STR_USER_ERROR[-OB_OBJ_DIVIDE_ERROR] = "Divide error";
    ORACLE_ERRNO[-OB_OBJ_DIVIDE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_OBJ_DIVIDE_ERROR] = "ORA-00600: internal error code, arguments: -4060, Divide error";
    ORACLE_STR_USER_ERROR[-OB_OBJ_DIVIDE_ERROR] = "ORA-00600: internal error code, arguments: -4060, Divide error";
    ERROR_NAME[-OB_NOT_A_DECIMAL] = "OB_NOT_A_DECIMAL";
    MYSQL_ERRNO[-OB_NOT_A_DECIMAL] = -1;
    SQLSTATE[-OB_NOT_A_DECIMAL] = "HY000";
    STR_ERROR[-OB_NOT_A_DECIMAL] = "Not a decimal";
    STR_USER_ERROR[-OB_NOT_A_DECIMAL] = "Not a decimal";
    ORACLE_ERRNO[-OB_NOT_A_DECIMAL] = 600;
    ORACLE_STR_ERROR[-OB_NOT_A_DECIMAL] = "ORA-00600: internal error code, arguments: -4061, Not a decimal";
    ORACLE_STR_USER_ERROR[-OB_NOT_A_DECIMAL] = "ORA-00600: internal error code, arguments: -4061, Not a decimal";
    ERROR_NAME[-OB_DECIMAL_PRECISION_NOT_EQUAL] = "OB_DECIMAL_PRECISION_NOT_EQUAL";
    MYSQL_ERRNO[-OB_DECIMAL_PRECISION_NOT_EQUAL] = -1;
    SQLSTATE[-OB_DECIMAL_PRECISION_NOT_EQUAL] = "HY104";
    STR_ERROR[-OB_DECIMAL_PRECISION_NOT_EQUAL] = "Decimal precision error";
    STR_USER_ERROR[-OB_DECIMAL_PRECISION_NOT_EQUAL] = "Decimal precision error";
    ORACLE_ERRNO[-OB_DECIMAL_PRECISION_NOT_EQUAL] = 600;
    ORACLE_STR_ERROR[-OB_DECIMAL_PRECISION_NOT_EQUAL] =
        "ORA-00600: internal error code, arguments: -4062, Decimal precision error";
    ORACLE_STR_USER_ERROR[-OB_DECIMAL_PRECISION_NOT_EQUAL] =
        "ORA-00600: internal error code, arguments: -4062, Decimal precision error";
    ERROR_NAME[-OB_EMPTY_RANGE] = "OB_EMPTY_RANGE";
    MYSQL_ERRNO[-OB_EMPTY_RANGE] = -1;
    SQLSTATE[-OB_EMPTY_RANGE] = "HY000";
    STR_ERROR[-OB_EMPTY_RANGE] = "Empty range";
    STR_USER_ERROR[-OB_EMPTY_RANGE] = "Empty range";
    ORACLE_ERRNO[-OB_EMPTY_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_EMPTY_RANGE] = "ORA-00600: internal error code, arguments: -4063, Empty range";
    ORACLE_STR_USER_ERROR[-OB_EMPTY_RANGE] = "ORA-00600: internal error code, arguments: -4063, Empty range";
    ERROR_NAME[-OB_SESSION_KILLED] = "OB_SESSION_KILLED";
    MYSQL_ERRNO[-OB_SESSION_KILLED] = -1;
    SQLSTATE[-OB_SESSION_KILLED] = "HY000";
    STR_ERROR[-OB_SESSION_KILLED] = "Session killed";
    STR_USER_ERROR[-OB_SESSION_KILLED] = "Session killed";
    ORACLE_ERRNO[-OB_SESSION_KILLED] = 600;
    ORACLE_STR_ERROR[-OB_SESSION_KILLED] = "ORA-00600: internal error code, arguments: -4064, Session killed";
    ORACLE_STR_USER_ERROR[-OB_SESSION_KILLED] = "ORA-00600: internal error code, arguments: -4064, Session killed";
    ERROR_NAME[-OB_LOG_NOT_SYNC] = "OB_LOG_NOT_SYNC";
    MYSQL_ERRNO[-OB_LOG_NOT_SYNC] = -1;
    SQLSTATE[-OB_LOG_NOT_SYNC] = "HY000";
    STR_ERROR[-OB_LOG_NOT_SYNC] = "Log not sync";
    STR_USER_ERROR[-OB_LOG_NOT_SYNC] = "Log not sync";
    ORACLE_ERRNO[-OB_LOG_NOT_SYNC] = 600;
    ORACLE_STR_ERROR[-OB_LOG_NOT_SYNC] = "ORA-00600: internal error code, arguments: -4065, Log not sync";
    ORACLE_STR_USER_ERROR[-OB_LOG_NOT_SYNC] = "ORA-00600: internal error code, arguments: -4065, Log not sync";
    ERROR_NAME[-OB_DIR_NOT_EXIST] = "OB_DIR_NOT_EXIST";
    MYSQL_ERRNO[-OB_DIR_NOT_EXIST] = ER_CANT_READ_DIR;
    SQLSTATE[-OB_DIR_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_DIR_NOT_EXIST] = "Directory not exist";
    STR_USER_ERROR[-OB_DIR_NOT_EXIST] = "Directory not exist";
    ORACLE_ERRNO[-OB_DIR_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_DIR_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4066, Directory not exist";
    ORACLE_STR_USER_ERROR[-OB_DIR_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4066, Directory not exist";
    ERROR_NAME[-OB_SESSION_NOT_FOUND] = "OB_SESSION_NOT_FOUND";
    MYSQL_ERRNO[-OB_SESSION_NOT_FOUND] = 4012;
    SQLSTATE[-OB_SESSION_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_SESSION_NOT_FOUND] = "RPC session not found";
    STR_USER_ERROR[-OB_SESSION_NOT_FOUND] = "RPC session not found";
    ORACLE_ERRNO[-OB_SESSION_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_SESSION_NOT_FOUND] = "ORA-00600: internal error code, arguments: -4067, RPC session not found";
    ORACLE_STR_USER_ERROR[-OB_SESSION_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -4067, RPC session not found";
    ERROR_NAME[-OB_INVALID_LOG] = "OB_INVALID_LOG";
    MYSQL_ERRNO[-OB_INVALID_LOG] = -1;
    SQLSTATE[-OB_INVALID_LOG] = "HY000";
    STR_ERROR[-OB_INVALID_LOG] = "Invalid log";
    STR_USER_ERROR[-OB_INVALID_LOG] = "Invalid log";
    ORACLE_ERRNO[-OB_INVALID_LOG] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_LOG] = "ORA-00600: internal error code, arguments: -4068, Invalid log";
    ORACLE_STR_USER_ERROR[-OB_INVALID_LOG] = "ORA-00600: internal error code, arguments: -4068, Invalid log";
    ERROR_NAME[-OB_INVALID_DATA] = "OB_INVALID_DATA";
    MYSQL_ERRNO[-OB_INVALID_DATA] = -1;
    SQLSTATE[-OB_INVALID_DATA] = "HY000";
    STR_ERROR[-OB_INVALID_DATA] = "Invalid data";
    STR_USER_ERROR[-OB_INVALID_DATA] = "Invalid data";
    ORACLE_ERRNO[-OB_INVALID_DATA] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_DATA] = "ORA-00600: internal error code, arguments: -4070, Invalid data";
    ORACLE_STR_USER_ERROR[-OB_INVALID_DATA] = "ORA-00600: internal error code, arguments: -4070, Invalid data";
    ERROR_NAME[-OB_ALREADY_DONE] = "OB_ALREADY_DONE";
    MYSQL_ERRNO[-OB_ALREADY_DONE] = -1;
    SQLSTATE[-OB_ALREADY_DONE] = "HY000";
    STR_ERROR[-OB_ALREADY_DONE] = "Already done";
    STR_USER_ERROR[-OB_ALREADY_DONE] = "Already done";
    ORACLE_ERRNO[-OB_ALREADY_DONE] = 600;
    ORACLE_STR_ERROR[-OB_ALREADY_DONE] = "ORA-00600: internal error code, arguments: -4071, Already done";
    ORACLE_STR_USER_ERROR[-OB_ALREADY_DONE] = "ORA-00600: internal error code, arguments: -4071, Already done";
    ERROR_NAME[-OB_CANCELED] = "OB_CANCELED";
    MYSQL_ERRNO[-OB_CANCELED] = -1;
    SQLSTATE[-OB_CANCELED] = "HY000";
    STR_ERROR[-OB_CANCELED] = "Operation canceled";
    STR_USER_ERROR[-OB_CANCELED] = "Operation canceled";
    ORACLE_ERRNO[-OB_CANCELED] = 600;
    ORACLE_STR_ERROR[-OB_CANCELED] = "ORA-00600: internal error code, arguments: -4072, Operation canceled";
    ORACLE_STR_USER_ERROR[-OB_CANCELED] = "ORA-00600: internal error code, arguments: -4072, Operation canceled";
    ERROR_NAME[-OB_LOG_SRC_CHANGED] = "OB_LOG_SRC_CHANGED";
    MYSQL_ERRNO[-OB_LOG_SRC_CHANGED] = -1;
    SQLSTATE[-OB_LOG_SRC_CHANGED] = "HY000";
    STR_ERROR[-OB_LOG_SRC_CHANGED] = "Log source changed";
    STR_USER_ERROR[-OB_LOG_SRC_CHANGED] = "Log source changed";
    ORACLE_ERRNO[-OB_LOG_SRC_CHANGED] = 600;
    ORACLE_STR_ERROR[-OB_LOG_SRC_CHANGED] = "ORA-00600: internal error code, arguments: -4073, Log source changed";
    ORACLE_STR_USER_ERROR[-OB_LOG_SRC_CHANGED] = "ORA-00600: internal error code, arguments: -4073, Log source changed";
    ERROR_NAME[-OB_LOG_NOT_ALIGN] = "OB_LOG_NOT_ALIGN";
    MYSQL_ERRNO[-OB_LOG_NOT_ALIGN] = -1;
    SQLSTATE[-OB_LOG_NOT_ALIGN] = "HY000";
    STR_ERROR[-OB_LOG_NOT_ALIGN] = "Log not aligned";
    STR_USER_ERROR[-OB_LOG_NOT_ALIGN] = "Log not aligned";
    ORACLE_ERRNO[-OB_LOG_NOT_ALIGN] = 600;
    ORACLE_STR_ERROR[-OB_LOG_NOT_ALIGN] = "ORA-00600: internal error code, arguments: -4074, Log not aligned";
    ORACLE_STR_USER_ERROR[-OB_LOG_NOT_ALIGN] = "ORA-00600: internal error code, arguments: -4074, Log not aligned";
    ERROR_NAME[-OB_LOG_MISSING] = "OB_LOG_MISSING";
    MYSQL_ERRNO[-OB_LOG_MISSING] = -1;
    SQLSTATE[-OB_LOG_MISSING] = "HY000";
    STR_ERROR[-OB_LOG_MISSING] = "Log entry missed";
    STR_USER_ERROR[-OB_LOG_MISSING] = "Log entry missed";
    ORACLE_ERRNO[-OB_LOG_MISSING] = 600;
    ORACLE_STR_ERROR[-OB_LOG_MISSING] = "ORA-00600: internal error code, arguments: -4075, Log entry missed";
    ORACLE_STR_USER_ERROR[-OB_LOG_MISSING] = "ORA-00600: internal error code, arguments: -4075, Log entry missed";
    ERROR_NAME[-OB_NEED_WAIT] = "OB_NEED_WAIT";
    MYSQL_ERRNO[-OB_NEED_WAIT] = -1;
    SQLSTATE[-OB_NEED_WAIT] = "HY000";
    STR_ERROR[-OB_NEED_WAIT] = "Need wait";
    STR_USER_ERROR[-OB_NEED_WAIT] = "Need wait";
    ORACLE_ERRNO[-OB_NEED_WAIT] = 600;
    ORACLE_STR_ERROR[-OB_NEED_WAIT] = "ORA-00600: internal error code, arguments: -4076, Need wait";
    ORACLE_STR_USER_ERROR[-OB_NEED_WAIT] = "ORA-00600: internal error code, arguments: -4076, Need wait";
    ERROR_NAME[-OB_NOT_IMPLEMENT] = "OB_NOT_IMPLEMENT";
    MYSQL_ERRNO[-OB_NOT_IMPLEMENT] = -1;
    SQLSTATE[-OB_NOT_IMPLEMENT] = "0A000";
    STR_ERROR[-OB_NOT_IMPLEMENT] = "Not implemented feature";
    STR_USER_ERROR[-OB_NOT_IMPLEMENT] = "Not implemented feature";
    ORACLE_ERRNO[-OB_NOT_IMPLEMENT] = 600;
    ORACLE_STR_ERROR[-OB_NOT_IMPLEMENT] = "ORA-00600: internal error code, arguments: -4077, Not implemented feature";
    ORACLE_STR_USER_ERROR[-OB_NOT_IMPLEMENT] =
        "ORA-00600: internal error code, arguments: -4077, Not implemented feature";
    ERROR_NAME[-OB_DIVISION_BY_ZERO] = "OB_DIVISION_BY_ZERO";
    MYSQL_ERRNO[-OB_DIVISION_BY_ZERO] = ER_DIVISION_BY_ZERO;
    SQLSTATE[-OB_DIVISION_BY_ZERO] = "42000";
    STR_ERROR[-OB_DIVISION_BY_ZERO] = "Divided by zero";
    STR_USER_ERROR[-OB_DIVISION_BY_ZERO] = "Divided by zero";
    ORACLE_ERRNO[-OB_DIVISION_BY_ZERO] = 600;
    ORACLE_STR_ERROR[-OB_DIVISION_BY_ZERO] = "ORA-00600: internal error code, arguments: -4078, Divided by zero";
    ORACLE_STR_USER_ERROR[-OB_DIVISION_BY_ZERO] = "ORA-00600: internal error code, arguments: -4078, Divided by zero";
    ERROR_NAME[-OB_EXCEED_MEM_LIMIT] = "OB_EXCEED_MEM_LIMIT";
    MYSQL_ERRNO[-OB_EXCEED_MEM_LIMIT] = -1;
    SQLSTATE[-OB_EXCEED_MEM_LIMIT] = "HY013";
    STR_ERROR[-OB_EXCEED_MEM_LIMIT] = "exceed memory limit";
    STR_USER_ERROR[-OB_EXCEED_MEM_LIMIT] = "exceed memory limit";
    ORACLE_ERRNO[-OB_EXCEED_MEM_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_EXCEED_MEM_LIMIT] = "ORA-00600: internal error code, arguments: -4080, exceed memory limit";
    ORACLE_STR_USER_ERROR[-OB_EXCEED_MEM_LIMIT] =
        "ORA-00600: internal error code, arguments: -4080, exceed memory limit";
    ERROR_NAME[-OB_RESULT_UNKNOWN] = "OB_RESULT_UNKNOWN";
    MYSQL_ERRNO[-OB_RESULT_UNKNOWN] = -1;
    SQLSTATE[-OB_RESULT_UNKNOWN] = "HY000";
    STR_ERROR[-OB_RESULT_UNKNOWN] = "Unknown result";
    STR_USER_ERROR[-OB_RESULT_UNKNOWN] = "Unknown result";
    ORACLE_ERRNO[-OB_RESULT_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_RESULT_UNKNOWN] = "ORA-00600: internal error code, arguments: -4081, Unknown result";
    ORACLE_STR_USER_ERROR[-OB_RESULT_UNKNOWN] = "ORA-00600: internal error code, arguments: -4081, Unknown result";
    ERROR_NAME[-OB_NO_RESULT] = "OB_NO_RESULT";
    MYSQL_ERRNO[-OB_NO_RESULT] = -1;
    SQLSTATE[-OB_NO_RESULT] = "02000";
    STR_ERROR[-OB_NO_RESULT] = "No result";
    STR_USER_ERROR[-OB_NO_RESULT] = "No result";
    ORACLE_ERRNO[-OB_NO_RESULT] = 600;
    ORACLE_STR_ERROR[-OB_NO_RESULT] = "ORA-00600: internal error code, arguments: -4084, No result";
    ORACLE_STR_USER_ERROR[-OB_NO_RESULT] = "ORA-00600: internal error code, arguments: -4084, No result";
    ERROR_NAME[-OB_QUEUE_OVERFLOW] = "OB_QUEUE_OVERFLOW";
    MYSQL_ERRNO[-OB_QUEUE_OVERFLOW] = -1;
    SQLSTATE[-OB_QUEUE_OVERFLOW] = "HY000";
    STR_ERROR[-OB_QUEUE_OVERFLOW] = "Queue overflow";
    STR_USER_ERROR[-OB_QUEUE_OVERFLOW] = "Queue overflow";
    ORACLE_ERRNO[-OB_QUEUE_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_QUEUE_OVERFLOW] = "ORA-00600: internal error code, arguments: -4085, Queue overflow";
    ORACLE_STR_USER_ERROR[-OB_QUEUE_OVERFLOW] = "ORA-00600: internal error code, arguments: -4085, Queue overflow";
    ERROR_NAME[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] = "OB_LOG_ID_RANGE_NOT_CONTINUOUS";
    MYSQL_ERRNO[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] = -1;
    SQLSTATE[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] = "HY000";
    STR_ERROR[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] = "Table log_id range no continuous";
    STR_USER_ERROR[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] = "Table log_id range no continuous";
    ORACLE_ERRNO[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] =
        "ORA-00600: internal error code, arguments: -4090, Table log_id range no continuous";
    ORACLE_STR_USER_ERROR[-OB_LOG_ID_RANGE_NOT_CONTINUOUS] =
        "ORA-00600: internal error code, arguments: -4090, Table log_id range no continuous";
    ERROR_NAME[-OB_TERM_LAGGED] = "OB_TERM_LAGGED";
    MYSQL_ERRNO[-OB_TERM_LAGGED] = -1;
    SQLSTATE[-OB_TERM_LAGGED] = "HY000";
    STR_ERROR[-OB_TERM_LAGGED] = "Term lagged";
    STR_USER_ERROR[-OB_TERM_LAGGED] = "Term lagged";
    ORACLE_ERRNO[-OB_TERM_LAGGED] = 600;
    ORACLE_STR_ERROR[-OB_TERM_LAGGED] = "ORA-00600: internal error code, arguments: -4097, Term lagged";
    ORACLE_STR_USER_ERROR[-OB_TERM_LAGGED] = "ORA-00600: internal error code, arguments: -4097, Term lagged";
    ERROR_NAME[-OB_TERM_NOT_MATCH] = "OB_TERM_NOT_MATCH";
    MYSQL_ERRNO[-OB_TERM_NOT_MATCH] = -1;
    SQLSTATE[-OB_TERM_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_TERM_NOT_MATCH] = "Term not match";
    STR_USER_ERROR[-OB_TERM_NOT_MATCH] = "Term not match";
    ORACLE_ERRNO[-OB_TERM_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_TERM_NOT_MATCH] = "ORA-00600: internal error code, arguments: -4098, Term not match";
    ORACLE_STR_USER_ERROR[-OB_TERM_NOT_MATCH] = "ORA-00600: internal error code, arguments: -4098, Term not match";
    ERROR_NAME[-OB_START_LOG_CURSOR_INVALID] = "OB_START_LOG_CURSOR_INVALID";
    MYSQL_ERRNO[-OB_START_LOG_CURSOR_INVALID] = -1;
    SQLSTATE[-OB_START_LOG_CURSOR_INVALID] = "HY000";
    STR_ERROR[-OB_START_LOG_CURSOR_INVALID] = "Invalid log cursor";
    STR_USER_ERROR[-OB_START_LOG_CURSOR_INVALID] = "Invalid log cursor";
    ORACLE_ERRNO[-OB_START_LOG_CURSOR_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_START_LOG_CURSOR_INVALID] =
        "ORA-00600: internal error code, arguments: -4099, Invalid log cursor";
    ORACLE_STR_USER_ERROR[-OB_START_LOG_CURSOR_INVALID] =
        "ORA-00600: internal error code, arguments: -4099, Invalid log cursor";
    ERROR_NAME[-OB_LOCK_NOT_MATCH] = "OB_LOCK_NOT_MATCH";
    MYSQL_ERRNO[-OB_LOCK_NOT_MATCH] = -1;
    SQLSTATE[-OB_LOCK_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_LOCK_NOT_MATCH] = "Lock not match";
    STR_USER_ERROR[-OB_LOCK_NOT_MATCH] = "Lock not match";
    ORACLE_ERRNO[-OB_LOCK_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_LOCK_NOT_MATCH] = "ORA-00600: internal error code, arguments: -4100, Lock not match";
    ORACLE_STR_USER_ERROR[-OB_LOCK_NOT_MATCH] = "ORA-00600: internal error code, arguments: -4100, Lock not match";
    ERROR_NAME[-OB_DEAD_LOCK] = "OB_DEAD_LOCK";
    MYSQL_ERRNO[-OB_DEAD_LOCK] = ER_LOCK_DEADLOCK;
    SQLSTATE[-OB_DEAD_LOCK] = "HY000";
    STR_ERROR[-OB_DEAD_LOCK] = "Deadlock";
    STR_USER_ERROR[-OB_DEAD_LOCK] = "Deadlock";
    ORACLE_ERRNO[-OB_DEAD_LOCK] = 60;
    ORACLE_STR_ERROR[-OB_DEAD_LOCK] = "ORA-00060: deadlock detected while waiting for resource";
    ORACLE_STR_USER_ERROR[-OB_DEAD_LOCK] = "ORA-00060: deadlock detected while waiting for resource";
    ERROR_NAME[-OB_PARTIAL_LOG] = "OB_PARTIAL_LOG";
    MYSQL_ERRNO[-OB_PARTIAL_LOG] = -1;
    SQLSTATE[-OB_PARTIAL_LOG] = "HY000";
    STR_ERROR[-OB_PARTIAL_LOG] = "Incomplete log entry";
    STR_USER_ERROR[-OB_PARTIAL_LOG] = "Incomplete log entry";
    ORACLE_ERRNO[-OB_PARTIAL_LOG] = 600;
    ORACLE_STR_ERROR[-OB_PARTIAL_LOG] = "ORA-00600: internal error code, arguments: -4102, Incomplete log entry";
    ORACLE_STR_USER_ERROR[-OB_PARTIAL_LOG] = "ORA-00600: internal error code, arguments: -4102, Incomplete log entry";
    ERROR_NAME[-OB_CHECKSUM_ERROR] = "OB_CHECKSUM_ERROR";
    MYSQL_ERRNO[-OB_CHECKSUM_ERROR] = -1;
    SQLSTATE[-OB_CHECKSUM_ERROR] = "42000";
    STR_ERROR[-OB_CHECKSUM_ERROR] = "Data checksum error";
    STR_USER_ERROR[-OB_CHECKSUM_ERROR] = "Data checksum error";
    ORACLE_ERRNO[-OB_CHECKSUM_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_CHECKSUM_ERROR] = "ORA-00600: internal error code, arguments: -4103, Data checksum error";
    ORACLE_STR_USER_ERROR[-OB_CHECKSUM_ERROR] = "ORA-00600: internal error code, arguments: -4103, Data checksum error";
    ERROR_NAME[-OB_INIT_FAIL] = "OB_INIT_FAIL";
    MYSQL_ERRNO[-OB_INIT_FAIL] = -1;
    SQLSTATE[-OB_INIT_FAIL] = "HY000";
    STR_ERROR[-OB_INIT_FAIL] = "Initialize error";
    STR_USER_ERROR[-OB_INIT_FAIL] = "Initialize error";
    ORACLE_ERRNO[-OB_INIT_FAIL] = 600;
    ORACLE_STR_ERROR[-OB_INIT_FAIL] = "ORA-00600: internal error code, arguments: -4104, Initialize error";
    ORACLE_STR_USER_ERROR[-OB_INIT_FAIL] = "ORA-00600: internal error code, arguments: -4104, Initialize error";
    ERROR_NAME[-OB_ROWKEY_ORDER_ERROR] = "OB_ROWKEY_ORDER_ERROR";
    MYSQL_ERRNO[-OB_ROWKEY_ORDER_ERROR] = -1;
    SQLSTATE[-OB_ROWKEY_ORDER_ERROR] = "HY000";
    STR_ERROR[-OB_ROWKEY_ORDER_ERROR] = "Rowkey order error";
    STR_USER_ERROR[-OB_ROWKEY_ORDER_ERROR] = "Rowkey order error";
    ORACLE_ERRNO[-OB_ROWKEY_ORDER_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ROWKEY_ORDER_ERROR] = "ORA-00600: internal error code, arguments: -4105, Rowkey order error";
    ORACLE_STR_USER_ERROR[-OB_ROWKEY_ORDER_ERROR] =
        "ORA-00600: internal error code, arguments: -4105, Rowkey order error";
    ERROR_NAME[-OB_NOT_ENOUGH_STORE] = "OB_NOT_ENOUGH_STORE";
    MYSQL_ERRNO[-OB_NOT_ENOUGH_STORE] = -1;
    SQLSTATE[-OB_NOT_ENOUGH_STORE] = "HY000";
    STR_ERROR[-OB_NOT_ENOUGH_STORE] = "not enough commitlog store";
    STR_USER_ERROR[-OB_NOT_ENOUGH_STORE] = "not enough commitlog store";
    ORACLE_ERRNO[-OB_NOT_ENOUGH_STORE] = 600;
    ORACLE_STR_ERROR[-OB_NOT_ENOUGH_STORE] =
        "ORA-00600: internal error code, arguments: -4106, not enough commitlog store";
    ORACLE_STR_USER_ERROR[-OB_NOT_ENOUGH_STORE] =
        "ORA-00600: internal error code, arguments: -4106, not enough commitlog store";
    ERROR_NAME[-OB_BLOCK_SWITCHED] = "OB_BLOCK_SWITCHED";
    MYSQL_ERRNO[-OB_BLOCK_SWITCHED] = -1;
    SQLSTATE[-OB_BLOCK_SWITCHED] = "HY000";
    STR_ERROR[-OB_BLOCK_SWITCHED] = "block switched when fill commitlog";
    STR_USER_ERROR[-OB_BLOCK_SWITCHED] = "block switched when fill commitlog";
    ORACLE_ERRNO[-OB_BLOCK_SWITCHED] = 600;
    ORACLE_STR_ERROR[-OB_BLOCK_SWITCHED] =
        "ORA-00600: internal error code, arguments: -4107, block switched when fill commitlog";
    ORACLE_STR_USER_ERROR[-OB_BLOCK_SWITCHED] =
        "ORA-00600: internal error code, arguments: -4107, block switched when fill commitlog";
    ERROR_NAME[-OB_PHYSIC_CHECKSUM_ERROR] = "OB_PHYSIC_CHECKSUM_ERROR";
    MYSQL_ERRNO[-OB_PHYSIC_CHECKSUM_ERROR] = -1;
    SQLSTATE[-OB_PHYSIC_CHECKSUM_ERROR] = "42000";
    STR_ERROR[-OB_PHYSIC_CHECKSUM_ERROR] = "Physic data checksum error";
    STR_USER_ERROR[-OB_PHYSIC_CHECKSUM_ERROR] = "Physic data checksum error";
    ORACLE_ERRNO[-OB_PHYSIC_CHECKSUM_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_PHYSIC_CHECKSUM_ERROR] =
        "ORA-00600: internal error code, arguments: -4108, Physic data checksum error";
    ORACLE_STR_USER_ERROR[-OB_PHYSIC_CHECKSUM_ERROR] =
        "ORA-00600: internal error code, arguments: -4108, Physic data checksum error";
    ERROR_NAME[-OB_STATE_NOT_MATCH] = "OB_STATE_NOT_MATCH";
    MYSQL_ERRNO[-OB_STATE_NOT_MATCH] = -1;
    SQLSTATE[-OB_STATE_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_STATE_NOT_MATCH] = "Server state or role not the same as expected";
    STR_USER_ERROR[-OB_STATE_NOT_MATCH] = "Server state or role not the same as expected";
    ORACLE_ERRNO[-OB_STATE_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_STATE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4109, Server state or role not the same as expected";
    ORACLE_STR_USER_ERROR[-OB_STATE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4109, Server state or role not the same as expected";
    ERROR_NAME[-OB_READ_ZERO_LOG] = "OB_READ_ZERO_LOG";
    MYSQL_ERRNO[-OB_READ_ZERO_LOG] = -1;
    SQLSTATE[-OB_READ_ZERO_LOG] = "HY000";
    STR_ERROR[-OB_READ_ZERO_LOG] = "Read zero log";
    STR_USER_ERROR[-OB_READ_ZERO_LOG] = "Read zero log";
    ORACLE_ERRNO[-OB_READ_ZERO_LOG] = 600;
    ORACLE_STR_ERROR[-OB_READ_ZERO_LOG] = "ORA-00600: internal error code, arguments: -4110, Read zero log";
    ORACLE_STR_USER_ERROR[-OB_READ_ZERO_LOG] = "ORA-00600: internal error code, arguments: -4110, Read zero log";
    ERROR_NAME[-OB_BLOCK_NEED_FREEZE] = "OB_BLOCK_NEED_FREEZE";
    MYSQL_ERRNO[-OB_BLOCK_NEED_FREEZE] = -1;
    SQLSTATE[-OB_BLOCK_NEED_FREEZE] = "HY000";
    STR_ERROR[-OB_BLOCK_NEED_FREEZE] = "block need freeze";
    STR_USER_ERROR[-OB_BLOCK_NEED_FREEZE] = "block need freeze";
    ORACLE_ERRNO[-OB_BLOCK_NEED_FREEZE] = 600;
    ORACLE_STR_ERROR[-OB_BLOCK_NEED_FREEZE] = "ORA-00600: internal error code, arguments: -4111, block need freeze";
    ORACLE_STR_USER_ERROR[-OB_BLOCK_NEED_FREEZE] =
        "ORA-00600: internal error code, arguments: -4111, block need freeze";
    ERROR_NAME[-OB_BLOCK_FROZEN] = "OB_BLOCK_FROZEN";
    MYSQL_ERRNO[-OB_BLOCK_FROZEN] = -1;
    SQLSTATE[-OB_BLOCK_FROZEN] = "HY000";
    STR_ERROR[-OB_BLOCK_FROZEN] = "block frozen";
    STR_USER_ERROR[-OB_BLOCK_FROZEN] = "block frozen";
    ORACLE_ERRNO[-OB_BLOCK_FROZEN] = 600;
    ORACLE_STR_ERROR[-OB_BLOCK_FROZEN] = "ORA-00600: internal error code, arguments: -4112, block frozen";
    ORACLE_STR_USER_ERROR[-OB_BLOCK_FROZEN] = "ORA-00600: internal error code, arguments: -4112, block frozen";
    ERROR_NAME[-OB_IN_FATAL_STATE] = "OB_IN_FATAL_STATE";
    MYSQL_ERRNO[-OB_IN_FATAL_STATE] = -1;
    SQLSTATE[-OB_IN_FATAL_STATE] = "HY000";
    STR_ERROR[-OB_IN_FATAL_STATE] = "In FATAL state";
    STR_USER_ERROR[-OB_IN_FATAL_STATE] = "In FATAL state";
    ORACLE_ERRNO[-OB_IN_FATAL_STATE] = 600;
    ORACLE_STR_ERROR[-OB_IN_FATAL_STATE] = "ORA-00600: internal error code, arguments: -4113, In FATAL state";
    ORACLE_STR_USER_ERROR[-OB_IN_FATAL_STATE] = "ORA-00600: internal error code, arguments: -4113, In FATAL state";
    ERROR_NAME[-OB_IN_STOP_STATE] = "OB_IN_STOP_STATE";
    MYSQL_ERRNO[-OB_IN_STOP_STATE] = -1;
    SQLSTATE[-OB_IN_STOP_STATE] = "08S01";
    STR_ERROR[-OB_IN_STOP_STATE] = "In STOP state";
    STR_USER_ERROR[-OB_IN_STOP_STATE] = "In STOP state";
    ORACLE_ERRNO[-OB_IN_STOP_STATE] = 600;
    ORACLE_STR_ERROR[-OB_IN_STOP_STATE] = "ORA-00600: internal error code, arguments: -4114, In STOP state";
    ORACLE_STR_USER_ERROR[-OB_IN_STOP_STATE] = "ORA-00600: internal error code, arguments: -4114, In STOP state";
    ERROR_NAME[-OB_UPS_MASTER_EXISTS] = "OB_UPS_MASTER_EXISTS";
    MYSQL_ERRNO[-OB_UPS_MASTER_EXISTS] = -1;
    SQLSTATE[-OB_UPS_MASTER_EXISTS] = "HY000";
    STR_ERROR[-OB_UPS_MASTER_EXISTS] = "Master UpdateServer already exists";
    STR_USER_ERROR[-OB_UPS_MASTER_EXISTS] = "Master UpdateServer already exists";
    ORACLE_ERRNO[-OB_UPS_MASTER_EXISTS] = 600;
    ORACLE_STR_ERROR[-OB_UPS_MASTER_EXISTS] =
        "ORA-00600: internal error code, arguments: -4115, Master UpdateServer already exists";
    ORACLE_STR_USER_ERROR[-OB_UPS_MASTER_EXISTS] =
        "ORA-00600: internal error code, arguments: -4115, Master UpdateServer already exists";
    ERROR_NAME[-OB_LOG_NOT_CLEAR] = "OB_LOG_NOT_CLEAR";
    MYSQL_ERRNO[-OB_LOG_NOT_CLEAR] = -1;
    SQLSTATE[-OB_LOG_NOT_CLEAR] = "42000";
    STR_ERROR[-OB_LOG_NOT_CLEAR] = "Log not clear";
    STR_USER_ERROR[-OB_LOG_NOT_CLEAR] = "Log not clear";
    ORACLE_ERRNO[-OB_LOG_NOT_CLEAR] = 600;
    ORACLE_STR_ERROR[-OB_LOG_NOT_CLEAR] = "ORA-00600: internal error code, arguments: -4116, Log not clear";
    ORACLE_STR_USER_ERROR[-OB_LOG_NOT_CLEAR] = "ORA-00600: internal error code, arguments: -4116, Log not clear";
    ERROR_NAME[-OB_FILE_ALREADY_EXIST] = "OB_FILE_ALREADY_EXIST";
    MYSQL_ERRNO[-OB_FILE_ALREADY_EXIST] = ER_FILE_EXISTS_ERROR;
    SQLSTATE[-OB_FILE_ALREADY_EXIST] = "58000";
    STR_ERROR[-OB_FILE_ALREADY_EXIST] = "File already exist";
    STR_USER_ERROR[-OB_FILE_ALREADY_EXIST] = "File already exist";
    ORACLE_ERRNO[-OB_FILE_ALREADY_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_FILE_ALREADY_EXIST] = "ORA-00600: internal error code, arguments: -4117, File already exist";
    ORACLE_STR_USER_ERROR[-OB_FILE_ALREADY_EXIST] =
        "ORA-00600: internal error code, arguments: -4117, File already exist";
    ERROR_NAME[-OB_UNKNOWN_PACKET] = "OB_UNKNOWN_PACKET";
    MYSQL_ERRNO[-OB_UNKNOWN_PACKET] = ER_UNKNOWN_COM_ERROR;
    SQLSTATE[-OB_UNKNOWN_PACKET] = "HY001";
    STR_ERROR[-OB_UNKNOWN_PACKET] = "Unknown packet";
    STR_USER_ERROR[-OB_UNKNOWN_PACKET] = "Unknown packet";
    ORACLE_ERRNO[-OB_UNKNOWN_PACKET] = 600;
    ORACLE_STR_ERROR[-OB_UNKNOWN_PACKET] = "ORA-00600: internal error code, arguments: -4118, Unknown packet";
    ORACLE_STR_USER_ERROR[-OB_UNKNOWN_PACKET] = "ORA-00600: internal error code, arguments: -4118, Unknown packet";
    ERROR_NAME[-OB_RPC_PACKET_TOO_LONG] = "OB_RPC_PACKET_TOO_LONG";
    MYSQL_ERRNO[-OB_RPC_PACKET_TOO_LONG] = -1;
    SQLSTATE[-OB_RPC_PACKET_TOO_LONG] = "08000";
    STR_ERROR[-OB_RPC_PACKET_TOO_LONG] = "RPC packet to send too long";
    STR_USER_ERROR[-OB_RPC_PACKET_TOO_LONG] = "RPC packet to send too long";
    ORACLE_ERRNO[-OB_RPC_PACKET_TOO_LONG] = 600;
    ORACLE_STR_ERROR[-OB_RPC_PACKET_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -4119, RPC packet to send too long";
    ORACLE_STR_USER_ERROR[-OB_RPC_PACKET_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -4119, RPC packet to send too long";
    ERROR_NAME[-OB_LOG_TOO_LARGE] = "OB_LOG_TOO_LARGE";
    MYSQL_ERRNO[-OB_LOG_TOO_LARGE] = -1;
    SQLSTATE[-OB_LOG_TOO_LARGE] = "HY000";
    STR_ERROR[-OB_LOG_TOO_LARGE] = "Log too large";
    STR_USER_ERROR[-OB_LOG_TOO_LARGE] = "Log too large";
    ORACLE_ERRNO[-OB_LOG_TOO_LARGE] = 600;
    ORACLE_STR_ERROR[-OB_LOG_TOO_LARGE] = "ORA-00600: internal error code, arguments: -4120, Log too large";
    ORACLE_STR_USER_ERROR[-OB_LOG_TOO_LARGE] = "ORA-00600: internal error code, arguments: -4120, Log too large";
    ERROR_NAME[-OB_RPC_SEND_ERROR] = "OB_RPC_SEND_ERROR";
    MYSQL_ERRNO[-OB_RPC_SEND_ERROR] = 4012;
    SQLSTATE[-OB_RPC_SEND_ERROR] = "HY000";
    STR_ERROR[-OB_RPC_SEND_ERROR] = "RPC send error";
    STR_USER_ERROR[-OB_RPC_SEND_ERROR] = "RPC send error";
    ORACLE_ERRNO[-OB_RPC_SEND_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_RPC_SEND_ERROR] = "ORA-00600: internal error code, arguments: -4121, RPC send error";
    ORACLE_STR_USER_ERROR[-OB_RPC_SEND_ERROR] = "ORA-00600: internal error code, arguments: -4121, RPC send error";
    ERROR_NAME[-OB_RPC_POST_ERROR] = "OB_RPC_POST_ERROR";
    MYSQL_ERRNO[-OB_RPC_POST_ERROR] = 4012;
    SQLSTATE[-OB_RPC_POST_ERROR] = "HY000";
    STR_ERROR[-OB_RPC_POST_ERROR] = "RPC post error";
    STR_USER_ERROR[-OB_RPC_POST_ERROR] = "RPC post error";
    ORACLE_ERRNO[-OB_RPC_POST_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_RPC_POST_ERROR] = "ORA-00600: internal error code, arguments: -4122, RPC post error";
    ORACLE_STR_USER_ERROR[-OB_RPC_POST_ERROR] = "ORA-00600: internal error code, arguments: -4122, RPC post error";
    ERROR_NAME[-OB_LIBEASY_ERROR] = "OB_LIBEASY_ERROR";
    MYSQL_ERRNO[-OB_LIBEASY_ERROR] = -1;
    SQLSTATE[-OB_LIBEASY_ERROR] = "08000";
    STR_ERROR[-OB_LIBEASY_ERROR] = "Libeasy error";
    STR_USER_ERROR[-OB_LIBEASY_ERROR] = "Libeasy error";
    ORACLE_ERRNO[-OB_LIBEASY_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_LIBEASY_ERROR] = "ORA-00600: internal error code, arguments: -4123, Libeasy error";
    ORACLE_STR_USER_ERROR[-OB_LIBEASY_ERROR] = "ORA-00600: internal error code, arguments: -4123, Libeasy error";
    ERROR_NAME[-OB_CONNECT_ERROR] = "OB_CONNECT_ERROR";
    MYSQL_ERRNO[-OB_CONNECT_ERROR] = -1;
    SQLSTATE[-OB_CONNECT_ERROR] = "HY000";
    STR_ERROR[-OB_CONNECT_ERROR] = "Connect error";
    STR_USER_ERROR[-OB_CONNECT_ERROR] = "Connect error";
    ORACLE_ERRNO[-OB_CONNECT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_CONNECT_ERROR] = "ORA-00600: internal error code, arguments: -4124, Connect error";
    ORACLE_STR_USER_ERROR[-OB_CONNECT_ERROR] = "ORA-00600: internal error code, arguments: -4124, Connect error";
    ERROR_NAME[-OB_NOT_FREE] = "OB_NOT_FREE";
    MYSQL_ERRNO[-OB_NOT_FREE] = -1;
    SQLSTATE[-OB_NOT_FREE] = "HY000";
    STR_ERROR[-OB_NOT_FREE] = "Not free";
    STR_USER_ERROR[-OB_NOT_FREE] = "Not free";
    ORACLE_ERRNO[-OB_NOT_FREE] = 600;
    ORACLE_STR_ERROR[-OB_NOT_FREE] = "ORA-00600: internal error code, arguments: -4125, Not free";
    ORACLE_STR_USER_ERROR[-OB_NOT_FREE] = "ORA-00600: internal error code, arguments: -4125, Not free";
    ERROR_NAME[-OB_INIT_SQL_CONTEXT_ERROR] = "OB_INIT_SQL_CONTEXT_ERROR";
    MYSQL_ERRNO[-OB_INIT_SQL_CONTEXT_ERROR] = -1;
    SQLSTATE[-OB_INIT_SQL_CONTEXT_ERROR] = "HY000";
    STR_ERROR[-OB_INIT_SQL_CONTEXT_ERROR] = "Init SQL context error";
    STR_USER_ERROR[-OB_INIT_SQL_CONTEXT_ERROR] = "Init SQL context error";
    ORACLE_ERRNO[-OB_INIT_SQL_CONTEXT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_INIT_SQL_CONTEXT_ERROR] =
        "ORA-00600: internal error code, arguments: -4126, Init SQL context error";
    ORACLE_STR_USER_ERROR[-OB_INIT_SQL_CONTEXT_ERROR] =
        "ORA-00600: internal error code, arguments: -4126, Init SQL context error";
    ERROR_NAME[-OB_SKIP_INVALID_ROW] = "OB_SKIP_INVALID_ROW";
    MYSQL_ERRNO[-OB_SKIP_INVALID_ROW] = -1;
    SQLSTATE[-OB_SKIP_INVALID_ROW] = "42000";
    STR_ERROR[-OB_SKIP_INVALID_ROW] = "Skip invalid row";
    STR_USER_ERROR[-OB_SKIP_INVALID_ROW] = "Skip invalid row";
    ORACLE_ERRNO[-OB_SKIP_INVALID_ROW] = 600;
    ORACLE_STR_ERROR[-OB_SKIP_INVALID_ROW] = "ORA-00600: internal error code, arguments: -4127, Skip invalid row";
    ORACLE_STR_USER_ERROR[-OB_SKIP_INVALID_ROW] = "ORA-00600: internal error code, arguments: -4127, Skip invalid row";
    ERROR_NAME[-OB_RPC_PACKET_INVALID] = "OB_RPC_PACKET_INVALID";
    MYSQL_ERRNO[-OB_RPC_PACKET_INVALID] = -1;
    SQLSTATE[-OB_RPC_PACKET_INVALID] = "HY000";
    STR_ERROR[-OB_RPC_PACKET_INVALID] = "RPC packet is invalid";
    STR_USER_ERROR[-OB_RPC_PACKET_INVALID] = "RPC packet is invalid";
    ORACLE_ERRNO[-OB_RPC_PACKET_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_RPC_PACKET_INVALID] =
        "ORA-00600: internal error code, arguments: -4128, RPC packet is invalid";
    ORACLE_STR_USER_ERROR[-OB_RPC_PACKET_INVALID] =
        "ORA-00600: internal error code, arguments: -4128, RPC packet is invalid";
    ERROR_NAME[-OB_NO_TABLET] = "OB_NO_TABLET";
    MYSQL_ERRNO[-OB_NO_TABLET] = -1;
    SQLSTATE[-OB_NO_TABLET] = "HY000";
    STR_ERROR[-OB_NO_TABLET] = "No tablets";
    STR_USER_ERROR[-OB_NO_TABLET] = "No tablets";
    ORACLE_ERRNO[-OB_NO_TABLET] = 600;
    ORACLE_STR_ERROR[-OB_NO_TABLET] = "ORA-00600: internal error code, arguments: -4133, No tablets";
    ORACLE_STR_USER_ERROR[-OB_NO_TABLET] = "ORA-00600: internal error code, arguments: -4133, No tablets";
    ERROR_NAME[-OB_SNAPSHOT_DISCARDED] = "OB_SNAPSHOT_DISCARDED";
    MYSQL_ERRNO[-OB_SNAPSHOT_DISCARDED] = -1;
    SQLSTATE[-OB_SNAPSHOT_DISCARDED] = "HY000";
    STR_ERROR[-OB_SNAPSHOT_DISCARDED] = "Request to read too old versioned data";
    STR_USER_ERROR[-OB_SNAPSHOT_DISCARDED] = "Request to read too old versioned data";
    ORACLE_ERRNO[-OB_SNAPSHOT_DISCARDED] = 1555;
    ORACLE_STR_ERROR[-OB_SNAPSHOT_DISCARDED] = "ORA-01555: snapshot too old";
    ORACLE_STR_USER_ERROR[-OB_SNAPSHOT_DISCARDED] = "ORA-01555: snapshot too old";
    ERROR_NAME[-OB_DATA_NOT_UPTODATE] = "OB_DATA_NOT_UPTODATE";
    MYSQL_ERRNO[-OB_DATA_NOT_UPTODATE] = -1;
    SQLSTATE[-OB_DATA_NOT_UPTODATE] = "HY000";
    STR_ERROR[-OB_DATA_NOT_UPTODATE] = "State is stale";
    STR_USER_ERROR[-OB_DATA_NOT_UPTODATE] = "State is stale";
    ORACLE_ERRNO[-OB_DATA_NOT_UPTODATE] = 600;
    ORACLE_STR_ERROR[-OB_DATA_NOT_UPTODATE] = "ORA-00600: internal error code, arguments: -4139, State is stale";
    ORACLE_STR_USER_ERROR[-OB_DATA_NOT_UPTODATE] = "ORA-00600: internal error code, arguments: -4139, State is stale";
    ERROR_NAME[-OB_ROW_MODIFIED] = "OB_ROW_MODIFIED";
    MYSQL_ERRNO[-OB_ROW_MODIFIED] = -1;
    SQLSTATE[-OB_ROW_MODIFIED] = "HY000";
    STR_ERROR[-OB_ROW_MODIFIED] = "Row modified";
    STR_USER_ERROR[-OB_ROW_MODIFIED] = "Row modified";
    ORACLE_ERRNO[-OB_ROW_MODIFIED] = 600;
    ORACLE_STR_ERROR[-OB_ROW_MODIFIED] = "ORA-00600: internal error code, arguments: -4142, Row modified";
    ORACLE_STR_USER_ERROR[-OB_ROW_MODIFIED] = "ORA-00600: internal error code, arguments: -4142, Row modified";
    ERROR_NAME[-OB_VERSION_NOT_MATCH] = "OB_VERSION_NOT_MATCH";
    MYSQL_ERRNO[-OB_VERSION_NOT_MATCH] = -1;
    SQLSTATE[-OB_VERSION_NOT_MATCH] = "42000";
    STR_ERROR[-OB_VERSION_NOT_MATCH] = "Version not match";
    STR_USER_ERROR[-OB_VERSION_NOT_MATCH] = "Version not match";
    ORACLE_ERRNO[-OB_VERSION_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_VERSION_NOT_MATCH] = "ORA-00600: internal error code, arguments: -4143, Version not match";
    ORACLE_STR_USER_ERROR[-OB_VERSION_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4143, Version not match";
    ERROR_NAME[-OB_BAD_ADDRESS] = "OB_BAD_ADDRESS";
    MYSQL_ERRNO[-OB_BAD_ADDRESS] = -1;
    SQLSTATE[-OB_BAD_ADDRESS] = "42000";
    STR_ERROR[-OB_BAD_ADDRESS] = "Bad address";
    STR_USER_ERROR[-OB_BAD_ADDRESS] = "Bad address";
    ORACLE_ERRNO[-OB_BAD_ADDRESS] = 600;
    ORACLE_STR_ERROR[-OB_BAD_ADDRESS] = "ORA-00600: internal error code, arguments: -4144, Bad address";
    ORACLE_STR_USER_ERROR[-OB_BAD_ADDRESS] = "ORA-00600: internal error code, arguments: -4144, Bad address";
    ERROR_NAME[-OB_ENQUEUE_FAILED] = "OB_ENQUEUE_FAILED";
    MYSQL_ERRNO[-OB_ENQUEUE_FAILED] = -1;
    SQLSTATE[-OB_ENQUEUE_FAILED] = "HY000";
    STR_ERROR[-OB_ENQUEUE_FAILED] = "Enqueue error";
    STR_USER_ERROR[-OB_ENQUEUE_FAILED] = "Enqueue error";
    ORACLE_ERRNO[-OB_ENQUEUE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_ENQUEUE_FAILED] = "ORA-00600: internal error code, arguments: -4146, Enqueue error";
    ORACLE_STR_USER_ERROR[-OB_ENQUEUE_FAILED] = "ORA-00600: internal error code, arguments: -4146, Enqueue error";
    ERROR_NAME[-OB_INVALID_CONFIG] = "OB_INVALID_CONFIG";
    MYSQL_ERRNO[-OB_INVALID_CONFIG] = -1;
    SQLSTATE[-OB_INVALID_CONFIG] = "HY000";
    STR_ERROR[-OB_INVALID_CONFIG] = "Invalid config";
    STR_USER_ERROR[-OB_INVALID_CONFIG] = "%s";
    ORACLE_ERRNO[-OB_INVALID_CONFIG] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_CONFIG] = "ORA-00600: internal error code, arguments: -4147, Invalid config";
    ORACLE_STR_USER_ERROR[-OB_INVALID_CONFIG] = "ORA-00600: internal error code, arguments: -4147, %s";
    ERROR_NAME[-OB_STMT_EXPIRED] = "OB_STMT_EXPIRED";
    MYSQL_ERRNO[-OB_STMT_EXPIRED] = -1;
    SQLSTATE[-OB_STMT_EXPIRED] = "HY000";
    STR_ERROR[-OB_STMT_EXPIRED] = "Expired statement";
    STR_USER_ERROR[-OB_STMT_EXPIRED] = "Expired statement";
    ORACLE_ERRNO[-OB_STMT_EXPIRED] = 600;
    ORACLE_STR_ERROR[-OB_STMT_EXPIRED] = "ORA-00600: internal error code, arguments: -4149, Expired statement";
    ORACLE_STR_USER_ERROR[-OB_STMT_EXPIRED] = "ORA-00600: internal error code, arguments: -4149, Expired statement";
    ERROR_NAME[-OB_ERR_MIN_VALUE] = "OB_ERR_MIN_VALUE";
    MYSQL_ERRNO[-OB_ERR_MIN_VALUE] = -1;
    SQLSTATE[-OB_ERR_MIN_VALUE] = "42000";
    STR_ERROR[-OB_ERR_MIN_VALUE] = "Min value";
    STR_USER_ERROR[-OB_ERR_MIN_VALUE] = "Min value";
    ORACLE_ERRNO[-OB_ERR_MIN_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MIN_VALUE] = "ORA-00600: internal error code, arguments: -4150, Min value";
    ORACLE_STR_USER_ERROR[-OB_ERR_MIN_VALUE] = "ORA-00600: internal error code, arguments: -4150, Min value";
    ERROR_NAME[-OB_ERR_MAX_VALUE] = "OB_ERR_MAX_VALUE";
    MYSQL_ERRNO[-OB_ERR_MAX_VALUE] = -1;
    SQLSTATE[-OB_ERR_MAX_VALUE] = "42000";
    STR_ERROR[-OB_ERR_MAX_VALUE] = "Max value";
    STR_USER_ERROR[-OB_ERR_MAX_VALUE] = "Max value";
    ORACLE_ERRNO[-OB_ERR_MAX_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MAX_VALUE] = "ORA-00600: internal error code, arguments: -4151, Max value";
    ORACLE_STR_USER_ERROR[-OB_ERR_MAX_VALUE] = "ORA-00600: internal error code, arguments: -4151, Max value";
    ERROR_NAME[-OB_ERR_NULL_VALUE] = "OB_ERR_NULL_VALUE";
    MYSQL_ERRNO[-OB_ERR_NULL_VALUE] = -1;
    SQLSTATE[-OB_ERR_NULL_VALUE] = "42000";
    STR_ERROR[-OB_ERR_NULL_VALUE] = "Null value";
    STR_USER_ERROR[-OB_ERR_NULL_VALUE] = "%s";
    ORACLE_ERRNO[-OB_ERR_NULL_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NULL_VALUE] = "ORA-00600: internal error code, arguments: -4152, Null value";
    ORACLE_STR_USER_ERROR[-OB_ERR_NULL_VALUE] = "ORA-00600: internal error code, arguments: -4152, %s";
    ERROR_NAME[-OB_RESOURCE_OUT] = "OB_RESOURCE_OUT";
    MYSQL_ERRNO[-OB_RESOURCE_OUT] = ER_OUT_OF_RESOURCES;
    SQLSTATE[-OB_RESOURCE_OUT] = "53000";
    STR_ERROR[-OB_RESOURCE_OUT] = "Out of resource";
    STR_USER_ERROR[-OB_RESOURCE_OUT] = "Out of resource";
    ORACLE_ERRNO[-OB_RESOURCE_OUT] = 600;
    ORACLE_STR_ERROR[-OB_RESOURCE_OUT] = "ORA-00600: internal error code, arguments: -4153, Out of resource";
    ORACLE_STR_USER_ERROR[-OB_RESOURCE_OUT] = "ORA-00600: internal error code, arguments: -4153, Out of resource";
    ERROR_NAME[-OB_ERR_SQL_CLIENT] = "OB_ERR_SQL_CLIENT";
    MYSQL_ERRNO[-OB_ERR_SQL_CLIENT] = -1;
    SQLSTATE[-OB_ERR_SQL_CLIENT] = "HY000";
    STR_ERROR[-OB_ERR_SQL_CLIENT] = "Internal SQL client error";
    STR_USER_ERROR[-OB_ERR_SQL_CLIENT] = "Internal SQL client error";
    ORACLE_ERRNO[-OB_ERR_SQL_CLIENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SQL_CLIENT] =
        "ORA-00600: internal error code, arguments: -4154, Internal SQL client error";
    ORACLE_STR_USER_ERROR[-OB_ERR_SQL_CLIENT] =
        "ORA-00600: internal error code, arguments: -4154, Internal SQL client error";
    ERROR_NAME[-OB_META_TABLE_WITHOUT_USE_TABLE] = "OB_META_TABLE_WITHOUT_USE_TABLE";
    MYSQL_ERRNO[-OB_META_TABLE_WITHOUT_USE_TABLE] = -1;
    SQLSTATE[-OB_META_TABLE_WITHOUT_USE_TABLE] = "HY000";
    STR_ERROR[-OB_META_TABLE_WITHOUT_USE_TABLE] = "Meta table without use table";
    STR_USER_ERROR[-OB_META_TABLE_WITHOUT_USE_TABLE] = "Meta table without use table";
    ORACLE_ERRNO[-OB_META_TABLE_WITHOUT_USE_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_META_TABLE_WITHOUT_USE_TABLE] =
        "ORA-00600: internal error code, arguments: -4155, Meta table without use table";
    ORACLE_STR_USER_ERROR[-OB_META_TABLE_WITHOUT_USE_TABLE] =
        "ORA-00600: internal error code, arguments: -4155, Meta table without use table";
    ERROR_NAME[-OB_DISCARD_PACKET] = "OB_DISCARD_PACKET";
    MYSQL_ERRNO[-OB_DISCARD_PACKET] = -1;
    SQLSTATE[-OB_DISCARD_PACKET] = "HY000";
    STR_ERROR[-OB_DISCARD_PACKET] = "Discard packet";
    STR_USER_ERROR[-OB_DISCARD_PACKET] = "Discard packet";
    ORACLE_ERRNO[-OB_DISCARD_PACKET] = 600;
    ORACLE_STR_ERROR[-OB_DISCARD_PACKET] = "ORA-00600: internal error code, arguments: -4156, Discard packet";
    ORACLE_STR_USER_ERROR[-OB_DISCARD_PACKET] = "ORA-00600: internal error code, arguments: -4156, Discard packet";
    ERROR_NAME[-OB_OPERATE_OVERFLOW] = "OB_OPERATE_OVERFLOW";
    MYSQL_ERRNO[-OB_OPERATE_OVERFLOW] = ER_DATA_OUT_OF_RANGE;
    SQLSTATE[-OB_OPERATE_OVERFLOW] = "22003";
    STR_ERROR[-OB_OPERATE_OVERFLOW] = "value is out of range";
    STR_USER_ERROR[-OB_OPERATE_OVERFLOW] = "%s value is out of range in '%s'";
    ORACLE_ERRNO[-OB_OPERATE_OVERFLOW] = 25137;
    ORACLE_STR_ERROR[-OB_OPERATE_OVERFLOW] = "ORA-25137: Data value out of range";
    ORACLE_STR_USER_ERROR[-OB_OPERATE_OVERFLOW] = "ORA-25137: Data value %s out of range in '%s'";
    ERROR_NAME[-OB_INVALID_DATE_FORMAT] = "OB_INVALID_DATE_FORMAT";
    MYSQL_ERRNO[-OB_INVALID_DATE_FORMAT] = ER_TRUNCATED_WRONG_VALUE;
    SQLSTATE[-OB_INVALID_DATE_FORMAT] = "22007";
    STR_ERROR[-OB_INVALID_DATE_FORMAT] = "Incorrect value";
    STR_USER_ERROR[-OB_INVALID_DATE_FORMAT] = "%s=%d must between %d and %d";
    ORACLE_ERRNO[-OB_INVALID_DATE_FORMAT] = 1821;
    ORACLE_STR_ERROR[-OB_INVALID_DATE_FORMAT] = "ORA-01821: date format not recognized";
    ORACLE_STR_USER_ERROR[-OB_INVALID_DATE_FORMAT] =
        "ORA-01821: date format not recognized, %s=%d must between %d and %d";
    ERROR_NAME[-OB_POOL_REGISTERED_FAILED] = "OB_POOL_REGISTERED_FAILED";
    MYSQL_ERRNO[-OB_POOL_REGISTERED_FAILED] = -1;
    SQLSTATE[-OB_POOL_REGISTERED_FAILED] = "HY000";
    STR_ERROR[-OB_POOL_REGISTERED_FAILED] = "register pool failed";
    STR_USER_ERROR[-OB_POOL_REGISTERED_FAILED] = "register pool failed";
    ORACLE_ERRNO[-OB_POOL_REGISTERED_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_POOL_REGISTERED_FAILED] =
        "ORA-00600: internal error code, arguments: -4159, register pool failed";
    ORACLE_STR_USER_ERROR[-OB_POOL_REGISTERED_FAILED] =
        "ORA-00600: internal error code, arguments: -4159, register pool failed";
    ERROR_NAME[-OB_POOL_UNREGISTERED_FAILED] = "OB_POOL_UNREGISTERED_FAILED";
    MYSQL_ERRNO[-OB_POOL_UNREGISTERED_FAILED] = -1;
    SQLSTATE[-OB_POOL_UNREGISTERED_FAILED] = "HY000";
    STR_ERROR[-OB_POOL_UNREGISTERED_FAILED] = "unregister pool failed";
    STR_USER_ERROR[-OB_POOL_UNREGISTERED_FAILED] = "unregister pool failed";
    ORACLE_ERRNO[-OB_POOL_UNREGISTERED_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_POOL_UNREGISTERED_FAILED] =
        "ORA-00600: internal error code, arguments: -4160, unregister pool failed";
    ORACLE_STR_USER_ERROR[-OB_POOL_UNREGISTERED_FAILED] =
        "ORA-00600: internal error code, arguments: -4160, unregister pool failed";
    ERROR_NAME[-OB_INVALID_ARGUMENT_NUM] = "OB_INVALID_ARGUMENT_NUM";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT_NUM] = -1;
    SQLSTATE[-OB_INVALID_ARGUMENT_NUM] = "42000";
    STR_ERROR[-OB_INVALID_ARGUMENT_NUM] = "Invalid argument num";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT_NUM] = "Invalid argument num";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT_NUM] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT_NUM] =
        "ORA-00600: internal error code, arguments: -4161, Invalid argument num";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT_NUM] =
        "ORA-00600: internal error code, arguments: -4161, Invalid argument num";
    ERROR_NAME[-OB_LEASE_NOT_ENOUGH] = "OB_LEASE_NOT_ENOUGH";
    MYSQL_ERRNO[-OB_LEASE_NOT_ENOUGH] = -1;
    SQLSTATE[-OB_LEASE_NOT_ENOUGH] = "HY000";
    STR_ERROR[-OB_LEASE_NOT_ENOUGH] = "reserved lease not enough";
    STR_USER_ERROR[-OB_LEASE_NOT_ENOUGH] = "reserved lease not enough";
    ORACLE_ERRNO[-OB_LEASE_NOT_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_LEASE_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4162, reserved lease not enough";
    ORACLE_STR_USER_ERROR[-OB_LEASE_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4162, reserved lease not enough";
    ERROR_NAME[-OB_LEASE_NOT_MATCH] = "OB_LEASE_NOT_MATCH";
    MYSQL_ERRNO[-OB_LEASE_NOT_MATCH] = -1;
    SQLSTATE[-OB_LEASE_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_LEASE_NOT_MATCH] = "ups lease not match with rs";
    STR_USER_ERROR[-OB_LEASE_NOT_MATCH] = "ups lease not match with rs";
    ORACLE_ERRNO[-OB_LEASE_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_LEASE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4163, ups lease not match with rs";
    ORACLE_STR_USER_ERROR[-OB_LEASE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4163, ups lease not match with rs";
    ERROR_NAME[-OB_UPS_SWITCH_NOT_HAPPEN] = "OB_UPS_SWITCH_NOT_HAPPEN";
    MYSQL_ERRNO[-OB_UPS_SWITCH_NOT_HAPPEN] = -1;
    SQLSTATE[-OB_UPS_SWITCH_NOT_HAPPEN] = "HY000";
    STR_ERROR[-OB_UPS_SWITCH_NOT_HAPPEN] = "ups switch not happen";
    STR_USER_ERROR[-OB_UPS_SWITCH_NOT_HAPPEN] = "ups switch not happen";
    ORACLE_ERRNO[-OB_UPS_SWITCH_NOT_HAPPEN] = 600;
    ORACLE_STR_ERROR[-OB_UPS_SWITCH_NOT_HAPPEN] =
        "ORA-00600: internal error code, arguments: -4164, ups switch not happen";
    ORACLE_STR_USER_ERROR[-OB_UPS_SWITCH_NOT_HAPPEN] =
        "ORA-00600: internal error code, arguments: -4164, ups switch not happen";
    ERROR_NAME[-OB_EMPTY_RESULT] = "OB_EMPTY_RESULT";
    MYSQL_ERRNO[-OB_EMPTY_RESULT] = -1;
    SQLSTATE[-OB_EMPTY_RESULT] = "HY000";
    STR_ERROR[-OB_EMPTY_RESULT] = "Empty result";
    STR_USER_ERROR[-OB_EMPTY_RESULT] = "Empty result";
    ORACLE_ERRNO[-OB_EMPTY_RESULT] = 600;
    ORACLE_STR_ERROR[-OB_EMPTY_RESULT] = "ORA-00600: internal error code, arguments: -4165, Empty result";
    ORACLE_STR_USER_ERROR[-OB_EMPTY_RESULT] = "ORA-00600: internal error code, arguments: -4165, Empty result";
    ERROR_NAME[-OB_CACHE_NOT_HIT] = "OB_CACHE_NOT_HIT";
    MYSQL_ERRNO[-OB_CACHE_NOT_HIT] = -1;
    SQLSTATE[-OB_CACHE_NOT_HIT] = "HY000";
    STR_ERROR[-OB_CACHE_NOT_HIT] = "Cache not hit";
    STR_USER_ERROR[-OB_CACHE_NOT_HIT] = "Cache not hit";
    ORACLE_ERRNO[-OB_CACHE_NOT_HIT] = 600;
    ORACLE_STR_ERROR[-OB_CACHE_NOT_HIT] = "ORA-00600: internal error code, arguments: -4166, Cache not hit";
    ORACLE_STR_USER_ERROR[-OB_CACHE_NOT_HIT] = "ORA-00600: internal error code, arguments: -4166, Cache not hit";
    ERROR_NAME[-OB_NESTED_LOOP_NOT_SUPPORT] = "OB_NESTED_LOOP_NOT_SUPPORT";
    MYSQL_ERRNO[-OB_NESTED_LOOP_NOT_SUPPORT] = -1;
    SQLSTATE[-OB_NESTED_LOOP_NOT_SUPPORT] = "HY000";
    STR_ERROR[-OB_NESTED_LOOP_NOT_SUPPORT] = "Nested loop not support";
    STR_USER_ERROR[-OB_NESTED_LOOP_NOT_SUPPORT] = "Nested loop not support";
    ORACLE_ERRNO[-OB_NESTED_LOOP_NOT_SUPPORT] = 600;
    ORACLE_STR_ERROR[-OB_NESTED_LOOP_NOT_SUPPORT] =
        "ORA-00600: internal error code, arguments: -4167, Nested loop not support";
    ORACLE_STR_USER_ERROR[-OB_NESTED_LOOP_NOT_SUPPORT] =
        "ORA-00600: internal error code, arguments: -4167, Nested loop not support";
    ERROR_NAME[-OB_LOG_INVALID_MOD_ID] = "OB_LOG_INVALID_MOD_ID";
    MYSQL_ERRNO[-OB_LOG_INVALID_MOD_ID] = -1;
    SQLSTATE[-OB_LOG_INVALID_MOD_ID] = "HY000";
    STR_ERROR[-OB_LOG_INVALID_MOD_ID] = "Invalid log module id";
    STR_USER_ERROR[-OB_LOG_INVALID_MOD_ID] = "Invalid log module id";
    ORACLE_ERRNO[-OB_LOG_INVALID_MOD_ID] = 600;
    ORACLE_STR_ERROR[-OB_LOG_INVALID_MOD_ID] =
        "ORA-00600: internal error code, arguments: -4168, Invalid log module id";
    ORACLE_STR_USER_ERROR[-OB_LOG_INVALID_MOD_ID] =
        "ORA-00600: internal error code, arguments: -4168, Invalid log module id";
    ERROR_NAME[-OB_LOG_MODULE_UNKNOWN] = "OB_LOG_MODULE_UNKNOWN";
    MYSQL_ERRNO[-OB_LOG_MODULE_UNKNOWN] = -1;
    SQLSTATE[-OB_LOG_MODULE_UNKNOWN] = "HY000";
    STR_ERROR[-OB_LOG_MODULE_UNKNOWN] = "Unknown module name";
    STR_USER_ERROR[-OB_LOG_MODULE_UNKNOWN] =
        "Unknown module name. Invalid Setting:'%.*s'. Syntax:parMod.subMod:level, parMod.subMod:level";
    ORACLE_ERRNO[-OB_LOG_MODULE_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_LOG_MODULE_UNKNOWN] = "ORA-00600: internal error code, arguments: -4169, Unknown module name";
    ORACLE_STR_USER_ERROR[-OB_LOG_MODULE_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -4169, Unknown module name. Invalid Setting:'%.*s'. "
        "Syntax:parMod.subMod:level, parMod.subMod:level";
    ERROR_NAME[-OB_LOG_LEVEL_INVALID] = "OB_LOG_LEVEL_INVALID";
    MYSQL_ERRNO[-OB_LOG_LEVEL_INVALID] = -1;
    SQLSTATE[-OB_LOG_LEVEL_INVALID] = "HY000";
    STR_ERROR[-OB_LOG_LEVEL_INVALID] = "Invalid level";
    STR_USER_ERROR[-OB_LOG_LEVEL_INVALID] =
        "Invalid level. Invalid setting:'%.*s'. Syntax:parMod.subMod:level, parMod.subMod:level";
    ORACLE_ERRNO[-OB_LOG_LEVEL_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_LOG_LEVEL_INVALID] = "ORA-00600: internal error code, arguments: -4170, Invalid level";
    ORACLE_STR_USER_ERROR[-OB_LOG_LEVEL_INVALID] =
        "ORA-00600: internal error code, arguments: -4170, Invalid level. Invalid setting:'%.*s'. "
        "Syntax:parMod.subMod:level, parMod.subMod:level";
    ERROR_NAME[-OB_LOG_PARSER_SYNTAX_ERR] = "OB_LOG_PARSER_SYNTAX_ERR";
    MYSQL_ERRNO[-OB_LOG_PARSER_SYNTAX_ERR] = -1;
    SQLSTATE[-OB_LOG_PARSER_SYNTAX_ERR] = "HY000";
    STR_ERROR[-OB_LOG_PARSER_SYNTAX_ERR] = "Syntax to set log_level error";
    STR_USER_ERROR[-OB_LOG_PARSER_SYNTAX_ERR] =
        "Syntax to set log_level error. Invalid setting:'%.*s'. Syntax:parMod.subMod:level, parMod.subMod:level";
    ORACLE_ERRNO[-OB_LOG_PARSER_SYNTAX_ERR] = 600;
    ORACLE_STR_ERROR[-OB_LOG_PARSER_SYNTAX_ERR] =
        "ORA-00600: internal error code, arguments: -4171, Syntax to set log_level error";
    ORACLE_STR_USER_ERROR[-OB_LOG_PARSER_SYNTAX_ERR] =
        "ORA-00600: internal error code, arguments: -4171, Syntax to set log_level error. Invalid setting:'%.*s'. "
        "Syntax:parMod.subMod:level, parMod.subMod:level";
    ERROR_NAME[-OB_INDEX_OUT_OF_RANGE] = "OB_INDEX_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_INDEX_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_INDEX_OUT_OF_RANGE] = "HY000";
    STR_ERROR[-OB_INDEX_OUT_OF_RANGE] = "Index out of range";
    STR_USER_ERROR[-OB_INDEX_OUT_OF_RANGE] = "Index out of range";
    ORACLE_ERRNO[-OB_INDEX_OUT_OF_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_INDEX_OUT_OF_RANGE] = "ORA-00600: internal error code, arguments: -4172, Index out of range";
    ORACLE_STR_USER_ERROR[-OB_INDEX_OUT_OF_RANGE] =
        "ORA-00600: internal error code, arguments: -4172, Index out of range";
    ERROR_NAME[-OB_INT_UNDERFLOW] = "OB_INT_UNDERFLOW";
    MYSQL_ERRNO[-OB_INT_UNDERFLOW] = -1;
    SQLSTATE[-OB_INT_UNDERFLOW] = "HY000";
    STR_ERROR[-OB_INT_UNDERFLOW] = "Int underflow";
    STR_USER_ERROR[-OB_INT_UNDERFLOW] = "Int underflow";
    ORACLE_ERRNO[-OB_INT_UNDERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_INT_UNDERFLOW] = "ORA-00600: internal error code, arguments: -4173, Int underflow";
    ORACLE_STR_USER_ERROR[-OB_INT_UNDERFLOW] = "ORA-00600: internal error code, arguments: -4173, Int underflow";
    ERROR_NAME[-OB_UNKNOWN_CONNECTION] = "OB_UNKNOWN_CONNECTION";
    MYSQL_ERRNO[-OB_UNKNOWN_CONNECTION] = ER_NO_SUCH_THREAD;
    SQLSTATE[-OB_UNKNOWN_CONNECTION] = "HY000";
    STR_ERROR[-OB_UNKNOWN_CONNECTION] = "Unknown thread id";
    STR_USER_ERROR[-OB_UNKNOWN_CONNECTION] = "Unknown thread id: %lu";
    ORACLE_ERRNO[-OB_UNKNOWN_CONNECTION] = 600;
    ORACLE_STR_ERROR[-OB_UNKNOWN_CONNECTION] = "ORA-00600: internal error code, arguments: -4174, Unknown thread id";
    ORACLE_STR_USER_ERROR[-OB_UNKNOWN_CONNECTION] =
        "ORA-00600: internal error code, arguments: -4174, Unknown thread id: %lu";
    ERROR_NAME[-OB_ERROR_OUT_OF_RANGE] = "OB_ERROR_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ERROR_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_ERROR_OUT_OF_RANGE] = "42000";
    STR_ERROR[-OB_ERROR_OUT_OF_RANGE] = "Out of range";
    STR_USER_ERROR[-OB_ERROR_OUT_OF_RANGE] = "Out of range";
    ORACLE_ERRNO[-OB_ERROR_OUT_OF_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_ERROR_OUT_OF_RANGE] = "ORA-00600: internal error code, arguments: -4175, Out of range";
    ORACLE_STR_USER_ERROR[-OB_ERROR_OUT_OF_RANGE] = "ORA-00600: internal error code, arguments: -4175, Out of range";
    ERROR_NAME[-OB_CACHE_SHRINK_FAILED] = "OB_CACHE_SHRINK_FAILED";
    MYSQL_ERRNO[-OB_CACHE_SHRINK_FAILED] = -1;
    SQLSTATE[-OB_CACHE_SHRINK_FAILED] = "HY001";
    STR_ERROR[-OB_CACHE_SHRINK_FAILED] = "shrink cache failed, no available cache";
    STR_USER_ERROR[-OB_CACHE_SHRINK_FAILED] = "shrink cache failed, no available cache";
    ORACLE_ERRNO[-OB_CACHE_SHRINK_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_CACHE_SHRINK_FAILED] =
        "ORA-00600: internal error code, arguments: -4176, shrink cache failed, no available cache";
    ORACLE_STR_USER_ERROR[-OB_CACHE_SHRINK_FAILED] =
        "ORA-00600: internal error code, arguments: -4176, shrink cache failed, no available cache";
    ERROR_NAME[-OB_OLD_SCHEMA_VERSION] = "OB_OLD_SCHEMA_VERSION";
    MYSQL_ERRNO[-OB_OLD_SCHEMA_VERSION] = -1;
    SQLSTATE[-OB_OLD_SCHEMA_VERSION] = "42000";
    STR_ERROR[-OB_OLD_SCHEMA_VERSION] = "Schema version too old";
    STR_USER_ERROR[-OB_OLD_SCHEMA_VERSION] = "Schema version too old";
    ORACLE_ERRNO[-OB_OLD_SCHEMA_VERSION] = 600;
    ORACLE_STR_ERROR[-OB_OLD_SCHEMA_VERSION] =
        "ORA-00600: internal error code, arguments: -4177, Schema version too old";
    ORACLE_STR_USER_ERROR[-OB_OLD_SCHEMA_VERSION] =
        "ORA-00600: internal error code, arguments: -4177, Schema version too old";
    ERROR_NAME[-OB_RELEASE_SCHEMA_ERROR] = "OB_RELEASE_SCHEMA_ERROR";
    MYSQL_ERRNO[-OB_RELEASE_SCHEMA_ERROR] = -1;
    SQLSTATE[-OB_RELEASE_SCHEMA_ERROR] = "HY000";
    STR_ERROR[-OB_RELEASE_SCHEMA_ERROR] = "Release schema error";
    STR_USER_ERROR[-OB_RELEASE_SCHEMA_ERROR] = "Release schema error";
    ORACLE_ERRNO[-OB_RELEASE_SCHEMA_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_RELEASE_SCHEMA_ERROR] =
        "ORA-00600: internal error code, arguments: -4178, Release schema error";
    ORACLE_STR_USER_ERROR[-OB_RELEASE_SCHEMA_ERROR] =
        "ORA-00600: internal error code, arguments: -4178, Release schema error";
    ERROR_NAME[-OB_OP_NOT_ALLOW] = "OB_OP_NOT_ALLOW";
    MYSQL_ERRNO[-OB_OP_NOT_ALLOW] = -1;
    SQLSTATE[-OB_OP_NOT_ALLOW] = "HY000";
    STR_ERROR[-OB_OP_NOT_ALLOW] = "Operation not allowed now";
    STR_USER_ERROR[-OB_OP_NOT_ALLOW] = "%s not allowed";
    ORACLE_ERRNO[-OB_OP_NOT_ALLOW] = 600;
    ORACLE_STR_ERROR[-OB_OP_NOT_ALLOW] = "ORA-00600: internal error code, arguments: -4179, Operation not allowed now";
    ORACLE_STR_USER_ERROR[-OB_OP_NOT_ALLOW] = "ORA-00600: internal error code, arguments: -4179, %s not allowed";
    ERROR_NAME[-OB_NO_EMPTY_ENTRY] = "OB_NO_EMPTY_ENTRY";
    MYSQL_ERRNO[-OB_NO_EMPTY_ENTRY] = -1;
    SQLSTATE[-OB_NO_EMPTY_ENTRY] = "HY000";
    STR_ERROR[-OB_NO_EMPTY_ENTRY] = "No empty entry";
    STR_USER_ERROR[-OB_NO_EMPTY_ENTRY] = "No empty entry";
    ORACLE_ERRNO[-OB_NO_EMPTY_ENTRY] = 600;
    ORACLE_STR_ERROR[-OB_NO_EMPTY_ENTRY] = "ORA-00600: internal error code, arguments: -4180, No empty entry";
    ORACLE_STR_USER_ERROR[-OB_NO_EMPTY_ENTRY] = "ORA-00600: internal error code, arguments: -4180, No empty entry";
    ERROR_NAME[-OB_ERR_ALREADY_EXISTS] = "OB_ERR_ALREADY_EXISTS";
    MYSQL_ERRNO[-OB_ERR_ALREADY_EXISTS] = -1;
    SQLSTATE[-OB_ERR_ALREADY_EXISTS] = "42S01";
    STR_ERROR[-OB_ERR_ALREADY_EXISTS] = "Already exist";
    STR_USER_ERROR[-OB_ERR_ALREADY_EXISTS] = "Already exist";
    ORACLE_ERRNO[-OB_ERR_ALREADY_EXISTS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ALREADY_EXISTS] = "ORA-00600: internal error code, arguments: -4181, Already exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_ALREADY_EXISTS] = "ORA-00600: internal error code, arguments: -4181, Already exist";
    ERROR_NAME[-OB_SEARCH_NOT_FOUND] = "OB_SEARCH_NOT_FOUND";
    MYSQL_ERRNO[-OB_SEARCH_NOT_FOUND] = -1;
    SQLSTATE[-OB_SEARCH_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_SEARCH_NOT_FOUND] = "Value not found";
    STR_USER_ERROR[-OB_SEARCH_NOT_FOUND] = "Value not found";
    ORACLE_ERRNO[-OB_SEARCH_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_SEARCH_NOT_FOUND] = "ORA-00600: internal error code, arguments: -4182, Value not found";
    ORACLE_STR_USER_ERROR[-OB_SEARCH_NOT_FOUND] = "ORA-00600: internal error code, arguments: -4182, Value not found";
    ERROR_NAME[-OB_BEYOND_THE_RANGE] = "OB_BEYOND_THE_RANGE";
    MYSQL_ERRNO[-OB_BEYOND_THE_RANGE] = -1;
    SQLSTATE[-OB_BEYOND_THE_RANGE] = "HY000";
    STR_ERROR[-OB_BEYOND_THE_RANGE] = "Key out of range";
    STR_USER_ERROR[-OB_BEYOND_THE_RANGE] = "Key out of range";
    ORACLE_ERRNO[-OB_BEYOND_THE_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_BEYOND_THE_RANGE] = "ORA-00600: internal error code, arguments: -4183, Key out of range";
    ORACLE_STR_USER_ERROR[-OB_BEYOND_THE_RANGE] = "ORA-00600: internal error code, arguments: -4183, Key out of range";
    ERROR_NAME[-OB_CS_OUTOF_DISK_SPACE] = "OB_CS_OUTOF_DISK_SPACE";
    MYSQL_ERRNO[-OB_CS_OUTOF_DISK_SPACE] = -1;
    SQLSTATE[-OB_CS_OUTOF_DISK_SPACE] = "53100";
    STR_ERROR[-OB_CS_OUTOF_DISK_SPACE] = "ChunkServer out of disk space";
    STR_USER_ERROR[-OB_CS_OUTOF_DISK_SPACE] = "ChunkServer out of disk space";
    ORACLE_ERRNO[-OB_CS_OUTOF_DISK_SPACE] = 600;
    ORACLE_STR_ERROR[-OB_CS_OUTOF_DISK_SPACE] =
        "ORA-00600: internal error code, arguments: -4184, ChunkServer out of disk space";
    ORACLE_STR_USER_ERROR[-OB_CS_OUTOF_DISK_SPACE] =
        "ORA-00600: internal error code, arguments: -4184, ChunkServer out of disk space";
    ERROR_NAME[-OB_COLUMN_GROUP_NOT_FOUND] = "OB_COLUMN_GROUP_NOT_FOUND";
    MYSQL_ERRNO[-OB_COLUMN_GROUP_NOT_FOUND] = -1;
    SQLSTATE[-OB_COLUMN_GROUP_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_COLUMN_GROUP_NOT_FOUND] = "Column group not found";
    STR_USER_ERROR[-OB_COLUMN_GROUP_NOT_FOUND] = "Column group not found";
    ORACLE_ERRNO[-OB_COLUMN_GROUP_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_COLUMN_GROUP_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -4185, Column group not found";
    ORACLE_STR_USER_ERROR[-OB_COLUMN_GROUP_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -4185, Column group not found";
    ERROR_NAME[-OB_CS_COMPRESS_LIB_ERROR] = "OB_CS_COMPRESS_LIB_ERROR";
    MYSQL_ERRNO[-OB_CS_COMPRESS_LIB_ERROR] = -1;
    SQLSTATE[-OB_CS_COMPRESS_LIB_ERROR] = "HY000";
    STR_ERROR[-OB_CS_COMPRESS_LIB_ERROR] = "ChunkServer failed to get compress library";
    STR_USER_ERROR[-OB_CS_COMPRESS_LIB_ERROR] = "ChunkServer failed to get compress library";
    ORACLE_ERRNO[-OB_CS_COMPRESS_LIB_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_CS_COMPRESS_LIB_ERROR] =
        "ORA-00600: internal error code, arguments: -4186, ChunkServer failed to get compress library";
    ORACLE_STR_USER_ERROR[-OB_CS_COMPRESS_LIB_ERROR] =
        "ORA-00600: internal error code, arguments: -4186, ChunkServer failed to get compress library";
    ERROR_NAME[-OB_ITEM_NOT_MATCH] = "OB_ITEM_NOT_MATCH";
    MYSQL_ERRNO[-OB_ITEM_NOT_MATCH] = -1;
    SQLSTATE[-OB_ITEM_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_ITEM_NOT_MATCH] = "Item not match";
    STR_USER_ERROR[-OB_ITEM_NOT_MATCH] = "Item not match";
    ORACLE_ERRNO[-OB_ITEM_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_ITEM_NOT_MATCH] = "ORA-00600: internal error code, arguments: -4187, Item not match";
    ORACLE_STR_USER_ERROR[-OB_ITEM_NOT_MATCH] = "ORA-00600: internal error code, arguments: -4187, Item not match";
    ERROR_NAME[-OB_SCHEDULER_TASK_CNT_MISMATCH] = "OB_SCHEDULER_TASK_CNT_MISMATCH";
    MYSQL_ERRNO[-OB_SCHEDULER_TASK_CNT_MISMATCH] = -1;
    SQLSTATE[-OB_SCHEDULER_TASK_CNT_MISMATCH] = "HY000";
    STR_ERROR[-OB_SCHEDULER_TASK_CNT_MISMATCH] = "Running task cnt and unfinished task cnt not consistent";
    STR_USER_ERROR[-OB_SCHEDULER_TASK_CNT_MISMATCH] = "Running task cnt and unfinished task cnt not consistent";
    ORACLE_ERRNO[-OB_SCHEDULER_TASK_CNT_MISMATCH] = 600;
    ORACLE_STR_ERROR[-OB_SCHEDULER_TASK_CNT_MISMATCH] =
        "ORA-00600: internal error code, arguments: -4188, Running task cnt and unfinished task cnt not consistent";
    ORACLE_STR_USER_ERROR[-OB_SCHEDULER_TASK_CNT_MISMATCH] =
        "ORA-00600: internal error code, arguments: -4188, Running task cnt and unfinished task cnt not consistent";
    ERROR_NAME[-OB_INVALID_MACRO_BLOCK_TYPE] = "OB_INVALID_MACRO_BLOCK_TYPE";
    MYSQL_ERRNO[-OB_INVALID_MACRO_BLOCK_TYPE] = -1;
    SQLSTATE[-OB_INVALID_MACRO_BLOCK_TYPE] = "HY000";
    STR_ERROR[-OB_INVALID_MACRO_BLOCK_TYPE] = "the macro block type does not exist";
    STR_USER_ERROR[-OB_INVALID_MACRO_BLOCK_TYPE] = "the macro block type does not exist";
    ORACLE_ERRNO[-OB_INVALID_MACRO_BLOCK_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_MACRO_BLOCK_TYPE] =
        "ORA-00600: internal error code, arguments: -4189, the macro block type does not exist";
    ORACLE_STR_USER_ERROR[-OB_INVALID_MACRO_BLOCK_TYPE] =
        "ORA-00600: internal error code, arguments: -4189, the macro block type does not exist";
    ERROR_NAME[-OB_INVALID_DATE_FORMAT_END] = "OB_INVALID_DATE_FORMAT_END";
    MYSQL_ERRNO[-OB_INVALID_DATE_FORMAT_END] = ER_TRUNCATED_WRONG_VALUE;
    SQLSTATE[-OB_INVALID_DATE_FORMAT_END] = "22007";
    STR_ERROR[-OB_INVALID_DATE_FORMAT_END] = "Incorrect value";
    STR_USER_ERROR[-OB_INVALID_DATE_FORMAT_END] = "Incorrect value";
    ORACLE_ERRNO[-OB_INVALID_DATE_FORMAT_END] = 1830;
    ORACLE_STR_ERROR[-OB_INVALID_DATE_FORMAT_END] =
        "ORA-01830: date format picture ends before converting entire input string";
    ORACLE_STR_USER_ERROR[-OB_INVALID_DATE_FORMAT_END] =
        "ORA-01830: date format picture ends before converting entire input string";
    ERROR_NAME[-OB_PG_IS_REMOVED] = "OB_PG_IS_REMOVED";
    MYSQL_ERRNO[-OB_PG_IS_REMOVED] = -1;
    SQLSTATE[-OB_PG_IS_REMOVED] = "HY000";
    STR_ERROR[-OB_PG_IS_REMOVED] = "partition group is removed";
    STR_USER_ERROR[-OB_PG_IS_REMOVED] = "partition group is removed";
    ORACLE_ERRNO[-OB_PG_IS_REMOVED] = 600;
    ORACLE_STR_ERROR[-OB_PG_IS_REMOVED] =
        "ORA-00600: internal error code, arguments: -4191, partition group is removed";
    ORACLE_STR_USER_ERROR[-OB_PG_IS_REMOVED] =
        "ORA-00600: internal error code, arguments: -4191, partition group is removed";
    ERROR_NAME[-OB_HASH_EXIST] = "OB_HASH_EXIST";
    MYSQL_ERRNO[-OB_HASH_EXIST] = -1;
    SQLSTATE[-OB_HASH_EXIST] = "HY000";
    STR_ERROR[-OB_HASH_EXIST] = "hash map/set entry exist";
    STR_USER_ERROR[-OB_HASH_EXIST] = "%s";
    ORACLE_ERRNO[-OB_HASH_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_HASH_EXIST] = "ORA-00600: internal error code, arguments: -4200, hash map/set entry exist";
    ORACLE_STR_USER_ERROR[-OB_HASH_EXIST] = "ORA-00600: internal error code, arguments: -4200, %s";
    ERROR_NAME[-OB_HASH_NOT_EXIST] = "OB_HASH_NOT_EXIST";
    MYSQL_ERRNO[-OB_HASH_NOT_EXIST] = -1;
    SQLSTATE[-OB_HASH_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_HASH_NOT_EXIST] = "hash map/set entry not exist";
    STR_USER_ERROR[-OB_HASH_NOT_EXIST] = "%s";
    ORACLE_ERRNO[-OB_HASH_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_HASH_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4201, hash map/set entry not exist";
    ORACLE_STR_USER_ERROR[-OB_HASH_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4201, %s";
    ERROR_NAME[-OB_HASH_GET_TIMEOUT] = "OB_HASH_GET_TIMEOUT";
    MYSQL_ERRNO[-OB_HASH_GET_TIMEOUT] = -1;
    SQLSTATE[-OB_HASH_GET_TIMEOUT] = "HY000";
    STR_ERROR[-OB_HASH_GET_TIMEOUT] = "hash map/set get timeout";
    STR_USER_ERROR[-OB_HASH_GET_TIMEOUT] = "hash map/set get timeout";
    ORACLE_ERRNO[-OB_HASH_GET_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_HASH_GET_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4204, hash map/set get timeout";
    ORACLE_STR_USER_ERROR[-OB_HASH_GET_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4204, hash map/set get timeout";
    ERROR_NAME[-OB_HASH_PLACEMENT_RETRY] = "OB_HASH_PLACEMENT_RETRY";
    MYSQL_ERRNO[-OB_HASH_PLACEMENT_RETRY] = -1;
    SQLSTATE[-OB_HASH_PLACEMENT_RETRY] = "HY000";
    STR_ERROR[-OB_HASH_PLACEMENT_RETRY] = "hash map/set retry";
    STR_USER_ERROR[-OB_HASH_PLACEMENT_RETRY] = "hash map/set retry";
    ORACLE_ERRNO[-OB_HASH_PLACEMENT_RETRY] = 600;
    ORACLE_STR_ERROR[-OB_HASH_PLACEMENT_RETRY] = "ORA-00600: internal error code, arguments: -4205, hash map/set retry";
    ORACLE_STR_USER_ERROR[-OB_HASH_PLACEMENT_RETRY] =
        "ORA-00600: internal error code, arguments: -4205, hash map/set retry";
    ERROR_NAME[-OB_HASH_FULL] = "OB_HASH_FULL";
    MYSQL_ERRNO[-OB_HASH_FULL] = -1;
    SQLSTATE[-OB_HASH_FULL] = "HY000";
    STR_ERROR[-OB_HASH_FULL] = "hash map/set full";
    STR_USER_ERROR[-OB_HASH_FULL] = "hash map/set full";
    ORACLE_ERRNO[-OB_HASH_FULL] = 600;
    ORACLE_STR_ERROR[-OB_HASH_FULL] = "ORA-00600: internal error code, arguments: -4206, hash map/set full";
    ORACLE_STR_USER_ERROR[-OB_HASH_FULL] = "ORA-00600: internal error code, arguments: -4206, hash map/set full";
    ERROR_NAME[-OB_PACKET_PROCESSED] = "OB_PACKET_PROCESSED";
    MYSQL_ERRNO[-OB_PACKET_PROCESSED] = -1;
    SQLSTATE[-OB_PACKET_PROCESSED] = "HY000";
    STR_ERROR[-OB_PACKET_PROCESSED] = "packet processed";
    STR_USER_ERROR[-OB_PACKET_PROCESSED] = "packet processed";
    ORACLE_ERRNO[-OB_PACKET_PROCESSED] = 600;
    ORACLE_STR_ERROR[-OB_PACKET_PROCESSED] = "ORA-00600: internal error code, arguments: -4207, packet processed";
    ORACLE_STR_USER_ERROR[-OB_PACKET_PROCESSED] = "ORA-00600: internal error code, arguments: -4207, packet processed";
    ERROR_NAME[-OB_WAIT_NEXT_TIMEOUT] = "OB_WAIT_NEXT_TIMEOUT";
    MYSQL_ERRNO[-OB_WAIT_NEXT_TIMEOUT] = -1;
    SQLSTATE[-OB_WAIT_NEXT_TIMEOUT] = "HY000";
    STR_ERROR[-OB_WAIT_NEXT_TIMEOUT] = "wait next packet timeout";
    STR_USER_ERROR[-OB_WAIT_NEXT_TIMEOUT] = "wait next packet timeout";
    ORACLE_ERRNO[-OB_WAIT_NEXT_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_WAIT_NEXT_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4208, wait next packet timeout";
    ORACLE_STR_USER_ERROR[-OB_WAIT_NEXT_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4208, wait next packet timeout";
    ERROR_NAME[-OB_LEADER_NOT_EXIST] = "OB_LEADER_NOT_EXIST";
    MYSQL_ERRNO[-OB_LEADER_NOT_EXIST] = -1;
    SQLSTATE[-OB_LEADER_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_LEADER_NOT_EXIST] = "partition has not leader";
    STR_USER_ERROR[-OB_LEADER_NOT_EXIST] = "partition has not leader";
    ORACLE_ERRNO[-OB_LEADER_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_LEADER_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4209, partition has not leader";
    ORACLE_STR_USER_ERROR[-OB_LEADER_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4209, partition has not leader";
    ERROR_NAME[-OB_PREPARE_MAJOR_FREEZE_FAILED] = "OB_PREPARE_MAJOR_FREEZE_FAILED";
    MYSQL_ERRNO[-OB_PREPARE_MAJOR_FREEZE_FAILED] = -1;
    SQLSTATE[-OB_PREPARE_MAJOR_FREEZE_FAILED] = "HY000";
    STR_ERROR[-OB_PREPARE_MAJOR_FREEZE_FAILED] = "prepare major freeze failed";
    STR_USER_ERROR[-OB_PREPARE_MAJOR_FREEZE_FAILED] = "prepare major freeze failed";
    ORACLE_ERRNO[-OB_PREPARE_MAJOR_FREEZE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_PREPARE_MAJOR_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4210, prepare major freeze failed";
    ORACLE_STR_USER_ERROR[-OB_PREPARE_MAJOR_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4210, prepare major freeze failed";
    ERROR_NAME[-OB_COMMIT_MAJOR_FREEZE_FAILED] = "OB_COMMIT_MAJOR_FREEZE_FAILED";
    MYSQL_ERRNO[-OB_COMMIT_MAJOR_FREEZE_FAILED] = -1;
    SQLSTATE[-OB_COMMIT_MAJOR_FREEZE_FAILED] = "HY000";
    STR_ERROR[-OB_COMMIT_MAJOR_FREEZE_FAILED] = "commit major freeze failed";
    STR_USER_ERROR[-OB_COMMIT_MAJOR_FREEZE_FAILED] = "commit major freeze failed";
    ORACLE_ERRNO[-OB_COMMIT_MAJOR_FREEZE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_COMMIT_MAJOR_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4211, commit major freeze failed";
    ORACLE_STR_USER_ERROR[-OB_COMMIT_MAJOR_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4211, commit major freeze failed";
    ERROR_NAME[-OB_ABORT_MAJOR_FREEZE_FAILED] = "OB_ABORT_MAJOR_FREEZE_FAILED";
    MYSQL_ERRNO[-OB_ABORT_MAJOR_FREEZE_FAILED] = -1;
    SQLSTATE[-OB_ABORT_MAJOR_FREEZE_FAILED] = "HY000";
    STR_ERROR[-OB_ABORT_MAJOR_FREEZE_FAILED] = "abort major freeze failed";
    STR_USER_ERROR[-OB_ABORT_MAJOR_FREEZE_FAILED] = "abort major freeze failed";
    ORACLE_ERRNO[-OB_ABORT_MAJOR_FREEZE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_ABORT_MAJOR_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4212, abort major freeze failed";
    ORACLE_STR_USER_ERROR[-OB_ABORT_MAJOR_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4212, abort major freeze failed";
    ERROR_NAME[-OB_MAJOR_FREEZE_NOT_FINISHED] = "OB_MAJOR_FREEZE_NOT_FINISHED";
    MYSQL_ERRNO[-OB_MAJOR_FREEZE_NOT_FINISHED] = -1;
    SQLSTATE[-OB_MAJOR_FREEZE_NOT_FINISHED] = "HY000";
    STR_ERROR[-OB_MAJOR_FREEZE_NOT_FINISHED] = "last major freeze not finish";
    STR_USER_ERROR[-OB_MAJOR_FREEZE_NOT_FINISHED] = "last major freeze not finish";
    ORACLE_ERRNO[-OB_MAJOR_FREEZE_NOT_FINISHED] = 600;
    ORACLE_STR_ERROR[-OB_MAJOR_FREEZE_NOT_FINISHED] =
        "ORA-00600: internal error code, arguments: -4213, last major freeze not finish";
    ORACLE_STR_USER_ERROR[-OB_MAJOR_FREEZE_NOT_FINISHED] =
        "ORA-00600: internal error code, arguments: -4213, last major freeze not finish";
    ERROR_NAME[-OB_PARTITION_NOT_LEADER] = "OB_PARTITION_NOT_LEADER";
    MYSQL_ERRNO[-OB_PARTITION_NOT_LEADER] = -1;
    SQLSTATE[-OB_PARTITION_NOT_LEADER] = "HY000";
    STR_ERROR[-OB_PARTITION_NOT_LEADER] = "partition is not leader partition";
    STR_USER_ERROR[-OB_PARTITION_NOT_LEADER] = "partition is not leader partition";
    ORACLE_ERRNO[-OB_PARTITION_NOT_LEADER] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_NOT_LEADER] =
        "ORA-00600: internal error code, arguments: -4214, partition is not leader partition";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_NOT_LEADER] =
        "ORA-00600: internal error code, arguments: -4214, partition is not leader partition";
    ERROR_NAME[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] = "OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT";
    MYSQL_ERRNO[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] = -1;
    SQLSTATE[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] = "HY000";
    STR_ERROR[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] = "wait major freeze response timeout";
    STR_USER_ERROR[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] = "wait major freeze response timeout";
    ORACLE_ERRNO[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4215, wait major freeze response timeout";
    ORACLE_STR_USER_ERROR[-OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4215, wait major freeze response timeout";
    ERROR_NAME[-OB_CURL_ERROR] = "OB_CURL_ERROR";
    MYSQL_ERRNO[-OB_CURL_ERROR] = -1;
    SQLSTATE[-OB_CURL_ERROR] = "HY000";
    STR_ERROR[-OB_CURL_ERROR] = "curl error";
    STR_USER_ERROR[-OB_CURL_ERROR] = "curl error";
    ORACLE_ERRNO[-OB_CURL_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_CURL_ERROR] = "ORA-00600: internal error code, arguments: -4216, curl error";
    ORACLE_STR_USER_ERROR[-OB_CURL_ERROR] = "ORA-00600: internal error code, arguments: -4216, curl error";
    ERROR_NAME[-OB_MAJOR_FREEZE_NOT_ALLOW] = "OB_MAJOR_FREEZE_NOT_ALLOW";
    MYSQL_ERRNO[-OB_MAJOR_FREEZE_NOT_ALLOW] = -1;
    SQLSTATE[-OB_MAJOR_FREEZE_NOT_ALLOW] = "HY000";
    STR_ERROR[-OB_MAJOR_FREEZE_NOT_ALLOW] = "Major freeze not allowed now";
    STR_USER_ERROR[-OB_MAJOR_FREEZE_NOT_ALLOW] = "%s";
    ORACLE_ERRNO[-OB_MAJOR_FREEZE_NOT_ALLOW] = 600;
    ORACLE_STR_ERROR[-OB_MAJOR_FREEZE_NOT_ALLOW] =
        "ORA-00600: internal error code, arguments: -4217, Major freeze not allowed now";
    ORACLE_STR_USER_ERROR[-OB_MAJOR_FREEZE_NOT_ALLOW] = "ORA-00600: internal error code, arguments: -4217, %s";
    ERROR_NAME[-OB_PREPARE_FREEZE_FAILED] = "OB_PREPARE_FREEZE_FAILED";
    MYSQL_ERRNO[-OB_PREPARE_FREEZE_FAILED] = -1;
    SQLSTATE[-OB_PREPARE_FREEZE_FAILED] = "HY000";
    STR_ERROR[-OB_PREPARE_FREEZE_FAILED] = "prepare freeze failed";
    STR_USER_ERROR[-OB_PREPARE_FREEZE_FAILED] = "prepare freeze failed";
    ORACLE_ERRNO[-OB_PREPARE_FREEZE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_PREPARE_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4218, prepare freeze failed";
    ORACLE_STR_USER_ERROR[-OB_PREPARE_FREEZE_FAILED] =
        "ORA-00600: internal error code, arguments: -4218, prepare freeze failed";
    ERROR_NAME[-OB_INVALID_DATE_VALUE] = "OB_INVALID_DATE_VALUE";
    MYSQL_ERRNO[-OB_INVALID_DATE_VALUE] = ER_TRUNCATED_WRONG_VALUE;
    SQLSTATE[-OB_INVALID_DATE_VALUE] = "22007";
    STR_ERROR[-OB_INVALID_DATE_VALUE] = "Incorrect value";
    STR_USER_ERROR[-OB_INVALID_DATE_VALUE] = "Incorrect datetime value: '%s' for column '%s'";
    ORACLE_ERRNO[-OB_INVALID_DATE_VALUE] = 1861;
    ORACLE_STR_ERROR[-OB_INVALID_DATE_VALUE] = "ORA-01861: literal does not match format string";
    ORACLE_STR_USER_ERROR[-OB_INVALID_DATE_VALUE] =
        "ORA-01861: literal does not match format string: '%s' for column '%s'";
    ERROR_NAME[-OB_INACTIVE_SQL_CLIENT] = "OB_INACTIVE_SQL_CLIENT";
    MYSQL_ERRNO[-OB_INACTIVE_SQL_CLIENT] = -1;
    SQLSTATE[-OB_INACTIVE_SQL_CLIENT] = "HY000";
    STR_ERROR[-OB_INACTIVE_SQL_CLIENT] = "Inactive sql client, only read allowed";
    STR_USER_ERROR[-OB_INACTIVE_SQL_CLIENT] = "Inactive sql client, only read allowed";
    ORACLE_ERRNO[-OB_INACTIVE_SQL_CLIENT] = 600;
    ORACLE_STR_ERROR[-OB_INACTIVE_SQL_CLIENT] =
        "ORA-00600: internal error code, arguments: -4220, Inactive sql client, only read allowed";
    ORACLE_STR_USER_ERROR[-OB_INACTIVE_SQL_CLIENT] =
        "ORA-00600: internal error code, arguments: -4220, Inactive sql client, only read allowed";
    ERROR_NAME[-OB_INACTIVE_RPC_PROXY] = "OB_INACTIVE_RPC_PROXY";
    MYSQL_ERRNO[-OB_INACTIVE_RPC_PROXY] = -1;
    SQLSTATE[-OB_INACTIVE_RPC_PROXY] = "HY000";
    STR_ERROR[-OB_INACTIVE_RPC_PROXY] = "Inactive rpc proxy, can not send RPC request";
    STR_USER_ERROR[-OB_INACTIVE_RPC_PROXY] = "Inactive rpc proxy, can not send RPC request";
    ORACLE_ERRNO[-OB_INACTIVE_RPC_PROXY] = 600;
    ORACLE_STR_ERROR[-OB_INACTIVE_RPC_PROXY] =
        "ORA-00600: internal error code, arguments: -4221, Inactive rpc proxy, can not send RPC request";
    ORACLE_STR_USER_ERROR[-OB_INACTIVE_RPC_PROXY] =
        "ORA-00600: internal error code, arguments: -4221, Inactive rpc proxy, can not send RPC request";
    ERROR_NAME[-OB_INTERVAL_WITH_MONTH] = "OB_INTERVAL_WITH_MONTH";
    MYSQL_ERRNO[-OB_INTERVAL_WITH_MONTH] = -1;
    SQLSTATE[-OB_INTERVAL_WITH_MONTH] = "42000";
    STR_ERROR[-OB_INTERVAL_WITH_MONTH] = "Interval with year or month can not be converted to microseconds";
    STR_USER_ERROR[-OB_INTERVAL_WITH_MONTH] = "Interval with year or month can not be converted to microseconds";
    ORACLE_ERRNO[-OB_INTERVAL_WITH_MONTH] = 600;
    ORACLE_STR_ERROR[-OB_INTERVAL_WITH_MONTH] = "ORA-00600: internal error code, arguments: -4222, Interval with year "
                                                "or month can not be converted to microseconds";
    ORACLE_STR_USER_ERROR[-OB_INTERVAL_WITH_MONTH] = "ORA-00600: internal error code, arguments: -4222, Interval with "
                                                     "year or month can not be converted to microseconds";
    ERROR_NAME[-OB_TOO_MANY_DATETIME_PARTS] = "OB_TOO_MANY_DATETIME_PARTS";
    MYSQL_ERRNO[-OB_TOO_MANY_DATETIME_PARTS] = -1;
    SQLSTATE[-OB_TOO_MANY_DATETIME_PARTS] = "42000";
    STR_ERROR[-OB_TOO_MANY_DATETIME_PARTS] = "Interval has too many datetime parts";
    STR_USER_ERROR[-OB_TOO_MANY_DATETIME_PARTS] = "Interval has too many datetime parts";
    ORACLE_ERRNO[-OB_TOO_MANY_DATETIME_PARTS] = 600;
    ORACLE_STR_ERROR[-OB_TOO_MANY_DATETIME_PARTS] =
        "ORA-00600: internal error code, arguments: -4223, Interval has too many datetime parts";
    ORACLE_STR_USER_ERROR[-OB_TOO_MANY_DATETIME_PARTS] =
        "ORA-00600: internal error code, arguments: -4223, Interval has too many datetime parts";
    ERROR_NAME[-OB_DATA_OUT_OF_RANGE] = "OB_DATA_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_DATA_OUT_OF_RANGE] = ER_WARN_DATA_OUT_OF_RANGE;
    SQLSTATE[-OB_DATA_OUT_OF_RANGE] = "22003";
    STR_ERROR[-OB_DATA_OUT_OF_RANGE] = "Out of range value for column";
    STR_USER_ERROR[-OB_DATA_OUT_OF_RANGE] = "Out of range value for column '%.*s' at row %ld";
    ORACLE_ERRNO[-OB_DATA_OUT_OF_RANGE] = 1438;
    ORACLE_STR_ERROR[-OB_DATA_OUT_OF_RANGE] =
        "ORA-01438: value larger than specified precision allowed for this column";
    ORACLE_STR_USER_ERROR[-OB_DATA_OUT_OF_RANGE] =
        "ORA-01438: value larger than specified precision allowed for this column '%.*s' at row %ld";
    ERROR_NAME[-OB_PARTITION_NOT_EXIST] = "OB_PARTITION_NOT_EXIST";
    MYSQL_ERRNO[-OB_PARTITION_NOT_EXIST] = -1;
    SQLSTATE[-OB_PARTITION_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_PARTITION_NOT_EXIST] = "Partition entry not exists";
    STR_USER_ERROR[-OB_PARTITION_NOT_EXIST] = "Partition entry not exists";
    ORACLE_ERRNO[-OB_PARTITION_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4225, Partition entry not exists";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4225, Partition entry not exists";
    ERROR_NAME[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] = "OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD";
    MYSQL_ERRNO[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] = ER_TRUNCATED_WRONG_VALUE_FOR_FIELD;
    SQLSTATE[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] = "HY000";
    STR_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] = "Incorrect integer value";
    STR_USER_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] = "Incorrect integer value: '%.*s'";
    ORACLE_ERRNO[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] =
        "ORA-00600: internal error code, arguments: -4226, Incorrect integer value";
    ORACLE_STR_USER_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD] =
        "ORA-00600: internal error code, arguments: -4226, Incorrect integer value: '%.*s'";
    ERROR_NAME[-OB_ERR_NO_DEFAULT_FOR_FIELD] = "OB_ERR_NO_DEFAULT_FOR_FIELD";
    MYSQL_ERRNO[-OB_ERR_NO_DEFAULT_FOR_FIELD] = ER_NO_DEFAULT_FOR_FIELD;
    SQLSTATE[-OB_ERR_NO_DEFAULT_FOR_FIELD] = "HY000";
    STR_ERROR[-OB_ERR_NO_DEFAULT_FOR_FIELD] = "Field doesn't have a default value";
    STR_USER_ERROR[-OB_ERR_NO_DEFAULT_FOR_FIELD] = "Field \'%s\' doesn't have a default value";
    ORACLE_ERRNO[-OB_ERR_NO_DEFAULT_FOR_FIELD] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_DEFAULT_FOR_FIELD] =
        "ORA-00600: internal error code, arguments: -4227, Field doesn't have a default value";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_DEFAULT_FOR_FIELD] =
        "ORA-00600: internal error code, arguments: -4227, Field \'%s\' doesn't have a default value";
    ERROR_NAME[-OB_ERR_FIELD_SPECIFIED_TWICE] = "OB_ERR_FIELD_SPECIFIED_TWICE";
    MYSQL_ERRNO[-OB_ERR_FIELD_SPECIFIED_TWICE] = ER_FIELD_SPECIFIED_TWICE;
    SQLSTATE[-OB_ERR_FIELD_SPECIFIED_TWICE] = "42000";
    STR_ERROR[-OB_ERR_FIELD_SPECIFIED_TWICE] = "Column specified twice";
    STR_USER_ERROR[-OB_ERR_FIELD_SPECIFIED_TWICE] = "Column \'%s\' specified twice";
    ORACLE_ERRNO[-OB_ERR_FIELD_SPECIFIED_TWICE] = 957;
    ORACLE_STR_ERROR[-OB_ERR_FIELD_SPECIFIED_TWICE] = "ORA-00957: duplicate column name";
    ORACLE_STR_USER_ERROR[-OB_ERR_FIELD_SPECIFIED_TWICE] = "ORA-00957: duplicate column name \'%s\'";
    ERROR_NAME[-OB_ERR_TOO_LONG_TABLE_COMMENT] = "OB_ERR_TOO_LONG_TABLE_COMMENT";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_TABLE_COMMENT] = ER_TOO_LONG_TABLE_COMMENT;
    SQLSTATE[-OB_ERR_TOO_LONG_TABLE_COMMENT] = "HY000";
    STR_ERROR[-OB_ERR_TOO_LONG_TABLE_COMMENT] = "Comment for table is too long";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_TABLE_COMMENT] = "Comment for table is too long (max = %ld)";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_TABLE_COMMENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_TABLE_COMMENT] =
        "ORA-00600: internal error code, arguments: -4229, Comment for table is too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_TABLE_COMMENT] =
        "ORA-00600: internal error code, arguments: -4229, Comment for table is too long (max = %ld)";
    ERROR_NAME[-OB_ERR_TOO_LONG_FIELD_COMMENT] = "OB_ERR_TOO_LONG_FIELD_COMMENT";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_FIELD_COMMENT] = ER_TOO_LONG_FIELD_COMMENT;
    SQLSTATE[-OB_ERR_TOO_LONG_FIELD_COMMENT] = "HY000";
    STR_ERROR[-OB_ERR_TOO_LONG_FIELD_COMMENT] = "Comment for field is too long";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_FIELD_COMMENT] = "Comment for field is too long (max = %ld)";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_FIELD_COMMENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_FIELD_COMMENT] =
        "ORA-00600: internal error code, arguments: -4230, Comment for field is too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_FIELD_COMMENT] =
        "ORA-00600: internal error code, arguments: -4230, Comment for field is too long (max = %ld)";
    ERROR_NAME[-OB_ERR_TOO_LONG_INDEX_COMMENT] = "OB_ERR_TOO_LONG_INDEX_COMMENT";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_INDEX_COMMENT] = ER_TOO_LONG_INDEX_COMMENT;
    SQLSTATE[-OB_ERR_TOO_LONG_INDEX_COMMENT] = "HY000";
    STR_ERROR[-OB_ERR_TOO_LONG_INDEX_COMMENT] = "Comment for index is too long";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_INDEX_COMMENT] = "Comment for index is too long (max = %ld)";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_INDEX_COMMENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_INDEX_COMMENT] =
        "ORA-00600: internal error code, arguments: -4231, Comment for index is too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_INDEX_COMMENT] =
        "ORA-00600: internal error code, arguments: -4231, Comment for index is too long (max = %ld)";
    ERROR_NAME[-OB_NOT_FOLLOWER] = "OB_NOT_FOLLOWER";
    MYSQL_ERRNO[-OB_NOT_FOLLOWER] = -1;
    SQLSTATE[-OB_NOT_FOLLOWER] = "HY000";
    STR_ERROR[-OB_NOT_FOLLOWER] = "The observer or zone is not a follower";
    STR_USER_ERROR[-OB_NOT_FOLLOWER] = "The observer or zone is not a follower";
    ORACLE_ERRNO[-OB_NOT_FOLLOWER] = 600;
    ORACLE_STR_ERROR[-OB_NOT_FOLLOWER] =
        "ORA-00600: internal error code, arguments: -4232, The observer or zone is not a follower";
    ORACLE_STR_USER_ERROR[-OB_NOT_FOLLOWER] =
        "ORA-00600: internal error code, arguments: -4232, The observer or zone is not a follower";
    ERROR_NAME[-OB_ERR_OUT_OF_LOWER_BOUND] = "OB_ERR_OUT_OF_LOWER_BOUND";
    MYSQL_ERRNO[-OB_ERR_OUT_OF_LOWER_BOUND] = -1;
    SQLSTATE[-OB_ERR_OUT_OF_LOWER_BOUND] = "HY000";
    STR_ERROR[-OB_ERR_OUT_OF_LOWER_BOUND] = "smaller than container lower bound";
    STR_USER_ERROR[-OB_ERR_OUT_OF_LOWER_BOUND] = "smaller than container lower bound";
    ORACLE_ERRNO[-OB_ERR_OUT_OF_LOWER_BOUND] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OUT_OF_LOWER_BOUND] =
        "ORA-00600: internal error code, arguments: -4233, smaller than container lower bound";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUT_OF_LOWER_BOUND] =
        "ORA-00600: internal error code, arguments: -4233, smaller than container lower bound";
    ERROR_NAME[-OB_ERR_OUT_OF_UPPER_BOUND] = "OB_ERR_OUT_OF_UPPER_BOUND";
    MYSQL_ERRNO[-OB_ERR_OUT_OF_UPPER_BOUND] = -1;
    SQLSTATE[-OB_ERR_OUT_OF_UPPER_BOUND] = "HY000";
    STR_ERROR[-OB_ERR_OUT_OF_UPPER_BOUND] = "bigger than container upper bound";
    STR_USER_ERROR[-OB_ERR_OUT_OF_UPPER_BOUND] = "bigger than container upper bound";
    ORACLE_ERRNO[-OB_ERR_OUT_OF_UPPER_BOUND] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OUT_OF_UPPER_BOUND] =
        "ORA-00600: internal error code, arguments: -4234, bigger than container upper bound";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUT_OF_UPPER_BOUND] =
        "ORA-00600: internal error code, arguments: -4234, bigger than container upper bound";
    ERROR_NAME[-OB_BAD_NULL_ERROR] = "OB_BAD_NULL_ERROR";
    MYSQL_ERRNO[-OB_BAD_NULL_ERROR] = ER_BAD_NULL_ERROR;
    SQLSTATE[-OB_BAD_NULL_ERROR] = "23000";
    STR_ERROR[-OB_BAD_NULL_ERROR] = "Column cannot be null";
    STR_USER_ERROR[-OB_BAD_NULL_ERROR] = "Column '%.*s' cannot be null";
    ORACLE_ERRNO[-OB_BAD_NULL_ERROR] = 1400;
    ORACLE_STR_ERROR[-OB_BAD_NULL_ERROR] = "ORA-01400: cannot insert NULL";
    ORACLE_STR_USER_ERROR[-OB_BAD_NULL_ERROR] = "ORA-01400: cannot insert NULL into '(%.*s)'";
    ERROR_NAME[-OB_OBCONFIG_RETURN_ERROR] = "OB_OBCONFIG_RETURN_ERROR";
    MYSQL_ERRNO[-OB_OBCONFIG_RETURN_ERROR] = -1;
    SQLSTATE[-OB_OBCONFIG_RETURN_ERROR] = "HY000";
    STR_ERROR[-OB_OBCONFIG_RETURN_ERROR] = "ObConfig return error code";
    STR_USER_ERROR[-OB_OBCONFIG_RETURN_ERROR] = "ObConfig return error code";
    ORACLE_ERRNO[-OB_OBCONFIG_RETURN_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_OBCONFIG_RETURN_ERROR] =
        "ORA-00600: internal error code, arguments: -4236, ObConfig return error code";
    ORACLE_STR_USER_ERROR[-OB_OBCONFIG_RETURN_ERROR] =
        "ORA-00600: internal error code, arguments: -4236, ObConfig return error code";
    ERROR_NAME[-OB_OBCONFIG_APPNAME_MISMATCH] = "OB_OBCONFIG_APPNAME_MISMATCH";
    MYSQL_ERRNO[-OB_OBCONFIG_APPNAME_MISMATCH] = -1;
    SQLSTATE[-OB_OBCONFIG_APPNAME_MISMATCH] = "HY000";
    STR_ERROR[-OB_OBCONFIG_APPNAME_MISMATCH] = "Appname mismatch with obconfig result";
    STR_USER_ERROR[-OB_OBCONFIG_APPNAME_MISMATCH] = "Appname mismatch with obconfig result";
    ORACLE_ERRNO[-OB_OBCONFIG_APPNAME_MISMATCH] = 600;
    ORACLE_STR_ERROR[-OB_OBCONFIG_APPNAME_MISMATCH] =
        "ORA-00600: internal error code, arguments: -4237, Appname mismatch with obconfig result";
    ORACLE_STR_USER_ERROR[-OB_OBCONFIG_APPNAME_MISMATCH] =
        "ORA-00600: internal error code, arguments: -4237, Appname mismatch with obconfig result";
    ERROR_NAME[-OB_ERR_VIEW_SELECT_DERIVED] = "OB_ERR_VIEW_SELECT_DERIVED";
    MYSQL_ERRNO[-OB_ERR_VIEW_SELECT_DERIVED] = ER_VIEW_SELECT_DERIVED;
    SQLSTATE[-OB_ERR_VIEW_SELECT_DERIVED] = "HY000";
    STR_ERROR[-OB_ERR_VIEW_SELECT_DERIVED] = "View's SELECT contains a subquery in the FROM clause";
    STR_USER_ERROR[-OB_ERR_VIEW_SELECT_DERIVED] = "View's SELECT contains a subquery in the FROM clause";
    ORACLE_ERRNO[-OB_ERR_VIEW_SELECT_DERIVED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VIEW_SELECT_DERIVED] =
        "ORA-00600: internal error code, arguments: -4238, View's SELECT contains a subquery in the FROM clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_VIEW_SELECT_DERIVED] =
        "ORA-00600: internal error code, arguments: -4238, View's SELECT contains a subquery in the FROM clause";
    ERROR_NAME[-OB_CANT_MJ_PATH] = "OB_CANT_MJ_PATH";
    MYSQL_ERRNO[-OB_CANT_MJ_PATH] = -1;
    SQLSTATE[-OB_CANT_MJ_PATH] = "HY000";
    STR_ERROR[-OB_CANT_MJ_PATH] = "Can not use merge-join to join the tables without join conditions";
    STR_USER_ERROR[-OB_CANT_MJ_PATH] = "Can not use merge-join to join the tables without join conditions";
    ORACLE_ERRNO[-OB_CANT_MJ_PATH] = 600;
    ORACLE_STR_ERROR[-OB_CANT_MJ_PATH] = "ORA-00600: internal error code, arguments: -4239, Can not use merge-join to "
                                         "join the tables without join conditions";
    ORACLE_STR_USER_ERROR[-OB_CANT_MJ_PATH] = "ORA-00600: internal error code, arguments: -4239, Can not use "
                                              "merge-join to join the tables without join conditions";
    ERROR_NAME[-OB_ERR_NO_JOIN_ORDER_GENERATED] = "OB_ERR_NO_JOIN_ORDER_GENERATED";
    MYSQL_ERRNO[-OB_ERR_NO_JOIN_ORDER_GENERATED] = -1;
    SQLSTATE[-OB_ERR_NO_JOIN_ORDER_GENERATED] = "HY000";
    STR_ERROR[-OB_ERR_NO_JOIN_ORDER_GENERATED] = "No join order generated";
    STR_USER_ERROR[-OB_ERR_NO_JOIN_ORDER_GENERATED] = "No join order generated";
    ORACLE_ERRNO[-OB_ERR_NO_JOIN_ORDER_GENERATED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_JOIN_ORDER_GENERATED] =
        "ORA-00600: internal error code, arguments: -4240, No join order generated";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_JOIN_ORDER_GENERATED] =
        "ORA-00600: internal error code, arguments: -4240, No join order generated";
    ERROR_NAME[-OB_ERR_NO_PATH_GENERATED] = "OB_ERR_NO_PATH_GENERATED";
    MYSQL_ERRNO[-OB_ERR_NO_PATH_GENERATED] = -1;
    SQLSTATE[-OB_ERR_NO_PATH_GENERATED] = "HY000";
    STR_ERROR[-OB_ERR_NO_PATH_GENERATED] = "No join path generated";
    STR_USER_ERROR[-OB_ERR_NO_PATH_GENERATED] = "No join path generated";
    ORACLE_ERRNO[-OB_ERR_NO_PATH_GENERATED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_PATH_GENERATED] =
        "ORA-00600: internal error code, arguments: -4241, No join path generated";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_PATH_GENERATED] =
        "ORA-00600: internal error code, arguments: -4241, No join path generated";
    ERROR_NAME[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] = "OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH";
    MYSQL_ERRNO[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] = -1;
    SQLSTATE[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] = "HY000";
    STR_ERROR[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] = "Schema error";
    STR_USER_ERROR[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] = "Schema error";
    ORACLE_ERRNO[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] =
        "ORA-00600: internal error code, arguments: -4242, Schema error";
    ORACLE_STR_USER_ERROR[-OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH] =
        "ORA-00600: internal error code, arguments: -4242, Schema error";
    ERROR_NAME[-OB_FILE_NOT_OPENED] = "OB_FILE_NOT_OPENED";
    MYSQL_ERRNO[-OB_FILE_NOT_OPENED] = -1;
    SQLSTATE[-OB_FILE_NOT_OPENED] = "HY000";
    STR_ERROR[-OB_FILE_NOT_OPENED] = "file not opened";
    STR_USER_ERROR[-OB_FILE_NOT_OPENED] = "file not opened";
    ORACLE_ERRNO[-OB_FILE_NOT_OPENED] = 600;
    ORACLE_STR_ERROR[-OB_FILE_NOT_OPENED] = "ORA-00600: internal error code, arguments: -4243, file not opened";
    ORACLE_STR_USER_ERROR[-OB_FILE_NOT_OPENED] = "ORA-00600: internal error code, arguments: -4243, file not opened";
    ERROR_NAME[-OB_TIMER_TASK_HAS_SCHEDULED] = "OB_TIMER_TASK_HAS_SCHEDULED";
    MYSQL_ERRNO[-OB_TIMER_TASK_HAS_SCHEDULED] = -1;
    SQLSTATE[-OB_TIMER_TASK_HAS_SCHEDULED] = "HY000";
    STR_ERROR[-OB_TIMER_TASK_HAS_SCHEDULED] = "Timer task has been scheduled";
    STR_USER_ERROR[-OB_TIMER_TASK_HAS_SCHEDULED] = "Timer task has been scheduled";
    ORACLE_ERRNO[-OB_TIMER_TASK_HAS_SCHEDULED] = 600;
    ORACLE_STR_ERROR[-OB_TIMER_TASK_HAS_SCHEDULED] =
        "ORA-00600: internal error code, arguments: -4244, Timer task has been scheduled";
    ORACLE_STR_USER_ERROR[-OB_TIMER_TASK_HAS_SCHEDULED] =
        "ORA-00600: internal error code, arguments: -4244, Timer task has been scheduled";
    ERROR_NAME[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] = "OB_TIMER_TASK_HAS_NOT_SCHEDULED";
    MYSQL_ERRNO[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] = -1;
    SQLSTATE[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] = "HY000";
    STR_ERROR[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] = "Timer task has not been scheduled";
    STR_USER_ERROR[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] = "Timer task has not been scheduled";
    ORACLE_ERRNO[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] = 600;
    ORACLE_STR_ERROR[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] =
        "ORA-00600: internal error code, arguments: -4245, Timer task has not been scheduled";
    ORACLE_STR_USER_ERROR[-OB_TIMER_TASK_HAS_NOT_SCHEDULED] =
        "ORA-00600: internal error code, arguments: -4245, Timer task has not been scheduled";
    ERROR_NAME[-OB_PARSE_DEBUG_SYNC_ERROR] = "OB_PARSE_DEBUG_SYNC_ERROR";
    MYSQL_ERRNO[-OB_PARSE_DEBUG_SYNC_ERROR] = -1;
    SQLSTATE[-OB_PARSE_DEBUG_SYNC_ERROR] = "HY000";
    STR_ERROR[-OB_PARSE_DEBUG_SYNC_ERROR] = "parse debug sync string error";
    STR_USER_ERROR[-OB_PARSE_DEBUG_SYNC_ERROR] = "parse debug sync string error";
    ORACLE_ERRNO[-OB_PARSE_DEBUG_SYNC_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_PARSE_DEBUG_SYNC_ERROR] =
        "ORA-00600: internal error code, arguments: -4246, parse debug sync string error";
    ORACLE_STR_USER_ERROR[-OB_PARSE_DEBUG_SYNC_ERROR] =
        "ORA-00600: internal error code, arguments: -4246, parse debug sync string error";
    ERROR_NAME[-OB_UNKNOWN_DEBUG_SYNC_POINT] = "OB_UNKNOWN_DEBUG_SYNC_POINT";
    MYSQL_ERRNO[-OB_UNKNOWN_DEBUG_SYNC_POINT] = -1;
    SQLSTATE[-OB_UNKNOWN_DEBUG_SYNC_POINT] = "HY000";
    STR_ERROR[-OB_UNKNOWN_DEBUG_SYNC_POINT] = "unknown debug sync point";
    STR_USER_ERROR[-OB_UNKNOWN_DEBUG_SYNC_POINT] = "unknown debug sync point";
    ORACLE_ERRNO[-OB_UNKNOWN_DEBUG_SYNC_POINT] = 600;
    ORACLE_STR_ERROR[-OB_UNKNOWN_DEBUG_SYNC_POINT] =
        "ORA-00600: internal error code, arguments: -4247, unknown debug sync point";
    ORACLE_STR_USER_ERROR[-OB_UNKNOWN_DEBUG_SYNC_POINT] =
        "ORA-00600: internal error code, arguments: -4247, unknown debug sync point";
    ERROR_NAME[-OB_ERR_INTERRUPTED] = "OB_ERR_INTERRUPTED";
    MYSQL_ERRNO[-OB_ERR_INTERRUPTED] = -1;
    SQLSTATE[-OB_ERR_INTERRUPTED] = "HY000";
    STR_ERROR[-OB_ERR_INTERRUPTED] = "task is interrupted while running";
    STR_USER_ERROR[-OB_ERR_INTERRUPTED] = "task is interrupted while running";
    ORACLE_ERRNO[-OB_ERR_INTERRUPTED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INTERRUPTED] =
        "ORA-00600: internal error code, arguments: -4248, task is interrupted while running";
    ORACLE_STR_USER_ERROR[-OB_ERR_INTERRUPTED] =
        "ORA-00600: internal error code, arguments: -4248, task is interrupted while running";
    ERROR_NAME[-OB_ERR_DATA_TRUNCATED] = "OB_ERR_DATA_TRUNCATED";
    MYSQL_ERRNO[-OB_ERR_DATA_TRUNCATED] = WARN_DATA_TRUNCATED;
    SQLSTATE[-OB_ERR_DATA_TRUNCATED] = "01000";
    STR_ERROR[-OB_ERR_DATA_TRUNCATED] = "Data truncated for argument";
    STR_USER_ERROR[-OB_ERR_DATA_TRUNCATED] = "Data truncated for column '%.*s' at row %ld";
    ORACLE_ERRNO[-OB_ERR_DATA_TRUNCATED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DATA_TRUNCATED] =
        "ORA-00600: internal error code, arguments: -4249, Data truncated for argument";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATA_TRUNCATED] =
        "ORA-00600: internal error code, arguments: -4249, Data truncated for column '%.*s' at row %ld";
    ERROR_NAME[-OB_NOT_RUNNING] = "OB_NOT_RUNNING";
    MYSQL_ERRNO[-OB_NOT_RUNNING] = -1;
    SQLSTATE[-OB_NOT_RUNNING] = "HY000";
    STR_ERROR[-OB_NOT_RUNNING] = "module is not running";
    STR_USER_ERROR[-OB_NOT_RUNNING] = "module is not running";
    ORACLE_ERRNO[-OB_NOT_RUNNING] = 600;
    ORACLE_STR_ERROR[-OB_NOT_RUNNING] = "ORA-00600: internal error code, arguments: -4250, module is not running";
    ORACLE_STR_USER_ERROR[-OB_NOT_RUNNING] = "ORA-00600: internal error code, arguments: -4250, module is not running";
    ERROR_NAME[-OB_INVALID_PARTITION] = "OB_INVALID_PARTITION";
    MYSQL_ERRNO[-OB_INVALID_PARTITION] = -1;
    SQLSTATE[-OB_INVALID_PARTITION] = "HY000";
    STR_ERROR[-OB_INVALID_PARTITION] = "partition not valid";
    STR_USER_ERROR[-OB_INVALID_PARTITION] = "partition not valid";
    ORACLE_ERRNO[-OB_INVALID_PARTITION] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_PARTITION] = "ORA-00600: internal error code, arguments: -4251, partition not valid";
    ORACLE_STR_USER_ERROR[-OB_INVALID_PARTITION] =
        "ORA-00600: internal error code, arguments: -4251, partition not valid";
    ERROR_NAME[-OB_ERR_TIMEOUT_TRUNCATED] = "OB_ERR_TIMEOUT_TRUNCATED";
    MYSQL_ERRNO[-OB_ERR_TIMEOUT_TRUNCATED] = WARN_DATA_TRUNCATED;
    SQLSTATE[-OB_ERR_TIMEOUT_TRUNCATED] = "01000";
    STR_ERROR[-OB_ERR_TIMEOUT_TRUNCATED] = "Timeout value truncated to 102 years";
    STR_USER_ERROR[-OB_ERR_TIMEOUT_TRUNCATED] = "Timeout value truncated to 102 years";
    ORACLE_ERRNO[-OB_ERR_TIMEOUT_TRUNCATED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TIMEOUT_TRUNCATED] =
        "ORA-00600: internal error code, arguments: -4252, Timeout value truncated to 102 years";
    ORACLE_STR_USER_ERROR[-OB_ERR_TIMEOUT_TRUNCATED] =
        "ORA-00600: internal error code, arguments: -4252, Timeout value truncated to 102 years";
    ERROR_NAME[-OB_ERR_TOO_LONG_TENANT_COMMENT] = "OB_ERR_TOO_LONG_TENANT_COMMENT";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_TENANT_COMMENT] = -1;
    SQLSTATE[-OB_ERR_TOO_LONG_TENANT_COMMENT] = "HY000";
    STR_ERROR[-OB_ERR_TOO_LONG_TENANT_COMMENT] = "Comment for tenant is too long";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_TENANT_COMMENT] = "Comment for tenant is too long (max = %ld)";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_TENANT_COMMENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_TENANT_COMMENT] =
        "ORA-00600: internal error code, arguments: -4253, Comment for tenant is too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_TENANT_COMMENT] =
        "ORA-00600: internal error code, arguments: -4253, Comment for tenant is too long (max = %ld)";
    ERROR_NAME[-OB_ERR_NET_PACKET_TOO_LARGE] = "OB_ERR_NET_PACKET_TOO_LARGE";
    MYSQL_ERRNO[-OB_ERR_NET_PACKET_TOO_LARGE] = ER_NET_PACKET_TOO_LARGE;
    SQLSTATE[-OB_ERR_NET_PACKET_TOO_LARGE] = "08S01";
    STR_ERROR[-OB_ERR_NET_PACKET_TOO_LARGE] = "Got a packet bigger than \'max_allowed_packet\' bytes";
    STR_USER_ERROR[-OB_ERR_NET_PACKET_TOO_LARGE] = "Got a packet bigger than \'max_allowed_packet\' bytes";
    ORACLE_ERRNO[-OB_ERR_NET_PACKET_TOO_LARGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NET_PACKET_TOO_LARGE] =
        "ORA-00600: internal error code, arguments: -4254, Got a packet bigger than \'max_allowed_packet\' bytes";
    ORACLE_STR_USER_ERROR[-OB_ERR_NET_PACKET_TOO_LARGE] =
        "ORA-00600: internal error code, arguments: -4254, Got a packet bigger than \'max_allowed_packet\' bytes";
    ERROR_NAME[-OB_TRACE_DESC_NOT_EXIST] = "OB_TRACE_DESC_NOT_EXIST";
    MYSQL_ERRNO[-OB_TRACE_DESC_NOT_EXIST] = -1;
    SQLSTATE[-OB_TRACE_DESC_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_TRACE_DESC_NOT_EXIST] = "trace log title or key not exist describle";
    STR_USER_ERROR[-OB_TRACE_DESC_NOT_EXIST] = "trace log title or key not exist describle";
    ORACLE_ERRNO[-OB_TRACE_DESC_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TRACE_DESC_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4255, trace log title or key not exist describle";
    ORACLE_STR_USER_ERROR[-OB_TRACE_DESC_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4255, trace log title or key not exist describle";
    ERROR_NAME[-OB_ERR_NO_DEFAULT] = "OB_ERR_NO_DEFAULT";
    MYSQL_ERRNO[-OB_ERR_NO_DEFAULT] = ER_NO_DEFAULT;
    SQLSTATE[-OB_ERR_NO_DEFAULT] = "42000";
    STR_ERROR[-OB_ERR_NO_DEFAULT] = "Variable doesn't have a default value";
    STR_USER_ERROR[-OB_ERR_NO_DEFAULT] = "Variable '%.*s' doesn't have a default value";
    ORACLE_ERRNO[-OB_ERR_NO_DEFAULT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_DEFAULT] =
        "ORA-00600: internal error code, arguments: -4256, Variable doesn't have a default value";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_DEFAULT] =
        "ORA-00600: internal error code, arguments: -4256, Variable '%.*s' doesn't have a default value";
    ERROR_NAME[-OB_ERR_COMPRESS_DECOMPRESS_DATA] = "OB_ERR_COMPRESS_DECOMPRESS_DATA";
    MYSQL_ERRNO[-OB_ERR_COMPRESS_DECOMPRESS_DATA] = -1;
    SQLSTATE[-OB_ERR_COMPRESS_DECOMPRESS_DATA] = "HY000";
    STR_ERROR[-OB_ERR_COMPRESS_DECOMPRESS_DATA] = "compress data or decompress data failed";
    STR_USER_ERROR[-OB_ERR_COMPRESS_DECOMPRESS_DATA] = "compress data or decompress data failed";
    ORACLE_ERRNO[-OB_ERR_COMPRESS_DECOMPRESS_DATA] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COMPRESS_DECOMPRESS_DATA] =
        "ORA-00600: internal error code, arguments: -4257, compress data or decompress data failed";
    ORACLE_STR_USER_ERROR[-OB_ERR_COMPRESS_DECOMPRESS_DATA] =
        "ORA-00600: internal error code, arguments: -4257, compress data or decompress data failed";
    ERROR_NAME[-OB_ERR_INCORRECT_STRING_VALUE] = "OB_ERR_INCORRECT_STRING_VALUE";
    MYSQL_ERRNO[-OB_ERR_INCORRECT_STRING_VALUE] = ER_TRUNCATED_WRONG_VALUE_FOR_FIELD;
    SQLSTATE[-OB_ERR_INCORRECT_STRING_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INCORRECT_STRING_VALUE] = "Incorrect string value";
    STR_USER_ERROR[-OB_ERR_INCORRECT_STRING_VALUE] = "Incorrect string value for column '%.*s' at row %ld";
    ORACLE_ERRNO[-OB_ERR_INCORRECT_STRING_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INCORRECT_STRING_VALUE] =
        "ORA-00600: internal error code, arguments: -4258, Incorrect string value";
    ORACLE_STR_USER_ERROR[-OB_ERR_INCORRECT_STRING_VALUE] =
        "ORA-00600: internal error code, arguments: -4258, Incorrect string value for column '%.*s' at row %ld";
    ERROR_NAME[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] = "OB_ERR_DISTRIBUTED_NOT_SUPPORTED";
    MYSQL_ERRNO[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] = ER_NOT_SUPPORTED_YET;
    SQLSTATE[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] = "0A000";
    STR_ERROR[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] = "Not supported feature or function";
    STR_USER_ERROR[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] = "%s not supported";
    ORACLE_ERRNO[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] =
        "ORA-00600: internal error code, arguments: -4259, Not supported feature or function";
    ORACLE_STR_USER_ERROR[-OB_ERR_DISTRIBUTED_NOT_SUPPORTED] =
        "ORA-00600: internal error code, arguments: -4259, %s not supported";
    ERROR_NAME[-OB_IS_CHANGING_LEADER] = "OB_IS_CHANGING_LEADER";
    MYSQL_ERRNO[-OB_IS_CHANGING_LEADER] = -1;
    SQLSTATE[-OB_IS_CHANGING_LEADER] = "HY000";
    STR_ERROR[-OB_IS_CHANGING_LEADER] = "the partition is changing leader";
    STR_USER_ERROR[-OB_IS_CHANGING_LEADER] = "the partition is changing leader";
    ORACLE_ERRNO[-OB_IS_CHANGING_LEADER] = 600;
    ORACLE_STR_ERROR[-OB_IS_CHANGING_LEADER] =
        "ORA-00600: internal error code, arguments: -4260, the partition is changing leader";
    ORACLE_STR_USER_ERROR[-OB_IS_CHANGING_LEADER] =
        "ORA-00600: internal error code, arguments: -4260, the partition is changing leader";
    ERROR_NAME[-OB_DATETIME_FUNCTION_OVERFLOW] = "OB_DATETIME_FUNCTION_OVERFLOW";
    MYSQL_ERRNO[-OB_DATETIME_FUNCTION_OVERFLOW] = ER_DATETIME_FUNCTION_OVERFLOW;
    SQLSTATE[-OB_DATETIME_FUNCTION_OVERFLOW] = "22008";
    STR_ERROR[-OB_DATETIME_FUNCTION_OVERFLOW] = "Datetime overflow";
    STR_USER_ERROR[-OB_DATETIME_FUNCTION_OVERFLOW] = "Datetime overflow";
    ORACLE_ERRNO[-OB_DATETIME_FUNCTION_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_DATETIME_FUNCTION_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -4261, Datetime overflow";
    ORACLE_STR_USER_ERROR[-OB_DATETIME_FUNCTION_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -4261, Datetime overflow";
    ERROR_NAME[-OB_ERR_DOUBLE_TRUNCATED] = "OB_ERR_DOUBLE_TRUNCATED";
    MYSQL_ERRNO[-OB_ERR_DOUBLE_TRUNCATED] = ER_TRUNCATED_WRONG_VALUE;
    SQLSTATE[-OB_ERR_DOUBLE_TRUNCATED] = "01000";
    STR_ERROR[-OB_ERR_DOUBLE_TRUNCATED] = "Truncated incorrect DOUBLE value";
    STR_USER_ERROR[-OB_ERR_DOUBLE_TRUNCATED] = "Truncated incorrect DOUBLE value: '%.*s'";
    ORACLE_ERRNO[-OB_ERR_DOUBLE_TRUNCATED] = 1722;
    ORACLE_STR_ERROR[-OB_ERR_DOUBLE_TRUNCATED] = "ORA-01722: invalid number";
    ORACLE_STR_USER_ERROR[-OB_ERR_DOUBLE_TRUNCATED] = "ORA-01722: invalid number: '%.*s'";
    ERROR_NAME[-OB_MINOR_FREEZE_NOT_ALLOW] = "OB_MINOR_FREEZE_NOT_ALLOW";
    MYSQL_ERRNO[-OB_MINOR_FREEZE_NOT_ALLOW] = -1;
    SQLSTATE[-OB_MINOR_FREEZE_NOT_ALLOW] = "HY000";
    STR_ERROR[-OB_MINOR_FREEZE_NOT_ALLOW] = "Minor freeze not allowed now";
    STR_USER_ERROR[-OB_MINOR_FREEZE_NOT_ALLOW] = "%s";
    ORACLE_ERRNO[-OB_MINOR_FREEZE_NOT_ALLOW] = 600;
    ORACLE_STR_ERROR[-OB_MINOR_FREEZE_NOT_ALLOW] =
        "ORA-00600: internal error code, arguments: -4263, Minor freeze not allowed now";
    ORACLE_STR_USER_ERROR[-OB_MINOR_FREEZE_NOT_ALLOW] = "ORA-00600: internal error code, arguments: -4263, %s";
    ERROR_NAME[-OB_LOG_OUTOF_DISK_SPACE] = "OB_LOG_OUTOF_DISK_SPACE";
    MYSQL_ERRNO[-OB_LOG_OUTOF_DISK_SPACE] = -1;
    SQLSTATE[-OB_LOG_OUTOF_DISK_SPACE] = "HY000";
    STR_ERROR[-OB_LOG_OUTOF_DISK_SPACE] = "Log out of disk space";
    STR_USER_ERROR[-OB_LOG_OUTOF_DISK_SPACE] = "Log out of disk space";
    ORACLE_ERRNO[-OB_LOG_OUTOF_DISK_SPACE] = 600;
    ORACLE_STR_ERROR[-OB_LOG_OUTOF_DISK_SPACE] =
        "ORA-00600: internal error code, arguments: -4264, Log out of disk space";
    ORACLE_STR_USER_ERROR[-OB_LOG_OUTOF_DISK_SPACE] =
        "ORA-00600: internal error code, arguments: -4264, Log out of disk space";
    ERROR_NAME[-OB_RPC_CONNECT_ERROR] = "OB_RPC_CONNECT_ERROR";
    MYSQL_ERRNO[-OB_RPC_CONNECT_ERROR] = -1;
    SQLSTATE[-OB_RPC_CONNECT_ERROR] = "HY000";
    STR_ERROR[-OB_RPC_CONNECT_ERROR] = "Rpc connect error";
    STR_USER_ERROR[-OB_RPC_CONNECT_ERROR] = "Rpc connect error";
    ORACLE_ERRNO[-OB_RPC_CONNECT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_RPC_CONNECT_ERROR] = "ORA-00600: internal error code, arguments: -4265, Rpc connect error";
    ORACLE_STR_USER_ERROR[-OB_RPC_CONNECT_ERROR] =
        "ORA-00600: internal error code, arguments: -4265, Rpc connect error";
    ERROR_NAME[-OB_MINOR_MERGE_NOT_ALLOW] = "OB_MINOR_MERGE_NOT_ALLOW";
    MYSQL_ERRNO[-OB_MINOR_MERGE_NOT_ALLOW] = -1;
    SQLSTATE[-OB_MINOR_MERGE_NOT_ALLOW] = "HY000";
    STR_ERROR[-OB_MINOR_MERGE_NOT_ALLOW] = "minor merge not allow";
    STR_USER_ERROR[-OB_MINOR_MERGE_NOT_ALLOW] = "minor merge not allow";
    ORACLE_ERRNO[-OB_MINOR_MERGE_NOT_ALLOW] = 600;
    ORACLE_STR_ERROR[-OB_MINOR_MERGE_NOT_ALLOW] =
        "ORA-00600: internal error code, arguments: -4266, minor merge not allow";
    ORACLE_STR_USER_ERROR[-OB_MINOR_MERGE_NOT_ALLOW] =
        "ORA-00600: internal error code, arguments: -4266, minor merge not allow";
    ERROR_NAME[-OB_CACHE_INVALID] = "OB_CACHE_INVALID";
    MYSQL_ERRNO[-OB_CACHE_INVALID] = -1;
    SQLSTATE[-OB_CACHE_INVALID] = "HY000";
    STR_ERROR[-OB_CACHE_INVALID] = "Cache invalid";
    STR_USER_ERROR[-OB_CACHE_INVALID] = "Cache invalid";
    ORACLE_ERRNO[-OB_CACHE_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_CACHE_INVALID] = "ORA-00600: internal error code, arguments: -4267, Cache invalid";
    ORACLE_STR_USER_ERROR[-OB_CACHE_INVALID] = "ORA-00600: internal error code, arguments: -4267, Cache invalid";
    ERROR_NAME[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] = "OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT";
    MYSQL_ERRNO[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] = -1;
    SQLSTATE[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] = "HY000";
    STR_ERROR[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] = "reach server data copy in concurrency";
    STR_USER_ERROR[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] = "reach server data copy in concurrency";
    ORACLE_ERRNO[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] =
        "ORA-00600: internal error code, arguments: -4268, reach server data copy in concurrency";
    ORACLE_STR_USER_ERROR[-OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT] =
        "ORA-00600: internal error code, arguments: -4268, reach server data copy in concurrency";
    ERROR_NAME[-OB_WORKING_PARTITION_EXIST] = "OB_WORKING_PARTITION_EXIST";
    MYSQL_ERRNO[-OB_WORKING_PARTITION_EXIST] = -1;
    SQLSTATE[-OB_WORKING_PARTITION_EXIST] = "HY000";
    STR_ERROR[-OB_WORKING_PARTITION_EXIST] = "Working partition entry already exists";
    STR_USER_ERROR[-OB_WORKING_PARTITION_EXIST] = "Working partition entry already exists";
    ORACLE_ERRNO[-OB_WORKING_PARTITION_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_WORKING_PARTITION_EXIST] =
        "ORA-00600: internal error code, arguments: -4269, Working partition entry already exists";
    ORACLE_STR_USER_ERROR[-OB_WORKING_PARTITION_EXIST] =
        "ORA-00600: internal error code, arguments: -4269, Working partition entry already exists";
    ERROR_NAME[-OB_WORKING_PARTITION_NOT_EXIST] = "OB_WORKING_PARTITION_NOT_EXIST";
    MYSQL_ERRNO[-OB_WORKING_PARTITION_NOT_EXIST] = -1;
    SQLSTATE[-OB_WORKING_PARTITION_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_WORKING_PARTITION_NOT_EXIST] = "Working partition entry does not exists";
    STR_USER_ERROR[-OB_WORKING_PARTITION_NOT_EXIST] = "Working partition entry does not exists";
    ORACLE_ERRNO[-OB_WORKING_PARTITION_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_WORKING_PARTITION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4270, Working partition entry does not exists";
    ORACLE_STR_USER_ERROR[-OB_WORKING_PARTITION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4270, Working partition entry does not exists";
    ERROR_NAME[-OB_LIBEASY_REACH_MEM_LIMIT] = "OB_LIBEASY_REACH_MEM_LIMIT";
    MYSQL_ERRNO[-OB_LIBEASY_REACH_MEM_LIMIT] = -1;
    SQLSTATE[-OB_LIBEASY_REACH_MEM_LIMIT] = "HY000";
    STR_ERROR[-OB_LIBEASY_REACH_MEM_LIMIT] = "LIBEASY reach memory limit";
    STR_USER_ERROR[-OB_LIBEASY_REACH_MEM_LIMIT] = "LIBEASY reach memory limit";
    ORACLE_ERRNO[-OB_LIBEASY_REACH_MEM_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_LIBEASY_REACH_MEM_LIMIT] =
        "ORA-00600: internal error code, arguments: -4271, LIBEASY reach memory limit";
    ORACLE_STR_USER_ERROR[-OB_LIBEASY_REACH_MEM_LIMIT] =
        "ORA-00600: internal error code, arguments: -4271, LIBEASY reach memory limit";
    ERROR_NAME[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] = "OB_CACHE_FREE_BLOCK_NOT_ENOUGH";
    MYSQL_ERRNO[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] = -1;
    SQLSTATE[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] = "HY000";
    STR_ERROR[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] = "free memblock in cache is not enough";
    STR_USER_ERROR[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] = "free memblock in cache is not enough";
    ORACLE_ERRNO[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4273, free memblock in cache is not enough";
    ORACLE_STR_USER_ERROR[-OB_CACHE_FREE_BLOCK_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4273, free memblock in cache is not enough";
    ERROR_NAME[-OB_SYNC_WASH_MB_TIMEOUT] = "OB_SYNC_WASH_MB_TIMEOUT";
    MYSQL_ERRNO[-OB_SYNC_WASH_MB_TIMEOUT] = -1;
    SQLSTATE[-OB_SYNC_WASH_MB_TIMEOUT] = "HY000";
    STR_ERROR[-OB_SYNC_WASH_MB_TIMEOUT] = "sync wash memblock timeout";
    STR_USER_ERROR[-OB_SYNC_WASH_MB_TIMEOUT] = "sync wash memblock timeout";
    ORACLE_ERRNO[-OB_SYNC_WASH_MB_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_SYNC_WASH_MB_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4274, sync wash memblock timeout";
    ORACLE_STR_USER_ERROR[-OB_SYNC_WASH_MB_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4274, sync wash memblock timeout";
    ERROR_NAME[-OB_NOT_ALLOW_MIGRATE_IN] = "OB_NOT_ALLOW_MIGRATE_IN";
    MYSQL_ERRNO[-OB_NOT_ALLOW_MIGRATE_IN] = -1;
    SQLSTATE[-OB_NOT_ALLOW_MIGRATE_IN] = "HY000";
    STR_ERROR[-OB_NOT_ALLOW_MIGRATE_IN] = "not allow migrate in";
    STR_USER_ERROR[-OB_NOT_ALLOW_MIGRATE_IN] = "not allow migrate in";
    ORACLE_ERRNO[-OB_NOT_ALLOW_MIGRATE_IN] = 600;
    ORACLE_STR_ERROR[-OB_NOT_ALLOW_MIGRATE_IN] =
        "ORA-00600: internal error code, arguments: -4275, not allow migrate in";
    ORACLE_STR_USER_ERROR[-OB_NOT_ALLOW_MIGRATE_IN] =
        "ORA-00600: internal error code, arguments: -4275, not allow migrate in";
    ERROR_NAME[-OB_SCHEDULER_TASK_CNT_MISTACH] = "OB_SCHEDULER_TASK_CNT_MISTACH";
    MYSQL_ERRNO[-OB_SCHEDULER_TASK_CNT_MISTACH] = -1;
    SQLSTATE[-OB_SCHEDULER_TASK_CNT_MISTACH] = "HY000";
    STR_ERROR[-OB_SCHEDULER_TASK_CNT_MISTACH] = "Scheduler task cnt does not match";
    STR_USER_ERROR[-OB_SCHEDULER_TASK_CNT_MISTACH] = "Scheduler task cnt does not match";
    ORACLE_ERRNO[-OB_SCHEDULER_TASK_CNT_MISTACH] = 600;
    ORACLE_STR_ERROR[-OB_SCHEDULER_TASK_CNT_MISTACH] =
        "ORA-00600: internal error code, arguments: -4276, Scheduler task cnt does not match";
    ORACLE_STR_USER_ERROR[-OB_SCHEDULER_TASK_CNT_MISTACH] =
        "ORA-00600: internal error code, arguments: -4276, Scheduler task cnt does not match";
    ERROR_NAME[-OB_MISS_ARGUMENT] = "OB_MISS_ARGUMENT";
    MYSQL_ERRNO[-OB_MISS_ARGUMENT] = ER_WRONG_ARGUMENTS;
    SQLSTATE[-OB_MISS_ARGUMENT] = "HY000";
    STR_ERROR[-OB_MISS_ARGUMENT] = "Miss argument";
    STR_USER_ERROR[-OB_MISS_ARGUMENT] = "Miss argument for %s";
    ORACLE_ERRNO[-OB_MISS_ARGUMENT] = 600;
    ORACLE_STR_ERROR[-OB_MISS_ARGUMENT] = "ORA-00600: internal error code, arguments: -4277, Miss argument";
    ORACLE_STR_USER_ERROR[-OB_MISS_ARGUMENT] = "ORA-00600: internal error code, arguments: -4277, Miss argument for %s";
    ERROR_NAME[-OB_LAST_LOG_NOT_COMPLETE] = "OB_LAST_LOG_NOT_COMPLETE";
    MYSQL_ERRNO[-OB_LAST_LOG_NOT_COMPLETE] = -1;
    SQLSTATE[-OB_LAST_LOG_NOT_COMPLETE] = "HY000";
    STR_ERROR[-OB_LAST_LOG_NOT_COMPLETE] = "last log is not complete";
    STR_USER_ERROR[-OB_LAST_LOG_NOT_COMPLETE] = "last log is not complete";
    ORACLE_ERRNO[-OB_LAST_LOG_NOT_COMPLETE] = 600;
    ORACLE_STR_ERROR[-OB_LAST_LOG_NOT_COMPLETE] =
        "ORA-00600: internal error code, arguments: -4278, last log is not complete";
    ORACLE_STR_USER_ERROR[-OB_LAST_LOG_NOT_COMPLETE] =
        "ORA-00600: internal error code, arguments: -4278, last log is not complete";
    ERROR_NAME[-OB_TABLE_IS_DELETED] = "OB_TABLE_IS_DELETED";
    MYSQL_ERRNO[-OB_TABLE_IS_DELETED] = -1;
    SQLSTATE[-OB_TABLE_IS_DELETED] = "HY000";
    STR_ERROR[-OB_TABLE_IS_DELETED] = "table is deleted";
    STR_USER_ERROR[-OB_TABLE_IS_DELETED] = "table is deleted";
    ORACLE_ERRNO[-OB_TABLE_IS_DELETED] = 600;
    ORACLE_STR_ERROR[-OB_TABLE_IS_DELETED] = "ORA-00600: internal error code, arguments: -4279, table is deleted";
    ORACLE_STR_USER_ERROR[-OB_TABLE_IS_DELETED] = "ORA-00600: internal error code, arguments: -4279, table is deleted";
    ERROR_NAME[-OB_VERSION_RANGE_NOT_CONTINUES] = "OB_VERSION_RANGE_NOT_CONTINUES";
    MYSQL_ERRNO[-OB_VERSION_RANGE_NOT_CONTINUES] = -1;
    SQLSTATE[-OB_VERSION_RANGE_NOT_CONTINUES] = "HY000";
    STR_ERROR[-OB_VERSION_RANGE_NOT_CONTINUES] = "version range not continues";
    STR_USER_ERROR[-OB_VERSION_RANGE_NOT_CONTINUES] = "version range not continues";
    ORACLE_ERRNO[-OB_VERSION_RANGE_NOT_CONTINUES] = 600;
    ORACLE_STR_ERROR[-OB_VERSION_RANGE_NOT_CONTINUES] =
        "ORA-00600: internal error code, arguments: -4280, version range not continues";
    ORACLE_STR_USER_ERROR[-OB_VERSION_RANGE_NOT_CONTINUES] =
        "ORA-00600: internal error code, arguments: -4280, version range not continues";
    ERROR_NAME[-OB_INVALID_IO_BUFFER] = "OB_INVALID_IO_BUFFER";
    MYSQL_ERRNO[-OB_INVALID_IO_BUFFER] = -1;
    SQLSTATE[-OB_INVALID_IO_BUFFER] = "HY000";
    STR_ERROR[-OB_INVALID_IO_BUFFER] = "io buffer is invalid";
    STR_USER_ERROR[-OB_INVALID_IO_BUFFER] = "io buffer is invalid";
    ORACLE_ERRNO[-OB_INVALID_IO_BUFFER] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_IO_BUFFER] = "ORA-00600: internal error code, arguments: -4281, io buffer is invalid";
    ORACLE_STR_USER_ERROR[-OB_INVALID_IO_BUFFER] =
        "ORA-00600: internal error code, arguments: -4281, io buffer is invalid";
    ERROR_NAME[-OB_PARTITION_IS_REMOVED] = "OB_PARTITION_IS_REMOVED";
    MYSQL_ERRNO[-OB_PARTITION_IS_REMOVED] = -1;
    SQLSTATE[-OB_PARTITION_IS_REMOVED] = "HY000";
    STR_ERROR[-OB_PARTITION_IS_REMOVED] = "partition is removed";
    STR_USER_ERROR[-OB_PARTITION_IS_REMOVED] = "partition is removed";
    ORACLE_ERRNO[-OB_PARTITION_IS_REMOVED] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_IS_REMOVED] =
        "ORA-00600: internal error code, arguments: -4282, partition is removed";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_IS_REMOVED] =
        "ORA-00600: internal error code, arguments: -4282, partition is removed";
    ERROR_NAME[-OB_GTS_NOT_READY] = "OB_GTS_NOT_READY";
    MYSQL_ERRNO[-OB_GTS_NOT_READY] = -1;
    SQLSTATE[-OB_GTS_NOT_READY] = "HY000";
    STR_ERROR[-OB_GTS_NOT_READY] = "gts is not ready";
    STR_USER_ERROR[-OB_GTS_NOT_READY] = "gts is not ready";
    ORACLE_ERRNO[-OB_GTS_NOT_READY] = 600;
    ORACLE_STR_ERROR[-OB_GTS_NOT_READY] = "ORA-00600: internal error code, arguments: -4283, gts is not ready";
    ORACLE_STR_USER_ERROR[-OB_GTS_NOT_READY] = "ORA-00600: internal error code, arguments: -4283, gts is not ready";
    ERROR_NAME[-OB_MAJOR_SSTABLE_NOT_EXIST] = "OB_MAJOR_SSTABLE_NOT_EXIST";
    MYSQL_ERRNO[-OB_MAJOR_SSTABLE_NOT_EXIST] = -1;
    SQLSTATE[-OB_MAJOR_SSTABLE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_MAJOR_SSTABLE_NOT_EXIST] = "major sstable not exist";
    STR_USER_ERROR[-OB_MAJOR_SSTABLE_NOT_EXIST] = "major sstable not exist";
    ORACLE_ERRNO[-OB_MAJOR_SSTABLE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_MAJOR_SSTABLE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4284, major sstable not exist";
    ORACLE_STR_USER_ERROR[-OB_MAJOR_SSTABLE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4284, major sstable not exist";
    ERROR_NAME[-OB_VERSION_RANGE_DISCARDED] = "OB_VERSION_RANGE_DISCARDED";
    MYSQL_ERRNO[-OB_VERSION_RANGE_DISCARDED] = -1;
    SQLSTATE[-OB_VERSION_RANGE_DISCARDED] = "HY000";
    STR_ERROR[-OB_VERSION_RANGE_DISCARDED] = "Request to read too old version range data";
    STR_USER_ERROR[-OB_VERSION_RANGE_DISCARDED] = "Request to read too old version range data";
    ORACLE_ERRNO[-OB_VERSION_RANGE_DISCARDED] = 600;
    ORACLE_STR_ERROR[-OB_VERSION_RANGE_DISCARDED] =
        "ORA-00600: internal error code, arguments: -4285, Request to read too old version range data";
    ORACLE_STR_USER_ERROR[-OB_VERSION_RANGE_DISCARDED] =
        "ORA-00600: internal error code, arguments: -4285, Request to read too old version range data";
    ERROR_NAME[-OB_MAJOR_SSTABLE_HAS_MERGED] = "OB_MAJOR_SSTABLE_HAS_MERGED";
    MYSQL_ERRNO[-OB_MAJOR_SSTABLE_HAS_MERGED] = -1;
    SQLSTATE[-OB_MAJOR_SSTABLE_HAS_MERGED] = "HY000";
    STR_ERROR[-OB_MAJOR_SSTABLE_HAS_MERGED] = "major sstable may has been merged";
    STR_USER_ERROR[-OB_MAJOR_SSTABLE_HAS_MERGED] = "major sstable may has been merged";
    ORACLE_ERRNO[-OB_MAJOR_SSTABLE_HAS_MERGED] = 600;
    ORACLE_STR_ERROR[-OB_MAJOR_SSTABLE_HAS_MERGED] =
        "ORA-00600: internal error code, arguments: -4286, major sstable may has been merged";
    ORACLE_STR_USER_ERROR[-OB_MAJOR_SSTABLE_HAS_MERGED] =
        "ORA-00600: internal error code, arguments: -4286, major sstable may has been merged";
    ERROR_NAME[-OB_MINOR_SSTABLE_RANGE_CROSS] = "OB_MINOR_SSTABLE_RANGE_CROSS";
    MYSQL_ERRNO[-OB_MINOR_SSTABLE_RANGE_CROSS] = -1;
    SQLSTATE[-OB_MINOR_SSTABLE_RANGE_CROSS] = "HY000";
    STR_ERROR[-OB_MINOR_SSTABLE_RANGE_CROSS] = "minor sstable version range cross";
    STR_USER_ERROR[-OB_MINOR_SSTABLE_RANGE_CROSS] = "minor sstable version range cross";
    ORACLE_ERRNO[-OB_MINOR_SSTABLE_RANGE_CROSS] = 600;
    ORACLE_STR_ERROR[-OB_MINOR_SSTABLE_RANGE_CROSS] =
        "ORA-00600: internal error code, arguments: -4287, minor sstable version range cross";
    ORACLE_STR_USER_ERROR[-OB_MINOR_SSTABLE_RANGE_CROSS] =
        "ORA-00600: internal error code, arguments: -4287, minor sstable version range cross";
    ERROR_NAME[-OB_MEMTABLE_CANNOT_MINOR_MERGE] = "OB_MEMTABLE_CANNOT_MINOR_MERGE";
    MYSQL_ERRNO[-OB_MEMTABLE_CANNOT_MINOR_MERGE] = -1;
    SQLSTATE[-OB_MEMTABLE_CANNOT_MINOR_MERGE] = "HY000";
    STR_ERROR[-OB_MEMTABLE_CANNOT_MINOR_MERGE] = "memtable cannot minor merge";
    STR_USER_ERROR[-OB_MEMTABLE_CANNOT_MINOR_MERGE] = "memtable cannot minor merge";
    ORACLE_ERRNO[-OB_MEMTABLE_CANNOT_MINOR_MERGE] = 600;
    ORACLE_STR_ERROR[-OB_MEMTABLE_CANNOT_MINOR_MERGE] =
        "ORA-00600: internal error code, arguments: -4288, memtable cannot minor merge";
    ORACLE_STR_USER_ERROR[-OB_MEMTABLE_CANNOT_MINOR_MERGE] =
        "ORA-00600: internal error code, arguments: -4288, memtable cannot minor merge";
    ERROR_NAME[-OB_TASK_EXIST] = "OB_TASK_EXIST";
    MYSQL_ERRNO[-OB_TASK_EXIST] = -1;
    SQLSTATE[-OB_TASK_EXIST] = "HY000";
    STR_ERROR[-OB_TASK_EXIST] = "task exist";
    STR_USER_ERROR[-OB_TASK_EXIST] = "task exist";
    ORACLE_ERRNO[-OB_TASK_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TASK_EXIST] = "ORA-00600: internal error code, arguments: -4289, task exist";
    ORACLE_STR_USER_ERROR[-OB_TASK_EXIST] = "ORA-00600: internal error code, arguments: -4289, task exist";
    ERROR_NAME[-OB_ALLOCATE_DISK_SPACE_FAILED] = "OB_ALLOCATE_DISK_SPACE_FAILED";
    MYSQL_ERRNO[-OB_ALLOCATE_DISK_SPACE_FAILED] = -1;
    SQLSTATE[-OB_ALLOCATE_DISK_SPACE_FAILED] = "HY000";
    STR_ERROR[-OB_ALLOCATE_DISK_SPACE_FAILED] = "cannot allocate disk space";
    STR_USER_ERROR[-OB_ALLOCATE_DISK_SPACE_FAILED] = "cannot allocate disk space";
    ORACLE_ERRNO[-OB_ALLOCATE_DISK_SPACE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_ALLOCATE_DISK_SPACE_FAILED] =
        "ORA-00600: internal error code, arguments: -4290, cannot allocate disk space";
    ORACLE_STR_USER_ERROR[-OB_ALLOCATE_DISK_SPACE_FAILED] =
        "ORA-00600: internal error code, arguments: -4290, cannot allocate disk space";
    ERROR_NAME[-OB_CANT_FIND_UDF] = "OB_CANT_FIND_UDF";
    MYSQL_ERRNO[-OB_CANT_FIND_UDF] = ER_CANT_FIND_UDF;
    SQLSTATE[-OB_CANT_FIND_UDF] = "HY000";
    STR_ERROR[-OB_CANT_FIND_UDF] = "Can't load function";
    STR_USER_ERROR[-OB_CANT_FIND_UDF] = "Can not load function %s";
    ORACLE_ERRNO[-OB_CANT_FIND_UDF] = 600;
    ORACLE_STR_ERROR[-OB_CANT_FIND_UDF] = "ORA-00600: internal error code, arguments: -4291, Can't load function";
    ORACLE_STR_USER_ERROR[-OB_CANT_FIND_UDF] =
        "ORA-00600: internal error code, arguments: -4291, Can not load function %s";
    ERROR_NAME[-OB_CANT_INITIALIZE_UDF] = "OB_CANT_INITIALIZE_UDF";
    MYSQL_ERRNO[-OB_CANT_INITIALIZE_UDF] = ER_CANT_INITIALIZE_UDF;
    SQLSTATE[-OB_CANT_INITIALIZE_UDF] = "HY000";
    STR_ERROR[-OB_CANT_INITIALIZE_UDF] = "Can't initialize function";
    STR_USER_ERROR[-OB_CANT_INITIALIZE_UDF] = "Can not initialize function '%.*s'";
    ORACLE_ERRNO[-OB_CANT_INITIALIZE_UDF] = 600;
    ORACLE_STR_ERROR[-OB_CANT_INITIALIZE_UDF] =
        "ORA-00600: internal error code, arguments: -4292, Can't initialize function";
    ORACLE_STR_USER_ERROR[-OB_CANT_INITIALIZE_UDF] =
        "ORA-00600: internal error code, arguments: -4292, Can not initialize function '%.*s'";
    ERROR_NAME[-OB_UDF_NO_PATHS] = "OB_UDF_NO_PATHS";
    MYSQL_ERRNO[-OB_UDF_NO_PATHS] = ER_UDF_NO_PATHS;
    SQLSTATE[-OB_UDF_NO_PATHS] = "HY000";
    STR_ERROR[-OB_UDF_NO_PATHS] = "No paths allowed for shared library";
    STR_USER_ERROR[-OB_UDF_NO_PATHS] = "No paths allowed for shared library";
    ORACLE_ERRNO[-OB_UDF_NO_PATHS] = 600;
    ORACLE_STR_ERROR[-OB_UDF_NO_PATHS] =
        "ORA-00600: internal error code, arguments: -4293, No paths allowed for shared library";
    ORACLE_STR_USER_ERROR[-OB_UDF_NO_PATHS] =
        "ORA-00600: internal error code, arguments: -4293, No paths allowed for shared library";
    ERROR_NAME[-OB_UDF_EXISTS] = "OB_UDF_EXISTS";
    MYSQL_ERRNO[-OB_UDF_EXISTS] = ER_UDF_EXISTS;
    SQLSTATE[-OB_UDF_EXISTS] = "HY000";
    STR_ERROR[-OB_UDF_EXISTS] = "Function already exists";
    STR_USER_ERROR[-OB_UDF_EXISTS] = "Function %.*s already exists";
    ORACLE_ERRNO[-OB_UDF_EXISTS] = 600;
    ORACLE_STR_ERROR[-OB_UDF_EXISTS] = "ORA-00600: internal error code, arguments: -4294, Function already exists";
    ORACLE_STR_USER_ERROR[-OB_UDF_EXISTS] =
        "ORA-00600: internal error code, arguments: -4294, Function %.*s already exists";
    ERROR_NAME[-OB_CANT_OPEN_LIBRARY] = "OB_CANT_OPEN_LIBRARY";
    MYSQL_ERRNO[-OB_CANT_OPEN_LIBRARY] = ER_CANT_OPEN_LIBRARY;
    SQLSTATE[-OB_CANT_OPEN_LIBRARY] = "HY000";
    STR_ERROR[-OB_CANT_OPEN_LIBRARY] = "Can't open shared library";
    STR_USER_ERROR[-OB_CANT_OPEN_LIBRARY] = "Can not open shared library '%.*s'";
    ORACLE_ERRNO[-OB_CANT_OPEN_LIBRARY] = 600;
    ORACLE_STR_ERROR[-OB_CANT_OPEN_LIBRARY] =
        "ORA-00600: internal error code, arguments: -4295, Can't open shared library";
    ORACLE_STR_USER_ERROR[-OB_CANT_OPEN_LIBRARY] =
        "ORA-00600: internal error code, arguments: -4295, Can not open shared library '%.*s'";
    ERROR_NAME[-OB_CANT_FIND_DL_ENTRY] = "OB_CANT_FIND_DL_ENTRY";
    MYSQL_ERRNO[-OB_CANT_FIND_DL_ENTRY] = ER_CANT_FIND_DL_ENTRY;
    SQLSTATE[-OB_CANT_FIND_DL_ENTRY] = "HY000";
    STR_ERROR[-OB_CANT_FIND_DL_ENTRY] = "Can't find symbol";
    STR_USER_ERROR[-OB_CANT_FIND_DL_ENTRY] = "Can't find symbol %.*s in library";
    ORACLE_ERRNO[-OB_CANT_FIND_DL_ENTRY] = 600;
    ORACLE_STR_ERROR[-OB_CANT_FIND_DL_ENTRY] = "ORA-00600: internal error code, arguments: -4296, Can't find symbol";
    ORACLE_STR_USER_ERROR[-OB_CANT_FIND_DL_ENTRY] =
        "ORA-00600: internal error code, arguments: -4296, Can't find symbol %.*s in library";
    ERROR_NAME[-OB_OBJECT_NAME_EXIST] = "OB_OBJECT_NAME_EXIST";
    MYSQL_ERRNO[-OB_OBJECT_NAME_EXIST] = -1;
    SQLSTATE[-OB_OBJECT_NAME_EXIST] = "HY000";
    STR_ERROR[-OB_OBJECT_NAME_EXIST] = "name is already used by an existing object";
    STR_USER_ERROR[-OB_OBJECT_NAME_EXIST] = "name is already used by an existing object";
    ORACLE_ERRNO[-OB_OBJECT_NAME_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_OBJECT_NAME_EXIST] =
        "ORA-00600: internal error code, arguments: -4297, name is already used by an existing object";
    ORACLE_STR_USER_ERROR[-OB_OBJECT_NAME_EXIST] =
        "ORA-00600: internal error code, arguments: -4297, name is already used by an existing object";
    ERROR_NAME[-OB_OBJECT_NAME_NOT_EXIST] = "OB_OBJECT_NAME_NOT_EXIST";
    MYSQL_ERRNO[-OB_OBJECT_NAME_NOT_EXIST] = -1;
    SQLSTATE[-OB_OBJECT_NAME_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_OBJECT_NAME_NOT_EXIST] = "object does not exist";
    STR_USER_ERROR[-OB_OBJECT_NAME_NOT_EXIST] = "%s does not exist";
    ORACLE_ERRNO[-OB_OBJECT_NAME_NOT_EXIST] = 2289;
    ORACLE_STR_ERROR[-OB_OBJECT_NAME_NOT_EXIST] = "ORA-02289: object does not exist";
    ORACLE_STR_USER_ERROR[-OB_OBJECT_NAME_NOT_EXIST] = "ORA-02289: %s does not exist";
    ERROR_NAME[-OB_ERR_DUP_ARGUMENT] = "OB_ERR_DUP_ARGUMENT";
    MYSQL_ERRNO[-OB_ERR_DUP_ARGUMENT] = ER_DUP_ARGUMENT;
    SQLSTATE[-OB_ERR_DUP_ARGUMENT] = "HY000";
    STR_ERROR[-OB_ERR_DUP_ARGUMENT] = "Option used twice in statement";
    STR_USER_ERROR[-OB_ERR_DUP_ARGUMENT] = "Option '%s' used twice in statement";
    ORACLE_ERRNO[-OB_ERR_DUP_ARGUMENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DUP_ARGUMENT] =
        "ORA-00600: internal error code, arguments: -4299, Option used twice in statement";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_ARGUMENT] =
        "ORA-00600: internal error code, arguments: -4299, Option '%s' used twice in statement";
    ERROR_NAME[-OB_ERR_INVALID_SEQUENCE_NAME] = "OB_ERR_INVALID_SEQUENCE_NAME";
    MYSQL_ERRNO[-OB_ERR_INVALID_SEQUENCE_NAME] = -1;
    SQLSTATE[-OB_ERR_INVALID_SEQUENCE_NAME] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SEQUENCE_NAME] = "invalid sequence name";
    STR_USER_ERROR[-OB_ERR_INVALID_SEQUENCE_NAME] = "invalid sequence name";
    ORACLE_ERRNO[-OB_ERR_INVALID_SEQUENCE_NAME] = 2277;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SEQUENCE_NAME] = "ORA-02277: invalid sequence name";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SEQUENCE_NAME] = "ORA-02277: invalid sequence name";
    ERROR_NAME[-OB_ERR_DUP_MAXVALUE_SPEC] = "OB_ERR_DUP_MAXVALUE_SPEC";
    MYSQL_ERRNO[-OB_ERR_DUP_MAXVALUE_SPEC] = -1;
    SQLSTATE[-OB_ERR_DUP_MAXVALUE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_DUP_MAXVALUE_SPEC] = "duplicate MAXVALUE/NOMAXVALUE specifications";
    STR_USER_ERROR[-OB_ERR_DUP_MAXVALUE_SPEC] = "duplicate MAXVALUE/NOMAXVALUE specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_MAXVALUE_SPEC] = 2278;
    ORACLE_STR_ERROR[-OB_ERR_DUP_MAXVALUE_SPEC] = "ORA-02278: duplicate MAXVALUE/NOMAXVALUE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_MAXVALUE_SPEC] = "ORA-02278: duplicate MAXVALUE/NOMAXVALUE specifications";
    ERROR_NAME[-OB_ERR_DUP_MINVALUE_SPEC] = "OB_ERR_DUP_MINVALUE_SPEC";
    MYSQL_ERRNO[-OB_ERR_DUP_MINVALUE_SPEC] = -1;
    SQLSTATE[-OB_ERR_DUP_MINVALUE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_DUP_MINVALUE_SPEC] = "duplicate MINVALUE/NOMINVALUE specifications";
    STR_USER_ERROR[-OB_ERR_DUP_MINVALUE_SPEC] = "duplicate MINVALUE/NOMINVALUE specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_MINVALUE_SPEC] = 2279;
    ORACLE_STR_ERROR[-OB_ERR_DUP_MINVALUE_SPEC] = "ORA-02279: duplicate MINVALUE/NOMINVALUE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_MINVALUE_SPEC] = "ORA-02279: duplicate MINVALUE/NOMINVALUE specifications";
    ERROR_NAME[-OB_ERR_DUP_CYCLE_SPEC] = "OB_ERR_DUP_CYCLE_SPEC";
    MYSQL_ERRNO[-OB_ERR_DUP_CYCLE_SPEC] = -1;
    SQLSTATE[-OB_ERR_DUP_CYCLE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_DUP_CYCLE_SPEC] = "duplicate CYCLE/NOCYCLE specifications";
    STR_USER_ERROR[-OB_ERR_DUP_CYCLE_SPEC] = "duplicate CYCLE/NOCYCLE specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_CYCLE_SPEC] = 2280;
    ORACLE_STR_ERROR[-OB_ERR_DUP_CYCLE_SPEC] = "ORA-02280: duplicate CYCLE/NOCYCLE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_CYCLE_SPEC] = "ORA-02280: duplicate CYCLE/NOCYCLE specifications";
    ERROR_NAME[-OB_ERR_DUP_CACHE_SPEC] = "OB_ERR_DUP_CACHE_SPEC";
    MYSQL_ERRNO[-OB_ERR_DUP_CACHE_SPEC] = -1;
    SQLSTATE[-OB_ERR_DUP_CACHE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_DUP_CACHE_SPEC] = "duplicate CACHE/NOCACHE specifications";
    STR_USER_ERROR[-OB_ERR_DUP_CACHE_SPEC] = "duplicate CACHE/NOCACHE specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_CACHE_SPEC] = 2281;
    ORACLE_STR_ERROR[-OB_ERR_DUP_CACHE_SPEC] = "ORA-02281: duplicate CACHE/NOCACHE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_CACHE_SPEC] = "ORA-02281: duplicate CACHE/NOCACHE specifications";
    ERROR_NAME[-OB_ERR_DUP_ORDER_SPEC] = "OB_ERR_DUP_ORDER_SPEC";
    MYSQL_ERRNO[-OB_ERR_DUP_ORDER_SPEC] = -1;
    SQLSTATE[-OB_ERR_DUP_ORDER_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_DUP_ORDER_SPEC] = "duplicate ORDER/NOORDER specifications";
    STR_USER_ERROR[-OB_ERR_DUP_ORDER_SPEC] = "duplicate ORDER/NOORDER specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_ORDER_SPEC] = 2282;
    ORACLE_STR_ERROR[-OB_ERR_DUP_ORDER_SPEC] = "ORA-02282: duplicate ORDER/NOORDER specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_ORDER_SPEC] = "ORA-02282: duplicate ORDER/NOORDER specifications";
    ERROR_NAME[-OB_ERR_CONFL_MAXVALUE_SPEC] = "OB_ERR_CONFL_MAXVALUE_SPEC";
    MYSQL_ERRNO[-OB_ERR_CONFL_MAXVALUE_SPEC] = -1;
    SQLSTATE[-OB_ERR_CONFL_MAXVALUE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_CONFL_MAXVALUE_SPEC] = "conflicting MAXVALUE/NOMAXVALUE specifications";
    STR_USER_ERROR[-OB_ERR_CONFL_MAXVALUE_SPEC] = "conflicting MAXVALUE/NOMAXVALUE specifications";
    ORACLE_ERRNO[-OB_ERR_CONFL_MAXVALUE_SPEC] = 2278;
    ORACLE_STR_ERROR[-OB_ERR_CONFL_MAXVALUE_SPEC] = "ORA-02278: conflicting MAXVALUE/NOMAXVALUE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONFL_MAXVALUE_SPEC] = "ORA-02278: conflicting MAXVALUE/NOMAXVALUE specifications";
    ERROR_NAME[-OB_ERR_CONFL_MINVALUE_SPEC] = "OB_ERR_CONFL_MINVALUE_SPEC";
    MYSQL_ERRNO[-OB_ERR_CONFL_MINVALUE_SPEC] = -1;
    SQLSTATE[-OB_ERR_CONFL_MINVALUE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_CONFL_MINVALUE_SPEC] = "conflicting MINVALUE/NOMINVALUE specifications";
    STR_USER_ERROR[-OB_ERR_CONFL_MINVALUE_SPEC] = "conflicting MINVALUE/NOMINVALUE specifications";
    ORACLE_ERRNO[-OB_ERR_CONFL_MINVALUE_SPEC] = 2279;
    ORACLE_STR_ERROR[-OB_ERR_CONFL_MINVALUE_SPEC] = "ORA-02279: conflicting MINVALUE/NOMINVALUE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONFL_MINVALUE_SPEC] = "ORA-02279: conflicting MINVALUE/NOMINVALUE specifications";
    ERROR_NAME[-OB_ERR_CONFL_CYCLE_SPEC] = "OB_ERR_CONFL_CYCLE_SPEC";
    MYSQL_ERRNO[-OB_ERR_CONFL_CYCLE_SPEC] = -1;
    SQLSTATE[-OB_ERR_CONFL_CYCLE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_CONFL_CYCLE_SPEC] = "conflicting CYCLE/NOCYCLE specifications";
    STR_USER_ERROR[-OB_ERR_CONFL_CYCLE_SPEC] = "conflicting CYCLE/NOCYCLE specifications";
    ORACLE_ERRNO[-OB_ERR_CONFL_CYCLE_SPEC] = 2280;
    ORACLE_STR_ERROR[-OB_ERR_CONFL_CYCLE_SPEC] = "ORA-02280: conflicting CYCLE/NOCYCLE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONFL_CYCLE_SPEC] = "ORA-02280: conflicting CYCLE/NOCYCLE specifications";
    ERROR_NAME[-OB_ERR_CONFL_CACHE_SPEC] = "OB_ERR_CONFL_CACHE_SPEC";
    MYSQL_ERRNO[-OB_ERR_CONFL_CACHE_SPEC] = -1;
    SQLSTATE[-OB_ERR_CONFL_CACHE_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_CONFL_CACHE_SPEC] = "conflicting CACHE/NOCACHE specifications";
    STR_USER_ERROR[-OB_ERR_CONFL_CACHE_SPEC] = "conflicting CACHE/NOCACHE specifications";
    ORACLE_ERRNO[-OB_ERR_CONFL_CACHE_SPEC] = 2281;
    ORACLE_STR_ERROR[-OB_ERR_CONFL_CACHE_SPEC] = "ORA-02281: conflicting CACHE/NOCACHE specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONFL_CACHE_SPEC] = "ORA-02281: conflicting CACHE/NOCACHE specifications";
    ERROR_NAME[-OB_ERR_CONFL_ORDER_SPEC] = "OB_ERR_CONFL_ORDER_SPEC";
    MYSQL_ERRNO[-OB_ERR_CONFL_ORDER_SPEC] = -1;
    SQLSTATE[-OB_ERR_CONFL_ORDER_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_CONFL_ORDER_SPEC] = "conflicting ORDER/NOORDER specifications";
    STR_USER_ERROR[-OB_ERR_CONFL_ORDER_SPEC] = "conflicting ORDER/NOORDER specifications";
    ORACLE_ERRNO[-OB_ERR_CONFL_ORDER_SPEC] = 2282;
    ORACLE_STR_ERROR[-OB_ERR_CONFL_ORDER_SPEC] = "ORA-02282: conflicting ORDER/NOORDER specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONFL_ORDER_SPEC] = "ORA-02282: conflicting ORDER/NOORDER specifications";
    ERROR_NAME[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] = "OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] = "cannot alter starting sequence number";
    STR_USER_ERROR[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] = "cannot alter starting sequence number";
    ORACLE_ERRNO[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] = 2283;
    ORACLE_STR_ERROR[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] = "ORA-02283: cannot alter starting sequence number";
    ORACLE_STR_USER_ERROR[-OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED] =
        "ORA-02283: cannot alter starting sequence number";
    ERROR_NAME[-OB_ERR_DUP_INCREMENT_BY_SPEC] = "OB_ERR_DUP_INCREMENT_BY_SPEC";
    MYSQL_ERRNO[-OB_ERR_DUP_INCREMENT_BY_SPEC] = -1;
    SQLSTATE[-OB_ERR_DUP_INCREMENT_BY_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_DUP_INCREMENT_BY_SPEC] = "duplicate INCREMENT BY specifications";
    STR_USER_ERROR[-OB_ERR_DUP_INCREMENT_BY_SPEC] = "duplicate INCREMENT BY specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_INCREMENT_BY_SPEC] = 2284;
    ORACLE_STR_ERROR[-OB_ERR_DUP_INCREMENT_BY_SPEC] = "ORA-02284: duplicate INCREMENT BY specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_INCREMENT_BY_SPEC] = "ORA-02284: duplicate INCREMENT BY specifications";
    ERROR_NAME[-OB_ERR_DUP_START_WITH_SPEC] = "OB_ERR_DUP_START_WITH_SPEC";
    MYSQL_ERRNO[-OB_ERR_DUP_START_WITH_SPEC] = -1;
    SQLSTATE[-OB_ERR_DUP_START_WITH_SPEC] = "HY000";
    STR_ERROR[-OB_ERR_DUP_START_WITH_SPEC] = "duplicate START WITH specifications";
    STR_USER_ERROR[-OB_ERR_DUP_START_WITH_SPEC] = "duplicate START WITH specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_START_WITH_SPEC] = 2285;
    ORACLE_STR_ERROR[-OB_ERR_DUP_START_WITH_SPEC] = "ORA-02285: duplicate START WITH specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_START_WITH_SPEC] = "ORA-02285: duplicate START WITH specifications";
    ERROR_NAME[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = "OB_ERR_REQUIRE_ALTER_SEQ_OPTION";
    MYSQL_ERRNO[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = -1;
    SQLSTATE[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = "HY000";
    STR_ERROR[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = "no options specified for ALTER SEQUENCE";
    STR_USER_ERROR[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = "no options specified for ALTER SEQUENCE";
    ORACLE_ERRNO[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = 2286;
    ORACLE_STR_ERROR[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = "ORA-02286: no options specified for ALTER SEQUENCE";
    ORACLE_STR_USER_ERROR[-OB_ERR_REQUIRE_ALTER_SEQ_OPTION] = "ORA-02286: no options specified for ALTER SEQUENCE";
    ERROR_NAME[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = "OB_ERR_SEQ_NOT_ALLOWED_HERE";
    MYSQL_ERRNO[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = -1;
    SQLSTATE[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = "sequence number not allowed here";
    STR_USER_ERROR[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = "sequence number not allowed here";
    ORACLE_ERRNO[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = 2287;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = "ORA-02287: sequence number not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_NOT_ALLOWED_HERE] = "ORA-02287: sequence number not allowed here";
    ERROR_NAME[-OB_ERR_SEQ_NOT_EXIST] = "OB_ERR_SEQ_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_SEQ_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_SEQ_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_NOT_EXIST] = "sequence does not exist";
    STR_USER_ERROR[-OB_ERR_SEQ_NOT_EXIST] = "sequence does not exist";
    ORACLE_ERRNO[-OB_ERR_SEQ_NOT_EXIST] = 2289;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_NOT_EXIST] = "ORA-02289: sequence does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_NOT_EXIST] = "ORA-02289: sequence does not exist";
    ERROR_NAME[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = "OB_ERR_SEQ_OPTION_MUST_BE_INTEGER";
    MYSQL_ERRNO[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = -1;
    SQLSTATE[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = "sequence parameter must be an integer";
    STR_USER_ERROR[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = "sequence parameter %s must be an integer";
    ORACLE_ERRNO[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = 4001;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = "ORA-04001: sequence parameter must be an integer";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_OPTION_MUST_BE_INTEGER] = "ORA-04001: sequence parameter %s must be an integer";
    ERROR_NAME[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = "OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO";
    MYSQL_ERRNO[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = -1;
    SQLSTATE[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = "INCREMENT must be a nonzero integer";
    STR_USER_ERROR[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = "INCREMENT must be a nonzero integer";
    ORACLE_ERRNO[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = 4002;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = "ORA-04002: INCREMENT must be a nonzero integer";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO] = "ORA-04002: INCREMENT must be a nonzero integer";
    ERROR_NAME[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] = "OB_ERR_SEQ_OPTION_EXCEED_RANGE";
    MYSQL_ERRNO[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] = -1;
    SQLSTATE[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] = "sequence parameter exceeds maximum size allowed";
    STR_USER_ERROR[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] = "sequence parameter exceeds maximum size allowed";
    ORACLE_ERRNO[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] = 4003;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] = "ORA-04003: sequence parameter exceeds maximum size allowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_OPTION_EXCEED_RANGE] =
        "ORA-04003: sequence parameter exceeds maximum size allowed";
    ERROR_NAME[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = "OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE";
    MYSQL_ERRNO[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = -1;
    SQLSTATE[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = "HY000";
    STR_ERROR[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = "MINVALUE must be less than MAXVALUE";
    STR_USER_ERROR[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = "MINVALUE must be less than MAXVALUE";
    ORACLE_ERRNO[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = 4004;
    ORACLE_STR_ERROR[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = "ORA-04004: MINVALUE must be less than MAXVALUE";
    ORACLE_STR_USER_ERROR[-OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE] = "ORA-04004: MINVALUE must be less than MAXVALUE";
    ERROR_NAME[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] = "OB_ERR_SEQ_INCREMENT_TOO_LARGE";
    MYSQL_ERRNO[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] = -1;
    SQLSTATE[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] = "INCREMENT must be less than MAXVALUE minus MINVALUE";
    STR_USER_ERROR[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] = "INCREMENT must be less than MAXVALUE minus MINVALUE";
    ORACLE_ERRNO[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] = 4005;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] =
        "ORA-04005: INCREMENT must be less than MAXVALUE minus MINVALUE";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_INCREMENT_TOO_LARGE] =
        "ORA-04005: INCREMENT must be less than MAXVALUE minus MINVALUE";
    ERROR_NAME[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = "OB_ERR_START_WITH_LESS_THAN_MINVALUE";
    MYSQL_ERRNO[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = -1;
    SQLSTATE[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = "HY000";
    STR_ERROR[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = "START WITH cannot be less than MINVALUE";
    STR_USER_ERROR[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = "START WITH cannot be less than MINVALUE";
    ORACLE_ERRNO[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = 4006;
    ORACLE_STR_ERROR[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = "ORA-04006: START WITH cannot be less than MINVALUE";
    ORACLE_STR_USER_ERROR[-OB_ERR_START_WITH_LESS_THAN_MINVALUE] = "ORA-04006: START WITH cannot be less than MINVALUE";
    ERROR_NAME[-OB_ERR_MINVALUE_EXCEED_CURRVAL] = "OB_ERR_MINVALUE_EXCEED_CURRVAL";
    MYSQL_ERRNO[-OB_ERR_MINVALUE_EXCEED_CURRVAL] = -1;
    SQLSTATE[-OB_ERR_MINVALUE_EXCEED_CURRVAL] = "HY000";
    STR_ERROR[-OB_ERR_MINVALUE_EXCEED_CURRVAL] = "MINVALUE cannot be made to exceed the current value";
    STR_USER_ERROR[-OB_ERR_MINVALUE_EXCEED_CURRVAL] = "MINVALUE cannot be made to exceed the current value";
    ORACLE_ERRNO[-OB_ERR_MINVALUE_EXCEED_CURRVAL] = 4007;
    ORACLE_STR_ERROR[-OB_ERR_MINVALUE_EXCEED_CURRVAL] =
        "ORA-04007: MINVALUE cannot be made to exceed the current value";
    ORACLE_STR_USER_ERROR[-OB_ERR_MINVALUE_EXCEED_CURRVAL] =
        "ORA-04007: MINVALUE cannot be made to exceed the current value";
    ERROR_NAME[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = "OB_ERR_START_WITH_EXCEED_MAXVALUE";
    MYSQL_ERRNO[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = -1;
    SQLSTATE[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = "HY000";
    STR_ERROR[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = "START WITH cannot be more than MAXVALUE";
    STR_USER_ERROR[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = "START WITH cannot be more than MAXVALUE";
    ORACLE_ERRNO[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = 4008;
    ORACLE_STR_ERROR[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = "ORA-04008: START WITH cannot be more than MAXVALUE";
    ORACLE_STR_USER_ERROR[-OB_ERR_START_WITH_EXCEED_MAXVALUE] = "ORA-04008: START WITH cannot be more than MAXVALUE";
    ERROR_NAME[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] = "OB_ERR_MAXVALUE_EXCEED_CURRVAL";
    MYSQL_ERRNO[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] = -1;
    SQLSTATE[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] = "HY000";
    STR_ERROR[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] = "MAXVALUE cannot be made to be less than the current value";
    STR_USER_ERROR[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] = "MAXVALUE cannot be made to be less than the current value";
    ORACLE_ERRNO[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] = 4009;
    ORACLE_STR_ERROR[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] =
        "ORA-04009: MAXVALUE cannot be made to be less than the current value";
    ORACLE_STR_USER_ERROR[-OB_ERR_MAXVALUE_EXCEED_CURRVAL] =
        "ORA-04009: MAXVALUE cannot be made to be less than the current value";
    ERROR_NAME[-OB_ERR_SEQ_CACHE_TOO_SMALL] = "OB_ERR_SEQ_CACHE_TOO_SMALL";
    MYSQL_ERRNO[-OB_ERR_SEQ_CACHE_TOO_SMALL] = -1;
    SQLSTATE[-OB_ERR_SEQ_CACHE_TOO_SMALL] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_CACHE_TOO_SMALL] = "the number of values to CACHE must be greater than 1";
    STR_USER_ERROR[-OB_ERR_SEQ_CACHE_TOO_SMALL] = "the number of values to CACHE must be greater than 1";
    ORACLE_ERRNO[-OB_ERR_SEQ_CACHE_TOO_SMALL] = 4010;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_CACHE_TOO_SMALL] = "ORA-04010: the number of values to CACHE must be greater than 1";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_CACHE_TOO_SMALL] =
        "ORA-04010: the number of values to CACHE must be greater than 1";
    ERROR_NAME[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = "OB_ERR_SEQ_OPTION_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = "sequence option value out of range";
    STR_USER_ERROR[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = "sequence option value out of range";
    ORACLE_ERRNO[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = 4011;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = "ORA-04011: sequence option value out of range";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_OPTION_OUT_OF_RANGE] = "ORA-04011: sequence option value out of range";
    ERROR_NAME[-OB_ERR_SEQ_CACHE_TOO_LARGE] = "OB_ERR_SEQ_CACHE_TOO_LARGE";
    MYSQL_ERRNO[-OB_ERR_SEQ_CACHE_TOO_LARGE] = -1;
    SQLSTATE[-OB_ERR_SEQ_CACHE_TOO_LARGE] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_CACHE_TOO_LARGE] = "number to CACHE must be less than one cycle";
    STR_USER_ERROR[-OB_ERR_SEQ_CACHE_TOO_LARGE] = "number to CACHE must be less than one cycle";
    ORACLE_ERRNO[-OB_ERR_SEQ_CACHE_TOO_LARGE] = 4013;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_CACHE_TOO_LARGE] = "ORA-04013: number to CACHE must be less than one cycle";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_CACHE_TOO_LARGE] = "ORA-04013: number to CACHE must be less than one cycle";
    ERROR_NAME[-OB_ERR_SEQ_REQUIRE_MINVALUE] = "OB_ERR_SEQ_REQUIRE_MINVALUE";
    MYSQL_ERRNO[-OB_ERR_SEQ_REQUIRE_MINVALUE] = -1;
    SQLSTATE[-OB_ERR_SEQ_REQUIRE_MINVALUE] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_REQUIRE_MINVALUE] = "descending sequences that CYCLE must specify MINVALUE";
    STR_USER_ERROR[-OB_ERR_SEQ_REQUIRE_MINVALUE] = "descending sequences that CYCLE must specify MINVALUE";
    ORACLE_ERRNO[-OB_ERR_SEQ_REQUIRE_MINVALUE] = 4014;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_REQUIRE_MINVALUE] = "ORA-04014: descending sequences that CYCLE must specify MINVALUE";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_REQUIRE_MINVALUE] =
        "ORA-04014: descending sequences that CYCLE must specify MINVALUE";
    ERROR_NAME[-OB_ERR_SEQ_REQUIRE_MAXVALUE] = "OB_ERR_SEQ_REQUIRE_MAXVALUE";
    MYSQL_ERRNO[-OB_ERR_SEQ_REQUIRE_MAXVALUE] = -1;
    SQLSTATE[-OB_ERR_SEQ_REQUIRE_MAXVALUE] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_REQUIRE_MAXVALUE] = "ascending sequences that CYCLE must specify MAXVALUE";
    STR_USER_ERROR[-OB_ERR_SEQ_REQUIRE_MAXVALUE] = "ascending sequences that CYCLE must specify MAXVALUE";
    ORACLE_ERRNO[-OB_ERR_SEQ_REQUIRE_MAXVALUE] = 4015;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_REQUIRE_MAXVALUE] = "ORA-04015: ascending sequences that CYCLE must specify MAXVALUE";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_REQUIRE_MAXVALUE] =
        "ORA-04015: ascending sequences that CYCLE must specify MAXVALUE";
    ERROR_NAME[-OB_ERR_SEQ_NO_LONGER_EXIST] = "OB_ERR_SEQ_NO_LONGER_EXIST";
    MYSQL_ERRNO[-OB_ERR_SEQ_NO_LONGER_EXIST] = -1;
    SQLSTATE[-OB_ERR_SEQ_NO_LONGER_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_NO_LONGER_EXIST] = "sequence no longer exists";
    STR_USER_ERROR[-OB_ERR_SEQ_NO_LONGER_EXIST] = "sequence %s no longer exists";
    ORACLE_ERRNO[-OB_ERR_SEQ_NO_LONGER_EXIST] = 4015;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_NO_LONGER_EXIST] = "ORA-04015: sequence no longer exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_NO_LONGER_EXIST] = "ORA-04015: sequence %s no longer exists";
    ERROR_NAME[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = "OB_ERR_SEQ_VALUE_EXCEED_LIMIT";
    MYSQL_ERRNO[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = -1;
    SQLSTATE[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = "HY000";
    STR_ERROR[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = "sequence exceeds limit and cannot be instantiated";
    STR_USER_ERROR[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = "sequence exceeds %s and cannot be instantiated";
    ORACLE_ERRNO[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = 8004;
    ORACLE_STR_ERROR[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = "ORA-08004: sequence exceeds limit and cannot be instantiated";
    ORACLE_STR_USER_ERROR[-OB_ERR_SEQ_VALUE_EXCEED_LIMIT] = "ORA-08004: sequence exceeds %s and cannot be instantiated";
    ERROR_NAME[-OB_ERR_DIVISOR_IS_ZERO] = "OB_ERR_DIVISOR_IS_ZERO";
    MYSQL_ERRNO[-OB_ERR_DIVISOR_IS_ZERO] = -1;
    SQLSTATE[-OB_ERR_DIVISOR_IS_ZERO] = "HY000";
    STR_ERROR[-OB_ERR_DIVISOR_IS_ZERO] = "divisor is equal to zero";
    STR_USER_ERROR[-OB_ERR_DIVISOR_IS_ZERO] = "divisor is equal to zero";
    ORACLE_ERRNO[-OB_ERR_DIVISOR_IS_ZERO] = 1476;
    ORACLE_STR_ERROR[-OB_ERR_DIVISOR_IS_ZERO] = "ORA-01476: divisor is equal to zero";
    ORACLE_STR_USER_ERROR[-OB_ERR_DIVISOR_IS_ZERO] = "ORA-01476: divisor is equal to zero";
    ERROR_NAME[-OB_ERR_AES_IV_LENGTH] = "OB_ERR_AES_IV_LENGTH";
    MYSQL_ERRNO[-OB_ERR_AES_IV_LENGTH] = ER_AES_INVALID_IV;
    SQLSTATE[-OB_ERR_AES_IV_LENGTH] = "HY000";
    STR_ERROR[-OB_ERR_AES_IV_LENGTH] =
        "The initialization vector supplied to aes_encrypt is too short. Must be at least 16 bytes long";
    STR_USER_ERROR[-OB_ERR_AES_IV_LENGTH] =
        "The initialization vector supplied to aes_encrypt is too short. Must be at least 16 bytes long";
    ORACLE_ERRNO[-OB_ERR_AES_IV_LENGTH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_AES_IV_LENGTH] =
        "ORA-00600: internal error code, arguments: -4336, The initialization vector supplied to aes_encrypt is too "
        "short. Must be at least 16 bytes long";
    ORACLE_STR_USER_ERROR[-OB_ERR_AES_IV_LENGTH] =
        "ORA-00600: internal error code, arguments: -4336, The initialization vector supplied to aes_encrypt is too "
        "short. Must be at least 16 bytes long";
    ERROR_NAME[-OB_STORE_DIR_ERROR] = "OB_STORE_DIR_ERROR";
    MYSQL_ERRNO[-OB_STORE_DIR_ERROR] = -1;
    SQLSTATE[-OB_STORE_DIR_ERROR] = "HY000";
    STR_ERROR[-OB_STORE_DIR_ERROR] = "store directory structure error";
    STR_USER_ERROR[-OB_STORE_DIR_ERROR] = "store directory structure error";
    ORACLE_ERRNO[-OB_STORE_DIR_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_STORE_DIR_ERROR] =
        "ORA-00600: internal error code, arguments: -4337, store directory structure error";
    ORACLE_STR_USER_ERROR[-OB_STORE_DIR_ERROR] =
        "ORA-00600: internal error code, arguments: -4337, store directory structure error";
    ERROR_NAME[-OB_OPEN_TWICE] = "OB_OPEN_TWICE";
    MYSQL_ERRNO[-OB_OPEN_TWICE] = -1;
    SQLSTATE[-OB_OPEN_TWICE] = "HY000";
    STR_ERROR[-OB_OPEN_TWICE] = "open twice";
    STR_USER_ERROR[-OB_OPEN_TWICE] = "open twice";
    ORACLE_ERRNO[-OB_OPEN_TWICE] = 600;
    ORACLE_STR_ERROR[-OB_OPEN_TWICE] = "ORA-00600: internal error code, arguments: -4338, open twice";
    ORACLE_STR_USER_ERROR[-OB_OPEN_TWICE] = "ORA-00600: internal error code, arguments: -4338, open twice";
    ERROR_NAME[-OB_RAID_SUPER_BLOCK_NOT_MATCH] = "OB_RAID_SUPER_BLOCK_NOT_MATCH";
    MYSQL_ERRNO[-OB_RAID_SUPER_BLOCK_NOT_MATCH] = -1;
    SQLSTATE[-OB_RAID_SUPER_BLOCK_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_RAID_SUPER_BLOCK_NOT_MATCH] = "raid super block not match";
    STR_USER_ERROR[-OB_RAID_SUPER_BLOCK_NOT_MATCH] = "raid super block not match";
    ORACLE_ERRNO[-OB_RAID_SUPER_BLOCK_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_RAID_SUPER_BLOCK_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4339, raid super block not match";
    ORACLE_STR_USER_ERROR[-OB_RAID_SUPER_BLOCK_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4339, raid super block not match";
    ERROR_NAME[-OB_NOT_OPEN] = "OB_NOT_OPEN";
    MYSQL_ERRNO[-OB_NOT_OPEN] = -1;
    SQLSTATE[-OB_NOT_OPEN] = "HY000";
    STR_ERROR[-OB_NOT_OPEN] = "not opened";
    STR_USER_ERROR[-OB_NOT_OPEN] = "not opened";
    ORACLE_ERRNO[-OB_NOT_OPEN] = 600;
    ORACLE_STR_ERROR[-OB_NOT_OPEN] = "ORA-00600: internal error code, arguments: -4340, not opened";
    ORACLE_STR_USER_ERROR[-OB_NOT_OPEN] = "ORA-00600: internal error code, arguments: -4340, not opened";
    ERROR_NAME[-OB_NOT_IN_SERVICE] = "OB_NOT_IN_SERVICE";
    MYSQL_ERRNO[-OB_NOT_IN_SERVICE] = -1;
    SQLSTATE[-OB_NOT_IN_SERVICE] = "HY000";
    STR_ERROR[-OB_NOT_IN_SERVICE] = "target module is not in service";
    STR_USER_ERROR[-OB_NOT_IN_SERVICE] = "target module is not in service";
    ORACLE_ERRNO[-OB_NOT_IN_SERVICE] = 600;
    ORACLE_STR_ERROR[-OB_NOT_IN_SERVICE] =
        "ORA-00600: internal error code, arguments: -4341, target module is not in service";
    ORACLE_STR_USER_ERROR[-OB_NOT_IN_SERVICE] =
        "ORA-00600: internal error code, arguments: -4341, target module is not in service";
    ERROR_NAME[-OB_RAID_DISK_NOT_NORMAL] = "OB_RAID_DISK_NOT_NORMAL";
    MYSQL_ERRNO[-OB_RAID_DISK_NOT_NORMAL] = -1;
    SQLSTATE[-OB_RAID_DISK_NOT_NORMAL] = "HY000";
    STR_ERROR[-OB_RAID_DISK_NOT_NORMAL] = "raid disk not in normal status";
    STR_USER_ERROR[-OB_RAID_DISK_NOT_NORMAL] = "raid disk not in normal status";
    ORACLE_ERRNO[-OB_RAID_DISK_NOT_NORMAL] = 600;
    ORACLE_STR_ERROR[-OB_RAID_DISK_NOT_NORMAL] =
        "ORA-00600: internal error code, arguments: -4342, raid disk not in normal status";
    ORACLE_STR_USER_ERROR[-OB_RAID_DISK_NOT_NORMAL] =
        "ORA-00600: internal error code, arguments: -4342, raid disk not in normal status";
    ERROR_NAME[-OB_TENANT_SCHEMA_NOT_FULL] = "OB_TENANT_SCHEMA_NOT_FULL";
    MYSQL_ERRNO[-OB_TENANT_SCHEMA_NOT_FULL] = -1;
    SQLSTATE[-OB_TENANT_SCHEMA_NOT_FULL] = "HY000";
    STR_ERROR[-OB_TENANT_SCHEMA_NOT_FULL] = "tenant schema is not full";
    STR_USER_ERROR[-OB_TENANT_SCHEMA_NOT_FULL] = "tenant schema is not full";
    ORACLE_ERRNO[-OB_TENANT_SCHEMA_NOT_FULL] = 600;
    ORACLE_STR_ERROR[-OB_TENANT_SCHEMA_NOT_FULL] =
        "ORA-00600: internal error code, arguments: -4343, tenant schema is not full";
    ORACLE_STR_USER_ERROR[-OB_TENANT_SCHEMA_NOT_FULL] =
        "ORA-00600: internal error code, arguments: -4343, tenant schema is not full";
    ERROR_NAME[-OB_INVALID_QUERY_TIMESTAMP] = "OB_INVALID_QUERY_TIMESTAMP";
    MYSQL_ERRNO[-OB_INVALID_QUERY_TIMESTAMP] = -1;
    SQLSTATE[-OB_INVALID_QUERY_TIMESTAMP] = "HY000";
    STR_ERROR[-OB_INVALID_QUERY_TIMESTAMP] = "invalid timestamp";
    STR_USER_ERROR[-OB_INVALID_QUERY_TIMESTAMP] = "invalid timestamp";
    ORACLE_ERRNO[-OB_INVALID_QUERY_TIMESTAMP] = 8186;
    ORACLE_STR_ERROR[-OB_INVALID_QUERY_TIMESTAMP] = "ORA-08186: invalid timestamp";
    ORACLE_STR_USER_ERROR[-OB_INVALID_QUERY_TIMESTAMP] = "ORA-08186: invalid timestamp";
    ERROR_NAME[-OB_DIR_NOT_EMPTY] = "OB_DIR_NOT_EMPTY";
    MYSQL_ERRNO[-OB_DIR_NOT_EMPTY] = -1;
    SQLSTATE[-OB_DIR_NOT_EMPTY] = "HY000";
    STR_ERROR[-OB_DIR_NOT_EMPTY] = "dir not empty";
    STR_USER_ERROR[-OB_DIR_NOT_EMPTY] = "dir not empty";
    ORACLE_ERRNO[-OB_DIR_NOT_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_DIR_NOT_EMPTY] = "ORA-00600: internal error code, arguments: -4345, dir not empty";
    ORACLE_STR_USER_ERROR[-OB_DIR_NOT_EMPTY] = "ORA-00600: internal error code, arguments: -4345, dir not empty";
    ERROR_NAME[-OB_SCHEMA_NOT_UPTODATE] = "OB_SCHEMA_NOT_UPTODATE";
    MYSQL_ERRNO[-OB_SCHEMA_NOT_UPTODATE] = -1;
    SQLSTATE[-OB_SCHEMA_NOT_UPTODATE] = "HY000";
    STR_ERROR[-OB_SCHEMA_NOT_UPTODATE] = "schema is not up to date for read";
    STR_USER_ERROR[-OB_SCHEMA_NOT_UPTODATE] = "schema is not up to date for read";
    ORACLE_ERRNO[-OB_SCHEMA_NOT_UPTODATE] = 600;
    ORACLE_STR_ERROR[-OB_SCHEMA_NOT_UPTODATE] =
        "ORA-00600: internal error code, arguments: -4346, schema is not up to date for read";
    ORACLE_STR_USER_ERROR[-OB_SCHEMA_NOT_UPTODATE] =
        "ORA-00600: internal error code, arguments: -4346, schema is not up to date for read";
    ERROR_NAME[-OB_ROLE_NOT_EXIST] = "OB_ROLE_NOT_EXIST";
    MYSQL_ERRNO[-OB_ROLE_NOT_EXIST] = -1;
    SQLSTATE[-OB_ROLE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ROLE_NOT_EXIST] = "role does not exist";
    STR_USER_ERROR[-OB_ROLE_NOT_EXIST] = "role '%.*s' does not exist";
    ORACLE_ERRNO[-OB_ROLE_NOT_EXIST] = 1919;
    ORACLE_STR_ERROR[-OB_ROLE_NOT_EXIST] = "ORA-01919: role does not exist";
    ORACLE_STR_USER_ERROR[-OB_ROLE_NOT_EXIST] = "ORA-01919: role '%.*s' does not exist";
    ERROR_NAME[-OB_ROLE_EXIST] = "OB_ROLE_EXIST";
    MYSQL_ERRNO[-OB_ROLE_EXIST] = -1;
    SQLSTATE[-OB_ROLE_EXIST] = "HY000";
    STR_ERROR[-OB_ROLE_EXIST] = "role exists";
    STR_USER_ERROR[-OB_ROLE_EXIST] = "role '%.*s' exists";
    ORACLE_ERRNO[-OB_ROLE_EXIST] = 1921;
    ORACLE_STR_ERROR[-OB_ROLE_EXIST] = "ORA-01921: role name conflicts with another user or role name";
    ORACLE_STR_USER_ERROR[-OB_ROLE_EXIST] = "ORA-01921: role name '%.*s' conflicts with another user or role name";
    ERROR_NAME[-OB_PRIV_DUP] = "OB_PRIV_DUP";
    MYSQL_ERRNO[-OB_PRIV_DUP] = -1;
    SQLSTATE[-OB_PRIV_DUP] = "HY000";
    STR_ERROR[-OB_PRIV_DUP] = "duplicate privilege listed";
    STR_USER_ERROR[-OB_PRIV_DUP] = "duplicate privilege listed";
    ORACLE_ERRNO[-OB_PRIV_DUP] = 1711;
    ORACLE_STR_ERROR[-OB_PRIV_DUP] = "ORA-01711: duplicate privilege listed";
    ORACLE_STR_USER_ERROR[-OB_PRIV_DUP] = "ORA-01711: duplicate privilege listed";
    ERROR_NAME[-OB_KEYSTORE_EXIST] = "OB_KEYSTORE_EXIST";
    MYSQL_ERRNO[-OB_KEYSTORE_EXIST] = -1;
    SQLSTATE[-OB_KEYSTORE_EXIST] = "HY000";
    STR_ERROR[-OB_KEYSTORE_EXIST] = "the keystore already exists and each tenant can only have at most one";
    STR_USER_ERROR[-OB_KEYSTORE_EXIST] = "the keystore already exists and each tenant can only have at most one";
    ORACLE_ERRNO[-OB_KEYSTORE_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_KEYSTORE_EXIST] = "ORA-00600: internal error code, arguments: -4350, the keystore already "
                                           "exists and each tenant can only have at most one";
    ORACLE_STR_USER_ERROR[-OB_KEYSTORE_EXIST] = "ORA-00600: internal error code, arguments: -4350, the keystore "
                                                "already exists and each tenant can only have at most one";
    ERROR_NAME[-OB_KEYSTORE_NOT_EXIST] = "OB_KEYSTORE_NOT_EXIST";
    MYSQL_ERRNO[-OB_KEYSTORE_NOT_EXIST] = -1;
    SQLSTATE[-OB_KEYSTORE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_KEYSTORE_NOT_EXIST] = "the keystore is not exist";
    STR_USER_ERROR[-OB_KEYSTORE_NOT_EXIST] = "the keystore is not exist";
    ORACLE_ERRNO[-OB_KEYSTORE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_KEYSTORE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4351, the keystore is not exist";
    ORACLE_STR_USER_ERROR[-OB_KEYSTORE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4351, the keystore is not exist";
    ERROR_NAME[-OB_KEYSTORE_WRONG_PASSWORD] = "OB_KEYSTORE_WRONG_PASSWORD";
    MYSQL_ERRNO[-OB_KEYSTORE_WRONG_PASSWORD] = -1;
    SQLSTATE[-OB_KEYSTORE_WRONG_PASSWORD] = "HY000";
    STR_ERROR[-OB_KEYSTORE_WRONG_PASSWORD] = "the password is wrong for keystore";
    STR_USER_ERROR[-OB_KEYSTORE_WRONG_PASSWORD] = "the password is wrong for keystore";
    ORACLE_ERRNO[-OB_KEYSTORE_WRONG_PASSWORD] = 600;
    ORACLE_STR_ERROR[-OB_KEYSTORE_WRONG_PASSWORD] =
        "ORA-00600: internal error code, arguments: -4352, the password is wrong for keystore";
    ORACLE_STR_USER_ERROR[-OB_KEYSTORE_WRONG_PASSWORD] =
        "ORA-00600: internal error code, arguments: -4352, the password is wrong for keystore";
    ERROR_NAME[-OB_TABLESPACE_EXIST] = "OB_TABLESPACE_EXIST";
    MYSQL_ERRNO[-OB_TABLESPACE_EXIST] = -1;
    SQLSTATE[-OB_TABLESPACE_EXIST] = "HY000";
    STR_ERROR[-OB_TABLESPACE_EXIST] = "tablespace already exists";
    STR_USER_ERROR[-OB_TABLESPACE_EXIST] = "tablespace '%.*s' already exists";
    ORACLE_ERRNO[-OB_TABLESPACE_EXIST] = 1543;
    ORACLE_STR_ERROR[-OB_TABLESPACE_EXIST] = "ORA-01543: tablespace already exists";
    ORACLE_STR_USER_ERROR[-OB_TABLESPACE_EXIST] = "ORA-01543: tablespace '%.*s' already exists";
    ERROR_NAME[-OB_TABLESPACE_NOT_EXIST] = "OB_TABLESPACE_NOT_EXIST";
    MYSQL_ERRNO[-OB_TABLESPACE_NOT_EXIST] = -1;
    SQLSTATE[-OB_TABLESPACE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_TABLESPACE_NOT_EXIST] = "tablespace does not exist";
    STR_USER_ERROR[-OB_TABLESPACE_NOT_EXIST] = "tablespace '%.*s' does not exist";
    ORACLE_ERRNO[-OB_TABLESPACE_NOT_EXIST] = 959;
    ORACLE_STR_ERROR[-OB_TABLESPACE_NOT_EXIST] = "ORA-00959: tablespace does not exist";
    ORACLE_STR_USER_ERROR[-OB_TABLESPACE_NOT_EXIST] = "ORA-00959: tablespace '%.*s' does not exist";
    ERROR_NAME[-OB_TABLESPACE_DELETE_NOT_EMPTY] = "OB_TABLESPACE_DELETE_NOT_EMPTY";
    MYSQL_ERRNO[-OB_TABLESPACE_DELETE_NOT_EMPTY] = -1;
    SQLSTATE[-OB_TABLESPACE_DELETE_NOT_EMPTY] = "HY000";
    STR_ERROR[-OB_TABLESPACE_DELETE_NOT_EMPTY] = "cannot delete a tablespace which is not empty";
    STR_USER_ERROR[-OB_TABLESPACE_DELETE_NOT_EMPTY] = "cannot delete a tablespace which is not empty";
    ORACLE_ERRNO[-OB_TABLESPACE_DELETE_NOT_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_TABLESPACE_DELETE_NOT_EMPTY] =
        "ORA-00600: internal error code, arguments: -4355, cannot delete a tablespace which is not empty";
    ORACLE_STR_USER_ERROR[-OB_TABLESPACE_DELETE_NOT_EMPTY] =
        "ORA-00600: internal error code, arguments: -4355, cannot delete a tablespace which is not empty";
    ERROR_NAME[-OB_FLOAT_PRECISION_OUT_RANGE] = "OB_FLOAT_PRECISION_OUT_RANGE";
    MYSQL_ERRNO[-OB_FLOAT_PRECISION_OUT_RANGE] = -1;
    SQLSTATE[-OB_FLOAT_PRECISION_OUT_RANGE] = "HY000";
    STR_ERROR[-OB_FLOAT_PRECISION_OUT_RANGE] = "floating point precision is out of range (1 to 126)";
    STR_USER_ERROR[-OB_FLOAT_PRECISION_OUT_RANGE] = "floating point precision is out of range (1 to 126)";
    ORACLE_ERRNO[-OB_FLOAT_PRECISION_OUT_RANGE] = 1724;
    ORACLE_STR_ERROR[-OB_FLOAT_PRECISION_OUT_RANGE] = "ORA-01724: floating point precision is out of range (1 to 126)";
    ORACLE_STR_USER_ERROR[-OB_FLOAT_PRECISION_OUT_RANGE] =
        "ORA-01724: floating point precision is out of range (1 to 126)";
    ERROR_NAME[-OB_NUMERIC_PRECISION_OUT_RANGE] = "OB_NUMERIC_PRECISION_OUT_RANGE";
    MYSQL_ERRNO[-OB_NUMERIC_PRECISION_OUT_RANGE] = -1;
    SQLSTATE[-OB_NUMERIC_PRECISION_OUT_RANGE] = "HY000";
    STR_ERROR[-OB_NUMERIC_PRECISION_OUT_RANGE] = "numeric precision specifier is out of range (1 to 38)";
    STR_USER_ERROR[-OB_NUMERIC_PRECISION_OUT_RANGE] = "numeric precision specifier is out of range (1 to 38)";
    ORACLE_ERRNO[-OB_NUMERIC_PRECISION_OUT_RANGE] = 1727;
    ORACLE_STR_ERROR[-OB_NUMERIC_PRECISION_OUT_RANGE] =
        "ORA-01727: numeric precision specifier is out of range (1 to 38)";
    ORACLE_STR_USER_ERROR[-OB_NUMERIC_PRECISION_OUT_RANGE] =
        "ORA-01727: numeric precision specifier is out of range (1 to 38)";
    ERROR_NAME[-OB_NUMERIC_SCALE_OUT_RANGE] = "OB_NUMERIC_SCALE_OUT_RANGE";
    MYSQL_ERRNO[-OB_NUMERIC_SCALE_OUT_RANGE] = -1;
    SQLSTATE[-OB_NUMERIC_SCALE_OUT_RANGE] = "HY000";
    STR_ERROR[-OB_NUMERIC_SCALE_OUT_RANGE] = "numeric scale specifier is out of range (-84 to 127)";
    STR_USER_ERROR[-OB_NUMERIC_SCALE_OUT_RANGE] = "numeric scale specifier is out of range (-84 to 127)";
    ORACLE_ERRNO[-OB_NUMERIC_SCALE_OUT_RANGE] = 1728;
    ORACLE_STR_ERROR[-OB_NUMERIC_SCALE_OUT_RANGE] = "ORA-01728: numeric scale specifier is out of range (-84 to 127)";
    ORACLE_STR_USER_ERROR[-OB_NUMERIC_SCALE_OUT_RANGE] =
        "ORA-01728: numeric scale specifier is out of range (-84 to 127)";
    ERROR_NAME[-OB_KEYSTORE_NOT_OPEN] = "OB_KEYSTORE_NOT_OPEN";
    MYSQL_ERRNO[-OB_KEYSTORE_NOT_OPEN] = -1;
    SQLSTATE[-OB_KEYSTORE_NOT_OPEN] = "HY000";
    STR_ERROR[-OB_KEYSTORE_NOT_OPEN] = "the keystore is not open";
    STR_USER_ERROR[-OB_KEYSTORE_NOT_OPEN] = "the keystore is not open";
    ORACLE_ERRNO[-OB_KEYSTORE_NOT_OPEN] = 600;
    ORACLE_STR_ERROR[-OB_KEYSTORE_NOT_OPEN] =
        "ORA-00600: internal error code, arguments: -4359, the keystore is not open";
    ORACLE_STR_USER_ERROR[-OB_KEYSTORE_NOT_OPEN] =
        "ORA-00600: internal error code, arguments: -4359, the keystore is not open";
    ERROR_NAME[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] = "OB_KEYSTORE_OPEN_NO_MASTER_KEY";
    MYSQL_ERRNO[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] = -1;
    SQLSTATE[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] = "HY000";
    STR_ERROR[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] = "the keystore opened with dont have a master key";
    STR_USER_ERROR[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] = "the keystore opened with dont have a master key";
    ORACLE_ERRNO[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] = 600;
    ORACLE_STR_ERROR[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] =
        "ORA-00600: internal error code, arguments: -4360, the keystore opened with dont have a master key";
    ORACLE_STR_USER_ERROR[-OB_KEYSTORE_OPEN_NO_MASTER_KEY] =
        "ORA-00600: internal error code, arguments: -4360, the keystore opened with dont have a master key";
    ERROR_NAME[-OB_SLOG_REACH_MAX_CONCURRENCY] = "OB_SLOG_REACH_MAX_CONCURRENCY";
    MYSQL_ERRNO[-OB_SLOG_REACH_MAX_CONCURRENCY] = -1;
    SQLSTATE[-OB_SLOG_REACH_MAX_CONCURRENCY] = "HY000";
    STR_ERROR[-OB_SLOG_REACH_MAX_CONCURRENCY] = "slog active transaction entries reach maximum";
    STR_USER_ERROR[-OB_SLOG_REACH_MAX_CONCURRENCY] = "slog active transaction entries reach maximum";
    ORACLE_ERRNO[-OB_SLOG_REACH_MAX_CONCURRENCY] = 600;
    ORACLE_STR_ERROR[-OB_SLOG_REACH_MAX_CONCURRENCY] =
        "ORA-00600: internal error code, arguments: -4361, slog active transaction entries reach maximum";
    ORACLE_STR_USER_ERROR[-OB_SLOG_REACH_MAX_CONCURRENCY] =
        "ORA-00600: internal error code, arguments: -4361, slog active transaction entries reach maximum";
    ERROR_NAME[-OB_ERR_NOT_VALID_PASSWORD] = "OB_ERR_NOT_VALID_PASSWORD";
    MYSQL_ERRNO[-OB_ERR_NOT_VALID_PASSWORD] = ER_NOT_VALID_PASSWORD;
    SQLSTATE[-OB_ERR_NOT_VALID_PASSWORD] = "HY000";
    STR_ERROR[-OB_ERR_NOT_VALID_PASSWORD] = "Your password does not satisfy the current policy requirements";
    STR_USER_ERROR[-OB_ERR_NOT_VALID_PASSWORD] = "Your password does not satisfy the current policy requirements";
    ORACLE_ERRNO[-OB_ERR_NOT_VALID_PASSWORD] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NOT_VALID_PASSWORD] = "ORA-00600: internal error code, arguments: -4365, Your password "
                                                   "does not satisfy the current policy requirements";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_VALID_PASSWORD] = "ORA-00600: internal error code, arguments: -4365, Your "
                                                        "password does not satisfy the current policy requirements";
    ERROR_NAME[-OB_ERR_MUST_CHANGE_PASSWORD] = "OB_ERR_MUST_CHANGE_PASSWORD";
    MYSQL_ERRNO[-OB_ERR_MUST_CHANGE_PASSWORD] = ER_MUST_CHANGE_PASSWORD;
    SQLSTATE[-OB_ERR_MUST_CHANGE_PASSWORD] = "HY000";
    STR_ERROR[-OB_ERR_MUST_CHANGE_PASSWORD] =
        "You must reset your password using ALTER USER statement before executing this statement";
    STR_USER_ERROR[-OB_ERR_MUST_CHANGE_PASSWORD] =
        "You must reset your password using ALTER USER statement before executing this statement";
    ORACLE_ERRNO[-OB_ERR_MUST_CHANGE_PASSWORD] = 28001;
    ORACLE_STR_ERROR[-OB_ERR_MUST_CHANGE_PASSWORD] = "ORA-28001: the password has expired";
    ORACLE_STR_USER_ERROR[-OB_ERR_MUST_CHANGE_PASSWORD] = "ORA-28001: the password has expired";
    ERROR_NAME[-OB_OVERSIZE_NEED_RETRY] = "OB_OVERSIZE_NEED_RETRY";
    MYSQL_ERRNO[-OB_OVERSIZE_NEED_RETRY] = ER_OVERSIZE_NEED_RETRY;
    SQLSTATE[-OB_OVERSIZE_NEED_RETRY] = "HY000";
    STR_ERROR[-OB_OVERSIZE_NEED_RETRY] = "The data more than 64M(rpc limit), split into smaller task and retry";
    STR_USER_ERROR[-OB_OVERSIZE_NEED_RETRY] = "The data more than 64M(rpc limit), split into smaller task and retry";
    ORACLE_ERRNO[-OB_OVERSIZE_NEED_RETRY] = 600;
    ORACLE_STR_ERROR[-OB_OVERSIZE_NEED_RETRY] = "ORA-00600: internal error code, arguments: -4367, The data more than "
                                                "64M(rpc limit), split into smaller task and retry";
    ORACLE_STR_USER_ERROR[-OB_OVERSIZE_NEED_RETRY] = "ORA-00600: internal error code, arguments: -4367, The data more "
                                                     "than 64M(rpc limit), split into smaller task and retry";
    ERROR_NAME[-OB_OBCONFIG_CLUSTER_NOT_EXIST] = "OB_OBCONFIG_CLUSTER_NOT_EXIST";
    MYSQL_ERRNO[-OB_OBCONFIG_CLUSTER_NOT_EXIST] = -1;
    SQLSTATE[-OB_OBCONFIG_CLUSTER_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_OBCONFIG_CLUSTER_NOT_EXIST] = "cluster not exists";
    STR_USER_ERROR[-OB_OBCONFIG_CLUSTER_NOT_EXIST] = "cluster not exists";
    ORACLE_ERRNO[-OB_OBCONFIG_CLUSTER_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_OBCONFIG_CLUSTER_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4368, cluster not exists";
    ORACLE_STR_USER_ERROR[-OB_OBCONFIG_CLUSTER_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4368, cluster not exists";
    ERROR_NAME[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] = "OB_ERR_VALUE_LARGER_THAN_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] = "value larger than specified precision allowed for this column";
    STR_USER_ERROR[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] = "value larger than specified precision allowed for this column";
    ORACLE_ERRNO[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] = 1438;
    ORACLE_STR_ERROR[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] =
        "ORA-01438: value larger than specified precision allowed for this column";
    ORACLE_STR_USER_ERROR[-OB_ERR_VALUE_LARGER_THAN_ALLOWED] =
        "ORA-01438: value larger than specified precision allowed for this column";
    ERROR_NAME[-OB_DISK_ERROR] = "OB_DISK_ERROR";
    MYSQL_ERRNO[-OB_DISK_ERROR] = -1;
    SQLSTATE[-OB_DISK_ERROR] = "HY000";
    STR_ERROR[-OB_DISK_ERROR] = "observer has disk error";
    STR_USER_ERROR[-OB_DISK_ERROR] = "observer has disk error";
    ORACLE_ERRNO[-OB_DISK_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_DISK_ERROR] = "ORA-00600: internal error code, arguments: -4375, observer has disk error";
    ORACLE_STR_USER_ERROR[-OB_DISK_ERROR] = "ORA-00600: internal error code, arguments: -4375, observer has disk error";
    ERROR_NAME[-OB_UNIMPLEMENTED_FEATURE] = "OB_UNIMPLEMENTED_FEATURE";
    MYSQL_ERRNO[-OB_UNIMPLEMENTED_FEATURE] = -1;
    SQLSTATE[-OB_UNIMPLEMENTED_FEATURE] = "HY000";
    STR_ERROR[-OB_UNIMPLEMENTED_FEATURE] = "unimplemented feature";
    STR_USER_ERROR[-OB_UNIMPLEMENTED_FEATURE] = "unimplemented feature";
    ORACLE_ERRNO[-OB_UNIMPLEMENTED_FEATURE] = 3001;
    ORACLE_STR_ERROR[-OB_UNIMPLEMENTED_FEATURE] = "ORA-03001: unimplemented feature";
    ORACLE_STR_USER_ERROR[-OB_UNIMPLEMENTED_FEATURE] = "ORA-03001: unimplemented feature";
    ERROR_NAME[-OB_IMPORT_NOT_IN_SERVER] = "OB_IMPORT_NOT_IN_SERVER";
    MYSQL_ERRNO[-OB_IMPORT_NOT_IN_SERVER] = -1;
    SQLSTATE[-OB_IMPORT_NOT_IN_SERVER] = "HY000";
    STR_ERROR[-OB_IMPORT_NOT_IN_SERVER] = "Import not in service";
    STR_USER_ERROR[-OB_IMPORT_NOT_IN_SERVER] = "Import not in service";
    ORACLE_ERRNO[-OB_IMPORT_NOT_IN_SERVER] = 600;
    ORACLE_STR_ERROR[-OB_IMPORT_NOT_IN_SERVER] =
        "ORA-00600: internal error code, arguments: -4505, Import not in service";
    ORACLE_STR_USER_ERROR[-OB_IMPORT_NOT_IN_SERVER] =
        "ORA-00600: internal error code, arguments: -4505, Import not in service";
    ERROR_NAME[-OB_CONVERT_ERROR] = "OB_CONVERT_ERROR";
    MYSQL_ERRNO[-OB_CONVERT_ERROR] = -1;
    SQLSTATE[-OB_CONVERT_ERROR] = "42000";
    STR_ERROR[-OB_CONVERT_ERROR] = "Convert error";
    STR_USER_ERROR[-OB_CONVERT_ERROR] = "Convert error";
    ORACLE_ERRNO[-OB_CONVERT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_CONVERT_ERROR] = "ORA-00600: internal error code, arguments: -4507, Convert error";
    ORACLE_STR_USER_ERROR[-OB_CONVERT_ERROR] = "ORA-00600: internal error code, arguments: -4507, Convert error";
    ERROR_NAME[-OB_BYPASS_TIMEOUT] = "OB_BYPASS_TIMEOUT";
    MYSQL_ERRNO[-OB_BYPASS_TIMEOUT] = -1;
    SQLSTATE[-OB_BYPASS_TIMEOUT] = "HY000";
    STR_ERROR[-OB_BYPASS_TIMEOUT] = "Bypass timeout";
    STR_USER_ERROR[-OB_BYPASS_TIMEOUT] = "Bypass timeout";
    ORACLE_ERRNO[-OB_BYPASS_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_BYPASS_TIMEOUT] = "ORA-00600: internal error code, arguments: -4510, Bypass timeout";
    ORACLE_STR_USER_ERROR[-OB_BYPASS_TIMEOUT] = "ORA-00600: internal error code, arguments: -4510, Bypass timeout";
    ERROR_NAME[-OB_RS_STATE_NOT_ALLOW] = "OB_RS_STATE_NOT_ALLOW";
    MYSQL_ERRNO[-OB_RS_STATE_NOT_ALLOW] = -1;
    SQLSTATE[-OB_RS_STATE_NOT_ALLOW] = "HY000";
    STR_ERROR[-OB_RS_STATE_NOT_ALLOW] = "RootServer state error";
    STR_USER_ERROR[-OB_RS_STATE_NOT_ALLOW] = "RootServer state error";
    ORACLE_ERRNO[-OB_RS_STATE_NOT_ALLOW] = 600;
    ORACLE_STR_ERROR[-OB_RS_STATE_NOT_ALLOW] =
        "ORA-00600: internal error code, arguments: -4512, RootServer state error";
    ORACLE_STR_USER_ERROR[-OB_RS_STATE_NOT_ALLOW] =
        "ORA-00600: internal error code, arguments: -4512, RootServer state error";
    ERROR_NAME[-OB_NO_REPLICA_VALID] = "OB_NO_REPLICA_VALID";
    MYSQL_ERRNO[-OB_NO_REPLICA_VALID] = -1;
    SQLSTATE[-OB_NO_REPLICA_VALID] = "HY000";
    STR_ERROR[-OB_NO_REPLICA_VALID] = "No replica is valid";
    STR_USER_ERROR[-OB_NO_REPLICA_VALID] = "No replica is valid";
    ORACLE_ERRNO[-OB_NO_REPLICA_VALID] = 600;
    ORACLE_STR_ERROR[-OB_NO_REPLICA_VALID] = "ORA-00600: internal error code, arguments: -4515, No replica is valid";
    ORACLE_STR_USER_ERROR[-OB_NO_REPLICA_VALID] =
        "ORA-00600: internal error code, arguments: -4515, No replica is valid";
    ERROR_NAME[-OB_NO_NEED_UPDATE] = "OB_NO_NEED_UPDATE";
    MYSQL_ERRNO[-OB_NO_NEED_UPDATE] = -1;
    SQLSTATE[-OB_NO_NEED_UPDATE] = "HY000";
    STR_ERROR[-OB_NO_NEED_UPDATE] = "No need to update";
    STR_USER_ERROR[-OB_NO_NEED_UPDATE] = "No need to update";
    ORACLE_ERRNO[-OB_NO_NEED_UPDATE] = 600;
    ORACLE_STR_ERROR[-OB_NO_NEED_UPDATE] = "ORA-00600: internal error code, arguments: -4517, No need to update";
    ORACLE_STR_USER_ERROR[-OB_NO_NEED_UPDATE] = "ORA-00600: internal error code, arguments: -4517, No need to update";
    ERROR_NAME[-OB_CACHE_TIMEOUT] = "OB_CACHE_TIMEOUT";
    MYSQL_ERRNO[-OB_CACHE_TIMEOUT] = -1;
    SQLSTATE[-OB_CACHE_TIMEOUT] = "HY000";
    STR_ERROR[-OB_CACHE_TIMEOUT] = "Cache timeout";
    STR_USER_ERROR[-OB_CACHE_TIMEOUT] = "Cache timeout";
    ORACLE_ERRNO[-OB_CACHE_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_CACHE_TIMEOUT] = "ORA-00600: internal error code, arguments: -4518, Cache timeout";
    ORACLE_STR_USER_ERROR[-OB_CACHE_TIMEOUT] = "ORA-00600: internal error code, arguments: -4518, Cache timeout";
    ERROR_NAME[-OB_ITER_STOP] = "OB_ITER_STOP";
    MYSQL_ERRNO[-OB_ITER_STOP] = -1;
    SQLSTATE[-OB_ITER_STOP] = "HY000";
    STR_ERROR[-OB_ITER_STOP] = "Iteration was stopped";
    STR_USER_ERROR[-OB_ITER_STOP] = "Iteration was stopped";
    ORACLE_ERRNO[-OB_ITER_STOP] = 600;
    ORACLE_STR_ERROR[-OB_ITER_STOP] = "ORA-00600: internal error code, arguments: -4519, Iteration was stopped";
    ORACLE_STR_USER_ERROR[-OB_ITER_STOP] = "ORA-00600: internal error code, arguments: -4519, Iteration was stopped";
    ERROR_NAME[-OB_ZONE_ALREADY_MASTER] = "OB_ZONE_ALREADY_MASTER";
    MYSQL_ERRNO[-OB_ZONE_ALREADY_MASTER] = -1;
    SQLSTATE[-OB_ZONE_ALREADY_MASTER] = "HY000";
    STR_ERROR[-OB_ZONE_ALREADY_MASTER] = "The zone is the master already";
    STR_USER_ERROR[-OB_ZONE_ALREADY_MASTER] = "The zone is the master already";
    ORACLE_ERRNO[-OB_ZONE_ALREADY_MASTER] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_ALREADY_MASTER] =
        "ORA-00600: internal error code, arguments: -4523, The zone is the master already";
    ORACLE_STR_USER_ERROR[-OB_ZONE_ALREADY_MASTER] =
        "ORA-00600: internal error code, arguments: -4523, The zone is the master already";
    ERROR_NAME[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] = "OB_IP_PORT_IS_NOT_SLAVE_ZONE";
    MYSQL_ERRNO[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] = -1;
    SQLSTATE[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] = "HY000";
    STR_ERROR[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] = "Not slave zone";
    STR_USER_ERROR[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] = "Not slave zone";
    ORACLE_ERRNO[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] = 600;
    ORACLE_STR_ERROR[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] =
        "ORA-00600: internal error code, arguments: -4524, Not slave zone";
    ORACLE_STR_USER_ERROR[-OB_IP_PORT_IS_NOT_SLAVE_ZONE] =
        "ORA-00600: internal error code, arguments: -4524, Not slave zone";
    ERROR_NAME[-OB_ZONE_IS_NOT_SLAVE] = "OB_ZONE_IS_NOT_SLAVE";
    MYSQL_ERRNO[-OB_ZONE_IS_NOT_SLAVE] = -1;
    SQLSTATE[-OB_ZONE_IS_NOT_SLAVE] = "HY000";
    STR_ERROR[-OB_ZONE_IS_NOT_SLAVE] = "Not slave zone";
    STR_USER_ERROR[-OB_ZONE_IS_NOT_SLAVE] = "Not slave zone";
    ORACLE_ERRNO[-OB_ZONE_IS_NOT_SLAVE] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_IS_NOT_SLAVE] = "ORA-00600: internal error code, arguments: -4525, Not slave zone";
    ORACLE_STR_USER_ERROR[-OB_ZONE_IS_NOT_SLAVE] = "ORA-00600: internal error code, arguments: -4525, Not slave zone";
    ERROR_NAME[-OB_ZONE_IS_NOT_MASTER] = "OB_ZONE_IS_NOT_MASTER";
    MYSQL_ERRNO[-OB_ZONE_IS_NOT_MASTER] = -1;
    SQLSTATE[-OB_ZONE_IS_NOT_MASTER] = "HY000";
    STR_ERROR[-OB_ZONE_IS_NOT_MASTER] = "Not master zone";
    STR_USER_ERROR[-OB_ZONE_IS_NOT_MASTER] = "Not master zone";
    ORACLE_ERRNO[-OB_ZONE_IS_NOT_MASTER] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_IS_NOT_MASTER] = "ORA-00600: internal error code, arguments: -4526, Not master zone";
    ORACLE_STR_USER_ERROR[-OB_ZONE_IS_NOT_MASTER] = "ORA-00600: internal error code, arguments: -4526, Not master zone";
    ERROR_NAME[-OB_CONFIG_NOT_SYNC] = "OB_CONFIG_NOT_SYNC";
    MYSQL_ERRNO[-OB_CONFIG_NOT_SYNC] = -1;
    SQLSTATE[-OB_CONFIG_NOT_SYNC] = "F0000";
    STR_ERROR[-OB_CONFIG_NOT_SYNC] = "Configuration not sync";
    STR_USER_ERROR[-OB_CONFIG_NOT_SYNC] = "Configuration not sync";
    ORACLE_ERRNO[-OB_CONFIG_NOT_SYNC] = 600;
    ORACLE_STR_ERROR[-OB_CONFIG_NOT_SYNC] = "ORA-00600: internal error code, arguments: -4527, Configuration not sync";
    ORACLE_STR_USER_ERROR[-OB_CONFIG_NOT_SYNC] =
        "ORA-00600: internal error code, arguments: -4527, Configuration not sync";
    ERROR_NAME[-OB_IP_PORT_IS_NOT_ZONE] = "OB_IP_PORT_IS_NOT_ZONE";
    MYSQL_ERRNO[-OB_IP_PORT_IS_NOT_ZONE] = -1;
    SQLSTATE[-OB_IP_PORT_IS_NOT_ZONE] = "42000";
    STR_ERROR[-OB_IP_PORT_IS_NOT_ZONE] = "Not a zone address";
    STR_USER_ERROR[-OB_IP_PORT_IS_NOT_ZONE] = "Not a zone address";
    ORACLE_ERRNO[-OB_IP_PORT_IS_NOT_ZONE] = 600;
    ORACLE_STR_ERROR[-OB_IP_PORT_IS_NOT_ZONE] = "ORA-00600: internal error code, arguments: -4528, Not a zone address";
    ORACLE_STR_USER_ERROR[-OB_IP_PORT_IS_NOT_ZONE] =
        "ORA-00600: internal error code, arguments: -4528, Not a zone address";
    ERROR_NAME[-OB_MASTER_ZONE_NOT_EXIST] = "OB_MASTER_ZONE_NOT_EXIST";
    MYSQL_ERRNO[-OB_MASTER_ZONE_NOT_EXIST] = -1;
    SQLSTATE[-OB_MASTER_ZONE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_MASTER_ZONE_NOT_EXIST] = "Master zone not exist";
    STR_USER_ERROR[-OB_MASTER_ZONE_NOT_EXIST] = "Master zone not exist";
    ORACLE_ERRNO[-OB_MASTER_ZONE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_MASTER_ZONE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4529, Master zone not exist";
    ORACLE_STR_USER_ERROR[-OB_MASTER_ZONE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4529, Master zone not exist";
    ERROR_NAME[-OB_ZONE_INFO_NOT_EXIST] = "OB_ZONE_INFO_NOT_EXIST";
    MYSQL_ERRNO[-OB_ZONE_INFO_NOT_EXIST] = -1;
    SQLSTATE[-OB_ZONE_INFO_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ZONE_INFO_NOT_EXIST] = "Zone info not exist";
    STR_USER_ERROR[-OB_ZONE_INFO_NOT_EXIST] = "Zone info \'%s\' not exist";
    ORACLE_ERRNO[-OB_ZONE_INFO_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_INFO_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4530, Zone info not exist";
    ORACLE_STR_USER_ERROR[-OB_ZONE_INFO_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4530, Zone info \'%s\' not exist";
    ERROR_NAME[-OB_GET_ZONE_MASTER_UPS_FAILED] = "OB_GET_ZONE_MASTER_UPS_FAILED";
    MYSQL_ERRNO[-OB_GET_ZONE_MASTER_UPS_FAILED] = -1;
    SQLSTATE[-OB_GET_ZONE_MASTER_UPS_FAILED] = "HY000";
    STR_ERROR[-OB_GET_ZONE_MASTER_UPS_FAILED] = "Failed to get master UpdateServer";
    STR_USER_ERROR[-OB_GET_ZONE_MASTER_UPS_FAILED] = "Failed to get master UpdateServer";
    ORACLE_ERRNO[-OB_GET_ZONE_MASTER_UPS_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_GET_ZONE_MASTER_UPS_FAILED] =
        "ORA-00600: internal error code, arguments: -4531, Failed to get master UpdateServer";
    ORACLE_STR_USER_ERROR[-OB_GET_ZONE_MASTER_UPS_FAILED] =
        "ORA-00600: internal error code, arguments: -4531, Failed to get master UpdateServer";
    ERROR_NAME[-OB_MULTIPLE_MASTER_ZONES_EXIST] = "OB_MULTIPLE_MASTER_ZONES_EXIST";
    MYSQL_ERRNO[-OB_MULTIPLE_MASTER_ZONES_EXIST] = -1;
    SQLSTATE[-OB_MULTIPLE_MASTER_ZONES_EXIST] = "HY000";
    STR_ERROR[-OB_MULTIPLE_MASTER_ZONES_EXIST] = "Multiple master zones";
    STR_USER_ERROR[-OB_MULTIPLE_MASTER_ZONES_EXIST] = "Multiple master zones";
    ORACLE_ERRNO[-OB_MULTIPLE_MASTER_ZONES_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_MULTIPLE_MASTER_ZONES_EXIST] =
        "ORA-00600: internal error code, arguments: -4532, Multiple master zones";
    ORACLE_STR_USER_ERROR[-OB_MULTIPLE_MASTER_ZONES_EXIST] =
        "ORA-00600: internal error code, arguments: -4532, Multiple master zones";
    ERROR_NAME[-OB_INDEXING_ZONE_INVALID] = "OB_INDEXING_ZONE_INVALID";
    MYSQL_ERRNO[-OB_INDEXING_ZONE_INVALID] = -1;
    SQLSTATE[-OB_INDEXING_ZONE_INVALID] = "HY000";
    STR_ERROR[-OB_INDEXING_ZONE_INVALID] = "indexing zone is not exist anymore or not active";
    STR_USER_ERROR[-OB_INDEXING_ZONE_INVALID] = "indexing zone is not exist anymore or not active";
    ORACLE_ERRNO[-OB_INDEXING_ZONE_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_INDEXING_ZONE_INVALID] =
        "ORA-00600: internal error code, arguments: -4533, indexing zone is not exist anymore or not active";
    ORACLE_STR_USER_ERROR[-OB_INDEXING_ZONE_INVALID] =
        "ORA-00600: internal error code, arguments: -4533, indexing zone is not exist anymore or not active";
    ERROR_NAME[-OB_ROOT_TABLE_RANGE_NOT_EXIST] = "OB_ROOT_TABLE_RANGE_NOT_EXIST";
    MYSQL_ERRNO[-OB_ROOT_TABLE_RANGE_NOT_EXIST] = -1;
    SQLSTATE[-OB_ROOT_TABLE_RANGE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ROOT_TABLE_RANGE_NOT_EXIST] = "Tablet range not exist";
    STR_USER_ERROR[-OB_ROOT_TABLE_RANGE_NOT_EXIST] = "Tablet range not exist";
    ORACLE_ERRNO[-OB_ROOT_TABLE_RANGE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ROOT_TABLE_RANGE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4537, Tablet range not exist";
    ORACLE_STR_USER_ERROR[-OB_ROOT_TABLE_RANGE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4537, Tablet range not exist";
    ERROR_NAME[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] = "OB_ROOT_MIGRATE_CONCURRENCY_FULL";
    MYSQL_ERRNO[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] = -1;
    SQLSTATE[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] = "HY000";
    STR_ERROR[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] = "Migrate concurrency full";
    STR_USER_ERROR[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] = "Migrate concurrency full";
    ORACLE_ERRNO[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] = 600;
    ORACLE_STR_ERROR[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] =
        "ORA-00600: internal error code, arguments: -4538, Migrate concurrency full";
    ORACLE_STR_USER_ERROR[-OB_ROOT_MIGRATE_CONCURRENCY_FULL] =
        "ORA-00600: internal error code, arguments: -4538, Migrate concurrency full";
    ERROR_NAME[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] = "OB_ROOT_MIGRATE_INFO_NOT_FOUND";
    MYSQL_ERRNO[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] = -1;
    SQLSTATE[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] = "Migrate info not found";
    STR_USER_ERROR[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] = "Migrate info not found";
    ORACLE_ERRNO[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -4539, Migrate info not found";
    ORACLE_STR_USER_ERROR[-OB_ROOT_MIGRATE_INFO_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -4539, Migrate info not found";
    ERROR_NAME[-OB_NOT_DATA_LOAD_TABLE] = "OB_NOT_DATA_LOAD_TABLE";
    MYSQL_ERRNO[-OB_NOT_DATA_LOAD_TABLE] = -1;
    SQLSTATE[-OB_NOT_DATA_LOAD_TABLE] = "HY000";
    STR_ERROR[-OB_NOT_DATA_LOAD_TABLE] = "No data to load";
    STR_USER_ERROR[-OB_NOT_DATA_LOAD_TABLE] = "No data to load";
    ORACLE_ERRNO[-OB_NOT_DATA_LOAD_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_NOT_DATA_LOAD_TABLE] = "ORA-00600: internal error code, arguments: -4540, No data to load";
    ORACLE_STR_USER_ERROR[-OB_NOT_DATA_LOAD_TABLE] =
        "ORA-00600: internal error code, arguments: -4540, No data to load";
    ERROR_NAME[-OB_DATA_LOAD_TABLE_DUPLICATED] = "OB_DATA_LOAD_TABLE_DUPLICATED";
    MYSQL_ERRNO[-OB_DATA_LOAD_TABLE_DUPLICATED] = -1;
    SQLSTATE[-OB_DATA_LOAD_TABLE_DUPLICATED] = "HY000";
    STR_ERROR[-OB_DATA_LOAD_TABLE_DUPLICATED] = "Duplicated table data to load";
    STR_USER_ERROR[-OB_DATA_LOAD_TABLE_DUPLICATED] = "Duplicated table data to load";
    ORACLE_ERRNO[-OB_DATA_LOAD_TABLE_DUPLICATED] = 600;
    ORACLE_STR_ERROR[-OB_DATA_LOAD_TABLE_DUPLICATED] =
        "ORA-00600: internal error code, arguments: -4541, Duplicated table data to load";
    ORACLE_STR_USER_ERROR[-OB_DATA_LOAD_TABLE_DUPLICATED] =
        "ORA-00600: internal error code, arguments: -4541, Duplicated table data to load";
    ERROR_NAME[-OB_ROOT_TABLE_ID_EXIST] = "OB_ROOT_TABLE_ID_EXIST";
    MYSQL_ERRNO[-OB_ROOT_TABLE_ID_EXIST] = -1;
    SQLSTATE[-OB_ROOT_TABLE_ID_EXIST] = "HY000";
    STR_ERROR[-OB_ROOT_TABLE_ID_EXIST] = "Table ID exist";
    STR_USER_ERROR[-OB_ROOT_TABLE_ID_EXIST] = "Table ID exist";
    ORACLE_ERRNO[-OB_ROOT_TABLE_ID_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ROOT_TABLE_ID_EXIST] = "ORA-00600: internal error code, arguments: -4542, Table ID exist";
    ORACLE_STR_USER_ERROR[-OB_ROOT_TABLE_ID_EXIST] = "ORA-00600: internal error code, arguments: -4542, Table ID exist";
    ERROR_NAME[-OB_INDEX_TIMEOUT] = "OB_INDEX_TIMEOUT";
    MYSQL_ERRNO[-OB_INDEX_TIMEOUT] = -1;
    SQLSTATE[-OB_INDEX_TIMEOUT] = "HY000";
    STR_ERROR[-OB_INDEX_TIMEOUT] = "Building index timeout";
    STR_USER_ERROR[-OB_INDEX_TIMEOUT] = "Building index timeout";
    ORACLE_ERRNO[-OB_INDEX_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_INDEX_TIMEOUT] = "ORA-00600: internal error code, arguments: -4543, Building index timeout";
    ORACLE_STR_USER_ERROR[-OB_INDEX_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4543, Building index timeout";
    ERROR_NAME[-OB_ROOT_NOT_INTEGRATED] = "OB_ROOT_NOT_INTEGRATED";
    MYSQL_ERRNO[-OB_ROOT_NOT_INTEGRATED] = -1;
    SQLSTATE[-OB_ROOT_NOT_INTEGRATED] = "42000";
    STR_ERROR[-OB_ROOT_NOT_INTEGRATED] = "Root not integrated";
    STR_USER_ERROR[-OB_ROOT_NOT_INTEGRATED] = "Root not integrated";
    ORACLE_ERRNO[-OB_ROOT_NOT_INTEGRATED] = 600;
    ORACLE_STR_ERROR[-OB_ROOT_NOT_INTEGRATED] = "ORA-00600: internal error code, arguments: -4544, Root not integrated";
    ORACLE_STR_USER_ERROR[-OB_ROOT_NOT_INTEGRATED] =
        "ORA-00600: internal error code, arguments: -4544, Root not integrated";
    ERROR_NAME[-OB_INDEX_INELIGIBLE] = "OB_INDEX_INELIGIBLE";
    MYSQL_ERRNO[-OB_INDEX_INELIGIBLE] = -1;
    SQLSTATE[-OB_INDEX_INELIGIBLE] = "HY000";
    STR_ERROR[-OB_INDEX_INELIGIBLE] = "index data not unique";
    STR_USER_ERROR[-OB_INDEX_INELIGIBLE] = "index data not unique";
    ORACLE_ERRNO[-OB_INDEX_INELIGIBLE] = 600;
    ORACLE_STR_ERROR[-OB_INDEX_INELIGIBLE] = "ORA-00600: internal error code, arguments: -4545, index data not unique";
    ORACLE_STR_USER_ERROR[-OB_INDEX_INELIGIBLE] =
        "ORA-00600: internal error code, arguments: -4545, index data not unique";
    ERROR_NAME[-OB_REBALANCE_EXEC_TIMEOUT] = "OB_REBALANCE_EXEC_TIMEOUT";
    MYSQL_ERRNO[-OB_REBALANCE_EXEC_TIMEOUT] = -1;
    SQLSTATE[-OB_REBALANCE_EXEC_TIMEOUT] = "HY000";
    STR_ERROR[-OB_REBALANCE_EXEC_TIMEOUT] = "execute replication or migration task timeout";
    STR_USER_ERROR[-OB_REBALANCE_EXEC_TIMEOUT] = "execute replication or migration task timeout";
    ORACLE_ERRNO[-OB_REBALANCE_EXEC_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_REBALANCE_EXEC_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4546, execute replication or migration task timeout";
    ORACLE_STR_USER_ERROR[-OB_REBALANCE_EXEC_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4546, execute replication or migration task timeout";
    ERROR_NAME[-OB_MERGE_NOT_STARTED] = "OB_MERGE_NOT_STARTED";
    MYSQL_ERRNO[-OB_MERGE_NOT_STARTED] = -1;
    SQLSTATE[-OB_MERGE_NOT_STARTED] = "HY000";
    STR_ERROR[-OB_MERGE_NOT_STARTED] = "global merge not started";
    STR_USER_ERROR[-OB_MERGE_NOT_STARTED] = "global merge not started";
    ORACLE_ERRNO[-OB_MERGE_NOT_STARTED] = 600;
    ORACLE_STR_ERROR[-OB_MERGE_NOT_STARTED] =
        "ORA-00600: internal error code, arguments: -4547, global merge not started";
    ORACLE_STR_USER_ERROR[-OB_MERGE_NOT_STARTED] =
        "ORA-00600: internal error code, arguments: -4547, global merge not started";
    ERROR_NAME[-OB_MERGE_ALREADY_STARTED] = "OB_MERGE_ALREADY_STARTED";
    MYSQL_ERRNO[-OB_MERGE_ALREADY_STARTED] = -1;
    SQLSTATE[-OB_MERGE_ALREADY_STARTED] = "HY000";
    STR_ERROR[-OB_MERGE_ALREADY_STARTED] = "merge already started";
    STR_USER_ERROR[-OB_MERGE_ALREADY_STARTED] = "merge already started";
    ORACLE_ERRNO[-OB_MERGE_ALREADY_STARTED] = 600;
    ORACLE_STR_ERROR[-OB_MERGE_ALREADY_STARTED] =
        "ORA-00600: internal error code, arguments: -4548, merge already started";
    ORACLE_STR_USER_ERROR[-OB_MERGE_ALREADY_STARTED] =
        "ORA-00600: internal error code, arguments: -4548, merge already started";
    ERROR_NAME[-OB_ROOTSERVICE_EXIST] = "OB_ROOTSERVICE_EXIST";
    MYSQL_ERRNO[-OB_ROOTSERVICE_EXIST] = -1;
    SQLSTATE[-OB_ROOTSERVICE_EXIST] = "HY000";
    STR_ERROR[-OB_ROOTSERVICE_EXIST] = "rootservice already exist";
    STR_USER_ERROR[-OB_ROOTSERVICE_EXIST] = "rootservice already exist";
    ORACLE_ERRNO[-OB_ROOTSERVICE_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ROOTSERVICE_EXIST] =
        "ORA-00600: internal error code, arguments: -4549, rootservice already exist";
    ORACLE_STR_USER_ERROR[-OB_ROOTSERVICE_EXIST] =
        "ORA-00600: internal error code, arguments: -4549, rootservice already exist";
    ERROR_NAME[-OB_RS_SHUTDOWN] = "OB_RS_SHUTDOWN";
    MYSQL_ERRNO[-OB_RS_SHUTDOWN] = -1;
    SQLSTATE[-OB_RS_SHUTDOWN] = "HY000";
    STR_ERROR[-OB_RS_SHUTDOWN] = "rootservice is shutdown";
    STR_USER_ERROR[-OB_RS_SHUTDOWN] = "rootservice is shutdown";
    ORACLE_ERRNO[-OB_RS_SHUTDOWN] = 600;
    ORACLE_STR_ERROR[-OB_RS_SHUTDOWN] = "ORA-00600: internal error code, arguments: -4550, rootservice is shutdown";
    ORACLE_STR_USER_ERROR[-OB_RS_SHUTDOWN] =
        "ORA-00600: internal error code, arguments: -4550, rootservice is shutdown";
    ERROR_NAME[-OB_SERVER_MIGRATE_IN_DENIED] = "OB_SERVER_MIGRATE_IN_DENIED";
    MYSQL_ERRNO[-OB_SERVER_MIGRATE_IN_DENIED] = -1;
    SQLSTATE[-OB_SERVER_MIGRATE_IN_DENIED] = "HY000";
    STR_ERROR[-OB_SERVER_MIGRATE_IN_DENIED] = "server migrate in denied";
    STR_USER_ERROR[-OB_SERVER_MIGRATE_IN_DENIED] = "server migrate in denied";
    ORACLE_ERRNO[-OB_SERVER_MIGRATE_IN_DENIED] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_MIGRATE_IN_DENIED] =
        "ORA-00600: internal error code, arguments: -4551, server migrate in denied";
    ORACLE_STR_USER_ERROR[-OB_SERVER_MIGRATE_IN_DENIED] =
        "ORA-00600: internal error code, arguments: -4551, server migrate in denied";
    ERROR_NAME[-OB_REBALANCE_TASK_CANT_EXEC] = "OB_REBALANCE_TASK_CANT_EXEC";
    MYSQL_ERRNO[-OB_REBALANCE_TASK_CANT_EXEC] = -1;
    SQLSTATE[-OB_REBALANCE_TASK_CANT_EXEC] = "HY000";
    STR_ERROR[-OB_REBALANCE_TASK_CANT_EXEC] = "rebalance task can not executing now";
    STR_USER_ERROR[-OB_REBALANCE_TASK_CANT_EXEC] = "rebalance task can not executing now";
    ORACLE_ERRNO[-OB_REBALANCE_TASK_CANT_EXEC] = 600;
    ORACLE_STR_ERROR[-OB_REBALANCE_TASK_CANT_EXEC] =
        "ORA-00600: internal error code, arguments: -4552, rebalance task can not executing now";
    ORACLE_STR_USER_ERROR[-OB_REBALANCE_TASK_CANT_EXEC] =
        "ORA-00600: internal error code, arguments: -4552, rebalance task can not executing now";
    ERROR_NAME[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] = "OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT";
    MYSQL_ERRNO[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] = -1;
    SQLSTATE[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] = "HY000";
    STR_ERROR[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] = "rootserver can not hold more partition";
    STR_USER_ERROR[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] = "rootserver can not hold more partition";
    ORACLE_ERRNO[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] =
        "ORA-00600: internal error code, arguments: -4553, rootserver can not hold more partition";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT] =
        "ORA-00600: internal error code, arguments: -4553, rootserver can not hold more partition";
    ERROR_NAME[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] = "OB_REBALANCE_TASK_NOT_IN_PROGRESS";
    MYSQL_ERRNO[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] = -1;
    SQLSTATE[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] = "HY000";
    STR_ERROR[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] = "rebalance task not in progress on observer";
    STR_USER_ERROR[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] = "rebalance task not in progress on observer";
    ORACLE_ERRNO[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] = 600;
    ORACLE_STR_ERROR[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -4554, rebalance task not in progress on observer";
    ORACLE_STR_USER_ERROR[-OB_REBALANCE_TASK_NOT_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -4554, rebalance task not in progress on observer";
    ERROR_NAME[-OB_DATA_SOURCE_NOT_EXIST] = "OB_DATA_SOURCE_NOT_EXIST";
    MYSQL_ERRNO[-OB_DATA_SOURCE_NOT_EXIST] = -1;
    SQLSTATE[-OB_DATA_SOURCE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_DATA_SOURCE_NOT_EXIST] = "Data source not exist";
    STR_USER_ERROR[-OB_DATA_SOURCE_NOT_EXIST] = "Data source not exist";
    ORACLE_ERRNO[-OB_DATA_SOURCE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4600, Data source not exist";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4600, Data source not exist";
    ERROR_NAME[-OB_DATA_SOURCE_TABLE_NOT_EXIST] = "OB_DATA_SOURCE_TABLE_NOT_EXIST";
    MYSQL_ERRNO[-OB_DATA_SOURCE_TABLE_NOT_EXIST] = -1;
    SQLSTATE[-OB_DATA_SOURCE_TABLE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_DATA_SOURCE_TABLE_NOT_EXIST] = "Data source table not exist";
    STR_USER_ERROR[-OB_DATA_SOURCE_TABLE_NOT_EXIST] = "Data source table not exist";
    ORACLE_ERRNO[-OB_DATA_SOURCE_TABLE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_TABLE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4601, Data source table not exist";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_TABLE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4601, Data source table not exist";
    ERROR_NAME[-OB_DATA_SOURCE_RANGE_NOT_EXIST] = "OB_DATA_SOURCE_RANGE_NOT_EXIST";
    MYSQL_ERRNO[-OB_DATA_SOURCE_RANGE_NOT_EXIST] = -1;
    SQLSTATE[-OB_DATA_SOURCE_RANGE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_DATA_SOURCE_RANGE_NOT_EXIST] = "Data source range not exist";
    STR_USER_ERROR[-OB_DATA_SOURCE_RANGE_NOT_EXIST] = "Data source range not exist";
    ORACLE_ERRNO[-OB_DATA_SOURCE_RANGE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_RANGE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4602, Data source range not exist";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_RANGE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4602, Data source range not exist";
    ERROR_NAME[-OB_DATA_SOURCE_DATA_NOT_EXIST] = "OB_DATA_SOURCE_DATA_NOT_EXIST";
    MYSQL_ERRNO[-OB_DATA_SOURCE_DATA_NOT_EXIST] = -1;
    SQLSTATE[-OB_DATA_SOURCE_DATA_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_DATA_SOURCE_DATA_NOT_EXIST] = "Data source data not exist";
    STR_USER_ERROR[-OB_DATA_SOURCE_DATA_NOT_EXIST] = "Data source data not exist";
    ORACLE_ERRNO[-OB_DATA_SOURCE_DATA_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_DATA_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4603, Data source data not exist";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_DATA_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4603, Data source data not exist";
    ERROR_NAME[-OB_DATA_SOURCE_SYS_ERROR] = "OB_DATA_SOURCE_SYS_ERROR";
    MYSQL_ERRNO[-OB_DATA_SOURCE_SYS_ERROR] = -1;
    SQLSTATE[-OB_DATA_SOURCE_SYS_ERROR] = "HY000";
    STR_ERROR[-OB_DATA_SOURCE_SYS_ERROR] = "Data source sys error";
    STR_USER_ERROR[-OB_DATA_SOURCE_SYS_ERROR] = "Data source sys error";
    ORACLE_ERRNO[-OB_DATA_SOURCE_SYS_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_SYS_ERROR] =
        "ORA-00600: internal error code, arguments: -4604, Data source sys error";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_SYS_ERROR] =
        "ORA-00600: internal error code, arguments: -4604, Data source sys error";
    ERROR_NAME[-OB_DATA_SOURCE_TIMEOUT] = "OB_DATA_SOURCE_TIMEOUT";
    MYSQL_ERRNO[-OB_DATA_SOURCE_TIMEOUT] = -1;
    SQLSTATE[-OB_DATA_SOURCE_TIMEOUT] = "HY000";
    STR_ERROR[-OB_DATA_SOURCE_TIMEOUT] = "Data source timeout";
    STR_USER_ERROR[-OB_DATA_SOURCE_TIMEOUT] = "Data source timeout";
    ORACLE_ERRNO[-OB_DATA_SOURCE_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_TIMEOUT] = "ORA-00600: internal error code, arguments: -4605, Data source timeout";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4605, Data source timeout";
    ERROR_NAME[-OB_DATA_SOURCE_CONCURRENCY_FULL] = "OB_DATA_SOURCE_CONCURRENCY_FULL";
    MYSQL_ERRNO[-OB_DATA_SOURCE_CONCURRENCY_FULL] = -1;
    SQLSTATE[-OB_DATA_SOURCE_CONCURRENCY_FULL] = "53000";
    STR_ERROR[-OB_DATA_SOURCE_CONCURRENCY_FULL] = "Data source concurrency full";
    STR_USER_ERROR[-OB_DATA_SOURCE_CONCURRENCY_FULL] = "Data source concurrency full";
    ORACLE_ERRNO[-OB_DATA_SOURCE_CONCURRENCY_FULL] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_CONCURRENCY_FULL] =
        "ORA-00600: internal error code, arguments: -4606, Data source concurrency full";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_CONCURRENCY_FULL] =
        "ORA-00600: internal error code, arguments: -4606, Data source concurrency full";
    ERROR_NAME[-OB_DATA_SOURCE_WRONG_URI_FORMAT] = "OB_DATA_SOURCE_WRONG_URI_FORMAT";
    MYSQL_ERRNO[-OB_DATA_SOURCE_WRONG_URI_FORMAT] = -1;
    SQLSTATE[-OB_DATA_SOURCE_WRONG_URI_FORMAT] = "42000";
    STR_ERROR[-OB_DATA_SOURCE_WRONG_URI_FORMAT] = "Data source wrong URI format";
    STR_USER_ERROR[-OB_DATA_SOURCE_WRONG_URI_FORMAT] = "Data source wrong URI format";
    ORACLE_ERRNO[-OB_DATA_SOURCE_WRONG_URI_FORMAT] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_WRONG_URI_FORMAT] =
        "ORA-00600: internal error code, arguments: -4607, Data source wrong URI format";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_WRONG_URI_FORMAT] =
        "ORA-00600: internal error code, arguments: -4607, Data source wrong URI format";
    ERROR_NAME[-OB_SSTABLE_VERSION_UNEQUAL] = "OB_SSTABLE_VERSION_UNEQUAL";
    MYSQL_ERRNO[-OB_SSTABLE_VERSION_UNEQUAL] = -1;
    SQLSTATE[-OB_SSTABLE_VERSION_UNEQUAL] = "42000";
    STR_ERROR[-OB_SSTABLE_VERSION_UNEQUAL] = "SSTable version not equal";
    STR_USER_ERROR[-OB_SSTABLE_VERSION_UNEQUAL] = "SSTable version not equal";
    ORACLE_ERRNO[-OB_SSTABLE_VERSION_UNEQUAL] = 600;
    ORACLE_STR_ERROR[-OB_SSTABLE_VERSION_UNEQUAL] =
        "ORA-00600: internal error code, arguments: -4608, SSTable version not equal";
    ORACLE_STR_USER_ERROR[-OB_SSTABLE_VERSION_UNEQUAL] =
        "ORA-00600: internal error code, arguments: -4608, SSTable version not equal";
    ERROR_NAME[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] = "OB_UPS_RENEW_LEASE_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] = "ups should not renew its lease";
    STR_USER_ERROR[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] = "ups should not renew its lease";
    ORACLE_ERRNO[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -4609, ups should not renew its lease";
    ORACLE_STR_USER_ERROR[-OB_UPS_RENEW_LEASE_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -4609, ups should not renew its lease";
    ERROR_NAME[-OB_UPS_COUNT_OVER_LIMIT] = "OB_UPS_COUNT_OVER_LIMIT";
    MYSQL_ERRNO[-OB_UPS_COUNT_OVER_LIMIT] = -1;
    SQLSTATE[-OB_UPS_COUNT_OVER_LIMIT] = "HY000";
    STR_ERROR[-OB_UPS_COUNT_OVER_LIMIT] = "ups count over limit";
    STR_USER_ERROR[-OB_UPS_COUNT_OVER_LIMIT] = "ups count over limit";
    ORACLE_ERRNO[-OB_UPS_COUNT_OVER_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_UPS_COUNT_OVER_LIMIT] =
        "ORA-00600: internal error code, arguments: -4610, ups count over limit";
    ORACLE_STR_USER_ERROR[-OB_UPS_COUNT_OVER_LIMIT] =
        "ORA-00600: internal error code, arguments: -4610, ups count over limit";
    ERROR_NAME[-OB_NO_UPS_MAJORITY] = "OB_NO_UPS_MAJORITY";
    MYSQL_ERRNO[-OB_NO_UPS_MAJORITY] = -1;
    SQLSTATE[-OB_NO_UPS_MAJORITY] = "HY000";
    STR_ERROR[-OB_NO_UPS_MAJORITY] = "ups not form a majority";
    STR_USER_ERROR[-OB_NO_UPS_MAJORITY] = "ups not form a majority";
    ORACLE_ERRNO[-OB_NO_UPS_MAJORITY] = 600;
    ORACLE_STR_ERROR[-OB_NO_UPS_MAJORITY] = "ORA-00600: internal error code, arguments: -4611, ups not form a majority";
    ORACLE_STR_USER_ERROR[-OB_NO_UPS_MAJORITY] =
        "ORA-00600: internal error code, arguments: -4611, ups not form a majority";
    ERROR_NAME[-OB_INDEX_COUNT_REACH_THE_LIMIT] = "OB_INDEX_COUNT_REACH_THE_LIMIT";
    MYSQL_ERRNO[-OB_INDEX_COUNT_REACH_THE_LIMIT] = -1;
    SQLSTATE[-OB_INDEX_COUNT_REACH_THE_LIMIT] = "HY000";
    STR_ERROR[-OB_INDEX_COUNT_REACH_THE_LIMIT] = "created index tables count has reach the limit:128";
    STR_USER_ERROR[-OB_INDEX_COUNT_REACH_THE_LIMIT] = "created index tables count has reach the limit:128";
    ORACLE_ERRNO[-OB_INDEX_COUNT_REACH_THE_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_INDEX_COUNT_REACH_THE_LIMIT] =
        "ORA-00600: internal error code, arguments: -4613, created index tables count has reach the limit:128";
    ORACLE_STR_USER_ERROR[-OB_INDEX_COUNT_REACH_THE_LIMIT] =
        "ORA-00600: internal error code, arguments: -4613, created index tables count has reach the limit:128";
    ERROR_NAME[-OB_TASK_EXPIRED] = "OB_TASK_EXPIRED";
    MYSQL_ERRNO[-OB_TASK_EXPIRED] = -1;
    SQLSTATE[-OB_TASK_EXPIRED] = "HY000";
    STR_ERROR[-OB_TASK_EXPIRED] = "task expired";
    STR_USER_ERROR[-OB_TASK_EXPIRED] = "task expired";
    ORACLE_ERRNO[-OB_TASK_EXPIRED] = 600;
    ORACLE_STR_ERROR[-OB_TASK_EXPIRED] = "ORA-00600: internal error code, arguments: -4614, task expired";
    ORACLE_STR_USER_ERROR[-OB_TASK_EXPIRED] = "ORA-00600: internal error code, arguments: -4614, task expired";
    ERROR_NAME[-OB_TABLEGROUP_NOT_EMPTY] = "OB_TABLEGROUP_NOT_EMPTY";
    MYSQL_ERRNO[-OB_TABLEGROUP_NOT_EMPTY] = -1;
    SQLSTATE[-OB_TABLEGROUP_NOT_EMPTY] = "HY000";
    STR_ERROR[-OB_TABLEGROUP_NOT_EMPTY] = "tablegroup is not empty";
    STR_USER_ERROR[-OB_TABLEGROUP_NOT_EMPTY] = "tablegroup is not empty";
    ORACLE_ERRNO[-OB_TABLEGROUP_NOT_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_TABLEGROUP_NOT_EMPTY] =
        "ORA-00600: internal error code, arguments: -4615, tablegroup is not empty";
    ORACLE_STR_USER_ERROR[-OB_TABLEGROUP_NOT_EMPTY] =
        "ORA-00600: internal error code, arguments: -4615, tablegroup is not empty";
    ERROR_NAME[-OB_INVALID_SERVER_STATUS] = "OB_INVALID_SERVER_STATUS";
    MYSQL_ERRNO[-OB_INVALID_SERVER_STATUS] = -1;
    SQLSTATE[-OB_INVALID_SERVER_STATUS] = "HY000";
    STR_ERROR[-OB_INVALID_SERVER_STATUS] = "server status is not valid";
    STR_USER_ERROR[-OB_INVALID_SERVER_STATUS] = "server status is not valid";
    ORACLE_ERRNO[-OB_INVALID_SERVER_STATUS] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_SERVER_STATUS] =
        "ORA-00600: internal error code, arguments: -4620, server status is not valid";
    ORACLE_STR_USER_ERROR[-OB_INVALID_SERVER_STATUS] =
        "ORA-00600: internal error code, arguments: -4620, server status is not valid";
    ERROR_NAME[-OB_WAIT_ELEC_LEADER_TIMEOUT] = "OB_WAIT_ELEC_LEADER_TIMEOUT";
    MYSQL_ERRNO[-OB_WAIT_ELEC_LEADER_TIMEOUT] = -1;
    SQLSTATE[-OB_WAIT_ELEC_LEADER_TIMEOUT] = "HY000";
    STR_ERROR[-OB_WAIT_ELEC_LEADER_TIMEOUT] = "wait elect partition leader timeout";
    STR_USER_ERROR[-OB_WAIT_ELEC_LEADER_TIMEOUT] = "wait elect partition leader timeout";
    ORACLE_ERRNO[-OB_WAIT_ELEC_LEADER_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_WAIT_ELEC_LEADER_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4621, wait elect partition leader timeout";
    ORACLE_STR_USER_ERROR[-OB_WAIT_ELEC_LEADER_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4621, wait elect partition leader timeout";
    ERROR_NAME[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] = "OB_WAIT_ALL_RS_ONLINE_TIMEOUT";
    MYSQL_ERRNO[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] = -1;
    SQLSTATE[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] = "HY000";
    STR_ERROR[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] = "wait all rs online timeout";
    STR_USER_ERROR[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] = "wait all rs online timeout";
    ORACLE_ERRNO[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4622, wait all rs online timeout";
    ORACLE_STR_USER_ERROR[-OB_WAIT_ALL_RS_ONLINE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4622, wait all rs online timeout";
    ERROR_NAME[-OB_ALL_REPLICAS_ON_MERGE_ZONE] = "OB_ALL_REPLICAS_ON_MERGE_ZONE";
    MYSQL_ERRNO[-OB_ALL_REPLICAS_ON_MERGE_ZONE] = -1;
    SQLSTATE[-OB_ALL_REPLICAS_ON_MERGE_ZONE] = "HY000";
    STR_ERROR[-OB_ALL_REPLICAS_ON_MERGE_ZONE] = "all replicas of partition group are on zones to merge";
    STR_USER_ERROR[-OB_ALL_REPLICAS_ON_MERGE_ZONE] = "all replicas of partition group are on zones to merge";
    ORACLE_ERRNO[-OB_ALL_REPLICAS_ON_MERGE_ZONE] = 600;
    ORACLE_STR_ERROR[-OB_ALL_REPLICAS_ON_MERGE_ZONE] =
        "ORA-00600: internal error code, arguments: -4623, all replicas of partition group are on zones to merge";
    ORACLE_STR_USER_ERROR[-OB_ALL_REPLICAS_ON_MERGE_ZONE] =
        "ORA-00600: internal error code, arguments: -4623, all replicas of partition group are on zones to merge";
    ERROR_NAME[-OB_MACHINE_RESOURCE_NOT_ENOUGH] = "OB_MACHINE_RESOURCE_NOT_ENOUGH";
    MYSQL_ERRNO[-OB_MACHINE_RESOURCE_NOT_ENOUGH] = -1;
    SQLSTATE[-OB_MACHINE_RESOURCE_NOT_ENOUGH] = "HY000";
    STR_ERROR[-OB_MACHINE_RESOURCE_NOT_ENOUGH] = "machine resource is not enough to hold a new unit";
    STR_USER_ERROR[-OB_MACHINE_RESOURCE_NOT_ENOUGH] = " machine resource \'%s\' is not enough to hold a new unit";
    ORACLE_ERRNO[-OB_MACHINE_RESOURCE_NOT_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_MACHINE_RESOURCE_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4624, machine resource is not enough to hold a new unit";
    ORACLE_STR_USER_ERROR[-OB_MACHINE_RESOURCE_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4624,  machine resource \'%s\' is not enough to hold a new unit";
    ERROR_NAME[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] = "OB_NOT_SERVER_CAN_HOLD_SOFTLY";
    MYSQL_ERRNO[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] = -1;
    SQLSTATE[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] = "HY000";
    STR_ERROR[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] = "not server can hole the unit and not over soft limit";
    STR_USER_ERROR[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] = "not server can hole the unit and not over soft limit";
    ORACLE_ERRNO[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] = 600;
    ORACLE_STR_ERROR[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] =
        "ORA-00600: internal error code, arguments: -4625, not server can hole the unit and not over soft limit";
    ORACLE_STR_USER_ERROR[-OB_NOT_SERVER_CAN_HOLD_SOFTLY] =
        "ORA-00600: internal error code, arguments: -4625, not server can hole the unit and not over soft limit";
    ERROR_NAME[-OB_RESOURCE_POOL_ALREADY_GRANTED] = "OB_RESOURCE_POOL_ALREADY_GRANTED";
    MYSQL_ERRNO[-OB_RESOURCE_POOL_ALREADY_GRANTED] = -1;
    SQLSTATE[-OB_RESOURCE_POOL_ALREADY_GRANTED] = "HY000";
    STR_ERROR[-OB_RESOURCE_POOL_ALREADY_GRANTED] = "resource pool has already been granted to a tenant";
    STR_USER_ERROR[-OB_RESOURCE_POOL_ALREADY_GRANTED] = "resource pool \'%s\' has already been granted to a tenant";
    ORACLE_ERRNO[-OB_RESOURCE_POOL_ALREADY_GRANTED] = 600;
    ORACLE_STR_ERROR[-OB_RESOURCE_POOL_ALREADY_GRANTED] =
        "ORA-00600: internal error code, arguments: -4626, resource pool has already been granted to a tenant";
    ORACLE_STR_USER_ERROR[-OB_RESOURCE_POOL_ALREADY_GRANTED] =
        "ORA-00600: internal error code, arguments: -4626, resource pool \'%s\' has already been granted to a tenant";
    ERROR_NAME[-OB_SERVER_ALREADY_DELETED] = "OB_SERVER_ALREADY_DELETED";
    MYSQL_ERRNO[-OB_SERVER_ALREADY_DELETED] = -1;
    SQLSTATE[-OB_SERVER_ALREADY_DELETED] = "HY000";
    STR_ERROR[-OB_SERVER_ALREADY_DELETED] = "server has already been deleted";
    STR_USER_ERROR[-OB_SERVER_ALREADY_DELETED] = "server has already been deleted";
    ORACLE_ERRNO[-OB_SERVER_ALREADY_DELETED] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_ALREADY_DELETED] =
        "ORA-00600: internal error code, arguments: -4628, server has already been deleted";
    ORACLE_STR_USER_ERROR[-OB_SERVER_ALREADY_DELETED] =
        "ORA-00600: internal error code, arguments: -4628, server has already been deleted";
    ERROR_NAME[-OB_SERVER_NOT_DELETING] = "OB_SERVER_NOT_DELETING";
    MYSQL_ERRNO[-OB_SERVER_NOT_DELETING] = -1;
    SQLSTATE[-OB_SERVER_NOT_DELETING] = "HY000";
    STR_ERROR[-OB_SERVER_NOT_DELETING] = "server is not in deleting status";
    STR_USER_ERROR[-OB_SERVER_NOT_DELETING] = "server is not in deleting status";
    ORACLE_ERRNO[-OB_SERVER_NOT_DELETING] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_NOT_DELETING] =
        "ORA-00600: internal error code, arguments: -4629, server is not in deleting status";
    ORACLE_STR_USER_ERROR[-OB_SERVER_NOT_DELETING] =
        "ORA-00600: internal error code, arguments: -4629, server is not in deleting status";
    ERROR_NAME[-OB_SERVER_NOT_IN_WHITE_LIST] = "OB_SERVER_NOT_IN_WHITE_LIST";
    MYSQL_ERRNO[-OB_SERVER_NOT_IN_WHITE_LIST] = -1;
    SQLSTATE[-OB_SERVER_NOT_IN_WHITE_LIST] = "HY000";
    STR_ERROR[-OB_SERVER_NOT_IN_WHITE_LIST] = "server not in server white list";
    STR_USER_ERROR[-OB_SERVER_NOT_IN_WHITE_LIST] = "server not in server white list";
    ORACLE_ERRNO[-OB_SERVER_NOT_IN_WHITE_LIST] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_NOT_IN_WHITE_LIST] =
        "ORA-00600: internal error code, arguments: -4630, server not in server white list";
    ORACLE_STR_USER_ERROR[-OB_SERVER_NOT_IN_WHITE_LIST] =
        "ORA-00600: internal error code, arguments: -4630, server not in server white list";
    ERROR_NAME[-OB_SERVER_ZONE_NOT_MATCH] = "OB_SERVER_ZONE_NOT_MATCH";
    MYSQL_ERRNO[-OB_SERVER_ZONE_NOT_MATCH] = -1;
    SQLSTATE[-OB_SERVER_ZONE_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_SERVER_ZONE_NOT_MATCH] = "server zone not match";
    STR_USER_ERROR[-OB_SERVER_ZONE_NOT_MATCH] = "server zone not match";
    ORACLE_ERRNO[-OB_SERVER_ZONE_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_ZONE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4631, server zone not match";
    ORACLE_STR_USER_ERROR[-OB_SERVER_ZONE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4631, server zone not match";
    ERROR_NAME[-OB_OVER_ZONE_NUM_LIMIT] = "OB_OVER_ZONE_NUM_LIMIT";
    MYSQL_ERRNO[-OB_OVER_ZONE_NUM_LIMIT] = -1;
    SQLSTATE[-OB_OVER_ZONE_NUM_LIMIT] = "HY000";
    STR_ERROR[-OB_OVER_ZONE_NUM_LIMIT] = "zone num has reach max zone num";
    STR_USER_ERROR[-OB_OVER_ZONE_NUM_LIMIT] = "zone num has reach max zone num";
    ORACLE_ERRNO[-OB_OVER_ZONE_NUM_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_OVER_ZONE_NUM_LIMIT] =
        "ORA-00600: internal error code, arguments: -4632, zone num has reach max zone num";
    ORACLE_STR_USER_ERROR[-OB_OVER_ZONE_NUM_LIMIT] =
        "ORA-00600: internal error code, arguments: -4632, zone num has reach max zone num";
    ERROR_NAME[-OB_ZONE_STATUS_NOT_MATCH] = "OB_ZONE_STATUS_NOT_MATCH";
    MYSQL_ERRNO[-OB_ZONE_STATUS_NOT_MATCH] = -1;
    SQLSTATE[-OB_ZONE_STATUS_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_ZONE_STATUS_NOT_MATCH] = "zone status not match";
    STR_USER_ERROR[-OB_ZONE_STATUS_NOT_MATCH] = "zone status not match";
    ORACLE_ERRNO[-OB_ZONE_STATUS_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_STATUS_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4633, zone status not match";
    ORACLE_STR_USER_ERROR[-OB_ZONE_STATUS_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4633, zone status not match";
    ERROR_NAME[-OB_RESOURCE_UNIT_IS_REFERENCED] = "OB_RESOURCE_UNIT_IS_REFERENCED";
    MYSQL_ERRNO[-OB_RESOURCE_UNIT_IS_REFERENCED] = -1;
    SQLSTATE[-OB_RESOURCE_UNIT_IS_REFERENCED] = "HY000";
    STR_ERROR[-OB_RESOURCE_UNIT_IS_REFERENCED] = "resource unit is referenced by resource pool";
    STR_USER_ERROR[-OB_RESOURCE_UNIT_IS_REFERENCED] = "resource unit \'%s\' is referenced by some resource pool";
    ORACLE_ERRNO[-OB_RESOURCE_UNIT_IS_REFERENCED] = 600;
    ORACLE_STR_ERROR[-OB_RESOURCE_UNIT_IS_REFERENCED] =
        "ORA-00600: internal error code, arguments: -4634, resource unit is referenced by resource pool";
    ORACLE_STR_USER_ERROR[-OB_RESOURCE_UNIT_IS_REFERENCED] =
        "ORA-00600: internal error code, arguments: -4634, resource unit \'%s\' is referenced by some resource pool";
    ERROR_NAME[-OB_DIFFERENT_PRIMARY_ZONE] = "OB_DIFFERENT_PRIMARY_ZONE";
    MYSQL_ERRNO[-OB_DIFFERENT_PRIMARY_ZONE] = -1;
    SQLSTATE[-OB_DIFFERENT_PRIMARY_ZONE] = "HY000";
    STR_ERROR[-OB_DIFFERENT_PRIMARY_ZONE] = "table schema primary zone different with other table in sampe tablegroup";
    STR_USER_ERROR[-OB_DIFFERENT_PRIMARY_ZONE] =
        "table schema primary zone different with other table in sampe tablegroup";
    ORACLE_ERRNO[-OB_DIFFERENT_PRIMARY_ZONE] = 600;
    ORACLE_STR_ERROR[-OB_DIFFERENT_PRIMARY_ZONE] = "ORA-00600: internal error code, arguments: -4636, table schema "
                                                   "primary zone different with other table in sampe tablegroup";
    ORACLE_STR_USER_ERROR[-OB_DIFFERENT_PRIMARY_ZONE] =
        "ORA-00600: internal error code, arguments: -4636, table schema primary zone different with other table in "
        "sampe tablegroup";
    ERROR_NAME[-OB_SERVER_NOT_ACTIVE] = "OB_SERVER_NOT_ACTIVE";
    MYSQL_ERRNO[-OB_SERVER_NOT_ACTIVE] = -1;
    SQLSTATE[-OB_SERVER_NOT_ACTIVE] = "HY000";
    STR_ERROR[-OB_SERVER_NOT_ACTIVE] = "server is not active";
    STR_USER_ERROR[-OB_SERVER_NOT_ACTIVE] = "server is not active";
    ORACLE_ERRNO[-OB_SERVER_NOT_ACTIVE] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_NOT_ACTIVE] = "ORA-00600: internal error code, arguments: -4637, server is not active";
    ORACLE_STR_USER_ERROR[-OB_SERVER_NOT_ACTIVE] =
        "ORA-00600: internal error code, arguments: -4637, server is not active";
    ERROR_NAME[-OB_RS_NOT_MASTER] = "OB_RS_NOT_MASTER";
    MYSQL_ERRNO[-OB_RS_NOT_MASTER] = -1;
    SQLSTATE[-OB_RS_NOT_MASTER] = "HY000";
    STR_ERROR[-OB_RS_NOT_MASTER] = "The RootServer is not the master";
    STR_USER_ERROR[-OB_RS_NOT_MASTER] = "The RootServer is not the master";
    ORACLE_ERRNO[-OB_RS_NOT_MASTER] = 600;
    ORACLE_STR_ERROR[-OB_RS_NOT_MASTER] =
        "ORA-00600: internal error code, arguments: -4638, The RootServer is not the master";
    ORACLE_STR_USER_ERROR[-OB_RS_NOT_MASTER] =
        "ORA-00600: internal error code, arguments: -4638, The RootServer is not the master";
    ERROR_NAME[-OB_CANDIDATE_LIST_ERROR] = "OB_CANDIDATE_LIST_ERROR";
    MYSQL_ERRNO[-OB_CANDIDATE_LIST_ERROR] = -1;
    SQLSTATE[-OB_CANDIDATE_LIST_ERROR] = "HY000";
    STR_ERROR[-OB_CANDIDATE_LIST_ERROR] = "The candidate list is invalid";
    STR_USER_ERROR[-OB_CANDIDATE_LIST_ERROR] = "The candidate list is invalid";
    ORACLE_ERRNO[-OB_CANDIDATE_LIST_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_CANDIDATE_LIST_ERROR] =
        "ORA-00600: internal error code, arguments: -4639, The candidate list is invalid";
    ORACLE_STR_USER_ERROR[-OB_CANDIDATE_LIST_ERROR] =
        "ORA-00600: internal error code, arguments: -4639, The candidate list is invalid";
    ERROR_NAME[-OB_PARTITION_ZONE_DUPLICATED] = "OB_PARTITION_ZONE_DUPLICATED";
    MYSQL_ERRNO[-OB_PARTITION_ZONE_DUPLICATED] = -1;
    SQLSTATE[-OB_PARTITION_ZONE_DUPLICATED] = "HY000";
    STR_ERROR[-OB_PARTITION_ZONE_DUPLICATED] = "The chosen partition servers belong to same zone.";
    STR_USER_ERROR[-OB_PARTITION_ZONE_DUPLICATED] = "The chosen partition servers belong to same zone.";
    ORACLE_ERRNO[-OB_PARTITION_ZONE_DUPLICATED] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_ZONE_DUPLICATED] =
        "ORA-00600: internal error code, arguments: -4640, The chosen partition servers belong to same zone.";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_ZONE_DUPLICATED] =
        "ORA-00600: internal error code, arguments: -4640, The chosen partition servers belong to same zone.";
    ERROR_NAME[-OB_ZONE_DUPLICATED] = "OB_ZONE_DUPLICATED";
    MYSQL_ERRNO[-OB_ZONE_DUPLICATED] = -1;
    SQLSTATE[-OB_ZONE_DUPLICATED] = "HY000";
    STR_ERROR[-OB_ZONE_DUPLICATED] = "Duplicated zone in zone list";
    STR_USER_ERROR[-OB_ZONE_DUPLICATED] = "Duplicated zone \'%s\' in zone list %s";
    ORACLE_ERRNO[-OB_ZONE_DUPLICATED] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_DUPLICATED] =
        "ORA-00600: internal error code, arguments: -4641, Duplicated zone in zone list";
    ORACLE_STR_USER_ERROR[-OB_ZONE_DUPLICATED] =
        "ORA-00600: internal error code, arguments: -4641, Duplicated zone \'%s\' in zone list %s";
    ERROR_NAME[-OB_NOT_ALL_ZONE_ACTIVE] = "OB_NOT_ALL_ZONE_ACTIVE";
    MYSQL_ERRNO[-OB_NOT_ALL_ZONE_ACTIVE] = -1;
    SQLSTATE[-OB_NOT_ALL_ZONE_ACTIVE] = "HY000";
    STR_ERROR[-OB_NOT_ALL_ZONE_ACTIVE] = "Not all zone in zone list are active";
    STR_USER_ERROR[-OB_NOT_ALL_ZONE_ACTIVE] = "Not all zone in zone list are active";
    ORACLE_ERRNO[-OB_NOT_ALL_ZONE_ACTIVE] = 600;
    ORACLE_STR_ERROR[-OB_NOT_ALL_ZONE_ACTIVE] =
        "ORA-00600: internal error code, arguments: -4642, Not all zone in zone list are active";
    ORACLE_STR_USER_ERROR[-OB_NOT_ALL_ZONE_ACTIVE] =
        "ORA-00600: internal error code, arguments: -4642, Not all zone in zone list are active";
    ERROR_NAME[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] = "OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST";
    MYSQL_ERRNO[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] = -1;
    SQLSTATE[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] = "HY000";
    STR_ERROR[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] = "primary zone not in zone list";
    STR_USER_ERROR[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] = "primary zone \'%s\' not in zone list %s";
    ORACLE_ERRNO[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] = 600;
    ORACLE_STR_ERROR[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] =
        "ORA-00600: internal error code, arguments: -4643, primary zone not in zone list";
    ORACLE_STR_USER_ERROR[-OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST] =
        "ORA-00600: internal error code, arguments: -4643, primary zone \'%s\' not in zone list %s";
    ERROR_NAME[-OB_REPLICA_NUM_NOT_MATCH] = "OB_REPLICA_NUM_NOT_MATCH";
    MYSQL_ERRNO[-OB_REPLICA_NUM_NOT_MATCH] = -1;
    SQLSTATE[-OB_REPLICA_NUM_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_REPLICA_NUM_NOT_MATCH] = "replica num not same with zone count";
    STR_USER_ERROR[-OB_REPLICA_NUM_NOT_MATCH] = "replica num not same with zone count";
    ORACLE_ERRNO[-OB_REPLICA_NUM_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_REPLICA_NUM_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4644, replica num not same with zone count";
    ORACLE_STR_USER_ERROR[-OB_REPLICA_NUM_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4644, replica num not same with zone count";
    ERROR_NAME[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] = "OB_ZONE_LIST_POOL_LIST_NOT_MATCH";
    MYSQL_ERRNO[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] = -1;
    SQLSTATE[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] = "zone list not a subset of  resource pool list";
    STR_USER_ERROR[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] = "zone list %s not a subset of resource pool zone list %s";
    ORACLE_ERRNO[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4645, zone list not a subset of  resource pool list";
    ORACLE_STR_USER_ERROR[-OB_ZONE_LIST_POOL_LIST_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -4645, zone list %s not a subset of resource pool zone list %s";
    ERROR_NAME[-OB_INVALID_TENANT_NAME] = "OB_INVALID_TENANT_NAME";
    MYSQL_ERRNO[-OB_INVALID_TENANT_NAME] = -1;
    SQLSTATE[-OB_INVALID_TENANT_NAME] = "HY000";
    STR_ERROR[-OB_INVALID_TENANT_NAME] = "tenant name is too long";
    STR_USER_ERROR[-OB_INVALID_TENANT_NAME] = "tenant name \'%s\' over max_tenant_name_length %ld";
    ORACLE_ERRNO[-OB_INVALID_TENANT_NAME] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_TENANT_NAME] =
        "ORA-00600: internal error code, arguments: -4646, tenant name is too long";
    ORACLE_STR_USER_ERROR[-OB_INVALID_TENANT_NAME] =
        "ORA-00600: internal error code, arguments: -4646, tenant name \'%s\' over max_tenant_name_length %ld";
    ERROR_NAME[-OB_EMPTY_RESOURCE_POOL_LIST] = "OB_EMPTY_RESOURCE_POOL_LIST";
    MYSQL_ERRNO[-OB_EMPTY_RESOURCE_POOL_LIST] = -1;
    SQLSTATE[-OB_EMPTY_RESOURCE_POOL_LIST] = "HY000";
    STR_ERROR[-OB_EMPTY_RESOURCE_POOL_LIST] = "resource pool list is empty";
    STR_USER_ERROR[-OB_EMPTY_RESOURCE_POOL_LIST] = "resource pool list is empty";
    ORACLE_ERRNO[-OB_EMPTY_RESOURCE_POOL_LIST] = 600;
    ORACLE_STR_ERROR[-OB_EMPTY_RESOURCE_POOL_LIST] =
        "ORA-00600: internal error code, arguments: -4647, resource pool list is empty";
    ORACLE_STR_USER_ERROR[-OB_EMPTY_RESOURCE_POOL_LIST] =
        "ORA-00600: internal error code, arguments: -4647, resource pool list is empty";
    ERROR_NAME[-OB_RESOURCE_UNIT_NOT_EXIST] = "OB_RESOURCE_UNIT_NOT_EXIST";
    MYSQL_ERRNO[-OB_RESOURCE_UNIT_NOT_EXIST] = -1;
    SQLSTATE[-OB_RESOURCE_UNIT_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_RESOURCE_UNIT_NOT_EXIST] = "resource unit not exist";
    STR_USER_ERROR[-OB_RESOURCE_UNIT_NOT_EXIST] = "resource unit \'%s\' not exist";
    ORACLE_ERRNO[-OB_RESOURCE_UNIT_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_RESOURCE_UNIT_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4648, resource unit not exist";
    ORACLE_STR_USER_ERROR[-OB_RESOURCE_UNIT_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4648, resource unit \'%s\' not exist";
    ERROR_NAME[-OB_RESOURCE_UNIT_EXIST] = "OB_RESOURCE_UNIT_EXIST";
    MYSQL_ERRNO[-OB_RESOURCE_UNIT_EXIST] = -1;
    SQLSTATE[-OB_RESOURCE_UNIT_EXIST] = "HY000";
    STR_ERROR[-OB_RESOURCE_UNIT_EXIST] = "resource unit already exist";
    STR_USER_ERROR[-OB_RESOURCE_UNIT_EXIST] = "resource unit \'%s\' already exist";
    ORACLE_ERRNO[-OB_RESOURCE_UNIT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_RESOURCE_UNIT_EXIST] =
        "ORA-00600: internal error code, arguments: -4649, resource unit already exist";
    ORACLE_STR_USER_ERROR[-OB_RESOURCE_UNIT_EXIST] =
        "ORA-00600: internal error code, arguments: -4649, resource unit \'%s\' already exist";
    ERROR_NAME[-OB_RESOURCE_POOL_NOT_EXIST] = "OB_RESOURCE_POOL_NOT_EXIST";
    MYSQL_ERRNO[-OB_RESOURCE_POOL_NOT_EXIST] = -1;
    SQLSTATE[-OB_RESOURCE_POOL_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_RESOURCE_POOL_NOT_EXIST] = "resource pool not exist";
    STR_USER_ERROR[-OB_RESOURCE_POOL_NOT_EXIST] = "resource pool \'%s\' not exist";
    ORACLE_ERRNO[-OB_RESOURCE_POOL_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_RESOURCE_POOL_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4650, resource pool not exist";
    ORACLE_STR_USER_ERROR[-OB_RESOURCE_POOL_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4650, resource pool \'%s\' not exist";
    ERROR_NAME[-OB_RESOURCE_POOL_EXIST] = "OB_RESOURCE_POOL_EXIST";
    MYSQL_ERRNO[-OB_RESOURCE_POOL_EXIST] = -1;
    SQLSTATE[-OB_RESOURCE_POOL_EXIST] = "HY000";
    STR_ERROR[-OB_RESOURCE_POOL_EXIST] = "resource pool already exist";
    STR_USER_ERROR[-OB_RESOURCE_POOL_EXIST] = "resource pool \'%s\' exist";
    ORACLE_ERRNO[-OB_RESOURCE_POOL_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_RESOURCE_POOL_EXIST] =
        "ORA-00600: internal error code, arguments: -4651, resource pool already exist";
    ORACLE_STR_USER_ERROR[-OB_RESOURCE_POOL_EXIST] =
        "ORA-00600: internal error code, arguments: -4651, resource pool \'%s\' exist";
    ERROR_NAME[-OB_WAIT_LEADER_SWITCH_TIMEOUT] = "OB_WAIT_LEADER_SWITCH_TIMEOUT";
    MYSQL_ERRNO[-OB_WAIT_LEADER_SWITCH_TIMEOUT] = -1;
    SQLSTATE[-OB_WAIT_LEADER_SWITCH_TIMEOUT] = "HY000";
    STR_ERROR[-OB_WAIT_LEADER_SWITCH_TIMEOUT] = "wait leader switch timeout";
    STR_USER_ERROR[-OB_WAIT_LEADER_SWITCH_TIMEOUT] = "wait leader switch timeout";
    ORACLE_ERRNO[-OB_WAIT_LEADER_SWITCH_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_WAIT_LEADER_SWITCH_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4652, wait leader switch timeout";
    ORACLE_STR_USER_ERROR[-OB_WAIT_LEADER_SWITCH_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4652, wait leader switch timeout";
    ERROR_NAME[-OB_LOCATION_NOT_EXIST] = "OB_LOCATION_NOT_EXIST";
    MYSQL_ERRNO[-OB_LOCATION_NOT_EXIST] = -1;
    SQLSTATE[-OB_LOCATION_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_LOCATION_NOT_EXIST] = "location not exist";
    STR_USER_ERROR[-OB_LOCATION_NOT_EXIST] = "location not exist";
    ORACLE_ERRNO[-OB_LOCATION_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_LOCATION_NOT_EXIST] = "ORA-00600: internal error code, arguments: -4653, location not exist";
    ORACLE_STR_USER_ERROR[-OB_LOCATION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4653, location not exist";
    ERROR_NAME[-OB_LOCATION_LEADER_NOT_EXIST] = "OB_LOCATION_LEADER_NOT_EXIST";
    MYSQL_ERRNO[-OB_LOCATION_LEADER_NOT_EXIST] = -1;
    SQLSTATE[-OB_LOCATION_LEADER_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_LOCATION_LEADER_NOT_EXIST] = "location leader not exist";
    STR_USER_ERROR[-OB_LOCATION_LEADER_NOT_EXIST] = "location leader not exist";
    ORACLE_ERRNO[-OB_LOCATION_LEADER_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_LOCATION_LEADER_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4654, location leader not exist";
    ORACLE_STR_USER_ERROR[-OB_LOCATION_LEADER_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -4654, location leader not exist";
    ERROR_NAME[-OB_ZONE_NOT_ACTIVE] = "OB_ZONE_NOT_ACTIVE";
    MYSQL_ERRNO[-OB_ZONE_NOT_ACTIVE] = -1;
    SQLSTATE[-OB_ZONE_NOT_ACTIVE] = "HY000";
    STR_ERROR[-OB_ZONE_NOT_ACTIVE] = "zone not active";
    STR_USER_ERROR[-OB_ZONE_NOT_ACTIVE] = "zone not active";
    ORACLE_ERRNO[-OB_ZONE_NOT_ACTIVE] = 600;
    ORACLE_STR_ERROR[-OB_ZONE_NOT_ACTIVE] = "ORA-00600: internal error code, arguments: -4655, zone not active";
    ORACLE_STR_USER_ERROR[-OB_ZONE_NOT_ACTIVE] = "ORA-00600: internal error code, arguments: -4655, zone not active";
    ERROR_NAME[-OB_UNIT_NUM_OVER_SERVER_COUNT] = "OB_UNIT_NUM_OVER_SERVER_COUNT";
    MYSQL_ERRNO[-OB_UNIT_NUM_OVER_SERVER_COUNT] = -1;
    SQLSTATE[-OB_UNIT_NUM_OVER_SERVER_COUNT] = "HY000";
    STR_ERROR[-OB_UNIT_NUM_OVER_SERVER_COUNT] = "resource pool unit num is bigger than zone server count";
    STR_USER_ERROR[-OB_UNIT_NUM_OVER_SERVER_COUNT] = "resource pool unit num is bigger than zone server count";
    ORACLE_ERRNO[-OB_UNIT_NUM_OVER_SERVER_COUNT] = 600;
    ORACLE_STR_ERROR[-OB_UNIT_NUM_OVER_SERVER_COUNT] =
        "ORA-00600: internal error code, arguments: -4656, resource pool unit num is bigger than zone server count";
    ORACLE_STR_USER_ERROR[-OB_UNIT_NUM_OVER_SERVER_COUNT] =
        "ORA-00600: internal error code, arguments: -4656, resource pool unit num is bigger than zone server count";
    ERROR_NAME[-OB_POOL_SERVER_INTERSECT] = "OB_POOL_SERVER_INTERSECT";
    MYSQL_ERRNO[-OB_POOL_SERVER_INTERSECT] = -1;
    SQLSTATE[-OB_POOL_SERVER_INTERSECT] = "HY000";
    STR_ERROR[-OB_POOL_SERVER_INTERSECT] = "resource pool list unit server intersect";
    STR_USER_ERROR[-OB_POOL_SERVER_INTERSECT] = "resource pool list %s unit servers intersect";
    ORACLE_ERRNO[-OB_POOL_SERVER_INTERSECT] = 600;
    ORACLE_STR_ERROR[-OB_POOL_SERVER_INTERSECT] =
        "ORA-00600: internal error code, arguments: -4657, resource pool list unit server intersect";
    ORACLE_STR_USER_ERROR[-OB_POOL_SERVER_INTERSECT] =
        "ORA-00600: internal error code, arguments: -4657, resource pool list %s unit servers intersect";
    ERROR_NAME[-OB_NOT_SINGLE_RESOURCE_POOL] = "OB_NOT_SINGLE_RESOURCE_POOL";
    MYSQL_ERRNO[-OB_NOT_SINGLE_RESOURCE_POOL] = -1;
    SQLSTATE[-OB_NOT_SINGLE_RESOURCE_POOL] = "HY000";
    STR_ERROR[-OB_NOT_SINGLE_RESOURCE_POOL] = "create tenant only support single resource pool now";
    STR_USER_ERROR[-OB_NOT_SINGLE_RESOURCE_POOL] =
        "create tenant only support single resource pool now, but pool list is %s";
    ORACLE_ERRNO[-OB_NOT_SINGLE_RESOURCE_POOL] = 600;
    ORACLE_STR_ERROR[-OB_NOT_SINGLE_RESOURCE_POOL] =
        "ORA-00600: internal error code, arguments: -4658, create tenant only support single resource pool now";
    ORACLE_STR_USER_ERROR[-OB_NOT_SINGLE_RESOURCE_POOL] =
        "ORA-00600: internal error code, arguments: -4658, create tenant only support single resource pool now, but "
        "pool list is %s";
    ERROR_NAME[-OB_INVALID_RESOURCE_UNIT] = "OB_INVALID_RESOURCE_UNIT";
    MYSQL_ERRNO[-OB_INVALID_RESOURCE_UNIT] = -1;
    SQLSTATE[-OB_INVALID_RESOURCE_UNIT] = "HY000";
    STR_ERROR[-OB_INVALID_RESOURCE_UNIT] = "invalid resource unit";
    STR_USER_ERROR[-OB_INVALID_RESOURCE_UNIT] = "invalid resource unit, %s\'s min value is %s";
    ORACLE_ERRNO[-OB_INVALID_RESOURCE_UNIT] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_RESOURCE_UNIT] =
        "ORA-00600: internal error code, arguments: -4659, invalid resource unit";
    ORACLE_STR_USER_ERROR[-OB_INVALID_RESOURCE_UNIT] =
        "ORA-00600: internal error code, arguments: -4659, invalid resource unit, %s\'s min value is %s";
    ERROR_NAME[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] = "OB_STOP_SERVER_IN_MULTIPLE_ZONES";
    MYSQL_ERRNO[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] = -1;
    SQLSTATE[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] = "HY000";
    STR_ERROR[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] = "Can not stop server in multiple zones";
    STR_USER_ERROR[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] = "%s";
    ORACLE_ERRNO[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] = 600;
    ORACLE_STR_ERROR[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] =
        "ORA-00600: internal error code, arguments: -4660, Can not stop server in multiple zones";
    ORACLE_STR_USER_ERROR[-OB_STOP_SERVER_IN_MULTIPLE_ZONES] = "ORA-00600: internal error code, arguments: -4660, %s";
    ERROR_NAME[-OB_SESSION_ENTRY_EXIST] = "OB_SESSION_ENTRY_EXIST";
    MYSQL_ERRNO[-OB_SESSION_ENTRY_EXIST] = -1;
    SQLSTATE[-OB_SESSION_ENTRY_EXIST] = "HY000";
    STR_ERROR[-OB_SESSION_ENTRY_EXIST] = "Session already exist";
    STR_USER_ERROR[-OB_SESSION_ENTRY_EXIST] = "Session already exist";
    ORACLE_ERRNO[-OB_SESSION_ENTRY_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_SESSION_ENTRY_EXIST] =
        "ORA-00600: internal error code, arguments: -4661, Session already exist";
    ORACLE_STR_USER_ERROR[-OB_SESSION_ENTRY_EXIST] =
        "ORA-00600: internal error code, arguments: -4661, Session already exist";
    ERROR_NAME[-OB_GOT_SIGNAL_ABORTING] = "OB_GOT_SIGNAL_ABORTING";
    MYSQL_ERRNO[-OB_GOT_SIGNAL_ABORTING] = ER_GOT_SIGNAL;
    SQLSTATE[-OB_GOT_SIGNAL_ABORTING] = "01000";
    STR_ERROR[-OB_GOT_SIGNAL_ABORTING] = "Got signal. Aborting!";
    STR_USER_ERROR[-OB_GOT_SIGNAL_ABORTING] = "%s: Got signal %d. Aborting!";
    ORACLE_ERRNO[-OB_GOT_SIGNAL_ABORTING] = 600;
    ORACLE_STR_ERROR[-OB_GOT_SIGNAL_ABORTING] =
        "ORA-00600: internal error code, arguments: -4662, Got signal. Aborting!";
    ORACLE_STR_USER_ERROR[-OB_GOT_SIGNAL_ABORTING] =
        "ORA-00600: internal error code, arguments: -4662, %s: Got signal %d. Aborting!";
    ERROR_NAME[-OB_SERVER_NOT_ALIVE] = "OB_SERVER_NOT_ALIVE";
    MYSQL_ERRNO[-OB_SERVER_NOT_ALIVE] = -1;
    SQLSTATE[-OB_SERVER_NOT_ALIVE] = "HY000";
    STR_ERROR[-OB_SERVER_NOT_ALIVE] = "server is not alive";
    STR_USER_ERROR[-OB_SERVER_NOT_ALIVE] = "server is not alive";
    ORACLE_ERRNO[-OB_SERVER_NOT_ALIVE] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_NOT_ALIVE] = "ORA-00600: internal error code, arguments: -4663, server is not alive";
    ORACLE_STR_USER_ERROR[-OB_SERVER_NOT_ALIVE] =
        "ORA-00600: internal error code, arguments: -4663, server is not alive";
    ERROR_NAME[-OB_GET_LOCATION_TIME_OUT] = "OB_GET_LOCATION_TIME_OUT";
    MYSQL_ERRNO[-OB_GET_LOCATION_TIME_OUT] = 4012;
    SQLSTATE[-OB_GET_LOCATION_TIME_OUT] = "HY000";
    STR_ERROR[-OB_GET_LOCATION_TIME_OUT] = "Timeout";
    STR_USER_ERROR[-OB_GET_LOCATION_TIME_OUT] = "Timeout";
    ORACLE_ERRNO[-OB_GET_LOCATION_TIME_OUT] = 600;
    ORACLE_STR_ERROR[-OB_GET_LOCATION_TIME_OUT] = "ORA-00600: internal error code, arguments: -4664, Timeout";
    ORACLE_STR_USER_ERROR[-OB_GET_LOCATION_TIME_OUT] = "ORA-00600: internal error code, arguments: -4664, Timeout";
    ERROR_NAME[-OB_UNIT_IS_MIGRATING] = "OB_UNIT_IS_MIGRATING";
    MYSQL_ERRNO[-OB_UNIT_IS_MIGRATING] = -1;
    SQLSTATE[-OB_UNIT_IS_MIGRATING] = "HY000";
    STR_ERROR[-OB_UNIT_IS_MIGRATING] = "Unit is migrating, can not migrate again";
    STR_USER_ERROR[-OB_UNIT_IS_MIGRATING] = "Unit is migrating, can not migrate again";
    ORACLE_ERRNO[-OB_UNIT_IS_MIGRATING] = 600;
    ORACLE_STR_ERROR[-OB_UNIT_IS_MIGRATING] =
        "ORA-00600: internal error code, arguments: -4665, Unit is migrating, can not migrate again";
    ORACLE_STR_USER_ERROR[-OB_UNIT_IS_MIGRATING] =
        "ORA-00600: internal error code, arguments: -4665, Unit is migrating, can not migrate again";
    ERROR_NAME[-OB_CLUSTER_NO_MATCH] = "OB_CLUSTER_NO_MATCH";
    MYSQL_ERRNO[-OB_CLUSTER_NO_MATCH] = -1;
    SQLSTATE[-OB_CLUSTER_NO_MATCH] = "HY000";
    STR_ERROR[-OB_CLUSTER_NO_MATCH] = "cluster name does not match";
    STR_USER_ERROR[-OB_CLUSTER_NO_MATCH] = "cluster name does not match to \'%s\'";
    ORACLE_ERRNO[-OB_CLUSTER_NO_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_CLUSTER_NO_MATCH] =
        "ORA-00600: internal error code, arguments: -4666, cluster name does not match";
    ORACLE_STR_USER_ERROR[-OB_CLUSTER_NO_MATCH] =
        "ORA-00600: internal error code, arguments: -4666, cluster name does not match to \'%s\'";
    ERROR_NAME[-OB_CHECK_ZONE_MERGE_ORDER] = "OB_CHECK_ZONE_MERGE_ORDER";
    MYSQL_ERRNO[-OB_CHECK_ZONE_MERGE_ORDER] = -1;
    SQLSTATE[-OB_CHECK_ZONE_MERGE_ORDER] = "HY000";
    STR_ERROR[-OB_CHECK_ZONE_MERGE_ORDER] =
        "Please check new zone in zone_merge_order. You can show parameters like 'zone_merge_order'";
    STR_USER_ERROR[-OB_CHECK_ZONE_MERGE_ORDER] =
        "Please check new zone in zone_merge_order. You can show parameters like 'zone_merge_order'";
    ORACLE_ERRNO[-OB_CHECK_ZONE_MERGE_ORDER] = 600;
    ORACLE_STR_ERROR[-OB_CHECK_ZONE_MERGE_ORDER] =
        "ORA-00600: internal error code, arguments: -4667, Please check new zone in zone_merge_order. You can show "
        "parameters like 'zone_merge_order'";
    ORACLE_STR_USER_ERROR[-OB_CHECK_ZONE_MERGE_ORDER] =
        "ORA-00600: internal error code, arguments: -4667, Please check new zone in zone_merge_order. You can show "
        "parameters like 'zone_merge_order'";
    ERROR_NAME[-OB_ERR_ZONE_NOT_EMPTY] = "OB_ERR_ZONE_NOT_EMPTY";
    MYSQL_ERRNO[-OB_ERR_ZONE_NOT_EMPTY] = -1;
    SQLSTATE[-OB_ERR_ZONE_NOT_EMPTY] = "HY000";
    STR_ERROR[-OB_ERR_ZONE_NOT_EMPTY] = "zone not empty";
    STR_USER_ERROR[-OB_ERR_ZONE_NOT_EMPTY] = "The zone is not empty and can not be deleted. You should delete the "
                                             "servers of the zone. There are %ld servers alive and %ld not alive.";
    ORACLE_ERRNO[-OB_ERR_ZONE_NOT_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ZONE_NOT_EMPTY] = "ORA-00600: internal error code, arguments: -4668, zone not empty";
    ORACLE_STR_USER_ERROR[-OB_ERR_ZONE_NOT_EMPTY] =
        "ORA-00600: internal error code, arguments: -4668, The zone is not empty and can not be deleted. You should "
        "delete the servers of the zone. There are %ld servers alive and %ld not alive.";
    ERROR_NAME[-OB_DIFFERENT_LOCALITY] = "OB_DIFFERENT_LOCALITY";
    MYSQL_ERRNO[-OB_DIFFERENT_LOCALITY] = -1;
    SQLSTATE[-OB_DIFFERENT_LOCALITY] = "HY000";
    STR_ERROR[-OB_DIFFERENT_LOCALITY] = "locality not match, check it";
    STR_USER_ERROR[-OB_DIFFERENT_LOCALITY] = "locality not match, check it";
    ORACLE_ERRNO[-OB_DIFFERENT_LOCALITY] = 600;
    ORACLE_STR_ERROR[-OB_DIFFERENT_LOCALITY] =
        "ORA-00600: internal error code, arguments: -4669, locality not match, check it";
    ORACLE_STR_USER_ERROR[-OB_DIFFERENT_LOCALITY] =
        "ORA-00600: internal error code, arguments: -4669, locality not match, check it";
    ERROR_NAME[-OB_EMPTY_LOCALITY] = "OB_EMPTY_LOCALITY";
    MYSQL_ERRNO[-OB_EMPTY_LOCALITY] = -1;
    SQLSTATE[-OB_EMPTY_LOCALITY] = "HY000";
    STR_ERROR[-OB_EMPTY_LOCALITY] = "locality is empty";
    STR_USER_ERROR[-OB_EMPTY_LOCALITY] = "locality is empty";
    ORACLE_ERRNO[-OB_EMPTY_LOCALITY] = 600;
    ORACLE_STR_ERROR[-OB_EMPTY_LOCALITY] = "ORA-00600: internal error code, arguments: -4670, locality is empty";
    ORACLE_STR_USER_ERROR[-OB_EMPTY_LOCALITY] = "ORA-00600: internal error code, arguments: -4670, locality is empty";
    ERROR_NAME[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] = "OB_FULL_REPLICA_NUM_NOT_ENOUGH";
    MYSQL_ERRNO[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] = -1;
    SQLSTATE[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] = "HY000";
    STR_ERROR[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] = "full replica num not enough";
    STR_USER_ERROR[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] = "full replica num not enough";
    ORACLE_ERRNO[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4671, full replica num not enough";
    ORACLE_STR_USER_ERROR[-OB_FULL_REPLICA_NUM_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4671, full replica num not enough";
    ERROR_NAME[-OB_REPLICA_NUM_NOT_ENOUGH] = "OB_REPLICA_NUM_NOT_ENOUGH";
    MYSQL_ERRNO[-OB_REPLICA_NUM_NOT_ENOUGH] = -1;
    SQLSTATE[-OB_REPLICA_NUM_NOT_ENOUGH] = "HY000";
    STR_ERROR[-OB_REPLICA_NUM_NOT_ENOUGH] = "replica num not enough";
    STR_USER_ERROR[-OB_REPLICA_NUM_NOT_ENOUGH] = "replica num not enough";
    ORACLE_ERRNO[-OB_REPLICA_NUM_NOT_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_REPLICA_NUM_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4672, replica num not enough";
    ORACLE_STR_USER_ERROR[-OB_REPLICA_NUM_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -4672, replica num not enough";
    ERROR_NAME[-OB_DATA_SOURCE_NOT_VALID] = "OB_DATA_SOURCE_NOT_VALID";
    MYSQL_ERRNO[-OB_DATA_SOURCE_NOT_VALID] = -1;
    SQLSTATE[-OB_DATA_SOURCE_NOT_VALID] = "HY000";
    STR_ERROR[-OB_DATA_SOURCE_NOT_VALID] = "Data source not valid";
    STR_USER_ERROR[-OB_DATA_SOURCE_NOT_VALID] = "Data source not valid";
    ORACLE_ERRNO[-OB_DATA_SOURCE_NOT_VALID] = 600;
    ORACLE_STR_ERROR[-OB_DATA_SOURCE_NOT_VALID] =
        "ORA-00600: internal error code, arguments: -4673, Data source not valid";
    ORACLE_STR_USER_ERROR[-OB_DATA_SOURCE_NOT_VALID] =
        "ORA-00600: internal error code, arguments: -4673, Data source not valid";
    ERROR_NAME[-OB_RUN_JOB_NOT_SUCCESS] = "OB_RUN_JOB_NOT_SUCCESS";
    MYSQL_ERRNO[-OB_RUN_JOB_NOT_SUCCESS] = -1;
    SQLSTATE[-OB_RUN_JOB_NOT_SUCCESS] = "HY000";
    STR_ERROR[-OB_RUN_JOB_NOT_SUCCESS] = "run job not success yet";
    STR_USER_ERROR[-OB_RUN_JOB_NOT_SUCCESS] = "run job not success yet";
    ORACLE_ERRNO[-OB_RUN_JOB_NOT_SUCCESS] = 600;
    ORACLE_STR_ERROR[-OB_RUN_JOB_NOT_SUCCESS] =
        "ORA-00600: internal error code, arguments: -4674, run job not success yet";
    ORACLE_STR_USER_ERROR[-OB_RUN_JOB_NOT_SUCCESS] =
        "ORA-00600: internal error code, arguments: -4674, run job not success yet";
    ERROR_NAME[-OB_NO_NEED_REBUILD] = "OB_NO_NEED_REBUILD";
    MYSQL_ERRNO[-OB_NO_NEED_REBUILD] = -1;
    SQLSTATE[-OB_NO_NEED_REBUILD] = "HY000";
    STR_ERROR[-OB_NO_NEED_REBUILD] = "no need to rebuild";
    STR_USER_ERROR[-OB_NO_NEED_REBUILD] = "no need to rebuild";
    ORACLE_ERRNO[-OB_NO_NEED_REBUILD] = 600;
    ORACLE_STR_ERROR[-OB_NO_NEED_REBUILD] = "ORA-00600: internal error code, arguments: -4675, no need to rebuild";
    ORACLE_STR_USER_ERROR[-OB_NO_NEED_REBUILD] = "ORA-00600: internal error code, arguments: -4675, no need to rebuild";
    ERROR_NAME[-OB_NEED_REMOVE_UNNEED_TABLE] = "OB_NEED_REMOVE_UNNEED_TABLE";
    MYSQL_ERRNO[-OB_NEED_REMOVE_UNNEED_TABLE] = -1;
    SQLSTATE[-OB_NEED_REMOVE_UNNEED_TABLE] = "HY000";
    STR_ERROR[-OB_NEED_REMOVE_UNNEED_TABLE] = "need remove unneed table";
    STR_USER_ERROR[-OB_NEED_REMOVE_UNNEED_TABLE] = "need remove unneed table";
    ORACLE_ERRNO[-OB_NEED_REMOVE_UNNEED_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_NEED_REMOVE_UNNEED_TABLE] =
        "ORA-00600: internal error code, arguments: -4676, need remove unneed table";
    ORACLE_STR_USER_ERROR[-OB_NEED_REMOVE_UNNEED_TABLE] =
        "ORA-00600: internal error code, arguments: -4676, need remove unneed table";
    ERROR_NAME[-OB_NO_NEED_MERGE] = "OB_NO_NEED_MERGE";
    MYSQL_ERRNO[-OB_NO_NEED_MERGE] = -1;
    SQLSTATE[-OB_NO_NEED_MERGE] = "HY000";
    STR_ERROR[-OB_NO_NEED_MERGE] = "no need to merge";
    STR_USER_ERROR[-OB_NO_NEED_MERGE] = "no need to merge";
    ORACLE_ERRNO[-OB_NO_NEED_MERGE] = 600;
    ORACLE_STR_ERROR[-OB_NO_NEED_MERGE] = "ORA-00600: internal error code, arguments: -4677, no need to merge";
    ORACLE_STR_USER_ERROR[-OB_NO_NEED_MERGE] = "ORA-00600: internal error code, arguments: -4677, no need to merge";
    ERROR_NAME[-OB_CONFLICT_OPTION] = "OB_CONFLICT_OPTION";
    MYSQL_ERRNO[-OB_CONFLICT_OPTION] = -1;
    SQLSTATE[-OB_CONFLICT_OPTION] = "HY000";
    STR_ERROR[-OB_CONFLICT_OPTION] = "conflicting specifications";
    STR_USER_ERROR[-OB_CONFLICT_OPTION] = "conflicting %.*s specifications";
    ORACLE_ERRNO[-OB_CONFLICT_OPTION] = 600;
    ORACLE_STR_ERROR[-OB_CONFLICT_OPTION] =
        "ORA-00600: internal error code, arguments: -4678, conflicting specifications";
    ORACLE_STR_USER_ERROR[-OB_CONFLICT_OPTION] =
        "ORA-00600: internal error code, arguments: -4678, conflicting %.*s specifications";
    ERROR_NAME[-OB_DUPLICATE_OPTION] = "OB_DUPLICATE_OPTION";
    MYSQL_ERRNO[-OB_DUPLICATE_OPTION] = -1;
    SQLSTATE[-OB_DUPLICATE_OPTION] = "HY000";
    STR_ERROR[-OB_DUPLICATE_OPTION] = "duplicate specifications";
    STR_USER_ERROR[-OB_DUPLICATE_OPTION] = "duplicate %.*s specifications";
    ORACLE_ERRNO[-OB_DUPLICATE_OPTION] = 600;
    ORACLE_STR_ERROR[-OB_DUPLICATE_OPTION] =
        "ORA-00600: internal error code, arguments: -4679, duplicate specifications";
    ORACLE_STR_USER_ERROR[-OB_DUPLICATE_OPTION] =
        "ORA-00600: internal error code, arguments: -4679, duplicate %.*s specifications";
    ERROR_NAME[-OB_INVALID_OPTION] = "OB_INVALID_OPTION";
    MYSQL_ERRNO[-OB_INVALID_OPTION] = -1;
    SQLSTATE[-OB_INVALID_OPTION] = "HY000";
    STR_ERROR[-OB_INVALID_OPTION] = "invalid specifications";
    STR_USER_ERROR[-OB_INVALID_OPTION] = "%s";
    ORACLE_ERRNO[-OB_INVALID_OPTION] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_OPTION] = "ORA-00600: internal error code, arguments: -4680, invalid specifications";
    ORACLE_STR_USER_ERROR[-OB_INVALID_OPTION] = "ORA-00600: internal error code, arguments: -4680, %s";
    ERROR_NAME[-OB_RPC_NEED_RECONNECT] = "OB_RPC_NEED_RECONNECT";
    MYSQL_ERRNO[-OB_RPC_NEED_RECONNECT] = -1;
    SQLSTATE[-OB_RPC_NEED_RECONNECT] = "HY000";
    STR_ERROR[-OB_RPC_NEED_RECONNECT] = "rpc need reconnect";
    STR_USER_ERROR[-OB_RPC_NEED_RECONNECT] = "%s";
    ORACLE_ERRNO[-OB_RPC_NEED_RECONNECT] = 600;
    ORACLE_STR_ERROR[-OB_RPC_NEED_RECONNECT] = "ORA-00600: internal error code, arguments: -4681, rpc need reconnect";
    ORACLE_STR_USER_ERROR[-OB_RPC_NEED_RECONNECT] = "ORA-00600: internal error code, arguments: -4681, %s";
    ERROR_NAME[-OB_CANNOT_COPY_MAJOR_SSTABLE] = "OB_CANNOT_COPY_MAJOR_SSTABLE";
    MYSQL_ERRNO[-OB_CANNOT_COPY_MAJOR_SSTABLE] = -1;
    SQLSTATE[-OB_CANNOT_COPY_MAJOR_SSTABLE] = "HY000";
    STR_ERROR[-OB_CANNOT_COPY_MAJOR_SSTABLE] = "cannot copy major sstable now";
    STR_USER_ERROR[-OB_CANNOT_COPY_MAJOR_SSTABLE] = "cannot copy major sstable now";
    ORACLE_ERRNO[-OB_CANNOT_COPY_MAJOR_SSTABLE] = 600;
    ORACLE_STR_ERROR[-OB_CANNOT_COPY_MAJOR_SSTABLE] =
        "ORA-00600: internal error code, arguments: -4682, cannot copy major sstable now";
    ORACLE_STR_USER_ERROR[-OB_CANNOT_COPY_MAJOR_SSTABLE] =
        "ORA-00600: internal error code, arguments: -4682, cannot copy major sstable now";
    ERROR_NAME[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] = "OB_SRC_DO_NOT_ALLOWED_MIGRATE";
    MYSQL_ERRNO[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] = -1;
    SQLSTATE[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] = "HY000";
    STR_ERROR[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] = "src do not allowed migrate";
    STR_USER_ERROR[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] = "src do not allowed migrate";
    ORACLE_ERRNO[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] = 600;
    ORACLE_STR_ERROR[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] =
        "ORA-00600: internal error code, arguments: -4683, src do not allowed migrate";
    ORACLE_STR_USER_ERROR[-OB_SRC_DO_NOT_ALLOWED_MIGRATE] =
        "ORA-00600: internal error code, arguments: -4683, src do not allowed migrate";
    ERROR_NAME[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] = "OB_TOO_MANY_TENANT_PARTITIONS_ERROR";
    MYSQL_ERRNO[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] = -1;
    SQLSTATE[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] = "HY000";
    STR_ERROR[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] = "Too many partitions were defined for this tenant";
    STR_USER_ERROR[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] = "Too many partitions were defined for this tenant";
    ORACLE_ERRNO[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] =
        "ORA-00600: internal error code, arguments: -4684, Too many partitions were defined for this tenant";
    ORACLE_STR_USER_ERROR[-OB_TOO_MANY_TENANT_PARTITIONS_ERROR] =
        "ORA-00600: internal error code, arguments: -4684, Too many partitions were defined for this tenant";
    ERROR_NAME[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] = "OB_ACTIVE_MEMTBALE_NOT_EXSIT";
    MYSQL_ERRNO[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] = -1;
    SQLSTATE[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] = "HY000";
    STR_ERROR[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] = "active memtable not exist";
    STR_USER_ERROR[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] = "active memtable not exist";
    ORACLE_ERRNO[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] = 600;
    ORACLE_STR_ERROR[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] =
        "ORA-00600: internal error code, arguments: -4685, active memtable not exist";
    ORACLE_STR_USER_ERROR[-OB_ACTIVE_MEMTBALE_NOT_EXSIT] =
        "ORA-00600: internal error code, arguments: -4685, active memtable not exist";
    ERROR_NAME[-OB_USE_DUP_FOLLOW_AFTER_DML] = "OB_USE_DUP_FOLLOW_AFTER_DML";
    MYSQL_ERRNO[-OB_USE_DUP_FOLLOW_AFTER_DML] = -1;
    SQLSTATE[-OB_USE_DUP_FOLLOW_AFTER_DML] = "HY000";
    STR_ERROR[-OB_USE_DUP_FOLLOW_AFTER_DML] = "Should use leader replica for duplicate table after DML operator";
    STR_USER_ERROR[-OB_USE_DUP_FOLLOW_AFTER_DML] = "Should use leader replica for duplicate table after DML operator";
    ORACLE_ERRNO[-OB_USE_DUP_FOLLOW_AFTER_DML] = 600;
    ORACLE_STR_ERROR[-OB_USE_DUP_FOLLOW_AFTER_DML] = "ORA-00600: internal error code, arguments: -4686, Should use "
                                                     "leader replica for duplicate table after DML operator";
    ORACLE_STR_USER_ERROR[-OB_USE_DUP_FOLLOW_AFTER_DML] = "ORA-00600: internal error code, arguments: -4686, Should "
                                                          "use leader replica for duplicate table after DML operator";
    ERROR_NAME[-OB_NO_DISK_NEED_REBUILD] = "OB_NO_DISK_NEED_REBUILD";
    MYSQL_ERRNO[-OB_NO_DISK_NEED_REBUILD] = -1;
    SQLSTATE[-OB_NO_DISK_NEED_REBUILD] = "HY000";
    STR_ERROR[-OB_NO_DISK_NEED_REBUILD] = "no disk need rebuild";
    STR_USER_ERROR[-OB_NO_DISK_NEED_REBUILD] = "no disk need rebuild";
    ORACLE_ERRNO[-OB_NO_DISK_NEED_REBUILD] = 600;
    ORACLE_STR_ERROR[-OB_NO_DISK_NEED_REBUILD] =
        "ORA-00600: internal error code, arguments: -4687, no disk need rebuild";
    ORACLE_STR_USER_ERROR[-OB_NO_DISK_NEED_REBUILD] =
        "ORA-00600: internal error code, arguments: -4687, no disk need rebuild";
    ERROR_NAME[-OB_STANDBY_WEAK_READ_ONLY] = "OB_STANDBY_WEAK_READ_ONLY";
    MYSQL_ERRNO[-OB_STANDBY_WEAK_READ_ONLY] = -1;
    SQLSTATE[-OB_STANDBY_WEAK_READ_ONLY] = "HY000";
    STR_ERROR[-OB_STANDBY_WEAK_READ_ONLY] = "standby cluster support weak read only";
    STR_USER_ERROR[-OB_STANDBY_WEAK_READ_ONLY] = "standby cluster support weak read only";
    ORACLE_ERRNO[-OB_STANDBY_WEAK_READ_ONLY] = 16003;
    ORACLE_STR_ERROR[-OB_STANDBY_WEAK_READ_ONLY] = "ORA-16003: standby cluster support weak read only";
    ORACLE_STR_USER_ERROR[-OB_STANDBY_WEAK_READ_ONLY] = "ORA-16003: standby cluster support weak read only";
    ERROR_NAME[-OB_INVALD_WEB_SERVICE_CONTENT] = "OB_INVALD_WEB_SERVICE_CONTENT";
    MYSQL_ERRNO[-OB_INVALD_WEB_SERVICE_CONTENT] = -1;
    SQLSTATE[-OB_INVALD_WEB_SERVICE_CONTENT] = "HY000";
    STR_ERROR[-OB_INVALD_WEB_SERVICE_CONTENT] = "web service content not valid";
    STR_USER_ERROR[-OB_INVALD_WEB_SERVICE_CONTENT] = "web service content not valid";
    ORACLE_ERRNO[-OB_INVALD_WEB_SERVICE_CONTENT] = 600;
    ORACLE_STR_ERROR[-OB_INVALD_WEB_SERVICE_CONTENT] =
        "ORA-00600: internal error code, arguments: -4689, web service content not valid";
    ORACLE_STR_USER_ERROR[-OB_INVALD_WEB_SERVICE_CONTENT] =
        "ORA-00600: internal error code, arguments: -4689, web service content not valid";
    ERROR_NAME[-OB_PRIMARY_CLUSTER_EXIST] = "OB_PRIMARY_CLUSTER_EXIST";
    MYSQL_ERRNO[-OB_PRIMARY_CLUSTER_EXIST] = -1;
    SQLSTATE[-OB_PRIMARY_CLUSTER_EXIST] = "HY000";
    STR_ERROR[-OB_PRIMARY_CLUSTER_EXIST] = "other primary cluster already exist, can not start as primary";
    STR_USER_ERROR[-OB_PRIMARY_CLUSTER_EXIST] = "other primary cluster already exist, can not start as primary";
    ORACLE_ERRNO[-OB_PRIMARY_CLUSTER_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_PRIMARY_CLUSTER_EXIST] = "ORA-00600: internal error code, arguments: -4690, other primary "
                                                  "cluster already exist, can not start as primary";
    ORACLE_STR_USER_ERROR[-OB_PRIMARY_CLUSTER_EXIST] = "ORA-00600: internal error code, arguments: -4690, other "
                                                       "primary cluster already exist, can not start as primary";
    ERROR_NAME[-OB_ARRAY_BINDING_SWITCH_ITERATOR] = "OB_ARRAY_BINDING_SWITCH_ITERATOR";
    MYSQL_ERRNO[-OB_ARRAY_BINDING_SWITCH_ITERATOR] = -1;
    SQLSTATE[-OB_ARRAY_BINDING_SWITCH_ITERATOR] = "HY000";
    STR_ERROR[-OB_ARRAY_BINDING_SWITCH_ITERATOR] = "array binding needs to switch iterator";
    STR_USER_ERROR[-OB_ARRAY_BINDING_SWITCH_ITERATOR] = "array binding needs to switch iterator";
    ORACLE_ERRNO[-OB_ARRAY_BINDING_SWITCH_ITERATOR] = 600;
    ORACLE_STR_ERROR[-OB_ARRAY_BINDING_SWITCH_ITERATOR] =
        "ORA-00600: internal error code, arguments: -4691, array binding needs to switch iterator";
    ORACLE_STR_USER_ERROR[-OB_ARRAY_BINDING_SWITCH_ITERATOR] =
        "ORA-00600: internal error code, arguments: -4691, array binding needs to switch iterator";
    ERROR_NAME[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] = "OB_ERR_STANDBY_CLUSTER_NOT_EMPTY";
    MYSQL_ERRNO[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] = -1;
    SQLSTATE[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] = "HY000";
    STR_ERROR[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] = "standby cluster not empty";
    STR_USER_ERROR[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] = "standby cluster not empty";
    ORACLE_ERRNO[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] =
        "ORA-00600: internal error code, arguments: -4692, standby cluster not empty";
    ORACLE_STR_USER_ERROR[-OB_ERR_STANDBY_CLUSTER_NOT_EMPTY] =
        "ORA-00600: internal error code, arguments: -4692, standby cluster not empty";
    ERROR_NAME[-OB_NOT_PRIMARY_CLUSTER] = "OB_NOT_PRIMARY_CLUSTER";
    MYSQL_ERRNO[-OB_NOT_PRIMARY_CLUSTER] = -1;
    SQLSTATE[-OB_NOT_PRIMARY_CLUSTER] = "HY000";
    STR_ERROR[-OB_NOT_PRIMARY_CLUSTER] = "not primary cluster";
    STR_USER_ERROR[-OB_NOT_PRIMARY_CLUSTER] = "not primary cluster";
    ORACLE_ERRNO[-OB_NOT_PRIMARY_CLUSTER] = 600;
    ORACLE_STR_ERROR[-OB_NOT_PRIMARY_CLUSTER] = "ORA-00600: internal error code, arguments: -4693, not primary cluster";
    ORACLE_STR_USER_ERROR[-OB_NOT_PRIMARY_CLUSTER] =
        "ORA-00600: internal error code, arguments: -4693, not primary cluster";
    ERROR_NAME[-OB_ERR_CHECK_DROP_COLUMN_FAILED] = "OB_ERR_CHECK_DROP_COLUMN_FAILED";
    MYSQL_ERRNO[-OB_ERR_CHECK_DROP_COLUMN_FAILED] = -1;
    SQLSTATE[-OB_ERR_CHECK_DROP_COLUMN_FAILED] = "HY000";
    STR_ERROR[-OB_ERR_CHECK_DROP_COLUMN_FAILED] = "check drop column failed";
    STR_USER_ERROR[-OB_ERR_CHECK_DROP_COLUMN_FAILED] = "check drop column failed";
    ORACLE_ERRNO[-OB_ERR_CHECK_DROP_COLUMN_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CHECK_DROP_COLUMN_FAILED] =
        "ORA-00600: internal error code, arguments: -4694, check drop column failed";
    ORACLE_STR_USER_ERROR[-OB_ERR_CHECK_DROP_COLUMN_FAILED] =
        "ORA-00600: internal error code, arguments: -4694, check drop column failed";
    ERROR_NAME[-OB_NOT_STANDBY_CLUSTER] = "OB_NOT_STANDBY_CLUSTER";
    MYSQL_ERRNO[-OB_NOT_STANDBY_CLUSTER] = -1;
    SQLSTATE[-OB_NOT_STANDBY_CLUSTER] = "HY000";
    STR_ERROR[-OB_NOT_STANDBY_CLUSTER] = "not standby cluster";
    STR_USER_ERROR[-OB_NOT_STANDBY_CLUSTER] = "not standby cluster";
    ORACLE_ERRNO[-OB_NOT_STANDBY_CLUSTER] = 600;
    ORACLE_STR_ERROR[-OB_NOT_STANDBY_CLUSTER] = "ORA-00600: internal error code, arguments: -4695, not standby cluster";
    ORACLE_STR_USER_ERROR[-OB_NOT_STANDBY_CLUSTER] =
        "ORA-00600: internal error code, arguments: -4695, not standby cluster";
    ERROR_NAME[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] = "OB_CLUSTER_VERSION_NOT_COMPATIBLE";
    MYSQL_ERRNO[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] = -1;
    SQLSTATE[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] = "HY000";
    STR_ERROR[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] = "cluster version not compatible";
    STR_USER_ERROR[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] = "cluster version not compatible";
    ORACLE_ERRNO[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] = 600;
    ORACLE_STR_ERROR[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] =
        "ORA-00600: internal error code, arguments: -4696, cluster version not compatible";
    ORACLE_STR_USER_ERROR[-OB_CLUSTER_VERSION_NOT_COMPATIBLE] =
        "ORA-00600: internal error code, arguments: -4696, cluster version not compatible";
    ERROR_NAME[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] = "OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT";
    MYSQL_ERRNO[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] = -1;
    SQLSTATE[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] = "HY000";
    STR_ERROR[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] = "wait trans table merge finish timeout";
    STR_USER_ERROR[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] = "wait trans table merge finish timeout";
    ORACLE_ERRNO[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4697, wait trans table merge finish timeout";
    ORACLE_STR_USER_ERROR[-OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -4697, wait trans table merge finish timeout";
    ERROR_NAME[-OB_SKIP_RENEW_LOCATION_BY_RPC] = "OB_SKIP_RENEW_LOCATION_BY_RPC";
    MYSQL_ERRNO[-OB_SKIP_RENEW_LOCATION_BY_RPC] = -1;
    SQLSTATE[-OB_SKIP_RENEW_LOCATION_BY_RPC] = "HY000";
    STR_ERROR[-OB_SKIP_RENEW_LOCATION_BY_RPC] = "skip renew location by rpc";
    STR_USER_ERROR[-OB_SKIP_RENEW_LOCATION_BY_RPC] = "skip renew location by rpc";
    ORACLE_ERRNO[-OB_SKIP_RENEW_LOCATION_BY_RPC] = 600;
    ORACLE_STR_ERROR[-OB_SKIP_RENEW_LOCATION_BY_RPC] =
        "ORA-00600: internal error code, arguments: -4698, skip renew location by rpc";
    ORACLE_STR_USER_ERROR[-OB_SKIP_RENEW_LOCATION_BY_RPC] =
        "ORA-00600: internal error code, arguments: -4698, skip renew location by rpc";
    ERROR_NAME[-OB_RENEW_LOCATION_BY_RPC_FAILED] = "OB_RENEW_LOCATION_BY_RPC_FAILED";
    MYSQL_ERRNO[-OB_RENEW_LOCATION_BY_RPC_FAILED] = -1;
    SQLSTATE[-OB_RENEW_LOCATION_BY_RPC_FAILED] = "HY000";
    STR_ERROR[-OB_RENEW_LOCATION_BY_RPC_FAILED] = "renew location by rpc failed";
    STR_USER_ERROR[-OB_RENEW_LOCATION_BY_RPC_FAILED] = "renew location by rpc failed";
    ORACLE_ERRNO[-OB_RENEW_LOCATION_BY_RPC_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_RENEW_LOCATION_BY_RPC_FAILED] =
        "ORA-00600: internal error code, arguments: -4699, renew location by rpc failed";
    ORACLE_STR_USER_ERROR[-OB_RENEW_LOCATION_BY_RPC_FAILED] =
        "ORA-00600: internal error code, arguments: -4699, renew location by rpc failed";
    ERROR_NAME[-OB_CLUSTER_ID_NO_MATCH] = "OB_CLUSTER_ID_NO_MATCH";
    MYSQL_ERRNO[-OB_CLUSTER_ID_NO_MATCH] = -1;
    SQLSTATE[-OB_CLUSTER_ID_NO_MATCH] = "HY000";
    STR_ERROR[-OB_CLUSTER_ID_NO_MATCH] = "cluster id does not match";
    STR_USER_ERROR[-OB_CLUSTER_ID_NO_MATCH] = "cluster id does not match";
    ORACLE_ERRNO[-OB_CLUSTER_ID_NO_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_CLUSTER_ID_NO_MATCH] =
        "ORA-00600: internal error code, arguments: -4700, cluster id does not match";
    ORACLE_STR_USER_ERROR[-OB_CLUSTER_ID_NO_MATCH] =
        "ORA-00600: internal error code, arguments: -4700, cluster id does not match";
    ERROR_NAME[-OB_ERR_PARAM_INVALID] = "OB_ERR_PARAM_INVALID";
    MYSQL_ERRNO[-OB_ERR_PARAM_INVALID] = -1;
    SQLSTATE[-OB_ERR_PARAM_INVALID] = "HY000";
    STR_ERROR[-OB_ERR_PARAM_INVALID] = "parameter cannot be modified because specified value is invalid";
    STR_USER_ERROR[-OB_ERR_PARAM_INVALID] = "parameter cannot be modified because specified value is invalid";
    ORACLE_ERRNO[-OB_ERR_PARAM_INVALID] = 2097;
    ORACLE_STR_ERROR[-OB_ERR_PARAM_INVALID] =
        "ORA-02097: parameter cannot be modified because specified value is invalid";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARAM_INVALID] =
        "ORA-02097: parameter cannot be modified because specified value is invalid";
    ERROR_NAME[-OB_ERR_RES_OBJ_ALREADY_EXIST] = "OB_ERR_RES_OBJ_ALREADY_EXIST";
    MYSQL_ERRNO[-OB_ERR_RES_OBJ_ALREADY_EXIST] = -1;
    SQLSTATE[-OB_ERR_RES_OBJ_ALREADY_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_RES_OBJ_ALREADY_EXIST] = "resource object already exists";
    STR_USER_ERROR[-OB_ERR_RES_OBJ_ALREADY_EXIST] = "%s %.*s already exists";
    ORACLE_ERRNO[-OB_ERR_RES_OBJ_ALREADY_EXIST] = 29357;
    ORACLE_STR_ERROR[-OB_ERR_RES_OBJ_ALREADY_EXIST] = "ORA-29357: resource object already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_RES_OBJ_ALREADY_EXIST] = "ORA-29357: %s %.*s already exists";
    ERROR_NAME[-OB_ERR_RES_PLAN_NOT_EXIST] = "OB_ERR_RES_PLAN_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_RES_PLAN_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_RES_PLAN_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_RES_PLAN_NOT_EXIST] = "resource plan does not exist";
    STR_USER_ERROR[-OB_ERR_RES_PLAN_NOT_EXIST] = "resource plan %.*s does not exist";
    ORACLE_ERRNO[-OB_ERR_RES_PLAN_NOT_EXIST] = 29358;
    ORACLE_STR_ERROR[-OB_ERR_RES_PLAN_NOT_EXIST] = "ORA-29358: resource plan does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_RES_PLAN_NOT_EXIST] = "ORA-29358: resource plan %.*s does not exist";
    ERROR_NAME[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] = "OB_ERR_PERCENTAGE_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] = "value is outside valid range of 0 to 100";
    STR_USER_ERROR[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] = "value %ld for %s is outside valid range of 0 to 100";
    ORACLE_ERRNO[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] = 29361;
    ORACLE_STR_ERROR[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] = "ORA-29361: value is outside valid range of 0 to 100";
    ORACLE_STR_USER_ERROR[-OB_ERR_PERCENTAGE_OUT_OF_RANGE] =
        "ORA-29361: value %ld for %s is outside valid range of 0 to 100";
    ERROR_NAME[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = "OB_ERR_PLAN_DIRECTIVE_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = "plan directive does not exist";
    STR_USER_ERROR[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = "plan directive %.*s, %.*s does not exist";
    ORACLE_ERRNO[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = 29362;
    ORACLE_STR_ERROR[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = "ORA-29362: plan directive does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_PLAN_DIRECTIVE_NOT_EXIST] = "ORA-29362: plan directive %.*s, %.*s does not exist";
    ERROR_NAME[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = "OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST";
    MYSQL_ERRNO[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = -1;
    SQLSTATE[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = "plan directive already exists";
    STR_USER_ERROR[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = "plan directive %.*s, %.*s already exists";
    ORACLE_ERRNO[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = 29364;
    ORACLE_STR_ERROR[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = "ORA-29364: plan directive already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST] = "ORA-29364: plan directive %.*s, %.*s already exists";
    ERROR_NAME[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = "OB_ERR_INVALID_PLAN_DIRECTIVE_NAME";
    MYSQL_ERRNO[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = -1;
    SQLSTATE[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = "plan directive name not supported.";
    STR_USER_ERROR[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = "plan directive name '%.*s' not supported.";
    ORACLE_ERRNO[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = 29366;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = "ORA-29366: plan directive name not supported.";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_PLAN_DIRECTIVE_NAME] = "ORA-29366: plan directive name '%.*s' not supported.";
    ERROR_NAME[-OB_FAILOVER_NOT_ALLOW] = "OB_FAILOVER_NOT_ALLOW";
    MYSQL_ERRNO[-OB_FAILOVER_NOT_ALLOW] = -1;
    SQLSTATE[-OB_FAILOVER_NOT_ALLOW] = "HY000";
    STR_ERROR[-OB_FAILOVER_NOT_ALLOW] = "Failover is not allowed";
    STR_USER_ERROR[-OB_FAILOVER_NOT_ALLOW] = "%s";
    ORACLE_ERRNO[-OB_FAILOVER_NOT_ALLOW] = 600;
    ORACLE_STR_ERROR[-OB_FAILOVER_NOT_ALLOW] =
        "ORA-00600: internal error code, arguments: -4708, Failover is not allowed";
    ORACLE_STR_USER_ERROR[-OB_FAILOVER_NOT_ALLOW] = "ORA-00600: internal error code, arguments: -4708, %s";
    ERROR_NAME[-OB_ADD_CLUSTER_NOT_ALLOWED] = "OB_ADD_CLUSTER_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ADD_CLUSTER_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ADD_CLUSTER_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ADD_CLUSTER_NOT_ALLOWED] = "Add cluster not allowed.";
    STR_USER_ERROR[-OB_ADD_CLUSTER_NOT_ALLOWED] = "Add cluster not allowed. Actions: %s";
    ORACLE_ERRNO[-OB_ADD_CLUSTER_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_ADD_CLUSTER_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -4709, Add cluster not allowed.";
    ORACLE_STR_USER_ERROR[-OB_ADD_CLUSTER_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -4709, Add cluster not allowed. Actions: %s";
    ERROR_NAME[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = "OB_ERR_CONSUMER_GROUP_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = "consumer group does not exist";
    STR_USER_ERROR[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = "consumer group %.*s does not exist";
    ORACLE_ERRNO[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = 29368;
    ORACLE_STR_ERROR[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = "ORA-29368: consumer group does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONSUMER_GROUP_NOT_EXIST] = "ORA-29368: consumer group %.*s does not exist";
    ERROR_NAME[-OB_CLUSTER_NOT_ACCESSIBLE] = "OB_CLUSTER_NOT_ACCESSIBLE";
    MYSQL_ERRNO[-OB_CLUSTER_NOT_ACCESSIBLE] = -1;
    SQLSTATE[-OB_CLUSTER_NOT_ACCESSIBLE] = "HY000";
    STR_ERROR[-OB_CLUSTER_NOT_ACCESSIBLE] = "cluster is not accessible";
    STR_USER_ERROR[-OB_CLUSTER_NOT_ACCESSIBLE] = "cluster is not accessible, cluster_id: %ld";
    ORACLE_ERRNO[-OB_CLUSTER_NOT_ACCESSIBLE] = 600;
    ORACLE_STR_ERROR[-OB_CLUSTER_NOT_ACCESSIBLE] =
        "ORA-00600: internal error code, arguments: -4711, cluster is not accessible";
    ORACLE_STR_USER_ERROR[-OB_CLUSTER_NOT_ACCESSIBLE] =
        "ORA-00600: internal error code, arguments: -4711, cluster is not accessible, cluster_id: %ld";
    ERROR_NAME[-OB_TENANT_RESOURCE_UNIT_EXIST] = "OB_TENANT_RESOURCE_UNIT_EXIST";
    MYSQL_ERRNO[-OB_TENANT_RESOURCE_UNIT_EXIST] = -1;
    SQLSTATE[-OB_TENANT_RESOURCE_UNIT_EXIST] = "HY000";
    STR_ERROR[-OB_TENANT_RESOURCE_UNIT_EXIST] = "tenant already has resource unit configured";
    STR_USER_ERROR[-OB_TENANT_RESOURCE_UNIT_EXIST] =
        "tenant already has resource unit configured, tenant_id: %ld, observer: \'%s\'";
    ORACLE_ERRNO[-OB_TENANT_RESOURCE_UNIT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TENANT_RESOURCE_UNIT_EXIST] =
        "ORA-00600: internal error code, arguments: -4712, tenant already has resource unit configured";
    ORACLE_STR_USER_ERROR[-OB_TENANT_RESOURCE_UNIT_EXIST] =
        "ORA-00600: internal error code, arguments: -4712, tenant already has resource unit configured, tenant_id: "
        "%ld, observer: \'%s\'";
    ERROR_NAME[-OB_ERR_PARSER_INIT] = "OB_ERR_PARSER_INIT";
    MYSQL_ERRNO[-OB_ERR_PARSER_INIT] = ER_PARSE_ERROR;
    SQLSTATE[-OB_ERR_PARSER_INIT] = "0B000";
    STR_ERROR[-OB_ERR_PARSER_INIT] = "Failed to init SQL parser";
    STR_USER_ERROR[-OB_ERR_PARSER_INIT] = "Failed to init SQL parser";
    ORACLE_ERRNO[-OB_ERR_PARSER_INIT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARSER_INIT] =
        "ORA-00600: internal error code, arguments: -5000, Failed to init SQL parser";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARSER_INIT] =
        "ORA-00600: internal error code, arguments: -5000, Failed to init SQL parser";
    ERROR_NAME[-OB_ERR_PARSE_SQL] = "OB_ERR_PARSE_SQL";
    MYSQL_ERRNO[-OB_ERR_PARSE_SQL] = ER_PARSE_ERROR;
    SQLSTATE[-OB_ERR_PARSE_SQL] = "42000";
    STR_ERROR[-OB_ERR_PARSE_SQL] = "Parse error";
    STR_USER_ERROR[-OB_ERR_PARSE_SQL] = "%s near \'%.*s\' at line %d";
    ORACLE_ERRNO[-OB_ERR_PARSE_SQL] = 900;
    ORACLE_STR_ERROR[-OB_ERR_PARSE_SQL] = "ORA-00900: invalid SQL statement";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARSE_SQL] = "ORA-00900: %s near \'%.*s\' at line %d";
    ERROR_NAME[-OB_ERR_RESOLVE_SQL] = "OB_ERR_RESOLVE_SQL";
    MYSQL_ERRNO[-OB_ERR_RESOLVE_SQL] = -1;
    SQLSTATE[-OB_ERR_RESOLVE_SQL] = "HY000";
    STR_ERROR[-OB_ERR_RESOLVE_SQL] = "Resolve error";
    STR_USER_ERROR[-OB_ERR_RESOLVE_SQL] = "Resolve error";
    ORACLE_ERRNO[-OB_ERR_RESOLVE_SQL] = 600;
    ORACLE_STR_ERROR[-OB_ERR_RESOLVE_SQL] = "ORA-00600: internal error code, arguments: -5002, Resolve error";
    ORACLE_STR_USER_ERROR[-OB_ERR_RESOLVE_SQL] = "ORA-00600: internal error code, arguments: -5002, Resolve error";
    ERROR_NAME[-OB_ERR_GEN_PLAN] = "OB_ERR_GEN_PLAN";
    MYSQL_ERRNO[-OB_ERR_GEN_PLAN] = -1;
    SQLSTATE[-OB_ERR_GEN_PLAN] = "HY000";
    STR_ERROR[-OB_ERR_GEN_PLAN] = "Generate plan error";
    STR_USER_ERROR[-OB_ERR_GEN_PLAN] = "Generate plan error";
    ORACLE_ERRNO[-OB_ERR_GEN_PLAN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_GEN_PLAN] = "ORA-00600: internal error code, arguments: -5003, Generate plan error";
    ORACLE_STR_USER_ERROR[-OB_ERR_GEN_PLAN] = "ORA-00600: internal error code, arguments: -5003, Generate plan error";
    ERROR_NAME[-OB_ERR_PARSER_SYNTAX] = "OB_ERR_PARSER_SYNTAX";
    MYSQL_ERRNO[-OB_ERR_PARSER_SYNTAX] = ER_SYNTAX_ERROR;
    SQLSTATE[-OB_ERR_PARSER_SYNTAX] = "42000";
    STR_ERROR[-OB_ERR_PARSER_SYNTAX] = "You have an error in your SQL syntax; check the manual that corresponds to "
                                       "your OceanBase version for the right syntax to use";
    STR_USER_ERROR[-OB_ERR_PARSER_SYNTAX] = "You have an error in your SQL syntax; check the manual that corresponds "
                                            "to your OceanBase version for the right syntax to use";
    ORACLE_ERRNO[-OB_ERR_PARSER_SYNTAX] = 900;
    ORACLE_STR_ERROR[-OB_ERR_PARSER_SYNTAX] = "ORA-00900: You have an error in your SQL syntax; check the manual that "
                                              "corresponds to your OceanBase version for the right syntax to use";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARSER_SYNTAX] =
        "ORA-00900: You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version "
        "for the right syntax to use";
    ERROR_NAME[-OB_ERR_COLUMN_SIZE] = "OB_ERR_COLUMN_SIZE";
    MYSQL_ERRNO[-OB_ERR_COLUMN_SIZE] = ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT;
    SQLSTATE[-OB_ERR_COLUMN_SIZE] = "21000";
    STR_ERROR[-OB_ERR_COLUMN_SIZE] = "The used SELECT statements have a different number of columns";
    STR_USER_ERROR[-OB_ERR_COLUMN_SIZE] = "The used SELECT statements have a different number of columns";
    ORACLE_ERRNO[-OB_ERR_COLUMN_SIZE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_SIZE] = "ORA-00600: internal error code, arguments: -5007, The used SELECT "
                                            "statements have a different number of columns";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_SIZE] = "ORA-00600: internal error code, arguments: -5007, The used SELECT "
                                                 "statements have a different number of columns";
    ERROR_NAME[-OB_ERR_COLUMN_DUPLICATE] = "OB_ERR_COLUMN_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_COLUMN_DUPLICATE] = ER_DUP_FIELDNAME;
    SQLSTATE[-OB_ERR_COLUMN_DUPLICATE] = "42S21";
    STR_ERROR[-OB_ERR_COLUMN_DUPLICATE] = "Duplicate column name";
    STR_USER_ERROR[-OB_ERR_COLUMN_DUPLICATE] = "Duplicate column name '%.*s'";
    ORACLE_ERRNO[-OB_ERR_COLUMN_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5008, Duplicate column name";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5008, Duplicate column name '%.*s'";
    ERROR_NAME[-OB_ERR_OPERATOR_UNKNOWN] = "OB_ERR_OPERATOR_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_OPERATOR_UNKNOWN] = -1;
    SQLSTATE[-OB_ERR_OPERATOR_UNKNOWN] = "21000";
    STR_ERROR[-OB_ERR_OPERATOR_UNKNOWN] = "Unknown operator";
    STR_USER_ERROR[-OB_ERR_OPERATOR_UNKNOWN] = "Unknown operator";
    ORACLE_ERRNO[-OB_ERR_OPERATOR_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OPERATOR_UNKNOWN] = "ORA-00600: internal error code, arguments: -5010, Unknown operator";
    ORACLE_STR_USER_ERROR[-OB_ERR_OPERATOR_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5010, Unknown operator";
    ERROR_NAME[-OB_ERR_STAR_DUPLICATE] = "OB_ERR_STAR_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_STAR_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_STAR_DUPLICATE] = "42000";
    STR_ERROR[-OB_ERR_STAR_DUPLICATE] = "Duplicated star";
    STR_USER_ERROR[-OB_ERR_STAR_DUPLICATE] = "Duplicated star";
    ORACLE_ERRNO[-OB_ERR_STAR_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_STAR_DUPLICATE] = "ORA-00600: internal error code, arguments: -5011, Duplicated star";
    ORACLE_STR_USER_ERROR[-OB_ERR_STAR_DUPLICATE] = "ORA-00600: internal error code, arguments: -5011, Duplicated star";
    ERROR_NAME[-OB_ERR_ILLEGAL_ID] = "OB_ERR_ILLEGAL_ID";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_ID] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_ID] = "HY000";
    STR_ERROR[-OB_ERR_ILLEGAL_ID] = "Illegal ID";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_ID] = "%s";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_ID] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_ID] = "ORA-00600: internal error code, arguments: -5012, Illegal ID";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_ID] = "ORA-00600: internal error code, arguments: -5012, %s";
    ERROR_NAME[-OB_ERR_ILLEGAL_VALUE] = "OB_ERR_ILLEGAL_VALUE";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_VALUE] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_ILLEGAL_VALUE] = "Illegal value";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_VALUE] = "Illegal value";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_VALUE] = "ORA-00600: internal error code, arguments: -5014, Illegal value";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_VALUE] = "ORA-00600: internal error code, arguments: -5014, Illegal value";
    ERROR_NAME[-OB_ERR_COLUMN_AMBIGUOUS] = "OB_ERR_COLUMN_AMBIGUOUS";
    MYSQL_ERRNO[-OB_ERR_COLUMN_AMBIGUOUS] = ER_AMBIGUOUS_FIELD_TERM;
    SQLSTATE[-OB_ERR_COLUMN_AMBIGUOUS] = "42000";
    STR_ERROR[-OB_ERR_COLUMN_AMBIGUOUS] = "Ambiguous column";
    STR_USER_ERROR[-OB_ERR_COLUMN_AMBIGUOUS] = "Ambiguous column";
    ORACLE_ERRNO[-OB_ERR_COLUMN_AMBIGUOUS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_AMBIGUOUS] = "ORA-00600: internal error code, arguments: -5015, Ambiguous column";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_AMBIGUOUS] =
        "ORA-00600: internal error code, arguments: -5015, Ambiguous column";
    ERROR_NAME[-OB_ERR_LOGICAL_PLAN_FAILD] = "OB_ERR_LOGICAL_PLAN_FAILD";
    MYSQL_ERRNO[-OB_ERR_LOGICAL_PLAN_FAILD] = -1;
    SQLSTATE[-OB_ERR_LOGICAL_PLAN_FAILD] = "HY000";
    STR_ERROR[-OB_ERR_LOGICAL_PLAN_FAILD] = "Generate logical plan error";
    STR_USER_ERROR[-OB_ERR_LOGICAL_PLAN_FAILD] = "Generate logical plan error";
    ORACLE_ERRNO[-OB_ERR_LOGICAL_PLAN_FAILD] = 600;
    ORACLE_STR_ERROR[-OB_ERR_LOGICAL_PLAN_FAILD] =
        "ORA-00600: internal error code, arguments: -5016, Generate logical plan error";
    ORACLE_STR_USER_ERROR[-OB_ERR_LOGICAL_PLAN_FAILD] =
        "ORA-00600: internal error code, arguments: -5016, Generate logical plan error";
    ERROR_NAME[-OB_ERR_SCHEMA_UNSET] = "OB_ERR_SCHEMA_UNSET";
    MYSQL_ERRNO[-OB_ERR_SCHEMA_UNSET] = -1;
    SQLSTATE[-OB_ERR_SCHEMA_UNSET] = "HY000";
    STR_ERROR[-OB_ERR_SCHEMA_UNSET] = "Schema not set";
    STR_USER_ERROR[-OB_ERR_SCHEMA_UNSET] = "Schema not set";
    ORACLE_ERRNO[-OB_ERR_SCHEMA_UNSET] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SCHEMA_UNSET] = "ORA-00600: internal error code, arguments: -5017, Schema not set";
    ORACLE_STR_USER_ERROR[-OB_ERR_SCHEMA_UNSET] = "ORA-00600: internal error code, arguments: -5017, Schema not set";
    ERROR_NAME[-OB_ERR_ILLEGAL_NAME] = "OB_ERR_ILLEGAL_NAME";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_NAME] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_NAME] = "42000";
    STR_ERROR[-OB_ERR_ILLEGAL_NAME] = "Illegal name";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_NAME] = "Illegal name";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_NAME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_NAME] = "ORA-00600: internal error code, arguments: -5018, Illegal name";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_NAME] = "ORA-00600: internal error code, arguments: -5018, Illegal name";
    ERROR_NAME[-OB_TABLE_NOT_EXIST] = "OB_TABLE_NOT_EXIST";
    MYSQL_ERRNO[-OB_TABLE_NOT_EXIST] = ER_NO_SUCH_TABLE;
    SQLSTATE[-OB_TABLE_NOT_EXIST] = "42S02";
    STR_ERROR[-OB_TABLE_NOT_EXIST] = "Table doesn\'t exist";
    STR_USER_ERROR[-OB_TABLE_NOT_EXIST] = "Table \'%s.%s\' doesn\'t exist";
    ORACLE_ERRNO[-OB_TABLE_NOT_EXIST] = 942;
    ORACLE_STR_ERROR[-OB_TABLE_NOT_EXIST] = "ORA-00942: table or view does not exist";
    ORACLE_STR_USER_ERROR[-OB_TABLE_NOT_EXIST] = "ORA-00942: table or view \'%s.%s\' does not exist";
    ERROR_NAME[-OB_ERR_TABLE_EXIST] = "OB_ERR_TABLE_EXIST";
    MYSQL_ERRNO[-OB_ERR_TABLE_EXIST] = ER_TABLE_EXISTS_ERROR;
    SQLSTATE[-OB_ERR_TABLE_EXIST] = "42S01";
    STR_ERROR[-OB_ERR_TABLE_EXIST] = "Table already exists";
    STR_USER_ERROR[-OB_ERR_TABLE_EXIST] = "Table '%.*s' already exists";
    ORACLE_ERRNO[-OB_ERR_TABLE_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TABLE_EXIST] = "ORA-00600: internal error code, arguments: -5020, Table already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_TABLE_EXIST] =
        "ORA-00600: internal error code, arguments: -5020, Table '%.*s' already exists";
    ERROR_NAME[-OB_ERR_EXPR_UNKNOWN] = "OB_ERR_EXPR_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_EXPR_UNKNOWN] = -1;
    SQLSTATE[-OB_ERR_EXPR_UNKNOWN] = "42000";
    STR_ERROR[-OB_ERR_EXPR_UNKNOWN] = "Unknown expression";
    STR_USER_ERROR[-OB_ERR_EXPR_UNKNOWN] = "Unknown expression";
    ORACLE_ERRNO[-OB_ERR_EXPR_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_EXPR_UNKNOWN] = "ORA-00600: internal error code, arguments: -5022, Unknown expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXPR_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5022, Unknown expression";
    ERROR_NAME[-OB_ERR_ILLEGAL_TYPE] = "OB_ERR_ILLEGAL_TYPE";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_TYPE] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_TYPE] = "S1004";
    STR_ERROR[-OB_ERR_ILLEGAL_TYPE] = "Illegal type";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_TYPE] =
        "unsupport MySQL type %d. Maybe you should use java.sql.Timestamp instead of java.util.Date.";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_TYPE] = "ORA-00600: internal error code, arguments: -5023, Illegal type";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_TYPE] =
        "ORA-00600: internal error code, arguments: -5023, unsupport MySQL type %d. Maybe you should use "
        "java.sql.Timestamp instead of java.util.Date.";
    ERROR_NAME[-OB_ERR_PRIMARY_KEY_DUPLICATE] = "OB_ERR_PRIMARY_KEY_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_PRIMARY_KEY_DUPLICATE] = ER_DUP_ENTRY;
    SQLSTATE[-OB_ERR_PRIMARY_KEY_DUPLICATE] = "23000";
    STR_ERROR[-OB_ERR_PRIMARY_KEY_DUPLICATE] = "Duplicated primary key";
    STR_USER_ERROR[-OB_ERR_PRIMARY_KEY_DUPLICATE] = "Duplicate entry \'%s\' for key \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_PRIMARY_KEY_DUPLICATE] = 1;
    ORACLE_STR_ERROR[-OB_ERR_PRIMARY_KEY_DUPLICATE] = "ORA-00001: unique constraint violated";
    ORACLE_STR_USER_ERROR[-OB_ERR_PRIMARY_KEY_DUPLICATE] =
        "ORA-00001: unique constraint \'%s\' for key \'%.*s\' violated";
    ERROR_NAME[-OB_ERR_KEY_NAME_DUPLICATE] = "OB_ERR_KEY_NAME_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_KEY_NAME_DUPLICATE] = ER_DUP_KEYNAME;
    SQLSTATE[-OB_ERR_KEY_NAME_DUPLICATE] = "42000";
    STR_ERROR[-OB_ERR_KEY_NAME_DUPLICATE] = "Duplicated key name";
    STR_USER_ERROR[-OB_ERR_KEY_NAME_DUPLICATE] = "Duplicate key name \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_KEY_NAME_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_KEY_NAME_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5025, Duplicated key name";
    ORACLE_STR_USER_ERROR[-OB_ERR_KEY_NAME_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5025, Duplicate key name \'%.*s\'";
    ERROR_NAME[-OB_ERR_CREATETIME_DUPLICATE] = "OB_ERR_CREATETIME_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_CREATETIME_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_CREATETIME_DUPLICATE] = "42000";
    STR_ERROR[-OB_ERR_CREATETIME_DUPLICATE] = "Duplicated createtime";
    STR_USER_ERROR[-OB_ERR_CREATETIME_DUPLICATE] = "Duplicated createtime";
    ORACLE_ERRNO[-OB_ERR_CREATETIME_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CREATETIME_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5026, Duplicated createtime";
    ORACLE_STR_USER_ERROR[-OB_ERR_CREATETIME_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5026, Duplicated createtime";
    ERROR_NAME[-OB_ERR_MODIFYTIME_DUPLICATE] = "OB_ERR_MODIFYTIME_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_MODIFYTIME_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_MODIFYTIME_DUPLICATE] = "42000";
    STR_ERROR[-OB_ERR_MODIFYTIME_DUPLICATE] = "Duplicated modifytime";
    STR_USER_ERROR[-OB_ERR_MODIFYTIME_DUPLICATE] = "Duplicated modifytime";
    ORACLE_ERRNO[-OB_ERR_MODIFYTIME_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MODIFYTIME_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5027, Duplicated modifytime";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFYTIME_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5027, Duplicated modifytime";
    ERROR_NAME[-OB_ERR_ILLEGAL_INDEX] = "OB_ERR_ILLEGAL_INDEX";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_INDEX] = ER_NO_SUCH_INDEX;
    SQLSTATE[-OB_ERR_ILLEGAL_INDEX] = "42S12";
    STR_ERROR[-OB_ERR_ILLEGAL_INDEX] = "Illegal index";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_INDEX] = "Illegal index";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_INDEX] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_INDEX] = "ORA-00600: internal error code, arguments: -5028, Illegal index";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_INDEX] = "ORA-00600: internal error code, arguments: -5028, Illegal index";
    ERROR_NAME[-OB_ERR_INVALID_SCHEMA] = "OB_ERR_INVALID_SCHEMA";
    MYSQL_ERRNO[-OB_ERR_INVALID_SCHEMA] = -1;
    SQLSTATE[-OB_ERR_INVALID_SCHEMA] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SCHEMA] = "Invalid schema";
    STR_USER_ERROR[-OB_ERR_INVALID_SCHEMA] = "Invalid schema";
    ORACLE_ERRNO[-OB_ERR_INVALID_SCHEMA] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SCHEMA] = "ORA-00600: internal error code, arguments: -5029, Invalid schema";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SCHEMA] = "ORA-00600: internal error code, arguments: -5029, Invalid schema";
    ERROR_NAME[-OB_ERR_INSERT_NULL_ROWKEY] = "OB_ERR_INSERT_NULL_ROWKEY";
    MYSQL_ERRNO[-OB_ERR_INSERT_NULL_ROWKEY] = ER_PRIMARY_CANT_HAVE_NULL;
    SQLSTATE[-OB_ERR_INSERT_NULL_ROWKEY] = "42000";
    STR_ERROR[-OB_ERR_INSERT_NULL_ROWKEY] = "Insert null rowkey";
    STR_USER_ERROR[-OB_ERR_INSERT_NULL_ROWKEY] = "Insert null into %.*s";
    ORACLE_ERRNO[-OB_ERR_INSERT_NULL_ROWKEY] = 1400;
    ORACLE_STR_ERROR[-OB_ERR_INSERT_NULL_ROWKEY] = "ORA-01400: cannot insert NULL into";
    ORACLE_STR_USER_ERROR[-OB_ERR_INSERT_NULL_ROWKEY] = "ORA-01400: cannot insert NULL into (%.*s)";
    ERROR_NAME[-OB_ERR_COLUMN_NOT_FOUND] = "OB_ERR_COLUMN_NOT_FOUND";
    MYSQL_ERRNO[-OB_ERR_COLUMN_NOT_FOUND] = -1;
    SQLSTATE[-OB_ERR_COLUMN_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_ERR_COLUMN_NOT_FOUND] = "Column not found";
    STR_USER_ERROR[-OB_ERR_COLUMN_NOT_FOUND] = "Column not found";
    ORACLE_ERRNO[-OB_ERR_COLUMN_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_NOT_FOUND] = "ORA-00600: internal error code, arguments: -5031, Column not found";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -5031, Column not found";
    ERROR_NAME[-OB_ERR_DELETE_NULL_ROWKEY] = "OB_ERR_DELETE_NULL_ROWKEY";
    MYSQL_ERRNO[-OB_ERR_DELETE_NULL_ROWKEY] = -1;
    SQLSTATE[-OB_ERR_DELETE_NULL_ROWKEY] = "23000";
    STR_ERROR[-OB_ERR_DELETE_NULL_ROWKEY] = "Delete null rowkey";
    STR_USER_ERROR[-OB_ERR_DELETE_NULL_ROWKEY] = "Delete null rowkey";
    ORACLE_ERRNO[-OB_ERR_DELETE_NULL_ROWKEY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DELETE_NULL_ROWKEY] =
        "ORA-00600: internal error code, arguments: -5032, Delete null rowkey";
    ORACLE_STR_USER_ERROR[-OB_ERR_DELETE_NULL_ROWKEY] =
        "ORA-00600: internal error code, arguments: -5032, Delete null rowkey";
    ERROR_NAME[-OB_ERR_USER_EMPTY] = "OB_ERR_USER_EMPTY";
    MYSQL_ERRNO[-OB_ERR_USER_EMPTY] = -1;
    SQLSTATE[-OB_ERR_USER_EMPTY] = "01007";
    STR_ERROR[-OB_ERR_USER_EMPTY] = "No user";
    STR_USER_ERROR[-OB_ERR_USER_EMPTY] = "No user";
    ORACLE_ERRNO[-OB_ERR_USER_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_USER_EMPTY] = "ORA-00600: internal error code, arguments: -5034, No user";
    ORACLE_STR_USER_ERROR[-OB_ERR_USER_EMPTY] = "ORA-00600: internal error code, arguments: -5034, No user";
    ERROR_NAME[-OB_ERR_USER_NOT_EXIST] = "OB_ERR_USER_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_USER_NOT_EXIST] = ER_NO_SUCH_USER;
    SQLSTATE[-OB_ERR_USER_NOT_EXIST] = "01007";
    STR_ERROR[-OB_ERR_USER_NOT_EXIST] = "User not exist";
    STR_USER_ERROR[-OB_ERR_USER_NOT_EXIST] = "User not exist";
    ORACLE_ERRNO[-OB_ERR_USER_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_USER_NOT_EXIST] = "ORA-00600: internal error code, arguments: -5035, User not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_USER_NOT_EXIST] = "ORA-00600: internal error code, arguments: -5035, User not exist";
    ERROR_NAME[-OB_ERR_NO_PRIVILEGE] = "OB_ERR_NO_PRIVILEGE";
    MYSQL_ERRNO[-OB_ERR_NO_PRIVILEGE] = ER_SPECIFIC_ACCESS_DENIED_ERROR;
    SQLSTATE[-OB_ERR_NO_PRIVILEGE] = "42501";
    STR_ERROR[-OB_ERR_NO_PRIVILEGE] = "Access denied";
    STR_USER_ERROR[-OB_ERR_NO_PRIVILEGE] =
        "Access denied; you need (at least one of) the %s privilege(s) for this operation";
    ORACLE_ERRNO[-OB_ERR_NO_PRIVILEGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_PRIVILEGE] = "ORA-00600: internal error code, arguments: -5036, Access denied";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_PRIVILEGE] = "ORA-00600: internal error code, arguments: -5036, Access denied; "
                                                  "you need (at least one of) the %s privilege(s) for this operation";
    ERROR_NAME[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] = "OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY";
    MYSQL_ERRNO[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] = -1;
    SQLSTATE[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] = "HY000";
    STR_ERROR[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] = "No privilege entry";
    STR_USER_ERROR[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] = "No privilege entry";
    ORACLE_ERRNO[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] =
        "ORA-00600: internal error code, arguments: -5037, No privilege entry";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY] =
        "ORA-00600: internal error code, arguments: -5037, No privilege entry";
    ERROR_NAME[-OB_ERR_WRONG_PASSWORD] = "OB_ERR_WRONG_PASSWORD";
    MYSQL_ERRNO[-OB_ERR_WRONG_PASSWORD] = ER_PASSWORD_NO_MATCH;
    SQLSTATE[-OB_ERR_WRONG_PASSWORD] = "42000";
    STR_ERROR[-OB_ERR_WRONG_PASSWORD] = "Incorrect password";
    STR_USER_ERROR[-OB_ERR_WRONG_PASSWORD] = "Incorrect password";
    ORACLE_ERRNO[-OB_ERR_WRONG_PASSWORD] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_PASSWORD] = "ORA-00600: internal error code, arguments: -5038, Incorrect password";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_PASSWORD] =
        "ORA-00600: internal error code, arguments: -5038, Incorrect password";
    ERROR_NAME[-OB_ERR_USER_IS_LOCKED] = "OB_ERR_USER_IS_LOCKED";
    MYSQL_ERRNO[-OB_ERR_USER_IS_LOCKED] = ER_ACCOUNT_HAS_BEEN_LOCKED;
    SQLSTATE[-OB_ERR_USER_IS_LOCKED] = "HY000";
    STR_ERROR[-OB_ERR_USER_IS_LOCKED] = "User locked";
    STR_USER_ERROR[-OB_ERR_USER_IS_LOCKED] = "User locked";
    ORACLE_ERRNO[-OB_ERR_USER_IS_LOCKED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_USER_IS_LOCKED] = "ORA-00600: internal error code, arguments: -5039, User locked";
    ORACLE_STR_USER_ERROR[-OB_ERR_USER_IS_LOCKED] = "ORA-00600: internal error code, arguments: -5039, User locked";
    ERROR_NAME[-OB_ERR_UPDATE_ROWKEY_COLUMN] = "OB_ERR_UPDATE_ROWKEY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_UPDATE_ROWKEY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_UPDATE_ROWKEY_COLUMN] = "42000";
    STR_ERROR[-OB_ERR_UPDATE_ROWKEY_COLUMN] = "Can not update rowkey column";
    STR_USER_ERROR[-OB_ERR_UPDATE_ROWKEY_COLUMN] = "Can not update rowkey column";
    ORACLE_ERRNO[-OB_ERR_UPDATE_ROWKEY_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_ROWKEY_COLUMN] =
        "ORA-00600: internal error code, arguments: -5040, Can not update rowkey column";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_ROWKEY_COLUMN] =
        "ORA-00600: internal error code, arguments: -5040, Can not update rowkey column";
    ERROR_NAME[-OB_ERR_UPDATE_JOIN_COLUMN] = "OB_ERR_UPDATE_JOIN_COLUMN";
    MYSQL_ERRNO[-OB_ERR_UPDATE_JOIN_COLUMN] = -1;
    SQLSTATE[-OB_ERR_UPDATE_JOIN_COLUMN] = "42000";
    STR_ERROR[-OB_ERR_UPDATE_JOIN_COLUMN] = "Can not update join column";
    STR_USER_ERROR[-OB_ERR_UPDATE_JOIN_COLUMN] = "Can not update join column";
    ORACLE_ERRNO[-OB_ERR_UPDATE_JOIN_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_JOIN_COLUMN] =
        "ORA-00600: internal error code, arguments: -5041, Can not update join column";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_JOIN_COLUMN] =
        "ORA-00600: internal error code, arguments: -5041, Can not update join column";
    ERROR_NAME[-OB_ERR_INVALID_COLUMN_NUM] = "OB_ERR_INVALID_COLUMN_NUM";
    MYSQL_ERRNO[-OB_ERR_INVALID_COLUMN_NUM] = ER_OPERAND_COLUMNS;
    SQLSTATE[-OB_ERR_INVALID_COLUMN_NUM] = "21000";
    STR_ERROR[-OB_ERR_INVALID_COLUMN_NUM] = "Invalid column number";
    STR_USER_ERROR[-OB_ERR_INVALID_COLUMN_NUM] = "Operand should contain %ld column(s)";
    ORACLE_ERRNO[-OB_ERR_INVALID_COLUMN_NUM] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_COLUMN_NUM] =
        "ORA-00600: internal error code, arguments: -5042, Invalid column number";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_COLUMN_NUM] =
        "ORA-00600: internal error code, arguments: -5042, Operand should contain %ld column(s)";
    ERROR_NAME[-OB_ERR_PREPARE_STMT_NOT_FOUND] = "OB_ERR_PREPARE_STMT_NOT_FOUND";
    MYSQL_ERRNO[-OB_ERR_PREPARE_STMT_NOT_FOUND] = ER_UNKNOWN_STMT_HANDLER;
    SQLSTATE[-OB_ERR_PREPARE_STMT_NOT_FOUND] = "HY007";
    STR_ERROR[-OB_ERR_PREPARE_STMT_NOT_FOUND] = "Unknown prepared statement";
    STR_USER_ERROR[-OB_ERR_PREPARE_STMT_NOT_FOUND] = "statement not prepared, stmt_id=%u";
    ORACLE_ERRNO[-OB_ERR_PREPARE_STMT_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PREPARE_STMT_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -5043, Unknown prepared statement";
    ORACLE_STR_USER_ERROR[-OB_ERR_PREPARE_STMT_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -5043, statement not prepared, stmt_id=%u";
    ERROR_NAME[-OB_ERR_SYS_VARIABLE_UNKNOWN] = "OB_ERR_SYS_VARIABLE_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_SYS_VARIABLE_UNKNOWN] = ER_UNKNOWN_SYSTEM_VARIABLE;
    SQLSTATE[-OB_ERR_SYS_VARIABLE_UNKNOWN] = "HY000";
    STR_ERROR[-OB_ERR_SYS_VARIABLE_UNKNOWN] = "Unknown system variable";
    STR_USER_ERROR[-OB_ERR_SYS_VARIABLE_UNKNOWN] = "Unknown system variable '%.*s'";
    ORACLE_ERRNO[-OB_ERR_SYS_VARIABLE_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SYS_VARIABLE_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5044, Unknown system variable";
    ORACLE_STR_USER_ERROR[-OB_ERR_SYS_VARIABLE_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5044, Unknown system variable '%.*s'";
    ERROR_NAME[-OB_ERR_OLDER_PRIVILEGE_VERSION] = "OB_ERR_OLDER_PRIVILEGE_VERSION";
    MYSQL_ERRNO[-OB_ERR_OLDER_PRIVILEGE_VERSION] = -1;
    SQLSTATE[-OB_ERR_OLDER_PRIVILEGE_VERSION] = "HY000";
    STR_ERROR[-OB_ERR_OLDER_PRIVILEGE_VERSION] = "Older privilege version";
    STR_USER_ERROR[-OB_ERR_OLDER_PRIVILEGE_VERSION] = "Older privilege version";
    ORACLE_ERRNO[-OB_ERR_OLDER_PRIVILEGE_VERSION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OLDER_PRIVILEGE_VERSION] =
        "ORA-00600: internal error code, arguments: -5046, Older privilege version";
    ORACLE_STR_USER_ERROR[-OB_ERR_OLDER_PRIVILEGE_VERSION] =
        "ORA-00600: internal error code, arguments: -5046, Older privilege version";
    ERROR_NAME[-OB_ERR_LACK_OF_ROWKEY_COL] = "OB_ERR_LACK_OF_ROWKEY_COL";
    MYSQL_ERRNO[-OB_ERR_LACK_OF_ROWKEY_COL] = ER_REQUIRES_PRIMARY_KEY;
    SQLSTATE[-OB_ERR_LACK_OF_ROWKEY_COL] = "42000";
    STR_ERROR[-OB_ERR_LACK_OF_ROWKEY_COL] = "No rowkey column specified";
    STR_USER_ERROR[-OB_ERR_LACK_OF_ROWKEY_COL] = "Primary key column(s) not specified in the WHERE clause";
    ORACLE_ERRNO[-OB_ERR_LACK_OF_ROWKEY_COL] = 600;
    ORACLE_STR_ERROR[-OB_ERR_LACK_OF_ROWKEY_COL] =
        "ORA-00600: internal error code, arguments: -5047, No rowkey column specified";
    ORACLE_STR_USER_ERROR[-OB_ERR_LACK_OF_ROWKEY_COL] =
        "ORA-00600: internal error code, arguments: -5047, Primary key column(s) not specified in the WHERE clause";
    ERROR_NAME[-OB_ERR_USER_EXIST] = "OB_ERR_USER_EXIST";
    MYSQL_ERRNO[-OB_ERR_USER_EXIST] = -1;
    SQLSTATE[-OB_ERR_USER_EXIST] = "42710";
    STR_ERROR[-OB_ERR_USER_EXIST] = "User exists";
    STR_USER_ERROR[-OB_ERR_USER_EXIST] = "User exists";
    ORACLE_ERRNO[-OB_ERR_USER_EXIST] = 1920;
    ORACLE_STR_ERROR[-OB_ERR_USER_EXIST] = "ORA-01920: user name conflicts with another user or role name";
    ORACLE_STR_USER_ERROR[-OB_ERR_USER_EXIST] = "ORA-01920: user name conflicts with another user or role name";
    ERROR_NAME[-OB_ERR_PASSWORD_EMPTY] = "OB_ERR_PASSWORD_EMPTY";
    MYSQL_ERRNO[-OB_ERR_PASSWORD_EMPTY] = -1;
    SQLSTATE[-OB_ERR_PASSWORD_EMPTY] = "HY000";
    STR_ERROR[-OB_ERR_PASSWORD_EMPTY] = "Empty password";
    STR_USER_ERROR[-OB_ERR_PASSWORD_EMPTY] = "Empty password";
    ORACLE_ERRNO[-OB_ERR_PASSWORD_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PASSWORD_EMPTY] = "ORA-00600: internal error code, arguments: -5051, Empty password";
    ORACLE_STR_USER_ERROR[-OB_ERR_PASSWORD_EMPTY] = "ORA-00600: internal error code, arguments: -5051, Empty password";
    ERROR_NAME[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] = "OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE";
    MYSQL_ERRNO[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] = -1;
    SQLSTATE[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] = "42000";
    STR_ERROR[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] = "Failed to grant privelege";
    STR_USER_ERROR[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] = "Failed to grant privelege";
    ORACLE_ERRNO[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] =
        "ORA-00600: internal error code, arguments: -5052, Failed to grant privelege";
    ORACLE_STR_USER_ERROR[-OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE] =
        "ORA-00600: internal error code, arguments: -5052, Failed to grant privelege";
    ERROR_NAME[-OB_ERR_WRONG_DYNAMIC_PARAM] = "OB_ERR_WRONG_DYNAMIC_PARAM";
    MYSQL_ERRNO[-OB_ERR_WRONG_DYNAMIC_PARAM] = -1;
    SQLSTATE[-OB_ERR_WRONG_DYNAMIC_PARAM] = "HY093";
    STR_ERROR[-OB_ERR_WRONG_DYNAMIC_PARAM] = "Wrong dynamic parameters";
    STR_USER_ERROR[-OB_ERR_WRONG_DYNAMIC_PARAM] =
        "Incorrect arguments number to EXECUTE, need %ld arguments but give %ld";
    ORACLE_ERRNO[-OB_ERR_WRONG_DYNAMIC_PARAM] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_DYNAMIC_PARAM] =
        "ORA-00600: internal error code, arguments: -5053, Wrong dynamic parameters";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_DYNAMIC_PARAM] = "ORA-00600: internal error code, arguments: -5053, Incorrect "
                                                         "arguments number to EXECUTE, need %ld arguments but give %ld";
    ERROR_NAME[-OB_ERR_PARAM_SIZE] = "OB_ERR_PARAM_SIZE";
    MYSQL_ERRNO[-OB_ERR_PARAM_SIZE] = ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT;
    SQLSTATE[-OB_ERR_PARAM_SIZE] = "42000";
    STR_ERROR[-OB_ERR_PARAM_SIZE] = "Incorrect parameter count";
    STR_USER_ERROR[-OB_ERR_PARAM_SIZE] = "Incorrect parameter count in the call to native function '%.*s'";
    ORACLE_ERRNO[-OB_ERR_PARAM_SIZE] = 909;
    ORACLE_STR_ERROR[-OB_ERR_PARAM_SIZE] = "ORA-00909: invalid number of arguments";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARAM_SIZE] =
        "ORA-00909: invalid number of arguments in the call to native function '%.*s'";
    ERROR_NAME[-OB_ERR_FUNCTION_UNKNOWN] = "OB_ERR_FUNCTION_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_FUNCTION_UNKNOWN] = ER_SP_DOES_NOT_EXIST;
    SQLSTATE[-OB_ERR_FUNCTION_UNKNOWN] = "42000";
    STR_ERROR[-OB_ERR_FUNCTION_UNKNOWN] = "FUNCTION does not exist";
    STR_USER_ERROR[-OB_ERR_FUNCTION_UNKNOWN] = "FUNCTION %.*s does not exist";
    ORACLE_ERRNO[-OB_ERR_FUNCTION_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_FUNCTION_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5055, FUNCTION does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_FUNCTION_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5055, FUNCTION %.*s does not exist";
    ERROR_NAME[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] = "OB_ERR_CREAT_MODIFY_TIME_COLUMN";
    MYSQL_ERRNO[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] = -1;
    SQLSTATE[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] = "23000";
    STR_ERROR[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] = "CreateTime or ModifyTime column cannot be modified";
    STR_USER_ERROR[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] = "CreateTime or ModifyTime column cannot be modified";
    ORACLE_ERRNO[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] =
        "ORA-00600: internal error code, arguments: -5056, CreateTime or ModifyTime column cannot be modified";
    ORACLE_STR_USER_ERROR[-OB_ERR_CREAT_MODIFY_TIME_COLUMN] =
        "ORA-00600: internal error code, arguments: -5056, CreateTime or ModifyTime column cannot be modified";
    ERROR_NAME[-OB_ERR_MODIFY_PRIMARY_KEY] = "OB_ERR_MODIFY_PRIMARY_KEY";
    MYSQL_ERRNO[-OB_ERR_MODIFY_PRIMARY_KEY] = -1;
    SQLSTATE[-OB_ERR_MODIFY_PRIMARY_KEY] = "23000";
    STR_ERROR[-OB_ERR_MODIFY_PRIMARY_KEY] = "Primary key cannot be modified";
    STR_USER_ERROR[-OB_ERR_MODIFY_PRIMARY_KEY] = "Primary key cannot be modified";
    ORACLE_ERRNO[-OB_ERR_MODIFY_PRIMARY_KEY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_PRIMARY_KEY] =
        "ORA-00600: internal error code, arguments: -5057, Primary key cannot be modified";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_PRIMARY_KEY] =
        "ORA-00600: internal error code, arguments: -5057, Primary key cannot be modified";
    ERROR_NAME[-OB_ERR_PARAM_DUPLICATE] = "OB_ERR_PARAM_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_PARAM_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_PARAM_DUPLICATE] = "42000";
    STR_ERROR[-OB_ERR_PARAM_DUPLICATE] = "Duplicated parameters";
    STR_USER_ERROR[-OB_ERR_PARAM_DUPLICATE] = "Duplicated parameters";
    ORACLE_ERRNO[-OB_ERR_PARAM_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARAM_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5058, Duplicated parameters";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARAM_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5058, Duplicated parameters";
    ERROR_NAME[-OB_ERR_TOO_MANY_SESSIONS] = "OB_ERR_TOO_MANY_SESSIONS";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_SESSIONS] = ER_TOO_MANY_USER_CONNECTIONS;
    SQLSTATE[-OB_ERR_TOO_MANY_SESSIONS] = "42000";
    STR_ERROR[-OB_ERR_TOO_MANY_SESSIONS] = "Too many sessions";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_SESSIONS] = "Too many sessions";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_SESSIONS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_SESSIONS] = "ORA-00600: internal error code, arguments: -5059, Too many sessions";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_SESSIONS] =
        "ORA-00600: internal error code, arguments: -5059, Too many sessions";
    ERROR_NAME[-OB_ERR_TOO_MANY_PS] = "OB_ERR_TOO_MANY_PS";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_PS] = -1;
    SQLSTATE[-OB_ERR_TOO_MANY_PS] = "54023";
    STR_ERROR[-OB_ERR_TOO_MANY_PS] = "Too many prepared statements";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_PS] = "Too many prepared statements";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_PS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_PS] =
        "ORA-00600: internal error code, arguments: -5061, Too many prepared statements";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_PS] =
        "ORA-00600: internal error code, arguments: -5061, Too many prepared statements";
    ERROR_NAME[-OB_ERR_HINT_UNKNOWN] = "OB_ERR_HINT_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_HINT_UNKNOWN] = -1;
    SQLSTATE[-OB_ERR_HINT_UNKNOWN] = "42000";
    STR_ERROR[-OB_ERR_HINT_UNKNOWN] = "Unknown hint";
    STR_USER_ERROR[-OB_ERR_HINT_UNKNOWN] = "Unknown hint";
    ORACLE_ERRNO[-OB_ERR_HINT_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_HINT_UNKNOWN] = "ORA-00600: internal error code, arguments: -5063, Unknown hint";
    ORACLE_STR_USER_ERROR[-OB_ERR_HINT_UNKNOWN] = "ORA-00600: internal error code, arguments: -5063, Unknown hint";
    ERROR_NAME[-OB_ERR_WHEN_UNSATISFIED] = "OB_ERR_WHEN_UNSATISFIED";
    MYSQL_ERRNO[-OB_ERR_WHEN_UNSATISFIED] = -1;
    SQLSTATE[-OB_ERR_WHEN_UNSATISFIED] = "23000";
    STR_ERROR[-OB_ERR_WHEN_UNSATISFIED] = "When condition not satisfied";
    STR_USER_ERROR[-OB_ERR_WHEN_UNSATISFIED] = "When condition not satisfied";
    ORACLE_ERRNO[-OB_ERR_WHEN_UNSATISFIED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WHEN_UNSATISFIED] =
        "ORA-00600: internal error code, arguments: -5064, When condition not satisfied";
    ORACLE_STR_USER_ERROR[-OB_ERR_WHEN_UNSATISFIED] =
        "ORA-00600: internal error code, arguments: -5064, When condition not satisfied";
    ERROR_NAME[-OB_ERR_QUERY_INTERRUPTED] = "OB_ERR_QUERY_INTERRUPTED";
    MYSQL_ERRNO[-OB_ERR_QUERY_INTERRUPTED] = ER_QUERY_INTERRUPTED;
    SQLSTATE[-OB_ERR_QUERY_INTERRUPTED] = "70100";
    STR_ERROR[-OB_ERR_QUERY_INTERRUPTED] = "Query execution was interrupted";
    STR_USER_ERROR[-OB_ERR_QUERY_INTERRUPTED] = "Query execution was interrupted";
    ORACLE_ERRNO[-OB_ERR_QUERY_INTERRUPTED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_QUERY_INTERRUPTED] =
        "ORA-00600: internal error code, arguments: -5065, Query execution was interrupted";
    ORACLE_STR_USER_ERROR[-OB_ERR_QUERY_INTERRUPTED] =
        "ORA-00600: internal error code, arguments: -5065, Query execution was interrupted";
    ERROR_NAME[-OB_ERR_SESSION_INTERRUPTED] = "OB_ERR_SESSION_INTERRUPTED";
    MYSQL_ERRNO[-OB_ERR_SESSION_INTERRUPTED] = -1;
    SQLSTATE[-OB_ERR_SESSION_INTERRUPTED] = "HY000";
    STR_ERROR[-OB_ERR_SESSION_INTERRUPTED] = "Session interrupted";
    STR_USER_ERROR[-OB_ERR_SESSION_INTERRUPTED] = "Session interrupted";
    ORACLE_ERRNO[-OB_ERR_SESSION_INTERRUPTED] = 1092;
    ORACLE_STR_ERROR[-OB_ERR_SESSION_INTERRUPTED] = "ORA-01092: OceanBase instance terminated. Disconnection forced";
    ORACLE_STR_USER_ERROR[-OB_ERR_SESSION_INTERRUPTED] =
        "ORA-01092: OceanBase instance terminated. Disconnection forced";
    ERROR_NAME[-OB_ERR_UNKNOWN_SESSION_ID] = "OB_ERR_UNKNOWN_SESSION_ID";
    MYSQL_ERRNO[-OB_ERR_UNKNOWN_SESSION_ID] = -1;
    SQLSTATE[-OB_ERR_UNKNOWN_SESSION_ID] = "HY000";
    STR_ERROR[-OB_ERR_UNKNOWN_SESSION_ID] = "Unknown session ID";
    STR_USER_ERROR[-OB_ERR_UNKNOWN_SESSION_ID] = "Unknown session ID";
    ORACLE_ERRNO[-OB_ERR_UNKNOWN_SESSION_ID] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNKNOWN_SESSION_ID] =
        "ORA-00600: internal error code, arguments: -5067, Unknown session ID";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNKNOWN_SESSION_ID] =
        "ORA-00600: internal error code, arguments: -5067, Unknown session ID";
    ERROR_NAME[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] = "OB_ERR_PROTOCOL_NOT_RECOGNIZE";
    MYSQL_ERRNO[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] = -1;
    SQLSTATE[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] = "HY000";
    STR_ERROR[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] = "Incorrect protocol";
    STR_USER_ERROR[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] = "Incorrect protocol";
    ORACLE_ERRNO[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] =
        "ORA-00600: internal error code, arguments: -5068, Incorrect protocol";
    ORACLE_STR_USER_ERROR[-OB_ERR_PROTOCOL_NOT_RECOGNIZE] =
        "ORA-00600: internal error code, arguments: -5068, Incorrect protocol";
    ERROR_NAME[-OB_ERR_WRITE_AUTH_ERROR] = "OB_ERR_WRITE_AUTH_ERROR";
    MYSQL_ERRNO[-OB_ERR_WRITE_AUTH_ERROR] = -1;
    SQLSTATE[-OB_ERR_WRITE_AUTH_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_WRITE_AUTH_ERROR] = "Write auth packet error";
    STR_USER_ERROR[-OB_ERR_WRITE_AUTH_ERROR] = "Write auth packet error";
    ORACLE_ERRNO[-OB_ERR_WRITE_AUTH_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRITE_AUTH_ERROR] =
        "ORA-00600: internal error code, arguments: -5069, Write auth packet error";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRITE_AUTH_ERROR] =
        "ORA-00600: internal error code, arguments: -5069, Write auth packet error";
    ERROR_NAME[-OB_ERR_PARSE_JOIN_INFO] = "OB_ERR_PARSE_JOIN_INFO";
    MYSQL_ERRNO[-OB_ERR_PARSE_JOIN_INFO] = -1;
    SQLSTATE[-OB_ERR_PARSE_JOIN_INFO] = "42000";
    STR_ERROR[-OB_ERR_PARSE_JOIN_INFO] = "Wrong join info";
    STR_USER_ERROR[-OB_ERR_PARSE_JOIN_INFO] = "Wrong join info";
    ORACLE_ERRNO[-OB_ERR_PARSE_JOIN_INFO] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARSE_JOIN_INFO] = "ORA-00600: internal error code, arguments: -5070, Wrong join info";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARSE_JOIN_INFO] =
        "ORA-00600: internal error code, arguments: -5070, Wrong join info";
    ERROR_NAME[-OB_ERR_ALTER_INDEX_COLUMN] = "OB_ERR_ALTER_INDEX_COLUMN";
    MYSQL_ERRNO[-OB_ERR_ALTER_INDEX_COLUMN] = -1;
    SQLSTATE[-OB_ERR_ALTER_INDEX_COLUMN] = "42000";
    STR_ERROR[-OB_ERR_ALTER_INDEX_COLUMN] = "Cannot alter index column";
    STR_USER_ERROR[-OB_ERR_ALTER_INDEX_COLUMN] = "Cannot alter index column";
    ORACLE_ERRNO[-OB_ERR_ALTER_INDEX_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ALTER_INDEX_COLUMN] =
        "ORA-00600: internal error code, arguments: -5071, Cannot alter index column";
    ORACLE_STR_USER_ERROR[-OB_ERR_ALTER_INDEX_COLUMN] =
        "ORA-00600: internal error code, arguments: -5071, Cannot alter index column";
    ERROR_NAME[-OB_ERR_MODIFY_INDEX_TABLE] = "OB_ERR_MODIFY_INDEX_TABLE";
    MYSQL_ERRNO[-OB_ERR_MODIFY_INDEX_TABLE] = -1;
    SQLSTATE[-OB_ERR_MODIFY_INDEX_TABLE] = "42000";
    STR_ERROR[-OB_ERR_MODIFY_INDEX_TABLE] = "Cannot modify index table";
    STR_USER_ERROR[-OB_ERR_MODIFY_INDEX_TABLE] = "Cannot modify index table";
    ORACLE_ERRNO[-OB_ERR_MODIFY_INDEX_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_INDEX_TABLE] =
        "ORA-00600: internal error code, arguments: -5072, Cannot modify index table";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_INDEX_TABLE] =
        "ORA-00600: internal error code, arguments: -5072, Cannot modify index table";
    ERROR_NAME[-OB_ERR_INDEX_UNAVAILABLE] = "OB_ERR_INDEX_UNAVAILABLE";
    MYSQL_ERRNO[-OB_ERR_INDEX_UNAVAILABLE] = ER_NO_SUCH_INDEX;
    SQLSTATE[-OB_ERR_INDEX_UNAVAILABLE] = "42000";
    STR_ERROR[-OB_ERR_INDEX_UNAVAILABLE] = "Index unavailable";
    STR_USER_ERROR[-OB_ERR_INDEX_UNAVAILABLE] = "Index unavailable";
    ORACLE_ERRNO[-OB_ERR_INDEX_UNAVAILABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INDEX_UNAVAILABLE] = "ORA-00600: internal error code, arguments: -5073, Index unavailable";
    ORACLE_STR_USER_ERROR[-OB_ERR_INDEX_UNAVAILABLE] =
        "ORA-00600: internal error code, arguments: -5073, Index unavailable";
    ERROR_NAME[-OB_ERR_NOP_VALUE] = "OB_ERR_NOP_VALUE";
    MYSQL_ERRNO[-OB_ERR_NOP_VALUE] = -1;
    SQLSTATE[-OB_ERR_NOP_VALUE] = "23000";
    STR_ERROR[-OB_ERR_NOP_VALUE] = "NOP cannot be used here";
    STR_USER_ERROR[-OB_ERR_NOP_VALUE] = "NOP cannot be used here";
    ORACLE_ERRNO[-OB_ERR_NOP_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NOP_VALUE] = "ORA-00600: internal error code, arguments: -5074, NOP cannot be used here";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOP_VALUE] =
        "ORA-00600: internal error code, arguments: -5074, NOP cannot be used here";
    ERROR_NAME[-OB_ERR_PS_TOO_MANY_PARAM] = "OB_ERR_PS_TOO_MANY_PARAM";
    MYSQL_ERRNO[-OB_ERR_PS_TOO_MANY_PARAM] = ER_PS_MANY_PARAM;
    SQLSTATE[-OB_ERR_PS_TOO_MANY_PARAM] = "54000";
    STR_ERROR[-OB_ERR_PS_TOO_MANY_PARAM] = "Prepared statement contains too many placeholders";
    STR_USER_ERROR[-OB_ERR_PS_TOO_MANY_PARAM] = "Prepared statement contains too many placeholders";
    ORACLE_ERRNO[-OB_ERR_PS_TOO_MANY_PARAM] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PS_TOO_MANY_PARAM] =
        "ORA-00600: internal error code, arguments: -5080, Prepared statement contains too many placeholders";
    ORACLE_STR_USER_ERROR[-OB_ERR_PS_TOO_MANY_PARAM] =
        "ORA-00600: internal error code, arguments: -5080, Prepared statement contains too many placeholders";
    ERROR_NAME[-OB_ERR_READ_ONLY] = "OB_ERR_READ_ONLY";
    MYSQL_ERRNO[-OB_ERR_READ_ONLY] = -1;
    SQLSTATE[-OB_ERR_READ_ONLY] = "25000";
    STR_ERROR[-OB_ERR_READ_ONLY] = "The server is read only now";
    STR_USER_ERROR[-OB_ERR_READ_ONLY] = "The server is read only now";
    ORACLE_ERRNO[-OB_ERR_READ_ONLY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_READ_ONLY] =
        "ORA-00600: internal error code, arguments: -5081, The server is read only now";
    ORACLE_STR_USER_ERROR[-OB_ERR_READ_ONLY] =
        "ORA-00600: internal error code, arguments: -5081, The server is read only now";
    ERROR_NAME[-OB_ERR_INVALID_TYPE_FOR_OP] = "OB_ERR_INVALID_TYPE_FOR_OP";
    MYSQL_ERRNO[-OB_ERR_INVALID_TYPE_FOR_OP] = -1;
    SQLSTATE[-OB_ERR_INVALID_TYPE_FOR_OP] = "22000";
    STR_ERROR[-OB_ERR_INVALID_TYPE_FOR_OP] = "Invalid data type for the operation";
    STR_USER_ERROR[-OB_ERR_INVALID_TYPE_FOR_OP] = "invalid obj type for type promotion: left_type=%s right_type=%s";
    ORACLE_ERRNO[-OB_ERR_INVALID_TYPE_FOR_OP] = 932;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_TYPE_FOR_OP] = "ORA-00932: inconsistent datatypes";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_TYPE_FOR_OP] =
        "ORA-00932: inconsistent datatypes: left_type=%s right_type=%s";
    ERROR_NAME[-OB_ERR_CAST_VARCHAR_TO_BOOL] = "OB_ERR_CAST_VARCHAR_TO_BOOL";
    MYSQL_ERRNO[-OB_ERR_CAST_VARCHAR_TO_BOOL] = -1;
    SQLSTATE[-OB_ERR_CAST_VARCHAR_TO_BOOL] = "22000";
    STR_ERROR[-OB_ERR_CAST_VARCHAR_TO_BOOL] = "Can not cast varchar value to bool type";
    STR_USER_ERROR[-OB_ERR_CAST_VARCHAR_TO_BOOL] = "Can not cast varchar value to bool type";
    ORACLE_ERRNO[-OB_ERR_CAST_VARCHAR_TO_BOOL] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CAST_VARCHAR_TO_BOOL] =
        "ORA-00600: internal error code, arguments: -5084, Can not cast varchar value to bool type";
    ORACLE_STR_USER_ERROR[-OB_ERR_CAST_VARCHAR_TO_BOOL] =
        "ORA-00600: internal error code, arguments: -5084, Can not cast varchar value to bool type";
    ERROR_NAME[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = "OB_ERR_CAST_VARCHAR_TO_NUMBER";
    MYSQL_ERRNO[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = -1;
    SQLSTATE[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = "22000";
    STR_ERROR[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = "Not a number Can not cast varchar value to number type";
    STR_USER_ERROR[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = "Not a number Can not cast varchar value to number type";
    ORACLE_ERRNO[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = 1722;
    ORACLE_STR_ERROR[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = "ORA-01722: invalid number";
    ORACLE_STR_USER_ERROR[-OB_ERR_CAST_VARCHAR_TO_NUMBER] = "ORA-01722: invalid number";
    ERROR_NAME[-OB_ERR_CAST_VARCHAR_TO_TIME] = "OB_ERR_CAST_VARCHAR_TO_TIME";
    MYSQL_ERRNO[-OB_ERR_CAST_VARCHAR_TO_TIME] = -1;
    SQLSTATE[-OB_ERR_CAST_VARCHAR_TO_TIME] = "22000";
    STR_ERROR[-OB_ERR_CAST_VARCHAR_TO_TIME] = "Not timestamp Can not cast varchar value to timestamp type";
    STR_USER_ERROR[-OB_ERR_CAST_VARCHAR_TO_TIME] = "Not timestamp Can not cast varchar value to timestamp type";
    ORACLE_ERRNO[-OB_ERR_CAST_VARCHAR_TO_TIME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CAST_VARCHAR_TO_TIME] =
        "ORA-00600: internal error code, arguments: -5086, Not timestamp Can not cast varchar value to timestamp type";
    ORACLE_STR_USER_ERROR[-OB_ERR_CAST_VARCHAR_TO_TIME] =
        "ORA-00600: internal error code, arguments: -5086, Not timestamp Can not cast varchar value to timestamp type";
    ERROR_NAME[-OB_ERR_CAST_NUMBER_OVERFLOW] = "OB_ERR_CAST_NUMBER_OVERFLOW";
    MYSQL_ERRNO[-OB_ERR_CAST_NUMBER_OVERFLOW] = -1;
    SQLSTATE[-OB_ERR_CAST_NUMBER_OVERFLOW] = "22000";
    STR_ERROR[-OB_ERR_CAST_NUMBER_OVERFLOW] = "Result value was out of range when cast to number";
    STR_USER_ERROR[-OB_ERR_CAST_NUMBER_OVERFLOW] = "Result value was out of range when cast to number";
    ORACLE_ERRNO[-OB_ERR_CAST_NUMBER_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CAST_NUMBER_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5087, Result value was out of range when cast to number";
    ORACLE_STR_USER_ERROR[-OB_ERR_CAST_NUMBER_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5087, Result value was out of range when cast to number";
    ERROR_NAME[-OB_INTEGER_PRECISION_OVERFLOW] = "OB_INTEGER_PRECISION_OVERFLOW";
    MYSQL_ERRNO[-OB_INTEGER_PRECISION_OVERFLOW] = ER_WARN_DATA_OUT_OF_RANGE;
    SQLSTATE[-OB_INTEGER_PRECISION_OVERFLOW] = "22003";
    STR_ERROR[-OB_INTEGER_PRECISION_OVERFLOW] = "Result value was out of range when casting varchar to number";
    STR_USER_ERROR[-OB_INTEGER_PRECISION_OVERFLOW] =
        "value larger than specified precision(%ld,%ld) allowed for this column";
    ORACLE_ERRNO[-OB_INTEGER_PRECISION_OVERFLOW] = 1426;
    ORACLE_STR_ERROR[-OB_INTEGER_PRECISION_OVERFLOW] = "ORA-01426: numeric overflow";
    ORACLE_STR_USER_ERROR[-OB_INTEGER_PRECISION_OVERFLOW] =
        "ORA-01426: numeric overflow, value larger than specified precision(%ld,%ld) allowed for this column";
    ERROR_NAME[-OB_DECIMAL_PRECISION_OVERFLOW] = "OB_DECIMAL_PRECISION_OVERFLOW";
    MYSQL_ERRNO[-OB_DECIMAL_PRECISION_OVERFLOW] = ER_WARN_DATA_OUT_OF_RANGE;
    SQLSTATE[-OB_DECIMAL_PRECISION_OVERFLOW] = "22003";
    STR_ERROR[-OB_DECIMAL_PRECISION_OVERFLOW] = "Result value was out of range when casting varchar to number";
    STR_USER_ERROR[-OB_DECIMAL_PRECISION_OVERFLOW] =
        "value(%s) larger than specified precision(%ld,%ld) allowed for this column";
    ORACLE_ERRNO[-OB_DECIMAL_PRECISION_OVERFLOW] = 1426;
    ORACLE_STR_ERROR[-OB_DECIMAL_PRECISION_OVERFLOW] = "ORA-01426: numeric overflow";
    ORACLE_STR_USER_ERROR[-OB_DECIMAL_PRECISION_OVERFLOW] =
        "ORA-01426: numeric overflow, value(%s) larger than specified precision(%ld,%ld) allowed for this column";
    ERROR_NAME[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] = "OB_SCHEMA_NUMBER_PRECISION_OVERFLOW";
    MYSQL_ERRNO[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] = -1;
    SQLSTATE[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] = "22000";
    STR_ERROR[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] = "Precision was out of range";
    STR_USER_ERROR[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] = "Precision was out of range";
    ORACLE_ERRNO[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5090, Precision was out of range";
    ORACLE_STR_USER_ERROR[-OB_SCHEMA_NUMBER_PRECISION_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5090, Precision was out of range";
    ERROR_NAME[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] = "OB_SCHEMA_NUMBER_SCALE_OVERFLOW";
    MYSQL_ERRNO[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] = -1;
    SQLSTATE[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] = "22000";
    STR_ERROR[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] = "Scale value was out of range";
    STR_USER_ERROR[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] = "Scale value was out of range";
    ORACLE_ERRNO[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5091, Scale value was out of range";
    ORACLE_STR_USER_ERROR[-OB_SCHEMA_NUMBER_SCALE_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5091, Scale value was out of range";
    ERROR_NAME[-OB_ERR_INDEX_UNKNOWN] = "OB_ERR_INDEX_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_INDEX_UNKNOWN] = -1;
    SQLSTATE[-OB_ERR_INDEX_UNKNOWN] = "42000";
    STR_ERROR[-OB_ERR_INDEX_UNKNOWN] = "Unknown index";
    STR_USER_ERROR[-OB_ERR_INDEX_UNKNOWN] = "Unknown index";
    ORACLE_ERRNO[-OB_ERR_INDEX_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INDEX_UNKNOWN] = "ORA-00600: internal error code, arguments: -5092, Unknown index";
    ORACLE_STR_USER_ERROR[-OB_ERR_INDEX_UNKNOWN] = "ORA-00600: internal error code, arguments: -5092, Unknown index";
    ERROR_NAME[-OB_NUMERIC_OVERFLOW] = "OB_NUMERIC_OVERFLOW";
    MYSQL_ERRNO[-OB_NUMERIC_OVERFLOW] = ER_WARN_DATA_OUT_OF_RANGE;
    SQLSTATE[-OB_NUMERIC_OVERFLOW] = "22003";
    STR_ERROR[-OB_NUMERIC_OVERFLOW] = "Result value was out of range when casting varchar to number";
    STR_USER_ERROR[-OB_NUMERIC_OVERFLOW] = "Result value was out of range when casting varchar to number";
    ORACLE_ERRNO[-OB_NUMERIC_OVERFLOW] = 1426;
    ORACLE_STR_ERROR[-OB_NUMERIC_OVERFLOW] = "ORA-01426: numeric overflow";
    ORACLE_STR_USER_ERROR[-OB_NUMERIC_OVERFLOW] = "ORA-01426: numeric overflow";
    ERROR_NAME[-OB_ERR_TOO_MANY_JOIN_TABLES] = "OB_ERR_TOO_MANY_JOIN_TABLES";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_JOIN_TABLES] = -1;
    SQLSTATE[-OB_ERR_TOO_MANY_JOIN_TABLES] = "HY000";
    STR_ERROR[-OB_ERR_TOO_MANY_JOIN_TABLES] = "too many joined tables";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_JOIN_TABLES] = "too many joined tables";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_JOIN_TABLES] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_JOIN_TABLES] =
        "ORA-00600: internal error code, arguments: -5094, too many joined tables";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_JOIN_TABLES] =
        "ORA-00600: internal error code, arguments: -5094, too many joined tables";
    ERROR_NAME[-OB_ERR_VARCHAR_TOO_LONG] = "OB_ERR_VARCHAR_TOO_LONG";
    MYSQL_ERRNO[-OB_ERR_VARCHAR_TOO_LONG] = -1;
    SQLSTATE[-OB_ERR_VARCHAR_TOO_LONG] = "22001";
    STR_ERROR[-OB_ERR_VARCHAR_TOO_LONG] = "Varchar value is too long for the column";
    STR_USER_ERROR[-OB_ERR_VARCHAR_TOO_LONG] = "Data too long(%d>%ld) for column '%s'";
    ORACLE_ERRNO[-OB_ERR_VARCHAR_TOO_LONG] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VARCHAR_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -5098, Varchar value is too long for the column";
    ORACLE_STR_USER_ERROR[-OB_ERR_VARCHAR_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -5098, Data too long(%d>%ld) for column '%s'";
    ERROR_NAME[-OB_ERR_SYS_CONFIG_UNKNOWN] = "OB_ERR_SYS_CONFIG_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_SYS_CONFIG_UNKNOWN] = -1;
    SQLSTATE[-OB_ERR_SYS_CONFIG_UNKNOWN] = "42000";
    STR_ERROR[-OB_ERR_SYS_CONFIG_UNKNOWN] = "System config unknown";
    STR_USER_ERROR[-OB_ERR_SYS_CONFIG_UNKNOWN] = "System config unknown";
    ORACLE_ERRNO[-OB_ERR_SYS_CONFIG_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SYS_CONFIG_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5099, System config unknown";
    ORACLE_STR_USER_ERROR[-OB_ERR_SYS_CONFIG_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5099, System config unknown";
    ERROR_NAME[-OB_ERR_LOCAL_VARIABLE] = "OB_ERR_LOCAL_VARIABLE";
    MYSQL_ERRNO[-OB_ERR_LOCAL_VARIABLE] = ER_LOCAL_VARIABLE;
    SQLSTATE[-OB_ERR_LOCAL_VARIABLE] = "HY000";
    STR_ERROR[-OB_ERR_LOCAL_VARIABLE] = "Local variable";
    STR_USER_ERROR[-OB_ERR_LOCAL_VARIABLE] =
        "Variable \'%.*s\' is a SESSION variable and can't be used with SET GLOBAL";
    ORACLE_ERRNO[-OB_ERR_LOCAL_VARIABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_LOCAL_VARIABLE] = "ORA-00600: internal error code, arguments: -5100, Local variable";
    ORACLE_STR_USER_ERROR[-OB_ERR_LOCAL_VARIABLE] = "ORA-00600: internal error code, arguments: -5100, Variable "
                                                    "\'%.*s\' is a SESSION variable and can't be used with SET GLOBAL";
    ERROR_NAME[-OB_ERR_GLOBAL_VARIABLE] = "OB_ERR_GLOBAL_VARIABLE";
    MYSQL_ERRNO[-OB_ERR_GLOBAL_VARIABLE] = ER_GLOBAL_VARIABLE;
    SQLSTATE[-OB_ERR_GLOBAL_VARIABLE] = "HY000";
    STR_ERROR[-OB_ERR_GLOBAL_VARIABLE] = "Global variable";
    STR_USER_ERROR[-OB_ERR_GLOBAL_VARIABLE] =
        "Variable \'%.*s\' is a GLOBAL variable and should be set with SET GLOBAL";
    ORACLE_ERRNO[-OB_ERR_GLOBAL_VARIABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_GLOBAL_VARIABLE] = "ORA-00600: internal error code, arguments: -5101, Global variable";
    ORACLE_STR_USER_ERROR[-OB_ERR_GLOBAL_VARIABLE] = "ORA-00600: internal error code, arguments: -5101, Variable "
                                                     "\'%.*s\' is a GLOBAL variable and should be set with SET GLOBAL";
    ERROR_NAME[-OB_ERR_VARIABLE_IS_READONLY] = "OB_ERR_VARIABLE_IS_READONLY";
    MYSQL_ERRNO[-OB_ERR_VARIABLE_IS_READONLY] = ER_VARIABLE_IS_READONLY;
    SQLSTATE[-OB_ERR_VARIABLE_IS_READONLY] = "HY000";
    STR_ERROR[-OB_ERR_VARIABLE_IS_READONLY] = "variable is read only";
    STR_USER_ERROR[-OB_ERR_VARIABLE_IS_READONLY] =
        "%.*s variable '%.*s' is read-only. Use SET %.*s to assign the value";
    ORACLE_ERRNO[-OB_ERR_VARIABLE_IS_READONLY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VARIABLE_IS_READONLY] =
        "ORA-00600: internal error code, arguments: -5102, variable is read only";
    ORACLE_STR_USER_ERROR[-OB_ERR_VARIABLE_IS_READONLY] =
        "ORA-00600: internal error code, arguments: -5102, %.*s variable '%.*s' is read-only. Use SET %.*s to assign "
        "the value";
    ERROR_NAME[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] = "OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR";
    MYSQL_ERRNO[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] = ER_INCORRECT_GLOBAL_LOCAL_VAR;
    SQLSTATE[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] = "HY000";
    STR_ERROR[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] = "incorrect global or local variable";
    STR_USER_ERROR[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] = "Variable '%.*s' is a %.*s variable";
    ORACLE_ERRNO[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] =
        "ORA-00600: internal error code, arguments: -5103, incorrect global or local variable";
    ORACLE_STR_USER_ERROR[-OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR] =
        "ORA-00600: internal error code, arguments: -5103, Variable '%.*s' is a %.*s variable";
    ERROR_NAME[-OB_ERR_EXPIRE_INFO_TOO_LONG] = "OB_ERR_EXPIRE_INFO_TOO_LONG";
    MYSQL_ERRNO[-OB_ERR_EXPIRE_INFO_TOO_LONG] = -1;
    SQLSTATE[-OB_ERR_EXPIRE_INFO_TOO_LONG] = "42000";
    STR_ERROR[-OB_ERR_EXPIRE_INFO_TOO_LONG] = "Expire expression too long";
    STR_USER_ERROR[-OB_ERR_EXPIRE_INFO_TOO_LONG] = "length(%d) of expire_info is larger than the max allowed(%ld)";
    ORACLE_ERRNO[-OB_ERR_EXPIRE_INFO_TOO_LONG] = 600;
    ORACLE_STR_ERROR[-OB_ERR_EXPIRE_INFO_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -5104, Expire expression too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXPIRE_INFO_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -5104, length(%d) of expire_info is larger than the max "
        "allowed(%ld)";
    ERROR_NAME[-OB_ERR_EXPIRE_COND_TOO_LONG] = "OB_ERR_EXPIRE_COND_TOO_LONG";
    MYSQL_ERRNO[-OB_ERR_EXPIRE_COND_TOO_LONG] = -1;
    SQLSTATE[-OB_ERR_EXPIRE_COND_TOO_LONG] = "42000";
    STR_ERROR[-OB_ERR_EXPIRE_COND_TOO_LONG] = "Expire condition too long";
    STR_USER_ERROR[-OB_ERR_EXPIRE_COND_TOO_LONG] =
        "total length(%ld) of expire_info and its expression is larger than the max allowed(%ld)";
    ORACLE_ERRNO[-OB_ERR_EXPIRE_COND_TOO_LONG] = 600;
    ORACLE_STR_ERROR[-OB_ERR_EXPIRE_COND_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -5105, Expire condition too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXPIRE_COND_TOO_LONG] =
        "ORA-00600: internal error code, arguments: -5105, total length(%ld) of expire_info and its expression is "
        "larger than the max allowed(%ld)";
    ERROR_NAME[-OB_INVALID_ARGUMENT_FOR_EXTRACT] = "OB_INVALID_ARGUMENT_FOR_EXTRACT";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT_FOR_EXTRACT] = -1;
    SQLSTATE[-OB_INVALID_ARGUMENT_FOR_EXTRACT] = "42000";
    STR_ERROR[-OB_INVALID_ARGUMENT_FOR_EXTRACT] = "Invalid argument for extract()";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_EXTRACT] = "EXTRACT() expected timestamp or a string as date argument";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT_FOR_EXTRACT] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT_FOR_EXTRACT] =
        "ORA-00600: internal error code, arguments: -5106, Invalid argument for extract()";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_EXTRACT] =
        "ORA-00600: internal error code, arguments: -5106, EXTRACT() expected timestamp or a string as date argument";
    ERROR_NAME[-OB_INVALID_ARGUMENT_FOR_IS] = "OB_INVALID_ARGUMENT_FOR_IS";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT_FOR_IS] = -1;
    SQLSTATE[-OB_INVALID_ARGUMENT_FOR_IS] = "42000";
    STR_ERROR[-OB_INVALID_ARGUMENT_FOR_IS] = "Invalid argument for IS operator";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_IS] = "Invalid operand type for IS operator, lval=%s";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT_FOR_IS] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT_FOR_IS] =
        "ORA-00600: internal error code, arguments: -5107, Invalid argument for IS operator";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_IS] =
        "ORA-00600: internal error code, arguments: -5107, Invalid operand type for IS operator, lval=%s";
    ERROR_NAME[-OB_INVALID_ARGUMENT_FOR_LENGTH] = "OB_INVALID_ARGUMENT_FOR_LENGTH";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT_FOR_LENGTH] = -1;
    SQLSTATE[-OB_INVALID_ARGUMENT_FOR_LENGTH] = "42000";
    STR_ERROR[-OB_INVALID_ARGUMENT_FOR_LENGTH] = "Invalid argument for length()";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_LENGTH] = "function LENGTH() expected a varchar argument";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT_FOR_LENGTH] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT_FOR_LENGTH] =
        "ORA-00600: internal error code, arguments: -5108, Invalid argument for length()";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_LENGTH] =
        "ORA-00600: internal error code, arguments: -5108, function LENGTH() expected a varchar argument";
    ERROR_NAME[-OB_INVALID_ARGUMENT_FOR_SUBSTR] = "OB_INVALID_ARGUMENT_FOR_SUBSTR";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT_FOR_SUBSTR] = -1;
    SQLSTATE[-OB_INVALID_ARGUMENT_FOR_SUBSTR] = "42000";
    STR_ERROR[-OB_INVALID_ARGUMENT_FOR_SUBSTR] = "Invalid argument for substr()";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_SUBSTR] = "invalid input format. ret=%d text=%s start=%s length=%s";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT_FOR_SUBSTR] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT_FOR_SUBSTR] =
        "ORA-00600: internal error code, arguments: -5109, Invalid argument for substr()";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_SUBSTR] =
        "ORA-00600: internal error code, arguments: -5109, invalid input format. ret=%d text=%s start=%s length=%s";
    ERROR_NAME[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] = "OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] = -1;
    SQLSTATE[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] = "42000";
    STR_ERROR[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] = "Invalid argument for time_to_usec()";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] =
        "TIME_TO_USEC() expected timestamp or a string as date argument";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] =
        "ORA-00600: internal error code, arguments: -5110, Invalid argument for time_to_usec()";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC] =
        "ORA-00600: internal error code, arguments: -5110, TIME_TO_USEC() expected timestamp or a string as date "
        "argument";
    ERROR_NAME[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] = "OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME";
    MYSQL_ERRNO[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] = -1;
    SQLSTATE[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] = "42000";
    STR_ERROR[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] = "Invalid argument for usec_to_time()";
    STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] = "USEC_TO_TIME expected a interger number as usec argument";
    ORACLE_ERRNO[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] =
        "ORA-00600: internal error code, arguments: -5111, Invalid argument for usec_to_time()";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME] =
        "ORA-00600: internal error code, arguments: -5111, USEC_TO_TIME expected a interger number as usec argument";
    ERROR_NAME[-OB_ERR_USER_VARIABLE_UNKNOWN] = "OB_ERR_USER_VARIABLE_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_USER_VARIABLE_UNKNOWN] = -1;
    SQLSTATE[-OB_ERR_USER_VARIABLE_UNKNOWN] = "42P01";
    STR_ERROR[-OB_ERR_USER_VARIABLE_UNKNOWN] = "Unknown user variable";
    STR_USER_ERROR[-OB_ERR_USER_VARIABLE_UNKNOWN] = "Variable %.*s does not exists";
    ORACLE_ERRNO[-OB_ERR_USER_VARIABLE_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_USER_VARIABLE_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5112, Unknown user variable";
    ORACLE_STR_USER_ERROR[-OB_ERR_USER_VARIABLE_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -5112, Variable %.*s does not exists";
    ERROR_NAME[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] = "OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME";
    MYSQL_ERRNO[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] = -1;
    SQLSTATE[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] = "42000";
    STR_ERROR[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] = "Illegal usage of merging_frozen_time()";
    STR_USER_ERROR[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] =
        "MERGING_FROZEN_TIME() system function only be used in daily merging.";
    ORACLE_ERRNO[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] = 600;
    ORACLE_STR_ERROR[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] =
        "ORA-00600: internal error code, arguments: -5113, Illegal usage of merging_frozen_time()";
    ORACLE_STR_USER_ERROR[-OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME] =
        "ORA-00600: internal error code, arguments: -5113, MERGING_FROZEN_TIME() system function only be used in daily "
        "merging.";
    ERROR_NAME[-OB_INVALID_NUMERIC] = "OB_INVALID_NUMERIC";
    MYSQL_ERRNO[-OB_INVALID_NUMERIC] = -1;
    SQLSTATE[-OB_INVALID_NUMERIC] = "42000";
    STR_ERROR[-OB_INVALID_NUMERIC] = "Invalid numeric";
    STR_USER_ERROR[-OB_INVALID_NUMERIC] = "Invalid numeric char '%c'";
    ORACLE_ERRNO[-OB_INVALID_NUMERIC] = 1722;
    ORACLE_STR_ERROR[-OB_INVALID_NUMERIC] = "ORA-01722: invalid number";
    ORACLE_STR_USER_ERROR[-OB_INVALID_NUMERIC] = "ORA-01722: invalid number char '%c'";
    ERROR_NAME[-OB_ERR_REGEXP_ERROR] = "OB_ERR_REGEXP_ERROR";
    MYSQL_ERRNO[-OB_ERR_REGEXP_ERROR] = ER_REGEXP_ERROR;
    SQLSTATE[-OB_ERR_REGEXP_ERROR] = "42000";
    STR_ERROR[-OB_ERR_REGEXP_ERROR] = "Got error 'empty (sub)expression' from regexp";
    STR_USER_ERROR[-OB_ERR_REGEXP_ERROR] = "Got error 'empty (sub)expression' from regexp";
    ORACLE_ERRNO[-OB_ERR_REGEXP_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_ERROR] =
        "ORA-00600: internal error code, arguments: -5115, Got error 'empty (sub)expression' from regexp";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_ERROR] =
        "ORA-00600: internal error code, arguments: -5115, Got error 'empty (sub)expression' from regexp";
    ERROR_NAME[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] = "OB_SQL_LOG_OP_SETCHILD_OVERFLOW";
    MYSQL_ERRNO[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] = -1;
    SQLSTATE[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] = "HY000";
    STR_ERROR[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] = "Logical operator child index overflow";
    STR_USER_ERROR[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] = "Logical operator child index overflow";
    ORACLE_ERRNO[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5116, Logical operator child index overflow";
    ORACLE_STR_USER_ERROR[-OB_SQL_LOG_OP_SETCHILD_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5116, Logical operator child index overflow";
    ERROR_NAME[-OB_SQL_EXPLAIN_FAILED] = "OB_SQL_EXPLAIN_FAILED";
    MYSQL_ERRNO[-OB_SQL_EXPLAIN_FAILED] = -1;
    SQLSTATE[-OB_SQL_EXPLAIN_FAILED] = "HY000";
    STR_ERROR[-OB_SQL_EXPLAIN_FAILED] = "fail to explain plan";
    STR_USER_ERROR[-OB_SQL_EXPLAIN_FAILED] = "fail to explain plan";
    ORACLE_ERRNO[-OB_SQL_EXPLAIN_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_SQL_EXPLAIN_FAILED] = "ORA-00600: internal error code, arguments: -5117, fail to explain plan";
    ORACLE_STR_USER_ERROR[-OB_SQL_EXPLAIN_FAILED] =
        "ORA-00600: internal error code, arguments: -5117, fail to explain plan";
    ERROR_NAME[-OB_SQL_OPT_COPY_OP_FAILED] = "OB_SQL_OPT_COPY_OP_FAILED";
    MYSQL_ERRNO[-OB_SQL_OPT_COPY_OP_FAILED] = -1;
    SQLSTATE[-OB_SQL_OPT_COPY_OP_FAILED] = "HY000";
    STR_ERROR[-OB_SQL_OPT_COPY_OP_FAILED] = "fail to copy logical operator";
    STR_USER_ERROR[-OB_SQL_OPT_COPY_OP_FAILED] = "fail to copy logical operator";
    ORACLE_ERRNO[-OB_SQL_OPT_COPY_OP_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_SQL_OPT_COPY_OP_FAILED] =
        "ORA-00600: internal error code, arguments: -5118, fail to copy logical operator";
    ORACLE_STR_USER_ERROR[-OB_SQL_OPT_COPY_OP_FAILED] =
        "ORA-00600: internal error code, arguments: -5118, fail to copy logical operator";
    ERROR_NAME[-OB_SQL_OPT_GEN_PLAN_FALIED] = "OB_SQL_OPT_GEN_PLAN_FALIED";
    MYSQL_ERRNO[-OB_SQL_OPT_GEN_PLAN_FALIED] = -1;
    SQLSTATE[-OB_SQL_OPT_GEN_PLAN_FALIED] = "HY000";
    STR_ERROR[-OB_SQL_OPT_GEN_PLAN_FALIED] = "fail to generate plan";
    STR_USER_ERROR[-OB_SQL_OPT_GEN_PLAN_FALIED] = "fail to generate plan";
    ORACLE_ERRNO[-OB_SQL_OPT_GEN_PLAN_FALIED] = 600;
    ORACLE_STR_ERROR[-OB_SQL_OPT_GEN_PLAN_FALIED] =
        "ORA-00600: internal error code, arguments: -5119, fail to generate plan";
    ORACLE_STR_USER_ERROR[-OB_SQL_OPT_GEN_PLAN_FALIED] =
        "ORA-00600: internal error code, arguments: -5119, fail to generate plan";
    ERROR_NAME[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] = "OB_SQL_OPT_CREATE_RAWEXPR_FAILED";
    MYSQL_ERRNO[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] = -1;
    SQLSTATE[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] = "HY000";
    STR_ERROR[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] = "fail to create raw expr";
    STR_USER_ERROR[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] = "fail to create raw expr";
    ORACLE_ERRNO[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] =
        "ORA-00600: internal error code, arguments: -5120, fail to create raw expr";
    ORACLE_STR_USER_ERROR[-OB_SQL_OPT_CREATE_RAWEXPR_FAILED] =
        "ORA-00600: internal error code, arguments: -5120, fail to create raw expr";
    ERROR_NAME[-OB_SQL_OPT_JOIN_ORDER_FAILED] = "OB_SQL_OPT_JOIN_ORDER_FAILED";
    MYSQL_ERRNO[-OB_SQL_OPT_JOIN_ORDER_FAILED] = -1;
    SQLSTATE[-OB_SQL_OPT_JOIN_ORDER_FAILED] = "HY000";
    STR_ERROR[-OB_SQL_OPT_JOIN_ORDER_FAILED] = "fail to generate join order";
    STR_USER_ERROR[-OB_SQL_OPT_JOIN_ORDER_FAILED] = "fail to generate join order";
    ORACLE_ERRNO[-OB_SQL_OPT_JOIN_ORDER_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_SQL_OPT_JOIN_ORDER_FAILED] =
        "ORA-00600: internal error code, arguments: -5121, fail to generate join order";
    ORACLE_STR_USER_ERROR[-OB_SQL_OPT_JOIN_ORDER_FAILED] =
        "ORA-00600: internal error code, arguments: -5121, fail to generate join order";
    ERROR_NAME[-OB_SQL_OPT_ERROR] = "OB_SQL_OPT_ERROR";
    MYSQL_ERRNO[-OB_SQL_OPT_ERROR] = -1;
    SQLSTATE[-OB_SQL_OPT_ERROR] = "HY000";
    STR_ERROR[-OB_SQL_OPT_ERROR] = "optimizer general error";
    STR_USER_ERROR[-OB_SQL_OPT_ERROR] = "optimizer general error";
    ORACLE_ERRNO[-OB_SQL_OPT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_SQL_OPT_ERROR] = "ORA-00600: internal error code, arguments: -5122, optimizer general error";
    ORACLE_STR_USER_ERROR[-OB_SQL_OPT_ERROR] =
        "ORA-00600: internal error code, arguments: -5122, optimizer general error";
    ERROR_NAME[-OB_SQL_RESOLVER_NO_MEMORY] = "OB_SQL_RESOLVER_NO_MEMORY";
    MYSQL_ERRNO[-OB_SQL_RESOLVER_NO_MEMORY] = -1;
    SQLSTATE[-OB_SQL_RESOLVER_NO_MEMORY] = "HY000";
    STR_ERROR[-OB_SQL_RESOLVER_NO_MEMORY] = "sql resolver no memory";
    STR_USER_ERROR[-OB_SQL_RESOLVER_NO_MEMORY] = "sql resolver no memory";
    ORACLE_ERRNO[-OB_SQL_RESOLVER_NO_MEMORY] = 600;
    ORACLE_STR_ERROR[-OB_SQL_RESOLVER_NO_MEMORY] =
        "ORA-00600: internal error code, arguments: -5130, sql resolver no memory";
    ORACLE_STR_USER_ERROR[-OB_SQL_RESOLVER_NO_MEMORY] =
        "ORA-00600: internal error code, arguments: -5130, sql resolver no memory";
    ERROR_NAME[-OB_SQL_DML_ONLY] = "OB_SQL_DML_ONLY";
    MYSQL_ERRNO[-OB_SQL_DML_ONLY] = -1;
    SQLSTATE[-OB_SQL_DML_ONLY] = "HY000";
    STR_ERROR[-OB_SQL_DML_ONLY] = "plan cache support dml only";
    STR_USER_ERROR[-OB_SQL_DML_ONLY] = "plan cache support dml only";
    ORACLE_ERRNO[-OB_SQL_DML_ONLY] = 600;
    ORACLE_STR_ERROR[-OB_SQL_DML_ONLY] =
        "ORA-00600: internal error code, arguments: -5131, plan cache support dml only";
    ORACLE_STR_USER_ERROR[-OB_SQL_DML_ONLY] =
        "ORA-00600: internal error code, arguments: -5131, plan cache support dml only";
    ERROR_NAME[-OB_ERR_NO_GRANT] = "OB_ERR_NO_GRANT";
    MYSQL_ERRNO[-OB_ERR_NO_GRANT] = -1;
    SQLSTATE[-OB_ERR_NO_GRANT] = "42000";
    STR_ERROR[-OB_ERR_NO_GRANT] = "No such grant defined";
    STR_USER_ERROR[-OB_ERR_NO_GRANT] = "No such grant defined";
    ORACLE_ERRNO[-OB_ERR_NO_GRANT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_GRANT] = "ORA-00600: internal error code, arguments: -5133, No such grant defined";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_GRANT] = "ORA-00600: internal error code, arguments: -5133, No such grant defined";
    ERROR_NAME[-OB_ERR_NO_DB_SELECTED] = "OB_ERR_NO_DB_SELECTED";
    MYSQL_ERRNO[-OB_ERR_NO_DB_SELECTED] = ER_NO_DB_ERROR;
    SQLSTATE[-OB_ERR_NO_DB_SELECTED] = "3D000";
    STR_ERROR[-OB_ERR_NO_DB_SELECTED] = "No database selected";
    STR_USER_ERROR[-OB_ERR_NO_DB_SELECTED] = "No database selected";
    ORACLE_ERRNO[-OB_ERR_NO_DB_SELECTED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_DB_SELECTED] = "ORA-00600: internal error code, arguments: -5134, No database selected";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_DB_SELECTED] =
        "ORA-00600: internal error code, arguments: -5134, No database selected";
    ERROR_NAME[-OB_SQL_PC_OVERFLOW] = "OB_SQL_PC_OVERFLOW";
    MYSQL_ERRNO[-OB_SQL_PC_OVERFLOW] = -1;
    SQLSTATE[-OB_SQL_PC_OVERFLOW] = "HY000";
    STR_ERROR[-OB_SQL_PC_OVERFLOW] = "plan cache is overflow";
    STR_USER_ERROR[-OB_SQL_PC_OVERFLOW] = "plan cache is overflow";
    ORACLE_ERRNO[-OB_SQL_PC_OVERFLOW] = 600;
    ORACLE_STR_ERROR[-OB_SQL_PC_OVERFLOW] = "ORA-00600: internal error code, arguments: -5135, plan cache is overflow";
    ORACLE_STR_USER_ERROR[-OB_SQL_PC_OVERFLOW] =
        "ORA-00600: internal error code, arguments: -5135, plan cache is overflow";
    ERROR_NAME[-OB_SQL_PC_PLAN_DUPLICATE] = "OB_SQL_PC_PLAN_DUPLICATE";
    MYSQL_ERRNO[-OB_SQL_PC_PLAN_DUPLICATE] = -1;
    SQLSTATE[-OB_SQL_PC_PLAN_DUPLICATE] = "HY000";
    STR_ERROR[-OB_SQL_PC_PLAN_DUPLICATE] = "plan exists in plan cache already";
    STR_USER_ERROR[-OB_SQL_PC_PLAN_DUPLICATE] = "plan exists in plan cache already";
    ORACLE_ERRNO[-OB_SQL_PC_PLAN_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_SQL_PC_PLAN_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5136, plan exists in plan cache already";
    ORACLE_STR_USER_ERROR[-OB_SQL_PC_PLAN_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5136, plan exists in plan cache already";
    ERROR_NAME[-OB_SQL_PC_PLAN_EXPIRE] = "OB_SQL_PC_PLAN_EXPIRE";
    MYSQL_ERRNO[-OB_SQL_PC_PLAN_EXPIRE] = -1;
    SQLSTATE[-OB_SQL_PC_PLAN_EXPIRE] = "HY000";
    STR_ERROR[-OB_SQL_PC_PLAN_EXPIRE] = "plan is expired";
    STR_USER_ERROR[-OB_SQL_PC_PLAN_EXPIRE] = "plan is expired";
    ORACLE_ERRNO[-OB_SQL_PC_PLAN_EXPIRE] = 600;
    ORACLE_STR_ERROR[-OB_SQL_PC_PLAN_EXPIRE] = "ORA-00600: internal error code, arguments: -5137, plan is expired";
    ORACLE_STR_USER_ERROR[-OB_SQL_PC_PLAN_EXPIRE] = "ORA-00600: internal error code, arguments: -5137, plan is expired";
    ERROR_NAME[-OB_SQL_PC_NOT_EXIST] = "OB_SQL_PC_NOT_EXIST";
    MYSQL_ERRNO[-OB_SQL_PC_NOT_EXIST] = -1;
    SQLSTATE[-OB_SQL_PC_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_SQL_PC_NOT_EXIST] = "no plan exist";
    STR_USER_ERROR[-OB_SQL_PC_NOT_EXIST] = "no plan exist";
    ORACLE_ERRNO[-OB_SQL_PC_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_SQL_PC_NOT_EXIST] = "ORA-00600: internal error code, arguments: -5138, no plan exist";
    ORACLE_STR_USER_ERROR[-OB_SQL_PC_NOT_EXIST] = "ORA-00600: internal error code, arguments: -5138, no plan exist";
    ERROR_NAME[-OB_SQL_PARAMS_LIMIT] = "OB_SQL_PARAMS_LIMIT";
    MYSQL_ERRNO[-OB_SQL_PARAMS_LIMIT] = -1;
    SQLSTATE[-OB_SQL_PARAMS_LIMIT] = "HY000";
    STR_ERROR[-OB_SQL_PARAMS_LIMIT] = "too many params, plan cache not support";
    STR_USER_ERROR[-OB_SQL_PARAMS_LIMIT] = "too many params, plan cache not support";
    ORACLE_ERRNO[-OB_SQL_PARAMS_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_SQL_PARAMS_LIMIT] =
        "ORA-00600: internal error code, arguments: -5139, too many params, plan cache not support";
    ORACLE_STR_USER_ERROR[-OB_SQL_PARAMS_LIMIT] =
        "ORA-00600: internal error code, arguments: -5139, too many params, plan cache not support";
    ERROR_NAME[-OB_SQL_PC_PLAN_SIZE_LIMIT] = "OB_SQL_PC_PLAN_SIZE_LIMIT";
    MYSQL_ERRNO[-OB_SQL_PC_PLAN_SIZE_LIMIT] = -1;
    SQLSTATE[-OB_SQL_PC_PLAN_SIZE_LIMIT] = "HY000";
    STR_ERROR[-OB_SQL_PC_PLAN_SIZE_LIMIT] = "plan is too big to add to plan cache";
    STR_USER_ERROR[-OB_SQL_PC_PLAN_SIZE_LIMIT] = "plan is too big to add to plan cache";
    ORACLE_ERRNO[-OB_SQL_PC_PLAN_SIZE_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_SQL_PC_PLAN_SIZE_LIMIT] =
        "ORA-00600: internal error code, arguments: -5140, plan is too big to add to plan cache";
    ORACLE_STR_USER_ERROR[-OB_SQL_PC_PLAN_SIZE_LIMIT] =
        "ORA-00600: internal error code, arguments: -5140, plan is too big to add to plan cache";
    ERROR_NAME[-OB_ERR_UNKNOWN_CHARSET] = "OB_ERR_UNKNOWN_CHARSET";
    MYSQL_ERRNO[-OB_ERR_UNKNOWN_CHARSET] = ER_UNKNOWN_CHARACTER_SET;
    SQLSTATE[-OB_ERR_UNKNOWN_CHARSET] = "42000";
    STR_ERROR[-OB_ERR_UNKNOWN_CHARSET] = "Unknown character set";
    STR_USER_ERROR[-OB_ERR_UNKNOWN_CHARSET] = "Unknown character set: '%.*s'";
    ORACLE_ERRNO[-OB_ERR_UNKNOWN_CHARSET] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNKNOWN_CHARSET] =
        "ORA-00600: internal error code, arguments: -5142, Unknown character set";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNKNOWN_CHARSET] =
        "ORA-00600: internal error code, arguments: -5142, Unknown character set: '%.*s'";
    ERROR_NAME[-OB_ERR_UNKNOWN_COLLATION] = "OB_ERR_UNKNOWN_COLLATION";
    MYSQL_ERRNO[-OB_ERR_UNKNOWN_COLLATION] = ER_UNKNOWN_COLLATION;
    SQLSTATE[-OB_ERR_UNKNOWN_COLLATION] = "HY000";
    STR_ERROR[-OB_ERR_UNKNOWN_COLLATION] = "Unknown collation";
    STR_USER_ERROR[-OB_ERR_UNKNOWN_COLLATION] = "Unknown collation: '%.*s'";
    ORACLE_ERRNO[-OB_ERR_UNKNOWN_COLLATION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNKNOWN_COLLATION] = "ORA-00600: internal error code, arguments: -5143, Unknown collation";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNKNOWN_COLLATION] =
        "ORA-00600: internal error code, arguments: -5143, Unknown collation: '%.*s'";
    ERROR_NAME[-OB_ERR_COLLATION_MISMATCH] = "OB_ERR_COLLATION_MISMATCH";
    MYSQL_ERRNO[-OB_ERR_COLLATION_MISMATCH] = ER_COLLATION_CHARSET_MISMATCH;
    SQLSTATE[-OB_ERR_COLLATION_MISMATCH] = "42000";
    STR_ERROR[-OB_ERR_COLLATION_MISMATCH] = "The collation is not valid for the character set";
    STR_USER_ERROR[-OB_ERR_COLLATION_MISMATCH] = "COLLATION '%.*s' is not valid for CHARACTER SET '%.*s'";
    ORACLE_ERRNO[-OB_ERR_COLLATION_MISMATCH] = 12704;
    ORACLE_STR_ERROR[-OB_ERR_COLLATION_MISMATCH] = "ORA-12704: character set mismatch";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLLATION_MISMATCH] =
        "ORA-12704: COLLATION '%.*s' is not valid for CHARACTER SET '%.*s'";
    ERROR_NAME[-OB_ERR_WRONG_VALUE_FOR_VAR] = "OB_ERR_WRONG_VALUE_FOR_VAR";
    MYSQL_ERRNO[-OB_ERR_WRONG_VALUE_FOR_VAR] = ER_WRONG_VALUE_FOR_VAR;
    SQLSTATE[-OB_ERR_WRONG_VALUE_FOR_VAR] = "42000";
    STR_ERROR[-OB_ERR_WRONG_VALUE_FOR_VAR] = "Variable can't be set to the value";
    STR_USER_ERROR[-OB_ERR_WRONG_VALUE_FOR_VAR] = "Variable \'%.*s\' can't be set to the value of \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_WRONG_VALUE_FOR_VAR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_VALUE_FOR_VAR] =
        "ORA-00600: internal error code, arguments: -5145, Variable can't be set to the value";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_VALUE_FOR_VAR] =
        "ORA-00600: internal error code, arguments: -5145, Variable \'%.*s\' can't be set to the value of \'%.*s\'";
    ERROR_NAME[-OB_UNKNOWN_PARTITION] = "OB_UNKNOWN_PARTITION";
    MYSQL_ERRNO[-OB_UNKNOWN_PARTITION] = ER_UNKNOWN_PARTITION;
    SQLSTATE[-OB_UNKNOWN_PARTITION] = "HY000";
    STR_ERROR[-OB_UNKNOWN_PARTITION] = "Unknown partition";
    STR_USER_ERROR[-OB_UNKNOWN_PARTITION] = "Unkown partition '%.*s' in table '%.*s'";
    ORACLE_ERRNO[-OB_UNKNOWN_PARTITION] = 2149;
    ORACLE_STR_ERROR[-OB_UNKNOWN_PARTITION] = "ORA-02149: Specified partition does not exist";
    ORACLE_STR_USER_ERROR[-OB_UNKNOWN_PARTITION] =
        "ORA-02149: Specified partition does not exist '%.*s' in table '%.*s'";
    ERROR_NAME[-OB_PARTITION_NOT_MATCH] = "OB_PARTITION_NOT_MATCH";
    MYSQL_ERRNO[-OB_PARTITION_NOT_MATCH] = ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET;
    SQLSTATE[-OB_PARTITION_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_PARTITION_NOT_MATCH] = "Found a row not matching the given partition set";
    STR_USER_ERROR[-OB_PARTITION_NOT_MATCH] = "Found a row not matching the given partition set";
    ORACLE_ERRNO[-OB_PARTITION_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -5147, Found a row not matching the given partition set";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -5147, Found a row not matching the given partition set";
    ERROR_NAME[-OB_ER_PASSWD_LENGTH] = "OB_ER_PASSWD_LENGTH";
    MYSQL_ERRNO[-OB_ER_PASSWD_LENGTH] = -1;
    SQLSTATE[-OB_ER_PASSWD_LENGTH] = "HY000";
    STR_ERROR[-OB_ER_PASSWD_LENGTH] = " Password hash should be a 40-digit hexadecimal number";
    STR_USER_ERROR[-OB_ER_PASSWD_LENGTH] = " Password hash should be a 40-digit hexadecimal number";
    ORACLE_ERRNO[-OB_ER_PASSWD_LENGTH] = 600;
    ORACLE_STR_ERROR[-OB_ER_PASSWD_LENGTH] =
        "ORA-00600: internal error code, arguments: -5148,  Password hash should be a 40-digit hexadecimal number";
    ORACLE_STR_USER_ERROR[-OB_ER_PASSWD_LENGTH] =
        "ORA-00600: internal error code, arguments: -5148,  Password hash should be a 40-digit hexadecimal number";
    ERROR_NAME[-OB_ERR_INSERT_INNER_JOIN_COLUMN] = "OB_ERR_INSERT_INNER_JOIN_COLUMN";
    MYSQL_ERRNO[-OB_ERR_INSERT_INNER_JOIN_COLUMN] = -1;
    SQLSTATE[-OB_ERR_INSERT_INNER_JOIN_COLUMN] = "07000";
    STR_ERROR[-OB_ERR_INSERT_INNER_JOIN_COLUMN] = "Insert inner join column error";
    STR_USER_ERROR[-OB_ERR_INSERT_INNER_JOIN_COLUMN] = "Insert inner join column error";
    ORACLE_ERRNO[-OB_ERR_INSERT_INNER_JOIN_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INSERT_INNER_JOIN_COLUMN] =
        "ORA-00600: internal error code, arguments: -5149, Insert inner join column error";
    ORACLE_STR_USER_ERROR[-OB_ERR_INSERT_INNER_JOIN_COLUMN] =
        "ORA-00600: internal error code, arguments: -5149, Insert inner join column error";
    ERROR_NAME[-OB_TENANT_NOT_IN_SERVER] = "OB_TENANT_NOT_IN_SERVER";
    MYSQL_ERRNO[-OB_TENANT_NOT_IN_SERVER] = -1;
    SQLSTATE[-OB_TENANT_NOT_IN_SERVER] = "HY000";
    STR_ERROR[-OB_TENANT_NOT_IN_SERVER] = "Tenant not in this server";
    STR_USER_ERROR[-OB_TENANT_NOT_IN_SERVER] = "Tenant not in this server";
    ORACLE_ERRNO[-OB_TENANT_NOT_IN_SERVER] = 600;
    ORACLE_STR_ERROR[-OB_TENANT_NOT_IN_SERVER] =
        "ORA-00600: internal error code, arguments: -5150, Tenant not in this server";
    ORACLE_STR_USER_ERROR[-OB_TENANT_NOT_IN_SERVER] =
        "ORA-00600: internal error code, arguments: -5150, Tenant not in this server";
    ERROR_NAME[-OB_TABLEGROUP_NOT_EXIST] = "OB_TABLEGROUP_NOT_EXIST";
    MYSQL_ERRNO[-OB_TABLEGROUP_NOT_EXIST] = -1;
    SQLSTATE[-OB_TABLEGROUP_NOT_EXIST] = "42P01";
    STR_ERROR[-OB_TABLEGROUP_NOT_EXIST] = "tablegroup not exist";
    STR_USER_ERROR[-OB_TABLEGROUP_NOT_EXIST] = "tablegroup not exist";
    ORACLE_ERRNO[-OB_TABLEGROUP_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TABLEGROUP_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5151, tablegroup not exist";
    ORACLE_STR_USER_ERROR[-OB_TABLEGROUP_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5151, tablegroup not exist";
    ERROR_NAME[-OB_SUBQUERY_TOO_MANY_ROW] = "OB_SUBQUERY_TOO_MANY_ROW";
    MYSQL_ERRNO[-OB_SUBQUERY_TOO_MANY_ROW] = ER_SUBQUERY_NO_1_ROW;
    SQLSTATE[-OB_SUBQUERY_TOO_MANY_ROW] = "21000";
    STR_ERROR[-OB_SUBQUERY_TOO_MANY_ROW] = "Subquery returns more than 1 row";
    STR_USER_ERROR[-OB_SUBQUERY_TOO_MANY_ROW] = "Subquery returns more than 1 row";
    ORACLE_ERRNO[-OB_SUBQUERY_TOO_MANY_ROW] = 1427;
    ORACLE_STR_ERROR[-OB_SUBQUERY_TOO_MANY_ROW] = "ORA-01427: single-row subquery returns more than one row";
    ORACLE_STR_USER_ERROR[-OB_SUBQUERY_TOO_MANY_ROW] = "ORA-01427: single-row subquery returns more than one row";
    ERROR_NAME[-OB_ERR_BAD_DATABASE] = "OB_ERR_BAD_DATABASE";
    MYSQL_ERRNO[-OB_ERR_BAD_DATABASE] = ER_BAD_DB_ERROR;
    SQLSTATE[-OB_ERR_BAD_DATABASE] = "42000";
    STR_ERROR[-OB_ERR_BAD_DATABASE] = "Unknown database";
    STR_USER_ERROR[-OB_ERR_BAD_DATABASE] = "Unknown database '%.*s'";
    ORACLE_ERRNO[-OB_ERR_BAD_DATABASE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_BAD_DATABASE] = "ORA-00600: internal error code, arguments: -5154, Unknown database";
    ORACLE_STR_USER_ERROR[-OB_ERR_BAD_DATABASE] =
        "ORA-00600: internal error code, arguments: -5154, Unknown database '%.*s'";
    ERROR_NAME[-OB_CANNOT_USER] = "OB_CANNOT_USER";
    MYSQL_ERRNO[-OB_CANNOT_USER] = ER_CANNOT_USER;
    SQLSTATE[-OB_CANNOT_USER] = "HY000";
    STR_ERROR[-OB_CANNOT_USER] = "User operation failed";
    STR_USER_ERROR[-OB_CANNOT_USER] = "Operation %.*s failed for %.*s";
    ORACLE_ERRNO[-OB_CANNOT_USER] = 600;
    ORACLE_STR_ERROR[-OB_CANNOT_USER] = "ORA-00600: internal error code, arguments: -5155, User operation failed";
    ORACLE_STR_USER_ERROR[-OB_CANNOT_USER] =
        "ORA-00600: internal error code, arguments: -5155, Operation %.*s failed for %.*s";
    ERROR_NAME[-OB_TENANT_EXIST] = "OB_TENANT_EXIST";
    MYSQL_ERRNO[-OB_TENANT_EXIST] = -1;
    SQLSTATE[-OB_TENANT_EXIST] = "HY000";
    STR_ERROR[-OB_TENANT_EXIST] = "tenant already exist";
    STR_USER_ERROR[-OB_TENANT_EXIST] = "tenant \'%s\' already exist";
    ORACLE_ERRNO[-OB_TENANT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TENANT_EXIST] = "ORA-00600: internal error code, arguments: -5156, tenant already exist";
    ORACLE_STR_USER_ERROR[-OB_TENANT_EXIST] =
        "ORA-00600: internal error code, arguments: -5156, tenant \'%s\' already exist";
    ERROR_NAME[-OB_TENANT_NOT_EXIST] = "OB_TENANT_NOT_EXIST";
    MYSQL_ERRNO[-OB_TENANT_NOT_EXIST] = -1;
    SQLSTATE[-OB_TENANT_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_TENANT_NOT_EXIST] = "Unknown tenant";
    STR_USER_ERROR[-OB_TENANT_NOT_EXIST] = "Unknown tenant '%.*s'";
    ORACLE_ERRNO[-OB_TENANT_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TENANT_NOT_EXIST] = "ORA-00600: internal error code, arguments: -5157, Unknown tenant";
    ORACLE_STR_USER_ERROR[-OB_TENANT_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5157, Unknown tenant '%.*s'";
    ERROR_NAME[-OB_DATABASE_EXIST] = "OB_DATABASE_EXIST";
    MYSQL_ERRNO[-OB_DATABASE_EXIST] = ER_DB_CREATE_EXISTS;
    SQLSTATE[-OB_DATABASE_EXIST] = "HY000";
    STR_ERROR[-OB_DATABASE_EXIST] = "Can't create database;database exists";
    STR_USER_ERROR[-OB_DATABASE_EXIST] = "Can't create database '%.*s'; database exists";
    ORACLE_ERRNO[-OB_DATABASE_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_DATABASE_EXIST] =
        "ORA-00600: internal error code, arguments: -5158, Can't create database;database exists";
    ORACLE_STR_USER_ERROR[-OB_DATABASE_EXIST] =
        "ORA-00600: internal error code, arguments: -5158, Can't create database '%.*s'; database exists";
    ERROR_NAME[-OB_TABLEGROUP_EXIST] = "OB_TABLEGROUP_EXIST";
    MYSQL_ERRNO[-OB_TABLEGROUP_EXIST] = -1;
    SQLSTATE[-OB_TABLEGROUP_EXIST] = "HY000";
    STR_ERROR[-OB_TABLEGROUP_EXIST] = "tablegroup already exist";
    STR_USER_ERROR[-OB_TABLEGROUP_EXIST] = "tablegroup already exist";
    ORACLE_ERRNO[-OB_TABLEGROUP_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TABLEGROUP_EXIST] =
        "ORA-00600: internal error code, arguments: -5159, tablegroup already exist";
    ORACLE_STR_USER_ERROR[-OB_TABLEGROUP_EXIST] =
        "ORA-00600: internal error code, arguments: -5159, tablegroup already exist";
    ERROR_NAME[-OB_ERR_INVALID_TENANT_NAME] = "OB_ERR_INVALID_TENANT_NAME";
    MYSQL_ERRNO[-OB_ERR_INVALID_TENANT_NAME] = -1;
    SQLSTATE[-OB_ERR_INVALID_TENANT_NAME] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_TENANT_NAME] = "invalid tenant name specified in connection string";
    STR_USER_ERROR[-OB_ERR_INVALID_TENANT_NAME] = "invalid tenant name specified in connection string";
    ORACLE_ERRNO[-OB_ERR_INVALID_TENANT_NAME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_TENANT_NAME] =
        "ORA-00600: internal error code, arguments: -5160, invalid tenant name specified in connection string";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_TENANT_NAME] =
        "ORA-00600: internal error code, arguments: -5160, invalid tenant name specified in connection string";
    ERROR_NAME[-OB_EMPTY_TENANT] = "OB_EMPTY_TENANT";
    MYSQL_ERRNO[-OB_EMPTY_TENANT] = -1;
    SQLSTATE[-OB_EMPTY_TENANT] = "HY000";
    STR_ERROR[-OB_EMPTY_TENANT] = "tenant is empty";
    STR_USER_ERROR[-OB_EMPTY_TENANT] = "tenant is empty";
    ORACLE_ERRNO[-OB_EMPTY_TENANT] = 600;
    ORACLE_STR_ERROR[-OB_EMPTY_TENANT] = "ORA-00600: internal error code, arguments: -5161, tenant is empty";
    ORACLE_STR_USER_ERROR[-OB_EMPTY_TENANT] = "ORA-00600: internal error code, arguments: -5161, tenant is empty";
    ERROR_NAME[-OB_WRONG_DB_NAME] = "OB_WRONG_DB_NAME";
    MYSQL_ERRNO[-OB_WRONG_DB_NAME] = ER_WRONG_DB_NAME;
    SQLSTATE[-OB_WRONG_DB_NAME] = "42000";
    STR_ERROR[-OB_WRONG_DB_NAME] = "Incorrect database name";
    STR_USER_ERROR[-OB_WRONG_DB_NAME] = "Incorrect database name '%.*s'";
    ORACLE_ERRNO[-OB_WRONG_DB_NAME] = 600;
    ORACLE_STR_ERROR[-OB_WRONG_DB_NAME] = "ORA-00600: internal error code, arguments: -5162, Incorrect database name";
    ORACLE_STR_USER_ERROR[-OB_WRONG_DB_NAME] =
        "ORA-00600: internal error code, arguments: -5162, Incorrect database name '%.*s'";
    ERROR_NAME[-OB_WRONG_TABLE_NAME] = "OB_WRONG_TABLE_NAME";
    MYSQL_ERRNO[-OB_WRONG_TABLE_NAME] = ER_WRONG_TABLE_NAME;
    SQLSTATE[-OB_WRONG_TABLE_NAME] = "42000";
    STR_ERROR[-OB_WRONG_TABLE_NAME] = "Incorrect table name";
    STR_USER_ERROR[-OB_WRONG_TABLE_NAME] = "Incorrect table name '%.*s'";
    ORACLE_ERRNO[-OB_WRONG_TABLE_NAME] = 903;
    ORACLE_STR_ERROR[-OB_WRONG_TABLE_NAME] = "ORA-00903: invalid table name";
    ORACLE_STR_USER_ERROR[-OB_WRONG_TABLE_NAME] = "ORA-00903: invalid table name '%.*s'";
    ERROR_NAME[-OB_WRONG_COLUMN_NAME] = "OB_WRONG_COLUMN_NAME";
    MYSQL_ERRNO[-OB_WRONG_COLUMN_NAME] = ER_WRONG_COLUMN_NAME;
    SQLSTATE[-OB_WRONG_COLUMN_NAME] = "42000";
    STR_ERROR[-OB_WRONG_COLUMN_NAME] = "Incorrect column name";
    STR_USER_ERROR[-OB_WRONG_COLUMN_NAME] = "Incorrect column name '%.*s'";
    ORACLE_ERRNO[-OB_WRONG_COLUMN_NAME] = 904;
    ORACLE_STR_ERROR[-OB_WRONG_COLUMN_NAME] = "ORA-00904: invalid identifier";
    ORACLE_STR_USER_ERROR[-OB_WRONG_COLUMN_NAME] = "ORA-00904: invalid identifier '%.*s'";
    ERROR_NAME[-OB_ERR_COLUMN_SPEC] = "OB_ERR_COLUMN_SPEC";
    MYSQL_ERRNO[-OB_ERR_COLUMN_SPEC] = ER_WRONG_FIELD_SPEC;
    SQLSTATE[-OB_ERR_COLUMN_SPEC] = "42000";
    STR_ERROR[-OB_ERR_COLUMN_SPEC] = "Incorrect column specifier";
    STR_USER_ERROR[-OB_ERR_COLUMN_SPEC] = "Incorrect column specifier for column '%.*s'";
    ORACLE_ERRNO[-OB_ERR_COLUMN_SPEC] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_SPEC] =
        "ORA-00600: internal error code, arguments: -5165, Incorrect column specifier";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_SPEC] =
        "ORA-00600: internal error code, arguments: -5165, Incorrect column specifier for column '%.*s'";
    ERROR_NAME[-OB_ERR_DB_DROP_EXISTS] = "OB_ERR_DB_DROP_EXISTS";
    MYSQL_ERRNO[-OB_ERR_DB_DROP_EXISTS] = ER_DB_DROP_EXISTS;
    SQLSTATE[-OB_ERR_DB_DROP_EXISTS] = "HY000";
    STR_ERROR[-OB_ERR_DB_DROP_EXISTS] = "Can't drop database;database doesn't exist";
    STR_USER_ERROR[-OB_ERR_DB_DROP_EXISTS] = "Can't drop database '%.*s'; database doesn't exist";
    ORACLE_ERRNO[-OB_ERR_DB_DROP_EXISTS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DB_DROP_EXISTS] =
        "ORA-00600: internal error code, arguments: -5166, Can't drop database;database doesn't exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_DB_DROP_EXISTS] =
        "ORA-00600: internal error code, arguments: -5166, Can't drop database '%.*s'; database doesn't exist";
    ERROR_NAME[-OB_ERR_DATA_TOO_LONG] = "OB_ERR_DATA_TOO_LONG";
    MYSQL_ERRNO[-OB_ERR_DATA_TOO_LONG] = ER_DATA_TOO_LONG;
    SQLSTATE[-OB_ERR_DATA_TOO_LONG] = "22001";
    STR_ERROR[-OB_ERR_DATA_TOO_LONG] = "Data too long for column";
    STR_USER_ERROR[-OB_ERR_DATA_TOO_LONG] = "Data too long for column '%.*s' at row %ld";
    ORACLE_ERRNO[-OB_ERR_DATA_TOO_LONG] = 12899;
    ORACLE_STR_ERROR[-OB_ERR_DATA_TOO_LONG] = "ORA-12899: value too large for column";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATA_TOO_LONG] = "ORA-12899: value too large for column '%.*s' at row %ld";
    ERROR_NAME[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] = "OB_ERR_WRONG_VALUE_COUNT_ON_ROW";
    MYSQL_ERRNO[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] = ER_WRONG_VALUE_COUNT_ON_ROW;
    SQLSTATE[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] = "21S01";
    STR_ERROR[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] = "column count does not match value count";
    STR_USER_ERROR[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] = "column count does not match value count at row '%d'";
    ORACLE_ERRNO[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] =
        "ORA-00600: internal error code, arguments: -5168, column count does not match value count";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_VALUE_COUNT_ON_ROW] =
        "ORA-00600: internal error code, arguments: -5168, column count does not match value count at row '%d'";
    ERROR_NAME[-OB_ERR_CREATE_USER_WITH_GRANT] = "OB_ERR_CREATE_USER_WITH_GRANT";
    MYSQL_ERRNO[-OB_ERR_CREATE_USER_WITH_GRANT] = ER_CANT_CREATE_USER_WITH_GRANT;
    SQLSTATE[-OB_ERR_CREATE_USER_WITH_GRANT] = "42000";
    STR_ERROR[-OB_ERR_CREATE_USER_WITH_GRANT] = "You are not allowed to create a user with GRANT";
    STR_USER_ERROR[-OB_ERR_CREATE_USER_WITH_GRANT] = "You are not allowed to create a user with GRANT";
    ORACLE_ERRNO[-OB_ERR_CREATE_USER_WITH_GRANT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CREATE_USER_WITH_GRANT] =
        "ORA-00600: internal error code, arguments: -5169, You are not allowed to create a user with GRANT";
    ORACLE_STR_USER_ERROR[-OB_ERR_CREATE_USER_WITH_GRANT] =
        "ORA-00600: internal error code, arguments: -5169, You are not allowed to create a user with GRANT";
    ERROR_NAME[-OB_ERR_NO_DB_PRIVILEGE] = "OB_ERR_NO_DB_PRIVILEGE";
    MYSQL_ERRNO[-OB_ERR_NO_DB_PRIVILEGE] = ER_DBACCESS_DENIED_ERROR;
    SQLSTATE[-OB_ERR_NO_DB_PRIVILEGE] = "42000";
    STR_ERROR[-OB_ERR_NO_DB_PRIVILEGE] = "Access denied for user to database";
    STR_USER_ERROR[-OB_ERR_NO_DB_PRIVILEGE] = "Access denied for user '%.*s'@'%.*s' to database '%.*s'";
    ORACLE_ERRNO[-OB_ERR_NO_DB_PRIVILEGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_DB_PRIVILEGE] =
        "ORA-00600: internal error code, arguments: -5170, Access denied for user to database";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_DB_PRIVILEGE] =
        "ORA-00600: internal error code, arguments: -5170, Access denied for user '%.*s'@'%.*s' to database '%.*s'";
    ERROR_NAME[-OB_ERR_NO_TABLE_PRIVILEGE] = "OB_ERR_NO_TABLE_PRIVILEGE";
    MYSQL_ERRNO[-OB_ERR_NO_TABLE_PRIVILEGE] = ER_TABLEACCESS_DENIED_ERROR;
    SQLSTATE[-OB_ERR_NO_TABLE_PRIVILEGE] = "42000";
    STR_ERROR[-OB_ERR_NO_TABLE_PRIVILEGE] = "Command denied to user for table";
    STR_USER_ERROR[-OB_ERR_NO_TABLE_PRIVILEGE] = "%.*s command denied to user '%.*s'@'%.*s' for table '%.*s'";
    ORACLE_ERRNO[-OB_ERR_NO_TABLE_PRIVILEGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_TABLE_PRIVILEGE] =
        "ORA-00600: internal error code, arguments: -5171, Command denied to user for table";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_TABLE_PRIVILEGE] =
        "ORA-00600: internal error code, arguments: -5171, %.*s command denied to user '%.*s'@'%.*s' for table '%.*s'";
    ERROR_NAME[-OB_INVALID_ON_UPDATE] = "OB_INVALID_ON_UPDATE";
    MYSQL_ERRNO[-OB_INVALID_ON_UPDATE] = ER_INVALID_ON_UPDATE;
    SQLSTATE[-OB_INVALID_ON_UPDATE] = "HY000";
    STR_ERROR[-OB_INVALID_ON_UPDATE] = "Invalid ON UPDATE clause";
    STR_USER_ERROR[-OB_INVALID_ON_UPDATE] = "Invalid ON UPDATE clause for \'%s\' column";
    ORACLE_ERRNO[-OB_INVALID_ON_UPDATE] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_ON_UPDATE] =
        "ORA-00600: internal error code, arguments: -5172, Invalid ON UPDATE clause";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ON_UPDATE] =
        "ORA-00600: internal error code, arguments: -5172, Invalid ON UPDATE clause for \'%s\' column";
    ERROR_NAME[-OB_INVALID_DEFAULT] = "OB_INVALID_DEFAULT";
    MYSQL_ERRNO[-OB_INVALID_DEFAULT] = ER_INVALID_DEFAULT;
    SQLSTATE[-OB_INVALID_DEFAULT] = "42000";
    STR_ERROR[-OB_INVALID_DEFAULT] = "Invalid default value";
    STR_USER_ERROR[-OB_INVALID_DEFAULT] = "Invalid default value for \'%.*s\'";
    ORACLE_ERRNO[-OB_INVALID_DEFAULT] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_DEFAULT] = "ORA-00600: internal error code, arguments: -5173, Invalid default value";
    ORACLE_STR_USER_ERROR[-OB_INVALID_DEFAULT] =
        "ORA-00600: internal error code, arguments: -5173, Invalid default value for \'%.*s\'";
    ERROR_NAME[-OB_ERR_UPDATE_TABLE_USED] = "OB_ERR_UPDATE_TABLE_USED";
    MYSQL_ERRNO[-OB_ERR_UPDATE_TABLE_USED] = ER_UPDATE_TABLE_USED;
    SQLSTATE[-OB_ERR_UPDATE_TABLE_USED] = "HY000";
    STR_ERROR[-OB_ERR_UPDATE_TABLE_USED] = "Update table used";
    STR_USER_ERROR[-OB_ERR_UPDATE_TABLE_USED] = "You can\'t specify target table \'%s\' for update in FROM clause";
    ORACLE_ERRNO[-OB_ERR_UPDATE_TABLE_USED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_TABLE_USED] = "ORA-00600: internal error code, arguments: -5174, Update table used";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_TABLE_USED] = "ORA-00600: internal error code, arguments: -5174, You can\'t "
                                                       "specify target table \'%s\' for update in FROM clause";
    ERROR_NAME[-OB_ERR_COULUMN_VALUE_NOT_MATCH] = "OB_ERR_COULUMN_VALUE_NOT_MATCH";
    MYSQL_ERRNO[-OB_ERR_COULUMN_VALUE_NOT_MATCH] = ER_WRONG_VALUE_COUNT_ON_ROW;
    SQLSTATE[-OB_ERR_COULUMN_VALUE_NOT_MATCH] = "21S01";
    STR_ERROR[-OB_ERR_COULUMN_VALUE_NOT_MATCH] = "Column count doesn\'t match value count";
    STR_USER_ERROR[-OB_ERR_COULUMN_VALUE_NOT_MATCH] = "Column count doesn\'t match value count at row %ld";
    ORACLE_ERRNO[-OB_ERR_COULUMN_VALUE_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COULUMN_VALUE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -5175, Column count doesn\'t match value count";
    ORACLE_STR_USER_ERROR[-OB_ERR_COULUMN_VALUE_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -5175, Column count doesn\'t match value count at row %ld";
    ERROR_NAME[-OB_ERR_INVALID_GROUP_FUNC_USE] = "OB_ERR_INVALID_GROUP_FUNC_USE";
    MYSQL_ERRNO[-OB_ERR_INVALID_GROUP_FUNC_USE] = ER_INVALID_GROUP_FUNC_USE;
    SQLSTATE[-OB_ERR_INVALID_GROUP_FUNC_USE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_GROUP_FUNC_USE] = "Invalid use of group function";
    STR_USER_ERROR[-OB_ERR_INVALID_GROUP_FUNC_USE] = "Invalid use of group function";
    ORACLE_ERRNO[-OB_ERR_INVALID_GROUP_FUNC_USE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_GROUP_FUNC_USE] =
        "ORA-00600: internal error code, arguments: -5176, Invalid use of group function";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_GROUP_FUNC_USE] =
        "ORA-00600: internal error code, arguments: -5176, Invalid use of group function";
    ERROR_NAME[-OB_CANT_AGGREGATE_2COLLATIONS] = "OB_CANT_AGGREGATE_2COLLATIONS";
    MYSQL_ERRNO[-OB_CANT_AGGREGATE_2COLLATIONS] = ER_CANT_AGGREGATE_2COLLATIONS;
    SQLSTATE[-OB_CANT_AGGREGATE_2COLLATIONS] = "HY000";
    STR_ERROR[-OB_CANT_AGGREGATE_2COLLATIONS] = "Illegal mix of collations";
    STR_USER_ERROR[-OB_CANT_AGGREGATE_2COLLATIONS] = "Illegal mix of collations";
    ORACLE_ERRNO[-OB_CANT_AGGREGATE_2COLLATIONS] = 600;
    ORACLE_STR_ERROR[-OB_CANT_AGGREGATE_2COLLATIONS] =
        "ORA-00600: internal error code, arguments: -5177, Illegal mix of collations";
    ORACLE_STR_USER_ERROR[-OB_CANT_AGGREGATE_2COLLATIONS] =
        "ORA-00600: internal error code, arguments: -5177, Illegal mix of collations";
    ERROR_NAME[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] = "OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD";
    MYSQL_ERRNO[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] = ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
    SQLSTATE[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] = "HY000";
    STR_ERROR[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] =
        "Field is of a not allowed type for this type of partitioning";
    STR_USER_ERROR[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] =
        "Field \'%.*s\' is of a not allowed type for this type of partitioning";
    ORACLE_ERRNO[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] = 600;
    ORACLE_STR_ERROR[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] =
        "ORA-00600: internal error code, arguments: -5178, Field is of a not allowed type for this type of "
        "partitioning";
    ORACLE_STR_USER_ERROR[-OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD] =
        "ORA-00600: internal error code, arguments: -5178, Field \'%.*s\' is of a not allowed type for this type of "
        "partitioning";
    ERROR_NAME[-OB_ERR_TOO_LONG_IDENT] = "OB_ERR_TOO_LONG_IDENT";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_IDENT] = ER_TOO_LONG_IDENT;
    SQLSTATE[-OB_ERR_TOO_LONG_IDENT] = "42000";
    STR_ERROR[-OB_ERR_TOO_LONG_IDENT] = "Identifier name is too long";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_IDENT] = "Identifier name \'%.*s\' is too long";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_IDENT] = 972;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_IDENT] = "ORA-00972: identifier is too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_IDENT] = "ORA-00972: identifier \'%.*s\' is too long";
    ERROR_NAME[-OB_ERR_WRONG_TYPE_FOR_VAR] = "OB_ERR_WRONG_TYPE_FOR_VAR";
    MYSQL_ERRNO[-OB_ERR_WRONG_TYPE_FOR_VAR] = ER_WRONG_TYPE_FOR_VAR;
    SQLSTATE[-OB_ERR_WRONG_TYPE_FOR_VAR] = "42000";
    STR_ERROR[-OB_ERR_WRONG_TYPE_FOR_VAR] = "Incorrect argument type to variable";
    STR_USER_ERROR[-OB_ERR_WRONG_TYPE_FOR_VAR] = "Incorrect argument type to variable '%.*s'";
    ORACLE_ERRNO[-OB_ERR_WRONG_TYPE_FOR_VAR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_TYPE_FOR_VAR] =
        "ORA-00600: internal error code, arguments: -5180, Incorrect argument type to variable";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_TYPE_FOR_VAR] =
        "ORA-00600: internal error code, arguments: -5180, Incorrect argument type to variable '%.*s'";
    ERROR_NAME[-OB_WRONG_USER_NAME_LENGTH] = "OB_WRONG_USER_NAME_LENGTH";
    MYSQL_ERRNO[-OB_WRONG_USER_NAME_LENGTH] = ER_WRONG_STRING_LENGTH;
    SQLSTATE[-OB_WRONG_USER_NAME_LENGTH] = "HY000";
    STR_ERROR[-OB_WRONG_USER_NAME_LENGTH] = "String is too long for user_name (should be no longer than 16)";
    STR_USER_ERROR[-OB_WRONG_USER_NAME_LENGTH] =
        "String '%.*s' is too long for user name (should be no longer than 16)";
    ORACLE_ERRNO[-OB_WRONG_USER_NAME_LENGTH] = 600;
    ORACLE_STR_ERROR[-OB_WRONG_USER_NAME_LENGTH] = "ORA-00600: internal error code, arguments: -5181, String is too "
                                                   "long for user_name (should be no longer than 16)";
    ORACLE_STR_USER_ERROR[-OB_WRONG_USER_NAME_LENGTH] =
        "ORA-00600: internal error code, arguments: -5181, String '%.*s' is too long for user name (should be no "
        "longer than 16)";
    ERROR_NAME[-OB_ERR_PRIV_USAGE] = "OB_ERR_PRIV_USAGE";
    MYSQL_ERRNO[-OB_ERR_PRIV_USAGE] = ER_WRONG_USAGE;
    SQLSTATE[-OB_ERR_PRIV_USAGE] = "HY000";
    STR_ERROR[-OB_ERR_PRIV_USAGE] = "Incorrect usage of DB GRANT and GLOBAL PRIVILEGES";
    STR_USER_ERROR[-OB_ERR_PRIV_USAGE] = "Incorrect usage of DB GRANT and GLOBAL PRIVILEGES";
    ORACLE_ERRNO[-OB_ERR_PRIV_USAGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PRIV_USAGE] =
        "ORA-00600: internal error code, arguments: -5182, Incorrect usage of DB GRANT and GLOBAL PRIVILEGES";
    ORACLE_STR_USER_ERROR[-OB_ERR_PRIV_USAGE] =
        "ORA-00600: internal error code, arguments: -5182, Incorrect usage of DB GRANT and GLOBAL PRIVILEGES";
    ERROR_NAME[-OB_ILLEGAL_GRANT_FOR_TABLE] = "OB_ILLEGAL_GRANT_FOR_TABLE";
    MYSQL_ERRNO[-OB_ILLEGAL_GRANT_FOR_TABLE] = ER_ILLEGAL_GRANT_FOR_TABLE;
    SQLSTATE[-OB_ILLEGAL_GRANT_FOR_TABLE] = "42000";
    STR_ERROR[-OB_ILLEGAL_GRANT_FOR_TABLE] =
        "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used";
    STR_USER_ERROR[-OB_ILLEGAL_GRANT_FOR_TABLE] =
        "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used";
    ORACLE_ERRNO[-OB_ILLEGAL_GRANT_FOR_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ILLEGAL_GRANT_FOR_TABLE] =
        "ORA-00600: internal error code, arguments: -5183, Illegal GRANT/REVOKE command; please consult the manual to "
        "see which privileges can be used";
    ORACLE_STR_USER_ERROR[-OB_ILLEGAL_GRANT_FOR_TABLE] =
        "ORA-00600: internal error code, arguments: -5183, Illegal GRANT/REVOKE command; please consult the manual to "
        "see which privileges can be used";
    ERROR_NAME[-OB_ERR_REACH_AUTOINC_MAX] = "OB_ERR_REACH_AUTOINC_MAX";
    MYSQL_ERRNO[-OB_ERR_REACH_AUTOINC_MAX] = ER_AUTOINC_READ_FAILED;
    SQLSTATE[-OB_ERR_REACH_AUTOINC_MAX] = "HY000";
    STR_ERROR[-OB_ERR_REACH_AUTOINC_MAX] = "Failed to read auto-increment value from storage engine";
    STR_USER_ERROR[-OB_ERR_REACH_AUTOINC_MAX] = "Failed to read auto-increment value from storage engine";
    ORACLE_ERRNO[-OB_ERR_REACH_AUTOINC_MAX] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REACH_AUTOINC_MAX] =
        "ORA-00600: internal error code, arguments: -5184, Failed to read auto-increment value from storage engine";
    ORACLE_STR_USER_ERROR[-OB_ERR_REACH_AUTOINC_MAX] =
        "ORA-00600: internal error code, arguments: -5184, Failed to read auto-increment value from storage engine";
    ERROR_NAME[-OB_ERR_NO_TABLES_USED] = "OB_ERR_NO_TABLES_USED";
    MYSQL_ERRNO[-OB_ERR_NO_TABLES_USED] = ER_NO_TABLES_USED;
    SQLSTATE[-OB_ERR_NO_TABLES_USED] = "HY000";
    STR_ERROR[-OB_ERR_NO_TABLES_USED] = "No tables used";
    STR_USER_ERROR[-OB_ERR_NO_TABLES_USED] = "No tables used";
    ORACLE_ERRNO[-OB_ERR_NO_TABLES_USED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_TABLES_USED] = "ORA-00600: internal error code, arguments: -5185, No tables used";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_TABLES_USED] = "ORA-00600: internal error code, arguments: -5185, No tables used";
    ERROR_NAME[-OB_CANT_REMOVE_ALL_FIELDS] = "OB_CANT_REMOVE_ALL_FIELDS";
    MYSQL_ERRNO[-OB_CANT_REMOVE_ALL_FIELDS] = ER_CANT_REMOVE_ALL_FIELDS;
    SQLSTATE[-OB_CANT_REMOVE_ALL_FIELDS] = "42000";
    STR_ERROR[-OB_CANT_REMOVE_ALL_FIELDS] = "You can't delete all columns with ALTER TABLE; use DROP TABLE instead";
    STR_USER_ERROR[-OB_CANT_REMOVE_ALL_FIELDS] =
        "You can't delete all columns with ALTER TABLE; use DROP TABLE instead";
    ORACLE_ERRNO[-OB_CANT_REMOVE_ALL_FIELDS] = 600;
    ORACLE_STR_ERROR[-OB_CANT_REMOVE_ALL_FIELDS] = "ORA-00600: internal error code, arguments: -5187, You can't delete "
                                                   "all columns with ALTER TABLE; use DROP TABLE instead";
    ORACLE_STR_USER_ERROR[-OB_CANT_REMOVE_ALL_FIELDS] = "ORA-00600: internal error code, arguments: -5187, You can't "
                                                        "delete all columns with ALTER TABLE; use DROP TABLE instead";
    ERROR_NAME[-OB_TOO_MANY_PARTITIONS_ERROR] = "OB_TOO_MANY_PARTITIONS_ERROR";
    MYSQL_ERRNO[-OB_TOO_MANY_PARTITIONS_ERROR] = ER_TOO_MANY_PARTITIONS_ERROR;
    SQLSTATE[-OB_TOO_MANY_PARTITIONS_ERROR] = "HY000";
    STR_ERROR[-OB_TOO_MANY_PARTITIONS_ERROR] = "Too many partitions (including subpartitions) were defined";
    STR_USER_ERROR[-OB_TOO_MANY_PARTITIONS_ERROR] = "Too many partitions (including subpartitions) were defined";
    ORACLE_ERRNO[-OB_TOO_MANY_PARTITIONS_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_TOO_MANY_PARTITIONS_ERROR] =
        "ORA-00600: internal error code, arguments: -5188, Too many partitions (including subpartitions) were defined";
    ORACLE_STR_USER_ERROR[-OB_TOO_MANY_PARTITIONS_ERROR] =
        "ORA-00600: internal error code, arguments: -5188, Too many partitions (including subpartitions) were defined";
    ERROR_NAME[-OB_NO_PARTS_ERROR] = "OB_NO_PARTS_ERROR";
    MYSQL_ERRNO[-OB_NO_PARTS_ERROR] = ER_NO_PARTS_ERROR;
    SQLSTATE[-OB_NO_PARTS_ERROR] = "HY000";
    STR_ERROR[-OB_NO_PARTS_ERROR] = "Number of partitions = 0 is not an allowed value";
    STR_USER_ERROR[-OB_NO_PARTS_ERROR] = "Number of partitions = 0 is not an allowed value";
    ORACLE_ERRNO[-OB_NO_PARTS_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_NO_PARTS_ERROR] =
        "ORA-00600: internal error code, arguments: -5189, Number of partitions = 0 is not an allowed value";
    ORACLE_STR_USER_ERROR[-OB_NO_PARTS_ERROR] =
        "ORA-00600: internal error code, arguments: -5189, Number of partitions = 0 is not an allowed value";
    ERROR_NAME[-OB_WRONG_SUB_KEY] = "OB_WRONG_SUB_KEY";
    MYSQL_ERRNO[-OB_WRONG_SUB_KEY] = ER_WRONG_SUB_KEY;
    SQLSTATE[-OB_WRONG_SUB_KEY] = "HY000";
    STR_ERROR[-OB_WRONG_SUB_KEY] = "Incorrect prefix key; the used key part isn't a string, the used length is longer "
                                   "than the key part, or the storage engine doesn't support unique prefix keys";
    STR_USER_ERROR[-OB_WRONG_SUB_KEY] =
        "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the "
        "storage engine doesn't support unique prefix keys";
    ORACLE_ERRNO[-OB_WRONG_SUB_KEY] = 600;
    ORACLE_STR_ERROR[-OB_WRONG_SUB_KEY] =
        "ORA-00600: internal error code, arguments: -5190, Incorrect prefix key; the used key part isn't a string, the "
        "used length is longer than the key part, or the storage engine doesn't support unique prefix keys";
    ORACLE_STR_USER_ERROR[-OB_WRONG_SUB_KEY] =
        "ORA-00600: internal error code, arguments: -5190, Incorrect prefix key; the used key part isn't a string, the "
        "used length is longer than the key part, or the storage engine doesn't support unique prefix keys";
    ERROR_NAME[-OB_KEY_PART_0] = "OB_KEY_PART_0";
    MYSQL_ERRNO[-OB_KEY_PART_0] = ER_KEY_PART_0;
    SQLSTATE[-OB_KEY_PART_0] = "HY000";
    STR_ERROR[-OB_KEY_PART_0] = "Key part length cannot be 0";
    STR_USER_ERROR[-OB_KEY_PART_0] = "Key part \'%.*s\' length cannot be 0";
    ORACLE_ERRNO[-OB_KEY_PART_0] = 600;
    ORACLE_STR_ERROR[-OB_KEY_PART_0] = "ORA-00600: internal error code, arguments: -5191, Key part length cannot be 0";
    ORACLE_STR_USER_ERROR[-OB_KEY_PART_0] =
        "ORA-00600: internal error code, arguments: -5191, Key part \'%.*s\' length cannot be 0";
    ERROR_NAME[-OB_ERR_UNKNOWN_TIME_ZONE] = "OB_ERR_UNKNOWN_TIME_ZONE";
    MYSQL_ERRNO[-OB_ERR_UNKNOWN_TIME_ZONE] = ER_UNKNOWN_TIME_ZONE;
    SQLSTATE[-OB_ERR_UNKNOWN_TIME_ZONE] = "HY000";
    STR_ERROR[-OB_ERR_UNKNOWN_TIME_ZONE] = "Unknown or incorrect time zone";
    STR_USER_ERROR[-OB_ERR_UNKNOWN_TIME_ZONE] = "Unknown or incorrect time zone: \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_UNKNOWN_TIME_ZONE] = 1882;
    ORACLE_STR_ERROR[-OB_ERR_UNKNOWN_TIME_ZONE] = "ORA-01882: timezone region string not found";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNKNOWN_TIME_ZONE] = "ORA-01882: timezone region \'%.*s\' not found";
    ERROR_NAME[-OB_ERR_WRONG_AUTO_KEY] = "OB_ERR_WRONG_AUTO_KEY";
    MYSQL_ERRNO[-OB_ERR_WRONG_AUTO_KEY] = ER_WRONG_AUTO_KEY;
    SQLSTATE[-OB_ERR_WRONG_AUTO_KEY] = "42000";
    STR_ERROR[-OB_ERR_WRONG_AUTO_KEY] = "Incorrect table definition; there can be only one auto column";
    STR_USER_ERROR[-OB_ERR_WRONG_AUTO_KEY] = "Incorrect table definition; there can be only one auto column";
    ORACLE_ERRNO[-OB_ERR_WRONG_AUTO_KEY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_AUTO_KEY] = "ORA-00600: internal error code, arguments: -5193, Incorrect table "
                                               "definition; there can be only one auto column";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_AUTO_KEY] = "ORA-00600: internal error code, arguments: -5193, Incorrect table "
                                                    "definition; there can be only one auto column";
    ERROR_NAME[-OB_ERR_TOO_MANY_KEYS] = "OB_ERR_TOO_MANY_KEYS";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_KEYS] = ER_TOO_MANY_KEYS;
    SQLSTATE[-OB_ERR_TOO_MANY_KEYS] = "42000";
    STR_ERROR[-OB_ERR_TOO_MANY_KEYS] = "Too many keys specified";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_KEYS] = "Too many keys specified; max %ld keys allowed";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_KEYS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_KEYS] =
        "ORA-00600: internal error code, arguments: -5194, Too many keys specified";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_KEYS] =
        "ORA-00600: internal error code, arguments: -5194, Too many keys specified; max %ld keys allowed";
    ERROR_NAME[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] = "OB_ERR_TOO_MANY_ROWKEY_COLUMNS";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] = ER_TOO_MANY_KEY_PARTS;
    SQLSTATE[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] = "42000";
    STR_ERROR[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] = "Too many key parts specified";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] = "Too many key parts specified; max %ld parts allowed";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] =
        "ORA-00600: internal error code, arguments: -5195, Too many key parts specified";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_ROWKEY_COLUMNS] =
        "ORA-00600: internal error code, arguments: -5195, Too many key parts specified; max %ld parts allowed";
    ERROR_NAME[-OB_ERR_TOO_LONG_KEY_LENGTH] = "OB_ERR_TOO_LONG_KEY_LENGTH";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_KEY_LENGTH] = ER_TOO_LONG_KEY;
    SQLSTATE[-OB_ERR_TOO_LONG_KEY_LENGTH] = "42000";
    STR_ERROR[-OB_ERR_TOO_LONG_KEY_LENGTH] = "Specified key was too long";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_KEY_LENGTH] = "Specified key was too long; max key length is %ld bytes";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_KEY_LENGTH] = 1450;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_KEY_LENGTH] = "ORA-01450: maximum key length exceeded";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_KEY_LENGTH] = "ORA-01450: maximum key length (%ld) exceeded";
    ERROR_NAME[-OB_ERR_TOO_MANY_COLUMNS] = "OB_ERR_TOO_MANY_COLUMNS";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_COLUMNS] = ER_TOO_MANY_FIELDS;
    SQLSTATE[-OB_ERR_TOO_MANY_COLUMNS] = "42000";
    STR_ERROR[-OB_ERR_TOO_MANY_COLUMNS] = "Too many columns";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_COLUMNS] = "Too many columns";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_COLUMNS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_COLUMNS] = "ORA-00600: internal error code, arguments: -5197, Too many columns";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_COLUMNS] =
        "ORA-00600: internal error code, arguments: -5197, Too many columns";
    ERROR_NAME[-OB_ERR_TOO_LONG_COLUMN_LENGTH] = "OB_ERR_TOO_LONG_COLUMN_LENGTH";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_COLUMN_LENGTH] = ER_TOO_BIG_FIELDLENGTH;
    SQLSTATE[-OB_ERR_TOO_LONG_COLUMN_LENGTH] = "42000";
    STR_ERROR[-OB_ERR_TOO_LONG_COLUMN_LENGTH] = "Column length too big";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_COLUMN_LENGTH] = "Column length too big for column '%s' (max = %d)";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_COLUMN_LENGTH] = 910;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_COLUMN_LENGTH] = "ORA-00910: specified length too long for its datatype";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_COLUMN_LENGTH] =
        "ORA-00910: specified length too long for column '%s' (max = %d byte)";
    ERROR_NAME[-OB_ERR_TOO_BIG_ROWSIZE] = "OB_ERR_TOO_BIG_ROWSIZE";
    MYSQL_ERRNO[-OB_ERR_TOO_BIG_ROWSIZE] = ER_TOO_BIG_ROWSIZE;
    SQLSTATE[-OB_ERR_TOO_BIG_ROWSIZE] = "42000";
    STR_ERROR[-OB_ERR_TOO_BIG_ROWSIZE] = "Row size too large";
    STR_USER_ERROR[-OB_ERR_TOO_BIG_ROWSIZE] = "Row size too large";
    ORACLE_ERRNO[-OB_ERR_TOO_BIG_ROWSIZE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_BIG_ROWSIZE] = "ORA-00600: internal error code, arguments: -5199, Row size too large";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_BIG_ROWSIZE] =
        "ORA-00600: internal error code, arguments: -5199, Row size too large";
    ERROR_NAME[-OB_ERR_UNKNOWN_TABLE] = "OB_ERR_UNKNOWN_TABLE";
    MYSQL_ERRNO[-OB_ERR_UNKNOWN_TABLE] = ER_UNKNOWN_TABLE;
    SQLSTATE[-OB_ERR_UNKNOWN_TABLE] = "42S02";
    STR_ERROR[-OB_ERR_UNKNOWN_TABLE] = "Unknown table";
    STR_USER_ERROR[-OB_ERR_UNKNOWN_TABLE] = "Unknown table '%.*s' in %.*s";
    ORACLE_ERRNO[-OB_ERR_UNKNOWN_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNKNOWN_TABLE] = "ORA-00600: internal error code, arguments: -5200, Unknown table";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNKNOWN_TABLE] =
        "ORA-00600: internal error code, arguments: -5200, Unknown table '%.*s' in %.*s";
    ERROR_NAME[-OB_ERR_BAD_TABLE] = "OB_ERR_BAD_TABLE";
    MYSQL_ERRNO[-OB_ERR_BAD_TABLE] = ER_BAD_TABLE_ERROR;
    SQLSTATE[-OB_ERR_BAD_TABLE] = "42S02";
    STR_ERROR[-OB_ERR_BAD_TABLE] = "Unknown table";
    STR_USER_ERROR[-OB_ERR_BAD_TABLE] = "Unknown table '%.*s'";
    ORACLE_ERRNO[-OB_ERR_BAD_TABLE] = 942;
    ORACLE_STR_ERROR[-OB_ERR_BAD_TABLE] = "ORA-00942: table or view does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_BAD_TABLE] = "ORA-00942: table or view '%.*s' does not exist";
    ERROR_NAME[-OB_ERR_TOO_BIG_SCALE] = "OB_ERR_TOO_BIG_SCALE";
    MYSQL_ERRNO[-OB_ERR_TOO_BIG_SCALE] = ER_TOO_BIG_SCALE;
    SQLSTATE[-OB_ERR_TOO_BIG_SCALE] = "42000";
    STR_ERROR[-OB_ERR_TOO_BIG_SCALE] = "Too big scale specified for column";
    STR_USER_ERROR[-OB_ERR_TOO_BIG_SCALE] = "Too big scale %d specified for column '%s'. Maximum is %ld.";
    ORACLE_ERRNO[-OB_ERR_TOO_BIG_SCALE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_BIG_SCALE] =
        "ORA-00600: internal error code, arguments: -5202, Too big scale specified for column";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_BIG_SCALE] =
        "ORA-00600: internal error code, arguments: -5202, Too big scale %d specified for column '%s'. Maximum is %ld.";
    ERROR_NAME[-OB_ERR_TOO_BIG_PRECISION] = "OB_ERR_TOO_BIG_PRECISION";
    MYSQL_ERRNO[-OB_ERR_TOO_BIG_PRECISION] = ER_TOO_BIG_PRECISION;
    SQLSTATE[-OB_ERR_TOO_BIG_PRECISION] = "42000";
    STR_ERROR[-OB_ERR_TOO_BIG_PRECISION] = "Too big precision specified for column";
    STR_USER_ERROR[-OB_ERR_TOO_BIG_PRECISION] = "Too big precision %d specified for column '%s'. Maximum is %ld.";
    ORACLE_ERRNO[-OB_ERR_TOO_BIG_PRECISION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_BIG_PRECISION] =
        "ORA-00600: internal error code, arguments: -5203, Too big precision specified for column";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_BIG_PRECISION] = "ORA-00600: internal error code, arguments: -5203, Too big "
                                                       "precision %d specified for column '%s'. Maximum is %ld.";
    ERROR_NAME[-OB_ERR_M_BIGGER_THAN_D] = "OB_ERR_M_BIGGER_THAN_D";
    MYSQL_ERRNO[-OB_ERR_M_BIGGER_THAN_D] = ER_M_BIGGER_THAN_D;
    SQLSTATE[-OB_ERR_M_BIGGER_THAN_D] = "42000";
    STR_ERROR[-OB_ERR_M_BIGGER_THAN_D] = "precision must be >= scale";
    STR_USER_ERROR[-OB_ERR_M_BIGGER_THAN_D] =
        "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').";
    ORACLE_ERRNO[-OB_ERR_M_BIGGER_THAN_D] = 600;
    ORACLE_STR_ERROR[-OB_ERR_M_BIGGER_THAN_D] =
        "ORA-00600: internal error code, arguments: -5204, precision must be >= scale";
    ORACLE_STR_USER_ERROR[-OB_ERR_M_BIGGER_THAN_D] =
        "ORA-00600: internal error code, arguments: -5204, For float(M,D), double(M,D) or decimal(M,D), M must be >= D "
        "(column '%s').";
    ERROR_NAME[-OB_ERR_TOO_BIG_DISPLAYWIDTH] = "OB_ERR_TOO_BIG_DISPLAYWIDTH";
    MYSQL_ERRNO[-OB_ERR_TOO_BIG_DISPLAYWIDTH] = ER_TOO_BIG_DISPLAYWIDTH;
    SQLSTATE[-OB_ERR_TOO_BIG_DISPLAYWIDTH] = "42000";
    STR_ERROR[-OB_ERR_TOO_BIG_DISPLAYWIDTH] = "Display width out of range for column";
    STR_USER_ERROR[-OB_ERR_TOO_BIG_DISPLAYWIDTH] = "Display width out of range for column '%s' (max = %ld)";
    ORACLE_ERRNO[-OB_ERR_TOO_BIG_DISPLAYWIDTH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_BIG_DISPLAYWIDTH] =
        "ORA-00600: internal error code, arguments: -5205, Display width out of range for column";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_BIG_DISPLAYWIDTH] =
        "ORA-00600: internal error code, arguments: -5205, Display width out of range for column '%s' (max = %ld)";
    ERROR_NAME[-OB_WRONG_GROUP_FIELD] = "OB_WRONG_GROUP_FIELD";
    MYSQL_ERRNO[-OB_WRONG_GROUP_FIELD] = ER_WRONG_GROUP_FIELD;
    SQLSTATE[-OB_WRONG_GROUP_FIELD] = "42000";
    STR_ERROR[-OB_WRONG_GROUP_FIELD] = "Can't group on column";
    STR_USER_ERROR[-OB_WRONG_GROUP_FIELD] = "Can't group on '%.*s'";
    ORACLE_ERRNO[-OB_WRONG_GROUP_FIELD] = 600;
    ORACLE_STR_ERROR[-OB_WRONG_GROUP_FIELD] = "ORA-00600: internal error code, arguments: -5206, Can't group on column";
    ORACLE_STR_USER_ERROR[-OB_WRONG_GROUP_FIELD] =
        "ORA-00600: internal error code, arguments: -5206, Can't group on '%.*s'";
    ERROR_NAME[-OB_NON_UNIQ_ERROR] = "OB_NON_UNIQ_ERROR";
    MYSQL_ERRNO[-OB_NON_UNIQ_ERROR] = ER_NON_UNIQ_ERROR;
    SQLSTATE[-OB_NON_UNIQ_ERROR] = "23000";
    STR_ERROR[-OB_NON_UNIQ_ERROR] = "Column is ambiguous";
    STR_USER_ERROR[-OB_NON_UNIQ_ERROR] = "Column '%.*s' in %.*s is ambiguous";
    ORACLE_ERRNO[-OB_NON_UNIQ_ERROR] = 918;
    ORACLE_STR_ERROR[-OB_NON_UNIQ_ERROR] = "ORA-00918: Column is ambiguous";
    ORACLE_STR_USER_ERROR[-OB_NON_UNIQ_ERROR] = "ORA-00918: column '%.*s' in %.*s ambiguously defined";
    ERROR_NAME[-OB_ERR_NONUNIQ_TABLE] = "OB_ERR_NONUNIQ_TABLE";
    MYSQL_ERRNO[-OB_ERR_NONUNIQ_TABLE] = ER_NONUNIQ_TABLE;
    SQLSTATE[-OB_ERR_NONUNIQ_TABLE] = "42000";
    STR_ERROR[-OB_ERR_NONUNIQ_TABLE] = "Not unique table/alias";
    STR_USER_ERROR[-OB_ERR_NONUNIQ_TABLE] = "Not unique table/alias: \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_NONUNIQ_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NONUNIQ_TABLE] =
        "ORA-00600: internal error code, arguments: -5208, Not unique table/alias";
    ORACLE_STR_USER_ERROR[-OB_ERR_NONUNIQ_TABLE] =
        "ORA-00600: internal error code, arguments: -5208, Not unique table/alias: \'%.*s\'";
    ERROR_NAME[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = "OB_ERR_CANT_DROP_FIELD_OR_KEY";
    MYSQL_ERRNO[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = ER_CANT_DROP_FIELD_OR_KEY;
    SQLSTATE[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = "42000";
    STR_ERROR[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = "Can't DROP Column; check that column/key exists";
    STR_USER_ERROR[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = "Can't DROP '%.*s'; check that column/key exists";
    ORACLE_ERRNO[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = 1418;
    ORACLE_STR_ERROR[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = "ORA-01418: specified index does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANT_DROP_FIELD_OR_KEY] = "ORA-01418: specified index '%.*s' does not exist";
    ERROR_NAME[-OB_ERR_MULTIPLE_PRI_KEY] = "OB_ERR_MULTIPLE_PRI_KEY";
    MYSQL_ERRNO[-OB_ERR_MULTIPLE_PRI_KEY] = ER_MULTIPLE_PRI_KEY;
    SQLSTATE[-OB_ERR_MULTIPLE_PRI_KEY] = "42000";
    STR_ERROR[-OB_ERR_MULTIPLE_PRI_KEY] = "Multiple primary key defined";
    STR_USER_ERROR[-OB_ERR_MULTIPLE_PRI_KEY] = "Multiple primary key defined";
    ORACLE_ERRNO[-OB_ERR_MULTIPLE_PRI_KEY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MULTIPLE_PRI_KEY] =
        "ORA-00600: internal error code, arguments: -5210, Multiple primary key defined";
    ORACLE_STR_USER_ERROR[-OB_ERR_MULTIPLE_PRI_KEY] =
        "ORA-00600: internal error code, arguments: -5210, Multiple primary key defined";
    ERROR_NAME[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] = "OB_ERR_KEY_COLUMN_DOES_NOT_EXITS";
    MYSQL_ERRNO[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] = ER_KEY_COLUMN_DOES_NOT_EXITS;
    SQLSTATE[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] = "42000";
    STR_ERROR[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] = "Key column doesn't exist in table";
    STR_USER_ERROR[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] = "Key column '%.*s' doesn't exist in table";
    ORACLE_ERRNO[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] =
        "ORA-00600: internal error code, arguments: -5211, Key column doesn't exist in table";
    ORACLE_STR_USER_ERROR[-OB_ERR_KEY_COLUMN_DOES_NOT_EXITS] =
        "ORA-00600: internal error code, arguments: -5211, Key column '%.*s' doesn't exist in table";
    ERROR_NAME[-OB_ERR_AUTO_PARTITION_KEY] = "OB_ERR_AUTO_PARTITION_KEY";
    MYSQL_ERRNO[-OB_ERR_AUTO_PARTITION_KEY] = -1;
    SQLSTATE[-OB_ERR_AUTO_PARTITION_KEY] = "42000";
    STR_ERROR[-OB_ERR_AUTO_PARTITION_KEY] = "auto-increment column should not be part of partition key";
    STR_USER_ERROR[-OB_ERR_AUTO_PARTITION_KEY] = "auto-increment column '%.*s' should not be part of partition key";
    ORACLE_ERRNO[-OB_ERR_AUTO_PARTITION_KEY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_AUTO_PARTITION_KEY] =
        "ORA-00600: internal error code, arguments: -5212, auto-increment column should not be part of partition key";
    ORACLE_STR_USER_ERROR[-OB_ERR_AUTO_PARTITION_KEY] =
        "ORA-00600: internal error code, arguments: -5212, auto-increment column '%.*s' should not be part of "
        "partition key";
    ERROR_NAME[-OB_ERR_CANT_USE_OPTION_HERE] = "OB_ERR_CANT_USE_OPTION_HERE";
    MYSQL_ERRNO[-OB_ERR_CANT_USE_OPTION_HERE] = ER_CANT_USE_OPTION_HERE;
    SQLSTATE[-OB_ERR_CANT_USE_OPTION_HERE] = "42000";
    STR_ERROR[-OB_ERR_CANT_USE_OPTION_HERE] = "Incorrect usage/placement";
    STR_USER_ERROR[-OB_ERR_CANT_USE_OPTION_HERE] = "Incorrect usage/placement of '%s'";
    ORACLE_ERRNO[-OB_ERR_CANT_USE_OPTION_HERE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CANT_USE_OPTION_HERE] =
        "ORA-00600: internal error code, arguments: -5213, Incorrect usage/placement";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANT_USE_OPTION_HERE] =
        "ORA-00600: internal error code, arguments: -5213, Incorrect usage/placement of '%s'";
    ERROR_NAME[-OB_ERR_WRONG_OBJECT] = "OB_ERR_WRONG_OBJECT";
    MYSQL_ERRNO[-OB_ERR_WRONG_OBJECT] = ER_WRONG_OBJECT;
    SQLSTATE[-OB_ERR_WRONG_OBJECT] = "HY000";
    STR_ERROR[-OB_ERR_WRONG_OBJECT] = "Wrong object";
    STR_USER_ERROR[-OB_ERR_WRONG_OBJECT] = "\'%s.%s\' is not %s";
    ORACLE_ERRNO[-OB_ERR_WRONG_OBJECT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_OBJECT] = "ORA-00600: internal error code, arguments: -5214, Wrong object";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_OBJECT] =
        "ORA-00600: internal error code, arguments: -5214, \'%s.%s\' is not %s";
    ERROR_NAME[-OB_ERR_ON_RENAME] = "OB_ERR_ON_RENAME";
    MYSQL_ERRNO[-OB_ERR_ON_RENAME] = ER_ERROR_ON_RENAME;
    SQLSTATE[-OB_ERR_ON_RENAME] = "HY000";
    STR_ERROR[-OB_ERR_ON_RENAME] = "Error on rename table";
    STR_USER_ERROR[-OB_ERR_ON_RENAME] = "Error on rename of \'%s.%s\' to \'%s.%s\'";
    ORACLE_ERRNO[-OB_ERR_ON_RENAME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ON_RENAME] = "ORA-00600: internal error code, arguments: -5215, Error on rename table";
    ORACLE_STR_USER_ERROR[-OB_ERR_ON_RENAME] =
        "ORA-00600: internal error code, arguments: -5215, Error on rename of \'%s.%s\' to \'%s.%s\'";
    ERROR_NAME[-OB_ERR_WRONG_KEY_COLUMN] = "OB_ERR_WRONG_KEY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_WRONG_KEY_COLUMN] = ER_WRONG_KEY_COLUMN;
    SQLSTATE[-OB_ERR_WRONG_KEY_COLUMN] = "42000";
    STR_ERROR[-OB_ERR_WRONG_KEY_COLUMN] = "The used storage engine can't index column";
    STR_USER_ERROR[-OB_ERR_WRONG_KEY_COLUMN] = "The used storage engine can't index column '%.*s'";
    ORACLE_ERRNO[-OB_ERR_WRONG_KEY_COLUMN] = 2329;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_KEY_COLUMN] =
        "ORA-02329: column of datatype string cannot be unique or a primary key";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_KEY_COLUMN] =
        "ORA-02329: column '%.*s' of datatype string cannot be unique or a primary key";
    ERROR_NAME[-OB_ERR_BAD_FIELD_ERROR] = "OB_ERR_BAD_FIELD_ERROR";
    MYSQL_ERRNO[-OB_ERR_BAD_FIELD_ERROR] = ER_BAD_FIELD_ERROR;
    SQLSTATE[-OB_ERR_BAD_FIELD_ERROR] = "42S22";
    STR_ERROR[-OB_ERR_BAD_FIELD_ERROR] = "Unknown column";
    STR_USER_ERROR[-OB_ERR_BAD_FIELD_ERROR] = "Unknown column '%.*s' in '%.*s'";
    ORACLE_ERRNO[-OB_ERR_BAD_FIELD_ERROR] = 904;
    ORACLE_STR_ERROR[-OB_ERR_BAD_FIELD_ERROR] = "ORA-00904: invalid identifier";
    ORACLE_STR_USER_ERROR[-OB_ERR_BAD_FIELD_ERROR] = "ORA-00904: invalid identifier '%.*s' in '%.*s'";
    ERROR_NAME[-OB_ERR_WRONG_FIELD_WITH_GROUP] = "OB_ERR_WRONG_FIELD_WITH_GROUP";
    MYSQL_ERRNO[-OB_ERR_WRONG_FIELD_WITH_GROUP] = ER_WRONG_FIELD_WITH_GROUP;
    SQLSTATE[-OB_ERR_WRONG_FIELD_WITH_GROUP] = "42000";
    STR_ERROR[-OB_ERR_WRONG_FIELD_WITH_GROUP] = "column is not in GROUP BY";
    STR_USER_ERROR[-OB_ERR_WRONG_FIELD_WITH_GROUP] = "\'%.*s\' is not in GROUP BY";
    ORACLE_ERRNO[-OB_ERR_WRONG_FIELD_WITH_GROUP] = 979;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_FIELD_WITH_GROUP] = "ORA-00979: not a GROUP BY expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_FIELD_WITH_GROUP] = "ORA-00979: \'%.*s\' not a GROUP BY expression";
    ERROR_NAME[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] = "OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS";
    MYSQL_ERRNO[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] = ER_CANT_CHANGE_TX_CHARACTERISTICS;
    SQLSTATE[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] = "25001";
    STR_ERROR[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] =
        "Transaction characteristics can't be changed while a transaction is in progress";
    STR_USER_ERROR[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] =
        "Transaction characteristics can't be changed while a transaction is in progress";
    ORACLE_ERRNO[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] = 1453;
    ORACLE_STR_ERROR[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] =
        "ORA-01453: SET TRANSACTION must be first statement of transaction";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS] =
        "ORA-01453: SET TRANSACTION must be first statement of transaction";
    ERROR_NAME[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] = "OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION";
    MYSQL_ERRNO[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] = ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION;
    SQLSTATE[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] = "25006";
    STR_ERROR[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] = "Cannot execute statement in a READ ONLY transaction.";
    STR_USER_ERROR[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] =
        "Cannot execute statement in a READ ONLY transaction.";
    ORACLE_ERRNO[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] =
        "ORA-00600: internal error code, arguments: -5220, Cannot execute statement in a READ ONLY transaction.";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION] =
        "ORA-00600: internal error code, arguments: -5220, Cannot execute statement in a READ ONLY transaction.";
    ERROR_NAME[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] = "OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS";
    MYSQL_ERRNO[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] = ER_MIX_OF_GROUP_FUNC_AND_FIELDS;
    SQLSTATE[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] = "42000";
    STR_ERROR[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] = "Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no GROUP "
                                                      "columns is illegal if there is no GROUP BY clause";
    STR_USER_ERROR[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] = "Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no "
                                                           "GROUP columns is illegal if there is no GROUP BY clause";
    ORACLE_ERRNO[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] =
        "ORA-00600: internal error code, arguments: -5221, Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no "
        "GROUP columns is illegal if there is no GROUP BY clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS] =
        "ORA-00600: internal error code, arguments: -5221, Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no "
        "GROUP columns is illegal if there is no GROUP BY clause";
    ERROR_NAME[-OB_ERR_TRUNCATED_WRONG_VALUE] = "OB_ERR_TRUNCATED_WRONG_VALUE";
    MYSQL_ERRNO[-OB_ERR_TRUNCATED_WRONG_VALUE] = ER_TRUNCATED_WRONG_VALUE;
    SQLSTATE[-OB_ERR_TRUNCATED_WRONG_VALUE] = "22007";
    STR_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE] = "Incorrect value";
    STR_USER_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE] = "Truncated incorrect %.*s value: '%.*s'";
    ORACLE_ERRNO[-OB_ERR_TRUNCATED_WRONG_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE] =
        "ORA-00600: internal error code, arguments: -5222, Incorrect value";
    ORACLE_STR_USER_ERROR[-OB_ERR_TRUNCATED_WRONG_VALUE] =
        "ORA-00600: internal error code, arguments: -5222, Truncated incorrect %.*s value: '%.*s'";
    ERROR_NAME[-OB_ERR_WRONG_IDENT_NAME] = "OB_ERR_WRONG_IDENT_NAME";
    MYSQL_ERRNO[-OB_ERR_WRONG_IDENT_NAME] = -1;
    SQLSTATE[-OB_ERR_WRONG_IDENT_NAME] = "42000";
    STR_ERROR[-OB_ERR_WRONG_IDENT_NAME] = "wrong ident name";
    STR_USER_ERROR[-OB_ERR_WRONG_IDENT_NAME] = "wrong ident name";
    ORACLE_ERRNO[-OB_ERR_WRONG_IDENT_NAME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_IDENT_NAME] = "ORA-00600: internal error code, arguments: -5223, wrong ident name";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_IDENT_NAME] =
        "ORA-00600: internal error code, arguments: -5223, wrong ident name";
    ERROR_NAME[-OB_WRONG_NAME_FOR_INDEX] = "OB_WRONG_NAME_FOR_INDEX";
    MYSQL_ERRNO[-OB_WRONG_NAME_FOR_INDEX] = ER_WRONG_NAME_FOR_INDEX;
    SQLSTATE[-OB_WRONG_NAME_FOR_INDEX] = "42000";
    STR_ERROR[-OB_WRONG_NAME_FOR_INDEX] = "Incorrect index name";
    STR_USER_ERROR[-OB_WRONG_NAME_FOR_INDEX] = "Incorrect index name '%.*s'";
    ORACLE_ERRNO[-OB_WRONG_NAME_FOR_INDEX] = 600;
    ORACLE_STR_ERROR[-OB_WRONG_NAME_FOR_INDEX] =
        "ORA-00600: internal error code, arguments: -5224, Incorrect index name";
    ORACLE_STR_USER_ERROR[-OB_WRONG_NAME_FOR_INDEX] =
        "ORA-00600: internal error code, arguments: -5224, Incorrect index name '%.*s'";
    ERROR_NAME[-OB_ILLEGAL_REFERENCE] = "OB_ILLEGAL_REFERENCE";
    MYSQL_ERRNO[-OB_ILLEGAL_REFERENCE] = ER_ILLEGAL_REFERENCE;
    SQLSTATE[-OB_ILLEGAL_REFERENCE] = "42S22";
    STR_ERROR[-OB_ILLEGAL_REFERENCE] = "Reference not supported (reference to group function)";
    STR_USER_ERROR[-OB_ILLEGAL_REFERENCE] = "Reference '%.*s' not supported (reference to group function)";
    ORACLE_ERRNO[-OB_ILLEGAL_REFERENCE] = 600;
    ORACLE_STR_ERROR[-OB_ILLEGAL_REFERENCE] =
        "ORA-00600: internal error code, arguments: -5225, Reference not supported (reference to group function)";
    ORACLE_STR_USER_ERROR[-OB_ILLEGAL_REFERENCE] = "ORA-00600: internal error code, arguments: -5225, Reference '%.*s' "
                                                   "not supported (reference to group function)";
    ERROR_NAME[-OB_REACH_MEMORY_LIMIT] = "OB_REACH_MEMORY_LIMIT";
    MYSQL_ERRNO[-OB_REACH_MEMORY_LIMIT] = -1;
    SQLSTATE[-OB_REACH_MEMORY_LIMIT] = "42000";
    STR_ERROR[-OB_REACH_MEMORY_LIMIT] = "plan cache memory used reach the high water mark.";
    STR_USER_ERROR[-OB_REACH_MEMORY_LIMIT] = "plan cache memory used reach the high water mark.";
    ORACLE_ERRNO[-OB_REACH_MEMORY_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_REACH_MEMORY_LIMIT] =
        "ORA-00600: internal error code, arguments: -5226, plan cache memory used reach the high water mark.";
    ORACLE_STR_USER_ERROR[-OB_REACH_MEMORY_LIMIT] =
        "ORA-00600: internal error code, arguments: -5226, plan cache memory used reach the high water mark.";
    ERROR_NAME[-OB_ERR_PASSWORD_FORMAT] = "OB_ERR_PASSWORD_FORMAT";
    MYSQL_ERRNO[-OB_ERR_PASSWORD_FORMAT] = ER_PASSWORD_FORMAT;
    SQLSTATE[-OB_ERR_PASSWORD_FORMAT] = "42000";
    STR_ERROR[-OB_ERR_PASSWORD_FORMAT] = "The password hash doesn't have the expected format. Check if the correct "
                                         "password algorithm is being used with the PASSWORD() function.";
    STR_USER_ERROR[-OB_ERR_PASSWORD_FORMAT] = "The password hash doesn't have the expected format. Check if the "
                                              "correct password algorithm is being used with the PASSWORD() function.";
    ORACLE_ERRNO[-OB_ERR_PASSWORD_FORMAT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PASSWORD_FORMAT] =
        "ORA-00600: internal error code, arguments: -5227, The password hash doesn't have the expected format. Check "
        "if the correct password algorithm is being used with the PASSWORD() function.";
    ORACLE_STR_USER_ERROR[-OB_ERR_PASSWORD_FORMAT] =
        "ORA-00600: internal error code, arguments: -5227, The password hash doesn't have the expected format. Check "
        "if the correct password algorithm is being used with the PASSWORD() function.";
    ERROR_NAME[-OB_ERR_NON_UPDATABLE_TABLE] = "OB_ERR_NON_UPDATABLE_TABLE";
    MYSQL_ERRNO[-OB_ERR_NON_UPDATABLE_TABLE] = ER_NON_UPDATABLE_TABLE;
    SQLSTATE[-OB_ERR_NON_UPDATABLE_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_NON_UPDATABLE_TABLE] = "The target table is not updatable";
    STR_USER_ERROR[-OB_ERR_NON_UPDATABLE_TABLE] = "The target table %.*s of the %.*s is not updatable";
    ORACLE_ERRNO[-OB_ERR_NON_UPDATABLE_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NON_UPDATABLE_TABLE] =
        "ORA-00600: internal error code, arguments: -5228, The target table is not updatable";
    ORACLE_STR_USER_ERROR[-OB_ERR_NON_UPDATABLE_TABLE] =
        "ORA-00600: internal error code, arguments: -5228, The target table %.*s of the %.*s is not updatable";
    ERROR_NAME[-OB_ERR_WARN_DATA_OUT_OF_RANGE] = "OB_ERR_WARN_DATA_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ERR_WARN_DATA_OUT_OF_RANGE] = ER_WARN_DATA_OUT_OF_RANGE;
    SQLSTATE[-OB_ERR_WARN_DATA_OUT_OF_RANGE] = "22003";
    STR_ERROR[-OB_ERR_WARN_DATA_OUT_OF_RANGE] = "Out of range value for column";
    STR_USER_ERROR[-OB_ERR_WARN_DATA_OUT_OF_RANGE] = "Out of range value for column '%.*s' at row %ld";
    ORACLE_ERRNO[-OB_ERR_WARN_DATA_OUT_OF_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WARN_DATA_OUT_OF_RANGE] =
        "ORA-00600: internal error code, arguments: -5229, Out of range value for column";
    ORACLE_STR_USER_ERROR[-OB_ERR_WARN_DATA_OUT_OF_RANGE] =
        "ORA-00600: internal error code, arguments: -5229, Out of range value for column '%.*s' at row %ld";
    ERROR_NAME[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] = "OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR";
    MYSQL_ERRNO[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] = ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
    SQLSTATE[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] =
        "Constant or random or timezone-dependent expressions in (sub)partitioning function are not allowed";
    STR_USER_ERROR[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] =
        "Constant or random or timezone-dependent expressions in (sub)partitioning function are not allowed";
    ORACLE_ERRNO[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] =
        "ORA-00600: internal error code, arguments: -5230, Constant or random or timezone-dependent expressions in "
        "(sub)partitioning function are not allowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR] =
        "ORA-00600: internal error code, arguments: -5230, Constant or random or timezone-dependent expressions in "
        "(sub)partitioning function are not allowed";
    ERROR_NAME[-OB_ERR_VIEW_INVALID] = "OB_ERR_VIEW_INVALID";
    MYSQL_ERRNO[-OB_ERR_VIEW_INVALID] = ER_VIEW_INVALID;
    SQLSTATE[-OB_ERR_VIEW_INVALID] = "42S22";
    STR_ERROR[-OB_ERR_VIEW_INVALID] = "view invalid";
    STR_USER_ERROR[-OB_ERR_VIEW_INVALID] = "View \'%.*s.%.*s\' references invalid table(s) or column(s) or function(s) "
                                           "or definer/invoker of view lack rights to use them";
    ORACLE_ERRNO[-OB_ERR_VIEW_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VIEW_INVALID] = "ORA-00600: internal error code, arguments: -5231, view invalid";
    ORACLE_STR_USER_ERROR[-OB_ERR_VIEW_INVALID] =
        "ORA-00600: internal error code, arguments: -5231, View \'%.*s.%.*s\' references invalid table(s) or column(s) "
        "or function(s) or definer/invoker of view lack rights to use them";
    ERROR_NAME[-OB_ERR_OPTION_PREVENTS_STATEMENT] = "OB_ERR_OPTION_PREVENTS_STATEMENT";
    MYSQL_ERRNO[-OB_ERR_OPTION_PREVENTS_STATEMENT] = ER_OPTION_PREVENTS_STATEMENT;
    SQLSTATE[-OB_ERR_OPTION_PREVENTS_STATEMENT] = "HY000";
    STR_ERROR[-OB_ERR_OPTION_PREVENTS_STATEMENT] =
        "The MySQL server is running with the --read-only option so it cannot execute this statement";
    STR_USER_ERROR[-OB_ERR_OPTION_PREVENTS_STATEMENT] =
        "The MySQL server is running with the --read-only option so it cannot execute this statement";
    ORACLE_ERRNO[-OB_ERR_OPTION_PREVENTS_STATEMENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OPTION_PREVENTS_STATEMENT] =
        "ORA-00600: internal error code, arguments: -5233, The MySQL server is running with the --read-only option so "
        "it cannot execute this statement";
    ORACLE_STR_USER_ERROR[-OB_ERR_OPTION_PREVENTS_STATEMENT] =
        "ORA-00600: internal error code, arguments: -5233, The MySQL server is running with the --read-only option so "
        "it cannot execute this statement";
    ERROR_NAME[-OB_ERR_DB_READ_ONLY] = "OB_ERR_DB_READ_ONLY";
    MYSQL_ERRNO[-OB_ERR_DB_READ_ONLY] = -1;
    SQLSTATE[-OB_ERR_DB_READ_ONLY] = "HY000";
    STR_ERROR[-OB_ERR_DB_READ_ONLY] = "The database is read only so it cannot execute this statement";
    STR_USER_ERROR[-OB_ERR_DB_READ_ONLY] = "The database \'%.*s\' is read only so it cannot execute this statement";
    ORACLE_ERRNO[-OB_ERR_DB_READ_ONLY] = 16000;
    ORACLE_STR_ERROR[-OB_ERR_DB_READ_ONLY] = "ORA-16000: database open for read-only access";
    ORACLE_STR_USER_ERROR[-OB_ERR_DB_READ_ONLY] = "ORA-16000: database \'%.*s\' open for read-only access";
    ERROR_NAME[-OB_ERR_TABLE_READ_ONLY] = "OB_ERR_TABLE_READ_ONLY";
    MYSQL_ERRNO[-OB_ERR_TABLE_READ_ONLY] = -1;
    SQLSTATE[-OB_ERR_TABLE_READ_ONLY] = "HY000";
    STR_ERROR[-OB_ERR_TABLE_READ_ONLY] = "The table is read only so it cannot execute this statement";
    STR_USER_ERROR[-OB_ERR_TABLE_READ_ONLY] =
        "The table \'%.*s.%.*s\' is read only so it cannot execute this statement";
    ORACLE_ERRNO[-OB_ERR_TABLE_READ_ONLY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TABLE_READ_ONLY] =
        "ORA-00600: internal error code, arguments: -5235, The table is read only so it cannot execute this statement";
    ORACLE_STR_USER_ERROR[-OB_ERR_TABLE_READ_ONLY] = "ORA-00600: internal error code, arguments: -5235, The table "
                                                     "\'%.*s.%.*s\' is read only so it cannot execute this statement";
    ERROR_NAME[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] = "OB_ERR_LOCK_OR_ACTIVE_TRANSACTION";
    MYSQL_ERRNO[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] = ER_LOCK_OR_ACTIVE_TRANSACTION;
    SQLSTATE[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] = "HY000";
    STR_ERROR[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] =
        "Can't execute the given command because you have active locked tables or an active transaction";
    STR_USER_ERROR[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] =
        "Can't execute the given command because you have active locked tables or an active transaction";
    ORACLE_ERRNO[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] =
        "ORA-00600: internal error code, arguments: -5236, Can't execute the given command because you have active "
        "locked tables or an active transaction";
    ORACLE_STR_USER_ERROR[-OB_ERR_LOCK_OR_ACTIVE_TRANSACTION] =
        "ORA-00600: internal error code, arguments: -5236, Can't execute the given command because you have active "
        "locked tables or an active transaction";
    ERROR_NAME[-OB_ERR_SAME_NAME_PARTITION_FIELD] = "OB_ERR_SAME_NAME_PARTITION_FIELD";
    MYSQL_ERRNO[-OB_ERR_SAME_NAME_PARTITION_FIELD] = ER_SAME_NAME_PARTITION_FIELD;
    SQLSTATE[-OB_ERR_SAME_NAME_PARTITION_FIELD] = "HY000";
    STR_ERROR[-OB_ERR_SAME_NAME_PARTITION_FIELD] = "Duplicate partition field name";
    STR_USER_ERROR[-OB_ERR_SAME_NAME_PARTITION_FIELD] = "Duplicate partition field name '%.*s'";
    ORACLE_ERRNO[-OB_ERR_SAME_NAME_PARTITION_FIELD] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SAME_NAME_PARTITION_FIELD] =
        "ORA-00600: internal error code, arguments: -5237, Duplicate partition field name";
    ORACLE_STR_USER_ERROR[-OB_ERR_SAME_NAME_PARTITION_FIELD] =
        "ORA-00600: internal error code, arguments: -5237, Duplicate partition field name '%.*s'";
    ERROR_NAME[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] = "OB_ERR_TABLENAME_NOT_ALLOWED_HERE";
    MYSQL_ERRNO[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] = ER_TABLENAME_NOT_ALLOWED_HERE;
    SQLSTATE[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] = "42000";
    STR_ERROR[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] =
        "Table from one of the SELECTs cannot be used in global ORDER clause";
    STR_USER_ERROR[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] =
        "Table \'%.*s\' from one of the SELECTs cannot be used in global ORDER clause";
    ORACLE_ERRNO[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] =
        "ORA-00600: internal error code, arguments: -5238, Table from one of the SELECTs cannot be used in global "
        "ORDER clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_TABLENAME_NOT_ALLOWED_HERE] =
        "ORA-00600: internal error code, arguments: -5238, Table \'%.*s\' from one of the SELECTs cannot be used in "
        "global ORDER clause";
    ERROR_NAME[-OB_ERR_VIEW_RECURSIVE] = "OB_ERR_VIEW_RECURSIVE";
    MYSQL_ERRNO[-OB_ERR_VIEW_RECURSIVE] = ER_VIEW_RECURSIVE;
    SQLSTATE[-OB_ERR_VIEW_RECURSIVE] = "42S02";
    STR_ERROR[-OB_ERR_VIEW_RECURSIVE] = "view contains recursion";
    STR_USER_ERROR[-OB_ERR_VIEW_RECURSIVE] = "\'%.*s.%.*s\' contains view recursion";
    ORACLE_ERRNO[-OB_ERR_VIEW_RECURSIVE] = 1731;
    ORACLE_STR_ERROR[-OB_ERR_VIEW_RECURSIVE] = "ORA-01731: circular view definition encountered";
    ORACLE_STR_USER_ERROR[-OB_ERR_VIEW_RECURSIVE] = "ORA-01731: view '%.*s.%.*s' encounters circular definition";
    ERROR_NAME[-OB_ERR_QUALIFIER] = "OB_ERR_QUALIFIER";
    MYSQL_ERRNO[-OB_ERR_QUALIFIER] = -1;
    SQLSTATE[-OB_ERR_QUALIFIER] = "HY000";
    STR_ERROR[-OB_ERR_QUALIFIER] = "Column part of USING clause cannot have qualifier";
    STR_USER_ERROR[-OB_ERR_QUALIFIER] = "Column part of USING clause cannot have qualifier";
    ORACLE_ERRNO[-OB_ERR_QUALIFIER] = 600;
    ORACLE_STR_ERROR[-OB_ERR_QUALIFIER] =
        "ORA-00600: internal error code, arguments: -5240, Column part of USING clause cannot have qualifier";
    ORACLE_STR_USER_ERROR[-OB_ERR_QUALIFIER] =
        "ORA-00600: internal error code, arguments: -5240, Column part of USING clause cannot have qualifier";
    ERROR_NAME[-OB_ERR_WRONG_VALUE] = "OB_ERR_WRONG_VALUE";
    MYSQL_ERRNO[-OB_ERR_WRONG_VALUE] = ER_WRONG_VALUE;
    SQLSTATE[-OB_ERR_WRONG_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_WRONG_VALUE] = "Incorrect value";
    STR_USER_ERROR[-OB_ERR_WRONG_VALUE] = "Incorrect %s value: '%s'";
    ORACLE_ERRNO[-OB_ERR_WRONG_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_VALUE] = "ORA-00600: internal error code, arguments: -5241, Incorrect value";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_VALUE] =
        "ORA-00600: internal error code, arguments: -5241, Incorrect %s value: '%s'";
    ERROR_NAME[-OB_ERR_VIEW_WRONG_LIST] = "OB_ERR_VIEW_WRONG_LIST";
    MYSQL_ERRNO[-OB_ERR_VIEW_WRONG_LIST] = ER_VIEW_WRONG_LIST;
    SQLSTATE[-OB_ERR_VIEW_WRONG_LIST] = "HY000";
    STR_ERROR[-OB_ERR_VIEW_WRONG_LIST] = "View's SELECT and view's field list have different column counts";
    STR_USER_ERROR[-OB_ERR_VIEW_WRONG_LIST] = "View's SELECT and view's field list have different column counts";
    ORACLE_ERRNO[-OB_ERR_VIEW_WRONG_LIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VIEW_WRONG_LIST] = "ORA-00600: internal error code, arguments: -5242, View's SELECT and "
                                                "view's field list have different column counts";
    ORACLE_STR_USER_ERROR[-OB_ERR_VIEW_WRONG_LIST] = "ORA-00600: internal error code, arguments: -5242, View's SELECT "
                                                     "and view's field list have different column counts";
    ERROR_NAME[-OB_SYS_VARS_MAYBE_DIFF_VERSION] = "OB_SYS_VARS_MAYBE_DIFF_VERSION";
    MYSQL_ERRNO[-OB_SYS_VARS_MAYBE_DIFF_VERSION] = -1;
    SQLSTATE[-OB_SYS_VARS_MAYBE_DIFF_VERSION] = "HY000";
    STR_ERROR[-OB_SYS_VARS_MAYBE_DIFF_VERSION] = "system variables' version maybe different";
    STR_USER_ERROR[-OB_SYS_VARS_MAYBE_DIFF_VERSION] = "system variables' version maybe different";
    ORACLE_ERRNO[-OB_SYS_VARS_MAYBE_DIFF_VERSION] = 600;
    ORACLE_STR_ERROR[-OB_SYS_VARS_MAYBE_DIFF_VERSION] =
        "ORA-00600: internal error code, arguments: -5243, system variables' version maybe different";
    ORACLE_STR_USER_ERROR[-OB_SYS_VARS_MAYBE_DIFF_VERSION] =
        "ORA-00600: internal error code, arguments: -5243, system variables' version maybe different";
    ERROR_NAME[-OB_ERR_AUTO_INCREMENT_CONFLICT] = "OB_ERR_AUTO_INCREMENT_CONFLICT";
    MYSQL_ERRNO[-OB_ERR_AUTO_INCREMENT_CONFLICT] = ER_AUTO_INCREMENT_CONFLICT;
    SQLSTATE[-OB_ERR_AUTO_INCREMENT_CONFLICT] = "HY000";
    STR_ERROR[-OB_ERR_AUTO_INCREMENT_CONFLICT] =
        "Auto-increment value in UPDATE conflicts with internally generated values";
    STR_USER_ERROR[-OB_ERR_AUTO_INCREMENT_CONFLICT] =
        "Auto-increment value in UPDATE conflicts with internally generated values";
    ORACLE_ERRNO[-OB_ERR_AUTO_INCREMENT_CONFLICT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_AUTO_INCREMENT_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5244, Auto-increment value in UPDATE conflicts with internally "
        "generated values";
    ORACLE_STR_USER_ERROR[-OB_ERR_AUTO_INCREMENT_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5244, Auto-increment value in UPDATE conflicts with internally "
        "generated values";
    ERROR_NAME[-OB_ERR_TASK_SKIPPED] = "OB_ERR_TASK_SKIPPED";
    MYSQL_ERRNO[-OB_ERR_TASK_SKIPPED] = -1;
    SQLSTATE[-OB_ERR_TASK_SKIPPED] = "HY000";
    STR_ERROR[-OB_ERR_TASK_SKIPPED] = "some tasks are skipped";
    STR_USER_ERROR[-OB_ERR_TASK_SKIPPED] =
        "some tasks are skipped, skipped server addr is '%s', the orginal error code is %d";
    ORACLE_ERRNO[-OB_ERR_TASK_SKIPPED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TASK_SKIPPED] = "ORA-00600: internal error code, arguments: -5245, some tasks are skipped";
    ORACLE_STR_USER_ERROR[-OB_ERR_TASK_SKIPPED] = "ORA-00600: internal error code, arguments: -5245, some tasks are "
                                                  "skipped, skipped server addr is '%s', the orginal error code is %d";
    ERROR_NAME[-OB_ERR_NAME_BECOMES_EMPTY] = "OB_ERR_NAME_BECOMES_EMPTY";
    MYSQL_ERRNO[-OB_ERR_NAME_BECOMES_EMPTY] = ER_NAME_BECOMES_EMPTY;
    SQLSTATE[-OB_ERR_NAME_BECOMES_EMPTY] = "HY000";
    STR_ERROR[-OB_ERR_NAME_BECOMES_EMPTY] = "Name has become ''";
    STR_USER_ERROR[-OB_ERR_NAME_BECOMES_EMPTY] = "Name \'%.*s\' has become ''";
    ORACLE_ERRNO[-OB_ERR_NAME_BECOMES_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NAME_BECOMES_EMPTY] =
        "ORA-00600: internal error code, arguments: -5246, Name has become ''";
    ORACLE_STR_USER_ERROR[-OB_ERR_NAME_BECOMES_EMPTY] =
        "ORA-00600: internal error code, arguments: -5246, Name \'%.*s\' has become ''";
    ERROR_NAME[-OB_ERR_REMOVED_SPACES] = "OB_ERR_REMOVED_SPACES";
    MYSQL_ERRNO[-OB_ERR_REMOVED_SPACES] = ER_REMOVED_SPACES;
    SQLSTATE[-OB_ERR_REMOVED_SPACES] = "HY000";
    STR_ERROR[-OB_ERR_REMOVED_SPACES] = "Leading spaces are removed from name ";
    STR_USER_ERROR[-OB_ERR_REMOVED_SPACES] = "Leading spaces are removed from name \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_REMOVED_SPACES] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REMOVED_SPACES] =
        "ORA-00600: internal error code, arguments: -5247, Leading spaces are removed from name ";
    ORACLE_STR_USER_ERROR[-OB_ERR_REMOVED_SPACES] =
        "ORA-00600: internal error code, arguments: -5247, Leading spaces are removed from name \'%.*s\'";
    ERROR_NAME[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] = "OB_WARN_ADD_AUTOINCREMENT_COLUMN";
    MYSQL_ERRNO[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] = -1;
    SQLSTATE[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] = "HY000";
    STR_ERROR[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] = "Alter table add auto_increment column is dangerous";
    STR_USER_ERROR[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] =
        "Alter table add auto_increment column is dangerous, table_name=\'%.*s\', column_name=\'%s\'";
    ORACLE_ERRNO[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] =
        "ORA-00600: internal error code, arguments: -5248, Alter table add auto_increment column is dangerous";
    ORACLE_STR_USER_ERROR[-OB_WARN_ADD_AUTOINCREMENT_COLUMN] =
        "ORA-00600: internal error code, arguments: -5248, Alter table add auto_increment column is dangerous, "
        "table_name=\'%.*s\', column_name=\'%s\'";
    ERROR_NAME[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] = "OB_WARN_CHAMGE_NULL_ATTRIBUTE";
    MYSQL_ERRNO[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] = -1;
    SQLSTATE[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] = "HY000";
    STR_ERROR[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] = "Alter table change nullable column to not nullable is dangerous";
    STR_USER_ERROR[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] =
        "Alter table change nullable column to not nullable is dangerous, table_name=\'%.*s\', column_name=\'%.*s\'";
    ORACLE_ERRNO[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] = 600;
    ORACLE_STR_ERROR[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] = "ORA-00600: internal error code, arguments: -5249, Alter table "
                                                       "change nullable column to not nullable is dangerous";
    ORACLE_STR_USER_ERROR[-OB_WARN_CHAMGE_NULL_ATTRIBUTE] =
        "ORA-00600: internal error code, arguments: -5249, Alter table change nullable column to not nullable is "
        "dangerous, table_name=\'%.*s\', column_name=\'%.*s\'";
    ERROR_NAME[-OB_ERR_INVALID_CHARACTER_STRING] = "OB_ERR_INVALID_CHARACTER_STRING";
    MYSQL_ERRNO[-OB_ERR_INVALID_CHARACTER_STRING] = ER_INVALID_CHARACTER_STRING;
    SQLSTATE[-OB_ERR_INVALID_CHARACTER_STRING] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_CHARACTER_STRING] = "Invalid character string";
    STR_USER_ERROR[-OB_ERR_INVALID_CHARACTER_STRING] = "Invalid %.*s character string: \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_INVALID_CHARACTER_STRING] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_CHARACTER_STRING] =
        "ORA-00600: internal error code, arguments: -5250, Invalid character string";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_CHARACTER_STRING] =
        "ORA-00600: internal error code, arguments: -5250, Invalid %.*s character string: \'%.*s\'";
    ERROR_NAME[-OB_ERR_KILL_DENIED] = "OB_ERR_KILL_DENIED";
    MYSQL_ERRNO[-OB_ERR_KILL_DENIED] = ER_KILL_DENIED_ERROR;
    SQLSTATE[-OB_ERR_KILL_DENIED] = "HY000";
    STR_ERROR[-OB_ERR_KILL_DENIED] = "You are not owner of thread";
    STR_USER_ERROR[-OB_ERR_KILL_DENIED] = "You are not owner of thread %lu";
    ORACLE_ERRNO[-OB_ERR_KILL_DENIED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_KILL_DENIED] =
        "ORA-00600: internal error code, arguments: -5251, You are not owner of thread";
    ORACLE_STR_USER_ERROR[-OB_ERR_KILL_DENIED] =
        "ORA-00600: internal error code, arguments: -5251, You are not owner of thread %lu";
    ERROR_NAME[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] = "OB_ERR_COLUMN_DEFINITION_AMBIGUOUS";
    MYSQL_ERRNO[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] = -1;
    SQLSTATE[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] = "HY000";
    STR_ERROR[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] =
        "Column definition is ambiguous. Column has both NULL and NOT NULL attributes";
    STR_USER_ERROR[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] =
        "Column \'%.*s\' definition is ambiguous. Column has both NULL and NOT NULL attributes";
    ORACLE_ERRNO[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] =
        "ORA-00600: internal error code, arguments: -5252, Column definition is ambiguous. Column has both NULL and "
        "NOT NULL attributes";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_DEFINITION_AMBIGUOUS] =
        "ORA-00600: internal error code, arguments: -5252, Column \'%.*s\' definition is ambiguous. Column has both "
        "NULL and NOT NULL attributes";
    ERROR_NAME[-OB_ERR_EMPTY_QUERY] = "OB_ERR_EMPTY_QUERY";
    MYSQL_ERRNO[-OB_ERR_EMPTY_QUERY] = ER_EMPTY_QUERY;
    SQLSTATE[-OB_ERR_EMPTY_QUERY] = "42000";
    STR_ERROR[-OB_ERR_EMPTY_QUERY] = "Query was empty";
    STR_USER_ERROR[-OB_ERR_EMPTY_QUERY] = "Query was empty";
    ORACLE_ERRNO[-OB_ERR_EMPTY_QUERY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_EMPTY_QUERY] = "ORA-00600: internal error code, arguments: -5253, Query was empty";
    ORACLE_STR_USER_ERROR[-OB_ERR_EMPTY_QUERY] = "ORA-00600: internal error code, arguments: -5253, Query was empty";
    ERROR_NAME[-OB_ERR_CUT_VALUE_GROUP_CONCAT] = "OB_ERR_CUT_VALUE_GROUP_CONCAT";
    MYSQL_ERRNO[-OB_ERR_CUT_VALUE_GROUP_CONCAT] = ER_CUT_VALUE_GROUP_CONCAT;
    SQLSTATE[-OB_ERR_CUT_VALUE_GROUP_CONCAT] = "42000";
    STR_ERROR[-OB_ERR_CUT_VALUE_GROUP_CONCAT] = "Row was cut by GROUP_CONCAT()";
    STR_USER_ERROR[-OB_ERR_CUT_VALUE_GROUP_CONCAT] = "Row %ld was cut by GROUP_CONCAT()";
    ORACLE_ERRNO[-OB_ERR_CUT_VALUE_GROUP_CONCAT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CUT_VALUE_GROUP_CONCAT] =
        "ORA-00600: internal error code, arguments: -5254, Row was cut by GROUP_CONCAT()";
    ORACLE_STR_USER_ERROR[-OB_ERR_CUT_VALUE_GROUP_CONCAT] =
        "ORA-00600: internal error code, arguments: -5254, Row %ld was cut by GROUP_CONCAT()";
    ERROR_NAME[-OB_ERR_FIELD_NOT_FOUND_PART] = "OB_ERR_FIELD_NOT_FOUND_PART";
    MYSQL_ERRNO[-OB_ERR_FIELD_NOT_FOUND_PART] = ER_FIELD_NOT_FOUND_PART_ERROR;
    SQLSTATE[-OB_ERR_FIELD_NOT_FOUND_PART] = "HY000";
    STR_ERROR[-OB_ERR_FIELD_NOT_FOUND_PART] = "Field in list of fields for partition function not found in table";
    STR_USER_ERROR[-OB_ERR_FIELD_NOT_FOUND_PART] = "Field in list of fields for partition function not found in table";
    ORACLE_ERRNO[-OB_ERR_FIELD_NOT_FOUND_PART] = 600;
    ORACLE_STR_ERROR[-OB_ERR_FIELD_NOT_FOUND_PART] = "ORA-00600: internal error code, arguments: -5255, Field in list "
                                                     "of fields for partition function not found in table";
    ORACLE_STR_USER_ERROR[-OB_ERR_FIELD_NOT_FOUND_PART] = "ORA-00600: internal error code, arguments: -5255, Field in "
                                                          "list of fields for partition function not found in table";
    ERROR_NAME[-OB_ERR_PRIMARY_CANT_HAVE_NULL] = "OB_ERR_PRIMARY_CANT_HAVE_NULL";
    MYSQL_ERRNO[-OB_ERR_PRIMARY_CANT_HAVE_NULL] = ER_PRIMARY_CANT_HAVE_NULL;
    SQLSTATE[-OB_ERR_PRIMARY_CANT_HAVE_NULL] = "42000";
    STR_ERROR[-OB_ERR_PRIMARY_CANT_HAVE_NULL] =
        "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead";
    STR_USER_ERROR[-OB_ERR_PRIMARY_CANT_HAVE_NULL] =
        "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead";
    ORACLE_ERRNO[-OB_ERR_PRIMARY_CANT_HAVE_NULL] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PRIMARY_CANT_HAVE_NULL] =
        "ORA-00600: internal error code, arguments: -5256, All parts of a PRIMARY KEY must be NOT NULL; if you need "
        "NULL in a key, use UNIQUE instead";
    ORACLE_STR_USER_ERROR[-OB_ERR_PRIMARY_CANT_HAVE_NULL] =
        "ORA-00600: internal error code, arguments: -5256, All parts of a PRIMARY KEY must be NOT NULL; if you need "
        "NULL in a key, use UNIQUE instead";
    ERROR_NAME[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] = "OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR";
    MYSQL_ERRNO[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] = ER_PARTITION_FUNC_NOT_ALLOWED_ERROR;
    SQLSTATE[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] = "The PARTITION function returns the wrong type";
    STR_USER_ERROR[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] = "The PARTITION function returns the wrong type";
    ORACLE_ERRNO[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] =
        "ORA-00600: internal error code, arguments: -5257, The PARTITION function returns the wrong type";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR] =
        "ORA-00600: internal error code, arguments: -5257, The PARTITION function returns the wrong type";
    ERROR_NAME[-OB_ERR_INVALID_BLOCK_SIZE] = "OB_ERR_INVALID_BLOCK_SIZE";
    MYSQL_ERRNO[-OB_ERR_INVALID_BLOCK_SIZE] = -1;
    SQLSTATE[-OB_ERR_INVALID_BLOCK_SIZE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_BLOCK_SIZE] = "Invalid block size, block size should between 1024 and 1048576";
    STR_USER_ERROR[-OB_ERR_INVALID_BLOCK_SIZE] = "Invalid block size, block size should between 1024 and 1048576";
    ORACLE_ERRNO[-OB_ERR_INVALID_BLOCK_SIZE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_BLOCK_SIZE] = "ORA-00600: internal error code, arguments: -5258, Invalid block "
                                                   "size, block size should between 1024 and 1048576";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_BLOCK_SIZE] = "ORA-00600: internal error code, arguments: -5258, Invalid "
                                                        "block size, block size should between 1024 and 1048576";
    ERROR_NAME[-OB_ERR_UNKNOWN_STORAGE_ENGINE] = "OB_ERR_UNKNOWN_STORAGE_ENGINE";
    MYSQL_ERRNO[-OB_ERR_UNKNOWN_STORAGE_ENGINE] = ER_UNKNOWN_STORAGE_ENGINE;
    SQLSTATE[-OB_ERR_UNKNOWN_STORAGE_ENGINE] = "42000";
    STR_ERROR[-OB_ERR_UNKNOWN_STORAGE_ENGINE] = "Unknown storage engine";
    STR_USER_ERROR[-OB_ERR_UNKNOWN_STORAGE_ENGINE] = "Unknown storage engine \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_UNKNOWN_STORAGE_ENGINE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNKNOWN_STORAGE_ENGINE] =
        "ORA-00600: internal error code, arguments: -5259, Unknown storage engine";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNKNOWN_STORAGE_ENGINE] =
        "ORA-00600: internal error code, arguments: -5259, Unknown storage engine \'%.*s\'";
    ERROR_NAME[-OB_ERR_TENANT_IS_LOCKED] = "OB_ERR_TENANT_IS_LOCKED";
    MYSQL_ERRNO[-OB_ERR_TENANT_IS_LOCKED] = -1;
    SQLSTATE[-OB_ERR_TENANT_IS_LOCKED] = "HY000";
    STR_ERROR[-OB_ERR_TENANT_IS_LOCKED] = "Tenant is locked";
    STR_USER_ERROR[-OB_ERR_TENANT_IS_LOCKED] = "Tenant \'%.*s\' is locked";
    ORACLE_ERRNO[-OB_ERR_TENANT_IS_LOCKED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TENANT_IS_LOCKED] = "ORA-00600: internal error code, arguments: -5260, Tenant is locked";
    ORACLE_STR_USER_ERROR[-OB_ERR_TENANT_IS_LOCKED] =
        "ORA-00600: internal error code, arguments: -5260, Tenant \'%.*s\' is locked";
    ERROR_NAME[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = "OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF";
    MYSQL_ERRNO[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
    SQLSTATE[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = "HY000";
    STR_ERROR[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "A UNIQUE INDEX/PRIMARY KEY must include all columns in the table's partitioning function";
    STR_USER_ERROR[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "A %s must include all columns in the table's partitioning function";
    ORACLE_ERRNO[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = 600;
    ORACLE_STR_ERROR[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "ORA-00600: internal error code, arguments: -5261, A UNIQUE INDEX/PRIMARY KEY must include all columns in the "
        "table's partitioning function";
    ORACLE_STR_USER_ERROR[-OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "ORA-00600: internal error code, arguments: -5261, A %s must include all columns in the table's partitioning "
        "function";
    ERROR_NAME[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] = "OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] = ER_PARTITION_FUNCTION_IS_NOT_ALLOWED;
    SQLSTATE[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] = "This partition function is not allowed";
    STR_USER_ERROR[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] = "This partition function is not allowed";
    ORACLE_ERRNO[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -5262, This partition function is not allowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -5262, This partition function is not allowed";
    ERROR_NAME[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] = "OB_ERR_AGGREGATE_ORDER_FOR_UNION";
    MYSQL_ERRNO[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] = ER_AGGREGATE_ORDER_FOR_UNION;
    SQLSTATE[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] = "HY000";
    STR_ERROR[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] = "aggregate order for union";
    STR_USER_ERROR[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] =
        "Expression #%d of ORDER BY contains aggregate function and applies to a UNION";
    ORACLE_ERRNO[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] =
        "ORA-00600: internal error code, arguments: -5263, aggregate order for union";
    ORACLE_STR_USER_ERROR[-OB_ERR_AGGREGATE_ORDER_FOR_UNION] =
        "ORA-00600: internal error code, arguments: -5263, Expression #%d of ORDER BY contains aggregate function and "
        "applies to a UNION";
    ERROR_NAME[-OB_ERR_OUTLINE_EXIST] = "OB_ERR_OUTLINE_EXIST";
    MYSQL_ERRNO[-OB_ERR_OUTLINE_EXIST] = -1;
    SQLSTATE[-OB_ERR_OUTLINE_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_OUTLINE_EXIST] = "Outline exists";
    STR_USER_ERROR[-OB_ERR_OUTLINE_EXIST] = "Outline '%.*s' already exists";
    ORACLE_ERRNO[-OB_ERR_OUTLINE_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OUTLINE_EXIST] = "ORA-00600: internal error code, arguments: -5264, Outline exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTLINE_EXIST] =
        "ORA-00600: internal error code, arguments: -5264, Outline '%.*s' already exists";
    ERROR_NAME[-OB_OUTLINE_NOT_EXIST] = "OB_OUTLINE_NOT_EXIST";
    MYSQL_ERRNO[-OB_OUTLINE_NOT_EXIST] = -1;
    SQLSTATE[-OB_OUTLINE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_OUTLINE_NOT_EXIST] = "Outline not exists";
    STR_USER_ERROR[-OB_OUTLINE_NOT_EXIST] = "Outline \'%.*s.%.*s\' doesn\'t exist";
    ORACLE_ERRNO[-OB_OUTLINE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_OUTLINE_NOT_EXIST] = "ORA-00600: internal error code, arguments: -5265, Outline not exists";
    ORACLE_STR_USER_ERROR[-OB_OUTLINE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5265, Outline \'%.*s.%.*s\' doesn\'t exist";
    ERROR_NAME[-OB_WARN_OPTION_BELOW_LIMIT] = "OB_WARN_OPTION_BELOW_LIMIT";
    MYSQL_ERRNO[-OB_WARN_OPTION_BELOW_LIMIT] = WARN_OPTION_BELOW_LIMIT;
    SQLSTATE[-OB_WARN_OPTION_BELOW_LIMIT] = "HY000";
    STR_ERROR[-OB_WARN_OPTION_BELOW_LIMIT] = "The value should be no less than the limit";
    STR_USER_ERROR[-OB_WARN_OPTION_BELOW_LIMIT] = "The value of \'%s\' should be no less than the value of \'%s\'";
    ORACLE_ERRNO[-OB_WARN_OPTION_BELOW_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_WARN_OPTION_BELOW_LIMIT] =
        "ORA-00600: internal error code, arguments: -5266, The value should be no less than the limit";
    ORACLE_STR_USER_ERROR[-OB_WARN_OPTION_BELOW_LIMIT] = "ORA-00600: internal error code, arguments: -5266, The value "
                                                         "of \'%s\' should be no less than the value of \'%s\'";
    ERROR_NAME[-OB_INVALID_OUTLINE] = "OB_INVALID_OUTLINE";
    MYSQL_ERRNO[-OB_INVALID_OUTLINE] = -1;
    SQLSTATE[-OB_INVALID_OUTLINE] = "HY000";
    STR_ERROR[-OB_INVALID_OUTLINE] = "invalid outline";
    STR_USER_ERROR[-OB_INVALID_OUTLINE] = "invalid outline ,error info:%s";
    ORACLE_ERRNO[-OB_INVALID_OUTLINE] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_OUTLINE] = "ORA-00600: internal error code, arguments: -5267, invalid outline";
    ORACLE_STR_USER_ERROR[-OB_INVALID_OUTLINE] =
        "ORA-00600: internal error code, arguments: -5267, invalid outline ,error info:%s";
    ERROR_NAME[-OB_REACH_MAX_CONCURRENT_NUM] = "OB_REACH_MAX_CONCURRENT_NUM";
    MYSQL_ERRNO[-OB_REACH_MAX_CONCURRENT_NUM] = -1;
    SQLSTATE[-OB_REACH_MAX_CONCURRENT_NUM] = "HY000";
    STR_ERROR[-OB_REACH_MAX_CONCURRENT_NUM] = "SQL reach max concurrent num";
    STR_USER_ERROR[-OB_REACH_MAX_CONCURRENT_NUM] = "SQL reach max concurrent num %ld";
    ORACLE_ERRNO[-OB_REACH_MAX_CONCURRENT_NUM] = 600;
    ORACLE_STR_ERROR[-OB_REACH_MAX_CONCURRENT_NUM] =
        "ORA-00600: internal error code, arguments: -5268, SQL reach max concurrent num";
    ORACLE_STR_USER_ERROR[-OB_REACH_MAX_CONCURRENT_NUM] =
        "ORA-00600: internal error code, arguments: -5268, SQL reach max concurrent num %ld";
    ERROR_NAME[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] = "OB_ERR_OPERATION_ON_RECYCLE_OBJECT";
    MYSQL_ERRNO[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] = -1;
    SQLSTATE[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] = "HY000";
    STR_ERROR[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] = "can not perform DDL/DML over objects in Recycle Bin";
    STR_USER_ERROR[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] = "can not perform DDL/DML over objects in Recycle Bin";
    ORACLE_ERRNO[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] =
        "ORA-00600: internal error code, arguments: -5269, can not perform DDL/DML over objects in Recycle Bin";
    ORACLE_STR_USER_ERROR[-OB_ERR_OPERATION_ON_RECYCLE_OBJECT] =
        "ORA-00600: internal error code, arguments: -5269, can not perform DDL/DML over objects in Recycle Bin";
    ERROR_NAME[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] = "OB_ERR_OBJECT_NOT_IN_RECYCLEBIN";
    MYSQL_ERRNO[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] = -1;
    SQLSTATE[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] = "HY000";
    STR_ERROR[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] = "object not in RECYCLE BIN";
    STR_USER_ERROR[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] = "object not in RECYCLE BIN";
    ORACLE_ERRNO[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] =
        "ORA-00600: internal error code, arguments: -5270, object not in RECYCLE BIN";
    ORACLE_STR_USER_ERROR[-OB_ERR_OBJECT_NOT_IN_RECYCLEBIN] =
        "ORA-00600: internal error code, arguments: -5270, object not in RECYCLE BIN";
    ERROR_NAME[-OB_ERR_CON_COUNT_ERROR] = "OB_ERR_CON_COUNT_ERROR";
    MYSQL_ERRNO[-OB_ERR_CON_COUNT_ERROR] = ER_CON_COUNT_ERROR;
    SQLSTATE[-OB_ERR_CON_COUNT_ERROR] = "08004";
    STR_ERROR[-OB_ERR_CON_COUNT_ERROR] = "Too many connections";
    STR_USER_ERROR[-OB_ERR_CON_COUNT_ERROR] = "Too many connections";
    ORACLE_ERRNO[-OB_ERR_CON_COUNT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CON_COUNT_ERROR] =
        "ORA-00600: internal error code, arguments: -5271, Too many connections";
    ORACLE_STR_USER_ERROR[-OB_ERR_CON_COUNT_ERROR] =
        "ORA-00600: internal error code, arguments: -5271, Too many connections";
    ERROR_NAME[-OB_ERR_OUTLINE_CONTENT_EXIST] = "OB_ERR_OUTLINE_CONTENT_EXIST";
    MYSQL_ERRNO[-OB_ERR_OUTLINE_CONTENT_EXIST] = -1;
    SQLSTATE[-OB_ERR_OUTLINE_CONTENT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_OUTLINE_CONTENT_EXIST] = "Outline content already exists when added";
    STR_USER_ERROR[-OB_ERR_OUTLINE_CONTENT_EXIST] =
        "Outline content '%.*s' of outline '%.*s' already exists when added";
    ORACLE_ERRNO[-OB_ERR_OUTLINE_CONTENT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OUTLINE_CONTENT_EXIST] =
        "ORA-00600: internal error code, arguments: -5272, Outline content already exists when added";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTLINE_CONTENT_EXIST] = "ORA-00600: internal error code, arguments: -5272, Outline "
                                                           "content '%.*s' of outline '%.*s' already exists when added";
    ERROR_NAME[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] = "OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST";
    MYSQL_ERRNO[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] = -1;
    SQLSTATE[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] = "Max concurrent already exists when added";
    STR_USER_ERROR[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] = "Max concurrent in outline '%.*s' already exists when added";
    ORACLE_ERRNO[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] =
        "ORA-00600: internal error code, arguments: -5273, Max concurrent already exists when added";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST] =
        "ORA-00600: internal error code, arguments: -5273, Max concurrent in outline '%.*s' already exists when added";
    ERROR_NAME[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] = "OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR";
    MYSQL_ERRNO[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] = ER_VALUES_IS_NOT_INT_TYPE_ERROR;
    SQLSTATE[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] = "VALUES value for partition must have type INT";
    STR_USER_ERROR[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] = "VALUES value for partition \'%.*s\' must have type INT";
    ORACLE_ERRNO[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] =
        "ORA-00600: internal error code, arguments: -5274, VALUES value for partition must have type INT";
    ORACLE_STR_USER_ERROR[-OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR] =
        "ORA-00600: internal error code, arguments: -5274, VALUES value for partition \'%.*s\' must have type INT";
    ERROR_NAME[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] = "OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR";
    MYSQL_ERRNO[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] = ER_WRONG_TYPE_COLUMN_VALUE_ERROR;
    SQLSTATE[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] = "Partition column values of incorrect type";
    STR_USER_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] = "Partition column values of incorrect type";
    ORACLE_ERRNO[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] = 14019;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] =
        "ORA-14019: partition bound element must be one of: string, datetime or interval literal, number, or MAXVALUE";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR] =
        "ORA-14019: partition bound element must be one of: string, datetime or interval literal, number, or MAXVALUE";
    ERROR_NAME[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] = "OB_ERR_PARTITION_COLUMN_LIST_ERROR";
    MYSQL_ERRNO[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] = ER_PARTITION_COLUMN_LIST_ERROR;
    SQLSTATE[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] = "Inconsistency in usage of column lists for partitioning";
    STR_USER_ERROR[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] = "Inconsistency in usage of column lists for partitioning";
    ORACLE_ERRNO[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] =
        "ORA-00600: internal error code, arguments: -5276, Inconsistency in usage of column lists for partitioning";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_COLUMN_LIST_ERROR] =
        "ORA-00600: internal error code, arguments: -5276, Inconsistency in usage of column lists for partitioning";
    ERROR_NAME[-OB_ERR_TOO_MANY_VALUES_ERROR] = "OB_ERR_TOO_MANY_VALUES_ERROR";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_VALUES_ERROR] = ER_TOO_MANY_VALUES_ERROR;
    SQLSTATE[-OB_ERR_TOO_MANY_VALUES_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_TOO_MANY_VALUES_ERROR] = "Cannot have more than one value for this type of RANGE partitioning";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_VALUES_ERROR] =
        "Cannot have more than one value for this type of RANGE partitioning";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_VALUES_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_VALUES_ERROR] = "ORA-00600: internal error code, arguments: -5277, Cannot have "
                                                      "more than one value for this type of RANGE partitioning";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_VALUES_ERROR] =
        "ORA-00600: internal error code, arguments: -5277, Cannot have more than one value for this type of RANGE "
        "partitioning";
    ERROR_NAME[-OB_ERR_PARTITION_VALUE_ERROR] = "OB_ERR_PARTITION_VALUE_ERROR";
    MYSQL_ERRNO[-OB_ERR_PARTITION_VALUE_ERROR] = -1;
    SQLSTATE[-OB_ERR_PARTITION_VALUE_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_VALUE_ERROR] = "This partition value with incorrect charset type";
    STR_USER_ERROR[-OB_ERR_PARTITION_VALUE_ERROR] = "This partition value with incorrect charset type";
    ORACLE_ERRNO[-OB_ERR_PARTITION_VALUE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_VALUE_ERROR] =
        "ORA-00600: internal error code, arguments: -5278, This partition value with incorrect charset type";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_VALUE_ERROR] =
        "ORA-00600: internal error code, arguments: -5278, This partition value with incorrect charset type";
    ERROR_NAME[-OB_ERR_PARTITION_INTERVAL_ERROR] = "OB_ERR_PARTITION_INTERVAL_ERROR";
    MYSQL_ERRNO[-OB_ERR_PARTITION_INTERVAL_ERROR] = -1;
    SQLSTATE[-OB_ERR_PARTITION_INTERVAL_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_INTERVAL_ERROR] = "Partition interval must have type INT";
    STR_USER_ERROR[-OB_ERR_PARTITION_INTERVAL_ERROR] = "Partition interval must have type INT";
    ORACLE_ERRNO[-OB_ERR_PARTITION_INTERVAL_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_INTERVAL_ERROR] =
        "ORA-00600: internal error code, arguments: -5279, Partition interval must have type INT";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_INTERVAL_ERROR] =
        "ORA-00600: internal error code, arguments: -5279, Partition interval must have type INT";
    ERROR_NAME[-OB_ERR_SAME_NAME_PARTITION] = "OB_ERR_SAME_NAME_PARTITION";
    MYSQL_ERRNO[-OB_ERR_SAME_NAME_PARTITION] = ER_SAME_NAME_PARTITION;
    SQLSTATE[-OB_ERR_SAME_NAME_PARTITION] = "HY000";
    STR_ERROR[-OB_ERR_SAME_NAME_PARTITION] = "Duplicate partition name";
    STR_USER_ERROR[-OB_ERR_SAME_NAME_PARTITION] = "Duplicate partition name \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_SAME_NAME_PARTITION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SAME_NAME_PARTITION] =
        "ORA-00600: internal error code, arguments: -5280, Duplicate partition name";
    ORACLE_STR_USER_ERROR[-OB_ERR_SAME_NAME_PARTITION] =
        "ORA-00600: internal error code, arguments: -5280, Duplicate partition name \'%.*s\'";
    ERROR_NAME[-OB_ERR_RANGE_NOT_INCREASING_ERROR] = "OB_ERR_RANGE_NOT_INCREASING_ERROR";
    MYSQL_ERRNO[-OB_ERR_RANGE_NOT_INCREASING_ERROR] = ER_RANGE_NOT_INCREASING_ERROR;
    SQLSTATE[-OB_ERR_RANGE_NOT_INCREASING_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_RANGE_NOT_INCREASING_ERROR] =
        "VALUES LESS THAN value must be strictly increasing for each partition";
    STR_USER_ERROR[-OB_ERR_RANGE_NOT_INCREASING_ERROR] =
        "VALUES LESS THAN value must be strictly increasing for each partition%.*s";
    ORACLE_ERRNO[-OB_ERR_RANGE_NOT_INCREASING_ERROR] = 14037;
    ORACLE_STR_ERROR[-OB_ERR_RANGE_NOT_INCREASING_ERROR] = "ORA-14037: partition bound is too high";
    ORACLE_STR_USER_ERROR[-OB_ERR_RANGE_NOT_INCREASING_ERROR] =
        "ORA-14037: partition bound of partition '%.*s' is too high";
    ERROR_NAME[-OB_ERR_PARSE_PARTITION_RANGE] = "OB_ERR_PARSE_PARTITION_RANGE";
    MYSQL_ERRNO[-OB_ERR_PARSE_PARTITION_RANGE] = ER_PARSE_ERROR;
    SQLSTATE[-OB_ERR_PARSE_PARTITION_RANGE] = "42000";
    STR_ERROR[-OB_ERR_PARSE_PARTITION_RANGE] = "Wrong number of partitions defined, mismatch with previous setting";
    STR_USER_ERROR[-OB_ERR_PARSE_PARTITION_RANGE] =
        "Wrong number of partitions defined, mismatch with previous setting";
    ORACLE_ERRNO[-OB_ERR_PARSE_PARTITION_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARSE_PARTITION_RANGE] = "ORA-00600: internal error code, arguments: -5282, Wrong number "
                                                      "of partitions defined, mismatch with previous setting";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARSE_PARTITION_RANGE] =
        "ORA-00600: internal error code, arguments: -5282, Wrong number of partitions defined, mismatch with previous "
        "setting";
    ERROR_NAME[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = "OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF";
    MYSQL_ERRNO[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
    SQLSTATE[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = "HY000";
    STR_ERROR[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "A PRIMARY KEY must include all columns in the table\'s partitioning function";
    STR_USER_ERROR[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "A PRIMARY KEY must include all columns in the table\'s partitioning function";
    ORACLE_ERRNO[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "ORA-00600: internal error code, arguments: -5283, A PRIMARY KEY must include all columns in the table\'s "
        "partitioning function";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF] =
        "ORA-00600: internal error code, arguments: -5283, A PRIMARY KEY must include all columns in the table\'s "
        "partitioning function";
    ERROR_NAME[-OB_NO_PARTITION_FOR_GIVEN_VALUE] = "OB_NO_PARTITION_FOR_GIVEN_VALUE";
    MYSQL_ERRNO[-OB_NO_PARTITION_FOR_GIVEN_VALUE] = ER_NO_PARTITION_FOR_GIVEN_VALUE;
    SQLSTATE[-OB_NO_PARTITION_FOR_GIVEN_VALUE] = "HY000";
    STR_ERROR[-OB_NO_PARTITION_FOR_GIVEN_VALUE] = "Table has no partition for value";
    STR_USER_ERROR[-OB_NO_PARTITION_FOR_GIVEN_VALUE] = "Table has no partition for value";
    ORACLE_ERRNO[-OB_NO_PARTITION_FOR_GIVEN_VALUE] = 14400;
    ORACLE_STR_ERROR[-OB_NO_PARTITION_FOR_GIVEN_VALUE] =
        "ORA-14400: inserted partition key does not map to any partition";
    ORACLE_STR_USER_ERROR[-OB_NO_PARTITION_FOR_GIVEN_VALUE] =
        "ORA-14400: inserted partition key does not map to any partition";
    ERROR_NAME[-OB_EER_NULL_IN_VALUES_LESS_THAN] = "OB_EER_NULL_IN_VALUES_LESS_THAN";
    MYSQL_ERRNO[-OB_EER_NULL_IN_VALUES_LESS_THAN] = ER_NULL_IN_VALUES_LESS_THAN;
    SQLSTATE[-OB_EER_NULL_IN_VALUES_LESS_THAN] = "HY000";
    STR_ERROR[-OB_EER_NULL_IN_VALUES_LESS_THAN] = "Not allowed to use NULL value in VALUES LESS THAN";
    STR_USER_ERROR[-OB_EER_NULL_IN_VALUES_LESS_THAN] = "Not allowed to use NULL value in VALUES LESS THAN";
    ORACLE_ERRNO[-OB_EER_NULL_IN_VALUES_LESS_THAN] = 14019;
    ORACLE_STR_ERROR[-OB_EER_NULL_IN_VALUES_LESS_THAN] =
        "ORA-14019: partition bound element must be one of: datetime or interval literal, number, or MAXVALUE";
    ORACLE_STR_USER_ERROR[-OB_EER_NULL_IN_VALUES_LESS_THAN] =
        "ORA-14019: partition bound element must be one of: datetime or interval literal, number, or MAXVALUE";
    ERROR_NAME[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] = "OB_ERR_PARTITION_CONST_DOMAIN_ERROR";
    MYSQL_ERRNO[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] = ER_PARTITION_CONST_DOMAIN_ERROR;
    SQLSTATE[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] = "Partition constant is out of partition function domain";
    STR_USER_ERROR[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] = "Partition constant is out of partition function domain";
    ORACLE_ERRNO[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] =
        "ORA-00600: internal error code, arguments: -5286, Partition constant is out of partition function domain";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_CONST_DOMAIN_ERROR] =
        "ORA-00600: internal error code, arguments: -5286, Partition constant is out of partition function domain";
    ERROR_NAME[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] = "OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] = ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR;
    SQLSTATE[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] = "HY000";
    STR_ERROR[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] = "Too many fields in \'list of partition fields\'";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] = "Too many fields in \'list of partition fields\'";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] =
        "ORA-00600: internal error code, arguments: -5287, Too many fields in \'list of partition fields\'";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS] =
        "ORA-00600: internal error code, arguments: -5287, Too many fields in \'list of partition fields\'";
    ERROR_NAME[-OB_ERR_BAD_FT_COLUMN] = "OB_ERR_BAD_FT_COLUMN";
    MYSQL_ERRNO[-OB_ERR_BAD_FT_COLUMN] = ER_BAD_FT_COLUMN;
    SQLSTATE[-OB_ERR_BAD_FT_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_BAD_FT_COLUMN] = "Column cannot be part of FULLTEXT index";
    STR_USER_ERROR[-OB_ERR_BAD_FT_COLUMN] = "Column '%.*s' cannot be part of FULLTEXT index";
    ORACLE_ERRNO[-OB_ERR_BAD_FT_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_BAD_FT_COLUMN] =
        "ORA-00600: internal error code, arguments: -5288, Column cannot be part of FULLTEXT index";
    ORACLE_STR_USER_ERROR[-OB_ERR_BAD_FT_COLUMN] =
        "ORA-00600: internal error code, arguments: -5288, Column '%.*s' cannot be part of FULLTEXT index";
    ERROR_NAME[-OB_ERR_KEY_DOES_NOT_EXISTS] = "OB_ERR_KEY_DOES_NOT_EXISTS";
    MYSQL_ERRNO[-OB_ERR_KEY_DOES_NOT_EXISTS] = ER_KEY_DOES_NOT_EXISTS;
    SQLSTATE[-OB_ERR_KEY_DOES_NOT_EXISTS] = "42000";
    STR_ERROR[-OB_ERR_KEY_DOES_NOT_EXISTS] = "key does not exist in table";
    STR_USER_ERROR[-OB_ERR_KEY_DOES_NOT_EXISTS] = "Key '%.*s' doesn't exist in table '%.*s'";
    ORACLE_ERRNO[-OB_ERR_KEY_DOES_NOT_EXISTS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_KEY_DOES_NOT_EXISTS] =
        "ORA-00600: internal error code, arguments: -5289, key does not exist in table";
    ORACLE_STR_USER_ERROR[-OB_ERR_KEY_DOES_NOT_EXISTS] =
        "ORA-00600: internal error code, arguments: -5289, Key '%.*s' doesn't exist in table '%.*s'";
    ERROR_NAME[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] = "OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN";
    MYSQL_ERRNO[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] = ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
    SQLSTATE[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] = "HY000";
    STR_ERROR[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] = "non-default value for generated column is not allowed";
    STR_USER_ERROR[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] =
        "The value specified for generated column '%.*s' in table '%.*s' is not allowed";
    ORACLE_ERRNO[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] =
        "ORA-00600: internal error code, arguments: -5290, non-default value for generated column is not allowed";
    ORACLE_STR_USER_ERROR[-OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN] =
        "ORA-00600: internal error code, arguments: -5290, The value specified for generated column '%.*s' in table "
        "'%.*s' is not allowed";
    ERROR_NAME[-OB_ERR_BAD_CTXCAT_COLUMN] = "OB_ERR_BAD_CTXCAT_COLUMN";
    MYSQL_ERRNO[-OB_ERR_BAD_CTXCAT_COLUMN] = -1;
    SQLSTATE[-OB_ERR_BAD_CTXCAT_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_BAD_CTXCAT_COLUMN] = "The CTXCAT column must be contiguous in the index column list";
    STR_USER_ERROR[-OB_ERR_BAD_CTXCAT_COLUMN] = "The CTXCAT column must be contiguous in the index column list";
    ORACLE_ERRNO[-OB_ERR_BAD_CTXCAT_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_BAD_CTXCAT_COLUMN] = "ORA-00600: internal error code, arguments: -5291, The CTXCAT column "
                                                  "must be contiguous in the index column list";
    ORACLE_STR_USER_ERROR[-OB_ERR_BAD_CTXCAT_COLUMN] = "ORA-00600: internal error code, arguments: -5291, The CTXCAT "
                                                       "column must be contiguous in the index column list";
    ERROR_NAME[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] = "OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN";
    MYSQL_ERRNO[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] = ER_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
    SQLSTATE[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] = "not supported for generated columns";
    STR_USER_ERROR[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] = "'%s' is not supported for generated columns.";
    ORACLE_ERRNO[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] =
        "ORA-00600: internal error code, arguments: -5292, not supported for generated columns";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN] =
        "ORA-00600: internal error code, arguments: -5292, '%s' is not supported for generated columns.";
    ERROR_NAME[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] = "OB_ERR_DEPENDENT_BY_GENERATED_COLUMN";
    MYSQL_ERRNO[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] = ER_DEPENDENT_BY_GENERATED_COLUMN;
    SQLSTATE[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] = "Column has a generated column dependency";
    STR_USER_ERROR[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] = "Column '%.*s' has a generated column dependency";
    ORACLE_ERRNO[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] =
        "ORA-00600: internal error code, arguments: -5293, Column has a generated column dependency";
    ORACLE_STR_USER_ERROR[-OB_ERR_DEPENDENT_BY_GENERATED_COLUMN] =
        "ORA-00600: internal error code, arguments: -5293, Column '%.*s' has a generated column dependency";
    ERROR_NAME[-OB_ERR_TOO_MANY_ROWS] = "OB_ERR_TOO_MANY_ROWS";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_ROWS] = ER_TOO_MANY_ROWS;
    SQLSTATE[-OB_ERR_TOO_MANY_ROWS] = "42000";
    STR_ERROR[-OB_ERR_TOO_MANY_ROWS] = "Result consisted of more than one row";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_ROWS] = "Result consisted of more than one row";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_ROWS] = 1422;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_ROWS] = "ORA-01422: exact fetch returns more than requested number of rows";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_ROWS] = "ORA-01422: exact fetch returns more than requested number of rows";
    ERROR_NAME[-OB_WRONG_FIELD_TERMINATORS] = "OB_WRONG_FIELD_TERMINATORS";
    MYSQL_ERRNO[-OB_WRONG_FIELD_TERMINATORS] = ER_WRONG_FIELD_TERMINATORS;
    SQLSTATE[-OB_WRONG_FIELD_TERMINATORS] = "42000";
    STR_ERROR[-OB_WRONG_FIELD_TERMINATORS] = "Field separator argument is not what is expected; check the manual";
    STR_USER_ERROR[-OB_WRONG_FIELD_TERMINATORS] = "Field separator argument is not what is expected; check the manual";
    ORACLE_ERRNO[-OB_WRONG_FIELD_TERMINATORS] = 600;
    ORACLE_STR_ERROR[-OB_WRONG_FIELD_TERMINATORS] = "ORA-00600: internal error code, arguments: -5295, Field separator "
                                                    "argument is not what is expected; check the manual";
    ORACLE_STR_USER_ERROR[-OB_WRONG_FIELD_TERMINATORS] = "ORA-00600: internal error code, arguments: -5295, Field "
                                                         "separator argument is not what is expected; check the manual";
    ERROR_NAME[-OB_NO_READABLE_REPLICA] = "OB_NO_READABLE_REPLICA";
    MYSQL_ERRNO[-OB_NO_READABLE_REPLICA] = -1;
    SQLSTATE[-OB_NO_READABLE_REPLICA] = "42000";
    STR_ERROR[-OB_NO_READABLE_REPLICA] = "there has no readable replica";
    STR_USER_ERROR[-OB_NO_READABLE_REPLICA] = "there has no readable replica";
    ORACLE_ERRNO[-OB_NO_READABLE_REPLICA] = 600;
    ORACLE_STR_ERROR[-OB_NO_READABLE_REPLICA] =
        "ORA-00600: internal error code, arguments: -5296, there has no readable replica";
    ORACLE_STR_USER_ERROR[-OB_NO_READABLE_REPLICA] =
        "ORA-00600: internal error code, arguments: -5296, there has no readable replica";
    ERROR_NAME[-OB_ERR_UNEXPECTED_TZ_TRANSITION] = "OB_ERR_UNEXPECTED_TZ_TRANSITION";
    MYSQL_ERRNO[-OB_ERR_UNEXPECTED_TZ_TRANSITION] = -1;
    SQLSTATE[-OB_ERR_UNEXPECTED_TZ_TRANSITION] = "HY000";
    STR_ERROR[-OB_ERR_UNEXPECTED_TZ_TRANSITION] = "unexpected time zone info transition";
    STR_USER_ERROR[-OB_ERR_UNEXPECTED_TZ_TRANSITION] = "unexpected time zone info transition";
    ORACLE_ERRNO[-OB_ERR_UNEXPECTED_TZ_TRANSITION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UNEXPECTED_TZ_TRANSITION] =
        "ORA-00600: internal error code, arguments: -5297, unexpected time zone info transition";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNEXPECTED_TZ_TRANSITION] =
        "ORA-00600: internal error code, arguments: -5297, unexpected time zone info transition";
    ERROR_NAME[-OB_ERR_SYNONYM_EXIST] = "OB_ERR_SYNONYM_EXIST";
    MYSQL_ERRNO[-OB_ERR_SYNONYM_EXIST] = -1;
    SQLSTATE[-OB_ERR_SYNONYM_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_SYNONYM_EXIST] = "synonym exists";
    STR_USER_ERROR[-OB_ERR_SYNONYM_EXIST] = "synonym '%.*s' already exists";
    ORACLE_ERRNO[-OB_ERR_SYNONYM_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SYNONYM_EXIST] = "ORA-00600: internal error code, arguments: -5298, synonym exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_SYNONYM_EXIST] =
        "ORA-00600: internal error code, arguments: -5298, synonym '%.*s' already exists";
    ERROR_NAME[-OB_SYNONYM_NOT_EXIST] = "OB_SYNONYM_NOT_EXIST";
    MYSQL_ERRNO[-OB_SYNONYM_NOT_EXIST] = -1;
    SQLSTATE[-OB_SYNONYM_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_SYNONYM_NOT_EXIST] = "synonym not exists";
    STR_USER_ERROR[-OB_SYNONYM_NOT_EXIST] = "synonym \'%.*s.%.*s\' doesn\'t exist";
    ORACLE_ERRNO[-OB_SYNONYM_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_SYNONYM_NOT_EXIST] = "ORA-00600: internal error code, arguments: -5299, synonym not exists";
    ORACLE_STR_USER_ERROR[-OB_SYNONYM_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5299, synonym \'%.*s.%.*s\' doesn\'t exist";
    ERROR_NAME[-OB_ERR_MISS_ORDER_BY_EXPR] = "OB_ERR_MISS_ORDER_BY_EXPR";
    MYSQL_ERRNO[-OB_ERR_MISS_ORDER_BY_EXPR] = -1;
    SQLSTATE[-OB_ERR_MISS_ORDER_BY_EXPR] = "HY000";
    STR_ERROR[-OB_ERR_MISS_ORDER_BY_EXPR] = "missing ORDER BY expression in the window specification";
    STR_USER_ERROR[-OB_ERR_MISS_ORDER_BY_EXPR] = "missing ORDER BY expression in the window specification";
    ORACLE_ERRNO[-OB_ERR_MISS_ORDER_BY_EXPR] = 30485;
    ORACLE_STR_ERROR[-OB_ERR_MISS_ORDER_BY_EXPR] = "ORA-30485: missing ORDER BY expression in the window specification";
    ORACLE_STR_USER_ERROR[-OB_ERR_MISS_ORDER_BY_EXPR] =
        "ORA-30485: missing ORDER BY expression in the window specification";
    ERROR_NAME[-OB_ERR_NOT_CONST_EXPR] = "OB_ERR_NOT_CONST_EXPR";
    MYSQL_ERRNO[-OB_ERR_NOT_CONST_EXPR] = -1;
    SQLSTATE[-OB_ERR_NOT_CONST_EXPR] = "HY000";
    STR_ERROR[-OB_ERR_NOT_CONST_EXPR] = "The argument of the window function should be a constant for a partition";
    STR_USER_ERROR[-OB_ERR_NOT_CONST_EXPR] = "The argument of the window function should be a constant for a partition";
    ORACLE_ERRNO[-OB_ERR_NOT_CONST_EXPR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NOT_CONST_EXPR] = "ORA-00600: internal error code, arguments: -5301, The argument of the "
                                               "window function should be a constant for a partition";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_CONST_EXPR] = "ORA-00600: internal error code, arguments: -5301, The argument of "
                                                    "the window function should be a constant for a partition";
    ERROR_NAME[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] = "OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED";
    MYSQL_ERRNO[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] = ER_PARTITION_MGMT_ON_NONPARTITIONED;
    SQLSTATE[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] =
        "Partition management on a not partitioned table is not possible";
    STR_USER_ERROR[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] =
        "Partition management on a not partitioned table is not possible";
    ORACLE_ERRNO[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] =
        "ORA-00600: internal error code, arguments: -5302, Partition management on a not partitioned table is not "
        "possible";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED] =
        "ORA-00600: internal error code, arguments: -5302, Partition management on a not partitioned table is not "
        "possible";
    ERROR_NAME[-OB_ERR_DROP_PARTITION_NON_EXISTENT] = "OB_ERR_DROP_PARTITION_NON_EXISTENT";
    MYSQL_ERRNO[-OB_ERR_DROP_PARTITION_NON_EXISTENT] = ER_DROP_PARTITION_NON_EXISTENT;
    SQLSTATE[-OB_ERR_DROP_PARTITION_NON_EXISTENT] = "HY000";
    STR_ERROR[-OB_ERR_DROP_PARTITION_NON_EXISTENT] = "Error in list of partitions";
    STR_USER_ERROR[-OB_ERR_DROP_PARTITION_NON_EXISTENT] = "Error in list of partitions to %s";
    ORACLE_ERRNO[-OB_ERR_DROP_PARTITION_NON_EXISTENT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DROP_PARTITION_NON_EXISTENT] =
        "ORA-00600: internal error code, arguments: -5303, Error in list of partitions";
    ORACLE_STR_USER_ERROR[-OB_ERR_DROP_PARTITION_NON_EXISTENT] =
        "ORA-00600: internal error code, arguments: -5303, Error in list of partitions to %s";
    ERROR_NAME[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] = "OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE";
    MYSQL_ERRNO[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] = -1;
    SQLSTATE[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] = "Partition management on a two-part table is not possible";
    STR_USER_ERROR[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] =
        "Partition management on a two-part table is not possible";
    ORACLE_ERRNO[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] =
        "ORA-00600: internal error code, arguments: -5304, Partition management on a two-part table is not possible";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE] =
        "ORA-00600: internal error code, arguments: -5304, Partition management on a two-part table is not possible";
    ERROR_NAME[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] = "OB_ERR_ONLY_ON_RANGE_LIST_PARTITION";
    MYSQL_ERRNO[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] = ER_ONLY_ON_RANGE_LIST_PARTITION;
    SQLSTATE[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] = "can only be used on RANGE/LIST partitions";
    STR_USER_ERROR[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] = "%s PARTITION can only be used on RANGE/LIST partitions";
    ORACLE_ERRNO[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] =
        "ORA-00600: internal error code, arguments: -5305, can only be used on RANGE/LIST partitions";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_ON_RANGE_LIST_PARTITION] =
        "ORA-00600: internal error code, arguments: -5305, %s PARTITION can only be used on RANGE/LIST partitions";
    ERROR_NAME[-OB_ERR_DROP_LAST_PARTITION] = "OB_ERR_DROP_LAST_PARTITION";
    MYSQL_ERRNO[-OB_ERR_DROP_LAST_PARTITION] = ER_DROP_LAST_PARTITION;
    SQLSTATE[-OB_ERR_DROP_LAST_PARTITION] = "HY000";
    STR_ERROR[-OB_ERR_DROP_LAST_PARTITION] = "Cannot remove all partitions, use DROP TABLE instead";
    STR_USER_ERROR[-OB_ERR_DROP_LAST_PARTITION] = "Cannot remove all partitions, use DROP TABLE instead";
    ORACLE_ERRNO[-OB_ERR_DROP_LAST_PARTITION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DROP_LAST_PARTITION] =
        "ORA-00600: internal error code, arguments: -5306, Cannot remove all partitions, use DROP TABLE instead";
    ORACLE_STR_USER_ERROR[-OB_ERR_DROP_LAST_PARTITION] =
        "ORA-00600: internal error code, arguments: -5306, Cannot remove all partitions, use DROP TABLE instead";
    ERROR_NAME[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] = "OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH";
    MYSQL_ERRNO[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] = -1;
    SQLSTATE[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] = "HY000";
    STR_ERROR[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] = "Scheduler thread number is not enough";
    STR_USER_ERROR[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] = "Scheduler thread number is not enough";
    ORACLE_ERRNO[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -5307, Scheduler thread number is not enough";
    ORACLE_STR_USER_ERROR[-OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH] =
        "ORA-00600: internal error code, arguments: -5307, Scheduler thread number is not enough";
    ERROR_NAME[-OB_ERR_IGNORE_USER_HOST_NAME] = "OB_ERR_IGNORE_USER_HOST_NAME";
    MYSQL_ERRNO[-OB_ERR_IGNORE_USER_HOST_NAME] = -1;
    SQLSTATE[-OB_ERR_IGNORE_USER_HOST_NAME] = "HY000";
    STR_ERROR[-OB_ERR_IGNORE_USER_HOST_NAME] = "Ignore the host name";
    STR_USER_ERROR[-OB_ERR_IGNORE_USER_HOST_NAME] = "Ignore the host name";
    ORACLE_ERRNO[-OB_ERR_IGNORE_USER_HOST_NAME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_IGNORE_USER_HOST_NAME] =
        "ORA-00600: internal error code, arguments: -5308, Ignore the host name";
    ORACLE_STR_USER_ERROR[-OB_ERR_IGNORE_USER_HOST_NAME] =
        "ORA-00600: internal error code, arguments: -5308, Ignore the host name";
    ERROR_NAME[-OB_IGNORE_SQL_IN_RESTORE] = "OB_IGNORE_SQL_IN_RESTORE";
    MYSQL_ERRNO[-OB_IGNORE_SQL_IN_RESTORE] = -1;
    SQLSTATE[-OB_IGNORE_SQL_IN_RESTORE] = "HY000";
    STR_ERROR[-OB_IGNORE_SQL_IN_RESTORE] = "Ignore sql in restore process";
    STR_USER_ERROR[-OB_IGNORE_SQL_IN_RESTORE] = "Ignore sql in restore process";
    ORACLE_ERRNO[-OB_IGNORE_SQL_IN_RESTORE] = 600;
    ORACLE_STR_ERROR[-OB_IGNORE_SQL_IN_RESTORE] =
        "ORA-00600: internal error code, arguments: -5309, Ignore sql in restore process";
    ORACLE_STR_USER_ERROR[-OB_IGNORE_SQL_IN_RESTORE] =
        "ORA-00600: internal error code, arguments: -5309, Ignore sql in restore process";
    ERROR_NAME[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] = "OB_ERR_TEMPORARY_TABLE_WITH_PARTITION";
    MYSQL_ERRNO[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] = ER_PARTITION_NO_TEMPORARY;
    SQLSTATE[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] = "HY000";
    STR_ERROR[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] = "Cannot create temporary table with partitions";
    STR_USER_ERROR[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] = "Cannot create temporary table with partitions";
    ORACLE_ERRNO[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] =
        "ORA-00600: internal error code, arguments: -5310, Cannot create temporary table with partitions";
    ORACLE_STR_USER_ERROR[-OB_ERR_TEMPORARY_TABLE_WITH_PARTITION] =
        "ORA-00600: internal error code, arguments: -5310, Cannot create temporary table with partitions";
    ERROR_NAME[-OB_ERR_INVALID_COLUMN_ID] = "OB_ERR_INVALID_COLUMN_ID";
    MYSQL_ERRNO[-OB_ERR_INVALID_COLUMN_ID] = -1;
    SQLSTATE[-OB_ERR_INVALID_COLUMN_ID] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_COLUMN_ID] = "Invalid column id";
    STR_USER_ERROR[-OB_ERR_INVALID_COLUMN_ID] = "Invalid column id for %.*s";
    ORACLE_ERRNO[-OB_ERR_INVALID_COLUMN_ID] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_COLUMN_ID] = "ORA-00600: internal error code, arguments: -5311, Invalid column id";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_COLUMN_ID] =
        "ORA-00600: internal error code, arguments: -5311, Invalid column id for %.*s";
    ERROR_NAME[-OB_SYNC_DDL_DUPLICATE] = "OB_SYNC_DDL_DUPLICATE";
    MYSQL_ERRNO[-OB_SYNC_DDL_DUPLICATE] = -1;
    SQLSTATE[-OB_SYNC_DDL_DUPLICATE] = "HY000";
    STR_ERROR[-OB_SYNC_DDL_DUPLICATE] = "Duplicated ddl id";
    STR_USER_ERROR[-OB_SYNC_DDL_DUPLICATE] = "Duplicated ddl id '%.*s'";
    ORACLE_ERRNO[-OB_SYNC_DDL_DUPLICATE] = 600;
    ORACLE_STR_ERROR[-OB_SYNC_DDL_DUPLICATE] = "ORA-00600: internal error code, arguments: -5312, Duplicated ddl id";
    ORACLE_STR_USER_ERROR[-OB_SYNC_DDL_DUPLICATE] =
        "ORA-00600: internal error code, arguments: -5312, Duplicated ddl id '%.*s'";
    ERROR_NAME[-OB_SYNC_DDL_ERROR] = "OB_SYNC_DDL_ERROR";
    MYSQL_ERRNO[-OB_SYNC_DDL_ERROR] = -1;
    SQLSTATE[-OB_SYNC_DDL_ERROR] = "HY000";
    STR_ERROR[-OB_SYNC_DDL_ERROR] = "Failed to sync ddl";
    STR_USER_ERROR[-OB_SYNC_DDL_ERROR] = "Failed to sync ddl '%.*s'";
    ORACLE_ERRNO[-OB_SYNC_DDL_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_SYNC_DDL_ERROR] = "ORA-00600: internal error code, arguments: -5313, Failed to sync ddl";
    ORACLE_STR_USER_ERROR[-OB_SYNC_DDL_ERROR] =
        "ORA-00600: internal error code, arguments: -5313, Failed to sync ddl '%.*s'";
    ERROR_NAME[-OB_ERR_ROW_IS_REFERENCED] = "OB_ERR_ROW_IS_REFERENCED";
    MYSQL_ERRNO[-OB_ERR_ROW_IS_REFERENCED] = ER_ROW_IS_REFERENCED_2;
    SQLSTATE[-OB_ERR_ROW_IS_REFERENCED] = "23000";
    STR_ERROR[-OB_ERR_ROW_IS_REFERENCED] = "Cannot delete or update a parent row: a foreign key constraint fails";
    STR_USER_ERROR[-OB_ERR_ROW_IS_REFERENCED] = "Cannot delete or update a parent row: a foreign key constraint fails";
    ORACLE_ERRNO[-OB_ERR_ROW_IS_REFERENCED] = 2292;
    ORACLE_STR_ERROR[-OB_ERR_ROW_IS_REFERENCED] = "ORA-02292: integrity constraint violated - child record found";
    ORACLE_STR_USER_ERROR[-OB_ERR_ROW_IS_REFERENCED] = "ORA-02292: integrity constraint violated - child record found";
    ERROR_NAME[-OB_ERR_NO_REFERENCED_ROW] = "OB_ERR_NO_REFERENCED_ROW";
    MYSQL_ERRNO[-OB_ERR_NO_REFERENCED_ROW] = ER_NO_REFERENCED_ROW_2;
    SQLSTATE[-OB_ERR_NO_REFERENCED_ROW] = "23000";
    STR_ERROR[-OB_ERR_NO_REFERENCED_ROW] = "Cannot add or update a child row: a foreign key constraint fails";
    STR_USER_ERROR[-OB_ERR_NO_REFERENCED_ROW] = "Cannot add or update a child row: a foreign key constraint fails";
    ORACLE_ERRNO[-OB_ERR_NO_REFERENCED_ROW] = 2291;
    ORACLE_STR_ERROR[-OB_ERR_NO_REFERENCED_ROW] = "ORA-02291: integrity constraint violated - parent key not found";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_REFERENCED_ROW] =
        "ORA-02291: integrity constraint violated - parent key not found";
    ERROR_NAME[-OB_ERR_FUNC_RESULT_TOO_LARGE] = "OB_ERR_FUNC_RESULT_TOO_LARGE";
    MYSQL_ERRNO[-OB_ERR_FUNC_RESULT_TOO_LARGE] = ER_WARN_ALLOWED_PACKET_OVERFLOWED;
    SQLSTATE[-OB_ERR_FUNC_RESULT_TOO_LARGE] = "HY000";
    STR_ERROR[-OB_ERR_FUNC_RESULT_TOO_LARGE] = "Result of function was larger than max_allowed_packet - truncated";
    STR_USER_ERROR[-OB_ERR_FUNC_RESULT_TOO_LARGE] =
        "Result of %s() was larger than max_allowed_packet (%d) - truncated";
    ORACLE_ERRNO[-OB_ERR_FUNC_RESULT_TOO_LARGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_FUNC_RESULT_TOO_LARGE] = "ORA-00600: internal error code, arguments: -5316, Result of "
                                                      "function was larger than max_allowed_packet - truncated";
    ORACLE_STR_USER_ERROR[-OB_ERR_FUNC_RESULT_TOO_LARGE] =
        "ORA-00600: internal error code, arguments: -5316, Result of %s() was larger than max_allowed_packet (%d) - "
        "truncated";
    ERROR_NAME[-OB_ERR_CANNOT_ADD_FOREIGN] = "OB_ERR_CANNOT_ADD_FOREIGN";
    MYSQL_ERRNO[-OB_ERR_CANNOT_ADD_FOREIGN] = ER_CANNOT_ADD_FOREIGN;
    SQLSTATE[-OB_ERR_CANNOT_ADD_FOREIGN] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_ADD_FOREIGN] = "Cannot add foreign key constraint";
    STR_USER_ERROR[-OB_ERR_CANNOT_ADD_FOREIGN] = "Cannot add foreign key constraint";
    ORACLE_ERRNO[-OB_ERR_CANNOT_ADD_FOREIGN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_ADD_FOREIGN] =
        "ORA-00600: internal error code, arguments: -5317, Cannot add foreign key constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_ADD_FOREIGN] =
        "ORA-00600: internal error code, arguments: -5317, Cannot add foreign key constraint";
    ERROR_NAME[-OB_ERR_WRONG_FK_DEF] = "OB_ERR_WRONG_FK_DEF";
    MYSQL_ERRNO[-OB_ERR_WRONG_FK_DEF] = ER_WRONG_FK_DEF;
    SQLSTATE[-OB_ERR_WRONG_FK_DEF] = "42000";
    STR_ERROR[-OB_ERR_WRONG_FK_DEF] = "Incorrect foreign key definition: Key reference and table reference don't match";
    STR_USER_ERROR[-OB_ERR_WRONG_FK_DEF] =
        "Incorrect foreign key definition: Key reference and table reference don't match";
    ORACLE_ERRNO[-OB_ERR_WRONG_FK_DEF] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_FK_DEF] = "ORA-00600: internal error code, arguments: -5318, Incorrect foreign key "
                                             "definition: Key reference and table reference don't match";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_FK_DEF] = "ORA-00600: internal error code, arguments: -5318, Incorrect foreign "
                                                  "key definition: Key reference and table reference don't match";
    ERROR_NAME[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] = "OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK";
    MYSQL_ERRNO[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] = -1;
    SQLSTATE[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] = "Invalid child column length";
    STR_USER_ERROR[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] =
        "Child column \'%.*s\' data length cannot be less than parent column \'%.*s\' data length";
    ORACLE_ERRNO[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] =
        "ORA-00600: internal error code, arguments: -5319, Invalid child column length";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK] =
        "ORA-00600: internal error code, arguments: -5319, Child column \'%.*s\' data length cannot be less than "
        "parent column \'%.*s\' data length";
    ERROR_NAME[-OB_ERR_ALTER_COLUMN_FK] = "OB_ERR_ALTER_COLUMN_FK";
    MYSQL_ERRNO[-OB_ERR_ALTER_COLUMN_FK] = -1;
    SQLSTATE[-OB_ERR_ALTER_COLUMN_FK] = "HY000";
    STR_ERROR[-OB_ERR_ALTER_COLUMN_FK] = "Cannot alter foreign key column";
    STR_USER_ERROR[-OB_ERR_ALTER_COLUMN_FK] = "\'%.*s\': used in a foreign key constraint";
    ORACLE_ERRNO[-OB_ERR_ALTER_COLUMN_FK] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ALTER_COLUMN_FK] =
        "ORA-00600: internal error code, arguments: -5320, Cannot alter foreign key column";
    ORACLE_STR_USER_ERROR[-OB_ERR_ALTER_COLUMN_FK] =
        "ORA-00600: internal error code, arguments: -5320, \'%.*s\': used in a foreign key constraint";
    ERROR_NAME[-OB_ERR_CONNECT_BY_REQUIRED] = "OB_ERR_CONNECT_BY_REQUIRED";
    MYSQL_ERRNO[-OB_ERR_CONNECT_BY_REQUIRED] = -1;
    SQLSTATE[-OB_ERR_CONNECT_BY_REQUIRED] = "HY000";
    STR_ERROR[-OB_ERR_CONNECT_BY_REQUIRED] = "CONNECT BY clause required in this query block";
    STR_USER_ERROR[-OB_ERR_CONNECT_BY_REQUIRED] = "CONNECT BY clause required in this query block";
    ORACLE_ERRNO[-OB_ERR_CONNECT_BY_REQUIRED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CONNECT_BY_REQUIRED] =
        "ORA-00600: internal error code, arguments: -5321, CONNECT BY clause required in this query block";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONNECT_BY_REQUIRED] =
        "ORA-00600: internal error code, arguments: -5321, CONNECT BY clause required in this query block";
    ERROR_NAME[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] = "OB_ERR_INVALID_PSEUDO_COLUMN_PLACE";
    MYSQL_ERRNO[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] = -1;
    SQLSTATE[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] = "Specified pseudocolumn, operator or function not allowed here";
    STR_USER_ERROR[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] =
        "Specified pseudocolumn, operator or function not allowed here";
    ORACLE_ERRNO[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] =
        "ORA-00600: internal error code, arguments: -5322, Specified pseudocolumn, operator or function not allowed "
        "here";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_PSEUDO_COLUMN_PLACE] =
        "ORA-00600: internal error code, arguments: -5322, Specified pseudocolumn, operator or function not allowed "
        "here";
    ERROR_NAME[-OB_ERR_NOCYCLE_REQUIRED] = "OB_ERR_NOCYCLE_REQUIRED";
    MYSQL_ERRNO[-OB_ERR_NOCYCLE_REQUIRED] = -1;
    SQLSTATE[-OB_ERR_NOCYCLE_REQUIRED] = "HY000";
    STR_ERROR[-OB_ERR_NOCYCLE_REQUIRED] = "NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudocolumn";
    STR_USER_ERROR[-OB_ERR_NOCYCLE_REQUIRED] = "NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudocolumn";
    ORACLE_ERRNO[-OB_ERR_NOCYCLE_REQUIRED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NOCYCLE_REQUIRED] = "ORA-00600: internal error code, arguments: -5323, NOCYCLE keyword is "
                                                 "required with CONNECT_BY_ISCYCLE pseudocolumn";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOCYCLE_REQUIRED] = "ORA-00600: internal error code, arguments: -5323, NOCYCLE "
                                                      "keyword is required with CONNECT_BY_ISCYCLE pseudocolumn";
    ERROR_NAME[-OB_ERR_CONNECT_BY_LOOP] = "OB_ERR_CONNECT_BY_LOOP";
    MYSQL_ERRNO[-OB_ERR_CONNECT_BY_LOOP] = -1;
    SQLSTATE[-OB_ERR_CONNECT_BY_LOOP] = "HY000";
    STR_ERROR[-OB_ERR_CONNECT_BY_LOOP] = "CONNECT BY loop in user data";
    STR_USER_ERROR[-OB_ERR_CONNECT_BY_LOOP] = "CONNECT BY loop in user data";
    ORACLE_ERRNO[-OB_ERR_CONNECT_BY_LOOP] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CONNECT_BY_LOOP] =
        "ORA-00600: internal error code, arguments: -5324, CONNECT BY loop in user data";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONNECT_BY_LOOP] =
        "ORA-00600: internal error code, arguments: -5324, CONNECT BY loop in user data";
    ERROR_NAME[-OB_ERR_INVALID_SIBLINGS] = "OB_ERR_INVALID_SIBLINGS";
    MYSQL_ERRNO[-OB_ERR_INVALID_SIBLINGS] = -1;
    SQLSTATE[-OB_ERR_INVALID_SIBLINGS] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SIBLINGS] = "ORDER SIBLINGS BY clause not allowed here";
    STR_USER_ERROR[-OB_ERR_INVALID_SIBLINGS] = "ORDER SIBLINGS BY clause not allowed here";
    ORACLE_ERRNO[-OB_ERR_INVALID_SIBLINGS] = 30929;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SIBLINGS] = "ORA-30929: ORDER SIBLINGS BY clause not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SIBLINGS] = "ORA-30929: ORDER SIBLINGS BY clause not allowed here";
    ERROR_NAME[-OB_ERR_INVALID_SEPARATOR] = "OB_ERR_INVALID_SEPARATOR";
    MYSQL_ERRNO[-OB_ERR_INVALID_SEPARATOR] = -1;
    SQLSTATE[-OB_ERR_INVALID_SEPARATOR] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SEPARATOR] =
        "when using SYS_CONNECT_BY_PATH function, cannot have separator as part of column value";
    STR_USER_ERROR[-OB_ERR_INVALID_SEPARATOR] =
        "when using SYS_CONNECT_BY_PATH function, cannot have separator as part of column value";
    ORACLE_ERRNO[-OB_ERR_INVALID_SEPARATOR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SEPARATOR] =
        "ORA-00600: internal error code, arguments: -5326, when using SYS_CONNECT_BY_PATH function, cannot have "
        "separator as part of column value";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SEPARATOR] =
        "ORA-00600: internal error code, arguments: -5326, when using SYS_CONNECT_BY_PATH function, cannot have "
        "separator as part of column value";
    ERROR_NAME[-OB_ERR_INVALID_SYNONYM_NAME] = "OB_ERR_INVALID_SYNONYM_NAME";
    MYSQL_ERRNO[-OB_ERR_INVALID_SYNONYM_NAME] = -1;
    SQLSTATE[-OB_ERR_INVALID_SYNONYM_NAME] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SYNONYM_NAME] = "Database can not be specified in public synonym";
    STR_USER_ERROR[-OB_ERR_INVALID_SYNONYM_NAME] = "Database can not be specified in public synonym";
    ORACLE_ERRNO[-OB_ERR_INVALID_SYNONYM_NAME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SYNONYM_NAME] =
        "ORA-00600: internal error code, arguments: -5327, Database can not be specified in public synonym";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SYNONYM_NAME] =
        "ORA-00600: internal error code, arguments: -5327, Database can not be specified in public synonym";
    ERROR_NAME[-OB_ERR_LOOP_OF_SYNONYM] = "OB_ERR_LOOP_OF_SYNONYM";
    MYSQL_ERRNO[-OB_ERR_LOOP_OF_SYNONYM] = -1;
    SQLSTATE[-OB_ERR_LOOP_OF_SYNONYM] = "HY000";
    STR_ERROR[-OB_ERR_LOOP_OF_SYNONYM] = "Looping chain of synonyms";
    STR_USER_ERROR[-OB_ERR_LOOP_OF_SYNONYM] = "Looping chain of synonyms";
    ORACLE_ERRNO[-OB_ERR_LOOP_OF_SYNONYM] = 600;
    ORACLE_STR_ERROR[-OB_ERR_LOOP_OF_SYNONYM] =
        "ORA-00600: internal error code, arguments: -5328, Looping chain of synonyms";
    ORACLE_STR_USER_ERROR[-OB_ERR_LOOP_OF_SYNONYM] =
        "ORA-00600: internal error code, arguments: -5328, Looping chain of synonyms";
    ERROR_NAME[-OB_ERR_SYNONYM_SAME_AS_OBJECT] = "OB_ERR_SYNONYM_SAME_AS_OBJECT";
    MYSQL_ERRNO[-OB_ERR_SYNONYM_SAME_AS_OBJECT] = -1;
    SQLSTATE[-OB_ERR_SYNONYM_SAME_AS_OBJECT] = "HY000";
    STR_ERROR[-OB_ERR_SYNONYM_SAME_AS_OBJECT] = "Cannot create a synonym with same name as object";
    STR_USER_ERROR[-OB_ERR_SYNONYM_SAME_AS_OBJECT] = "Cannot create a synonym with same name as object";
    ORACLE_ERRNO[-OB_ERR_SYNONYM_SAME_AS_OBJECT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SYNONYM_SAME_AS_OBJECT] =
        "ORA-00600: internal error code, arguments: -5329, Cannot create a synonym with same name as object";
    ORACLE_STR_USER_ERROR[-OB_ERR_SYNONYM_SAME_AS_OBJECT] =
        "ORA-00600: internal error code, arguments: -5329, Cannot create a synonym with same name as object";
    ERROR_NAME[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = "OB_ERR_SYNONYM_TRANSLATION_INVALID";
    MYSQL_ERRNO[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = -1;
    SQLSTATE[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = "HY000";
    STR_ERROR[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = "Synonym translation is no longer valid";
    STR_USER_ERROR[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = "Synonym %s translation is no longer valid";
    ORACLE_ERRNO[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = 980;
    ORACLE_STR_ERROR[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = "ORA-00980: synonym translation is no longer valid";
    ORACLE_STR_USER_ERROR[-OB_ERR_SYNONYM_TRANSLATION_INVALID] = "ORA-00980: synonym %s translation is no longer valid";
    ERROR_NAME[-OB_ERR_EXIST_OBJECT] = "OB_ERR_EXIST_OBJECT";
    MYSQL_ERRNO[-OB_ERR_EXIST_OBJECT] = -1;
    SQLSTATE[-OB_ERR_EXIST_OBJECT] = "HY000";
    STR_ERROR[-OB_ERR_EXIST_OBJECT] = "name is already used by an existing object";
    STR_USER_ERROR[-OB_ERR_EXIST_OBJECT] = "name is already used by an existing object";
    ORACLE_ERRNO[-OB_ERR_EXIST_OBJECT] = 955;
    ORACLE_STR_ERROR[-OB_ERR_EXIST_OBJECT] = "ORA-00955: name is already used by an existing object";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXIST_OBJECT] = "ORA-00955: name is already used by an existing object";
    ERROR_NAME[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] = "OB_ERR_ILLEGAL_VALUE_FOR_TYPE";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] = ER_ILLEGAL_VALUE_FOR_TYPE;
    SQLSTATE[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] = "22007";
    STR_ERROR[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] = "Illegal value found during parsing";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] = "Illegal %s '%.*s' value found during parsing";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] =
        "ORA-00600: internal error code, arguments: -5332, Illegal value found during parsing";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_VALUE_FOR_TYPE] =
        "ORA-00600: internal error code, arguments: -5332, Illegal %s '%.*s' value found during parsing";
    ERROR_NAME[-OB_ER_TOO_LONG_SET_ENUM_VALUE] = "OB_ER_TOO_LONG_SET_ENUM_VALUE";
    MYSQL_ERRNO[-OB_ER_TOO_LONG_SET_ENUM_VALUE] = ER_TOO_LONG_SET_ENUM_VALUE;
    SQLSTATE[-OB_ER_TOO_LONG_SET_ENUM_VALUE] = "HY000";
    STR_ERROR[-OB_ER_TOO_LONG_SET_ENUM_VALUE] = "Too long enumeration/set value for column.";
    STR_USER_ERROR[-OB_ER_TOO_LONG_SET_ENUM_VALUE] = "Too long enumeration/set value for column %.*s.";
    ORACLE_ERRNO[-OB_ER_TOO_LONG_SET_ENUM_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ER_TOO_LONG_SET_ENUM_VALUE] =
        "ORA-00600: internal error code, arguments: -5333, Too long enumeration/set value for column.";
    ORACLE_STR_USER_ERROR[-OB_ER_TOO_LONG_SET_ENUM_VALUE] =
        "ORA-00600: internal error code, arguments: -5333, Too long enumeration/set value for column %.*s.";
    ERROR_NAME[-OB_ER_DUPLICATED_VALUE_IN_TYPE] = "OB_ER_DUPLICATED_VALUE_IN_TYPE";
    MYSQL_ERRNO[-OB_ER_DUPLICATED_VALUE_IN_TYPE] = ER_DUPLICATED_VALUE_IN_TYPE;
    SQLSTATE[-OB_ER_DUPLICATED_VALUE_IN_TYPE] = "HY000";
    STR_ERROR[-OB_ER_DUPLICATED_VALUE_IN_TYPE] = "Column has duplicated value";
    STR_USER_ERROR[-OB_ER_DUPLICATED_VALUE_IN_TYPE] = "Column '%.*s' has duplicated value '%.*s' in %s ";
    ORACLE_ERRNO[-OB_ER_DUPLICATED_VALUE_IN_TYPE] = 1;
    ORACLE_STR_ERROR[-OB_ER_DUPLICATED_VALUE_IN_TYPE] = "ORA-00001: unique constraint violated";
    ORACLE_STR_USER_ERROR[-OB_ER_DUPLICATED_VALUE_IN_TYPE] =
        "ORA-00001: unique constraint violated, column '%.*s' with value '%.*s' in %s";
    ERROR_NAME[-OB_ER_TOO_BIG_ENUM] = "OB_ER_TOO_BIG_ENUM";
    MYSQL_ERRNO[-OB_ER_TOO_BIG_ENUM] = ER_TOO_BIG_ENUM;
    SQLSTATE[-OB_ER_TOO_BIG_ENUM] = "HY000";
    STR_ERROR[-OB_ER_TOO_BIG_ENUM] = "Too many enumeration values for column";
    STR_USER_ERROR[-OB_ER_TOO_BIG_ENUM] = "Too many enumeration values for column %.*s";
    ORACLE_ERRNO[-OB_ER_TOO_BIG_ENUM] = 600;
    ORACLE_STR_ERROR[-OB_ER_TOO_BIG_ENUM] =
        "ORA-00600: internal error code, arguments: -5335, Too many enumeration values for column";
    ORACLE_STR_USER_ERROR[-OB_ER_TOO_BIG_ENUM] =
        "ORA-00600: internal error code, arguments: -5335, Too many enumeration values for column %.*s";
    ERROR_NAME[-OB_ERR_TOO_BIG_SET] = "OB_ERR_TOO_BIG_SET";
    MYSQL_ERRNO[-OB_ERR_TOO_BIG_SET] = ER_TOO_BIG_SET;
    SQLSTATE[-OB_ERR_TOO_BIG_SET] = "HY000";
    STR_ERROR[-OB_ERR_TOO_BIG_SET] = "Too many strings for column";
    STR_USER_ERROR[-OB_ERR_TOO_BIG_SET] = "Too many strings for column %.*s and SET";
    ORACLE_ERRNO[-OB_ERR_TOO_BIG_SET] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TOO_BIG_SET] =
        "ORA-00600: internal error code, arguments: -5336, Too many strings for column";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_BIG_SET] =
        "ORA-00600: internal error code, arguments: -5336, Too many strings for column %.*s and SET";
    ERROR_NAME[-OB_ERR_WRONG_ROWID] = "OB_ERR_WRONG_ROWID";
    MYSQL_ERRNO[-OB_ERR_WRONG_ROWID] = -1;
    SQLSTATE[-OB_ERR_WRONG_ROWID] = "HY000";
    STR_ERROR[-OB_ERR_WRONG_ROWID] = "rowid is wrong";
    STR_USER_ERROR[-OB_ERR_WRONG_ROWID] = "rowid is wrong";
    ORACLE_ERRNO[-OB_ERR_WRONG_ROWID] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_ROWID] = "ORA-00600: internal error code, arguments: -5337, rowid is wrong";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_ROWID] = "ORA-00600: internal error code, arguments: -5337, rowid is wrong";
    ERROR_NAME[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] = "OB_ERR_INVALID_WINDOW_FUNCTION_PLACE";
    MYSQL_ERRNO[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] = -1;
    SQLSTATE[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] = "Window Function not allowed here";
    STR_USER_ERROR[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] = "Window Function not allowed here";
    ORACLE_ERRNO[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] =
        "ORA-00600: internal error code, arguments: -5338, Window Function not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_WINDOW_FUNCTION_PLACE] =
        "ORA-00600: internal error code, arguments: -5338, Window Function not allowed here";
    ERROR_NAME[-OB_ERR_PARSE_PARTITION_LIST] = "OB_ERR_PARSE_PARTITION_LIST";
    MYSQL_ERRNO[-OB_ERR_PARSE_PARTITION_LIST] = -1;
    SQLSTATE[-OB_ERR_PARSE_PARTITION_LIST] = "HY000";
    STR_ERROR[-OB_ERR_PARSE_PARTITION_LIST] = "Fail to parse list partition";
    STR_USER_ERROR[-OB_ERR_PARSE_PARTITION_LIST] = "Fail to parse list partition";
    ORACLE_ERRNO[-OB_ERR_PARSE_PARTITION_LIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARSE_PARTITION_LIST] =
        "ORA-00600: internal error code, arguments: -5339, Fail to parse list partition";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARSE_PARTITION_LIST] =
        "ORA-00600: internal error code, arguments: -5339, Fail to parse list partition";
    ERROR_NAME[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] = "OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART";
    MYSQL_ERRNO[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] = ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR;
    SQLSTATE[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] = "HY000";
    STR_ERROR[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] = "Multiple definition of same constant in list partitioning";
    STR_USER_ERROR[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] =
        "Multiple definition of same constant in list partitioning";
    ORACLE_ERRNO[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] =
        "ORA-00600: internal error code, arguments: -5340, Multiple definition of same constant in list partitioning";
    ORACLE_STR_USER_ERROR[-OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART] =
        "ORA-00600: internal error code, arguments: -5340, Multiple definition of same constant in list partitioning";
    ERROR_NAME[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = "OB_ERR_INVALID_TIMEZONE_REGION_ID";
    MYSQL_ERRNO[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = -1;
    SQLSTATE[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = "timezone region ID is invalid";
    STR_USER_ERROR[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = "timezone region ID is invalid";
    ORACLE_ERRNO[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = 1881;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = "ORA-01881: timezone region ID is invalid";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_TIMEZONE_REGION_ID] = "ORA-01881: timezone region ID is invalid";
    ERROR_NAME[-OB_ERR_INVALID_HEX_NUMBER] = "OB_ERR_INVALID_HEX_NUMBER";
    MYSQL_ERRNO[-OB_ERR_INVALID_HEX_NUMBER] = -1;
    SQLSTATE[-OB_ERR_INVALID_HEX_NUMBER] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_HEX_NUMBER] = "invalid hex number";
    STR_USER_ERROR[-OB_ERR_INVALID_HEX_NUMBER] = "invalid hex number";
    ORACLE_ERRNO[-OB_ERR_INVALID_HEX_NUMBER] = 1465;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_HEX_NUMBER] = "ORA-01465: invalid hex number";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_HEX_NUMBER] = "ORA-01465: invalid hex number";
    ERROR_NAME[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] = "OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE";
    MYSQL_ERRNO[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] = -1;
    SQLSTATE[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] = "42000";
    STR_ERROR[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] = "wrong number or types of arguments in function";
    STR_USER_ERROR[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] = "wrong number or types of arguments in call to '%.*s'";
    ORACLE_ERRNO[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] = 6553;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] = "ORA-06553: wrong number or types of arguments in function";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE] =
        "ORA-06553: wrong number or types of arguments in call to '%.*s'";
    ERROR_NAME[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] = "OB_ERR_MULTI_UPDATE_KEY_CONFLICT";
    MYSQL_ERRNO[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] = ER_MULTI_UPDATE_KEY_CONFLICT;
    SQLSTATE[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] = "HY000";
    STR_ERROR[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] = "Primary key/index key/partition key update is not allowed";
    STR_USER_ERROR[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] = "Primary key/index key/partition key update is not allowed "
                                                        "since the table is updated both as '%.*s' and '%.*s'";
    ORACLE_ERRNO[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5344, Primary key/index key/partition key update is not allowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_MULTI_UPDATE_KEY_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5344, Primary key/index key/partition key update is not allowed "
        "since the table is updated both as '%.*s' and '%.*s'";
    ERROR_NAME[-OB_ERR_INSUFFICIENT_PX_WORKER] = "OB_ERR_INSUFFICIENT_PX_WORKER";
    MYSQL_ERRNO[-OB_ERR_INSUFFICIENT_PX_WORKER] = -1;
    SQLSTATE[-OB_ERR_INSUFFICIENT_PX_WORKER] = "HY000";
    STR_ERROR[-OB_ERR_INSUFFICIENT_PX_WORKER] = "insufficient parallel query worker available";
    STR_USER_ERROR[-OB_ERR_INSUFFICIENT_PX_WORKER] = "insufficient parallel query worker available";
    ORACLE_ERRNO[-OB_ERR_INSUFFICIENT_PX_WORKER] = 12827;
    ORACLE_STR_ERROR[-OB_ERR_INSUFFICIENT_PX_WORKER] = "ORA-12827: insufficient parallel query worker available";
    ORACLE_STR_USER_ERROR[-OB_ERR_INSUFFICIENT_PX_WORKER] = "ORA-12827: insufficient parallel query worker available";
    ERROR_NAME[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] = "OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] = "FOR UPDATE of this query expression is not allowed";
    STR_USER_ERROR[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] = "FOR UPDATE of this query expression is not allowed";
    ORACLE_ERRNO[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] = 1786;
    ORACLE_STR_ERROR[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] =
        "ORA-01786: FOR UPDATE of this query expression is not allowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED] =
        "ORA-01786: FOR UPDATE of this query expression is not allowed";
    ERROR_NAME[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] = "OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY";
    MYSQL_ERRNO[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] = -1;
    SQLSTATE[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] = "HY000";
    STR_ERROR[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] =
        "argument should be a function of expressions in PARTITION BY";
    STR_USER_ERROR[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] =
        "argument should be a function of expressions in PARTITION BY";
    ORACLE_ERRNO[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] = 30488;
    ORACLE_STR_ERROR[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] =
        "ORA-30488: argument should be a function of expressions in PARTITION BY";
    ORACLE_STR_USER_ERROR[-OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY] =
        "ORA-30488: argument should be a function of expressions in PARTITION BY";
    ERROR_NAME[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = "OB_ERR_TOO_LONG_STRING_IN_CONCAT";
    MYSQL_ERRNO[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = -1;
    SQLSTATE[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = "HY000";
    STR_ERROR[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = "result of string concatenation is too long";
    STR_USER_ERROR[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = "result of string concatenation is too long";
    ORACLE_ERRNO[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = 1489;
    ORACLE_STR_ERROR[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = "ORA-01489: result of string concatenation is too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_LONG_STRING_IN_CONCAT] = "ORA-01489: result of string concatenation is too long";
    ERROR_NAME[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] = "OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR";
    MYSQL_ERRNO[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] = ER_WRONG_TYPE_COLUMN_VALUE_ERROR;
    SQLSTATE[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] = "Partition column values of incorrect type";
    STR_USER_ERROR[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] = "Partition column values of incorrect type";
    ORACLE_ERRNO[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] = 30078;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] =
        "ORA-30078: partition bound must be TIME/TIMESTAMP WITH TIME ZONE literals";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR] =
        "ORA-30078: partition bound must be TIME/TIMESTAMP WITH TIME ZONE literals";
    ERROR_NAME[-OB_ERR_UPD_CAUSE_PART_CHANGE] = "OB_ERR_UPD_CAUSE_PART_CHANGE";
    MYSQL_ERRNO[-OB_ERR_UPD_CAUSE_PART_CHANGE] = -1;
    SQLSTATE[-OB_ERR_UPD_CAUSE_PART_CHANGE] = "HY000";
    STR_ERROR[-OB_ERR_UPD_CAUSE_PART_CHANGE] = "updating partition key column would cause a partition change";
    STR_USER_ERROR[-OB_ERR_UPD_CAUSE_PART_CHANGE] = "updating partition key column would cause a partition change";
    ORACLE_ERRNO[-OB_ERR_UPD_CAUSE_PART_CHANGE] = 14402;
    ORACLE_STR_ERROR[-OB_ERR_UPD_CAUSE_PART_CHANGE] =
        "ORA-14402: updating partition key column would cause a partition change";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPD_CAUSE_PART_CHANGE] =
        "ORA-14402: updating partition key column would cause a partition change";
    ERROR_NAME[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = "OB_ERR_INVALID_TYPE_FOR_ARGUMENT";
    MYSQL_ERRNO[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = -1;
    SQLSTATE[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = "invalid type given for an argument";
    STR_USER_ERROR[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = "invalid type given for an argument";
    ORACLE_ERRNO[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = 30175;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = "ORA-30175: invalid type given for an argument";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_TYPE_FOR_ARGUMENT] = "ORA-30175: invalid type given for an argument";
    ERROR_NAME[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] = "OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL";
    MYSQL_ERRNO[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] = -1;
    SQLSTATE[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] = "HY000";
    STR_ERROR[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] = "specified field not found in datetime or interval";
    STR_USER_ERROR[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] =
        "specified field not found in datetime or interval";
    ORACLE_ERRNO[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] = 1878;
    ORACLE_STR_ERROR[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] =
        "ORA-01878: specified field not found in datetime or interval";
    ORACLE_STR_USER_ERROR[-OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL] =
        "ORA-01878: specified field not found in datetime or interval";
    ERROR_NAME[-OB_ERR_ADD_PART_BOUN_NOT_INC] = "OB_ERR_ADD_PART_BOUN_NOT_INC";
    MYSQL_ERRNO[-OB_ERR_ADD_PART_BOUN_NOT_INC] = ER_RANGE_NOT_INCREASING_ERROR;
    SQLSTATE[-OB_ERR_ADD_PART_BOUN_NOT_INC] = "HY000";
    STR_ERROR[-OB_ERR_ADD_PART_BOUN_NOT_INC] = "VALUES LESS THAN value must be strictly increasing for each partition";
    STR_USER_ERROR[-OB_ERR_ADD_PART_BOUN_NOT_INC] =
        "VALUES LESS THAN value must be strictly increasing for each partition";
    ORACLE_ERRNO[-OB_ERR_ADD_PART_BOUN_NOT_INC] = 14074;
    ORACLE_STR_ERROR[-OB_ERR_ADD_PART_BOUN_NOT_INC] =
        "ORA-14074: partition bound must collate higher than that of the last partition";
    ORACLE_STR_USER_ERROR[-OB_ERR_ADD_PART_BOUN_NOT_INC] =
        "ORA-14074: partition bound must collate higher than that of the last partition";
    ERROR_NAME[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] = "OB_ERR_DATA_TOO_LONG_IN_PART_CHECK";
    MYSQL_ERRNO[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] = ER_DATA_TOO_LONG;
    SQLSTATE[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] = "22001";
    STR_ERROR[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] = "Data too long for column";
    STR_USER_ERROR[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] = "Data too long for column";
    ORACLE_ERRNO[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] = 14036;
    ORACLE_STR_ERROR[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] = "ORA-14036: partition bound value too large for column";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATA_TOO_LONG_IN_PART_CHECK] =
        "ORA-14036: partition bound value too large for column";
    ERROR_NAME[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] = "OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR";
    MYSQL_ERRNO[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] = ER_WRONG_TYPE_COLUMN_VALUE_ERROR;
    SQLSTATE[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] = "Partition column values of incorrect type";
    STR_USER_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] = "Partition column values of incorrect type";
    ORACLE_ERRNO[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] = 14308;
    ORACLE_STR_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] =
        "ORA-14308: partition bound element must be one of: string, datetime or interval literal, number, or NULL";
    ORACLE_STR_USER_ERROR[-OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR] =
        "ORA-14308: partition bound element must be one of: string, datetime or interval literal, number, or NULL";
    ERROR_NAME[-OB_CANT_AGGREGATE_3COLLATIONS] = "OB_CANT_AGGREGATE_3COLLATIONS";
    MYSQL_ERRNO[-OB_CANT_AGGREGATE_3COLLATIONS] = ER_CANT_AGGREGATE_3COLLATIONS;
    SQLSTATE[-OB_CANT_AGGREGATE_3COLLATIONS] = "HY000";
    STR_ERROR[-OB_CANT_AGGREGATE_3COLLATIONS] = "Illegal mix of collations";
    STR_USER_ERROR[-OB_CANT_AGGREGATE_3COLLATIONS] = "Illegal mix of collations";
    ORACLE_ERRNO[-OB_CANT_AGGREGATE_3COLLATIONS] = 600;
    ORACLE_STR_ERROR[-OB_CANT_AGGREGATE_3COLLATIONS] =
        "ORA-00600: internal error code, arguments: -5356, Illegal mix of collations";
    ORACLE_STR_USER_ERROR[-OB_CANT_AGGREGATE_3COLLATIONS] =
        "ORA-00600: internal error code, arguments: -5356, Illegal mix of collations";
    ERROR_NAME[-OB_CANT_AGGREGATE_NCOLLATIONS] = "OB_CANT_AGGREGATE_NCOLLATIONS";
    MYSQL_ERRNO[-OB_CANT_AGGREGATE_NCOLLATIONS] = ER_CANT_AGGREGATE_NCOLLATIONS;
    SQLSTATE[-OB_CANT_AGGREGATE_NCOLLATIONS] = "HY000";
    STR_ERROR[-OB_CANT_AGGREGATE_NCOLLATIONS] = "Illegal mix of collations";
    STR_USER_ERROR[-OB_CANT_AGGREGATE_NCOLLATIONS] = "Illegal mix of collations";
    ORACLE_ERRNO[-OB_CANT_AGGREGATE_NCOLLATIONS] = 600;
    ORACLE_STR_ERROR[-OB_CANT_AGGREGATE_NCOLLATIONS] =
        "ORA-00600: internal error code, arguments: -5357, Illegal mix of collations";
    ORACLE_STR_USER_ERROR[-OB_CANT_AGGREGATE_NCOLLATIONS] =
        "ORA-00600: internal error code, arguments: -5357, Illegal mix of collations";
    ERROR_NAME[-OB_ERR_NO_SYS_PRIVILEGE] = "OB_ERR_NO_SYS_PRIVILEGE";
    MYSQL_ERRNO[-OB_ERR_NO_SYS_PRIVILEGE] = -1;
    SQLSTATE[-OB_ERR_NO_SYS_PRIVILEGE] = "HY000";
    STR_ERROR[-OB_ERR_NO_SYS_PRIVILEGE] = "insufficient privileges";
    STR_USER_ERROR[-OB_ERR_NO_SYS_PRIVILEGE] = "insufficient privileges";
    ORACLE_ERRNO[-OB_ERR_NO_SYS_PRIVILEGE] = 1031;
    ORACLE_STR_ERROR[-OB_ERR_NO_SYS_PRIVILEGE] = "ORA-01031: insufficient privileges";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_SYS_PRIVILEGE] = "ORA-01031: insufficient privileges";
    ERROR_NAME[-OB_ERR_NO_LOGIN_PRIVILEGE] = "OB_ERR_NO_LOGIN_PRIVILEGE";
    MYSQL_ERRNO[-OB_ERR_NO_LOGIN_PRIVILEGE] = -1;
    SQLSTATE[-OB_ERR_NO_LOGIN_PRIVILEGE] = "HY000";
    STR_ERROR[-OB_ERR_NO_LOGIN_PRIVILEGE] = "user lacks CREATE SESSION privilege logon denied";
    STR_USER_ERROR[-OB_ERR_NO_LOGIN_PRIVILEGE] = "user %.*s lacks CREATE SESSION privilege; logon denied";
    ORACLE_ERRNO[-OB_ERR_NO_LOGIN_PRIVILEGE] = 1045;
    ORACLE_STR_ERROR[-OB_ERR_NO_LOGIN_PRIVILEGE] = "ORA-01045: user lacks CREATE SESSION privilege; logon denied";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_LOGIN_PRIVILEGE] =
        "ORA-01045: user %.*s lacks CREATE SESSION privilege; logon denied";
    ERROR_NAME[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] =
        "OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT";
    MYSQL_ERRNO[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] = OB_ERR_NO_GRANT;
    SQLSTATE[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] = "42000";
    STR_ERROR[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] = "No such grant defined";
    STR_USER_ERROR[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] = "No such grant defined";
    ORACLE_ERRNO[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] = 1927;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] =
        "ORA-01927: cannot REVOKE privileges you did not grant";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT] =
        "ORA-01927: cannot REVOKE privileges you did not grant";
    ERROR_NAME[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] = "OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO";
    MYSQL_ERRNO[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] = -1;
    SQLSTATE[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] = "HY000";
    STR_ERROR[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] = "system privileges not granted to";
    STR_USER_ERROR[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] = "system privileges not granted to '%.*s'";
    ORACLE_ERRNO[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] = 1952;
    ORACLE_STR_ERROR[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] = "ORA-01952: system privileges not granted to";
    ORACLE_STR_USER_ERROR[-OB_ERR_SYSTEM_PRIVILEGES_NOT_GRANTED_TO] =
        "ORA-01952: system privileges not granted to '%.*s'";
    ERROR_NAME[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] =
        "OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES";
    MYSQL_ERRNO[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] = -1;
    SQLSTATE[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] =
        "only SELECT and ALTER privileges are valid for sequences";
    STR_USER_ERROR[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] =
        "only SELECT and ALTER privileges are valid for sequences";
    ORACLE_ERRNO[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] = 2205;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] =
        "ORA-02205: only SELECT and ALTER privileges are valid for sequences";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES] =
        "ORA-02205: only SELECT and ALTER privileges are valid for sequences";
    ERROR_NAME[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] = "OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES";
    MYSQL_ERRNO[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] = -1;
    SQLSTATE[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] = "HY000";
    STR_ERROR[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] = "EXECUTE privilege not allowed for tables";
    STR_USER_ERROR[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] = "EXECUTE privilege not allowed for tables";
    ORACLE_ERRNO[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] = 2224;
    ORACLE_STR_ERROR[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] =
        "ORA-02224: EXECUTE privilege not allowed for tables";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES] =
        "ORA-02224: EXECUTE privilege not allowed for tables";
    ERROR_NAME[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] =
        "OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES";
    MYSQL_ERRNO[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] = -1;
    SQLSTATE[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] =
        "only EXECUTE and DEBUG privileges are valid for procedures";
    STR_USER_ERROR[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] =
        "only EXECUTE and DEBUG privileges are valid for procedures";
    ORACLE_ERRNO[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] = 2225;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] =
        "ORA-02225: only EXECUTE and DEBUG privileges are valid for procedures";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES] =
        "ORA-02225: only EXECUTE and DEBUG privileges are valid for procedures";
    ERROR_NAME[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] =
        "OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES";
    MYSQL_ERRNO[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] = -1;
    SQLSTATE[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] =
        "only EXECUTE, DEBUG, and UNDER privileges are valid for types";
    STR_USER_ERROR[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] =
        "only EXECUTE, DEBUG, and UNDER privileges are valid for types";
    ORACLE_ERRNO[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] = 2305;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] =
        "ORA-02305: only EXECUTE, DEBUG, and UNDER privileges are valid for types";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES] =
        "ORA-02305: only EXECUTE, DEBUG, and UNDER privileges are valid for types";
    ERROR_NAME[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] = "OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE";
    MYSQL_ERRNO[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] = -1;
    SQLSTATE[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] = "HY000";
    STR_ERROR[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] = "ADMIN option not granted for role";
    STR_USER_ERROR[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] = "ADMIN option not granted for role '%.*s'";
    ORACLE_ERRNO[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] = 1932;
    ORACLE_STR_ERROR[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] = "ORA-01932: ADMIN option not granted for role";
    ORACLE_STR_USER_ERROR[-OB_ERR_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE] =
        "ORA-01932: ADMIN option not granted for role '%.*s'";
    ERROR_NAME[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = "OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = "user or role does not exist";
    STR_USER_ERROR[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = "user or role '%.*s' does not exist";
    ORACLE_ERRNO[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = 1917;
    ORACLE_STR_ERROR[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = "ORA-01917: user or role does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_USER_OR_ROLE_DOES_NOT_EXIST] = "ORA-01917: user or role '%.*s' does not exist";
    ERROR_NAME[-OB_ERR_MISSING_ON_KEYWORD] = "OB_ERR_MISSING_ON_KEYWORD";
    MYSQL_ERRNO[-OB_ERR_MISSING_ON_KEYWORD] = -1;
    SQLSTATE[-OB_ERR_MISSING_ON_KEYWORD] = "HY000";
    STR_ERROR[-OB_ERR_MISSING_ON_KEYWORD] = "missing ON keyword";
    STR_USER_ERROR[-OB_ERR_MISSING_ON_KEYWORD] = "missing ON keyword";
    ORACLE_ERRNO[-OB_ERR_MISSING_ON_KEYWORD] = 969;
    ORACLE_STR_ERROR[-OB_ERR_MISSING_ON_KEYWORD] = "ORA-00969: missing ON keyword";
    ORACLE_STR_USER_ERROR[-OB_ERR_MISSING_ON_KEYWORD] = "ORA-00969: missing ON keyword";
    ERROR_NAME[-OB_ERR_NO_GRANT_OPTION] = "OB_ERR_NO_GRANT_OPTION";
    MYSQL_ERRNO[-OB_ERR_NO_GRANT_OPTION] = -1;
    SQLSTATE[-OB_ERR_NO_GRANT_OPTION] = "HY000";
    STR_ERROR[-OB_ERR_NO_GRANT_OPTION] = "grant option does not exists";
    STR_USER_ERROR[-OB_ERR_NO_GRANT_OPTION] = "grant option does not exist for '%.*s.%.*s'";
    ORACLE_ERRNO[-OB_ERR_NO_GRANT_OPTION] = 1720;
    ORACLE_STR_ERROR[-OB_ERR_NO_GRANT_OPTION] = "ORA-01720: grant option does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_GRANT_OPTION] = "ORA-01720: grant option does not exist for '%.*s.%.*s'";
    ERROR_NAME[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] =
        "OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS";
    MYSQL_ERRNO[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] = -1;
    SQLSTATE[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] = "HY000";
    STR_ERROR[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] = "ALTER, INDEX and EXECUTE not allowed for views";
    STR_USER_ERROR[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] =
        "ALTER, INDEX and EXECUTE not allowed for views";
    ORACLE_ERRNO[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] = 2204;
    ORACLE_STR_ERROR[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] =
        "ORA-02204: ALTER, INDEX and EXECUTE not allowed for views";
    ORACLE_STR_USER_ERROR[-OB_ERR_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS] =
        "ORA-02204: ALTER, INDEX and EXECUTE not allowed for views";
    ERROR_NAME[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = "OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED";
    MYSQL_ERRNO[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = -1;
    SQLSTATE[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = "HY000";
    STR_ERROR[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = "circular role grant detected";
    STR_USER_ERROR[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = "circular role grant detected";
    ORACLE_ERRNO[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = 1934;
    ORACLE_STR_ERROR[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = "ORA-01934: circular role grant detected";
    ORACLE_STR_USER_ERROR[-OB_ERR_CIRCULAR_ROLE_GRANT_DETECTED] = "ORA-01934: circular role grant detected";
    ERROR_NAME[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = "OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES";
    MYSQL_ERRNO[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = -1;
    SQLSTATE[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = "invalid privilege on directories";
    STR_USER_ERROR[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = "invalid privilege on directories";
    ORACLE_ERRNO[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = 22928;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = "ORA-22928: invalid privilege on directories";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES] = "ORA-22928: invalid privilege on directories";
    ERROR_NAME[-OB_ERR_DIRECTORY_ACCESS_DENIED] = "OB_ERR_DIRECTORY_ACCESS_DENIED";
    MYSQL_ERRNO[-OB_ERR_DIRECTORY_ACCESS_DENIED] = -1;
    SQLSTATE[-OB_ERR_DIRECTORY_ACCESS_DENIED] = "HY000";
    STR_ERROR[-OB_ERR_DIRECTORY_ACCESS_DENIED] = "directory access denied";
    STR_USER_ERROR[-OB_ERR_DIRECTORY_ACCESS_DENIED] = "directory access denied";
    ORACLE_ERRNO[-OB_ERR_DIRECTORY_ACCESS_DENIED] = 29289;
    ORACLE_STR_ERROR[-OB_ERR_DIRECTORY_ACCESS_DENIED] = "ORA-29289: directory access denied";
    ORACLE_STR_USER_ERROR[-OB_ERR_DIRECTORY_ACCESS_DENIED] = "ORA-29289: directory access denied";
    ERROR_NAME[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = "OB_ERR_MISSING_OR_INVALID_ROLE_NAME";
    MYSQL_ERRNO[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = -1;
    SQLSTATE[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = "HY000";
    STR_ERROR[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = "missing or invalid role name";
    STR_USER_ERROR[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = "missing or invalid role name";
    ORACLE_ERRNO[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = 1937;
    ORACLE_STR_ERROR[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = "ORA-01937: missing or invalid role name";
    ORACLE_STR_USER_ERROR[-OB_ERR_MISSING_OR_INVALID_ROLE_NAME] = "ORA-01937: missing or invalid role name";
    ERROR_NAME[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] = "OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] = "role not granted or does not exist";
    STR_USER_ERROR[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] = "role '%.*s' not granted or does not exist";
    ORACLE_ERRNO[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] = 1924;
    ORACLE_STR_ERROR[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] = "ORA-01924: role not granted or does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST] =
        "ORA-01924: role '%.*s' not granted or does not exist";
    ERROR_NAME[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] = "OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER";
    MYSQL_ERRNO[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] = -1;
    SQLSTATE[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] = "HY000";
    STR_ERROR[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] = "DEFAULT ROLE not granted to user";
    STR_USER_ERROR[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] = "DEFAULT ROLE '%.*s' not granted to user";
    ORACLE_ERRNO[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] = 1955;
    ORACLE_STR_ERROR[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] = "ORA-01955: DEFAULT ROLE not granted to user";
    ORACLE_STR_USER_ERROR[-OB_ERR_DEFAULT_ROLE_NOT_GRANTED_TO_USER] =
        "ORA-01955: DEFAULT ROLE '%.*s' not granted to user";
    ERROR_NAME[-OB_ERR_ROLE_NOT_GRANTED_TO] = "OB_ERR_ROLE_NOT_GRANTED_TO";
    MYSQL_ERRNO[-OB_ERR_ROLE_NOT_GRANTED_TO] = -1;
    SQLSTATE[-OB_ERR_ROLE_NOT_GRANTED_TO] = "HY000";
    STR_ERROR[-OB_ERR_ROLE_NOT_GRANTED_TO] = "ROLE not granted to";
    STR_USER_ERROR[-OB_ERR_ROLE_NOT_GRANTED_TO] = "ROLE '%.*s' not granted to '%.*s'";
    ORACLE_ERRNO[-OB_ERR_ROLE_NOT_GRANTED_TO] = 1951;
    ORACLE_STR_ERROR[-OB_ERR_ROLE_NOT_GRANTED_TO] = "ORA-01951: ROLE not granted to";
    ORACLE_STR_USER_ERROR[-OB_ERR_ROLE_NOT_GRANTED_TO] = "ORA-01951: ROLE '%.*s' not granted to '%.*s'";
    ERROR_NAME[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] = "OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION";
    MYSQL_ERRNO[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] = -1;
    SQLSTATE[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] = "cannot GRANT to a role WITH GRANT OPTION";
    STR_USER_ERROR[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] = "cannot GRANT to a role WITH GRANT OPTION";
    ORACLE_ERRNO[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] = 1926;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] =
        "ORA-01926: cannot GRANT to a role WITH GRANT OPTION";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_GRANT_TO_A_ROLE_WITH_GRANT_OPTION] =
        "ORA-01926: cannot GRANT to a role WITH GRANT OPTION";
    ERROR_NAME[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = "OB_ERR_DUPLICATE_USERNAME_IN_LIST";
    MYSQL_ERRNO[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = -1;
    SQLSTATE[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = "HY000";
    STR_ERROR[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = "duplicate username in list";
    STR_USER_ERROR[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = "duplicate username in list";
    ORACLE_ERRNO[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = 1700;
    ORACLE_STR_ERROR[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = "ORA-01700: duplicate username in list";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUPLICATE_USERNAME_IN_LIST] = "ORA-01700: duplicate username in list";
    ERROR_NAME[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = "OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE";
    MYSQL_ERRNO[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = -1;
    SQLSTATE[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = "cannot grant string to a role";
    STR_USER_ERROR[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = "cannot grant %.*s to a role";
    ORACLE_ERRNO[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = 1931;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = "ORA-01931: cannot grant string to a role";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_GRANT_STRING_TO_A_ROLE] = "ORA-01931: cannot grant %.*s to a role";
    ERROR_NAME[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] =
        "OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE";
    MYSQL_ERRNO[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] = -1;
    SQLSTATE[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] = "HY000";
    STR_ERROR[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] =
        "CASCADE CONSTRAINTS must be specified to perform this revoke";
    STR_USER_ERROR[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] =
        "CASCADE CONSTRAINTS must be specified to perform this revoke";
    ORACLE_ERRNO[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] = 1981;
    ORACLE_STR_ERROR[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] =
        "ORA-01981: CASCADE CONSTRAINTS must be specified to perform this revoke";
    ORACLE_STR_USER_ERROR[-OB_ERR_CASCADE_CONSTRAINTS_MUST_BE_SPECIFIED_TO_PERFORM_THIS_REVOKE] =
        "ORA-01981: CASCADE CONSTRAINTS must be specified to perform this revoke";
    ERROR_NAME[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] =
        "OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF";
    MYSQL_ERRNO[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] = -1;
    SQLSTATE[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] = "HY000";
    STR_ERROR[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] =
        "you may not GRANT/REVOKE privileges to/from yourself";
    STR_USER_ERROR[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] =
        "you may not GRANT/REVOKE privileges to/from yourself";
    ORACLE_ERRNO[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] = 1749;
    ORACLE_STR_ERROR[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] =
        "ORA-01749: you may not GRANT/REVOKE privileges to/from yourself";
    ORACLE_STR_USER_ERROR[-OB_ERR_YOU_MAY_NOT_REVOKE_PRIVILEGES_FROM_YOURSELF] =
        "ORA-01749: you may not GRANT/REVOKE privileges to/from yourself";
    ERROR_NAME[-OB_ERR_SP_ALREADY_EXISTS] = "OB_ERR_SP_ALREADY_EXISTS";
    MYSQL_ERRNO[-OB_ERR_SP_ALREADY_EXISTS] = ER_SP_ALREADY_EXISTS;
    SQLSTATE[-OB_ERR_SP_ALREADY_EXISTS] = "42000";
    STR_ERROR[-OB_ERR_SP_ALREADY_EXISTS] = "procedure/function already exists";
    STR_USER_ERROR[-OB_ERR_SP_ALREADY_EXISTS] = "%s %.*s already exists";
    ORACLE_ERRNO[-OB_ERR_SP_ALREADY_EXISTS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_ALREADY_EXISTS] =
        "ORA-00600: internal error code, arguments: -5541, procedure/function already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_ALREADY_EXISTS] =
        "ORA-00600: internal error code, arguments: -5541, %s %.*s already exists";
    ERROR_NAME[-OB_ERR_SP_DOES_NOT_EXIST] = "OB_ERR_SP_DOES_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_SP_DOES_NOT_EXIST] = ER_SP_DOES_NOT_EXIST;
    SQLSTATE[-OB_ERR_SP_DOES_NOT_EXIST] = "42000";
    STR_ERROR[-OB_ERR_SP_DOES_NOT_EXIST] = "procedure/function does not exist";
    STR_USER_ERROR[-OB_ERR_SP_DOES_NOT_EXIST] = "%s %.*s.%.*s does not exist";
    ORACLE_ERRNO[-OB_ERR_SP_DOES_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DOES_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5542, procedure/function does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DOES_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5542, %s %.*s.%.*s does not exist";
    ERROR_NAME[-OB_ERR_SP_UNDECLARED_VAR] = "OB_ERR_SP_UNDECLARED_VAR";
    MYSQL_ERRNO[-OB_ERR_SP_UNDECLARED_VAR] = ER_SP_UNDECLARED_VAR;
    SQLSTATE[-OB_ERR_SP_UNDECLARED_VAR] = "42000";
    STR_ERROR[-OB_ERR_SP_UNDECLARED_VAR] = "Undeclared variable";
    STR_USER_ERROR[-OB_ERR_SP_UNDECLARED_VAR] = "Undeclared variable: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_UNDECLARED_VAR] = 201;
    ORACLE_STR_ERROR[-OB_ERR_SP_UNDECLARED_VAR] = "PLS-00201: identifier must be declared";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_UNDECLARED_VAR] = "PLS-00201: identifier '%.*s' must be declared";
    ERROR_NAME[-OB_ERR_SP_UNDECLARED_TYPE] = "OB_ERR_SP_UNDECLARED_TYPE";
    MYSQL_ERRNO[-OB_ERR_SP_UNDECLARED_TYPE] = ER_SP_UNDECLARED_VAR;
    SQLSTATE[-OB_ERR_SP_UNDECLARED_TYPE] = "42000";
    STR_ERROR[-OB_ERR_SP_UNDECLARED_TYPE] = "Undeclared type";
    STR_USER_ERROR[-OB_ERR_SP_UNDECLARED_TYPE] = "Undeclared type: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_UNDECLARED_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_UNDECLARED_TYPE] = "ORA-00600: internal error code, arguments: -5544, Undeclared type";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_UNDECLARED_TYPE] =
        "ORA-00600: internal error code, arguments: -5544, Undeclared type: %.*s";
    ERROR_NAME[-OB_ERR_SP_COND_MISMATCH] = "OB_ERR_SP_COND_MISMATCH";
    MYSQL_ERRNO[-OB_ERR_SP_COND_MISMATCH] = ER_SP_COND_MISMATCH;
    SQLSTATE[-OB_ERR_SP_COND_MISMATCH] = "42000";
    STR_ERROR[-OB_ERR_SP_COND_MISMATCH] = "Undefined CONDITION";
    STR_USER_ERROR[-OB_ERR_SP_COND_MISMATCH] = "Undefined CONDITION: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_COND_MISMATCH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_COND_MISMATCH] =
        "ORA-00600: internal error code, arguments: -5545, Undefined CONDITION";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_COND_MISMATCH] =
        "ORA-00600: internal error code, arguments: -5545, Undefined CONDITION: %.*s";
    ERROR_NAME[-OB_ERR_SP_LILABEL_MISMATCH] = "OB_ERR_SP_LILABEL_MISMATCH";
    MYSQL_ERRNO[-OB_ERR_SP_LILABEL_MISMATCH] = ER_SP_LILABEL_MISMATCH;
    SQLSTATE[-OB_ERR_SP_LILABEL_MISMATCH] = "42000";
    STR_ERROR[-OB_ERR_SP_LILABEL_MISMATCH] = "no matching label";
    STR_USER_ERROR[-OB_ERR_SP_LILABEL_MISMATCH] = "no matching label: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_LILABEL_MISMATCH] = 201;
    ORACLE_STR_ERROR[-OB_ERR_SP_LILABEL_MISMATCH] = "PLS-00201: identifier must be declared";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_LILABEL_MISMATCH] = "PLS-00201: identifier '%.*s' must be declared";
    ERROR_NAME[-OB_ERR_SP_CURSOR_MISMATCH] = "OB_ERR_SP_CURSOR_MISMATCH";
    MYSQL_ERRNO[-OB_ERR_SP_CURSOR_MISMATCH] = ER_SP_CURSOR_MISMATCH;
    SQLSTATE[-OB_ERR_SP_CURSOR_MISMATCH] = "42000";
    STR_ERROR[-OB_ERR_SP_CURSOR_MISMATCH] = "Undefined CURSOR";
    STR_USER_ERROR[-OB_ERR_SP_CURSOR_MISMATCH] = "Undefined CURSOR: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_CURSOR_MISMATCH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_CURSOR_MISMATCH] = "ORA-00600: internal error code, arguments: -5547, Undefined CURSOR";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_CURSOR_MISMATCH] =
        "ORA-00600: internal error code, arguments: -5547, Undefined CURSOR: %.*s";
    ERROR_NAME[-OB_ERR_SP_DUP_PARAM] = "OB_ERR_SP_DUP_PARAM";
    MYSQL_ERRNO[-OB_ERR_SP_DUP_PARAM] = ER_SP_DUP_PARAM;
    SQLSTATE[-OB_ERR_SP_DUP_PARAM] = "42000";
    STR_ERROR[-OB_ERR_SP_DUP_PARAM] = "Duplicate parameter";
    STR_USER_ERROR[-OB_ERR_SP_DUP_PARAM] = "Duplicate parameter: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_DUP_PARAM] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DUP_PARAM] = "ORA-00600: internal error code, arguments: -5548, Duplicate parameter";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DUP_PARAM] =
        "ORA-00600: internal error code, arguments: -5548, Duplicate parameter: %.*s";
    ERROR_NAME[-OB_ERR_SP_DUP_VAR] = "OB_ERR_SP_DUP_VAR";
    MYSQL_ERRNO[-OB_ERR_SP_DUP_VAR] = ER_SP_DUP_VAR;
    SQLSTATE[-OB_ERR_SP_DUP_VAR] = "42000";
    STR_ERROR[-OB_ERR_SP_DUP_VAR] = "Duplicate variable";
    STR_USER_ERROR[-OB_ERR_SP_DUP_VAR] = "Duplicate variable: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_DUP_VAR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DUP_VAR] = "ORA-00600: internal error code, arguments: -5549, Duplicate variable";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DUP_VAR] =
        "ORA-00600: internal error code, arguments: -5549, Duplicate variable: %.*s";
    ERROR_NAME[-OB_ERR_SP_DUP_TYPE] = "OB_ERR_SP_DUP_TYPE";
    MYSQL_ERRNO[-OB_ERR_SP_DUP_TYPE] = ER_SP_DUP_PARAM;
    SQLSTATE[-OB_ERR_SP_DUP_TYPE] = "42000";
    STR_ERROR[-OB_ERR_SP_DUP_TYPE] = "Duplicate type";
    STR_USER_ERROR[-OB_ERR_SP_DUP_TYPE] = "Duplicate type: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_DUP_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DUP_TYPE] = "ORA-00600: internal error code, arguments: -5550, Duplicate type";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DUP_TYPE] =
        "ORA-00600: internal error code, arguments: -5550, Duplicate type: %.*s";
    ERROR_NAME[-OB_ERR_SP_DUP_CONDITION] = "OB_ERR_SP_DUP_CONDITION";
    MYSQL_ERRNO[-OB_ERR_SP_DUP_CONDITION] = ER_SP_DUP_COND;
    SQLSTATE[-OB_ERR_SP_DUP_CONDITION] = "42000";
    STR_ERROR[-OB_ERR_SP_DUP_CONDITION] = "Duplicate condition";
    STR_USER_ERROR[-OB_ERR_SP_DUP_CONDITION] = "Duplicate condition: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_DUP_CONDITION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DUP_CONDITION] =
        "ORA-00600: internal error code, arguments: -5551, Duplicate condition";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DUP_CONDITION] =
        "ORA-00600: internal error code, arguments: -5551, Duplicate condition: %.*s";
    ERROR_NAME[-OB_ERR_SP_DUP_LABEL] = "OB_ERR_SP_DUP_LABEL";
    MYSQL_ERRNO[-OB_ERR_SP_DUP_LABEL] = ER_SP_DUP_PARAM;
    SQLSTATE[-OB_ERR_SP_DUP_LABEL] = "42000";
    STR_ERROR[-OB_ERR_SP_DUP_LABEL] = "Duplicate label";
    STR_USER_ERROR[-OB_ERR_SP_DUP_LABEL] = "Duplicate label: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_DUP_LABEL] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DUP_LABEL] = "ORA-00600: internal error code, arguments: -5552, Duplicate label";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DUP_LABEL] =
        "ORA-00600: internal error code, arguments: -5552, Duplicate label: %.*s";
    ERROR_NAME[-OB_ERR_SP_DUP_CURSOR] = "OB_ERR_SP_DUP_CURSOR";
    MYSQL_ERRNO[-OB_ERR_SP_DUP_CURSOR] = ER_SP_DUP_CURS;
    SQLSTATE[-OB_ERR_SP_DUP_CURSOR] = "42000";
    STR_ERROR[-OB_ERR_SP_DUP_CURSOR] = "Duplicate cursor";
    STR_USER_ERROR[-OB_ERR_SP_DUP_CURSOR] = "Duplicate cursor: %.*s";
    ORACLE_ERRNO[-OB_ERR_SP_DUP_CURSOR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DUP_CURSOR] = "ORA-00600: internal error code, arguments: -5553, Duplicate cursor";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DUP_CURSOR] =
        "ORA-00600: internal error code, arguments: -5553, Duplicate cursor: %.*s";
    ERROR_NAME[-OB_ERR_SP_INVALID_FETCH_ARG] = "OB_ERR_SP_INVALID_FETCH_ARG";
    MYSQL_ERRNO[-OB_ERR_SP_INVALID_FETCH_ARG] = ER_SP_WRONG_NO_OF_FETCH_ARGS;
    SQLSTATE[-OB_ERR_SP_INVALID_FETCH_ARG] = "HY000";
    STR_ERROR[-OB_ERR_SP_INVALID_FETCH_ARG] = "Incorrect number of FETCH variables";
    STR_USER_ERROR[-OB_ERR_SP_INVALID_FETCH_ARG] = "Incorrect number of FETCH variables";
    ORACLE_ERRNO[-OB_ERR_SP_INVALID_FETCH_ARG] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_INVALID_FETCH_ARG] =
        "ORA-00600: internal error code, arguments: -5554, Incorrect number of FETCH variables";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_INVALID_FETCH_ARG] =
        "ORA-00600: internal error code, arguments: -5554, Incorrect number of FETCH variables";
    ERROR_NAME[-OB_ERR_SP_WRONG_ARG_NUM] = "OB_ERR_SP_WRONG_ARG_NUM";
    MYSQL_ERRNO[-OB_ERR_SP_WRONG_ARG_NUM] = ER_SP_WRONG_NO_OF_ARGS;
    SQLSTATE[-OB_ERR_SP_WRONG_ARG_NUM] = "42000";
    STR_ERROR[-OB_ERR_SP_WRONG_ARG_NUM] = "Incorrect number of arguments";
    STR_USER_ERROR[-OB_ERR_SP_WRONG_ARG_NUM] = "Incorrect number of arguments for %s %s; expected %u, got %u";
    ORACLE_ERRNO[-OB_ERR_SP_WRONG_ARG_NUM] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_WRONG_ARG_NUM] =
        "ORA-00600: internal error code, arguments: -5555, Incorrect number of arguments";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_WRONG_ARG_NUM] = "ORA-00600: internal error code, arguments: -5555, Incorrect "
                                                      "number of arguments for %s %s; expected %u, got %u";
    ERROR_NAME[-OB_ERR_SP_UNHANDLED_EXCEPTION] = "OB_ERR_SP_UNHANDLED_EXCEPTION";
    MYSQL_ERRNO[-OB_ERR_SP_UNHANDLED_EXCEPTION] = ER_SIGNAL_EXCEPTION;
    SQLSTATE[-OB_ERR_SP_UNHANDLED_EXCEPTION] = "HY000";
    STR_ERROR[-OB_ERR_SP_UNHANDLED_EXCEPTION] = "Unhandled exception has occurred in PL";
    STR_USER_ERROR[-OB_ERR_SP_UNHANDLED_EXCEPTION] = "Unhandled user-defined exception condition";
    ORACLE_ERRNO[-OB_ERR_SP_UNHANDLED_EXCEPTION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_UNHANDLED_EXCEPTION] =
        "ORA-00600: internal error code, arguments: -5556, Unhandled exception has occurred in PL";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_UNHANDLED_EXCEPTION] =
        "ORA-00600: internal error code, arguments: -5556, Unhandled user-defined exception condition";
    ERROR_NAME[-OB_ERR_SP_BAD_CONDITION_TYPE] = "OB_ERR_SP_BAD_CONDITION_TYPE";
    MYSQL_ERRNO[-OB_ERR_SP_BAD_CONDITION_TYPE] = ER_SIGNAL_BAD_CONDITION_TYPE;
    SQLSTATE[-OB_ERR_SP_BAD_CONDITION_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_SP_BAD_CONDITION_TYPE] = "SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE";
    STR_USER_ERROR[-OB_ERR_SP_BAD_CONDITION_TYPE] = "SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE";
    ORACLE_ERRNO[-OB_ERR_SP_BAD_CONDITION_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_BAD_CONDITION_TYPE] = "ORA-00600: internal error code, arguments: -5557, "
                                                      "SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_BAD_CONDITION_TYPE] =
        "ORA-00600: internal error code, arguments: -5557, SIGNAL/RESIGNAL can only use a CONDITION defined with "
        "SQLSTATE";
    ERROR_NAME[-OB_ERR_PACKAGE_ALREADY_EXISTS] = "OB_ERR_PACKAGE_ALREADY_EXISTS";
    MYSQL_ERRNO[-OB_ERR_PACKAGE_ALREADY_EXISTS] = -1;
    SQLSTATE[-OB_ERR_PACKAGE_ALREADY_EXISTS] = "42000";
    STR_ERROR[-OB_ERR_PACKAGE_ALREADY_EXISTS] = "package already exists";
    STR_USER_ERROR[-OB_ERR_PACKAGE_ALREADY_EXISTS] = "%s \'%.*s.%.*s\' already exists";
    ORACLE_ERRNO[-OB_ERR_PACKAGE_ALREADY_EXISTS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PACKAGE_ALREADY_EXISTS] =
        "ORA-00600: internal error code, arguments: -5558, package already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_PACKAGE_ALREADY_EXISTS] =
        "ORA-00600: internal error code, arguments: -5558, %s \'%.*s.%.*s\' already exists";
    ERROR_NAME[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] = "OB_ERR_PACKAGE_DOSE_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] = "42000";
    STR_ERROR[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] = "package does not exist";
    STR_USER_ERROR[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] = "%s \'%.*s.%.*s\' does not exist";
    ORACLE_ERRNO[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5559, package does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_PACKAGE_DOSE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -5559, %s \'%.*s.%.*s\' does not exist";
    ERROR_NAME[-OB_EER_UNKNOWN_STMT_HANDLER] = "OB_EER_UNKNOWN_STMT_HANDLER";
    MYSQL_ERRNO[-OB_EER_UNKNOWN_STMT_HANDLER] = ER_UNKNOWN_STMT_HANDLER;
    SQLSTATE[-OB_EER_UNKNOWN_STMT_HANDLER] = "HY000";
    STR_ERROR[-OB_EER_UNKNOWN_STMT_HANDLER] = "Unknown prepared statement handle";
    STR_USER_ERROR[-OB_EER_UNKNOWN_STMT_HANDLER] = "Unknown prepared statement handle";
    ORACLE_ERRNO[-OB_EER_UNKNOWN_STMT_HANDLER] = 600;
    ORACLE_STR_ERROR[-OB_EER_UNKNOWN_STMT_HANDLER] =
        "ORA-00600: internal error code, arguments: -5560, Unknown prepared statement handle";
    ORACLE_STR_USER_ERROR[-OB_EER_UNKNOWN_STMT_HANDLER] =
        "ORA-00600: internal error code, arguments: -5560, Unknown prepared statement handle";
    ERROR_NAME[-OB_ERR_INVALID_WINDOW_FUNC_USE] = "OB_ERR_INVALID_WINDOW_FUNC_USE";
    MYSQL_ERRNO[-OB_ERR_INVALID_WINDOW_FUNC_USE] = -1;
    SQLSTATE[-OB_ERR_INVALID_WINDOW_FUNC_USE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_WINDOW_FUNC_USE] = "Invalid use of window function";
    STR_USER_ERROR[-OB_ERR_INVALID_WINDOW_FUNC_USE] = "Invalid use of window function";
    ORACLE_ERRNO[-OB_ERR_INVALID_WINDOW_FUNC_USE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_WINDOW_FUNC_USE] =
        "ORA-00600: internal error code, arguments: -5561, Invalid use of window function";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_WINDOW_FUNC_USE] =
        "ORA-00600: internal error code, arguments: -5561, Invalid use of window function";
    ERROR_NAME[-OB_ERR_CONSTRAINT_DUPLICATE] = "OB_ERR_CONSTRAINT_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_CONSTRAINT_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_CONSTRAINT_DUPLICATE] = "HY000";
    STR_ERROR[-OB_ERR_CONSTRAINT_DUPLICATE] = "Duplicate constraint name";
    STR_USER_ERROR[-OB_ERR_CONSTRAINT_DUPLICATE] = "Duplicate constraint name '%.*s'";
    ORACLE_ERRNO[-OB_ERR_CONSTRAINT_DUPLICATE] = 1;
    ORACLE_STR_ERROR[-OB_ERR_CONSTRAINT_DUPLICATE] = "ORA-00001: unique constraint violated";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONSTRAINT_DUPLICATE] = "ORA-00001: unique constraint (%.*s) violated";
    ERROR_NAME[-OB_ERR_CONTRAINT_NOT_FOUND] = "OB_ERR_CONTRAINT_NOT_FOUND";
    MYSQL_ERRNO[-OB_ERR_CONTRAINT_NOT_FOUND] = -1;
    SQLSTATE[-OB_ERR_CONTRAINT_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_ERR_CONTRAINT_NOT_FOUND] = "Constraint not found";
    STR_USER_ERROR[-OB_ERR_CONTRAINT_NOT_FOUND] = "Constraint not found";
    ORACLE_ERRNO[-OB_ERR_CONTRAINT_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CONTRAINT_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -5563, Constraint not found";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONTRAINT_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -5563, Constraint not found";
    ERROR_NAME[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] = "OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX";
    MYSQL_ERRNO[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] = -1;
    SQLSTATE[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] = "HY000";
    STR_ERROR[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] = "Duplicate alter index operations";
    STR_USER_ERROR[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] = "Duplicate alter index operations on column \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] =
        "ORA-00600: internal error code, arguments: -5564, Duplicate alter index operations";
    ORACLE_STR_USER_ERROR[-OB_ERR_ALTER_TABLE_ALTER_DUPLICATED_INDEX] =
        "ORA-00600: internal error code, arguments: -5564, Duplicate alter index operations on column \'%.*s\'";
    ERROR_NAME[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] = "OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM";
    MYSQL_ERRNO[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] = ER_INVALID_ARGUMENT_FOR_LOGARITHM;
    SQLSTATE[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] = "2201E";
    STR_ERROR[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] = "Invalid argument for logarithm";
    STR_USER_ERROR[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] = "Invalid argument for logarithm";
    ORACLE_ERRNO[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] = 600;
    ORACLE_STR_ERROR[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] =
        "ORA-00600: internal error code, arguments: -5565, Invalid argument for logarithm";
    ORACLE_STR_USER_ERROR[-OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM] =
        "ORA-00600: internal error code, arguments: -5565, Invalid argument for logarithm";
    ERROR_NAME[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] = "OB_ERR_REORGANIZE_OUTSIDE_RANGE";
    MYSQL_ERRNO[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] = ER_REORG_OUTSIDE_RANGE;
    SQLSTATE[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] = "Reorganize of range partitions cannot change total ranges except "
                                                  "for last partition where it can extend the range";
    STR_USER_ERROR[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] = "Reorganize of range partitions cannot change total ranges "
                                                       "except for last partition where it can extend the range";
    ORACLE_ERRNO[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] =
        "ORA-00600: internal error code, arguments: -5566, Reorganize of range partitions cannot change total ranges "
        "except for last partition where it can extend the range";
    ORACLE_STR_USER_ERROR[-OB_ERR_REORGANIZE_OUTSIDE_RANGE] =
        "ORA-00600: internal error code, arguments: -5566, Reorganize of range partitions cannot change total ranges "
        "except for last partition where it can extend the range";
    ERROR_NAME[-OB_ER_SP_RECURSION_LIMIT] = "OB_ER_SP_RECURSION_LIMIT";
    MYSQL_ERRNO[-OB_ER_SP_RECURSION_LIMIT] = ER_SP_RECURSION_LIMIT;
    SQLSTATE[-OB_ER_SP_RECURSION_LIMIT] = "HY000";
    STR_ERROR[-OB_ER_SP_RECURSION_LIMIT] = "Recursive limit was exceeded";
    STR_USER_ERROR[-OB_ER_SP_RECURSION_LIMIT] =
        "Recursive limit %ld (as set by the max_sp_recursion_depth variable) was exceeded for routine";
    ORACLE_ERRNO[-OB_ER_SP_RECURSION_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_RECURSION_LIMIT] =
        "ORA-00600: internal error code, arguments: -5567, Recursive limit was exceeded";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_RECURSION_LIMIT] =
        "ORA-00600: internal error code, arguments: -5567, Recursive limit %ld (as set by the max_sp_recursion_depth "
        "variable) was exceeded for routine";
    ERROR_NAME[-OB_ER_UNSUPPORTED_PS] = "OB_ER_UNSUPPORTED_PS";
    MYSQL_ERRNO[-OB_ER_UNSUPPORTED_PS] = ER_UNSUPPORTED_PS;
    SQLSTATE[-OB_ER_UNSUPPORTED_PS] = "HY000";
    STR_ERROR[-OB_ER_UNSUPPORTED_PS] = "This command is not supported in the prepared statement protocol yet";
    STR_USER_ERROR[-OB_ER_UNSUPPORTED_PS] = "This command is not supported in the prepared statement protocol yet";
    ORACLE_ERRNO[-OB_ER_UNSUPPORTED_PS] = 600;
    ORACLE_STR_ERROR[-OB_ER_UNSUPPORTED_PS] = "ORA-00600: internal error code, arguments: -5568, This command is not "
                                              "supported in the prepared statement protocol yet";
    ORACLE_STR_USER_ERROR[-OB_ER_UNSUPPORTED_PS] = "ORA-00600: internal error code, arguments: -5568, This command is "
                                                   "not supported in the prepared statement protocol yet";
    ERROR_NAME[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] = "OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG";
    MYSQL_ERRNO[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] = ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG;
    SQLSTATE[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] = "0A000";
    STR_ERROR[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] = "stmt is not allowed in stored function";
    STR_USER_ERROR[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] = "%s is not allowed in stored function";
    ORACLE_ERRNO[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] = 600;
    ORACLE_STR_ERROR[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] =
        "ORA-00600: internal error code, arguments: -5569, stmt is not allowed in stored function";
    ORACLE_STR_USER_ERROR[-OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG] =
        "ORA-00600: internal error code, arguments: -5569, %s is not allowed in stored function";
    ERROR_NAME[-OB_ER_SP_NO_RECURSION] = "OB_ER_SP_NO_RECURSION";
    MYSQL_ERRNO[-OB_ER_SP_NO_RECURSION] = ER_SP_NO_RECURSION;
    SQLSTATE[-OB_ER_SP_NO_RECURSION] = "HY000";
    STR_ERROR[-OB_ER_SP_NO_RECURSION] = "Recursive stored functions are not allowed.";
    STR_USER_ERROR[-OB_ER_SP_NO_RECURSION] = "Recursive stored functions are not allowed.";
    ORACLE_ERRNO[-OB_ER_SP_NO_RECURSION] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_NO_RECURSION] =
        "ORA-00600: internal error code, arguments: -5570, Recursive stored functions are not allowed.";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_NO_RECURSION] =
        "ORA-00600: internal error code, arguments: -5570, Recursive stored functions are not allowed.";
    ERROR_NAME[-OB_ER_SP_CASE_NOT_FOUND] = "OB_ER_SP_CASE_NOT_FOUND";
    MYSQL_ERRNO[-OB_ER_SP_CASE_NOT_FOUND] = ER_SP_CASE_NOT_FOUND;
    SQLSTATE[-OB_ER_SP_CASE_NOT_FOUND] = "20000";
    STR_ERROR[-OB_ER_SP_CASE_NOT_FOUND] = "Case not found for CASE statement";
    STR_USER_ERROR[-OB_ER_SP_CASE_NOT_FOUND] = "Case not found for CASE statement";
    ORACLE_ERRNO[-OB_ER_SP_CASE_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_CASE_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -5571, Case not found for CASE statement";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_CASE_NOT_FOUND] =
        "ORA-00600: internal error code, arguments: -5571, Case not found for CASE statement";
    ERROR_NAME[-OB_ERR_INVALID_SPLIT_COUNT] = "OB_ERR_INVALID_SPLIT_COUNT";
    MYSQL_ERRNO[-OB_ERR_INVALID_SPLIT_COUNT] = -1;
    SQLSTATE[-OB_ERR_INVALID_SPLIT_COUNT] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SPLIT_COUNT] = "a partition may be split into exactly two new partitions";
    STR_USER_ERROR[-OB_ERR_INVALID_SPLIT_COUNT] = "a partition may be split into exactly two new partitions";
    ORACLE_ERRNO[-OB_ERR_INVALID_SPLIT_COUNT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SPLIT_COUNT] =
        "ORA-00600: internal error code, arguments: -5572, a partition may be split into exactly two new partitions";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SPLIT_COUNT] =
        "ORA-00600: internal error code, arguments: -5572, a partition may be split into exactly two new partitions";
    ERROR_NAME[-OB_ERR_INVALID_SPLIT_GRAMMAR] = "OB_ERR_INVALID_SPLIT_GRAMMAR";
    MYSQL_ERRNO[-OB_ERR_INVALID_SPLIT_GRAMMAR] = -1;
    SQLSTATE[-OB_ERR_INVALID_SPLIT_GRAMMAR] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SPLIT_GRAMMAR] = "this physical attribute may not be specified for a table partition";
    STR_USER_ERROR[-OB_ERR_INVALID_SPLIT_GRAMMAR] =
        "this physical attribute may not be specified for a table partition";
    ORACLE_ERRNO[-OB_ERR_INVALID_SPLIT_GRAMMAR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SPLIT_GRAMMAR] = "ORA-00600: internal error code, arguments: -5573, this physical "
                                                      "attribute may not be specified for a table partition";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SPLIT_GRAMMAR] =
        "ORA-00600: internal error code, arguments: -5573, this physical attribute may not be specified for a table "
        "partition";
    ERROR_NAME[-OB_ERR_MISS_VALUES] = "OB_ERR_MISS_VALUES";
    MYSQL_ERRNO[-OB_ERR_MISS_VALUES] = -1;
    SQLSTATE[-OB_ERR_MISS_VALUES] = "HY000";
    STR_ERROR[-OB_ERR_MISS_VALUES] = "missing VALUES keyword";
    STR_USER_ERROR[-OB_ERR_MISS_VALUES] = "missing VALUES keyword";
    ORACLE_ERRNO[-OB_ERR_MISS_VALUES] = 926;
    ORACLE_STR_ERROR[-OB_ERR_MISS_VALUES] = "ORA-00926: missing VALUES keyword";
    ORACLE_STR_USER_ERROR[-OB_ERR_MISS_VALUES] = "ORA-00926: missing VALUES keyword";
    ERROR_NAME[-OB_ERR_MISS_AT_VALUES] = "OB_ERR_MISS_AT_VALUES";
    MYSQL_ERRNO[-OB_ERR_MISS_AT_VALUES] = -1;
    SQLSTATE[-OB_ERR_MISS_AT_VALUES] = "HY000";
    STR_ERROR[-OB_ERR_MISS_AT_VALUES] = "missing AT or VALUES keyword";
    STR_USER_ERROR[-OB_ERR_MISS_AT_VALUES] = "missing AT or VALUES keyword";
    ORACLE_ERRNO[-OB_ERR_MISS_AT_VALUES] = 600;
    ORACLE_STR_ERROR[-OB_ERR_MISS_AT_VALUES] =
        "ORA-00600: internal error code, arguments: -5575, missing AT or VALUES keyword";
    ORACLE_STR_USER_ERROR[-OB_ERR_MISS_AT_VALUES] =
        "ORA-00600: internal error code, arguments: -5575, missing AT or VALUES keyword";
    ERROR_NAME[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] = "OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG";
    MYSQL_ERRNO[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] = ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG;
    SQLSTATE[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] = "HY000";
    STR_ERROR[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] =
        "Explicit or implicit commit is not allowed in stored function.";
    STR_USER_ERROR[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] =
        "Explicit or implicit commit is not allowed in stored function.";
    ORACLE_ERRNO[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] = 600;
    ORACLE_STR_ERROR[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] =
        "ORA-00600: internal error code, arguments: -5576, Explicit or implicit commit is not allowed in stored "
        "function.";
    ORACLE_STR_USER_ERROR[-OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG] =
        "ORA-00600: internal error code, arguments: -5576, Explicit or implicit commit is not allowed in stored "
        "function.";
    ERROR_NAME[-OB_PC_GET_LOCATION_ERROR] = "OB_PC_GET_LOCATION_ERROR";
    MYSQL_ERRNO[-OB_PC_GET_LOCATION_ERROR] = -1;
    SQLSTATE[-OB_PC_GET_LOCATION_ERROR] = "HY000";
    STR_ERROR[-OB_PC_GET_LOCATION_ERROR] = "Plan cache get location failed";
    STR_USER_ERROR[-OB_PC_GET_LOCATION_ERROR] = "Plan cache get location failed";
    ORACLE_ERRNO[-OB_PC_GET_LOCATION_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_PC_GET_LOCATION_ERROR] =
        "ORA-00600: internal error code, arguments: -5577, Plan cache get location failed";
    ORACLE_STR_USER_ERROR[-OB_PC_GET_LOCATION_ERROR] =
        "ORA-00600: internal error code, arguments: -5577, Plan cache get location failed";
    ERROR_NAME[-OB_PC_LOCK_CONFLICT] = "OB_PC_LOCK_CONFLICT";
    MYSQL_ERRNO[-OB_PC_LOCK_CONFLICT] = -1;
    SQLSTATE[-OB_PC_LOCK_CONFLICT] = "HY000";
    STR_ERROR[-OB_PC_LOCK_CONFLICT] = "Plan cache lock conflict";
    STR_USER_ERROR[-OB_PC_LOCK_CONFLICT] = "Plan cache lock conflict";
    ORACLE_ERRNO[-OB_PC_LOCK_CONFLICT] = 600;
    ORACLE_STR_ERROR[-OB_PC_LOCK_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5578, Plan cache lock conflict";
    ORACLE_STR_USER_ERROR[-OB_PC_LOCK_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5578, Plan cache lock conflict";
    ERROR_NAME[-OB_ER_SP_NO_RETSET] = "OB_ER_SP_NO_RETSET";
    MYSQL_ERRNO[-OB_ER_SP_NO_RETSET] = ER_SP_NO_RETSET;
    SQLSTATE[-OB_ER_SP_NO_RETSET] = "0A000";
    STR_ERROR[-OB_ER_SP_NO_RETSET] = "Not allowed to return a result set in pl function";
    STR_USER_ERROR[-OB_ER_SP_NO_RETSET] = "Not allowed to return a result set in pl function";
    ORACLE_ERRNO[-OB_ER_SP_NO_RETSET] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_NO_RETSET] =
        "ORA-00600: internal error code, arguments: -5579, Not allowed to return a result set in pl function";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_NO_RETSET] =
        "ORA-00600: internal error code, arguments: -5579, Not allowed to return a result set in pl function";
    ERROR_NAME[-OB_ER_SP_NORETURNEND] = "OB_ER_SP_NORETURNEND";
    MYSQL_ERRNO[-OB_ER_SP_NORETURNEND] = ER_SP_NORETURNEND;
    SQLSTATE[-OB_ER_SP_NORETURNEND] = "2F005";
    STR_ERROR[-OB_ER_SP_NORETURNEND] = "FUNCTION ended without RETURN";
    STR_USER_ERROR[-OB_ER_SP_NORETURNEND] = "FUNCTION %s ended without RETURN";
    ORACLE_ERRNO[-OB_ER_SP_NORETURNEND] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_NORETURNEND] =
        "ORA-00600: internal error code, arguments: -5580, FUNCTION ended without RETURN";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_NORETURNEND] =
        "ORA-00600: internal error code, arguments: -5580, FUNCTION %s ended without RETURN";
    ERROR_NAME[-OB_ERR_SP_DUP_HANDLER] = "OB_ERR_SP_DUP_HANDLER";
    MYSQL_ERRNO[-OB_ERR_SP_DUP_HANDLER] = ER_SP_DUP_HANDLER;
    SQLSTATE[-OB_ERR_SP_DUP_HANDLER] = "42000";
    STR_ERROR[-OB_ERR_SP_DUP_HANDLER] = "Duplicate handler declared in the same block";
    STR_USER_ERROR[-OB_ERR_SP_DUP_HANDLER] = "Duplicate handler declared in the same block";
    ORACLE_ERRNO[-OB_ERR_SP_DUP_HANDLER] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_DUP_HANDLER] =
        "ORA-00600: internal error code, arguments: -5581, Duplicate handler declared in the same block";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_DUP_HANDLER] =
        "ORA-00600: internal error code, arguments: -5581, Duplicate handler declared in the same block";
    ERROR_NAME[-OB_ER_SP_NO_RECURSIVE_CREATE] = "OB_ER_SP_NO_RECURSIVE_CREATE";
    MYSQL_ERRNO[-OB_ER_SP_NO_RECURSIVE_CREATE] = ER_SP_NO_RECURSIVE_CREATE;
    SQLSTATE[-OB_ER_SP_NO_RECURSIVE_CREATE] = "2F003";
    STR_ERROR[-OB_ER_SP_NO_RECURSIVE_CREATE] = "Can\'t create a routine from within another routine";
    STR_USER_ERROR[-OB_ER_SP_NO_RECURSIVE_CREATE] = "Can\'t create a routine from within another routine";
    ORACLE_ERRNO[-OB_ER_SP_NO_RECURSIVE_CREATE] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_NO_RECURSIVE_CREATE] =
        "ORA-00600: internal error code, arguments: -5582, Can\'t create a routine from within another routine";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_NO_RECURSIVE_CREATE] =
        "ORA-00600: internal error code, arguments: -5582, Can\'t create a routine from within another routine";
    ERROR_NAME[-OB_ER_SP_BADRETURN] = "OB_ER_SP_BADRETURN";
    MYSQL_ERRNO[-OB_ER_SP_BADRETURN] = ER_SP_BADRETURN;
    SQLSTATE[-OB_ER_SP_BADRETURN] = "42000";
    STR_ERROR[-OB_ER_SP_BADRETURN] = "RETURN is only allowed in a FUNCTION";
    STR_USER_ERROR[-OB_ER_SP_BADRETURN] = "RETURN is only allowed in a FUNCTION";
    ORACLE_ERRNO[-OB_ER_SP_BADRETURN] = 372;
    ORACLE_STR_ERROR[-OB_ER_SP_BADRETURN] = "PLS-00372: In a procedure, RETURN statement cannot contain an expression";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_BADRETURN] =
        "PLS-00372: In a procedure, RETURN statement cannot contain an expression";
    ERROR_NAME[-OB_ER_SP_BAD_CURSOR_SELECT] = "OB_ER_SP_BAD_CURSOR_SELECT";
    MYSQL_ERRNO[-OB_ER_SP_BAD_CURSOR_SELECT] = ER_SP_BAD_CURSOR_SELECT;
    SQLSTATE[-OB_ER_SP_BAD_CURSOR_SELECT] = "42000";
    STR_ERROR[-OB_ER_SP_BAD_CURSOR_SELECT] = "Cursor SELECT must not have INTO";
    STR_USER_ERROR[-OB_ER_SP_BAD_CURSOR_SELECT] = "Cursor SELECT must not have INTO";
    ORACLE_ERRNO[-OB_ER_SP_BAD_CURSOR_SELECT] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_BAD_CURSOR_SELECT] =
        "ORA-00600: internal error code, arguments: -5584, Cursor SELECT must not have INTO";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_BAD_CURSOR_SELECT] =
        "ORA-00600: internal error code, arguments: -5584, Cursor SELECT must not have INTO";
    ERROR_NAME[-OB_ER_SP_BAD_SQLSTATE] = "OB_ER_SP_BAD_SQLSTATE";
    MYSQL_ERRNO[-OB_ER_SP_BAD_SQLSTATE] = ER_SP_BAD_SQLSTATE;
    SQLSTATE[-OB_ER_SP_BAD_SQLSTATE] = "42000";
    STR_ERROR[-OB_ER_SP_BAD_SQLSTATE] = "Bad SQLSTATE";
    STR_USER_ERROR[-OB_ER_SP_BAD_SQLSTATE] = "Bad SQLSTATE: \'%.*s\'";
    ORACLE_ERRNO[-OB_ER_SP_BAD_SQLSTATE] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_BAD_SQLSTATE] = "ORA-00600: internal error code, arguments: -5585, Bad SQLSTATE";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_BAD_SQLSTATE] =
        "ORA-00600: internal error code, arguments: -5585, Bad SQLSTATE: \'%.*s\'";
    ERROR_NAME[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] = "OB_ER_SP_VARCOND_AFTER_CURSHNDLR";
    MYSQL_ERRNO[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] = ER_SP_VARCOND_AFTER_CURSHNDLR;
    SQLSTATE[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] = "42000";
    STR_ERROR[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] =
        "Variable or condition declaration after cursor or handler declaration";
    STR_USER_ERROR[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] =
        "Variable or condition declaration after cursor or handler declaration";
    ORACLE_ERRNO[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] =
        "ORA-00600: internal error code, arguments: -5586, Variable or condition declaration after cursor or handler "
        "declaration";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_VARCOND_AFTER_CURSHNDLR] =
        "ORA-00600: internal error code, arguments: -5586, Variable or condition declaration after cursor or handler "
        "declaration";
    ERROR_NAME[-OB_ER_SP_CURSOR_AFTER_HANDLER] = "OB_ER_SP_CURSOR_AFTER_HANDLER";
    MYSQL_ERRNO[-OB_ER_SP_CURSOR_AFTER_HANDLER] = ER_SP_CURSOR_AFTER_HANDLER;
    SQLSTATE[-OB_ER_SP_CURSOR_AFTER_HANDLER] = "42000";
    STR_ERROR[-OB_ER_SP_CURSOR_AFTER_HANDLER] = "Cursor declaration after handler declaration";
    STR_USER_ERROR[-OB_ER_SP_CURSOR_AFTER_HANDLER] = "Cursor declaration after handler declaration";
    ORACLE_ERRNO[-OB_ER_SP_CURSOR_AFTER_HANDLER] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_CURSOR_AFTER_HANDLER] =
        "ORA-00600: internal error code, arguments: -5587, Cursor declaration after handler declaration";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_CURSOR_AFTER_HANDLER] =
        "ORA-00600: internal error code, arguments: -5587, Cursor declaration after handler declaration";
    ERROR_NAME[-OB_ER_SP_WRONG_NAME] = "OB_ER_SP_WRONG_NAME";
    MYSQL_ERRNO[-OB_ER_SP_WRONG_NAME] = ER_SP_WRONG_NAME;
    SQLSTATE[-OB_ER_SP_WRONG_NAME] = "42000";
    STR_ERROR[-OB_ER_SP_WRONG_NAME] = "Incorrect routine name";
    STR_USER_ERROR[-OB_ER_SP_WRONG_NAME] = "Incorrect routine name \'%.*s\'";
    ORACLE_ERRNO[-OB_ER_SP_WRONG_NAME] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_WRONG_NAME] = "ORA-00600: internal error code, arguments: -5588, Incorrect routine name";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_WRONG_NAME] =
        "ORA-00600: internal error code, arguments: -5588, Incorrect routine name \'%.*s\'";
    ERROR_NAME[-OB_ER_SP_CURSOR_ALREADY_OPEN] = "OB_ER_SP_CURSOR_ALREADY_OPEN";
    MYSQL_ERRNO[-OB_ER_SP_CURSOR_ALREADY_OPEN] = ER_SP_CURSOR_ALREADY_OPEN;
    SQLSTATE[-OB_ER_SP_CURSOR_ALREADY_OPEN] = "24000";
    STR_ERROR[-OB_ER_SP_CURSOR_ALREADY_OPEN] = "Cursor is already open";
    STR_USER_ERROR[-OB_ER_SP_CURSOR_ALREADY_OPEN] = "Cursor is already open";
    ORACLE_ERRNO[-OB_ER_SP_CURSOR_ALREADY_OPEN] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_CURSOR_ALREADY_OPEN] =
        "ORA-00600: internal error code, arguments: -5589, Cursor is already open";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_CURSOR_ALREADY_OPEN] =
        "ORA-00600: internal error code, arguments: -5589, Cursor is already open";
    ERROR_NAME[-OB_ER_SP_CURSOR_NOT_OPEN] = "OB_ER_SP_CURSOR_NOT_OPEN";
    MYSQL_ERRNO[-OB_ER_SP_CURSOR_NOT_OPEN] = ER_SP_CURSOR_NOT_OPEN;
    SQLSTATE[-OB_ER_SP_CURSOR_NOT_OPEN] = "24000";
    STR_ERROR[-OB_ER_SP_CURSOR_NOT_OPEN] = "Cursor is not open";
    STR_USER_ERROR[-OB_ER_SP_CURSOR_NOT_OPEN] = "Cursor is not open";
    ORACLE_ERRNO[-OB_ER_SP_CURSOR_NOT_OPEN] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_CURSOR_NOT_OPEN] =
        "ORA-00600: internal error code, arguments: -5590, Cursor is not open";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_CURSOR_NOT_OPEN] =
        "ORA-00600: internal error code, arguments: -5590, Cursor is not open";
    ERROR_NAME[-OB_ER_SP_CANT_SET_AUTOCOMMIT] = "OB_ER_SP_CANT_SET_AUTOCOMMIT";
    MYSQL_ERRNO[-OB_ER_SP_CANT_SET_AUTOCOMMIT] = ER_SP_CANT_SET_AUTOCOMMIT;
    SQLSTATE[-OB_ER_SP_CANT_SET_AUTOCOMMIT] = "HY000";
    STR_ERROR[-OB_ER_SP_CANT_SET_AUTOCOMMIT] = "Not allowed to set autocommit from a stored function";
    STR_USER_ERROR[-OB_ER_SP_CANT_SET_AUTOCOMMIT] = "Not allowed to set autocommit from a stored function";
    ORACLE_ERRNO[-OB_ER_SP_CANT_SET_AUTOCOMMIT] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_CANT_SET_AUTOCOMMIT] =
        "ORA-00600: internal error code, arguments: -5591, Not allowed to set autocommit from a stored function";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_CANT_SET_AUTOCOMMIT] =
        "ORA-00600: internal error code, arguments: -5591, Not allowed to set autocommit from a stored function";
    ERROR_NAME[-OB_ER_SP_NOT_VAR_ARG] = "OB_ER_SP_NOT_VAR_ARG";
    MYSQL_ERRNO[-OB_ER_SP_NOT_VAR_ARG] = ER_SP_NOT_VAR_ARG;
    SQLSTATE[-OB_ER_SP_NOT_VAR_ARG] = "42000";
    STR_ERROR[-OB_ER_SP_NOT_VAR_ARG] = "OUT or INOUT argument for routine is not a variable";
    STR_USER_ERROR[-OB_ER_SP_NOT_VAR_ARG] = "OUT or INOUT argument %d for routine %.*s is not a variable";
    ORACLE_ERRNO[-OB_ER_SP_NOT_VAR_ARG] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_NOT_VAR_ARG] =
        "ORA-00600: internal error code, arguments: -5592, OUT or INOUT argument for routine is not a variable";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_NOT_VAR_ARG] =
        "ORA-00600: internal error code, arguments: -5592, OUT or INOUT argument %d for routine %.*s is not a variable";
    ERROR_NAME[-OB_ER_SP_LILABEL_MISMATCH] = "OB_ER_SP_LILABEL_MISMATCH";
    MYSQL_ERRNO[-OB_ER_SP_LILABEL_MISMATCH] = ER_SP_LILABEL_MISMATCH;
    SQLSTATE[-OB_ER_SP_LILABEL_MISMATCH] = "42000";
    STR_ERROR[-OB_ER_SP_LILABEL_MISMATCH] = "with no matching label";
    STR_USER_ERROR[-OB_ER_SP_LILABEL_MISMATCH] = "%s with no matching label: %s";
    ORACLE_ERRNO[-OB_ER_SP_LILABEL_MISMATCH] = 600;
    ORACLE_STR_ERROR[-OB_ER_SP_LILABEL_MISMATCH] =
        "ORA-00600: internal error code, arguments: -5593, with no matching label";
    ORACLE_STR_USER_ERROR[-OB_ER_SP_LILABEL_MISMATCH] =
        "ORA-00600: internal error code, arguments: -5593, %s with no matching label: %s";
    ERROR_NAME[-OB_ERR_TRUNCATE_ILLEGAL_FK] = "OB_ERR_TRUNCATE_ILLEGAL_FK";
    MYSQL_ERRNO[-OB_ERR_TRUNCATE_ILLEGAL_FK] = ER_TRUNCATE_ILLEGAL_FK;
    SQLSTATE[-OB_ERR_TRUNCATE_ILLEGAL_FK] = "42000";
    STR_ERROR[-OB_ERR_TRUNCATE_ILLEGAL_FK] = "Cannot truncate a table referenced in a foreign key constraint";
    STR_USER_ERROR[-OB_ERR_TRUNCATE_ILLEGAL_FK] = "Cannot truncate a table referenced in a foreign key constraint %.*s";
    ORACLE_ERRNO[-OB_ERR_TRUNCATE_ILLEGAL_FK] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TRUNCATE_ILLEGAL_FK] = "ORA-00600: internal error code, arguments: -5594, Cannot truncate "
                                                    "a table referenced in a foreign key constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_TRUNCATE_ILLEGAL_FK] = "ORA-00600: internal error code, arguments: -5594, Cannot "
                                                         "truncate a table referenced in a foreign key constraint %.*s";
    ERROR_NAME[-OB_ERR_DUP_KEY] = "OB_ERR_DUP_KEY";
    MYSQL_ERRNO[-OB_ERR_DUP_KEY] = ER_DUP_KEY;
    SQLSTATE[-OB_ERR_DUP_KEY] = "23000";
    STR_ERROR[-OB_ERR_DUP_KEY] = "Can't write; duplicate key in table";
    STR_USER_ERROR[-OB_ERR_DUP_KEY] = "Can't write; duplicate key in table \'%.*s\'";
    ORACLE_ERRNO[-OB_ERR_DUP_KEY] = 1;
    ORACLE_STR_ERROR[-OB_ERR_DUP_KEY] = "ORA-00001: unique constraint violated";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_KEY] = "ORA-00001: unique constraint (%.*s) violated";
    ERROR_NAME[-OB_ER_INVALID_USE_OF_NULL] = "OB_ER_INVALID_USE_OF_NULL";
    MYSQL_ERRNO[-OB_ER_INVALID_USE_OF_NULL] = ER_INVALID_USE_OF_NULL;
    SQLSTATE[-OB_ER_INVALID_USE_OF_NULL] = "22004";
    STR_ERROR[-OB_ER_INVALID_USE_OF_NULL] = "Invalid use of NULL value";
    STR_USER_ERROR[-OB_ER_INVALID_USE_OF_NULL] = "Invalid use of NULL value";
    ORACLE_ERRNO[-OB_ER_INVALID_USE_OF_NULL] = 600;
    ORACLE_STR_ERROR[-OB_ER_INVALID_USE_OF_NULL] =
        "ORA-00600: internal error code, arguments: -5596, Invalid use of NULL value";
    ORACLE_STR_USER_ERROR[-OB_ER_INVALID_USE_OF_NULL] =
        "ORA-00600: internal error code, arguments: -5596, Invalid use of NULL value";
    ERROR_NAME[-OB_ERR_SPLIT_LIST_LESS_VALUE] = "OB_ERR_SPLIT_LIST_LESS_VALUE";
    MYSQL_ERRNO[-OB_ERR_SPLIT_LIST_LESS_VALUE] = -1;
    SQLSTATE[-OB_ERR_SPLIT_LIST_LESS_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_SPLIT_LIST_LESS_VALUE] = "last resulting partition cannot contain bounds";
    STR_USER_ERROR[-OB_ERR_SPLIT_LIST_LESS_VALUE] = "last resulting partition cannot contain bounds";
    ORACLE_ERRNO[-OB_ERR_SPLIT_LIST_LESS_VALUE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SPLIT_LIST_LESS_VALUE] =
        "ORA-00600: internal error code, arguments: -5597, last resulting partition cannot contain bounds";
    ORACLE_STR_USER_ERROR[-OB_ERR_SPLIT_LIST_LESS_VALUE] =
        "ORA-00600: internal error code, arguments: -5597, last resulting partition cannot contain bounds";
    ERROR_NAME[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] = "OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST";
    MYSQL_ERRNO[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] = -1;
    SQLSTATE[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] = "HY000";
    STR_ERROR[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] = "cannot add partition when DEFAULT partition exists";
    STR_USER_ERROR[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] = "cannot add partition when DEFAULT partition exists";
    ORACLE_ERRNO[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] =
        "ORA-00600: internal error code, arguments: -5598, cannot add partition when DEFAULT partition exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_ADD_PARTITION_TO_DEFAULT_LIST] =
        "ORA-00600: internal error code, arguments: -5598, cannot add partition when DEFAULT partition exists";
    ERROR_NAME[-OB_ERR_SPLIT_INTO_ONE_PARTITION] = "OB_ERR_SPLIT_INTO_ONE_PARTITION";
    MYSQL_ERRNO[-OB_ERR_SPLIT_INTO_ONE_PARTITION] = -1;
    SQLSTATE[-OB_ERR_SPLIT_INTO_ONE_PARTITION] = "HY000";
    STR_ERROR[-OB_ERR_SPLIT_INTO_ONE_PARTITION] = "cannot split partition into one partition, use rename instead";
    STR_USER_ERROR[-OB_ERR_SPLIT_INTO_ONE_PARTITION] = "cannot split partition into one partition, use rename instead";
    ORACLE_ERRNO[-OB_ERR_SPLIT_INTO_ONE_PARTITION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SPLIT_INTO_ONE_PARTITION] = "ORA-00600: internal error code, arguments: -5599, cannot "
                                                         "split partition into one partition, use rename instead";
    ORACLE_STR_USER_ERROR[-OB_ERR_SPLIT_INTO_ONE_PARTITION] =
        "ORA-00600: internal error code, arguments: -5599, cannot split partition into one partition, use rename "
        "instead";
    ERROR_NAME[-OB_ERR_NO_TENANT_PRIVILEGE] = "OB_ERR_NO_TENANT_PRIVILEGE";
    MYSQL_ERRNO[-OB_ERR_NO_TENANT_PRIVILEGE] = -1;
    SQLSTATE[-OB_ERR_NO_TENANT_PRIVILEGE] = "HY000";
    STR_ERROR[-OB_ERR_NO_TENANT_PRIVILEGE] = "can not create user in sys tenant";
    STR_USER_ERROR[-OB_ERR_NO_TENANT_PRIVILEGE] = "can not create user %s in sys tenant, name %.*s";
    ORACLE_ERRNO[-OB_ERR_NO_TENANT_PRIVILEGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NO_TENANT_PRIVILEGE] =
        "ORA-00600: internal error code, arguments: -5600, can not create user in sys tenant";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_TENANT_PRIVILEGE] =
        "ORA-00600: internal error code, arguments: -5600, can not create user %s in sys tenant, name %.*s";
    ERROR_NAME[-OB_ERR_INVALID_PERCENTAGE] = "OB_ERR_INVALID_PERCENTAGE";
    MYSQL_ERRNO[-OB_ERR_INVALID_PERCENTAGE] = -1;
    SQLSTATE[-OB_ERR_INVALID_PERCENTAGE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_PERCENTAGE] = "Percentage should between 1 and 99";
    STR_USER_ERROR[-OB_ERR_INVALID_PERCENTAGE] = "Percentage should between 1 and 99";
    ORACLE_ERRNO[-OB_ERR_INVALID_PERCENTAGE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_PERCENTAGE] =
        "ORA-00600: internal error code, arguments: -5601, Percentage should between 1 and 99";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_PERCENTAGE] =
        "ORA-00600: internal error code, arguments: -5601, Percentage should between 1 and 99";
    ERROR_NAME[-OB_ERR_COLLECT_HISTOGRAM] = "OB_ERR_COLLECT_HISTOGRAM";
    MYSQL_ERRNO[-OB_ERR_COLLECT_HISTOGRAM] = -1;
    SQLSTATE[-OB_ERR_COLLECT_HISTOGRAM] = "HY000";
    STR_ERROR[-OB_ERR_COLLECT_HISTOGRAM] = "Should collect histogram after major freeze";
    STR_USER_ERROR[-OB_ERR_COLLECT_HISTOGRAM] = "Should collect histogram after major freeze";
    ORACLE_ERRNO[-OB_ERR_COLLECT_HISTOGRAM] = 600;
    ORACLE_STR_ERROR[-OB_ERR_COLLECT_HISTOGRAM] =
        "ORA-00600: internal error code, arguments: -5602, Should collect histogram after major freeze";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLLECT_HISTOGRAM] =
        "ORA-00600: internal error code, arguments: -5602, Should collect histogram after major freeze";
    ERROR_NAME[-OB_ER_TEMP_TABLE_IN_USE] = "OB_ER_TEMP_TABLE_IN_USE";
    MYSQL_ERRNO[-OB_ER_TEMP_TABLE_IN_USE] = -1;
    SQLSTATE[-OB_ER_TEMP_TABLE_IN_USE] = "0A000";
    STR_ERROR[-OB_ER_TEMP_TABLE_IN_USE] = "Attempt to create, alter or drop an index on temporary table already in use";
    STR_USER_ERROR[-OB_ER_TEMP_TABLE_IN_USE] =
        "Attempt to create, alter or drop an index on temporary table already in use";
    ORACLE_ERRNO[-OB_ER_TEMP_TABLE_IN_USE] = 600;
    ORACLE_STR_ERROR[-OB_ER_TEMP_TABLE_IN_USE] = "ORA-00600: internal error code, arguments: -5603, Attempt to create, "
                                                 "alter or drop an index on temporary table already in use";
    ORACLE_STR_USER_ERROR[-OB_ER_TEMP_TABLE_IN_USE] =
        "ORA-00600: internal error code, arguments: -5603, Attempt to create, alter or drop an index on temporary "
        "table already in use";
    ERROR_NAME[-OB_ERR_INVALID_NLS_PARAMETER_STRING] = "OB_ERR_INVALID_NLS_PARAMETER_STRING";
    MYSQL_ERRNO[-OB_ERR_INVALID_NLS_PARAMETER_STRING] = -1;
    SQLSTATE[-OB_ERR_INVALID_NLS_PARAMETER_STRING] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_NLS_PARAMETER_STRING] = "invalid NLS parameter string used in SQL function";
    STR_USER_ERROR[-OB_ERR_INVALID_NLS_PARAMETER_STRING] = "invalid NLS parameter string used in SQL function";
    ORACLE_ERRNO[-OB_ERR_INVALID_NLS_PARAMETER_STRING] = 12702;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_NLS_PARAMETER_STRING] =
        "ORA-12702: invalid NLS parameter string used in SQL function";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_NLS_PARAMETER_STRING] =
        "ORA-12702: invalid NLS parameter string used in SQL function";
    ERROR_NAME[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] = "OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] = "datetime/interval precision is out of range";
    STR_USER_ERROR[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] = "datetime/interval precision is out of range";
    ORACLE_ERRNO[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] = 30088;
    ORACLE_STR_ERROR[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] =
        "ORA-30088: datetime/interval precision is out of range";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE] =
        "ORA-30088: datetime/interval precision is out of range";
    ERROR_NAME[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = "OB_ERR_CMD_NOT_PROPERLY_ENDED";
    MYSQL_ERRNO[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = -1;
    SQLSTATE[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = "HY000";
    STR_ERROR[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = "SQL command not properly ended";
    STR_USER_ERROR[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = "SQL command not properly ended";
    ORACLE_ERRNO[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = 933;
    ORACLE_STR_ERROR[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = "ORA-00933: SQL command not properly ended";
    ORACLE_STR_USER_ERROR[-OB_ERR_CMD_NOT_PROPERLY_ENDED] = "ORA-00933: SQL command not properly ended";
    ERROR_NAME[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = "OB_ERR_INVALID_NUMBER_FORMAT_MODEL";
    MYSQL_ERRNO[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = -1;
    SQLSTATE[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = "42000";
    STR_ERROR[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = "invalid number format model";
    STR_USER_ERROR[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = "invalid number format model";
    ORACLE_ERRNO[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = 1481;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = "ORA-01481: invalid number format model";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_NUMBER_FORMAT_MODEL] = "ORA-01481: invalid number format model";
    ERROR_NAME[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] = "OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED";
    MYSQL_ERRNO[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] = ER_WRONG_FIELD_TERMINATORS;
    SQLSTATE[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] = "HY000";
    STR_ERROR[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] = "Non-ASCII separator arguments are not fully supported";
    STR_USER_ERROR[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] =
        "Non-ASCII separator arguments are not fully supported";
    ORACLE_ERRNO[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] = 600;
    ORACLE_STR_ERROR[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] =
        "ORA-00600: internal error code, arguments: -5609, Non-ASCII separator arguments are not fully supported";
    ORACLE_STR_USER_ERROR[-OB_WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED] =
        "ORA-00600: internal error code, arguments: -5609, Non-ASCII separator arguments are not fully supported";
    ERROR_NAME[-OB_WARN_AMBIGUOUS_FIELD_TERM] = "OB_WARN_AMBIGUOUS_FIELD_TERM";
    MYSQL_ERRNO[-OB_WARN_AMBIGUOUS_FIELD_TERM] = ER_WRONG_FIELD_TERMINATORS;
    SQLSTATE[-OB_WARN_AMBIGUOUS_FIELD_TERM] = "HY000";
    STR_ERROR[-OB_WARN_AMBIGUOUS_FIELD_TERM] = "First character of the FIELDS TERMINATED string is ambiguous; please "
                                               "use non-optional and non-empty FIELDS ENCLOSED BY";
    STR_USER_ERROR[-OB_WARN_AMBIGUOUS_FIELD_TERM] = "First character of the FIELDS TERMINATED string is ambiguous; "
                                                    "please use non-optional and non-empty FIELDS ENCLOSED BY";
    ORACLE_ERRNO[-OB_WARN_AMBIGUOUS_FIELD_TERM] = 600;
    ORACLE_STR_ERROR[-OB_WARN_AMBIGUOUS_FIELD_TERM] =
        "ORA-00600: internal error code, arguments: -5610, First character of the FIELDS TERMINATED string is "
        "ambiguous; please use non-optional and non-empty FIELDS ENCLOSED BY";
    ORACLE_STR_USER_ERROR[-OB_WARN_AMBIGUOUS_FIELD_TERM] =
        "ORA-00600: internal error code, arguments: -5610, First character of the FIELDS TERMINATED string is "
        "ambiguous; please use non-optional and non-empty FIELDS ENCLOSED BY";
    ERROR_NAME[-OB_WARN_TOO_FEW_RECORDS] = "OB_WARN_TOO_FEW_RECORDS";
    MYSQL_ERRNO[-OB_WARN_TOO_FEW_RECORDS] = ER_WARN_TOO_FEW_RECORDS;
    SQLSTATE[-OB_WARN_TOO_FEW_RECORDS] = "01000";
    STR_ERROR[-OB_WARN_TOO_FEW_RECORDS] = "Row doesn't contain data for all columns";
    STR_USER_ERROR[-OB_WARN_TOO_FEW_RECORDS] = "Row %ld doesn't contain data for all columns";
    ORACLE_ERRNO[-OB_WARN_TOO_FEW_RECORDS] = 600;
    ORACLE_STR_ERROR[-OB_WARN_TOO_FEW_RECORDS] =
        "ORA-00600: internal error code, arguments: -5611, Row doesn't contain data for all columns";
    ORACLE_STR_USER_ERROR[-OB_WARN_TOO_FEW_RECORDS] =
        "ORA-00600: internal error code, arguments: -5611, Row %ld doesn't contain data for all columns";
    ERROR_NAME[-OB_WARN_TOO_MANY_RECORDS] = "OB_WARN_TOO_MANY_RECORDS";
    MYSQL_ERRNO[-OB_WARN_TOO_MANY_RECORDS] = ER_WARN_TOO_MANY_RECORDS;
    SQLSTATE[-OB_WARN_TOO_MANY_RECORDS] = "01000";
    STR_ERROR[-OB_WARN_TOO_MANY_RECORDS] = "Row was truncated; it contained more data than there were input columns";
    STR_USER_ERROR[-OB_WARN_TOO_MANY_RECORDS] =
        "Row %ld was truncated; it contained more data than there were input columns";
    ORACLE_ERRNO[-OB_WARN_TOO_MANY_RECORDS] = 600;
    ORACLE_STR_ERROR[-OB_WARN_TOO_MANY_RECORDS] = "ORA-00600: internal error code, arguments: -5612, Row was "
                                                  "truncated; it contained more data than there were input columns";
    ORACLE_STR_USER_ERROR[-OB_WARN_TOO_MANY_RECORDS] =
        "ORA-00600: internal error code, arguments: -5612, Row %ld was truncated; it contained more data than there "
        "were input columns";
    ERROR_NAME[-OB_ERR_TOO_MANY_VALUES] = "OB_ERR_TOO_MANY_VALUES";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_VALUES] = -1;
    SQLSTATE[-OB_ERR_TOO_MANY_VALUES] = "HY000";
    STR_ERROR[-OB_ERR_TOO_MANY_VALUES] = "too many values";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_VALUES] = "too many values";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_VALUES] = 913;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_VALUES] = "ORA-00913: too many values";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_VALUES] = "ORA-00913: too many values";
    ERROR_NAME[-OB_ERR_NOT_ENOUGH_VALUES] = "OB_ERR_NOT_ENOUGH_VALUES";
    MYSQL_ERRNO[-OB_ERR_NOT_ENOUGH_VALUES] = -1;
    SQLSTATE[-OB_ERR_NOT_ENOUGH_VALUES] = "HY000";
    STR_ERROR[-OB_ERR_NOT_ENOUGH_VALUES] = "not enough values";
    STR_USER_ERROR[-OB_ERR_NOT_ENOUGH_VALUES] = "not enough values";
    ORACLE_ERRNO[-OB_ERR_NOT_ENOUGH_VALUES] = 947;
    ORACLE_STR_ERROR[-OB_ERR_NOT_ENOUGH_VALUES] = "ORA-00947: not enough values";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_ENOUGH_VALUES] = "ORA-00947: not enough values";
    ERROR_NAME[-OB_ERR_MORE_THAN_ONE_ROW] = "OB_ERR_MORE_THAN_ONE_ROW";
    MYSQL_ERRNO[-OB_ERR_MORE_THAN_ONE_ROW] = -1;
    SQLSTATE[-OB_ERR_MORE_THAN_ONE_ROW] = "HY000";
    STR_ERROR[-OB_ERR_MORE_THAN_ONE_ROW] = "single-row subquery returns more than one row";
    STR_USER_ERROR[-OB_ERR_MORE_THAN_ONE_ROW] = "single-row subquery returns more than one row";
    ORACLE_ERRNO[-OB_ERR_MORE_THAN_ONE_ROW] = 1427;
    ORACLE_STR_ERROR[-OB_ERR_MORE_THAN_ONE_ROW] = "ORA-01427: single-row subquery returns more than one row";
    ORACLE_STR_USER_ERROR[-OB_ERR_MORE_THAN_ONE_ROW] = "ORA-01427: single-row subquery returns more than one row";
    ERROR_NAME[-OB_ERR_NOT_SUBQUERY] = "OB_ERR_NOT_SUBQUERY";
    MYSQL_ERRNO[-OB_ERR_NOT_SUBQUERY] = -1;
    SQLSTATE[-OB_ERR_NOT_SUBQUERY] = "HY000";
    STR_ERROR[-OB_ERR_NOT_SUBQUERY] = "UPDATE ... SET expression must be a subquery";
    STR_USER_ERROR[-OB_ERR_NOT_SUBQUERY] = "UPDATE ... SET expression must be a subquery";
    ORACLE_ERRNO[-OB_ERR_NOT_SUBQUERY] = 1767;
    ORACLE_STR_ERROR[-OB_ERR_NOT_SUBQUERY] = "ORA-01767: UPDATE ... SET expression must be a subquery";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_SUBQUERY] = "ORA-01767: UPDATE ... SET expression must be a subquery";
    ERROR_NAME[-OB_INAPPROPRIATE_INTO] = "OB_INAPPROPRIATE_INTO";
    MYSQL_ERRNO[-OB_INAPPROPRIATE_INTO] = -1;
    SQLSTATE[-OB_INAPPROPRIATE_INTO] = "HY000";
    STR_ERROR[-OB_INAPPROPRIATE_INTO] = "inappropriate INTO";
    STR_USER_ERROR[-OB_INAPPROPRIATE_INTO] = "inappropriate INTO";
    ORACLE_ERRNO[-OB_INAPPROPRIATE_INTO] = 1744;
    ORACLE_STR_ERROR[-OB_INAPPROPRIATE_INTO] = "ORA-01744: inappropriate INTO";
    ORACLE_STR_USER_ERROR[-OB_INAPPROPRIATE_INTO] = "ORA-01744: inappropriate INTO";
    ERROR_NAME[-OB_ERR_TABLE_IS_REFERENCED] = "OB_ERR_TABLE_IS_REFERENCED";
    MYSQL_ERRNO[-OB_ERR_TABLE_IS_REFERENCED] = ER_ROW_IS_REFERENCED;
    SQLSTATE[-OB_ERR_TABLE_IS_REFERENCED] = "23000";
    STR_ERROR[-OB_ERR_TABLE_IS_REFERENCED] = "Cannot delete or update a parent row: a foreign key constraint fails";
    STR_USER_ERROR[-OB_ERR_TABLE_IS_REFERENCED] =
        "Cannot delete or update a parent row: a foreign key constraint fails";
    ORACLE_ERRNO[-OB_ERR_TABLE_IS_REFERENCED] = 2292;
    ORACLE_STR_ERROR[-OB_ERR_TABLE_IS_REFERENCED] = "ORA-02292: integrity constraint violated - child record found";
    ORACLE_STR_USER_ERROR[-OB_ERR_TABLE_IS_REFERENCED] =
        "ORA-02292: integrity constraint violated - child record found";
    ERROR_NAME[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] = "OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN";
    MYSQL_ERRNO[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] = -1;
    SQLSTATE[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] = "Column part of using clause can not have qualifier";
    STR_USER_ERROR[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] = "Column part of using clause can not have qualifier";
    ORACLE_ERRNO[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] = 25154;
    ORACLE_STR_ERROR[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] =
        "ORA-25154: Column part of using clause can not have qualifier";
    ORACLE_STR_USER_ERROR[-OB_ERR_QUALIFIER_EXISTS_FOR_USING_COLUMN] =
        "ORA-25154: Column part of using clause can not have qualifier";
    ERROR_NAME[-OB_ERR_OUTER_JOIN_NESTED] = "OB_ERR_OUTER_JOIN_NESTED";
    MYSQL_ERRNO[-OB_ERR_OUTER_JOIN_NESTED] = -1;
    SQLSTATE[-OB_ERR_OUTER_JOIN_NESTED] = "HY000";
    STR_ERROR[-OB_ERR_OUTER_JOIN_NESTED] = "two tables cannot be outer-joined to each other";
    STR_USER_ERROR[-OB_ERR_OUTER_JOIN_NESTED] = "two tables cannot be outer-joined to each other";
    ORACLE_ERRNO[-OB_ERR_OUTER_JOIN_NESTED] = 1416;
    ORACLE_STR_ERROR[-OB_ERR_OUTER_JOIN_NESTED] = "ORA-01416: two tables cannot be outer-joined to each other";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTER_JOIN_NESTED] = "ORA-01416: two tables cannot be outer-joined to each other";
    ERROR_NAME[-OB_ERR_MULTI_OUTER_JOIN_TABLE] = "OB_ERR_MULTI_OUTER_JOIN_TABLE";
    MYSQL_ERRNO[-OB_ERR_MULTI_OUTER_JOIN_TABLE] = -1;
    SQLSTATE[-OB_ERR_MULTI_OUTER_JOIN_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_MULTI_OUTER_JOIN_TABLE] = "a predicate may reference only one outer-joined table";
    STR_USER_ERROR[-OB_ERR_MULTI_OUTER_JOIN_TABLE] = "a predicate may reference only one outer-joined table";
    ORACLE_ERRNO[-OB_ERR_MULTI_OUTER_JOIN_TABLE] = 1468;
    ORACLE_STR_ERROR[-OB_ERR_MULTI_OUTER_JOIN_TABLE] =
        "ORA-01468: a predicate may reference only one outer-joined table";
    ORACLE_STR_USER_ERROR[-OB_ERR_MULTI_OUTER_JOIN_TABLE] =
        "ORA-01468: a predicate may reference only one outer-joined table";
    ERROR_NAME[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] = "OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN";
    MYSQL_ERRNO[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] = -1;
    SQLSTATE[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] = "an outer join cannot be specified on a correlation column";
    STR_USER_ERROR[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] =
        "an outer join cannot be specified on a correlation column";
    ORACLE_ERRNO[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] = 1705;
    ORACLE_STR_ERROR[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] =
        "ORA-01705: an outer join cannot be specified on a correlation column";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTER_JOIN_ON_CORRELATION_COLUMN] =
        "ORA-01705: an outer join cannot be specified on a correlation column";
    ERROR_NAME[-OB_ERR_OUTER_JOIN_AMBIGUOUS] = "OB_ERR_OUTER_JOIN_AMBIGUOUS";
    MYSQL_ERRNO[-OB_ERR_OUTER_JOIN_AMBIGUOUS] = -1;
    SQLSTATE[-OB_ERR_OUTER_JOIN_AMBIGUOUS] = "HY000";
    STR_ERROR[-OB_ERR_OUTER_JOIN_AMBIGUOUS] = "outer join operator (+) not allowed in operand of OR or IN";
    STR_USER_ERROR[-OB_ERR_OUTER_JOIN_AMBIGUOUS] = "outer join operator (+) not allowed in operand of OR or IN";
    ORACLE_ERRNO[-OB_ERR_OUTER_JOIN_AMBIGUOUS] = 1719;
    ORACLE_STR_ERROR[-OB_ERR_OUTER_JOIN_AMBIGUOUS] =
        "ORA-01719: outer join operator (+) not allowed in operand of OR or IN";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTER_JOIN_AMBIGUOUS] =
        "ORA-01719: outer join operator (+) not allowed in operand of OR or IN";
    ERROR_NAME[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] = "OB_ERR_OUTER_JOIN_WITH_SUBQUERY";
    MYSQL_ERRNO[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] = -1;
    SQLSTATE[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] = "HY000";
    STR_ERROR[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] = "a column may not be outer-joined to a subquery";
    STR_USER_ERROR[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] = "a column may not be outer-joined to a subquery";
    ORACLE_ERRNO[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] = 1799;
    ORACLE_STR_ERROR[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] = "ORA-01799: a column may not be outer-joined to a subquery";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTER_JOIN_WITH_SUBQUERY] =
        "ORA-01799: a column may not be outer-joined to a subquery";
    ERROR_NAME[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] = "OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN";
    MYSQL_ERRNO[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] = -1;
    SQLSTATE[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] = "HY000";
    STR_ERROR[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] = "old style outer join (+) cannot be used with ANSI joins";
    STR_USER_ERROR[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] = "old style outer join (+) cannot be used with ANSI joins";
    ORACLE_ERRNO[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] = 25156;
    ORACLE_STR_ERROR[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] =
        "ORA-25156: old style outer join (+) cannot be used with ANSI joins";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN] =
        "ORA-25156: old style outer join (+) cannot be used with ANSI joins";
    ERROR_NAME[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = "OB_ERR_OUTER_JOIN_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = "outer join operator (+) is not allowed here";
    STR_USER_ERROR[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = "outer join operator (+) is not allowed here";
    ORACLE_ERRNO[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = 30563;
    ORACLE_STR_ERROR[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = "ORA-30563: outer join operator (+) is not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_OUTER_JOIN_NOT_ALLOWED] = "ORA-30563: outer join operator (+) is not allowed here";
    ERROR_NAME[-OB_SCHEMA_EAGAIN] = "OB_SCHEMA_EAGAIN";
    MYSQL_ERRNO[-OB_SCHEMA_EAGAIN] = -1;
    SQLSTATE[-OB_SCHEMA_EAGAIN] = "HY000";
    STR_ERROR[-OB_SCHEMA_EAGAIN] = "Schema try again";
    STR_USER_ERROR[-OB_SCHEMA_EAGAIN] = "Schema try again";
    ORACLE_ERRNO[-OB_SCHEMA_EAGAIN] = 600;
    ORACLE_STR_ERROR[-OB_SCHEMA_EAGAIN] = "ORA-00600: internal error code, arguments: -5627, Schema try again";
    ORACLE_STR_USER_ERROR[-OB_SCHEMA_EAGAIN] = "ORA-00600: internal error code, arguments: -5627, Schema try again";
    ERROR_NAME[-OB_ERR_ZERO_LEN_COL] = "OB_ERR_ZERO_LEN_COL";
    MYSQL_ERRNO[-OB_ERR_ZERO_LEN_COL] = -1;
    SQLSTATE[-OB_ERR_ZERO_LEN_COL] = "HY000";
    STR_ERROR[-OB_ERR_ZERO_LEN_COL] = "zero-length columns are not allowed";
    STR_USER_ERROR[-OB_ERR_ZERO_LEN_COL] = "zero-length columns are not allowed";
    ORACLE_ERRNO[-OB_ERR_ZERO_LEN_COL] = 1723;
    ORACLE_STR_ERROR[-OB_ERR_ZERO_LEN_COL] = "ORA-01723: zero-length columns are not allowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_ZERO_LEN_COL] = "ORA-01723: zero-length columns are not allowed";
    ERROR_NAME[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE";
    MYSQL_ERRNO[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = -1;
    SQLSTATE[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "HY000";
    STR_ERROR[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "year conflicts with Julian date";
    STR_USER_ERROR[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "year conflicts with Julian date";
    ORACLE_ERRNO[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = 1831;
    ORACLE_STR_ERROR[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "ORA-01831: year conflicts with Julian date";
    ORACLE_STR_USER_ERROR[-OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "ORA-01831: year conflicts with Julian date";
    ERROR_NAME[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE";
    MYSQL_ERRNO[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] = -1;
    SQLSTATE[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "HY000";
    STR_ERROR[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "day of year conflicts with Julian date";
    STR_USER_ERROR[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] = "day of year conflicts with Julian date";
    ORACLE_ERRNO[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] = 1832;
    ORACLE_STR_ERROR[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] =
        "ORA-01832: day of year conflicts with Julian date";
    ORACLE_STR_USER_ERROR[-OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE] =
        "ORA-01832: day of year conflicts with Julian date";
    ERROR_NAME[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE";
    MYSQL_ERRNO[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = -1;
    SQLSTATE[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "HY000";
    STR_ERROR[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "month conflicts with Julian date";
    STR_USER_ERROR[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "month conflicts with Julian date";
    ORACLE_ERRNO[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = 1833;
    ORACLE_STR_ERROR[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "ORA-01833: month conflicts with Julian date";
    ORACLE_STR_USER_ERROR[-OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "ORA-01833: month conflicts with Julian date";
    ERROR_NAME[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE";
    MYSQL_ERRNO[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] = -1;
    SQLSTATE[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "HY000";
    STR_ERROR[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "day of month conflicts with Julian date";
    STR_USER_ERROR[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] = "day of month conflicts with Julian date";
    ORACLE_ERRNO[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] = 1834;
    ORACLE_STR_ERROR[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] =
        "ORA-01834: day of month conflicts with Julian date";
    ORACLE_STR_USER_ERROR[-OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE] =
        "ORA-01834: day of month conflicts with Julian date";
    ERROR_NAME[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] = "OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE";
    MYSQL_ERRNO[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] = -1;
    SQLSTATE[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] = "HY000";
    STR_ERROR[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] = "day of week conflicts with Julian date";
    STR_USER_ERROR[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] = "day of week conflicts with Julian date";
    ORACLE_ERRNO[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] = 1835;
    ORACLE_STR_ERROR[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] =
        "ORA-01835: day of week conflicts with Julian date";
    ORACLE_STR_USER_ERROR[-OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE] =
        "ORA-01835: day of week conflicts with Julian date";
    ERROR_NAME[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY";
    MYSQL_ERRNO[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = -1;
    SQLSTATE[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "HY000";
    STR_ERROR[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "hour conflicts with seconds in day";
    STR_USER_ERROR[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "hour conflicts with seconds in day";
    ORACLE_ERRNO[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = 1836;
    ORACLE_STR_ERROR[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "ORA-01836: hour conflicts with seconds in day";
    ORACLE_STR_USER_ERROR[-OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "ORA-01836: hour conflicts with seconds in day";
    ERROR_NAME[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY";
    MYSQL_ERRNO[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = -1;
    SQLSTATE[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "HY000";
    STR_ERROR[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = "minutes of hour conflicts with seconds in day";
    STR_USER_ERROR[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "minutes of hour conflicts with seconds in day";
    ORACLE_ERRNO[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] = 1837;
    ORACLE_STR_ERROR[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "ORA-01837: minutes of hour conflicts with seconds in day";
    ORACLE_STR_USER_ERROR[-OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "ORA-01837: minutes of hour conflicts with seconds in day";
    ERROR_NAME[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY";
    MYSQL_ERRNO[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] = -1;
    SQLSTATE[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] = "HY000";
    STR_ERROR[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "seconds of minute conflicts with seconds in day";
    STR_USER_ERROR[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "seconds of minute conflicts with seconds in day";
    ORACLE_ERRNO[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] = 1838;
    ORACLE_STR_ERROR[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "ORA-01838: seconds of minute conflicts with seconds in day";
    ORACLE_STR_USER_ERROR[-OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY] =
        "ORA-01838: seconds of minute conflicts with seconds in day";
    ERROR_NAME[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = "OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED";
    MYSQL_ERRNO[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = -1;
    SQLSTATE[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = "HY000";
    STR_ERROR[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = "date not valid for month specified";
    STR_USER_ERROR[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = "date not valid for month specified";
    ORACLE_ERRNO[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = 1839;
    ORACLE_STR_ERROR[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = "ORA-01839: date not valid for month specified";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED] = "ORA-01839: date not valid for month specified";
    ERROR_NAME[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] = "OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH";
    MYSQL_ERRNO[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] = -1;
    SQLSTATE[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] = "HY000";
    STR_ERROR[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] = "input value not long enough for date format";
    STR_USER_ERROR[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] = "input value not long enough for date format";
    ORACLE_ERRNO[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] = 1840;
    ORACLE_STR_ERROR[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] = "ORA-01840: input value not long enough for date format";
    ORACLE_STR_USER_ERROR[-OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH] =
        "ORA-01840: input value not long enough for date format";
    ERROR_NAME[-OB_ERR_INVALID_QUARTER_VALUE] = "OB_ERR_INVALID_QUARTER_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_QUARTER_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_QUARTER_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_QUARTER_VALUE] = "quarter must be between 1 and 4";
    STR_USER_ERROR[-OB_ERR_INVALID_QUARTER_VALUE] = "quarter must be between 1 and 4";
    ORACLE_ERRNO[-OB_ERR_INVALID_QUARTER_VALUE] = 1842;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_QUARTER_VALUE] = "ORA-01842: quarter must be between 1 and 4";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_QUARTER_VALUE] = "ORA-01842: quarter must be between 1 and 4";
    ERROR_NAME[-OB_ERR_INVALID_MONTH] = "OB_ERR_INVALID_MONTH";
    MYSQL_ERRNO[-OB_ERR_INVALID_MONTH] = -1;
    SQLSTATE[-OB_ERR_INVALID_MONTH] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_MONTH] = "not a valid month";
    STR_USER_ERROR[-OB_ERR_INVALID_MONTH] = "not a valid month";
    ORACLE_ERRNO[-OB_ERR_INVALID_MONTH] = 1843;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_MONTH] = "ORA-01843: not a valid month";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_MONTH] = "ORA-01843: not a valid month";
    ERROR_NAME[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = "OB_ERR_INVALID_DAY_OF_THE_WEEK";
    MYSQL_ERRNO[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = -1;
    SQLSTATE[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = "not a valid day of the week";
    STR_USER_ERROR[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = "not a valid day of the week";
    ORACLE_ERRNO[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = 1846;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = "ORA-01846: not a valid day of the week";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_DAY_OF_THE_WEEK] = "ORA-01846: not a valid day of the week";
    ERROR_NAME[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] = "OB_ERR_INVALID_DAY_OF_YEAR_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] = "day of year must be between 1 and 365 (366 for leap year)";
    STR_USER_ERROR[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] = "day of year must be between 1 and 365 (366 for leap year)";
    ORACLE_ERRNO[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] = 1848;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] =
        "ORA-01848: day of year must be between 1 and 365 (366 for leap year)";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_DAY_OF_YEAR_VALUE] =
        "ORA-01848: day of year must be between 1 and 365 (366 for leap year)";
    ERROR_NAME[-OB_ERR_INVALID_HOUR12_VALUE] = "OB_ERR_INVALID_HOUR12_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_HOUR12_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_HOUR12_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_HOUR12_VALUE] = "hour must be between 1 and 12";
    STR_USER_ERROR[-OB_ERR_INVALID_HOUR12_VALUE] = "hour must be between 1 and 12";
    ORACLE_ERRNO[-OB_ERR_INVALID_HOUR12_VALUE] = 1849;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_HOUR12_VALUE] = "ORA-01849: hour must be between 1 and 12";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_HOUR12_VALUE] = "ORA-01849: hour must be between 1 and 12";
    ERROR_NAME[-OB_ERR_INVALID_HOUR24_VALUE] = "OB_ERR_INVALID_HOUR24_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_HOUR24_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_HOUR24_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_HOUR24_VALUE] = "hour must be between 0 and 23";
    STR_USER_ERROR[-OB_ERR_INVALID_HOUR24_VALUE] = "hour must be between 0 and 23";
    ORACLE_ERRNO[-OB_ERR_INVALID_HOUR24_VALUE] = 1850;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_HOUR24_VALUE] = "ORA-01850: hour must be between 0 and 23";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_HOUR24_VALUE] = "ORA-01850: hour must be between 0 and 23";
    ERROR_NAME[-OB_ERR_INVALID_MINUTES_VALUE] = "OB_ERR_INVALID_MINUTES_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_MINUTES_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_MINUTES_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_MINUTES_VALUE] = "minutes must be between 0 and 59";
    STR_USER_ERROR[-OB_ERR_INVALID_MINUTES_VALUE] = "minutes must be between 0 and 59";
    ORACLE_ERRNO[-OB_ERR_INVALID_MINUTES_VALUE] = 1851;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_MINUTES_VALUE] = "ORA-01851: minutes must be between 0 and 59";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_MINUTES_VALUE] = "ORA-01851: minutes must be between 0 and 59";
    ERROR_NAME[-OB_ERR_INVALID_SECONDS_VALUE] = "OB_ERR_INVALID_SECONDS_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_SECONDS_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_SECONDS_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SECONDS_VALUE] = "seconds must be between 0 and 59";
    STR_USER_ERROR[-OB_ERR_INVALID_SECONDS_VALUE] = "seconds must be between 0 and 59";
    ORACLE_ERRNO[-OB_ERR_INVALID_SECONDS_VALUE] = 1852;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SECONDS_VALUE] = "ORA-01852: seconds must be between 0 and 59";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SECONDS_VALUE] = "ORA-01852: seconds must be between 0 and 59";
    ERROR_NAME[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] = "OB_ERR_INVALID_SECONDS_IN_DAY_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] = "seconds in day must be between 0 and 86399";
    STR_USER_ERROR[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] = "seconds in day must be between 0 and 86399";
    ORACLE_ERRNO[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] = 1853;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] = "ORA-01853: seconds in day must be between 0 and 86399";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SECONDS_IN_DAY_VALUE] =
        "ORA-01853: seconds in day must be between 0 and 86399";
    ERROR_NAME[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = "OB_ERR_INVALID_JULIAN_DATE_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = "julian date must be between 1 and 5373484";
    STR_USER_ERROR[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = "julian date must be between 1 and 5373484";
    ORACLE_ERRNO[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = 1854;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = "ORA-01854: julian date must be between 1 and 5373484";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_JULIAN_DATE_VALUE] = "ORA-01854: julian date must be between 1 and 5373484";
    ERROR_NAME[-OB_ERR_AM_OR_PM_REQUIRED] = "OB_ERR_AM_OR_PM_REQUIRED";
    MYSQL_ERRNO[-OB_ERR_AM_OR_PM_REQUIRED] = -1;
    SQLSTATE[-OB_ERR_AM_OR_PM_REQUIRED] = "HY000";
    STR_ERROR[-OB_ERR_AM_OR_PM_REQUIRED] = "AM/A.M. or PM/P.M. required";
    STR_USER_ERROR[-OB_ERR_AM_OR_PM_REQUIRED] = "AM/A.M. or PM/P.M. required";
    ORACLE_ERRNO[-OB_ERR_AM_OR_PM_REQUIRED] = 1855;
    ORACLE_STR_ERROR[-OB_ERR_AM_OR_PM_REQUIRED] = "ORA-01855: AM/A.M. or PM/P.M. required";
    ORACLE_STR_USER_ERROR[-OB_ERR_AM_OR_PM_REQUIRED] = "ORA-01855: AM/A.M. or PM/P.M. required";
    ERROR_NAME[-OB_ERR_BC_OR_AD_REQUIRED] = "OB_ERR_BC_OR_AD_REQUIRED";
    MYSQL_ERRNO[-OB_ERR_BC_OR_AD_REQUIRED] = -1;
    SQLSTATE[-OB_ERR_BC_OR_AD_REQUIRED] = "HY000";
    STR_ERROR[-OB_ERR_BC_OR_AD_REQUIRED] = "BC/B.C. or AD/A.D. required";
    STR_USER_ERROR[-OB_ERR_BC_OR_AD_REQUIRED] = "BC/B.C. or AD/A.D. required";
    ORACLE_ERRNO[-OB_ERR_BC_OR_AD_REQUIRED] = 1856;
    ORACLE_STR_ERROR[-OB_ERR_BC_OR_AD_REQUIRED] = "ORA-01856: BC/B.C. or AD/A.D. required";
    ORACLE_STR_USER_ERROR[-OB_ERR_BC_OR_AD_REQUIRED] = "ORA-01856: BC/B.C. or AD/A.D. required";
    ERROR_NAME[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = "OB_ERR_FORMAT_CODE_APPEARS_TWICE";
    MYSQL_ERRNO[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = -1;
    SQLSTATE[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = "HY000";
    STR_ERROR[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = "format code appears twice";
    STR_USER_ERROR[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = "format code appears twice";
    ORACLE_ERRNO[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = 1810;
    ORACLE_STR_ERROR[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = "ORA-01810: format code appears twice";
    ORACLE_STR_USER_ERROR[-OB_ERR_FORMAT_CODE_APPEARS_TWICE] = "ORA-01810: format code appears twice";
    ERROR_NAME[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] = "OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE";
    MYSQL_ERRNO[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] = -1;
    SQLSTATE[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] = "HY000";
    STR_ERROR[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] = "day of week may only be specified once";
    STR_USER_ERROR[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] = "day of week may only be specified once";
    ORACLE_ERRNO[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] = 1817;
    ORACLE_STR_ERROR[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] =
        "ORA-01817: day of week may only be specified once";
    ORACLE_STR_USER_ERROR[-OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE] =
        "ORA-01817: day of week may only be specified once";
    ERROR_NAME[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = "OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD";
    MYSQL_ERRNO[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = -1;
    SQLSTATE[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = "HY000";
    STR_ERROR[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = "signed year precludes use of BC/AD";
    STR_USER_ERROR[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = "signed year precludes use of BC/AD";
    ORACLE_ERRNO[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = 1819;
    ORACLE_STR_ERROR[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = "ORA-01819: signed year precludes use of BC/AD";
    ORACLE_STR_USER_ERROR[-OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD] = "ORA-01819: signed year precludes use of BC/AD";
    ERROR_NAME[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] = "OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR";
    MYSQL_ERRNO[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] = -1;
    SQLSTATE[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] = "HY000";
    STR_ERROR[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] = "Julian date precludes use of day of year";
    STR_USER_ERROR[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] = "Julian date precludes use of day of year";
    ORACLE_ERRNO[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] = 1811;
    ORACLE_STR_ERROR[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] =
        "ORA-01811: Julian date precludes use of day of year";
    ORACLE_STR_USER_ERROR[-OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR] =
        "ORA-01811: Julian date precludes use of day of year";
    ERROR_NAME[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = "OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE";
    MYSQL_ERRNO[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = -1;
    SQLSTATE[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = "HY000";
    STR_ERROR[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = "year may only be specified once";
    STR_USER_ERROR[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = "year may only be specified once";
    ORACLE_ERRNO[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = 1812;
    ORACLE_STR_ERROR[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = "ORA-01812: year may only be specified once";
    ORACLE_STR_USER_ERROR[-OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE] = "ORA-01812: year may only be specified once";
    ERROR_NAME[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = "OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE";
    MYSQL_ERRNO[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = -1;
    SQLSTATE[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = "HY000";
    STR_ERROR[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = "hour may only be specified once";
    STR_USER_ERROR[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = "hour may only be specified once";
    ORACLE_ERRNO[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = 1813;
    ORACLE_STR_ERROR[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = "ORA-01813: hour may only be specified once";
    ORACLE_STR_USER_ERROR[-OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE] = "ORA-01813: hour may only be specified once";
    ERROR_NAME[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] = "OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT";
    MYSQL_ERRNO[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] = -1;
    SQLSTATE[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] = "HY000";
    STR_ERROR[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] = "AM/PM conflicts with use of A.M./P.M.";
    STR_USER_ERROR[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] = "AM/PM conflicts with use of A.M./P.M.";
    ORACLE_ERRNO[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] = 1814;
    ORACLE_STR_ERROR[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] =
        "ORA-01814: AM/PM conflicts with use of A.M./P.M.";
    ORACLE_STR_USER_ERROR[-OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT] =
        "ORA-01814: AM/PM conflicts with use of A.M./P.M.";
    ERROR_NAME[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] = "OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT";
    MYSQL_ERRNO[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] = -1;
    SQLSTATE[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] = "HY000";
    STR_ERROR[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] = "BC/AD conflicts with use of B.C./A.D.";
    STR_USER_ERROR[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] = "BC/AD conflicts with use of B.C./A.D.";
    ORACLE_ERRNO[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] = 1815;
    ORACLE_STR_ERROR[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] =
        "ORA-01815: BC/AD conflicts with use of B.C./A.D.";
    ORACLE_STR_USER_ERROR[-OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT] =
        "ORA-01815: BC/AD conflicts with use of B.C./A.D.";
    ERROR_NAME[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = "OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE";
    MYSQL_ERRNO[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = -1;
    SQLSTATE[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = "HY000";
    STR_ERROR[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = "month may only be specified once";
    STR_USER_ERROR[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = "month may only be specified once";
    ORACLE_ERRNO[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = 1816;
    ORACLE_STR_ERROR[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = "ORA-01816: month may only be specified once";
    ORACLE_STR_USER_ERROR[-OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE] = "ORA-01816: month may only be specified once";
    ERROR_NAME[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] = "OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE";
    MYSQL_ERRNO[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] = -1;
    SQLSTATE[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] = "HY000";
    STR_ERROR[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] = "day of week may only be specified once";
    STR_USER_ERROR[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] = "day of week may only be specified once";
    ORACLE_ERRNO[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] = 1817;
    ORACLE_STR_ERROR[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] =
        "ORA-01817: day of week may only be specified once";
    ORACLE_STR_USER_ERROR[-OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE] =
        "ORA-01817: day of week may only be specified once";
    ERROR_NAME[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] = "OB_ERR_FORMAT_CODE_CANNOT_APPEAR";
    MYSQL_ERRNO[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] = -1;
    SQLSTATE[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] = "HY000";
    STR_ERROR[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] = "format code cannot appear in date input format";
    STR_USER_ERROR[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] = "format code cannot appear in date input format";
    ORACLE_ERRNO[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] = 1820;
    ORACLE_STR_ERROR[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] = "ORA-01820: format code cannot appear in date input format";
    ORACLE_STR_USER_ERROR[-OB_ERR_FORMAT_CODE_CANNOT_APPEAR] =
        "ORA-01820: format code cannot appear in date input format";
    ERROR_NAME[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] = "OB_ERR_NON_NUMERIC_CHARACTER_VALUE";
    MYSQL_ERRNO[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] = -1;
    SQLSTATE[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] = "a non-numeric character was found where a numeric was expected";
    STR_USER_ERROR[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] =
        "a non-numeric character was found where a numeric was expected";
    ORACLE_ERRNO[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] = 1858;
    ORACLE_STR_ERROR[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] =
        "ORA-01858: a non-numeric character was found where a numeric was expected";
    ORACLE_STR_USER_ERROR[-OB_ERR_NON_NUMERIC_CHARACTER_VALUE] =
        "ORA-01858: a non-numeric character was found where a numeric was expected";
    ERROR_NAME[-OB_INVALID_MERIDIAN_INDICATOR_USE] = "OB_INVALID_MERIDIAN_INDICATOR_USE";
    MYSQL_ERRNO[-OB_INVALID_MERIDIAN_INDICATOR_USE] = -1;
    SQLSTATE[-OB_INVALID_MERIDIAN_INDICATOR_USE] = "HY000";
    STR_ERROR[-OB_INVALID_MERIDIAN_INDICATOR_USE] = "'HH24' precludes use of meridian indicator";
    STR_USER_ERROR[-OB_INVALID_MERIDIAN_INDICATOR_USE] = "'HH24' precludes use of meridian indicator";
    ORACLE_ERRNO[-OB_INVALID_MERIDIAN_INDICATOR_USE] = 1818;
    ORACLE_STR_ERROR[-OB_INVALID_MERIDIAN_INDICATOR_USE] = "ORA-01818: 'HH24' precludes use of meridian indicator";
    ORACLE_STR_USER_ERROR[-OB_INVALID_MERIDIAN_INDICATOR_USE] = "ORA-01818: 'HH24' precludes use of meridian indicator";
    ERROR_NAME[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] = "OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR";
    MYSQL_ERRNO[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] = -1;
    SQLSTATE[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] =
        "missing or illegal character following the escape character";
    STR_USER_ERROR[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] =
        "missing or illegal character following the escape character";
    ORACLE_ERRNO[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] = 1424;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] =
        "ORA-01424: missing or illegal character following the escape character";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR] =
        "ORA-01424: missing or illegal character following the escape character";
    ERROR_NAME[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] = "OB_ERR_INVALID_ESCAPE_CHAR_LENGTH";
    MYSQL_ERRNO[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] = -1;
    SQLSTATE[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] = "escape character must be character string of length 1";
    STR_USER_ERROR[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] = "escape character must be character string of length 1";
    ORACLE_ERRNO[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] = 1425;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] =
        "ORA-01425: escape character must be character string of length 1";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_ESCAPE_CHAR_LENGTH] =
        "ORA-01425: escape character must be character string of length 1";
    ERROR_NAME[-OB_ERR_DAY_OF_MONTH_RANGE] = "OB_ERR_DAY_OF_MONTH_RANGE";
    MYSQL_ERRNO[-OB_ERR_DAY_OF_MONTH_RANGE] = -1;
    SQLSTATE[-OB_ERR_DAY_OF_MONTH_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_DAY_OF_MONTH_RANGE] = "day of month must be between 1 and last day of month";
    STR_USER_ERROR[-OB_ERR_DAY_OF_MONTH_RANGE] = "day of month must be between 1 and last day of month";
    ORACLE_ERRNO[-OB_ERR_DAY_OF_MONTH_RANGE] = 1847;
    ORACLE_STR_ERROR[-OB_ERR_DAY_OF_MONTH_RANGE] = "ORA-01847: day of month must be between 1 and last day of month";
    ORACLE_STR_USER_ERROR[-OB_ERR_DAY_OF_MONTH_RANGE] =
        "ORA-01847: day of month must be between 1 and last day of month";
    ERROR_NAME[-OB_ERR_NOT_SELECTED_EXPR] = "OB_ERR_NOT_SELECTED_EXPR";
    MYSQL_ERRNO[-OB_ERR_NOT_SELECTED_EXPR] = -1;
    SQLSTATE[-OB_ERR_NOT_SELECTED_EXPR] = "HY000";
    STR_ERROR[-OB_ERR_NOT_SELECTED_EXPR] = "not a SELECTed expression";
    STR_USER_ERROR[-OB_ERR_NOT_SELECTED_EXPR] = "not a SELECTed expression";
    ORACLE_ERRNO[-OB_ERR_NOT_SELECTED_EXPR] = 1791;
    ORACLE_STR_ERROR[-OB_ERR_NOT_SELECTED_EXPR] = "ORA-01791: not a SELECTed expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_SELECTED_EXPR] = "ORA-01791: not a SELECTed expression";
    ERROR_NAME[-OB_ERR_INVALID_YEAR_VALUE] = "OB_ERR_INVALID_YEAR_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_YEAR_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_YEAR_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_YEAR_VALUE] = "(full) year must be between -4713 and +9999, and not be 0";
    STR_USER_ERROR[-OB_ERR_INVALID_YEAR_VALUE] = "(full) year must be between -4713 and +9999, and not be 0";
    ORACLE_ERRNO[-OB_ERR_INVALID_YEAR_VALUE] = 1841;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_YEAR_VALUE] =
        "ORA-01841: (full) year must be between -4713 and +9999, and not be 0";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_YEAR_VALUE] =
        "ORA-01841: (full) year must be between -4713 and +9999, and not be 0";
    ERROR_NAME[-OB_ERR_UK_PK_DUPLICATE] = "OB_ERR_UK_PK_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_UK_PK_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_UK_PK_DUPLICATE] = "HY000";
    STR_ERROR[-OB_ERR_UK_PK_DUPLICATE] = "such unique or primary key already exists in the table";
    STR_USER_ERROR[-OB_ERR_UK_PK_DUPLICATE] = "such unique or primary key already exists in the table";
    ORACLE_ERRNO[-OB_ERR_UK_PK_DUPLICATE] = 2261;
    ORACLE_STR_ERROR[-OB_ERR_UK_PK_DUPLICATE] = "ORA-02261: such unique or primary key already exists in the table";
    ORACLE_STR_USER_ERROR[-OB_ERR_UK_PK_DUPLICATE] =
        "ORA-02261: such unique or primary key already exists in the table";
    ERROR_NAME[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = "OB_ERR_COLUMN_LIST_ALREADY_INDEXED";
    MYSQL_ERRNO[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = -1;
    SQLSTATE[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = "HY000";
    STR_ERROR[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = "such column list already indexed";
    STR_USER_ERROR[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = "such column list already indexed";
    ORACLE_ERRNO[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = 1408;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = "ORA-01408: such column list already indexed";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_LIST_ALREADY_INDEXED] = "ORA-01408: such column list already indexed";
    ERROR_NAME[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] = "OB_ERR_BUSHY_TREE_NOT_SUPPORTED";
    MYSQL_ERRNO[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] = -1;
    SQLSTATE[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] = "HY000";
    STR_ERROR[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] = "PX does not support processing a bushy tree";
    STR_USER_ERROR[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] = "PX does not support processing a bushy tree";
    ORACLE_ERRNO[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] =
        "ORA-00600: internal error code, arguments: -5673, PX does not support processing a bushy tree";
    ORACLE_STR_USER_ERROR[-OB_ERR_BUSHY_TREE_NOT_SUPPORTED] =
        "ORA-00600: internal error code, arguments: -5673, PX does not support processing a bushy tree";
    ERROR_NAME[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = "OB_ERR_ARGUMENT_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = "argument is out of range";
    STR_USER_ERROR[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = "argument '%ld' is out of range";
    ORACLE_ERRNO[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = 1428;
    ORACLE_STR_ERROR[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = "ORA-01428: argument is out of range";
    ORACLE_STR_USER_ERROR[-OB_ERR_ARGUMENT_OUT_OF_RANGE] = "ORA-01428: argument '%ld' is out of range";
    ERROR_NAME[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] = "OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST";
    MYSQL_ERRNO[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] = -1;
    SQLSTATE[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] = "HY000";
    STR_ERROR[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] =
        "ORDER BY item must be the number of a SELECT-list expression";
    STR_USER_ERROR[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] =
        "ORDER BY item must be the number of a SELECT-list expression";
    ORACLE_ERRNO[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] = 1785;
    ORACLE_STR_ERROR[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] =
        "ORA-01785: ORDER BY item must be the number of a SELECT-list expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST] =
        "ORA-01785: ORDER BY item must be the number of a SELECT-list expression";
    ERROR_NAME[-OB_ERR_INTERVAL_INVALID] = "OB_ERR_INTERVAL_INVALID";
    MYSQL_ERRNO[-OB_ERR_INTERVAL_INVALID] = -1;
    SQLSTATE[-OB_ERR_INTERVAL_INVALID] = "HY000";
    STR_ERROR[-OB_ERR_INTERVAL_INVALID] = "the interval is invalid";
    STR_USER_ERROR[-OB_ERR_INTERVAL_INVALID] = "the interval is invalid";
    ORACLE_ERRNO[-OB_ERR_INTERVAL_INVALID] = 1867;
    ORACLE_STR_ERROR[-OB_ERR_INTERVAL_INVALID] = "ORA-01867: the interval is invalid";
    ORACLE_STR_USER_ERROR[-OB_ERR_INTERVAL_INVALID] = "ORA-01867: the interval is invalid";
    ERROR_NAME[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = "OB_ERR_NUMERIC_OR_VALUE_ERROR";
    MYSQL_ERRNO[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = -1;
    SQLSTATE[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = "PL/SQL: numeric or value error";
    STR_USER_ERROR[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = "PL/SQL: numeric or value error: %.*s";
    ORACLE_ERRNO[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = 6502;
    ORACLE_STR_ERROR[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = "ORA-06502: PL/SQL: numeric or value error";
    ORACLE_STR_USER_ERROR[-OB_ERR_NUMERIC_OR_VALUE_ERROR] = "ORA-06502: PL/SQL: numeric or value error: %.*s";
    ERROR_NAME[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = "OB_ERR_CONSTRAINT_NAME_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = "HY000";
    STR_ERROR[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = "name already used by an existing constraint";
    STR_USER_ERROR[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = "name already used by an existing constraint";
    ORACLE_ERRNO[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = 2264;
    ORACLE_STR_ERROR[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = "ORA-02264: name already used by an existing constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONSTRAINT_NAME_DUPLICATE] = "ORA-02264: name already used by an existing constraint";
    ERROR_NAME[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] = "OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE";
    MYSQL_ERRNO[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] = -1;
    SQLSTATE[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] = "table must have at least one column that is not invisible";
    STR_USER_ERROR[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] =
        "table must have at least one column that is not invisible";
    ORACLE_ERRNO[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] = 54039;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] =
        "ORA-54039: table must have at least one column that is not invisible";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE] =
        "ORA-54039: table must have at least one column that is not invisible";
    ERROR_NAME[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] = "OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE";
    MYSQL_ERRNO[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] = -1;
    SQLSTATE[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] =
        "Invisible column is not supported on this type of table.";
    STR_USER_ERROR[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] =
        "Invisible column is not supported on this type of table.";
    ORACLE_ERRNO[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] = 54042;
    ORACLE_STR_ERROR[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] =
        "ORA-54042: Invisible column is not supported on this type of table.";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE] =
        "ORA-54042: Invisible column is not supported on this type of table.";
    ERROR_NAME[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] =
        "OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION";
    MYSQL_ERRNO[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] = -1;
    SQLSTATE[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] =
        "Column visibility modifications cannot be combined with any other modified column DDL option.";
    STR_USER_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] =
        "Column visibility modifications cannot be combined with any other modified column DDL option.";
    ORACLE_ERRNO[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] = 54046;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] =
        "ORA-54046: Column visibility modifications cannot be combined with any other modified column DDL option.";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION] =
        "ORA-54046: Column visibility modifications cannot be combined with any other modified column DDL option.";
    ERROR_NAME[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] = "OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER";
    MYSQL_ERRNO[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] = -1;
    SQLSTATE[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] =
        "The visibility of a column from a table owned by a SYS user cannot be changed.";
    STR_USER_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] =
        "The visibility of a column from a table owned by a SYS user cannot be changed.";
    ORACLE_ERRNO[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] = 54053;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] =
        "ORA-54053: The visibility of a column from a table owned by a SYS user cannot be changed.";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER] =
        "ORA-54053: The visibility of a column from a table owned by a SYS user cannot be changed.";
    ERROR_NAME[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = "OB_ERR_TOO_MANY_ARGS_FOR_FUN";
    MYSQL_ERRNO[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = -1;
    SQLSTATE[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = "HY000";
    STR_ERROR[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = "too many arguments for function";
    STR_USER_ERROR[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = "too many arguments for function";
    ORACLE_ERRNO[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = 939;
    ORACLE_STR_ERROR[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = "ORA-00939: too many arguments for function";
    ORACLE_STR_USER_ERROR[-OB_ERR_TOO_MANY_ARGS_FOR_FUN] = "ORA-00939: too many arguments for function";
    ERROR_NAME[-OB_PX_SQL_NEED_RETRY] = "OB_PX_SQL_NEED_RETRY";
    MYSQL_ERRNO[-OB_PX_SQL_NEED_RETRY] = -1;
    SQLSTATE[-OB_PX_SQL_NEED_RETRY] = "HY000";
    STR_ERROR[-OB_PX_SQL_NEED_RETRY] = "PX sql need retry";
    STR_USER_ERROR[-OB_PX_SQL_NEED_RETRY] = "PX sql need retry";
    ORACLE_ERRNO[-OB_PX_SQL_NEED_RETRY] = 600;
    ORACLE_STR_ERROR[-OB_PX_SQL_NEED_RETRY] = "ORA-00600: internal error code, arguments: -5684, PX sql need retry";
    ORACLE_STR_USER_ERROR[-OB_PX_SQL_NEED_RETRY] =
        "ORA-00600: internal error code, arguments: -5684, PX sql need retry";
    ERROR_NAME[-OB_TENANT_HAS_BEEN_DROPPED] = "OB_TENANT_HAS_BEEN_DROPPED";
    MYSQL_ERRNO[-OB_TENANT_HAS_BEEN_DROPPED] = -1;
    SQLSTATE[-OB_TENANT_HAS_BEEN_DROPPED] = "HY000";
    STR_ERROR[-OB_TENANT_HAS_BEEN_DROPPED] = "tenant has been dropped";
    STR_USER_ERROR[-OB_TENANT_HAS_BEEN_DROPPED] = "Tenant '%.*s' has been dropped";
    ORACLE_ERRNO[-OB_TENANT_HAS_BEEN_DROPPED] = 600;
    ORACLE_STR_ERROR[-OB_TENANT_HAS_BEEN_DROPPED] =
        "ORA-00600: internal error code, arguments: -5685, tenant has been dropped";
    ORACLE_STR_USER_ERROR[-OB_TENANT_HAS_BEEN_DROPPED] =
        "ORA-00600: internal error code, arguments: -5685, Tenant '%.*s' has been dropped";
    ERROR_NAME[-OB_ERR_EXTRACT_FIELD_INVALID] = "OB_ERR_EXTRACT_FIELD_INVALID";
    MYSQL_ERRNO[-OB_ERR_EXTRACT_FIELD_INVALID] = -1;
    SQLSTATE[-OB_ERR_EXTRACT_FIELD_INVALID] = "HY000";
    STR_ERROR[-OB_ERR_EXTRACT_FIELD_INVALID] = "invalid extract field for extract source";
    STR_USER_ERROR[-OB_ERR_EXTRACT_FIELD_INVALID] = "invalid extract field for extract source";
    ORACLE_ERRNO[-OB_ERR_EXTRACT_FIELD_INVALID] = 30076;
    ORACLE_STR_ERROR[-OB_ERR_EXTRACT_FIELD_INVALID] = "ORA-30076: invalid extract field for extract source";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXTRACT_FIELD_INVALID] = "ORA-30076: invalid extract field for extract source";
    ERROR_NAME[-OB_ERR_PACKAGE_COMPILE_ERROR] = "OB_ERR_PACKAGE_COMPILE_ERROR";
    MYSQL_ERRNO[-OB_ERR_PACKAGE_COMPILE_ERROR] = -1;
    SQLSTATE[-OB_ERR_PACKAGE_COMPILE_ERROR] = "42000";
    STR_ERROR[-OB_ERR_PACKAGE_COMPILE_ERROR] = "package compile error";
    STR_USER_ERROR[-OB_ERR_PACKAGE_COMPILE_ERROR] = "%s \'%.*s.%.*s\' compile error";
    ORACLE_ERRNO[-OB_ERR_PACKAGE_COMPILE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PACKAGE_COMPILE_ERROR] =
        "ORA-00600: internal error code, arguments: -5687, package compile error";
    ORACLE_STR_USER_ERROR[-OB_ERR_PACKAGE_COMPILE_ERROR] =
        "ORA-00600: internal error code, arguments: -5687, %s \'%.*s.%.*s\' compile error";
    ERROR_NAME[-OB_ERR_SP_EMPTY_BLOCK] = "OB_ERR_SP_EMPTY_BLOCK";
    MYSQL_ERRNO[-OB_ERR_SP_EMPTY_BLOCK] = -1;
    SQLSTATE[-OB_ERR_SP_EMPTY_BLOCK] = "42000";
    STR_ERROR[-OB_ERR_SP_EMPTY_BLOCK] = "Empty block prohibited in Oracle";
    STR_USER_ERROR[-OB_ERR_SP_EMPTY_BLOCK] = "Empty block prohibited in Oracle";
    ORACLE_ERRNO[-OB_ERR_SP_EMPTY_BLOCK] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_EMPTY_BLOCK] =
        "ORA-00600: internal error code, arguments: -5688, Empty block prohibited in Oracle";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_EMPTY_BLOCK] =
        "ORA-00600: internal error code, arguments: -5688, Empty block prohibited in Oracle";
    ERROR_NAME[-OB_ARRAY_BINDING_ROLLBACK] = "OB_ARRAY_BINDING_ROLLBACK";
    MYSQL_ERRNO[-OB_ARRAY_BINDING_ROLLBACK] = -1;
    SQLSTATE[-OB_ARRAY_BINDING_ROLLBACK] = "HY000";
    STR_ERROR[-OB_ARRAY_BINDING_ROLLBACK] = "array binding need rollback";
    STR_USER_ERROR[-OB_ARRAY_BINDING_ROLLBACK] = "array binding need rollback";
    ORACLE_ERRNO[-OB_ARRAY_BINDING_ROLLBACK] = 600;
    ORACLE_STR_ERROR[-OB_ARRAY_BINDING_ROLLBACK] =
        "ORA-00600: internal error code, arguments: -5689, array binding need rollback";
    ORACLE_STR_USER_ERROR[-OB_ARRAY_BINDING_ROLLBACK] =
        "ORA-00600: internal error code, arguments: -5689, array binding need rollback";
    ERROR_NAME[-OB_ERR_INVALID_SUBQUERY_USE] = "OB_ERR_INVALID_SUBQUERY_USE";
    MYSQL_ERRNO[-OB_ERR_INVALID_SUBQUERY_USE] = -1;
    SQLSTATE[-OB_ERR_INVALID_SUBQUERY_USE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SUBQUERY_USE] = "subquery not allowed here";
    STR_USER_ERROR[-OB_ERR_INVALID_SUBQUERY_USE] = "subquery not allowed here";
    ORACLE_ERRNO[-OB_ERR_INVALID_SUBQUERY_USE] = 2251;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SUBQUERY_USE] = "ORA-02251: subquery not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SUBQUERY_USE] = "ORA-02251: subquery not allowed here";
    ERROR_NAME[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] = "OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST";
    MYSQL_ERRNO[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] = -1;
    SQLSTATE[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] = "HY000";
    STR_ERROR[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] =
        "date or system variable wrongly specified in CHECK constraint";
    STR_USER_ERROR[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] =
        "date or system variable wrongly specified in CHECK constraint";
    ORACLE_ERRNO[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] = 2436;
    ORACLE_STR_ERROR[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] =
        "ORA-02436: date or system variable wrongly specified in CHECK constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST] =
        "ORA-02436: date or system variable wrongly specified in CHECK constraint";
    ERROR_NAME[-OB_ERR_NONEXISTENT_CONSTRAINT] = "OB_ERR_NONEXISTENT_CONSTRAINT";
    MYSQL_ERRNO[-OB_ERR_NONEXISTENT_CONSTRAINT] = -1;
    SQLSTATE[-OB_ERR_NONEXISTENT_CONSTRAINT] = "HY000";
    STR_ERROR[-OB_ERR_NONEXISTENT_CONSTRAINT] = "Cannot drop constraint  - nonexistent constraint";
    STR_USER_ERROR[-OB_ERR_NONEXISTENT_CONSTRAINT] = "Cannot drop constraint  - nonexistent constraint";
    ORACLE_ERRNO[-OB_ERR_NONEXISTENT_CONSTRAINT] = 2443;
    ORACLE_STR_ERROR[-OB_ERR_NONEXISTENT_CONSTRAINT] = "ORA-02443: Cannot drop constraint  - nonexistent constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_NONEXISTENT_CONSTRAINT] =
        "ORA-02443: Cannot drop constraint  - nonexistent constraint";
    ERROR_NAME[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = "OB_ERR_CHECK_CONSTRAINT_VIOLATED";
    MYSQL_ERRNO[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = -1;
    SQLSTATE[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = "HY000";
    STR_ERROR[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = "check constraint violated";
    STR_USER_ERROR[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = "check constraint violated";
    ORACLE_ERRNO[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = 2290;
    ORACLE_STR_ERROR[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = "ORA-02290: check constraint violated";
    ORACLE_STR_USER_ERROR[-OB_ERR_CHECK_CONSTRAINT_VIOLATED] = "ORA-02290: check constraint violated";
    ERROR_NAME[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = "OB_ERR_GROUP_FUNC_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = "group function is not allowed here";
    STR_USER_ERROR[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = "group function is not allowed here";
    ORACLE_ERRNO[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = 934;
    ORACLE_STR_ERROR[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = "ORA-00934: group function is not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_GROUP_FUNC_NOT_ALLOWED] = "ORA-00934: group function is not allowed here";
    ERROR_NAME[-OB_ERR_POLICY_STRING_NOT_FOUND] = "OB_ERR_POLICY_STRING_NOT_FOUND";
    MYSQL_ERRNO[-OB_ERR_POLICY_STRING_NOT_FOUND] = -1;
    SQLSTATE[-OB_ERR_POLICY_STRING_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_ERR_POLICY_STRING_NOT_FOUND] = "policy string not found";
    STR_USER_ERROR[-OB_ERR_POLICY_STRING_NOT_FOUND] = "policy string not found";
    ORACLE_ERRNO[-OB_ERR_POLICY_STRING_NOT_FOUND] = 12416;
    ORACLE_STR_ERROR[-OB_ERR_POLICY_STRING_NOT_FOUND] = "ORA-12416: policy string not found";
    ORACLE_STR_USER_ERROR[-OB_ERR_POLICY_STRING_NOT_FOUND] = "ORA-12416: policy string not found";
    ERROR_NAME[-OB_ERR_INVALID_LABEL_STRING] = "OB_ERR_INVALID_LABEL_STRING";
    MYSQL_ERRNO[-OB_ERR_INVALID_LABEL_STRING] = -1;
    SQLSTATE[-OB_ERR_INVALID_LABEL_STRING] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_LABEL_STRING] = "invalid label string";
    STR_USER_ERROR[-OB_ERR_INVALID_LABEL_STRING] = "invalid label string";
    ORACLE_ERRNO[-OB_ERR_INVALID_LABEL_STRING] = 12401;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_LABEL_STRING] = "ORA-12401: invalid label string";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_LABEL_STRING] = "ORA-12401: invalid label string";
    ERROR_NAME[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] =
        "OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING";
    MYSQL_ERRNO[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] = -1;
    SQLSTATE[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] = "HY000";
    STR_ERROR[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] =
        "undefined compartment string for policy string";
    STR_USER_ERROR[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] =
        "undefined compartment string for policy string";
    ORACLE_ERRNO[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] = 12462;
    ORACLE_STR_ERROR[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] =
        "ORA-12462: undefined compartment string for policy string";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING] =
        "ORA-12462: undefined compartment string for policy string";
    ERROR_NAME[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] = "OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING";
    MYSQL_ERRNO[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] = -1;
    SQLSTATE[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] = "HY000";
    STR_ERROR[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] = "undefined level string for policy string";
    STR_USER_ERROR[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] = "undefined level string for policy string";
    ORACLE_ERRNO[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] = 12461;
    ORACLE_STR_ERROR[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] =
        "ORA-12461: undefined level string for policy string";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING] =
        "ORA-12461: undefined level string for policy string";
    ERROR_NAME[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] = "OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING";
    MYSQL_ERRNO[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] = -1;
    SQLSTATE[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] = "HY000";
    STR_ERROR[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] = "undefined group string for policy string";
    STR_USER_ERROR[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] = "undefined group string for policy string";
    ORACLE_ERRNO[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] = 12463;
    ORACLE_STR_ERROR[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] =
        "ORA-12463: undefined group string for policy string";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING] =
        "ORA-12463: undefined group string for policy string";
    ERROR_NAME[-OB_ERR_LBAC_ERROR] = "OB_ERR_LBAC_ERROR";
    MYSQL_ERRNO[-OB_ERR_LBAC_ERROR] = -1;
    SQLSTATE[-OB_ERR_LBAC_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_LBAC_ERROR] = "LBAC error";
    STR_USER_ERROR[-OB_ERR_LBAC_ERROR] = "LBAC error: %s";
    ORACLE_ERRNO[-OB_ERR_LBAC_ERROR] = 12432;
    ORACLE_STR_ERROR[-OB_ERR_LBAC_ERROR] = "ORA-12432: LBAC error";
    ORACLE_STR_USER_ERROR[-OB_ERR_LBAC_ERROR] = "ORA-12432: LBAC error: %s";
    ERROR_NAME[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] =
        "OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING";
    MYSQL_ERRNO[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] = -1;
    SQLSTATE[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] = "HY000";
    STR_ERROR[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] = "policy role already exists for policy string";
    STR_USER_ERROR[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] =
        "policy role already exists for policy string";
    ORACLE_ERRNO[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] = 12447;
    ORACLE_STR_ERROR[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] =
        "ORA-12447: policy role already exists for policy string";
    ORACLE_STR_USER_ERROR[-OB_ERR_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING] =
        "ORA-12447: policy role already exists for policy string";
    ERROR_NAME[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = "OB_ERR_NULL_OR_INVALID_USER_LABEL";
    MYSQL_ERRNO[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = -1;
    SQLSTATE[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = "HY000";
    STR_ERROR[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = "NULL or invalid user label";
    STR_USER_ERROR[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = "NULL or invalid user label: %s";
    ORACLE_ERRNO[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = 12470;
    ORACLE_STR_ERROR[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = "ORA-12470: NULL or invalid user label";
    ORACLE_STR_USER_ERROR[-OB_ERR_NULL_OR_INVALID_USER_LABEL] = "ORA-12470: NULL or invalid user label: %s";
    ERROR_NAME[-OB_ERR_ADD_INDEX] = "OB_ERR_ADD_INDEX";
    MYSQL_ERRNO[-OB_ERR_ADD_INDEX] = -1;
    SQLSTATE[-OB_ERR_ADD_INDEX] = "HY000";
    STR_ERROR[-OB_ERR_ADD_INDEX] = "Add index failed";
    STR_USER_ERROR[-OB_ERR_ADD_INDEX] = "Add index failed";
    ORACLE_ERRNO[-OB_ERR_ADD_INDEX] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ADD_INDEX] = "ORA-00600: internal error code, arguments: -5703, Add index failed";
    ORACLE_STR_USER_ERROR[-OB_ERR_ADD_INDEX] = "ORA-00600: internal error code, arguments: -5703, Add index failed";
    ERROR_NAME[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = "OB_ERR_PROFILE_STRING_DOES_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = "profile string does not exist";
    STR_USER_ERROR[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = "profile %.*s does not exist";
    ORACLE_ERRNO[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = 2380;
    ORACLE_STR_ERROR[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = "ORA-02380: profile string does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_PROFILE_STRING_DOES_NOT_EXIST] = "ORA-02380: profile %.*s does not exist";
    ERROR_NAME[-OB_ERR_INVALID_RESOURCE_LIMIT] = "OB_ERR_INVALID_RESOURCE_LIMIT";
    MYSQL_ERRNO[-OB_ERR_INVALID_RESOURCE_LIMIT] = -1;
    SQLSTATE[-OB_ERR_INVALID_RESOURCE_LIMIT] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_RESOURCE_LIMIT] = "invalid resource limit";
    STR_USER_ERROR[-OB_ERR_INVALID_RESOURCE_LIMIT] = "invalid resource limit %s";
    ORACLE_ERRNO[-OB_ERR_INVALID_RESOURCE_LIMIT] = 2377;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_RESOURCE_LIMIT] = "ORA-02377: invalid resource limit";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_RESOURCE_LIMIT] = "ORA-02377: invalid resource limit %s";
    ERROR_NAME[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = "OB_ERR_PROFILE_STRING_ALREADY_EXISTS";
    MYSQL_ERRNO[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = -1;
    SQLSTATE[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = "HY000";
    STR_ERROR[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = "profile string already exists";
    STR_USER_ERROR[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = "profile %.*s already exists";
    ORACLE_ERRNO[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = 2379;
    ORACLE_STR_ERROR[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = "ORA-02379: profile string already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_PROFILE_STRING_ALREADY_EXISTS] = "ORA-02379: profile %.*s already exists";
    ERROR_NAME[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] = "OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED";
    MYSQL_ERRNO[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] = -1;
    SQLSTATE[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] = "HY000";
    STR_ERROR[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] =
        "profile string has users assigned, cannot drop without CASCADE";
    STR_USER_ERROR[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] =
        "profile %.*s has users assigned, cannot drop without CASCADE";
    ORACLE_ERRNO[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] = 2382;
    ORACLE_STR_ERROR[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] =
        "ORA-02382: profile string has users assigned, cannot drop without CASCADE";
    ORACLE_STR_USER_ERROR[-OB_ERR_PROFILE_STRING_HAS_USERS_ASSIGNED] =
        "ORA-02382: profile %.*s has users assigned, cannot drop without CASCADE";
    ERROR_NAME[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] =
        "OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL";
    MYSQL_ERRNO[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] = -1;
    SQLSTATE[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] = "HY000";
    STR_ERROR[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] =
        "the leading precision of the interval is too small";
    STR_USER_ERROR[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] =
        "the leading precision of the interval is too small";
    ORACLE_ERRNO[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] = 1873;
    ORACLE_STR_ERROR[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] =
        "ORA-01873: the leading precision of the interval is too small";
    ORACLE_STR_USER_ERROR[-OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL] =
        "ORA-01873: the leading precision of the interval is too small";
    ERROR_NAME[-OB_ERR_INVALID_TIME_ZONE_HOUR] = "OB_ERR_INVALID_TIME_ZONE_HOUR";
    MYSQL_ERRNO[-OB_ERR_INVALID_TIME_ZONE_HOUR] = -1;
    SQLSTATE[-OB_ERR_INVALID_TIME_ZONE_HOUR] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_TIME_ZONE_HOUR] = "time zone hour must be between -12 and 14";
    STR_USER_ERROR[-OB_ERR_INVALID_TIME_ZONE_HOUR] = "time zone hour must be between -12 and 14";
    ORACLE_ERRNO[-OB_ERR_INVALID_TIME_ZONE_HOUR] = 1874;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_TIME_ZONE_HOUR] = "ORA-01874: time zone hour must be between -15 and 15";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_TIME_ZONE_HOUR] = "ORA-01874: time zone hour must be between -15 and 15";
    ERROR_NAME[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = "OB_ERR_INVALID_TIME_ZONE_MINUTE";
    MYSQL_ERRNO[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = -1;
    SQLSTATE[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = "time zone minute must be between -59 and 59";
    STR_USER_ERROR[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = "time zone minute must be between -59 and 59";
    ORACLE_ERRNO[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = 1875;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = "ORA-01875: time zone minute must be between -59 and 59";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_TIME_ZONE_MINUTE] = "ORA-01875: time zone minute must be between -59 and 59";
    ERROR_NAME[-OB_ERR_NOT_A_VALID_TIME_ZONE] = "OB_ERR_NOT_A_VALID_TIME_ZONE";
    MYSQL_ERRNO[-OB_ERR_NOT_A_VALID_TIME_ZONE] = -1;
    SQLSTATE[-OB_ERR_NOT_A_VALID_TIME_ZONE] = "HY000";
    STR_ERROR[-OB_ERR_NOT_A_VALID_TIME_ZONE] = "not a valid time zone";
    STR_USER_ERROR[-OB_ERR_NOT_A_VALID_TIME_ZONE] = "not a valid time zone";
    ORACLE_ERRNO[-OB_ERR_NOT_A_VALID_TIME_ZONE] = 1857;
    ORACLE_STR_ERROR[-OB_ERR_NOT_A_VALID_TIME_ZONE] = "ORA-01857: not a valid time zone";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_A_VALID_TIME_ZONE] = "ORA-01857: not a valid time zone";
    ERROR_NAME[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] =
        "OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER";
    MYSQL_ERRNO[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] = -1;
    SQLSTATE[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] = "HY000";
    STR_ERROR[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] = "date format is too long for internal buffer";
    STR_USER_ERROR[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] = "date format is too long for internal buffer";
    ORACLE_ERRNO[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] = 1801;
    ORACLE_STR_ERROR[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] =
        "ORA-01801: date format is too long for internal buffer";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER] =
        "ORA-01801: date format is too long for internal buffer";
    ERROR_NAME[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] = "OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED";
    MYSQL_ERRNO[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] = -1;
    SQLSTATE[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] = "HY000";
    STR_ERROR[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] = "cannot validate - check constraint violated";
    STR_USER_ERROR[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] = "cannot validate (%.*s.%.*s) - check constraint violated";
    ORACLE_ERRNO[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] = 2293;
    ORACLE_STR_ERROR[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] = "ORA-02293: cannot validate - check constraint violated";
    ORACLE_STR_USER_ERROR[-OB_ERR_ADD_CHECK_CONSTRAINT_VIOLATED] =
        "ORA-02293: cannot validate (%.*s.%.*s) - check constraint violated";
    ERROR_NAME[-OB_ERR_ILLEGAL_VIEW_UPDATE] = "OB_ERR_ILLEGAL_VIEW_UPDATE";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_VIEW_UPDATE] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_VIEW_UPDATE] = "HY000";
    STR_ERROR[-OB_ERR_ILLEGAL_VIEW_UPDATE] = "data manipulation operation not legal on this view";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_VIEW_UPDATE] = "data manipulation operation not legal on this view";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_VIEW_UPDATE] = 1732;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_VIEW_UPDATE] = "ORA-01732: data manipulation operation not legal on this view";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_VIEW_UPDATE] =
        "ORA-01732: data manipulation operation not legal on this view";
    ERROR_NAME[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = "OB_ERR_VIRTUAL_COL_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = "virtual column not allowed here";
    STR_USER_ERROR[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = "virtual column not allowed here";
    ORACLE_ERRNO[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = 1733;
    ORACLE_STR_ERROR[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = "ORA-01733: virtual column not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_VIRTUAL_COL_NOT_ALLOWED] = "ORA-01733: virtual column not allowed here";
    ERROR_NAME[-OB_ERR_O_VIEW_MULTIUPDATE] = "OB_ERR_O_VIEW_MULTIUPDATE";
    MYSQL_ERRNO[-OB_ERR_O_VIEW_MULTIUPDATE] = -1;
    SQLSTATE[-OB_ERR_O_VIEW_MULTIUPDATE] = "HY000";
    STR_ERROR[-OB_ERR_O_VIEW_MULTIUPDATE] = "cannot modify more than one base table through a join view";
    STR_USER_ERROR[-OB_ERR_O_VIEW_MULTIUPDATE] = "cannot modify more than one base table through a join view";
    ORACLE_ERRNO[-OB_ERR_O_VIEW_MULTIUPDATE] = 1776;
    ORACLE_STR_ERROR[-OB_ERR_O_VIEW_MULTIUPDATE] =
        "ORA-01776: cannot modify more than one base table through a join view";
    ORACLE_STR_USER_ERROR[-OB_ERR_O_VIEW_MULTIUPDATE] =
        "ORA-01776: cannot modify more than one base table through a join view";
    ERROR_NAME[-OB_ERR_NON_INSERTABLE_TABLE] = "OB_ERR_NON_INSERTABLE_TABLE";
    MYSQL_ERRNO[-OB_ERR_NON_INSERTABLE_TABLE] = ER_NON_INSERTABLE_TABLE;
    SQLSTATE[-OB_ERR_NON_INSERTABLE_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_NON_INSERTABLE_TABLE] = "The target table is not insertable";
    STR_USER_ERROR[-OB_ERR_NON_INSERTABLE_TABLE] = "The target table %.*s of the INSERT is not insertable-into";
    ORACLE_ERRNO[-OB_ERR_NON_INSERTABLE_TABLE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NON_INSERTABLE_TABLE] =
        "ORA-00600: internal error code, arguments: -5717, The target table is not insertable";
    ORACLE_STR_USER_ERROR[-OB_ERR_NON_INSERTABLE_TABLE] =
        "ORA-00600: internal error code, arguments: -5717, The target table %.*s of the INSERT is not insertable-into";
    ERROR_NAME[-OB_ERR_VIEW_MULTIUPDATE] = "OB_ERR_VIEW_MULTIUPDATE";
    MYSQL_ERRNO[-OB_ERR_VIEW_MULTIUPDATE] = ER_VIEW_MULTIUPDATE;
    SQLSTATE[-OB_ERR_VIEW_MULTIUPDATE] = "HY000";
    STR_ERROR[-OB_ERR_VIEW_MULTIUPDATE] = "Can not modify more than one base table through a join view";
    STR_USER_ERROR[-OB_ERR_VIEW_MULTIUPDATE] =
        "Can not modify more than one base table through a join view '%.*s.%.*s'";
    ORACLE_ERRNO[-OB_ERR_VIEW_MULTIUPDATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VIEW_MULTIUPDATE] =
        "ORA-00600: internal error code, arguments: -5718, Can not modify more than one base table through a join view";
    ORACLE_STR_USER_ERROR[-OB_ERR_VIEW_MULTIUPDATE] = "ORA-00600: internal error code, arguments: -5718, Can not "
                                                      "modify more than one base table through a join view '%.*s.%.*s'";
    ERROR_NAME[-OB_ERR_NONUPDATEABLE_COLUMN] = "OB_ERR_NONUPDATEABLE_COLUMN";
    MYSQL_ERRNO[-OB_ERR_NONUPDATEABLE_COLUMN] = ER_NONUPDATEABLE_COLUMN;
    SQLSTATE[-OB_ERR_NONUPDATEABLE_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_NONUPDATEABLE_COLUMN] = "Column is not updatable";
    STR_USER_ERROR[-OB_ERR_NONUPDATEABLE_COLUMN] = "Column '%.*s' is not updatable";
    ORACLE_ERRNO[-OB_ERR_NONUPDATEABLE_COLUMN] = 600;
    ORACLE_STR_ERROR[-OB_ERR_NONUPDATEABLE_COLUMN] =
        "ORA-00600: internal error code, arguments: -5719, Column is not updatable";
    ORACLE_STR_USER_ERROR[-OB_ERR_NONUPDATEABLE_COLUMN] =
        "ORA-00600: internal error code, arguments: -5719, Column '%.*s' is not updatable";
    ERROR_NAME[-OB_ERR_VIEW_DELETE_MERGE_VIEW] = "OB_ERR_VIEW_DELETE_MERGE_VIEW";
    MYSQL_ERRNO[-OB_ERR_VIEW_DELETE_MERGE_VIEW] = ER_VIEW_DELETE_MERGE_VIEW;
    SQLSTATE[-OB_ERR_VIEW_DELETE_MERGE_VIEW] = "HY000";
    STR_ERROR[-OB_ERR_VIEW_DELETE_MERGE_VIEW] = "Can not delete from join view";
    STR_USER_ERROR[-OB_ERR_VIEW_DELETE_MERGE_VIEW] = "Can not delete from join view '%.*s.%.*s'";
    ORACLE_ERRNO[-OB_ERR_VIEW_DELETE_MERGE_VIEW] = 600;
    ORACLE_STR_ERROR[-OB_ERR_VIEW_DELETE_MERGE_VIEW] =
        "ORA-00600: internal error code, arguments: -5720, Can not delete from join view";
    ORACLE_STR_USER_ERROR[-OB_ERR_VIEW_DELETE_MERGE_VIEW] =
        "ORA-00600: internal error code, arguments: -5720, Can not delete from join view '%.*s.%.*s'";
    ERROR_NAME[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] = "OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED";
    MYSQL_ERRNO[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] = -1;
    SQLSTATE[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] = "HY000";
    STR_ERROR[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] =
        "cannot delete from view without exactly one key-preserved table";
    STR_USER_ERROR[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] =
        "cannot delete from view without exactly one key-preserved table";
    ORACLE_ERRNO[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] = 1752;
    ORACLE_STR_ERROR[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] =
        "ORA-01752: cannot delete from view without exactly one key-preserved table";
    ORACLE_STR_USER_ERROR[-OB_ERR_O_DELETE_VIEW_NON_KEY_PRESERVED] =
        "ORA-01752: cannot delete from view without exactly one key-preserved table";
    ERROR_NAME[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] = "OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED";
    MYSQL_ERRNO[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] = -1;
    SQLSTATE[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] = "HY000";
    STR_ERROR[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] =
        "cannot modify a column which maps to a non key-preserved table";
    STR_USER_ERROR[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] =
        "cannot modify a column which maps to a non key-preserved table";
    ORACLE_ERRNO[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] = 1779;
    ORACLE_STR_ERROR[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] =
        "ORA-01779: cannot modify a column which maps to a non key-preserved table";
    ORACLE_STR_USER_ERROR[-OB_ERR_O_UPDATE_VIEW_NON_KEY_PRESERVED] =
        "ORA-01779: cannot modify a column which maps to a non key-preserved table";
    ERROR_NAME[-OB_ERR_MODIFY_READ_ONLY_VIEW] = "OB_ERR_MODIFY_READ_ONLY_VIEW";
    MYSQL_ERRNO[-OB_ERR_MODIFY_READ_ONLY_VIEW] = -1;
    SQLSTATE[-OB_ERR_MODIFY_READ_ONLY_VIEW] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_READ_ONLY_VIEW] = "cannot perform a DML operation on a read-only view";
    STR_USER_ERROR[-OB_ERR_MODIFY_READ_ONLY_VIEW] = "cannot perform a DML operation on a read-only view";
    ORACLE_ERRNO[-OB_ERR_MODIFY_READ_ONLY_VIEW] = 42399;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_READ_ONLY_VIEW] = "ORA-42399: cannot perform a DML operation on a read-only view";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_READ_ONLY_VIEW] =
        "ORA-42399: cannot perform a DML operation on a read-only view";
    ERROR_NAME[-OB_ERR_INVALID_INITRANS_VALUE] = "OB_ERR_INVALID_INITRANS_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_INITRANS_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_INITRANS_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_INITRANS_VALUE] = "invalid INITRANS option value";
    STR_USER_ERROR[-OB_ERR_INVALID_INITRANS_VALUE] = "invalid INITRANS option value";
    ORACLE_ERRNO[-OB_ERR_INVALID_INITRANS_VALUE] = 2207;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_INITRANS_VALUE] = "ORA-02207: invalid INITRANS option value";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_INITRANS_VALUE] = "ORA-02207: invalid INITRANS option value";
    ERROR_NAME[-OB_ERR_INVALID_MAXTRANS_VALUE] = "OB_ERR_INVALID_MAXTRANS_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_MAXTRANS_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_MAXTRANS_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_MAXTRANS_VALUE] = "invalid MAXTRANS option value";
    STR_USER_ERROR[-OB_ERR_INVALID_MAXTRANS_VALUE] = "invalid MAXTRANS option value";
    ORACLE_ERRNO[-OB_ERR_INVALID_MAXTRANS_VALUE] = 2209;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_MAXTRANS_VALUE] = "ORA-02209: invalid MAXTRANS option value";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_MAXTRANS_VALUE] = "ORA-02209: invalid MAXTRANS option value";
    ERROR_NAME[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = "OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = "invalid value for PCTFREE or PCTUSED";
    STR_USER_ERROR[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = "invalid value for PCTFREE or PCTUSED";
    ORACLE_ERRNO[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = 2211;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = "ORA-02211: invalid value for PCTFREE or PCTUSED";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE] = "ORA-02211: invalid value for PCTFREE or PCTUSED";
    ERROR_NAME[-OB_ERR_PROXY_REROUTE] = "OB_ERR_PROXY_REROUTE";
    MYSQL_ERRNO[-OB_ERR_PROXY_REROUTE] = -1;
    SQLSTATE[-OB_ERR_PROXY_REROUTE] = "HY000";
    STR_ERROR[-OB_ERR_PROXY_REROUTE] = "SQL request should be rerouted";
    STR_USER_ERROR[-OB_ERR_PROXY_REROUTE] = "SQL request should be rerouted";
    ORACLE_ERRNO[-OB_ERR_PROXY_REROUTE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PROXY_REROUTE] =
        "ORA-00600: internal error code, arguments: -5727, SQL request should be rerouted";
    ORACLE_STR_USER_ERROR[-OB_ERR_PROXY_REROUTE] =
        "ORA-00600: internal error code, arguments: -5727, SQL request should be rerouted";
    ERROR_NAME[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = "OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = "HY000";
    STR_ERROR[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = "illegal argument for function";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = "illegal argument for function";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = 1760;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = "ORA-01760: illegal argument for function";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION] = "ORA-01760: illegal argument for function";
    ERROR_NAME[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] = "OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST";
    MYSQL_ERRNO[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] = -1;
    SQLSTATE[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] = "HY000";
    STR_ERROR[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] = "this operator cannot be used with lists";
    STR_USER_ERROR[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] = "this operator cannot be used with lists";
    ORACLE_ERRNO[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] = 1796;
    ORACLE_STR_ERROR[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] = "ORA-01796: this operator cannot be used with lists";
    ORACLE_STR_USER_ERROR[-OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST] =
        "ORA-01796: this operator cannot be used with lists";
    ERROR_NAME[-OB_ERR_INVALID_SAMPLING_RANGE] = "OB_ERR_INVALID_SAMPLING_RANGE";
    MYSQL_ERRNO[-OB_ERR_INVALID_SAMPLING_RANGE] = -1;
    SQLSTATE[-OB_ERR_INVALID_SAMPLING_RANGE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SAMPLING_RANGE] = "SAMPLE percentage must be in the range [0.000001,100)";
    STR_USER_ERROR[-OB_ERR_INVALID_SAMPLING_RANGE] = "SAMPLE percentage must be in the range [0.000001,100)";
    ORACLE_ERRNO[-OB_ERR_INVALID_SAMPLING_RANGE] = 30562;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SAMPLING_RANGE] =
        "ORA-30562: SAMPLE percentage must be in the range [0.000001,100)";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SAMPLING_RANGE] =
        "ORA-30562: SAMPLE percentage must be in the range [0.000001,100)";
    ERROR_NAME[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] = "OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] = "specifying owner's name of the table is not allowed";
    STR_USER_ERROR[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] = "specifying owner's name of the table is not allowed";
    ORACLE_ERRNO[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] = 1765;
    ORACLE_STR_ERROR[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] =
        "ORA-01765: specifying owner's name of the table is not allowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED] =
        "ORA-01765: specifying owner's name of the table is not allowed";
    ERROR_NAME[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = "OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE";
    MYSQL_ERRNO[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = -1;
    SQLSTATE[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = "HY000";
    STR_ERROR[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = "stmt trigger with when clause";
    STR_USER_ERROR[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = "stmt trigger with when clause";
    ORACLE_ERRNO[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = 4077;
    ORACLE_STR_ERROR[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = "ORA-04077: stmt trigger with when clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_STMT_TRIGGER_WITH_WHEN_CLAUSE] = "ORA-04077: stmt trigger with when clause";
    ERROR_NAME[-OB_ERR_TRIGGER_NOT_EXIST] = "OB_ERR_TRIGGER_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_TRIGGER_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_TRIGGER_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_TRIGGER_NOT_EXIST] = "trigger does not exist";
    STR_USER_ERROR[-OB_ERR_TRIGGER_NOT_EXIST] = "trigger '%.*s' does not exist";
    ORACLE_ERRNO[-OB_ERR_TRIGGER_NOT_EXIST] = 4080;
    ORACLE_STR_ERROR[-OB_ERR_TRIGGER_NOT_EXIST] = "ORA-04080: trigger not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_TRIGGER_NOT_EXIST] = "ORA-04080: trigger '%.*s' does not exist";
    ERROR_NAME[-OB_ERR_TRIGGER_ALREADY_EXIST] = "OB_ERR_TRIGGER_ALREADY_EXIST";
    MYSQL_ERRNO[-OB_ERR_TRIGGER_ALREADY_EXIST] = -1;
    SQLSTATE[-OB_ERR_TRIGGER_ALREADY_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_TRIGGER_ALREADY_EXIST] = "trigger already exist";
    STR_USER_ERROR[-OB_ERR_TRIGGER_ALREADY_EXIST] = "trigger '%.*s' already exist";
    ORACLE_ERRNO[-OB_ERR_TRIGGER_ALREADY_EXIST] = 4081;
    ORACLE_STR_ERROR[-OB_ERR_TRIGGER_ALREADY_EXIST] = "ORA-04081: trigger already exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_TRIGGER_ALREADY_EXIST] = "ORA-04081: trigger '%.*s' already exist";
    ERROR_NAME[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] = "OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE";
    MYSQL_ERRNO[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] = -1;
    SQLSTATE[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] = "trigger already exists on another table, cannot replace it";
    STR_USER_ERROR[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] =
        "trigger '%.*s' already exists on another table, cannot replace it";
    ORACLE_ERRNO[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] = 4095;
    ORACLE_STR_ERROR[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] =
        "ORA-04095: trigger already exists on another table, cannot replace it";
    ORACLE_STR_USER_ERROR[-OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE] =
        "ORA-04095: trigger '%.*s' already exists on another table, cannot replace it";
    ERROR_NAME[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] = "OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER";
    MYSQL_ERRNO[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] = -1;
    SQLSTATE[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] = "HY000";
    STR_ERROR[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] = "error signaled in parallel query server";
    STR_USER_ERROR[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] = "error signaled in parallel query server";
    ORACLE_ERRNO[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] = 12801;
    ORACLE_STR_ERROR[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] = "ORA-12801: error signaled in parallel query server";
    ORACLE_STR_USER_ERROR[-OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER] =
        "ORA-12801: error signaled in parallel query server";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] = "OB_ERR_CTE_ILLEGAL_QUERY_NAME";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] = "illegal reference of a query name in WITH clause";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] = "illegal reference of a query name in WITH clause";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] = 32031;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] = "ORA-32031: illegal reference of a query name in WITH clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_QUERY_NAME] =
        "ORA-32031: illegal reference of a query name in WITH clause";
    ERROR_NAME[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = "OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING";
    MYSQL_ERRNO[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = -1;
    SQLSTATE[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = "HY000";
    STR_ERROR[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = "unsupported column aliasing";
    STR_USER_ERROR[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = "unsupported column aliasing";
    ORACLE_ERRNO[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = 32033;
    ORACLE_STR_ERROR[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = "ORA-32033: unsupported column aliasing";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_UNSUPPORTED_COLUMN_ALIASING] = "ORA-32033: unsupported column aliasing";
    ERROR_NAME[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = "OB_ERR_UNSUPPORTED_USE_OF_CTE";
    MYSQL_ERRNO[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = -1;
    SQLSTATE[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = "HY000";
    STR_ERROR[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = "unsupported use of WITH clause";
    STR_USER_ERROR[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = "unsupported use of WITH clause";
    ORACLE_ERRNO[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = 32034;
    ORACLE_STR_ERROR[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = "ORA-32034: unsupported use of WITH clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_UNSUPPORTED_USE_OF_CTE] = "ORA-32034: unsupported use of WITH clause";
    ERROR_NAME[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] = "OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH";
    MYSQL_ERRNO[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] = -1;
    SQLSTATE[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] =
        "number of WITH clause column names does not match number of elements in select list";
    STR_USER_ERROR[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] =
        "number of WITH clause column names does not match number of elements in select list";
    ORACLE_ERRNO[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] = 32038;
    ORACLE_STR_ERROR[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] =
        "ORA-32038: number of WITH clause column names does not match number of elements in select list";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_COLUMN_NUMBER_NOT_MATCH] =
        "ORA-32038: number of WITH clause column names does not match number of elements in select list";
    ERROR_NAME[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] = "OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE";
    MYSQL_ERRNO[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] = -1;
    SQLSTATE[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] = "HY000";
    STR_ERROR[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] = "recursive WITH clause must have column alias list";
    STR_USER_ERROR[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] =
        "recursive WITH clause must have column alias list";
    ORACLE_ERRNO[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] = 32039;
    ORACLE_STR_ERROR[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] =
        "ORA-32039: recursive WITH clause must have column alias list";
    ORACLE_STR_USER_ERROR[-OB_ERR_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE] =
        "ORA-32039: recursive WITH clause must have column alias list";
    ERROR_NAME[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] = "OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE";
    MYSQL_ERRNO[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] = -1;
    SQLSTATE[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] = "HY000";
    STR_ERROR[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] = "recursive WITH clause must use a UNION ALL operation";
    STR_USER_ERROR[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] = "recursive WITH clause must use a UNION ALL operation";
    ORACLE_ERRNO[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] = 32040;
    ORACLE_STR_ERROR[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] =
        "ORA-32040: recursive WITH clause must use a UNION ALL operation";
    ORACLE_STR_USER_ERROR[-OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE] =
        "ORA-32040: recursive WITH clause must use a UNION ALL operation";
    ERROR_NAME[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] = "OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE";
    MYSQL_ERRNO[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] = -1;
    SQLSTATE[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] = "HY000";
    STR_ERROR[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] =
        "UNION ALL operation in recursive WITH clause must have only two branches";
    STR_USER_ERROR[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] =
        "UNION ALL operation in recursive WITH clause must have only two branches";
    ORACLE_ERRNO[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] = 32041;
    ORACLE_STR_ERROR[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] =
        "ORA-32041: UNION ALL operation in recursive WITH clause must have only two branches";
    ORACLE_STR_USER_ERROR[-OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE] =
        "ORA-32041: UNION ALL operation in recursive WITH clause must have only two branches";
    ERROR_NAME[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] =
        "OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE";
    MYSQL_ERRNO[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] = -1;
    SQLSTATE[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] = "HY000";
    STR_ERROR[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] =
        "recursive WITH clause must reference itself directly in one of the UNION ALL branches";
    STR_USER_ERROR[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] =
        "recursive WITH clause must reference itself directly in one of the UNION ALL branches";
    ORACLE_ERRNO[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] = 32042;
    ORACLE_STR_ERROR[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] =
        "ORA-32042: recursive WITH clause must reference itself directly in one of the UNION ALL branches";
    ORACLE_STR_USER_ERROR[-OB_ERR_NEED_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE] =
        "ORA-32042: recursive WITH clause must reference itself directly in one of the UNION ALL branches";
    ERROR_NAME[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] = "OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE";
    MYSQL_ERRNO[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] = -1;
    SQLSTATE[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] = "HY000";
    STR_ERROR[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] = "recursive WITH clause needs an initialization branch";
    STR_USER_ERROR[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] = "recursive WITH clause needs an initialization branch";
    ORACLE_ERRNO[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] = 32043;
    ORACLE_STR_ERROR[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] =
        "ORA-32043: recursive WITH clause needs an initialization branch";
    ORACLE_STR_USER_ERROR[-OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE] =
        "ORA-32043: recursive WITH clause needs an initialization branch";
    ERROR_NAME[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] = "OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE";
    MYSQL_ERRNO[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] = -1;
    SQLSTATE[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] = "HY000";
    STR_ERROR[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] = "cycle detected while executing recursive WITH query";
    STR_USER_ERROR[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] = "cycle detected while executing recursive WITH query";
    ORACLE_ERRNO[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] = 32044;
    ORACLE_STR_ERROR[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] =
        "ORA-32044: cycle detected while executing recursive WITH query";
    ORACLE_STR_USER_ERROR[-OB_ERR_CYCLE_FOUND_IN_RECURSIVE_CTE] =
        "ORA-32044: cycle detected while executing recursive WITH query";
    ERROR_NAME[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] = "OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION";
    MYSQL_ERRNO[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] = -1;
    SQLSTATE[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] = "HY000";
    STR_ERROR[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] =
        "maximum level of recursion reached while executing recursive WITH query";
    STR_USER_ERROR[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] =
        "maximum level of recursion reached while executing recursive WITH query";
    ORACLE_ERRNO[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] = 32045;
    ORACLE_STR_ERROR[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] =
        "ORA-32045: maximum level of recursion reached while executing recursive WITH query";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_REACH_MAX_LEVEL_RECURSION] =
        "ORA-32045: maximum level of recursion reached while executing recursive WITH query";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] = "OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] =
        "sequence column name for SEARCH clause must not be part of the column alias list";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] =
        "sequence column name for SEARCH clause must not be part of the column alias list";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] = 32046;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] =
        "ORA-32046: sequence column name for SEARCH clause must not be part of the column alias list";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME] =
        "ORA-32046: sequence column name for SEARCH clause must not be part of the column alias list";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] = "OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] =
        "cycle mark value and non-cycle mark value must be one byte character string values";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] =
        "cycle mark value and non-cycle mark value must be one byte character string values";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] = 32047;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] =
        "ORA-32047: cycle mark value and non-cycle mark value must be one byte character string values";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE] =
        "ORA-32047: cycle mark value and non-cycle mark value must be one byte character string values";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] = "OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] =
        "cycle mark column name for CYCLE clause must not be part of the column alias list";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] =
        "cycle mark column name for CYCLE clause must not be part of the column alias list";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] = 32048;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] =
        "ORA-32048: cycle mark column name for CYCLE clause must not be part of the column alias list";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME] =
        "ORA-32048: cycle mark column name for CYCLE clause must not be part of the column alias list";
    ERROR_NAME[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] = "OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE";
    MYSQL_ERRNO[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] = -1;
    SQLSTATE[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] = "duplicate name found in column alias list for WITH clause";
    STR_USER_ERROR[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] = "duplicate name found in column alias list for WITH clause";
    ORACLE_ERRNO[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] = 32049;
    ORACLE_STR_ERROR[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] =
        "ORA-32049: duplicate name found in column alias list for WITH clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_COLUMN_ALIAS_DUPLICATE] =
        "ORA-32049: duplicate name found in column alias list for WITH clause";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] = "OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] =
        "SEARCH and CYCLE clauses can only be specified for recursive WITH clause elements";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] =
        "SEARCH and CYCLE clauses can only be specified for recursive WITH clause elements";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] = 32480;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] =
        "ORA-32480: SEARCH and CYCLE clauses can only be specified for recursive WITH clause elements";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE] =
        "ORA-32480: SEARCH and CYCLE clauses can only be specified for recursive WITH clause elements";
    ERROR_NAME[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] = "OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE";
    MYSQL_ERRNO[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] = -1;
    SQLSTATE[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] =
        "cycle value for CYCLE clause must be different from the non-cycle value";
    STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] =
        "cycle value for CYCLE clause must be different from the non-cycle value";
    ORACLE_ERRNO[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] = 32481;
    ORACLE_STR_ERROR[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] =
        "ORA-32481: cycle value for CYCLE clause must be different from the non-cycle value";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE] =
        "ORA-32481: cycle value for CYCLE clause must be different from the non-cycle value";
    ERROR_NAME[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] = "OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN";
    MYSQL_ERRNO[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] = -1;
    SQLSTATE[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] =
        "sequence column for SEARCH clause must be different from the cycle mark column for CYCLE clause";
    STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] =
        "sequence column for SEARCH clause must be different from the cycle mark column for CYCLE clause";
    ORACLE_ERRNO[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] = 32482;
    ORACLE_STR_ERROR[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] =
        "ORA-32482: sequence column for SEARCH clause must be different from the cycle mark column for CYCLE clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN] =
        "ORA-32482: sequence column for SEARCH clause must be different from the cycle mark column for CYCLE clause";
    ERROR_NAME[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] = "OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE";
    MYSQL_ERRNO[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] = -1;
    SQLSTATE[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] =
        "duplicate name found in sort specification list for SEARCH clause of WITH clause";
    STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] =
        "duplicate name found in sort specification list for SEARCH clause of WITH clause";
    ORACLE_ERRNO[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] = 32483;
    ORACLE_STR_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] =
        "ORA-32483: duplicate name found in sort specification list for SEARCH clause of WITH clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE] =
        "ORA-32483: duplicate name found in sort specification list for SEARCH clause of WITH clause";
    ERROR_NAME[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] = "OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE";
    MYSQL_ERRNO[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] = -1;
    SQLSTATE[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] =
        "duplicate name found in cycle column list for CYCLE clause of WITH clause";
    STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] =
        "duplicate name found in cycle column list for CYCLE clause of WITH clause";
    ORACLE_ERRNO[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] = 32484;
    ORACLE_STR_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] =
        "ORA-32484: duplicate name found in cycle column list for CYCLE clause of WITH clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE] =
        "ORA-32484: duplicate name found in cycle column list for CYCLE clause of WITH clause";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] = "OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] =
        "element in cycle column list of CYCLE clause must appear in the column alias list of the WITH clause element";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] =
        "element in cycle column list of CYCLE clause must appear in the column alias list of the WITH clause element";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] = 32485;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] =
        "ORA-32485: element in cycle column list of CYCLE clause must appear in the column alias list of the WITH "
        "clause element";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE] =
        "ORA-32485: element in cycle column list of CYCLE clause must appear in the column alias list of the WITH "
        "clause element";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] = "OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] =
        "unsupported operation in recursive branch of recursive WITH clause";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] =
        "unsupported operation in recursive branch of recursive WITH clause";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] = 32486;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] =
        "ORA-32486: unsupported operation in recursive branch of recursive WITH clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH] =
        "ORA-32486: unsupported operation in recursive branch of recursive WITH clause";
    ERROR_NAME[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] = "OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] = "HY000";
    STR_ERROR[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] = "unsupported join in recursive WITH query";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] = "unsupported join in recursive WITH query";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] = 32487;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] = "ORA-32487: unsupported join in recursive WITH query";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE] =
        "ORA-32487: unsupported join in recursive WITH query";
    ERROR_NAME[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] = "OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST";
    MYSQL_ERRNO[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] = -1;
    SQLSTATE[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] = "HY000";
    STR_ERROR[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] = "WITH clause element did not have a column alias list";
    STR_USER_ERROR[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] = "WITH clause element did not have a column alias list";
    ORACLE_ERRNO[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] = 32488;
    ORACLE_STR_ERROR[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] =
        "ORA-32488: WITH clause element did not have a column alias list";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST] =
        "ORA-32488: WITH clause element did not have a column alias list";
    ERROR_NAME[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] = "OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE";
    MYSQL_ERRNO[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] = -1;
    SQLSTATE[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] =
        "element in sort specification list of SEARCH clause did not appear in the column alias list of the WITH "
        "clause element";
    STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] =
        "element in sort specification list of SEARCH clause did not appear in the column alias list of the WITH "
        "clause element";
    ORACLE_ERRNO[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] = 32489;
    ORACLE_STR_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] =
        "ORA-32489: element in sort specification list of SEARCH clause did not appear in the column alias list of the "
        "WITH clause element";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE] =
        "ORA-32489: element in sort specification list of SEARCH clause did not appear in the column alias list of the "
        "WITH clause element";
    ERROR_NAME[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] =
        "OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE";
    MYSQL_ERRNO[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] = -1;
    SQLSTATE[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] = "HY000";
    STR_ERROR[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] =
        "recursive query name referenced more than once in recursive branch of recursive WITH clause element";
    STR_USER_ERROR[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] =
        "recursive query name referenced more than once in recursive branch of recursive WITH clause element";
    ORACLE_ERRNO[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] = 32490;
    ORACLE_STR_ERROR[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] =
        "ORA-32490: recursive query name referenced more than once in recursive branch of recursive WITH clause "
        "element";
    ORACLE_STR_USER_ERROR[-OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE] =
        "ORA-32490: recursive query name referenced more than once in recursive branch of recursive WITH clause "
        "element";
    ERROR_NAME[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] = "OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] = "Specified pseudo column or operator not allowed here";
    STR_USER_ERROR[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] = "Specified pseudo column or operator not allowed here";
    ORACLE_ERRNO[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] = 976;
    ORACLE_STR_ERROR[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] =
        "ORA-00976: Specified pseudo column or operator not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED] =
        "ORA-00976: Specified pseudo column or operator not allowed here";
    ERROR_NAME[-OB_ERR_CBY_LOOP] = "OB_ERR_CBY_LOOP";
    MYSQL_ERRNO[-OB_ERR_CBY_LOOP] = -1;
    SQLSTATE[-OB_ERR_CBY_LOOP] = "HY000";
    STR_ERROR[-OB_ERR_CBY_LOOP] = "CONNECT BY loop in user data";
    STR_USER_ERROR[-OB_ERR_CBY_LOOP] = "CONNECT BY loop in user data";
    ORACLE_ERRNO[-OB_ERR_CBY_LOOP] = 1436;
    ORACLE_STR_ERROR[-OB_ERR_CBY_LOOP] = "ORA-01436: CONNECT BY loop in user data";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_LOOP] = "ORA-01436: CONNECT BY loop in user data";
    ERROR_NAME[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = "OB_ERR_CBY_JOIN_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = "cannot have join with CONNECT BY";
    STR_USER_ERROR[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = "cannot have join with CONNECT BY";
    ORACLE_ERRNO[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = 1437;
    ORACLE_STR_ERROR[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = "ORA-01437: cannot have join with CONNECT BY";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_JOIN_NOT_ALLOWED] = "ORA-01437: cannot have join with CONNECT BY";
    ERROR_NAME[-OB_ERR_CBY_CONNECT_BY_REQUIRED] = "OB_ERR_CBY_CONNECT_BY_REQUIRED";
    MYSQL_ERRNO[-OB_ERR_CBY_CONNECT_BY_REQUIRED] = -1;
    SQLSTATE[-OB_ERR_CBY_CONNECT_BY_REQUIRED] = "HY000";
    STR_ERROR[-OB_ERR_CBY_CONNECT_BY_REQUIRED] = "CONNECT BY clause required in this query block";
    STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_REQUIRED] = "CONNECT BY clause required in this query block";
    ORACLE_ERRNO[-OB_ERR_CBY_CONNECT_BY_REQUIRED] = 1788;
    ORACLE_STR_ERROR[-OB_ERR_CBY_CONNECT_BY_REQUIRED] = "ORA-01788: CONNECT BY clause required in this query block";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_REQUIRED] =
        "ORA-01788: CONNECT BY clause required in this query block";
    ERROR_NAME[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] = "OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] = "SYS_CONNECT_BY_PATH function is not allowed here";
    STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] = "SYS_CONNECT_BY_PATH function is not allowed here";
    ORACLE_ERRNO[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] = 30002;
    ORACLE_STR_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] =
        "ORA-30002: SYS_CONNECT_BY_PATH function is not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED] =
        "ORA-30002: SYS_CONNECT_BY_PATH function is not allowed here";
    ERROR_NAME[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] = "OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM";
    MYSQL_ERRNO[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] = -1;
    SQLSTATE[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] = "HY000";
    STR_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] = "illegal parameter in SYS_CONNECT_BY_PATH function";
    STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] = "illegal parameter in SYS_CONNECT_BY_PATH function";
    ORACLE_ERRNO[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] = 30003;
    ORACLE_STR_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] =
        "ORA-30003: illegal parameter in SYS_CONNECT_BY_PATH function";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM] =
        "ORA-30003: illegal parameter in SYS_CONNECT_BY_PATH function";
    ERROR_NAME[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] = "OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR";
    MYSQL_ERRNO[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] = -1;
    SQLSTATE[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] = "HY000";
    STR_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] =
        "A column value contained the string that the SYS_CONNECT_BY_PATH function was to use to separate column "
        "values";
    STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] =
        "A column value contained the string that the SYS_CONNECT_BY_PATH function was to use to separate column "
        "values";
    ORACLE_ERRNO[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] = 30004;
    ORACLE_STR_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] =
        "ORA-30004: A column value contained the string that the SYS_CONNECT_BY_PATH function was to use to separate "
        "column values.";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR] =
        "ORA-30004: A column value contained the string that the SYS_CONNECT_BY_PATH function was to use to separate "
        "column values.";
    ERROR_NAME[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] = "OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED";
    MYSQL_ERRNO[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] = -1;
    SQLSTATE[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] = "HY000";
    STR_ERROR[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] =
        "CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition";
    STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] =
        "CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition";
    ORACLE_ERRNO[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] = 30007;
    ORACLE_STR_ERROR[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] =
        "ORA-30007: CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED] =
        "ORA-30007: CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition";
    ERROR_NAME[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] = "OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] = "ORDER SIBLINGS BY clause not allowed here";
    STR_USER_ERROR[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] = "ORDER SIBLINGS BY clause not allowed here";
    ORACLE_ERRNO[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] = 30929;
    ORACLE_STR_ERROR[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] =
        "ORA-30929: ORDER SIBLINGS BY clause not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED] =
        "ORA-30929: ORDER SIBLINGS BY clause not allowed here";
    ERROR_NAME[-OB_ERR_CBY_NOCYCLE_REQUIRED] = "OB_ERR_CBY_NOCYCLE_REQUIRED";
    MYSQL_ERRNO[-OB_ERR_CBY_NOCYCLE_REQUIRED] = -1;
    SQLSTATE[-OB_ERR_CBY_NOCYCLE_REQUIRED] = "HY000";
    STR_ERROR[-OB_ERR_CBY_NOCYCLE_REQUIRED] = "NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudo column";
    STR_USER_ERROR[-OB_ERR_CBY_NOCYCLE_REQUIRED] = "NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudo column";
    ORACLE_ERRNO[-OB_ERR_CBY_NOCYCLE_REQUIRED] = 30930;
    ORACLE_STR_ERROR[-OB_ERR_CBY_NOCYCLE_REQUIRED] =
        "ORA-30930: NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudo column";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_NOCYCLE_REQUIRED] =
        "ORA-30930: NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudo column";
    ERROR_NAME[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = "OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN";
    MYSQL_ERRNO[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = -1;
    SQLSTATE[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = "HY000";
    STR_ERROR[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = "not enough arguments for function";
    STR_USER_ERROR[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = "not enough arguments for function";
    ORACLE_ERRNO[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = 938;
    ORACLE_STR_ERROR[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = "ORA-00938: not enough arguments for function";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN] = "ORA-00938: not enough arguments for function";
    ERROR_NAME[-OB_ERR_PREPARE_STMT_CHECKSUM] = "OB_ERR_PREPARE_STMT_CHECKSUM";
    MYSQL_ERRNO[-OB_ERR_PREPARE_STMT_CHECKSUM] = -1;
    SQLSTATE[-OB_ERR_PREPARE_STMT_CHECKSUM] = "HY000";
    STR_ERROR[-OB_ERR_PREPARE_STMT_CHECKSUM] = "Prepare statement checksum error";
    STR_USER_ERROR[-OB_ERR_PREPARE_STMT_CHECKSUM] = "Prepare statement checksum error";
    ORACLE_ERRNO[-OB_ERR_PREPARE_STMT_CHECKSUM] = 603;
    ORACLE_STR_ERROR[-OB_ERR_PREPARE_STMT_CHECKSUM] = "ORA-00603: Oracle Server session terminated by fatal error";
    ORACLE_STR_USER_ERROR[-OB_ERR_PREPARE_STMT_CHECKSUM] = "ORA-00603: Oracle Server session terminated by fatal error";
    ERROR_NAME[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] = "OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT";
    MYSQL_ERRNO[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] = -1;
    SQLSTATE[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] = "HY000";
    STR_ERROR[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] = "cannot enable constraint - no such constraint";
    STR_USER_ERROR[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] = "cannot enable constraint (%.*s) - no such constraint";
    ORACLE_ERRNO[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] = 2430;
    ORACLE_STR_ERROR[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] =
        "ORA-02430: cannot enable constraint - no such constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_ENABLE_NONEXISTENT_CONSTRAINT] =
        "ORA-02430: cannot enable constraint (%.*s) - no such constraint";
    ERROR_NAME[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] = "OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT";
    MYSQL_ERRNO[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] = -1;
    SQLSTATE[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] = "HY000";
    STR_ERROR[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] = "cannot disable constraint - no such constraint";
    STR_USER_ERROR[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] = "cannot disable constraint (%.*s) - no such constraint";
    ORACLE_ERRNO[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] = 2431;
    ORACLE_STR_ERROR[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] =
        "ORA-02431: cannot disable constraint - no such constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_DISABLE_NONEXISTENT_CONSTRAINT] =
        "ORA-02431: cannot disable constraint (%.*s) - no such constraint";
    ERROR_NAME[-OB_ERR_DOWNGRADE_DOP] = "OB_ERR_DOWNGRADE_DOP";
    MYSQL_ERRNO[-OB_ERR_DOWNGRADE_DOP] = -1;
    SQLSTATE[-OB_ERR_DOWNGRADE_DOP] = "HY000";
    STR_ERROR[-OB_ERR_DOWNGRADE_DOP] = "PX DOP downgrade";
    STR_USER_ERROR[-OB_ERR_DOWNGRADE_DOP] = "PX DOP downgrade from %ld to %ld";
    ORACLE_ERRNO[-OB_ERR_DOWNGRADE_DOP] = 0;
    ORACLE_STR_ERROR[-OB_ERR_DOWNGRADE_DOP] = "ORA-00000: PX DOP downgrade";
    ORACLE_STR_USER_ERROR[-OB_ERR_DOWNGRADE_DOP] = "ORA-00000: PX DOP downgrade from %ld to %ld";
    ERROR_NAME[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] = "OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS";
    MYSQL_ERRNO[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] = -1;
    SQLSTATE[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] = "HY000";
    STR_ERROR[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] =
        "parallel_max_servers downgrade due to insufficent cpu resource";
    STR_USER_ERROR[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] =
        "parallel_max_servers downgrade due to insufficent cpu resource from %ld to %ld";
    ORACLE_ERRNO[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] = 0;
    ORACLE_STR_ERROR[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] =
        "ORA-00000: parallel_max_servers downgrade due to insufficent cpu resource";
    ORACLE_STR_USER_ERROR[-OB_ERR_DOWNGRADE_PARALLEL_MAX_SERVERS] =
        "ORA-00000: parallel_max_servers downgrade due to insufficent cpu resource from %ld to %ld";
    ERROR_NAME[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] = "OB_ERR_ORPHANED_CHILD_RECORD_EXISTS";
    MYSQL_ERRNO[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] = -1;
    SQLSTATE[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] = "HY000";
    STR_ERROR[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] = "cannot validate - parent keys not found";
    STR_USER_ERROR[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] = "cannot validate (%.*s.%.*s) - parent keys not found";
    ORACLE_ERRNO[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] = 2298;
    ORACLE_STR_ERROR[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] = "ORA-02298: cannot validate - parent keys not found";
    ORACLE_STR_USER_ERROR[-OB_ERR_ORPHANED_CHILD_RECORD_EXISTS] =
        "ORA-02298: cannot validate (%.*s.%.*s) - parent keys not found";
    ERROR_NAME[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] = "OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL";
    MYSQL_ERRNO[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] = -1;
    SQLSTATE[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] = "HY000";
    STR_ERROR[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] = "Column check constraint cannot reference other columns";
    STR_USER_ERROR[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] = "Column check constraint cannot reference other columns";
    ORACLE_ERRNO[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] = 2438;
    ORACLE_STR_ERROR[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] =
        "ORA-02438: Column check constraint cannot reference other columns";
    ORACLE_STR_USER_ERROR[-OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL] =
        "ORA-02438: Column check constraint cannot reference other columns";
    ERROR_NAME[-OB_BATCHED_MULTI_STMT_ROLLBACK] = "OB_BATCHED_MULTI_STMT_ROLLBACK";
    MYSQL_ERRNO[-OB_BATCHED_MULTI_STMT_ROLLBACK] = -1;
    SQLSTATE[-OB_BATCHED_MULTI_STMT_ROLLBACK] = "HY000";
    STR_ERROR[-OB_BATCHED_MULTI_STMT_ROLLBACK] = "batched multi statement execution needs rollback";
    STR_USER_ERROR[-OB_BATCHED_MULTI_STMT_ROLLBACK] = "batched multi statement execution needs rollback";
    ORACLE_ERRNO[-OB_BATCHED_MULTI_STMT_ROLLBACK] = 600;
    ORACLE_STR_ERROR[-OB_BATCHED_MULTI_STMT_ROLLBACK] =
        "ORA-00600: internal error code, arguments: -5787, batched multi statement execution needs rollback";
    ORACLE_STR_USER_ERROR[-OB_BATCHED_MULTI_STMT_ROLLBACK] =
        "ORA-00600: internal error code, arguments: -5787, batched multi statement execution needs rollback";
    ERROR_NAME[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] = "OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT";
    MYSQL_ERRNO[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] = -1;
    SQLSTATE[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] = "HY000";
    STR_ERROR[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] =
        "cannot select FOR UPDATE from view with DISTINCT, GROUP BY, etc.";
    STR_USER_ERROR[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] =
        "cannot select FOR UPDATE from view with DISTINCT, GROUP BY, etc.";
    ORACLE_ERRNO[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] = 2014;
    ORACLE_STR_ERROR[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] =
        "ORA-02014: cannot select FOR UPDATE from view with DISTINCT, GROUP BY, etc.";
    ORACLE_STR_USER_ERROR[-OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT] =
        "ORA-02014: cannot select FOR UPDATE from view with DISTINCT, GROUP BY, etc.";
    ERROR_NAME[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = "OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION";
    MYSQL_ERRNO[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = -1;
    SQLSTATE[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = "HY000";
    STR_ERROR[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = "policy with check option violation";
    STR_USER_ERROR[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = "policy with check option violation";
    ORACLE_ERRNO[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = 28115;
    ORACLE_STR_ERROR[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = "ORA-28115: policy with check option violation";
    ORACLE_STR_USER_ERROR[-OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION] = "ORA-28115: policy with check option violation";
    ERROR_NAME[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = "OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE";
    MYSQL_ERRNO[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = -1;
    SQLSTATE[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = "policy already applied to table";
    STR_USER_ERROR[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = "policy already applied to table";
    ORACLE_ERRNO[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = 12444;
    ORACLE_STR_ERROR[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = "ORA-12444: policy already applied to table";
    ORACLE_STR_USER_ERROR[-OB_ERR_POLICY_ALREADY_APPLIED_TO_TABLE] = "ORA-12444: policy already applied to table";
    ERROR_NAME[-OB_ERR_MUTATING_TABLE_OPERATION] = "OB_ERR_MUTATING_TABLE_OPERATION";
    MYSQL_ERRNO[-OB_ERR_MUTATING_TABLE_OPERATION] = -1;
    SQLSTATE[-OB_ERR_MUTATING_TABLE_OPERATION] = "HY000";
    STR_ERROR[-OB_ERR_MUTATING_TABLE_OPERATION] = "table is mutating, trigger/function may not see it";
    STR_USER_ERROR[-OB_ERR_MUTATING_TABLE_OPERATION] =
        "table '%.*s'.'%.*s' is mutating, trigger/function may not see it";
    ORACLE_ERRNO[-OB_ERR_MUTATING_TABLE_OPERATION] = 4091;
    ORACLE_STR_ERROR[-OB_ERR_MUTATING_TABLE_OPERATION] =
        "ORA-04091: table is mutating, trigger/function may not see it";
    ORACLE_STR_USER_ERROR[-OB_ERR_MUTATING_TABLE_OPERATION] =
        "ORA-04091: table '%.*s'.'%.*s' is mutating, trigger/function may not see it";
    ERROR_NAME[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] = "OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT";
    MYSQL_ERRNO[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] = -1;
    SQLSTATE[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] = "column is referenced in a multi-column constraint";
    STR_USER_ERROR[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] =
        "column is referenced in a multi-column constraint";
    ORACLE_ERRNO[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] = 12991;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] =
        "ORA-12991: column is referenced in a multi-column constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT] =
        "ORA-12991: column is referenced in a multi-column constraint";
    ERROR_NAME[-OB_ERR_DROP_PARENT_KEY_COLUMN] = "OB_ERR_DROP_PARENT_KEY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_DROP_PARENT_KEY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_DROP_PARENT_KEY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_DROP_PARENT_KEY_COLUMN] = "cannot drop parent key column";
    STR_USER_ERROR[-OB_ERR_DROP_PARENT_KEY_COLUMN] = "cannot drop parent key column";
    ORACLE_ERRNO[-OB_ERR_DROP_PARENT_KEY_COLUMN] = 12992;
    ORACLE_STR_ERROR[-OB_ERR_DROP_PARENT_KEY_COLUMN] = "ORA-12992: cannot drop parent key column";
    ORACLE_STR_USER_ERROR[-OB_ERR_DROP_PARENT_KEY_COLUMN] = "ORA-12992: cannot drop parent key column";
    ERROR_NAME[-OB_AUTOINC_SERVICE_BUSY] = "OB_AUTOINC_SERVICE_BUSY";
    MYSQL_ERRNO[-OB_AUTOINC_SERVICE_BUSY] = -1;
    SQLSTATE[-OB_AUTOINC_SERVICE_BUSY] = "HY000";
    STR_ERROR[-OB_AUTOINC_SERVICE_BUSY] = "auto increment service busy";
    STR_USER_ERROR[-OB_AUTOINC_SERVICE_BUSY] = "auto increment service busy";
    ORACLE_ERRNO[-OB_AUTOINC_SERVICE_BUSY] = 600;
    ORACLE_STR_ERROR[-OB_AUTOINC_SERVICE_BUSY] = "ORA-00600: auto increment service busy";
    ORACLE_STR_USER_ERROR[-OB_AUTOINC_SERVICE_BUSY] = "ORA-00600: auto increment service busy";
    ERROR_NAME[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] = "OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE";
    MYSQL_ERRNO[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] = -1;
    SQLSTATE[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] = "HY000";
    STR_ERROR[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] =
        "No insert/update/delete on table with constraint disabled and validated";
    STR_USER_ERROR[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] =
        "No insert/update/delete on table with constraint (%.*s.%.*s) disabled and validated";
    ORACLE_ERRNO[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] = 25128;
    ORACLE_STR_ERROR[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] =
        "ORA-25128: No insert/update/delete on table with constraint disabled and validated";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE] =
        "ORA-25128: No insert/update/delete on table with constraint (%.*s.%.*s) disabled and validated";
    ERROR_NAME[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] = "OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK";
    MYSQL_ERRNO[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] = -1;
    SQLSTATE[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] = "HY000";
    STR_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] = "active autonomous transaction detected and rolled back";
    STR_USER_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] = "active autonomous transaction detected and rolled back";
    ORACLE_ERRNO[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] = 6519;
    ORACLE_STR_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] =
        "ORA-06519: active autonomous transaction detected and rolled back";
    ORACLE_STR_USER_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ROLLBACK] =
        "ORA-06519: active autonomous transaction detected and rolled back";
    ERROR_NAME[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = "OB_ORDERBY_CLAUSE_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = "ORDER BY not allowed here";
    STR_USER_ERROR[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = "ORDER BY not allowed here";
    ORACLE_ERRNO[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = 30487;
    ORACLE_STR_ERROR[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = "ORA-30487: ORDER BY not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ORDERBY_CLAUSE_NOT_ALLOWED] = "ORA-30487: ORDER BY not allowed here";
    ERROR_NAME[-OB_DISTINCT_NOT_ALLOWED] = "OB_DISTINCT_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_DISTINCT_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_DISTINCT_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_DISTINCT_NOT_ALLOWED] = "DISTINCT not allowed here";
    STR_USER_ERROR[-OB_DISTINCT_NOT_ALLOWED] = "DISTINCT not allowed here";
    ORACLE_ERRNO[-OB_DISTINCT_NOT_ALLOWED] = 30482;
    ORACLE_STR_ERROR[-OB_DISTINCT_NOT_ALLOWED] = "ORA-30482: DISTINCT not allowed here";
    ORACLE_STR_USER_ERROR[-OB_DISTINCT_NOT_ALLOWED] = "ORA-30482: DISTINCT not allowed here";
    ERROR_NAME[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] = "OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] =
        "assign user variable with := only allowed in select filed list and as root expression";
    STR_USER_ERROR[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] =
        "assign user variable with := only allowed in select filed list and as root expression";
    ORACLE_ERRNO[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -5799, assign user variable with := only allowed in select filed "
        "list and as root expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_ASSIGN_USER_VARIABLE_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -5799, assign user variable with := only allowed in select filed "
        "list and as root expression";
    ERROR_NAME[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] = "OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT";
    MYSQL_ERRNO[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] = -1;
    SQLSTATE[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] = "cannot modify constraint - no such constraint";
    STR_USER_ERROR[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] = "cannot modify constraint (%.*s) - no such constraint";
    ORACLE_ERRNO[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] = 25129;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] =
        "ORA-25129: cannot modify constraint - no such constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT] =
        "ORA-25129: cannot modify constraint (%.*s) - no such constraint";
    ERROR_NAME[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] = "OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] = -1;
    SQLSTATE[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] = "HY000";
    STR_ERROR[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] =
        "implementation restriction: exception handler in nested transaction is illegal";
    STR_USER_ERROR[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] =
        "implementation restriction: exception handler in nested transaction is illegal";
    ORACLE_ERRNO[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] =
        "ORA-00600: internal error code, arguments: -5801, implementation restriction: exception handler in nested "
        "transaction is illegal";
    ORACLE_STR_USER_ERROR[-OB_ERR_SP_EXCEPTION_HANDLE_ILLEGAL] =
        "ORA-00600: internal error code, arguments: -5801, implementation restriction: exception handler in nested "
        "transaction is illegal";
    ERROR_NAME[-OB_INVALID_ROWID] = "OB_INVALID_ROWID";
    MYSQL_ERRNO[-OB_INVALID_ROWID] = -1;
    SQLSTATE[-OB_INVALID_ROWID] = "HY000";
    STR_ERROR[-OB_INVALID_ROWID] = "invalid ROWID";
    STR_USER_ERROR[-OB_INVALID_ROWID] = "invalid ROWID";
    ORACLE_ERRNO[-OB_INVALID_ROWID] = 1410;
    ORACLE_STR_ERROR[-OB_INVALID_ROWID] = "ORA-01410: invalid ROWID";
    ORACLE_STR_USER_ERROR[-OB_INVALID_ROWID] = "ORA-01410: invalid ROWID";
    ERROR_NAME[-OB_ERR_INVALID_INSERT_COLUMN] = "OB_ERR_INVALID_INSERT_COLUMN";
    MYSQL_ERRNO[-OB_ERR_INVALID_INSERT_COLUMN] = -1;
    SQLSTATE[-OB_ERR_INVALID_INSERT_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_INSERT_COLUMN] = "Invalid column in the INSERT VALUES Clause";
    STR_USER_ERROR[-OB_ERR_INVALID_INSERT_COLUMN] = "Invalid column in the INSERT VALUES Clause:'%.*s'.'%.*s'";
    ORACLE_ERRNO[-OB_ERR_INVALID_INSERT_COLUMN] = 38101;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_INSERT_COLUMN] = "ORA-38101: Invalid column in the INSERT VALUES Clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_INSERT_COLUMN] =
        "ORA-38101: Invalid column in the INSERT VALUES Clause:'%.*s'.'%.*s'";
    ERROR_NAME[-OB_INCORRECT_USE_OF_OPERATOR] = "OB_INCORRECT_USE_OF_OPERATOR";
    MYSQL_ERRNO[-OB_INCORRECT_USE_OF_OPERATOR] = -1;
    SQLSTATE[-OB_INCORRECT_USE_OF_OPERATOR] = "HY000";
    STR_ERROR[-OB_INCORRECT_USE_OF_OPERATOR] = "incorrect use of operator";
    STR_USER_ERROR[-OB_INCORRECT_USE_OF_OPERATOR] = "incorrect use of the ['%.*s'] operator";
    ORACLE_ERRNO[-OB_INCORRECT_USE_OF_OPERATOR] = 13207;
    ORACLE_STR_ERROR[-OB_INCORRECT_USE_OF_OPERATOR] = "ORA-13207: incorrect use of operator";
    ORACLE_STR_USER_ERROR[-OB_INCORRECT_USE_OF_OPERATOR] = "ORA-13207: incorrect use of the ['%.*s'] operator";
    ERROR_NAME[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] =
        "OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES";
    MYSQL_ERRNO[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] = -1;
    SQLSTATE[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] = "HY000";
    STR_ERROR[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] =
        "non-constant expression is not allowed for pivot|unpivot values";
    STR_USER_ERROR[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] =
        "non-constant expression is not allowed for pivot|unpivot values";
    ORACLE_ERRNO[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] = 56901;
    ORACLE_STR_ERROR[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] =
        "ORA-56901: non-constant expression is not allowed for pivot|unpivot values";
    ORACLE_STR_USER_ERROR[-OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES] =
        "ORA-56901: non-constant expression is not allowed for pivot|unpivot values";
    ERROR_NAME[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] =
        "OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION";
    MYSQL_ERRNO[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] = -1;
    SQLSTATE[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] = "HY000";
    STR_ERROR[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] =
        "expect aggregate function inside pivot operation";
    STR_USER_ERROR[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] =
        "expect aggregate function inside pivot operation";
    ORACLE_ERRNO[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] = 56902;
    ORACLE_STR_ERROR[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] =
        "ORA-56902: expect aggregate function inside pivot operation";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION] =
        "ORA-56902: expect aggregate function inside pivot operation";
    ERROR_NAME[-OB_ERR_EXP_NEED_SAME_DATATYPE] = "OB_ERR_EXP_NEED_SAME_DATATYPE";
    MYSQL_ERRNO[-OB_ERR_EXP_NEED_SAME_DATATYPE] = -1;
    SQLSTATE[-OB_ERR_EXP_NEED_SAME_DATATYPE] = "HY000";
    STR_ERROR[-OB_ERR_EXP_NEED_SAME_DATATYPE] = "expression must have same datatype as corresponding expression";
    STR_USER_ERROR[-OB_ERR_EXP_NEED_SAME_DATATYPE] = "expression must have same datatype as corresponding expression";
    ORACLE_ERRNO[-OB_ERR_EXP_NEED_SAME_DATATYPE] = 1790;
    ORACLE_STR_ERROR[-OB_ERR_EXP_NEED_SAME_DATATYPE] =
        "ORA-01790: expression must have same datatype as corresponding expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXP_NEED_SAME_DATATYPE] =
        "ORA-01790: expression must have same datatype as corresponding expression";
    ERROR_NAME[-OB_ERR_CHARACTER_SET_MISMATCH] = "OB_ERR_CHARACTER_SET_MISMATCH";
    MYSQL_ERRNO[-OB_ERR_CHARACTER_SET_MISMATCH] = -1;
    SQLSTATE[-OB_ERR_CHARACTER_SET_MISMATCH] = "HY000";
    STR_ERROR[-OB_ERR_CHARACTER_SET_MISMATCH] = "character set mismatch";
    STR_USER_ERROR[-OB_ERR_CHARACTER_SET_MISMATCH] = "character set mismatch";
    ORACLE_ERRNO[-OB_ERR_CHARACTER_SET_MISMATCH] = 12704;
    ORACLE_STR_ERROR[-OB_ERR_CHARACTER_SET_MISMATCH] = "ORA-12704: character set mismatch";
    ORACLE_STR_USER_ERROR[-OB_ERR_CHARACTER_SET_MISMATCH] = "ORA-12704: character set mismatch";
    ERROR_NAME[-OB_ERR_REGEXP_NOMATCH] = "OB_ERR_REGEXP_NOMATCH";
    MYSQL_ERRNO[-OB_ERR_REGEXP_NOMATCH] = -1;
    SQLSTATE[-OB_ERR_REGEXP_NOMATCH] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_NOMATCH] = "regular expression failed to match";
    STR_USER_ERROR[-OB_ERR_REGEXP_NOMATCH] = "regular expression failed to match";
    ORACLE_ERRNO[-OB_ERR_REGEXP_NOMATCH] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_NOMATCH] =
        "ORA-00600: internal error code, arguments: -5809, regular expression failed to match";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_NOMATCH] =
        "ORA-00600: internal error code, arguments: -5809, regular expression failed to match";
    ERROR_NAME[-OB_ERR_REGEXP_BADPAT] = "OB_ERR_REGEXP_BADPAT";
    MYSQL_ERRNO[-OB_ERR_REGEXP_BADPAT] = -1;
    SQLSTATE[-OB_ERR_REGEXP_BADPAT] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_BADPAT] = "invalid regular expression (reg version 0.8)";
    STR_USER_ERROR[-OB_ERR_REGEXP_BADPAT] = "invalid regular expression (reg version 0.8)";
    ORACLE_ERRNO[-OB_ERR_REGEXP_BADPAT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_BADPAT] =
        "ORA-00600: internal error code, arguments: -5810, invalid regular expression (reg version 0.8)";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_BADPAT] =
        "ORA-00600: internal error code, arguments: -5810, invalid regular expression (reg version 0.8)";
    ERROR_NAME[-OB_ERR_REGEXP_EESCAPE] = "OB_ERR_REGEXP_EESCAPE";
    MYSQL_ERRNO[-OB_ERR_REGEXP_EESCAPE] = -1;
    SQLSTATE[-OB_ERR_REGEXP_EESCAPE] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_EESCAPE] = "invalid escape \\ sequence in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_EESCAPE] = "invalid escape \\ sequence in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_EESCAPE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_EESCAPE] =
        "ORA-00600: internal error code, arguments: -5811, invalid escape \\ sequence in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_EESCAPE] =
        "ORA-00600: internal error code, arguments: -5811, invalid escape \\ sequence in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_EBRACK] = "OB_ERR_REGEXP_EBRACK";
    MYSQL_ERRNO[-OB_ERR_REGEXP_EBRACK] = -1;
    SQLSTATE[-OB_ERR_REGEXP_EBRACK] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_EBRACK] = "unmatched bracket in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_EBRACK] = "unmatched bracket in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_EBRACK] = 12726;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_EBRACK] = "ORA-12726: unmatched bracket in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_EBRACK] = "ORA-12726: unmatched bracket in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_EPAREN] = "OB_ERR_REGEXP_EPAREN";
    MYSQL_ERRNO[-OB_ERR_REGEXP_EPAREN] = -1;
    SQLSTATE[-OB_ERR_REGEXP_EPAREN] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_EPAREN] = "unmatched parentheses in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_EPAREN] = "unmatched parentheses in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_EPAREN] = 12725;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_EPAREN] = "ORA-12725: unmatched parentheses in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_EPAREN] = "ORA-12725: unmatched parentheses in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_ESUBREG] = "OB_ERR_REGEXP_ESUBREG";
    MYSQL_ERRNO[-OB_ERR_REGEXP_ESUBREG] = -1;
    SQLSTATE[-OB_ERR_REGEXP_ESUBREG] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_ESUBREG] = "invalid back reference in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_ESUBREG] = "invalid back reference in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_ESUBREG] = 12727;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_ESUBREG] = "ORA-12727: invalid back reference in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_ESUBREG] = "ORA-12727: invalid back reference in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_ERANGE] = "OB_ERR_REGEXP_ERANGE";
    MYSQL_ERRNO[-OB_ERR_REGEXP_ERANGE] = -1;
    SQLSTATE[-OB_ERR_REGEXP_ERANGE] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_ERANGE] = "invalid range in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_ERANGE] = "invalid range in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_ERANGE] = 12728;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_ERANGE] = "ORA-12728: invalid range in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_ERANGE] = "ORA-12728: invalid range in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_ECTYPE] = "OB_ERR_REGEXP_ECTYPE";
    MYSQL_ERRNO[-OB_ERR_REGEXP_ECTYPE] = -1;
    SQLSTATE[-OB_ERR_REGEXP_ECTYPE] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_ECTYPE] = "invalid character class in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_ECTYPE] = "invalid character class in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_ECTYPE] = 12729;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_ECTYPE] = "ORA-12729: invalid character class in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_ECTYPE] = "ORA-12729: invalid character class in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_ECOLLATE] = "OB_ERR_REGEXP_ECOLLATE";
    MYSQL_ERRNO[-OB_ERR_REGEXP_ECOLLATE] = -1;
    SQLSTATE[-OB_ERR_REGEXP_ECOLLATE] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_ECOLLATE] = "invalid collation class in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_ECOLLATE] = "invalid collation class in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_ECOLLATE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_ECOLLATE] =
        "ORA-00600: internal error code, arguments: -5817, invalid collation class in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_ECOLLATE] =
        "ORA-00600: internal error code, arguments: -5817, invalid collation class in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_EBRACE] = "OB_ERR_REGEXP_EBRACE";
    MYSQL_ERRNO[-OB_ERR_REGEXP_EBRACE] = -1;
    SQLSTATE[-OB_ERR_REGEXP_EBRACE] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_EBRACE] = "braces {} not balanced in in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_EBRACE] = "braces {} not balanced in in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_EBRACE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_EBRACE] =
        "ORA-00600: internal error code, arguments: -5818, braces {} not balanced in in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_EBRACE] =
        "ORA-00600: internal error code, arguments: -5818, braces {} not balanced in in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_BADBR] = "OB_ERR_REGEXP_BADBR";
    MYSQL_ERRNO[-OB_ERR_REGEXP_BADBR] = -1;
    SQLSTATE[-OB_ERR_REGEXP_BADBR] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_BADBR] = "invalid repetition count(s) in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_BADBR] = "invalid repetition count(s) in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_BADBR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_BADBR] =
        "ORA-00600: internal error code, arguments: -5819, invalid repetition count(s) in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_BADBR] =
        "ORA-00600: internal error code, arguments: -5819, invalid repetition count(s) in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_BADRPT] = "OB_ERR_REGEXP_BADRPT";
    MYSQL_ERRNO[-OB_ERR_REGEXP_BADRPT] = -1;
    SQLSTATE[-OB_ERR_REGEXP_BADRPT] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_BADRPT] = "The regular expression was too complex and current library can't be parsed";
    STR_USER_ERROR[-OB_ERR_REGEXP_BADRPT] =
        "The regular expression was too complex and current library can't be parsed";
    ORACLE_ERRNO[-OB_ERR_REGEXP_BADRPT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_BADRPT] = "ORA-00600: internal error code, arguments: -5820, The regular "
                                              "expression was too complex and current library can't be parsed";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_BADRPT] = "ORA-00600: internal error code, arguments: -5820, The regular "
                                                   "expression was too complex and current library can't be parsed";
    ERROR_NAME[-OB_ERR_REGEXP_ASSERT] = "OB_ERR_REGEXP_ASSERT";
    MYSQL_ERRNO[-OB_ERR_REGEXP_ASSERT] = -1;
    SQLSTATE[-OB_ERR_REGEXP_ASSERT] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_ASSERT] = "regular expression internal error";
    STR_USER_ERROR[-OB_ERR_REGEXP_ASSERT] = "regular expression internal error";
    ORACLE_ERRNO[-OB_ERR_REGEXP_ASSERT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_ASSERT] =
        "ORA-00600: internal error code, arguments: -5821, regular expression internal error";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_ASSERT] =
        "ORA-00600: internal error code, arguments: -5821, regular expression internal error";
    ERROR_NAME[-OB_ERR_REGEXP_INVARG] = "OB_ERR_REGEXP_INVARG";
    MYSQL_ERRNO[-OB_ERR_REGEXP_INVARG] = -1;
    SQLSTATE[-OB_ERR_REGEXP_INVARG] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_INVARG] = "invalid argument in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_INVARG] = "invalid argument in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_INVARG] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_INVARG] =
        "ORA-00600: internal error code, arguments: -5822, invalid argument in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_INVARG] =
        "ORA-00600: internal error code, arguments: -5822, invalid argument in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_MIXED] = "OB_ERR_REGEXP_MIXED";
    MYSQL_ERRNO[-OB_ERR_REGEXP_MIXED] = -1;
    SQLSTATE[-OB_ERR_REGEXP_MIXED] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_MIXED] = "character widths of regex and string differ in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_MIXED] = "character widths of regex and string differ in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_MIXED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_MIXED] = "ORA-00600: internal error code, arguments: -5823, character widths of "
                                             "regex and string differ in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_MIXED] = "ORA-00600: internal error code, arguments: -5823, character widths "
                                                  "of regex and string differ in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_BADOPT] = "OB_ERR_REGEXP_BADOPT";
    MYSQL_ERRNO[-OB_ERR_REGEXP_BADOPT] = -1;
    SQLSTATE[-OB_ERR_REGEXP_BADOPT] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_BADOPT] = "invalid embedded option in regular expression";
    STR_USER_ERROR[-OB_ERR_REGEXP_BADOPT] = "invalid embedded option in regular expression";
    ORACLE_ERRNO[-OB_ERR_REGEXP_BADOPT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_BADOPT] =
        "ORA-00600: internal error code, arguments: -5824, invalid embedded option in regular expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_BADOPT] =
        "ORA-00600: internal error code, arguments: -5824, invalid embedded option in regular expression";
    ERROR_NAME[-OB_ERR_REGEXP_ETOOBIG] = "OB_ERR_REGEXP_ETOOBIG";
    MYSQL_ERRNO[-OB_ERR_REGEXP_ETOOBIG] = -1;
    SQLSTATE[-OB_ERR_REGEXP_ETOOBIG] = "HY000";
    STR_ERROR[-OB_ERR_REGEXP_ETOOBIG] =
        "nfa has too many states in regular expression, may be the regular expression too long";
    STR_USER_ERROR[-OB_ERR_REGEXP_ETOOBIG] =
        "nfa has too many states in regular expression, may be the regular expression too long";
    ORACLE_ERRNO[-OB_ERR_REGEXP_ETOOBIG] = 600;
    ORACLE_STR_ERROR[-OB_ERR_REGEXP_ETOOBIG] = "ORA-00600: internal error code, arguments: -5825, nfa has too many "
                                               "states in regular expression, may be the regular expression too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_REGEXP_ETOOBIG] =
        "ORA-00600: internal error code, arguments: -5825, nfa has too many states in regular expression, may be the "
        "regular expression too long";
    ERROR_NAME[-OB_NOT_SUPPORTED_ROWID_TYPE] = "OB_NOT_SUPPORTED_ROWID_TYPE";
    MYSQL_ERRNO[-OB_NOT_SUPPORTED_ROWID_TYPE] = -1;
    SQLSTATE[-OB_NOT_SUPPORTED_ROWID_TYPE] = "HY000";
    STR_ERROR[-OB_NOT_SUPPORTED_ROWID_TYPE] = "ROWID for tables without primary key is not implemented";
    STR_USER_ERROR[-OB_NOT_SUPPORTED_ROWID_TYPE] = "ROWID for tables without primary key is not implemented";
    ORACLE_ERRNO[-OB_NOT_SUPPORTED_ROWID_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_NOT_SUPPORTED_ROWID_TYPE] =
        "ORA-00600: internal error code, arguments: -5826, ROWID for tables without primary key is not implemented";
    ORACLE_STR_USER_ERROR[-OB_NOT_SUPPORTED_ROWID_TYPE] =
        "ORA-00600: internal error code, arguments: -5826, ROWID for tables without primary key is not implemented";
    ERROR_NAME[-OB_ERR_PARALLEL_DDL_CONFLICT] = "OB_ERR_PARALLEL_DDL_CONFLICT";
    MYSQL_ERRNO[-OB_ERR_PARALLEL_DDL_CONFLICT] = -1;
    SQLSTATE[-OB_ERR_PARALLEL_DDL_CONFLICT] = "HY000";
    STR_ERROR[-OB_ERR_PARALLEL_DDL_CONFLICT] =
        "the definition of relative objects have been modified, please check and retry";
    STR_USER_ERROR[-OB_ERR_PARALLEL_DDL_CONFLICT] =
        "the definition of relative objects have been modified, please check and retry";
    ORACLE_ERRNO[-OB_ERR_PARALLEL_DDL_CONFLICT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_PARALLEL_DDL_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5827, the definition of relative objects have been modified, "
        "please check and retry";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARALLEL_DDL_CONFLICT] =
        "ORA-00600: internal error code, arguments: -5827, the definition of relative objects have been modified, "
        "please check and retry";
    ERROR_NAME[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = "OB_ERR_SUBSCRIPT_BEYOND_COUNT";
    MYSQL_ERRNO[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = -1;
    SQLSTATE[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = "HY000";
    STR_ERROR[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = "Subscript beyond count";
    STR_USER_ERROR[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = "Subscript beyond count";
    ORACLE_ERRNO[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = 6533;
    ORACLE_STR_ERROR[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = "ORA-06533: Subscript beyond count";
    ORACLE_STR_USER_ERROR[-OB_ERR_SUBSCRIPT_BEYOND_COUNT] = "ORA-06533: Subscript beyond count";
    ERROR_NAME[-OB_ERR_NOT_PARTITIONED] = "OB_ERR_NOT_PARTITIONED";
    MYSQL_ERRNO[-OB_ERR_NOT_PARTITIONED] = -1;
    SQLSTATE[-OB_ERR_NOT_PARTITIONED] = "HY000";
    STR_ERROR[-OB_ERR_NOT_PARTITIONED] = "PARTITION () clause on non partitioned table";
    STR_USER_ERROR[-OB_ERR_NOT_PARTITIONED] = "PARTITION () clause on non partitioned table";
    ORACLE_ERRNO[-OB_ERR_NOT_PARTITIONED] = 14501;
    ORACLE_STR_ERROR[-OB_ERR_NOT_PARTITIONED] = "ORA-14501: object is not partitioned";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_PARTITIONED] = "ORA-14501: object is not partitioned";
    ERROR_NAME[-OB_UNKNOWN_SUBPARTITION] = "OB_UNKNOWN_SUBPARTITION";
    MYSQL_ERRNO[-OB_UNKNOWN_SUBPARTITION] = -1;
    SQLSTATE[-OB_UNKNOWN_SUBPARTITION] = "HY000";
    STR_ERROR[-OB_UNKNOWN_SUBPARTITION] = "Unknown subpartition";
    STR_USER_ERROR[-OB_UNKNOWN_SUBPARTITION] = "Unknown subpartition";
    ORACLE_ERRNO[-OB_UNKNOWN_SUBPARTITION] = 14251;
    ORACLE_STR_ERROR[-OB_UNKNOWN_SUBPARTITION] = "ORA-14251: Specified subpartition does not exist";
    ORACLE_STR_USER_ERROR[-OB_UNKNOWN_SUBPARTITION] = "ORA-14251: Specified subpartition does not exist";
    ERROR_NAME[-OB_ERR_INVALID_SQL_ROW_LIMITING] = "OB_ERR_INVALID_SQL_ROW_LIMITING";
    MYSQL_ERRNO[-OB_ERR_INVALID_SQL_ROW_LIMITING] = -1;
    SQLSTATE[-OB_ERR_INVALID_SQL_ROW_LIMITING] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_SQL_ROW_LIMITING] = "Invalid SQL ROW LIMITING expression was specified.";
    STR_USER_ERROR[-OB_ERR_INVALID_SQL_ROW_LIMITING] = "Invalid SQL ROW LIMITING expression was specified.";
    ORACLE_ERRNO[-OB_ERR_INVALID_SQL_ROW_LIMITING] = 62550;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_SQL_ROW_LIMITING] =
        "ORA-62550: Invalid SQL ROW LIMITING expression was specified.";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_SQL_ROW_LIMITING] =
        "ORA-62550: Invalid SQL ROW LIMITING expression was specified.";
    ERROR_NAME[-INCORRECT_ARGUMENTS_TO_ESCAPE] = "INCORRECT_ARGUMENTS_TO_ESCAPE";
    MYSQL_ERRNO[-INCORRECT_ARGUMENTS_TO_ESCAPE] = -1210;
    SQLSTATE[-INCORRECT_ARGUMENTS_TO_ESCAPE] = "HY000";
    STR_ERROR[-INCORRECT_ARGUMENTS_TO_ESCAPE] = "Incorrect arguments to ESCAPE";
    STR_USER_ERROR[-INCORRECT_ARGUMENTS_TO_ESCAPE] = "Incorrect arguments to ESCAPE";
    ORACLE_ERRNO[-INCORRECT_ARGUMENTS_TO_ESCAPE] = 600;
    ORACLE_STR_ERROR[-INCORRECT_ARGUMENTS_TO_ESCAPE] =
        "ORA-00600: internal error code, arguments: -5832, Incorrect arguments to ESCAPE";
    ORACLE_STR_USER_ERROR[-INCORRECT_ARGUMENTS_TO_ESCAPE] =
        "ORA-00600: internal error code, arguments: -5832, Incorrect arguments to ESCAPE";
    ERROR_NAME[-STATIC_ENG_NOT_IMPLEMENT] = "STATIC_ENG_NOT_IMPLEMENT";
    MYSQL_ERRNO[-STATIC_ENG_NOT_IMPLEMENT] = -1;
    SQLSTATE[-STATIC_ENG_NOT_IMPLEMENT] = "HY000";
    STR_ERROR[-STATIC_ENG_NOT_IMPLEMENT] =
        "not implemented in SQL static typing engine, will try the old engine automatically";
    STR_USER_ERROR[-STATIC_ENG_NOT_IMPLEMENT] =
        "not implemented in SQL static typing engine, will try the old engine automatically";
    ORACLE_ERRNO[-STATIC_ENG_NOT_IMPLEMENT] = 600;
    ORACLE_STR_ERROR[-STATIC_ENG_NOT_IMPLEMENT] = "ORA-00600: internal error code, arguments: -5833, not implemented "
                                                  "in SQL static typing engine, will try the old engine automatically";
    ORACLE_STR_USER_ERROR[-STATIC_ENG_NOT_IMPLEMENT] =
        "ORA-00600: internal error code, arguments: -5833, not implemented in SQL static typing engine, will try the "
        "old engine automatically";
    ERROR_NAME[-OB_OBJ_ALREADY_EXIST] = "OB_OBJ_ALREADY_EXIST";
    MYSQL_ERRNO[-OB_OBJ_ALREADY_EXIST] = -1;
    SQLSTATE[-OB_OBJ_ALREADY_EXIST] = "HY000";
    STR_ERROR[-OB_OBJ_ALREADY_EXIST] = "name is already used by an existing object";
    STR_USER_ERROR[-OB_OBJ_ALREADY_EXIST] = "name is already used by an existing object";
    ORACLE_ERRNO[-OB_OBJ_ALREADY_EXIST] = 955;
    ORACLE_STR_ERROR[-OB_OBJ_ALREADY_EXIST] = "ORA-00955: name is already used by an existing object";
    ORACLE_STR_USER_ERROR[-OB_OBJ_ALREADY_EXIST] = "ORA-00955: name is already used by an existing object";
    ERROR_NAME[-OB_DBLINK_NOT_EXIST_TO_ACCESS] = "OB_DBLINK_NOT_EXIST_TO_ACCESS";
    MYSQL_ERRNO[-OB_DBLINK_NOT_EXIST_TO_ACCESS] = -1;
    SQLSTATE[-OB_DBLINK_NOT_EXIST_TO_ACCESS] = "HY000";
    STR_ERROR[-OB_DBLINK_NOT_EXIST_TO_ACCESS] = "connection description for remote database not found";
    STR_USER_ERROR[-OB_DBLINK_NOT_EXIST_TO_ACCESS] = "connection description for remote database not found";
    ORACLE_ERRNO[-OB_DBLINK_NOT_EXIST_TO_ACCESS] = 2019;
    ORACLE_STR_ERROR[-OB_DBLINK_NOT_EXIST_TO_ACCESS] =
        "ORA-02019: connection description for remote database not found";
    ORACLE_STR_USER_ERROR[-OB_DBLINK_NOT_EXIST_TO_ACCESS] =
        "ORA-02019: connection description for remote database not found";
    ERROR_NAME[-OB_DBLINK_NOT_EXIST_TO_DROP] = "OB_DBLINK_NOT_EXIST_TO_DROP";
    MYSQL_ERRNO[-OB_DBLINK_NOT_EXIST_TO_DROP] = -1;
    SQLSTATE[-OB_DBLINK_NOT_EXIST_TO_DROP] = "HY000";
    STR_ERROR[-OB_DBLINK_NOT_EXIST_TO_DROP] = "database link not found";
    STR_USER_ERROR[-OB_DBLINK_NOT_EXIST_TO_DROP] = "database link not found";
    ORACLE_ERRNO[-OB_DBLINK_NOT_EXIST_TO_DROP] = 2024;
    ORACLE_STR_ERROR[-OB_DBLINK_NOT_EXIST_TO_DROP] = "ORA-02024: database link not found";
    ORACLE_STR_USER_ERROR[-OB_DBLINK_NOT_EXIST_TO_DROP] = "ORA-02024: database link not found";
    ERROR_NAME[-OB_ERR_ACCESS_INTO_NULL] = "OB_ERR_ACCESS_INTO_NULL";
    MYSQL_ERRNO[-OB_ERR_ACCESS_INTO_NULL] = -1;
    SQLSTATE[-OB_ERR_ACCESS_INTO_NULL] = "HY000";
    STR_ERROR[-OB_ERR_ACCESS_INTO_NULL] = "Reference to uninitialized composite";
    STR_USER_ERROR[-OB_ERR_ACCESS_INTO_NULL] = "Reference to uninitialized composite";
    ORACLE_ERRNO[-OB_ERR_ACCESS_INTO_NULL] = 6530;
    ORACLE_STR_ERROR[-OB_ERR_ACCESS_INTO_NULL] = "ORA-06530: Reference to uninitialized composite";
    ORACLE_STR_USER_ERROR[-OB_ERR_ACCESS_INTO_NULL] = "ORA-06530: Reference to uninitialized composite";
    ERROR_NAME[-OB_ERR_COLLECION_NULL] = "OB_ERR_COLLECION_NULL";
    MYSQL_ERRNO[-OB_ERR_COLLECION_NULL] = -1;
    SQLSTATE[-OB_ERR_COLLECION_NULL] = "HY000";
    STR_ERROR[-OB_ERR_COLLECION_NULL] = "Reference to uninitialized collection";
    STR_USER_ERROR[-OB_ERR_COLLECION_NULL] = "Reference to uninitialized collection";
    ORACLE_ERRNO[-OB_ERR_COLLECION_NULL] = 6531;
    ORACLE_STR_ERROR[-OB_ERR_COLLECION_NULL] = "ORA-06531: Reference to uninitialized collection";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLLECION_NULL] = "ORA-06531: Reference to uninitialized collection";
    ERROR_NAME[-OB_ERR_NO_DATA_NEEDED] = "OB_ERR_NO_DATA_NEEDED";
    MYSQL_ERRNO[-OB_ERR_NO_DATA_NEEDED] = -1;
    SQLSTATE[-OB_ERR_NO_DATA_NEEDED] = "HY000";
    STR_ERROR[-OB_ERR_NO_DATA_NEEDED] = "no more rows needed";
    STR_USER_ERROR[-OB_ERR_NO_DATA_NEEDED] = "no more rows needed";
    ORACLE_ERRNO[-OB_ERR_NO_DATA_NEEDED] = 6548;
    ORACLE_STR_ERROR[-OB_ERR_NO_DATA_NEEDED] = "ORA-06548: no more rows needed";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_DATA_NEEDED] = "ORA-06548: no more rows needed";
    ERROR_NAME[-OB_ERR_PROGRAM_ERROR] = "OB_ERR_PROGRAM_ERROR";
    MYSQL_ERRNO[-OB_ERR_PROGRAM_ERROR] = -1;
    SQLSTATE[-OB_ERR_PROGRAM_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_PROGRAM_ERROR] = "PL/SQL: program error";
    STR_USER_ERROR[-OB_ERR_PROGRAM_ERROR] = "PL/SQL: program error";
    ORACLE_ERRNO[-OB_ERR_PROGRAM_ERROR] = 6501;
    ORACLE_STR_ERROR[-OB_ERR_PROGRAM_ERROR] = "ORA-06501: PL/SQL: program error";
    ORACLE_STR_USER_ERROR[-OB_ERR_PROGRAM_ERROR] = "ORA-06501: PL/SQL: program error";
    ERROR_NAME[-OB_ERR_ROWTYPE_MISMATCH] = "OB_ERR_ROWTYPE_MISMATCH";
    MYSQL_ERRNO[-OB_ERR_ROWTYPE_MISMATCH] = -1;
    SQLSTATE[-OB_ERR_ROWTYPE_MISMATCH] = "HY000";
    STR_ERROR[-OB_ERR_ROWTYPE_MISMATCH] = "PL/SQL: Return types of Result Set variables or query do not match";
    STR_USER_ERROR[-OB_ERR_ROWTYPE_MISMATCH] = "PL/SQL: Return types of Result Set variables or query do not match";
    ORACLE_ERRNO[-OB_ERR_ROWTYPE_MISMATCH] = 6504;
    ORACLE_STR_ERROR[-OB_ERR_ROWTYPE_MISMATCH] =
        "ORA-06504: PL/SQL: Return types of Result Set variables or query do not match";
    ORACLE_STR_USER_ERROR[-OB_ERR_ROWTYPE_MISMATCH] =
        "ORA-06504: PL/SQL: Return types of Result Set variables or query do not match";
    ERROR_NAME[-OB_ERR_STORAGE_ERROR] = "OB_ERR_STORAGE_ERROR";
    MYSQL_ERRNO[-OB_ERR_STORAGE_ERROR] = -1;
    SQLSTATE[-OB_ERR_STORAGE_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_STORAGE_ERROR] = "PL/SQL: storage error";
    STR_USER_ERROR[-OB_ERR_STORAGE_ERROR] = "PL/SQL: storage error";
    ORACLE_ERRNO[-OB_ERR_STORAGE_ERROR] = 6500;
    ORACLE_STR_ERROR[-OB_ERR_STORAGE_ERROR] = "ORA-06500: PL/SQL: storage error";
    ORACLE_STR_USER_ERROR[-OB_ERR_STORAGE_ERROR] = "ORA-06500: PL/SQL: storage error";
    ERROR_NAME[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = "OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT";
    MYSQL_ERRNO[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = -1;
    SQLSTATE[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = "HY000";
    STR_ERROR[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = "Subscript outside of limit";
    STR_USER_ERROR[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = "Subscript outside of limit";
    ORACLE_ERRNO[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = 6532;
    ORACLE_STR_ERROR[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = "ORA-06532: Subscript outside of limit";
    ORACLE_STR_USER_ERROR[-OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT] = "ORA-06532: Subscript outside of limit";
    ERROR_NAME[-OB_ERR_INVALID_CURSOR] = "OB_ERR_INVALID_CURSOR";
    MYSQL_ERRNO[-OB_ERR_INVALID_CURSOR] = -1;
    SQLSTATE[-OB_ERR_INVALID_CURSOR] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_CURSOR] = "invalid cursor";
    STR_USER_ERROR[-OB_ERR_INVALID_CURSOR] = "invalid cursor";
    ORACLE_ERRNO[-OB_ERR_INVALID_CURSOR] = 1001;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_CURSOR] = "ORA-01001: invalid cursor";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_CURSOR] = "ORA-01001: invalid cursor";
    ERROR_NAME[-OB_ERR_LOGIN_DENIED] = "OB_ERR_LOGIN_DENIED";
    MYSQL_ERRNO[-OB_ERR_LOGIN_DENIED] = -1;
    SQLSTATE[-OB_ERR_LOGIN_DENIED] = "HY000";
    STR_ERROR[-OB_ERR_LOGIN_DENIED] = "invalid username/password; logon denied";
    STR_USER_ERROR[-OB_ERR_LOGIN_DENIED] = "invalid username/password; logon denied";
    ORACLE_ERRNO[-OB_ERR_LOGIN_DENIED] = 1017;
    ORACLE_STR_ERROR[-OB_ERR_LOGIN_DENIED] = "ORA-01017: invalid username/password; logon deniedd";
    ORACLE_STR_USER_ERROR[-OB_ERR_LOGIN_DENIED] = "ORA-01017: invalid username/password; logon deniedd";
    ERROR_NAME[-OB_ERR_NOT_LOGGED_ON] = "OB_ERR_NOT_LOGGED_ON";
    MYSQL_ERRNO[-OB_ERR_NOT_LOGGED_ON] = -1;
    SQLSTATE[-OB_ERR_NOT_LOGGED_ON] = "HY000";
    STR_ERROR[-OB_ERR_NOT_LOGGED_ON] = "not logged on";
    STR_USER_ERROR[-OB_ERR_NOT_LOGGED_ON] = "not logged on";
    ORACLE_ERRNO[-OB_ERR_NOT_LOGGED_ON] = 1012;
    ORACLE_STR_ERROR[-OB_ERR_NOT_LOGGED_ON] = "ORA-01012: not logged on";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_LOGGED_ON] = "ORA-01012: not logged on";
    ERROR_NAME[-OB_ERR_SELF_IS_NULL] = "OB_ERR_SELF_IS_NULL";
    MYSQL_ERRNO[-OB_ERR_SELF_IS_NULL] = -1;
    SQLSTATE[-OB_ERR_SELF_IS_NULL] = "HY000";
    STR_ERROR[-OB_ERR_SELF_IS_NULL] = "method dispatch on NULL SELF argument is disallowed";
    STR_USER_ERROR[-OB_ERR_SELF_IS_NULL] = "method dispatch on NULL SELF argument is disallowed";
    ORACLE_ERRNO[-OB_ERR_SELF_IS_NULL] = 30625;
    ORACLE_STR_ERROR[-OB_ERR_SELF_IS_NULL] = "ORA-30625: method dispatch on NULL SELF argument is disallowed";
    ORACLE_STR_USER_ERROR[-OB_ERR_SELF_IS_NULL] = "ORA-30625: method dispatch on NULL SELF argument is disallowed";
    ERROR_NAME[-OB_ERR_TIMEOUT_ON_RESOURCE] = "OB_ERR_TIMEOUT_ON_RESOURCE";
    MYSQL_ERRNO[-OB_ERR_TIMEOUT_ON_RESOURCE] = -1;
    SQLSTATE[-OB_ERR_TIMEOUT_ON_RESOURCE] = "HY000";
    STR_ERROR[-OB_ERR_TIMEOUT_ON_RESOURCE] = "timeout occurred while waiting for a resource";
    STR_USER_ERROR[-OB_ERR_TIMEOUT_ON_RESOURCE] = "timeout occurred while waiting for a resource";
    ORACLE_ERRNO[-OB_ERR_TIMEOUT_ON_RESOURCE] = 51;
    ORACLE_STR_ERROR[-OB_ERR_TIMEOUT_ON_RESOURCE] = "ORA-00051: timeout occurred while waiting for a resource";
    ORACLE_STR_USER_ERROR[-OB_ERR_TIMEOUT_ON_RESOURCE] = "ORA-00051: timeout occurred while waiting for a resource";
    ERROR_NAME[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] = "OB_COLUMN_CANT_CHANGE_TO_NOT_NULL";
    MYSQL_ERRNO[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] = -1;
    SQLSTATE[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] = "HY000";
    STR_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] = "column to be modified to NOT NULL is already NOT NULL";
    STR_USER_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] = "column to be modified to NOT NULL is already NOT NULL";
    ORACLE_ERRNO[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] = 1442;
    ORACLE_STR_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] =
        "ORA-01442: column to be modified to NOT NULL is already NOT NULL";
    ORACLE_STR_USER_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NOT_NULL] =
        "ORA-01442: column to be modified to NOT NULL is already NOT NULL";
    ERROR_NAME[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] = "OB_COLUMN_CANT_CHANGE_TO_NULLALE";
    MYSQL_ERRNO[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] = -1;
    SQLSTATE[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] = "HY000";
    STR_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] = "column to be modified to NULL cannot be modified to NULL";
    STR_USER_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] = "column to be modified to NULL cannot be modified to NULL";
    ORACLE_ERRNO[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] = 1451;
    ORACLE_STR_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] =
        "ORA-01451: column to be modified to NULL cannot be modified to NULL";
    ORACLE_STR_USER_ERROR[-OB_COLUMN_CANT_CHANGE_TO_NULLALE] =
        "ORA-01451: column to be modified to NULL cannot be modified to NULL";
    ERROR_NAME[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] = "OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED";
    MYSQL_ERRNO[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] = -1;
    SQLSTATE[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] = "HY000";
    STR_ERROR[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] = "cannot enable - null values found";
    STR_USER_ERROR[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] = "cannot enable (%.*s.%.*s) - null values found";
    ORACLE_ERRNO[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] = 2296;
    ORACLE_STR_ERROR[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] = "ORA-02296: cannot enable - null values found";
    ORACLE_STR_USER_ERROR[-OB_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED] =
        "ORA-02296: cannot enable (%.*s.%.*s) - null values found";
    ERROR_NAME[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = "OB_ERR_ARGUMENT_SHOULD_CONSTANT";
    MYSQL_ERRNO[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = -1;
    SQLSTATE[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = "HY000";
    STR_ERROR[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = "Argument should be a constant.";
    STR_USER_ERROR[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = "Argument should be a constant.";
    ORACLE_ERRNO[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = 30496;
    ORACLE_STR_ERROR[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = "ORA-30496: Argument should be a constant.";
    ORACLE_STR_USER_ERROR[-OB_ERR_ARGUMENT_SHOULD_CONSTANT] = "ORA-30496: Argument should be a constant.";
    ERROR_NAME[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = "OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION";
    MYSQL_ERRNO[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = -1;
    SQLSTATE[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = "HY000";
    STR_ERROR[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = "not a single-group group function";
    STR_USER_ERROR[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = "not a single-group group function";
    ORACLE_ERRNO[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = 937;
    ORACLE_STR_ERROR[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = "ORA-00937: not a single-group group function";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION] = "ORA-00937: not a single-group group function";
    ERROR_NAME[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = "OB_ERR_ZERO_LENGTH_IDENTIFIER";
    MYSQL_ERRNO[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = -1;
    SQLSTATE[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = "HY000";
    STR_ERROR[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = "illegal zero-length identifier";
    STR_USER_ERROR[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = "illegal zero-length identifier";
    ORACLE_ERRNO[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = 1741;
    ORACLE_STR_ERROR[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = "ORA-01741: illegal zero-length identifier";
    ORACLE_STR_USER_ERROR[-OB_ERR_ZERO_LENGTH_IDENTIFIER] = "ORA-01741: illegal zero-length identifier";
    ERROR_NAME[-OB_ERR_PARAM_VALUE_INVALID] = "OB_ERR_PARAM_VALUE_INVALID";
    MYSQL_ERRNO[-OB_ERR_PARAM_VALUE_INVALID] = -1;
    SQLSTATE[-OB_ERR_PARAM_VALUE_INVALID] = "HY000";
    STR_ERROR[-OB_ERR_PARAM_VALUE_INVALID] = "parameter cannot be modified because specified value is invalid";
    STR_USER_ERROR[-OB_ERR_PARAM_VALUE_INVALID] = "parameter cannot be modified because specified value is invalid";
    ORACLE_ERRNO[-OB_ERR_PARAM_VALUE_INVALID] = 2097;
    ORACLE_STR_ERROR[-OB_ERR_PARAM_VALUE_INVALID] =
        "ORA-02097: parameter cannot be modified because specified value is invalid";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARAM_VALUE_INVALID] =
        "ORA-02097: parameter cannot be modified because specified value is invalid";
    ERROR_NAME[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = "OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = "DBMS_SQL access denied";
    STR_USER_ERROR[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = "DBMS_SQL access denied";
    ORACLE_ERRNO[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = 29471;
    ORACLE_STR_ERROR[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = "ORA-29471: DBMS_SQL access denied";
    ORACLE_STR_USER_ERROR[-OB_ERR_DBMS_SQL_CURSOR_NOT_EXIST] = "ORA-29471: DBMS_SQL access denied";
    ERROR_NAME[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = "OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND";
    MYSQL_ERRNO[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = -1;
    SQLSTATE[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = "HY000";
    STR_ERROR[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = "not all variables bound";
    STR_USER_ERROR[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = "not all variables bound";
    ORACLE_ERRNO[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = 1008;
    ORACLE_STR_ERROR[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = "ORA-01008: not all variables bound";
    ORACLE_STR_USER_ERROR[-OB_ERR_DBMS_SQL_NOT_ALL_VAR_BIND] = "ORA-01008: not all variables bound";
    ERROR_NAME[-OB_ERR_CONFLICTING_DECLARATIONS] = "OB_ERR_CONFLICTING_DECLARATIONS";
    MYSQL_ERRNO[-OB_ERR_CONFLICTING_DECLARATIONS] = ER_CONFLICTING_DECLARATIONS;
    SQLSTATE[-OB_ERR_CONFLICTING_DECLARATIONS] = "42000";
    STR_ERROR[-OB_ERR_CONFLICTING_DECLARATIONS] = "Conflicting declarations";
    STR_USER_ERROR[-OB_ERR_CONFLICTING_DECLARATIONS] = "Conflicting declarations: '%s' and '%s'";
    ORACLE_ERRNO[-OB_ERR_CONFLICTING_DECLARATIONS] = 600;
    ORACLE_STR_ERROR[-OB_ERR_CONFLICTING_DECLARATIONS] =
        "ORA-00600: internal error code, arguments: -5858, Conflicting declarations";
    ORACLE_STR_USER_ERROR[-OB_ERR_CONFLICTING_DECLARATIONS] =
        "ORA-00600: internal error code, arguments: -5858, Conflicting declarations: '%s' and '%s'";
    ERROR_NAME[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] = "OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT";
    MYSQL_ERRNO[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] = -1;
    SQLSTATE[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] = "HY000";
    STR_ERROR[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] = "column is referenced in a multi-column constraint";
    STR_USER_ERROR[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] =
        "column is referenced in a multi-column constraint";
    ORACLE_ERRNO[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] = 12991;
    ORACLE_STR_ERROR[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] =
        "ORA-12991: column is referenced in a multi-column constraint";
    ORACLE_STR_USER_ERROR[-OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT] =
        "ORA-12991: column is referenced in a multi-column constraint";
    ERROR_NAME[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] = "OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT";
    MYSQL_ERRNO[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] = -1;
    SQLSTATE[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] =
        "cannot modify column datatype with current constraint(s)";
    STR_USER_ERROR[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] =
        "cannot modify column datatype with current constraint(s)";
    ORACLE_ERRNO[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] = 1463;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] =
        "ORA-01463: cannot modify column datatype with current constraint(s)";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_COL_DATATYEP_REFERENCED_CONSTRAINT] =
        "ORA-01463: cannot modify column datatype with current constraint(s)";
    ERROR_NAME[-OB_ERR_PERCENTILE_VALUE_INVALID] = "OB_ERR_PERCENTILE_VALUE_INVALID";
    MYSQL_ERRNO[-OB_ERR_PERCENTILE_VALUE_INVALID] = -1;
    SQLSTATE[-OB_ERR_PERCENTILE_VALUE_INVALID] = "HY000";
    STR_ERROR[-OB_ERR_PERCENTILE_VALUE_INVALID] = "The percentile value should be a number between 0 and 1.";
    STR_USER_ERROR[-OB_ERR_PERCENTILE_VALUE_INVALID] = "The percentile value should be a number between 0 and 1.";
    ORACLE_ERRNO[-OB_ERR_PERCENTILE_VALUE_INVALID] = 30493;
    ORACLE_STR_ERROR[-OB_ERR_PERCENTILE_VALUE_INVALID] =
        "ORA-30493: The percentile value should be a number between 0 and 1.";
    ORACLE_STR_USER_ERROR[-OB_ERR_PERCENTILE_VALUE_INVALID] =
        "ORA-30493: The percentile value should be a number between 0 and 1.";
    ERROR_NAME[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] =
        "OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE";
    MYSQL_ERRNO[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] = -1;
    SQLSTATE[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] =
        "The argument should be of numeric or date/datetime type.";
    STR_USER_ERROR[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] =
        "The argument should be of numeric or date/datetime type.";
    ORACLE_ERRNO[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] = 30495;
    ORACLE_STR_ERROR[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] =
        "ORA-30495: The argument should be of numeric or date/datetime type.";
    ORACLE_STR_USER_ERROR[-OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE] =
        "ORA-30495: The argument should be of numeric or date/datetime type.";
    ERROR_NAME[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] = "OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION";
    MYSQL_ERRNO[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] = -1;
    SQLSTATE[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] = "HY000";
    STR_ERROR[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] =
        "ALTER TABLE|INDEX RENAME may not be combined with other operations";
    STR_USER_ERROR[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] =
        "ALTER TABLE|INDEX RENAME may not be combined with other operations";
    ORACLE_ERRNO[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] = 14047;
    ORACLE_STR_ERROR[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] =
        "ORA-14047: ALTER TABLE|INDEX RENAME may not be combined with other operations";
    ORACLE_STR_USER_ERROR[-OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION] =
        "ORA-14047: ALTER TABLE|INDEX RENAME may not be combined with other operations";
    ERROR_NAME[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] = "OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED";
    MYSQL_ERRNO[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] = -1;
    SQLSTATE[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] = "only simple column names allowed here.";
    STR_USER_ERROR[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] = "only simple column names allowed here.";
    ORACLE_ERRNO[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] = 1748;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] = "ORA-01748: only simple column names allowed here.";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED] =
        "ORA-01748: only simple column names allowed here.";
    ERROR_NAME[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] = "OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT";
    MYSQL_ERRNO[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] = ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE;
    SQLSTATE[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] = "HY000";
    STR_ERROR[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] =
        "You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column";
    STR_USER_ERROR[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] =
        "You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column";
    ORACLE_ERRNO[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] =
        "ORA-00600: internal error code, arguments: -5865, You are using safe update mode and you tried to update a "
        "table without a WHERE that uses a KEY column";
    ORACLE_STR_USER_ERROR[-OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT] =
        "ORA-00600: internal error code, arguments: -5865, You are using safe update mode and you tried to update a "
        "table without a WHERE that uses a KEY column";
    ERROR_NAME[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] = "OB_ERR_SPECIFIY_PARTITION_DESCRIPTION";
    MYSQL_ERRNO[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] = -1;
    SQLSTATE[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] = "HY000";
    STR_ERROR[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] =
        "cannot specify <(sub)partition-description> clause in CREATE TABLE or CREATE INDEX";
    STR_USER_ERROR[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] =
        "cannot specify <(sub)partition-description> clause in CREATE TABLE or CREATE INDEX";
    ORACLE_ERRNO[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] = 14170;
    ORACLE_STR_ERROR[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] =
        "ORA-14170: cannot specify <(sub)partition-description> clause in CREATE TABLE or CREATE INDEX";
    ORACLE_STR_USER_ERROR[-OB_ERR_SPECIFIY_PARTITION_DESCRIPTION] =
        "ORA-14170: cannot specify <(sub)partition-description> clause in CREATE TABLE or CREATE INDEX";
    ERROR_NAME[-OB_ERR_SAME_NAME_SUBPARTITION] = "OB_ERR_SAME_NAME_SUBPARTITION";
    MYSQL_ERRNO[-OB_ERR_SAME_NAME_SUBPARTITION] = -1;
    SQLSTATE[-OB_ERR_SAME_NAME_SUBPARTITION] = "HY000";
    STR_ERROR[-OB_ERR_SAME_NAME_SUBPARTITION] = "Duplicate partition name";
    STR_USER_ERROR[-OB_ERR_SAME_NAME_SUBPARTITION] = "Duplicate partition name %.*s";
    ORACLE_ERRNO[-OB_ERR_SAME_NAME_SUBPARTITION] = 14159;
    ORACLE_STR_ERROR[-OB_ERR_SAME_NAME_SUBPARTITION] = "ORA-14159: duplicate subpartition name";
    ORACLE_STR_USER_ERROR[-OB_ERR_SAME_NAME_SUBPARTITION] = "ORA-14159: duplicate subpartition name %.*s";
    ERROR_NAME[-OB_ERR_UPDATE_ORDER_BY] = "OB_ERR_UPDATE_ORDER_BY";
    MYSQL_ERRNO[-OB_ERR_UPDATE_ORDER_BY] = -1;
    SQLSTATE[-OB_ERR_UPDATE_ORDER_BY] = "HY000";
    STR_ERROR[-OB_ERR_UPDATE_ORDER_BY] = "Incorrect usage of UPDATE and ORDER BY";
    STR_USER_ERROR[-OB_ERR_UPDATE_ORDER_BY] = "Incorrect usage of UPDATE and ORDER BY";
    ORACLE_ERRNO[-OB_ERR_UPDATE_ORDER_BY] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_ORDER_BY] =
        "ORA-00600: internal error code, arguments: -5868, Incorrect usage of UPDATE and ORDER BY";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_ORDER_BY] =
        "ORA-00600: internal error code, arguments: -5868, Incorrect usage of UPDATE and ORDER BY";
    ERROR_NAME[-OB_ERR_UPDATE_LIMIT] = "OB_ERR_UPDATE_LIMIT";
    MYSQL_ERRNO[-OB_ERR_UPDATE_LIMIT] = -1;
    SQLSTATE[-OB_ERR_UPDATE_LIMIT] = "HY000";
    STR_ERROR[-OB_ERR_UPDATE_LIMIT] = "Incorrect usage of UPDATE and LIMIT";
    STR_USER_ERROR[-OB_ERR_UPDATE_LIMIT] = "Incorrect usage of UPDATE and LIMIT";
    ORACLE_ERRNO[-OB_ERR_UPDATE_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_LIMIT] =
        "ORA-00600: internal error code, arguments: -5869, Incorrect usage of UPDATE and LIMIT";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_LIMIT] =
        "ORA-00600: internal error code, arguments: -5869, Incorrect usage of UPDATE and LIMIT";
    ERROR_NAME[-OB_ROWID_TYPE_MISMATCH] = "OB_ROWID_TYPE_MISMATCH";
    MYSQL_ERRNO[-OB_ROWID_TYPE_MISMATCH] = -1;
    SQLSTATE[-OB_ROWID_TYPE_MISMATCH] = "HY000";
    STR_ERROR[-OB_ROWID_TYPE_MISMATCH] = "rowid type mismatch";
    STR_USER_ERROR[-OB_ROWID_TYPE_MISMATCH] = "rowid type mismatch, expect %.*s, got %.*s";
    ORACLE_ERRNO[-OB_ROWID_TYPE_MISMATCH] = 600;
    ORACLE_STR_ERROR[-OB_ROWID_TYPE_MISMATCH] = "ORA-00600: rowid type mismatch";
    ORACLE_STR_USER_ERROR[-OB_ROWID_TYPE_MISMATCH] = "ORA-00600: rowid type mismatch, expect %s, got %s";
    ERROR_NAME[-OB_ROWID_NUM_MISMATCH] = "OB_ROWID_NUM_MISMATCH";
    MYSQL_ERRNO[-OB_ROWID_NUM_MISMATCH] = -1;
    SQLSTATE[-OB_ROWID_NUM_MISMATCH] = "HY000";
    STR_ERROR[-OB_ROWID_NUM_MISMATCH] = "rowid num mismatch";
    STR_USER_ERROR[-OB_ROWID_NUM_MISMATCH] = "rowid num mismatch, expect %ld, actual %ld";
    ORACLE_ERRNO[-OB_ROWID_NUM_MISMATCH] = 600;
    ORACLE_STR_ERROR[-OB_ROWID_NUM_MISMATCH] = "ORA-00600: rowid type mismatch";
    ORACLE_STR_USER_ERROR[-OB_ROWID_NUM_MISMATCH] = "ORA-00600: rowid type mismatch, expect %ld, actual %ld";
    ERROR_NAME[-OB_NO_COLUMN_ALIAS] = "OB_NO_COLUMN_ALIAS";
    MYSQL_ERRNO[-OB_NO_COLUMN_ALIAS] = -1;
    SQLSTATE[-OB_NO_COLUMN_ALIAS] = "HY000";
    STR_ERROR[-OB_NO_COLUMN_ALIAS] = "must name this expression with a column alias";
    STR_USER_ERROR[-OB_NO_COLUMN_ALIAS] = "must name %.*s with a column alias";
    ORACLE_ERRNO[-OB_NO_COLUMN_ALIAS] = 998;
    ORACLE_STR_ERROR[-OB_NO_COLUMN_ALIAS] = "ORA-00998: must name this expression with a column alias";
    ORACLE_STR_USER_ERROR[-OB_NO_COLUMN_ALIAS] = "ORA-00998: must name %.*s with a column alias";
    ERROR_NAME[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] = "OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH";
    MYSQL_ERRNO[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] = -1;
    SQLSTATE[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] = "HY000";
    STR_ERROR[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] =
        "the numeric value does not match the length of the format item";
    STR_USER_ERROR[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] =
        "the numeric value does not match the length of the format item";
    ORACLE_ERRNO[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] = 1862;
    ORACLE_STR_ERROR[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] =
        "ORA-01862: the numeric value does not match the length of the format item";
    ORACLE_STR_USER_ERROR[-OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH] =
        "ORA-01862: the numeric value does not match the length of the format item";
    ERROR_NAME[-OB_ERR_INVALID_DATATYPE] = "OB_ERR_INVALID_DATATYPE";
    MYSQL_ERRNO[-OB_ERR_INVALID_DATATYPE] = -1;
    SQLSTATE[-OB_ERR_INVALID_DATATYPE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_DATATYPE] = "invalid datatype";
    STR_USER_ERROR[-OB_ERR_INVALID_DATATYPE] = "invalid datatype";
    ORACLE_ERRNO[-OB_ERR_INVALID_DATATYPE] = 902;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_DATATYPE] = "ORA-00902: invalid datatype";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_DATATYPE] = "ORA-00902: invalid datatype";
    ERROR_NAME[-OB_ERR_NOT_COMPOSITE_PARTITION] = "OB_ERR_NOT_COMPOSITE_PARTITION";
    MYSQL_ERRNO[-OB_ERR_NOT_COMPOSITE_PARTITION] = -1;
    SQLSTATE[-OB_ERR_NOT_COMPOSITE_PARTITION] = "HY000";
    STR_ERROR[-OB_ERR_NOT_COMPOSITE_PARTITION] = "table is not partitioned by composite partition method";
    STR_USER_ERROR[-OB_ERR_NOT_COMPOSITE_PARTITION] = "table is not partitioned by composite partition method";
    ORACLE_ERRNO[-OB_ERR_NOT_COMPOSITE_PARTITION] = 14253;
    ORACLE_STR_ERROR[-OB_ERR_NOT_COMPOSITE_PARTITION] =
        "ORA-14253: table is not partitioned by composite partition method";
    ORACLE_STR_USER_ERROR[-OB_ERR_NOT_COMPOSITE_PARTITION] =
        "ORA-14253: table is not partitioned by composite partition method";
    ERROR_NAME[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] = "OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN";
    MYSQL_ERRNO[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] = -1;
    SQLSTATE[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] = "HY000";
    STR_ERROR[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] =
        "VALUES IN (<value list>) cannot be used for Range subpartitioned tables";
    STR_USER_ERROR[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] =
        "VALUES IN (<value list>) cannot be used for Range subpartitioned tables";
    ORACLE_ERRNO[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] = 14214;
    ORACLE_STR_ERROR[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] =
        "ORA-14214: VALUES (<value list>) cannot be used for Range subpartitioned tables";
    ORACLE_STR_USER_ERROR[-OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN] =
        "ORA-14214: VALUES (<value list>) cannot be used for Range subpartitioned tables";
    ERROR_NAME[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = "OB_ERR_SUBPARTITION_EXPECT_VALUES_IN";
    MYSQL_ERRNO[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = -1;
    SQLSTATE[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = "HY000";
    STR_ERROR[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = "VALUES IN (<value list>) clause expected";
    STR_USER_ERROR[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = "VALUES IN (<value list>) clause expected";
    ORACLE_ERRNO[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = 14217;
    ORACLE_STR_ERROR[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = "ORA-14217: VALUES (<value list>) clause expected";
    ORACLE_STR_USER_ERROR[-OB_ERR_SUBPARTITION_EXPECT_VALUES_IN] = "ORA-14217: VALUES (<value list>) clause expected";
    ERROR_NAME[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] = "OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN";
    MYSQL_ERRNO[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] = -1;
    SQLSTATE[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] =
        "VALUES LESS THAN or AT clause cannot be used with List partitioned tables";
    STR_USER_ERROR[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] =
        "VALUES LESS THAN or AT clause cannot be used with List partitioned tables";
    ORACLE_ERRNO[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] = 14310;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] =
        "ORA-14310: VALUES LESS THAN or AT clause cannot be used with List partitioned tables";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN] =
        "ORA-14310: VALUES LESS THAN or AT clause cannot be used with List partitioned tables";
    ERROR_NAME[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] = "OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN";
    MYSQL_ERRNO[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] = -1;
    SQLSTATE[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] = "HY000";
    STR_ERROR[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] = "Expecting VALUES LESS THAN or AT clause";
    STR_USER_ERROR[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] = "Expecting VALUES LESS THAN or AT clause";
    ORACLE_ERRNO[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] = 14311;
    ORACLE_STR_ERROR[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] = "ORA-14311: Expecting VALUES LESS THAN or AT clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN] =
        "ORA-14311: Expecting VALUES LESS THAN or AT clause";
    ERROR_NAME[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] = "OB_ERR_PROGRAM_UNIT_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] = "Procedure, function, package, or package body does not exist";
    STR_USER_ERROR[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] = "Procedure, function, package, or package body does not exist";
    ORACLE_ERRNO[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] = 4042;
    ORACLE_STR_ERROR[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] =
        "ORA-04042: procedure, function, package, or package body does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_PROGRAM_UNIT_NOT_EXIST] =
        "ORA-04042: procedure, function, package, or package body does not exist";
    ERROR_NAME[-OB_ERR_INVALID_RESTORE_POINT_NAME] = "OB_ERR_INVALID_RESTORE_POINT_NAME";
    MYSQL_ERRNO[-OB_ERR_INVALID_RESTORE_POINT_NAME] = -1;
    SQLSTATE[-OB_ERR_INVALID_RESTORE_POINT_NAME] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_RESTORE_POINT_NAME] = "invalid restore point name specified in connection string";
    STR_USER_ERROR[-OB_ERR_INVALID_RESTORE_POINT_NAME] = "invalid restore point name specified in connection string";
    ORACLE_ERRNO[-OB_ERR_INVALID_RESTORE_POINT_NAME] = 600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_RESTORE_POINT_NAME] =
        "ORA-00600: internal error code, arguments: -5881, invalid restore point name specified in connection string";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_RESTORE_POINT_NAME] =
        "ORA-00600: internal error code, arguments: -5881, invalid restore point name specified in connection string";
    ERROR_NAME[-OB_ERR_INPUT_TIME_TYPE] = "OB_ERR_INPUT_TIME_TYPE";
    MYSQL_ERRNO[-OB_ERR_INPUT_TIME_TYPE] = -1;
    SQLSTATE[-OB_ERR_INPUT_TIME_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_INPUT_TIME_TYPE] = "invalid time limit specified";
    STR_USER_ERROR[-OB_ERR_INPUT_TIME_TYPE] = "invalid time limit specified";
    ORACLE_ERRNO[-OB_ERR_INPUT_TIME_TYPE] = 14312;
    ORACLE_STR_ERROR[-OB_ERR_INPUT_TIME_TYPE] = "ORA-14312: invalid time limit specified";
    ORACLE_STR_USER_ERROR[-OB_ERR_INPUT_TIME_TYPE] = "ORA-14312: invalid time limit specified";
    ERROR_NAME[-OB_ERR_IN_ARRAY_DML] = "OB_ERR_IN_ARRAY_DML";
    MYSQL_ERRNO[-OB_ERR_IN_ARRAY_DML] = -1;
    SQLSTATE[-OB_ERR_IN_ARRAY_DML] = "HY000";
    STR_ERROR[-OB_ERR_IN_ARRAY_DML] = "error(s) in array DML";
    STR_USER_ERROR[-OB_ERR_IN_ARRAY_DML] = "error(s) in array DML";
    ORACLE_ERRNO[-OB_ERR_IN_ARRAY_DML] = 24381;
    ORACLE_STR_ERROR[-OB_ERR_IN_ARRAY_DML] = "ORA-24381: error(s) in array DML";
    ORACLE_STR_USER_ERROR[-OB_ERR_IN_ARRAY_DML] = "ORA-24381: error(s) in array DML";
    ERROR_NAME[-OB_ERR_TRIGGER_COMPILE_ERROR] = "OB_ERR_TRIGGER_COMPILE_ERROR";
    MYSQL_ERRNO[-OB_ERR_TRIGGER_COMPILE_ERROR] = -1;
    SQLSTATE[-OB_ERR_TRIGGER_COMPILE_ERROR] = "42000";
    STR_ERROR[-OB_ERR_TRIGGER_COMPILE_ERROR] = "trigger compile error";
    STR_USER_ERROR[-OB_ERR_TRIGGER_COMPILE_ERROR] = "%s \'%.*s.%.*s\' compile error";
    ORACLE_ERRNO[-OB_ERR_TRIGGER_COMPILE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ERR_TRIGGER_COMPILE_ERROR] =
        "ORA-00600: internal error code, arguments: -5884, trigger compile error";
    ORACLE_STR_USER_ERROR[-OB_ERR_TRIGGER_COMPILE_ERROR] =
        "ORA-00600: internal error code, arguments: -5884, %s \'%.*s.%.*s\' compile error";
    ERROR_NAME[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = "OB_ERR_MISSING_OR_INVALID_PASSWORD";
    MYSQL_ERRNO[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = -1;
    SQLSTATE[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = "HY000";
    STR_ERROR[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = "missing or invalid password(s)";
    STR_USER_ERROR[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = "missing or invalid password(s)";
    ORACLE_ERRNO[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = 988;
    ORACLE_STR_ERROR[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = "ORA-00988: missing or invalid password(s)";
    ORACLE_STR_USER_ERROR[-OB_ERR_MISSING_OR_INVALID_PASSWORD] = "ORA-00988: missing or invalid password(s)";
    ERROR_NAME[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] = "OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST";
    MYSQL_ERRNO[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] = -1;
    SQLSTATE[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] = "HY000";
    STR_ERROR[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] = "no matching unique or primary key for this column-list";
    STR_USER_ERROR[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] = "no matching unique or primary key for this column-list";
    ORACLE_ERRNO[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] = 2270;
    ORACLE_STR_ERROR[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] =
        "ORA-02270: no matching unique or primary key for this column-list";
    ORACLE_STR_USER_ERROR[-OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST] =
        "ORA-02270: no matching unique or primary key for this column-list";
    ERROR_NAME[-OB_ERR_DUP_FK_IN_TABLE] = "OB_ERR_DUP_FK_IN_TABLE";
    MYSQL_ERRNO[-OB_ERR_DUP_FK_IN_TABLE] = -1;
    SQLSTATE[-OB_ERR_DUP_FK_IN_TABLE] = "HY000";
    STR_ERROR[-OB_ERR_DUP_FK_IN_TABLE] = "duplicate referential constraint specifications";
    STR_USER_ERROR[-OB_ERR_DUP_FK_IN_TABLE] = "duplicate referential constraint specifications";
    ORACLE_ERRNO[-OB_ERR_DUP_FK_IN_TABLE] = 2774;
    ORACLE_STR_ERROR[-OB_ERR_DUP_FK_IN_TABLE] = "ORA-02774: duplicate referential constraint specifications";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_FK_IN_TABLE] = "ORA-02774: duplicate referential constraint specifications";
    ERROR_NAME[-OB_ERR_DUP_FK_EXISTS] = "OB_ERR_DUP_FK_EXISTS";
    MYSQL_ERRNO[-OB_ERR_DUP_FK_EXISTS] = -1;
    SQLSTATE[-OB_ERR_DUP_FK_EXISTS] = "HY000";
    STR_ERROR[-OB_ERR_DUP_FK_EXISTS] = "such a referential constraint already exists in the table";
    STR_USER_ERROR[-OB_ERR_DUP_FK_EXISTS] = "such a referential constraint already exists in the table";
    ORACLE_ERRNO[-OB_ERR_DUP_FK_EXISTS] = 2775;
    ORACLE_STR_ERROR[-OB_ERR_DUP_FK_EXISTS] = "ORA-02775: such a referential constraint already exists in the table";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUP_FK_EXISTS] =
        "ORA-02775: such a referential constraint already exists in the table";
    ERROR_NAME[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] = "OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE";
    MYSQL_ERRNO[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] = -1;
    SQLSTATE[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] = "specified data type is not supported for a virtual column";
    STR_USER_ERROR[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] = "specified data type is not supported for a virtual column";
    ORACLE_ERRNO[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] = 54003;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] =
        "ORA-54003: specified data type is not supported for a virtual column";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE] =
        "ORA-54003: specified data type is not supported for a virtual column";
    ERROR_NAME[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] = "OB_ERR_REFERENCED_TABLE_HAS_NO_PK";
    MYSQL_ERRNO[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] = -1;
    SQLSTATE[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] = "HY000";
    STR_ERROR[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] = "referenced table does not have a primary key";
    STR_USER_ERROR[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] = "referenced table does not have a primary key";
    ORACLE_ERRNO[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] = 2268;
    ORACLE_STR_ERROR[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] = "ORA-02268: referenced table does not have a primary key";
    ORACLE_STR_USER_ERROR[-OB_ERR_REFERENCED_TABLE_HAS_NO_PK] =
        "ORA-02268: referenced table does not have a primary key";
    ERROR_NAME[-OB_ERR_MODIFY_PART_COLUMN_TYPE] = "OB_ERR_MODIFY_PART_COLUMN_TYPE";
    MYSQL_ERRNO[-OB_ERR_MODIFY_PART_COLUMN_TYPE] = -1;
    SQLSTATE[-OB_ERR_MODIFY_PART_COLUMN_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_PART_COLUMN_TYPE] =
        "data type or length of a table partitioning column may not be changed";
    STR_USER_ERROR[-OB_ERR_MODIFY_PART_COLUMN_TYPE] =
        "data type or length of a table partitioning column may not be changed";
    ORACLE_ERRNO[-OB_ERR_MODIFY_PART_COLUMN_TYPE] = 14060;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_PART_COLUMN_TYPE] =
        "ORA-14060: data type or length of a table partitioning column may not be changed";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_PART_COLUMN_TYPE] =
        "ORA-14060: data type or length of a table partitioning column may not be changed";
    ERROR_NAME[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] = "OB_ERR_MODIFY_SUBPART_COLUMN_TYPE";
    MYSQL_ERRNO[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] = -1;
    SQLSTATE[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] =
        "data type or length of a table subpartitioning column may not be changed";
    STR_USER_ERROR[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] =
        "data type or length of a table subpartitioning column may not be changed";
    ORACLE_ERRNO[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] = 14265;
    ORACLE_STR_ERROR[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] =
        "ORA-14265: data type or length of a table subpartitioning column may not be changed";
    ORACLE_STR_USER_ERROR[-OB_ERR_MODIFY_SUBPART_COLUMN_TYPE] =
        "ORA-14265: data type or length of a table subpartitioning column may not be changed";
    ERROR_NAME[-OB_ERR_DECREASE_COLUMN_LENGTH] = "OB_ERR_DECREASE_COLUMN_LENGTH";
    MYSQL_ERRNO[-OB_ERR_DECREASE_COLUMN_LENGTH] = -1;
    SQLSTATE[-OB_ERR_DECREASE_COLUMN_LENGTH] = "HY000";
    STR_ERROR[-OB_ERR_DECREASE_COLUMN_LENGTH] = "cannot decrease column length because some value is too big";
    STR_USER_ERROR[-OB_ERR_DECREASE_COLUMN_LENGTH] = "cannot decrease column length because some value is too big";
    ORACLE_ERRNO[-OB_ERR_DECREASE_COLUMN_LENGTH] = 1441;
    ORACLE_STR_ERROR[-OB_ERR_DECREASE_COLUMN_LENGTH] =
        "ORA-01441: cannot decrease column length because some value is too big";
    ORACLE_STR_USER_ERROR[-OB_ERR_DECREASE_COLUMN_LENGTH] =
        "ORA-01441: cannot decrease column length because some value is too big";
    ERROR_NAME[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = "OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR";
    MYSQL_ERRNO[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = -1;
    SQLSTATE[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = "HY000";
    STR_ERROR[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = "Datetime/Interval internal error";
    STR_USER_ERROR[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = "Datetime/Interval internal error";
    ORACLE_ERRNO[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = 1891;
    ORACLE_STR_ERROR[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = "ORA-01891: Datetime/Interval internal error";
    ORACLE_STR_USER_ERROR[-OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR] = "ORA-01891: Datetime/Interval internal error";
    ERROR_NAME[-OB_ERR_REMOTE_PART_ILLEGAL] = "OB_ERR_REMOTE_PART_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_REMOTE_PART_ILLEGAL] = -1;
    SQLSTATE[-OB_ERR_REMOTE_PART_ILLEGAL] = "HY000";
    STR_ERROR[-OB_ERR_REMOTE_PART_ILLEGAL] = "partition extended table name cannot refer to a remote object";
    STR_USER_ERROR[-OB_ERR_REMOTE_PART_ILLEGAL] = "partition extended table name cannot refer to a remote object";
    ORACLE_ERRNO[-OB_ERR_REMOTE_PART_ILLEGAL] = 14100;
    ORACLE_STR_ERROR[-OB_ERR_REMOTE_PART_ILLEGAL] =
        "ORA-14100: partition extended table name cannot refer to a remote object";
    ORACLE_STR_USER_ERROR[-OB_ERR_REMOTE_PART_ILLEGAL] =
        "ORA-14100: partition extended table name cannot refer to a remote object";
    ERROR_NAME[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = "OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE";
    MYSQL_ERRNO[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = -1;
    SQLSTATE[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = "HY000";
    STR_ERROR[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = "a view is not appropriate here";
    STR_USER_ERROR[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = "a view is not appropriate here";
    ORACLE_ERRNO[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = 1702;
    ORACLE_STR_ERROR[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = "ORA-01702: a view is not appropriate here";
    ORACLE_STR_USER_ERROR[-OB_ERR_A_VIEW_NOT_APPROPRIATE_HERE] = "ORA-01702: a view is not appropriate here";
    ERROR_NAME[-OB_ROWID_VIEW_NO_KEY_PRESERVED] = "OB_ROWID_VIEW_NO_KEY_PRESERVED";
    MYSQL_ERRNO[-OB_ROWID_VIEW_NO_KEY_PRESERVED] = -1;
    SQLSTATE[-OB_ROWID_VIEW_NO_KEY_PRESERVED] = "HY000";
    STR_ERROR[-OB_ROWID_VIEW_NO_KEY_PRESERVED] =
        "cannot select ROWID from, or sample, a join view without a key-preserved table";
    STR_USER_ERROR[-OB_ROWID_VIEW_NO_KEY_PRESERVED] =
        "cannot select ROWID from, or sample, a join view without a key-preserved table";
    ORACLE_ERRNO[-OB_ROWID_VIEW_NO_KEY_PRESERVED] = 1445;
    ORACLE_STR_ERROR[-OB_ROWID_VIEW_NO_KEY_PRESERVED] =
        "ORA-01445: cannot select ROWID from, or sample, a join view without a key-preserved table";
    ORACLE_STR_USER_ERROR[-OB_ROWID_VIEW_NO_KEY_PRESERVED] =
        "ORA-01445: cannot select ROWID from, or sample, a join view without a key-preserved table";
    ERROR_NAME[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] = "OB_ROWID_VIEW_HAS_DISTINCT_ETC";
    MYSQL_ERRNO[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] = -1;
    SQLSTATE[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] = "HY000";
    STR_ERROR[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] =
        "cannot select ROWID from, or sample, a view with DISTINCT, GROUP BY, etc";
    STR_USER_ERROR[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] =
        "cannot select ROWID from, or sample, a view with DISTINCT, GROUP BY, etc";
    ORACLE_ERRNO[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] = 1446;
    ORACLE_STR_ERROR[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] =
        "ORA-01446: cannot select ROWID from, or sample, a view with DISTINCT, GROUP BY, etc";
    ORACLE_STR_USER_ERROR[-OB_ROWID_VIEW_HAS_DISTINCT_ETC] =
        "ORA-01446: cannot select ROWID from, or sample, a view with DISTINCT, GROUP BY, etc";
    ERROR_NAME[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] = "OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL";
    MYSQL_ERRNO[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] = -1;
    SQLSTATE[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] = "HY000";
    STR_ERROR[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] = "table must have at least 1 column that is not virtual";
    STR_USER_ERROR[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] = "table must have at least 1 column that is not virtual";
    ORACLE_ERRNO[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] = 54037;
    ORACLE_STR_ERROR[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] =
        "ORA-54037: table must have at least 1 column that is not virtual";
    ORACLE_STR_USER_ERROR[-OB_ERR_AT_LEAST_ONE_COLUMN_NOT_VIRTUAL] =
        "ORA-54037: table must have at least 1 column that is not virtual";
    ERROR_NAME[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = "OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED";
    MYSQL_ERRNO[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = -1;
    SQLSTATE[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = "only pure functions can be indexed";
    STR_USER_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = "only pure functions can be indexed";
    ORACLE_ERRNO[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = 1743;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = "ORA-01743: only pure functions can be indexed";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED] = "ORA-01743: only pure functions can be indexed";
    ERROR_NAME[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] =
        "OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION";
    MYSQL_ERRNO[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] = -1;
    SQLSTATE[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] =
        "only pure functions can be specified in a virtual column expression";
    STR_USER_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] =
        "only pure functions can be specified in a virtual column expression";
    ORACLE_ERRNO[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] = 54002;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] =
        "ORA-54002: only pure functions can be specified in a virtual column expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION] =
        "ORA-54002: only pure functions can be specified in a virtual column expression";
    ERROR_NAME[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] = "OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS";
    MYSQL_ERRNO[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] = -1;
    SQLSTATE[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] = "HY000";
    STR_ERROR[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] = "UPDATE operation disallowed on virtual columns";
    STR_USER_ERROR[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] = "UPDATE operation disallowed on virtual columns";
    ORACLE_ERRNO[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] = 54017;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] =
        "ORA-54017: UPDATE operation disallowed on virtual columns";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS] =
        "ORA-54017: UPDATE operation disallowed on virtual columns";
    ERROR_NAME[-OB_ERR_INVALID_COLUMN_EXPRESSION] = "OB_ERR_INVALID_COLUMN_EXPRESSION";
    MYSQL_ERRNO[-OB_ERR_INVALID_COLUMN_EXPRESSION] = -1;
    SQLSTATE[-OB_ERR_INVALID_COLUMN_EXPRESSION] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_COLUMN_EXPRESSION] = "Invalid column expression was specified";
    STR_USER_ERROR[-OB_ERR_INVALID_COLUMN_EXPRESSION] = "Invalid column expression was specified";
    ORACLE_ERRNO[-OB_ERR_INVALID_COLUMN_EXPRESSION] = 54016;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_COLUMN_EXPRESSION] = "ORA-54016: Invalid column expression was specified";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_COLUMN_EXPRESSION] = "ORA-54016: Invalid column expression was specified";
    ERROR_NAME[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] = "OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT";
    MYSQL_ERRNO[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] = -1;
    SQLSTATE[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] = "HY000";
    STR_ERROR[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] = "table can have only one identity column";
    STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] = "table can have only one identity column";
    ORACLE_ERRNO[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] = 30669;
    ORACLE_STR_ERROR[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] = "ORA-30669: table can have only one identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT] =
        "ORA-30669: table can have only one identity column";
    ERROR_NAME[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "invalid NOT NULL constraint specified on an identity column";
    STR_USER_ERROR[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "invalid NOT NULL constraint specified on an identity column";
    ORACLE_ERRNO[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = 30670;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "ORA-30670: invalid NOT NULL constraint specified on an identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "ORA-30670: invalid NOT NULL constraint specified on an identity column";
    ERROR_NAME[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "cannot modify NOT NULL constraint on an identity column";
    STR_USER_ERROR[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "cannot modify NOT NULL constraint on an identity column";
    ORACLE_ERRNO[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = 30671;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "ORA-30671: cannot modify NOT NULL constraint on an identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "ORA-30671: cannot modify NOT NULL constraint on an identity column";
    ERROR_NAME[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "cannot drop NOT NULL constraint on an identity column";
    STR_USER_ERROR[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "cannot drop NOT NULL constraint on an identity column";
    ORACLE_ERRNO[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] = 30672;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "ORA-30672: cannot drop NOT NULL constraint on an identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN] =
        "ORA-30672: cannot drop NOT NULL constraint on an identity column";
    ERROR_NAME[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] = "OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] = "column to be modified is not an identity column";
    STR_USER_ERROR[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] = "column to be modified is not an identity column";
    ORACLE_ERRNO[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] = 30673;
    ORACLE_STR_ERROR[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] =
        "ORA-30673: column to be modified is not an identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN] =
        "ORA-30673: column to be modified is not an identity column";
    ERROR_NAME[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] = "OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE";
    MYSQL_ERRNO[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] = -1;
    SQLSTATE[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] = "identity column cannot have a default value";
    STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] = "identity column cannot have a default value";
    ORACLE_ERRNO[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] = 30674;
    ORACLE_STR_ERROR[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] =
        "ORA-30674: identity column cannot have a default value";
    ORACLE_STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE] =
        "ORA-30674: identity column cannot have a default value";
    ERROR_NAME[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] = "OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE";
    MYSQL_ERRNO[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] = -1;
    SQLSTATE[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] = "identity column must be a numeric type";
    STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] = "identity column must be a numeric type";
    ORACLE_ERRNO[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] = 30675;
    ORACLE_STR_ERROR[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] =
        "ORA-30675: identity column must be a numeric type";
    ORACLE_STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE] =
        "ORA-30675: identity column must be a numeric type";
    ERROR_NAME[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] =
        "OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] =
        "prebuilt table managed column cannot be an identity column";
    STR_USER_ERROR[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] =
        "prebuilt table managed column cannot be an identity column";
    ORACLE_ERRNO[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] = 32792;
    ORACLE_STR_ERROR[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] =
        "ORA-32792: prebuilt table managed column cannot be an identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_PREBUILT_TABLE_MANAGED_CANNOT_BE_IDENTITY_COLUMN] =
        "ORA-32792: prebuilt table managed column cannot be an identity column";
    ERROR_NAME[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] = "OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE";
    MYSQL_ERRNO[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] = -1;
    SQLSTATE[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] = "cannot alter a system-generated sequence";
    STR_USER_ERROR[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] = "cannot alter a system-generated sequence";
    ORACLE_ERRNO[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] = 32793;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] =
        "ORA-32793: cannot alter a system-generated sequence";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_ALTER_SYSTEM_GENERATED_SEQUENCE] =
        "ORA-32793: cannot alter a system-generated sequence";
    ERROR_NAME[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] = "OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE";
    MYSQL_ERRNO[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] = -1;
    SQLSTATE[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] = "cannot drop a system-generated sequence";
    STR_USER_ERROR[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] = "cannot drop a system-generated sequence";
    ORACLE_ERRNO[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] = 32794;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] =
        "ORA-32794: cannot drop a system-generated sequence";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_DROP_SYSTEM_GENERATED_SEQUENCE] =
        "ORA-32794: cannot drop a system-generated sequence";
    ERROR_NAME[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "cannot insert into a generated always identity column";
    STR_USER_ERROR[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "cannot insert into a generated always identity column";
    ORACLE_ERRNO[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] = 32795;
    ORACLE_STR_ERROR[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "ORA-32795: cannot insert into a generated always identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "ORA-32795: cannot insert into a generated always identity column";
    ERROR_NAME[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] = "OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] = "cannot update a generated always identity column";
    STR_USER_ERROR[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "cannot update a generated always identity column";
    ORACLE_ERRNO[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] = 32796;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "ORA-32796: cannot update a generated always identity column";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN] =
        "ORA-32796: cannot update a generated always identity column";
    ERROR_NAME[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] =
        "OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION";
    MYSQL_ERRNO[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] = -1;
    SQLSTATE[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] = "HY000";
    STR_ERROR[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] =
        "identity column sequence mismatch in ALTER TABLE EXCHANGE PARTITION";
    STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] =
        "identity column sequence mismatch in ALTER TABLE EXCHANGE PARTITION";
    ORACLE_ERRNO[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] = 32797;
    ORACLE_STR_ERROR[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] =
        "ORA-32797: identity column sequence mismatch in ALTER TABLE EXCHANGE PARTITION";
    ORACLE_STR_USER_ERROR[-OB_ERR_IDENTITY_COLUMN_SEQUENCE_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION] =
        "ORA-32797: identity column sequence mismatch in ALTER TABLE EXCHANGE PARTITION";
    ERROR_NAME[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] = "OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE";
    MYSQL_ERRNO[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] = -1;
    SQLSTATE[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] = "HY000";
    STR_ERROR[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] = "cannot rename a system-generated sequence";
    STR_USER_ERROR[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] = "cannot rename a system-generated sequence";
    ORACLE_ERRNO[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] = 32798;
    ORACLE_STR_ERROR[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] =
        "ORA-32798: cannot rename a system-generated sequence";
    ORACLE_STR_USER_ERROR[-OB_ERR_CANNOT_RENAME_SYSTEM_GENERATED_SEQUENCE] =
        "ORA-32798: cannot rename a system-generated sequence";
    ERROR_NAME[-OB_ERR_REVOKE_BY_COLUMN] = "OB_ERR_REVOKE_BY_COLUMN";
    MYSQL_ERRNO[-OB_ERR_REVOKE_BY_COLUMN] = -1;
    SQLSTATE[-OB_ERR_REVOKE_BY_COLUMN] = "HY000";
    STR_ERROR[-OB_ERR_REVOKE_BY_COLUMN] = "UPDATE/REFERENCES may only be REVOKEd from the whole table, not by column";
    STR_USER_ERROR[-OB_ERR_REVOKE_BY_COLUMN] =
        "UPDATE/REFERENCES may only be REVOKEd from the whole table, not by column";
    ORACLE_ERRNO[-OB_ERR_REVOKE_BY_COLUMN] = 1750;
    ORACLE_STR_ERROR[-OB_ERR_REVOKE_BY_COLUMN] =
        "ORA-01750: UPDATE/REFERENCES may only be REVOKEd from the whole table, not by column";
    ORACLE_STR_USER_ERROR[-OB_ERR_REVOKE_BY_COLUMN] =
        "ORA-01750: UPDATE/REFERENCES may only be REVOKEd from the whole table, not by column";
    ERROR_NAME[-OB_ERR_TYPE_BODY_NOT_EXIST] = "OB_ERR_TYPE_BODY_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_TYPE_BODY_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_TYPE_BODY_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_TYPE_BODY_NOT_EXIST] = "not executed, type body does not exist";
    STR_USER_ERROR[-OB_ERR_TYPE_BODY_NOT_EXIST] = "not executed, type body '%.*s' does not exist";
    ORACLE_ERRNO[-OB_ERR_TYPE_BODY_NOT_EXIST] = 4067;
    ORACLE_STR_ERROR[-OB_ERR_TYPE_BODY_NOT_EXIST] = "ORA-04067: not executed, type body does not exist";
    ORACLE_STR_USER_ERROR[-OB_ERR_TYPE_BODY_NOT_EXIST] = "ORA-04067: not executed, type body '%.*s' does not exist";
    ERROR_NAME[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] = "OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET";
    MYSQL_ERRNO[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] = -1;
    SQLSTATE[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] = "The argument of WIDTH_BUCKET function is NULL or invalid.";
    STR_USER_ERROR[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] =
        "The argument [%s] of WIDTH_BUCKET function is NULL or invalid.";
    ORACLE_ERRNO[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] = 30494;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] =
        "ORA-30494: The argument of WIDTH_BUCKET function is NULL or invalid.";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET] =
        "ORA-30494: The argument [%s] of WIDTH_BUCKET function is NULL or invalid.";
    ERROR_NAME[-OB_ERR_CBY_NO_MEMORY] = "OB_ERR_CBY_NO_MEMORY";
    MYSQL_ERRNO[-OB_ERR_CBY_NO_MEMORY] = -1;
    SQLSTATE[-OB_ERR_CBY_NO_MEMORY] = "HY000";
    STR_ERROR[-OB_ERR_CBY_NO_MEMORY] = "Not enough memory for CONNECT BY operation";
    STR_USER_ERROR[-OB_ERR_CBY_NO_MEMORY] = "Not enough memory for CONNECT BY operation";
    ORACLE_ERRNO[-OB_ERR_CBY_NO_MEMORY] = 30009;
    ORACLE_STR_ERROR[-OB_ERR_CBY_NO_MEMORY] = "ORA-30009: Not enough memory for CONNECT BY operation";
    ORACLE_STR_USER_ERROR[-OB_ERR_CBY_NO_MEMORY] = "ORA-30009: Not enough memory for CONNECT BY operation";
    ERROR_NAME[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] = "OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH";
    MYSQL_ERRNO[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] = -1;
    SQLSTATE[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] = "HY000";
    STR_ERROR[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] = "illegal parameter in SYS_CONNECT_BY_PATH function";
    STR_USER_ERROR[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] = "illegal parameter in SYS_CONNECT_BY_PATH function";
    ORACLE_ERRNO[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] = 30003;
    ORACLE_STR_ERROR[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] =
        "ORA-30003: illegal parameter in SYS_CONNECT_BY_PATH function";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH] =
        "ORA-30003: illegal parameter in SYS_CONNECT_BY_PATH function";
    ERROR_NAME[-OB_ERR_HOST_UNKNOWN] = "OB_ERR_HOST_UNKNOWN";
    MYSQL_ERRNO[-OB_ERR_HOST_UNKNOWN] = -1;
    SQLSTATE[-OB_ERR_HOST_UNKNOWN] = "HY000";
    STR_ERROR[-OB_ERR_HOST_UNKNOWN] = "host unknown";
    STR_USER_ERROR[-OB_ERR_HOST_UNKNOWN] = "host %.*s unknown";
    ORACLE_ERRNO[-OB_ERR_HOST_UNKNOWN] = 29257;
    ORACLE_STR_ERROR[-OB_ERR_HOST_UNKNOWN] = "ORA-29257: host unknown";
    ORACLE_STR_USER_ERROR[-OB_ERR_HOST_UNKNOWN] = "ORA-29257: host %.*s unknown";
    ERROR_NAME[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] = "OB_ERR_WINDOW_NAME_IS_NOT_DEFINE";
    MYSQL_ERRNO[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] = -1;
    SQLSTATE[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] = "HY000";
    STR_ERROR[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] = "Window name is not defined.";
    STR_USER_ERROR[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] = "Window name '%.*s' is not defined.";
    ORACLE_ERRNO[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] = 600;
    ORACLE_STR_ERROR[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] =
        "ORA-00600: internal error code, arguments: -5929, Window name is not defined.";
    ORACLE_STR_USER_ERROR[-OB_ERR_WINDOW_NAME_IS_NOT_DEFINE] =
        "ORA-00600: internal error code, arguments: -5929, Window name '%.*s' is not defined.";
    ERROR_NAME[-OB_ERR_OPEN_CURSORS_EXCEEDED] = "OB_ERR_OPEN_CURSORS_EXCEEDED";
    MYSQL_ERRNO[-OB_ERR_OPEN_CURSORS_EXCEEDED] = -1;
    SQLSTATE[-OB_ERR_OPEN_CURSORS_EXCEEDED] = "HY000";
    STR_ERROR[-OB_ERR_OPEN_CURSORS_EXCEEDED] = "maximum open cursors exceeded";
    STR_USER_ERROR[-OB_ERR_OPEN_CURSORS_EXCEEDED] = "maximum open cursors exceeded";
    ORACLE_ERRNO[-OB_ERR_OPEN_CURSORS_EXCEEDED] = 1000;
    ORACLE_STR_ERROR[-OB_ERR_OPEN_CURSORS_EXCEEDED] = "ORA-01000: maximum open cursors exceeded";
    ORACLE_STR_USER_ERROR[-OB_ERR_OPEN_CURSORS_EXCEEDED] = "ORA-01000: maximum open cursors exceeded";
    ERROR_NAME[-OB_ERR_ARG_INVALID] = "OB_ERR_ARG_INVALID";
    MYSQL_ERRNO[-OB_ERR_ARG_INVALID] = -1;
    SQLSTATE[-OB_ERR_ARG_INVALID] = "HY000";
    STR_ERROR[-OB_ERR_ARG_INVALID] = "argument is null, invalid, or out of range";
    STR_USER_ERROR[-OB_ERR_ARG_INVALID] = "argument is null, invalid, or out of range";
    ORACLE_ERRNO[-OB_ERR_ARG_INVALID] = 21560;
    ORACLE_STR_ERROR[-OB_ERR_ARG_INVALID] = "ORA-21560: argument %.*s is null, invalid, or out of range";
    ORACLE_STR_USER_ERROR[-OB_ERR_ARG_INVALID] = "ORA-21560: argument %.*s is null, invalid, or out of range";
    ERROR_NAME[-OB_ERR_ILL_NAME_STRING] = "OB_ERR_ILL_NAME_STRING";
    MYSQL_ERRNO[-OB_ERR_ILL_NAME_STRING] = -1;
    SQLSTATE[-OB_ERR_ILL_NAME_STRING] = "HY000";
    STR_ERROR[-OB_ERR_ILL_NAME_STRING] = "unexpected name string";
    STR_USER_ERROR[-OB_ERR_ILL_NAME_STRING] = "unexpected name string";
    ORACLE_ERRNO[-OB_ERR_ILL_NAME_STRING] = 21560;
    ORACLE_STR_ERROR[-OB_ERR_ILL_NAME_STRING] = "ORA-21560: unexpected name string '%.*s'";
    ORACLE_STR_USER_ERROR[-OB_ERR_ILL_NAME_STRING] = "ORA-21560: unexpected name string '%.*s'";
    ERROR_NAME[-OB_TRANSACTION_SET_VIOLATION] = "OB_TRANSACTION_SET_VIOLATION";
    MYSQL_ERRNO[-OB_TRANSACTION_SET_VIOLATION] = -1;
    SQLSTATE[-OB_TRANSACTION_SET_VIOLATION] = "25000";
    STR_ERROR[-OB_TRANSACTION_SET_VIOLATION] = "Transaction set changed during the execution";
    STR_USER_ERROR[-OB_TRANSACTION_SET_VIOLATION] = "Transaction set changed during the execution";
    ORACLE_ERRNO[-OB_TRANSACTION_SET_VIOLATION] = 600;
    ORACLE_STR_ERROR[-OB_TRANSACTION_SET_VIOLATION] =
        "ORA-00600: internal error code, arguments: -6001, Transaction set changed during the execution";
    ORACLE_STR_USER_ERROR[-OB_TRANSACTION_SET_VIOLATION] =
        "ORA-00600: internal error code, arguments: -6001, Transaction set changed during the execution";
    ERROR_NAME[-OB_TRANS_ROLLBACKED] = "OB_TRANS_ROLLBACKED";
    MYSQL_ERRNO[-OB_TRANS_ROLLBACKED] = -1;
    SQLSTATE[-OB_TRANS_ROLLBACKED] = "40000";
    STR_ERROR[-OB_TRANS_ROLLBACKED] = "Transaction rollbacked";
    STR_USER_ERROR[-OB_TRANS_ROLLBACKED] = "transaction is rolled back";
    ORACLE_ERRNO[-OB_TRANS_ROLLBACKED] = 24761;
    ORACLE_STR_ERROR[-OB_TRANS_ROLLBACKED] = "ORA-24761: transaction rolled back";
    ORACLE_STR_USER_ERROR[-OB_TRANS_ROLLBACKED] = "ORA-24761: transaction rolled back";
    ERROR_NAME[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] = "OB_ERR_EXCLUSIVE_LOCK_CONFLICT";
    MYSQL_ERRNO[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] = ER_LOCK_WAIT_TIMEOUT;
    SQLSTATE[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] = "HY000";
    STR_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] = "Lock wait timeout exceeded; try restarting transaction";
    STR_USER_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] = "Lock wait timeout exceeded; try restarting transaction";
    ORACLE_ERRNO[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] = 30006;
    ORACLE_STR_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] = "ORA-30006: resource busy; acquire with WAIT timeout expired";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT] =
        "ORA-30006: resource busy; acquire with WAIT timeout expired";
    ERROR_NAME[-OB_ERR_SHARED_LOCK_CONFLICT] = "OB_ERR_SHARED_LOCK_CONFLICT";
    MYSQL_ERRNO[-OB_ERR_SHARED_LOCK_CONFLICT] = 4012;
    SQLSTATE[-OB_ERR_SHARED_LOCK_CONFLICT] = "HY000";
    STR_ERROR[-OB_ERR_SHARED_LOCK_CONFLICT] = "Statement is timeout";
    STR_USER_ERROR[-OB_ERR_SHARED_LOCK_CONFLICT] = "Statement is timeout";
    ORACLE_ERRNO[-OB_ERR_SHARED_LOCK_CONFLICT] = 2049;
    ORACLE_STR_ERROR[-OB_ERR_SHARED_LOCK_CONFLICT] = "ORA-02049: timeout: distributed transaction waiting for lock";
    ORACLE_STR_USER_ERROR[-OB_ERR_SHARED_LOCK_CONFLICT] =
        "ORA-02049: timeout: distributed transaction waiting for lock";
    ERROR_NAME[-OB_TRY_LOCK_ROW_CONFLICT] = "OB_TRY_LOCK_ROW_CONFLICT";
    MYSQL_ERRNO[-OB_TRY_LOCK_ROW_CONFLICT] = -1;
    SQLSTATE[-OB_TRY_LOCK_ROW_CONFLICT] = "HY000";
    STR_ERROR[-OB_TRY_LOCK_ROW_CONFLICT] = "Try lock row conflict";
    STR_USER_ERROR[-OB_TRY_LOCK_ROW_CONFLICT] = "Try lock row conflict";
    ORACLE_ERRNO[-OB_TRY_LOCK_ROW_CONFLICT] = 600;
    ORACLE_STR_ERROR[-OB_TRY_LOCK_ROW_CONFLICT] =
        "ORA-00600: internal error code, arguments: -6005, Try lock row conflict";
    ORACLE_STR_USER_ERROR[-OB_TRY_LOCK_ROW_CONFLICT] =
        "ORA-00600: internal error code, arguments: -6005, Try lock row conflict";
    ERROR_NAME[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] = "OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT";
    MYSQL_ERRNO[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] = ER_LOCK_WAIT_TIMEOUT;
    SQLSTATE[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] = "HY000";
    STR_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] = "Lock wait timeout exceeded; try restarting transaction";
    STR_USER_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] = "Lock wait timeout exceeded; try restarting transaction";
    ORACLE_ERRNO[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] = 54;
    ORACLE_STR_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] =
        "ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT] =
        "ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired";
    ERROR_NAME[-OB_CLOCK_OUT_OF_ORDER] = "OB_CLOCK_OUT_OF_ORDER";
    MYSQL_ERRNO[-OB_CLOCK_OUT_OF_ORDER] = -1;
    SQLSTATE[-OB_CLOCK_OUT_OF_ORDER] = "25000";
    STR_ERROR[-OB_CLOCK_OUT_OF_ORDER] = "Clock out of order";
    STR_USER_ERROR[-OB_CLOCK_OUT_OF_ORDER] = "Clock out of order";
    ORACLE_ERRNO[-OB_CLOCK_OUT_OF_ORDER] = 600;
    ORACLE_STR_ERROR[-OB_CLOCK_OUT_OF_ORDER] = "ORA-00600: internal error code, arguments: -6201, Clock out of order";
    ORACLE_STR_USER_ERROR[-OB_CLOCK_OUT_OF_ORDER] =
        "ORA-00600: internal error code, arguments: -6201, Clock out of order";
    ERROR_NAME[-OB_MASK_SET_NO_NODE] = "OB_MASK_SET_NO_NODE";
    MYSQL_ERRNO[-OB_MASK_SET_NO_NODE] = -1;
    SQLSTATE[-OB_MASK_SET_NO_NODE] = "25000";
    STR_ERROR[-OB_MASK_SET_NO_NODE] = "Mask set has no node";
    STR_USER_ERROR[-OB_MASK_SET_NO_NODE] = "Mask set has no node";
    ORACLE_ERRNO[-OB_MASK_SET_NO_NODE] = 600;
    ORACLE_STR_ERROR[-OB_MASK_SET_NO_NODE] = "ORA-00600: internal error code, arguments: -6203, Mask set has no node";
    ORACLE_STR_USER_ERROR[-OB_MASK_SET_NO_NODE] =
        "ORA-00600: internal error code, arguments: -6203, Mask set has no node";
    ERROR_NAME[-OB_TRANS_HAS_DECIDED] = "OB_TRANS_HAS_DECIDED";
    MYSQL_ERRNO[-OB_TRANS_HAS_DECIDED] = -1;
    SQLSTATE[-OB_TRANS_HAS_DECIDED] = "HY000";
    STR_ERROR[-OB_TRANS_HAS_DECIDED] = "Transaction has been decided";
    STR_USER_ERROR[-OB_TRANS_HAS_DECIDED] = "Transaction has been decided";
    ORACLE_ERRNO[-OB_TRANS_HAS_DECIDED] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_HAS_DECIDED] =
        "ORA-00600: internal error code, arguments: -6204, Transaction has been decided";
    ORACLE_STR_USER_ERROR[-OB_TRANS_HAS_DECIDED] =
        "ORA-00600: internal error code, arguments: -6204, Transaction has been decided";
    ERROR_NAME[-OB_TRANS_INVALID_STATE] = "OB_TRANS_INVALID_STATE";
    MYSQL_ERRNO[-OB_TRANS_INVALID_STATE] = -1;
    SQLSTATE[-OB_TRANS_INVALID_STATE] = "HY000";
    STR_ERROR[-OB_TRANS_INVALID_STATE] = "Transaction state invalid";
    STR_USER_ERROR[-OB_TRANS_INVALID_STATE] = "Transaction state invalid";
    ORACLE_ERRNO[-OB_TRANS_INVALID_STATE] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_INVALID_STATE] =
        "ORA-00600: internal error code, arguments: -6205, Transaction state invalid";
    ORACLE_STR_USER_ERROR[-OB_TRANS_INVALID_STATE] =
        "ORA-00600: internal error code, arguments: -6205, Transaction state invalid";
    ERROR_NAME[-OB_TRANS_STATE_NOT_CHANGE] = "OB_TRANS_STATE_NOT_CHANGE";
    MYSQL_ERRNO[-OB_TRANS_STATE_NOT_CHANGE] = -1;
    SQLSTATE[-OB_TRANS_STATE_NOT_CHANGE] = "HY000";
    STR_ERROR[-OB_TRANS_STATE_NOT_CHANGE] = "Transaction state not changed";
    STR_USER_ERROR[-OB_TRANS_STATE_NOT_CHANGE] = "Transaction state not changed";
    ORACLE_ERRNO[-OB_TRANS_STATE_NOT_CHANGE] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_STATE_NOT_CHANGE] =
        "ORA-00600: internal error code, arguments: -6206, Transaction state not changed";
    ORACLE_STR_USER_ERROR[-OB_TRANS_STATE_NOT_CHANGE] =
        "ORA-00600: internal error code, arguments: -6206, Transaction state not changed";
    ERROR_NAME[-OB_TRANS_PROTOCOL_ERROR] = "OB_TRANS_PROTOCOL_ERROR";
    MYSQL_ERRNO[-OB_TRANS_PROTOCOL_ERROR] = -1;
    SQLSTATE[-OB_TRANS_PROTOCOL_ERROR] = "HY000";
    STR_ERROR[-OB_TRANS_PROTOCOL_ERROR] = "Transaction protocol error";
    STR_USER_ERROR[-OB_TRANS_PROTOCOL_ERROR] = "Transaction protocol error";
    ORACLE_ERRNO[-OB_TRANS_PROTOCOL_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_PROTOCOL_ERROR] =
        "ORA-00600: internal error code, arguments: -6207, Transaction protocol error";
    ORACLE_STR_USER_ERROR[-OB_TRANS_PROTOCOL_ERROR] =
        "ORA-00600: internal error code, arguments: -6207, Transaction protocol error";
    ERROR_NAME[-OB_TRANS_INVALID_MESSAGE] = "OB_TRANS_INVALID_MESSAGE";
    MYSQL_ERRNO[-OB_TRANS_INVALID_MESSAGE] = -1;
    SQLSTATE[-OB_TRANS_INVALID_MESSAGE] = "HY000";
    STR_ERROR[-OB_TRANS_INVALID_MESSAGE] = "Transaction message invalid";
    STR_USER_ERROR[-OB_TRANS_INVALID_MESSAGE] = "Transaction message invalid";
    ORACLE_ERRNO[-OB_TRANS_INVALID_MESSAGE] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_INVALID_MESSAGE] =
        "ORA-00600: internal error code, arguments: -6208, Transaction message invalid";
    ORACLE_STR_USER_ERROR[-OB_TRANS_INVALID_MESSAGE] =
        "ORA-00600: internal error code, arguments: -6208, Transaction message invalid";
    ERROR_NAME[-OB_TRANS_INVALID_MESSAGE_TYPE] = "OB_TRANS_INVALID_MESSAGE_TYPE";
    MYSQL_ERRNO[-OB_TRANS_INVALID_MESSAGE_TYPE] = -1;
    SQLSTATE[-OB_TRANS_INVALID_MESSAGE_TYPE] = "HY000";
    STR_ERROR[-OB_TRANS_INVALID_MESSAGE_TYPE] = "Transaction message type invalid";
    STR_USER_ERROR[-OB_TRANS_INVALID_MESSAGE_TYPE] = "Transaction message type invalid";
    ORACLE_ERRNO[-OB_TRANS_INVALID_MESSAGE_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_INVALID_MESSAGE_TYPE] =
        "ORA-00600: internal error code, arguments: -6209, Transaction message type invalid";
    ORACLE_STR_USER_ERROR[-OB_TRANS_INVALID_MESSAGE_TYPE] =
        "ORA-00600: internal error code, arguments: -6209, Transaction message type invalid";
    ERROR_NAME[-OB_TRANS_TIMEOUT] = "OB_TRANS_TIMEOUT";
    MYSQL_ERRNO[-OB_TRANS_TIMEOUT] = 4012;
    SQLSTATE[-OB_TRANS_TIMEOUT] = "25000";
    STR_ERROR[-OB_TRANS_TIMEOUT] = "Transaction is timeout";
    STR_USER_ERROR[-OB_TRANS_TIMEOUT] = "Transaction is timeout";
    ORACLE_ERRNO[-OB_TRANS_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_TIMEOUT] = "ORA-00600: internal error code, arguments: -6210, Transaction is timeout";
    ORACLE_STR_USER_ERROR[-OB_TRANS_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -6210, Transaction is timeout";
    ERROR_NAME[-OB_TRANS_KILLED] = "OB_TRANS_KILLED";
    MYSQL_ERRNO[-OB_TRANS_KILLED] = 6002;
    SQLSTATE[-OB_TRANS_KILLED] = "25000";
    STR_ERROR[-OB_TRANS_KILLED] = "Transaction is killed";
    STR_USER_ERROR[-OB_TRANS_KILLED] = "Transaction is killed";
    ORACLE_ERRNO[-OB_TRANS_KILLED] = 24761;
    ORACLE_STR_ERROR[-OB_TRANS_KILLED] = "ORA-24761: transaction rolled back: transaction is killed";
    ORACLE_STR_USER_ERROR[-OB_TRANS_KILLED] = "ORA-24761: transaction rolled back: transaction is killed";
    ERROR_NAME[-OB_TRANS_STMT_TIMEOUT] = "OB_TRANS_STMT_TIMEOUT";
    MYSQL_ERRNO[-OB_TRANS_STMT_TIMEOUT] = 4012;
    SQLSTATE[-OB_TRANS_STMT_TIMEOUT] = "25000";
    STR_ERROR[-OB_TRANS_STMT_TIMEOUT] = "Statement is timeout";
    STR_USER_ERROR[-OB_TRANS_STMT_TIMEOUT] = "Statement is timeout";
    ORACLE_ERRNO[-OB_TRANS_STMT_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_STMT_TIMEOUT] = "ORA-00600: internal error code, arguments: -6212, Statement is timeout";
    ORACLE_STR_USER_ERROR[-OB_TRANS_STMT_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -6212, Statement is timeout";
    ERROR_NAME[-OB_TRANS_CTX_NOT_EXIST] = "OB_TRANS_CTX_NOT_EXIST";
    MYSQL_ERRNO[-OB_TRANS_CTX_NOT_EXIST] = 6002;
    SQLSTATE[-OB_TRANS_CTX_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_TRANS_CTX_NOT_EXIST] = "Transaction context does not exist";
    STR_USER_ERROR[-OB_TRANS_CTX_NOT_EXIST] = "Transaction context does not exist";
    ORACLE_ERRNO[-OB_TRANS_CTX_NOT_EXIST] = 24761;
    ORACLE_STR_ERROR[-OB_TRANS_CTX_NOT_EXIST] =
        "ORA-24761: transaction rolled back: transaction context does not exist";
    ORACLE_STR_USER_ERROR[-OB_TRANS_CTX_NOT_EXIST] =
        "ORA-24761: transaction rolled back: transaction context does not exist";
    ERROR_NAME[-OB_PARTITION_IS_FROZEN] = "OB_PARTITION_IS_FROZEN";
    MYSQL_ERRNO[-OB_PARTITION_IS_FROZEN] = 6002;
    SQLSTATE[-OB_PARTITION_IS_FROZEN] = "25000";
    STR_ERROR[-OB_PARTITION_IS_FROZEN] = "Partition is frozen";
    STR_USER_ERROR[-OB_PARTITION_IS_FROZEN] = "Partition is frozen";
    ORACLE_ERRNO[-OB_PARTITION_IS_FROZEN] = 24761;
    ORACLE_STR_ERROR[-OB_PARTITION_IS_FROZEN] = "ORA-24761: transaction rolled back: partition is frozen";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_IS_FROZEN] = "ORA-24761: transaction rolled back: partition is frozen";
    ERROR_NAME[-OB_PARTITION_IS_NOT_FROZEN] = "OB_PARTITION_IS_NOT_FROZEN";
    MYSQL_ERRNO[-OB_PARTITION_IS_NOT_FROZEN] = -1;
    SQLSTATE[-OB_PARTITION_IS_NOT_FROZEN] = "HY000";
    STR_ERROR[-OB_PARTITION_IS_NOT_FROZEN] = "Partition is not frozen";
    STR_USER_ERROR[-OB_PARTITION_IS_NOT_FROZEN] = "Partition is not frozen";
    ORACLE_ERRNO[-OB_PARTITION_IS_NOT_FROZEN] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_IS_NOT_FROZEN] =
        "ORA-00600: internal error code, arguments: -6215, Partition is not frozen";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_IS_NOT_FROZEN] =
        "ORA-00600: internal error code, arguments: -6215, Partition is not frozen";
    ERROR_NAME[-OB_TRANS_INVALID_LOG_TYPE] = "OB_TRANS_INVALID_LOG_TYPE";
    MYSQL_ERRNO[-OB_TRANS_INVALID_LOG_TYPE] = -1;
    SQLSTATE[-OB_TRANS_INVALID_LOG_TYPE] = "HY000";
    STR_ERROR[-OB_TRANS_INVALID_LOG_TYPE] = "Transaction invalid log type";
    STR_USER_ERROR[-OB_TRANS_INVALID_LOG_TYPE] = "Transaction invalid log type";
    ORACLE_ERRNO[-OB_TRANS_INVALID_LOG_TYPE] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_INVALID_LOG_TYPE] =
        "ORA-00600: internal error code, arguments: -6219, Transaction invalid log type";
    ORACLE_STR_USER_ERROR[-OB_TRANS_INVALID_LOG_TYPE] =
        "ORA-00600: internal error code, arguments: -6219, Transaction invalid log type";
    ERROR_NAME[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] = "OB_TRANS_SQL_SEQUENCE_ILLEGAL";
    MYSQL_ERRNO[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] = -1;
    SQLSTATE[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] = "HY000";
    STR_ERROR[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] = "SQL sequence illegal";
    STR_USER_ERROR[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] = "SQL sequence illegal";
    ORACLE_ERRNO[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] =
        "ORA-00600: internal error code, arguments: -6220, SQL sequence illegal";
    ORACLE_STR_USER_ERROR[-OB_TRANS_SQL_SEQUENCE_ILLEGAL] =
        "ORA-00600: internal error code, arguments: -6220, SQL sequence illegal";
    ERROR_NAME[-OB_TRANS_CANNOT_BE_KILLED] = "OB_TRANS_CANNOT_BE_KILLED";
    MYSQL_ERRNO[-OB_TRANS_CANNOT_BE_KILLED] = -1;
    SQLSTATE[-OB_TRANS_CANNOT_BE_KILLED] = "HY000";
    STR_ERROR[-OB_TRANS_CANNOT_BE_KILLED] = "Transaction context cannot be killed";
    STR_USER_ERROR[-OB_TRANS_CANNOT_BE_KILLED] = "Transaction context cannot be killed";
    ORACLE_ERRNO[-OB_TRANS_CANNOT_BE_KILLED] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_CANNOT_BE_KILLED] =
        "ORA-00600: internal error code, arguments: -6221, Transaction context cannot be killed";
    ORACLE_STR_USER_ERROR[-OB_TRANS_CANNOT_BE_KILLED] =
        "ORA-00600: internal error code, arguments: -6221, Transaction context cannot be killed";
    ERROR_NAME[-OB_TRANS_STATE_UNKNOWN] = "OB_TRANS_STATE_UNKNOWN";
    MYSQL_ERRNO[-OB_TRANS_STATE_UNKNOWN] = -1;
    SQLSTATE[-OB_TRANS_STATE_UNKNOWN] = "HY000";
    STR_ERROR[-OB_TRANS_STATE_UNKNOWN] = "Transaction state unknown";
    STR_USER_ERROR[-OB_TRANS_STATE_UNKNOWN] = "Transaction state unknown";
    ORACLE_ERRNO[-OB_TRANS_STATE_UNKNOWN] = 25405;
    ORACLE_STR_ERROR[-OB_TRANS_STATE_UNKNOWN] = "ORA-25405: transaction status unknown";
    ORACLE_STR_USER_ERROR[-OB_TRANS_STATE_UNKNOWN] = "ORA-25405: transaction status unknown";
    ERROR_NAME[-OB_TRANS_IS_EXITING] = "OB_TRANS_IS_EXITING";
    MYSQL_ERRNO[-OB_TRANS_IS_EXITING] = 6002;
    SQLSTATE[-OB_TRANS_IS_EXITING] = "25000";
    STR_ERROR[-OB_TRANS_IS_EXITING] = "Transaction exiting";
    STR_USER_ERROR[-OB_TRANS_IS_EXITING] = "Transaction exiting";
    ORACLE_ERRNO[-OB_TRANS_IS_EXITING] = 24761;
    ORACLE_STR_ERROR[-OB_TRANS_IS_EXITING] = "ORA-24761: transaction rolled back: Transaction exiting";
    ORACLE_STR_USER_ERROR[-OB_TRANS_IS_EXITING] = "ORA-24761: transaction rolled back: Transaction exiting";
    ERROR_NAME[-OB_TRANS_NEED_ROLLBACK] = "OB_TRANS_NEED_ROLLBACK";
    MYSQL_ERRNO[-OB_TRANS_NEED_ROLLBACK] = 6002;
    SQLSTATE[-OB_TRANS_NEED_ROLLBACK] = "25000";
    STR_ERROR[-OB_TRANS_NEED_ROLLBACK] = "transaction needs rollback";
    STR_USER_ERROR[-OB_TRANS_NEED_ROLLBACK] = "transaction needs rollback";
    ORACLE_ERRNO[-OB_TRANS_NEED_ROLLBACK] = 24761;
    ORACLE_STR_ERROR[-OB_TRANS_NEED_ROLLBACK] = "ORA-24761: transaction rolled back: transaction needs rollback";
    ORACLE_STR_USER_ERROR[-OB_TRANS_NEED_ROLLBACK] = "ORA-24761: transaction rolled back: transaction needs rollback";
    ERROR_NAME[-OB_TRANS_UNKNOWN] = "OB_TRANS_UNKNOWN";
    MYSQL_ERRNO[-OB_TRANS_UNKNOWN] = 4012;
    SQLSTATE[-OB_TRANS_UNKNOWN] = "25000";
    STR_ERROR[-OB_TRANS_UNKNOWN] = "Transaction result is unknown";
    STR_USER_ERROR[-OB_TRANS_UNKNOWN] = "Transaction result is unknown";
    ORACLE_ERRNO[-OB_TRANS_UNKNOWN] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -6225, Transaction result is unknown";
    ORACLE_STR_USER_ERROR[-OB_TRANS_UNKNOWN] =
        "ORA-00600: internal error code, arguments: -6225, Transaction result is unknown";
    ERROR_NAME[-OB_ERR_READ_ONLY_TRANSACTION] = "OB_ERR_READ_ONLY_TRANSACTION";
    MYSQL_ERRNO[-OB_ERR_READ_ONLY_TRANSACTION] = ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION;
    SQLSTATE[-OB_ERR_READ_ONLY_TRANSACTION] = "25006";
    STR_ERROR[-OB_ERR_READ_ONLY_TRANSACTION] = "Cannot execute statement in a READ ONLY transaction";
    STR_USER_ERROR[-OB_ERR_READ_ONLY_TRANSACTION] = "Cannot execute statement in a READ ONLY transaction";
    ORACLE_ERRNO[-OB_ERR_READ_ONLY_TRANSACTION] = 600;
    ORACLE_STR_ERROR[-OB_ERR_READ_ONLY_TRANSACTION] =
        "ORA-00600: internal error code, arguments: -6226, Cannot execute statement in a READ ONLY transaction";
    ORACLE_STR_USER_ERROR[-OB_ERR_READ_ONLY_TRANSACTION] =
        "ORA-00600: internal error code, arguments: -6226, Cannot execute statement in a READ ONLY transaction";
    ERROR_NAME[-OB_PARTITION_IS_NOT_STOPPED] = "OB_PARTITION_IS_NOT_STOPPED";
    MYSQL_ERRNO[-OB_PARTITION_IS_NOT_STOPPED] = -1;
    SQLSTATE[-OB_PARTITION_IS_NOT_STOPPED] = "HY000";
    STR_ERROR[-OB_PARTITION_IS_NOT_STOPPED] = "Partition is not stopped";
    STR_USER_ERROR[-OB_PARTITION_IS_NOT_STOPPED] = "Partition is not stopped";
    ORACLE_ERRNO[-OB_PARTITION_IS_NOT_STOPPED] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_IS_NOT_STOPPED] =
        "ORA-00600: internal error code, arguments: -6227, Partition is not stopped";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_IS_NOT_STOPPED] =
        "ORA-00600: internal error code, arguments: -6227, Partition is not stopped";
    ERROR_NAME[-OB_PARTITION_IS_STOPPED] = "OB_PARTITION_IS_STOPPED";
    MYSQL_ERRNO[-OB_PARTITION_IS_STOPPED] = -1;
    SQLSTATE[-OB_PARTITION_IS_STOPPED] = "HY000";
    STR_ERROR[-OB_PARTITION_IS_STOPPED] = "Partition has been stopped";
    STR_USER_ERROR[-OB_PARTITION_IS_STOPPED] = "Partition has been stopped";
    ORACLE_ERRNO[-OB_PARTITION_IS_STOPPED] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_IS_STOPPED] =
        "ORA-00600: internal error code, arguments: -6228, Partition has been stopped";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_IS_STOPPED] =
        "ORA-00600: internal error code, arguments: -6228, Partition has been stopped";
    ERROR_NAME[-OB_PARTITION_IS_BLOCKED] = "OB_PARTITION_IS_BLOCKED";
    MYSQL_ERRNO[-OB_PARTITION_IS_BLOCKED] = -1;
    SQLSTATE[-OB_PARTITION_IS_BLOCKED] = "HY000";
    STR_ERROR[-OB_PARTITION_IS_BLOCKED] = "Partition has been blocked";
    STR_USER_ERROR[-OB_PARTITION_IS_BLOCKED] = "Partition has been blocked";
    ORACLE_ERRNO[-OB_PARTITION_IS_BLOCKED] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_IS_BLOCKED] =
        "ORA-00600: internal error code, arguments: -6229, Partition has been blocked";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_IS_BLOCKED] =
        "ORA-00600: internal error code, arguments: -6229, Partition has been blocked";
    ERROR_NAME[-OB_TRANS_RPC_TIMEOUT] = "OB_TRANS_RPC_TIMEOUT";
    MYSQL_ERRNO[-OB_TRANS_RPC_TIMEOUT] = 4012;
    SQLSTATE[-OB_TRANS_RPC_TIMEOUT] = "25000";
    STR_ERROR[-OB_TRANS_RPC_TIMEOUT] = "transaction rpc timeout";
    STR_USER_ERROR[-OB_TRANS_RPC_TIMEOUT] = "transaction rpc timeout";
    ORACLE_ERRNO[-OB_TRANS_RPC_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_RPC_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -6230, transaction rpc timeout";
    ORACLE_STR_USER_ERROR[-OB_TRANS_RPC_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -6230, transaction rpc timeout";
    ERROR_NAME[-OB_REPLICA_NOT_READABLE] = "OB_REPLICA_NOT_READABLE";
    MYSQL_ERRNO[-OB_REPLICA_NOT_READABLE] = -1;
    SQLSTATE[-OB_REPLICA_NOT_READABLE] = "HY000";
    STR_ERROR[-OB_REPLICA_NOT_READABLE] = "replica is not readable";
    STR_USER_ERROR[-OB_REPLICA_NOT_READABLE] = "replica is not readable";
    ORACLE_ERRNO[-OB_REPLICA_NOT_READABLE] = 600;
    ORACLE_STR_ERROR[-OB_REPLICA_NOT_READABLE] =
        "ORA-00600: internal error code, arguments: -6231, replica is not readable";
    ORACLE_STR_USER_ERROR[-OB_REPLICA_NOT_READABLE] =
        "ORA-00600: internal error code, arguments: -6231, replica is not readable";
    ERROR_NAME[-OB_PARTITION_IS_SPLITTING] = "OB_PARTITION_IS_SPLITTING";
    MYSQL_ERRNO[-OB_PARTITION_IS_SPLITTING] = -1;
    SQLSTATE[-OB_PARTITION_IS_SPLITTING] = "HY000";
    STR_ERROR[-OB_PARTITION_IS_SPLITTING] = "Partition is splitting";
    STR_USER_ERROR[-OB_PARTITION_IS_SPLITTING] = "Partition is splitting";
    ORACLE_ERRNO[-OB_PARTITION_IS_SPLITTING] = 600;
    ORACLE_STR_ERROR[-OB_PARTITION_IS_SPLITTING] =
        "ORA-00600: internal error code, arguments: -6232, Partition is splitting";
    ORACLE_STR_USER_ERROR[-OB_PARTITION_IS_SPLITTING] =
        "ORA-00600: internal error code, arguments: -6232, Partition is splitting";
    ERROR_NAME[-OB_TRANS_COMMITED] = "OB_TRANS_COMMITED";
    MYSQL_ERRNO[-OB_TRANS_COMMITED] = -1;
    SQLSTATE[-OB_TRANS_COMMITED] = "HY000";
    STR_ERROR[-OB_TRANS_COMMITED] = "Transaction has been commited";
    STR_USER_ERROR[-OB_TRANS_COMMITED] = "Transaction has been commited";
    ORACLE_ERRNO[-OB_TRANS_COMMITED] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_COMMITED] =
        "ORA-00600: internal error code, arguments: -6233, Transaction has been commited";
    ORACLE_STR_USER_ERROR[-OB_TRANS_COMMITED] =
        "ORA-00600: internal error code, arguments: -6233, Transaction has been commited";
    ERROR_NAME[-OB_TRANS_CTX_COUNT_REACH_LIMIT] = "OB_TRANS_CTX_COUNT_REACH_LIMIT";
    MYSQL_ERRNO[-OB_TRANS_CTX_COUNT_REACH_LIMIT] = 6002;
    SQLSTATE[-OB_TRANS_CTX_COUNT_REACH_LIMIT] = "25000";
    STR_ERROR[-OB_TRANS_CTX_COUNT_REACH_LIMIT] = "transaction context count reach limit";
    STR_USER_ERROR[-OB_TRANS_CTX_COUNT_REACH_LIMIT] = "transaction context count reach limit";
    ORACLE_ERRNO[-OB_TRANS_CTX_COUNT_REACH_LIMIT] = 24761;
    ORACLE_STR_ERROR[-OB_TRANS_CTX_COUNT_REACH_LIMIT] =
        "ORA-24761: transaction rolled back: transaction context count reach limit";
    ORACLE_STR_USER_ERROR[-OB_TRANS_CTX_COUNT_REACH_LIMIT] =
        "ORA-24761: transaction rolled back: transaction context count reach limit";
    ERROR_NAME[-OB_TRANS_CANNOT_SERIALIZE] = "OB_TRANS_CANNOT_SERIALIZE";
    MYSQL_ERRNO[-OB_TRANS_CANNOT_SERIALIZE] = -1;
    SQLSTATE[-OB_TRANS_CANNOT_SERIALIZE] = "25000";
    STR_ERROR[-OB_TRANS_CANNOT_SERIALIZE] = "can't serialize access for this transaction";
    STR_USER_ERROR[-OB_TRANS_CANNOT_SERIALIZE] = "can't serialize access for this transaction";
    ORACLE_ERRNO[-OB_TRANS_CANNOT_SERIALIZE] = 8177;
    ORACLE_STR_ERROR[-OB_TRANS_CANNOT_SERIALIZE] = "ORA-08177: can't serialize access for this transaction";
    ORACLE_STR_USER_ERROR[-OB_TRANS_CANNOT_SERIALIZE] = "ORA-08177: can't serialize access for this transaction";
    ERROR_NAME[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] = "OB_TRANS_WEAK_READ_VERSION_NOT_READY";
    MYSQL_ERRNO[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] = -1;
    SQLSTATE[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] = "HY000";
    STR_ERROR[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] = "transaction weak read version is not ready";
    STR_USER_ERROR[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] = "transaction weak read version is not ready";
    ORACLE_ERRNO[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] =
        "ORA-00600: internal error code, arguments: -6236, transaction weak read version is not ready";
    ORACLE_STR_USER_ERROR[-OB_TRANS_WEAK_READ_VERSION_NOT_READY] =
        "ORA-00600: internal error code, arguments: -6236, transaction weak read version is not ready";
    ERROR_NAME[-OB_GTS_STANDBY_IS_INVALID] = "OB_GTS_STANDBY_IS_INVALID";
    MYSQL_ERRNO[-OB_GTS_STANDBY_IS_INVALID] = -1;
    SQLSTATE[-OB_GTS_STANDBY_IS_INVALID] = "HY000";
    STR_ERROR[-OB_GTS_STANDBY_IS_INVALID] = "gts standby is invalid";
    STR_USER_ERROR[-OB_GTS_STANDBY_IS_INVALID] = "gts standby is invalid";
    ORACLE_ERRNO[-OB_GTS_STANDBY_IS_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_GTS_STANDBY_IS_INVALID] =
        "ORA-00600: internal error code, arguments: -6237, gts standby is invalid";
    ORACLE_STR_USER_ERROR[-OB_GTS_STANDBY_IS_INVALID] =
        "ORA-00600: internal error code, arguments: -6237, gts standby is invalid";
    ERROR_NAME[-OB_GTS_UPDATE_FAILED] = "OB_GTS_UPDATE_FAILED";
    MYSQL_ERRNO[-OB_GTS_UPDATE_FAILED] = -1;
    SQLSTATE[-OB_GTS_UPDATE_FAILED] = "HY000";
    STR_ERROR[-OB_GTS_UPDATE_FAILED] = "gts update failed";
    STR_USER_ERROR[-OB_GTS_UPDATE_FAILED] = "gts update failed";
    ORACLE_ERRNO[-OB_GTS_UPDATE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_GTS_UPDATE_FAILED] = "ORA-00600: internal error code, arguments: -6238, gts update failed";
    ORACLE_STR_USER_ERROR[-OB_GTS_UPDATE_FAILED] =
        "ORA-00600: internal error code, arguments: -6238, gts update failed";
    ERROR_NAME[-OB_GTS_IS_NOT_SERVING] = "OB_GTS_IS_NOT_SERVING";
    MYSQL_ERRNO[-OB_GTS_IS_NOT_SERVING] = -1;
    SQLSTATE[-OB_GTS_IS_NOT_SERVING] = "HY000";
    STR_ERROR[-OB_GTS_IS_NOT_SERVING] = "gts is not serving";
    STR_USER_ERROR[-OB_GTS_IS_NOT_SERVING] = "gts is not serving";
    ORACLE_ERRNO[-OB_GTS_IS_NOT_SERVING] = 600;
    ORACLE_STR_ERROR[-OB_GTS_IS_NOT_SERVING] = "ORA-00600: internal error code, arguments: -6239, gts is not serving";
    ORACLE_STR_USER_ERROR[-OB_GTS_IS_NOT_SERVING] =
        "ORA-00600: internal error code, arguments: -6239, gts is not serving";
    ERROR_NAME[-OB_PG_PARTITION_NOT_EXIST] = "OB_PG_PARTITION_NOT_EXIST";
    MYSQL_ERRNO[-OB_PG_PARTITION_NOT_EXIST] = -1;
    SQLSTATE[-OB_PG_PARTITION_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_PG_PARTITION_NOT_EXIST] = "pg partition not exist";
    STR_USER_ERROR[-OB_PG_PARTITION_NOT_EXIST] = "pg partition not exist";
    ORACLE_ERRNO[-OB_PG_PARTITION_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_PG_PARTITION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -6240, pg partition not exist";
    ORACLE_STR_USER_ERROR[-OB_PG_PARTITION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -6240, pg partition not exist";
    ERROR_NAME[-OB_TRANS_STMT_NEED_RETRY] = "OB_TRANS_STMT_NEED_RETRY";
    MYSQL_ERRNO[-OB_TRANS_STMT_NEED_RETRY] = -1;
    SQLSTATE[-OB_TRANS_STMT_NEED_RETRY] = "HY000";
    STR_ERROR[-OB_TRANS_STMT_NEED_RETRY] = "transaction statement need retry";
    STR_USER_ERROR[-OB_TRANS_STMT_NEED_RETRY] = "transaction statement need retry";
    ORACLE_ERRNO[-OB_TRANS_STMT_NEED_RETRY] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_STMT_NEED_RETRY] =
        "ORA-00600: internal error code, arguments: -6241, transaction statement need retry";
    ORACLE_STR_USER_ERROR[-OB_TRANS_STMT_NEED_RETRY] =
        "ORA-00600: internal error code, arguments: -6241, transaction statement need retry";
    ERROR_NAME[-OB_SAVEPOINT_NOT_EXIST] = "OB_SAVEPOINT_NOT_EXIST";
    MYSQL_ERRNO[-OB_SAVEPOINT_NOT_EXIST] = ER_SP_DOES_NOT_EXIST;
    SQLSTATE[-OB_SAVEPOINT_NOT_EXIST] = "42000";
    STR_ERROR[-OB_SAVEPOINT_NOT_EXIST] = "savepoint does not exist";
    STR_USER_ERROR[-OB_SAVEPOINT_NOT_EXIST] = "savepoint does not exist";
    ORACLE_ERRNO[-OB_SAVEPOINT_NOT_EXIST] = 1086;
    ORACLE_STR_ERROR[-OB_SAVEPOINT_NOT_EXIST] = "ORA-01086: savepoint does not exist";
    ORACLE_STR_USER_ERROR[-OB_SAVEPOINT_NOT_EXIST] = "ORA-01086: savepoint does not exist";
    ERROR_NAME[-OB_TRANS_WAIT_SCHEMA_REFRESH] = "OB_TRANS_WAIT_SCHEMA_REFRESH";
    MYSQL_ERRNO[-OB_TRANS_WAIT_SCHEMA_REFRESH] = -1;
    SQLSTATE[-OB_TRANS_WAIT_SCHEMA_REFRESH] = "HY000";
    STR_ERROR[-OB_TRANS_WAIT_SCHEMA_REFRESH] = "local schema is not new enough, replaying logs of user table from "
                                               "standby cluster needs to wait for schema refreshing ";
    STR_USER_ERROR[-OB_TRANS_WAIT_SCHEMA_REFRESH] = "local schema is not new enough, replaying logs of user table from "
                                                    "standby cluster needs to wait for schema refreshing ";
    ORACLE_ERRNO[-OB_TRANS_WAIT_SCHEMA_REFRESH] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_WAIT_SCHEMA_REFRESH] =
        "ORA-00600: internal error code, arguments: -6243, local schema is not new enough, replaying logs of user "
        "table from standby cluster needs to wait for schema refreshing ";
    ORACLE_STR_USER_ERROR[-OB_TRANS_WAIT_SCHEMA_REFRESH] =
        "ORA-00600: internal error code, arguments: -6243, local schema is not new enough, replaying logs of user "
        "table from standby cluster needs to wait for schema refreshing ";
    ERROR_NAME[-OB_TRANS_OUT_OF_THRESHOLD] = "OB_TRANS_OUT_OF_THRESHOLD";
    MYSQL_ERRNO[-OB_TRANS_OUT_OF_THRESHOLD] = -1;
    SQLSTATE[-OB_TRANS_OUT_OF_THRESHOLD] = "HY000";
    STR_ERROR[-OB_TRANS_OUT_OF_THRESHOLD] = "out of transaction threshold";
    STR_USER_ERROR[-OB_TRANS_OUT_OF_THRESHOLD] = "out of transaction threshold";
    ORACLE_ERRNO[-OB_TRANS_OUT_OF_THRESHOLD] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_OUT_OF_THRESHOLD] =
        "ORA-00600: internal error code, arguments: -6244, out of transaction threshold";
    ORACLE_STR_USER_ERROR[-OB_TRANS_OUT_OF_THRESHOLD] =
        "ORA-00600: internal error code, arguments: -6244, out of transaction threshold";
    ERROR_NAME[-OB_TRANS_XA_NOTA] = "OB_TRANS_XA_NOTA";
    MYSQL_ERRNO[-OB_TRANS_XA_NOTA] = ER_XAER_NOTA;
    SQLSTATE[-OB_TRANS_XA_NOTA] = "XAE04";
    STR_ERROR[-OB_TRANS_XA_NOTA] = "Unknown XID";
    STR_USER_ERROR[-OB_TRANS_XA_NOTA] = "Unknown XID";
    ORACLE_ERRNO[-OB_TRANS_XA_NOTA] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_NOTA] = "ORA-00600: internal error code, arguments: -6245, Unknown XID";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_NOTA] = "ORA-00600: internal error code, arguments: -6245, Unknown XID";
    ERROR_NAME[-OB_TRANS_XA_RMFAIL] = "OB_TRANS_XA_RMFAIL";
    MYSQL_ERRNO[-OB_TRANS_XA_RMFAIL] = ER_XAER_RMFAIL;
    SQLSTATE[-OB_TRANS_XA_RMFAIL] = "XAE07";
    STR_ERROR[-OB_TRANS_XA_RMFAIL] = "The command cannot be executed when global transaction is in this state";
    STR_USER_ERROR[-OB_TRANS_XA_RMFAIL] = "The command cannot be executed when global transaction is in the %s state";
    ORACLE_ERRNO[-OB_TRANS_XA_RMFAIL] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_RMFAIL] = "ORA-00600: internal error code, arguments: -6246, The command cannot be "
                                            "executed when global transaction is in this state";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_RMFAIL] = "ORA-00600: internal error code, arguments: -6246, The command cannot "
                                                 "be executed when global transaction is in the %s state";
    ERROR_NAME[-OB_TRANS_XA_DUPID] = "OB_TRANS_XA_DUPID";
    MYSQL_ERRNO[-OB_TRANS_XA_DUPID] = ER_XAER_DUPID;
    SQLSTATE[-OB_TRANS_XA_DUPID] = "XAE08";
    STR_ERROR[-OB_TRANS_XA_DUPID] = "The XID already exists";
    STR_USER_ERROR[-OB_TRANS_XA_DUPID] = "The XID already exists";
    ORACLE_ERRNO[-OB_TRANS_XA_DUPID] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_DUPID] = "ORA-00600: internal error code, arguments: -6247, The XID already exists";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_DUPID] =
        "ORA-00600: internal error code, arguments: -6247, The XID already exists";
    ERROR_NAME[-OB_TRANS_XA_OUTSIDE] = "OB_TRANS_XA_OUTSIDE";
    MYSQL_ERRNO[-OB_TRANS_XA_OUTSIDE] = ER_XAER_OUTSIDE;
    SQLSTATE[-OB_TRANS_XA_OUTSIDE] = "XAE09";
    STR_ERROR[-OB_TRANS_XA_OUTSIDE] = "Some work is done outside global transaction";
    STR_USER_ERROR[-OB_TRANS_XA_OUTSIDE] = "Some work is done outside global transaction";
    ORACLE_ERRNO[-OB_TRANS_XA_OUTSIDE] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_OUTSIDE] =
        "ORA-00600: internal error code, arguments: -6248, Some work is done outside global transaction";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_OUTSIDE] =
        "ORA-00600: internal error code, arguments: -6248, Some work is done outside global transaction";
    ERROR_NAME[-OB_TRANS_XA_INVAL] = "OB_TRANS_XA_INVAL";
    MYSQL_ERRNO[-OB_TRANS_XA_INVAL] = ER_XAER_INVAL;
    SQLSTATE[-OB_TRANS_XA_INVAL] = "XAE05";
    STR_ERROR[-OB_TRANS_XA_INVAL] = "Invalid arguments were given";
    STR_USER_ERROR[-OB_TRANS_XA_INVAL] = "Invalid arguments were given";
    ORACLE_ERRNO[-OB_TRANS_XA_INVAL] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_INVAL] =
        "ORA-00600: internal error code, arguments: -6249, Invalid arguments were given";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_INVAL] =
        "ORA-00600: internal error code, arguments: -6249, Invalid arguments were given";
    ERROR_NAME[-OB_TRANS_XA_RMERR] = "OB_TRANS_XA_RMERR";
    MYSQL_ERRNO[-OB_TRANS_XA_RMERR] = ER_XAER_RMERR;
    SQLSTATE[-OB_TRANS_XA_RMERR] = "XAE03";
    STR_ERROR[-OB_TRANS_XA_RMERR] = "Resource manager error occurred in the transaction branch";
    STR_USER_ERROR[-OB_TRANS_XA_RMERR] = "Resource manager error occurred in the transaction branch";
    ORACLE_ERRNO[-OB_TRANS_XA_RMERR] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_RMERR] =
        "ORA-00600: internal error code, arguments: -6250, Resource manager error occurred in the transaction branch";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_RMERR] =
        "ORA-00600: internal error code, arguments: -6250, Resource manager error occurred in the transaction branch";
    ERROR_NAME[-OB_TRANS_XA_PROTO] = "OB_TRANS_XA_PROTO";
    MYSQL_ERRNO[-OB_TRANS_XA_PROTO] = -1;
    SQLSTATE[-OB_TRANS_XA_PROTO] = "HY000";
    STR_ERROR[-OB_TRANS_XA_PROTO] = "Routine invoked in an improper context";
    STR_USER_ERROR[-OB_TRANS_XA_PROTO] = "Routine invoked in an improper context";
    ORACLE_ERRNO[-OB_TRANS_XA_PROTO] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_PROTO] =
        "ORA-00600: internal error code, arguments: -6251, Routine invoked in an improper context";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_PROTO] =
        "ORA-00600: internal error code, arguments: -6251, Routine invoked in an improper context";
    ERROR_NAME[-OB_TRANS_XA_RBROLLBACK] = "OB_TRANS_XA_RBROLLBACK";
    MYSQL_ERRNO[-OB_TRANS_XA_RBROLLBACK] = ER_XA_RBROLLBACK;
    SQLSTATE[-OB_TRANS_XA_RBROLLBACK] = "XA100";
    STR_ERROR[-OB_TRANS_XA_RBROLLBACK] = "Rollback was caused by an unspecified reason";
    STR_USER_ERROR[-OB_TRANS_XA_RBROLLBACK] = "Rollback was caused by an unspecified reason";
    ORACLE_ERRNO[-OB_TRANS_XA_RBROLLBACK] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_RBROLLBACK] =
        "ORA-00600: internal error code, arguments: -6252, Rollback was caused by an unspecified reason";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_RBROLLBACK] =
        "ORA-00600: internal error code, arguments: -6252, Rollback was caused by an unspecified reason";
    ERROR_NAME[-OB_TRANS_XA_RBTIMEOUT] = "OB_TRANS_XA_RBTIMEOUT";
    MYSQL_ERRNO[-OB_TRANS_XA_RBTIMEOUT] = ER_XA_RBTIMEOUT;
    SQLSTATE[-OB_TRANS_XA_RBTIMEOUT] = "XA106";
    STR_ERROR[-OB_TRANS_XA_RBTIMEOUT] = "Transaction branch took long";
    STR_USER_ERROR[-OB_TRANS_XA_RBTIMEOUT] = "Transaction branch took long";
    ORACLE_ERRNO[-OB_TRANS_XA_RBTIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_RBTIMEOUT] =
        "ORA-00600: internal error code, arguments: -6253, Transaction branch took long";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_RBTIMEOUT] =
        "ORA-00600: internal error code, arguments: -6253, Transaction branch took long";
    ERROR_NAME[-OB_TRANS_XA_RDONLY] = "OB_TRANS_XA_RDONLY";
    MYSQL_ERRNO[-OB_TRANS_XA_RDONLY] = -1;
    SQLSTATE[-OB_TRANS_XA_RDONLY] = "HY000";
    STR_ERROR[-OB_TRANS_XA_RDONLY] = "Transaction was read-only and has been committed";
    STR_USER_ERROR[-OB_TRANS_XA_RDONLY] = "Transaction was read-only and has been committed";
    ORACLE_ERRNO[-OB_TRANS_XA_RDONLY] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_RDONLY] =
        "ORA-00600: internal error code, arguments: -6254, Transaction was read-only and has been committed";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_RDONLY] =
        "ORA-00600: internal error code, arguments: -6254, Transaction was read-only and has been committed";
    ERROR_NAME[-OB_TRANS_XA_RETRY] = "OB_TRANS_XA_RETRY";
    MYSQL_ERRNO[-OB_TRANS_XA_RETRY] = -1;
    SQLSTATE[-OB_TRANS_XA_RETRY] = "HY000";
    STR_ERROR[-OB_TRANS_XA_RETRY] = "Routine returned with no effect and may be re-issued";
    STR_USER_ERROR[-OB_TRANS_XA_RETRY] = "Routine returned with no effect and may be re-issued";
    ORACLE_ERRNO[-OB_TRANS_XA_RETRY] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_XA_RETRY] =
        "ORA-00600: internal error code, arguments: -6255, Routine returned with no effect and may be re-issued";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_RETRY] =
        "ORA-00600: internal error code, arguments: -6255, Routine returned with no effect and may be re-issued";
    ERROR_NAME[-OB_ERR_ROW_NOT_LOCKED] = "OB_ERR_ROW_NOT_LOCKED";
    MYSQL_ERRNO[-OB_ERR_ROW_NOT_LOCKED] = -1;
    SQLSTATE[-OB_ERR_ROW_NOT_LOCKED] = "HY000";
    STR_ERROR[-OB_ERR_ROW_NOT_LOCKED] = "Row has not been locked";
    STR_USER_ERROR[-OB_ERR_ROW_NOT_LOCKED] = "Row has not been locked";
    ORACLE_ERRNO[-OB_ERR_ROW_NOT_LOCKED] = 600;
    ORACLE_STR_ERROR[-OB_ERR_ROW_NOT_LOCKED] =
        "ORA-00600: internal error code, arguments: -6256, Row has not been locked";
    ORACLE_STR_USER_ERROR[-OB_ERR_ROW_NOT_LOCKED] =
        "ORA-00600: internal error code, arguments: -6256, Row has not been locked";
    ERROR_NAME[-OB_EMPTY_PG] = "OB_EMPTY_PG";
    MYSQL_ERRNO[-OB_EMPTY_PG] = -1;
    SQLSTATE[-OB_EMPTY_PG] = "HY000";
    STR_ERROR[-OB_EMPTY_PG] = "Empty partition group";
    STR_USER_ERROR[-OB_EMPTY_PG] = "Empty partition group";
    ORACLE_ERRNO[-OB_EMPTY_PG] = 600;
    ORACLE_STR_ERROR[-OB_EMPTY_PG] = "ORA-00600: internal error code, arguments: -6257, Empty partition group";
    ORACLE_STR_USER_ERROR[-OB_EMPTY_PG] = "ORA-00600: internal error code, arguments: -6257, Empty partition group";
    ERROR_NAME[-OB_TRANS_XA_ERR_COMMIT] = "OB_TRANS_XA_ERR_COMMIT";
    MYSQL_ERRNO[-OB_TRANS_XA_ERR_COMMIT] = ER_XAER_RMFAIL;
    SQLSTATE[-OB_TRANS_XA_ERR_COMMIT] = "XAE07";
    STR_ERROR[-OB_TRANS_XA_ERR_COMMIT] =
        "RMFAIL: The command cannot be executed when global transaction is in this state";
    STR_USER_ERROR[-OB_TRANS_XA_ERR_COMMIT] =
        "RMFAIL: The command cannot be executed when global transaction is in the %s state";
    ORACLE_ERRNO[-OB_TRANS_XA_ERR_COMMIT] = 2089;
    ORACLE_STR_ERROR[-OB_TRANS_XA_ERR_COMMIT] = "ORA-02089: COMMIT is not allowed in a subordinate session";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_ERR_COMMIT] = "ORA-02089: COMMIT is not allowed in a subordinate session";
    ERROR_NAME[-OB_ERR_RESTORE_POINT_EXIST] = "OB_ERR_RESTORE_POINT_EXIST";
    MYSQL_ERRNO[-OB_ERR_RESTORE_POINT_EXIST] = -1;
    SQLSTATE[-OB_ERR_RESTORE_POINT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_RESTORE_POINT_EXIST] = "Restore point %s already exists";
    STR_USER_ERROR[-OB_ERR_RESTORE_POINT_EXIST] = "Restore point %s already exists";
    ORACLE_ERRNO[-OB_ERR_RESTORE_POINT_EXIST] = 38778;
    ORACLE_STR_ERROR[-OB_ERR_RESTORE_POINT_EXIST] = "ORA-38778: Restore point %s already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_RESTORE_POINT_EXIST] = "ORA-38778: Restore point %s already exists";
    ERROR_NAME[-OB_ERR_RESTORE_POINT_NOT_EXIST] = "OB_ERR_RESTORE_POINT_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_RESTORE_POINT_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_RESTORE_POINT_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_RESTORE_POINT_NOT_EXIST] = "Restore point %s does not exist";
    STR_USER_ERROR[-OB_ERR_RESTORE_POINT_NOT_EXIST] = "Restore point %s does not exist";
    ORACLE_ERRNO[-OB_ERR_RESTORE_POINT_NOT_EXIST] = 38780;
    ORACLE_STR_ERROR[-OB_ERR_RESTORE_POINT_NOT_EXIST] = "ORA-38780: Restore point %s does not exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_RESTORE_POINT_NOT_EXIST] = "ORA-38780: Restore point %s does not exists";
    ERROR_NAME[-OB_ERR_BACKUP_POINT_EXIST] = "OB_ERR_BACKUP_POINT_EXIST";
    MYSQL_ERRNO[-OB_ERR_BACKUP_POINT_EXIST] = -1;
    SQLSTATE[-OB_ERR_BACKUP_POINT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_BACKUP_POINT_EXIST] = "Backup point %s already exists";
    STR_USER_ERROR[-OB_ERR_BACKUP_POINT_EXIST] = "Backup point %s already exists";
    ORACLE_ERRNO[-OB_ERR_BACKUP_POINT_EXIST] = 38778;
    ORACLE_STR_ERROR[-OB_ERR_BACKUP_POINT_EXIST] = "ORA-38778: Restore point %s already exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_BACKUP_POINT_EXIST] = "ORA-38778: Restore point %s already exists";
    ERROR_NAME[-OB_ERR_BACKUP_POINT_NOT_EXIST] = "OB_ERR_BACKUP_POINT_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_BACKUP_POINT_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_BACKUP_POINT_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_BACKUP_POINT_NOT_EXIST] = "Backup point %s does not exist";
    STR_USER_ERROR[-OB_ERR_BACKUP_POINT_NOT_EXIST] = "Backup point %s does not exist";
    ORACLE_ERRNO[-OB_ERR_BACKUP_POINT_NOT_EXIST] = 38780;
    ORACLE_STR_ERROR[-OB_ERR_BACKUP_POINT_NOT_EXIST] = "ORA-38780: Restore point %s does not exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_BACKUP_POINT_NOT_EXIST] = "ORA-38780: Restore point %s does not exists";
    ERROR_NAME[-OB_ERR_RESTORE_POINT_TOO_MANY] = "OB_ERR_RESTORE_POINT_TOO_MANY";
    MYSQL_ERRNO[-OB_ERR_RESTORE_POINT_TOO_MANY] = -1;
    SQLSTATE[-OB_ERR_RESTORE_POINT_TOO_MANY] = "HY000";
    STR_ERROR[-OB_ERR_RESTORE_POINT_TOO_MANY] = "cannot create restore point - too many restore points";
    STR_USER_ERROR[-OB_ERR_RESTORE_POINT_TOO_MANY] = "cannot create restore point - too many restore points";
    ORACLE_ERRNO[-OB_ERR_RESTORE_POINT_TOO_MANY] = 38779;
    ORACLE_STR_ERROR[-OB_ERR_RESTORE_POINT_TOO_MANY] =
        "ORA-38779: cannot create restore point - too many restore points";
    ORACLE_STR_USER_ERROR[-OB_ERR_RESTORE_POINT_TOO_MANY] =
        "ORA-38779: cannot create restore point - too many restore points";
    ERROR_NAME[-OB_TRANS_XA_BRANCH_FAIL] = "OB_TRANS_XA_BRANCH_FAIL";
    MYSQL_ERRNO[-OB_TRANS_XA_BRANCH_FAIL] = -1;
    SQLSTATE[-OB_TRANS_XA_BRANCH_FAIL] = "HY000";
    STR_ERROR[-OB_TRANS_XA_BRANCH_FAIL] = "another session or branch in same transaction failed or finalized";
    STR_USER_ERROR[-OB_TRANS_XA_BRANCH_FAIL] = "another session or branch in same transaction failed or finalized";
    ORACLE_ERRNO[-OB_TRANS_XA_BRANCH_FAIL] = 2051;
    ORACLE_STR_ERROR[-OB_TRANS_XA_BRANCH_FAIL] =
        "ORA-02051: another session or branch in same transaction failed or finalized";
    ORACLE_STR_USER_ERROR[-OB_TRANS_XA_BRANCH_FAIL] =
        "ORA-02051: another session or branch in same transaction failed or finalized";
    ERROR_NAME[-OB_LOG_ID_NOT_FOUND] = "OB_LOG_ID_NOT_FOUND";
    MYSQL_ERRNO[-OB_LOG_ID_NOT_FOUND] = -1;
    SQLSTATE[-OB_LOG_ID_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_LOG_ID_NOT_FOUND] = "log id not found";
    STR_USER_ERROR[-OB_LOG_ID_NOT_FOUND] = "log id not found";
    ORACLE_ERRNO[-OB_LOG_ID_NOT_FOUND] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ID_NOT_FOUND] = "ORA-00600: internal error code, arguments: -6301, log id not found";
    ORACLE_STR_USER_ERROR[-OB_LOG_ID_NOT_FOUND] = "ORA-00600: internal error code, arguments: -6301, log id not found";
    ERROR_NAME[-OB_LSR_THREAD_STOPPED] = "OB_LSR_THREAD_STOPPED";
    MYSQL_ERRNO[-OB_LSR_THREAD_STOPPED] = -1;
    SQLSTATE[-OB_LSR_THREAD_STOPPED] = "HY000";
    STR_ERROR[-OB_LSR_THREAD_STOPPED] = "log scan runnable thread stop";
    STR_USER_ERROR[-OB_LSR_THREAD_STOPPED] = "log scan runnable thread stop";
    ORACLE_ERRNO[-OB_LSR_THREAD_STOPPED] = 600;
    ORACLE_STR_ERROR[-OB_LSR_THREAD_STOPPED] =
        "ORA-00600: internal error code, arguments: -6302, log scan runnable thread stop";
    ORACLE_STR_USER_ERROR[-OB_LSR_THREAD_STOPPED] =
        "ORA-00600: internal error code, arguments: -6302, log scan runnable thread stop";
    ERROR_NAME[-OB_NO_LOG] = "OB_NO_LOG";
    MYSQL_ERRNO[-OB_NO_LOG] = -1;
    SQLSTATE[-OB_NO_LOG] = "HY000";
    STR_ERROR[-OB_NO_LOG] = "no log ever scanned";
    STR_USER_ERROR[-OB_NO_LOG] = "no log ever scanned";
    ORACLE_ERRNO[-OB_NO_LOG] = 600;
    ORACLE_STR_ERROR[-OB_NO_LOG] = "ORA-00600: internal error code, arguments: -6303, no log ever scanned";
    ORACLE_STR_USER_ERROR[-OB_NO_LOG] = "ORA-00600: internal error code, arguments: -6303, no log ever scanned";
    ERROR_NAME[-OB_LOG_ID_RANGE_ERROR] = "OB_LOG_ID_RANGE_ERROR";
    MYSQL_ERRNO[-OB_LOG_ID_RANGE_ERROR] = -1;
    SQLSTATE[-OB_LOG_ID_RANGE_ERROR] = "HY000";
    STR_ERROR[-OB_LOG_ID_RANGE_ERROR] = "log id range error";
    STR_USER_ERROR[-OB_LOG_ID_RANGE_ERROR] = "log id range error";
    ORACLE_ERRNO[-OB_LOG_ID_RANGE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ID_RANGE_ERROR] = "ORA-00600: internal error code, arguments: -6304, log id range error";
    ORACLE_STR_USER_ERROR[-OB_LOG_ID_RANGE_ERROR] =
        "ORA-00600: internal error code, arguments: -6304, log id range error";
    ERROR_NAME[-OB_LOG_ITER_ENOUGH] = "OB_LOG_ITER_ENOUGH";
    MYSQL_ERRNO[-OB_LOG_ITER_ENOUGH] = -1;
    SQLSTATE[-OB_LOG_ITER_ENOUGH] = "HY000";
    STR_ERROR[-OB_LOG_ITER_ENOUGH] = "iter scans enough files";
    STR_USER_ERROR[-OB_LOG_ITER_ENOUGH] = "iter scans enough files";
    ORACLE_ERRNO[-OB_LOG_ITER_ENOUGH] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ITER_ENOUGH] = "ORA-00600: internal error code, arguments: -6305, iter scans enough files";
    ORACLE_STR_USER_ERROR[-OB_LOG_ITER_ENOUGH] =
        "ORA-00600: internal error code, arguments: -6305, iter scans enough files";
    ERROR_NAME[-OB_CLOG_INVALID_ACK] = "OB_CLOG_INVALID_ACK";
    MYSQL_ERRNO[-OB_CLOG_INVALID_ACK] = -1;
    SQLSTATE[-OB_CLOG_INVALID_ACK] = "HY000";
    STR_ERROR[-OB_CLOG_INVALID_ACK] = "invalid ack msg";
    STR_USER_ERROR[-OB_CLOG_INVALID_ACK] = "invalid ack msg";
    ORACLE_ERRNO[-OB_CLOG_INVALID_ACK] = 600;
    ORACLE_STR_ERROR[-OB_CLOG_INVALID_ACK] = "ORA-00600: internal error code, arguments: -6306, invalid ack msg";
    ORACLE_STR_USER_ERROR[-OB_CLOG_INVALID_ACK] = "ORA-00600: internal error code, arguments: -6306, invalid ack msg";
    ERROR_NAME[-OB_CLOG_CACHE_INVALID] = "OB_CLOG_CACHE_INVALID";
    MYSQL_ERRNO[-OB_CLOG_CACHE_INVALID] = -1;
    SQLSTATE[-OB_CLOG_CACHE_INVALID] = "HY000";
    STR_ERROR[-OB_CLOG_CACHE_INVALID] = "clog cache invalid";
    STR_USER_ERROR[-OB_CLOG_CACHE_INVALID] = "clog cache invalid";
    ORACLE_ERRNO[-OB_CLOG_CACHE_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_CLOG_CACHE_INVALID] = "ORA-00600: internal error code, arguments: -6307, clog cache invalid";
    ORACLE_STR_USER_ERROR[-OB_CLOG_CACHE_INVALID] =
        "ORA-00600: internal error code, arguments: -6307, clog cache invalid";
    ERROR_NAME[-OB_EXT_HANDLE_UNFINISH] = "OB_EXT_HANDLE_UNFINISH";
    MYSQL_ERRNO[-OB_EXT_HANDLE_UNFINISH] = -1;
    SQLSTATE[-OB_EXT_HANDLE_UNFINISH] = "HY000";
    STR_ERROR[-OB_EXT_HANDLE_UNFINISH] = "external executor handle do not finish";
    STR_USER_ERROR[-OB_EXT_HANDLE_UNFINISH] = "external executor handle do not finish";
    ORACLE_ERRNO[-OB_EXT_HANDLE_UNFINISH] = 600;
    ORACLE_STR_ERROR[-OB_EXT_HANDLE_UNFINISH] =
        "ORA-00600: internal error code, arguments: -6308, external executor handle do not finish";
    ORACLE_STR_USER_ERROR[-OB_EXT_HANDLE_UNFINISH] =
        "ORA-00600: internal error code, arguments: -6308, external executor handle do not finish";
    ERROR_NAME[-OB_CURSOR_NOT_EXIST] = "OB_CURSOR_NOT_EXIST";
    MYSQL_ERRNO[-OB_CURSOR_NOT_EXIST] = -1;
    SQLSTATE[-OB_CURSOR_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_CURSOR_NOT_EXIST] = "cursor not exist";
    STR_USER_ERROR[-OB_CURSOR_NOT_EXIST] = "cursor not exist";
    ORACLE_ERRNO[-OB_CURSOR_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_CURSOR_NOT_EXIST] = "ORA-00600: internal error code, arguments: -6309, cursor not exist";
    ORACLE_STR_USER_ERROR[-OB_CURSOR_NOT_EXIST] = "ORA-00600: internal error code, arguments: -6309, cursor not exist";
    ERROR_NAME[-OB_STREAM_NOT_EXIST] = "OB_STREAM_NOT_EXIST";
    MYSQL_ERRNO[-OB_STREAM_NOT_EXIST] = -1;
    SQLSTATE[-OB_STREAM_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_STREAM_NOT_EXIST] = "stream not exist";
    STR_USER_ERROR[-OB_STREAM_NOT_EXIST] = "stream not exist";
    ORACLE_ERRNO[-OB_STREAM_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_STREAM_NOT_EXIST] = "ORA-00600: internal error code, arguments: -6310, stream not exist";
    ORACLE_STR_USER_ERROR[-OB_STREAM_NOT_EXIST] = "ORA-00600: internal error code, arguments: -6310, stream not exist";
    ERROR_NAME[-OB_STREAM_BUSY] = "OB_STREAM_BUSY";
    MYSQL_ERRNO[-OB_STREAM_BUSY] = -1;
    SQLSTATE[-OB_STREAM_BUSY] = "HY000";
    STR_ERROR[-OB_STREAM_BUSY] = "stream busy";
    STR_USER_ERROR[-OB_STREAM_BUSY] = "stream busy";
    ORACLE_ERRNO[-OB_STREAM_BUSY] = 600;
    ORACLE_STR_ERROR[-OB_STREAM_BUSY] = "ORA-00600: internal error code, arguments: -6311, stream busy";
    ORACLE_STR_USER_ERROR[-OB_STREAM_BUSY] = "ORA-00600: internal error code, arguments: -6311, stream busy";
    ERROR_NAME[-OB_FILE_RECYCLED] = "OB_FILE_RECYCLED";
    MYSQL_ERRNO[-OB_FILE_RECYCLED] = -1;
    SQLSTATE[-OB_FILE_RECYCLED] = "HY000";
    STR_ERROR[-OB_FILE_RECYCLED] = "file recycled";
    STR_USER_ERROR[-OB_FILE_RECYCLED] = "file recycled";
    ORACLE_ERRNO[-OB_FILE_RECYCLED] = 600;
    ORACLE_STR_ERROR[-OB_FILE_RECYCLED] = "ORA-00600: internal error code, arguments: -6312, file recycled";
    ORACLE_STR_USER_ERROR[-OB_FILE_RECYCLED] = "ORA-00600: internal error code, arguments: -6312, file recycled";
    ERROR_NAME[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] = "OB_REPLAY_EAGAIN_TOO_MUCH_TIME";
    MYSQL_ERRNO[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] = -1;
    SQLSTATE[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] = "HY000";
    STR_ERROR[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] = "replay eagain cost too much time";
    STR_USER_ERROR[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] = "replay eagain cost too much time";
    ORACLE_ERRNO[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] = 600;
    ORACLE_STR_ERROR[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] =
        "ORA-00600: internal error code, arguments: -6313, replay eagain cost too much time";
    ORACLE_STR_USER_ERROR[-OB_REPLAY_EAGAIN_TOO_MUCH_TIME] =
        "ORA-00600: internal error code, arguments: -6313, replay eagain cost too much time";
    ERROR_NAME[-OB_MEMBER_CHANGE_FAILED] = "OB_MEMBER_CHANGE_FAILED";
    MYSQL_ERRNO[-OB_MEMBER_CHANGE_FAILED] = -1;
    SQLSTATE[-OB_MEMBER_CHANGE_FAILED] = "HY000";
    STR_ERROR[-OB_MEMBER_CHANGE_FAILED] = "member change log sync failed";
    STR_USER_ERROR[-OB_MEMBER_CHANGE_FAILED] = "member change log sync failed";
    ORACLE_ERRNO[-OB_MEMBER_CHANGE_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_MEMBER_CHANGE_FAILED] =
        "ORA-00600: internal error code, arguments: -6314, member change log sync failed";
    ORACLE_STR_USER_ERROR[-OB_MEMBER_CHANGE_FAILED] =
        "ORA-00600: internal error code, arguments: -6314, member change log sync failed";
    ERROR_NAME[-OB_NO_NEED_BATCH_CTX] = "OB_NO_NEED_BATCH_CTX";
    MYSQL_ERRNO[-OB_NO_NEED_BATCH_CTX] = -1;
    SQLSTATE[-OB_NO_NEED_BATCH_CTX] = "HY000";
    STR_ERROR[-OB_NO_NEED_BATCH_CTX] = "no need batch ctx";
    STR_USER_ERROR[-OB_NO_NEED_BATCH_CTX] = "no need batch ctx";
    ORACLE_ERRNO[-OB_NO_NEED_BATCH_CTX] = 600;
    ORACLE_STR_ERROR[-OB_NO_NEED_BATCH_CTX] = "ORA-00600: internal error code, arguments: -6315, no need batch ctx";
    ORACLE_STR_USER_ERROR[-OB_NO_NEED_BATCH_CTX] =
        "ORA-00600: internal error code, arguments: -6315, no need batch ctx";
    ERROR_NAME[-OB_TOO_LARGE_LOG_ID] = "OB_TOO_LARGE_LOG_ID";
    MYSQL_ERRNO[-OB_TOO_LARGE_LOG_ID] = -1;
    SQLSTATE[-OB_TOO_LARGE_LOG_ID] = "HY000";
    STR_ERROR[-OB_TOO_LARGE_LOG_ID] = "too large log id";
    STR_USER_ERROR[-OB_TOO_LARGE_LOG_ID] = "too large log id";
    ORACLE_ERRNO[-OB_TOO_LARGE_LOG_ID] = 600;
    ORACLE_STR_ERROR[-OB_TOO_LARGE_LOG_ID] = "ORA-00600: internal error code, arguments: -6316, too large log id";
    ORACLE_STR_USER_ERROR[-OB_TOO_LARGE_LOG_ID] = "ORA-00600: internal error code, arguments: -6316, too large log id";
    ERROR_NAME[-OB_ALLOC_LOG_ID_NEED_RETRY] = "OB_ALLOC_LOG_ID_NEED_RETRY";
    MYSQL_ERRNO[-OB_ALLOC_LOG_ID_NEED_RETRY] = -1;
    SQLSTATE[-OB_ALLOC_LOG_ID_NEED_RETRY] = "HY000";
    STR_ERROR[-OB_ALLOC_LOG_ID_NEED_RETRY] = "alloc log id need retry";
    STR_USER_ERROR[-OB_ALLOC_LOG_ID_NEED_RETRY] = "alloc log id need retry";
    ORACLE_ERRNO[-OB_ALLOC_LOG_ID_NEED_RETRY] = 600;
    ORACLE_STR_ERROR[-OB_ALLOC_LOG_ID_NEED_RETRY] =
        "ORA-00600: internal error code, arguments: -6317, alloc log id need retry";
    ORACLE_STR_USER_ERROR[-OB_ALLOC_LOG_ID_NEED_RETRY] =
        "ORA-00600: internal error code, arguments: -6317, alloc log id need retry";
    ERROR_NAME[-OB_TRANS_ONE_PC_NOT_ALLOWED] = "OB_TRANS_ONE_PC_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_TRANS_ONE_PC_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_TRANS_ONE_PC_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_TRANS_ONE_PC_NOT_ALLOWED] = "transaction one pc not allowed";
    STR_USER_ERROR[-OB_TRANS_ONE_PC_NOT_ALLOWED] = "transaction one pc not allowed";
    ORACLE_ERRNO[-OB_TRANS_ONE_PC_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_TRANS_ONE_PC_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -6318, transaction one pc not allowed";
    ORACLE_STR_USER_ERROR[-OB_TRANS_ONE_PC_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -6318, transaction one pc not allowed";
    ERROR_NAME[-OB_LOG_NEED_REBUILD] = "OB_LOG_NEED_REBUILD";
    MYSQL_ERRNO[-OB_LOG_NEED_REBUILD] = -1;
    SQLSTATE[-OB_LOG_NEED_REBUILD] = "HY000";
    STR_ERROR[-OB_LOG_NEED_REBUILD] = "need rebuild";
    STR_USER_ERROR[-OB_LOG_NEED_REBUILD] = "need rebuild";
    ORACLE_ERRNO[-OB_LOG_NEED_REBUILD] = 600;
    ORACLE_STR_ERROR[-OB_LOG_NEED_REBUILD] = "ORA-00600: internal error code, arguments: -6319, need rebuild";
    ORACLE_STR_USER_ERROR[-OB_LOG_NEED_REBUILD] = "ORA-00600: internal error code, arguments: -6319, need rebuild";
    ERROR_NAME[-OB_TOO_MANY_LOG_TASK] = "OB_TOO_MANY_LOG_TASK";
    MYSQL_ERRNO[-OB_TOO_MANY_LOG_TASK] = -1;
    SQLSTATE[-OB_TOO_MANY_LOG_TASK] = "HY000";
    STR_ERROR[-OB_TOO_MANY_LOG_TASK] = "too many log tasks";
    STR_USER_ERROR[-OB_TOO_MANY_LOG_TASK] = "too many log tasks";
    ORACLE_ERRNO[-OB_TOO_MANY_LOG_TASK] = 600;
    ORACLE_STR_ERROR[-OB_TOO_MANY_LOG_TASK] = "ORA-00600: internal error code, arguments: -6320, too many log tasks";
    ORACLE_STR_USER_ERROR[-OB_TOO_MANY_LOG_TASK] =
        "ORA-00600: internal error code, arguments: -6320, too many log tasks";
    ERROR_NAME[-OB_INVALID_BATCH_SIZE] = "OB_INVALID_BATCH_SIZE";
    MYSQL_ERRNO[-OB_INVALID_BATCH_SIZE] = -1;
    SQLSTATE[-OB_INVALID_BATCH_SIZE] = "HY000";
    STR_ERROR[-OB_INVALID_BATCH_SIZE] = "ob invalid batch size";
    STR_USER_ERROR[-OB_INVALID_BATCH_SIZE] = "ob invalid batch size";
    ORACLE_ERRNO[-OB_INVALID_BATCH_SIZE] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_BATCH_SIZE] =
        "ORA-00600: internal error code, arguments: -6321, ob invalid batch size";
    ORACLE_STR_USER_ERROR[-OB_INVALID_BATCH_SIZE] =
        "ORA-00600: internal error code, arguments: -6321, ob invalid batch size";
    ERROR_NAME[-OB_CLOG_SLIDE_TIMEOUT] = "OB_CLOG_SLIDE_TIMEOUT";
    MYSQL_ERRNO[-OB_CLOG_SLIDE_TIMEOUT] = -1;
    SQLSTATE[-OB_CLOG_SLIDE_TIMEOUT] = "HY000";
    STR_ERROR[-OB_CLOG_SLIDE_TIMEOUT] = "ob clog slide timeout";
    STR_USER_ERROR[-OB_CLOG_SLIDE_TIMEOUT] = "ob clog slide timeout";
    ORACLE_ERRNO[-OB_CLOG_SLIDE_TIMEOUT] = 600;
    ORACLE_STR_ERROR[-OB_CLOG_SLIDE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -6322, ob clog slide timeout";
    ORACLE_STR_USER_ERROR[-OB_CLOG_SLIDE_TIMEOUT] =
        "ORA-00600: internal error code, arguments: -6322, ob clog slide timeout";
    ERROR_NAME[-OB_ELECTION_WARN_LOGBUF_FULL] = "OB_ELECTION_WARN_LOGBUF_FULL";
    MYSQL_ERRNO[-OB_ELECTION_WARN_LOGBUF_FULL] = -1;
    SQLSTATE[-OB_ELECTION_WARN_LOGBUF_FULL] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_LOGBUF_FULL] = "The log buffer is full";
    STR_USER_ERROR[-OB_ELECTION_WARN_LOGBUF_FULL] = "The log buffer is full";
    ORACLE_ERRNO[-OB_ELECTION_WARN_LOGBUF_FULL] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_LOGBUF_FULL] =
        "ORA-00600: internal error code, arguments: -7000, The log buffer is full";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_LOGBUF_FULL] =
        "ORA-00600: internal error code, arguments: -7000, The log buffer is full";
    ERROR_NAME[-OB_ELECTION_WARN_LOGBUF_EMPTY] = "OB_ELECTION_WARN_LOGBUF_EMPTY";
    MYSQL_ERRNO[-OB_ELECTION_WARN_LOGBUF_EMPTY] = -1;
    SQLSTATE[-OB_ELECTION_WARN_LOGBUF_EMPTY] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_LOGBUF_EMPTY] = "The log buffer is empty";
    STR_USER_ERROR[-OB_ELECTION_WARN_LOGBUF_EMPTY] = "The log buffer is empty";
    ORACLE_ERRNO[-OB_ELECTION_WARN_LOGBUF_EMPTY] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_LOGBUF_EMPTY] =
        "ORA-00600: internal error code, arguments: -7001, The log buffer is empty";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_LOGBUF_EMPTY] =
        "ORA-00600: internal error code, arguments: -7001, The log buffer is empty";
    ERROR_NAME[-OB_ELECTION_WARN_NOT_RUNNING] = "OB_ELECTION_WARN_NOT_RUNNING";
    MYSQL_ERRNO[-OB_ELECTION_WARN_NOT_RUNNING] = -1;
    SQLSTATE[-OB_ELECTION_WARN_NOT_RUNNING] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_NOT_RUNNING] = "The object is not running";
    STR_USER_ERROR[-OB_ELECTION_WARN_NOT_RUNNING] = "The object is not running";
    ORACLE_ERRNO[-OB_ELECTION_WARN_NOT_RUNNING] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_NOT_RUNNING] =
        "ORA-00600: internal error code, arguments: -7002, The object is not running";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_NOT_RUNNING] =
        "ORA-00600: internal error code, arguments: -7002, The object is not running";
    ERROR_NAME[-OB_ELECTION_WARN_IS_RUNNING] = "OB_ELECTION_WARN_IS_RUNNING";
    MYSQL_ERRNO[-OB_ELECTION_WARN_IS_RUNNING] = -1;
    SQLSTATE[-OB_ELECTION_WARN_IS_RUNNING] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_IS_RUNNING] = "The object is running";
    STR_USER_ERROR[-OB_ELECTION_WARN_IS_RUNNING] = "The object is running";
    ORACLE_ERRNO[-OB_ELECTION_WARN_IS_RUNNING] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_IS_RUNNING] =
        "ORA-00600: internal error code, arguments: -7003, The object is running";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_IS_RUNNING] =
        "ORA-00600: internal error code, arguments: -7003, The object is running";
    ERROR_NAME[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] = "OB_ELECTION_WARN_NOT_REACH_MAJORITY";
    MYSQL_ERRNO[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] = -1;
    SQLSTATE[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] = "Election does not reach majority";
    STR_USER_ERROR[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] = "Election does not reach majority";
    ORACLE_ERRNO[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] =
        "ORA-00600: internal error code, arguments: -7004, Election does not reach majority";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_NOT_REACH_MAJORITY] =
        "ORA-00600: internal error code, arguments: -7004, Election does not reach majority";
    ERROR_NAME[-OB_ELECTION_WARN_INVALID_SERVER] = "OB_ELECTION_WARN_INVALID_SERVER";
    MYSQL_ERRNO[-OB_ELECTION_WARN_INVALID_SERVER] = -1;
    SQLSTATE[-OB_ELECTION_WARN_INVALID_SERVER] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_INVALID_SERVER] = "The server is not valid";
    STR_USER_ERROR[-OB_ELECTION_WARN_INVALID_SERVER] = "The server is not valid";
    ORACLE_ERRNO[-OB_ELECTION_WARN_INVALID_SERVER] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_INVALID_SERVER] =
        "ORA-00600: internal error code, arguments: -7005, The server is not valid";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_INVALID_SERVER] =
        "ORA-00600: internal error code, arguments: -7005, The server is not valid";
    ERROR_NAME[-OB_ELECTION_WARN_INVALID_LEADER] = "OB_ELECTION_WARN_INVALID_LEADER";
    MYSQL_ERRNO[-OB_ELECTION_WARN_INVALID_LEADER] = -1;
    SQLSTATE[-OB_ELECTION_WARN_INVALID_LEADER] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_INVALID_LEADER] = "The leader is not valid";
    STR_USER_ERROR[-OB_ELECTION_WARN_INVALID_LEADER] = "The leader is not valid";
    ORACLE_ERRNO[-OB_ELECTION_WARN_INVALID_LEADER] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_INVALID_LEADER] =
        "ORA-00600: internal error code, arguments: -7006, The leader is not valid";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_INVALID_LEADER] =
        "ORA-00600: internal error code, arguments: -7006, The leader is not valid";
    ERROR_NAME[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] = "OB_ELECTION_WARN_LEADER_LEASE_EXPIRED";
    MYSQL_ERRNO[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] = -1;
    SQLSTATE[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] = "The leader lease is expired";
    STR_USER_ERROR[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] = "The leader lease is expired";
    ORACLE_ERRNO[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] =
        "ORA-00600: internal error code, arguments: -7007, The leader lease is expired";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_LEADER_LEASE_EXPIRED] =
        "ORA-00600: internal error code, arguments: -7007, The leader lease is expired";
    ERROR_NAME[-OB_ELECTION_WARN_INVALID_MESSAGE] = "OB_ELECTION_WARN_INVALID_MESSAGE";
    MYSQL_ERRNO[-OB_ELECTION_WARN_INVALID_MESSAGE] = -1;
    SQLSTATE[-OB_ELECTION_WARN_INVALID_MESSAGE] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_INVALID_MESSAGE] = "The message is not valid";
    STR_USER_ERROR[-OB_ELECTION_WARN_INVALID_MESSAGE] = "The message is not valid";
    ORACLE_ERRNO[-OB_ELECTION_WARN_INVALID_MESSAGE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_INVALID_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7010, The message is not valid";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_INVALID_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7010, The message is not valid";
    ERROR_NAME[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] = "OB_ELECTION_WARN_MESSAGE_NOT_INTIME";
    MYSQL_ERRNO[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] = -1;
    SQLSTATE[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] = "The message is not intime";
    STR_USER_ERROR[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] = "The message is not intime";
    ORACLE_ERRNO[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] =
        "ORA-00600: internal error code, arguments: -7011, The message is not intime";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_MESSAGE_NOT_INTIME] =
        "ORA-00600: internal error code, arguments: -7011, The message is not intime";
    ERROR_NAME[-OB_ELECTION_WARN_NOT_CANDIDATE] = "OB_ELECTION_WARN_NOT_CANDIDATE";
    MYSQL_ERRNO[-OB_ELECTION_WARN_NOT_CANDIDATE] = -1;
    SQLSTATE[-OB_ELECTION_WARN_NOT_CANDIDATE] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE] = "The server is not candidate";
    STR_USER_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE] = "The server is not candidate";
    ORACLE_ERRNO[-OB_ELECTION_WARN_NOT_CANDIDATE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE] =
        "ORA-00600: internal error code, arguments: -7012, The server is not candidate";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE] =
        "ORA-00600: internal error code, arguments: -7012, The server is not candidate";
    ERROR_NAME[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] = "OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER";
    MYSQL_ERRNO[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] = -1;
    SQLSTATE[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] = "The server is not candidate or voter";
    STR_USER_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] = "The server is not candidate or voter";
    ORACLE_ERRNO[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] =
        "ORA-00600: internal error code, arguments: -7013, The server is not candidate or voter";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER] =
        "ORA-00600: internal error code, arguments: -7013, The server is not candidate or voter";
    ERROR_NAME[-OB_ELECTION_WARN_PROTOCOL_ERROR] = "OB_ELECTION_WARN_PROTOCOL_ERROR";
    MYSQL_ERRNO[-OB_ELECTION_WARN_PROTOCOL_ERROR] = -1;
    SQLSTATE[-OB_ELECTION_WARN_PROTOCOL_ERROR] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_PROTOCOL_ERROR] = "Election protocol error";
    STR_USER_ERROR[-OB_ELECTION_WARN_PROTOCOL_ERROR] = "Election protocol error";
    ORACLE_ERRNO[-OB_ELECTION_WARN_PROTOCOL_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_PROTOCOL_ERROR] =
        "ORA-00600: internal error code, arguments: -7014, Election protocol error";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_PROTOCOL_ERROR] =
        "ORA-00600: internal error code, arguments: -7014, Election protocol error";
    ERROR_NAME[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] = "OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE";
    MYSQL_ERRNO[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] = -1;
    SQLSTATE[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] = "The task run time out of range";
    STR_USER_ERROR[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] = "The task run time out of range";
    ORACLE_ERRNO[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] =
        "ORA-00600: internal error code, arguments: -7015, The task run time out of range";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE] =
        "ORA-00600: internal error code, arguments: -7015, The task run time out of range";
    ERROR_NAME[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] = "OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE";
    MYSQL_ERRNO[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] = -1;
    SQLSTATE[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] = "Last operation has not done";
    STR_USER_ERROR[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] = "Last operation has not done";
    ORACLE_ERRNO[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] =
        "ORA-00600: internal error code, arguments: -7021, Last operation has not done";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE] =
        "ORA-00600: internal error code, arguments: -7021, Last operation has not done";
    ERROR_NAME[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] = "OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER";
    MYSQL_ERRNO[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] = -1;
    SQLSTATE[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] = "Current server is not leader";
    STR_USER_ERROR[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] = "Current server is not leader";
    ORACLE_ERRNO[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] =
        "ORA-00600: internal error code, arguments: -7022, Current server is not leader";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER] =
        "ORA-00600: internal error code, arguments: -7022, Current server is not leader";
    ERROR_NAME[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] = "OB_ELECTION_WARN_NO_PREPARE_MESSAGE";
    MYSQL_ERRNO[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] = -1;
    SQLSTATE[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] = "There is not prepare message";
    STR_USER_ERROR[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] = "There is not prepare message";
    ORACLE_ERRNO[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7024, There is not prepare message";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_NO_PREPARE_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7024, There is not prepare message";
    ERROR_NAME[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] = "OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE";
    MYSQL_ERRNO[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] = -1;
    SQLSTATE[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] = "HY000";
    STR_ERROR[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] = "There is more than one prepare message";
    STR_USER_ERROR[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] = "There is more than one prepare message";
    ORACLE_ERRNO[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7025, There is more than one prepare message";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7025, There is more than one prepare message";
    ERROR_NAME[-OB_ELECTION_NOT_EXIST] = "OB_ELECTION_NOT_EXIST";
    MYSQL_ERRNO[-OB_ELECTION_NOT_EXIST] = -1;
    SQLSTATE[-OB_ELECTION_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ELECTION_NOT_EXIST] = "Election does not exist";
    STR_USER_ERROR[-OB_ELECTION_NOT_EXIST] = "Election does not exist";
    ORACLE_ERRNO[-OB_ELECTION_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -7026, Election does not exist";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -7026, Election does not exist";
    ERROR_NAME[-OB_ELECTION_MGR_IS_RUNNING] = "OB_ELECTION_MGR_IS_RUNNING";
    MYSQL_ERRNO[-OB_ELECTION_MGR_IS_RUNNING] = -1;
    SQLSTATE[-OB_ELECTION_MGR_IS_RUNNING] = "HY000";
    STR_ERROR[-OB_ELECTION_MGR_IS_RUNNING] = "Election manager is running";
    STR_USER_ERROR[-OB_ELECTION_MGR_IS_RUNNING] = "Election manager is running";
    ORACLE_ERRNO[-OB_ELECTION_MGR_IS_RUNNING] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_MGR_IS_RUNNING] =
        "ORA-00600: internal error code, arguments: -7027, Election manager is running";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_MGR_IS_RUNNING] =
        "ORA-00600: internal error code, arguments: -7027, Election manager is running";
    ERROR_NAME[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] = "OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE";
    MYSQL_ERRNO[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] = -1;
    SQLSTATE[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] = "Election msg pool not have majority prepare message";
    STR_USER_ERROR[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] =
        "Election msg pool not have majority prepare message";
    ORACLE_ERRNO[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7029, Election msg pool not have majority prepare message";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7029, Election msg pool not have majority prepare message";
    ERROR_NAME[-OB_ELECTION_ASYNC_LOG_WARN_INIT] = "OB_ELECTION_ASYNC_LOG_WARN_INIT";
    MYSQL_ERRNO[-OB_ELECTION_ASYNC_LOG_WARN_INIT] = -1;
    SQLSTATE[-OB_ELECTION_ASYNC_LOG_WARN_INIT] = "HY000";
    STR_ERROR[-OB_ELECTION_ASYNC_LOG_WARN_INIT] = "Election async log init error";
    STR_USER_ERROR[-OB_ELECTION_ASYNC_LOG_WARN_INIT] = "Election async log init error";
    ORACLE_ERRNO[-OB_ELECTION_ASYNC_LOG_WARN_INIT] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_ASYNC_LOG_WARN_INIT] =
        "ORA-00600: internal error code, arguments: -7030, Election async log init error";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_ASYNC_LOG_WARN_INIT] =
        "ORA-00600: internal error code, arguments: -7030, Election async log init error";
    ERROR_NAME[-OB_ELECTION_WAIT_LEADER_MESSAGE] = "OB_ELECTION_WAIT_LEADER_MESSAGE";
    MYSQL_ERRNO[-OB_ELECTION_WAIT_LEADER_MESSAGE] = -1;
    SQLSTATE[-OB_ELECTION_WAIT_LEADER_MESSAGE] = "HY000";
    STR_ERROR[-OB_ELECTION_WAIT_LEADER_MESSAGE] = "Election waiting leader message";
    STR_USER_ERROR[-OB_ELECTION_WAIT_LEADER_MESSAGE] = "Election waiting leader message";
    ORACLE_ERRNO[-OB_ELECTION_WAIT_LEADER_MESSAGE] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WAIT_LEADER_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7031, Election waiting leader message";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WAIT_LEADER_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7031, Election waiting leader message";
    ERROR_NAME[-OB_ELECTION_GROUP_NOT_EXIST] = "OB_ELECTION_GROUP_NOT_EXIST";
    MYSQL_ERRNO[-OB_ELECTION_GROUP_NOT_EXIST] = -1;
    SQLSTATE[-OB_ELECTION_GROUP_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ELECTION_GROUP_NOT_EXIST] = "Election group not exist";
    STR_USER_ERROR[-OB_ELECTION_GROUP_NOT_EXIST] = "Election group not exist";
    ORACLE_ERRNO[-OB_ELECTION_GROUP_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_GROUP_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -7032, Election group not exist";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_GROUP_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -7032, Election group not exist";
    ERROR_NAME[-OB_UNEXPECT_EG_VERSION] = "OB_UNEXPECT_EG_VERSION";
    MYSQL_ERRNO[-OB_UNEXPECT_EG_VERSION] = -1;
    SQLSTATE[-OB_UNEXPECT_EG_VERSION] = "HY000";
    STR_ERROR[-OB_UNEXPECT_EG_VERSION] = "unexpected eg_version";
    STR_USER_ERROR[-OB_UNEXPECT_EG_VERSION] = "unexpected eg_version";
    ORACLE_ERRNO[-OB_UNEXPECT_EG_VERSION] = 600;
    ORACLE_STR_ERROR[-OB_UNEXPECT_EG_VERSION] =
        "ORA-00600: internal error code, arguments: -7033, unexpected eg_version";
    ORACLE_STR_USER_ERROR[-OB_UNEXPECT_EG_VERSION] =
        "ORA-00600: internal error code, arguments: -7033, unexpected eg_version";
    ERROR_NAME[-OB_ELECTION_GROUP_MGR_IS_RUNNING] = "OB_ELECTION_GROUP_MGR_IS_RUNNING";
    MYSQL_ERRNO[-OB_ELECTION_GROUP_MGR_IS_RUNNING] = -1;
    SQLSTATE[-OB_ELECTION_GROUP_MGR_IS_RUNNING] = "HY000";
    STR_ERROR[-OB_ELECTION_GROUP_MGR_IS_RUNNING] = "election_group_mgr is running";
    STR_USER_ERROR[-OB_ELECTION_GROUP_MGR_IS_RUNNING] = "election_group_mgr is running";
    ORACLE_ERRNO[-OB_ELECTION_GROUP_MGR_IS_RUNNING] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_GROUP_MGR_IS_RUNNING] =
        "ORA-00600: internal error code, arguments: -7034, election_group_mgr is running";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_GROUP_MGR_IS_RUNNING] =
        "ORA-00600: internal error code, arguments: -7034, election_group_mgr is running";
    ERROR_NAME[-OB_ELECTION_MGR_NOT_RUNNING] = "OB_ELECTION_MGR_NOT_RUNNING";
    MYSQL_ERRNO[-OB_ELECTION_MGR_NOT_RUNNING] = -1;
    SQLSTATE[-OB_ELECTION_MGR_NOT_RUNNING] = "HY000";
    STR_ERROR[-OB_ELECTION_MGR_NOT_RUNNING] = "Election manager is not running";
    STR_USER_ERROR[-OB_ELECTION_MGR_NOT_RUNNING] = "Election manager is not running";
    ORACLE_ERRNO[-OB_ELECTION_MGR_NOT_RUNNING] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_MGR_NOT_RUNNING] =
        "ORA-00600: internal error code, arguments: -7035, Election manager is not running";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_MGR_NOT_RUNNING] =
        "ORA-00600: internal error code, arguments: -7035, Election manager is not running";
    ERROR_NAME[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] = "OB_ELECTION_ERROR_VOTE_MSG_CONFLICT";
    MYSQL_ERRNO[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] = -1;
    SQLSTATE[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] = "HY000";
    STR_ERROR[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] = "Receive change leader msg and vote msg with same T1 timestamp";
    STR_USER_ERROR[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] =
        "Receive change leader msg and vote msg with same T1 timestamp";
    ORACLE_ERRNO[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] =
        "ORA-00600: internal error code, arguments: -7036, Receive change leader msg and vote msg with same T1 "
        "timestamp";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_ERROR_VOTE_MSG_CONFLICT] =
        "ORA-00600: internal error code, arguments: -7036, Receive change leader msg and vote msg with same T1 "
        "timestamp";
    ERROR_NAME[-OB_ELECTION_ERROR_DUPLICATED_MSG] = "OB_ELECTION_ERROR_DUPLICATED_MSG";
    MYSQL_ERRNO[-OB_ELECTION_ERROR_DUPLICATED_MSG] = -1;
    SQLSTATE[-OB_ELECTION_ERROR_DUPLICATED_MSG] = "HY000";
    STR_ERROR[-OB_ELECTION_ERROR_DUPLICATED_MSG] = "Receive duplicated prepare/vote msg with same T1 timestamp";
    STR_USER_ERROR[-OB_ELECTION_ERROR_DUPLICATED_MSG] = "Receive duplicated prepare/vote msg with same T1 timestamp";
    ORACLE_ERRNO[-OB_ELECTION_ERROR_DUPLICATED_MSG] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_ERROR_DUPLICATED_MSG] =
        "ORA-00600: internal error code, arguments: -7037, Receive duplicated prepare/vote msg with same T1 timestamp";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_ERROR_DUPLICATED_MSG] =
        "ORA-00600: internal error code, arguments: -7037, Receive duplicated prepare/vote msg with same T1 timestamp";
    ERROR_NAME[-OB_ELECTION_WARN_T1_NOT_MATCH] = "OB_ELECTION_WARN_T1_NOT_MATCH";
    MYSQL_ERRNO[-OB_ELECTION_WARN_T1_NOT_MATCH] = -1;
    SQLSTATE[-OB_ELECTION_WARN_T1_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_ELECTION_WARN_T1_NOT_MATCH] = "T1 timestamp is not match";
    STR_USER_ERROR[-OB_ELECTION_WARN_T1_NOT_MATCH] = "T1 timestamp is not match";
    ORACLE_ERRNO[-OB_ELECTION_WARN_T1_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_ELECTION_WARN_T1_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -7038, T1 timestamp is not match";
    ORACLE_STR_USER_ERROR[-OB_ELECTION_WARN_T1_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -7038, T1 timestamp is not match";
    ERROR_NAME[-OB_TRANSFER_TASK_COMPLETED] = "OB_TRANSFER_TASK_COMPLETED";
    MYSQL_ERRNO[-OB_TRANSFER_TASK_COMPLETED] = -1;
    SQLSTATE[-OB_TRANSFER_TASK_COMPLETED] = "HY000";
    STR_ERROR[-OB_TRANSFER_TASK_COMPLETED] = "transfer task completed";
    STR_USER_ERROR[-OB_TRANSFER_TASK_COMPLETED] = "transfer task completed";
    ORACLE_ERRNO[-OB_TRANSFER_TASK_COMPLETED] = 600;
    ORACLE_STR_ERROR[-OB_TRANSFER_TASK_COMPLETED] =
        "ORA-00600: internal error code, arguments: -7100, transfer task completed";
    ORACLE_STR_USER_ERROR[-OB_TRANSFER_TASK_COMPLETED] =
        "ORA-00600: internal error code, arguments: -7100, transfer task completed";
    ERROR_NAME[-OB_TOO_MANY_TRANSFER_TASK] = "OB_TOO_MANY_TRANSFER_TASK";
    MYSQL_ERRNO[-OB_TOO_MANY_TRANSFER_TASK] = -1;
    SQLSTATE[-OB_TOO_MANY_TRANSFER_TASK] = "HY000";
    STR_ERROR[-OB_TOO_MANY_TRANSFER_TASK] = "too many transfer tasks";
    STR_USER_ERROR[-OB_TOO_MANY_TRANSFER_TASK] = "too many transfer tasks";
    ORACLE_ERRNO[-OB_TOO_MANY_TRANSFER_TASK] = 600;
    ORACLE_STR_ERROR[-OB_TOO_MANY_TRANSFER_TASK] =
        "ORA-00600: internal error code, arguments: -7101, too many transfer tasks";
    ORACLE_STR_USER_ERROR[-OB_TOO_MANY_TRANSFER_TASK] =
        "ORA-00600: internal error code, arguments: -7101, too many transfer tasks";
    ERROR_NAME[-OB_TRANSFER_TASK_EXIST] = "OB_TRANSFER_TASK_EXIST";
    MYSQL_ERRNO[-OB_TRANSFER_TASK_EXIST] = -1;
    SQLSTATE[-OB_TRANSFER_TASK_EXIST] = "HY000";
    STR_ERROR[-OB_TRANSFER_TASK_EXIST] = "transfer task exist";
    STR_USER_ERROR[-OB_TRANSFER_TASK_EXIST] = "transfer task exist";
    ORACLE_ERRNO[-OB_TRANSFER_TASK_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TRANSFER_TASK_EXIST] = "ORA-00600: internal error code, arguments: -7102, transfer task exist";
    ORACLE_STR_USER_ERROR[-OB_TRANSFER_TASK_EXIST] =
        "ORA-00600: internal error code, arguments: -7102, transfer task exist";
    ERROR_NAME[-OB_TRANSFER_TASK_NOT_EXIST] = "OB_TRANSFER_TASK_NOT_EXIST";
    MYSQL_ERRNO[-OB_TRANSFER_TASK_NOT_EXIST] = -1;
    SQLSTATE[-OB_TRANSFER_TASK_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_TRANSFER_TASK_NOT_EXIST] = "transfer task not exist";
    STR_USER_ERROR[-OB_TRANSFER_TASK_NOT_EXIST] = "transfer task not exist";
    ORACLE_ERRNO[-OB_TRANSFER_TASK_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_TRANSFER_TASK_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -7103, transfer task not exist";
    ORACLE_STR_USER_ERROR[-OB_TRANSFER_TASK_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -7103, transfer task not exist";
    ERROR_NAME[-OB_NOT_ALLOW_TO_REMOVE] = "OB_NOT_ALLOW_TO_REMOVE";
    MYSQL_ERRNO[-OB_NOT_ALLOW_TO_REMOVE] = -1;
    SQLSTATE[-OB_NOT_ALLOW_TO_REMOVE] = "HY000";
    STR_ERROR[-OB_NOT_ALLOW_TO_REMOVE] = "not allow to remove";
    STR_USER_ERROR[-OB_NOT_ALLOW_TO_REMOVE] = "not allow to remove";
    ORACLE_ERRNO[-OB_NOT_ALLOW_TO_REMOVE] = 600;
    ORACLE_STR_ERROR[-OB_NOT_ALLOW_TO_REMOVE] = "ORA-00600: internal error code, arguments: -7104, not allow to remove";
    ORACLE_STR_USER_ERROR[-OB_NOT_ALLOW_TO_REMOVE] =
        "ORA-00600: internal error code, arguments: -7104, not allow to remove";
    ERROR_NAME[-OB_RG_NOT_MATCH] = "OB_RG_NOT_MATCH";
    MYSQL_ERRNO[-OB_RG_NOT_MATCH] = -1;
    SQLSTATE[-OB_RG_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_RG_NOT_MATCH] = "replication group not match";
    STR_USER_ERROR[-OB_RG_NOT_MATCH] = "replication group not match";
    ORACLE_ERRNO[-OB_RG_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_RG_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -7105, replication group not match";
    ORACLE_STR_USER_ERROR[-OB_RG_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -7105, replication group not match";
    ERROR_NAME[-OB_TRANSFER_TASK_ABORTED] = "OB_TRANSFER_TASK_ABORTED";
    MYSQL_ERRNO[-OB_TRANSFER_TASK_ABORTED] = -1;
    SQLSTATE[-OB_TRANSFER_TASK_ABORTED] = "HY000";
    STR_ERROR[-OB_TRANSFER_TASK_ABORTED] = "transfer task aborted";
    STR_USER_ERROR[-OB_TRANSFER_TASK_ABORTED] = "transfer task aborted";
    ORACLE_ERRNO[-OB_TRANSFER_TASK_ABORTED] = 600;
    ORACLE_STR_ERROR[-OB_TRANSFER_TASK_ABORTED] =
        "ORA-00600: internal error code, arguments: -7106, transfer task aborted";
    ORACLE_STR_USER_ERROR[-OB_TRANSFER_TASK_ABORTED] =
        "ORA-00600: internal error code, arguments: -7106, transfer task aborted";
    ERROR_NAME[-OB_TRANSFER_INVALID_MESSAGE] = "OB_TRANSFER_INVALID_MESSAGE";
    MYSQL_ERRNO[-OB_TRANSFER_INVALID_MESSAGE] = -1;
    SQLSTATE[-OB_TRANSFER_INVALID_MESSAGE] = "HY000";
    STR_ERROR[-OB_TRANSFER_INVALID_MESSAGE] = "transfer invalid message";
    STR_USER_ERROR[-OB_TRANSFER_INVALID_MESSAGE] = "transfer invalid message";
    ORACLE_ERRNO[-OB_TRANSFER_INVALID_MESSAGE] = 600;
    ORACLE_STR_ERROR[-OB_TRANSFER_INVALID_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7107, transfer invalid message";
    ORACLE_STR_USER_ERROR[-OB_TRANSFER_INVALID_MESSAGE] =
        "ORA-00600: internal error code, arguments: -7107, transfer invalid message";
    ERROR_NAME[-OB_TRANSFER_CTX_TS_NOT_MATCH] = "OB_TRANSFER_CTX_TS_NOT_MATCH";
    MYSQL_ERRNO[-OB_TRANSFER_CTX_TS_NOT_MATCH] = -1;
    SQLSTATE[-OB_TRANSFER_CTX_TS_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_TRANSFER_CTX_TS_NOT_MATCH] = "transfer ctx_ts not match";
    STR_USER_ERROR[-OB_TRANSFER_CTX_TS_NOT_MATCH] = "transfer ctx_ts not match";
    ORACLE_ERRNO[-OB_TRANSFER_CTX_TS_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_TRANSFER_CTX_TS_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -7108, transfer ctx_ts not match";
    ORACLE_STR_USER_ERROR[-OB_TRANSFER_CTX_TS_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -7108, transfer ctx_ts not match";
    ERROR_NAME[-OB_SERVER_IS_INIT] = "OB_SERVER_IS_INIT";
    MYSQL_ERRNO[-OB_SERVER_IS_INIT] = -1;
    SQLSTATE[-OB_SERVER_IS_INIT] = "08004";
    STR_ERROR[-OB_SERVER_IS_INIT] = "Server is initializing";
    STR_USER_ERROR[-OB_SERVER_IS_INIT] = "Server is initializing";
    ORACLE_ERRNO[-OB_SERVER_IS_INIT] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_IS_INIT] = "ORA-00600: internal error code, arguments: -8001, Server is initializing";
    ORACLE_STR_USER_ERROR[-OB_SERVER_IS_INIT] =
        "ORA-00600: internal error code, arguments: -8001, Server is initializing";
    ERROR_NAME[-OB_SERVER_IS_STOPPING] = "OB_SERVER_IS_STOPPING";
    MYSQL_ERRNO[-OB_SERVER_IS_STOPPING] = -1;
    SQLSTATE[-OB_SERVER_IS_STOPPING] = "08004";
    STR_ERROR[-OB_SERVER_IS_STOPPING] = "Server is stopping";
    STR_USER_ERROR[-OB_SERVER_IS_STOPPING] = "Server is stopping";
    ORACLE_ERRNO[-OB_SERVER_IS_STOPPING] = 600;
    ORACLE_STR_ERROR[-OB_SERVER_IS_STOPPING] = "ORA-00600: internal error code, arguments: -8002, Server is stopping";
    ORACLE_STR_USER_ERROR[-OB_SERVER_IS_STOPPING] =
        "ORA-00600: internal error code, arguments: -8002, Server is stopping";
    ERROR_NAME[-OB_PACKET_CHECKSUM_ERROR] = "OB_PACKET_CHECKSUM_ERROR";
    MYSQL_ERRNO[-OB_PACKET_CHECKSUM_ERROR] = -1;
    SQLSTATE[-OB_PACKET_CHECKSUM_ERROR] = "08004";
    STR_ERROR[-OB_PACKET_CHECKSUM_ERROR] = "Packet checksum error";
    STR_USER_ERROR[-OB_PACKET_CHECKSUM_ERROR] = "Packet checksum error";
    ORACLE_ERRNO[-OB_PACKET_CHECKSUM_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_PACKET_CHECKSUM_ERROR] =
        "ORA-00600: internal error code, arguments: -8003, Packet checksum error";
    ORACLE_STR_USER_ERROR[-OB_PACKET_CHECKSUM_ERROR] =
        "ORA-00600: internal error code, arguments: -8003, Packet checksum error";
    ERROR_NAME[-OB_PACKET_CLUSTER_ID_NOT_MATCH] = "OB_PACKET_CLUSTER_ID_NOT_MATCH";
    MYSQL_ERRNO[-OB_PACKET_CLUSTER_ID_NOT_MATCH] = -1;
    SQLSTATE[-OB_PACKET_CLUSTER_ID_NOT_MATCH] = "08004";
    STR_ERROR[-OB_PACKET_CLUSTER_ID_NOT_MATCH] = "Packet cluster_id not match";
    STR_USER_ERROR[-OB_PACKET_CLUSTER_ID_NOT_MATCH] = "Packet cluster_id not match";
    ORACLE_ERRNO[-OB_PACKET_CLUSTER_ID_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_PACKET_CLUSTER_ID_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -8004, Packet cluster_id not match";
    ORACLE_STR_USER_ERROR[-OB_PACKET_CLUSTER_ID_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -8004, Packet cluster_id not match";
    ERROR_NAME[-OB_URI_ERROR] = "OB_URI_ERROR";
    MYSQL_ERRNO[-OB_URI_ERROR] = -1;
    SQLSTATE[-OB_URI_ERROR] = "HY000";
    STR_ERROR[-OB_URI_ERROR] = "URI error";
    STR_USER_ERROR[-OB_URI_ERROR] = "URI error";
    ORACLE_ERRNO[-OB_URI_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_URI_ERROR] = "ORA-00600: internal error code, arguments: -9001, URI error";
    ORACLE_STR_USER_ERROR[-OB_URI_ERROR] = "ORA-00600: internal error code, arguments: -9001, URI error";
    ERROR_NAME[-OB_FINAL_MD5_ERROR] = "OB_FINAL_MD5_ERROR";
    MYSQL_ERRNO[-OB_FINAL_MD5_ERROR] = -1;
    SQLSTATE[-OB_FINAL_MD5_ERROR] = "HY000";
    STR_ERROR[-OB_FINAL_MD5_ERROR] = "OSS file MD5 error";
    STR_USER_ERROR[-OB_FINAL_MD5_ERROR] = "OSS file MD5 error";
    ORACLE_ERRNO[-OB_FINAL_MD5_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_FINAL_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9002, OSS file MD5 error";
    ORACLE_STR_USER_ERROR[-OB_FINAL_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9002, OSS file MD5 error";
    ERROR_NAME[-OB_OSS_ERROR] = "OB_OSS_ERROR";
    MYSQL_ERRNO[-OB_OSS_ERROR] = -1;
    SQLSTATE[-OB_OSS_ERROR] = "HY000";
    STR_ERROR[-OB_OSS_ERROR] = "OSS error";
    STR_USER_ERROR[-OB_OSS_ERROR] = "OSS error";
    ORACLE_ERRNO[-OB_OSS_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_OSS_ERROR] = "ORA-00600: internal error code, arguments: -9003, OSS error";
    ORACLE_STR_USER_ERROR[-OB_OSS_ERROR] = "ORA-00600: internal error code, arguments: -9003, OSS error";
    ERROR_NAME[-OB_INIT_MD5_ERROR] = "OB_INIT_MD5_ERROR";
    MYSQL_ERRNO[-OB_INIT_MD5_ERROR] = -1;
    SQLSTATE[-OB_INIT_MD5_ERROR] = "HY000";
    STR_ERROR[-OB_INIT_MD5_ERROR] = "Init MD5 fail";
    STR_USER_ERROR[-OB_INIT_MD5_ERROR] = "Init MD5 fail";
    ORACLE_ERRNO[-OB_INIT_MD5_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_INIT_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9004, Init MD5 fail";
    ORACLE_STR_USER_ERROR[-OB_INIT_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9004, Init MD5 fail";
    ERROR_NAME[-OB_OUT_OF_ELEMENT] = "OB_OUT_OF_ELEMENT";
    MYSQL_ERRNO[-OB_OUT_OF_ELEMENT] = -1;
    SQLSTATE[-OB_OUT_OF_ELEMENT] = "HY000";
    STR_ERROR[-OB_OUT_OF_ELEMENT] = "Out of element";
    STR_USER_ERROR[-OB_OUT_OF_ELEMENT] = "Out of element";
    ORACLE_ERRNO[-OB_OUT_OF_ELEMENT] = 600;
    ORACLE_STR_ERROR[-OB_OUT_OF_ELEMENT] = "ORA-00600: internal error code, arguments: -9005, Out of element";
    ORACLE_STR_USER_ERROR[-OB_OUT_OF_ELEMENT] = "ORA-00600: internal error code, arguments: -9005, Out of element";
    ERROR_NAME[-OB_UPDATE_MD5_ERROR] = "OB_UPDATE_MD5_ERROR";
    MYSQL_ERRNO[-OB_UPDATE_MD5_ERROR] = -1;
    SQLSTATE[-OB_UPDATE_MD5_ERROR] = "HY000";
    STR_ERROR[-OB_UPDATE_MD5_ERROR] = "Update MD5 fail";
    STR_USER_ERROR[-OB_UPDATE_MD5_ERROR] = "Update MD5 fail";
    ORACLE_ERRNO[-OB_UPDATE_MD5_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_UPDATE_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9006, Update MD5 fail";
    ORACLE_STR_USER_ERROR[-OB_UPDATE_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9006, Update MD5 fail";
    ERROR_NAME[-OB_FILE_LENGTH_INVALID] = "OB_FILE_LENGTH_INVALID";
    MYSQL_ERRNO[-OB_FILE_LENGTH_INVALID] = -1;
    SQLSTATE[-OB_FILE_LENGTH_INVALID] = "HY000";
    STR_ERROR[-OB_FILE_LENGTH_INVALID] = "Invalid OSS file length";
    STR_USER_ERROR[-OB_FILE_LENGTH_INVALID] = "Invalid OSS file length";
    ORACLE_ERRNO[-OB_FILE_LENGTH_INVALID] = 600;
    ORACLE_STR_ERROR[-OB_FILE_LENGTH_INVALID] =
        "ORA-00600: internal error code, arguments: -9007, Invalid OSS file length";
    ORACLE_STR_USER_ERROR[-OB_FILE_LENGTH_INVALID] =
        "ORA-00600: internal error code, arguments: -9007, Invalid OSS file length";
    ERROR_NAME[-OB_NOT_READ_ALL_DATA] = "OB_NOT_READ_ALL_DATA";
    MYSQL_ERRNO[-OB_NOT_READ_ALL_DATA] = -1;
    SQLSTATE[-OB_NOT_READ_ALL_DATA] = "HY000";
    STR_ERROR[-OB_NOT_READ_ALL_DATA] = "Read all data fail";
    STR_USER_ERROR[-OB_NOT_READ_ALL_DATA] = "Read all data fail";
    ORACLE_ERRNO[-OB_NOT_READ_ALL_DATA] = 600;
    ORACLE_STR_ERROR[-OB_NOT_READ_ALL_DATA] = "ORA-00600: internal error code, arguments: -9008, Read all data fail";
    ORACLE_STR_USER_ERROR[-OB_NOT_READ_ALL_DATA] =
        "ORA-00600: internal error code, arguments: -9008, Read all data fail";
    ERROR_NAME[-OB_BUILD_MD5_ERROR] = "OB_BUILD_MD5_ERROR";
    MYSQL_ERRNO[-OB_BUILD_MD5_ERROR] = -1;
    SQLSTATE[-OB_BUILD_MD5_ERROR] = "HY000";
    STR_ERROR[-OB_BUILD_MD5_ERROR] = "Build MD5 fail";
    STR_USER_ERROR[-OB_BUILD_MD5_ERROR] = "Build MD5 fail";
    ORACLE_ERRNO[-OB_BUILD_MD5_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_BUILD_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9009, Build MD5 fail";
    ORACLE_STR_USER_ERROR[-OB_BUILD_MD5_ERROR] = "ORA-00600: internal error code, arguments: -9009, Build MD5 fail";
    ERROR_NAME[-OB_MD5_NOT_MATCH] = "OB_MD5_NOT_MATCH";
    MYSQL_ERRNO[-OB_MD5_NOT_MATCH] = -1;
    SQLSTATE[-OB_MD5_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_MD5_NOT_MATCH] = "OSS file MD5 not match";
    STR_USER_ERROR[-OB_MD5_NOT_MATCH] = "OSS file MD5 not match";
    ORACLE_ERRNO[-OB_MD5_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_MD5_NOT_MATCH] = "ORA-00600: internal error code, arguments: -9010, OSS file MD5 not match";
    ORACLE_STR_USER_ERROR[-OB_MD5_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -9010, OSS file MD5 not match";
    ERROR_NAME[-OB_BACKUP_FILE_NOT_EXIST] = "OB_BACKUP_FILE_NOT_EXIST";
    MYSQL_ERRNO[-OB_BACKUP_FILE_NOT_EXIST] = -1;
    SQLSTATE[-OB_BACKUP_FILE_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_BACKUP_FILE_NOT_EXIST] = "cannot find backup file";
    STR_USER_ERROR[-OB_BACKUP_FILE_NOT_EXIST] = "cannot find backup file";
    ORACLE_ERRNO[-OB_BACKUP_FILE_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_FILE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9011, cannot find backup file";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_FILE_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9011, cannot find backup file";
    ERROR_NAME[-OB_OSS_DATA_VERSION_NOT_MATCHED] = "OB_OSS_DATA_VERSION_NOT_MATCHED";
    MYSQL_ERRNO[-OB_OSS_DATA_VERSION_NOT_MATCHED] = -1;
    SQLSTATE[-OB_OSS_DATA_VERSION_NOT_MATCHED] = "HY000";
    STR_ERROR[-OB_OSS_DATA_VERSION_NOT_MATCHED] = "Can not get data version from timestamp";
    STR_USER_ERROR[-OB_OSS_DATA_VERSION_NOT_MATCHED] = "Can not get data version from timestamp";
    ORACLE_ERRNO[-OB_OSS_DATA_VERSION_NOT_MATCHED] = 600;
    ORACLE_STR_ERROR[-OB_OSS_DATA_VERSION_NOT_MATCHED] =
        "ORA-00600: internal error code, arguments: -9012, Can not get data version from timestamp";
    ORACLE_STR_USER_ERROR[-OB_OSS_DATA_VERSION_NOT_MATCHED] =
        "ORA-00600: internal error code, arguments: -9012, Can not get data version from timestamp";
    ERROR_NAME[-OB_OSS_WRITE_ERROR] = "OB_OSS_WRITE_ERROR";
    MYSQL_ERRNO[-OB_OSS_WRITE_ERROR] = -1;
    SQLSTATE[-OB_OSS_WRITE_ERROR] = "HY000";
    STR_ERROR[-OB_OSS_WRITE_ERROR] = "Write OSS file error";
    STR_USER_ERROR[-OB_OSS_WRITE_ERROR] = "Write OSS file error";
    ORACLE_ERRNO[-OB_OSS_WRITE_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_OSS_WRITE_ERROR] = "ORA-00600: internal error code, arguments: -9013, Write OSS file error";
    ORACLE_STR_USER_ERROR[-OB_OSS_WRITE_ERROR] =
        "ORA-00600: internal error code, arguments: -9013, Write OSS file error";
    ERROR_NAME[-OB_RESTORE_IN_PROGRESS] = "OB_RESTORE_IN_PROGRESS";
    MYSQL_ERRNO[-OB_RESTORE_IN_PROGRESS] = -1;
    SQLSTATE[-OB_RESTORE_IN_PROGRESS] = "HY000";
    STR_ERROR[-OB_RESTORE_IN_PROGRESS] = "Another restore is in progress";
    STR_USER_ERROR[-OB_RESTORE_IN_PROGRESS] = "Another restore is in progress";
    ORACLE_ERRNO[-OB_RESTORE_IN_PROGRESS] = 600;
    ORACLE_STR_ERROR[-OB_RESTORE_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -9014, Another restore is in progress";
    ORACLE_STR_USER_ERROR[-OB_RESTORE_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -9014, Another restore is in progress";
    ERROR_NAME[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] = "OB_AGENT_INITING_BACKUP_COUNT_ERROR";
    MYSQL_ERRNO[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] = -1;
    SQLSTATE[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] = "HY000";
    STR_ERROR[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] = "agent initing backup count error";
    STR_USER_ERROR[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] = "agent initing backup count error";
    ORACLE_ERRNO[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] = 600;
    ORACLE_STR_ERROR[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] =
        "ORA-00600: internal error code, arguments: -9015, agent initing backup count error";
    ORACLE_STR_USER_ERROR[-OB_AGENT_INITING_BACKUP_COUNT_ERROR] =
        "ORA-00600: internal error code, arguments: -9015, agent initing backup count error";
    ERROR_NAME[-OB_CLUSTER_NAME_NOT_EQUAL] = "OB_CLUSTER_NAME_NOT_EQUAL";
    MYSQL_ERRNO[-OB_CLUSTER_NAME_NOT_EQUAL] = -1;
    SQLSTATE[-OB_CLUSTER_NAME_NOT_EQUAL] = "HY000";
    STR_ERROR[-OB_CLUSTER_NAME_NOT_EQUAL] = "ob cluster name not equal";
    STR_USER_ERROR[-OB_CLUSTER_NAME_NOT_EQUAL] = "ob cluster name not equal";
    ORACLE_ERRNO[-OB_CLUSTER_NAME_NOT_EQUAL] = 600;
    ORACLE_STR_ERROR[-OB_CLUSTER_NAME_NOT_EQUAL] =
        "ORA-00600: internal error code, arguments: -9016, ob cluster name not equal";
    ORACLE_STR_USER_ERROR[-OB_CLUSTER_NAME_NOT_EQUAL] =
        "ORA-00600: internal error code, arguments: -9016, ob cluster name not equal";
    ERROR_NAME[-OB_RS_LIST_INVAILD] = "OB_RS_LIST_INVAILD";
    MYSQL_ERRNO[-OB_RS_LIST_INVAILD] = -1;
    SQLSTATE[-OB_RS_LIST_INVAILD] = "HY000";
    STR_ERROR[-OB_RS_LIST_INVAILD] = "rs list invalid";
    STR_USER_ERROR[-OB_RS_LIST_INVAILD] = "rs list invalid";
    ORACLE_ERRNO[-OB_RS_LIST_INVAILD] = 600;
    ORACLE_STR_ERROR[-OB_RS_LIST_INVAILD] = "ORA-00600: internal error code, arguments: -9017, rs list invalid";
    ORACLE_STR_USER_ERROR[-OB_RS_LIST_INVAILD] = "ORA-00600: internal error code, arguments: -9017, rs list invalid";
    ERROR_NAME[-OB_AGENT_HAS_FAILED_TASK] = "OB_AGENT_HAS_FAILED_TASK";
    MYSQL_ERRNO[-OB_AGENT_HAS_FAILED_TASK] = -1;
    SQLSTATE[-OB_AGENT_HAS_FAILED_TASK] = "HY000";
    STR_ERROR[-OB_AGENT_HAS_FAILED_TASK] = "agent has failed task";
    STR_USER_ERROR[-OB_AGENT_HAS_FAILED_TASK] = "agent has failed task";
    ORACLE_ERRNO[-OB_AGENT_HAS_FAILED_TASK] = 600;
    ORACLE_STR_ERROR[-OB_AGENT_HAS_FAILED_TASK] =
        "ORA-00600: internal error code, arguments: -9018, agent has failed task";
    ORACLE_STR_USER_ERROR[-OB_AGENT_HAS_FAILED_TASK] =
        "ORA-00600: internal error code, arguments: -9018, agent has failed task";
    ERROR_NAME[-OB_RESTORE_PARTITION_IS_COMPELETE] = "OB_RESTORE_PARTITION_IS_COMPELETE";
    MYSQL_ERRNO[-OB_RESTORE_PARTITION_IS_COMPELETE] = -1;
    SQLSTATE[-OB_RESTORE_PARTITION_IS_COMPELETE] = "HY000";
    STR_ERROR[-OB_RESTORE_PARTITION_IS_COMPELETE] = "restore partition is compelete";
    STR_USER_ERROR[-OB_RESTORE_PARTITION_IS_COMPELETE] = "restore partition is compelete";
    ORACLE_ERRNO[-OB_RESTORE_PARTITION_IS_COMPELETE] = 600;
    ORACLE_STR_ERROR[-OB_RESTORE_PARTITION_IS_COMPELETE] =
        "ORA-00600: internal error code, arguments: -9019, restore partition is compelete";
    ORACLE_STR_USER_ERROR[-OB_RESTORE_PARTITION_IS_COMPELETE] =
        "ORA-00600: internal error code, arguments: -9019, restore partition is compelete";
    ERROR_NAME[-OB_RESTORE_PARTITION_TWICE] = "OB_RESTORE_PARTITION_TWICE";
    MYSQL_ERRNO[-OB_RESTORE_PARTITION_TWICE] = -1;
    SQLSTATE[-OB_RESTORE_PARTITION_TWICE] = "HY000";
    STR_ERROR[-OB_RESTORE_PARTITION_TWICE] = "restore partition twice";
    STR_USER_ERROR[-OB_RESTORE_PARTITION_TWICE] = "restore partition twice";
    ORACLE_ERRNO[-OB_RESTORE_PARTITION_TWICE] = 600;
    ORACLE_STR_ERROR[-OB_RESTORE_PARTITION_TWICE] =
        "ORA-00600: internal error code, arguments: -9020, restore partition twice";
    ORACLE_STR_USER_ERROR[-OB_RESTORE_PARTITION_TWICE] =
        "ORA-00600: internal error code, arguments: -9020, restore partition twice";
    ERROR_NAME[-OB_STOP_DROP_SCHEMA] = "OB_STOP_DROP_SCHEMA";
    MYSQL_ERRNO[-OB_STOP_DROP_SCHEMA] = -1;
    SQLSTATE[-OB_STOP_DROP_SCHEMA] = "HY000";
    STR_ERROR[-OB_STOP_DROP_SCHEMA] = "physical backup switch is on";
    STR_USER_ERROR[-OB_STOP_DROP_SCHEMA] = "physical backup switch is on";
    ORACLE_ERRNO[-OB_STOP_DROP_SCHEMA] = 600;
    ORACLE_STR_ERROR[-OB_STOP_DROP_SCHEMA] =
        "ORA-00600: internal error code, arguments: -9022, physical backup switch is on";
    ORACLE_STR_USER_ERROR[-OB_STOP_DROP_SCHEMA] =
        "ORA-00600: internal error code, arguments: -9022, physical backup switch is on";
    ERROR_NAME[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] = "OB_CANNOT_START_LOG_ARCHIVE_BACKUP";
    MYSQL_ERRNO[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] = -1;
    SQLSTATE[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] = "HY000";
    STR_ERROR[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] = "cannot start log archive backup";
    STR_USER_ERROR[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] = "cannot start log archive backup";
    ORACLE_ERRNO[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] = 600;
    ORACLE_STR_ERROR[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] =
        "ORA-00600: internal error code, arguments: -9023, cannot start log archive backup";
    ORACLE_STR_USER_ERROR[-OB_CANNOT_START_LOG_ARCHIVE_BACKUP] =
        "ORA-00600: internal error code, arguments: -9023, cannot start log archive backup";
    ERROR_NAME[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] = "OB_ALREADY_NO_LOG_ARCHIVE_BACKUP";
    MYSQL_ERRNO[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] = -1;
    SQLSTATE[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] = "HY000";
    STR_ERROR[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] = "log archive backup is already disabled";
    STR_USER_ERROR[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] = "log archive backup is already disabled";
    ORACLE_ERRNO[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] = 600;
    ORACLE_STR_ERROR[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] =
        "ORA-00600: internal error code, arguments: -9024, log archive backup is already disabled";
    ORACLE_STR_USER_ERROR[-OB_ALREADY_NO_LOG_ARCHIVE_BACKUP] =
        "ORA-00600: internal error code, arguments: -9024, log archive backup is already disabled";
    ERROR_NAME[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] = "OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST";
    MYSQL_ERRNO[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] = -1;
    SQLSTATE[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] = "log archive backup info not exists";
    STR_USER_ERROR[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] = "log archive backup info not exists";
    ORACLE_ERRNO[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9025, log archive backup info not exists";
    ORACLE_STR_USER_ERROR[-OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9025, log archive backup info not exists";
    ERROR_NAME[-OB_INVALID_BACKUP_DEST] = "OB_INVALID_BACKUP_DEST";
    MYSQL_ERRNO[-OB_INVALID_BACKUP_DEST] = -1;
    SQLSTATE[-OB_INVALID_BACKUP_DEST] = "HY000";
    STR_ERROR[-OB_INVALID_BACKUP_DEST] = "backup destination is not valid";
    STR_USER_ERROR[-OB_INVALID_BACKUP_DEST] = "backup destination is not valid";
    ORACLE_ERRNO[-OB_INVALID_BACKUP_DEST] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_BACKUP_DEST] =
        "ORA-00600: internal error code, arguments: -9026, backup destination is not valid";
    ORACLE_STR_USER_ERROR[-OB_INVALID_BACKUP_DEST] =
        "ORA-00600: internal error code, arguments: -9026, backup destination is not valid";
    ERROR_NAME[-OB_LOG_ARCHIVE_INTERRUPTED] = "OB_LOG_ARCHIVE_INTERRUPTED";
    MYSQL_ERRNO[-OB_LOG_ARCHIVE_INTERRUPTED] = -1;
    SQLSTATE[-OB_LOG_ARCHIVE_INTERRUPTED] = "HY000";
    STR_ERROR[-OB_LOG_ARCHIVE_INTERRUPTED] = "ob log archive interrupted";
    STR_USER_ERROR[-OB_LOG_ARCHIVE_INTERRUPTED] = "ob log archive interrupted";
    ORACLE_ERRNO[-OB_LOG_ARCHIVE_INTERRUPTED] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ARCHIVE_INTERRUPTED] =
        "ORA-00600: internal error code, arguments: -9027, ob log archive interrupted";
    ORACLE_STR_USER_ERROR[-OB_LOG_ARCHIVE_INTERRUPTED] =
        "ORA-00600: internal error code, arguments: -9027, ob log archive interrupted";
    ERROR_NAME[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] = "OB_LOG_ARCHIVE_STAT_NOT_MATCH";
    MYSQL_ERRNO[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] = -1;
    SQLSTATE[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] = "ob log archive stat not match";
    STR_USER_ERROR[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] = "ob log archive stat not match";
    ORACLE_ERRNO[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -9028, ob log archive stat not match";
    ORACLE_STR_USER_ERROR[-OB_LOG_ARCHIVE_STAT_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -9028, ob log archive stat not match";
    ERROR_NAME[-OB_LOG_ARCHIVE_NOT_RUNNING] = "OB_LOG_ARCHIVE_NOT_RUNNING";
    MYSQL_ERRNO[-OB_LOG_ARCHIVE_NOT_RUNNING] = -1;
    SQLSTATE[-OB_LOG_ARCHIVE_NOT_RUNNING] = "HY000";
    STR_ERROR[-OB_LOG_ARCHIVE_NOT_RUNNING] = "log archive is not running";
    STR_USER_ERROR[-OB_LOG_ARCHIVE_NOT_RUNNING] = "log archive is not running";
    ORACLE_ERRNO[-OB_LOG_ARCHIVE_NOT_RUNNING] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ARCHIVE_NOT_RUNNING] =
        "ORA-00600: internal error code, arguments: -9029, log archive is not running";
    ORACLE_STR_USER_ERROR[-OB_LOG_ARCHIVE_NOT_RUNNING] =
        "ORA-00600: internal error code, arguments: -9029, log archive is not running";
    ERROR_NAME[-OB_LOG_ARCHIVE_INVALID_ROUND] = "OB_LOG_ARCHIVE_INVALID_ROUND";
    MYSQL_ERRNO[-OB_LOG_ARCHIVE_INVALID_ROUND] = -1;
    SQLSTATE[-OB_LOG_ARCHIVE_INVALID_ROUND] = "HY000";
    STR_ERROR[-OB_LOG_ARCHIVE_INVALID_ROUND] = "log archive invalid round";
    STR_USER_ERROR[-OB_LOG_ARCHIVE_INVALID_ROUND] = "log archive invalid round";
    ORACLE_ERRNO[-OB_LOG_ARCHIVE_INVALID_ROUND] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ARCHIVE_INVALID_ROUND] =
        "ORA-00600: internal error code, arguments: -9030, log archive invalid round";
    ORACLE_STR_USER_ERROR[-OB_LOG_ARCHIVE_INVALID_ROUND] =
        "ORA-00600: internal error code, arguments: -9030, log archive invalid round";
    ERROR_NAME[-OB_REPLICA_CANNOT_BACKUP] = "OB_REPLICA_CANNOT_BACKUP";
    MYSQL_ERRNO[-OB_REPLICA_CANNOT_BACKUP] = -1;
    SQLSTATE[-OB_REPLICA_CANNOT_BACKUP] = "HY000";
    STR_ERROR[-OB_REPLICA_CANNOT_BACKUP] = "Cannot backup ob replica";
    STR_USER_ERROR[-OB_REPLICA_CANNOT_BACKUP] = "Cannot backup ob replica";
    ORACLE_ERRNO[-OB_REPLICA_CANNOT_BACKUP] = 600;
    ORACLE_STR_ERROR[-OB_REPLICA_CANNOT_BACKUP] =
        "ORA-00600: internal error code, arguments: -9031, Cannot backup ob replica";
    ORACLE_STR_USER_ERROR[-OB_REPLICA_CANNOT_BACKUP] =
        "ORA-00600: internal error code, arguments: -9031, Cannot backup ob replica";
    ERROR_NAME[-OB_BACKUP_INFO_NOT_EXIST] = "OB_BACKUP_INFO_NOT_EXIST";
    MYSQL_ERRNO[-OB_BACKUP_INFO_NOT_EXIST] = -1;
    SQLSTATE[-OB_BACKUP_INFO_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_BACKUP_INFO_NOT_EXIST] = "backup info not exists";
    STR_USER_ERROR[-OB_BACKUP_INFO_NOT_EXIST] = "backup info not exists";
    ORACLE_ERRNO[-OB_BACKUP_INFO_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_INFO_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9032, backup info not exists";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_INFO_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9032, backup info not exists";
    ERROR_NAME[-OB_BACKUP_INFO_NOT_MATCH] = "OB_BACKUP_INFO_NOT_MATCH";
    MYSQL_ERRNO[-OB_BACKUP_INFO_NOT_MATCH] = -1;
    SQLSTATE[-OB_BACKUP_INFO_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_BACKUP_INFO_NOT_MATCH] =
        "Backup meta info stored in system dictionary does not match with current system status";
    STR_USER_ERROR[-OB_BACKUP_INFO_NOT_MATCH] =
        "Backup meta info stored in system dictionary does not match with current system status";
    ORACLE_ERRNO[-OB_BACKUP_INFO_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_INFO_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -9033, Backup meta info stored in system dictionary does not match "
        "with current system status";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_INFO_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -9033, Backup meta info stored in system dictionary does not match "
        "with current system status";
    ERROR_NAME[-OB_LOG_ARCHIVE_ALREADY_STOPPED] = "OB_LOG_ARCHIVE_ALREADY_STOPPED";
    MYSQL_ERRNO[-OB_LOG_ARCHIVE_ALREADY_STOPPED] = -1;
    SQLSTATE[-OB_LOG_ARCHIVE_ALREADY_STOPPED] = "HY000";
    STR_ERROR[-OB_LOG_ARCHIVE_ALREADY_STOPPED] = "log archive already stopped";
    STR_USER_ERROR[-OB_LOG_ARCHIVE_ALREADY_STOPPED] = "log archive already stopped";
    ORACLE_ERRNO[-OB_LOG_ARCHIVE_ALREADY_STOPPED] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ARCHIVE_ALREADY_STOPPED] =
        "ORA-00600: internal error code, arguments: -9034, log archive already stopped";
    ORACLE_STR_USER_ERROR[-OB_LOG_ARCHIVE_ALREADY_STOPPED] =
        "ORA-00600: internal error code, arguments: -9034, log archive already stopped";
    ERROR_NAME[-OB_RESTORE_INDEX_FAILED] = "OB_RESTORE_INDEX_FAILED";
    MYSQL_ERRNO[-OB_RESTORE_INDEX_FAILED] = -1;
    SQLSTATE[-OB_RESTORE_INDEX_FAILED] = "HY000";
    STR_ERROR[-OB_RESTORE_INDEX_FAILED] = "restore index failed";
    STR_USER_ERROR[-OB_RESTORE_INDEX_FAILED] = "restore index failed";
    ORACLE_ERRNO[-OB_RESTORE_INDEX_FAILED] = 600;
    ORACLE_STR_ERROR[-OB_RESTORE_INDEX_FAILED] =
        "ORA-00600: internal error code, arguments: -9035, restore index failed";
    ORACLE_STR_USER_ERROR[-OB_RESTORE_INDEX_FAILED] =
        "ORA-00600: internal error code, arguments: -9035, restore index failed";
    ERROR_NAME[-OB_BACKUP_IN_PROGRESS] = "OB_BACKUP_IN_PROGRESS";
    MYSQL_ERRNO[-OB_BACKUP_IN_PROGRESS] = -1;
    SQLSTATE[-OB_BACKUP_IN_PROGRESS] = "HY000";
    STR_ERROR[-OB_BACKUP_IN_PROGRESS] = "Backup is in progress";
    STR_USER_ERROR[-OB_BACKUP_IN_PROGRESS] = "Backup is in progress";
    ORACLE_ERRNO[-OB_BACKUP_IN_PROGRESS] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -9036, Backup is in progress";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -9036, Backup is in progress";
    ERROR_NAME[-OB_INVALID_LOG_ARCHIVE_STATUS] = "OB_INVALID_LOG_ARCHIVE_STATUS";
    MYSQL_ERRNO[-OB_INVALID_LOG_ARCHIVE_STATUS] = -1;
    SQLSTATE[-OB_INVALID_LOG_ARCHIVE_STATUS] = "HY000";
    STR_ERROR[-OB_INVALID_LOG_ARCHIVE_STATUS] = "log archive status is not valid";
    STR_USER_ERROR[-OB_INVALID_LOG_ARCHIVE_STATUS] = "log archive status is not valid";
    ORACLE_ERRNO[-OB_INVALID_LOG_ARCHIVE_STATUS] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_LOG_ARCHIVE_STATUS] =
        "ORA-00600: internal error code, arguments: -9037, log archive status is not valid";
    ORACLE_STR_USER_ERROR[-OB_INVALID_LOG_ARCHIVE_STATUS] =
        "ORA-00600: internal error code, arguments: -9037, log archive status is not valid";
    ERROR_NAME[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] = "OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST";
    MYSQL_ERRNO[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] = -1;
    SQLSTATE[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] = "HY000";
    STR_ERROR[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] = "Cannot add replica during set member list in restore";
    STR_USER_ERROR[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] =
        "Cannot add replica during set member list in restore";
    ORACLE_ERRNO[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] = 600;
    ORACLE_STR_ERROR[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] =
        "ORA-00600: internal error code, arguments: -9038, Cannot add replica during set member list in restore";
    ORACLE_STR_USER_ERROR[-OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST] =
        "ORA-00600: internal error code, arguments: -9038, Cannot add replica during set member list in restore";
    ERROR_NAME[-OB_LOG_ARCHIVE_LEADER_CHANGED] = "OB_LOG_ARCHIVE_LEADER_CHANGED";
    MYSQL_ERRNO[-OB_LOG_ARCHIVE_LEADER_CHANGED] = -1;
    SQLSTATE[-OB_LOG_ARCHIVE_LEADER_CHANGED] = "HY000";
    STR_ERROR[-OB_LOG_ARCHIVE_LEADER_CHANGED] = "pg log archive leader changed";
    STR_USER_ERROR[-OB_LOG_ARCHIVE_LEADER_CHANGED] = "pg log archive leader changed";
    ORACLE_ERRNO[-OB_LOG_ARCHIVE_LEADER_CHANGED] = 600;
    ORACLE_STR_ERROR[-OB_LOG_ARCHIVE_LEADER_CHANGED] =
        "ORA-00600: internal error code, arguments: -9039, pg log archive leader changed";
    ORACLE_STR_USER_ERROR[-OB_LOG_ARCHIVE_LEADER_CHANGED] =
        "ORA-00600: internal error code, arguments: -9039, pg log archive leader changed";
    ERROR_NAME[-OB_BACKUP_CAN_NOT_START] = "OB_BACKUP_CAN_NOT_START";
    MYSQL_ERRNO[-OB_BACKUP_CAN_NOT_START] = -1;
    SQLSTATE[-OB_BACKUP_CAN_NOT_START] = "HY000";
    STR_ERROR[-OB_BACKUP_CAN_NOT_START] = "backup can not start";
    STR_USER_ERROR[-OB_BACKUP_CAN_NOT_START] = "backup can not start, because %s";
    ORACLE_ERRNO[-OB_BACKUP_CAN_NOT_START] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_CAN_NOT_START] =
        "ORA-00600: internal error code, arguments: -9040, backup can not start";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_CAN_NOT_START] =
        "ORA-00600: internal error code, arguments: -9040, backup can not start, because %s";
    ERROR_NAME[-OB_CANCEL_BACKUP_NOT_ALLOWED] = "OB_CANCEL_BACKUP_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_CANCEL_BACKUP_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_CANCEL_BACKUP_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_CANCEL_BACKUP_NOT_ALLOWED] = "cancel backup do not allow";
    STR_USER_ERROR[-OB_CANCEL_BACKUP_NOT_ALLOWED] = "cancel backup do not allow";
    ORACLE_ERRNO[-OB_CANCEL_BACKUP_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_CANCEL_BACKUP_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -9041, cancel backup do not allow";
    ORACLE_STR_USER_ERROR[-OB_CANCEL_BACKUP_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -9041, cancel backup do not allow";
    ERROR_NAME[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] = "OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT";
    MYSQL_ERRNO[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] = -1;
    SQLSTATE[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] = "HY000";
    STR_ERROR[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] = "backup data version gap over limit";
    STR_USER_ERROR[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] = "backup data version gap over limit";
    ORACLE_ERRNO[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] =
        "ORA-00600: internal error code, arguments: -9042, backup data version gap over limit";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT] =
        "ORA-00600: internal error code, arguments: -9042, backup data version gap over limit";
    ERROR_NAME[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] = "OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT";
    MYSQL_ERRNO[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] = -1;
    SQLSTATE[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] = "HY000";
    STR_ERROR[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] = "pg log archive status is still invalid";
    STR_USER_ERROR[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] = "pg log archive status is still invalid";
    ORACLE_ERRNO[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] = 600;
    ORACLE_STR_ERROR[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] =
        "ORA-00600: internal error code, arguments: -9043, pg log archive status is still invalid";
    ORACLE_STR_USER_ERROR[-OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT] =
        "ORA-00600: internal error code, arguments: -9043, pg log archive status is still invalid";
    ERROR_NAME[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] = "OB_BACKUP_DELETE_DATA_IN_PROGRESS";
    MYSQL_ERRNO[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] = -1;
    SQLSTATE[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] = "HY000";
    STR_ERROR[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] = "delete backup data is in progress";
    STR_USER_ERROR[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] = "delete backup data is in progress";
    ORACLE_ERRNO[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -9044, delete backup data is in progress";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_DELETE_DATA_IN_PROGRESS] =
        "ORA-00600: internal error code, arguments: -9044, delete backup data is in progress";
    ERROR_NAME[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] = "OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] = "delete backup set do not allow";
    STR_USER_ERROR[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] = "delete backup set do not allow";
    ORACLE_ERRNO[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -9045, delete backup set do not allow";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -9045, delete backup set do not allow";
    ERROR_NAME[-OB_INVALID_BACKUP_SET_ID] = "OB_INVALID_BACKUP_SET_ID";
    MYSQL_ERRNO[-OB_INVALID_BACKUP_SET_ID] = -1;
    SQLSTATE[-OB_INVALID_BACKUP_SET_ID] = "HY000";
    STR_ERROR[-OB_INVALID_BACKUP_SET_ID] = "backup set id is not valid";
    STR_USER_ERROR[-OB_INVALID_BACKUP_SET_ID] = "backup set id is not valid";
    ORACLE_ERRNO[-OB_INVALID_BACKUP_SET_ID] = 600;
    ORACLE_STR_ERROR[-OB_INVALID_BACKUP_SET_ID] =
        "ORA-00600: internal error code, arguments: -9046, backup set id is not valid";
    ORACLE_STR_USER_ERROR[-OB_INVALID_BACKUP_SET_ID] =
        "ORA-00600: internal error code, arguments: -9046, backup set id is not valid";
    ERROR_NAME[-OB_BACKUP_INVALID_PASSWORD] = "OB_BACKUP_INVALID_PASSWORD";
    MYSQL_ERRNO[-OB_BACKUP_INVALID_PASSWORD] = -1;
    SQLSTATE[-OB_BACKUP_INVALID_PASSWORD] = "HY000";
    STR_ERROR[-OB_BACKUP_INVALID_PASSWORD] = "invalid password for backup";
    STR_USER_ERROR[-OB_BACKUP_INVALID_PASSWORD] = "invalid password for backup";
    ORACLE_ERRNO[-OB_BACKUP_INVALID_PASSWORD] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_INVALID_PASSWORD] =
        "ORA-00600: internal error code, arguments: -9047, invalid password for backup";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_INVALID_PASSWORD] =
        "ORA-00600: internal error code, arguments: -9047, invalid password for backup";
    ERROR_NAME[-OB_ISOLATED_BACKUP_SET] = "OB_ISOLATED_BACKUP_SET";
    MYSQL_ERRNO[-OB_ISOLATED_BACKUP_SET] = -1;
    SQLSTATE[-OB_ISOLATED_BACKUP_SET] = "HY000";
    STR_ERROR[-OB_ISOLATED_BACKUP_SET] = "backup set is isolated by two log archive round";
    STR_USER_ERROR[-OB_ISOLATED_BACKUP_SET] = "backup set is isolated by two log archive round";
    ORACLE_ERRNO[-OB_ISOLATED_BACKUP_SET] = 600;
    ORACLE_STR_ERROR[-OB_ISOLATED_BACKUP_SET] =
        "ORA-00600: internal error code, arguments: -9048, backup set is isolated by two log archive round";
    ORACLE_STR_USER_ERROR[-OB_ISOLATED_BACKUP_SET] =
        "ORA-00600: internal error code, arguments: -9048, backup set is isolated by two log archive round";
    ERROR_NAME[-OB_CANNOT_CANCEL_STOPPED_BACKUP] = "OB_CANNOT_CANCEL_STOPPED_BACKUP";
    MYSQL_ERRNO[-OB_CANNOT_CANCEL_STOPPED_BACKUP] = -1;
    SQLSTATE[-OB_CANNOT_CANCEL_STOPPED_BACKUP] = "HY000";
    STR_ERROR[-OB_CANNOT_CANCEL_STOPPED_BACKUP] = "backup status is stopped, can not cancel";
    STR_USER_ERROR[-OB_CANNOT_CANCEL_STOPPED_BACKUP] = "backup status is stopped, can not cancel";
    ORACLE_ERRNO[-OB_CANNOT_CANCEL_STOPPED_BACKUP] = 600;
    ORACLE_STR_ERROR[-OB_CANNOT_CANCEL_STOPPED_BACKUP] =
        "ORA-00600: internal error code, arguments: -9049, backup status is stopped, can not cancel";
    ORACLE_STR_USER_ERROR[-OB_CANNOT_CANCEL_STOPPED_BACKUP] =
        "ORA-00600: internal error code, arguments: -9049, backup status is stopped, can not cancel";
    ERROR_NAME[-OB_BACKUP_BACKUP_CAN_NOT_START] = "OB_BACKUP_BACKUP_CAN_NOT_START";
    MYSQL_ERRNO[-OB_BACKUP_BACKUP_CAN_NOT_START] = -1;
    SQLSTATE[-OB_BACKUP_BACKUP_CAN_NOT_START] = "HY000";
    STR_ERROR[-OB_BACKUP_BACKUP_CAN_NOT_START] = "no backup data to be backuped up";
    STR_USER_ERROR[-OB_BACKUP_BACKUP_CAN_NOT_START] = "no backup data to be backuped up";
    ORACLE_ERRNO[-OB_BACKUP_BACKUP_CAN_NOT_START] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_BACKUP_CAN_NOT_START] =
        "ORA-00600: internal error code, arguments: -9050, no backup data to be backuped up";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_BACKUP_CAN_NOT_START] =
        "ORA-00600: internal error code, arguments: -9050, no backup data to be backuped up";
    ERROR_NAME[-OB_BACKUP_MOUNT_FILE_NOT_VALID] = "OB_BACKUP_MOUNT_FILE_NOT_VALID";
    MYSQL_ERRNO[-OB_BACKUP_MOUNT_FILE_NOT_VALID] = -1;
    SQLSTATE[-OB_BACKUP_MOUNT_FILE_NOT_VALID] = "HY000";
    STR_ERROR[-OB_BACKUP_MOUNT_FILE_NOT_VALID] = "backup mount file is not valid";
    STR_USER_ERROR[-OB_BACKUP_MOUNT_FILE_NOT_VALID] = "backup mount file is not valid";
    ORACLE_ERRNO[-OB_BACKUP_MOUNT_FILE_NOT_VALID] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_MOUNT_FILE_NOT_VALID] =
        "ORA-00600: internal error code, arguments: -9051, backup mount file is not valid";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_MOUNT_FILE_NOT_VALID] =
        "ORA-00600: internal error code, arguments: -9051, backup mount file is not valid";
    ERROR_NAME[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] = "OB_BACKUP_CLEAN_INFO_NOT_MATCH";
    MYSQL_ERRNO[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] = -1;
    SQLSTATE[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] = "HY000";
    STR_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] = "backup clean info not match";
    STR_USER_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] = "backup clean info not match";
    ORACLE_ERRNO[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -9052, backup clean info not match";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_MATCH] =
        "ORA-00600: internal error code, arguments: -9052, backup clean info not match";
    ERROR_NAME[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] = "OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED";
    MYSQL_ERRNO[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] = -1;
    SQLSTATE[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] = "HY000";
    STR_ERROR[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] = "cancel delete backup do not allow";
    STR_USER_ERROR[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] = "cancel delete backup do not allow";
    ORACLE_ERRNO[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] = 600;
    ORACLE_STR_ERROR[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -9053, cancel delete backup do not allow";
    ORACLE_STR_USER_ERROR[-OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED] =
        "ORA-00600: internal error code, arguments: -9053, cancel delete backup do not allow";
    ERROR_NAME[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] = "OB_BACKUP_CLEAN_INFO_NOT_EXIST";
    MYSQL_ERRNO[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] = -1;
    SQLSTATE[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] = "backup clean info not exists";
    STR_USER_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] = "backup clean info not exists";
    ORACLE_ERRNO[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9054, backup clean info not exists";
    ORACLE_STR_USER_ERROR[-OB_BACKUP_CLEAN_INFO_NOT_EXIST] =
        "ORA-00600: internal error code, arguments: -9054, backup clean info not exists";
    ERROR_NAME[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] = "OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX";
    MYSQL_ERRNO[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] = -1;
    SQLSTATE[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] = "HY000";
    STR_ERROR[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] =
        "rebuild global index failed when drop/truncate partitions";
    STR_USER_ERROR[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] =
        "rebuild global index:'%.*s' failed when drop/truncate partitions";
    ORACLE_ERRNO[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] = 600;
    ORACLE_STR_ERROR[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] =
        "ORA-00600: internal error code, arguments: -9055, rebuild global index failed when drop/truncate partitions";
    ORACLE_STR_USER_ERROR[-OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX] =
        "ORA-00600: internal error code, arguments: -9055, rebuild global index:'%.*s' failed when drop/truncate "
        "partitions";
    ERROR_NAME[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] = "OB_ERR_ATLER_TABLE_ILLEGAL_FK";
    MYSQL_ERRNO[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] = -1;
    SQLSTATE[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] = "HY000";
    STR_ERROR[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] = "unique/primary keys in table referenced by enabled foreign keys";
    STR_USER_ERROR[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] = "unique/primary keys in table referenced by enabled foreign keys";
    ORACLE_ERRNO[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] = 02266;
    ORACLE_STR_ERROR[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] =
        "ORA-02266: unique/primary keys in table referenced by enabled foreign keys";
    ORACLE_STR_USER_ERROR[-OB_ERR_ATLER_TABLE_ILLEGAL_FK] =
        "ORA-02266: unique/primary keys in table referenced by enabled foreign keys";
    ERROR_NAME[-OB_NO_SUCH_FILE_OR_DIRECTORY] = "OB_NO_SUCH_FILE_OR_DIRECTORY";
    MYSQL_ERRNO[-OB_NO_SUCH_FILE_OR_DIRECTORY] = -1;
    SQLSTATE[-OB_NO_SUCH_FILE_OR_DIRECTORY] = "HY000";
    STR_ERROR[-OB_NO_SUCH_FILE_OR_DIRECTORY] = "no such file or directory";
    STR_USER_ERROR[-OB_NO_SUCH_FILE_OR_DIRECTORY] = "no such file or directory";
    ORACLE_ERRNO[-OB_NO_SUCH_FILE_OR_DIRECTORY] = 600;
    ORACLE_STR_ERROR[-OB_NO_SUCH_FILE_OR_DIRECTORY] =
        "ORA-00600: internal error code, arguments: -9100, no such file or directory";
    ORACLE_STR_USER_ERROR[-OB_NO_SUCH_FILE_OR_DIRECTORY] =
        "ORA-00600: internal error code, arguments: -9100, no such file or directory";
    ERROR_NAME[-OB_FILE_OR_DIRECTORY_EXIST] = "OB_FILE_OR_DIRECTORY_EXIST";
    MYSQL_ERRNO[-OB_FILE_OR_DIRECTORY_EXIST] = -1;
    SQLSTATE[-OB_FILE_OR_DIRECTORY_EXIST] = "HY000";
    STR_ERROR[-OB_FILE_OR_DIRECTORY_EXIST] = "file or directory already exist";
    STR_USER_ERROR[-OB_FILE_OR_DIRECTORY_EXIST] = "file or directory already exist";
    ORACLE_ERRNO[-OB_FILE_OR_DIRECTORY_EXIST] = 600;
    ORACLE_STR_ERROR[-OB_FILE_OR_DIRECTORY_EXIST] =
        "ORA-00600: internal error code, arguments: -9101, file or directory already exist";
    ORACLE_STR_USER_ERROR[-OB_FILE_OR_DIRECTORY_EXIST] =
        "ORA-00600: internal error code, arguments: -9101, file or directory already exist";
    ERROR_NAME[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] =
        "OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION";
    MYSQL_ERRNO[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] = -1;
    SQLSTATE[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] = "HY000";
    STR_ERROR[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] = "Duplicate having-clause in table expression";
    STR_USER_ERROR[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] = "Duplicate having-clause in table expression";
    ORACLE_ERRNO[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] = 119;
    ORACLE_STR_ERROR[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] =
        "PLS-00119: Duplicate having-clause in table expression";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUPLICATE_HAVING_CLAUSE_IN_TABLE_EXPRESSION] =
        "PLS-00119: Duplicate having-clause in table expression";
    ERROR_NAME[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] = "OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY";
    MYSQL_ERRNO[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] = -1;
    SQLSTATE[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] = "HY000";
    STR_ERROR[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] = "OUT and IN/OUT modes cannot be used in this context";
    STR_USER_ERROR[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] = "OUT and IN/OUT modes cannot be used in this context";
    ORACLE_ERRNO[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] = 254;
    ORACLE_STR_ERROR[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] =
        "PLS-00254: OUT and IN/OUT modes cannot be used in this context";
    ORACLE_STR_USER_ERROR[-OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY] =
        "PLS-00254: OUT and IN/OUT modes cannot be used in this context";
    ERROR_NAME[-OB_ERR_OBJECT_NOT_FOUND] = "OB_ERR_OBJECT_NOT_FOUND";
    MYSQL_ERRNO[-OB_ERR_OBJECT_NOT_FOUND] = -1;
    SQLSTATE[-OB_ERR_OBJECT_NOT_FOUND] = "HY000";
    STR_ERROR[-OB_ERR_OBJECT_NOT_FOUND] = "object not found";
    STR_USER_ERROR[-OB_ERR_OBJECT_NOT_FOUND] = "object '%.*s' of type %.*s not found in schema '%.*s'";
    ORACLE_ERRNO[-OB_ERR_OBJECT_NOT_FOUND] = 31603;
    ORACLE_STR_ERROR[-OB_ERR_OBJECT_NOT_FOUND] = "ORA-31603: object not found";
    ORACLE_STR_USER_ERROR[-OB_ERR_OBJECT_NOT_FOUND] =
        "ORA-31603: object '%.*s' of type %.*s not found in schema '%.*s'";
    ERROR_NAME[-OB_ERR_INVALID_INPUT_VALUE] = "OB_ERR_INVALID_INPUT_VALUE";
    MYSQL_ERRNO[-OB_ERR_INVALID_INPUT_VALUE] = -1;
    SQLSTATE[-OB_ERR_INVALID_INPUT_VALUE] = "HY000";
    STR_ERROR[-OB_ERR_INVALID_INPUT_VALUE] = "invalid input value";
    STR_USER_ERROR[-OB_ERR_INVALID_INPUT_VALUE] = "invalid input value %.*s for parameter %.*s in function %.*s";
    ORACLE_ERRNO[-OB_ERR_INVALID_INPUT_VALUE] = 31600;
    ORACLE_STR_ERROR[-OB_ERR_INVALID_INPUT_VALUE] = "ORA-31600: invalid input value";
    ORACLE_STR_USER_ERROR[-OB_ERR_INVALID_INPUT_VALUE] =
        "ORA-31600: invalid input value %.*s for parameter %.*s in function %.*s";
    ERROR_NAME[-OB_ERR_GOTO_BRANCH_ILLEGAL] = "OB_ERR_GOTO_BRANCH_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_GOTO_BRANCH_ILLEGAL] = ER_SP_LILABEL_MISMATCH;
    SQLSTATE[-OB_ERR_GOTO_BRANCH_ILLEGAL] = "42000";
    STR_ERROR[-OB_ERR_GOTO_BRANCH_ILLEGAL] = "no matching label: %.*s";
    STR_USER_ERROR[-OB_ERR_GOTO_BRANCH_ILLEGAL] = "no matching label: %.*s";
    ORACLE_ERRNO[-OB_ERR_GOTO_BRANCH_ILLEGAL] = 375;
    ORACLE_STR_ERROR[-OB_ERR_GOTO_BRANCH_ILLEGAL] =
        "PLS-00375: illegal GOTO statement; this GOTO cannot branch to label '%.*s'";
    ORACLE_STR_USER_ERROR[-OB_ERR_GOTO_BRANCH_ILLEGAL] =
        "PLS-00375: illegal GOTO statement; this GOTO cannot branch to label '%.*s'";
    ERROR_NAME[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] = "OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW";
    MYSQL_ERRNO[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] = -1;
    SQLSTATE[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] = "HY000";
    STR_ERROR[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] = "Only schema-level programs allow AUTHID or DEFAULT COLLATION clause";
    STR_USER_ERROR[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] = "Only schema-level programs allow %s";
    ORACLE_ERRNO[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] = 157;
    ORACLE_STR_ERROR[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] =
        "PLS-00157: Only schema-level programs allow AUTHID or DEFAULT COLLATION clause";
    ORACLE_STR_USER_ERROR[-OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW] = "PLS-00157: Only schema-level programs allow %s";
    ERROR_NAME[-OB_ERR_DECL_MORE_THAN_ONCE] = "OB_ERR_DECL_MORE_THAN_ONCE";
    MYSQL_ERRNO[-OB_ERR_DECL_MORE_THAN_ONCE] = -1;
    SQLSTATE[-OB_ERR_DECL_MORE_THAN_ONCE] = "HY000";
    STR_ERROR[-OB_ERR_DECL_MORE_THAN_ONCE] = "at most one declaration for identifier is permitted";
    STR_USER_ERROR[-OB_ERR_DECL_MORE_THAN_ONCE] = "at most one declaration for '%.*s' is permitted";
    ORACLE_ERRNO[-OB_ERR_DECL_MORE_THAN_ONCE] = 371;
    ORACLE_STR_ERROR[-OB_ERR_DECL_MORE_THAN_ONCE] = "PLS-00371: at most one declaration for identifier is permitted";
    ORACLE_STR_USER_ERROR[-OB_ERR_DECL_MORE_THAN_ONCE] = "PLS-00371: at most one declaration for '%.*s' is permitted";
    ERROR_NAME[-OB_ERR_DUPLICATE_FILED] = "OB_ERR_DUPLICATE_FILED";
    MYSQL_ERRNO[-OB_ERR_DUPLICATE_FILED] = -1;
    SQLSTATE[-OB_ERR_DUPLICATE_FILED] = "HY000";
    STR_ERROR[-OB_ERR_DUPLICATE_FILED] = "duplicate fields in RECORD,TABLE or argument list are not permitted";
    STR_USER_ERROR[-OB_ERR_DUPLICATE_FILED] = "duplicate fields in RECORD,TABLE or argument list are not permitted";
    ORACLE_ERRNO[-OB_ERR_DUPLICATE_FILED] = 410;
    ORACLE_STR_ERROR[-OB_ERR_DUPLICATE_FILED] =
        "PLS-00410: duplicate fields in RECORD,TABLE or argument list are not permitted";
    ORACLE_STR_USER_ERROR[-OB_ERR_DUPLICATE_FILED] =
        "PLS-00410: duplicate fields in RECORD,TABLE or argument list are not permitted";
    ERROR_NAME[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] = "OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] = -1;
    SQLSTATE[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] = "HY000";
    STR_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] = "Pragma AUTONOMOUS_TRANSACTION cannot be specified here";
    STR_USER_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] = "Pragma AUTONOMOUS_TRANSACTION cannot be specified here";
    ORACLE_ERRNO[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] = 710;
    ORACLE_STR_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] =
        "PLS-00710: Pragma AUTONOMOUS_TRANSACTION cannot be specified here";
    ORACLE_STR_USER_ERROR[-OB_ERR_AUTONOMOUS_TRANSACTION_ILLEGAL] =
        "PLS-00710: Pragma AUTONOMOUS_TRANSACTION cannot be specified here";
    ERROR_NAME[-OB_ERR_EXIT_CONTINUE_ILLEGAL] = "OB_ERR_EXIT_CONTINUE_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_EXIT_CONTINUE_ILLEGAL] = -1;
    SQLSTATE[-OB_ERR_EXIT_CONTINUE_ILLEGAL] = "HY000";
    STR_ERROR[-OB_ERR_EXIT_CONTINUE_ILLEGAL] = "illegal EXIT/CONTINUE statement; it must appear inside a loop";
    STR_USER_ERROR[-OB_ERR_EXIT_CONTINUE_ILLEGAL] = "illegal EXIT/CONTINUE statement; it must appear inside a loop";
    ORACLE_ERRNO[-OB_ERR_EXIT_CONTINUE_ILLEGAL] = 376;
    ORACLE_STR_ERROR[-OB_ERR_EXIT_CONTINUE_ILLEGAL] =
        "PLS-00376: illegal EXIT/CONTINUE statement; it must appear inside a loop";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXIT_CONTINUE_ILLEGAL] =
        "PLS-00376: illegal EXIT/CONTINUE statement; it must appear inside a loop";
    ERROR_NAME[-OB_ERR_LABEL_ILLEGAL] = "OB_ERR_LABEL_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_LABEL_ILLEGAL] = -1;
    SQLSTATE[-OB_ERR_LABEL_ILLEGAL] = "HY000";
    STR_ERROR[-OB_ERR_LABEL_ILLEGAL] = "EXIT/CONTINUE label must label a LOOP statement";
    STR_USER_ERROR[-OB_ERR_LABEL_ILLEGAL] = "EXIT/CONTINUE label '%.*s' must label a LOOP statement";
    ORACLE_ERRNO[-OB_ERR_LABEL_ILLEGAL] = 373;
    ORACLE_STR_ERROR[-OB_ERR_LABEL_ILLEGAL] = "PLS-00373: EXIT/CONTINUE label must label a LOOP statement";
    ORACLE_STR_USER_ERROR[-OB_ERR_LABEL_ILLEGAL] = "PLS-00373: EXIT/CONTINUE label '%.*s' must label a LOOP statement";
    ERROR_NAME[-OB_ERR_CURSOR_LEFT_ASSIGN] = "OB_ERR_CURSOR_LEFT_ASSIGN";
    MYSQL_ERRNO[-OB_ERR_CURSOR_LEFT_ASSIGN] = -1;
    SQLSTATE[-OB_ERR_CURSOR_LEFT_ASSIGN] = "HY000";
    STR_ERROR[-OB_ERR_CURSOR_LEFT_ASSIGN] =
        "expression '%.*s' is inappropriate as the left hand side of an assignment statement";
    STR_USER_ERROR[-OB_ERR_CURSOR_LEFT_ASSIGN] =
        "expression '%.*s' is inappropriate as the left hand side of an assignment statement";
    ORACLE_ERRNO[-OB_ERR_CURSOR_LEFT_ASSIGN] = 321;
    ORACLE_STR_ERROR[-OB_ERR_CURSOR_LEFT_ASSIGN] =
        "PLS-00321: expression '%.*s' is inappropriate as the left hand side of anassignment statement";
    ORACLE_STR_USER_ERROR[-OB_ERR_CURSOR_LEFT_ASSIGN] =
        "PLS-00321: expression '%.*s' is inappropriate as the left hand side of anassignment statement";
    ERROR_NAME[-OB_ERR_INIT_NOTNULL_ILLEGAL] = "OB_ERR_INIT_NOTNULL_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_INIT_NOTNULL_ILLEGAL] = -1;
    SQLSTATE[-OB_ERR_INIT_NOTNULL_ILLEGAL] = "HY000";
    STR_ERROR[-OB_ERR_INIT_NOTNULL_ILLEGAL] = "a variable declared NOT NULL must have an initialization assignment";
    STR_USER_ERROR[-OB_ERR_INIT_NOTNULL_ILLEGAL] =
        "a variable declared NOT NULL must have an initialization assignment";
    ORACLE_ERRNO[-OB_ERR_INIT_NOTNULL_ILLEGAL] = 218;
    ORACLE_STR_ERROR[-OB_ERR_INIT_NOTNULL_ILLEGAL] =
        "PLS-00218: a variable declared NOT NULL must have an initialization assignment";
    ORACLE_STR_USER_ERROR[-OB_ERR_INIT_NOTNULL_ILLEGAL] =
        "PLS-00218: a variable declared NOT NULL must have an initialization assignment";
    ERROR_NAME[-OB_ERR_INIT_CONST_ILLEGAL] = "OB_ERR_INIT_CONST_ILLEGAL";
    MYSQL_ERRNO[-OB_ERR_INIT_CONST_ILLEGAL] = -1;
    SQLSTATE[-OB_ERR_INIT_CONST_ILLEGAL] = "HY000";
    STR_ERROR[-OB_ERR_INIT_CONST_ILLEGAL] = "Constant declarations should contain initialization assignments";
    STR_USER_ERROR[-OB_ERR_INIT_CONST_ILLEGAL] = "Constant declarations should contain initialization assignments";
    ORACLE_ERRNO[-OB_ERR_INIT_CONST_ILLEGAL] = 322;
    ORACLE_STR_ERROR[-OB_ERR_INIT_CONST_ILLEGAL] =
        "PLS-00322: Constant declarations should contain initialization assignments";
    ORACLE_STR_USER_ERROR[-OB_ERR_INIT_CONST_ILLEGAL] =
        "PLS-00322: Constant declarations should contain initialization assignments";
    ERROR_NAME[-OB_ERR_CURSOR_VAR_IN_PKG] = "OB_ERR_CURSOR_VAR_IN_PKG";
    MYSQL_ERRNO[-OB_ERR_CURSOR_VAR_IN_PKG] = -1;
    SQLSTATE[-OB_ERR_CURSOR_VAR_IN_PKG] = "HY000";
    STR_ERROR[-OB_ERR_CURSOR_VAR_IN_PKG] = "Cursor Variables cannot be declared as part of a package";
    STR_USER_ERROR[-OB_ERR_CURSOR_VAR_IN_PKG] = "Cursor Variables cannot be declared as part of a package";
    ORACLE_ERRNO[-OB_ERR_CURSOR_VAR_IN_PKG] = 994;
    ORACLE_STR_ERROR[-OB_ERR_CURSOR_VAR_IN_PKG] = "PLS-00994: Cursor Variables cannot be declared as part of a package";
    ORACLE_STR_USER_ERROR[-OB_ERR_CURSOR_VAR_IN_PKG] =
        "PLS-00994: Cursor Variables cannot be declared as part of a package";
    ERROR_NAME[-OB_ERR_LIMIT_CLAUSE] = "OB_ERR_LIMIT_CLAUSE";
    MYSQL_ERRNO[-OB_ERR_LIMIT_CLAUSE] = -1;
    SQLSTATE[-OB_ERR_LIMIT_CLAUSE] = "HY000";
    STR_ERROR[-OB_ERR_LIMIT_CLAUSE] = "value in LIMIT clause: \'%.*s\' use is invalid";
    STR_USER_ERROR[-OB_ERR_LIMIT_CLAUSE] = "value in LIMIT clause: \'%.*s\' use is invalid";
    ORACLE_ERRNO[-OB_ERR_LIMIT_CLAUSE] = 438;
    ORACLE_STR_ERROR[-OB_ERR_LIMIT_CLAUSE] = "PLS-00438: value in LIMIT clause: \'%.*s\' use is invalid";
    ORACLE_STR_USER_ERROR[-OB_ERR_LIMIT_CLAUSE] = "PLS-00438: value in LIMIT clause: \'%.*s\' use is invalid";
    ERROR_NAME[-OB_ERR_EXPRESSION_WRONG_TYPE] = "OB_ERR_EXPRESSION_WRONG_TYPE";
    MYSQL_ERRNO[-OB_ERR_EXPRESSION_WRONG_TYPE] = -1;
    SQLSTATE[-OB_ERR_EXPRESSION_WRONG_TYPE] = "HY000";
    STR_ERROR[-OB_ERR_EXPRESSION_WRONG_TYPE] = "expression is of wrong type";
    STR_USER_ERROR[-OB_ERR_EXPRESSION_WRONG_TYPE] = "expression is of wrong type";
    ORACLE_ERRNO[-OB_ERR_EXPRESSION_WRONG_TYPE] = 382;
    ORACLE_STR_ERROR[-OB_ERR_EXPRESSION_WRONG_TYPE] = "PLS-00382: expression is of wrong type";
    ORACLE_STR_USER_ERROR[-OB_ERR_EXPRESSION_WRONG_TYPE] = "PLS-00382: expression is of wrong type";
    ERROR_NAME[-OB_ERR_TYPE_SPEC_NOT_EXIST] = "OB_ERR_TYPE_SPEC_NOT_EXIST";
    MYSQL_ERRNO[-OB_ERR_TYPE_SPEC_NOT_EXIST] = -1;
    SQLSTATE[-OB_ERR_TYPE_SPEC_NOT_EXIST] = "HY000";
    STR_ERROR[-OB_ERR_TYPE_SPEC_NOT_EXIST] = "cannot compile type body without its specification";
    STR_USER_ERROR[-OB_ERR_TYPE_SPEC_NOT_EXIST] = "cannot compile body of '%.*s' without its specification";
    ORACLE_ERRNO[-OB_ERR_TYPE_SPEC_NOT_EXIST] = 304;
    ORACLE_STR_ERROR[-OB_ERR_TYPE_SPEC_NOT_EXIST] = "PLS-00304: cannot compile type body without its specification";
    ORACLE_STR_USER_ERROR[-OB_ERR_TYPE_SPEC_NOT_EXIST] =
        "PLS-00304: cannot compile body of '%.*s' without its specification";
    ERROR_NAME[-OB_ERR_TYPE_SPEC_NO_ROUTINE] = "OB_ERR_TYPE_SPEC_NO_ROUTINE";
    MYSQL_ERRNO[-OB_ERR_TYPE_SPEC_NO_ROUTINE] = -1;
    SQLSTATE[-OB_ERR_TYPE_SPEC_NO_ROUTINE] = "HY000";
    STR_ERROR[-OB_ERR_TYPE_SPEC_NO_ROUTINE] =
        "subprogram is declared in an object type body and must be defined in the object type specification";
    STR_USER_ERROR[-OB_ERR_TYPE_SPEC_NO_ROUTINE] =
        "subprogram '%.*s' is declared in an object type body and must be defined in the object type specification";
    ORACLE_ERRNO[-OB_ERR_TYPE_SPEC_NO_ROUTINE] = 539;
    ORACLE_STR_ERROR[-OB_ERR_TYPE_SPEC_NO_ROUTINE] =
        "PLS-00539: subprogram is declared in an object type body and must be defined in the object type specification";
    ORACLE_STR_USER_ERROR[-OB_ERR_TYPE_SPEC_NO_ROUTINE] = "PLS-00539: subprogram '%.*s' is declared in an object type "
                                                          "body and must be defined in the object type specification";
    ERROR_NAME[-OB_ERR_TYPE_BODY_NO_ROUTINE] = "OB_ERR_TYPE_BODY_NO_ROUTINE";
    MYSQL_ERRNO[-OB_ERR_TYPE_BODY_NO_ROUTINE] = -1;
    SQLSTATE[-OB_ERR_TYPE_BODY_NO_ROUTINE] = "HY000";
    STR_ERROR[-OB_ERR_TYPE_BODY_NO_ROUTINE] =
        "subprogram or cursor is declared in an object type specification and must be defined in the object type body";
    STR_USER_ERROR[-OB_ERR_TYPE_BODY_NO_ROUTINE] = "subprogram or cursor '%.*s' is declared in an object type "
                                                   "specification and must be defined in the object type body";
    ORACLE_ERRNO[-OB_ERR_TYPE_BODY_NO_ROUTINE] = 538;
    ORACLE_STR_ERROR[-OB_ERR_TYPE_BODY_NO_ROUTINE] = "PLS-00538: subprogram or cursor is declared in an object type "
                                                     "specification and must be defined in the object type body";
    ORACLE_STR_USER_ERROR[-OB_ERR_TYPE_BODY_NO_ROUTINE] =
        "PLS-00538: subprogram or cursor '%.*s' is declared in an object type specification and must be defined in the "
        "object type body";
    ERROR_NAME[-OB_ERR_IDENTIFIER_TOO_LONG] = "OB_ERR_IDENTIFIER_TOO_LONG";
    MYSQL_ERRNO[-OB_ERR_IDENTIFIER_TOO_LONG] = -1;
    SQLSTATE[-OB_ERR_IDENTIFIER_TOO_LONG] = "HY000";
    STR_ERROR[-OB_ERR_IDENTIFIER_TOO_LONG] = "identifier '%.*s' too long";
    STR_USER_ERROR[-OB_ERR_IDENTIFIER_TOO_LONG] = "identifier '%.*s' too long";
    ORACLE_ERRNO[-OB_ERR_IDENTIFIER_TOO_LONG] = 114;
    ORACLE_STR_ERROR[-OB_ERR_IDENTIFIER_TOO_LONG] = "PLS-00114: identifier '%.*s' too long";
    ORACLE_STR_USER_ERROR[-OB_ERR_IDENTIFIER_TOO_LONG] = "PLS-00114: identifier '%.*s' too long";
    ERROR_NAME[-OB_SP_RAISE_APPLICATION_ERROR] = "OB_SP_RAISE_APPLICATION_ERROR";
    MYSQL_ERRNO[-OB_SP_RAISE_APPLICATION_ERROR] = -1;
    SQLSTATE[-OB_SP_RAISE_APPLICATION_ERROR] = "HY000";
    STR_ERROR[-OB_SP_RAISE_APPLICATION_ERROR] =
        "The stored procedure 'raise_application_error' was called which causes this error to be generated";
    STR_USER_ERROR[-OB_SP_RAISE_APPLICATION_ERROR] = "-%05ld: %.*s";
    ORACLE_ERRNO[-OB_SP_RAISE_APPLICATION_ERROR] = 20000;
    ORACLE_STR_ERROR[-OB_SP_RAISE_APPLICATION_ERROR] =
        "ORA-20000: The stored procedure 'raise_application_error' was called which causes this error to be generated";
    ORACLE_STR_USER_ERROR[-OB_SP_RAISE_APPLICATION_ERROR] = "ORA%06ld: %.*s";
    ERROR_NAME[-OB_SP_RAISE_APPLICATION_ERROR_NUM] = "OB_SP_RAISE_APPLICATION_ERROR_NUM";
    MYSQL_ERRNO[-OB_SP_RAISE_APPLICATION_ERROR_NUM] = -1;
    SQLSTATE[-OB_SP_RAISE_APPLICATION_ERROR_NUM] = "HY000";
    STR_ERROR[-OB_SP_RAISE_APPLICATION_ERROR_NUM] =
        "error number argument to raise_application_error of stringstring is out of range";
    STR_USER_ERROR[-OB_SP_RAISE_APPLICATION_ERROR_NUM] =
        "error number argument to raise_application_error of '%d' is out of range";
    ORACLE_ERRNO[-OB_SP_RAISE_APPLICATION_ERROR_NUM] = 21000;
    ORACLE_STR_ERROR[-OB_SP_RAISE_APPLICATION_ERROR_NUM] =
        "ORA-21000: error number argument to raise_application_error of stringstring is out of range";
    ORACLE_STR_USER_ERROR[-OB_SP_RAISE_APPLICATION_ERROR_NUM] =
        "ORA-21000: error number argument to raise_application_error of '%d' is out of range";
    ERROR_NAME[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] = "OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN";
    MYSQL_ERRNO[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] = -1;
    SQLSTATE[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] = "HY000";
    STR_ERROR[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] = "CLOB or NCLOB in multibyte character set not supported";
    STR_USER_ERROR[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] = "CLOB or NCLOB in multibyte character set not supported";
    ORACLE_ERRNO[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] = 22998;
    ORACLE_STR_ERROR[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] =
        "ORA-22998: CLOB or NCLOB in multibyte character set not supported";
    ORACLE_STR_USER_ERROR[-OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN] =
        "ORA-22998: CLOB or NCLOB in multibyte character set not supported";
    ERROR_NAME[-OB_ERR_UPDATE_TWICE] = "OB_ERR_UPDATE_TWICE";
    MYSQL_ERRNO[-OB_ERR_UPDATE_TWICE] = -1;
    SQLSTATE[-OB_ERR_UPDATE_TWICE] = "HY000";
    STR_ERROR[-OB_ERR_UPDATE_TWICE] = "unable to get a stable set of rows in the source tables";
    STR_USER_ERROR[-OB_ERR_UPDATE_TWICE] = "unable to get a stable set of rows in the source tables";
    ORACLE_ERRNO[-OB_ERR_UPDATE_TWICE] = 30926;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_TWICE] = "ORA-30926: unable to get a stable set of rows in the source tables";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_TWICE] = "ORA-30926: unable to get a stable set of rows in the source tables";
    ERROR_NAME[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = "OB_ERR_FLASHBACK_QUERY_WITH_UPDATE";
    MYSQL_ERRNO[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = -1;
    SQLSTATE[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = "HY000";
    STR_ERROR[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = "snapshot expression not allowed here";
    STR_USER_ERROR[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = "snapshot expression not allowed here";
    ORACLE_ERRNO[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = 8187;
    ORACLE_STR_ERROR[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = "ORA-08187: snapshot expression not allowed here";
    ORACLE_STR_USER_ERROR[-OB_ERR_FLASHBACK_QUERY_WITH_UPDATE] = "ORA-08187: snapshot expression not allowed here";
    ERROR_NAME[-OB_ERR_UPDATE_ON_EXPR] = "OB_ERR_UPDATE_ON_EXPR";
    MYSQL_ERRNO[-OB_ERR_UPDATE_ON_EXPR] = -1;
    SQLSTATE[-OB_ERR_UPDATE_ON_EXPR] = "HY000";
    STR_ERROR[-OB_ERR_UPDATE_ON_EXPR] = "Columns referenced in the ON Clause cannot be updated";
    STR_USER_ERROR[-OB_ERR_UPDATE_ON_EXPR] = "Columns referenced in the ON Clause cannot be updated:'%.*s'.'%.*s'";
    ORACLE_ERRNO[-OB_ERR_UPDATE_ON_EXPR] = 38104;
    ORACLE_STR_ERROR[-OB_ERR_UPDATE_ON_EXPR] = "ORA-38104: Columns referenced in the ON Clause cannot be updated";
    ORACLE_STR_USER_ERROR[-OB_ERR_UPDATE_ON_EXPR] =
        "ORA-38104: Columns referenced in the ON Clause cannot be updated:'%.*s'.'%.*s'";
    ERROR_NAME[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = "OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS";
    MYSQL_ERRNO[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = -1;
    SQLSTATE[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = "HY000";
    STR_ERROR[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = "specified row no longer exists";
    STR_USER_ERROR[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = "specified row no longer exists";
    ORACLE_ERRNO[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = 8006;
    ORACLE_STR_ERROR[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = "ORA-08006: specified row no longer exists";
    ORACLE_STR_USER_ERROR[-OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS] = "ORA-08006: specified row no longer exists";
  }
} local_init;

namespace oceanbase {
namespace common {
const char* ob_error_name(const int err)
{
  const char* ret = "Unknown error";
  if (OB_UNLIKELY(0 == err)) {
    ret = "OB_SUCCESS";
  } else if (OB_LIKELY(0 > err && err > -OB_MAX_ERROR_CODE)) {
    ret = ERROR_NAME[-err];
    if (OB_UNLIKELY(NULL == ret || '\0' == ret[0])) {
      ret = "Unknown Error";
    }
  }
  return ret;
}

const char* ob_strerror(const int err)
{
  const char* ret = "Unknown error";
  if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
    ret = STR_ERROR[-err];
    if (OB_UNLIKELY(NULL == ret || '\0' == ret[0])) {
      ret = "Unknown Error";
    }
  }
  return ret;
}
const char* ob_str_user_error(const int err)
{
  const char* ret = NULL;
  if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
    ret = STR_USER_ERROR[-err];
    if (OB_UNLIKELY(NULL == ret || '\0' == ret[0])) {
      ret = NULL;
    }
  }
  return ret;
}
const char* ob_sqlstate(const int err)
{
  const char* ret = "HY000";
  if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
    ret = SQLSTATE[-err];
    if (OB_UNLIKELY(NULL == ret || '\0' == ret[0])) {
      ret = "HY000";
    }
  }
  return ret;
}
int ob_mysql_errno(const int err)
{
  int ret = -1;
  if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
    ret = MYSQL_ERRNO[-err];
  }
  return ret;
}
int ob_mysql_errno_with_check(const int err)
{
  int ret = ob_mysql_errno(err);
  if (ret < 0) {
    ret = -err;
  }
  return ret;
}
const char* ob_oracle_strerror(const int err)
{
  const char* ret = "Unknown error";
  if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
    ret = ORACLE_STR_ERROR[-err];
    if (OB_UNLIKELY(NULL == ret || '\0' == ret[0])) {
      ret = "Unknown Error";
    }
  }
  return ret;
}
const char* ob_oracle_str_user_error(const int err)
{
  const char* ret = NULL;
  if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
    ret = ORACLE_STR_USER_ERROR[-err];
    if (OB_UNLIKELY(NULL == ret || '\0' == ret[0])) {
      ret = NULL;
    }
  }
  return ret;
}
int ob_oracle_errno(const int err)
{
  int ret = -1;
  if (OB_ERR_PROXY_REROUTE == err) {
    // Oracle Mode and MySQL mode should return same errcode for reroute sql
    // thus we make the specialization here
    ret = -1;
  } else if (err >= OB_MIN_RAISE_APPLICATION_ERROR && err <= OB_MAX_RAISE_APPLICATION_ERROR) {
    ret = err;  // PL/SQL Raise Application Error
  } else if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE)) {
    ret = ORACLE_ERRNO[-err];
  }
  return ret;
}
int ob_oracle_errno_with_check(const int err)
{
  int ret = ob_oracle_errno(err);
  if (ret < 0) {
    ret = -err;
  }
  return ret;
}
int ob_errpkt_errno(const int err, const bool is_oracle_mode)
{
  return (is_oracle_mode ? ob_oracle_errno_with_check(err) : ob_mysql_errno_with_check(err));
}
const char* ob_errpkt_strerror(const int err, const bool is_oracle_mode)
{
  return (is_oracle_mode ? ob_oracle_strerror(err) : ob_strerror(err));
}
const char* ob_errpkt_str_user_error(const int err, const bool is_oracle_mode)
{
  return (is_oracle_mode ? ob_oracle_str_user_error(err) : ob_str_user_error(err));
}

}  // end namespace common
}  // end namespace oceanbase
