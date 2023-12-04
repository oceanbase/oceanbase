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

#ifndef OB_DEFINE_H
#define OB_DEFINE_H

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/profile/ob_trace_id.h"
#include "common/ob_tablet_id.h"
#include "share/ob_errno.h"
#include "lib/worker.h"
#include "share/ob_ls_id.h"
#include "cmath"
#include <features.h>
#if __GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ > 17)
using std::isinf;
using std::isnan;
#endif

/****** UTILS FOR PROGRAMMING *****/
#define CK_1(a1)\
  CK_0(#a1, a1)
#define CK_2(a1, a2) \
  CK_1(a1) else CK_0(#a2, a2)
#define CK_3(a1, a2, a3) \
  CK_2(a1, a2) else CK_0(#a3, a3)
#define CK_4(a1, a2, a3, a4) \
  CK_3(a1, a2, a3) else CK_0(#a4, a4)
#define CK_5(a1, a2, a3, a4, a5) \
  CK_4(a1, a2, a3, a4) else CK_0(#a5, a5)
#define CK_6(a1, a2, a3, a4, a5, a6) \
  CK_5(a1, a2, a3, a4, a5) else CK_0(#a6, a6)
#define CK_7(a1, a2, a3, a4, a5, a6, a7) \
  CK_6(a1, a2, a3, a4, a5, a6) else CK_0(#a7, a7)
#define CK_8(a1, a2, a3, a4, a5, a6, a7, a8) \
  CK_7(a1, a2, a3, a4, a5, a6, a7) else CK_0(#a8, a8)
#define CK_9(a1, a2, a3, a4, a5, a6, a7, a8, a9) \
  CK_8(a1, a2, a3, a4, a5, a6, a7, a8) else CK_0(#a9, a9)
#define CK_10(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) \
  CK_9(a1, a2, a3, a4, a5, a6, a7, a8, a9) else CK_0(#a10, a10)
#define CK_11(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) \
  CK_10(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) else CK_0(#a11, a11)
#define CK_12(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) \
  CK_11(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) else CK_0(#a12, a12)
#define CK_13(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) \
  CK_12(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) else CK_0(#a13, a13)
#define CK_14(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) \
  CK_13(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) else CK_0(#a14, a14)
#define CK_15(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) \
  CK_14(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) else CK_0(#a15, a15)
#define CK_16(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16) \
  CK_15(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) else CK_0(#a16, a16)

#define CK_0(a, b)                              \
  if (!(b)) {                                   \
    ret = OB_ERR_UNEXPECTED;                    \
    LOG_WARN("invalid arguments", a, b);        \
  }

// Reference document:
// Check every argument and stop to print if anyone of them is false
#define CK(...)                                                         \
  if (OB_SUCC(ret)) { CONCAT(CK_, ARGS_NUM(__VA_ARGS__))(__VA_ARGS__) }

// Reference document:
// execute an instruction
#define OX(statement)                           \
  if (OB_SUCC(ret)) {                           \
    statement;                                  \
  }


/*

  参考文档：

  This better be the last macro we ever need to define in the
  O-series, hence 'Z'.

  OZ( f(a1, a2, a3) );          // print ret in case of failure
  OZ( f(a1, a2, a3), a3 );      // print ret, a3 in case of failure
  OZ( f(a1, a2, a3), a2, a3 );  // print ret, a2, a3 in case of failure
*/
#define OZ(func, ...) OC_I5(func, OC_I4(__VA_ARGS__))


#define OC_I4(...) ret, ##__VA_ARGS__
#define OC_I3(...) OC_I4(__VA_ARGS__)
#define OC_I(func) OC_I1(func,
#define OC_I1(func, a) OC_I2(func, a)
#define KK(a) K(a)
#define OC_I2(func, a)                            \
  if (OB_SUCC(ret)) {                             \
    if (OB_FAIL(func a)) {                        \
      LOG_WARN("fail to exec "#func #a,           \
               LST_DO(KK, (,), OC_I3(EXPAND a))); \
    }                                             \
  }

// Should be combined with OC_I2...
#define OC_I5(func, a)                          \
  do {                                          \
    if (OB_SUCC(ret)) {                         \
      if (OB_FAIL(func)) {                      \
        LOG_WARN("fail to exec "#func,          \
                 LST_DO(KK, (,), a));           \
      }                                         \
    }                                           \
  } while(0)

/*
  OZ的扩展，增加一个retcode，当返回值为retcode时，用log info代替log warn，减少不必要的刷屏
  eg:
  OZX1( f(a1, a2, a3), OB_ERR_NO_PRIVILEGE);          // print ret in case of failure
  OZX1( f(a1, a2, a3), OB_ERR_NO_PRIVILEGE, a3 );      // print ret, a3 in case of failure
  OZX1( f(a1, a2, a3), OB_ERR_NO_PRIVILEGE, a2, a3 );  // print ret, a2, a3 in case of failure
*/
#define OZX1(func, ret_code, ...) OCX1_I5(func, ret_code, OC_I4(__VA_ARGS__))

// Should be combined with OC_I2...
#define OCX1_I5(func, ret_code, a)              \
  do {                                          \
    if (OB_SUCC(ret)) {                         \
      if (OB_FAIL(func)) {                      \
        if (ret == ret_code) {                  \
          LOG_DEBUG("fail to exec "#func,       \
                   LST_DO(KK, (,), a));         \
        } else {                                \
          LOG_WARN("fail to exec "#func,        \
                   LST_DO(KK, (,), a));         \
        }                                       \
      }                                         \
    }                                           \
  } while(0)

/*
  OZ的扩展，增加一个retcode，当返回值为retcode时，用log info代替log warn，减少不必要的刷屏
  OZX2( f(a1, a2, a3), OB_ERR_NO_PRIVILEGE, OB_ERR_EMPTY_QUERY); // print ret in case of failure
  OZX2( f(a1, a2, a3), OB_ERR_NO_PRIVILEGE, OB_ERR_EMPTY_QUERY, a3 );
  OZX2( f(a1, a2, a3), OB_ERR_NO_PRIVILEGE, OB_ERR_EMPTY_QUERY, a2, a3 );
*/
#define OZX2(func, ret_code1, ret_code2, ...) OCX2_I5(func, ret_code1, ret_code2, OC_I4(__VA_ARGS__))

// Should be combined with OC_I2...
#define OCX2_I5(func, ret_code1, ret_code2, a)  \
  do {                                          \
    if (OB_SUCC(ret)) {                         \
      if (OB_FAIL(func)) {                      \
        if (ret == ret_code1                    \
            || ret == ret_code2) {              \
          LOG_DEBUG("fail to exec "#func,       \
                   LST_DO(KK, (,), a));         \
        } else {                                \
          LOG_WARN("fail to exec "#func,        \
                   LST_DO(KK, (,), a));         \
        }                                       \
      }                                         \
    }                                           \
  } while(0)

#define OV(...) \
  CONCAT(OV_, ARGS_NUM(__VA_ARGS__))(__VA_ARGS__)

#define OV_0()                         OV_I5(false, common::OB_ERR_UNEXPECTED)
#define OV_1(condition)                OV_I5(condition, common::OB_ERR_UNEXPECTED)
#define OV_2(condition, errcode)       OV_I5(condition, errcode)
#define OV_3(condition, errcode, ...)  OV_I5(condition, errcode, __VA_ARGS__)
#define OV_4(condition, errcode, ...)  OV_I5(condition, errcode, __VA_ARGS__)
#define OV_5(condition, errcode, ...)  OV_I5(condition, errcode, __VA_ARGS__)
#define OV_6(condition, errcode, ...)  OV_I5(condition, errcode, __VA_ARGS__)
#define OV_7(condition, errcode, ...)  OV_I5(condition, errcode, __VA_ARGS__)
#define OV_8(condition, errcode, ...)  OV_I5(condition, errcode, __VA_ARGS__)
#define OV_9(condition, errcode, ...)  OV_I5(condition, errcode, __VA_ARGS__)

#define OV_I4(...) ret, ##__VA_ARGS__
#define OV_I5(condition, errcode, ...)                \
  if (common::OB_SUCCESS == (ret)) {                  \
    if (OB_UNLIKELY(!(condition))) {                  \
      ret = (errcode);                                \
      LOG_WARN("fail to check ("#condition")",        \
               LST_DO(KK, (,), OV_I4(__VA_ARGS__)));  \
    }                                                 \
  }

// run the specified function and print out every argument
// in case of failure (obsoleted)
#define OC(func) OC_I func )

namespace oceanbase {
namespace common {

// iternal recyclebin object prefix
const char *const OB_MYSQL_RECYCLE_PREFIX = "__recycle_$_";
const char *const OB_ORACLE_RECYCLE_PREFIX = "RECYCLE_$_";

OB_INLINE bool is_valid_log_compressor_type(common::ObCompressorType compressor_type)
{
   bool b_ret = false;
   if (common::ObCompressorType::LZ4_COMPRESSOR == compressor_type
   || common::ObCompressorType::ZSTD_COMPRESSOR == compressor_type
   || common::ObCompressorType::ZSTD_1_3_8_COMPRESSOR == compressor_type) {
    b_ret = true;
   }
   return b_ret;
}
//check whether transaction version is valid
OB_INLINE bool is_valid_trans_version(const int64_t trans_version)
{
  // When the observer has not performed any transactions, publish_version is 0
  return trans_version >= 0;
}

OB_INLINE bool is_valid_membership_version(const int64_t membership_version)
{
  // When the observer does not perform any member changes, membership_version is 0
  return membership_version >= 0;
}

OB_INLINE bool is_valid_read_snapshot_version(const int64_t read_snapshot_version)
{
  // read snapshot version should be greater than 0 and should not be INT64_MAX
  return read_snapshot_version > 0 && INT64_MAX != read_snapshot_version;
}

inline bool is_need_retry_interval_part_error(int code)
{
  bool ret = false;
  if (OB_ERR_INTERVAL_PARTITION_EXIST == code
     || OB_ERR_INTERVAL_PARTITION_ERROR == code) {
    ret = true;
  }
  return ret;
}

inline bool is_schema_error(int err)
{
  bool ret = false;
  switch(err) {
    case OB_TENANT_EXIST:
    case OB_TENANT_NOT_EXIST:
    case OB_ERR_BAD_DATABASE:
    case OB_DATABASE_EXIST:
    case OB_TABLEGROUP_NOT_EXIST:
    case OB_TABLEGROUP_EXIST:
    case OB_TABLE_NOT_EXIST:
    case OB_ERR_TABLE_EXIST:
    case OB_ERR_BAD_FIELD_ERROR:
    case OB_ERR_COLUMN_DUPLICATE:
    case OB_ERR_USER_EXIST:
    case OB_ERR_USER_NOT_EXIST:
    case OB_ERR_NO_PRIVILEGE:
    case OB_ERR_NO_DB_PRIVILEGE:
    case OB_ERR_NO_TABLE_PRIVILEGE:
    case OB_SCHEMA_ERROR:
    case OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH:
    case OB_ERR_REMOTE_SCHEMA_NOT_FULL:
    case OB_ERR_SP_ALREADY_EXISTS:
    case OB_ERR_SP_DOES_NOT_EXIST:
    case OB_OBJECT_NAME_NOT_EXIST:
    case OB_OBJECT_NAME_EXIST:
    case OB_SCHEMA_EAGAIN:
    case OB_SCHEMA_NOT_UPTODATE:
    case OB_ERR_PARALLEL_DDL_CONFLICT:
      ret = true;
      break;
    default:
      break;
  }
  return ret;
}

// this function only used for error logging
// expr eval error range (-5000, -6000]
inline bool should_catch_err(int err)
{
  bool ret = false;
  // think that expr_eval err only in (-5000, -6000] should catch
  if (err > -6000 && err < -5000) {
    ret = true;
  } else {
    switch (err) {
    case OB_ERR_DIVISOR_IS_ZERO:
    case OB_INVALID_DATE_VALUE:
    case OB_INVALID_DATE_FORMAT:
    case OB_BAD_NULL_ERROR:
    case OB_ERR_VALUE_LARGER_THAN_ALLOWED:
      ret = true;
      break;
    default:
      break;
    }
  }
  return ret;
}

inline bool is_duplicate_key_err(int err)
{
  bool ret = false;
  if (OB_ERR_PRIMARY_KEY_DUPLICATE == err) {
    ret = true;
  }
  return ret;
}

inline bool is_get_location_timeout_error(int err)
{
  return OB_GET_LOCATION_TIME_OUT == err;
}

inline bool is_partition_change_error(int err)
{
  bool ret = false;
  switch (err) {
    case OB_PARTITION_NOT_EXIST:
    case OB_LOCATION_NOT_EXIST:
    case OB_PARTITION_IS_STOPPED:
    case OB_PARTITION_IS_BLOCKED:
    case OB_LS_LOCATION_NOT_EXIST:
    case OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST:
    case OB_LS_NOT_EXIST:
    case OB_TABLET_NOT_EXIST:
      ret = true;
      break;
    default:
      break;
  }
  return ret;
}

inline bool is_server_down_error(int err)
{
  bool ret = false;
  ret = (OB_RPC_CONNECT_ERROR == err || OB_RPC_SEND_ERROR == err || OB_RPC_POST_ERROR == err);
  return ret;
}

inline bool is_trans_stmt_need_retry_error(int err)
{
  bool ret = false;
  ret = (OB_TRANS_STMT_NEED_RETRY == err);
  return ret;
}

inline bool is_server_status_error(int err)
{
  bool ret = false;
  ret = (OB_SERVER_IS_INIT == err || OB_SERVER_IS_STOPPING == err);
  return ret;
}

inline bool is_unit_migrate(int err)
{
  return OB_TENANT_NOT_IN_SERVER == err;
}

inline bool is_process_timeout_error(int err)
{
  bool ret = false;
  ret = (OB_TIMEOUT == err);
  return ret;
}

inline bool is_location_leader_not_exist_error(int err)
{
  return OB_LOCATION_LEADER_NOT_EXIST == err
      || OB_LS_LOCATION_LEADER_NOT_EXIST == err;
}

inline bool is_master_changed_error(int err)
{
  bool ret = false;
  switch (err) {
    case OB_LOCATION_LEADER_NOT_EXIST:
    case OB_LS_LOCATION_LEADER_NOT_EXIST:
    case OB_NOT_MASTER:
    case OB_RS_NOT_MASTER:
    case OB_RS_SHUTDOWN:
      ret = true;
      break;
    default:
      ret = false;
      break;
  }
  return ret;
}

inline bool is_timeout_err(int err)
{
  return OB_TIMEOUT == err
      || OB_TRANS_TIMEOUT == err
      || OB_TRANS_STMT_TIMEOUT == err
      || OB_TRANS_RPC_TIMEOUT == err;
}

inline bool is_not_supported_err(int err)
{
  return OB_NOT_SUPPORTED == err;
}

inline bool is_try_lock_row_err(int err)
{
  return OB_TRY_LOCK_ROW_CONFLICT == err;
}

inline bool is_transaction_set_violation_err(int err)
{
  return OB_TRANSACTION_SET_VIOLATION == err;
}

inline bool is_transaction_cannot_serialize_err(int err)
{
  return OB_TRANS_CANNOT_SERIALIZE == err;
}

inline bool is_snapshot_discarded_err(const int err)
{
  return OB_SNAPSHOT_DISCARDED == err;
}

inline bool is_transaction_rpc_timeout_err(int err)
{
  return OB_TRANS_RPC_TIMEOUT == err;
}

inline bool is_data_not_readable_err(int err)
{
  return OB_DATA_NOT_UPTODATE == err
         || OB_REPLICA_NOT_READABLE == err
         || OB_SNAPSHOT_DISCARDED == err;
}

inline bool is_has_no_readable_replica_err(int err)
{
  return OB_NO_READABLE_REPLICA == err;
}

inline bool is_partition_splitting(const int err)
{
  return OB_PARTITION_IS_SPLITTING == err;
}

inline bool is_id_not_ready_err(const int err)
{
  return OB_GTS_NOT_READY == err || OB_GTI_NOT_READY == err;
}

inline bool is_weak_read_service_ready_err(const int err)
{
  return OB_TRANS_WEAK_READ_VERSION_NOT_READY == err;
}

inline bool is_select_dup_follow_replic_err(const int err)
{
  return OB_USE_DUP_FOLLOW_AFTER_DML == err;
}

inline bool is_static_engine_retry(const int err)
{
  return STATIC_ENG_NOT_IMPLEMENT == err;
}

inline void set_interval_partition_insert_error(int &ret)
{
  ret = OB_NO_PARTITION_FOR_INTERVAL_PART;
}
inline bool is_interval_partition_insert_error(const int err)
{
  return OB_NO_PARTITION_FOR_INTERVAL_PART == err;
}

inline bool is_query_killed_return(const int ret)
{
  // TODO(handora.qc): check the mode for OB_DEAD_LOCK
  return OB_ERR_QUERY_INTERRUPTED == ret
    || OB_DEAD_LOCK == ret;
}

//@TODO shanyan.g Temporary settings for elr
static const bool CAN_ELR = false;

#define LOG_WARN_IGNORE_ITER_END(ret, fmt, args...) \
  do {\
    if (OB_UNLIKELY(common::OB_ITER_END != ret)) {\
      LOG_WARN(fmt, ##args);\
    }\
  } while(0);

// Weakly consistent read related macros
const int64_t OB_WRS_LEVEL_VALUE_LENGTH = 128; // Maximum length of the level_value field of the __all_weak_read_service internal table
const int64_t OB_WRS_LEVEL_NAME_LENGTH = 128; // Maximum length of the level_name field of the __all_weak_read_service internal table

//Encryption related macros
const int64_t OB_MAX_ENCRYPTION_NAME_LENGTH = 128;
const int64_t OB_MAX_ENCRYPTION_KEY_NAME_LENGTH = 256;
const char *const OB_MYSQL_ENCRYPTION_DEFAULT_MODE = "aes-128";
const char *const OB_MYSQL_ENCRYPTION_NONE_MODE = "none";
//--end---Encryption related macros
const int64_t OB_MAX_ENCRYPTION_MODE_LENGTH = 64;

/**
 *  整理发现内部表, 内部视图的定义中有很多位置使用了OB_MAX_TABLE_NAME_LENGTH来限制字段长度为128字节,
 *  但是对应字段的语意并不是table_name。
 *  由于需要调整OB_MAX_TABLE_NAME_LENGTH长度为256, 为保证使用了OB_MAX_TABLE_NAME_LENGTH的内部表, 视图定义不变,
 *  新增以下定义替代原有的OB_MAX_TABLE_NAME_LENGTH
 */
const int64_t OB_MAX_CORE_TALBE_NAME_LENGTH = 128;
const int64_t OB_MAX_OUTLINE_NAME_LENGTH = 128;
const int64_t OB_MAX_ROUTINE_NAME_LENGTH = 128;
const int64_t OB_MAX_ROUTINE_NAME_BINARY_LENGTH = 2048; // Should be OB_MAX_ROUTINE_NAME_LENGTH * 4(max char bytes),
                                                         // reserve some bytes thus OB_MAX_ROUTINE_NAME_LENGTH changes will probably not influence it
                                                         // it is defined in primary key, and can not change randomly.
const int64_t OB_MAX_PACKAGE_NAME_LENGTH = 128;
const int64_t OB_MAX_KVCACHE_NAME_LENGTH = 128;
const int64_t OB_MAX_SYNONYM_NAME_LENGTH = 128;
const int64_t OB_MAX_PARAMETERS_NAME_LENGTH = 128;
const int64_t OB_MAX_RESOURCE_PLAN_NAME_LENGTH = 128;
// end for const define replace OB_MAX_TABLE_NAME_LENGTH

///////////////////////////////////////////////////////
//          Schema defination                        //

// internal aux-vertical partition table name prefix
const char *const OB_AUX_VP_PREFIX = "__AUX_VP_";

//          End of Schema defination                 //
///////////////////////////////////////////////////////

const int64_t OB_STATUS_LENGTH = 64;


///////////////////////////
//// used for replay
const int64_t REPLAY_TASK_QUEUE_SIZE = 32;
const int64_t APPLY_TASK_QUEUE_SIZE = 32;
inline int64_t &get_replay_queue_index()
{
  struct DEFAULT_WRAPPER {
    DEFAULT_WRAPPER() : v_(-1) {}
    int64_t v_;
  };
  RLOCAL_INLINE(DEFAULT_WRAPPER, replay_queue_index);
  return (&replay_queue_index)->v_;
}

inline bool &get_replay_is_writing_throttling()
{
  struct DEFAULT_WRAPPER {
    DEFAULT_WRAPPER() : v_(false) {}
    bool v_;
  };
  RLOCAL_INLINE(DEFAULT_WRAPPER, is_writing_throttling);
  return (&is_writing_throttling)->v_;
}
///////////////////////////////////

enum ObDmlEventType
{
  DE_INVALID = 0,
  DE_INSERTING = (1 << 0),
  DE_UPDATING = (1 << 1),
  DE_DELETING = (1 << 2)
};

const char *const NORMAL_MODE_STR = "normal";
const char *const FLASHBACK_MODE_STR = "physical_flashback";
const char *const ARBITRATION_MODE_STR = "arbitration";
const char *const FLASHBACK_VERIFY_MODE_STR = "physical_flashback_verify";
const char *const DISABLED_CLUSTER_MODE_STR = "disabled_cluster";
const char *const DISABLED_WITH_READONLY_CLUSTER_MODE_STR = "disabled_with_readonly_cluster";

static const int64_t MODIFY_GC_SNAPSHOT_INTERVAL = 2 * 1000 * 1000; //2s

//reserved table id for information schema
const uint64_t OB_ALL_VIRTUAL_PARAMETERS_OLD_TID = 12037; // "PARAMETERS_OLD"
const uint64_t OB_ALL_VIRTUAL_TABLE_CONSTRAINTS_OLD_TID = 12005; // "TABLE_CONSTRAINTS_OLD"
const uint64_t OB_ALL_VIRTUAL_REFERENTIAL_CONSTRAINTS_OLD_TID = 12177; // "REFERENTIAL_CONSTRAINTS_OLD"
const uint64_t OB_ALL_VIRTUAL_CHECK_CONSTRAINTS_OLD_TID = 12235; // "CHECK_CONSTRAINTS_OLD"
const uint64_t OB_ALL_VIRTUAL_TRIGGERS_OLD_TID = 12221; // "TRIGGERS_OLD"
const uint64_t OB_TABLE_PRIVILEGES_OLD_TID = 12002;  // not used anymore for "TABLE_PRIVILEGES" has a new table id
const uint64_t OB_USER_PRIVILEGES_OLD_TID = 12003;   // not used anymore for "USER_PRIVILEGES" has a new table id
const uint64_t OB_SCHEMA_PRIVILEGES_OLD_TID = 12004; // not used anymore for "SCHEMA_PRIVILEGES" has a new table id
const uint64_t OB_PARTITIONS_OLD_TID = 12007;        // not used anymore for "PARTITIONS" has a new table id
//end of reserved table id for information schema

////////////////typedef
typedef common::ObSEArray<int64_t, 8> PartitionIdArray;
///////////////
}  // common
namespace share
{
// alias
using ObTaskId = ::oceanbase::common::ObCurTraceId::TraceId;
}  // share
}  // oceanbase

#endif /* OB_DEFINE_H */
