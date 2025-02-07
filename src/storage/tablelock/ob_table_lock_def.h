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

// DEF_LOCK_PRIORITY(n, type)
// the type will be #type, the type value will be n
// the name will be #type
#ifdef DEF_LOCK_PRIORITY
DEF_LOCK_PRIORITY(0, HIGH1)
DEF_LOCK_PRIORITY(10, HIGH2)
DEF_LOCK_PRIORITY(20, NORMAL)
DEF_LOCK_PRIORITY(30, LOW)
#endif

// DEF_LOCK_MODE(n, type, name)
// the type will be #type, the type value will be n
// the name will be #name
#ifdef DEF_LOCK_MODE
DEF_LOCK_MODE(0x0, NO_LOCK, N)
DEF_LOCK_MODE(0x8, ROW_SHARE, RS)
DEF_LOCK_MODE(0x4, ROW_EXCLUSIVE, RX)
DEF_LOCK_MODE(0x2, SHARE, S)
DEF_LOCK_MODE(0x6, SHARE_ROW_EXCLUSIVE, SRX)
DEF_LOCK_MODE(0x1, EXCLUSIVE, X)
#endif

// DEF_LOCK_OP_TYPE(n, type)
// the type will be type
// the type name will be #type
#ifdef DEF_LOCK_OP_TYPE
DEF_LOCK_OP_TYPE(1, IN_TRANS_DML_LOCK)
DEF_LOCK_OP_TYPE(2, OUT_TRANS_LOCK)
DEF_LOCK_OP_TYPE(3, OUT_TRANS_UNLOCK)
DEF_LOCK_OP_TYPE(4, IN_TRANS_COMMON_LOCK)
DEF_LOCK_OP_TYPE(5, TABLET_SPLIT)
#endif

// DEF_LOCK_OP_STATUS(n, type)
// the type will be LOCK_OP_##type
// the type name will be #type
#ifdef DEF_LOCK_OP_STATUS
DEF_LOCK_OP_STATUS(1, DOING)
DEF_LOCK_OP_STATUS(2, COMPLETE)
#endif


// DEF_OBJ_TYPE(n, type)
// the type will be OBJ_TYPE_##type
// the type name will be #type
// WARNNING: the type name should less than 24
#ifdef DEF_OBJ_TYPE
DEF_OBJ_TYPE(1, TABLE)
DEF_OBJ_TYPE(2, TABLET)
DEF_OBJ_TYPE(3, COMMON_OBJ)
DEF_OBJ_TYPE(4, LS)
DEF_OBJ_TYPE(5, TENANT)
DEF_OBJ_TYPE(6, EXTERNAL_TABLE_REFRESH)
DEF_OBJ_TYPE(7, ONLINE_DDL_TABLE)
DEF_OBJ_TYPE(8, ONLINE_DDL_TABLET)
DEF_OBJ_TYPE(9, DATABASE_NAME) // for database related ddl
DEF_OBJ_TYPE(10, OBJECT_NAME) // for obj related ddl
DEF_OBJ_TYPE(11, DBMS_LOCK)
DEF_OBJ_TYPE(12, MATERIALIZED_VIEW)
DEF_OBJ_TYPE(13, MYSQL_LOCK_FUNC)
DEF_OBJ_TYPE(14, REFRESH_VECTOR_INDEX)
#endif

// DEF_LOCK_OWNER_TYPE(n, type)
// the type will be type##_OWNER_TYPE
// the name will be #type
#ifdef DEF_LOCK_OWNER_TYPE
DEF_LOCK_OWNER_TYPE(0, DEFAULT)
DEF_LOCK_OWNER_TYPE(1, SESS_ID)
#endif
