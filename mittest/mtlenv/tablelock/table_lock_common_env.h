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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMMON_ENV_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMMON_ENV_

#include "storage/tablelock/ob_table_lock_common.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

uint64_t            DEFAULT_TABLE = 1;
uint64_t            TABLE2 = 2;
uint64_t            TABLE3 = 3;
ObTabletID          DEFAULT_TABLET(1);
ObTabletID          TABLET2(2);
ObTabletID          TABLET3(3);
ObLockID            DEFAULT_TABLET_LOCK_ID;
ObLockID            TABLET_LOCK_ID2;
ObLockID            TABLET_LOCK_ID3;
ObLockID            DEFAULT_TABLE_LOCK_ID;
ObLockID            TABLE_LOCK_ID2;
ObLockID            TABLE_LOCK_ID3;
ObTableLockMode     DEFAULT_LOCK_MODE = ROW_EXCLUSIVE;
ObTableLockMode     DEFAULT_COFLICT_LOCK_MODE = EXCLUSIVE;
ObTableLockOwnerID  DEFAULT_IN_TRANS_OWNER_ID(ObTableLockOwnerID::default_owner());
ObTableLockOwnerID  DEFAULT_OUT_TRANS_OWNER_ID(ObTableLockOwnerID::get_owner_by_value(100));
ObTableLockOwnerID  CONFLICT_OWNER_ID(ObTableLockOwnerID::get_owner_by_value(1));
ObTableLockOwnerID  OWNER_ID2(ObTableLockOwnerID::get_owner_by_value(2));
ObTableLockOwnerID  OWNER_ID3(ObTableLockOwnerID::get_owner_by_value(3));
ObTableLockOpType   DEFAULT_LOCK_OP_TYPE = ObTableLockOpType::IN_TRANS_DML_LOCK;
ObTableLockOpType   OUT_TRANS_LOCK_OP_TYPE = ObTableLockOpType::OUT_TRANS_LOCK;
ObTableLockOpType   OUT_TRANS_UNLOCK_OP_TYPE = ObTableLockOpType::OUT_TRANS_UNLOCK;
ObTableLockOpStatus DEFAULT_LOCK_OP_STATUS = ObTableLockOpStatus::LOCK_OP_DOING;
ObTableLockOpStatus COMMIT_LOCK_OP_STATUS = ObTableLockOpStatus::LOCK_OP_COMPLETE;
ObTransID           DEFAULT_TRANS_ID = 1;
ObTransID           TRANS_ID2 = 2;
ObTransID           TRANS_ID3 = 3;

ObTableLockOp       DEFAULT_IN_TRANS_LOCK_OP;
ObTableLockOp       DEFAULT_OUT_TRANS_LOCK_OP;
ObTableLockOp       DEFAULT_OUT_TRANS_UNLOCK_OP;

ObTableLockOp       DEFAULT_CONFLICT_OUT_TRANS_LOCK_OP;

void init_default_lock_test_value()
{
  int ret = OB_SUCCESS;
  const ObTxSEQ seq_no(100,0);
  const int64_t create_timestamp = 1;
  const int64_t create_schema_version = 1;

  ret =  get_lock_id(DEFAULT_TABLET, DEFAULT_TABLET_LOCK_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret =  get_lock_id(TABLET2, TABLET_LOCK_ID2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret =  get_lock_id(TABLET3, TABLET_LOCK_ID3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret =  get_lock_id(DEFAULT_TABLE, DEFAULT_TABLE_LOCK_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret =  get_lock_id(TABLE2, TABLE_LOCK_ID2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret =  get_lock_id(TABLE3, TABLE_LOCK_ID3);
  ASSERT_EQ(OB_SUCCESS, ret);

  DEFAULT_IN_TRANS_LOCK_OP.set(DEFAULT_TABLET_LOCK_ID,
                               DEFAULT_LOCK_MODE,
                               DEFAULT_IN_TRANS_OWNER_ID,
                               DEFAULT_TRANS_ID,
                               DEFAULT_LOCK_OP_TYPE,
                               DEFAULT_LOCK_OP_STATUS,
                               seq_no,
                               create_timestamp,
                               create_schema_version);

  DEFAULT_OUT_TRANS_LOCK_OP.set(DEFAULT_TABLE_LOCK_ID,
                                DEFAULT_LOCK_MODE,
                                DEFAULT_OUT_TRANS_OWNER_ID,
                                DEFAULT_TRANS_ID,
                                OUT_TRANS_LOCK_OP_TYPE,
                                DEFAULT_LOCK_OP_STATUS,
                                seq_no,
                                create_timestamp,
                                create_schema_version);

  DEFAULT_OUT_TRANS_UNLOCK_OP.set(DEFAULT_TABLE_LOCK_ID,
                                  DEFAULT_LOCK_MODE,
                                  DEFAULT_OUT_TRANS_OWNER_ID,
                                  DEFAULT_TRANS_ID,
                                  OUT_TRANS_UNLOCK_OP_TYPE,
                                  DEFAULT_LOCK_OP_STATUS,
                                  seq_no,
                                  create_timestamp,
                                  create_schema_version);

  DEFAULT_CONFLICT_OUT_TRANS_LOCK_OP.set(DEFAULT_TABLET_LOCK_ID,
                                         DEFAULT_COFLICT_LOCK_MODE,
                                         CONFLICT_OWNER_ID,
                                         TRANS_ID2,
                                         OUT_TRANS_LOCK_OP_TYPE,
                                         DEFAULT_LOCK_OP_STATUS,
                                         seq_no,
                                         create_timestamp,
                                         create_schema_version);
}

} // tablelock
} // transaction
} // oceanbase

#endif
