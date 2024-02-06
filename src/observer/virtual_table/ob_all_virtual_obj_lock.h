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

#ifndef OB_ALL_VIRTUAL_OB_OBJ_LOCK_H_
#define OB_ALL_VIRTUAL_OB_OBJ_LOCK_H_

#include "observer/omt/ob_multi_tenant_operator.h"
#include "storage/tablelock/ob_obj_lock.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualObjLock : public common::ObVirtualTableScannerIterator,
                            public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualObjLock();
  virtual ~ObAllVirtualObjLock();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int get_next_ls(ObLS *&ls);
  int get_next_obj_lock_or_iter_tx(ObLockID &lock_id);
  int get_next_lock_op(transaction::tablelock::ObTableLockOp &lock_op);

private:
  enum
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    LOCK_ID,
    LOCK_MODE,
    OWNER_ID,
    CREATE_TRANS_ID,
    OP_TYPE,
    OP_STATUS,
    TRANS_VERSION,
    CREATE_TIMESTAMP,
    CREATE_SCHEMA_VERSION,
    EXTRA_INFO,
    TIME_AFTER_CREATE,
    OBJ_TYPE,
    OBJ_ID
  };
private:
  common::ObAddr addr_;
  int64_t ls_id_;
  ObLS *ls_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  // the lock id of a ls
  ObLockIDIterator obj_lock_iter_;
  // the lock op of a obj lock
  ObLockOpIterator lock_op_iter_;
  // whether iterate tx or not now.
  bool is_iter_tx_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char lock_id_buf_[common::MAX_LOCK_ID_BUF_LENGTH];
  char lock_mode_buf_[common::MAX_LOCK_MODE_BUF_LENGTH];
  char lock_obj_type_buf_[common::MAX_LOCK_OBJ_TYPE_BUF_LENGTH];
  char lock_op_type_buf_[common::MAX_LOCK_OP_TYPE_BUF_LENGTH];
  char lock_op_status_buf_[common::MAX_LOCK_OP_STATUS_BUF_LENGTH];
  char lock_op_extra_info_[common::MAX_LOCK_OP_EXTRA_INFO_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualObjLock);
};

} // observer
} // oceanbase
#endif
