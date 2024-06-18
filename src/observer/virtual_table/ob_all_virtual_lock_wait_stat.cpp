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

#include "observer/virtual_table/ob_all_virtual_lock_wait_stat.h"

#include "storage/memtable/ob_lock_wait_mgr.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::rpc;
using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

void ObAllVirtualLockWaitStat::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

bool ObAllVirtualLockWaitStat::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualLockWaitStat::release_last_tenant()
{
  rowkey_[0] = '\0';
  lock_mode_[0] = '\0';

  // let next tenant to init init txs_,
  // ls_id_iter_ and tx_lock_stat_iter_
  start_to_read_ = false;
  node_iter_ = nullptr;
}

int ObAllVirtualLockWaitStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

int ObAllVirtualLockWaitStat::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (!start_to_read_ && OB_FAIL(make_this_ready_to_read())) {
    SERVER_LOG(WARN, "prepare_start_to_read_ error", K(ret), K(start_to_read_));
  } else if (OB_ISNULL(node_iter_ = MTL(memtable::ObLockWaitMgr *)
                                        ->next(node_iter_, &cur_node_))) {
    ret = OB_ITER_END;
  } else {
    int type = 0; // 1-TR 2-TX 3-TM
    get_lock_type(node_iter_->hash_, type);
    const int64_t col_count = output_column_ids_.count();
    ObString ipstr;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        // svr_ip
        case SVR_IP: {
          ipstr.reset();
          if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
            SERVER_LOG(ERROR, "get server ip failed", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(ipstr);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        // svr_port
        case SVR_PORT: {
          cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
          break;
        }
        case TENANT_ID: {
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        }
        case TABLET_ID:
          cur_row_.cells_[i].set_int(node_iter_->tablet_id_);
          break;
        case ROWKEY:
          {
            snprintf(rowkey_, sizeof(rowkey_), "%s", node_iter_->key_);
            cur_row_.cells_[i].set_varchar(rowkey_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case ADDR:
          cur_row_.cells_[i].set_uint64((uint64_t)node_iter_->addr_);
          break;
        case NEED_WAIT:
          cur_row_.cells_[i].set_bool(node_iter_->need_wait_);
          break;
        case RECV_TS:
          cur_row_.cells_[i].set_int(node_iter_->recv_ts_);
          break;
        case LOCK_TS:
          cur_row_.cells_[i].set_int(node_iter_->lock_ts_);
          break;
        case ABS_TIMEOUT:
          cur_row_.cells_[i].set_int(node_iter_->abs_timeout_);
          break;
        case TRY_LOCK_TIMES:
          cur_row_.cells_[i].set_int(node_iter_->try_lock_times_);
          break;
        case TIME_AFTER_RECV:
          {
            int64_t cur_ts = ObTimeUtility::current_time();
            cur_row_.cells_[i].set_int(cur_ts - node_iter_->recv_ts_);
            break;
          }
        case SESSION_ID:
          cur_row_.cells_[i].set_int(node_iter_->sessid_);
          break;
        case HOLDER_SESSION_ID:
          cur_row_.cells_[i].set_int(node_iter_->holder_sessid_);
          break;
        case BLOCK_SESSION_ID:
          cur_row_.cells_[i].set_int(node_iter_->block_sessid_);
          break;
        case TYPE:
          cur_row_.cells_[i].set_int(type);
          break;
        case LMODE: {
          // For compatibility, column type should be determined by schema before cluster is in upgrade mode.
          const ObColumnSchemaV2 *tmp_column_schema = nullptr;
          bool type_is_varchar = true;
          if (OB_ISNULL(table_schema_) ||
              OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
          } else {
            type_is_varchar = tmp_column_schema->get_meta_type().is_character_type();
          }
          if (OB_SUCC(ret)) {
            if (type_is_varchar) {
              if (type == 1 || type == 2) {
                cur_row_.cells_[i].set_varchar("X");
              } else if (type == 3) {
                char lock_mode_tmp[MAX_LOCK_MODE_BUF_LENGTH];
                if (OB_FAIL(lock_mode_to_string(node_iter_->lock_mode_,
                                                lock_mode_tmp,
                                                sizeof(lock_mode_tmp)))) {
                  SERVER_LOG(WARN, "get lock mode failed", K(ret),
                              K(node_iter_));
                } else {
                  snprintf(lock_mode_, sizeof(lock_mode_), "%s",
                            lock_mode_tmp);
                  cur_row_.cells_[i].set_varchar(lock_mode_);
                }
              }
              cur_row_.cells_[i].set_collation_type(
                  ObCharset::get_default_collation(
                      ObCharset::get_default_charset()));
            } else {
              // this column is invalid when the
              // version of observer is before 4.2
              cur_row_.cells_[i].set_int(0);
            }
          }
          break;
        }
        case LAST_COMPACT_CNT:
          cur_row_.cells_[i].set_int(node_iter_->last_compact_cnt_);
          break;
        case TOTAL_UPDATE_CNT:
          cur_row_.cells_[i].set_int(node_iter_->total_update_cnt_);
          break;
        case TRANS_ID:
          cur_row_.cells_[i].set_int(node_iter_->tx_id_);
          break;
        case HOLDER_TRANS_ID: {
          // The default value is holder_tx_id on the ObLockWaitNode,
          // which is the holder for TX lock only gererated when the
          // row is dumped into sstable.
          // Actually, the lock on a row, which we named it as TR lock, is a
          // part of TX. It represents a class of resources held by TX lock.
          ObTransID holder_tx_id { node_iter_->holder_tx_id_ };
          // If the waiter is waitting on the row, we need to get the
          // real holder from the hash_holder, instead of the holder_tx_id
          // on the ObLockWaitNode.
          if (type == 2) {
            // TODO(yangyifei.yyf): rowkey holder is unstable now, so we use
            // tmp ret to catch error code here. We we fix it in the future.
            if (OB_TMP_FAIL(get_rowkey_holder(node_iter_->hash_, holder_tx_id))) {
              SERVER_LOG(WARN, "can not get the hash holder from lock wait mgr",
                         K_(node_iter_->tablet_id), K_(node_iter_->key),
                         K_(node_iter_->tx_id), K_(node_iter_->hash));
            }
          }
          cur_row_.cells_[i].set_int(holder_tx_id.get_id());
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualLockWaitStat::get_lock_type(int64_t hash, int &type)
{
  int ret = OB_SUCCESS;
  type = 0; // invalid type
  if (LockHashHelper::is_rowkey_hash(hash)) {
    type = 1;
  } else if (LockHashHelper::is_trans_hash(hash)) {
    type = 2;
  } else if (LockHashHelper::is_table_lock_hash(hash)) {
    type = 3;
  }
  return ret;
}

int ObAllVirtualLockWaitStat::get_rowkey_holder(int64_t hash, transaction::ObTransID &holder)
{
  int ret = OB_SUCCESS;
  ObLockWaitMgr *lwm = NULL;
  if (OB_ISNULL(lwm = MTL(ObLockWaitMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "MTL(LockWaitMgr) is null");
  } else if (OB_FAIL(lwm->get_hash_holder(hash, holder))){
    SERVER_LOG(WARN, "get rowkey holder from lock wait mgr failed", K(ret), K(hash));
  }
  return ret;
}

int ObAllVirtualLockWaitStat::make_this_ready_to_read()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid allocator is NULL", K(allocator_), K(ret));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
