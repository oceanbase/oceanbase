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

#define USING_LOG_PREFIX STORAGE

#include "lib/guard/ob_shared_guard.h"
#include "observer/ob_service.h"
#include "observer/ob_srv_network_frame.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/rc/ob_tenant_base.h"  // MTL_IS_RESTORE_TENANT
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_lock.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/ls/ob_ls_state.h"
#include "storage/tx_storage/ob_ls_safe_destroy_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/ob_inner_tablet_access_service.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_schema_helper.h"
#include "share/schema/ob_table_param.h"
#include "storage/access/ob_dml_param.h"


namespace oceanbase
{
using namespace share;
using namespace palf;
using namespace lib;
using namespace transaction;
namespace storage
{

ObInnerTabletWriteCtx::ObInnerTabletWriteCtx()
    : tx_desc_(nullptr),
      ls_id_(),
      tablet_id_(),
      buf_(nullptr),
      buf_len_(0)
{
}

ObInnerTabletWriteCtx::~ObInnerTabletWriteCtx()
{
  reset();
}

void ObInnerTabletWriteCtx::reset()
{
  tx_desc_ = nullptr;
  ls_id_.reset();
  tablet_id_.reset();
  buf_ = nullptr;
  buf_len_ = 0;
}

bool ObInnerTabletWriteCtx::is_valid() const
{
  return OB_NOT_NULL(tx_desc_)
      && ls_id_.is_valid()
      && tablet_id_.is_valid()
      && tablet_id_.is_ls_inner_tablet()
      && OB_NOT_NULL(buf_)
      && buf_len_ > 0;
}

ObInnerTableReadCtx::ObInnerTableReadCtx()
  : ls_id_(),
    tablet_id_(),
    key_range_(nullptr),
    is_get_(false),
    snapshot_(),
    abs_timeout_us_(0),
    get_multi_version_row_(false),
    allocator_(),
    table_param_(allocator_),
    scan_param_()
{
}

ObInnerTableReadCtx::~ObInnerTableReadCtx()
{
}

void ObInnerTableReadCtx::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  key_range_ = nullptr;
  is_get_ = false;
  snapshot_.reset();
  abs_timeout_us_ = 0;
  get_multi_version_row_ = false;
}

bool ObInnerTableReadCtx::is_valid() const
{
  return ls_id_.is_valid()
      && tablet_id_.is_valid()
      && OB_NOT_NULL(key_range_)
      && key_range_->is_valid()
      && snapshot_.is_valid()
      && abs_timeout_us_ > 0;
}

ObInnerTabletIterator::ObInnerTabletIterator()
  : is_inited_(false),
    allocator_(nullptr),
    datum_row_array_(),
    data_buffer_(),
    total_count_(0),
    index_(-1),
    cur_datum_rows_(nullptr)
{
}

ObInnerTabletIterator::~ObInnerTabletIterator()
{
  destroy();
}

void ObInnerTabletIterator::destroy()
{
  if (OB_NOT_NULL(cur_datum_rows_)) {
    for (int64_t i = 0; i < total_count_; ++i) {
      cur_datum_rows_[i].~ObDatumRow();
    }
  }

  for (int64_t i = 0; i < datum_row_array_.count(); ++i) {
    ObDatumRow *datum_row = datum_row_array_.at(i);
    if (OB_NOT_NULL(datum_row)) {
      datum_row->~ObDatumRow();
    }
  }
  allocator_= nullptr;
  data_buffer_.reset();
  total_count_ = 0;
  index_ = 0;
  datum_row_array_.reset();
  cur_datum_rows_ = nullptr;
  is_inited_ = false;
}

int ObInnerTabletIterator::init(
    char *buf,
    const int64_t buf_length,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inner tablet iterator init twice", K(ret));
  } else if (OB_ISNULL(buf) || buf_length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init inner tablet iterator get invalid argument", K(ret), KP(buf), K(buf_length));
  } else if (!data_buffer_.set_data(buf, buf_length)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set buffer data", K(ret));
  } else {
    index_ = 0;
    allocator_ = &allocator;
    if (OB_FAIL(convert_buffer_to_datum_())) {
      LOG_WARN("failed to convert buffer to datum", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObInnerTabletIterator::convert_buffer_to_datum_()
{
  int ret = OB_SUCCESS;
  void *row_buffer = nullptr;
  void *rows_buffer = nullptr;
  ObDatumRow *row = nullptr;
  ObDatumRow datum_row;

  if (OB_ISNULL(data_buffer_.get_data()) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("convert buffer to datum get invalid argument", K(ret));
  } else if (OB_FAIL(datum_row.init(*allocator_, OB_USER_ROW_MAX_COLUMNS_COUNT))) {
    LOG_WARN("failed to init datum row", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      datum_row.reuse();
      if (0 == data_buffer_.get_remain()) {
        break;
      } else if (OB_FAIL(serialization::decode(data_buffer_.get_data(),
          data_buffer_.get_capacity(),
          data_buffer_.get_position(),
          datum_row))) {
        STORAGE_LOG(WARN, "failed to decode", K_(data_buffer), K(ret));
      } else if (OB_ISNULL(row_buffer = allocator_->alloc(sizeof(ObDatumRow)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc row buf", K(ret));
      } else if (FALSE_IT(row = new (static_cast<char *>(row_buffer))ObDatumRow())) {
      } else if (OB_FAIL(row->init(*allocator_, datum_row.get_column_count()))) {
        STORAGE_LOG(WARN, "failed to init datum row", K(ret), K(datum_row));
      } else if (OB_FAIL(row->deep_copy(datum_row, *allocator_))) {
        STORAGE_LOG(WARN, "failed to copy datum row", K(ret), K(datum_row));
      } else if (OB_FAIL(datum_row_array_.push_back(row))) {
        LOG_WARN("failed to push row into array", K(ret), KPC(row));
      } else {
        row = nullptr;
      }

      if (OB_NOT_NULL(row)) {
        row->~ObDatumRow();
        row = nullptr;
      }
    }

    if (OB_SUCC(ret)) {
      total_count_ = datum_row_array_.count();
      const size_t rows_buf_len = sizeof(blocksstable::ObDatumRow) * total_count_;
      if (OB_ISNULL(rows_buffer = allocator_->alloc(rows_buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate row buffer", K(ret), K(rows_buf_len));
      } else {
        cur_datum_rows_ = new(static_cast<char *>(rows_buffer)) blocksstable::ObDatumRow[total_count_]();
        int64_t i = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < total_count_; ++i) {
          if (OB_FAIL(cur_datum_rows_[i].shallow_copy(*datum_row_array_[i]))) {
            LOG_WARN("failed to shallow copy datum row", K(ret), KPC(datum_row_array_[i]));
          }
        }
      }
    }
  }
  return ret;
}

int ObInnerTabletIterator::get_next_row(
    blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet iterator do not init", K(ret));
  } else if (index_ >= total_count_) {
    ret = OB_ITER_END;
  } else {
    datum_row = &cur_datum_rows_[index_];
    ++index_;
  }
  return ret;
}

int ObInnerTabletIterator::get_next_rows(
    blocksstable::ObDatumRow *&rows, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  rows = nullptr;
  row_count = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet iterator do not init", K(ret));
  } else if (index_ >= total_count_) {
    ret = OB_ITER_END;
  } else {
    rows = &cur_datum_rows_[index_];
    row_count = total_count_ - index_;
    index_ = total_count_;
  }
  return ret;
}

void ObInnerTabletIterator::reuse()
{
  index_ = 0;
}

ObInnerTabletSQLStr::ObInnerTabletSQLStr()
  : ls_id_(),
    tablet_id_(),
    inner_tablet_str_()
{
}

ObInnerTabletSQLStr::~ObInnerTabletSQLStr()
{
}

void ObInnerTabletSQLStr::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  inner_tablet_str_.reset();
}

int ObInnerTabletSQLStr::set(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !tablet_id.is_valid() || OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set inner tablet sql str get invalid argument", K(ret), K(ls_id), K(tablet_id), KP(buf), K(buf_len));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    inner_tablet_str_.assign_ptr(buf, buf_len);
  }
  return ret;
}

bool ObInnerTabletSQLStr::is_valid() const
{
  return ls_id_.is_valid()
      && tablet_id_.is_valid()
      && !inner_tablet_str_.empty();
}

OB_SERIALIZE_MEMBER(ObInnerTabletSQLStr, ls_id_, tablet_id_, inner_tablet_str_);

ObInnerTabletAccessService::ObInnerTabletAccessService()
    : is_inited_(false),
      trans_service_(nullptr),
      access_service_(nullptr),
      rpc_proxy_(nullptr),
      location_adapter_def_(),
      location_adapter_(nullptr),
      self_()
{
}

ObInnerTabletAccessService::~ObInnerTabletAccessService()
{
}

int ObInnerTabletAccessService::mtl_init(ObInnerTabletAccessService *&access_service)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(access_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner tablet access service should not be NULL", K(ret), KP(access_service));
  } else if (OB_FAIL(access_service->init())) {
    LOG_WARN("failed to init rebuild service", K(ret));
  }
  return ret;
}

int ObInnerTabletAccessService::init()
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *trans_service = MTL(transaction::ObTransService *);
  obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  share::ObLocationService *location_service = GCTX.location_service_;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObAccessService *access_service = MTL(storage::ObAccessService *);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inner tablet access service init twice", K(ret));
  } else if (OB_ISNULL(trans_service) || OB_ISNULL(rpc_proxy) || OB_ISNULL(location_service) || OB_ISNULL(schema_service)
      || OB_ISNULL(access_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init inner tablet access service has unexpected values", K(ret), KP(trans_service),
        KP(rpc_proxy), KP(location_service), KP(schema_service));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy should not be NULL", K(ret), KP(rpc_proxy));
  } else if (OB_FAIL(location_adapter_def_.init(schema_service, location_service))) {
    LOG_WARN("failed to init location adapter", K(ret));
  } else {
    location_adapter_ = &location_adapter_def_;
    trans_service_ = trans_service;
    access_service_ = access_service;
    rpc_proxy_ = rpc_proxy;
    self_ = GCTX.self_addr();
    is_inited_ = true;
  }
  return ret;
}

void ObInnerTabletAccessService::destroy()
{
  COMMON_LOG(INFO, "ObInnerTabletAccessService starts to destroy");
  is_inited_ = false;
  COMMON_LOG(INFO, "ObInnerTabletAccessService destroyed");
}

int ObInnerTabletAccessService::insert_rows(
    const ObInnerTabletWriteCtx &ctx,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert rows get invalid argument", K(ret), K(ctx));
  } else if (OB_FAIL(inner_insert_rows_(ctx, affected_rows))) {
    LOG_WARN("failed to inner insert rows", K(ret), K(ctx));
  }
  return ret;
}

int ObInnerTabletAccessService::inner_insert_rows_(
    const ObInnerTabletWriteCtx &ctx,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t MAX_RETRY_CNT = 5;
  const int64_t RETRY_INTERVAL = 400 * 1000;
  transaction::ObTxDesc &tx_desc = *ctx.tx_desc_;
  ObTxSEQ savepoint;
  const transaction::ObTxSEQ seq_no = transaction::ObTxSEQ();
  int64_t retry_cnt = 0;
  int64_t remain_timeout_us = 0;
  const share::ObLSID &ls_id = ctx.ls_id_;
  ObAddr ls_leader_addr;
  ObTimeGuard time_guard("inner tablet insert rows", 1 * 1000 * 1000);
  SMART_VAR(ObTxExecResult, tx_result)
  {
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("inner table access service do not init", K(ret));
    } else if (!tx_desc.is_tx_active()) {
      ret = OB_TRANS_IS_EXITING;
      LOG_WARN("tx desc must in active for inner tablet insert rows", K(ret));
    } else if (OB_FAIL(create_implicit_savepoint_(tx_desc, savepoint))) {
      LOG_WARN("failed to create implicit savepoint", K(ret), K(tx_desc), K(ctx));
    } else {
      time_guard.click("inner tablet start insert rows");
      do {
        tx_result.reset();
        if (OB_NOT_MASTER == ret) {
          ob_usleep(RETRY_INTERVAL);
          retry_cnt += 1;
        }

        if (ObTimeUtil::current_time() >= tx_desc.get_expire_ts()) {
          ret = OB_TIMEOUT;
          LOG_WARN("inner tablet insert rows timeout", KR(ret), K(tx_desc), K(ls_id),
                    K(retry_cnt));
        } else if (OB_FAIL(location_adapter_->nonblock_get_leader(
                       tx_desc.get_cluster_id(), tx_desc.get_tenant_id(), ls_id, ls_leader_addr))) {
          TRANS_LOG(WARN, "get leader failed", KR(ret), K(ls_id));
        } else if (ls_leader_addr == self_) {
          time_guard.click("inner tablet insert rows begin");
          if (OB_FAIL(execute_local_insert_rows_(ctx, affected_rows))) {
            LOG_WARN("failed to execute local insert rows", K(ctx));
          }
          time_guard.click("inner tablet insert rows end");

          // collect participants regardless of register error
          if (OB_TMP_FAIL(collect_tx_exec_result_(tx_desc, tx_result))) {
            LOG_WARN("failed to collect tx exec result", K(ret), K(ctx));
          }
          if (OB_TMP_FAIL(tmp_ret)) {
            if (OB_SUCC(ret)) {
              ret = tmp_ret;
            }
            LOG_WARN("set exec result failed when insert rows", K(ret), K(tmp_ret),
                      K(tx_desc));
          }
        } else if (self_ != tx_desc.get_addr()) {
          ret = OB_NOT_MASTER;
          LOG_INFO("The follower receive a inner tablet insert rows request. we will return err_code to scheduler",
                    K(ret), K(tx_desc), K(ls_id));
        } else if (FALSE_IT(time_guard.click("inner tablet remote insert rows begin"))) {
        } else if (OB_FAIL(execute_remote_insert_rows_(ctx, ls_leader_addr, tx_result, affected_rows))) {
          LOG_WARN("failed to execute remote insert rows", K(ret), K(ctx));
        } else if (FALSE_IT(time_guard.click("inner tablet remote insert rows end"))) {
        }

        if (OB_NOT_MASTER == ret) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS
              != (tmp_ret = location_adapter_->nonblock_renew(tx_desc.get_cluster_id(), tx_desc.get_tenant_id(),
                                                              ls_id))) {
            LOG_WARN("failed to refresh location cache", KR(tmp_ret), K(tx_desc), K(ls_id));
          }
        }
      } while (OB_NOT_MASTER == ret && this->self_ == tx_desc.get_addr());


      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_tx_exec_result_(tx_desc, tx_result))) {
          LOG_WARN("failed to add tx exec result", K(ret), K(tx_desc), K(tx_result));
        }
      }
      if (OB_FAIL(ret) && OB_NOT_MASTER != ret) {
        if (OB_TMP_FAIL(rollback_to_implicit_savepoint_(savepoint, ls_id, tx_desc))) {
          LOG_WARN("failed to rollback to savepoint", K(tmp_ret), K(savepoint));
        }
      }
    }
    LOG_INFO("inner tablet insert rows result", KR(ret), K(ctx), K(tx_result), K(tx_desc),
        K(retry_cnt), K(time_guard));
  }
  return ret;
}

int ObInnerTabletAccessService::create_implicit_savepoint_(
    transaction::ObTxDesc &tx_desc,
    transaction::ObTxSEQ &savepoint)
{
  int ret = OB_SUCCESS;
  savepoint.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else {
    ObTxParam tx_param;
    tx_param.cluster_id_ = tx_desc.get_cluster_id();
    tx_param.access_mode_ = tx_desc.get_tx_access_mode();
    tx_param.isolation_ = tx_desc.get_isolation_level();
    tx_param.timeout_us_ = tx_desc.get_timeout_us();
    if (OB_FAIL(trans_service_->create_implicit_savepoint(tx_desc, tx_param, savepoint))) {
      LOG_WARN("failed to create implicit save point", K(ret), K(tx_param));
    }
  }
  return ret;
}

int ObInnerTabletAccessService::collect_tx_exec_result_(
    transaction::ObTxDesc &tx_desc,
    transaction::ObTxExecResult &result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(trans_service_->collect_tx_exec_result(tx_desc, result))) {
    LOG_WARN("failed to collect tx execute result", K(ret));
  }
  return ret;
}

int ObInnerTabletAccessService::add_tx_exec_result_(
    transaction::ObTxDesc &tx_desc,
    const transaction::ObTxExecResult &result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(trans_service_->add_tx_exec_result(tx_desc, result))) {
    LOG_WARN("failed to collect tx execute result", K(ret));
  }
  return ret;
}

int ObInnerTabletAccessService::rollback_to_implicit_savepoint_(
    const transaction::ObTxSEQ &savepoint,
    const share::ObLSID &ls_id,
    transaction::ObTxDesc &tx_desc)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else {
    share::ObLSArray ls_list;
    const int64_t expire_ts = THIS_WORKER.get_timeout_remain();
    if (OB_FAIL(ls_list.push_back(ls_id))) {
      LOG_WARN("failed to push ls id into array", K(ret), K(ls_id));
    } else if (OB_FAIL(trans_service_->rollback_to_implicit_savepoint(tx_desc, savepoint, expire_ts, &ls_list))) {
      LOG_WARN("failed to rollback to savepoint fail", K(ret), K(savepoint), K(expire_ts));
    }
  }
  return ret;
}

int ObInnerTabletAccessService::get_read_snapshot_(
    transaction::ObTxDesc &tx_desc,
    const share::ObLSID &ls_id,
    transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  const int64_t expire_ts = THIS_WORKER.get_timeout_remain();
  const transaction::ObTxIsolationLevel isolation_level = transaction::ObTxIsolationLevel::RC;
  snapshot.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(trans_service_->get_ls_read_snapshot(tx_desc, isolation_level, ls_id, expire_ts, snapshot))) {
    LOG_WARN("failed to get ls read snapshot", K(ret), K(tx_desc), K(ls_id));
  }
  return ret;
}

int ObInnerTabletAccessService::get_write_store_ctx_guard_(
    const share::ObLSID &ls_id,
    transaction::ObTxDesc &tx_desc,
    const transaction::ObTxReadSnapshot &snapshot,
    ObStoreCtxGuard &ctx_guard)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else {
    // branch id for parallel write, required for partially rollback
    // branch id = 0 means do not parallel write
    const int16_t branch_id = 0;
    const int64_t timeout = THIS_WORKER.get_timeout_remain();
    concurrent_control::ObWriteFlag write_flag;
    write_flag.set_is_dml_batch_opt();
    write_flag.set_is_insert_up();
    if (OB_FAIL(access_service_->get_write_store_ctx_guard(ls_id, timeout, tx_desc, snapshot, branch_id, write_flag, ctx_guard))) {
      LOG_WARN("failed to get write store ctx guard", K(ret), K(ls_id), K(tx_desc));
    }
  }
  return ret;
}

int ObInnerTabletAccessService::execute_local_insert_rows_(
    const ObInnerTabletWriteCtx &ctx,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  transaction::ObTxDesc &tx_desc = *ctx.tx_desc_;
  const ObLSID &ls_id = ctx.ls_id_;
  int64_t local_retry_cnt = 0;
  const int64_t MAX_RETRY_CNT = 5;
  ObInnerTabletIterator iterator;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(iterator.init(const_cast<char *>(ctx.buf_), ctx.buf_len_, allocator))) {
    LOG_WARN("failed to init inner tablet iterator", K(ret), K(ctx));
  } else {
    do {
      if (OB_FAIL(do_insert_rows_(ctx, &iterator, affected_rows))) {
        LOG_WARN("failed to do inner tablet insert rows", K(ret));
        if (OB_EAGAIN == ret) {
          if (ObTimeUtil::current_time() >= tx_desc.get_expire_ts()) {
            ret = OB_TIMEOUT;
            LOG_WARN("inner tablet insert rows timeout in this participant", KR(ret), K(tx_desc),
                      K(ls_id), K(local_retry_cnt));
          } else if (local_retry_cnt > MAX_RETRY_CNT) {
            ret = OB_NOT_MASTER;
            LOG_WARN("local retry too many times, need retry by the scheduler", K(ret),
                      K(local_retry_cnt));
          } else {
            local_retry_cnt++;
            iterator.reuse();
          }
        }
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObInnerTabletAccessService::execute_remote_insert_rows_(
    const ObInnerTabletWriteCtx &ctx,
    const ObAddr &ls_leader_addr,
    transaction::ObTxExecResult &result,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObWriteInnerTabletArg arg;
  ObWriteInnerTabletResult rpc_result;
  transaction::ObTxDesc &tx_desc = *ctx.tx_desc_;
  const ObLSID &ls_id = ctx.ls_id_;
  const ObTabletID &tablet_id = ctx.tablet_id_;
  int64_t remain_timeout_us = 0;
  affected_rows = -1;
  ObTimeGuard time_guard("inner tablet insert rows rpc", 1 * 1000 * 1000);
  const ObString buf(ctx.buf_len_, ctx.buf_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(arg.init(tx_desc.get_tenant_id(),
      tx_desc,
      ls_id,
      tablet_id,
      buf))) {
    LOG_WARN("failed to init rpc arg", KR(ret), K(tx_desc), K(ls_id), K(tablet_id));
  } else if (OB_FALSE_IT(time_guard.click("register by rpc begin"))) {
  } else if (OB_FALSE_IT(remain_timeout_us = tx_desc.get_expire_ts() - ObTimeUtil::fast_current_time())) {
  } else if (OB_FAIL(rpc_proxy_->to(ls_leader_addr)
                          .by(tx_desc.get_tenant_id())
                          .timeout(remain_timeout_us)
                          .write_inner_tablet(arg, rpc_result))) {
    LOG_WARN("failed to execute write inner tablet", KR(ret), K(ls_leader_addr), K(arg), K(tx_desc),
              K(ctx), K(result));
    time_guard.click("register by rpc end");
  } else if (OB_FALSE_IT(time_guard.click("register by rpc end"))) {
  } else if (OB_FAIL(rpc_result.result_)) {
    LOG_WARN("write inner tablet failed in remote", KR(ret), K(tx_desc), K(ctx));
  } else if (OB_FAIL(result.merge_result(rpc_result.tx_result_))) {
    LOG_WARN("merge tx result failed", KR(ret), K(rpc_result));
  } else {
    affected_rows = rpc_result.affected_rows_;
  }
  return ret;
}

int ObInnerTabletAccessService::init_dml_param_(
    const ObInnerTabletWriteCtx &ctx,
    const transaction::ObTxReadSnapshot &snapshot,
    common::ObIAllocator &allocator,
    share::schema::ObTableDMLParam &table_dml_param,
    ObStoreCtxGuard &store_ctx_guard,
    storage::ObDMLBaseParam &dml_param)
{
  int ret = OB_SUCCESS;
  transaction::ObTxDesc &tx_desc = *ctx.tx_desc_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else {
    dml_param.timeout_ = tx_desc.get_expire_ts();
    dml_param.schema_version_ = 0;
    dml_param.is_total_quantity_log_ = false;
    dml_param.tz_info_ = nullptr;
    dml_param.sql_mode_ = SMO_DEFAULT;

    dml_param.table_param_ = &table_dml_param;
    dml_param.tenant_schema_version_ = 0;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_; //do not encrypt meta
    dml_param.prelock_ = false;
    dml_param.is_batch_stmt_ = false;
    dml_param.dml_allocator_ = &allocator;
    if (OB_FAIL(dml_param.snapshot_.assign(snapshot))) {
      LOG_WARN("assign snapshot fail", K(ret));
    }
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    //dml_param.write_flag_ using default;
    dml_param.check_schema_version_ = false;
  }
  return ret;
}

int ObInnerTabletAccessService::get_table_schema_(
    const common::ObTabletID &tablet_id,
    const share::schema::ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (LS_REORG_INFO_TABLET == tablet_id) {
    if (OB_ISNULL(table_schema = ObTabletReorgInfoTableSchemaHelper::get_instance().get_table_schema())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner tablet table schema is null, unexpected", K(ret), K(tablet_id));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not supported this type inner tablet", K(ret), K(tablet_id));
  }
  return ret;
}

int ObInnerTabletAccessService::get_column_ids_(
    const common::ObTabletID &tablet_id,
    const bool is_query,
    common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (LS_REORG_INFO_TABLET == tablet_id) {
    if (OB_FAIL(ObTabletReorgInfoTableSchemaHelper::get_instance().get_column_ids(is_query, column_ids))) {
      LOG_WARN("failed to get column ids", K(ret), K(tablet_id), K(is_query));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not supported this type inner tablet", K(ret), K(tablet_id));
  }
  return ret;
}


int ObInnerTabletAccessService::init_table_dml_param_(
    const ObInnerTabletWriteCtx &ctx,
    share::schema::ObTableDMLParam &table_dml_param,
    common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ctx.ls_id_;
  const ObTabletID &tablet_id = ctx.tablet_id_;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const int64_t tenant_schema_version = 0;
  const bool is_query = false;
  column_ids.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(get_table_schema_(tablet_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tablet_id), K(ctx));
  } else if (OB_FAIL(get_column_ids_(tablet_id, is_query, column_ids))) {
    LOG_WARN("failed to get column ids", K(ret), K(tablet_id));
  } else if (OB_FAIL(table_dml_param.convert(table_schema, tenant_schema_version, column_ids))) {
    LOG_WARN("failed to convert to table dml param", K(ret), KPC(table_schema));
  }
  return ret;
}

int ObInnerTabletAccessService::do_insert_rows_(
    const ObInnerTabletWriteCtx &ctx,
    blocksstable::ObDatumRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  common::ObArray<uint64_t> column_ids;
  transaction::ObTxDesc &tx_desc = *ctx.tx_desc_;
  const ObLSID &ls_id = ctx.ls_id_;
  const ObTabletID &tablet_id = ctx.tablet_id_;
  transaction::ObTxReadSnapshot snapshot;
  affected_rows = -1;

  SMART_VARS_3((storage::ObStoreCtxGuard, store_ctx_guard), (share::schema::ObTableDMLParam, table_dml_param, allocator),
      (storage::ObDMLBaseParam, dml_param)) {
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("inner tablet access service do not init", K(ret));
    } else if (OB_ISNULL(row_iter)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("do insert rows get invalid argument", K(ret), K(ctx));
    } else if (OB_FAIL(get_read_snapshot_(tx_desc, ls_id, snapshot))) {
      LOG_WARN("failed to get read snapshot", K(ret), K(ls_id));
    } else if (OB_FAIL(get_write_store_ctx_guard_(ls_id, tx_desc, snapshot, store_ctx_guard))) {
      LOG_WARN("failed to get write store ctx guard", K(ret));
    } else if (OB_FAIL(init_table_dml_param_(ctx, table_dml_param, column_ids))) {
      LOG_WARN("failed to init table param", K(ret), K(ctx));
    } else if (OB_FAIL(init_dml_param_(ctx, snapshot, allocator, table_dml_param, store_ctx_guard, dml_param))) {
      LOG_WARN("failed to init dml param", K(ret), K(ctx));
    } else if (OB_FAIL(access_service_->insert_rows(ls_id, tablet_id, tx_desc, dml_param, column_ids, row_iter, affected_rows))) {
      LOG_WARN("failed to inert rows", K(ret), K(ctx));
    }
  }
  return ret;
}

int ObInnerTabletAccessService::read_rows(
    ObInnerTableReadCtx &ctx,
    common::ObNewRowIterator *&scan_iter)
{
  int ret = OB_SUCCESS;
  scan_iter = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner tablet access service get invalid argument", K(ret), K(ctx));
  } else if (OB_FAIL(do_read_rows_(ctx, scan_iter))) {
    LOG_WARN("failed to do read rows", K(ret), K(ctx));
  }
  return ret;
}

int ObInnerTabletAccessService::do_read_rows_(
    ObInnerTableReadCtx &ctx,
    common::ObNewRowIterator *&scan_iter)
{
  int ret = OB_SUCCESS;
  scan_iter = nullptr;
  const share::ObLSID &ls_id = ctx.ls_id_;
  const common::ObTabletID &tablet_id = ctx.tablet_id_;
  const share::schema::ObTableSchema *table_schema = nullptr;
  share::schema::ObTableParam &table_param = ctx.table_param_;
  ObTableScanParam &scan_param = ctx.scan_param_;
  ObArray<uint64_t> column_ids;
  const bool is_query = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(get_table_schema_(ctx.tablet_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(ctx));
  } else if (OB_FAIL(get_column_ids_(ctx.tablet_id_, is_query, column_ids))) {
    LOG_WARN("failed to get column ids", K(ret), K(ctx));
  } else if (OB_FAIL(init_table_param_(*table_schema, column_ids, table_param))) {
    LOG_WARN("failed to init table param", K(ret), KPC(table_schema), K(ctx));
  } else if (OB_FAIL(build_scan_param_(ctx, *table_schema, column_ids, table_param, scan_param))) {
    LOG_WARN("failed to build scan param", K(ret), K(ctx), KPC(table_schema));
  } else if (OB_FAIL(access_service_->inner_tablet_scan(ls_id, tablet_id, scan_param, scan_iter))) {
    LOG_WARN("failed to do inner tablet scan", K(ret), K(ctx));
  }
  return ret;
}

int ObInnerTabletAccessService::build_scan_param_(
    ObInnerTableReadCtx &ctx,
    const share::schema::ObTableSchema &table_schema,
    const common::ObIArray<uint64_t> &column_ids,
    share::schema::ObTableParam &table_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ctx.ls_id_;
  const ObTabletID &tablet_id = ctx.tablet_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(*ctx.key_range_))) {
    LOG_WARN("fail to push back key range", K(ret));
  } else {
    scan_param.tenant_id_ = MTL_ID();
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    scan_param.is_get_ = ctx.is_get_;
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = ctx.abs_timeout_us_;

    ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                          false, // daily_merge
                          false, // optimize
                          false, // sys scan
                          true, // full_row
                          false, // index_back
                          false, // query_stat
                          ObQueryFlag::MysqlMode, // sql_mode
                          false // read_latest
                        );
    scan_param.scan_flag_ = query_flag;
    scan_param.allocator_ = &ctx.allocator_;
    scan_param.scan_allocator_ = &ctx.allocator_;
    scan_param.frozen_version_ = -1;
    scan_param.need_scn_ = false;
    scan_param.for_update_ = false;
    scan_param.is_mds_query_ = false;
    scan_param.fb_snapshot_ = ctx.snapshot_;
    scan_param.pd_storage_flag_ = 0;
    scan_param.index_id_ = table_schema.get_table_id();
    scan_param.schema_version_ = table_schema.get_schema_version();
    transaction::ObTxSnapshot tx_snapshot;
    tx_snapshot.version_ = ctx.snapshot_;
    scan_param.snapshot_.init_ls_read(ls_id, tx_snapshot);
    scan_param.table_param_ = &table_param;
    scan_param.main_table_scan_stat_.tsc_monitor_info_ = nullptr;
    scan_param.need_scn_ = true; //get trans version
  }

  if (OB_FAIL(ret)) {
  } else {
    if (OB_FAIL(scan_param.column_ids_.assign(column_ids))) {
      LOG_WARN("failed to assign column ids", K(ret), K(column_ids));
    } else {
      scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
    }
  }
  return ret;
}

int ObInnerTabletAccessService::init_table_param_(
    const share::schema::ObTableSchema &table_schema,
    const common::ObIArray<uint64_t> &column_ids,
    share::schema::ObTableParam &table_param)
{
  int ret = OB_SUCCESS;
  const sql::ObStoragePushdownFlag pd_pushdown_flag(0); //do not push down
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner tablet access service do not init", K(ret));
  } else if (OB_FAIL(table_param.convert(table_schema, column_ids, pd_pushdown_flag))) {
    LOG_WARN("failed to convert table schema to table param", K(ret), K(table_schema));
  }
  return ret;
}

}
}
