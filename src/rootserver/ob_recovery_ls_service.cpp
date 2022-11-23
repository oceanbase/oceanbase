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

#define USING_LOG_PREFIX RS
#include "ob_recovery_ls_service.h"

#include "lib/thread/threads.h"               //set_run_wrapper
#include "lib/mysqlclient/ob_mysql_transaction.h"  //ObMySQLTransaction
#include "lib/profile/ob_trace_id.h"
#include "logservice/ob_log_base_header.h"          //ObLogBaseHeader
#include "logservice/ob_log_handler.h"              //ObLogHandler
#include "logservice/palf/log_entry.h"              //LogEntry
#include "logservice/palf/log_define.h"//SCN
#include "logservice/ob_garbage_collector.h"//ObGCLSLog
#include "logservice/restoreservice/ob_log_restore_handler.h"//ObLogRestoreHandler
#include "observer/ob_server_struct.h"              //GCTX
#include "rootserver/ob_primary_ls_service.h"       //ObTenantLSInfo
#include "rootserver/ob_tenant_recovery_reportor.h" //ObTenantRecoveryReportor
#include "share/ls/ob_ls_life_manager.h"            //ObLSLifeManger
#include "share/ls/ob_ls_operator.h"                //ObLSAttr
#include "share/ls/ob_ls_recovery_stat_operator.h"  //ObLSRecoveryLSStatOperator
#include "share/ob_errno.h"
#include "share/ob_share_util.h"                           //ObShareUtil
#include "share/schema/ob_multi_version_schema_service.h"  //ObMultiSchemaService
#include "share/restore/ob_physical_restore_info.h" //restore_status
#include "share/restore/ob_physical_restore_table_operator.h"//ObPhysicalRestoreTableOperator
#include "storage/tx/ob_tx_log.h"                          //ObTxLogHeader
#include "storage/tx_storage/ob_ls_service.h"              //ObLSService
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle

namespace oceanbase
{
using namespace logservice;
using namespace transaction;
using namespace share;
using namespace storage;
using namespace palf;
namespace rootserver
{

int ObRecoveryLSService::mtl_init(ObRecoveryLSService*&ka)
{
  return ka->init();
}
int ObRecoveryLSService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("RecLSSer",
         lib::TGDefIDs::LSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    proxy_ = GCTX.sql_proxy_;
    inited_ = true;
  }

  return ret;
}

void ObRecoveryLSService::destroy()
{
  LOG_INFO("recovery ls service destory", KPC(this));
  ObTenantThreadHelper::destroy();
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  proxy_ = NULL;
}

void ObRecoveryLSService::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_), KP(proxy_));
  } else {
    ObLSRecoveryStatOperator ls_recovery;
    palf::PalfBufferIterator iterator;
    int tmp_ret = OB_SUCCESS;
    const int64_t idle_time_us = 100 * 1000L;
    int64_t start_scn = OB_LS_INVALID_SCN_VALUE;
    while (!has_set_stop()) {
      ObCurTraceId::init(GCONF.self_addr_);
      uint64_t thread_idx = get_thread_idx();
      //two thread for seed log and recovery_ls_manager 
      if (!is_user_tenant(tenant_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls recovery thread must run on user tenant", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(check_can_do_recovery_())) {
        LOG_WARN("failed to check do recovery", KR(ret));
      } else if (0 == thread_idx) { 
        if (OB_SUCCESS != (tmp_ret = process_recovery_ls_manager())) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to process recovery ls manager", KR(ret), KR(tmp_ret));
        }
      } else {
        if (OB_LS_INVALID_SCN_VALUE == start_scn) {
          ObLSRecoveryStat ls_recovery_stat;
          if (OB_FAIL(ls_recovery.get_ls_recovery_stat(tenant_id_,
                  SYS_LS, false, ls_recovery_stat, *proxy_))) {
            LOG_WARN("failed to load sys recovery stat", KR(ret), K(tenant_id_));
          } else if (OB_LS_MIN_SCN_VALUE == ls_recovery_stat.get_sync_scn()) {
            //等待物理恢复设置完系统日志流初始同步点之后开始迭代
            ret = OB_EAGAIN;
            LOG_WARN("restore init sync scn has not setted, need wait", KR(ret), K(ls_recovery_stat));
          } else if (OB_FAIL(report_sys_ls_recovery_stat_(ls_recovery_stat.get_sync_scn()))) {
            //may recovery end, but readable scn need report
            LOG_WARN("failed to report ls recovery stat", KR(ret), K(ls_recovery_stat));
          } else if (OB_FAIL(seek_log_iterator_(ls_recovery_stat.get_sync_scn(), iterator))) {
            LOG_WARN("failed to seek log iterator", KR(ret), K(ls_recovery_stat));
          } else {
            start_scn = ls_recovery_stat.get_sync_scn();
            LOG_INFO("start to seek at", K(start_scn));
          }
        }
        if (FAILEDx(process_ls_log_(start_scn, iterator))) {
          LOG_WARN("failed to process ls log", KR(ret), K(start_scn));
        }
         
        if (OB_FAIL(ret)) {
          start_scn = OB_LS_INVALID_SCN_VALUE;
          LOG_WARN("failed to do wowrk", KR(ret), K(start_scn));
        }
      }
      LOG_INFO("[LS_RECOVERY] finish one round", KR(ret), KR(tmp_ret), K(start_scn), K(thread_idx));
      idle(idle_time_us);
    }
  }
}

int ObRecoveryLSService::seek_log_iterator_(const int64_t sync_scn, PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_LS_INVALID_SCN_VALUE == sync_scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sync scn is invalid", KR(ret), K(sync_scn));
  } else {
    ObLSService *ls_svr = MTL(ObLSService *);
    ObLSHandle ls_handle;
    if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, storage::ObLSGetMod::RS_MOD))) {
      LOG_WARN("failed to get ls", KR(ret));
    } else {
      ObLogHandler *log_handler = NULL;
      ObLS *ls = NULL;
      palf::LSN start_lsn;
      if (OB_ISNULL(ls = ls_handle.get_ls())
          || OB_ISNULL(log_handler = ls->get_log_handler())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ls or log handle is null", KR(ret), KP(ls),
                     KP(log_handler));
      } else if (OB_FAIL(log_handler->locate_by_ts_ns_coarsely(sync_scn, start_lsn))) {
        LOG_WARN("failed to locate lsn", KR(ret), K(sync_scn));
      } else if (OB_FAIL(log_handler->seek(start_lsn, iterator))) {
        LOG_WARN("failed to seek iterator", KR(ret), K(sync_scn));
      }
    }
  }
  return ret;
}

int ObRecoveryLSService::process_ls_log_(const int64_t start_scn, PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  palf::LogEntry log_entry;
  palf::LSN target_lsn;
  int64_t sync_scn = OB_INVALID_TIMESTAMP;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  }
  while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
    if (OB_FAIL(iterator.get_entry(log_entry, target_lsn))) {
      LOG_WARN("failed to get log", KR(ret), K(log_entry));
    } else if (OB_ISNULL(log_entry.get_data_buf())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log entry is null", KR(ret));
    } else {
      LOG_DEBUG("get log", K(log_entry), K(target_lsn), K(start_scn));
      sync_scn = log_entry.get_log_ts();
      const char *log_buf = log_entry.get_data_buf();
      const int64_t log_length = log_entry.get_data_len();
      logservice::ObLogBaseHeader header;
      const int64_t HEADER_SIZE = header.get_serialize_size();
      int64_t log_pos = 0;
      if (OB_UNLIKELY(sync_scn <= start_scn)) {
        //通过scn定位的LSN是不准确的，可能会获取多余的数据，所以需要把小于等于sync_scn的过滤掉
        continue;
      } else if (OB_FAIL(header.deserialize(log_buf, HEADER_SIZE, log_pos))) {
        LOG_WARN("failed to deserialize", KR(ret), K(HEADER_SIZE));
      } else if (OB_UNLIKELY(log_pos >= log_length)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log pos is not expected", KR(ret), K(log_pos), K(log_length));
      } else if (logservice::GC_LS_LOG_BASE_TYPE == header.get_log_type()) {
        ObGCLSLog gc_log;
        if (OB_FAIL(gc_log.deserialize(log_buf, log_length, log_pos))) {
          LOG_WARN("failed to deserialize gc log", KR(ret), K(log_length));
        } else if (ObGCLSLOGType::OFFLINE_LS == gc_log.get_log_type()) {
          //set sys ls offline
          if (OB_FAIL(process_gc_log_(gc_log, sync_scn))) {
            LOG_WARN("failed to process gc log", KR(ret), K(sync_scn));
          }
        }
        // nothing
      } else if (logservice::TRANS_SERVICE_LOG_BASE_TYPE == header.get_log_type()) {
        ObTxLogBlock tx_log_block;
        ObTxLogBlockHeader tx_block_header;
        if (OB_FAIL(tx_log_block.init(log_buf, log_length, log_pos, tx_block_header))) {
          LOG_WARN("failed to init tx log block", KR(ret), K(log_length));
        } else if (OB_FAIL(process_ls_tx_log_(tx_log_block, sync_scn))) {
          LOG_WARN("failed to process ls tx log", KR(ret), K(tx_log_block), K(sync_scn));
        }
      } else {}
    }
  }//end for each log
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (OB_FAIL(report_sys_ls_recovery_stat_(sync_scn))) {
      LOG_WARN("failed to report ls recovery stat", KR(ret), K(sync_scn));
    }
    if (FAILEDx(update_sys_ls_restore_finish_())) {
      LOG_WARN("failed to update sys ls recovery finish", KR(ret), K(sync_scn));
    }
  } else if (OB_SUCC(ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator must be end", KR(ret));
  } else {
    LOG_WARN("failed to get next log", KR(ret));
  }

  return ret;
}

int ObRecoveryLSService::process_ls_tx_log_(ObTxLogBlock &tx_log_block,
                                     const int64_t sync_scn)
{
  int ret = OB_SUCCESS;
  bool has_operation = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  }
  while (OB_SUCC(ret)) {
    transaction::ObTxLogHeader tx_header;
    if (OB_FAIL(tx_log_block.get_next_log(tx_header))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next log", KR(ret));
      }
    } else if (transaction::ObTxLogType::TX_COMMIT_LOG !=
               tx_header.get_tx_log_type()) {
      // nothing
    } else {
      ObTxCommitLogTempRef temp_ref;
      ObTxCommitLog commit_log(temp_ref);
      const int64_t COMMIT_SIZE = commit_log.get_serialize_size();
      //TODO commit log may too large
      if (OB_FAIL(tx_log_block.deserialize_log_body(commit_log))) {
        LOG_WARN("failed to deserialize", KR(ret));
      } else {
        const ObTxBufferNodeArray &source_data =
            commit_log.get_multi_source_data();
        for (int64_t i = 0; OB_SUCC(ret) && i < source_data.count(); ++i) {
          const ObTxBufferNode &node = source_data.at(i);
          if (ObTxDataSourceType::LS_TABLE != node.get_data_source_type()) {
            // nothing
          } else if (has_operation) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("one clog has more than one operation", KR(ret), K(commit_log));
          } else {
            has_operation = true;
            ObLSAttr ls_attr;
            const int64_t LS_SIZE = ls_attr.get_serialize_size();
            int64_t pos = 0;
            if (OB_FAIL(ls_attr.deserialize(node.get_data_buf().ptr(), LS_SIZE,
                                            pos))) {
              LOG_WARN("failed to deserialize", KR(ret), K(node), K(LS_SIZE));
            } else if (OB_UNLIKELY(pos > LS_SIZE)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get ls attr", KR(ret), K(pos), K(LS_SIZE));
            } else {
              LOG_INFO("get ls operation", K(ls_attr), K(sync_scn));
              //TODO ls recovery is too fast for ls manager, so it maybe failed, while change ls status
              //consider how to retry
              if (OB_FAIL(process_ls_operator_(ls_attr,
                                               sync_scn))) {
                LOG_WARN("failed to process ls operator", KR(ret), K(ls_attr),
                         K(sync_scn));
              }
            }
          }
        }// end for
      }
    }
  }  // end while for each tx_log

  return ret;
}

int ObRecoveryLSService::process_gc_log_(logservice::ObGCLSLog &gc_log,
                        const int64_t sync_scn)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  ObLSLifeAgentManager ls_life_agent(*proxy_);
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret)); 
   } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(ls_life_agent.set_ls_offline_in_trans(
            tenant_id_, SYS_LS, share::OB_LS_TENANT_DROPPING, sync_scn,
            trans))) {
    LOG_WARN("failed to set offline", KR(ret), K(tenant_id_), K(sync_scn));
  } else {
    ObLSRecoveryStatOperator ls_recovery;
    ObLSRecoveryStat ls_recovery_stat;
    if (OB_FAIL(construct_ls_recovery_stat(sync_scn, ls_recovery_stat))) {
      LOG_WARN("failed to construct ls recovery stat", KR(ret), K(sync_scn));
    } else if (OB_FAIL(ls_recovery.update_ls_recovery_stat_in_trans(
            ls_recovery_stat, trans))) {
      LOG_WARN("failed to update ls recovery stat", KR(ret), K(ls_recovery_stat));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObRecoveryLSService::construct_ls_recovery_stat(const int64_t sync_scn,
                                                    ObLSRecoveryStat &ls_stat)
{
  int ret = OB_SUCCESS;
  ls_stat.reset();
  int64_t readable_scn = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_FAIL(ObTenantRecoveryReportor::get_readable_scn(SYS_LS, readable_scn))) {
    LOG_WARN("failed to get readable scn", KR(ret));
  } else if (OB_FAIL(ls_stat.init_only_recovery_stat(
          tenant_id_, SYS_LS, sync_scn, readable_scn))) {
    LOG_WARN("failed to init ls recovery stat", KR(ret), K(tenant_id_),
             K(sync_scn), K(readable_scn));
  }
  return ret;
}

//TODO check other ls is valid to process ls operator
int ObRecoveryLSService::process_ls_operator_(const share::ObLSAttr &ls_attr,
    const int64_t sync_scn)
{
  int ret = OB_SUCCESS;
  ObLSRecoveryStatOperator ls_recovery;
  common::ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  ObLSStatusOperator ls_operator;
  share::ObLSStatusInfo ls_status;
  ObLSLifeAgentManager ls_life_agent(*proxy_);
  if (OB_UNLIKELY(!ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls attr is invalid", KR(ret), K(ls_attr));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (OB_FAIL(check_valid_to_operator_ls_(sync_scn))) {
    LOG_WARN("failed to check valid to operator ls", KR(ret), K(sync_scn));
  } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(meta_tenant_id));
  } else if (share::is_ls_create_pre_op(ls_attr.get_ls_operatin_type())) {
    //create new ls;
    if (OB_FAIL(create_new_ls_(ls_attr, sync_scn, trans))) {
      LOG_WARN("failed to create new ls", KR(ret), K(sync_scn), K(ls_attr));
    }
  } else if (share::is_ls_create_abort_op(ls_attr.get_ls_operatin_type())) {
    if (OB_FAIL(ls_life_agent.drop_ls_in_trans(tenant_id_, ls_attr.get_ls_id(), trans))) {
      LOG_WARN("failed to drop ls", KR(ret), K(tenant_id_), K(ls_attr));
    }
  } else if (share::is_ls_drop_end_op(ls_attr.get_ls_operatin_type())) {
    if (OB_FAIL(ls_life_agent.set_ls_offline_in_trans(tenant_id_, ls_attr.get_ls_id(),
            ls_attr.get_ls_status(), sync_scn, trans))) {
      LOG_WARN("failed to set offline", KR(ret), K(tenant_id_), K(ls_attr), K(sync_scn));
    }
  } else {
    ObLSStatus target_status = share::OB_LS_EMPTY;
    if (OB_FAIL(ls_operator.get_ls_status_info(tenant_id_, ls_attr.get_ls_id(),
                                               ls_status, trans))) {
      LOG_WARN("failed to get ls status", KR(ret), K(tenant_id_), K(ls_attr));
    } else if (ls_status.ls_is_creating()) {
      ret = OB_EAGAIN;
      LOG_WARN("ls not created, need wait", KR(ret), K(ls_status));
    } else if (share::is_ls_create_end_op(ls_attr.get_ls_operatin_type())) {
      // set ls to normal
      target_status = share::OB_LS_NORMAL;
    } else if (share::is_ls_tenant_drop_op(ls_attr.get_ls_operatin_type())) {
      target_status = share::OB_LS_TENANT_DROPPING;
    } else if (share::is_ls_drop_pre_op(ls_attr.get_ls_operatin_type())) {
      target_status = share::OB_LS_DROPPING;
    } else if (share::is_ls_tenant_drop_pre_op(ls_attr.get_ls_operatin_type())) {
       target_status = share::OB_LS_PRE_TENANT_DROPPING;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operation type", KR(ret), K(ls_attr));
    }
    if (FAILEDx(ls_operator.update_ls_status(tenant_id_, ls_attr.get_ls_id(),
                                             ls_status.status_, target_status,
                                             share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to update ls status", KR(ret), K(tenant_id_), K(ls_attr),
               K(ls_status), K(target_status));
    }
    LOG_INFO("[LS_RECOVERY] update ls status", KR(ret), K(ls_attr), K(target_status));
  }
  if (OB_SUCC(ret)) {
    ObLSRecoveryStat ls_recovery_stat;
    if (OB_FAIL(construct_ls_recovery_stat(sync_scn, ls_recovery_stat))) {
      LOG_WARN("failed to construct ls recovery stat", KR(ret), K(sync_scn));
    } else if (OB_FAIL(ls_recovery.update_ls_recovery_stat_in_trans(
            ls_recovery_stat, trans))) {
      LOG_WARN("failed to update ls recovery stat", KR(ret), K(ls_recovery_stat));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObRecoveryLSService::check_valid_to_operator_ls_(const int64_t sync_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (OB_UNLIKELY(OB_LS_INVALID_SCN_VALUE == sync_scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("syns scn is invalid", KR(ret), K(sync_scn));
  } else {
    int64_t user_scn = OB_LS_INVALID_SCN_VALUE;
    ObLSRecoveryStatOperator ls_recovery;
    if (OB_FAIL(ls_recovery.get_user_ls_sync_scn(tenant_id_, *proxy_, user_scn))) {
      LOG_WARN("failed to get user scn", KR(ret), K(tenant_id_));
    } else if (user_scn >= sync_scn) {
      //其他日志流越过系统日志流的同步点，可以处理日志流操作了
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("can not process ls operator, need wait other ls sync", KR(ret),
          K(user_scn), K(sync_scn));
    }
  } 

  return ret;
}

int ObRecoveryLSService::create_new_ls_(const share::ObLSAttr &ls_attr,
                                        const int64_t sync_scn,
                                 common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!share::is_ls_create_pre_op(ls_attr.get_ls_operatin_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls not create pre operation", KR(ret), K(ls_attr));
  } else {
    //create new ls;
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTenantSchema *tenant_schema = NULL;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
      LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id_));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(tenant_id_));
    } else {
      ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, tenant_schema, tenant_id_,
                                 GCTX.srv_rpc_proxy_, GCTX.lst_operator_);
      if (OB_FAIL(tenant_stat.gather_stat(true))) {
        LOG_WARN("failed to gather stat", KR(ret));
      } else if (OB_FAIL(tenant_stat.create_new_ls_for_recovery(ls_attr.get_ls_id(), 
              ls_attr.get_ls_group_id(), ls_attr.get_create_scn(), trans))) {
        LOG_WARN("failed to add new ls status info", KR(ret), K(ls_attr), K(sync_scn));
      }
    }
    LOG_INFO("[LS_RECOVERY] create new ls", KR(ret), K(ls_attr));
  }
  return ret;
}


int ObRecoveryLSService::process_recovery_ls_manager()
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id_));
  } else {
    ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, tenant_schema, tenant_id_,
                               GCTX.srv_rpc_proxy_, GCTX.lst_operator_);
    if (OB_FAIL(tenant_stat.process_ls_stats_for_recovery())) {
      LOG_WARN("failed to do process in standby", KR(ret), K(tenant_id_));
    }
  }
  return ret;
}

int ObRecoveryLSService::check_can_do_recovery_()
{
  int ret = OB_SUCCESS;
  ObTenantRecoveryReportor *tenant_report = MTL(ObTenantRecoveryReportor*);
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_));
  } else if (OB_ISNULL(tenant_report)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant report is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tenant_report->get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  } else if (tenant_info.is_primary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary tenant can not do recovery", KR(ret), K(tenant_info));
  } else if (tenant_info.is_standby()) {
    //standby can do recovery
  } else if (tenant_info.is_restore()) {
    //need to check success to create init ls
    share::ObPhysicalRestoreTableOperator restore_table_operator;
    share::ObPhysicalRestoreJob job_info;
    if (OB_ISNULL(proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql can't null", K(ret), K(proxy_));
    } else if (OB_FAIL(restore_table_operator.init(proxy_, tenant_id_))) {
      LOG_WARN("fail to init restore table operator", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id_,
                                                                job_info))) {
      LOG_WARN("fail to get restore job", K(ret), K(tenant_id_));
    } else if (job_info.is_valid_status_to_recovery()) {
      //can do recovery
    } else {
      ret = OB_NEED_WAIT;
      LOG_WARN("restore tenant not valid to recovery", KR(ret), K(job_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant role", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObRecoveryLSService::report_sys_ls_recovery_stat_(const int64_t sync_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(proxy_));
  } else if (OB_LS_MIN_SCN_VALUE != sync_scn) {
    ObLSRecoveryStatOperator ls_recovery;
    ObLSRecoveryStat ls_recovery_stat;
    if (OB_FAIL(construct_ls_recovery_stat(sync_scn, ls_recovery_stat))) {
      LOG_WARN("failed to construct ls recovery stat", KR(ret), K(sync_scn));
    } else if (OB_FAIL(ls_recovery.update_ls_recovery_stat(ls_recovery_stat,
            *proxy_))) {
      LOG_WARN("failed to update ls recovery stat", KR(ret),
          K(ls_recovery_stat));
    }
  }
  return ret;
}

int ObRecoveryLSService::update_sys_ls_restore_finish_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(proxy_));
  } else {
    ObLSService *ls_svr = MTL(ObLSService *);
    ObLSHandle ls_handle;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, storage::ObLSGetMod::RS_MOD))) {
      LOG_WARN("failed to get ls", KR(ret));
    } else {
      logservice::ObLogRestoreHandler *restore_handler = NULL;
      ObLS *ls = NULL;
      if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is NULL", K(ret), K(ls));
      } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("restore_handler is NULL", K(ret), K(restore_handler));
      } else {
        ObLSRecoveryStatOperator ls_recovery;
        ObLSRecoveryStat ls_recovery_stat;
        int64_t max_ls_scn = OB_LS_INVALID_SCN_VALUE;
        int64_t recovery_until_scn = OB_LS_INVALID_SCN_VALUE;
        if (OB_FAIL(ls_recovery.get_ls_recovery_stat(tenant_id_,
                SYS_LS, false, ls_recovery_stat, *proxy_))) {
          LOG_WARN("failed to load sys recovery stat", KR(ret), K(tenant_id_));
        } else if (OB_FAIL(restore_handler->get_max_restore_log_ts(max_ls_scn))) {
          LOG_WARN("failed to get max restore log ts", KR(ret));
        } else if (max_ls_scn == ls_recovery_stat.get_sync_scn()) {
          //restore finish, update sync scn to restore_unti_scn
          if (OB_FAIL(restore_handler->get_upper_limit_ts(recovery_until_scn))) {
            LOG_WARN("failed to get upper limit ts", KR(ret));
          } else if (ls_recovery_stat.get_sync_scn() == recovery_until_scn) {
            //nothing
          } else if (ls_recovery_stat.get_sync_scn() > recovery_until_scn) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("in restore status, sync scn can not larger than recovery until scn", KR(ret),
                K(recovery_until_scn), K(ls_recovery_stat), K(max_ls_scn));
          } else {
            ObLSRecoveryStat ls_recovery_stat_new;
            if (OB_FAIL(construct_ls_recovery_stat(recovery_until_scn, ls_recovery_stat_new))) {
              LOG_WARN("failed to construct ls recovery stat", KR(ret), K(recovery_until_scn));
            } else if (OB_FAIL(ls_recovery.update_ls_recovery_stat(
                    ls_recovery_stat_new, *proxy_))) {
              LOG_WARN("failed to update ls recovery stat", KR(ret), K(ls_recovery_stat));
            } else {
              LOG_INFO("[RECOVERY LS] SYS LS RESTORE FINISH", K(recovery_until_scn), K(max_ls_scn), K(ls_recovery_stat));
            }
          }
        }
      }
    }
  }
  return ret;
}

}
}


