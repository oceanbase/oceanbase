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
#include "logservice/palf/log_define.h"
#include "logservice/ob_log_service.h"//open_palf
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
#include "logservice/ob_log_compression.h"
#endif
#include "share/scn.h"//SCN
#include "logservice/ob_garbage_collector.h"//ObGCLSLog
#include "logservice/palf_handle_guard.h"//ObPalfHandleGuard
#include "observer/ob_server_struct.h"              //GCTX
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
#include "rootserver/ob_ls_recovery_reportor.h" //ObLSRecoveryReportor
#include "rootserver/ob_ls_service_helper.h"//ObTenantLSInfo, ObLSServiceHelper
#include "rootserver/ob_balance_ls_primary_zone.h"
#include "src/share/balance/ob_balance_task_helper_operator.h"//insert_new_ls
#include "rootserver/ob_create_standby_from_net_actor.h" // ObCreateStandbyFromNetActor
#include "share/ls/ob_ls_life_manager.h"            //ObLSLifeManger
#include "share/ls/ob_ls_operator.h"                //ObLSAttr
#include "share/ls/ob_ls_recovery_stat_operator.h"  //ObLSRecoveryLSStatOperator
#include "share/ob_errno.h"
#include "share/ob_share_util.h"                           //ObShareUtil
#include "share/schema/ob_multi_version_schema_service.h"  //ObMultiSchemaService
#include "share/ob_primary_standby_service.h" // ObPrimaryStandbyService
#include "share/ob_standby_upgrade.h"  // ObStandbyUpgrade
#include "share/ob_upgrade_utils.h"  // ObUpgradeChecker
#include "share/ob_global_stat_proxy.h" // ObGlobalStatProxy
#include "storage/tx/ob_tx_log.h"                          //ObTxLogHeader
#include "storage/tx_storage/ob_ls_service.h"              //ObLSService
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle
#include "storage/tx/ob_multi_data_source.h" //ObTxBufferNode
#include "share/ob_log_restore_proxy.h"  // ObLogRestoreProxyUtil
#include "share/ob_occam_time_guard.h"//ObTimeGuard
#include "src/rootserver/ob_rs_event_history_table_operator.h"

namespace oceanbase
{
using namespace logservice;
using namespace transaction;
using namespace share;
using namespace storage;
using namespace palf;
namespace rootserver
{
ERRSIM_POINT_DEF(ERRSIM_END_TRANS_ERROR);

int ObRecoveryLSService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("RecLSSer",
         lib::TGDefIDs::LSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    proxy_ = GCTX.sql_proxy_;
    last_report_ts_ = OB_INVALID_TIMESTAMP;
    restore_status_.reset();
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
  last_report_ts_ = OB_INVALID_TIMESTAMP;
  restore_status_.reset();
}

int ObRecoveryLSService::get_sys_restore_status(logservice::RestoreStatusInfo &restore_status)
{
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard palf_handle_guard;
  palf::LSN report_lsn;
  SCN report_scn;
  if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("thread is in stop state");
  } else if (!restore_status_.is_valid()) {
    LOG_INFO("restore status is invalid", K(restore_status_));
  } else if (OB_FAIL(restore_status.assign(restore_status_))) {
    LOG_WARN("restore status assign failed", K(restore_status_));
  } else if (OB_FAIL(init_palf_handle_guard_(palf_handle_guard))) {
    LOG_WARN("init palf handle guard failed");
  } else if (OB_FAIL(palf_handle_guard.get_end_lsn(report_lsn))) {
    LOG_WARN("fail to get end lsn");
  } else if (OB_FAIL(palf_handle_guard.get_end_scn(report_scn))) {
    LOG_WARN("fail to get end scn");
  } else {
    restore_status.sync_lsn_ = report_lsn.val_;
    restore_status.sync_scn_ = report_scn;
  }
  return ret;
}

void ObRecoveryLSService::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_), KP(proxy_));
  } else {
    palf::PalfBufferIterator iterator(SYS_LS.id(), palf::LogIOUser::RECOVERY);//can not use without palf_guard
    int64_t idle_time_us = 100 * 1000L;
    SCN start_scn;
    last_report_ts_ = OB_INVALID_TIMESTAMP;
    uint64_t thread_idx = get_thread_idx();
    while (!has_set_stop() && OB_SUCC(ret)) {
      ObCurTraceId::init(GCONF.self_addr_);
      ObTenantInfoLoader *tenant_info_loader = MTL(ObTenantInfoLoader*);
      ObAllTenantInfo tenant_info;
      //two thread for seed log and recovery_ls_manager
      if (!is_user_tenant(tenant_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls recovery thread must run on user tenant", KR(ret), K(tenant_id_));
      } else if (OB_ISNULL(tenant_info_loader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant report is null", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
        LOG_WARN("failed to get tenant info", KR(ret));
      } else if (OB_UNLIKELY(tenant_info.is_primary())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant info is primary", KR(ret), K(tenant_info));
      } else if (OB_FAIL(check_can_do_recovery_(tenant_id_))) {
        LOG_WARN("can not do recovery now", KR(ret), K(tenant_id_));
        restore_status_.reset();
      } else if (0 == thread_idx) {
        idle_time_us = 10 * 1000 * 1000L;
        if (OB_FAIL(process_thread0_(tenant_info))) {
          LOG_WARN("failed to process thread0", KR(ret));
        }
      } else {
        if (OB_FAIL(process_thread1_(tenant_info, start_scn, iterator))) {
          LOG_WARN("failed to process thread1", KR(ret));
        }
      }//end thread1
      LOG_INFO("[LS_RECOVERY] finish one round", KR(ret), K(idle_time_us),
               K(start_scn), K(thread_idx), K(tenant_info), K_(restore_status));
      idle(idle_time_us);
      ret = OB_SUCCESS;
    }//end while
    restore_status_.reset();
  }
}

int ObRecoveryLSService::process_thread0_(const ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  DEBUG_SYNC(STOP_RECOVERY_LS_THREAD0);
  //adjust primary zone and balance ls group
  if (OB_TMP_FAIL(do_standby_balance_())) {
    ret = OB_SUCC(ret) ? tmp_ret : ret;
    LOG_WARN("do standby balance", KR(ret), KR(tmp_ret));
  }
  if (OB_TMP_FAIL(do_ls_balance_task_())) {
    ret = OB_SUCC(ret) ? tmp_ret : ret;
    LOG_WARN("failed to process ls balance task", KR(ret), KR(tmp_ret));
  }
  (void)try_tenant_upgrade_end_();

  return ret;
}

int ObRecoveryLSService::process_thread1_(const ObAllTenantInfo &tenant_info,
     share::SCN &start_scn,
     palf::PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(STOP_RECOVERY_LS_THREAD1);
  if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant info is invalid", KR(ret), K(tenant_info));
  } else if (OB_UNLIKELY(!inited_) || OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_), KP(proxy_));
  } else {
    palf::PalfHandleGuard palf_handle_guard;
    ObLSRecoveryStat ls_recovery_stat;
    ObLSRecoveryStatOperator ls_recovery;
    if (OB_FAIL(init_palf_handle_guard_(palf_handle_guard))) {
      LOG_WARN("failed to init palf handle guard", KR(ret));
    } else if (!start_scn.is_valid()) {
      if (OB_FAIL(ls_recovery.get_ls_recovery_stat(tenant_id_,
              SYS_LS, false, ls_recovery_stat, *proxy_))) {
        LOG_WARN("failed to load sys recovery stat", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(report_sys_ls_recovery_stat_(ls_recovery_stat.get_sync_scn(), true,
              "report readable_scn while start_scn is invalid"))) {
        //may recovery end, but readable scn need report
        LOG_WARN("failed to report ls recovery stat", KR(ret), K(ls_recovery_stat));
      } else if (tenant_info.get_recovery_until_scn() <= ls_recovery_stat.get_sync_scn()) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) { // every minute
          LOG_INFO("has recovered to recovery_until_scn", KR(ret), K(ls_recovery_stat), K(tenant_info));
        }
      } else if (OB_FAIL(seek_log_iterator_(ls_recovery_stat.get_sync_scn(), palf_handle_guard, iterator))) {
        LOG_WARN("failed to seek log iterator", KR(ret), K(ls_recovery_stat));
      } else {
        start_scn = ls_recovery_stat.get_sync_scn();
        LOG_INFO("start to seek at", K(start_scn));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (start_scn.is_valid() && OB_FAIL(process_ls_log_(tenant_info, start_scn, iterator))) {
      if (OB_ITER_STOP != ret) {
        LOG_WARN("failed to process ls log", KR(ret), K(start_scn), K(tenant_info));
      }
    }
    if (OB_FAIL(ret)) {
      start_scn.reset();
    }
  }
  return ret;
}

int ObRecoveryLSService::init_palf_handle_guard_(palf::PalfHandleGuard &palf_handle_guard)
{
  int ret = OB_SUCCESS;
  palf_handle_guard.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else {
    logservice::ObLogService *log_service = MTL(logservice::ObLogService *);
    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log service is null", KR(ret));
    } else if (OB_FAIL(log_service->open_palf(SYS_LS, palf_handle_guard))) {
      LOG_WARN("ObLogService open_palf fail", KR(ret), K(tenant_id_));
    }
  }

  return ret;
}

int ObRecoveryLSService::seek_log_iterator_(const SCN &sync_scn,
    palf::PalfHandleGuard &palf_handle_guard,
    PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  palf::LSN start_lsn(0);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!sync_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sync scn is invalid", KR(ret), K(sync_scn));
  } else if (SCN::base_scn() == sync_scn) {
    // start_lsn = 0;
  } else if (OB_FAIL(palf_handle_guard.locate_by_scn_coarsely(sync_scn, start_lsn))) {
    LOG_WARN("failed to locate lsn", KR(ret), K(sync_scn));
  }
  if (FAILEDx(palf_handle_guard.seek(start_lsn, iterator))) {
    LOG_WARN("failed to seek iterator", KR(ret), K(sync_scn), K(start_lsn));
  }
  return ret;
}

int ObRecoveryLSService::process_ls_log_(
      const ObAllTenantInfo &tenant_info,
      SCN &start_scn,
      PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  palf::LogEntry log_entry;
  palf::LSN target_lsn;
  SCN sync_scn;
  SCN last_sync_scn;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_info));
  }
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
  char *decompress_buf = NULL;
  int64_t decompress_buf_len = 0;
  int64_t decompressed_len = 0;
#endif
  const char *log_body = NULL;
  int64_t log_body_size = 0;
  while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
    if (OB_FAIL(iterator.get_entry(log_entry, target_lsn))) {
      LOG_WARN("failed to get log", KR(ret), K(log_entry));
    } else if (OB_ISNULL(log_entry.get_data_buf())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log entry is null", KR(ret));
    } else {
      LOG_DEBUG("get log", K(log_entry), K(target_lsn), K(start_scn));
      sync_scn = log_entry.get_scn();
      const char *log_buf = log_entry.get_data_buf();
      const int64_t log_length = log_entry.get_data_len();
      logservice::ObLogBaseHeader header;
      const int64_t HEADER_SIZE = header.get_serialize_size();
      int64_t log_pos = 0;
      if (OB_UNLIKELY(sync_scn <= start_scn)) {
        //通过scn定位的LSN是不准确的，可能会获取多余的数据，所以需要把小于等于sync_scn的过滤掉
        continue;
      } else if (tenant_info.get_recovery_until_scn() < sync_scn) {
        if (OB_FAIL(report_sys_ls_recovery_stat_(tenant_info.get_recovery_until_scn(), false,
                "SYS log scn beyond recovery_until_scn"))) {
          LOG_WARN("failed to report_sys_ls_recovery_stat_", KR(ret), K(sync_scn), K(tenant_info),
                   K(log_entry), K(target_lsn), K(start_scn));
        } else {
          ret = OB_ITER_STOP;
          LOG_WARN("SYS LS has recovered to the recovery_until_scn, need stop iterate SYS LS log",
                   KR(ret), K(sync_scn), K(tenant_info), K(log_entry), K(target_lsn), K(start_scn));
          start_scn.reset();
          restore_status_.reset(); // need to reset restore status if iterate to recovery end
        }
        //防止sync_scn - 1被汇报上去
        sync_scn.reset();
      } else if (OB_FAIL(header.deserialize(log_buf, HEADER_SIZE, log_pos))) {
        LOG_WARN("failed to deserialize", KR(ret), K(HEADER_SIZE));
      } else if (OB_UNLIKELY(log_pos >= log_length)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log pos is not expected", KR(ret), K(log_pos), K(log_length));
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
      } else if (header.is_compressed()) {
        if (OB_FAIL(decompress_log_payload_(log_buf + log_pos, log_length - log_pos,
                                            decompress_buf, decompressed_len))) {
          LOG_WARN("failed to decompress log payload", K(header), K(log_entry));
        } else {
          log_body = decompress_buf;
          log_body_size = decompressed_len;
        }
#endif
      } else {
        log_body = log_buf + log_pos;
        log_body_size = log_length - log_pos;
      }

      if (OB_SUCC(ret)) {
        int64_t pos = 0;
        if (logservice::GC_LS_LOG_BASE_TYPE == header.get_log_type()) {
          ObGCLSLog gc_log;
          if (OB_FAIL(gc_log.deserialize(log_body, log_body_size, pos))) {
            LOG_WARN("failed to deserialize gc log", KR(ret), K(log_body_size));
          } else if (ObGCLSLOGType::OFFLINE_LS == gc_log.get_log_type()) {
            //set sys ls offline
            if (OB_FAIL(process_gc_log_(gc_log, sync_scn))) {
              LOG_WARN("failed to process gc log", KR(ret), K(sync_scn));
            }
          }
          // nothing
        } else if (logservice::TRANS_SERVICE_LOG_BASE_TYPE == header.get_log_type()) {
          ObTxLogBlock tx_log_block;
          if (OB_FAIL(tx_log_block.init_for_replay(log_body, log_body_size, pos))) {
            LOG_WARN("failed to init tx log block", KR(ret), K(log_length));
          } else if (OB_FAIL(process_ls_tx_log_(tx_log_block, sync_scn))) {
            LOG_WARN("failed to process ls tx log", KR(ret), K(tx_log_block), K(sync_scn));
          }
        } else {}
      }

      if (OB_SUCC(ret)) {
        last_sync_scn = sync_scn;
      } else if (sync_scn.is_valid()) {
        //如果本条日志不可用，至少汇报sync_scn - 1，
        //防止拉日志齐步走和系统日志流的汇报参考其他日志流出现的死锁问题
        last_sync_scn = SCN::scn_dec(sync_scn);
      }

      if (last_sync_scn.is_valid() && (OB_FAIL(ret) || OB_INVALID_TIMESTAMP == last_report_ts_
            || ObTimeUtility::current_time() - last_report_ts_ > 100 * 1000)) {
        //if ls_operator can not process, need to report last sync scn
        int tmp_ret = OB_SUCCESS;
        const char* comment = OB_SUCC(ret) ? "regular report when iterate log" :
                              "report sync_scn -1 when not valid to process";
        if (OB_TMP_FAIL(report_sys_ls_recovery_stat_(last_sync_scn, false, comment))) {
          LOG_WARN("failed to report ls recovery stat", KR(ret), KR(tmp_ret),
                    K(last_sync_scn), K(comment));
        }
      }
    }
  }//end for each log

#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
  if (NULL != decompress_buf) {
    mtl_free(decompress_buf);
  }
#endif

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (!sync_scn.is_valid() || start_scn >= sync_scn) {
      //No logs need to be iterated, or iterate log have beed reported
      //but need to report readable_scn and tenant_info
      const char* comment = sync_scn.is_valid() ? "iterate no new log" : "iterate no log";
      if (OB_FAIL(report_sys_ls_recovery_stat_(start_scn, true, comment))) {
        LOG_WARN("failed to report sys ls readable scn", KR(ret), K(start_scn), K(sync_scn), K(comment));
      }
    } else if (OB_FAIL(report_sys_ls_recovery_stat_(sync_scn, false,
            "iterate log end"))) {
      LOG_WARN("failed to report ls recovery stat", KR(ret), K(sync_scn));
    } else {
      LOG_INFO("iterate log end, start scn change", K(start_scn), K(sync_scn));
      start_scn = sync_scn;
    }
  } else if (OB_SUCC(ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterate must be end", KR(ret), K(iterator));
  } else {
    LOG_WARN("failed to get next log", KR(ret), K(iterator));
  }

  return ret;
}

int ObRecoveryLSService::process_ls_tx_log_(ObTxLogBlock &tx_log_block, const SCN &sync_scn)
{
  int ret = OB_SUCCESS;
  bool has_operation = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  }
  while (OB_SUCC(ret)) {
    transaction::ObTxLogHeader tx_header;
    bool contain_big_segment = false;
    if (OB_FAIL(tx_log_block.get_next_log(tx_header, nullptr, &contain_big_segment))) {
      if (OB_ITER_END == ret || contain_big_segment) {
        if (contain_big_segment) {
          LOG_INFO("skip process a big segment log", KR(ret), K(tx_log_block));
        }
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next log", KR(ret));
      }
    } else if (transaction::ObTxLogType::TX_COMMIT_LOG !=
               tx_header.get_tx_log_type()) {
      // nothing
    } else if (has_operation) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("has more commit log", KR(ret), K(sync_scn));
    } else {
      ObTxCommitLogTempRef temp_ref;
      ObTxCommitLog commit_log(temp_ref);
      //TODO commit log may too large
      if (OB_FAIL(tx_log_block.deserialize_log_body(commit_log))) {
        LOG_WARN("failed to deserialize", KR(ret));
      } else {
        const ObTxBufferNodeArray &source_data =
            commit_log.get_multi_source_data();
        const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
        ObMySQLTransaction trans;
        for (int64_t i = 0; OB_SUCC(ret) && i < source_data.count(); ++i) {
          const ObTxBufferNode &node = source_data.at(i);
          if (OB_FAIL(try_cancel_clone_job_for_standby_tenant_(node))) {
            LOG_WARN("fail to cancel clone job for standby tenant", KR(ret), K(node));
          } else if (ObTxDataSourceType::STANDBY_UPGRADE == node.get_data_source_type()) {
            if (OB_FAIL(process_upgrade_log_(sync_scn, node))) {
              LOG_WARN("failed to process_upgrade_log_", KR(ret), K(node));
            }
          } else if (ObTxDataSourceType::STANDBY_UPGRADE_DATA_VERSION !=
                         node.get_data_source_type() &&
                     ObTxDataSourceType::LS_TABLE !=
                         node.get_data_source_type() &&
                     ObTxDataSourceType::TRANSFER_TASK !=
                         node.get_data_source_type()) {
            // nothing
          } else if (! trans.is_started() && OB_FAIL(trans.start(proxy_, exec_tenant_id))) {
            LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
          } else if (FALSE_IT(has_operation = true)) {
            //can not be there;
          } else if (OB_FAIL(check_valid_to_operator_ls_(sync_scn))) {
            LOG_WARN("failed to check valid to operator ls", KR(ret), K(sync_scn));
          } else if (ObTxDataSourceType::STANDBY_UPGRADE_DATA_VERSION ==
                     node.get_data_source_type()) {
            if (OB_FAIL(process_upgrade_data_version_log_(sync_scn, node, trans))) {
              LOG_WARN("failed to process_upgrade_data_version_log_", KR(ret), K(node));
            }
          } else if (ObTxDataSourceType::TRANSFER_TASK == node.get_data_source_type()) {
            if (OB_FAIL(process_ls_transfer_task_in_trans_(node, sync_scn, trans))) {
              LOG_WARN("failed to process ls transfer task", KR(ret), K(node));
            }
          } else if (OB_FAIL(process_ls_table_in_trans_(node, sync_scn, trans))) {
            //TODO ls recovery is too fast for ls manager, so it maybe failed, while change ls status
            //consider how to retry
            LOG_WARN("failed to process ls operator", KR(ret), K(node), K(sync_scn));
          }
        }// end for
        if (OB_FAIL(ret) || !has_operation) {
        } else if (OB_FAIL(report_sys_ls_recovery_stat_in_trans_(sync_scn, false, trans,
                "report recovery stat and has multi data source"))) {
          LOG_WARN("failed to report sys ls recovery stat", KR(ret), K(sync_scn));
        } else if (OB_FAIL(check_standby_tenant_not_in_cloning_(trans))) {
          LOG_WARN("fail to check standby tenant in cloning", KR(ret));
        }
        ret = ERRSIM_END_TRANS_ERROR ? : ret;
        END_TRANSACTION(trans)
      }
    }
  }  // end while for each tx_log

  return ret;
}

int ObRecoveryLSService::try_cancel_clone_job_for_standby_tenant_(
    const transaction::ObTxBufferNode &node)
{
  int ret = OB_SUCCESS;
  bool is_conflict_with_clone = true;
  if (OB_UNLIKELY(!node.is_valid())
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(node), K_(tenant_id));
  } else if (OB_FAIL(log_type_conflict_with_clone_procedure_(node, is_conflict_with_clone))) {
    LOG_WARN("fail to check whether log type is conflict with clone", KR(ret), K(node));
  } else if (is_conflict_with_clone) {
    // determine case to check
    ObConflictCaseWithClone case_to_check;
    if (ObTxDataSourceType::STANDBY_UPGRADE == node.get_data_source_type()) {
      case_to_check = ObConflictCaseWithClone::STANDBY_UPGRADE;
    } else if (ObTxDataSourceType::LS_TABLE == node.get_data_source_type()) {
      case_to_check = ObConflictCaseWithClone::STANDBY_MODIFY_LS;
    } else if (ObTxDataSourceType::TRANSFER_TASK == node.get_data_source_type()) {
      case_to_check = ObConflictCaseWithClone::STANDBY_TRANSFER;
    } else {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("unexpected log type", KR(ret), K(node));
    }
    // cancel clone job if exists
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTenantSnapshotUtil::cancel_existed_clone_job_if_need(tenant_id_, case_to_check))) {
      LOG_WARN("fail to check tenant whether in cloning ", KR(ret), K_(tenant_id), K(case_to_check));
    }
  } else {
    // only standby upgrade/transfer/ls_alter conflict with standby clone job
    // So if multi source log not these types, just skip, where is no need to cancel clone job
    LOG_TRACE("log type is not conflict with clone operation", K(node));
  }
  DEBUG_SYNC(AFTER_FIRST_CLONE_CHECK_FOR_STANDBY);
  return ret;
}

int ObRecoveryLSService::check_standby_tenant_not_in_cloning_(
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_cloning = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_standby_tenant_not_in_cloning_procedure(
                         trans, tenant_id_, is_cloning))) {
    LOG_WARN("fail to check standby tenant whether in cloning procedure", KR(ret), K_(tenant_id));
  } else if (is_cloning) {
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("tenant is cloning, can not process balance task", KR(ret), K_(tenant_id), K(is_cloning));
  }
  return ret;
}

int ObRecoveryLSService::log_type_conflict_with_clone_procedure_(
    const transaction::ObTxBufferNode &node,
    bool &is_conflict)
{
  int ret = OB_SUCCESS;
  is_conflict = true;
  if (OB_UNLIKELY(!node.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(node));
  } else if (ObTxDataSourceType::STANDBY_UPGRADE == node.get_data_source_type()
             || ObTxDataSourceType::LS_TABLE == node.get_data_source_type()
             || ObTxDataSourceType::TRANSFER_TASK == node.get_data_source_type()) {
    is_conflict = true;
  } else {
    is_conflict = false;
  }
  return ret;
}

int ObRecoveryLSService::process_upgrade_log_(
    const share::SCN &sync_scn, const ObTxBufferNode &node)
{
  int ret = OB_SUCCESS;
  uint64_t standby_data_version = 0;

  if (!node.is_valid() || !sync_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is invalid", KR(ret), K(node), K(sync_scn));
  } else {
    ObStandbyUpgrade primary_data_version;
    int64_t pos = 0;
    if (OB_FAIL(primary_data_version.deserialize(node.get_data_buf().ptr(), node.get_data_buf().length(), pos))) {
      LOG_WARN("failed to deserialize", KR(ret), K(node), KPHEX(node.get_data_buf().ptr(), node.get_data_buf().length()));
    } else if (OB_UNLIKELY(pos > node.get_data_buf().length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get primary_data_version", KR(ret), K(pos), K(node.get_data_buf().length()));
    } else {
      LOG_INFO("get primary_data_version", K(primary_data_version));
      uint64_t current_data_version = 0;//用户网络备库的版本号校验，防止备租户创建的版本号大于主租户的版本号
      if (!primary_data_version.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary_data_version not valid", KR(ret), K(primary_data_version));
      } else if (OB_FAIL(ObUpgradeChecker::get_data_version_by_cluster_version(
                                           GET_MIN_CLUSTER_VERSION(),
                                           standby_data_version))) {
        LOG_WARN("failed to get_data_version_by_cluster_version", KR(ret), K(GET_MIN_CLUSTER_VERSION()));
      } else if (primary_data_version.get_data_version() > standby_data_version) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(30 * 60 * 1000 * 1000)) { // 30min
          LOG_ERROR("standby version is not new enough to recover primary clog", KR(ret),
                   K(primary_data_version), K(standby_data_version));
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(init_restore_status(sync_scn, OB_ERR_RESTORE_STANDBY_VERSION_LAG))) {
          LOG_WARN("failed to init restore status", KR(tmp_ret), K(sync_scn));
        }
      } else if (OB_FAIL(get_min_data_version_(current_data_version))) {
        LOG_WARN("failed to get min data version", KR(ret));
      } else if (0 == current_data_version) {
        //standby cluster not set data version now, need retry
        ret = OB_EAGAIN;
        LOG_WARN("standby data version not valid, need retry", KR(ret), K(current_data_version),
            K(primary_data_version));
      } else if (primary_data_version.get_data_version() < current_data_version) {
        ret = OB_ITER_STOP;
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {//1min
          LOG_ERROR("standby version is larger than primary version", KR(ret),
              K(current_data_version), K(primary_data_version));
        }
      }
    }
  }
  return ret;
}

int ObRecoveryLSService::process_upgrade_data_version_log_(
    const share::SCN &sync_scn,
    const ObTxBufferNode &node,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t standby_data_version = 0;

  if (!node.is_valid() || !sync_scn.is_valid() || !trans.is_started()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(node), K(sync_scn), "trans_started",
             trans.is_started());
  } else {
    ObStandbyUpgrade primary_data_version;
    int64_t pos = 0;
    if (OB_FAIL(primary_data_version.deserialize(
            node.get_data_buf().ptr(), node.get_data_buf().length(), pos))) {
      LOG_WARN("failed to deserialize", KR(ret), K(node),
               KPHEX(node.get_data_buf().ptr(), node.get_data_buf().length()));
    } else if (OB_UNLIKELY(pos > node.get_data_buf().length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get primary_data_version", KR(ret), K(pos),
               K(node.get_data_buf().length()));
    } else if (!primary_data_version.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary_data_version not valid", KR(ret), K(primary_data_version));
    } else {
      const uint64_t primary_data_version_value = primary_data_version.get_data_version();
      uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
      ObGlobalStatProxy stat_proxy(trans, exec_tenant_id);
      if (OB_FAIL(stat_proxy.update_finish_data_version(primary_data_version_value, sync_scn))) {
        LOG_WARN("fail to update finish_data_version", K(ret), K(tenant_id_), K(sync_scn),
                  K(primary_data_version_value));
      }
    }
    LOG_INFO("handle upgrade_data_version_log", KR(ret), K(tenant_id_), K(sync_scn),
             K(primary_data_version));
  }
  return ret;
}

int ObRecoveryLSService::get_min_data_version_(uint64_t &compatible)
{
  int ret = OB_SUCCESS;
  compatible = 0;
  if (OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt("select value as compatible from %s where tenant_id = '%lu' and name = 'compatible' ",
                                 OB_TENANT_PARAMETER_TNAME, tenant_id_))) {
        LOG_WARN("fail to generate sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(proxy_->read(result, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("read config from __tenant_parameter failed",
                KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("config result is null", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        if (OB_ITER_END == ret) {
          LOG_WARN("compatible not set, it is net standby tenant", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
          ret = OB_SUCCESS;
          compatible = 0;
        } else {
          LOG_WARN("get result next failed", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
        }
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
      } else {
        ObString compatible_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result.get_result(), "compatible", compatible_str);

        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get result", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
        } else if (OB_FAIL(ObClusterVersion::get_version(compatible_str, compatible))) {
          LOG_WARN("parse version failed", KR(ret), K(compatible_str));
        }
      }
    }
  }
  return ret;
}

void ObRecoveryLSService::try_tenant_upgrade_end_()
{
  int ret = OB_SUCCESS;
  uint64_t min_data_version = 0;
  obrpc::ObCommonRpcProxy *rs_rpc_proxy_ = GCTX.rs_rpc_proxy_;
  if (OB_ISNULL(proxy_) || !is_user_tenant(tenant_id_) || OB_ISNULL(rs_rpc_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", KR(ret), KP(proxy_), K_(tenant_id), KP(rs_rpc_proxy_));
  } else if (OB_FAIL(get_min_data_version_(min_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
  } else if (min_data_version == DATA_CURRENT_VERSION) {
    // already upgrade end
  } else {
    ObGlobalStatProxy proxy(*proxy_, tenant_id_);
    uint64_t target_data_version = 0;
    uint64_t current_data_version = 0;
    if (OB_FAIL(proxy.get_target_data_version(false /* for_update */, target_data_version))) {
      LOG_WARN("fail to get target data version", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K_(tenant_id));
    } else if (!(target_data_version == current_data_version
              && target_data_version != min_data_version
              && min_data_version <= DATA_CURRENT_VERSION)) {
      ret = EAGAIN;
      LOG_WARN("data_version not match, run upgrade end later",
               KR(ret), K_(tenant_id), K(target_data_version), K(current_data_version),
               K(DATA_CURRENT_VERSION), K(min_data_version));
    } else {
      HEAP_VAR(obrpc::ObAdminSetConfigItem, item) {
      ObSchemaGetterGuard guard;
      const ObSimpleTenantSchema *tenant = NULL;
      obrpc::ObAdminSetConfigArg arg;
      item.exec_tenant_id_ = OB_SYS_TENANT_ID;
      const int64_t timeout = GCONF.internal_sql_execute_timeout;
      int64_t pos = ObClusterVersion::print_version_str(
                    item.value_.ptr(), item.value_.capacity(),
                    current_data_version);
      if (pos <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("current_data_version is invalid",
                  KR(ret), K_(tenant_id), K(current_data_version));
      } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
        LOG_WARN("fail to get schema guard", KR(ret));
      } else if (OB_FAIL(guard.get_tenant_info(tenant_id_, tenant))) {
        LOG_WARN("fail to get tenant info", KR(ret), K_(tenant_id));
      } else if (OB_ISNULL(tenant)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(item.tenant_name_.assign(tenant->get_tenant_name()))) {
        LOG_WARN("fail to assign tenant name", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(item.name_.assign("compatible"))) {
        LOG_WARN("fail to assign config name", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(arg.items_.push_back(item))) {
        LOG_WARN("fail to push back item", KR(ret), K(item));
      } else if (OB_FAIL(rs_rpc_proxy_->timeout(timeout).admin_set_config(arg))) {
        LOG_WARN("fail to set config", KR(ret), K(arg), K(timeout));
      } else {
        LOG_INFO("upgrade end", KR(ret), K(arg), K(timeout), K_(tenant_id), K(target_data_version),
                 K(current_data_version), K(DATA_CURRENT_VERSION), K(min_data_version));
      }
      } // end HEAP_VAR
    }
  }
}

int ObRecoveryLSService::process_gc_log_(logservice::ObGCLSLog &gc_log, const SCN &sync_scn)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  ObLSLifeAgentManager ls_life_agent(*proxy_);
  ObTenantInfoLoader *tenant_info_loader = MTL(ObTenantInfoLoader*);
  ObAllTenantInfo tenant_info;
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
   } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(meta_tenant_id));
   } else if (OB_ISNULL(tenant_info_loader)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("tenant report is null", KR(ret), K(tenant_id_));
   } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
     LOG_WARN("failed to get tenant info", KR(ret));
   } else if (OB_UNLIKELY(tenant_info.is_primary())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("tenant info is primary", KR(ret), K(tenant_info));
   } else if (OB_FAIL(ls_life_agent.set_ls_offline_in_trans(
           tenant_id_, SYS_LS, share::OB_LS_TENANT_DROPPING, sync_scn, tenant_info.get_switchover_status(),
            trans))) {
    LOG_WARN("failed to set offline", KR(ret), K(tenant_id_), K(sync_scn), K(tenant_info));
  } else if (OB_FAIL(report_sys_ls_recovery_stat_in_trans_(sync_scn, false, trans,
          "report recovery stat and process gc log"))) {
    LOG_WARN("failed to report sys ls recovery stat", KR(ret), K(sync_scn));
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

int ObRecoveryLSService::construct_sys_ls_recovery_stat_based_on_sync_scn_(
    const SCN &sync_scn,
    ObLSRecoveryStat &ls_stat,
    const ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ls_stat.reset();
  ObLSService *ls_svr = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSRecoveryStat tmp_ls_stat;
  SCN readable_scn;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, storage::ObLSGetMod::RS_MOD))) {
    LOG_WARN("failed to get ls", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is NULL", KR(ret), K(ls_handle));
  } else if (OB_FAIL(ls->get_ls_level_recovery_stat(tmp_ls_stat))) {
    LOG_WARN("failed to get readable scn", KR(ret));
  } else if (sync_scn < tmp_ls_stat.get_readable_scn()) {
    // SYS LS latest readable scn may be greater than sync_scn in __all_ls_recovery_stat
    //
    // 1. At first, Tenant is PRIMARY, SYS LS latest log scn is 100, and report to __all_ls_recovery_stat.
    //
    // 2. Then, Tenant switch to STANDBY, tenant sync_scn in __all_tenant_info is advanced to 200, and
    //    tenant replayable_scn in __all_tenant_info is also advanced to 200.
    //
    // 3. SYS LS continues to sync log, next log scn must be greater than 200, for example, next log
    //    scn is 300. Now, SYS LS sync_scn in __all_ls_recovery_stat is still 100.
    //
    // 4. Now, we decide to report SYS LS recovery stat. Its sync_scn is 100, but the readable_scn
    //    will be advanced to 200 by replay engine. Because tenant replayable scn is 200.
    //    There is no log between 100 and 200.
    //
    // So, Here SYS LS readable_scn may be greater than sync_scn in __all_ls_recovery_stat.
    //
    // In this case, we just change the readable_scn to be same with sync_scn.

    readable_scn = sync_scn;

    LOG_INFO("SYS LS sync_scn in __all_ls_recovery_stat is less than its real readable scn. "
        "This is NORMAL after tenant switchover from PRIMARY TO STANDBY.",
        KR(ret), K(readable_scn), K(tenant_info), K(tmp_ls_stat));
  } else {
    readable_scn = tmp_ls_stat.get_readable_scn();
  }

  if (FAILEDx(ls_stat.init_only_recovery_stat(tenant_id_, SYS_LS, sync_scn, readable_scn,
          tmp_ls_stat.get_config_version()))) {
    LOG_WARN("failed to init ls recovery stat", KR(ret), K(tenant_id_),
             K(sync_scn), K(readable_scn), K(tmp_ls_stat), K(tenant_info));
  }
  return ret;
}

int ObRecoveryLSService::process_ls_table_in_trans_(const transaction::ObTxBufferNode &node,
    const SCN &sync_scn, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!node.is_valid() || !sync_scn.is_valid()
        || ObTxDataSourceType::LS_TABLE != node.get_data_source_type()
        || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sync_scn), K(node), "trans_start", trans.is_started());
  } else {
    ObLSAttr ls_attr;
    int64_t pos = 0;
    if (OB_FAIL(ls_attr.deserialize(node.get_data_buf().ptr(), node.get_data_buf().length(),
            pos))) {
      LOG_WARN("failed to deserialize", KR(ret), K(node));
    } else if (OB_UNLIKELY(pos > node.get_data_buf().length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ls attr", KR(ret), K(pos), K(node));
    } else {
      LOG_INFO("get ls operation", K(ls_attr), K(sync_scn));
      if (share::is_ls_tenant_drop_pre_op(ls_attr.get_ls_operation_type())) {
        ret = OB_ITER_STOP;
        LOG_WARN("can not process ls operator after tenant dropping", K(ls_attr));
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(init_restore_status(sync_scn, OB_ERR_RESTORE_PRIMARY_TENANT_DROPPED))) {
          LOG_WARN("failed to init restore status", KR(tmp_ret), K(sync_scn), KR(tmp_ret));
        }
      } else if (share::is_ls_tenant_drop_op(ls_attr.get_ls_operation_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls recovery must stop while pre tenant dropping", KR(ret), K(ls_attr));
      } else if (OB_FAIL(process_ls_operator_in_trans_(ls_attr, sync_scn, trans))) {
        LOG_WARN("failed to process ls operator in trans", KR(ret), K(ls_attr), K(sync_scn));
      }
    }
  }
  return ret;
}

int ObRecoveryLSService::check_valid_to_operator_ls_(const SCN &sync_scn)
{
  int ret = OB_SUCCESS;
  bool has_user_ls = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!sync_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("syns scn is invalid", KR(ret), K(sync_scn));
  } else if (OB_FAIL(ObCreateStandbyFromNetActor::check_has_user_ls(tenant_id_, proxy_, has_user_ls))) {
    LOG_WARN("check_has_user_ls failed", KR(ret), K(tenant_id_));
  } else if (has_user_ls) {
    SCN user_scn;
    ObLSRecoveryStatOperator ls_recovery;
    if (OB_FAIL(ls_recovery.get_user_ls_sync_scn(tenant_id_, *proxy_, user_scn))) {
      LOG_WARN("failed to get user scn", KR(ret), K(tenant_id_));
    } else if (user_scn >= sync_scn) {
      //other ls has larger sync scn, ls can operator
    } else {
      ret = OB_NEED_WAIT;
      LOG_WARN("can not process ls operator, need wait other ls sync", KR(ret),
          K(user_scn), K(sync_scn));
      restore_status_.reset();
    }
  }

  return ret;
}

int ObRecoveryLSService::process_ls_operator_in_trans_(
    const share::ObLSAttr &ls_attr, const SCN &sync_scn, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTenantInfoLoader *tenant_info_loader = MTL(ObTenantInfoLoader*);
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!ls_attr.is_valid() || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls attr is invalid", KR(ret), K(ls_attr), "trans_start", trans.is_started());
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant report is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  } else if (OB_UNLIKELY(tenant_info.is_primary())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info is primary", KR(ret), K(tenant_info));
  } else {
    ObLSStatusOperator ls_operator;
    share::ObLSStatusInfo ls_status;
    ObLSLifeAgentManager ls_life_agent(*proxy_);
    if (OB_UNLIKELY(!ls_attr.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls attr is invalid", KR(ret), K(ls_attr));
    } else if (OB_FAIL(share::is_ls_alter_ls_group_op(ls_attr.get_ls_operation_type()))) {
      if (OB_FAIL(porcess_alter_ls_group_(ls_attr, sync_scn, trans))) {
        LOG_WARN("failed to process alter ls group", KR(ret), K(ls_attr), K(sync_scn));
      }
    } else if (share::is_ls_create_pre_op(ls_attr.get_ls_operation_type())) {
      //create new ls;
      if (OB_FAIL(create_new_ls_(ls_attr, sync_scn, tenant_info.get_switchover_status(), trans))) {
        LOG_WARN("failed to create new ls", KR(ret), K(sync_scn), K(ls_attr), K(tenant_info));
      }
    } else if (share::is_ls_create_abort_op(ls_attr.get_ls_operation_type())) {
      if (OB_FAIL(ls_life_agent.drop_ls_in_trans(tenant_id_, ls_attr.get_ls_id(),
              tenant_info.get_switchover_status(), trans))) {
        LOG_WARN("failed to drop ls", KR(ret), K(tenant_id_), K(ls_attr));
      }
    } else if (OB_FAIL(ls_operator.get_ls_status_info(tenant_id_, ls_attr.get_ls_id(),
            ls_status, trans))) {
      LOG_WARN("failed to get ls status", KR(ret), K(tenant_id_), K(ls_attr));
    } else if (share::is_ls_drop_end_op(ls_attr.get_ls_operation_type())) {
      if (OB_FAIL(ls_life_agent.set_ls_offline_in_trans(tenant_id_, ls_attr.get_ls_id(),
              ls_status.status_, sync_scn, tenant_info.get_switchover_status(), trans))) {
        LOG_WARN("failed to set offline", KR(ret), K(tenant_id_), K(ls_attr),
            K(sync_scn), K(tenant_info), K(ls_status));
      }
    } else if (ls_status.ls_is_creating()) {
      //can not be creating, must be created or other status
      ret = OB_EAGAIN;
      LOG_WARN("ls not created, need wait", KR(ret), K(ls_status));
      int tmp_ret = OB_SUCCESS;
      //reuse OB_LS_NOT_EXIST error code, which means the ls has not created in this scenario.
      if (OB_TMP_FAIL(init_restore_status(sync_scn, OB_LS_NOT_EXIST))) {
        LOG_WARN("failed to init restore status", KR(tmp_ret), K(sync_scn));
      }
    } else {
      ObLSStatus target_status = share::OB_LS_EMPTY;
      if (share::is_ls_create_end_op(ls_attr.get_ls_operation_type())) {
        // set ls to normal
        target_status = share::OB_LS_NORMAL;
      } else if (share::is_ls_tenant_drop_op(ls_attr.get_ls_operation_type())) {
        target_status = share::OB_LS_TENANT_DROPPING;
      } else if (share::is_ls_drop_pre_op(ls_attr.get_ls_operation_type())) {
        target_status = share::OB_LS_DROPPING;
      } else if (share::is_ls_tenant_drop_pre_op(ls_attr.get_ls_operation_type())) {
        target_status = share::OB_LS_PRE_TENANT_DROPPING;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected operation type", KR(ret), K(ls_attr));
      }
      if (FAILEDx(ls_operator.update_ls_status_in_trans(tenant_id_, ls_attr.get_ls_id(),
              ls_status.status_, target_status, tenant_info.get_switchover_status(), trans))) {
        LOG_WARN("failed to update ls status", KR(ret), K(tenant_id_), K(ls_attr),
            K(ls_status), K(target_status));
      }
      ret = ERRSIM_END_TRANS_ERROR ? : ret;
      LOG_INFO("[LS_RECOVERY] update ls status", KR(ret), K(ls_attr), K(target_status));
    }
  }

  return ret;
}

int ObRecoveryLSService::porcess_alter_ls_group_(const share::ObLSAttr &ls_attr,
                                                 const share::SCN &sync_scn,
                                                 common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!share::is_ls_alter_ls_group_op(ls_attr.get_ls_operation_type())
      || !sync_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_attr), K(sync_scn));
  } else {
    ObBalanceTaskHelper ls_balance_task;
    ObBalanceTaskHelperOp task_op(share::ObBalanceTaskHelperOp::LS_BALANCE_TASK_OP_ALTER);
    if (OB_FAIL(ls_balance_task.init(tenant_id_,
            sync_scn, task_op, ls_attr.get_ls_id(), ls_attr.get_ls_id(),
            ls_attr.get_ls_group_id()))) {
      LOG_WARN("failed to init ls balance task", KR(ret), K(ls_attr), K(sync_scn));
    } else if (OB_FAIL(ObBalanceTaskHelperTableOperator::insert_ls_balance_task(ls_balance_task, trans))) {
      LOG_WARN("failed to insert ls balance task", KR(ret), K(ls_balance_task));
    }
  }
  return ret;
}

int ObRecoveryLSService::create_new_ls_(const share::ObLSAttr &ls_attr,
                                        const SCN &sync_scn,
                                        const ObTenantSwitchoverStatus &switchover_status,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!share::is_ls_create_pre_op(ls_attr.get_ls_operation_type())
      || ! switchover_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_attr), K(switchover_status));
  } else {
    //create new ls;
    DEBUG_SYNC(BEFORE_RECOVER_USER_LS);
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
      ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, tenant_schema, tenant_id_, &trans);
      ObLSFlag ls_flag = ls_attr.get_ls_flag();
      if (OB_FAIL(ObLSServiceHelper::create_new_ls_in_trans(ls_attr.get_ls_id(),
              ls_attr.get_ls_group_id(), ls_attr.get_create_scn(),
              switchover_status, tenant_stat, trans, ls_flag, OB_INVALID_TENANT_ID/*source_tenant_id*/))) {
        LOG_WARN("failed to add new ls status info", KR(ret), K(ls_attr), K(sync_scn),
            K(tenant_stat), K(switchover_status));
      }
    }
    LOG_INFO("[LS_RECOVERY] create new ls", KR(ret), K(ls_attr));
  }
  return ret;
}

int ObRecoveryLSService::report_sys_ls_recovery_stat_(const SCN &sync_scn, const bool only_update_readable_scn,
                                                       const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(proxy_));
  } else if (SCN::base_scn() != sync_scn) {
    ObLSRecoveryStatOperator ls_recovery;
    const uint64_t exec_tenant_id = ls_recovery.get_exec_tenant_id(tenant_id_);
    START_TRANSACTION(proxy_, exec_tenant_id)
    if (FAILEDx(report_sys_ls_recovery_stat_in_trans_(sync_scn,
            only_update_readable_scn, trans, comment))) {
      LOG_WARN("failed to report sys ls recovery stat", KR(ret), K(sync_scn), K(only_update_readable_scn));
    }
    END_TRANSACTION(trans)
  }
  return ret;
}

int ObRecoveryLSService::report_sys_ls_recovery_stat_in_trans_(
    const share::SCN &sync_scn, const bool only_update_readable_scn, common::ObMySQLTransaction &trans,
    const char* comment)
{
  int ret = OB_SUCCESS;
  TIMEGUARD_INIT(RECOVERY_LS, 100_ms, 10_s);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(proxy_));
  } else if (OB_UNLIKELY(!sync_scn.is_valid())
             || OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sync_scn), KP(comment));
  } else if (SCN::base_scn() != sync_scn) {
    ObLSRecoveryStatOperator ls_recovery;
    ObLSRecoveryStat ls_recovery_stat;
    ObAllTenantInfo tenant_info;
    rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
    //two thread for seed log and recovery_ls_manager
    if (OB_ISNULL(tenant_info_loader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl pointer is null", KR(ret), KP(tenant_info_loader));
    } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
      LOG_WARN("get_tenant_info failed", K(ret));
    } else if (tenant_info.is_primary()) {
      ret = OB_ITER_STOP;
      LOG_WARN("is primary cluster, no need report", KR(ret), K(tenant_info));
    } else if (OB_FAIL(construct_sys_ls_recovery_stat_based_on_sync_scn_(sync_scn, ls_recovery_stat,
        tenant_info))) {
      LOG_WARN("failed to construct ls recovery stat", KR(ret), K(sync_scn), K(tenant_info));
    }
    CLICK();

    if (FAILEDx(ObLSRecoveryReportor::update_sys_ls_recovery_stat_and_tenant_info(
            ls_recovery_stat, tenant_info.get_tenant_role(), only_update_readable_scn, trans))) {
      LOG_WARN("failed to update sys ls recovery stat", KR(ret),
          K(ls_recovery_stat), K(tenant_info));
    } else {
      last_report_ts_ = ObTimeUtility::current_time();
      if (!only_update_readable_scn && sync_scn >= restore_status_.sync_scn_) {
        //如果汇报了sync_scn，需要把restore_status重置掉
        restore_status_.reset();
      }
    }
    CLICK();
    FLOG_INFO("report sys ls recovery stat", KR(ret), K(sync_scn), K(only_update_readable_scn),
        K(tenant_info), K(ls_recovery_stat), K(comment));
  }
  return ret;
}

int ObRecoveryLSService::process_ls_transfer_task_in_trans_(
    const transaction::ObTxBufferNode &node, const share::SCN &sync_scn,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!node.is_valid() || !sync_scn.is_valid()
        || ObTxDataSourceType::TRANSFER_TASK != node.get_data_source_type()
        || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sync_scn), K(node), "trans_start", trans.is_started());
  } else {
    ObBalanceTaskHelperMeta task_meta;
    ObBalanceTaskHelper ls_balance_task;
    int64_t pos = 0;
    if (OB_FAIL(task_meta.deserialize(node.get_data_buf().ptr(), node.get_data_buf().length(), pos))) {
      LOG_WARN("failed to deserialize", KR(ret), K(node));
    } else if (OB_UNLIKELY(pos > node.get_data_buf().length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to deserialize balance task", KR(ret), K(pos), K(node));
    } else {
      LOG_INFO("get new balance task", K(task_meta));
      const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
      if (FAILEDx(ls_balance_task.init(sync_scn, tenant_id_, task_meta))) {
        LOG_WARN("failed to init ls balance task", KR(ret), K(task_meta), K(sync_scn));
      } else if (OB_FAIL(ObBalanceTaskHelperTableOperator::insert_ls_balance_task(ls_balance_task, trans))) {
        LOG_WARN("failed to insert ls balance task", KR(ret), K(ls_balance_task));
      }
    }
  }
  return ret;
}

//balance ls group and balance primary zone
int ObRecoveryLSService::do_standby_balance_()
{
  int ret = OB_SUCCESS;
  ObTenantSchema tenant_schema;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(proxy_));
  } else if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
  } else {
    ObTenantLSInfo tenant_info(proxy_, &tenant_schema, tenant_id_);
    bool is_balanced = false;
    bool need_execute_balance = true;
    if (OB_FAIL(ObLSServiceHelper::balance_ls_group(need_execute_balance, tenant_info, is_balanced))) {
      LOG_WARN("failed to balance ls group", KR(ret));
    }
  }
  return ret;
}

int ObRecoveryLSService::do_ls_balance_task_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBalanceTaskHelper> ls_balance_tasks;
  ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(proxy_));
  } else if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl pointer is null", KR(ret), KP(tenant_info_loader));
  } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
    LOG_WARN("get_tenant_info failed", K(ret));
  } else if (OB_FAIL(ObBalanceTaskHelperTableOperator::load_tasks_order_by_scn(
          tenant_id_, *proxy_, tenant_info.get_standby_scn(),
          ls_balance_tasks))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("no need process balance task", K(tenant_info));
    } else {
      LOG_WARN("failed to load task", KR(ret), K(tenant_id_), K(tenant_info));
    }
  } else {
    //使用接口保证获取到的ls_balance_task都是可读点越过，可以处理的task,task是按照顺序删除的，
    //这个是必须要保证的
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_balance_tasks.count() && !has_set_stop(); ++i) {
      const ObBalanceTaskHelper &ls_balance_task = ls_balance_tasks.at(i);
      //按顺序梳理task，不能如果一个task处理失败就失败掉
      if (OB_FAIL(try_do_ls_balance_task_(ls_balance_task, tenant_info))) {
        LOG_WARN("failed to ls balance task", KR(ret), K(ls_balance_task), K(tenant_info));
      }
    }
  }
  return ret;
}

int ObRecoveryLSService::try_do_ls_balance_task_(
    const share::ObBalanceTaskHelper &ls_balance_task,
    const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!ls_balance_task.is_valid() || !tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_balance_task), K(tenant_info));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql is null", KR(ret), KP(proxy_));
  } else {
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    bool can_remove = false;
    START_TRANSACTION(proxy_, exec_tenant_id)
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to start trans", KR(ret));
    } else if (ls_balance_task.get_task_op().is_ls_alter()) {
      can_remove = true;
      if (OB_FAIL(do_ls_balance_alter_task_(ls_balance_task, trans))) {
        LOG_WARN("failed to do ls alter task", KR(ret), K(ls_balance_task));
      }
    } else if (ls_balance_task.get_task_op().is_transfer_end()) {
      can_remove = true;
      bool is_replay_finish = true;
      if (OB_FAIL(ObLSServiceHelper::check_transfer_task_replay(
              tenant_id_, ls_balance_task.get_src_ls(),
              ls_balance_task.get_dest_ls(), ls_balance_task.get_operation_scn(),
              is_replay_finish))) {
        LOG_WARN("failed to check transfer task replay", KR(ret), K(tenant_id_),
            K(ls_balance_task), K(tenant_info));
      } else if (!is_replay_finish) {
        ret = OB_NEED_RETRY;
        LOG_WARN("can not remove ls balance task helper", KR(ret), K(ls_balance_task));
      }
    } else if (ls_balance_task.get_task_op().is_transfer_begin()) {
      if (OB_FAIL(check_transfer_begin_can_remove_(ls_balance_task, tenant_info, can_remove))) {
        LOG_WARN("failed to check transfer begin can remove", KR(ret),
            K(ls_balance_task), K(tenant_info));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls balance task op is unexpected", KR(ret), K(ls_balance_task));
    }
    if (OB_FAIL(ret)) {
    } else if (!can_remove) {
      LOG_INFO("balance task helper can not remove", K(ls_balance_task), K(tenant_info));
    } else if (OB_FAIL(ObBalanceTaskHelperTableOperator::remove_task(tenant_id_,
            ls_balance_task.get_operation_scn(), trans))) {
      LOG_WARN("failed to remove task", KR(ret), K(tenant_id_), K(ls_balance_task));
    } else {
      LOG_INFO("task can be remove", KR(ret), K(ls_balance_task));
      ROOTSERVICE_EVENT_ADD("standby_tenant", "remove_balance_task",
          K_(tenant_id), "task_type", ls_balance_task.get_task_op(),
          "task_scn", ls_balance_task.get_operation_scn(),
          "switchover_status", tenant_info.get_switchover_status(),
          "src_ls", ls_balance_task.get_src_ls(),
          "dest_ls", ls_balance_task.get_dest_ls());
    }
    END_TRANSACTION(trans)
  }
  return ret;
}

int ObRecoveryLSService::check_transfer_begin_can_remove_(
    const share::ObBalanceTaskHelper &ls_balance_task,
    const share::ObAllTenantInfo &tenant_info,
    bool &can_remove)
{
  int ret = OB_SUCCESS;
  can_remove = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!ls_balance_task.is_valid()
        || !ls_balance_task.get_task_op().is_transfer_begin())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_balance_task));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(proxy_));
  } else {
    //find transfer end, or tenant is in flashback
    ObBalanceTaskHelper transfer_end_task;
    SCN transfer_scn;
    bool is_replay_finish = false;
    ret = ObBalanceTaskHelperTableOperator::try_find_transfer_end(tenant_id_,
        ls_balance_task.get_operation_scn(), ls_balance_task.get_src_ls(),
        ls_balance_task.get_dest_ls(), *proxy_, transfer_end_task);
    if (OB_SUCC(ret)) {
      //if has transfer end, can remove transfer begin
      transfer_scn = ls_balance_task.get_operation_scn();
      LOG_INFO("has transfer end task, can remove transfer begin", KR(ret),
          K(ls_balance_task), K(transfer_end_task));
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to find transfer end task", KR(ret), K(tenant_id_), K(ls_balance_task));
    } else if (tenant_info.is_prepare_flashback_for_switch_to_primary_status()
        || tenant_info.is_prepare_flashback_for_failover_to_primary_status()) {
      //check tenant_info status and check wait readable_scn is equal to sync_scn
      ret = OB_SUCCESS;
      transfer_scn = tenant_info.get_sync_scn();
      if (tenant_info.get_sync_scn() != tenant_info.get_standby_scn()) {
        can_remove = false;
        LOG_WARN("There are transfer tasks in progress. Must wait for replay to newest",
KR(ret), K(tenant_id_), K(tenant_info), K(ls_balance_task));
      } else {
        LOG_INFO("replay to newest, can remove transfer begin before switchover/failover to primary",
            K(tenant_info), K(ls_balance_task));
      }
    } else {
      can_remove = false;
      LOG_WARN("can not find transfer end task, can not end transfer begin task", KR(ret), K(tenant_info), K(ls_balance_task));
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret) || !can_remove) {
    } else if (OB_FAIL(ObLSServiceHelper::check_transfer_task_replay(
            tenant_id_, ls_balance_task.get_src_ls(),
            ls_balance_task.get_dest_ls(), transfer_scn, is_replay_finish))) {
      LOG_WARN("failed to check transfer task replay", KR(ret), K(tenant_id_), K(ls_balance_task),
          K(tenant_info), K(transfer_scn));
    } else if (!is_replay_finish) {
      ret = OB_NEED_RETRY;
      LOG_WARN("can not remove ls balance task helper", KR(ret), K(ls_balance_task), K(transfer_scn));
    }

  }
  return ret;
}

int ObRecoveryLSService::do_ls_balance_alter_task_(const share::ObBalanceTaskHelper &ls_balance_task,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!ls_balance_task.is_valid() || !ls_balance_task.get_task_op().is_ls_alter())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_balance_task));
  } else {
    ObLSStatusInfo status_info;
    ObLSStatusOperator status_op;
    ObTenantSchema tenant_schema;
    if (FALSE_IT(ret = status_op.get_ls_status_info(tenant_id_, ls_balance_task.get_src_ls(),
            status_info, trans))) {
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      //ls maybe gc, no need to check
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id_), K(ls_balance_task));
    } else if (status_info.ls_group_id_ == ls_balance_task.get_ls_group_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", KR(ret), K(status_info), K(ls_balance_task));
    } else if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema))) {
      LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
    } else {
      ObTenantLSInfo tenant_info(proxy_, &tenant_schema, tenant_id_);
      if (OB_FAIL(ObLSServiceHelper::process_alter_ls(ls_balance_task.get_src_ls(), status_info.ls_group_id_,
              ls_balance_task.get_ls_group_id(), status_info.unit_group_id_, tenant_info, trans))) {
        LOG_WARN("failed to process alter ls", KR(ret), K(ls_balance_task), K(status_info));
      }
    }
  }
  return ret;
}

int ObRecoveryLSService::init_restore_status(const share::SCN &sync_scn, int err_code)
{
  int ret = OB_SUCCESS;
  if (!sync_scn.is_valid() || OB_SUCCESS == err_code) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KR(err_code), K(sync_scn));
  } else {
    palf::LSN start_lsn(0);
    ObSqlString sync_comment;
    RestoreSyncStatus sync_status;
    ObLSService *ls_svr = MTL(ObLSService *);
    ObLSHandle ls_handle;
    if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, storage::ObLSGetMod::RS_MOD))) {
      LOG_WARN("failed to get ls", KR(ret));
    } else {
      ObLogHandler *log_handler = NULL;
      ObLogRestoreHandler *restore_handler = NULL;
      ObLS *ls = NULL;
      palf::LSN start_lsn;
      if (OB_ISNULL(ls = ls_handle.get_ls()) || OB_ISNULL(log_handler = ls->get_log_handler())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls or log handle is null", KR(ret), KP(ls), KP(log_handler));
      } else if (OB_FAIL(log_handler->locate_by_scn_coarsely(sync_scn, start_lsn))) {
        LOG_WARN("failed to locate lsn", KR(ret), K(sync_scn));
      } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get restore handler", KR(ret), K(sync_scn));
      } else if (OB_FAIL(restore_handler->get_restore_sync_status(err_code,
                                                                  ObLogRestoreErrorContext::ErrorType::FETCH_LOG,
                                                                  sync_status))) {
        LOG_WARN("fail to get error code and message", KR(ret), K(sync_scn), K_(restore_status));
      } else if (OB_FAIL(restore_status_.set(SYS_LS, start_lsn, sync_scn, err_code, sync_status))) {
        LOG_WARN("failed to init restore status", KR(ret), K(start_lsn), K(err_code), K(sync_scn));
      } else {
        LOG_TRACE("init sys restore status success", K(restore_status_));
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
int ObRecoveryLSService::decompress_log_payload_(const char *in_buf, const int64_t in_buf_len,
                                                 char *&decompress_buf, int64_t &decompressed_len)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "RecoveryLS");
  const int64_t decompress_buf_len = palf::MAX_LOG_BODY_SIZE;
  if (NULL == decompress_buf
      && NULL == (decompress_buf = static_cast<char *>(mtl_malloc(decompress_buf_len, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory");
  } else if (OB_FAIL(logservice::decompress(in_buf, in_buf_len, decompress_buf,
                                            decompress_buf_len, decompressed_len))) {
    LOG_WARN("failed to decompress");
  } else {/*do nothing*/}
  return ret;
}
#endif
}//end of namespace rootserver
}//end of namespace oceanbase


