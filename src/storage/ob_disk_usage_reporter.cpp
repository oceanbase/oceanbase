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

#include "storage/ob_disk_usage_reporter.h"

#include "share/ob_disk_usage_table_operator.h"
#include "storage/blocksstable/ob_tmp_file_store.h"
#include "observer/omt/ob_multi_tenant.h"

#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "logservice/ob_log_service.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "lib/function/ob_function.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace logservice;
namespace storage
{

ObDiskUsageReportTask::ObDiskUsageReportTask()
    : is_inited_(false),
      sql_proxy_(NULL)
{
  // do nothing
}

int ObDiskUsageReportTask::init(ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "DiskReport");
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(result_map_.create(OB_MAX_SERVER_TENANT_CNT * 5, SET_USE_500("OB_DISK_REP")))) {
    STORAGE_LOG(WARN, "Failed to create result_map_", K(ret));
  } else if (OB_FAIL(disk_usage_table_operator_.init(sql_proxy))) {
    STORAGE_LOG(WARN, "failed to init disk_usage_table_operator_", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
    STORAGE_LOG(INFO, "ObDistUsageReportTask init successful", K(ret));
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObDiskUsageReportTask::destroy()
{
  result_map_.destroy();
  is_inited_ = false;
}

void ObDiskUsageReportTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  const ObAddr addr = GCTX.self_addr();
  const int64_t self_svr_seq = GCTX.server_id_;
  char addr_buffer[MAX_IP_ADDR_LENGTH] = {};

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDistUsageReportTask not init", K(ret));
  } else if (!addr.ip_to_string(addr_buffer, MAX_IP_ADDR_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get server ip", K(ret), K(addr));
  } else {
     if (OB_FAIL(execute_gc_disk_usage(addr_buffer, addr.get_port(), self_svr_seq))) {
        STORAGE_LOG(WARN, "fail to gc tenant stat", K(ret));
     }
     if (OB_FAIL(report_tenant_disk_usage(addr_buffer, addr.get_port(), self_svr_seq))) {
       STORAGE_LOG(WARN, "Failed to report tenant disk usage", K(ret));
     }
  }

}

/**
int ObDiskUsageReportTask::set_tenant_data_usage(
    const int64_t data_size_before,
    const int64_t used_size_before,
    const int64_t data_size_after,
    const int64_t used_size_after,
    const ObDiskReportFileType file_type)
{
  int ret = OB_SUCCESS;

  ObDiskUsageReportKey report_key;
  TenantUsage tenant_usage;
  report_key.file_type_ = file_type;
  report_key.tenant_id_ = MTL_ID();
  tenant_usage.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDistUsageReportTask hasn't been inited", K(ret));
  } else if (OB_FAIL(tenant_usage_map_.get_refactored(report_key, tenant_usage)) && OB_HASH_NOT_EXIST != ret) {
    STORAGE_LOG(WARN, "Failed to get tenant's disk usage", K(ret), K(report_key), K(tenant_usage));
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    tenant_usage.data_size_ += (data_size_after - data_size_before);
    tenant_usage.used_size_ += (used_size_after - used_size_before);
    if (OB_FAIL(tenant_usage_map_.set_refactored(report_key, tenant_usage, 1))) {
      STORAGE_LOG(WARN, "Failed to update tenant's disk usage", K(ret), K(report_key), K(tenant_usage));
    }
  }

  return ret;
}
*/

int ObDiskUsageReportTask::report_tenant_disk_usage(const char *svr_ip,
                                                    const int32_t svr_port,
                                                    const int64_t seq_num)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(svr_ip) || OB_UNLIKELY(svr_port <= 0 || seq_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(svr_ip), K(svr_port), K(seq_num));
  } else if (OB_FAIL(count_tenant())) {
    STORAGE_LOG(WARN, "failed to count tenant log and meta", K(ret));
  } else if (OB_FAIL(count_tenant_tmp())) {
    STORAGE_LOG(WARN, "failed to count tenant tmp", K(ret));
  }

  // to reduce the locking time of the result_map_,
  // copy the value to array and then update the usage table,
  ObArray<ObDiskUsageReportMap> result_arr;
  ObReportResultGetter copy_result(result_arr);

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(result_map_.foreach_refactored(copy_result))) {
    STORAGE_LOG(WARN, "fail to copy result", K(ret));
  }
  for (int64_t i = 0; i < result_arr.count() && OB_SUCC(ret); ++i) {
    const ObDiskUsageReportMap &pair = result_arr.at(i);
    if (OB_FAIL(disk_usage_table_operator_.update_tenant_space_usage(
        pair.first.tenant_id_, svr_ip, svr_port, seq_num,
        pair.first.file_type_, pair.second.first, pair.second.second))) {
      STORAGE_LOG(WARN, "failed to update disk usage of log and meta", K(ret), K(pair.first));
    }
  }

  return ret;
}


int ObDiskUsageReportTask::count_tenant_data(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObSArray<blocksstable::MacroBlockId> block_list;
  ObDiskUsageReportKey meta_key;
  ObDiskUsageReportKey data_key;
  int64_t meta_size = 0;
  int64_t data_size = 0;
  int64_t occupy_size = 0;

  if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->get_meta_block_list(block_list))) {
    STORAGE_LOG(WARN, "failed to get tenant's meta block list", K(ret));
  } else {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObTenantTabletPtrWithInMemObjIterator tablet_ptr_iter(*t3m);
    ObTabletPointerHandle pointer_handle;
    ObTabletHandle unused_tablet_handle;
    ObTabletMapKey tablet_map_key;
    const ObTabletPointer *tablet_pointer = nullptr;

    while (OB_SUCC(ret) && OB_SUCC(tablet_ptr_iter.get_next_tablet_pointer(tablet_map_key, pointer_handle, unused_tablet_handle))) {
      if (OB_UNLIKELY(!pointer_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected invalid tablet", K(ret), K(pointer_handle));
      } else if (OB_ISNULL(tablet_pointer = static_cast<const ObTabletPointer*>(pointer_handle.get_resource_ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to cast ptr to ObTabletPointer*", K(ret), K(pointer_handle));
      } else {
        ObTabletResidentInfo tablet_info(tablet_map_key, *tablet_pointer);
        occupy_size += tablet_info.get_occupy_size();
        data_size += tablet_info.get_required_size();
        meta_size += tablet_info.get_meta_size();
      }
      pointer_handle.reset();
    }

    if (OB_ITER_END == ret || OB_SUCCESS == ret) {
      ret = OB_SUCCESS;
      meta_size += block_list.count() * OB_DEFAULT_MACRO_BLOCK_SIZE;
    }
  }

  if (OB_SUCC(ret)) {
    meta_key.tenant_id_ = tenant_id;
    meta_key.file_type_ = ObDiskReportFileType::TENANT_META_DATA;
    data_key.tenant_id_ = tenant_id;
    data_key.file_type_ = ObDiskReportFileType::TENANT_DATA;
    if (OB_FAIL(result_map_.set_refactored(meta_key, std::make_pair(meta_size, meta_size), 1 /* whether allowed to override */))) {
      STORAGE_LOG(WARN, "failed to insert meta info result_map_", K(ret), K(meta_key), K(meta_size));
    } else if (OB_FAIL(result_map_.set_refactored(data_key,std::make_pair(occupy_size, data_size), 1 /* whether allowed to override */))) {
      STORAGE_LOG(WARN, "failed to insert data info result_map_", K(ret), K(data_key), K(occupy_size), K(data_size));
    }
  }
  return ret;
}

int ObDiskUsageReportTask::count_tenant()
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, omt is nullptr", K(ret));
  } else if (OB_FAIL(omt->get_mtl_tenant_ids(tenant_ids))) {
    STORAGE_LOG(WARN, "fail to get_mtl_tenant_ids", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); i++) {
    const uint64_t &tenant_id = tenant_ids.at(i);
    MTL_SWITCH(tenant_id) {
      if (OB_FAIL(count_tenant_slog(tenant_id))) {
        STORAGE_LOG(WARN, "failed to count tenant's slog", K(ret));
      } else if (OB_FAIL(count_tenant_clog(tenant_id))) {
        STORAGE_LOG(WARN, "failed to count tenant's clog", K(ret));
      } else if (OB_FAIL(count_tenant_data(tenant_id))) {
        STORAGE_LOG(WARN, "failed to count tenant's data", K(ret));
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        ret = OB_SUCCESS;
        STORAGE_LOG(INFO, "tenant is stopped, ingnore", K(tenant_id));
      } else {
        STORAGE_LOG(WARN, "fail to switch tenant", K(ret), K(tenant_id));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(count_server_slog())) {
    STORAGE_LOG(WARN, "failed to count server's slog", K(ret));
  } else if (OB_FAIL(count_server_clog())) {
    STORAGE_LOG(WARN, "failed to count server's clog", K(ret));
  } else if (OB_FAIL(count_server_meta())) {
    STORAGE_LOG(WARN, "failed to count server's meta", K(ret));
  }

  return ret;
}

int ObDiskUsageReportTask::count_tenant_slog(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObDiskUsageReportKey report_key;
  int64_t slog_space = 0;
  int64_t size_limit = 0;
  ObStorageLogger *slogger = nullptr;

  if (OB_ISNULL(slogger = MTL(ObStorageLogger*))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "slogger is null", K(ret), KP(slogger));
  } else if (OB_FAIL(slogger->get_using_disk_space(slog_space))) {
    STORAGE_LOG(WARN, "failed to get the disk space that slog used", K(ret));
  } else {
    report_key.file_type_ = ObDiskReportFileType::TENANT_SLOG_DATA;
    report_key.tenant_id_ = tenant_id;
    if (OB_FAIL(result_map_.set_refactored(report_key, std::make_pair(slog_space, slog_space), 1))) {
      STORAGE_LOG(WARN, "failed to set result_map_", K(ret), K(report_key), K(slog_space));
    }
  }
  return ret;
}

int ObDiskUsageReportTask::count_tenant_clog(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObDiskUsageReportKey report_key;
  int64_t clog_space = 0;
  int64_t size_limit = 0;
  ObLogService *log_svr = nullptr;

  if (OB_ISNULL(log_svr = MTL(ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "log service is null", K(ret), KP(log_svr));
  } else if (OB_FAIL(log_svr->get_palf_stable_disk_usage(clog_space, size_limit))) {
    STORAGE_LOG(WARN, "failed to get the disk space that clog used", K(ret));
  } else {
    report_key.file_type_ = ObDiskReportFileType::TENANT_CLOG_DATA;
    report_key.tenant_id_ = tenant_id;
    if (OB_FAIL(result_map_.set_refactored(report_key, std::make_pair(clog_space, clog_space), 1))) {
      STORAGE_LOG(WARN, "failed to set result_map_", K(ret), K(report_key), K(clog_space));
    }
  }
  return ret;
}

int ObDiskUsageReportTask::count_server_slog()
{
  int ret = OB_SUCCESS;
  ObDiskUsageReportKey report_key;
  ObStorageLogger *slogger = nullptr;
  int64_t slog_space = 0;
  if (OB_FAIL(SLOGGERMGR.get_server_slogger(slogger))) {
    STORAGE_LOG(WARN, "failed to get server slogger", K(ret));
  } else if (OB_ISNULL(slogger)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, slogger is nullptr", K(ret), KP(slogger));
  } else if (OB_FAIL(slogger->get_using_disk_space(slog_space))) {
    STORAGE_LOG(WARN, "failed to get the disk space that server slog used", K(ret));
  } else {
    report_key.file_type_ = ObDiskReportFileType::TENANT_SLOG_DATA;
    report_key.tenant_id_ = OB_SERVER_TENANT_ID;
    if (OB_FAIL(result_map_.set_refactored(report_key, std::make_pair(slog_space, slog_space), 1))) {
      STORAGE_LOG(WARN, "failed to set result_map_", K(ret), K(report_key), K(slog_space));
    }
  }
  return ret;
}

int ObDiskUsageReportTask::count_server_clog()
{
  int ret = OB_SUCCESS;
  ObDiskUsageReportKey report_key;
  logservice::ObServerLogBlockMgr *log_block_mgr = GCTX.log_block_mgr_;

  int64_t clog_in_use_size_byte = 0;
  int64_t clog_total_size_byte = 0;

  if (OB_ISNULL(log_block_mgr)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(ERROR, "log_block_mgr is null", KR(ret), K(GCTX.log_block_mgr_));
  } else if (OB_FAIL(log_block_mgr->get_disk_usage(clog_in_use_size_byte, clog_total_size_byte))) {
    STORAGE_LOG(ERROR, "Failed to get clog stat ", KR(ret));
  } else {
    report_key.file_type_ = ObDiskReportFileType::TENANT_CLOG_DATA;
    report_key.tenant_id_ = OB_SERVER_TENANT_ID;
    int64_t clog_space = clog_in_use_size_byte;
    if (OB_FAIL(result_map_.set_refactored(report_key, std::make_pair(clog_space, clog_space), 1))) {
      STORAGE_LOG(WARN, "failed to set result_map_", K(ret), K(report_key), K(clog_space));
    }
  }
  return ret;
}

int ObDiskUsageReportTask::count_server_meta()
{
  int ret = OB_SUCCESS;
  common::ObSArray<blocksstable::MacroBlockId> block_list;
  ObDiskUsageReportKey report_key;
  if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().get_meta_block_list(block_list))) {
    STORAGE_LOG(WARN, "failed to get server's meta block list", K(ret));
  } else {
    report_key.tenant_id_ = OB_SERVER_TENANT_ID;
    report_key.file_type_ = ObDiskReportFileType::TENANT_META_DATA;
    int64_t server_meta_size = block_list.count() * common::OB_DEFAULT_MACRO_BLOCK_SIZE;
    if (OB_FAIL(result_map_.set_refactored(report_key, std::make_pair(server_meta_size, server_meta_size), 1))) {
      STORAGE_LOG(WARN, "failed to set result_map_", K(ret), K(report_key), K(block_list.count()));
    }
  }
  return ret;
}

int ObDiskUsageReportTask::count_tenant_tmp()
{
  int ret = OB_SUCCESS;
  ObDiskUsageReportKey report_key;
  int64_t macro_block_cnt = 0;
  common::ObArray<blocksstable::ObTmpFileStore::TenantTmpBlockCntPair> tenant_block_cnt_pairs;
  if (OB_FAIL(OB_TMP_FILE_STORE.get_macro_block_list(tenant_block_cnt_pairs))) {
    STORAGE_LOG(WARN, "failed to get tenant tmp macro block list", K(ret));
  } else {
    report_key.file_type_ = ObDiskReportFileType::TENANT_TMP_DATA;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_block_cnt_pairs.count(); ++i) {
      report_key.tenant_id_ = tenant_block_cnt_pairs.at(i).first;
      macro_block_cnt = tenant_block_cnt_pairs.at(i).second;
      int64_t tenant_tmp_size = macro_block_cnt * common::OB_DEFAULT_MACRO_BLOCK_SIZE;
      if (OB_FAIL(result_map_.set_refactored(report_key, std::make_pair(tenant_tmp_size, tenant_tmp_size), 1))) {
        STORAGE_LOG(WARN, "failed to set tenant tmp usage into result map", K(ret), K(report_key), K(macro_block_cnt));
      }
    }
  }
  return ret;
}


int ObDiskUsageReportTask::delete_tenant_all(const uint64_t tenant_id,
                                             const char *svr_ip,
                                             const int32_t svr_port,
                                             const int64_t seq_num)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDistUsageReportTask not init", K(ret));
  } else if (OB_FAIL(disk_usage_table_operator_.delete_tenant_all(tenant_id,
                                                                  svr_ip,
                                                                  svr_port,
                                                                  seq_num))) {
    STORAGE_LOG(WARN, "failed to delete all tenant rows", K(ret), K(tenant_id), K(svr_ip),
        K(svr_port), K(seq_num));
  }
  return ret;
}

int ObDiskUsageReportTask::execute_gc_disk_usage(const char *svr_ip,
                                                 const int32_t svr_port,
                                                 const int64_t seq_num)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> tenant_ids;

  if (OB_ISNULL(svr_ip) || OB_UNLIKELY(svr_port <= 0 || seq_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(svr_ip), K(svr_port), K(seq_num));
  } else if (OB_FAIL(disk_usage_table_operator_.get_all_tenant_ids(
      svr_ip, svr_port, seq_num, tenant_ids))) {
    STORAGE_LOG(WARN, "failed to get all tenant ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (GCTX.omt_->has_tenant(tenant_id)) {
        continue; // do nothing, just skip;
      } else if (OB_FAIL(delete_tenant_all(tenant_id, svr_ip, svr_port, seq_num))) {
        STORAGE_LOG(WARN, "failed to delete tenant all", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObDiskUsageReportTask::get_data_disk_used_size(const uint64_t tenant_id, int64_t &used_size) const
{
  int ret = OB_SUCCESS;
  used_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDiskUsageReportTask not inited", K(ret));
  } else {
    // the file_type_ of which data is on data disk is needed
    const int need_cnt = 3;
    const ObDiskReportFileType file_types_need[need_cnt] = {
        ObDiskReportFileType::TENANT_DATA,
        ObDiskReportFileType::TENANT_META_DATA,
        ObDiskReportFileType::TENANT_TMP_DATA
    };
    ObDiskUsageReportKey key;
    key.tenant_id_ = tenant_id;
    std::pair<int64_t, int64_t> size = std::make_pair(0, 0);

    for (int64_t i = 0; i < need_cnt && OB_SUCC(ret); i++) {
      key.file_type_ = file_types_need[i];

      if (OB_FAIL(result_map_.get_refactored(key, size)) && OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get file type size", K(ret), K(key));
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        used_size += size.second;
      }
    }
  }

  return ret;
}

int ObDiskUsageReportTask::get_clog_disk_used_size(const uint64_t tenant_id, int64_t &used_size) const
{
  int ret = OB_SUCCESS;
  used_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDiskUsageReportTask not inited", K(ret));
  } else {
    ObDiskUsageReportKey key;
    key.tenant_id_ = tenant_id;
    key.file_type_ = ObDiskReportFileType::TENANT_CLOG_DATA;

    std::pair<int64_t, int64_t> size = std::make_pair(0, 0);
    if (OB_FAIL(result_map_.get_refactored(key, size)) && OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get file type size", K(ret), K(key));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      used_size = size.second;
    }
  }
  return ret;
}

int ObDiskUsageReportTask::delete_tenant_usage_stat(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObDiskUsageReportKey key;
  key.tenant_id_ = tenant_id;
  int64_t file_type_num = static_cast<int64_t>(ObDiskReportFileType::TYPE_MAX);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDiskUsageReportTask not inited", K(ret));
  }

  for (int64_t i = 0; i < file_type_num && OB_SUCC(ret); i++) {
    key.file_type_ = static_cast<ObDiskReportFileType>(i);
    if (OB_FAIL(result_map_.erase_refactored(key)) && OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to erase", K(ret), K(key));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
