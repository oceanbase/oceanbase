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
#define USING_LOG_PREFIX SHARE
#include "ob_storage_io_usage_reporter.h"
#include "ob_storage_io_usage_proxy.h"
#include "observer/ob_server.h"


namespace oceanbase {
using namespace common;
using namespace share;

int ObStorageIOUsageRepoter::mtl_init(ObStorageIOUsageRepoter *&reporter)
{
    int ret = OB_SUCCESS;

    if (is_meta_tenant(MTL_ID())) {
        // do nothing
    } else if (OB_FAIL(reporter->init())) {
        LOG_ERROR("StorageIOUsageRepoter init failed", K(ret));
    }

    return ret;
}

int ObStorageIOUsageRepoter::init()
{
    int ret = OB_SUCCESS;

    if (is_meta_tenant(MTL_ID())) {
        // do nothing
    } else if (OB_UNLIKELY(is_inited_)) {
        ret = OB_INIT_TWICE;
        LOG_ERROR("IOUsageRepoter has been inited", K(ret));
    } else if (OB_FAIL(io_usage_report_task_.init(this))) {
        LOG_ERROR("IOUsageReportedTask init failed", K(ret));
    } else {
        is_inited_ = true;
        LOG_INFO("IOUsageRepoter inited success");
    }

    return ret;
}

int ObStorageIOUsageRepoter::start()
{
    int ret = OB_SUCCESS;
    if (is_meta_tenant(MTL_ID())) {
        // do nothing
    } else if (!is_inited_) {
        ret = OB_NOT_INIT;
        LOG_ERROR("IOUsageRepoter has not been inited", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), io_usage_report_task_, TenantIOUsageReportTask::SLEEP_SECONDS, true))) {
        LOG_WARN("schedule report_io_usage_task failed", K(ret));
    }

    return ret;
}

void ObStorageIOUsageRepoter::stop()
{
    if (is_meta_tenant(MTL_ID())) {
        // do nothing
    } else if (is_inited_) {
        TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), io_usage_report_task_);
    }
}

void ObStorageIOUsageRepoter::wait()
{
    if (is_meta_tenant(MTL_ID())) {
        // do nothing
    } else if (is_inited_) {
        TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), io_usage_report_task_);
    }
}

void ObStorageIOUsageRepoter::destroy()
{
    int ret = OB_SUCCESS;
    if (is_meta_tenant(MTL_ID())) {
        // do nothing
    } else if (is_inited_) {
        if (OB_FAIL(cancel_report_task())) {
            LOG_WARN("failed to cancel io usage report task", K(ret));
        } else {
            is_inited_ = false;
        }
    }
}

int ObStorageIOUsageRepoter::cancel_report_task()
{
    int ret = OB_SUCCESS;
    bool is_exist = true;
    if (OB_FAIL(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), io_usage_report_task_, is_exist))) {
        LOG_WARN("failed to check io usage report task", K(ret));
    } else if (is_exist) {
        if (OB_FAIL(TG_CANCEL_R(MTL(omt::ObSharedTimer*)->get_tg_id(), io_usage_report_task_))) {
        LOG_WARN("failed to cancel io usage report task", K(ret));
        }
    }
    return ret;
}

int ObStorageIOUsageRepoter::TenantIOUsageReportTask::init(ObStorageIOUsageRepoter *reporter)
{
    int ret = OB_SUCCESS;

    if (OB_ISNULL(reporter)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("reporter is null", K(ret), KP(reporter));
    } else {
        reporter_ = reporter;
    }

    return ret;
}

void ObStorageIOUsageRepoter::TenantIOUsageReportTask::runTimerTask()
{
    int ret = OB_SUCCESS;
    const uint64_t tenant_id = MTL_ID();

    if (OB_FAIL(reporter_->report_tenant_io_usage(tenant_id))) {
        LOG_WARN("update storage_io_usage failed", K(ret), K(tenant_id));
    }
}

using namespace obrpc;
int ObStorageIOUsageRepoter::UpdateIOUsageFunctor::
        operator()
        (common::hash::HashMapPair<oceanbase::common::ObTrafficControl::ObIORecordKey,
         common::ObTrafficControl::ObSharedDeviceIORecord> &entry)
{
    int ret = OB_SUCCESS;

    if (entry.first.tenant_id_ == tenant_id_) {
        uint64_t tmp_storage_id = OB_INVALID_ID;
        uint64_t tmp_dest_id = OB_INVALID_ID;
        const ObStorageInfoType table_type = entry.first.id_.get_category();
        ObString storage_mod_str;
        // get storage id and storage str
        if (table_type == ObStorageInfoType::ALL_ZONE_STORAGE) {
            tmp_storage_id = entry.first.id_.get_storage_id();
            storage_mod_str = ObStorageIOUsageRepoter::get_storage_mod_str(table_type);
        } else if (table_type == ObStorageInfoType::ALL_BACKUP_STORAGE_INFO) {
            tmp_dest_id = entry.first.id_.get_storage_id();
            storage_mod_str = ObStorageIOUsageRepoter::get_storage_mod_str(table_type);
        }
        // update io usage table
        ObMySQLTransaction trans;
        ObStorageIOUsageProxy io_usage_proxy;
        ResourceUsage usages[ResourceType::ResourceTypeCnt];
        entry.second.reset_total_size(usages);

        for (int64_t i = 0; i < ResourceType::ResourceTypeCnt; ++i) {
            if (usages[i].type_ >= ResourceType::ResourceTypeCnt ||
                (ResourceType::iops == usages[i].type_ || ResourceType::iobw == usages[i].type_)) {
              // skip
            } else if (usages[i].total_ <= 0 ) {
              // skip
            } else if (OB_FAIL(io_usage_proxy.update_storage_io_usage(trans,
                                                                      tenant_id_,
                                                                      tmp_storage_id,
                                                                      tmp_dest_id,
                                                                      storage_mod_str,
                                                                      ObStorageIOUsageRepoter::get_type_str(usages[i].type_),
                                                                      usages[i].total_))) {
                LOG_WARN("update_storage_io_usage failed", K(ret), K(tenant_id_));
            }
            usleep(100);
        }
    }

    return ret;
}

int ObStorageIOUsageRepoter::report_tenant_io_usage(const uint64_t tenant_id)
{
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(tenant_id));
    } else {
        // for each tenant and meta tenant to report io usage
        UpdateIOUsageFunctor fn(tenant_id);
        OB_IO_MANAGER.get_tc().foreach_record(fn);
        if (is_sys_tenant(tenant_id)) {
            // do nothing
        } else {
            UpdateIOUsageFunctor meta_fn(gen_meta_tenant_id(tenant_id));
            OB_IO_MANAGER.get_tc().foreach_record(meta_fn);
        }
    }

    return ret;
}

const ObString &ObStorageIOUsageRepoter::get_storage_mod_str(const ObStorageInfoType table_type)
{
    static ObString storage_mod_str[] {
        ObString("CLOG/DATA"),
        ObString("BACKUP/ARCHIVE/RESTORE"),
        ObString("UNKNOWN")
    };
    if (table_type >= ObStorageInfoType::ALL_RESTORE_INFO) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected table type", K(table_type));
    }

    return storage_mod_str[uint8_t(table_type)];
}

// NOTE: not support tag now
const ObString &ObStorageIOUsageRepoter::get_type_str(const ResourceType resource_type)
{
    static ObString type_str[] {
        ObString("READ"),
        ObString("WRITE"),
        ObString("READ_BYTES"),
        ObString("WRITE_BYTES"),
        ObString("TAG"),
        ObString("UNKNOWN")
    };
    const int64_t unknown_idx = 5;

    int64_t idx = unknown_idx; // unknown;
    switch (resource_type) {
        case ResourceType::ips:
            idx = 0;
            break;
        case ResourceType::ops:
            idx = 1;
            break;
        case ResourceType::ibw:
            idx = 2;
            break;
        case ResourceType::obw:
            idx = 3;
            break;
        case ResourceType::tag:
            idx = 4;
            break;
        default:
            idx = unknown_idx;
    }

    return type_str[idx];
}

}