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

#ifndef OCEANBASE_LIB_OB_STORAGE_IO_USAGE_REPORTER_H
#define OCEANBASE_LIB_OB_STORAGE_IO_USAGE_REPORTER_H

#include "lib/ob_define.h"
// #include "share/ob_thread_pool.h"
#include "share/ob_thread_mgr.h"
#include "common/storage/ob_device_common.h"
#include "observer/net/ob_shared_storage_net_throt_rpc_struct.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObString;
class ObMySQLProxy;
}
namespace share
{

class ObStorageIOUsageRepoter
{
public:
    class TenantIOUsageReportTask : public common::ObTimerTask
    {
    public:
        TenantIOUsageReportTask() : reporter_(nullptr) {}
        virtual ~TenantIOUsageReportTask() {}
        int init(ObStorageIOUsageRepoter *reporter);
        virtual void runTimerTask(void) override;

        static const uint64_t SLEEP_SECONDS = 5 * 1000L * 1000L; // 5s
        static const uint64_t BOOTSTRAP_PERIOD = 20 * 1000L * 1000L; //5s
    private:
        DISALLOW_COPY_AND_ASSIGN(TenantIOUsageReportTask);
        ObStorageIOUsageRepoter *reporter_;
    };
private:
    class UpdateIOUsageFunctor
    {
    public:
        UpdateIOUsageFunctor(const int64_t tenant_id): tenant_id_(tenant_id) {}
        ~UpdateIOUsageFunctor() {}
        int operator()(common::hash::HashMapPair<oceanbase::common::ObTrafficControl::ObIORecordKey,
                                                  common::ObTrafficControl::ObSharedDeviceIORecord> &entry);
    private:
        int64_t tenant_id_;
    };
public:
    explicit ObStorageIOUsageRepoter() : is_inited_(false),
                                         io_usage_report_task_()
    {}
    ~ObStorageIOUsageRepoter() { destroy(); }
    static int mtl_init(ObStorageIOUsageRepoter *&reporter);
    int init();
    void reset();
    void destroy();
    int start();
    void stop();
    void wait();
    int report_tenant_io_usage(const uint64_t tenant_id);
    int cancel_report_task();
    static const ObString &get_storage_mod_str(const ObStorageInfoType table_type);
    static const ObString &get_type_str(const obrpc::ResourceType resource_type);

    TO_STRING_KV(K_(is_inited));

private:
    bool is_inited_;
    TenantIOUsageReportTask io_usage_report_task_;
};

}
}
#endif