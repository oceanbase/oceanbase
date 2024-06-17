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

#ifndef OCEANBASE_STORAGE_OB_DISK_USAGE_REPORTER_H_
#define OCEANBASE_STORAGE_OB_DISK_USAGE_REPORTER_H_

#include "lib/task/ob_timer.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "share/ob_disk_usage_table_operator.h"
#include "observer/report/ob_i_disk_report.h"
#include "lib/allocator/ob_fifo_allocator.h"

namespace oceanbase
{
namespace common
{
  class ObMySQLProxy;
}

namespace storage
{
class ObTenantTabletIterator;


struct ObDiskUsageReportKey
{
  ObDiskReportFileType file_type_;
  uint64_t tenant_id_;

  uint64_t hash() const
  {
    uint64_t hash_value = static_cast<uint64_t>(file_type_) * 10000L + tenant_id_;
    return hash_value;
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool operator ==(const ObDiskUsageReportKey report_key) const
  {
    return report_key.file_type_ == file_type_ && report_key.tenant_id_ == tenant_id_;
  }

  TO_STRING_KV(K_(file_type), K_(tenant_id));
};

typedef hash::HashMapPair<ObDiskUsageReportKey, std::pair<int64_t, int64_t>> ObDiskUsageReportMap;// pair(occupy_size, required_size)

class ObDiskUsageReportTask : public common::ObTimerTask, public observer::ObIDiskReport
{
public:
  ObDiskUsageReportTask();
  virtual ~ObDiskUsageReportTask() {};
  int init(common::ObMySQLProxy &sql_proxy);
  void destroy();
  virtual int delete_tenant_usage_stat(const uint64_t tenant_id) override;

  // get data disk used size of specified tenant_id in current observer
  int get_data_disk_used_size(const uint64_t tenant_id, int64_t &used_size) const;
  int get_clog_disk_used_size(const uint64_t tenant_id, int64_t &used_size) const;

private:
  class ObReportResultGetter final
  {
  public:
    explicit ObReportResultGetter(ObArray<ObDiskUsageReportMap> &result_arr)
      : result_arr_(result_arr)
    {}
    ~ObReportResultGetter() = default;
    int operator()(const ObDiskUsageReportMap &pair)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(result_arr_.push_back(pair))) {
        STORAGE_LOG(WARN, "failed to push back pair", K(ret));
      }
      return ret;
    }
  private:
    ObArray<ObDiskUsageReportMap> &result_arr_;
    DISALLOW_COPY_AND_ASSIGN(ObReportResultGetter);
  };

private:
  int report_tenant_disk_usage(const char *svr_ip, const int32_t svr_port, const int64_t seq_num);
  int refresh_tenant_disk_usage();

  int count_tenant();
  int count_tenant_slog(const uint64_t tenant_id);
  int count_tenant_clog(const uint64_t tenant_id);
  int count_tenant_data(const uint64_t tenant_id);
  int count_server_slog();
  int count_server_clog();
  int count_server_meta();
  int count_tenant_tmp();

  int delete_tenant_all(const uint64_t tenant_id,
                        const char *svr_ip,
                        const int32_t svr_port,
                        const int64_t seq_num);
  int execute_gc_disk_usage(const char *svr_ip,
                            const int32_t svr_port,
                            const int64_t seq_num);

  virtual void runTimerTask();
  typedef common::hash::ObHashMap<ObDiskUsageReportKey, std::pair<int64_t, int64_t>> ReportResultMap; // pair(occupy_size, required_size)
private:
  bool is_inited_;
  ReportResultMap result_map_;
  common::ObMySQLProxy *sql_proxy_;
  share::ObDiskUsageTableOperator disk_usage_table_operator_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_DISK_USAGE_REPORTER_H_
