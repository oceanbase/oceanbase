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

#ifndef OB_SQL_SPM_OB_PLAN_BASELINE_SQL_SERVICE_H_
#define OB_SQL_SPM_OB_PLAN_BASELINE_SQL_SERVICE_H_
#include "sql/spm/ob_spm_define.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLResult;
}
class ObMySQLProxy;
}

namespace sql
{
struct ObImportBaseline;
class ObSpmBaselineLoader;

class ObPlanBaselineSqlService
{
public:
  ObPlanBaselineSqlService()
  : inited_(false), mysql_proxy_(nullptr) {}
  ~ObPlanBaselineSqlService() {}
  int init(ObMySQLProxy *proxy);

  int load_plan_baseline(const uint64_t tenant_id, ObSpmBaselineLoader &baseline_loader);
  int update_baseline_item(ObMySQLTransaction& trans,
                           ObIAllocator& allocator,
                           const uint64_t tenant_id,
                           const ObBaselineKey& key,
                           const ObPlanBaselineItem& baseline_item);
  int update_baseline_info(ObMySQLTransaction& trans,
                           ObIAllocator& allocator,
                           const uint64_t tenant_id,
                           const ObBaselineKey& key);

  int update_baseline_info_gmt_modified(ObMySQLTransaction& trans,
                                        const uint64_t tenant_id,
                                        const ObBaselineKey& key);
  int update_plan_baselines(ObIAllocator& allocator,
                            const uint64_t tenant_id,
                            const ObBaselineKey& key,
                            ObIArray<ObPlanBaselineItem*>& baselines);
  int update_plan_baseline(ObIAllocator& allocator,
                           const uint64_t tenant_id,
                           const ObBaselineKey& key,
                           ObPlanBaselineItem* baseline,
                           bool update_info,
                           int64_t record_type = 0);

  int delete_baseline_item(ObMySQLTransaction& trans,
                           const uint64_t tenant_id,
                           const ObBaselineKey& key,
                           int64_t &baseline_affected);
  
  int delete_baseline_item(ObMySQLTransaction& trans,
                           const uint64_t tenant_id,
                           const ObBaselineKey& key,
                           const uint64_t plan_hash_value,
                           int64_t &baseline_affected);

  int delete_baseline_info(ObMySQLTransaction& trans,
                           const uint64_t tenant_id,
                           const ObBaselineKey& key);

  int batch_delete_plan_baselines(const uint64_t tenant_id, const uint64_t parallel, int64_t &baseline_affected);
  int do_batch_delete(const uint64_t tenant_id, ObSqlString &sql, int64_t &affected_rows);
  int delete_all_plan_baselines(const uint64_t tenant_id, const ObBaselineKey& key, int64_t &baseline_affected);

  int delete_plan_baseline(const uint64_t tenant_id,
                           const ObBaselineKey& key,
                           const uint64_t plan_hash,
                           int64_t &baseline_affected);

  int get_plan_baseline_item(const uint64_t tenant_id,
                             const ObBaselineKey& key,
                             ObIArray<ObPlanBaselineItem>& baselines);

  int fill_plan_baseline_item(common::sqlclient::ObMySQLResult& result, ObPlanBaselineItem& baseline_item);

  int get_plan_baselines(ObPlanCache* lib_cache, ObSpmCacheCtx& spm_ctx, ObBaselineKey& key);

  int get_need_sync_baseline_keys(ObIAllocator& allocator,
                                  const uint64_t tenant_id,
                                  const int64_t last_sync_time,
                                  ObIArray<ObBaselineKey>& keys);

  int sync_baseline_from_table(ObPlanCache* lib_cache,
                               ObSpmCacheCtx& spm_ctx,
                               ObIArray<ObBaselineKey>& keys);

  int alter_plan_baseline(const uint64_t tenant_id,
                          const uint64_t database_id,
                          AlterPlanBaselineArg& arg,
                          int64_t &baseline_affected);

  int spm_configure(const uint64_t tenant_id, const uint64_t database_id, const ObString& param_name, const int64_t& param_value);
  int purge_baselines(const uint64_t tenant_id, const uint64_t current_time, int64_t &baseline_affected);

  static int convert_sql_string(ObIAllocator &allocator,
                                const ObCollationType input_collation,
                                const ObString &input_str,
                                bool truncate_str,
                                ObString &output_str);

  int update_plan_baselines_result(const uint64_t tenant_id,
                                   EvolutionTaskResult& evo_res);

  int update_baseline_item_evolving_result(ObMySQLTransaction& trans,
                                           const uint64_t tenant_id,
                                           const ObBaselineKey& key,
                                           const uint64_t& plan_hash,
                                           const ObEvolutionStat &evo_stat,
                                           int64_t& affected_rows);

  int insert_new_baseline(ObIAllocator& allocator,
                          const uint64_t tenant_id,
                          const ObBaselineKey& key,
                          ObPlanBaselineItem* baseline);

  int insert_new_baseline_item(ObMySQLTransaction& trans,
                               ObIAllocator& allocator,
                               const uint64_t tenant_id,
                               const ObBaselineKey& key,
                               const ObPlanBaselineItem& baseline_item);

  int import_plan_baseline(ObIAllocator& allocator,
                           const uint64_t tenant_id,
                           const uint64_t baseline_db_id,
                           ObIArray<ObImportBaseline*> &baselines);
  int insert_baseline_item(ObMySQLTransaction& trans,
                           ObIAllocator& allocator,
                           const uint64_t tenant_id,
                           const uint64_t baseline_db_id,
                           const ObImportBaseline* baseline);
  int update_baseline_info(ObMySQLTransaction& trans,
                           ObIAllocator& allocator,
                           const uint64_t tenant_id,
                           const uint64_t baseline_db_id,
                           const ObImportBaseline* baseline);

  int batch_record_evolution_result(const uint64_t tenant_id,
                                    ObIArray<EvolutionTaskResult*> &evo_res_array);
  int insert_spm_record(ObMySQLProxy *proxy,
                        const uint64_t tenant_id,
                        const ObBaselineKey& key,
                        int64_t record_type);
  int delete_timeout_record(const uint64_t tenant_id, const uint64_t current_time);

  int update_baseline_item_verify_result(ObMySQLTransaction& trans,
                                         const uint64_t tenant_id,
                                         const ObBaselineKey& key,
                                         const uint64_t& plan_hash,
                                         const ObEvolutionStat &evo_stat,
                                         int64_t& affected_rows);
private:
  static const int64_t max_sql_text_size = 10 * 1024 * 1024; // 10M
  const static char *EMPTY_STR;
  bool inited_;
  char ip_buff_[common::MAX_IP_ADDR_LENGTH] = {'\0'};
  ObMySQLProxy* mysql_proxy_;
};

} // namespace sql end
} // namespace oceanbase end

#endif
