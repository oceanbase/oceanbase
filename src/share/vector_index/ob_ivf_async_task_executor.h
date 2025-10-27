/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_IVF_ASYNC_TASK_EXECUTOR_H_
#define OCEANBASE_SHARE_OB_IVF_ASYNC_TASK_EXECUTOR_H_

#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"

namespace oceanbase
{
namespace share
{
// schedule ivf vector tasks for a ls
class ObIvfAsyncTaskExector final : public ObVecITaskExecutor
{
public:
  ObIvfAsyncTaskExector() : ObVecITaskExecutor(), local_schema_version_(OB_INVALID_VERSION) {}
  virtual ~ObIvfAsyncTaskExector() {}
  int load_task(uint64_t &task_trace_base_num) override;
  int check_and_set_thread_pool() override;
  int check_schema_version_changed(bool &schema_changed);

private:
  static const int64_t DEFAULT_TABLE_ID_ARRAY_SIZE = 128;

  struct ObIvfAuxKey final
  {
  public:
    ObIvfAuxKey() : data_table_id_(OB_INVALID_ID), base_col_id_(OB_INVALID_ID) {}
    ObIvfAuxKey(const uint64_t &data_table_id, const uint64_t base_col_id)
        : data_table_id_(data_table_id), base_col_id_(base_col_id)
    {}
    ~ObIvfAuxKey() = default;
    uint64_t hash() const
    {
      return murmurhash(&data_table_id_, sizeof(data_table_id_), 0)
             + murmurhash(&base_col_id_, sizeof(base_col_id_), 0);
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool is_valid() const
    {
      return data_table_id_ != OB_INVALID_ID && base_col_id_ != OB_INVALID_ID;
    }
    bool operator==(const ObIvfAuxKey &other) const
    {
      return data_table_id_ == other.data_table_id_ && base_col_id_ == other.base_col_id_;
    }
    TO_STRING_KV(K_(data_table_id), K_(base_col_id));

  public:
    uint64_t data_table_id_;
    uint64_t base_col_id_;
  };

  using ObIvfAuxTableInfoMap =
      hash::ObHashMap<ObIvfAuxKey, ObIvfAuxTableInfo, hash::NoPthreadDefendMode>;
  using ObIvfAuxTableInfoEntry = common::hash::HashMapPair<ObIvfAuxKey, ObIvfAuxTableInfo>;
  using IvfCacheMgrEntry = common::hash::HashMapPair<common::ObTabletID, ObIvfCacheMgr *>;

  struct LoadTaskCallback final
  {
  public:
    LoadTaskCallback(ObVecIndexAsyncTaskOption &task_opt, int64_t tenant_id, storage::ObLS &ls,
                     ObVecIndexTaskCtxArray &task_status_array,
                     ObSchemaGetterGuard &schema_guard, uint64_t &task_trace_base_num)
        : task_opt_(task_opt),
          trace_base_num_(0),
          tenant_id_(tenant_id),
          ls_(&ls),
          task_status_array_(task_status_array),
          schema_guard_(schema_guard),
          task_trace_base_num_(task_trace_base_num)
    {}

    ~LoadTaskCallback() = default;
    int operator()(ObIvfAuxTableInfoEntry &entry);
    int operator()(IvfCacheMgrEntry &entry);
    int is_cache_mgr_deprecated(ObIvfCacheMgr &cache_mgr, bool &is_deprecated);
    int is_cache_writable(const ObIvfAuxTableInfo &table_info, int64_t idx, bool &is_writable);
    TO_STRING_KV(K(task_opt_), K(trace_base_num_), K(tenant_id_), KP(ls_));

  public:
    ObVecIndexAsyncTaskOption &task_opt_;
    uint64_t trace_base_num_;
    int64_t tenant_id_;
    storage::ObLS *ls_;
    ObVecIndexTaskCtxArray &task_status_array_;
    ObSchemaGetterGuard &schema_guard_;
    uint64_t &task_trace_base_num_;
  };

  bool check_operation_allow() override;
  int check_has_ivf_index(bool &has_ivf_index);
  int generate_aux_table_info_map(ObIvfAuxTableInfoMap &aux_table_info_map);
  int generate_aux_table_info_map(ObSchemaGetterGuard &schema_guard, const int64_t table_id,
                                  ObIvfAuxTableInfoMap &aux_table_info_map);
  int record_aux_table_info(ObSchemaGetterGuard &schema_guard,
                            const ObTableSchema &index_table_schema,
                            ObIvfAuxTableInfo &aux_table_info);
  int get_tablet_ids_by_ls(const ObTableSchema &index_table_schema,
                           common::ObIArray<ObTabletID> &tablet_id_array);

private:
  int64_t local_schema_version_;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_IVF_ASYNC_TASK_EXECUTOR_H_
