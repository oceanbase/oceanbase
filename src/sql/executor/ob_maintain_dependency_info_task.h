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

#ifndef OCEANBASE_SQL_EXECUTOR_MAINTAIN_DEPENDENCY_INFO_TASK_
#define OCEANBASE_SQL_EXECUTOR_MAINTAIN_DEPENDENCY_INFO_TASK_

#include "lib/container/ob_fixed_array.h"
#include "lib/thread/ob_async_task_queue.h"
#include "lib/hash/ob_hashset.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
namespace oceanbase
{

namespace observer
{
class ObGlobalContext;
}

namespace sql
{
class ObMaintainObjDepInfoTask : public share::ObAsyncTask
{
public:
  typedef share::schema::ObReferenceObjTable::DependencyObjKeyItemPair DepObjKeyItem;
  typedef share::schema::ObReferenceObjTable::DependencyObjKeyItemPairs DepObjKeyItemList;
  ObMaintainObjDepInfoTask (
    const uint64_t tenant_id);
  ObMaintainObjDepInfoTask (
    uint64_t tenant_id,
    obrpc::ObCommonRpcProxy &rs_rpc_proxy,
    const DepObjKeyItemList &insert_dep_objs,
    const DepObjKeyItemList &update_dep_objs,
    const DepObjKeyItemList &delete_dep_objs);
  virtual ~ObMaintainObjDepInfoTask()
  {
    insert_dep_objs_.destroy();
    update_dep_objs_.destroy();
    delete_dep_objs_.destroy();
  }
  DepObjKeyItemList& get_insert_dep_objs() { return insert_dep_objs_; }
  DepObjKeyItemList& get_update_dep_objs() { return update_dep_objs_; }
  DepObjKeyItemList& get_delete_dep_objs() { return delete_dep_objs_; }
  bool is_empty_task() const
  { return !reset_view_column_infos_ && (insert_dep_objs_.empty() && update_dep_objs_.empty() && delete_dep_objs_.empty() && !view_schema_.is_valid()); }
  // int check_and_refresh_schema(uint64_t effective_tenant_id);
  int check_cur_maintain_task_is_valid(
      const share::schema::ObReferenceObjTable::ObDependencyObjKey &dep_obj_key,
      int64_t dep_obj_schema_version,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &is_valid);
  int check_and_build_dep_info_arg(
      share::schema::ObSchemaGetterGuard &schema_guard,
      obrpc::ObDependencyObjDDLArg &dep_obj_info_arg,
      const common::ObIArray<DepObjKeyItem> &dep_objs,
      share::schema::ObReferenceObjTable::ObSchemaRefObjOp op);

  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(ObMaintainObjDepInfoTask); }
  ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  int assign_view_schema(const share::schema::ObTableSchema &view_schema);
  share::schema::ObTableSchema &get_view_schema() { return view_schema_; }
  void set_reset_view_column_infos(bool flag) { reset_view_column_infos_ = flag; }
  bool reset_view_column_infos() const { return reset_view_column_infos_; }

private:
  uint64_t tenant_id_;
  const observer::ObGlobalContext &gctx_;
  obrpc::ObCommonRpcProxy &rs_rpc_proxy_;
  DepObjKeyItemList insert_dep_objs_;
  DepObjKeyItemList update_dep_objs_;
  DepObjKeyItemList delete_dep_objs_;
  ObArenaAllocator alloc_;
  share::schema::ObTableSchema view_schema_;
  bool reset_view_column_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObMaintainObjDepInfoTask);
};

class ObMaintainDepInfoTaskQueue: public share::ObAsyncTaskQueue
{
public:
  static const int64_t INIT_BKT_SIZE = 512;
  static const int64_t MAX_SYS_VIEW_SIZE = 65536;
  constexpr static const double MAX_QUEUE_USAGE_RATIO = 0.8;
  ObMaintainDepInfoTaskQueue() : last_execute_time_(0) {}
  virtual ~ObMaintainDepInfoTaskQueue()
  {
    view_info_set_.destroy();
    sys_view_consistent_.destroy();
  }
  int init(const int64_t thread_cnt, const int64_t queue_size);
  virtual void run2() override;
  inline int64_t get_last_execute_time() const { return last_execute_time_; }
  inline void set_last_execute_time(const int64_t execute_time)
  { last_execute_time_ = execute_time; }
  int add_view_id_to_set(const uint64_t view_id) { return view_info_set_.set_refactored(view_id); }
  int erase_view_id_from_set(const uint64_t view_id) { return view_info_set_.erase_refactored(view_id); }
  int add_consistent_sys_view_id_to_set(const uint64_t tenant_id, const uint64_t view_id) { return sys_view_consistent_.set_refactored(std::make_pair(tenant_id, view_id)); }
  int read_consistent_sys_view_from_set(const uint64_t tenant_id, const uint64_t view_id) { return sys_view_consistent_.exist_refactored(std::make_pair(tenant_id, view_id)); }
  bool is_queue_almost_full() const { return queue_.size() > queue_.capacity() * MAX_QUEUE_USAGE_RATIO; }
private:
  int64_t last_execute_time_;
  common::hash::ObHashSet<uint64_t, common::hash::ReadWriteDefendMode> view_info_set_;
  common::hash::ObHashSet<std::pair<uint64_t, uint64_t>, common::hash::ReadWriteDefendMode> sys_view_consistent_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_MAINTAIN_DEPENDENCY_INFO_TASK_ */
