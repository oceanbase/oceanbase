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

#ifndef __OCEANBASE_SQL_ENGINE_PX_DFO_MGR_H__
#define __OCEANBASE_SQL_ENGINE_PX_DFO_MGR_H__

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/px/ob_dfo.h"

namespace oceanbase
{
namespace sql
{
class ObPxCoordInfo;
class ObDfoMgr
{
public:
  explicit ObDfoMgr(common::ObIAllocator &allocator) :
      allocator_(allocator), inited_(false),
      root_dfo_(NULL)
  {}
  virtual ~ObDfoMgr() = default;
  void destroy();
  void reset();
  int init(ObExecContext &exec_ctx,
           const ObOpSpec &root_op_spec,
           const ObDfoInterruptIdGen &dfo_int_gen,
           ObPxCoordInfo &px_coord_info);
  ObDfo *get_root_dfo() { return root_dfo_; }
  
  virtual int get_ready_dfo(ObDfo *&dfo) const; // 仅用于单层dfo调度
  // 可以入选即将调度队列的 DFO
  virtual int get_ready_dfos(common::ObIArray<ObDfo *> &dfos) const;
  // 已经入选即将调度队列的 DFO
  virtual int get_active_dfos(common::ObIArray<ObDfo *> &dfos) const;
  // 已经调度的 DFO
  virtual int get_scheduled_dfos(ObIArray<ObDfo*> &dfos) const;
  // 已经调度，且没有执行完成的 DFO
  virtual int get_running_dfos(ObIArray<ObDfo*> &dfos) const;

  int add_dfo_edge(ObDfo *edge);
  int find_dfo_edge(int64_t id, ObDfo *&edge);
  const ObIArray<ObDfo *> &get_all_dfos() { return edges_; }
  ObIArray<ObDfo *> &get_all_dfos_for_update() { return edges_; }

  DECLARE_TO_STRING;
private:
  int do_split(ObExecContext &exec_ctx,
               common::ObIAllocator &allocator,
               const ObOpSpec *phy_op,
               ObDfo *&parent_dfo,
               const ObDfoInterruptIdGen &dfo_id_gen,
               ObPxCoordInfo &px_coord_info) const;
  int create_dfo(common::ObIAllocator &allocator,
                 const ObOpSpec *dfo_root_op,
                 ObDfo *&dfo) const;

  inline int schedule_child_parent(ObIArray<ObDfo *> &dfos, ObDfo *child) const;
protected:
  common::ObIAllocator &allocator_;
  bool inited_;
  ObDfo *root_dfo_;
  common::ObSEArray<ObDfo *, 2> edges_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDfoMgr);
};

class ObDfoSchedOrderGenerator
{
public:
  static int generate_sched_order(ObDfoMgr &dfo_mgr);
private:
  static int do_generate_sched_order(ObDfoMgr &dfo_mgr, ObDfo &root);
};

class ObDfoSchedDepthGenerator
{
public:
  static int generate_sched_depth(ObExecContext &ctx, ObDfoMgr &dfo_mgr, const int64_t pipeline_depth);
private:
  static int do_generate_sched_depth(ObExecContext &ctx, ObDfoMgr &dfo_mgr, ObDfo &root, const int64_t pipeline_depth);
  static int try_set_dfo_block(ObExecContext &exec_ctx, ObDfo &dfo, bool block = true);
  static int try_set_dfo_unblock(ObExecContext &exec_ctx, ObDfo &dfo);
  static bool check_if_need_do_earlier_sched(ObDfo &child, const int64_t pipeline_depth);
  static int bypass_material(ObExecContext &exec_ctx, const ObOpSpec *phy_op, bool bypass);
};

class ObDfoSchedSimulator
{
  enum class ObDfoState
  {
    WAIT,
    BLOCK,
    RUNNING,
    FINISH,
    FAIL
  };
  using ObDfoAssignGetter = std::function<int64_t(const ObDfo &)>;
  using ObDfoStateMap = common::hash::ObHashMap<int64_t, ObDfoState>;

public:
  ObDfoSchedSimulator(const ObIArray<ObDfo *> &dfos) :
    dfos_(dfos), dfo_state_map_(), dfo_start_ts_map_(), ts_worker_cnt_map_(), dfo_assign_getter_(),
    inited_(false), collect_maximum_worker_(false), max_assigned_(0)
  {}

  ~ObDfoSchedSimulator() = default;

public:
  int create(const ObDfoAssignGetter &dfo_assign_getter, bool collect_maximum_worker);

  int destroy();

  int schedule();

  int get_max_assigned_worker(int64_t &max_assigned) const;

  int get_max_concurrent_workers(const ObDfo *dfo, int64_t &max_concurrent_worker) const;

public:
  // different Getter of DFO's worker count
  struct ObMinimalCountGetter
  {
    int64_t operator()(const ObDfo &dfo) const
    {
      UNUSED(dfo);
      return 1;
    }
  };

  struct ObFinalCountGetter
  {
    int64_t operator()(const ObDfo &dfo) const;
  };

  struct ObDopCountGetter
  {
    int64_t operator()(const ObDfo &dfo) const;
  };

private:
  bool is_inited() const
  {
    return inited_;
  }
  int all_child_dfo_finish(const ObDfo *dfo, bool &all_finish);

  int schedule_dfo(const ObDfo *dfo, int64_t &assigned, int64_t &max_assigned);

  int finish_dfo(const ObDfo *dfo, int64_t &assigned);

  int schedule_ancester_dfos(const ObDfo *dfo, int64_t &assigned, int64_t &max_assigned);

  int schedule_sibling_dfos(const ObDfo *dfo, int64_t &assigned, int64_t &max_assigned);

private:
  const ObIArray<ObDfo *> &dfos_;
  ObDfoStateMap dfo_state_map_;
  common::hash::ObHashMap<int64_t, int64_t> dfo_start_ts_map_;
  common::hash::ObHashMap<int64_t, int64_t> dfo_max_wokrer_cnt_map_;
  ObSEArray<int64_t, 16> ts_worker_cnt_map_;
  ObDfoAssignGetter dfo_assign_getter_;
  bool inited_;
  bool collect_maximum_worker_;
  int64_t max_assigned_;
};

class ObDfoWorkerAssignment
{
public:
  static int assign_worker(ObDfoMgr &dfo_mgr,
                           int64_t expected_worker_count,
                           int64_t minimal_worker_count,
                           int64_t admited_worker_count);
  static int get_dfos_worker_count(const ObIArray<ObDfo*> &dfos,
                                   const bool get_minimal,
                                   int64_t &total_assigned);
  static int calc_admited_worker_count(const ObIArray<ObDfo*> &dfos,
                                       ObExecContext &exec_ctx,
                                       const ObOpSpec &root_op_spec,
                                       int64_t &px_expected,
                                       int64_t &px_minimal,
                                       int64_t &px_admited);
};

}
}
#endif /* __OCEANBASE_SQL_ENGINE_PX_DFO_MGR_H__ */
