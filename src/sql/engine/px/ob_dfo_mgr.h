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
  static int generate_sched_depth(ObExecContext &ctx, ObDfoMgr &dfo_mgr);
private:
  static int do_generate_sched_depth(ObExecContext &ctx, ObDfoMgr &dfo_mgr, ObDfo &root);
  static int try_set_dfo_block(ObExecContext &exec_ctx, ObDfo &dfo, bool block = true);
  static int try_set_dfo_unblock(ObExecContext &exec_ctx, ObDfo &dfo);
  static bool check_if_need_do_earlier_sched(ObDfo &child);
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
