/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "storage/compaction/ob_compaction_dag_ranker.h"
#include "storage/compaction/ob_tablet_merge_task.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace compaction
{
namespace
{

void add_merge_dag(ObDList<ObIDag> &dag_list,
                   ObTabletMergeDag &dag,
                   const ObMergeType merge_type,
                   const ObDagListIndex list_idx)
{
  dag.get_param().merge_type_ = merge_type;
  dag.set_dag_status(ObIDag::DAG_STATUS_READY);
  dag.set_list_idx(list_idx);
  ASSERT_TRUE(dag_list.add_last(&dag));
}

void clear_dag_list(ObDList<ObIDag> &dag_list)
{
  while (!dag_list.is_empty()) {
    dag_list.remove_first();
  }
}

void test_collect_batch(const ObDagPrio::ObDagPrioEnum priority,
                        const ObDagType::ObDagTypeEnum dag_type)
{
  ObDList<ObIDag> ready_dag_list;
  ObDList<ObIDag> rank_dag_list;
  ObTabletMergeDag dag1(dag_type);
  ObTabletMergeDag dag2(dag_type);
  ObTabletMergeDag dag3(dag_type);
  ObTabletMergeDag *dags[] = {&dag1, &dag2, &dag3};

  for (ObTabletMergeDag *dag : dags) {
    dag->set_dag_status(ObIDag::DAG_STATUS_READY);
    dag->set_list_idx(RANK_DAG_LIST);
    ASSERT_TRUE(rank_dag_list.add_last(dag));
  }

  ObCompactionDagRanker ranker(100, ready_dag_list, rank_dag_list);
  ASSERT_EQ(OB_SUCCESS, ranker.process(priority, ARRAYSIZEOF(dags), ARRAYSIZEOF(dags)));
  ASSERT_EQ(0, rank_dag_list.get_size());
  ASSERT_EQ(ARRAYSIZEOF(dags), ready_dag_list.get_size());

  for (ObTabletMergeDag *dag : dags) {
    ASSERT_EQ(READY_DAG_LIST, dag->get_list_idx());
    ASSERT_EQ(dag, ready_dag_list.remove(dag));
  }
}

TEST(ObCompactionDagRankerTest, high_and_mid_collect_full_batch)
{
  test_collect_batch(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObDagType::DAG_TYPE_MINI_MERGE);
  test_collect_batch(ObDagPrio::DAG_PRIO_COMPACTION_MID, ObDagType::DAG_TYPE_MERGE_EXECUTE);
}

TEST(ObCompactionDagRankerTest, low_adds_one_meta_outside_rank_batch)
{
  ObDList<ObIDag> ready_dag_list;
  ObDList<ObIDag> rank_dag_list;
  ObTabletMergeDag medium_dag1(ObDagType::DAG_TYPE_MAJOR_MERGE);
  ObTabletMergeDag medium_dag2(ObDagType::DAG_TYPE_MAJOR_MERGE);
  ObTabletMergeDag medium_dag3(ObDagType::DAG_TYPE_MAJOR_MERGE);
  ObTabletMergeDag meta_dag(ObDagType::DAG_TYPE_META_MAJOR_MERGE);
  ObTabletMergeDag *medium_dags[] = {&medium_dag1, &medium_dag2, &medium_dag3};

  for (int64_t i = 0; i < ARRAYSIZEOF(medium_dags); ++i) {
    ObTabletMergeDag *dag = medium_dags[i];
    dag->get_param().merge_type_ = MEDIUM_MERGE;
    dag->get_param().merge_version_ = i + 1;
    dag->set_dag_status(ObIDag::DAG_STATUS_READY);
    dag->set_list_idx(RANK_DAG_LIST);
    ASSERT_TRUE(rank_dag_list.add_last(dag));
  }
  meta_dag.get_param().merge_type_ = META_MAJOR_MERGE;
  meta_dag.get_param().merge_version_ = 100;
  meta_dag.set_dag_status(ObIDag::DAG_STATUS_READY);
  meta_dag.set_list_idx(RANK_DAG_LIST);
  ASSERT_TRUE(rank_dag_list.add_last(&meta_dag));

  ObCompactionDagRanker ranker(100, ready_dag_list, rank_dag_list);
  ASSERT_EQ(OB_SUCCESS, ranker.process(ObDagPrio::DAG_PRIO_COMPACTION_LOW, 2, 2));
  ASSERT_EQ(3, ready_dag_list.get_size());
  ASSERT_EQ(1, rank_dag_list.get_size());
  ASSERT_EQ(READY_DAG_LIST, meta_dag.get_list_idx());
  ASSERT_EQ(&medium_dag3, rank_dag_list.get_header()->get_next());

  clear_dag_list(ready_dag_list);
  clear_dag_list(rank_dag_list);
}

TEST(ObCompactionDagRankerTest, low_does_not_add_meta_when_rank_batch_has_meta)
{
  ObDList<ObIDag> ready_dag_list;
  ObDList<ObIDag> rank_dag_list;
  ObTabletMergeDag meta_dag(ObDagType::DAG_TYPE_META_MAJOR_MERGE);
  ObTabletMergeDag medium_dag(ObDagType::DAG_TYPE_MAJOR_MERGE);
  ObTabletMergeDag queued_meta_dag(ObDagType::DAG_TYPE_META_MAJOR_MERGE);
  add_merge_dag(rank_dag_list, meta_dag, META_MAJOR_MERGE, RANK_DAG_LIST);
  add_merge_dag(rank_dag_list, medium_dag, MEDIUM_MERGE, RANK_DAG_LIST);
  add_merge_dag(rank_dag_list, queued_meta_dag, META_MAJOR_MERGE, RANK_DAG_LIST);

  ObCompactionDagRanker ranker(100, ready_dag_list, rank_dag_list);
  ASSERT_EQ(OB_SUCCESS, ranker.process(ObDagPrio::DAG_PRIO_COMPACTION_LOW, 2, 2));
  ASSERT_EQ(2, ready_dag_list.get_size());
  ASSERT_EQ(1, rank_dag_list.get_size());
  ASSERT_EQ(&queued_meta_dag, rank_dag_list.get_header()->get_next());

  clear_dag_list(ready_dag_list);
  clear_dag_list(rank_dag_list);
}

TEST(ObCompactionDagRankerTest, low_adds_at_most_one_meta_to_rank_batch)
{
  ObDList<ObIDag> ready_dag_list;
  ObDList<ObIDag> rank_dag_list;
  ObTabletMergeDag medium_dag1(ObDagType::DAG_TYPE_MAJOR_MERGE);
  ObTabletMergeDag medium_dag2(ObDagType::DAG_TYPE_MAJOR_MERGE);
  ObTabletMergeDag meta_dag1(ObDagType::DAG_TYPE_META_MAJOR_MERGE);
  ObTabletMergeDag meta_dag2(ObDagType::DAG_TYPE_META_MAJOR_MERGE);
  add_merge_dag(rank_dag_list, medium_dag1, MEDIUM_MERGE, RANK_DAG_LIST);
  add_merge_dag(rank_dag_list, medium_dag2, MEDIUM_MERGE, RANK_DAG_LIST);
  add_merge_dag(rank_dag_list, meta_dag1, META_MAJOR_MERGE, RANK_DAG_LIST);
  add_merge_dag(rank_dag_list, meta_dag2, META_MAJOR_MERGE, RANK_DAG_LIST);

  ObCompactionDagRanker ranker(100, ready_dag_list, rank_dag_list);
  ASSERT_EQ(OB_SUCCESS, ranker.process(ObDagPrio::DAG_PRIO_COMPACTION_LOW, 2, 2));
  ASSERT_EQ(3, ready_dag_list.get_size());
  ASSERT_EQ(1, rank_dag_list.get_size());
  ASSERT_EQ(READY_DAG_LIST, meta_dag1.get_list_idx());
  ASSERT_EQ(RANK_DAG_LIST, meta_dag2.get_list_idx());
  ASSERT_EQ(&meta_dag2, rank_dag_list.get_header()->get_next());

  clear_dag_list(ready_dag_list);
  clear_dag_list(rank_dag_list);
}

} // namespace
} // namespace compaction
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
