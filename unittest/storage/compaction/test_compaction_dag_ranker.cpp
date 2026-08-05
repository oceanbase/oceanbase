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

} // namespace
} // namespace compaction
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
