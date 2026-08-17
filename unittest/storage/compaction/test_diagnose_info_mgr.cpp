/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "src/storage/ob_i_store.h"
#include "src/storage/compaction/ob_i_compaction_filter.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace storage;

namespace unittest
{

class TestDiagnoseInfoMgr : public ::testing::Test
{
public:
  TestDiagnoseInfoMgr()
  : ls_id_(TEST_LS_ID),
    tenant_base_(TEST_TENANT_ID),
    suspect_info_mgr_(NULL),
    diagnose_tablet_mgr_(NULL),
    is_inited_(false)
  {}
  ~TestDiagnoseInfoMgr() = default;
  void SetUp();
  void TearDown();
  int gene_suspect_info(
    const ObDiagnoseInfoPrio &prio,
    const ObMergeType &merge_type,
    const ObTabletID &tablet_id,
    ObScheduleSuspectInfo &info);
  static constexpr int64_t TEST_TENANT_ID = 1001;
  static constexpr int64_t TEST_LS_ID = 9001;
  share::ObLSID ls_id_;
  ObTenantBase tenant_base_;
  ObScheduleSuspectInfoMgr *suspect_info_mgr_;
  ObDiagnoseTabletMgr *diagnose_tablet_mgr_;
  ObDiagnoseInfoParam<2, 0> param_;
  bool is_inited_;
};

void TestDiagnoseInfoMgr::SetUp()
{
  if (!is_inited_) {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(TEST_TENANT_ID);
    is_inited_ = true;
  }
  if (OB_ISNULL(suspect_info_mgr_)) {
    suspect_info_mgr_ = OB_NEW(ObScheduleSuspectInfoMgr, ObModIds::TEST);
  }
  tenant_base_.set(suspect_info_mgr_);

  if (OB_ISNULL(diagnose_tablet_mgr_)) {
    diagnose_tablet_mgr_ = OB_NEW(ObDiagnoseTabletMgr, ObModIds::TEST);
  }
  tenant_base_.set(diagnose_tablet_mgr_);

  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

  ObMallocAllocator *ma = ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(TEST_TENANT_ID, 1LL << 30));
}

void TestDiagnoseInfoMgr::TearDown()
{
  if (OB_NOT_NULL(suspect_info_mgr_)) {
    suspect_info_mgr_->destroy();
    suspect_info_mgr_ = nullptr;
  }
  if (OB_NOT_NULL(diagnose_tablet_mgr_)) {
    diagnose_tablet_mgr_->destroy();
    diagnose_tablet_mgr_ = nullptr;
  }
  tenant_base_.destroy();
  ObTenantEnv::set_tenant(nullptr);
}

int TestDiagnoseInfoMgr::gene_suspect_info(
  const ObDiagnoseInfoPrio &prio,
  const ObMergeType &merge_type,
  const ObTabletID &tablet_id,
  ObScheduleSuspectInfo &info)
{
  int ret = OB_SUCCESS;
  info.priority_ = static_cast<uint32_t>(prio);
  info.merge_type_ = merge_type;
  info.ls_id_ = ls_id_;
  info.tablet_id_ = tablet_id;
  info.info_param_ = &param_;
  return ret;
}

bool judge_equal(const ObScheduleSuspectInfo &a, const ObScheduleSuspectInfo &b)
{
  return a.priority_ == b.priority_
    && a.ls_id_ == b.ls_id_
    && a.tablet_id_ == b.tablet_id_
    && a.merge_type_ == b.merge_type_;
}

class InitFailCompactionFilter final : public ObICompactionFilter
{
public:
  InitFailCompactionFilter() = default;
  ~InitFailCompactionFilter() override { ++destructor_count_; }
  int init() { return OB_ERR_UNEXPECTED; }
  int filter(const blocksstable::ObDatumRow &, ObFilterRet &) const override
  {
    return OB_NOT_SUPPORTED;
  }
  CompactionFilterType get_filter_type() const override { return FILTER_TYPE_MAX; }
  static int64_t destructor_count_;
};

int64_t InitFailCompactionFilter::destructor_count_ = 0;

TEST_F(TestDiagnoseInfoMgr, test_filter_factory_destroys_failed_filter)
{
  ObArenaAllocator allocator;
  ObICompactionFilter *filter = nullptr;
  InitFailCompactionFilter::destructor_count_ = 0;

  ASSERT_EQ(OB_ERR_UNEXPECTED,
            ObCompactionFilterFactory::alloc_compaction_filter<InitFailCompactionFilter>(
                allocator, filter));
  ASSERT_EQ(nullptr, filter);
  ASSERT_EQ(1, InitFailCompactionFilter::destructor_count_);
}

TEST_F(TestDiagnoseInfoMgr, test_compaction_dag_snapshot_reuse)
{
  ObCompactionDagSnapshot snapshot;
  const ObTabletID tablet_id(1);
  ASSERT_EQ(OB_SUCCESS, snapshot.init(4, 4));

  ObDiagnoseTabletCompProgress first_progress;
  first_progress.create_time_ = 1;
  ObDagNetProgressSnapshot first_dag_net;
  first_dag_net.start_time_ = 1;
  ASSERT_EQ(OB_SUCCESS, snapshot.put_dag(MINOR_MERGE, ls_id_, tablet_id, first_progress));
  ASSERT_EQ(OB_SUCCESS, snapshot.put_dag_net(ls_id_, tablet_id, first_dag_net));

  const ObDiagnoseTabletCompProgress *progress = nullptr;
  const ObDagNetProgressSnapshot *dag_net = nullptr;
  ASSERT_EQ(OB_SUCCESS, snapshot.get_dag_progress(MINOR_MERGE, ls_id_, tablet_id, progress));
  ASSERT_EQ(1, progress->create_time_);
  ASSERT_EQ(OB_SUCCESS, snapshot.get_dag_net_snapshot(ls_id_, tablet_id, dag_net));
  ASSERT_EQ(1, dag_net->start_time_);

  ASSERT_EQ(OB_SUCCESS, snapshot.reuse());
  ASSERT_EQ(OB_HASH_NOT_EXIST,
      snapshot.get_dag_progress(MINOR_MERGE, ls_id_, tablet_id, progress));
  ASSERT_EQ(OB_HASH_NOT_EXIST, snapshot.get_dag_net_snapshot(ls_id_, tablet_id, dag_net));

  ObDiagnoseTabletCompProgress second_progress;
  second_progress.create_time_ = 2;
  ObDagNetProgressSnapshot second_dag_net;
  second_dag_net.start_time_ = 2;
  ASSERT_EQ(OB_SUCCESS, snapshot.put_dag(MINOR_MERGE, ls_id_, tablet_id, second_progress));
  ASSERT_EQ(OB_SUCCESS, snapshot.put_dag_net(ls_id_, tablet_id, second_dag_net));
  ASSERT_EQ(OB_SUCCESS, snapshot.get_dag_progress(MINOR_MERGE, ls_id_, tablet_id, progress));
  ASSERT_EQ(2, progress->create_time_);
  ASSERT_EQ(OB_SUCCESS, snapshot.get_dag_net_snapshot(ls_id_, tablet_id, dag_net));
  ASSERT_EQ(2, dag_net->start_time_);
}

TEST_F(TestDiagnoseInfoMgr, test_add_del_suspect_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const ObTabletID tablet_id(1);
  ASSERT_EQ(OB_SUCCESS, ObScheduleSuspectInfoMgr::mtl_init(suspect_info_mgr_));

  ObScheduleSuspectInfo info;
  ObScheduleSuspectInfo ret_info;
  ret = gene_suspect_info(ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW, MINOR_MERGE, tablet_id, info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = suspect_info_mgr_->add_suspect_info(info.hash(), info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // high priority could cover low priority
  info.priority_ = static_cast<uint32_t>(ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH);
  ret = suspect_info_mgr_->add_suspect_info(info.hash(), info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = suspect_info_mgr_->get_with_param(info.hash(), ret_info, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, judge_equal(info, ret_info));

  // mid priority couldn't cover high priority
  info.priority_ = static_cast<uint32_t>(ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_MID);
  ret = suspect_info_mgr_->add_suspect_info(info.hash(), info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = suspect_info_mgr_->get_with_param(info.hash(), ret_info, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  // still be high priority
  ASSERT_EQ(static_cast<uint32_t>(ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_HIGH), ret_info.priority_);
}

TEST_F(TestDiagnoseInfoMgr, test_generic_schedule_failure_does_not_cover_specific_failure)
{
  ObArenaAllocator allocator;
  const ObTabletID tablet_id(1);
  ASSERT_EQ(OB_SUCCESS, ObScheduleSuspectInfoMgr::mtl_init(suspect_info_mgr_));

  ObDiagnoseInfoParam<4, 0> specific_param;
  specific_param.struct_type_ = ObInfoParamStructType::SUSPECT_INFO_PARAM;
  specific_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_SCHEDULE_MEDIUM_FAILED;
  ObScheduleSuspectInfo specific_info;
  ASSERT_EQ(OB_SUCCESS,
            gene_suspect_info(
                OB_SUSPECT_INFO_TYPES[ObSuspectInfoType::SUSPECT_SCHEDULE_MEDIUM_FAILED].priority,
                MEDIUM_MERGE,
                tablet_id,
                specific_info));
  specific_info.info_param_ = &specific_param;
  ASSERT_EQ(OB_SUCCESS, suspect_info_mgr_->add_suspect_info(specific_info.hash(), specific_info));

  ObDiagnoseInfoParam<1, 0> generic_param;
  generic_param.struct_type_ = ObInfoParamStructType::SUSPECT_INFO_PARAM;
  generic_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_SCHEDULE_NEW_ROUND_FAILED;
  ObScheduleSuspectInfo generic_info;
  ASSERT_EQ(OB_SUCCESS,
            gene_suspect_info(
                OB_SUSPECT_INFO_TYPES[ObSuspectInfoType::SUSPECT_SCHEDULE_NEW_ROUND_FAILED].priority,
                MEDIUM_MERGE,
                tablet_id,
                generic_info));
  generic_info.info_param_ = &generic_param;
  ASSERT_LT(generic_info.priority_, specific_info.priority_);
  ASSERT_EQ(OB_SUCCESS, suspect_info_mgr_->add_suspect_info(generic_info.hash(), generic_info));

  ObScheduleSuspectInfo ret_info;
  ASSERT_EQ(OB_SUCCESS, suspect_info_mgr_->get_with_param(specific_info.hash(), ret_info, allocator));
  ASSERT_EQ(ObSuspectInfoType::SUSPECT_SCHEDULE_MEDIUM_FAILED,
            ret_info.info_param_->type_.suspect_type_);

  // The generic fallback is still recorded when there is no specific failure for the key.
  const ObTabletID fallback_only_tablet_id(2);
  generic_info.tablet_id_ = fallback_only_tablet_id;
  ASSERT_EQ(OB_SUCCESS, suspect_info_mgr_->add_suspect_info(generic_info.hash(), generic_info));
  ObScheduleSuspectInfo fallback_ret_info;
  ASSERT_EQ(OB_SUCCESS,
            suspect_info_mgr_->get_with_param(generic_info.hash(), fallback_ret_info, allocator));
  ASSERT_EQ(static_cast<uint32_t>(ObDiagnoseInfoPrio::DIAGNOSE_PRIORITY_LOW),
            fallback_ret_info.priority_);
  ASSERT_EQ(ObSuspectInfoType::SUSPECT_SCHEDULE_NEW_ROUND_FAILED,
            fallback_ret_info.info_param_->type_.suspect_type_);
}

TEST_F(TestDiagnoseInfoMgr, test_diagnose_tablet_mgr)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(1);
  ASSERT_EQ(OB_SUCCESS, ObDiagnoseTabletMgr::mtl_init(diagnose_tablet_mgr_));

  ret = diagnose_tablet_mgr_->add_diagnose_tablet(ls_id_, tablet_id, TYPE_DIAGNOSE_TABLET_MAX);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = diagnose_tablet_mgr_->add_diagnose_tablet(ls_id_, tablet_id, TYPE_MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = diagnose_tablet_mgr_->add_diagnose_tablet(ls_id_, tablet_id, TYPE_MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  // same diagnose type is registed, return success anyway
  ret = diagnose_tablet_mgr_->add_diagnose_tablet(ls_id_, tablet_id, TYPE_MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);

  // same diagnose type is registed, return success anyway
  ret = diagnose_tablet_mgr_->add_diagnose_tablet(ls_id_, tablet_id, TYPE_RS_MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = diagnose_tablet_mgr_->delete_diagnose_tablet(ls_id_, tablet_id, TYPE_MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, diagnose_tablet_mgr_->diagnose_tablet_map_.size());
  // after remove all flag, registed tablet is deleted
  ret = diagnose_tablet_mgr_->delete_diagnose_tablet(ls_id_, tablet_id, TYPE_RS_MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, diagnose_tablet_mgr_->diagnose_tablet_map_.size());
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_diagnose_info_mgr.log*");
  OB_LOGGER.set_file_name("test_diagnose_info_mgr.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
