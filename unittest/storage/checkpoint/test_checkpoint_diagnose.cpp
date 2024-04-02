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

#include "share/ob_errno.h"
#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public

#define UNITTEST
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{
class TestChekpointDiagnose : public ::testing::Test
{
public:
  TestChekpointDiagnose() {}
  virtual ~TestChekpointDiagnose() = default;
  virtual void SetUp() override {}
  virtual void TearDown() override {}
};

TEST_F(TestChekpointDiagnose, trace_info) {
  ObCheckpointDiagnoseMgr mgr(2);
  mgr.init();
  int64_t trace_id1 = -1;
  int64_t trace_id2 = -1;
  int64_t trace_id3 = -1;
  int64_t trace_id4 = -1;
  share::ObLSID ls_id1(1);
  share::ObLSID ls_id2(1);
  share::ObLSID ls_id3(1);
  share::ObLSID ls_id4(1);
  share::SCN scn1;
  mgr.acquire_trace_id(ls_id1, trace_id1);
  ASSERT_EQ(0, trace_id1);
  ASSERT_EQ(0, mgr.first_pos_);
  ASSERT_EQ(0, mgr.last_pos_);
  ASSERT_EQ(1, mgr.get_trace_info_count());
  ASSERT_EQ(trace_id1, mgr.trace_info_arr_[trace_id1].trace_id_);

  mgr.acquire_trace_id(ls_id2, trace_id2);
  ASSERT_EQ(1, trace_id2);
  ASSERT_EQ(0, mgr.first_pos_);
  ASSERT_EQ(1, mgr.last_pos_);
  ASSERT_EQ(2, mgr.get_trace_info_count());
  ASSERT_EQ(trace_id2, mgr.trace_info_arr_[trace_id2].trace_id_);

  mgr.acquire_trace_id(ls_id3, trace_id3);
  ASSERT_EQ(2, trace_id3);
  ASSERT_EQ(1, mgr.first_pos_);
  ASSERT_EQ(2, mgr.last_pos_);
  ASSERT_EQ(2, mgr.get_trace_info_count());
  ASSERT_EQ(trace_id3, mgr.trace_info_arr_[trace_id3].trace_id_);
  ASSERT_EQ(-1, mgr.trace_info_arr_[0].trace_id_);

  mgr.read_trace_info([&](const ObTraceInfo &trace_info) -> int {
    if (trace_info.trace_id_ == trace_id1) {
      OB_ASSERT(true);
    } else if (trace_info.trace_id_ == trace_id2) {
      OB_ASSERT(trace_info.ls_id_ == ls_id2);
    } else if (trace_info.trace_id_ == trace_id3) {
      OB_ASSERT(trace_info.ls_id_ == ls_id3);
    } else {
      OB_ASSERT(false);
    }
    return OB_SUCCESS;
  });

  mgr.update_max_trace_info_size(1);
  ASSERT_EQ(1, mgr.get_trace_info_count());
  ASSERT_EQ(2, mgr.first_pos_);
  ASSERT_EQ(2, mgr.last_pos_);
  ASSERT_EQ(-1, mgr.trace_info_arr_[0].trace_id_);
  ASSERT_EQ(-1, mgr.trace_info_arr_[1].trace_id_);
  ASSERT_EQ(trace_id3, mgr.trace_info_arr_[2].trace_id_);

  mgr.acquire_trace_id(ls_id4, trace_id4);
  ASSERT_EQ(3, trace_id4);
  ASSERT_EQ(3, mgr.first_pos_);
  ASSERT_EQ(3, mgr.last_pos_);
  ASSERT_EQ(1, mgr.get_trace_info_count());
  ASSERT_EQ(trace_id4, mgr.trace_info_arr_[trace_id4].trace_id_);
  ASSERT_EQ(-1, mgr.trace_info_arr_[0].trace_id_);
  ASSERT_EQ(-1, mgr.trace_info_arr_[1].trace_id_);
  ASSERT_EQ(-1, mgr.trace_info_arr_[2].trace_id_);

}

TEST_F(TestChekpointDiagnose, diagnose_info) {
  int ret = OB_SUCCESS;
  share::SCN scn1;
  scn1.set_base();
  share::SCN scn2 = share::SCN::scn_inc(scn1);
  share::SCN scn3 = share::SCN::scn_inc(scn2);
  ObCheckpointDiagnoseMgr mgr;
  mgr.init();
  int64_t trace_id1 = -1;
  int64_t trace_id2 = -1;
  ObTabletID tablet_id1 = ObTabletID(1);
  ObTabletID tablet_id2 = ObTabletID(2);
  ObTabletID tablet_id3 = ObTabletID(3);
  share::ObLSID ls_id(1);
  void *ptr = (void*)1;
  uint32_t freeze_clock = 1;

  // batch tablet freeze
  ret = mgr.acquire_trace_id(ls_id, trace_id1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ObCheckpointDiagnoseParam param1(trace_id1, tablet_id1, ptr);
  ObCheckpointDiagnoseParam param2(trace_id1, tablet_id2, ptr);

  ret = mgr.add_diagnose_info<ObMemtableDiagnoseInfo>(param1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = mgr.update_freeze_info(param1, scn1, scn1, scn1, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = mgr.update_schedule_dag_time(param1);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = mgr.add_diagnose_info<ObMemtableDiagnoseInfo>(param2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = mgr.update_freeze_info(param2, scn2, scn2, scn2, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = mgr.update_schedule_dag_time(param2);
  ASSERT_EQ(ret, OB_SUCCESS);

  ObMemtableDiagnoseInfo info1;
  ret = mgr.read_diagnose_info<ObMemtableDiagnoseInfo>(trace_id1, [&](const ObTraceInfo &trace_info, const ObCheckpointDiagnoseKey& key, const ObMemtableDiagnoseInfo &info) -> int {
      if (key.tablet_id_ == tablet_id1) {
        OB_ASSERT(trace_info.trace_id_ == trace_id1);
        OB_ASSERT(trace_info.memtable_diagnose_info_map_.size() == 2);
        OB_ASSERT(info.rec_scn_ == scn1);
        OB_ASSERT(info.start_scn_ == scn1);
        OB_ASSERT(info.end_scn_ == scn1);
        OB_ASSERT(info.create_flush_dag_time_ != 0);
      } else if (key.tablet_id_ == tablet_id2) {
        OB_ASSERT(info.rec_scn_ == scn2);
        OB_ASSERT(info.start_scn_ == scn2);
        OB_ASSERT(info.end_scn_ == scn2);
        OB_ASSERT(info.create_flush_dag_time_ != 0);
      } else {
        OB_ASSERT(false);
      }
      return OB_SUCCESS;
  });
  ASSERT_EQ(ret, OB_SUCCESS);

  // logstream freeze
  ret = mgr.acquire_trace_id(ls_id, trace_id2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = mgr.update_freeze_clock(ls_id, trace_id2, freeze_clock);
  ASSERT_EQ(ret, OB_SUCCESS);
  ObCheckpointDiagnoseParam param3(ls_id.id(), freeze_clock - 1, tablet_id3, ptr);
  ret = mgr.add_diagnose_info<ObMemtableDiagnoseInfo>(param3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = mgr.update_freeze_info(param3, scn3, scn3, scn3, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = mgr.update_schedule_dag_time(param3);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = mgr.read_diagnose_info<ObMemtableDiagnoseInfo>(trace_id2, [&](const ObTraceInfo &trace_info, const ObCheckpointDiagnoseKey& key, const ObMemtableDiagnoseInfo &info) -> int {
      if (key.tablet_id_ == tablet_id3) {
        OB_ASSERT(trace_info.trace_id_ == trace_id2);
        OB_ASSERT(trace_info.memtable_diagnose_info_map_.size() == 1);
        OB_ASSERT(info.rec_scn_ == scn3);
        OB_ASSERT(info.start_scn_ == scn3);
        OB_ASSERT(info.end_scn_ == scn3);
        OB_ASSERT(info.create_flush_dag_time_ != 0);
      } else {
        OB_ASSERT(false);
      }
      return OB_SUCCESS;
      });
  ASSERT_EQ(ret, OB_SUCCESS);
}

}
} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f test_checkpoint_diagnose.log*");
  OB_LOGGER.set_file_name("test_checkpoint_diagnose.log", true);
  OB_LOGGER.set_log_level("INFO");
  signal(49, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
