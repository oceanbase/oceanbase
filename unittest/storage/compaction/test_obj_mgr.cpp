/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define private public
#define protected public

#ifdef OB_BUILD_SHARED_STORAGE

#include "storage/compaction/ob_ls_compaction_status.h"
#include "storage/compaction/ob_ls_compaction_list.h"
#include "mtlenv/mock_tenant_module_env.h"
namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace share;

namespace compaction
{
  int ObLSCompactionStatusObj::init(const ObLSID &input_ls_id)
  {
    int ret = OB_SUCCESS;

    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (OB_UNLIKELY(!input_ls_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ls_id_ = input_ls_id;
      state_ = COMPACTION_STATE_IDLE;
      is_inited_ = true;
    }
    return ret;
  }

  int ObLSCompactionListObj::init(const share::ObLSID &input_ls_id)
  {
    int ret = OB_SUCCESS;

    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (OB_UNLIKELY(!input_ls_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      (void) skip_merge_tablets_.create(DEFAULT_BUCKET_CNT, lib::ObMemAttr(MTL_ID(), "SkipMrgTblMap"));
      ls_id_ = input_ls_id;
      compaction_scn_ = ObBasicMergeScheduler::INIT_COMPACTION_SCN;
      is_inited_ = true;
    }
    return ret;
  }
}


namespace unittest
{
class TestObjMgr : public ::testing::Test
{
public:
  TestObjMgr() : tenant_base_(tenant_id) {}
  void SetUp()
  {
    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
    ASSERT_EQ(tenant_id, MTL_ID());
  }
  void TearDown()
  {
    ObTenantEnv::set_tenant(nullptr);
  }
  const uint64_t tenant_id = 1;
  ObTenantBase tenant_base_;
};

TEST_F(TestObjMgr, test_tablet_compaction_state_op)
{
  int ret = OB_SUCCESS;
  ObSvrLSCompactionStatusObj svr_ls_status_obj;
  const int64_t svr_id = 1;
  const ObLSID ls_id = ObLSID(1001);
  ASSERT_EQ(OB_SUCCESS, svr_ls_status_obj.init(svr_id, ls_id, OBJ_EXEC_MODE_WRITE_ONLY));

  // insert one & select
  ObTabletID tablet_id(100);
  ObTabletCompactionState state;
  state.compaction_scn_ = 100;
  ASSERT_EQ(OB_SUCCESS, svr_ls_status_obj.update_tablet_state(tablet_id, state));

  ObTabletCompactionState tmp_state;
  ASSERT_EQ(OB_SUCCESS, svr_ls_status_obj.get_tablet_state(tablet_id, tmp_state));
  ASSERT_EQ(tmp_state, state);

  // update with smaller merged_scn
  state.compaction_scn_ = 50;
  ASSERT_EQ(OB_EAGAIN, svr_ls_status_obj.update_tablet_state(tablet_id, state));
}

TEST_F(TestObjMgr, test_ls_compaction_status_serialize)
{
  ObSvrLSCompactionStatusObj svr_ls_status_obj;
  const int64_t svr_id = 1;
  const ObLSID ls_id = ObLSID(1001);
  ASSERT_EQ(OB_SUCCESS, svr_ls_status_obj.init(svr_id, ls_id, OBJ_EXEC_MODE_WRITE_ONLY));

  const int64_t CNT = 1000;
  ObTabletID tablet_id(100);
  ObTabletCompactionState state;
  state.compaction_scn_ = 100;
  for (int64_t idx = 0; idx < CNT; ++idx) {
    state.compaction_scn_ = ObTimeUtility::fast_current_time() + idx;
    ASSERT_EQ(OB_SUCCESS, svr_ls_status_obj.update_tablet_state(ObTabletID(idx + 1), state));
  }
  for (int64_t idx = 0; idx < CNT; ++idx) {
    ASSERT_EQ(OB_SUCCESS, svr_ls_status_obj.get_tablet_state(ObTabletID(idx + 1), state));
  }
  ASSERT_EQ(CNT, svr_ls_status_obj.tablet_info_map_.size());
  const int64_t BUF_LEN = 2 * 1024 * 1024;
  char buf[BUF_LEN];
  MEMSET(buf, '\0', sizeof(char) * BUF_LEN);
  int64_t write_pos = 0;
  ASSERT_EQ(OB_SUCCESS, svr_ls_status_obj.serialize(buf, BUF_LEN, write_pos));
  ASSERT_EQ(write_pos, svr_ls_status_obj.get_serialize_size());
  COMMON_LOG(INFO, "serialize len", K(write_pos));

  ObSvrLSCompactionStatusObj tmp_obj;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_obj.deserialize(buf, write_pos, read_pos));
  ASSERT_EQ(tmp_obj, svr_ls_status_obj);
  ASSERT_EQ(write_pos, read_pos);

  ObArenaAllocator allocator;
  ObVirtualTableInfo info;
  info.init(allocator);
  svr_ls_status_obj.fill_info(info);
  COMMON_LOG(INFO, "print info", K(info));
}

TEST_F(TestObjMgr, test_ls_compaction_list)
{
  // init
  ObLSCompactionListObj compaction_list_obj;
  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.init(ObLSID(1001)));

  ObTabletID tablet_id = ObTabletID(200001);
  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.add_skip_tablet(100, tablet_id));
  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.add_skip_tablet(100, tablet_id));
  EXPECT_EQ(1, compaction_list_obj.skip_merge_tablets_.size());

  bool need_skip = false;
  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.tablet_need_skip(100, tablet_id, need_skip));
  EXPECT_EQ(true, need_skip);

  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.tablet_need_skip(100, ObTabletID(200003), need_skip));
  EXPECT_EQ(false, need_skip);

  ObTabletID tablet_id2 = ObTabletID(200002);
  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.add_skip_tablet(100, tablet_id2));
  EXPECT_EQ(2, compaction_list_obj.skip_merge_tablets_.size());

  EXPECT_EQ(OB_INVALID_ARGUMENT, compaction_list_obj.add_skip_tablet(50, tablet_id));

  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.add_skip_tablet(200, tablet_id));
  EXPECT_EQ(1, compaction_list_obj.skip_merge_tablets_.size());

  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.tablet_need_skip(100, tablet_id, need_skip));
  EXPECT_EQ(false, need_skip);
}

TEST_F(TestObjMgr, test_ls_compaction_list_serialize)
{
  // init
  ObLSCompactionListObj compaction_list_obj;
  const ObLSID ls_id = ObLSID(1001);
  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.init(ObLSID(1001)));

  // prepare skip merge info
  const int64_t CNT = 1000;
  int64_t merge_version = 100;
  for (int64_t idx = 0; idx < CNT; ++idx) {
    ObTabletID tablet_id = ObTabletID(200001 + idx);
    EXPECT_EQ(OB_SUCCESS, compaction_list_obj.add_skip_tablet(merge_version, tablet_id));

    bool need_skip = false;
    EXPECT_EQ(OB_SUCCESS, compaction_list_obj.tablet_need_skip(merge_version, tablet_id, need_skip));
    EXPECT_EQ(true, need_skip);
  }
  ASSERT_EQ(CNT, compaction_list_obj.skip_merge_tablets_.size());

  // serialize
  const int64_t BUF_LEN = 2_MB;
  char buf[BUF_LEN];
  MEMSET(buf, '\0', sizeof(char) * BUF_LEN);
  int64_t write_pos = 0;
  EXPECT_EQ(OB_SUCCESS, compaction_list_obj.serialize(buf, BUF_LEN, write_pos));
  EXPECT_EQ(write_pos, compaction_list_obj.get_serialize_size());
  COMMON_LOG(INFO, "serialize len", K(write_pos));

  // deserialize
  ObLSCompactionListObj read_obj;
  int64_t read_pos = 0;
  EXPECT_EQ(OB_SUCCESS, read_obj.deserialize(buf, write_pos, read_pos));
  ASSERT_EQ(CNT, read_obj.skip_merge_tablets_.size());
  for (int64_t idx = 0; idx < CNT; ++idx) {
    bool need_skip = false;
    EXPECT_EQ(OB_SUCCESS, read_obj.tablet_need_skip(merge_version, ObTabletID(200001 + idx), need_skip));
    EXPECT_EQ(true, need_skip);
  }
}

TEST_F(TestObjMgr, test_ls_compaction_list_obj_cache)
{
  hash::ObHashMap<ObLSID, ObLSCompactionListObj *> obj_cache;
  EXPECT_EQ(OB_SUCCESS, obj_cache.create(5, lib::ObMemAttr(1, "MrgListCache")));
  ObLSCompactionListObj* ls_obj_ptrs[5] = { nullptr };

  // prepare skip merge info
  const int64_t CNT = 1000;
  int64_t merge_version = 100;
  ObArray<ObTabletID> skip_merge_tablets;
  for (int64_t idx = 0; idx < CNT; ++idx) {
    EXPECT_EQ(OB_SUCCESS, skip_merge_tablets.push_back(ObTabletID(300001 + idx)));
  }

  ObArenaAllocator arena;
  // prepare list obj
  for (int64_t idx = 0; idx < 5; ++idx) {
    ObLSID ls_id = ObLSID(1001 + idx);

    void *buf = nullptr;
    buf = arena.alloc(sizeof(ObLSCompactionListObj));
    ASSERT_TRUE(nullptr != buf);
    ASSERT_TRUE(0 != arena.total());

    ObLSCompactionListObj *obj_ptr = new (buf) ObLSCompactionListObj();
    EXPECT_EQ(OB_SUCCESS, obj_ptr->init(ls_id));

    for (int64_t idx = 0; idx < CNT; ++idx) {
      ObTabletID tablet_id = ObTabletID(200001 + idx);
      EXPECT_EQ(OB_SUCCESS, obj_ptr->add_skip_tablet(merge_version, tablet_id));
    }
    EXPECT_EQ(OB_SUCCESS, obj_cache.set_refactored(ls_id, obj_ptr));
    ls_obj_ptrs[idx] = obj_ptr;
  }

  // check ls obj ptr before destroy
  for (int64_t idx = 0; idx < 5; ++idx) {
    ObLSCompactionListObj *ptr = ls_obj_ptrs[idx];
    EXPECT_EQ(true, ptr->skip_merge_tablets_.size() > 0);
  }

  // destroy
  hash::ObHashMap<share::ObLSID, ObLSCompactionListObj*>::iterator iter = obj_cache.begin();
  for ( ; iter != obj_cache.end(); ++iter) {
    ObLSCompactionListObj *obj_ptr = iter->second;
    if (OB_NOT_NULL(obj_ptr)) {
      obj_ptr->~ObLSCompactionListObj();
      arena.free(obj_ptr);
      obj_ptr = nullptr;
    }
  }

  for (int64_t idx = 0; idx < 5; ++idx) {
    ObLSCompactionListObj *ptr = ls_obj_ptrs[idx];
    EXPECT_EQ(false, ptr->is_inited_);
    EXPECT_EQ(false, ptr->skip_merge_tablets_.created());
  }

  obj_cache.destroy();
  arena.clear();
  EXPECT_EQ(0, arena.total());
}


} //end namespace unittest
} //end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_obj_mgr.log*");
  OB_LOGGER.set_file_name("test_obj_mgr.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif