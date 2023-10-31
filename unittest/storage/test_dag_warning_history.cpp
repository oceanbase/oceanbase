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

#include <gtest/gtest.h>

#define private public
#define protected public

#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_sstable_struct.h"
#include "observer/omt/ob_tenant_node_balancer.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace lib;

namespace unittest
{
static const int64_t INFO_PAGE_SIZE = (1 << 12); // 4KB
class TestDagWarningHistory : public ::testing::Test
{
public:
  TestDagWarningHistory()
    : tenant_id_(1001),
      dag_history_mgr_(nullptr),
      tenant_base_(1001),
      inited_(false)
  { }
  ~TestDagWarningHistory() {}
  void SetUp()
  {
    if (!inited_) {
      ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id_);
      ObMallocAllocator::get_instance()->set_tenant_limit(tenant_id_, 1LL << 30);
      inited_ = true;
    }
    ObUnitInfoGetter::ObTenantConfig unit_config;
    unit_config.mode_ = lib::Worker::CompatMode::MYSQL;
    unit_config.tenant_id_ = 0;
    TenantUnits units;
    ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));

    dag_history_mgr_ = OB_NEW(ObDagWarningHistoryManager, ObModIds::TEST);
    tenant_base_.set(dag_history_mgr_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

  }
  void TearDown()
  {
    dag_history_mgr_->~ObDagWarningHistoryManager();
    dag_history_mgr_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
private:
  const uint64_t tenant_id_;
  ObDagWarningHistoryManager *dag_history_mgr_;
  ObTenantBase tenant_base_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(TestDagWarningHistory);
};

class ObBasicDag : public ObIDag
{
public:
  ObBasicDag() :
    ObIDag(ObDagType::DAG_TYPE_TABLET_BACKFILL_TX)
  {}
  void init() { is_inited_ = true; }
  virtual int64_t hash() const {
    return KEY_START;
  }
  virtual bool operator == (const ObIDag &other) const
  {
    bool bret = false;
    if (get_type() == other.get_type()) {
      const ObBasicDag &dag = static_cast<const ObBasicDag &>(other);
      bret = dag.id_ == id_;
    }
    return bret;
  }
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), 1, 2, "table_id", 10))) {
      COMMON_LOG(WARN, "fail to add dag warning info param", K(ret));
    }
    return ret;
  }
  virtual int fill_dag_key(char *buf,const int64_t size) const override { UNUSEDx(buf, size); return OB_SUCCESS; }
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(type), K(task_list_.get_size()), K_(dag_ret));

private:
  DISALLOW_COPY_AND_ASSIGN(ObBasicDag);

public:
  static const int64_t KEY_START = 8888;
  static const int DAG_RET_START = -4016;
};

class ObSeqDag : public ObBasicDag
{
public:
  ObSeqDag() :
    ObBasicDag()
  {}
  virtual int64_t hash() const {
    static int64_t key = KEY_START;
    return key++;
  }
};

class ObComplexDag : public ObBasicDag
{
public:
  ObComplexDag(int64_t hash) :
    ObBasicDag()
  {
    hash_ = hash;
  }
  virtual int64_t hash() const {
    return hash_;
  }
  int64_t hash_;
};

TEST_F(TestDagWarningHistory, simple_add)
{
  int ret = OB_SUCCESS;
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);

  ObBasicDag dag;
  dag.init();
  dag.set_dag_ret(ObBasicDag::DAG_RET_START);
  dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
  STORAGE_LOG(DEBUG, "hash print", K(dag.hash()));

  //not init
  ret = MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag);
  ASSERT_EQ(OB_SUCCESS, ret);

  compaction::ObInfoParamBuffer allocator;
  ObDagWarningInfo ret_info;
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START+1, &ret_info, allocator);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);

  allocator.reuse();
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, &ret_info, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(TRUE, ret_info.dag_ret_ == ObBasicDag::DAG_RET_START);
  STORAGE_LOG(DEBUG, "", K(ret_info));
  ASSERT_EQ(TRUE, ret_info.tenant_id_ == tenant_id_);

  char comment[common::OB_DAG_WARNING_INFO_LENGTH];
  memset(comment, '\0', sizeof(comment));
  allocator.reuse();
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, &ret_info, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ (TRUE, ret_info.dag_ret_ == ObBasicDag::DAG_RET_START);
  memset(comment, '\0', sizeof(comment));
  ASSERT_EQ(OB_SUCCESS, ret_info.info_param_->fill_comment(comment, sizeof(comment)));
  STORAGE_LOG(DEBUG, "comment", K(comment));
}

TEST_F(TestDagWarningHistory, simple_del)
{
  int ret = OB_SUCCESS;

  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ret = MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBasicDag dag;
  dag.init();
  dag.set_dag_ret(ObBasicDag::DAG_RET_START);
  dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  ASSERT_EQ(OB_HASH_NOT_EXIST, MTL(ObDagWarningHistoryManager *)->delete_info(ObBasicDag::KEY_START + 1));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->delete_info(ObBasicDag::KEY_START));
  ASSERT_EQ(1, MTL(ObDagWarningHistoryManager *)->size());

  compaction::ObInfoParamBuffer allocator;
  ObDagWarningInfo ret_info;
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, &ret_info, allocator);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);

  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  compaction::ObIDiagnoseInfoMgr::Iterator iterator;
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->delete_info(ObBasicDag::KEY_START));
  // delete_info only delete info from map, there is still 2 info in list
  ASSERT_EQ(2, MTL(ObDagWarningHistoryManager *)->size());
  ASSERT_EQ(OB_ITER_END, iterator.get_next(&ret_info, nullptr, 0));

  allocator.reuse();
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, &ret_info, allocator);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);
}

TEST_F(TestDagWarningHistory, simple_loop_get)
{
  int ret = OB_SUCCESS;

  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ret = MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t max_cnt = 4000;
  for (int i = 0; i < max_cnt; ++i) {
    ObSeqDag dag;
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  }

  ASSERT_EQ(max_cnt, MTL(ObDagWarningHistoryManager *)->size());

  compaction::ObIDiagnoseInfoMgr::Iterator iterator;
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDagWarningInfo read_info;
  char comment[common::OB_DAG_WARNING_INFO_LENGTH];
  int i = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.get_next(&read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i));
      ++i;
    }
  }

  int64_t key = 0;
  const int64_t del_cnt = 3000;
  for (int i = 0; i < del_cnt; i += 1) {
    key = ObBasicDag::KEY_START + i;
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->delete_info(key));
  }

  iterator.reset();
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator);
  ASSERT_EQ(OB_SUCCESS, ret);
  i = del_cnt;
  while (OB_SUCC(ret)) {
    if (OB_SUCC(iterator.get_next(&read_info, nullptr, 0))) {
      ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i));
      i += 1;
    }
  }

  MTL(ObDagWarningHistoryManager *)->clear();
  ASSERT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

TEST_F(TestDagWarningHistory, resize)
{
  int ret = OB_SUCCESS;

  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ret = MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t max_cnt = 40;
  for (int i = 0; i < max_cnt; ++i) {
    ObSeqDag dag;
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  }

  ASSERT_EQ(max_cnt, MTL(ObDagWarningHistoryManager *)->size());

  // every info is 264 bytes, each page contains 13 info
  // 40 infos are in 4 pages (13 13 13 1), set_max will left 12 info (12 * 264 < 4096 * 0.4), means 2 page (11 1)
  ret = MTL(ObDagWarningHistoryManager *)->set_max(2 * INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, MTL(ObDagWarningHistoryManager *)->size());

  ret = MTL(ObDagWarningHistoryManager *)->set_max(3 * INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, MTL(ObDagWarningHistoryManager *)->size());

  compaction::ObIDiagnoseInfoMgr::Iterator iterator;
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDagWarningInfo read_info;
  char comment[common::OB_DAG_WARNING_INFO_LENGTH];
  int i = 28;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.get_next(&read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i));
      ++i;
    }
  }
}

TEST_F(TestDagWarningHistory, gc_info)
{
  int ret = OB_SUCCESS;
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ret = MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObDagWarningHistoryManager *)->set_max(10 * INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t hash_value = ObBasicDag::KEY_START;
  const int64_t max_cnt = 129;
  for (int i = 0; i < max_cnt; ++i) {
    ObComplexDag dag(hash_value++);
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  }
  // 9 page full, 1 page contain 12 info (13 13 13 13 13 13 13 13 13 12)
  ASSERT_EQ(max_cnt, MTL(ObDagWarningHistoryManager *)->size());

  for (int i = 0; i < 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->delete_info(ObBasicDag::KEY_START+i));
  }

  // every info is 264 bytes, each page contains 13 info
  // 129 * 264 > 40960 * 0.8 // after gc, left 62 info (62 * 264 < 40960 * 0.4), means 5 pages (11 13 13 13 12)
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->gc_info());
  ASSERT_EQ(62, MTL(ObDagWarningHistoryManager *)->size());
}

TEST_F(TestDagWarningHistory, complex_test)
{
  int ret = OB_SUCCESS;

  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ret = MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObDagWarningHistoryManager *)->set_max(2 * INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t hash_value = ObBasicDag::KEY_START;
  const int64_t max_cnt = 39;
  for (int i = 0; i < max_cnt; ++i) {
    ObComplexDag dag(hash_value++);
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  }
  // every info is 264 bytes, each page contains 13 infos
  // 20 info will be purged , left 2 pages (6 13)
  ASSERT_EQ(19, MTL(ObDagWarningHistoryManager *)->size());

  compaction::ObIDiagnoseInfoMgr::Iterator iterator;
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDagWarningInfo read_info;
  char comment[common::OB_DAG_WARNING_INFO_LENGTH];
  // the first 20 info have been purged
  int i = 20;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.get_next(&read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i));
      ++i;
    }
  }

  // test when two iter is accessing info pool
  compaction::ObIDiagnoseInfoMgr::Iterator iterator1;
  compaction::ObIDiagnoseInfoMgr::Iterator iterator2;
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator2);
  ASSERT_EQ(OB_SUCCESS, ret);

  i = 20;
  ASSERT_EQ(OB_SUCCESS, iterator2.get_next(&read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i++));
  ASSERT_EQ(OB_SUCCESS, iterator2.get_next(&read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i++));

  i = 20;
  ASSERT_EQ(OB_SUCCESS, iterator1.get_next(&read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i++));
  ASSERT_EQ(OB_SUCCESS, iterator1.get_next(&read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i++));
  // let the iterator1 in iter_end
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iterator1.get_next(&read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i));
      ++i;
    }
  }

  // before set_max (6 13), after set_max (6)
  ret = MTL(ObDagWarningHistoryManager *)->set_max(INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, MTL(ObDagWarningHistoryManager *)->size());

  {
    ObComplexDag dag(hash_value++);
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i++);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
    ASSERT_EQ(1, MTL(ObDagWarningHistoryManager *)->size());
  }

  ASSERT_EQ(OB_ITER_END, iterator1.get_next(&read_info, comment, sizeof(comment)));
  ASSERT_EQ(OB_SUCCESS, iterator2.get_next(&read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + max_cnt));

  // test purge cuz add when there are some deleted info on list
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->delete_info(hash_value-1));
  for (int i = 0; i < max_cnt; ++i) {
    ObComplexDag dag(hash_value++);
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  }
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  system("rm -f test_dag_warning_history.log*");
  OB_LOGGER.set_file_name("test_dag_warning_history.log");
  CLOG_LOG(INFO, "begin unittest: test_dag_warning_history");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  return RUN_ALL_TESTS();
}
