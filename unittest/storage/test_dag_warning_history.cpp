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

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace lib;
using namespace compaction;

namespace unittest
{
static const int64_t INFO_PAGE_SIZE = (1 << 13); // 8KB
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
  void calc_info_cnt_per_page(ObIDag &dag, int64_t &info_mem, int64_t &info_cnt_per_page);
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

void TestDagWarningHistory::calc_info_cnt_per_page(ObIDag &dag, int64_t &info_mem_size, int64_t &info_cnt_per_page)
{
  int ret = OB_SUCCESS;
  ObDagWarningInfo tmp_info;
  compaction::ObInfoParamBuffer allocator;
  if (OB_FAIL(dag.gene_warning_info(tmp_info, allocator))) {
    COMMON_LOG(WARN, "failed to gene dag warning info", K(ret));
  }
  // every time will contain a header(16B)
  info_mem_size = tmp_info.get_deep_copy_size();
  info_cnt_per_page = (INFO_PAGE_SIZE - sizeof(ObFIFOAllocator::NormalPageHeader))
    / (info_mem_size + sizeof(ObFIFOAllocator::AllocHeader));
  STORAGE_LOG(INFO, "size", K(info_mem_size), K(info_cnt_per_page));
}

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
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START+1, ret_info, allocator);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);

  allocator.reuse();
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, ret_info, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(TRUE, ret_info.dag_ret_ == ObBasicDag::DAG_RET_START);
  STORAGE_LOG(DEBUG, "", K(ret_info));

  char comment[common::OB_DAG_WARNING_INFO_LENGTH];
  memset(comment, '\0', sizeof(comment));
  allocator.reuse();
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, ret_info, allocator);
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
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, ret_info, allocator);
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
  ret = MTL(ObDagWarningHistoryManager *)->get_with_param(ObBasicDag::KEY_START, ret_info, allocator);
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

  int64_t info_cnt_per_page = 0;
  int64_t info_mem_size = 0;
  ObSeqDag dag;
  dag.init();
  dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
  dag.set_dag_ret(ObBasicDag::DAG_RET_START);
  calc_info_cnt_per_page(dag, info_mem_size, info_cnt_per_page);
  ASSERT_TRUE(info_cnt_per_page > 0);

  const int64_t max_cnt = info_cnt_per_page * 3;
  for (int i = 0; i < max_cnt; ++i) {
    ObSeqDag dag;
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  }
  ASSERT_EQ(max_cnt, MTL(ObDagWarningHistoryManager *)->size());

  // after set_max, mgr will gc info util memory usage below GC_LOW_PERCENTAGE * mem_max
  const int64_t new_mem_max = 2 * INFO_PAGE_SIZE;
  ret = MTL(ObDagWarningHistoryManager *)->set_max(new_mem_max);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t new_size = MTL(ObDagWarningHistoryManager *)->size();
  const int64_t gc_cnt = max_cnt - new_size;
  STORAGE_LOG(INFO, "new size", K(new_size));
  ASSERT_TRUE(new_size * info_mem_size  < ObIDiagnoseInfoMgr::GC_LOW_PERCENTAGE * new_mem_max / 100.0);

  ret = MTL(ObDagWarningHistoryManager *)->set_max(3 * INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_size, MTL(ObDagWarningHistoryManager *)->size());

  compaction::ObIDiagnoseInfoMgr::Iterator iterator;
  ret = MTL(ObDagWarningHistoryManager *)->open_iter(iterator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDagWarningInfo read_info;
  char comment[common::OB_DAG_WARNING_INFO_LENGTH];
  int64_t i = gc_cnt;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.get_next(&read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.dag_ret_ == (ObBasicDag::DAG_RET_START + i));
    }
    ++i;
  }
}

TEST_F(TestDagWarningHistory, gc_info)
{
  int ret = OB_SUCCESS;
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ret = MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis", INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t page_cnt = 10;
  ret = MTL(ObDagWarningHistoryManager *)->set_max(page_cnt * INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t hash_value = ObBasicDag::KEY_START;

  ObComplexDag dag(1);
  dag.init();
  dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
  dag.set_dag_ret(ObBasicDag::DAG_RET_START);
  int64_t info_mem_size = 0;
  int64_t info_cnt_per_page = 0;
  calc_info_cnt_per_page(dag, info_mem_size, info_cnt_per_page);
  const int64_t max_cnt = page_cnt * info_cnt_per_page - 1;

  for (int i = 0; i < max_cnt; ++i) {
    ObComplexDag dag(hash_value++);
    dag.init();
    dag.set_dag_status(ObBasicDag::ObDagStatus::DAG_STATUS_ABORT);
    dag.set_dag_ret(ObBasicDag::DAG_RET_START + i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->add_dag_warning_info(&dag));
  }
  ASSERT_TRUE(info_cnt_per_page > 0);
  // 9 page full, 1 page remain one empty space
  ASSERT_EQ(max_cnt, MTL(ObDagWarningHistoryManager *)->size());

  const int64_t delete_cnt = 30;
  for (int i = 0; i < delete_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->delete_info(ObBasicDag::KEY_START + i));
  }
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->gc_info());
  const int64_t cnt_after_gc = MTL(ObDagWarningHistoryManager *)->size();
  ASSERT_TRUE(cnt_after_gc * sizeof(ObDagWarningInfo) <
              page_cnt * INFO_PAGE_SIZE * ObIDiagnoseInfoMgr::GC_HIGH_PERCENTAGE * 1.0);
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
