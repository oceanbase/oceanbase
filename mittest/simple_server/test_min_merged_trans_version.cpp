/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "env/ob_simple_cluster_test_base.h"
#include "simple_server/compaction_basic_func.h"
#include "unittest/storage/ob_truncate_info_helper.h"
#include "storage/compaction/ob_index_block_micro_iterator.h"
#include "storage/compaction/ob_tenant_status_cache.h"
#include "storage/ob_trans_version_skip_index_util.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace compaction;
using namespace blocksstable;

int64_t MOCK_MIN_DATA_VERSION = DATA_CURRENT_VERSION;

namespace compaction
{
int ObTenantStatusCache::get_min_data_version(uint64_t &min_data_version)
{
  min_data_version = MOCK_MIN_DATA_VERSION;
  FLOG_INFO("MOCK ObTenantStatusCache::get_min_data_version", K(min_data_version));
  return OB_SUCCESS;
}
}
namespace unittest
{
#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
class ObMinMergedTransVersionTest : public ObSimpleClusterTestBase
{
public:
  ObMinMergedTransVersionTest()
    : ObSimpleClusterTestBase("test_min_merged_trans_version"),
      tenant_id_(1),
      allocator_()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql_proxy.write("drop database test", affected_rows);
    sql_proxy.write("create database test", affected_rows);
    sql_proxy.write("use test", affected_rows);
  }
  int check_macro_and_micro(
    const ObTablet &tablet,
    ObSSTable &last_major,
    bool expect_compat,
    const int64_t border_macro_version = INT64_MAX);
  int get_last_major_sstable(
    sqlclient::ObISQLConnection &conn,
    const char *table_name,
    const bool is_index,
    ObTabletHandle &tablet_handle,
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    ObSSTable *&last_major);
  uint64_t tenant_id_;
  ObArenaAllocator allocator_;
};

/*
* expect_val == ObMacroMinTransVersion::INIT_VERSION or SKIP_RECORDING_VERSION means no valid info
* else means valid (expect not 0)
*/
int ObMinMergedTransVersionTest::check_macro_and_micro(
  const ObTablet &tablet,
  ObSSTable &last_major,
  bool expect_compat,
  const int64_t border_macro_version)
{
  int ret = OB_SUCCESS;
  ObIMacroBlockIterator *macro_block_iter = nullptr;
  ObIndexBlockMicroIterator micro_block_iter;
  const blocksstable::ObMicroBlock *micro_block = nullptr;
  ObDatumRange range;
  range.set_whole_range();
  ObMacroBlockDesc macro_desc;
  ObDataMacroBlockMeta block_meta;
  macro_desc.macro_meta_ = &block_meta;
  bool meet_border_macro = false;
  if (OB_FAIL(last_major.scan_macro_block(
        range,
        tablet.get_rowkey_read_info(),
        allocator_,
        macro_block_iter,
        false, /* reverse scan */
        true, /* need micro info */
        true /* need secondary meta */))) {
    LOG_WARN("Fail to scan macro block", K(ret));
  }
  ObTransVersionSkipIndexInfo skip_index_info;
  const int64_t schema_rowkey_cnt = tablet.get_rowkey_read_info().get_schema_rowkey_count();
#define CHECK_MIN_VER(min_ver) \
  (expect_compat ? (0 != min_ver) : (0 == min_ver))
  while (OB_SUCC(ret) && OB_SUCC(macro_block_iter->get_next_macro_block(macro_desc))) {
    if (OB_ISNULL(macro_desc.macro_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null macro meta", K(ret), K(macro_desc.macro_meta_->val_));
    } else if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
        macro_desc, schema_rowkey_cnt, skip_index_info))) {
      LOG_WARN("Failed to read min max snapshot", K(ret), K(macro_desc));
    } else if (INT64_MAX != border_macro_version
       && !meet_border_macro
       && macro_desc.macro_meta_->val_.snapshot_version_ > border_macro_version) {
      // first meet border macro
      meet_border_macro = true;
      expect_compat = false;
      if (0 != skip_index_info.min_snapshot_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("first border macro meta should have skip recording trans version", K(ret),
          K(border_macro_version), K(macro_desc.macro_meta_->val_), K(skip_index_info));
      }
    } else if (INT64_MAX == skip_index_info.max_snapshot_ || CHECK_MIN_VER(skip_index_info.min_snapshot_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("macro meta should record max & min version", K(ret), K(expect_compat), K(skip_index_info));
    } else if (FALSE_IT(micro_block_iter.reset())) {
    } else if (OB_FAIL(micro_block_iter.init(
                macro_desc,
                tablet.get_rowkey_read_info(),
                macro_block_iter->get_micro_index_infos(),
                macro_block_iter->get_micro_endkeys(),
                static_cast<ObRowStoreType>(macro_desc.row_store_type_),
                &last_major))) {
        LOG_WARN("Failed to init micro_block_iter", K(ret), K(macro_desc));
    } else {
      LOG_INFO("check macro", "macro_id", macro_desc.macro_meta_->val_.macro_id_, K(skip_index_info));
      while (OB_SUCC(ret) && OB_SUCC(micro_block_iter.next(micro_block))) {
        if (OB_ISNULL(micro_block)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("null micro block", K(ret), K(micro_block));
        } else if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
            *micro_block, schema_rowkey_cnt, skip_index_info))) {
          LOG_WARN("Failed to read min max snapshot", K(ret), K(micro_block));
        } else if (INT64_MAX == skip_index_info.max_snapshot_ || CHECK_MIN_VER(skip_index_info.min_snapshot_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("macro meta should record max&min version", K(ret), K(micro_block));
        } else {
          LOG_INFO("check micro", K(skip_index_info));
        }
      } // while
      ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
    }
  } // while
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObMinMergedTransVersionTest::get_last_major_sstable(
    sqlclient::ObISQLConnection &conn,
    const char *table_name,
    const bool is_index,
    ObTabletHandle &tablet_handle,
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    ObSSTable *&last_major)
{
  int ret = OB_SUCCESS;
  int64_t table_id = 0;
  ObTabletID tablet_id;
  ObLSID ls_id;
  if (OB_FAIL(CompactionBasicFunc::get_table_id(conn, table_name, is_index, table_id))) {
    LOG_WARN("fail to get table id", K(ret));
  } else if (OB_FAIL(CompactionBasicFunc::get_tablet_and_ls_id(conn, table_id, tablet_id, ls_id))) {
    LOG_WARN("fail to get tablet and ls id", K(ret));
  } else if (OB_FAIL(TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("fail to get tablet", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(last_major = static_cast<ObSSTable*>(
    table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("major sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  }
  return ret;
}

TEST_F(ObMinMergedTransVersionTest, gene_major_with_min_ver)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  EXE_SQL("create table t1(c1 bigint, c2 bigint, c3 varchar(40960), c4 varchar(40960)) compression= 'none' ");
  EXE_SQL("insert into t1 select random(), random(), randstr(40960, random()), randstr(40960, random()) from table(generator(400))");
  EXE_SQL("alter system set ob_compaction_schedule_interval = '3s'");
  EXE_SQL("alter system set merger_check_interval = '10s'");
  EXE_SQL("alter system major freeze");
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::check_major_finish(*conn, tenant_id_));

  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *last_major = nullptr;
  if (OB_FAIL(get_last_major_sstable(*conn, "t1", false/*is_index*/, tablet_handle, table_store_wrapper, last_major))) {
    LOG_WARN("fail to get last major sstable", K(ret));
  } else if (OB_FAIL(check_macro_and_micro(*tablet_handle.get_obj(), *last_major, 0/*expect not max*/))) {
    LOG_ERROR("failed to check macro and micro", K(ret), KPC(last_major));
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  tablet_handle.reset();
  EXE_SQL("alter table t1 add index idx1(c1)");
  if (OB_FAIL(get_last_major_sstable(*conn, "t1", true/*is_index*/, tablet_handle, table_store_wrapper, last_major))) {
    LOG_WARN("fail to get last major sstable", K(ret));
  } else if (OB_FAIL(check_macro_and_micro(*tablet_handle.get_obj(), *last_major, 0/*expect not max*/))) {
    LOG_ERROR("failed to check macro and micro", K(ret), KPC(last_major));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObMinMergedTransVersionTest, compat_major)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  int64_t first_major_scn = 0;
  int64_t affected_rows = 0;
  EXE_SQL("create table t2(c1 int primary key, c2 bigint, c3 varchar(40960), c4 varchar(40960)) compression= 'none' ");
  int64_t skip_insert_idx = 30;
  int64_t end_idx = 40;
  for (int64_t idx = 0; idx < end_idx; ++idx) {
    if (skip_insert_idx != idx) {
      EXE_SQL_FMT("insert into t2 values(%ld, random(), randstr(40960, random()), randstr(40960, random()))", idx);
    }
  }
  EXE_SQL("alter system set ob_compaction_schedule_interval = '3s'");
  EXE_SQL("alter system set merger_check_interval = '10s'");

  // use old data version to do major
  MOCK_MIN_DATA_VERSION = DATA_VERSION_4_3_5_2;
  EXE_SQL("alter system major freeze");
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::check_major_finish(*conn, tenant_id_));

  sql.assign_fmt("select GLOBAL_BROADCAST_SCN as val from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=%lu", tenant_id_);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn, sql.ptr(), first_major_scn));

  EXE_SQL_FMT("insert into t2 values(%ld, random(), randstr(40960, random()), randstr(40960, random()))", skip_insert_idx);
  for (int64_t idx = end_idx; idx < 2 * end_idx; ++idx) {
    EXE_SQL_FMT("insert into t2 values(%ld, random(), randstr(40960, random()), randstr(40960, random()))", idx);
  }
  // use new data version to do major
  MOCK_MIN_DATA_VERSION = DATA_CURRENT_VERSION;
  EXE_SQL("alter system major freeze");
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::check_major_finish(*conn, tenant_id_));

  int64_t table_id = 0;
  ObTabletID tablet_id;
  ObLSID ls_id;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, "t2", false/*is_index*/, table_id));
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_tablet_and_ls_id(*conn, table_id, tablet_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));

  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *last_major = nullptr;
  if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(last_major = static_cast<ObSSTable*>(
    table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("major sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_FAIL(check_macro_and_micro(*tablet_handle.get_obj(), *last_major, true/*expect_compat*/, first_major_scn))) {
    LOG_ERROR("failed to check macro and micro", K(ret), KPC(last_major));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}