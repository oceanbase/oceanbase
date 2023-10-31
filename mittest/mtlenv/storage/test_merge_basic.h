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
#ifndef OB_TEST_MERGE_BASIC_H_
#define OB_TEST_MERGE_BASIC_H_

#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"

namespace oceanbase
{
namespace storage
{
class TestMergeBasic : public ObMultiVersionSSTableTest
{
public:
  TestMergeBasic(const char *test_name)
    : ObMultiVersionSSTableTest(test_name)
  {}
  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             compaction::ObBasicTabletMergeCtx &merge_context)
  {
    bool has_lob = false;
    ObLSID ls_id(ls_id_);
    ObTabletID tablet_id(tablet_id_);
    ObLSHandle ls_handle;
    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
    compaction::ObStaticMergeParam &static_param = merge_context.static_param_;
    static_param.ls_handle_ = ls_handle;

    ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    merge_context.tablet_handle_ = tablet_handle;

    table_merge_schema_.reset();
    OK(table_merge_schema_.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    static_param.schema_version_ = table_schema_.get_schema_version();
    static_param.schema_ = &table_merge_schema_;

    static_param.is_full_merge_ = is_full_merge;
    static_param.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    static_param.dag_param_.merge_type_ = merge_type;
    static_param.dag_param_.merge_version_ = 0;
    static_param.dag_param_.ls_id_ = ls_id_;
    static_param.dag_param_.tablet_id_ = tablet_id_;
    static_param.version_range_ = trans_version_range;
    static_param.report_ = &rs_reporter_;
    static_param.progressive_merge_num_ = 0;
    const int64_t tables_count = static_param.tables_handle_.get_count();
    static_param.scn_range_.start_scn_ = static_param.tables_handle_.get_table(0)->get_start_scn();
    static_param.scn_range_.end_scn_ = static_param.tables_handle_.get_table(tables_count - 1)->get_end_scn();
    static_param.merge_scn_ = static_param.scn_range_.end_scn_;
  }

  ObStorageSchema table_merge_schema_;
};

}
}

#endif
