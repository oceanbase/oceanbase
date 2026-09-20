/**
 * Copyright (c) 2022 OceanBase
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
#include "lib/alloc/ob_malloc_allocator.h"

#define private public
#include "ob_log_part_mgr.h"
#include "ob_log_tenant_mgr.h"
#include "ob_log_instance.h"
#include "ob_log_meta_manager.h"
#include "ob_log_sequencer1.h"
#include "ob_log_config.h"
#include "ob_log_dml_parser.h"
#include "ob_log_reader.h"
#include "ob_log_formatter.h"
#include "ob_log_trans_redo_dispatcher.h"
#include "ob_log_ddl_processor.h"
#include "ob_log_schema_getter.h"
#include "ob_log_trans_ctx.h"
#include "ob_log_meta_data_service.h"
#include "ob_log_table_matcher.h"
#include "logservice/data_dictionary/ob_data_dict_service.h"
#undef private

using namespace oceanbase;
using namespace common;
using namespace libobcdc;
using namespace share::schema;
using namespace datadict;

namespace oceanbase
{
namespace unittest
{
class MViewTestTenantMgr : public ObLogTenantMgr
{
public:
  explicit MViewTestTenantMgr(ObLogTenant &tenant) : tenant_(tenant) {}
  int get_tenant_guard(const uint64_t, ObLogTenantGuard &guard) override
  {
    guard.set_tenant(&tenant_);
    return OB_SUCCESS;
  }
  int revert_tenant(ObLogTenant *) override { return OB_SUCCESS; }
  int filter_ddl_stmt(const uint64_t, bool &chosen) override
  {
    chosen = true;
    return OB_SUCCESS;
  }
private:
  ObLogTenant &tenant_;
};

class TestMViewPartMgr : public ::testing::Test
{
public:
  TestMViewPartMgr() : tenant_(), part_mgr_(tenant_.part_mgr_), tenant_mgr_(tenant_) {}

  void SetUp() override
  {
    old_mode_ = TCTX.refresh_mode_;
    old_parser_ = TCTX.dml_parser_;
    old_reader_ = TCTX.reader_;
    old_dispatcher_ = TCTX.trans_redo_dispatcher_;
    old_formatter_ = TCTX.formatter_;
    old_processor_ = TCTX.ddl_processor_;
    old_matcher_ = TCTX.tb_matcher_;
    old_tenant_mgr_ = TCTX.tenant_mgr_;
    old_schema_getter_ = TCTX.schema_getter_;
    ASSERT_EQ(OB_SUCCESS, lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001));
    ASSERT_EQ(OB_SUCCESS, lib::ObMallocAllocator::get_instance()->set_tenant_limit(1001, 1L << 30));
    tenant_.tenant_id_ = 1001;
    tenant_.start_schema_version_ = 10;
    ASSERT_EQ(OB_SUCCESS, gindex_.init("TestMView"));
    ASSERT_EQ(OB_SUCCESS, dict_.init());
    ObTenantSchema tenant_schema;
    tenant_schema.set_tenant_id(1001);
    tenant_schema.set_schema_version(10);
    ASSERT_EQ(OB_SUCCESS, tenant_schema.set_tenant_name("tenant"));
    ASSERT_EQ(OB_SUCCESS, dict_.get_dict_tenant_meta().init(tenant_schema));
    ObDatabaseSchema database_schema;
    database_schema.set_tenant_id(1001);
    database_schema.set_database_id(500000);
    database_schema.set_schema_version(10);
    ASSERT_EQ(OB_SUCCESS, database_schema.set_database_name("db"));
    ObDictDatabaseMeta *database_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, dict_.alloc_dict_db_meta(database_meta));
    ASSERT_EQ(OB_SUCCESS, database_meta->init(database_schema));
    ASSERT_EQ(OB_SUCCESS, dict_.insert_dict_db_meta(database_meta));
    TCTX.tb_matcher_ = &matcher_;
    TCTX.tenant_mgr_ = &tenant_mgr_;
    TCONF.enable_hbase_mode = false;
    ASSERT_EQ(OB_SUCCESS, matcher_.init("*.*.*", "|", "*.*", "|"));
  }

  void TearDown() override
  {
    TCTX.refresh_mode_ = old_mode_;
    TCTX.dml_parser_ = old_parser_;
    TCTX.reader_ = old_reader_;
    TCTX.trans_redo_dispatcher_ = old_dispatcher_;
    TCTX.formatter_ = old_formatter_;
    TCTX.ddl_processor_ = old_processor_;
    GLOGMETADATASERVICE.baseline_loader_.destroy();
    TCTX.tb_matcher_ = old_matcher_;
    TCTX.tenant_mgr_ = old_tenant_mgr_;
    TCTX.schema_getter_ = old_schema_getter_;
    part_mgr_.reset();
    gindex_.destroy();
  }

  void init_mgr(const bool lists, const bool output)
  {
    part_mgr_.reset();
    ASSERT_EQ(OB_SUCCESS, part_mgr_.init(1001, 10, false, lists, gindex_, output));
  }

  void set_matcher(const char *white, const char *black = "|")
  {
    matcher_.destroy();
    ASSERT_EQ(OB_SUCCESS, matcher_.init(white, black, "*.*", "|"));
  }

  void add_key_column(ObTableSchema &schema)
  {
    ObColumnSchemaV2 column;
    column.set_tenant_id(schema.get_tenant_id());
    column.set_table_id(schema.get_table_id());
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    column.set_data_type(ObIntType);
    column.set_data_length(static_cast<int64_t>(sizeof(int64_t)));
    column.set_rowkey_position(1);
    column.set_nullable(false);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name("id"));
    ASSERT_EQ(OB_SUCCESS, schema.add_column(column));
    schema.set_rowkey_column_num(1);
    schema.set_max_used_column_id(OB_APP_MIN_COLUMN_ID);
  }

  void add_schema(const uint64_t table_id, const char *name, const ObTableType type,
      const uint64_t data_table_id = OB_INVALID_ID, const bool container = false,
      const uint64_t association_id = OB_INVALID_ID, const uint64_t aux_meta_id = OB_INVALID_ID)
  {
    ObTableSchema schema;
    schema.set_tenant_id(1001);
    schema.set_database_id(500000);
    schema.set_table_id(table_id);
    schema.set_schema_version(10);
    schema.set_table_type(type);
    schema.set_data_table_id(data_table_id);
    schema.set_aux_lob_meta_tid(aux_meta_id);
    schema.set_tablet_id(ObTabletID(table_id + 10000));
    schema.set_mv_container_table(container ? IS_MV_CONTAINER_TABLE : IS_NOT_MV_CONTAINER_TABLE);
    if (OB_INVALID_ID != association_id) {
      ObTableMode mode = schema.get_table_mode_struct();
      mode.state_flag_ |= TABLE_STATE_IS_HIDDEN_MASK;
      schema.set_table_mode_struct(mode);
      schema.set_association_table_id(association_id);
    }
    ASSERT_EQ(OB_SUCCESS, schema.set_table_name(name));
    add_key_column(schema);
    ObDictTableMeta *meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, dict_.alloc_dict_table_meta(meta));
    ASSERT_EQ(OB_SUCCESS, meta->init(schema));
    ASSERT_EQ(OB_SUCCESS, dict_.insert_dict_table_meta(meta));
    ASSERT_EQ(OB_SUCCESS, tables_.push_back(meta));
  }

  void add_mview_set(const bool include_mv = true)
  {
    add_schema(500004, "hidden_container", USER_TABLE, OB_INVALID_ID, true, 500003);
    add_schema(500005, "lob_meta", AUX_LOB_META, 500004);
    add_schema(500003, "__mv_container_500002", USER_TABLE, OB_INVALID_ID, true);
    add_schema(500001, "sales", USER_TABLE);
    add_schema(500007, "sales_lob", AUX_LOB_META, 500001);
    if (include_mv) {
      add_schema(500002, "sales_summary", MATERIALIZED_VIEW, 500003);
    }
  }

  void baseline()
  {
    ASSERT_EQ(OB_SUCCESS, part_mgr_.add_all_user_tablets_and_tables_info(&dict_, tables_, 1000000));
  }

  void expect_chosen(const uint64_t table_id, const bool expected)
  {
    bool chosen = !expected;
    ASSERT_EQ(OB_SUCCESS, part_mgr_.is_exist_table_id_cache(table_id, chosen));
    EXPECT_EQ(expected, chosen) << table_id;
  }

  void expect_name(const uint64_t table_id, const char *expected)
  {
    ObArenaAllocator allocator;
    ObString name;
    ASSERT_EQ(OB_SUCCESS, part_mgr_.get_mview_name(table_id, allocator, name));
    EXPECT_EQ(ObString(expected), name);
  }

  void append_dict_meta(PartTransTask &task, const ObDictTableMeta &source)
  {
    void *buffer = allocator_.alloc(sizeof(ObDictTableMeta));
    ASSERT_NE(nullptr, buffer);
    ObDictTableMeta *meta = new (buffer) ObDictTableMeta(&allocator_);
    ASSERT_EQ(OB_SUCCESS, meta->assign(source));
    ASSERT_EQ(OB_SUCCESS, task.get_dict_table_array().push_back(meta));
  }

  void append_mapping(PartTransTask &task, const uint64_t table_id, const char *name = "sales_summary")
  {
    ObLogMViewInfo mapping;
    mapping.container_table_id_ = table_id;
    mapping.mview_id_ = 500002;
    mapping.mapping_state_ = ObLogMViewInfo::MAPPED;
    ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator_, ObString(name), mapping.mview_name_, true));
    ASSERT_EQ(OB_SUCCESS, task.get_mview_mappings().push_back(mapping));
  }

  void append_container(PartTransTask &task, const uint64_t table_id,
      const uint64_t association_id = OB_INVALID_ID)
  {
    ObLogMViewContainerInfo container;
    container.container_table_id_ = table_id;
    container.association_table_id_ = association_id;
    ASSERT_EQ(OB_SUCCESS, task.get_mview_containers().push_back(container));
  }

  ObLogTenant tenant_;
  ObLogPartMgr &part_mgr_;
  MViewTestTenantMgr tenant_mgr_;
  GIndexCache gindex_;
  ObDictTenantInfo dict_;
  ObArray<const ObDictTableMeta *> tables_;
  ObLogTableMatcher matcher_;
  ObArenaAllocator allocator_;
  RefreshMode old_mode_;
  IObLogDmlParser *old_parser_;
  IObLogReader *old_reader_;
  IObLogTransRedoDispatcher *old_dispatcher_;
  IObLogFormatter *old_formatter_;
  ObLogDDLProcessor *old_processor_;
  IObLogTableMatcher *old_matcher_;
  IObLogTenantMgr *old_tenant_mgr_;
  IObLogSchemaGetter *old_schema_getter_;
};

TEST_F(TestMViewPartMgr, baseline_switch_combinations_preserve_physical_tablets)
{
  add_mview_set();
  for (int lists = 0; lists <= 1; ++lists) {
    for (int output = 0; output <= 1; ++output) {
      init_mgr(lists, output);
      baseline();
      expect_chosen(500001, true);
      expect_chosen(500007, true);
      expect_chosen(500003, output);
      expect_chosen(500004, output);
      expect_chosen(500005, output);
      if (lists || !output) {
        expect_chosen(500002, false);
        EXPECT_EQ(output ? 5 : 2, part_mgr_.table_id_cache_.count());
      } else {
        EXPECT_EQ(0, part_mgr_.table_id_cache_.count());
      }
      expect_name(500003, "sales_summary");
      expect_name(500004, "sales_summary");
      ObCDCTableInfo info;
      ASSERT_EQ(OB_SUCCESS, part_mgr_.get_table_info_of_tablet_id(ObTabletID(510004), info));
      EXPECT_EQ(500004, info.table_id_);
      EXPECT_EQ(USER_TABLE, info.table_type_);
    }
  }
}

TEST_F(TestMViewPartMgr, whitelist_uses_only_materialized_view_name)
{
  add_mview_set();
  init_mgr(true, true);
  set_matcher("tenant.db.sales_summary");
  baseline();
  expect_chosen(500001, false);
  expect_chosen(500003, true);
  expect_chosen(500004, true);
  expect_chosen(500005, true);
  expect_chosen(500002, false);
  init_mgr(true, true);
  set_matcher("tenant.db.__mv_container_500002");
  baseline();
  expect_chosen(500003, false);
  expect_chosen(500004, false);
}

TEST_F(TestMViewPartMgr, blacklist_follows_hidden_container_and_lob)
{
  add_mview_set();
  init_mgr(true, true);
  set_matcher("*.*.*", "tenant.db.sales_summary");
  baseline();
  expect_chosen(500001, true);
  expect_chosen(500003, false);
  expect_chosen(500004, false);
  expect_chosen(500005, false);
}

TEST_F(TestMViewPartMgr, disabled_lists_never_use_name_rules)
{
  add_mview_set();
  init_mgr(false, false);
  set_matcher("tenant.db.missing", "*.*.*");
  baseline();
  expect_chosen(500001, true);
  expect_chosen(500007, true);
  expect_chosen(500003, false);
  expect_chosen(500005, false);
}

TEST_F(TestMViewPartMgr, compatibility_missing_is_distinct_from_uninitialized_or_missing_cache)
{
  init_mgr(true, true);
  ObString name;
  EXPECT_EQ(OB_NOT_INIT, part_mgr_.get_mview_name(500003, allocator_, name));
  add_mview_set(false);
  baseline();
  expect_name(500003, "");
  expect_name(500004, "");
  expect_chosen(500003, true);
  EXPECT_EQ(OB_ERR_UNEXPECTED, part_mgr_.get_mview_name(509999, allocator_, name));
  init_mgr(true, true);
  set_matcher("tenant.db.sales_summary");
  baseline();
  expect_chosen(500003, false);
  init_mgr(false, false);
  baseline();
  expect_chosen(500003, false);
  expect_chosen(500004, false);
}

TEST_F(TestMViewPartMgr, residual_view_metadata_does_not_recreate_dropped_container)
{
  init_mgr(true, true);
  add_schema(500002, "sales_summary", MATERIALIZED_VIEW, 500003);
  add_schema(500001, "sales", USER_TABLE);
  baseline();
  EXPECT_EQ(0, part_mgr_.mv_container_map_.size());
  expect_chosen(500003, false);
}

TEST_F(TestMViewPartMgr, pending_hidden_creation_waits_for_previous_mapping_publication)
{
  init_mgr(true, true);
  baseline();
  PartTransTask first;
  PartTransTask second;
  append_mapping(first, 500003);
  append_container(second, 500004, 500003);
  EXPECT_TRUE(first.need_update_mview_cache());
  EXPECT_FALSE(part_mgr_.has_mview_mapping(500003));
  EXPECT_FALSE(part_mgr_.has_mview_mapping(500004));
  ASSERT_EQ(OB_SUCCESS, part_mgr_.apply_mview_updates(first));
  ASSERT_EQ(OB_SUCCESS, part_mgr_.apply_mview_updates(second));
  expect_name(500004, "sales_summary");
  ASSERT_EQ(OB_SUCCESS, part_mgr_.remove_mview_mapping(500003));
  allocator_.reset();
  expect_name(500004, "sales_summary");
  ASSERT_EQ(OB_SUCCESS, part_mgr_.remove_mview_mapping(500003));
}

TEST_F(TestMViewPartMgr, creation_order_is_independent_and_conflicts_fail)
{
  init_mgr(true, true);
  baseline();
  PartTransTask task;
  append_container(task, 500004, 500003);
  append_container(task, 500003);
  append_mapping(task, 500003);
  ASSERT_EQ(OB_SUCCESS, part_mgr_.apply_mview_updates(task));
  ASSERT_EQ(OB_SUCCESS, part_mgr_.apply_mview_updates(task));
  EXPECT_EQ(2, part_mgr_.mv_container_map_.size());
  expect_name(500004, "sales_summary");
  task.get_mview_mappings().at(0).mview_id_ = 509999;
  EXPECT_EQ(OB_ERR_UNEXPECTED, part_mgr_.apply_mview_updates(task));
  expect_name(500003, "sales_summary");
}

TEST_F(TestMViewPartMgr, cyclic_hidden_association_is_not_compatibility_missing)
{
  init_mgr(true, true);
  add_schema(500004, "hidden_one", USER_TABLE, OB_INVALID_ID, true, 500003);
  add_schema(500003, "hidden_two", USER_TABLE, OB_INVALID_ID, true, 500004);
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      part_mgr_.add_all_user_tablets_and_tables_info(&dict_, tables_, 1000000));
  EXPECT_EQ(0, part_mgr_.mv_container_map_.size());
}

TEST_F(TestMViewPartMgr, repeated_refresh_cleanup_keeps_only_live_mapping)
{
  init_mgr(true, true);
  baseline();
  PartTransTask first;
  append_container(first, 500003);
  append_mapping(first, 500003);
  ASSERT_EQ(OB_SUCCESS, part_mgr_.apply_mview_updates(first));
  PartTransTask refresh;
  for (uint64_t id = 500004; id < 500104; ++id) {
    refresh.get_mview_containers().reuse();
    append_container(refresh, id, id - 1);
    ASSERT_EQ(OB_SUCCESS, part_mgr_.apply_mview_updates(refresh));
    ASSERT_EQ(OB_SUCCESS, part_mgr_.remove_mview_mapping(id - 1));
    EXPECT_EQ(1, part_mgr_.mv_container_map_.size());
    expect_name(id, "sales_summary");
  }
}

TEST_F(TestMViewPartMgr, formatter_name_owns_copy_after_cache_reclamation)
{
  init_mgr(true, true);
  add_mview_set();
  baseline();
  ObLogMetaManager manager;
  ITableMeta *meta = DRCMessageFactory::createTableMeta();
  ASSERT_NE(nullptr, meta);
  ASSERT_EQ(OB_SUCCESS, manager.set_mview_table_name_(1001, 500003, "__mv_container_500002", *meta));
  EXPECT_STREQ("sales_summary", meta->getName());
  ASSERT_EQ(OB_SUCCESS, part_mgr_.remove_mview_mapping(500003));
  EXPECT_STREQ("sales_summary", meta->getName());
  ObDictTableMeta *raw = nullptr;
  ASSERT_EQ(OB_SUCCESS, dict_.get_table_meta(500003, raw));
  EXPECT_STREQ("__mv_container_500002", raw->get_table_name());
  DRCMessageFactory::destroy(meta);
}

TEST_F(TestMViewPartMgr, formatter_keeps_raw_name_for_confirmed_old_metadata)
{
  init_mgr(true, true);
  add_mview_set(false);
  baseline();
  ObLogMetaManager manager;
  ITableMeta *meta = DRCMessageFactory::createTableMeta();
  ASSERT_NE(nullptr, meta);
  ASSERT_EQ(OB_SUCCESS, manager.set_mview_table_name_(1001, 500003, "__mv_container_500002", *meta));
  EXPECT_STREQ("__mv_container_500002", meta->getName());
  DRCMessageFactory::destroy(meta);
}

TEST_F(TestMViewPartMgr, drop_view_preserves_container_but_physical_drop_reclaims_it)
{
  init_mgr(true, true);
  add_mview_set();
  baseline();
  ObLogSequencer sequencer;
  PartTransTask task;
  MemtableMutatorRow row(allocator_);
  DdlStmtTask drop(task, row);
  drop.ddl_operation_type_ = OB_DDL_DROP_VIEW;
  drop.ddl_op_table_id_ = 500002;
  task.stmt_list_.head_ = &drop;
  EXPECT_FALSE(sequencer.has_dict_drop_op_(task));
  EXPECT_FALSE(sequencer.needs_mview_drop_barrier_(task, part_mgr_));
  ASSERT_EQ(OB_SUCCESS, sequencer.recycle_ddl_table_state_(task, part_mgr_));
  ASSERT_EQ(OB_SUCCESS, sequencer.remove_dict_table_metas_(task, dict_));
  expect_chosen(500003, true);
  expect_name(500003, "sales_summary");
  drop.ddl_operation_type_ = OB_DDL_DROP_TABLE;
  drop.ddl_op_table_id_ = 500003;
  EXPECT_TRUE(task.get_multi_data_source_info().is_empty_dict_info());
  EXPECT_TRUE(sequencer.has_dict_drop_op_(task));
  EXPECT_TRUE(sequencer.needs_mview_drop_barrier_(task, part_mgr_));
  ASSERT_EQ(OB_SUCCESS, sequencer.recycle_ddl_table_state_(task, part_mgr_));
  expect_chosen(500003, false);
  EXPECT_FALSE(part_mgr_.has_mview_mapping(500003));
  expect_name(500004, "sales_summary");
  ObDictTableMeta *raw = nullptr;
  ASSERT_EQ(OB_SUCCESS, dict_.get_table_meta(500003, raw));
  ASSERT_EQ(OB_SUCCESS, sequencer.remove_dict_table_metas_(task, dict_));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, dict_.get_table_meta(500003, raw));
  ASSERT_EQ(OB_SUCCESS, dict_.get_table_meta(500002, raw));
  ASSERT_EQ(OB_SUCCESS, sequencer.remove_dict_table_metas_(task, dict_));
  task.stmt_list_.head_ = nullptr;
}

TEST_F(TestMViewPartMgr, runtime_drop_configuration_is_dynamic_and_independent)
{
  ObLogConfig config;
  ASSERT_EQ(OB_SUCCESS, config.init());
  EXPECT_FALSE(config.enable_output_mv);
  EXPECT_TRUE(config.enable_data_dict_runtime_drop);
  ObLogSequencer sequencer;
  sequencer.configure(config);
  const bool first_transaction = ATOMIC_LOAD(&sequencer.enable_data_dict_runtime_drop_);
  EXPECT_TRUE(first_transaction);
  config.enable_data_dict_runtime_drop = false;
  sequencer.configure(config);
  EXPECT_FALSE(ATOMIC_LOAD(&sequencer.enable_data_dict_runtime_drop_));
  EXPECT_TRUE(first_transaction);
  config.enable_output_mv = true;
  sequencer.configure(config);
  EXPECT_FALSE(ATOMIC_LOAD(&sequencer.enable_data_dict_runtime_drop_));
}

class MViewTestParser : public ObLogDmlParser
{
public:
  MViewTestParser(ObLogSequencer &sequencer, ObLogConfig &config, const bool next_value)
      : sequencer_(sequencer), config_(config), next_value_(next_value) {}
  int get_log_entry_task_count(int64_t &count) override
  {
    config_.enable_data_dict_runtime_drop = next_value_;
    sequencer_.configure(config_);
    count = 0;
    return OB_SUCCESS;
  }
private:
  ObLogSequencer &sequencer_;
  ObLogConfig &config_;
  bool next_value_;
};

class MViewTestFormatter : public ObLogFormatter
{
public:
  MViewTestFormatter() : calls_(0), fail_(false) {}
  int get_task_count(int64_t &br_count, int64_t &entry_count, int64_t &lob_count) override
  {
    br_count = 0;
    entry_count = 0;
    lob_count = 0 == calls_++ ? 1 : 0;
    return fail_ ? OB_TIMEOUT : OB_SUCCESS;
  }
  int64_t calls_;
  bool fail_;
};

TEST_F(TestMViewPartMgr, empty_dictionary_drop_uses_one_snapshot_and_waits_for_lob)
{
  init_mgr(false, true);
  tenant_.tenant_id_ = 1001;
  tenant_.start_schema_version_ = 10;
  add_schema(500001, "sales", USER_TABLE);
  baseline();
  ObLogConfig config;
  ASSERT_EQ(OB_SUCCESS, config.init());
  ObLogSequencer sequencer;
  ObLogDDLProcessor processor;
  ObLogReader reader;
  ObLogTransRedoDispatcher dispatcher;
  MViewTestFormatter formatter;
  MViewTestParser parser(sequencer, config, false);
  TCTX.refresh_mode_ = DATA_DICT;
  TCTX.dml_parser_ = &parser;
  TCTX.reader_ = &reader;
  TCTX.trans_redo_dispatcher_ = &dispatcher;
  TCTX.formatter_ = &formatter;
  TCTX.ddl_processor_ = &processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(nullptr, false, false));
  ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.baseline_loader_.init(config));
  ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.baseline_loader_.add_tenant(1001));
  {
    ObDictTenantInfoGuard guard;
    ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.get_tenant_info_guard(1001, guard));
    ObDictTenantInfo *runtime_dict = guard.get_tenant_info();
    runtime_dict->get_dict_tenant_meta().set_tenant_id(1001);
    ASSERT_EQ(OB_SUCCESS, runtime_dict->replace_dict_table_meta(*tables_.at(0)));
    PartTransTask task;
    task.set_allocator(8192, allocator_);
    task.type_ = PartTransTask::TASK_TYPE_DDL_TRANS;
    task.set_tls_id(logservice::TenantLSID(1001, share::ObLSID(1)));
    task.local_schema_version_ = 20;
    MemtableMutatorRow row(allocator_);
    DdlStmtTask drop(task, row);
    drop.ddl_operation_type_ = OB_DDL_DROP_TABLE;
    drop.ddl_op_table_id_ = 500001;
    task.stmt_list_.head_ = &drop;
    TransCtx trans;
    trans.ready_participant_objs_ = &task;
    volatile bool stop = false;
    ObDictTableMeta *raw = nullptr;
    config.enable_data_dict_runtime_drop = true;
    sequencer.configure(config);
    ASSERT_EQ(OB_SUCCESS, sequencer.handle_multi_data_source_info_(tenant_, trans, stop));
    EXPECT_FALSE(ATOMIC_LOAD(&sequencer.enable_data_dict_runtime_drop_));
    EXPECT_EQ(2, formatter.calls_);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, runtime_dict->get_table_meta(500001, raw));

    // The next transaction keeps metadata after the dynamic change in the preceding barrier.
    ASSERT_EQ(OB_SUCCESS, runtime_dict->replace_dict_table_meta(*tables_.at(0)));
    formatter.calls_ = 0;
    ASSERT_EQ(OB_SUCCESS, sequencer.handle_multi_data_source_info_(tenant_, trans, stop));
    EXPECT_EQ(0, formatter.calls_);
    ASSERT_EQ(OB_SUCCESS, runtime_dict->get_table_meta(500001, raw));

    // Switching on after an entry snapshot of false must not reclaim without its barrier.
    MViewTestParser enable_parser(sequencer, config, true);
    TCTX.dml_parser_ = &enable_parser;
    ASSERT_EQ(OB_SUCCESS, sequencer.handle_multi_data_source_info_(tenant_, trans, stop));
    EXPECT_TRUE(ATOMIC_LOAD(&sequencer.enable_data_dict_runtime_drop_));
    EXPECT_EQ(0, formatter.calls_);
    ASSERT_EQ(OB_SUCCESS, runtime_dict->get_table_meta(500001, raw));
    ASSERT_EQ(OB_SUCCESS, sequencer.handle_multi_data_source_info_(tenant_, trans, stop));
    EXPECT_EQ(2, formatter.calls_);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, runtime_dict->get_table_meta(500001, raw));

    // A failed Formatter barrier preserves both the raw object and all derived state.
    ASSERT_EQ(OB_SUCCESS, runtime_dict->replace_dict_table_meta(*tables_.at(0)));
    formatter.fail_ = true;
    EXPECT_EQ(OB_TIMEOUT, sequencer.handle_multi_data_source_info_(tenant_, trans, stop));
    ASSERT_EQ(OB_SUCCESS, runtime_dict->get_table_meta(500001, raw));

    formatter.fail_ = false;
    formatter.calls_ = 0;
    ASSERT_EQ(OB_SUCCESS, sequencer.schema_inc_replay_.init(false));
    task.multi_data_source_info_.set_ddl_trans();
    append_dict_meta(task, *tables_.at(0));
    ASSERT_EQ(OB_SUCCESS, sequencer.handle_multi_data_source_info_(tenant_, trans, stop));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, runtime_dict->get_table_meta(500001, raw));
    EXPECT_EQ(2, formatter.calls_);

    // A CREATE/DROP batch replays schemas before matching, and reclaims DROP only after all replay.
    add_schema(500003, "__mv_container_500002", USER_TABLE, OB_INVALID_ID, true, OB_INVALID_ID, 500005);
    add_schema(500005, "lob_meta", AUX_LOB_META, 500003);
    add_schema(500002, "sales_summary", MATERIALIZED_VIEW, 500003);
    ASSERT_EQ(OB_SUCCESS, runtime_dict->update_tenant_name("tenant"));
    ObDictDatabaseMeta *database_meta = nullptr;
    ASSERT_EQ(OB_SUCCESS, dict_.db_map_.get(MetaDataKey(500000), database_meta));
    void *db_buffer = allocator_.alloc(sizeof(ObDictDatabaseMeta));
    ASSERT_NE(nullptr, db_buffer);
    ObDictDatabaseMeta *new_database_meta = new (db_buffer) ObDictDatabaseMeta(&allocator_);
    ASSERT_EQ(OB_SUCCESS, new_database_meta->assign(*database_meta));
    ASSERT_EQ(OB_SUCCESS, task.get_dict_database_array().push_back(new_database_meta));
    for (int64_t i = 1; i < tables_.count(); ++i) {
      append_dict_meta(task, *tables_.at(i));
    }
    DdlStmtTask create_container(task, row);
    DdlStmtTask create_view(task, row);
    create_container.ddl_operation_type_ = OB_DDL_CREATE_TABLE;
    create_container.ddl_op_table_id_ = 500003;
    create_view.ddl_operation_type_ = OB_DDL_CREATE_VIEW;
    create_view.ddl_op_table_id_ = 500002;
    drop.set_next(&create_container);
    create_container.set_next(&create_view);
    processor.enable_white_black_list_ = true;
    part_mgr_.enable_white_black_list_ = true;
    formatter.calls_ = 0;
    ASSERT_EQ(OB_SUCCESS, sequencer.handle_multi_data_source_info_(tenant_, trans, stop));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, runtime_dict->get_table_meta(500001, raw));
    expect_name(500003, "sales_summary");
    expect_chosen(500003, true);
    expect_chosen(500005, true);
    expect_chosen(500002, false);
    EXPECT_EQ(2, formatter.calls_);

    PartTransTask incomplete;
    incomplete.set_allocator(8192, allocator_);
    DdlStmtTask known_view(incomplete, row);
    DdlStmtTask missing_container(incomplete, row);
    known_view.ddl_operation_type_ = OB_DDL_CREATE_VIEW;
    known_view.ddl_op_table_id_ = 500002;
    missing_container.ddl_operation_type_ = OB_DDL_CREATE_TABLE;
    missing_container.ddl_op_table_id_ = 500099;
    known_view.set_next(&missing_container);
    incomplete.stmt_list_.head_ = &known_view;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, part_mgr_.prepare_mview_create(incomplete, 20, 1000000));
    EXPECT_TRUE(incomplete.get_mview_mappings().empty());
    EXPECT_TRUE(incomplete.get_mview_containers().empty());
    expect_name(500003, "sales_summary");
    incomplete.stmt_list_.head_ = nullptr;
    task.stmt_list_.head_ = nullptr;
    trans.ready_participant_objs_ = nullptr;
  }
}

class MViewTestSchemaGetter : public ObLogSchemaGetter
{
public:
  explicit MViewTestSchemaGetter(const ObSimpleTableSchemaV2 &schema)
      : schema_(schema), error_(OB_TENANT_HAS_BEEN_DROPPED), call_count_(0) {}
  int get_schema_guard_and_table_schema(const uint64_t tenant_id, const uint64_t table_id,
      const int64_t expected_version, const int64_t timeout, IObLogSchemaGuard &schema_guard,
      const ObSimpleTableSchemaV2 *&table_schema) override
  {
    ++call_count_;
    table_schema = table_id == schema_.get_table_id() ? &schema_ : nullptr;
    return nullptr == table_schema ? error_ : OB_SUCCESS;
  }
  const ObSimpleTableSchemaV2 &schema_;
  int error_;
  int64_t call_count_;
};

TEST_F(TestMViewPartMgr, dropped_tenant_during_mview_prepare_skips_ddl_batch)
{
  init_mgr(true, true);
  TCTX.refresh_mode_ = ONLINE;
  ObTableSchema schema;
  schema.set_tenant_id(1001);
  schema.set_table_id(500003);
  schema.set_table_type(USER_TABLE);
  schema.set_mv_container_table(IS_MV_CONTAINER_TABLE);
  ASSERT_EQ(OB_SUCCESS, schema.set_table_name("__mv_container_500002"));
  MViewTestSchemaGetter schema_getter(schema);
  ObLogDDLProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(&schema_getter, false, true));
  TCTX.schema_getter_ = &schema_getter;
  PartTransTask task;
  task.set_allocator(8192, allocator_);
  task.type_ = PartTransTask::TASK_TYPE_DDL_TRANS;
  task.set_tls_id(logservice::TenantLSID(1001, share::ObLSID(1)));
  task.local_schema_version_ = 20;
  MemtableMutatorRow row(allocator_);
  DdlStmtTask created_container(task, row);
  DdlStmtTask view(task, row);
  DdlStmtTask container(task, row);
  ObLogBR created_br;
  ObLogBR view_br;
  ObLogBR container_br;
  created_container.ddl_operation_type_ = OB_DDL_CREATE_TABLE;
  created_container.ddl_op_table_id_ = 500003;
  view.ddl_operation_type_ = OB_DDL_CREATE_VIEW;
  view.ddl_op_table_id_ = 500009;
  container.ddl_operation_type_ = OB_DDL_CREATE_TABLE;
  container.ddl_op_table_id_ = 500004;
  created_container.set_binlog_record(&created_br);
  view.set_binlog_record(&view_br);
  container.set_binlog_record(&container_br);
  created_container.set_next(&view);
  view.set_next(&container);
  task.stmt_list_.head_ = &created_container;
  const int errors[] = {OB_TENANT_HAS_BEEN_DROPPED, OB_ERR_UNEXPECTED, OB_IN_STOP_STATE};
  volatile bool stop = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(errors); ++i) {
    schema_getter.error_ = errors[i];
    schema_getter.call_count_ = 0;
    created_br.set_is_valid(true);
    view_br.set_is_valid(true);
    container_br.set_is_valid(true);
    const bool tenant_dropped = OB_TENANT_HAS_BEEN_DROPPED == errors[i];
    EXPECT_EQ(tenant_dropped ? OB_SUCCESS : errors[i],
        processor.handle_ddl_trans(task, tenant_, true, stop));
    EXPECT_EQ(2, schema_getter.call_count_);
    EXPECT_EQ(!tenant_dropped, created_br.valid_);
    EXPECT_EQ(!tenant_dropped, view_br.valid_);
    EXPECT_EQ(!tenant_dropped, container_br.valid_);
    EXPECT_TRUE(task.get_mview_mappings().empty());
    EXPECT_TRUE(task.get_mview_containers().empty());
    EXPECT_FALSE(task.need_update_table_id_cache());
    EXPECT_FALSE(task.need_update_mview_cache());
    EXPECT_FALSE(part_mgr_.has_mview_mapping(500003));
  }
  task.stmt_list_.head_ = nullptr;
}

TEST_F(TestMViewPartMgr, prepare_ignores_internal_table_creation)
{
  init_mgr(true, true);
  ObLogConfig config;
  ASSERT_EQ(OB_SUCCESS, config.init());
  ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.baseline_loader_.init(config));
  ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.baseline_loader_.add_tenant(1001));
  PartTransTask task;
  task.set_allocator(8192, allocator_);
  MemtableMutatorRow row(allocator_);
  DdlStmtTask first(task, row);
  DdlStmtTask second(task, row);
  first.ddl_operation_type_ = OB_DDL_CREATE_TABLE;
  first.ddl_op_table_id_ = 419;
  second.ddl_operation_type_ = OB_DDL_CREATE_TABLE;
  second.ddl_op_table_id_ = 604;
  first.set_next(&second);
  for (int mode = 0; mode < 2; ++mode) {
    TCTX.refresh_mode_ = 0 == mode ? DATA_DICT : ONLINE;
    task.stmt_list_.head_ = &first;
    const int ret = part_mgr_.prepare_mview_create(task, 20, 1000000);
    task.stmt_list_.head_ = nullptr;
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_TRUE(task.get_mview_mappings().empty());
    EXPECT_TRUE(task.get_mview_containers().empty());
  }
}

TEST_F(TestMViewPartMgr, alter_preserves_pending_mview_state_and_regular_table_updates)
{
  add_mview_set();
  TCTX.refresh_mode_ = DATA_DICT;
  ObLogConfig config;
  ASSERT_EQ(OB_SUCCESS, config.init());
  ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.baseline_loader_.init(config));
  ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.baseline_loader_.add_tenant(1001));
  ObDictTenantInfoGuard guard;
  ASSERT_EQ(OB_SUCCESS, GLOGMETADATASERVICE.get_tenant_info_guard(1001, guard));
  ObDictTenantInfo *runtime_dict = guard.get_tenant_info();
  runtime_dict->get_dict_tenant_meta().set_tenant_id(1001);
  ASSERT_EQ(OB_SUCCESS, runtime_dict->update_tenant_name("tenant"));
  ObDictDatabaseMeta *database_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, dict_.db_map_.get(MetaDataKey(500000), database_meta));
  ASSERT_EQ(OB_SUCCESS, runtime_dict->replace_dict_db_meta(*database_meta));
  for (int64_t i = 0; i < tables_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, runtime_dict->replace_dict_table_meta(*tables_.at(i)));
  }
  PartTransTask task;
  task.set_allocator(8192, allocator_);
  MemtableMutatorRow row(allocator_);
  DdlStmtTask alter(task, row);
  const uint64_t mview_ids[] = {500002, 500003, 500004, 500005};
  for (int lists = 0; lists < 2; ++lists) {
    for (int output = 0; output < 2; ++output) {
      init_mgr(lists, output);
      for (int64_t i = 0; i < ARRAYSIZEOF(mview_ids); ++i) {
        EXPECT_EQ(OB_SUCCESS, part_mgr_.alter_table(mview_ids[i], alter, 20, 1000000));
        EXPECT_FALSE(task.need_update_table_id_cache());
        EXPECT_FALSE(task.need_update_mview_cache());
      }
      EXPECT_EQ(OB_SUCCESS, part_mgr_.alter_table(500001, alter, 20, 1000000));
      EXPECT_TRUE(task.need_update_table_id_cache());
      task.tic_update_infos_.reuse();
      EXPECT_FALSE(part_mgr_.has_mview_mapping(500003));
    }
  }
  init_mgr(true, true);
  const char *tenant_name = nullptr;
  const char *database_name = nullptr;
  const char *table_name = nullptr;
  bool is_user_table = false;
  bool chosen = false;
  uint64_t database_id = OB_INVALID_ID;
  uint64_t container_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, part_mgr_.table_match_(500003, 20, tenant_name, database_name,
      table_name, is_user_table, chosen, database_id, 1000000, container_id));
  EXPECT_EQ(500003, container_id);
  EXPECT_FALSE(chosen);
  EXPECT_FALSE(part_mgr_.has_mview_mapping(500003));
  EXPECT_EQ(OB_SUCCESS, part_mgr_.table_match_(500001, 20, tenant_name, database_name,
      table_name, is_user_table, chosen, database_id, 1000000, container_id));
  EXPECT_EQ(OB_INVALID_ID, container_id);
  EXPECT_TRUE(chosen);

  baseline();
  const char *patterns[] = {"tenant.db.sales_summary", "tenant.db.sales"};
  for (int64_t i = 0; i < ARRAYSIZEOF(patterns); ++i) {
    set_matcher(patterns[i]);
    ASSERT_EQ(OB_SUCCESS, part_mgr_.rename_table(500003, alter, 20, 1000000));
    ASSERT_EQ(1, task.tic_update_infos_.count());
    EXPECT_EQ(0 == i ? TICUpdateInfo::RENAME_TABLE_ADD : TICUpdateInfo::RENAME_TABLE_REMOVE,
        task.tic_update_infos_.at(0).reason_);
    task.tic_update_infos_.reuse();
    ASSERT_EQ(OB_SUCCESS, part_mgr_.recover_table_end(500003, alter, 20, 1000000));
    EXPECT_EQ(0 == i ? 1 : 0, task.tic_update_infos_.count());
    task.tic_update_infos_.reuse();
  }
}

class MViewTestSchemaGuard : public ObLogSchemaGuard
{
public:
  explicit MViewTestSchemaGuard(const ObSimpleTableSchemaV2 &schema) : schema_(schema) {}
  int get_table_schema(const uint64_t tenant_id, const uint64_t table_id,
      const ObSimpleTableSchemaV2 *&table_schema, const int64_t timeout) override
  {
    table_schema = table_id == schema_.get_table_id() ? &schema_ : nullptr;
    return OB_SUCCESS;
  }
private:
  const ObSimpleTableSchemaV2 &schema_;
};

TEST_F(TestMViewPartMgr, online_and_dictionary_collect_same_mapping_and_only_mv_enters_baseline)
{
  init_mgr(true, true);
  add_schema(500003, "__mv_container_500002", USER_TABLE, OB_INVALID_ID, true);
  ObTableSchema container_schema;
  container_schema.set_table_id(500003);
  container_schema.set_table_type(USER_TABLE);
  container_schema.set_mv_container_table(IS_MV_CONTAINER_TABLE);
  MViewTestSchemaGuard schema_guard(container_schema);
  ObTableSchema schema;
  schema.set_tenant_id(1001);
  schema.set_database_id(500000);
  schema.set_table_id(500002);
  schema.set_data_table_id(500003);
  schema.set_table_type(MATERIALIZED_VIEW);
  ASSERT_EQ(OB_SUCCESS, schema.set_table_name("sales_summary"));
  add_key_column(schema);
  ObDictTableMeta dict_schema(&allocator_);
  ASSERT_EQ(OB_SUCCESS, dict_schema.init(schema));
  ObArray<ObLogMViewInfo> online;
  ObArray<ObLogMViewInfo> dictionary;
  ObArray<ObLogMViewContainerInfo> containers;
  ASSERT_EQ(OB_SUCCESS, part_mgr_.collect_mview_schema_(static_cast<const ObSimpleTableSchemaV2 &>(schema),
      static_cast<ObLogSchemaGuard &>(schema_guard), 1000000, allocator_, online, containers));
  ASSERT_EQ(OB_SUCCESS, part_mgr_.collect_mview_schema_(dict_schema, dict_, 1000000,
      allocator_, dictionary, containers));
  ASSERT_EQ(1, online.count());
  ASSERT_EQ(1, dictionary.count());
  EXPECT_EQ(online.at(0).container_table_id_, dictionary.at(0).container_table_id_);
  EXPECT_EQ(online.at(0).mview_id_, dictionary.at(0).mview_id_);
  EXPECT_EQ(online.at(0).mview_name_, dictionary.at(0).mview_name_);
  ObDataDictService service;
  bool filtered = true;
  ASSERT_EQ(OB_SUCCESS, service.filter_table_(schema, filtered));
  EXPECT_FALSE(filtered);
  schema.set_table_type(USER_VIEW);
  ASSERT_EQ(OB_SUCCESS, service.filter_table_(schema, filtered));
  EXPECT_TRUE(filtered);
  online.reuse();
  ASSERT_EQ(OB_SUCCESS, part_mgr_.collect_mview_schema_(static_cast<const ObSimpleTableSchemaV2 &>(schema),
      static_cast<ObLogSchemaGuard &>(schema_guard), 1000000, allocator_, online, containers));
  EXPECT_EQ(0, online.count());
}


}
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  if (nullptr == ObLogInstance::get_instance()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ret = RUN_ALL_TESTS();
  }
  ObLogInstance::destroy_instance();
  return ret;
}
