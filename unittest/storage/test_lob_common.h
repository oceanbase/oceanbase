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

#define protected public
#define private public
#include "test_dml_common.h"

namespace oceanbase
{
namespace storage
{

class TestLobCommon
{
public:
  static int create_data_tablet(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id);
  static void build_data_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
  static void build_lob_meta_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
  static int build_lob_meta_table_scan_param(
      const uint64_t tenant_id,
      ObTxDesc *tx_desc,
      const share::schema::ObTableParam &table_param,
      ObTableScanParam &scan_param);
  static void build_lob_piece_table_schema(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &table_schema);
protected:
  static int build_lob_tablet_arg(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &data_tablet_id,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id,
      obrpc::ObBatchCreateTabletArg &arg);
public:
  static const uint64_t TX_LOB_EXPIRE_TIME_US = 120 * 1000 * 1000; // 120s
  static const int64_t TEST_LOB_LS_ID = 2;
  static const uint64_t TEST_LOB_TABLE_ID = 50;
  static const uint64_t TEST_LOB_META_TABLE_ID = 51;
  static const uint64_t TEST_LOB_PIECE_TABLE_ID = 52;
};

int TestLobCommon::create_data_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &lob_meta_tablet_id,
    const common::ObTabletID &lob_piece_tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  obrpc::ObBatchCreateTabletArg arg;

  if (OB_FAIL(TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle))) {
    STORAGE_LOG(WARN, "failed to create ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(build_lob_tablet_arg(tenant_id, ls_id, tablet_id, lob_meta_tablet_id, lob_piece_tablet_id, arg))) {
    STORAGE_LOG(WARN, "failed to build lob tablets arg", K(ret),
        K(tenant_id), K(ls_id), K(tablet_id));
  } else {
    transaction::ObMulSourceDataNotifyArg trans_flags;
    trans_flags.tx_id_ = 123;
    trans_flags.scn_ = share::SCN::invalid_scn();
    trans_flags.for_replay_ = false;

    ObLS *ls = ls_handle.get_ls();
    if (OB_FAIL(ls->get_tablet_svr()->on_prepare_create_tablets(arg, trans_flags))) {
      STORAGE_LOG(WARN, "failed to prepare create tablets", K(ret), K(arg));
    } else if (FALSE_IT(trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100))) {
    } else if (OB_FAIL(ls->get_tablet_svr()->on_redo_create_tablets(arg, trans_flags))) {
      STORAGE_LOG(WARN, "failed to redo create tablets", K(ret), K(arg));
    } else if (FALSE_IT(trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1))) {
    } else if (OB_FAIL(ls->get_tablet_svr()->on_tx_end_create_tablets(arg, trans_flags))) {
      STORAGE_LOG(WARN, "failed to tx end create tablets", K(ret), K(arg));
    } else if (FALSE_IT(trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1))) {
    } else if (OB_FAIL(ls->get_tablet_svr()->on_commit_create_tablets(arg, trans_flags))) {
      STORAGE_LOG(WARN, "failed to commit create tablets", K(ret), K(arg));
    }
  }

  return ret;
}

void TestLobCommon::build_data_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t table_id = TEST_LOB_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  table_schema.reset();
  table_schema.set_table_name("test_lob_common");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_schema_version(share::OB_CORE_SCHEMA_VERSION + 1);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);

#define ADD_COLUMN(column_id, column_name, data_type, collation_type, is_row_key) \
  { \
    ObColumnSchemaV2 column; \
    column.set_tenant_id(tenant_id); \
    column.set_column_id(column_id); \
    column.set_column_name(column_name); \
    column.set_data_type(data_type); \
    column.set_collation_type(collation_type); \
    if (is_row_key) { \
      column.set_rowkey_position(1); \
    } \
    table_schema.add_column(column); \
  }

  // table schema
  // a(bigint)  b(bigint)  c(bigint)  d(longtext)  e(varchar)
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 0, "a", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, true);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 1, "b", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 2, "c", ObIntType, CS_TYPE_UTF8MB4_GENERAL_CI, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 3, "d", ObLongTextType, CS_TYPE_UTF8MB4_BIN, false);
  ADD_COLUMN(OB_APP_MIN_COLUMN_ID + 4, "e", ObVarcharType, CS_TYPE_UTF8MB4_BIN, false);
#undef ADD_COLUMN
}

void TestLobCommon::build_lob_meta_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t data_table_id = TEST_LOB_TABLE_ID;
  const uint64_t lob_meta_table_id = TEST_LOB_META_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  table_schema.reset();
  table_schema.set_table_name("test_lob_meta_common");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_data_table_id(data_table_id);
  table_schema.set_table_id(lob_meta_table_id);
  table_schema.set_table_type(AUX_LOB_META);
  table_schema.set_schema_version(share::OB_CORE_SCHEMA_VERSION + 1);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);

  // add lob_id
  {
    ObColumnSchemaV2 lob_id_col;
    lob_id_col.set_tenant_id(tenant_id);
    lob_id_col.set_column_id(common::OB_APP_MIN_COLUMN_ID);
    lob_id_col.set_column_name("lob_id");
    lob_id_col.set_rowkey_position(1);
    lob_id_col.set_data_type(ObVarcharType);
    lob_id_col.set_collation_type(CS_TYPE_BINARY);
    lob_id_col.set_data_length(sizeof(ObLobId));
    table_schema.add_column(lob_id_col);
  }
  // seq_id
  {
    ObColumnSchemaV2 seq_id_col;

    seq_id_col.set_tenant_id(tenant_id);
    seq_id_col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 1);
    seq_id_col.set_column_name("seq_id");
    seq_id_col.set_rowkey_position(2); // set
    seq_id_col.set_data_type(ObVarcharType);
    seq_id_col.set_collation_type(CS_TYPE_BINARY);
    seq_id_col.set_data_length(8192);
    table_schema.add_column(seq_id_col);
  }
  // bin_len
  {
    ObColumnSchemaV2 bin_len_col;
    bin_len_col.set_tenant_id(tenant_id);
    bin_len_col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 2);
    bin_len_col.set_column_name("binary_len");
    bin_len_col.set_data_type(ObUInt32Type);
    bin_len_col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(bin_len_col);
  }
  // char_len
  {
    ObColumnSchemaV2 char_len_col;
    char_len_col.set_tenant_id(tenant_id);
    char_len_col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 3);
    char_len_col.set_column_name("char_len");
    char_len_col.set_data_type(ObUInt32Type);
    char_len_col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(char_len_col);
  }
  // piece_id
  {
    ObColumnSchemaV2 piece_id_col;
    piece_id_col.set_tenant_id(tenant_id);
    piece_id_col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 4);
    piece_id_col.set_column_name("piece_id");
    piece_id_col.set_data_type(ObUInt64Type);
    piece_id_col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(piece_id_col);
  }
  // seq_id
  {
    ObColumnSchemaV2 lob_data_col;

    lob_data_col.set_tenant_id(tenant_id);
    lob_data_col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 5);
    lob_data_col.set_column_name("lob_data");
    lob_data_col.set_data_type(ObVarcharType);
    lob_data_col.set_collation_type(CS_TYPE_BINARY);
    lob_data_col.set_data_length(256*1024); // TODO 16k is too big?
    table_schema.add_column(lob_data_col);
  }
}

void TestLobCommon::build_lob_piece_table_schema(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &table_schema)
{
  const uint64_t data_table_id = TEST_LOB_TABLE_ID;
  const uint64_t lob_piece_table_id = TEST_LOB_PIECE_TABLE_ID;
  const int64_t micro_block_size = 16 * 1024;

  table_schema.reset();
  table_schema.set_table_name("test_lob_piece_common");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_data_table_id(data_table_id);
  table_schema.set_table_id(lob_piece_table_id);
  table_schema.set_table_type(AUX_LOB_PIECE);
  table_schema.set_schema_version(share::OB_CORE_SCHEMA_VERSION + 1);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_max_used_column_id(ObObjType::ObExtendType - 1);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::ENCODING_ROW_STORE);
  table_schema.set_storage_format_version(ObStorageFormatVersion::OB_STORAGE_FORMAT_VERSION_V4);

  // add piece_id
  {
    ObColumnSchemaV2 piece_id_col;
    piece_id_col.set_tenant_id(tenant_id);
    piece_id_col.set_column_id(common::OB_APP_MIN_COLUMN_ID);
    piece_id_col.set_column_name("piece_id");
    piece_id_col.set_rowkey_position(1);
    piece_id_col.set_order_in_rowkey(ObOrderType::ASC);
    piece_id_col.set_data_type(ObUInt64Type);
    piece_id_col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(piece_id_col);
  }
  // len
  {
    ObColumnSchemaV2 len_col;
    len_col.set_tenant_id(tenant_id);
    len_col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 1);
    len_col.set_column_name("len");
    len_col.set_data_type(ObUInt32Type);
    len_col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    table_schema.add_column(len_col);
  }
  // Macro_id
  {
    ObColumnSchemaV2 lob_data_col;
    lob_data_col.set_tenant_id(tenant_id);
    lob_data_col.set_column_id(common::OB_APP_MIN_COLUMN_ID + 2);
    lob_data_col.set_column_name("macro_id");
    // data_len_col.set_rowkey_position(1);
    // lob_data_col.set_order_in_rowkey(ObOrderType::ASC);
    // lob_data_col.set_data_length(100);
    lob_data_col.set_data_type(ObVarcharType);
    lob_data_col.set_collation_type(CS_TYPE_BINARY);
    lob_data_col.set_data_length(100); // TODO sizeof(MacroBlockId)
    table_schema.add_column(lob_data_col);
  }
}

int TestLobCommon::build_lob_tablet_arg(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &data_tablet_id,
    const common::ObTabletID &lob_meta_tablet_id,
    const common::ObTabletID &lob_piece_tablet_id,
    obrpc::ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;

  ObCreateTabletInfo tablet_info;
  ObArray<common::ObTabletID> tablet_id_array;
  ObArray<int64_t> tablet_schema_index_array;
  share::schema::ObTableSchema table_schema;
  build_data_table_schema(tenant_id, table_schema);
  share::schema::ObTableSchema lob_meta_schema;
  build_lob_meta_table_schema(tenant_id, lob_meta_schema);
  share::schema::ObTableSchema lob_piece_schema;
  build_lob_piece_table_schema(tenant_id, lob_piece_schema);

  arg.reset();
  if (OB_FAIL(tablet_id_array.push_back(data_tablet_id))) {
    STORAGE_LOG(WARN, "failed to push tablet id into array", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(tablet_id_array.push_back(lob_meta_tablet_id))) {
    STORAGE_LOG(WARN, "failed to push lob meta tablet id into array", K(ret), K(lob_meta_tablet_id));
  } else if (OB_FAIL(tablet_id_array.push_back(lob_piece_tablet_id))) {
    STORAGE_LOG(WARN, "failed to push lob meta tablet id into array", K(ret), K(lob_meta_tablet_id));
  } else if (OB_FAIL(tablet_schema_index_array.push_back(0))) {
    STORAGE_LOG(WARN, "failed to push index into array", K(ret));
  } else if (OB_FAIL(tablet_schema_index_array.push_back(1))) {
    STORAGE_LOG(WARN, "failed to push index into array", K(ret));
  } else if (OB_FAIL(tablet_schema_index_array.push_back(2))) {
    STORAGE_LOG(WARN, "failed to push index into array", K(ret));
  } else if (OB_FAIL(tablet_info.init(tablet_id_array, data_tablet_id, tablet_schema_index_array,
      lib::get_compat_mode(), false/*is_create_bind_hidden_tablets*/))) {
    STORAGE_LOG(WARN, "failed to init tablet info", K(ret), K(tablet_id_array),
        K(data_tablet_id), K(tablet_schema_index_array));
  } else if (OB_FAIL(arg.init_create_tablet(ls_id, share::SCN::min_scn(), false/*need_check_tablet_cnt*/))) {
    STORAGE_LOG(WARN, "failed to init create tablet", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(arg.table_schemas_.push_back(table_schema))) {
    STORAGE_LOG(WARN, "failed to push back table schema", K(ret), K(table_schema));
  } else if (OB_FAIL(arg.table_schemas_.push_back(lob_meta_schema))) {
    STORAGE_LOG(WARN, "failed to push back lob meta table schema", K(ret), K(lob_meta_schema));
  } else if (OB_FAIL(arg.table_schemas_.push_back(lob_piece_schema))) {
    STORAGE_LOG(WARN, "failed to push back lob meta table schema", K(ret), K(lob_piece_schema));
  } else if (OB_FAIL(arg.tablets_.push_back(tablet_info))) {
    STORAGE_LOG(WARN, "failed to push back tablet info", K(ret), K(tablet_info));
  }

  if (OB_FAIL(ret)) {
    arg.reset();
  }

  return ret;
}

int TestLobCommon::build_lob_meta_table_scan_param(
    const uint64_t tenant_id,
    ObTxDesc *tx_desc,
    const share::schema::ObTableParam &table_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  int64_t expire_time = ObTimeUtility::current_time() + TX_LOB_EXPIRE_TIME_US;
  const uint64_t table_id = TEST_LOB_META_TABLE_ID;

  for (int i = 0; i < ObLobMetaUtil::LOB_META_COLUMN_CNT; i++) {
    scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + i);
  }

  //scan_param.pkey_.part_id_ = share::ObLSID::SYS_LS_ID;
  //scan_param.pkey_.table_id_ = table_id;
  //scan_param.pkey_.assit_id_ = 0;

  scan_param.ls_id_ = TestDmlCommon::TEST_LS_ID;
  scan_param.tablet_id_ = TEST_LOB_META_TABLE_ID;

  scan_param.trans_desc_ = tx_desc;
  scan_param.table_param_ = &table_param;
  //scan_param.ref_table_id_ = table_id; // main table id
  scan_param.index_id_ = table_id; // table id
  scan_param.is_get_ = false;
  scan_param.timeout_ = expire_time;

  ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true // read_latest
                        );
  scan_param.scan_flag_.flag_ = query_flag.flag_;

  scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
  scan_param.allocator_ = &CURRENT_CONTEXT->get_arena_allocator();
  scan_param.for_update_ = false;
  scan_param.for_update_wait_timeout_ = expire_time;
  scan_param.sql_mode_ = SMO_DEFAULT;
  scan_param.scan_allocator_ = &CURRENT_CONTEXT->get_arena_allocator();
  scan_param.frozen_version_ = -1;
  scan_param.force_refresh_lc_ = false;
  scan_param.output_exprs_ = nullptr;
  scan_param.aggregate_exprs_ = nullptr;
  scan_param.op_ = nullptr;
  scan_param.row2exprs_projector_ = nullptr;
  scan_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  scan_param.limit_param_.limit_ = -1;
  scan_param.limit_param_.offset_ = 0;
  scan_param.need_scn_ = false;
  scan_param.pd_storage_flag_ = false;
  scan_param.fb_snapshot_.reset();

  ObNewRange range;
  range.table_id_ = table_id;
  range.start_key_.set_min_row();
  range.end_key_.set_max_row();
  scan_param.key_ranges_.push_back(range);

  return ret;
}


} // storage
} // oceanbase
