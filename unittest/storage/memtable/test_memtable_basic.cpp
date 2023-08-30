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

#include <cstdint>
#include <gtest/gtest.h>

#define private public
#define protected public
#include "lib/ob_errno.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_freezer.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "share/scn.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "share/schema/ob_column_schema.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"

namespace oceanbase
{
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace transaction;

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag ,
                                 const transaction::ObTxSEQ ,
                                 const transaction::ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const transaction::ObTxSEQ ,
                                 const transaction::ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const transaction::ObTxSEQ )
{
  return OB_SUCCESS;
}
} // concurrent_control

namespace common
{
// override the function
int ObGMemstoreAllocator::set_memstore_threshold_without_lock(uint64_t tenant_id)
{
  int64_t memstore_threshold = INT64_MAX;
  arena_.set_memstore_threshold(memstore_threshold);
  return OB_SUCCESS;
}
void* ObGMemstoreAllocator::alloc(AllocHandle& handle, int64_t size)
{
  int64_t align_size = upper_align(size, sizeof(int64_t));
  if (!handle.is_id_valid()) {
    COMMON_LOG(TRACE, "MTALLOC.first_alloc", KP(&handle.mt_));
    LockGuard guard(lock_);
    if (!handle.is_id_valid()) {
      handle.set_clock(arena_.retired());
      hlist_.set_active(handle);
    }
  }
  if (arena_.allocator_ == nullptr) {
    if (arena_.init(OB_SERVER_TENANT_ID) != OB_SUCCESS) {
      abort();
    }
  }
  return arena_.alloc(handle.id_, handle.arena_handle_, align_size);
}

} // end common

namespace storage
{
// override the function make it do nothing
int ObTenantFreezer::unset_tenant_slow_freeze(const common::ObTabletID &tablet_id)
{
  UNUSED(tablet_id);
  return OB_SUCCESS;
}

int ObTxTableGuard::init(ObTxTable *tx_table)
{
  tx_table_ = tx_table;
  epoch_ = 0;
  return OB_SUCCESS;
}

int ObReadInfoStruct::init_compat_version()
{
  compat_version_ = READ_INFO_VERSION_V1;
  return OB_SUCCESS;
}

} // end storage

namespace memtable
{
int ObMvccRow::check_double_insert_(const share::SCN ,
                                    ObMvccTransNode &,
                                    ObMvccTransNode *)
{
  return OB_SUCCESS;
}
}

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag ,
                                 const int64_t ,
                                 const ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const int64_t ,
                                 const ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const int64_t )
{
  return OB_SUCCESS;
}

} // end memtable

class TestMemtable : public testing::Test
{
public:
  TestMemtable() : tenant_base_(1),tablet_id_(1000),rowkey_cnt_(1) { freezer_.init(&ls_); }
  void SetUp() override {
    share::ObTenantEnv::set_tenant(&tenant_base_);
    // mock columns
    EXPECT_EQ(OB_SUCCESS, mock_col_desc());
    // mock iterator parameter
    EXPECT_EQ(OB_SUCCESS, mock_iter_param());
  }
  void TearDown() override {
    reset_iter_param();
    columns_.reset();
    share::ObTenantEnv::set_tenant(nullptr);
  }
  int init_memtable(ObMemtable &mt_table)
  {
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::DATA_MEMTABLE;
    table_key.tablet_id_ = ObTabletID(tablet_id_.id());
    table_key.scn_range_.start_scn_ = share::SCN::base_scn();
    table_key.scn_range_.end_scn_ = share::SCN::max_scn();
    int64_t schema_version  = 1;
    uint32_t freeze_clock = 0;
    ObLSHandle ls_handle;
    ls_handle.set_ls(ls_map_, ls_, ObLSGetMod::DATA_MEMTABLE_MOD);

    return mt_table.init(table_key, ls_handle, &freezer_, &memtable_mgr_, schema_version, freeze_clock);
  }
  int mock_col_desc()
  {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
    col_desc.col_type_.set_type(ObIntType);
    col_desc.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    columns_.push_back(col_desc);

    share::schema::ObColDesc col_desc2;
    col_desc2.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
    col_desc2.col_type_.set_type(ObIntType);
    col_desc2.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    columns_.push_back(col_desc2);

    return OB_SUCCESS;
  }
  int mock_row(const int64_t key,
               const int64_t value,
               ObDatumRowkey &rowkey,
               ObStoreRow &row)
  {
    rowkey_datums_[0].set_int(key);
    //rowkey_datums_[1].set_int(value);
    rowkey.assign(rowkey_datums_, 1);

    ObObj *obj = new ObObj[2];
    obj[0].set_int(key);
    obj[1].set_int(value);

    row.row_val_.cells_ = obj;
    row.row_val_.count_ = 2;
    row.row_val_.projector_ = NULL;
    row.flag_.set_flag(ObDmlFlag::DF_INSERT);
    rowkey.store_rowkey_.assign(obj, 1);

    return OB_SUCCESS;
  }
  int mock_iter_param()
  {
    // iter_param_.rowkey_cnt_ = rowkey_cnt_;
    iter_param_.tablet_id_ = tablet_id_;
    iter_param_.table_id_ = tablet_id_.id();
    int ret = read_info_.init(allocator_, 16000, rowkey_cnt_, false, columns_, nullptr/*storage_cols_index*/);
    iter_param_.read_info_ = &read_info_;

    return ret;
  }

  void prepare_schema(share::schema::ObTableSchema &table_schema)
  {
    int ret = OB_SUCCESS;
    int64_t micro_block_size = 16 * 1024;
    const uint64_t tenant_id = 1;
    const uint64_t table_id = 777;
    share::schema::ObColumnSchemaV2 column;

    //generate data table schema
    table_schema.reset();
    ret = table_schema.set_table_name("test_merge_multi_version");
    ASSERT_EQ(OB_SUCCESS, ret);
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_tablegroup_id(1);
    table_schema.set_database_id(1);
    table_schema.set_table_id(table_id);
    table_schema.set_rowkey_column_num(3);
    table_schema.set_max_used_column_id(6);
    table_schema.set_block_size(micro_block_size);
    table_schema.set_compress_func_name("none");
    table_schema.set_row_store_type(FLAT_ROW_STORE);
    //init column
    char name[OB_MAX_FILE_NAME_LENGTH];
    memset(name, 0, sizeof(name));
    const int64_t column_ids[] = {16,17,20,21,22,23,24,29};
    for(int64_t i = 0; i < 6; ++i){
      ObObjType obj_type = ObIntType;
      const int64_t column_id = column_ids[i];

      if (i == 1) {
        obj_type = ObVarcharType;
      }
      column.reset();
      column.set_table_id(table_id);
      column.set_column_id(column_id);
      sprintf(name, "test%020ld", i);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
      column.set_data_type(obj_type);
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_data_length(10);
      if (i < 3) {
        column.set_rowkey_position(i + 1);
      } else {
        column.set_rowkey_position(0);
      }
      COMMON_LOG(INFO, "add column", K(i), K(column));
      ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
    }
    COMMON_LOG(INFO, "dump stable schema", K(table_schema));
  }

  void reset_iter_param()
  {
    iter_param_.reset();
    read_info_.reset();
  }
public:
  ObLS ls_;
  share::ObTenantBase tenant_base_;
  storage::ObFreezer freezer_;
  storage::ObTabletMemtableMgr memtable_mgr_;
  ObTabletID tablet_id_;
  const int64_t rowkey_cnt_;
  ObTableIterParam iter_param_;
  ObSEArray<share::schema::ObColDesc, 2> columns_;
  ObStorageDatum rowkey_datums_[2];
  ObTableReadInfo read_info_;
  ObArenaAllocator allocator_;
  MemtableIDMap ctx_map_;
  ObLSMap ls_map_;
};

class RunCtxGuard
{
public:
  int init(int64_t trans_id, TestMemtable *tm) {
    tm_ = tm;
    trans_ctx_.trans_id_ = ObTransID(trans_id);
    mem_ctx_.set_trans_ctx(&trans_ctx_);
    return mem_ctx_.init(MTL_ID());
  }

  int write(int64_t key, int64_t val, ObMemtable &mt, ObDatumRowkey &row_key, int64_t snapshot_version = 1000) {
    ObStoreCtx store_ctx;
    ObTxSnapshot snapshot;
    ObTxTableGuard tx_table_guard;
    concurrent_control::ObWriteFlag write_flag;
    tx_table_guard.init((ObTxTable*)0x100);
    snapshot.version_.convert_for_gts(snapshot_version);
    store_ctx.mvcc_acc_ctx_.init_write(trans_ctx_,
                                       mem_ctx_,
                                       tx_desc_.tx_id_,
                                       ObTxSEQ(1000, 0),
                                       tx_desc_,
                                       tx_table_guard,
                                       snapshot,
                                       INT64_MAX,
                                       INT64_MAX,
                                       write_flag);
    ObTableStoreIterator table_iter;
    store_ctx.table_iter_ = &table_iter;
    ObStoreRow write_row;
    tm_->mock_row(key, val, row_key, write_row);

    ObArenaAllocator allocator;
    ObTableAccessContext context;
    ObVersionRange trans_version_range;
    const bool read_latest = true;
    ObQueryFlag query_flag;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
    query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

    context.init(query_flag, store_ctx, allocator, trans_version_range);

    return mt.set_(tm_->iter_param_, context, tm_->columns_, write_row, NULL, NULL);
  }
  int write(int64_t key, int64_t val, ObMemtable &mt, int64_t snapshot_version = 1000) {
    ObDatumRowkey row_key;
    return write(key, val, mt, row_key, snapshot_version);
  }
  int write(int64_t key, int64_t val, ObMemtable &mt, ObMvccRow *&mvcc_row, int64_t snapshot_version = 1000) {
    int ret = OB_SUCCESS;
    ObDatumRowkey row_key;
    ObMemtableKey mtk;
    ObMemtableKey stored_key;
    if (OB_FAIL(ret = write(key, val, mt, row_key, snapshot_version))) {
    } else if (OB_FAIL(mtk.encode(tm_->columns_, &row_key.get_store_rowkey()))) {
    } else if (OB_FAIL(mt.query_engine_.get(&mtk, mvcc_row, &stored_key))) {
    }
    return ret;
  }
  int read(int64_t key, int64_t &val, ObMemtable &mt, int64_t tx_id = -1, int64_t snapshot = INT64_MAX) {
    int ret = OB_SUCCESS;
    ObStorageDatum rowkey_datums[2];
    ObDatumRowkey row_key;
    rowkey_datums[0].set_int(key);
    //rowkey_datums_[1].set_int(value);
    row_key.assign(rowkey_datums, 1);
    ObMemtableKey mtk;
    ObMemtableKey stored_key;
    ObMvccRow *mvcc_row = nullptr;
    if (OB_FAIL(mtk.encode(tm_->columns_, &row_key.get_store_rowkey()))) {
    } else if (OB_FAIL(mt.query_engine_.get(&mtk, mvcc_row, &stored_key))) {
    } else {
      auto trans_node = mvcc_row->list_head_;
      while (trans_node != nullptr) {
        if (trans_node->is_aborted()) {
          trans_node = trans_node->prev_;
        } else if (trans_node->is_committed()) {
          if (trans_node->trans_version_.get_val_for_logservice() <= snapshot) {
            break;
          } else {
            trans_node = trans_node->prev_;
          }
        } else if (trans_node->tx_id_.get_id() == tx_id) {
          // skip sql_no compare
          //if (trans_node->seq_no__ <= snapshot) {
          break;
        } else {
          if (snapshot < trans_node->trans_version_.get_val_for_logservice()) {
            trans_node = trans_node->prev_;
          } else {
            ret = OB_ERR_SHARED_LOCK_CONFLICT;
            break;
          }
        }
        if (OB_SUCC(ret)) {
          if (trans_node != nullptr) {
            //val = trans_node->buf_;
          } else {
            ret = OB_ENTRY_NOT_EXIST;
          }
        }
      }
    }
    return ret;
  }

  TestMemtable *tm_;
  ObPartTransCtx trans_ctx_;
  ObMemtableCtx mem_ctx_;
  ObTxDesc tx_desc_;
};

void print(ObMvccRow *mvcc_row)
{
  printf("-----------mvcc row %p------------------\n", mvcc_row);
  ObMvccTransNode *node = mvcc_row->get_list_head();
  while (node != nullptr) {
    printf("%p tx_id:%ld trans_version:%ld log_ts:%ld prev:%p next:%p version:%ld\n",
           node, node->tx_id_.get_id(), node->trans_version_.get_val_for_logservice(),
           node->scn_.get_val_for_logservice(), node->prev_, node->next_, node->version_);
    node = node->prev_;
  }
  printf("\n");
}

// test for init memtable
TEST_F(TestMemtable, init_mt)
{
  ObMemtable mt_table;
  EXPECT_EQ(OB_SUCCESS, init_memtable(mt_table));
}

TEST_F(TestMemtable, mt_set)
{
  ObMemtable mt;
  EXPECT_EQ(OB_SUCCESS, init_memtable(mt));

  RunCtxGuard rg;
  EXPECT_EQ(OB_SUCCESS, rg.init(1, this));

  ObMvccRow *mvcc_row = nullptr;
  EXPECT_EQ(OB_SUCCESS, rg.write(1, 2, mt, mvcc_row));

  print(mvcc_row);
  EXPECT_EQ(1, rg.mem_ctx_.trans_mgr_.get_main_list_length());

  EXPECT_EQ(OB_SUCCESS, rg.write(1, 3, mt));

  print(mvcc_row);
  EXPECT_EQ(2, rg.mem_ctx_.trans_mgr_.get_main_list_length());

  share::SCN val_1000;
  val_1000.convert_for_logservice(1000);
  EXPECT_EQ(OB_SUCCESS, rg.mem_ctx_.do_trans_end(true, val_1000, val_1000, 0));
  print(mvcc_row);
}

TEST_F(TestMemtable, conflict)
{
  ObMemtable mt;
  EXPECT_EQ(OB_SUCCESS, init_memtable(mt));

  RunCtxGuard rg;
  EXPECT_EQ(OB_SUCCESS, rg.init(1, this));

  ObMvccRow *mvcc_row = nullptr;
  EXPECT_EQ(OB_SUCCESS, rg.write(1, 2, mt, mvcc_row));

  RunCtxGuard rg2;
  EXPECT_EQ(OB_SUCCESS, rg2.init(2, this));
  EXPECT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, rg2.write(1, 3, mt));

  share::SCN val_1000;
  val_1000.convert_for_logservice(1000);
  EXPECT_EQ(OB_SUCCESS, rg.mem_ctx_.do_trans_end(true, val_1000, val_1000, 0));

  EXPECT_EQ(OB_TRANSACTION_SET_VIOLATION, rg2.write(1, 3, mt, 900));
  EXPECT_EQ(OB_SUCCESS, rg2.write(1, 3, mt, 1000));
  EXPECT_EQ(OB_SUCCESS, rg2.write(1, 4, mt, 1001));

  share::SCN val_1002;
  val_1002.convert_for_logservice(1002);
  EXPECT_EQ(OB_SUCCESS, rg2.mem_ctx_.do_trans_end(true, val_1002, val_1002, 0));

  print(mvcc_row);
}

TEST_F(TestMemtable, except)
{
  ObMemtable mt;
  EXPECT_EQ(OB_SUCCESS, init_memtable(mt));

  RunCtxGuard rg;
  EXPECT_EQ(OB_SUCCESS, rg.init(1, this));

  ObMvccRow *mvcc_row = nullptr;
  share::SCN val_900;
  val_900.convert_for_logservice(900);

  EXPECT_EQ(OB_SUCCESS, rg.write(1, 2, mt, mvcc_row, 1000));
  EXPECT_EQ(OB_SUCCESS, rg.mem_ctx_.do_trans_end(true, val_900, val_900, 0));

  RunCtxGuard rg2;
  EXPECT_EQ(OB_SUCCESS, rg2.init(2, this));

  EXPECT_EQ(OB_SUCCESS, rg2.write(1, 3, mt, 1000));
  EXPECT_EQ(OB_SUCCESS, rg2.mem_ctx_.do_trans_end(true, val_900, val_900, 0));
  print(mvcc_row);

  RunCtxGuard rg3;
  EXPECT_EQ(OB_SUCCESS, rg3.init(3, this));

  EXPECT_EQ(OB_SUCCESS, rg3.write(1, 4, mt, 900));
  EXPECT_EQ(OB_SUCCESS, rg3.mem_ctx_.do_trans_end(true, val_900, val_900, 0));
  print(mvcc_row);
}


TEST_F(TestMemtable, multi_key)
{
  ObMemtable mt;
  EXPECT_EQ(OB_SUCCESS, init_memtable(mt));

  RunCtxGuard rg;
  EXPECT_EQ(OB_SUCCESS, rg.init(1, this));

  share::SCN val_900;
  val_900.convert_for_logservice(900);

  ObMvccRow *mvcc_row = nullptr;
  ObMvccRow *mvcc_row2 = nullptr;
  EXPECT_EQ(OB_SUCCESS, rg.write(1, 10, mt, mvcc_row, 1000));
  EXPECT_EQ(OB_SUCCESS, rg.write(2, 20, mt, mvcc_row2, 1000));
  EXPECT_EQ(OB_SUCCESS, rg.mem_ctx_.do_trans_end(true, val_900, val_900, 0));
  print(mvcc_row);
  print(mvcc_row2);


  RunCtxGuard rg2;
  EXPECT_EQ(OB_SUCCESS, rg2.init(2, this));

  EXPECT_EQ(OB_SUCCESS, rg2.write(1, 100, mt, 1000));
  EXPECT_EQ(OB_SUCCESS, rg2.write(2, 200, mt, 1000));
  EXPECT_EQ(OB_SUCCESS, rg2.mem_ctx_.do_trans_end(true, val_900, val_900, 0));
  print(mvcc_row);
  print(mvcc_row2);
}


}// end of oceanbase




int main(int argc, char **argv)
{
  const char* log_file_name = "test_memtable_basic.log";
  system("rm -rf test_memtable_basic.log*");
  OB_LOGGER.set_file_name(log_file_name, true, false, log_file_name, log_file_name);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
