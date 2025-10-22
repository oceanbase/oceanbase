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
#define UNITTEST
#include "lib/container/ob_iarray.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "observer/ob_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tx/ob_mock_tx_ctx.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"

#include "common/cell/ob_cell_reader.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"

#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_partition_merge_iter.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/mockcontainer/mock_ob_merge_iterator.h"

#include "storage/memtable/utils_rowkey_builder.h"
#include "storage/memtable/utils_mock_row.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "storage/tx/ob_tx_data_define.h"
#include "share/scn.h"
#include "src/storage/column_store/ob_column_oriented_sstable.h"
#include "storage/column_store/ob_column_oriented_merger.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "unittest/storage/test_schema_prepare.h"
#include "test_merge_basic.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/memtable/ob_memtable_block_reader.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/column_store/ob_co_sstable_row_scanner.h"
#include "storage/column_store/ob_cg_scanner.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace memtable;
using namespace transaction;
using namespace palf;

namespace storage
{

ObSEArray<ObTxData, 8> TX_DATA_ARR;

int ObTxTable::insert(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  ret = TX_DATA_ARR.push_back(*tx_data);
  return ret;
}

int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < TX_DATA_ARR.count(); i++)
  {
    if (read_tx_data_arg.tx_id_ == TX_DATA_ARR.at(i).tx_id_) {
      if (TX_DATA_ARR.at(i).state_ == ObTxData::RUNNING) {
        SCN tmp_scn;
        tmp_scn.convert_from_ts(30);
        ObTxCCCtx tmp_ctx(ObTxState::PREPARE, tmp_scn);
        ret = fn(TX_DATA_ARR[i], &tmp_ctx);
      } else {
        ret = fn(TX_DATA_ARR[i]);
      }
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "check with tx data failed", KR(ret), K(read_tx_data_arg), K(TX_DATA_ARR.at(i)));
      }
      break;
    }
  }
  return ret;
}

void clear_tx_data()
{
  TX_DATA_ARR.reset();
};

class ObMockWhiteFilterExecutor : public ObWhiteFilterExecutor
{
public:
  ObMockWhiteFilterExecutor(common::ObIAllocator &alloc,
                            ObPushdownWhiteFilterNode &filter,
                            ObPushdownOperator &op) :
      ObWhiteFilterExecutor(alloc, filter, op)
  {}

  virtual int init_evaluated_datums(bool &is_valid) override
  {
    UNUSED(is_valid);
    return OB_SUCCESS;
  };
};

class ObMockMicroBlockRowScanner : public ObMicroBlockRowScanner
{
public:
  ObMockMicroBlockRowScanner(ObIAllocator &alloc) :
                             ObMicroBlockRowScanner(alloc)
  {}

  virtual int filter_pushdown_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor *filter,
      sql::PushdownFilterInfo &pd_filter_info,
      const bool can_use_vectorize,
      common::ObBitmap &bitmap) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == reader_ || nullptr == filter || !filter->is_filter_node())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(ret), KP(reader_), KPC(filter));
    } else if (filter->is_truncate_node()) {
      if (OB_FAIL(reader_->filter_pushdown_truncate_filter(parent, *filter, pd_filter_info, bitmap))) {
        LOG_WARN("Failed to pushdown truncate scn filter", K(ret));
      }
    } else if (filter->is_sample_node()) {
      if (ObIMicroBlockReader::MemtableReader == reader_->get_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("memtable does not support sample pushdown", KR(ret), KP(this), KP(reader_));
      } else if (OB_FAIL(static_cast<ObSampleFilterExecutor *>(filter)->apply_sample_filter(
                  pd_filter_info,
                  sstable_->is_major_sstable(),
                  bitmap))) {
        LOG_WARN("Failed to execute sample pushdown filter", K(ret));
      }
    // use uniform base currently, support new format later
    // TODO(hanling): If the new vectorization format does not start counting from the 0th row, it can be computed in batches.
    } else if (can_use_vectorize &&
              filter->get_op().enable_rich_format_ &&
              OB_FAIL(init_exprs_uniform_header(filter->get_cg_col_exprs(),
                                                filter->get_op().get_eval_ctx(),
                                                filter->get_op().get_eval_ctx().max_batch_size_))) {
      LOG_WARN("Failed to init exprs vector header", K(ret));
    } else {
      switch (reader_->get_type())
      {
        case ObIMicroBlockReader::Decoder:
        case ObIMicroBlockReader::CSDecoder: {
          ObIMicroBlockDecoder *decoder = static_cast<ObIMicroBlockDecoder *>(reader_);
          // change to black filter in below situation
          // 1. if need project lob column and micro block has outrow lob
          // 2. if it is semistruct_filter_node, but not cs decoder (beacuase semistrcut white filter only support cs decoder)
          if ((param_->has_lob_column_out() && reader_->has_lob_out_row())
              || (filter->is_semistruct_filter_node() && ObIMicroBlockReader::CSDecoder != reader_->get_type())) {
            sql::ObPhysicalFilterExecutor *physical_filter = static_cast<sql::ObPhysicalFilterExecutor *>(filter);
            if (OB_FAIL(decoder->filter_pushdown_filter(parent,
                                                        *physical_filter,
                                                        pd_filter_info,
                                                        bitmap))) {
              LOG_WARN("Failed to execute pushdown filter", K(ret));
            }
          } else if (filter->is_filter_black_node()) {
            sql::ObBlackFilterExecutor *black_filter = static_cast<sql::ObBlackFilterExecutor *>(filter);
            if (can_use_vectorize && black_filter->can_vectorized()) {
              if (OB_FAIL(apply_filter_batch(
                          parent,
                          *black_filter,
                          pd_filter_info,
                          bitmap))) {
                LOG_WARN("Failed to execute black pushdown filter in batch", K(ret));
              }
            } else if (OB_FAIL(decoder->filter_pushdown_filter(
                        parent,
                        *black_filter,
                        pd_filter_info,
                        bitmap))) {
              LOG_WARN("Failed to execute black pushdown filter", K(ret));
            }
          } else {
            if (OB_FAIL(decoder->filter_pushdown_filter(
                        parent,
                        *static_cast<sql::ObWhiteFilterExecutor *>(filter),
                        pd_filter_info,
                        bitmap))) {
              LOG_WARN("Failed to execute white pushdown filter", K(ret));
            }
          }
          break;
        }

        case ObIMicroBlockReader::MemtableReader: {
          if (OB_FAIL(
                  memtable_reader_->filter_pushdown_filter(parent, *filter, pd_filter_info, bitmap))) {
            LOG_WARN("failed to execute memtable pushdown filter");
          }
          break;
        }

        case ObIMicroBlockReader::Reader:
        case ObIMicroBlockReader::NewFlatReader: {
          if (OB_FAIL(reader_->filter_pushdown_filter(parent, *filter, pd_filter_info, bitmap))) {
            LOG_WARN("Failed to execute pushdown filter", K(ret));
          }
          break;
        }

        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unknown type of reader", KR(ret), K(reader_->get_type()));
          break;
        }
      }
    }

    return ret;
  }
};

class ObMockMultiVersionDIMicroBlockRowScanner : public ObMultiVersionDIMicroBlockRowScanner
{
public:
  ObMockMultiVersionDIMicroBlockRowScanner(ObIAllocator &alloc) : ObMultiVersionDIMicroBlockRowScanner(alloc)
  {}

  virtual int filter_pushdown_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor *filter,
      sql::PushdownFilterInfo &pd_filter_info,
      const bool can_use_vectorize,
      common::ObBitmap &bitmap) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == reader_ || nullptr == filter || !filter->is_filter_node())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(ret), KP(reader_), KPC(filter));
    } else if (filter->is_truncate_node()) {
      if (OB_FAIL(reader_->filter_pushdown_truncate_filter(parent, *filter, pd_filter_info, bitmap))) {
        LOG_WARN("Failed to pushdown truncate scn filter", K(ret));
      }
    } else if (filter->is_sample_node()) {
      if (ObIMicroBlockReader::MemtableReader == reader_->get_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("memtable does not support sample pushdown", KR(ret), KP(this), KP(reader_));
      } else if (OB_FAIL(static_cast<ObSampleFilterExecutor *>(filter)->apply_sample_filter(
                  pd_filter_info,
                  sstable_->is_major_sstable(),
                  bitmap))) {
        LOG_WARN("Failed to execute sample pushdown filter", K(ret));
      }
    // use uniform base currently, support new format later
    // TODO(hanling): If the new vectorization format does not start counting from the 0th row, it can be computed in batches.
    } else if (can_use_vectorize &&
              filter->get_op().enable_rich_format_ &&
              OB_FAIL(init_exprs_uniform_header(filter->get_cg_col_exprs(),
                                                filter->get_op().get_eval_ctx(),
                                                filter->get_op().get_eval_ctx().max_batch_size_))) {
      LOG_WARN("Failed to init exprs vector header", K(ret));
    } else {
      switch (reader_->get_type())
      {
        case ObIMicroBlockReader::Decoder:
        case ObIMicroBlockReader::CSDecoder: {
          ObIMicroBlockDecoder *decoder = static_cast<ObIMicroBlockDecoder *>(reader_);
          // change to black filter in below situation
          // 1. if need project lob column and micro block has outrow lob
          // 2. if it is semistruct_filter_node, but not cs decoder (beacuase semistrcut white filter only support cs decoder)
          if ((param_->has_lob_column_out() && reader_->has_lob_out_row())
              || (filter->is_semistruct_filter_node() && ObIMicroBlockReader::CSDecoder != reader_->get_type())) {
            sql::ObPhysicalFilterExecutor *physical_filter = static_cast<sql::ObPhysicalFilterExecutor *>(filter);
            if (OB_FAIL(decoder->filter_pushdown_filter(parent,
                                                        *physical_filter,
                                                        pd_filter_info,
                                                        bitmap))) {
              LOG_WARN("Failed to execute pushdown filter", K(ret));
            }
          } else if (filter->is_filter_black_node()) {
            sql::ObBlackFilterExecutor *black_filter = static_cast<sql::ObBlackFilterExecutor *>(filter);
            if (can_use_vectorize && black_filter->can_vectorized()) {
              if (OB_FAIL(apply_filter_batch(
                          parent,
                          *black_filter,
                          pd_filter_info,
                          bitmap))) {
                LOG_WARN("Failed to execute black pushdown filter in batch", K(ret));
              }
            } else if (OB_FAIL(decoder->filter_pushdown_filter(
                        parent,
                        *black_filter,
                        pd_filter_info,
                        bitmap))) {
              LOG_WARN("Failed to execute black pushdown filter", K(ret));
            }
          } else {
            if (OB_FAIL(decoder->filter_pushdown_filter(
                        parent,
                        *static_cast<sql::ObWhiteFilterExecutor *>(filter),
                        pd_filter_info,
                        bitmap))) {
              LOG_WARN("Failed to execute white pushdown filter", K(ret));
            }
          }
          break;
        }

        case ObIMicroBlockReader::MemtableReader: {
          if (OB_FAIL(
                  memtable_reader_->filter_pushdown_filter(parent, *filter, pd_filter_info, bitmap))) {
            LOG_WARN("failed to execute memtable pushdown filter");
          }
          break;
        }

        case ObIMicroBlockReader::Reader:
        case ObIMicroBlockReader::NewFlatReader: {
          if (OB_FAIL(reader_->filter_pushdown_filter(parent, *filter, pd_filter_info, bitmap))) {
            LOG_WARN("Failed to execute pushdown filter", K(ret));
          }
          break;
        }

        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unknown type of reader", KR(ret), K(reader_->get_type()));
          break;
        }
      }
    }

    return ret;
  }
};

class ObMockMultipleScanMerge : public ObMultipleScanMerge
{
public:
  ObMockMultipleScanMerge() : ObMultipleScanMerge()
  {}

  int init_micro_scanner(ObSSTable *sstable, ObIMicroBlockRowScanner *&micro_scanner)
  {
    int ret = OB_SUCCESS;
    ObMockMultiVersionDIMicroBlockRowScanner *mv_di_micro_data_scanner = nullptr;
    ObMultiVersionMicroBlockRowScanner *mv_micro_data_scanner = nullptr;
    ObMockMicroBlockRowScanner *micro_data_scanner = nullptr;
  #define INIT_MICRO_DATA_SCANNER(ptr, type)                                                \
    do {                                                                                    \
      if (ptr == nullptr) {                                                                 \
        if (OB_ISNULL(ptr = OB_NEWx(type, long_life_allocator_, *long_life_allocator_))) {  \
          ret = OB_ALLOCATE_MEMORY_FAILED;                                                  \
          LOG_WARN("Failed to alloc memory for scanner", K(ret));                           \
        } else if (OB_FAIL(ptr->init(access_param_->iter_param_, *access_ctx_, sstable))) {              \
          LOG_WARN("Fail to init micro scanner", K(ret));                                   \
        }                                                                                   \
      } else if (OB_LIKELY(!ptr->is_valid())) {                                             \
        if (OB_FAIL(ptr->switch_context(access_param_->iter_param_, *access_ctx_, sstable))) {           \
          LOG_WARN("Failed to switch micro scanner", K(ret), KPC(ptr), KPC(&access_param_->iter_param_));   \
        }                                                                                   \
      }                                                                                     \
      if (OB_SUCC(ret)) {                                                                   \
        micro_scanner = ptr;                                                               \
      }                                                                                     \
    } while(0)

    if (sstable->is_multi_version_minor_sstable()) {
      if (access_param_->iter_param_.is_delete_insert_) {
        INIT_MICRO_DATA_SCANNER(mv_di_micro_data_scanner, ObMockMultiVersionDIMicroBlockRowScanner);
      } else {
        INIT_MICRO_DATA_SCANNER(mv_micro_data_scanner, ObMultiVersionMicroBlockRowScanner);
      }
    } else {
      INIT_MICRO_DATA_SCANNER(micro_data_scanner, ObMockMicroBlockRowScanner);
    }
  #undef INIT_MICRO_DATA_SCANNER

    return ret;
  }

  virtual int construct_iters() override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(range_) || OB_ISNULL(di_base_range_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "range or di_base_range is NULL", K(ret), KP(range_), KP(di_base_range_));
    } else if (OB_UNLIKELY(iters_.count() > 0 && iters_.count() + di_base_iters_.count() != tables_.count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "iter cnt is not equal to table cnt", K(ret), "iter cnt", iters_.count(),
          "di_base_iter cnt", di_base_iters_.count(), "table cnt", tables_.count(), KP(this));
    } else if (tables_.count() > 0) {
      STORAGE_LOG(TRACE, "construct iters begin", K(tables_.count()), K(iters_.count()), K(di_base_iters_.count()),
                  K(access_param_->iter_param_.is_delete_insert_), KPC_(range), KPC_(di_base_range), K_(tables), KPC_(access_param));
      ObITable *table = NULL;
      ObStoreRowIterator *iter = NULL;
      const ObTableIterParam *iter_param = NULL;
      const bool use_cache_iter = iters_.count() > 0 || di_base_iters_.count() > 0; // rescan with the same iters and different range

      if (access_param_->iter_param_.is_delete_insert_) {
        if (OB_FAIL(tables_.at(0, table))) {  // only one di base iter currently
          STORAGE_LOG(WARN, "Fail to get 0th store, ", K(ret), K_(tables));
        } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Fail to get 0th access param", K(ret), KPC(table));
        } else if (table->is_major_sstable()) {
          if (!use_cache_iter) {
            if (OB_FAIL(table->scan(*iter_param, *access_ctx_, *di_base_range_, iter))) {
              STORAGE_LOG(WARN, "Fail to get di base iterator", K(ret), KPC(table), K(*iter_param));
            } else if (OB_FAIL(di_base_iters_.push_back(iter))) {
              iter->~ObStoreRowIterator();
              STORAGE_LOG(WARN, "Fail to push di base iter to di base iterator array", K(ret));
            }
          } else if (OB_ISNULL(iter = di_base_iters_.at(0))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected null di_base_iters_", K(ret), "idx", 0, K(di_base_iters_));
          } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, di_base_range_))) {
            STORAGE_LOG(WARN, "failed to init scan di_base_iters_", K(ret), "idx", 0);
          }
          if OB_SUCC(ret) {
            ObCOSSTableRowScanner *sstable_scanner = static_cast<ObCOSSTableRowScanner *>(iter);
            FOREACH_CNT_X(cg_iter, sstable_scanner->rows_filter_->filter_iters_, OB_SUCC(ret)) {
              ObCGScanner *cg_scanner = static_cast<ObCGScanner *>(*cg_iter);
              ObIMicroBlockRowScanner *micro_scanner = nullptr;
              if (OB_FAIL(init_micro_scanner(cg_scanner->sstable_, micro_scanner))) {
                STORAGE_LOG(WARN, "Failed to init micro scanner", K(ret));
              } else {
                cg_scanner->micro_scanner_ = micro_scanner;
              }
            }
          }
          if (OB_SUCC(ret)) {
            STORAGE_LOG(DEBUG, "add di base iter for consumer", KPC(table));
          }
        }
      }

      consumer_cnt_ = 0;
      int32_t di_base_cnt = di_base_iters_.count();
      if (OB_FAIL(ret) || di_base_cnt == tables_.count()) {
      } else if (OB_FAIL(set_rows_merger(tables_.count() - di_base_cnt))) {
        STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret), K(di_base_cnt), K(tables_));
      } else {
        const int64_t table_cnt = tables_.count() - 1;
        for (int64_t i = table_cnt; OB_SUCC(ret) && i >= di_base_cnt; --i) {
          if (OB_FAIL(tables_.at(i, table))) {
            STORAGE_LOG(WARN, "Fail to get ith store, ", K(ret), K(i), K_(tables));
          } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Fail to get access param", K(ret), K(i), KPC(table));
          } else if (!use_cache_iter) {
            if (OB_FAIL(table->scan(*iter_param, *access_ctx_, *range_, iter))) {
              STORAGE_LOG(WARN, "Fail to get iterator", K(ret), K(i), KPC(table), K(*iter_param));
            } else if (OB_FAIL(iters_.push_back(iter))) {
              iter->~ObStoreRowIterator();
              STORAGE_LOG(WARN, "Fail to push iter to iterator array", K(ret), K(i));
            }
          } else if (OB_ISNULL(iter = iters_.at(table_cnt - i))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected null iter", K(ret), "idx", table_cnt - i, K_(iters));
          } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, range_))) {
            STORAGE_LOG(WARN, "failed to init scan iter", K(ret), "idx", table_cnt - i);
          }

          if OB_SUCC(ret) {
            ObIMicroBlockRowScanner *micro_scanner = nullptr;
            if (OB_FAIL(init_micro_scanner(static_cast<ObSSTable *>(table), micro_scanner))) {
              STORAGE_LOG(WARN, "Failed to init micro scanner", K(ret));
            } else {
              ObSSTableRowScanner<> *sstable_scanner = static_cast<ObSSTableRowScanner<> *>(iter);
              sstable_scanner->micro_scanner_ = micro_scanner;
            }
          }
          if (OB_SUCC(ret)) {
            consumers_[consumer_cnt_++] = i - di_base_cnt;
            STORAGE_LOG(DEBUG, "add iter for consumer", K(i), KPC(table));
          }
        }
      }

      if (OB_SUCC(ret) && access_param_->iter_param_.enable_pd_blockscan()) {
        if (ScanState::DI_BASE == scan_state_) {
          if (OB_FAIL(get_di_base_iter()->refresh_blockscan_checker(curr_rowkey_))) {
            STORAGE_LOG(WARN, "Failed to refresh di base blockscan checker", K(ret), K(curr_rowkey_));
          }
        } else if (0 == consumer_cnt_ && 0 < di_base_iters_.count()) {
          if (OB_FAIL(prepare_di_base_blockscan(true))) {
            STORAGE_LOG(WARN, "Failed to prepare di base blockscan", K(ret));
          } else {
            scan_state_ = ScanState::DI_BASE;
          }
        } else if (consumer_cnt_ > 0 && nullptr != iters_.at(consumers_[0]) && iters_.at(consumers_[0])->is_sstable_iter()) {
          if (OB_FAIL(locate_blockscan_border())) {
            STORAGE_LOG(WARN, "Fail to locate blockscan border", K(ret), K(iters_.count()), K(di_base_iters_.count()), K_(tables));
          }
        }
      }
      STORAGE_LOG(DEBUG, "construct iters end", K(ret), K(iters_.count()), K(di_base_iters_.count()));
    }
    return ret;
  }
};

class TestDeleteInsertCSScan : public TestMergeBasic
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestDeleteInsertCSScan();
  virtual ~TestDeleteInsertCSScan() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_scan_param(const ObVersionRange &version_range,
                          const ObTableStoreIterator &table_store_iter);
  void prepare_output_expr(const ObIArray<int32_t> &projector,
                           const ObIArray<ObColDesc> &cols_desc);
  int create_pushdown_filter(const bool is_white,
                             const int64_t col_id,
                             const ObDatum &datum,
                             const ObWhiteFilterOperatorType &op_type,
                             const ObITableReadInfo &read_info,
                             ObPushdownFilterExecutor *&pushdown_filter);
  void prepare_txn(ObStoreCtx *store_ctx, const int64_t prepare_version);

  void fake_freeze_info();
  void get_tx_table_guard(ObTxTableGuard &tx_table_guard);
  int convert_to_co_sstable(ObTableHandleV2 &row_store, ObTableHandleV2 &co_store);
  void test_keep_order_blockscan(
      const char **major_data,
      const char **inc_data,
      const char *expected,
      int64_t expected_count,
      std::initializer_list<std::initializer_list<int>>&& intervals);

  public:
  static const int64_t DATUM_ARRAY_CNT = 1024;
  static const int64_t DATUM_RES_SIZE = 10;
  static const int64_t SQL_BATCH_SIZE = 256;
  static ObLSTxCtxMgr ls_tx_ctx_mgr_;
  ObArenaAllocator query_allocator_;
  ObStoreCtx store_ctx_;
  ObTabletMergeExecuteDag merge_dag_;
  ObTableAccessParam access_param_;
  ObGetTableParam get_table_param_;
  ObTableReadInfo read_info_;
  ObFixedArray<int32_t, ObIAllocator> output_cols_project_;
  ObFixedArray<share::schema::ObColumnParam*, ObIAllocator> cols_param_;
  sql::ExprFixedArray output_exprs_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ObPushdownOperator op_;
  void *datum_buf_;
  int64_t datum_buf_offset_;
};

ObLSTxCtxMgr TestDeleteInsertCSScan::ls_tx_ctx_mgr_;

void TestDeleteInsertCSScan::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  MTL(ObTenantTabletScheduler*)->resume_major_merge();

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

}

void TestDeleteInsertCSScan::TearDownTestCase()
{
  clear_tx_data();
  ObMultiVersionSSTableTest::TearDownTestCase();
  ls_tx_ctx_mgr_.reset();
  ls_tx_ctx_mgr_.ls_tx_ctx_map_.reset();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestDeleteInsertCSScan::TestDeleteInsertCSScan()
  : TestMergeBasic("test_delete_insert_cs_scan"),
    exec_ctx_(query_allocator_),
    eval_ctx_(exec_ctx_),
    expr_spec_(query_allocator_),
    op_(eval_ctx_, expr_spec_)
{}

void TestDeleteInsertCSScan::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestDeleteInsertCSScan::fake_freeze_info()
{
  share::ObFreezeInfoList &info_list = MTL(ObTenantFreezeInfoMgr *)->freeze_info_mgr_.freeze_info_;
  info_list.reset();

  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 100;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 200;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 400;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  info_list.latest_snapshot_gc_scn_.val_ = 500;
}

void TestDeleteInsertCSScan::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestDeleteInsertCSScan::prepare_txn(ObStoreCtx *store_ctx,
                                        const int64_t prepare_version)
{
  share::SCN prepare_scn;
  prepare_scn.convert_for_tx(prepare_version);
  ObPartTransCtx *tx_ctx = store_ctx->mvcc_acc_ctx_.tx_ctx_;
  ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
  tx_ctx->exec_info_.state_ = ObTxState::PREPARE;
  tx_ctx->exec_info_.prepare_version_ = prepare_scn;
  mt_ctx->trans_version_ = prepare_scn;
}

void TestDeleteInsertCSScan::prepare_scan_param(
    const ObVersionRange &version_range,
    const ObTableStoreIterator &table_store_iter)
{
  context_.reset();
  access_param_.reset();
  get_table_param_.reset();
  read_info_.reset();
  output_cols_project_.reset();
  cols_param_.reset();
  output_exprs_.reset();
  datum_buf_ = nullptr;
  datum_buf_offset_ = 0;
  query_allocator_.reset();

  ObTabletHandle tablet_handle;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  get_table_param_.frozen_version_ = INT64_MAX;
  get_table_param_.refreshed_merge_ = nullptr;
  get_table_param_.need_split_dst_table_ = false;
  get_table_param_.tablet_iter_.tablet_handle_.assign(tablet_handle);
  get_table_param_.tablet_iter_.table_store_iter_.assign(table_store_iter);
  get_table_param_.tablet_iter_.transfer_src_handle_ = nullptr;
  get_table_param_.tablet_iter_.split_extra_tablet_handles_.reset();

  int64_t schema_column_count = full_read_info_.get_schema_column_count();
  int64_t schema_rowkey_count = full_read_info_.get_schema_rowkey_count();
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = query_allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * schema_column_count);
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&query_allocator_);
  output_cols_project_.init(schema_column_count);
  for (int64_t i = 0; i < schema_column_count; i++) {
    output_cols_project_.push_back(i);
  }

  ObSEArray<ObColDesc, 8> tmp_col_descs;
  ObSEArray<int32_t, 8> tmp_cg_idxs;
  const common::ObIArray<ObColDesc> &cols_desc = full_read_info_.get_columns_desc();
  for (int64_t i = 0; i < schema_column_count; i++) {
    if (i < schema_rowkey_count) {
      tmp_col_descs.push_back(cols_desc.at(i));
    } else {
      tmp_col_descs.push_back(cols_desc.at(i + 2));
    }
  }

  cols_param_.set_allocator(&query_allocator_);
  cols_param_.init(tmp_col_descs.count());
  for (int64_t i = 0; i < tmp_col_descs.count(); ++i) {
    void *col_param_buf = query_allocator_.alloc(sizeof(ObColumnParam));
    ObColumnParam *col_param = new(col_param_buf) ObColumnParam(query_allocator_);
    col_param->set_meta_type(tmp_col_descs.at(i).col_type_);
    col_param->set_nullable_for_write(true);
    col_param->set_column_id(common::OB_APP_MIN_COLUMN_ID + i);
    cols_param_.push_back(col_param);
    tmp_cg_idxs.push_back(i + 1);
  }

  ASSERT_EQ(OB_SUCCESS,
            read_info_.init(query_allocator_,
                            full_read_info_.get_schema_column_count(),
                            full_read_info_.get_schema_rowkey_count(),
                            lib::is_oracle_mode(),
                            tmp_col_descs,
                            nullptr/*storage_cols_index*/,
                            &cols_param_,
                            &tmp_cg_idxs));
  access_param_.iter_param_.read_info_ = &read_info_;
  access_param_.iter_param_.rowkey_read_info_ = &full_read_info_;

  access_param_.iter_param_.table_id_ = table_id_;
  access_param_.iter_param_.tablet_id_ = tablet_id_;
  access_param_.iter_param_.is_same_schema_column_ = true;
  access_param_.iter_param_.has_virtual_columns_ = false;
  access_param_.iter_param_.vectorized_enabled_ = true;
  access_param_.iter_param_.has_lob_column_out_ = false;
  access_param_.iter_param_.pd_storage_flag_.pd_blockscan_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_filter_ = true;
  access_param_.iter_param_.is_delete_insert_ = true;
  access_param_.iter_param_.set_use_column_store();

  prepare_output_expr(output_cols_project_, tmp_col_descs);
  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.op_ = &op_;
  access_param_.padding_cols_ = nullptr;
  access_param_.aggregate_exprs_ = nullptr;
  access_param_.op_filters_ = nullptr;
  access_param_.output_sel_mask_ = nullptr;
  void *buf = query_allocator_.alloc(sizeof(ObRow2ExprsProjector));
  access_param_.row2exprs_projector_ = new (buf) ObRow2ExprsProjector(query_allocator_);
  // TODO: construct pushdown filter
  ASSERT_EQ(OB_SUCCESS, access_param_.iter_param_.op_->init_pushdown_storage_filter());
  access_param_.is_inited_ = true;

  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
                                     tablet_id,
                                     INT64_MAX, // query_expire_ts
                                     -1, // lock_timeout_us
                                     share::SCN::max_scn()));

  ObQueryFlag query_flag(ObQueryFlag::NoOrder,
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true // read_latest
                        );
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  //query_flag.multi_version_minor_merge_ = true;
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          query_allocator_,
                          query_allocator_,
                          version_range));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestDeleteInsertCSScan::prepare_output_expr(
    const ObIArray<int32_t> &projector,
    const ObIArray<ObColDesc> &cols_desc)
{
  output_exprs_.set_allocator(&query_allocator_);
  output_exprs_.init(projector.count());
  for (int64_t i = 0; i < projector.count(); ++i) {
    void *expr_buf = query_allocator_.alloc(sizeof(sql::ObExpr));
    ASSERT_NE(nullptr, expr_buf);
    sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
    expr->reset();

    expr->frame_idx_ = 0;
    expr->datum_off_ = datum_buf_offset_;
    sql::ObDatum *datums = new ((char*)datum_buf_ + datum_buf_offset_) sql::ObDatum[DATUM_ARRAY_CNT];
    datum_buf_offset_ += sizeof(sql::ObDatum) * DATUM_ARRAY_CNT;
    expr->res_buf_off_ = datum_buf_offset_;
    expr->res_buf_len_ = DATUM_RES_SIZE;
    char *ptr = (char *)datum_buf_ + expr->res_buf_off_;
    for (int64_t i = 0; i < DATUM_ARRAY_CNT; i++) {
      datums[i].ptr_ = ptr;
      ptr += expr->res_buf_len_;
    }
    datum_buf_offset_ += expr->res_buf_len_ * DATUM_ARRAY_CNT;
    expr->type_ = T_REF_COLUMN;
    expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    expr->batch_result_ = true;
    expr->datum_meta_.type_ = cols_desc.at(i).col_type_.get_type();
    expr->obj_meta_ = cols_desc.at(i).col_type_;
    output_exprs_.push_back(expr);
  }
}

int TestDeleteInsertCSScan::create_pushdown_filter(
    const bool is_white,
    const int64_t col_id,
    const ObDatum &datum,
    const ObWhiteFilterOperatorType &op_type,
    const ObITableReadInfo &read_info,
    ObPushdownFilterExecutor *&pushdown_filter)
{
  int ret = OB_SUCCESS;
  ObMockWhiteFilterExecutor *filter = nullptr;
  ObIAllocator* allocator_ptr = &query_allocator_;
  ExprFixedArray *column_exprs = nullptr;
  if (!is_white) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Not support to create not white filter", K(ret));
  } else {
    ObPushdownWhiteFilterNode* white_node =
        OB_NEWx(ObPushdownWhiteFilterNode, allocator_ptr, query_allocator_);
    filter = OB_NEWx(ObMockWhiteFilterExecutor, allocator_ptr, allocator_,
                     *white_node, op_);
    column_exprs = &(white_node->column_exprs_);
    white_node->op_type_ = op_type;
    pushdown_filter = filter;
  }

  if (OB_SUCC(ret)) {
    filter->null_param_contained_ = false;
    const common::ObIArray<ObColDesc> &cols_desc = read_info.get_columns_desc();
    const ObColumnIndexArray &cols_index = read_info.get_columns_index();
    if (OB_FAIL(column_exprs->init(1))) {
      STORAGE_LOG(WARN, "Fail to init column exprs", K(ret));
    } else if (OB_FAIL(column_exprs->push_back(nullptr))) {
      STORAGE_LOG(WARN, "Fail to push back col expr", K(ret));
    } else if (OB_FAIL(filter->filter_.col_ids_.init(1))) {
      STORAGE_LOG(WARN, "Fail to init col ids", K(ret));
    } else if (OB_FAIL(filter->filter_.col_ids_.push_back(col_id))) {
      STORAGE_LOG(WARN, "Fail to push back col id", K(ret));
    } else if (OB_FAIL(filter->datum_params_.init(1))) {
      STORAGE_LOG(WARN, "Fail to init datum params", K(ret));
    } else if (OB_FAIL(filter->datum_params_.push_back(datum))) {
      STORAGE_LOG(WARN, "Fail to push back datum", K(ret), K(datum));
    } else if (OB_FAIL(filter->cg_col_exprs_.init(1))) {
      STORAGE_LOG(WARN, "Fail to init cg col exprs", K(ret));
    } else if (OB_FAIL(filter->cg_col_exprs_.push_back(access_param_.output_exprs_->at(col_id - common::OB_APP_MIN_COLUMN_ID)))) {
      STORAGE_LOG(WARN, "Fail to push back col expr", K(ret));
    } else {
      filter->cmp_func_ = get_datum_cmp_func(cols_desc.at(col_id - common::OB_APP_MIN_COLUMN_ID).col_type_,
                                             cols_desc.at(col_id - common::OB_APP_MIN_COLUMN_ID).col_type_);
      STORAGE_LOG(INFO, "finish create pushdown filter");
    }
  }
  return ret;
}

void TestDeleteInsertCSScan::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

int TestDeleteInsertCSScan::convert_to_co_sstable(ObTableHandleV2 &row_store, ObTableHandleV2 &co_store)
{
  int ret = OB_SUCCESS;
  ObCOMergeDagParam param;
  ObCOMergeDagNet dag_net;
  param.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  int64_t column_cnt = table_schema_.get_column_count();
  int64_t cg_cnt = column_cnt + 1;
  ObCOTabletMergeCtx merge_context(dag_net, param, allocator_);
  unittest::TestSchemaPrepare::add_all_and_each_column_group(allocator_, table_schema_);
  merge_context.static_param_.tables_handle_.add_table(row_store);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = row_store.get_table()->get_snapshot_version();
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  //prepare merge_ctx
  TestMergeBasic::prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.static_param_.co_major_merge_type_ = ObCOMajorMergePolicy::BUILD_COLUMN_STORE_MERGE;
  merge_context.static_param_.data_version_ = DATA_VERSION_4_3_0_0;
  merge_context.static_param_.dag_param_.merge_version_ = trans_version_range.snapshot_version_;
  if (OB_FAIL(merge_context.cal_merge_param())) {
    STORAGE_LOG(WARN, "Fail to cal merge param", K(ret));
  } else if (OB_FAIL(merge_context.init_parallel_merge_ctx())) {
    STORAGE_LOG(WARN, "Fail to init parallel merge ctx", K(ret));
  } else if (OB_FAIL(merge_context.static_param_.init_static_info(merge_context.tablet_handle_))) {
    STORAGE_LOG(WARN, "Fail to init static param", K(ret));
  } else if (OB_FAIL(merge_context.init_static_desc())) {
    STORAGE_LOG(WARN, "Fail to init static desc", K(ret));
  } else if (OB_FAIL(merge_context.init_read_info())) {
    STORAGE_LOG(WARN, "Fail to init read info", K(ret));
  } else if (FALSE_IT(merge_context.array_count_ = cg_cnt)) {
  } else if (OB_FAIL(merge_context.init_tablet_merge_info())) {
    STORAGE_LOG(WARN, "Fail to init tablet merge info", K(ret));
  } else if (OB_FAIL(merge_context.prepare_index_builder(0, cg_cnt))) {
    STORAGE_LOG(WARN, "Fail to prepare index builder", K(ret));
  } else {
    ObCOMergeDagParam *dag_param = static_cast<ObCOMergeDagParam *>(&merge_context.static_param_.dag_param_);
    dag_param->start_cg_idx_ = 0;
    dag_param->end_cg_idx_ = cg_cnt;
    dag_param->compat_mode_ = lib::Worker::CompatMode::MYSQL;
    ObCOMerger merger(local_arena_, merge_context.static_param_, 0, cg_cnt);
    if (OB_FAIL(merger.merge_partition(merge_context, 0))) {
      STORAGE_LOG(WARN, "Fail to merge partition", K(ret));
    } else if (OB_FAIL(merge_context.create_sstables(0, cg_cnt))) {
      STORAGE_LOG(WARN, "Fail to create sstable", K(ret));
    } else if (OB_UNLIKELY(cg_cnt != merge_context.merged_cg_tables_handle_.get_count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected table count", K(ret), K(cg_cnt),
                  K(merge_context.merged_cg_tables_handle_.get_count()));
    } else {
      common::ObArray<ObITable *> cg_tables;
      ObCOSSTableV2 *co_sstable = nullptr;
      storage::ObTablesHandleArray &merged_cg_tables_handle = merge_context.merged_cg_tables_handle_;
      for (int64_t i = 0; i < cg_cnt; i++) {
        ObSSTable *merged_sstable = static_cast<ObSSTable *>(merged_cg_tables_handle.get_table(i));
        assert(merged_sstable);
        if (!merged_sstable->is_co_sstable()) {
          cg_tables.push_back(merged_sstable);
        } else {
          co_sstable = static_cast<ObCOSSTableV2 *>(merged_sstable);
        }
      }
      assert(co_sstable);
      if (OB_FAIL(co_sstable->fill_cg_sstables(cg_tables))) {
        STORAGE_LOG(WARN, "Failed to fill cg sstable", K(ret));
      } else {
        co_store.set_sstable(co_sstable, &allocator_);
      }
    }
  }

  return ret;
}

void prepare_ranges(ObIArray<ObDatumRange>* ranges, std::initializer_list<std::initializer_list<int>> intervals)
{
  ranges->reset();
  for (const auto& interval : intervals) {
    ObDatumRange range;
    range.start_key_.datums_ = new ObStorageDatum();
    ASSERT_NE(nullptr, range.start_key_.datums_);
    range.start_key_.datums_[0].set_int(interval.begin()[0]);
    range.start_key_.datum_cnt_ = 1;
    range.end_key_.datums_ = new ObStorageDatum();
    ASSERT_NE(nullptr, range.end_key_.datums_);
    range.end_key_.datums_[0].set_int(interval.begin()[1]);
    range.end_key_.datum_cnt_ = 1;
    range.set_left_closed();
    range.set_right_closed();
    OK(ranges->push_back(range));
  }
}

void TestDeleteInsertCSScan::test_keep_order_blockscan(
      const char **major_data,
      const char **inc_data,
      const char *expected,
      int64_t expected_count,
      std::initializer_list<std::initializer_list<int>>&& intervals)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(major_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(major_data, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(inc_data, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 2;

  ObMockIterator res_iter;
  ObArray<ObDatumRange> ranges;

  prepare_ranges(&ranges, intervals);
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  access_param_.iter_param_.is_delete_insert_ = false;
  access_param_.iter_param_.pd_storage_flag_.set_blockscan_pushdown(true);
  context_.query_flag_.scan_order_ = ObQueryFlag::ScanOrder::KeepOrder;

  ObMultipleMultiScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(ranges));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(expected));

  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(expected_count, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_co_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -70      MIN        99     99     INSERT    NORMAL        SCF\n"
      "5        -70      DI_VERSION 99     99     INSERT    NORMAL        C\n"
      "5        -70      0          19     19     DELETE    NORMAL        C\n"
      "5        -60      DI_VERSION 19     19     INSERT    NORMAL        C\n"
      "5        -60      0          9       9     DELETE    NORMAL        CL\n"
      "10       -70      DI_VERSION 9       9     INSERT    NORMAL        CLF\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "5        99      99   INSERT    NORMAL\n"
      "1        9       9    INSERT    NORMAL\n"
      "2        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "10       9       9    INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "7        9       9    INSERT    NORMAL\n"
      "8        9       9    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(10, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_co_scan_with_reverted_delete_row)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -70      MIN        99     99     INSERT    NORMAL        SCF\n"
      "5        -70      DI_VERSION 99     99     INSERT    NORMAL        C\n"
      "5        -70      0          19     19     DELETE    NORMAL        C\n"
      "5        -60      DI_VERSION 19     19     INSERT    NORMAL        C\n"
      "5        -60      0          9       9     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "5        99      99   INSERT    NORMAL\n"
      "1        9       9    INSERT    NORMAL\n"
      "2        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "7        9       9    INSERT    NORMAL\n"
      "8        9       9    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_co_filter)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    19       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    99       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "4        19       9    INSERT    NORMAL\n"
      "6        99       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(9);
  // second column > 9
  ASSERT_EQ(OB_SUCCESS, create_pushdown_filter(true,
                                               common::OB_APP_MIN_COLUMN_ID + 1,
                                               filter_val,
                                               ObWhiteFilterOperatorType::WHITE_OP_GT,
                                               *access_param_.iter_param_.get_read_info(),
                                               access_param_.iter_param_.pushdown_filter_));

  ObMockMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(2, total_count);

  handle1.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_multi_version_row_filter)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;
  ObTableHandleV2 handle1;
  const char *micro_data1[3];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    1       1    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    2       2    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    3       3    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    4       4    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    5       5    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    6       6    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    7       7    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    8       8    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";
  micro_data1[1] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10       -50      DI_VERSION    10      10   INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    11      11   INSERT    NORMAL      CLF\n"
      "12       -50      DI_VERSION    12      12   INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION    13      13   INSERT    NORMAL      CLF\n"
      "14       -50      DI_VERSION    14      14   INSERT    NORMAL      CLF\n"
      "15       -50      DI_VERSION    15      15   INSERT    NORMAL      CLF\n"
      "16       -50      DI_VERSION    16      16   INSERT    NORMAL      CLF\n"
      "17       -50      DI_VERSION    17      17   INSERT    NORMAL      CLF\n"
      "18       -50      DI_VERSION    18      18   INSERT    NORMAL      CLF\n";
  micro_data1[2] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "19       -50      DI_VERSION    19      19   INSERT    NORMAL      CLF\n"
      "20       -50      DI_VERSION    20      20   INSERT    NORMAL      CLF\n"
      "21       -50      DI_VERSION    21      21   INSERT    NORMAL      CLF\n"
      "22       -50      DI_VERSION    22      22   INSERT    NORMAL      CLF\n"
      "23       -50      DI_VERSION    23      23   INSERT    NORMAL      CLF\n"
      "24       -50      DI_VERSION    24      24   INSERT    NORMAL      CLF\n"
      "25       -50      DI_VERSION    25      25   INSERT    NORMAL      CLF\n"
      "26       -50      DI_VERSION    26      26   INSERT    NORMAL      CLF\n"
      "27       -50      DI_VERSION    27      27   INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 3);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  // case 1: return insert row without delete version
  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -70      MIN        45     45     INSERT    NORMAL        SCF\n"
      "5        -70      DI_VERSION 45     45     INSERT    NORMAL        C\n"
      "5        -70      0          35     35     DELETE    NORMAL        C\n"
      "5        -60      DI_VERSION 35     35     INSERT    NORMAL        C\n"
      "5        -60      0          5       5     DELETE    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(70);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // case 2: return insert row with delete version
  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "13       -90      MIN        43     43     INSERT    NORMAL        SCF\n"
      "13       -90      DI_VERSION 43     43     INSERT    NORMAL        C\n"
      "13       -90      0          33     33     DELETE    NORMAL        C\n"
      "13       -80      DI_VERSION 33     33     INSERT    NORMAL        C\n"
      "13       -80      0          13     13     DELETE    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(70);
  scn_range.end_scn_.convert_for_tx(90);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  // case 3: return delete row
  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "26       -110     MIN        5      5      INSERT    NORMAL        SCF\n"
      "26       -110     DI_VERSION 5      5      INSERT    NORMAL        C\n"
      "26       -110     0          36     36     DELETE    NORMAL        C\n"
      "26       -100     DI_VERSION 36     36     INSERT    NORMAL        C\n"
      "26       -100     0          26     26     DELETE    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(90);
  scn_range.end_scn_.convert_for_tx(110);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  prepare_data_end(handle4);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable4");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "5        45      45   INSERT    NORMAL\n"
      "13       43      43   INSERT    NORMAL\n"
      "1        1       1    INSERT    NORMAL\n"
      "2        2       2    INSERT    NORMAL\n"
      "3        3       3    INSERT    NORMAL\n"
      "4        4       4    INSERT    NORMAL\n"
      "6        6       6    INSERT    NORMAL\n"
      "7        7       7    INSERT    NORMAL\n"
      "8        8       8    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n"
      "10       10      10   INSERT    NORMAL\n"
      "11       11      11   INSERT    NORMAL\n"
      "12       12      12   INSERT    NORMAL\n"
      "14       14      14   INSERT    NORMAL\n"
      "15       15      15   INSERT    NORMAL\n"
      "16       16      16   INSERT    NORMAL\n"
      "17       17      17   INSERT    NORMAL\n"
      "18       18      18   INSERT    NORMAL\n"
      "19       19      19   INSERT    NORMAL\n"
      "20       20      20   INSERT    NORMAL\n"
      "21       21      21   INSERT    NORMAL\n"
      "22       22      22   INSERT    NORMAL\n"
      "23       23      23   INSERT    NORMAL\n"
      "24       24      24   INSERT    NORMAL\n"
      "25       25      25   INSERT    NORMAL\n"
      "27       27      27   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(5);
  // second column != 5
  OK(create_pushdown_filter(true,
                            common::OB_APP_MIN_COLUMN_ID + 1,
                            filter_val,
                            ObWhiteFilterOperatorType::WHITE_OP_NE,
                            *access_param_.iter_param_.get_read_info(),
                            access_param_.iter_param_.pushdown_filter_));
  ObMockMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(26, total_count);
  handle1.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_multi_co_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      -100        9     9     DELETE  NORMAL        CLF\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -70      -101        19     19     INSERT   NORMAL        CLF\n";

  snapshot_version = 150;
  scn_range.start_scn_.convert_for_tx(100);
  scn_range.end_scn_.convert_for_tx(150);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "2        19      19   INSERT    NORMAL\n"
      "1        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(4, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_minor_major_version_overlap)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;
  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -20      DI_VERSION    20      20    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
    "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
    "1        MIN      -10          20      20    DELETE    NORMAL        UCF     trans_id_4\n"
    "1        -20      DI_VERSION   20      20    INSERT    NORMAL        CL      trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());
  ObTxData *tx_data = new ObTxData();
  transaction::ObTransID tx_id = 4;
  tx_data->tx_id_ = tx_id;
  tx_data->commit_version_.convert_for_tx(40);
  tx_data->state_ = ObTxData::COMMIT;
  OK(tx_table->insert(tx_data));
  delete tx_data;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 30;
  trans_version_range.base_version_ = 30;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(0, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_minor_major_version_overlap2)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;
  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -20      DI_VERSION    20      20    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
    "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
    "1        MIN      -10          30      30    INSERT    NORMAL        UCF     trans_id_4\n"
    "1        MIN      -9           20      20    DELETE    NORMAL        UC      trans_id_4\n"
    "1        -20      DI_VERSION   20      20    INSERT    NORMAL        CL      trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());
  ObTxData *tx_data = new ObTxData();
  transaction::ObTransID tx_id = 4;
  tx_data->tx_id_ = tx_id;
  tx_data->commit_version_.convert_for_tx(40);
  tx_data->state_ = ObTxData::COMMIT;
  OK(tx_table->insert(tx_data));
  delete tx_data;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        30      30   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 30;
  trans_version_range.base_version_ = 30;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(1, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_multi_minor_major_version_overlap)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;
  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -20      DI_VERSION    1       1    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -40      MIN        20     20     INSERT    INSERT_DELETE        SCF\n"
      "1        -40      DI_VERSION 20     20     INSERT    NORMAL        C\n"
      "1        -40      0          10     10     DELETE    NORMAL        C\n"
      "1        -30      DI_VERSION 10     10     INSERT    NORMAL        C\n"
      "1        -30      0          1       1     DELETE    NORMAL        C\n"
      "1        -20      DI_VERSION 1       1     INSERT    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(40);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -60      MIN        30     30     DELETE    NORMAL        SCF\n"
      "1        -60      0          30     30     DELETE    NORMAL        C\n"
      "1        -50      DI_VERSION 30     30     INSERT    NORMAL        C\n"
      "1        -50      0          20     20     DELETE    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(40);
  scn_range.end_scn_.convert_for_tx(50);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 25;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(30);
  // second column != 30
  OK(create_pushdown_filter(true,
                            common::OB_APP_MIN_COLUMN_ID + 1,
                            filter_val,
                            ObWhiteFilterOperatorType::WHITE_OP_NE,
                            *access_param_.iter_param_.get_read_info(),
                            access_param_.iter_param_.pushdown_filter_));
  ObMockMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(0, total_count);
  handle1.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, test_co_filter_with_uncommit)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag   trans_id\n"
      "2        -70      DI_VERSION    9      9    INSERT    NORMAL        CLF            trans_id_0\n"
      "4        MIN      -20          99     99    INSERT    NORMAL        FU             trans_id_1\n"
      "4        MIN      -10          9      9     DELETE     NORMAL       U              trans_id_1\n"
      "4        -50      DI_VERSION   9       9    INSERT    NORMAL       CL              trans_id_1\n";

  micro_data2[1] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "7        MIN      -20          99     99    INSERT    NORMAL        FU             trans_id_1\n"
      "7        MIN      -10          9      9     DELETE     NORMAL       U              trans_id_1\n"
      "7        -50      DI_VERSION   9       9    INSERT    NORMAL       CL              trans_id_1\n"
      "8        MIN      -20          99     99    INSERT    NORMAL        FU             trans_id_1\n"
      "8        MIN      -10          9      9     DELETE     NORMAL       U              trans_id_1\n"
      "8        -50      DI_VERSION   9       9    INSERT    NORMAL       CL              trans_id_1\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 2);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "4        -90      DI_VERSION 199    199    INSERT    NORMAL        CF\n"
      "4        -90      0          99     99     DELETE    NORMAL        CL\n"
      "8        -90      DI_VERSION 199    199    INSERT    NORMAL        CF\n"
      "8        -90      0          99      99    DELETE    NORMAL        CL\n";

  snapshot_version = 200;
  scn_range.start_scn_.convert_for_tx(100);
  scn_range.end_scn_.convert_for_tx(150);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());
  for (int64_t i = 1; i <= 1; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(60);
    tx_data->state_ = ObTxData::COMMIT;
    OK(tx_table->insert(tx_data));
    delete tx_data;
  }

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "2        9       9    INSERT    NORMAL\n"
      "4       199     199   INSERT    NORMAL\n"
      "1        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "7        99     99    INSERT    NORMAL\n"
      "5        9       9    INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "8        199   199    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();

  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
  clear_tx_data();
}

TEST_F(TestDeleteInsertCSScan, test_refresh_table)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  ObTableHandleV2 co_sstable;
  ASSERT_EQ(OB_SUCCESS, convert_to_co_sstable(handle1, co_sstable));
  STORAGE_LOG(INFO, "finish convert co sstable");
  table_store_iter.add_table(co_sstable.get_table());

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -50      DI_VERSION  9      9     INSERT    NORMAL        CLF\n"
      "4        -50      DI_VERSION  9      9     INSERT    NORMAL        CLF\n"
      "5        -70      MIN        99     99     INSERT    NORMAL        SCF\n"
      "5        -70      DI_VERSION 99     99     INSERT    NORMAL        C\n"
      "5        -70      0           9      9     DELETE    NORMAL        C\n"
      "5        -50      DI_VERSION  9      9     INSERT    NORMAL        CL\n"
      "7        -70      DI_VERSION 99     99     INSERT    NORMAL        CF\n"
      "7        -70      0           9      9     DELETE    NORMAL        CL\n"
      "8        -70      DI_VERSION 99     99     INSERT    NORMAL        CF\n"
      "8        -70      0           9      9     DELETE    NORMAL        CL\n"
      "9        -70      DI_VERSION 99     99     INSERT    NORMAL        CF\n"
      "9        -70      0           9      9     DELETE    NORMAL        CL\n"
      "15       -70      0           9      9     DELETE    NORMAL        CLF\n"
      "18       -70      0           9      9     INSERT    NORMAL        CLF\n"
      "20       -70      0           9      9     DELETE    NORMAL        CLF\n"
      "22       -70      0           9      9     INSERT    NORMAL        CLF\n"
      "25       -70      0           99     99     INSERT    NORMAL        CF\n"
      "25       -70      0           9      9     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "5        99      99   INSERT    NORMAL\n"
      "1        9       9    INSERT    NORMAL\n"
      "2        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "7        99      99   INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "8        99      99    INSERT    NORMAL\n"
      "9        99      99    INSERT    NORMAL\n"
      "18       9       9    INSERT    NORMAL\n"
      "22       9       9    INSERT    NORMAL\n"
      "25       99      99   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, 1);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));

      ObStoreRowIterator *iter = scan_merge.get_di_base_iter();
      int64_t di_base_curr_scan_index = -1;
      blocksstable::ObDatumRowkey di_base_curr_rowkey;
      blocksstable::ObDatumRowkey di_base_border_rowkey;
      if (OB_FAIL(iter->get_next_rowkey(false,
                                        di_base_curr_scan_index,
                                        di_base_curr_rowkey,
                                        di_base_border_rowkey,
                                        *context_.allocator_))) {
        LOG_WARN("Failed to get di base rowkey", K(ret));
        ret = OB_SUCCESS;
      } else {
        if (total_count > 9) {
          ASSERT_TRUE(di_base_curr_rowkey.is_max_rowkey());
        }
      }
    } else {
      break;
    }
  }
  ASSERT_EQ(12, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertCSScan, keep_order_blockscan_basic)
{
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  "
      "multi_version_row_flag\n"
      "1        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "2        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "3        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "4        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "5        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "6        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "7        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "8        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "9        -50       0            9       9    INSERT    NORMAL      "
      "CLF\n";

  const char *inc_data[1];
  inc_data[0] = "bigint   bigint  bigint     bigint bigint  flag     flag_type "
                " multi_version_row_flag\n"
                "1          -70      0          99     99     UPDATE    NORMAL "
                "       CLF\n"
                "10         -70      0           9      9     INSERT    NORMAL "
                "       CLF\n";

  const char *expected =
      "bigint  bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1         99      99    INSERT    NORMAL      CLF\n"
      "2          9       9    INSERT    NORMAL      CLF\n"
      "3          9       9    INSERT    NORMAL      CLF\n"
      "4          9       9    INSERT    NORMAL      CLF\n"
      "5          9       9    INSERT    NORMAL      CLF\n"
      "6          9       9    INSERT    NORMAL      CLF\n"
      "7          9       9    INSERT    NORMAL      CLF\n"
      "8          9       9    INSERT    NORMAL      CLF\n"
      "9          9       9    INSERT    NORMAL      CLF\n"
      "10         9       9    INSERT    NORMAL      CLF\n";

  test_keep_order_blockscan(major_data, inc_data, expected, 10, {{1, 4}, {5, 10}});
}

TEST_F(TestDeleteInsertCSScan, keep_order_blockscan_simple)
{
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  "
      "multi_version_row_flag\n"
      "1        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "2        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "3        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "4        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "5        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "6        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "7        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "8        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "9        -50       0            9       9    INSERT    NORMAL      "
      "CLF\n";

  const char *inc_data[1];
  inc_data[0] = "bigint   bigint  bigint     bigint bigint  flag     flag_type "
                " multi_version_row_flag\n"
                "1          -70      0          99     99     UPDATE    NORMAL "
                "       CLF\n"
                "10         -70      0           9      9     INSERT    NORMAL "
                "       CLF\n";

  const char *expected =
      "bigint  bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5          9       9    INSERT    NORMAL      CLF\n"
      "6          9       9    INSERT    NORMAL      CLF\n"
      "7          9       9    INSERT    NORMAL      CLF\n"
      "8          9       9    INSERT    NORMAL      CLF\n"
      "9          9       9    INSERT    NORMAL      CLF\n"
      "10         9       9    INSERT    NORMAL      CLF\n"
      "1         99      99    INSERT    NORMAL      CLF\n"
      "2          9       9    INSERT    NORMAL      CLF\n"
      "3          9       9    INSERT    NORMAL      CLF\n"
      "4          9       9    INSERT    NORMAL      CLF\n";

  test_keep_order_blockscan(major_data, inc_data, expected, 10, {{5, 10}, {1, 4}});
}

TEST_F(TestDeleteInsertCSScan, keep_order_blockscan_first_range_major_empty)
{
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  "
      "multi_version_row_flag\n"
      "1        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "2        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "3        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "4        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "5        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "6        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "7        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "8        -50       0            9       9    INSERT    NORMAL      CLF\n"
      "9        -50       0            9       9    INSERT    NORMAL      "
      "CLF\n";

  const char *inc_data[1];
  inc_data[0] = "bigint   bigint  bigint     bigint bigint  flag     flag_type "
                " multi_version_row_flag\n"
                "10          -70     0          99     99     INSERT    NORMAL "
                "       CLF\n"
                "11          -70     0          99     99     INSERT    NORMAL "
                "       CLF\n";

  const char *expected =
      "bigint  bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10         99      99   INSERT    NORMAL      CLF\n"
      "11         99      99   INSERT    NORMAL      CLF\n"
      "1          9       9    INSERT    NORMAL      CLF\n"
      "2          9       9    INSERT    NORMAL      CLF\n"
      "3          9       9    INSERT    NORMAL      CLF\n"
      "4          9       9    INSERT    NORMAL      CLF\n"
      "5          9       9    INSERT    NORMAL      CLF\n"
      "6          9       9    INSERT    NORMAL      CLF\n"
      "7          9       9    INSERT    NORMAL      CLF\n"
      "8          9       9    INSERT    NORMAL      CLF\n"
      "9          9       9    INSERT    NORMAL      CLF\n";

  test_keep_order_blockscan(
      major_data, inc_data, expected, 11, {{10, 11}, {1, 9}});
}

TEST_F(TestDeleteInsertCSScan, keep_order_blockscan_second_range_major_empty)
{
  const char *major_data[1];
  major_data[0] =

      "bigint   bigint  bigint     bigint bigint  flag     flag_type "
      " multi_version_row_flag\n"
      "10         -50      0           99     99     INSERT    NORMAL "
      "       CLF\n"
      "11         -50      0           99     99     INSERT    NORMAL "
      "       CLF\n";

  const char *inc_data[1];
  inc_data[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  "
      "multi_version_row_flag\n"
      "1        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "2        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "3        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "4        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "5        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "6        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "7        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "8        -70       0            9       9    INSERT    NORMAL      CLF\n"
      "9        -70       0            9       9    INSERT    NORMAL      "
      "CLF\n";

  const char *expected =
      "bigint  bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10         99      99   INSERT    NORMAL      CLF\n"
      "11         99      99   INSERT    NORMAL      CLF\n"
      "1          9       9    INSERT    NORMAL      CLF\n"
      "2          9       9    INSERT    NORMAL      CLF\n"
      "3          9       9    INSERT    NORMAL      CLF\n"
      "4          9       9    INSERT    NORMAL      CLF\n"
      "5          9       9    INSERT    NORMAL      CLF\n"
      "6          9       9    INSERT    NORMAL      CLF\n"
      "7          9       9    INSERT    NORMAL      CLF\n"
      "8          9       9    INSERT    NORMAL      CLF\n"
      "9          9       9    INSERT    NORMAL      CLF\n";

  test_keep_order_blockscan(
      major_data, inc_data, expected, 11, {{10, 11}, {1, 9}});
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_delete_insert_cs_scan.log*");
  OB_LOGGER.set_file_name("test_delete_insert_cs_scan.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
