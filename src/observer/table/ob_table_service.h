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

#ifndef _OB_TABLE_SERVICE_H
#define _OB_TABLE_SERVICE_H 1
#include "share/table/ob_table.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "share/table/ob_table_rpc_struct.h"
#include "share/schema/ob_table_param.h"
#include "storage/access/ob_dml_param.h"
#include "ob_table_scan_executor.h"
#include "ob_table_session_pool.h"
namespace oceanbase
{
namespace table
{
class ObHTableFilterOperator;
class ObHColumnDescriptor;
} // end namespace table

namespace storage
{
class ObAccessService;
} // end namespace storage

namespace observer
{
using table::ObTableOperation;
using table::ObTableOperationResult;
using table::ObTableBatchOperation;
using table::ObTableBatchOperationResult;
using table::ObITableBatchOperationResult;
using table::ObTableQuery;
using table::ObTableQueryResult;
using table::ObTableQuerySyncResult;
class ObTableApiProcessorBase;
class ObTableService;

class ObTableServiceCtx
{
  static const int64_t COMMON_COLUMN_NUM = 16;
public:
  common::ObSEArray<sql::ObExprResType, COMMON_COLUMN_NUM> columns_type_;
protected:
  friend class ObTableService;
  struct Param
  {
    uint64_t table_id_;
    uint64_t partition_id_;
    common::ObTabletID tablet_id_;
    common::ObTabletID index_tablet_id_;
    share::ObLSID ls_id_;
    int64_t timeout_ts_;
    ObTableApiProcessorBase *processor_;
    common::ObArenaAllocator *allocator_;
    bool returning_affected_rows_;
    table::ObTableEntityType entity_type_;
    table::ObBinlogRowImageType binlog_row_image_type_;
    bool returning_affected_entity_;
    bool returning_rowkey_;
    Param()
        :table_id_(common::OB_INVALID_ID),
         partition_id_(0), // @dazhi: to be removed
         tablet_id_(ObTabletID::INVALID_TABLET_ID),
         index_tablet_id_(ObTabletID::INVALID_TABLET_ID),
         timeout_ts_(0),
         processor_(nullptr),
         allocator_(nullptr),
         returning_affected_rows_(false),
         entity_type_(table::ObTableEntityType::ET_DYNAMIC),
         binlog_row_image_type_(table::ObBinlogRowImageType::FULL),
         returning_affected_entity_(false),
         returning_rowkey_(false)
    {}
  } param_;
public:
  ObTableServiceCtx()
      :param_()
  {}
  void reset_dml()
  {
    columns_type_.reset();
  }
  void init_param(int64_t timeout_ts,
                  ObTableApiProcessorBase *processor,
                  common::ObArenaAllocator *allocator,
                  bool returning_affected_rows,
                  table::ObTableEntityType entity_type,
                  table::ObBinlogRowImageType binlog_row_image_type,
                  bool returning_affected_entity = false,
                  bool returning_rowkey = false)
  {
    param_.timeout_ts_ = timeout_ts;
    param_.processor_ = processor;
    param_.allocator_ = allocator;
    param_.returning_affected_rows_ = returning_affected_rows;
    param_.entity_type_ = entity_type;
    param_.binlog_row_image_type_ = binlog_row_image_type;
    param_.returning_affected_entity_ = returning_affected_entity;
    param_.returning_rowkey_ = returning_rowkey;
  }
  uint64_t &param_table_id() { return param_.table_id_; }
  common::ObTabletID &param_tablet_id() { return param_.tablet_id_; }
  share::ObLSID &param_ls_id() { return param_.ls_id_; }
};

class ObNormalTableQueryResultIterator: public table::ObTableQueryResultIterator
{
public:
  ObNormalTableQueryResultIterator(const ObTableQuery &query, table::ObTableQueryResult &one_result)
      :one_result_(&one_result),
       query_(&query),
       last_row_(NULL),
       batch_size_(query.get_batch()),
       max_result_size_(std::min(query.get_max_result_size(),
                                 static_cast<int64_t>(common::OB_MAX_PACKET_BUFFER_LENGTH-1024))),
       scan_result_(NULL),
       is_first_result_(true),
       has_more_rows_(true),
       is_query_sync_(false)
  {
  }
  virtual ~ObNormalTableQueryResultIterator() {}
  virtual int get_next_result(table::ObTableQueryResult *&one_result) override;
  virtual bool has_more_result() const override;
  void set_scan_result(table::ObTableApiScanRowIterator *scan_result) { scan_result_ = scan_result; }
  virtual void set_one_result(ObTableQueryResult *result) override {one_result_ = result;}
  virtual table::ObTableQueryResult *get_one_result() override { return one_result_; }
  void set_query(const ObTableQuery *query) {query_ = query;}
  void set_query_sync() { is_query_sync_ = true ; }
private:
  table::ObTableQueryResult *one_result_;
  const ObTableQuery *query_;
  common::ObNewRow *last_row_;
  int32_t batch_size_;
  int64_t max_result_size_;
  table::ObTableApiScanRowIterator *scan_result_;
  bool is_first_result_;
  bool has_more_rows_;
  bool is_query_sync_;
};

/// table service
class ObTableService
{
public:
  friend class TestBatchExecute_obj_increment_Test;
public:
  ObTableService()
      : schema_service_(NULL)
  {}
  virtual ~ObTableService() = default;
  int init(ObGlobalContext &gctx);
  table::ObTableApiSessPoolMgr& get_sess_mgr() { return sess_pool_mgr_; }
  static int check_htable_query_args(const ObTableQuery &query);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableService);
private:
  share::schema::ObMultiVersionSchemaService *schema_service_;
  table::ObTableApiSessPoolMgr sess_pool_mgr_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_SERVICE_H */
