// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <jianming.cjq@alipay.com>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_processor.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_csv_parser.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace observer
{
using namespace table;
using namespace sql;

/**
 * ObTableLoadP
 */

int ObTableLoadP::deserialize()
{
  return ParentType::deserialize();
}

int ObTableLoadP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadSharedAllocatorHandle allocator_handle = ObTableLoadSharedAllocatorHandle::make_handle();
      ObTableLoadCoordinator coordinator(table_ctx);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (!allocator_handle) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to make allocator handle", KR(ret));
      } else if (table_ctx->param_.data_type_ == ObTableLoadDataType::RAW_STRING) {
        int64_t col_count = table_ctx->param_.column_count_;
        ObTableLoadObjRowArray obj_rows;
        obj_rows.set_allocator(allocator_handle);
        ObTableLoadCSVParser csv_parser;
        ObTableLoadArray<ObObj> store_column_objs;
        if (OB_FAIL(csv_parser.init(table_ctx, arg_.payload_))) {
          LOG_WARN("fail to init csv parser", KR(ret));
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(csv_parser.get_batch_objs(store_column_objs))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get batch objs", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            ObObj *src_objs = store_column_objs.ptr();
            int64_t row_count = store_column_objs.count() / col_count;
            for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
              ObTableLoadObjRow obj_row;
              if (OB_FAIL(obj_row.deep_copy_and_assign(src_objs, col_count, allocator_handle))) {
                LOG_WARN("failed to deep copy and assign src_objs to obj_row", KR(ret));
              } else if (OB_FAIL(obj_rows.push_back(obj_row))) {
                LOG_WARN("failed to add row to obj_rows", KR(ret), K(obj_row));
              } else {
                src_objs += col_count;
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(coordinator.write(arg_.trans_id_, arg_.session_id_, arg_.sequence_no_,
                                            obj_rows))) {
                LOG_WARN("fail to coordinator write objs", KR(ret));
              }
            }
          }
        }
      } else {
        int64_t pos = 0;
        int64_t data_len = arg_.payload_.length();
        char *buf = static_cast<char *>(allocator_handle->alloc(data_len));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", KR(ret));
        } else {
          MEMCPY(buf, arg_.payload_.ptr(), data_len);
          if (table_ctx->param_.data_type_ == ObTableLoadDataType::OBJ_ARRAY) {
            ObTableLoadObjRowArray obj_rows;
            obj_rows.set_allocator(allocator_handle);
            if (OB_FAIL(obj_rows.deserialize(buf, data_len, pos))) {
              LOG_WARN("failed to deserialize obj rows", KR(ret));
            } else if (OB_FAIL(
                coordinator.write(arg_.trans_id_, arg_.session_id_, arg_.sequence_no_, obj_rows))) {
              LOG_WARN("fail to coordinator write", KR(ret));
            }
          } else if (table_ctx->param_.data_type_ == ObTableLoadDataType::STR_ARRAY) {
            ObTableLoadObjRowArray obj_rows;
            obj_rows.set_allocator(allocator_handle);
            ObTableLoadStrRowArray str_rows;
            str_rows.set_allocator(allocator_handle);

            if (OB_FAIL(str_rows.deserialize(buf, data_len, pos))) {
              LOG_WARN("failed to deserialize str rows", KR(ret));
            } else {
              int64_t col_count = table_ctx->param_.column_count_;
              for (int64_t i = 0; OB_SUCC(ret) && (i < str_rows.count()); ++i) {
                ObTableLoadObjRow obj_row;
                if (OB_FAIL(obj_row.init(col_count, allocator_handle))) {
                  LOG_WARN("failed to init obj_row", KR(ret));
                } else {
                  ObTableLoadStrRow &str_row = str_rows.at(i);
                  for (int64_t j = 0; OB_SUCC(ret) && (j < col_count); ++j) {
                    obj_row.cells_[j].set_string(ObVarcharType, str_row.cells_[j]);
                    obj_row.cells_[j].set_collation_type(
                        ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  }
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(obj_rows.push_back(obj_row))) {
                    LOG_WARN("failed to push back obj_row to obj_rows", KR(ret));
                  }
                }
              } // end for()
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(coordinator.write(arg_.trans_id_, arg_.session_id_, arg_.sequence_no_,
                                            obj_rows))) {
                LOG_WARN("fail to coordinator write objs", KR(ret));
              }
            }
          }
        }
      } // end if
    } // end if
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadPeerP
 */

int ObTableLoadPeerP::deserialize()
{
  return ParentType::deserialize();
}

int ObTableLoadPeerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret));
  } else {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    ObTableLoadSharedAllocatorHandle allocator_handle = ObTableLoadSharedAllocatorHandle::make_handle();
    int64_t data_len = arg_.payload_.length();
    char *buf = nullptr;
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else if (!allocator_handle) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to make allocator handle", KR(ret));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_handle->alloc(data_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", KR(ret));
    } else {
      int64_t pos = 0;
      ObTableLoadStore store(table_ctx);
      ObTableLoadTabletObjRowArray row_array;
      row_array.set_allocator(allocator_handle);
      MEMCPY(buf, arg_.payload_.ptr(), data_len);
      if (OB_FAIL(row_array.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize obj rows", KR(ret));
      } else if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.write(arg_.trans_id_, arg_.session_id_, arg_.sequence_no_, row_array))) {
        LOG_WARN("fail to store write", KR(ret), K_(arg));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

} // namespace observer
} // namespace oceanbase
