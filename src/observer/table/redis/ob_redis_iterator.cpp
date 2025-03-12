/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_redis_iterator.h"
#include "lib/utility/ob_fast_convert.h"
#include "observer/table/redis/operator/ob_redis_operator.h"
#include "observer/table/redis/ob_redis_ttl.h"
#include "observer/table/redis/ob_redis_rkey.h"

namespace oceanbase
{
// using namespace observer;
using namespace common;
// using namespace share;
// using namespace sql;
namespace table
{
int ObRedisCellEntity::get_cell(int64_t idx, ObObj *&obj) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= ob_row_->get_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx argument", K(ret), K(idx), KPC(ob_row_));
  } else {
    obj = &ob_row_->get_cell(idx);
  }
  return ret;
}

int ObRedisCellEntity::get_expire_ts(int64_t &expire_ts) const
{
  int ret = OB_SUCCESS;
  ObObj *obj = nullptr;
  if (model_ == ObRedisModel::STRING) {
    if (OB_FAIL(get_cell(ObRedisUtil::STRING_COL_IDX_EXPIRE_TS, obj))) {
      LOG_WARN("fail to get cell with idx", K(ret));
    } else if (obj->is_null()) {
      expire_ts = INT64_MAX;
    } else if (OB_FAIL(obj->get_timestamp(expire_ts))) {
      LOG_WARN("fail to get expire ts", K(ret), KPC(ob_row_), KPC(obj));
    }
  } else {
    if (OB_FAIL(get_cell(ObRedisUtil::COL_IDX_EXPIRE_TS, obj))) {
      LOG_WARN("fail to get cell with idx", K(ret));
    } else if (OB_FAIL(obj->get_timestamp(expire_ts))) {
      LOG_WARN("fail to get expire ts", K(ret), KPC(ob_row_), KPC(obj));
    }
  }

  return ret;
}

int ObRedisCellEntity::get_insert_ts(int64_t &insert_ts) const
{
  int ret = OB_SUCCESS;
  ObObj *obj = nullptr;
  if (model_ == ObRedisModel::STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid model type, string do not have insert_ts", K(ret));
  } else if (OB_FAIL(get_cell(ObRedisUtil::COL_IDX_INSERT_TS, obj))) {
    LOG_WARN("fail to get cell with idx", K(ret));
  } else if (OB_FAIL(obj->get_timestamp(insert_ts))) {
    LOG_WARN("fail to get expire ts", K(ret), KPC(ob_row_), KPC(obj));
  }
  return ret;
}

int ObRedisCellEntity::get_is_data(bool &is_data) const
{
  int ret = OB_SUCCESS;
  ObObj *obj = nullptr;
  if (model_ == ObRedisModel::STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid model type, string do not have is_data", K(ret));
  } else if (model_ == ObRedisModel::LIST) {
    if (OB_FAIL(get_cell(ObRedisUtil::COL_IDX_IS_DATA, obj))) {
      LOG_WARN("fail to get cell with idx", K(ret));
    } else if (OB_FAIL(obj->get_bool(is_data))) {
      LOG_WARN("fail to get is data", K(ret), KPC(ob_row_), KPC(obj));
    }
  } else {
    ObString rkey;
    if (OB_FAIL(get_cell(ObRedisUtil::COL_IDX_RKEY, obj))) {
      LOG_WARN("fail to get cell with idx", K(ret));
    } else if (OB_FAIL(obj->get_varbinary(rkey))) {
      LOG_WARN("fail to rkey", K(ret), KPC(ob_row_), KPC(obj));
    } else if (OB_FAIL(ObRedisRKeyUtil::decode_is_data(rkey, is_data))) {
      LOG_WARN("fail to decode is_data", K(ret), K(rkey));
    }
  }

  return ret;
}

ObRedisRowIterator::ObRedisRowIterator()
    : limit_del_rows_(0),
      meta_row_(nullptr),
      row_allocator_("RedisIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      is_finished_(false),
      return_expired_row_(true),
      cur_ts_(ObTimeUtility::fast_current_time()),
      meta_expire_ts_(-1),
      meta_insert_ts_(-1),
      expire_ts_(-1),
      model_(ObRedisModel::INVALID),
      is_ttl_table_(false),
      meta_(nullptr),
      return_redis_meta_(false)
{}

int ObRedisRowIterator::close()
{
  row_allocator_.reset();
  return ObTableTTLRowIterator::close();
}

int ObRedisRowIterator::init_scan(const ObKVAttr &kv_attribute, const ObRedisTTLCtx *ttl_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init ObRedisRowIterator twice", K(ret), K(is_inited_));
  } else if (OB_ISNULL(ttl_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ObRedisTTLCtx", K(ret));
  } else {
    model_ = kv_attribute.redis_model_;
    is_ttl_table_ = kv_attribute.is_redis_ttl_;
    return_expired_row_ = false;
    is_inited_ = true;
    meta_ = ttl_ctx->get_meta();
    return_redis_meta_ = ttl_ctx->is_return_meta();
    if (OB_NOT_NULL(meta_)) {
      if (OB_FAIL(update_expire_ts_with_meta())) {
        LOG_WARN("fail to update expire ts with meta", K(ret));
      }
    }
  }

  return ret;
}

// del_row_limit: 0 means no limit
int ObRedisRowIterator::init_ttl(const schema::ObTableSchema &table_schema, uint64_t del_row_limit)
{
  int ret = OB_SUCCESS;
  ObKVAttr kv_attr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init ObRedisRowIterator twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(init_common(table_schema))) {
    LOG_WARN("fail to init ObTableTTLRowIterator", K(ret), K(table_schema));
  } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(table_schema.get_kv_attributes(), kv_attr))) {
    LOG_WARN("fail to parse kv attributes", KR(ret), K(table_schema.get_kv_attributes()));
  } else if (kv_attr.type_ != ObKVAttr::REDIS || !kv_attr.is_ttl_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("KV attribute is not redis ttl table", K(ret), K(kv_attr));
  } else {
    model_ = kv_attr.redis_model_;
    is_ttl_table_ = kv_attr.is_redis_ttl_;
  }

  if (OB_SUCC(ret)) {
    return_expired_row_ = true;
    iter_end_ts_ = ONE_ITER_EXECUTE_MAX_TIME + cur_ts_;
    limit_del_rows_ = del_row_limit;
    is_inited_ = true;
  }
  return ret;
}

// If the current time has exceeded the meta expire_ts, then all keys are removed, that is,
// expire_ts = cur_ts.
// If not, only the key before insert_ts is deleted, that is, expire_ts = insert_ts
int ObRedisRowIterator::update_expire_ts_with_last_row()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(meta_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("redis meta should be null", K(ret), KPC(meta_));
  } else {
    ObRedisCellEntity cell(*last_row_, model_);
    if (OB_FAIL(cell.get_expire_ts(meta_expire_ts_))) {
      LOG_WARN("fail to get expire ts", K(ret));
    } else if (OB_FAIL(cell.get_insert_ts(meta_insert_ts_))) {
      LOG_WARN("fail to get expire ts", K(ret));
    } else {
      expire_ts_ = cur_ts_ > meta_expire_ts_ ? cur_ts_ : meta_insert_ts_;
    }
  }
  return ret;
}

int ObRedisRowIterator::update_expire_ts_with_meta()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null meta", K(ret));
  } else {
    meta_expire_ts_ = meta_->get_expire_ts();
    meta_insert_ts_ = meta_->get_insert_ts();
    expire_ts_ = cur_ts_ > meta_expire_ts_ ? cur_ts_ : meta_insert_ts_;
  }
  return ret;
}

int ObRedisRowIterator::is_last_row_expired(bool &is_expired)
{
  int ret = OB_SUCCESS;
  ObRedisCellEntity cell(*last_row_, model_);
  // 3. When it expires, the key is deleted
  if (model_ == ObRedisModel::STRING) {
    int64_t expire_ts = -1;
    if (OB_FAIL(cell.get_expire_ts(expire_ts))) {
      LOG_WARN("fail to get expire timestamp", K(ret));
    } else {
      is_expired = expire_ts < cur_ts_;
    }
  } else {
    int64_t sub_insert_ts = -1;
    if (OB_FAIL(cell.get_insert_ts(sub_insert_ts))) {
      LOG_WARN("fail to get insert timestamp", K(ret));
    } else {
      is_expired = sub_insert_ts < expire_ts_;
    }
  }

  return ret;
}

int ObRedisRowIterator::is_meta_row_expired(bool &is_expired)
{
  int ret = OB_SUCCESS;
  ObRedisCellEntity cell(*meta_row_, model_);
  // 3. When it expires, the key is deleted
  int64_t meta_expire_ts = -1;
  if (OB_FAIL(cell.get_expire_ts(meta_expire_ts))) {
    LOG_WARN("fail to get insert timestamp", K(ret));
  } else {
    is_expired = meta_expire_ts < cur_ts_;
  }
  return ret;
}

int ObRedisRowIterator::deep_copy_last_meta_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(last_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null last row", K(ret));
  } else {
    ObNewRow *tmp_row = nullptr;
    int64_t buf_size = last_row_->get_deep_copy_size() + sizeof(ObNewRow);
    char *tmp_row_buf = static_cast<char *>(row_allocator_.alloc(buf_size));
    if (OB_ISNULL(tmp_row_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new row", KR(ret));
    } else {
      tmp_row = new(tmp_row_buf)ObNewRow();
      int64_t pos = sizeof(ObNewRow);
      if (OB_FAIL(tmp_row->deep_copy(*last_row_, tmp_row_buf, buf_size, pos))) {
        LOG_WARN("fail to deep copy new row", KR(ret));
      } else {
        meta_row_ = tmp_row;
      }
    }
  }
  return ret;
}

int ObRedisRowIterator::get_next_row_ttl(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  // End Condition 1: The number of lines to be deleted has exceeded the upper limit.
  //     The default limit_del_rows_ is 1024
  if (ttl_cnt_ >= limit_del_rows_ || is_finished_) {
    last_row_ = is_finished_ ? nullptr : meta_row_;
    ret = OB_ITER_END;
    LOG_DEBUG("The upper limit for deleting rows exceeded, finish get next row",
        KR(ret), K(ttl_cnt_), K(limit_del_rows_), K(is_finished_), KPC(meta_row_));
  } else {
    bool is_expired = false;
    while (OB_SUCC(ret) && !is_expired) {
      // End condition 2: timeout, currently the default setting is 30s
      if (ObTimeUtility::fast_current_time() > iter_end_ts_) {
        last_row_ = meta_row_;
        ret = OB_ITER_END;
        LOG_INFO(
            "iter_end_ts reached, stop current iterator", KR(ret), K(cur_ts_), K_(iter_end_ts));
      } else if (OB_FAIL(ObTableApiScanRowIterator::get_next_row(row))) {
        if (OB_ITER_END != ret) {
          last_row_ = nullptr;
          LOG_WARN("fail to get next row", K(ret));
        } else {
          last_row_ = nullptr;
          if (OB_NOT_NULL(meta_row_)) {
            // reach end but may still have expired meta
            int tmp_ret = is_meta_row_expired(is_expired);
            if (tmp_ret != OB_SUCCESS) {
              ret = tmp_ret;
              last_row_ = nullptr; // finish scan
              LOG_WARN("fail to check is meta row expired", K(ret));
            } else if (is_expired) {
              row = meta_row_;
              meta_row_ = nullptr;
              ret = OB_SUCCESS;
              is_finished_ = true;
              last_row_ = row;
            } else {
              LOG_DEBUG("finish table ttl because ITER_END");
              last_row_ = nullptr; // finish scan
            }
          }
        }
      } else {
        last_row_ = row;
        scan_cnt_++;
        if (model_ == ObRedisModel::STRING) {
          if (OB_FAIL(is_last_row_expired(is_expired))) {
            LOG_WARN("fail to check is last row expired", K(ret), K(expire_ts_));
          }
        } else {
          ObRedisCellEntity cell(*row, model_);
          bool is_data = false;
          if (OB_FAIL(cell.get_is_data(is_data))) {
            LOG_WARN("fail to get is meta col", K(ret));
          } else if (!is_data) {
            // 2. if is_data = false, update expire_ts
            if (OB_FAIL(update_expire_ts_with_last_row())) {
              LOG_WARN("fail to update expire ts", K(ret), K(meta_expire_ts_), K(cur_ts_), K(meta_insert_ts_));
            } else if (OB_ISNULL(meta_row_)) {
              // first get meta row, just record it
              if (OB_FAIL(deep_copy_last_meta_row())) {
                LOG_WARN("fail to copy last meta row", K(ret));
              } else {
                // skip delete meta row
                is_expired = false;
              }
            } else {
              // not first get meta row, try to delete last meta row
              row = meta_row_;
              if (OB_FAIL(is_meta_row_expired(is_expired))) {
                LOG_WARN("fail to check is meta row expired", K(ret));
              } else if (OB_FAIL(deep_copy_last_meta_row())) {
                LOG_WARN("fail to copy last meta row", K(ret));
              }
            }
          } else {
            if (expire_ts_ == -1) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("expire ts is not inited before scan data rows, please check scan range", K(ret));
            } else if (OB_FAIL(is_last_row_expired(is_expired))) {
              LOG_WARN("fail to check is last row expired", K(ret), K(expire_ts_));
            }
          }
        }
      }
    }

    // 4. statistical expired information
    if (OB_SUCC(ret) && is_expired) {
      ++ttl_cnt_;
    }
  }

  if (OB_FAIL(ret) && ret != OB_ITER_END) {
    LOG_WARN("fail to get next expired row", KR(ret));
  }

  return ret;
}

int ObRedisRowIterator::get_next_row_scan(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_expired = true;
  // get first not expired data row
  while (OB_SUCC(ret) && is_expired) {
    if (OB_FAIL(ObTableApiScanRowIterator::get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
      last_row_ = nullptr;
    } else {
      last_row_ = row;
      scan_cnt_++;
      if (model_ == ObRedisModel::STRING) {
        if (OB_FAIL(is_last_row_expired(is_expired))) {
          LOG_WARN("fail to check is last row expired", K(ret), K(expire_ts_));
        }
      } else {
        ObRedisCellEntity cell(*row, model_);
        bool is_data = false;
        if (OB_FAIL(cell.get_is_data(is_data))) {
          LOG_WARN("fail to get is meta col", K(ret));
        } else if (!is_data) {
          if (OB_ISNULL(meta_)) {
            if (OB_FAIL(update_expire_ts_with_last_row())) {
              LOG_WARN("fail to update expire ts", K(ret), K(meta_expire_ts_), K(cur_ts_), K(meta_insert_ts_));
            }
          } else if (expire_ts_ == -1) {
            // if meta_ is not null, expire_ts_ should be updated
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expire ts should be init by meta", K(ret), KPC(meta_));
          }
          if (OB_SUCC(ret)) {
            // if is_expired = true, meta will be skipped
            is_expired = return_redis_meta_ ? false : true;
          }
        } else {
          if (OB_FAIL(is_last_row_expired(is_expired))) {
            LOG_WARN("fail to check is last row expired", K(ret), K(expire_ts_));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next not expired row", KR(ret));
      row = nullptr;
      last_row_ = nullptr;
    }
  }
  return ret;
}

int ObRedisRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("should not use not inited ObRedisInterator", K(ret), K(is_inited_));
  } else if (!is_ttl_table_) {
    ret = ObTableApiScanRowIterator::get_next_row(row);
  } else {
    ret = return_expired_row_ ? get_next_row_ttl(row) : get_next_row_scan(row);
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
