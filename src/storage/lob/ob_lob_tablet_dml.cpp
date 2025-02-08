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

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_tablet_dml.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
#include "share/schema/ob_table_dml_param.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

int ObLobTabletDmlHelper::handle_valid_old_outrow_lob_value(
    const bool is_total_quantity_log,
    ObLobCommon* old_lob_common,
    ObLobCommon* new_lob_common)
{
  int ret = OB_SUCCESS;

  if (!is_total_quantity_log) {
    // do nothing.
  } else if (OB_FAIL(ObLobTabletDmlHelper::copy_seq_no_(old_lob_common, new_lob_common))) {
    LOG_WARN("[STORAGE_LOB]copy_seq_no failed.", K(ret), K(new_lob_common));
  } else if (OB_FAIL(ObLobTabletDmlHelper::set_lob_data_outrow_ctx_op(old_lob_common, ObLobDataOutRowCtx::OpType::VALID_OLD_VALUE))) {
    LOG_WARN("[STORAGE_LOB]set_lob_data_outrow_ctx_op failed.", K(ret), KP(old_lob_common));
  }

  return ret;
}

int ObLobTabletDmlHelper::is_support_ext_info_log(ObDMLRunningCtx &run_ctx, bool &is_support)
{
  int ret = OB_SUCCESS;
  bool below_425_version = false;
  bool disable_record_outrow_lob_in_clog = false;

  if (!run_ctx.dml_param_.is_total_quantity_log_ || is_sys_table(run_ctx.relative_table_.get_table_id())) {
    is_support = false;
  } else if (OB_FAIL(is_below_4_2_5_version_(below_425_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (below_425_version) {
    is_support = false;
    LOG_DEBUG("do not support is_support_ext_info_log below 4_2_5 version");
  } else if (OB_FAIL(is_disable_record_outrow_lob_in_clog_(disable_record_outrow_lob_in_clog))) {
    LOG_WARN("failed to get disable_record_outrow_lob_in_clog", K(ret));
  } else {
    is_support = !disable_record_outrow_lob_in_clog;
  }

  return ret;
}

int ObLobTabletDmlHelper::set_lob_data_outrow_ctx_op(ObLobCommon* lob_common, ObLobDataOutRowCtx::OpType op)
{
  int ret = OB_SUCCESS;
  ObLobData* lob_data = nullptr;
  ObLobDataOutRowCtx *lob_outrow_ctx = nullptr;

  if (OB_ISNULL(lob_common)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_common is nullptr");
  } else {
    lob_data = reinterpret_cast<ObLobData*>(lob_common->buffer_);
    if (OB_ISNULL(lob_data)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob_data is nullptr");
    } else {
      lob_outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(lob_data->buffer_);
      if (OB_ISNULL(lob_outrow_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lob_outrow_ctx is nullptr");
      } else {
        lob_outrow_ctx->op_ = op;
      }
    }
  }

  return ret;
}

int ObLobTabletDmlHelper::is_below_4_2_5_version_(bool &is_below)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else {
    is_below = data_version < DATA_VERSION_4_2_5_0;
  }

  return ret;
}

int ObLobTabletDmlHelper::copy_seq_no_(ObLobCommon* old_lob_common, ObLobCommon* new_lob_common)
{
  int ret = OB_SUCCESS;

  ObLobData* old_lob_data = reinterpret_cast<ObLobData*>(old_lob_common->buffer_);
  ObLobData* new_lob_data = reinterpret_cast<ObLobData*>(new_lob_common->buffer_);
  if (OB_ISNULL(old_lob_data) || OB_ISNULL(new_lob_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old_lob_data or new_lob_data is nullptr", K(old_lob_data), K(new_lob_data));
  } else {
    ObLobDataOutRowCtx *old_lob_outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(old_lob_data->buffer_);
    ObLobDataOutRowCtx *new_lob_outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(new_lob_data->buffer_);

    if (OB_ISNULL(old_lob_outrow_ctx) || OB_ISNULL(new_lob_outrow_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old_lob_outrow_ctx or new_lob_outrow_ctx is nullptr", KP(old_lob_outrow_ctx), KP(new_lob_outrow_ctx));
    } else {
      old_lob_outrow_ctx->seq_no_st_ = new_lob_outrow_ctx->seq_no_st_;
      old_lob_outrow_ctx->seq_no_cnt_ = new_lob_outrow_ctx->seq_no_cnt_;
      old_lob_outrow_ctx->del_seq_no_cnt_ = new_lob_outrow_ctx->seq_no_cnt_;
    }
  }

  return ret;
}

int ObLobTabletDmlHelper::is_disable_record_outrow_lob_in_clog_(bool &disable_record_outrow_lob_in_clog)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tenant config", K(ret));
  } else {
    disable_record_outrow_lob_in_clog = tenant_config->_disable_record_outrow_lob_in_clog;
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
