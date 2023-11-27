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
 * This file is for func xmlsequence.
 */

#include "ob_expr_xml_sequence.h"
#include "ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_package.h"
#include "lib/utility/utility.h"
#define USING_LOG_PREFIX SQL_ENG

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(ObExprXmlSequence::ObSequenceExtraInfo,
                    type_, not_null_, elem_type_, capacity_, udt_id_);

int ObExprXmlSequence::ObSequenceExtraInfo::deep_copy(common::ObIAllocator &allocator,
                                                    const ObExprOperatorType type,
                                                    ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObSequenceExtraInfo &other = *static_cast<ObSequenceExtraInfo *>(copied_info);
  if (OB_SUCC(ret)) {
    other = *this;
  }
  return ret;
}

ObExprXmlSequence::ObExprXmlSequence(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_XMLSEQUENCE, N_XMLSEQUENCE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlSequence::~ObExprXmlSequence() {}

int ObExprXmlSequence::calc_result_type1(ObExprResType &type,
                      ObExprResType &text,
                      common::ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);
  if (!ob_is_xml_pl_type(text.get_type(), text.get_udt_id()) && !ob_is_xml_sql_type(text.get_type(), text.get_subschema_id())) {
    ret = OB_ERR_PARAM_INVALID;
    LOG_WARN("get type invaid.", K(ret), K(text));
  } else {
    type.get_calc_meta().set_sql_udt(ObXMLSqlType);
    type.set_type(ObExtendType);
    type.set_udt_id(XmlSequenceUdtID);
    OX (type.set_extend_type(pl::PL_VARRAY_TYPE));
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObExprXmlSequence::eval_xml_sequence(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObIAllocator &allocator = ctx.exec_ctx_.get_allocator();
  pl::ObPLCollection *coll = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObDatum *xml_datum = NULL;
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObIMulModeBase *xml_tree = NULL;
  ObMulModeMemCtx* mem_ctx = nullptr;
  ObSequenceExtraInfo *info = nullptr;
  if (OB_ISNULL(info = OB_NEWx(ObSequenceExtraInfo, (&allocator), allocator, T_FUN_SYS_XMLSEQUENCE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (expr.arg_cnt_ < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum))) {
    LOG_WARN("fail to get xmltype value", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, ObCollationType::CS_TYPE_INVALID, ObNodeMemType::TREE_TYPE, xml_tree, node_type, ObGetXmlBaseType::OB_IS_REPARSE))) {
    LOG_WARN("fail to parse xml doc", K(ret));
  } else {
    info->type_ = info->type_ = pl::PL_VARRAY_TYPE;
    info->not_null_ = true;
    info->capacity_ = xml_tree->count();
    info->udt_id_ = XmlSequenceUdtID;
    info->elem_type_.set_obj_type(ObExtendType);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(coll = static_cast<pl::ObPLVArray*>(allocator.alloc(sizeof(pl::ObPLVArray) + 8)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
  } else {
    coll = new(coll)pl::ObPLVArray(info->udt_id_);
    static_cast<pl::ObPLVArray*>(coll)->set_capacity(info->capacity_);
  }

  OZ(expr.eval_param_value(ctx));

  if (OB_SUCC(ret)) {
    const pl::ObUserDefinedType *type = NULL;
    const pl::ObCollectionType *collection_type = NULL;
    pl::ObElemDesc elem_desc;
    pl::ObPLPackageGuard package_guard(session->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(allocator,
                                   *session,
                                   *(exec_ctx.get_sql_ctx()->schema_guard_),
                                   package_guard,
                                   *(exec_ctx.get_sql_proxy()),
                                   false);
    OZ (package_guard.init());
    pl::ObPLINS *ns = NULL;
    if (NULL == session->get_pl_context()) {
      ns = &resolve_ctx;
    } else {
      ns = session->get_pl_context()->get_current_ctx();
    }

    CK (OB_NOT_NULL(ns));
    OZ (ns->get_user_type(info->udt_id_, type));
    OV (OB_NOT_NULL(type), OB_ERR_UNEXPECTED, K(info->udt_id_));
    CK (type->is_collection_type());
    CK (OB_NOT_NULL(collection_type = static_cast<const pl::ObCollectionType*>(type)));
    OX (elem_desc.set_obj_type(common::ObExtendType));
    OX (elem_desc.set_pl_type(collection_type->get_element_type().get_type()));
    OX (elem_desc.set_field_count(1));
    OX (elem_desc.set_udt_id(collection_type->get_element_type().get_user_type_id()));

    OX (coll->set_element_desc(elem_desc));
    OX (coll->set_not_null(info->not_null_));

    OZ (ObSPIService::spi_set_collection(session->get_effective_tenant_id(),
                                         ns,
                                         allocator,
                                         *coll,
                                         xml_tree->size()));

    if (OB_SUCC(ret)) {
      int64_t array_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < xml_tree->count(); i++) {
        ObIMulModeBase *ele = xml_tree->at(i);
        ObObj ele_obj;
        ObXmlDocument *doc = nullptr;
        ObXmlElement *xml_node = nullptr;
        ObString blob_locator;
        ObString binary_str;
        if (OB_ISNULL(coll->get_data())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get coll data null", K(ret));
        } else if (M_ELEMENT != ele->type()) {
          // do nothing
        } else if (OB_ISNULL(xml_node = static_cast<ObXmlElement*>(ele))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get ele null.", K(ret));
        } else if (OB_ISNULL(doc = OB_NEWx(ObXmlDocument, mem_ctx->allocator_, ObMulModeNodeType::M_DOCUMENT, mem_ctx))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create document", K(ret));
        } else if (OB_FAIL(doc->add_element(xml_node))) {
        } else if (OB_FAIL(doc->get_raw_binary(binary_str, mem_ctx->allocator_))) {
          LOG_WARN("get raw binary failed", K(ret));
        } else {
          ObTextStringResult text_result(ObLongTextType, true, &allocator);
          if (OB_FAIL(text_result.init(binary_str.length()))) {
            LOG_WARN("failed to init text result.", K(ret));
          } else if (OB_FAIL(text_result.append(binary_str))) {
            LOG_WARN("failed to append binary str.", K(ret), K(binary_str));
          } else {
            text_result.get_result_buffer(blob_locator);
            ele_obj.meta_ = expr.args_[0]->obj_meta_;
            ele_obj.meta_.set_subschema_id(ObXMLSqlType);
            ele_obj.set_string(ObUserDefinedSQLType, blob_locator);
            OZ(deep_copy_obj(*coll->get_allocator(), ele_obj, static_cast<ObObj*>(coll->get_data())[array_count]));
            array_count++;
          }
        }
      }
    }

    ObObj result;
    result.set_extend(reinterpret_cast<int64_t>(coll), coll->get_type());
    OZ(res.from_obj(result, expr.obj_datum_map_));

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(coll->get_allocator())) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(exec_ctx.get_pl_ctx())) {
        tmp_ret = exec_ctx.init_pl_ctx();
      }
      if (OB_SUCCESS == tmp_ret && OB_NOT_NULL(exec_ctx.get_pl_ctx())) {
        tmp_ret = exec_ctx.get_pl_ctx()->add(result);
      }
      if (OB_SUCCESS != tmp_ret) {
        LOG_ERROR("fail to collect pl collection allocator, may be exist memory issue", K(tmp_ret));
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }

  }

  return ret;
}
#endif

int ObExprXmlSequence::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xml_sequence;
  return OB_SUCCESS;
}

};

};