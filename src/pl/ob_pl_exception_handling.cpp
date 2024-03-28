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

#define USING_LOG_PREFIX PL
#include "ob_pl_exception_handling.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;
using namespace jit;

namespace pl
{

_RLOCAL(_Unwind_Exception*, tl_eptr);
ObPLException pre_reserved_e(OB_ALLOCATE_MEMORY_FAILED); //预留的exception空间，防止出现没内存的时候抛不出来exception

void ObPLEH::eh_debug_int64(const char *name_ptr, int64_t name_len, int64_t object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(object));
}

void ObPLEH::eh_debug_int64ptr(const char *name_ptr, int64_t name_len, const int64_t *object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(*object));
}

void ObPLEH::eh_debug_int32(const char *name_ptr, int64_t name_len, int32_t object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(object));
}

void ObPLEH::eh_debug_int32ptr(const char *name_ptr, int64_t name_len, const int32_t *object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(*object));
}

void ObPLEH::eh_debug_int8(const char *name_ptr, int64_t name_len, const int8_t object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(object));
}

void ObPLEH::eh_debug_int8ptr(const char *name_ptr, int64_t name_len, const int8_t *object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(*object));
}

void ObPLEH::eh_debug_obj(const char *name_ptr, int64_t name_len, const ObObj *object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(*object));
}

void ObPLEH::eh_debug_objparam(const char *name_ptr, int64_t name_len, const ObObjParam *object)
{
  LOG_DEBUG(">>>>>>>>>>0", K(ObString(name_len, name_ptr)), K(*object));
}

int ObPLEH::eh_convert_exception(bool oracle_mode, int oberr, ObPLConditionType *type, int64_t *error_code, const char **sql_state, int64_t *str_len)
{
  UNUSED(oracle_mode);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type) || OB_ISNULL(error_code) || OB_ISNULL(sql_state)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid Argument", K(oberr), K(type), K(error_code), K(sql_state), K(ret));
  } else {
    *sql_state = ob_sqlstate(oberr);
    *str_len = STRLEN(*sql_state);
    if (oracle_mode) {
      *error_code = oberr;
      *type = ERROR_CODE;
    } else {
      if (OB_SP_RAISE_APPLICATION_ERROR == oberr) {
        ObWarningBuffer *wb = NULL;
        CK (OB_NOT_NULL(wb = common::ob_get_tsi_warning_buffer()));
        OX (*error_code = wb->get_err_code());
        OX (*sql_state = wb->get_sql_state());
        OX (*str_len = STRLEN(*sql_state));
        if (OB_FAIL(ret)) {
        } else if (-1 == *error_code) {
          *type = SQL_STATE;
        } else {
          *type = ERROR_CODE;
        }
      } else {
        if (oberr < 0) {
          *error_code = ob_mysql_errno(oberr);
          if (-1 == *error_code) {
            *type = SQL_STATE;
          } else {
            *type = ERROR_CODE;
          }
        } else {
          *error_code = oberr;
          *type = SQL_STATE;
        }
      }
    }
  }
  return ret;
}

ObPLException::ObPLException(int64_t error_code)
{
  body_.exception_class = ObPLEHService::get_exception_class();
  body_.exception_cleanup = NULL;
  body_.private_1 = 0;
  body_.private_2 = 0;
  new(&type_)ObPLConditionValue(ERROR_CODE, error_code);
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLEH::eh_adjust_call_stack(
  ObPLFunction *pl_func, ObPLContext *pl_ctx, uint64_t loc, int error_code)
{
  int ret = OB_SUCCESS;

  if (error_code != OB_SUCCESS
      && OB_ALLOCATE_MEMORY_FAILED != error_code
      && OB_TENANT_OUT_OF_MEM != error_code
      && OB_EXCEED_MEM_LIMIT != error_code
      && OB_NOT_NULL(pl_ctx)
      && lib::is_oracle_mode()) {

    ObIAllocator *allocator = NULL;
    CK (OB_NOT_NULL(pl_ctx->get_exec_stack().at(0)));
    CK (OB_NOT_NULL(pl_ctx->get_exec_stack().at(0)->get_exec_ctx().status_));
    OX (*(pl_ctx->get_exec_stack().at(0)->get_exec_ctx().status_) = error_code);
    OX (allocator = pl_ctx->get_exec_stack().at(0)->get_exec_ctx().allocator_);
    CK (OB_NOT_NULL(allocator));
    if (0 == pl_ctx->get_call_stack().count()) {
      OZ (DbmsUtilityHelper::get_backtrace(*allocator, *pl_ctx, pl_ctx->get_call_stack()));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(pl_func)) {
      bool is_exist = false;
      DbmsUtilityHelper::BtInfo *tbi = NULL;
      DbmsUtilityHelper::BtInfo *bi = NULL;
      // check if same error code exist, this is possible
      for (int64_t i = 0;
            OB_SUCC(ret) && !is_exist && i < pl_ctx->get_error_trace().count(); ++i) {
        tbi = pl_ctx->get_error_trace().at(i);
        CK (OB_NOT_NULL(tbi));
        OX (is_exist = (tbi->error_code == error_code));
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(allocator)) {
        bi = static_cast<DbmsUtilityHelper::BtInfo*>(
          allocator->alloc(sizeof(DbmsUtilityHelper::BtInfo)));
      }
      if (OB_SUCC(ret) && !is_exist && OB_NOT_NULL(bi)) {
        bi->init();
        bi->error_type = DbmsUtilityHelper::BTErrorType::ERROR_INFO; // 0: stack info, 1 : error info
        bi->loc = loc + (1LL << 32);
        bi->handler = pl_func->get_action();
        bi->error_code = error_code;
        ObSqlString exception_msg;
        OZ (pl_ctx->get_error_trace().push_back(bi));
        for (int64_t i = 0; OB_SUCC(ret) && i < pl_ctx->get_error_trace().count(); ++i) {
          if (pl_ctx->get_error_trace().at(i)->handler == bi->handler) {
            pl_ctx->get_error_trace().at(i)->loc = loc + (1LL << 32);
          }
        }
        OZ (pl_ctx->get_exact_error_msg(pl_ctx->get_error_trace(),
                                       pl_ctx->get_call_stack(),
                                       exception_msg));
        LOG_WARN("got exception! ", KR(error_code), K(exception_msg));
      }
    }
    pl_ctx->set_call_trace_error_code(ret);
  }
  return ret;
}
#endif

ObUnwindException *ObPLEH::eh_create_exception(int64_t pl_context,
                                               int64_t pl_function,
                                               int64_t loc,
                                               int64_t allocator,
                                               const ObPLConditionValue *value)
{
  ObUnwindException *unwind = NULL;
  UNUSED (allocator);
  if (NULL != value) {
    int ret = OB_SUCCESS;
    ObPLContext *pl_ctx = reinterpret_cast<ObPLContext *>(pl_context);
    ObPLExecState *frame = NULL;
    ObIAllocator *pl_allocator = NULL;
    CK (OB_NOT_NULL(pl_ctx));
    CK (pl_ctx->get_exec_stack().count() > 0);
    CK (OB_NOT_NULL(frame = pl_ctx->get_exec_stack().at(0)));
    CK (frame->is_top_call());
    CK (OB_NOT_NULL(pl_allocator = frame->get_exec_ctx().allocator_));
    if (OB_FAIL(ret)) {
    } else if (OB_ALLOCATE_MEMORY_FAILED == value->error_code_) {
      unwind = &pre_reserved_e.body_;
    } else {
      ObPLException *exception
        = static_cast<ObPLException *>(pl_allocator->alloc(sizeof(ObPLException)));
      if (NULL != exception) {
        exception = new(exception)ObPLException();
        unwind = &exception->body_;
        unwind->exception_class = ObPLEHService::get_exception_class();
        unwind->exception_cleanup = NULL;
        exception->type_.type_ = value->type_;
        exception->type_.error_code_ = value->error_code_;
        if (NULL == value->sql_state_) {
          exception->type_.sql_state_ = NULL;
        } else {
          char* str = static_cast<char*>(pl_allocator->alloc(value->str_len_));
          if (NULL != str) {
            STRNCPY(str, value->sql_state_, value->str_len_);
            exception->type_.sql_state_ = str;
          } else {
            reinterpret_cast<ObIAllocator*>(allocator)->free(exception);
            unwind = &pre_reserved_e.body_;
          }
        }
        exception->type_.str_len_ = value->str_len_;
        exception->type_.stmt_id_ = value->stmt_id_;
        exception->type_.signal_ = value->signal_;
      } else {
        unwind = &pre_reserved_e.body_;
      }
    }
    tl_eptr = unwind;

#ifdef OB_BUILD_ORACLE_PL
    OZ (eh_adjust_call_stack(
      reinterpret_cast<ObPLFunction *>(pl_function), pl_ctx, loc, value->error_code_));
#endif

  }
  return unwind;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////Runtime Library functions///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename Type_>
uintptr_t ObPLEH::ReadType(const uint8_t *&p)
{
  Type_ value;
  memcpy(&value, p, sizeof(Type_));
  p += sizeof(Type_);
  return static_cast<uintptr_t>(value);
}

uintptr_t ObPLEH::readULEB128(const uint8_t **data)
{
  uintptr_t result = 0;
  uintptr_t shift = 0;
  unsigned char byte;
  const uint8_t *p = *data;

  do {
    byte = *p++;
    result |= (byte & 0x7f) << shift;
    shift += 7;
  }
  while (byte & 0x80);

  *data = p;

  return result;
}

uintptr_t ObPLEH::readSLEB128(const uint8_t **data)
{
  uintptr_t result = 0;
  uintptr_t shift = 0;
  unsigned char byte;
  const uint8_t *p = *data;

  do {
    byte = *p++;
    result |= (byte & 0x7f) << shift;
    shift += 7;
  }
  while (byte & 0x80);

  *data = p;

  if ((byte & 0x40) && (shift < (sizeof(result) << 3))) {
    result |= (~0ULL << shift);
  }

  return result;
}

uintptr_t ObPLEH::readEncodedPointer(const uint8_t **data, uint8_t encoding)
{
  uintptr_t result = 0;
  const uint8_t *p = *data;

  if (encoding == DW_EH_PE_omit)
    return(result);

  // first get value
  switch (encoding & 0x0F) {
    case DW_EH_PE_absptr:
      result = ReadType<uintptr_t>(p);
      break;
    case DW_EH_PE_uleb128:
      result = readULEB128(&p);
      break;
      // Note: This case has not been tested
    case DW_EH_PE_sleb128:
      result = readSLEB128(&p);
      break;
    case DW_EH_PE_udata2:
      result = ReadType<uint16_t>(p);
      break;
    case DW_EH_PE_udata4:
      result = ReadType<uint32_t>(p);
      break;
    case DW_EH_PE_udata8:
      result = ReadType<uint64_t>(p);
      break;
    case DW_EH_PE_sdata2:
      result = ReadType<int16_t>(p);
      break;
    case DW_EH_PE_sdata4:
      result = ReadType<int32_t>(p);
      break;
    case DW_EH_PE_sdata8:
      result = ReadType<int64_t>(p);
      break;
    default:
      // not supported
      ob_abort();
      break;
  }

  // then add relative offset
  switch (encoding & 0x70) {
    case DW_EH_PE_absptr:
      // do nothing
      break;
    case DW_EH_PE_pcrel:
      result += (uintptr_t)(*data);
      break;
    case DW_EH_PE_textrel:
    case DW_EH_PE_datarel:
    case DW_EH_PE_funcrel:
    case DW_EH_PE_aligned:
    default:
      // not supported
      ob_abort();
      break;
  }

  // then apply indirection
  if (0 != (encoding & DW_EH_PE_indirect)) {
    result = *((uintptr_t*)result);
  }

  *data = p;

  return result;
}

unsigned ObPLEH::getEncodingSize(uint8_t Encoding)
{
  if (Encoding == DW_EH_PE_omit)
    return 0;

  switch (Encoding & 0x0F) {
  case DW_EH_PE_absptr:
    return sizeof(uintptr_t);
  case DW_EH_PE_udata2:
    return sizeof(uint16_t);
  case DW_EH_PE_udata4:
    return sizeof(uint32_t);
  case DW_EH_PE_udata8:
    return sizeof(uint64_t);
  case DW_EH_PE_sdata2:
    return sizeof(int16_t);
  case DW_EH_PE_sdata4:
    return sizeof(int32_t);
  case DW_EH_PE_sdata8:
    return sizeof(int64_t);
  default:
    // not supported
    ob_abort();
  }
  return 0;
}

bool ObPLEH::handleActionValue(int64_t *resultAction,
                               uint8_t TTypeEncoding,
                               const uint8_t *ClassInfo,
                               uintptr_t actionEntry,
                               uint64_t exceptionClass,
                               struct _Unwind_Exception *exceptionObject)
{
  bool ret = false;

  if (!resultAction || !exceptionObject || exceptionClass != ObPLEHService::get_exception_class())
    return(ret);

  ObPLException *excp = (ObPLException*)(((char*) exceptionObject) + ObPLEHService::get_exception_base_offset());
  ObPLConditionValue &condition_value = excp->type_;

  const uint8_t *actionPos = (uint8_t*) actionEntry,
  *tempActionPos;
  int64_t typeOffset = 0;
  int64 actionOffset = 0;

  int64_t precedence = MAX_TYPE;
  for (int i = 0; true; ++i) {
    // Each emitted dwarf action corresponds to a 2 tuple of
    // type info address offset, and action offset to the next
    // emitted action.
    typeOffset = readSLEB128(&actionPos);
    tempActionPos = actionPos;
    actionOffset = readSLEB128(&tempActionPos);

    assert((typeOffset >= 0) && "handleActionValue(...):filters are not supported.");

    // Note: A typeOffset == 0 implies that a cleanup llvm.eh.selector
    //       argument has been matched.
    if (typeOffset > 0) {
      unsigned EncSize = getEncodingSize(TTypeEncoding);
      const uint8_t *EntryP = ClassInfo - typeOffset * EncSize;
      uintptr_t P = readEncodedPointer(&EntryP, TTypeEncoding);
      ObPLConditionValue *ThisClassInfo = reinterpret_cast<ObPLConditionValue*>(P);
      int64_t cur_pre = 0;
      if (OB_SUCCESS !=match_action_value(ThisClassInfo, &condition_value, cur_pre)) {
        LOG_WARN("Bug: Failed to match action value", K(ThisClassInfo), K(condition_value), K(ret));
      } else if (cur_pre < 0) {
        /*do nothing*/
      } else if (cur_pre < precedence) {
        precedence = cur_pre;
        *resultAction = i + 1;
        ret = true;
       break; //这里其实不应break，应该寻找precedence最高的，但是我们在前面CG阶段已经把condition已经按precedence排过序了，这里可以break提升效率
      } else { /*do nothing*/ }
    }

    if (!actionOffset)
      break;

    actionPos += actionOffset;
  }
  return ret;
}

int ObPLEH::match_action_value(const ObPLConditionValue *action, const ObPLConditionValue *exception, int64_t &precedence)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(action) || OB_ISNULL(exception)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(action), K(exception), K(ret));
  } else if (ERROR_CODE != exception->type_ && SQL_STATE != exception->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected exception type", K(exception->type_), K(ret));
  } else {
    switch (action->type_) {
    case ERROR_CODE: {
      precedence = action->error_code_ == exception->error_code_ ? ERROR_CODE : INVALID_TYPE;
      break;
    }
    case SQL_STATE: {
      precedence = exception->str_len_ == action->str_len_ ? (0 == STRNCMP(action->sql_state_, exception->sql_state_, exception->str_len_) ? SQL_STATE : INVALID_TYPE) : INVALID_TYPE;
      break;
    }
    case SQL_EXCEPTION: {
      precedence = (eh_classify_exception(exception->sql_state_) == SQL_EXCEPTION) && !is_internal_error(exception->error_code_) ? SQL_EXCEPTION : INVALID_TYPE;
      break;
    }
    case SQL_WARNING: {
      precedence = eh_classify_exception(exception->sql_state_) == SQL_WARNING ? SQL_WARNING : INVALID_TYPE;
      break;
    }
    case NOT_FOUND: {
      precedence = eh_classify_exception(exception->sql_state_) == NOT_FOUND ? NOT_FOUND : INVALID_TYPE;
      break;
    }
    case OTHERS: {
      if (ERROR_CODE == exception->type_) {
        precedence = is_internal_error(exception->error_code_) ? INVALID_TYPE : OTHERS;
      } else {
        precedence = OTHERS;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected exception type", K(action->type_), K(ret));
      break;
    }
    }
  }
  return ret;
}

bool ObPLEH::is_internal_error(int error_code)
{
  // these error code is oceanbase inner error, should not catched by exception handler.
  return OB_TRY_LOCK_ROW_CONFLICT == error_code
    || OB_ERR_UNEXPECTED == error_code
    || OB_ALLOCATE_MEMORY_FAILED == error_code
    || OB_ERR_DEFENSIVE_CHECK == error_code
    || OB_TRANS_XA_BRANCH_FAIL == error_code
    || OB_TRANS_SQL_SEQUENCE_ILLEGAL == error_code
    || OB_ERR_SESSION_INTERRUPTED == error_code
    || OB_ERR_QUERY_INTERRUPTED == error_code
    || OB_TIMEOUT == error_code;
}

ObPLConditionType ObPLEH::eh_classify_exception(const char *sql_state)
{
  ObPLConditionType type = INVALID_TYPE;
  if (NULL != sql_state) {
    if ('0' == sql_state[0] && '0' == sql_state[1]) {
      type = INVALID_TYPE;
    } else if ('0' == sql_state[0] && '1' == sql_state[1]) {
      type = SQL_WARNING;
    } else if ('0' == sql_state[0] && '2' == sql_state[1]) {
      type = NOT_FOUND;
    } else {
      type = SQL_EXCEPTION;
    }
  }
  return type;
}

_Unwind_Reason_Code ObPLEH::handleLsda(int version,
                                       const uint8_t *lsda,
                                       _Unwind_Action actions,
                                       _Unwind_Exception_Class exceptionClass,
                                       struct _Unwind_Exception *exceptionObject,
                                       struct _Unwind_Context *context)
{
  UNUSED(version);
  _Unwind_Reason_Code ret = _URC_CONTINUE_UNWIND;

  if (NULL != lsda) {
    uintptr_t pc = _Unwind_GetIP(context)-1;

    uintptr_t funcStart = _Unwind_GetRegionStart(context);
    uintptr_t pcOffset = pc - funcStart;
    const uint8_t *ClassInfo = NULL;

    uint8_t lpStartEncoding = *lsda++;

    if (lpStartEncoding != DW_EH_PE_omit) {
      readEncodedPointer(&lsda, lpStartEncoding);
    }

    uint8_t ttypeEncoding = *lsda++;
    uintptr_t classInfoOffset;

    if (ttypeEncoding != DW_EH_PE_omit) {
      classInfoOffset = readULEB128(&lsda);
      ClassInfo = lsda + classInfoOffset;
    }

    uint8_t         callSiteEncoding = *lsda++;
    uint32_t        callSiteTableLength = static_cast<uint32_t>(readULEB128(&lsda));
    const uint8_t   *callSiteTableStart = lsda;
    const uint8_t   *callSiteTableEnd = callSiteTableStart + callSiteTableLength;
    const uint8_t   *actionTableStart = callSiteTableEnd;
    const uint8_t   *callSitePtr = callSiteTableStart;

    while (callSitePtr < callSiteTableEnd) {
      uintptr_t start = readEncodedPointer(&callSitePtr, callSiteEncoding);
      uintptr_t length = readEncodedPointer(&callSitePtr, callSiteEncoding);
      uintptr_t landingPad = readEncodedPointer(&callSitePtr, callSiteEncoding);

      // Note: Action value
      uintptr_t actionEntry = readULEB128(&callSitePtr);

      if (exceptionClass != ObPLEHService::get_exception_class()) {
        // We have been notified of a foreign exception being thrown,
        // and we therefore need to execute cleanup landing pads
        actionEntry = 0;
      }

      if (0 == landingPad) {
        continue; // no landing pad for this entry
      }

      if (0 != actionEntry) {
        actionEntry += ((uintptr_t) actionTableStart) - 1;
      }

      bool exceptionMatched = false;

      if ((start <= pcOffset) && (pcOffset < (start + length))) {
        int64_t actionValue = 0;

        if (0 != actionEntry) {
          exceptionMatched = handleActionValue(&actionValue,
                                               ttypeEncoding,
                                               ClassInfo,
                                               actionEntry,
                                               exceptionClass,
                                               exceptionObject);
        }

        if (!(actions & _UA_SEARCH_PHASE)) {

          // Found landing pad for the PC.
          // Set Instruction Pointer to so we re-enter function
          // at landing pad. The landing pad is created by the
          // compiler to take two parameters in registers.
          _Unwind_SetGR(context, __builtin_eh_return_data_regno(0), (uintptr_t)exceptionObject);

          // Note: this virtual register directly corresponds
          //       to the return of the llvm.eh.selector intrinsic
          if (!actionEntry || !exceptionMatched) {
            // We indicate cleanup only
            _Unwind_SetGR(context, __builtin_eh_return_data_regno(1), 0);
          } else {
            // Matched type info index of llvm.eh.selector intrinsic
            // passed here.
            _Unwind_SetGR(context, __builtin_eh_return_data_regno(1), actionValue);
          }

          // To execute landing pad set here
          _Unwind_SetIP(context, funcStart + landingPad);
          ret = _URC_INSTALL_CONTEXT;
        } else if (exceptionMatched) {
          ret = _URC_HANDLER_FOUND;
        } else {
          // Note: Only non-clean up handlers are marked as
          //       found. Otherwise the clean up handlers will be
          //       re-found and executed during the clean up
          //       phase.
        }

        break;
      }
    }
  }
  return ret;
}

_Unwind_Reason_Code ObPLEH::eh_personality(int version, _Unwind_Action actions,
                                   _Unwind_Exception_Class exceptionClass,
                                   ObUnwindException *exceptionObject,
                                   struct _Unwind_Context *context)
{
  const uint8_t *lsda = static_cast<const uint8_t *>(_Unwind_GetLanguageSpecificData(context));
  LOG_DEBUG(">>>>>>>>>>0", K(version), K(actions), K(exceptionClass), K(lsda));
  return handleLsda(version, lsda, actions, exceptionClass, exceptionObject, context);
}

}
}
