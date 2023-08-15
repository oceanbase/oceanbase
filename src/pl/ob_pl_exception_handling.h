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

#ifndef OCEANBASE_SRC_PL_OB_PL_EXCEPTION_HANDLING_H_
#define OCEANBASE_SRC_PL_OB_PL_EXCEPTION_HANDLING_H_

#include "ob_pl_stmt.h"

#include "lib/clang/11.0.1/include/unwind.h"

namespace oceanbase
{
namespace pl
{

RLOCAL_EXTERN(_Unwind_Exception*, tl_eptr);
extern ObPLException pre_reserved_e;

static constexpr const char* ConditionType[MAX_TYPE] =
{
  "ERROR_CODE",
  "SQL_STATE",
  "SQL_EXCEPTION",
  "SQL_WARNING",
  "NOT_FOUND",
  "OTHERS",
};

typedef struct _Unwind_Exception ObUnwindException;

struct ObPLException
{
  ObPLException() : type_(), body_() {}
  ObPLException(int64_t error_code);
  static uint32_t type_offset_bits() { return offsetof(ObPLException, type_) * 8; }
  static uint32_t body_offset_bits() { return offsetof(ObPLException, body_) * 8; }
  ObPLConditionValue type_;
  ObUnwindException body_;
};

class ObPLEH
{
public:
  ObPLEH() {}
  virtual ~ObPLEH() {}

  static ObUnwindException *eh_create_exception(int64_t pl_context, //obplcontext, for the purpose of saving error bt
                                                int64_t pl_function, // obplfunction, get the function addr
                                                int64_t loc, // the line and col number of this exception raised
                                                int64_t allocator,
                                                const ObPLConditionValue *value);
  static _Unwind_Reason_Code eh_personality(int version, _Unwind_Action actions,
                                                    _Unwind_Exception_Class exceptionClass,
                                                    ObUnwindException *exceptionObject,
                                                    struct _Unwind_Context *context);
  static int eh_convert_exception(bool oracle_mode, int oberr, ObPLConditionType *type, int64_t *error_code, const char **sql_state, int64_t *str_len);
  static ObPLConditionType eh_classify_exception(const char *sql_state);

#ifdef OB_BUILD_ORACLE_PL
  static int eh_adjust_call_stack(
    ObPLFunction *pl_func, ObPLContext *pl_ctx, uint64_t loc, int error_code);
#endif

public:
  static void eh_debug_int64(const char *name_ptr, int64_t name_len, int64_t object);

  static void eh_debug_int64ptr(const char *name_ptr, int64_t name_len, const int64_t *object);

  static void eh_debug_int32(const char *name_ptr, int64_t name_len, int32_t object);

  static void eh_debug_int32ptr(const char *name_ptr, int64_t name_len, const int32_t *object);

  static void eh_debug_int8(const char *name_ptr, int64_t name_len, int8_t object);

  static void eh_debug_int8ptr(const char *name_ptr, int64_t name_len, const int8_t *object);

  static void eh_debug_obj(const char *name_ptr, int64_t name_len, const ObObj *object);

  static void eh_debug_objparam(const char *name_ptr, int64_t name_len, const ObObjParam *object);

private:
  static uintptr_t readULEB128(const uint8_t **data);
  static uintptr_t readSLEB128(const uint8_t **data);
  static uintptr_t readEncodedPointer(const uint8_t **data, uint8_t encoding);
  static unsigned getEncodingSize(uint8_t Encoding);
  static bool handleActionValue(int64_t *resultAction,
                                uint8_t TTypeEncoding,
                                const uint8_t *ClassInfo,
                                uintptr_t actionEntry,
                                uint64_t exceptionClass,
                                struct _Unwind_Exception *exceptionObject);
  static int match_action_value(const ObPLConditionValue *action, const ObPLConditionValue *exception, int64_t &precedence);
  static _Unwind_Reason_Code handleLsda(int version, const uint8_t *lsda,
                                        _Unwind_Action actions,
                                        _Unwind_Exception_Class exceptionClass,
                                        struct _Unwind_Exception *exceptionObject,
                                        struct _Unwind_Context *context);
  template <typename Type_>
  static uintptr_t ReadType(const uint8_t *&p);

  static bool is_internal_error(int errorcode);
};

class ObPLEHService
{
public:
  ObPLEHService() :
    pl_exception_class_(0),
    pl_exception_base_offset_(0),
    eh_create_exception_(),
    eh_raise_exception_(),
    eh_resume_(),
    eh_personality_(),
    eh_convert_exception_(),
    eh_classify_exception() {}
  virtual ~ObPLEHService() {}

public:
  static uint64_t get_exception_class()
  {
    uint64_t pl_exception_class = 0;
    const unsigned char exception_chars[] = {'o', 'b', '1', '\0', 'p', 'l', '\0', '\0'};
    pl_exception_class = exception_chars[0];
    for (int64_t i = 1; i < sizeof(exception_chars); ++i) {
      pl_exception_class <<= 8;
      pl_exception_class += exception_chars[i];
    }
    return pl_exception_class;
  }


  static int64_t get_exception_base_offset()
  {
    int64_t pl_exception_base_offset = 0;
    ObPLException exp;
    pl_exception_base_offset = reinterpret_cast<int64_t>(&exp) - reinterpret_cast<int64_t>(&exp.body_);
    return pl_exception_base_offset;
  }

  uint64_t pl_exception_class_;
  int64_t pl_exception_base_offset_;
  jit::ObLLVMFunction eh_create_exception_;
  jit::ObLLVMFunction eh_raise_exception_;
  jit::ObLLVMFunction eh_resume_;
  jit::ObLLVMFunction eh_personality_;
  jit::ObLLVMFunction eh_convert_exception_;
  jit::ObLLVMFunction eh_classify_exception;

  jit::ObLLVMFunction eh_debug_int64_;
  jit::ObLLVMFunction eh_debug_int64ptr_;
  jit::ObLLVMFunction eh_debug_int32_;
  jit::ObLLVMFunction eh_debug_int32ptr_;
  jit::ObLLVMFunction eh_debug_int8_;
  jit::ObLLVMFunction eh_debug_int8ptr_;
  jit::ObLLVMFunction eh_debug_obj_;
  jit::ObLLVMFunction eh_debug_objparam_;
};

}
}



#endif /* OCEANBASE_SRC_PL_OB_PL_EXCEPTION_HANDLING_H_ */
