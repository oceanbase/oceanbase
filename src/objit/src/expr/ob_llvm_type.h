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

#ifndef OB_LLVM_TYPE_H
#define OB_LLVM_TYPE_H

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/DIBuilder.h"
#include "llvm/IR/DebugInfoMetadata.h"
#include "llvm/ADT/APInt.h"
#include "common/object/ob_object.h"

namespace oceanbase {
namespace jit {
namespace core {
  class JitContext;
}

typedef ::llvm::Type ObIRType;
typedef ::llvm::IntegerType ObIRIntegerType;
typedef ::llvm::StructType ObIRStructType;
typedef ::llvm::FunctionType ObIRFunctionType;

typedef ::llvm::ArrayRef<::llvm::Type *> ObIRTypeArray;
typedef ::llvm::GlobalValue ObIRGlobalValue;
typedef ::llvm::UndefValue ObIRUndefValue;
typedef ::llvm::ConstantInt ObIRConstantInt;

typedef ::llvm::Value* ObIRValuePtr;
typedef ::llvm::Value ObIRValue;
typedef ::llvm::Module ObIRModule;
typedef ::llvm::Function ObIRFunction;
typedef ::llvm::BasicBlock ObIRBasicBlock;
typedef ::llvm::IRBuilder<> ObIRBuilder;

typedef ::llvm::LLVMContext ObLLVMContext;

typedef ::llvm::DIBuilder ObDIBuilder;
typedef ::llvm::Metadata ObMetadata;
typedef ::llvm::DINode ObDINode;
typedef ::llvm::DIScope ObDIScope;
typedef ::llvm::DICompileUnit ObDICompileUnit;
typedef ::llvm::DIFile ObDIFile;
typedef ::llvm::DISubprogram ObDISubprogram;
typedef ::llvm::DILocalVariable ObDILocalVariable;
typedef ::llvm::DIExpression ObDIExpression;
typedef ::llvm::DebugLoc ObDebugLoc;
typedef ::llvm::DIType ObDIType;
typedef ::llvm::DISubroutineType ObDISubroutineType;

/*static ObIRType *get_ir_type(ObLLVMContext &ctx,*/
                             //const ::oceanbase::common::ObObjType &type)
//{
  //using namespace ::oceanbase::common;
  //#define GET_TY(t) ::llvm::Type::get##t##Ty(ctx)
  //ObIRType *ret = NULL;
  //switch(type) {
    //case ObNullType: { ret = NULL; } break;

    //case ObTinyIntType: { ret = GET_TY(Int8); } break; // int8, aka mysql boolean type
    //case ObSmallIntType: { ret = GET_TY(Int16); } break; // int16
    //case ObMediumIntType: { ret = ObIRType::getIntNTy(ctx, 24); } break; // int24
    //case ObInt32Type: { ret = GET_TY(Int32); } break;  // int32
    //case ObIntType: { ret = GET_TY(Int64); } break;  // int64, aka bigint

    //case ObUTinyIntType: { ret = GET_TY(Int8); } break; // uint8
    //case ObUSmallIntType: { ret = GET_TY(Int16); } break; // uint16
    //case ObUMediumIntType: { ret = ObIRType::getIntNTy(ctx, 24); } break;// uint24
    //case ObUInt32Type: { ret = GET_TY(Int32); } break;  // uint32
    //case ObUInt64Type: { ret = GET_TY(Int64); } break;  // uint64

    //case ObFloatType: { ret = GET_TY(Float); } break; // single-precision floating point
    //case ObDoubleType: { ret = GET_TY(Double); } break; // double-precision floating point
    //case ObUFloatType: { ret = GET_TY(Float); } break; // unsigned single-precision floating point
    //case ObUDoubleType: { ret = GET_TY(Double); } break; // unsigned double-precision floating point
    //case ObNumberType:  // aka decimal/numeric
    //case ObUNumberType:

    //case ObDateTimeType:
    //case ObTimestampType:
    //case ObDateType:
    //case ObTimeType:
    //case ObYearType: {
      //ret = NULL;
    //} break;

    //case ObVarcharType: { ret = GET_TY(Int8Ptr); } break;  // charset: utf8mb4 or binary
    //case ObCharType:    // charset: utf8mb4 or binary

    //case ObHexStringType:  // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001
    //case ObExtendType:                 // Min, Max, NOP etc.
    //case ObUnknownType:                // For question mark(?) in prepared statement, no need to serialize
    //// @note future new types to be defined here !!!
    //case ObTinyTextType:
    //case ObTextType:
    //case ObMediumTextType:
    //case ObLongTextType:
    //case ObBitType: {
      //ret = NULL;
    //} break;
    //default: {
     //ret = NULL;
    //}
  //}

  //return ret;
/*}*/

//在code gen过程中，如果使用ir struct传递Obj, 需要增加GEP操作,
//ir代码会不好看，另一方面也会增加一定开销
//该思路参考论文<Compiling Database Queries into Machine Code>3.2节
class ObIRObj {
public:
  const static int OBJ_IDX_TYPE = 0;
  const static int OBJ_IDX_CS_LEVEL = 1;
  const static int OBJ_IDX_CS_TYPE = 2;
  const static int OBJ_IDX_SCALE = 3;
  const static int OBJ_IDX_VAL_LEN = 4;
  const static int OBJ_IDX_VAL = 5;

public:
  ObIRObj() : type_(nullptr), cs_level_(nullptr),
              cs_type_(nullptr), scale_(nullptr),
              val_len_(nullptr), v_(nullptr)
  {}

  void reset();
  int set_obj(core::JitContext &jc, const common::ObObj &obj);
  //在执行期, 接口type表示该column在schema中类型,
  //row 中Obj类型可能与type不一样, 比如Obj可能为T_NULL,
  //而column的type可能为其他类型, 因此在接口中显示给定type
  int set_obj(core::JitContext &jc,
              const ObIRValuePtr obj,
              const ::oceanbase::common::ObObjType type);

  void set_type(ObLLVMContext &ctx,
                const ::oceanbase::common::ObObjType type) {
    type_ = get_const(ctx, 8, (int64_t)type);
  }

  void set_cs_level(ObLLVMContext &ctx,
                    const ::oceanbase::common::ObCollationLevel cs_level)
  {
    cs_level_ = get_const(ctx, 8, static_cast<int64_t>(cs_level));
  }

  void set_cs_type(ObLLVMContext &ctx,
                   const ::oceanbase::common::ObCollationType cs_type)
  {
    cs_type_ = get_const(ctx, 8, static_cast<int64_t>(cs_type));
  }

  ObIRValuePtr is_null(core::JitContext &jc);
  ObIRValuePtr is_not_null(core::JitContext &jc);

  static ObIRValuePtr get_const(ObLLVMContext &ctx,
                                int64_t bit_num,
                                int64_t value,
                                bool is_sign = true)
  {
    return ObIRConstantInt::get(ctx,
                                ::llvm::APInt(bit_num,
                                value,
                                is_sign));
  }
  //从ObObj的ir对象中获取其成员
  static ObIRValuePtr get_ir_value_element(core::JitContext &jc,
                                           const ObIRValuePtr obj,
                                           int64_t idx);

public:
  //meta_
  ObIRValuePtr type_;
  ObIRValuePtr cs_level_;
  ObIRValuePtr cs_type_;
  ObIRValuePtr scale_;
  //val_len_ or nmb_desc_
  ObIRValuePtr val_len_;
  //ObObjValue
  ObIRValuePtr v_;
};

} //jit
} //oceanbase

#endif

