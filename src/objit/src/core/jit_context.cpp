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

#define USING_LOG_PREFIX JIT
#include "core/jit_context.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace jit
{
namespace core
{

using namespace llvm;

int JitContext::InitializeModule(ObOrcJit &jit)
{
  int ret = OB_SUCCESS;

  TheJIT = &jit;
  if (nullptr == (TheModule = std::make_unique<Module>("PL/SQL", TheJIT->getContext()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for LLVM Module", K(ret));
  } else if (nullptr == (Builder = std::make_unique<IRBuilder<>>(TheJIT->getContext()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for LLVM Builder", K(ret));
  } else {
    TheContext = &TheJIT->getContext();
    TheModule->setDataLayout(TheJIT->getDataLayout());
    Builder = std::make_unique<IRBuilder<>>(*TheContext);
    TheFPM = std::make_unique<legacy::FunctionPassManager>(TheModule.get());
    TheFPM->add(createInstructionCombiningPass());
    TheFPM->add(createReassociatePass());
    TheFPM->add(createGVNPass());
    TheFPM->add(createCFGSimplificationPass());
    TheFPM->doInitialization();
  }

  return ret;
}

void JitContext::compile()
{
  if (!Compile && nullptr != TheJIT) {
    TheJIT->addModule(std::move(TheModule));
    Compile = true;
  }
}

int JitContext::optimize()
{
  int ret = OB_SUCCESS;
  if (nullptr == TheModule || nullptr == TheFPM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(TheModule.get()), KP(TheFPM.get()));
  }
  for (Module::iterator It = TheModule->begin();
        OB_SUCC(ret) && It != TheModule->end(); ++It) {
    // Run the FPM on this function
    TheFPM->run(*It);
  }
  return ret;
}

int ObDWARFContext::init()
{
  int ret = OB_SUCCESS;
  auto BinOrErr = llvm::object::createBinary(MemoryRef);
  if (!BinOrErr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create binary memory", K(ret));
  } else {
    Bin = std::move(BinOrErr.get());
    llvm::object::ObjectFile *DebugObj = dyn_cast<llvm::object::ObjectFile>(Bin.get());
    if (!DebugObj) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to dynamic cast", K(ret));
    } else {
      std::string s;
      llvm::raw_string_ostream Out(s);
      Context = DWARFContext::create(*DebugObj);
      if (!Context) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create dwarf context", K(ret));
      } else if (!Context->verify(Out)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to verify DWARFContext", K(ret));
      } else if (Context->getNumCompileUnits() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Compile Unist is not 1", K(ret), K(Context->getNumCompileUnits()));
      } else if (!Context->getUnitAtIndex(0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Compile Unit is null", K(ret));
      } else if (!Context->getLineTableForUnit(Context->getUnitAtIndex(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Line Table is null", K(ret));
      } else if (Context->getUnitAtIndex(0)->getNumDIEs() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Compile DIEs less than 2",
                 K(ret), K(Context->getUnitAtIndex(0)->getNumDIEs()));
      } else if (!Context->verify(Out)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to verify DWARFContext", K(ret));
      } else {
        LOG_INFO("success to initialize DWARFContext", K(ret), K(Out.str().c_str()));
      }
    }
  }
  return ret;
}

}  // core
}  // jit
}  // oceanbase
