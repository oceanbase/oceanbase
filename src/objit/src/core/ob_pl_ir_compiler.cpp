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

#include "core/ob_pl_ir_compiler.h"

#include <algorithm>
#include <charconv>

#include "llvm/ADT/SmallVector.h"
#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Remarks/RemarkStreamer.h"
#include "llvm/Remarks/RemarkSerializer.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/LLVMRemarkStreamer.h"
#include "llvm/Object/ObjectFile.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/ErrorHandling.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/SmallVectorMemoryBuffer.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/CodeGen/MachineFunction.h"
#include "llvm/CodeGen/MachineFrameInfo.h"
#include "llvm/CodeGen/MachineFunctionPass.h"
#include "llvm/CodeGen/Passes.h"
#include "llvm/CodeGen/MachineOptimizationRemarkEmitter.h"
#include "llvm/CodeGen/MachineModuleInfo.h"

#include "core/ob_orc_jit.h"

using namespace llvm;
using namespace llvm::orc;

namespace oceanbase
{

namespace jit
{

namespace core
{

IRSymbolMapper::ManglingOptions
irManglingOptionsFromTargetOptions(const TargetOptions &Opts)
{
  IRSymbolMapper::ManglingOptions MO;

  MO.EmulatedTLS = Opts.EmulatedTLS;

  return MO;
}

class ObJitMetaSerializer : public llvm::remarks::MetaSerializer
{
public:
  ObJitMetaSerializer(raw_ostream &OS)
    : MetaSerializer(OS)
  {  }

  virtual ~ObJitMetaSerializer() = default;

  inline void emit() override
  {
    // do nothing
  }
};

class ObJitRemarkSerializer : public llvm::remarks::RemarkSerializer
{
public:
  ObJitRemarkSerializer(ObOrcJit &Engine, raw_ostream &OS)
    : RemarkSerializer(remarks::Format::Unknown,
                       OS,
                       remarks::SerializerMode::Standalone),
      JitEngine(Engine)
  {  }

  virtual ~ObJitRemarkSerializer() = default;

  inline void emit(const remarks::Remark &Remark) override
  {
    if ("StackSize" == Remark.RemarkName) {
      for (const auto &arg : Remark.Args) {
        if ("NumStackBytes" == arg.Key) {
          ObString function_name(Remark.FunctionName.size(), Remark.FunctionName.data());
          uint64_t stack_size = 0;

          auto result = std::from_chars(arg.Val.begin(), arg.Val.end(), stack_size);

          if (result.ec != std::errc()) {
            LOG_ERROR_RET(OB_ERR_UNEXPECTED,
                          "failed to get stack size from remark",
                          "arg.Val", ObString(arg.Val.size(), arg.Val.data()),
                          K(stack_size),
                          K(result.ec == std::errc::invalid_argument),
                          K(result.ec == std::errc::result_out_of_range),
                          K(function_name));
          } else {
            LOG_INFO( "[JIT] function stack_size", K(function_name), K(stack_size));
            JitEngine.update_stack_size(stack_size);
          }
        }
      }
    }
  }

  inline std::unique_ptr<llvm::remarks::MetaSerializer>
  metaSerializer(raw_ostream &OS,
#ifndef CPP_STANDARD_20
                 Optional<StringRef> ExternalFilename = None) override
#else
                 std::optional<StringRef> ExternalFilename = std::nullopt) override
#endif
  {
    return std::make_unique<ObJitMetaSerializer>(OS);
  }

private:
  ObOrcJit &JitEngine;
};

/// Compile a Module to an ObjectFile.
Expected<ObPLIRCompiler::CompileResult> ObPLIRCompiler::operator()(Module &M)
{
  CompileResult CachedObject = tryToLoadFromObjectCache(M);
  if (CachedObject) {
    return std::move(CachedObject);
  }

  SmallVector<char, 0> ObjBufferSV;

  {
    raw_svector_ostream ObjStream(ObjBufferSV);

    legacy::PassManager PM;
    MCContext *Ctx;

    if (TM->addPassesToEmitMC(PM, Ctx, ObjStream)) {
      return make_error<StringError>("Target does not support MC emission",
                                     inconvertibleErrorCode());
    } else {
      auto &ctx = M.getContext();

      std::string stream_buffer;
      raw_string_ostream buffer(stream_buffer);
      auto serializer = std::make_unique<ObJitRemarkSerializer>(JitEngine, buffer);
      auto streamer = std::make_unique<remarks::RemarkStreamer>(std::move(serializer));
      auto llvm_streamer = std::make_unique<LLVMRemarkStreamer>(*streamer);

      ctx.setLLVMRemarkStreamer(std::move(llvm_streamer));
      PM.run(M);
      ctx.setLLVMRemarkStreamer(nullptr);
    }
  }

  auto ObjBuffer = std::make_unique<SmallVectorMemoryBuffer>(
      std::move(ObjBufferSV), M.getModuleIdentifier() + "-jitted-objectbuffer");

  auto Obj = object::ObjectFile::createObjectFile(ObjBuffer->getMemBufferRef());

  if (!Obj) {
    return Obj.takeError();
  }

  notifyObjectCompiled(M, *ObjBuffer);
  return std::move(ObjBuffer);
}

ObPLIRCompiler::CompileResult
ObPLIRCompiler::tryToLoadFromObjectCache(const Module &M)
{
  if (!ObjCache) {
    return {};
  }

  return ObjCache->getObject(&M);
}

void ObPLIRCompiler::notifyObjectCompiled(const Module &M,
                                          const MemoryBuffer &ObjBuffer)
{
  if (ObjCache) {
    ObjCache->notifyObjectCompiled(&M, ObjBuffer.getMemBufferRef());
  }
}

} // namespace core
} // namespace jit
} // namespace oceanbase
