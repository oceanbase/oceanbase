/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#if defined(__x86_64__)
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#endif
#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "core/ob_orc_jit.h"
#include "core/ob_pl_ir_compiler.h"
#include "lib/allocator/page_arena.h"
#include "llvm/AsmParser/Parser.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"

using namespace oceanbase::common;
using namespace oceanbase::jit::core;

#if defined(__x86_64__)
namespace
{

// The indirect calls and volatile locals keep both JIT frames alive at the
// sampling point. Start with frame-pointer=none to test the backend override.
const char *TEST_IR = R"(
declare void @external_declaration()

define i64 @jit_inner(void (i8*, i8*)* %callback, i8* %state, i64 %seed) #0 {
  %buf = alloca [32 x i64], align 16
  %slot = getelementptr [32 x i64], [32 x i64]* %buf, i64 0, i64 0
  store volatile i64 %seed, i64* %slot
  %scratch = bitcast [32 x i64]* %buf to i8*
  call void %callback(i8* %state, i8* %scratch)
  %value = load volatile i64, i64* %slot
  ret i64 %value
}

define i64 @jit_outer(i64 (void (i8*, i8*)*, i8*, i64)* %inner,
                      void (i8*, i8*)* %callback, i8* %state, i64 %seed) #0 {
  %buf = alloca [32 x i64], align 16
  %slot = getelementptr [32 x i64], [32 x i64]* %buf, i64 0, i64 0
  store volatile i64 %seed, i64* %slot
  %result = call i64 %inner(void (i8*, i8*)* %callback, i8* %state, i64 %seed)
  %value = load volatile i64, i64* %slot
  %sum = add i64 %value, %result
  ret i64 %sum
}

define i64 @jit_leaf(i64 %value) #0 {
  ret i64 %value
}

define i64 @jit_recursive(void (i8*, i8*)* %callback, i8* %state, i64 %depth) #0 {
  %buf = alloca [32 x i64], align 16
  %slot = getelementptr [32 x i64], [32 x i64]* %buf, i64 0, i64 0
  store volatile i64 %depth, i64* %slot
  %done = icmp eq i64 %depth, 0
  br i1 %done, label %sample, label %recurse
sample:
  %scratch = bitcast [32 x i64]* %buf to i8*
  call void %callback(i8* %state, i8* %scratch)
  %base = load volatile i64, i64* %slot
  ret i64 %base
recurse:
  %next = sub i64 %depth, 1
  %result = call i64 @jit_recursive(void (i8*, i8*)* %callback, i8* %state, i64 %next)
  %value = load volatile i64, i64* %slot
  %sum = add i64 %value, %result
  ret i64 %sum
}

attributes #0 = { noinline nounwind uwtable "frame-pointer"="none" }
)";

class TestObjectCache : public llvm::ObjectCache
{
public:
  void notifyObjectCompiled(const llvm::Module *, llvm::MemoryBufferRef object) override
  {
    object_ = llvm::MemoryBuffer::getMemBufferCopy(object.getBuffer());
    ++compile_count_;
  }

  std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module *) override
  {
    std::unique_ptr<llvm::MemoryBuffer> result;
    if (object_) {
      result = llvm::MemoryBuffer::getMemBufferCopy(object_->getBuffer());
    }
    return result;
  }

  std::unique_ptr<llvm::MemoryBuffer> object_;
  int compile_count_ = 0;
};

class UnsupportedTargetMachine : public llvm::TargetMachine
{
public:
  explicit UnsupportedTargetMachine(const llvm::TargetMachine &target)
    : TargetMachine(target.getTarget(), target.createDataLayout().getStringRepresentation(),
                    target.getTargetTriple(), target.getTargetCPU(), target.getTargetFeatureString(),
                    target.Options)
  {}
};

class InvalidObjectTargetMachine : public UnsupportedTargetMachine
{
public:
  explicit InvalidObjectTargetMachine(const llvm::TargetMachine &target)
    : UnsupportedTargetMachine(target)
  {}

  bool addPassesToEmitMC(llvm::legacy::PassManagerBase &, llvm::MCContext *&context,
                       llvm::raw_pwrite_stream &output, bool) override
  {
    context = nullptr;
    output << "invalid object";
    return false;
  }
};

struct TraceState
{
  void *frames_[64] = {};
  int count_ = 0;
};

using TraceCallback = void (*)(void *, void *);
using InnerFunction = int64_t (*)(TraceCallback, void *, int64_t);
using OuterFunction = int64_t (*)(InnerFunction, TraceCallback, void *, int64_t);

__attribute__((noinline)) void capture_backtrace(void *state, void *)
{
  auto &trace = *static_cast<TraceState *>(state);
  trace.count_ = unw_backtrace(trace.frames_, ARRAYSIZEOF(trace.frames_));
}

__attribute__((noinline)) int64_t invoke_jit(OuterFunction outer, InnerFunction inner, TraceState &trace)
{
  int64_t result = outer(inner, capture_backtrace, &trace, 42);
  // Keep this native frame present so the test can detect truncation at JIT.
  asm volatile("" : "+r"(result) : : "memory");
  return result;
}

void check_uncached_unwind(int level, const char *inner_name, int64_t stack_words,
                          int64_t expected_result, bool require_native_caller)
{
  SCOPED_TRACE(level);
  SCOPED_TRACE(stack_words);
  ObArenaAllocator allocator;
  ObOrcJit jit(allocator);
  ASSERT_EQ(OB_SUCCESS, jit.init());
  auto builder = llvm::orc::JITTargetMachineBuilder::detectHost();
  ASSERT_TRUE(static_cast<bool>(builder)) << llvm::toString(builder.takeError());
  builder->setCodeGenOptLevel(static_cast<llvm::CodeGenOpt::Level>(level));
  if (level == 0) {
    builder->getOptions().EnableFastISel = true;
  }
  auto target = builder->createTargetMachine();
  ASSERT_TRUE(static_cast<bool>(target)) << llvm::toString(target.takeError());
  llvm::LLVMContext context;
  llvm::SMDiagnostic diagnostic;
  std::string ir(TEST_IR);
  const std::string old_type = "[32 x i64]";
  const std::string new_type = "[" + std::to_string(stack_words) + " x i64]";
  size_t offset = 0;
  while ((offset = ir.find(old_type, offset)) != std::string::npos) {
    ir.replace(offset, old_type.size(), new_type);
    offset += new_type.size();
  }
  auto module = llvm::parseAssemblyString(ir, diagnostic, context);
  ASSERT_NE(nullptr, module);
  module->setDataLayout((*target)->createDataLayout());
  module->setTargetTriple((*target)->getTargetTriple().str());

  // Production constructs the compiler without an ObjectCache.
  ObPLIRCompiler compiler(jit, std::move(*target));
  auto object = compiler(*module);
  ASSERT_TRUE(static_cast<bool>(object)) << llvm::toString(object.takeError());
  ASSERT_NE(nullptr, *object);
  ASSERT_EQ(OB_SUCCESS, jit.add_compiled_object(
      (*object)->getBufferSize(), (*object)->getBufferStart()));
  uint64_t inner = 0;
  uint64_t outer = 0;
  ASSERT_EQ(OB_SUCCESS, jit.get_function_address(inner_name, inner));
  ASSERT_EQ(OB_SUCCESS, jit.get_function_address("jit_outer", outer));
  unw_proc_info_t info = {};
  ASSERT_EQ(-UNW_ENOINFO, unw_get_proc_info_by_ip(unw_local_addr_space, inner, &info, nullptr));
  ASSERT_EQ(-UNW_ENOINFO, unw_get_proc_info_by_ip(unw_local_addr_space, outer, &info, nullptr));
  TraceState trace;
  ASSERT_EQ(expected_result, invoke_jit(reinterpret_cast<OuterFunction>(outer),
                                       reinterpret_cast<InnerFunction>(inner), trace));
  ASSERT_GT(trace.count_, 0);
  ASSERT_LE(trace.count_, static_cast<int>(ARRAYSIZEOF(trace.frames_)));
  if (require_native_caller) {
    ASSERT_EQ(0, unw_get_proc_info_by_ip(unw_local_addr_space,
        reinterpret_cast<unw_word_t>(&invoke_jit), &info, nullptr));
    bool reached_native_caller = false;
    for (int i = 0; i < trace.count_; ++i) {
      const unw_word_t pc = reinterpret_cast<unw_word_t>(trace.frames_[i]);
      reached_native_caller |= pc >= info.start_ip && pc < info.end_ip;
    }
    EXPECT_TRUE(reached_native_caller);
  }
}

} // namespace

TEST(TestPLJitFramePointer, unwind_without_registered_fde_and_reuse_object)
{
  for (int level = 0; level <= 3; ++level) {
    SCOPED_TRACE(level);
    ObArenaAllocator allocator;
    ObOrcJit jit(allocator);
    ASSERT_EQ(OB_SUCCESS, jit.init());
    auto builder = llvm::orc::JITTargetMachineBuilder::detectHost();
    ASSERT_TRUE(static_cast<bool>(builder));
    builder->setCodeGenOptLevel(static_cast<llvm::CodeGenOpt::Level>(level));
    if (level == 0) {
      builder->getOptions().EnableFastISel = true;
    }
    auto target = builder->createTargetMachine();
    ASSERT_TRUE(static_cast<bool>(target));
    llvm::LLVMContext context;
    llvm::SMDiagnostic diagnostic;
    auto module = llvm::parseAssemblyString(TEST_IR, diagnostic, context);
    ASSERT_NE(nullptr, module);
    module->setDataLayout((*target)->createDataLayout());
    module->setTargetTriple((*target)->getTargetTriple().str());

    TestObjectCache cache;
    ObPLIRCompiler compiler(jit, std::move(*target), &cache);
    auto object = compiler(*module);
    ASSERT_TRUE(static_cast<bool>(object));
    ASSERT_NE(nullptr, *object);
    for (const llvm::Function &function : *module) {
      if (function.isDeclaration()) {
        EXPECT_FALSE(function.hasFnAttribute("frame-pointer"));
      } else {
        EXPECT_EQ("all", function.getFnAttribute("frame-pointer").getValueAsString());
      }
    }
    auto cached_object = compiler(*module);
    ASSERT_TRUE(static_cast<bool>(cached_object));
    ASSERT_NE(nullptr, *cached_object);
    EXPECT_EQ(1, cache.compile_count_);
    EXPECT_EQ((*object)->getBuffer(), (*cached_object)->getBuffer());

    ASSERT_EQ(OB_SUCCESS, jit.add_compiled_object(
        (*cached_object)->getBufferSize(), (*cached_object)->getBufferStart()));
    uint64_t inner = 0;
    uint64_t outer = 0;
    ASSERT_EQ(OB_SUCCESS, jit.get_function_address("jit_inner", inner));
    ASSERT_EQ(OB_SUCCESS, jit.get_function_address("jit_outer", outer));
    unw_proc_info_t info = {};
    ASSERT_EQ(-UNW_ENOINFO, unw_get_proc_info_by_ip(unw_local_addr_space, inner, &info, nullptr));
    ASSERT_EQ(-UNW_ENOINFO, unw_get_proc_info_by_ip(unw_local_addr_space, outer, &info, nullptr));

    TraceState trace;
    ASSERT_EQ(84, invoke_jit(reinterpret_cast<OuterFunction>(outer),
                            reinterpret_cast<InnerFunction>(inner), trace));
    ASSERT_EQ(0, unw_get_proc_info_by_ip(unw_local_addr_space,
        reinterpret_cast<unw_word_t>(&invoke_jit), &info, nullptr));
    bool reached_native_caller = false;
    for (int i = 0; i < trace.count_; ++i) {
      const unw_word_t pc = reinterpret_cast<unw_word_t>(trace.frames_[i]);
      reached_native_caller |= pc >= info.start_ip && pc < info.end_ip;
    }
    EXPECT_TRUE(reached_native_caller);
  }
}

TEST(TestPLJitFramePointer, report_unsupported_target_without_caching)
{
  ObArenaAllocator allocator;
  ObOrcJit jit(allocator);
  auto builder = llvm::orc::JITTargetMachineBuilder::detectHost();
  ASSERT_TRUE(static_cast<bool>(builder));
  auto target = builder->createTargetMachine();
  ASSERT_TRUE(static_cast<bool>(target));
  TestObjectCache cache;
  ObPLIRCompiler compiler(jit, std::make_unique<UnsupportedTargetMachine>(**target), &cache);
  llvm::LLVMContext context;
  llvm::Module module("unsupported_target", context);
  auto result = compiler(module);
  ASSERT_FALSE(static_cast<bool>(result));
  EXPECT_EQ("Target does not support MC emission", llvm::toString(result.takeError()));
  EXPECT_EQ(0, cache.compile_count_);
  EXPECT_EQ(nullptr, cache.object_);
}

TEST(TestPLJitFramePointer, unwind_without_object_cache)
{
  for (int level = 0; level <= 3; ++level) {
    check_uncached_unwind(level, "jit_inner", 32, 84, true);
  }
}

TEST(TestPLJitFramePointer, unwind_deep_jit_calls)
{
  for (int level = 0; level <= 3; ++level) {
    // The recursive function sums 0..42; jit_outer adds another 42.
    check_uncached_unwind(level, "jit_recursive", 32, 945, true);
  }
}

TEST(TestPLJitFramePointer, safely_unwind_large_stack_frames)
{
  for (int level = 0; level <= 3; ++level) {
    // libunwind may truncate its fallback at large frames. Both a complete
    // trace and safe truncation are valid; the JIT call must finish normally.
    check_uncached_unwind(level, "jit_inner", 4096, 84, false);
  }
}

TEST(TestPLJitFramePointer, concurrent_compile_unwind_and_destroy)
{
  std::vector<std::thread> workers;
  for (int worker = 0; worker < 4; ++worker) {
    workers.emplace_back([worker]() {
      for (int iteration = 0; iteration < 8; ++iteration) {
        SCOPED_TRACE(worker);
        SCOPED_TRACE(iteration);
        check_uncached_unwind((worker + iteration) % 4, "jit_inner", 32, 84, true);
      }
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }
}

TEST(TestPLJitFramePointer, reject_invalid_object_without_caching)
{
  ObArenaAllocator allocator;
  ObOrcJit jit(allocator);
  auto builder = llvm::orc::JITTargetMachineBuilder::detectHost();
  ASSERT_TRUE(static_cast<bool>(builder)) << llvm::toString(builder.takeError());
  auto target = builder->createTargetMachine();
  ASSERT_TRUE(static_cast<bool>(target)) << llvm::toString(target.takeError());
  TestObjectCache cache;
  ObPLIRCompiler compiler(jit, std::make_unique<InvalidObjectTargetMachine>(**target), &cache);
  llvm::LLVMContext context;
  llvm::Module module("invalid_object", context);
  auto result = compiler(module);
  ASSERT_FALSE(static_cast<bool>(result));
  EXPECT_FALSE(llvm::toString(result.takeError()).empty());
  EXPECT_EQ(0, cache.compile_count_);
  EXPECT_EQ(nullptr, cache.object_);
}
#endif

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
  ObNotifyLoaded::initGdbHelper();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
