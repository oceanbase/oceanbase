/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_sandbox_setup.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <limits.h>
#include <link.h>
#include <dlfcn.h>
#include <elf.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <linux/audit.h>
#include <stddef.h>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// Minimal BPF helpers — kept local so we don't depend on libminijail's
// internal headers (bpf.h is not exported by the deps/devel install).
//
// Resolution actions encoded as the BPF return value:
//   SECCOMP_RET_ALLOW       — let the syscall through
//   SECCOMP_RET_ERRNO | err — return -1 with errno=err to userspace
//   SECCOMP_RET_KILL        — kill the calling thread
#ifndef SECCOMP_RET_KILL
#define SECCOMP_RET_KILL 0x00000000U
#endif
#ifndef SECCOMP_RET_ERRNO
#define SECCOMP_RET_ERRNO 0x00050000U
#endif
#ifndef SECCOMP_RET_ALLOW
#define SECCOMP_RET_ALLOW 0x7fff0000U
#endif
#ifndef SECCOMP_RET_DATA
#define SECCOMP_RET_DATA 0x0000ffffU
#endif

// Architecture audit constant for filter portability.
#if defined(__x86_64__)
#define OB_SECCOMP_ARCH AUDIT_ARCH_X86_64
#elif defined(__aarch64__)
#define OB_SECCOMP_ARCH AUDIT_ARCH_AARCH64
#else
#error "Unsupported architecture for seccomp blacklist filter"
#endif

// Offsets into struct seccomp_data {int nr; __u32 arch; __u64 ip; __u64 args[6];}
#define OB_SECCOMP_OFF_NR    (offsetof(struct seccomp_data, nr))
#define OB_SECCOMP_OFF_ARCH  (offsetof(struct seccomp_data, arch))

#define USING_LOG_PREFIX COMMON

// Blacklist of syscalls denied inside the sandbox
struct BannedSyscall {
  const char* name;
  int nr;
};

static const BannedSyscall BANNED_SYSCALLS[] = {
#ifdef __NR_acct
    {"acct", __NR_acct},
#endif
#ifdef __NR_add_key
    {"add_key", __NR_add_key},
#endif
#ifdef __NR_bpf
    {"bpf", __NR_bpf},
#endif
#ifdef __NR_clock_adjtime
    {"clock_adjtime", __NR_clock_adjtime},
#endif
#ifdef __NR_clock_settime
    {"clock_settime", __NR_clock_settime},
#endif
#ifdef __NR_create_module
    {"create_module", __NR_create_module},
#endif
#ifdef __NR_delete_module
    {"delete_module", __NR_delete_module},
#endif
#ifdef __NR_finit_module
    {"finit_module", __NR_finit_module},
#endif
#ifdef __NR_get_kernel_syms
    {"get_kernel_syms", __NR_get_kernel_syms},
#endif
#ifdef __NR_get_mempolicy
    {"get_mempolicy", __NR_get_mempolicy},
#endif
#ifdef __NR_init_module
    {"init_module", __NR_init_module},
#endif
#ifdef __NR_ioperm
    {"ioperm", __NR_ioperm},
#endif
#ifdef __NR_iopl
    {"iopl", __NR_iopl},
#endif
#ifdef __NR_kcmp
    {"kcmp", __NR_kcmp},
#endif
#ifdef __NR_kexec_file_load
    {"kexec_file_load", __NR_kexec_file_load},
#endif
#ifdef __NR_kexec_load
    {"kexec_load", __NR_kexec_load},
#endif
#ifdef __NR_keyctl
    {"keyctl", __NR_keyctl},
#endif
#ifdef __NR_lookup_dcookie
    {"lookup_dcookie", __NR_lookup_dcookie},
#endif
#ifdef __NR_mbind
    {"mbind", __NR_mbind},
#endif
#ifdef __NR_migrate_pages
    {"migrate_pages", __NR_migrate_pages},
#endif
#ifdef __NR_mount
    {"mount", __NR_mount},
#endif
#ifdef __NR_move_pages
    {"move_pages", __NR_move_pages},
#endif
#ifdef __NR_name_to_handle_at
    {"name_to_handle_at", __NR_name_to_handle_at},
#endif
#ifdef __NR_nfsservctl
    {"nfsservctl", __NR_nfsservctl},
#endif
#ifdef __NR_open_by_handle_at
    {"open_by_handle_at", __NR_open_by_handle_at},
#endif
#ifdef __NR_perf_event_open
    {"perf_event_open", __NR_perf_event_open},
#endif
#ifdef __NR_personality
    {"personality", __NR_personality},
#endif
#ifdef __NR_pivot_root
    {"pivot_root", __NR_pivot_root},
#endif
#ifdef __NR_process_vm_readv
    {"process_vm_readv", __NR_process_vm_readv},
#endif
#ifdef __NR_process_vm_writev
    {"process_vm_writev", __NR_process_vm_writev},
#endif
#ifdef __NR_ptrace
    {"ptrace", __NR_ptrace},
#endif
#ifdef __NR_query_module
    {"query_module", __NR_query_module},
#endif
#ifdef __NR_quotactl
    {"quotactl", __NR_quotactl},
#endif
#ifdef __NR_reboot
    {"reboot", __NR_reboot},
#endif
#ifdef __NR_request_key
    {"request_key", __NR_request_key},
#endif
#ifdef __NR_set_mempolicy
    {"set_mempolicy", __NR_set_mempolicy},
#endif
#ifdef __NR_setns
    {"setns", __NR_setns},
#endif
#ifdef __NR_settimeofday
    {"settimeofday", __NR_settimeofday},
#endif
#ifdef __NR_stime
    {"stime", __NR_stime},
#endif
#ifdef __NR_swapoff
    {"swapoff", __NR_swapoff},
#endif
#ifdef __NR_swapon
    {"swapon", __NR_swapon},
#endif
#ifdef __NR_sysfs
    {"sysfs", __NR_sysfs},
#endif
#ifdef __NR_syslog
    {"syslog", __NR_syslog},
#endif
#ifdef __NR_umount
    {"umount", __NR_umount},
#endif
#ifdef __NR_umount2
    {"umount2", __NR_umount2},
#endif
#ifdef __NR_unshare
    {"unshare", __NR_unshare},
#endif
#ifdef __NR_uselib
    {"uselib", __NR_uselib},
#endif
#ifdef __NR_userfaultfd
    {"userfaultfd", __NR_userfaultfd},
#endif
#ifdef __NR_ustat
    {"ustat", __NR_ustat},
#endif
#ifdef __NR_vm86
    {"vm86", __NR_vm86},
#endif
#ifdef __NR_vm86old
    {"vm86old", __NR_vm86old},
#endif
};

static const size_t BANNED_SYSCALLS_COUNT =
    sizeof(BANNED_SYSCALLS) / sizeof(BANNED_SYSCALLS[0]);

// Build a default-ALLOW + per-banned-syscall EPERM BPF program.
//
// Layout (instruction count in brackets):
//   [load arch into A]                       (1)
//   [JEQ A == OB_SECCOMP_ARCH ? jt=0 : jf=1] (1)
//   [RET KILL]  // wrong arch                (1)
//   [load syscall_nr into A]                 (1)
//   for each banned syscall:
//     [JEQ A == nr ? jt=0 : jf=1]            (1)
//     [RET ERRNO(EPERM)]                     (1)
//   [RET ALLOW]                               (1)
int build_blacklist_bpf_program(struct sock_fprog *out_prog) {
  int ret = oceanbase::common::OB_SUCCESS;
  if (out_prog == nullptr) {
    ret = oceanbase::common::OB_INVALID_ARGUMENT;
    LOG_ERROR("build_blacklist_bpf_program: out_prog is null", K(ret));
  } else {
    const size_t arch_block = 3U;
    const size_t load_nr_block = 1U;
    const size_t per_banned = 2U;
    const size_t fallthrough = 1U;
    const size_t total = arch_block + load_nr_block
                       + BANNED_SYSCALLS_COUNT * per_banned + fallthrough;
    struct sock_filter *filter =
        (struct sock_filter*)calloc(total, sizeof(struct sock_filter));
    if (filter == nullptr) {
      ret = oceanbase::common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("calloc BPF program failed", K(ret), K(total));
    } else if (total > USHRT_MAX) {
      // sock_fprog::len is unsigned short. With the current banned list this
      // is impossible (~50 entries), but check defensively in case the list
      // grows or someone adds per-syscall arg-checks later.
      ret = oceanbase::common::OB_ERR_UNEXPECTED;
      LOG_ERROR("BPF program too large for sock_fprog", K(ret), K(total));
      free(filter);
    } else {
      size_t pos = 0;

      // Arch validation: kill if running on an unexpected arch (defense
      // against syscall-number aliasing across personalities).
      //
      // BPF JEQ semantics:
      //   jt = forward offset to take when A == K (match)
      //   jf = forward offset to take when A != K (no match)
      // We want: arch matches -> skip the KILL and fall through to the
      // syscall-nr load; arch doesn't match -> fall through to KILL.
      // So jt=1 (skip 1 instruction over KILL), jf=0 (next instruction).
      filter[pos++] = (struct sock_filter)BPF_STMT(
          BPF_LD + BPF_W + BPF_ABS, (uint32_t)OB_SECCOMP_OFF_ARCH);
      filter[pos++] = (struct sock_filter)BPF_JUMP(
          BPF_JMP + BPF_JEQ + BPF_K, OB_SECCOMP_ARCH, /*jt=*/1, /*jf=*/0);
      filter[pos++] = (struct sock_filter)BPF_STMT(
          BPF_RET + BPF_K, SECCOMP_RET_KILL);

      // Load syscall number into accumulator.
      filter[pos++] = (struct sock_filter)BPF_STMT(
          BPF_LD + BPF_W + BPF_ABS, (uint32_t)OB_SECCOMP_OFF_NR);

      // Per-banned-syscall: JEQ nr → RET EPERM, otherwise skip 1.
      for (size_t i = 0; i < BANNED_SYSCALLS_COUNT; ++i) {
        filter[pos++] = (struct sock_filter)BPF_JUMP(
            BPF_JMP + BPF_JEQ + BPF_K,
            (uint32_t)BANNED_SYSCALLS[i].nr,
            /*jt=*/0, /*jf=*/1);
        filter[pos++] = (struct sock_filter)BPF_STMT(
            BPF_RET + BPF_K,
            SECCOMP_RET_ERRNO | (EPERM & SECCOMP_RET_DATA));
      }

      // Default: ALLOW (this is the blacklist-mode semantics).
      filter[pos++] = (struct sock_filter)BPF_STMT(
          BPF_RET + BPF_K, SECCOMP_RET_ALLOW);

      if (pos != total) {
        ret = oceanbase::common::OB_ERR_UNEXPECTED;
        LOG_ERROR("BPF program length mismatch", K(ret), K(pos), K(total));
        free(filter);
      } else {
        out_prog->len = (unsigned short)total;
        out_prog->filter = filter;
        LOG_INFO("Built blacklist seccomp BPF program",
                 "instructions", total,
                 "banned_count", BANNED_SYSCALLS_COUNT);
      }
    }
  }
  return ret;
}

// Read the PT_INTERP path from an ELF64 executable.
// PT_INTERP is the dynamic linker the kernel uses when execve'ing this binary;
// it is the single authoritative source for "which ld.so is this binary paired with".
// On success, `out` holds the absolute path string (NUL-terminated in ELF).
static int read_elf_interp(const char* elf_path, std::string& out) {
  int ret = oceanbase::common::OB_SUCCESS;
  int fd = ::open(elf_path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    ret = oceanbase::common::OB_IO_ERROR;
    LOG_WARN("open ELF for PT_INTERP failed", K(elf_path), K(errno));
  } else {
    Elf64_Ehdr ehdr;
    ssize_t n = ::pread(fd, &ehdr, sizeof(ehdr), 0);
    bool header_ok = (n == (ssize_t)sizeof(ehdr))
                  && (memcmp(ehdr.e_ident, ELFMAG, SELFMAG) == 0)
                  && (ehdr.e_ident[EI_CLASS] == ELFCLASS64);
    if (!header_ok) {
      ret = oceanbase::common::OB_INVALID_DATA;
      LOG_WARN("not a valid ELF64", K(elf_path));
    } else if (ehdr.e_phnum == 0 || ehdr.e_phentsize != sizeof(Elf64_Phdr)) {
      ret = oceanbase::common::OB_INVALID_DATA;
      LOG_WARN("unexpected phdr layout", K(elf_path),
               "e_phnum", (int)ehdr.e_phnum,
               "e_phentsize", (int)ehdr.e_phentsize);
    } else {
      std::vector<Elf64_Phdr> phdrs(ehdr.e_phnum);
      ssize_t ph_bytes = (ssize_t)(ehdr.e_phnum * sizeof(Elf64_Phdr));
      n = ::pread(fd, phdrs.data(), ph_bytes, ehdr.e_phoff);
      if (n != ph_bytes) {
        ret = oceanbase::common::OB_IO_ERROR;
        LOG_WARN("pread phdrs failed", K(elf_path), K(errno));
      } else {
        bool found = false;
        for (const auto& ph : phdrs) {
          if (ph.p_type == PT_INTERP) {
            if (ph.p_filesz == 0 || ph.p_filesz > PATH_MAX) {
              ret = oceanbase::common::OB_INVALID_DATA;
              LOG_WARN("PT_INTERP size out of range", K(elf_path),
                       "size", (uint64_t)ph.p_filesz);
            } else {
              std::vector<char> buf(ph.p_filesz);
              n = ::pread(fd, buf.data(), ph.p_filesz, ph.p_offset);
              if (n != (ssize_t)ph.p_filesz) {
                ret = oceanbase::common::OB_IO_ERROR;
                LOG_WARN("pread PT_INTERP failed", K(elf_path), K(errno));
              } else {
                out.assign(buf.data());   // stops at first NUL, matches ELF encoding
                found = true;
              }
            }
            break;
          }
        }
        if (!found && ret == oceanbase::common::OB_SUCCESS) {
          ret = oceanbase::common::OB_ENTRY_NOT_EXIST;
          LOG_WARN("no PT_INTERP in ELF", K(elf_path));
        }
      }
    }
    ::close(fd);
  }
  return ret;
}

// Helper function to copy a file using C++17 filesystem.
// Behavior:
//   - If dst already matches src in size and mtime, skip (fast path for idempotent reboots).
//   - Otherwise overwrite dst with src, then sync dst's mtime to src's
//     so future staleness checks work against the upstream time, not the copy time.
static int copy_file_fs(const fs::path& src, const fs::path& dst) {
  int ret = oceanbase::common::OB_SUCCESS;

  try {
    // Create parent directories if needed
    fs::path dst_dir = dst.parent_path();
    if (!dst_dir.empty() && !fs::exists(dst_dir)) {
      std::error_code ec;
      fs::create_directories(dst_dir, ec);
      if (ec) {
        LOG_WARN("Failed to create directory (non-fatal)", "path", dst_dir.c_str(), K(ec.value()));
      }
    }

    if (!fs::exists(src)) {
      ret = oceanbase::common::OB_IO_ERROR;
      LOG_WARN("Source file not found", "src", src.c_str());
    } else {
      bool up_to_date = false;
      std::error_code ec;
      if (fs::exists(dst)) {
        auto src_size = fs::file_size(src, ec);
        auto dst_size = ec ? (uintmax_t)0 : fs::file_size(dst, ec);
        if (!ec && src_size == dst_size) {
          auto src_time = fs::last_write_time(src, ec);
          auto dst_time = ec ? fs::file_time_type::min() : fs::last_write_time(dst, ec);
          if (!ec && dst_time == src_time) {
            up_to_date = true;
          }
        }
      }

      if (!up_to_date) {
        std::error_code copy_ec;
        fs::copy_file(src, dst, fs::copy_options::overwrite_existing, copy_ec);
        if (copy_ec) {
          ret = oceanbase::common::OB_IO_ERROR;
          LOG_WARN("Failed to copy file (non-fatal)", "src", src.c_str(), "dst", dst.c_str(), K(copy_ec.value()));
        } else {
          // Sync mtime so that subsequent runs can detect upstream changes
          // (including downgrades) reliably via size+mtime comparison.
          std::error_code t_ec;
          auto src_time = fs::last_write_time(src, t_ec);
          if (!t_ec) {
            fs::last_write_time(dst, src_time, t_ec);
          }
          LOG_INFO("Copied file", "src", src.c_str(), "dst", dst.c_str());
        }
      }
    }
  } catch (const fs::filesystem_error& e) {
    ret = oceanbase::common::OB_IO_ERROR;
    LOG_WARN("Filesystem error during copy (non-fatal)", "src", src.c_str(), "dst", dst.c_str(), "what", e.what());
  }

  return ret;
}

// Callback for dl_iterate_phdr
struct CopySharedLibsData {
  const fs::path root_path;
  int success_count;
  int fail_count;
};

static int copy_shared_libs_callback(struct dl_phdr_info *info, size_t size, void *data) {
  int ret = 0;
  CopySharedLibsData* copy_data = static_cast<CopySharedLibsData*>(data);

  // Skip if no name or empty name (main executable or vDSO)
  bool skip = (info->dlpi_name == NULL || info->dlpi_name[0] == '\0');
  const char* src_path = info->dlpi_name;

  // Only copy absolute paths (system libraries)
  if (!skip && src_path[0] != '/') {
    skip = true;
  }

  if (!skip) {
    // Build destination path and let copy_file_fs handle up-to-date check + overwrite.
    fs::path src(src_path);
    fs::path dst = copy_data->root_path / src.relative_path();
    int copy_ret = copy_file_fs(src, dst);
    if (copy_ret != oceanbase::common::OB_SUCCESS) {
      copy_data->fail_count++;
    } else {
      copy_data->success_count++;
    }
  }

  return ret;
}

// Copy required system files
static int copy_system_files(const fs::path& root_path) {
  int ret = oceanbase::common::OB_SUCCESS;

  // Files to copy: /etc/localtime, /usr/lib/locale/locale-archive, libselinux.so.1
  const char* files_to_copy[] = {
    "/etc/localtime",
    "/usr/lib/locale/locale-archive",
    "/lib64/libselinux.so.1",
    "/lib64/libcap.so.2",
    "/lib64/libpcre2-8.so.0",
    "/lib64/libutil.so.1",       // Python interpreter (libpython3) links libutil for pty/openpty
    "/lib64/libstdc++.so.6",     // pyarrow C++ runtime (libarrow_python.so, Cython .so modules)
    "/lib64/libgcc_s.so.1",      // pyarrow GCC runtime (same as above)
    "/lib64/libz.so.1",          // Python zlib module (zlib.cpython-*.so), Cython COMPRESS_STRINGS needs it
    NULL
  };

  for (int i = 0; files_to_copy[i] != NULL; ++i) {
    fs::path src(files_to_copy[i]);
    fs::path dst = root_path / src.relative_path();

    // Check if source file exists
    if (!fs::exists(src)) {
      LOG_WARN("System file not found (non-fatal)", "src", src.c_str());
      continue;
    }

    copy_file_fs(src, dst);
  }

  return ret;
}

// Copy the dynamic linker (ld-linux).
// Primary source: /proc/self/exe's PT_INTERP — the linker ob_sandbox itself
// was built against, which is almost certainly the same one Python and other
// system binaries use on this host. Fallback: a short candidate list for
// environments where procfs is unavailable.
static int copy_dynamic_linker(const fs::path& root_path) {
  int ret = oceanbase::common::OB_SUCCESS;
  bool copied = false;

  // Primary: read PT_INTERP from our own ELF.
  std::string interp;
  if (read_elf_interp("/proc/self/exe", interp) == oceanbase::common::OB_SUCCESS
      && !interp.empty()) {
    fs::path src(interp);
    if (fs::exists(src)) {
      fs::path dst = root_path / src.relative_path();
      if (copy_file_fs(src, dst) == oceanbase::common::OB_SUCCESS) {
        copied = true;
        LOG_INFO("copied dynamic linker from PT_INTERP", "path", interp.c_str());
      }
    } else {
      LOG_WARN("PT_INTERP points to nonexistent file", "path", interp.c_str());
    }
  }

  // Fallback: hardcoded candidate list, used only when PT_INTERP is unavailable
  // or points to a missing file. Kept short since the primary path covers
  // the common cases.
  if (!copied) {
    const char* ld_paths[] = {
      "/lib64/ld-linux-x86-64.so.2",      // x86_64 glibc
      "/lib/ld-linux-aarch64.so.1",       // aarch64 glibc
      "/lib/ld-linux.so.2",               // 32-bit x86
      "/lib64/ld-lsb-x86-64.so.3",        // LSB compatible
      NULL
    };
    for (int i = 0; ld_paths[i] != NULL && !copied; ++i) {
      fs::path src(ld_paths[i]);
      if (!fs::exists(src)) {
        continue;
      }
      fs::path dst = root_path / src.relative_path();
      if (copy_file_fs(src, dst) == oceanbase::common::OB_SUCCESS) {
        copied = true;
      }
    }
  }

  if (!copied) {
    LOG_WARN("Could not find dynamic linker to copy");
  }

  return ret;
}

int prepare_sandbox_root() {
  int ret = oceanbase::common::OB_SUCCESS;
  fs::path root_path(OB_SANDBOX_ROOT_PATH);

  try {
    // Create root directory structure
    std::error_code ec;
    fs::create_directories(root_path, ec);
    if (ec) {
      LOG_ERROR("Failed to create sandbox root directory", "path", root_path.c_str(), K(ec.value()));
      ret = oceanbase::common::OB_IO_ERROR;
    } else {
      // Create necessary subdirectories
      const char* subdirs[] = {
        "lib",
        "lib64",
        "usr/lib",
        "usr/lib64",
        "usr/lib/locale",
        "etc",
        "proc",
        "dev",
        NULL
      };

      for (int i = 0; subdirs[i] != NULL; ++i) {
        fs::path subdir = root_path / subdirs[i];
        fs::create_directories(subdir, ec);
        if (ec) {
          LOG_WARN("Failed to create subdirectory (non-fatal)", "path", subdir.c_str(), K(ec.value()));
        }
      }
    }
  } catch (const fs::filesystem_error& e) {
    LOG_ERROR("Filesystem error creating directories", "what", e.what());
    ret = oceanbase::common::OB_IO_ERROR;
  }

  if (OB_SUCC(ret)) {
    // Create empty files in dev/ as bind-mount points for device nodes.
    // minijail_bind() requires targets to pre-exist in the sandbox root.
    static const char* dev_nodes[] = {
      "dev/null", "dev/zero", "dev/urandom", "dev/random", "dev/full", NULL
    };
    for (int i = 0; dev_nodes[i] != NULL; ++i) {
      fs::path dev_path = root_path / dev_nodes[i];
      if (!fs::exists(dev_path)) {
        FILE *fp = fopen(dev_path.c_str(), "w");
        if (fp) {
          fclose(fp);
        } else {
          LOG_WARN("Failed to create dev mount point (non-fatal)", "path", dev_path.c_str(), K(errno));
        }
      }
    }

    // Copy system files (localtime, locale-archive)
    copy_system_files(root_path);

    // Copy dynamic linker
    copy_dynamic_linker(root_path);

    // Use dl_iterate_phdr to copy all loaded shared libraries
    CopySharedLibsData copy_data = {root_path, 0, 0};
    dl_iterate_phdr(copy_shared_libs_callback, &copy_data);

    LOG_INFO("Sandbox root preparation completed",
             "path", root_path.c_str(), "libs_copied", copy_data.success_count,
             "libs_failed", copy_data.fail_count);
  }

  return ret;
}

// Fallback dynamic linker path. Used only when reading PT_INTERP from the
// target binary fails (procfs unavailable, binary unreadable, corrupt ELF).
// Kept as the x86_64 glibc default since that covers OceanBase's supported distros.
static const char* DEFAULT_LINKER_PATH = "/lib64/ld-linux-x86-64.so.2";

// Parse `ld.so --list <binary>` output, collecting absolute paths of dependencies.
// Handles three kinds of lines:
//   "    libfoo.so.1 => /abs/path/libfoo.so.1 (0x...)"
//   "    /abs/path/ld-linux-x86-64.so.2 (0x...)"          (the linker itself)
//   "    linux-vdso.so.1 (0x...)"                          (vDSO, skipped)
//   "    libmissing.so.1 => not found"                     (logs WARN, skipped)
static void parse_ldd_output(const std::string& buf, std::vector<std::string>& out_paths) {
  size_t start = 0;
  for (size_t i = 0; i <= buf.size(); ++i) {
    if (i < buf.size() && buf[i] != '\n') {
      continue;
    }
    std::string line(buf, start, i - start);
    start = i + 1;

    if (line.empty()) {
      continue;
    }
    if (line.find("linux-vdso") != std::string::npos
        || line.find("linux-gate") != std::string::npos) {
      continue;
    }
    if (line.find("not found") != std::string::npos) {
      LOG_WARN_RET(oceanbase::common::OB_ENTRY_NOT_EXIST,"ld.so reports missing library (non-fatal)", "line", line.c_str());
      continue;
    }

    const char* p = nullptr;
    size_t arrow = line.find("=> ");
    if (arrow != std::string::npos) {
      p = line.c_str() + arrow + 3;
    } else {
      // Line without "=>", e.g. the linker itself. Take the first token if absolute.
      size_t first = line.find_first_not_of(" \t");
      if (first != std::string::npos && line[first] == '/') {
        p = line.c_str() + first;
      }
    }
    if (p != nullptr && *p == '/') {
      const char* end = p;
      while (*end != '\0' && *end != ' ' && *end != '\t' && *end != '\n') {
        ++end;
      }
      out_paths.emplace_back(p, end - p);
    }
  }
}

// Run `<linker> --list <binary>` via vfork + syscall(SYS_execve) and capture stdout/stderr.
// vfork is used to avoid duplicating ob_sandbox's page table; the child is constrained
// to async-signal-safe calls only (dup2/close/syscall/_exit) between vfork and execve.
static int run_ld_list(const char* linker_path,
                       const char* binary_path,
                       std::string& output) {
  int ret = oceanbase::common::OB_SUCCESS;
  int pipefd[2] = {-1, -1};

  if (::pipe2(pipefd, O_CLOEXEC) != 0) {
    ret = oceanbase::common::OB_IO_ERROR;
    LOG_ERROR("pipe2 failed", K(errno));
  } else {
    // argv/envp must be fully prepared BEFORE vfork — child cannot allocate.
    const char* argv[] = { linker_path, "--list", binary_path, nullptr };
    const char* envp[] = { "PATH=/usr/bin:/bin", "LC_ALL=C", nullptr };

    pid_t pid = ::vfork();
    if (pid < 0) {
      ret = oceanbase::common::OB_ERR_SYS;
      LOG_ERROR("vfork failed for ld.so --list", K(errno));
      ::close(pipefd[0]);
      ::close(pipefd[1]);
    } else if (pid == 0) {
      // Child: vfork context. Only async-signal-safe calls allowed.
      ::dup2(pipefd[1], STDOUT_FILENO);
      ::dup2(pipefd[1], STDERR_FILENO);
      ::close(pipefd[0]);
      ::close(pipefd[1]);
      ::syscall(SYS_execve, linker_path,
                (char* const*)argv, (char* const*)envp);
      ::_exit(127);
    } else {
      // Parent
      ::close(pipefd[1]);
      char chunk[4096];
      ssize_t n = 0;
      while ((n = ::read(pipefd[0], chunk, sizeof(chunk))) > 0) {
        output.append(chunk, n);
      }
      ::close(pipefd[0]);

      int status = 0;
      pid_t wret = ::waitpid(pid, &status, 0);
      if (wret < 0) {
        ret = oceanbase::common::OB_ERR_SYS;
        LOG_ERROR("waitpid failed for ld.so --list", K(errno));
      } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        ret = oceanbase::common::OB_ERR_UNEXPECTED;
        LOG_ERROR("ld.so --list exited with error",
                  "binary", binary_path,
                  "exited", WIFEXITED(status),
                  "exit_code", WIFEXITED(status) ? WEXITSTATUS(status) : -1);
      }
    }
  }
  return ret;
}

// Discover and copy the runtime dependency closure of `binary_path` into the sandbox root.
// Best-effort: errors are logged at ERROR level but never propagated back to the caller.
// Called from handle_create on the first encounter with each binary_path.
int copy_binary_dependencies(const char* binary_path) {
  int ret = oceanbase::common::OB_SUCCESS;
  if (binary_path == nullptr || binary_path[0] == '\0') {
    ret = oceanbase::common::OB_INVALID_ARGUMENT;
    LOG_ERROR("copy_binary_dependencies: empty binary_path");
  } else {
    fs::path root_path(OB_SANDBOX_ROOT_PATH);

    // Resolve the linker to use via the target binary's PT_INTERP. That is
    // the exact linker the kernel would pick at execve time, so running
    // `<linker> --list <binary>` yields the same resolution the real load
    // would see. Fall back to DEFAULT_LINKER_PATH if PT_INTERP is unreadable.
    std::string linker;
    if (read_elf_interp(binary_path, linker) != oceanbase::common::OB_SUCCESS
        || linker.empty()) {
      LOG_WARN("read PT_INTERP for binary failed, falling back to default",
               "binary", binary_path);
      linker = DEFAULT_LINKER_PATH;
    }

    std::string output;
    int list_ret = run_ld_list(linker.c_str(), binary_path, output);
    if (list_ret != oceanbase::common::OB_SUCCESS) {
      LOG_ERROR("run_ld_list failed (best-effort, continuing)",
                "binary", binary_path, "linker", linker.c_str(), K(list_ret));
    } else {
      std::vector<std::string> deps;
      parse_ldd_output(output, deps);
      int success = 0;
      int failed = 0;
      for (const auto& dep : deps) {
        fs::path src(dep);
        fs::path dst = root_path / src.relative_path();
        if (copy_file_fs(src, dst) == oceanbase::common::OB_SUCCESS) {
          ++success;
        } else {
          ++failed;
        }
      }
      LOG_INFO("Copied binary dependencies",
               "binary", binary_path,
               "linker", linker.c_str(),
               "deps", deps.size(),
               "success", success,
               "failed", failed);
    }
  }
  return ret;
}
