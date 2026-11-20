// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "../connector_base.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <chrono>
#include <cstring>
#include <deque>
#include <filesystem>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace lmcache {
namespace connector {

// Key encoding constants — must match fs_l2_adapter.py
static constexpr char KEY_SEP = '@';
static constexpr const char* PATH_SLASH_REPLACEMENT = "-SEP-";
static constexpr const char* FILE_EXT = ".data";
static constexpr const char* TMP_EXT = ".tmp";

// Per-worker connection state for the FS connector.
// Each worker maintains its own I/O buffer for O_DIRECT.
struct WorkerFSConn {
  std::filesystem::path base_path;
  std::filesystem::path tmp_dir;  // empty if not configured
  bool use_odirect = false;
  size_t disk_block_size = 0;
  // If > 0, trigger filesystem readahead by issuing a small
  // initial read of this many bytes before reading the rest.
  size_t read_ahead_size = 0;
};

class FSConnector : public ConnectorBase<WorkerFSConn> {
 public:
  FSConnector(std::string base_path, int num_workers,
              std::string relative_tmp_dir = "", bool use_odirect = false,
              size_t read_ahead_size = 0);
  ~FSConnector() override;

  std::vector<IOSample> drain_io_samples();
  std::pair<uint64_t, uint64_t> get_io_inflight_counts() const;
  std::pair<uint64_t, uint64_t> drain_io_peak_counts();

 protected:
  WorkerFSConn create_connection() override;
  void do_batch_get(WorkerFSConn& conn, const Request& req) override;
  void do_batch_set(WorkerFSConn& conn, const Request& req) override;
  void do_single_get(WorkerFSConn& conn, const std::string& key, void* buf,
                     size_t len, size_t chunk_size) override;
  void do_single_set(WorkerFSConn& conn, const std::string& key,
                     const void* buf, size_t len, size_t chunk_size) override;
  bool do_single_exists(WorkerFSConn& conn, const std::string& key) override;
  bool do_single_delete(WorkerFSConn& conn, const std::string& key) override;

 private:
  // Build the filesystem-safe filename from a serialized key string.
  //
  // Input key (from NativeConnectorL2Adapter._object_key_to_string):
  //   Unsalted: "{model}@{kv_rank:08x}@{hash.hex()}"
  //   Salted  : "{model}@{kv_rank:08x}@{hash.hex()}@{cache_salt}"
  //
  // Output filename (matching fs_l2_adapter.py._object_key_to_filename):
  //   Unsalted: "{safe_model}@{kv_rank:#010x}@{hash.hex()}.data"
  //   Salted  : "{safe_model}@{kv_rank:#010x}@{hash.hex()}@{cache_salt}.data"
  //
  // Differences from the input: '/' in model becomes '-SEP-', kv_rank
  // gains a '0x' prefix, and '.data' is appended. Both model_name and
  // cache_salt are forbidden from containing '@' (enforced on the
  // Python side), so the parse is unambiguous.
  static std::string key_to_filename(const std::string& key);

  static std::string replace_all(const std::string& str,
                                 const std::string& from,
                                 const std::string& to);
  void do_single_set_impl(WorkerFSConn& conn, const std::string& key,
                          const void* buf, size_t len, size_t chunk_size,
                          bool* did_attempt,
                          std::chrono::steady_clock::time_point* started_at);
  void record_io_sample(bool is_write, size_t io_size,
                        std::chrono::steady_clock::time_point submitted_at,
                        std::chrono::steady_clock::time_point started_at,
                        std::chrono::steady_clock::time_point finished_at,
                        bool ok);

  std::string base_path_;
  std::string relative_tmp_dir_;
  bool use_odirect_;
  size_t disk_block_size_;
  size_t read_ahead_size_;
  std::mutex io_samples_mu_;
  std::deque<IOSample> io_samples_;
  mutable std::mutex io_concurrency_mu_;
  uint64_t inflight_reads_ = 0;
  uint64_t inflight_writes_ = 0;
  uint64_t peak_reads_ = 0;
  uint64_t peak_writes_ = 0;
};

}  // namespace connector
}  // namespace lmcache
