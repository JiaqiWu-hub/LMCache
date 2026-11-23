// SPDX-License-Identifier: Apache-2.0

#include "connector.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <stdexcept>
#include <string>

namespace lmcache {
namespace connector {

using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

namespace {

class IOConcurrencyGuard {
 public:
  IOConcurrencyGuard(uint64_t& inflight, uint64_t& peak, std::mutex& mu)
      : inflight_(inflight), mu_(mu) {
    std::lock_guard<std::mutex> lock(mu_);
    ++inflight_;
    peak = std::max(peak, inflight_);
  }

  ~IOConcurrencyGuard() {
    std::lock_guard<std::mutex> lock(mu_);
    --inflight_;
  }

  IOConcurrencyGuard(const IOConcurrencyGuard&) = delete;
  IOConcurrencyGuard& operator=(const IOConcurrencyGuard&) = delete;

 private:
  uint64_t& inflight_;
  std::mutex& mu_;
};

}  // namespace

std::string FSConnector::replace_all(const std::string& str,
                                     const std::string& from,
                                     const std::string& to) {
  std::string result = str;
  size_t pos = 0;
  while ((pos = result.find(from, pos)) != std::string::npos) {
    result.replace(pos, from.size(), to);
    pos += to.size();
  }
  return result;
}

std::string FSConnector::key_to_filename(const std::string& key) {
  // Input key format (from _object_key_to_string):
  //   Unsalted: <model_name>@<kv_rank_hex>@<chunk_hash_hex>
  //   Salted  : <model_name>@<kv_rank_hex>@<chunk_hash_hex>@<cache_salt>
  //
  // Output filename (matching fs_l2_adapter.py._object_key_to_filename):
  //   Unsalted: <model_name_safe>@0x<kv_rank_hex>@<chunk_hash_hex>.data
  //   Salted  :
  //   <model_name_safe>@0x<kv_rank_hex>@<chunk_hash_hex>@<cache_salt>.data
  //
  // The unsalted 3-field shape is bit-identical to the pre-cache_salt
  // format, so existing cache directories remain valid.
  //
  // NOTE: both model_name and cache_salt are forbidden from containing
  // '@' (invariant enforced on the Python side), so splitting on '@'
  // is unambiguous — no marker, no rsplit.

  // Split on '@' — must yield 3 (unsalted) or 4 (salted) fields.
  std::vector<std::string> parts;
  size_t start = 0;
  for (size_t pos = 0; pos <= key.size(); ++pos) {
    if (pos == key.size() || key[pos] == KEY_SEP) {
      parts.emplace_back(key.substr(start, pos - start));
      start = pos + 1;
    }
  }
  if (parts.size() != 3 && parts.size() != 4) {
    throw std::runtime_error(
        "FSConnector: malformed key (expected 3 or 4 '@'-separated fields): " +
        key);
  }

  const std::string& model_name = parts[0];
  const std::string& kv_rank_hex = parts[1];
  const std::string& chunk_hash = parts[2];
  const std::string cache_salt = parts.size() == 4 ? parts[3] : std::string();

  // Replace '/' with '-SEP-' for filesystem safety
  std::string safe_model = replace_all(model_name, "/", PATH_SLASH_REPLACEMENT);

  // Emit filename. Salt is appended at the tail so the unsalted shape
  // matches what older builds wrote to disk.
  std::string result;
  result.reserve(safe_model.size() + kv_rank_hex.size() + chunk_hash.size() +
                 cache_salt.size() + 32);
  result += safe_model;
  result += KEY_SEP;
  result += "0x";
  result += kv_rank_hex;
  result += KEY_SEP;
  result += chunk_hash;
  if (!cache_salt.empty()) {
    result += KEY_SEP;
    result += cache_salt;
  }
  result += FILE_EXT;
  return result;
}

// ---------------------------------------------------------------
// read/write helpers
// ---------------------------------------------------------------

static void write_all(int fd, const void* data, size_t len) {
  size_t written = 0;
  const char* ptr = static_cast<const char*>(data);
  while (written < len) {
    ssize_t n = ::write(fd, ptr + written, len - written);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error("write failed: " + std::string(strerror(errno)));
    }
    if (n == 0) {
      throw std::runtime_error("write returned 0");
    }
    written += static_cast<size_t>(n);
  }
}

static size_t read_all(int fd, void* buf, size_t len) {
  size_t total = 0;
  char* ptr = static_cast<char*>(buf);
  while (total < len) {
    ssize_t n = ::read(fd, ptr + total, len - total);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error("read failed: " + std::string(strerror(errno)));
    }
    if (n == 0) break;  // EOF
    total += static_cast<size_t>(n);
  }
  return total;
}

// ---------------------------------------------------------------
// FSConnector
// ---------------------------------------------------------------

FSConnector::FSConnector(std::string base_path, int num_workers,
                         std::string relative_tmp_dir, bool use_odirect,
                         size_t read_ahead_size)
    : ConnectorBase(num_workers),
      base_path_(std::move(base_path)),
      relative_tmp_dir_(std::move(relative_tmp_dir)),
      use_odirect_(use_odirect),
      disk_block_size_(0),
      read_ahead_size_(read_ahead_size) {
  // Create base directory
  std::filesystem::create_directories(base_path_);

  // Create tmp directory if configured
  if (!relative_tmp_dir_.empty()) {
    auto tmp_path = std::filesystem::path(base_path_) / relative_tmp_dir_;
    std::filesystem::create_directories(tmp_path);
  }

  // Query disk block size for O_DIRECT
  if (use_odirect_) {
    struct statvfs st;
    if (statvfs(base_path_.c_str(), &st) == 0) {
      disk_block_size_ = st.f_bsize;
    }
  }

  start_workers();  // IMPORTANT: call at END of constructor
}

FSConnector::~FSConnector() { close(); }

std::vector<IOSample> FSConnector::drain_io_samples() {
  std::vector<IOSample> samples;
  std::lock_guard<std::mutex> lock(io_samples_mu_);
  samples.reserve(io_samples_.size());
  while (!io_samples_.empty()) {
    samples.push_back(io_samples_.front());
    io_samples_.pop_front();
  }
  return samples;
}

std::pair<uint64_t, uint64_t> FSConnector::get_io_inflight_counts() const {
  std::lock_guard<std::mutex> lock(io_concurrency_mu_);
  return {inflight_reads_, inflight_writes_};
}

std::pair<uint64_t, uint64_t> FSConnector::drain_io_peak_counts() {
  std::lock_guard<std::mutex> lock(io_concurrency_mu_);
  const uint64_t read_peak = std::max(peak_reads_, inflight_reads_);
  const uint64_t write_peak = std::max(peak_writes_, inflight_writes_);
  peak_reads_ = inflight_reads_;
  peak_writes_ = inflight_writes_;
  return {read_peak, write_peak};
}

void FSConnector::record_io_sample(
    bool is_write, size_t io_size, Clock::time_point submitted_at,
    Clock::time_point started_at, Clock::time_point finished_at, bool ok) {
  IOSample sample;
  sample.is_write = is_write;
  sample.io_size = io_size;
  sample.queue_wait_seconds =
      std::chrono::duration<double>(started_at - submitted_at).count();
  sample.service_seconds =
      std::chrono::duration<double>(finished_at - started_at).count();
  sample.ok = ok;

  std::lock_guard<std::mutex> lock(io_samples_mu_);
  io_samples_.push_back(sample);
}

WorkerFSConn FSConnector::create_connection() {
  WorkerFSConn conn;
  conn.base_path = base_path_;
  if (!relative_tmp_dir_.empty()) {
    conn.tmp_dir = std::filesystem::path(base_path_) / relative_tmp_dir_;
  }
  conn.use_odirect = use_odirect_;
  conn.disk_block_size = disk_block_size_;
  conn.read_ahead_size = read_ahead_size_;
  return conn;
}

void FSConnector::do_batch_get(WorkerFSConn& conn, const Request& req) {
  for (size_t i = 0; i < req.keys.size(); ++i) {
    const auto submitted_at = req.batch->submitted_at;
    const auto started_at = Clock::now();
    bool ok = false;
    try {
      do_single_get(conn, req.keys[i], req.buf_ptrs[i], req.buf_lens[i],
                    req.batch_chunk_num_bytes);
      req.batch->per_key_results[req.start_idx + i] = 1;
      ok = true;
    } catch (const std::exception& e) {
      req.batch->per_key_results[req.start_idx + i] = 0;
      fprintf(stderr, "[LMCache GET] key %s failed: %s\n",
              req.keys[i].c_str(), e.what());
    }
    const auto finished_at = Clock::now();
    req.batch->record_read_io(started_at, finished_at);
    record_io_sample(false, req.buf_lens[i], submitted_at, started_at,
                     finished_at, ok);
  }
}

void FSConnector::do_batch_set(WorkerFSConn& conn, const Request& req) {
  for (size_t i = 0; i < req.keys.size(); ++i) {
    const auto submitted_at = req.batch->submitted_at;
    Clock::time_point started_at{};
    bool did_attempt = false;
    bool ok = false;
    try {
      do_single_set_impl(conn, req.keys[i], req.buf_ptrs[i], req.buf_lens[i],
                         req.batch_chunk_num_bytes, &did_attempt,
                         &started_at);
      ok = did_attempt;
    } catch (...) {
      if (did_attempt) {
        const auto finished_at = Clock::now();
        record_io_sample(true, req.buf_lens[i], submitted_at, started_at,
                         finished_at, false);
      }
      throw;
    }
    if (did_attempt) {
      const auto finished_at = Clock::now();
      record_io_sample(true, req.buf_lens[i], submitted_at, started_at,
                       finished_at, ok);
    }
  }
}

void FSConnector::do_single_get(WorkerFSConn& conn, const std::string& key,
                                void* buf, size_t len, size_t chunk_size) {
  std::string filename = key_to_filename(key);
  auto file_path = conn.base_path / filename;

  int flags = O_RDONLY;
  bool do_odirect = conn.use_odirect;
  if (do_odirect) {
    bool aligned = conn.disk_block_size > 0 && len % conn.disk_block_size == 0;
    if (aligned) {
#ifdef O_DIRECT
      flags |= O_DIRECT;
#endif
    } else {
      do_odirect = false;
    }
  }

  IOConcurrencyGuard guard(inflight_reads_, peak_reads_, io_concurrency_mu_);
  int fd = ::open(file_path.c_str(), flags);
  if (fd < 0) {
    throw std::runtime_error("open for read failed: " + file_path.string() +
                             ": " + strerror(errno));
  }

  try {
    size_t n;
    if (conn.read_ahead_size > 0 && len > conn.read_ahead_size) {
      // Trigger filesystem readahead with a small initial
      // read, then read the remainder.
      size_t ra = conn.read_ahead_size;
      size_t n_head = read_all(fd, buf, ra);
      if (n_head < ra) {
        // Short read on the head portion — treat as
        // incomplete
        n = n_head;
      } else {
        size_t n_tail = read_all(fd, static_cast<char*>(buf) + ra, len - ra);
        n = n_head + n_tail;
      }
    } else {
      n = read_all(fd, buf, len);
    }
    if (n != len) {
      throw std::runtime_error("incomplete read for " + file_path.string() +
                               ": expected " + std::to_string(len) + ", got " +
                               std::to_string(n));
    }
  } catch (...) {
    ::close(fd);
    throw;
  }
  ::close(fd);
}

void FSConnector::do_single_set(WorkerFSConn& conn, const std::string& key,
                                const void* buf, size_t len,
                                size_t chunk_size) {
  do_single_set_impl(conn, key, buf, len, chunk_size, nullptr, nullptr);
}

void FSConnector::do_single_set_impl(WorkerFSConn& conn,
                                     const std::string& key, const void* buf,
                                     size_t len, size_t chunk_size,
                                     bool* did_attempt,
                                     Clock::time_point* started_at) {
  (void)chunk_size;
  if (did_attempt != nullptr) {
    *did_attempt = false;
  }
  std::string filename = key_to_filename(key);
  auto file_path = conn.base_path / filename;

  // Skip if already stored on disk
  if (std::filesystem::exists(file_path)) {
    return;
  }

  if (did_attempt != nullptr) {
    *did_attempt = true;
  }
  if (started_at != nullptr) {
    *started_at = Clock::now();
  }
  IOConcurrencyGuard guard(inflight_writes_, peak_writes_, io_concurrency_mu_);

  // Determine temp file path
  std::filesystem::path tmp_path;
  if (!conn.tmp_dir.empty()) {
    tmp_path = conn.tmp_dir / filename;
  } else {
    tmp_path = file_path;
    tmp_path.replace_extension(TMP_EXT);
  }

  int flags = O_CREAT | O_WRONLY | O_TRUNC;
  bool do_odirect = conn.use_odirect;
  if (do_odirect) {
    bool aligned = conn.disk_block_size > 0 && len % conn.disk_block_size == 0;
    if (aligned) {
#ifdef O_DIRECT
      flags |= O_DIRECT;
#endif
    } else {
      do_odirect = false;
    }
  }

  int fd = ::open(tmp_path.c_str(), flags, 0644);
  if (fd < 0) {
    throw std::runtime_error("open for write failed: " + tmp_path.string() +
                             ": " + strerror(errno));
  }

  try {
    write_all(fd, buf, len);
  } catch (...) {
    ::close(fd);
    // Clean up temp file on failure
    std::filesystem::remove(tmp_path);
    throw;
  }
  ::close(fd);

  // Atomic rename: tmp -> final
  std::error_code ec;
  std::filesystem::rename(tmp_path, file_path, ec);
  if (ec) {
    // Try to clean up, but prioritize reporting the original error.
    std::error_code remove_ec;
    std::filesystem::remove(tmp_path, remove_ec);
    throw std::runtime_error("rename failed: " + tmp_path.string() + " -> " +
                             file_path.string() + ": " + ec.message());
  }
}

bool FSConnector::do_single_exists(WorkerFSConn& conn, const std::string& key) {
  std::string filename = key_to_filename(key);
  auto file_path = conn.base_path / filename;
  return std::filesystem::exists(file_path);
}

bool FSConnector::do_single_delete(WorkerFSConn& conn, const std::string& key) {
  std::string filename = key_to_filename(key);
  auto file_path = conn.base_path / filename;
  std::error_code ec;
  return std::filesystem::remove(file_path, ec);
}

}  // namespace connector
}  // namespace lmcache
