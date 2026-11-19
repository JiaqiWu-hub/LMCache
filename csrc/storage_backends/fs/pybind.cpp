// SPDX-License-Identifier: Apache-2.0
#include <pybind11/pybind11.h>
#include "../connector_pybind_utils.h"
#include "connector.h"

namespace py = pybind11;

PYBIND11_MODULE(lmcache_fs, m) {
  py::class_<lmcache::connector::FSConnector>(m, "LMCacheFSClient")
      .def(py::init<std::string, int, std::string, bool, size_t>(),
           py::arg("base_path"), py::arg("num_workers"),
           py::arg("relative_tmp_dir") = "", py::arg("use_odirect") = false,
           py::arg("read_ahead_size") = 0)
          LMCACHE_BIND_CONNECTOR_METHODS(lmcache::connector::FSConnector)
      .def("drain_io_samples", [](lmcache::connector::FSConnector& self) {
        std::vector<lmcache::connector::IOSample> samples;
        {
          py::gil_scoped_release release;
          samples = self.drain_io_samples();
        }

        py::list result;
        for (const auto& sample : samples) {
          result.append(py::make_tuple(
              sample.is_write, sample.io_size, sample.queue_wait_seconds,
              sample.service_seconds, sample.ok));
        }
        return result;
      });
}
