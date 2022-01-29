#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <chrono>
#include <memory>

#include "db_interface.h"
#include "delegating_db.h"
#include "python_db.h"
#include "ycsbr/gen.h"

namespace py = pybind11;

using Session = ycsbr::Session<ycsbr::py::DelegatingDB>;

PYBIND11_MODULE(ycsbr_py, m) {
  m.doc() = "YCSBR Python bindings";

  // DatabaseInterface binding
  py::class_<ycsbr::py::DatabaseInterface, ycsbr::py::PythonDatabase,
             std::shared_ptr<ycsbr::py::DatabaseInterface>>(m,
                                                            "DatabaseInterface")
      .def(py::init<>())
      .def("initialize_database",
           &ycsbr::py::DatabaseInterface::InitializeDatabase)
      .def("shutdown_database", &ycsbr::py::DatabaseInterface::ShutdownDatabase)
      .def("insert", &ycsbr::py::DatabaseInterface::Insert, py::arg("key"),
           py::arg("value"))
      .def("update", &ycsbr::py::DatabaseInterface::Update, py::arg("key"),
           py::arg("value"))
      .def("read", &ycsbr::py::DatabaseInterface::Read, py::arg("key"))
      .def("scan", &ycsbr::py::DatabaseInterface::Scan, py::arg("key"),
           py::arg("amount"));

  // BulkLoadTrace binding
  // NOTE: We currently do not expose the request objects in the trace.
  py::class_<ycsbr::BulkLoadTrace>(m, "BulkLoadTrace")
      // Load a new BulkLoadTrace from a serialized file.
      .def(py::init([](const std::string& path, bool sort_requests,
                       size_t value_size, int rng_seed) {
             ycsbr::Trace::Options options;
             options.sort_requests = sort_requests;
             options.value_size = value_size;
             options.rng_seed = rng_seed;
             auto trace = ycsbr::BulkLoadTrace::LoadFromFile(path, options);
             return std::make_unique<ycsbr::BulkLoadTrace>(std::move(trace));
           }),
           py::arg("path"), py::arg("sort_requests") = false,
           py::arg("value_size") = 1024, py::arg("rng_seed") = 42)
      // Create a new BulkLoadTrace from a list of integers.
      .def(py::init([](const std::vector<ycsbr::Request::Key>& keys,
                       bool sort_requests, size_t value_size, int rng_seed) {
             ycsbr::Trace::Options options;
             options.sort_requests = sort_requests;
             options.value_size = value_size;
             options.rng_seed = rng_seed;
             auto trace = ycsbr::BulkLoadTrace::LoadFromKeys(keys, options);
             return std::make_unique<ycsbr::BulkLoadTrace>(std::move(trace));
           }),
           py::arg("keys"), py::arg("sort_requests") = false,
           py::arg("value_size") = 1024, py::arg("rng_seed") = 42)
      .def_property_readonly("size_bytes",
                             &ycsbr::BulkLoadTrace::DatasetSizeBytes)
      .def_property_readonly("key_range",
                             [](const ycsbr::BulkLoadTrace& trace) {
                               const auto range = trace.GetKeyRange();
                               return py::make_tuple(range.min, range.max);
                             })
      .def("__len__",
           [](const ycsbr::BulkLoadTrace& trace) { return trace.size(); })
      .def(
          "get_key_at",
          [](const ycsbr::BulkLoadTrace& trace, const size_t index) {
            return trace.at(index).key;
          },
          py::arg("index"));

  // PhasedWorkload binding
  py::class_<ycsbr::gen::PhasedWorkload>(m, "PhasedWorkload")
      .def_static(
          "from_file",
          [](const std::string& path, const uint32_t prng_seed,
             const size_t set_record_size_bytes) {
            return ycsbr::gen::PhasedWorkload::LoadFrom(path, prng_seed,
                                                        set_record_size_bytes);
          },
          py::arg("path"), py::arg("prng_seed") = 42,
          py::arg("set_record_size_bytes") = 0)
      .def_static(
          "from_string",
          [](const std::string& string, const uint32_t prng_seed,
             const size_t set_record_size_bytes) {
            return ycsbr::gen::PhasedWorkload::LoadFromString(
                string, prng_seed, set_record_size_bytes);
          },
          py::arg("path"), py::arg("prng_seed") = 42,
          py::arg("set_record_size_bytes") = 0)
      .def(
          "set_custom_load_dataset",
          [](ycsbr::gen::PhasedWorkload& workload,
             std::vector<ycsbr::Request::Key> dataset) {
            workload.SetCustomLoadDataset(std::move(dataset));
          },
          py::arg("dataset"))
      .def("get_load_trace",
           [](const ycsbr::gen::PhasedWorkload& workload) {
             return workload.GetLoadTrace();
           })
      .def_property_readonly("record_size_bytes",
                             &ycsbr::gen::PhasedWorkload::GetRecordSizeBytes);

  // BenchmarkResult partial bindings
  py::class_<ycsbr::BenchmarkResult>(m, "BenchmarkResult")
      .def_property_readonly(
          "run_time_ns",
          [](const ycsbr::BenchmarkResult& result) {
            return result.RunTime<std::chrono::nanoseconds>().count();
          })
      .def_property_readonly(
          "throughput_krequests_per_s",
          [](const ycsbr::BenchmarkResult& result) {
            return result.ThroughputThousandRequestsPerSecond();
          })
      .def_property_readonly(
          "throughput_krecords_per_s",
          [](const ycsbr::BenchmarkResult& result) {
            return result.ThroughputThousandRecordsPerSecond();
          })
      .def_property_readonly("num_reads",
                             [](const ycsbr::BenchmarkResult& result) {
                               return result.Reads().NumRequests();
                             })
      .def_property_readonly("num_writes",
                             [](const ycsbr::BenchmarkResult& result) {
                               return result.Writes().NumRequests();
                             })
      .def_property_readonly("num_scans",
                             [](const ycsbr::BenchmarkResult& result) {
                               return result.Scans().NumRequests();
                             })
      .def_property_readonly("num_keys_scanned",
                             [](const ycsbr::BenchmarkResult& result) {
                               return result.Scans().NumRecords();
                             })
      .def_property_readonly("num_failed_reads",
                             [](const ycsbr::BenchmarkResult& result) {
                               return result.NumFailedReads();
                             })
      .def_property_readonly("num_failed_writes",
                             [](const ycsbr::BenchmarkResult& result) {
                               return result.NumFailedWrites();
                             });

  // yscbr::Session binding for a delegating database
  py::class_<Session>(m, "Session")
      .def(py::init<size_t>(), py::arg("num_threads"))
      .def(
          "set_database",
          [](Session& session,
             std::shared_ptr<ycsbr::py::DatabaseInterface> db) {
            session.db().SetDatabase(db);
          },
          py::arg("db"))
      .def("initialize",
           [](Session& session) {
             // Releasing the GIL is important in this class binding because the
             // initialization runs on a different thread. That other thread
             // will need to acquire the GIL to call the user's `initialize()`
             // code.
             py::gil_scoped_release release;
             session.Initialize();
           })
      .def("terminate",
           [](Session& session) {
             py::gil_scoped_release release;
             session.Terminate();
           })
      .def(
          "replay_bulk_load_trace",
          [](Session& session, const ycsbr::BulkLoadTrace& trace) {
            py::gil_scoped_release release;
            return session.ReplayBulkLoadTrace(trace);
          },
          py::arg("bulk_load_trace"))
      .def(
          "run_phased_workload",
          [](Session& session, const ycsbr::gen::PhasedWorkload& workload) {
            py::gil_scoped_release release;
            return session.RunWorkload<ycsbr::gen::PhasedWorkload>(workload);
          },
          py::arg("workload"));
}
