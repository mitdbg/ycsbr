namespace ycsbr {

inline BenchmarkResult::BenchmarkResult(std::chrono::nanoseconds total_run_time)
    : BenchmarkResult(total_run_time, 0, FrozenMeter(), FrozenMeter(),
                      FrozenMeter(), 0, 0, 0) {}

inline BenchmarkResult::BenchmarkResult(std::chrono::nanoseconds total_run_time,
                                        uint32_t read_xor, FrozenMeter reads,
                                        FrozenMeter writes, FrozenMeter scans,
                                        size_t failed_reads,
                                        size_t failed_writes,
                                        size_t failed_scans)
    : run_time_(total_run_time),
      reads_(reads),
      writes_(writes),
      scans_(scans),
      failed_reads_(failed_reads),
      failed_writes_(failed_writes),
      failed_scans_(failed_scans),
      read_xor_(read_xor) {}

template <typename Units>
inline Units BenchmarkResult::RunTime() const {
  return std::chrono::duration_cast<Units>(run_time_);
}

inline double BenchmarkResult::ThroughputThousandRequestsPerSecond() const {
  const uint64_t total_reqs = reads_.NumRequests() + writes_.NumRequests() +
                              scans_.NumRequests() + failed_reads_ +
                              failed_writes_ + failed_scans_;
  // (requests / millisecond) is equivalent to (krequests / second)
  return total_reqs /
         std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
             run_time_)
             .count();
}

inline double BenchmarkResult::ThroughputThousandRecordsPerSecond() const {
  const uint64_t total_records =
      reads_.NumRecords() + writes_.NumRecords() + scans_.NumRecords();
  // (records / millisecond) is equivalent to (krecords / second)
  return total_records /
         std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
             run_time_)
             .count();
}

inline double BenchmarkResult::ThroughputReadMiBPerSecond() const {
  size_t total_read = reads_.TotalBytes() + scans_.TotalBytes();
  double read_mib = total_read / 1024.0 / 1024.0;
  return read_mib / RunTime<std::chrono::duration<double>>().count();
}

inline double BenchmarkResult::ThroughputWriteMiBPerSecond() const {
  double write_mib = writes_.TotalBytes() / 1024.0 / 1024.0;
  return write_mib / RunTime<std::chrono::duration<double>>().count();
}

inline std::ostream& operator<<(std::ostream& out, const BenchmarkResult& res) {
  out << "Total run time (us):       "
      << res.RunTime<std::chrono::microseconds>().count() << std::endl;
  out << "Total read requests:       " << res.Reads().NumRequests()
      << std::endl;
  out << "Total write requests:      " << res.Writes().NumRequests()
      << std::endl;
  out << "Total scan requests:       " << res.Scans().NumRequests()
      << std::endl;
  out << "Total scanned records:     " << res.Scans().NumRecords() << std::endl;
  out << "Throughput (krequests/s):  "
      << res.ThroughputThousandRequestsPerSecond() << std::endl;
  out << "Throughput (krecords/s):   "
      << res.ThroughputThousandRecordsPerSecond() << std::endl;
  out << "Read Throughput (MiB/s):   " << res.ThroughputReadMiBPerSecond()
      << std::endl;
  out << "Write Throughput (MiB/s):  " << res.ThroughputWriteMiBPerSecond()
      << std::endl;
  out << "Read XOR (ignore):         " << res.read_xor_;
  return out;
}

inline void BenchmarkResult::PrintCSVHeader(std::ostream& out) {
  out << "num_reads,num_writes,num_scans,num_scanned_keys,reads_ns_p99,"
         "reads_ns_p50,writes_ns_p99,writes_ns_p50,krequests_per_s,"
         "krecords_per_s,read_mib_per_s,write_mib_per_s"
      << std::endl;
}

inline void BenchmarkResult::PrintAsCSV(std::ostream& out,
                                        bool print_header) const {
  using nanoseconds = std::chrono::nanoseconds;
  if (print_header) {
    PrintCSVHeader(out);
  }
  out << Reads().NumRequests() << ",";
  out << Writes().NumRequests() << ",";
  out << Scans().NumRequests() << ",";
  out << Scans().NumRecords() << ",";
  out << Reads().LatencyPercentile<nanoseconds>(0.99).count() << ",";
  out << Reads().LatencyPercentile<nanoseconds>(0.5).count() << ",";
  out << Writes().LatencyPercentile<nanoseconds>(0.99).count() << ",";
  out << Writes().LatencyPercentile<nanoseconds>(0.5).count() << ",";
  out << ThroughputThousandRequestsPerSecond() << ",";
  out << ThroughputThousandRecordsPerSecond() << ",";
  out << ThroughputReadMiBPerSecond() << ",";
  out << ThroughputWriteMiBPerSecond() << std::endl;
}

}  // namespace ycsbr
