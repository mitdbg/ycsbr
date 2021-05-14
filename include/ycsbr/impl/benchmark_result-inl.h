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

inline double BenchmarkResult::ThroughputMopsPerSecond() const {
  const uint64_t total_ops = reads_.NumOperations() + writes_.NumOperations() +
                             scans_.NumOperations() + failed_reads_ +
                             failed_writes_ + failed_scans_;
  return total_ops /
         (double)std::chrono::duration_cast<std::chrono::microseconds>(
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
  out << "Total reads:               " << res.Reads().NumOperations()
      << std::endl;
  out << "Total writes:              " << res.Writes().NumOperations()
      << std::endl;
  out << "Total scanned keys:        " << res.Scans().NumOperations()
      << std::endl;
  out << "Throughput (Mops/s):       " << res.ThroughputMopsPerSecond()
      << std::endl;
  out << "Read Throughput (MiB/s):   " << res.ThroughputReadMiBPerSecond()
      << std::endl;
  out << "Write Throughput (MiB/s):  " << res.ThroughputWriteMiBPerSecond()
      << std::endl;
  out << "Read XOR (ignore):         " << res.read_xor_;
  return out;
}

inline void BenchmarkResult::PrintCSVHeader(std::ostream& out) {
  out << "num_reads,num_writes,num_scanned_keys,reads_ns_p99,reads_ns_p50,"
         "writes_ns_p99,writes_ns_p50,mops_per_s,read_mib_per_s,write_mib_per_s"
      << std::endl;
}

inline void BenchmarkResult::PrintAsCSV(std::ostream& out,
                                        bool print_header) const {
  using nanoseconds = std::chrono::nanoseconds;
  if (print_header) {
    PrintCSVHeader(out);
  }
  out << Reads().NumOperations() << ",";
  out << Writes().NumOperations() << ",";
  out << Scans().NumOperations() << ",";
  out << Reads().LatencyPercentile<nanoseconds>(0.99).count() << ",";
  out << Reads().LatencyPercentile<nanoseconds>(0.5).count() << ",";
  out << Writes().LatencyPercentile<nanoseconds>(0.99).count() << ",";
  out << Writes().LatencyPercentile<nanoseconds>(0.5).count() << ",";
  out << ThroughputMopsPerSecond() << ",";
  out << ThroughputReadMiBPerSecond() << ",";
  out << ThroughputWriteMiBPerSecond() << std::endl;
}

}  // namespace ycsbr
