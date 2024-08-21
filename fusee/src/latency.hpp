#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <numeric>
#include <vector>

#include <fmt/chrono.h>
#include <fmt/core.h>

class LatencyProfiler {
 public:
  using Nano = std::chrono::nanoseconds;
  using Micro = std::chrono::microseconds;
  using Milli = std::chrono::milliseconds;
  using Second = std::chrono::seconds;

  struct MeasurementGroup {
    Nano start;
    Nano end;
    Nano granularity;
    size_t indices;
    size_t start_idx;

    MeasurementGroup(Nano start, Nano end, Nano granularity)
        : start{start}, end{end}, granularity{granularity} {
      indices = static_cast<size_t>((end - start) / granularity);

      if (start + indices * granularity != end) {
        throw std::runtime_error("Imperfect granularity!");
      }
    }
  };

  LatencyProfiler() {
    grp.emplace_back(Nano(0), Micro(1000), Nano(5));
    grp.emplace_back(Micro(1), Micro(10), Nano(10));
    grp.emplace_back(Micro(10), Micro(20), Nano(20));
    grp.emplace_back(Micro(20), Micro(50), Nano(100));
    grp.emplace_back(Micro(50), Micro(100), Nano(200));
    grp.emplace_back(Micro(100), Micro(500), Micro(1));
    grp.emplace_back(Micro(500), Milli(1), Micro(10));
    grp.emplace_back(Milli(1), Milli(10), Micro(100));
    grp.emplace_back(Milli(10), Milli(100), Milli(1));
    grp.emplace_back(Milli(100), Second(1), Milli(10));
    grp.emplace_back(Second(1), Second(10), Milli(100));

    // Compute the start_idx for all groups
    size_t start_idx = 0;
    for (auto &g : grp) {
      g.start_idx = start_idx;
      start_idx += g.indices;
    }

    // Set the vector size to fit the buckets of all groups
    freq.resize(start_idx);
  }

  template <typename Duration>
  void addMeasurement(Duration const &duration) {
    auto d = std::chrono::duration_cast<Nano>(duration);
    measurement_idx++;
    total_time += d;
    // auto count = d.count();

    if (d < std::chrono::nanoseconds(0)) {
      fmt::print("!PROFILER WARNING! Duration underflow: {}\n", d);
      return;
    }

    if (duration >= grp.back().end) {
      // fmt::print("!PROFILER WARN! {} > max {}.\n", duration, grp.back().end);
      return;
    }

    // Find the right group
    auto it = std::lower_bound(grp.begin(), grp.end(), d,
                               [](MeasurementGroup const &g, Nano duration) {
                                 return g.start <= duration;
                               });

    auto group_index = static_cast<size_t>(std::distance(grp.begin(), it - 1));

    // Find the index inside the group
    auto &group = grp.at(group_index);
    auto freq_index = group.start_idx + static_cast<size_t>((d - group.start) /
                                                            group.granularity);

    freq.at(freq_index)++;
  }

  Nano percentile(double const perc) const {
    auto acc_freq(freq);
    auto measurents_cnt =
        std::accumulate(acc_freq.begin(), acc_freq.end(), 0UL);

    std::partial_sum(acc_freq.begin(), acc_freq.end(), acc_freq.begin());

    auto it_freq =
        std::lower_bound(acc_freq.begin(), acc_freq.end(),
                         static_cast<double>(measurents_cnt) * perc / 100.0);

    auto freq_idx =
        static_cast<size_t>(std::distance(acc_freq.begin(), it_freq));

    // Find the right group
    auto it =
        std::lower_bound(grp.begin(), grp.end(), freq_idx,
                         [](MeasurementGroup const &g, uint64_t freq_idx) {
                           return g.start_idx <= freq_idx;
                         });

    auto group_index = static_cast<size_t>(std::distance(grp.begin(), it - 1));

    // Find the index inside the group
    auto &group = grp.at(group_index);
    auto time = group.start + (freq_idx - group.start_idx) * group.granularity;

    return time + group.granularity;
  }

  template <typename Duration>
  static std::string prettyTime(Duration const &d) {
    if (d < Nano(1000)) {
      Nano dd = std::chrono::duration_cast<Nano>(d);
      return std::to_string(dd.count()) + "ns";
    }

    if (d < Micro(1000)) {
      Micro dd = std::chrono::duration_cast<Micro>(d);
      return std::to_string(dd.count()) + "us";
    }

    /*if (d < Milli(1000))*/ {
      Milli dd = std::chrono::duration_cast<Milli>(d);
      return std::to_string(dd.count()) + "ms";
    }
  }

  void report(bool detailed = false) const {
    fmt::print("Number of measurements: {}.\n", measurement_idx);
    fmt::print("Average latency: {}.\n", measurement_idx > 0
                                             ? total_time / measurement_idx
                                             : total_time / 1ul);
    if (detailed) {
      fmt::print("{}%: {}.\n", 0.1, percentile(0.1));
      for (auto ptile = 1; ptile < 100; ptile++) {
        fmt::print("{}%: {}.\n", ptile, percentile(ptile));
      }
    } else {
      fmt::print("{}%: {},  ", 0.1, percentile(0.1));
      for (auto ptile : {5, 25, 50, 75, 95}) {
        fmt::print("{}%: {},  ", ptile, percentile(ptile));
      }
    }
    fmt::print("{}%: {}.\n", 99.9, percentile(99.9));
  }

  void reportOnce(bool detailed = false) {
    if (!reported) {
      report(detailed);
      reported = true;
    }
  }

  void reportBuckets() const {
    for (auto &g : grp) {
      fmt::print("Reporting detailed data for range (in ns) [{},{})\n",
                 prettyTime(g.start), prettyTime(g.end));

      for (size_t i = 0; i < g.indices; i++) {
        auto f = freq.at(g.start_idx + i);
        if (f == 0) {
          continue;
        }

        fmt::print("[{},{}) {}\n", g.start + i * g.granularity,
                   g.start + (i + 1) * g.granularity, f);
      }
      fmt::print("\n");
    }
  }

  size_t getMeasurementCount() const { return measurement_idx; }

 private:
  size_t measurement_idx = 0;
  bool reported = false;
  std::vector<MeasurementGroup> grp;
  std::vector<uint64_t> freq;
  Nano total_time;
};