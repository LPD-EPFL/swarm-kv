#include <dory/memstore/store.hpp>
#include <dory/shared/bench.hpp>
#include <dory/shared/logger.hpp>
#include <dory/shared/pointer-wrapper.hpp>

#include <atomic>
#include <fstream>
#include <iostream>
#include <numeric>
#include <thread>

#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <random>

#include <dory/crypto/asymmetric/dalek.hpp>
#include <dory/crypto/asymmetric/sodium.hpp>

#include <lyra/lyra.hpp>

auto logger = dory::std_out_logger("MAIN");

inline void pin_thread_to_core(std::thread &thd, size_t cpu_id) {
  cpu_set_t cpuset;

  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);

  int rc =
      pthread_setaffinity_np(thd.native_handle(), sizeof(cpu_set_t), &cpuset);

  if (rc != 0) {
    throw std::runtime_error("Error calling pthread_setaffinity_np: " +
                             std::string(std::strerror(errno)));
  }
}

inline void stats_latency(size_t tid, std::vector<long long> &numbers) {
  std::vector<long long> filtered;

  const int upper_bound = 200000;

  std::cout
      << "\n================================================================"
      << "\nTHREAD NUMBER: " << tid
      << "\n================================================================"
      << std::endl;
  std::cout << "Keep only values lower than " << upper_bound << "ns"
            << std::endl;
  std::copy_if(numbers.begin(), numbers.end(), std::back_inserter(filtered),
               [&](int i) { return i < upper_bound; });

  if (filtered.size() == 0) {
    std::cout << "All samples are above the upper bound of " << upper_bound
              << std::endl;
    return;
  }

  sort(filtered.begin(), filtered.end());

  long long sum = std::accumulate(filtered.begin(), filtered.end(), 0ll);
  double mean = double(sum) / static_cast<double>(filtered.size());

  auto max_idx = filtered.size() - 1;
  auto [min_elem, max_elem] =
      std::minmax_element(filtered.begin(), filtered.end());

  std::cout << "Samples #: " << filtered.size() << std::endl;
  std::cout << "Skipped: " << numbers.size() - filtered.size() << std::endl;
  std::cout << "(Min, Max): " << *min_elem << ", " << *max_elem << std::endl;
  std::cout << "Average: " << mean << "ns" << std::endl;
  std::cout << "25th %-tile: "
            << filtered[static_cast<size_t>(
                   std::roundl(static_cast<long double>(max_idx) * 0.25))]
            << "ns" << std::endl;
  std::cout << "50th %-tile: "
            << filtered[static_cast<size_t>(
                   std::roundl(static_cast<long double>(max_idx) * 0.50))]
            << "ns" << std::endl;
  std::cout << "75th %-tile: "
            << filtered[static_cast<size_t>(
                   std::roundl(static_cast<long double>(max_idx) * 0.75))]
            << "ns" << std::endl;
  std::cout << "90th %-tile: "
            << filtered[static_cast<size_t>(
                   std::roundl(static_cast<long double>(max_idx) * 0.90))]
            << "ns" << std::endl;
  std::cout << "95th %-tile: "
            << filtered[static_cast<size_t>(
                   std::roundl(static_cast<long double>(max_idx) * 0.95))]
            << "ns" << std::endl;
  std::cout << "98th %-tile: "
            << filtered[static_cast<size_t>(
                   std::roundl(static_cast<long double>(max_idx) * 0.98))]
            << "ns" << std::endl;
  std::cout << "99th %-tile: "
            << filtered[static_cast<size_t>(
                   std::roundl(static_cast<long double>(max_idx) * 0.99))]
            << "ns" << std::endl;
}

std::vector<unsigned char> generate_message(int len);
std::vector<unsigned char> generate_message(int len) {
  // Create a random message
  std::vector<unsigned char> msg_vec(len);

  // First create an instance of an engine.
  std::random_device rnd_device;
  // Specify the engine and distribution.
  std::mt19937 mersenne_engine{rnd_device()};  // Generates random integers
  std::uniform_int_distribution<unsigned char> dist{0, 255};

  auto gen = [&dist, &mersenne_engine]() { return dist(mersenne_engine); };

  generate(std::begin(msg_vec), std::end(msg_vec), gen);

  return msg_vec;
}

enum class Implementation { DALEK, SODIUM };

void setup(Implementation impl);
void setup(Implementation impl) {
  std::cout << "Using the ";
  switch (impl) {
    case Implementation::DALEK:
      std::cout << "Dalek";
      dory::crypto::asymmetric::dalek::init();
      dory::crypto::asymmetric::dalek::publish_pub_key_nostore("p1-pk");
      break;
    case Implementation::SODIUM:
      std::cout << "Sodium";
      dory::crypto::asymmetric::sodium::init();
      dory::crypto::asymmetric::sodium::publish_pub_key_nostore("p1-pk");
      break;
    default:
      break;
  }

  std::cout << " implementation\n";
}

enum class Mode { SIGN, VERIFY };

#define IMPL dory::crypto::asymmetric::dalek
// IMPL::pub_key pk;

std::vector<long long> latency(size_t thread_id, int validations, int len,
                               Mode mode);
std::vector<long long> latency(size_t thread_id, int validations, int len,
                               Mode mode) {
  auto pk = IMPL::get_public_key_nostore("p1-pk");

  auto *raw = reinterpret_cast<unsigned char *>(malloc(IMPL::SignatureLength));

  auto sig = dory::deleted_unique_ptr<unsigned char>(
      raw, [](unsigned char *sig) noexcept { free(sig); });

  auto msg_vec(generate_message(len));
  unsigned char *msg = msg_vec.data();

  IMPL::sign(sig.get(), msg, len);

  std::vector<long long> numbers(validations);

  if (mode == Mode::SIGN) {
    for (int i = 0; i < validations; i++) {
      struct timespec t1, t2;
      double elapsed;

      clock_gettime(CLOCK_MONOTONIC, &t1);
      IMPL::sign(sig.get(), msg, len);
      clock_gettime(CLOCK_MONOTONIC, &t2);

      elapsed = static_cast<double>(t2.tv_nsec + t2.tv_sec * 1000000000UL -
                                    t1.tv_nsec - t1.tv_sec * 1000000000UL);

      numbers[i] = static_cast<long long>(elapsed);
    }
  } else if (mode == Mode::VERIFY) {
    for (int i = 0; i < validations; i++) {
      struct timespec t1, t2;
      double elapsed;

      clock_gettime(CLOCK_MONOTONIC, &t1);
      if (!IMPL::verify(sig.get(), msg, len, pk)) {
        throw std::runtime_error("sig not valid");
      };
      clock_gettime(CLOCK_MONOTONIC, &t2);

      elapsed = static_cast<double>(t2.tv_nsec + t2.tv_sec * 1000000000UL -
                                    t1.tv_nsec - t1.tv_sec * 1000000000UL);

      numbers[i] = static_cast<long long>(elapsed);
    }
  } else {
    throw std::runtime_error("Unknown mode");
  }

  return numbers;
}

double throughput(size_t thread_id, int validations, int len, Mode mode);
double throughput(size_t thread_id, int validations, int len, Mode mode) {
  auto pk = IMPL::get_public_key_nostore("p1-pk");

  auto *raw = reinterpret_cast<unsigned char *>(malloc(IMPL::PublicKeyLength));

  auto sig = dory::deleted_unique_ptr<unsigned char>(
      raw, [](unsigned char *sig) noexcept { free(sig); });

  auto msg_vec(generate_message(len));
  unsigned char *msg = msg_vec.data();

  IMPL::sign(sig.get(), msg, len);

  std::vector<long long> numbers(validations);

  if (mode == Mode::SIGN) {
    struct timespec t1, t2;
    double elapsed;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    for (int i = 0; i < validations; i++) {
      IMPL::sign(sig.get(), msg, len);
    }
    clock_gettime(CLOCK_MONOTONIC, &t2);
    elapsed = static_cast<double>(t2.tv_nsec + t2.tv_sec * 1000000000UL -
                                  t1.tv_nsec - t1.tv_sec * 1000000000UL);

    return elapsed / 1000.0;
  } else if (mode == Mode::VERIFY) {
    struct timespec t1, t2;
    double elapsed;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    for (int i = 0; i < validations; i++) {
      if (!IMPL::verify(sig.get(), msg, len, pk)) {
        throw std::runtime_error("sig not valid");
      };
    }
    clock_gettime(CLOCK_MONOTONIC, &t2);
    elapsed = static_cast<double>(t2.tv_nsec + t2.tv_sec * 1000000000UL -
                                  t1.tv_nsec - t1.tv_sec * 1000000000UL);

    return elapsed / 1000.0;
  } else {
    throw std::runtime_error("Unknown mode");
  }

  return 0;
}

inline void write(size_t tid, std::vector<long long> &numbers) {
  sort(numbers.begin(), numbers.end());

  auto filename =
      std::string("/tmp/verify") + std::to_string(tid) + std::string(".dat");

  std::cout << "Writing samples to" << filename << std::endl;

  std::ofstream fs;
  fs.open(filename);
  for (auto &p : numbers) fs << p << "\n";
}

/**
 * NOTE:  For this to successfully run, you need to have memcached running.
 *        Refer to the memstore package for further information.
 * */
int main(int argc, char *argv[]) {
  int threads;
  int msg_size = 64;
  int validations = 100000;
  std::string mode_raw = "verify";
  std::string impl_raw = "dalek";
  std::string benchmark = "throughput";

  bool show_help = false;

  auto cli =
      lyra::help(show_help) |
      lyra::opt(threads,
                "threads")["-t"]["--threads"]("Core IDs to pin the threads") |
      lyra::opt(msg_size,
                "message_size")["-s"]["--size"]("Length of message in bytes") |
      lyra::opt(validations, "validations")["-v"]["--validations"](
          "Number of iterations of the cryptographic operation") |
      lyra::opt(benchmark, "throughput_or_latency")
          .choices("throughput", "latency")["-b"]["--bench"](
              "Benchmark throughput or latency") |
      lyra::opt(mode_raw, "sign_or_verify")
          .required()
          .name("-m")
          .name("--mode")
          .choices("sign", "verify")("Cryptographic operation") |
      lyra::opt(impl_raw, "dalek_or_sodium")
          .required()
          .name("-i")
          .name("--implementation")
          .choices("dalek", "sodium")("Cryptographic primitives library");

  auto result = cli.parse({argc, argv});

  if (!result) {
    std::cerr << "Error in command line: " << result.errorMessage()
              << std::endl;
    std::cerr << cli << "\n";
    return 1;
  }

  if (show_help) {
    std::cout << cli << "\n";
    return 0;
  }

  std::cout << threads << "\n"
            << msg_size << "\n"
            << validations << "\n"
            << mode_raw << "\n"
            << impl_raw << "\n"
            << benchmark << std::endl;

  Implementation impl;
  if (impl_raw == "dalek") {
    impl = Implementation::DALEK;
  } else {
    impl = Implementation::SODIUM;
  }

  Mode mode;
  if (mode_raw == "verify") {
    mode = Mode::VERIFY;
  } else {
    mode = Mode::SIGN;
  }

  logger->info("Creating and publishing key and verifying own signature");

  setup(impl);

  std::vector<size_t> thread_pins = {0, 1, 2,  3,  4,  5,  6,  7,
                                     8, 9, 10, 11, 12, 13, 14, 15};
  thread_pins.resize(threads);

  std::vector<std::thread> workers(thread_pins.size());
  std::atomic<size_t> done(0);
  std::atomic<size_t> printed(0);

  for (size_t j = 0; j < workers.size(); ++j) {
    workers[j] = std::thread([&, j]() {
      if (benchmark == "throughput") {
        double numbers(throughput(j, validations, msg_size, mode));

        done++;

        // wait for others to finish!
        while (done != workers.size()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // please, wait until it's your turn!
        while (printed != j) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        std::cout << "Thread " << j << ":" << 100000 << " signatures in "
                  << numbers << "us\n";
        printed++;
      } else {
        std::vector<long long> numbers(latency(j, validations, msg_size, mode));

        done++;

        // wait for others to finish!
        while (done != workers.size()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // please, wait until it's your turn!
        while (printed != j) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        stats_latency(j, numbers);
        // write(j, numbers);
        printed++;
      }
    });

    pin_thread_to_core(workers[j], thread_pins[j]);
  }

  for (auto &w : workers) {
    w.join();
  }

  logger->info("Testing finished successfully!");

  return 0;
}
