#include <memory>

#include <lyra/lyra.hpp>

#include "race/client_index.hpp"
#include "race/server_index.hpp"

#include "latency.hpp"
#include "layout.hpp"
#include "main.hpp"
#include "oops_client.hpp"

#include <dory/shared/match.hpp>

using namespace dory;
using namespace dory::conn;
using namespace conn;

const uint64_t default_warmup = 1'000'000;
const uint64_t default_iter_count = 1'000'000;
const uint64_t default_keepwarm = 500'000;

int main(int argc, char* argv[]) {
  ProcId proc_id = 0;

  Layout layout;
  layout.doorbell = false;
  layout.in_place = true;
  layout.num_clients = 1;
  layout.num_servers = 1;
  layout.majority = 0;
  layout.guess_ts = true;
  layout.async_parallelism = 1;
  layout.keys_per_server = 100000;
  layout.server_logs_per_client = 0;
  layout.key_size = 24;
  layout.value_size = 64;
  layout.num_tsp = 1;
  uint64_t death_point = uint64_t(-1);
  bool measure_batches = false;

  uint64_t iter_count = default_iter_count;
  uint64_t warmup = UINT64_MAX;

  layout.bucket_bits = 18;
  size_t pointer_cache_size = UINT64_MAX;
  bool detailed = true;

  std::string ycsb_path = "./YCSB/bin/ycsb.sh";
  std::string workload = "./YCSB/workloads/swarm-workloada";

  auto cli =
      lyra::cli() |
      lyra::opt(proc_id, "proc_id")
          .required()["-i"]["-p"]["--id"]["--process"]
          .help("ID of this process.") |
      lyra::opt(layout.num_clients, "num_clients")
          .optional()["-c"]["--clients"] |
      lyra::opt(layout.num_servers, "num_servers")
          .optional()["-s"]["--servers"] |
      lyra::opt(layout.majority, "majority").optional()["-m"]["--majority"] |
      lyra::opt(layout.async_parallelism, "async_parallelism")
          .optional()["-a"]["--async"] |
      lyra::opt(layout.keys_per_server, "num_keys")
          .optional()["-n"]["--numkeys"] |
      lyra::opt(layout.server_logs_per_client, "server_logs_per_client")
          .optional()["-l"]["--logs"] |
      lyra::opt(layout.key_size, "key_size").optional()["-k"]["--keysize"] |
      lyra::opt(layout.value_size, "value_size").optional()["-v"]["--valuesize"] |
      //  | lyra::opt(layout.logs_per_client, "logs_per_client")
      //        .optional()["-l"]["--logsperclient"]
      lyra::opt(layout.bucket_bits, "bucket_bits")
            .optional()["-b"]["--bucketbits"] |
      lyra::opt(pointer_cache_size, "pointer_cache_size")
          .optional()["-t"]["--pointercachesize"] |
      lyra::opt(workload, "workload").optional()["-w"]["--workload"] |
      lyra::opt(ycsb_path, "ycsb_path").optional()["-y"]["--ycsbpath"] |
      lyra::opt(detailed, "detailed").optional()["-d"]["--detailed"] |
      lyra::opt(layout.guess_ts, "guess_ts").optional()["-g"]["--guess"] |
      lyra::opt(layout.doorbell, "use_doorbell")
          .optional()["-o"]["--doorbell"] |
      lyra::opt(layout.in_place, "in_place").optional()["-e"]["--in_place"] |
      lyra::opt(layout.num_tsp, "num_tsp").optional()["-T"]["--num_tsp"] |
      lyra::opt(death_point, "death_point").optional()["-D"]["--death_point"] |
      lyra::opt(measure_batches, "measure_batches")
          .optional()["-B"]["--measure_batches"] |
      lyra::opt(iter_count, "iter_count").optional()["-I"]["--iter_count"] |
      lyra::opt(warmup, "warmup").optional()["-W"]["--warmup"];

  auto result = cli.parse({argc, argv});
  if (!result) {
    std::cerr << "Error in command line: " << result.errorMessage()
              << std::endl;
    return 1;
  }
  if(warmup == UINT64_MAX) {
    warmup = iter_count < default_warmup ? iter_count : default_warmup;
  }
  const uint64_t keepwarm = (iter_count + warmup) / 4;

  const uint64_t start_measurements = warmup;
  const uint64_t stop_measurements = start_measurements + iter_count;
  const uint64_t total_iter_count = stop_measurements + keepwarm;

  if (layout.majority == 0) {
    layout.majority = layout.num_servers;
  }
  if (layout.num_tsp == 0) {
    layout.num_tsp = layout.num_clients;
  }
  if (layout.server_logs_per_client == 0) {
    layout.server_logs_per_client = layout.keys_per_server + 10'000 + total_iter_count * 11 / 20;
  }

  if(pointer_cache_size != UINT64_MAX) {
    pointer_cache_size =
        (pointer_cache_size * 1024) / (layout.guess_ts ? 32 : 24);
  }

  auto num_proc = layout.num_clients + layout.num_servers;

  if (proc_id > num_proc) {
    std::cerr << "Invalid process id error: " << proc_id << " is bigger than "
              << num_proc << " (number of processes)" << std::endl;
    return 1;
  }
  bool is_client = proc_id > layout.num_servers;
  ProcId first_client = layout.num_servers + 1;
  if (is_client) {
    std::cout << "Workload: " << workload << std::endl;
    std::cout << "Log entry size: " << layout.fullLogEntrySize() << std::endl;
    std::cout << "KV entry size: " << layout.fullKVSize() << std::endl;
    // pin_main_to_core(0);
  }

  std::vector<ProcId> remote_ids;
  for (ProcId id = 1; id <= num_proc; id++) {
    if (id == proc_id) {
      continue;
    }
    // if (is_client == (id > num_servers)) {
    //   continue; // Only connect to clients if we are a server and vice versa
    // }
    remote_ids.push_back(id);
  }

  using namespace units;

  ctrl::Devices d;
  ctrl::OpenDevice od;

  // Get the last device
  od = std::move(d.list().back());

  std::cout << od.name() << " " << od.devName() << " "
            << ctrl::OpenDevice::typeStr(od.nodeType()) << " "
            << ctrl::OpenDevice::typeStr(od.transportType()) << std::endl;

  ctrl::ResolvedPort resolved_port(od);
  auto binded = resolved_port.bindTo(0);
  if (!binded) {
    throw std::runtime_error("Couldn't bind the device.");
  }
  std::cout << "Binded successfully (port_id, port_lid) = ("
            << +resolved_port.portId() << ", " << +resolved_port.portLid()
            << ")" << std::endl;

  // 2. We configure the control block.
  ctrl::ControlBlock cb(resolved_port);
  cb.registerPd("primary");

  {
    size_t allocated_size =
        is_client ? layout.clientSize() : layout.serverSize();
    size_t alignment = 64;
    cb.allocateBuffer("shared-buf", allocated_size, alignment);
  }

  cb.registerMr(
      "shared-mr", "primary", "shared-buf",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
          ctrl::ControlBlock::REMOTE_ATOMIC);

  for (auto const& id : remote_ids) {
    auto cq = fmt::format("cq{}", id);
    cb.registerCq(cq);
  }

  auto local_region = cb.mr("shared-mr").addr;
  if (is_client) {
    layout.client_local_region = local_region;
  }

  // 3. We establish reliable connections.
  auto& store = memstore::MemoryStore::getInstance();

  RcConnectionExchanger<ProcId> ce(proc_id, remote_ids, cb);
  for (auto const& id : remote_ids) {
    auto cq = fmt::format("cq{}", id);
    ce.configure(id, "primary", "shared-mr", cq, cq);
  }
  ce.announceAll(store, "qp");

  ce.announceReady(store, "qp", "prepared");
  ce.waitReadyAll(store, "qp", "prepared");

  ce.unannounceReady(store, "qp", "finished");  // Clean up from previous runs

  ce.connectAll(
      store, "qp",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
          ctrl::ControlBlock::REMOTE_ATOMIC);

  ce.announceReady(store, "qp", "connected");
  ce.waitReadyAll(store, "qp", "connected");

  // Clean up
  ce.unannounceAll(store, "qp");
  ce.unannounceReady(store, "qp", "prepared");

  if (is_client) {
    OopsClient client{
        layout,          ce,          proc_id,   pointer_cache_size,
        measure_batches, death_point, iter_count};

    if (proc_id == layout.firstClientId()) {
      std::cout << "Querying YCSB for the set of initial key-pairs... "
                << std::flush;
      std::vector<std::pair<std::string, std::string>> inserts = {};

      {
        auto output =
            exec(ycsb_path + " load basic -P " + workload + " -s 2> /dev/null");
        std::string line;
        while (std::getline(output, line)) {
          if (std::strncmp("INSERT ", line.c_str(),
                           std::string("INSERT ").length())) {
            continue;
          }
          auto keystart = std::string("INSERT usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          auto start = keyend + std::string(" [ field0=").length();
          auto end = line.length() - std::string(" ]").length();
          auto value = line.substr(start, end - start);

          inserts.emplace_back(key, value);
        }
      }
      std::cout << "Done." << std::endl;

      // Initialize the index
      std::cout << "Inserting the initial key-pairs... " << std::flush;
      for (size_t kvIndex = 0; kvIndex < inserts.size(); kvIndex++) {
        auto& insert = inserts[kvIndex];
        auto& key = insert.first;
        auto& value = insert.second;

        client.finishAllFutures();

        client.getFuture(0).doInsert(key, value);
      }

      // Finish inserts
      client.finishAllFutures();

      std::cout << " Done." << std::endl;
    }

    std::cout << "Querying YCSB for the list of operations... " << std::flush;
    std::vector<std::pair<std::string, std::optional<std::string>>> operations =
        {};

    {
      auto output =
          exec(ycsb_path + " run basic -P " + workload + " -s 2> /dev/null");
      std::string line;
      while (std::getline(output, line)) {
        if (!(std::strncmp("READ ", line.c_str(),
                           std::string("READ ").length()))) {
          auto keystart = std::string("READ usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          operations.emplace_back(key, std::optional<std::string>());
        } else if (!(std::strncmp("UPDATE ", line.c_str(),
                                  std::string("UPDATE ").length()))) {
          auto keystart = std::string("UPDATE usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          auto start = keyend + std::string(" [ field0=").length();
          auto end = line.length() - std::string(" ]").length();
          auto value = line.substr(start, end - start);

          operations.emplace_back(key, std::optional<std::string>(value));

        } else {
          continue;
        }
      }
    }

    std::cout << "Done." << std::endl;
    std::cout << "Waiting for the initialization of other clients... "
              << std::flush;

    ce.announceReady(store, "qp", "initialized");
    ce.waitReadyAll(store, "qp", "initialized");

    std::cout << "Done." << std::endl;
    std::cout << "Running the benchmark... " << std::endl;

    client.initClock();

    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point end;
    bool measuring = false;
    size_t skipped = 0;

    for (size_t i = 0; i < total_iter_count; i++) {

      retry_next_key:
      auto& [key, value] = operations[(i + skipped) % operations.size()];

      if (layout.async_parallelism > 1) {
        auto hkey = hash(key);
        for (size_t i = 0; i < layout.async_parallelism; i++) {
          auto& future = client.getFuture(i);
          if (future.hkey == hkey && !(future.isDone())) {
            ++skipped;
            goto retry_next_key;
          }
        }
      }

      auto& future = client.getFreeFuture();

      if (i == start_measurements) {
        measuring = true;
        start = std::chrono::steady_clock::now();
        client.startMeasurements(start);
      } else if (i == stop_measurements) {
        measuring = false;
        end = std::chrono::steady_clock::now();
      }

      if (value.has_value()) {
        future.doUpdate(key, value.value(), measuring);
      } else {
        future.doRead(key, measuring);
      }
    }

    client.finishAllFutures();

    std::cout << "Done. Results:" << std::endl;

    client.reportStats(detailed);
    fmt::print(
        "Local tput: {}kpos\n",
        iter_count * 1'000'000 / static_cast<uint64_t>((end - start).count()));
    std::cout << std::flush;

    ce.announceReady(store, "qp", "finished");
    ce.waitReadyAll(store, "qp", "finished");
    ce.unannounceReady(store, "qp", "initialized");
  } else {
    std::vector<ProcId> client_ids;
    for (ProcId id = layout.firstClientId();
         id < layout.firstClientId() + layout.num_clients; id++) {
      client_ids.push_back(id);
    }
    dory::race::ServerIndex server(proc_id, client_ids, layout.bucket_bits);
    ce.announceReady(store, "qp", "initialized");
    ce.announceReady(store, "qp", "finished");
    ce.waitReadyAll(store, "qp", "finished");
    ce.unannounceReady(store, "qp", "initialized");
    std::cout << "Closing server index connection." << std::endl;
  }

  // 9. Clean up.
  ce.unannounceReady(store, "qp", "connected");

  std::cout << "###DONE###" << std::endl;
  return 0;
}
