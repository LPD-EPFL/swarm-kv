#include "layout.hpp"
#include "client.hpp"
#include "latency.hpp"
#include <memory>
#include "race/server_index.hpp"
#include "race/client_index.hpp"

#include <dory/shared/match.hpp>
#include "lru-cache.hpp"

using namespace dory;
using namespace dory::conn;
using namespace conn;

const uint64_t default_warmup = 1'000'000;
const uint64_t default_iter_count = 1'000'000;
const uint64_t default_keepwarm = 500'000;

int main(int argc, char* argv[]) {
  Layout layout;
  layout.num_clients = 1;

  layout.num_keys = 100'000;
  layout.key_size = 24;
  layout.value_size = 64;

  size_t bucket_bits = 18;
  uint64_t pointer_cache_size = UINT64_MAX;

  layout.kv_read_slot_count = 128;

  std::string ycsb_path = "./YCSB/bin/ycsb";
  std::string workload = "./YCSB/workloads/swarm-workloada";

  size_t iter_count = default_iter_count;
  uint64_t warmup = UINT64_MAX;

  LatencyProfiler get_stat, update_stat, search_stat;

  auto cli =
      lyra::cli() |
      lyra::opt(layout.num_clients, "num_clients")
          .optional()["-c"]["--clients"] |
      lyra::opt(layout.num_servers, "num_servers")
          .optional()["-s"]["--servers"] |
      lyra::opt(layout.proc_id, "proc_id")
          .required()["-i"]["-p"]["--id"]["--process"]
          .help("ID of this process.") |
      lyra::opt(layout.key_size, "key_size").optional()["-k"]["--keysize"] |
      lyra::opt(layout.value_size, "value_size")
          .optional()["-v"]["--valuesize"] |
      lyra::opt(layout.num_keys, "num_keys").optional()["-n"]["--numkeys"] |
      lyra::opt(layout.kv_read_slot_count, "kv_read_slot_count")
          .optional()["-e"]["--kvcacheentrycount"] |
      lyra::opt(bucket_bits, "bucket_bits").optional()["-b"]["--bucketbits"] |
      lyra::opt(pointer_cache_size, "pointer_cache_size")
          .optional()["-t"]["--pointercachesize"] |
      lyra::opt(workload, "workload").optional()["-w"]["--workload"] |
      lyra::opt(ycsb_path, "ycsb_path").optional()["-y"]["--ycsbpath"] |
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

  layout.max_num_updates = layout.num_keys + 10'000 + total_iter_count * 11 / 20;

  if(pointer_cache_size != UINT64_MAX) {
    pointer_cache_size = (pointer_cache_size * 1024) / 24;
  }

  layout.keys_per_server = (layout.num_keys + layout.max_num_updates) * layout.num_clients;

  auto num_proc = layout.num_clients + layout.num_servers;

  if (layout.proc_id > num_proc) {
    std::cerr << "Invalid process id error: " << layout.proc_id << " is bigger than "
              << num_proc << " (number of processes)" << std::endl;
    return 1;
  }
  bool is_server = layout.proc_id <= layout.num_servers;
  bool is_client = layout.proc_id > layout.num_servers;

  if (is_client) {
    std::cout << "Workload: " << workload << std::endl;
    std::cout << "Log entry size: " << layout.fullEntrySize() << std::endl;
    std::cout << "Read entry size: " << layout.kvSize() << std::endl;
    // pin_main_to_core(0);
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
        is_client ? layout.clientSize() :
            layout.serverSize();
    size_t alignment = 64;
    cb.allocateBuffer("shared-buf", allocated_size, alignment);
  }

  cb.registerMr(
      "shared-mr", "primary", "shared-buf",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
          ctrl::ControlBlock::REMOTE_ATOMIC);
  cb.registerCq("cq");
  if (is_client) {
    cb.registerCq("cq2");
  }
  
  auto local_region = cb.mr("shared-mr").addr;

  // if (is_server) {
  //   for (uint64_t i = 0; i < layout.keys_per_server; i++) {
  //     *reinterpret_cast<uint64_t*>(layout.getServerKV(local_region, i)->key()) = i;
  //   }
  // }

  // 3. We establish reliable connections.
  auto& store = memstore::MemoryStore::getInstance();

  std::vector<ProcId> remote_ids;
  for (ProcId id = 1; id <= num_proc; id++) {
    if (id == layout.proc_id) {
      continue;
    }
    // if (is_client == (id > layout.num_servers)) {
    //   continue; // Only connect to clients if we are a server or backup and vice versa
    // }
    remote_ids.push_back(id);
  }

  RcConnectionExchanger<ProcId> ce(layout.proc_id, remote_ids, cb);
  for (auto const& id : remote_ids) {
    ce.configure(id, "primary", "shared-mr", "cq", "cq");
  }
  ce.announceAll(store, "qp");

  ce.announceReady(store, "qp", "prepared");
  ce.waitReadyAll(store, "qp", "prepared");

  ce.unannounceReady(store, "qp", "finished"); // Clean up from previous runs

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
    std::vector<std::shared_ptr<dory::race::ClientIndex>> indexes;
    for (ProcId id = 1; id <= layout.num_servers; id++) {
      auto x = std::make_shared<dory::race::ClientIndex>(
        layout.proc_id, id, bucket_bits, std::to_string(id));
      indexes.emplace_back(x);
    }

    // TODO(zyf): handle multiple servers
    auto client = Client(layout, local_region, ce, layout.num_servers);
    auto pointer_cache = LRUCache<HashedKey, uint64_t>(pointer_cache_size);

    ProcId first_client = layout.num_servers + 1;
    if (layout.proc_id == first_client) {
      std::cout << "Querying YCSB for the set of initial key-pairs... " << std::flush;
      std::vector<std::pair<std::string, std::string>> inserts = {};

      {
        auto output = exec(ycsb_path + " load basic -P " + workload + " -s 2> /dev/null");
        std::string line;
        while (std::getline(output, line)) {
          if (std::strncmp("INSERT ", line.c_str(), std::string("INSERT ").length())) {
            continue;
          }
          auto keystart = std::string("INSERT usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          auto start = keyend + std::string(" [ field0=").length();
          auto end = line.length() - std::string(" ]").length();
          auto value = line.substr(start, end - start);

          inserts.emplace_back(key, value);

          // std::cout << line << std::endl;
          // std::cout << inserts.back().first << ": ";
          // for (auto s : inserts.back().second) {
          //   std::cout << s << "€";
          // }
          // std::cout << std::endl;
        }
      }
      std::cout << "Done." << std::endl;


      // Initialize the index
      std::vector<dory::race::ClientIndex::TryInsertFuture> futures;
      futures.reserve(layout.num_servers);
      std::cout << "Inserting the initial key-pairs... " << std::flush;
      for (size_t kvIndex = 0; kvIndex < inserts.size(); kvIndex++) {
        auto insert = inserts.at(kvIndex);
        auto key = insert.first;
        auto value = insert.second;

        auto hkey = hash(key);
        auto search = indexes[0]->search(hkey);

        auto* local_log = client.prepareToWriteEntry(key, value);
        auto kv_id = client.write(local_log, 0);

        auto search_res = search.await();
        if (search_res.nb_free == 0) {
          throw std::runtime_error("No free space in the index");
        }
        auto free = search_res.free;

        futures.clear();
        for (size_t s = 0; s < layout.num_servers; s++) {
          auto& index = *(indexes.at(s));
          futures.emplace_back(index.tryInsert(free, hkey, kv_id));
        }

        for (size_t s = 0; s < layout.num_servers; s++) {
          auto res = futures.at(s).await();
          if (res.asUint64() != 0) {
            throw std::runtime_error("Failed to insert key " + key + " in server " + std::to_string(s));
          }
        }


        // pointer_cache.put(hkey, kv_id);
        // fmt::print("Ts: {}, CachedTs : {}\n", local_log->kv.ts, *(pointer_cache.get(hkey)));

        // auto sf = index.search(hkey).await();

        // if (sf.nb_matches == 0) {
        //   std::cout << "No match for key " << fakekey << " even though it was just inserted ???" << std::endl;
        // }
      }
      std::cout << " Done." << std::endl;
    }

    std::cout << "Querying YCSB for the list of operations... " << std::flush;
    std::vector<std::pair<std::string, std::optional<std::string>>> operations = {};

    {
      auto output = exec(ycsb_path + " run basic -P " + workload + " -s 2> /dev/null");
      std::string line;
      while (std::getline(output, line)) {
        if (!(std::strncmp("READ ", line.c_str(), std::string("READ ").length()))) {
          auto keystart = std::string("READ usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          operations.emplace_back(key, std::optional<std::string>());
        } else if (!(std::strncmp("UPDATE ", line.c_str(), std::string("UPDATE ").length()))) {
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

        // std::cout << line << std::endl;
        // std::cout << operations.back().first << ": ";
        // if (operations.back().second.has_value()) {
        //   std::cout << "UDPATE " << operations.back().second.value()
        //             << " (" << operations.back().second.value().length()
        //             << ", " << (line.length() - operations.back().first.length() - std::string("UPDATE usertable  [ field0= ]").length()) << ")";
        // } else { std::cout << "READ"; }
        // std::cout << std::endl;

        // if (operations.size() >= 10) {
        //   throw std::runtime_error("Only 10 operations for now");
        // }
      }
    }

    std::cout << "Done." << std::endl;
    // std::cout << "Waiting for the initialization of other clients... " << std::flush;

    ce.announceReady(store, "qp", "initialized");
    ce.waitReadyAll(store, "qp", "initialized");

    // std::cout << "Done." << std::endl;
    std::cout << "Running the benchmark... " << std::flush;

    std::vector<dory::race::ClientIndex::TryInsertFuture> futures;
    futures.reserve(layout.num_servers - 1);

    std::chrono::time_point now = std::chrono::steady_clock::now();
    auto start = now;
    auto end = now;
    auto last_time = start;
    auto zero = now - now;
    auto total_search_time = zero;
    auto total_read_time = zero;
    auto total_update_time = zero;
    int32_t total_read_count = 0;
    int32_t total_update_count = 0;
    int32_t total_cache_hit_count = 0;
    int32_t total_true_cache_hit_count = 0;

    for (size_t i = 0; i < total_iter_count; i++) {
      if (i == start_measurements) {
        start = now;
      } else if (i == stop_measurements) {
        end = now;
      }
      
      auto operation = operations[i % operations.size()];

      // std::cout << operation.first << ": ";
      // if (operation.second.has_value()) {
      //   std::cout << "UDPATE " << operation.second.value()
      //             << " (" << operation.second.value().length() << ")";
      // } else { std::cout << "READ"; }
      // std::cout << std::endl;

      auto true_key = operation.first;
      auto hkey = hash(true_key);
      auto random_server = (reinterpret_cast<uint64_t const*>(hkey.data())[0] % (layout.num_servers - 1)) + 1;
      auto main_server = 0UL;
      auto& main_index = *(indexes.at(main_server));

      auto search = main_index.search(hkey);
      uint64_t new_kv_id;
      
      auto cache_entry = pointer_cache.get(hkey);
      if (cache_entry) {
        auto kv_id = *cache_entry;
        now = std::chrono::steady_clock::now();
        auto search_time = now - last_time;
        if (start_measurements <= i && i < stop_measurements) {
          total_cache_hit_count += 1;
        }

        if (operation.second.has_value()) {
          auto value = operation.second.value();

          // Phase 1 : Write KV and read index

          auto* local_log = client.prepareToWriteEntry(true_key, value);
          new_kv_id = client.readAndWrite(local_log, kv_id, random_server).second;
          auto matches = search.await();
          
          auto prev_entry = search.entryFor(kv_id);
          auto offset = search.getOffsetOf(prev_entry);
          if (!offset.has_value()) {
            // Index was updated
            goto cache_miss_or_fake_hit;
          }

          futures.clear();
          // Phase 2: Update backup indexes
          for (size_t s = 1; s < layout.num_servers; s++) {
            auto& index = *(indexes.at((main_server + s) % layout.num_servers));
            futures.emplace_back(index.tryUpdate(*offset, prev_entry, hkey, new_kv_id));
          }

          auto res = futures.at(0).await();

          if (res.asUint64() != prev_entry.asUint64()) {
            // Lost the game, letting the other finish.
            auto iter = 0;
            do {
              res = main_index.tryCheck(*offset).await();
              if(iter++ == 1000) {
                fmt::print("Concurrent update from other client (ts: {}), waiting for the update to finish.\r", prev_entry.asUint64());
              }
            } while(res.asUint64() == prev_entry.asUint64());
            new_kv_id = res.getValue();
            goto cache_hit_update_finish;
          }

          for (size_t s = 2; s < layout.num_servers; s++) {
            res = futures.at(s - 1).await();

            if (res.asUint64() != prev_entry.asUint64()) {
              // Won the game, erasing the other value.
              auto& index =
                  *(indexes.at((main_server + s) % layout.num_servers));
              auto new_res =
                  index.tryUpdate(*offset, prev_entry, hkey, new_kv_id).await();
              if (new_res.asUint64() != res.asUint64()) {
                throw std::runtime_error(
                    "Error: Failed to overwrite other CAS in the backup index "
                    "for key " +
                    true_key + " in server " + std::to_string(1 + main_server));
              }
            }
          }


          // Phase 3: Write "log commit"
          client.writeLogCommit(main_server, kv_id, new_kv_id);

          // Phase 4: Write main index
          {
            auto res = main_index.tryUpdate(*offset, prev_entry, hkey, new_kv_id).await();
            if (res.asUint64() != prev_entry.asUint64()) {
              throw std::runtime_error("Error: Failed to update the main index for key " + true_key +
                                      " in server " + std::to_string(1 + main_server));
            }
          }

          cache_hit_update_finish:

          pointer_cache.put(hkey, new_kv_id); // TODO(zyf): actually not cache ?
          // fmt::print("Ts: {}, CachedTs : {}\n", local_log->kv.ts, *(pointer_cache.get(hkey)));

          now = std::chrono::steady_clock::now();
          if (start_measurements <= i && i < stop_measurements) {
            auto measure = now - last_time;
            update_stat.addMeasurement(measure);
            total_update_time += measure;
            ++total_update_count;
          }
        } else {
          auto* ignored = client.read(random_server, kv_id);
          auto matches = search.await();

          auto prev_entry = search.entryFor(kv_id);
          auto offset = search.getOffsetOf(prev_entry);
          if (!offset.has_value()) {
            // Index was updated
            goto cache_miss_or_fake_hit;
          }

          now = std::chrono::steady_clock::now();
          if (start_measurements <= i && i < stop_measurements) {
            auto measure = now - last_time;
            get_stat.addMeasurement(measure);
            total_read_time += measure;
            ++total_read_count;
          }
        }
        
        if (start_measurements <= i && i < stop_measurements) {
          search_stat.addMeasurement(search_time);
          total_search_time += search_time;
          total_true_cache_hit_count += 1;
        }

        last_time = now;
        continue;
      }

      // Phase 1.1: Read index
      if (operation.second.has_value()) {
        // And pre-write new kv
        auto value = operation.second.value();
        auto* local_log = client.prepareToWriteEntry(true_key, value);
        new_kv_id = client.write(local_log, random_server);
      }

      cache_miss_or_fake_hit:

      auto sf = search.await();
      
      now = std::chrono::steady_clock::now();
      if (start_measurements <= i && i < stop_measurements) {
        auto measure = now - last_time;
        search_stat.addMeasurement(measure);
        total_search_time += measure;
      }

      if (sf.nb_matches == 0) {
        std::cout << "No match for key " << true_key << std::endl;
        throw std::runtime_error("No match for key");
      }

      // std::cout << "Multiple matches for key " << true_key << std::endl;

      auto found = false;

      for (size_t j = 0; j < sf.nb_matches; j++) {
        // Phase 1.2 : read kv(s)
        auto kv_id = sf.matches[j].getValue();
        auto entry = client.read(random_server, kv_id);

        if (!(std::strncmp(true_key.c_str(), entry->kv.key(), layout.key_size))) {
          found = true;
          if (operation.second.has_value()) {
            auto value = operation.second.value();

            auto prev_entry = search.entryFor(kv_id);
            auto offset = search.getOffsetOf(prev_entry);
            if (!offset.has_value()) {
              throw std::runtime_error("Error: could not find entry ???");
            }

            // Phase 2: Update backup indexes
            futures.clear();
            for (size_t s = 1; s < layout.num_servers; s++) {
              auto& index = *(indexes.at((main_server + s) % layout.num_servers));
              futures.emplace_back(index.tryUpdate(*offset, prev_entry, hkey, new_kv_id));
            }

            auto res = futures.at(0).await();

            if (res.asUint64() != prev_entry.asUint64()) {
              // Lost the game, letting the other finish.
              auto iter = 0;
              do {
                res = main_index.tryCheck(*offset).await();
                if(++iter == 1000) {
                  fmt::print("Concurrent update from other client (ts: {}), waiting for the update to finish.\r", prev_entry.asUint64());
                }
              } while(res.asUint64() == prev_entry.asUint64());
              new_kv_id = res.getValue();
              goto cache_miss_update_finish;
            }

            for (size_t s = 2; s < layout.num_servers; s++) {
              res = futures.at(s - 1).await();

              if (res.asUint64() != prev_entry.asUint64()) {
                // Won the game, erasing the other value.
                auto& index =
                    *(indexes.at((main_server + s) % layout.num_servers));
                auto new_res =
                    index.tryUpdate(*offset, prev_entry, hkey, new_kv_id)
                        .await();
                if (new_res.asUint64() != res.asUint64()) {
                  throw std::runtime_error(
                      "Error: Failed to overwrite other CAS in the backup "
                      "index "
                      "for key " +
                      true_key + " in server " +
                      std::to_string(1 + main_server));
                }
              }
            }

            // Phase 3: Write "log commit"
            client.writeLogCommit(main_server, kv_id, new_kv_id);

            // Phase 4: Write main index
            {
              auto res =
                  main_index.tryUpdate(*offset, prev_entry, hkey, new_kv_id)
                      .await();
              if (res.asUint64() != prev_entry.asUint64()) {
                throw std::runtime_error(
                    "Error: Failed to update the main index for key " +
                    true_key + " in server " + std::to_string(1 + main_server));
              }
            }

            cache_miss_update_finish:

            pointer_cache.put(hkey, new_kv_id); // TODO(zyf): actually not cache ?
            // fmt::print("Ts: {}, CachedTs : {}\n", local_log->kv.ts, *(pointer_cache.get(hkey)));

            now = std::chrono::steady_clock::now();
            if (start_measurements <= i && i < stop_measurements) {
              auto measure = now - last_time;
              update_stat.addMeasurement(measure);
              total_update_time += measure;
              ++total_update_count;
            }
            }
            else {
              // Well, we're already done.
              pointer_cache.put(hkey, kv_id);

              now = std::chrono::steady_clock::now();
              if (start_measurements <= i && i < stop_measurements) {
                auto measure = now - last_time;
                get_stat.addMeasurement(measure);
                total_read_time += measure;
                ++total_read_count;
              }
            }

          break;
        }
        // } else {
        //   std::cout << "Invalid match for key " << true_key
        //             << ": " << entry->kv.key()
        //             << " @ " << kv_id << std::endl;
        // }
      }

      last_time = now;

      if (found == false) {
        std::cout << sf.nb_matches << " matchs for key " << true_key
                  << " yet none is correct." << std::endl;

        throw std::runtime_error(
            "Matchs found for key but no correct match");
      }
    }
    std::cout << "Done. Results:" << std::endl;

    fmt::print("\n");
    fmt::print("################ Main stats:\n");
    fmt::print("######## SEARCH stats:\n");
    fmt::print("cache-hits: {}%\n",
               search_stat.getMeasurementCount() > 0
                   ? static_cast<double>(total_cache_hit_count) * 100.0 /
                         static_cast<double>(search_stat.getMeasurementCount())
                   : 0);
    fmt::print("true-hit: {}% ({}% of hits)\n", (100.0 * total_true_cache_hit_count) / static_cast<double>(iter_count), (100.0 * total_true_cache_hit_count) / total_cache_hit_count);
    search_stat.report(true);
    fmt::print("######## GET stats:\n");
    get_stat.report(true);
    fmt::print("######## UPDATE stats:\n");
    update_stat.report(true);
    fmt::print("global average: {}\n", (end - start) / static_cast<double>(iter_count));
    fmt::print("aggregated tput: {}kops\n", (layout.num_clients * iter_count * 1'000'000) / static_cast<uint64_t>((end - start).count()));
    std::cout << std::flush;

    ce.announceReady(store, "qp", "finished");
    ce.waitReadyAll(store, "qp", "finished");
    ce.unannounceReady(store, "qp", "initialized");
  } else if (is_server) {
    std::vector<ProcId> client_ids;
    for (ProcId id = layout.num_servers + 1; id <= num_proc; id++) {
      client_ids.push_back(id);
    }
    dory::race::ServerIndex server(layout.proc_id, client_ids, bucket_bits, std::to_string(layout.proc_id));
    ce.announceReady(store, "qp", "initialized");
    ce.announceReady(store, "qp", "finished");
    ce.waitReadyAll(store, "qp", "finished");
    ce.unannounceReady(store, "qp", "initialized");
  }

  // 9. Clean up.
  ce.unannounceReady(store, "qp", "connected");
  
  std::cout << "###DONE###" << std::endl;
  return 0;
}
