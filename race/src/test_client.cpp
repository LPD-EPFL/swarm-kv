#include <memory>
#include <thread>
#include <vector>

#include <hipony/enumerate.hpp>

#include <dory/shared/match.hpp>
#include <dory/shared/logger.hpp>

#include "client.hpp"

using Client = dory::race::Client;
using Key = dory::race::Client::Key;
using Value = dory::race::Client::Value;

static void search(Client& client, dory::logger& logger, std::vector<Key> const& keys) {
  std::vector<Client::SearchFuture> futures;
  for (auto const& key : keys) {
    logger->info("Searching {}...", key);
    futures.emplace_back(client.search(key));
  }
  for (auto const& [k, key] : hipony::enumerate(keys)) {
    auto res = futures[k].await();
    logger->info("Potential matches for {}: {}", key, res.nb_matches);
    for (size_t i = 0; i < res.nb_matches; i++)
      logger->info("- #{}={:x}", i, *res.matches[i]);
  }
}

static void insert(Client& client, dory::logger& logger, std::vector<std::pair<Key, Value>> const kvs) {
  std::vector<Client::TryInsertFuture> futures;
  for (auto const& [key, val] : kvs) {
    logger->info("Inserting {}={:x}...", key, val);
    futures.emplace_back(client.try_insert(key, val, {}));
  }
  for (auto const& [k, kv] : hipony::enumerate(kvs)) {
    auto res = futures[k].await();
    dory::match{res} (
      [&](Client::Ok&) {
        logger->info("{}={:x} inserted!", kv.first, kv.second);
      },
      [&](Client::Matches& matches) {
        logger->warn("Key {} may already exist:", kv.first);
        for (size_t i = 0; i < matches.nb_matches; i++)
          logger->warn("- #{}={:x}", i, *matches.matches[i]);
      }
    );
  }
}

static void remove(Client& client, dory::logger& logger, std::vector<std::pair<Key, Value>> const kvs) {
  std::vector<Client::TryDeleteFuture> futures;
  for (auto const& [key, val] : kvs) {
    logger->info("Deleting {}={:x}...", key, val);
    futures.emplace_back(client.try_delete(key, val));
  }
  for (auto const& [k, kv] : hipony::enumerate(kvs)) {
    if (futures[k].await()) {
      logger->info("Deleted {}={:x}", kv.first, kv.second);
    } else {
      logger->warn("Did not delete {}={:x}", kv.first, kv.second);
    }
  }
}

int main() {
  LOGGER_DECL_INIT(logger, "Test");
  auto client = std::make_shared<Client>(2, 1, 10);
  search(*client, logger, {"dog", "cat", "wolf"});
  insert(*client, logger, {{"dog", 0x1337}, {"dog", 0xC0DE}, {"fox", 0xC01A}});
  search(*client, logger, {"dog"});
  remove(*client, logger, {{"dog", 0x1337}, {"dog", 0x1337}});
  search(*client, logger, {"dog"});
  insert(*client, logger, {{"dog", 0xC0DE}});
  search(*client, logger, {"dog", "fox"});

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return 0;
}