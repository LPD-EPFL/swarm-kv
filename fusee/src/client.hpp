#pragma once

#include "layout.hpp"
#include <dory/third-party/blake3/blake3.h>
#include "race/client_index.hpp"

static thread_local std::vector<struct ibv_wc> wce;

static thread_local XXH3_state_t* hash_state{XXH3_createState()};

class Client {
    Layout layout;
    uintptr_t local_region;

    std::vector<dory::conn::ReliableConnection*> server_connections;

    uint64_t next_kv_id;
    uint64_t write_count = 0;
    size_t next_cache_entry = 0;

    public:
    Client(Layout layout, uintptr_t local_region, dory::conn::RcConnectionExchanger<ProcId> &ce, uint64_t num_servers) {
        this->layout = layout;
        this->local_region = local_region;
        next_kv_id = layout.proc_id;

        for (size_t i = 0; i < num_servers; i++) {
          server_connections.push_back(&ce.connections().at(i + 1));
        }

        wce.reserve(num_servers);
    }

    KVReadSlot* read(uint64_t server, uint64_t kv_id) {
      size_t cache_id = next_cache_entry++ % layout.kv_read_slot_count;
      auto& rc = *server_connections.at(server % layout.num_servers);
      auto remote_kv = layout.getServerKVAddress(rc.remoteBuf(), kv_id);
      auto* cache = layout.getClientKVCache(local_region, cache_id);

      rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead, (kv_id * 1000) + 0, &(cache->kv),
                        layout.kvSize(), remote_kv);

      while (true) {
        wce.resize(1);
        if (!rc.pollCqIsOk(dory::conn::ReliableConnection::Cq::SendCq, wce)) {
          throw std::runtime_error(
              "Error polling Cq for " +
              std::to_string(static_cast<unsigned>(server + 1)) + ".");
        }
        if (!wce.empty()) {
          break;
        }
        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      if (wce[0].status != IBV_WC_SUCCESS) {
        throw std::runtime_error(
            "RDMA read from " +
            std::to_string(static_cast<unsigned>(server + 1)) + " failed.");
      }
      if(wce[0].wr_id != (kv_id * 1000) + 0) {
        throw std::runtime_error(
            "read: wr_id != kv_id." + std::to_string(wce[0].wr_id) + " != " + std::to_string(kv_id));
      }

      chsum_t chsum;
      kvHash(&(cache->kv), chsum);
      uint64_t subsum = reinterpret_cast<uint64_t*>(chsum.data())[0];
      if (cache->kv.key_size != 0 && subsum != cache->kv.chsum) {
        std::cerr << write_count - 1 << "," << cache_id << ": ";
        std::cerr << fmt::format("{:016x}", subsum) << " != "
                  << fmt::format("{:016x}", cache->kv.chsum);
        std::cerr << std::endl;
        throw std::runtime_error("Invalid checksum.");
        // TODO(zyf): Error handling (unfortunate timing, retry or read log)
      }

      cache->kv_entry = remote_kv;
      return cache;
    }

    FullKVEntry* getFreeLogEntry() {
      write_count++;
      auto* local_log = layout.getClientLog(local_region);

      memset(local_log, 0, layout.fullEntrySize());

      return local_log;
    }

    FullKVEntry* prepareToWriteEntry(std::string& key, std::string& value) {
      auto* local_log = getFreeLogEntry();

      local_log->kv.key_size = layout.key_size;
      local_log->kv.value_size = layout.value_size;

      if (key.size() >= layout.key_size) {
        throw std::runtime_error("Key too long.");
      }
      if (value.size() >= layout.value_size) {
        throw std::runtime_error("Value too long.");
      }

      key.copy(local_log->kv.key(), layout.key_size);
      value.copy(local_log->kv.value(), layout.value_size);

      chsum_t chsum;
      kvHash(&(local_log->kv), chsum);
      local_log->kv.chsum = reinterpret_cast<uint64_t*>(chsum.data())[0];
      local_log->chsum = chsum.data()[8];

      return local_log;
    }

    std::pair<KVReadSlot*, uint64_t> readAndWrite(FullKVEntry* local_log, uint64_t old_kv_id, uint64_t random_server) {
      size_t cache_id = next_cache_entry++ % layout.kv_read_slot_count;
      auto* cache = layout.getClientKVCache(local_region, cache_id);

      {
        auto& rc = *server_connections.at(random_server);

        auto remote_kv = layout.getServerKVAddress(rc.remoteBuf(), old_kv_id);

        rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead, (old_kv_id * 1000) + 4, &(cache->kv),
                          layout.kvSize(), remote_kv, false);
      }

      auto new_kv_id = write(local_log, random_server);

      return {cache, new_kv_id};
    }

    uint64_t write(FullKVEntry* local_log, uint64_t random_server) {
      auto new_kv_id = next_kv_id;
      next_kv_id += layout.num_clients;

      for(uint64_t i = 0; i < layout.num_servers; i++) {
        auto& rc = *server_connections.at((random_server + i) % layout.num_servers);
        auto remote_addr = layout.getServerEntryAddress(rc.remoteBuf(), new_kv_id);

        rc.postSendSingle(dory::conn::ReliableConnection::RdmaWrite, (new_kv_id * 1000) + 5,
                          local_log, layout.fullEntrySize(), remote_addr, true);
      }

      size_t to_poll = layout.num_servers;

      while (to_poll > 0) {
        wce.resize(to_poll);
        if (!(*server_connections[0]).pollCqIsOk(dory::conn::ReliableConnection::Cq::SendCq, wce)) {
          throw std::runtime_error(
              "Error polling Cq.");
        }
        if (wce.size() > 0) {
          for(auto w : wce) {
            if (w.status != IBV_WC_SUCCESS) {
              std::cerr << ibv_wc_status_str(w.status) << std::endl;
              std::cerr << write_count << std::endl;
              throw std::runtime_error("RDMA write failed.");
            }
            if(w.wr_id != (new_kv_id * 1000) + 5) {
              throw std::runtime_error(
                  "update: wrong wr_id: " + std::to_string(wce[0].wr_id) + " != " + std::to_string((new_kv_id * 1000) + 5));
            }
          }
          to_poll -= wce.size();
          if (to_poll == 0) {
            break;
          }
        } else {
          // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      }
      
      return new_kv_id;
    }

    void writeLogCommit(uint64_t server, uint64_t old_kv_id, uint64_t new_kv_id) {
      for(size_t i = 0; i < layout.num_servers; i++) {
        auto& rc = *server_connections.at((server + i) % layout.num_servers);
        auto remote_entry = layout.getServerEntry(rc.remoteBuf(), new_kv_id);
        auto remote_old = reinterpret_cast<uintptr_t>(&(remote_entry->old));
        auto swapback = layout.getClientCasRet(local_region, i);

        rc.postSendSingleCas((old_kv_id * 1000) + 6, swapback, remote_old, 0, old_kv_id + 1, true);
      }

      size_t to_poll = layout.num_servers;

      while (true) {
        wce.resize(to_poll);
        if (!(*server_connections[0]).pollCqIsOk(dory::conn::ReliableConnection::Cq::SendCq, wce)) {
          throw std::runtime_error(
              "Error polling Cq.");
        }
        if (wce.size() > 0) {
          for(auto w : wce) {
            if (w.status != IBV_WC_SUCCESS) {
              std::cerr << ibv_wc_status_str(w.status) << std::endl;
              std::cerr << write_count << std::endl;
              throw std::runtime_error("RDMA CAS of old pointers failed.");
            }
            if(w.wr_id != (old_kv_id * 1000) + 6) {
              throw std::runtime_error(
                  "update: wrong wr_id: " + std::to_string(wce[0].wr_id) + " != " + std::to_string((old_kv_id * 1000) + 6));
            }
          }
          to_poll -= wce.size();
          if (to_poll == 0) {
            break;
          }
        } else {
          // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      }
    }

private:
    static void kvHash(KV* kv, chsum_t& out) {
      XXH3_128bits_reset(hash_state);
      XXH3_128bits_update(hash_state, &(kv->key_size), kv->size() - offsetof(KV, key_size));
      *reinterpret_cast<XXH128_hash_t*>(out.data()) = XXH3_128bits_digest(hash_state);
    }
};