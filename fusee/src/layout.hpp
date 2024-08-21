#pragma once

#include "main.hpp"

using chsum_t = std::array<uint8_t, 16>;

struct KV {
  uint64_t chsum;
  uint32_t key_size;
  uint32_t value_size;

  uint8_t* rawKV() {
    return reinterpret_cast<uint8_t*>(this) + sizeof(KV);
  }

  char* key() {
    return reinterpret_cast<char*>(rawKV());
  }

  char* value() {
    return key() + key_size;
  }

  size_t size() const { return sizeof(KV) + key_size + value_size; }

  size_t rawSize() const { return key_size + value_size; }
};

struct FullKVEntry {
  uint64_t osef1;
  uint32_t osef2;
  uint64_t old;
  uint8_t chsum;
  uint8_t osef3;
  KV kv;
};

struct KVReadSlot {
  // int64_t expire_ts;
  uintptr_t kv_entry;
  KV kv;
};

struct Layout {
  ProcId proc_id;
  uint64_t num_clients;
  uint64_t num_servers = 3;

  uint32_t key_size;
  uint32_t value_size;
  uint32_t rawKVSize() const {
    return static_cast<uint32_t>(sizeof(uint8_t)) * (key_size + value_size);
  }
  uint32_t kvSize() const {
    return static_cast<uint32_t>(sizeof(KV)) + rawKVSize();
  }
  uint32_t fullEntrySize() const {
    return static_cast<uint32_t>(sizeof(FullKVEntry)) + rawKVSize();
  }
  uint32_t kvReadSlotSize() const {
    return static_cast<uint32_t>(sizeof(KVReadSlot)) + rawKVSize();
  }

  uint64_t num_keys;
  uint64_t max_num_updates;
  uint64_t keys_per_server;
  uint64_t dataSize() const { return fullEntrySize() * keys_per_server; }
  uint64_t clientLogSize() const { return fullEntrySize(); }

  uint64_t kv_read_slot_count;
  uint64_t kvReadBufferSize() const {
    return kv_read_slot_count * kvReadSlotSize();
  }

  static uint64_t clientLogsOffset() { return 0; }
  uint64_t clientKVCacheOffset() const { return clientLogSize(); }
  uint64_t clientCasRetOffset() const { return clientLogSize() + kvReadBufferSize(); }
  uint64_t clientSize() const { return clientLogSize() + kvReadBufferSize() + num_servers * sizeof(uint64_t); }

  uint64_t serverDataOffset() const { return 0; }
  uint64_t serverSize() const { return dataSize(); }

  uintptr_t getClientLogAddress(uintptr_t region) const {
    return region + clientLogsOffset();
  }

  FullKVEntry* getClientLog(uintptr_t region) const {
    return reinterpret_cast<FullKVEntry*>(getClientLogAddress(region));
  }

  uintptr_t getClientKVCacheAddress(uintptr_t region, uint64_t i) const {
    return region + clientKVCacheOffset() +
           kvReadSlotSize() * (i % kv_read_slot_count);
  }

  KVReadSlot* getClientKVCache(uintptr_t region, uint64_t i) const {
    return reinterpret_cast<KVReadSlot*>(getClientKVCacheAddress(region, i));
  }

  uint64_t* getClientCasRet(uintptr_t region, uint64_t server) const {
    return reinterpret_cast<uint64_t*>(region + clientCasRetOffset() +
           (server % num_servers) * sizeof(uint64_t));
  }

  uintptr_t getServerEntryAddress(uintptr_t region, uint64_t i) const {
    return region + serverDataOffset() + fullEntrySize() * (i % keys_per_server);
  }

  FullKVEntry* getServerEntry(uintptr_t region, uint64_t i) const {
    return reinterpret_cast<FullKVEntry*>(getServerEntryAddress(region, i));
  }

  KV* getServerKV(uintptr_t region, uint64_t i) const {
    return &(getServerEntry(region, i)->kv);
  }

  uintptr_t getServerKVAddress(uintptr_t region, uint64_t i) const {
    return reinterpret_cast<uintptr_t>(getServerKV(region, i));
  }
};
