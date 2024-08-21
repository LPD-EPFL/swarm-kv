#pragma once

#include <thread>

namespace dory {

class PinnableThread {
 public:
  virtual void pinToCore(int cpu_id) = 0;
};

void pin_main_to_core(int cpu_id);
void reset_main_pinning();

void pin_thread_to_core(std::thread &thd, int cpu_id);
void reset_thread_pinning(std::thread &thd);

void set_thread_name(std::thread::native_handle_type pthread, char const *name);
void set_thread_name(std::thread &thd, char const *name);
}  // namespace dory
