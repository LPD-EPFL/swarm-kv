#pragma once

#if defined(DALEK)
#define IMPL Dalek
#include "dalek-impl.hpp"
#elif defined(SODIUM)
#define IMPL Sodium
#include "sodium-impl.hpp"
#else
#ifndef DORY_TIDIER_ON
#error "Define DALEK or SODIUM"
#endif
#endif
