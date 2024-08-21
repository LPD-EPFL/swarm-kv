#pragma once

#if defined(DALEK)
#include "dalek-impl.hpp"
#elif defined(SODIUM)
#undef DALEK_LEGACY
#include "sodium-impl.hpp"
#else
#error "Define DALEK or SODIUM"
#endif
