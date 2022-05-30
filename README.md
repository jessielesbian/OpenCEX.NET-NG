# OpenCEX.NET Next Generation: high-performance open-source cryptocurrency exchange

NOTE: This is still work in progress.

## What's wrong with OpenCEX Classic and OpenCEX.NET?
1. Hardcoded listings
2. Lack of flexibility
3. Slow blocking IO

## The OpenCEX.NET Next Generation enhancements
1. New configurations engine
2. New modloader
3. More event-driven

## Isn't OpenCEX.NET event-driven enough?
OpenCEX.NET is partially event-driven since it uses an event queue and multiple execution threads to execute requests. But that's not good enough since request execution threads block on IO operations, which means wasted CPU. OpenCEX.NET Next Generation will offer better request throughput and latency by the use of nonblocking IO and better thread pooling.

## OpenCEX.NET parallelism enhancement solution
1. Concurrent job chaining
2. Execution thread pooling
3. Async functions
