//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/28.
// test/container/simd_test.cpp
//
//===-----------------------------------------------------

//#include <gtest/gtest.h>
//#include <stdio.h>
//#include <cmath>
//#include <memory>
//#include <numeric>
//
//#include <highway.h>
//#include <aligned_allocator.h>
//#include <nanobenchmark.h>
//#include <foreach_target.h>
//
//HWY_BEFORE_NAMESPACE();
//namespace hwy {
//namespace HWY_NAMESPACE {
//
//#if HWY_TARGET != HWY_SCALAR
//using hwy::HWY_NAMESPACE::CombineShiftRightLanes;
//#endif
//
//class TwoArray {
// public:
//  // Must be a multiple of the vector lane count*8
//  static size_t NumItems() { return 3456; }
//
//  TwoArray()
//    : a_(AllocateAligned<float>(NumItems() * 2)), b_(a_.get() + NumItems()) {
//    const auto init = static_cast<float>(Unpredictable1());
//    std::iota(a_.get(), a_.get() + NumItems(), init);
//    std::iota(b_, b_ + NumItems(), init);
//  }
//
// protected:
//  AlignedFreeUniquePtr<float[]> a_;
//  float* b_;
//};
//
//// Measures durations, verifies results, prints timings.
//template<class Benchmark>
//void RunBenchMark(const char *caption) {
//  printf("%10s: ", caption);
//  const size_t k_nums_inputs = 1;
//  const size_t num_items = Benchmark::NumItems() * size_t (Unpredictable1());
//  const FuncInput inputs[k_nums_inputs] = {num_items};
//  Result results[k_nums_inputs];
//
//  Benchmark benchmark;
//
//  Params p;
//  p.verbose = false;
//  p.max_evals = 7;
//  p.target_rel_mad = 0.002;
//  const size_t num_results = MeasureClosure(
//      [&benchmark] (const FuncInput input) { return benchmark(input); }, inputs,
//      k_nums_inputs, results, p);
//
//  if (num_results != k_nums_inputs) {
//    fprintf(stderr, "MeasureClosure failed.\n");
//  }
//
//  benchmark.Verify(num_items);
//
//  for (size_t i = 0; i < num_results; ++i) {
//    const double cycles_per_item =
//        results[i].ticks / static_cast<double>(results[i].input);
//    const double mad = results[i].variability * cycles_per_item;
//    printf("%6d: %6.3f (+/- %5.3f)\n", static_cast<int>(results[i].input),
//           cycles_per_item, mad);
//  }
//}
//
//void Intro() {
//  const float in[16] = {1, 2, 3, 4, 5, 6};
//  float out[16];
//  const ScalableTag<float> d; // largest possible vector
//  for (size_t i = 0; i < 16; i += Lanes(d)) {
//    const auto vec = LoadU(d, in + i); // no alignment requirement
//    auto result = Mul(vec, vec);
//    result = Add(result, result);
//    Store(result, d, out + i);
//  }
//  printf("\nF(x)->2*x^2, F(%.0f) = %.1f\n", in[2], out[2]);
//}
//
//// BEGINNER: dot product
//// 0.4 cyc/float = bronze, 0.25 = silver, 0.15 = gold
//class BenchmarkDot : public TwoArray {
// public:
//    BenchmarkDot() : dot_(-0.1f) {}
//
//    FuncOutput operator()(const size_t num_items) {
//      const ScalableTag<float> d;
//      const size_t N = Lanes(d);
//      using V = decltype(Zero(d));
//      // Compiler doesn't make independent sum* accumulators, so unroll manually.
//      // We cannot use an array because V might be a sizeless type. For reasonable
//      // code, we unroll 4x, but 8x might help (2 FMA ports * 4 cycle latency).
//      V sum[4] = {Zero(d), Zero(d), Zero(d), Zero(d)};
//      const float * const HWY_RESTRICT pa = &a_[0];
//      const float* const HWY_RESTRICT pb = b_;
//      for (size_t i = 0; i < num_items; i += 4 * N) {
//        const auto a0 = Load(d, pa + i + 0 * N);
//        const auto b0 = Load(d, pb + i + 0 * N);
//        sum[0] = MulAdd(a0, b0, sum[0]);
//        const auto a1 = Load(d, pa + i + 1 * N);
//        const auto b1 = Load(d, pb + i + 1 * N);
//        sum[1] = MulAdd(a1, b1, sum[1]);
//        const auto a2 = Load(d, pa + i + 2 * N);
//        const auto b2 = Load(d, pb + i + 2 * N);
//        sum[2] = MulAdd(a2, b2, sum[2]);
//        const auto a3 = Load(d, pa + i + 3 * N);
//        const auto b3 = Load(d, pb + i + 3 * N);
//        sum[3] = MulAdd(a3, b3, sum[3]);
//      }
//      // Reduction tree: sum of all accumulators by pairs into sum0.
//      sum[0] = Add(sum[0], sum[1]);
//      sum[2] = Add(sum[2], sum[3]);
//      sum[0] = Add(sum[0], sum[2]);
//      // Remember to store the result in `dot_` for verification; see `Verify`.
//      dot_ = ReduceSum(d, sum[0]);
//      // Return the result so that the benchmarking framework can ensure that the
//      // computation is not elided by the compiler.
//      return static_cast<FuncOutput>(dot_);
//    }
//
//    void Verify(size_t num_items) {
//      if (dot_ == -1.0f) {
//        fprintf(stderr, "Dot: must call Verify after benchmark");
//        abort();
//      }
//
//      const float expected = std::inner_product(a_.get(), a_.get() + num_items,
//                                                b_, 0.0f);
//      const float rel_err = std::abs(expected - dot_) / expected;
//      if (rel_err > 1.1E-6F) {
//        fprintf(stderr, "Dot: expected %e actual %e (%e)\n", expected, dot_, rel_err);
//        abort();
//      }
//    }
//
// private:
//    float dot_; // for verify
//};
//
//
//
//} // namespace HWY_NAMESPACE
//} // namespace hwy
//HWY_AFTER_NAMESPACE();


