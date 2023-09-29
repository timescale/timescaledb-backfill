// Copyright 2023 Timescale, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Assert that two numbers are within tolerance percent of one another
///
/// Example:
/// ```rust
///     # #[macro_use] extern crate test_common;
///     # fn main() {
///         // Asserts that 90 and 100 are within 10% of one another
///         assert_within!(100, 90, 0.10);
///     # }
/// ```
#[macro_export]
macro_rules! assert_within {
    ($a:expr, $b:expr, $tolerance:expr) => {
        let min = f64::min($a as f64, $b as f64);
        let max = f64::max($a as f64, $b as f64);

        let actual_ratio = 1f64 - (min / max);

        assert!(
            actual_ratio < $tolerance,
            "{} and {} are not within {} of one another (actually {:.2})",
            $a,
            $b,
            $tolerance,
            actual_ratio
        );
    };
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic(expected = "90 and 100 are not within 0.01 of one another (actually 0.10)")]
    fn panics_when_not_in_range() {
        assert_within!(90, 100, 0.01);
    }

    #[test]
    #[should_panic(expected = "100 and 90 are not within 0.01 of one another (actually 0.10)")]
    fn panics_when_not_in_range_order_independent() {
        assert_within!(100, 90, 0.01);
    }

    #[test]
    fn no_panic_when_in_range() {
        assert_within!(100, 90, 0.10);
    }
}
