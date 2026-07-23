# Changelog

## [0.25.0](https://github.com/timescale/test-common/compare/v0.24.0...v0.25.0) (2026-07-20)


### Features

* add TS 2.27, 2.28, nightly and chunk_has_dropped_column ([ff37eb9](https://github.com/timescale/test-common/commit/ff37eb93776d5aeaf7c2a9ea7e29720315ec6220))


### Miscellaneous

* drop chunk_has_dropped_column helper ([737f877](https://github.com/timescale/test-common/commit/737f8774db26678f9935018f3b7c629b7ef10300))

## [0.24.0](https://github.com/timescale/test-common/compare/v0.23.1...v0.24.0) (2026-04-16)


### Features

* add PG18 support to PgVersion enum ([c541473](https://github.com/timescale/test-common/commit/c54147361ab6008adc7a8bbbb5bdea8dcd9d6e3e))

## [0.23.1](https://github.com/timescale/test-common/compare/v0.23.0...v0.23.1) (2026-03-27)


### Bug Fixes

* support timescale 2.22 ([f9dfe3d](https://github.com/timescale/test-common/commit/f9dfe3d1676889b70004f31cee59026cbe19983b))

## [0.23.0](https://github.com/timescale/test-common/compare/v0.22.0...v0.23.0) (2026-03-26)


### Features

* add support from 2.23 to 2.26 ([241ba7b](https://github.com/timescale/test-common/commit/241ba7be262548a3082b393c0044f36ef37bed8b))


### Miscellaneous

* dependabot ci ([d53a93d](https://github.com/timescale/test-common/commit/d53a93da8bf7c45ca39bb983a6b79386b8c55318))
* dependabot ci ([e220670](https://github.com/timescale/test-common/commit/e220670faa2f58af26f415bcb54578257b5d5ba2))
* **deps:** bump bytes from 1.5.0 to 1.11.1 ([8bfb0c8](https://github.com/timescale/test-common/commit/8bfb0c8e2639095158f1717ab9a5fa6d573be963))
* **deps:** bump tokio from 1.34.0 to 1.38.2 ([4c133ef](https://github.com/timescale/test-common/commit/4c133ef93ee4ba20065ef0a4ed5f7258e1f0e1b2))

## [0.22.0](https://github.com/timescale/test-common/compare/v0.21.0...v0.22.0) (2025-10-14)


### Features

* support timescale 2.22 ([0551844](https://github.com/timescale/test-common/commit/05518444b4592d3972edb0914363dde11f387f74))

## [0.20.0](https://github.com/timescale/test-common/compare/v0.19.0...v0.20.0) (2024-07-22)


### Features

* add enums for older tsdb versions(2.5 to 2.9) ([0ad226d](https://github.com/timescale/test-common/commit/0ad226d8f432ec49d1652a0105dbbe682481d54d))
* add must_true/must_false helpers ([d33f153](https://github.com/timescale/test-common/commit/d33f1536d6d3385b3a56ddebe4d8a017be797c5d))


### Bug Fixes

* has_table_count must escape table names to support case-sensitive tables ([b61343d](https://github.com/timescale/test-common/commit/b61343d6434ff9fb0a017027c9f5e9bdc41a3fa0))

## [0.19.0](https://github.com/timescale/test-common/compare/v0.18.0...v0.19.0) (2024-05-28)


### Features

* add has_user_defined_job ([0eab25a](https://github.com/timescale/test-common/commit/0eab25ae472f1043d2175da7867c9b6e664cd4b1))

## [0.18.0](https://github.com/timescale/test-common/compare/v0.17.0...v0.18.0) (2024-05-16)


### Features

* add assert for TS uuid ([2446b3c](https://github.com/timescale/test-common/commit/2446b3cc8a87f9025e353d8e8c21e11ad0111727))

## [0.17.0](https://github.com/timescale/test-common/compare/v0.16.0...v0.17.0) (2024-05-14)


### Features

* add cloud config ([7598663](https://github.com/timescale/test-common/commit/7598663e48f1449fa138fb71c553f548120957aa))
* add helper to read ts and pg version from env ([7805745](https://github.com/timescale/test-common/commit/7805745b667d79ebb69341bf8a973921e839eb5f))

## [0.16.0](https://github.com/timescale/test-common/compare/v0.15.0...v0.16.0) (2024-04-23)


### ⚠ BREAKING CHANGES

* add timescaledb version to timescaledb helper

### Features

* add timescaledb version to timescaledb helper ([181ab3d](https://github.com/timescale/test-common/commit/181ab3d4bb10526051bdafd505f3570c2ef4c7d4))

## [0.15.0](https://github.com/timescale/test-common/compare/v0.14.0...v0.15.0) (2024-04-23)


### Features

* get internal connection string from container ([3d60517](https://github.com/timescale/test-common/commit/3d6051706b411a8e8c1e5608fd9754362d728b37))


### Miscellaneous

* **deps:** bump rustls from 0.21.9 to 0.21.11 ([211375c](https://github.com/timescale/test-common/commit/211375cead18ec2b661d22262a1a8fb4a4503062))
* **deps:** bump whoami from 1.4.1 to 1.5.1 ([bb7c48c](https://github.com/timescale/test-common/commit/bb7c48c35ca4d2d3ad703121d1dec4c36593c2f2))

## [0.14.0](https://github.com/timescale/test-common/compare/v0.13.1...v0.14.0) (2024-04-08)


### Features

* add pg16 ([894621e](https://github.com/timescale/test-common/commit/894621ea432f0cf81a5a26e3f84d79d770a1829e))

## [0.13.1](https://github.com/timescale/test-common/compare/v0.13.0...v0.13.1) (2024-04-02)


### Miscellaneous

* bump dependency 'mio' ([447fb35](https://github.com/timescale/test-common/commit/447fb35b9c027745da6ec5b36677109ebd561134))

## [0.13.0](https://github.com/timescale/test-common/compare/v0.12.0...v0.13.0) (2024-01-17)


### Features

* add asserts for constraints and indexes ([2c8da47](https://github.com/timescale/test-common/commit/2c8da4746dfcc26e4df282b6eb939b9df37edbbb))

## [0.12.0](https://github.com/timescale/test-common/compare/v0.11.0...v0.12.0) (2024-01-17)


### Features

* add 'has_index_count' ([0caeb9a](https://github.com/timescale/test-common/commit/0caeb9a4d904d18dcbd4c8089688bba37efede8b))

## [0.11.0](https://github.com/timescale/test-common/compare/v0.10.0...v0.11.0) (2024-01-10)


### Features

* add has_fk_constraint_named ([2096f2c](https://github.com/timescale/test-common/commit/2096f2c2beda27ab6bbf2a660923a8e78442fbd8))

## [0.10.0](https://github.com/timescale/test-common/compare/v0.9.0...v0.10.0) (2023-12-13)


### Features

* **dbassert:** add has_hypertable and not_has_hypertable ([7732562](https://github.com/timescale/test-common/commit/77325622bff8d1be04b44c8fd7a6bbf155222b05))


### Bug Fixes

* replace anyhow with thiserror ([7dec980](https://github.com/timescale/test-common/commit/7dec9807d8a8a99e9851958e8f6e50a3fb6409f7))

## [0.9.0](https://github.com/timescale/test-common/compare/v0.8.0...v0.9.0) (2023-12-07)


### Features

* validate hypertables schemas match ([db50b46](https://github.com/timescale/test-common/commit/db50b460f1abb560fe77d0fd0c18105d58897e00))


### Miscellaneous

* clean up test-common ([77a6716](https://github.com/timescale/test-common/commit/77a67160a0d9e2d7da2036ff9e172f987f7ec9bb))
* fix build scripts ([c1d3edc](https://github.com/timescale/test-common/commit/c1d3edc64d15229a337479192ff707e5bfb95cb7))
* ignore target ([0f5184e](https://github.com/timescale/test-common/commit/0f5184e877b9dd94341693d354bd2e7c839ba422))
