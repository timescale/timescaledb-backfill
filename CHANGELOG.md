# Changelog

## [0.9.1](https://github.com/timescale/timescaledb-backfill/compare/v0.9.0...v0.9.1) (2024-04-02)


### Bug Fixes

* column mismatch error due to ordinal position ([7570bde](https://github.com/timescale/timescaledb-backfill/commit/7570bde1546947e7062c0e13482428ad05e878b1))


### Miscellaneous

* bump deps ([6d8c2b3](https://github.com/timescale/timescaledb-backfill/commit/6d8c2b38a1de42c8e948672716e16b955a4abab0))

## [0.9.0](https://github.com/timescale/timescaledb-backfill/compare/v0.8.0...v0.9.0) (2024-03-12)


### Features

* abort on mismatching timescaledb versions ([4b17bba](https://github.com/timescale/timescaledb-backfill/commit/4b17bba61407d2d287bf12c2691c071d5c87efbf))
* report telemetry to new telemetry backend ([c187cef](https://github.com/timescale/timescaledb-backfill/commit/c187cef1983d1ea198c2c519f9fd977c4671270f))
* validate hypertables schemas match ([78f9645](https://github.com/timescale/timescaledb-backfill/commit/78f9645ac2ea32d62e2361e975df49e6e6b18882))


### Bug Fixes

* copy partial chunks without modifying _timescaledb_catalog.chunk ([e41791b](https://github.com/timescale/timescaledb-backfill/commit/e41791be0340e2d05cde9bef856b4ce49f557ded)), closes [#149](https://github.com/timescale/timescaledb-backfill/issues/149)


### Miscellaneous

* abort on timescaledb &gt;= 2.14.0 ([0215503](https://github.com/timescale/timescaledb-backfill/commit/021550394fe628887f3bc51119a608f13f5b2078))
* checkout dependencies with org automation token ([64a4df9](https://github.com/timescale/timescaledb-backfill/commit/64a4df9d581c7b48358da38e0d3d2ccc54430fa4))
* clean up test-common ([0486d5e](https://github.com/timescale/timescaledb-backfill/commit/0486d5e02a988de380c7b70f260e5f964f40971e))
* configure cloud-like environment for tests ([94f1a51](https://github.com/timescale/timescaledb-backfill/commit/94f1a511cebfe124959c32bb77e9037d1c7e6f52))
* **deps:** bump rustix from 0.38.14 to 0.38.19 ([9dc0bc9](https://github.com/timescale/timescaledb-backfill/commit/9dc0bc9148b4b9c32e844cefaebb312a6e4a1070))
* disable timescaledb telemetry in tests ([c359bad](https://github.com/timescale/timescaledb-backfill/commit/c359badf7034f5d9750cf41b44929339336c6754))
* drop PG 12 tests ([ee6e5bc](https://github.com/timescale/timescaledb-backfill/commit/ee6e5bc0a8b0d35cabb3ae3140fe995adde25a15))
* hide pg_dump output from test logs ([76627ae](https://github.com/timescale/timescaledb-backfill/commit/76627aee5913f922c1d6a6a674c863745aa68ebe))
* switch to timescale/test-common ([e5fe399](https://github.com/timescale/timescaledb-backfill/commit/e5fe399123c9381b1c2921533f4444ddea78a727))

## [0.8.0](https://github.com/timescale/timescaledb-backfill/compare/v0.7.0...v0.8.0) (2023-10-05)


### Features

* add usage to --help flag ([6e0d0a3](https://github.com/timescale/timescaledb-backfill/commit/6e0d0a3d153c4d87f51f98b108a0a4ab067788c8))


### Bug Fixes

* --from including extra chunks ([d96083b](https://github.com/timescale/timescaledb-backfill/commit/d96083bb08552a0f679547a1056ac21b59be0a95))

## [0.7.0](https://github.com/timescale/timescaledb-backfill/compare/v0.6.0...v0.7.0) (2023-10-03)


### Features

* add --from flag to stage command ([5aed2fb](https://github.com/timescale/timescaledb-backfill/commit/5aed2fb500c3013486ef37f2cc8535817c72410f))
* improve help documentation ([5d7b53b](https://github.com/timescale/timescaledb-backfill/commit/5d7b53bdbccbaf5f89c570f3f70fcdc5dbad1dd5))
* improve help documentation ([f1b64c0](https://github.com/timescale/timescaledb-backfill/commit/f1b64c07e1448bbd88b0daafaee1de962fa97cb0))


### Bug Fixes

* verify fails with 'permission denied for schema pg_toast' ([01ead6f](https://github.com/timescale/timescaledb-backfill/commit/01ead6f227ef6a0db979d479e41f484824398497))

## [0.6.0](https://github.com/timescale/timescaledb-backfill/compare/v0.5.0...v0.6.0) (2023-09-28)


### Features

* set application_name on db connections ([6f8fbf4](https://github.com/timescale/timescaledb-backfill/commit/6f8fbf460b0a707f3ea71d49db02aed9ca8da9d8))


### Bug Fixes

* proc schema query support for function in multiple schemas ([a189271](https://github.com/timescale/timescaledb-backfill/commit/a1892711ae180a81b8096b2c20d22c324e8c0743))


### Miscellaneous

* test caggs refresh with 3 level hierarchy ([58f3e17](https://github.com/timescale/timescaledb-backfill/commit/58f3e173eabc18361c8cc7965d95f293818a26fc))

## [0.5.0](https://github.com/timescale/timescaledb-backfill/compare/v0.4.1...v0.5.0) (2023-09-25)


### Features

* add cascade to caggs-refresh ([623b02d](https://github.com/timescale/timescaledb-backfill/commit/623b02dfa14c220957e3a64580a57031e2e85811))
* add filter to caggs-refresh ([c6dffa9](https://github.com/timescale/timescaledb-backfill/commit/c6dffa914be2ddca32a8154f592f6d69e48ad871))
* add refresh-caggs subcommand ([4938627](https://github.com/timescale/timescaledb-backfill/commit/4938627c9c06a99fd62859d6d6940279cd466468))
* telemetry ([243f671](https://github.com/timescale/timescaledb-backfill/commit/243f6713963b07d790c45574dc0d705e227dda58))


### Miscellaneous

* update dependencies ([af36076](https://github.com/timescale/timescaledb-backfill/commit/af36076d5d8282f617d9b861d6c17cd26f8136d0))

## [0.4.1](https://github.com/timescale/timescaledb-backfill/compare/v0.4.0...v0.4.1) (2023-09-13)


### Miscellaneous

* upload releases to assets.timescale.com ([384c704](https://github.com/timescale/timescaledb-backfill/commit/384c7045c4d1d241c4eb1b6124ca32fc52f80ee7))

## [0.4.0](https://github.com/timescale/timescaledb-backfill/compare/v0.3.0...v0.4.0) (2023-09-12)


### Features

* --filter arg is now verified to be valid regexp ([bcb7b1c](https://github.com/timescale/timescaledb-backfill/commit/bcb7b1cb34add552a123b2cae04c9d59ded64a38))
* --until argument is now verified to be valid bigint or datetime ([887cb70](https://github.com/timescale/timescaledb-backfill/commit/887cb703480e0914f1f23c9b2f202d83e77dbd2b))
* filter can match ht and caggs and cascade to/from ht/caggs ([5da5e81](https://github.com/timescale/timescaledb-backfill/commit/5da5e819ace1384c87865ee02dc6df19f42daff6))
* support timescale 2.12 new functions schema ([6b3875f](https://github.com/timescale/timescaledb-backfill/commit/6b3875fbcfe3aac874bbca1efeec7aae8a6f7b27))


### Bug Fixes

* remove tzrange as valid time dimension ([cc91a56](https://github.com/timescale/timescaledb-backfill/commit/cc91a5664d7082d18f9722570c805b47ad3b252c))
* use updated fork of tokio-postgres-rustls ([18d0d39](https://github.com/timescale/timescaledb-backfill/commit/18d0d39b937c050afeb4796e9c2b29c8b80b62c7))


### Miscellaneous

* add --filter test ([ffd344b](https://github.com/timescale/timescaledb-backfill/commit/ffd344b19b7185c7f276d0e94ea1db61f885f118))
* change release-please to github-token ([3c48a8d](https://github.com/timescale/timescaledb-backfill/commit/3c48a8d676402028025b11018cd53b7b42539add))
* skip double_ctrl_c_stops_hard on macos ([0af9515](https://github.com/timescale/timescaledb-backfill/commit/0af9515947208a1cc3f97ec1989e232b78cdfe50))

## [0.3.0](https://github.com/timescale/timescaledb-backfill/compare/v0.2.2...v0.3.0) (2023-08-28)


### Features

* add verify command to compare chunks ([86c9e66](https://github.com/timescale/timescaledb-backfill/commit/86c9e662ba521464b69d09ba78f11d37dd8738db))


### Bug Fixes

* do not cast non-existent relations to regclass ([243afe1](https://github.com/timescale/timescaledb-backfill/commit/243afe1cc5d18c9eb66ac6b53833b2af2bd6e96d))
* do not stage chunks marked as dropped ([a363f00](https://github.com/timescale/timescaledb-backfill/commit/a363f0000293a7d51375fd1b8477a65ef81e8d4b))
* multiple stage runs duplicates the tasks ([801aebe](https://github.com/timescale/timescaledb-backfill/commit/801aebe0f61eaf9ca0f47792b48159a195a63b1b))
* update src/main.rs ([86c9e66](https://github.com/timescale/timescaledb-backfill/commit/86c9e662ba521464b69d09ba78f11d37dd8738db))
* update src/verify.rs ([86c9e66](https://github.com/timescale/timescaledb-backfill/commit/86c9e662ba521464b69d09ba78f11d37dd8738db))


### Miscellaneous

* bump to 1.72.0 rust compiler in CI ([e8b2a72](https://github.com/timescale/timescaledb-backfill/commit/e8b2a72229b92f808f52324b3d223752387ab20e))
* optimise binary and symbols size ([3cb3c3b](https://github.com/timescale/timescaledb-backfill/commit/3cb3c3bb73a09808bd5ecd5b2dc4c5f3a45c6c90))

## [0.2.2](https://github.com/timescale/timescaledb-backfill/compare/v0.2.1...v0.2.2) (2023-08-24)


### Bug Fixes

* skip dropped chunks on a hypertable with continuous aggregate ([4612ed7](https://github.com/timescale/timescaledb-backfill/commit/4612ed7ecb4d917db53d85c5aac6454fdeb389b0))

## [0.2.1](https://github.com/timescale/timescaledb-backfill/compare/v0.2.0...v0.2.1) (2023-08-23)


### Bug Fixes

* ignore task when source chunk doesn't exists ([#83](https://github.com/timescale/timescaledb-backfill/issues/83)) ([b3ea19f](https://github.com/timescale/timescaledb-backfill/commit/b3ea19f9f6eee1c46fd7564061a37063aa31cec0))


### Miscellaneous

* remove cargo config ([afce7f2](https://github.com/timescale/timescaledb-backfill/commit/afce7f20768a5cf6a3d2f90ddb3f7050baea23c0))
* update dependencies ([e2ca690](https://github.com/timescale/timescaledb-backfill/commit/e2ca6908b93a51b7e9e9373a42dcff2d4041ae69))

## [0.2.0](https://github.com/timescale/timescaledb-backfill/compare/v0.1.2...v0.2.0) (2023-08-22)


### Features

* make --until mandatory ([ca9038e](https://github.com/timescale/timescaledb-backfill/commit/ca9038ee998fd06162ff223d64b636871477a540))


### Bug Fixes

* ensure correct chunk status ([964de48](https://github.com/timescale/timescaledb-backfill/commit/964de48c8a617842e785f98056f130737ff055d5))
* recompress when 'until' falls within a compressed chunk ([cf0ebe7](https://github.com/timescale/timescaledb-backfill/commit/cf0ebe7dfc065ca031fadd408fb4c65875aa91e0)), closes [#69](https://github.com/timescale/timescaledb-backfill/issues/69)
* support pg12 ([a9c1885](https://github.com/timescale/timescaledb-backfill/commit/a9c188537ade3ccd757645ce2070c9487b58e8a3))


### Miscellaneous

* run test suite against pg12 through pg15 ([0b92f19](https://github.com/timescale/timescaledb-backfill/commit/0b92f199869f85f7e98f6e85e01700c0e5e46a8c))

## [0.1.2](https://github.com/timescale/timescaledb-backfill/compare/v0.1.1...v0.1.2) (2023-08-21)


### Bug Fixes

* make sslmode=require work ([fec3902](https://github.com/timescale/timescaledb-backfill/commit/fec3902909cc05c34e13aafebfcfe61370124607)), closes [#41](https://github.com/timescale/timescaledb-backfill/issues/41)


### Miscellaneous

* put compiled binary in gzipped tar archive ([e803a0c](https://github.com/timescale/timescaledb-backfill/commit/e803a0cb97a974afa3fe74be0f8de923233b4332))

## [0.1.1](https://github.com/timescale/timescaledb-backfill/compare/v0.1.0...v0.1.1) (2023-08-17)


### Bug Fixes

* install build dependencies ([86be373](https://github.com/timescale/timescaledb-backfill/commit/86be37333c6efce60df0c7699d098ee7476c929c))

## 0.1.0 (2023-08-17)


### Features

* abort when hypertable setup not supported ([b28c1a0](https://github.com/timescale/timescaledb-backfill/commit/b28c1a06ef40e6e708fab487b5d095fae14decc7))
* abort when hypertable setup not supported ([16b9700](https://github.com/timescale/timescaledb-backfill/commit/16b97008a85e1a84ba2d74dde6a35bddce62eaa4))
* add clean clean ([fecbc9c](https://github.com/timescale/timescaledb-backfill/commit/fecbc9c0716fb1379352d28068deb7dd8bca07bc))
* add copy subcommand ([3b8276a](https://github.com/timescale/timescaledb-backfill/commit/3b8276a57902b509a7657d00fc8974ba8ba89320))
* add parallelism ([6cf7e07](https://github.com/timescale/timescaledb-backfill/commit/6cf7e07d267bf4f317c4502fe1beef358b832835))
* add progress output ([b550ad5](https://github.com/timescale/timescaledb-backfill/commit/b550ad5f9d5421103edc5fc928128f426b10977e))
* assert tasks are available when running copy ([5020842](https://github.com/timescale/timescaledb-backfill/commit/5020842b530911f9dbfeb13b41a0ca34f5a8f104))
* build static binary ([f682efe](https://github.com/timescale/timescaledb-backfill/commit/f682efeb6070469c024f7b83657f5b72bb4c0791))
* create compressed chunk if it doesn't exist ([560ddb7](https://github.com/timescale/timescaledb-backfill/commit/560ddb7d6ae47440ef5b78c55ede1a00d33fdf15))
* create uncompressed chunk if it does not exists ([9d51359](https://github.com/timescale/timescaledb-backfill/commit/9d51359a48792ec574af3407d639abdbc8d8906d))
* handle active chunk data deletion ([22d7eff](https://github.com/timescale/timescaledb-backfill/commit/22d7effe020e01ddd6857cc8f85676bd8cc4a5f7))
* handle ctrl-c ([40dab0e](https://github.com/timescale/timescaledb-backfill/commit/40dab0eb3538bfd750f7f9650dbf859dce8f58ee))
* manual integration test scripts ([ac645b0](https://github.com/timescale/timescaledb-backfill/commit/ac645b05de4464b59e5dc0d0c3e96015f41b8d6a))
* pull tasks from the queue ([d81d86b](https://github.com/timescale/timescaledb-backfill/commit/d81d86b0f3517272d2875e958bc7d911f782f5d1))
* read the max_identifier_length  from the DB ([850f068](https://github.com/timescale/timescaledb-backfill/commit/850f068e05d4b8f98590db8aef014ed6311b3727))
* show per-chunk copy throughput ([528b313](https://github.com/timescale/timescaledb-backfill/commit/528b313159d8bebc716467b943a5eeda962122a7))
* suppress cagg hypertable invalidation ([0a97ecc](https://github.com/timescale/timescaledb-backfill/commit/0a97eccf611bd1ebb0bcf03512f3790fc900d1c8))
* work queue in database table ([91d7287](https://github.com/timescale/timescaledb-backfill/commit/91d7287d18123ad57c371148fc2a437fcd465969))


### Bug Fixes

* bytes copied output and broken tests ([bed0ca4](https://github.com/timescale/timescaledb-backfill/commit/bed0ca4e5b9de3ddcec8cc707b8603095131ae27))
* correctly copy data when 'until' falls on compressed chunk ([4bb223d](https://github.com/timescale/timescaledb-backfill/commit/4bb223dc5731a07730914142219fde4f4a327fab)), closes [#49](https://github.com/timescale/timescaledb-backfill/issues/49)
* do not overfill copy buffer ([f4fe10a](https://github.com/timescale/timescaledb-backfill/commit/f4fe10aebede8f5e0bd587c13c5cfa231db1558c))
* exclude pg_toast when querying triggers ([688681e](https://github.com/timescale/timescaledb-backfill/commit/688681e66632dc769193b6ea08e064e2445ca732))
* improve copy performance ([e097d94](https://github.com/timescale/timescaledb-backfill/commit/e097d94df15254ef843eb5d8cc52777822035df5))
* use prepared insert for chunk staging ([b85eb90](https://github.com/timescale/timescaledb-backfill/commit/b85eb903866e83525b104ed3d5d1e147c7ce523d))


### Miscellaneous

* add CI ([843c636](https://github.com/timescale/timescaledb-backfill/commit/843c6361f50acf01eba2a6786e55288869a9f72d))
* add Makefile ([9a6af2a](https://github.com/timescale/timescaledb-backfill/commit/9a6af2a7a17b1df4249f8e11827281ae3b97c230))
* add README.md ([7851e7d](https://github.com/timescale/timescaledb-backfill/commit/7851e7d9a1cfa7056cd30c41c0d5f1c0934bf627))
* add release-please and upload artifacts ([4b7135c](https://github.com/timescale/timescaledb-backfill/commit/4b7135c3e8c3e4d73d8651b35819aa2d99fdbc1b))
* add simple test ([c729340](https://github.com/timescale/timescaledb-backfill/commit/c7293400df8ab7742d962d9c411127b30d415390))
* add test for active chunk row deletion ([e7830ee](https://github.com/timescale/timescaledb-backfill/commit/e7830ee9a0ef2618cbd47713b550f9f891e3ab14))
* add test for compressed chunk ([93df20e](https://github.com/timescale/timescaledb-backfill/commit/93df20ee1df3e176201212326534c05aec5205f1))
* add tests for bigint and space partitioning ([72efca7](https://github.com/timescale/timescaledb-backfill/commit/72efca75c52acdbff7c62848590a8785c153e1ee))
* add tests for ctrl-c ([0c7a6a7](https://github.com/timescale/timescaledb-backfill/commit/0c7a6a716a40e1586132277391250129ad1380e3))
* de-flake ctrl-c tests ([0ec1c8b](https://github.com/timescale/timescaledb-backfill/commit/0ec1c8b03b3511bddf4b3f863e56d8939ed0ef6a))
* enable tests ([c3ca181](https://github.com/timescale/timescaledb-backfill/commit/c3ca181b44e43bb3e6da99d6237703fe7b3e237e))
* expose info about '--filter' param in '--help' ([b699a35](https://github.com/timescale/timescaledb-backfill/commit/b699a351c18a341d0a625a603b1b20d4feeebe96))
* format code ([af28859](https://github.com/timescale/timescaledb-backfill/commit/af288593992e754ce846a436a549289422e1643e))
* refactor ([d3f5d74](https://github.com/timescale/timescaledb-backfill/commit/d3f5d7467fae25be94453899bf313ebec5bba486))
* refactor tests ([e104b71](https://github.com/timescale/timescaledb-backfill/commit/e104b715d4c3a6661b47607a8197ff9952a64d05))
* remove unnecesary lifetime specifier ([bcc9b79](https://github.com/timescale/timescaledb-backfill/commit/bcc9b7998dd1ad98eef1a426bdcb4b57a2a4e217))
* remove unused pg_query ([fbcd6c4](https://github.com/timescale/timescaledb-backfill/commit/fbcd6c4fa1fffe3a5a73ad3ec0ad8bf55f951117))

## Changelog
