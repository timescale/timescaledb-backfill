# Changelog

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
