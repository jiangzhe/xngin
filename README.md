# XNGIN

![build](https://github.com/jiangzhe/xngin/actions/workflows/build.yml/badge.svg)
![codecov](https://codecov.io/gh/jiangzhe/xngin/branch/main/graph/badge.svg?token=T3RMZE2998)

Xngin(pronounced "X Engine") is a personal project to build a SQL engine from scratch.

The project name is inspired by [Nginx](https://en.wikipedia.org/wiki/Nginx), which is a
very popular web server with high performance and ease to use. 

## Goal

1. Fast.
2. Easy to use.
3. Distributed.

## Non-Goal

Transactional management.

## Development Plan

There are lots of things to do. Just list some as below.

| Functionality | Status |
|---------------|--------|
| Frontend AST Definition | Done |
| Frontend AST Parse | Done |
| Frontend AST Format | Done |
| Logical IR Definition | In progress |
| Logical IR Rewrite   | In progress |
| Logical Rule-based Optimization | In progress |
| Catalog Definition | Demo |
| Catalog Maintain | Todo |
| Statistics Definition | Todo |
| Statistics Maintain | Todo |
| Optimizer Framework | Todo |
| Cost Model | Todo |
| Optimizer implementation | Todo |
| Plan Cache | Todo |
| Physical Plan Definition | Todo |
| Execution Framework | Todo |
| In-Memory Data Format | Demo |
| Physical Operators | Todo |
| Client Protocol | Todo |
| Internal Network Protocol | Todo |
| Index Framework | Todo |
| Index Implementation | Todo |
| Backend Storage | Todo |
| Backend Operators | Todo |
| Backend Adaptor | Todo |
| Data Exporter and Importer | Todo |

Current focus is on SQL interface and optimizer framework.

## License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

at your option.
