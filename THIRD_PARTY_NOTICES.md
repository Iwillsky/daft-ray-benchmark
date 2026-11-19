# 第三方内容说明

以下内容来自第三方, 保留各自的许可证和归属:

| 内容 | 来源 | 许可证 | 说明 |
|---|---|---|---|
| `job/tpch_queries.py` 中的 TPC-H Q1-Q22 Daft 实现 | [Eventual-Inc/Daft](https://github.com/Eventual-Inc/Daft) `benchmarking/tpch/answers.py` | Apache-2.0 | 在原实现上做了少量修改 (q11 / q13 / q15 / q17 / q20), 文件头部逐条说明了改动。使用或再分发这部分代码时请遵守 Apache-2.0 条款 (保留版权声明并注明改动)。 |
| `codebuild/tpch_gen.py` 的数据布局 / 列类型 | 参照 Daft 官方 TPC-H benchmark 的 `data_generation.py` | Apache-2.0 | 只参照布局和 schema; 数据由 DuckDB 的 tpch 扩展生成。 |
| `job/tpch_answers_sf1.json`、`job/tpch_duckdb_queries.json` | [DuckDB](https://duckdb.org) tpch 扩展的 `tpch_answers()` / `tpch_queries()` | MIT (DuckDB) | 内容是 TPC-H 规范里的 SF1 标准答案和标准查询参数。 |
| Ray 基础镜像、KubeRay、kube-prometheus-stack、Grafana 等 | 各自的开源项目 | 各自的许可证 | 本仓库只提供构建 / 部署脚本, 不包含这些项目的源码或镜像。 |

**关于 TPC-H**: 本仓库中的测试是基于 TPC-H 规范的自定义实现, **不是 TPC 官方认证的结果**, 其结果不可与 TPC 官方发布的成绩比较。"TPC-H" 是 Transaction Processing Performance Council 的商标。
