"""TPC-H Q1-Q10 的 Daft 实现。

来源: Eventual-Inc/Daft  benchmarking/tpch/answers.py (main 分支, 2026-10-20 取得), Apache-2.0 许可。
Daft 官方 TPC-H benchmark 用的就是这些查询 (https://docs.getdaft.io/en/stable/benchmarks/ : "TPC-H queries for Daft were written by us"),
q1-q22 原样保留。列名为大写, 对应 tpch_gen.py 生成的数据。
与官方版本的唯一改动: q11 增加参数 sf, 阈值取规范规定的 0.0001 / SF (官方实现写死成 0.0001, 只在 SF1 下正确)。
注意: Daft 源码自己在 q21 里注释"distinct not needed?", 其 DataFrame 版本可能与规范结果不一致, 需要用 SF1 官方答案逐题验证。
SF1 实测 (Daft 0.7.26, 对照 TPC-H 官方标准答案): 官方原样的 q1-q12、q14、q16-q22 共 20 题通过, q13、q15 有问题, 改动如下:
  - q13: 官方用 Expression.match, Daft 0.7.26 里没有这个方法, 改用等价的 .regexp
  - q15: 官方按浮点相等 join 最大值, 因并行求和顺序不同在 SF1 上得到 0 行 (应为 1 行), 改成相对误差 < 1e-9 的比较
  - q11: 阈值按规范取 0.0001/SF (见上)
newtest 追加的两处修正 (目的: 让 Daft 结果与 DuckDB 参考结果/官方答案在所有 SF 上一致):
  - q17: 官方 mean(0.2*l_quantity) 与规范 0.2*avg(l_quantity) 浮点舍入不同, 并列点上多/少几行 -> 改成规范写法
  - q20: 官方写法在 Ray 分布式 runner 上丢行 (SF1 得 2 行/应 186, SF100 得 740/应 17971) -> 改成等价的 semi join 写法
"""
from __future__ import annotations

import datetime
from typing import Callable

from daft import DataFrame, col, lit
from daft.functions import when

GetDFFunc = Callable[[str], DataFrame]


def q1(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")

    discounted_price = col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))
    taxed_discounted_price = discounted_price * (1 + col("L_TAX"))
    daft_df = (
        lineitem.where(col("L_SHIPDATE") <= datetime.date(1998, 9, 2))
        .groupby(col("L_RETURNFLAG"), col("L_LINESTATUS"))
        .agg(
            col("L_QUANTITY").sum().alias("sum_qty"),
            col("L_EXTENDEDPRICE").sum().alias("sum_base_price"),
            discounted_price.sum().alias("sum_disc_price"),
            taxed_discounted_price.sum().alias("sum_charge"),
            col("L_QUANTITY").mean().alias("avg_qty"),
            col("L_EXTENDEDPRICE").mean().alias("avg_price"),
            col("L_DISCOUNT").mean().alias("avg_disc"),
            col("L_QUANTITY").count().alias("count_order"),
        )
        .sort(["L_RETURNFLAG", "L_LINESTATUS"])
    )
    return daft_df


def q2(get_df: GetDFFunc) -> DataFrame:
    region = get_df("region")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    part = get_df("part")

    europe = (
        region.where(col("R_NAME") == "EUROPE")
        .join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(partsupp, left_on=col("S_SUPPKEY"), right_on=col("PS_SUPPKEY"))
    )

    brass = part.where((col("P_SIZE") == 15) & col("P_TYPE").endswith("BRASS")).join(
        europe,
        left_on=col("P_PARTKEY"),
        right_on=col("PS_PARTKEY"),
    )
    min_cost = brass.groupby(col("P_PARTKEY")).agg(col("PS_SUPPLYCOST").min().alias("min"))

    daft_df = (
        brass.join(min_cost, on=col("P_PARTKEY"))
        .where(col("PS_SUPPLYCOST") == col("min"))
        .select(
            col("S_ACCTBAL"),
            col("S_NAME"),
            col("N_NAME"),
            col("P_PARTKEY"),
            col("P_MFGR"),
            col("S_ADDRESS"),
            col("S_PHONE"),
            col("S_COMMENT"),
        )
        .sort(by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"], desc=[True, False, False, False])
        .limit(100)
    )
    return daft_df


def q3(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    customer = get_df("customer").where(col("C_MKTSEGMENT") == "BUILDING")
    orders = get_df("orders").where(col("O_ORDERDATE") < datetime.date(1995, 3, 15))
    lineitem = get_df("lineitem").where(col("L_SHIPDATE") > datetime.date(1995, 3, 15))

    daft_df = (
        customer.join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("O_ORDERKEY"), col("O_ORDERDATE"), col("O_SHIPPRIORITY"))
        .join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_ORDERKEY"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
            col("O_ORDERDATE"),
            col("O_SHIPPRIORITY"),
        )
        .groupby(col("O_ORDERKEY"), col("O_ORDERDATE"), col("O_SHIPPRIORITY"))
        .agg(col("volume").sum().alias("revenue"))
        .sort(by=["revenue", "O_ORDERDATE"], desc=[True, False])
        .limit(10)
        .select("O_ORDERKEY", "revenue", "O_ORDERDATE", "O_SHIPPRIORITY")
    )
    return daft_df


def q4(get_df: GetDFFunc) -> DataFrame:
    orders = get_df("orders")
    lineitems = get_df("lineitem")

    orders = orders.where(
        (col("O_ORDERDATE") >= datetime.date(1993, 7, 1)) & (col("O_ORDERDATE") < datetime.date(1993, 10, 1))
    )

    lineitems = lineitems.where(col("L_COMMITDATE") < col("L_RECEIPTDATE"))

    daft_df = (
        orders.join(lineitems, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"), how="semi")
        .groupby(col("O_ORDERPRIORITY"))
        .agg(col("O_ORDERKEY").count().alias("order_count"))
        .sort(col("O_ORDERPRIORITY"))
    )
    return daft_df


def q5(get_df: GetDFFunc) -> DataFrame:
    orders = get_df("orders").where(
        (col("O_ORDERDATE") >= datetime.date(1994, 1, 1)) & (col("O_ORDERDATE") < datetime.date(1995, 1, 1))
    )
    region = get_df("region").where(col("R_NAME") == "ASIA")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    daft_df = (
        region.join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(lineitem, left_on=col("S_SUPPKEY"), right_on=col("L_SUPPKEY"))
        .select(col("N_NAME"), col("L_EXTENDEDPRICE"), col("L_DISCOUNT"), col("L_ORDERKEY"), col("N_NATIONKEY"))
        .join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .join(customer, left_on=[col("O_CUSTKEY"), col("N_NATIONKEY")], right_on=[col("C_CUSTKEY"), col("C_NATIONKEY")])
        .select(col("N_NAME"), (col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("value"))
        .groupby(col("N_NAME"))
        .agg(col("value").sum().alias("revenue"))
        .sort(col("revenue"), desc=True)
    )
    return daft_df


def q6(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")
    daft_df = lineitem.where(
        (col("L_SHIPDATE") >= datetime.date(1994, 1, 1))
        & (col("L_SHIPDATE") < datetime.date(1995, 1, 1))
        & (col("L_DISCOUNT") >= 0.05)
        & (col("L_DISCOUNT") <= 0.07)
        & (col("L_QUANTITY") < 24)
    ).sum(col("L_EXTENDEDPRICE") * col("L_DISCOUNT"))
    return daft_df


def q7(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(
        (col("L_SHIPDATE") >= datetime.date(1995, 1, 1)) & (col("L_SHIPDATE") <= datetime.date(1996, 12, 31))
    )
    nation = get_df("nation").where((col("N_NAME") == "FRANCE") | (col("N_NAME") == "GERMANY"))
    supplier = get_df("supplier")
    customer = get_df("customer")
    orders = get_df("orders")

    supNation = (
        nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(lineitem, left_on=col("S_SUPPKEY"), right_on=col("L_SUPPKEY"))
        .select(
            col("N_NAME").alias("supp_nation"),
            col("L_ORDERKEY"),
            col("L_EXTENDEDPRICE"),
            col("L_DISCOUNT"),
            col("L_SHIPDATE"),
        )
    )

    daft_df = (
        nation.join(customer, left_on=col("N_NATIONKEY"), right_on=col("C_NATIONKEY"))
        .join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("N_NAME").alias("cust_nation"), col("O_ORDERKEY"))
        .join(supNation, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .where(
            ((col("supp_nation") == "FRANCE") & (col("cust_nation") == "GERMANY"))
            | ((col("supp_nation") == "GERMANY") & (col("cust_nation") == "FRANCE"))
        )
        .select(
            col("supp_nation"),
            col("cust_nation"),
            col("L_SHIPDATE").year().alias("l_year"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
        )
        .groupby(col("supp_nation"), col("cust_nation"), col("l_year"))
        .agg(col("volume").sum().alias("revenue"))
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )
    return daft_df


def q8(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    region = get_df("region").where(col("R_NAME") == "AMERICA")
    orders = get_df("orders").where(
        (col("O_ORDERDATE") <= datetime.date(1996, 12, 31)) & (col("O_ORDERDATE") >= datetime.date(1995, 1, 1))
    )
    part = get_df("part").where(col("P_TYPE") == "ECONOMY ANODIZED STEEL")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    nat = nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))

    line = (
        lineitem.select(
            col("L_PARTKEY"),
            col("L_SUPPKEY"),
            col("L_ORDERKEY"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
        )
        .join(part, left_on=col("L_PARTKEY"), right_on=col("P_PARTKEY"))
        .join(nat, left_on=col("L_SUPPKEY"), right_on=col("S_SUPPKEY"))
    )

    daft_df = (
        nation.join(region, left_on=col("N_REGIONKEY"), right_on=col("R_REGIONKEY"))
        .select(col("N_NATIONKEY"))
        .join(customer, left_on=col("N_NATIONKEY"), right_on=col("C_NATIONKEY"))
        .select(col("C_CUSTKEY"))
        .join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("O_ORDERKEY"), col("O_ORDERDATE"))
        .join(line, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_ORDERDATE").year().alias("o_year"),
            col("volume"),
            when(col("N_NAME") == "BRAZIL", col("volume")).otherwise(0.0).alias("case_volume"),
        )
        .groupby(col("o_year"))
        .agg(col("case_volume").sum().alias("case_volume_sum"), col("volume").sum().alias("volume_sum"))
        .select(col("o_year"), col("case_volume_sum") / col("volume_sum"))
        .sort(col("o_year"))
    )

    return daft_df


def q9(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")
    part = get_df("part")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    orders = get_df("orders")

    def expr(x, y, v, w):
        return x * (1 - y) - (v * w)

    linepart = part.where(col("P_NAME").contains("green")).join(
        lineitem, left_on=col("P_PARTKEY"), right_on=col("L_PARTKEY")
    )
    natsup = nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))

    daft_df = (
        linepart.join(natsup, left_on=col("L_SUPPKEY"), right_on=col("S_SUPPKEY"))
        .join(partsupp, left_on=[col("L_SUPPKEY"), col("P_PARTKEY")], right_on=[col("PS_SUPPKEY"), col("PS_PARTKEY")])
        .join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .select(
            col("N_NAME"),
            col("O_ORDERDATE").year().alias("o_year"),
            expr(col("L_EXTENDEDPRICE"), col("L_DISCOUNT"), col("PS_SUPPLYCOST"), col("L_QUANTITY")).alias("amount"),
        )
        .groupby(col("N_NAME"), col("o_year"))
        .agg(col("amount").sum())
        .sort(by=["N_NAME", "o_year"], desc=[False, True])
    )

    return daft_df


def q10(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(col("L_RETURNFLAG") == "R")
    orders = get_df("orders")
    nation = get_df("nation")
    customer = get_df("customer")

    daft_df = (
        orders.where(
            (col("O_ORDERDATE") < datetime.date(1994, 1, 1)) & (col("O_ORDERDATE") >= datetime.date(1993, 10, 1))
        )
        .join(customer, left_on=col("O_CUSTKEY"), right_on=col("C_CUSTKEY"))
        .join(nation, left_on=col("C_NATIONKEY"), right_on=col("N_NATIONKEY"))
        .join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_CUSTKEY"),
            col("C_NAME"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
            col("C_ACCTBAL"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_PHONE"),
            col("C_COMMENT"),
        )
        .groupby(
            col("O_CUSTKEY"),
            col("C_NAME"),
            col("C_ACCTBAL"),
            col("C_PHONE"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_COMMENT"),
        )
        .agg(col("volume").sum().alias("revenue"))
        .sort(col("revenue"), desc=True)
        .select(
            col("O_CUSTKEY"),
            col("C_NAME"),
            col("revenue"),
            col("C_ACCTBAL"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_PHONE"),
            col("C_COMMENT"),
        )
        .limit(20)
    )

    return daft_df


def q11(get_df: GetDFFunc, sf: float = 1.0) -> DataFrame:
    partsupp = get_df("partsupp")
    supplier = get_df("supplier")
    nation = get_df("nation")

    var_1 = "GERMANY"
    var_2 = 0.0001 / sf  # 规范: FRACTION = 0.0001 / SF (Daft 官方实现这里写死成 0.0001 / 1)

    res_1 = (
        partsupp.join(supplier, left_on=col("PS_SUPPKEY"), right_on=col("S_SUPPKEY"))
        .join(nation, left_on=col("S_NATIONKEY"), right_on=col("N_NATIONKEY"))
        .where(col("N_NAME") == var_1)
    )

    res_2 = res_1.agg((col("PS_SUPPLYCOST") * col("PS_AVAILQTY")).sum().alias("tmp")).select(
        col("tmp") * var_2, lit(1).alias("lit")
    )

    daft_df = (
        res_1.groupby("PS_PARTKEY")
        .agg(
            (col("PS_SUPPLYCOST") * col("PS_AVAILQTY")).sum().alias("value"),
        )
        .with_column("lit", lit(1))
        .join(res_2, on="lit")
        .where(col("value") > col("tmp"))
        .select(col("PS_PARTKEY"), col("value").round(2))
        .sort(col("value"), desc=True)
    )

    return daft_df


def q12(get_df: GetDFFunc) -> DataFrame:
    orders = get_df("orders")
    lineitem = get_df("lineitem")

    daft_df = (
        orders.join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .where(
            col("L_SHIPMODE").is_in(["MAIL", "SHIP"])
            & (col("L_COMMITDATE") < col("L_RECEIPTDATE"))
            & (col("L_SHIPDATE") < col("L_COMMITDATE"))
            & (col("L_RECEIPTDATE") >= datetime.date(1994, 1, 1))
            & (col("L_RECEIPTDATE") < datetime.date(1995, 1, 1))
        )
        .groupby(col("L_SHIPMODE"))
        .agg(
            when(col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"]), 1).otherwise(0).sum().alias("high_line_count"),
            when(~col("O_ORDERPRIORITY").is_in(["1-URGENT", "2-HIGH"]), 1).otherwise(0).sum().alias("low_line_count"),
        )
        .sort(col("L_SHIPMODE"))
    )

    return daft_df


def q13(get_df: GetDFFunc) -> DataFrame:
    customers = get_df("customer")
    orders = get_df("orders")

    daft_df = (
        customers.join(
            orders.where(~col("O_COMMENT").regexp(".*special.*requests.*")),  # 官方写 .match, Daft 0.7.26 里叫 .regexp
            left_on="C_CUSTKEY",
            right_on="O_CUSTKEY",
            how="left",
        )
        .groupby(col("C_CUSTKEY"))
        .agg(col("O_ORDERKEY").count().alias("c_count"))
        .sort("C_CUSTKEY")
        .groupby("c_count")
        .agg(col("c_count").count().alias("custdist"))
        .sort(["custdist", "c_count"], desc=[True, True])
    )

    return daft_df


def q14(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")
    part = get_df("part")

    daft_df = (
        lineitem.join(part, left_on=col("L_PARTKEY"), right_on=col("P_PARTKEY"))
        .where((col("L_SHIPDATE") >= datetime.date(1995, 9, 1)) & (col("L_SHIPDATE") < datetime.date(1995, 10, 1)))
        .agg(
            when(col("P_TYPE").startswith("PROMO"), col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).otherwise(0)
            .sum()
            .alias("tmp_1"),
            (col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).sum().alias("tmp_2"),
        )
        .select(100.00 * (col("tmp_1") / col("tmp_2")).alias("promo_revenue"))
    )

    return daft_df


def q15(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")

    revenue = (
        lineitem.where(
            (col("L_SHIPDATE") >= datetime.date(1996, 1, 1)) & (col("L_SHIPDATE") < datetime.date(1996, 4, 1))
        )
        .groupby(col("L_SUPPKEY"))
        .agg((col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).sum().alias("total_revenue"))
        .select(col("L_SUPPKEY").alias("supplier_no"), "total_revenue")
    )

    # 官方写法是按浮点数相等 join (revenue.join(revenue.max(...), on="total_revenue")), 但 revenue 在计划里被计算了两次,
    # 并行求和顺序不同会让最后几位浮点数不一样, 结果可能一行都匹配不上 (SF1 实测 0 行)。
    # 这里改成"与最大值的相对误差小于 1e-9"来取并列最大的行, 语义等价, 不依赖浮点位级一致。
    max_rev = revenue.max("total_revenue").select(col("total_revenue").alias("max_revenue")).with_column("lit", lit(1))
    revenue = (
        revenue.with_column("lit", lit(1))
        .join(max_rev, on="lit")
        .where(col("total_revenue") >= col("max_revenue") * (1 - 1e-9))
        .select("supplier_no", "total_revenue")
    )

    supplier = get_df("supplier")

    daft_df = (
        supplier.join(revenue, left_on=col("S_SUPPKEY"), right_on=col("supplier_no"))
        .select("S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "total_revenue")
        .sort("S_SUPPKEY")
    )

    return daft_df


def q16(get_df: GetDFFunc) -> DataFrame:
    part = get_df("part")
    partsupp = get_df("partsupp")

    supplier = get_df("supplier")

    suppkeys = supplier.where(col("S_COMMENT").regexp(".*Customer.*Complaints.*")).select(
        col("S_SUPPKEY"), col("S_SUPPKEY").alias("PS_SUPPKEY_RIGHT")
    )

    daft_df = (
        part.join(partsupp, left_on=col("P_PARTKEY"), right_on=col("PS_PARTKEY"))
        .where(
            (col("P_BRAND") != "Brand#45")
            & ~col("P_TYPE").startswith("MEDIUM POLISHED")
            & (col("P_SIZE").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        )
        .join(suppkeys, left_on="PS_SUPPKEY", right_on="S_SUPPKEY", how="left")
        .where(col("PS_SUPPKEY_RIGHT").is_null())
        .select("P_BRAND", "P_TYPE", "P_SIZE", "PS_SUPPKEY")
        .distinct()
        .groupby("P_BRAND", "P_TYPE", "P_SIZE")
        .agg(col("PS_SUPPKEY").count().alias("supplier_cnt"))
        .sort(["supplier_cnt", "P_BRAND", "P_TYPE", "P_SIZE"], desc=[True, False, False, False])
    )

    return daft_df


def q17(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")
    part = get_df("part")

    res_1 = part.where((col("P_BRAND") == "Brand#23") & (col("P_CONTAINER") == "MED BOX")).join(
        lineitem, left_on="P_PARTKEY", right_on="L_PARTKEY", how="left"
    )

    daft_df = (
        res_1.groupby("P_PARTKEY")
        # 规范写法是 l_quantity < 0.2 * avg(l_quantity): 先求平均再乘 0.2。
        # Daft 官方实现是 mean(0.2 * l_quantity), 数学上等价, 但浮点舍入不同, 在 l_quantity == 0.2 * avg 的并列点
        # (例如 avg=25 时 0.2*25 = 5.0, 而 mean(0.2*q) 可能是 4.999999...) 比较结果相反, 会多/少几行, 与 DuckDB / 官方答案对不上。
        .agg(col("L_QUANTITY").mean().alias("avg_quantity"))
        .select(col("P_PARTKEY").alias("key"), (0.2 * col("avg_quantity")).alias("avg_quantity"))
        .join(res_1, left_on="key", right_on="P_PARTKEY")
        .where(col("L_QUANTITY") < col("avg_quantity"))
        .agg((col("L_EXTENDEDPRICE") / 7.0).sum().alias("avg_yearly"))
    )

    return daft_df


def q18(get_df: GetDFFunc) -> DataFrame:
    customer = get_df("customer")
    orders = get_df("orders")
    lineitem = get_df("lineitem")

    res_1 = lineitem.groupby("L_ORDERKEY").agg(col("L_QUANTITY").sum().alias("sum_qty")).where(col("sum_qty") > 300)

    daft_df = (
        orders.join(res_1, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .join(customer, left_on=col("O_CUSTKEY"), right_on=col("C_CUSTKEY"))
        .join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .groupby("C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE")
        .agg(col("L_QUANTITY").sum().alias("sum"))
        .select("C_NAME", "C_CUSTKEY", "O_ORDERKEY", col("O_ORDERDATE").alias("O_ORDERDAT"), "O_TOTALPRICE", "sum")
        .sort(["O_TOTALPRICE", "O_ORDERDAT"], desc=[True, False])
        .limit(100)
    )

    return daft_df


def q19(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")
    part = get_df("part")

    daft_df = (
        lineitem.join(part, left_on=col("L_PARTKEY"), right_on=col("P_PARTKEY"))
        .where(
            (
                (col("P_BRAND") == "Brand#12")
                & col("P_CONTAINER").is_in(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])
                & (col("L_QUANTITY") >= 1)
                & (col("L_QUANTITY") <= 11)
                & (col("P_SIZE") >= 1)
                & (col("P_SIZE") <= 5)
                & col("L_SHIPMODE").is_in(["AIR", "AIR REG"])
                & (col("L_SHIPINSTRUCT") == "DELIVER IN PERSON")
            )
            | (
                (col("P_BRAND") == "Brand#23")
                & col("P_CONTAINER").is_in(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])
                & (col("L_QUANTITY") >= 10)
                & (col("L_QUANTITY") <= 20)
                & (col("P_SIZE") >= 1)
                & (col("P_SIZE") <= 10)
                & col("L_SHIPMODE").is_in(["AIR", "AIR REG"])
                & (col("L_SHIPINSTRUCT") == "DELIVER IN PERSON")
            )
            | (
                (col("P_BRAND") == "Brand#34")
                & col("P_CONTAINER").is_in(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])
                & (col("L_QUANTITY") >= 20)
                & (col("L_QUANTITY") <= 30)
                & (col("P_SIZE") >= 1)
                & (col("P_SIZE") <= 15)
                & col("L_SHIPMODE").is_in(["AIR", "AIR REG"])
                & (col("L_SHIPINSTRUCT") == "DELIVER IN PERSON")
            )
        )
        .agg((col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).sum().alias("revenue"))
    )

    return daft_df


def q20(get_df: GetDFFunc) -> DataFrame:
    # 规范: s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (forest 零件)
    #                      AND ps_availqty > 0.5 * sum(l_quantity of 1994 shipments for that part/supplier))
    # Daft 官方写法 (part.distinct -> join partsupp -> join lineitem 聚合 -> distinct suppkey -> join supplier) 在 Ray 分布式 runner 上
    # 会丢行: SF1 得 2 行 (应为 186), SF100 得 740 行 (应为 17971); 单机 native runner、SF1000 上却是对的, 与分区数/并行度有关。
    # 这里改成语义等价的 semi join 写法 (SF100 上与 DuckDB 参考结果的 17971 行一致), 不在中间结果上做 distinct+join。
    supplier = get_df("supplier")
    nation = get_df("nation")
    part = get_df("part")
    partsupp = get_df("partsupp")
    lineitem = get_df("lineitem")

    res_1 = (
        lineitem.where(
            (col("L_SHIPDATE") >= datetime.date(1994, 1, 1)) & (col("L_SHIPDATE") < datetime.date(1995, 1, 1))
        )
        .groupby("L_PARTKEY", "L_SUPPKEY")
        .agg(((col("L_QUANTITY") * 0.5).sum()).alias("sum_quantity"))
    )
    forest_parts = part.where(col("P_NAME").startswith("forest")).select("P_PARTKEY")
    candidate_suppliers = (
        partsupp.join(forest_parts, left_on="PS_PARTKEY", right_on="P_PARTKEY", how="semi")
        .join(res_1, left_on=["PS_PARTKEY", "PS_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"])
        .where(col("PS_AVAILQTY") > col("sum_quantity"))
        .select("PS_SUPPKEY")
        .distinct()
    )
    canada_suppliers = supplier.join(
        nation.where(col("N_NAME") == "CANADA"), left_on="S_NATIONKEY", right_on="N_NATIONKEY"
    )
    daft_df = (
        canada_suppliers.join(candidate_suppliers, left_on="S_SUPPKEY", right_on="PS_SUPPKEY", how="semi")
        .select("S_NAME", "S_ADDRESS")
        .sort("S_NAME")
    )
    return daft_df


def q21(get_df: GetDFFunc) -> DataFrame:
    supplier = get_df("supplier")
    nation = get_df("nation")
    lineitem = get_df("lineitem")
    orders = get_df("orders")

    res_1 = (
        lineitem.select("L_SUPPKEY", "L_ORDERKEY")
        # distinct not needed? either way we should stop using dataframe answers soon
        # .distinct()
        .groupby("L_ORDERKEY")
        .agg(col("L_SUPPKEY").count().alias("nunique_col"))
        .where(col("nunique_col") > 1)
        .join(lineitem.where(col("L_RECEIPTDATE") > col("L_COMMITDATE")), on="L_ORDERKEY")
    )

    daft_df = (
        res_1.select("L_SUPPKEY", "L_ORDERKEY")
        .groupby("L_ORDERKEY")
        .agg(col("L_SUPPKEY").count().alias("nunique_col"))
        .join(res_1, on="L_ORDERKEY")
        .join(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .join(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .where((col("nunique_col") == 1) & (col("N_NAME") == "SAUDI ARABIA") & (col("O_ORDERSTATUS") == "F"))
        .groupby("S_NAME")
        .agg(col("O_ORDERKEY").count().alias("numwait"))
        .sort(["numwait", "S_NAME"], desc=[True, False])
        .limit(100)
    )

    return daft_df


def q22(get_df: GetDFFunc) -> DataFrame:
    orders = get_df("orders")
    customer = get_df("customer")

    res_1 = (
        customer.with_column("cntrycode", col("C_PHONE").left(2))
        .where(col("cntrycode").is_in(["13", "31", "23", "29", "30", "18", "17"]))
        .select("C_ACCTBAL", "C_CUSTKEY", "cntrycode")
    )

    res_2 = (
        res_1.where(col("C_ACCTBAL") > 0).agg(col("C_ACCTBAL").mean().alias("avg_acctbal")).with_column("lit", lit(1))
    )

    daft_df = (
        res_1.join(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="anti")
        .with_column("lit", lit(1))
        .join(res_2, on="lit")
        .where(col("C_ACCTBAL") > col("avg_acctbal"))
        .groupby("cntrycode")
        .agg(
            col("C_ACCTBAL").count().alias("numcust"),
            col("C_ACCTBAL").sum().alias("totacctbal"),
        )
        .sort("cntrycode")
    )

    return daft_df
