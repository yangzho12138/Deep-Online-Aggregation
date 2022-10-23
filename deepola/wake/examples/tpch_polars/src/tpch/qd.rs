// TODO: You need to implement the query d.sql in this file.
use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	sum(l_extendedprice* (1 - l_discount)) as revenue
// from
// 	lineitem,
// 	part
// where
//     p_partkey = l_partkey
//     and l_shipinstruct = 'DELIVER IN PERSON'
//     and
//     (
//         (
//             p_brand = 'Brand#12'
//             and l_quantity >= 1 and l_quantity <= 1 + 10
//             and p_size between 1 and 5
//         )
//         or
//         (
//             p_brand = 'Brand#23'
//             and l_quantity >= 10 and l_quantity <= 10 + 10
//             and p_size between 1 and 10
//         )
//         or
//         (
//             p_brand = 'Brand#34'
//             and l_quantity >= 20 and l_quantity <= 20 + 10
//             and p_size between 1 and 15
//         )
//     )

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_partkey", "l_shipinstruct", "l_extendedprice", "l_quantity", "l_discount"],
        ),
        ("part".into(), vec!["p_partkey", "p_brand", "p_size"]),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);
    

    // WHERE Node
    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("l_shipinstruct").unwrap();
            let mask = a.equal("DELIVER IN PERSON").unwrap();
            let result = df.filter(&mask).unwrap();
            result
        })))
        .build();

    // HASH JOIN Node
    let lp_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();
    
    let lp_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let pb = df.column("p_brand").unwrap();
            let lq = df.column("l_quantity").unwrap();
            let ps = df.column("p_size").unwrap(); 
            let mask1 = pb.equal("Brand#12").unwrap() & lq.gt_eq(1).unwrap() & lq.lt_eq(11).unwrap() & ps.gt_eq(1).unwrap() & ps.lt_eq(5).unwrap();
            let mask2 = pb.equal("Brand#23").unwrap() & lq.gt_eq(10).unwrap() & lq.lt_eq(20).unwrap() & ps.gt_eq(1).unwrap() & ps.lt_eq(10).unwrap();
            let mask3 = pb.equal("Brand#34").unwrap() & lq.gt_eq(20).unwrap() & lq.lt_eq(30).unwrap() & ps.gt_eq(1).unwrap() & ps.lt_eq(15).unwrap();
            let result1 = df.filter(&mask1).unwrap();
            let result2 = df.filter(&mask2).unwrap();
            let result3 = df.filter(&mask3).unwrap();
            let tmp1 = result1.vstack(&result2).unwrap();
            let result = result3.vstack(&tmp1).unwrap();
            result
        })))
        .build();
    
    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![
                Series::new(
                    "revenue",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                ),
            ];
            df.hstack(&columns).unwrap()
        })))
        .build();

    // GROUP BY AGGREGATE Node
    let sum_accumulator = SumAccumulator::new();
    
    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let columns = vec![
            Series::new("revenue", df.column("revenue").unwrap()),
        ];
        DataFrame::new(columns)
            .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    lp_hash_join_node.subscribe_to_node(&lineitem_where_node, 0); // Left Node
    lp_hash_join_node.subscribe_to_node(&part_csvreader_node, 1); // Right Node
    lp_where_node.subscribe_to_node(&lp_hash_join_node, 0);
    expression_node.subscribe_to_node(&lp_where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(part_csvreader_node);
    service.add(lineitem_where_node);
    service.add(lp_hash_join_node);
    service.add(lp_where_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
