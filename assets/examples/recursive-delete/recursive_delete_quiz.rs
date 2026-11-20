use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;

use differential_dataflow::input::Input;
use differential_dataflow::operators::Iterate;
use timely::dataflow::operators::probe::Handle;
use timely::order::Product;

fn trace_event(counter: &Rc<Cell<usize>>, message: impl std::fmt::Display) {
    if std::env::var_os("DD_TRACE_ARRIVALS").is_some() {
        let sequence = counter.get() + 1;
        counter.set(sequence);
        println!("RAW {sequence:02}: {message}");
    }
}

fn settle(worker: &mut timely::worker::Worker, mut done: impl FnMut() -> bool) {
    for _ in 0..100_000 {
        if done() { return; }
        worker.step();
    }
    panic!("progress did not reach the expected state");
}

fn main() {
    timely::execute_directly(|worker| {
        let sequence = Rc::new(Cell::new(0));
        let entry_updates = Rc::new(RefCell::new(Vec::<(u64, Product<u64, u64>, isize)>::new()));
        let join_updates = Rc::new(RefCell::new(Vec::<(u64, Product<u64, u64>, isize)>::new()));
        let distinct_updates = Rc::new(RefCell::new(Vec::<(u64, Product<u64, u64>, isize)>::new()));
        let outer_updates = Rc::new(RefCell::new(Vec::<(u64, u64, isize)>::new()));
        let (mut seed_input, mut edge_input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (seed_input, seeds) = scope.new_collection::<u64, isize>();
            let (edge_input, edges) = scope.new_collection::<(u64,u64), isize>();
            let inside = Rc::clone(&entry_updates);
            let joined = Rc::clone(&join_updates);
            let distincted = Rc::clone(&distinct_updates);
            let outside = Rc::clone(&outer_updates);
            let entry_sequence = Rc::clone(&sequence);
            let join_sequence = Rc::clone(&sequence);
            let distinct_sequence = Rc::clone(&sequence);
            let outside_sequence = Rc::clone(&sequence);
            let output = seeds.clone().iterate(|inner, known| {
                // Fix the observation point: the recursive variable at the loop entry.
                // Its iteration 0 contains only the seed; feedback advances iteration by 1.
                let known = known.inspect(move |update| {
                    trace_event(&entry_sequence, format_args!("ENTRY {update:?}"));
                    inside.borrow_mut().push(*update);
                });
                known.map(|node| (node, ()))
                    .join_map(edges.enter(inner), |_, _, destination| *destination)
                    .inspect(move |update| {
                        trace_event(&join_sequence, format_args!("JOIN {update:?}"));
                        joined.borrow_mut().push(*update);
                    })
                    .concat(seeds.enter(inner))
                    .distinct()
                    .inspect(move |update| {
                        trace_event(&distinct_sequence, format_args!("DISTINCT {update:?}"));
                        distincted.borrow_mut().push(*update);
                    })
            });
            let probe = Handle::new();
            output.filter(|node| *node != 0)
                .inspect(move |update| {
                    trace_event(&outside_sequence, format_args!("OUTER {update:?}"));
                    outside.borrow_mut().push(*update);
                })
                .probe_with(&probe);
            (seed_input, edge_input, probe)
        });

        // 0=P, 1=甲, 2=乙. Both nodes have a direct seed edge initially.
        trace_event(&sequence, "INPUT: insert seed P and four initial edges");
        seed_input.insert(0);
        seed_input.close();
        for edge in [(0,1), (0,2), (1,2), (2,1)] { edge_input.insert(edge); }
        edge_input.advance_to(1);
        edge_input.flush();
        settle(worker, || !probe.less_equal(&0));

        trace_event(&sequence, "INPUT: epoch 0 complete; remove P -> alpha at epoch 1");
        edge_input.remove((0,1));
        edge_input.advance_to(2);
        edge_input.flush();
        settle(worker, || !probe.less_equal(&1));
        trace_event(&sequence, "PROGRESS: epoch 1 complete");

        let mut delta_at_entry = BTreeMap::<(u64,u64),isize>::new();
        for (node, time, diff) in entry_updates.borrow().iter() {
            if time.outer == 1 { *delta_at_entry.entry((*node, time.inner)).or_default() += diff; }
        }
        delta_at_entry.retain(|_, diff| *diff != 0);
        assert_eq!(delta_at_entry, BTreeMap::from([((1,1),-1), ((1,2),1)]));
        println!("Observation: loop entry; timestamp=(input version, iteration), 0=P, 1=甲, 2=乙");
        for ((node, iteration), diff) in delta_at_entry {
            println!("entry update: node={node}, time=(1,{iteration}), diff={diff:+}");
        }

        for (label, updates) in [("JOIN output / distinct input", &join_updates), ("distinct output / before feedback", &distinct_updates)] {
            let mut net = BTreeMap::new();
            for (node, time, diff) in updates.borrow().iter() {
                *net.entry((*node, *time)).or_insert(0isize) += diff;
            }
            net.retain(|_, diff| *diff != 0);
            println!("{label}:");
            for ((node, time), diff) in &net {
                println!("  node={node}, time={time:?}, diff={diff:+}");
            }
            let alpha: BTreeMap<_,_> = net.into_iter().filter_map(|((node, time), diff)| {
                (node == 1).then_some((time, diff))
            }).collect();
            if label.starts_with("JOIN") {
                assert_eq!(alpha, BTreeMap::from([
                    (Product::new(0,0), 1), (Product::new(0,1), 1), (Product::new(1,0), -1),
                ]));
            } else {
                assert_eq!(alpha, BTreeMap::from([
                    (Product::new(0,0), 1), (Product::new(1,0), -1), (Product::new(1,1), 1),
                ]));
            }
        }

        let mut final_delta = BTreeMap::<u64,isize>::new();
        for (node, epoch, diff) in outer_updates.borrow().iter() {
            if *epoch == 1 { *final_delta.entry(*node).or_default() += diff; }
        }
        final_delta.retain(|_, diff| *diff != 0);
        assert!(final_delta.is_empty());
        println!("outer output net changes at input version 1: {final_delta:?}");

        for epoch in [0,1] {
            let mut snapshot = BTreeMap::<u64,isize>::new();
            for (node, time, diff) in outer_updates.borrow().iter() {
                if *time <= epoch { *snapshot.entry(*node).or_default() += diff; }
            }
            snapshot.retain(|_, diff| *diff != 0);
            assert_eq!(snapshot, BTreeMap::from([(1,1),(2,1)]));
            println!("outer snapshot at {epoch}: {snapshot:?}");
        }
        trace_event(&sequence, "INPUT: epoch 1 complete; remove P -> beta at epoch 2");
        edge_input.remove((0,2));
        edge_input.advance_to(3);
        edge_input.flush();
        settle(worker, || !probe.less_equal(&2));
        trace_event(&sequence, "PROGRESS: epoch 2 complete");

        let mut final_snapshot = BTreeMap::<u64,isize>::new();
        let mut last_distinct = BTreeMap::new();
        for (node, epoch, diff) in outer_updates.borrow().iter() {
            if *epoch <= 2 { *final_snapshot.entry(*node).or_default() += diff; }
        }
        final_snapshot.retain(|_, diff| *diff != 0);
        for (node, time, diff) in distinct_updates.borrow().iter() {
            if time.outer == 2 { *last_distinct.entry((*node, time.inner)).or_insert(0isize) += diff; }
        }
        last_distinct.retain(|_, diff| *diff != 0);
        assert!(final_snapshot.is_empty());
        assert_eq!(last_distinct, BTreeMap::from([((1,1),-1), ((2,0),-1)]));
        println!("epoch 2 distinct net updates (node, iteration): {last_distinct:?}");
        println!("outer snapshot at 2: {final_snapshot:?}");
        edge_input.close();
        settle(worker, || probe.done());
        println!("PASS: actual Differential loop-entry updates and outer snapshots");
    });
}
