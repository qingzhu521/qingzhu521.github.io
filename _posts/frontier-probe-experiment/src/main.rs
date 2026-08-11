use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::vec::BranchWhen;
use timely::dataflow::operators::{Capability, Concat, ConnectLoop, Enter, Feedback, Leave, Probe, ToStream};
use timely::order::Product;

type TOuter = Product<u64, u64>; // (e, o)
type TInner = Product<TOuter, u64>; // ((e,o), i)

fn main() {
    timely::execute(timely::Config::thread(), |worker| {
        // phase: 0 = hold cap at ((0,2),4); 1 = advance to ((0,2),5); 2 = drop everything
        let phase = Rc::new(RefCell::new(0u8));

        let mut probe = ProbeHandle::<TInner>::new(); // body output (where the stash cap lives)
        let mut probe_p = ProbeHandle::<TInner>::new(); // inner entry P
        let mut probe_reader = probe.clone();
        let mut probe_p_reader = probe_p.clone();
        let body_addr: Rc<RefCell<std::rc::Rc<[usize]>>> =
            Rc::new(RefCell::new(std::rc::Rc::from(vec![] as Vec<usize>)));

        worker.dataflow::<u64, _, _>(|root| {
            // one record at time 0 (epoch 0)
            let input = std::iter::once(()).to_stream(root).container::<Vec<()>>();

            root.scoped::<TOuter, _, _>("outer", |outer| {
                let (ohandle, ocycle) = outer.feedback::<Vec<()>>(Product::new(0, 1));
                let recirc = input.enter(outer).concat(ocycle);

                let inner_out = outer.scoped::<TInner, _, _>("inner", |inner| {
                    let entered = recirc.enter(inner); // ((e,o),0)
                    let (ihandle, icycle) =
                        inner.feedback::<Vec<()>>(Product::new(Product::new(0, 0), 1));

                    // P = inner-loop entry: probe
                    let p_in = entered.concat(icycle).probe_with(&mut probe_p);

                    // inner body: pass through for o<2; stash cap at ((e,2),4) for o==2
                    let flag = phase.clone();
                    let mut b = OperatorBuilder::new("inner_body".into(), inner.clone());
                    {
                        let addr = b.operator_info().address.clone();
                        *body_addr.borrow_mut() = addr;
                    }
                    let mut inp = b.new_input(p_in, Pipeline);
                    let (mut out, out_stream) = b.new_output::<Vec<()>>();
                    b.build_reschedule(move |_caps| {
                        let mut held: Option<Capability<TInner>> = None;
                        move |_frontiers| {
                            inp.for_each(|cap, data| {
                                let t = cap.time();
                                let e = t.outer.outer;
                                let o = t.outer.inner;
                                if o < 2 {
                                    let mut s = out.activate();
                                    s.give(&cap, data); // pass through
                                } else {
                                    // stash: hold a capability at ((e,2),4)
                                    held = Some(cap.delayed(&Product::new(Product::new(e, 2), 4), 0));
                                }
                            });
                            let ph = *flag.borrow();
                            if ph == 1 {
                                if let Some(h) = held.as_mut() {
                                    h.downgrade(&Product::new(Product::new(0, 2), 5));
                                    *flag.borrow_mut() = 0; // act once
                                }
                            } else if ph == 2 {
                                held = None;
                                *flag.borrow_mut() = 0;
                            }
                            held.is_some()
                        }
                    });

                    // observe the body output frontier (this is where the stash cap lives)
                    let probed = out_stream.probe_with(&mut probe);

                    // inner loop back-edge exists for topology; nothing uses it in this test
                    let (to_exit, to_loop) =
                        probed.branch_when(|t: &TInner| t.outer.inner == 2 && t.inner < 4);
                    to_loop.connect_loop(ihandle);
                    to_exit.leave(outer)
                });

                // outer routing: o < 3 loops the outer feedback; o >= 3 leaves the outer scope
                let (outer_loop, outer_exit) = inner_out.branch_when(|t: &TOuter| t.inner >= 3);
                outer_loop.connect_loop(ohandle);
                outer_exit.leave(root)
            });
        });

        let mut flipped1 = false;
        let mut flipped2 = false;
        for step in 0..600 {
            worker.step();

            let f = probe_reader.with_frontier(|f| f.to_vec());
            let fp = probe_p_reader.with_frontier(|f| f.to_vec());
            println!("step {:3} | @body {:?} | @P {:?}", step, f, fp);

            let stable_hold = f.iter().any(|t| *t == Product::new(Product::new(0, 2), 4));
            if !flipped1 && step > 5 && stable_hold {
                println!("--- phase 1: advance held cap to ((0,2),5) ---");
                *phase.borrow_mut() = 1;
                worker.activations().borrow_mut().activate(&body_addr.borrow());
                flipped1 = true;
            }
            let advanced = f.iter().any(|t| *t == Product::new(Product::new(0, 2), 5));
            if flipped1 && !flipped2 && advanced {
                println!("--- phase 2: drop everything ---");
                *phase.borrow_mut() = 2;
                worker.activations().borrow_mut().activate(&body_addr.borrow());
                flipped2 = true;
            }
            if flipped2 && f.is_empty() {
                println!("--- frontier EMPTY at step {} ---", step);
                break;
            }
        }
        ()
    })
    .unwrap();
}
