//! Bounded exhaustive search (08 step 5): depth-first over the decisions
//! of a schedule, with at most K preemptions (CHESS-style context
//! bounding). Every branch reruns the schedule from the start (stateless
//! search); each run must replay its prefix exactly.
//!
//! [`dfs`] is the generic core: it knows decisions, options and their
//! costs, nothing of the driver. [`explore`] runs it over the driver's
//! cases, with the cost of an option from [`free_actions`]. The gate-stepped
//! lock search of `tests::sim_exhaustive` reuses [`dfs`] with its own
//! decisions.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use super::driver::{Case, Outcome, Policy, Signature, Template, free_actions, run_case};

/// One decision of a run as the search sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Point {
    /// Cost of each option: 0 free, 1 a preemption.
    pub costs: Vec<u32>,
    /// The option taken.
    pub taken: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct Bound {
    pub preemptions: u32,
    /// Stop after this many runs; the search is then incomplete.
    pub max_runs: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Search {
    pub runs: usize,
    /// Every schedule within the bound was run.
    pub complete: bool,
    /// The most decisions of one run.
    pub max_depth: usize,
}

/// A decision of the last run, with what the search did there.
struct Node {
    costs: Vec<u32>,
    taken: usize,
    /// Sum of the costs of the options taken at the nodes before it.
    budget_before: u32,
    /// Options run from here (the taken one included).
    tried: Vec<bool>,
}

/// Every schedule with at most `bound.preemptions` costly decisions.
///
/// `run(prefix)` runs one schedule: decision i takes `prefix[i]` for
/// i < prefix.len(), and a free option after that; it returns every
/// decision's point, or None to stop the search. Panics if a run does not
/// replay its prefix (another option count or costs, or another option
/// taken, at some i < prefix.len()) or takes a costly option past it: a
/// determinism leak, not a result.
///
/// The algorithm, iterative: keep the last run's points with, per node, the
/// budget used before it and the set of options tried there (initially the
/// one taken). The next branch is, from the deepest node upward, the first
/// node i with an untried option o (ascending index) whose cost fits:
/// `budget_before[i] + costs[o] <= bound.preemptions`; it is marked tried.
/// With none anywhere the search is complete. Otherwise run
/// `taken[0..i] ++ [o]`, check that it replays `0..=i`, and replace the
/// nodes after i by the new run's points. `runs` counts every call of
/// `run`, the first included; at `bound.max_runs` the search stops
/// incomplete.
pub fn dfs(bound: Bound, mut run: impl FnMut(&[usize]) -> Option<Vec<Point>>) -> Search {
    let mut search = Search::default();
    let mut nodes: Vec<Node> = Vec::new();
    let mut prefix: Vec<usize> = Vec::new();
    loop {
        if search.runs >= bound.max_runs {
            return search;
        }
        search.runs += 1;
        let Some(points) = run(&prefix) else {
            return search;
        };
        check_replay(&prefix, &nodes, &points);
        search.max_depth = search.max_depth.max(points.len());
        nodes.truncate(prefix.len());
        if let Some(last) = nodes.last_mut() {
            last.taken = prefix[prefix.len() - 1];
        }
        for p in points.into_iter().skip(prefix.len()) {
            let budget_before = nodes
                .last()
                .map_or(0, |n| n.budget_before + n.costs[n.taken]);
            let mut tried = vec![false; p.costs.len()];
            tried[p.taken] = true;
            nodes.push(Node {
                costs: p.costs,
                taken: p.taken,
                budget_before,
                tried,
            });
        }
        let Some((i, o)) = next_branch(&nodes, bound.preemptions) else {
            search.complete = true;
            return search;
        };
        nodes[i].tried[o] = true;
        prefix = nodes[..i].iter().map(|n| n.taken).collect();
        prefix.push(o);
    }
}

/// The deepest node with an untried option that fits the budget.
fn next_branch(nodes: &[Node], preemptions: u32) -> Option<(usize, usize)> {
    nodes.iter().enumerate().rev().find_map(|(i, n)| {
        (0..n.costs.len())
            .find(|&o| !n.tried[o] && n.budget_before + n.costs[o] <= preemptions)
            .map(|o| (i, o))
    })
}

/// A run must replay its prefix: the decisions before the branch as last
/// time, the branch's option taken, and only free options after it.
fn check_replay(prefix: &[usize], nodes: &[Node], points: &[Point]) {
    assert!(
        points.len() >= prefix.len(),
        "a run ended after {} decisions, inside its prefix of {}: {prefix:?}",
        points.len(),
        prefix.len()
    );
    for (i, (want, p)) in prefix.iter().zip(points).enumerate() {
        let n = &nodes[i];
        assert!(
            p.costs == n.costs && p.taken == *want,
            "decision {i} does not replay: costs {:?} (were {:?}), took {} (asked {want}); \
             prefix {prefix:?}",
            p.costs,
            n.costs,
            p.taken
        );
    }
    for (i, p) in points.iter().enumerate().skip(prefix.len()) {
        assert!(
            p.taken < p.costs.len() && p.costs[p.taken] == 0,
            "decision {i} past the prefix took costly option {} of {:?}",
            p.taken,
            p.costs
        );
    }
}

/// What a bounded search over a case found.
#[derive(Debug)]
pub struct Exploration {
    pub search: Search,
    /// Runs with an empty signature set (a clean report).
    pub clean: usize,
    /// Per signature: how many runs showed it, and the first such run's case
    /// (choices = every index taken, policy `Continue`: replays with
    /// `run_case`).
    pub signatures: BTreeMap<Signature, (usize, Case)>,
    pub elapsed: Duration,
}

/// Whether the search goes on after a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Control {
    Continue,
    Stop,
}

/// Every schedule of `base` (its choices and policy are ignored) with at
/// most `bound.preemptions` costly decisions; an option costs 0 if
/// [`free_actions`] marks it free, else 1. `seen` gets each run's outcome
/// and may stop the search.
pub fn explore(
    template: &Template,
    base: &Case,
    bound: Bound,
    seen: impl FnMut(&Outcome) -> Control,
) -> Exploration {
    explore_with(template, base, bound, |_| {}, seen)
}

/// [`explore`], with `before` called with each run's case before it runs
/// (its choices are the prefix; with policy `Continue` it replays the run).
pub fn explore_with(
    template: &Template,
    base: &Case,
    bound: Bound,
    mut before: impl FnMut(&Case),
    mut seen: impl FnMut(&Outcome) -> Control,
) -> Exploration {
    let start = Instant::now();
    let mut clean = 0;
    let mut signatures: BTreeMap<Signature, (usize, Case)> = BTreeMap::new();
    let search = dfs(bound, |prefix| {
        let case = Case {
            choices: prefix
                .iter()
                .map(|&c| u16::try_from(c).expect("fewer than 2^16 actions"))
                .collect(),
            policy: Policy::Continue,
            ..base.clone()
        };
        before(&case);
        let o = run_case(template, &case);
        let entries = &o.trace.entries;
        let points: Vec<Point> = entries
            .iter()
            .enumerate()
            .map(|(n, e)| Point {
                costs: free_actions(&e.actions, &entries[..n])
                    .iter()
                    .map(|free| u32::from(!free))
                    .collect(),
                taken: usize::from(e.choice),
            })
            .collect();
        if o.signatures.is_empty() {
            clean += 1;
        }
        for s in &o.signatures {
            signatures
                .entry(*s)
                .or_insert_with(|| {
                    let choices = entries.iter().map(|e| e.choice).collect();
                    (
                        0,
                        Case {
                            choices,
                            ..case.clone()
                        },
                    )
                })
                .0 += 1;
        }
        match seen(&o) {
            Control::Continue => Some(points),
            Control::Stop => None,
        }
    });
    Exploration {
        search,
        clean,
        signatures,
        elapsed: start.elapsed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two threads of three steps each. A decision offers the threads with
    /// steps left, in order; continuing the thread that stepped last is
    /// free, and when it is done (or at the start) every option is free.
    fn two_threads(prefix: &[usize], lie_at: Option<usize>) -> Vec<Point> {
        let mut left = [3u32, 3];
        let mut last: Option<usize> = None;
        let mut points = Vec::new();
        while left.iter().any(|l| *l > 0) {
            let options: Vec<usize> = (0..2).filter(|t| left[*t] > 0).collect();
            let continuing = last.filter(|t| left[*t] > 0);
            let mut costs: Vec<u32> = options
                .iter()
                .map(|t| u32::from(continuing.is_some_and(|c| c != *t)))
                .collect();
            let n = points.len();
            let taken = match prefix.get(n) {
                Some(o) => *o,
                None => costs.iter().position(|c| *c == 0).unwrap(),
            };
            if lie_at.is_some_and(|l| l == n) && !prefix.is_empty() {
                costs.push(1);
            }
            let t = options[taken];
            left[t] -= 1;
            last = Some(t);
            points.push(Point { costs, taken });
        }
        points
    }

    fn search(k: u32) -> Search {
        let mut seen = std::collections::BTreeSet::new();
        let s = dfs(
            Bound {
                preemptions: k,
                max_runs: 1000,
            },
            |prefix| {
                let points = two_threads(prefix, None);
                let schedule: Vec<usize> = points.iter().map(|p| p.taken).collect();
                assert!(seen.insert(schedule), "a schedule ran twice");
                Some(points)
            },
        );
        assert!(s.complete);
        assert_eq!(s.max_depth, 6);
        s
    }

    /// A schedule of the two threads is a sequence of blocks, alternating
    /// threads. The switch into the last block is free (the other thread is
    /// done), every other switch a preemption, so b blocks cost b - 2.
    /// Splitting 3 steps into j blocks has C(2, j - 1) ways:
    /// - 2 blocks: AAABBB, BBBAAA: 2 schedules, cost 0;
    /// - 3 blocks: ABA (2 x 1), BAB (1 x 2): 4, cost 1;
    /// - 4 blocks: ABAB, BABA (2 x 2 each): 8, cost 2;
    /// - 5 and 6 blocks: 4 and 2 (20 in all, C(6, 3)).
    ///
    /// So K = 0, 1, 2 run 2, 6 and 14 schedules, and K = 4 all 20.
    #[test]
    fn dfs_counts_schedules_within_the_bound() {
        assert_eq!(search(0).runs, 2);
        assert_eq!(search(1).runs, 6);
        assert_eq!(search(2).runs, 14);
        assert_eq!(search(4).runs, 20);
    }

    #[test]
    fn dfs_stops_at_max_runs() {
        let s = dfs(
            Bound {
                preemptions: 2,
                max_runs: 5,
            },
            |prefix| Some(two_threads(prefix, None)),
        );
        assert_eq!(s.runs, 5);
        assert!(!s.complete);
    }

    #[test]
    #[should_panic(expected = "does not replay")]
    fn dfs_panics_when_a_prefix_does_not_replay() {
        dfs(
            Bound {
                preemptions: 1,
                max_runs: 100,
            },
            |prefix| Some(two_threads(prefix, Some(0))),
        );
    }
}
