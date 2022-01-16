use crate::op::{Join, Op, OpVisitor};
use crate::query::{QueryPlan, QuerySet};
use std::fmt::{self, Write};

const INDENT: usize = 4;
const BRANCH_1: char = '└';
const BRANCH_N: char = '├';
const BRANCH_V: char = '│';
const LINE: char = '─';

pub trait Explain {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result;
}

impl Explain for QueryPlan {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self.queries.get(&self.root) {
            Some(subq) => {
                let mut qe = QueryExplain {
                    queries: &self.queries,
                    f,
                    spans: vec![],
                    res: Ok(()),
                };
                subq.root.walk(&mut qe);
                qe.res
            }
            None => f.write_str("No plan found"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Span {
    Space(u16),
    Branch(u16, bool),
}

struct QueryExplain<'a, F> {
    queries: &'a QuerySet,
    f: &'a mut F,
    spans: Vec<Span>,
    res: fmt::Result,
}

impl<'a, F: Write> QueryExplain<'a, F> {
    // returns true if continue
    fn write_prefix(&mut self) -> bool {
        self.res = write_prefix(self.f, &self.spans);
        self.res.is_ok()
    }

    fn write_str(&mut self, s: &str) -> bool {
        self.res = self.f.write_str(s);
        self.res.is_ok()
    }

    fn set_res(&mut self, res: fmt::Result) -> bool {
        self.res = res;
        self.res.is_ok()
    }
}

impl<F: Write> OpVisitor for QueryExplain<'_, F> {
    #[inline]
    fn enter(&mut self, op: &Op) -> bool {
        let (text, span) = match op {
            Op::Aggr(_) => ("Aggr", Some(Span::Branch(1, false))),
            Op::Filt(_) => ("Filt", Some(Span::Branch(1, false))),
            Op::Limit(_) => ("Limit", Some(Span::Branch(1, false))),
            Op::Sort(_) => ("Sort", Some(Span::Branch(1, false))),
            Op::Proj(_) => ("Proj", Some(Span::Branch(1, false))),
            Op::Row(_) => ("Row", None),
            Op::Table(..) => ("Table", None),
            Op::Join(j) => (
                "Join",
                match j.as_ref() {
                    Join::Cross(jos) => Some(Span::Branch(jos.len() as u16, false)),
                    _ => Some(Span::Branch(2, false)),
                },
            ),
            Op::Subquery(query_id) => {
                if let Some(subq) = self.queries.get(query_id) {
                    let mut qe = QueryExplain {
                        queries: self.queries,
                        f: self.f,
                        spans: self.spans.clone(),
                        res: Ok(()),
                    };
                    subq.root.walk(&mut qe);
                    let res = qe.res;
                    return self.set_res(res);
                }
                ("(No subquery found)", None)
            }
        };
        if !self.write_prefix() {
            return false;
        }
        if let Some(span) = self.spans.pop() {
            match span {
                Span::Branch(1, _) => {
                    if let Some(Span::Space(n)) = self.spans.last_mut() {
                        *n += INDENT as u16
                    } else {
                        self.spans.push(Span::Space(INDENT as u16))
                    }
                }
                Span::Branch(n, _) => self.spans.push(Span::Branch(n - 1, true)),
                _ => self.spans.push(span),
            }
        }
        if let Some(span) = span {
            self.spans.push(span)
        }
        if !self.write_str(text) {
            return false;
        }
        self.write_str("\n")
    }

    #[inline]
    fn leave(&mut self, _op: &Op) -> bool {
        if let Some(span) = self.spans.last_mut() {
            match span {
                Span::Branch(1, _) => {
                    let _ = self.spans.pop();
                }
                Span::Branch(n, vertical) => {
                    *n -= 1;
                    *vertical = false;
                }
                _ => (),
            }
        }
        true
    }
}

fn write_prefix<F: Write>(f: &mut F, spans: &[Span]) -> fmt::Result {
    for &span in spans {
        match span {
            Span::Space(n) => {
                for _ in 0..n {
                    f.write_char(' ')?
                }
            }
            Span::Branch(1, false) => {
                f.write_char(BRANCH_1)?;
                for _ in 1..INDENT {
                    f.write_char(LINE)?
                }
            }
            Span::Branch(_, false) => {
                f.write_char(BRANCH_N)?;
                for _ in 1..INDENT {
                    f.write_char(LINE)?
                }
            }
            Span::Branch(_, true) => {
                f.write_char(BRANCH_V)?;
                for _ in 1..INDENT {
                    f.write_char(' ')?
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Explain;
    use crate::builder::tests::tpch_catalog;
    use crate::builder::PlanBuilder;
    use std::sync::Arc;
    use xngin_frontend::parser::dialect::MySQL;
    use xngin_frontend::parser::parse_query;

    #[test]
    fn test_explain_plan() {
        let cat = tpch_catalog();
        for sql in vec![
            "select 1",
            "with cte1 as (select 1), cte2 as (select 2) select * from cte1",
            "select l1.l_orderkey from lineitem l1, lineitem l2",
        ] {
            let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
            let (_, qr) = parse_query(MySQL(sql)).unwrap();
            let plan = builder.build_plan(&qr).unwrap();
            let mut s = String::new();
            let _ = plan.explain(&mut s).unwrap();
            println!("Explain plan:\n{}", s)
        }
    }
}
