use crate::join::graph::Edge;
use crate::join::{Join, JoinGraph, QualifiedJoin};
use crate::op::{Aggr, Apply, Op, OpVisitor, SortItem};
use crate::query::{QueryPlan, QuerySet};
use crate::setop::Setop;
use std::fmt::{self, Write};
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::{AggKind, Aggf, Col, Const, Expr, ExprKind, Farg, Pred, QueryID, Setq};

const INDENT: usize = 4;
const BRANCH_1: char = '└';
const BRANCH_N: char = '├';
const BRANCH_V: char = '│';
const LINE: char = '─';

/// Explain defines how to explain an expression, an operator
/// or a plan.
pub trait Explain {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result;
}

impl Explain for QueryPlan {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        for attach in &self.attaches {
            match self.qry_set.get(attach) {
                Some(subq) => {
                    let mut qe = QueryExplain {
                        title: Some(format!("(aq{})", **attach)),
                        queries: &self.qry_set,
                        f,
                        spans: vec![],
                    };
                    subq.root.walk(&mut qe).unbranch()?
                }
                None => f.write_str("No attached plan found")?,
            }
        }
        match self.qry_set.get(&self.root) {
            Some(subq) => {
                let mut qe = QueryExplain {
                    title: Some("(root) ".to_string()),
                    queries: &self.qry_set,
                    f,
                    spans: vec![],
                };
                subq.root.walk(&mut qe).unbranch()
            }
            None => f.write_str("No plan found"),
        }
    }
}

/* Implements Explain for all operators */

impl Explain for Op {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Op::Proj { cols, .. } => {
                f.write_str("Proj{")?;
                write_refs(f, cols.iter().map(|(e, _)| e), ", ")?;
                f.write_str("}")
            }
            Op::Filt { pred, .. } => {
                f.write_str("Filt{")?;
                if !pred.is_empty() {
                    write_refs(f, pred, " and ")?;
                }
                f.write_char('}')
            }
            Op::Aggr(aggr) => aggr.explain(f),
            Op::Sort { items, .. } => {
                f.write_str("Sort{")?;
                write_refs(f, items, ", ")?;
                f.write_char('}')
            }
            Op::Join(join) => join.explain(f),
            Op::JoinGraph(graph) => graph.explain(f),
            Op::Setop(setop) => setop.explain(f),
            Op::Limit { start, end, .. } => {
                write!(f, "Limit{{{}, {}}}", start, end)
            }
            Op::Attach(_, qry_id) => {
                f.write_str("Attach{")?;
                qry_id.explain(f)?;
                f.write_char('}')
            }
            Op::Row(row) => {
                f.write_str("Row{")?;
                write_refs(f, row.iter().map(|(e, _)| e), ", ")?;
                f.write_char('}')
            }
            Op::Table(_, table_id) => {
                write!(f, "Table{{{}}}", table_id.value())
            }
            Op::Query(_) => f.write_str("(subquery todo)"),
            Op::Empty => f.write_str("Empty"),
        }
    }
}

impl Explain for SortItem {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        self.expr.explain(f)?;
        if self.desc {
            f.write_str(" desc")?
        }
        Ok(())
    }
}

impl Explain for Aggr {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Aggr{")?;
        f.write_str("proj=[")?;
        write_refs(f, self.proj.iter().map(|(e, _)| e), ", ")?;
        f.write_str("]}")
    }
}

impl Explain for Join {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Join{")?;
        match self {
            Join::Cross(_) => f.write_str("cross")?,
            Join::Qualified(QualifiedJoin {
                kind, cond, filt, ..
            }) => {
                f.write_str(kind.to_lower())?;
                if !cond.is_empty() {
                    f.write_str(", cond=[")?;
                    write_refs(f, cond, " and ")?;
                    f.write_char(']')?
                }
                if !filt.is_empty() {
                    f.write_str(", filt=[")?;
                    write_refs(f, filt, " and ")?;
                    f.write_char(']')?
                }
            }
        }
        f.write_char('}')
    }
}

impl Explain for JoinGraph {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("JoinGraph{vs=[")?;
        write_refs(f, &self.queries(), ", ")?;
        f.write_str("]")?;
        if self.n_edges() > 0 {
            f.write_str(", es=[{")?;
            write_objs(
                f,
                self.eids().map(|eid| GraphEdge {
                    g: self,
                    e: self.edge(eid),
                }),
                "}, {",
            )?;
            f.write_str("}]")?
        }
        f.write_char('}')
    }
}

impl Explain for QueryID {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        write!(f, "q{}", **self)
    }
}

struct GraphEdge<'a> {
    g: &'a JoinGraph,
    e: &'a Edge,
}

impl<'a> Explain for GraphEdge<'a> {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str(self.e.kind.to_lower())?;
        write!(
            f,
            ", ls={}, rs={}, es={}, cond=[",
            self.e.l_vset.len(),
            self.e.r_vset.len(),
            self.e.e_vset.len()
        )?;
        write_refs(f, self.g.preds(self.e.cond.clone()), " and ")?;
        if !self.e.filt.is_empty() {
            f.write_str("], filt=[")?;
            write_refs(f, self.g.preds(self.e.filt.clone()), " and ")?;
        }
        f.write_char(']')
    }
}

impl Explain for Apply {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Apply{[")?;
        write_refs(f, &self.vars, ", ")?;
        f.write_str("]}")
    }
}

impl Explain for Setop {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Setop{")?;
        f.write_str(self.kind.to_lower())?;
        if self.q == Setq::All {
            f.write_str(" all")?
        }
        f.write_char('}')
    }
}

/* Implements Explain for all expressions */

impl Explain for Expr {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match &self.kind {
            ExprKind::Const(c) => c.explain(f),
            ExprKind::Col(c) => c.explain(f),
            ExprKind::Aggf(a) => a.explain(f),
            ExprKind::Func { kind, args, .. } => {
                f.write_str(kind.to_lower())?;
                f.write_char('(')?;
                if args.is_empty() {
                    return f.write_char(')');
                }
                write_refs(f, args.as_ref(), ", ")?;
                f.write_char(')')
            }
            ExprKind::Case { op, acts, fallback } => {
                f.write_str("case ")?;
                if op.kind != ExprKind::Farg(Farg::None) {
                    op.explain(f)?;
                    f.write_char(' ')?
                }
                for branch in acts.as_ref().chunks_exact(2) {
                    f.write_str("when ")?;
                    branch[0].explain(f)?;
                    f.write_str(" then ")?;
                    branch[1].explain(f)?;
                    f.write_char(' ')?
                }
                if fallback.kind != ExprKind::Farg(Farg::None) {
                    f.write_str("else ")?;
                    fallback.explain(f)?;
                    f.write_char(' ')?
                }
                f.write_str("end")
            }
            ExprKind::Pred(p) => p.explain(f),
            ExprKind::Tuple(es) => {
                f.write_char('(')?;
                write_refs(f, es, ", ")?;
                f.write_char(')')
            }
            ExprKind::Subq(_, qry_id) => {
                write!(f, "subq({})", **qry_id)
            }
            ExprKind::Attval(qry_id) => {
                write!(f, "attval({})", **qry_id)
            }
            ExprKind::Plhd(_) => write!(f, "(placeholder todo)"),
            ExprKind::Farg(_) => write!(f, "(funcarg todo)"),
        }
    }
}

impl Explain for Const {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Const::I64(v) => write!(f, "{}", v),
            Const::U64(v) => write!(f, "{}", v),
            Const::F64(v) => write!(f, "{}", v.value()),
            Const::Decimal(v) => write!(f, "{}", v.to_string(-1)),
            Const::Date(v) => write!(f, "date'{}'", v),
            Const::Time(v) => write!(f, "time'{}'", v),
            Const::Datetime(v) => write!(f, "timestamp'{}'", v),
            Const::Interval(v) => write!(f, "interval'{}'{}", v.value, v.unit.to_lower()),
            Const::String(s) => write!(f, "'{}'", s),
            Const::Bytes(_) => write!(f, "(bytes todo)"),
            Const::Bool(b) => write!(f, "{}", b),
            Const::Null => f.write_str("null"),
        }
    }
}

impl Explain for Col {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Col::TableCol(table_id, idx) => write!(f, "t{}.{}", table_id.value(), idx),
            Col::QueryCol(query_id, idx) => write!(f, "q{}.{}", **query_id, idx),
            Col::CorrelatedCol(query_id, idx) => write!(f, "cq{}.{}", **query_id, idx),
        }
    }
}

impl Explain for Aggf {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self.kind {
            AggKind::Count => f.write_str("count(")?,
            AggKind::Sum => f.write_str("sum(")?,
            AggKind::Max => f.write_str("max(")?,
            AggKind::Min => f.write_str("min(")?,
            AggKind::Avg => f.write_str("avg(")?,
        }
        if self.q == Setq::Distinct {
            f.write_str("distinct ")?
        }
        self.arg.explain(f)?;
        f.write_char(')')
    }
}

impl Explain for Pred {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Pred::Conj(es) => write_refs(f, es, " and "),
            Pred::Disj(es) => write_refs(f, es, " or "),
            Pred::Xor(es) => write_refs(f, es, " xor "),
            Pred::Func { kind, args } => {
                f.write_str(kind.to_lower())?;
                f.write_char('(')?;
                write_refs(f, args.as_ref(), ", ")?;
                f.write_char(')')
            }
            Pred::Not(e) => {
                f.write_str("not ")?;
                e.explain(f)
            }
            Pred::InSubquery(lhs, subq) => {
                lhs.explain(f)?;
                f.write_str(" in ")?;
                subq.explain(f)
            }
            Pred::NotInSubquery(lhs, subq) => {
                lhs.explain(f)?;
                f.write_str(" not in ")?;
                subq.explain(f)
            }
            Pred::Exists(subq) => {
                f.write_str("exists ")?;
                subq.explain(f)
            }
            Pred::NotExists(subq) => {
                f.write_str("not exists ")?;
                subq.explain(f)
            }
        }
    }
}

fn write_refs<'i, F, E: 'i, I>(f: &mut F, exprs: I, delimiter: &str) -> fmt::Result
where
    F: Write,
    E: Explain,
    I: IntoIterator<Item = &'i E>,
{
    let mut exprs = exprs.into_iter();
    if let Some(head) = exprs.next() {
        head.explain(f)?
    }
    for e in exprs {
        f.write_str(delimiter)?;
        e.explain(f)?
    }
    Ok(())
}

fn write_objs<'i, F, E: 'i, I>(f: &mut F, exprs: I, delimiter: &str) -> fmt::Result
where
    F: Write,
    E: Explain,
    I: IntoIterator<Item = E>,
{
    let mut exprs = exprs.into_iter();
    if let Some(head) = exprs.next() {
        head.explain(f)?
    }
    for e in exprs {
        f.write_str(delimiter)?;
        e.explain(f)?
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Span {
    Space(u16),
    Branch(u16, bool),
}

struct QueryExplain<'a, F> {
    title: Option<String>,
    queries: &'a QuerySet,
    f: &'a mut F,
    spans: Vec<Span>,
    // res: fmt::Result,
}

impl<'a, F: Write> QueryExplain<'a, F> {
    // returns true if continue
    fn write_prefix(&mut self) -> fmt::Result {
        write_prefix(self.f, &self.spans)?;
        // only write title once
        if let Some(s) = self.title.take() {
            self.write_str(&s)?;
        }
        Ok(())
    }

    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.f.write_str(s)
    }
}

impl<F: Write> OpVisitor for QueryExplain<'_, F> {
    type Cont = ();
    type Break = fmt::Error;
    #[inline]
    fn enter(&mut self, op: &Op) -> ControlFlow<fmt::Error> {
        // special handling Subquery
        if let Op::Query(query_id) = op {
            return if let Some(subq) = self.queries.get(query_id) {
                let mut qe = QueryExplain {
                    title: Some(format!("(q{}) ", **query_id)),
                    queries: self.queries,
                    f: self.f,
                    spans: self.spans.clone(),
                };
                subq.root.walk(&mut qe)
            } else {
                ControlFlow::Break(fmt::Error)
            };
        }
        let child_cnt = op.inputs().len();
        self.write_prefix().branch()?;
        // process at parent level
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
        // process at current level
        if child_cnt > 0 {
            self.spans.push(Span::Branch(child_cnt as u16, false))
        }
        op.explain(self.f).branch()?;
        self.write_str("\n").branch()
    }

    #[inline]
    fn leave(&mut self, _op: &Op) -> ControlFlow<fmt::Error> {
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
        ControlFlow::Continue(())
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
            "select 1, true, 1.0e2, 1 & 2, 1 | 2, 1 ^ 2, 1 << 2, 1 >> 2, 1 and 2, 1 or 2, 1 xor 2",
            "with cte1 as (select 1), cte2 as (select 2) select * from cte1",
            "select l1.l_orderkey from lineitem l1, lineitem l2",
            "select l_orderkey from lineitem union all select l_orderkey from lineitem",
            "select 1 union select 2",
            "select 1 from lineitem join (select 1) t1",
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
