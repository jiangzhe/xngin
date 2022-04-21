use crate::binary::ArithKind;
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::mem;
use xngin_catalog::TableID;
use xngin_datatype::{PreciseType, Typed};
use xngin_expr::{Const, DataSourceID, Expr, ExprKind, FuncKind, QueryID};
use xngin_storage::attr::Attr;
use xngin_storage::block::Block;
use xngin_storage::codec::Single;

/// Eval is similar to [`xngin_expr::Expr`], but only for evaluation.
/// It supports deterministic scalar expressions and is restricted to
/// evaluate within single block.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Eval {
    pub kind: EvalKind,
    pub ty: PreciseType,
}

impl Eval {
    #[inline]
    pub fn new_const(value: Const) -> Self {
        let ty = value.pty();
        Eval {
            kind: EvalKind::Compute {
                kind: ComputeKind::Const(value),
                idx: 0,
            },
            ty,
        }
    }

    #[inline]
    pub fn new_ref(r: EvalRef, ty: PreciseType) -> Self {
        Eval {
            kind: EvalKind::Ref(r),
            ty,
        }
    }

    #[inline]
    pub fn arith(kind: ArithKind, lhs: Eval, rhs: Eval, ty: PreciseType) -> Self {
        Eval {
            kind: EvalKind::Compute {
                kind: ComputeKind::Arith {
                    kind,
                    args: vec![lhs, rhs].into_boxed_slice(),
                },
                idx: 0,
            },
            ty,
        }
    }

    #[inline]
    pub fn set_idx(&mut self, new_idx: usize) {
        if let EvalKind::Compute { idx, .. } = &mut self.kind {
            *idx = new_idx;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EvalKind {
    Ref(EvalRef),
    /// idx is the index of cached intermediate result.
    Compute {
        kind: ComputeKind,
        idx: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComputeKind {
    Const(Const),
    Arith { kind: ArithKind, args: Box<[Eval]> },
}

#[derive(Debug)]
struct Builder<'a, T> {
    input: Vec<(T, u32)>,
    input_map: HashMap<(T, u32), (usize, PreciseType)>,
    cache: Vec<Eval>,
    cache_map: HashMap<&'a Expr, (usize, PreciseType)>,
}

impl<'a, T> Builder<'a, T> {
    #[inline]
    fn new() -> Self {
        Builder {
            input: vec![],
            input_map: HashMap::new(),
            cache: vec![],
            cache_map: HashMap::new(),
        }
    }
}

impl<'a, T: DataSourceID> Builder<'a, T> {
    #[inline]
    pub fn build<I: IntoIterator<Item = &'a Expr>>(mut self, exprs: I) -> Result<EvalPlan<T>> {
        let mut output = vec![];
        for e in exprs {
            let (out, _) = self.find_ref_or_gen(e)?;
            output.push(out);
        }
        Ok(EvalPlan {
            input: self.input,
            output,
            evals: self.cache,
        })
    }

    #[inline]
    fn find_ref_or_gen(&mut self, e: &'a Expr) -> Result<(EvalRef, PreciseType)> {
        if let Some(out) = self.find(e) {
            return Ok(out);
        }
        self.gen(e)
    }

    #[inline]
    fn find_or_gen(&mut self, e: &'a Expr) -> Result<Eval> {
        let (r, ty) = self.find_ref_or_gen(e)?;
        Ok(Eval::new_ref(r, ty))
    }

    #[inline]
    fn find(&self, e: &'a Expr) -> Option<(EvalRef, PreciseType)> {
        if let Some((dsid, idx)) = T::from_expr(e) {
            self.input_map
                .get(&(dsid, idx))
                .map(|(idx, ty)| (EvalRef::Input(*idx), *ty))
        } else {
            self.cache_map
                .get(e)
                .map(|(idx, ty)| (EvalRef::Cache(*idx), *ty))
        }
    }

    #[inline]
    fn gen(&mut self, e: &'a Expr) -> Result<(EvalRef, PreciseType)> {
        let mut cached = match &e.kind {
            ExprKind::Col(_) => {
                let (dsid, idx) = T::from_expr(e).ok_or(Error::FailToBuildEvalArgs)?;
                let in_idx = self.input.len();
                self.input_map.insert((dsid, idx), (in_idx, e.ty));
                self.input.push((dsid, idx));
                return Ok((EvalRef::Input(in_idx), e.ty));
            }
            ExprKind::Const(c) => Eval::new_const(c.clone()),
            ExprKind::Func {
                kind: FuncKind::Add,
                args,
            } => {
                let lhs = self.find_or_gen(&args[0])?;
                let rhs = self.find_or_gen(&args[1])?;
                Eval::arith(ArithKind::Add, lhs, rhs, e.ty)
            }
            other => todo!("unimplemented evaluation of {:?}", other),
        };
        let ty = cached.ty;
        let cache_idx = self.cache.len();
        cached.set_idx(cache_idx);
        self.cache_map.insert(e, (cache_idx, ty));
        self.cache.push(cached);
        Ok((EvalRef::Cache(cache_idx), ty))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvalRef {
    Input(usize),
    Cache(usize),
}

pub type TableEvalPlan = EvalPlan<TableID>;
pub type QueryEvalPlan = EvalPlan<QueryID>;

/// Evaluation Plan.
///
/// It is designed to evaluate all expressions in order,
/// and make any common expression be evaluated only once.
/// That means if we have expression, e.g. `abs(c0)`, `abs(c0)+1`, `abs(c0)+2`,
/// the common expression `abs(c0)` will be evaluated only once, and be reused
/// by following expressions.
///
/// The plan maintains the input columns, evaluation cache instructions, and
/// output references to input and cache.
///
/// Currently, all expressions are considered as deterministic.
#[derive(Debug)]
pub struct EvalPlan<T> {
    pub input: Vec<(T, u32)>,
    pub evals: Vec<Eval>,
    pub output: Vec<EvalRef>,
}

impl<T: DataSourceID> EvalPlan<T> {
    #[inline]
    pub fn new<'a, I: IntoIterator<Item = &'a Expr>>(exprs: I) -> Result<Self> {
        Builder::new().build(exprs)
    }

    #[inline]
    pub fn eval(&self, block: &Block) -> Result<Vec<Attr>> {
        assert!(block.data.len() >= self.input.len());
        let mut cache: EvalCache = (0..self.evals.len())
            .map(|_| CacheEntry::default())
            .collect();
        // Evaluate all expressions with cache
        for e in &self.evals {
            self.eval_single(block, &mut cache, e)?;
        }
        // fetch output from cache
        self.fetch_output(block, cache)
    }

    #[inline]
    pub fn fetch_output(&self, block: &Block, mut cache: EvalCache) -> Result<Vec<Attr>> {
        let mut output = Vec::with_capacity(self.output.len());
        for r in &self.output {
            match r {
                EvalRef::Input(idx) => {
                    let ext = block.fetch_attr(*idx).ok_or(Error::FailToFetchAttr)?;
                    output.push(ext);
                }
                EvalRef::Cache(idx) => {
                    let ent = cache.get_mut(*idx).ok_or(Error::FailToFetchEvalCache)?;
                    match mem::take(ent) {
                        CacheEntry::Empty => return Err(Error::FailToFetchEvalCache),
                        CacheEntry::Some(attr) => {
                            let idx = output.len();
                            output.push(attr);
                            *ent = CacheEntry::Taken(idx); // update output index back
                        }
                        CacheEntry::Taken(out_idx) => {
                            let ext = output[out_idx].to_owned();
                            output.push(ext);
                            *ent = CacheEntry::Taken(out_idx); // update it back
                        }
                    }
                }
            }
        }
        Ok(output)
    }

    #[inline]
    fn eval_single(&self, input: &Block, cache: &mut EvalCache, e: &Eval) -> Result<EvalRef> {
        match &e.kind {
            EvalKind::Ref(r) => Ok(*r),
            EvalKind::Compute { kind, idx } => {
                let res = match kind {
                    ComputeKind::Const(c) => self.eval_const(input, c),
                    ComputeKind::Arith { kind, args } => {
                        self.eval_arith(input, cache, *kind, &args[0], &args[1])
                    }
                }?;
                debug_assert_eq!(e.ty, res.ty);
                self.store(cache, *idx, res);
                Ok(EvalRef::Cache(*idx))
            }
        }
    }

    #[inline]
    fn eval_const(&self, input: &Block, c: &Const) -> Result<Attr> {
        let res = match c {
            Const::I64(i) => {
                Attr::new_single(PreciseType::i64(), Single::new(*i, input.n_records()))
            }
            _ => todo!(),
        };
        Ok(res)
    }

    #[inline]
    fn eval_arith(
        &self,
        input: &Block,
        cache: &mut EvalCache,
        kind: ArithKind,
        lhs: &Eval,
        rhs: &Eval,
    ) -> Result<Attr> {
        let l_idx = self.eval_single(input, cache, lhs)?;
        let r_idx = self.eval_single(input, cache, rhs)?;
        let l = self.load_res(input, cache, l_idx)?;
        let r = self.load_res(input, cache, r_idx)?;
        kind.eval(l, r)
    }

    #[inline]
    fn store(&self, cache: &mut EvalCache, idx: usize, attr: Attr) {
        assert!(idx < cache.len());
        cache[idx] = CacheEntry::Some(attr);
    }

    #[inline]
    fn load<'a>(&self, cache: &'a EvalCache, idx: usize) -> Option<&'a Attr> {
        cache.get(idx).and_then(|v| match v {
            CacheEntry::Some(attr) => Some(attr),
            _ => None,
        })
    }

    #[inline]
    fn load_res<'a>(&self, input: &'a Block, cache: &'a EvalCache, r: EvalRef) -> Result<&'a Attr> {
        let res = match r {
            EvalRef::Input(in_idx) => input.data.get(in_idx),
            EvalRef::Cache(eval_idx) => self.load(cache, eval_idx),
        };
        res.ok_or(Error::MissingAttr)
    }
}

/// Evaluation cache, backed by vector of codec.
/// It enables reusing intermediate computations.
/// e.g. SELECT abs(a)+1, abs(a)+2, abs(a)+3 from t
/// We compute abs(a) only once and store it in the cache,
/// and let other expressions to reuse it if possible.
pub type EvalCache = Vec<CacheEntry>;

#[derive(Debug)]
pub enum CacheEntry {
    Empty,
    Some(Attr),
    /// Taken is a special cases that the value is taken
    /// for output, but maybe other outputs also require
    /// it, so we leave the index and let latter to copy it
    /// from that index.
    Taken(usize),
}

impl Default for CacheEntry {
    #[inline]
    fn default() -> Self {
        CacheEntry::Empty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xngin_expr::infer::fix_rec;
    use xngin_expr::QueryID;

    #[test]
    fn test_build_eval() {
        let col1 = Expr::query_col(QueryID::from(0), 0);
        let col2 = Expr::query_col(QueryID::from(0), 1);
        for (exprs, (n_input, n_cache, n_output)) in vec![
            // select c1
            (vec![col1.clone()], (1, 0, 1)),
            // select c1 + 1
            (
                vec![Expr::func(
                    FuncKind::Add,
                    vec![col1.clone(), Expr::new_const(Const::I64(1))],
                )],
                (1, 2, 1),
            ),
            // select c1 + c2
            (
                vec![Expr::func(FuncKind::Add, vec![col1.clone(), col2.clone()])],
                (2, 1, 1),
            ),
            // select c1 + 1 + c2
            (
                vec![Expr::func(
                    FuncKind::Add,
                    vec![
                        Expr::func(
                            FuncKind::Add,
                            vec![col1.clone(), Expr::new_const(Const::I64(1))],
                        ),
                        col2.clone(),
                    ],
                )],
                (2, 3, 1),
            ),
            // select c1, c2
            (vec![col1.clone(), col2.clone()], (2, 0, 2)),
            (
                vec![
                    Expr::func(
                        FuncKind::Add,
                        vec![col1.clone(), Expr::new_const(Const::I64(1))],
                    ),
                    Expr::func(
                        FuncKind::Add,
                        vec![col1.clone(), Expr::new_const(Const::I64(1))],
                    ),
                ],
                (1, 2, 2),
            ),
        ] {
            let plan = build_plan(exprs);
            assert_eq!(n_input, plan.input.len());
            assert_eq!(n_cache, plan.evals.len());
            assert_eq!(n_output, plan.output.len());
        }
    }

    #[test]
    fn test_run_eval() {
        let size = 1024i32;
        let attr1 = Attr::from((0..size).into_iter().map(|i| i as i64));
        let attr2 = attr1.to_owned();
        let block = Block::new(vec![attr1, attr2]);
        let col1 = Expr::query_col(QueryID::from(0), 0);
        let col2 = Expr::query_col(QueryID::from(0), 1);
        // select c1
        let es = vec![col1.clone()];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_none());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| i as i64).collect();
        assert_eq!(&expected, data);
        // select c1 + 1
        let es = vec![Expr::func(
            FuncKind::Add,
            vec![col1.clone(), Expr::new_const(Const::I64(1))],
        )];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_none());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
        assert_eq!(&expected, data);
        // select c1 + 1, c1 + 1
        let es = vec![
            Expr::func(
                FuncKind::Add,
                vec![col1.clone(), Expr::new_const(Const::I64(1))],
            ),
            Expr::func(
                FuncKind::Add,
                vec![col1.clone(), Expr::new_const(Const::I64(1))],
            ),
        ];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(2, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        assert_eq!(PreciseType::i64(), res[1].ty);
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_none());
        assert_eq!(&expected, data);
        let data: &[i64] = res[1].codec.as_array().unwrap().cast_slice();
        assert!(res[1].validity.is_none());
        assert_eq!(&expected, data);
        // select c1 + c2
        let es = vec![Expr::func(FuncKind::Add, vec![col1.clone(), col2.clone()])];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_none());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + i) as i64).collect();
        assert_eq!(&expected, data);
        // select c1 + 1, c1 + c2
        let es = vec![
            Expr::func(
                FuncKind::Add,
                vec![col1.clone(), Expr::new_const(Const::I64(1))],
            ),
            Expr::func(FuncKind::Add, vec![col1.clone(), col2.clone()]),
        ];
        let plan = build_plan(es);
        let res = plan.eval(&block).unwrap();
        assert_eq!(2, res.len());
        assert_eq!(PreciseType::i64(), res[0].ty);
        assert_eq!(PreciseType::i64(), res[1].ty);
        let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
        assert!(res[0].validity.is_none());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
        assert_eq!(&expected, data);
        let data: &[i64] = res[1].codec.as_array().unwrap().cast_slice();
        assert!(res[1].validity.is_none());
        let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + i) as i64).collect();
        assert_eq!(&expected, data);
    }

    #[inline]
    fn build_plan(mut exprs: Vec<Expr>) -> QueryEvalPlan {
        for e in &mut exprs {
            fix_rec(e, |_, _| Some(PreciseType::i64())).unwrap();
        }
        QueryEvalPlan::new(&exprs).unwrap()
    }
}
