use crate::controlflow::{Branch, ControlFlow, Unbranch};
use crate::error::{Error, Result};
use crate::{
    Col, ColIndex, ColKind, Expr, ExprKind, ExprMutVisitor, FuncKind, Pred, PredFuncKind, QueryID,
};
use std::cmp::Ordering;
use std::mem;
use xngin_datatype::{Collation, PreciseType, Typed};

#[inline]
pub fn fix_rec<F: Fn(QueryID, ColIndex) -> Option<PreciseType>>(e: &mut Expr, f: F) -> Result<()> {
    e.walk_mut(&mut FixRec(f)).unbranch()
}

struct FixRec<F>(F);

impl<F> ExprMutVisitor for FixRec<F>
where
    F: Fn(QueryID, ColIndex) -> Option<PreciseType>,
{
    type Cont = ();
    type Break = Error;
    #[inline]
    fn leave(&mut self, e: &mut Expr) -> ControlFlow<Error> {
        fix(e, &self.0).branch()
    }
}

/// Fix data type of given expression, with the assumption that
/// all its arguments have been already fixed.
#[inline]
pub fn fix<F: Fn(QueryID, ColIndex) -> Option<PreciseType>>(e: &mut Expr, f: F) -> Result<()> {
    match &mut e.kind {
        ExprKind::Const(c) => e.ty = c.pty(),
        ExprKind::Col(Col { kind, idx, .. }) => match kind {
            ColKind::QueryCol(qid) | ColKind::CorrelatedCol(qid) => {
                if let Some(pty) = f(*qid, *idx) {
                    e.ty = pty;
                } else {
                    return Err(Error::UnknownColumnType);
                }
            }
            ColKind::TableCol(..) => (), // table column already has type, do nothing
        },
        ExprKind::Func { kind, args } => {
            let pty = fix_func(*kind, args.as_mut())?;
            e.ty = pty;
        }
        ExprKind::Pred(pred) => {
            // Predicates always return bool
            fix_pred(pred)?;
            e.ty = PreciseType::bool();
        }
        _ => todo!(),
    }
    Ok(())
}

#[inline]
pub fn fix_bools(preds: &mut [Expr]) {
    for e in preds {
        if !e.ty.is_bool() {
            cast_arg(e, PreciseType::bool());
        }
    }
}

#[inline]
fn fix_func(kind: FuncKind, args: &mut [Expr]) -> Result<PreciseType> {
    use PreciseType as PT;
    match kind {
        FuncKind::Add | FuncKind::Sub => match (args[0].ty, args[1].ty) {
            (PT::Unknown, _) | (_, PT::Unknown) => Err(Error::UnknownArgumentType),
            (PT::Compound, _) | (_, PT::Compound) => todo!("compound type infer"),
            // float vs float
            (PT::Float(lhs_bytes), PT::Float(rhs_bytes)) => {
                match lhs_bytes.cmp(&rhs_bytes) {
                    Ordering::Greater => {
                        // implicit cast rhs
                        let res_ty = PT::Float(lhs_bytes);
                        cast_arg(&mut args[1], res_ty);
                        Ok(res_ty)
                    }
                    Ordering::Less => {
                        // implicit cast lhs
                        let res_ty = PT::Float(rhs_bytes);
                        cast_arg(&mut args[0], res_ty);
                        Ok(res_ty)
                    }
                    Ordering::Equal => Ok(PT::Float(lhs_bytes)),
                }
            }
            // decimal vs decimal
            (PT::Decimal(lhs_prec, lhs_frac), PT::Decimal(rhs_prec, rhs_frac)) => {
                let lhs_intg = lhs_prec - lhs_frac;
                let rhs_intg = rhs_prec - rhs_frac;
                match (lhs_intg.cmp(&rhs_intg), lhs_frac.cmp(&rhs_frac)) {
                    (Ordering::Equal, Ordering::Equal) => Ok(PT::Decimal(lhs_prec, lhs_frac)),
                    (Ordering::Equal, Ordering::Greater)
                    | (Ordering::Greater, Ordering::Greater | Ordering::Equal) => {
                        let res_ty = PT::Decimal(lhs_prec, lhs_frac);
                        cast_arg(&mut args[1], res_ty);
                        Ok(res_ty)
                    }
                    (Ordering::Equal, Ordering::Less)
                    | (Ordering::Less, Ordering::Less | Ordering::Equal) => {
                        let res_ty = PT::Decimal(rhs_prec, rhs_frac);
                        cast_arg(&mut args[0], res_ty);
                        Ok(res_ty)
                    }
                    (Ordering::Greater, Ordering::Less) => {
                        let res_ty = PT::Decimal(lhs_intg + rhs_frac, rhs_frac);
                        cast_arg(&mut args[0], res_ty);
                        cast_arg(&mut args[1], res_ty);
                        Ok(res_ty)
                    }
                    (Ordering::Less, Ordering::Greater) => {
                        let res_ty = PT::Decimal(rhs_intg + lhs_frac, lhs_frac);
                        cast_arg(&mut args[0], res_ty);
                        cast_arg(&mut args[1], res_ty);
                        Ok(res_ty)
                    }
                }
            }
            // int vs int
            // Align integer with same width, prioritize unsigned over signed.
            (PT::Int(lhs_bytes, lhs_unsigned), PT::Int(rhs_bytes, rhs_unsigned)) => {
                match lhs_bytes.cmp(&rhs_bytes) {
                    Ordering::Greater => {
                        // implicit cast rhs
                        cast_arg(&mut args[1], PT::int(lhs_bytes, rhs_unsigned));
                        Ok(PT::int(lhs_bytes, lhs_unsigned))
                    }
                    Ordering::Less => {
                        // implicit cast lhs
                        cast_arg(&mut args[0], PT::int(rhs_bytes, lhs_unsigned));
                        Ok(PT::int(rhs_bytes, rhs_unsigned))
                    }
                    // coerce to unsigned if any unsigned.
                    Ordering::Equal => Ok(PT::int(lhs_bytes, lhs_unsigned || rhs_unsigned)),
                }
            }
            // bool vs bool
            (PT::Bool, PT::Bool) => {
                cast_arg(&mut args[0], PT::u64());
                cast_arg(&mut args[1], PT::u64());
                Ok(PT::u64())
            }
            // int vs bool
            (PT::Int(lhs_bytes, lhs_unsigned), PT::Bool) => {
                if lhs_bytes != 8 {
                    cast_arg(&mut args[0], PT::int(8, lhs_unsigned));
                }
                cast_arg(&mut args[1], PT::u64());
                Ok(PT::u64())
            }
            (PT::Bool, PT::Int(rhs_bytes, rhs_unsigned)) => {
                if rhs_bytes != 8 {
                    cast_arg(&mut args[1], PT::int(8, rhs_unsigned));
                }
                cast_arg(&mut args[0], PT::u64());
                Ok(PT::u64())
            }
            // datetime/date/time vs interval
            (lhs_ty @ PT::Datetime(_) | lhs_ty @ PT::Date | lhs_ty @ PT::Time(_), PT::Interval) => {
                Ok(lhs_ty)
            }
            (PT::Interval, rhs_ty @ PT::Datetime(_) | rhs_ty @ PT::Date | rhs_ty @ PT::Time(_)) => {
                Ok(rhs_ty)
            }
            // datetime/time vs int
            (PT::Datetime(lhs_frac) | PT::Time(lhs_frac), PT::Int(rhs_bytes, rhs_unsigned)) => {
                let res_ty = if lhs_frac == 0 {
                    PT::int(8, rhs_unsigned)
                } else {
                    PT::decimal(18, lhs_frac)
                };
                if rhs_bytes != 8 {
                    cast_arg(&mut args[1], res_ty);
                }
                cast_arg(&mut args[0], res_ty);
                Ok(res_ty)
            }
            (PT::Int(lhs_bytes, lhs_unsigned), PT::Datetime(rhs_frac) | PT::Time(rhs_frac)) => {
                let res_ty = if rhs_frac == 0 {
                    PT::int(8, lhs_unsigned)
                } else {
                    PT::decimal(18, rhs_frac)
                };
                if lhs_bytes != 8 {
                    cast_arg(&mut args[0], res_ty);
                }
                cast_arg(&mut args[1], res_ty);
                Ok(res_ty)
            }
            // datetime/time vs bool
            (PT::Datetime(frac) | PT::Time(frac), PT::Bool)
            | (PT::Bool, PT::Datetime(frac) | PT::Time(frac)) => {
                let res_ty = if frac == 0 {
                    PT::u64()
                } else {
                    PT::decimal(18, frac)
                };
                cast_arg(&mut args[0], res_ty);
                cast_arg(&mut args[1], res_ty);
                Ok(res_ty)
            }
            // datetime/time vs datetime/time
            (
                PT::Datetime(lhs_frac) | PT::Time(lhs_frac),
                PT::Datetime(rhs_frac) | PT::Time(rhs_frac),
            ) => {
                let max_frac = lhs_frac.max(rhs_frac);
                let res_ty = if max_frac == 0 {
                    PT::i64()
                } else {
                    PT::decimal(18, max_frac)
                };
                cast_arg(&mut args[0], res_ty);
                cast_arg(&mut args[1], res_ty);
                Ok(res_ty)
            }
            // datetime/time vs date
            (PT::Datetime(frac) | PT::Time(frac), PT::Date)
            | (PT::Date, PT::Datetime(frac) | PT::Time(frac)) => {
                let res_ty = if frac == 0 {
                    PT::i64()
                } else {
                    PT::decimal(18, frac)
                };
                cast_arg(&mut args[0], res_ty);
                cast_arg(&mut args[1], res_ty);
                Ok(res_ty)
            }
            // date vs int
            (PT::Date, PT::Int(rhs_bytes, rhs_unsigned)) => {
                let res_ty = PT::int(8, rhs_unsigned);
                if rhs_bytes != 8 {
                    cast_arg(&mut args[1], res_ty);
                }
                cast_arg(&mut args[0], res_ty);
                Ok(res_ty)
            }
            (PT::Int(lhs_bytes, lhs_unsigned), PT::Date) => {
                let res_ty = PT::int(8, lhs_unsigned);
                if lhs_bytes != 8 {
                    cast_arg(&mut args[0], res_ty);
                }
                cast_arg(&mut args[1], res_ty);
                Ok(res_ty)
            }
            // date vs bool
            (PT::Date, PT::Bool) | (PT::Bool, PT::Date) => {
                cast_arg(&mut args[0], PT::u64());
                cast_arg(&mut args[1], PT::u64());
                Ok(PT::u64())
            }
            // date vs date
            (PT::Date, PT::Date) => {
                cast_arg(&mut args[0], PT::i64());
                cast_arg(&mut args[1], PT::i64());
                Ok(PT::i64())
            }
            // float vs any
            (lhs_ty @ PT::Float(_), _) => {
                cast_arg(&mut args[1], lhs_ty);
                Ok(lhs_ty)
            }
            (_, rhs_ty @ PT::Float(_)) => {
                cast_arg(&mut args[0], rhs_ty);
                Ok(rhs_ty)
            }
            // decimal vs any
            (lhs_ty @ PT::Decimal(..), _) => {
                cast_arg(&mut args[1], lhs_ty);
                Ok(lhs_ty)
            }
            (_, rhs_ty @ PT::Decimal(..)) => {
                cast_arg(&mut args[0], rhs_ty);
                Ok(rhs_ty)
            }
            (_, _) => {
                cast_arg(&mut args[0], PT::f64());
                cast_arg(&mut args[1], PT::f64());
                Ok(PT::f64())
            }
        },
        _ => todo!(),
    }
}

#[inline]
fn fix_pred(pred: &mut Pred) -> Result<()> {
    match pred {
        Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => {
            fix_bools(es);
            Ok(())
        }
        Pred::InSubquery(..) | Pred::NotInSubquery(..) | Pred::Exists(_) | Pred::NotExists(_) => {
            Err(Error::InferSubqueryNotSupport)
        }
        Pred::Not(e) => {
            if !e.ty.is_bool() {
                cast_arg(e.as_mut(), PreciseType::bool());
            }
            Ok(())
        }
        Pred::Func { kind, args } => match *kind {
            PredFuncKind::Equal
            | PredFuncKind::NotEqual
            | PredFuncKind::Greater
            | PredFuncKind::GreaterEqual
            | PredFuncKind::Less
            | PredFuncKind::LessEqual
            | PredFuncKind::SafeEqual => fix_cmp(args.as_mut()),
            _ => todo!(),
        },
    }
}

#[inline]
fn fix_cmp(args: &mut [Expr]) -> Result<()> {
    use PreciseType as PT;
    match (args[0].ty, args[1].ty) {
        (PT::Unknown, _) | (_, PT::Unknown) => return Err(Error::UnknownArgumentType),
        (PT::Compound, _) | (_, PT::Compound) => todo!("compound type infer"),
        (PT::Interval, _) | (_, PT::Interval) => return Err(Error::InvalidTypeToCompare),
        (PT::Null, PT::Null) => unreachable!(),
        /* first, we handle comparision between identical types */
        // float vs float
        (PT::Float(lhs_bytes), PT::Float(rhs_bytes)) => match lhs_bytes.cmp(&rhs_bytes) {
            Ordering::Greater => cast_arg(&mut args[1], PT::Float(lhs_bytes)),
            Ordering::Less => cast_arg(&mut args[0], PT::Float(rhs_bytes)),
            Ordering::Equal => (),
        },
        // decimal vs decimal
        (PT::Decimal(lhs_prec, lhs_frac), PT::Decimal(rhs_prec, rhs_frac)) => {
            let lhs_intg = lhs_prec - lhs_frac;
            let rhs_intg = rhs_prec - rhs_frac;
            match (lhs_intg.cmp(&rhs_intg), lhs_frac.cmp(&rhs_frac)) {
                (Ordering::Equal, Ordering::Equal) => (),
                (Ordering::Equal, Ordering::Greater)
                | (Ordering::Greater, Ordering::Greater | Ordering::Equal) => {
                    let res_ty = PT::Decimal(lhs_prec, lhs_frac);
                    cast_arg(&mut args[1], res_ty);
                }
                (Ordering::Equal, Ordering::Less)
                | (Ordering::Less, Ordering::Less | Ordering::Equal) => {
                    let res_ty = PT::Decimal(rhs_prec, rhs_frac);
                    cast_arg(&mut args[0], res_ty);
                }
                (Ordering::Greater, Ordering::Less) => {
                    let res_ty = PT::Decimal(lhs_intg + rhs_frac, rhs_frac);
                    cast_arg(&mut args[0], res_ty);
                    cast_arg(&mut args[1], res_ty);
                }
                (Ordering::Less, Ordering::Greater) => {
                    let res_ty = PT::Decimal(rhs_intg + lhs_frac, lhs_frac);
                    cast_arg(&mut args[0], res_ty);
                    cast_arg(&mut args[1], res_ty);
                }
            }
        }
        // bool vs bool
        (PT::Bool, PT::Bool) => (),
        // int vs int
        (PT::Int(lhs_bytes, lhs_unsigned), PT::Int(rhs_bytes, rhs_unsigned)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => cast_arg(&mut args[1], PT::Int(lhs_bytes, rhs_unsigned)),
                Ordering::Less => cast_arg(&mut args[0], PT::Int(rhs_bytes, lhs_unsigned)),
                Ordering::Equal => (),
            }
        }
        // datetime vs datetime
        (PT::Datetime(lhs_frac), PT::Datetime(rhs_frac)) => match lhs_frac.cmp(&rhs_frac) {
            Ordering::Greater => cast_arg(&mut args[1], PT::Datetime(lhs_frac)),
            Ordering::Less => cast_arg(&mut args[0], PT::Datetime(rhs_frac)),
            Ordering::Equal => (),
        },
        // date vs date
        (PT::Date, PT::Date) => (),
        // time vs time
        (PT::Time(lhs_frac), PT::Time(rhs_frac)) => match lhs_frac.cmp(&rhs_frac) {
            Ordering::Greater => cast_arg(&mut args[1], PT::Time(lhs_frac)),
            Ordering::Less => cast_arg(&mut args[0], PT::Time(rhs_frac)),
            Ordering::Equal => (),
        },
        // bytes vs bytes
        (PT::Varchar(_, Collation::Binary), PT::Varchar(_, Collation::Binary)) => (),
        // utf8 vs utf8
        (PT::Varchar(_, Collation::Utf8mb4), PT::Varchar(_, Collation::Utf8mb4)) => (),
        // ascii vs ascii
        (PT::Varchar(_, Collation::Ascii), PT::Varchar(_, Collation::Ascii)) => (),
        // fixed bytes vs fixed bytes
        (PT::Char(lhs_bytes, Collation::Binary), PT::Char(rhs_bytes, Collation::Binary)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => cast_arg(&mut args[1], PT::Char(lhs_bytes, Collation::Binary)),
                Ordering::Less => cast_arg(&mut args[0], PT::Char(rhs_bytes, Collation::Binary)),
                Ordering::Equal => (),
            }
        }
        // fixed utf8 vs fixed utf8
        (PT::Char(lhs_bytes, Collation::Utf8mb4), PT::Char(rhs_bytes, Collation::Utf8mb4)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => {
                    cast_arg(&mut args[1], PT::Char(lhs_bytes, Collation::Utf8mb4))
                }
                Ordering::Less => cast_arg(&mut args[0], PT::Char(rhs_bytes, Collation::Utf8mb4)),
                Ordering::Equal => (),
            }
        }
        // fixed ascii vs fixed ascii
        (PT::Char(lhs_bytes, Collation::Ascii), PT::Char(rhs_bytes, Collation::Ascii)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => cast_arg(&mut args[1], PT::Char(lhs_bytes, Collation::Ascii)),
                Ordering::Less => cast_arg(&mut args[0], PT::Char(rhs_bytes, Collation::Ascii)),
                Ordering::Equal => (),
            }
        }
        /* then, we handle special types vs special types */
        // datetime/date/time vs int
        (PT::Datetime(_) | PT::Date | PT::Time(_), PT::Int(bytes, unsigned)) => {
            if bytes != 8 {
                cast_arg(&mut args[1], PT::Int(8, unsigned));
            }
            cast_arg(&mut args[0], PT::Int(8, unsigned))
        }
        (PT::Int(bytes, unsigned), PT::Datetime(_) | PT::Date | PT::Time(_)) => {
            if bytes != 8 {
                cast_arg(&mut args[0], PT::Int(8, unsigned));
            }
            cast_arg(&mut args[1], PT::Int(8, unsigned))
        }
        // datetime/date/time vs bool
        (PT::Datetime(_) | PT::Date | PT::Time(_), PT::Bool)
        | (PT::Bool, PT::Datetime(_) | PT::Date | PT::Time(_)) => {
            cast_arg(&mut args[0], PT::u64());
            cast_arg(&mut args[1], PT::u64())
        }
        /* finally, we handle prioritized fallbacks */
        // float vs any
        (lhs_ty @ PT::Float(_), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Float(_)) => cast_arg(&mut args[0], rhs_ty),
        // decimal vs any
        (lhs_ty @ PT::Decimal(..), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Decimal(..)) => cast_arg(&mut args[0], rhs_ty),
        // bool vs any
        (PT::Bool, _) => cast_arg(&mut args[1], PT::Bool),
        (_, PT::Bool) => cast_arg(&mut args[0], PT::Bool),
        // datetime vs any
        (PT::Datetime(frac), _) => {
            if frac < 6 {
                cast_arg(&mut args[0], PT::Datetime(6))
            }
            cast_arg(&mut args[1], PT::Datetime(6))
        }
        (_, PT::Datetime(frac)) => {
            if frac < 6 {
                cast_arg(&mut args[1], PT::Datetime(6))
            }
            cast_arg(&mut args[0], PT::Datetime(6))
        }
        // date vs any, time vs any
        (PT::Date, _) | (_, PT::Date) | (PT::Time(_), _) | (_, PT::Time(_)) => {
            cast_arg(&mut args[0], PT::Datetime(6));
            cast_arg(&mut args[1], PT::Datetime(6))
        }
        // int vs any
        (PT::Int(..), _) | (_, PT::Int(..)) => {
            cast_arg(&mut args[0], PT::f64());
            cast_arg(&mut args[1], PT::f64());
        }
        // bytes vs any
        (PT::Varchar(bytes, Collation::Binary), _) => {
            cast_arg(&mut args[1], PT::Varchar(bytes, Collation::Binary))
        }
        (_, PT::Varchar(bytes, Collation::Binary)) => {
            cast_arg(&mut args[0], PT::Varchar(bytes, Collation::Binary))
        }
        // fixed bytes vs any
        (lhs_ty @ PT::Char(_, Collation::Binary), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Char(_, Collation::Binary)) => cast_arg(&mut args[0], rhs_ty),
        // utf8 vs any
        (PT::Varchar(bytes, Collation::Utf8mb4), _) => {
            cast_arg(&mut args[1], PT::Varchar(bytes, Collation::Utf8mb4))
        }
        (_, PT::Varchar(bytes, Collation::Utf8mb4)) => {
            cast_arg(&mut args[0], PT::Varchar(bytes, Collation::Utf8mb4))
        }
        // fixed utf8 vs any
        (lhs_ty @ PT::Char(_, Collation::Utf8mb4), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Char(_, Collation::Utf8mb4)) => cast_arg(&mut args[0], rhs_ty),
        // asci vs any
        (PT::Varchar(bytes, Collation::Ascii), _) => {
            cast_arg(&mut args[1], PT::Varchar(bytes, Collation::Ascii))
        }
        (_, PT::Varchar(bytes, Collation::Ascii)) => {
            cast_arg(&mut args[0], PT::Varchar(bytes, Collation::Ascii))
        }
        // fixed ascii vs any
        (lhs_ty @ PT::Char(_, Collation::Ascii), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Char(_, Collation::Ascii)) => cast_arg(&mut args[0], rhs_ty),
        // todo: specialize Char(1)
    }
    Ok(())
}

#[inline]
fn cast_arg(arg: &mut Expr, new_ty: PreciseType) {
    let old = mem::take(arg);
    *arg = Expr::implicit_cast(old, new_ty)
}
