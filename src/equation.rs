extern crate meval;
extern crate regex;

use meval::{Expr, Context};

struct Equation<'a> {
    equation: &'a str,
    periodVariables: &'a Vec<str>,
    economicVariables: &'a Vec<f64>,
}

