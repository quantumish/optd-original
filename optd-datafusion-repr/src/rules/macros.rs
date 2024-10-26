macro_rules! define_matcher {
    ( $pick_num:ident, ( $typ:expr $(, $children:tt )* ) ) => {
        RuleMatcher::MatchNode {
            typ: $typ,
            children: vec![
                $( crate::rules::macros::define_matcher!($pick_num, $children) ),*
            ],
        }
    };
    ( $pick_num:ident, [$pick_one:tt] ) => {
        RuleMatcher::PickOne {
            pick_to: { let x = $pick_num; $pick_num += 1; x },
            expand: true,
        }
    };
    ( $pick_num:ident, $pick_one:tt ) => {
        RuleMatcher::PickOne {
            pick_to: { let x = $pick_num; $pick_num += 1; x },
            expand: false,
        }
    };
}

macro_rules! define_picks {
    ( ( $typ:expr $(, $children:tt )* ) ) => {
        $( crate::rules::macros::define_picks!($children); )*
    };
    ( [ $pick_one:ident ] ) => {
        let $pick_one : PlanNode<OptRelNodeTyp>;
    };
    ( $pick_one:ident ) => {
        let $pick_one : PlanNode<OptRelNodeTyp>;
    };
}

macro_rules! collect_picks {
    ( @ $name:ident { } { } -> ($($result:tt)*) ) => (
        $name {
            $($result)*
        }
    );

    ( @ $name:ident { ( $typ:expr $(, $children:tt )* ) } { $($rest:tt),* } -> ($($result:tt)*) ) => (
        crate::rules::macros::collect_picks!(@@ $name { $($children),* $(, $rest)* } -> (
            $($result)*
        ))
    );

    ( @ $name:ident { [ $pick_one:ident ] } { $($rest:tt),* } -> ($($result:tt)*) ) => (
        crate::rules::macros::collect_picks!(@@ $name { $($rest),* } -> (
            $($result)*
            $pick_one,
        ))
    );

    ( @ $name:ident { $pick_one:ident } { $($rest:tt),* } -> ($($result:tt)*) ) => (
        crate::rules::macros::collect_picks!(@@ $name { $($rest),* } -> (
            $($result)*
            $pick_one,
        ))
    );

    ( @@ $name:ident { $item:tt $(, $rest:tt )* } -> ($($result:tt)*) ) => (
        crate::rules::macros::collect_picks!(@ $name { $item } { $($rest),* } -> (
            $($result)*
        ))
    );


    ( @@ $name:ident { } -> ($($result:tt)*) ) => (
        crate::rules::macros::collect_picks!(@ $name { } { } -> (
            $($result)*
        ))
    );


    ($name:ident, $($matcher:tt)+) => {
        crate::rules::macros::collect_picks!(@ $name { $($matcher)+ } {} -> ())
    };
}

macro_rules! define_picks_struct {
    ( @ $name:ident { } { } -> ($($result:tt)*) ) => (
        pub struct $name {
            $($result)*
        }
    );

    ( @ $name:ident { ( $typ:expr $(, $children:tt )* ) } { $($rest:tt),* } -> ($($result:tt)*) ) => (
        crate::rules::macros::define_picks_struct!(@@ $name { $($children),* $(, $rest)* } -> (
            $($result)*
        ));
    );


    ( @ $name:ident { [ $pick_one:ident ] } { $($rest:tt),* } -> ($($result:tt)*) ) => (
        crate::rules::macros::define_picks_struct!(@@ $name { $($rest),* } -> (
            $($result)*
            pub $pick_one: PlanNode<OptRelNodeTyp>,
        ));
    );

    ( @ $name:ident { $pick_one:ident } { $($rest:tt),* } -> ($($result:tt)*) ) => (
        crate::rules::macros::define_picks_struct!(@@ $name { $($rest),* } -> (
            $($result)*
            pub $pick_one: PlanNode<OptRelNodeTyp>,
        ));
    );

    ( @@ $name:ident { $item:tt $(, $rest:tt )* } -> ($($result:tt)*) ) => (
        crate::rules::macros::define_picks_struct!(@ $name { $item } { $($rest),* } -> (
            $($result)*
        ));
    );


    ( @@ $name:ident { } -> ($($result:tt)*) ) => (
        crate::rules::macros::define_picks_struct!(@ $name { } { } -> (
            $($result)*
        ));
    );


    ($name:ident, $($matcher:tt)+) => {
        crate::rules::macros::define_picks_struct!(@ $name { $($matcher)+ } {} -> ());
    };
}

macro_rules! apply_matcher {
    ( $pick_num:ident, $input:ident, ( $typ:expr $(, $children:tt )* ) ) => {
        $( crate::rules::macros::apply_matcher!($pick_num, $input, $children) ;)*
    };
    ( $pick_num:ident, $input:ident, [ $pick_one:ident ] ) => {
        {
            $pick_one = $input.remove(&$pick_num).unwrap();
            $pick_num += 1;
        }
    };
    ( $pick_num:ident, $input:ident, $pick_one:ident ) => {
        {
            $pick_one = $input.remove(&$pick_num).unwrap();
            $pick_num += 1;
        }
    };
}

macro_rules! define_rule_inner {
    ($rule_type:expr, $name:ident, $apply:ident, $($matcher:tt)+) => {
        pub struct $name {
            matcher: RuleMatcher<OptRelNodeTyp>,
        }

        impl $name {
            pub fn new() -> Self {
                #[allow(unused_imports)]
                use OptRelNodeTyp::*;

                let mut pick_num = 0;
                let matcher = crate::rules::macros::define_matcher!(pick_num, $($matcher)+);
                let _ = pick_num;
                Self { matcher }
            }
        }

        camelpaste::paste! {
            crate::rules::macros::define_picks_struct! { [<$name Picks>], $($matcher)+ }
        }

        impl <O: Optimizer<OptRelNodeTyp>> Rule<OptRelNodeTyp, O> for $name {
            fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
                &self.matcher
            }

            fn apply(
                &self,
                optimizer: &O,
                mut input: HashMap<usize, PlanNode<OptRelNodeTyp>>,
            ) -> Vec<PlanNode<OptRelNodeTyp>> {

                crate::rules::macros::define_picks!( $($matcher)+ );

                let mut pick_num = 0;

                crate::rules::macros::apply_matcher!(pick_num, input, $($matcher)+);
                let res;
                camelpaste::paste! {
                    res = crate::rules::macros::collect_picks!( [<$name Picks>], $($matcher)+ );
                }
                let _ = pick_num;
                $apply(optimizer, res)
            }

            camelpaste::paste! {
                fn name(&self) -> &'static str {
                    stringify!([< $name:snake >])
                }
            }

            fn is_impl_rule(&self) -> bool {
                $rule_type
            }
        }
    };
}

macro_rules! define_rule {
    ($name:ident, $apply:ident, $($matcher:tt)+) => {
        crate::rules::macros::define_rule_inner! { false, $name, $apply, $($matcher)+ }
    };
}

macro_rules! define_impl_rule {
    ($name:ident, $apply:ident, $($matcher:tt)+) => {
        crate::rules::macros::define_rule_inner! { true, $name, $apply, $($matcher)+ }
    };
}

pub(crate) use apply_matcher;
pub(crate) use collect_picks;
pub(crate) use define_impl_rule;
pub(crate) use define_matcher;
pub(crate) use define_picks;
pub(crate) use define_picks_struct;
pub(crate) use define_rule;
pub(crate) use define_rule_inner;
