#![macro_escape]

//use std;




//// macro /////

#[macro_export]
macro_rules! err {
    ( $code:expr, $( $t:expr ),* ) => {{
        let _ = write!(&mut std::io::stderr(), "\nERROR ({}): ", $code);
        let _ = writeln!(&mut std::io::stderr(), $( $t ),* );
        std::process::exit($code);
    }};
}
