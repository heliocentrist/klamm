#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate termion;
extern crate hyper;
extern crate argparse;

mod operations;

use argparse::{ArgumentParser, StoreOption, Store};

use Result;

// We'll put our errors in an `errors` module, and other modules in
// this crate will `use errors::*;` to get access to everything
// `error_chain!` creates.
mod errors {

    extern crate serde_json;
    extern crate reqwest;

    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain!{
        foreign_links {
            //Fmt(::std::fmt::Error);
            //Io(::std::io::Error) #[cfg(unix)];
            Reqwest(reqwest::Error);
            SerdeJson(serde_json::Error);
        }
    }
}

use errors::*;
use operations::*;

struct Options {
    command: String,
    url: String,
    topic: Option<String>,
    partition: Option<u32>,
    offset: Option<u32>,
    count: Option<u32>
}

fn main() {

    let mut options = Options { command: "get-topics".to_owned(), url: "http://localhost:8082".to_owned(), topic: None, partition: None, offset: None, count: None };

    {  // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();
        ap.set_description("Choose a command!");
        ap.refer(&mut options.command).add_argument("command", Store, "A command to execute").required();
        ap.refer(&mut options.url).add_option(&["-u", "--url"], Store, "Kafka REST proxy URL").required();
        ap.refer(&mut options.topic).add_option(&["-t", "--topic"], StoreOption, "The topic");
        ap.refer(&mut options.partition).add_option(&["-p", "--partition"], StoreOption, "The partition");
        ap.refer(&mut options.offset).add_option(&["-o", "--offset"], StoreOption, "The offset");
        ap.refer(&mut options.count).add_option(&["-n", "--count"], StoreOption, "How many messages to retrieve");
        ap.parse_args_or_exit();
    }

    if let Err(ref e) = run(&options) {
        use std::io::Write;
        let stderr = &mut ::std::io::stderr();
        let errmsg = "Error writing to stderr";

        writeln!(stderr, "error: {}", e).expect(errmsg);

        for e in e.iter().skip(1) {
            writeln!(stderr, "caused by: {}", e).expect(errmsg);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            writeln!(stderr, "backtrace: {:?}", backtrace).expect(errmsg);
        }

        ::std::process::exit(1);
    }
}

fn run(options: &Options) -> Result<()> {
    let get_topics: &str = "get-topics";
    let describe_topic: &str = "describe-topic";
    let get_messages: &str = "get-messages";

    match options {
        &Options { ref command, ref url, .. } if command == get_topics =>
            print_topics(url.to_owned()),
        &Options { ref command, ref url, topic: Some(ref t), ..} if command == describe_topic =>
            print_topic_metadata(url.to_owned(), t.to_owned()),
        &Options { ref command, ref url, topic: Some(ref t), partition: Some(p), offset, count } if command == get_messages =>
            print_messages(url.to_owned(), t.to_owned(), p, offset, count),
        _ => bail!("Didn't understand the command")
    }
}




/*

fn get_topics(include_system: bool) ->

fn doit() -> Result<(), hyper::Error>{
    let mut core = Core::new()?;
    let client = Client::new(&core.handle());

    let uri = "http://httpbin.org/ip".parse()?;

    let work = client.get(uri).and_then(|res| {
        println!("Response: {}", res.status());

        res.body().for_each(|chunk| {
            io::stdout()
                .write_all(&chunk)
                .map(|_| ())
                .map_err(From::from)
        })
    });

    Ok(core.run(work)?)
}
*/