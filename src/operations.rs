extern crate serde;
extern crate reqwest;
extern crate serde_json;
extern crate termion;
extern crate hyper;

use termion::{color};
use serde_json::{Value};

use super::Result;
use super::errors::*;

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    #[serde(rename = "name")]
    name: String,
    #[serde(rename = "configs")]
    configs: Configs,
    #[serde(rename = "partitions")]
    partitions: Vec<Partition>,
}

#[derive(Serialize, Deserialize)]
pub struct Configs {
}

#[derive(Serialize, Deserialize)]
pub struct Partition {
    #[serde(rename = "partition")]
    partition: i64,
    #[serde(rename = "leader")]
    leader: i64,
    #[serde(rename = "replicas")]
    replicas: Vec<Replica>,
}

#[derive(Serialize, Deserialize)]
pub struct Replica {
    #[serde(rename = "broker")]
    broker: i64,
    #[serde(rename = "leader")]
    leader: bool,
    #[serde(rename = "in_sync")]
    in_sync: bool,
}

pub fn print_topic_metadata(url: String, topic: String) -> Result<()> {
    let s = get_topic_metadata(url, topic).chain_err(|| "unable to get topic metadata")?;
    println!("{}", serde_json::to_string_pretty(&s)?);
    Ok(())
}

fn get_topic_metadata(url: String, topic: String) -> Result<Metadata> {
    let resp: Metadata = reqwest::get(format!("{}/topics/{}", url, topic).as_str())?.error_for_status()?.json()?;

    Ok(resp)
}

pub fn print_topics(url: String) -> Result<()> {
    let v = get_topics(url).chain_err(|| "unable to get topics")?;

    v.iter().for_each(|s: &String| {

        if s.starts_with("_") {
            print!("{}", color::Fg(color::Rgb(200, 200, 200)));
        } else {
            print!("{}", color::Fg(color::Blue));
        }

        println!("{}", s);
        print!("{}", color::Fg(color::Reset));
    });

    Ok(())
}

fn get_topics(url: String) -> Result<Vec<String>> {

    let mut resp: reqwest::Response = reqwest::get(format!("{}/topics", url).as_str())?;

    let body = resp.text()?;

    let vv: Value = serde_json::from_str(body.as_str())?;

    let mut res: Vec<String> = Vec::new();

    let vector: &Vec<Value> = vv.as_array().unwrap();

    for v in vector {
        let s: &str = v.as_str().unwrap();
        res.push(s.to_owned());
    }

    //let res2 = vector.iter().map(|v: &Value| { v.as_str().unwrap().to_owned() }).collect::<Vec<String>>();

    Ok(res)
}

pub fn print_messages(url: String, topic: String, partition: u32, offset: Option<u32>, count: Option<u32>) -> Result<()> {
    let v = get_messages_v1(url, topic, partition, offset, count).chain_err(|| "unable to get messages")?;

    v.iter().for_each(|s: &String| {
        println!("{}", s);
    });

    Ok(())
}

#[allow(dead_code)]
fn get_messages() -> Result<Vec<String>> {

    use hyper::header::{ContentType, Accept, qitem};
    //use hyper::mime;

    let client = reqwest::Client::new();

    client
        .post("http://localhost:8082/consumers/my_avro_consumer")
        .body(r#"{"name": "my_consumer_instance", "format": "avro", "auto.offset.reset": "earliest"}"#)
        .header(ContentType("application/vnd.kafka.v2+json".parse().unwrap()))
        .send()?;

    client
        .post("http://localhost:8082/consumers/my_avro_consumer/instances/my_consumer_instance/subscription")
        .body(r#"{"topics":["avrokeytest2"]}"#)
        .header(ContentType("application/vnd.kafka.v2+json".parse().unwrap()))
        .send()?;

    let mut resp = client
        .get("http://localhost:8082/consumers/my_avro_consumer/instances/my_consumer_instance/records")
        .header(Accept(vec![
            qitem("application/vnd.kafka.avro.v2+json".parse().unwrap()),
        ]))
        .send()?;

    client
        .delete("http://localhost:8082/consumers/my_avro_consumer/instances/my_consumer_instance")
        .send()?;

    let body = resp.text()?;

    Ok(vec![body])
}

fn get_messages_v1(url: String, topic: String, partition: u32, offset: Option<u32>, count: Option<u32>) -> Result<Vec<String>> {
    use hyper::header::{Accept, qitem};

    let client = reqwest::Client::new();

    let command_url;

    if let Some(n) = count {
        command_url = format!("{}/topics/{}/partitions/{}/messages?offset={}&count={}", url, topic, partition, offset.unwrap_or(0), n);
    } else {
        command_url = format!("{}/topics/{}/partitions/{}/messages?offset={}", url, topic, partition, offset.unwrap_or(0));
    }

    let mut resp = client.get(command_url.as_str())
        .header(Accept(vec![
            qitem("application/vnd.kafka.avro.v1+json".parse().unwrap()),
        ]))
        .send()?;

    Ok(vec![resp.text()?])
}
