extern crate serde_json;
extern crate postgres;
extern crate chrono;

use chrono::{Duration, Local, Datelike, NaiveDateTime};
use serde_json::Value;

mod database;

use database::{Momentum, Database, Datasource};

fn main() {

    let time = Local::now();
    // subtract enough days to bring us to last month, then subtract enough days to bring us back 1 year.
    let last_month: i64 = (time.day() + month_days(time.month()-1, time.year())) as i64;
    let end = match time.clone().checked_sub_signed(Duration::days(last_month)) {
        Some(time) => time,
        None => panic!("cannot get end time"),
    };
    let last_year = (previous_year_days(end.month(), end.year()) as i64) + last_month;
    let start = match time.clone().checked_sub_signed(Duration::days(last_year)) {
        Some(time) => time,
        None => panic!("cannot get start time"),
    };


    let market_cap_minimum = 3_000_000_000.0 / 1_000_000.0;
    let data_source = Datasource{version:"v1",source:"AMERITRADE"};
    let strategy: &str = "core_momentum";
    println!("accumulating data base start {} and end {}",start,end);
    match Database::new() {
        Err(e) => panic!("Could not obtain database connection with {} ",e),
        Ok(mut d) => {

            let mut records: Vec<Momentum> = Vec::new();
            for rows in d.loop_through_equity_data(&data_source, 500) { // get page
                for row in rows { // loop through rows in page

                    let symbol: String = row.get(0);
                    let stats: Value = row.get(2);

                    let market_cap = stats["marketCap"].as_f64().unwrap();
                    if market_cap < market_cap_minimum {
                        continue;
                    }

                    // momentum
                    let dividend: f64 = match stats["dividendAmount"].as_f64() {
                        Some(dividend) => dividend,
                        None => 0.0,
                    };

                    // frog in the pan
                    let mut total_trading_days: f64 = 0.0;
                    let mut total_positive_days: f64 = 0.0;
                    let mut total_negative_days: f64 = 0.0;

                    let mut current_month: u32 = 0;
                    let mut momentum: Vec<f64> = Vec::new();
                    let mut added_dividend: bool = false;
                    let mut month_start_price: f64 = 0.0;
                    let mut month_end_price: f64 = 0.0;

                    // compute score
                    let candle_container: Value = row.get(1);
                    let candles: &Vec<Value> =  match candle_container["candles"].as_array() {
                        Some(v)=>v,
                        None => panic!("failed!"),
                    };

                    for candle_value in candles { // row.get(1) is the chart column
                        let candle = candle_value.as_object().unwrap();

                        // Check we are in the right range
                        let candle_date = NaiveDateTime::from_timestamp(candle["datetime"].as_i64().unwrap()/1000, 0);
                        if candle_date.year() < start.year() ||
                            (candle_date.month() < start.month() && candle_date.year() <= start.year()) {
                            //println!("found date before start: {} \n candle date: {}\n\n",start, candle_date);
                            continue;
                        }

                        if candle_date.year() > end.year() ||
                            (candle_date.month() > end.month() && candle_date.year() >= end.year()) {
                            //println!("found date after end: {} \n candle date: {}\n\n",end, candle_date);
                            continue;
                        }

                        total_trading_days = total_trading_days + 1.0;
                        let open: f64 = candle["open"].as_f64().unwrap();
                        let close: f64 = candle["close"].as_f64().unwrap();

                        if current_month == 0 {
                            current_month = candle_date.month();
                            month_start_price = open;
                        }

                        if current_month != candle_date.month() {
                            current_month = candle_date.month();
                            if !added_dividend {
                                momentum.push(1.0 + ( ( month_end_price + dividend - month_start_price) / month_start_price ));
                                added_dividend = true;
                            } else {
                                momentum.push(1.0 + ( (month_end_price - month_start_price) / month_start_price ));
                            }

                            month_start_price = open;
                        }
                        month_end_price = close;

                        let change: f64 = close - open;
                        if change > 0.0 {
                            total_positive_days = total_positive_days + 1.0;
                        }

                        if change < 0.0 {
                            total_negative_days = total_negative_days + 1.0;
                        }
                    };

                    let mut score: f64 = 1.0;
                    for entry in momentum {
                        score = score * entry;
                    }

                    score = score - 1.0;

                    let mut fip= 1.0;
                    if score > 0.0 {
                        fip = fip * 1.0 * ( (total_negative_days / total_trading_days) - (total_positive_days / total_trading_days)  )
                    } else {
                        fip = fip * -1.0 * ( (total_negative_days / total_trading_days) - (total_positive_days / total_trading_days)  )
                    }

                    records.push(Momentum{ symbol, strategy, score, fip});

                }
            }

            for record in records {
                d.insert_momentum(&record.symbol[..], strategy, record.score, record.fip);
            }
        }
    }
}

fn previous_year_days(month: u32, year: i32) -> u32 {
    let mut days: u32 = 0;
    let mut counter: u32 = 12;
    let mut current_month = month;
    let mut current_year: i32 = year;
    while counter > 0 {
        days = days + month_days(current_month,current_year);
        if current_month == 1 {
            current_month = 12;
            current_year = current_year - 1;
        } else {
            current_month = current_month - 1;
        }
        counter = counter - 1;
    }

    days -1

}

fn month_days(month: u32, year: i32) -> u32 {
    if month == 9 || month == 4 || month == 6 || month == 11 {
       return 30 as u32;
    }
    if month == 2 {
        if year % 4 == 0 {
            return 29 as u32;
        } else {
            return 28 as u32;
        }
    }

    return 31 as u32;
}