use serde_json::{Value, from_str};

pub fn change(chart_data: &str) -> Result<f64, &'static str> {
    // Parse the string of data into serde_json::Value.
    let serde_parsed_data: Value = match from_str(chart_data) {
        Ok(chart) => chart,
        Err(e) => panic!("Could not parse database jsonb with error: {:?}",e)
    };

    let mut total_delta: f64 = 0.0;
    for candle in serde_parsed_data {
        let change = candle["close"] - candle["open"];
        total_delta = total_delta + change;
    }

    Ok(total_delta)
}


#[cfg(test)]
mod tests {
    use database::Database;

    #[test]
    fn it_works() {
        match Database.new() {
            Err(e) => panic!("Failed to start database"),
            Ok(d) => {
                d.c
            }
        }
    }
}
